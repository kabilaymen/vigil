// Package tracker combines the two failure signals Vigil has for each machine:
//
//   - connection-level: the MQTT Last Will ("offline") published by the broker
//     when a machine's TCP session dies;
//   - application-level: heartbeats (telemetry messages) judged by a phi
//     accrual detector, which catches machines that are still connected but
//     stalled — MQTT keepalives are sent by the client library's network
//     thread and keep flowing even if the application is stuck.
package tracker

import (
	"time"

	"github.com/kabilaymen/vigil/detector/phi"
)

type Config struct {
	Threshold  float64       // φ above which a machine is suspected
	WindowSize int           // intervals kept per machine
	MinSamples int           // intervals required before judging a machine
	MinStd     time.Duration // lower bound on the interval standard deviation
}

type AlertType string

const (
	Suspected      AlertType = "suspected"       // silent for too long while connected
	Recovered      AlertType = "recovered"       // heartbeat received after a suspicion
	ConnectionLost AlertType = "connection_lost" // Last Will fired by the broker
	Reconnected    AlertType = "reconnected"     // machine announced itself online again
)

type Alert struct {
	Machine string        `json:"machine_id"`
	Type    AlertType     `json:"type"`
	At      time.Time     `json:"at"`
	Phi     float64       `json:"phi,omitempty"`
	Silence time.Duration `json:"silence_ns,omitempty"` // time since the last heartbeat
}

type machine struct {
	win       *phi.Window
	connected bool
	suspected bool
}

type Tracker struct {
	cfg      Config
	machines map[string]*machine
}

func New(cfg Config) *Tracker {
	return &Tracker{cfg: cfg, machines: make(map[string]*machine)}
}

func (t *Tracker) get(id string) *machine {
	m, ok := t.machines[id]
	if !ok {
		m = &machine{win: t.newWindow(), connected: true}
		t.machines[id] = m
	}
	return m
}

func (t *Tracker) newWindow() *phi.Window {
	return phi.NewWindow(t.cfg.WindowSize, t.cfg.MinSamples, t.cfg.MinStd)
}

// Heartbeat records an application-level sign of life.
func (t *Tracker) Heartbeat(id string, at time.Time) []Alert {
	m := t.get(id)
	var alerts []Alert
	if m.suspected && m.connected {
		alerts = append(alerts, Alert{Machine: id, Type: Recovered, At: at, Silence: at.Sub(m.win.Last())})
	}
	m.suspected = false
	m.connected = true
	m.win.Heartbeat(at)
	return alerts
}

// Offline handles the broker's Last Will: the connection is gone, no need to wait for φ.
func (t *Tracker) Offline(id string, at time.Time) []Alert {
	m := t.get(id)
	if !m.connected {
		return nil
	}
	m.connected, m.suspected = false, true
	return []Alert{{Machine: id, Type: ConnectionLost, At: at, Silence: silence(m.win, at)}}
}

// Online handles a machine (re)announcing itself. The heartbeat history is
// reset: the downtime gap is not a normal interval and would inflate the
// variance, making the detector lenient for a long time after every outage.
func (t *Tracker) Online(id string, at time.Time) []Alert {
	m, known := t.machines[id]
	if !known {
		t.get(id)
		return nil
	}
	wasDown := !m.connected
	m.connected, m.suspected = true, false
	m.win = t.newWindow()
	if wasDown {
		return []Alert{{Machine: id, Type: Reconnected, At: at}}
	}
	return nil
}

// Evaluate checks every connected machine at `now` and reports new suspicions.
func (t *Tracker) Evaluate(now time.Time) []Alert {
	var alerts []Alert
	for id, m := range t.machines {
		if !m.connected || m.suspected {
			continue
		}
		if p := m.win.Phi(now); p >= t.cfg.Threshold {
			m.suspected = true
			alerts = append(alerts, Alert{Machine: id, Type: Suspected, At: now, Phi: p, Silence: now.Sub(m.win.Last())})
		}
	}
	return alerts
}

// Snapshot returns the current φ of every machine and the number of machines
// considered down (suspected or disconnected).
func (t *Tracker) Snapshot(now time.Time) (phis map[string]float64, down int) {
	phis = make(map[string]float64, len(t.machines))
	for id, m := range t.machines {
		phis[id] = m.win.Phi(now)
		if m.suspected || !m.connected {
			down++
		}
	}
	return phis, down
}

func silence(w *phi.Window, at time.Time) time.Duration {
	if w.Last().IsZero() {
		return 0
	}
	return at.Sub(w.Last())
}
