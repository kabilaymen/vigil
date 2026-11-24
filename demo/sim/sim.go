// Package sim runs a simulated fleet against Vigil's real failure detector
// (detector/tracker and detector/phi), for the browser demo. Time is
// simulated: nothing here reads the wall clock, so a run replays exactly from
// its seed.
//
// What is simulated is what reaches the detector in production: one
// telemetry message per machine every 0.5 s (the heartbeat), timestamped on
// arrival, and the broker's Last Will ("offline") when a machine's
// connection drops. The detector is evaluated every 200 ms, as in
// detector/main.go, with the same configuration.
package sim

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/kabilaymen/vigil/detector/tracker"
)

const (
	TickMillis     = 10  // simulation resolution
	EvalEveryTicks = 20  // detector evaluation period: 200 ms
	PeriodMillis   = 500 // 2 telemetry messages per second per machine
	JitterMillis   = 20  // arrival jitter around the period
	HistoryPoints  = 150 // phi history kept per machine: 30 s at 200 ms
)

var types = []struct {
	name             string
	temp, throughput float64
}{
	{"embosser", 45, 120}, {"laser-personalizer", 60, 80}, {"chip-encoder", 38, 200},
}

type Config struct {
	Machines     int
	Seed         int64
	Chaos        bool // production schedule: a crash every 45 s, a stall every 30 s
	CrashEvery   time.Duration
	RecoverAfter time.Duration
	StallEvery   time.Duration
	StallFor     time.Duration
	MaxAlerts    int
}

func DefaultConfig(seed int64) Config {
	return Config{Machines: 50, Seed: seed, Chaos: true, CrashEvery: 45 * time.Second,
		RecoverAfter: 20 * time.Second, StallEvery: 30 * time.Second, StallFor: 15 * time.Second, MaxAlerts: 30}
}

// The detector's configuration in production (detector/main.go).
func trackerConfig() tracker.Config {
	return tracker.Config{Threshold: 8, WindowSize: 100, MinSamples: 10, MinStd: 100 * time.Millisecond}
}

type machine struct {
	id, typ          string
	temp, throughput float64
	nextBeat         int // tick of the next telemetry message
	crashedUntil     int // > now: connection dropped, reconnects then
	stalledUntil     int // > now: connected but silent
	beatsSinceOnline int
	lastBeat         int // tick of the last telemetry message received
	view             string // what the detector currently says
	phi              []float64
}

type Alert struct {
	Millis   int     `json:"ms"`
	Machine  string  `json:"machine"`
	Type     string  `json:"type"`
	SilentMs int     `json:"silentMs"`
	Phi      float64 `json:"phi"`
}

type Sim struct {
	cfg       Config
	rand      *rand.Rand
	tr        *tracker.Tracker
	machines  []*machine
	tick      int
	nextCrash int
	nextStall int
	alerts    []Alert
	counts    map[string]int
	detection map[string][]int // silence at detection, per alert type
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func ticks(d time.Duration) int { return int(d / (TickMillis * time.Millisecond)) }

func New(cfg Config) *Sim {
	s := &Sim{cfg: cfg, rand: rand.New(rand.NewSource(cfg.Seed)), tr: tracker.New(trackerConfig()),
		counts: map[string]int{}, detection: map[string][]int{}}
	for i := 0; i < cfg.Machines; i++ {
		t := types[i%len(types)]
		m := &machine{id: fmt.Sprintf("m-%04d", i), typ: t.name, temp: t.temp, throughput: t.throughput,
			nextBeat: s.rand.Intn(PeriodMillis / TickMillis), view: "warming up"}
		s.machines = append(s.machines, m)
		s.tr.Online(m.id, s.now()) // retained "online" status on connect
	}
	s.nextCrash = ticks(cfg.CrashEvery)
	s.nextStall = ticks(cfg.StallEvery)
	return s
}

func (s *Sim) now() time.Time { return epoch.Add(time.Duration(s.tick*TickMillis) * time.Millisecond) }
func (s *Sim) Millis() int    { return s.tick * TickMillis }

func (s *Sim) find(id string) *machine {
	for _, m := range s.machines {
		if m.id == id {
			return m
		}
	}
	return nil
}

func (s *Sim) connected(m *machine) bool { return m.crashedUntil <= s.tick }

// Step advances the fleet and the detector by n ticks of 10 ms.
func (s *Sim) Step(n int) {
	for i := 0; i < n; i++ {
		s.tick++
		now := s.now()
		if s.cfg.Chaos {
			s.chaos()
		}
		for _, m := range s.machines {
			if m.crashedUntil == s.tick { // the machine comes back and announces itself
				s.record(s.tr.Online(m.id, now))
				m.beatsSinceOnline = 0
				m.nextBeat = s.tick + 1
			}
			if !s.connected(m) || s.tick < m.nextBeat {
				continue
			}
			jitter := s.rand.Intn(2*JitterMillis/TickMillis+1) - JitterMillis/TickMillis
			m.nextBeat = s.tick + PeriodMillis/TickMillis + jitter
			if m.stalledUntil > s.tick {
				continue // stuck application: the connection stays up, nothing is published
			}
			s.telemetry(m)
			m.beatsSinceOnline++
			m.lastBeat = s.tick
			s.record(s.tr.Heartbeat(m.id, now))
		}
		if s.tick%EvalEveryTicks == 0 {
			s.record(s.tr.Evaluate(now))
			phis, _ := s.tr.Snapshot(now)
			for _, m := range s.machines {
				m.phi = append(m.phi, round(phis[m.id], 2))
				if len(m.phi) > HistoryPoints {
					m.phi = m.phi[len(m.phi)-HistoryPoints:]
				}
				if m.view == "warming up" && m.beatsSinceOnline > trackerConfig().MinSamples {
					m.view = "healthy"
				}
			}
		}
	}
}

// telemetry moves temperature and throughput as simulator/simulator.py does:
// a bounded random walk around the machine type's nominal values.
func (s *Sim) telemetry(m *machine) {
	for _, t := range types {
		if t.name == m.typ {
			m.temp += s.rand.NormFloat64()*0.4 + 0.05*(t.temp-m.temp)
			m.throughput = math.Max(0, m.throughput+s.rand.NormFloat64()*2+0.1*(t.throughput-m.throughput))
		}
	}
}

func (s *Sim) chaos() {
	if s.tick >= s.nextCrash {
		s.nextCrash = s.tick + ticks(s.cfg.CrashEvery)
		if m := s.randomHealthy(); m != nil {
			s.Crash(m.id)
		}
	}
	if s.tick >= s.nextStall {
		s.nextStall = s.tick + ticks(s.cfg.StallEvery)
		if m := s.randomHealthy(); m != nil {
			s.Stall(m.id)
		}
	}
}

func (s *Sim) randomHealthy() *machine {
	var ok []*machine
	for _, m := range s.machines {
		if s.connected(m) && m.stalledUntil <= s.tick && m.view == "healthy" {
			ok = append(ok, m)
		}
	}
	if len(ok) == 0 {
		return nil
	}
	return ok[s.rand.Intn(len(ok))]
}

// Crash drops a machine's TCP connection without an MQTT DISCONNECT: the
// broker publishes its Last Will at once. It reconnects after RecoverAfter.
func (s *Sim) Crash(id string) {
	m := s.find(id)
	if m == nil || !s.connected(m) {
		return
	}
	m.crashedUntil = s.tick + ticks(s.cfg.RecoverAfter)
	m.stalledUntil = 0
	s.record(s.tr.Offline(m.id, s.now()))
}

// Stall freezes a machine's application while its connection stays up: MQTT
// keepalives keep flowing, so no Last Will. Only the heartbeat analysis can
// notice.
func (s *Sim) Stall(id string) {
	m := s.find(id)
	if m == nil || !s.connected(m) || m.stalledUntil > s.tick {
		return
	}
	m.stalledUntil = s.tick + ticks(s.cfg.StallFor)
}

func (s *Sim) SetChaos(on bool) { s.cfg.Chaos = on }

func (s *Sim) record(alerts []tracker.Alert) {
	// tracker.Evaluate ranges over a map, so simultaneous alerts come in a
	// random order; sort them so that a seed always replays the same run.
	sort.Slice(alerts, func(i, j int) bool { return alerts[i].Machine < alerts[j].Machine })
	for _, a := range alerts {
		m := s.find(a.Machine)
		switch a.Type {
		case tracker.Suspected:
			m.view = "suspected"
		case tracker.ConnectionLost:
			m.view = "connection lost"
		case tracker.Recovered:
			m.view = "healthy"
		case tracker.Reconnected:
			m.view = "warming up"
		}
		silent := int(a.Silence / time.Millisecond)
		s.counts[string(a.Type)]++
		if a.Type == tracker.Suspected || a.Type == tracker.ConnectionLost {
			s.detection[string(a.Type)] = append(s.detection[string(a.Type)], silent)
		}
		s.alerts = append(s.alerts, Alert{Millis: s.Millis(), Machine: a.Machine, Type: string(a.Type),
			SilentMs: silent, Phi: round(a.Phi, 1)})
		if len(s.alerts) > s.cfg.MaxAlerts {
			s.alerts = s.alerts[len(s.alerts)-s.cfg.MaxAlerts:]
		}
	}
}

func round(x float64, digits int) float64 {
	p := math.Pow(10, float64(digits))
	return math.Round(x*p) / p
}

// ---------------------------------------------------------------- state for the page

type MachineState struct {
	ID         string  `json:"id"`
	Type       string  `json:"type"`
	View       string  `json:"view"`  // what the detector says
	Truth      string  `json:"truth"` // what is really happening
	Phi        float64 `json:"phi"`
	SilentMs   int     `json:"silentMs"`
	Temp       float64 `json:"temp"`
	Throughput float64 `json:"throughput"`
}

type State struct {
	Millis    int            `json:"ms"`
	Machines  []MachineState `json:"machines"`
	Alerts    []Alert        `json:"alerts"`
	Counts    map[string]int `json:"counts"`
	Detection map[string]int `json:"detectionMedianMs"`
	Chaos     bool           `json:"chaos"`
	TickMs    int            `json:"tickMs"`
}

func (s *Sim) truth(m *machine) string {
	switch {
	case !s.connected(m):
		return "crashed"
	case m.stalledUntil > s.tick:
		return "stalled"
	default:
		return "running"
	}
}

func median(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	c := append([]int(nil), xs...)
	for i := 1; i < len(c); i++ { // small lists: insertion sort
		for j := i; j > 0 && c[j] < c[j-1]; j-- {
			c[j], c[j-1] = c[j-1], c[j]
		}
	}
	return c[len(c)/2]
}

func (s *Sim) State() State {
	now := s.now()
	phis, _ := s.tr.Snapshot(now)
	st := State{Millis: s.Millis(), Alerts: append([]Alert{}, s.alerts...), Counts: map[string]int{},
		Detection: map[string]int{}, Chaos: s.cfg.Chaos, TickMs: TickMillis}
	for k, v := range s.counts {
		st.Counts[k] = v
	}
	for k, v := range s.detection {
		st.Detection[k] = median(v)
	}
	for _, m := range s.machines {
		st.Machines = append(st.Machines, MachineState{ID: m.id, Type: m.typ, View: m.view, Truth: s.truth(m),
			Phi: round(phis[m.id], 2), SilentMs: (s.tick - m.lastBeat) * TickMillis, Temp: round(m.temp, 1), Throughput: round(m.throughput, 1)})
	}
	return st
}

// PhiHistory returns the last 30 s of a machine's phi, one point per 200 ms.
func (s *Sim) PhiHistory(id string) []float64 {
	if m := s.find(id); m != nil {
		return append([]float64{}, m.phi...) // never nil: JSON "[]", not "null"
	}
	return []float64{}
}
