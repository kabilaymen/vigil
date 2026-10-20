// Package phi implements the phi accrual failure detector
// (Hayashibara et al., "The φ Accrual Failure Detector", SRDS 2004).
//
// Instead of a binary "alive / dead" answer after a fixed timeout, the detector
// outputs a suspicion level φ derived from the observed distribution of
// heartbeat inter-arrival times: φ = -log10(P(next heartbeat arrives later than now)).
package phi

import (
	"math"
	"time"
)

// Window keeps the last `size` inter-arrival intervals of one machine's heartbeats.
type Window struct {
	size       int
	minSamples int
	minStd     float64 // seconds
	intervals  []float64
	last       time.Time
}

func NewWindow(size, minSamples int, minStd time.Duration) *Window {
	return &Window{size: size, minSamples: minSamples, minStd: minStd.Seconds()}
}

// Heartbeat records an arrival. Arrivals that are not strictly after the
// previous one (duplicates, replays) are ignored.
func (w *Window) Heartbeat(t time.Time) {
	if !w.last.IsZero() {
		if !t.After(w.last) {
			return
		}
		w.intervals = append(w.intervals, t.Sub(w.last).Seconds())
		if len(w.intervals) > w.size {
			w.intervals = w.intervals[1:]
		}
	}
	w.last = t
}

// Last returns the time of the most recent heartbeat.
func (w *Window) Last() time.Time { return w.last }

// Phi returns the suspicion level at `now`, or 0 while fewer than minSamples
// intervals have been observed (the machine is not judged during warm-up).
func (w *Window) Phi(now time.Time) float64 {
	n := len(w.intervals)
	if n < w.minSamples {
		return 0
	}
	var sum float64
	for _, x := range w.intervals {
		sum += x
	}
	mean := sum / float64(n)
	var sq float64
	for _, x := range w.intervals {
		sq += (x - mean) * (x - mean)
	}
	std := math.Max(math.Sqrt(sq/float64(n)), w.minStd)

	elapsed := now.Sub(w.last).Seconds()
	pLater := 0.5 * math.Erfc((elapsed-mean)/(std*math.Sqrt2))
	const floor = 1e-300 // erfc underflows to 0 for very long silences
	if pLater < floor {
		pLater = floor
	}
	return -math.Log10(pLater)
}
