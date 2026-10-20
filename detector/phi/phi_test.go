package phi

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

var t0 = time.Date(2027, 4, 1, 9, 0, 0, 0, time.UTC)

// feed adds n heartbeats spaced by interval (± jitter) and returns the last arrival time.
func feed(w *Window, n int, interval, jitter time.Duration, seed int64) time.Time {
	r := rand.New(rand.NewSource(seed))
	t := t0
	for i := 0; i < n; i++ {
		d := interval
		if jitter > 0 {
			d += time.Duration(r.Int63n(int64(2*jitter))) - jitter
		}
		t = t.Add(d)
		w.Heartbeat(t)
	}
	return t
}

func TestNotJudgedDuringWarmup(t *testing.T) {
	w := NewWindow(100, 3, 50*time.Millisecond)
	if got := w.Phi(t0.Add(time.Hour)); got != 0 {
		t.Fatalf("no heartbeat yet: phi = %v, want 0", got)
	}
	last := feed(w, 2, time.Second, 0, 1) // 2 heartbeats = 1 interval < minSamples
	if got := w.Phi(last.Add(time.Hour)); got != 0 {
		t.Fatalf("still warming up: phi = %v, want 0", got)
	}
}

func TestRegularHeartbeats(t *testing.T) {
	w := NewWindow(100, 3, 50*time.Millisecond)
	last := feed(w, 50, time.Second, 50*time.Millisecond, 2)

	if p := w.Phi(last.Add(900 * time.Millisecond)); p > 1 {
		t.Errorf("on time: phi = %.2f, want <= 1", p)
	}
	if p := w.Phi(last.Add(3 * time.Second)); p < 8 {
		t.Errorf("3 intervals late: phi = %.2f, want >= 8 (suspected)", p)
	}
}

func TestPhiIsMonotonic(t *testing.T) {
	w := NewWindow(100, 3, 50*time.Millisecond)
	last := feed(w, 50, time.Second, 100*time.Millisecond, 3)
	prev := -1.0
	for d := time.Duration(0); d <= 10*time.Second; d += 100 * time.Millisecond {
		p := w.Phi(last.Add(d))
		if p < prev {
			t.Fatalf("phi decreased at +%v: %.3f < %.3f", d, p, prev)
		}
		if math.IsNaN(p) || math.IsInf(p, -1) {
			t.Fatalf("phi at +%v is %v", d, p)
		}
		prev = p
	}
}

func TestAdaptsToEachMachineRate(t *testing.T) {
	fast := NewWindow(100, 3, 20*time.Millisecond)
	slow := NewWindow(100, 3, 20*time.Millisecond)
	lf := feed(fast, 50, 200*time.Millisecond, 10*time.Millisecond, 4)
	ls := feed(slow, 50, 5*time.Second, 200*time.Millisecond, 5)

	// 2 seconds of silence: dramatic for a 200 ms machine, normal for a 5 s one.
	if p := fast.Phi(lf.Add(2 * time.Second)); p < 8 {
		t.Errorf("fast machine silent 2s: phi = %.2f, want >= 8", p)
	}
	if p := slow.Phi(ls.Add(2 * time.Second)); p > 1 {
		t.Errorf("slow machine silent 2s: phi = %.2f, want <= 1", p)
	}
}

func TestJitterRaisesTolerance(t *testing.T) {
	calm := NewWindow(100, 3, 10*time.Millisecond)
	noisy := NewWindow(100, 3, 10*time.Millisecond)
	lc := feed(calm, 100, time.Second, 10*time.Millisecond, 6)
	ln := feed(noisy, 100, time.Second, 400*time.Millisecond, 7)
	late := 1500 * time.Millisecond
	if pc, pn := calm.Phi(lc.Add(late)), noisy.Phi(ln.Add(late)); pn >= pc {
		t.Errorf("same delay, noisier machine should be less suspicious: calm %.2f, noisy %.2f", pc, pn)
	}
}

func TestSlidingWindowForgetsOldBehaviour(t *testing.T) {
	w := NewWindow(20, 3, 20*time.Millisecond)
	feed(w, 50, 5*time.Second, 0, 8) // used to be slow...
	last := w.last
	for i := 0; i < 20; i++ { // ...now sends every 500 ms
		last = last.Add(500 * time.Millisecond)
		w.Heartbeat(last)
	}
	if p := w.Phi(last.Add(3 * time.Second)); p < 8 {
		t.Errorf("window should only reflect the last 20 intervals: phi = %.2f, want >= 8", p)
	}
}

func TestMinStdAvoidsDivisionByZero(t *testing.T) {
	w := NewWindow(100, 3, 50*time.Millisecond)
	last := feed(w, 20, time.Second, 0, 9) // perfectly regular: stddev = 0
	p := w.Phi(last.Add(1010 * time.Millisecond))
	if math.IsNaN(p) || math.IsInf(p, 0) || p > 1 {
		t.Errorf("10 ms late on a perfect clock with minStd 50ms: phi = %v, want small and finite", p)
	}
}

func TestOutOfOrderHeartbeatIgnored(t *testing.T) {
	w := NewWindow(100, 3, 50*time.Millisecond)
	last := feed(w, 20, time.Second, 0, 10)
	w.Heartbeat(last.Add(-10 * time.Second)) // late duplicate replayed from Kafka
	if p := w.Phi(last.Add(900 * time.Millisecond)); p > 1 {
		t.Errorf("stale heartbeat must not corrupt the window: phi = %.2f", p)
	}
}
