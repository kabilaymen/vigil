package tracker

import (
	"testing"
	"time"
)

var t0 = time.Date(2027, 4, 1, 9, 0, 0, 0, time.UTC)

func cfg() Config {
	return Config{Threshold: 8, WindowSize: 50, MinSamples: 5, MinStd: 50 * time.Millisecond}
}

// beat sends n heartbeats one second apart starting after `from`, returns the last time.
func beat(tr *Tracker, id string, from time.Time, n int) time.Time {
	t := from
	for i := 0; i < n; i++ {
		t = t.Add(time.Second)
		tr.Heartbeat(id, t)
	}
	return t
}

func only(t *testing.T, alerts []Alert, want AlertType) Alert {
	t.Helper()
	if len(alerts) != 1 || alerts[0].Type != want {
		t.Fatalf("got %+v, want exactly one %s", alerts, want)
	}
	return alerts[0]
}

func TestStalledMachineIsSuspectedThenRecovers(t *testing.T) {
	tr := New(cfg())
	last := beat(tr, "m1", t0, 20)

	if a := tr.Evaluate(last.Add(1100 * time.Millisecond)); len(a) != 0 {
		t.Fatalf("on time, got %+v", a)
	}
	a := only(t, tr.Evaluate(last.Add(4*time.Second)), Suspected)
	if a.Phi < 8 || a.Silence != 4*time.Second {
		t.Fatalf("bad alert %+v", a)
	}
	if a := tr.Evaluate(last.Add(6 * time.Second)); len(a) != 0 {
		t.Fatalf("suspicion must be reported once, got %+v", a)
	}
	only(t, tr.Heartbeat("m1", last.Add(7*time.Second)), Recovered)
}

func TestLastWillIsImmediate(t *testing.T) {
	tr := New(cfg())
	last := beat(tr, "m1", t0, 20)
	a := only(t, tr.Offline("m1", last.Add(200*time.Millisecond)), ConnectionLost)
	if a.Silence != 200*time.Millisecond {
		t.Fatalf("silence = %v", a.Silence)
	}
	if a := tr.Evaluate(last.Add(time.Minute)); len(a) != 0 {
		t.Fatalf("disconnected machine must not be re-suspected, got %+v", a)
	}
	if a := tr.Offline("m1", last.Add(time.Minute)); len(a) != 0 {
		t.Fatalf("duplicate Last Will, got %+v", a)
	}
	only(t, tr.Online("m1", last.Add(2*time.Minute)), Reconnected)
}

func TestNoAlertDuringWarmup(t *testing.T) {
	tr := New(cfg())
	last := beat(tr, "m1", t0, 3) // 2 intervals < MinSamples
	if a := tr.Evaluate(last.Add(time.Hour)); len(a) != 0 {
		t.Fatalf("machine still warming up, got %+v", a)
	}
}

func TestReconnectResetsHistory(t *testing.T) {
	tr := New(cfg())
	last := beat(tr, "m1", t0, 20)
	tr.Offline("m1", last.Add(time.Second))
	back := last.Add(10 * time.Minute)
	tr.Online("m1", back)
	// Without a reset, the 10-minute gap would be in the window and a
	// 4-second silence would look normal.
	last = beat(tr, "m1", back, 10)
	only(t, tr.Evaluate(last.Add(4*time.Second)), Suspected)
}

func TestOnlineForUnknownMachineIsSilent(t *testing.T) {
	tr := New(cfg())
	if a := tr.Online("m9", t0); len(a) != 0 {
		t.Fatalf("first sighting is not a reconnection, got %+v", a)
	}
}

func TestSnapshotCountsDownMachines(t *testing.T) {
	tr := New(cfg())
	l1 := beat(tr, "m1", t0, 10)
	beat(tr, "m2", t0, 10)
	tr.Offline("m2", l1)
	tr.Evaluate(l1.Add(10 * time.Second)) // m1 suspected
	if _, down := tr.Snapshot(l1.Add(10 * time.Second)); down != 2 {
		t.Fatalf("down = %d, want 2", down)
	}
}
