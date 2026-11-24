package sim

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func quiet(seed int64) *Sim {
	cfg := DefaultConfig(seed)
	cfg.Chaos = false
	return New(cfg)
}

// warm runs long enough for every machine to have the 10 samples the
// detector needs before judging it.
func warm(s *Sim) { s.Step(800) } // 8 s

func lastAlert(s *Sim, machine, typ string) *Alert {
	for i := len(s.alerts) - 1; i >= 0; i-- {
		if a := s.alerts[i]; a.Machine == machine && a.Type == typ {
			return &a
		}
	}
	return nil
}

func TestStallIsSuspectedAfterAboutOneSecond(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		s := quiet(seed)
		warm(s)
		s.Stall("m-0007")
		s.Step(500) // 5 s, less than the 15 s stall
		a := lastAlert(s, "m-0007", "suspected")
		if a == nil {
			t.Fatalf("seed %d: stall never suspected", seed)
		}
		if a.SilentMs < 1000 || a.SilentMs > 1300 {
			t.Fatalf("seed %d: suspected after %d ms of silence, want 1000-1300 ms", seed, a.SilentMs)
		}
		if a.Phi < 8 {
			t.Fatalf("seed %d: phi %.2f at the alert", seed, a.Phi)
		}
	}
}

func TestCrashIsReportedAtOnce(t *testing.T) {
	s := quiet(2)
	warm(s)
	s.Crash("m-0011")
	a := s.alerts[len(s.alerts)-1]
	if a.Machine != "m-0011" || a.Type != "connection_lost" || a.Millis != s.Millis() {
		t.Fatalf("got %+v at %d ms, want connection_lost for m-0011 at once", a, s.Millis())
	}
	if a.SilentMs >= PeriodMillis+JitterMillis+TickMillis {
		t.Fatalf("silence %d ms: the Last Will must not wait for the heartbeats", a.SilentMs)
	}
}

func TestHealthyFleetRaisesNoAlert(t *testing.T) {
	s := quiet(3)
	s.Step(int(time.Hour / (TickMillis * time.Millisecond)))
	if len(s.alerts) != 0 {
		t.Fatalf("%d alerts on a healthy fleet in one hour, first: %+v", len(s.alerts), s.alerts[0])
	}
	for _, m := range s.State().Machines {
		if m.View != "healthy" {
			t.Fatalf("%s is %q on a healthy fleet", m.ID, m.View)
		}
	}
}

func TestRecoveryAndReconnection(t *testing.T) {
	s := quiet(4)
	warm(s)
	s.Stall("m-0003")
	s.Step(1600) // 16 s: the 15 s stall is over
	if lastAlert(s, "m-0003", "recovered") == nil {
		t.Fatal("no recovered alert after the stall")
	}
	s.Crash("m-0004")
	s.Step(2100) // 21 s: reconnected after 20 s
	if lastAlert(s, "m-0004", "reconnected") == nil {
		t.Fatal("no reconnected alert after the crash")
	}
	// Freshly reconnected, not enough samples yet: a stall now is not judged
	// (the detector does not suspect a machine it cannot estimate).
	s.Stall("m-0004")
	s.Step(300)
	if a := lastAlert(s, "m-0004", "suspected"); a != nil && a.Millis > s.Millis()-3000 {
		t.Fatalf("machine judged during warm-up: %+v", a)
	}
}

func TestChaosScheduleShowsBothFailureKinds(t *testing.T) {
	s := New(DefaultConfig(5))
	s.Step(int(5 * time.Minute / (TickMillis * time.Millisecond)))
	st := s.State()
	if st.Counts["connection_lost"] < 5 || st.Counts["suspected"] < 8 {
		t.Fatalf("in 5 min of chaos: %v, want ~6 crashes and ~10 stalls", st.Counts)
	}
	if m := st.Detection["suspected"]; m < 1000 || m > 1300 {
		t.Fatalf("median stall detection %d ms", m)
	}
}

func TestSameSeedSameRun(t *testing.T) {
	run := func() string {
		s := New(DefaultConfig(42))
		s.Step(3000)
		s.Stall("m-0001")
		s.Crash("m-0002")
		s.Step(2000)
		b, _ := json.Marshal(s.State())
		return string(b) + string(must(json.Marshal(s.PhiHistory("m-0001"))))
	}
	if run() != run() {
		t.Fatal("two runs with the same seed and actions differ")
	}
}

// The page calls PhiHistory before any evaluation: it must encode as [], not
// null (a null history crashed the first render).
func TestPhiHistoryIsNeverNull(t *testing.T) {
	s := quiet(6)
	for _, id := range []string{"m-0000", "no-such-machine"} {
		if got := string(must(json.Marshal(s.PhiHistory(id)))); got != "[]" {
			t.Fatalf("PhiHistory(%q) before any step = %s, want []", id, got)
		}
	}
	if st := string(must(json.Marshal(s.State()))); strings.Contains(st, "null") {
		t.Fatalf("initial state contains null: %s", st)
	}
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}
