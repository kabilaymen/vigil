package window

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

func TestAggregatesAndClosesOnWatermark(t *testing.T) {
	a := New(10*time.Second, 2*time.Second)
	a.Add(0, 0, "m1", t0.Add(1*time.Second), 20, 100)
	a.Add(0, 1, "m1", t0.Add(5*time.Second), 30, 200)
	a.Add(1, 0, "m2", t0.Add(9*time.Second), 50, 10)
	// 11 s is past the bucket end but within the 2 s lateness: still open.
	if c := a.Add(0, 2, "m1", t0.Add(11*time.Second), 0, 0); len(c) != 0 {
		t.Fatalf("closed too early: %+v", c)
	}
	// A record from the first bucket arriving late but within lateness counts.
	a.Add(1, 1, "m2", t0.Add(8*time.Second), 70, 30)
	// Partition 0 is past the bucket end + lateness, but partition 1 is not:
	// the bucket stays open until both are.
	if c := a.Add(0, 3, "m1", t0.Add(12*time.Second), 0, 0); len(c) != 0 {
		t.Fatalf("a fast partition closed the bucket for a slow one: %+v", c)
	}
	c := a.Add(1, 2, "m2", t0.Add(12*time.Second), 0, 0)
	if len(c) != 1 || !c[0].Bucket.Equal(t0) || len(c[0].Rows) != 2 {
		t.Fatalf("closed = %+v", c)
	}
	m1, m2 := c[0].Rows[0], c[0].Rows[1]
	if m1.Machine != "m1" || m1.Count != 2 || m1.TempAvg != 25 || m1.TempMin != 20 || m1.TempMax != 30 || m1.ThroughputAvg != 150 {
		t.Fatalf("m1 = %+v", m1)
	}
	if m2.Count != 2 || m2.TempAvg != 60 || m2.TempMax != 70 {
		t.Fatalf("m2 = %+v", m2)
	}
	// Too late now: counted, not aggregated.
	a.Add(1, 3, "m2", t0.Add(3*time.Second), 99, 0)
	if a.Late != 1 {
		t.Fatalf("late = %d", a.Late)
	}
}

func TestIdlePartitionDoesNotHoldBucketsOpen(t *testing.T) {
	a := New(10*time.Second, 2*time.Second)
	a.Add(0, 0, "m1", t0.Add(1*time.Second), 1, 1)
	a.Add(1, 0, "m2", t0.Add(2*time.Second), 1, 1) // then partition 1 goes quiet
	if c := a.Add(0, 1, "m1", t0.Add(30*time.Second), 1, 1); len(c) != 0 {
		t.Fatalf("closed while partition 1 still counts: %+v", c)
	}
	c := a.MarkIdle(1)
	if len(c) != 1 || len(c[0].Rows) != 2 {
		t.Fatalf("closed after idle = %+v, want bucket 0 with m1 and m2", c)
	}
}

func TestResumeOffsetsPointAtOldestOpenBucket(t *testing.T) {
	a := New(10*time.Second, 2*time.Second)
	a.Add(0, 10, "m1", t0.Add(1*time.Second), 1, 1)
	a.Add(0, 11, "m1", t0.Add(12*time.Second), 1, 1) // closes nothing yet (watermark 10 s)
	a.Add(1, 40, "m2", t0.Add(13*time.Second), 1, 1) // closes bucket 0
	a.Add(1, 41, "m2", t0.Add(14*time.Second), 1, 1)
	got := a.ResumeOffsets()
	if got[0] != 11 || got[1] != 40 {
		t.Fatalf("resume offsets = %v, want p0:11 p1:40", got)
	}
}

func TestForgetDropsPartialAggregatesOfThePartition(t *testing.T) {
	a := New(10*time.Second, 0)
	a.Add(0, 0, "m1", t0.Add(1*time.Second), 1, 1)
	a.Add(1, 0, "m2", t0.Add(2*time.Second), 1, 1)
	a.Forget(1)
	c := a.Add(0, 1, "m1", t0.Add(10*time.Second), 1, 1)
	if len(c) != 1 || len(c[0].Rows) != 1 || c[0].Rows[0].Machine != "m1" {
		t.Fatalf("closed = %+v, want only m1 (m2 now belongs to another processor)", c)
	}
}

type record struct {
	p     int32
	off   int64
	m     string
	at    time.Time
	temp  float64
	speed float64
}

// stream builds per-partition logs of telemetry for a fleet, with event times
// slightly out of order (within the allowed lateness), like the real topic.
func stream(r *rand.Rand, machines, perMachine int) map[int32][]record {
	parts := map[int32][]record{}
	for i := 0; i < machines*perMachine; i++ {
		m := r.Intn(machines)
		p := int32(m % 3)
		at := t0.Add(time.Duration(i)*50*time.Millisecond - time.Duration(r.Intn(1500))*time.Millisecond)
		parts[p] = append(parts[p], record{p: p, off: int64(len(parts[p])), m: fmt.Sprintf("m%d", m), at: at,
			temp: float64(r.Intn(1000)) / 10, speed: float64(r.Intn(300))})
	}
	return parts
}

// db models the processor's sink: rows upserted by (bucket, machine), and per
// partition the stored resume offset and last written bucket, all updated in
// one transaction per closed bucket.
type db struct {
	late     int
	rows     map[string]Row
	offsets  map[int32]int64
	last     map[int32]time.Time
	hasFirst bool
}

func (d *db) write(c Closed, a *Aggregator) {
	for _, row := range c.Rows {
		d.rows[fmt.Sprint(row.Bucket.Unix(), row.Machine)] = row
	}
	for p, off := range a.ResumeOffsets() {
		d.offsets[p] = off
		d.last[p] = c.Bucket
	}
}

// run consumes the partitions round-robin from the stored offsets, and stops
// ("crashes") after `budget` records, losing everything not written.
func run(d *db, parts map[int32][]record, budget int) (done bool) {
	a := New(10*time.Second, 2*time.Second)
	for p, last := range d.last {
		a.Resume(p, last)
	}
	pos := map[int32]int64{}
	for p := range parts {
		pos[p] = d.offsets[p]
	}
	for budget > 0 {
		progressed := false
		for p := int32(0); p < 3; p++ {
			if pos[p] >= int64(len(parts[p])) {
				continue
			}
			rec := parts[p][pos[p]]
			pos[p]++
			progressed = true
			budget--
			for _, c := range a.Add(rec.p, rec.off, rec.m, rec.at, rec.temp, rec.speed) {
				d.write(c, a)
			}
			d.late += a.Late
			a.Late = 0
		}
		if !progressed {
			return true
		}
	}
	return false
}

// TestCrashAndReplayGiveTheSameRows is the property the design relies on:
// crashing anywhere and restarting from the stored offsets writes exactly the
// rows an uninterrupted run writes (no bucket lost, none written partially).
func TestCrashAndReplayGiveTheSameRows(t *testing.T) {
	total := 0
	defer func() { t.Logf("%d crashes and restarts over 50 seeds", total) }()
	for seed := int64(1); seed <= 50; seed++ {
		r := rand.New(rand.NewSource(seed))
		parts := stream(r, 8, 800)

		ref := &db{rows: map[string]Row{}, offsets: map[int32]int64{}, last: map[int32]time.Time{}}
		run(ref, parts, 1<<30)

		got := &db{rows: map[string]Row{}, offsets: map[int32]int64{}, last: map[int32]time.Time{}}
		// A restart resumes at the oldest open bucket, so a process must
		// outlive that many records between crashes to make progress.
		crashes := 0
		for ; !run(got, parts, 400+r.Intn(800)); crashes++ {
			if crashes > 10000 {
				t.Fatal("no progress across restarts")
			}
		}
		if crashes == 0 {
			t.Fatalf("seed %d: never crashed, the test proves nothing", seed)
		}
		total += crashes
		if got.late != 0 || ref.late != 0 {
			t.Fatalf("seed %d: %d/%d late records, the lateness is too small for this test", seed, got.late, ref.late)
		}
		if len(got.rows) != len(ref.rows) {
			t.Fatalf("seed %d: %d rows after crashes, %d without", seed, len(got.rows), len(ref.rows))
		}
		for k, want := range ref.rows {
			if g := got.rows[k]; g != want {
				t.Fatalf("seed %d: row %s = %+v after crashes, want %+v", seed, k, g, want)
			}
		}
	}
}
