// Package window aggregates machine telemetry into fixed, aligned time
// buckets (tumbling windows) by event time, with a watermark that tolerates
// some reordering. It is pure: no Kafka, no database, no clock.
//
// The event time is the gateway's ingest time (one clock for the whole
// fleet). Each partition's watermark is the highest event time seen in it
// minus the allowed lateness, and a bucket closes once the lowest of them
// passes its end: a partition that is read faster than another (fewer
// machines, or consumed first after a restart) cannot close buckets the slower
// one is still filling. A record for a closed bucket is late and counted, not
// aggregated. A partition the caller assigned but has not read from yet holds
// the watermark too: when catching up, Kafka often returns a long run of one
// partition first, and closing buckets on it alone would drop the others'
// records as late. A partition with nothing left to read is marked idle by
// the caller so that it does not hold every bucket open.
//
// Buckets are aligned on multiples of the width, so every machine's bucket
// [t, t+width) closes at the same moment: the processor writes one bucket for
// the whole fleet in one transaction, together with its Kafka offsets.
package window

import (
	"math"
	"sort"
	"time"
)

// Row is one machine's aggregate over one bucket.
type Row struct {
	Machine       string
	Bucket        time.Time // start of the bucket
	Count         int
	TempAvg       float64
	TempMin       float64
	TempMax       float64
	ThroughputAvg float64
}

type acc struct {
	count               int
	tempSum, tempMin    float64
	tempMax, throughSum float64
}

// Closed is a bucket that just closed, for the whole fleet.
type Closed struct {
	Bucket time.Time
	Rows   []Row // sorted by machine
}

type Aggregator struct {
	width    time.Duration
	lateness time.Duration
	maxSeen  map[int32]time.Time // per active partition; zero = assigned, nothing read yet
	closedTo time.Time           // buckets starting before this are closed
	open     map[time.Time]map[string]*acc
	// firstOffset[bucket][partition]: offset of the first record of that open
	// bucket in that partition, to compute where a restart must resume.
	firstOffset map[time.Time]map[int32]int64
	nextOffset  map[int32]int64 // one past the last record seen, per partition
	// skipBefore[p]: buckets starting before this were already written for
	// partition p (by this process before a restart, or by a previous owner).
	skipBefore  map[int32]time.Time
	machinePart map[string]int32
	Late        int // records for a bucket this aggregator had already closed
	Replayed    int // records for a bucket already written before a restart
}

func New(width, lateness time.Duration) *Aggregator {
	return &Aggregator{width: width, lateness: lateness,
		open: map[time.Time]map[string]*acc{}, firstOffset: map[time.Time]map[int32]int64{}, nextOffset: map[int32]int64{},
		skipBefore: map[int32]time.Time{}, machinePart: map[string]int32{}, maxSeen: map[int32]time.Time{}}
}

// Assign registers a partition this aggregator is about to consume: until a
// record arrives from it (or it is marked idle), no bucket closes.
func (a *Aggregator) Assign(partition int32) {
	if _, ok := a.maxSeen[partition]; !ok {
		a.maxSeen[partition] = time.Time{}
	}
}

// Resume tells the aggregator that, for this partition, buckets up to and
// including last were already written. Their records are skipped on replay.
func (a *Aggregator) Resume(partition int32, last time.Time) {
	a.skipBefore[partition] = last.Add(a.width)
}

// Add feeds one telemetry record and returns the buckets it closed.
func (a *Aggregator) Add(partition int32, offset int64, machine string, at time.Time, temp, throughput float64) []Closed {
	a.nextOffset[partition] = offset + 1
	a.machinePart[machine] = partition
	b := at.Truncate(a.width)
	switch {
	case b.Before(a.skipBefore[partition]):
		a.Replayed++
	case b.Before(a.closedTo):
		a.Late++
	default:
		m := a.open[b]
		if m == nil {
			m = map[string]*acc{}
			a.open[b] = m
			a.firstOffset[b] = map[int32]int64{}
		}
		if _, ok := a.firstOffset[b][partition]; !ok {
			a.firstOffset[b][partition] = offset
		}
		x := m[machine]
		if x == nil {
			x = &acc{tempMin: math.Inf(1), tempMax: math.Inf(-1)}
			m[machine] = x
		}
		x.count++
		x.tempSum += temp
		x.tempMin = math.Min(x.tempMin, temp)
		x.tempMax = math.Max(x.tempMax, temp)
		x.throughSum += throughput
	}
	if at.After(a.maxSeen[partition]) {
		a.maxSeen[partition] = at
	}
	return a.closeUpTo(a.watermark())
}

// Seen records that an offset was consumed without aggregating it (another
// kind of message, or a malformed one), so that restarts do not reread it.
func (a *Aggregator) Seen(partition int32, offset int64) {
	a.nextOffset[partition] = offset + 1
}

// MarkIdle excludes a partition from the watermark until its next record
// (the caller decides, from wall-clock time, that it has nothing to read).
func (a *Aggregator) MarkIdle(partition int32) []Closed {
	delete(a.maxSeen, partition)
	return a.closeUpTo(a.watermark())
}

func (a *Aggregator) watermark() time.Time {
	var low time.Time
	first := true
	for _, t := range a.maxSeen {
		if first || t.Before(low) {
			low, first = t, false
		}
	}
	if first || low.IsZero() {
		return time.Time{} // nothing active, or a partition not read yet: close nothing
	}
	return low.Add(-a.lateness)
}

// closeUpTo closes every open bucket that ends at or before the watermark.
func (a *Aggregator) closeUpTo(watermark time.Time) []Closed {
	var out []Closed
	for _, b := range a.openBuckets() {
		if b.Add(a.width).After(watermark) {
			break
		}
		out = append(out, Closed{Bucket: b, Rows: rows(b, a.open[b])})
		delete(a.open, b)
		delete(a.firstOffset, b)
		a.closedTo = b.Add(a.width)
	}
	return out
}

func (a *Aggregator) openBuckets() []time.Time {
	bs := make([]time.Time, 0, len(a.open))
	for b := range a.open {
		bs = append(bs, b)
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].Before(bs[j]) })
	return bs
}

// ResumeOffsets returns, per partition seen, the offset a restarted processor
// must consume from so that every still-open bucket is rebuilt entirely:
// the first record of the oldest open bucket, or past the last record seen.
func (a *Aggregator) ResumeOffsets() map[int32]int64 {
	out := map[int32]int64{}
	for p, next := range a.nextOffset {
		out[p] = next
	}
	for _, firsts := range a.firstOffset {
		for p, off := range firsts {
			if off < out[p] {
				out[p] = off
			}
		}
	}
	return out
}

// Forget drops everything about a partition that was reassigned to another
// processor: that one rebuilds the open buckets from the stored offsets, so
// writing our partial aggregates for its machines would be wrong.
func (a *Aggregator) Forget(partition int32) {
	delete(a.nextOffset, partition)
	delete(a.maxSeen, partition)
	delete(a.skipBefore, partition)
	for _, firsts := range a.firstOffset {
		delete(firsts, partition)
	}
	for machine, p := range a.machinePart {
		if p != partition {
			continue
		}
		for _, m := range a.open {
			delete(m, machine)
		}
		delete(a.machinePart, machine)
	}
}

func rows(b time.Time, m map[string]*acc) []Row {
	out := make([]Row, 0, len(m))
	for id, x := range m {
		n := float64(x.count)
		out = append(out, Row{Machine: id, Bucket: b, Count: x.count, TempAvg: x.tempSum / n,
			TempMin: x.tempMin, TempMax: x.tempMax, ThroughputAvg: x.throughSum / n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Machine < out[j].Machine })
	return out
}
