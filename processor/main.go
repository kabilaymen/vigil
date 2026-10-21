// Command processor turns the telemetry stream into 10-second aggregates per
// machine (message count, temperature avg/min/max, average throughput) in a
// TimescaleDB hypertable that the Grafana fleet dashboard reads.
//
// Exactly-once effect in the database: the Kafka offsets to resume from are
// stored in the same transaction as the aggregates, and on start or partition
// assignment the consumer seeks to them. A crash anywhere replays from the
// oldest bucket that was still open, so a bucket is always written whole
// (window.TestCrashAndReplayGiveTheSameRows checks this property).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kabilaymen/vigil/processor/window"
)

var (
	rowsWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_processor_rows_written_total",
		Help: "Machine aggregates written to TimescaleDB.",
	})
	bucketsWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_processor_buckets_written_total",
		Help: "10-second buckets closed and written.",
	})
	lateRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_processor_late_records_total",
		Help: "Telemetry records that arrived after their bucket was closed (dropped).",
	})
	replayedRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_processor_replayed_records_total",
		Help: "Records skipped after a restart because their bucket was already written.",
	})
	malformedRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_processor_malformed_total",
		Help: "Telemetry records whose payload could not be parsed.",
	})
	writeSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vigil_processor_write_seconds",
		Help:    "Duration of one transaction (closed buckets + offsets).",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	})
	consumeLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vigil_processor_consume_lag_seconds",
		Help: "Age of the last consumed record (gateway ingest time → now).",
	})
	lastBucket = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vigil_processor_last_bucket_timestamp_seconds",
		Help: "Start of the most recent bucket written (data freshness).",
	})
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// ingestTime is the gateway's receipt time: one clock for the whole fleet.
func ingestTime(r *kgo.Record) time.Time {
	if ms, err := strconv.ParseInt(header(r, "ingest_ts_ms"), 10, 64); err == nil {
		return time.UnixMilli(ms)
	}
	return r.Timestamp
}

type telemetry struct {
	Params struct {
		Temperature float64 `json:"temperature_c"`
		Throughput  float64 `json:"throughput_ppm"`
	} `json:"params"`
}

type processor struct {
	mu       sync.Mutex
	agg      *window.Aggregator
	lastRead map[int32]time.Time // wall time of the last record per partition
	next     map[int32]int64     // next offset to read, per partition
	high     map[int32]int64     // high watermark (end of the partition) from the last fetch
	idle     time.Duration
	st       *store
	log      *slog.Logger
}

// assigned seeks newly assigned partitions to the offsets stored with the
// aggregates. Partitions never processed start from the oldest retained data.
func (p *processor) assigned(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
	for topic, parts := range assigned {
		prog, err := p.st.progress(ctx, parts)
		if err != nil {
			p.log.Error("load progress", "err", err)
			os.Exit(1) // restarting is safe; guessing an offset is not
		}
		offsets := map[int32]kgo.EpochOffset{}
		p.mu.Lock()
		for _, part := range parts {
			// Hold every bucket open until this partition is read (or found empty).
			p.agg.Assign(part)
			p.lastRead[part] = time.Now()
		}
		for part, pr := range prog {
			offsets[part] = kgo.EpochOffset{Epoch: -1, Offset: pr.nextOffset}
			p.agg.Resume(part, pr.lastBucket)
		}
		p.mu.Unlock()
		if len(offsets) > 0 {
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: offsets})
		}
		p.log.Info("partitions assigned", "topic", topic, "partitions", parts, "resumed", len(offsets))
	}
}

func (p *processor) revoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, parts := range revoked {
		for _, part := range parts {
			p.agg.Forget(part)
			delete(p.lastRead, part)
			delete(p.next, part)
			delete(p.high, part)
		}
	}
}

// consume aggregates one batch of records and returns the buckets it closed.
func (p *processor) consume(fetches kgo.Fetches) ([]window.Closed, map[int32]int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var closed []window.Closed
	late, replayed := p.agg.Late, p.agg.Replayed
	fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
		p.high[fp.Partition] = fp.HighWatermark
	})
	fetches.EachRecord(func(r *kgo.Record) {
		p.lastRead[r.Partition] = time.Now()
		p.next[r.Partition] = r.Offset + 1
		at := ingestTime(r)
		consumeLag.Set(time.Since(at).Seconds())
		if header(r, "kind") != "telemetry" {
			p.agg.Seen(r.Partition, r.Offset)
			return
		}
		var t telemetry
		if json.Unmarshal(r.Value, &t) != nil {
			malformedRecords.Inc()
			p.agg.Seen(r.Partition, r.Offset)
			return
		}
		closed = append(closed, p.agg.Add(r.Partition, r.Offset, string(r.Key), at, t.Params.Temperature, t.Params.Throughput)...)
	})
	// A partition with nothing to read (all its machines down) must not hold
	// every bucket open. "Nothing to read" means caught up to its end, not
	// just quiet for a moment: while catching up, Kafka may serve one
	// partition for a while before another. Without any fetch information, a
	// longer silence is required.
	for part, at := range p.lastRead {
		high, known := p.high[part]
		caughtUp := known && p.next[part] >= high
		if quiet := time.Since(at); (caughtUp && quiet > p.idle) || quiet > 6*p.idle {
			closed = append(closed, p.agg.MarkIdle(part)...)
		}
	}
	lateRecords.Add(float64(p.agg.Late - late))
	replayedRecords.Add(float64(p.agg.Replayed - replayed))
	return closed, p.agg.ResumeOffsets()
}

func connect(ctx context.Context, dsn string, log *slog.Logger) *pgxpool.Pool {
	for {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool
			}
			pool.Close()
		}
		log.Warn("database not ready", "err", err)
		select {
		case <-ctx.Done():
			os.Exit(1)
		case <-time.After(2 * time.Second):
		}
	}
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	topic := env("KAFKA_TOPIC", "telemetry")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool := connect(ctx, env("DATABASE_URL", "postgres://vigil:vigil@localhost:5432/vigil"), log)
	defer pool.Close()
	st := &store{pool: pool, topic: topic}
	if err := st.migrate(ctx); err != nil {
		log.Error("schema", "err", err)
		os.Exit(1)
	}

	p := &processor{
		agg:      window.New(10*time.Second, 2*time.Second),
		lastRead: map[int32]time.Time{},
		next:     map[int32]int64{},
		high:     map[int32]int64{},
		idle:     5 * time.Second,
		st:       st,
		log:      log,
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "localhost:29092"), ",")...),
		kgo.ConsumerGroup(env("CONSUMER_GROUP", "vigil-processor")),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), // offsets live in the database, with the rows
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(), // rebalances only between two batches
		kgo.OnPartitionsAssigned(p.assigned),
		kgo.OnPartitionsRevoked(p.revoked),
		kgo.OnPartitionsLost(p.revoked),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: env("METRICS_ADDR", ":9102"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()

	log.Info("processor started", "topic", topic)
	for ctx.Err() == nil {
		pollCtx, done := context.WithTimeout(ctx, time.Second) // wake up to detect idle partitions
		fetches := cl.PollFetches(pollCtx)
		done()
		fetches.EachError(func(t string, part int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Warn("fetch", "topic", t, "partition", part, "err", err)
			}
		})
		closed, resume := p.consume(fetches)
		if len(closed) > 0 {
			start := time.Now()
			rows, err := st.write(ctx, closed, resume)
			if err != nil {
				// Nothing of this batch is stored: restart from the stored offsets.
				log.Error("write", "err", err)
				os.Exit(1)
			}
			writeSeconds.Observe(time.Since(start).Seconds())
			rowsWritten.Add(float64(rows))
			bucketsWritten.Add(float64(len(closed)))
			lastBucket.Set(float64(closed[len(closed)-1].Bucket.Unix()))
		}
		cl.AllowRebalance()
	}

	log.Info("shutting down")
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	srv.Shutdown(shutdown)
}
