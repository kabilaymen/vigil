// Command detector consumes the telemetry stream from Kafka, tracks every
// machine with a phi accrual failure detector plus the broker's Last Will, and
// publishes alerts to the "alerts" topic.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kabilaymen/vigil/detector/tracker"
)

var (
	alertsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_detector_alerts_total",
		Help: "Alerts emitted, by type.",
	}, []string{"type"})
	machinesDown = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vigil_detector_machines_down",
		Help: "Machines currently suspected or disconnected.",
	})
	machinesTracked = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vigil_detector_machines_tracked",
		Help: "Machines known to the detector.",
	})
	phiGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vigil_detector_phi",
		Help: "Current suspicion level per machine.",
	}, []string{"machine"})
	detectionSilence = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vigil_detector_detection_seconds",
		Help:    "Silence since the last heartbeat when a failure was declared.",
		Buckets: []float64{0.25, 0.5, 1, 1.5, 2, 3, 4, 5, 7.5, 10, 15, 30},
	}, []string{"type"})
	consumeLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vigil_detector_consume_lag_seconds",
		Help: "Age of the last consumed record (gateway ingest time → now).",
	})
	suppressed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_detector_evaluations_suppressed_total",
		Help: "Evaluation rounds skipped because the detector itself was lagging.",
	})
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
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

// ingestTime is the gateway's receipt time: a single clock for all machines,
// which avoids depending on each device's (possibly skewed) clock.
func ingestTime(r *kgo.Record) time.Time {
	if ms, err := strconv.ParseInt(header(r, "ingest_ts_ms"), 10, 64); err == nil {
		return time.UnixMilli(ms)
	}
	return r.Timestamp
}

type detector struct {
	mu     sync.Mutex
	tr     *tracker.Tracker
	lag    time.Duration
	maxLag time.Duration
	out    *kgo.Client
	topic  string
	log    *slog.Logger
}

func (d *detector) handle(r *kgo.Record) {
	id := string(r.Key)
	at := ingestTime(r)
	d.mu.Lock()
	d.lag = time.Since(at)
	var alerts []tracker.Alert
	switch header(r, "kind") {
	case "telemetry":
		alerts = d.tr.Heartbeat(id, at)
	case "status":
		var s struct {
			State string `json:"state"`
		}
		if json.Unmarshal(r.Value, &s) == nil {
			if s.State == "offline" {
				alerts = d.tr.Offline(id, at)
			} else if s.State == "online" {
				alerts = d.tr.Online(id, at)
			}
		}
	}
	d.mu.Unlock()
	d.publish(alerts)
}

func (d *detector) evaluate(now time.Time) {
	d.mu.Lock()
	lag := d.lag
	consumeLag.Set(lag.Seconds())
	var alerts []tracker.Alert
	// If we are behind on the stream, silence is our fault, not the machines':
	// keep consuming but do not judge anyone until we have caught up.
	if lag > d.maxLag {
		suppressed.Inc()
	} else {
		alerts = d.tr.Evaluate(now)
	}
	phis, down := d.tr.Snapshot(now)
	d.mu.Unlock()

	machinesTracked.Set(float64(len(phis)))
	machinesDown.Set(float64(down))
	for id, p := range phis {
		phiGauge.WithLabelValues(id).Set(p)
	}
	d.publish(alerts)
}

func (d *detector) publish(alerts []tracker.Alert) {
	for _, a := range alerts {
		alertsTotal.WithLabelValues(string(a.Type)).Inc()
		if a.Type == tracker.Suspected || a.Type == tracker.ConnectionLost {
			detectionSilence.WithLabelValues(string(a.Type)).Observe(a.Silence.Seconds())
		}
		d.log.Info("alert", "machine", a.Machine, "type", a.Type, "phi", a.Phi, "silence", a.Silence)
		payload, _ := json.Marshal(a)
		d.out.Produce(context.Background(), &kgo.Record{Topic: d.topic, Key: []byte(a.Machine), Value: payload}, nil)
	}
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:29092"), ",")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(env("CONSUMER_GROUP", "vigil-detector")),
		kgo.ConsumeTopics(env("KAFKA_TOPIC", "telemetry")),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), // first start: only live data
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	d := &detector{
		tr: tracker.New(tracker.Config{
			Threshold:  envFloat("PHI_THRESHOLD", 8),
			WindowSize: 100,
			MinSamples: 10,
			MinStd:     100 * time.Millisecond,
		}),
		maxLag: 2 * time.Second,
		out:    client,
		topic:  env("ALERTS_TOPIC", "alerts"),
		log:    log,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: env("METRICS_ADDR", ":9101"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()

	go func() {
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				d.evaluate(now)
			}
		}
	}()

	log.Info("detector started", "brokers", brokers)
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Warn("fetch", "topic", t, "partition", p, "err", err)
			}
		})
		fetches.EachRecord(d.handle)
	}

	log.Info("shutting down")
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	client.Flush(shutdown)
	srv.Shutdown(shutdown)
}
