// Command gateway bridges machine telemetry from MQTT into Kafka.
//
// Every message received on vigil/machines/<id>/<kind> is produced to a Kafka
// topic, keyed by machine id so that all events of one machine land in the same
// partition (per-machine ordering is preserved downstream).
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
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const subscription = "vigil/machines/+/+"

var (
	received = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_gateway_messages_received_total",
		Help: "MQTT messages received, by kind.",
	}, []string{"kind"})
	produced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_gateway_messages_produced_total",
		Help: "Messages acknowledged by Kafka.",
	})
	produceErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_gateway_produce_errors_total",
		Help: "Messages Kafka failed to acknowledge.",
	})
	malformed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_gateway_malformed_total",
		Help: "Messages dropped because the topic or payload was invalid.",
	})
	produceLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vigil_gateway_produce_latency_seconds",
		Help:    "Time from MQTT receipt to Kafka acknowledgement.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})
	deviceToAck = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vigil_gateway_device_to_kafka_seconds",
		Help:    "Time from the device timestamp to Kafka acknowledgement (includes clock skew).",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
)

type config struct {
	mqttURL     string
	brokers     []string
	topic       string
	metricsAddr string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() config {
	return config{
		mqttURL:     env("MQTT_URL", "tcp://localhost:1883"),
		brokers:     strings.Split(env("KAFKA_BROKERS", "localhost:29092"), ","),
		topic:       env("KAFKA_TOPIC", "telemetry"),
		metricsAddr: env("METRICS_ADDR", ":9100"),
	}
}

// parseTopic extracts machine id and message kind from vigil/machines/<id>/<kind>.
func parseTopic(topic string) (machineID, kind string, err error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "vigil" || parts[1] != "machines" || parts[2] == "" || parts[3] == "" {
		return "", "", errors.New("unexpected topic " + topic)
	}
	return parts[2], parts[3], nil
}

// deviceTimestamp returns the "ts" field (unix milliseconds) of a payload, if any.
func deviceTimestamp(payload []byte) (time.Time, bool) {
	var m struct {
		TS int64 `json:"ts"`
	}
	if err := json.Unmarshal(payload, &m); err != nil || m.TS == 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(m.TS), true
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()

	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.DefaultProduceTopic(cfg.topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.MaxBufferedRecords(100_000), // Produce blocks beyond this: backpressure towards MQTT
	)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer kafka.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	onMessage := func(_ mqtt.Client, msg mqtt.Message) {
		receivedAt := time.Now()
		machineID, kind, err := parseTopic(msg.Topic())
		if err != nil {
			malformed.Inc()
			return
		}
		received.WithLabelValues(kind).Inc()
		payload := msg.Payload()
		rec := &kgo.Record{
			Key:   []byte(machineID),
			Value: payload,
			Headers: []kgo.RecordHeader{
				{Key: "kind", Value: []byte(kind)},
				{Key: "ingest_ts_ms", Value: []byte(strconv.FormatInt(receivedAt.UnixMilli(), 10))},
			},
		}
		kafka.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				produceErrors.Inc()
				return
			}
			produced.Inc()
			produceLatency.Observe(time.Since(receivedAt).Seconds())
			if ts, ok := deviceTimestamp(payload); ok {
				deviceToAck.Observe(time.Since(ts).Seconds())
			}
		})
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.mqttURL).
		SetClientID("vigil-gateway").
		SetCleanSession(false). // keep the QoS1 subscription across gateway restarts
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetOrderMatters(false).
		SetOnConnectHandler(func(c mqtt.Client) {
			if t := c.Subscribe(subscription, 1, onMessage); t.Wait() && t.Error() != nil {
				log.Error("subscribe", "err", t.Error())
				return
			}
			log.Info("subscribed", "topic", subscription)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Warn("mqtt connection lost", "err", err)
		})
	client := mqtt.NewClient(opts)
	client.Connect()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !client.IsConnectionOpen() {
			http.Error(w, "mqtt disconnected", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: cfg.metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	log.Info("gateway started", "mqtt", cfg.mqttURL, "kafka", cfg.brokers, "topic", cfg.topic)

	<-ctx.Done()
	log.Info("shutting down")
	client.Disconnect(500)
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	if err := kafka.Flush(flushCtx); err != nil {
		log.Warn("flush", "err", err)
	}
	srv.Shutdown(flushCtx)
}
