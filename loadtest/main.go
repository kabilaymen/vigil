// Command loadtest drives the ingestion path (MQTT broker → gateway → Kafka)
// with a growing simulated fleet and measures what comes out of Kafka:
// delivered rate, loss and end-to-end latency (device timestamp → record read
// from Kafka; publisher and reader run in this process, so on one clock).
//
//	go run ./loadtest -steps 250,500,1000,2000 -rate 2 -duration 30s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/twmb/franz-go/pkg/kgo"
)

type result struct {
	Machines  int     `json:"machines"`
	Offered   float64 `json:"offered_msgs_per_s"`
	Delivered float64 `json:"delivered_msgs_per_s"`
	LossPct   float64 `json:"loss_pct"`
	P50ms     float64 `json:"e2e_p50_ms"`
	P99ms     float64 `json:"e2e_p99_ms"`
	P999ms    float64 `json:"e2e_p999_ms"`
	MaxMs     float64 `json:"e2e_max_ms"`
}

// reader consumes the telemetry topic and keeps the latency of every record
// published during the current measurement window.
type reader struct {
	mu        sync.Mutex
	run       string // only count records of the current step
	window    [2]int64
	latencies []time.Duration
	count     int
}

func (r *reader) start(run string, from, to time.Time) {
	r.mu.Lock()
	r.run, r.window = run, [2]int64{from.UnixMilli(), to.UnixMilli()}
	r.latencies, r.count = r.latencies[:0], 0
	r.mu.Unlock()
}

func (r *reader) take() ([]time.Duration, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.latencies), r.count
}

func (r *reader) loop(ctx context.Context, cl *kgo.Client) {
	for ctx.Err() == nil {
		f := cl.PollFetches(ctx)
		now := time.Now()
		f.EachRecord(func(rec *kgo.Record) {
			var m struct {
				Run string `json:"run"`
				TS  int64  `json:"ts"`
			}
			if json.Unmarshal(rec.Value, &m) != nil {
				return
			}
			r.mu.Lock()
			if m.Run == r.run && m.TS >= r.window[0] && m.TS < r.window[1] {
				r.count++
				r.latencies = append(r.latencies, now.Sub(time.UnixMilli(m.TS)))
			}
			r.mu.Unlock()
		})
	}
}

func q(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return float64(sorted[min(len(sorted)-1, int(p*float64(len(sorted))))].Microseconds()) / 1000
}

// step connects n machines, lets them publish for warmup+duration, and counts
// what reached Kafka among the messages sent during the measured window.
func step(ctx context.Context, broker string, n int, rate float64, warmup, duration, drain time.Duration, rd *reader, log *slog.Logger) result {
	run := fmt.Sprintf("n%d-%d", n, time.Now().UnixNano())
	clients := make([]mqtt.Client, n)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64) // connect in parallel, but not all at once
	for i := range clients {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			o := mqtt.NewClientOptions().AddBroker(broker).SetClientID(fmt.Sprintf("lt-%s-%05d", run, i)).
				SetAutoReconnect(false).SetConnectTimeout(10 * time.Second)
			c := mqtt.NewClient(o)
			if t := c.Connect(); t.Wait() && t.Error() != nil {
				log.Warn("connect", "err", t.Error())
			}
			clients[i] = c
		}(i)
	}
	wg.Wait()

	start := time.Now().Add(warmup)
	end := start.Add(duration)
	rd.start(run, start, end)
	var sent atomic.Int64
	stepCtx, cancel := context.WithDeadline(ctx, end)
	defer cancel()
	interval := time.Duration(float64(time.Second) / rate)
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c mqtt.Client) {
			defer wg.Done()
			id := fmt.Sprintf("lt-%05d", i)
			topic := "vigil/machines/" + id + "/telemetry"
			// Spread the machines over the interval instead of a thundering herd.
			time.Sleep(time.Duration(i) * interval / time.Duration(n))
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for seq := 0; ; seq++ {
				select {
				case <-stepCtx.Done():
					return
				case now := <-tick.C:
					payload := `{"machine_id":"` + id + `","kind":"telemetry","run":"` + run + `","seq":` + strconv.Itoa(seq) +
						`,"ts":` + strconv.FormatInt(now.UnixMilli(), 10) + `,"params":{"temperature_c":60,"throughput_ppm":120}}`
					c.Publish(topic, 0, false, payload)
					if !now.Before(start) {
						sent.Add(1)
					}
				}
			}
		}(i, c)
	}
	wg.Wait()
	time.Sleep(drain) // let the last messages through the pipeline
	lat, got := rd.take()
	for _, c := range clients {
		c.Disconnect(0)
	}
	slices.Sort(lat)
	s := float64(sent.Load())
	res := result{Machines: n, Offered: s / duration.Seconds(), Delivered: float64(got) / duration.Seconds(),
		P50ms: q(lat, 0.5), P99ms: q(lat, 0.99), P999ms: q(lat, 0.999)}
	if len(lat) > 0 {
		res.MaxMs = float64(lat[len(lat)-1].Microseconds()) / 1000
	}
	if s > 0 {
		res.LossPct = 100 * max(0, s-float64(got)) / s
	}
	log.Info("step", "machines", n, "offered", int(res.Offered), "delivered", int(res.Delivered), "loss_pct", res.LossPct, "p99_ms", res.P99ms)
	return res
}

func main() {
	broker := flag.String("mqtt", "tcp://localhost:1883", "MQTT broker")
	brokers := flag.String("kafka", "localhost:29092", "Kafka bootstrap servers")
	steps := flag.String("steps", "250,500,1000,2000", "fleet sizes to test, in order")
	rate := flag.Float64("rate", 2, "messages per second per machine")
	warmup := flag.Duration("warmup", 5*time.Second, "publishing time before measuring")
	duration := flag.Duration("duration", 30*time.Second, "measured publishing time per step")
	drain := flag.Duration("drain", 5*time.Second, "wait after publishing stops")
	out := flag.String("json", "", "write results as JSON to this file")
	env := flag.String("env", "", "environment description for the report")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics("telemetry"), kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		log.Error("kafka", "err", err)
		os.Exit(1)
	}
	defer cl.Close()
	rd := &reader{}
	go rd.loop(ctx, cl)
	time.Sleep(3 * time.Second) // let the reader join before the first step

	var results []result
	for _, f := range strings.Split(*steps, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			log.Error("bad step", "value", f)
			os.Exit(2)
		}
		results = append(results, step(ctx, *broker, n, *rate, *warmup, *duration, *drain, rd, log))
	}

	report(os.Stdout, results, *env, *rate, *duration)
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		// Annotations are public on a public repository (logs need a login).
		fmt.Printf("::notice title=Load test environment::%s\n", *env)
		for _, r := range results {
			fmt.Printf("::notice title=Load test, %d machines::offered %.0f msg/s, delivered %.0f msg/s, loss %.2f%%, e2e p50 %.1f ms, p99 %.1f ms, p99.9 %.1f ms, max %.0f ms\n",
				r.Machines, r.Offered, r.Delivered, r.LossPct, r.P50ms, r.P99ms, r.P999ms, r.MaxMs)
		}
	}
	if p := os.Getenv("GITHUB_STEP_SUMMARY"); p != "" {
		if f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644); err == nil {
			report(f, results, *env, *rate, *duration)
			f.Close()
		}
	}
	if *out != "" {
		b, _ := json.MarshalIndent(map[string]any{"env": *env, "rate_per_machine": *rate, "duration_s": duration.Seconds(), "results": results}, "", "  ")
		os.WriteFile(*out, b, 0o644)
	}
}

func report(w io.Writer, rs []result, env string, rate float64, d time.Duration) {
	fmt.Fprintf(w, "## Ingestion load test\n\n%s — %.0f msg/s per machine, %s measured per step, QoS 0\n\n", env, rate, d)
	fmt.Fprintln(w, "| Machines | Offered msg/s | Delivered msg/s | Loss | e2e p50 | e2e p99 | e2e p99.9 | max |")
	fmt.Fprintln(w, "|---:|---:|---:|---:|---:|---:|---:|---:|")
	for _, r := range rs {
		fmt.Fprintf(w, "| %d | %.0f | %.0f | %.2f %% | %.1f ms | %.1f ms | %.1f ms | %.0f ms |\n",
			r.Machines, r.Offered, r.Delivered, r.LossPct, r.P50ms, r.P99ms, r.P999ms, r.MaxMs)
	}
	fmt.Fprintln(w)
}
