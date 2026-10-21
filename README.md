# Vigil

Real-time telemetry and failure detection for fleets of production machines (or compute jobs).

Vigil ingests machine telemetry over MQTT, streams it through Kafka, detects dead and stalled
machines in about a second, and keeps a 10-second history per machine in TimescaleDB — with every
stage instrumented.

> Status: ingestion, failure detector and stream processor working end to end; deployed on a small
> cloud VM with a simulated fleet of 50 machines: **[live dashboards](https://vigil.aymenkabil.me)**.
> Next: load-test report, Slurm job-monitoring mode.

How it was built, the bugs found on the way and how they were fixed: [`JOURNAL.md`](JOURNAL.md).

## Architecture

```
 ┌────────────┐  MQTT (QoS0/1,     ┌───────────┐       ┌──────────┐   Kafka    ┌──────────────┐
 │ simulator  │  Last Will per  ──▶│ mosquitto │──────▶│ gateway  │──────────▶ │ topic        │
 │ N machines │  machine)          └───────────┘       │  (Go)    │  key =     │ "telemetry"  │
 └────────────┘                                        └────┬─────┘  machine   │ 6 partitions │
                                                            │ /metrics         └──────────────┘
                                                      ┌─────▼──────┐   ┌─────────┐       │
                                                      │ Prometheus │──▶│ Grafana │       ▼
                                                      └─────▲──────┘   └─────────┘ ┌──────────────┐  topic
                                                            └──────────────────────│  detector    │─▶ "alerts"
                                                                                   │  (Go, φ)     │
                                                                                   └──────────────┘
```

- **simulator** (Python) — N machines (embossers, laser personalizers, chip encoders), each with its
  own MQTT connection and **Last Will**: when a machine crashes (TCP dropped without DISCONNECT),
  the broker itself publishes `offline` on its status topic. Crashes and recoveries are injected
  on a schedule.
- **gateway** (Go) — subscribes to `vigil/machines/+/+`, produces each message to Kafka **keyed by
  machine id** (per-machine ordering is preserved inside a partition). Backpressure: when Kafka's
  producer buffer is full, `Produce` blocks and slows MQTT consumption instead of dropping data.
  Exposes ingest rate, produce latency histogram (MQTT receipt → Kafka ack), errors.

- **processor** (Go) — consumes `telemetry` and writes, per machine and per 10-second bucket, the
  message count, temperature avg/min/max and average throughput to a TimescaleDB hypertable
  (3-day retention). Buckets are closed by a watermark (lowest partition event time minus 2 s),
  so a partition read faster than another cannot close buckets the slower one is still filling.
  The Kafka offsets to resume from are written in the same transaction as the rows, so a crash
  replays from the oldest open bucket and never leaves a bucket half written; a randomized test
  crashes the processor hundreds of times and checks the rows are identical to an uninterrupted
  run.
- **detector** (Go) — consumes `telemetry` and combines two failure signals per machine:
  - **connection-level**: the broker's Last Will → `connection_lost` immediately;
  - **application-level**: a **phi accrual failure detector** (Hayashibara et al., 2004) over the
    heartbeat inter-arrival times of each machine → `suspected` when φ ≥ 8. This catches machines
    that are *connected but stalled*: MQTT keepalives are sent by the client's network thread and
    keep the session alive even when the application is stuck, so the Last Will never fires.

  Design choices: heartbeats are timestamped by the gateway (one clock for the whole fleet);
  machines are not judged during warm-up; the heartbeat history is reset on reconnection (the
  outage gap would otherwise inflate the variance); evaluation is suspended while the detector
  itself lags behind the stream, so a slow detector never declares the fleet dead.

### Observed (local, 50 machines at 2 msg/s, threshold φ = 8)

| Failure | Signal | Detected after |
|---|---|---|
| Machine stalled, still connected | φ accrual | ~1.2 s of silence (nominal interval 0.5 s) |
| Machine crashed (TCP dropped) | Last Will | ~1 s |

### Topics

| MQTT topic | Payload | QoS |
|---|---|---|
| `vigil/machines/<id>/telemetry` | `{seq, ts, params: {temperature_c, throughput_ppm}}` | 0 |
| `vigil/machines/<id>/alarm` | `{ts, code, severity}` | 1 |
| `vigil/machines/<id>/status` | `{state: online\|offline, ts}` (retained, Last Will) | 1 |

## Run locally

Requirements: Docker.

```bash
docker compose up -d --build
```

| Service | URL |
|---|---|
| Grafana (dashboards "Vigil — Gateway", "Vigil — Failure detector", "Vigil — Fleet history") | http://localhost:3000 |
| TimescaleDB (user/password `vigil`) | localhost:5432 |
| Processor metrics | http://localhost:9102/metrics |
| Detector metrics | http://localhost:9101/metrics |
| Prometheus | http://localhost:9090 |
| Gateway metrics | http://localhost:9100/metrics |
| Kafka (from host) | localhost:29092 |
| MQTT | localhost:1883 |

Watch the stream:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic telemetry
```

Alerts:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic alerts
```

Tune the fleet in `docker-compose.yml` (`--machines`, `--rate`, `--crash-every`, `--stall-every`).

## Development

```bash
(cd gateway && go test ./...)
(cd detector && go test ./...)
(cd processor && go test ./...)   # TEST_DATABASE_URL=postgres://vigil:vigil@localhost:5432/vigil also runs the SQL test
```

## Roadmap

- [x] Simulator with Last-Will-based crash injection
- [x] MQTT → Kafka gateway with backpressure and Prometheus metrics
- [x] Stream processor: windowed aggregates into TimescaleDB, fleet dashboard
- [x] Failure detector: phi accrual + Last Will, alerts topic, detection-time histogram
- [x] Stall scenario (connected but silent machine)
- [ ] Load-test harness and benchmark report (throughput, p99, detection time vs. load)
- [ ] Slurm job-monitoring mode
- [x] CI/CD: images built by GitHub Actions, pulled by the VM; public read-only dashboards
- [ ] k3s
