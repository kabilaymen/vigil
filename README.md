# Vigil

Real-time telemetry and failure detection for fleets of production machines (or compute jobs).

Vigil ingests telemetry from thousands of devices over MQTT, streams it through Kafka, and detects
disconnections and anomalies in seconds — with every stage instrumented and load-tested.

> Status: **step 1/5 — ingestion pipeline** (simulator → MQTT → gateway → Kafka, with Prometheus/Grafana).
> Next: stream processor + TimescaleDB, failure detector, load-test report, k3s deployment.

## Architecture

```
 ┌────────────┐  MQTT (QoS0/1,     ┌───────────┐       ┌──────────┐   Kafka    ┌──────────────┐
 │ simulator  │  Last Will per  ──▶│ mosquitto │──────▶│ gateway  │──────────▶ │ topic        │
 │ N machines │  machine)          └───────────┘       │  (Go)    │  key =     │ "telemetry"  │
 └────────────┘                                        └────┬─────┘  machine   │ 6 partitions │
                                                            │ /metrics         └──────────────┘
                                                      ┌─────▼──────┐   ┌─────────┐
                                                      │ Prometheus │──▶│ Grafana │
                                                      └────────────┘   └─────────┘
```

- **simulator** (Python) — N machines (embossers, laser personalizers, chip encoders), each with its
  own MQTT connection and **Last Will**: when a machine crashes (TCP dropped without DISCONNECT),
  the broker itself publishes `offline` on its status topic. Crashes and recoveries are injected
  on a schedule.
- **gateway** (Go) — subscribes to `vigil/machines/+/+`, produces each message to Kafka **keyed by
  machine id** (per-machine ordering is preserved inside a partition). Backpressure: when Kafka's
  producer buffer is full, `Produce` blocks and slows MQTT consumption instead of dropping data.
  Exposes ingest rate, produce latency histogram (MQTT receipt → Kafka ack), errors.

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
| Grafana (dashboard "Vigil — Gateway") | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Gateway metrics | http://localhost:9100/metrics |
| Kafka (from host) | localhost:29092 |
| MQTT | localhost:1883 |

Watch the stream:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic telemetry
```

Tune the fleet in `docker-compose.yml` (`--machines`, `--rate`, `--crash-every`).

## Development

```bash
cd gateway && go test ./...
```

## Roadmap

- [x] Simulator with Last-Will-based crash injection
- [x] MQTT → Kafka gateway with backpressure and Prometheus metrics
- [ ] Stream processor: windowed aggregates into TimescaleDB, fleet dashboards
- [ ] Failure detector: heartbeat (phi-accrual) + Last Will, alerting, detection-time measurement
- [ ] Load-test harness and benchmark report (throughput, p99, detection time vs. load)
- [ ] Slurm job-monitoring mode
- [ ] CI/CD and deployment on k3s, public read-only dashboard
