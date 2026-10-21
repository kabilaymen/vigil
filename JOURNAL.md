# Engineering journal

What was built so far, in order, and the reasoning behind the non-obvious choices.

> **How this project was built.** I wrote it with an AI coding assistant (Claude): I set the goals
> and acceptance criteria, ran the stack and read the results, and reviewed the code; much of the
> first-draft code came from the assistant.

## 1. Ingestion: simulator → MQTT → gateway → Kafka

- **One MQTT connection per simulated machine.** The Last Will belongs to a connection; with one
  shared connection the broker could not say *which* machine died.
- **A crash closes the socket without DISCONNECT.** A clean DISCONNECT cancels the Last Will (a
  voluntary leave is not a failure), so a realistic crash must drop TCP.
- **QoS 0 for telemetry, QoS 1 for alarms and status.** Telemetry is frequent and replaceable;
  an alarm or a state change must not be lost.
- **Gateway keys Kafka records by machine id**, so each machine's messages stay ordered inside one
  partition.
- **Backpressure instead of drops**: when the producer buffer is full, `Produce` blocks and MQTT
  consumption slows down.
- Prometheus metrics from the start (ingest rate, MQTT receipt → Kafka ack latency, errors) and a
  Grafana dashboard.

Then a housekeeping commit: the repo was created on Windows, so line endings were normalized to LF
(`.gitattributes`) before anything else landed.

## 2. Failure detector: two signals, because one is not enough

The Last Will only covers dead connections. **A stalled application stays "online"**: the MQTT
client sends its keepalive pings from its own network thread, so the broker keeps the session alive
while the application publishes nothing. Hence a second, application-level signal: a
**phi accrual detector** (Hayashibara et al., 2004) on heartbeat inter-arrival times, per machine.
The simulator gained a `--stall-every` scenario to exercise exactly this case.

Choices that matter for false positives:

- **Heartbeats are timestamped by the gateway**, not by the machines: one clock for the whole fleet,
  no skew between devices.
- **Warm-up**: a machine is not judged before enough samples exist to estimate its rhythm.
- **History reset on reconnection**: the outage gap would otherwise inflate the variance and make
  the detector slow to notice the next failure.
- **Suspended while the detector lags behind the stream**: if the detector itself is slow, the gaps
  it sees are its own, and it must not declare the whole fleet dead.

Observed locally (50 machines, 2 msg/s, φ ≥ 8): a stalled machine is suspected after ~1.2 s of
silence (nominal interval 0.5 s); a crashed one is reported via its Last Will in ~1 s.

## Not done yet

- No load test: the numbers above are from a 50-machine fleet, not a stress run.
- Stream processor with windowed aggregates in TimescaleDB.
- Deployment with a public read-only dashboard.
