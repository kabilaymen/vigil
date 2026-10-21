# Ingestion load test

`loadtest/` connects N simulated machines to the MQTT broker, each publishing telemetry at 2 msg/s
(QoS 0), and reads the Kafka `telemetry` topic in the same process: delivered rate, loss, and
end-to-end latency from the device timestamp to the record read from Kafka (publisher and reader
share one clock). Each step publishes for 5 s of warm-up, then 20 s measured, then waits 10 s.

Run by the `loadtest` workflow on a GitHub Actions runner — **4 vCPU, 15 GB, with the broker,
Kafka, the gateway and the load generator all on that one machine**, so these are lower bounds for a
deployment where they are separate. Run of 2026-10-21:
[actions/runs/39596873307](https://github.com/kabilaymen/vigil/actions/runs/39596873307).

| Machines | Offered msg/s | Delivered msg/s | Loss | e2e p50 | e2e p99 | e2e p99.9 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 500 | 500 | 0 % | 14.9 ms | 23.4 ms | 25.8 ms | 29 ms |
| 1,000 | 2,000 | 2,000 | 0.01 % | 7.7 ms | 13.7 ms | 16.1 ms | 23 ms |
| 2,000 | 4,000 | 4,000 | 0 % | 5.7 ms | 10.7 ms | 14.1 ms | 23 ms |
| 4,000 | 8,000 | 8,000 | 0 % | 6.2 ms | 16.2 ms | 34.6 ms | 49 ms |
| 8,000 | 16,000 | 16,000 | 0 % | 7.0 ms | 193.8 ms | 332.5 ms | 400 ms |

Reading it:

- No loss up to 16,000 msg/s; the one 0.01 % step (about 4 messages of 40,000) is QoS 0 telemetry,
  which MQTT does not guarantee.
- The tail is flat up to 8,000 msg/s and grows tenfold at 16,000 msg/s (p99 16 → 194 ms): the
  single machine is close to saturation there; the next step would find the limit.
- p50 is higher at the smallest step: the gateway's producer lingers 5 ms to batch, and with
  little traffic batches fill slowly.

Not measured here: the detector and the processor (consumer lag under this load), and QoS 1.

Reproduce locally (heavy: Kafka plus thousands of MQTT connections):

```bash
docker compose up -d --wait mosquitto kafka gateway
(cd loadtest && go run . -steps 250,1000,2000 -duration 20s)
```
