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

## 3. Stream processor: 10-second history in TimescaleDB

The processor aggregates telemetry per machine into aligned 10-second buckets and writes them to a
hypertable. The hard part is restarts: Kafka replays from the last committed offset, and rewriting
a bucket from a partial replay would replace a correct aggregate with a smaller one. So the offsets
are not committed to Kafka at all: each closed bucket is written in one transaction together with,
per partition, the offset of the first record of the oldest bucket still open and the last bucket
written. On start the consumer seeks there and skips records of buckets already written.

The aggregator is pure (no Kafka, no database), which made one test possible: crash the processor
at random points, restart it from what the "database" holds, and compare with an uninterrupted
run. Two deliberate mutations (resume from the last offset read; do not skip written buckets) make
it fail — the second rewrites a 40-record bucket with 3 records, the exact bug the design avoids.

That test also found a real design flaw. My first watermark was the highest event time seen across
all partitions: a partition with fewer machines, read at the same rate, runs ahead in event time
and closed buckets the others were still filling — 2,967 records dropped as "late" in a run with
no crash at all. The watermark is now the lowest of the per-partition watermarks (as in Flink), and
a partition with nothing to read for 5 s is marked idle so that it cannot hold every bucket open.
With that change the crash test passes with the real 2 s lateness (542 crashes over 50 seeds, no
record late, identical rows).

Deployed, it failed twice on the first day:

- `create_hypertable` did not exist. I had mounted my init script directory over the image's
  `/docker-entrypoint-initdb.d`, which hid the image's own script that installs the extension. Now
  only the file is mounted, and the processor also runs `CREATE EXTENSION IF NOT EXISTS`.
- **Only 5 machines of 50 ended up in the table.** On its first start the processor caught up
  80 minutes of retained data, and Kafka served long runs of one partition before the others. The
  per-partition watermark only counted partitions already read, so buckets closed on the first
  partition alone and every other machine's records were dropped as late. An assigned partition
  that has not been read yet now holds the watermark, and a partition is idle only once read to its
  end (high watermark) — or after 30 s with no fetch at all. The crash test missed it because it
  read all partitions in lockstep, one record at a time; it now reads them in random batches, like
  Kafka, and fails without the fix (as does a test that replays one partition after another).
- Then it was **OOM-killed** while catching up (the kernel log shows the memory cgroup kill at
  64 MiB): franz-go's default fetch size is 50 MiB. Fetches are now capped at 4 MiB (1 MiB per
  partition), the limit is 128 MiB, and `GOMEMLIMIT` tells the Go GC about it.

The SQL runs against a real TimescaleDB in a test (`TEST_DATABASE_URL`): idempotent schema, upsert,
progress read back. Grafana reads through a read-only role created at database initialisation.

## 4. Load test

Run on a GitHub Actions runner rather than on my laptop or the demo VM: a clean machine, the same
one every run, and nothing else on it. Steps from 250 to 8,000 machines at 2 msg/s; no loss up to
16,000 msg/s, p99 flat around 10–16 ms up to 8,000 msg/s and ~194 ms at 16,000 msg/s, where the
single 4-vCPU machine (broker + Kafka + gateway + generator) approaches saturation
([bench/LOADTEST.md](bench/LOADTEST.md)).

Small thing learned on the way: the workflow's summary and logs need a GitHub login even on a public
repository, but annotations do not, so the results are also published as annotations.

## Not done yet

- Detection time and processor lag under load (the load test covers the ingestion path).
