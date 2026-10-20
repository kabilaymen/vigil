"""Fleet simulator: N virtual production machines publishing telemetry over MQTT.

Topics (one MQTT connection per machine, so that each one has its own Last Will):
  vigil/machines/<id>/telemetry   periodic measurements
  vigil/machines/<id>/alarm       sporadic alarms
  vigil/machines/<id>/status      "online" (retained) on connect; broker publishes
                                  "offline" (Last Will) if the connection drops uncleanly
"""

import argparse
import json
import logging
import random
import signal
import threading
import time

import paho.mqtt.client as mqtt

log = logging.getLogger("simulator")

MACHINE_TYPES = {
    # type: (nominal temperature °C, nominal throughput parts/min)
    "embosser": (45.0, 120.0),
    "laser-personalizer": (60.0, 80.0),
    "chip-encoder": (38.0, 200.0),
}

ALARMS = [
    ("TEMP_HIGH", "warning"),
    ("FEEDER_JAM", "critical"),
    ("ENCODER_TIMEOUT", "warning"),
    ("DOOR_OPEN", "info"),
]


def now_ms() -> int:
    return int(time.time() * 1000)


class Machine:
    def __init__(self, machine_id: str, mtype: str, broker: str, port: int, rate: float):
        self.id = machine_id
        self.type = mtype
        self.rate = rate
        self.seq = 0
        self.temp, self.throughput = MACHINE_TYPES[mtype]
        self.broker, self.port = broker, port
        self.client = None
        self.alive = False
        self.stalled_until = 0.0

    def topic(self, kind: str) -> str:
        return f"vigil/machines/{self.id}/{kind}"

    def connect(self) -> None:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.id, clean_session=True)
        c.will_set(self.topic("status"), self.status_payload("offline"), qos=1, retain=True)
        c.connect(self.broker, self.port, keepalive=5)
        c.loop_start()
        c.publish(self.topic("status"), self.status_payload("online"), qos=1, retain=True)
        self.client, self.alive = c, True

    def status_payload(self, state: str) -> str:
        return json.dumps({"machine_id": self.id, "type": self.type, "kind": "status",
                           "state": state, "ts": now_ms()})

    def crash(self) -> None:
        """Drop the TCP connection without an MQTT DISCONNECT, so the broker fires the Last Will."""
        if not self.alive:
            return
        log.info("crashing %s", self.id)
        self.alive = False
        sock = self.client.socket()
        self.client.loop_stop()
        if sock:
            sock.close()

    def stall(self, seconds: float) -> None:
        """Stop publishing while keeping the MQTT session open: keepalives are sent by
        paho's network thread, so the broker never notices. Only heartbeat analysis can."""
        log.info("stalling %s for %.0fs", self.id, seconds)
        self.stalled_until = time.monotonic() + seconds

    def tick(self) -> None:
        if not self.alive or time.monotonic() < self.stalled_until:
            return
        # bounded random walk around the nominal values
        nominal_t, nominal_p = MACHINE_TYPES[self.type]
        self.temp += random.gauss(0, 0.4) + 0.05 * (nominal_t - self.temp)
        self.throughput = max(0.0, self.throughput + random.gauss(0, 2) + 0.1 * (nominal_p - self.throughput))
        self.seq += 1
        msg = {
            "machine_id": self.id, "type": self.type, "kind": "telemetry", "seq": self.seq, "ts": now_ms(),
            "params": {"temperature_c": round(self.temp, 2), "throughput_ppm": round(self.throughput, 1)},
        }
        self.client.publish(self.topic("telemetry"), json.dumps(msg), qos=0)
        if random.random() < 0.002:
            code, severity = random.choice(ALARMS)
            self.client.publish(self.topic("alarm"), json.dumps({
                "machine_id": self.id, "type": self.type, "kind": "alarm", "ts": now_ms(),
                "code": code, "severity": severity}), qos=1)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--broker", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--machines", type=int, default=20, help="number of simulated machines")
    p.add_argument("--rate", type=float, default=1.0, help="telemetry messages per second per machine")
    p.add_argument("--crash-every", type=float, default=0, help="crash a random machine every N seconds (0 = never)")
    p.add_argument("--recover-after", type=float, default=20, help="seconds before a crashed machine reconnects")
    p.add_argument("--stall-every", type=float, default=0, help="stall a random machine every N seconds (0 = never)")
    p.add_argument("--stall-for", type=float, default=15, help="duration of a stall in seconds")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    types = list(MACHINE_TYPES)
    machines = [Machine(f"m-{i:04d}", types[i % len(types)], args.broker, args.port, args.rate)
                for i in range(args.machines)]
    for m in machines:
        for attempt in range(30):
            try:
                m.connect()
                break
            except OSError:
                time.sleep(1)
        else:
            raise SystemExit(f"cannot reach broker {args.broker}:{args.port}")
    log.info("%d machines online, %.1f msg/s each", len(machines), args.rate)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    period = 1.0 / args.rate
    next_crash = time.monotonic() + args.crash_every if args.crash_every else float("inf")
    next_stall = time.monotonic() + args.stall_every if args.stall_every else float("inf")
    recover_at: dict[str, float] = {}
    while not stop.is_set():
        start = time.monotonic()
        for m in machines:
            m.tick()
        if start >= next_crash:
            victim = random.choice([m for m in machines if m.alive])
            victim.crash()
            recover_at[victim.id] = start + args.recover_after
            next_crash = start + args.crash_every
        if start >= next_stall:
            random.choice([m for m in machines if m.alive]).stall(args.stall_for)
            next_stall = start + args.stall_every
        for m in machines:
            if m.id in recover_at and start >= recover_at[m.id]:
                del recover_at[m.id]
                log.info("recovering %s", m.id)
                m.connect()
        stop.wait(max(0.0, period - (time.monotonic() - start)))

    for m in machines:
        if m.alive:
            m.client.publish(m.topic("status"), m.status_payload("offline"), qos=1, retain=True)
            m.client.disconnect()
            m.client.loop_stop()


if __name__ == "__main__":
    main()
