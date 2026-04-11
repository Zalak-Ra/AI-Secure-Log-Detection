from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ai_secure_cloud_log_detection.config import (  # noqa: E402
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_HISTORY_PATH,
    DEFAULT_HOST_COUNT,
    DEFAULT_SEED,
    DEFAULT_TICK_SECONDS,
    RAW_TOPIC,
)
from ai_secure_cloud_log_detection.simulator import (  # noqa: E402
    TelemetrySimulator,
    append_jsonl,
    validate_event_payload,
)


def create_producer(bootstrap_servers: str):
    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Install dependencies before streaming to Kafka."
        ) from exc

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic telemetry and optionally stream it to Kafka.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--history-path", type=Path, default=DEFAULT_HISTORY_PATH)
    parser.add_argument("--host-count", type=int, default=DEFAULT_HOST_COUNT)
    parser.add_argument("--tick-seconds", type=int, default=DEFAULT_TICK_SECONDS)
    parser.add_argument("--incident-rate-scale", type=float, default=1.0)
    parser.add_argument("--steps", type=int, default=None, help="Total events to generate. Omit for endless streaming.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--no-kafka", action="store_true", help="Write history locally without producing to Kafka.")
    parser.add_argument("--fast", action="store_true", help="Do not sleep between ticks when generating finite history.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    simulator = TelemetrySimulator(
        host_count=args.host_count,
        tick_seconds=args.tick_seconds,
        seed=args.seed,
        incident_rate_scale=args.incident_rate_scale,
    )
    producer = None if args.no_kafka else create_producer(args.bootstrap_servers)

    start_time = datetime.now(timezone.utc)
    batch: list[dict[str, object]] = []
    try:
        for event in simulator.iter_events(
            start_time=start_time,
            steps=args.steps,
            sleep=(not args.fast and args.steps is None),
        ):
            validate_event_payload(event)
            batch.append(event)
            if producer is not None:
                producer.send(args.topic, event)
            if len(batch) >= max(1, args.host_count):
                append_jsonl(batch, args.history_path)
                batch.clear()
                if producer is not None:
                    producer.flush()
            if args.steps is not None and not args.fast:
                time.sleep(0.0)
    finally:
        if batch:
            append_jsonl(batch, args.history_path)
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
