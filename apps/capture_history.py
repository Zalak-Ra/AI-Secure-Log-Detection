from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ai_secure_cloud_log_detection.config import (  # noqa: E402
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_HISTORY_PATH,
    RAW_TOPIC,
)
from ai_secure_cloud_log_detection.simulator import append_jsonl, validate_event_payload  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture raw Kafka telemetry to JSONL history.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--history-path", type=Path, default=DEFAULT_HISTORY_PATH)
    return parser.parse_args()


def main() -> None:
    try:
        from kafka import KafkaConsumer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Install dependencies before running the capture step."
        ) from exc

    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap_servers],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )

    print(f"Capturing topic '{args.topic}' to {args.history_path.resolve()}")
    try:
        for message in consumer:
            event = message.value
            validate_event_payload(event)
            append_jsonl([event], args.history_path)
    except KeyboardInterrupt:
        print("Stopping history capture.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
