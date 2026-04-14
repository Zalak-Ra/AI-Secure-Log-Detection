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
    DEFAULT_FEATURE_HISTORY_PATH,
    DEFAULT_MODEL_DIR,
    FEATURE_TOPIC,
    PREDICTION_TOPIC,
)
from ai_secure_cloud_log_detection.inference import OnlinePredictor  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve live failure predictions from aggregated feature windows.")
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--feature-topic", default=FEATURE_TOPIC)
    parser.add_argument("--prediction-topic", default=PREDICTION_TOPIC)
    parser.add_argument("--feature-file", type=Path, default=None)
    parser.add_argument("--output-file", type=Path, default=DEFAULT_FEATURE_HISTORY_PATH.parent / "predictions.jsonl")
    parser.add_argument("--stdout-only", action="store_true")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-event JSON output and print only summaries.")
    return parser.parse_args()


def iter_feature_events(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def append_jsonl(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def create_kafka_clients(bootstrap_servers: str, feature_topic: str, prediction_topic: str):
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Install dependencies before online scoring from Kafka."
        ) from exc

    consumer = KafkaConsumer(
        feature_topic,
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )
    return consumer, producer, prediction_topic


def main() -> None:
    args = parse_args()
    predictor = OnlinePredictor(args.model_dir)
    processed_events = 0
    emitted_predictions = 0

    if args.feature_file is not None:
        print(f"Scoring feature windows from {args.feature_file} using model {args.model_dir}")
        for event in iter_feature_events(args.feature_file):
            processed_events += 1
            prediction = predictor.process_bucket_event(event)
            if prediction is None:
                continue
            if not args.quiet:
                print(json.dumps(prediction))
            if not args.stdout_only:
                append_jsonl(args.output_file, prediction)
            emitted_predictions += 1
        print(
            f"Processed {processed_events} feature windows and emitted "
            f"{emitted_predictions} predictions"
            + ("" if args.stdout_only else f" to {args.output_file}")
        )
        return

    consumer, producer, prediction_topic = create_kafka_clients(
        args.bootstrap_servers,
        args.feature_topic,
        args.prediction_topic,
    )
    print(
        f"Serving live predictions from topic '{args.feature_topic}' to "
        f"'{prediction_topic}' using model {args.model_dir}"
    )
    try:
        for message in consumer:
            processed_events += 1
            prediction = predictor.process_bucket_event(message.value)
            if prediction is None:
                continue
            if not args.quiet:
                print(json.dumps(prediction))
            if not args.stdout_only:
                append_jsonl(args.output_file, prediction)
            producer.send(prediction_topic, prediction)
            producer.flush()
            emitted_predictions += 1
    except KeyboardInterrupt:
        print("Stopping prediction service.")
    finally:
        consumer.close()
        producer.close()
        print(
            f"Processed {processed_events} feature windows and emitted "
            f"{emitted_predictions} predictions"
            + ("" if args.stdout_only else f" to {args.output_file}")
        )


if __name__ == "__main__":
    main()
