import csv
import json
from pathlib import Path
from kafka import KafkaConsumer


KAFKA_TOPIC = "server_logs"
BOOTSTRAP_SERVERS = ["localhost:9092"]
CSV_PATH = Path("live_data.csv")
FIELDNAMES = [
    "timestamp",
    "machine",
    "cpu_usage",
    "gpu_wrk_util",
    "avg_mem",
    "max_mem",
    "avg_gpu_wrk_mem",
    "max_gpu_wrk_mem",
    "read",
    "write",
    "read_count",
    "write_count",
]


def ensure_csv_header(path):
    if path.exists() and path.stat().st_size > 0:
        return

    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(FIELDNAMES)


def row_from_payload(payload):
    return [payload.get(field, "") for field in FIELDNAMES]


def main():
    ensure_csv_header(CSV_PATH)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print(f"Appending Kafka topic '{KAFKA_TOPIC}' to {CSV_PATH.resolve()}")
    try:
        with CSV_PATH.open("a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            for message in consumer:
                writer.writerow(row_from_payload(message.value))
                csv_file.flush()
    except KeyboardInterrupt:
        print("Stopping log harvester.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
