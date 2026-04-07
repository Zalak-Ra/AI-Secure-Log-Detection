#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$PWD/kafka}"
TOPIC="${TOPIC:-server_logs}"
LOG_DIR="${LOG_DIR:-$PWD/logs}"

if [[ ! -x "$KAFKA_HOME/bin/zookeeper-server-start.sh" ]]; then
  echo "Kafka was not found at $KAFKA_HOME."
  echo "Set KAFKA_HOME=/path/to/kafka before running this script."
  exit 1
fi

mkdir -p "$LOG_DIR"

echo "Starting Zookeeper..."
"$KAFKA_HOME/bin/zookeeper-server-start.sh" "$KAFKA_HOME/config/zookeeper.properties" \
  > "$LOG_DIR/zookeeper.log" 2>&1 &
ZOOKEEPER_PID=$!

echo "Starting Kafka broker..."
sleep 5
"$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" \
  > "$LOG_DIR/kafka.log" 2>&1 &
KAFKA_PID=$!

cleanup() {
  echo "Stopping Kafka and Zookeeper..."
  kill "$KAFKA_PID" "$ZOOKEEPER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting for Kafka on localhost:9092..."
for _ in {1..30}; do
  if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1

echo "Kafka is running. Logs: $LOG_DIR"
echo "Topic ready: $TOPIC"
wait "$KAFKA_PID"
