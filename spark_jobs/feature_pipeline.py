from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ai_secure_cloud_log_detection.config import (  # noqa: E402
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_BUCKET_SECONDS,
    FEATURE_TOPIC,
    RAW_TOPIC,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming feature aggregation pipeline.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--input-topic", default=RAW_TOPIC)
    parser.add_argument("--output-topic", default=FEATURE_TOPIC)
    parser.add_argument("--bucket-seconds", type=int, default=DEFAULT_BUCKET_SECONDS)
    parser.add_argument("--tick-seconds", type=int, default=5)
    parser.add_argument("--checkpoint-dir", default="data/checkpoints/feature_pipeline")
    parser.add_argument("--output-dir", default="data/feature_windows_stream")
    parser.add_argument("--query-name", default="feature_window_pipeline")
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        count,
        from_json,
        lit,
        max as spark_max,
        mean,
        struct,
        sum as spark_sum,
        to_json,
        to_timestamp,
        window,
    )
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

    args = parse_args()
    spark = SparkSession.builder.appName("FailureFeaturePipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_schema = StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_ts", LongType(), True),
            StructField("machine_id", StringType(), True),
            StructField("cluster_id", StringType(), True),
            StructField("role", StringType(), True),
            StructField("sequence_no", LongType(), True),
            StructField("cpu_pct", DoubleType(), True),
            StructField("mem_pct", DoubleType(), True),
            StructField("gpu_util_pct", DoubleType(), True),
            StructField("gpu_mem_pct", DoubleType(), True),
            StructField("disk_used_pct", DoubleType(), True),
            StructField("disk_io_wait_pct", DoubleType(), True),
            StructField("net_rx_bytes", LongType(), True),
            StructField("net_tx_bytes", LongType(), True),
            StructField("net_retransmit_pct", DoubleType(), True),
            StructField("request_rate", DoubleType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("latency_p95_ms", DoubleType(), True),
            StructField("queue_depth", DoubleType(), True),
            StructField("restart_count", IntegerType(), True),
            StructField("heartbeat_ok", IntegerType(), True),
            StructField("gt_phase_code", IntegerType(), True),
            StructField("gt_is_failure_now", IntegerType(), True),
        ]
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.select(from_json(col("value").cast("string"), raw_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp("event_time"))
        .dropDuplicates(["machine_id", "sequence_no"])
        .withWatermark("event_time", "2 minutes")
    )

    expected_rows = max(1, int(round(args.bucket_seconds / args.tick_seconds)))
    bucketed = (
        parsed.groupBy(
            window("event_time", f"{args.bucket_seconds} seconds").alias("time_window"),
            col("machine_id"),
            col("cluster_id"),
            col("role"),
        )
        .agg(
            mean("cpu_pct").alias("cpu_pct_mean"),
            spark_max("cpu_pct").alias("cpu_pct_max"),
            mean("mem_pct").alias("mem_pct_mean"),
            spark_max("mem_pct").alias("mem_pct_max"),
            mean("gpu_util_pct").alias("gpu_util_pct_mean"),
            mean("gpu_mem_pct").alias("gpu_mem_pct_mean"),
            mean("disk_used_pct").alias("disk_used_pct_mean"),
            mean("disk_io_wait_pct").alias("disk_io_wait_pct_mean"),
            spark_max("disk_io_wait_pct").alias("disk_io_wait_pct_max"),
            spark_sum("net_rx_bytes").alias("net_rx_bytes_sum"),
            spark_sum("net_tx_bytes").alias("net_tx_bytes_sum"),
            mean("net_retransmit_pct").alias("net_retransmit_pct_mean"),
            mean("request_rate").alias("request_rate_mean"),
            mean("latency_p95_ms").alias("latency_p95_ms_mean"),
            spark_max("latency_p95_ms").alias("latency_p95_ms_max"),
            spark_sum("error_count").alias("error_count_sum"),
            mean("queue_depth").alias("queue_depth_mean"),
            spark_sum("restart_count").alias("restart_count_sum"),
            mean("heartbeat_ok").alias("heartbeat_ok_mean"),
            count(lit(1)).alias("event_count"),
            spark_max("gt_phase_code").alias("gt_phase_code_max"),
            spark_max("gt_is_failure_now").alias("gt_is_failure_now_max"),
        )
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            "machine_id",
            "cluster_id",
            "role",
            "cpu_pct_mean",
            "cpu_pct_max",
            "mem_pct_mean",
            "mem_pct_max",
            "gpu_util_pct_mean",
            "gpu_mem_pct_mean",
            "disk_used_pct_mean",
            "disk_io_wait_pct_mean",
            "disk_io_wait_pct_max",
            "net_rx_bytes_sum",
            "net_tx_bytes_sum",
            "net_retransmit_pct_mean",
            "request_rate_mean",
            "latency_p95_ms_mean",
            "latency_p95_ms_max",
            "error_count_sum",
            "queue_depth_mean",
            "restart_count_sum",
            "heartbeat_ok_mean",
            "event_count",
            ((lit(float(expected_rows)) - col("event_count")) / lit(float(expected_rows))).alias("missing_ratio"),
            "gt_phase_code_max",
            "gt_is_failure_now_max",
        )
    )

    kafka_payload = bucketed.select(
        col("machine_id").cast("string").alias("key"),
        to_json(struct(*bucketed.columns)).alias("value"),
    )

    kafka_query = (
        kafka_payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", args.output_topic)
        .option("checkpointLocation", f"{args.checkpoint_dir}/kafka")
        .outputMode("update")
        .queryName(f"{args.query_name}_kafka")
        .start()
    )

    file_query = (
        bucketed.writeStream.format("json")
        .option("path", args.output_dir)
        .option("checkpointLocation", f"{args.checkpoint_dir}/files")
        .outputMode("append")
        .queryName(f"{args.query_name}_files")
        .start()
    )

    kafka_query.awaitTermination()
    file_query.awaitTermination()


if __name__ == "__main__":
    main()
