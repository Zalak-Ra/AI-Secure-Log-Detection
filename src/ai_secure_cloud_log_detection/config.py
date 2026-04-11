from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


RAW_TOPIC = "infra.raw.metrics"
FEATURE_TOPIC = "infra.feature.windows"
PREDICTION_TOPIC = "infra.predictions"
DEAD_LETTER_TOPIC = "infra.dlq"

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_CLUSTER_ID = "cluster_primary"
DEFAULT_HOST_COUNT = 25
DEFAULT_TICK_SECONDS = 5
DEFAULT_BUCKET_SECONDS = 30
DEFAULT_LOOKBACK_STEPS = 20
DEFAULT_HORIZON_MINUTES = 5
DEFAULT_SEED = 42

DEFAULT_HISTORY_PATH = Path("data/raw_history.jsonl")
DEFAULT_FEATURE_HISTORY_PATH = Path("data/feature_windows.jsonl")
DEFAULT_MODEL_DIR = Path("models/failure_predictor")

FAILURE_TYPES = (
    "memory_leak",
    "cpu_saturation",
    "network_meltdown",
    "disk_exhaustion",
    "gpu_oom",
    "app_crash",
)

PHASE_TO_CODE = {
    "healthy": 0,
    "stress": 1,
    "degraded": 2,
    "incident": 3,
    "recovery": 4,
}

OBSERVABLE_COLUMNS = [
    "event_time",
    "event_ts",
    "machine_id",
    "cluster_id",
    "role",
    "sequence_no",
    "cpu_pct",
    "mem_pct",
    "gpu_util_pct",
    "gpu_mem_pct",
    "disk_used_pct",
    "disk_io_wait_pct",
    "net_rx_bytes",
    "net_tx_bytes",
    "net_retransmit_pct",
    "request_rate",
    "error_count",
    "latency_p95_ms",
    "queue_depth",
    "restart_count",
    "heartbeat_ok",
]

GROUND_TRUTH_COLUMNS = [
    "gt_incident_id",
    "gt_failure_type",
    "gt_phase",
    "gt_phase_code",
    "gt_failure_start_ts",
    "gt_time_to_failure_sec",
    "gt_is_failure_now",
]

RAW_EVENT_COLUMNS = OBSERVABLE_COLUMNS + GROUND_TRUTH_COLUMNS

BASE_BUCKET_COLUMNS = [
    "window_start",
    "window_end",
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
    "missing_ratio",
    "gt_phase_code_max",
    "gt_is_failure_now_max",
]

MODEL_FEATURE_COLUMNS = [
    "cpu_pct_mean",
    "cpu_pct_max",
    "cpu_delta",
    "mem_pct_mean",
    "mem_pct_max",
    "mem_delta",
    "disk_used_pct_mean",
    "disk_io_wait_pct_mean",
    "disk_io_wait_pct_max",
    "request_rate_mean",
    "latency_p95_ms_mean",
    "latency_delta",
    "error_count_sum",
    "error_rate",
    "queue_depth_mean",
    "queue_delta",
    "net_retransmit_pct_mean",
    "throughput_total",
    "heartbeat_drop_ratio",
    "missing_ratio",
]


def expected_rows_per_bucket(
    tick_seconds: int = DEFAULT_TICK_SECONDS,
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS,
) -> int:
    return max(1, int(round(bucket_seconds / tick_seconds)))


@dataclass(frozen=True)
class PipelineDefaults:
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    raw_topic: str = RAW_TOPIC
    feature_topic: str = FEATURE_TOPIC
    prediction_topic: str = PREDICTION_TOPIC
    history_path: Path = DEFAULT_HISTORY_PATH
    feature_history_path: Path = DEFAULT_FEATURE_HISTORY_PATH
    model_dir: Path = DEFAULT_MODEL_DIR
    host_count: int = DEFAULT_HOST_COUNT
    tick_seconds: int = DEFAULT_TICK_SECONDS
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS
    lookback_steps: int = DEFAULT_LOOKBACK_STEPS
    horizon_minutes: int = DEFAULT_HORIZON_MINUTES
