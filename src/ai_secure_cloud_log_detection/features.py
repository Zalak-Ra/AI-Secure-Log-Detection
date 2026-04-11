from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import (
    BASE_BUCKET_COLUMNS,
    DEFAULT_BUCKET_SECONDS,
    DEFAULT_HORIZON_MINUTES,
    DEFAULT_LOOKBACK_STEPS,
    DEFAULT_TICK_SECONDS,
    MODEL_FEATURE_COLUMNS,
    expected_rows_per_bucket,
)


def _pd():
    import pandas as pd

    return pd


def _np():
    import numpy as np

    return np


def load_raw_history(path: str | Path):
    pd = _pd()
    frame = pd.read_json(path, lines=True)
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    frame = frame.sort_values(["machine_id", "event_time"]).reset_index(drop=True)
    return frame


def aggregate_raw_events(
    raw_frame,
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS,
    tick_seconds: int = DEFAULT_TICK_SECONDS,
):
    pd = _pd()

    expected = expected_rows_per_bucket(tick_seconds=tick_seconds, bucket_seconds=bucket_seconds)
    grouped_frames = []
    bucket_rule = f"{bucket_seconds}s"

    for machine_id, machine_frame in raw_frame.groupby("machine_id"):
        machine_frame = machine_frame.sort_values("event_time").set_index("event_time")
        identities = machine_frame.iloc[0][["cluster_id", "role"]]
        agg_frame = machine_frame.resample(bucket_rule).agg(
            {
                "cpu_pct": ["mean", "max"],
                "mem_pct": ["mean", "max"],
                "gpu_util_pct": "mean",
                "gpu_mem_pct": "mean",
                "disk_used_pct": "mean",
                "disk_io_wait_pct": ["mean", "max"],
                "net_rx_bytes": "sum",
                "net_tx_bytes": "sum",
                "net_retransmit_pct": "mean",
                "request_rate": "mean",
                "latency_p95_ms": ["mean", "max"],
                "error_count": "sum",
                "queue_depth": "mean",
                "restart_count": "sum",
                "heartbeat_ok": "mean",
                "sequence_no": "count",
                "gt_phase_code": "max",
                "gt_is_failure_now": "max",
            }
        )
        agg_frame.columns = [
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
            "gt_phase_code_max",
            "gt_is_failure_now_max",
        ]
        agg_frame["machine_id"] = machine_id
        agg_frame["cluster_id"] = identities["cluster_id"]
        agg_frame["role"] = identities["role"]
        agg_frame["window_start"] = agg_frame.index
        agg_frame["window_end"] = agg_frame.index + pd.to_timedelta(bucket_seconds, unit="s")
        agg_frame["missing_ratio"] = 1.0 - (agg_frame["event_count"].clip(upper=expected) / expected)

        fill_columns = [
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
        ]
        agg_frame[fill_columns] = agg_frame[fill_columns].ffill(limit=2).fillna(0.0)
        agg_frame["gt_phase_code_max"] = agg_frame["gt_phase_code_max"].fillna(0).astype(int)
        agg_frame["gt_is_failure_now_max"] = agg_frame["gt_is_failure_now_max"].fillna(0).astype(int)
        grouped_frames.append(agg_frame.reset_index(drop=True))

    if not grouped_frames:
        return pd.DataFrame(columns=BASE_BUCKET_COLUMNS)
    feature_frame = pd.concat(grouped_frames, ignore_index=True)
    return feature_frame[BASE_BUCKET_COLUMNS]


def collect_incident_table(raw_frame):
    pd = _pd()

    incidents = raw_frame.dropna(subset=["gt_incident_id", "gt_failure_start_ts"]).copy()
    if incidents.empty:
        return incidents
    incidents["gt_failure_start_ts"] = pd.to_datetime(incidents["gt_failure_start_ts"], utc=True)
    incidents = (
        incidents[["machine_id", "gt_incident_id", "gt_failure_type", "gt_failure_start_ts"]]
        .drop_duplicates()
        .sort_values(["machine_id", "gt_failure_start_ts"])
        .reset_index(drop=True)
    )
    return incidents


def label_feature_windows(
    feature_frame,
    incident_table,
    horizon_minutes: int = DEFAULT_HORIZON_MINUTES,
):
    np = _np()
    pd = _pd()

    frame = feature_frame.copy()
    frame["target_failure_within_horizon"] = 0
    frame["lead_time_sec"] = np.nan
    frame["eligible_for_training"] = (
        (frame["gt_is_failure_now_max"] == 0)
        & (frame["gt_phase_code_max"] <= 2)
    )

    horizon_seconds = pd.to_timedelta(horizon_minutes, unit="m").total_seconds()
    for machine_id, machine_frame in frame.groupby("machine_id"):
        failure_starts = incident_table.loc[
            incident_table["machine_id"] == machine_id, "gt_failure_start_ts"
        ]
        if failure_starts.empty:
            continue

        starts = failure_starts.sort_values().to_numpy(dtype="datetime64[ns]")
        machine_indices = machine_frame.index.to_numpy()
        end_values = machine_frame["window_end"].to_numpy(dtype="datetime64[ns]")
        lookup_indices = np.searchsorted(starts, end_values, side="right")
        valid = lookup_indices < len(starts)
        if not valid.any():
            continue

        next_starts = starts[lookup_indices[valid]]
        lead_seconds = (next_starts - end_values[valid]) / np.timedelta64(1, "s")
        within_horizon = lead_seconds <= horizon_seconds
        target_indices = machine_indices[valid][within_horizon]
        frame.loc[target_indices, "target_failure_within_horizon"] = 1
        frame.loc[target_indices, "lead_time_sec"] = lead_seconds[within_horizon]

    return frame


def add_derived_features(feature_frame):
    frame = feature_frame.copy()
    for _, machine_frame in frame.groupby("machine_id"):
        ordered = machine_frame.sort_values("window_end")
        indices = ordered.index
        frame.loc[indices, "cpu_delta"] = ordered["cpu_pct_mean"].diff().fillna(0.0)
        frame.loc[indices, "mem_delta"] = ordered["mem_pct_mean"].diff().fillna(0.0)
        frame.loc[indices, "latency_delta"] = ordered["latency_p95_ms_mean"].diff().fillna(0.0)
        frame.loc[indices, "queue_delta"] = ordered["queue_depth_mean"].diff().fillna(0.0)
        frame.loc[indices, "throughput_total"] = ordered["net_rx_bytes_sum"] + ordered["net_tx_bytes_sum"]
        frame.loc[indices, "heartbeat_drop_ratio"] = 1.0 - ordered["heartbeat_ok_mean"]
        frame.loc[indices, "error_rate"] = ordered["error_count_sum"] / ordered["request_rate_mean"].clip(lower=1.0)

    return frame


def split_by_time(feature_frame, train_ratio: float = 0.7, val_ratio: float = 0.15):
    frame = feature_frame.sort_values("window_end").reset_index(drop=True)
    if frame.empty:
        return frame, frame, frame
    train_cutoff = frame["window_end"].quantile(train_ratio)
    val_cutoff = frame["window_end"].quantile(train_ratio + val_ratio)
    train_frame = frame[frame["window_end"] <= train_cutoff].copy()
    val_frame = frame[(frame["window_end"] > train_cutoff) & (frame["window_end"] <= val_cutoff)].copy()
    test_frame = frame[frame["window_end"] > val_cutoff].copy()
    return train_frame, val_frame, test_frame


def create_sequences(
    feature_frame,
    lookback_steps: int = DEFAULT_LOOKBACK_STEPS,
    feature_columns: list[str] | None = None,
):
    np = _np()

    feature_columns = feature_columns or MODEL_FEATURE_COLUMNS
    sequences = []
    labels = []
    metadata: list[dict[str, Any]] = []

    for machine_id, machine_frame in feature_frame.groupby("machine_id"):
        ordered = machine_frame.sort_values("window_end").reset_index(drop=True)
        if len(ordered) < lookback_steps:
            continue
        for index in range(lookback_steps - 1, len(ordered)):
            current_row = ordered.iloc[index]
            if not bool(current_row.get("eligible_for_training", True)):
                continue
            window = ordered.iloc[index - lookback_steps + 1 : index + 1]
            if float(window["missing_ratio"].mean()) > 0.8:
                continue
            sequences.append(window[feature_columns].to_numpy(dtype="float32"))
            labels.append(int(current_row["target_failure_within_horizon"]))
            metadata.append(
                {
                    "machine_id": machine_id,
                    "window_end": str(current_row["window_end"]),
                    "label": int(current_row["target_failure_within_horizon"]),
                    "lead_time_sec": (
                        None if current_row["lead_time_sec"] != current_row["lead_time_sec"] else float(current_row["lead_time_sec"])
                    ),
                }
            )

    if not sequences:
        return (
            np.empty((0, lookback_steps, len(feature_columns)), dtype="float32"),
            np.empty((0,), dtype="int32"),
            metadata,
        )
    return np.stack(sequences), np.asarray(labels, dtype="int32"), metadata


def build_training_frame(
    raw_history_path: str | Path,
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS,
    tick_seconds: int = DEFAULT_TICK_SECONDS,
    horizon_minutes: int = DEFAULT_HORIZON_MINUTES,
):
    raw_frame = load_raw_history(raw_history_path)
    incident_table = collect_incident_table(raw_frame)
    feature_frame = aggregate_raw_events(
        raw_frame,
        bucket_seconds=bucket_seconds,
        tick_seconds=tick_seconds,
    )
    feature_frame = label_feature_windows(
        feature_frame,
        incident_table,
        horizon_minutes=horizon_minutes,
    )
    feature_frame = add_derived_features(feature_frame)
    return feature_frame
