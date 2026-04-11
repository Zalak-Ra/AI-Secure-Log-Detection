from __future__ import annotations

import pickle
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifacts import apply_saved_scaler, load_manifest, model_file
from .features import add_derived_features


def _ensure_tensorflow():
    try:
        import tensorflow as tf
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "TensorFlow is not installed, so online inference cannot start in this environment."
        ) from exc
    return tf


def infer_likely_failure_mode(latest_row: dict[str, Any]) -> str:
    if latest_row.get("mem_pct_mean", 0.0) > 88 and latest_row.get("mem_delta", 0.0) > 0:
        return "memory_leak"
    if latest_row.get("cpu_pct_mean", 0.0) > 88 and latest_row.get("queue_depth_mean", 0.0) > 30:
        return "cpu_saturation"
    if latest_row.get("net_retransmit_pct_mean", 0.0) > 4.0:
        return "network_meltdown"
    if latest_row.get("disk_used_pct_mean", 0.0) > 92 and latest_row.get("disk_io_wait_pct_mean", 0.0) > 35:
        return "disk_exhaustion"
    if latest_row.get("gpu_mem_pct_mean", 0.0) > 90 and latest_row.get("gpu_util_pct_mean", 0.0) < 30:
        return "gpu_oom"
    if latest_row.get("heartbeat_drop_ratio", 0.0) > 0.3 and latest_row.get("error_count_sum", 0.0) > 10:
        return "app_crash"
    return "unknown"


def risk_band(score: float, threshold: float) -> str:
    if score >= max(0.95, threshold + 0.12):
        return "critical"
    if score >= threshold:
        return "high"
    if score >= threshold * 0.7:
        return "medium"
    return "low"


class OnlinePredictor:
    def __init__(self, model_dir: str | Path) -> None:
        import pandas as pd

        self.pd = pd
        self.manifest = load_manifest(model_dir)
        self.backend = str(self.manifest.get("model_backend", "tensorflow"))
        if self.backend == "tensorflow":
            tf = _ensure_tensorflow()
            self.model = tf.keras.models.load_model(model_file(model_dir, backend=self.backend))
        else:
            with model_file(model_dir, backend=self.backend).open("rb") as handle:
                self.model = pickle.load(handle)
        self.lookback_steps = int(self.manifest["lookback_steps"])
        self.feature_columns = list(self.manifest["feature_columns"])
        self.buffers: dict[str, deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.lookback_steps)
        )

    def process_bucket_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        machine_id = str(event["machine_id"])
        window_end = str(event["window_end"])
        buffer = self.buffers[machine_id]
        if buffer and str(buffer[-1]["window_end"]) == window_end:
            buffer[-1] = event
        else:
            existing_items = list(buffer)
            buffer.clear()
            replaced = False
            for existing in existing_items:
                if str(existing["window_end"]) == window_end:
                    buffer.append(event)
                    replaced = True
                else:
                    buffer.append(existing)
            if not replaced:
                buffer.append(event)

        if len(buffer) < self.lookback_steps:
            return None

        frame = self.pd.DataFrame(list(buffer))
        frame["window_start"] = self.pd.to_datetime(frame["window_start"], utc=True)
        frame["window_end"] = self.pd.to_datetime(frame["window_end"], utc=True)
        frame = frame.sort_values("window_end").reset_index(drop=True)
        machine_frame = add_derived_features(frame)

        matrix = machine_frame[self.feature_columns].to_numpy(dtype="float32")
        matrix = apply_saved_scaler(matrix, self.manifest["scaler"])
        if self.backend == "tensorflow":
            score = float(self.model.predict(matrix[None, :, :], verbose=0)[0][0])
        else:
            score = float(self.model.predict_proba(matrix.reshape(1, -1))[0][1])
        latest = machine_frame.iloc[-1].to_dict()
        threshold = float(self.manifest["threshold"])

        return {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "machine_id": machine_id,
            "cluster_id": event["cluster_id"],
            "role": event["role"],
            "window_start": latest["window_start"].isoformat(),
            "window_end": latest["window_end"].isoformat(),
            "p_fail_5m": round(score, 6),
            "risk_band": risk_band(score, threshold),
            "predicted_failure_type": infer_likely_failure_mode(latest),
            "alert_fired": bool(score >= threshold),
            "threshold": threshold,
            "model_version": self.manifest["model_version"],
            "missing_ratio": float(latest["missing_ratio"]),
            "cpu_pct_mean": float(latest["cpu_pct_mean"]),
            "mem_pct_mean": float(latest["mem_pct_mean"]),
            "latency_p95_ms_mean": float(latest["latency_p95_ms_mean"]),
            "error_count_sum": float(latest["error_count_sum"]),
            "queue_depth_mean": float(latest["queue_depth_mean"]),
            "net_retransmit_pct_mean": float(latest["net_retransmit_pct_mean"]),
        }
