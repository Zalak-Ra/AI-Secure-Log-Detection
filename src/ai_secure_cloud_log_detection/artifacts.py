from __future__ import annotations

import json
from pathlib import Path


MANIFEST_FILENAME = "manifest.json"
MODEL_FILENAME = "failure_predictor.keras"
BASELINE_MODEL_FILENAME = "failure_predictor.pkl"


def save_manifest(model_dir: str | Path, manifest: dict[str, object]) -> Path:
    model_path = Path(model_dir)
    model_path.mkdir(parents=True, exist_ok=True)
    destination = model_path / MANIFEST_FILENAME
    destination.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return destination


def load_manifest(model_dir: str | Path) -> dict[str, object]:
    return json.loads((Path(model_dir) / MANIFEST_FILENAME).read_text(encoding="utf-8"))


def model_file(model_dir: str | Path, backend: str = "tensorflow") -> Path:
    filename = MODEL_FILENAME if backend == "tensorflow" else BASELINE_MODEL_FILENAME
    return Path(model_dir) / filename


def scaler_payload(scaler, feature_columns: list[str]) -> dict[str, object]:
    return {
        "feature_columns": feature_columns,
        "data_min": scaler.data_min_.tolist(),
        "data_max": scaler.data_max_.tolist(),
        "scale": scaler.scale_.tolist(),
        "min": scaler.min_.tolist(),
    }


def apply_saved_scaler(matrix, scaler_manifest: dict[str, object]):
    import numpy as np

    scale = np.asarray(scaler_manifest["scale"], dtype="float32")
    min_values = np.asarray(scaler_manifest["min"], dtype="float32")
    return matrix * scale + min_values
