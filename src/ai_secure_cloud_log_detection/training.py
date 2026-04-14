from __future__ import annotations

import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path

from .artifacts import BASELINE_MODEL_FILENAME, MODEL_FILENAME, model_file, save_manifest, scaler_payload
from .config import (
    DEFAULT_BUCKET_SECONDS,
    DEFAULT_HORIZON_MINUTES,
    DEFAULT_LOOKBACK_STEPS,
    DEFAULT_MODEL_DIR,
    DEFAULT_TICK_SECONDS,
    MODEL_FEATURE_COLUMNS,
)
from .features import build_training_frame, create_sequences, split_by_time


def _ensure_tensorflow():
    try:
        import tensorflow as tf
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "TensorFlow is not installed in this environment. "
            "Use Python 3.10 or 3.11 with `pip install -r requirements.txt` "
            "before running the training stage."
        ) from exc
    return tf


def prepare_datasets(
    raw_history_path: str | Path,
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS,
    tick_seconds: int = DEFAULT_TICK_SECONDS,
    lookback_steps: int = DEFAULT_LOOKBACK_STEPS,
    horizon_minutes: int = DEFAULT_HORIZON_MINUTES,
):
    from sklearn.preprocessing import MinMaxScaler

    feature_frame = build_training_frame(
        raw_history_path=raw_history_path,
        bucket_seconds=bucket_seconds,
        tick_seconds=tick_seconds,
        horizon_minutes=horizon_minutes,
    )
    train_frame, val_frame, test_frame = split_by_time(feature_frame)
    if train_frame.empty or val_frame.empty or test_frame.empty:
        raise ValueError(
            "Not enough feature history to create train/validation/test splits. "
            "Generate more telemetry before training."
        )

    scaler = MinMaxScaler()
    scaler.fit(train_frame[MODEL_FEATURE_COLUMNS])

    for frame in (train_frame, val_frame, test_frame):
        transformed = scaler.transform(frame[MODEL_FEATURE_COLUMNS]).astype("float32")
        for column_index, column_name in enumerate(MODEL_FEATURE_COLUMNS):
            frame[column_name] = transformed[:, column_index]

    x_train, y_train, train_meta = create_sequences(train_frame, lookback_steps, MODEL_FEATURE_COLUMNS)
    x_val, y_val, val_meta = create_sequences(val_frame, lookback_steps, MODEL_FEATURE_COLUMNS)
    x_test, y_test, test_meta = create_sequences(test_frame, lookback_steps, MODEL_FEATURE_COLUMNS)

    if len(x_train) == 0 or len(x_val) == 0 or len(x_test) == 0:
        raise ValueError(
            "Feature windows were generated, but not enough clean sequences exist for all splits."
        )

    return {
        "feature_frame": feature_frame,
        "scaler": scaler,
        "x_train": x_train,
        "y_train": y_train,
        "x_val": x_val,
        "y_val": y_val,
        "x_test": x_test,
        "y_test": y_test,
        "train_meta": train_meta,
        "val_meta": val_meta,
        "test_meta": test_meta,
    }


def build_lstm_classifier(n_timesteps: int, n_features: int):
    tf = _ensure_tensorflow()
    model = tf.keras.Sequential(
        [
            tf.keras.layers.Input(shape=(n_timesteps, n_features)),
            tf.keras.layers.Masking(mask_value=-1.0),
            tf.keras.layers.LSTM(64, return_sequences=True),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.LSTM(32),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(32, activation="relu"),
            tf.keras.layers.Dense(1, activation="sigmoid"),
        ]
    )
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3),
        loss="binary_crossentropy",
        metrics=[
            tf.keras.metrics.AUC(name="auc_roc"),
            tf.keras.metrics.AUC(name="auc_pr", curve="PR"),
            tf.keras.metrics.Precision(name="precision"),
            tf.keras.metrics.Recall(name="recall"),
        ],
    )
    return model


def flatten_sequences(x):
    return x.reshape((x.shape[0], x.shape[1] * x.shape[2]))


def train_sklearn_baseline(x_train, y_train, x_val, y_val):
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier

    flat_train = flatten_sequences(x_train)
    flat_val = flatten_sequences(x_val)
    positive = int(y_train.sum())
    negative = int(len(y_train) - positive)
    pos_weight = min(20.0, max(1.0, negative / max(1, positive)))
    sample_weight = np.where(y_train == 1, pos_weight, 1.0)

    # Keep the fallback baseline single-process so it remains usable in
    # constrained Windows environments where worker-pool creation can fail.
    model = GradientBoostingClassifier(
        max_depth=6,
        n_estimators=250,
        learning_rate=0.05,
        random_state=42,
    )
    model.fit(flat_train, y_train, sample_weight=sample_weight)
    val_scores = model.predict_proba(flat_val)[:, 1]
    threshold = choose_threshold(y_val, val_scores)
    return model, threshold


def choose_threshold(y_true, y_scores) -> float:
    import numpy as np
    from sklearn.metrics import precision_recall_curve

    precision, recall, thresholds = precision_recall_curve(y_true, y_scores)
    if len(thresholds) == 0:
        return 0.5
    f1_scores = (2 * precision[:-1] * recall[:-1]) / (precision[:-1] + recall[:-1] + 1e-12)
    best_index = int(np.nanargmax(f1_scores))
    return float(thresholds[best_index])


def evaluate_classifier(y_true, y_scores, threshold: float) -> dict[str, float]:
    from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score, roc_auc_score

    y_pred = (y_scores >= threshold).astype(int)
    return {
        "roc_auc": float(roc_auc_score(y_true, y_scores)) if len(set(y_true.tolist())) > 1 else 0.0,
        "pr_auc": float(average_precision_score(y_true, y_scores)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
    }


def train_and_save(
    raw_history_path: str | Path,
    model_dir: str | Path = DEFAULT_MODEL_DIR,
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS,
    tick_seconds: int = DEFAULT_TICK_SECONDS,
    lookback_steps: int = DEFAULT_LOOKBACK_STEPS,
    horizon_minutes: int = DEFAULT_HORIZON_MINUTES,
    epochs: int = 15,
    batch_size: int = 64,
) -> dict[str, object]:
    dataset = prepare_datasets(
        raw_history_path=raw_history_path,
        bucket_seconds=bucket_seconds,
        tick_seconds=tick_seconds,
        lookback_steps=lookback_steps,
        horizon_minutes=horizon_minutes,
    )

    x_train = dataset["x_train"]
    y_train = dataset["y_train"]
    x_val = dataset["x_val"]
    y_val = dataset["y_val"]
    x_test = dataset["x_test"]
    y_test = dataset["y_test"]
    scaler = dataset["scaler"]

    backend = "tensorflow"
    history_keys: list[str] = []
    model_artifact = MODEL_FILENAME
    try:
        tf = _ensure_tensorflow()
    except RuntimeError:
        tf = None
        backend = "sklearn_flattened_baseline"
        print("TensorFlow is unavailable; training the sklearn flattened baseline instead.")

    if backend == "tensorflow":
        model = build_lstm_classifier(x_train.shape[1], x_train.shape[2])

        positive = int(y_train.sum())
        negative = int(len(y_train) - positive)
        pos_weight = min(20.0, max(1.0, negative / max(1, positive)))
        class_weight = {0: 1.0, 1: pos_weight}

        callbacks = [
            tf.keras.callbacks.EarlyStopping(
                monitor="val_auc_pr",
                mode="max",
                patience=4,
                restore_best_weights=True,
            )
        ]
        history = model.fit(
            x_train,
            y_train,
            validation_data=(x_val, y_val),
            epochs=epochs,
            batch_size=batch_size,
            class_weight=class_weight,
            callbacks=callbacks,
            verbose=1,
        )
        history_keys = list(history.history.keys())
        val_scores = model.predict(x_val, verbose=0).reshape(-1)
        threshold = choose_threshold(y_val, val_scores)
        test_scores = model.predict(x_test, verbose=0).reshape(-1)
        model_path = model_file(model_dir, backend=backend)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model.save(model_path)
    else:
        model_artifact = BASELINE_MODEL_FILENAME
        model, threshold = train_sklearn_baseline(x_train, y_train, x_val, y_val)
        test_scores = model.predict_proba(flatten_sequences(x_test))[:, 1]
        model_path = model_file(model_dir, backend=backend)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        with model_path.open("wb") as handle:
            pickle.dump(model, handle)

    test_metrics = evaluate_classifier(y_test, test_scores, threshold)
    model_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = {
        "model_version": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "model_type": "lstm_binary_failure_horizon_classifier" if backend == "tensorflow" else "sequence_baseline_classifier",
        "model_backend": backend,
        "python_version": sys.version,
        "lookback_steps": lookback_steps,
        "bucket_seconds": bucket_seconds,
        "tick_seconds": tick_seconds,
        "horizon_minutes": horizon_minutes,
        "feature_columns": MODEL_FEATURE_COLUMNS,
        "scaler": scaler_payload(scaler, MODEL_FEATURE_COLUMNS),
        "threshold": threshold,
        "train_sequences": int(len(x_train)),
        "validation_sequences": int(len(x_val)),
        "test_sequences": int(len(x_test)),
        "positive_train_sequences": int(y_train.sum()),
        "test_metrics": test_metrics,
        "history_keys": history_keys,
        "model_file": model_artifact,
    }
    save_manifest(model_dir, manifest)
    return manifest
