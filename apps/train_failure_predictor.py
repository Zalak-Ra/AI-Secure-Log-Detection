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
    DEFAULT_BUCKET_SECONDS,
    DEFAULT_HISTORY_PATH,
    DEFAULT_HORIZON_MINUTES,
    DEFAULT_LOOKBACK_STEPS,
    DEFAULT_MODEL_DIR,
    DEFAULT_TICK_SECONDS,
)
from ai_secure_cloud_log_detection.training import prepare_datasets, train_and_save  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the LSTM failure horizon predictor.")
    parser.add_argument("--raw-history", type=Path, default=DEFAULT_HISTORY_PATH)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--bucket-seconds", type=int, default=DEFAULT_BUCKET_SECONDS)
    parser.add_argument("--tick-seconds", type=int, default=DEFAULT_TICK_SECONDS)
    parser.add_argument("--lookback-steps", type=int, default=DEFAULT_LOOKBACK_STEPS)
    parser.add_argument("--horizon-minutes", type=int, default=DEFAULT_HORIZON_MINUTES)
    parser.add_argument("--epochs", type=int, default=15)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--prepare-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.prepare_only:
        datasets = prepare_datasets(
            raw_history_path=args.raw_history,
            bucket_seconds=args.bucket_seconds,
            tick_seconds=args.tick_seconds,
            lookback_steps=args.lookback_steps,
            horizon_minutes=args.horizon_minutes,
        )
        summary = {
            "train_sequences": int(len(datasets["x_train"])),
            "validation_sequences": int(len(datasets["x_val"])),
            "test_sequences": int(len(datasets["x_test"])),
            "positive_train_sequences": int(datasets["y_train"].sum()),
            "positive_validation_sequences": int(datasets["y_val"].sum()),
            "positive_test_sequences": int(datasets["y_test"].sum()),
        }
        print(json.dumps(summary, indent=2))
        return

    manifest = train_and_save(
        raw_history_path=args.raw_history,
        model_dir=args.model_dir,
        bucket_seconds=args.bucket_seconds,
        tick_seconds=args.tick_seconds,
        lookback_steps=args.lookback_steps,
        horizon_minutes=args.horizon_minutes,
        epochs=args.epochs,
        batch_size=args.batch_size,
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
