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
    DEFAULT_FEATURE_HISTORY_PATH,
    DEFAULT_HISTORY_PATH,
    DEFAULT_TICK_SECONDS,
)
from ai_secure_cloud_log_detection.features import aggregate_raw_events, load_raw_history  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export base feature windows from raw JSONL history.")
    parser.add_argument("--raw-history", type=Path, default=DEFAULT_HISTORY_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_FEATURE_HISTORY_PATH)
    parser.add_argument("--bucket-seconds", type=int, default=DEFAULT_BUCKET_SECONDS)
    parser.add_argument("--tick-seconds", type=int, default=DEFAULT_TICK_SECONDS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_frame = load_raw_history(args.raw_history)
    feature_frame = aggregate_raw_events(
        raw_frame,
        bucket_seconds=args.bucket_seconds,
        tick_seconds=args.tick_seconds,
    )
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", encoding="utf-8") as handle:
        for record in feature_frame.to_dict(orient="records"):
            record["window_start"] = record["window_start"].isoformat()
            record["window_end"] = record["window_end"].isoformat()
            handle.write(json.dumps(record) + "\n")
    print(f"Wrote {len(feature_frame)} feature windows to {args.output_path}")


if __name__ == "__main__":
    main()
