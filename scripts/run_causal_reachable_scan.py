#!/usr/bin/env python3
"""Run the bounded future-dependency scan over the frozen reachable surface."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hydra.validation.causal_reachable_scan import (
    DEFAULT_OUTPUT_PATH,
    SCAN_PASS,
    write_causal_reachable_scan,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument(
        "--created-at",
        help="Optional ISO-8601 timestamp for deterministic controlled runs.",
    )
    arguments = parser.parse_args()
    created_at = (
        datetime.fromisoformat(arguments.created_at.replace("Z", "+00:00"))
        if arguments.created_at
        else None
    )
    receipt = write_causal_reachable_scan(
        repository_root=Path(arguments.repository_root),
        output_path=arguments.output,
        created_at=created_at,
    )
    print(json.dumps(receipt, indent=2, sort_keys=True, allow_nan=False))
    return 0 if receipt["status"] == SCAN_PASS else 2


if __name__ == "__main__":
    raise SystemExit(main())
