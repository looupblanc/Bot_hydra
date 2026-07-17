#!/usr/bin/env python3
"""Run/resume the single frozen causal target-velocity manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.production.causal_target_velocity_runtime import (
    read_causal_target_velocity_status,
    run_causal_target_velocity_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run/resume HYDRA Causal Target Velocity 0028."
    )
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--contract-map", type=Path)
    parser.add_argument("--cache-root", type=Path)
    parser.add_argument("--status-only", action="store_true")
    parser.add_argument(
        "--stop-after",
        choices=("FAST_SCREEN", "FIRST_HALVING"),
        help="Test-only checkpoint; requires HYDRA_PRODUCTION_TEST_MODE=1.",
    )
    args = parser.parse_args()
    if args.status_only:
        value = read_causal_target_velocity_status(args.manifest)
    else:
        if args.contract_map is None or args.cache_root is None:
            parser.error("--contract-map and --cache-root are required for execution")
        value = run_causal_target_velocity_manifest(
            args.manifest,
            contract_map_path=args.contract_map,
            cache_root=args.cache_root,
            stop_after=args.stop_after,
        )
    print(json.dumps(value, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
