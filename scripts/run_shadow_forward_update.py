#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import DatabentoBudgetConfig
from hydra.shadow.databento_forward_feed import run_databento_forward_update


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run one bounded Databento append-only shadow update. This command "
            "has market-data read capability and no broker/order capability."
        )
    )
    parser.add_argument("--boundary-manifest", required=True)
    parser.add_argument("--boundary-manifest-sha256", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--state-dir", default="shadow/state")
    parser.add_argument("--contract-map-dir", default="data/cache/contract_maps")
    parser.add_argument("--code-commit", required=True)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--minimum-reserve-usd", type=float, default=30.0)
    parser.add_argument("--maximum-incremental-cost-usd", type=float, default=0.10)
    args = parser.parse_args()
    observed = (
        datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
        if args.as_of
        else None
    )
    result = run_databento_forward_update(
        args.output_dir,
        boundary_manifest_path=args.boundary_manifest,
        boundary_manifest_sha256=args.boundary_manifest_sha256,
        state_dir=args.state_dir,
        contract_map_dir=args.contract_map_dir,
        budget=DatabentoBudgetConfig(),
        code_commit=args.code_commit,
        minimum_reserve_usd=args.minimum_reserve_usd,
        maximum_incremental_cost_usd=args.maximum_incremental_cost_usd,
        now=observed,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
