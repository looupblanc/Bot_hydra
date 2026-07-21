#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from hydra.research.treasury_auction_demand_shock_causal import run_tripwire, write_outputs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(PROJECT))
    parser.add_argument(
        "--manifest", default="config/research/treasury_auction_demand_shock_causal_v1.json"
    )
    parser.add_argument(
        "--output-dir", default="reports/research_tripwires/treasury_auction_demand_shock_causal_v1"
    )
    args = parser.parse_args()
    root = Path(args.root).resolve()
    result, events, windows = run_tripwire(root, Path(args.manifest))
    paths = write_outputs(result, events, windows, root / args.output_dir)
    print(
        json.dumps(
            {
                "status": result["status"],
                "causal_feature_event_count": result["causal_feature_event_count"],
                "action_count": result["action_count"],
                "gate_pass": result["gate_pass"],
                "result_hash": result["result_hash"],
                "paths": paths,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    for variable in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[variable] = "1"
    raise SystemExit(main())
