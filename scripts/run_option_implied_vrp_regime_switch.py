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

from hydra.research.option_implied_vrp_regime_switch import run_tripwire


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the frozen option-implied VRP action-switch tripwire")
    parser.add_argument("--root", default=str(PROJECT))
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[name] = "1"
    result = run_tripwire(args.root, args.output_dir)
    evaluation = result["evaluation"]
    print(json.dumps({
        "status": evaluation["status"],
        "opportunity_counts": evaluation["opportunity_counts"],
        "best_static_action": evaluation["best_static_action"],
        "gate": evaluation["gate"],
        "runtime_seconds": result["runtime_seconds"],
        "result_hash": result["result_hash"],
        "result_path": result["result_path"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
