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
from hydra.research.option_settlement_surface_tripwire import run_tripwire


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the frozen option-settlement teacher/student tripwire")
    parser.add_argument("--root", default=str(PROJECT))
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ.setdefault(name, "1")
    result = run_tripwire(args.root, args.output_dir)
    summary = {
        "status": result["evaluation"]["status"],
        "opportunity_counts": result["evaluation"]["opportunity_counts"],
        "teacher_gate_pass": result["evaluation"]["teacher_gate_pass"],
        "student_gate_pass": result["evaluation"]["student_gate_pass"],
        "result_hash": result["result_hash"],
        "result_path": result["result_path"],
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
