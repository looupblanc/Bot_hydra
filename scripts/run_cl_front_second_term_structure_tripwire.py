#!/usr/bin/env python3
"""Audit or run the frozen CL front/second economic tripwire."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Numeric runtimes inspect these variables during import.  Set them before the
# research module imports NumPy/Pandas so a standalone invocation cannot
# oversubscribe the bounded worker contract.
for name in (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[name] = "1"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.cl_front_second_term_structure_economic_runner import (
    audit_tripwire_inputs,
    run_tripwire,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default="config/research/cl_front_second_term_structure_tripwire_v1.json")
    parser.add_argument(
        "--receipt",
        default="reports/data_access/cl_front_second_term_structure_acquisition_receipt.json",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--audit-only", action="store_true")
    mode.add_argument("--run", action="store_true")
    parser.add_argument(
        "--output",
        help="optional research-result JSON path; never writes mission/runtime state",
    )
    args = parser.parse_args()
    result = (
        audit_tripwire_inputs(args.root, card_path=args.card, receipt_path=args.receipt)
        if args.audit_only
        else run_tripwire(args.root, card_path=args.card, receipt_path=args.receipt)
    )
    rendered = json.dumps(result, indent=2, sort_keys=True, allow_nan=False, default=str) + "\n"
    if args.output:
        destination = Path(args.output)
        if not destination.is_absolute():
            destination = Path(args.root).resolve() / destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, destination)
        print(json.dumps({"status": result["status"], "result_hash": result.get("result_hash") or result.get("audit_hash"), "output": str(destination)}, sort_keys=True))
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
