#!/usr/bin/env python3
"""Run and persist the frozen soybean-crush causal economic tripwire."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hydra.research.soybean_crush_structural_value_router import run_tripwire


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument(
        "--output",
        default="reports/research_tripwires/soybean_crush_structural_value_router_v1/economic_result.json",
    )
    args = parser.parse_args()
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    root = Path(args.root).resolve()
    result = run_tripwire(root)
    output = Path(args.output)
    output = output if output.is_absolute() else root / output
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, output)
    print(json.dumps({
        "status": result["status"],
        "proposal_count": result["proposal_count"],
        "selected_candidate_ids": result["selected_candidate_ids"],
        "validation_passers": result["validation_event_gate_passer_ids"],
        "final_passers": result["final_development_event_gate_passer_ids"],
        "result_hash": result["result_hash"],
        "output": str(output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
