#!/usr/bin/env python3
"""Run and atomically persist the bounded NG storage-shock tripwire."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.natural_gas_storage_shock_tripwire import run_tripwire


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output",
        default="reports/research_tripwires/natural_gas_storage_shock_v1/economic_result.json",
    )
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    result = run_tripwire(root)
    output = Path(args.output)
    output = output if output.is_absolute() else root / output
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, output)
    print(
        json.dumps(
            {
                "status": result["status"],
                "proposal_count": result["proposal_count"],
                "discovery_eligible_count": result["discovery_eligible_count"],
                "selected_candidate_ids": result["selected_candidate_ids"],
                "event_gate_passer_ids": result["event_gate_passer_ids"],
                "account_matrix_executed": result["account_matrix_executed"],
                "runtime_seconds": result["runtime_seconds"],
                "result_hash": result["result_hash"],
                "output": str(output),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
