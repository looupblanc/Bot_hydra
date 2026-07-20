#!/usr/bin/env python3
"""Run and atomically persist the bounded RB+HO -> MCL economic pilot."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.refinery_products_to_mcl_pilot import run_pilot


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output",
        default="reports/economic_evolution/refinery_products_to_mcl_pilot_v1/economic_result.json",
    )
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    result = run_pilot(root)
    output = Path(args.output)
    output = output if output.is_absolute() else root / output
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, output)
    print(json.dumps({
        "status": result["status"],
        "result_hash": result["result_hash"],
        "proposal_count": result["proposal_count"],
        "selected_candidate_ids": result["selected_candidate_ids"],
        "event_gate_passers": result["event_gate_passers"],
        "account_cell_count": result["account_cell_count"],
        "account_episode_count": result["account_episode_count"],
        "exact_normal_passes": result["exact_normal_passes"],
        "exact_stressed_passes": result["exact_stressed_passes"],
        "output": str(output),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
