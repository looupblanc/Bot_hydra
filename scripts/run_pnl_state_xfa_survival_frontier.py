#!/usr/bin/env python3
"""Run the isolated post-payout survival frontier."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from hydra.research.pnl_state_xfa_survival_frontier import (
    build_pnl_state_xfa_survival_frontier,
)


DEFAULT_OUTPUT = Path(
    "reports/economic_evolution/xfa_post_payout_survival_frontier_v2/"
    "economic_result.json"
)


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    output = (
        arguments.output
        if arguments.output.is_absolute()
        else root / arguments.output
    )
    result = build_pnl_state_xfa_survival_frontier(root)
    _write(output, result)
    print(
        json.dumps(
            {
                "output": str(output),
                "result_hash": result["result_hash"],
                "eligible_cells": result["eligible_cell_count"],
                "evaluations": result["evaluation_count"],
                "payout_events": result["canonical_payout_event_count"],
                "baseline": result["baseline_reconciliation"]["status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
