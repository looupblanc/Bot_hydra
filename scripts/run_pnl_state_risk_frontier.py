#!/usr/bin/env python3
"""Run and persist the isolated pass-observed PnL-state sizing frontier."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from hydra.research.pnl_state_risk_frontier import (
    build_pnl_state_risk_frontier,
    continue_contract_only_pnl_state_frontier,
)


DEFAULT_OUTPUT = Path(
    "reports/economic_evolution/pnl_state_risk_frontier_v1/economic_result.json"
)
DEFAULT_RECONCILED_OUTPUT = Path(
    "reports/economic_evolution/pnl_state_risk_frontier_v1/"
    "economic_result_reconciled.json"
)
DEFAULT_PARTIAL_OUTPUT = Path(
    "reports/economic_evolution/pnl_state_risk_frontier_v1/"
    "immutable_partial_24_reconciled.json"
)
DEFAULT_TARGETED_OUTPUT = Path(
    "reports/economic_evolution/pnl_state_risk_frontier_v1/"
    "targeted_contract_only_20_result.json"
)


def _write_atomic(output: Path, result: dict) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, output)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--continue-contract-only", action="store_true")
    parser.add_argument("--partial-output", type=Path, default=DEFAULT_PARTIAL_OUTPUT)
    parser.add_argument("--targeted-output", type=Path, default=DEFAULT_TARGETED_OUTPUT)
    args = parser.parse_args()
    root = args.root.resolve()
    if args.continue_contract_only:
        output_arg = (
            DEFAULT_RECONCILED_OUTPUT if args.output == DEFAULT_OUTPUT else args.output
        )
        output = output_arg if output_arg.is_absolute() else root / output_arg
        partial_output = (
            args.partial_output
            if args.partial_output.is_absolute()
            else root / args.partial_output
        )
        targeted_output = (
            args.targeted_output
            if args.targeted_output.is_absolute()
            else root / args.targeted_output
        )
        artifacts = continue_contract_only_pnl_state_frontier(root)
        _write_atomic(partial_output, artifacts["partial"])
        _write_atomic(targeted_output, artifacts["targeted"])
        result = artifacts["reconciled"]
        _write_atomic(output, result)
    else:
        output = args.output if args.output.is_absolute() else root / args.output
        result = build_pnl_state_risk_frontier(root)
        _write_atomic(output, result)
    print(
        json.dumps(
            {
                "output": str(output),
                "result_hash": result["result_hash"],
                "status": result["status"],
                "inventory": result["inventory"],
                "aggregate": result["aggregate"],
                "counters": result["counters"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
