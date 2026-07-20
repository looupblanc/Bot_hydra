#!/usr/bin/env python3
"""Run isolated PnL-state XFA alternatives and survivor validation."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from hydra.research.pnl_state_xfa_diagnostic import (
    DYNAMIC_SURVIVOR_IDS,
    build_dynamic_survivor_chronological_validation,
    build_pnl_state_xfa_diagnostic,
    build_xfa_decision_summary,
)


DEFAULT_ROOT = Path("reports/economic_evolution/pnl_state_xfa_diagnostic_v1")


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
    parser.add_argument("--dynamic-only", action="store_true")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output_dir if args.output_dir.is_absolute() else root / args.output_dir
    suffix = "dynamic_3" if args.dynamic_only else "all_clean_handoffs"
    xfa_path = output / f"xfa_{suffix}.json"
    validation_path = output / "dynamic_survivor_validation.json"
    if args.summary_only:
        if args.dynamic_only:
            parser.error("--summary-only requires the complete all-handoff result")
        xfa = json.loads(xfa_path.read_text(encoding="utf-8"))
        validation = json.loads(validation_path.read_text(encoding="utf-8"))
    else:
        scope = DYNAMIC_SURVIVOR_IDS if args.dynamic_only else None
        xfa = build_pnl_state_xfa_diagnostic(root, policy_ids=scope)
        validation = build_dynamic_survivor_chronological_validation(root)
        _write(xfa_path, xfa)
        _write(validation_path, validation)
    summary = build_xfa_decision_summary(xfa, validation)
    summary_path = output / "xfa_decision_summary.json"
    _write(summary_path, summary)
    print(
        json.dumps(
            {
                "xfa_path": str(xfa_path),
                "xfa_result_hash": xfa["result_hash"],
                "counts": xfa["counts"],
                "validation_path": str(validation_path),
                "validation_hash": validation["result_hash"],
                "validation_status": validation["status"],
                "decision_summary_path": str(summary_path),
                "decision_summary_hash": summary["result_hash"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
