#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import DatabentoBudgetConfig, read_ledger, write_budget_summary
from hydra.utils.config import project_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Databento new-spend budget ledger.")
    parser.add_argument("--ledger", default="reports/data_budget/databento_spend_ledger.jsonl")
    parser.add_argument("--summary", default="reports/data_budget/databento_budget_summary.md")
    parser.add_argument("--hard-cap-usd", type=float, default=100.0)
    parser.add_argument("--safety-ceiling-usd", type=float, default=98.0)
    args = parser.parse_args()
    cfg = DatabentoBudgetConfig(
        hard_cap_usd=args.hard_cap_usd,
        safety_ceiling_usd=args.safety_ceiling_usd,
        ledger_path=args.ledger,
        summary_path=args.summary,
    )
    path = write_budget_summary(cfg)
    rows = read_ledger(project_path(args.ledger))
    print(json.dumps({"ledger_records": len(rows), "summary_path": str(path)}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

