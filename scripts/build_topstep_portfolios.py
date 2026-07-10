#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.portfolio.remediation_portfolio import build_remediation_portfolio_candidates


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument("--output", default="reports/portfolio/topstep_portfolios.json")
    args = parser.parse_args()
    conn = sqlite3.connect(args.registry)
    conn.row_factory = sqlite3.Row
    rows = [dict(row) for row in conn.execute("SELECT * FROM candidates")]
    portfolios = build_remediation_portfolio_candidates(rows)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(portfolios, indent=2, sort_keys=True), encoding="utf-8")
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
