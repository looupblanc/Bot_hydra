#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.registry.db import connect
from hydra.utils.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect HYDRA registry.")
    parser.add_argument("--top", type=int)
    parser.add_argument("--status")
    parser.add_argument("--family")
    parser.add_argument("--mll-risk", action="store_true")
    parser.add_argument("--summary", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    conn = connect(cfg["registry"]["path"])
    if args.summary:
        total = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
        print(f"Total candidates: {total}")
        for row in conn.execute("SELECT validation_status, COUNT(*) c FROM candidates GROUP BY validation_status ORDER BY c DESC"):
            print(f"{row['validation_status']}: {row['c']}")
        print("Rejection reasons:")
        for row in conn.execute("SELECT rejection_reason, COUNT(*) c FROM candidates WHERE rejection_reason IS NOT NULL GROUP BY rejection_reason ORDER BY c DESC"):
            print(f"- {row['rejection_reason']}: {row['c']}")
        return
    sql = "SELECT candidate_id,family,symbol,timeframe,net_profit,max_drawdown,profit_factor,sharpe,trade_count,mll_breached,mll_buffer,robustness_score,validation_status,rejection_reason FROM candidates"
    clauses = []
    params = []
    if args.status:
        clauses.append("validation_status=?")
        params.append(args.status)
    if args.family:
        clauses.append("family=?")
        params.append(args.family)
    if args.mll_risk:
        clauses.append("(mll_breached=1 OR mll_buffer < ?)")
        params.append(cfg["propfirm"]["min_mll_buffer"])
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY robustness_score DESC, net_profit DESC"
    if args.top:
        sql += f" LIMIT {int(args.top)}"
    for row in conn.execute(sql, params):
        print(
            f"{row['candidate_id']} {row['validation_status']} {row['family']} {row['symbol']} {row['timeframe']} "
            f"net={row['net_profit']:.2f} dd={row['max_drawdown']:.2f} pf={row['profit_factor']:.2f} "
            f"sharpe={row['sharpe']:.2f} trades={row['trade_count']} buffer={row['mll_buffer']:.2f} "
            f"robust={row['robustness_score']:.3f} reason={row['rejection_reason']}"
        )


if __name__ == "__main__":
    main()
