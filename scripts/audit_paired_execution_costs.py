#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_loader import load_cached_databento_range
from hydra.execution.two_leg_cost_audit import audit_two_leg_trade, summarize_cost_audit
from hydra.representations.paired_relative_value import PairedRelativeValueConfig, build_paired_residual_frame
from hydra.risk.pair_risk import integer_hedge_ratio
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.data_roles import DEFAULT_2024_PERIODS
from hydra.validation.lockbox_guard import enforce_data_access


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit paired ES/NQ execution-cost units without generating new paired strategies.")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--explicit-contract-map", required=True)
    parser.add_argument("--report-tag", default="paired_execution_cost_forensics_v1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = run_paired_cost_audit(
        dataset=args.dataset,
        schema=args.schema,
        symbols=args.symbols,
        roll_map_path=args.explicit_contract_map,
        report_tag=args.report_tag,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


def run_paired_cost_audit(
    *,
    dataset: str,
    schema: str,
    symbols: list[str],
    roll_map_path: str,
    report_tag: str,
) -> dict[str, Any]:
    roll_map = load_roll_map(roll_map_path)
    frames = _load_development_frames(dataset, schema, symbols)
    rows = []
    for period_name, frame in frames.items():
        paired = build_paired_residual_frame(frame, roll_map, PairedRelativeValueConfig(hedge_window=120, z_window=120))
        entries = paired.index[(paired["signal"] != 0) & (paired["signal"].shift(1).fillna(0) == 0)]
        for entry_i in list(entries)[:60]:
            exit_i = min(int(entry_i) + 30, len(paired) - 1)
            if exit_i <= entry_i:
                continue
            entry = paired.iloc[int(entry_i)]
            exit_ = paired.iloc[exit_i]
            hedge = integer_hedge_ratio(
                left_symbol="MNQ",
                right_symbol="MES",
                theoretical_ratio=abs(float(entry["hedge_ratio"])),
                left_price=float(entry["left_close"]),
                right_price=float(entry["right_close"]),
                prefer_micro=True,
            )
            rows.append(
                audit_two_leg_trade(
                    prototype_id=f"paired_audit_{period_name}_{int(entry_i)}",
                    left_symbol="MNQ",
                    right_symbol="MES",
                    left_quantity=hedge.left_quantity,
                    right_quantity=hedge.right_quantity,
                    theoretical_hedge_ratio=float(entry["hedge_ratio"]),
                    executable_hedge_ratio=hedge.executable_ratio,
                    left_entry=float(entry["left_close"]),
                    right_entry=float(entry["right_close"]),
                    left_exit=float(exit_["left_close"]),
                    right_exit=float(exit_["right_close"]),
                    direction=int(entry["signal"]),
                    entry_slippage_ticks=1.0,
                    exit_slippage_ticks=1.0,
                    legacy_slippage_bps=0.5,
                )
            )
    summary = summarize_cost_audit(rows)
    summary["legacy_reported_mean_cost_usd"] = 806.2583
    summary["cost_bug_existed"] = bool(summary["legacy_mislabeled_cost"]["mean"] > summary["corrected_cost"]["mean"] * 5)
    summary["paired_lane_unfairly_penalized_by_cost"] = summary["cost_bug_existed"]
    summary["rows_sample"] = [row.to_dict() for row in rows[:25]]
    summary["report_path"] = str(_write_report(summary, report_tag))
    return summary


def _load_development_frames(dataset: str, schema: str, symbols: list[str]) -> dict[str, pd.DataFrame]:
    out = {}
    for key in ("q1", "q2", "q3"):
        period = DEFAULT_2024_PERIODS[key]
        enforce_data_access(
            f"{period.start}:{period.end}",
            period.role,
            "scripts/audit_paired_execution_costs.py",
            [],
            "paired execution-cost unit forensics; no Q4 access",
            None,
        )
        out[key] = _load_governed_cached_frame(dataset, schema, symbols, period.start, period.end)
    return out


def _load_governed_cached_frame(dataset: str, schema: str, symbols: list[str], start: str, end: str) -> pd.DataFrame:
    cache_ranges = [("2024-01-01", "2024-03-31"), ("2024-04-01", "2024-07-01"), ("2024-07-01", "2024-10-01")]
    requested_start = pd.Timestamp(start, tz="UTC")
    requested_end = pd.Timestamp(end, tz="UTC")
    for cache_start, cache_end in cache_ranges:
        if pd.Timestamp(cache_start, tz="UTC") <= requested_start and pd.Timestamp(cache_end, tz="UTC") >= requested_end:
            frame = load_cached_databento_range(dataset, schema, symbols, cache_start, cache_end)
            timestamps = pd.to_datetime(frame["timestamp"], utc=True)
            return frame[(timestamps >= requested_start) & (timestamps < requested_end)].reset_index(drop=True)
    raise FileNotFoundError(f"No governed cache for {start} to {end}")


def _write_report(summary: dict[str, Any], tag: str) -> Path:
    stamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    path = project_path("reports", "execution_cost_forensics", f"paired_execution_cost_forensics_{stamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Paired Execution Cost Forensics {tag}",
        "",
        "Historical research only. No live trading approval.",
        "",
        f"- Classification: {summary['classification']}",
        f"- Corrected mean cost USD: {summary['corrected_cost']['mean']}",
        f"- Corrected median cost USD: {summary['corrected_cost']['median']}",
        f"- Legacy mislabeled mean cost USD: {summary['legacy_mislabeled_cost']['mean']}",
        f"- Cost bug existed: {summary['cost_bug_existed']}",
        "",
        "```json",
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


if __name__ == "__main__":
    raise SystemExit(main())
