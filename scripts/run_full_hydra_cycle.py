#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.databento_loader import load_cached_databento_range, validate_ohlcv_frame
from hydra.data.loader import load_market_data
from hydra.factory.diagnostics import DIAGNOSTIC_WARNING, apply_diagnostic_relaxed_config, diagnostic_bars, run_mode_label
from hydra.factory.expansion import evaluate_candidate_on_frame
from hydra.factory.risk_compression import run_risk_compression
from hydra.features.market_state import build_market_state
from hydra.registry.db import connect
from hydra.registry.reports import build_markdown_report
from hydra.strategies.generator import generate_candidates
from hydra.utils.config import load_config, project_path
from hydra.utils.logging import setup_logging
from hydra.utils.time import utc_now_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full HYDRA research cycle end-to-end.")
    parser.add_argument("--candidates", type=int, default=500)
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--real-data", action="store_true")
    parser.add_argument("--data-provider", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--timeframes", nargs="+", default=None)
    parser.add_argument("--diagnostic-relaxed", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-buffer", type=float, default=500)
    parser.add_argument("--target-buffer", type=float, default=2500)
    parser.add_argument("--max-strategies", type=int, default=10)
    parser.add_argument("--reset-registry", action="store_true")
    parser.add_argument("--skip-compile", action="store_true")
    parser.add_argument("--report-tag")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.strict and args.diagnostic_relaxed:
        print("ERROR: --strict and --diagnostic-relaxed are mutually exclusive.", file=sys.stderr)
        return 2
    if args.synthetic and args.real_data:
        print("ERROR: --synthetic and --real-data are mutually exclusive.", file=sys.stderr)
        return 2
    cfg = load_config()
    setup_logging(cfg["logging"]["folder"], cfg["logging"]["level"])
    diagnostic_relaxed = bool(args.synthetic and args.diagnostic_relaxed and not args.strict)
    if diagnostic_relaxed:
        cfg = apply_diagnostic_relaxed_config(cfg)
    if args.real_data and args.diagnostic_relaxed:
        print("ERROR: Real-data runs cannot use --diagnostic-relaxed.", file=sys.stderr)
        return 2
    if args.real_data and not args.strict:
        print("ERROR: Real-data runs must use --strict.", file=sys.stderr)
        return 2
    if args.real_data and (args.data_provider or "databento") != "databento":
        print("ERROR: Only --data-provider databento is supported for real-data runs.", file=sys.stderr)
        return 2
    if not args.synthetic and not args.real_data:
        print("ERROR: Choose --synthetic for diagnostics or --real-data --data-provider databento for historical research. No live trading or broker access is used.", file=sys.stderr)
        return 2

    try:
        if not args.skip_compile:
            run_compile()
        archive_path = reset_registry(cfg["registry"]["path"], args.report_tag) if args.reset_registry else None
        conn = connect(cfg["registry"]["path"])
        symbols = args.symbols or cfg["markets"]["symbols"]
        timeframes = args.timeframes or (["1m"] if args.real_data else cfg["markets"]["timeframes"])
        candidates = generate_candidates(args.candidates, symbols, timeframes, args.seed, diagnostic_relaxed=diagnostic_relaxed)
        print(f"V3 expansion: generated {len(candidates)} candidates")
        counts, data_stats = run_v3(conn, candidates, cfg, args, symbols, timeframes, diagnostic_relaxed)
        print_registry_summary(conn)
        selected = run_risk_compression(conn, args.min_buffer, args.target_buffer, args.max_strategies)
        print("HYDRA V4 risk compression complete")
        print(f"Portfolio selections/promotions: {len(selected)}")
        qualified_count = count_qualified(conn)
        warnings = [DIAGNOSTIC_WARNING] if args.synthetic else ["Historical real-data research only. No live trading or broker execution was used."]
        next_action = (
            "Diagnose strict real-data rejections, then expand sample length and add out-of-sample Databento validation before any paper/shadow validation."
            if args.real_data and qualified_count == 0
            else "Run longer out-of-sample real-data and shadow/paper validation only after repeated strict passes."
            if args.real_data
            else "Add Databento historical futures ingestion and strict no-lookahead tests before expanding real-data validation."
        )
        metadata = {
            "run_mode": "real data strict" if args.real_data else run_mode_label(args.synthetic, diagnostic_relaxed),
            "data_provider": args.data_provider or ("databento" if args.real_data else "synthetic"),
            "dataset": args.dataset or cfg.get("data", {}).get("databento", {}).get("dataset"),
            "schema": args.schema or cfg.get("data", {}).get("databento", {}).get("schema"),
            "requested_start": args.start,
            "requested_end": args.end,
            "actual_start": data_stats.get("actual_start") if data_stats else None,
            "actual_end": data_stats.get("actual_end") if data_stats else None,
            "bars_per_symbol": data_stats.get("rows_by_symbol", {}) if data_stats else {},
            "missing_intervals": data_stats.get("missing_intervals", {}) if data_stats else {},
            "candidate_count": args.candidates,
            "symbols": symbols,
            "timeframes": timeframes,
            "seed": args.seed,
            "report_tag": args.report_tag,
            "v4_selected_portfolio_count": len(selected),
            "warnings": warnings,
            "next_recommended_action": next_action,
        }
        report_path = build_markdown_report(conn, cfg["reports"]["folder"], metadata)
        summary = build_summary(conn, args, symbols, selected, report_path, archive_path, metadata)
        summary_path = write_summary_json(summary, cfg["reports"]["folder"], args.report_tag)
        print_final_summary(summary, summary_path)
        return 0
    except Exception as exc:
        print(f"ERROR: full HYDRA cycle failed: {exc}", file=sys.stderr)
        return 1


def run_compile() -> None:
    print("Compile: python -m compileall hydra scripts")
    result = subprocess.run([sys.executable, "-m", "compileall", "hydra", "scripts"], cwd=project_path(), text=True)
    if result.returncode != 0:
        raise RuntimeError("compileall failed")


def reset_registry(db_path: str, report_tag: str | None) -> str | None:
    path = project_path(db_path)
    if not path.exists():
        print(f"Registry reset: no existing registry at {path}")
        return None
    archive_dir = path.parent / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    tag = f"_{report_tag}" if report_tag else ""
    archive_path = archive_dir / f"{path.stem}_{utc_now_iso().replace(':', '').replace('+', 'Z')}{tag}{path.suffix}"
    shutil.move(str(path), archive_path)
    print(f"Registry reset: archived old registry to {archive_path}")
    return str(archive_path)


def run_v3(conn, candidates, cfg: dict[str, Any], args: argparse.Namespace, symbols: list[str], timeframes: list[str], diagnostic_relaxed: bool) -> tuple[Counter[str], dict[str, Any]]:
    bars = diagnostic_bars(cfg) if diagnostic_relaxed else 1500
    market_state = {}
    data_stats: dict[str, Any] = {}
    if args.real_data:
        db_cfg = cfg.get("data", {}).get("databento", {})
        dataset = args.dataset or db_cfg.get("dataset")
        schema = args.schema or db_cfg.get("schema")
        start = args.start or db_cfg.get("start_date")
        end = args.end or db_cfg.get("end_date")
        cache_folder = db_cfg.get("cache_folder", "data/cache/databento")
        raw_all = load_cached_databento_range(dataset, schema, symbols, start, end, cache_folder=cache_folder, timeframe=timeframes[0])
        validation = validate_ohlcv_frame(raw_all, timeframe=timeframes[0])
        timestamps = pd.to_datetime(raw_all["timestamp"], utc=True)
        data_stats = {
            **validation,
            "actual_start": timestamps.min().isoformat(),
            "actual_end": timestamps.max().isoformat(),
        }
        print(f"Real-data cache loaded: {len(raw_all)} rows from {data_stats['actual_start']} to {data_stats['actual_end']}")
        print(f"Bars per symbol: {validation['rows_by_symbol']}")
        for symbol in symbols:
            raw = raw_all[raw_all["symbol"] == symbol].reset_index(drop=True)
            if raw.empty:
                raise FileNotFoundError(f"Databento cache has no rows for {symbol}.")
            for timeframe in timeframes:
                market_state[(symbol, timeframe)] = build_market_state(raw)
    else:
        for symbol in symbols:
            for timeframe in timeframes:
                raw = load_market_data(symbol, timeframe, args.synthetic, args.seed, bars=bars, diagnostic_relaxed=diagnostic_relaxed)
                market_state[(symbol, timeframe)] = build_market_state(raw)
    counts: Counter[str] = Counter()
    existing_curves: dict = {}
    for i, candidate in enumerate(candidates, start=1):
        df = market_state[(candidate.symbol, candidate.timeframe)]
        status = evaluate_candidate_on_frame(conn, candidate, df, cfg, args.seed + i, existing_curves)
        counts[status] += 1
        if i % 250 == 0 or i == len(candidates):
            print(f"V3 progress: {i}/{len(candidates)} candidates evaluated")
    return counts, data_stats


def print_registry_summary(conn) -> None:
    print("Registry summary:")
    total = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    print(f"Total candidates: {total}")
    for row in conn.execute("SELECT validation_status, COUNT(*) c FROM candidates GROUP BY validation_status ORDER BY c DESC"):
        print(f"{row['validation_status']}: {row['c']}")
    print("Rejection reasons:")
    for row in conn.execute("SELECT rejection_reason, COUNT(*) c FROM candidates WHERE rejection_reason IS NOT NULL GROUP BY rejection_reason ORDER BY c DESC"):
        print(f"- {row['rejection_reason']}: {row['c']}")


def count_qualified(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status IN ('QUALIFIED','PROMOTED_TO_PORTFOLIO')").fetchone()[0]


def build_summary(conn, args: argparse.Namespace, symbols: list[str], selected: list[str], report_path: Path, archive_path: str | None, metadata: dict[str, Any]) -> dict[str, Any]:
    status_distribution = {row["validation_status"]: row["c"] for row in conn.execute("SELECT validation_status, COUNT(*) c FROM candidates GROUP BY validation_status ORDER BY c DESC")}
    rejection_reasons = {row["rejection_reason"]: row["c"] for row in conn.execute("SELECT rejection_reason, COUNT(*) c FROM candidates WHERE rejection_reason IS NOT NULL GROUP BY rejection_reason ORDER BY c DESC")}
    top_families = {row["family"]: row["c"] for row in conn.execute("SELECT family, COUNT(*) c FROM candidates GROUP BY family ORDER BY c DESC")}
    correlation_clusters = {row["correlation_cluster"]: row["c"] for row in conn.execute("SELECT correlation_cluster, COUNT(*) c FROM candidates WHERE correlation_cluster IS NOT NULL GROUP BY correlation_cluster ORDER BY c DESC")}
    mll = conn.execute("SELECT MIN(mll_buffer), AVG(mll_buffer), SUM(mll_breached) FROM candidates").fetchone()
    return {
        "run_mode": metadata["run_mode"],
        "data_provider": metadata.get("data_provider"),
        "dataset": metadata.get("dataset"),
        "schema": metadata.get("schema"),
        "requested_start": metadata.get("requested_start"),
        "requested_end": metadata.get("requested_end"),
        "actual_start": metadata.get("actual_start"),
        "actual_end": metadata.get("actual_end"),
        "bars_per_symbol": metadata.get("bars_per_symbol", {}),
        "missing_intervals": metadata.get("missing_intervals", {}),
        "candidate_count": args.candidates,
        "symbols": symbols,
        "seed": args.seed,
        "total_candidates": conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0],
        "status_distribution": status_distribution,
        "rejection_reasons": rejection_reasons,
        "qualified_count": status_distribution.get("QUALIFIED", 0) + status_distribution.get("PROMOTED_TO_PORTFOLIO", 0),
        "v4_selected_portfolio_count": len(selected),
        "top_families": top_families,
        "correlation_clusters": correlation_clusters,
        "mll_summary": {
            "min_buffer": float(mll[0] or 0.0),
            "avg_buffer": float(mll[1] or 0.0),
            "breaches": int(mll[2] or 0),
        },
        "warnings": metadata["warnings"],
        "report_path": str(report_path),
        "archived_registry_path": archive_path,
        "next_recommended_action": metadata["next_recommended_action"],
    }


def write_summary_json(summary: dict[str, Any], output_folder: str, report_tag: str | None) -> Path:
    folder = project_path(output_folder)
    folder.mkdir(parents=True, exist_ok=True)
    tag = f"_{report_tag}" if report_tag else ""
    path = folder / f"hydra_cycle_summary_{utc_now_iso().replace(':', '').replace('+', 'Z')}{tag}.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def print_final_summary(summary: dict[str, Any], summary_path: Path) -> None:
    print("Final summary:")
    print(f"Run mode: {summary['run_mode']}")
    print(f"Total candidates: {summary['total_candidates']}")
    print(f"Status distribution: {summary['status_distribution']}")
    print(f"Rejection reasons: {summary['rejection_reasons']}")
    print(f"Qualified count: {summary['qualified_count']}")
    print(f"V4 selected portfolio count: {summary['v4_selected_portfolio_count']}")
    print(f"Report: {summary['report_path']}")
    print(f"Summary JSON: {summary_path}")


if __name__ == "__main__":
    raise SystemExit(main())
