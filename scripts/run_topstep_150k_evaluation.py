#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.backtest.engine import run_backtest
from hydra.backtest.metrics import max_drawdown
from hydra.data.databento_loader import load_cached_ohlcv, request_from_config, validate_ohlcv_frame
from hydra.features.market_state import build_market_state
from hydra.propfirm.topstep_150k import InternalRiskOverlay, Topstep150KConfig, evaluate_topstep_150k, trades_to_topstep_daily
from hydra.registry.candidates import upsert_topstep_candidate
from hydra.registry.db import connect
from hydra.strategies.generator import generate_candidates
from hydra.utils.config import load_config, project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.no_leak import audit_no_lookahead


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Topstep 150K cache-only real-data evaluation.")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--use-cache-only", action="store_true")
    parser.add_argument("--candidates", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=45)
    parser.add_argument("--account-size", type=float, default=150000)
    parser.add_argument("--profit-target", type=float, default=9000)
    parser.add_argument("--mll", type=float, default=4500)
    parser.add_argument("--no-daily-loss-limit", action="store_true")
    parser.add_argument("--simulate-funded", action="store_true")
    parser.add_argument("--simulate-payouts", action="store_true")
    parser.add_argument("--max-strategies", type=int, default=10)
    parser.add_argument("--report-tag", default="topstep_150k")
    parser.add_argument("--registry-path", default="registry/hydra_registry.db")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.use_cache_only:
        print("ERROR: This runner is intentionally cache-only. Pass --use-cache-only.", file=sys.stderr)
        return 2
    cfg = load_config()
    try:
        run_compile()
        conn = reset_and_connect(args.registry_path, args.report_tag)
        raw = load_cached_range(args, cfg)
        data_validation = validate_ohlcv_frame(raw, timeframe="1m")
        market_state = build_market_states(raw, args.symbols)
        candidates = generate_candidates(args.candidates, args.symbols, ["1m"], args.seed, topstep_mode=True)
        topstep_cfg = Topstep150KConfig(
            account_size=args.account_size,
            combine_starting_balance=args.account_size,
            combine_profit_target=args.profit_target,
            combine_max_loss_limit=args.mll,
            no_daily_loss_limit=bool(args.no_daily_loss_limit),
        )
        counts, report_rows = evaluate_candidates(conn, candidates, market_state, topstep_cfg, args)
        enforce_portfolio_cap(conn, args.max_strategies)
        summary = build_summary(conn, args, topstep_cfg, data_validation, counts)
        report_path = write_topstep_report(conn, summary, cfg["reports"]["folder"], args.report_tag)
        summary["report_path"] = str(report_path)
        summary_path = write_summary_json(summary, cfg["reports"]["folder"], args.report_tag)
        print_final(summary, summary_path)
        return 0
    except Exception as exc:
        print(f"ERROR: Topstep 150K evaluation failed: {exc}", file=sys.stderr)
        return 1


def run_compile() -> None:
    import subprocess

    result = subprocess.run([sys.executable, "-m", "compileall", "hydra", "scripts"], cwd=project_path(), text=True)
    if result.returncode != 0:
        raise RuntimeError("compileall failed")


def reset_and_connect(db_path: str, tag: str) -> sqlite3.Connection:
    path = project_path(db_path)
    if path.exists():
        archive_dir = path.parent / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"{path.stem}_{utc_now_iso().replace(':', '').replace('+', 'Z')}_{tag}{path.suffix}"
        shutil.move(str(path), archive_path)
        print(f"Registry reset: archived old registry to {archive_path}")
    return connect(db_path)


def load_cached_range(args: argparse.Namespace, cfg: dict[str, Any]) -> pd.DataFrame:
    db_cfg = cfg.get("data", {}).get("databento", {})
    cache_folder = db_cfg.get("cache_folder", "data/cache/databento")
    request = request_from_config(
        {"data": {"databento": {"dataset": args.dataset, "schema": args.schema, "symbols": args.symbols, "start_date": args.start, "end_date": args.end, "cache_folder": cache_folder}}},
        symbols=args.symbols,
        start=args.start,
        end=args.end,
        schema=args.schema,
        dataset=args.dataset,
    )
    exact = Path(request.output_path)
    source = exact if exact.exists() else find_compatible_cache(request, cache_folder)
    if not source:
        raise FileNotFoundError(f"No compatible Databento cache found for {args.dataset} {args.schema} {args.symbols} {args.start} to {args.end}.")
    df = load_cached_ohlcv(source, timeframe=request.timeframe)
    ts = pd.to_datetime(df["timestamp"], utc=True)
    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC")
    if len(args.end) == 10:
        end = end + pd.Timedelta(days=1)
    out = df[(ts >= start) & (ts < end) & (df["symbol"].isin(args.symbols))].reset_index(drop=True)
    if out.empty:
        raise FileNotFoundError(f"Compatible cache {source} produced no rows after local date filtering.")
    print(f"Cache used: {source}")
    print(f"Rows after filter: {len(out)}")
    return out


def find_compatible_cache(request, cache_folder: str) -> Path | None:
    folder = project_path(cache_folder)
    safe_dataset = request.dataset.replace(".", "-")
    safe_symbols = "_".join(request.symbols)
    candidates = sorted(folder.glob(f"{safe_dataset}_{request.schema}_{safe_symbols}_{request.start}_*.parquet"))
    return candidates[-1] if candidates else None


def build_market_states(raw: pd.DataFrame, symbols: list[str]) -> dict[str, pd.DataFrame]:
    states: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        frame = raw[raw["symbol"] == symbol].reset_index(drop=True)
        if frame.empty:
            raise ValueError(f"No rows for {symbol}")
        state = build_market_state(frame)
        leak_ok, leak_reason = audit_no_lookahead(state)
        if not leak_ok:
            raise RuntimeError(f"No-lookahead audit failed for {symbol}: {leak_reason}")
        states[symbol] = state
    return states


def evaluate_candidates(
    conn: sqlite3.Connection,
    candidates,
    market_state: dict[str, pd.DataFrame],
    config: Topstep150KConfig,
    args: argparse.Namespace,
) -> tuple[Counter[str], list[dict[str, Any]]]:
    counts: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, start=1):
        df = market_state[candidate.symbol]
        result = run_backtest(candidate, df, args.seed + index)
        overlay = InternalRiskOverlay(
            daily_stop=float(candidate.risk_parameters.get("internal_daily_stop", 1000)),
            daily_profit_lock=float(candidate.risk_parameters.get("daily_profit_lock", 1500)),
        )
        daily = trades_to_topstep_daily(result.trades, df, overlay)
        split_daily = split_daily_frames(daily)
        evaluation = evaluate_topstep_150k(result.trades, df, config, overlay, split_daily=split_daily)
        record = evaluation.to_record()
        metrics = dict(result.metrics)
        metrics["net_profit"] = record["adjusted_net_profit"]
        metrics["max_drawdown"] = max_drawdown(pd.Series(daily["pnl"].cumsum(), dtype=float)) if len(daily) else 0.0
        metrics["trade_count"] = record["trade_count"]
        upsert_topstep_candidate(
            conn,
            candidate,
            metrics,
            record["status"],
            record["rejection_reason"],
            record,
            robustness=record["topstep_score"],
        )
        counts[record["status"]] += 1
        rows.append({"candidate": candidate, "evaluation": record, "metrics": metrics})
        if index % 250 == 0 or index == len(candidates):
            print(f"Topstep progress: {index}/{len(candidates)} candidates evaluated")
    return counts, rows


def split_daily_frames(daily: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if daily.empty:
        return {"jan": daily, "feb": daily, "mar": daily}
    dates = pd.to_datetime(daily["date"])
    return {
        "jan": daily[(dates >= "2024-01-01") & (dates < "2024-02-01")].reset_index(drop=True),
        "feb": daily[(dates >= "2024-02-01") & (dates < "2024-03-01")].reset_index(drop=True),
        "mar": daily[(dates >= "2024-03-01") & (dates < "2024-04-01")].reset_index(drop=True),
    }


def enforce_portfolio_cap(conn: sqlite3.Connection, max_strategies: int) -> None:
    rows = list(conn.execute("SELECT candidate_id FROM candidates WHERE validation_status='TOPSTEP_PORTFOLIO_CANDIDATE' ORDER BY topstep_score DESC"))
    for row in rows[max_strategies:]:
        conn.execute(
            "UPDATE candidates SET validation_status='TOPSTEP_PAYOUT_SURVIVED', rejection_reason=NULL WHERE candidate_id=?",
            (row["candidate_id"],),
        )
    conn.commit()


def build_summary(conn: sqlite3.Connection, args: argparse.Namespace, config: Topstep150KConfig, data_validation: dict[str, Any], counts: Counter[str]) -> dict[str, Any]:
    status_distribution = {row["validation_status"]: row["c"] for row in conn.execute("SELECT validation_status, COUNT(*) c FROM candidates GROUP BY validation_status ORDER BY c DESC")}
    failure_reasons = {row["rejection_reason"]: row["c"] for row in conn.execute("SELECT rejection_reason, COUNT(*) c FROM candidates WHERE rejection_reason IS NOT NULL GROUP BY rejection_reason ORDER BY c DESC")}
    best = conn.execute("SELECT MAX(topstep_score) FROM candidates").fetchone()[0] or 0.0
    return {
        "run_mode": "topstep 150k real data strict",
        "dataset": args.dataset,
        "schema": args.schema,
        "symbols": args.symbols,
        "start": args.start,
        "end": args.end,
        "cached_data_used": True,
        "new_databento_request_made": False,
        "account_model": config.__dict__,
        "no_daily_loss_limit_modeled": bool(args.no_daily_loss_limit),
        "optional_daily_loss_limit_disabled": not config.use_optional_daily_loss_limit,
        "internal_risk_overlay_tested": True,
        "candidates_tested": int(conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]),
        "status_distribution": status_distribution,
        "failure_reasons": failure_reasons,
        "combine_passed": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE topstep_passed=1").fetchone()[0]),
        "mll_respected": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE combine_mll_breached=0").fetchone()[0]),
        "consistency_respected": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE combine_consistency_ok=1").fetchone()[0]),
        "target_reached": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE combine_profit_target_hit=1").fetchone()[0]),
        "funded_survived": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE funded_sim_survived=1").fetchone()[0]),
        "payout_eligible": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE payout_eligible=1").fetchone()[0]),
        "portfolio_candidates": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status='TOPSTEP_PORTFOLIO_CANDIDATE'").fetchone()[0]),
        "gross_payout": float(conn.execute("SELECT COALESCE(SUM(gross_payout_available),0) FROM candidates").fetchone()[0]),
        "trader_net_payout": float(conn.execute("SELECT COALESCE(SUM(trader_net_payout),0) FROM candidates").fetchone()[0]),
        "avg_days_to_pass": _scalar(conn, "SELECT AVG(combine_days_to_pass) FROM candidates WHERE topstep_passed=1"),
        "avg_days_to_payout": _scalar(conn, "SELECT AVG(payout_days_to_eligibility) FROM candidates WHERE payout_eligible=1"),
        "mll_breach_rate": _scalar(conn, "SELECT AVG(combine_mll_breached) FROM candidates"),
        "min_mll_buffer": _scalar(conn, "SELECT MIN(combine_min_mll_buffer) FROM candidates"),
        "median_mll_buffer": _median(conn, "combine_min_mll_buffer"),
        "best_day_concentration_avg": _scalar(conn, "SELECT AVG(combine_best_day_pct_of_total_profit) FROM candidates"),
        "worst_day_loss_min": _scalar(conn, "SELECT MIN(worst_day_loss) FROM candidates"),
        "max_losing_streak": int(conn.execute("SELECT COALESCE(MAX(max_consecutive_losing_days),0) FROM candidates").fetchone()[0]),
        "best_topstep_score": float(best),
        "bars_per_symbol": data_validation["rows_by_symbol"],
        "missing_intervals": data_validation["missing_intervals"],
        "top_families": {row["family"]: row["c"] for row in conn.execute("SELECT family, COUNT(*) c FROM candidates GROUP BY family ORDER BY c DESC")},
    }


def write_topstep_report(conn: sqlite3.Connection, summary: dict[str, Any], report_folder: str, tag: str) -> Path:
    folder = project_path(report_folder)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"topstep_150k_report_{utc_now_iso().replace(':', '').replace('+', 'Z')}_{tag}.md"
    top = list(
        conn.execute(
            """
            SELECT candidate_id,family,symbol,topstep_score,validation_status,combine_days_to_pass,
                   combine_min_mll_buffer,combine_best_day_pct_of_total_profit,worst_day_loss,
                   payout_cycles_survived,trader_net_payout,rejection_reason
            FROM candidates
            ORDER BY topstep_score DESC, trader_net_payout DESC
            LIMIT 20
            """
        )
    )
    lines = [
        "# HYDRA Topstep 150K Research Report",
        "",
        f"Generated: {utc_now_iso()}",
        "",
        "## Warning",
        "- This is historical research, not live trading approval. Passing simulation does not guarantee passing Topstep.",
        "",
        "## Account Model",
        f"- Program: topstep",
        f"- Account size: {summary['account_model']['account_size']:.0f}",
        f"- Combine profit target: {summary['account_model']['combine_profit_target']:.0f}",
        f"- Combine max loss limit: {summary['account_model']['combine_max_loss_limit']:.0f}",
        f"- No daily loss limit modeled: {summary['no_daily_loss_limit_modeled']}",
        f"- Optional DLL disabled: {summary['optional_daily_loss_limit_disabled']}",
        f"- Internal risk overlay tested: {summary['internal_risk_overlay_tested']}",
        "",
        "## Data",
        f"- Provider: Databento cache",
        f"- Dataset: {summary['dataset']}",
        f"- Schema: {summary['schema']}",
        f"- Symbols: {', '.join(summary['symbols'])}",
        f"- Date range: {summary['start']} to {summary['end']}",
        f"- Bars per symbol: {summary['bars_per_symbol']}",
        "",
        "## Summary",
        f"- Candidates tested: {summary['candidates_tested']}",
        f"- Combine passed: {summary['combine_passed']}",
        f"- Funded/XFA survived: {summary['funded_survived']}",
        f"- Payout eligible: {summary['payout_eligible']}",
        f"- Portfolio candidates: {summary['portfolio_candidates']}",
        f"- Expected gross payout: {summary['gross_payout']:.2f}",
        f"- Expected trader net payout after 90/10: {summary['trader_net_payout']:.2f}",
        f"- Average days to pass: {_fmt_optional_days(summary['avg_days_to_pass'])}",
        f"- Average days to first payout eligibility: {_fmt_optional_days(summary['avg_days_to_payout'])}",
        f"- MLL breach rate: {summary['mll_breach_rate']:.3f}",
        f"- Min/median MLL buffer: {summary['min_mll_buffer']:.2f} / {summary['median_mll_buffer']:.2f}",
        f"- Best day concentration average: {summary['best_day_concentration_avg']:.3f}",
        f"- Worst day loss: {summary['worst_day_loss_min']:.2f}",
        f"- Max losing streak: {summary['max_losing_streak']}",
        f"- Best Topstep score: {summary['best_topstep_score']:.6f}",
        "",
        "## Status Distribution",
    ]
    lines += [f"- {status}: {count}" for status, count in summary["status_distribution"].items()] or ["- None."]
    lines += ["", "## Failure Reasons"]
    lines += [f"- {reason}: {count}" for reason, count in summary["failure_reasons"].items()] or ["- None."]
    lines += ["", "## Strategy Families Tested"]
    lines += [f"- {family}: {count}" for family, count in summary["top_families"].items()]
    lines += ["", "## Best Topstep Score Candidates"]
    for row in top:
        lines.append(
            f"- {row['candidate_id']} {row['family']} {row['symbol']} status={row['validation_status']} "
            f"score={row['topstep_score']:.6f} days={row['combine_days_to_pass']} "
            f"buffer={row['combine_min_mll_buffer']:.2f} best_day_pct={row['combine_best_day_pct_of_total_profit']:.3f} "
            f"worst_day={row['worst_day_loss']:.2f} payouts={row['payout_cycles_survived']} "
            f"net_payout={row['trader_net_payout']:.2f} reason={row['rejection_reason']}"
        )
    lines += [
        "",
        "## Recommended Mutations",
        "- Prioritize micro-first MES/MNQ variants with lower daily locks and fewer trade opportunities.",
        "- Add explicit stop-distance exits and per-trade loss caps to reduce Combine MLL breaches.",
        "- Add month-aware selection pressure so January, February, and March profiles must all remain acceptable.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_summary_json(summary: dict[str, Any], report_folder: str, tag: str) -> Path:
    path = project_path(report_folder) / f"topstep_150k_summary_{utc_now_iso().replace(':', '').replace('+', 'Z')}_{tag}.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def print_final(summary: dict[str, Any], summary_path: Path) -> None:
    print("Topstep final summary:")
    for key in [
        "cached_data_used",
        "new_databento_request_made",
        "candidates_tested",
        "combine_passed",
        "mll_respected",
        "consistency_respected",
        "target_reached",
        "funded_survived",
        "payout_eligible",
        "gross_payout",
        "trader_net_payout",
        "best_topstep_score",
        "portfolio_candidates",
        "report_path",
    ]:
        print(f"{key}: {summary.get(key)}")
    print(f"Summary JSON: {summary_path}")


def _scalar(conn: sqlite3.Connection, sql: str) -> float:
    value = conn.execute(sql).fetchone()[0]
    return float(value or 0.0)


def _fmt_optional_days(value: float) -> str:
    return "N/A" if value == 0 else f"{value:.2f}"


def _median(conn: sqlite3.Connection, column: str) -> float:
    values = [float(row[0]) for row in conn.execute(f"SELECT {column} FROM candidates ORDER BY {column}")]
    if not values:
        return 0.0
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


if __name__ == "__main__":
    raise SystemExit(main())
