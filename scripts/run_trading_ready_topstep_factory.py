#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.backtest.engine import run_backtest
from hydra.backtest.metrics import max_drawdown
from hydra.data.databento_loader import load_cached_ohlcv, request_from_config, validate_ohlcv_frame
from hydra.factory.adaptive_mutation import LANES, LANE_TO_FAMILY
from hydra.features.market_state import build_market_state
from hydra.promotion.pipeline import PromotionInput, run_promotion_pipeline
from hydra.propfirm.pass_path_optimizer import analyze_pass_path
from hydra.propfirm.topstep_150k import InternalRiskOverlay, Topstep150KConfig, evaluate_topstep_150k, trades_to_topstep_daily
from hydra.registry.candidates import update_promotion_metadata, upsert_topstep_candidate
from hydra.registry.db import connect
from hydra.strategies.generator import generate_topstep_lane_candidates
from hydra.utils.config import load_config, project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.no_leak import audit_no_lookahead


WORKER_STATES: dict[str, pd.DataFrame] = {}
WORKER_TOPSTEP_CFG: Topstep150KConfig | None = None
WORKER_PROFIT_TARGET = 9000.0
WORKER_MLL = 4500.0

FOCUSED_LANES = [
    "portfolio_diversification_lane",
    "topstep_nq_es_divergence_controlled_v2",
    "topstep_nq_es_divergence_controlled_v2",
    "topstep_opening_range_controlled_runner_v2",
    "topstep_vwap_exhaustion_payout_engine_v2",
    "topstep_volatility_expansion_limited_risk_v2",
    "near_miss_adaptive_mutator",
    "creative_market_representation_lane",
    "payout_cycle_smooth_climber_v1",
    "consistency_safe_runner_v1",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuous trading-ready Topstep strategy factory.")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--use-cache-only", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--seed", type=int, default=49)
    parser.add_argument("--workers", default="auto")
    parser.add_argument("--min-total-candidates", type=int, default=80000)
    parser.add_argument("--target-trading-ready", type=int, default=50)
    parser.add_argument("--target-economically-viable", type=int, default=30)
    parser.add_argument("--target-topstep-viable", type=int, default=20)
    parser.add_argument("--target-portfolio-candidates", type=int, default=10)
    parser.add_argument("--account-size", type=float, default=150000)
    parser.add_argument("--profit-target", type=float, default=9000)
    parser.add_argument("--mll", type=float, default=4500)
    parser.add_argument("--no-daily-loss-limit", action="store_true")
    parser.add_argument("--simulate-funded", action="store_true")
    parser.add_argument("--simulate-payouts", action="store_true")
    parser.add_argument("--checkpoint-every-minutes", type=float, default=20)
    parser.add_argument("--max-runtime-hours", type=float, default=12)
    parser.add_argument("--continue-until-quality", action="store_true")
    parser.add_argument("--report-tag", required=True)
    parser.add_argument("--registry-path", default="registry/hydra_registry.db")
    parser.add_argument("--batch-size", type=int, default=100)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    if not args.use_cache_only:
        print("ERROR: factory is cache-only unless a Databento download is explicitly approved.", file=sys.stderr)
        return 2
    if not args.strict:
        print("ERROR: trading-ready factory requires --strict.", file=sys.stderr)
        return 2
    cfg = load_config()
    conn = connect(args.registry_path)
    raw = load_cached_range(args, cfg)
    data_validation = validate_ohlcv_frame(raw, timeframe="1m")
    states, leak = build_states(raw, args.symbols)
    topstep_cfg = Topstep150KConfig(
        account_size=args.account_size,
        combine_starting_balance=args.account_size,
        combine_profit_target=args.profit_target,
        combine_max_loss_limit=args.mll,
        no_daily_loss_limit=bool(args.no_daily_loss_limit),
    )
    existing_fingerprints = load_all_strategy_fingerprints(conn)
    existing_curves: dict[str, pd.Series] = {}
    checkpoint_dir = project_path("reports", "checkpoints")
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    deadline = started + args.max_runtime_hours * 3600
    next_checkpoint = started + max(args.checkpoint_every_minutes, 0.1) * 60
    tested_this_run = 0
    workers_used = resolve_workers(args.workers)
    print(f"Workers used: {workers_used}")
    print(f"Resume fingerprints loaded: {len(existing_fingerprints)}")
    stop_reason = "max_runtime_reached"
    seed_cursor = args.seed
    while time.monotonic() < deadline:
        if quality_targets_reached(conn, args):
            stop_reason = "quality_target_reached"
            break
        if total_candidates(conn) >= args.min_total_candidates and not args.continue_until_quality:
            stop_reason = "minimum_total_reached"
            break
        batch_lanes = [lane_for_index(tested_this_run + i) for i in range(args.batch_size)]
        lane_families = [LANE_TO_FAMILY[lane] for lane in batch_lanes]
        batch = generate_topstep_lane_candidates(args.batch_size, args.symbols, ["1m"], seed_cursor, lane_families)
        seed_cursor += args.batch_size + 17
        tasks = [(candidate, batch_lanes[offset], args.seed + tested_this_run + offset) for offset, candidate in enumerate(batch)]
        for promotion in evaluate_batch(
            conn,
            tasks,
            states,
            leak,
            data_validation,
            topstep_cfg,
            args,
            existing_fingerprints,
            existing_curves,
            workers_used,
        ):
            if time.monotonic() >= deadline:
                break
            if promotion.get("strategy_fingerprint"):
                existing_fingerprints.add(promotion["strategy_fingerprint"])
            tested_this_run += 1
        if time.monotonic() >= next_checkpoint:
            write_checkpoint(conn, args, checkpoint_dir, started, tested_this_run, workers_used, stop_reason="running")
            next_checkpoint = time.monotonic() + max(args.checkpoint_every_minutes, 0.1) * 60
        print(f"Factory progress: tested_this_run={tested_this_run} total_registry={total_candidates(conn)} ready={count_status(conn, 'TRADING_READY_CANDIDATE')}")
    checkpoint_path = write_checkpoint(conn, args, checkpoint_dir, started, tested_this_run, workers_used, stop_reason=stop_reason)
    report_path = write_final_report(conn, args, started, tested_this_run, workers_used, checkpoint_dir, checkpoint_path, stop_reason)
    print_final(conn, args, started, tested_this_run, workers_used, report_path, checkpoint_dir)
    return 0


def resolve_workers(value: str) -> int:
    if value == "auto":
        cpu_count = os.cpu_count() or 1
        if cpu_count >= 4:
            return max(2, cpu_count - 1)
        if cpu_count == 2:
            return 2
        return 1
    return max(1, int(value))


def lane_for_index(index: int) -> str:
    return FOCUSED_LANES[index % len(FOCUSED_LANES)]


def load_all_strategy_fingerprints(conn: sqlite3.Connection) -> set[str]:
    from hydra.promotion.gates import strategy_fingerprint
    from hydra.strategies.dsl import StrategyCandidate

    fingerprints: set[str] = set()
    for row in conn.execute("SELECT * FROM candidates"):
        if row["strategy_fingerprint"]:
            fingerprints.add(row["strategy_fingerprint"])
            continue
        try:
            candidate = StrategyCandidate(
                candidate_id=row["candidate_id"],
                family=row["family"],
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                parameters=json.loads(row["parameters_json"]),
                entry_logic=f"{row['family']}_regime_path_entry",
                exit_logic="unknown",
                risk_parameters=json.loads(row["risk_json"]),
                parent_candidate_id=row["parent_candidate_id"],
                mutation_type=row["mutation_type"],
            )
            fingerprints.add(strategy_fingerprint(candidate))
        except Exception:
            continue
    return fingerprints


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
    source = Path(request.output_path)
    if not source.exists():
        candidates = sorted(project_path(cache_folder).glob(f"{args.dataset.replace('.', '-')}_{args.schema}_{'_'.join(args.symbols)}_{args.start}_*.parquet"))
        source = candidates[-1] if candidates else source
    if not source.exists():
        raise FileNotFoundError(f"Required cached Databento data is missing for {args.dataset} {args.schema} {args.start} to {args.end}. No download was attempted.")
    df = load_cached_ohlcv(source, timeframe=request.timeframe)
    ts = pd.to_datetime(df["timestamp"], utc=True)
    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC")
    if len(args.end) == 10:
        end += pd.Timedelta(days=1)
    out = df[(ts >= start) & (ts < end) & (df["symbol"].isin(args.symbols))].reset_index(drop=True)
    if out.empty:
        raise FileNotFoundError("Cached Databento data exists but yielded no rows after requested date/symbol filter.")
    print(f"Cached Databento data used: {source}")
    print("New Databento request made: no")
    return out


def build_states(raw: pd.DataFrame, symbols: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, tuple[bool, str]]]:
    states: dict[str, pd.DataFrame] = {}
    leak: dict[str, tuple[bool, str]] = {}
    for symbol in symbols:
        frame = raw[raw["symbol"] == symbol].reset_index(drop=True)
        state = build_market_state(frame)
        leak[symbol] = audit_no_lookahead(state)
        if not leak[symbol][0]:
            raise RuntimeError(f"No-lookahead audit failed for {symbol}: {leak[symbol][1]}")
        states[symbol] = state
    return states, leak


def evaluate_batch(
    conn: sqlite3.Connection,
    tasks,
    states: dict[str, pd.DataFrame],
    leak: dict[str, tuple[bool, str]],
    data_validation: dict[str, Any],
    topstep_cfg: Topstep150KConfig,
    args: argparse.Namespace,
    existing_fingerprints: set[str],
    existing_curves: dict[str, pd.Series],
    workers_used: int,
):
    if workers_used <= 1:
        for candidate, lane, seed in tasks:
            yield evaluate_and_store(
                conn,
                candidate,
                lane,
                states,
                leak,
                data_validation,
                topstep_cfg,
                args,
                existing_fingerprints,
                existing_curves,
                seed,
            )
        return
    with ProcessPoolExecutor(
        max_workers=workers_used,
        initializer=init_worker,
        initargs=(states, topstep_cfg, args.profit_target, args.mll),
    ) as pool:
        futures = [pool.submit(worker_evaluate_candidate, candidate, lane, seed) for candidate, lane, seed in tasks]
        for future in as_completed(futures):
            worker_result = future.result()
            yield store_worker_result(
                conn,
                worker_result,
                leak,
                data_validation,
                args,
                existing_fingerprints,
                existing_curves,
            )


def init_worker(states: dict[str, pd.DataFrame], topstep_cfg: Topstep150KConfig, profit_target: float, mll: float) -> None:
    global WORKER_STATES, WORKER_TOPSTEP_CFG, WORKER_PROFIT_TARGET, WORKER_MLL
    WORKER_STATES = states
    WORKER_TOPSTEP_CFG = topstep_cfg
    WORKER_PROFIT_TARGET = profit_target
    WORKER_MLL = mll


def worker_evaluate_candidate(candidate, lane: str, seed: int) -> dict[str, Any]:
    if WORKER_TOPSTEP_CFG is None:
        raise RuntimeError("Worker Topstep config was not initialized.")
    df = WORKER_STATES[candidate.symbol]
    result = run_backtest(candidate, df, seed)
    overlay = InternalRiskOverlay(
        daily_stop=float(candidate.risk_parameters.get("internal_daily_stop", 1000)),
        daily_profit_lock=float(candidate.risk_parameters.get("daily_profit_lock", 1500)),
    )
    daily = trades_to_topstep_daily(result.trades, df, overlay)
    split_daily = split_daily_frames(daily)
    evaluation = evaluate_topstep_150k(result.trades, df, WORKER_TOPSTEP_CFG, overlay, split_daily=split_daily)
    topstep_record = evaluation.to_record()
    pass_path = analyze_pass_path(topstep_record, WORKER_PROFIT_TARGET, WORKER_MLL)
    topstep_record["pass_path_diagnosis"] = pass_path.diagnosis
    metrics = dict(result.metrics)
    metrics["net_profit"] = topstep_record["adjusted_net_profit"]
    metrics["max_drawdown"] = max_drawdown(pd.Series(daily["pnl"].cumsum(), dtype=float)) if len(daily) else 0.0
    metrics["trade_count"] = topstep_record["trade_count"]
    return {
        "candidate": candidate,
        "lane": lane,
        "seed": seed,
        "result": result,
        "daily": daily,
        "topstep_record": topstep_record,
        "metrics": metrics,
    }


def store_worker_result(
    conn: sqlite3.Connection,
    worker_result: dict[str, Any],
    leak: dict[str, tuple[bool, str]],
    data_validation: dict[str, Any],
    args: argparse.Namespace,
    existing_fingerprints: set[str],
    existing_curves: dict[str, pd.Series],
) -> dict[str, Any]:
    candidate = worker_result["candidate"]
    result = worker_result["result"]
    topstep_record = worker_result["topstep_record"]
    metrics = worker_result["metrics"]
    max_corr = max_correlation(result.equity_curve, existing_curves)
    promotion = run_promotion_pipeline(
        PromotionInput(
            candidate=candidate,
            result=result,
            daily=worker_result["daily"],
            topstep_record=topstep_record,
            data_validation=data_validation,
            split_scores=topstep_record.get("split_scores", {}),
            leak_ok=leak[candidate.symbol][0],
            leak_reason=leak[candidate.symbol][1],
            existing_fingerprints=existing_fingerprints,
            max_correlation=max_corr,
            seed=worker_result["seed"],
            lane=worker_result["lane"],
            report_tag=args.report_tag,
        )
    )
    upsert_topstep_candidate(conn, candidate, metrics, topstep_record["status"], topstep_record["rejection_reason"], topstep_record, robustness=topstep_record["topstep_score"])
    update_promotion_metadata(conn, candidate.candidate_id, promotion)
    if promotion["classification"] in {"ECONOMICALLY_VIABLE", "TOPSTEP_VIABLE", "TRADING_READY_CANDIDATE", "TOPSTEP_NEAR_MISS"}:
        existing_curves[candidate.candidate_id] = result.equity_curve
        if len(existing_curves) > 300:
            existing_curves.pop(next(iter(existing_curves)))
    return promotion


def evaluate_and_store(
    conn: sqlite3.Connection,
    candidate,
    lane: str,
    states: dict[str, pd.DataFrame],
    leak: dict[str, tuple[bool, str]],
    data_validation: dict[str, Any],
    topstep_cfg: Topstep150KConfig,
    args: argparse.Namespace,
    existing_fingerprints: set[str],
    existing_curves: dict[str, pd.Series],
    seed: int,
) -> dict[str, Any]:
    df = states[candidate.symbol]
    result = run_backtest(candidate, df, seed)
    overlay = InternalRiskOverlay(
        daily_stop=float(candidate.risk_parameters.get("internal_daily_stop", 1000)),
        daily_profit_lock=float(candidate.risk_parameters.get("daily_profit_lock", 1500)),
    )
    daily = trades_to_topstep_daily(result.trades, df, overlay)
    split_daily = split_daily_frames(daily)
    evaluation = evaluate_topstep_150k(result.trades, df, topstep_cfg, overlay, split_daily=split_daily)
    topstep_record = evaluation.to_record()
    pass_path = analyze_pass_path(topstep_record, args.profit_target, args.mll)
    topstep_record["pass_path_diagnosis"] = pass_path.diagnosis
    metrics = dict(result.metrics)
    metrics["net_profit"] = topstep_record["adjusted_net_profit"]
    metrics["max_drawdown"] = max_drawdown(pd.Series(daily["pnl"].cumsum(), dtype=float)) if len(daily) else 0.0
    metrics["trade_count"] = topstep_record["trade_count"]
    max_corr = max_correlation(result.equity_curve, existing_curves)
    promotion = run_promotion_pipeline(
        PromotionInput(
            candidate=candidate,
            result=result,
            daily=daily,
            topstep_record=topstep_record,
            data_validation=data_validation,
            split_scores=topstep_record.get("split_scores", {}),
            leak_ok=leak[candidate.symbol][0],
            leak_reason=leak[candidate.symbol][1],
            existing_fingerprints=existing_fingerprints,
            max_correlation=max_corr,
            seed=seed,
            lane=lane,
            report_tag=args.report_tag,
        )
    )
    upsert_topstep_candidate(conn, candidate, metrics, topstep_record["status"], topstep_record["rejection_reason"], topstep_record, robustness=topstep_record["topstep_score"])
    update_promotion_metadata(conn, candidate.candidate_id, promotion)
    if promotion["classification"] in {"ECONOMICALLY_VIABLE", "TOPSTEP_VIABLE", "TRADING_READY_CANDIDATE", "TOPSTEP_NEAR_MISS"}:
        existing_curves[candidate.candidate_id] = result.equity_curve
        if len(existing_curves) > 300:
            existing_curves.pop(next(iter(existing_curves)))
    return promotion


def split_daily_frames(daily: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if daily.empty:
        return {"jan": daily, "feb": daily, "mar": daily}
    dates = pd.to_datetime(daily["date"])
    return {
        "jan": daily[(dates >= "2024-01-01") & (dates < "2024-02-01")].reset_index(drop=True),
        "feb": daily[(dates >= "2024-02-01") & (dates < "2024-03-01")].reset_index(drop=True),
        "mar": daily[(dates >= "2024-03-01") & (dates < "2024-04-01")].reset_index(drop=True),
    }


def max_correlation(curve: pd.Series, existing: dict[str, pd.Series]) -> float:
    if not existing:
        return 0.0
    returns = curve.diff().fillna(0.0)
    values = []
    for other in existing.values():
        joined = pd.concat([returns, other.diff().fillna(0.0)], axis=1).dropna()
        if len(joined) > 5:
            corr = joined.iloc[:, 0].corr(joined.iloc[:, 1])
            if pd.notna(corr):
                values.append(abs(float(corr)))
    return max(values) if values else 0.0


def quality_targets_reached(conn: sqlite3.Connection, args: argparse.Namespace) -> bool:
    return (
        count_status(conn, "TRADING_READY_CANDIDATE") >= args.target_trading_ready
        and count_status(conn, "ECONOMICALLY_VIABLE") >= args.target_economically_viable
        and count_status(conn, "TOPSTEP_VIABLE") >= args.target_topstep_viable
    )


def total_candidates(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0])


def count_status(conn: sqlite3.Connection, status: str) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status=?", (status,)).fetchone()[0])


def summary(conn: sqlite3.Connection, args: argparse.Namespace, started: float, tested_this_run: int, workers_used: int, checkpoint_dir: Path, stop_reason: str) -> dict[str, Any]:
    status_distribution = {row["validation_status"]: row["c"] for row in conn.execute("SELECT validation_status, COUNT(*) c FROM candidates GROUP BY validation_status ORDER BY c DESC")}
    failure_reasons = {row["rejection_reason"]: row["c"] for row in conn.execute("SELECT rejection_reason, COUNT(*) c FROM candidates WHERE rejection_reason IS NOT NULL GROUP BY rejection_reason ORDER BY c DESC")}
    gate_distribution = gate_failure_distribution(conn)
    near_miss = {row["research_lane"]: row["c"] for row in conn.execute("SELECT research_lane, COUNT(*) c FROM candidates WHERE validation_status IN ('PROMISING_NEEDS_MUTATION','TOPSTEP_NEAR_MISS') GROUP BY research_lane ORDER BY c DESC")}
    return {
        "runtime_seconds": round(time.monotonic() - started, 2),
        "cache_only_respected": True,
        "new_databento_request_made": False,
        "workers_used": workers_used,
        "candidates_tested_this_run": tested_this_run,
        "total_candidates_in_registry": total_candidates(conn),
        "target_80000_progress": round(total_candidates(conn) / max(args.min_total_candidates, 1), 6),
        "trading_ready_count": count_status(conn, "TRADING_READY_CANDIDATE"),
        "economically_viable_count": count_status(conn, "ECONOMICALLY_VIABLE"),
        "topstep_viable_count": count_status(conn, "TOPSTEP_VIABLE"),
        "near_miss_count": count_status(conn, "TOPSTEP_NEAR_MISS") + count_status(conn, "PROMISING_NEEDS_MUTATION"),
        "target_50_reached": count_status(conn, "TRADING_READY_CANDIDATE") >= args.target_trading_ready,
        "status_distribution": status_distribution,
        "failure_reasons": failure_reasons,
        "promotion_gate_distribution": gate_distribution,
        "target_reached_count": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE combine_profit_target_hit=1").fetchone()[0]),
        "mll_respected_count": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE combine_mll_breached=0").fetchone()[0]),
        "consistency_respected_count": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE combine_consistency_ok=1").fetchone()[0]),
        "funded_survival_count": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE funded_sim_survived=1").fetchone()[0]),
        "payout_eligible_count": int(conn.execute("SELECT COUNT(*) FROM candidates WHERE payout_eligible=1").fetchone()[0]),
        "payout_cycles_survived": int(conn.execute("SELECT COALESCE(SUM(payout_cycles_survived),0) FROM candidates").fetchone()[0]),
        "gross_payout_estimate": float(conn.execute("SELECT COALESCE(SUM(gross_payout_available),0) FROM candidates").fetchone()[0]),
        "trader_net_payout_estimate": float(conn.execute("SELECT COALESCE(SUM(trader_net_payout),0) FROM candidates").fetchone()[0]),
        "best_topstep_score": float(conn.execute("SELECT COALESCE(MAX(topstep_score),0) FROM candidates").fetchone()[0]),
        "best_promotion_score": float(conn.execute("SELECT COALESCE(MAX(promotion_score),0) FROM candidates").fetchone()[0]),
        "best_economic_score": float(conn.execute("SELECT COALESCE(MAX(economic_score),0) FROM candidates").fetchone()[0]),
        "best_families": {row["family"]: row["c"] for row in conn.execute("SELECT family, COUNT(*) c FROM candidates WHERE promotion_score >= 0.45 GROUP BY family ORDER BY c DESC LIMIT 10")},
        "near_miss_map": near_miss,
        "branches_to_kill": [row["research_lane"] for row in conn.execute("SELECT research_lane FROM candidates WHERE branch_action='kill' GROUP BY research_lane ORDER BY COUNT(*) DESC LIMIT 10")],
        "branches_to_expand": [row["research_lane"] for row in conn.execute("SELECT research_lane FROM candidates WHERE branch_action='expand' GROUP BY research_lane ORDER BY COUNT(*) DESC LIMIT 10")],
        "exported_configs": [row["config_export_path"] for row in conn.execute("SELECT config_export_path FROM candidates WHERE config_export_path IS NOT NULL")],
        "checkpoint_folder": str(checkpoint_dir),
        "stop_reason": stop_reason,
        "resume_command": resume_command(args),
    }


def gate_failure_distribution(conn: sqlite3.Connection) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in conn.execute("SELECT gate_history_json FROM candidates WHERE gate_history_json != '[]'"):
        try:
            gates = json.loads(row["gate_history_json"])
        except json.JSONDecodeError:
            continue
        for item in gates:
            if not item.get("passed"):
                counts[f"{item.get('name')}:{item.get('severity')}:{item.get('reason')}"] += 1
    return dict(counts.most_common(20))


def write_checkpoint(conn: sqlite3.Connection, args: argparse.Namespace, checkpoint_dir: Path, started: float, tested_this_run: int, workers_used: int, stop_reason: str) -> Path:
    data = summary(conn, args, started, tested_this_run, workers_used, checkpoint_dir, stop_reason)
    path = checkpoint_dir / f"trading_ready_checkpoint_{utc_now_iso().replace(':', '').replace('+', 'Z')}_{args.report_tag}.md"
    lines = ["# HYDRA Trading-Ready Factory Checkpoint", "", f"Generated: {utc_now_iso()}", ""]
    for key, value in data.items():
        lines.append(f"- {key}: {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_final_report(conn: sqlite3.Connection, args: argparse.Namespace, started: float, tested_this_run: int, workers_used: int, checkpoint_dir: Path, checkpoint_path: Path, stop_reason: str) -> Path:
    data = summary(conn, args, started, tested_this_run, workers_used, checkpoint_dir, stop_reason)
    path = project_path("reports") / f"trading_ready_topstep_factory_{utc_now_iso().replace(':', '').replace('+', 'Z')}_{args.report_tag}.md"
    top_ready = list(conn.execute("SELECT candidate_id,family,symbol,promotion_score,topstep_score,economic_score,config_export_path FROM candidates WHERE validation_status='TRADING_READY_CANDIDATE' ORDER BY promotion_score DESC LIMIT 20"))
    lines = [
        "# HYDRA Trading-Ready Topstep Factory Report",
        "",
        f"Generated: {utc_now_iso()}",
        "",
        "## Warning",
        "- This is historical research only. It is not live trading approval.",
        "",
        "## Summary",
    ]
    for key, value in data.items():
        lines.append(f"- {key}: {value}")
    lines += ["", "## Best Trading-Ready Candidates"]
    lines += [
        f"- {row['candidate_id']} {row['family']} {row['symbol']} promotion={row['promotion_score']:.3f} topstep={row['topstep_score']:.3f} economic={row['economic_score']:.3f} config={row['config_export_path']}"
        for row in top_ready
    ] or ["- None."]
    lines += ["", "## Checkpoint", f"- Latest checkpoint: {checkpoint_path}"]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def resume_command(args: argparse.Namespace) -> str:
    return (
        "python scripts/run_trading_ready_topstep_factory.py "
        f"--symbols {' '.join(args.symbols)} --start {args.start} --end {args.end} --schema {args.schema} --dataset {args.dataset} "
        "--use-cache-only --strict "
        f"--seed {args.seed + 1000} --workers {args.workers} --min-total-candidates {args.min_total_candidates} "
        f"--target-trading-ready {args.target_trading_ready} --target-economically-viable {args.target_economically_viable} "
        f"--target-topstep-viable {args.target_topstep_viable} --target-portfolio-candidates {args.target_portfolio_candidates} "
        f"--account-size {args.account_size:.0f} --profit-target {args.profit_target:.0f} --mll {args.mll:.0f} "
        "--no-daily-loss-limit --simulate-funded --simulate-payouts "
        f"--checkpoint-every-minutes {args.checkpoint_every_minutes:g} --max-runtime-hours {args.max_runtime_hours:g} --continue-until-quality "
        f"--report-tag {args.report_tag}"
    )


def print_final(conn: sqlite3.Connection, args: argparse.Namespace, started: float, tested_this_run: int, workers_used: int, report_path: Path, checkpoint_dir: Path) -> None:
    data = summary(conn, args, started, tested_this_run, workers_used, checkpoint_dir, "completed")
    print("Trading-ready factory final summary:")
    for key in [
        "runtime_seconds",
        "cache_only_respected",
        "new_databento_request_made",
        "workers_used",
        "candidates_tested_this_run",
        "total_candidates_in_registry",
        "target_80000_progress",
        "economically_viable_count",
        "topstep_viable_count",
        "trading_ready_count",
        "target_50_reached",
        "target_reached_count",
        "mll_respected_count",
        "consistency_respected_count",
        "funded_survival_count",
        "payout_eligible_count",
        "gross_payout_estimate",
        "trader_net_payout_estimate",
        "best_topstep_score",
        "best_promotion_score",
    ]:
        print(f"{key}: {data[key]}")
    print(f"Report: {report_path}")
    print(f"Checkpoint folder: {checkpoint_dir}")
    print(f"Resume command: {data['resume_command']}")


if __name__ == "__main__":
    raise SystemExit(main())
