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
from hydra.data.acquisition_policy import (
    decide_databento_acquisition,
    record_download_complete,
)
from hydra.data.budget import DatabentoBudgetConfig, read_ledger, sha256_file, write_budget_summary
from hydra.data.databento_loader import (
    download_historical_ohlcv,
    download_historical_raw_only,
    estimate_request,
    load_api_key,
    load_cached_ohlcv,
    request_from_config,
    validate_ohlcv_frame,
)
from hydra.factory.gate_aware_remediation import child_from_registry_row
from hydra.factory.remediation_policy import POLICIES
from hydra.factory.run_control import RunControlConfig, RunControlState, evaluate_stop, validated_quality_target_reached
from hydra.features.market_state import build_market_state
from hydra.portfolio.remediation_portfolio import build_remediation_portfolio_candidates
from hydra.promotion.candidate_dossier import build_candidate_dossier, write_dossiers
from hydra.promotion.equivalence_clusters import cluster_summary
from hydra.promotion.failure_attribution import attribute_candidate_failure
from hydra.promotion.pareto import pareto_frontier
from hydra.promotion.pipeline import PromotionInput, run_promotion_pipeline
from hydra.promotion.gates import strategy_fingerprint
from hydra.propfirm.pass_path_optimizer import analyze_pass_path
from hydra.propfirm.rule_versioning import DEFAULT_TOPSTEP_RULE_PATH, load_topstep_rule_snapshot
from hydra.propfirm.topstep_150k import InternalRiskOverlay, Topstep150KConfig, evaluate_topstep_150k, trades_to_topstep_daily
from hydra.registry.candidates import update_promotion_metadata, upsert_topstep_candidate
from hydra.registry.db import connect
from hydra.utils.config import load_config, project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.data_roles import DataRole
from hydra.validation.family_fdr import family_false_discovery_proxy
from hydra.validation.freeze_manifest import build_manifest, write_manifest
from hydra.validation.lockbox_guard import current_commit, enforce_data_access
from hydra.validation.multiple_testing import effective_independent_trials, selection_adjusted_score
from hydra.validation.no_leak import audit_no_lookahead


WORKER_STATES: dict[str, pd.DataFrame] = {}
WORKER_TOPSTEP_CFG: Topstep150KConfig | None = None
WORKER_DATA_VALIDATION: dict[str, Any] = {}
WORKER_LEAK: dict[str, tuple[bool, str]] = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gate-aware HYDRA remediation and blind-validation governance orchestrator.")
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--development-start", required=True)
    parser.add_argument("--development-end", required=True)
    parser.add_argument("--q2-start", required=True)
    parser.add_argument("--q2-end", required=True)
    parser.add_argument("--q3-start", required=True)
    parser.add_argument("--q3-end", required=True)
    parser.add_argument("--q4-start", required=True)
    parser.add_argument("--q4-end", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--databento-budget-usd", type=float, default=100.0)
    parser.add_argument("--databento-budget-start", default="2026-07-10")
    parser.add_argument("--auto-purchase-under-budget", action="store_true")
    parser.add_argument("--budget-safety-ceiling-usd", type=float, default=98.0)
    parser.add_argument("--primary-topstep-mode", default="no-dll")
    parser.add_argument("--evaluate-xfa-standard", action="store_true")
    parser.add_argument("--evaluate-xfa-consistency", action="store_true")
    parser.add_argument("--evaluate-optional-dll-sensitivity", action="store_true")
    parser.add_argument("--account-size", type=float, default=150000)
    parser.add_argument("--profit-target", type=float, default=9000)
    parser.add_argument("--mll-distance", type=float, default=4500)
    parser.add_argument("--workers", default="auto")
    parser.add_argument("--single-writer-registry", action="store_true")
    parser.add_argument("--runtime-hours", type=float, default=None, help="Deprecated alias for --max-runtime-hours.")
    parser.add_argument("--min-runtime-hours", type=float, default=0.0)
    parser.add_argument("--max-runtime-hours", type=float, default=6.0)
    parser.add_argument("--continue-until-deadline", action="store_true")
    parser.add_argument("--minimum-cycles", type=int, default=1)
    parser.add_argument("--minimum-remediation-children", type=int, default=0)
    parser.add_argument("--stop-only-on-valid-quality-target", action="store_true")
    parser.add_argument("--allow-early-stop-on-exhaustion", action="store_true")
    parser.add_argument("--checkpoint-every-minutes", type=float, default=20)
    parser.add_argument("--target-economic-strategy-units", type=int, default=50)
    parser.add_argument("--strict-lockbox", action="store_true")
    parser.add_argument("--conservative-intrabar", action="store_true")
    parser.add_argument("--seed", type=int, default=4050)
    parser.add_argument("--report-tag", required=True)
    parser.add_argument("--max-remediation-children", type=int, default=600)
    parser.add_argument("--cycle-size", type=int, default=150)
    parser.add_argument("--creative-exploration-ratio", type=float, default=0.10)
    parser.add_argument("--skip-data-purchase", action="store_true")
    args = parser.parse_args()
    if args.runtime_hours is not None:
        args.max_runtime_hours = args.runtime_hours
    if args.min_runtime_hours > args.max_runtime_hours:
        parser.error("--min-runtime-hours cannot exceed --max-runtime-hours")
    return args


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    report_dir = project_path("reports", "gate_aware_remediation")
    checkpoint_dir = project_path("reports", "checkpoints", "gate_aware_remediation")
    blind_dir = project_path("reports", "blind_validation")
    for folder in [report_dir, checkpoint_dir, blind_dir, project_path("reports", "portfolio"), project_path("reports", "lockbox")]:
        folder.mkdir(parents=True, exist_ok=True)
    conn = connect(args.registry)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise RuntimeError(f"Registry integrity failed: {integrity}")
    workers = resolve_workers(args.workers)
    run_control = RunControlConfig(
        min_runtime_hours=args.min_runtime_hours,
        max_runtime_hours=args.max_runtime_hours,
        continue_until_deadline=args.continue_until_deadline,
        minimum_cycles=args.minimum_cycles,
        minimum_remediation_children=args.minimum_remediation_children,
        stop_only_on_valid_quality_target=args.stop_only_on_valid_quality_target,
        allow_early_stop_on_exhaustion=args.allow_early_stop_on_exhaustion,
    )
    topstep_cfg = Topstep150KConfig(
        account_size=args.account_size,
        combine_starting_balance=args.account_size,
        combine_profit_target=args.profit_target,
        combine_max_loss_limit=args.mll_distance,
        no_daily_loss_limit=args.primary_topstep_mode == "no-dll",
    )
    budget = DatabentoBudgetConfig(
        budget_start=args.databento_budget_start,
        hard_cap_usd=args.databento_budget_usd,
        safety_ceiling_usd=args.budget_safety_ceiling_usd,
    )
    cache_audit = audit_cache(args)
    raw = load_development_data(args)
    data_validation = validate_ohlcv_frame(raw, timeframe="1m")
    states, leak = build_states(raw, args.symbols)
    enforce_data_access(
        period=f"{args.development_start}:{args.development_end}",
        role=DataRole.DEVELOPMENT,
        requesting_module="run_gate_aware_remediation_factory",
        candidate_ids=[],
        reason="Q1 development remediation and knowledge-base construction",
        freeze_manifest_hash=None,
    )
    rows = load_candidate_rows(conn)
    selected = select_priority_rows(rows)
    dossier_paths = write_dossiers([build_candidate_dossier(row) for row in selected])
    q1_manifest_path, q1_manifest_hash = freeze_candidates(
        "q1_remediation_freeze",
        selected[:100],
        cache_audit.get("checksums", {}),
        args,
    )
    enforce_data_access(
        period=f"{args.q2_start}:{args.q2_end}",
        role=DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION,
        requesting_module="run_gate_aware_remediation_factory",
        candidate_ids=[str(row["candidate_id"]) for row in selected[:100]],
        reason="Q2 confirmation may become development for modified lineages",
        freeze_manifest_hash=q1_manifest_hash,
    )
    q3_contaminated = lockbox_contaminated(f"{args.q3_start}:{args.q3_end}")
    q3_manifest_name = "q3_quarantined_confirmation_manifest" if q3_contaminated else "q3_blind_validation_freeze"
    q3_manifest_path, q3_manifest_hash = freeze_candidates(
        q3_manifest_name,
        selected[:50],
        cache_audit.get("checksums", {}),
        args,
    )
    q3_role = DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION if q3_contaminated else DataRole.BLIND_VALIDATION
    enforce_data_access(
        period=f"{args.q3_start}:{args.q3_end}",
        role=q3_role,
        requesting_module="run_gate_aware_remediation_factory",
        candidate_ids=[str(row["candidate_id"]) for row in selected[:50]],
        reason="Q3 contaminated by prior pre-freeze access; treated as confirmation data" if q3_contaminated else "Blind-validation governance manifest written; data loading deferred to validation workers",
        freeze_manifest_hash=None if q3_contaminated else q3_manifest_hash,
    )
    key = load_api_key()
    acquisition = acquire_governed_data(args, budget, key, q3_contaminated=q3_contaminated) if not args.skip_data_purchase else {"skipped": True}
    cache_audit = audit_cache(args)
    existing_fingerprints = {str(row["strategy_fingerprint"]) for row in rows if row.get("strategy_fingerprint")}
    run_stats = run_remediation_cycles(
        conn,
        selected,
        states,
        leak,
        data_validation,
        topstep_cfg,
        args,
        workers,
        existing_fingerprints,
        started,
        checkpoint_dir,
        run_control,
    )
    final_rows = load_candidate_rows(conn)
    clusters = cluster_summary(final_rows)
    portfolios = build_remediation_portfolio_candidates(final_rows)
    frontier = pareto_frontier(final_rows, limit=50)
    summary = build_summary(
        args,
        integrity,
        workers,
        started,
        rows,
        final_rows,
        selected,
        run_stats,
        dossier_paths,
        clusters,
        portfolios,
        frontier,
        cache_audit,
        acquisition,
        q1_manifest_path,
        q3_manifest_path,
        q3_contaminated,
    )
    checkpoint_path = write_checkpoint(checkpoint_dir, args.report_tag, summary)
    report_path = write_report(report_dir, args.report_tag, summary, checkpoint_path)
    print(json.dumps({"report_path": str(report_path), "checkpoint_path": str(checkpoint_path), **summary}, indent=2, sort_keys=True, default=str))
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


def audit_cache(args: argparse.Namespace) -> dict[str, Any]:
    cache_dir = project_path("data", "cache", "databento")
    files = sorted(cache_dir.glob("*")) if cache_dir.exists() else []
    checksums = {str(path): sha256_file(path) for path in files if path.is_file()}
    return {"cache_dir": str(cache_dir), "file_count": len(files), "files": [str(p) for p in files], "checksums": checksums}


def lockbox_contaminated(period: str) -> bool:
    path = project_path("reports", "data_access", "lockbox_contamination_events.jsonl")
    if not path.exists():
        return False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() and period in line:
            return True
    return False


def acquire_governed_data(
    args: argparse.Namespace,
    budget: DatabentoBudgetConfig,
    key: str | None,
    *,
    q3_contaminated: bool = False,
) -> dict[str, Any]:
    cfg = load_config()
    results: dict[str, Any] = {}
    q3_purpose = "Q3 quarantined confirmation cache; not blind validation" if q3_contaminated else "Q3 blind validation OHLCV after freeze"
    periods = [
        ("q2", args.q2_start, args.q2_end, "Q2 confirmation OHLCV"),
        ("q3", args.q3_start, args.q3_end, q3_purpose),
        ("q4", args.q4_start, args.q4_end, "Q4 final lockbox raw-only acquisition"),
    ]
    for name, start, end, purpose in periods:
        request = request_from_config(cfg, symbols=args.symbols, start=start, end=end, schema=args.schema, dataset=args.dataset)
        estimate = None
        if not Path(request.output_path).exists() and not Path(request.raw_output_path).exists():
            if key is None:
                results[name] = {"status": "missing_key_no_request", "request": request.to_dict()}
                continue
            estimate = estimate_request(request, key)
        decision = decide_databento_acquisition(
            request,
            budget,
            research_purpose=purpose,
            candidate_tier="validation_package",
            key=key,
            estimate=estimate,
        )
        if decision.cache_hit or not decision.may_download:
            results[name] = decision.__dict__
            continue
        if not args.auto_purchase_under_budget:
            results[name] = {**decision.__dict__, "status": "auto_purchase_disabled"}
            continue
        if name == "q4":
            download_historical_raw_only(request, key)
            record = record_download_complete(
                request,
                budget,
                decision.request_id,
                decision.estimated_cost_usd,
                decision.estimated_cost_usd,
                purpose,
                "validation_package",
                resulting_file=request.raw_output_path,
            )
            results[name] = {**decision.__dict__, "status": "raw_only_downloaded", "ledger": record.__dict__}
        else:
            result = download_historical_ohlcv(
                cfg,
                symbols=args.symbols,
                start=start,
                end=end,
                schema=args.schema,
                dataset=args.dataset,
                dry_run=False,
                max_cost_usd=max(decision.estimated_cost_usd + 0.01, 0.05),
            )
            record = record_download_complete(request, budget, decision.request_id, decision.estimated_cost_usd, decision.estimated_cost_usd, purpose, "validation_package")
            results[name] = {**decision.__dict__, "status": "downloaded", "download": result, "ledger": record.__dict__}
    write_budget_summary(budget)
    return results


def load_development_data(args: argparse.Namespace) -> pd.DataFrame:
    cfg = load_config()
    request = request_from_config(
        cfg,
        symbols=args.symbols,
        start=args.development_start,
        end=args.development_end,
        schema=args.schema,
        dataset=args.dataset,
    )
    path = Path(request.output_path)
    if not path.exists():
        candidates = sorted(project_path(request.cache_folder).glob(f"{args.dataset.replace('.', '-')}_{args.schema}_{'_'.join(args.symbols)}_2024-01-01_*.parquet"))
        if not candidates:
            raise FileNotFoundError("Q1 development cache missing; no Databento request made by this loader.")
        path = candidates[-1]
    df = load_cached_ohlcv(path, timeframe=request.timeframe)
    ts = pd.to_datetime(df["timestamp"], utc=True)
    start = pd.Timestamp(args.development_start, tz="UTC")
    end = pd.Timestamp(args.development_end, tz="UTC")
    return df[(ts >= start) & (ts < end) & (df["symbol"].isin(args.symbols))].reset_index(drop=True)


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


def load_candidate_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT * FROM candidates")]


def select_priority_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row["validation_status"] in {"TOPSTEP_VIABLE", "TOPSTEP_NEAR_MISS", "PROMISING_NEEDS_MUTATION"}:
            selected[row["candidate_id"]] = row
        if row.get("combine_profit_target_hit"):
            selected[row["candidate_id"]] = row
    econ = [row for row in rows if row["validation_status"] == "ECONOMICALLY_VIABLE"]
    for row in sorted(econ, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:200]:
        selected[row["candidate_id"]] = row
    one_gate = []
    for row in rows:
        if attribute_candidate_failure(row)["failed_gate_count"] == 1:
            one_gate.append(row)
    for row in sorted(one_gate, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:100]:
        selected[row["candidate_id"]] = row
    return sorted(selected.values(), key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)


def freeze_candidates(name: str, rows: list[dict[str, Any]], data_fingerprints: dict[str, str], args: argparse.Namespace) -> tuple[str, str]:
    manifest = build_manifest(
        manifest_type=name,
        candidate_rows=rows,
        source_code_commit=current_commit(),
        data_fingerprints=data_fingerprints,
        topstep_rule_version=load_topstep_rule_snapshot().rule_version_id,
        validation_thresholds={"topstep_mode": args.primary_topstep_mode, "conservative_intrabar": args.conservative_intrabar},
        expected_decision_policy={"no_q4_tuning": True, "no_threshold_weakening": True, "q1_cannot_promote_trading_ready": True},
    )
    path, digest = write_manifest(manifest)
    return str(path), digest


def run_remediation_cycles(
    conn: sqlite3.Connection,
    selected: list[dict[str, Any]],
    states: dict[str, pd.DataFrame],
    leak: dict[str, tuple[bool, str]],
    data_validation: dict[str, Any],
    topstep_cfg: Topstep150KConfig,
    args: argparse.Namespace,
    workers: int,
    existing_fingerprints: set[str],
    started: float,
    checkpoint_dir: Path,
    run_control: RunControlConfig,
) -> dict[str, Any]:
    deadline = started + args.max_runtime_hours * 3600
    parent_pool = list(selected)
    parent_scores = {str(row["candidate_id"]): float(row.get("promotion_score") or 0.0) for row in parent_pool}
    tested = 0
    improved = 0
    cycles = 0
    duplicate_attempts = 0
    generated_total = 0
    policy_counts: Counter[str] = Counter()
    policy_improvements: Counter[str] = Counter()
    policies_frozen: set[str] = set()
    last_stop = None
    next_checkpoint = time.monotonic() + max(args.checkpoint_every_minutes, 0.1) * 60
    while True:
        elapsed = time.monotonic() - started
        final_rows_for_stop = load_candidate_rows(conn)
        valid_quality = validated_quality_target_reached(
            trading_ready=sum(1 for row in final_rows_for_stop if row["validation_status"] == "TRADING_READY_CANDIDATE"),
            q4_passes=0,
            execution_passes=0,
            target_units=args.target_economic_strategy_units,
        )
        provisional_quality = len(parent_pool) >= args.target_economic_strategy_units
        stop = evaluate_stop(
            run_control,
            RunControlState(
                elapsed_seconds=elapsed,
                cycles_completed=cycles,
                remediation_children_completed=tested,
                queue_size=0,
                eligible_parents=len(parent_pool),
                valid_quality_target_reached=valid_quality,
                provisional_quality_target_reached=provisional_quality,
                proven_work_exhaustion=not parent_pool,
            ),
        )
        last_stop = stop
        if stop.should_stop:
            break
        cycle_work, cycle_duplicates = generate_cycle_work(parent_pool, existing_fingerprints, cycles, args, policies_frozen)
        duplicate_attempts += cycle_duplicates
        if not cycle_work:
            if args.allow_early_stop_on_exhaustion and time.monotonic() >= deadline:
                break
            time.sleep(5)
            cycles += 1
            continue
        generated_total += len(cycle_work)
        for hyp in cycle_work:
            policy_counts[hyp.policy.name] += 1
        results = evaluate_cycle(cycle_work, states, topstep_cfg, args.seed + cycles * 100_000, workers)
        for result in results:
            if time.monotonic() >= deadline and tested >= args.minimum_remediation_children:
                break
            promotion = store_child_result(conn, result, leak, data_validation, args, existing_fingerprints)
            tested += 1
            policy = result["hypothesis"].policy.name
            parent_score = parent_scores.get(str(result["hypothesis"].parent_candidate_id), 0.0)
            child_score = float(promotion.get("promotion_score") or 0.0)
            if child_score > parent_score:
                improved += 1
                policy_improvements[policy] += 1
            if (
                not args.continue_until_deadline
                and args.max_remediation_children > 0
                and tested >= args.max_remediation_children
                and tested >= args.minimum_remediation_children
            ):
                break
        cycles += 1
        final_rows = load_candidate_rows(conn)
        parent_pool = refresh_parent_pool(final_rows)
        parent_scores = {str(row["candidate_id"]): float(row.get("promotion_score") or 0.0) for row in parent_pool}
        policies_frozen = freeze_policies(policy_counts, policy_improvements)
        heartbeat = {
            "elapsed_runtime_seconds": round(time.monotonic() - started, 2),
            "remaining_runtime_seconds": round(max(deadline - time.monotonic(), 0.0), 2),
            "cycle_number": cycles,
            "queue_size": len(cycle_work),
            "eligible_parents": len(parent_pool),
            "children_generated_this_cycle": len(cycle_work),
            "cumulative_children": tested,
            "duplicate_rate": round(duplicate_attempts / max(generated_total + duplicate_attempts, 1), 6),
            "parent_to_child_improvement_rate": round(improved / max(tested, 1), 6),
            "policies_expanded": dict(policy_counts),
            "policies_frozen": sorted(policies_frozen),
            "current_stop_conditions": last_stop.diagnostics if last_stop else {},
            "quality_target_validated": valid_quality,
            "quality_target_provisional": provisional_quality,
        }
        print(json.dumps({"heartbeat": heartbeat}, sort_keys=True), flush=True)
        if time.monotonic() >= next_checkpoint:
            write_checkpoint(checkpoint_dir, args.report_tag, {"status": "running", **heartbeat})
            next_checkpoint = time.monotonic() + max(args.checkpoint_every_minutes, 0.1) * 60
        if (
            not args.continue_until_deadline
            and args.max_remediation_children > 0
            and tested >= args.max_remediation_children
            and tested >= args.minimum_remediation_children
        ):
            if not args.continue_until_deadline and time.monotonic() >= run_control.min_runtime_hours * 3600 + started:
                break
    return {
        "remediation_children_tested": tested,
        "remediation_children_generated": generated_total,
        "parent_to_child_improved": improved,
        "cycles_completed": cycles,
        "duplicate_attempts": duplicate_attempts,
        "duplicate_rate": round(duplicate_attempts / max(generated_total + duplicate_attempts, 1), 6),
        "policy_counts": dict(policy_counts),
        "policy_improvements": dict(policy_improvements),
        "policies_frozen": sorted(policies_frozen),
        "last_stop_reason": last_stop.reason if last_stop else "unknown",
        "last_stop_diagnostics": last_stop.diagnostics if last_stop else {},
    }


def generate_cycle_work(
    parent_pool: list[dict[str, Any]],
    existing_fingerprints: set[str],
    cycle: int,
    args: argparse.Namespace,
    frozen_policies: set[str] | None = None,
) -> tuple[list[Any], int]:
    if not parent_pool:
        return [], 0
    work = []
    duplicates = 0
    cycle_fingerprints: set[str] = set()
    attempts = 0
    max_attempts = max(args.cycle_size * 20, args.cycle_size)
    frozen_policies = frozen_policies or set()
    active_policies = [policy for policy in POLICIES if policy.name not in frozen_policies] or list(POLICIES)
    while len(work) < args.cycle_size and attempts < max_attempts:
        parent = parent_pool[(cycle * args.cycle_size + attempts) % len(parent_pool)]
        creative = (attempts / max(args.cycle_size, 1)) < max(0.0, min(args.creative_exploration_ratio, 0.5))
        policy_name = active_policies[(cycle + attempts) % len(active_policies)].name if creative else None
        hyp = child_from_registry_row(parent, variant=cycle * max_attempts + attempts, policy_name=policy_name)
        if hyp.policy.name in frozen_policies:
            policy_name = active_policies[(cycle + attempts) % len(active_policies)].name
            hyp = child_from_registry_row(parent, variant=cycle * max_attempts + attempts, policy_name=policy_name)
        fingerprint = strategy_fingerprint(hyp.child)
        if fingerprint in existing_fingerprints or fingerprint in cycle_fingerprints:
            duplicates += 1
            attempts += 1
            continue
        cycle_fingerprints.add(fingerprint)
        work.append(hyp)
        attempts += 1
    return work, duplicates


def evaluate_cycle(cycle_work, states, topstep_cfg, seed_base: int, workers: int) -> list[dict[str, Any]]:
    if workers <= 1:
        return [evaluate_child(hyp, states, topstep_cfg, seed_base + i) for i, hyp in enumerate(cycle_work)]
    with ProcessPoolExecutor(max_workers=workers, initializer=init_worker, initargs=(states, topstep_cfg)) as pool:
        futures = [pool.submit(worker_evaluate_child, hyp, seed_base + i) for i, hyp in enumerate(cycle_work)]
        return [future.result() for future in as_completed(futures)]


def refresh_parent_pool(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return select_priority_rows(rows)[:750]


def freeze_policies(policy_counts: Counter[str], policy_improvements: Counter[str]) -> set[str]:
    frozen = set()
    for name, count in policy_counts.items():
        if count >= 200 and policy_improvements.get(name, 0) / max(count, 1) < 0.02:
            frozen.add(name)
    return frozen


def init_worker(states: dict[str, pd.DataFrame], topstep_cfg: Topstep150KConfig) -> None:
    global WORKER_STATES, WORKER_TOPSTEP_CFG
    WORKER_STATES = states
    WORKER_TOPSTEP_CFG = topstep_cfg


def worker_evaluate_child(hypothesis, seed: int) -> dict[str, Any]:
    if WORKER_TOPSTEP_CFG is None:
        raise RuntimeError("Worker not initialized")
    return evaluate_child(hypothesis, WORKER_STATES, WORKER_TOPSTEP_CFG, seed)


def evaluate_child(hypothesis, states: dict[str, pd.DataFrame], topstep_cfg: Topstep150KConfig, seed: int) -> dict[str, Any]:
    child = hypothesis.child
    df = states[child.symbol]
    result = run_backtest(child, df, seed)
    overlay = InternalRiskOverlay(
        daily_stop=float(child.risk_parameters.get("internal_daily_stop", 1000)),
        daily_profit_lock=float(child.risk_parameters.get("daily_profit_lock", 1500)),
    )
    daily = trades_to_topstep_daily(result.trades, df, overlay)
    evaluation = evaluate_topstep_150k(result.trades, df, topstep_cfg, overlay, split_daily=split_daily_frames(daily))
    topstep_record = evaluation.to_record()
    pass_path = analyze_pass_path(topstep_record, topstep_cfg.combine_profit_target, topstep_cfg.combine_max_loss_limit)
    topstep_record["pass_path_diagnosis"] = pass_path.diagnosis
    metrics = dict(result.metrics)
    metrics["net_profit"] = topstep_record["adjusted_net_profit"]
    metrics["max_drawdown"] = max_drawdown(pd.Series(daily["pnl"].cumsum(), dtype=float)) if len(daily) else 0.0
    metrics["trade_count"] = topstep_record["trade_count"]
    return {
        "hypothesis": hypothesis,
        "parent": {"candidate_id": hypothesis.parent_candidate_id},
        "candidate": child,
        "result": result,
        "daily": daily,
        "topstep_record": topstep_record,
        "metrics": metrics,
        "seed": seed,
    }


def store_child_result(conn, result, leak, data_validation, args, existing_fingerprints):
    child = result["candidate"]
    topstep_record = result["topstep_record"]
    promotion = run_promotion_pipeline(
        PromotionInput(
            candidate=child,
            result=result["result"],
            daily=result["daily"],
            topstep_record=topstep_record,
            data_validation=data_validation,
            split_scores=topstep_record.get("split_scores", {}),
            leak_ok=leak[child.symbol][0],
            leak_reason=leak[child.symbol][1],
            existing_fingerprints=existing_fingerprints,
            max_correlation=0.0,
            seed=result["seed"],
            lane="gate_aware_remediation",
            report_tag=args.report_tag,
        )
    )
    upsert_topstep_candidate(conn, child, result["metrics"], topstep_record["status"], topstep_record["rejection_reason"], topstep_record, robustness=topstep_record["topstep_score"])
    update_promotion_metadata(conn, child.candidate_id, promotion)
    if promotion.get("strategy_fingerprint"):
        existing_fingerprints.add(promotion["strategy_fingerprint"])
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


def build_summary(args, integrity, workers, started, base_rows, final_rows, selected, run_stats, dossier_paths, clusters, portfolios, frontier, cache_audit, acquisition, q1_manifest_path, q3_manifest_path, q3_contaminated):
    attributions = [attribute_candidate_failure(row) for row in selected]
    effective_trials = effective_independent_trials(len(final_rows), len(clusters))
    ledger = read_ledger(project_path("reports/data_budget/databento_spend_ledger.jsonl"))
    status_counts = {}
    for row in final_rows:
        status_counts[row["validation_status"]] = status_counts.get(row["validation_status"], 0) + 1
    return {
        "baseline_commit": "bad3b5e77400ae676f1d7c0c047eb9fd3e41e9c1",
        "current_commit": current_commit(),
        "registry_integrity": integrity,
        "runtime_seconds": round(time.monotonic() - started, 2),
        "workers_used": workers,
        "starting_candidates": len(base_rows),
        "total_candidates": len(final_rows),
        "remediation_children_tested": run_stats["remediation_children_tested"],
        "remediation_children_generated": run_stats["remediation_children_generated"],
        "cycles_completed": run_stats["cycles_completed"],
        "duplicate_rate": run_stats["duplicate_rate"],
        "parent_to_child_improvement_rate": round(run_stats["parent_to_child_improved"] / max(run_stats["remediation_children_tested"], 1), 6),
        "policy_counts": run_stats["policy_counts"],
        "policy_improvements": run_stats["policy_improvements"],
        "policies_frozen": run_stats["policies_frozen"],
        "last_stop_reason": run_stats["last_stop_reason"],
        "last_stop_diagnostics": run_stats["last_stop_diagnostics"],
        "dossiers_generated": len(dossier_paths),
        "topstep_viable_reaudited": sum(1 for row in selected if row["validation_status"] == "TOPSTEP_VIABLE"),
        "near_misses_analyzed": sum(1 for row in selected if row["validation_status"] in {"PROMISING_NEEDS_MUTATION", "TOPSTEP_NEAR_MISS"}),
        "economically_viable_analyzed": sum(1 for row in selected if row["validation_status"] == "ECONOMICALLY_VIABLE"),
        "hard_invalid_count": sum(1 for item in attributions if item["policy_classification"] == "HARD_INVALID"),
        "repairable_count": sum(1 for item in attributions if item["policy_classification"] == "REPAIRABLE_NEAR_MISS"),
        "candidates_failing_exactly_one_gate": sum(1 for item in attributions if item["failed_gate_count"] == 1),
        "candidates_failing_exactly_two_gates": sum(1 for item in attributions if item["failed_gate_count"] == 2),
        "q1_repair_parent_pool_size": len(selected),
        "q1_promotion_finalists": 0,
        "q1_promotion_finalists_status": "not_assigned_by_dossier_or_repairability_selection",
        "q2_confirmed_candidates": 0,
        "q3_blind_validation_passes": "not_applicable_q3_quarantined" if q3_contaminated else 0,
        "q3_confirmation_passes": 0,
        "execution_validation_passes": 0,
        "q4_lockbox_passes": 0,
        "trading_ready_candidates": status_counts.get("TRADING_READY_CANDIDATE", 0),
        "economic_strategy_units": 0,
        "economic_strategy_units_status": "not_validated_without_trade_level_behavioral_clustering",
        "equivalence_clusters": len(clusters),
        "best_pareto_candidates": [row["candidate_id"] for row in frontier[:20]],
        "portfolio_baskets": portfolios,
        "status_distribution": status_counts,
        "family_fdr_proxy": family_false_discovery_proxy(final_rows),
        "effective_independent_trials_proxy": effective_trials,
        "selection_adjusted_best_promotion_proxy": selection_adjusted_score(max(float(row.get("promotion_score") or 0.0) for row in final_rows), len(final_rows), effective_trials),
        "cache_audit": cache_audit,
        "data_acquisition": acquisition,
        "budget_ledger_records": len(ledger),
        "q1_manifest_path": q1_manifest_path,
        "q3_manifest_path": q3_manifest_path,
        "q3_lockbox_contaminated": q3_contaminated,
        "lockbox_integrity": "Q3 contaminated for affected lineages; Q4 raw-only remains uninspected" if q3_contaminated else "Q3 freeze-before-access enforced; Q4 raw-only remains uninspected",
        "rule_snapshot_path": DEFAULT_TOPSTEP_RULE_PATH,
        "budget_ledger_path": "reports/data_budget/databento_spend_ledger.jsonl",
        "data_access_ledger_path": "reports/data_access/data_access_ledger.jsonl",
        "checkpoint_folder": "reports/checkpoints/gate_aware_remediation",
        "resume_command": resume_command(args),
    }


def write_checkpoint(folder: Path, tag: str, summary: dict[str, Any]) -> Path:
    path = folder / f"gate_aware_checkpoint_{utc_now_iso().replace(':','').replace('+','Z')}_{tag}.md"
    lines = ["# Gate-Aware Remediation Checkpoint", "", f"Generated: {utc_now_iso()}", ""]
    for key, value in summary.items():
        if key in {"family_fdr_proxy", "cache_audit", "data_acquisition"}:
            continue
        lines.append(f"- {key}: {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_report(folder: Path, tag: str, summary: dict[str, Any], checkpoint_path: Path) -> Path:
    path = folder / f"gate_aware_remediation_report_{utc_now_iso().replace(':','').replace('+','Z')}_{tag}.md"
    lines = [
        "# HYDRA Gate-Aware Remediation Report",
        "",
        f"Generated: {utc_now_iso()}",
        "",
        "## Warning",
        "- This is historical research only. It is not live trading approval.",
        "- Q1/Q2 evidence cannot create TRADING_READY_CANDIDATE status.",
        "- Q4 lockbox remains unavailable for tuning.",
        "",
        "## Summary",
    ]
    for key, value in summary.items():
        if isinstance(value, (dict, list)):
            lines.append(f"- {key}: `{json.dumps(value, sort_keys=True, default=str)[:4000]}`")
        else:
            lines.append(f"- {key}: {value}")
    lines.append(f"- checkpoint_path: {checkpoint_path}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def resume_command(args: argparse.Namespace) -> str:
    return (
        "python scripts/run_gate_aware_remediation_factory.py "
        f"--registry {args.registry} --dataset {args.dataset} --symbols {' '.join(args.symbols)} "
        f"--development-start {args.development_start} --development-end {args.development_end} "
        f"--q2-start {args.q2_start} --q2-end {args.q2_end} --q3-start {args.q3_start} --q3-end {args.q3_end} "
        f"--q4-start {args.q4_start} --q4-end {args.q4_end} --schema {args.schema} "
        f"--databento-budget-usd {args.databento_budget_usd:g} --databento-budget-start {args.databento_budget_start} "
        "--auto-purchase-under-budget "
        f"--budget-safety-ceiling-usd {args.budget_safety_ceiling_usd:g} --primary-topstep-mode {args.primary_topstep_mode} "
        "--evaluate-xfa-standard --evaluate-xfa-consistency --evaluate-optional-dll-sensitivity "
        f"--account-size {args.account_size:g} --profit-target {args.profit_target:g} --mll-distance {args.mll_distance:g} "
        f"--workers {args.workers} --single-writer-registry --min-runtime-hours {args.min_runtime_hours:g} --max-runtime-hours {args.max_runtime_hours:g} "
        "--continue-until-deadline "
        f"--minimum-cycles {args.minimum_cycles} --minimum-remediation-children {args.minimum_remediation_children} "
        "--stop-only-on-valid-quality-target "
        f"--checkpoint-every-minutes {args.checkpoint_every_minutes:g} --target-economic-strategy-units {args.target_economic_strategy_units} "
        f"--max-remediation-children {args.max_remediation_children} --cycle-size {args.cycle_size} "
        f"--creative-exploration-ratio {args.creative_exploration_ratio:g} "
        "--strict-lockbox --conservative-intrabar "
        f"--seed {args.seed + 1000} --report-tag {args.report_tag}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
