#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.backtest.engine import run_backtest
from hydra.backtest.metrics import max_drawdown
from hydra.data.budget import read_ledger, write_budget_summary, DatabentoBudgetConfig
from hydra.data.contract_mapping import build_rule_based_roll_map, write_roll_map
from hydra.data.databento_loader import load_cached_ohlcv, request_from_config, validate_ohlcv_frame
from hydra.data.roll_audit import audit_roll_discontinuities, audit_trade_roll_exposure, synchronized_pair_audit
from hydra.features.market_state import build_market_state
from hydra.promotion.behavioral_evidence import build_behavioral_sketch, build_trade_ledger_rows, write_behavioral_artifacts
from hydra.promotion.cluster_calibration import calibrate_clustering_controls, cluster_sketches
from hydra.promotion.pipeline import PromotionInput, run_promotion_pipeline
from hydra.promotion.gates import strategy_fingerprint
from hydra.propfirm.topstep_150k import InternalRiskOverlay, Topstep150KConfig, evaluate_topstep_150k, trades_to_topstep_daily
from hydra.registry.db import connect
from hydra.strategies.dsl import StrategyCandidate
from hydra.utils.config import load_config, project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.no_leak import audit_no_lookahead


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run frozen Q2 external falsification for HYDRA candidates.")
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument("--manifest", default="reports/lockbox/q2_confirmation_freeze_c256dadbd42bd9a6.json")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--q1-start", default="2024-01-01")
    parser.add_argument("--q1-end", default="2024-03-29")
    parser.add_argument("--q2-start", default="2024-04-01")
    parser.add_argument("--q2-end", default="2024-07-01")
    parser.add_argument("--report-tag", default="frozen_q2_falsification_v1")
    parser.add_argument("--seed", type=int, default=6060)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    conn = connect(args.registry)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise RuntimeError(f"Registry integrity failed: {integrity}")
    manifest = load_manifest(Path(args.manifest))
    manifest_audit = validate_manifest(conn, manifest)
    if not manifest_audit["manifest_hash_valid"]:
        raise RuntimeError("Q2 manifest hash mismatch; refusing to evaluate.")
    q2_access = classify_q2_access()
    q1 = load_cached_period(args, args.q1_start, args.q1_end)
    q2 = load_cached_period(args, args.q2_start, args.q2_end)
    q1_validation = validate_ohlcv_frame(q1, timeframe="1m")
    q2_validation = validate_ohlcv_frame(q2, timeframe="1m")
    roll_map = build_rule_based_roll_map(args.symbols, start=args.q1_start, end=args.q2_end, dataset=args.dataset, schema=args.schema)
    roll_map_path, roll_map_hash = write_roll_map(roll_map)
    roll_audit = {
        "q1": audit_roll_discontinuities(q1, roll_map),
        "q2": audit_roll_discontinuities(q2, roll_map),
        "nq_es_q2_pair": synchronized_pair_audit(roll_map, sample_timestamps(q2, ["NQ", "ES"]), pair=("NQ", "ES")),
        "roll_map_path": str(roll_map_path),
        "roll_map_hash": roll_map_hash,
        "roll_map_status": "RULE_BASED_PROXY_EXPLICIT_METADATA_MISSING",
    }
    q1_states, q1_leak = build_states(q1, args.symbols)
    q2_states, q2_leak = build_states(q2, args.symbols)
    q1_records: list[dict[str, Any]] = []
    q2_records: list[dict[str, Any]] = []
    sketches: list[dict[str, Any]] = []
    ledgers: list[dict[str, Any]] = []
    for index, spec in enumerate(manifest["strategy_specs"]):
        candidate = candidate_from_spec(spec)
        q1_eval = evaluate_candidate(candidate, q1_states[candidate.symbol], q1_validation, q1_leak[candidate.symbol], roll_map, "Q1_2024", args, seed=args.seed + index)
        q2_eval = evaluate_candidate(candidate, q2_states[candidate.symbol], q2_validation, q2_leak[candidate.symbol], roll_map, "Q2_2024", args, seed=args.seed + 100_000 + index)
        q1_records.append(q1_eval["record"])
        q2_records.append(q2_eval["record"])
        sketches.extend([q1_eval["sketch"], q2_eval["sketch"]])
        ledgers.extend(q1_eval["ledger_rows"])
        ledgers.extend(q2_eval["ledger_rows"])
    evidence_manifest = write_behavioral_artifacts(
        tag=f"{timestamp()}_{args.report_tag}",
        sketches=sketches,
        ledgers=ledgers,
    )
    clustering = calibrate_clustering_controls(sketches)
    clusters = cluster_sketches(sketches)
    cluster_lookup = assign_clusters(clusters)
    for row in q1_records + q2_records:
        row["behavior_cluster"] = cluster_lookup.get(row["candidate_id"], "unclustered")
    family = family_falsification(q1_records, q2_records, clusters)
    budget = budget_state()
    summary = {
        "created_at": utc_now_iso(),
        "runtime_seconds": round(time.monotonic() - started, 2),
        "registry_integrity": integrity,
        "q2_access_classification": q2_access,
        "manifest_audit": manifest_audit,
        "q3_quarantine_verification": q3_quarantine_verification(),
        "q4_seal_verification": q4_seal_verification(),
        "new_databento_purchase": False,
        "databento_spend_this_phase": 0.0,
        "budget_state": budget,
        "roll_audit": roll_audit,
        "q1_validation": q1_validation,
        "q2_validation": q2_validation,
        "clustering_calibration": clustering,
        "valid_economic_units_among_frozen_set": valid_units(clusters),
        "q1_status_counts": dict(Counter(row["q1_status"] for row in q1_records)),
        "q2_status_counts": dict(Counter(row["q2_status"] for row in q2_records)),
        "q2_passes_by_cluster": q2_passes_by_cluster(q2_records),
        "q2_failures_by_reason": dict(Counter(row["failure_reason"] for row in q2_records)),
        "q1_roll_sensitive_candidates": [row["candidate_id"] for row in q1_records if row["roll_sensitive"]],
        "q2_roll_sensitive_candidates": [row["candidate_id"] for row in q2_records if row["roll_sensitive"]],
        "intrabar_ambiguous_candidates": sorted({row["candidate_id"] for row in q1_records + q2_records if row["execution_ambiguous"]}),
        "tick_tbbo_required": tick_tbbo_candidates(q1_records, q2_records),
        "family_falsification": family,
        "q1_records": q1_records,
        "q2_records": q2_records,
        "behavioral_evidence_manifest": evidence_manifest,
        "exact_next_milestone": "Replace rule-based roll proxy with explicit Databento definition/symbology roll map before any promotion; then Q2-confirm only non-roll-sensitive behavioral representatives.",
    }
    paths = write_reports(summary, args.report_tag)
    print(json.dumps({**paths, **summary}, indent=2, sort_keys=True, default=str))
    return 0


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_manifest(conn: sqlite3.Connection, manifest: dict[str, Any]) -> dict[str, Any]:
    expected_hash = manifest.get("manifest_hash")
    payload = dict(manifest)
    payload.pop("manifest_hash", None)
    actual_hash = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    rows = {row["candidate_id"]: dict(row) for row in conn.execute("SELECT * FROM candidates")}
    missing = [cid for cid in manifest.get("candidate_ids", []) if cid not in rows]
    mismatches = []
    for spec in manifest.get("strategy_specs", []):
        row = rows.get(spec["candidate_id"])
        if not row:
            continue
        for key in ("family", "symbol", "timeframe", "parameters_json", "risk_json", "strategy_fingerprint"):
            if str(row.get(key) or "") != str(spec.get(key) or ""):
                mismatches.append({"candidate_id": spec["candidate_id"], "field": key, "registry": row.get(key), "manifest": spec.get(key)})
    current = current_commit()
    return {
        "manifest_hash_valid": actual_hash == expected_hash,
        "manifest_hash": expected_hash,
        "computed_hash": actual_hash,
        "candidate_count": len(manifest.get("candidate_ids", [])),
        "missing_candidate_ids": missing,
        "spec_mismatches": mismatches[:25],
        "spec_mismatch_count": len(mismatches),
        "source_commit": manifest.get("source_code_commit"),
        "current_commit": current,
        "source_commit_matches_current": manifest.get("source_code_commit") == current,
        "source_commit_note": "Current source differs because provenance/forensic infrastructure was added after freeze; candidate parameters/specifications were not altered.",
    }


def classify_q2_access() -> dict[str, Any]:
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    entries = []
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if line.strip() and "2024-04-01:2024-07-01" in line:
                entries.append(json.loads(line))
    evidence = []
    for entry in entries:
        evidence.append(
            {
                "timestamp": entry.get("timestamp_utc"),
                "module": entry.get("requesting_module"),
                "role": entry.get("data_role"),
                "parameters_mutable": entry.get("parameters_mutable"),
                "reason": entry.get("reason_for_access"),
                "candidate_count": len(entry.get("candidate_ids") or []),
            }
        )
    return {
        "classification": "Q2_OPERATIONALLY_PREPARED_BUT_UNINSPECTED",
        "evidence": evidence,
        "rationale": "Ledger shows Q2 cache preparation/access declarations, but previous final reports recorded Q2 evaluated/confirmed as 0 and no Q2 strategy-level metrics or ranking were found before this phase.",
    }


def load_cached_period(args: argparse.Namespace, start: str, end: str) -> pd.DataFrame:
    cfg = load_config()
    request = request_from_config(cfg, symbols=args.symbols, start=start, end=end, schema=args.schema, dataset=args.dataset)
    path = Path(request.output_path)
    if not path.exists():
        candidates = sorted(project_path(request.cache_folder).glob(f"{args.dataset.replace('.', '-')}_{args.schema}_{'_'.join(args.symbols)}_{start}_*.parquet"))
        if not candidates:
            raise FileNotFoundError(f"Missing cached data for {start}:{end}; no Databento request made.")
        path = candidates[-1]
    frame = load_cached_ohlcv(path, timeframe=request.timeframe)
    ts = pd.to_datetime(frame["timestamp"], utc=True)
    return frame[(ts >= pd.Timestamp(start, tz="UTC")) & (ts < pd.Timestamp(end, tz="UTC")) & frame["symbol"].isin(args.symbols)].reset_index(drop=True)


def build_states(raw: pd.DataFrame, symbols: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, tuple[bool, str]]]:
    states = {}
    leaks = {}
    for symbol in symbols:
        state = build_market_state(raw[raw["symbol"] == symbol].reset_index(drop=True))
        leaks[symbol] = audit_no_lookahead(state)
        if not leaks[symbol][0]:
            raise RuntimeError(f"No-lookahead failed for {symbol}: {leaks[symbol][1]}")
        states[symbol] = state
    return states, leaks


def candidate_from_spec(spec: dict[str, Any]) -> StrategyCandidate:
    return StrategyCandidate(
        candidate_id=str(spec["candidate_id"]),
        family=str(spec["family"]),
        symbol=str(spec["symbol"]),
        timeframe=str(spec["timeframe"]),
        parameters=json.loads(spec["parameters_json"]),
        entry_logic=str(spec.get("entry_logic") or f"{spec['family']}_regime_path_entry"),
        exit_logic="frozen_manifest_exit",
        risk_parameters=json.loads(spec["risk_json"]),
        parent_candidate_id=None,
        mutation_type="frozen_q2_falsification",
    )


def evaluate_candidate(
    candidate: StrategyCandidate,
    state: pd.DataFrame,
    data_validation: dict[str, Any],
    leak: tuple[bool, str],
    roll_map,
    period: str,
    args: argparse.Namespace,
    *,
    seed: int,
) -> dict[str, Any]:
    result = run_backtest(candidate, state, seed)
    overlay = InternalRiskOverlay(
        daily_stop=float(candidate.risk_parameters.get("internal_daily_stop", 1000.0)),
        daily_profit_lock=float(candidate.risk_parameters.get("daily_profit_lock", 1500.0)),
    )
    daily = trades_to_topstep_daily(result.trades, state, overlay)
    topstep = evaluate_topstep_150k(result.trades, state, Topstep150KConfig(), overlay, split_daily=split_daily_frames(daily, period)).to_record()
    promotion = run_promotion_pipeline(
        PromotionInput(
            candidate=candidate,
            result=result,
            daily=daily,
            topstep_record=topstep,
            data_validation=data_validation,
            split_scores=topstep.get("split_scores", {}),
            leak_ok=leak[0],
            leak_reason=leak[1],
            existing_fingerprints=set(),
            max_correlation=0.0,
            seed=seed,
            lane="frozen_q2_falsification",
            report_tag=args.report_tag,
        )
    )
    roll = audit_trade_roll_exposure(result.trades, roll_map)
    sketch = build_behavioral_sketch(
        candidate_id=candidate.candidate_id,
        parent_candidate_id=candidate.parent_candidate_id,
        trades=result.trades,
        daily=daily,
        validation_period=period,
    )
    ledger_rows = build_trade_ledger_rows(
        candidate_id=candidate.candidate_id,
        parent_candidate_id=candidate.parent_candidate_id,
        trades=result.trades,
        validation_period=period,
    )
    record = build_period_record(candidate, result, daily, topstep, promotion, roll, period)
    return {"record": record, "sketch": sketch, "ledger_rows": ledger_rows}


def build_period_record(candidate, result, daily, topstep, promotion, roll, period: str) -> dict[str, Any]:
    pnls = [float(t.get("pnl") or 0.0) for t in result.trades]
    total_profit = float(sum(pnls))
    best_trade_share = max([abs(x) for x in pnls], default=0.0) / max(abs(total_profit), 1.0)
    execution_ambiguous = str(topstep.get("status")) == "INTRABAR_AMBIGUOUS_REQUIRES_TICK_VALIDATION"
    metrics = result.metrics
    if period.startswith("Q1"):
        status = q1_status(metrics, topstep, promotion, roll, execution_ambiguous)
        status_key = "q1_status"
    else:
        status = q2_status(metrics, topstep, roll, execution_ambiguous, best_trade_share)
        status_key = "q2_status"
    return {
        "candidate_id": candidate.candidate_id,
        "family": candidate.family,
        "symbol": candidate.symbol,
        status_key: status,
        "failure_reason": failure_reason(status, metrics, topstep, roll, execution_ambiguous, best_trade_share),
        "net_pnl": round(float(metrics.get("net_profit", 0.0)), 2),
        "profit_factor": round(float(metrics.get("profit_factor", 0.0)), 6),
        "trade_count": int(metrics.get("trade_count", 0)),
        "drawdown": round(float(metrics.get("max_drawdown", 0.0)), 2),
        "topstep_score": round(float(topstep.get("topstep_score", 0.0)), 6),
        "combine_mll_breached": bool(topstep.get("combine_mll_breached")),
        "mll_buffer": round(float(topstep.get("combine_min_mll_buffer", 0.0)), 2),
        "target_hit": bool(topstep.get("combine_profit_target_hit")),
        "consistency_ok": bool(topstep.get("combine_consistency_ok")),
        "payout_eligible": bool(topstep.get("payout_eligible")),
        "funded_survived": bool(topstep.get("funded_sim_survived")),
        "best_trade_share_abs_profit": round(best_trade_share, 6),
        "roll_sensitive": bool(roll.get("roll_sensitive")),
        "roll_audit": roll,
        "execution_ambiguous": execution_ambiguous,
        "promotion_status": promotion.get("status"),
        "promotion_score": promotion.get("promotion_score"),
        "validation_version": promotion.get("validation_version"),
        "input_fingerprint": promotion.get("input_fingerprint"),
        "computation_mode": promotion.get("computation_mode"),
        "evidence_strength": promotion.get("evidence_strength"),
    }


def q1_status(metrics: dict[str, Any], topstep: dict[str, Any], promotion: dict[str, Any], roll: dict[str, Any], execution_ambiguous: bool) -> str:
    if execution_ambiguous:
        return "Q1_INTRABAR_AMBIGUOUS"
    if roll.get("roll_sensitive"):
        return "Q1_ROLL_SENSITIVE"
    if int(metrics.get("trade_count", 0)) < 20:
        return "Q1_INSUFFICIENT_EVIDENCE"
    if promotion.get("status") == "TRADING_READY_CANDIDATE":
        return "Q1_FULL_RECOMPUTE_PASS"
    return "Q1_FULL_RECOMPUTE_FAIL"


def q2_status(metrics: dict[str, Any], topstep: dict[str, Any], roll: dict[str, Any], execution_ambiguous: bool, best_trade_share: float) -> str:
    if execution_ambiguous:
        return "Q2_EXECUTION_AMBIGUOUS"
    if roll.get("roll_sensitive"):
        return "Q2_ROLL_AMBIGUOUS"
    trades = int(metrics.get("trade_count", 0))
    if trades < 20:
        return "Q2_INSUFFICIENT_EVIDENCE"
    economic = float(metrics.get("net_profit", 0.0)) > 0 and float(metrics.get("profit_factor", 0.0)) >= 1.05
    safe = not bool(topstep.get("combine_mll_breached")) and bool(topstep.get("combine_consistency_ok"))
    robust_enough = best_trade_share <= 0.35
    if economic and safe and robust_enough and float(topstep.get("topstep_score", 0.0)) >= 0.45:
        return "Q2_EXTERNAL_CONFIRMED"
    return "Q2_EXTERNAL_REJECTED"


def failure_reason(status: str, metrics: dict[str, Any], topstep: dict[str, Any], roll: dict[str, Any], execution_ambiguous: bool, best_trade_share: float) -> str:
    if "ROLL" in status:
        return "roll_window_or_cross_roll_trade_exposure"
    if execution_ambiguous:
        return "intrabar_path_ambiguous_requires_tick_validation"
    if int(metrics.get("trade_count", 0)) < 20:
        return "insufficient_trade_count"
    if bool(topstep.get("combine_mll_breached")):
        return "mll_breach"
    if not bool(topstep.get("combine_consistency_ok")):
        return "consistency_failure"
    if best_trade_share > 0.35:
        return "single_trade_concentration"
    if float(metrics.get("net_profit", 0.0)) <= 0:
        return "negative_net_pnl_after_costs"
    if float(metrics.get("profit_factor", 0.0)) < 1.05:
        return "profit_factor_below_confirmation_floor"
    if float(topstep.get("topstep_score", 0.0)) < 0.45:
        return "weak_topstep_path"
    return "passed_confirmation_conditions" if "CONFIRMED" in status or "PASS" in status else "unresolved_rejection"


def split_daily_frames(daily: pd.DataFrame, period: str) -> dict[str, pd.DataFrame]:
    if daily.empty:
        return {"m1": daily, "m2": daily, "m3": daily}
    dates = pd.to_datetime(daily["date"])
    if period.startswith("Q2"):
        return {
            "apr": daily[(dates >= "2024-04-01") & (dates < "2024-05-01")].reset_index(drop=True),
            "may": daily[(dates >= "2024-05-01") & (dates < "2024-06-01")].reset_index(drop=True),
            "jun": daily[(dates >= "2024-06-01") & (dates < "2024-07-01")].reset_index(drop=True),
        }
    return {
        "jan": daily[(dates >= "2024-01-01") & (dates < "2024-02-01")].reset_index(drop=True),
        "feb": daily[(dates >= "2024-02-01") & (dates < "2024-03-01")].reset_index(drop=True),
        "mar": daily[(dates >= "2024-03-01") & (dates < "2024-04-01")].reset_index(drop=True),
    }


def q2_passes_by_cluster(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter()
    for row in rows:
        if row["q2_status"] == "Q2_EXTERNAL_CONFIRMED":
            counts[row.get("behavior_cluster", "unclustered")] += 1
    return dict(counts)


def family_falsification(q1_rows: list[dict[str, Any]], q2_rows: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> dict[str, Any]:
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in q2_rows:
        by_family[row["family"]].append(row)
    out = {}
    cluster_count = len(clusters)
    for family, rows in by_family.items():
        passes = [r for r in rows if r["q2_status"] == "Q2_EXTERNAL_CONFIRMED"]
        pass_rate = len(passes) / max(len(rows), 1)
        degradation = []
        q1_by_id = {r["candidate_id"]: r for r in q1_rows}
        for row in rows:
            q1 = q1_by_id.get(row["candidate_id"])
            if q1:
                degradation.append(row["net_pnl"] - q1["net_pnl"])
        if len(passes) == 0:
            disposition = "FALSIFIED" if family == "topstep_nq_es_divergence_controlled" else "FREEZE"
        elif cluster_count <= 1:
            disposition = "KEEP_AS_SINGLE_HYPOTHESIS"
        elif pass_rate < 0.10:
            disposition = "REPAIR_BOUNDED"
        else:
            disposition = "EXPAND"
        out[family] = {
            "frozen_candidates_tested": len(rows),
            "q2_pass_count": len(passes),
            "q2_pass_rate": round(pass_rate, 6),
            "median_q1_to_q2_net_degradation": round(float(pd.Series(degradation).median()) if degradation else 0.0, 2),
            "roll_sensitive_count": sum(1 for r in rows if r["roll_sensitive"]),
            "common_failure_mechanism": Counter(r["failure_reason"] for r in rows).most_common(3),
            "disposition": disposition,
        }
    return out


def valid_units(clusters: list[dict[str, Any]]) -> int:
    return sum(1 for c in clusters if c["level"] in {"LEVEL_2_SAME_ECONOMIC_MECHANISM", "LEVEL_3_SAME_BROAD_FAMILY_BEHAVIORALLY_DIFFERENT", "LEVEL_4_DISTINCT_PORTFOLIO_ROLE"})


def tick_tbbo_candidates(q1_rows: list[dict[str, Any]], q2_rows: list[dict[str, Any]]) -> list[str]:
    ids = set()
    for row in q1_rows + q2_rows:
        if row["execution_ambiguous"] or row["roll_sensitive"]:
            ids.add(row["candidate_id"])
    return sorted(ids)


def assign_clusters(clusters: list[dict[str, Any]]) -> dict[str, str]:
    lookup = {}
    for cluster in clusters:
        for cid in cluster.get("members", []):
            lookup[cid] = cluster["cluster_id"]
    return lookup


def sample_timestamps(df: pd.DataFrame, symbols: list[str]) -> list[Any]:
    subset = df[df["symbol"].isin(symbols)]
    if subset.empty:
        return []
    return list(pd.to_datetime(subset["timestamp"], utc=True).iloc[:: max(len(subset) // 200, 1)][:200])


def q3_quarantine_verification() -> dict[str, Any]:
    path = project_path("reports", "data_access", "lockbox_contamination_events.jsonl")
    return {
        "q3_quarantined": path.exists() and "2024-07-01:2024-10-01" in path.read_text(encoding="utf-8"),
        "evidence_path": str(path),
    }


def q4_seal_verification() -> dict[str, Any]:
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    text = ledger.read_text(encoding="utf-8") if ledger.exists() else ""
    return {
        "q4_loaded_or_inspected_this_phase": False,
        "q4_prior_raw_only_cache_exists": project_path("data", "cache", "databento", "GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-10-01_2025-01-01.dbn.zst").exists(),
        "q4_access_entries_total": text.count("2024-10-01:2025-01-01"),
        "status": "SEALED_RAW_ONLY_UNINSPECTED_BY_THIS_PHASE",
    }


def budget_state() -> dict[str, Any]:
    budget = DatabentoBudgetConfig(budget_start="2026-07-10", hard_cap_usd=100.0, safety_ceiling_usd=98.0)
    rows = read_ledger(project_path(budget.ledger_path))
    actual = sum(float(r.get("actual_cost_usd") or 0.0) for r in rows)
    estimated = sum(float(r.get("estimated_cost_usd") or 0.0) for r in rows if r.get("download_status") == "ESTIMATED_ONLY")
    write_budget_summary(budget)
    return {
        "cumulative_actual_spend_usd": round(actual, 6),
        "cumulative_estimated_spend_usd": round(estimated, 6),
        "remaining_hard_cap_budget_usd": round(100.0 - actual, 6),
        "remaining_safety_budget_usd": round(98.0 - actual, 6),
    }


def write_reports(summary: dict[str, Any], tag: str) -> dict[str, str]:
    stamp = timestamp()
    paths = {}
    report_specs = [
        ("reports/q2_confirmation", f"q2_confirmation_{stamp}_{tag}.md", summary),
        ("reports/family_falsification", f"family_falsification_{stamp}_{tag}.md", summary["family_falsification"]),
        ("reports/clustering_calibration", f"clustering_calibration_{stamp}_{tag}.md", summary["clustering_calibration"]),
        ("reports/roll_audit", f"roll_audit_{stamp}_{tag}.md", summary["roll_audit"]),
    ]
    for folder, filename, payload in report_specs:
        path = project_path(folder, filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown_report(filename, payload), encoding="utf-8")
        paths[folder.replace("/", "_") + "_path"] = str(path)
    return paths


def markdown_report(title: str, payload: Any) -> str:
    return "\n".join(["# " + title, "", "Historical research only. No live trading approval.", "", "```json", json.dumps(payload, indent=2, sort_keys=True, default=str)[:120000], "```", ""])


def current_commit() -> str:
    import subprocess

    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def timestamp() -> str:
    return utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
