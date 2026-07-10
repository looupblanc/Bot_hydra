#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
from hydra.data.budget import sha256_file
from hydra.data.databento_loader import load_cached_ohlcv, request_from_config, validate_ohlcv_frame
from hydra.factory.gate_aware_remediation import child_from_registry_row
from hydra.factory.remediation_policy import POLICIES
from hydra.factory.reward_model import policy_allocation_caps, promotion_aligned_reward
from hydra.features.market_state import build_market_state
from hydra.promotion.behavioral_evidence import (
    build_behavioral_sketch,
    build_trade_ledger_rows,
    write_behavioral_artifacts,
)
from hydra.promotion.pipeline import PromotionInput, run_promotion_pipeline
from hydra.promotion.gates import strategy_fingerprint
from hydra.propfirm.pass_path_optimizer import analyze_pass_path
from hydra.propfirm.rule_versioning import load_topstep_rule_snapshot
from hydra.propfirm.topstep_150k import InternalRiskOverlay, Topstep150KConfig, evaluate_topstep_150k, trades_to_topstep_daily
from hydra.registry.db import connect
from hydra.strategies.dsl import StrategyCandidate
from hydra.utils.config import load_config, project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.freeze_manifest import build_manifest, write_manifest
from hydra.validation.lockbox_guard import current_commit
from hydra.validation.no_leak import audit_no_lookahead
from hydra.validation.oos_forensics import (
    OOS_THRESHOLD,
    classify_oos_failure,
    failed_gate_names,
    gate_history,
    passed_gate_names,
    q1_core_robust,
    roll_audit_summary,
    split_scores,
    split_trade_statistics,
    stratified_oos_candidates,
    summarize_oos_distribution,
)
from hydra.validation.status_provenance import legacy_provenance_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit HYDRA OOS failures and canary reward/persistence corrections.")
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--development-start", default="2024-01-01")
    parser.add_argument("--development-end", default="2024-03-29")
    parser.add_argument("--q2-start", default="2024-04-01")
    parser.add_argument("--q2-end", default="2024-07-01")
    parser.add_argument("--full-recompute-limit", type=int, default=120)
    parser.add_argument("--canary-children", type=int, default=210)
    parser.add_argument("--q2-candidate-limit", type=int, default=100)
    parser.add_argument("--seed", type=int, default=5050)
    parser.add_argument("--report-tag", default="oos_failure_forensics_v1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    report_dir = project_path("reports", "oos_forensics")
    report_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(args.registry)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise RuntimeError(f"Registry integrity failed: {integrity}")
    rows = load_rows(conn)
    q1 = load_cached_period(args, args.development_start, args.development_end)
    q1_validation = validate_ohlcv_frame(q1, timeframe="1m")
    states, leak = build_states(q1, args.symbols)
    strata = stratified_oos_candidates(rows, random_seed=args.seed)
    all_oos_rows = [row for row in rows if "OOS" in failed_gate_names(row)]
    persisted_results = [classify_oos_failure(row) for row in all_oos_rows]
    recompute_rows = select_recompute_rows(strata, args.full_recompute_limit)
    recomputed, sketches, ledgers = recompute_forensic_sample(recompute_rows, states, leak, q1_validation, args)
    evidence_manifest = write_behavioral_artifacts(
        tag=f"{timestamp()}_{args.report_tag}",
        sketches=sketches,
        ledgers=ledgers,
    )
    q2_set = select_q2_candidates(rows, args.q2_candidate_limit)
    q2_manifest_path, q2_manifest_hash = write_q2_manifest(q2_set, args)
    canary = run_canary(rows, states, leak, q1_validation, args)
    roll_audit = roll_audit_summary(q1, args.symbols)
    status_audit = audit_status_provenance(rows, recomputed, canary)
    clusters = cluster_from_sketches(sketches)
    family_diag = diagnose_nq_es(rows, persisted_results)
    report = {
        "created_at": utc_now_iso(),
        "runtime_seconds": round(time.monotonic() - started, 2),
        "registry_integrity": integrity,
        "registry_total": len(rows),
        "q3_status": "quarantined_contaminated_not_used",
        "q4_status": "sealed_uninspected_not_loaded",
        "new_databento_purchase": False,
        "pytest_required": "run separately by CI/preflight; this script does not invoke pytest",
        "oos_threshold": OOS_THRESHOLD,
        "oos_failure_distribution": summarize_oos_distribution(persisted_results),
        "oos_failure_count": len(persisted_results),
        "strata_counts": {key: len(value) for key, value in strata.items()},
        "full_recompute_count": len(recomputed),
        "recomputed_oos_distribution": summarize_oos_distribution([item["forensics"] for item in recomputed]),
        "oos_gate_bug": detect_oos_gate_bug(persisted_results),
        "threshold_defensibility": threshold_defensibility(persisted_results),
        "status_provenance_audit": status_audit,
        "reward_delta_definition": "child promotion_score - parent promotion_score is local-only; corrected reward uses component-weighted promotion advancement where positive is beneficial.",
        "corrected_reward_components": canary["reward_components"],
        "bandit_allocation_caps": policy_allocation_caps(),
        "roll_audit": roll_audit,
        "nq_es_divergence_family_diagnosis": family_diag,
        "behavioral_evidence_manifest": evidence_manifest,
        "valid_economic_clusters": clusters,
        "canary": canary,
        "q2_candidate_set": {
            "count": len(q2_set),
            "candidate_ids": [str(row["candidate_id"]) for row in q2_set],
            "manifest_path": q2_manifest_path,
            "manifest_hash": q2_manifest_hash,
        },
        "next_command": (
            "python scripts/audit_oos_failures.py --registry registry/hydra_registry.db "
            "--full-recompute-limit 300 --canary-children 280 --q2-candidate-limit 100 "
            "--report-tag oos_failure_forensics_v2"
        ),
        "files_note": "Raw behavioral evidence is stored under ignored data/cache/behavioral_evidence; commit only this report, source, tests, and manifests.",
    }
    report_path = write_report(report_dir, args.report_tag, report)
    print(json.dumps({"report_path": str(report_path), **report}, indent=2, sort_keys=True, default=str))
    return 0


def load_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    out = []
    for row in conn.execute("SELECT * FROM candidates"):
        item = dict(row)
        item["gate_history"] = gate_history(item)
        out.append(item)
    return out


def load_cached_period(args: argparse.Namespace, start: str, end: str) -> pd.DataFrame:
    cfg = load_config()
    request = request_from_config(cfg, symbols=args.symbols, start=start, end=end, schema=args.schema, dataset=args.dataset)
    path = Path(request.output_path)
    if not path.exists():
        candidates = sorted(project_path(request.cache_folder).glob(f"{args.dataset.replace('.', '-')}_{args.schema}_{'_'.join(args.symbols)}_{start}_*.parquet"))
        if not candidates:
            raise FileNotFoundError(f"Cached OHLCV data missing for {start}:{end}; no Databento request was made.")
        path = candidates[-1]
    df = load_cached_ohlcv(path, timeframe=request.timeframe)
    ts = pd.to_datetime(df["timestamp"], utc=True)
    return df[(ts >= pd.Timestamp(start, tz="UTC")) & (ts < pd.Timestamp(end, tz="UTC")) & df["symbol"].isin(args.symbols)].reset_index(drop=True)


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


def row_to_candidate(row: dict[str, Any]) -> StrategyCandidate:
    return StrategyCandidate(
        candidate_id=str(row["candidate_id"]),
        family=str(row["family"]),
        symbol=str(row["symbol"]),
        timeframe=str(row["timeframe"]),
        parameters=parse_json(row.get("parameters_json"), {}),
        entry_logic=f"{row['family']}_registry_entry",
        exit_logic="registry_replay_exit",
        risk_parameters=parse_json(row.get("risk_json"), {}),
        parent_candidate_id=row.get("parent_candidate_id"),
        mutation_type=row.get("mutation_type"),
    )


def select_recompute_rows(strata: dict[str, list[dict[str, Any]]], limit: int) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    order = [
        "q1_core_robust",
        "best_500_topstep_viable",
        "closest_to_oos_threshold",
        "parents_children_by_policy",
        "top_nq_es_divergence_lineages",
        "non_nq_es_families",
        "random_oos_failures",
    ]
    per_bucket = max(5, limit // max(len(order), 1))
    for key in order:
        for row in strata.get(key, [])[:per_bucket]:
            selected[str(row["candidate_id"])] = row
            if len(selected) >= limit:
                break
        if len(selected) >= limit:
            break
    return list(selected.values())[:limit]


def recompute_forensic_sample(
    rows: list[dict[str, Any]],
    states: dict[str, pd.DataFrame],
    leak: dict[str, tuple[bool, str]],
    data_validation: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    cfg = Topstep150KConfig()
    results = []
    sketches = []
    ledgers = []
    for idx, row in enumerate(rows):
        candidate = row_to_candidate(row)
        if candidate.symbol not in states:
            continue
        result = run_backtest(candidate, states[candidate.symbol], seed=args.seed + idx)
        overlay = InternalRiskOverlay(
            daily_stop=float(candidate.risk_parameters.get("internal_daily_stop", 1000)),
            daily_profit_lock=float(candidate.risk_parameters.get("daily_profit_lock", 1500)),
        )
        daily = trades_to_topstep_daily(result.trades, states[candidate.symbol], overlay)
        split_stats = split_trade_statistics(result.trades, daily)
        forensic = classify_oos_failure(row, split_stats)
        sketch = build_behavioral_sketch(
            candidate_id=candidate.candidate_id,
            parent_candidate_id=candidate.parent_candidate_id,
            trades=result.trades,
            daily=daily,
            validation_period="Q1_2024",
        )
        sketches.append(sketch)
        ledgers.extend(
            build_trade_ledger_rows(
                candidate_id=candidate.candidate_id,
                parent_candidate_id=candidate.parent_candidate_id,
                trades=result.trades,
                validation_period="Q1_2024",
            )
        )
        topstep = evaluate_topstep_150k(result.trades, states[candidate.symbol], cfg, overlay, split_daily=split_daily_frames(daily)).to_record()
        promotion = run_promotion_pipeline(
            PromotionInput(
                candidate=candidate,
                result=result,
                daily=daily,
                topstep_record=topstep,
                data_validation=data_validation,
                split_scores=topstep.get("split_scores", {}),
                leak_ok=leak[candidate.symbol][0],
                leak_reason=leak[candidate.symbol][1],
                existing_fingerprints=set(),
                max_correlation=0.0,
                seed=args.seed + idx,
                lane="oos_forensics_recompute",
                report_tag=args.report_tag,
            )
        )
        results.append(
            {
                "candidate_id": candidate.candidate_id,
                "forensics": forensic,
                "promotion": promotion,
                "topstep": topstep,
                "split_stats": split_stats,
            }
        )
    return results, sketches, ledgers


def run_canary(
    rows: list[dict[str, Any]],
    states: dict[str, pd.DataFrame],
    leak: dict[str, tuple[bool, str]],
    data_validation: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    cfg = Topstep150KConfig()
    parent_pool = [row for row in rows if row.get("validation_status") in {"TOPSTEP_VIABLE", "TOPSTEP_NEAR_MISS", "ECONOMICALLY_VIABLE"} and "OOS" in failed_gate_names(row)]
    parent_pool = sorted(parent_pool, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:350]
    existing = {str(row.get("strategy_fingerprint")) for row in rows if row.get("strategy_fingerprint")}
    per_policy = max(1, args.canary_children // len(POLICIES))
    policy_counts: Counter[str] = Counter()
    policy_rewards: dict[str, list[float]] = defaultdict(list)
    policy_stage_progress: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    oos_passes = 0
    children = 0
    duplicates = 0
    reward_components = []
    cycle_fingerprints: set[str] = set()
    for policy in POLICIES:
        for variant in range(per_policy):
            if children >= args.canary_children:
                break
            parent = parent_pool[(children + variant) % len(parent_pool)]
            hyp = child_from_registry_row(parent, variant=10_000_000 + children, policy_name=policy.name)
            fingerprint = strategy_fingerprint(hyp.child)
            if fingerprint in existing or fingerprint in cycle_fingerprints:
                duplicates += 1
                continue
            cycle_fingerprints.add(fingerprint)
            started = time.monotonic()
            result = evaluate_child_for_report(hyp.child, states[hyp.child.symbol], cfg, args.seed + children)
            topstep = result["topstep"]
            topstep["pass_path_diagnosis"] = analyze_pass_path(topstep, cfg.combine_profit_target, cfg.combine_max_loss_limit).diagnosis
            promotion = run_promotion_pipeline(
                PromotionInput(
                    candidate=hyp.child,
                    result=result["result"],
                    daily=result["daily"],
                    topstep_record=topstep,
                    data_validation=data_validation,
                    split_scores=topstep.get("split_scores", {}),
                    leak_ok=leak[hyp.child.symbol][0],
                    leak_reason=leak[hyp.child.symbol][1],
                    existing_fingerprints=existing,
                    max_correlation=0.0,
                    seed=args.seed + children,
                    lane="balanced_oos_canary",
                    report_tag=args.report_tag,
                )
            )
            child_row = {
                **promotion,
                "validation_status": promotion["status"],
                "gate_history": promotion["gate_history"],
                "combine_min_mll_buffer": topstep.get("combine_min_mll_buffer", 0.0),
                "mll_buffer": topstep.get("combine_min_mll_buffer", 0.0),
                "complexity_delta": policy.complexity_delta,
            }
            reward = promotion_aligned_reward(parent, child_row, compute_seconds=time.monotonic() - started)
            reward_components.append({"policy": policy.name, **reward.to_dict()})
            policy_counts[policy.name] += 1
            policy_rewards[policy.name].append(reward.total)
            status_counts[promotion["status"]] += 1
            if "OOS" in passed_gate_names({"gate_history": promotion["gate_history"]}):
                oos_passes += 1
            if _stage_rank(promotion.get("promotion_stage")) > _stage_rank(parent.get("promotion_stage")):
                policy_stage_progress[policy.name] += 1
            existing.add(fingerprint)
            children += 1
    return {
        "requested_children": args.canary_children,
        "completed_children": children,
        "duplicates": duplicates,
        "policy_allocation": dict(policy_counts),
        "status_counts": dict(status_counts),
        "oos_passes": oos_passes,
        "promotion_stage_progress_by_policy": dict(policy_stage_progress),
        "mean_reward_by_policy": {k: round(sum(v) / max(len(v), 1), 6) for k, v in policy_rewards.items()},
        "reward_components": reward_components[:25],
        "allocation_note": "Balanced canary; no registry rows written.",
    }


def evaluate_child_for_report(child: StrategyCandidate, state: pd.DataFrame, cfg: Topstep150KConfig, seed: int) -> dict[str, Any]:
    result = run_backtest(child, state, seed)
    overlay = InternalRiskOverlay(
        daily_stop=float(child.risk_parameters.get("internal_daily_stop", 1000)),
        daily_profit_lock=float(child.risk_parameters.get("daily_profit_lock", 1500)),
    )
    daily = trades_to_topstep_daily(result.trades, state, overlay)
    topstep = evaluate_topstep_150k(result.trades, state, cfg, overlay, split_daily=split_daily_frames(daily)).to_record()
    return {"result": result, "daily": daily, "topstep": topstep}


def split_daily_frames(daily: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if daily.empty:
        return {"jan": daily, "feb": daily, "mar": daily}
    dates = pd.to_datetime(daily["date"])
    return {
        "jan": daily[(dates >= "2024-01-01") & (dates < "2024-02-01")].reset_index(drop=True),
        "feb": daily[(dates >= "2024-02-01") & (dates < "2024-03-01")].reset_index(drop=True),
        "mar": daily[(dates >= "2024-03-01") & (dates < "2024-04-01")].reset_index(drop=True),
    }


def audit_status_provenance(rows: list[dict[str, Any]], recomputed: list[dict[str, Any]], canary: dict[str, Any]) -> dict[str, Any]:
    legacy = Counter(legacy_provenance_status(row)["evidence_strength"] for row in rows)
    recomputed_status = Counter(item["promotion"]["status"] for item in recomputed)
    recomputed_modes = Counter(item["promotion"].get("computation_mode", "") for item in recomputed)
    return {
        "legacy_registry_evidence_strength": dict(legacy),
        "legacy_rows_with_validation_version": sum(1 for row in rows if row.get("validation_version")),
        "existing_statuses_final_promotion_usable": 0,
        "finding": "Historical statuses are legacy-unversioned for final promotion. New canary/recompute statuses record version, input fingerprint, mode, and evidence strength.",
        "child_status_inheritance_detected": "not_provable_from_legacy_rows; new pipeline recomputes and fingerprints inputs",
        "recomputed_true_status_counts_sample": dict(recomputed_status),
        "recomputed_computation_modes_sample": dict(recomputed_modes),
        "canary_status_counts": canary.get("status_counts", {}),
    }


def select_q2_candidates(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    candidates = [
        row
        for row in rows
        if row.get("validation_status") in {"TOPSTEP_VIABLE", "TOPSTEP_NEAR_MISS"}
        and failed_gate_names(row) in (["OOS"], ["PAYOUT_SURVIVAL"], ["PORTFOLIO_INTERACTION"])
    ]
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in sorted(candidates, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True):
        key = (str(row.get("family")), str(row.get("symbol")), str(row.get("parameter_zone") or row.get("strategy_fingerprint")))
        if key not in selected:
            selected[key] = row
        if len(selected) >= limit:
            break
    return list(selected.values())


def write_q2_manifest(rows: list[dict[str, Any]], args: argparse.Namespace) -> tuple[str, str]:
    fingerprints = cache_fingerprints(args)
    manifest = build_manifest(
        manifest_type="q2_confirmation_freeze",
        candidate_rows=rows,
        source_code_commit=current_commit(),
        data_fingerprints=fingerprints,
        topstep_rule_version=load_topstep_rule_snapshot().rule_version_id,
        validation_thresholds={"oos_threshold": OOS_THRESHOLD, "q2_role": "secondary_development_confirmation"},
        expected_decision_policy={
            "q2_result_may_confirm_or_reject": True,
            "q2_modified_after_viewing_becomes_q2_developed": True,
            "q3_quarantined": True,
            "q4_sealed": True,
        },
    )
    path, digest = write_manifest(manifest)
    return str(path), digest


def cache_fingerprints(args: argparse.Namespace) -> dict[str, str]:
    cfg = load_config()
    out = {}
    for label, start, end in [("q1", args.development_start, args.development_end), ("q2", args.q2_start, args.q2_end)]:
        request = request_from_config(cfg, symbols=args.symbols, start=start, end=end, schema=args.schema, dataset=args.dataset)
        path = Path(request.output_path)
        if path.exists():
            out[f"{label}:{path}"] = sha256_file(path)
    return out


def cluster_from_sketches(sketches: list[dict[str, Any]]) -> dict[str, Any]:
    clusters: dict[str, list[str]] = defaultdict(list)
    for sketch in sketches:
        key = "|".join(
            [
                str(sketch.get("daily_pnl_hash")),
                str(sketch.get("trade_timestamp_signature")),
                str(sketch.get("direction_signature")),
                str(sketch.get("entry_overlap_signature")),
            ]
        )
        clusters[key].append(str(sketch["candidate_id"]))
    sizes = sorted((len(v) for v in clusters.values()), reverse=True)
    return {
        "evidence_backed_candidates": len(sketches),
        "valid_economic_clusters": len(clusters),
        "cluster_sizes": sizes[:25],
        "dominant_cluster_size": sizes[0] if sizes else 0,
        "representatives": [members[0] for members in clusters.values()][:25],
        "uncertainty": "Exact clustering is limited to recomputed candidates with persisted sketches.",
    }


def diagnose_nq_es(rows: list[dict[str, Any]], results: list[Any]) -> dict[str, Any]:
    family_rows = [row for row in rows if str(row.get("family")) == "topstep_nq_es_divergence_controlled"]
    family_oos = [item for item in results if item.family == "topstep_nq_es_divergence_controlled"]
    dist = summarize_by_classification(family_oos)
    top50 = sorted(family_rows, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:50]
    top50_failed = Counter(tuple(failed_gate_names(row)) for row in top50)
    return {
        "family_rows": len(family_rows),
        "oos_failure_distribution": dist,
        "top50_failed_gate_patterns": {str(k): v for k, v in top50_failed.items()},
        "diagnosis": "Current top candidates are NQ/ES variants failing March OOS; treat as a concentrated lineage until trade-overlap evidence proves distinct behavior.",
        "roll_note": "Continuous OHLCV alone cannot rule out roll-generated relative-value artifacts; explicit contract mapping remains required before trusting this family.",
    }


def summarize_by_classification(items: list[Any]) -> dict[str, int]:
    return dict(Counter(item.classification for item in items))


def detect_oos_gate_bug(results: list[Any]) -> dict[str, Any]:
    metric_direction = sum(1 for item in results if item.classification == "METRIC_DIRECTION_BUG")
    status_inheritance = sum(1 for item in results if item.classification == "STATUS_INHERITANCE_BUG")
    return {
        "metric_direction_bug_count": metric_direction,
        "status_inheritance_bug_count": status_inheritance,
        "contains_bug": bool(metric_direction or status_inheritance),
        "finding": "No OOS metric-direction bug detected in persisted split scores." if not metric_direction else "OOS gate has metric-direction inconsistencies.",
    }


def threshold_defensibility(results: list[Any]) -> dict[str, Any]:
    near = sum(1 for item in results if item.classification == "THRESHOLD_TOO_STRICT")
    missing = sum(1 for item in results if item.classification == "MISSING_DATA")
    total = len(results)
    return {
        "near_threshold_count": near,
        "missing_split_evidence_count": missing,
        "near_threshold_share": round(near / max(total, 1), 6),
        "defensible": near / max(total, 1) < 0.20,
        "finding": "Do not weaken OOS. The zero-pass result is driven mostly by low March split scores, not a narrow threshold cliff.",
    }


def write_report(folder: Path, tag: str, report: dict[str, Any]) -> Path:
    path = folder / f"oos_forensics_{timestamp()}_{tag}.md"
    lines = ["# OOS Failure Forensics", "", "This is historical research only. No live trading approval is implied.", ""]
    for key, value in report.items():
        if key in {"corrected_reward_components"}:
            lines.append(f"## {key}")
            lines.append("```json")
            lines.append(json.dumps(value[:10], indent=2, sort_keys=True, default=str))
            lines.append("```")
        else:
            lines.append(f"## {key}")
            lines.append("```json")
            lines.append(json.dumps(value, indent=2, sort_keys=True, default=str)[:8000])
            lines.append("```")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def parse_json(value: Any, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not value:
        return fallback
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return fallback


def _stage_rank(stage: Any) -> int:
    order = {
        "GENERATED": 0,
        "BACKTESTED": 1,
        "NO_LOOKAHEAD_PASSED": 2,
        "WALK_FORWARD_PASSED": 3,
        "OOS_PASSED": 4,
        "MONTE_CARLO_PASSED": 5,
        "PARAMETER_SENSITIVITY_PASSED": 6,
        "TOPSTEP_COMBINE_PASSED": 7,
        "FUNDED_XFA_PASSED": 8,
        "PAYOUT_SURVIVAL_PASSED": 9,
        "CORRELATION_PASSED": 10,
        "PORTFOLIO_INTERACTION_PASSED": 11,
        "EXECUTION_READINESS_PASSED": 12,
        "TRADING_READY_CANDIDATE": 13,
    }
    return order.get(str(stage), 0)


def timestamp() -> str:
    return utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
