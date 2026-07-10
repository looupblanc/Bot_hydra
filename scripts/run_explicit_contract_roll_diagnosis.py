#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.contract_mapping import build_rule_based_roll_map, load_roll_map
from hydra.data.roll_audit import audit_trade_roll_exposure, compare_roll_maps
from hydra.promotion.behavioral_evidence import write_behavioral_artifacts
from hydra.promotion.cluster_calibration import calibrate_clustering_controls, cluster_sketches
from hydra.registry.db import connect
from hydra.strategies.families import signal_for_candidate
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access

from scripts.run_frozen_q2_falsification import (
    assign_clusters,
    build_states,
    budget_state,
    candidate_from_spec,
    evaluate_candidate,
    load_cached_period,
    load_manifest,
    q3_quarantine_verification,
    q4_seal_verification,
    validate_manifest,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bounded explicit-contract roll diagnosis for frozen HYDRA candidates.")
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument("--manifest", default="reports/lockbox/q2_confirmation_freeze_c256dadbd42bd9a6.json")
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--q1-start", default="2024-01-01")
    parser.add_argument("--q1-end", default="2024-03-29")
    parser.add_argument("--q2-start", default="2024-04-01")
    parser.add_argument("--q2-end", default="2024-07-01")
    parser.add_argument("--report-tag", default="explicit_contract_roll_diagnosis_v1")
    parser.add_argument("--seed", type=int, default=7070)
    parser.add_argument("--max-candidates", type=int, default=100)
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
    if not manifest_audit["manifest_hash_valid"] or manifest_audit["spec_mismatch_count"]:
        raise RuntimeError("Frozen manifest mismatch; refusing explicit-roll diagnosis.")
    candidate_ids = list(manifest["candidate_ids"])[: args.max_candidates]
    specs = [spec for spec in manifest["strategy_specs"] if spec["candidate_id"] in set(candidate_ids)]
    enforce_data_access(
        period=f"{args.q1_start}:{args.q1_end}",
        role=DataRole.DEVELOPMENT,
        requesting_module="run_explicit_contract_roll_diagnosis",
        candidate_ids=candidate_ids,
        reason="explicit-contract Q1 recomputation for frozen diagnostic set",
        freeze_manifest_hash=manifest.get("manifest_hash"),
    )
    enforce_data_access(
        period=f"{args.q2_start}:{args.q2_end}",
        role=DataRole.DIAGNOSTIC_CONFIRMATION_CONSUMED,
        requesting_module="run_explicit_contract_roll_diagnosis",
        candidate_ids=candidate_ids,
        reason="Q2 is consumed diagnostic data; replay only if explicit Q1 passes exist",
        freeze_manifest_hash=manifest.get("manifest_hash"),
    )
    explicit_map = load_roll_map(args.contract_map)
    proxy_map = build_rule_based_roll_map(args.symbols, start=args.q1_start, end=args.q2_end, dataset=args.dataset, schema=args.schema)
    q1 = load_cached_period(args, args.q1_start, args.q1_end)
    q1_states, q1_leak = build_states(q1, args.symbols)
    map_comparison = compare_roll_maps(proxy_map, explicit_map, _sample_timestamps_by_symbol(q1, args.symbols))
    q1_records: list[dict[str, Any]] = []
    q2_records: list[dict[str, Any]] = []
    sketches: list[dict[str, Any]] = []
    ledgers: list[dict[str, Any]] = []
    candidate_roll_impacts = []
    for index, spec in enumerate(specs):
        candidate = candidate_from_spec(spec)
        explicit_eval = evaluate_candidate(
            candidate,
            q1_states[candidate.symbol],
            {"duplicate_timestamp_symbol_rows": 0, "future_timestamp_rows": 0, "columns": list(q1.columns)},
            q1_leak[candidate.symbol],
            explicit_map,
            "Q1_EXPLICIT_2024",
            args,
            seed=args.seed + index,
        )
        proxy_roll = audit_trade_roll_exposure(explicit_eval["ledger_rows"], proxy_map)
        record = explicit_q1_record(explicit_eval["record"], proxy_roll)
        q1_records.append(record)
        sketches.append(explicit_eval["sketch"])
        ledgers.extend(explicit_eval["ledger_rows"])
        candidate_roll_impacts.append(
            {
                "candidate_id": candidate.candidate_id,
                "explicit_roll_impact": record["roll_impact_classification"],
                "proxy_roll_sensitive": bool(proxy_roll.get("roll_sensitive")),
                "explicit_roll_sensitive": bool(explicit_eval["record"].get("roll_sensitive")),
                "proxy_unsafe_roll_trade_count": int(proxy_roll.get("unsafe_roll_trade_count", 0)),
                "explicit_unsafe_roll_trade_count": int(explicit_eval["record"]["roll_audit"].get("unsafe_roll_trade_count", 0)),
                "proxy_cross_roll_trade_count": int(proxy_roll.get("cross_roll_trade_count", 0)),
                "explicit_cross_roll_trade_count": int(explicit_eval["record"]["roll_audit"].get("cross_roll_trade_count", 0)),
            }
        )
    q1_pass_ids = {row["candidate_id"] for row in q1_records if row["explicit_q1_status"] == "EXPLICIT_Q1_PASS"}
    if q1_pass_ids:
        q2 = load_cached_period(args, args.q2_start, args.q2_end)
        q2_states, q2_leak = build_states(q2, args.symbols)
        for index, spec in enumerate(specs):
            if spec["candidate_id"] not in q1_pass_ids:
                continue
            candidate = candidate_from_spec(spec)
            q2_eval = evaluate_candidate(
                candidate,
                q2_states[candidate.symbol],
                {"duplicate_timestamp_symbol_rows": 0, "future_timestamp_rows": 0, "columns": list(q2.columns)},
                q2_leak[candidate.symbol],
                explicit_map,
                "Q2_DIAGNOSTIC_2024",
                args,
                seed=args.seed + 100_000 + index,
            )
            q2_records.append(diagnostic_q2_record(q2_eval["record"]))
            sketches.append(q2_eval["sketch"])
            ledgers.extend(q2_eval["ledger_rows"])
    evidence_manifest = write_behavioral_artifacts(
        tag=f"{stamp()}_{args.report_tag}",
        sketches=sketches,
        ledgers=ledgers,
    )
    clustering = calibrate_clustering_controls(sketches)
    clusters = cluster_sketches(sketches)
    cluster_lookup = assign_clusters(clusters)
    for row in q1_records + q2_records:
        row["behavior_cluster"] = cluster_lookup.get(row["candidate_id"], "unclustered")
    summary = {
        "created_at": utc_now_iso(),
        "runtime_seconds": round(time.monotonic() - started, 2),
        "registry_integrity": integrity,
        "manifest_audit": manifest_audit,
        "data_role_correction": {
            "q1": "DEVELOPMENT",
            "q2": "DIAGNOSTIC_CONFIRMATION_CONSUMED",
            "q3": "CONTAMINATED_DEVELOPMENT",
            "q4": "SEALED_BLIND_HOLDOUT",
            "future_2025": "POTENTIAL_FINAL_LOCKBOX",
        },
        "q3_quarantine_verification": q3_quarantine_verification(),
        "q4_seal_verification": q4_seal_verification(),
        "contract_map_path": args.contract_map,
        "explicit_contract_map_type": explicit_map.map_type,
        "rule_proxy_comparison_q1": map_comparison,
        "candidate_roll_impacts": candidate_roll_impacts,
        "q1_status_counts": dict(Counter(row["explicit_q1_status"] for row in q1_records)),
        "q1_invalidations": [row["candidate_id"] for row in q1_records if row["explicit_q1_status"] == "EXPLICIT_Q1_ROLL_INVALIDATED"],
        "q2_status_counts": dict(Counter(row["q2_diagnostic_status"] for row in q2_records)),
        "q2_diagnostic_records": q2_records,
        "q1_records": q1_records,
        "clustering_calibration": clustering,
        "clusters": clusters,
        "calibrated_economic_clusters": len(clusters) if clustering["precision_known_clones"] >= 0.90 and clustering["recall_known_clones"] >= 0.90 else "PROVISIONAL_ONLY",
        "hidden_directional_beta": hidden_beta_diagnosis(),
        "lineage_disposition": lineage_disposition(q1_records, q2_records),
        "new_representation_specs": new_representation_specs(),
        "eligible_future_tick_tbbo": sorted({row["candidate_id"] for row in q1_records if row["explicit_q1_status"] == "EXPLICIT_Q1_ROLL_INVALIDATED"}),
        "budget_state": budget_state(),
        "behavioral_evidence_manifest": evidence_manifest,
        "exact_next_milestone": "Retire the current NQ/ES parameter-neighbor lineage, then implement one or two roll-aware beta-neutral representations with explicit ES/NQ paired inputs before any new search.",
    }
    paths = write_reports(summary, args.report_tag)
    print(json.dumps({**summary, **paths}, indent=2, sort_keys=True, default=str))
    return 0


def explicit_q1_record(row: dict[str, Any], proxy_roll: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    if row["execution_ambiguous"]:
        status = "EXPLICIT_Q1_INSUFFICIENT_EVIDENCE"
    elif row["roll_sensitive"]:
        status = "EXPLICIT_Q1_ROLL_INVALIDATED"
    elif int(row["trade_count"]) < 20:
        status = "EXPLICIT_Q1_INSUFFICIENT_EVIDENCE"
    elif row["promotion_status"] == "TRADING_READY_CANDIDATE":
        status = "EXPLICIT_Q1_PASS"
    else:
        status = "EXPLICIT_Q1_FAIL"
    out["explicit_q1_status"] = status
    out["proxy_roll_audit"] = proxy_roll
    out["roll_impact_classification"] = roll_impact_class(row["roll_audit"], proxy_roll)
    return out


def diagnostic_q2_record(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    if row["execution_ambiguous"]:
        status = "Q2_DIAGNOSTIC_INSUFFICIENT"
    elif row["roll_sensitive"]:
        status = "Q2_DIAGNOSTIC_ROLL_INVALIDATED"
    elif int(row["trade_count"]) < 20:
        status = "Q2_DIAGNOSTIC_INSUFFICIENT"
    elif (
        float(row["net_pnl"]) > 0
        and float(row["profit_factor"]) >= 1.05
        and not row["combine_mll_breached"]
        and row["consistency_ok"]
        and float(row["best_trade_share_abs_profit"]) <= 0.35
    ):
        status = "Q2_DIAGNOSTIC_TRANSFER"
    else:
        status = "Q2_DIAGNOSTIC_FAILURE"
    out["q2_diagnostic_status"] = status
    return out


def roll_impact_class(explicit_roll: dict[str, Any], proxy_roll: dict[str, Any]) -> str:
    if explicit_roll.get("cross_roll_trade_count", 0):
        return "ROLL_INVALIDATED"
    if explicit_roll.get("roll_sensitive"):
        return "ROLL_MATERIAL_IMPACT"
    if explicit_roll.get("unsafe_roll_trade_count", 0) != proxy_roll.get("unsafe_roll_trade_count", 0):
        return "ROLL_MINOR_IMPACT"
    return "ROLL_INVARIANT"


def lineage_disposition(q1_records: list[dict[str, Any]], q2_records: list[dict[str, Any]]) -> dict[str, Any]:
    q1_pass = [row for row in q1_records if row["explicit_q1_status"] == "EXPLICIT_Q1_PASS"]
    q2_transfer = [row for row in q2_records if row.get("q2_diagnostic_status") == "Q2_DIAGNOSTIC_TRANSFER"]
    hidden_beta = hidden_beta_diagnosis()["detected"]
    if not q1_pass:
        disposition = "KILL_EXISTING_LINEAGE"
        reason = "no representative passed explicit-contract Q1 recomputation"
    elif not q2_transfer:
        disposition = "KILL_EXISTING_LINEAGE"
        reason = "explicit Q1 survivors failed Q2 diagnostic transfer"
    elif hidden_beta:
        disposition = "RESEARCH_REFORMULATION_REQUIRED"
        reason = "implementation is not genuinely NQ/ES relative value"
    else:
        disposition = "KEEP_AS_SINGLE_RESEARCH_HYPOTHESIS"
        reason = "bounded transfer survived in at least one cluster"
    return {
        "family": "topstep_nq_es_divergence_controlled",
        "disposition": disposition,
        "reason": reason,
        "q1_pass_count": len(q1_pass),
        "q2_diagnostic_transfer_count": len(q2_transfer),
        "retain_representatives": [],
        "freeze_or_kill_parameter_neighbors": True,
    }


def hidden_beta_diagnosis() -> dict[str, Any]:
    source = inspect.getsource(signal_for_candidate)
    marker = "divergence_proxy = df[\"momentum_20\"] - df[\"session_return\"]"
    return {
        "detected": marker in source,
        "mechanism": "The current topstep_nq_es_divergence_controlled implementation uses only the candidate symbol dataframe: momentum_20 minus session_return, then direction from that same symbol momentum.",
        "es_input_used": False,
        "diagnosis": "This is hidden single-market directional beta, not a tradable ES/NQ relative-value spread.",
    }


def new_representation_specs() -> list[dict[str, Any]]:
    return [
        _spec("roll_aware_beta_neutral_nq_es_residual_divergence", "Past-window hedge ratio residual between synchronized NQ and ES contracts.", "Mean-reverting relative overextension after synchronized roll filtering.", "high correlation regimes with temporary index composition dislocation", "macro directional breakouts where both legs trend without convergence", 5, "Q1/Q2 residual sign and magnitude must transfer after excluding roll windows", "portfolio diversifier / relative-value sleeve", "paired OHLCV plus explicit roll map", "market or synthetic paired execution; no signal inside roll exclusion", "high"),
        _spec("dynamic_hedge_ratio_relative_value", "Rolling beta estimated only from prior bars and applied to current residual.", "Changing NQ/ES beta creates exploitable residual only when estimation is stable.", "stable intraday covariance and moderate volatility", "beta instability and event shocks", 6, "beta residual should outperform raw NQ momentum and survive parameter plateaus", "low-directional-beta Topstep candidate", "paired OHLCV", "requires spread slippage model", "high"),
        _spec("overnight_inventory_rth_resolution", "Overnight range/location pressure resolved during early RTH.", "Overnight positioning can unwind or continue after cash open.", "large overnight imbalance with early confirmation", "quiet overnight sessions and news shock opens", 4, "January-derived rules must transfer month-to-month without one-day concentration", "controlled opening-session climber", "OHLCV session features", "marketable orders during liquid RTH only", "medium"),
        _spec("opening_auction_displacement_failed_continuation", "Opening displacement that fails to maintain auction direction.", "Failed continuation traps early momentum and creates bounded reversal.", "first 60-120 RTH minutes", "trend days with persistent breadth", 5, "must reduce worst-day loss versus naive opening breakout", "Topstep consistency-safe short horizon", "OHLCV RTH features", "strict cutoff and same-bar conservative fills", "low"),
        _spec("volatility_shape_transition", "Transition in realized-vol shape, not simple range expansion.", "Convexity/decay profile distinguishes actionable expansion from chop.", "compression resolving into directional liquidity", "headline volatility and whipsaw expansion", 6, "shape feature must add value over range_expansion alone", "target velocity with MLL control", "OHLCV volatility windows", "market orders with volatility-scaled stops", "low"),
        _spec("cross_market_lead_lag_conditioned_on_vol_regime", "Lagged ES/NQ confirmation only in stable covariance states.", "Lead-lag appears only when one index reprices first under constrained vol.", "moderate vol and high index correlation", "roll windows and high-vol news", 6, "lagged feature must beat simultaneous correlation proxy", "relative-value signal filter", "paired OHLCV and explicit rolls", "requires synchronized contract pair", "high"),
        _spec("intraday_range_migration_path_asymmetry", "Where the range migrates inside the session and how price spends time near extremes.", "Path asymmetry can identify controlled continuation versus exhaustion.", "structured trend or failed trend days", "featureless rotational days", 5, "ablation must show path terms matter beyond momentum", "session role / diversifier", "OHLCV path geometry", "time stop plus daily lock", "low"),
        _spec("session_transition_state_models", "State changes around Globex/RTH boundaries with no post-cutoff holds.", "Liquidity regime change creates repeatable but time-bounded opportunity.", "session boundary repricing", "holiday/low-volume sessions", 4, "must transfer across months and respect 3:10 CT flatten", "low-overlap portfolio component", "OHLCV sessions and calendar", "strict session scheduler", "medium"),
        _spec("failed_directional_expansion_controlled_tail", "Expansion that fails after limited continuation, with tail-risk stop.", "Breakout failure can be monetized if tail exposure is capped.", "false breakout regimes", "true trend days", 5, "must improve MLL buffer versus raw reversal", "MLL-safe reversal sleeve", "OHLCV expansion and path confirmation", "conservative same-bar ordering", "low"),
        _spec("mes_mnq_micro_first_portfolio_roles", "Micro-first sizing across MES/MNQ to tune account-level risk.", "Fine sizing can preserve MLL while combining small independent edges.", "low-to-moderate opportunity regimes", "large gap days and high correlation stops", 5, "portfolio scheduler must beat standalone naive sum under shared MLL", "one-account risk smoother", "OHLCV plus portfolio scheduler", "micro contracts only until safety proven", "medium"),
    ]


def _spec(
    name: str,
    mechanism: str,
    hypothesis: str,
    expected_regime: str,
    failure_regime: str,
    minimal_parameter_count: int,
    falsification_test: str,
    topstep_role: str,
    data_requirement: str,
    execution_requirement: str,
    roll_sensitivity: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "economic_mechanism": mechanism,
        "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
        "hypothesis": hypothesis,
        "expected_regime": expected_regime,
        "expected_failure_regime": failure_regime,
        "minimal_parameter_count": minimal_parameter_count,
        "falsification_test": falsification_test,
        "likely_topstep_role": topstep_role,
        "data_requirement": data_requirement,
        "execution_requirement": execution_requirement,
        "roll_sensitivity": roll_sensitivity,
    }


def _sample_timestamps_by_symbol(df: pd.DataFrame, symbols: list[str]) -> dict[str, list[Any]]:
    out = {}
    for symbol in symbols:
        subset = df[df["symbol"] == symbol]
        out[symbol] = list(pd.to_datetime(subset["timestamp"], utc=True).iloc[:: max(len(subset) // 500, 1)][:500])
    return out


def write_reports(summary: dict[str, Any], tag: str) -> dict[str, str]:
    stamp_value = stamp()
    specs = [
        ("reports/roll_audit", f"explicit_roll_diagnosis_{stamp_value}_{tag}.md", compact_roll_report(summary)),
        ("reports/q2_confirmation", f"q2_diagnostic_replay_{stamp_value}_{tag}.md", {"q2_status_counts": summary["q2_status_counts"], "q2_records": summary["q2_diagnostic_records"]}),
        ("reports/family_falsification", f"nq_es_lineage_decision_{stamp_value}_{tag}.md", summary["lineage_disposition"]),
        ("reports/clustering_calibration", f"clustering_calibration_{stamp_value}_{tag}.md", summary["clustering_calibration"]),
        ("reports/research_representations", f"new_market_representations_{stamp_value}_{tag}.md", summary["new_representation_specs"]),
    ]
    paths = {}
    for folder, filename, payload in specs:
        path = project_path(folder, filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown(filename, payload), encoding="utf-8")
        paths[folder.replace("/", "_") + "_path"] = str(path)
    return paths


def markdown(title: str, payload: Any) -> str:
    return "\n".join(["# " + title, "", "Historical research only. No live trading approval.", "", "```json", json.dumps(payload, indent=2, sort_keys=True, default=str), "```", ""])


def compact_roll_report(summary: dict[str, Any]) -> dict[str, Any]:
    roll_impacts = Counter(item["explicit_roll_impact"] for item in summary["candidate_roll_impacts"])
    materially_affected = [
        item["candidate_id"]
        for item in summary["candidate_roll_impacts"]
        if item["explicit_roll_impact"] in {"ROLL_MATERIAL_IMPACT", "ROLL_INVALIDATED"}
    ]
    return {
        "created_at": summary["created_at"],
        "runtime_seconds": summary["runtime_seconds"],
        "registry_integrity": summary["registry_integrity"],
        "manifest_audit": summary["manifest_audit"],
        "data_role_correction": summary["data_role_correction"],
        "q3_quarantine_verification": summary["q3_quarantine_verification"],
        "q4_seal_verification": summary["q4_seal_verification"],
        "contract_map_path": summary["contract_map_path"],
        "explicit_contract_map_type": summary["explicit_contract_map_type"],
        "rule_proxy_comparison_q1": summary["rule_proxy_comparison_q1"],
        "roll_impact_counts": dict(roll_impacts),
        "materially_affected_candidates": materially_affected,
        "q1_status_counts": summary["q1_status_counts"],
        "q1_invalidations": summary["q1_invalidations"],
        "q2_status_counts": summary["q2_status_counts"],
        "q2_diagnostic_records": summary["q2_diagnostic_records"],
        "clustering_calibration": summary["clustering_calibration"],
        "calibrated_economic_clusters": summary["calibrated_economic_clusters"],
        "hidden_directional_beta": summary["hidden_directional_beta"],
        "lineage_disposition": summary["lineage_disposition"],
        "new_representation_names": [item["name"] for item in summary["new_representation_specs"]],
        "eligible_future_tick_tbbo": summary["eligible_future_tick_tbbo"],
        "budget_state": summary["budget_state"],
        "behavioral_evidence_manifest": summary["behavioral_evidence_manifest"],
        "exact_next_milestone": summary["exact_next_milestone"],
    }


def stamp() -> str:
    return utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
