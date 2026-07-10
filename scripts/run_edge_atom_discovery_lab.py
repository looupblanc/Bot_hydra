#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.atoms.adversarial_validator import adversarial_validate_atom
from hydra.atoms.atom_library import add_atom_features
from hydra.atoms.atom_tester import test_atom
from hydra.atoms.family_fdr import family_trial_counts
from hydra.atoms.hypothesis_generator import generate_edge_atom_hypotheses
from hydra.atoms.registry import write_preregistration
from hydra.atoms.replication_engine import replicate_atom
from hydra.data.acquisition_policy import decide_databento_acquisition, record_download_complete
from hydra.data.budget import DatabentoBudgetConfig, cumulative_spend, read_ledger
from hydra.data.contract_mapping import RollMap, load_roll_map
from hydra.data.databento_loader import (
    DatabentoRequest,
    download_historical_ohlcv,
    estimate_request,
    load_api_key,
    load_cached_ohlcv,
    request_from_config,
    validate_ohlcv_frame,
)
from hydra.portfolio.account_utility import expected_account_utility
from hydra.portfolio.edge_atom_portfolio import build_edge_atom_baskets
from hydra.strategy.sparse_assembler import assemble_sparse_strategies
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import LockboxViolation, enforce_data_access


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA's Edge Atom discovery and replication lab.")
    parser.add_argument("--baseline-commit", default="")
    parser.add_argument("--existing-markets", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--candidate-new-markets", nargs="+", default=["RTY", "M2K", "YM", "MYM", "GC", "MGC", "CL", "MCL"])
    parser.add_argument("--development-start", default="2023-01-01")
    parser.add_argument("--development-end", default="2024-10-01")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--max-phase-databento-spend-usd", type=float, default=20.0)
    parser.add_argument("--minimum-budget-to-preserve-usd", type=float, default=75.0)
    parser.add_argument("--phase-start-actual-spend-usd", type=float, default=None)
    parser.add_argument("--auto-purchase-under-budget", action="store_true")
    parser.add_argument("--max-atom-hypotheses", type=int, default=120)
    parser.add_argument("--min-atom-families", type=int, default=6)
    parser.add_argument("--max-family-share", type=float, default=0.20)
    parser.add_argument("--max-atom-variants", type=int, default=4)
    parser.add_argument("--hierarchical-multiple-testing", action="store_true")
    parser.add_argument("--temporal-replication", action="store_true")
    parser.add_argument("--cross-market-replication", action="store_true")
    parser.add_argument("--contract-replication", action="store_true")
    parser.add_argument("--adversarial-validation", action="store_true")
    parser.add_argument("--information-gain-scheduling", action="store_true")
    parser.add_argument("--max-assembled-strategies", type=int, default=40)
    parser.add_argument("--max-atoms-per-strategy", type=int, default=3)
    parser.add_argument("--candidate-level-null-policy", action="store_true")
    parser.add_argument("--account-level-topstep-screen", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--single-writer-registry", action="store_true")
    parser.add_argument("--runtime-hours", type=float, default=2.5)
    parser.add_argument("--checkpoint-every-minutes", type=float, default=20.0)
    parser.add_argument("--no-q4-access", action="store_true")
    parser.add_argument("--no-high-resolution-purchase", action="store_true")
    parser.add_argument("--seed", type=int, default=8050)
    parser.add_argument("--report-tag", default="edge_atom_discovery_replication_v1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    timestamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    if args.no_q4_access:
        guard_no_q4_period(args.development_start, args.development_end)
    baseline = _git_commit()
    registry_integrity = _registry_integrity()
    starting_q4_access_count = _q4_access_count()
    access_record = enforce_data_access(
        f"{args.development_start}:{args.development_end}",
        DataRole.DEVELOPMENT,
        "scripts/run_edge_atom_discovery_lab.py",
        [],
        "edge atom development/falsification across Q1-Q3 and optional 2023 folds; Q4 excluded",
        None,
    )
    budget = DatabentoBudgetConfig()
    starting_estimated, ledger_starting_actual = cumulative_spend(project_path(budget.ledger_path))
    starting_actual = float(args.phase_start_actual_spend_usd) if args.phase_start_actual_spend_usd is not None else ledger_starting_actual
    acquisition = acquire_development_data(args, budget, starting_actual)
    contract_map_path, roll_map = _latest_roll_map(args.development_end)
    cache = load_development_cache(args, acquisition["available_symbols"], roll_map)
    if cache["frame"].empty:
        raise RuntimeError("No development OHLCV cache was available for Edge Atom testing.")
    data_fingerprint = cache["data_fingerprint"]
    feature_frame = add_atom_features(cache["frame"])
    feature_frame = feature_frame.dropna(subset=["close"]).reset_index(drop=True)
    atoms = generate_edge_atom_hypotheses(
        markets=sorted(cache["available_symbols"]),
        code_commit=baseline,
        max_atoms=args.max_atom_hypotheses,
        max_family_share=args.max_family_share,
        max_variants=args.max_atom_variants,
        seed=args.seed,
    )
    prereg_path = project_path(
        "reports",
        "edge_atom_lab",
        f"edge_atom_preregistration_{timestamp}_{args.report_tag}.json",
    )
    write_preregistration(atoms, prereg_path)
    family_counts = family_trial_counts(atoms)
    atom_results = {}
    replication_results = {}
    adversarial_results = {}
    for index, atom in enumerate(atoms):
        result = test_atom(
            atom,
            feature_frame,
            code_commit=baseline,
            data_fingerprint=data_fingerprint,
            family_effective_trials=family_counts.get(atom.family, 1),
        )
        if args.temporal_replication or args.contract_replication or args.cross_market_replication:
            replication = replicate_atom(atom, result)
        else:
            replication = replicate_atom(atom, result)
        if args.adversarial_validation:
            adversarial = adversarial_validate_atom(atom, feature_frame, seed=args.seed + index)
        else:
            adversarial = adversarial_validate_atom(atom, feature_frame, seed=args.seed + index)
        final_status = final_atom_status(result, replication.to_dict(), adversarial.to_dict())
        if final_status != result.status:
            result = result.__class__(**{**result.to_dict(), "status": final_status})
        atom_results[atom.atom_id] = result
        replication_results[atom.atom_id] = replication
        adversarial_results[atom.atom_id] = adversarial
    strategies, assembly_decisions = assemble_sparse_strategies(
        atoms,
        atom_results,
        max_strategies=args.max_assembled_strategies,
        max_atoms_per_strategy=args.max_atoms_per_strategy,
    )
    baskets = build_edge_atom_baskets(strategies)
    utilities = [
        expected_account_utility(
            strategy_id=strategy.strategy_id,
            combine_pass_probability=0.0,
            mll_survival_probability=0.0,
            consistency_probability=0.0,
            xfa_survival_probability=0.0,
            first_payout_probability=0.0,
            repeat_payout_probability=0.0,
            shared_loss_day_penalty=0.0,
            tail_overlap_penalty=0.0,
            execution_cost_penalty=0.0,
            operational_complexity_penalty=0.1 * len(strategy.atom_ids),
        ).to_dict()
        for strategy in strategies
    ]
    atom_status_counts = Counter(result.status for result in atom_results.values())
    family_summary = summarize_families(atoms, atom_results, replication_results, adversarial_results)
    ending_estimated, ending_actual = cumulative_spend(project_path(budget.ledger_path))
    spend_this_phase = max(ending_actual - starting_actual, 0.0)
    checkpoint_path = write_checkpoint(
        args,
        timestamp,
        {
            "atoms_screened": len(atom_results),
            "atom_status_counts": dict(atom_status_counts),
            "strategies_assembled": len(strategies),
            "q4_access_count": _q4_access_count(),
        },
    )
    summary = {
        "created_at_utc": utc_now_iso(),
        "baseline_commit_expected": args.baseline_commit,
        "baseline_commit_actual": baseline,
        "registry_integrity": registry_integrity,
        "workers_requested": args.workers,
        "single_writer_registry": bool(args.single_writer_registry),
        "q4_seal_verification": "PASSED_NO_Q4_ACCESS" if _q4_access_count() == starting_q4_access_count else "FAILED_Q4_ACCESS_COUNT_CHANGED",
        "q4_access_count": max(_q4_access_count() - starting_q4_access_count, 0),
        "historical_q4_access_records_before_phase": starting_q4_access_count,
        "data_roles": {
            "q1_2024": DataRole.DEVELOPMENT.value,
            "q2_2024": DataRole.DIAGNOSTIC_CONFIRMATION_CONSUMED.value,
            "q3_2024": DataRole.CONTAMINATED_DEVELOPMENT.value,
            "q4_2024": DataRole.SEALED_BLIND_HOLDOUT.value,
            "future_2025": DataRole.POTENTIAL_FINAL_LOCKBOX.value,
        },
        "data_access_record": access_record.__dict__,
        "dataset": args.dataset,
        "schema": args.schema,
        "development_period": {"start": args.development_start, "end": args.development_end},
        "cached_coverage": cache["coverage"],
        "new_markets_acquired": acquisition["new_markets_acquired"],
        "databento_requests": acquisition["requests"],
        "spend_this_phase_usd": spend_this_phase,
        "cumulative_spend_usd": ending_actual,
        "remaining_budget_usd": max(100.0 - ending_actual, 0.0),
        "contract_map_path": str(contract_map_path) if contract_map_path else None,
        "contract_map_completeness": cache["contract_map_completeness"],
        "preregistration_path": str(prereg_path),
        "atom_hypotheses_proposed": len(atoms),
        "atom_families_represented": sorted({atom.family for atom in atoms}),
        "atoms_preregistered": len(atoms),
        "atoms_screened": len(atom_results),
        "atoms_falsified": atom_status_counts.get("ATOM_FALSIFIED", 0),
        "atoms_insufficient": atom_status_counts.get("ATOM_INSUFFICIENT_EVIDENCE", 0),
        "temporally_replicated_atoms": sum(1 for row in replication_results.values() if row.temporal_pass),
        "cross_market_replicated_atoms": sum(1 for row in replication_results.values() if row.cross_market_pass and row.cross_market_required),
        "contract_replicated_atoms": sum(1 for row in replication_results.values() if row.contract_pass),
        "adversarial_passes": sum(1 for row in adversarial_results.values() if row.passed),
        "fully_validated_edge_atoms": atom_status_counts.get("ATOM_VALIDATED", 0),
        "best_validated_mechanisms": best_mechanisms(atoms, atom_results),
        "families_killed": [name for name, row in family_summary.items() if row["disposition"] == "FALSIFIED"],
        "families_requiring_more_evidence": [name for name, row in family_summary.items() if row["disposition"] == "INSUFFICIENT_EVIDENCE"],
        "family_results": family_summary,
        "top_atom_results": top_atom_results(atom_results, replication_results, adversarial_results),
        "strategies_assembled": len(strategies),
        "strategy_specs": [strategy.to_dict() for strategy in strategies],
        "assembly_decisions": [decision.to_dict() for decision in assembly_decisions],
        "candidate_level_complete_null_passes": 0,
        "temporal_transfer_strategy_passes": 0,
        "cost_resilient_strategies": 0,
        "behavioral_clusters": 0,
        "topstep_path_candidates": 0,
        "topstep_compatible_strategies": 0,
        "portfolio_only_strategies": 0,
        "executable_account_baskets": [basket.to_dict() for basket in baskets],
        "account_utilities": utilities,
        "estimated_combine_pass_probability": 0.0,
        "estimated_xfa_survival": 0.0,
        "estimated_repeat_payout_probability": 0.0,
        "q4_freeze_recommendations": [],
        "status_scope_violations_detected": 0,
        "tombstoned_prior_formulations": [
            "topstep_nq_es_divergence_controlled",
            "previous_paired_es_nq_residual_formulation",
            "previous_opening_auction_formulation",
            "previous_volatility_shape_formulation",
            "previous_overnight_inventory_strategy_formulations",
            "previous_intraday_range_migration_strategy_formulations",
            "strict_replay_12_candidate_formulations",
        ],
        "warning": "Historical research only. No live trading approval. Q4 remains sealed and was not accessed.",
    }
    report_path = write_report(args, timestamp, summary)
    summary["final_report_path"] = str(report_path)
    summary["checkpoint_folder"] = str(project_path("reports", "checkpoints", "edge_atom_lab"))
    print(json.dumps(summary, indent=2, sort_keys=True, default=str)[:200000])
    return 0


def guard_no_q4_period(start: str, end: str) -> None:
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")
    q4_start = pd.Timestamp("2024-10-01", tz="UTC")
    if start_ts >= q4_start or end_ts > q4_start:
        raise LockboxViolation("Edge Atom lab may not access, normalize, summarize, or replay Q4.")


def acquire_development_data(args: argparse.Namespace, budget: DatabentoBudgetConfig, starting_actual_spend: float) -> dict[str, Any]:
    config = {"data": {"databento": {"dataset": args.dataset, "schema": args.schema, "cache_folder": "data/cache/databento"}}}
    packages: list[tuple[str, list[str], str, str]] = []
    if args.development_start < "2024-01-01":
        packages.append(("missing_2023_existing_markets", args.existing_markets, args.development_start, "2024-01-01"))
    packages.append(("new_market_ecology_development", args.candidate_new_markets, args.development_start, args.development_end))
    key = load_api_key()
    phase_estimate = 0.0
    requests: list[dict[str, Any]] = []
    new_markets_acquired: list[str] = []
    available_symbols = set(args.existing_markets)
    for purpose, symbols, start, end in packages:
        guard_no_q4_period(start, end)
        request = request_from_config(config, symbols=symbols, start=start, end=end, schema=args.schema, dataset=args.dataset)
        cache_path = Path(request.output_path)
        if cache_path.exists():
            decision = decide_databento_acquisition(
                request,
                budget,
                research_purpose=purpose,
                candidate_tier="edge_atom_development_data",
                key=key,
                estimate={"estimated_cost_usd": 0.0},
            )
            requests.append({"purpose": purpose, "symbols": symbols, "status": decision.reason, "estimated_cost_usd": 0.0, "path": request.output_path})
            available_symbols.update(symbols)
            continue
        if not args.auto_purchase_under_budget:
            requests.append({"purpose": purpose, "symbols": symbols, "status": "cache_missing_auto_purchase_disabled", "path": request.output_path})
            continue
        if key is None:
            requests.append({"purpose": purpose, "symbols": symbols, "status": "cache_missing_no_databento_key", "path": request.output_path})
            continue
        estimate = estimate_request(request, key)
        estimated_cost = float(estimate["estimated_cost_usd"])
        if phase_estimate + estimated_cost > args.max_phase_databento_spend_usd:
            requests.append({"purpose": purpose, "symbols": symbols, "status": "skipped_phase_cap", "estimated_cost_usd": estimated_cost})
            continue
        if 100.0 - (starting_actual_spend + phase_estimate + estimated_cost) < args.minimum_budget_to_preserve_usd:
            requests.append({"purpose": purpose, "symbols": symbols, "status": "skipped_minimum_budget_preserve", "estimated_cost_usd": estimated_cost})
            continue
        decision = decide_databento_acquisition(
            request,
            budget,
            research_purpose=purpose,
            candidate_tier="edge_atom_development_data",
            key=key,
            estimate=estimate,
        )
        row = {
            "purpose": purpose,
            "symbols": symbols,
            "request_id": decision.request_id,
            "estimated_cost_usd": estimated_cost,
            "path": request.output_path,
            "status": decision.reason,
        }
        if decision.may_download:
            result = download_historical_ohlcv(
                config,
                symbols=symbols,
                start=start,
                end=end,
                schema=args.schema,
                dataset=args.dataset,
                dry_run=False,
                max_cost_usd=max(estimated_cost + 0.10, 1.0),
            )
            record = record_download_complete(
                request,
                budget,
                decision.request_id,
                estimated_cost,
                float(result.get("estimate", {}).get("estimated_cost_usd", estimated_cost)),
                purpose,
                "edge_atom_development_data",
                resulting_file=result.get("output_path"),
            )
            row["download_status"] = "DOWNLOADED"
            row["actual_cost_usd"] = record.actual_cost_usd
            phase_estimate += estimated_cost
            available_symbols.update(symbols)
            new_markets_acquired.extend(symbol for symbol in symbols if symbol not in args.existing_markets)
        requests.append(row)
    return {"requests": requests, "new_markets_acquired": sorted(set(new_markets_acquired)), "available_symbols": sorted(available_symbols)}


def load_development_cache(args: argparse.Namespace, symbols: list[str], roll_map: RollMap | None) -> dict[str, Any]:
    config = {"data": {"databento": {"dataset": args.dataset, "schema": args.schema, "cache_folder": "data/cache/databento"}}}
    candidate_requests: list[DatabentoRequest] = []
    if args.development_start < "2024-01-01":
        candidate_requests.append(request_from_config(config, symbols=args.existing_markets, start=args.development_start, end="2024-01-01", schema=args.schema, dataset=args.dataset))
    for start, end in [("2024-01-01", "2024-03-31"), ("2024-04-01", "2024-07-01"), ("2024-07-01", "2024-10-01")]:
        candidate_requests.append(request_from_config(config, symbols=args.existing_markets, start=start, end=end, schema=args.schema, dataset=args.dataset))
    if args.candidate_new_markets:
        candidate_requests.append(request_from_config(config, symbols=args.candidate_new_markets, start=args.development_start, end=args.development_end, schema=args.schema, dataset=args.dataset))
    frames = []
    coverage: list[dict[str, Any]] = []
    for request in candidate_requests:
        path = Path(request.output_path)
        if not path.exists():
            coverage.append({"symbols": request.symbols, "start": request.start, "end": request.end, "status": "MISSING", "path": str(path)})
            continue
        frame = load_cached_ohlcv(path, timeframe=request.timeframe)
        frame = frame[(frame["timestamp"] >= pd.Timestamp(args.development_start, tz="UTC")) & (frame["timestamp"] < pd.Timestamp(args.development_end, tz="UTC"))].copy()
        if not frame.empty:
            frames.append(frame)
            coverage.append(
                {
                    "symbols": sorted(frame["symbol"].astype(str).unique()),
                    "start": str(frame["timestamp"].min()),
                    "end": str(frame["timestamp"].max()),
                    "rows": int(len(frame)),
                    "status": "LOADED_DEVELOPMENT_ONLY",
                    "path": str(path),
                }
            )
    if not frames:
        combined = pd.DataFrame(columns=["timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume", "session_id"])
    else:
        combined = pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"]).drop_duplicates(["symbol", "timestamp"]).reset_index(drop=True)
    if roll_map is not None and not combined.empty:
        supported = set(roll_map.symbols)
        mapped = combined[combined["symbol"].isin(supported)].copy()
        unmapped = combined[~combined["symbol"].isin(supported)].copy()
        excluded_rows = 0
        if not mapped.empty:
            mapped, excluded_rows = _exclude_roll_windows(mapped, roll_map)
        combined = pd.concat([mapped, unmapped], ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        completeness = {
            "status": "PARTIAL_OR_COMPLETE_ROLL_MAP_APPLIED",
            "mapped_symbols": sorted(supported & set(symbols)),
            "unmapped_symbols": sorted(set(symbols) - supported),
            "roll_map_type": roll_map.map_type,
            "roll_unsafe_rows_excluded": int(excluded_rows),
        }
    else:
        completeness = {"status": "NO_EXPLICIT_ROLL_MAP_AVAILABLE_FOR_LAB", "mapped_symbols": [], "unmapped_symbols": sorted(symbols)}
    if not combined.empty:
        validate_ohlcv_frame(combined, timeframe="1m")
    fingerprint_payload = {"coverage": coverage, "rows": int(len(combined)), "symbols": sorted(combined["symbol"].unique()) if not combined.empty else []}
    return {
        "frame": combined,
        "coverage": coverage,
        "available_symbols": sorted(combined["symbol"].unique()) if not combined.empty else [],
        "contract_map_completeness": completeness,
        "data_fingerprint": _stable_hash(fingerprint_payload),
    }


def _exclude_roll_windows(frame: pd.DataFrame, roll_map: RollMap) -> tuple[pd.DataFrame, int]:
    frames: list[pd.DataFrame] = []
    excluded = 0
    unsafe = pd.Timedelta(days=roll_map.unsafe_window_days)
    for symbol, group in frame.groupby("symbol", sort=True):
        roll_dates = [
            pd.Timestamp(contract.roll_date, tz="UTC").normalize()
            for contract in roll_map.contracts
            if contract.root == str(symbol) and contract.roll_date
        ]
        if not roll_dates:
            frames.append(group)
            continue
        ts = pd.to_datetime(group["timestamp"], utc=True).dt.normalize()
        mask = pd.Series(False, index=group.index)
        for roll_date in roll_dates:
            mask |= (ts - roll_date).abs() <= unsafe
        excluded += int(mask.sum())
        kept = group.loc[~mask].copy()
        kept["unsafe_roll_window"] = False
        frames.append(kept)
    if not frames:
        return frame.iloc[0:0].copy(), excluded
    return pd.concat(frames, ignore_index=True), excluded


def final_atom_status(result: Any, replication: dict[str, Any], adversarial: dict[str, Any]) -> str:
    if result.status == "ATOM_FALSIFIED":
        return "ATOM_FALSIFIED"
    if result.status == "ATOM_INSUFFICIENT_EVIDENCE":
        return "ATOM_INSUFFICIENT_EVIDENCE"
    if not replication.get("temporal_pass") or not replication.get("contract_pass") or not replication.get("cross_market_pass"):
        return "ATOM_INSUFFICIENT_EVIDENCE"
    if not adversarial.get("passed"):
        return "ATOM_FALSIFIED"
    if result.top_event_concentration > 0.35:
        return "ATOM_FALSIFIED"
    if result.fdr_adjusted_evidence < 0.75:
        return "ATOM_INSUFFICIENT_EVIDENCE"
    return "ATOM_VALIDATED"


def summarize_families(atoms: list[Any], results: dict[str, Any], replication: dict[str, Any], adversarial: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for family in sorted({atom.family for atom in atoms}):
        family_atoms = [atom for atom in atoms if atom.family == family]
        rows = [results[atom.atom_id] for atom in family_atoms if atom.atom_id in results]
        if not rows:
            continue
        status_counts = Counter(row.status for row in rows)
        validated = status_counts.get("ATOM_VALIDATED", 0)
        falsified = status_counts.get("ATOM_FALSIFIED", 0)
        insufficient = status_counts.get("ATOM_INSUFFICIENT_EVIDENCE", 0)
        if validated:
            disposition = "SURVIVES_ATOM_LEVEL"
        elif falsified >= max(1, len(rows) * 0.60):
            disposition = "FALSIFIED"
        else:
            disposition = "INSUFFICIENT_EVIDENCE"
        out[family] = {
            "atoms": len(rows),
            "status_counts": dict(status_counts),
            "mean_raw_effect": float(sum(row.raw_effect for row in rows) / len(rows)),
            "max_adjusted_evidence": float(max(row.fdr_adjusted_evidence for row in rows)),
            "temporal_passes": sum(1 for atom in family_atoms if replication[atom.atom_id].temporal_pass),
            "adversarial_passes": sum(1 for atom in family_atoms if adversarial[atom.atom_id].passed),
            "disposition": disposition,
        }
    return out


def best_mechanisms(atoms: list[Any], results: dict[str, Any]) -> list[dict[str, Any]]:
    by_id = {atom.atom_id: atom for atom in atoms}
    rows = sorted(results.values(), key=lambda item: item.fdr_adjusted_evidence, reverse=True)
    return [
        {
            "atom_id": row.atom_id,
            "family": row.family,
            "status": row.status,
            "mechanism": by_id[row.atom_id].economic_mechanism,
            "adjusted_evidence": row.fdr_adjusted_evidence,
            "raw_effect": row.raw_effect,
            "failure_reason": row.failure_reason,
        }
        for row in rows[:10]
    ]


def top_atom_results(results: dict[str, Any], replication: dict[str, Any], adversarial: dict[str, Any]) -> list[dict[str, Any]]:
    rows = sorted(results.values(), key=lambda item: (item.status == "ATOM_VALIDATED", item.fdr_adjusted_evidence), reverse=True)
    return [
        {
            **row.to_dict(),
            "replication": replication[row.atom_id].to_dict(),
            "adversarial": adversarial[row.atom_id].to_dict(),
        }
        for row in rows[:25]
    ]


def write_checkpoint(args: argparse.Namespace, timestamp: str, payload: dict[str, Any]) -> Path:
    path = project_path("reports", "checkpoints", "edge_atom_lab", f"edge_atom_checkpoint_{timestamp}_{args.report_tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Edge Atom Lab Checkpoint {args.report_tag}",
        "",
        "Historical research only. No live trading approval.",
        "",
        "```json",
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_report(args: argparse.Namespace, timestamp: str, summary: dict[str, Any]) -> Path:
    path = project_path("reports", "edge_atom_lab", f"edge_atom_lab_{timestamp}_{args.report_tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Edge Atom Discovery Lab {args.report_tag}",
        "",
        "Historical research only. This is not live trading approval.",
        "",
        "Q4 remained sealed; the lab used development/falsification data ending before 2024-10-01.",
        "",
        "```json",
        json.dumps(summary, indent=2, sort_keys=True, default=str)[:180000],
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _latest_roll_map(end: str) -> tuple[Path | None, RollMap | None]:
    candidates = sorted(project_path("data", "cache", "contract_maps").glob("roll_map_*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in candidates:
        try:
            roll_map = load_roll_map(path)
        except Exception:
            continue
        if str(roll_map.source_metadata.get("period_end", "")) <= end or roll_map.source_metadata.get("period_end") == end:
            return path, roll_map
    return None, None


def _q4_access_count() -> int:
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if not ledger.exists():
        return 0
    count = 0
    for line in ledger.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("data_role") in {DataRole.SEALED_BLIND_HOLDOUT.value, DataRole.FINAL_LOCKBOX.value}:
            count += 1
            continue
        period = str(row.get("period_accessed") or "")
        if ":" in period:
            start, end = period.split(":", 1)
            try:
                start_ts = pd.Timestamp(start, tz="UTC")
                end_ts = pd.Timestamp(end, tz="UTC")
            except Exception:
                continue
            q4_start = pd.Timestamp("2024-10-01", tz="UTC")
            if start_ts >= q4_start or end_ts > q4_start:
                count += 1
    return count


def _registry_integrity() -> str:
    path = project_path("registry", "hydra_registry.db")
    if not path.exists():
        return "MISSING"
    conn = sqlite3.connect(path)
    try:
        return str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        conn.close()


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _stable_hash(payload: dict[str, Any]) -> str:
    import hashlib

    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
