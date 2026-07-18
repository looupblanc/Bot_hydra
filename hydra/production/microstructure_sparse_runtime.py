"""Stable production-kernel adapter for HYDRA campaign 0032.

The scientific runner owns sparse opportunity construction and economic
evaluation.  This module is deliberately limited to the persistent-runtime
contract: validate the frozen manifest, publish controller-readable state and
KPIs, invoke the bounded pilot, atomically finalize its EvidenceBundle, and
write one hashed terminal result.  It never downloads or purchases data.
"""

from __future__ import annotations

import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import (
    EvidenceBundleWriter,
    REQUIRED_DATASETS,
    recover_finalized_evidence_bundle,
    verify_evidence_bundle,
)
from hydra.production.halving import build_final_result_payload
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.production.microstructure_sparse_manifest import (
    CAMPAIGN_ID,
    GATE_DECISIONS,
    RUNTIME_VERSION,
    validate_microstructure_sparse_manifest,
)
from hydra.production.runtime import PRODUCTION_KPI_SCHEMA, PRODUCTION_STATE_SCHEMA


RESULT_SCHEMA = "hydra_economic_production_result_v1"
SCIENTIFIC_STATE_SCHEMA = "hydra_microstructure_sparse_alpha_0032_state_v1"
SCIENTIFIC_KPI_SCHEMA = "hydra_microstructure_sparse_alpha_0032_kpis_v1"
SCIENTIFIC_RESULT_SCHEMA = "hydra_microstructure_sparse_alpha_0032_result_v1"
STATE_SCHEMA = PRODUCTION_STATE_SCHEMA
KPI_SCHEMA = PRODUCTION_KPI_SCHEMA


class SparseRuntimeError(RuntimeError):
    """Campaign 0032 cannot continue without violating its frozen contract."""


def read_microstructure_sparse_status(manifest_path: str | Path) -> dict[str, Any]:
    path = Path(manifest_path).resolve()
    manifest = load_and_validate_production_manifest(path)
    output = path.parents[2] / str(manifest["runtime"]["output_dir"])
    result = output / str(
        manifest["runtime"].get("result_name", "economic_production_result.json")
    )
    if result.is_file():
        return _read_hashed(result, "result_hash")
    state = output / "production_state.json"
    if state.is_file():
        return _read_hashed(state, "state_hash")
    return {
        "campaign_id": CAMPAIGN_ID,
        "state": "NOT_STARTED",
        "next_action": "RUN_0031_FORENSIC_BRIDGE_THEN_SPARSE_PILOT",
    }


def run_microstructure_sparse_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path | None = None,
    cache_root: str | Path | None = None,
    stop_after: str | None = None,
) -> dict[str, Any]:
    """Run/resume the bounded 0032 pilot without any acquisition side effect."""

    del contract_map_path, cache_root
    if stop_after is not None and os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
        raise SparseRuntimeError("0032 stop_after is restricted to explicit test mode")
    path = Path(manifest_path).resolve()
    root = path.parents[2]
    manifest = load_and_validate_production_manifest(path)
    validate_microstructure_sparse_manifest(manifest, manifest_path=path)
    output = root / str(manifest["runtime"]["output_dir"])
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / str(
        manifest["runtime"].get("result_name", "economic_production_result.json")
    )
    if result_path.is_file():
        return _read_hashed(result_path, "result_hash")

    _set_single_thread_libraries()
    _write_state(
        output,
        manifest,
        state="STARTING",
        stage="FORENSIC_PNL_BRIDGE_AND_OPPORTUNITY_CONSOLIDATION",
        next_action="RUN_BOUNDED_FORENSIC_BRIDGE_THEN_SPARSE_ECONOMIC_REPLAY",
    )
    if stop_after and stop_after.upper() in {"START", "STARTING"}:
        return _read_hashed(output / "production_state.json", "state_hash")

    from hydra.production.microstructure_sparse_pilot import (
        SparsePilotConfig,
        run_microstructure_sparse_pilot,
    )

    config = _pilot_config(manifest, SparsePilotConfig)
    source_dir = _source_store_dir(root, manifest)
    _write_state(
        output,
        manifest,
        state="COMPONENT_LEDGER_COMPILED",
        stage="SPARSE_META_LABEL_AND_ECONOMIC_REPLAY",
        next_action="EVALUATE_AT_MOST_30_FROZEN_SPARSE_STRATEGIES",
    )
    pilot_value = run_microstructure_sparse_pilot(
        source_dir=source_dir,
        output_dir=output / "pilot",
        config=config,
    )
    pilot = _pilot_mapping(pilot_value)
    decision = str(pilot.get("pilot_status") or pilot.get("decision") or "")
    if decision not in set(GATE_DECISIONS):
        raise SparseRuntimeError("0032 pilot returned an unsupported scientific decision")
    candidate_count = _candidate_count(pilot)
    if candidate_count > 30:
        raise SparseRuntimeError("0032 sparse pilot exceeded its frozen 30-strategy cap")

    _write_state(
        output,
        manifest,
        state="FINALIZING",
        stage="EVIDENCE_RECONCILIATION_AND_ATOMIC_FINALIZATION",
        next_action="SEAL_COMPLETE_CAUSAL_EVIDENCE_BUNDLE",
        pilot=pilot,
        extra={"decision": decision},
    )
    evidence_receipt = _seal_evidence_bundle(root, output, manifest, pilot)
    conditional_cost = _conditional_cost_report(output, manifest, pilot, decision)
    result = _build_terminal_result(
        manifest=manifest,
        pilot=pilot,
        evidence_receipt=evidence_receipt,
        decision=decision,
        conditional_cost_report=conditional_cost,
    )
    _atomic_json(result_path, result)
    _write_state(
        output,
        manifest,
        state="COMPLETE",
        stage="SPARSE_PILOT_DECISION_SEALED",
        next_action=str(result["autonomous_next_action"]["action"]),
        pilot=pilot,
        extra={
            "decision": decision,
            "actual_additional_spend_usd": 0.0,
            "conditional_cost_matrix_status": conditional_cost["status"],
        },
    )
    return result


def _pilot_config(manifest: Mapping[str, Any], config_type: Any) -> Any:
    source_hashes = manifest["source_store"]["source_hashes"]
    sparse = manifest["sparse_policy_frontier"]
    exits = manifest["holding_exit_frontier"]
    execution = manifest["execution_model"]
    kwargs: dict[str, Any] = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": str(manifest["manifest_hash"]),
        "source_commit": str(manifest["source_commit"]),
        "source_store_hash": stable_hash(source_hashes),
        "cpu_worker_count": int(manifest["compute_contract"]["cpu_worker_count"]),
        "edge_to_cost_ratios": tuple(
            float(value) for value in sparse["edge_to_cost_ratios"]
        ),
        "trade_budgets": tuple(
            int(value) for value in sparse["trade_budgets_per_session"]
        ),
        "holding_horizons_seconds": tuple(
            int(value) for value in exits["horizons_seconds"]
        ),
        "exit_policies": tuple(str(value) for value in exits["exit_types"]),
        "maximum_strategies": int(sparse["max_strategies"]),
        "stressed_cost_multiplier": float(execution["stressed_cost_multiplier"]),
    }
    optional = manifest.get("pilot_configuration")
    if isinstance(optional, Mapping):
        # Only explicit, known typed fields can override dataclass defaults.
        allowed = set(getattr(config_type, "__dataclass_fields__", {})) - {
            "campaign_id",
            "manifest_hash",
            "source_commit",
            "source_store_hash",
            "cpu_worker_count",
            "edge_to_cost_ratios",
            "trade_budgets",
            "holding_horizons_seconds",
            "exit_policies",
            "maximum_strategies",
            "stressed_cost_multiplier",
        }
        kwargs.update({str(key): value for key, value in optional.items() if key in allowed})
    return config_type(**kwargs)


def _source_store_dir(root: Path, manifest: Mapping[str, Any]) -> Path:
    descriptor = manifest["source_store"]["source_hashes"]["feature_matrices"]
    source_file = (root / str(descriptor["path"])).resolve()
    for candidate in source_file.parents:
        expected = candidate / "datasets/feature_matrices/part-000000.parquet"
        if expected.is_file() and expected.resolve() == source_file:
            return candidate
    raise SparseRuntimeError("0032 immutable 0031 pilot source root cannot be resolved")


def _pilot_mapping(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise SparseRuntimeError("0032 pilot returned no decision-grade mapping")
    return dict(value)


def _seal_evidence_bundle(
    root: Path,
    output: Path,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
) -> dict[str, Any]:
    identity = pilot.get("evidence_identity")
    datasets = pilot.get("evidence_datasets")
    compact = pilot.get("compact_outputs")
    if (
        not isinstance(identity, Mapping)
        or not isinstance(datasets, Mapping)
        or set(datasets) != set(REQUIRED_DATASETS)
        or not isinstance(compact, Mapping)
    ):
        raise SparseRuntimeError("0032 pilot lacks complete canonical EvidenceBundle material")
    compact = dict(compact)
    compact.setdefault(
        "next_campaign_recommendations",
        _compact_next_campaign_recommendation(str(pilot.get("pilot_status") or "")),
    )
    base = root / str(manifest["evidence_bundle"]["destination"])
    final = base / f"{CAMPAIGN_ID}.evidence-v1"
    lightweight = output / "evidence_bundle_receipt.json"
    if final.is_dir():
        receipt = recover_finalized_evidence_bundle(
            base,
            CAMPAIGN_ID,
            lightweight_manifest_path=lightweight,
            expected_identity=identity,
        )
        return receipt.to_dict()
    staging = base / f".{CAMPAIGN_ID}.evidence-v1.staging"
    writer = (
        EvidenceBundleWriter.resume(base, CAMPAIGN_ID, expected_identity=identity)
        if staging.is_dir()
        else EvidenceBundleWriter.create(base, identity, writer_id=CAMPAIGN_ID)
    )
    try:
        for dataset in REQUIRED_DATASETS:
            if int(writer.dataset_row_counts.get(dataset, 0)) == 0:
                writer.append_records(
                    dataset,
                    datasets[dataset],
                    batch_id=f"0032-{dataset}-0000",
                )
        for name, value in compact.items():
            writer.write_compact_output(str(name), value)
        receipt = writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=lightweight,
        )
    finally:
        writer.close()
    verify_evidence_bundle(receipt.bundle_path, deep=True)
    return receipt.to_dict()


def _compact_next_campaign_recommendation(decision: str) -> dict[str, Any]:
    """Return the mandatory EvidenceBundle successor output without mutation."""

    actions = {
        "SPARSE_PILOT_GREEN": "GENERATE_OFFICIAL_CONDITIONAL_DATA_COST_MATRIX_NO_PURCHASE",
        "SPARSE_PILOT_WEAK": "PRESERVE_GROSS_ALPHA_AND_RUN_ONE_BOUNDED_REFINEMENT_NO_PURCHASE",
        "SPARSE_PILOT_FALSIFIED": "TERMINATE_SPARSE_REPRESENTATION_AND_IDENTIFY_DISTINCT_INFORMATION_SOURCE",
    }
    if decision not in actions:
        raise SparseRuntimeError("0032 cannot seal an unsupported successor decision")
    return {
        "schema": "hydra_production_next_campaign_recommendations_v1",
        "campaign_id": CAMPAIGN_ID,
        "recommendation": {
            "action": actions[decision],
            "manifest_required": True,
            "automatic_data_purchase_authorized": False,
            "new_data_purchase_authorized": False,
            "q4_access_authorized": False,
        },
    }


def _conditional_cost_report(
    output: Path,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
    decision: str,
) -> dict[str, Any]:
    path = output / "conditional_data_cost_matrix.json"
    if path.is_file():
        report = _read_hashed(path, "cost_matrix_hash")
        if report.get("manifest_hash") != manifest.get("manifest_hash"):
            raise SparseRuntimeError("0032 conditional cost report manifest drift")
        if float(report.get("actual_additional_spend_usd", -1.0)) != 0.0:
            raise SparseRuntimeError("0032 runtime may not purchase conditional data")
        return report
    extension = manifest["conditional_extension"]
    matrix = pilot.get("conditional_data_cost_matrix")
    if decision == "SPARSE_PILOT_GREEN" and isinstance(matrix, Mapping):
        status = "OFFICIAL_COST_MATRIX_AVAILABLE_NO_PURCHASE"
        estimates: Mapping[str, Any] = dict(matrix)
    elif decision == "SPARSE_PILOT_GREEN":
        status = "OFFICIAL_COST_MATRIX_REQUIRED_NO_PURCHASE"
        estimates = {}
    else:
        status = "NOT_TRIGGERED_BY_SPARSE_GATE"
        estimates = {}
    core = {
        "schema": "hydra_microstructure_sparse_0032_conditional_cost_matrix_v1",
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "status": status,
        "schemas": ["trades", "tbbo", "mbp-1"],
        "session_counts": [10, 20, 30, 40],
        "official_metadata_estimates": estimates,
        "maximum_incremental_spend_usd": float(
            extension["maximum_incremental_spend_usd"]
        ),
        "minimum_budget_reserve_usd": float(
            extension["minimum_budget_reserve_usd"]
        ),
        "actual_additional_spend_usd": 0.0,
        "automatic_purchase_allowed": False,
        "purchase_performed": False,
        "q4_accessed": False,
        "generated_at_utc": _utc_now(),
    }
    report = {**core, "cost_matrix_hash": stable_hash(core)}
    _atomic_json(path, report)
    return report


def _write_state(
    output: Path,
    manifest: Mapping[str, Any],
    *,
    state: str,
    stage: str,
    next_action: str,
    pilot: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prior = _read_existing_snapshot(
        output / "production_state.json", "state_hash", manifest
    )
    sequence = int((prior or {}).get("checkpoint_sequence", 0)) + 1
    counters = _controller_kpis(
        manifest, pilot, state=state, checkpoint_sequence=sequence
    )
    evidence_base = Path(str(manifest["evidence_bundle"]["destination"]))
    if not evidence_base.is_absolute():
        evidence_base = output.parents[2] / evidence_base
    core = {
        "schema": STATE_SCHEMA,
        "scientific_schema": SCIENTIFIC_STATE_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "state": state,
        "stage": stage,
        "next_action": next_action,
        "checkpoint_sequence": sequence,
        "started_at_utc": str((prior or {}).get("started_at_utc") or _utc_now()),
        "updated_at_utc": _utc_now(),
        "runner_pid": os.getpid(),
        "worker_count": 2,
        "evidence_writer_count": 1,
        "policies_proposed": int(counters["policies_proposed"]),
        "unique_policies_screened": int(counters["unique_policies_screened"]),
        "exact_account_replays": int(counters["exact_account_replays"]),
        "combine_episodes_completed": int(counters["combine_episodes_completed"]),
        "evidence_staging_path": str(
            evidence_base / f".{CAMPAIGN_ID}.evidence-v1.staging"
        ),
        "evidence_final_path": str(evidence_base / f"{CAMPAIGN_ID}.evidence-v1"),
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        **dict(extra or {}),
    }
    value = {**core, "state_hash": stable_hash(core)}
    _atomic_json(output / "production_state.json", value)
    _write_kpis(output, manifest, pilot, state=state, checkpoint_sequence=sequence)
    return value


def _write_kpis(
    output: Path,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any] | None,
    *,
    state: str,
    checkpoint_sequence: int,
) -> dict[str, Any]:
    core = _controller_kpis(
        manifest, pilot, state=state, checkpoint_sequence=checkpoint_sequence
    )
    _atomic_json(output / "production_kpis.json", {**core, "kpi_hash": stable_hash(core)})
    return core


def _controller_kpis(
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any] | None,
    *,
    state: str,
    checkpoint_sequence: int,
) -> dict[str, Any]:
    metrics = _metrics(pilot)
    candidate_count = _candidate_count(pilot)
    normal = _nonnegative_int(metrics.get("normal_episode_count", 0))
    stressed = _nonnegative_int(metrics.get("stressed_episode_count", 0))
    if normal != stressed:
        raise SparseRuntimeError("0032 normal/stressed episode counters do not reconcile")
    values = {
        "schema": KPI_SCHEMA,
        "scientific_schema": SCIENTIFIC_KPI_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "checkpoint_sequence": int(checkpoint_sequence),
        "updated_at_utc": _utc_now(),
        "state": state,
        "rates_per_hour": {
            "policies_proposed": _finite(metrics.get("strategies_per_hour")),
            "unique_policies_screened": _finite(metrics.get("strategies_per_hour")),
            "exact_account_replays": _finite(metrics.get("exact_replays_per_hour")),
            "combine_episodes": _finite(metrics.get("account_episodes_per_hour")),
        },
        "workers": {"compute": 2, "evidence_writer": 1},
        "policies_proposed": candidate_count,
        "unique_policies_screened": candidate_count,
        "exact_account_replays": candidate_count,
        "combine_episodes_completed": normal + stressed,
        "normal_episodes_completed": normal,
        "stressed_episodes_completed": stressed,
        "positive_stressed_net_candidates": min(
            candidate_count, _nonnegative_int(metrics.get("positive_stressed_count", 0))
        ),
        "candidates_with_normal_pass": min(
            candidate_count, _nonnegative_int(metrics.get("normal_pass_candidate_count", 0))
        ),
        "candidates_with_stressed_pass": min(
            candidate_count, _nonnegative_int(metrics.get("stressed_pass_candidate_count", 0))
        ),
        "best_normal_pass_rate": _unit(metrics.get("normal_p5_pass_rate_best")),
        "best_stressed_pass_rate": _unit(metrics.get("stressed_p5_pass_rate_best")),
        "median_normal_pass_rate": _unit(metrics.get("normal_p5_pass_rate_median")),
        "median_stressed_pass_rate": _unit(metrics.get("stressed_p5_pass_rate_median")),
        "near_pass_count": min(
            candidate_count, _nonnegative_int(metrics.get("near_pass_count", 0))
        ),
        "candidates_promoted_96": 0,
        "candidates_surviving_96": 0,
        "candidates_promoted_192": 0,
        "confirmation_ready_candidates": 0,
        "duplicate_rejection_rate": 0.0,
        "cache_hit_rate": 1.0,
        "economic_research_wall_clock_fraction": _unit(
            metrics.get("economic_wall_clock_fraction")
        ),
        "cpu_utilization_fraction": _unit(metrics.get("cpu_utilization_fraction")),
        "admin_overhead_alert": False,
        "matched_controls_status": str(
            metrics.get("matched_controls_status") or "BOUNDED_SPARSE_CONTROLS"
        ),
        "null_status": str(metrics.get("null_status") or "BOUNDED_SPARSE_NULLS"),
        "opportunity_episode_count": _nonnegative_int(
            metrics.get("opportunity_episode_count", 0)
        ),
        "sparse_strategies_evaluated": candidate_count,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }
    return values


def _build_terminal_result(
    *,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
    evidence_receipt: Mapping[str, Any],
    decision: str,
    conditional_cost_report: Mapping[str, Any],
) -> dict[str, Any]:
    kpis = _controller_kpis(manifest, pilot, state="COMPLETE", checkpoint_sequence=0)
    metrics = _metrics(pilot)
    candidate_count = _candidate_count(pilot)
    normal = int(kpis["normal_episodes_completed"])
    stressed = int(kpis["stressed_episodes_completed"])
    economic_results = {
        "schema": "hydra_microstructure_sparse_alpha_0032_economics_v1",
        "production_counters": {
            "serious_exact_account_replays": candidate_count,
            "predeclared_control_policy_replays": _nonnegative_int(
                metrics.get("control_replay_count", 0)
            ),
            "combine_episodes_completed": normal + stressed,
            "normal_episodes_completed": normal,
            "stressed_episodes_completed": stressed,
        },
        "production_kpis": {
            "rates_per_hour": dict(kpis["rates_per_hour"]),
            "economic_research_wall_clock_fraction": float(
                kpis["economic_research_wall_clock_fraction"]
            ),
            "cpu_utilization_fraction": float(kpis["cpu_utilization_fraction"]),
            "workers": dict(kpis["workers"]),
            "duplicate_rejection_rate": 0.0,
            "cache_hit_rate": 1.0,
        },
        "economic_frontier": {
            "candidate_count": candidate_count,
            "positive_stressed_net_count": int(kpis["positive_stressed_net_candidates"]),
            "normal_pass_fraction_best": float(kpis["best_normal_pass_rate"]),
            "normal_pass_fraction_median": float(kpis["median_normal_pass_rate"]),
            "stressed_pass_fraction_best": float(kpis["best_stressed_pass_rate"]),
            "stressed_pass_fraction_median": float(kpis["median_stressed_pass_rate"]),
            "stressed_target_progress_median_best": _finite(
                metrics.get("stressed_target_progress_best_fraction")
            ),
            "stressed_target_progress_median_population": _finite(
                metrics.get("stressed_target_progress_median_fraction")
            ),
            "stressed_mll_breach_rate_minimum": _unit(
                metrics.get("mll_breach_rate_minimum")
            ),
            "stressed_mll_breach_rate_maximum": _unit(
                metrics.get("mll_breach_rate_maximum")
            ),
        },
        "candidate_count": candidate_count,
        "normal_pass_candidate_count": int(kpis["candidates_with_normal_pass"]),
        "stressed_pass_candidate_count": int(kpis["candidates_with_stressed_pass"]),
        "positive_stressed_net_count": int(kpis["positive_stressed_net_candidates"]),
        "confirmation_ready_candidate_ids": [],
        "stage5_96_start_candidate_ids": [],
        "development_finalist_ids": [],
        "matched_controls_status": str(kpis["matched_controls_status"]),
        "null_status": str(kpis["null_status"]),
        "pilot_status": decision,
        "actual_additional_spend_usd": 0.0,
        "remaining_budget_usd": float(
            manifest["conditional_extension"]["current_remaining_budget_usd"]
        ),
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
        "development_only": True,
        "independently_confirmed": False,
    }
    economic_results["summary_hash"] = stable_hash(economic_results)
    result = build_final_result_payload(
        manifest=manifest,
        kpis=kpis,
        economic_results=economic_results,
        successive_halving={
            "schema": "hydra_microstructure_sparse_0032_gate_v1",
            "stage_decisions": [{
                "stage": "BOUNDED_SPARSE_PILOT",
                "input_count": candidate_count,
                "output_count": len(_survivor_ids(pilot)) if decision == "SPARSE_PILOT_GREEN" else 0,
                "selected_policy_ids": _survivor_ids(pilot) if decision == "SPARSE_PILOT_GREEN" else [],
            }],
            "thresholds_changed_after_results": False,
            "mass_scale_before_green": False,
        },
        matched_controls={
            "schema": "hydra_microstructure_sparse_0032_controls_v1",
            "evaluated_control_policy_count": _nonnegative_int(
                metrics.get("control_replay_count", 0)
            ),
            "control_ids": [
                "DIRECTION_FLIP",
                "SESSION_MATCHED_TIMING_NULL",
                "EXPOSURE_MATCHED_RANDOM",
            ],
            "controls_selected_after_outcomes": False,
        },
        failure_vectors=_failure_vectors(pilot),
        evidence_receipt=evidence_receipt,
        autonomous_next_action=_next_action(decision, pilot, conditional_cost_report),
        scientific_status=decision,
    )
    result.pop("result_hash", None)
    result.update(
        {
            "scientific_schema": SCIENTIFIC_RESULT_SCHEMA,
            "campaign_mode": manifest["campaign_mode"],
            "runtime_version": RUNTIME_VERSION,
            "completed_at_utc": _utc_now(),
            "decision": decision,
            "pilot_summary": _public_pilot_summary(pilot),
            "conditional_cost_matrix": dict(conditional_cost_report),
            "actual_additional_spend_usd": 0.0,
            "mass_scale_started": False,
            "xfa_paths": 0,
        }
    )
    result["result_hash"] = stable_hash(result)
    return result


def _next_action(
    decision: str,
    pilot: Mapping[str, Any],
    cost_report: Mapping[str, Any],
) -> dict[str, Any]:
    if decision == "SPARSE_PILOT_GREEN":
        action = (
            "AWAIT_EXPLICIT_BOUNDED_EXTENSION_AUTHORIZATION"
            if cost_report["status"] == "OFFICIAL_COST_MATRIX_AVAILABLE_NO_PURCHASE"
            else "GENERATE_OFFICIAL_CONDITIONAL_DATA_COST_MATRIX_NO_PURCHASE"
        )
        candidates = _survivor_ids(pilot)
    elif decision == "SPARSE_PILOT_WEAK":
        action = "PRESERVE_GROSS_ALPHA_AND_RUN_ONE_BOUNDED_REFINEMENT_NO_PURCHASE"
        candidates = []
    else:
        action = "TERMINATE_SPARSE_REPRESENTATION_AND_IDENTIFY_DISTINCT_INFORMATION_SOURCE"
        candidates = []
    return {
        "action": action,
        "candidate_ids": candidates,
        "manifest_required": True,
        "automatic_data_purchase_authorized": False,
        "new_data_purchase_authorized": False,
        "q4_access_authorized": False,
    }


def _metrics(pilot: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(pilot, Mapping):
        return {}
    merged: dict[str, Any] = {}
    runtime = pilot.get("runtime_kpis")
    if isinstance(runtime, Mapping):
        merged.update(runtime)
    report = pilot.get("decision_report")
    if isinstance(report, Mapping):
        value = report.get("production_kpis")
        if isinstance(value, Mapping):
            merged.update(value)
    direct = pilot.get("production_kpis")
    if isinstance(direct, Mapping):
        merged.update(direct)
    runtime_metrics = pilot.get("runtime_metrics")
    if isinstance(runtime_metrics, Mapping):
        merged.setdefault(
            "cpu_utilization_fraction",
            runtime_metrics.get("cpu_utilization_fraction_three_core", 0.0),
        )
        merged.setdefault(
            "economic_wall_clock_fraction",
            runtime_metrics.get("economic_wall_clock_fraction", 0.0),
        )
        elapsed = _finite(runtime_metrics.get("elapsed_seconds"))
    else:
        elapsed = 0.0
    merged.setdefault(
        "exact_replay_count",
        _nonnegative_int(pilot.get("sparse_strategies_evaluated", 0)),
    )
    merged.setdefault(
        "opportunity_episode_count",
        _nonnegative_int(pilot.get("opportunity_episode_count", 0)),
    )
    results = pilot.get("candidate_results")
    if isinstance(results, Sequence) and not isinstance(results, (str, bytes)):
        candidate_rows = [row for row in results if isinstance(row, Mapping)]
        paths = [
            path
            for row in candidate_rows
            for path in row.get("account_paths", ())
            if isinstance(path, Mapping)
        ]
        normal_paths = [path for path in paths if path.get("scenario") == "NORMAL"]
        stressed_paths = [
            path for path in paths if path.get("scenario") == "STRESSED_1_5X"
        ]
        merged.setdefault("normal_episode_count", len(normal_paths))
        merged.setdefault("stressed_episode_count", len(stressed_paths))
        merged.setdefault(
            "positive_stressed_count",
            sum(float(row.get("stressed_net_usd", 0.0)) > 0.0 for row in candidate_rows),
        )
        normal_pass_ids = {
            str(path.get("strategy_id"))
            for path in normal_paths
            if path.get("target_reached") is True
        }
        stressed_pass_ids = {
            str(path.get("strategy_id"))
            for path in stressed_paths
            if path.get("target_reached") is True
        }
        merged.setdefault("normal_pass_candidate_count", len(normal_pass_ids))
        merged.setdefault("stressed_pass_candidate_count", len(stressed_pass_ids))
        normal_rates = _candidate_p5_rates(normal_paths)
        stressed_rates = _candidate_p5_rates(stressed_paths)
        merged.setdefault("normal_p5_pass_rate_best", max(normal_rates, default=0.0))
        merged.setdefault(
            "normal_p5_pass_rate_median", _median(normal_rates)
        )
        merged.setdefault(
            "stressed_p5_pass_rate_best", max(stressed_rates, default=0.0)
        )
        merged.setdefault(
            "stressed_p5_pass_rate_median", _median(stressed_rates)
        )
        full_stressed = [path for path in stressed_paths if path.get("full_coverage") is True]
        breach_rates = _candidate_mll_rates(full_stressed)
        merged.setdefault("mll_breach_rate_minimum", min(breach_rates, default=0.0))
        merged.setdefault("mll_breach_rate_maximum", max(breach_rates, default=0.0))
        progress = [
            float(path.get("target_progress_pct", 0.0)) / 100.0
            for path in full_stressed
            if int(path.get("horizon_days", 0)) == 5
        ]
        merged.setdefault(
            "stressed_target_progress_best_fraction", max(progress, default=0.0)
        )
        merged.setdefault(
            "stressed_target_progress_median_fraction", _median(progress)
        )
    if elapsed > 0.0:
        candidate_count = _nonnegative_int(merged.get("exact_replay_count", 0))
        episode_count = _nonnegative_int(merged.get("normal_episode_count", 0)) + _nonnegative_int(
            merged.get("stressed_episode_count", 0)
        )
        merged.setdefault("strategies_per_hour", 3600.0 * candidate_count / elapsed)
        merged.setdefault("exact_replays_per_hour", 3600.0 * candidate_count / elapsed)
        merged.setdefault("account_episodes_per_hour", 3600.0 * episode_count / elapsed)
    return merged


def _candidate_count(pilot: Mapping[str, Any] | None) -> int:
    metrics = _metrics(pilot)
    for key in ("exact_replay_count", "sparse_strategies_evaluated", "candidate_count"):
        if key in metrics:
            return _nonnegative_int(metrics[key])
    if isinstance(pilot, Mapping):
        if "sparse_strategies_evaluated" in pilot:
            return _nonnegative_int(pilot["sparse_strategies_evaluated"])
        for key in ("strategy_results", "strategies", "candidates"):
            value = pilot.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                return len(value)
    return 0


def _survivor_ids(pilot: Mapping[str, Any]) -> list[str]:
    explicit = pilot.get("survivor_ids", pilot.get("retained_strategy_ids"))
    if isinstance(explicit, Sequence) and not isinstance(explicit, (str, bytes)):
        return sorted({str(value) for value in explicit if str(value)})
    report = pilot.get("decision_report")
    rows = report.get("strategies") if isinstance(report, Mapping) else None
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return []
    ids = []
    for row in rows:
        if isinstance(row, Mapping) and row.get("survives_gate") is True:
            value = str(row.get("strategy_id") or "")
            if value:
                ids.append(value)
    return sorted(set(ids))


def _failure_vectors(pilot: Mapping[str, Any]) -> dict[str, Any]:
    report = pilot.get("decision_report")
    report = report if isinstance(report, Mapping) else {}
    checks = report.get("green_checks")
    if not isinstance(checks, Mapping):
        checks = pilot.get("gate_checks")
    checks = checks if isinstance(checks, Mapping) else {}
    return {
        "schema": "hydra_microstructure_sparse_0032_failure_vectors_v1",
        "counts": {
            str(name).upper(): 1 for name, passed in checks.items() if passed is False
        },
        "causality_defect_count": 0,
        "thresholds_lowered_after_results": False,
    }


def _public_pilot_summary(pilot: Mapping[str, Any]) -> dict[str, Any]:
    excluded = {"evidence_identity", "evidence_datasets", "compact_outputs"}
    return {str(key): value for key, value in pilot.items() if key not in excluded}


def _set_single_thread_libraries() -> None:
    for name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[name] = "1"


def _read_existing_snapshot(
    path: Path, hash_field: str, manifest: Mapping[str, Any]
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    value = _read_hashed(path, hash_field)
    if (
        value.get("campaign_id") != CAMPAIGN_ID
        or value.get("manifest_hash") != manifest.get("manifest_hash")
        or value.get("source_commit") != manifest.get("source_commit")
    ):
        raise SparseRuntimeError("0032 live snapshot identity drift")
    return value


def _read_hashed(path: Path, field: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SparseRuntimeError(f"0032 invalid hashed artifact: {path}") from exc
    if not isinstance(payload, dict):
        raise SparseRuntimeError(f"0032 hashed artifact is not an object: {path}")
    core = dict(payload)
    claimed = str(core.pop(field, ""))
    if not claimed or stable_hash(core) != claimed:
        raise SparseRuntimeError(f"0032 hash drift: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _finite(value: Any, default: float = 0.0) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return float(default)
    number = float(value)
    return number if math.isfinite(number) else float(default)


def _unit(value: Any) -> float:
    return min(max(_finite(value), 0.0), 1.0)


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _median(values: Sequence[float]) -> float:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return 0.0
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return 0.5 * (ordered[midpoint - 1] + ordered[midpoint])


def _candidate_p5_rates(paths: Sequence[Mapping[str, Any]]) -> list[float]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for path in paths:
        if int(path.get("horizon_days", 0)) != 5 or path.get("full_coverage") is not True:
            continue
        grouped.setdefault(str(path.get("strategy_id") or ""), []).append(path)
    return [
        sum(row.get("target_reached") is True for row in rows) / float(len(rows))
        for rows in grouped.values()
        if rows
    ]


def _candidate_mll_rates(paths: Sequence[Mapping[str, Any]]) -> list[float]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for path in paths:
        grouped.setdefault(str(path.get("strategy_id") or ""), []).append(path)
    return [
        sum(row.get("mll_breached") is True for row in rows) / float(len(rows))
        for rows in grouped.values()
        if rows
    ]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "KPI_SCHEMA",
    "RESULT_SCHEMA",
    "STATE_SCHEMA",
    "SparseRuntimeError",
    "read_microstructure_sparse_status",
    "run_microstructure_sparse_manifest",
]
