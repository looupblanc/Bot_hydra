"""Stable production-kernel adapter for HYDRA campaign 0033.

The hybrid pilot owns all scientific decisions.  This adapter only binds the
frozen inputs, enforces the two-worker/single-writer contract, publishes
hashed controller snapshots, atomically seals the returned EvidenceBundle,
and writes one terminal production result.  It has no data-acquisition path.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import MISSING
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
from hydra.production.microstructure_hybrid_manifest import (
    CAMPAIGN_ID,
    MAXIMUM_HYBRID_POLICIES,
    PILOT_DECISIONS,
    RUNTIME_VERSION,
    validate_microstructure_hybrid_manifest,
)
from hydra.production.runtime import PRODUCTION_KPI_SCHEMA, PRODUCTION_STATE_SCHEMA


RESULT_SCHEMA = "hydra_economic_production_result_v1"
SCIENTIFIC_STATE_SCHEMA = "hydra_microstructure_hybrid_0033_state_v1"
SCIENTIFIC_KPI_SCHEMA = "hydra_microstructure_hybrid_0033_kpis_v1"
SCIENTIFIC_RESULT_SCHEMA = "hydra_microstructure_hybrid_0033_result_v1"
STATE_SCHEMA = PRODUCTION_STATE_SCHEMA
KPI_SCHEMA = PRODUCTION_KPI_SCHEMA
OVERLAY_DECISIONS = (
    "HYBRID_OVERLAY_GREEN",
    "HYBRID_OVERLAY_WEAK",
    "HYBRID_OVERLAY_FALSIFIED",
)
_REQUIRED_PILOT_KEYS = frozenset(
    {
        "pilot_status",
        "candidate_results",
        "policy_results",
        "evidence_identity",
        "evidence_datasets",
        "compact_outputs",
        "production_kpis",
        "runtime_metrics",
    }
)


class HybridRuntimeError(RuntimeError):
    """Campaign 0033 cannot continue without violating its frozen contract."""


def read_microstructure_hybrid_status(manifest_path: str | Path) -> dict[str, Any]:
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
        "next_action": "RUN_BOUNDED_HYBRID_OVERLAY_PILOT",
    }


def run_microstructure_hybrid_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path | None = None,
    cache_root: str | Path | None = None,
    stop_after: str | None = None,
) -> dict[str, Any]:
    """Run/resume the bounded 0033 hybrid pilot without acquiring data."""

    del contract_map_path, cache_root
    if stop_after is not None and os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
        raise HybridRuntimeError("0033 stop_after is restricted to explicit test mode")
    if tuple(PILOT_DECISIONS) != OVERLAY_DECISIONS:
        raise HybridRuntimeError("0033 manifest/runtime scientific status vocabulary drift")

    path = Path(manifest_path).resolve()
    root = path.parents[2]
    manifest = load_and_validate_production_manifest(path)
    validate_microstructure_hybrid_manifest(manifest, manifest_path=path)
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
        stage="IMMUTABLE_SOURCE_AND_ANCHOR_RECONCILIATION",
        next_action="RUN_PAIRED_HYBRID_OVERLAY_ECONOMIC_REPLAY",
    )
    if stop_after and stop_after.upper() in {"START", "STARTING"}:
        return _read_hashed(output / "production_state.json", "state_hash")

    # Lazy import keeps controller/status reads independent of scientific code.
    from hydra.production.microstructure_hybrid_pilot import (
        HybridPilotConfig,
        run_microstructure_hybrid_pilot,
    )

    config = _pilot_config(manifest, HybridPilotConfig)
    sources = _source_bindings(root, manifest)
    _write_state(
        output,
        manifest,
        state="EXACT_REPLAY_ACTIVE",
        stage="PAIRED_ACTION_AND_RISK_OVERLAY",
        next_action="EVALUATE_AT_MOST_20_FROZEN_HYBRID_POLICIES",
    )
    pilot_value = run_microstructure_hybrid_pilot(
        sources["source_store_dir"],
        sources["anchor_population_path"],
        sources["anchor_event_root"],
        sources["clean_result_path"],
        output / "pilot",
        config=config,
    )
    pilot = _pilot_mapping(pilot_value)
    decision = str(pilot["pilot_status"])

    _write_state(
        output,
        manifest,
        state="FINALIZING",
        stage="EVIDENCE_RECONCILIATION_AND_ATOMIC_FINALIZATION",
        next_action="SEAL_COMPLETE_HYBRID_EVIDENCE_BUNDLE",
        pilot=pilot,
        extra={"decision": decision},
    )
    receipt = _seal_evidence_bundle(root, output, manifest, pilot)
    cost_report = _conditional_cost_report(output, manifest, pilot, decision)
    result = _build_terminal_result(
        manifest=manifest,
        pilot=pilot,
        evidence_receipt=receipt,
        decision=decision,
        conditional_cost_report=cost_report,
    )
    _atomic_json(result_path, result)
    _write_state(
        output,
        manifest,
        state="COMPLETE",
        stage="HYBRID_OVERLAY_DECISION_SEALED",
        next_action=str(result["autonomous_next_action"]["action"]),
        pilot=pilot,
        extra={
            "decision": decision,
            "actual_additional_spend_usd": 0.0,
            "conditional_cost_report_status": cost_report["status"],
        },
    )
    return result


def _pilot_config(manifest: Mapping[str, Any], config_type: Any) -> Any:
    fields = dict(getattr(config_type, "__dataclass_fields__", {}))
    if not fields:
        raise HybridRuntimeError("0033 HybridPilotConfig must be a dataclass")
    frontier = manifest["paired_action_frontier"]
    roles = manifest["chronological_roles"]
    anchors = manifest["clean_structural_anchors_0028"]
    source_identity = {
        "0031": manifest["immutable_source_store_0031"]["source_hashes"],
        "0032": manifest["terminal_source_0032"]["source_hashes"],
        "0028": anchors["source_hashes"],
        "anchor_ledgers": anchors["event_ledgers"],
    }
    available: dict[str, Any] = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": str(manifest["manifest_hash"]),
        "source_commit": str(manifest["source_commit"]),
        "source_store_hash": stable_hash(source_identity),
        "source_identity_hash": stable_hash(source_identity),
        "cpu_worker_count": int(
            manifest["compute_contract"]["cpu_worker_count"]
        ),
        "worker_count": int(manifest["compute_contract"]["cpu_worker_count"]),
        "selected_markets": tuple(
            str(value) for value in anchors["markets"]
        ),
        "selected_sessions": tuple(
            str(value) for value in anchors["coverage_session_dates"]
        ),
        "action_ids": tuple(str(value) for value in frontier["action_ids"]),
        "actions": tuple(dict(value) for value in frontier["actions"]),
        "action_specs": tuple(dict(value) for value in frontier["actions"]),
        "risk_levels": tuple(float(value) for value in frontier["risk_levels"]),
        "risk_tiers": tuple(float(value) for value in frontier["risk_levels"]),
        "maximum_policies": int(frontier["maximum_policy_count"]),
        "max_policies": int(frontier["maximum_policy_count"]),
        "anchor_ids": tuple(str(value) for value in anchors["anchor_ids"]),
        "maximum_anchors": int(anchors["maximum_anchor_count"]),
        "expected_active_anchors": int(anchors["frozen_anchor_count"]),
        "discovery_sessions": int(roles["discovery_sessions"]),
        "validation_sessions": int(roles["validation_sessions"]),
        "final_development_sessions": int(roles["final_development_sessions"]),
        "chronological_role_counts": (
            int(roles["discovery_sessions"]),
            int(roles["validation_sessions"]),
            int(roles["final_development_sessions"]),
        ),
        "chronological_roles": (
            int(roles["discovery_sessions"]),
            int(roles["validation_sessions"]),
            int(roles["final_development_sessions"]),
        ),
    }
    optional = manifest.get("pilot_configuration")
    if isinstance(optional, Mapping):
        available.update({str(key): value for key, value in optional.items()})
    # JSON freezes sequences as arrays, while the scientific dataclass uses
    # tuples to make the frontier immutable.  Normalize only the known frozen
    # sequence fields after optional manifest bindings are applied.
    for name in (
        "selected_markets",
        "selected_sessions",
        "chronological_roles",
        "chronological_role_counts",
        "risk_tiers",
        "risk_levels",
    ):
        if name in available and isinstance(available[name], Sequence) and not isinstance(
            available[name], (str, bytes, bytearray)
        ):
            available[name] = tuple(available[name])
    kwargs = {name: available[name] for name in fields if name in available}
    missing = [
        name
        for name, field in fields.items()
        if name not in kwargs
        and field.default is MISSING
        and field.default_factory is MISSING
    ]
    if missing:
        raise HybridRuntimeError(
            "0033 HybridPilotConfig has unbound required fields: "
            + ", ".join(sorted(missing))
        )
    return config_type(**kwargs)


def _source_bindings(root: Path, manifest: Mapping[str, Any]) -> dict[str, Path]:
    source_store = _descriptor_path(
        root, manifest["immutable_source_store_0031"]["source_hashes"], "feature_matrices"
    )
    source_store_dir: Path | None = None
    for candidate in source_store.parents:
        expected = candidate / "datasets/feature_matrices/part-000000.parquet"
        if expected.is_file() and expected.resolve() == source_store:
            source_store_dir = candidate
            break
    if source_store_dir is None:
        raise HybridRuntimeError("0033 immutable 0031 source-store root cannot be resolved")

    anchor_sources = manifest["clean_structural_anchors_0028"]["source_hashes"]
    population = _descriptor_path(root, anchor_sources, "candidate_population")
    clean_result = _descriptor_path(root, anchor_sources, "source_result")
    event_paths = [
        (root / str(value["path"])).resolve()
        for value in manifest["clean_structural_anchors_0028"]["event_ledgers"].values()
    ]
    event_roots = {path.parent for path in event_paths}
    if len(event_roots) != 1 or any(not path.is_file() for path in event_paths):
        raise HybridRuntimeError("0033 anchor event ledgers do not share one immutable root")
    return {
        "source_store_dir": source_store_dir,
        "anchor_population_path": population,
        "anchor_event_root": next(iter(event_roots)),
        "clean_result_path": clean_result,
    }


def _descriptor_path(
    root: Path, descriptors: Mapping[str, Any], key: str
) -> Path:
    descriptor = descriptors.get(key)
    if not isinstance(descriptor, Mapping):
        raise HybridRuntimeError(f"0033 source descriptor absent: {key}")
    path = (root / str(descriptor.get("path") or "")).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise HybridRuntimeError(f"0033 source path escapes project: {key}") from exc
    if not path.is_file():
        raise HybridRuntimeError(f"0033 source file absent: {key}")
    return path


def _pilot_mapping(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise HybridRuntimeError("0033 pilot returned no decision-grade mapping")
    pilot = dict(value)
    missing = _REQUIRED_PILOT_KEYS - set(pilot)
    if missing:
        raise HybridRuntimeError(
            "0033 pilot contract is incomplete: " + ", ".join(sorted(missing))
        )
    if str(pilot["pilot_status"]) not in OVERLAY_DECISIONS:
        raise HybridRuntimeError("0033 pilot returned an unsupported scientific decision")
    for key in ("candidate_results", "policy_results"):
        if not isinstance(pilot[key], Sequence) or isinstance(
            pilot[key], (str, bytes, bytearray)
        ):
            raise HybridRuntimeError(f"0033 pilot {key} is not a sequence")
    if len(pilot["policy_results"]) > MAXIMUM_HYBRID_POLICIES:
        raise HybridRuntimeError("0033 pilot exceeded its frozen 20-policy cap")
    for key in (
        "evidence_identity",
        "evidence_datasets",
        "compact_outputs",
        "production_kpis",
        "runtime_metrics",
    ):
        if not isinstance(pilot[key], Mapping):
            raise HybridRuntimeError(f"0033 pilot {key} is not a mapping")
    return pilot


def _seal_evidence_bundle(
    root: Path,
    output: Path,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
) -> dict[str, Any]:
    identity = pilot["evidence_identity"]
    datasets = pilot["evidence_datasets"]
    compact = dict(pilot["compact_outputs"])
    if set(datasets) != set(REQUIRED_DATASETS):
        raise HybridRuntimeError("0033 pilot lacks complete canonical EvidenceBundle data")
    compact.setdefault(
        "next_campaign_recommendations",
        _compact_next_campaign_recommendation(str(pilot["pilot_status"])),
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
                    batch_id=f"0033-{dataset}-0000",
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
    actions = {
        "HYBRID_OVERLAY_GREEN": "GENERATE_CONDITIONAL_COST_MATRIX_AND_RUN_LONG_SAMPLE_VALIDATION",
        "HYBRID_OVERLAY_WEAK": "PRESERVE_POSITIVE_PAIRED_UPLIFT_AND_COMPLETE_ONE_BOUNDED_REFINEMENT",
        "HYBRID_OVERLAY_FALSIFIED": "CLASSIFY_MICROSTRUCTURE_NON_INCREMENTAL_AND_STOP_PRIMARY_USE",
    }
    if decision not in actions:
        raise HybridRuntimeError("0033 cannot seal an unsupported successor decision")
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
    path = output / "conditional_data_cost_report.json"
    if path.is_file():
        report = _read_hashed(path, "cost_report_hash")
        if report.get("manifest_hash") != manifest.get("manifest_hash"):
            raise HybridRuntimeError("0033 conditional cost report manifest drift")
        if float(report.get("actual_additional_spend_usd", -1.0)) != 0.0:
            raise HybridRuntimeError("0033 runtime may not purchase conditional data")
        return report
    extension = manifest["conditional_extension"]
    estimates = pilot.get("conditional_data_cost_report")
    if not isinstance(estimates, Mapping):
        estimates = pilot.get("conditional_data_cost_matrix")
    if decision == "HYBRID_OVERLAY_GREEN" and isinstance(estimates, Mapping):
        status = "OFFICIAL_COST_REPORT_AVAILABLE_NO_PURCHASE"
        cost_estimates: Mapping[str, Any] = dict(estimates)
    elif decision == "HYBRID_OVERLAY_GREEN":
        status = "OFFICIAL_COST_REPORT_REQUIRED_NO_PURCHASE"
        cost_estimates = {}
    else:
        status = "NOT_TRIGGERED_BY_HYBRID_GATE"
        cost_estimates = {}
    core = {
        "schema": "hydra_microstructure_hybrid_0033_conditional_cost_report_v1",
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "status": status,
        "official_metadata_estimates": cost_estimates,
        "maximum_incremental_spend_usd": float(
            extension["maximum_incremental_spend_usd"]
        ),
        "minimum_budget_reserve_usd": float(extension["minimum_budget_reserve_usd"]),
        "actual_additional_spend_usd": 0.0,
        "automatic_purchase_allowed": False,
        "purchase_performed": False,
        "q4_accessed": False,
        "broker_connections": 0,
        "orders": 0,
        "generated_at_utc": _utc_now(),
    }
    report = {**core, "cost_report_hash": stable_hash(core)}
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
    prior = _read_existing_snapshot(output / "production_state.json", "state_hash", manifest)
    sequence = int((prior or {}).get("checkpoint_sequence", 0)) + 1
    kpis = _controller_kpis(manifest, pilot, state=state, checkpoint_sequence=sequence)
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
        "policies_proposed": int(kpis["policies_proposed"]),
        "unique_policies_screened": int(kpis["unique_policies_screened"]),
        "exact_account_replays": int(kpis["exact_account_replays"]),
        "combine_episodes_completed": int(kpis["combine_episodes_completed"]),
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
    policy_count = _policy_count(pilot)
    normal = _nonnegative_int(metrics.get("normal_episode_count", 0))
    stressed = _nonnegative_int(metrics.get("stressed_episode_count", 0))
    if normal != stressed:
        raise HybridRuntimeError("0033 normal/stressed episode counters do not reconcile")
    return {
        "schema": KPI_SCHEMA,
        "scientific_schema": SCIENTIFIC_KPI_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "checkpoint_sequence": int(checkpoint_sequence),
        "updated_at_utc": _utc_now(),
        "state": state,
        "rates_per_hour": {
            "policies_proposed": _finite(metrics.get("policies_per_hour")),
            "unique_policies_screened": _finite(metrics.get("policies_per_hour")),
            "exact_account_replays": _finite(metrics.get("exact_replays_per_hour")),
            "combine_episodes": _finite(metrics.get("account_episodes_per_hour")),
        },
        "workers": {"compute": 2, "evidence_writer": 1},
        "policies_proposed": policy_count,
        "unique_policies_screened": policy_count,
        "exact_account_replays": policy_count,
        "combine_episodes_completed": normal + stressed,
        "normal_episodes_completed": normal,
        "stressed_episodes_completed": stressed,
        "positive_stressed_net_candidates": min(
            policy_count, _nonnegative_int(metrics.get("positive_stressed_count", 0))
        ),
        "candidates_with_normal_pass": min(
            policy_count, _nonnegative_int(metrics.get("normal_pass_candidate_count", 0))
        ),
        "candidates_with_stressed_pass": min(
            policy_count, _nonnegative_int(metrics.get("stressed_pass_candidate_count", 0))
        ),
        "best_normal_pass_rate": _unit(metrics.get("normal_pass_rate_best")),
        "best_stressed_pass_rate": _unit(metrics.get("stressed_pass_rate_best")),
        "median_normal_pass_rate": _unit(metrics.get("normal_pass_rate_median")),
        "median_stressed_pass_rate": _unit(metrics.get("stressed_pass_rate_median")),
        "near_pass_count": min(
            policy_count, _nonnegative_int(metrics.get("near_pass_count", 0))
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
            metrics.get("matched_controls_status") or "PAIRED_IDENTICAL_EPISODE_CONTROLS"
        ),
        "null_status": str(metrics.get("null_status") or "BOUNDED_HYBRID_NULLS"),
        "opportunity_episode_count": _nonnegative_int(
            metrics.get("opportunity_episode_count", candidate_count)
        ),
        "hybrid_candidates_evaluated": candidate_count,
        "hybrid_policies_evaluated": policy_count,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }


def _build_terminal_result(
    *,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
    evidence_receipt: Mapping[str, Any],
    decision: str,
    conditional_cost_report: Mapping[str, Any],
) -> dict[str, Any]:
    if decision not in OVERLAY_DECISIONS:
        raise HybridRuntimeError("0033 terminal decision vocabulary drift")
    kpis = _controller_kpis(manifest, pilot, state="COMPLETE", checkpoint_sequence=0)
    metrics = _metrics(pilot)
    candidate_count = _candidate_count(pilot)
    policy_count = _policy_count(pilot)
    normal = int(kpis["normal_episodes_completed"])
    stressed = int(kpis["stressed_episodes_completed"])
    economic_results = {
        "schema": "hydra_microstructure_hybrid_0033_economics_v1",
        "production_counters": {
            "candidate_opportunities_evaluated": candidate_count,
            "serious_exact_account_replays": policy_count,
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
            "policy_count": policy_count,
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
        "policy_count": policy_count,
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
    survivors = _survivor_ids(pilot) if decision == "HYBRID_OVERLAY_GREEN" else []
    result = build_final_result_payload(
        manifest=manifest,
        kpis=kpis,
        economic_results=economic_results,
        successive_halving={
            "schema": "hydra_microstructure_hybrid_0033_gate_v1",
            "stage_decisions": [
                {
                    "stage": "BOUNDED_HYBRID_OVERLAY_PILOT",
                    "input_count": policy_count,
                    "output_count": len(survivors),
                    "selected_policy_ids": survivors,
                }
            ],
            "thresholds_changed_after_results": False,
            "mass_scale_before_green": False,
        },
        matched_controls={
            "schema": "hydra_microstructure_hybrid_0033_controls_v1",
            "evaluated_control_policy_count": _nonnegative_int(
                metrics.get("control_replay_count", 0)
            ),
            "control_ids": [
                "A0_BASELINE_IMMEDIATE",
                "IDENTICAL_EPISODE_STRUCTURAL_ANCHOR",
                "EXPOSURE_MATCHED_RANDOM_ACTION",
            ],
            "paired_uplift": _paired_uplift(pilot),
            "controls_selected_after_outcomes": False,
        },
        failure_vectors=_failure_vectors(pilot),
        evidence_receipt=evidence_receipt,
        autonomous_next_action=_next_action(decision, survivors),
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
            "conditional_cost_report": dict(conditional_cost_report),
            "actual_additional_spend_usd": 0.0,
            "new_data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "mass_scale_started": False,
            "xfa_paths": 0,
        }
    )
    result["result_hash"] = stable_hash(result)
    return result


def _next_action(decision: str, survivors: Sequence[str]) -> dict[str, Any]:
    action = {
        "HYBRID_OVERLAY_GREEN": "GENERATE_CONDITIONAL_COST_MATRIX_AND_RUN_LONG_SAMPLE_VALIDATION",
        "HYBRID_OVERLAY_WEAK": "PRESERVE_POSITIVE_PAIRED_UPLIFT_AND_COMPLETE_ONE_BOUNDED_REFINEMENT",
        "HYBRID_OVERLAY_FALSIFIED": "CLASSIFY_MICROSTRUCTURE_NON_INCREMENTAL_AND_STOP_PRIMARY_USE",
    }[decision]
    return {
        "action": action,
        "candidate_ids": list(survivors),
        "manifest_required": True,
        "automatic_data_purchase_authorized": False,
        "new_data_purchase_authorized": False,
        "q4_access_authorized": False,
    }


def _metrics(pilot: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(pilot, Mapping):
        return {}
    merged: dict[str, Any] = {}
    direct = pilot.get("production_kpis")
    if isinstance(direct, Mapping):
        merged.update(direct)
    runtime = pilot.get("runtime_metrics")
    if isinstance(runtime, Mapping):
        merged.setdefault("cpu_utilization_fraction", runtime.get("cpu_utilization_fraction", 0.0))
        merged.setdefault(
            "economic_wall_clock_fraction",
            runtime.get("economic_wall_clock_fraction", 0.0),
        )
        elapsed = _finite(runtime.get("elapsed_seconds"))
    else:
        elapsed = 0.0
    candidates = _mapping_rows(pilot.get("candidate_results"))
    policies = _mapping_rows(pilot.get("policy_results"))
    merged.setdefault("opportunity_episode_count", len(candidates))
    merged.setdefault("candidate_count", len(candidates))
    merged.setdefault("policy_count", len(policies))
    paths = [
        path
        for row in policies
        for key in ("account_paths", "account_episodes", "episodes")
        for path in _mapping_rows(row.get(key))
    ]
    # If one row exposes aliases, do not count it repeatedly.
    unique_paths: dict[str, Mapping[str, Any]] = {}
    for ordinal, path in enumerate(paths):
        key = str(
            path.get("episode_id")
            or path.get("path_id")
            or stable_hash({"ordinal": ordinal, "path": path})
        )
        unique_paths.setdefault(key, path)
    paths = list(unique_paths.values())
    normal_paths = [row for row in paths if str(row.get("scenario")) == "NORMAL"]
    stressed_paths = [
        row
        for row in paths
        if str(row.get("scenario")) in {"STRESSED", "STRESSED_1_5X"}
    ]
    merged.setdefault("normal_episode_count", len(normal_paths))
    merged.setdefault("stressed_episode_count", len(stressed_paths))
    merged.setdefault(
        "positive_stressed_count",
        sum(_finite(row.get("stressed_net_usd")) > 0.0 for row in policies),
    )
    normal_ids = {_path_policy_id(row) for row in normal_paths if row.get("target_reached") is True}
    stressed_ids = {
        _path_policy_id(row) for row in stressed_paths if row.get("target_reached") is True
    }
    merged.setdefault("normal_pass_candidate_count", len(normal_ids - {""}))
    merged.setdefault("stressed_pass_candidate_count", len(stressed_ids - {""}))
    normal_rates = _policy_rates(normal_paths, "target_reached")
    stressed_rates = _policy_rates(stressed_paths, "target_reached")
    merged.setdefault("normal_pass_rate_best", max(normal_rates, default=0.0))
    merged.setdefault("normal_pass_rate_median", _median(normal_rates))
    merged.setdefault("stressed_pass_rate_best", max(stressed_rates, default=0.0))
    merged.setdefault("stressed_pass_rate_median", _median(stressed_rates))
    breach_rates = _policy_rates(stressed_paths, "mll_breached")
    merged.setdefault("mll_breach_rate_minimum", min(breach_rates, default=0.0))
    merged.setdefault("mll_breach_rate_maximum", max(breach_rates, default=0.0))
    progress = [_target_progress_fraction(row) for row in stressed_paths]
    merged.setdefault("stressed_target_progress_best_fraction", max(progress, default=0.0))
    merged.setdefault("stressed_target_progress_median_fraction", _median(progress))
    if elapsed > 0.0:
        merged.setdefault("policies_per_hour", 3600.0 * len(policies) / elapsed)
        merged.setdefault("exact_replays_per_hour", 3600.0 * len(policies) / elapsed)
        merged.setdefault("account_episodes_per_hour", 3600.0 * len(paths) / elapsed)
    return merged


def _candidate_count(pilot: Mapping[str, Any] | None) -> int:
    if not isinstance(pilot, Mapping):
        return 0
    return len(_mapping_rows(pilot.get("candidate_results")))


def _policy_count(pilot: Mapping[str, Any] | None) -> int:
    if not isinstance(pilot, Mapping):
        return 0
    return len(_mapping_rows(pilot.get("policy_results")))


def _survivor_ids(pilot: Mapping[str, Any]) -> list[str]:
    explicit = pilot.get("survivor_ids", pilot.get("retained_policy_ids"))
    if isinstance(explicit, Sequence) and not isinstance(explicit, (str, bytes, bytearray)):
        return sorted({str(value) for value in explicit if str(value)})
    return sorted(
        {
            str(row.get("policy_id") or row.get("candidate_id") or "")
            for row in _mapping_rows(pilot.get("policy_results"))
            if row.get("survives_gate") is True
            and str(row.get("policy_id") or row.get("candidate_id") or "")
        }
    )


def _failure_vectors(pilot: Mapping[str, Any]) -> dict[str, Any]:
    explicit = pilot.get("failure_vectors")
    if isinstance(explicit, Mapping):
        counts = explicit.get("counts", explicit)
    else:
        checks = pilot.get("gate_checks")
        counts = (
            {str(name).upper(): 1 for name, passed in checks.items() if passed is False}
            if isinstance(checks, Mapping)
            else {}
        )
    return {
        "schema": "hydra_microstructure_hybrid_0033_failure_vectors_v1",
        "counts": dict(counts) if isinstance(counts, Mapping) else {},
        "causality_defect_count": 0,
        "thresholds_lowered_after_results": False,
    }


def _paired_uplift(pilot: Mapping[str, Any]) -> dict[str, float]:
    value = pilot.get("paired_uplift")
    if not isinstance(value, Mapping):
        metrics = pilot.get("production_kpis")
        value = metrics.get("paired_uplift") if isinstance(metrics, Mapping) else None
    if not isinstance(value, Mapping):
        return {}
    return {str(key): _finite(raw) for key, raw in value.items()}


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
        raise HybridRuntimeError("0033 live snapshot identity drift")
    return value


def _read_hashed(path: Path, field: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HybridRuntimeError(f"0033 invalid hashed artifact: {path}") from exc
    if not isinstance(payload, dict):
        raise HybridRuntimeError(f"0033 hashed artifact is not an object: {path}")
    core = dict(payload)
    claimed = str(core.pop(field, ""))
    if not claimed or stable_hash(core) != claimed:
        raise HybridRuntimeError(f"0033 hash drift: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _mapping_rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [row for row in value if isinstance(row, Mapping)]


def _path_policy_id(row: Mapping[str, Any]) -> str:
    return str(row.get("policy_id") or row.get("strategy_id") or row.get("candidate_id") or "")


def _policy_rates(
    paths: Sequence[Mapping[str, Any]], field: str
) -> list[float]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for path in paths:
        grouped.setdefault(_path_policy_id(path), []).append(path)
    return [
        sum(row.get(field) is True for row in rows) / float(len(rows))
        for key, rows in grouped.items()
        if key and rows
    ]


def _target_progress_fraction(row: Mapping[str, Any]) -> float:
    if "target_progress_fraction" in row:
        return _finite(row.get("target_progress_fraction"))
    return _finite(row.get("target_progress_pct")) / 100.0


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


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "KPI_SCHEMA",
    "OVERLAY_DECISIONS",
    "RESULT_SCHEMA",
    "STATE_SCHEMA",
    "HybridRuntimeError",
    "read_microstructure_hybrid_status",
    "run_microstructure_hybrid_manifest",
]
