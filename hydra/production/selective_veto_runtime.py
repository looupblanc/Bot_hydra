"""Persistent-runtime adapter for HYDRA campaign 0034.

The adapter deliberately owns no economic model.  A lazy scientific backend
performs the seed audit, targeted cost estimation/acquisition, paired replay,
and long-sample evaluation.  This module validates the phase ordering and
economic postconditions, enforces the conditional-purchase envelope, seals one
EvidenceBundle through the authoritative process, and publishes resumable
controller snapshots.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import (
    EvidenceBundleWriter,
    RECORD_SPECS,
    REQUIRED_COMPACT_OUTPUTS,
    REQUIRED_DATASETS,
    recover_finalized_evidence_bundle,
    verify_evidence_bundle,
)
from hydra.production.halving import build_final_result_payload
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.production.runtime import PRODUCTION_KPI_SCHEMA, PRODUCTION_STATE_SCHEMA
from hydra.production.selective_veto_manifest import (
    ACCOUNT_RULE_SNAPSHOTS,
    CAMPAIGN_ID,
    LONG_SAMPLE_DECISIONS,
    MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM,
    PRIMARY_ACTIONS,
    RUNTIME_VERSION,
    SEED_DECISIONS,
    SEED_IDS,
    validate_selective_veto_manifest,
)


RESULT_SCHEMA = "hydra_economic_production_result_v1"
SCIENTIFIC_STATE_SCHEMA = "hydra_selective_order_flow_veto_0034_state_v1"
SCIENTIFIC_KPI_SCHEMA = "hydra_selective_order_flow_veto_0034_kpis_v1"
SCIENTIFIC_RESULT_SCHEMA = "hydra_selective_order_flow_veto_0034_result_v1"

Backend = Callable[..., Mapping[str, Any]]


class SelectiveVetoRuntimeError(RuntimeError):
    """Campaign 0034 cannot continue without violating its frozen contract."""


def read_selective_veto_status(manifest_path: str | Path) -> dict[str, Any]:
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
        "stage": "SEED_AUDIT_PENDING",
        "next_action": "RUN_NO_PURCHASE_SEED_ROBUSTNESS_AUDIT",
    }


def run_selective_veto_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path | None = None,
    cache_root: str | Path | None = None,
    stop_after: str | None = None,
    backend: Backend | None = None,
) -> dict[str, Any]:
    """Run/resume 0034 while enforcing seed-before-cost-before-purchase order.

    ``backend`` exists for targeted tests and deterministic integration.  The
    ordinary persistent service resolves the single scientific implementation
    lazily from :mod:`hydra.production.selective_veto_pilot`.
    """

    if stop_after is not None and os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
        raise SelectiveVetoRuntimeError(
            "0034 stop_after is restricted to explicit test mode"
        )
    path = Path(manifest_path).resolve()
    root = path.parents[2]
    manifest = load_and_validate_production_manifest(path)
    validate_selective_veto_manifest(manifest, manifest_path=path)
    output = root / str(manifest["runtime"]["output_dir"])
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / str(
        manifest["runtime"].get("result_name", "economic_production_result.json")
    )
    if result_path.is_file():
        result = _read_hashed(result_path, "result_hash")
        _verify_existing_result(result, manifest)
        return result

    _set_single_thread_libraries()
    _write_state(
        output,
        manifest,
        state="STARTING",
        stage="NO_PURCHASE_SEED_ROBUSTNESS_AUDIT",
        next_action="AUDIT_FROZEN_SEEDS_BEFORE_ANY_COST_OR_DATA_ACTION",
    )
    if stop_after and stop_after.upper() in {"START", "STARTING"}:
        return _read_hashed(output / "production_state.json", "state_hash")

    runner = backend or _default_backend()
    value = runner(
        manifest=manifest,
        project_root=root,
        output_dir=output / "pilot",
        contract_map_path=Path(contract_map_path).resolve()
        if contract_map_path is not None
        else None,
        cache_root=Path(cache_root).resolve() if cache_root is not None else None,
    )
    campaign = _campaign_mapping(value)
    _write_state(
        output,
        manifest,
        state="FINALIZING",
        stage="PHASE_ORDER_AND_ECONOMIC_RECONCILIATION",
        next_action="VERIFY_SEED_COST_ACQUISITION_LONG_SAMPLE_AND_FORWARD_POSTCONDITIONS",
        campaign=campaign,
    )
    scientific = _validate_campaign_result(manifest, campaign)
    receipt = _seal_evidence_bundle(root, output, manifest, campaign, scientific)
    result = _build_terminal_result(manifest, campaign, scientific, receipt)
    _atomic_json(result_path, result)
    _write_state(
        output,
        manifest,
        state="COMPLETE",
        stage="SELECTIVE_VETO_DECISION_SEALED",
        next_action=str(result["autonomous_next_action"]["action"]),
        campaign=campaign,
        extra={
            "seed_decision": scientific["seed_decision"],
            "decision": scientific["decision"],
            "actual_additional_spend_usd": scientific["actual_spend_usd"],
            "diagnostic_forward_status": scientific["diagnostic_forward_status"],
        },
    )
    return result


def _default_backend() -> Backend:
    try:
        from hydra.production.selective_veto_pilot import (
            run_selective_veto_campaign,
        )
    except ImportError as exc:
        raise SelectiveVetoRuntimeError(
            "0034 scientific backend is not installed; seed audit remains fail-closed"
        ) from exc
    return run_selective_veto_campaign


def _campaign_mapping(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise SelectiveVetoRuntimeError("0034 backend returned no campaign mapping")
    required = {
        "seed_audit",
        "anchor_universe",
        "window_cost_matrix",
        "acquisition",
        "long_sample",
        "diagnostic_forward",
        "evidence_identity",
        "evidence_datasets",
        "compact_outputs",
        "production_kpis",
        "runtime_metrics",
    }
    missing = required - set(value)
    if missing:
        raise SelectiveVetoRuntimeError(
            "0034 backend contract is incomplete: " + ", ".join(sorted(missing))
        )
    result = dict(value)
    for key in required:
        if not isinstance(result[key], Mapping):
            raise SelectiveVetoRuntimeError(f"0034 backend {key} is not a mapping")
    return result


def _validate_campaign_result(
    manifest: Mapping[str, Any], campaign: Mapping[str, Any]
) -> dict[str, Any]:
    seed = campaign["seed_audit"]
    seed_decision = str(seed.get("decision") or "")
    if seed_decision not in SEED_DECISIONS:
        raise SelectiveVetoRuntimeError("0034 seed audit decision is unsupported")
    if (
        seed.get("completed_before_cost_estimation") is not True
        or seed.get("completed_before_purchase") is not True
        or not _zero(seed.get("actual_spend_usd"))
    ):
        raise SelectiveVetoRuntimeError("0034 no-purchase seed gate ordering failed")
    policies = seed.get("policies")
    if not isinstance(policies, Mapping) or set(str(key) for key in policies) != set(SEED_IDS):
        raise SelectiveVetoRuntimeError("0034 seed audit policy denominator drift")
    for policy_id in SEED_IDS:
        row = policies[policy_id]
        if not isinstance(row, Mapping):
            raise SelectiveVetoRuntimeError("0034 seed audit policy result absent")
        required = {
            "leave_one_opportunity_out",
            "top_trade_removal",
            "leave_one_anchor_family_out",
            "cost_stress",
            "feature_dependencies",
            "account_size_matrix",
            "market_attribution",
            "anchor_family_attribution",
        }
        if not required <= set(row):
            raise SelectiveVetoRuntimeError(
                f"0034 incomplete robustness audit for {policy_id}"
            )

    acquisition = campaign["acquisition"]
    purchase_performed = acquisition.get("purchase_performed") is True
    actual_spend = _finite(acquisition.get("actual_spend_usd"))
    manifest_bound_purchase_count = _integer(
        acquisition.get("manifest_bound_data_purchase_count")
    )
    unmanifested_purchase_count = _integer(
        acquisition.get("unmanifested_data_purchase_count")
    )
    prior_budget = _finite(acquisition.get("prior_budget_usd"))
    remaining_budget = _finite(acquisition.get("remaining_budget_usd"))
    policy = manifest["targeted_cost_policy"]
    if (
        actual_spend < 0.0
        or actual_spend > float(policy["maximum_incremental_spend_usd"]) + 1e-9
        or not math.isclose(
            prior_budget,
            float(policy["current_remaining_budget_usd"]),
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        or not math.isclose(
            remaining_budget, prior_budget - actual_spend, rel_tol=0.0, abs_tol=1e-6
        )
        or remaining_budget < float(policy["minimum_budget_reserve_usd"]) - 1e-9
        or acquisition.get("q4_accessed") is not False
        or _integer(acquisition.get("broker_connections")) != 0
        or _integer(acquisition.get("orders")) != 0
        or manifest_bound_purchase_count != int(purchase_performed)
        or unmanifested_purchase_count != 0
    ):
        raise SelectiveVetoRuntimeError("0034 acquisition budget or safety invariant failed")
    if purchase_performed != (actual_spend > 0.0):
        raise SelectiveVetoRuntimeError("0034 purchase receipt/spend mismatch")
    _validate_manifest_bound_purchase(acquisition, purchase_performed=purchase_performed)

    cost = campaign["window_cost_matrix"]
    anchor = campaign["anchor_universe"]
    long_sample = campaign["long_sample"]
    decision = str(long_sample.get("decision") or "")
    if decision not in LONG_SAMPLE_DECISIONS:
        raise SelectiveVetoRuntimeError("0034 long-sample decision is unsupported")
    if seed_decision == "SELECTIVE_VETO_SEED_FALSIFIED":
        if (
            cost.get("status") != "NOT_RUN_SEED_FALSIFIED"
            or purchase_performed
            or _integer(anchor.get("anchors_generated")) != 0
            or long_sample.get("status") != "NOT_RUN_SEED_FALSIFIED"
            or decision != "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED"
        ):
            raise SelectiveVetoRuntimeError(
                "0034 falsified seed gate did not stop cost/acquisition/replay"
            )
    else:
        _validate_cost_matrix(manifest, cost, anchor)
        if purchase_performed:
            selected = cost.get("selected_offer")
            if not isinstance(selected, Mapping):
                raise SelectiveVetoRuntimeError("0034 purchased without selected cost offer")
            estimate = _finite(selected.get("estimated_cost_usd"))
            if estimate > float(policy["maximum_incremental_spend_usd"]) + 1e-9:
                raise SelectiveVetoRuntimeError("0034 selected cost exceeds purchase cap")
            if acquisition.get("official_estimate_fingerprint") != selected.get(
                "estimate_fingerprint"
            ):
                raise SelectiveVetoRuntimeError("0034 acquisition/cost fingerprint drift")
            if _integer(acquisition.get("independent_anchors_acquired")) <= 0:
                raise SelectiveVetoRuntimeError("0034 purchase acquired no anchor evidence")
            if (
                acquisition.get("raw_data_immutable") is not True
                or acquisition.get("temporal_roles_frozen_before_download") is not True
                or acquisition.get("data_access_ledger_appended") is not True
                or acquisition.get("budget_ledger_appended") is not True
                or not str(acquisition.get("acquisition_receipt_fingerprint") or "")
            ):
                raise SelectiveVetoRuntimeError(
                    "0034 acquisition provenance or ledger receipt is incomplete"
                )
            if long_sample.get("status") != "COMPLETE":
                raise SelectiveVetoRuntimeError(
                    "0034 purchased sample did not reach long-sample evaluation"
                )
        elif (
            long_sample.get("status") != "NOT_STARTED_NO_AFFORDABLE_SAMPLE"
            or decision != "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK"
        ):
            raise SelectiveVetoRuntimeError(
                "0034 no-purchase path may not claim long-sample evidence"
            )
        if long_sample.get("policy_frozen_before_final_development") is not True:
            if purchase_performed:
                raise SelectiveVetoRuntimeError(
                    "0034 policy was not frozen before final evidence"
                )

    _validate_long_sample(manifest, long_sample, purchase_performed=purchase_performed)

    forward = campaign["diagnostic_forward"]
    forward_status = str(forward.get("status") or "")
    allowed_forward = {
        "SELECTIVE_VETO_DIAGNOSTIC_FORWARD",
        "NOT_STARTED_NO_AUTHORIZED_RESEARCH_FEED",
        "NOT_STARTED_SEED_FALSIFIED",
    }
    if (
        forward_status not in allowed_forward
        or _integer(forward.get("broker_connections")) != 0
        or _integer(forward.get("orders")) != 0
        or _integer(forward.get("parameter_changes")) != 0
        or forward.get("economic_promotion_allowed") is not False
        or forward.get("paper_shadow_ready") is not False
    ):
        raise SelectiveVetoRuntimeError("0034 diagnostic-forward invariant failed")
    if forward_status == "SELECTIVE_VETO_DIAGNOSTIC_FORWARD" and (
        forward.get("authorized_research_feed") is not True
        or set(str(value) for value in forward.get("policy_ids") or ()) != set(SEED_IDS)
        or forward.get("append_only") is not True
        or forward.get("zero_order") is not True
    ):
        raise SelectiveVetoRuntimeError("0034 active diagnostic-forward receipt drift")
    _validate_forward_evidence(
        manifest,
        forward,
        seed_decision=seed_decision,
    )

    identity = campaign["evidence_identity"]
    if (
        identity.get("campaign_id") != CAMPAIGN_ID
        or identity.get("manifest_hash") != manifest.get("manifest_hash")
        or identity.get("source_commit") != manifest.get("source_commit")
    ):
        raise SelectiveVetoRuntimeError("0034 evidence identity drift")
    _validate_evidence_material(campaign, long_sample=long_sample)
    return {
        "seed_decision": seed_decision,
        "decision": decision,
        "actual_spend_usd": actual_spend,
        "remaining_budget_usd": remaining_budget,
        "purchase_performed": purchase_performed,
        "manifest_bound_data_purchase_count": manifest_bound_purchase_count,
        "unmanifested_data_purchase_count": unmanifested_purchase_count,
        "diagnostic_forward_status": forward_status,
    }


def _validate_manifest_bound_purchase(
    acquisition: Mapping[str, Any], *, purchase_performed: bool
) -> None:
    """Keep permitted manifest acquisition separate from forbidden ad-hoc access."""

    if not purchase_performed:
        return
    required_hashes = (
        "budget_ledger_before_sha256",
        "budget_ledger_after_sha256",
        "data_access_ledger_before_sha256",
        "data_access_ledger_after_sha256",
    )
    values = {name: str(acquisition.get(name) or "") for name in required_hashes}
    if (
        any(not _is_sha256(value) for value in values.values())
        or values["budget_ledger_before_sha256"]
        == values["budget_ledger_after_sha256"]
        or values["data_access_ledger_before_sha256"]
        == values["data_access_ledger_after_sha256"]
        or not str(acquisition.get("request_id") or "")
        or not _is_sha256(str(acquisition.get("acquisition_receipt_fingerprint") or ""))
    ):
        raise SelectiveVetoRuntimeError(
            "0034 manifest-bound purchase ledger reconciliation failed"
        )


def _validate_long_sample(
    manifest: Mapping[str, Any],
    long_sample: Mapping[str, Any],
    *,
    purchase_performed: bool,
) -> None:
    status = str(long_sample.get("status") or "")
    decision = str(long_sample.get("decision") or "")
    if not purchase_performed:
        if long_sample.get("paired_results"):
            raise SelectiveVetoRuntimeError(
                "0034 no-purchase result contains purported long-sample outcomes"
            )
        return
    if status != "COMPLETE" or long_sample.get(
        "policy_frozen_before_final_development"
    ) is not True:
        raise SelectiveVetoRuntimeError("0034 long-sample evidence is not frozen/complete")

    policy = long_sample.get("policy")
    if not isinstance(policy, Mapping):
        raise SelectiveVetoRuntimeError("0034 distilled policy specification is absent")
    actions = tuple(str(value) for value in policy.get("actions") or ())
    feature_names = tuple(str(value) for value in policy.get("feature_names") or ())
    if (
        actions != PRIMARY_ACTIONS
        or not feature_names
        or len(feature_names)
        > int(manifest["selective_policy_distillation"]["maximum_production_features"])
        or policy.get("direction_generation_allowed") is not False
        or policy.get("frozen_before_final_development") is not True
        or not _is_sha256(str(policy.get("policy_fingerprint") or ""))
    ):
        raise SelectiveVetoRuntimeError("0034 distilled policy contract drift")

    raw_rows = long_sample.get("paired_results")
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes)):
        raise SelectiveVetoRuntimeError("0034 paired long-sample rows are absent")
    rows = [row for row in raw_rows if isinstance(row, Mapping)]
    if len(rows) != len(raw_rows) or not rows:
        raise SelectiveVetoRuntimeError("0034 paired long-sample denominator is invalid")
    seen: set[str] = set()
    role_rows: dict[str, list[Mapping[str, Any]]] = {
        "DISCOVERY": [],
        "VALIDATION": [],
        "FINAL_DEVELOPMENT": [],
    }
    risk_for_action = {"ABSTAIN": 0.0, "TRADE_1X": 1.0, "TRADE_1_5X": 1.5}
    for row in rows:
        opportunity_id = str(row.get("anchor_event_id") or "")
        role = str(row.get("temporal_role") or "")
        action = str(row.get("action") or "")
        claimed_hash = str(row.get("paired_outcome_hash") or "")
        payload = dict(row)
        payload.pop("paired_outcome_hash", None)
        if (
            not opportunity_id
            or opportunity_id in seen
            or role not in role_rows
            or action not in risk_for_action
            or not math.isclose(
                _finite(row.get("risk_tier")),
                risk_for_action[action],
                rel_tol=0.0,
                abs_tol=1e-12,
            )
            or not _is_sha256(str(row.get("feature_hash") or ""))
            or not _is_sha256(claimed_hash)
            or stable_hash(payload) != claimed_hash
            or not str(row.get("market") or "")
            or not str(row.get("structural_family") or "")
            or not str(row.get("session_id") or "")
        ):
            raise SelectiveVetoRuntimeError("0034 paired opportunity identity/action drift")
        normal = _finite(row.get("normal_net_pnl_usd"))
        stressed = _finite(row.get("stressed_net_pnl_usd"))
        baseline_normal = _finite(row.get("baseline_normal_net_pnl_usd"))
        baseline_stressed = _finite(row.get("baseline_stressed_net_pnl_usd"))
        if (
            not math.isclose(
                _finite(row.get("paired_normal_uplift_usd")),
                normal - baseline_normal,
                rel_tol=0.0,
                abs_tol=1e-6,
            )
            or not math.isclose(
                _finite(row.get("paired_stressed_uplift_usd")),
                stressed - baseline_stressed,
                rel_tol=0.0,
                abs_tol=1e-6,
            )
            or (action == "ABSTAIN" and _integer(row.get("quantity")) != 0)
            or (action != "ABSTAIN" and _integer(row.get("quantity")) <= 0)
        ):
            raise SelectiveVetoRuntimeError("0034 paired economic arithmetic drift")
        seen.add(opportunity_id)
        role_rows[role].append(row)

    role_results = long_sample.get("role_results")
    if not isinstance(role_results, Mapping) or set(role_results) != set(role_rows):
        raise SelectiveVetoRuntimeError("0034 paired role summary is incomplete")
    for role, values in role_rows.items():
        if not values:
            raise SelectiveVetoRuntimeError("0034 chronological role has no opportunity")
        _validate_paired_role_summary(role_results[role], values, role=role)
    declared_counts = long_sample.get("role_counts")
    if not isinstance(declared_counts, Mapping) or any(
        _integer(declared_counts.get(role)) != len(values)
        for role, values in role_rows.items()
    ):
        raise SelectiveVetoRuntimeError("0034 chronological role denominator drift")

    matrix = _validate_account_size_matrix(long_sample)
    if decision == "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN":
        _validate_green_gate(manifest, long_sample, role_rows, matrix)


def _validate_paired_role_summary(
    raw: Any, rows: Sequence[Mapping[str, Any]], *, role: str
) -> None:
    if not isinstance(raw, Mapping):
        raise SelectiveVetoRuntimeError(f"0034 {role} paired summary is absent")
    executed = [row for row in rows if str(row["action"]) != "ABSTAIN"]
    expected = {
        "opportunity_count": len(rows),
        "trade_count": len(executed),
    }
    if any(_integer(raw.get(name)) != value for name, value in expected.items()):
        raise SelectiveVetoRuntimeError(f"0034 {role} paired count drift")
    for field, source in (
        ("normal_net_usd", "normal_net_pnl_usd"),
        ("stressed_net_usd", "stressed_net_pnl_usd"),
        ("baseline_normal_net_usd", "baseline_normal_net_pnl_usd"),
        ("baseline_stressed_net_usd", "baseline_stressed_net_pnl_usd"),
        ("paired_normal_uplift_usd", "paired_normal_uplift_usd"),
        ("paired_stressed_uplift_usd", "paired_stressed_uplift_usd"),
    ):
        total = math.fsum(_finite(row.get(source)) for row in rows)
        if not math.isclose(_finite(raw.get(field)), total, rel_tol=0.0, abs_tol=1e-6):
            raise SelectiveVetoRuntimeError(f"0034 {role} paired aggregate drift: {field}")
    coverage = len(executed) / len(rows)
    if (
        not math.isclose(
            _finite(raw.get("trade_coverage")), coverage, rel_tol=0.0, abs_tol=1e-12
        )
        or not math.isclose(
            _finite(raw.get("abstention_rate")),
            1.0 - coverage,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise SelectiveVetoRuntimeError(f"0034 {role} trade coverage drift")


def _validate_account_size_matrix(
    long_sample: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    raw = long_sample.get("account_size_matrix")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise SelectiveVetoRuntimeError("0034 account-size matrix is absent")
    rows = [row for row in raw if isinstance(row, Mapping)]
    matrix = {str(row.get("account_label") or ""): row for row in rows}
    if len(rows) != 3 or set(matrix) != set(ACCOUNT_RULE_SNAPSHOTS):
        raise SelectiveVetoRuntimeError("0034 account-size matrix inventory drift")
    for label, row in matrix.items():
        expected = ACCOUNT_RULE_SNAPSHOTS[label]
        if (
            row.get("rule_snapshot_id") != expected["snapshot_id"]
            or row.get("rule_snapshot_sha256") != expected["snapshot_sha256"]
            or row.get("provenance_class") != expected["provenance_class"]
        ):
            raise SelectiveVetoRuntimeError(
                f"0034 {label} account-rule snapshot label/hash drift"
            )
        by_role = row.get("role_results_by_scenario", row.get("by_role"))
        if not isinstance(by_role, Mapping) or set(by_role) != {
            "DISCOVERY",
            "VALIDATION",
            "FINAL_DEVELOPMENT",
        }:
            raise SelectiveVetoRuntimeError(f"0034 {label} role matrix is incomplete")
        for role, scenarios in by_role.items():
            if not isinstance(scenarios, Mapping) or set(scenarios) != {
                "NORMAL",
                "STRESSED_1_5X",
            }:
                raise SelectiveVetoRuntimeError(
                    f"0034 {label}/{role} normal-stressed matrix drift"
                )
            for scenario, horizons in scenarios.items():
                if not isinstance(horizons, Mapping) or set(horizons) != {"p5", "p10"}:
                    raise SelectiveVetoRuntimeError(
                        f"0034 {label}/{role}/{scenario} horizon matrix drift"
                    )
                for horizon, metrics in horizons.items():
                    _validate_horizon_metrics(
                        metrics, label=label, role=str(role), scenario=str(scenario), horizon=horizon
                    )
    return matrix


def _validate_horizon_metrics(
    raw: Any, *, label: str, role: str, scenario: str, horizon: str
) -> None:
    if not isinstance(raw, Mapping):
        raise SelectiveVetoRuntimeError("0034 account horizon metrics are absent")
    denominator = _integer(raw.get("full_coverage_windows"))
    passes = _integer(raw.get("pass_count"))
    breaches = _integer(raw.get("mll_breach_count"))
    if denominator < 0 or not 0 <= passes <= denominator or not 0 <= breaches <= denominator:
        raise SelectiveVetoRuntimeError("0034 account horizon count drift")
    expected_pass_rate = passes / denominator if denominator else 0.0
    expected_breach_rate = breaches / denominator if denominator else 0.0
    for field in (
        "consistency_compliance_rate",
        "median_target_progress",
        "lower_quartile_target_progress",
        "minimum_mll_buffer_usd",
        "net_total_usd",
    ):
        _finite(raw.get(field))
    if (
        not math.isclose(
            _finite(raw.get("pass_rate")), expected_pass_rate, rel_tol=0.0, abs_tol=1e-12
        )
        or not math.isclose(
            _finite(raw.get("mll_breach_rate")),
            expected_breach_rate,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not 0.0 <= _finite(raw.get("consistency_compliance_rate")) <= 1.0
    ):
        raise SelectiveVetoRuntimeError(
            f"0034 {label}/{role}/{scenario}/{horizon} rate reconciliation drift"
        )
    episodes = raw.get("episodes")
    if not isinstance(episodes, Sequence) or isinstance(episodes, (str, bytes)):
        raise SelectiveVetoRuntimeError("0034 exact account episodes are absent")
    if len(episodes) != denominator:
        raise SelectiveVetoRuntimeError("0034 exact account episode denominator drift")


def _validate_green_gate(
    manifest: Mapping[str, Any],
    long_sample: Mapping[str, Any],
    role_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    matrix: Mapping[str, Mapping[str, Any]],
) -> None:
    role_results = long_sample["role_results"]
    heldout = [*role_rows["VALIDATION"], *role_rows["FINAL_DEVELOPMENT"]]
    executed = [row for row in heldout if str(row["action"]) != "ABSTAIN"]
    coverage = len(executed) / len(heldout)
    positive = [max(0.0, _finite(row.get("stressed_net_pnl_usd"))) for row in executed]
    positive_total = math.fsum(positive)
    domination = max(positive, default=0.0) / positive_total if positive_total > 0.0 else 1.0
    context_counts: list[int] = []
    for field in ("structural_family", "session_id"):
        groups = {str(row[field]) for row in heldout}
        context_counts.append(
            sum(
                math.fsum(
                    _finite(row.get("stressed_net_pnl_usd"))
                    for row in heldout
                    if str(row[field]) == group
                )
                > 0.0
                for group in groups
            )
        )
    positive_contexts = max(context_counts, default=0)
    gate = manifest["account_speed_gate"]
    maximum_mll = float(gate["maximum_mll_breach_rate"])
    any_stressed_pass = False
    mll_ok = True
    for account in matrix.values():
        by_role = account.get("role_results_by_scenario", account.get("by_role"))
        for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
            for horizon in ("p5", "p10"):
                metrics = by_role[role]["STRESSED_1_5X"][horizon]
                any_stressed_pass |= _integer(metrics["pass_count"]) > 0
                mll_ok &= _finite(metrics["mll_breach_rate"]) <= maximum_mll + 1e-12
    progress = long_sample.get("material_stressed_target_progress_uplift_by_role")
    material_progress = bool(
        isinstance(progress, Mapping)
        and all(
            _finite(progress.get(role))
            >= MATERIAL_STRESSED_TARGET_PROGRESS_UPLIFT_MINIMUM - 1e-12
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        )
    )
    fastest = str(long_sample.get("fastest_viable_account_size") or "")
    if (
        any(
            _finite(role_results[role].get("stressed_net_usd")) <= 0.0
            or _finite(role_results[role].get("paired_stressed_uplift_usd")) <= 0.0
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        )
        or not 0.20 - 1e-12 <= coverage <= 0.80 + 1e-12
        or domination
        > float(gate["maximum_single_trade_positive_profit_fraction"]) + 1e-12
        or positive_contexts
        < int(gate["minimum_distinct_family_or_context_count"])
        or not mll_ok
        or long_sample.get("consistency_within_tolerance") is not True
        or not (any_stressed_pass or material_progress)
        or fastest not in ACCOUNT_RULE_SNAPSHOTS
    ):
        raise SelectiveVetoRuntimeError("0034 GREEN decision fails frozen economic gate")


def _validate_forward_evidence(
    manifest: Mapping[str, Any],
    forward: Mapping[str, Any],
    *,
    seed_decision: str,
) -> None:
    status = str(forward.get("status") or "")
    if seed_decision == "SELECTIVE_VETO_SEED_FALSIFIED" and status != (
        "NOT_STARTED_SEED_FALSIFIED"
    ):
        raise SelectiveVetoRuntimeError("0034 falsified seeds may not activate forward")
    if status != "SELECTIVE_VETO_DIAGNOSTIC_FORWARD":
        return
    freeze = _parse_utc(manifest.get("created_at_utc"), "manifest freeze")
    first = _parse_utc(forward.get("first_event_time_utc"), "first forward event")
    last = _parse_utc(forward.get("last_event_time_utc"), "last forward event")
    fingerprints = forward.get("raw_event_fingerprints")
    expected_seed_hashes = {
        str(row["policy_id"]): str(row["policy_fingerprint"])
        for row in manifest["frozen_seed_policies"]
    }
    if (
        first <= freeze
        or last < first
        or not _is_sha256(str(forward.get("feed_authorization_receipt_sha256") or ""))
        or not isinstance(fingerprints, Sequence)
        or isinstance(fingerprints, (str, bytes))
        or not fingerprints
        or any(not _is_sha256(str(value)) for value in fingerprints)
        or len(set(str(value) for value in fingerprints)) != len(fingerprints)
        or _integer(forward.get("raw_event_count")) != len(fingerprints)
        or dict(forward.get("policy_fingerprints") or {}) != expected_seed_hashes
    ):
        raise SelectiveVetoRuntimeError("0034 diagnostic-forward provenance drift")


def _validate_evidence_material(
    campaign: Mapping[str, Any], *, long_sample: Mapping[str, Any]
) -> None:
    datasets = campaign["evidence_datasets"]
    compact = campaign["compact_outputs"]
    if set(datasets) != set(REQUIRED_DATASETS):
        raise SelectiveVetoRuntimeError("0034 EvidenceBundle datasets are incomplete")
    if set(compact) != set(REQUIRED_COMPACT_OUTPUTS):
        raise SelectiveVetoRuntimeError("0034 EvidenceBundle compact outputs are incomplete")
    materialized: dict[str, list[dict[str, Any]]] = {}
    for dataset in REQUIRED_DATASETS:
        rows = datasets[dataset]
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not rows:
            raise SelectiveVetoRuntimeError(f"0034 EvidenceBundle dataset is empty: {dataset}")
        checked: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise SelectiveVetoRuntimeError(f"0034 malformed EvidenceBundle row: {dataset}")
            if _contains_economic_sentinel(row):
                raise SelectiveVetoRuntimeError("0034 fabricated/sentinel economic evidence forbidden")
            try:
                checked.append(
                    RECORD_SPECS[dataset].validate(row, campaign_id=CAMPAIGN_ID)
                )
            except Exception as exc:
                raise SelectiveVetoRuntimeError(
                    f"0034 EvidenceBundle schema failure: {dataset}"
                ) from exc
        materialized[dataset] = checked

    episode_scenarios: dict[tuple[str, str, str], set[str]] = {}
    for row in materialized["episodes"]:
        key = (str(row["policy_id"]), str(row["episode_id"]), str(row["horizon"]))
        episode_scenarios.setdefault(key, set()).add(str(row["cost_scenario"]))
    if any(value != {"NORMAL", "STRESSED_1_5X"} for value in episode_scenarios.values()):
        raise SelectiveVetoRuntimeError("0034 normal/stressed episode pairing drift")
    episode_rows = {
        (str(row["policy_id"]), str(row["episode_id"]), str(row["horizon"]), str(row["cost_scenario"]))
        for row in materialized["episodes"]
    }
    if any(
        (
            str(row["policy_id"]),
            str(row["episode_id"]),
            str(row["horizon"]),
            str(row["cost_scenario"]),
        )
        not in episode_rows
        for row in materialized["account_daily_paths"]
    ):
        raise SelectiveVetoRuntimeError("0034 daily paths lack a canonical episode")
    paired = list(long_sample.get("paired_results") or ())
    if paired:
        executed = sum(str(row.get("action")) != "ABSTAIN" for row in paired)
        if (
            len(materialized["component_signals"]) != len(paired)
            or len(materialized["component_entries"]) != executed
            or len(materialized["component_exits"]) != executed
            or len(materialized["component_trades"]) != executed
        ):
            raise SelectiveVetoRuntimeError(
                "0034 EvidenceBundle trade/decision denominator drift"
            )


def _contains_economic_sentinel(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(_contains_economic_sentinel(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_contains_economic_sentinel(item) for item in value)
    if isinstance(value, str):
        upper = value.upper()
        return "DIAGNOSTIC_ZERO_RISK_LEDGER_SENTINEL" in upper or upper == "DIAGNOSTIC_ONLY"
    return False


def _validate_cost_matrix(
    manifest: Mapping[str, Any], cost: Mapping[str, Any], anchor: Mapping[str, Any]
) -> None:
    if (
        cost.get("status") not in {"OFFICIAL_COST_MATRIX_COMPLETE", "NO_AFFORDABLE_OFFER"}
        or cost.get("official_metadata_get_cost_used") is not True
        or cost.get("full_session_matrix_reused_as_final") is not False
        or _integer(anchor.get("anchors_generated")) <= 0
        or _integer(anchor.get("merged_windows_estimated")) <= 0
    ):
        raise SelectiveVetoRuntimeError("0034 targeted cost matrix is incomplete")
    rows = cost.get("rows")
    if not isinstance(rows, list) or not rows:
        raise SelectiveVetoRuntimeError("0034 targeted cost matrix has no rows")
    expected_schemas = set(str(value) for value in manifest["targeted_cost_policy"]["schemas"])
    expected_counts = set(int(value) for value in manifest["targeted_cost_policy"]["window_counts"])
    seen_grid: set[tuple[str, int, int]] = set()
    row_by_fingerprint: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise SelectiveVetoRuntimeError("0034 malformed targeted cost row")
        schema = str(row.get("schema") or "")
        count = _integer(row.get("anchor_window_count"))
        if schema not in expected_schemas or count not in expected_counts:
            raise SelectiveVetoRuntimeError("0034 targeted cost row is outside frozen grid")
        if (
            _finite(row.get("merged_window_duration_seconds")) <= 0.0
            or _integer(row.get("estimated_records")) < 0
            or _integer(row.get("estimated_bytes")) < 0
            or _finite(row.get("estimated_cost_usd")) < 0.0
            or not str(row.get("feature_coverage") or "")
            or _integer(row.get("market_count")) not in {1, 2}
            or not str(row.get("estimate_fingerprint") or "")
        ):
            raise SelectiveVetoRuntimeError("0034 targeted cost row is incomplete")
        key = (schema, count, _integer(row["market_count"]))
        fingerprint = str(row["estimate_fingerprint"])
        if key in seen_grid or fingerprint in row_by_fingerprint:
            raise SelectiveVetoRuntimeError("0034 targeted cost row is duplicated")
        seen_grid.add(key)
        row_by_fingerprint[fingerprint] = row
    role_costs = cost.get("chronological_role_costs")
    if (
        seen_grid
        != {
            (schema, count, market_count)
            for schema in expected_schemas
            for count in expected_counts
            for market_count in (1, 2)
        }
        or not isinstance(role_costs, Mapping)
        or set(str(key) for key in role_costs)
        != {"DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT"}
    ):
        raise SelectiveVetoRuntimeError("0034 targeted cost grid coverage drift")
    selected = cost.get("selected_offer")
    if selected is not None:
        if not isinstance(selected, Mapping):
            raise SelectiveVetoRuntimeError("0034 selected cost offer is malformed")
        fingerprint = str(selected.get("estimate_fingerprint") or "")
        row = row_by_fingerprint.get(fingerprint)
        if row is None or any(
            selected.get(field) != row.get(field)
            for field in (
                "schema",
                "anchor_window_count",
                "market_count",
                "estimated_cost_usd",
            )
        ):
            raise SelectiveVetoRuntimeError("0034 selected offer is outside frozen cost grid")


def _seal_evidence_bundle(
    root: Path,
    output: Path,
    manifest: Mapping[str, Any],
    campaign: Mapping[str, Any],
    scientific: Mapping[str, Any],
) -> dict[str, Any]:
    identity = campaign["evidence_identity"]
    datasets = campaign["evidence_datasets"]
    compact = dict(campaign["compact_outputs"])
    compact.setdefault(
        "next_campaign_recommendations",
        _next_recommendation(str(scientific["decision"])),
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
        value = receipt.to_dict()
        _verify_evidence_receipt(value, expected_identity=identity)
        return value
    staging = base / f".{CAMPAIGN_ID}.evidence-v1.staging"
    writer = (
        EvidenceBundleWriter.resume(base, CAMPAIGN_ID, expected_identity=identity)
        if staging.is_dir()
        else EvidenceBundleWriter.create(base, identity, writer_id=CAMPAIGN_ID)
    )
    try:
        for dataset in REQUIRED_DATASETS:
            observed_count = int(writer.dataset_row_counts.get(dataset, 0))
            expected_count = len(datasets[dataset])
            if observed_count not in {0, expected_count}:
                raise SelectiveVetoRuntimeError(
                    f"0034 staged EvidenceBundle count drift: {dataset}"
                )
            if observed_count == 0:
                writer.append_records(
                    dataset,
                    datasets[dataset],
                    batch_id=f"0034-{dataset}-0000",
                )
        for name, value in compact.items():
            writer.write_compact_output(str(name), value)
        receipt = writer.finalize(
            evidence_status=str(manifest["evidence_bundle"]["evidence_status"]),
            lightweight_manifest_path=lightweight,
        )
    finally:
        writer.close()
    value = receipt.to_dict()
    _verify_evidence_receipt(value, expected_identity=identity)
    return value


def _verify_evidence_receipt(
    receipt: Mapping[str, Any], *, expected_identity: Mapping[str, Any]
) -> None:
    bundle_path = Path(str(receipt.get("bundle_path") or ""))
    verified = verify_evidence_bundle(bundle_path, deep=True)
    manifest_path = Path(str(receipt.get("manifest_path") or ""))
    try:
        observed_identity = json.loads(
            (bundle_path / "identity.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectiveVetoRuntimeError("0034 sealed EvidenceBundle identity absent") from exc
    if (
        verified.get("campaign_id") != CAMPAIGN_ID
        or observed_identity != dict(expected_identity)
        or verified.get("identity_sha256")
        != _file_sha256(bundle_path / "identity.json")
        or receipt.get("manifest_sha256") != _file_sha256(manifest_path)
        or receipt.get("bundle_content_sha256")
        != verified.get("bundle_content_sha256")
        or receipt.get("dataset_row_counts") != verified.get("dataset_row_counts")
    ):
        raise SelectiveVetoRuntimeError("0034 sealed EvidenceBundle receipt drift")


def _controller_kpis(
    manifest: Mapping[str, Any],
    campaign: Mapping[str, Any] | None,
    *,
    state: str,
    checkpoint_sequence: int,
) -> dict[str, Any]:
    if campaign is None:
        candidate_count = normal_episodes = stressed_episodes = 0
        view = {
            "pass_rates_normal": [],
            "pass_rates_stressed": [],
            "target_progress_stressed": [],
            "mll_rates_stressed": [],
            "positive_stressed_net_count": 0,
        }
        elapsed = 0.0
        economic_fraction = cpu_fraction = 0.0
    else:
        candidate_count = _candidate_count(campaign)
        scenario_counts = _episode_scenario_counts(campaign)
        normal_episodes = scenario_counts["NORMAL"]
        stressed_episodes = scenario_counts["STRESSED_1_5X"]
        view = _matrix_frontier(campaign["long_sample"])
        elapsed = max(_finite(campaign["runtime_metrics"].get("elapsed_seconds")), 0.0)
        economic_fraction = _unit(
            campaign["runtime_metrics"].get("economic_wall_clock_fraction")
        )
        cpu_fraction = _unit(
            campaign["runtime_metrics"].get("aggregate_cpu_utilization")
        )
    combined = normal_episodes + stressed_episodes
    hours = elapsed / 3_600.0 if elapsed > 0.0 else 0.0

    def rate(value: int) -> float:
        return float(value / hours) if hours > 0.0 else 0.0

    normal_rates = view["pass_rates_normal"]
    stressed_rates = view["pass_rates_stressed"]
    positive_count = min(candidate_count, int(view["positive_stressed_net_count"]))
    normal_pass_candidates = int(bool(normal_rates and max(normal_rates) > 0.0))
    stressed_pass_candidates = int(bool(stressed_rates and max(stressed_rates) > 0.0))
    return {
        "schema": PRODUCTION_KPI_SCHEMA,
        "scientific_schema": SCIENTIFIC_KPI_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "checkpoint_sequence": int(checkpoint_sequence),
        "updated_at_utc": _utc_now(),
        "state": state,
        "rates_per_hour": {
            "policies_proposed": rate(candidate_count),
            "unique_policies_screened": rate(candidate_count),
            "exact_account_replays": rate(candidate_count),
            "combine_episodes": rate(combined),
        },
        "workers": {"compute": 2, "evidence_writer": 1},
        "policies_proposed": candidate_count,
        "unique_policies_screened": candidate_count,
        "exact_account_replays": candidate_count,
        "combine_episodes_completed": combined,
        "normal_episodes_completed": normal_episodes,
        "stressed_episodes_completed": stressed_episodes,
        "positive_stressed_net_candidates": positive_count,
        "candidates_with_normal_pass": normal_pass_candidates,
        "candidates_with_stressed_pass": stressed_pass_candidates,
        "best_normal_pass_rate": max(normal_rates, default=0.0),
        "best_stressed_pass_rate": max(stressed_rates, default=0.0),
        "median_normal_pass_rate": _median(normal_rates),
        "median_stressed_pass_rate": _median(stressed_rates),
        "near_pass_count": 0,
        "candidates_promoted_96": 0,
        "candidates_surviving_96": 0,
        "candidates_promoted_192": 0,
        "confirmation_ready_candidates": 0,
        "duplicate_rejection_rate": 0.0,
        "cache_hit_rate": 1.0,
        "economic_research_wall_clock_fraction": economic_fraction,
        "cpu_utilization_fraction": cpu_fraction,
        "admin_overhead_alert": False,
        "matched_controls_status": "PAIRED_IDENTICAL_STRUCTURAL_OPPORTUNITY",
        "null_status": "BASELINE_IMMEDIATE_CAUSAL_STRUCTURAL_TRADE",
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "manifest_bound_data_purchase_count": int(
            bool(campaign and campaign["acquisition"].get("purchase_performed"))
        ),
        "data_purchase_count_scope": "UNMANIFESTED_CONTROLLER_ROUTE_ONLY",
    }


def _economic_view(
    campaign: Mapping[str, Any], kpis: Mapping[str, Any]
) -> dict[str, Any]:
    frontier = _matrix_frontier(campaign["long_sample"])
    candidate_count = _candidate_count(campaign)
    normal = int(kpis["normal_episodes_completed"])
    stressed = int(kpis["stressed_episodes_completed"])
    economic_frontier = {
        "candidate_count": candidate_count,
        "positive_stressed_net_count": min(
            candidate_count, int(frontier["positive_stressed_net_count"])
        ),
        "normal_pass_fraction_best": max(frontier["pass_rates_normal"], default=0.0),
        "normal_pass_fraction_median": _median(frontier["pass_rates_normal"]),
        "stressed_pass_fraction_best": max(
            frontier["pass_rates_stressed"], default=0.0
        ),
        "stressed_pass_fraction_median": _median(frontier["pass_rates_stressed"]),
        "stressed_target_progress_median_best": max(
            frontier["target_progress_stressed"], default=0.0
        ),
        "stressed_target_progress_median_population": _median(
            frontier["target_progress_stressed"]
        ),
        "stressed_mll_breach_rate_minimum": min(
            frontier["mll_rates_stressed"], default=0.0
        ),
        "stressed_mll_breach_rate_maximum": max(
            frontier["mll_rates_stressed"], default=0.0
        ),
    }
    return {
        "production_counters": {
            "serious_exact_account_replays": candidate_count,
            "predeclared_control_policy_replays": len(
                campaign["long_sample"].get("paired_results") or ()
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
        "economic_frontier": economic_frontier,
        "normal_pass_candidate_count": int(
            bool(frontier["pass_rates_normal"] and max(frontier["pass_rates_normal"]) > 0.0)
        ),
        "stressed_pass_candidate_count": int(
            bool(
                frontier["pass_rates_stressed"]
                and max(frontier["pass_rates_stressed"]) > 0.0
            )
        ),
    }


def _candidate_count(campaign: Mapping[str, Any]) -> int:
    long_sample = campaign["long_sample"]
    return 1 if long_sample.get("status") == "COMPLETE" else len(SEED_IDS)


def _episode_scenario_counts(campaign: Mapping[str, Any]) -> dict[str, int]:
    counts = {"NORMAL": 0, "STRESSED_1_5X": 0}
    for row in campaign["evidence_datasets"].get("episodes") or ():
        scenario = str(row.get("cost_scenario") or "")
        if scenario in counts:
            counts[scenario] += 1
    return counts


def _matrix_frontier(long_sample: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "pass_rates_normal": [],
        "pass_rates_stressed": [],
        "target_progress_stressed": [],
        "mll_rates_stressed": [],
        "positive_stressed_net_count": 0,
    }
    matrix = long_sample.get("account_size_matrix")
    if not isinstance(matrix, Sequence) or isinstance(matrix, (str, bytes)):
        return result
    for account in matrix:
        if not isinstance(account, Mapping):
            continue
        by_role = account.get("role_results_by_scenario", account.get("by_role"))
        if not isinstance(by_role, Mapping):
            continue
        for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
            scenarios = by_role.get(role)
            if not isinstance(scenarios, Mapping):
                continue
            for scenario, destination in (
                ("NORMAL", "pass_rates_normal"),
                ("STRESSED_1_5X", "pass_rates_stressed"),
            ):
                horizons = scenarios.get(scenario)
                if not isinstance(horizons, Mapping):
                    continue
                for horizon in ("p5", "p10"):
                    metrics = horizons.get(horizon)
                    if not isinstance(metrics, Mapping):
                        continue
                    result[destination].append(_unit(metrics.get("pass_rate")))
                    if scenario == "STRESSED_1_5X":
                        result["target_progress_stressed"].append(
                            _finite(metrics.get("median_target_progress"))
                        )
                        result["mll_rates_stressed"].append(
                            _unit(metrics.get("mll_breach_rate"))
                        )
    role_results = long_sample.get("role_results")
    if isinstance(role_results, Mapping) and all(
        isinstance(role_results.get(role), Mapping)
        and _finite(role_results[role].get("stressed_net_usd")) > 0.0
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
    ):
        result["positive_stressed_net_count"] = 1
    return result


def _build_terminal_result(
    manifest: Mapping[str, Any],
    campaign: Mapping[str, Any],
    scientific: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    decision = str(scientific["decision"])
    kpis = _controller_kpis(
        manifest,
        campaign,
        state="COMPLETE",
        checkpoint_sequence=0,
    )
    view = _economic_view(campaign, kpis)
    long_sample = campaign["long_sample"]
    selected_ids = []
    if decision == "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN" and isinstance(
        long_sample.get("policy"), Mapping
    ):
        selected_ids = [str(long_sample["policy"]["policy_id"])]
    economic_results = {
        "schema": "hydra_selective_veto_0034_economics_v1",
        "production_counters": view["production_counters"],
        "production_kpis": view["production_kpis"],
        "economic_frontier": view["economic_frontier"],
        "candidate_count": view["economic_frontier"]["candidate_count"],
        "normal_pass_candidate_count": view["normal_pass_candidate_count"],
        "stressed_pass_candidate_count": view["stressed_pass_candidate_count"],
        "positive_stressed_net_count": view["economic_frontier"][
            "positive_stressed_net_count"
        ],
        "confirmation_ready_candidate_ids": [],
        "development_only": True,
        "independently_confirmed": False,
        "decision": decision,
        "seed_decision": scientific["seed_decision"],
        "actual_additional_spend_usd": scientific["actual_spend_usd"],
        "remaining_budget_usd": scientific["remaining_budget_usd"],
        "manifest_bound_data_purchase_count": scientific[
            "manifest_bound_data_purchase_count"
        ],
        "unmanifested_data_purchase_count": 0,
        "diagnostic_forward_status": scientific["diagnostic_forward_status"],
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
    }
    economic_results["summary_hash"] = stable_hash(economic_results)
    result = build_final_result_payload(
        manifest=manifest,
        kpis=kpis,
        economic_results=economic_results,
        successive_halving={
            "schema": "hydra_selective_veto_0034_gate_v1",
            "stage_decisions": [
                {
                    "stage": "LONG_SAMPLE_SELECTIVE_OVERLAY",
                    "input_count": int(view["economic_frontier"]["candidate_count"]),
                    "output_count": len(selected_ids),
                    "selected_policy_ids": selected_ids,
                }
            ],
            "thresholds_changed_after_results": False,
            "broad_refinement_resumed": False,
        },
        matched_controls={
            "schema": "hydra_selective_veto_0034_paired_controls_v1",
            "baseline": "BASELINE_IMMEDIATE_CAUSAL_STRUCTURAL_TRADE",
            "identical_opportunity_pairing": True,
            "paired_opportunity_count": len(
                campaign["long_sample"].get("paired_results") or ()
            ),
            "controls_selected_after_outcomes": False,
        },
        failure_vectors={
            "SEED_FALSIFIED": int(
                scientific["seed_decision"] == "SELECTIVE_VETO_SEED_FALSIFIED"
            ),
            "NO_AFFORDABLE_LONG_SAMPLE": int(not scientific["purchase_performed"]),
            "LONG_SAMPLE_FALSIFIED": int(
                decision == "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED"
            ),
        },
        evidence_receipt=receipt,
        autonomous_next_action=_next_recommendation(decision)["recommendation"],
        scientific_status=decision,
    )
    result.pop("result_hash", None)
    result.update(
        {
            "scientific_schema": SCIENTIFIC_RESULT_SCHEMA,
            "campaign_mode": manifest["campaign_mode"],
            "runtime_version": RUNTIME_VERSION,
            "decision": decision,
            "seed_decision": scientific["seed_decision"],
            "economic_summary": {
                "seed_audit": campaign["seed_audit"],
                "anchor_universe": campaign["anchor_universe"],
                "window_cost_matrix": campaign["window_cost_matrix"],
                "acquisition": campaign["acquisition"],
                "long_sample": campaign["long_sample"],
                "diagnostic_forward": campaign["diagnostic_forward"],
            },
            "production_kpis": campaign["production_kpis"],
            "runtime_metrics": campaign["runtime_metrics"],
            "actual_additional_spend_usd": scientific["actual_spend_usd"],
            "remaining_budget_usd": scientific["remaining_budget_usd"],
            # Generic controller counters describe only forbidden, unmanifested
            # acquisition.  The permitted bounded purchase is reported
            # separately and reconciled to its immutable receipt above.
            "new_data_purchase_count": 0,
            "manifest_bound_data_purchase_count": scientific[
                "manifest_bound_data_purchase_count"
            ],
            "unmanifested_data_purchase_count": 0,
            "q4_access_delta": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "completed_at_utc": _utc_now(),
        }
    )
    result["result_hash"] = stable_hash(result)
    return result


def _next_recommendation(decision: str) -> dict[str, Any]:
    actions = {
        "LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN": "BUILD_10_TO_20_DISTINCT_SELECTIVE_OVERLAYS",
        "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK": "RUN_ONE_BOUNDED_ABSTENTION_SCORE_REFINEMENT",
        "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED": "TERMINATE_MICROSTRUCTURE_PRIMARY_LANE",
    }
    if decision not in actions:
        raise SelectiveVetoRuntimeError("0034 successor decision is unsupported")
    return {
        "schema": "hydra_production_next_campaign_recommendations_v1",
        "campaign_id": CAMPAIGN_ID,
        "recommendation": {
            "action": actions[decision],
            "manifest_required": decision != "LONG_SAMPLE_SELECTIVE_OVERLAY_FALSIFIED",
            "automatic_broad_data_purchase_authorized": False,
            "new_data_purchase_authorized": False,
            "q4_access_authorized": False,
            "xfa_work_authorized": False,
        },
    }


def _write_state(
    output: Path,
    manifest: Mapping[str, Any],
    *,
    state: str,
    stage: str,
    next_action: str,
    campaign: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> None:
    prior_path = output / "production_state.json"
    prior = _read_hashed(prior_path, "state_hash") if prior_path.is_file() else {}
    sequence = int(prior.get("checkpoint_sequence", 0)) + 1
    controller_kpis = _controller_kpis(
        manifest,
        campaign,
        state=state,
        checkpoint_sequence=sequence,
    )
    core = {
        "schema": PRODUCTION_STATE_SCHEMA,
        "scientific_schema": SCIENTIFIC_STATE_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "state": state,
        "stage": stage,
        "next_action": next_action,
        "checkpoint_sequence": sequence,
        "started_at_utc": str(prior.get("started_at_utc") or _utc_now()),
        "updated_at_utc": _utc_now(),
        "runner_pid": os.getpid(),
        "worker_count": 2,
        "evidence_writer_count": 1,
        "broker_connections": 0,
        "orders": 0,
        "data_purchase_count": 0,
        "manifest_bound_data_purchase_count": int(
            controller_kpis["manifest_bound_data_purchase_count"]
        ),
        "data_purchase_count_scope": "UNMANIFESTED_CONTROLLER_ROUTE_ONLY",
        "q4_access_count_delta": 0,
        "policies_proposed": int(controller_kpis["policies_proposed"]),
        "unique_policies_screened": int(
            controller_kpis["unique_policies_screened"]
        ),
        "exact_account_replays": int(controller_kpis["exact_account_replays"]),
        "combine_episodes_completed": int(
            controller_kpis["combine_episodes_completed"]
        ),
        **dict(extra or {}),
    }
    state_value = {**core, "state_hash": stable_hash(core)}
    _atomic_json(prior_path, state_value)
    kpi_core = dict(controller_kpis)
    _atomic_json(
        output / "production_kpis.json",
        {**kpi_core, "kpi_hash": stable_hash(kpi_core)},
    )


def _verify_existing_result(
    result: Mapping[str, Any], manifest: Mapping[str, Any]
) -> None:
    if (
        result.get("schema") != RESULT_SCHEMA
        or result.get("campaign_id") != CAMPAIGN_ID
        or result.get("manifest_hash") != manifest.get("manifest_hash")
        or result.get("source_commit") != manifest.get("source_commit")
        or result.get("status") != "COMPLETE"
        or result.get("decision") not in LONG_SAMPLE_DECISIONS
        or result.get("scientific_status") != result.get("decision")
        or not isinstance(result.get("kpis"), Mapping)
        or not isinstance(result.get("economic_results"), Mapping)
        or result.get("new_data_purchase_count") != 0
        or result.get("unmanifested_data_purchase_count") != 0
    ):
        raise SelectiveVetoRuntimeError("0034 existing result identity/status drift")
    receipt = result.get("evidence_bundle")
    if not isinstance(receipt, Mapping):
        raise SelectiveVetoRuntimeError("0034 existing result lacks EvidenceBundle")
    bundle_path = Path(str(receipt.get("bundle_path") or ""))
    try:
        expected_identity = json.loads(
            (bundle_path / "identity.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectiveVetoRuntimeError(
            "0034 existing EvidenceBundle identity is unreadable"
        ) from exc
    _verify_evidence_receipt(receipt, expected_identity=expected_identity)
    if (
        result.get("evidence_verification_manifest_sha256")
        != receipt.get("manifest_sha256")
        or _integer(result.get("manifest_bound_data_purchase_count"))
        != _integer(
            result["economic_results"].get("manifest_bound_data_purchase_count")
        )
    ):
        raise SelectiveVetoRuntimeError("0034 existing terminal receipt drift")


def _read_hashed(path: Path, hash_key: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectiveVetoRuntimeError(f"0034 invalid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise SelectiveVetoRuntimeError(f"0034 JSON artifact is not an object: {path}")
    claimed = str(value.get(hash_key) or "")
    payload = dict(value)
    payload.pop(hash_key, None)
    if not claimed or stable_hash(payload) != claimed:
        raise SelectiveVetoRuntimeError(f"0034 hash drift: {path}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(
        json.dumps(dict(value), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, path)


def _set_single_thread_libraries() -> None:
    for name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[name] = "1"


def _finite(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise SelectiveVetoRuntimeError("0034 finite numeric field is invalid") from exc
    if not math.isfinite(result):
        raise SelectiveVetoRuntimeError("0034 finite numeric field is invalid")
    return result


def _unit(value: Any) -> float:
    result = _finite(value)
    if not 0.0 <= result <= 1.0:
        raise SelectiveVetoRuntimeError("0034 unit-interval field is invalid")
    return result


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(_finite(value) for value in values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _parse_utc(value: Any, label: str) -> datetime:
    raw = str(value or "")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SelectiveVetoRuntimeError(f"0034 invalid UTC timestamp: {label}") from exc
    if parsed.tzinfo is None:
        raise SelectiveVetoRuntimeError(f"0034 naive UTC timestamp: {label}")
    return parsed.astimezone(UTC)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise SelectiveVetoRuntimeError(f"0034 artifact is unreadable: {path}") from exc
    return digest.hexdigest()


def _integer(value: Any) -> int:
    if isinstance(value, bool):
        raise SelectiveVetoRuntimeError("0034 integer field is invalid")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise SelectiveVetoRuntimeError("0034 integer field is invalid") from exc
    if isinstance(value, float) and not value.is_integer():
        raise SelectiveVetoRuntimeError("0034 integer field is invalid")
    return result


def _zero(value: Any) -> bool:
    try:
        return math.isclose(_finite(value), 0.0, rel_tol=0.0, abs_tol=1e-12)
    except SelectiveVetoRuntimeError:
        return False


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "SelectiveVetoRuntimeError",
    "read_selective_veto_status",
    "run_selective_veto_manifest",
]
