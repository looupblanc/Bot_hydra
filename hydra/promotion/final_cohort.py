from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


FINAL_COHORT_SCHEMA = "hydra_final_q4_cohort_v4"
ALLOWED_ROLES = {"COMBINE_PASSER", "XFA_PAYOUT", "DEFENSIVE", "PORTFOLIO_ONLY"}


class FinalCohortError(RuntimeError):
    pass


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def file_sha256(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build_final_cohort_manifest(
    *,
    pre_holdout_manifest: Mapping[str, Any],
    validations: Sequence[Mapping[str, Any]],
    behavioral_clusters: Sequence[Mapping[str, Any]],
    package_records: Mapping[str, Mapping[str, Any]],
    source_commit: str,
    freeze_timestamp_utc: str,
    policy_path: str | Path,
    policy_sha256: str,
    source_artifact_hashes: Mapping[str, str],
    q4_access_count_before: int,
) -> dict[str, Any]:
    if q4_access_count_before != 0:
        raise FinalCohortError("Final cohort cannot freeze after Q4 access.")
    if len(source_commit) != 40:
        raise FinalCohortError("A full frozen source commit is required.")
    if file_sha256(policy_path) != policy_sha256:
        raise FinalCohortError("Preregistered Q4 policy hash drifted.")
    source_ids = [str(value) for value in pre_holdout_manifest.get("candidate_ids") or []]
    if not 3 <= len(source_ids) <= 8 or len(source_ids) != len(set(source_ids)):
        raise FinalCohortError("Final Q4 cohort must contain three to eight unique candidates.")
    validation_by_id = {
        str(row.get("candidate_id")): dict(row) for row in validations
    }
    cluster_by_id: dict[str, str] = {}
    for cluster in behavioral_clusters:
        for candidate_id in cluster.get("member_ids") or []:
            cluster_by_id[str(candidate_id)] = str(cluster.get("cluster_id") or "")
    candidates: list[dict[str, Any]] = []
    seen_clusters: set[str] = set()
    market_counts: Counter[str] = Counter()
    role_counts: Counter[str] = Counter()
    lineage_counts: Counter[str] = Counter()
    specifications = dict(pre_holdout_manifest.get("specifications") or {})
    for candidate_id in source_ids:
        validation = validation_by_id.get(candidate_id)
        specification = dict(specifications.get(candidate_id) or {})
        package = dict(package_records.get(candidate_id) or {})
        if validation is None or not specification or not package:
            raise FinalCohortError(f"Incomplete cohort inputs for {candidate_id}.")
        stages = dict(validation.get("stage_completion") or {})
        if not all(
            bool(stages.get(key))
            for key in (
                "full_economic_replay",
                "full_risk_replay",
                "full_promotion_validation",
            )
        ):
            raise FinalCohortError(f"Promotion evidence is incomplete for {candidate_id}.")
        if str(validation.get("decision")) != "PRE_HOLDOUT_READY":
            raise FinalCohortError(f"Candidate is not PRE_HOLDOUT_READY: {candidate_id}.")
        if validation.get("decision_reasons"):
            raise FinalCohortError(f"Unresolved promotion reasons for {candidate_id}.")
        if not bool((validation.get("economic") or {}).get("complete")):
            raise FinalCohortError(f"Economic replay incomplete for {candidate_id}.")
        risk = dict(validation.get("risk") or {})
        if not bool(risk.get("complete")) or bool(risk.get("mll_breached")):
            raise FinalCohortError(f"Risk replay failed for {candidate_id}.")
        role = str(validation.get("role") or "")
        market = str(validation.get("primary_market") or "")
        lineage = str(specification.get("lineage_id") or "")
        cluster = cluster_by_id.get(candidate_id, "")
        if role not in ALLOWED_ROLES or not market or not lineage or not cluster:
            raise FinalCohortError(f"Role/market/lineage/cluster missing for {candidate_id}.")
        if cluster in seen_clusters:
            raise FinalCohortError(f"Duplicate Level-2 economic cluster: {cluster}.")
        seen_clusters.add(cluster)
        market_counts[market] += 1
        role_counts[role] += 1
        lineage_counts[lineage] += 1
        if market_counts[market] > 2 or role_counts[role] > 2 or lineage_counts[lineage] > 2:
            raise FinalCohortError("Frozen market, role, or lineage cap exceeded.")
        spec_hash = hashlib.sha256(
            json.dumps(specification, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if spec_hash != str(validation.get("immutable_specification_hash") or ""):
            raise FinalCohortError(f"Candidate specification hash mismatch: {candidate_id}.")
        if spec_hash != str(package.get("candidate_specification_hash") or ""):
            raise FinalCohortError(f"Shadow package specification mismatch: {candidate_id}.")
        if bool(package.get("broker_connectivity")) or bool(
            package.get("outbound_order_capability")
        ):
            raise FinalCohortError(f"Unsafe shadow package: {candidate_id}.")
        candidates.append(
            {
                "candidate_id": candidate_id,
                "role": role,
                "primary_market": market,
                "execution_market": str(validation.get("execution_market") or ""),
                "timeframe_profile": str(validation.get("timeframe") or ""),
                "lineage_id": lineage,
                "behavioral_cluster_id": cluster,
                "specification": specification,
                "specification_hash": spec_hash,
                "shadow_package_hash": str(package.get("package_hash") or ""),
                "development_events": int((validation.get("economic") or {}).get("events") or 0),
                "development_net_pnl": float((validation.get("economic") or {}).get("net_pnl") or 0.0),
                "development_min_mll_buffer": float(risk.get("minimum_mll_buffer") or 0.0),
                "selected_micro_contracts": int(
                    ((risk.get("topstep") or {}).get("selected_micro_contracts") or 1)
                ),
                "topstep_rule_version": str(
                    ((risk.get("topstep") or {}).get("rule_version") or "")
                ),
                "role_evidence_frozen": validation.get("role_evidence") or {},
            }
        )
    manifest: dict[str, Any] = {
        "schema": FINAL_COHORT_SCHEMA,
        "cohort_id": "hydra_decision_bridge_v4_q4_cohort_0001",
        "freeze_timestamp_utc": freeze_timestamp_utc,
        "source_commit": source_commit,
        "source_pre_holdout_manifest_hash": str(
            pre_holdout_manifest.get("manifest_hash") or ""
        ),
        "source_artifact_hashes": dict(sorted(source_artifact_hashes.items())),
        "policy_path": str(policy_path),
        "policy_sha256": policy_sha256,
        "selection_policy": {
            "version": "decision_bridge_v4_earliest_stop_role_diverse_v1",
            "development_only": True,
            "one_primary_per_level2_cluster": True,
            "maximum_market_count": 2,
            "maximum_role_count": 2,
            "maximum_lineage_count": 2,
            "backups_excluded": True,
            "parameter_neighbor_inflation_prohibited": True,
            "stop_reason": "TWO_CONSECUTIVE_ZERO_NEW_PRE_HOLDOUT",
        },
        "q4_decision_policy": {
            "version": "decision_bridge_v4_role_specific_q4_v1",
            "minimum_executable_events": 5,
            "maximum_best_day_positive_pnl_fraction": 0.50,
            "minimum_xfa_qualifying_days": 2,
            "maximum_defensive_target_velocity_loss_fraction": 0.25,
            "maximum_defensive_matched_control_probability": 0.10,
            "minimum_defensive_control_count": 32,
            "allowed_results": [
                "Q4_LOCKBOX_PASS",
                "Q4_LOCKBOX_FAIL",
                "Q4_LOCKBOX_INSUFFICIENT",
            ],
        },
        "cost_policy": "candidate_frozen_exact_cost_plus_1_5x_diagnostic_v1",
        "sizing_policy": "candidate_frozen_micro_quantity_no_reselection_v1",
        "feature_policy": "closed_bar_past_only_turbo_feature_dag_v3",
        "candidate_ids": [row["candidate_id"] for row in candidates],
        "candidate_count": len(candidates),
        "candidates": candidates,
        "q4_period": ["2024-10-01", "2025-01-01"],
        "q4_access_count_before": q4_access_count_before,
        "q4_access_authorized": False,
        "authorization_token_hash": None,
        "status": "FINAL_Q4_COHORT_FROZEN_UNAUTHORIZED",
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    return manifest


def validate_final_cohort_manifest(manifest: Mapping[str, Any]) -> None:
    payload = dict(manifest)
    expected = str(payload.pop("manifest_hash", ""))
    if not expected or stable_hash(payload) != expected:
        raise FinalCohortError("Final cohort manifest semantic hash is invalid.")
    if str(manifest.get("schema")) != FINAL_COHORT_SCHEMA:
        raise FinalCohortError("Unsupported final cohort schema.")
    if int(manifest.get("q4_access_count_before") or 0) != 0:
        raise FinalCohortError("Final cohort froze after Q4 access.")
    if bool(manifest.get("q4_access_authorized")):
        raise FinalCohortError("The immutable cohort manifest may not embed authorization.")
    candidates = list(manifest.get("candidates") or [])
    if not 3 <= len(candidates) <= 8:
        raise FinalCohortError("Invalid final cohort size.")
