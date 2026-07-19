"""Bounded consistency-aware direct account policies over immutable Tier-Q sleeves.

This read-only branch is a failure-guided successor to the marginal book batch.
It does not alter a sleeve signal, entry, exit, stop, target, or selected Tier-Q
quantity.  Six preregistered account governors may only reject an entry or
reduce its executable whole-contract quantity from causal account state and the
declared entry-time stop-risk charge already sealed in the source evidence.

Profile selection is performed from B1/B2 summaries only.  B3/B4 are evaluated
under the selected frozen profile as viewed final-development evidence.  The
module never promotes a policy, writes a database or registry, starts XFA, or
creates an order route.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.portfolio.marginal_contribution_builder import GovernorProfile
from hydra.production import autonomous_marginal_combine_books as books
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    HORIZONS,
)
from hydra.production.fast_pass_runtime_helpers import _role_horizon_summary


SCHEMA = "hydra_autonomous_consistency_direct_account_policies_v1"
COMPOSITE_SCHEMA = "hydra_autonomous_consistency_direct_account_policy_shards_v1"
MAXIMUM_PROFILES = 6
MAXIMUM_CANDIDATES = 64
MATERIAL_TARGET_PROGRESS_DELTA = 0.01
MATERIAL_NET_DELTA_FLOOR_USD = 50.0
MATERIAL_NET_DELTA_FRACTION = 0.01
MATERIAL_MLL_RATE_DELTA = 0.01


class AutonomousConsistencyAccountPolicyError(RuntimeError):
    """The bounded direct-account branch cannot preserve its frozen contract."""


def frozen_consistency_profiles() -> tuple[GovernorProfile, ...]:
    """Return the complete preregistered six-profile causal frontier.

    ``daily_profit_lock_fraction`` is applied by the authoritative active-pool
    router to ``target * consistency_limit``.  The resulting realized-profit
    entry locks are therefore 15%, 20%, or 25% of the account target.  A trade
    already admitted is never truncated using its later outcome.
    """

    rows = (
        ("consistency_direct_01", 0.20, 0.25, 0.30, 0.75),
        ("consistency_direct_02", 0.25, 0.25, 0.40, 0.80),
        ("consistency_direct_03", 0.33, 0.25, 0.50, 0.85),
        ("consistency_direct_04", 0.20, 0.50, 0.30, 0.75),
        ("consistency_direct_05", 0.33, 0.50, 0.40, 0.80),
        ("consistency_direct_06", 0.50, 0.50, 0.50, 0.85),
    )
    profiles = tuple(
        GovernorProfile(
            profile_id=profile_id,
            signal_quality_tiers=(1.0,),
            open_risk_ceiling_fraction=open_risk,
            daily_loss_budget_fraction=daily_loss,
            daily_profit_lock_fraction=profit_lock,
            maximum_concurrent_sleeves=1,
            target_protection_fraction=target_protection,
            same_instrument_conflict_policy="priority",
        )
        for profile_id, open_risk, daily_loss, profit_lock, target_protection in rows
    )
    if len(profiles) > MAXIMUM_PROFILES or len(
        {row.profile_id for row in profiles}
    ) != len(profiles):
        raise AutonomousConsistencyAccountPolicyError(
            "frozen consistency frontier is invalid"
        )
    return profiles


def build_autonomous_consistency_account_policies(
    root: str | Path,
    candidate_bank: Mapping[str, Any],
    initial_exact_result: Mapping[str, Any],
    continuation_results: Sequence[Mapping[str, Any]] = (),
    *,
    maximum_candidates: int = MAXIMUM_CANDIDATES,
    shard_index: int = 0,
    shard_count: int = 1,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Evaluate one deterministic shard without performing authoritative writes."""

    if not 0 <= int(maximum_candidates) <= MAXIMUM_CANDIDATES:
        raise AutonomousConsistencyAccountPolicyError(
            "maximum candidate count must be in [0,64]"
        )
    if int(shard_count) not in {1, 2} or not 0 <= int(shard_index) < int(
        shard_count
    ):
        raise AutonomousConsistencyAccountPolicyError(
            "deterministic shard contract requires shard_count 1/2 and valid index"
        )

    bank = books._verify_candidate_bank(candidate_bank)
    composite, exact_results = books._verified_exact_results(
        initial_exact_result, continuation_results
    )
    if str(bank["source_composite_result_hash"]) != str(composite["result_hash"]):
        raise AutonomousConsistencyAccountPolicyError(
            "candidate bank and exact composite provenance differ"
        )
    tier_q_rows = tuple(
        sorted(
            (
                dict(row)
                for row in bank.get("candidates", ())
                if row.get("tier_q_contract_cleared") is True
            ),
            key=lambda row: str(row["candidate_id"]),
        )
    )
    if not tier_q_rows or int(maximum_candidates) == 0:
        return _empty_result(
            bank=bank,
            composite=composite,
            tier_q_rows=tier_q_rows,
            maximum_candidates=int(maximum_candidates),
            shard_index=int(shard_index),
            shard_count=int(shard_count),
        )
    books._require_tier_q_rows(tier_q_rows)
    context = books._prepare_replay_context(
        Path(root).resolve(),
        tier_q_rows,
        exact_results,
        fast_pass_manifest_path=fast_pass_manifest_path,
        rule_snapshot_path=rule_snapshot_path,
    )
    books._verify_context_matches_bank(context, tier_q_rows)

    candidate_ids = tuple(sorted(context.components))[: int(maximum_candidates)]
    inventory = [
        {
            "position": position,
            "candidate_id": candidate_id,
            "account_label": context.components[candidate_id].account_label,
            "candidate_fingerprint": context.components[
                candidate_id
            ].candidate_fingerprint,
        }
        for position, candidate_id in enumerate(candidate_ids)
    ]
    inventory_hash = stable_hash(inventory)
    selected_ids = tuple(
        row["candidate_id"]
        for row in inventory
        if int(row["position"]) % int(shard_count) == int(shard_index)
    )
    profiles = frozen_consistency_profiles()
    frontier_hash = stable_hash([asdict(row) for row in profiles])

    baseline_results: list[dict[str, Any]] = []
    profile_results: list[dict[str, Any]] = []
    selected_results: list[dict[str, Any]] = []
    for candidate_id in selected_ids:
        component = context.components[candidate_id]
        baseline_spec = books._policy_spec(
            account_label=component.account_label,
            members=(candidate_id,),
            profile=books._identity_profile(),
            components=context.components,
            policy_role="CONSISTENCY_DIRECT_IDENTITY_CONTROL",
            predecessor_policy_id=None,
        )
        baseline_raw = books._evaluate_policy_spec(baseline_spec, context)
        baseline = books._compact_policy_result(baseline_raw)
        baseline["source_candidate_id"] = candidate_id
        baseline["classification_role"] = "IDENTITY_CONTROL"
        baseline["result_hash"] = stable_hash(
            {key: value for key, value in baseline.items() if key != "result_hash"}
        )
        baseline_results.append(baseline)

        alternatives: list[dict[str, Any]] = []
        for profile in profiles:
            spec = books._policy_spec(
                account_label=component.account_label,
                members=(candidate_id,),
                profile=profile,
                components=context.components,
                policy_role="CONSISTENCY_AWARE_DIRECT_ACCOUNT_POLICY",
                predecessor_policy_id=baseline["policy_id"],
            )
            raw = books._evaluate_policy_spec(spec, context)
            compact = books._compact_policy_result(raw)
            compact.update(
                {
                    "source_candidate_id": candidate_id,
                    "classification_role": (
                        "CONSISTENCY_AWARE_DIRECT_ACCOUNT_POLICY"
                    ),
                    "selection_outcome_role": "B1_B2_DESIGN_ONLY",
                    "authoritative_promotion_status": None,
                }
            )
            compact["result_hash"] = stable_hash(
                {
                    key: value
                    for key, value in compact.items()
                    if key != "result_hash"
                }
            )
            alternatives.append(compact)
            profile_results.append(compact)

        chosen = max(
            alternatives,
            key=lambda row: _direct_policy_selection_rank(row, baseline),
        )
        selected = dict(chosen)
        selected["design_selection"] = _design_selection_receipt(
            chosen, alternatives, baseline
        )
        selected["identity_control_comparison"] = _control_comparison(
            chosen, baseline
        )
        contribution = _direct_policy_contribution(chosen, baseline)
        selected["direct_policy_contribution"] = contribution
        gates = books._g_ready_gates(selected, singleton=True)
        overlapping_day_gate = gates.pop("no_single_day_domination", None)
        selected["overlapping_start_day_share_diagnostic"] = (
            _overlapping_start_day_share_diagnostic(
                selected,
                legacy_gate_result=overlapping_day_gate,
            )
        )
        gates["independent_day_concentration_control_deferred"] = True
        gates.update(dict(contribution["gate_results"]))
        selected["g_precontrol_gate_results"] = gates
        selected["g_precontrol_ready"] = all(gates.values())
        selected["computed_development_tier"] = (
            "G_PRECONTROL_READY"
            if selected["g_precontrol_ready"]
            else "Q_DIRECT_POLICY_DIAGNOSTIC"
        )
        selected["authoritative_promotion_status"] = None
        selected["result_hash"] = stable_hash(
            {key: value for key, value in selected.items() if key != "result_hash"}
        )
        selected_results.append(selected)

    ready_ids = sorted(
        str(row["policy_id"])
        for row in selected_results
        if row["g_precontrol_ready"] is True
    )
    contribution_ids = sorted(
        str(row["policy_id"])
        for row in selected_results
        if all(dict(row["direct_policy_contribution"]["gate_results"]).values())
    )
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "COMPLETE_BOUNDED_CONSISTENCY_DIRECT_ACCOUNT_SHARD",
        "source_candidate_bank_hash": str(bank["result_hash"]),
        "source_composite_result_hash": str(composite["result_hash"]),
        "source_manifest_hash": context.source_manifest_hash,
        "frozen_grid_hash": context.frozen_grid_hash,
        "official_rule_snapshot_hash": context.official_rule_snapshot_hash,
        "policy_frontier": {
            "profiles": [asdict(row) for row in profiles],
            "profile_count": len(profiles),
            "frontier_hash": frontier_hash,
            "candidate_signal_logic_changed": False,
            "candidate_exit_logic_changed": False,
            "allowed_account_actions": ["REJECT_ENTRY", "REDUCE_ENTRY_QUANTITY"],
            "future_outcome_fields_used": False,
        },
        "selection_contract": {
            "admissible_input_tier": "Q",
            "design_blocks": list(books.DESIGN_BLOCKS),
            "held_out_development_blocks": list(
                books.HELD_OUT_DEVELOPMENT_BLOCKS
            ),
            "profile_selection_uses": "B1_B2_ONLY",
            "g_precontrol_evaluation_uses": "B3_B4_ONLY",
            "direct_policy_contribution_evaluation_uses": "B1_B2_ONLY",
            "direct_policy_requires_material_uplift_vs_identity": True,
            "direct_policy_requires_distinct_design_behavior_vs_identity": True,
            "direct_policy_forbids_material_design_degradation_vs_identity": True,
            "profile_maximum": MAXIMUM_PROFILES,
            "candidate_maximum": int(maximum_candidates),
            "no_signal_recomputation": True,
            "no_authoritative_promotion": True,
            "no_xfa": True,
        },
        "tier_q_component_ids": list(candidate_ids),
        "design_cell_exclusions": dict(sorted(context.design_cell_exclusions.items())),
        "component_freeze": [
            books._component_receipt(context.components[candidate_id])
            for candidate_id in candidate_ids
        ],
        "shard": {
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
            "partition_rule": "DETERMINISTIC_CANDIDATE_POSITION_MODULO_SHARD_COUNT_V1",
            "candidate_inventory_count": len(inventory),
            "candidate_inventory_hash": inventory_hash,
            "candidate_inventory_ids": list(candidate_ids),
            "selected_candidate_ids": list(selected_ids),
            "ranking_recomputed_after_sharding": False,
        },
        "baseline_results": baseline_results,
        "profile_results": profile_results,
        "selected_policy_results": selected_results,
        "counts": {
            "tier_q_input_count": len(tier_q_rows),
            "b1_b2_executable_candidate_count": len(candidate_ids),
            "selected_candidate_count": len(selected_ids),
            "frozen_profile_count": len(profiles),
            "direct_policy_exact_replay_count": len(profile_results),
            "identity_control_exact_replay_count": len(baseline_results),
            "completed_episode_count": sum(
                int(row.get("completed_episode_count", 0))
                for row in profile_results + baseline_results
            ),
            "data_censored_episode_count": sum(
                int(row.get("data_censored_episode_count", 0))
                for row in profile_results + baseline_results
            ),
            "g_precontrol_ready_count": len(ready_ids),
            "direct_policy_contribution_cleared_count": len(contribution_ids),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "broker_connections": 0,
            "orders": 0,
            **books._pass_counters(selected_results),
        },
        "candidate_ids": {
            "g_precontrol_ready": ready_ids,
            "direct_policy_contribution_cleared": contribution_ids,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "next_action": (
            "RUN_TRADE_CONCENTRATION_AND_MATCHED_CONTROLS"
            if ready_ids
            else "TERMINALIZE_CONSISTENCY_AWARE_GOVERNOR_AND_DISPATCH_DISTINCT_BRANCH"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def compose_autonomous_consistency_account_policy_shards(
    shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reconcile deterministic read-only shards without replaying economics."""

    values = [_verify_shard(value) for value in shards]
    if not values:
        raise AutonomousConsistencyAccountPolicyError(
            "at least one consistency-policy shard is required"
        )
    declared_count = int(dict(values[0]["shard"])["shard_count"])
    if declared_count not in {1, 2} or len(values) != declared_count:
        raise AutonomousConsistencyAccountPolicyError(
            "consistency-policy shard set is incomplete"
        )
    shared_fields = (
        "source_candidate_bank_hash",
        "source_composite_result_hash",
        "source_manifest_hash",
        "frozen_grid_hash",
        "official_rule_snapshot_hash",
        "policy_frontier",
        "selection_contract",
        "tier_q_component_ids",
        "design_cell_exclusions",
        "component_freeze",
    )
    for field_name in shared_fields:
        expected = stable_hash(values[0].get(field_name))
        if any(stable_hash(value.get(field_name)) != expected for value in values[1:]):
            raise AutonomousConsistencyAccountPolicyError(
                f"consistency-policy shard shared field differs: {field_name}"
            )
    inventory_ids = tuple(
        str(value) for value in dict(values[0]["shard"])["candidate_inventory_ids"]
    )
    inventory_hash = str(
        dict(values[0]["shard"])["candidate_inventory_hash"]
    )
    indexes: set[int] = set()
    selected_sets: list[set[str]] = []
    for value in values:
        shard = dict(value["shard"])
        index = int(shard["shard_index"])
        if (
            index in indexes
            or int(shard["shard_count"]) != declared_count
            or tuple(str(row) for row in shard["candidate_inventory_ids"])
            != inventory_ids
            or str(shard["candidate_inventory_hash"]) != inventory_hash
        ):
            raise AutonomousConsistencyAccountPolicyError(
                "consistency-policy shard inventory/index drift"
            )
        indexes.add(index)
        selected = {str(row) for row in shard["selected_candidate_ids"]}
        if selected != {
            str(row["source_candidate_id"])
            for row in value["selected_policy_results"]
        }:
            raise AutonomousConsistencyAccountPolicyError(
                "shard candidate IDs differ from selected policy results"
            )
        selected_sets.append(selected)
    if indexes != set(range(declared_count)):
        raise AutonomousConsistencyAccountPolicyError("shard indexes are incomplete")
    for position, left in enumerate(selected_sets):
        if any(left & right for right in selected_sets[position + 1 :]):
            raise AutonomousConsistencyAccountPolicyError(
                "candidate appears in more than one consistency-policy shard"
            )
    if set().union(*selected_sets) != set(inventory_ids):
        raise AutonomousConsistencyAccountPolicyError(
            "consistency-policy shard union does not exhaust inventory"
        )

    baseline = _unique_rows(values, "baseline_results", "policy_id")
    profiles = _unique_rows(values, "profile_results", "policy_id")
    selected = _unique_rows(
        values, "selected_policy_results", "source_candidate_id"
    )
    ordered_selected = [selected[candidate_id] for candidate_id in inventory_ids]
    ordered_baseline = [baseline[key] for key in sorted(baseline)]
    ordered_profiles = [profiles[key] for key in sorted(profiles)]
    ready_ids = sorted(
        str(row["policy_id"])
        for row in ordered_selected
        if row["g_precontrol_ready"] is True
    )
    contribution_ids = sorted(
        str(row["policy_id"])
        for row in ordered_selected
        if all(dict(row["direct_policy_contribution"]["gate_results"]).values())
    )
    counts = {
        "tier_q_input_count": int(dict(values[0]["counts"])["tier_q_input_count"]),
        "b1_b2_executable_candidate_count": len(inventory_ids),
        "selected_candidate_count": len(ordered_selected),
        "frozen_profile_count": int(
            dict(values[0]["policy_frontier"])["profile_count"]
        ),
        "direct_policy_exact_replay_count": len(ordered_profiles),
        "identity_control_exact_replay_count": len(ordered_baseline),
        "completed_episode_count": sum(
            int(row.get("completed_episode_count", 0))
            for row in ordered_profiles + ordered_baseline
        ),
        "data_censored_episode_count": sum(
            int(row.get("data_censored_episode_count", 0))
            for row in ordered_profiles + ordered_baseline
        ),
        "g_precontrol_ready_count": len(ready_ids),
        "direct_policy_contribution_cleared_count": len(contribution_ids),
        "authoritative_promotion_count": 0,
        "xfa_paths_started": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "broker_connections": 0,
        "orders": 0,
        **books._pass_counters(ordered_selected),
    }
    core: dict[str, Any] = {
        "schema": COMPOSITE_SCHEMA,
        "status": "COMPLETE_RECONCILED_CONSISTENCY_DIRECT_ACCOUNT_SHARDS",
        **{field: values[0][field] for field in shared_fields},
        "proposal_inventory": {
            "count": len(inventory_ids),
            "hash": inventory_hash,
            "candidate_ids": list(inventory_ids),
        },
        "shard_receipts": [
            {
                "shard_index": int(dict(value["shard"])["shard_index"]),
                "result_hash": str(value["result_hash"]),
                "selected_candidate_ids": list(
                    dict(value["shard"])["selected_candidate_ids"]
                ),
            }
            for value in sorted(
                values, key=lambda row: int(dict(row["shard"])["shard_index"])
            )
        ],
        "baseline_results": ordered_baseline,
        "profile_results": ordered_profiles,
        "selected_policy_results": ordered_selected,
        "counts": counts,
        "candidate_ids": {
            "g_precontrol_ready": ready_ids,
            "direct_policy_contribution_cleared": contribution_ids,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "next_action": (
            "RUN_TRADE_CONCENTRATION_AND_MATCHED_CONTROLS"
            if ready_ids
            else "TERMINALIZE_CONSISTENCY_AWARE_GOVERNOR_AND_DISPATCH_DISTINCT_BRANCH"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def _design_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    normal5 = _role_horizon_summary(row, "DESIGN", "NORMAL", 5)
    stressed5 = _role_horizon_summary(row, "DESIGN", "STRESSED_1_5X", 5)
    normal10 = _role_horizon_summary(row, "DESIGN", "NORMAL", 10)
    stressed10 = _role_horizon_summary(row, "DESIGN", "STRESSED_1_5X", 10)
    passing_consistency = bool(
        normal5.get("all_passing_paths_consistency_compliant", False)
        and stressed5.get("all_passing_paths_consistency_compliant", False)
    )
    mll = max(
        float(normal5.get("mll_breach_rate", 1.0)),
        float(stressed5.get("mll_breach_rate", 1.0)),
    )
    gate_bits = (
        float(normal5.get("pass_rate", 0.0)) >= 0.05,
        float(stressed5.get("pass_rate", 0.0)) >= 0.02,
        float(normal10.get("pass_rate", 0.0)) >= 0.10,
        float(stressed10.get("pass_rate", 0.0)) >= 0.05,
        float(stressed5.get("net_total", 0.0)) > 0.0,
        mll <= 0.10,
        passing_consistency,
    )
    return (
        sum(gate_bits),
        int(passing_consistency),
        int(mll <= 0.10),
        float(stressed5.get("pass_rate", 0.0)),
        float(normal5.get("pass_rate", 0.0)),
        float(stressed10.get("pass_rate", 0.0)),
        float(stressed5.get("target_progress_p25", 0.0)),
        float(stressed5.get("net_total", 0.0)),
        str(row["policy_id"]),
    )


def _direct_policy_selection_rank(
    row: Mapping[str, Any], identity: Mapping[str, Any]
) -> tuple[Any, ...]:
    """Rank B1/B2 governors by honest contribution before absolute fitness."""

    receipt = _direct_policy_contribution(row, identity)
    gates = dict(receipt["gate_results"])
    return (
        int(all(gates.values())),
        int(gates["direct_policy_no_material_design_degradation_vs_identity"]),
        int(gates["direct_policy_material_design_uplift_vs_identity"]),
        int(gates["direct_policy_behavior_distinct_from_identity_on_b1_b2"]),
        len(receipt["material_improvements"]),
        -len(receipt["material_degradations"]),
        *_design_rank(row),
    )


def _design_selection_receipt(
    chosen: Mapping[str, Any],
    alternatives: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    core = {
        "schema": "hydra_consistency_direct_b1_b2_selection_v1",
        "source_candidate_id": str(chosen["source_candidate_id"]),
        "selected_policy_id": str(chosen["policy_id"]),
        "selected_profile_id": str(chosen["governor_profile_id"]),
        "identity_control_policy_id": str(baseline["policy_id"]),
        "selection_blocks": list(books.DESIGN_BLOCKS),
        "profile_policy_ids": sorted(str(row["policy_id"]) for row in alternatives),
        "design_rank": list(_design_rank(chosen)),
        "direct_contribution_rank": list(
            _direct_policy_selection_rank(chosen, baseline)
        ),
        "identity_is_control_not_promotable_direct_policy": True,
        "b3_b4_fields_used": False,
        "aggregate_summary_fields_used": False,
    }
    return {**core, "selection_hash": stable_hash(core)}


def _control_comparison(
    candidate: Mapping[str, Any], baseline: Mapping[str, Any]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for role in ("DESIGN", "HELD_OUT_DEVELOPMENT"):
        left = _role_horizon_summary(candidate, role, "STRESSED_1_5X", 5)
        right = _role_horizon_summary(baseline, role, "STRESSED_1_5X", 5)
        output[role] = {
            "stressed_5d_pass_rate_delta": float(left.get("pass_rate", 0.0))
            - float(right.get("pass_rate", 0.0)),
            "stressed_5d_p25_progress_delta": float(
                left.get("target_progress_p25", 0.0)
            )
            - float(right.get("target_progress_p25", 0.0)),
            "stressed_5d_net_delta": float(left.get("net_total", 0.0))
            - float(right.get("net_total", 0.0)),
            "stressed_5d_day_share_delta": float(
                left.get("maximum_positive_session_day_aggregate_share", 1.0)
            )
            - float(
                right.get("maximum_positive_session_day_aggregate_share", 1.0)
            ),
        }
    return output


def _direct_policy_contribution(
    candidate: Mapping[str, Any], baseline: Mapping[str, Any]
) -> dict[str, Any]:
    """Require honest B1/B2 marginal value over the identity governor.

    A singleton account governor is not exempt from marginal-contribution
    evidence merely because sleeve membership did not change.  The governor
    must produce a distinct design trajectory, improve at least one material
    account metric, and avoid material degradation of the remaining frozen
    metrics.  Held-out B3/B4 fields are intentionally absent from this receipt.
    """

    if tuple(candidate.get("component_ids") or ()) != tuple(
        baseline.get("component_ids") or ()
    ):
        raise AutonomousConsistencyAccountPolicyError(
            "direct policy and identity control do not share sleeve membership"
        )
    candidate_design = dict(
        dict(candidate.get("summaries_by_role") or {}).get("DESIGN") or {}
    )
    baseline_design = dict(
        dict(baseline.get("summaries_by_role") or {}).get("DESIGN") or {}
    )
    candidate_behavior_hash = stable_hash(
        _design_behavior_projection(candidate_design)
    )
    baseline_behavior_hash = stable_hash(_design_behavior_projection(baseline_design))

    rows: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for scenario in books.SCENARIOS:
        for horizon in (5, 10):
            key = f"{scenario}_{horizon}D"
            rows[key] = (
                _role_horizon_summary(candidate, "DESIGN", scenario, horizon),
                _role_horizon_summary(baseline, "DESIGN", scenario, horizon),
            )

    metric_deltas: dict[str, Any] = {}
    material_improvements: list[str] = []
    material_degradations: list[str] = []
    for key, (left, right) in rows.items():
        pass_count_delta = int(left.get("pass_count", 0)) - int(
            right.get("pass_count", 0)
        )
        pass_rate_delta = float(left.get("pass_rate", 0.0)) - float(
            right.get("pass_rate", 0.0)
        )
        metric_deltas[f"{key}_pass_count"] = pass_count_delta
        metric_deltas[f"{key}_pass_rate"] = pass_rate_delta
        if pass_count_delta > 0:
            material_improvements.append(f"{key}_PASS_COUNT")
        elif pass_count_delta < 0:
            material_degradations.append(f"{key}_PASS_COUNT")

    stressed5, identity_stressed5 = rows["STRESSED_1_5X_5D"]
    target_progress_delta = float(
        stressed5.get("target_progress_p25", 0.0)
    ) - float(identity_stressed5.get("target_progress_p25", 0.0))
    net_delta = float(stressed5.get("net_total", 0.0)) - float(
        identity_stressed5.get("net_total", 0.0)
    )
    net_materiality = max(
        MATERIAL_NET_DELTA_FLOOR_USD,
        abs(float(identity_stressed5.get("net_total", 0.0)))
        * MATERIAL_NET_DELTA_FRACTION,
    )
    mll_delta = float(stressed5.get("mll_breach_rate", 1.0)) - float(
        identity_stressed5.get("mll_breach_rate", 1.0)
    )
    day_share_delta = float(
        stressed5.get("maximum_positive_session_day_aggregate_share", 1.0)
    ) - float(
        identity_stressed5.get(
            "maximum_positive_session_day_aggregate_share", 1.0
        )
    )
    metric_deltas.update(
        {
            "STRESSED_1_5X_5D_target_progress_p25": target_progress_delta,
            "STRESSED_1_5X_5D_net_usd": net_delta,
            "STRESSED_1_5X_5D_mll_breach_rate": mll_delta,
            "STRESSED_1_5X_5D_positive_day_share": day_share_delta,
        }
    )
    if target_progress_delta >= MATERIAL_TARGET_PROGRESS_DELTA:
        material_improvements.append("STRESSED_1_5X_5D_TARGET_PROGRESS_P25")
    elif target_progress_delta < -MATERIAL_TARGET_PROGRESS_DELTA:
        material_degradations.append("STRESSED_1_5X_5D_TARGET_PROGRESS_P25")
    if net_delta >= net_materiality:
        material_improvements.append("STRESSED_1_5X_5D_NET")
    elif net_delta < -net_materiality:
        material_degradations.append("STRESSED_1_5X_5D_NET")
    if mll_delta <= -MATERIAL_MLL_RATE_DELTA:
        material_improvements.append("STRESSED_1_5X_5D_MLL")
    elif mll_delta > MATERIAL_MLL_RATE_DELTA:
        material_degradations.append("STRESSED_1_5X_5D_MLL")
    gates = {
        "direct_policy_behavior_distinct_from_identity_on_b1_b2": (
            candidate_behavior_hash != baseline_behavior_hash
        ),
        "direct_policy_material_design_uplift_vs_identity": bool(
            material_improvements
        ),
        "direct_policy_no_material_design_degradation_vs_identity": not bool(
            material_degradations
        ),
    }
    core = {
        "schema": "hydra_consistency_direct_identity_contribution_v1",
        "source_candidate_id": str(candidate.get("source_candidate_id") or ""),
        "candidate_policy_id": str(candidate["policy_id"]),
        "identity_control_policy_id": str(baseline["policy_id"]),
        "design_blocks": list(books.DESIGN_BLOCKS),
        "held_out_fields_used": False,
        "candidate_design_behavior_hash": candidate_behavior_hash,
        "identity_design_behavior_hash": baseline_behavior_hash,
        "materiality_thresholds": {
            "target_progress_p25_delta": MATERIAL_TARGET_PROGRESS_DELTA,
            "net_delta_floor_usd": MATERIAL_NET_DELTA_FLOOR_USD,
            "net_delta_fraction_of_identity": MATERIAL_NET_DELTA_FRACTION,
            "realized_net_delta_threshold_usd": net_materiality,
            "mll_breach_rate_delta": MATERIAL_MLL_RATE_DELTA,
            "pass_count_delta": 1,
        },
        "metric_deltas": metric_deltas,
        "diagnostics": {
            "positive_session_day_aggregate_share_delta": day_share_delta,
            "qualification": (
                "OVERLAPPING_START_AGGREGATE_NOT_INDEPENDENT_CONCENTRATION"
            ),
            "used_for_material_uplift": False,
            "used_for_material_degradation": False,
            "independent_concentration_control": "DEFERRED",
        },
        "material_improvements": sorted(material_improvements),
        "material_degradations": sorted(material_degradations),
        "gate_results": gates,
    }
    return {**core, "contribution_hash": stable_hash(core)}


def _design_behavior_projection(
    design: Mapping[str, Any],
) -> dict[str, Any]:
    """Exclude the overlapping-start day-share diagnostic from behavior gates."""

    output: dict[str, Any] = {}
    for scenario, raw_horizons in design.items():
        horizons: dict[str, Any] = {}
        for horizon, raw_summary in dict(raw_horizons or {}).items():
            summary = dict(raw_summary or {})
            summary.pop("maximum_positive_session_day_aggregate_share", None)
            horizons[str(horizon)] = summary
        output[str(scenario)] = horizons
    return output


def _overlapping_start_day_share_diagnostic(
    row: Mapping[str, Any], *, legacy_gate_result: Any
) -> dict[str, Any]:
    normal = _role_horizon_summary(
        row, "HELD_OUT_DEVELOPMENT", "NORMAL", 5
    )
    stressed = _role_horizon_summary(
        row, "HELD_OUT_DEVELOPMENT", "STRESSED_1_5X", 5
    )
    core = {
        "schema": "hydra_overlapping_start_day_share_diagnostic_v1",
        "role": "HELD_OUT_DEVELOPMENT",
        "horizon_trading_days": 5,
        "normal_maximum_positive_session_day_aggregate_share": float(
            normal.get("maximum_positive_session_day_aggregate_share", 0.0)
        ),
        "stressed_maximum_positive_session_day_aggregate_share": float(
            stressed.get("maximum_positive_session_day_aggregate_share", 0.0)
        ),
        "legacy_gate_result": legacy_gate_result,
        "qualification": (
            "OVERLAPPING_START_AGGREGATE_NOT_INDEPENDENT_CONCENTRATION"
        ),
        "used_for_profile_selection": False,
        "used_for_direct_policy_contribution": False,
        "used_for_g_precontrol": False,
        "independent_concentration_control": "DEFERRED",
    }
    return {**core, "diagnostic_hash": stable_hash(core)}


def _verify_shard(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    if row.get("schema") != SCHEMA or row.get("status") != (
        "COMPLETE_BOUNDED_CONSISTENCY_DIRECT_ACCOUNT_SHARD"
    ):
        raise AutonomousConsistencyAccountPolicyError(
            "consistency-policy shard schema/status mismatch"
        )
    expected = stable_hash({key: item for key, item in row.items() if key != "result_hash"})
    if str(row.get("result_hash")) != expected:
        raise AutonomousConsistencyAccountPolicyError(
            "consistency-policy shard result hash drift"
        )
    counts = dict(row.get("counts") or {})
    if any(
        int(counts.get(field, -1)) != 0
        for field in (
            "authoritative_promotion_count",
            "xfa_paths_started",
            "registry_writes",
            "database_writes",
            "broker_connections",
            "orders",
        )
    ):
        raise AutonomousConsistencyAccountPolicyError(
            "read-only consistency-policy safety invariant failed"
        )
    return row


def _unique_rows(
    shards: Sequence[Mapping[str, Any]], field: str, identity: str
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for shard in shards:
        for raw in shard[field]:
            row = dict(raw)
            key = str(row[identity])
            previous = output.get(key)
            if previous is not None and stable_hash(previous) != stable_hash(row):
                raise AutonomousConsistencyAccountPolicyError(
                    f"consistency-policy row collision: {field}/{key}"
                )
            output[key] = row
    return output


def _empty_result(
    *,
    bank: Mapping[str, Any],
    composite: Mapping[str, Any],
    tier_q_rows: Sequence[Mapping[str, Any]],
    maximum_candidates: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    inventory_hash = stable_hash([])
    core = {
        "schema": SCHEMA,
        "status": "NO_BOUNDED_CONSISTENCY_DIRECT_ACCOUNT_SHARD",
        "reason": "NO_ADMISSIBLE_TIER_Q_CANDIDATE",
        "source_candidate_bank_hash": str(bank["result_hash"]),
        "source_composite_result_hash": str(composite["result_hash"]),
        "tier_q_component_ids": sorted(
            str(row["candidate_id"]) for row in tier_q_rows
        ),
        "maximum_candidates": int(maximum_candidates),
        "shard": {
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
            "candidate_inventory_count": 0,
            "candidate_inventory_hash": inventory_hash,
            "candidate_inventory_ids": [],
            "selected_candidate_ids": [],
            "ranking_recomputed_after_sharding": False,
        },
        "baseline_results": [],
        "profile_results": [],
        "selected_policy_results": [],
        "counts": {
            "tier_q_input_count": len(tier_q_rows),
            "direct_policy_exact_replay_count": 0,
            "identity_control_exact_replay_count": 0,
            "g_precontrol_ready_count": 0,
            "direct_policy_contribution_cleared_count": 0,
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "candidate_ids": {
            "g_precontrol_ready": [],
            "direct_policy_contribution_cleared": [],
        },
        "promotion_status": None,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
    }
    return {**core, "result_hash": stable_hash(core)}


__all__ = [
    "AutonomousConsistencyAccountPolicyError",
    "COMPOSITE_SCHEMA",
    "MAXIMUM_CANDIDATES",
    "MAXIMUM_PROFILES",
    "SCHEMA",
    "build_autonomous_consistency_account_policies",
    "compose_autonomous_consistency_account_policy_shards",
    "frozen_consistency_profiles",
]
