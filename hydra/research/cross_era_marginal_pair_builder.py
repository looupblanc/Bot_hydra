"""Bounded marginal pair builder for the W2019H1 cross-era sieve.

The upstream sieve produced seven standalone policies that passed its frozen
development-only admission rule, but only four distinct representatives after
behavioural clustering.  This module runs one materially different experiment:
exact shared-account assembly of every pair of those seven components under
the already-frozen FAST-PASS governor frontier.  It does not mutate signals,
fills, component quantities, calibrations, or the W2019 evidence role.
"""

from __future__ import annotations

import argparse
import itertools
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    _load_self_hashed_manifest,
    _market_contract_limit_mini,
    _require_scenario_identity,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.fast_pass_runtime_helpers import _governor_profiles
from hydra.research.cross_era_bank_sieve import (
    ACCOUNT_LABELS,
    COHORT_NAME,
    CONTRACT_NAME,
    FEATURE_NAME,
    HORIZONS,
    OUTPUT_DIR,
    PROFILE_BY_ID,
    ROOT,
    CrossEraSieveError,
    _calendar,
    _component_replay,
    _open_features,
    _read,
    _write,
)
from hydra.research.causal_target_velocity import HazardCandidate
from hydra.research.pnl_state_risk_frontier import _PreparedPolicy, _evaluate_profile


SCHEMA = "hydra_cross_era_marginal_pair_builder_2019h1_v1"
PAIR_DIR_NAME = "marginal_pair_builder"
PAIR_CONTRACT_NAME = "pair_selection_contract.json"
PAIR_RESULT_NAME = "pair_economic_result.json"
COMBINED_COHORT_NAME = "combined_development_requalified_cohort.json"
W2018_CONTRACT_NAME = "w2018h1_one_shot_confirmation_contract.json"
STATE_NAME = "production_state.json"
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
MAXIMUM_RETAINED_PAIRS = 8
MINIMUM_COMBINED_COHORT = 8
MAXIMUM_COMBINED_COHORT = 12

# Frozen before pair outcomes.  These tolerances define *material* degradation
# rather than rewarding noisy basis-point changes in a tiny viewed packet.
P25_DEGRADATION_TOLERANCE = 0.01
CONSISTENCY_DEGRADATION_TOLERANCE = 0.05
MLL_BUFFER_DEGRADATION_FRACTION = 0.10
P25_STRICT_IMPROVEMENT = 0.01
CONSISTENCY_STRICT_IMPROVEMENT = 0.05
MLL_BUFFER_STRICT_IMPROVEMENT_FRACTION = 0.10


class CrossEraMarginalPairError(RuntimeError):
    """The bounded pair-builder contract cannot be satisfied exactly."""


def _pair_output_dir(base_output_dir: Path) -> Path:
    return base_output_dir / PAIR_DIR_NAME


def _eligible_inventory(
    source_contract: Mapping[str, Any], cohort: Mapping[str, Any]
) -> list[dict[str, Any]]:
    config_by_id = {
        str(row["configuration_id"]): dict(row)
        for row in source_contract["configurations"]
        if row.get("replay_eligible")
    }
    eligible = [
        dict(row)
        for row in cohort["all_candidate_decisions"]
        if row.get("eligible_for_requalification")
    ]
    eligible.sort(key=lambda row: tuple(row["selected_cell"]["rank"]), reverse=True)
    if len(eligible) != 7:
        raise CrossEraMarginalPairError("expected exactly seven eligible W2019 bases")
    behavior_hashes = {
        str(row["realized_behavior_hash"])
        for row in eligible
    }
    if len(behavior_hashes) != len(eligible):
        raise CrossEraMarginalPairError("eligible sources are not behaviorally distinct")
    output = []
    for rank, decision in enumerate(eligible):
        policy_id = str(decision["configuration_id"])
        source = config_by_id.get(policy_id)
        if source is None:
            raise CrossEraMarginalPairError(f"eligible source config absent: {policy_id}")
        if (
            source.get("record_type") != "BASE_POLICY"
            or source.get("source_kind") != "EXACT_STANDALONE"
            or list(source.get("component_ids") or ()) != [policy_id]
            or str(dict(source["pnl_state_profile"])["profile_id"])
            != "pnl_state_identity"
        ):
            raise CrossEraMarginalPairError(
                f"pair input escaped immutable standalone identity scope: {policy_id}"
            )
        component = dict(source_contract["components"])[policy_id]
        candidate = HazardCandidate(**dict(component["candidate"]))
        if (
            candidate.candidate_id != policy_id
            or candidate.structural_fingerprint
            != str(component["candidate_fingerprint"])
        ):
            raise CrossEraMarginalPairError(
                f"candidate structural identity drift: {policy_id}"
            )
        source_policy = ActiveRiskPoolPolicy.from_mapping(
            dict(source["frozen_account_policy"])
        )
        component_freeze_hash = stable_hash(
            {
                "candidate": component["candidate"],
                "candidate_fingerprint": component["candidate_fingerprint"],
                "calibration": component["calibration"],
                "quantity": dict(source["component_quantity_tiers"])[policy_id],
                "nominal_risk_charge": source_policy.nominal_risk_charge_map[
                    policy_id
                ],
            }
        )
        output.append(
            {
                "priority_rank": rank,
                "configuration_id": policy_id,
                "source_configuration": source,
                "source_result_hash": decision["result_hash"],
                "source_behavior_cluster_id": decision[
                    "realized_behavior_cluster_id"
                ],
                "old_development_best_stressed_progress": float(
                    decision["old_development_best_stressed_progress"]
                ),
                "w2019_selected_cell": decision["selected_cell"],
                "niches": decision["niches"],
                "component_freeze_hash": component_freeze_hash,
            }
        )
    return output


def freeze_pair_contract(base_output_dir: Path) -> dict[str, Any]:
    pair_dir = _pair_output_dir(base_output_dir)
    source_contract = _read(base_output_dir / CONTRACT_NAME)
    cohort = _read(base_output_dir / COHORT_NAME)
    feature_receipt = _read(base_output_dir / FEATURE_NAME)
    if cohort.get("evidence_role") != "VIEWED_DEVELOPMENT_ONLY":
        raise CrossEraMarginalPairError("W2019 evidence role drift")
    inventory = _eligible_inventory(source_contract, cohort)
    ordered_ids = [str(row["configuration_id"]) for row in inventory]
    source_by_id = {
        str(row["configuration_id"]): dict(row["source_configuration"])
        for row in inventory
    }
    components = {
        str(key): dict(value) for key, value in source_contract["components"].items()
    }

    manifest = _load_self_hashed_manifest(ROOT / DEFAULT_FAST_PASS_MANIFEST)
    profiles = _governor_profiles(manifest)
    profile_payloads = [asdict(row) for row in profiles]
    pair_members = [tuple(value) for value in itertools.combinations(ordered_ids, 2)]
    if len(pair_members) != 21:
        raise CrossEraMarginalPairError("seven-source pair frontier is not 21")

    policies = []
    for pair in pair_members:
        quantities = {
            member: int(
                dict(source_by_id[member]["component_quantity_tiers"])[member]
            )
            for member in pair
        }
        source_charges = {}
        for member in pair:
            policy = ActiveRiskPoolPolicy.from_mapping(
                dict(source_by_id[member]["frozen_account_policy"])
            )
            source_charges[member] = float(policy.nominal_risk_charge_map[member])
        pair_context_hash = stable_hash({"comparison_pair_members": list(pair)})
        for profile in profiles:
            for account_label in ACCOUNT_LABELS:
                rule = dict(source_contract["account_rules"])[account_label]
                shared_contract_cap = min(
                    _market_contract_limit_mini(
                        dict(components[member]["candidate"]), rule
                    )
                    for member in pair
                )
                if shared_contract_cap <= 0.0:
                    raise CrossEraMarginalPairError(
                        f"pair has no legal shared contract capacity: {pair}/{account_label}"
                    )
                for role, members in (
                    ("PAIR", pair),
                    ("MATCHED_PARENT", (pair[0],)),
                    ("MATCHED_PARENT", (pair[1],)),
                ):
                    semantic = {
                        "component_ids": list(members),
                        # Pair and parents share the exact union-availability
                        # window.  This prevents a marginal delta from being a
                        # hidden difference in full-coverage starts.
                        "availability_component_ids": list(pair),
                        "comparison_pair_members": list(pair),
                        "comparison_pair_context_hash": pair_context_hash,
                        "component_priority": list(members),
                        "component_quantity_tiers": {
                            member: quantities[member] for member in members
                        },
                        "component_nominal_risk_charges": {
                            member: source_charges[member] for member in members
                        },
                        "governor_profile": asdict(profile),
                        "account_label": account_label,
                        "maximum_mini_equivalent": shared_contract_cap,
                        "pnl_state_profile_id": "pnl_state_identity",
                        "book_static_risk_tier": 1.0,
                        "signal_or_trade_logic_mutated": False,
                    }
                    policies.append(
                        {
                            **semantic,
                            "policy_role": role,
                            "policy_id": f"w2019_pair_{stable_hash(semantic)[:24]}",
                            "policy_spec_hash": stable_hash(semantic),
                        }
                    )

    core = {
        "schema": f"{SCHEMA}_selection_contract",
        "status": "FROZEN_BEFORE_MARGINAL_PAIR_OUTCOMES",
        "evidence_role": "W2019H1_VIEWED_DEVELOPMENT_ONLY",
        "source_sieve_contract_hash": source_contract["contract_hash"],
        "source_sieve_cohort_hash": cohort["result_hash"],
        "source_feature_receipt_hash": feature_receipt["result_hash"],
        "eligible_source_count": len(inventory),
        "eligible_source_ids_in_frozen_priority_order": ordered_ids,
        "eligible_inventory": inventory,
        "source_retained_standalones": cohort["retained"],
        "pair_count": len(pair_members),
        "matched_parent_policy_count": len(pair_members)
        * 2
        * len(profiles)
        * len(ACCOUNT_LABELS),
        "pair_policy_count": len(pair_members) * len(profiles) * len(ACCOUNT_LABELS),
        "policies": policies,
        "governor_profiles": profile_payloads,
        "governor_profile_hash": stable_hash(profile_payloads),
        "account_labels": list(ACCOUNT_LABELS),
        "horizons_trading_days": list(HORIZONS),
        "scenarios": list(SCENARIOS),
        "account_rules": source_contract["account_rules"],
        "components": source_contract["components"],
        "selection_rule": {
            "benchmark": (
                "STRONGER_MATCHED_SINGLETON_PARENT_SAME_PROFILE_ACCOUNT_HORIZON_"
                "AND_PAIR_COMMON_FULL_COVERAGE_WINDOWS"
            ),
            "hard_requirements": [
                "POSITIVE_STRESSED_NET",
                "ZERO_NORMAL_AND_STRESSED_MLL_BREACH",
                "NORMAL_PASS_OR_POSITIVE_STRESSED_MEDIAN_PROGRESS",
                "NO_PASS_COUNT_DEGRADATION",
                "NO_MATERIAL_LOWER_QUARTILE_PROGRESS_DEGRADATION",
                "NO_MATERIAL_MLL_BUFFER_DEGRADATION",
                "NO_MATERIAL_CONSISTENCY_DEGRADATION",
                "STRICT_IMPROVEMENT_ON_PASS_LOWER_QUARTILE_MLL_OR_CONSISTENCY",
            ],
            "p25_degradation_tolerance": P25_DEGRADATION_TOLERANCE,
            "consistency_degradation_tolerance": CONSISTENCY_DEGRADATION_TOLERANCE,
            "mll_buffer_degradation_fraction_of_account_mll": MLL_BUFFER_DEGRADATION_FRACTION,
            "p25_strict_improvement": P25_STRICT_IMPROVEMENT,
            "consistency_strict_improvement": CONSISTENCY_STRICT_IMPROVEMENT,
            "mll_buffer_strict_improvement_fraction_of_account_mll": MLL_BUFFER_STRICT_IMPROVEMENT_FRACTION,
            "maximum_retained_pairs": MAXIMUM_RETAINED_PAIRS,
            "combined_cohort_minimum": MINIMUM_COMBINED_COHORT,
            "combined_cohort_maximum": MAXIMUM_COMBINED_COHORT,
            "one_primary_per_realized_account_behavior": True,
            "maximum_retained_pairs_per_component": 3,
            "selected_status": "DEVELOPMENT_REQUALIFIED",
            "confirmation_window": "W2018H1_UNTOUCHED_AND_NOT_OPENED_BY_THIS_RUN",
        },
        "worker_processes": 2,
        "numeric_threads_per_worker": 1,
        "new_strategy_grammar": False,
        "new_parameter_thresholds": False,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
    }
    contract = {**core, "contract_hash": stable_hash(core)}
    _write(pair_dir / PAIR_CONTRACT_NAME, contract, immutable=True)
    return contract


def _active_policy(
    spec: Mapping[str, Any], rule: Mapping[str, Any]
) -> ActiveRiskPoolPolicy:
    members = tuple(str(value) for value in spec["component_ids"])
    profile = dict(spec["governor_profile"])
    mll = float(rule["maximum_loss_limit_usd"])
    target = float(rule["profit_target_usd"])
    open_fraction = float(profile["open_risk_ceiling_fraction"])
    return ActiveRiskPoolPolicy(
        policy_id=str(spec["policy_id"]),
        component_priority=members,
        nominal_risk_charge_per_mini=tuple(
            (member, float(dict(spec["component_nominal_risk_charges"])[member]))
            for member in members
        ),
        maximum_concurrent_sleeves=min(
            int(profile["maximum_concurrent_sleeves"]), len(members)
        ),
        aggregate_open_risk_ceiling=mll * open_fraction,
        maximum_mll_buffer_fraction=open_fraction,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(spec["maximum_mini_equivalent"]),
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=mll * float(profile["daily_loss_budget_fraction"]),
        daily_consistency_profit_guard=(
            target
            * float(rule["consistency_target_fraction"])
            * float(profile["daily_profit_lock_fraction"])
        ),
        target_protection_distance=(
            target * (1.0 - float(profile["target_protection_fraction"]))
        ),
        target_protection_mode=TargetProtectionMode.SCALE_50,
        static_risk_tier=1.0,
    )


def _worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    os.environ.update(
        {
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        }
    )
    pair_contract = _read(Path(str(payload["pair_contract_path"])))
    source_contract = _read(Path(str(payload["source_contract_path"])))
    feature_receipt = _read(Path(str(payload["feature_receipt_path"])))
    matrices = _open_features(feature_receipt)
    calendar = _calendar(matrices)
    from hydra.production import fresh_confirmation_lane as lane

    starts = lane.non_overlapping_starts(calendar, HORIZONS)
    starts = {
        horizon: tuple((day, "W2019H1_VIEWED_DEVELOPMENT") for day, _ in values)
        for horizon, values in starts.items()
    }
    components = {
        str(key): dict(value) for key, value in source_contract["components"].items()
    }
    cache: dict[str, dict[str, Any]] = {}
    results = []
    exact_episodes = 0
    identity = PROFILE_BY_ID["pnl_state_identity"]

    for raw in payload["policies"]:
        spec = dict(raw)
        members = [str(value) for value in spec["component_ids"]]
        availability_members = [
            str(value) for value in spec["availability_component_ids"]
        ]
        for member in availability_members:
            if member not in cache:
                _replay, materialized = _component_replay(
                    components[member], matrices, calendar
                )
                cache[member] = materialized
        trajectories: dict[str, dict[str, tuple[Any, ...]]] = {
            "NORMAL": {},
            "STRESSED_1_5X": {},
        }
        unavailable: set[int] = set()
        component_receipts = []
        for member in members:
            value = cache[member]
            quantity = int(dict(spec["component_quantity_tiers"])[member])
            normal = tuple(
                scale_causal_trajectory(row, executable_quantity_multiplier=quantity)
                for row in value["normal"]
            )
            stressed = tuple(
                scale_causal_trajectory(row, executable_quantity_multiplier=quantity)
                for row in value["stressed"]
            )
            _require_scenario_identity(normal, stressed)
            trajectories["NORMAL"][member] = normal
            trajectories["STRESSED_1_5X"][member] = stressed
            unavailable.update(value["unavailable_days"])
            component_receipts.append(
                {
                    "candidate_id": member,
                    "quantity_tier": quantity,
                    "normal_trajectory_hash": value["normal_trajectory_hash"],
                    "stressed_trajectory_hash": value["stressed_trajectory_hash"],
                    "decision_hash": value["decision_hash"],
                    "fill_policy_hash": value["fill_policy_hash"],
                }
            )
        for member in availability_members:
            unavailable.update(cache[member]["unavailable_days"])
        account_label = str(spec["account_label"])
        rule = dict(pair_contract["account_rules"])[account_label]
        prepared = _PreparedPolicy(
            policy_id=str(spec["policy_id"]),
            source_kind=str(spec["policy_role"]),
            evidence_tier="DEVELOPMENT_REQUALIFIED_INPUT",
            account_label=account_label,
            baseline_policy=_active_policy(spec, rule),
            trajectories=trajectories,
            unavailable_days=frozenset(unavailable),
            source_policy=spec,
            source_metrics={},
            source_hashes={},
        )
        evaluation = _evaluate_profile(
            prepared,
            identity,
            blocks=("W2019H1_VIEWED_DEVELOPMENT",),
            calendar=calendar,
            starts=starts,
            rule=rule,
        )
        exact_episodes += int(evaluation["exact_episode_count"])
        core = {
            "policy_id": spec["policy_id"],
            "policy_role": spec["policy_role"],
            "policy_spec_hash": spec["policy_spec_hash"],
            "component_ids": members,
            "availability_component_ids": availability_members,
            "comparison_pair_members": spec["comparison_pair_members"],
            "comparison_pair_context_hash": spec["comparison_pair_context_hash"],
            "component_receipts": component_receipts,
            "governor_profile_id": dict(spec["governor_profile"])["profile_id"],
            "account_label": account_label,
            "evaluation": evaluation,
            "signal_or_trade_logic_mutated": False,
            "evidence_role": "W2019H1_VIEWED_DEVELOPMENT_ONLY",
            "promotion_status": None,
        }
        results.append({**core, "result_hash": stable_hash(core)})
    return {
        "results": results,
        "exact_account_episode_count": exact_episodes,
        "component_replay_count": len(cache),
        "calendar": list(calendar),
        "start_counts": {str(key): len(value) for key, value in starts.items()},
    }


def _cell(summary: Mapping[str, Any], scenario: str, horizon: int) -> dict[str, Any]:
    return dict(dict(summary["evaluation"])["summaries"][scenario][str(horizon)])


def _parent_rank(result: Mapping[str, Any], horizon: int) -> tuple[Any, ...]:
    normal = _cell(result, "NORMAL", horizon)
    stressed = _cell(result, "STRESSED_1_5X", horizon)
    return (
        int(stressed["pass_count"]),
        int(normal["pass_count"]),
        float(stressed["target_progress_p25"]),
        float(stressed["target_progress_median"]),
        float(stressed["net_total_usd"]),
        float(stressed["minimum_mll_buffer_usd"] or -1e18),
        float(stressed["consistency_compliance_rate"]),
    )


def _marginal_cell(
    pair: Mapping[str, Any],
    parents: Sequence[Mapping[str, Any]],
    *,
    horizon: int,
    rule: Mapping[str, Any],
    old_progress_floor: float,
) -> dict[str, Any] | None:
    benchmark = max(parents, key=lambda row: _parent_rank(row, horizon))
    normal = _cell(pair, "NORMAL", horizon)
    stressed = _cell(pair, "STRESSED_1_5X", horizon)
    parent_normal = _cell(benchmark, "NORMAL", horizon)
    parent_stressed = _cell(benchmark, "STRESSED_1_5X", horizon)
    if min(
        int(normal["full_coverage_start_count"]),
        int(stressed["full_coverage_start_count"]),
    ) <= 0:
        return None
    hard_safe = (
        int(normal["mll_breach_count"]) == 0
        and int(stressed["mll_breach_count"]) == 0
    )
    positive = float(stressed["net_total_usd"]) > 0.0
    useful = (
        int(normal["pass_count"]) > 0
        or float(stressed["target_progress_median"]) > 0.0
    )
    mll = float(rule["maximum_loss_limit_usd"])
    deltas = {
        "normal_pass_count": int(normal["pass_count"])
        - int(parent_normal["pass_count"]),
        "stressed_pass_count": int(stressed["pass_count"])
        - int(parent_stressed["pass_count"]),
        "normal_p25_progress": float(normal["target_progress_p25"])
        - float(parent_normal["target_progress_p25"]),
        "stressed_p25_progress": float(stressed["target_progress_p25"])
        - float(parent_stressed["target_progress_p25"]),
        "normal_consistency": float(normal["consistency_compliance_rate"])
        - float(parent_normal["consistency_compliance_rate"]),
        "stressed_consistency": float(stressed["consistency_compliance_rate"])
        - float(parent_stressed["consistency_compliance_rate"]),
        "normal_minimum_mll_buffer_usd": float(normal["minimum_mll_buffer_usd"])
        - float(parent_normal["minimum_mll_buffer_usd"]),
        "stressed_minimum_mll_buffer_usd": float(stressed["minimum_mll_buffer_usd"])
        - float(parent_stressed["minimum_mll_buffer_usd"]),
        "stressed_net_usd": float(stressed["net_total_usd"])
        - float(parent_stressed["net_total_usd"]),
        "stressed_median_progress": float(stressed["target_progress_median"])
        - float(parent_stressed["target_progress_median"]),
    }
    no_material_degradation = (
        deltas["normal_pass_count"] >= 0
        and deltas["stressed_pass_count"] >= 0
        and deltas["normal_p25_progress"] >= -P25_DEGRADATION_TOLERANCE
        and deltas["stressed_p25_progress"] >= -P25_DEGRADATION_TOLERANCE
        and deltas["normal_consistency"] >= -CONSISTENCY_DEGRADATION_TOLERANCE
        and deltas["stressed_consistency"] >= -CONSISTENCY_DEGRADATION_TOLERANCE
        and deltas["normal_minimum_mll_buffer_usd"]
        >= -(MLL_BUFFER_DEGRADATION_FRACTION * mll)
        and deltas["stressed_minimum_mll_buffer_usd"]
        >= -(MLL_BUFFER_DEGRADATION_FRACTION * mll)
    )
    strict_improvement = (
        deltas["normal_pass_count"] > 0
        or deltas["stressed_pass_count"] > 0
        or deltas["normal_p25_progress"] >= P25_STRICT_IMPROVEMENT
        or deltas["stressed_p25_progress"] >= P25_STRICT_IMPROVEMENT
        or deltas["normal_consistency"] >= CONSISTENCY_STRICT_IMPROVEMENT
        or deltas["stressed_consistency"] >= CONSISTENCY_STRICT_IMPROVEMENT
        or deltas["normal_minimum_mll_buffer_usd"]
        >= MLL_BUFFER_STRICT_IMPROVEMENT_FRACTION * mll
        or deltas["stressed_minimum_mll_buffer_usd"]
        >= MLL_BUFFER_STRICT_IMPROVEMENT_FRACTION * mll
    )
    if not (
        hard_safe
        and positive
        and useful
        and no_material_degradation
        and strict_improvement
    ):
        return None
    rank = (
        int(stressed["pass_count"]),
        int(normal["pass_count"]),
        min(float(old_progress_floor), float(stressed["target_progress_median"])),
        float(stressed["target_progress_p25"]),
        float(stressed["target_progress_median"]),
        float(stressed["net_total_usd"]),
        float(stressed["minimum_mll_buffer_usd"]),
        -horizon,
        -int(str(pair["account_label"]).removesuffix("K")),
    )
    return {
        "horizon_trading_days": horizon,
        "account_label": pair["account_label"],
        "governor_profile_id": pair["governor_profile_id"],
        "normal": normal,
        "stressed": stressed,
        "benchmark_parent_policy_id": benchmark["policy_id"],
        "marginal_deltas": deltas,
        "hard_safe": hard_safe,
        "positive_stressed_net": positive,
        "useful_progress_or_pass": useful,
        "no_material_degradation": no_material_degradation,
        "strict_marginal_improvement": strict_improvement,
        "old_component_stability_floor": old_progress_floor,
        "rank": list(rank),
    }


def _select(
    contract: Mapping[str, Any], results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    indexed = {
        (
            tuple(str(value) for value in row["comparison_pair_members"]),
            tuple(str(value) for value in row["component_ids"]),
            str(row["governor_profile_id"]),
            str(row["account_label"]),
        ): dict(row)
        for row in results
    }
    inventory = {
        str(row["configuration_id"]): dict(row)
        for row in contract["eligible_inventory"]
    }
    pair_decisions = []
    for pair in results:
        members = tuple(str(value) for value in pair["component_ids"])
        if len(members) != 2:
            continue
        profile_id = str(pair["governor_profile_id"])
        account_label = str(pair["account_label"])
        parents = [
            indexed[(members, (member,), profile_id, account_label)]
            for member in members
        ]
        old_floor = min(
            float(inventory[member]["old_development_best_stressed_progress"])
            for member in members
        )
        cells = [
            value
            for horizon in HORIZONS
            if (
                value := _marginal_cell(
                    pair,
                    parents,
                    horizon=horizon,
                    rule=dict(contract["account_rules"])[account_label],
                    old_progress_floor=old_floor,
                )
            )
            is not None
        ]
        pair_decisions.append(
            {
                "policy_id": pair["policy_id"],
                "policy_spec_hash": pair["policy_spec_hash"],
                "component_ids": list(members),
                "governor_profile_id": profile_id,
                "account_label": account_label,
                "selected_cell": (
                    max(cells, key=lambda row: tuple(row["rank"])) if cells else None
                ),
                "eligible": bool(cells),
                "result_hash": pair["result_hash"],
            }
        )

    eligible_cells = [row for row in pair_decisions if row["eligible"]]
    # One executable pair has twelve account/profile cells.  Preserve only its
    # best cell before clustering to prevent profile multiplicity from filling
    # the cohort.
    best_by_members: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in eligible_cells:
        members = tuple(row["component_ids"])
        prior = best_by_members.get(members)
        if prior is None or tuple(row["selected_cell"]["rank"]) > tuple(
            prior["selected_cell"]["rank"]
        ):
            best_by_members[members] = row
    ranked = sorted(
        best_by_members.values(),
        key=lambda row: tuple(row["selected_cell"]["rank"]),
        reverse=True,
    )
    retained = []
    seen_behavior: set[str] = set()
    component_counts: dict[str, int] = {}
    result_by_id = {str(row["policy_id"]): dict(row) for row in results}
    for row in ranked:
        result = result_by_id[str(row["policy_id"])]
        selected = dict(row["selected_cell"])
        behavior_payload = {
            "component_trade_hashes": sorted(
                str(value["stressed_trajectory_hash"])
                for value in result["component_receipts"]
            ),
            "normal_episode_path_hash": selected["normal"]["episode_path_hash"],
            "stressed_episode_path_hash": selected["stressed"]["episode_path_hash"],
        }
        behavior_hash = stable_hash(behavior_payload)
        members = [str(value) for value in row["component_ids"]]
        if behavior_hash in seen_behavior or any(
            component_counts.get(member, 0) >= 3 for member in members
        ):
            continue
        retained.append(
            {
                **row,
                "realized_behavior_cluster_id": f"w2019_pair_{behavior_hash[:16]}",
                "realized_behavior_hash": behavior_hash,
                "status": "DEVELOPMENT_REQUALIFIED",
                "independent_confirmation_claimed": False,
                "future_confirmation_window": "W2018H1_UNTOUCHED_PENDING_FREEZE",
            }
        )
        seen_behavior.add(behavior_hash)
        for member in members:
            component_counts[member] = component_counts.get(member, 0) + 1
        if len(retained) == MAXIMUM_RETAINED_PAIRS:
            break

    standalone = list(contract["source_retained_standalones"])
    capacity = max(0, MAXIMUM_COMBINED_COHORT - len(standalone))
    retained = retained[:capacity]
    combined = standalone + retained
    sufficient = len(combined) >= MINIMUM_COMBINED_COHORT
    core = {
        "schema": f"{SCHEMA}_combined_cohort",
        "status": (
            "CROSS_ERA_COHORT_SUFFICIENT_FOR_W2018_FREEZE"
            if sufficient
            else "MARGINAL_PAIR_ASSEMBLY_INSUFFICIENT_COHORT"
        ),
        "contract_hash": contract["contract_hash"],
        "evaluated_pair_cell_count": len(pair_decisions),
        "eligible_pair_cell_count": len(eligible_cells),
        "eligible_component_pair_count": len(best_by_members),
        "retained_pair_count": len(retained),
        "retained_pairs": retained,
        "source_standalone_count": len(standalone),
        "combined_count": len(combined),
        "combined": combined,
        "minimum_required": MINIMUM_COMBINED_COHORT,
        "cohort_sufficient": sufficient,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "selected_status": "DEVELOPMENT_REQUALIFIED",
        "promotion_status": None,
        "w2018_access_count_delta": 0,
        "next_action": (
            "FREEZE_COMBINED_COHORT_FOR_ONE_SHOT_W2018H1_CONFIRMATION"
            if sufficient
            else "CLOSE_PAIR_ASSEMBLY_AND_START_MATERIALLY_DISTINCT_BEST_EVI_BRANCH"
        ),
        "all_pair_cell_decisions": pair_decisions,
    }
    return {**core, "result_hash": stable_hash(core)}


def run_pair_builder(base_output_dir: Path) -> dict[str, Any]:
    started = time.perf_counter()
    pair_dir = _pair_output_dir(base_output_dir)
    contract = _read(pair_dir / PAIR_CONTRACT_NAME)
    policies = [dict(row) for row in contract["policies"]]
    midpoint = (len(policies) + 1) // 2
    shards = [policies[:midpoint], policies[midpoint:]]
    payloads = [
        {
            "pair_contract_path": str(pair_dir / PAIR_CONTRACT_NAME),
            "source_contract_path": str(base_output_dir / CONTRACT_NAME),
            "feature_receipt_path": str(base_output_dir / FEATURE_NAME),
            "policies": shard,
        }
        for shard in shards
        if shard
    ]
    _write(
        pair_dir / STATE_NAME,
        {
            "status": "W2019_MARGINAL_PAIR_EXACT_REPLAY_ACTIVE",
            "policy_count": len(policies),
            "worker_processes": len(payloads),
            "numeric_threads_per_worker": 1,
        },
    )
    results = []
    exact_episodes = 0
    component_replays = 0
    calendars = []
    start_counts = []
    with ProcessPoolExecutor(max_workers=min(2, len(payloads))) as pool:
        futures = [pool.submit(_worker, payload) for payload in payloads]
        for future in as_completed(futures):
            value = future.result()
            results.extend(value["results"])
            exact_episodes += int(value["exact_account_episode_count"])
            component_replays += int(value["component_replay_count"])
            calendars.append(value["calendar"])
            start_counts.append(value["start_counts"])
    if any(value != calendars[0] for value in calendars[1:]) or any(
        value != start_counts[0] for value in start_counts[1:]
    ):
        raise CrossEraMarginalPairError("worker calendar/start-grid divergence")
    results.sort(key=lambda row: str(row["policy_id"]))
    selection = _select(contract, results)
    _write(pair_dir / COMBINED_COHORT_NAME, selection, immutable=True)
    core = {
        "schema": f"{SCHEMA}_economic_result",
        "status": "W2019_MARGINAL_PAIR_EXACT_REPLAY_COMPLETE",
        "contract_hash": contract["contract_hash"],
        "policy_cell_count": len(results),
        "matched_parent_policy_cell_count": sum(
            row["policy_role"] == "MATCHED_PARENT" for row in results
        ),
        "pair_policy_cell_count": sum(row["policy_role"] == "PAIR" for row in results),
        "component_pair_count": contract["pair_count"],
        "exact_account_episode_count": exact_episodes,
        "component_worker_replay_count": component_replays,
        "calendar_session_count": len(calendars[0]),
        "full_coverage_start_counts": start_counts[0],
        "results": results,
        "combined_cohort_hash": selection["result_hash"],
        "combined_cohort_status": selection["status"],
        "combined_cohort_count": selection["combined_count"],
        "retained_pair_count": selection["retained_pair_count"],
        "runtime_seconds": time.perf_counter() - started,
        "economic_evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "w2018_access_count_delta": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
        "next_action": selection["next_action"],
    }
    hash_core = {key: value for key, value in core.items() if key != "runtime_seconds"}
    result = {**core, "result_hash": stable_hash(hash_core)}
    _write(pair_dir / PAIR_RESULT_NAME, result, immutable=True)
    _write(
        pair_dir / STATE_NAME,
        {
            "status": "W2019_MARGINAL_PAIR_EXACT_REPLAY_COMPLETE",
            "exact_account_episode_count": exact_episodes,
            "combined_cohort_count": selection["combined_count"],
            "cohort_sufficient": selection["cohort_sufficient"],
            "next_action": selection["next_action"],
        },
    )
    return result


def freeze_w2018_confirmation_contract(base_output_dir: Path) -> dict[str, Any]:
    """Freeze the sufficient cohort without opening or purchasing W2018 bars."""

    pair_dir = _pair_output_dir(base_output_dir)
    source_contract = _read(base_output_dir / CONTRACT_NAME)
    pair_contract = _read(pair_dir / PAIR_CONTRACT_NAME)
    pair_result = _read(pair_dir / PAIR_RESULT_NAME)
    cohort = _read(pair_dir / COMBINED_COHORT_NAME)
    if not cohort.get("cohort_sufficient") or int(cohort["combined_count"]) != 8:
        raise CrossEraMarginalPairError("insufficient cohort cannot open confirmation")
    source_configs = {
        str(row["configuration_id"]): dict(row)
        for row in source_contract["configurations"]
        if row.get("replay_eligible")
    }
    pair_specs = {
        str(row["policy_id"]): dict(row)
        for row in pair_contract["policies"]
        if row.get("policy_role") == "PAIR"
    }
    candidates = []
    for rank, row in enumerate(cohort["combined"]):
        value = dict(row)
        selected = dict(value["selected_cell"])
        if value.get("policy_id"):
            candidate_id = str(value["policy_id"])
            executable = pair_specs.get(candidate_id)
            source_kind = "MARGINALLY_ACCEPTED_PAIR"
            if executable is None:
                raise CrossEraMarginalPairError(
                    f"retained pair executable spec absent: {candidate_id}"
                )
            component_ids = list(executable["component_ids"])
            account_label = str(executable["account_label"])
            executable_hash = str(executable["policy_spec_hash"])
        else:
            candidate_id = str(value["configuration_id"])
            source = source_configs.get(candidate_id)
            source_kind = "EXACT_STANDALONE"
            if source is None:
                raise CrossEraMarginalPairError(
                    f"retained standalone executable spec absent: {candidate_id}"
                )
            executable = {
                "configuration_id": candidate_id,
                "component_ids": source["component_ids"],
                "component_priority": source["component_ids"],
                "component_quantity_tiers": source["component_quantity_tiers"],
                "frozen_account_policy": source["frozen_account_policy"],
                "pnl_state_profile": source["pnl_state_profile"],
                "signal_or_trade_logic_mutated": False,
            }
            component_ids = list(source["component_ids"])
            account_label = str(selected["account_label"])
            executable_hash = stable_hash(executable)
        if account_label != str(selected["account_label"]):
            raise CrossEraMarginalPairError(
                f"selected account/executable account drift: {candidate_id}"
            )
        core = {
            "confirmation_rank": rank,
            "candidate_id": candidate_id,
            "source_kind": source_kind,
            "component_ids": component_ids,
            "behavior_cluster_id": value["realized_behavior_cluster_id"],
            "behavior_hash": value["realized_behavior_hash"],
            "account_label": account_label,
            "headline_horizon_trading_days": int(
                selected["horizon_trading_days"]
            ),
            "development_selected_cell_hash": stable_hash(selected),
            "development_result_hash": value["result_hash"],
            "executable_spec": executable,
            "executable_spec_hash": executable_hash,
            "status_before_confirmation": "DEVELOPMENT_REQUALIFIED",
            "signal_entry_exit_stop_target_mutated": False,
            "promotion_status": None,
        }
        candidates.append({**core, "candidate_freeze_hash": stable_hash(core)})
    if len(candidates) != 8 or len(
        {row["behavior_cluster_id"] for row in candidates}
    ) != 8:
        raise CrossEraMarginalPairError(
            "confirmation cohort is not eight behaviorally distinct candidates"
        )

    core = {
        "schema": f"{SCHEMA}_w2018h1_confirmation_contract",
        "status": "W2018H1_ONE_SHOT_CONFIRMATION_CONTRACT_FROZEN_DATA_UNOPENED",
        "source_pair_result_hash": pair_result["result_hash"],
        "source_combined_cohort_hash": cohort["result_hash"],
        "candidate_count": len(candidates),
        "candidates": candidates,
        "data_role": "CONFIRMATION",
        "window": {
            "window_id": "W2018H1",
            "start_inclusive": "2018-01-02",
            "end_exclusive": "2018-07-02",
            "role_frozen_before_bar_access": True,
            "previous_candidate_outcome_access_count": 0,
            "q4_overlap": False,
        },
        "evaluation": {
            "horizons_trading_days": list(HORIZONS),
            "headline_horizon_is_candidate_specific_and_frozen": True,
            "complete_non_overlapping_starts_only": True,
            "scenarios": list(SCENARIOS),
            "single_evaluation_no_retuning": True,
            "candidate_logic_and_risk_immutable": True,
        },
        "decision_rule": {
            "confirmed_pass_requirements": [
                "AT_LEAST_ONE_NORMAL_PASS",
                "AT_LEAST_ONE_STRESSED_PASS",
                "POSITIVE_STRESSED_NET",
                "ZERO_MLL_BREACH_NORMAL_AND_STRESSED",
                "ALL_PASSING_PATHS_CONSISTENCY_COMPLIANT",
            ],
            "positive_replication_without_pass": (
                "REMAINS_DEVELOPMENT_REQUALIFIED_NO_TIER_C_CLAIM"
            ),
            "failure": "CANDIDATE_CONFIRMATION_FAILED_NO_RETUNING_ON_W2018",
            "stress_does_not_erase_a_valid_normal_pass_but_controls_PROMOTION": True,
        },
        "required_markets": sorted(
            {
                str(dict(source_contract["components"])[member]["candidate"]["market"])
                for row in candidates
                for member in row["component_ids"]
            }
        ),
        "required_execution_markets": sorted(
            {
                str(
                    dict(source_contract["components"])[member]["candidate"][
                        "execution_market"
                    ]
                )
                for row in candidates
                for member in row["component_ids"]
            }
        ),
        "data_purchase_count_at_freeze": 0,
        "w2018_bar_access_count_at_freeze": 0,
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
        "next_action": (
            "ACQUIRE_OR_BIND_W2018H1_ONCE_WITHIN_AUTHORITY_THEN_RUN_EXACT_"
            "CONFIRMATION_WITHOUT_RETUNING"
        ),
    }
    contract = {**core, "contract_hash": stable_hash(core)}
    _write(pair_dir / W2018_CONTRACT_NAME, contract, immutable=True)
    return contract


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Base W2019 cross-era sieve output directory.",
    )
    parser.add_argument("--freeze-only", action="store_true")
    parser.add_argument("--freeze-w2018-only", action="store_true")
    arguments = parser.parse_args(argv)
    if arguments.freeze_w2018_only:
        value = freeze_w2018_confirmation_contract(arguments.output_dir)
        print(value["contract_hash"])
        return 0
    contract = freeze_pair_contract(arguments.output_dir)
    if arguments.freeze_only:
        print(contract["contract_hash"])
        return 0
    result = run_pair_builder(arguments.output_dir)
    print(result["result_hash"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
