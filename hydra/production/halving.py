"""Leakage-resistant successive halving for economic production campaigns.

This module deliberately contains no replay engine.  It consumes immutable
episode rows produced by :mod:`hydra.production.episode_evidence` and returns
frozen selection/evaluation plans.  In particular, leave-one-block-out plans
are created before held-out or matched-control observations are supplied.

Pass rates use only terminal, evaluable episodes.  Profitable survivors whose
research horizon ended are retained as censored observations: their observed
PnL and progress remain reportable, but they are not silently counted as
failed Combine attempts.
"""

from __future__ import annotations

import math
import random
from collections import Counter, defaultdict
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.production.manifest import PRODUCTION_RESULT_SCHEMA


HALVING_VERSION = "hydra_economic_production_halving_v1"
CROSSFIT_VERSION = "hydra_economic_production_lobo_crossfit_v1"
BASELINE_VERSION = "hydra_economic_production_matched_baselines_v1"

NORMAL = "NORMAL"
STRESSED = "STRESSED_1_5X"
SCENARIOS = (NORMAL, STRESSED)
CENSORED_TERMINALS = {
    "DATA_CENSORED",
    "OPERATIONAL_HORIZON_NOT_REACHED",
}
PASS_TERMINALS = {"TARGET_REACHED", "PASSED"}
MLL_TERMINALS = {"MLL_BREACHED", "MLL_BREACH"}
HARD_FAILURE_TERMINALS = {
    "HARD_RULE_FAILURE",
    "COMPLIANCE_FAILURE",
}


class ProductionHalvingError(ValueError):
    """Raised when a supposedly frozen evidence set is incomplete or ambiguous."""


def aggregate_policy_evidence(
    policy: Mapping[str, Any],
    episode_rows: Sequence[Mapping[str, Any]],
    *,
    block_ids: Sequence[str],
    horizon: str | int = "60_TRADING_DAYS",
) -> dict[str, Any]:
    """Aggregate one policy without converting censoring into failure.

    ``block_ids`` is an explicit evidence boundary.  Passing design-block IDs
    therefore cannot accidentally read a held-out block.  Duplicate episode
    keys fail closed rather than changing the statistical denominator.
    """

    policy_id = str(policy.get("policy_id") or "")
    if not policy_id:
        raise ProductionHalvingError("policy_id is required")
    blocks = _unique_nonempty(block_ids, "block_ids")
    horizon_key = _horizon_key(horizon)
    selected = _select_observations(
        episode_rows,
        policy_id=policy_id,
        block_ids=set(blocks),
        horizon=horizon_key,
    )
    if not selected:
        raise ProductionHalvingError(
            f"no frozen {horizon_key} observations for policy {policy_id}"
        )
    block_metrics: dict[str, dict[str, Any]] = {}
    for block_id in blocks:
        values = [row for row in selected if row["temporal_block"] == block_id]
        if not values:
            raise ProductionHalvingError(
                f"policy {policy_id} has no evidence in frozen block {block_id}"
            )
        block_metrics[block_id] = {
            scenario: _scenario_metrics(
                [row for row in values if row["scenario"] == scenario]
            )
            for scenario in SCENARIOS
        }

    scenarios = {
        scenario: _scenario_metrics(
            [row for row in selected if row["scenario"] == scenario]
        )
        for scenario in SCENARIOS
    }
    stress_blocks = {
        block: row[STRESSED] for block, row in block_metrics.items()
    }
    block_passes = {
        block: int(row["pass_count"]) for block, row in stress_blocks.items()
    }
    positive_block_net = {
        block: max(float(row["observed_net_total"]), 0.0)
        for block, row in stress_blocks.items()
    }
    contribution, complete_attribution = _component_contribution(selected)
    positive_contribution = {
        key: max(value, 0.0) for key, value in contribution.items()
    }
    output = {
        "schema": "hydra_production_policy_block_metrics_v1",
        "policy_id": policy_id,
        "policy": dict(policy),
        "horizon": horizon_key,
        "block_ids": list(blocks),
        "normal": scenarios[NORMAL],
        "stressed_1_5x": scenarios[STRESSED],
        "by_block": block_metrics,
        "positive_stressed_block_count": sum(
            float(row["observed_net_total"]) > 0.0 for row in stress_blocks.values()
        ),
        "stressed_pass_block_count": sum(value > 0 for value in block_passes.values()),
        "maximum_block_pass_share": _maximum_share(block_passes),
        "maximum_block_positive_net_share": _maximum_share(positive_block_net),
        "component_contribution": dict(sorted(contribution.items())),
        "component_attribution_complete": complete_attribution,
        "maximum_component_positive_profit_share": (
            _maximum_share(positive_contribution) if complete_attribution else None
        ),
        "component_count": len(tuple(policy.get("sleeve_ids") or ())),
        "operational_simplicity": _operational_simplicity(policy),
        "risk_level": float(policy.get("risk_level") or 0.0),
        "integrity_issue": bool(policy.get("integrity_issue", False)),
        "behavioral_fingerprint": str(
            policy.get("behavioral_fingerprint") or policy_id
        ),
        "censored_pass_rate_policy": "EXCLUDE_FROM_PASS_DENOMINATOR_REPORT_SEPARATELY",
        "development_only": True,
        "independently_confirmed": False,
    }
    output["metrics_hash"] = stable_hash(output)
    return output


def pareto_select(
    candidates: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    stage: str,
    maximum_mll_breach_rate: float = 0.10,
    maximum_component_profit_share: float = 0.65,
    require_complete_component_attribution: bool = False,
    minimum_block_count: int = 1,
) -> dict[str, Any]:
    """Select successive non-dominated fronts with an explicit tie order.

    There is no weighted or learned score.  Candidate dominance uses the
    declared Pareto dimensions; the frozen lexicographic order is used only if
    a front crosses the stage capacity.
    """

    if limit < 1:
        raise ProductionHalvingError("selection limit must be positive")
    if not 0.0 <= maximum_mll_breach_rate <= 1.0:
        raise ProductionHalvingError("invalid MLL tolerance")
    if not 0.0 < maximum_component_profit_share <= 1.0:
        raise ProductionHalvingError("invalid component concentration limit")

    unique: list[Mapping[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    fingerprint_owner: dict[str, str] = {}
    for candidate in sorted(candidates, key=lambda row: str(row["policy_id"])):
        policy_id = str(candidate["policy_id"])
        reasons = _hard_filter_reasons(
            candidate,
            maximum_mll_breach_rate=maximum_mll_breach_rate,
            maximum_component_profit_share=maximum_component_profit_share,
            require_complete_component_attribution=require_complete_component_attribution,
            minimum_block_count=minimum_block_count,
        )
        fingerprint = str(candidate.get("behavioral_fingerprint") or policy_id)
        owner = fingerprint_owner.get(fingerprint)
        if owner is not None:
            reasons.append(f"BEHAVIORAL_CLONE_OF:{owner}")
        elif not reasons:
            fingerprint_owner[fingerprint] = policy_id
        if reasons:
            excluded.append({"policy_id": policy_id, "reasons": reasons})
        else:
            unique.append(candidate)

    remaining = list(unique)
    fronts: list[list[Mapping[str, Any]]] = []
    while remaining:
        front = [
            row
            for row in remaining
            if not any(
                _dominates(other, row)
                for other in remaining
                if other is not row
            )
        ]
        if not front:
            raise ProductionHalvingError("Pareto decomposition failed")
        front.sort(key=_lexicographic_key)
        fronts.append(front)
        front_ids = {id(row) for row in front}
        remaining = [row for row in remaining if id(row) not in front_ids]

    selected: list[Mapping[str, Any]] = []
    ranked_rows: list[dict[str, Any]] = []
    for front_number, front in enumerate(fronts, start=1):
        for within_front, row in enumerate(front, start=1):
            chosen = len(selected) < limit
            if chosen:
                selected.append(row)
            ranked_rows.append(
                {
                    "policy_id": str(row["policy_id"]),
                    "pareto_front": front_number,
                    "lexicographic_rank_within_front": within_front,
                    "selected": chosen,
                    "ranking_metrics": _ranking_metrics(row),
                }
            )

    decision = {
        "schema": "hydra_production_pareto_selection_v1",
        "halving_version": HALVING_VERSION,
        "stage": stage,
        "input_count": len(candidates),
        "eligible_count": len(unique),
        "output_limit": limit,
        "output_count": len(selected),
        "selected_policy_ids": [str(row["policy_id"]) for row in selected],
        "pareto_dimensions": [
            "stressed_evaluable_pass_rate:MAX",
            "normal_evaluable_pass_rate:MAX",
            "stressed_censoring_rate:MIN",
            "normal_censoring_rate:MIN",
            "stressed_target_progress_median:MAX",
            "stressed_target_progress_p25:MAX",
            "stressed_observed_net_total:MAX",
            "stressed_mll_breach_rate:MIN",
            "stressed_consistency_rate:MAX",
            "component_concentration:MIN",
            "block_concentration:MIN",
            "operational_simplicity:MIN",
        ],
        "cutoff_tie_policy": "FROZEN_LEXICOGRAPHIC_METRICS_THEN_POLICY_ID",
        "opaque_score_used": False,
        "censored_episodes_ranked_as_failures": False,
        "ranked_candidates": ranked_rows,
        "excluded": excluded,
    }
    decision["decision_hash"] = stable_hash(decision)
    return decision


def select_stage3_survivors(
    candidates: Sequence[Mapping[str, Any]], *, limit: int = 64
) -> dict[str, Any]:
    if limit > 64:
        raise ProductionHalvingError("Stage 3 survivor cap is 64")
    return pareto_select(candidates, limit=limit, stage="STAGE_3_ROLLING_COMBINE")


def select_stage4_survivors(
    candidates: Sequence[Mapping[str, Any]], *, limit: int = 16
) -> dict[str, Any]:
    if limit > 16:
        raise ProductionHalvingError("Stage 4 survivor cap is 16")
    return pareto_select(
        candidates,
        limit=limit,
        stage="STAGE_4_ROBUSTNESS_CROSSFIT",
        require_complete_component_attribution=True,
        minimum_block_count=4,
    )


def select_stage5_survivors(
    candidates: Sequence[Mapping[str, Any]],
    *,
    criteria: Mapping[str, Any],
    limit: int = 4,
) -> dict[str, Any]:
    """Select at most four candidates after genuine 96-start evaluation."""

    if limit > 4:
        raise ProductionHalvingError("Stage 5 survivor cap is 4")
    eligible: list[Mapping[str, Any]] = []
    development_checks: list[dict[str, Any]] = []
    for row in candidates:
        decision = development_decision(row, criteria=criteria, minimum_starts=96)
        development_checks.append(decision)
        if decision["criteria_satisfied"]:
            eligible.append(row)
    selected = pareto_select(
        eligible,
        limit=limit,
        stage="STAGE_5_EXPANDED_96_STARTS",
        require_complete_component_attribution=True,
        minimum_block_count=4,
    )
    selected["preselection_development_checks"] = development_checks
    payload = dict(selected)
    payload.pop("decision_hash", None)
    selected["decision_hash"] = stable_hash(payload)
    return selected


def build_leave_one_block_out_plan(
    policies: Sequence[Mapping[str, Any]],
    episode_rows: Sequence[Mapping[str, Any]],
    *,
    predeclared_baseline_policies: Sequence[Mapping[str, Any]],
    baseline_design_episode_rows: Sequence[Mapping[str, Any]],
    block_ids: Sequence[str],
    random_seeds: Sequence[int],
    horizon: str | int = "60_TRADING_DAYS",
) -> dict[str, Any]:
    """Freeze each outer-fold champion and baselines using design blocks only."""

    blocks = _unique_nonempty(block_ids, "block_ids")
    if len(blocks) < 4:
        raise ProductionHalvingError("leave-one-block-out requires at least four blocks")
    seeds = _unique_ints(random_seeds, "random_seeds")
    if not seeds:
        raise ProductionHalvingError("random baseline seeds are required")
    policy_by_id = _policy_map(policies)
    baseline_bank = _policy_map(predeclared_baseline_policies)
    singleton_policies = tuple(
        row
        for row in baseline_bank.values()
        if _baseline_role(row) == "BEST_PARENT_CANDIDATE"
        and len(tuple(row.get("sleeve_ids") or ())) == 1
    )
    if not singleton_policies:
        raise ProductionHalvingError(
            "predeclared baseline bank has no singleton parent candidates"
        )
    eligible_components = tuple(
        sorted(
            {
                str(tuple(row["sleeve_ids"])[0])
                for row in singleton_policies
            }
        )
    )
    folds: list[dict[str, Any]] = []
    for held_out in blocks:
        design = tuple(block for block in blocks if block != held_out)
        design_metrics = [
            aggregate_policy_evidence(
                policy,
                episode_rows,
                block_ids=design,
                horizon=horizon,
            )
            for policy in policy_by_id.values()
        ]
        selection = pareto_select(
            design_metrics,
            limit=1,
            stage=f"OUTER_FOLD_DESIGN:{held_out}",
            minimum_block_count=len(design),
        )
        if not selection["selected_policy_ids"]:
            raise ProductionHalvingError(
                f"no eligible design-set champion for held-out block {held_out}"
            )
        champion_id = str(selection["selected_policy_ids"][0])
        singleton_metrics = [
            aggregate_policy_evidence(
                singleton,
                baseline_design_episode_rows,
                block_ids=design,
                horizon=horizon,
            )
            for singleton in singleton_policies
        ]
        baselines = build_baseline_policies(
            fold_id=held_out,
            champion_policy=policy_by_id[champion_id],
            singleton_design_metrics=singleton_metrics,
            eligible_component_ids=eligible_components,
            random_seeds=seeds,
            predeclared_baseline_policies=tuple(baseline_bank.values()),
        )
        folds.append(
            {
                "fold_id": f"LOBO_HELD_OUT_{held_out}",
                "held_out_block": held_out,
                "design_blocks": list(design),
                "candidate_count": len(design_metrics),
                "selection_decision": selection,
                "champion_policy": dict(policy_by_id[champion_id]),
                "selected_risk_level": float(
                    policy_by_id[champion_id].get("risk_level") or 0.0
                ),
                "baselines": baselines,
                "held_out_outcomes_inspected": False,
                "retuning_after_holdout": False,
            }
        )
    plan = {
        "schema": "hydra_production_lobo_plan_v1",
        "crossfit_version": CROSSFIT_VERSION,
        "block_ids": list(blocks),
        "horizon": _horizon_key(horizon),
        "fold_count": len(folds),
        "folds": folds,
        "selection_unit": "TEMPORAL_BLOCK",
        "episode_starts_counted_as_independent": False,
        "held_out_data_used_for_selection": False,
        "development_only": True,
    }
    plan["plan_hash"] = stable_hash(plan)
    return plan


def build_baseline_policies(
    *,
    fold_id: str,
    champion_policy: Mapping[str, Any],
    singleton_design_metrics: Sequence[Mapping[str, Any]],
    eligible_component_ids: Sequence[str],
    random_seeds: Sequence[int],
    predeclared_baseline_policies: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build best-parent, equal-risk, and fixed-seed random policies.

    Best-parent identity/risk comes only from design metrics.  Equal-risk and
    random baskets are exact lookups in a bank frozen before any outcomes; no
    post-outcome policy ID or membership is synthesized here.
    """

    eligible = _unique_nonempty(eligible_component_ids, "eligible_component_ids")
    size = len(tuple(champion_policy.get("sleeve_ids") or ()))
    if size < 1 or size > len(eligible):
        raise ProductionHalvingError("invalid champion basket size for baselines")
    if not singleton_design_metrics:
        raise ProductionHalvingError("singleton design metrics are required")
    best_metric = min(
        singleton_design_metrics,
        key=_lexicographic_key,
    )
    if _baseline_role(best_metric["policy"]) != "BEST_PARENT_CANDIDATE":
        raise ProductionHalvingError("best-parent metric is not a preregistered singleton")
    best_parent = dict(best_metric["policy"])
    bank = tuple(dict(row) for row in predeclared_baseline_policies)
    risk = float(champion_policy.get("risk_level") or 0.0)
    expected_equal_components = tuple(sorted(eligible)[:size])
    equal_risk = _lookup_predeclared_baseline(
        bank,
        role="EQUAL_RISK",
        size=size,
        risk_level=risk,
        components=expected_equal_components,
    )
    random_rows: list[dict[str, Any]] = []
    for seed in _unique_ints(random_seeds, "random_seeds"):
        chooser = random.Random(seed)
        chosen = tuple(chooser.sample(sorted(eligible), size))
        row = _lookup_predeclared_baseline(
            bank,
            role="RANDOM_SELECTION",
            size=size,
            risk_level=risk,
            components=chosen,
            random_seed=seed,
        )
        random_rows.append(row)
    output = {
        "schema": "hydra_production_matched_baseline_plan_v1",
        "baseline_version": BASELINE_VERSION,
        "fold_id": fold_id,
        "design_selected_best_parent": best_parent,
        "deterministic_equal_risk": equal_risk,
        "fixed_seed_random_selection": random_rows,
        "eligible_component_ids": list(sorted(eligible)),
        "basket_size": size,
        "risk_level_frozen_from_design_champion": float(
            champion_policy.get("risk_level") or 0.0
        ),
        "all_policy_ids_predeclared_before_outcomes": True,
        "predeclared_bank_policy_count": len(bank),
        "held_out_outcomes_used": False,
    }
    output["baseline_plan_hash"] = stable_hash(output)
    return output


def complete_leave_one_block_out(
    frozen_plan: Mapping[str, Any],
    candidate_episode_rows: Sequence[Mapping[str, Any]],
    baseline_episode_rows: Sequence[Mapping[str, Any]],
    *,
    thresholds: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate an already-frozen plan exactly once on each held-out block."""

    _verify_self_hash(frozen_plan, "plan_hash", "crossfit plan")
    defaults = {
        "minimum_aggregate_passes": 3,
        "minimum_pass_blocks": 2,
        "minimum_positive_economic_blocks": 3,
        "maximum_mll_breach_rate": 0.10,
        "maximum_block_pass_share": 0.50,
        "maximum_component_profit_share": 0.65,
    }
    if thresholds:
        defaults.update(dict(thresholds))
    all_rows = list(candidate_episode_rows) + list(baseline_episode_rows)
    heldout_selector_rows: list[dict[str, Any]] = []
    baseline_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    folds: list[dict[str, Any]] = []
    for source_fold in frozen_plan["folds"]:
        fold = dict(source_fold)
        held_out = str(fold["held_out_block"])
        champion = dict(fold["champion_policy"])
        champion_metrics = aggregate_policy_evidence(
            champion,
            candidate_episode_rows,
            block_ids=(held_out,),
            horizon=str(frozen_plan["horizon"]),
        )
        heldout_selector_rows.extend(
            _select_observations(
                candidate_episode_rows,
                policy_id=str(champion["policy_id"]),
                block_ids={held_out},
                horizon=str(frozen_plan["horizon"]),
            )
        )
        baseline_plan = fold["baselines"]
        baseline_specs = [
            ("best_parent", baseline_plan["design_selected_best_parent"]),
            ("equal_risk", baseline_plan["deterministic_equal_risk"]),
        ] + [
            (f"random_seed_{row['random_seed']}", row)
            for row in baseline_plan["fixed_seed_random_selection"]
        ]
        evaluated_baselines: dict[str, Any] = {}
        for role, policy in baseline_specs:
            metrics = aggregate_policy_evidence(
                policy,
                all_rows,
                block_ids=(held_out,),
                horizon=str(frozen_plan["horizon"]),
            )
            evaluated_baselines[role] = metrics
            baseline_groups[role].extend(
                _select_observations(
                    all_rows,
                    policy_id=str(policy["policy_id"]),
                    block_ids={held_out},
                    horizon=str(frozen_plan["horizon"]),
                )
            )
        best_parent = evaluated_baselines["best_parent"]
        equal_risk = evaluated_baselines["equal_risk"]
        random_stress_nets = [
            float(value["stressed_1_5x"]["observed_net_total"])
            for key, value in evaluated_baselines.items()
            if key.startswith("random_seed_")
        ]
        folds.append(
            {
                **fold,
                "held_out_outcomes_inspected": True,
                "held_out_champion": champion_metrics,
                "held_out_baselines": evaluated_baselines,
                "held_out_comparison": {
                    "normal_net_delta_vs_best_parent": _net_delta(
                        champion_metrics, best_parent, NORMAL
                    ),
                    "stressed_net_delta_vs_best_parent": _net_delta(
                        champion_metrics, best_parent, STRESSED
                    ),
                    "stressed_target_progress_delta_vs_best_parent": (
                        float(
                            champion_metrics["stressed_1_5x"][
                                "target_progress_median"
                            ]
                        )
                        - float(
                            best_parent["stressed_1_5x"]["target_progress_median"]
                        )
                    ),
                    "stressed_net_delta_vs_equal_risk": _net_delta(
                        champion_metrics, equal_risk, STRESSED
                    ),
                    "stressed_net_delta_vs_random_median": (
                        float(
                            champion_metrics["stressed_1_5x"]["observed_net_total"]
                        )
                        - float(median(random_stress_nets))
                    ),
                },
                "retuning_after_holdout": False,
            }
        )

    selector_aggregate = _aggregate_mixed_observations(heldout_selector_rows)
    baseline_aggregate = {
        role: _aggregate_mixed_observations(rows)
        for role, rows in sorted(baseline_groups.items())
    }
    decision = _selector_decision(
        selector_aggregate,
        baseline_aggregate,
        thresholds=defaults,
    )
    result = {
        "schema": "hydra_production_lobo_result_v1",
        "crossfit_version": CROSSFIT_VERSION,
        "plan_hash": str(frozen_plan["plan_hash"]),
        "horizon": str(frozen_plan["horizon"]),
        "fold_count": len(folds),
        "folds": folds,
        "headline_held_out_selector": selector_aggregate,
        "held_out_baseline_aggregates": baseline_aggregate,
        "selector_decision": decision,
        "development_results_separate_from_design": True,
        "held_out_blocks_used_exactly_once": True,
        "independently_confirmed": False,
    }
    result["result_hash"] = stable_hash(result)
    return result


def development_decision(
    metrics: Mapping[str, Any],
    *,
    criteria: Mapping[str, Any],
    minimum_starts: int,
) -> dict[str, Any]:
    """Apply frozen development criteria without granting confirmation status."""

    normal = metrics["normal"]
    stress = metrics["stressed_1_5x"]
    checks = {
        "minimum_start_count": (
            int(normal["episode_count"]) >= minimum_starts
            and int(stress["episode_count"]) >= minimum_starts
        ),
        "normal_pass_rate": float(normal["observed_pass_fraction"])
        >= float(criteria["minimum_normal_pass_rate"]),
        "stressed_pass_rate": float(stress["observed_pass_fraction"])
        >= float(criteria["minimum_stressed_pass_rate"]),
        "positive_stressed_net": float(stress["observed_net_total"])
        > float(criteria.get("minimum_stressed_net", 0.0)),
        "mll_tolerance": float(stress["mll_breach_rate"])
        <= float(criteria["maximum_mll_breach_rate"]),
        "passes_across_blocks": int(metrics["stressed_pass_block_count"])
        >= int(criteria["minimum_positive_blocks"]),
        "block_concentration": float(metrics["maximum_block_pass_share"])
        <= float(criteria["maximum_block_pass_share"]),
        "component_attribution_complete": bool(
            metrics["component_attribution_complete"]
        ),
        "component_concentration": (
            metrics["maximum_component_positive_profit_share"] is not None
            and float(metrics["maximum_component_positive_profit_share"])
            <= float(criteria["maximum_component_profit_share"])
        ),
        "consistency": float(stress["consistency_rate"])
        >= float(criteria.get("minimum_consistency_rate", 0.0)),
    }
    satisfied = all(checks.values())
    return {
        "schema": "hydra_production_development_decision_v1",
        "policy_id": str(metrics["policy_id"]),
        "minimum_starts": minimum_starts,
        "checks": checks,
        "criteria_satisfied": satisfied,
        "status": (
            "BASKET_CONFIRMATION_READY" if satisfied else "DEVELOPMENT_REJECTED"
        ),
        "pass_threshold_definition": (
            "OBSERVED_TARGET_CUMULATIVE_INCIDENCE_AT_FROZEN_HORIZON_"
            "TARGETS_REACHED_DIVIDED_BY_ALL_FROZEN_STARTS"
        ),
        "censored_starts_classified_as_failures": False,
        "development_only": True,
        "independently_confirmed": False,
        "paper_shadow_ready": False,
    }


def classify_failure_vectors(
    metrics: Mapping[str, Any],
    *,
    minimum_episode_count: int = 48,
    incremental_stressed_net_delta: float | None = None,
) -> tuple[str, ...]:
    normal = metrics["normal"]
    stress = metrics["stressed_1_5x"]
    failures: list[str] = []
    if int(stress["episode_count"]) < minimum_episode_count:
        failures.append("INSUFFICIENT_OPPORTUNITIES")
    if float(stress["mll_breach_rate"]) > 0.10:
        failures.append("MLL_BREACH")
    if float(normal["observed_net_total"]) > 0.0 >= float(
        stress["observed_net_total"]
    ):
        failures.append("COST_FRAGILITY")
    if float(stress["consistency_rate"]) < 0.75:
        failures.append("CONSISTENCY_FAILURE")
    if float(stress["target_progress_median"]) < 0.70:
        failures.append("TARGET_TOO_SLOW")
    if int(metrics["positive_stressed_block_count"]) < 2:
        failures.append("TEMPORAL_INSTABILITY")
    component_share = metrics.get("maximum_component_positive_profit_share")
    if component_share is None or float(component_share) > 0.65:
        failures.append("OVER_CONCENTRATION")
    if incremental_stressed_net_delta is not None and incremental_stressed_net_delta <= 0:
        failures.append("NO_INCREMENTAL_VALUE")
    if not failures:
        failures.append("NO_INCREMENTAL_VALUE")
    return tuple(failures)


def build_compact_outputs(
    *,
    campaign_id: str,
    metrics: Sequence[Mapping[str, Any]],
    stage_decisions: Sequence[Mapping[str, Any]],
    crossfit_result: Mapping[str, Any] | None,
    development_decisions: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build the four compact outputs required by EvidenceBundle v1."""

    confirmation_ready = sorted(
        str(row["policy_id"])
        for row in development_decisions
        if row.get("status") == "BASKET_CONFIRMATION_READY"
    )
    failure_by_policy = {
        str(row["policy_id"]): list(classify_failure_vectors(row)) for row in metrics
    }
    counts = Counter(value for values in failure_by_policy.values() for value in values)
    final_selected = (
        list(stage_decisions[-1].get("selected_policy_ids") or ())
        if stage_decisions
        else []
    )
    if confirmation_ready:
        next_action = {
            "action": "FREEZE_DEVELOPMENT_FINALISTS_AND_AUDIT_UNTOUCHED_DATA_AVAILABILITY",
            "candidate_ids": confirmation_ready,
            "manifest_required": False,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        }
    elif final_selected:
        next_action = {
            "action": "QUEUE_FAILURE_GUIDED_TARGETED_MUTATION_MANIFEST",
            "parent_policy_ids": final_selected,
            "manifest_required": True,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        }
    else:
        next_action = {
            "action": "QUEUE_MATERIALLY_DISTINCT_MECHANISM_MANIFEST",
            "manifest_required": True,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        }
    summary = {
        "schema": "hydra_production_campaign_summary_v1",
        "campaign_id": campaign_id,
        "candidate_count": len(metrics),
        "positive_stressed_net_count": sum(
            float(row["stressed_1_5x"]["observed_net_total"]) > 0.0
            for row in metrics
        ),
        "normal_pass_candidate_count": sum(
            int(row["normal"]["pass_count"]) > 0 for row in metrics
        ),
        "stressed_pass_candidate_count": sum(
            int(row["stressed_1_5x"]["pass_count"]) > 0 for row in metrics
        ),
        "confirmation_ready_candidate_ids": confirmation_ready,
        "development_only": True,
        "independently_confirmed": False,
    }
    return {
        "campaign_summary": summary,
        "failure_vectors": {
            "schema": "hydra_production_failure_vectors_v1",
            "campaign_id": campaign_id,
            "by_policy": failure_by_policy,
            "counts": dict(sorted(counts.items())),
        },
        "pareto_archive": {
            "schema": "hydra_production_pareto_archive_v1",
            "campaign_id": campaign_id,
            "stage_decisions": [dict(row) for row in stage_decisions],
            "crossfit": dict(crossfit_result or {}),
            "opaque_score_used": False,
        },
        "next_campaign_recommendations": {
            "schema": "hydra_production_next_campaign_recommendations_v1",
            "campaign_id": campaign_id,
            "recommendation": next_action,
        },
    }


def build_final_result_payload(
    *,
    manifest: Mapping[str, Any],
    kpis: Mapping[str, Any],
    economic_results: Mapping[str, Any],
    successive_halving: Mapping[str, Any],
    matched_controls: Mapping[str, Any],
    failure_vectors: Mapping[str, Any],
    evidence_receipt: Mapping[str, Any],
    autonomous_next_action: Mapping[str, Any],
    scientific_status: str = "DEVELOPMENT_COMPLETE",
) -> dict[str, Any]:
    """Create a terminal payload only from an already sealed fresh bundle receipt."""

    if evidence_receipt.get("evidence_status") != "FRESH_DEVELOPMENT_EVIDENCE":
        raise ProductionHalvingError("terminal result requires fresh development evidence")
    if evidence_receipt.get("reconstruction_flag") is not False:
        raise ProductionHalvingError("production evidence cannot be a reconstruction")
    for field in (
        "bundle_path",
        "manifest_path",
        "manifest_sha256",
        "bundle_content_sha256",
        "dataset_row_counts",
    ):
        if not evidence_receipt.get(field):
            raise ProductionHalvingError(f"EvidenceBundle receipt omits {field}")
    result = {
        "schema": PRODUCTION_RESULT_SCHEMA,
        "campaign_id": str(manifest["campaign_id"]),
        "manifest_hash": str(manifest["manifest_hash"]),
        "source_commit": str(manifest["source_commit"]),
        "status": "COMPLETE",
        "scientific_status": scientific_status,
        "kpis": dict(kpis),
        "economic_results": dict(economic_results),
        "successive_halving": dict(successive_halving),
        "matched_controls": dict(matched_controls),
        "failure_vectors": dict(failure_vectors),
        "evidence_bundle": dict(evidence_receipt),
        "evidence_verification_manifest_sha256": str(
            evidence_receipt["manifest_sha256"]
        ),
        "autonomous_next_action": dict(autonomous_next_action),
        "development_only": True,
        "independently_confirmed": False,
        "status_inheritance": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    result["result_hash"] = stable_hash(result)
    return result


def _select_observations(
    episode_rows: Sequence[Mapping[str, Any]],
    *,
    policy_id: str,
    block_ids: set[str],
    horizon: str,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for raw in episode_rows:
        if str(raw.get("policy_id") or "") != policy_id:
            continue
        block = str(raw.get("temporal_block") or "")
        if block not in block_ids:
            continue
        if _horizon_key(raw.get("horizon") or raw.get("horizon_trading_days")) != horizon:
            continue
        row = _normalize_observation(raw)
        key = (
            row["policy_id"],
            row["episode_id"],
            row["scenario"],
            row["horizon"],
        )
        if key in seen:
            raise ProductionHalvingError(f"duplicate frozen episode key: {key}")
        seen.add(key)
        selected.append(row)
    return selected


def _normalize_observation(raw: Mapping[str, Any]) -> dict[str, Any]:
    policy_id = str(raw.get("policy_id") or "")
    scenario = str(raw.get("cost_scenario") or raw.get("scenario") or "")
    if scenario not in SCENARIOS:
        raise ProductionHalvingError(f"unknown cost scenario: {scenario}")
    terminal = str(
        raw.get("terminal_state")
        or raw.get("terminal_classification")
        or raw.get("terminal")
        or ""
    )
    target = bool(raw.get("target_reached", terminal in PASS_TERMINALS))
    mll = bool(raw.get("mll_breached", terminal in MLL_TERMINALS))
    censored = bool(raw.get("censored_state", raw.get("censored", False))) or (
        terminal in CENSORED_TERMINALS
    )
    hard = terminal in HARD_FAILURE_TERMINALS
    if sum((target, mll, censored, hard)) != 1:
        raise ProductionHalvingError(
            f"episode terminal must be exactly one pass/MLL/hard/censored: {policy_id}"
        )
    episode_id = str(
        raw.get("episode_id")
        or f"{policy_id}:{raw.get('start_day', raw.get('episode_start', ''))}"
    )
    contribution = raw.get("component_attribution")
    if contribution is None:
        contribution = raw.get("component_contribution")
    if contribution is not None and not isinstance(contribution, Mapping):
        raise ProductionHalvingError("component attribution must be an object")
    output = {
        "policy_id": policy_id,
        "episode_id": episode_id,
        "scenario": scenario,
        "temporal_block": str(raw.get("temporal_block") or ""),
        "horizon": _horizon_key(raw.get("horizon") or raw.get("horizon_trading_days")),
        "target_reached": target,
        "mll_breached": mll,
        "hard_failure": hard,
        "censored": censored,
        "net_pnl": _finite(raw.get("net_pnl", 0.0), "net_pnl"),
        "target_progress": _finite(raw.get("target_progress", 0.0), "target_progress"),
        "minimum_mll_buffer": _finite(
            raw.get("minimum_mll_buffer", 0.0), "minimum_mll_buffer"
        ),
        "consistency_ok": bool(raw.get("consistency_ok", False)),
        "days_to_target": (
            None
            if raw.get("days_to_target") is None
            else _finite(raw["days_to_target"], "days_to_target")
        ),
        "component_attribution": (
            None
            if contribution is None
            else {
                str(key): _finite(value, "component_attribution")
                for key, value in contribution.items()
            }
        ),
    }
    if not output["temporal_block"] or not output["episode_id"]:
        raise ProductionHalvingError("episode provenance is incomplete")
    return output


def _scenario_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ProductionHalvingError("both normal and stressed evidence are required")
    evaluable = [row for row in rows if not row["censored"]]
    passes = [row for row in evaluable if row["target_reached"]]
    mll = [row for row in evaluable if row["mll_breached"]]
    hard = [row for row in evaluable if row["hard_failure"]]
    denominator = len(evaluable)
    net = [float(row["net_pnl"]) for row in rows]
    progress = [float(row["target_progress"]) for row in rows]
    days = [float(row["days_to_target"]) for row in passes if row["days_to_target"] is not None]
    return {
        "episode_count": len(rows),
        "evaluable_episode_count": denominator,
        "censored_episode_count": len(rows) - denominator,
        "censoring_rate": (len(rows) - denominator) / len(rows),
        "pass_count": len(passes),
        "evaluable_pass_rate": len(passes) / denominator if denominator else 0.0,
        "pass_rate": len(passes) / denominator if denominator else 0.0,
        "pass_rate_defined": denominator > 0,
        "observed_pass_fraction": len(passes) / len(rows),
        "observed_target_cumulative_incidence": len(passes) / len(rows),
        "mll_breach_count": len(mll),
        "mll_breach_rate": len(mll) / denominator if denominator else 0.0,
        "hard_failure_count": len(hard),
        "observed_net_total": sum(net),
        "observed_net_median": float(median(net)),
        "target_progress_p25": _percentile(progress, 0.25),
        "target_progress_median": float(median(progress)),
        "target_progress_maximum": max(progress),
        "consistency_rate": sum(bool(row["consistency_ok"]) for row in rows) / len(rows),
        "minimum_mll_buffer": min(float(row["minimum_mll_buffer"]) for row in rows),
        "median_days_to_target": float(median(days)) if days else None,
        "censored_observed_net_total": sum(
            float(row["net_pnl"]) for row in rows if row["censored"]
        ),
    }


def _component_contribution(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, float], bool]:
    output: dict[str, float] = defaultdict(float)
    complete = True
    for row in rows:
        contribution = row.get("component_attribution")
        if contribution is None:
            complete = False
            continue
        for component, value in contribution.items():
            output[str(component)] += float(value)
    return dict(output), complete


def _hard_filter_reasons(
    row: Mapping[str, Any],
    *,
    maximum_mll_breach_rate: float,
    maximum_component_profit_share: float,
    require_complete_component_attribution: bool,
    minimum_block_count: int,
) -> list[str]:
    reasons: list[str] = []
    normal = row["normal"]
    stress = row["stressed_1_5x"]
    if float(normal["observed_net_total"]) <= 0.0:
        reasons.append("NON_POSITIVE_NORMAL_NET")
    if float(stress["observed_net_total"]) <= 0.0:
        reasons.append("NON_POSITIVE_STRESSED_NET")
    if not normal.get("pass_rate_defined") or not stress.get("pass_rate_defined"):
        reasons.append("NO_EVALUABLE_EPISODES")
    if float(normal["mll_breach_rate"]) > maximum_mll_breach_rate:
        reasons.append("NORMAL_MLL_TOLERANCE")
    if float(stress["mll_breach_rate"]) > maximum_mll_breach_rate:
        reasons.append("STRESSED_MLL_TOLERANCE")
    if row.get("integrity_issue"):
        reasons.append("EXECUTION_OR_INTEGRITY_ISSUE")
    if len(tuple(row.get("block_ids") or ())) < minimum_block_count:
        reasons.append("INSUFFICIENT_TEMPORAL_BLOCKS")
    if require_complete_component_attribution and not row.get(
        "component_attribution_complete"
    ):
        reasons.append("INCOMPLETE_COMPONENT_ATTRIBUTION")
    share = row.get("maximum_component_positive_profit_share")
    if share is not None and float(share) > maximum_component_profit_share:
        reasons.append("EXCESSIVE_COMPONENT_DOMINATION")
    return reasons


def _metric_vector(row: Mapping[str, Any]) -> tuple[float, ...]:
    normal = row["normal"]
    stress = row["stressed_1_5x"]
    component = row.get("maximum_component_positive_profit_share")
    component_value = 1.0 if component is None else float(component)
    block = max(
        float(row.get("maximum_block_pass_share") or 0.0),
        float(row.get("maximum_block_positive_net_share") or 0.0),
    )
    return (
        float(stress["pass_rate"]),
        float(normal["pass_rate"]),
        -float(stress["censoring_rate"]),
        -float(normal["censoring_rate"]),
        float(stress["target_progress_median"]),
        float(stress["target_progress_p25"]),
        float(stress["observed_net_total"]),
        -float(stress["mll_breach_rate"]),
        float(stress["consistency_rate"]),
        -component_value,
        -block,
        -float(row["operational_simplicity"]),
    )


def _dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    a = _metric_vector(left)
    b = _metric_vector(right)
    return all(x >= y for x, y in zip(a, b)) and any(x > y for x, y in zip(a, b))


def _lexicographic_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    stress = row["stressed_1_5x"]
    normal = row["normal"]
    component = row.get("maximum_component_positive_profit_share")
    return (
        -float(stress["pass_rate"]),
        -int(stress["pass_count"]),
        float(stress["censoring_rate"]),
        -float(normal["pass_rate"]),
        -int(normal["pass_count"]),
        float(normal["censoring_rate"]),
        -float(stress["target_progress_median"]),
        -float(stress["target_progress_p25"]),
        -float(stress["observed_net_total"]),
        float(stress["mll_breach_rate"]),
        -float(stress["consistency_rate"]),
        1.0 if component is None else float(component),
        max(
            float(row.get("maximum_block_pass_share") or 0.0),
            float(row.get("maximum_block_positive_net_share") or 0.0),
        ),
        float(row["operational_simplicity"]),
        str(row["policy_id"]),
    )


def _ranking_metrics(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "normal_evaluable_pass_rate": float(row["normal"]["pass_rate"]),
        "normal_observed_pass_fraction": float(
            row["normal"]["observed_pass_fraction"]
        ),
        "normal_censoring_rate": float(row["normal"]["censoring_rate"]),
        "normal_censored_count": int(row["normal"]["censored_episode_count"]),
        "stressed_evaluable_pass_rate": float(
            row["stressed_1_5x"]["pass_rate"]
        ),
        "stressed_observed_pass_fraction": float(
            row["stressed_1_5x"]["observed_pass_fraction"]
        ),
        "stressed_censoring_rate": float(
            row["stressed_1_5x"]["censoring_rate"]
        ),
        "stressed_censored_count": int(
            row["stressed_1_5x"]["censored_episode_count"]
        ),
        "stressed_target_progress_median": float(
            row["stressed_1_5x"]["target_progress_median"]
        ),
        "stressed_target_progress_p25": float(
            row["stressed_1_5x"]["target_progress_p25"]
        ),
        "stressed_observed_net_total": float(
            row["stressed_1_5x"]["observed_net_total"]
        ),
        "stressed_mll_breach_rate": float(
            row["stressed_1_5x"]["mll_breach_rate"]
        ),
        "stressed_consistency_rate": float(
            row["stressed_1_5x"]["consistency_rate"]
        ),
        "maximum_component_profit_share": row.get(
            "maximum_component_positive_profit_share"
        ),
        "maximum_block_pass_share": float(row["maximum_block_pass_share"]),
        "operational_simplicity": int(row["operational_simplicity"]),
    }


def _lookup_predeclared_baseline(
    bank: Sequence[Mapping[str, Any]],
    *,
    role: str,
    size: int,
    risk_level: float,
    components: Sequence[str],
    random_seed: int | None = None,
) -> dict[str, Any]:
    expected_components = tuple(str(value) for value in components)
    matches = []
    for row in bank:
        if _baseline_role(row) != role:
            continue
        if len(tuple(row.get("sleeve_ids") or ())) != size:
            continue
        if float(row.get("risk_level") or 0.0) != risk_level:
            continue
        if tuple(str(value) for value in row.get("sleeve_ids") or ()) != expected_components:
            continue
        observed_seed = row.get("random_seed")
        if random_seed is None:
            if observed_seed is not None:
                continue
        elif observed_seed != random_seed:
            continue
        matches.append(row)
    if len(matches) != 1:
        raise ProductionHalvingError(
            "predeclared baseline lookup must resolve exactly once: "
            f"role={role} size={size} risk={risk_level} seed={random_seed} "
            f"components={expected_components} matches={len(matches)}"
        )
    return dict(matches[0])


def _baseline_role(policy: Mapping[str, Any]) -> str:
    return str(policy.get("baseline_role") or "").strip().upper()


def _aggregate_mixed_observations(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ProductionHalvingError("held-out aggregate has no observations")
    by_block: dict[str, dict[str, Any]] = {}
    blocks = sorted({str(row["temporal_block"]) for row in rows})
    for block in blocks:
        by_block[block] = {
            scenario: _scenario_metrics(
                [
                    row
                    for row in rows
                    if row["temporal_block"] == block and row["scenario"] == scenario
                ]
            )
            for scenario in SCENARIOS
        }
    normal = _scenario_metrics([row for row in rows if row["scenario"] == NORMAL])
    stress = _scenario_metrics([row for row in rows if row["scenario"] == STRESSED])
    passes = {block: value[STRESSED]["pass_count"] for block, value in by_block.items()}
    contribution, complete = _component_contribution(rows)
    return {
        "normal": normal,
        "stressed_1_5x": stress,
        "by_block": by_block,
        "positive_stressed_block_count": sum(
            value[STRESSED]["observed_net_total"] > 0 for value in by_block.values()
        ),
        "stressed_pass_block_count": sum(value > 0 for value in passes.values()),
        "maximum_block_pass_share": _maximum_share(passes),
        "component_attribution_complete": complete,
        "maximum_component_positive_profit_share": (
            _maximum_share({key: max(value, 0.0) for key, value in contribution.items()})
            if complete
            else None
        ),
    }


def _selector_decision(
    selector: Mapping[str, Any],
    baselines: Mapping[str, Mapping[str, Any]],
    *,
    thresholds: Mapping[str, Any],
) -> dict[str, Any]:
    best = baselines["best_parent"]
    equal = baselines["equal_risk"]
    random_rows = [value for key, value in baselines.items() if key.startswith("random_seed_")]
    random_normal = median(
        float(row["normal"]["observed_net_total"]) for row in random_rows
    )
    random_stress = median(
        float(row["stressed_1_5x"]["observed_net_total"]) for row in random_rows
    )
    checks = {
        "normal_net_above_best_parent": float(selector["normal"]["observed_net_total"])
        > float(best["normal"]["observed_net_total"]),
        "stressed_net_above_best_parent": float(
            selector["stressed_1_5x"]["observed_net_total"]
        )
        > float(best["stressed_1_5x"]["observed_net_total"]),
        "target_progress_above_best_parent": float(
            selector["stressed_1_5x"]["target_progress_median"]
        )
        > float(best["stressed_1_5x"]["target_progress_median"]),
        "minimum_positive_blocks": int(selector["positive_stressed_block_count"])
        >= int(thresholds["minimum_positive_economic_blocks"]),
        "minimum_aggregate_passes": int(selector["stressed_1_5x"]["pass_count"])
        >= int(thresholds["minimum_aggregate_passes"]),
        "minimum_pass_blocks": int(selector["stressed_pass_block_count"])
        >= int(thresholds["minimum_pass_blocks"]),
        "positive_stressed_net": float(
            selector["stressed_1_5x"]["observed_net_total"]
        )
        > 0.0,
        "mll_tolerance": float(selector["stressed_1_5x"]["mll_breach_rate"])
        <= float(thresholds["maximum_mll_breach_rate"]),
        "block_concentration": float(selector["maximum_block_pass_share"])
        <= float(thresholds["maximum_block_pass_share"]),
        "component_attribution_complete": bool(
            selector["component_attribution_complete"]
        ),
        "component_concentration": (
            selector["maximum_component_positive_profit_share"] is not None
            and float(selector["maximum_component_positive_profit_share"])
            <= float(thresholds["maximum_component_profit_share"])
        ),
        "above_equal_risk_stressed_net": float(
            selector["stressed_1_5x"]["observed_net_total"]
        )
        > float(equal["stressed_1_5x"]["observed_net_total"]),
        "above_random_median_normal_net": float(
            selector["normal"]["observed_net_total"]
        )
        > random_normal,
        "above_random_median_stressed_net": float(
            selector["stressed_1_5x"]["observed_net_total"]
        )
        > random_stress,
    }
    if all(checks.values()):
        status = "SELECTOR_PROCEDURE_GREEN"
    elif (
        checks["positive_stressed_net"]
        and checks["mll_tolerance"]
        and (
            checks["stressed_net_above_best_parent"]
            or checks["target_progress_above_best_parent"]
        )
    ):
        status = "SELECTOR_PROCEDURE_WEAK"
    else:
        status = "SELECTOR_PROCEDURE_FALSIFIED"
    return {
        "status": status,
        "checks": checks,
        "thresholds": dict(thresholds),
        "thresholds_changed_after_results": False,
        "family_average_p_value_used_as_sole_decision": False,
        "development_only": True,
    }


def _net_delta(
    left: Mapping[str, Any], right: Mapping[str, Any], scenario: str
) -> float:
    key = "normal" if scenario == NORMAL else "stressed_1_5x"
    return float(left[key]["observed_net_total"]) - float(
        right[key]["observed_net_total"]
    )


def _policy_map(policies: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    output: dict[str, Mapping[str, Any]] = {}
    for row in policies:
        policy_id = str(row.get("policy_id") or "")
        if not policy_id or policy_id in output:
            raise ProductionHalvingError("policy IDs must be unique and non-empty")
        output[policy_id] = row
    if not output:
        raise ProductionHalvingError("candidate policy bank is empty")
    return output


def _operational_simplicity(policy: Mapping[str, Any]) -> int:
    return (
        len(tuple(policy.get("sleeve_ids") or ()))
        + len(tuple(policy.get("route_parameters") or ()))
        + int(policy.get("maximum_simultaneous_positions") or 0)
    )


def _maximum_share(values: Mapping[str, float | int]) -> float:
    positive = [max(float(value), 0.0) for value in values.values()]
    total = sum(positive)
    return max(positive, default=0.0) / total if total > 0.0 else 0.0


def _horizon_key(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        raise ProductionHalvingError("episode horizon is required")
    if isinstance(value, int):
        if value < 1:
            raise ProductionHalvingError("episode horizon must be positive")
        return f"{value}_TRADING_DAYS"
    text = str(value).strip().upper()
    if text.isdigit():
        return f"{int(text)}_TRADING_DAYS"
    if text.endswith("_TRADING_DAYS") or text == "FULL_AVAILABLE_CHRONOLOGICAL_HORIZON":
        return text
    raise ProductionHalvingError(f"invalid episode horizon: {value!r}")


def _unique_nonempty(values: Iterable[Any], label: str) -> tuple[str, ...]:
    output = tuple(str(value) for value in values)
    if not output or any(not value for value in output) or len(set(output)) != len(output):
        raise ProductionHalvingError(f"{label} must be non-empty and unique")
    return output


def _unique_ints(values: Iterable[Any], label: str) -> tuple[int, ...]:
    output: list[int] = []
    for value in values:
        if isinstance(value, bool) or int(value) != value or int(value) < 0:
            raise ProductionHalvingError(f"{label} must contain non-negative integers")
        output.append(int(value))
    if len(set(output)) != len(output):
        raise ProductionHalvingError(f"{label} must be unique")
    return tuple(output)


def _finite(value: Any, label: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ProductionHalvingError(f"{label} must be finite")
    return number


def _percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _verify_self_hash(value: Mapping[str, Any], field: str, label: str) -> None:
    payload = dict(value)
    claimed = str(payload.pop(field, ""))
    if not claimed or stable_hash(payload) != claimed:
        raise ProductionHalvingError(f"{label} hash drift")


__all__ = [
    "BASELINE_VERSION",
    "CROSSFIT_VERSION",
    "HALVING_VERSION",
    "ProductionHalvingError",
    "aggregate_policy_evidence",
    "build_baseline_policies",
    "build_compact_outputs",
    "build_final_result_payload",
    "build_leave_one_block_out_plan",
    "classify_failure_vectors",
    "complete_leave_one_block_out",
    "development_decision",
    "pareto_select",
    "select_stage3_survivors",
    "select_stage4_survivors",
    "select_stage5_survivors",
]
