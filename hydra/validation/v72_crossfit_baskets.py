from __future__ import annotations

import hashlib
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import (
    AccountPolicyEpisode,
    RoutedTrade,
    run_shared_account_episode,
)
from hydra.account_policy.schema import BasketPolicy
from hydra.account_policy.v72_static_basket import (
    FrozenRotationBasket,
    V72BasketStructure,
    V72_POLICY_VERSION,
    freeze_rotation_basket,
    scale_trade_path_event,
)
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.validation.v72_component_bank import (
    ComponentEventPaths,
    load_v72_component_event_paths,
)
from hydra.validation.v72_basket_search_freeze import RESERVATION_EVENT_ID
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.2-pareto-crossfit-account-policy-0001-2026-07-13.json"
POLICY_SHA256 = "94f4ad89a2ae2ea347f1fce4a9cb4682690652429f34e42e72edf79e03da6677"
COMPONENT_BANK_PATH = "WORM/v7.2-component-bank-0001-2026-07-13.json"
COMPONENT_BANK_SHA256 = "36987e68a670345c890e9d7d2d060263a13f1e94928563f777dfdc572773ba4c"
SEARCH_MANIFEST_PATH = "WORM/v7.2-static-basket-search-0001-2026-07-13.json"
SEARCH_MANIFEST_SHA256 = "9d0fccf04203d75a7d1f0648ed0ad619882f3dad06ed1f55da3f97878e8b1f98"
EXPECTED_GLOBAL_N_TRIALS = 264_911


class V72CrossFitError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CrossFitBlock:
    block_id: str
    start_day: int
    session_days: tuple[int, ...]


def run_v72_crossfit_baskets(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_2/crossfit_0001",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, bank, search = _verify_inputs(root, proof_registry_path)
    primary_rows = list(bank["primary_components"])
    event_paths, eligible_days = load_v72_component_event_paths(root, primary_rows)
    blocks = _build_blocks(policy, eligible_days)
    structures = tuple(
        V72BasketStructure(
            structure_id=str(row["structure_id"]),
            component_ids=tuple(str(value) for value in row["component_ids"]),
            allocation_profile=str(row["allocation_profile"]),
            structural_hash=str(row["structural_hash"]),
        )
        for row in search["structures"]
    )
    if len(structures) != 1_009:
        raise V72CrossFitError("frozen basket structure population drift")
    roles = {str(row["candidate_id"]): str(row["role"]) for row in primary_rows}
    routed_cache = _build_routed_cache(event_paths, blocks)
    individual = _evaluate_individual_components(
        sorted(roles), routed_cache, blocks
    )

    # Pass one: every rotation is selected and persisted before any held-out result.
    rotation_designs: list[dict[str, Any]] = []
    for held_out in blocks:
        design_blocks = tuple(row for row in blocks if row.block_id != held_out.block_id)
        frozen = tuple(
            freeze_rotation_basket(
                structure,
                individual_design_stress_net={
                    candidate_id: float(
                        sum(
                            individual[candidate_id][block.block_id]["STRESS_1_5X"][
                                "net_pnl"
                            ]
                            for block in design_blocks
                        )
                    )
                    for candidate_id in roles
                },
                component_roles=roles,
                design_block_ids=tuple(row.block_id for row in design_blocks),
                held_out_block_id=held_out.block_id,
            )
            for structure in structures
        )
        records = [
            _evaluate_design_basket(
                basket,
                design_blocks=design_blocks,
                routed_cache=routed_cache,
            )
            for basket in frozen
        ]
        selected, frontier_count, hard_filter_count = select_rotation_frontier(records)
        manifest = {
            "schema": "hydra_v7_2_rotation_selection_manifest_v1",
            "search_id": search["search_id"],
            "held_out_block_id": held_out.block_id,
            "design_block_ids": [row.block_id for row in design_blocks],
            "held_out_block_used_in_selection": False,
            "held_out_results_read": False,
            "structure_count": len(records),
            "hard_filter_pass_count": hard_filter_count,
            "pareto_frontier_count": frontier_count,
            "selected_count": len(selected),
            "selected_baskets": selected,
            "design_population_hash": _stable_hash(records),
            "selection_function": "frozen_hard_filter_then_pareto_then_lexicographic_frontier_order",
            "retuning_after_held_out_result": False,
        }
        manifest["rotation_manifest_hash"] = _stable_hash(manifest)
        path = _rotation_manifest_path(root, Path(output_dir), held_out.block_id)
        _write_once_json(path, manifest)
        rotation_designs.append(
            {
                "held_out": held_out,
                "design_blocks": design_blocks,
                "manifest": manifest,
                "manifest_path": path,
                "manifest_sha256": _sha256(path),
            }
        )

    if len(rotation_designs) != 4 or any(
        row["manifest"]["held_out_results_read"] is not False
        for row in rotation_designs
    ):
        raise V72CrossFitError("all four rotations must freeze before held-out reads")

    # Pass two: evaluate only already-frozen baskets once on the held-out block.
    held_out_results: list[dict[str, Any]] = []
    for rotation in rotation_designs:
        held_out = rotation["held_out"]
        for selected in rotation["manifest"]["selected_baskets"]:
            basket = _frozen_basket_from_dict(selected["frozen_basket"])
            base = _run_basket_block(
                basket, held_out, routed_cache=routed_cache, stress="BASE"
            )
            stressed = _run_basket_block(
                basket,
                held_out,
                routed_cache=routed_cache,
                stress="STRESS_1_5X",
            )
            parents = _parent_metrics_for_occurrence(
                basket, held_out, individual=individual
            )
            leave_one_out = _leave_one_out_occurrence(
                basket,
                held_out,
                routed_cache=routed_cache,
            )
            held_out_results.append(
                {
                    "rotation_manifest_path": str(
                        rotation["manifest_path"].relative_to(root)
                    ),
                    "rotation_manifest_sha256": rotation["manifest_sha256"],
                    "held_out_block_id": held_out.block_id,
                    "operational_signature": _operational_signature(basket),
                    "frozen_basket": basket.to_dict(),
                    "design_metrics": selected["design_metrics"],
                    "unseen_metrics": {
                        "BASE": _episode_metrics(base),
                        "STRESS_1_5X": _episode_metrics(stressed),
                    },
                    "parent_metrics": parents,
                    "component_marginal_contribution": leave_one_out,
                    "held_out_read_once": True,
                    "retuned_after_held_out": False,
                }
            )

    grouped = aggregate_crossfit_groups(held_out_results)
    status_counts = Counter(str(row["status"]) for row in grouped)
    survivors = [
        row for row in grouped if row["status"] == "BASKET_CROSS_FIT_SURVIVOR"
    ]
    promotion = [row for row in survivors if bool(row["promotion_to_48_starts"])]
    result = {
        "schema": "hydra_v7_2_static_basket_crossfit_result_v1",
        "experiment_id": "hydra_v7_2_static_basket_crossfit_0001",
        "verdict": "GREEN" if survivors else "NULL",
        "scientific_status": (
            "BASKET_CROSS_FIT_SURVIVORS_IDENTIFIED"
            if survivors
            else "BASKET_RESEARCH_FAILED_NO_CROSS_FIT_SURVIVOR"
        ),
        "policy_path": POLICY_PATH,
        "policy_sha256": POLICY_SHA256,
        "component_bank_path": COMPONENT_BANK_PATH,
        "component_bank_sha256": COMPONENT_BANK_SHA256,
        "search_manifest_path": SEARCH_MANIFEST_PATH,
        "search_manifest_sha256": SEARCH_MANIFEST_SHA256,
        "multiplicity_reservation_event_id": RESERVATION_EVENT_ID,
        "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "campaign_effective_N_trials": 1_513.5,
        "component_count": len(primary_rows),
        "structure_count": len(structures),
        "cross_fit_rotation_count": len(rotation_designs),
        "design_episode_count": len(structures) * 4 * 3 * 2,
        "held_out_basket_evaluation_count": len(held_out_results),
        "held_out_episode_count": len(held_out_results) * 2,
        "rotation_summaries": [
            {
                "held_out_block_id": row["held_out"].block_id,
                "design_block_ids": [block.block_id for block in row["design_blocks"]],
                "manifest_path": str(row["manifest_path"].relative_to(root)),
                "manifest_sha256": row["manifest_sha256"],
                "hard_filter_pass_count": row["manifest"]["hard_filter_pass_count"],
                "pareto_frontier_count": row["manifest"]["pareto_frontier_count"],
                "selected_count": row["manifest"]["selected_count"],
            }
            for row in rotation_designs
        ],
        "selected_held_out_results": held_out_results,
        "operational_basket_results": grouped,
        "status_counts": dict(sorted(status_counts.items())),
        "cross_fit_survivor_count": len(survivors),
        "promotion_to_48_starts_count": len(promotion),
        "risk_overlay_authorized_count": len(survivors),
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "design_and_unseen_metrics_separated": True,
        "all_rotation_manifests_frozen_before_held_out_read": True,
        "CONTRE": (
            "Only four ten-session blocks exist. Cross-fit selection is leakage-safe, "
            "but a survivor remains underpowered development evidence and every longer "
            "20/40/60/90-day conclusion is censored until fresh data exists."
        ),
        "prochaine_action": (
            "preregister_and_test_bounded_overlays_on_static_survivors"
            if survivors
            else "retain_components_and_pivot_to_new_distinct_mechanisms_without_data_purchase"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def select_rotation_frontier(
    records: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    eligible = [row for row in records if bool(row["hard_filter_passed"])]
    frontier: list[Mapping[str, Any]] = []
    for candidate in eligible:
        if any(_dominates(other, candidate) for other in eligible if other is not candidate):
            continue
        frontier.append(candidate)
    ordered = sorted(
        frontier,
        key=lambda row: (
            -float(row["design_metrics"]["STRESS_1_5X"]["account_net_sum"]),
            -float(
                row["design_metrics"]["STRESS_1_5X"][
                    "maximum_target_progress_median"
                ]
            ),
            float(row["design_metrics"]["STRESS_1_5X"]["mll_breach_rate"]),
            -float(row["design_metrics"]["STRESS_1_5X"]["consistency_pass_rate"]),
            float(row["design_metrics"]["STRESS_1_5X"]["conflict_rate"]),
            str(row["frozen_basket"]["basket_hash"]),
        ),
    )
    return [dict(row) for row in ordered[:3]], len(frontier), len(eligible)


def aggregate_crossfit_groups(
    held_out_results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in held_out_results:
        groups[str(row["operational_signature"])].append(row)
    output: list[dict[str, Any]] = []
    for signature, occurrences in sorted(groups.items()):
        basket = occurrences[0]["frozen_basket"]
        base = _aggregate_metric_rows(
            [row["unseen_metrics"]["BASE"] for row in occurrences]
        )
        stressed = _aggregate_metric_rows(
            [row["unseen_metrics"]["STRESS_1_5X"] for row in occurrences]
        )
        parent_by_id: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for occurrence in occurrences:
            for candidate_id, metrics in occurrence["parent_metrics"].items():
                parent_by_id[candidate_id].append(metrics["STRESS_1_5X"])
        parents = {
            candidate_id: _aggregate_metric_rows(rows)
            for candidate_id, rows in parent_by_id.items()
        }
        useful_blocks = sum(_useful_block(row) for row in occurrences)
        selected_rotations = len(occurrences)
        parent_improvement = _parent_improvement(stressed, parents)
        safer_parent_mll = min(
            (float(row["mll_breach_rate"]) for row in parents.values()),
            default=1.0,
        )
        dominated = any(
            _parent_dominates_basket(parent, stressed) for parent in parents.values()
        )
        marginal = _aggregate_marginal_contributions(occurrences, stressed)
        harmful_components = sorted(
            candidate_id
            for candidate_id, row in marginal.items()
            if bool(row["removal_dominates_full_basket"])
        )
        gate_checks = {
            "minimum_rotations_selected": selected_rotations >= 2,
            "unseen_normal_net_positive": base["account_net_sum"] > 0.0,
            "unseen_stress_net_positive": stressed["account_net_sum"] > 0.0,
            "minimum_useful_unseen_blocks": useful_blocks >= 2,
            "minimum_parent_improvement": parent_improvement,
            "maximum_mll_breach_rate": stressed["mll_breach_rate"] <= 0.2,
            "maximum_mll_deterioration": (
                stressed["mll_breach_rate"] - safer_parent_mll <= 0.1 + 1.0e-12
            ),
            "not_dominated_by_parent": not dominated,
            "no_component_removal_dominates": not harmful_components,
        }
        survivor = all(gate_checks.values())
        promotion_checks = {
            "path_A": bool(
                stressed["pass_count"] >= 2
                and stressed["pass_contributing_block_count"] >= 2
            ),
            "path_B": bool(
                stressed["maximum_target_progress_median"] >= 0.75
                and stressed["mll_breach_rate"] <= 0.1
                and stressed["account_net_sum"] > 0.0
                and _progress_improves_all_parents(stressed, parents, margin=0.05)
            ),
        }
        promote = bool(survivor and any(promotion_checks.values()))
        output.append(
            {
                "operational_signature": signature,
                "component_ids": list(basket["component_ids"]),
                "allocation_profile": basket["allocation_profile"],
                "component_risk_units": basket["component_risk_units"],
                "component_priority": basket["component_priority"],
                "selected_rotation_count": selected_rotations,
                "held_out_block_ids": sorted(
                    str(row["held_out_block_id"]) for row in occurrences
                ),
                "unseen_BASE": base,
                "unseen_STRESS_1_5X": stressed,
                "parent_STRESS_1_5X": parents,
                "useful_unseen_block_count": useful_blocks,
                "component_marginal_contribution": marginal,
                "harmful_component_ids": harmful_components,
                "dominated_by_best_single_parent": dominated,
                "survivor_gate_checks": gate_checks,
                "status": (
                    "BASKET_CROSS_FIT_SURVIVOR"
                    if survivor
                    else "BASKET_RESEARCH_FAILED"
                ),
                "promotion_gate_checks": promotion_checks,
                "promotion_to_48_starts": promote,
            }
        )
    return output


def _evaluate_design_basket(
    basket: FrozenRotationBasket,
    *,
    design_blocks: Sequence[CrossFitBlock],
    routed_cache: Mapping[tuple[str, str, int, str], tuple[RoutedTrade, ...]],
) -> dict[str, Any]:
    metrics = {
        stress: _aggregate_episodes(
            [
                _run_basket_block(
                    basket, block, routed_cache=routed_cache, stress=stress
                )
                for block in design_blocks
            ]
        )
        for stress in ("BASE", "STRESS_1_5X")
    }
    hard_filter = bool(
        metrics["BASE"]["account_net_sum"] > 0.0
        and metrics["STRESS_1_5X"]["account_net_sum"] > 0.0
        and metrics["STRESS_1_5X"]["mll_breach_rate"] <= 0.2
        and metrics["BASE"]["hard_rule_failure_count"] == 0
        and metrics["STRESS_1_5X"]["hard_rule_failure_count"] == 0
    )
    return {
        "source_structure_id": basket.source_structure_id,
        "frozen_basket": basket.to_dict(),
        "design_metrics": metrics,
        "hard_filter_passed": hard_filter,
    }


def _dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    a = left["design_metrics"]["STRESS_1_5X"]
    b = right["design_metrics"]["STRESS_1_5X"]
    maximize = (
        "account_net_sum",
        "maximum_target_progress_median",
        "consistency_pass_rate",
    )
    minimize = ("mll_breach_rate", "conflict_rate")
    weak = all(float(a[key]) >= float(b[key]) for key in maximize) and all(
        float(a[key]) <= float(b[key]) for key in minimize
    )
    strict = any(float(a[key]) > float(b[key]) for key in maximize) or any(
        float(a[key]) < float(b[key]) for key in minimize
    )
    return bool(weak and strict)


def _run_basket_block(
    basket: FrozenRotationBasket,
    block: CrossFitBlock,
    *,
    routed_cache: Mapping[tuple[str, str, int, str], tuple[RoutedTrade, ...]],
    stress: str,
) -> AccountPolicyEpisode:
    component_events = {
        candidate_id: routed_cache[
            (candidate_id, stress, basket.risk_units[candidate_id], block.block_id)
        ]
        for candidate_id in basket.component_ids
    }
    policy = BasketPolicy(
        policy_id=basket.basket_id,
        component_ids=basket.component_ids,
        archetype=basket.allocation_profile,
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        conflict_policy="FROZEN_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=basket.component_priority,
        policy_version=V72_POLICY_VERSION,
    )
    return run_shared_account_episode(
        component_events,
        block.session_days,
        basket=policy,
        start_day=block.start_day,
        maximum_duration_days=len(block.session_days),
        config=Topstep150KConfig(mll_mode=MllMode.EOD_LEVEL_RT_BREACH),
    )


def _build_routed_cache(
    paths: Mapping[str, ComponentEventPaths], blocks: Sequence[CrossFitBlock]
) -> dict[tuple[str, str, int, str], tuple[RoutedTrade, ...]]:
    cache: dict[tuple[str, str, int, str], tuple[RoutedTrade, ...]] = {}
    for candidate_id, source in paths.items():
        for stress, events in (
            ("BASE", source.base_events),
            ("STRESS_1_5X", source.stress_events),
        ):
            for units in (1, 2):
                for block in blocks:
                    days = set(block.session_days)
                    cache[(candidate_id, stress, units, block.block_id)] = tuple(
                        RoutedTrade(
                            component_id=candidate_id,
                            market="ES",
                            side=int(side),
                            event=scale_trade_path_event(event, units=units),
                        )
                        for side, event in zip(source.sides, events, strict=True)
                        if int(event.session_day) in days
                    )
    return cache


def _evaluate_individual_components(
    candidate_ids: Sequence[str],
    routed_cache: Mapping[tuple[str, str, int, str], tuple[RoutedTrade, ...]],
    blocks: Sequence[CrossFitBlock],
) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
    output: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    for candidate_id in candidate_ids:
        output[candidate_id] = {}
        for block in blocks:
            output[candidate_id][block.block_id] = {}
            for stress in ("BASE", "STRESS_1_5X"):
                basket = FrozenRotationBasket(
                    basket_id=f"individual_{candidate_id}_{block.block_id}_{stress}",
                    source_structure_id=f"individual_{candidate_id}",
                    source_structural_hash=_stable_hash(candidate_id),
                    component_ids=(candidate_id,),
                    allocation_profile="UNIT_EQUAL",
                    component_risk_units=((candidate_id, 1),),
                    component_priority=(candidate_id,),
                    design_block_ids=(),
                    held_out_block_id=block.block_id,
                    policy_version=V72_POLICY_VERSION,
                    basket_hash=_stable_hash((candidate_id, block.block_id, stress)),
                )
                output[candidate_id][block.block_id][stress] = _episode_metrics(
                    _run_basket_block(
                        basket,
                        block,
                        routed_cache=routed_cache,
                        stress=stress,
                    )
                )
    return output


def _episode_metrics(episode: AccountPolicyEpisode) -> dict[str, Any]:
    status = {
        CombineTerminal.PASSED: "TARGET_REACHED",
        CombineTerminal.MLL_BREACH: "MLL_BREACHED",
        CombineTerminal.TIMEOUT: "DATA_CENSORED",
        CombineTerminal.COMPLIANCE_FAILURE: "HARD_RULE_FAILURE",
    }[episode.terminal]
    return {
        "observation_status": status,
        "pass_count": int(episode.passed),
        "mll_breach_count": int(episode.mll_breached),
        "hard_rule_failure_count": int(
            episode.terminal is CombineTerminal.COMPLIANCE_FAILURE
        ),
        "net_pnl": float(episode.net_pnl),
        "total_cost": float(episode.total_cost),
        "target_progress": float(episode.target_progress),
        "maximum_target_progress": float(episode.maximum_target_progress),
        "minimum_mll_buffer": float(episode.minimum_mll_buffer),
        "consistency_ok": bool(episode.consistency_ok),
        "days_to_target": episode.days_to_target,
        "eligible_days": int(episode.eligible_days),
        "conflict_count": int(episode.conflict_count),
        "accepted_events": int(episode.accepted_events),
        "skipped_events": int(episode.skipped_events),
        "component_contribution": dict(episode.component_contribution),
    }


def _aggregate_episodes(
    episodes: Sequence[AccountPolicyEpisode],
) -> dict[str, Any]:
    return _aggregate_metric_rows([_episode_metrics(row) for row in episodes])


def _aggregate_metric_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise V72CrossFitError("cannot aggregate an empty metric set")
    maximum_progress = np.asarray(
        [float(row["maximum_target_progress"]) for row in rows]
    )
    net = np.asarray([float(row["net_pnl"]) for row in rows])
    passes = sum(int(row["pass_count"]) for row in rows)
    mll = sum(int(row["mll_breach_count"]) for row in rows)
    accepted = sum(int(row["accepted_events"]) for row in rows)
    skipped = sum(int(row["skipped_events"]) for row in rows)
    conflicts = sum(int(row["conflict_count"]) for row in rows)
    median_progress = float(np.median(maximum_progress))
    projected = 10.0 / median_progress if median_progress > 0.0 else None
    return {
        "episode_count": len(rows),
        "pass_count": passes,
        "pass_rate": passes / len(rows),
        "pass_contributing_block_count": sum(int(row["pass_count"]) > 0 for row in rows),
        "mll_breach_count": mll,
        "mll_breach_rate": mll / len(rows),
        "hard_rule_failure_count": sum(
            int(row["hard_rule_failure_count"]) for row in rows
        ),
        "data_censored_count": sum(
            row["observation_status"] == "DATA_CENSORED" for row in rows
        ),
        "account_net_sum": float(np.sum(net)),
        "account_net_median": float(np.median(net)),
        "account_net_p25": float(np.percentile(net, 25)),
        "account_net_p75": float(np.percentile(net, 75)),
        "maximum_target_progress_median": median_progress,
        "maximum_target_progress_p25": float(np.percentile(maximum_progress, 25)),
        "maximum_target_progress_p75": float(np.percentile(maximum_progress, 75)),
        "projected_days_to_target": projected,
        "minimum_mll_buffer": float(
            min(float(row["minimum_mll_buffer"]) for row in rows)
        ),
        "consistency_pass_rate": float(
            np.mean([bool(row["consistency_ok"]) for row in rows])
        ),
        "conflict_rate": conflicts / max(accepted + skipped, 1),
        "conflict_count": conflicts,
        "accepted_event_count": accepted,
        "skipped_event_count": skipped,
    }


def _parent_metrics_for_occurrence(
    basket: FrozenRotationBasket,
    block: CrossFitBlock,
    *,
    individual: Mapping[str, Mapping[str, Mapping[str, Mapping[str, Any]]]],
) -> dict[str, Any]:
    # Unit-risk parents are the preregistered fair baseline; risk tilt cannot claim
    # basket improvement merely by doubling a standalone component.
    return {
        candidate_id: {
            "BASE": dict(individual[candidate_id][block.block_id]["BASE"]),
            "STRESS_1_5X": dict(
                individual[candidate_id][block.block_id]["STRESS_1_5X"]
            ),
        }
        for candidate_id in basket.component_ids
    }


def _leave_one_out_occurrence(
    basket: FrozenRotationBasket,
    block: CrossFitBlock,
    *,
    routed_cache: Mapping[tuple[str, str, int, str], tuple[RoutedTrade, ...]],
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for removed in basket.component_ids:
        members = tuple(value for value in basket.component_ids if value != removed)
        if not members:
            continue
        reduced_units = tuple(
            (key, value)
            for key, value in basket.component_risk_units
            if key != removed
        )
        reduced_priority = tuple(
            value for value in basket.component_priority if value != removed
        )
        reduced = FrozenRotationBasket(
            basket_id=f"{basket.basket_id}_without_{removed}",
            source_structure_id=basket.source_structure_id,
            source_structural_hash=basket.source_structural_hash,
            component_ids=members,
            allocation_profile=basket.allocation_profile,
            component_risk_units=reduced_units,
            component_priority=reduced_priority,
            design_block_ids=basket.design_block_ids,
            held_out_block_id=basket.held_out_block_id,
            policy_version=basket.policy_version,
            basket_hash=_stable_hash((basket.basket_hash, "without", removed)),
        )
        output[removed] = {
            stress: _episode_metrics(
                _run_basket_block(
                    reduced, block, routed_cache=routed_cache, stress=stress
                )
            )
            for stress in ("BASE", "STRESS_1_5X")
        }
    return output


def _aggregate_marginal_contributions(
    occurrences: Sequence[Mapping[str, Any]],
    full_stressed: Mapping[str, Any],
) -> dict[str, Any]:
    component_rows: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for occurrence in occurrences:
        for candidate_id, metrics in occurrence[
            "component_marginal_contribution"
        ].items():
            component_rows[candidate_id].append(metrics["STRESS_1_5X"])
    output: dict[str, Any] = {}
    for candidate_id, rows in component_rows.items():
        reduced = _aggregate_metric_rows(rows)
        dominates = bool(
            reduced["pass_count"] >= full_stressed["pass_count"]
            and reduced["mll_breach_rate"] <= full_stressed["mll_breach_rate"]
            and reduced["account_net_sum"] > full_stressed["account_net_sum"]
        )
        output[candidate_id] = {
            "without_component": reduced,
            "component_net_contribution": float(
                full_stressed["account_net_sum"] - reduced["account_net_sum"]
            ),
            "component_progress_contribution": float(
                full_stressed["maximum_target_progress_median"]
                - reduced["maximum_target_progress_median"]
            ),
            "component_pass_contribution": int(
                full_stressed["pass_count"] - reduced["pass_count"]
            ),
            "component_mll_survival_contribution": float(
                reduced["mll_breach_rate"] - full_stressed["mll_breach_rate"]
            ),
            "removal_dominates_full_basket": dominates,
        }
    return dict(sorted(output.items()))


def _useful_block(row: Mapping[str, Any]) -> bool:
    basket = row["unseen_metrics"]["STRESS_1_5X"]
    parents = [
        value["STRESS_1_5X"] for value in row["parent_metrics"].values()
    ]
    best_progress = max(float(value["maximum_target_progress"]) for value in parents)
    return bool(
        float(basket["net_pnl"]) > 0.0
        and (
            int(basket["pass_count"]) > 0
            or float(basket["maximum_target_progress"]) >= best_progress + 0.05
        )
    )


def _parent_improvement(
    basket: Mapping[str, Any], parents: Mapping[str, Mapping[str, Any]]
) -> bool:
    return bool(
        basket["pass_count"] > max(row["pass_count"] for row in parents.values())
        or _progress_improves_all_parents(basket, parents, margin=0.05)
        or _projected_days_improve_all_parents(basket, parents, fraction=0.9)
    )


def _progress_improves_all_parents(
    basket: Mapping[str, Any],
    parents: Mapping[str, Mapping[str, Any]],
    *,
    margin: float,
) -> bool:
    return bool(
        float(basket["maximum_target_progress_median"])
        >= max(float(row["maximum_target_progress_median"]) for row in parents.values())
        + margin
    )


def _projected_days_improve_all_parents(
    basket: Mapping[str, Any],
    parents: Mapping[str, Mapping[str, Any]],
    *,
    fraction: float,
) -> bool:
    basket_days = basket["projected_days_to_target"]
    parent_days = [
        row["projected_days_to_target"]
        for row in parents.values()
        if row["projected_days_to_target"] is not None
    ]
    return bool(
        basket_days is not None
        and parent_days
        and float(basket_days) <= fraction * min(float(value) for value in parent_days)
    )


def _parent_dominates_basket(
    parent: Mapping[str, Any], basket: Mapping[str, Any]
) -> bool:
    weak = bool(
        parent["pass_count"] >= basket["pass_count"]
        and parent["maximum_target_progress_median"]
        >= basket["maximum_target_progress_median"]
        and parent["mll_breach_rate"] <= basket["mll_breach_rate"]
        and parent["account_net_sum"] >= basket["account_net_sum"]
    )
    strict = bool(
        parent["pass_count"] > basket["pass_count"]
        or parent["maximum_target_progress_median"]
        > basket["maximum_target_progress_median"]
        or parent["mll_breach_rate"] < basket["mll_breach_rate"]
        or parent["account_net_sum"] > basket["account_net_sum"]
    )
    return weak and strict


def _build_blocks(
    policy: Mapping[str, Any], eligible_days: Sequence[int]
) -> tuple[CrossFitBlock, ...]:
    days = tuple(sorted({int(value) for value in eligible_days}))
    positions = {day: position for position, day in enumerate(days)}
    output: list[CrossFitBlock] = []
    for frozen in policy["cross_fit"]["blocks"]:
        start = int(frozen["start_session_day"])
        duration = int(frozen["duration_sessions"])
        if start not in positions:
            raise V72CrossFitError(f"cross-fit start is absent: {start}")
        block_days = days[positions[start] : positions[start] + duration]
        if len(block_days) != duration:
            raise V72CrossFitError("cross-fit block is truncated")
        output.append(
            CrossFitBlock(
                block_id=str(frozen["block_id"]),
                start_day=start,
                session_days=block_days,
            )
        )
    if len(output) != 4 or len({day for row in output for day in row.session_days}) != 40:
        raise V72CrossFitError("cross-fit blocks overlap or count drift")
    return tuple(output)


def _operational_signature(basket: FrozenRotationBasket) -> str:
    return _stable_hash(
        {
            "component_ids": list(basket.component_ids),
            "allocation_profile": basket.allocation_profile,
            "component_risk_units": dict(basket.component_risk_units),
            "component_priority": list(basket.component_priority),
            "policy_version": basket.policy_version,
        }
    )


def _frozen_basket_from_dict(value: Mapping[str, Any]) -> FrozenRotationBasket:
    return FrozenRotationBasket(
        basket_id=str(value["basket_id"]),
        source_structure_id=str(value["source_structure_id"]),
        source_structural_hash=str(value["source_structural_hash"]),
        component_ids=tuple(str(row) for row in value["component_ids"]),
        allocation_profile=str(value["allocation_profile"]),
        component_risk_units=tuple(
            sorted(
                (str(key), int(amount))
                for key, amount in value["component_risk_units"].items()
            )
        ),
        component_priority=tuple(str(row) for row in value["component_priority"]),
        design_block_ids=tuple(str(row) for row in value["design_block_ids"]),
        held_out_block_id=str(value["held_out_block_id"]),
        policy_version=str(value["policy_version"]),
        basket_hash=str(value["basket_hash"]),
    )


def _rotation_manifest_path(root: Path, output_dir: Path, block_id: str) -> Path:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    path = destination / "rotations" / f"{block_id}_selection_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    expected = {
        POLICY_PATH: POLICY_SHA256,
        COMPONENT_BANK_PATH: COMPONENT_BANK_SHA256,
        SEARCH_MANIFEST_PATH: SEARCH_MANIFEST_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V72CrossFitError("frozen V7.2 source drift: " + ",".join(drift))
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    registry = load_and_verify(proof_path)
    if multiplicity_trial_count(registry) != EXPECTED_GLOBAL_N_TRIALS:
        raise V72CrossFitError("V7.2 multiplicity reservation is absent")
    if burned_window_ids(registry) != ("Q4_2024",):
        raise V72CrossFitError("unexpected proof-window state")
    reservation = next(
        (
            row
            for row in registry["entries"]
            if row["event_id"] == RESERVATION_EVENT_ID
        ),
        None,
    )
    if reservation is None or reservation["status"] != (
        "RESERVED_BEFORE_V72_STATIC_BASKET_RESULTS"
    ):
        raise V72CrossFitError("V7.2 reservation event mismatch")
    return tuple(
        json.loads((root / path).read_text(encoding="utf-8"))
        for path in (POLICY_PATH, COMPONENT_BANK_PATH, SEARCH_MANIFEST_PATH)
    )  # type: ignore[return-value]


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_static_basket_crossfit_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    unseen = list(result["selected_held_out_results"])
    operational = list(result["operational_basket_results"])
    unseen_base_passes = sum(
        int(row["unseen_metrics"]["BASE"]["pass_count"]) for row in unseen
    )
    unseen_base_breaches = sum(
        int(row["unseen_metrics"]["BASE"]["mll_breach_count"]) for row in unseen
    )
    unseen_base_positive = sum(
        float(row["unseen_metrics"]["BASE"]["net_pnl"]) > 0.0 for row in unseen
    )
    unseen_stress_positive = sum(
        float(row["unseen_metrics"]["STRESS_1_5X"]["net_pnl"]) > 0.0
        for row in unseen
    )
    unseen_base_net = sum(
        float(row["unseen_metrics"]["BASE"]["net_pnl"]) for row in unseen
    )
    unseen_stress_net = sum(
        float(row["unseen_metrics"]["STRESS_1_5X"]["net_pnl"])
        for row in unseen
    )
    maximum_unseen_progress = max(
        (
            float(row["unseen_metrics"]["BASE"]["maximum_target_progress"])
            for row in unseen
        ),
        default=0.0,
    )
    dominated_count = sum(
        bool(row["dominated_by_best_single_parent"]) for row in operational
    )
    report_path = destination / "v72_static_basket_crossfit_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Static basket cross-fit",
            "",
            f"[HYDRA-V7] phase=4 step=185 verdict={result['verdict']}",
            f"gate=V72_STATIC_BASKET_CROSS_FIT preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=4_leave_one_block_out_rotations",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/account_policy/basket.py,hydra/account_policy/v72_static_basket.py,hydra/validation/v72_crossfit_baskets.py CONTRE=quatre_blocs_courts_et_composants_sous_puissants",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Structures évaluées: `{result['structure_count']}`",
            f"- Rotations cross-fit: `{result['cross_fit_rotation_count']}`",
            f"- Épisodes design: `{result['design_episode_count']}`",
            f"- Paniers lus sur blocs invisibles: `{result['held_out_basket_evaluation_count']}`",
            f"- Passages sur blocs invisibles (coûts normaux): `{unseen_base_passes}/{len(unseen)}`",
            f"- Breaches MLL sur blocs invisibles (coûts normaux): `{unseen_base_breaches}/{len(unseen)}`",
            f"- Net positif invisible: normal `{unseen_base_positive}/{len(unseen)}`, stress 1.5x `{unseen_stress_positive}/{len(unseen)}`",
            f"- Net agrégé invisible: normal `${unseen_base_net:.2f}`, stress 1.5x `${unseen_stress_net:.2f}`",
            f"- Progrès cible maximal invisible: `{maximum_unseen_progress * 100.0:.2f}%`",
            f"- Paniers dominés par leur meilleur parent: `{dominated_count}/{len(operational)}`",
            f"- Survivants cross-fit: `{result['cross_fit_survivor_count']}`",
            f"- Promus à 48 starts: `{result['promotion_to_48_starts_count']}`",
            f"- Statuts: `{json.dumps(result['status_counts'], sort_keys=True)}`",
            "- Achats data/Q4/ordres: `0/0/0`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    return {
        **result,
        "result_path": str(result_path),
        "result_sha256": result_hash,
        "report_path": str(report_path),
    }


def _write_once_json(path: Path, payload: Mapping[str, Any]) -> None:
    serialized = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != serialized:
            raise V72CrossFitError(f"frozen rotation manifest drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "CrossFitBlock",
    "V72CrossFitError",
    "aggregate_crossfit_groups",
    "run_v72_crossfit_baskets",
    "select_rotation_frontier",
]
