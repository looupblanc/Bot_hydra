from __future__ import annotations

import hashlib
import json
import subprocess
import time
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.archive import AccountPolicyArchive, PolicyArchiveEntry
from hydra.account_policy.basket import AccountPolicyRollingSummary, RoutedTrade
from hydra.account_policy.controller import generate_controller_population
from hydra.account_policy.evolution import (
    ComponentRuntime,
    generate_basket_population,
    load_v5_component_bank,
    run_account_policy_job,
    select_cluster_primaries,
    summary_from_dict,
    write_component_bank_manifest,
)
from hydra.account_policy.fitness import (
    AccountFitness,
    adaptive_controller_fitness,
    basket_combine_fitness,
    individual_combine_fitness,
    paired_controller_evidence,
)
from hydra.account_policy.target_velocity import (
    TargetVelocityProposal,
    evaluate_target_velocity_outcome,
    generate_target_velocity_mutations,
)
from hydra.account_policy.xfa import evaluate_serial_xfa_basket
from hydra.account_policy.schema import (
    BasketPolicy,
    ComponentDescriptor,
    ComponentRole,
    ControllerPolicy,
    stable_hash,
)
from hydra.compute.resource_monitor import (
    capture_resource_snapshot,
    summarize_task_resources,
)
from hydra.compute.result_writer import AtomicResultWriter
from hydra.compute.worker_pool import LongLivedWorkerPool, PureTask
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.combine_first_evolution_v5 import _canonical_cache_root
from hydra.research.mechanism_grammar_v6 import (
    MechanismGraphSpec,
    build_mechanism_trade_path,
    fast_screen_mechanism,
    generate_mechanism_population,
    run_mechanism_exact_job,
)
from hydra.research.qd_economic_tournament import MARKET_PAIRS
from hydra.research.rolling_combine_replay import (
    build_exact_trade_path,
    run_rolling_combine_job,
)
from hydra.research.turbo_exact_replay import spec_from_dict, spec_to_dict
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.strategies.turbo_batch_fingerprint import structural_fingerprint
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "hydra_account_level_evolution_v6_epoch_v1"
DEFAULT_GRAMMAR_COUNT = 480
DEFAULT_GRAMMAR_EXACT_LIMIT = 72
DEFAULT_BASKET_COUNT = 600
DEFAULT_CONTROLLER_BASKET_LIMIT = 40
DEFAULT_TARGET_VELOCITY_MUTATION_LIMIT = 24
DEFAULT_SCREENING_STARTS = 24
DEFAULT_PROMOTION_STARTS = 48


class AccountLevelV6Error(RuntimeError):
    pass


def run_account_level_evolution_v6(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    contract_map_path: str | Path,
    contract_map_sha256: str,
    code_commit: str,
    source_report_root: str | Path,
    generation_index: int = 0,
    worker_count: int = 3,
    grammar_count: int = DEFAULT_GRAMMAR_COUNT,
    grammar_exact_limit: int = DEFAULT_GRAMMAR_EXACT_LIMIT,
    basket_count: int = DEFAULT_BASKET_COUNT,
    controller_basket_limit: int = DEFAULT_CONTROLLER_BASKET_LIMIT,
    target_velocity_mutation_limit: int = DEFAULT_TARGET_VELOCITY_MUTATION_LIMIT,
    screening_starts: int = DEFAULT_SCREENING_STARTS,
    promotion_starts: int = DEFAULT_PROMOTION_STARTS,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    if worker_count < 1 or worker_count > 3:
        raise AccountLevelV6Error("V6 supports one to three pure compute workers")
    if (
        grammar_count < 200
        or basket_count < 200
        or target_velocity_mutation_limit < 8
    ):
        raise AccountLevelV6Error("V6 production batches are underpowered")
    if screening_starts < 24 or promotion_starts < 48:
        raise AccountLevelV6Error("V6 episode-start policy is underpowered")
    task_path = Path(engineering_task_path)
    roll_map = Path(contract_map_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(roll_map, contract_map_sha256, "explicit-contract map")
    if len(code_commit) == 40:
        actual = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        if actual != code_commit:
            raise AccountLevelV6Error("worker commit differs from frozen V6 specification")
    destination = Path(output_dir)
    writer = AtomicResultWriter(destination)
    preregistration = {
        "schema": "hydra_account_level_v6_preregistration_v1",
        "generation_index": generation_index,
        "populations": [
            "INDIVIDUAL_STRATEGY_POPULATION",
            "STATIC_ACCOUNT_BASKET_POPULATION",
            "ADAPTIVE_ACCOUNT_CONTROLLER_POPULATION",
        ],
        "compute_allocation": {
            "account_basket_and_controller": 0.50,
            "new_mechanism_grammar": 0.30,
            "target_velocity_mutation": 0.15,
            "xfa_defensive": 0.05,
        },
        "grammar_count": grammar_count,
        "grammar_exact_limit": grammar_exact_limit,
        "basket_count": basket_count,
        "controller_basket_limit": controller_basket_limit,
        "target_velocity_mutation_limit": target_velocity_mutation_limit,
        "screening_episode_starts": screening_starts,
        "promotion_episode_starts": promotion_starts,
        "episode_horizons": {
            "SHORT_COMBINE_HORIZON": 30,
            "NORMAL_COMBINE_HORIZON": 60,
            "EXTENDED_DIAGNOSTIC_HORIZON": 90,
            "operational_screening_horizon": "NORMAL_COMBINE_HORIZON",
        },
        "v5_grammar_status": "V5_GRAMMAR_SATURATED_KEEP_ELITES_ONLY",
        "development_period": ["2023-01-01", "2024-10-01"],
        "q4_access_allowed": False,
        "q4_reuse_prohibited": True,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "outbound_order_capability": False,
        "paper_shadow_ready_allowed": False,
        "engineering_task_sha256": engineering_task_sha256,
        "contract_map_sha256": contract_map_sha256,
        "code_commit": code_commit,
    }
    preregistration["preregistration_hash"] = stable_hash(preregistration)
    writer.write_json("account_v6_preregistration.json", preregistration)
    if record_data_access:
        _record_access_once(generation_index)

    resource_before = capture_resource_snapshot()
    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=_canonical_cache_root(), contract_map_path=roll_map
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in sorted(feature_build.market_paths.items())
    }

    existing_components, component_audit = load_v5_component_bank(
        source_report_root,
        matrix_paths=feature_build.market_paths,
        maximum_components=36,
    )
    if len(select_cluster_primaries(existing_components)) < 2:
        raise AccountLevelV6Error("V6 needs at least two distinct exact components")

    historical_grammar = _historical_grammar_fingerprints(source_report_root)
    historical_basket_fingerprints = _historical_basket_fingerprints(
        source_report_root
    )
    historical_target_velocity_fingerprints = (
        _historical_target_velocity_fingerprints(source_report_root)
    )
    grammar_specs = generate_mechanism_population(
        matrices,
        count=grammar_count,
        generation_index=generation_index,
        excluded_fingerprints=historical_grammar,
    )
    existing_signatures = {
        _event_signature(component.events)
        for component in existing_components
    }
    fast_rows: list[dict[str, Any]] = []
    for spec in grammar_specs:
        row = fast_screen_mechanism(spec, matrices[spec.market])
        row["novel_against_component_bank"] = row["event_signature"] not in existing_signatures
        fast_rows.append(row)
    writer.write_jsonl_batch("account_v6_grammar_fast_screen.jsonl", fast_rows)
    spec_by_id = {spec.candidate_id: spec for spec in grammar_specs}
    grammar_survivors = [
        row
        for row in fast_rows
        if row["positive_after_costs"]
        and row["novel_against_component_bank"]
        and float(row["maximum_drawdown"]) <= 9_000.0
    ]
    grammar_survivors = _select_diverse_grammar_survivors(
        grammar_survivors,
        spec_by_id=spec_by_id,
        limit=min(grammar_exact_limit, len(grammar_survivors)),
    )
    exact_specs = [spec_by_id[str(row["candidate_id"])] for row in grammar_survivors]
    grammar_exact_started = time.perf_counter()
    grammar_results, grammar_resources = _evaluate_grammar_specs(
        exact_specs,
        matrix_paths=feature_build.market_paths,
        worker_count=worker_count,
        maximum_episode_starts=screening_starts,
    )
    grammar_exact_seconds = max(time.perf_counter() - grammar_exact_started, 1e-9)
    writer.write_jsonl_batch("account_v6_grammar_exact_results.jsonl", grammar_results)
    existing_structure_fingerprints = {
        structural_fingerprint(spec_from_dict(component.specification))
        for component in existing_components
    }
    target_velocity_proposals = generate_target_velocity_mutations(
        select_cluster_primaries(existing_components),
        generation_index=generation_index,
        maximum=target_velocity_mutation_limit,
        excluded_fingerprints=(
            historical_grammar
            | historical_target_velocity_fingerprints
            | existing_structure_fingerprints
        ),
    )
    target_velocity_results, target_velocity_resources = (
        _evaluate_target_velocity_proposals(
            target_velocity_proposals,
            components=existing_components,
            matrix_paths=feature_build.market_paths,
            worker_count=worker_count,
            maximum_episode_starts=screening_starts,
        )
    )
    target_velocity_outcomes = _target_velocity_outcomes(
        target_velocity_proposals,
        target_velocity_results,
    )
    writer.write_json(
        "account_v6_target_velocity_mutations.json",
        {
            "proposals": [row.to_dict() for row in target_velocity_proposals],
            "outcomes": target_velocity_outcomes,
        },
    )
    new_components = _new_grammar_components(
        grammar_results,
        spec_by_id=spec_by_id,
        matrices=matrices,
        existing_components=existing_components,
        source_experiment=f"account_level_evolution_v6_generation_{generation_index:04d}",
        maximum=16,
    )
    target_velocity_components, target_velocity_admission = (
        _target_velocity_components(
            target_velocity_proposals,
            target_velocity_results,
            target_velocity_outcomes,
            matrices=matrices,
            existing_components=existing_components + new_components,
            source_experiment=(
                f"account_level_evolution_v6_generation_{generation_index:04d}"
            ),
            maximum=target_velocity_mutation_limit,
        )
    )
    writer.write_json(
        "account_v6_target_velocity_admission.json",
        target_velocity_admission,
    )
    all_components = (
        existing_components + new_components + target_velocity_components
    )
    primary_components = select_cluster_primaries(all_components)
    bank_path = destination / "account_v6_component_bank.json"
    bank_manifest = write_component_bank_manifest(bank_path, all_components)
    component_by_id = {
        component.descriptor.component_id: component
        for component in primary_components
    }
    eligible_days = tuple(int(day) for day in bank_manifest["eligible_session_days"])
    screening_policy = EpisodeStartPolicy(
        maximum_starts=screening_starts,
        minimum_spacing_sessions=5,
        minimum_observation_sessions=30,
        maximum_duration_sessions=60,
        regime_balanced=False,
    )
    screening_start_days = select_episode_starts(
        eligible_days, policy=screening_policy
    )
    promotion_policy = EpisodeStartPolicy(
        maximum_starts=promotion_starts,
        minimum_spacing_sessions=2,
        minimum_observation_sessions=30,
        maximum_duration_sessions=60,
        regime_balanced=False,
    )
    promotion_start_days = select_episode_starts(
        eligible_days, policy=promotion_policy
    )

    individual_policies = [
        BasketPolicy(
            policy_id="individual_v6_" + stable_hash(component_id)[:18],
            component_ids=(component_id,),
            archetype="INDIVIDUAL_STRATEGY",
            maximum_simultaneous_positions=1,
            maximum_mini_equivalent=15,
            component_priority=(component_id,),
        )
        for component_id in sorted(component_by_id)
    ]
    basket_policies = generate_basket_population(
        primary_components,
        count=basket_count,
        generation_index=generation_index,
        excluded_fingerprints=historical_basket_fingerprints,
    )
    if len(basket_policies) < 200:
        raise AccountLevelV6Error(
            f"basket diversity is underpowered: {len(basket_policies)}"
        )
    writer.write_jsonl_batch(
        "account_v6_basket_population.jsonl",
        [policy.to_dict() for policy in basket_policies],
    )

    account_started = time.perf_counter()
    with LongLivedWorkerPool(max_workers=worker_count) as pool:
        initial_tasks = [
            _policy_task(
                policy,
                component_bank_path=bank_path,
                start_days=screening_start_days,
                maximum_starts=screening_starts,
            )
            for policy in individual_policies + basket_policies
        ]
        initial_task_started = time.perf_counter()
        initial_task_results = pool.run_batch(run_account_policy_job, initial_tasks)
        initial_task_seconds = max(time.perf_counter() - initial_task_started, 1e-9)
        initial_resources = summarize_task_resources(
            initial_task_results,
            batch_wall_seconds=initial_task_seconds,
            worker_count_budget=worker_count,
        )
        initial_rows = _successful_values(initial_task_results, "initial account policy")
        initial_by_id = {str(row["policy_id"]): row for row in initial_rows}
        individual_rows = [initial_by_id[policy.policy_id] for policy in individual_policies]
        basket_rows = [initial_by_id[policy.policy_id] for policy in basket_policies]

        individual_fitness: dict[str, AccountFitness] = {}
        individual_summaries: dict[str, AccountPolicyRollingSummary] = {}
        for policy, row in zip(individual_policies, individual_rows, strict=True):
            summary = summary_from_dict(row["summary"])
            individual_summaries[policy.component_ids[0]] = summary
            descriptor = component_by_id[policy.component_ids[0]].descriptor
            individual_fitness[policy.policy_id] = individual_combine_fitness(
                summary,
                positive_net_after_costs=descriptor.cost_stress_net_pnl > 0.0,
                complexity=1.0,
            )
        basket_fitness: dict[str, AccountFitness] = {}
        basket_summaries: dict[str, AccountPolicyRollingSummary] = {}
        for policy, row in zip(basket_policies, basket_rows, strict=True):
            summary = summary_from_dict(row["summary"])
            basket_summaries[policy.policy_id] = summary
            baseline = max(
                (individual_summaries[item] for item in policy.component_ids),
                key=_summary_rank,
            )
            basket_fitness[policy.policy_id] = basket_combine_fitness(
                summary,
                best_component=baseline,
                positive_net_after_costs=summary.median_episode_net_pnl > 0.0,
                complexity=float(len(policy.component_ids)),
            )
        xfa_baskets = sorted(
            basket_policies,
            key=lambda policy: (
                -basket_fitness[policy.policy_id].score,
                -basket_summaries[policy.policy_id].pass_rate,
                basket_summaries[policy.policy_id].mll_breach_rate,
                policy.policy_id,
            ),
        )[:12]
        xfa_started = time.perf_counter()
        xfa_rows = [
            evaluate_serial_xfa_basket(
                {
                    component_id: component_by_id[component_id].events
                    for component_id in basket.component_ids
                },
                eligible_days,
                basket=basket,
                maximum_starts=12,
            )
            for basket in xfa_baskets
        ]
        xfa_seconds = max(time.perf_counter() - xfa_started, 1e-9)
        controller_baskets = sorted(
            basket_policies,
            key=lambda policy: (
                -basket_fitness[policy.policy_id].score,
                -basket_summaries[policy.policy_id].pass_rate,
                -basket_summaries[policy.policy_id].target_progress_median,
                basket_summaries[policy.policy_id].mll_breach_rate,
                policy.policy_id,
            ),
        )[:controller_basket_limit]
        controllers = [
            controller
            for basket in controller_baskets
            for controller in generate_controller_population(
                basket, generation_index=generation_index
            )
        ]
        basket_by_id = {policy.policy_id: policy for policy in basket_policies}
        controller_tasks = [
            _policy_task(
                basket_by_id[controller.basket_policy_id],
                component_bank_path=bank_path,
                start_days=screening_start_days,
                maximum_starts=screening_starts,
                controller=controller,
            )
            for controller in controllers
        ]
        controller_started = time.perf_counter()
        controller_task_results = pool.run_batch(
            run_account_policy_job, controller_tasks
        )
        controller_seconds = max(time.perf_counter() - controller_started, 1e-9)
        controller_resources = summarize_task_resources(
            controller_task_results,
            batch_wall_seconds=controller_seconds,
            worker_count_budget=worker_count,
        )
        controller_rows = _successful_values(
            controller_task_results, "controller policy"
        )
        controller_row_by_id = {
            str(row["policy_id"]): row for row in controller_rows
        }
        controller_fitness: dict[str, AccountFitness] = {}
        random_by_basket: dict[str, AccountPolicyRollingSummary] = {}
        random_row_by_basket: dict[str, dict[str, Any]] = {}
        for controller in controllers:
            if controller.random_control_seed is not None:
                random_row = controller_row_by_id[controller.controller_id]
                random_by_basket[controller.basket_policy_id] = summary_from_dict(
                    random_row["summary"]
                )
                random_row_by_basket[controller.basket_policy_id] = random_row
        for controller in controllers:
            if controller.random_control_seed is not None:
                continue
            summary = summary_from_dict(
                controller_row_by_id[controller.controller_id]["summary"]
            )
            static = basket_summaries[controller.basket_policy_id]
            random = random_by_basket[controller.basket_policy_id]
            paired = paired_controller_evidence(
                list(controller_row_by_id[controller.controller_id]["episode_metrics"]),
                list(initial_by_id[controller.basket_policy_id]["episode_metrics"]),
                list(
                    random_row_by_basket[controller.basket_policy_id][
                        "episode_metrics"
                    ]
                ),
            )
            controller_fitness[controller.controller_id] = adaptive_controller_fitness(
                summary,
                static_baseline=static,
                random_baseline=random,
                positive_net_after_costs=summary.median_episode_net_pnl > 0.0,
                complexity=float(
                    len(basket_by_id[controller.basket_policy_id].component_ids) + 3
                ),
                paired_evidence=paired,
            )

        promotion_objects = _promotion_objects(
            basket_policies=basket_policies,
            basket_fitness=basket_fitness,
            controllers=controllers,
            controller_fitness=controller_fitness,
            maximum=12,
        )
        promotion_tasks = [
            _policy_task(
                basket,
                component_bank_path=bank_path,
                start_days=promotion_start_days,
                maximum_starts=promotion_starts,
                minimum_spacing_sessions=2,
                controller=controller,
                task_suffix="promotion",
            )
            for basket, controller in promotion_objects
        ]
        promotion_started = time.perf_counter()
        promotion_task_results = pool.run_batch(
            run_account_policy_job, promotion_tasks
        ) if promotion_tasks else ()
        promotion_seconds = max(time.perf_counter() - promotion_started, 1e-9)
        promotion_resources = summarize_task_resources(
            promotion_task_results,
            batch_wall_seconds=promotion_seconds,
            worker_count_budget=worker_count,
        )
        promotion_rows = _successful_values(
            promotion_task_results, "promotion account policy"
        ) if promotion_task_results else []
    account_seconds = max(time.perf_counter() - account_started, 1e-9)

    individual_output = _attach_fitness(
        individual_rows, individual_fitness
    )
    basket_output = _attach_fitness(basket_rows, basket_fitness)
    controller_output = [
        {
            **row,
            "fitness": (
                controller_fitness[str(row["policy_id"])].to_dict()
                if str(row["policy_id"]) in controller_fitness
                else None
            ),
            "is_random_control": bool(
                (row.get("controller") or {}).get("random_control_seed")
                is not None
            ),
        }
        for row in controller_rows
    ]
    writer.write_jsonl_batch("account_v6_individual_results.jsonl", individual_output)
    writer.write_jsonl_batch("account_v6_basket_results.jsonl", basket_output)
    writer.write_jsonl_batch("account_v6_controller_results.jsonl", controller_output)
    writer.write_jsonl_batch("account_v6_promotion_results.jsonl", promotion_rows)
    writer.write_jsonl_batch("account_v6_xfa_results.jsonl", xfa_rows)

    archive = _build_archive(
        individual_output, basket_output, controller_output
    )
    individual_elites = [
        row for row in individual_output if (row.get("fitness") or {}).get("elite")
    ]
    screening_basket_elites = [
        row for row in basket_output if (row.get("fitness") or {}).get("elite")
    ]
    screening_controller_elites = [
        row
        for row in controller_output
        if not row["is_random_control"]
        and (row.get("fitness") or {}).get("elite")
    ]
    basket_elite_ids, controller_elite_ids = _promotion_elite_ids(
        promotion_rows,
        basket_fitness=basket_fitness,
        controller_fitness=controller_fitness,
    )
    historical_xfa_components = [
        component.descriptor.component_id
        for component in primary_components
        if component.descriptor.expected_xfa_cycles >= 1.0
        and component.descriptor.rolling_mll_breach_rate <= 0.25
    ]
    xfa_elites = [
        str(row["policy_id"])
        for row in xfa_rows
        if bool((row.get("xfa_fitness") or {}).get("elite"))
    ]
    improvement_counts = {
        "baskets_improving_pass_rate": sum(
            float((row["fitness"]["comparison"] or {}).get("pass_rate_delta") or 0.0)
            > 0.0
            for row in basket_output
        ),
        "baskets_improving_target_progress": sum(
            float((row["fitness"]["comparison"] or {}).get("target_progress_delta") or 0.0)
            >= 0.10
            for row in basket_output
        ),
        "controllers_improving_static_pass_rate": sum(
            float((row.get("fitness") or {}).get("comparison", {}).get("static_pass_rate_delta") or 0.0)
            > 0.0
            for row in controller_output
            if not row["is_random_control"]
        ),
        "controllers_improving_static_target_progress": sum(
            float((row.get("fitness") or {}).get("comparison", {}).get("static_target_progress_delta") or 0.0)
            >= 0.10
            for row in controller_output
            if not row["is_random_control"]
        ),
        "controllers_beating_random_router": sum(
            float((row.get("fitness") or {}).get("comparison", {}).get("random_router_advantage") or 0.0)
            > 0.05
            for row in controller_output
            if not row["is_random_control"]
        ),
    }

    resource_after = capture_resource_snapshot()
    total_seconds = max(time.perf_counter() - started, 1e-9)
    payload: dict[str, Any] = {
        "schema": VERSION,
        "generation_index": generation_index,
        "scientific_conclusion": (
            "ACCOUNT_LEVEL_ELITES_FOUND"
            if basket_elite_ids or controller_elite_ids
            else "ACCOUNT_LEVEL_GENERATION_COMPLETED_NEW_GRAMMAR_OR_POLICY_MUTATION_REQUIRED"
        ),
        "interpretation_boundary": (
            "Development-only rolling shared-account evidence. No protected holdout, "
            "PAPER_SHADOW_READY, broker connection or funded authorization."
        ),
        "v5_grammar_status": "V5_GRAMMAR_SATURATED_KEEP_ELITES_ONLY",
        "component_bank": {
            **component_audit,
            "new_grammar_components": len(new_components),
            "target_velocity_components": len(target_velocity_components),
            "total_components": len(all_components),
            "primary_components": len(primary_components),
            "manifest_hash": bank_manifest["manifest_hash"],
            "role_distribution": dict(
                sorted(
                    Counter(
                        component.descriptor.role.value
                        for component in primary_components
                    ).items()
                )
            ),
        },
        "new_mechanism_grammar": {
            "structures_generated": len(grammar_specs),
            "fast_survivors": len(grammar_survivors),
            "exact_replays": len(grammar_results),
            "accepted_components": len(new_components),
            "novelty_yield": len(new_components) / max(len(grammar_specs), 1),
            "domains": dict(
                sorted(Counter(spec.mechanism_kind for spec in grammar_specs).items())
            ),
            "historical_fingerprints_excluded": len(historical_grammar),
        },
        "target_velocity_mutations": {
            "proposals": len(target_velocity_proposals),
            "parent_child_replays": len(target_velocity_results),
            "improved_children": sum(
                row["decision"] == "KEEP_CHILD"
                for row in target_velocity_outcomes
            ),
            "accepted_components": len(target_velocity_components),
            "mutation_classes": dict(
                sorted(
                    Counter(
                        row.hypothesis.mutation_class
                        for row in target_velocity_proposals
                    ).items()
                )
            ),
            "historical_fingerprints_excluded": len(
                historical_target_velocity_fingerprints
            ),
            "outcome_counts": dict(
                sorted(
                    Counter(
                        row["decision"] for row in target_velocity_outcomes
                    ).items()
                )
            ),
            "admission": target_velocity_admission,
        },
        "populations": {
            "individuals_evaluated": len(individual_output),
            "baskets_generated": len(basket_policies),
            "historical_basket_fingerprints_excluded": len(
                historical_basket_fingerprints
            ),
            "baskets_evaluated": len(basket_output),
            "controllers_evaluated": sum(
                not row["is_random_control"] for row in controller_output
            ),
            "random_router_controls": sum(
                row["is_random_control"] for row in controller_output
            ),
            "promotion_objects_evaluated": len(promotion_rows),
            "screening_episode_starts": len(screening_start_days),
            "promotion_episode_starts": len(promotion_start_days),
            "total_rolling_combine_episodes": (
                sum(int(row["summary"]["episode_start_count"]) for row in individual_output)
                + sum(int(row["summary"]["episode_start_count"]) for row in basket_output)
                + sum(int(row["summary"]["episode_start_count"]) for row in controller_output)
                + sum(int(row["summary"]["episode_start_count"]) for row in promotion_rows)
                + sum(
                    int(
                        (row.get("rolling_combine") or {}).get(
                            "episode_start_count"
                        )
                        or 0
                    )
                    for row in target_velocity_results
                )
            ),
            "target_velocity_mutation_rolling_episodes": sum(
                int(
                    (row.get("rolling_combine") or {}).get(
                        "episode_start_count"
                    )
                    or 0
                )
                for row in target_velocity_results
            ),
        },
        "individuals": _population_metrics(individual_output),
        "baskets": _population_metrics(basket_output),
        "controllers": _population_metrics(
            [row for row in controller_output if not row["is_random_control"]]
        ),
        "promotion": {
            **_population_metrics(promotion_rows),
            "elite_count": len(basket_elite_ids) + len(controller_elite_ids),
            "basket_elite_count": len(basket_elite_ids),
            "controller_elite_count": len(controller_elite_ids),
            "elite_policy_ids": [*basket_elite_ids, *controller_elite_ids],
        },
        "individual_combine_elites": [row["policy_id"] for row in individual_elites],
        "account_basket_elites": basket_elite_ids,
        "account_controller_elites": controller_elite_ids,
        "screening_frontier": {
            "basket_elite_ids": [
                row["policy_id"] for row in screening_basket_elites
            ],
            "controller_elite_ids": [
                row["policy_id"] for row in screening_controller_elites
            ],
            "not_a_promotion_status": True,
        },
        "xfa_payout_elites": xfa_elites,
        "xfa": _xfa_metrics(xfa_rows),
        "historical_xfa_component_bank": {
            "candidate_count": len(historical_xfa_components),
            "candidate_ids": historical_xfa_components,
            "status_inherited": False,
            "role": "INPUT_COMPONENT_ONLY",
        },
        "policy_improvements": improvement_counts,
        "archive": archive.summary(),
        "persistent_queues": {
            "individual_evolution_queue": "COMPLETED_AND_REFILL_PENDING",
            "basket_evolution_queue": "COMPLETED_AND_REFILL_PENDING",
            "controller_evolution_queue": "COMPLETED_AND_REFILL_PENDING",
            "new_grammar_queue": "COMPLETED_AND_REFILL_PENDING",
            "xfa_queue": "COMPONENT_EVIDENCE_PRESERVED",
            "shadow_queue": "WAITING_FOR_FRESH_FORWARD_DATA",
        },
        "shadow": {
            "new_config_complete": 0,
            "new_genuinely_active": 0,
            "fresh_bars": 0,
            "signals": 0,
            "virtual_fills": 0,
            "packaging_required_for_elites": [
                *basket_elite_ids,
                *controller_elite_ids,
            ],
        },
        "governance": {
            "development_end_exclusive": "2024-10-01",
            "q4_access_count_delta": 0,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "outbound_order_capability": False,
            "single_writer": True,
        },
        "paper_shadow_ready": 0,
        "next_recommended_action": (
            "PROMOTE_ACCOUNT_ELITES_AND_CONTINUE_V6"
            if basket_elite_ids or controller_elite_ids
            else "EXPAND_NEW_GRAMMAR_AND_MUTATE_TARGET_PROGRESS_FRONTIER"
        ),
        "performance": {
            "total_seconds": total_seconds,
            "grammar_exact_seconds": grammar_exact_seconds,
            "account_policy_seconds": account_seconds,
            "initial_policy_resources": asdict(initial_resources),
            "controller_resources": asdict(controller_resources),
            "promotion_resources": asdict(promotion_resources),
            "grammar_resources": asdict(grammar_resources),
            "target_velocity_mutation_resources": asdict(
                target_velocity_resources
            ),
            "xfa_policy_seconds": xfa_seconds,
            "resource_before": asdict(resource_before),
            "resource_after": asdict(resource_after),
            "worker_count": worker_count,
        },
        "feature_store": {
            "cache_hits": feature_build.cache_hits,
            "cache_misses": feature_build.cache_misses,
            "rows": feature_build.rows,
            "source_fingerprint": feature_build.source_fingerprint,
            "markets": sorted(feature_build.market_paths),
            "timeframes": ["1m", "5m", "15m", "30m", "60m", "session", "daily"],
        },
        "code_commit": code_commit,
    }
    payload["scientific_result_hash"] = stable_hash(
        _scientific_payload(payload)
    )
    payload["result_hash"] = payload["scientific_result_hash"]
    result_receipt = writer.write_json("account_v6_result.json", payload)
    report_receipt = writer.write_text(
        "account_v6_report.md", _render_report(payload)
    )
    return {
        **payload,
        "artifacts": {
            "result_path": str(destination / result_receipt.relative_path),
            "result_sha256": result_receipt.sha256,
            "report_path": str(destination / report_receipt.relative_path),
            "report_sha256": report_receipt.sha256,
            "component_bank_path": str(bank_path),
            "component_bank_sha256": _sha256(bank_path),
            "individual_results_path": str(destination / "account_v6_individual_results.jsonl"),
            "basket_results_path": str(destination / "account_v6_basket_results.jsonl"),
            "controller_results_path": str(destination / "account_v6_controller_results.jsonl"),
            "promotion_results_path": str(destination / "account_v6_promotion_results.jsonl"),
            "grammar_results_path": str(destination / "account_v6_grammar_exact_results.jsonl"),
            "target_velocity_mutations_path": str(
                destination / "account_v6_target_velocity_mutations.json"
            ),
            "xfa_results_path": str(destination / "account_v6_xfa_results.jsonl"),
        },
        "report_path": str(destination / report_receipt.relative_path),
    }


def _evaluate_grammar_specs(
    specs: Sequence[MechanismGraphSpec],
    *,
    matrix_paths: Mapping[str, str],
    worker_count: int,
    maximum_episode_starts: int,
) -> tuple[list[dict[str, Any]], Any]:
    tasks = [
        PureTask(
            task_id=spec.candidate_id,
            payload={
                "matrix_path": matrix_paths[spec.market],
                "specification": spec.to_dict(),
                "maximum_episode_starts": maximum_episode_starts,
            },
        )
        for spec in specs
    ]
    started = time.perf_counter()
    with LongLivedWorkerPool(max_workers=worker_count) as pool:
        results = pool.run_batch(run_mechanism_exact_job, tasks)
    seconds = max(time.perf_counter() - started, 1e-9)
    resources = summarize_task_resources(
        results,
        batch_wall_seconds=seconds,
        worker_count_budget=worker_count,
    )
    return _successful_values(results, "mechanism grammar"), resources


def _evaluate_target_velocity_proposals(
    proposals: Sequence[TargetVelocityProposal],
    *,
    components: Sequence[ComponentRuntime],
    matrix_paths: Mapping[str, str],
    worker_count: int,
    maximum_episode_starts: int,
) -> tuple[list[dict[str, Any]], Any]:
    specs: dict[str, Any] = {}
    parent_by_id = {
        component.descriptor.component_id: spec_from_dict(
            component.specification
        )
        for component in components
    }
    for proposal in proposals:
        parent = parent_by_id.get(proposal.hypothesis.parent_candidate_id)
        if parent is None:
            raise AccountLevelV6Error(
                "target-velocity proposal lost its immutable parent"
            )
        specs[parent.candidate_id] = parent
        specs[proposal.child.candidate_id] = proposal.child
    tasks = [
        PureTask(
            task_id=candidate_id,
            payload={
                "matrix_path": matrix_paths[spec.market],
                "specification": spec_to_dict(spec),
                "maximum_episode_starts": maximum_episode_starts,
                "maximum_xfa_starts": min(12, maximum_episode_starts),
            },
        )
        for candidate_id, spec in sorted(specs.items())
    ]
    started = time.perf_counter()
    if tasks:
        with LongLivedWorkerPool(max_workers=worker_count) as pool:
            results = pool.run_batch(run_rolling_combine_job, tasks)
    else:
        results = ()
    seconds = max(time.perf_counter() - started, 1e-9)
    resources = summarize_task_resources(
        results,
        batch_wall_seconds=seconds,
        worker_count_budget=worker_count,
    )
    return _successful_values(results, "target-velocity mutation"), resources


def _target_velocity_outcomes(
    proposals: Sequence[TargetVelocityProposal],
    results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_id = {str(row["candidate_id"]): row for row in results}
    output: list[dict[str, Any]] = []
    for proposal in proposals:
        parent = by_id.get(proposal.hypothesis.parent_candidate_id)
        child = by_id.get(proposal.child.candidate_id)
        if parent is None or child is None:
            raise AccountLevelV6Error(
                "target-velocity parent/child comparison is incomplete"
            )
        output.append(
            evaluate_target_velocity_outcome(
                proposal,
                parent_result=parent,
                child_result=child,
            )
        )
    return output


def _target_velocity_components(
    proposals: Sequence[TargetVelocityProposal],
    results: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    *,
    matrices: Mapping[str, FeatureMatrix],
    existing_components: Sequence[ComponentRuntime],
    source_experiment: str,
    maximum: int,
) -> tuple[list[ComponentRuntime], dict[str, Any]]:
    result_by_id = {str(row["candidate_id"]): row for row in results}
    outcome_by_id = {str(row["child_candidate_id"]): row for row in outcomes}
    existing_decisions: dict[str, list[set[int]]] = defaultdict(list)
    for component in existing_components:
        existing_decisions[component.descriptor.market].append(
            {trade.event.decision_ns for trade in component.events}
        )
    accepted: list[ComponentRuntime] = []
    decisions: list[dict[str, Any]] = []
    for proposal in proposals:
        candidate_id = proposal.child.candidate_id
        outcome = outcome_by_id[candidate_id]
        if outcome["decision"] != "KEEP_CHILD":
            decisions.append(
                {
                    "candidate_id": candidate_id,
                    "decision": "FREEZE_CHILD_DIAGNOSTIC",
                    "reason": "OBJECTIVE_OR_RISK_NOT_IMPROVED",
                }
            )
            continue
        if len(accepted) >= maximum:
            decisions.append(
                {
                    "candidate_id": candidate_id,
                    "decision": "FREEZE_CHILD_DIAGNOSTIC",
                    "reason": "MUTATION_COMPONENT_CAP",
                }
            )
            continue
        spec = proposal.child
        path = build_exact_trade_path(spec, matrices[spec.market])
        decision_set = {event.decision_ns for event in path.events}
        maximum_overlap = max(
            (
                _jaccard(decision_set, prior)
                for prior in existing_decisions[spec.market]
            ),
            default=0.0,
        )
        if maximum_overlap >= 0.80:
            decisions.append(
                {
                    "candidate_id": candidate_id,
                    "decision": "FREEZE_CHILD_DIAGNOSTIC",
                    "reason": "BEHAVIORAL_DUPLICATE",
                    "maximum_event_overlap": maximum_overlap,
                }
            )
            continue
        row = dict(result_by_id[candidate_id])
        rolling = dict(row["rolling_combine"])
        descriptor = ComponentDescriptor(
            component_id=candidate_id,
            specification_hash=stable_hash(spec_to_dict(spec)),
            market=spec.market,
            execution_market=MARKET_PAIRS[spec.market],
            family=f"target_velocity::{spec.family}",
            timeframe=spec.timeframe,
            role=ComponentRole.TARGET_VELOCITY_COMPONENT,
            behavioral_cluster=(
                "cluster_v6_velocity_" + _event_signature_from_path(path)[:20]
            ),
            source_experiment=source_experiment,
            source_result_hash=stable_hash(
                {
                    "candidate_id": candidate_id,
                    "exact_trade_path": row.get("exact_trade_path"),
                    "rolling_combine": rolling,
                    "mutation_hypothesis": proposal.hypothesis.to_dict(),
                }
            ),
            net_pnl_after_costs=path.net_pnl,
            cost_stress_net_pnl=path.cost_stress_1_5x_net,
            event_count=path.event_count,
            rolling_pass_rate=float(rolling.get("pass_rate") or 0.0),
            rolling_mll_breach_rate=float(
                rolling.get("mll_breach_rate") or 0.0
            ),
            median_target_progress=float(
                rolling.get("median_target_progress_when_not_passed") or 0.0
            ),
            expected_xfa_cycles=float(
                (row.get("rolling_xfa") or {}).get(
                    "expected_payout_cycles_before_ruin"
                )
                or 0.0
            ),
            inherited_status=False,
        )
        accepted.append(
            ComponentRuntime(
                descriptor=descriptor,
                specification=spec_to_dict(spec),
                events=tuple(
                    RoutedTrade(
                        component_id=candidate_id,
                        market=spec.market,
                        side=spec.side,
                        event=event,
                    )
                    for event in path.events
                ),
                eligible_session_days=path.eligible_session_days,
                source_kind="V6_TARGET_VELOCITY_MUTATION",
            )
        )
        existing_decisions[spec.market].append(decision_set)
        decisions.append(
            {
                "candidate_id": candidate_id,
                "decision": "ACCEPT_TARGET_VELOCITY_COMPONENT",
                "reason": "OBJECTIVE_IMPROVED_WITH_ACCEPTABLE_MLL_AND_NOVEL_BEHAVIOR",
                "maximum_event_overlap": maximum_overlap,
            }
        )
    return accepted, {
        "accepted_component_ids": [
            row.descriptor.component_id for row in accepted
        ],
        "decisions": decisions,
        "status_inherited": False,
    }


def _policy_task(
    basket: BasketPolicy,
    *,
    component_bank_path: Path,
    start_days: Sequence[int],
    maximum_starts: int,
    minimum_spacing_sessions: int = 5,
    controller: ControllerPolicy | None = None,
    task_suffix: str = "screen",
) -> PureTask[dict[str, Any]]:
    policy_id = controller.controller_id if controller else basket.policy_id
    return PureTask(
        task_id=f"{policy_id}:{task_suffix}",
        payload={
            "component_bank_path": str(component_bank_path),
            "basket": basket.to_dict(),
            "controller": controller.to_dict() if controller else None,
            "episode_start_days": list(start_days),
            "maximum_starts": maximum_starts,
            "minimum_spacing_sessions": minimum_spacing_sessions,
            "minimum_observation_sessions": 30,
            "maximum_duration_sessions": 60,
        },
    )


def _new_grammar_components(
    rows: Sequence[dict[str, Any]],
    *,
    spec_by_id: Mapping[str, MechanismGraphSpec],
    matrices: Mapping[str, FeatureMatrix],
    existing_components: Sequence[ComponentRuntime],
    source_experiment: str,
    maximum: int,
) -> list[ComponentRuntime]:
    ranked = sorted(
        (
            row
            for row in rows
            if float((row.get("exact_trade_path") or {}).get("net_pnl") or 0.0) > 0.0
            and float((row.get("exact_trade_path") or {}).get("cost_stress_1_5x_net") or 0.0) > 0.0
            and float((row.get("rolling_combine") or {}).get("mll_breach_rate") or 0.0) <= 0.35
        ),
        key=lambda row: (
            -float((row.get("rolling_combine") or {}).get("pass_rate") or 0.0),
            -float((row.get("rolling_combine") or {}).get("median_target_progress_when_not_passed") or 0.0),
            float((row.get("rolling_combine") or {}).get("mll_breach_rate") or 0.0),
            -float((row.get("exact_trade_path") or {}).get("cost_stress_1_5x_net") or 0.0),
            str(row["candidate_id"]),
        ),
    )
    existing_decisions: dict[str, list[set[int]]] = defaultdict(list)
    for component in existing_components:
        existing_decisions[component.descriptor.market].append(
            {trade.event.decision_ns for trade in component.events}
        )
    output: list[ComponentRuntime] = []
    for row in ranked:
        if len(output) >= maximum:
            break
        spec = spec_by_id[str(row["candidate_id"])]
        path = build_mechanism_trade_path(spec, matrices[spec.market])
        decisions = {event.decision_ns for event in path.events}
        if any(_jaccard(decisions, prior) >= 0.80 for prior in existing_decisions[spec.market]):
            continue
        rolling = dict(row["rolling_combine"])
        descriptor = ComponentDescriptor(
            component_id=spec.candidate_id,
            specification_hash=stable_hash(spec.to_dict()),
            market=spec.market,
            execution_market=MARKET_PAIRS[spec.market],
            family=spec.mechanism_kind,
            timeframe=spec.timeframe_profile,
            role=(
                ComponentRole.TARGET_VELOCITY_COMPONENT
                if float(rolling.get("pass_rate") or 0.0) > 0.0
                else ComponentRole.DIVERSIFIER_COMPONENT
                if spec.role.name == "COMBINE_PASSER"
                else ComponentRole.DEFENSIVE_COMPONENT
            ),
            behavioral_cluster="cluster_v6_" + _event_signature_from_path(path)[:16],
            source_experiment=source_experiment,
            source_result_hash=stable_hash(row),
            net_pnl_after_costs=path.net_pnl,
            cost_stress_net_pnl=path.cost_stress_1_5x_net,
            event_count=path.event_count,
            rolling_pass_rate=float(rolling.get("pass_rate") or 0.0),
            rolling_mll_breach_rate=float(rolling.get("mll_breach_rate") or 0.0),
            median_target_progress=float(
                rolling.get("median_target_progress_when_not_passed") or 0.0
            ),
            expected_xfa_cycles=0.0,
        )
        output.append(
            ComponentRuntime(
                descriptor=descriptor,
                specification=spec.to_dict(),
                events=tuple(
                    RoutedTrade(
                        component_id=spec.candidate_id,
                        market=spec.market,
                        side=spec.side,
                        event=event,
                    )
                    for event in path.events
                ),
                eligible_session_days=path.eligible_session_days,
                source_kind="V6_NEW_MECHANISM_GRAMMAR",
            )
        )
        existing_decisions[spec.market].append(decisions)
    return output


def _select_diverse_grammar_survivors(
    rows: Sequence[dict[str, Any]],
    *,
    spec_by_id: Mapping[str, MechanismGraphSpec],
    limit: int,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        spec = spec_by_id[str(row["candidate_id"])]
        groups[(spec.market, spec.mechanism_kind)].append(row)
    for values in groups.values():
        values.sort(
            key=lambda row: (
                -float(row["net_pnl"]),
                float(row["maximum_drawdown"]),
                str(row["candidate_id"]),
            )
        )
    selected: list[dict[str, Any]] = []
    depth = 0
    while len(selected) < limit:
        inserted = False
        for key in sorted(groups):
            if depth < len(groups[key]):
                selected.append(groups[key][depth])
                inserted = True
                if len(selected) >= limit:
                    break
        if not inserted:
            break
        depth += 1
    return selected


def _promotion_objects(
    *,
    basket_policies: Sequence[BasketPolicy],
    basket_fitness: Mapping[str, AccountFitness],
    controllers: Sequence[ControllerPolicy],
    controller_fitness: Mapping[str, AccountFitness],
    maximum: int,
) -> list[tuple[BasketPolicy, ControllerPolicy | None]]:
    basket_by_id = {policy.policy_id: policy for policy in basket_policies}
    candidates: list[tuple[float, str, BasketPolicy, ControllerPolicy | None]] = []
    for policy in basket_policies:
        fitness = basket_fitness[policy.policy_id]
        candidates.append((fitness.score, policy.policy_id, policy, None))
    for controller in controllers:
        if controller.random_control_seed is not None:
            continue
        fitness = controller_fitness[controller.controller_id]
        candidates.append(
            (
                fitness.score,
                controller.controller_id,
                basket_by_id[controller.basket_policy_id],
                controller,
            )
        )
    candidates.sort(key=lambda row: (-row[0], row[1]))
    return [(basket, controller) for _, _, basket, controller in candidates[:maximum]]


def _promotion_elite_ids(
    rows: Sequence[Mapping[str, Any]],
    *,
    basket_fitness: Mapping[str, AccountFitness],
    controller_fitness: Mapping[str, AccountFitness],
) -> tuple[list[str], list[str]]:
    baskets: list[str] = []
    controllers: list[str] = []
    for row in rows:
        summary = dict(row["summary"])
        policy_id = str(row["policy_id"])
        controller = dict(row.get("controller") or {})
        screening_fitness = (
            controller_fitness.get(policy_id)
            if controller
            else basket_fitness.get(policy_id)
        )
        if screening_fitness is None or not screening_fitness.elite:
            continue
        promotion_pass = bool(
            int(summary.get("effective_block_count") or 0) >= 4
            and float(summary.get("pass_rate") or 0.0) > 0.0
            and float(summary.get("mll_breach_rate") or 0.0) <= 0.25
            and float(summary.get("target_progress_median") or 0.0) > 0.0
            and float(summary.get("consistency_pass_rate") or 0.0) >= 0.50
            and float(summary.get("median_episode_net_pnl") or 0.0) > 0.0
            and int(summary.get("compliance_failure_count") or 0) == 0
        )
        if not promotion_pass:
            continue
        (controllers if controller else baskets).append(policy_id)
    return sorted(baskets), sorted(controllers)


def _attach_fitness(
    rows: Sequence[dict[str, Any]], fitness: Mapping[str, AccountFitness]
) -> list[dict[str, Any]]:
    return [
        {**row, "fitness": fitness[str(row["policy_id"])].to_dict()}
        for row in rows
    ]


def _build_archive(
    individuals: Sequence[dict[str, Any]],
    baskets: Sequence[dict[str, Any]],
    controllers: Sequence[dict[str, Any]],
) -> AccountPolicyArchive:
    archive = AccountPolicyArchive(maximum_per_niche=2)
    for row in [*individuals, *baskets, *controllers]:
        fitness = dict(row.get("fitness") or {})
        if not fitness:
            continue
        summary = dict(row["summary"])
        basket = dict(row["basket"])
        components = tuple(str(item) for item in basket["component_ids"])
        controller = dict(row.get("controller") or {})
        kind = str(summary["policy_kind"])
        niche = (
            kind,
            str(len(components)),
            str(basket.get("archetype")),
            "LOW_MLL" if float(summary["mll_breach_rate"]) <= 0.10 else "HIGHER_MLL",
            "PASSING" if float(summary["pass_rate"]) > 0.0 else "PROGRESS_ONLY",
            "CONTROLLED" if controller else "STATIC",
        )
        archive.insert(
            PolicyArchiveEntry(
                policy_id=str(row["policy_id"]),
                policy_kind=kind,
                niche=niche,
                score=float(fitness["score"]),
                pass_rate=float(summary["pass_rate"]),
                mll_breach_rate=float(summary["mll_breach_rate"]),
                target_progress=float(summary["target_progress_median"]),
                component_ids=components,
                payload=row,
            )
        )
    return archive


def _population_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "count": 0,
            "pass_rate_distribution": _distribution([]),
            "mll_breach_rate_distribution": _distribution([]),
            "target_progress_distribution": _distribution([]),
            "days_to_target_distribution": _distribution([]),
            "consistency_distribution": _distribution([]),
        }
    summaries = [dict(row["summary"]) for row in rows]
    return {
        "count": len(rows),
        "pass_rate_distribution": _distribution(
            [float(row["pass_rate"]) for row in summaries]
        ),
        "mll_breach_rate_distribution": _distribution(
            [float(row["mll_breach_rate"]) for row in summaries]
        ),
        "target_progress_distribution": _distribution(
            [float(row["target_progress_median"]) for row in summaries]
        ),
        "maximum_target_progress_distribution": _distribution(
            [float(row["maximum_target_progress"]) for row in summaries]
        ),
        "days_to_target_distribution": _distribution(
            [
                float(row["median_days_to_target"])
                for row in summaries
                if row.get("median_days_to_target") is not None
            ]
        ),
        "consistency_distribution": _distribution(
            [float(row["consistency_pass_rate"]) for row in summaries]
        ),
        "median_episode_net_distribution": _distribution(
            [float(row["median_episode_net_pnl"]) for row in summaries]
        ),
        "elite_count": sum(bool((row.get("fitness") or {}).get("elite")) for row in rows),
    }


def _xfa_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    summaries = [dict(row["rolling_xfa"]) for row in rows]
    return {
        "count": len(rows),
        "expected_payout_cycles_distribution": _distribution(
            [
                float(row["expected_payout_cycles_before_ruin"])
                for row in summaries
            ]
        ),
        "payout_probability_distribution": _distribution(
            [float(row["payout_probability"]) for row in summaries]
        ),
        "survival_rate_distribution": _distribution(
            [float(row["survival_rate"]) for row in summaries]
        ),
        "post_payout_survival_distribution": _distribution(
            [float(row["post_payout_survival_rate"]) for row in summaries]
        ),
        "elite_count": sum(
            bool((row.get("xfa_fitness") or {}).get("elite")) for row in rows
        ),
        "routing_policy": "GLOBAL_SERIAL_FIXED_PRIORITY",
        "status_inherited": False,
    }


def _successful_values(results: Sequence[Any], label: str) -> list[dict[str, Any]]:
    failures = [row for row in results if not row.succeeded]
    if failures:
        raise AccountLevelV6Error(
            f"{label} worker failures: "
            + repr(
                [
                    (row.task_id, row.error_type, row.error_message)
                    for row in failures[:5]
                ]
            )
        )
    output = [dict(row.value or {}) for row in results]
    output.sort(key=lambda row: str(row.get("policy_id") or row.get("candidate_id")))
    return output


def _summary_rank(summary: AccountPolicyRollingSummary) -> tuple[float, ...]:
    return (
        summary.pass_rate,
        -summary.mll_breach_rate,
        summary.target_progress_median,
        summary.consistency_pass_rate,
        summary.median_episode_net_pnl,
    )


def _historical_grammar_fingerprints(root: str | Path) -> set[str]:
    output: set[str] = set()
    for path in Path(root).glob(
        "account_level_evolution_v6_generation_*/account_v6_grammar_fast_screen.jsonl"
    ):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                value = json.loads(line).get("structural_fingerprint")
                if value:
                    output.add(str(value))
    return output


def _historical_basket_fingerprints(root: str | Path) -> set[str]:
    output: set[str] = set()
    for path in Path(root).glob(
        "account_level_evolution_v6_generation_*/account_v6_basket_population.jsonl"
    ):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            output.add(BasketPolicy.from_dict(json.loads(line)).structural_fingerprint)
    return output


def _historical_target_velocity_fingerprints(root: str | Path) -> set[str]:
    output: set[str] = set()
    for path in Path(root).glob(
        "account_level_evolution_v6_generation_*/account_v6_target_velocity_mutations.json"
    ):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for proposal in payload.get("proposals", ()):
            output.add(
                structural_fingerprint(
                    spec_from_dict(dict(proposal["child"]))
                )
            )
    return output


def _event_signature(events: Sequence[RoutedTrade]) -> str:
    return hashlib.sha256(
        b"".join(
            int(trade.event.decision_ns).to_bytes(8, "big", signed=True)
            for trade in events[:256]
        )
    ).hexdigest()


def _event_signature_from_path(path: Any) -> str:
    return hashlib.sha256(
        b"".join(
            int(event.decision_ns).to_bytes(8, "big", signed=True)
            for event in path.events[:256]
        )
    ).hexdigest()


def _jaccard(left: set[int], right: set[int]) -> float:
    union = len(left | right)
    return len(left & right) / union if union else 1.0


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "p25": None, "median": None, "p75": None, "max": None}
    array = np.asarray(values, dtype=float)
    return {
        "count": len(array),
        "min": float(np.min(array)),
        "p25": float(np.percentile(array, 25)),
        "median": float(np.median(array)),
        "p75": float(np.percentile(array, 75)),
        "max": float(np.max(array)),
    }


def _scientific_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(payload)
    row.pop("performance", None)
    row.pop("result_hash", None)
    row.pop("scientific_result_hash", None)
    feature = dict(row.get("feature_store") or {})
    feature.pop("cache_hits", None)
    feature.pop("cache_misses", None)
    row["feature_store"] = feature
    return row


def _record_access_once(generation_index: int) -> dict[str, Any]:
    reason = (
        f"Account-Level Evolution V6 generation {generation_index}: "
        "development-only shared-account and mechanism-grammar replay; Q4 excluded"
    )
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.is_file():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("requesting_module") == __name__ and row.get("reason_for_access") == reason:
                return row
    record = enforce_data_access(
        "2023-01-01:2024-10-01",
        DataRole.DEVELOPMENT,
        __name__,
        [f"account_level_v6_generation_{generation_index}_frozen_populations"],
        reason,
        None,
    )
    return record.__dict__


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _sha256(path) != expected:
        raise AccountLevelV6Error(f"frozen {label} is missing or changed: {path}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _render_report(payload: Mapping[str, Any]) -> str:
    populations = dict(payload["populations"])
    return "\n".join(
        [
            "# HYDRA Account-Level Evolution & Mechanism Grammar V6",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- V5 grammar: `{payload['v5_grammar_status']}`",
            f"- Components / behavioral clusters: `{payload['component_bank']['total_components']}` / `{payload['component_bank']['behavioral_clusters']}`",
            f"- New grammar structures / accepted components: `{payload['new_mechanism_grammar']['structures_generated']}` / `{payload['new_mechanism_grammar']['accepted_components']}`",
            f"- Target-velocity mutations / improved / admitted: `{payload['target_velocity_mutations']['proposals']}` / `{payload['target_velocity_mutations']['improved_children']}` / `{payload['target_velocity_mutations']['accepted_components']}`",
            f"- Individuals / baskets / controllers: `{populations['individuals_evaluated']}` / `{populations['baskets_evaluated']}` / `{populations['controllers_evaluated']}`",
            f"- Rolling Combine episodes: `{populations['total_rolling_combine_episodes']}`",
            f"- Screening basket/controller frontier: `{len(payload['screening_frontier']['basket_elite_ids'])}` / `{len(payload['screening_frontier']['controller_elite_ids'])}`",
            f"- Promoted individual / basket / controller elites: `{len(payload['individual_combine_elites'])}` / `{len(payload['account_basket_elites'])}` / `{len(payload['account_controller_elites'])}`",
            f"- XFA serial account-policy evaluations / elites: `{payload['xfa']['count']}` / `{len(payload['xfa_payout_elites'])}`",
            f"- Q4 access delta: `{payload['governance']['q4_access_count_delta']}`",
            f"- Databento spend delta: `{payload['governance']['incremental_databento_spend_usd']}`",
            f"- Outbound orders: `{int(payload['governance']['outbound_order_capability'])}`",
            f"- PAPER_SHADOW_READY: `{payload['paper_shadow_ready']}`",
            "",
            "Development evidence only; account-policy elites require independent evidence before paper-shadow promotion.",
        ]
    )


__all__ = ["AccountLevelV6Error", "run_account_level_evolution_v6"]
