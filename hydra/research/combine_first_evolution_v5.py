from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import time
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.compute.resource_monitor import (
    capture_resource_snapshot,
    summarize_task_resources,
)
from hydra.compute.result_writer import AtomicResultWriter
from hydra.compute.worker_pool import LongLivedWorkerPool, PureTask
from hydra.factory.failure_guided_evolution import (
    MutationProposal,
    configuration_fingerprint,
    constrained_crossover,
    mutation_outcome,
    propose_failure_guided_mutation,
)
from hydra.factory.quality_diversity import (
    ArchiveCandidate,
    ArchiveNiche,
    QualityDiversityArchive,
)
from hydra.features.feature_matrix import FeatureMatrix
from hydra.mission.calibration_retest_execution import _stable_hash
from hydra.research.propfirm_meta_screen import fit_registry_propfirm_meta_screen
from hydra.research.qd_economic_tournament import MARKET_PAIRS
from hydra.research.rolling_combine_replay import run_rolling_combine_job
from hydra.research.turbo_exact_replay import spec_from_dict, spec_to_dict
from hydra.research.turbo_feature_builder import (
    build_or_open_turbo_feature_bundles,
)
from hydra.research.turbo_foundry_v2 import (
    TurboFoundryError,
    _ecology,
    _historical_turbo_fingerprints,
    _population_coverage,
    _stage1_event_matrix,
    generate_turbo_population,
)
from hydra.strategies.turbo_batch_fingerprint import (
    deduplicate_specs,
    structural_fingerprint,
)
from hydra.strategies.turbo_compiler import compile_strategy_batch
from hydra.strategies.turbo_dsl import StrategySpec
from hydra.strategies.turbo_vectorized_executor import execute_stage1_vectorized
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "hydra_combine_first_evolution_v5_epoch_v1"
DEFAULT_PROPOSAL_COUNT = 10_000
DEFAULT_EXACT_LIMIT = 200
DEFAULT_MUTATION_LIMIT = 60
MINIMUM_EXPLORATION_SHARE = 0.20


class CombineFirstV5Error(RuntimeError):
    pass


def run_combine_first_evolution_v5(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    contract_map_path: str | Path,
    contract_map_sha256: str,
    code_commit: str,
    epoch_index: int = 0,
    worker_count: int = 3,
    proposal_count: int = DEFAULT_PROPOSAL_COUNT,
    exact_limit: int = DEFAULT_EXACT_LIMIT,
    mutation_limit: int = DEFAULT_MUTATION_LIMIT,
    maximum_episode_starts: int = 24,
    record_data_access: bool = True,
    random_seed: int = 20260712,
) -> dict[str, Any]:
    started = time.perf_counter()
    if worker_count < 1 or worker_count > 3:
        raise CombineFirstV5Error("V5 supports one to three pure compute workers")
    if proposal_count < 1_000 or exact_limit < 1 or mutation_limit < 0:
        raise CombineFirstV5Error("invalid V5 batch capacity")
    task_path = Path(engineering_task_path)
    roll_map = Path(contract_map_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(roll_map, contract_map_sha256, "explicit-contract map")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise CombineFirstV5Error("worker commit differs from frozen specification")
    destination = Path(output_dir)
    writer = AtomicResultWriter(destination)
    preregistration = {
        "schema": "hydra_combine_first_v5_preregistration_v1",
        "epoch_index": epoch_index,
        "proposal_count": proposal_count,
        "adaptive_capacity_policy": {
            "enabled": True,
            "preferred_minimum_structures": 1_000,
            "rule": "downsize_only_to_reported_pre_backtest_structural_capacity",
            "discovery_exhaustion_does_not_block_targeted_mutation": True,
            "future_economics_used": False,
        },
        "exact_rolling_limit": exact_limit,
        "mutation_limit": mutation_limit,
        "episode_start_policy": {
            "maximum_starts": maximum_episode_starts,
            "minimum_spacing_sessions": 5,
            "minimum_observation_sessions": 30,
            "maximum_duration_sessions": 60,
            "regime_balanced": True,
        },
        "compute_allocation": {
            "mass_generation_and_fast_combine": 0.45,
            "failure_guided_mutation": 0.30,
            "exact_rolling_validation": 0.15,
            "xfa_portfolio_shadow": 0.10,
        },
        "hard_gates": [
            "finite",
            "positive_net_after_approximate_realistic_costs",
            "valid_explicit_contract_feature_bundle",
            "non_duplicate_structure",
            "non_catastrophic_approximate_mll",
            "valid_session_and_sizing",
        ],
        "deferred_from_early_stage": [
            "candidate_level_null_suite",
            "block_bootstrap_and_monte_carlo",
            "unrelated_cross_market_replication",
            "diagnostic_parameter_neighborhood",
            "long_form_candidate_report",
            "protected_holdout",
        ],
        "minimum_exploration_share": MINIMUM_EXPLORATION_SHARE,
        "maximum_family_share": 0.25,
        "maximum_ecology_share": 0.40,
        "maximum_lineage_share": 0.02,
        "development_period": ["2023-01-01", "2024-10-01"],
        "engineering_task_sha256": engineering_task_sha256,
        "contract_map_sha256": contract_map_sha256,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "q4_access_allowed": False,
        "q4_reuse_prohibited": True,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "outbound_order_capability": False,
        "paper_shadow_ready_allowed": False,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    writer.write_json("combine_v5_preregistration.json", preregistration)
    if record_data_access:
        _record_access_once(epoch_index)
    resource_before = capture_resource_snapshot()

    cache_root = _canonical_cache_root()
    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=cache_root, contract_map_path=roll_map
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in feature_build.market_paths.items()
    }
    features_ready_at = time.perf_counter()
    historical_v5_structures, historical_v5_configurations = (
        _historical_v5_fingerprints()
    )
    historical_turbo_structures = _historical_turbo_fingerprints()
    population, capacity = _generate_adaptive_population(
        matrices,
        requested_count=proposal_count,
        batch_index=10_000 + epoch_index,
        random_seed=random_seed,
        excluded_fingerprints=(
            historical_turbo_structures | historical_v5_structures
        ),
    )
    capacity = {
        **capacity,
        "historical_turbo_structural_tombstones": len(
            historical_turbo_structures
        ),
        "historical_v5_structural_tombstones": len(
            historical_v5_structures
        ),
        "historical_v5_configuration_tombstones": len(
            historical_v5_configurations
        ),
    }
    deduplicated = deduplicate_specs(population)
    if len(deduplicated.specs) != len(population):
        raise CombineFirstV5Error("V5 population lost structures after deduplication")
    population = list(deduplicated.specs)
    spec_by_id = {spec.candidate_id: spec for spec in population}
    writer.write_json(
        "combine_v5_population_manifest.json",
        {
            "schema": "hydra_combine_first_v5_population_v1",
            "epoch_index": epoch_index,
            "population_hash": _stable_hash(
                [
                    (spec.candidate_id, structural_fingerprint(spec))
                    for spec in population
                ]
            ),
            "structural_proposals": len(population),
            "quality_diversity": _population_coverage(population),
            "specifications": [spec_to_dict(spec) for spec in population],
        },
    )
    generation_ready_at = time.perf_counter()

    stage1_rows: list[dict[str, Any]] = []
    for market in MARKET_PAIRS:
        market_specs = [spec for spec in population if spec.market == market]
        if not market_specs:
            continue
        event_matrix = _stage1_event_matrix(matrices[market])
        compiled = compile_strategy_batch(
            market_specs, event_matrix.feature_names, event_matrix.holding_horizons
        )
        result = execute_stage1_vectorized(compiled, event_matrix, micro_batch_size=64)
        for index, spec in enumerate(market_specs):
            events = int(result.opportunity_count[index])
            gross = float(result.gross_pnl[index])
            net = float(result.net_pnl[index])
            drawdown = float(result.approximate_max_drawdown[index])
            concentration = float(result.best_positive_event_share[index])
            stress = gross - 1.5 * spec.round_turn_cost * spec.quantity * events
            finite = bool(
                np.isfinite([events, gross, net, drawdown, concentration, stress]).all()
            )
            hard_pass = bool(
                finite
                and events >= 5
                and net > 0
                and drawdown < 4500.0
                and spec.quantity <= 15
            )
            fast_fitness = _fast_combine_fitness(
                net=net,
                drawdown=drawdown,
                events=events,
                concentration=concentration,
                cost_stress_net=stress,
            )
            stage1_rows.append(
                {
                    "candidate_id": spec.candidate_id,
                    "structural_fingerprint": structural_fingerprint(spec),
                    "market": spec.market,
                    "family": spec.family,
                    "timeframe": spec.timeframe,
                    "role": spec.role.name,
                    "lineage_id": spec.lineage_id,
                    "events": events,
                    "gross_pnl": gross,
                    "net_pnl": net,
                    "cost_stress_1_5x_net": stress,
                    "approximate_max_drawdown": drawdown,
                    "best_positive_event_share": concentration,
                    "fast_combine_fitness": fast_fitness,
                    "stage1_pass": hard_pass,
                    "soft_evidence": {
                        "cost_fragile": stress <= 0,
                        "concentrated": concentration > 0.50,
                        "low_event_count": events < 15,
                    },
                    "disposition": "ROLLING_EPISODE_ELIGIBLE" if hard_pass else _fast_failure(
                        finite=finite, events=events, net=net, drawdown=drawdown
                    ),
                }
            )
    stage1_finished_at = time.perf_counter()
    writer.write_jsonl_batch("combine_v5_fast_screen.jsonl", stage1_rows)
    survivors = [row for row in stage1_rows if row["stage1_pass"]]

    registry_path = project_path("registry", "hydra_registry.db")
    if not registry_path.is_file():
        registry_path = Path("/root/hydra-bot/registry/hydra_registry.db")
    meta = fit_registry_propfirm_meta_screen(registry_path)
    survivor_specs = [spec_by_id[str(row["candidate_id"])] for row in survivors]
    meta_predictions = meta.predict(survivor_specs) if survivor_specs else {}
    targeted_specs, targeted_seed_audit = _load_existing_promising_specs()
    targeted_limit = min(len(targeted_specs), max(1, math.floor(exact_limit * 0.30)))
    targeted_specs = _select_targeted_promising_specs(
        targeted_specs, limit=targeted_limit
    )
    targeted_seed_audit = {
        **targeted_seed_audit,
        "selected_count": len(targeted_specs),
        "selected_markets": dict(
            sorted(Counter(spec.market for spec in targeted_specs).items())
        ),
        "selected_roles": dict(
            sorted(Counter(spec.role.name for spec in targeted_specs).items())
        ),
        "selection_policy": "QUALITY_RANK_WITH_MARKET_ROLE_ROUND_ROBIN",
    }
    targeted_ids = {spec.candidate_id for spec in targeted_specs}
    remaining_exact_capacity = max(0, exact_limit - len(targeted_specs))
    exact_rows = _select_rolling_candidates(
        survivors,
        spec_by_id=spec_by_id,
        meta_predictions=meta_predictions,
        meta_enabled=meta.allocation_enabled,
        limit=min(remaining_exact_capacity, len(survivors)),
        seed=random_seed + epoch_index,
    )
    exact_specs = targeted_specs + [
        spec_by_id[str(row["candidate_id"])]
        for row in exact_rows
        if str(row["candidate_id"]) not in targeted_ids
    ]
    for spec in targeted_specs:
        spec_by_id[spec.candidate_id] = spec
    rolling_started = time.perf_counter()
    exact_results, task_resources = _evaluate_specs(
        exact_specs,
        matrix_paths=feature_build.market_paths,
        worker_count=worker_count,
        maximum_episode_starts=maximum_episode_starts,
    )
    exact_finished_at = time.perf_counter()
    writer.write_jsonl_batch("combine_v5_rolling_results.jsonl", exact_results)

    parent_by_id = {row["candidate_id"]: row for row in exact_results}
    ranked_parents = sorted(
        exact_results,
        key=lambda row: (
            -_mutation_objective_metrics(
                row, spec_by_id[str(row["candidate_id"])]
            )[0],
            -_mutation_objective_metrics(
                row, spec_by_id[str(row["candidate_id"])]
            )[1],
            str(row["candidate_id"]),
        ),
    )
    proposals: list[MutationProposal] = []
    seen_configurations = set(historical_v5_configurations)
    seen_configurations.update(
        configuration_fingerprint(spec) for spec in exact_specs
    )
    for row in ranked_parents:
        if len(proposals) >= mutation_limit:
            break
        parent = spec_by_id[str(row["candidate_id"])]
        proposal = propose_failure_guided_mutation(
            parent,
            diagnosed_failure=str(row["failure_diagnosis"]),
            rolling_summary=dict(row["rolling_combine"]),
        )
        if proposal is None:
            continue
        fingerprint = configuration_fingerprint(proposal.child)
        if fingerprint in seen_configurations:
            continue
        seen_configurations.add(fingerprint)
        proposals.append(proposal)
    crossover_budget = min(10, max(0, mutation_limit - len(proposals)))
    for index in range(0, min(len(ranked_parents) - 1, crossover_budget * 2), 2):
        left = spec_by_id[str(ranked_parents[index]["candidate_id"])]
        right = spec_by_id[str(ranked_parents[index + 1]["candidate_id"])]
        proposal = constrained_crossover(left, right)
        if proposal is None:
            continue
        fingerprint = configuration_fingerprint(proposal.child)
        if fingerprint in seen_configurations:
            continue
        seen_configurations.add(fingerprint)
        proposals.append(proposal)
        if len(proposals) >= mutation_limit:
            break
    child_specs = [proposal.child for proposal in proposals]
    mutation_started = time.perf_counter()
    child_results, child_resources = _evaluate_specs(
        child_specs,
        matrix_paths=feature_build.market_paths,
        worker_count=worker_count,
        maximum_episode_starts=maximum_episode_starts,
    ) if child_specs else ([], None)
    mutation_finished_at = time.perf_counter()
    child_by_id = {row["candidate_id"]: row for row in child_results}
    writer.write_jsonl_batch("combine_v5_child_results.jsonl", child_results)
    mutation_outcomes: list[dict[str, Any]] = []
    for proposal in proposals:
        child = child_by_id.get(proposal.child.candidate_id)
        parent = parent_by_id.get(proposal.hypothesis.parent_candidate_id)
        if child is None or parent is None:
            continue
        parent_objective = _mutation_objective_metrics(parent, proposal.child)
        child_objective = _mutation_objective_metrics(child, proposal.child)
        mutation_outcomes.append(
            mutation_outcome(
                proposal,
                parent_fitness=parent_objective[0],
                child_fitness=child_objective[0],
                parent_pass_rate=parent_objective[1],
                child_pass_rate=child_objective[1],
                parent_mll_breach_rate=parent_objective[2],
                child_mll_breach_rate=child_objective[2],
            ).to_dict()
        )
    writer.write_json(
        "combine_v5_mutations.json",
        {
            "proposals": [proposal.to_dict() for proposal in proposals],
            "outcomes": mutation_outcomes,
        },
    )

    all_results = exact_results + child_results
    all_specs = {spec.candidate_id: spec for spec in exact_specs + child_specs}
    archive, archive_decisions = _build_archive(all_results, all_specs)
    elites = sorted(
        archive.candidates,
        key=lambda item: (
            -float(item.quality.get("combine_fitness", 0.0)),
            item.candidate_id,
        ),
    )
    elite_ids = [item.candidate_id for item in elites]
    combine_elites = [
        row
        for row in all_results
        if all_specs[str(row["candidate_id"])].role.name
        in {"ALPHA", "COMBINE_PASSER"}
        and bool(row["combine_fitness"]["elite"])
    ]
    xfa_elites = [
        row
        for row in all_results
        if all_specs[str(row["candidate_id"])].role.name == "XFA_PAYOUT"
        and bool(row["xfa_fitness"]["elite"])
    ]
    factory_survivors = [
        row
        for row in all_results
        if _role_factory_survivor(
            row, all_specs[str(row["candidate_id"])]
        )
    ]
    candidates = [
        _candidate_record(row, all_specs[str(row["candidate_id"])])
        for row in all_results
        if row["candidate_id"] in elite_ids
        or _role_factory_survivor(
            row, all_specs[str(row["candidate_id"])]
        )
    ]
    resource_after = capture_resource_snapshot()
    total_seconds = max(time.perf_counter() - started, 1e-9)
    pass_rates = [float(row["rolling_combine"]["pass_rate"]) for row in all_results]
    breach_rates = [
        float(row["rolling_combine"]["mll_breach_rate"]) for row in all_results
    ]
    mutation_successes = sum(
        row["decision"] == "KEEP_CHILD" for row in mutation_outcomes
    )
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": (
            "COMBINE_FIRST_ELITES_FOUND"
            if combine_elites
            else "COMBINE_FIRST_EPOCH_COMPLETED_MUTATION_CONTINUES"
        ),
        "interpretation_boundary": (
            "Rolling development episodes are direct prop-firm research evidence, "
            "not protected holdout, PAPER_SHADOW_READY or funded authorization."
        ),
        "epoch_index": epoch_index,
        "candidate_count": len(population),
        "requested_structural_proposals": proposal_count,
        "structural_proposals": len(population),
        "adaptive_capacity": capacity,
        "stage0_valid": len(population),
        "fast_screens": len(stage1_rows),
        "fast_screen_survivors": len(survivors),
        "rolling_candidates_evaluated": len(exact_results),
        "targeted_historical_seeds_evaluated": len(targeted_specs),
        "targeted_seed_audit": targeted_seed_audit,
        "mutation_children_evaluated": len(child_results),
        "evolution_population_evaluated": len(all_results),
        "unique_rolling_configurations": len(
            {str(row["candidate_id"]) for row in all_results}
        ),
        "rolling_episode_count": sum(
            int(row["rolling_combine"]["episode_start_count"])
            for row in all_results
        ),
        "rolling_effective_block_count": sum(
            int(row["rolling_combine"]["effective_block_count"])
            for row in all_results
        ),
        "factory_survivor_count": len(factory_survivors),
        "combine_elite_count": len(combine_elites),
        "xfa_candidate_count": len(xfa_elites),
        "defensive_candidate_count": 0,
        "defensive_account_replay_pending_count": sum(
            all_specs[str(row["candidate_id"])].role.name
            in {"DEFENSIVE", "PORTFOLIO_ONLY", "HAZARD"}
            for row in all_results
        ),
        "mutation_success_count": mutation_successes,
        "mutation_success_rate": (
            mutation_successes / len(mutation_outcomes) if mutation_outcomes else 0.0
        ),
        "archive": {
            **archive.summary(),
            "accepted_candidate_ids": elite_ids,
            "decision_counts": dict(Counter(row["reason"] for row in archive_decisions)),
        },
        "combine_pass_rate_distribution": _distribution(pass_rates),
        "mll_breach_rate_distribution": _distribution(breach_rates),
        "median_days_to_target_distribution": _distribution(
            [
                float(row["rolling_combine"]["median_days_to_target"])
                for row in all_results
                if row["rolling_combine"]["median_days_to_target"] is not None
            ]
        ),
        "consistency_pass_rate_distribution": _distribution(
            [
                float(row["rolling_combine"]["consistency_pass_rate"])
                for row in all_results
            ]
        ),
        "payout_cycle_distribution": _distribution(
            [
                float(
                    row["rolling_xfa"]["expected_payout_cycles_before_ruin"]
                )
                for row in all_results
            ]
        ),
        "combine_elite_candidate_ids": [row["candidate_id"] for row in combine_elites],
        "xfa_candidate_ids": [row["candidate_id"] for row in xfa_elites],
        "candidates": candidates,
        "meta_screen": meta.report(),
        "quality_diversity": _population_coverage(population),
        "evaluated_role_distribution": dict(
            sorted(Counter(all_specs[str(row["candidate_id"])].role.name for row in all_results).items())
        ),
        "performance": {
            "total_seconds": total_seconds,
            "generation_seconds": generation_ready_at - features_ready_at,
            "feature_preparation_seconds": features_ready_at - started,
            "fast_screen_seconds": stage1_finished_at - generation_ready_at,
            "rolling_evaluation_seconds": exact_finished_at - rolling_started,
            "mutation_evaluation_seconds": mutation_finished_at - mutation_started,
            "proposals_per_hour": len(population) / total_seconds * 3600.0,
            "fast_screens_per_hour": len(stage1_rows) / max(
                stage1_finished_at - generation_ready_at, 1e-9
            ) * 3600.0,
            "rolling_candidates_per_hour": len(exact_results) / max(
                exact_finished_at - rolling_started, 1e-9
            ) * 3600.0,
            "worker_resources": asdict(task_resources),
            "mutation_worker_resources": (
                asdict(child_resources) if child_resources is not None else None
            ),
            "resource_before": asdict(resource_before),
            "resource_after": asdict(resource_after),
        },
        "feature_store": {
            "cache_hits": feature_build.cache_hits,
            "cache_misses": feature_build.cache_misses,
            "rows": feature_build.rows,
            "source_fingerprint": feature_build.source_fingerprint,
            "markets": sorted(feature_build.market_paths),
            "timeframes": ["1m", "5m", "15m", "30m", "60m", "session", "daily"],
        },
        "shadow": {
            "new_config_complete": 0,
            "new_waiting_for_market_open": 0,
            "new_genuinely_active": 0,
            "fresh_bars": 0,
            "signals": 0,
            "virtual_fills": 0,
            "packaging_required_for_elites": [row["candidate_id"] for row in combine_elites],
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
            "PACKAGE_COMBINE_ELITES_AND_RUN_NEXT_EVOLUTION_EPOCH"
            if combine_elites
            else "CONTINUE_FAILURE_GUIDED_COMBINE_EVOLUTION"
        ),
        "code_commit": code_commit,
    }
    payload["scientific_result_hash"] = _stable_hash(
        _scientific_hash_payload(payload)
    )
    payload["result_hash"] = payload["scientific_result_hash"]
    result_receipt = writer.write_json("combine_v5_result.json", payload)
    report_receipt = writer.write_text("combine_v5_report.md", _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_path": str(destination / result_receipt.relative_path),
            "result_sha256": result_receipt.sha256,
            "report_path": str(destination / report_receipt.relative_path),
            "report_sha256": report_receipt.sha256,
            "population_manifest_path": str(
                destination / "combine_v5_population_manifest.json"
            ),
            "fast_screen_path": str(destination / "combine_v5_fast_screen.jsonl"),
            "rolling_results_path": str(
                destination / "combine_v5_rolling_results.jsonl"
            ),
            "child_results_path": str(destination / "combine_v5_child_results.jsonl"),
        },
        "report_path": str(destination / report_receipt.relative_path),
    }


def _evaluate_specs(
    specs: Sequence[StrategySpec],
    *,
    matrix_paths: Mapping[str, str],
    worker_count: int,
    maximum_episode_starts: int,
) -> tuple[list[dict[str, Any]], Any]:
    if not specs:
        return [], summarize_task_resources(
            [], batch_wall_seconds=1e-9, worker_count_budget=worker_count
        )
    tasks = [
        PureTask(
            task_id=spec.candidate_id,
            payload={
                "matrix_path": matrix_paths[spec.market],
                "specification": spec_to_dict(spec),
                "maximum_episode_starts": maximum_episode_starts,
                "maximum_xfa_starts": min(12, maximum_episode_starts),
            },
        )
        for spec in specs
    ]
    started = time.perf_counter()
    with LongLivedWorkerPool(max_workers=worker_count) as pool:
        results = pool.run_batch(run_rolling_combine_job, tasks)
    wall = max(time.perf_counter() - started, 1e-9)
    failures = [row for row in results if not row.succeeded]
    if failures:
        raise CombineFirstV5Error(
            "rolling worker failures: "
            + repr(
                [
                    (row.task_id, row.error_type, row.error_message)
                    for row in failures[:3]
                ]
            )
        )
    resources = summarize_task_resources(
        results,
        batch_wall_seconds=wall,
        worker_count_budget=worker_count,
        scheduler_idle_seconds=0.0,
    )
    output = [dict(row.value or {}) for row in results]
    output.sort(key=lambda row: str(row["candidate_id"]))
    return output, resources


def _generate_adaptive_population(
    matrices: Mapping[str, FeatureMatrix],
    *,
    requested_count: int,
    batch_index: int,
    random_seed: int,
    excluded_fingerprints: Iterable[str],
) -> tuple[list[StrategySpec], dict[str, Any]]:
    excluded = tuple(excluded_fingerprints)
    attempted: list[int] = []
    count = requested_count
    while True:
        attempted.append(count)
        try:
            population = generate_turbo_population(
                matrices,
                count=count,
                batch_index=batch_index,
                random_seed=random_seed,
                excluded_fingerprints=excluded,
            )
            return population, {
                "requested_count": requested_count,
                "actual_count": len(population),
                "attempted_counts": attempted,
                "downscaled": len(population) < requested_count,
                "structural_discovery_exhausted": False,
                "selection_uses_future_economics": False,
            }
        except TurboFoundryError as exc:
            available = _reported_structural_capacity(str(exc))
            if available is None or available >= count:
                raise
            if available <= 0:
                return [], {
                    "requested_count": requested_count,
                    "actual_count": 0,
                    "attempted_counts": attempted,
                    "downscaled": True,
                    "structural_discovery_exhausted": True,
                    "selection_uses_future_economics": False,
                }
            count = available


def _reported_structural_capacity(message: str) -> int | None:
    patterns = (
        r"permit only\s+(\d+)\s+of\s+\d+",
        r"Only\s+(\d+)\s+unique structures are available",
    )
    for pattern in patterns:
        match = re.search(pattern, message, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _select_rolling_candidates(
    rows: Sequence[dict[str, Any]],
    *,
    spec_by_id: Mapping[str, StrategySpec],
    meta_predictions: Mapping[str, Mapping[str, float]],
    meta_enabled: bool,
    limit: int,
    seed: int,
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        spec = spec_by_id[str(row["candidate_id"])]
        meta_score = float(
            meta_predictions.get(spec.candidate_id, {}).get(
                "rolling_success_priority", 0.5
            )
        )
        row["meta_priority"] = meta_score
        row["selection_score"] = float(row["fast_combine_fitness"]) + (
            0.10 * meta_score if meta_enabled else 0.0
        )
        key = (
            _ecology(spec.market),
            spec.market,
            spec.family,
            spec.timeframe,
            spec.role.name,
            str(spec.session_code),
            str(spec.holding_events),
        )
        groups[key].append(row)
    for values in groups.values():
        values.sort(
            key=lambda row: (-float(row["selection_score"]), row["candidate_id"])
        )
    exploitation_limit = max(
        0, limit - max(1, math.ceil(limit * MINIMUM_EXPLORATION_SHARE))
    )
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    keys = sorted(groups)
    depth = 0
    while len(selected) < exploitation_limit:
        inserted = False
        for key in keys:
            values = groups[key]
            if depth >= len(values):
                continue
            row = values[depth]
            selected.append(row)
            selected_ids.add(str(row["candidate_id"]))
            inserted = True
            if len(selected) >= exploitation_limit:
                break
        if not inserted:
            break
        depth += 1
    remaining = [row for row in rows if str(row["candidate_id"]) not in selected_ids]
    remaining.sort(
        key=lambda row: hashlib.sha256(
            f"{seed}:{row['candidate_id']}".encode()
        ).hexdigest()
    )
    selected.extend(remaining[: max(0, limit - len(selected))])
    for rank, row in enumerate(selected, start=1):
        row["selection_rank"] = rank
        row["selection_lane"] = (
            "QUALITY_DIVERSITY_EXPLOITATION"
            if rank <= exploitation_limit
            else "PURE_EXPLORATION"
        )
    return selected


def _load_existing_promising_specs() -> tuple[list[StrategySpec], dict[str, Any]]:
    roots = [
        project_path("reports", "mission_experiments"),
        Path("/root/hydra-bot/reports/mission_experiments"),
    ]
    decided_q4 = {
        "strategy_turbo_513928315dfecc5eb134b00f_v1",
        "strategy_turbo_2781fc4182a7e6cf67b3af94_v1",
        "strategy_turbo_5a12cb218a9872f44939f0f3_v1",
    }
    retained_statuses = {
        "PROMISING_RESEARCH_CANDIDATE",
        "ROBUST_RESEARCH_CANDIDATE",
        "SHADOW_RESEARCH_CANDIDATE",
        "SHADOW_RESEARCH_ONLY",
    }
    rows: list[tuple[float, StrategySpec]] = []
    seen_files: set[Path] = set()
    turbo_source_files = 0
    v5_source_files = 0
    inspected = 0
    excluded_decided = 0
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.glob("turbo_promotion_batch_*/turbo_promotion_result.json"):
            resolved = path.resolve()
            if resolved in seen_files:
                continue
            seen_files.add(resolved)
            turbo_source_files += 1
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for candidate in payload.get("candidates") or []:
                inspected += 1
                candidate_id = str(candidate.get("candidate_id") or "")
                if candidate_id in decided_q4:
                    excluded_decided += 1
                    continue
                if str(candidate.get("status") or "") not in retained_statuses:
                    continue
                specification = candidate.get("specification")
                if not isinstance(specification, dict):
                    continue
                try:
                    spec = spec_from_dict(specification)
                except (TypeError, ValueError, KeyError):
                    continue
                quality = float(candidate.get("net_pnl") or 0.0) / max(
                    float(candidate.get("maximum_drawdown") or 0.0), 100.0
                )
                quality += 2.0 * int(
                    bool((candidate.get("topstep") or {}).get("path_candidate"))
                )
                rows.append((quality, spec))
        for path in root.glob(
            "combine_first_evolution_v5_epoch_*/combine_v5_result.json"
        ):
            resolved = path.resolve()
            if resolved in seen_files:
                continue
            seen_files.add(resolved)
            v5_source_files += 1
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for candidate in payload.get("candidates") or []:
                inspected += 1
                candidate_id = str(candidate.get("candidate_id") or "")
                if candidate_id in decided_q4:
                    excluded_decided += 1
                    continue
                if str(candidate.get("status") or "") not in retained_statuses:
                    continue
                specification = candidate.get("specification")
                if not isinstance(specification, dict):
                    continue
                try:
                    spec = spec_from_dict(specification)
                except (TypeError, ValueError, KeyError):
                    continue
                role_fitness = dict(candidate.get("role_specific_fitness") or {})
                quality = 10.0 * float(role_fitness.get("score") or 0.0)
                quality += float(candidate.get("net_pnl") or 0.0) / max(
                    float(candidate.get("maximum_drawdown") or 0.0), 100.0
                )
                quality += 3.0 * int(
                    bool(candidate.get("topstep_path_candidate"))
                )
                rows.append((quality, spec))
        for path in root.glob(
            "combine_first_evolution_v5_epoch_*/combine_v5_mutations.json"
        ):
            resolved = path.resolve()
            if resolved in seen_files:
                continue
            seen_files.add(resolved)
            v5_source_files += 1
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            outcomes = {
                str(row.get("child_candidate_id") or ""): row
                for row in payload.get("outcomes") or []
                if str(row.get("decision") or "") == "KEEP_CHILD"
            }
            for proposal in payload.get("proposals") or []:
                child_payload = proposal.get("child")
                if not isinstance(child_payload, dict):
                    continue
                candidate_id = str(child_payload.get("candidate_id") or "")
                outcome = outcomes.get(candidate_id)
                if outcome is None:
                    continue
                inspected += 1
                try:
                    spec = spec_from_dict(child_payload)
                except (TypeError, ValueError, KeyError):
                    continue
                rows.append((10.0 * float(outcome.get("child_fitness") or 0.0), spec))
    unique: dict[str, tuple[float, StrategySpec]] = {}
    for quality, spec in rows:
        fingerprint = structural_fingerprint(spec)
        current = unique.get(fingerprint)
        if current is None or quality > current[0]:
            unique[fingerprint] = (quality, spec)
    ranked = [
        row[1]
        for row in sorted(
            unique.values(),
            key=lambda row: (-row[0], row[1].candidate_id),
        )
    ]
    return ranked, {
        "source_files": len(seen_files),
        "turbo_source_files": turbo_source_files,
        "v5_source_files": v5_source_files,
        "candidate_rows_inspected": inspected,
        "q4_decided_versions_excluded": excluded_decided,
        "unique_promising_mechanisms": len(ranked),
        "q4_reuse_prohibited": True,
    }


def _historical_v5_fingerprints() -> tuple[set[str], set[str]]:
    roots = [
        project_path("reports", "mission_experiments"),
        Path("/root/hydra-bot/reports/mission_experiments"),
    ]
    structural: set[str] = set()
    configurations: set[str] = set()
    seen_paths: set[Path] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.glob(
            "combine_first_evolution_v5_epoch_*/combine_v5_population_manifest.json"
        ):
            resolved = path.resolve()
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for row in payload.get("specifications") or []:
                try:
                    spec = spec_from_dict(dict(row))
                except (KeyError, TypeError, ValueError):
                    continue
                structural.add(structural_fingerprint(spec))
                configurations.add(configuration_fingerprint(spec))
        for path in root.glob(
            "combine_first_evolution_v5_epoch_*/combine_v5_mutations.json"
        ):
            resolved = path.resolve()
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for proposal in payload.get("proposals") or []:
                try:
                    child = spec_from_dict(dict(proposal["child"]))
                except (KeyError, TypeError, ValueError):
                    continue
                configurations.add(configuration_fingerprint(child))
    return structural, configurations


def _select_targeted_promising_specs(
    ranked: Sequence[StrategySpec], *, limit: int
) -> list[StrategySpec]:
    """Retain quality order while preventing one liquid index role from seeding V5."""

    if limit <= 0:
        return []
    groups: dict[str, dict[str, list[StrategySpec]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for spec in ranked:
        groups[spec.market][spec.role.name].append(spec)
    ecology_markets: dict[str, list[str]] = defaultdict(list)
    for market in groups:
        ecology_markets[_ecology(market)].append(market)
    markets: list[str] = []
    market_depth = 0
    while True:
        inserted = False
        for ecology in sorted(ecology_markets):
            values = sorted(ecology_markets[ecology])
            if market_depth >= len(values):
                continue
            markets.append(values[market_depth])
            inserted = True
        if not inserted:
            break
        market_depth += 1
    market_lanes: dict[str, list[StrategySpec]] = {}
    for market_index, market in enumerate(markets):
        role_groups = groups[market]
        roles = sorted(role_groups)
        offset = market_index % len(roles)
        roles = roles[offset:] + roles[:offset]
        lane: list[StrategySpec] = []
        role_depth = 0
        while True:
            inserted = False
            for role in roles:
                values = role_groups[role]
                if role_depth >= len(values):
                    continue
                lane.append(values[role_depth])
                inserted = True
            if not inserted:
                break
            role_depth += 1
        market_lanes[market] = lane
    selected: list[StrategySpec] = []
    depth = 0
    while len(selected) < limit:
        inserted = False
        for market in markets:
            lane = market_lanes[market]
            if depth >= len(lane):
                continue
            selected.append(lane[depth])
            inserted = True
            if len(selected) >= limit:
                break
        if not inserted:
            break
        depth += 1
    return selected


def _build_archive(
    rows: Sequence[dict[str, Any]], specs: Mapping[str, StrategySpec]
) -> tuple[QualityDiversityArchive, list[dict[str, Any]]]:
    by_mechanism: dict[str, dict[str, Any]] = {}
    for row in rows:
        spec = specs[str(row["candidate_id"])]
        mechanism = structural_fingerprint(spec)
        current = by_mechanism.get(mechanism)
        if current is None or float(row["combine_fitness"]["score"]) > float(
            current["combine_fitness"]["score"]
        ):
            by_mechanism[mechanism] = row
    ecologies = {_ecology(specs[str(row["candidate_id"])].market) for row in by_mechanism.values()}
    archive = QualityDiversityArchive(
        niche_capacity=3,
        maximum_family_share=0.25,
        maximum_ecology_share=0.40 if len(ecologies) >= 3 else 1.0,
        maximum_lineage_share=0.02,
    )
    candidates: list[ArchiveCandidate] = []
    for mechanism, row in by_mechanism.items():
        spec = specs[str(row["candidate_id"])]
        rolling = dict(row["rolling_combine"])
        exact = dict(row["exact_trade_path"])
        fitness = dict(row["combine_fitness"])
        xfa = dict(row["xfa_fitness"])
        folds = dict(exact["fold_results"])
        positive_folds = sum(float(value["net_pnl"]) > 0 for value in folds.values())
        candidates.append(
            ArchiveCandidate(
                candidate_id=spec.candidate_id,
                structural_fingerprint=mechanism,
                lineage_id=spec.lineage_id,
                family=spec.family,
                niche=ArchiveNiche(
                    market_ecology=_ecology(spec.market),
                    timeframe_profile=spec.timeframe,
                    holding_horizon=(
                        "SCALP" if spec.holding_events <= 5 else "INTRADAY_SHORT"
                        if spec.holding_events <= 15 else "INTRADAY_LONG"
                    ),
                    session=str(spec.session_code),
                    mechanism_family=spec.family,
                    portfolio_role=spec.role.name,
                    turnover=(
                        "RARE" if int(exact["event_count"]) < 15 else "MEDIUM"
                        if int(exact["event_count"]) < 60 else "HIGH"
                    ),
                    behavioral_cluster=f"{spec.market}:{spec.family}:{spec.side}",
                ),
                quality={
                    "combine_pass_rate": float(rolling["pass_rate"]),
                    "combine_fitness": float(fitness["score"]),
                    "mll_survival": 1.0 - float(rolling["mll_breach_rate"]),
                    "target_velocity": float(
                        rolling["median_target_progress_when_not_passed"]
                    ),
                    "xfa_payout_utility": float(xfa["score"]),
                    "defensive_utility": 0.0,
                    "net_economics": _clip01(float(exact["net_pnl"]) / 9000.0),
                    "temporal_transfer": positive_folds / max(len(folds), 1),
                    "cost_resilience": _clip01(
                        float(exact["cost_stress_1_5x_net"])
                        / max(abs(float(exact["net_pnl"])), 1.0)
                    ),
                    "mll_buffer": _clip01(
                        float(rolling["minimum_mll_buffer"]) / 4500.0
                    ),
                    "null_evidence": 0.0,
                    "behavioral_novelty": 1.0,
                    "execution_confidence": 1.0
                    if int(rolling["same_bar_ambiguous_count"]) == 0
                    else 0.5,
                    "portfolio_utility": 0.0,
                    "complexity": float(
                        1 + int(spec.context_feature is not None) + int(spec.session_code >= 0)
                    ),
                },
                payload=row,
            )
        )
    candidates.sort(
        key=lambda item: (
            item.family,
            item.niche.market_ecology,
            -float(item.quality["combine_fitness"]),
            item.candidate_id,
        )
    )
    decisions: list[dict[str, Any]] = []
    for candidate in candidates:
        decision = archive.insert(candidate)
        decisions.append(
            {
                "candidate_id": candidate.candidate_id,
                "accepted": decision.accepted,
                "reason": decision.reason,
                "replaced_candidate_id": decision.replaced_candidate_id,
            }
        )
    return archive, decisions


def _candidate_record(row: Mapping[str, Any], spec: StrategySpec) -> dict[str, Any]:
    combine = dict(row["combine_fitness"])
    xfa = dict(row["xfa_fitness"])
    rolling = dict(row["rolling_combine"])
    exact = dict(row["exact_trade_path"])
    role_survivor = _role_factory_survivor(row, spec)
    tier = "FACTORY_SURVIVOR" if role_survivor else "RESEARCH_PROTOTYPE"
    status = (
        "PROMISING_RESEARCH_CANDIDATE"
        if tier == "FACTORY_SURVIVOR"
        else "RAW_ECONOMIC_SIGNAL"
        if float(exact["net_pnl"]) > 0
        else "RESEARCH_PROTOTYPE"
    )
    return {
        "candidate_id": spec.candidate_id,
        "lineage_id": spec.lineage_id,
        "status": status,
        "evidence_tier": tier,
        "mechanism_family": spec.family,
        "primary_market": spec.market,
        "execution_market": MARKET_PAIRS[spec.market],
        "role": spec.role.name,
        "objective_pool": _objective_pool(spec),
        "role_specific_fitness": (
            combine
            if _objective_pool(spec) == "COMBINE_PASSER_POOL"
            else xfa
            if _objective_pool(spec) == "XFA_PAYOUT_POOL"
            else {
                "objective": "DEFENSIVE_ACCOUNT_FITNESS",
                "decision": "ACCOUNT_BASELINE_REPLAY_REQUIRED",
                "factory_survivor": False,
                "elite": False,
            }
        ),
        "role_specific_evidence_complete": _objective_pool(spec)
        != "DEFENSIVE_ACCOUNT_POOL",
        "timeframe": spec.timeframe,
        "structural_fingerprint": structural_fingerprint(spec),
        "configuration_fingerprint": configuration_fingerprint(spec),
        "net_pnl": float(exact["net_pnl"]),
        "events": int(exact["event_count"]),
        "cost_stress_1_5x_net": float(exact["cost_stress_1_5x_net"]),
        "maximum_drawdown": float(exact["maximum_drawdown"]),
        "rolling_combine": rolling,
        "combine_fitness": combine,
        "rolling_xfa": row["rolling_xfa"],
        "xfa_fitness": xfa,
        "failure_diagnosis": row["failure_diagnosis"],
        "topstep_path_candidate": bool(combine["elite"]),
        "shadow_package_complete": False,
        "paper_shadow_ready": False,
        "hard_invalidation": False,
        "evidence_boundary": "development_rolling_episodes_q4_consumed_no_holdout_promotion",
        "specification": spec_to_dict(spec),
    }


def _objective_pool(spec: StrategySpec) -> str:
    if spec.role.name in {"ALPHA", "COMBINE_PASSER"}:
        return "COMBINE_PASSER_POOL"
    if spec.role.name == "XFA_PAYOUT":
        return "XFA_PAYOUT_POOL"
    return "DEFENSIVE_ACCOUNT_POOL"


def _mutation_objective_metrics(
    row: Mapping[str, Any], spec: StrategySpec
) -> tuple[float, float, float]:
    if _objective_pool(spec) == "XFA_PAYOUT_POOL":
        return (
            float(dict(row["xfa_fitness"])["score"]),
            float(dict(row["rolling_xfa"])["payout_probability"]),
            1.0 - float(dict(row["rolling_xfa"])["survival_rate"]),
        )
    return (
        float(dict(row["combine_fitness"])["score"]),
        float(dict(row["rolling_combine"])["pass_rate"]),
        float(dict(row["rolling_combine"])["mll_breach_rate"]),
    )


def _role_factory_survivor(
    row: Mapping[str, Any], spec: StrategySpec
) -> bool:
    pool = _objective_pool(spec)
    if pool == "COMBINE_PASSER_POOL":
        return bool(dict(row["combine_fitness"])["factory_survivor"])
    if pool == "XFA_PAYOUT_POOL":
        return bool(dict(row["xfa_fitness"])["factory_survivor"])
    # A directional replay cannot establish marginal defensive account value.
    # Those configurations remain in their QD niches for a later matched
    # shared-account replay but cannot inherit alpha/XFA survivor status.
    return False


def _fast_combine_fitness(
    *, net: float, drawdown: float, events: int, concentration: float, cost_stress_net: float
) -> float:
    target = _clip01(net / 9000.0)
    buffer = _clip01(1.0 - drawdown / 4500.0)
    opportunity = _clip01(events / 30.0)
    cost = _clip01(cost_stress_net / max(abs(net), 1.0)) if net > 0 else 0.0
    concentration_penalty = 0.10 * _clip01((concentration - 0.50) / 0.50)
    return round(
        _clip01(0.40 * target + 0.30 * buffer + 0.15 * opportunity + 0.15 * cost - concentration_penalty),
        8,
    )


def _fast_failure(*, finite: bool, events: int, net: float, drawdown: float) -> str:
    if not finite:
        return "HARD_NONFINITE"
    if net <= 0:
        return "NET_NEGATIVE"
    if drawdown >= 4500.0:
        return "CATASTROPHIC_MLL_PROXY"
    if events < 5:
        return "INSUFFICIENT_BATCH_POWER"
    return "FAST_SCREEN_REJECTED"


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "p25": None, "median": None, "p75": None, "max": None}
    array = np.asarray(values, dtype=float)
    return {
        "count": len(array),
        "min": float(array.min()),
        "p25": float(np.percentile(array, 25)),
        "median": float(np.median(array)),
        "p75": float(np.percentile(array, 75)),
        "max": float(array.max()),
    }


def _scientific_hash_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    deterministic = dict(payload)
    deterministic.pop("performance", None)
    deterministic.pop("result_hash", None)
    deterministic.pop("scientific_result_hash", None)
    feature_store = dict(deterministic.get("feature_store") or {})
    feature_store.pop("cache_hits", None)
    feature_store.pop("cache_misses", None)
    deterministic["feature_store"] = feature_store
    return deterministic


def _record_access_once(epoch_index: int) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = f"Combine-First V5 epoch {epoch_index}: rolling development account episodes; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.is_file():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("requesting_module") == __name__ and row.get("reason_for_access") == reason:
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        __name__,
        [f"combine_v5_epoch_{epoch_index}_frozen_population"],
        reason,
        None,
    )
    return record.__dict__


def _canonical_cache_root() -> Path:
    root = Path("/root/hydra-bot/data/cache/turbo_foundry_v2")
    return root if root.parent.is_dir() else project_path("data", "cache", "turbo_foundry_v2")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise CombineFirstV5Error(f"frozen {label} is missing or changed: {path}")


def _clip01(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return float(max(0.0, min(1.0, value)))


def _render_report(payload: Mapping[str, Any]) -> str:
    performance = dict(payload["performance"])
    return "\n".join(
        [
            "# HYDRA Combine-First Evolution Factory V5",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Structural proposals / fast screens: `{payload['structural_proposals']}` / `{payload['fast_screens']}`",
            f"- Fast survivors / rolling candidates: `{payload['fast_screen_survivors']}` / `{payload['rolling_candidates_evaluated']}`",
            f"- Mutation children / successful mutations: `{payload['mutation_children_evaluated']}` / `{payload['mutation_success_count']}`",
            f"- Combine elites / XFA candidates: `{payload['combine_elite_count']}` / `{payload['xfa_candidate_count']}`",
            f"- Fast-screen throughput: `{performance['fast_screens_per_hour']:.1f}` per hour",
            f"- Rolling-episode candidate throughput: `{performance['rolling_candidates_per_hour']:.1f}` per hour",
            f"- Worker utilization: `{performance['worker_resources']['aggregate_worker_utilization_pct']:.1f}%`",
            "- Q4 access delta: `0`",
            "- Databento spend: `$0`",
            "- Outbound order capability: `false`",
            "- PAPER_SHADOW_READY: `0`",
            "",
            "## Interpretation boundary",
            "",
            str(payload["interpretation_boundary"]),
            "",
        ]
    )


__all__ = [
    "CombineFirstV5Error",
    "run_combine_first_evolution_v5",
]
