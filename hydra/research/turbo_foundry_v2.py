from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import subprocess
import time
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.compute.backpressure import BackpressureLimits, assess_backpressure
from hydra.compute.resource_monitor import (
    capture_resource_snapshot,
    summarize_task_resources,
)
from hydra.compute.result_writer import AtomicResultWriter
from hydra.compute.worker_pool import LongLivedWorkerPool, PureTask
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.adaptive_batch_size import BatchCapacity, choose_adaptive_batch_size
from hydra.research.power_planner import PowerPlanningRequest, plan_experiment_power
from hydra.research.qd_economic_tournament import (
    FEATURE_FAMILIES,
    FEATURES,
    MARKET_PAIRS,
    _round_turn_cost_all,
)
from hydra.research.turbo_exact_replay import (
    benchmark_exact_replay,
    run_exact_replay_job,
    spec_to_dict,
)
from hydra.research.turbo_feature_builder import (
    HORIZONS,
    build_or_open_turbo_feature_bundles,
    feature_names_for_bundle,
)
from hydra.research.turbo_meta_screen import (
    MetaScreenError,
    fit_temporal_meta_screen,
    prioritize_with_exploration,
)
from hydra.strategies.turbo_batch_fingerprint import (
    deduplicate_specs,
    structural_fingerprint,
)
from hydra.strategies.turbo_compiler import compile_strategy_batch
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategyRole, StrategySpec
from hydra.strategies.turbo_vectorized_executor import (
    EventMatrix,
    benchmark_stage1,
    execute_stage1_vectorized,
)
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "hydra_turbo_foundry_v2_epoch_v1"
POPULATION_VERSION = "hydra_turbo_population_v2"
PROPOSAL_TARGET = 6_000
EXACT_REPLAY_LIMIT = 180
PROMOTION_QUEUE_LIMIT = 60
CALIBRATION_START = "2023-01-01"
CALIBRATION_END = "2023-04-01"
STAGE1_START = "2023-04-01"
STAGE1_END = "2024-01-01"
VALIDATION_END = "2024-10-01"
QUANTILES = (0.55, 0.65, 0.75, 0.85)
CONTEXTS: tuple[tuple[str | None, ComparisonOperator | None, float | None, str], ...] = (
    (None, None, None, "1m"),
    ("ctx_5m_return", ComparisonOperator.GREATER_THAN, 0.0, "1m|5m"),
    ("ctx_5m_return", ComparisonOperator.LESS_THAN, 0.0, "1m|5m"),
    ("ctx_15m_return", ComparisonOperator.GREATER_THAN, 0.0, "1m|15m"),
    ("ctx_15m_return", ComparisonOperator.LESS_THAN, 0.0, "1m|15m"),
    ("ctx_30m_return", ComparisonOperator.GREATER_THAN, 0.0, "1m|30m"),
    ("ctx_60m_return", ComparisonOperator.GREATER_THAN, 0.0, "1m|60m"),
    ("ctx_60m_return", ComparisonOperator.LESS_THAN, 0.0, "1m|60m"),
    (
        "ctx_15m_volatility_expansion",
        ComparisonOperator.GREATER_EQUAL,
        0.5,
        "1m|15m",
    ),
)
META_FEATURES = (
    "family_code",
    "market_code",
    "timeframe_code",
    "parameter_count",
    "numeric_parameter_scale",
    "risk_parameter_count",
    "is_mutation",
)


class TurboFoundryError(RuntimeError):
    pass


def run_turbo_foundry_v2_epoch(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    contract_map_path: str | Path,
    contract_map_sha256: str,
    code_commit: str,
    batch_index: int = 0,
    worker_count: int = 3,
    record_data_access: bool = True,
    random_seed: int = 20260712,
) -> dict[str, Any]:
    started = time.perf_counter()
    task_path = Path(engineering_task_path)
    map_path = Path(contract_map_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(map_path, contract_map_sha256, "explicit-contract map")
    if len(code_commit) == 40:
        actual_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        if actual_commit != code_commit:
            raise TurboFoundryError(
                "Turbo worker commit differs from its immutable queued specification."
            )
    if worker_count < 1 or worker_count > 3:
        raise TurboFoundryError("Turbo V2 supports one to three pure compute workers.")
    destination = Path(output_dir)
    writer = AtomicResultWriter(destination)
    power_plan = plan_experiment_power(
        PowerPlanningRequest(
            minimum_useful_effect=0.15,
            outcome_variance=1.0,
            expected_opportunity_frequency=0.01,
            observations_per_structure=50_000,
            available_events=10_000_000,
            maximum_structures=PROPOSAL_TARGET,
            effect_prevalence=0.0005,
            search_coverage_probability=0.95,
        )
    )
    adaptive = choose_adaptive_batch_size(
        power_plan,
        BatchCapacity(
            maximum_proposals=PROPOSAL_TARGET,
            available_proposals=PROPOSAL_TARGET,
            candidates_per_second=100.0,
            wall_time_budget_seconds=120.0,
            micro_batch_size=64,
            worker_count=worker_count,
        ),
    )
    proposal_count = max(5_000, min(PROPOSAL_TARGET, adaptive.scheduled_structures))
    preregistration = {
        "schema": "hydra_turbo_foundry_v2_preregistration_v1",
        "batch_index": batch_index,
        "proposal_count": proposal_count,
        "power_plan": power_plan.to_dict(),
        "adaptive_batch_plan": adaptive.to_dict(),
        "calibration_period": [CALIBRATION_START, CALIBRATION_END],
        "stage1_period": [STAGE1_START, STAGE1_END],
        "validation_period": ["2024-01-01", VALIDATION_END],
        "maximum_exact_replays": EXACT_REPLAY_LIMIT,
        "maximum_promotion_queue": PROMOTION_QUEUE_LIMIT,
        "minimum_exploration_share": 0.20,
        "maximum_family_share": 0.25,
        "maximum_ecology_share": 0.40,
        "ecology_cap_feasibility_policy": (
            "40_percent_with_three_valid_ecologies; redistribute unavailable "
            "ecology quota equally across ecologies with calibration support"
        ),
        "maximum_lineage_share": 0.02,
        "stage1_hard_gates": [
            "finite",
            "positive_net_after_cost",
            "positive_1_5x_cost_stress",
            "minimum_20_opportunities",
            "best_positive_event_share_lte_0_40",
        ],
        "exact_soft_transfer_policy": (
            "positive pooled development economics, at least one supportive fold, "
            "no catastrophic weak period, account MLL proxy safe"
        ),
        "engineering_task_sha256": engineering_task_sha256,
        "contract_map_sha256": contract_map_sha256,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "paper_shadow_ready_allowed": False,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    writer.write_json("turbo_preregistration.json", preregistration)
    access = _record_access_once(batch_index) if record_data_access else None
    resource_before = capture_resource_snapshot()
    cache_root = _canonical_cache_root()
    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=cache_root,
        contract_map_path=map_path,
    )
    features_ready_at = time.perf_counter()
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in feature_build.market_paths.items()
    }
    historical_fingerprints = _historical_turbo_fingerprints()
    specifications = generate_turbo_population(
        matrices,
        count=proposal_count,
        batch_index=batch_index,
        random_seed=random_seed,
        excluded_fingerprints=historical_fingerprints,
    )
    deduplicated = deduplicate_specs(specifications)
    if len(deduplicated.specs) != proposal_count:
        raise TurboFoundryError(
            f"Frozen population deduplicated to {len(deduplicated.specs)}, expected {proposal_count}."
        )
    specifications = list(deduplicated.specs)
    population_hash = _stable_hash(
        [(spec.candidate_id, structural_fingerprint(spec)) for spec in specifications]
    )
    writer.write_json(
        "turbo_population_manifest.json",
        {
            "schema": POPULATION_VERSION,
            "population_hash": population_hash,
            "batch_index": batch_index,
            "structural_proposals": len(specifications),
            "duplicates_rejected": len(deduplicated.duplicate_indices),
            "historical_fingerprints_excluded": len(historical_fingerprints),
            "tombstones_rejected": len(deduplicated.tombstoned_indices),
            "quality_diversity": _population_coverage(specifications),
            "specifications": [spec_to_dict(spec) for spec in specifications],
        },
    )
    generation_ready_at = time.perf_counter()
    stage1_rows: list[dict[str, Any]] = []
    stage1_benchmarks: list[dict[str, Any]] = []
    for market in MARKET_PAIRS:
        market_specs = [spec for spec in specifications if spec.market == market]
        matrix = _stage1_event_matrix(matrices[market])
        compiled = compile_strategy_batch(
            market_specs, matrix.feature_names, matrix.holding_horizons
        )
        if batch_index == 0 and market_specs:
            benchmark_specs = market_specs[: min(120, len(market_specs))]
            benchmark_matrix = _slice_event_matrix(matrix, min(2_500, matrix.event_count))
            benchmark = benchmark_stage1(
                compile_strategy_batch(
                    benchmark_specs,
                    benchmark_matrix.feature_names,
                    benchmark_matrix.holding_horizons,
                ),
                benchmark_matrix,
                repeats=2,
                micro_batch_size=32,
            )
            stage1_benchmarks.append(asdict(benchmark))
        result = execute_stage1_vectorized(compiled, matrix, micro_batch_size=32)
        for index, spec in enumerate(market_specs):
            opportunities = int(result.opportunity_count[index])
            gross = float(result.gross_pnl[index])
            net = float(result.net_pnl[index])
            stress = gross - 1.5 * spec.round_turn_cost * spec.quantity * opportunities
            finite = bool(
                np.isfinite(
                    [
                        gross,
                        net,
                        stress,
                        result.approximate_max_drawdown[index],
                        result.best_positive_event_share[index],
                    ]
                ).all()
            )
            passed = bool(
                finite
                and opportunities >= 20
                and net > 0
                and stress > 0
                and float(result.best_positive_event_share[index]) <= 0.40
            )
            stage1_rows.append(
                {
                    "candidate_id": spec.candidate_id,
                    "fingerprint": structural_fingerprint(spec),
                    "market": spec.market,
                    "family": spec.family,
                    "role": spec.role.name,
                    "timeframe": spec.timeframe,
                    "events": opportunities,
                    "gross_pnl": gross,
                    "net_pnl": net,
                    "cost_stress_1_5x_net": stress,
                    "maximum_drawdown": float(result.approximate_max_drawdown[index]),
                    "best_positive_event_share": float(result.best_positive_event_share[index]),
                    "first_half_net_pnl": float(result.first_half_net_pnl[index]),
                    "second_half_net_pnl": float(result.second_half_net_pnl[index]),
                    "stage1_pass": passed,
                    "disposition": "EXACT_REPLAY_ELIGIBLE" if passed else _stage1_failure(
                        finite=finite,
                        events=opportunities,
                        net=net,
                        stress=stress,
                        concentration=float(result.best_positive_event_share[index]),
                    ),
                }
            )
    stage1_finished_at = time.perf_counter()
    writer.write_jsonl_batch("turbo_stage1_results.jsonl", stage1_rows)
    spec_by_id = {spec.candidate_id: spec for spec in specifications}
    survivors = [row for row in stage1_rows if row["stage1_pass"]]
    meta_model, meta_report = _fit_registry_meta_screen()
    exact_rows = _select_exact_replays(
        survivors,
        spec_by_id=spec_by_id,
        fitted_meta=meta_model,
        limit=EXACT_REPLAY_LIMIT,
        seed=random_seed + batch_index,
    )
    exact_specs = [spec_by_id[str(row["candidate_id"])] for row in exact_rows]
    exact_benchmark = None
    if batch_index == 0 and exact_specs:
        first_market = exact_specs[0].market
        benchmark_specs = [spec for spec in exact_specs if spec.market == first_market][:12]
        if benchmark_specs:
            exact_benchmark = asdict(
                benchmark_exact_replay(
                    benchmark_specs,
                    matrices[first_market],
                    repeats=1,
                )
            )
    tasks = [
        PureTask(
            task_id=spec.candidate_id,
            payload={
                "matrix_path": feature_build.market_paths[spec.market],
                "specification": spec_to_dict(spec),
            },
        )
        for spec in exact_specs
    ]
    exact_started = time.perf_counter()
    with LongLivedWorkerPool(max_workers=worker_count) as pool:
        task_results = pool.run_batch(run_exact_replay_job, tasks)
    exact_wall = max(time.perf_counter() - exact_started, 1e-9)
    resource_metrics = summarize_task_resources(
        task_results,
        batch_wall_seconds=exact_wall,
        worker_count_budget=worker_count,
        scheduler_idle_seconds=0.0,
    )
    failures = [row for row in task_results if not row.succeeded]
    if failures:
        raise TurboFoundryError(
            f"Exact worker failures: {[(row.task_id, row.error_type) for row in failures[:3]]}"
        )
    exact_results = [dict(row.value or {}) for row in task_results]
    writer.write_jsonl_batch("turbo_exact_results.jsonl", exact_results)
    candidates = _classify_exact_candidates(exact_results, spec_by_id)
    promising = [
        row for row in candidates if row["status"] == "PROMISING_RESEARCH_CANDIDATE"
    ]
    promotion_candidates = _promotion_queue(promising, PROMOTION_QUEUE_LIMIT)
    backpressure = assess_backpressure(
        {
            "discovery_queue": 0,
            "exact_replay_queue": len(exact_specs),
            "promotion_queue": len(promotion_candidates),
            "writer_queue": 0,
        },
        limits=BackpressureLimits(),
    )
    resource_after = capture_resource_snapshot()
    stage1_speedup = min(
        (float(row["speedup"]) for row in stage1_benchmarks), default=0.0
    )
    exact_speedup = float((exact_benchmark or {}).get("speedup") or 0.0)
    total_seconds = time.perf_counter() - started
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": (
            "TURBO_V2_PROMOTION_CANDIDATES_FOUND"
            if promotion_candidates
            else "TURBO_V2_BATCH_COMPLETED_NO_PROMOTION_YET"
        ),
        "interpretation_boundary": (
            "Turbo Stage-1 is an approximate development screen. Exact 2024 Q1-Q3 replay "
            "does not include candidate-level nulls, protected Q4 or PAPER_SHADOW_READY admission."
        ),
        "batch_index": batch_index,
        "candidate_count": len(specifications),
        "requested_prototypes": proposal_count,
        "structural_prototypes": len(specifications),
        "stage0_valid": len(specifications),
        "duplicates_rejected": len(deduplicated.duplicate_indices),
        "stage1_screened": len(stage1_rows),
        "stage1_survivors": len(survivors),
        "exact_replays": len(exact_results),
        "promotion_candidates_queued": len(promotion_candidates),
        "promising_candidates": len(promising),
        "shadow_candidates": 0,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": 0,
        "candidates": candidates,
        "promotion_candidate_ids": [row["candidate_id"] for row in promotion_candidates],
        "power_plan": power_plan.to_dict(),
        "adaptive_batch_plan": adaptive.to_dict(),
        "quality_diversity": _population_coverage(specifications),
        "meta_screen": meta_report,
        "backpressure": asdict(backpressure),
        "feature_store": {
            "schema": "hydra_canonical_feature_store_v2",
            "cache_hits": feature_build.cache_hits,
            "cache_misses": feature_build.cache_misses,
            "rows": feature_build.rows,
            "source_fingerprint": feature_build.source_fingerprint,
            "markets": sorted(feature_build.market_paths),
            "timeframes": ["1m", "5m", "15m", "30m", "60m", "session", "daily"],
        },
        "performance": {
            "feature_preparation_seconds": feature_build.seconds,
            "generation_seconds": generation_ready_at - features_ready_at,
            "stage1_seconds": stage1_finished_at - generation_ready_at,
            "exact_replay_wall_seconds": exact_wall,
            "serialization_and_finalize_seconds": time.perf_counter() - stage1_finished_at - exact_wall,
            "total_seconds": total_seconds,
            "stage1_candidates_per_second": len(stage1_rows)
            / max(stage1_finished_at - generation_ready_at, 1e-9),
            "exact_candidates_per_second": len(exact_results) / exact_wall,
            "stage1_reference_benchmarks": stage1_benchmarks,
            "stage1_minimum_speedup": stage1_speedup,
            "exact_reference_benchmark": exact_benchmark,
            "exact_speedup": exact_speedup,
            "worker_resources": resource_metrics.to_dict(),
            "resource_before": resource_before.to_dict(),
            "resource_after": resource_after.to_dict(),
        },
        "integrity": {
            "deterministic_population": len({structural_fingerprint(spec) for spec in specifications})
            == len(specifications),
            "vectorized_reference_outputs_identical": all(
                bool(row["outputs_identical"]) for row in stage1_benchmarks
            ),
            "exact_reference_outputs_identical": bool(
                not exact_benchmark or exact_benchmark["outputs_identical"]
            ),
            "single_result_writer": True,
            "workers_write_free": sum(
                row.shared_state_writes for row in task_results
            )
            == 0,
            "q4_excluded": True,
            "no_status_inheritance": True,
            "no_outbound_order_capability": True,
        },
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": VALIDATION_END,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
            "data_access_record": access,
        },
        "next_recommended_action": (
            "RUN_TURBO_PROMOTION_IN_PARALLEL_WITH_NEXT_DISCOVERY_EPOCH"
            if promotion_candidates
            else "RUN_NEXT_POWER_AWARE_TURBO_DISCOVERY_EPOCH"
        ),
    }
    if not all(bool(value) for value in payload["integrity"].values()):
        raise TurboFoundryError(f"Turbo integrity proof failed: {payload['integrity']}")
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    writer.write_json("turbo_result.json", payload)
    report = _render_report(payload)
    writer.write_text("turbo_report.md", report)
    return {
        **payload,
        "artifacts": {
            "preregistration_path": str(destination / "turbo_preregistration.json"),
            "population_manifest_path": str(destination / "turbo_population_manifest.json"),
            "stage1_results_path": str(destination / "turbo_stage1_results.jsonl"),
            "exact_results_path": str(destination / "turbo_exact_results.jsonl"),
            "result_path": str(destination / "turbo_result.json"),
            "report_path": str(destination / "turbo_report.md"),
        },
        "report_path": str(destination / "turbo_report.md"),
    }


def generate_turbo_population(
    matrices: Mapping[str, FeatureMatrix],
    *,
    count: int,
    batch_index: int,
    random_seed: int,
    excluded_fingerprints: Iterable[str] = (),
) -> list[StrategySpec]:
    if count < 1:
        raise ValueError("Turbo population count must be positive.")
    possibilities: list[StrategySpec] = []
    for market in MARKET_PAIRS:
        if market not in matrices:
            continue
        matrix = matrices[market]
        calibration = _calibration_mask(matrix)
        ecology = _ecology(market)
        for feature in FEATURES:
            values = matrix.array(f"feature__{feature}")[calibration]
            finite = values[np.isfinite(values)]
            if len(finite) < 500:
                continue
            for quantile in QUANTILES:
                thresholds = (
                    (
                        ComparisonOperator.GREATER_EQUAL,
                        float(np.quantile(finite, quantile)),
                    ),
                    (
                        ComparisonOperator.LESS_EQUAL,
                        float(np.quantile(finite, 1.0 - quantile)),
                    ),
                )
                for operator, threshold in thresholds:
                    for side in (-1, 1):
                        for horizon in HORIZONS:
                            for session_code in (-1, 0, 1, 2):
                                for context_feature, context_operator, context_threshold, timeframe in CONTEXTS:
                                    role = _role(feature, horizon, context_feature)
                                    provisional = StrategySpec(
                                        candidate_id="provisional",
                                        lineage_id="provisional",
                                        family=FEATURE_FAMILIES[feature],
                                        market=market,
                                        timeframe=timeframe,
                                        feature=feature,
                                        operator=operator,
                                        threshold=threshold,
                                        side=side,
                                        holding_events=horizon,
                                        point_value=instrument_spec(market).point_value,
                                        round_turn_cost=_round_turn_cost_all(market),
                                        role=role,
                                        context_feature=context_feature,
                                        context_operator=context_operator,
                                        context_threshold=context_threshold,
                                        session_code=session_code,
                                    )
                                    fingerprint = structural_fingerprint(provisional)
                                    lineage = _stable_hash(
                                        {
                                            "market": market,
                                            "feature": feature,
                                            "operator": int(operator),
                                            "side": side,
                                            "context": context_feature,
                                            "context_operator": int(context_operator)
                                            if context_operator
                                            else None,
                                        }
                                    )
                                    possibilities.append(
                                        replace(
                                            provisional,
                                            candidate_id=f"strategy_turbo_{fingerprint[:24]}_v1",
                                            lineage_id=f"lineage_turbo_{lineage[:24]}",
                                        )
                                    )
    unique = list(
        deduplicate_specs(possibilities, tombstones=excluded_fingerprints).specs
    )
    unique.sort(
        key=lambda spec: hashlib.sha256(
            f"{random_seed}:{batch_index}:{structural_fingerprint(spec)}".encode()
        ).hexdigest()
    )
    return _quality_diversity_cap(unique, count=count)


def _quality_diversity_cap(
    specs: Sequence[StrategySpec], *, count: int
) -> list[StrategySpec]:
    available_ecologies = sorted({_ecology(spec.market) for spec in specs})
    if not available_ecologies:
        raise TurboFoundryError("No ecology has sufficient calibration observations.")
    if set(available_ecologies) == {"equity_indices", "metals", "energy"}:
        ecology_caps = {
            "equity_indices": math.floor(count * 0.40),
            "metals": math.floor(count * 0.30),
            "energy": count - math.floor(count * 0.40) - math.floor(count * 0.30),
        }
    else:
        # Missing calibration data must not make selection mathematically
        # impossible. Redistribute before Stage-1; never use future economics
        # to choose the relaxed allocation.
        base = count // len(available_ecologies)
        remainder = count - base * len(available_ecologies)
        ecology_caps = {
            ecology: base + int(index < remainder)
            for index, ecology in enumerate(available_ecologies)
        }
    family_cap = max(1, math.floor(count * 0.25))
    lineage_cap = max(1, math.floor(count * 0.02))
    selected: list[StrategySpec] = []
    ecologies: Counter[str] = Counter()
    families: Counter[str] = Counter()
    lineages: Counter[str] = Counter()
    for spec in specs:
        ecology = _ecology(spec.market)
        if ecologies[ecology] >= ecology_caps[ecology]:
            continue
        if families[spec.family] >= family_cap:
            continue
        if lineages[spec.lineage_id] >= lineage_cap:
            continue
        selected.append(spec)
        ecologies[ecology] += 1
        families[spec.family] += 1
        lineages[spec.lineage_id] += 1
        if len(selected) == count:
            return selected
    raise TurboFoundryError(
        f"Quality-diversity caps yielded {len(selected)} structures, below {count}."
    )


def _stage1_event_matrix(matrix: FeatureMatrix) -> EventMatrix:
    day = matrix.array("session_day")
    session = matrix.array("session_code")
    selected = (
        (day >= _day(STAGE1_START))
        & (day < _day(STAGE1_END))
        & (session >= 0)
    )
    names = feature_names_for_bundle()
    features = np.column_stack(
        [matrix.array(f"feature__{name}")[selected] for name in names]
    )
    forward = np.vstack(
        [matrix.array(f"forward_move__{horizon}")[selected] for horizon in HORIZONS]
    )
    return EventMatrix.from_arrays(
        feature_names=names,
        holding_horizons=HORIZONS,
        features=features,
        forward_moves=forward,
        decision_ns=matrix.array("decision_ns")[selected],
        availability_ns=matrix.array("availability_ns")[selected],
        session_codes=session[selected],
    )


def _slice_event_matrix(matrix: EventMatrix, count: int) -> EventMatrix:
    selected = slice(0, count)
    return EventMatrix.from_arrays(
        feature_names=matrix.feature_names,
        holding_horizons=matrix.holding_horizons,
        features=matrix.features[selected],
        forward_moves=matrix.forward_moves[:, selected],
        decision_ns=matrix.decision_ns[selected],
        availability_ns=matrix.availability_ns[selected],
        session_codes=matrix.session_codes[selected],
    )


def _select_exact_replays(
    survivors: list[dict[str, Any]],
    *,
    spec_by_id: Mapping[str, StrategySpec],
    fitted_meta: Any,
    limit: int,
    seed: int,
) -> list[dict[str, Any]]:
    if not survivors:
        return []
    capacity = min(limit, len(survivors))
    quality_sorted = sorted(
        survivors,
        key=lambda row: (
            -float(row["net_pnl"]) / max(float(row["maximum_drawdown"]), 100.0),
            str(row["candidate_id"]),
        ),
    )
    universe = quality_sorted[: min(len(quality_sorted), max(capacity * 3, capacity))]
    if fitted_meta is None or len(universe) == capacity:
        return universe[:capacity]
    meta_rows = [
        {"candidate_id": row["candidate_id"], **_new_meta_features(spec_by_id[row["candidate_id"]])}
        for row in universe
    ]
    allocation = prioritize_with_exploration(
        meta_rows,
        fitted=fitted_meta,
        capacity=capacity,
        exploration_share=0.20,
        seed=seed,
    )
    selected = {row.candidate_id: row for row in allocation.selected}
    output = [row for row in universe if row["candidate_id"] in selected]
    for row in output:
        decision = selected[str(row["candidate_id"])]
        row["meta_lane"] = decision.lane
        row["meta_probability"] = decision.predicted_stage1_probability
    return sorted(output, key=lambda row: selected[str(row["candidate_id"])].rank)


def _classify_exact_candidates(
    results: Sequence[Mapping[str, Any]],
    specs: Mapping[str, StrategySpec],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for raw in results:
        row = dict(raw)
        spec = specs[str(row["candidate_id"])]
        promising = bool(
            not row["hard_invalidation"]
            and row["finite"]
            and int(row["events"]) >= 15
            and float(row["net_pnl"]) > 0
            and float(row["cost_stress_1_5x_net"]) > 0
            and int(row["supportive_temporal_folds"]) >= 1
            and not bool(row["catastrophic_transfer"])
            and float(row["best_positive_event_share"]) <= 0.40
            and bool(row["mll_proxy_safe"])
        )
        status = (
            "PROMISING_RESEARCH_CANDIDATE"
            if promising
            else "RAW_ECONOMIC_SIGNAL"
            if float(row["net_pnl"]) > 0 and not row["hard_invalidation"]
            else "RESEARCH_PROTOTYPE"
        )
        candidates.append(
            {
                "candidate_id": spec.candidate_id,
                "lineage_id": spec.lineage_id,
                "status": status,
                "mechanism_family": spec.family,
                "primary_market": spec.market,
                "execution_market": MARKET_PAIRS[spec.market],
                "role": spec.role.name,
                "objective_pool": _objective_pool(spec.role),
                "timeframe": spec.timeframe,
                "structural_fingerprint": structural_fingerprint(spec),
                "net_pnl": float(row["net_pnl"]),
                "events": int(row["events"]),
                "supportive_temporal_folds": int(row["supportive_temporal_folds"]),
                "fold_results": row["fold_results"],
                "cost_stress_1_5x_net": float(row["cost_stress_1_5x_net"]),
                "maximum_drawdown": float(row["maximum_drawdown"]),
                "best_positive_event_share": float(row["best_positive_event_share"]),
                "one_bar_delay_net_pnl": float(row["one_bar_delay_net_pnl"]),
                "hard_invalidation": bool(row["hard_invalidation"]),
                "topstep": {"path_candidate": False, "not_run_before_promotion": True},
                "evidence_boundary": "development_exact_replay_no_candidate_null_or_q4",
                "specification": spec_to_dict(spec),
            }
        )
    return candidates


def _promotion_queue(
    candidates: Sequence[dict[str, Any]], limit: int
) -> list[dict[str, Any]]:
    niche: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in candidates:
        key = (
            _ecology(str(row["primary_market"])),
            str(row["mechanism_family"]),
            str(row["role"]),
            str(row["timeframe"]),
        )
        current = niche.get(key)
        quality = float(row["net_pnl"]) / max(float(row["maximum_drawdown"]), 100.0)
        if current is None or quality > float(current["_quality"]):
            niche[key] = {**row, "_quality": quality}
    ranked = sorted(niche.values(), key=lambda row: (-float(row["_quality"]), row["candidate_id"]))
    return [{key: value for key, value in row.items() if key != "_quality"} for row in ranked[:limit]]


def _fit_registry_meta_screen() -> tuple[Any, dict[str, Any]]:
    path = project_path("registry", "hydra_registry.db")
    if not path.is_file():
        path = Path("/root/hydra-bot/registry/hydra_registry.db")
    if not path.is_file():
        return None, {"status": "COLD_START_NO_REGISTRY", "strategy_evidence": False}
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        rows = conn.execute(
            "SELECT candidate_id,family,symbol,timeframe,parameters_json,risk_json,"
            "parent_candidate_id,validation_status,created_at FROM candidates "
            "ORDER BY created_at,candidate_id"
        ).fetchall()
        conn.close()
        training = []
        success = {
            "ECONOMICALLY_VIABLE",
            "TOPSTEP_VIABLE",
            "TOPSTEP_NEAR_MISS",
            "TOPSTEP_COMBINE_PASSED",
        }
        for row in rows:
            parameters = _json_dict(row[4])
            risk = _json_dict(row[5])
            training.append(
                {
                    "candidate_id": str(row[0]),
                    **_generic_meta_features(
                        family=str(row[1]),
                        market=str(row[2]),
                        timeframe=str(row[3]),
                        parameters=parameters,
                        risk=risk,
                        is_mutation=row[6] is not None,
                    ),
                    "stage1_success": int(str(row[7]) in success),
                }
            )
        fitted = fit_temporal_meta_screen(
            training,
            feature_names=META_FEATURES,
            minimum_rows=200,
        )
        report = fitted.report()
        report["registry_rows"] = len(training)
        if (
            fitted.oos_roc_auc is None
            or fitted.oos_recall_at_half_budget < 0.80
        ):
            report["status"] = "TRAINED_BUT_NOT_DECISION_USEFUL"
            report["allocation_enabled"] = False
            report["fallback"] = "STAGE1_QUALITY_PLUS_100_PERCENT_EXPLORATION"
            return None, report
        report["status"] = "TRAINED_REGISTRY_OOS_ALLOCATION_ONLY"
        report["allocation_enabled"] = True
        return fitted, report
    except (sqlite3.Error, MetaScreenError, ValueError, TypeError) as exc:
        return None, {
            "status": "COLD_START_META_UNAVAILABLE",
            "reason": f"{type(exc).__name__}:{exc}",
            "strategy_evidence": False,
            "may_validate_or_promote": False,
            "exploration_share": 1.0,
        }


def _new_meta_features(spec: StrategySpec) -> dict[str, float]:
    parameters = {
        "threshold": spec.threshold,
        "holding_events": spec.holding_events,
        "session_code": spec.session_code,
        "context_threshold": spec.context_threshold,
    }
    return _generic_meta_features(
        family=spec.family,
        market=spec.market,
        timeframe=spec.timeframe,
        parameters=parameters,
        risk={"quantity": spec.quantity},
        is_mutation=False,
    )


def _generic_meta_features(
    *,
    family: str,
    market: str,
    timeframe: str,
    parameters: Mapping[str, Any],
    risk: Mapping[str, Any],
    is_mutation: bool,
) -> dict[str, float]:
    numeric = [
        abs(float(value))
        for value in parameters.values()
        if isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    return {
        "family_code": _hash_unit(family),
        "market_code": _hash_unit(market),
        "timeframe_code": _hash_unit(timeframe),
        "parameter_count": float(len(parameters)),
        "numeric_parameter_scale": float(np.mean(numeric)) if numeric else 0.0,
        "risk_parameter_count": float(len(risk)),
        "is_mutation": float(bool(is_mutation)),
    }


def _population_coverage(specs: Sequence[StrategySpec]) -> dict[str, Any]:
    return {
        "market_ecologies": dict(Counter(_ecology(spec.market) for spec in specs)),
        "markets": dict(Counter(spec.market for spec in specs)),
        "mechanism_families": dict(Counter(spec.family for spec in specs)),
        "timeframes": dict(Counter(spec.timeframe for spec in specs)),
        "roles": dict(Counter(spec.role.name for spec in specs)),
        "lineages": len({spec.lineage_id for spec in specs}),
        "maximum_family_share": max(Counter(spec.family for spec in specs).values(), default=0)
        / max(len(specs), 1),
        "maximum_ecology_share": max(Counter(_ecology(spec.market) for spec in specs).values(), default=0)
        / max(len(specs), 1),
        "ecology_cap_relaxed_for_feasibility": bool(
            len({_ecology(spec.market) for spec in specs}) < 3
        ),
        "missing_ecologies": sorted(
            {"equity_indices", "metals", "energy"}
            - {_ecology(spec.market) for spec in specs}
        ),
        "maximum_lineage_share": max(Counter(spec.lineage_id for spec in specs).values(), default=0)
        / max(len(specs), 1),
    }


def _calibration_mask(matrix: FeatureMatrix) -> np.ndarray:
    day = matrix.array("session_day")
    return (
        (day >= _day(CALIBRATION_START))
        & (day < _day(CALIBRATION_END))
        & (matrix.array("session_code") >= 0)
    )


def _stage1_failure(
    *, finite: bool, events: int, net: float, stress: float, concentration: float
) -> str:
    if not finite:
        return "HARD_NONFINITE"
    if events < 20:
        return "INSUFFICIENT_SAMPLES"
    if net <= 0:
        return "NEGATIVE_ECONOMICS"
    if stress <= 0:
        return "COST_FRAGILITY"
    if concentration > 0.40:
        return "CONCENTRATION"
    return "SOFT_SCREEN_FAILURE"


def _role(feature: str, horizon: int, context: str | None) -> StrategyRole:
    if feature == "shared_loss_risk_state" or (
        context and "volatility_expansion" in context
    ):
        return StrategyRole.DEFENSIVE
    if horizon <= 15:
        return StrategyRole.COMBINE_PASSER
    return StrategyRole.XFA_PAYOUT


def _objective_pool(role: StrategyRole) -> str:
    if role == StrategyRole.COMBINE_PASSER:
        return "COMBINE_PASSER_POOL"
    if role == StrategyRole.XFA_PAYOUT:
        return "XFA_PAYOUT_POOL"
    return "DEFENSIVE_ACCOUNT_POOL"


def _ecology(market: str) -> str:
    if market in {"ES", "NQ", "RTY", "YM"}:
        return "equity_indices"
    return "metals" if market == "GC" else "energy"


def _day(value: str) -> int:
    return int(np.datetime64(value, "D").astype(np.int64))


def _hash_unit(value: str) -> float:
    integer = int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:13], 16)
    return integer / float(16**13 - 1)


def _json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _record_access_once(batch_index: int) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = (
        f"Turbo Foundry V2 epoch {batch_index}: calibration, Stage-1 and exact "
        "development replay; Q4 excluded"
    )
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.is_file():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.turbo_foundry_v2"
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.turbo_foundry_v2",
        [f"turbo_epoch_{batch_index}_frozen_population"],
        reason,
        None,
    )
    return record.__dict__


def _canonical_cache_root() -> Path:
    configured = os.environ.get("HYDRA_CANONICAL_FEATURE_CACHE")
    if configured:
        return Path(configured).expanduser().resolve()
    mission_root = Path("/root/hydra-bot/data/cache")
    if mission_root.is_dir():
        return mission_root / "turbo_foundry_v2"
    return project_path("data", "cache", "turbo_foundry_v2")


def _historical_turbo_fingerprints() -> set[str]:
    roots = [
        project_path("reports", "mission_experiments"),
        Path("/root/hydra-bot/reports/mission_experiments"),
    ]
    fingerprints: set[str] = set()
    seen_paths: set[Path] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.glob("turbo_foundry_v2_epoch_*/turbo_population_manifest.json"):
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
                    fingerprints.add(structural_fingerprint(_spec_from_manifest(row)))
                except (KeyError, TypeError, ValueError):
                    continue
    return fingerprints


def _spec_from_manifest(payload: Mapping[str, Any]) -> StrategySpec:
    from hydra.research.turbo_exact_replay import spec_from_dict

    return spec_from_dict(payload)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _sha256(path) != expected:
        raise TurboFoundryError(f"Frozen {label} is missing or changed: {path}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _render_report(payload: Mapping[str, Any]) -> str:
    performance = dict(payload["performance"])
    return "\n".join(
        [
            "# HYDRA Turbo Foundry V2 — epoch report",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Structural proposals / Stage-0 valid: `{payload['structural_prototypes']}` / `{payload['stage0_valid']}`",
            f"- Stage-1 survivors: `{payload['stage1_survivors']}`",
            f"- Exact replays / promotion queue: `{payload['exact_replays']}` / `{payload['promotion_candidates_queued']}`",
            f"- Promising candidates: `{payload['promising_candidates']}`",
            f"- Stage-1 throughput: `{performance['stage1_candidates_per_second']:.2f}` candidates/s",
            f"- Stage-1 scalar/vector speedup: `{performance['stage1_minimum_speedup']:.2f}x`",
            f"- Exact scalar/vector speedup: `{performance['exact_speedup']:.2f}x`",
            f"- Worker utilization: `{performance['worker_resources']['aggregate_worker_utilization_pct']:.1f}%`",
            "- Q4 access: `0`",
            "- PAPER_SHADOW_READY: `0`",
            "- Outbound order capability: `false`",
            "",
            "## Interpretation boundary",
            "",
            str(payload["interpretation_boundary"]),
            "",
        ]
    )
