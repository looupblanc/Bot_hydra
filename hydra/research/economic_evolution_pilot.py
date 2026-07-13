from __future__ import annotations

import hashlib
import json
import os
import resource
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.account_policy.schema import stable_hash
from hydra.compute.resource_monitor import capture_resource_snapshot
from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_evaluation import (
    AccountEvaluationResult,
    ExactSleeveRuntime,
    UnsupportedExactExecution,
    build_exact_sleeve_runtime,
    compile_account_policy,
    evaluate_compiled_account_policy,
    matched_observations_from_evaluation,
)
from hydra.economic_evolution.archive import (
    ArchiveEntry,
    BehavioralDescriptor,
    ParetoObjectives,
    ParetoQualityDiversityArchive,
)
from hydra.economic_evolution.assembly import (
    AssemblyInput,
    generate_account_policy_population,
)
from hydra.economic_evolution.failure_model import derive_failure_vector
from hydra.economic_evolution.generator import generate_structural_population
from hydra.economic_evolution.incremental_value import (
    IncrementalValuePolicy,
    evaluate_incremental_value,
)
from hydra.economic_evolution.mutation import propose_directed_mutation
from hydra.economic_evolution.null_calibration import calibrate_incremental_validator
from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    EconomicRole,
    SleeveSpec,
    deterministic_id,
)
from hydra.economic_evolution.screen import (
    BoundSleeve,
    CheapScreenPolicy,
    bind_sleeves_to_calibration,
    run_ultra_cheap_screen,
)
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles


ENGINE_VERSION = "hydra_economic_evolution_engine_v1"


class EconomicEvolutionPilotError(RuntimeError):
    pass


def run_economic_evolution_pilot(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Run the preregistered, development-only economic-evolution pilot."""

    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    resource_before = capture_resource_snapshot()
    prereg_path = Path(preregistration_path).resolve()
    prereg, prereg_source = _load_preregistration(prereg_path)
    _validate_preregistration(prereg, prereg_path)
    writer = AtomicResultWriter(output_dir)
    writer.write_json("preregistration_copy.json", prereg)
    writer.write_json("preregistration_source.json", prereg_source)

    incremental_policy = _incremental_policy(prereg)
    calibration = calibrate_incremental_validator(
        incremental_policy,
        seed=int(prereg["validator_calibration"]["seed"]),
        repetitions=int(prereg["validator_calibration"]["repetitions"]),
        starts_per_block=int(
            prereg["validator_calibration"]["starts_per_block"]
        ),
        noise_scale=float(prereg["validator_calibration"]["noise_scale"]),
    )
    writer.write_json("validator_null_calibration.json", calibration.to_dict())
    if calibration.null_false_positive_rate > float(
        prereg["validator_calibration"]["maximum_false_positive_rate"]
    ) or calibration.meaningful_effect_power < float(
        prereg["validator_calibration"]["minimum_meaningful_effect_power"]
    ):
        raise EconomicEvolutionPilotError(
            "fixed incremental validator failed preregistered control calibration"
        )

    timings: dict[str, float] = {}
    stage_start = time.perf_counter()
    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=cache_root,
        contract_map_path=contract_map_path,
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in feature_build.market_paths.items()
    }
    _verify_data_fingerprint(
        prereg,
        feature_build.source_fingerprint,
        contract_map_path,
        feature_build.market_paths,
    )
    timings["feature_open_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    generated = generate_structural_population(
        campaign_id=str(prereg["campaign_id"]),
        raw_proposal_count=int(prereg["funnel"]["raw_proposals"]),
    )
    if generated.candidate_manifest_hash != str(
        prereg["structural_population"]["candidate_manifest_hash"]
    ):
        raise EconomicEvolutionPilotError("frozen structural population drift")
    writer.write_json("structural_population_summary.json", generated.summary())
    writer.write_jsonl_batch(
        "structural_sleeves.jsonl", [row.to_dict() for row in generated.sleeves]
    )
    timings["generation_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    screen = run_ultra_cheap_screen(
        generated.sleeves, matrices, policy=screen_policy
    )
    writer.write_json("cheap_screen_summary.json", screen.summary())
    writer.write_jsonl_batch("cheap_screen_results.jsonl", list(screen.rows))
    timings["cheap_screen_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    sleeve_by_id = {row.sleeve_id: row for row in generated.sleeves}
    exact_selection = _quality_diverse_exact_selection(
        screen.rows,
        sleeve_by_id,
        limit=int(prereg["funnel"]["maximum_exact_component_replays"]),
    )
    bound = _bind_selected(exact_selection, matrices, policy=screen_policy)
    exact_runtimes, exact_failures = _build_exact_runtimes(
        bound,
        matrices,
        start_inclusive=str(prereg["exact_replay_period"][0]),
        end_exclusive=str(prereg["exact_replay_period"][1]),
        worker_count=int(prereg["compute"]["exact_worker_count"]),
    )
    exact_rows = [_runtime_row(row) for row in exact_runtimes.values()]
    exact_rows.sort(key=lambda row: str(row["sleeve_id"]))
    writer.write_jsonl_batch("exact_component_results.jsonl", exact_rows)
    writer.write_json("exact_component_failures.json", exact_failures)
    timings["exact_component_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    exact_eligible = _exact_component_eligible(
        exact_runtimes, prereg["incremental_value_policy"]
    )
    incremental_rows, incremental_evaluations = _run_incremental_tournament(
        exact_eligible,
        sleeve_by_id,
        prereg=prereg,
    )
    writer.write_jsonl_batch("incremental_value_results.jsonl", incremental_rows)
    timings["incremental_value_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    component_bank = _build_component_bank(
        exact_eligible,
        incremental_rows,
        sleeve_by_id,
        maximum_components=int(prereg["funnel"]["maximum_component_bank"]),
    )
    writer.write_json(
        "component_bank.json",
        {
            "component_count": len(component_bank),
            "micro_edge_useful_count": sum(
                row["incremental_status"] == "MICRO_EDGE_USEFUL"
                for row in component_bank
            ),
            "validated_component_count": 0,
            "components": component_bank,
        },
    )
    bank_inputs = _assembly_inputs(component_bank, sleeve_by_id, exact_runtimes)
    assembly = generate_account_policy_population(
        bank_inputs,
        campaign_id=f"{prereg['campaign_id']}::assembly",
        count=int(prereg["funnel"]["structural_account_policies"]),
    )
    executable_policies = [
        row for row in assembly.policies if row.conflict_policy == "FIXED_PRIORITY"
    ][: int(prereg["funnel"]["exact_account_policy_evaluations"])]
    writer.write_json(
        "account_policy_population.json",
        {
            **assembly.summary(),
            "exact_executable_fixed_priority_count": len(executable_policies),
            "policies": [row.to_dict() for row in assembly.policies],
        },
    )
    timings["assembly_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    account_results, archive = _evaluate_account_population(
        executable_policies,
        exact_runtimes,
        sleeve_by_id,
        prereg=prereg,
    )
    writer.write_jsonl_batch(
        "account_policy_results.jsonl",
        [row["result"] for row in account_results],
    )
    writer.write_json(
        "pareto_quality_diversity_archive.json",
        {
            "summary": archive.summary(),
            "entries": [
                {
                    "policy_id": row.policy_id,
                    "family": row.family,
                    "lineage_id": row.lineage_id,
                    "descriptor": asdict(row.descriptor),
                    "objectives": asdict(row.objectives),
                    "payload": row.payload,
                }
                for row in archive.entries
            ],
        },
    )
    timings["account_policy_development_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    rolling_results = _run_rolling_elite(
        account_results,
        exact_runtimes,
        prereg=prereg,
    )
    writer.write_jsonl_batch(
        "rolling_combine_elite_results.jsonl",
        [row["result"] for row in rolling_results],
    )
    timings["rolling_combine_seconds"] = time.perf_counter() - stage_start

    stage_start = time.perf_counter()
    mutation_rows = _run_failure_directed_mutations(
        rolling_results,
        exact_runtimes,
        tuple(sleeve_by_id[row] for row in exact_runtimes),
        prereg=prereg,
    )
    writer.write_jsonl_batch("directed_mutation_results.jsonl", mutation_rows)
    timings["failure_directed_mutation_seconds"] = time.perf_counter() - stage_start

    resource_after = capture_resource_snapshot()
    total_wall = time.perf_counter() - started_wall
    total_cpu = time.process_time() - started_cpu
    summary = _pilot_summary(
        prereg=prereg,
        feature_build=feature_build,
        generated=generated,
        screen=screen,
        exact_rows=exact_rows,
        incremental_rows=incremental_rows,
        component_bank=component_bank,
        assembly=assembly,
        account_results=account_results,
        rolling_results=rolling_results,
        mutation_rows=mutation_rows,
        archive=archive,
        timings=timings,
        total_wall=total_wall,
        total_cpu=total_cpu,
        resource_before=resource_before.to_dict(),
        resource_after=resource_after.to_dict(),
        writer_pid=os.getpid(),
    )
    receipt = writer.write_json("economic_evolution_pilot_result.json", summary)
    summary["result_sha256"] = receipt.sha256
    return summary


def _validate_preregistration(value: Mapping[str, Any], path: Path) -> None:
    if value.get("schema") != "hydra_economic_evolution_pilot_preregistration_v1":
        raise EconomicEvolutionPilotError("unexpected pilot preregistration schema")
    expected = dict(value)
    frozen_hash = str(expected.pop("preregistration_hash"))
    if stable_hash(expected) != frozen_hash:
        raise EconomicEvolutionPilotError("pilot preregistration hash drift")
    if value.get("q4_access_allowed") is not False:
        raise EconomicEvolutionPilotError("Q4 must remain inaccessible")
    if value.get("new_data_purchase_allowed") is not False:
        raise EconomicEvolutionPilotError("new data purchase must remain disabled")
    if value.get("broker_or_orders_allowed") is not False:
        raise EconomicEvolutionPilotError("broker/order capability must remain disabled")
    if int(value["funnel"]["raw_proposals"]) < 50_000:
        raise EconomicEvolutionPilotError("pilot must propose at least 50,000 structures")
    if int(value["funnel"]["exact_account_policy_evaluations"]) < 100:
        raise EconomicEvolutionPilotError("pilot must evaluate at least 100 policies")
    if not path.is_file():
        raise EconomicEvolutionPilotError("preregistration is missing")
    project_root = path.parents[2]
    for relative, digest in value["implementation_files"].items():
        candidate = project_root / str(relative)
        if not candidate.is_file() or _sha256(candidate) != str(digest):
            raise EconomicEvolutionPilotError(
                f"frozen implementation drift: {relative}"
            )
    implementation_commit = str(value["implementation_commit"])
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
        cwd=project_root,
        check=False,
    )
    if ancestor.returncode != 0:
        raise EconomicEvolutionPilotError("implementation commit is not an ancestor")


def _load_preregistration(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema") != "hydra_economic_evolution_pilot_revision_v1":
        return raw, {
            "source_path": str(path),
            "source_sha256": _sha256(path),
            "revision": None,
        }
    revision = dict(raw)
    revision_hash = str(revision.pop("revision_hash"))
    if stable_hash(revision) != revision_hash:
        raise EconomicEvolutionPilotError("pilot revision hash drift")
    project_root = path.parents[2]
    base_path = project_root / str(raw["base_preregistration_path"])
    if _sha256(base_path) != str(raw["base_preregistration_sha256"]):
        raise EconomicEvolutionPilotError("base pilot WORM hash drift")
    effective = json.loads(base_path.read_text(encoding="utf-8"))
    base_payload = dict(effective)
    base_hash = str(base_payload.pop("preregistration_hash"))
    if stable_hash(base_payload) != base_hash:
        raise EconomicEvolutionPilotError("base pilot preregistration hash drift")
    effective["implementation_commit"] = str(raw["implementation_commit"])
    implementation_files = dict(effective["implementation_files"])
    implementation_files.update(
        {str(key): str(value) for key, value in raw["implementation_file_overrides"].items()}
    )
    effective["implementation_files"] = implementation_files
    effective["revision_provenance"] = {
        "revision_id": raw["revision_id"],
        "reason": raw["reason"],
        "base_preregistration_path": raw["base_preregistration_path"],
        "base_preregistration_sha256": raw["base_preregistration_sha256"],
        "pre_outcome_abort_path": raw["pre_outcome_abort_path"],
        "threshold_change": False,
        "population_change": False,
    }
    effective.pop("preregistration_hash", None)
    effective_hash = stable_hash(effective)
    if effective_hash != str(raw["expected_effective_preregistration_hash"]):
        raise EconomicEvolutionPilotError("effective revised pilot hash drift")
    effective["preregistration_hash"] = effective_hash
    return effective, {
        "source_path": str(path),
        "source_sha256": _sha256(path),
        "revision": raw,
        "effective_preregistration_hash": effective_hash,
    }


def _verify_data_fingerprint(
    prereg: Mapping[str, Any],
    source_fingerprint: str,
    contract_map_path: str | Path,
    market_paths: Mapping[str, str],
) -> None:
    expected = prereg["data"]
    if source_fingerprint != expected["feature_source_fingerprint"]:
        raise EconomicEvolutionPilotError("feature source fingerprint drift")
    if _sha256(Path(contract_map_path)) != expected["contract_map_sha256"]:
        raise EconomicEvolutionPilotError("contract-map checksum drift")
    for market, digest in expected["feature_manifest_sha256"].items():
        if market not in market_paths:
            raise EconomicEvolutionPilotError(f"feature matrix missing: {market}")
        if _sha256(Path(market_paths[market]) / "manifest.json") != str(digest):
            raise EconomicEvolutionPilotError(f"feature manifest drift: {market}")


def _quality_diverse_exact_selection(
    screen_rows: Sequence[Mapping[str, Any]],
    sleeves: Mapping[str, SleeveSpec],
    *,
    limit: int,
) -> tuple[SleeveSpec, ...]:
    groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = {}
    for row in screen_rows:
        sleeve = sleeves[str(row["sleeve_id"])]
        if not bool(row["cheap_screen_survivor"]) or sleeve.exit_style != "TIME_ONLY":
            continue
        key = (sleeve.market, sleeve.role.value, sleeve.trigger_feature)
        groups.setdefault(key, []).append(row)
    for values in groups.values():
        values.sort(
            key=lambda row: (
                -float(row["stressed_net_pnl"]),
                float(row["best_positive_event_share"]),
                float(row["approximate_max_drawdown"]),
                str(row["sleeve_id"]),
            )
        )
    selected: list[SleeveSpec] = []
    keys = sorted(groups)
    cursor = 0
    while len(selected) < limit and keys:
        key = keys[cursor % len(keys)]
        values = groups[key]
        if values:
            selected.append(sleeves[str(values.pop(0)["sleeve_id"])])
        if not values:
            keys.remove(key)
            cursor = 0
        else:
            cursor += 1
    return tuple(selected)


def _bind_selected(
    selected: Sequence[SleeveSpec],
    matrices: Mapping[str, FeatureMatrix],
    *,
    policy: CheapScreenPolicy,
) -> dict[str, BoundSleeve]:
    output: dict[str, BoundSleeve] = {}
    for market in sorted({row.market for row in selected}):
        rows = bind_sleeves_to_calibration(
            [row for row in selected if row.market == market],
            matrices[market],
            policy=policy,
        )
        output.update({row.sleeve.sleeve_id: row for row in rows})
    return output


def _build_exact_runtimes(
    bound: Mapping[str, BoundSleeve],
    matrices: Mapping[str, FeatureMatrix],
    *,
    start_inclusive: str,
    end_exclusive: str,
    worker_count: int,
) -> tuple[dict[str, ExactSleeveRuntime], list[dict[str, str]]]:
    output: dict[str, ExactSleeveRuntime] = {}
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(
                build_exact_sleeve_runtime,
                row,
                matrices[row.sleeve.market],
                start_inclusive=start_inclusive,
                end_exclusive=end_exclusive,
            ): sleeve_id
            for sleeve_id, row in bound.items()
        }
        for future in as_completed(futures):
            sleeve_id = futures[future]
            try:
                output[sleeve_id] = future.result()
            except (ValueError, UnsupportedExactExecution) as exc:
                failures.append(
                    {
                        "sleeve_id": sleeve_id,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
    return dict(sorted(output.items())), sorted(
        failures, key=lambda row: row["sleeve_id"]
    )


def _runtime_row(runtime: ExactSleeveRuntime) -> dict[str, Any]:
    return runtime.to_dict(include_events=False)


def _exact_component_eligible(
    runtimes: Mapping[str, ExactSleeveRuntime], policy: Mapping[str, Any]
) -> dict[str, ExactSleeveRuntime]:
    return {
        key: row
        for key, row in runtimes.items()
        if row.event_count >= int(policy["minimum_exact_events"])
        and row.net_pnl > 0.0
        and row.cost_stress_1_5x_net > 0.0
        and row.best_positive_event_share
        <= float(policy["maximum_best_positive_event_share"])
    }


def _run_incremental_tournament(
    runtimes: Mapping[str, ExactSleeveRuntime],
    sleeves: Mapping[str, SleeveSpec],
    *,
    prereg: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, AccountEvaluationResult]]:
    limit = int(prereg["funnel"]["incremental_value_evaluations"])
    ranked = sorted(
        runtimes,
        key=lambda key: (
            -runtimes[key].cost_stress_1_5x_net,
            runtimes[key].best_positive_event_share,
            key,
        ),
    )[:limit]
    if len(ranked) < 2:
        return [], {}
    common_days = _common_days([runtimes[key] for key in ranked])
    episode_policy = EpisodeStartPolicy(**prereg["incremental_episode_policy"])
    starts = select_episode_starts(common_days, policy=episode_policy)
    if len(starts) < int(prereg["incremental_value_policy"]["minimum_matched_starts"]):
        raise EconomicEvolutionPilotError("insufficient common starts for incremental tests")
    block_by_start = _block_map(starts, int(prereg["incremental_value_policy"]["minimum_independent_blocks"]))
    decision_policy = _incremental_policy(prereg)
    baseline_cache: dict[str, AccountEvaluationResult] = {}
    evaluations: dict[str, AccountEvaluationResult] = {}
    rows: list[dict[str, Any]] = []
    for candidate_id in ranked:
        anchor_id = next(
            (
                key
                for key in ranked
                if key != candidate_id
                and sleeves[key].behavioral_fingerprint
                != sleeves[candidate_id].behavioral_fingerprint
                and sleeves[key].market != sleeves[candidate_id].market
            ),
            next(key for key in ranked if key != candidate_id),
        )
        baseline = baseline_cache.get(anchor_id)
        if baseline is None:
            baseline_genome = _research_genome(
                (anchor_id,), campaign=str(prereg["campaign_id"]), label="baseline"
            )
            baseline = evaluate_compiled_account_policy(
                compile_account_policy(baseline_genome, runtimes),
                episode_policy=episode_policy,
                explicit_start_days=starts,
            )
            baseline_cache[anchor_id] = baseline
        included_genome = _research_genome(
            (anchor_id, candidate_id),
            campaign=str(prereg["campaign_id"]),
            label="add_one",
        )
        included = evaluate_compiled_account_policy(
            compile_account_policy(included_genome, runtimes),
            episode_policy=episode_policy,
            explicit_start_days=starts,
        )
        evaluations[candidate_id] = included
        result = evaluate_incremental_value(
            candidate_id,
            sleeves[candidate_id].role,
            matched_observations_from_evaluation(
                baseline, block_by_start=block_by_start
            ),
            matched_observations_from_evaluation(
                included, block_by_start=block_by_start
            ),
            policy=decision_policy,
        )
        rows.append(
            {
                **result.to_dict(),
                "anchor_component_id": anchor_id,
                "candidate_specification_hash": runtimes[candidate_id].specification_hash,
                "validated": False,
                "proof_window_consumed": False,
            }
        )
    return rows, evaluations


def _build_component_bank(
    runtimes: Mapping[str, ExactSleeveRuntime],
    incremental_rows: Sequence[Mapping[str, Any]],
    sleeves: Mapping[str, SleeveSpec],
    *,
    maximum_components: int,
) -> list[dict[str, Any]]:
    incremental = {str(row["component_id"]): row for row in incremental_rows}
    ranked = sorted(
        runtimes,
        key=lambda key: (
            incremental.get(key, {}).get("status") != "MICRO_EDGE_USEFUL",
            -runtimes[key].cost_stress_1_5x_net,
            runtimes[key].best_positive_event_share,
            key,
        ),
    )
    output: list[dict[str, Any]] = []
    seen_behavior: set[str] = set()
    for key in ranked:
        sleeve = sleeves[key]
        if sleeve.behavioral_fingerprint in seen_behavior:
            continue
        seen_behavior.add(sleeve.behavioral_fingerprint)
        row = incremental.get(key, {})
        output.append(
            {
                "sleeve_id": key,
                "market": sleeve.market,
                "execution_market": sleeve.execution_market,
                "role": sleeve.role.value,
                "mechanism": sleeve.trigger_feature,
                "behavioral_fingerprint": sleeve.behavioral_fingerprint,
                "specification_hash": runtimes[key].specification_hash,
                "events": runtimes[key].event_count,
                "net_pnl": runtimes[key].net_pnl,
                "cost_stress_1_5x_net": runtimes[key].cost_stress_1_5x_net,
                "incremental_status": row.get("status", "COMPONENT_RESEARCH_ONLY"),
                "validated": False,
            }
        )
        if len(output) >= maximum_components:
            break
    return output


def _assembly_inputs(
    bank: Sequence[Mapping[str, Any]],
    sleeves: Mapping[str, SleeveSpec],
    runtimes: Mapping[str, ExactSleeveRuntime],
) -> tuple[AssemblyInput, ...]:
    return tuple(
        AssemblyInput(
            sleeve=sleeves[str(row["sleeve_id"])],
            behavioral_cluster=str(row["behavioral_fingerprint"])[:16],
            priority_score=(
                (0.5 if row["incremental_status"] == "MICRO_EDGE_USEFUL" else 0.0)
                + 0.5 * np.tanh(float(row["cost_stress_1_5x_net"]) / 5_000.0)
            ),
            cost_per_opportunity=max(
                0.0,
                (
                    sum(event.event.gross_pnl for event in runtimes[str(row["sleeve_id"])].events)
                    - runtimes[str(row["sleeve_id"])].net_pnl
                )
                / max(runtimes[str(row["sleeve_id"])].event_count, 1),
            ),
            approximate_event_count=int(row["events"]),
        )
        for row in bank
    )


def _evaluate_account_population(
    policies: Sequence[AccountPolicyGenome],
    runtimes: Mapping[str, ExactSleeveRuntime],
    sleeves: Mapping[str, SleeveSpec],
    *,
    prereg: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], ParetoQualityDiversityArchive]:
    if not policies:
        return [], ParetoQualityDiversityArchive()
    common_days = _common_days(runtimes[key] for policy in policies for key in policy.sleeve_ids)
    episode_policy = EpisodeStartPolicy(**prereg["assembly_episode_policy"])
    starts = select_episode_starts(common_days, policy=episode_policy)
    archive = ParetoQualityDiversityArchive(
        maximum_per_niche=int(prereg["archive_policy"]["maximum_per_niche"])
    )
    output: list[dict[str, Any]] = []
    for policy in policies:
        evaluation = evaluate_compiled_account_policy(
            compile_account_policy(policy, runtimes),
            episode_policy=episode_policy,
            explicit_start_days=starts,
        )
        status = _account_research_status(evaluation, prereg["account_research_gate"])
        descriptor = _descriptor(policy, sleeves, evaluation)
        objectives = _objectives(evaluation, policy)
        archive_decision = archive.insert(
            ArchiveEntry(
                policy_id=policy.policy_id,
                family="+".join(sorted({sleeves[key].trigger_feature for key in policy.sleeve_ids})),
                lineage_id=stable_hash([sleeves[key].lineage_id for key in policy.sleeve_ids]),
                descriptor=descriptor,
                objectives=objectives,
                payload={"status": status},
            )
        )
        output.append(
            {
                "policy": policy,
                "evaluation": evaluation,
                "result": {
                    "policy": policy.to_dict(),
                    "status": status,
                    "development_only": True,
                    "validated": False,
                    "evaluation": evaluation.to_dict(),
                    "archive_decision": asdict(archive_decision),
                },
            }
        )
    return output, archive


def _run_rolling_elite(
    account_results: Sequence[Mapping[str, Any]],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    prereg: Mapping[str, Any],
) -> list[dict[str, Any]]:
    ranked = sorted(
        account_results,
        key=lambda row: (
            -row["evaluation"].controlled_base.pass_rate,
            -row["evaluation"].controlled_base.target_progress_median,
            row["evaluation"].controlled_base.mll_breach_rate,
            -row["evaluation"].controlled_stress_1_5x.median_episode_net_pnl,
            row["policy"].policy_id,
        ),
    )[: int(prereg["funnel"]["rolling_combine_elite_count"])]
    if not ranked:
        return []
    common_days = _common_days(
        runtimes[key]
        for row in ranked
        for key in row["policy"].sleeve_ids
    )
    episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
    starts = select_episode_starts(common_days, policy=episode_policy)
    output: list[dict[str, Any]] = []
    xfa_limit = int(prereg["funnel"]["xfa_elite_count"])
    for index, row in enumerate(ranked):
        policy = row["policy"]
        evaluation = evaluate_compiled_account_policy(
            compile_account_policy(policy, runtimes),
            episode_policy=episode_policy,
            explicit_start_days=starts,
            evaluate_xfa=index < xfa_limit,
        )
        status = _rolling_status(evaluation, prereg["combine_path_gate"])
        failure = derive_failure_vector(
            policy.policy_id,
            evaluation.controlled_base,
            evaluation.controlled_stress_1_5x,
            minimum_research_events=int(prereg["failure_policy"]["minimum_research_events"]),
            minimum_effective_blocks=int(prereg["failure_policy"]["minimum_effective_blocks"]),
            useful_target_progress=float(prereg["failure_policy"]["useful_target_progress"]),
            maximum_acceptable_mll_breach_rate=float(prereg["failure_policy"]["maximum_acceptable_mll_breach_rate"]),
            expected_payouts=_expected_payouts(evaluation.xfa),
        )
        output.append(
            {
                "policy": policy,
                "evaluation": evaluation,
                "failure": failure,
                "result": {
                    "policy": policy.to_dict(),
                    "status": status,
                    "development_only": True,
                    "validated": False,
                    "evaluation": evaluation.to_dict(),
                    "failure_vector": {
                        "dominant": failure.dominant.value,
                        "scores": [
                            [dimension.value, score]
                            for dimension, score in failure.scores
                        ],
                        "evidence_hash": failure.evidence_hash,
                    },
                },
            }
        )
    return output


def _run_failure_directed_mutations(
    parents: Sequence[Mapping[str, Any]],
    runtimes: Mapping[str, ExactSleeveRuntime],
    available_sleeves: Sequence[SleeveSpec],
    *,
    prereg: Mapping[str, Any],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    limit = int(prereg["funnel"]["directed_mutation_count"])
    for row in parents[:limit]:
        parent = row["policy"]
        mutation = propose_directed_mutation(
            parent, row["failure"], available_sleeves=available_sleeves
        )
        record: dict[str, Any] = mutation.to_dict()
        child = mutation.child_policy
        if child is None or any(key not in runtimes for key in child.sleeve_ids):
            record["evaluated"] = False
            output.append(record)
            continue
        try:
            child_evaluation = evaluate_compiled_account_policy(
                compile_account_policy(child, runtimes),
                episode_policy=EpisodeStartPolicy(**prereg["rolling_episode_policy"]),
                explicit_start_days=row["evaluation"].episode_start_days,
            )
        except (ValueError, UnsupportedExactExecution) as exc:
            record.update(
                {
                    "evaluated": False,
                    "terminal_reason": f"{type(exc).__name__}:{exc}",
                }
            )
            output.append(record)
            continue
        parent_utility = _research_utility(row["evaluation"])
        child_utility = _research_utility(child_evaluation)
        record.update(
            {
                "evaluated": True,
                "identical_episode_starts": (
                    child_evaluation.episode_start_days
                    == row["evaluation"].episode_start_days
                ),
                "parent_utility": parent_utility,
                "child_utility": child_utility,
                "utility_delta": child_utility - parent_utility,
                "improved": child_utility > parent_utility + 1e-9,
                "child_evaluation": child_evaluation.to_dict(),
                "validated": False,
            }
        )
        output.append(record)
    return output


def _research_genome(
    sleeve_ids: tuple[str, ...], *, campaign: str, label: str
) -> AccountPolicyGenome:
    payload = {"sleeves": sleeve_ids, "campaign": campaign, "label": label}
    return AccountPolicyGenome(
        policy_id=deterministic_id("account_research_control", payload),
        sleeve_ids=sleeve_ids,
        allocation_units=tuple(1 for _ in sleeve_ids),
        maximum_simultaneous_positions=min(2, len(sleeve_ids)),
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY",
        daily_risk_budget=1_500.0,
        daily_profit_lock=2_250.0,
        low_mll_buffer=3_000.0,
        critical_mll_buffer=1_125.0,
        loss_streak_throttle_after=3,
        mode="COMBINE_RESEARCH",
        source_campaign=campaign,
    )


def _incremental_policy(prereg: Mapping[str, Any]) -> IncrementalValuePolicy:
    return IncrementalValuePolicy(
        **{
            key: value
            for key, value in prereg["incremental_value_policy"].items()
            if key
            not in {"minimum_exact_events", "maximum_best_positive_event_share"}
        }
    )


def _common_days(runtimes: Iterable[ExactSleeveRuntime]) -> tuple[int, ...]:
    values = list(runtimes)
    if not values:
        return ()
    days = set(values[0].eligible_session_days)
    for value in values[1:]:
        days.intersection_update(value.eligible_session_days)
    return tuple(sorted(days))


def _block_map(starts: Sequence[int], blocks: int) -> dict[int, str]:
    if len(starts) < blocks:
        raise ValueError("fewer starts than frozen temporal blocks")
    edges = np.linspace(0, len(starts), blocks + 1, dtype=int)
    output: dict[int, str] = {}
    for block in range(blocks):
        for index in range(int(edges[block]), int(edges[block + 1])):
            output[int(starts[index])] = f"TEMPORAL_BLOCK_{block + 1:02d}"
    return output


def _account_research_status(
    result: AccountEvaluationResult, gate: Mapping[str, Any]
) -> str:
    base = result.controlled_base
    stress = result.controlled_stress_1_5x
    passed = bool(
        base.median_episode_net_pnl > float(gate["minimum_base_median_net"])
        and stress.median_episode_net_pnl
        > float(gate["minimum_stressed_median_net"])
        and base.target_progress_median
        >= float(gate["minimum_median_target_progress"])
        and base.mll_breach_rate <= float(gate["maximum_mll_breach_rate"])
        and base.consistency_pass_rate
        >= float(gate["minimum_consistency_pass_rate"])
    )
    return (
        "ACCOUNT_POLICY_RESEARCH_CANDIDATE"
        if passed
        else "ACCOUNT_POLICY_DIAGNOSTIC_ONLY"
    )


def _rolling_status(
    result: AccountEvaluationResult, gate: Mapping[str, Any]
) -> str:
    base = result.controlled_base
    stress = result.controlled_stress_1_5x
    passed = bool(
        base.pass_count >= int(gate["minimum_pass_count"])
        and base.mll_breach_rate <= float(gate["maximum_mll_breach_rate"])
        and stress.median_episode_net_pnl
        > float(gate["minimum_stressed_median_net"])
        and base.consistency_pass_rate
        >= float(gate["minimum_consistency_pass_rate"])
    )
    return "COMBINE_PATH_CANDIDATE" if passed else "ACCOUNT_POLICY_RESEARCH_CANDIDATE"


def _descriptor(
    policy: AccountPolicyGenome,
    sleeves: Mapping[str, SleeveSpec],
    result: AccountEvaluationResult,
) -> BehavioralDescriptor:
    selected = [sleeves[key] for key in policy.sleeve_ids]
    summary = result.controlled_base
    stress = result.controlled_stress_1_5x
    return BehavioralDescriptor(
        market="+".join(sorted({row.execution_market for row in selected})),
        session="+".join(sorted({_session_label(row.session_code) for row in selected})),
        timeframe="+".join(sorted({row.timeframe for row in selected})),
        direction_balance=(
            "BALANCED" if len({row.side for row in selected}) > 1 else ("LONG" if selected[0].side > 0 else "SHORT")
        ),
        trade_frequency=_bin(summary.accepted_event_count / max(summary.episode_start_count, 1), (10, 30), ("LOW", "MEDIUM", "HIGH")),
        holding_horizon="+".join(sorted({str(row.holding_bars) for row in selected})),
        volatility_regime="PAST_ONLY_MIXED",
        trend_range_behavior="+".join(sorted({row.trigger_feature for row in selected})),
        pnl_skew="POSITIVE" if summary.median_episode_net_pnl > 0 else "NONPOSITIVE",
        drawdown_shape="MLL_BREACH" if summary.mll_breach_rate > 0 else "MLL_SURVIVOR",
        loss_clustering=_bin(summary.median_shared_loss_days, (1, 3), ("LOW", "MEDIUM", "HIGH")),
        target_velocity=_bin(summary.target_progress_median, (0.25, 0.75), ("SLOW", "MEDIUM", "FAST")),
        mll_usage=_bin(summary.mll_breach_rate, (0.01, 0.20), ("LOW", "MEDIUM", "HIGH")),
        cost_sensitivity=(
            "RESILIENT" if stress.median_episode_net_pnl > 0 else "FRAGILE"
        ),
        correlation_cluster=stable_hash([row.behavioral_fingerprint for row in selected])[:16],
        account_role="+".join(sorted({row.role.value for row in selected})),
    )


def _objectives(
    result: AccountEvaluationResult, policy: AccountPolicyGenome
) -> ParetoObjectives:
    base = result.controlled_base
    stress = result.controlled_stress_1_5x
    xfa = result.xfa or {}
    rolling_xfa = xfa.get("rolling_xfa") or {}
    return ParetoObjectives(
        stressed_net_pnl=stress.median_episode_net_pnl,
        target_progress=base.target_progress_median,
        target_velocity=(
            0.0 if base.projected_days_to_target is None else 1.0 / max(base.projected_days_to_target, 1.0)
        ),
        combine_pass_rate_diagnostic=base.pass_rate,
        mll_breach_rate=base.mll_breach_rate,
        consistency_rate=base.consistency_pass_rate,
        xfa_survival_rate=float(rolling_xfa.get("survival_rate") or 0.0),
        expected_payouts=float(rolling_xfa.get("expected_payout_cycles_before_ruin") or 0.0),
        total_cost=float(np.median([episode.total_cost for episode in base.episodes])),
        complexity=float(len(policy.sleeve_ids) + policy.maximum_simultaneous_positions),
    )


def _research_utility(result: AccountEvaluationResult) -> float:
    base = result.controlled_base
    stress = result.controlled_stress_1_5x
    return float(
        stress.median_episode_net_pnl
        + 9_000.0 * base.target_progress_median
        - 4_500.0 * base.mll_breach_rate
        + 1_000.0 * base.consistency_pass_rate
    )


def _expected_payouts(xfa: Mapping[str, Any] | None) -> float | None:
    if not xfa:
        return None
    return float(
        (xfa.get("rolling_xfa") or {}).get("expected_payout_cycles_before_ruin")
        or 0.0
    )


def _pilot_summary(**values: Any) -> dict[str, Any]:
    prereg = values["prereg"]
    generated = values["generated"]
    screen = values["screen"]
    exact_rows = values["exact_rows"]
    incremental_rows = values["incremental_rows"]
    component_bank = values["component_bank"]
    assembly = values["assembly"]
    account_results = values["account_results"]
    rolling_results = values["rolling_results"]
    mutation_rows = values["mutation_rows"]
    archive = values["archive"]
    timings = values["timings"]
    total_wall = float(values["total_wall"])
    total_cpu = float(values["total_cpu"])
    base_summaries = [row["evaluation"].controlled_base for row in rolling_results]
    stress_summaries = [
        row["evaluation"].controlled_stress_1_5x for row in rolling_results
    ]
    account_candidate_count = sum(
        row["result"]["status"] == "ACCOUNT_POLICY_RESEARCH_CANDIDATE"
        for row in account_results
    )
    combine_path_count = sum(
        row["result"]["status"] == "COMBINE_PATH_CANDIDATE"
        for row in rolling_results
    )
    rolling_episode_count = sum(row.episode_start_count for row in base_summaries)
    return {
        "schema": "hydra_economic_evolution_pilot_result_v1",
        "engine_version": ENGINE_VERSION,
        "campaign_id": prereg["campaign_id"],
        "source_commit": _git_head(),
        "preregistration_hash": prereg["preregistration_hash"],
        "data": {
            "source_fingerprint": values["feature_build"].source_fingerprint,
            "feature_rows": values["feature_build"].rows,
            "cache_hits": values["feature_build"].cache_hits,
            "cache_misses": values["feature_build"].cache_misses,
            "new_data_purchases": 0,
            "q4_access_delta": 0,
        },
        "funnel": {
            "raw_structural_proposals": generated.raw_proposal_count,
            "unique_sleeves": generated.unique_sleeve_count,
            "typed_components": len(generated.components),
            "duplicate_proposals_rejected": generated.duplicate_proposal_count,
            "incompatible_proposals_rejected": generated.rejected_incompatible_count,
            "cheap_execution_paths": screen.unique_execution_path_count,
            "cheap_screen_cache_hits": screen.execution_cache_hit_count,
            "cheap_screen_survivors": len(screen.survivors),
            "exact_component_replays": len(exact_rows),
            "incremental_value_evaluations": len(incremental_rows),
            "micro_edge_useful": sum(row["status"] == "MICRO_EDGE_USEFUL" for row in incremental_rows),
            "component_bank": len(component_bank),
            "structural_account_policies": len(assembly.policies),
            "exact_account_policies": len(account_results),
            "account_policy_research_candidates": account_candidate_count,
            "rolling_combine_elites": len(rolling_results),
            "combine_path_candidates": combine_path_count,
            "pre_holdout_ready": 0,
            "paper_shadow_ready": 0,
            "funded_research_candidates": 0,
        },
        "rolling_combine": {
            "episode_count": rolling_episode_count,
            "pass_count": sum(row.pass_count for row in base_summaries),
            "median_pass_rate": _median(row.pass_rate for row in base_summaries),
            "median_target_progress": _median(row.target_progress_median for row in base_summaries),
            "median_mll_breach_rate": _median(row.mll_breach_rate for row in base_summaries),
            "median_stressed_net": _median(row.median_episode_net_pnl for row in stress_summaries),
        },
        "mutations": {
            "proposed": len(mutation_rows),
            "evaluated": sum(bool(row.get("evaluated")) for row in mutation_rows),
            "improved": sum(bool(row.get("improved")) for row in mutation_rows),
        },
        "archive": archive.summary(),
        "throughput": {
            "generation_per_second": generated.raw_proposal_count / max(timings["generation_seconds"], 1e-12),
            "cheap_screens_per_second": screen.unique_execution_path_count / max(timings["cheap_screen_seconds"], 1e-12),
            "exact_component_replays_per_second": len(exact_rows) / max(timings["exact_component_seconds"], 1e-12),
            "account_policies_per_second": len(account_results) / max(timings["account_policy_development_seconds"], 1e-12),
            "rolling_combine_episodes_per_second": rolling_episode_count / max(timings["rolling_combine_seconds"], 1e-12),
            "process_cpu_utilization_pct_of_one_core": 100.0 * total_cpu / max(total_wall, 1e-12),
        },
        "resources": {
            "before": values["resource_before"],
            "after": values["resource_after"],
            "peak_process_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0,
            "writer_pid": values["writer_pid"],
            "writer_count": 1,
            "timings_seconds": timings,
            "total_wall_seconds": total_wall,
            "total_cpu_seconds": total_cpu,
        },
        "governance": {
            "development_only": True,
            "expensive_validation_executed": False,
            "protected_holdout_accessed": False,
            "q4_accessed": False,
            "outbound_order_capability": False,
            "broker_connections": 0,
            "orders": 0,
            "status_inheritance": False,
        },
        "scientific_status": (
            "PILOT_COMBINE_PATHS_FOUND_REQUIRES_EXPENSIVE_VALIDATION"
            if combine_path_count
            else "PILOT_ARCHITECTURE_OPERATIONAL_NO_CONFIRMED_EDGE"
        ),
        "CONTRE": (
            "All pilot selection and account diagnostics use development data; "
            "even a Combine path is not independent evidence and cannot justify "
            "PRE_HOLDOUT_READY or PAPER_SHADOW_READY."
        ),
    }


def _session_label(value: int) -> str:
    return {-1: "ALL", 0: "OPEN", 1: "MIDDLE", 2: "LATE"}[value]


def _bin(value: float, thresholds: tuple[float, float], labels: tuple[str, str, str]) -> str:
    if value < thresholds[0]:
        return labels[0]
    if value < thresholds[1]:
        return labels[1]
    return labels[2]


def _median(values: Iterable[float]) -> float | None:
    rows = list(values)
    return float(np.median(rows)) if rows else None


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


__all__ = [
    "ENGINE_VERSION",
    "EconomicEvolutionPilotError",
    "run_economic_evolution_pilot",
]
