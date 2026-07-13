from __future__ import annotations

import hashlib
import json
import os
import resource
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.compute.resource_monitor import capture_resource_snapshot
from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_evaluation import (
    AccountEvaluationResult,
    ExactSleeveRuntime,
    UnsupportedExactExecution,
    compile_account_policy,
    evaluate_compiled_account_policy,
)
from hydra.economic_evolution.archive import (
    ArchiveEntry,
    ParetoQualityDiversityArchive,
)
from hydra.economic_evolution.assembly import generate_account_policy_population
from hydra.economic_evolution.failure_model import derive_failure_vector
from hydra.economic_evolution.generator import generate_structural_population
from hydra.economic_evolution.null_calibration import calibrate_incremental_validator
from hydra.economic_evolution.parallel_screen import (
    run_ultra_cheap_screen_processes,
)
from hydra.economic_evolution.policy_evolution import (
    PolicyEvolutionPopulation,
    generate_failure_directed_policy_population,
)
from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    EconomicRole,
    FailureDimension,
    SleeveSpec,
    stable_hash,
)
from hydra.economic_evolution.screen import CheapScreenPolicy
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.economic_evolution.statuses import rolling_research_status
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.economic_evolution_pilot import (
    _account_research_status,
    _assembly_inputs,
    _bind_selected,
    _build_component_bank,
    _build_exact_runtimes,
    _common_days,
    _descriptor,
    _exact_component_eligible,
    _expected_payouts,
    _incremental_policy,
    _objectives,
    _quality_diverse_exact_selection,
    _run_incremental_tournament,
    _runtime_row,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.utils.time import utc_now_iso


ENGINE_VERSION = "hydra_economic_evolution_engine_v2"


class EconomicEvolutionCampaignError(RuntimeError):
    pass


def run_economic_evolution_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Run one preregistered persistent development campaign.

    The worker owns no SQLite or registry path.  It writes only immutable
    campaign artifacts; the persistent controller remains the sole mission DB
    and registry writer.
    """

    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    children_before = _children_cpu_seconds()
    resource_before = capture_resource_snapshot()
    prereg_path = Path(preregistration_path).resolve()
    prereg = _load_and_validate_preregistration(prereg_path)
    writer = AtomicResultWriter(output_dir)
    state_writer = AtomicResultWriter(output_dir, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, "PREREGISTRATION_VERIFIED", prereg)

    calibration = calibrate_incremental_validator(
        _incremental_policy(prereg),
        seed=int(prereg["validator_calibration"]["seed"]),
        repetitions=int(prereg["validator_calibration"]["repetitions"]),
        starts_per_block=int(prereg["validator_calibration"]["starts_per_block"]),
        noise_scale=float(prereg["validator_calibration"]["noise_scale"]),
    )
    writer.write_json("validator_null_calibration.json", calibration.to_dict())
    if calibration.null_false_positive_rate > float(
        prereg["validator_calibration"]["maximum_false_positive_rate"]
    ) or calibration.meaningful_effect_power < float(
        prereg["validator_calibration"]["minimum_meaningful_effect_power"]
    ):
        raise EconomicEvolutionCampaignError(
            "fixed incremental validator failed preregistered controls"
        )
    _stage(state_writer, "VALIDATOR_CALIBRATION_GREEN", prereg)

    stage_timings: dict[str, float] = {}
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
    stage_timings["feature_open_seconds"] = time.perf_counter() - stage_start
    _stage(state_writer, "FEATURE_STORE_VERIFIED", prereg)

    stage_start = time.perf_counter()
    generated = generate_structural_population(
        campaign_id=str(prereg["campaign_id"]),
        raw_proposal_count=int(prereg["funnel"]["raw_proposals"]),
    )
    if generated.candidate_manifest_hash != str(
        prereg["structural_population"]["candidate_manifest_hash"]
    ):
        raise EconomicEvolutionCampaignError("frozen structural population drift")
    seed_path = _resolve_project_path(
        prereg_path, str(prereg["seed_archive"]["path"])
    )
    seed = load_and_verify_seed_archive(seed_path)
    if seed["archive_hash"] != str(prereg["seed_archive"]["archive_hash"]):
        raise EconomicEvolutionCampaignError("seed archive hash drift")
    seed_sleeves = tuple(
        _sleeve_from_dict(row["specification"]) for row in seed["sleeves"]
    )
    seed_behaviors = {row.behavioral_fingerprint for row in seed_sleeves}
    novel_sleeves = tuple(
        row for row in generated.sleeves if row.behavioral_fingerprint not in seed_behaviors
    )
    writer.write_json(
        "structural_population_summary.json",
        {
            **generated.summary(),
            "prior_seed_behavioral_rejections": len(generated.sleeves) - len(novel_sleeves),
            "novel_sleeve_count": len(novel_sleeves),
        },
    )
    writer.write_jsonl_batch(
        "structural_sleeves.jsonl", [row.to_dict() for row in novel_sleeves]
    )
    stage_timings["generation_seconds"] = time.perf_counter() - stage_start
    _stage(state_writer, "STRUCTURAL_POPULATION_FROZEN", prereg)

    stage_start = time.perf_counter()
    screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    screen = run_ultra_cheap_screen_processes(
        novel_sleeves,
        feature_build.market_paths,
        policy=screen_policy,
        worker_count=int(prereg["compute"]["cheap_screen_worker_count"]),
    )
    writer.write_json("cheap_screen_summary.json", screen.summary())
    writer.write_jsonl_batch("cheap_screen_results.jsonl", list(screen.rows))
    stage_timings["cheap_screen_seconds"] = time.perf_counter() - stage_start
    _stage(state_writer, "ULTRA_CHEAP_SCREEN_COMPLETE", prereg)

    stage_start = time.perf_counter()
    sleeve_by_id = {row.sleeve_id: row for row in (*seed_sleeves, *novel_sleeves)}
    new_selection = _quality_diverse_exact_selection(
        screen.rows,
        sleeve_by_id,
        limit=max(
            0,
            int(prereg["funnel"]["maximum_exact_component_replays"])
            - len(seed_sleeves),
        ),
    )
    exact_selection = _deduplicate_sleeves((*seed_sleeves, *new_selection))[
        : int(prereg["funnel"]["maximum_exact_component_replays"])
    ]
    bound = _bind_selected(exact_selection, matrices, policy=screen_policy)
    exact_runtimes, exact_failures = _build_exact_runtimes(
        bound,
        matrices,
        start_inclusive=str(prereg["exact_replay_period"][0]),
        end_exclusive=str(prereg["exact_replay_period"][1]),
        worker_count=int(prereg["compute"]["exact_worker_count"]),
    )
    exact_rows = sorted(
        (_runtime_row(row) for row in exact_runtimes.values()),
        key=lambda row: str(row["sleeve_id"]),
    )
    writer.write_jsonl_batch("exact_component_results.jsonl", exact_rows)
    writer.write_json("exact_component_failures.json", exact_failures)
    stage_timings["exact_component_seconds"] = time.perf_counter() - stage_start
    _stage(state_writer, "EXACT_COMPONENT_REPLAY_COMPLETE", prereg)

    stage_start = time.perf_counter()
    exact_eligible = _exact_component_eligible(
        exact_runtimes, prereg["incremental_value_policy"]
    )
    incremental_rows, _ = _run_incremental_tournament(
        exact_eligible,
        sleeve_by_id,
        prereg=prereg,
    )
    writer.write_jsonl_batch("incremental_value_results.jsonl", incremental_rows)
    stage_timings["incremental_value_seconds"] = time.perf_counter() - stage_start
    _stage(state_writer, "INCREMENTAL_VALUE_COMPLETE", prereg)

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
    seed_parents, seed_failures = _seed_policy_population(seed, exact_runtimes)
    evolution = generate_failure_directed_policy_population(
        seed_parents,
        seed_failures,
        tuple(sleeve_by_id[key] for key in sorted(exact_runtimes)),
        campaign_id=f"{prereg['campaign_id']}::failure_directed",
        count=int(prereg["funnel"]["failure_directed_policy_children"]),
    )
    policies = _account_population(
        seed_parents,
        evolution,
        assembly.policies,
        exact_runtimes,
        limit=int(prereg["funnel"]["exact_account_policy_evaluations"]),
    )
    writer.write_json(
        "account_policy_population.json",
        {
            **assembly.summary(),
            "seed_parent_count": len(seed_parents),
            "failure_directed": evolution.summary(),
            "exact_executable_policy_count": len(policies),
            "policies": [row.to_dict() for row in policies],
        },
    )
    stage_timings["assembly_seconds"] = time.perf_counter() - stage_start
    _stage(state_writer, "ACCOUNT_POPULATION_FROZEN", prereg)

    stage_start = time.perf_counter()
    account_rows, archive = _evaluate_account_population(
        policies,
        exact_runtimes,
        sleeve_by_id,
        prereg=prereg,
    )
    writer.write_jsonl_batch(
        "account_policy_results.jsonl", [row["result"] for row in account_rows]
    )
    writer.write_json(
        "pareto_quality_diversity_archive.json", _archive_payload(archive)
    )
    mutation_comparisons = _mutation_comparisons(account_rows, evolution)
    writer.write_jsonl_batch(
        "failure_directed_policy_comparisons.jsonl", mutation_comparisons
    )
    stage_timings["account_policy_development_seconds"] = (
        time.perf_counter() - stage_start
    )
    _stage(state_writer, "ACCOUNT_TOURNAMENT_COMPLETE", prereg)

    stage_start = time.perf_counter()
    rolling_rows = _run_rolling_elite(
        account_rows,
        exact_runtimes,
        prereg=prereg,
    )
    writer.write_jsonl_batch(
        "rolling_combine_elite_results.jsonl",
        [row["result"] for row in rolling_rows],
    )
    stage_timings["rolling_combine_seconds"] = time.perf_counter() - stage_start

    resource_after = capture_resource_snapshot()
    total_wall = time.perf_counter() - started_wall
    total_cpu = time.process_time() - started_cpu
    children_cpu = _children_cpu_seconds() - children_before
    summary = _campaign_summary(
        prereg=prereg,
        feature_build=feature_build,
        generated=generated,
        novel_sleeves=novel_sleeves,
        screen=screen,
        exact_rows=exact_rows,
        incremental_rows=incremental_rows,
        component_bank=component_bank,
        assembly=assembly,
        evolution=evolution,
        account_rows=account_rows,
        mutation_comparisons=mutation_comparisons,
        rolling_rows=rolling_rows,
        archive=archive,
        calibration=calibration,
        timings=stage_timings,
        total_wall=total_wall,
        total_cpu=total_cpu,
        children_cpu=children_cpu,
        resource_before=resource_before.to_dict(),
        resource_after=resource_after.to_dict(),
    )
    receipt = writer.write_json("economic_evolution_campaign_result.json", summary)
    state_writer.write_json(
        "campaign_state.json",
        {
            "schema": "hydra_economic_evolution_campaign_state_v1",
            "campaign_id": prereg["campaign_id"],
            "stage": "COMPLETE",
            "completed_at_utc": utc_now_iso(),
            "result_path": str(Path(output_dir) / receipt.relative_path),
            "result_sha256": receipt.sha256,
            "worker_pid": os.getpid(),
        },
    )
    return {**summary, "result_sha256": receipt.sha256}


def _load_and_validate_preregistration(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema") != "hydra_economic_evolution_campaign_preregistration_v1":
        raise EconomicEvolutionCampaignError("unexpected campaign schema")
    payload = dict(value)
    frozen_hash = str(payload.pop("preregistration_hash", ""))
    if not frozen_hash or stable_hash(payload) != frozen_hash:
        raise EconomicEvolutionCampaignError("campaign preregistration hash drift")
    if any(
        value.get(key) is not False
        for key in (
            "q4_access_allowed",
            "new_data_purchase_allowed",
            "network_access_allowed",
            "broker_or_orders_allowed",
        )
    ):
        raise EconomicEvolutionCampaignError("protected or external action enabled")
    if int(value["funnel"]["raw_proposals"]) < 50_000:
        raise EconomicEvolutionCampaignError("campaign is below substantive scale")
    if int(value["funnel"]["exact_account_policy_evaluations"]) < 100:
        raise EconomicEvolutionCampaignError("account tournament is too small")
    project_root = path.parents[2]
    for relative, digest in value["implementation_files"].items():
        candidate = project_root / str(relative)
        if not candidate.is_file() or _sha256(candidate) != str(digest):
            raise EconomicEvolutionCampaignError(
                f"frozen implementation drift: {relative}"
            )
    commit = str(value["implementation_commit"])
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=project_root,
        check=False,
    )
    if ancestor.returncode != 0:
        raise EconomicEvolutionCampaignError("implementation commit is not an ancestor")
    seed = _resolve_project_path(path, str(value["seed_archive"]["path"]))
    if _sha256(seed) != str(value["seed_archive"]["file_sha256"]):
        raise EconomicEvolutionCampaignError("seed archive file drift")
    return value


def _resolve_project_path(preregistration_path: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else preregistration_path.parents[2] / path


def _sleeve_from_dict(value: Mapping[str, Any]) -> SleeveSpec:
    return SleeveSpec(
        sleeve_id=str(value["sleeve_id"]),
        component_ids=tuple(str(row) for row in value["component_ids"]),
        market=str(value["market"]),
        execution_market=str(value["execution_market"]),
        timeframe=str(value["timeframe"]),
        session_code=int(value["session_code"]),
        trigger_feature=str(value["trigger_feature"]),
        trigger_operator=str(value["trigger_operator"]),
        trigger_quantile=float(value["trigger_quantile"]),
        context_feature=(
            None if value.get("context_feature") is None else str(value["context_feature"])
        ),
        context_operator=(
            None if value.get("context_operator") is None else str(value["context_operator"])
        ),
        context_quantile=(
            None if value.get("context_quantile") is None else float(value["context_quantile"])
        ),
        side=int(value["side"]),
        holding_bars=int(value["holding_bars"]),
        exit_style=str(value["exit_style"]),
        role=EconomicRole(str(value["role"])),
        source_campaign=str(value["source_campaign"]),
        lineage_id=str(value["lineage_id"]),
        version=int(value.get("version") or 1),
    )


def _policy_from_dict(value: Mapping[str, Any]) -> AccountPolicyGenome:
    mutation = value.get("mutation_target")
    return AccountPolicyGenome(
        policy_id=str(value["policy_id"]),
        sleeve_ids=tuple(str(row) for row in value["sleeve_ids"]),
        allocation_units=tuple(int(row) for row in value["allocation_units"]),
        maximum_simultaneous_positions=int(value["maximum_simultaneous_positions"]),
        maximum_mini_equivalent=int(value["maximum_mini_equivalent"]),
        conflict_policy=str(value["conflict_policy"]),
        daily_risk_budget=float(value["daily_risk_budget"]),
        daily_profit_lock=float(value["daily_profit_lock"]),
        low_mll_buffer=float(value["low_mll_buffer"]),
        critical_mll_buffer=float(value["critical_mll_buffer"]),
        loss_streak_throttle_after=int(value["loss_streak_throttle_after"]),
        mode=str(value["mode"]),
        source_campaign=str(value["source_campaign"]),
        parent_policy_ids=tuple(str(row) for row in value.get("parent_policy_ids") or ()),
        mutation_target=(None if mutation is None else FailureDimension(str(mutation))),
        version=int(value.get("version") or 1),
    )


def _seed_policy_population(
    seed: Mapping[str, Any],
    runtimes: Mapping[str, ExactSleeveRuntime],
) -> tuple[tuple[AccountPolicyGenome, ...], dict[str, FailureDimension]]:
    parents: list[AccountPolicyGenome] = []
    failures: dict[str, FailureDimension] = {}
    for row in seed["policies"]:
        policy = _policy_from_dict(row["policy"])
        if all(key in runtimes for key in policy.sleeve_ids):
            parents.append(policy)
            failures[policy.policy_id] = FailureDimension(
                str(row["failure_vector"]["dominant"])
            )
    parents.sort(key=lambda row: row.policy_id)
    if not parents:
        raise EconomicEvolutionCampaignError("no seed policy can be replayed")
    return tuple(parents), failures


def _deduplicate_sleeves(rows: Sequence[SleeveSpec]) -> tuple[SleeveSpec, ...]:
    output: list[SleeveSpec] = []
    seen: set[str] = set()
    for row in rows:
        if row.exit_style != "TIME_ONLY" or row.behavioral_fingerprint in seen:
            continue
        seen.add(row.behavioral_fingerprint)
        output.append(row)
    return tuple(output)


def _account_population(
    parents: Sequence[AccountPolicyGenome],
    evolution: PolicyEvolutionPopulation,
    assembled: Sequence[AccountPolicyGenome],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    limit: int,
) -> tuple[AccountPolicyGenome, ...]:
    candidates = (
        *parents,
        *(row.child_policy for row in evolution.children),
        *(row for row in assembled if row.conflict_policy == "FIXED_PRIORITY"),
    )
    output: list[AccountPolicyGenome] = []
    seen: set[str] = set()
    for row in candidates:
        if row.structural_fingerprint in seen:
            continue
        if any(key not in runtimes for key in row.sleeve_ids):
            continue
        seen.add(row.structural_fingerprint)
        output.append(row)
        if len(output) >= limit:
            break
    return tuple(output)


def _evaluate_account_population(
    policies: Sequence[AccountPolicyGenome],
    runtimes: Mapping[str, ExactSleeveRuntime],
    sleeves: Mapping[str, SleeveSpec],
    *,
    prereg: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], ParetoQualityDiversityArchive]:
    if not policies:
        return [], ParetoQualityDiversityArchive()
    common_days = _common_days(
        runtimes[key] for policy in policies for key in policy.sleeve_ids
    )
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
        status = _account_research_status(
            evaluation, prereg["account_research_gate"]
        )
        descriptor = _descriptor(policy, sleeves, evaluation)
        objectives = _objectives(evaluation, policy)
        decision = archive.insert(
            ArchiveEntry(
                policy_id=policy.policy_id,
                family="+".join(
                    sorted({sleeves[key].trigger_feature for key in policy.sleeve_ids})
                ),
                lineage_id=stable_hash(
                    [sleeves[key].lineage_id for key in policy.sleeve_ids]
                ),
                descriptor=descriptor,
                objectives=objectives,
                payload={"status": status},
            )
        )
        output.append(
            {
                "policy": policy,
                "evaluation": evaluation,
                "status": status,
                "result": {
                    "policy": policy.to_dict(),
                    "status": status,
                    "development_only": True,
                    "validated": False,
                    "evaluation": evaluation.to_dict(),
                    "archive_decision": asdict(decision),
                },
            }
        )
    return output, archive


def _run_rolling_elite(
    account_rows: Sequence[Mapping[str, Any]],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    prereg: Mapping[str, Any],
) -> list[dict[str, Any]]:
    ranked = sorted(
        account_rows,
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
        runtimes[key] for row in ranked for key in row["policy"].sleeve_ids
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
        status = rolling_research_status(
            evaluation,
            prereg["combine_path_gate"],
            fallback_status=str(row["status"]),
        )
        failure = derive_failure_vector(
            policy.policy_id,
            evaluation.controlled_base,
            evaluation.controlled_stress_1_5x,
            minimum_research_events=int(
                prereg["failure_policy"]["minimum_research_events"]
            ),
            minimum_effective_blocks=int(
                prereg["failure_policy"]["minimum_effective_blocks"]
            ),
            useful_target_progress=float(
                prereg["failure_policy"]["useful_target_progress"]
            ),
            maximum_acceptable_mll_breach_rate=float(
                prereg["failure_policy"]["maximum_acceptable_mll_breach_rate"]
            ),
            expected_payouts=_expected_payouts(evaluation.xfa),
        )
        output.append(
            {
                "policy": policy,
                "evaluation": evaluation,
                "status": status,
                "result": {
                    "policy": policy.to_dict(),
                    "status": status,
                    "upstream_status": row["status"],
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


def _mutation_comparisons(
    account_rows: Sequence[Mapping[str, Any]],
    evolution: PolicyEvolutionPopulation,
) -> list[dict[str, Any]]:
    by_id = {str(row["policy"].policy_id): row for row in account_rows}
    output: list[dict[str, Any]] = []
    for child in evolution.children:
        parent = by_id.get(child.parent_policy_id)
        current = by_id.get(child.child_policy.policy_id)
        record = child.to_dict()
        if parent is None or current is None:
            record.update({"evaluated": False, "improved": False})
        else:
            parent_utility = _research_utility(parent["evaluation"])
            child_utility = _research_utility(current["evaluation"])
            record.update(
                {
                    "evaluated": True,
                    "identical_episode_starts": (
                        parent["evaluation"].episode_start_days
                        == current["evaluation"].episode_start_days
                    ),
                    "parent_utility": parent_utility,
                    "child_utility": child_utility,
                    "utility_delta": child_utility - parent_utility,
                    "improved": child_utility > parent_utility + 1e-9,
                    "validated": False,
                }
            )
        output.append(record)
    return output


def _research_utility(result: AccountEvaluationResult) -> float:
    base = result.controlled_base
    stress = result.controlled_stress_1_5x
    return float(
        stress.median_episode_net_pnl
        + 9_000.0 * base.target_progress_median
        - 4_500.0 * base.mll_breach_rate
        + 1_000.0 * base.consistency_pass_rate
    )


def _archive_payload(archive: ParetoQualityDiversityArchive) -> dict[str, Any]:
    return {
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
    }


def _campaign_summary(**values: Any) -> dict[str, Any]:
    prereg = values["prereg"]
    generated = values["generated"]
    screen = values["screen"]
    account_rows = values["account_rows"]
    rolling_rows = values["rolling_rows"]
    base = [row["evaluation"].controlled_base for row in rolling_rows]
    stress = [row["evaluation"].controlled_stress_1_5x for row in rolling_rows]
    timings = values["timings"]
    wall = float(values["total_wall"])
    aggregate_cpu = float(values["total_cpu"] + values["children_cpu"])
    return {
        "schema": "hydra_economic_evolution_campaign_result_v1",
        "engine_version": ENGINE_VERSION,
        "campaign_id": prereg["campaign_id"],
        "source_commit": _git_head(),
        "preregistration_hash": prereg["preregistration_hash"],
        "completed_at_utc": utc_now_iso(),
        "data": {
            "source_fingerprint": values["feature_build"].source_fingerprint,
            "feature_rows": values["feature_build"].rows,
            "cache_hits": values["feature_build"].cache_hits,
            "cache_misses": values["feature_build"].cache_misses,
            "new_data_purchases": 0,
            "q4_access_delta": 0,
        },
        "validator_calibration": values["calibration"].to_dict(),
        "funnel": {
            "raw_structural_proposals": generated.raw_proposal_count,
            "unique_sleeves": generated.unique_sleeve_count,
            "novel_sleeves_after_seed_dedup": len(values["novel_sleeves"]),
            "typed_components": len(generated.components),
            "duplicate_proposals_rejected": generated.duplicate_proposal_count,
            "cheap_execution_paths": screen.unique_execution_path_count,
            "cheap_screen_cache_hits": screen.execution_cache_hit_count,
            "cheap_screen_survivors": len(screen.survivors),
            "exact_component_replays": len(values["exact_rows"]),
            "incremental_value_evaluations": len(values["incremental_rows"]),
            "micro_edge_useful": sum(
                row["status"] == "MICRO_EDGE_USEFUL"
                for row in values["incremental_rows"]
            ),
            "component_bank": len(values["component_bank"]),
            "structural_account_policies": len(values["assembly"].policies),
            "failure_directed_policy_children": len(values["evolution"].children),
            "exact_account_policies": len(account_rows),
            "account_policy_research_candidates": sum(
                row["status"] == "ACCOUNT_POLICY_RESEARCH_CANDIDATE"
                for row in account_rows
            ),
            "rolling_combine_elites": len(rolling_rows),
            "combine_path_candidates": sum(
                row["status"] == "COMBINE_PATH_CANDIDATE"
                for row in rolling_rows
            ),
            "pre_holdout_ready": 0,
            "paper_shadow_ready": 0,
        },
        "rolling_combine": {
            "episode_count": sum(row.episode_start_count for row in base),
            "pass_count": sum(row.pass_count for row in base),
            "median_pass_rate": _median(row.pass_rate for row in base),
            "median_target_progress": _median(row.target_progress_median for row in base),
            "maximum_target_progress": max(
                (row.maximum_target_progress for row in base), default=None
            ),
            "median_mll_breach_rate": _median(row.mll_breach_rate for row in base),
            "median_stressed_net": _median(row.median_episode_net_pnl for row in stress),
        },
        "mutations": {
            "proposed": len(values["evolution"].children),
            "evaluated": sum(
                bool(row.get("evaluated")) for row in values["mutation_comparisons"]
            ),
            "improved": sum(
                bool(row.get("improved")) for row in values["mutation_comparisons"]
            ),
        },
        "archive": values["archive"].summary(),
        "throughput": {
            "generation_per_second": generated.raw_proposal_count
            / max(timings["generation_seconds"], 1e-12),
            "cheap_screens_per_second": screen.unique_execution_path_count
            / max(timings["cheap_screen_seconds"], 1e-12),
            "exact_component_replays_per_second": len(values["exact_rows"])
            / max(timings["exact_component_seconds"], 1e-12),
            "account_policies_per_second": len(account_rows)
            / max(timings["account_policy_development_seconds"], 1e-12),
            "rolling_combine_episodes_per_second": sum(
                row.episode_start_count for row in base
            )
            / max(timings["rolling_combine_seconds"], 1e-12),
            "aggregate_cpu_utilization_pct_of_one_core": 100.0
            * aggregate_cpu
            / max(wall, 1e-12),
        },
        "resources": {
            "before": values["resource_before"],
            "after": values["resource_after"],
            "peak_process_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            / 1024.0,
            "writer_pid": os.getpid(),
            "registry_writer_count": 0,
            "mission_db_writer_count": 0,
            "artifact_writer_count": 1,
            "timings_seconds": timings,
            "total_wall_seconds": wall,
            "aggregate_cpu_seconds": aggregate_cpu,
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
            "single_authoritative_mission_writer_preserved": True,
        },
        "scientific_status": (
            "CAMPAIGN_COMBINE_PATHS_FOUND_REQUIRES_EXPENSIVE_VALIDATION"
            if any(row["status"] == "COMBINE_PATH_CANDIDATE" for row in rolling_rows)
            else "CAMPAIGN_OPERATIONAL_NO_CONFIRMED_EDGE"
        ),
        "CONTRE": (
            "The persistent campaign still selects and evaluates on development "
            "history. Research candidates and diagnostic Combine paths are not "
            "independent evidence and cannot authorize shadow or deployment."
        ),
    }


def _stage(
    writer: AtomicResultWriter,
    stage: str,
    prereg: Mapping[str, Any],
) -> None:
    writer.write_json(
        "campaign_state.json",
        {
            "schema": "hydra_economic_evolution_campaign_state_v1",
            "campaign_id": prereg["campaign_id"],
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "worker_pid": os.getpid(),
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "orders": 0,
        },
    )


def _children_cpu_seconds() -> float:
    value = resource.getrusage(resource.RUSAGE_CHILDREN)
    return float(value.ru_utime + value.ru_stime)


def _median(values: Iterable[float]) -> float | None:
    rows = list(values)
    return float(np.median(rows)) if rows else None


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


__all__ = [
    "ENGINE_VERSION",
    "EconomicEvolutionCampaignError",
    "run_economic_evolution_campaign",
]
