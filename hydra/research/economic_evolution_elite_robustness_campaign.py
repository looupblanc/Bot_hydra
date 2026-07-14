from __future__ import annotations

import json
import statistics
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_complementary_sleeve import (
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.account_elite_robustness import (
    ELITE_ROBUSTNESS_CLASS_ID,
    DEEP_EVALUATION_QUOTAS,
    MUTATION_QUOTAS,
    generate_elite_robustness_population,
)
from hydra.economic_evolution.account_elite_robustness_evaluation import (
    evaluate_elite_robustness_policy_pairs,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy, run_ultra_cheap_screen
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.features.feature_matrix import FeatureMatrix
from hydra.mission.economic_evolution_manifest_runtime import (
    _load_and_verify_generic_account_pair_preregistration,
)
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.economic_evolution_account_timeline_campaign import (
    account_timeline_final_result,
    account_timeline_paired_tripwire,
)
from hydra.research.economic_evolution_coverage_sizing_campaign import (
    _load_json,
    _project_root,
    _sha256,
)
from hydra.research.economic_evolution_pilot import (
    _bind_selected,
    _build_exact_runtimes,
    _common_days,
    _runtime_row,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.utils.time import utc_now_iso


ELITE_ROBUSTNESS_ENGINE_VERSION = "hydra_0018_elite_robustness_campaign_v1"
SOURCE_POPULATION_CAMPAIGN_ID = "hydra_economic_evolution_complementary_sleeve_0017"
SOURCE_PARENT_CAMPAIGN_ID = "hydra_economic_evolution_three_zone_sizing_0016"
SOURCE_SIZING_PARENT_ID = "hydra_economic_evolution_buffer_sizing_0015"
SOURCE_COVERAGE_PARENT_ID = "hydra_economic_evolution_coverage_union_0014"


class EliteRobustnessCampaignError(RuntimeError):
    pass


def run_elite_robustness_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    phases: dict[str, float] = {}
    phase_started = started
    prereg_path = Path(preregistration_path).resolve()
    prereg = load_and_verify_elite_robustness_preregistration(prereg_path)
    root = _project_root(prereg_path)
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    state_writer = AtomicResultWriter(output, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    hypothesis = _verify_frozen_json_reference(
        root, prereg["hypothesis_worm"], semantic_key="hypothesis_hash"
    )
    elite_manifest = _verify_frozen_json_reference(
        root, prereg["elite_manifest"], semantic_key="manifest_hash"
    )
    if elite_manifest["manifest_hash"] != str(
        prereg["structural_population"]["elite_manifest_hash"]
    ):
        raise EliteRobustnessCampaignError("canonical 0018 elite manifest drift")
    if hypothesis["source_elite_manifest_hash"] != elite_manifest["manifest_hash"]:
        raise EliteRobustnessCampaignError("hypothesis points to another elite cohort")

    seed_path = root / str(prereg["source_seed"]["path"])
    seed = load_and_verify_seed_archive(seed_path)
    if _sha256(seed_path) != str(prereg["source_seed"]["file_sha256"]):
        raise EliteRobustnessCampaignError("elite robustness seed checksum drift")
    if seed["archive_hash"] != str(prereg["source_seed"]["archive_hash"]):
        raise EliteRobustnessCampaignError("elite robustness seed semantic drift")

    source = generate_complementary_sleeve_population(
        seed,
        campaign_id=SOURCE_POPULATION_CAMPAIGN_ID,
        parent_campaign_id=SOURCE_PARENT_CAMPAIGN_ID,
        sizing_parent_campaign_id=SOURCE_SIZING_PARENT_ID,
        coverage_parent_campaign_id=SOURCE_COVERAGE_PARENT_ID,
        policy_pair_count=512,
        maximum_components=48,
        minimum_component_events=20,
    )
    structural = prereg["structural_population"]
    if source.manifest_hash != str(structural["source_component_manifest_hash"]):
        raise EliteRobustnessCampaignError("0018 source component manifest drift")
    population = generate_elite_robustness_population(
        elite_manifest,
        [row.to_dict() for row in source.components],
        campaign_id=str(prereg["campaign_id"]),
        proposal_count=int(structural["proposal_count"]),
        deep_pair_count=int(structural["policy_pair_count"]),
    )
    if population.manifest_hash != str(structural["policy_manifest_hash"]):
        raise EliteRobustnessCampaignError("elite robustness population drift")
    summary = population.summary()
    writer.write_json(
        "elite_robustness_population.json",
        {
            **summary,
            "components": [asdict(row) for row in population.components],
            "pairs": [row.to_dict() for row in population.pairs],
        },
    )
    writer.write_jsonl_batch(
        "targeted_child_proposals.jsonl",
        [row.to_dict() for row in population.proposals],
    )
    writer.write_jsonl_batch(
        "targeted_child_cheap_screen.jsonl", list(population.screen_rows)
    )
    _stage(state_writer, prereg, "TARGETED_POPULATION_FROZEN")
    phases["preregistration_and_population"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
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
    _stage(state_writer, prereg, "FEATURE_STORE_VERIFIED")
    phases["feature_store_verification"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    sleeves = tuple(row.sleeve for row in source.components)
    screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    component_screen = run_ultra_cheap_screen(sleeves, matrices, policy=screen_policy)
    writer.write_json("source_component_screen_summary.json", component_screen.summary())
    writer.write_jsonl_batch("source_component_screen.jsonl", list(component_screen.rows))
    if len(component_screen.rows) != len(sleeves):
        raise EliteRobustnessCampaignError("source component screen is incomplete")
    policy_screen_summary = {
        "schema": "hydra_elite_robustness_cheap_policy_screen_v1",
        "proposal_count": len(population.proposals),
        "unique_policy_count": len(population.screen_rows),
        "survivor_count": sum(
            bool(row["cheap_screen_survivor"]) for row in population.screen_rows
        ),
        "duplicate_rejection_count": population.duplicate_rejection_count,
        "no_effect_rejection_count": population.no_effect_rejection_count,
        "rolling_combine_executed_during_screen": False,
        "outcomes_seen_during_generation": False,
    }
    _stage(state_writer, prereg, "CHEAP_POLICY_SCREEN_COMPLETE")
    phases["component_screen"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    bound = _bind_selected(sleeves, matrices, policy=screen_policy)
    runtimes, exact_failures = _build_exact_runtimes(
        bound,
        matrices,
        start_inclusive=str(prereg["exact_replay_period"][0]),
        end_exclusive=str(prereg["exact_replay_period"][1]),
        worker_count=int(prereg["compute"]["exact_worker_count"]),
    )
    writer.write_jsonl_batch(
        "exact_component_results.jsonl",
        [_runtime_row(row) for row in runtimes.values()],
    )
    writer.write_json("exact_component_failures.json", exact_failures)
    if len(runtimes) != len(sleeves) or exact_failures:
        raise EliteRobustnessCampaignError(
            "elite robustness requires every frozen component runtime"
        )
    _stage(state_writer, prereg, "EXACT_COMPONENT_REPLAY_COMPLETE")
    phases["exact_component_replay"] = time.perf_counter() - phase_started

    common_days = _common_days(list(runtimes.values()))
    episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
    starts = select_episode_starts(common_days, policy=episode_policy)
    if len(starts) != int(prereg["rolling_episode_policy"]["maximum_starts"]):
        raise EliteRobustnessCampaignError("incomplete frozen robustness starts")

    phase_started = time.perf_counter()
    pair_rows = evaluate_elite_robustness_policy_pairs(
        population.pairs,
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=int(prereg["compute"]["account_worker_count"]),
    )
    if len(pair_rows) != int(structural["policy_pair_count"]):
        raise EliteRobustnessCampaignError("elite robustness replay is incomplete")
    writer.write_jsonl_batch("elite_robustness_pair_results.jsonl", pair_rows)
    _stage(state_writer, prereg, "PAIRED_ACCOUNT_REPLAY_COMPLETE")
    phases["paired_account_replay"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    tripwire = account_timeline_paired_tripwire(
        pair_rows, prereg["family_tripwire"]
    )
    tripwire["class_id"] = ELITE_ROBUSTNESS_CLASS_ID
    writer.write_json("family_tripwire.json", tripwire)
    _stage(state_writer, prereg, "FAMILY_TRIPWIRE_COMPLETE")
    phases["family_tripwire"] = time.perf_counter() - phase_started

    result = account_timeline_final_result(
        prereg,
        population_summary=summary,
        screen_summary=policy_screen_summary,
        exact_runtime_count=len(runtimes),
        exact_failure_count=len(exact_failures),
        pair_rows=pair_rows,
        starts=starts,
        tripwire=tripwire,
        elapsed_seconds=time.perf_counter() - started,
        phase_seconds=phases,
    )
    result.update(
        {
            "schema": "hydra_elite_robustness_result_v1",
            "engine_version": ELITE_ROBUSTNESS_ENGINE_VERSION,
            "class_id": ELITE_ROBUSTNESS_CLASS_ID,
            "normal_plus_stressed_real_episode_count": (
                len(population.pairs) * len(starts) * 2
            ),
            "unique_matched_parent_evaluation_count": len(
                {row.parent_policy_id for row in population.pairs}
            ),
            "mutation_family_economics": _mutation_family_economics(pair_rows),
            "next_action": _next_action(result),
            "CONTRE": str(prereg["CONTRE"]),
        }
    )
    result["account_policy_economics"]["targeted_mutations_selected"] = (
        _targeted_mutations(
            result["account_policy_economics"][
                "economic_failure_vector_distribution"
            ]
        )
    )
    result.pop("result_sha256", None)
    result["result_sha256"] = stable_hash(result)
    writer.write_json("elite_robustness_result.json", result)
    writer.write_text("elite_robustness_report.md", _report(result, prereg))
    _stage(state_writer, prereg, "COMPLETE")
    return result


def load_and_verify_elite_robustness_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    resolved = Path(path).resolve()
    prereg = _load_and_verify_generic_account_pair_preregistration(resolved)
    structural = prereg.get("structural_population") or {}
    if (
        prereg.get("class_id") != ELITE_ROBUSTNESS_CLASS_ID
        or int(structural.get("proposal_count", -1)) != sum(MUTATION_QUOTAS.values())
        or int(structural.get("policy_pair_count", -1))
        != sum(DEEP_EVALUATION_QUOTAS.values())
        or int(structural.get("component_count", -1)) != 48
        or int(prereg["rolling_episode_policy"]["maximum_starts"]) != 48
        or structural.get("mutation_quotas") != MUTATION_QUOTAS
        or structural.get("deep_evaluation_quotas") != DEEP_EVALUATION_QUOTAS
        or prereg.get("compute_allocation")
        != {"elite_exploitation": 0.7, "distinct_mechanisms": 0.2, "validation": 0.1}
    ):
        raise EliteRobustnessCampaignError(
            "invalid 0018 elite robustness preregistration"
        )
    return prereg


def _verify_frozen_json_reference(
    root: Path, reference: Mapping[str, Any], *, semantic_key: str
) -> dict[str, Any]:
    path = root / str(reference["path"])
    if _sha256(path) != str(reference["file_sha256"]):
        raise EliteRobustnessCampaignError(f"frozen JSON checksum drift: {path}")
    value = _load_json(path)
    if value.get(semantic_key) != reference["semantic_hash"]:
        raise EliteRobustnessCampaignError(f"frozen JSON semantic drift: {path}")
    return value


def _mutation_family_economics(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["mutation_family"]), []).append(row)
    output: dict[str, dict[str, Any]] = {}
    for family, values in sorted(grouped.items()):
        normal = [row["real_evaluation"]["controlled_base"] for row in values]
        stress = [
            row["real_evaluation"]["controlled_stress_1_5x"] for row in values
        ]
        deltas = [float(row["paired_delta"]["stressed_median_net_usd"]) for row in values]
        output[family] = {
            "policy_count": len(values),
            "normal_pass_policy_count": sum(int(row["pass_count"]) > 0 for row in normal),
            "stressed_pass_policy_count": sum(int(row["pass_count"]) > 0 for row in stress),
            "median_normal_target_progress": statistics.median(
                float(row["target_progress_median"]) for row in normal
            ),
            "median_stressed_target_progress": statistics.median(
                float(row["target_progress_median"]) for row in stress
            ),
            "median_stressed_net_usd": statistics.median(
                float(row["median_episode_net_pnl"]) for row in stress
            ),
            "median_stressed_parent_delta_usd": statistics.median(deltas),
            "positive_parent_delta_count": sum(value > 0.0 for value in deltas),
            "maximum_normal_pass_rate": max(float(row["pass_rate"]) for row in normal),
            "maximum_stressed_pass_rate": max(float(row["pass_rate"]) for row in stress),
        }
    return output


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "TARGET_VELOCITY_LOW": "ALLOCATE_TO_PROVEN_OPPORTUNITY_REPLACEMENT_OR_EXIT_FAMILY",
        "NO_COMBINE_PASS": "RETAIN_POSITIVE_STRESS_CHILD_AND_REPAIR_EXACT_PATH_BOTTLENECK",
        "MLL_BREACH_EXCESS": "CAP_BUFFER_ACCELERATION_AND_ADD_SEQUENCE_DERISK",
        "CONSISTENCY_FAILURE": "RETAIN_ONLY_UNSEEN_START_PROFIT_SMOOTHERS",
        "STRESSED_ECONOMICS_NONPOSITIVE": "KILL_COST_FRAGILE_CHILD",
        "TEMPORAL_BLOCK_INSTABILITY": "KILL_BLOCK_DOMINATED_CHILD",
        "COMPONENT_CONCENTRATION": "REPLACE_DOMINANT_SLEEVE_WITH_DISTINCT_SESSION_ROLE",
        "MATCHED_CONTROL_NOT_BEATEN": "KILL_NONINCREMENTAL_CHILD",
    }
    output: list[dict[str, Any]] = []
    for failure, count in sorted(
        failures.items(), key=lambda item: (-int(item[1]), item[0])
    ):
        if failure not in actions:
            continue
        output.append(
            {
                "priority": len(output) + 1,
                "failure_vector": failure,
                "affected_policy_count": int(count),
                "action": actions[failure],
                "identical_episode_starts_required": True,
                "blind_parameter_grid": False,
            }
        )
        if len(output) == 4:
            break
    return output


def _next_action(result: Mapping[str, Any]) -> str:
    if bool(result["family_tripwire"]["family_green"]) and int(
        result["combine_path_diagnostic_count"]
    ):
        return "FREEZE_ROBUSTNESS_SURVIVORS_AND_LAUNCH_TARGETED_GENERATION_0021"
    if bool(result["family_tripwire"]["family_green"]) and int(
        result["account_research_candidate_count"]
    ):
        return "LAUNCH_FAILURE_GUIDED_ROBUSTNESS_GENERATION_0021"
    return "TOMBSTONE_EXACT_ROBUSTNESS_CLASS_AND_LAUNCH_DISTINCT_MECHANISM_0021"


def _report(result: Mapping[str, Any], prereg: Mapping[str, Any]) -> str:
    economics = result["account_policy_economics"]
    tripwire = result["family_tripwire"]
    wall = result["wall_clock_accounting"]
    budget = prereg["budget"]
    return "\n".join(
        [
            f"[HYDRA-V7] phase=4 step=0020 verdict={result['report_verdict']}",
            "gate=ELITE_ROBUSTNESS_TRIPWIRE preuve=reports/economic_evolution/elite_robustness_0020/elite_robustness_result.json#PENDING_WRITE tests=targeted",
            f"budget_llm={budget['llm_actual_spend_usd']}/{budget['llm_phase_max_usd']} budget_data={budget['actual_spend_usd']}/{budget['hard_cap_usd']} N_trials={prereg['multiplicity']['expected_global_N_trials_after_reservation']} burned={prereg['reporting']['burned_window_count']}",
            "diff_validation=hydra/economic_evolution/account_elite_robustness_evaluation.py",
            f"CONTRE={prereg['CONTRE']}",
            f"prochaine_action={result['next_action']}",
            "",
            "# HYDRA — 0018 elite robustness evolution 0020",
            "",
            f"- proposals / cheap survivors / deep: `{result['population']['proposal_count']}` / `{result['cheap_screen']['survivor_count']}` / `{result['policy_pair_evaluated_count']}`",
            f"- normal + stressed real episodes: `{result['normal_plus_stressed_real_episode_count']}`",
            f"- unchanged parents evaluated once: `{result['unique_matched_parent_evaluation_count']}`",
            f"- policies with normal/stressed pass: `{economics['policies_passing_at_least_one_combine_episode']}` / `{economics['stressed_policies_passing_at_least_one_combine_episode']}`",
            f"- best normal pass rate: `{economics['combine_pass_probability']['maximum']:.4%}`",
            f"- median/max target progress: `{economics['median_target_progress_distribution']['median']:.4%}` / `{economics['maximum_target_progress']:.4%}`",
            f"- median/max MLL breach: `{economics['mll_breach_rate_distribution']['median']:.4%}` / `{economics['mll_breach_rate_distribution']['maximum']:.4%}`",
            f"- real/control wins: `{tripwire['real_win_count']}` / `{tripwire['matched_control_win_count']}`; NULL_RATIO=`{tripwire['NULL_RATIO']}`",
            f"- research/admin: `{wall['research_percent']:.2f}%` / `{wall['tests_and_reporting_percent']:.2f}%`",
            "- new data / Q4 / broker / orders: `0 / 0 / 0 / 0`",
            "",
            "## CONTRE",
            "",
            str(prereg["CONTRE"]),
            "",
        ]
    )


def _stage(
    writer: AtomicResultWriter, prereg: Mapping[str, Any], stage: str
) -> None:
    writer.write_json(
        "elite_robustness_campaign_state.json",
        {
            "campaign_id": prereg["campaign_id"],
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "broker_connections": 0,
            "orders": 0,
        },
    )


__all__ = [
    "ELITE_ROBUSTNESS_ENGINE_VERSION",
    "EliteRobustnessCampaignError",
    "load_and_verify_elite_robustness_preregistration",
    "run_elite_robustness_campaign",
]
