from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Mapping

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_coverage_sizing import (
    BUFFER_SIZING_CLASS_ID,
    BUFFER_SIZING_LIMITS,
    generate_coverage_sizing_population,
)
from hydra.economic_evolution.account_coverage_sizing_evaluation import (
    evaluate_coverage_sizing_policy_pairs,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy, run_ultra_cheap_screen
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.economic_evolution_account_timeline_campaign import (
    account_timeline_final_result,
    account_timeline_paired_tripwire,
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


COVERAGE_SIZING_ENGINE_VERSION = "hydra_coverage_sizing_campaign_v1"


class CoverageSizingCampaignError(RuntimeError):
    pass


def run_coverage_sizing_campaign(
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
    prereg = load_and_verify_coverage_sizing_preregistration(prereg_path)
    root = _project_root(prereg_path)
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    state_writer = AtomicResultWriter(output, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    _verify_frozen_json_reference(
        root, prereg["hypothesis_worm"], semantic_key="hypothesis_hash"
    )
    _verify_frozen_json_reference(
        root, prereg["parent_terminal_verdict"], semantic_key="verdict_hash"
    )
    seed_path = root / str(prereg["source_seed"]["path"])
    seed = load_and_verify_seed_archive(seed_path)
    if _sha256(seed_path) != str(prereg["source_seed"]["file_sha256"]):
        raise CoverageSizingCampaignError("coverage-sizing seed checksum drift")
    if seed["archive_hash"] != str(prereg["source_seed"]["archive_hash"]):
        raise CoverageSizingCampaignError("coverage-sizing seed semantic drift")

    structural = prereg["structural_population"]
    population = generate_coverage_sizing_population(
        seed,
        campaign_id=str(prereg["campaign_id"]),
        parent_campaign_id=str(structural["parent_campaign_id"]),
        policy_pair_count=int(structural["policy_pair_count"]),
        maximum_components=int(structural["component_count"]),
        minimum_component_events=int(structural["minimum_component_events"]),
    )
    if population.manifest_hash != str(structural["policy_manifest_hash"]):
        raise CoverageSizingCampaignError("frozen sizing population manifest drift")
    if (
        population.parent_population_manifest_hash
        != str(structural["parent_population_manifest_hash"])
    ):
        raise CoverageSizingCampaignError("parent coverage population drift")
    writer.write_json(
        "coverage_sizing_population.json",
        {
            **population.summary(),
            "components": [row.to_dict() for row in population.components],
            "pairs": [row.to_dict() for row in population.pairs],
            "real_policies": [row.to_dict() for row in population.real_policies],
            "matched_control_policies": [
                row.to_dict() for row in population.matched_control_policies
            ],
        },
    )
    _stage(state_writer, prereg, "COVERAGE_SIZING_POPULATION_FROZEN")
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
    sleeves = tuple(row.sleeve for row in population.components)
    screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    screen = run_ultra_cheap_screen(sleeves, matrices, policy=screen_policy)
    writer.write_json("cheap_screen_summary.json", screen.summary())
    writer.write_jsonl_batch("cheap_screen_results.jsonl", list(screen.rows))
    if len(screen.rows) != len(sleeves):
        raise CoverageSizingCampaignError("coverage-sizing screen is incomplete")
    _stage(state_writer, prereg, "COMPONENT_SCREEN_COMPLETE")
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
        raise CoverageSizingCampaignError(
            "coverage sizing requires every frozen component runtime"
        )
    _stage(state_writer, prereg, "EXACT_COMPONENT_REPLAY_COMPLETE")
    phases["exact_component_replay"] = time.perf_counter() - phase_started

    common_days = _common_days(list(runtimes.values()))
    episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
    starts = select_episode_starts(common_days, policy=episode_policy)
    if len(starts) != int(prereg["rolling_episode_policy"]["maximum_starts"]):
        raise CoverageSizingCampaignError("incomplete frozen sizing starts")

    phase_started = time.perf_counter()
    pair_rows = evaluate_coverage_sizing_policy_pairs(
        population.pairs,
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=int(prereg["compute"]["account_worker_count"]),
    )
    expected_pairs = int(structural["policy_pair_count"])
    if len(pair_rows) != expected_pairs:
        raise CoverageSizingCampaignError("coverage sizing replay is incomplete")
    writer.write_jsonl_batch("coverage_sizing_pair_results.jsonl", pair_rows)
    _stage(state_writer, prereg, "PAIRED_ACCOUNT_REPLAY_COMPLETE")
    phases["paired_account_replay"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    tripwire = account_timeline_paired_tripwire(
        pair_rows, prereg["family_tripwire"]
    )
    tripwire["class_id"] = BUFFER_SIZING_CLASS_ID
    writer.write_json("family_tripwire.json", tripwire)
    _stage(state_writer, prereg, "FAMILY_TRIPWIRE_COMPLETE")
    phases["family_tripwire"] = time.perf_counter() - phase_started

    result = account_timeline_final_result(
        prereg,
        population_summary=population.summary(),
        screen_summary=screen.summary(),
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
            "schema": "hydra_coverage_sizing_result_v1",
            "engine_version": COVERAGE_SIZING_ENGINE_VERSION,
            "class_id": BUFFER_SIZING_CLASS_ID,
            "next_action": _next_action(result),
            "CONTRE": str(prereg["CONTRE"]),
        }
    )
    for row in result["best_development_policies"]:
        row["status"] = str(row["status"]).replace(
            "ACCOUNT_TIMELINE", "COVERAGE_SIZING"
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
    writer.write_json("coverage_sizing_result.json", result)
    writer.write_text("coverage_sizing_report.md", _report(result, prereg))
    _stage(state_writer, prereg, "COMPLETE")
    return result


def load_and_verify_coverage_sizing_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    resolved = Path(path).resolve()
    prereg = _load_json(resolved)
    claimed = prereg.get("preregistration_hash")
    payload = dict(prereg)
    payload.pop("preregistration_hash", None)
    structural = prereg.get("structural_population") or {}
    governance = prereg.get("governance") or {}
    statuses = prereg.get("statuses") or {}
    if (
        prereg.get("schema") != "hydra_manifest_account_pair_preregistration_v1"
        or prereg.get("class_id") != BUFFER_SIZING_CLASS_ID
        or stable_hash(payload) != claimed
        or int(structural.get("policy_pair_count", -1)) != 512
        or int(structural.get("component_count", -1)) != 48
        or structural.get("parent_campaign_id")
        != "hydra_economic_evolution_coverage_union_0014"
        or structural.get("parent_population_manifest_hash")
        != "d1644861031165aa66ba92fb69055268bcc44462679c5b8330a30f9d7cbb2424"
        or prereg.get("buffer_sizing_policy") != BUFFER_SIZING_LIMITS
        or statuses.get("validated_allowed") is not False
        or statuses.get("status_inheritance") is not False
        or governance.get("q4_access_allowed") is not False
        or governance.get("new_data_purchase_allowed") is not False
        or governance.get("broker_or_orders_allowed") is not False
    ):
        raise CoverageSizingCampaignError("invalid coverage-sizing preregistration")
    root = _project_root(resolved)
    for relative, expected in prereg.get("implementation_files", {}).items():
        if _sha256(root / str(relative)) != str(expected):
            raise CoverageSizingCampaignError(
                f"coverage-sizing implementation drift: {relative}"
            )
    return prereg


def load_and_verify_coverage_sizing_result(
    path: str | Path,
    prereg: Mapping[str, Any],
) -> dict[str, Any]:
    result = _load_json(Path(path).resolve())
    claimed = result.get("result_sha256")
    payload = dict(result)
    payload.pop("result_sha256", None)
    population = result.get("population") or {}
    economics = result.get("account_policy_economics") or {}
    governance = result.get("governance") or {}
    expected_pairs = int(prereg["structural_population"]["policy_pair_count"])
    expected_episodes = expected_pairs * int(
        prereg["rolling_episode_policy"]["maximum_starts"]
    )
    if (
        result.get("schema") != "hydra_coverage_sizing_result_v1"
        or result.get("campaign_id") != prereg.get("campaign_id")
        or result.get("class_id") != BUFFER_SIZING_CLASS_ID
        or claimed != stable_hash(payload)
        or population.get("manifest_hash")
        != prereg["structural_population"]["policy_manifest_hash"]
        or int(result.get("policy_pair_evaluated_count", -1)) != expected_pairs
        or int(economics.get("primary_rolling_combine_episode_count", -1))
        != expected_episodes
        or int(result.get("pre_holdout_ready_count", -1)) != 0
        or int(result.get("paper_shadow_ready_count", -1)) != 0
        or int(governance.get("proof_windows_consumed", -1)) != 0
        or int(governance.get("new_data_purchase_count", -1)) != 0
        or int(governance.get("q4_access_delta", -1)) != 0
        or int(governance.get("broker_connections", -1)) != 0
        or int(governance.get("orders", -1)) != 0
    ):
        raise CoverageSizingCampaignError("coverage-sizing result integrity failure")
    return result


def _next_action(result: Mapping[str, Any]) -> str:
    if int(result["combine_path_diagnostic_count"]):
        return "FREEZE_COMBINE_PATH_CHILDREN_AND_LAUNCH_0016"
    if int(result["account_research_candidate_count"]):
        return "KEEP_RESEARCH_CHILDREN_AND_CHANGE_TARGET_VELOCITY_REPRESENTATION"
    return "TOMBSTONE_EXACT_BUFFER_SIZING_CLASS"


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "NO_COMBINE_PASS": "TEST_STRUCTURALLY_DISTINCT_EXIT_ACCELERATOR",
        "TARGET_VELOCITY_LOW": "ADD_CAUSAL_OPPORTUNITY_DENSITY_NEW_CLASS",
        "MLL_BREACH_EXCESS": "REDUCE_ACCELERATION_OR_ADD_LOSS_THROTTLE",
        "CONSISTENCY_FAILURE": "ADD_PRECOMMITTED_PROFIT_SMOOTHER",
        "MATCHED_CONTROL_NOT_BEATEN": "TOMBSTONE_BUFFER_SIZING_CLASS",
        "STRESSED_ECONOMICS_NONPOSITIVE": "REMOVE_COST_FRAGILE_PARENT",
        "TEMPORAL_BLOCK_INSTABILITY": "FREEZE_UNSTABLE_PARENT",
        "COMPONENT_CONCENTRATION": "REMOVE_DOMINANT_PARENT_COMPONENT",
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
                "same_class_parameter_rescue": False,
            }
        )
        if len(output) == 4:
            break
    return output


def _report(result: Mapping[str, Any], prereg: Mapping[str, Any]) -> str:
    economics = result["account_policy_economics"]
    tripwire = result["family_tripwire"]
    wall = result["wall_clock_accounting"]
    lines = [
        "# HYDRA buffer-aware coverage sizing campaign 0015",
        "",
        f"- verdict: `{result['scientific_status']}`",
        f"- real/control policies: `{result['population']['real_policy_count']}` / `{result['population']['matched_control_policy_count']}`",
        f"- primary rolling Combine episodes: `{economics['primary_rolling_combine_episode_count']}`",
        f"- policies with >=1 pass: `{economics['policies_passing_at_least_one_combine_episode']}`",
        f"- median/max target progress: `{economics['median_target_progress_distribution']['median']:.4%}` / `{economics['maximum_target_progress']:.4%}`",
        f"- median/max MLL breach: `{economics['mll_breach_rate_distribution']['median']:.4%}` / `{economics['mll_breach_rate_distribution']['maximum']:.4%}`",
        f"- real/control wins: `{tripwire['real_win_count']}` / `{tripwire['matched_control_win_count']}`",
        f"- NULL_RATIO: `{tripwire['NULL_RATIO']}`",
        f"- research/admin: `{wall['research_percent']:.3f}%` / `{wall['tests_and_reporting_percent']:.3f}%`",
        "- data purchases: `0`; Q4 delta: `0`; broker/orders: `0/0`",
        "",
        "## CONTRE",
        "",
        str(prereg["CONTRE"]),
        "",
    ]
    return "\n".join(lines)


def _verify_frozen_json_reference(
    root: Path,
    reference: Mapping[str, Any],
    *,
    semantic_key: str,
) -> None:
    path = root / str(reference["path"])
    if _sha256(path) != str(reference["file_sha256"]):
        raise CoverageSizingCampaignError("coverage-sizing WORM checksum drift")
    if _load_json(path).get(semantic_key) != reference["semantic_hash"]:
        raise CoverageSizingCampaignError("coverage-sizing WORM semantic drift")


def _stage(
    writer: AtomicResultWriter,
    prereg: Mapping[str, Any],
    stage: str,
) -> None:
    writer.write_json(
        "coverage_sizing_campaign_state.json",
        {
            "campaign_id": prereg["campaign_id"],
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "broker_connections": 0,
            "orders": 0,
        },
    )


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise CoverageSizingCampaignError("project root not found")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise CoverageSizingCampaignError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CoverageSizingCampaignError(f"expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


__all__ = [
    "COVERAGE_SIZING_ENGINE_VERSION",
    "CoverageSizingCampaignError",
    "load_and_verify_coverage_sizing_preregistration",
    "load_and_verify_coverage_sizing_result",
    "run_coverage_sizing_campaign",
]
