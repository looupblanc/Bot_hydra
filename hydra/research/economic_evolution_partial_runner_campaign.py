from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

import hydra.research.economic_evolution_coverage_three_zone_campaign as base
from hydra.economic_evolution.account_coverage_three_zone import THREE_ZONE_LIMITS
from hydra.economic_evolution.account_partial_runner import (
    MATCHED_CONTROL_EXIT,
    PARTIAL_RUNNER_CLASS_ID,
    PARTIAL_RUNNER_EXIT,
    PARENT_POPULATION_CAMPAIGN_ID,
    PARENT_POPULATION_MANIFEST_HASH,
    RUNNER_QUANTITY,
    TARGET_QUANTITY,
    TARGET_VOLATILITY_MULTIPLE,
    generate_partial_runner_population,
)
from hydra.economic_evolution.account_partial_runner_evaluation import (
    build_partial_runner_exact_runtimes,
    evaluate_partial_runner_policy_pairs,
)
from hydra.mission.economic_evolution_manifest_runtime import (
    _load_and_verify_generic_account_pair_preregistration,
)


PARTIAL_RUNNER_ENGINE_VERSION = "hydra_partial_runner_campaign_v1"


class PartialRunnerCampaignError(RuntimeError):
    pass


def run_partial_runner_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    with _bound_partial_runner_campaign():
        return base.run_coverage_three_zone_campaign(
            output_dir,
            preregistration_path=preregistration_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
        )


def load_and_verify_partial_runner_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg = _load_and_verify_generic_account_pair_preregistration(
        Path(path).resolve()
    )
    structural = prereg.get("structural_population") or {}
    exit_policy = prereg.get("partial_runner_policy") or {}
    if (
        prereg.get("class_id") != PARTIAL_RUNNER_CLASS_ID
        or structural.get("parent_campaign_id")
        != "hydra_economic_evolution_three_zone_sizing_0016"
        or structural.get("sizing_parent_campaign_id")
        != "hydra_economic_evolution_buffer_sizing_0015"
        or structural.get("coverage_parent_campaign_id")
        != "hydra_economic_evolution_coverage_union_0014"
        or structural.get("exit_parent_population_campaign_id")
        != PARENT_POPULATION_CAMPAIGN_ID
        or structural.get("parent_population_manifest_hash")
        != PARENT_POPULATION_MANIFEST_HASH
        or prereg.get("three_zone_sizing_policy") != THREE_ZONE_LIMITS
        or exit_policy.get("real_exit_representation") != PARTIAL_RUNNER_EXIT
        or exit_policy.get("matched_control_exit_representation")
        != MATCHED_CONTROL_EXIT
        or float(exit_policy.get("target_volatility_multiple", -1.0))
        != TARGET_VOLATILITY_MULTIPLE
        or int(exit_policy.get("target_quantity", -1)) != TARGET_QUANTITY
        or int(exit_policy.get("runner_quantity", -1)) != RUNNER_QUANTITY
        or int(prereg["rolling_episode_policy"]["maximum_starts"]) != 36
    ):
        raise PartialRunnerCampaignError(
            "invalid partial-runner campaign preregistration"
        )
    return prereg


def _generate_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    parent_campaign_id: str,
    coverage_parent_campaign_id: str,
    policy_pair_count: int,
    maximum_components: int,
    minimum_component_events: int,
) -> Any:
    return generate_partial_runner_population(
        seed_archive,
        campaign_id=campaign_id,
        parent_campaign_id=parent_campaign_id,
        sizing_parent_campaign_id=(
            "hydra_economic_evolution_buffer_sizing_0015"
        ),
        coverage_parent_campaign_id=coverage_parent_campaign_id,
        policy_pair_count=policy_pair_count,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )


def _next_action(result: Mapping[str, Any]) -> str:
    economics = result["account_policy_economics"]
    if int(economics["stressed_policies_passing_at_least_one_combine_episode"]):
        return "FREEZE_STRESS_COMBINE_PATHS_AND_LAUNCH_BOUNDED_CONFIRMATION_0020"
    if int(result["account_research_candidate_count"]):
        return "KEEP_GREEN_EXIT_COMPONENTS_AND_LAUNCH_DISTINCT_OPPORTUNITY_0020"
    return "TOMBSTONE_PARTIAL_RUNNER_CLASS_AND_LAUNCH_DISTINCT_OPPORTUNITY_0020"


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "NO_COMBINE_PASS": "CHANGE_OPPORTUNITY_REPRESENTATION_NOT_EXIT_THRESHOLDS",
        "TARGET_VELOCITY_LOW": "ADD_DISTINCT_PREREGISTERED_OPPORTUNITY_SLEEVE",
        "MLL_BREACH_EXCESS": "REDUCE_PARTIAL_RUNNER_ACCOUNT_CONCURRENCY",
        "CONSISTENCY_FAILURE": "FREEZE_RUNNER_AND_ADD_ACCOUNT_PROFIT_SMOOTHER",
        "MATCHED_CONTROL_NOT_BEATEN": "TOMBSTONE_PARTIAL_RUNNER_FOR_POLICY",
        "STRESSED_ECONOMICS_NONPOSITIVE": "TOMBSTONE_COST_FRAGILE_RUNNER",
        "TEMPORAL_BLOCK_INSTABILITY": "FREEZE_UNSTABLE_RUNNER",
        "COMPONENT_CONCENTRATION": "REMOVE_DOMINANT_RUNNER_COMPONENT",
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
    return "\n".join(
        [
            "# HYDRA partial-plus-runner campaign 0019",
            "",
            f"- verdict: `{result['scientific_status']}`",
            f"- real/control policies: `{result['population']['real_policy_count']}` / `{result['population']['matched_control_policy_count']}`",
            f"- rolling Combine episodes: `{economics['primary_rolling_combine_episode_count']}`",
            f"- normal/stressed policies with >=1 pass: `{economics['policies_passing_at_least_one_combine_episode']}` / `{economics['stressed_policies_passing_at_least_one_combine_episode']}`",
            f"- median/max target progress: `{economics['median_target_progress_distribution']['median']:.4%}` / `{economics['maximum_target_progress']:.4%}`",
            f"- median stressed progress: `{economics['stressed_target_progress_distribution']['median']:.4%}`",
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
    )


@contextmanager
def _bound_partial_runner_campaign() -> Iterator[None]:
    replacements = {
        "THREE_ZONE_CLASS_ID": PARTIAL_RUNNER_CLASS_ID,
        "THREE_ZONE_ENGINE_VERSION": PARTIAL_RUNNER_ENGINE_VERSION,
        "generate_coverage_three_zone_population": _generate_population,
        "evaluate_coverage_three_zone_policy_pairs": (
            evaluate_partial_runner_policy_pairs
        ),
        "load_and_verify_coverage_three_zone_preregistration": (
            load_and_verify_partial_runner_preregistration
        ),
        "_build_exact_runtimes": build_partial_runner_exact_runtimes,
        "_next_action": _next_action,
        "_targeted_mutations": _targeted_mutations,
        "_report": _report,
    }
    prior = {key: getattr(base, key) for key in replacements}
    for key, value in replacements.items():
        setattr(base, key, value)
    try:
        yield
    finally:
        for key, value in prior.items():
            setattr(base, key, value)


__all__ = [
    "PARTIAL_RUNNER_ENGINE_VERSION",
    "PartialRunnerCampaignError",
    "load_and_verify_partial_runner_preregistration",
    "run_partial_runner_campaign",
]
