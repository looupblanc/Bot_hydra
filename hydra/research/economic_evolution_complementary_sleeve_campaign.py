from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

import hydra.research.economic_evolution_coverage_three_zone_campaign as base
from hydra.economic_evolution.account_complementary_sleeve import (
    COMPLEMENTARY_SLEEVE_CLASS_ID,
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    evaluate_complementary_sleeve_policy_pairs,
)
from hydra.economic_evolution.account_coverage_three_zone import THREE_ZONE_LIMITS
from hydra.mission.economic_evolution_manifest_runtime import (
    _load_and_verify_generic_account_pair_preregistration,
)


COMPLEMENTARY_SLEEVE_ENGINE_VERSION = (
    "hydra_complementary_sleeve_campaign_v1"
)


class ComplementarySleeveCampaignError(RuntimeError):
    pass


def run_complementary_sleeve_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    # The campaign process is isolated from the persistent controller. Bind a
    # new manifest grammar into the already-tested account-pair pipeline,
    # without changing shared execution, PnL, MLL, or controller semantics.
    with _bound_complementary_campaign():
        return base.run_coverage_three_zone_campaign(
            output_dir,
            preregistration_path=preregistration_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
        )


def load_and_verify_complementary_sleeve_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg = _load_and_verify_generic_account_pair_preregistration(
        Path(path).resolve()
    )
    structural = prereg.get("structural_population") or {}
    if (
        prereg.get("class_id") != COMPLEMENTARY_SLEEVE_CLASS_ID
        or structural.get("parent_campaign_id")
        != "hydra_economic_evolution_three_zone_sizing_0016"
        or structural.get("sizing_parent_campaign_id")
        != "hydra_economic_evolution_buffer_sizing_0015"
        or structural.get("coverage_parent_campaign_id")
        != "hydra_economic_evolution_coverage_union_0014"
        or structural.get("parent_population_manifest_hash")
        != "c46f5f7a304b1c7a0e252bdd80ebd98badad90f2dc87abdc21ad0c978e9e9978"
        or prereg.get("three_zone_sizing_policy") != THREE_ZONE_LIMITS
    ):
        raise ComplementarySleeveCampaignError(
            "invalid complementary-sleeve preregistration"
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
    return generate_complementary_sleeve_population(
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
    if int(result["combine_path_diagnostic_count"]):
        return "FREEZE_COMBINE_PATH_CHILDREN_AND_LAUNCH_48_START_0018"
    if int(result["account_research_candidate_count"]):
        return "KEEP_GREEN_COMPONENTS_AND_CHANGE_EXIT_REPRESENTATION"
    return "TOMBSTONE_EXACT_COMPLEMENTARY_SLEEVE_CLASS"


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "NO_COMBINE_PASS": "TEST_PREREGISTERED_PARTIAL_PLUS_RUNNER",
        "TARGET_VELOCITY_LOW": "INVENT_DISTINCT_EXIT_REPRESENTATION",
        "MLL_BREACH_EXCESS": "REMOVE_ADDED_CORRELATED_SLEEVE",
        "CONSISTENCY_FAILURE": "ADD_PRECOMMITTED_PROFIT_SMOOTHER",
        "MATCHED_CONTROL_NOT_BEATEN": "TOMBSTONE_COMPLEMENTARY_SLEEVE_CLASS",
        "STRESSED_ECONOMICS_NONPOSITIVE": "REMOVE_COST_FRAGILE_ADDITION",
        "TEMPORAL_BLOCK_INSTABILITY": "FREEZE_UNSTABLE_ADDITION",
        "COMPONENT_CONCENTRATION": "REMOVE_DOMINANT_ADDITION",
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
            "# HYDRA complementary sleeve campaign 0017",
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
    )


@contextmanager
def _bound_complementary_campaign() -> Iterator[None]:
    replacements = {
        "THREE_ZONE_CLASS_ID": COMPLEMENTARY_SLEEVE_CLASS_ID,
        "THREE_ZONE_ENGINE_VERSION": COMPLEMENTARY_SLEEVE_ENGINE_VERSION,
        "generate_coverage_three_zone_population": _generate_population,
        "evaluate_coverage_three_zone_policy_pairs": (
            evaluate_complementary_sleeve_policy_pairs
        ),
        "load_and_verify_coverage_three_zone_preregistration": (
            load_and_verify_complementary_sleeve_preregistration
        ),
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
    "COMPLEMENTARY_SLEEVE_ENGINE_VERSION",
    "ComplementarySleeveCampaignError",
    "load_and_verify_complementary_sleeve_preregistration",
    "run_complementary_sleeve_campaign",
]
