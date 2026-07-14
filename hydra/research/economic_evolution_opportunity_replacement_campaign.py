from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

import hydra.research.economic_evolution_coverage_three_zone_campaign as base
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    evaluate_complementary_sleeve_policy_pairs,
)
from hydra.economic_evolution.account_coverage_three_zone import THREE_ZONE_LIMITS
from hydra.economic_evolution.account_opportunity_replacement import (
    OPPORTUNITY_REPLACEMENT_CLASS_ID,
    PARENT_POPULATION_CAMPAIGN_ID,
    PARENT_POPULATION_MANIFEST_HASH,
    generate_opportunity_replacement_population,
)
from hydra.mission.economic_evolution_manifest_runtime import (
    _load_and_verify_generic_account_pair_preregistration,
)


OPPORTUNITY_REPLACEMENT_ENGINE_VERSION = (
    "hydra_opportunity_replacement_campaign_v1"
)


class OpportunityReplacementCampaignError(RuntimeError):
    pass


def run_opportunity_replacement_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    with _bound_opportunity_replacement_campaign():
        return base.run_coverage_three_zone_campaign(
            output_dir,
            preregistration_path=preregistration_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
        )


def load_and_verify_opportunity_replacement_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg = _load_and_verify_generic_account_pair_preregistration(
        Path(path).resolve()
    )
    structural = prereg.get("structural_population") or {}
    replacement = prereg.get("opportunity_replacement_policy") or {}
    if (
        prereg.get("class_id") != OPPORTUNITY_REPLACEMENT_CLASS_ID
        or structural.get("parent_campaign_id")
        != "hydra_economic_evolution_three_zone_sizing_0016"
        or structural.get("sizing_parent_campaign_id")
        != "hydra_economic_evolution_buffer_sizing_0015"
        or structural.get("coverage_parent_campaign_id")
        != "hydra_economic_evolution_coverage_union_0014"
        or structural.get("opportunity_parent_population_campaign_id")
        != PARENT_POPULATION_CAMPAIGN_ID
        or structural.get("parent_population_manifest_hash")
        != PARENT_POPULATION_MANIFEST_HASH
        or prereg.get("three_zone_sizing_policy") != THREE_ZONE_LIMITS
        or replacement.get("removal_rule")
        != "LOWEST_EVENT_COUNT_NON_COMPLEMENTARY"
        or replacement.get("replacement_rule")
        != "UNUSED_MARKET_SESSION_ROLE_NOVELTY_THEN_EVENT_COUNT"
        or int(prereg["rolling_episode_policy"]["maximum_starts"]) != 36
    ):
        raise OpportunityReplacementCampaignError(
            "invalid opportunity-replacement preregistration"
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
    return generate_opportunity_replacement_population(
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
    if not bool(result["family_tripwire"]["family_green"]):
        return "TOMBSTONE_OPPORTUNITY_REPLACEMENT_AND_LAUNCH_NEW_CLASS_0021"
    economics = result["account_policy_economics"]
    if int(economics["stressed_policies_passing_at_least_one_combine_episode"]):
        return "FREEZE_STRESSED_OPPORTUNITY_PATHS_AND_LAUNCH_0021"
    if int(result["account_research_candidate_count"]):
        return "KEEP_GREEN_REPLACEMENTS_AND_LAUNCH_TARGETED_CHILDREN_0021"
    return "TOMBSTONE_OPPORTUNITY_REPLACEMENT_AND_LAUNCH_NEW_CLASS_0021"


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "NO_COMBINE_PASS": "ADD_DISTINCT_SESSION_OPPORTUNITY_NOT_SIZE",
        "TARGET_VELOCITY_LOW": "REPLACE_NEXT_INACTIVE_ROLE_WITH_NEW_ID",
        "MLL_BREACH_EXCESS": "RESTORE_REMOVED_DEFENSIVE_ROLE",
        "CONSISTENCY_FAILURE": "SELECT_COMPLEMENTARY_CONSISTENCY_ROLE",
        "MATCHED_CONTROL_NOT_BEATEN": "TOMBSTONE_REPLACEMENT_FOR_POLICY",
        "STRESSED_ECONOMICS_NONPOSITIVE": "REMOVE_COST_FRAGILE_REPLACEMENT",
        "TEMPORAL_BLOCK_INSTABILITY": "FREEZE_UNSTABLE_REPLACEMENT",
        "COMPONENT_CONCENTRATION": "REPLACE_DOMINANT_COMPONENT",
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
            "# HYDRA opportunity-replacement campaign 0020",
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
def _bound_opportunity_replacement_campaign() -> Iterator[None]:
    replacements = {
        "THREE_ZONE_CLASS_ID": OPPORTUNITY_REPLACEMENT_CLASS_ID,
        "THREE_ZONE_ENGINE_VERSION": OPPORTUNITY_REPLACEMENT_ENGINE_VERSION,
        "generate_coverage_three_zone_population": _generate_population,
        "evaluate_coverage_three_zone_policy_pairs": (
            evaluate_complementary_sleeve_policy_pairs
        ),
        "load_and_verify_coverage_three_zone_preregistration": (
            load_and_verify_opportunity_replacement_preregistration
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
    "OPPORTUNITY_REPLACEMENT_ENGINE_VERSION",
    "OpportunityReplacementCampaignError",
    "load_and_verify_opportunity_replacement_preregistration",
    "run_opportunity_replacement_campaign",
]
