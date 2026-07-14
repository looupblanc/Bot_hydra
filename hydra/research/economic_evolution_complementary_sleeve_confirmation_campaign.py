from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

import hydra.research.economic_evolution_coverage_three_zone_campaign as base
from hydra.economic_evolution.account_complementary_sleeve import (
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    evaluate_complementary_sleeve_policy_pairs,
)
from hydra.economic_evolution.account_coverage_three_zone import THREE_ZONE_LIMITS
from hydra.mission.economic_evolution_manifest_runtime import (
    _load_and_verify_generic_account_pair_preregistration,
)


CONFIRMATION_CLASS_ID = "GREEN_COMPLEMENTARY_SLEEVE_48_START_CONFIRMATION_V1"
CONFIRMATION_ENGINE_VERSION = (
    "hydra_complementary_sleeve_confirmation_campaign_v1"
)
FROZEN_POPULATION_CAMPAIGN_ID = (
    "hydra_economic_evolution_complementary_sleeve_0017"
)


class ComplementarySleeveConfirmationError(RuntimeError):
    pass


def run_complementary_sleeve_confirmation_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    with _bound_confirmation_campaign():
        return base.run_coverage_three_zone_campaign(
            output_dir,
            preregistration_path=preregistration_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
        )


def load_and_verify_complementary_sleeve_confirmation_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg = _load_and_verify_generic_account_pair_preregistration(
        Path(path).resolve()
    )
    structural = prereg.get("structural_population") or {}
    if (
        prereg.get("class_id") != CONFIRMATION_CLASS_ID
        or structural.get("population_campaign_id")
        != FROZEN_POPULATION_CAMPAIGN_ID
        or structural.get("parent_campaign_id")
        != "hydra_economic_evolution_three_zone_sizing_0016"
        or structural.get("sizing_parent_campaign_id")
        != "hydra_economic_evolution_buffer_sizing_0015"
        or structural.get("coverage_parent_campaign_id")
        != "hydra_economic_evolution_coverage_union_0014"
        or structural.get("parent_population_manifest_hash")
        != "c46f5f7a304b1c7a0e252bdd80ebd98badad90f2dc87abdc21ad0c978e9e9978"
        or prereg.get("three_zone_sizing_policy") != THREE_ZONE_LIMITS
        or int(prereg["rolling_episode_policy"]["maximum_starts"])
        not in {36, 48}
    ):
        raise ComplementarySleeveConfirmationError(
            "invalid complementary-sleeve confirmation preregistration"
        )
    return prereg


def _generate_frozen_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    parent_campaign_id: str,
    coverage_parent_campaign_id: str,
    policy_pair_count: int,
    maximum_components: int,
    minimum_component_events: int,
) -> Any:
    del campaign_id
    return generate_complementary_sleeve_population(
        seed_archive,
        campaign_id=FROZEN_POPULATION_CAMPAIGN_ID,
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
        return "FREEZE_REPEATABLE_PATHS_AND_LAUNCH_TARGETED_CHILDREN_0019"
    if int(result["account_research_candidate_count"]):
        return "LAUNCH_PREREGISTERED_PARTIAL_PLUS_RUNNER_0019"
    return "TOMBSTONE_EXACT_COMPLEMENTARY_SLEEVE_CLASS"


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "NO_COMBINE_PASS": "TEST_PREREGISTERED_PARTIAL_PLUS_RUNNER",
        "TARGET_VELOCITY_LOW": "INVENT_DISTINCT_EXIT_REPRESENTATION",
        "MLL_BREACH_EXCESS": "REMOVE_CORRELATED_SLEEVE_OR_DERISK",
        "CONSISTENCY_FAILURE": "ADD_PRECOMMITTED_PROFIT_SMOOTHER",
        "MATCHED_CONTROL_NOT_BEATEN": "FREEZE_NONINCREMENTAL_POLICY",
        "STRESSED_ECONOMICS_NONPOSITIVE": "FREEZE_COST_FRAGILE_POLICY",
        "TEMPORAL_BLOCK_INSTABILITY": "FREEZE_UNSTABLE_POLICY",
        "COMPONENT_CONCENTRATION": "REMOVE_DOMINANT_COMPONENT",
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
            "# HYDRA complementary-sleeve 48-start confirmation 0018",
            "",
            f"- verdict: `{result['scientific_status']}`",
            f"- frozen real/control policies: `{result['population']['real_policy_count']}` / `{result['population']['matched_control_policy_count']}`",
            f"- rolling Combine episodes: `{economics['primary_rolling_combine_episode_count']}`",
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
def _bound_confirmation_campaign() -> Iterator[None]:
    replacements = {
        "THREE_ZONE_CLASS_ID": CONFIRMATION_CLASS_ID,
        "THREE_ZONE_ENGINE_VERSION": CONFIRMATION_ENGINE_VERSION,
        "generate_coverage_three_zone_population": _generate_frozen_population,
        "evaluate_coverage_three_zone_policy_pairs": (
            evaluate_complementary_sleeve_policy_pairs
        ),
        "load_and_verify_coverage_three_zone_preregistration": (
            load_and_verify_complementary_sleeve_confirmation_preregistration
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
    "CONFIRMATION_CLASS_ID",
    "CONFIRMATION_ENGINE_VERSION",
    "ComplementarySleeveConfirmationError",
    "load_and_verify_complementary_sleeve_confirmation_preregistration",
    "run_complementary_sleeve_confirmation_campaign",
]
