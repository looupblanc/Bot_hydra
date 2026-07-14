from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Mapping

import hydra.research.economic_evolution_elite_robustness_campaign as base
from hydra.economic_evolution.account_censored_horizon import (
    CENSORED_HORIZON_CLASS_ID,
    CENSORED_HORIZON_DEEP_EVALUATION_QUOTAS,
    CENSORED_HORIZON_MUTATION_QUOTAS,
    generate_censored_horizon_population,
)
from hydra.economic_evolution.account_censored_horizon_evaluation import (
    evaluate_censored_horizon_policy_pairs,
)


CENSORED_HORIZON_ENGINE_VERSION = "hydra_censored_horizon_campaign_v1"


def run_censored_horizon_campaign(*args: Any, **kwargs: Any) -> dict[str, Any]:
    with _patched_censored_horizon_campaign():
        return base.run_elite_robustness_campaign(*args, **kwargs)


@contextmanager
def _patched_censored_horizon_campaign() -> Iterator[None]:
    replacements: tuple[tuple[object, str, object], ...] = (
        (base, "ELITE_ROBUSTNESS_CLASS_ID", CENSORED_HORIZON_CLASS_ID),
        (base, "ELITE_ROBUSTNESS_ENGINE_VERSION", CENSORED_HORIZON_ENGINE_VERSION),
        (base, "MUTATION_QUOTAS", CENSORED_HORIZON_MUTATION_QUOTAS),
        (
            base,
            "DEEP_EVALUATION_QUOTAS",
            CENSORED_HORIZON_DEEP_EVALUATION_QUOTAS,
        ),
        (
            base,
            "generate_elite_robustness_population",
            generate_censored_horizon_population,
        ),
        (
            base,
            "evaluate_elite_robustness_policy_pairs",
            evaluate_censored_horizon_policy_pairs,
        ),
        (base, "_next_action", _censored_horizon_next_action),
        (base, "_report", _censored_horizon_report),
    )
    prior = [(owner, name, getattr(owner, name)) for owner, name, _ in replacements]
    for owner, name, value in replacements:
        setattr(owner, name, value)
    try:
        yield
    finally:
        for owner, name, value in reversed(prior):
            setattr(owner, name, value)


def _censored_horizon_next_action(result: Mapping[str, Any]) -> str:
    economics = result["account_policy_economics"]
    if int(economics["stressed_policies_passing_at_least_one_combine_episode"]):
        return "FREEZE_HORIZON_DIAGNOSTIC_AND_LAUNCH_60_DAY_VELOCITY_REPAIR_0023"
    if int(economics["policies_passing_at_least_one_combine_episode"]):
        return "LAUNCH_COST_RESILIENT_60_DAY_VELOCITY_REPAIR_0023"
    return "HORIZON_CENSORING_REJECTED_PIVOT_DISTINCT_MECHANISM_0023"


def _censored_horizon_report(
    result: Mapping[str, Any], prereg: Mapping[str, Any]
) -> str:
    economics = result["account_policy_economics"]
    tripwire = result["family_tripwire"]
    wall = result["wall_clock_accounting"]
    budget = prereg["budget"]
    return "\n".join(
        [
            f"[HYDRA-V7] phase=4 step=0022 verdict={result['report_verdict']}",
            "gate=CENSORED_HORIZON_DIAGNOSTIC "
            "preuve=reports/economic_evolution/censored_horizon_0022/"
            "elite_robustness_result.json#PENDING_WRITE tests=targeted",
            f"budget_llm={budget['llm_actual_spend_usd']}/"
            f"{budget['llm_phase_max_usd']} budget_data={budget['actual_spend_usd']}/"
            f"{budget['hard_cap_usd']} N_trials="
            f"{prereg['multiplicity']['expected_global_N_trials_after_reservation']} "
            f"burned={prereg['reporting']['burned_window_count']}",
            "diff_validation=hydra/economic_evolution/"
            "account_censored_horizon_evaluation.py",
            f"CONTRE={prereg['CONTRE']}",
            f"prochaine_action={result['next_action']}",
            "",
            "# HYDRA — frozen 0018 censored-horizon diagnostic 0022",
            "",
            f"- frozen policies / starts: `{result['policy_pair_evaluated_count']}` / "
            f"`{result['rolling_episode_start_count']}`",
            "- 90-day normal/stressed pass policies: "
            f"`{economics['policies_passing_at_least_one_combine_episode']}` / "
            f"`{economics['stressed_policies_passing_at_least_one_combine_episode']}`",
            f"- best 90-day normal pass rate: "
            f"`{economics['combine_pass_probability']['maximum']:.4%}`",
            f"- median/max 90-day target progress: "
            f"`{economics['median_target_progress_distribution']['median']:.4%}` / "
            f"`{economics['maximum_target_progress']:.4%}`",
            f"- 90-day/control wins: `{tripwire['real_win_count']}` / "
            f"`{tripwire['matched_control_win_count']}`",
            f"- research/admin: `{wall['research_percent']:.2f}%` / "
            f"`{wall['tests_and_reporting_percent']:.2f}%`",
            "- policy behavior / data / Q4 / broker / orders changed: "
            "`false / 0 / 0 / 0 / 0`",
            "",
            "## CONTRE",
            "",
            str(prereg["CONTRE"]),
            "",
        ]
    )


__all__ = [
    "CENSORED_HORIZON_ENGINE_VERSION",
    "run_censored_horizon_campaign",
]
