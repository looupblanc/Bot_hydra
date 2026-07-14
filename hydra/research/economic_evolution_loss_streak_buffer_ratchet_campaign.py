from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Mapping

import hydra.economic_evolution.account_elite_robustness_evaluation as evaluation
import hydra.research.economic_evolution_elite_robustness_campaign as base
from hydra.economic_evolution.account_loss_streak_buffer_ratchet import (
    LOSS_STREAK_BUFFER_RATCHET_CLASS_ID,
    LOSS_STREAK_BUFFER_RATCHET_POLICY_VERSION,
    RATCHET_DEEP_EVALUATION_QUOTAS,
    RATCHET_MUTATION_QUOTAS,
    generate_loss_streak_buffer_ratchet_population,
    route_loss_streak_buffer_ratchet_entry,
)
from hydra.economic_evolution.account_loss_streak_buffer_ratchet_evaluation import (
    evaluate_loss_streak_buffer_ratchet_policy_pairs,
)


LOSS_STREAK_BUFFER_RATCHET_ENGINE_VERSION = (
    "hydra_loss_streak_buffer_ratchet_campaign_v1"
)


def run_loss_streak_buffer_ratchet_campaign(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Run the homogeneous successor through the proven paired-account engine.

    The shared evaluator and campaign semantics stay frozen.  Only the
    preregistered population constructor, past-only router and identifying
    constants are replaced for this invocation.  The patch is process-local
    and restored even when evaluation fails.
    """

    with _patched_ratchet_campaign():
        return base.run_elite_robustness_campaign(*args, **kwargs)


@contextmanager
def _patched_ratchet_campaign() -> Iterator[None]:
    replacements: tuple[tuple[object, str, object], ...] = (
        (base, "ELITE_ROBUSTNESS_CLASS_ID", LOSS_STREAK_BUFFER_RATCHET_CLASS_ID),
        (
            base,
            "ELITE_ROBUSTNESS_ENGINE_VERSION",
            LOSS_STREAK_BUFFER_RATCHET_ENGINE_VERSION,
        ),
        (base, "MUTATION_QUOTAS", RATCHET_MUTATION_QUOTAS),
        (base, "DEEP_EVALUATION_QUOTAS", RATCHET_DEEP_EVALUATION_QUOTAS),
        (
            base,
            "generate_elite_robustness_population",
            generate_loss_streak_buffer_ratchet_population,
        ),
        (
            base,
            "evaluate_elite_robustness_policy_pairs",
            evaluate_loss_streak_buffer_ratchet_policy_pairs,
        ),
        (base, "_next_action", _ratchet_next_action),
        (base, "_report", _ratchet_report),
        (
            evaluation,
            "route_elite_robustness_entry",
            route_loss_streak_buffer_ratchet_entry,
        ),
        (
            evaluation,
            "ELITE_ROBUSTNESS_POLICY_VERSION",
            LOSS_STREAK_BUFFER_RATCHET_POLICY_VERSION,
        ),
    )
    prior = [(owner, name, getattr(owner, name)) for owner, name, _ in replacements]
    for owner, name, value in replacements:
        setattr(owner, name, value)
    try:
        yield
    finally:
        for owner, name, value in reversed(prior):
            setattr(owner, name, value)


def _ratchet_next_action(result: Mapping[str, Any]) -> str:
    if bool(result["family_tripwire"]["family_green"]) and int(
        result["combine_path_diagnostic_count"]
    ):
        return "FREEZE_RATCHET_SURVIVORS_AND_LAUNCH_CONFIRMATORY_GENERATION_0022"
    if bool(result["family_tripwire"]["family_green"]):
        return "LAUNCH_FAILURE_GUIDED_DISTINCT_ACCOUNT_STATE_GENERATION_0022"
    return "TOMBSTONE_EXACT_RATCHET_CLASS_AND_PIVOT_MECHANISM_0022"


def _ratchet_report(result: Mapping[str, Any], prereg: Mapping[str, Any]) -> str:
    economics = result["account_policy_economics"]
    tripwire = result["family_tripwire"]
    wall = result["wall_clock_accounting"]
    budget = prereg["budget"]
    return "\n".join(
        [
            f"[HYDRA-V7] phase=4 step=0021 verdict={result['report_verdict']}",
            "gate=LOSS_STREAK_BUFFER_RATCHET_TRIPWIRE "
            "preuve=reports/economic_evolution/loss_streak_buffer_ratchet_0021/"
            "elite_robustness_result.json#PENDING_WRITE tests=targeted",
            f"budget_llm={budget['llm_actual_spend_usd']}/"
            f"{budget['llm_phase_max_usd']} budget_data={budget['actual_spend_usd']}/"
            f"{budget['hard_cap_usd']} N_trials="
            f"{prereg['multiplicity']['expected_global_N_trials_after_reservation']} "
            f"burned={prereg['reporting']['burned_window_count']}",
            "diff_validation="
            "hydra/research/economic_evolution_loss_streak_buffer_ratchet_campaign.py",
            f"CONTRE={prereg['CONTRE']}",
            f"prochaine_action={result['next_action']}",
            "",
            "# HYDRA — loss-streak buffer ratchet 0021",
            "",
            f"- policies / starts / real episodes: `{result['policy_pair_evaluated_count']}` / "
            f"`{result['rolling_episode_start_count']}` / "
            f"`{result['normal_plus_stressed_real_episode_count']}`",
            "- normal/stressed pass policies: "
            f"`{economics['policies_passing_at_least_one_combine_episode']}` / "
            f"`{economics['stressed_policies_passing_at_least_one_combine_episode']}`",
            f"- best normal pass rate: `{economics['combine_pass_probability']['maximum']:.4%}`",
            f"- median/max target progress: "
            f"`{economics['median_target_progress_distribution']['median']:.4%}` / "
            f"`{economics['maximum_target_progress']:.4%}`",
            f"- real/control wins: `{tripwire['real_win_count']}` / "
            f"`{tripwire['matched_control_win_count']}`; "
            f"NULL_RATIO=`{tripwire['NULL_RATIO']}`",
            f"- research/admin: `{wall['research_percent']:.2f}%` / "
            f"`{wall['tests_and_reporting_percent']:.2f}%`",
            "- new data / Q4 / broker / orders: `0 / 0 / 0 / 0`",
            "",
            "## CONTRE",
            "",
            str(prereg["CONTRE"]),
            "",
        ]
    )


__all__ = [
    "LOSS_STREAK_BUFFER_RATCHET_ENGINE_VERSION",
    "run_loss_streak_buffer_ratchet_campaign",
]
