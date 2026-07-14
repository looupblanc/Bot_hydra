from __future__ import annotations

from hydra.research.economic_evolution_elite_robustness_campaign import (
    ELITE_ROBUSTNESS_ENGINE_VERSION,
    _mutation_family_economics,
)


def _row(family: str, *, passed: int, delta: float) -> dict:
    summary = {
        "pass_count": passed,
        "pass_rate": passed / 48.0,
        "target_progress_median": 0.6,
        "median_episode_net_pnl": 5_400.0,
    }
    return {
        "mutation_family": family,
        "real_evaluation": {
            "controlled_base": dict(summary),
            "controlled_stress_1_5x": {
                **summary,
                "pass_count": 0,
                "pass_rate": 0.0,
                "target_progress_median": 0.52,
                "median_episode_net_pnl": 4_680.0,
            },
        },
        "paired_delta": {"stressed_median_net_usd": delta},
    }


def test_elite_robustness_family_metrics_preserve_pass_and_parent_uplift() -> None:
    metrics = _mutation_family_economics(
        (
            _row("OPPORTUNITY_REPLACEMENT", passed=1, delta=200.0),
            _row("OPPORTUNITY_REPLACEMENT", passed=0, delta=-50.0),
            _row("PROFIT_SMOOTHER", passed=0, delta=25.0),
        )
    )
    replacement = metrics["OPPORTUNITY_REPLACEMENT"]
    assert replacement["policy_count"] == 2
    assert replacement["normal_pass_policy_count"] == 1
    assert replacement["stressed_pass_policy_count"] == 0
    assert replacement["positive_parent_delta_count"] == 1
    assert replacement["median_stressed_parent_delta_usd"] == 75.0
    assert ELITE_ROBUSTNESS_ENGINE_VERSION.endswith("_v1")
