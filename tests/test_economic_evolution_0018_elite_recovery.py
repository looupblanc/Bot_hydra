from __future__ import annotations

from types import SimpleNamespace

from hydra.research.economic_evolution_0018_elite_recovery import (
    decompose_episode_failure,
    select_0018_elites,
)


def _row(index: int, *, passed: bool = False) -> dict:
    normal_net = float(index + 1)
    stressed_net = float(index + 1)
    return {
        "real_policy_id": f"policy_{index:03d}",
        "real_evaluation": {
            "controlled_base": {
                "pass_count": int(passed),
                "maximum_target_progress": min(1.1, 0.1 + index / 100.0),
                "target_progress_median": min(0.9, index / 100.0),
                "median_episode_net_pnl": normal_net,
                "consistency_pass_rate": 1.0,
                "mll_breach_rate": 0.0,
                "projected_days_to_target": 60.0,
            },
            "controlled_stress_1_5x": {
                "maximum_target_progress": min(1.0, 0.08 + index / 110.0),
                "target_progress_median": min(0.8, index / 110.0),
                "median_episode_net_pnl": stressed_net,
            },
        },
    }


def test_elite_selection_keeps_passers_and_intersected_top_decile() -> None:
    rows = [_row(index, passed=index == 0) for index in range(100)]
    result = select_0018_elites(rows)
    assert "policy_000" in result["selected_policy_ids"]
    assert result["top_decile_both_policy_ids"] == [
        f"policy_{index:03d}" for index in range(90, 100)
    ]
    assert len(result["near_pass_policy_ids"]) == 16


def test_failure_decomposition_is_mutually_exclusive_and_cost_aware() -> None:
    normal = SimpleNamespace(
        start_day=1,
        passed=False,
        mll_breached=False,
        terminal=SimpleNamespace(value="TIMEOUT"),
        consistency_ok=True,
        net_pnl=100.0,
        target_progress=0.5,
        maximum_target_progress=0.6,
        accepted_events=20,
        minimum_mll_buffer=3_000.0,
        days_to_target=None,
        terminal_reason="maximum_evaluation_duration_reached",
    )
    stressed = SimpleNamespace(
        start_day=1,
        net_pnl=-1.0,
        target_progress=-0.01,
    )
    result = decompose_episode_failure(
        normal, stressed, median_accepted_events=20.0
    )
    assert result["failure_cause"] == "ADVERSE_COST_SENSITIVITY"
    assert result["highest_information_change"] == "REMOVE_LOW_MARGIN_OPPORTUNITIES"
