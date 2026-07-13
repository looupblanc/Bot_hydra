from __future__ import annotations

from hydra.account_policy.basket import AccountPolicyEpisode
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.research.economic_evolution_information_review import (
    _summarize_account_episodes,
    classify_account_observation,
)


def _episode(
    *,
    start: int,
    terminal: CombineTerminal,
    eligible_days: int,
    net: float,
    maximum_progress: float,
) -> AccountPolicyEpisode:
    return AccountPolicyEpisode(
        policy_id="policy_frozen",
        start_day=start,
        end_day=start + eligible_days - 1,
        terminal=terminal,
        terminal_reason="synthetic_test",
        eligible_days=eligible_days,
        traded_days=eligible_days,
        accepted_events=eligible_days,
        skipped_events=0,
        conflict_count=0,
        net_pnl=net,
        total_cost=10.0,
        target_progress=net / 9_000.0,
        maximum_target_progress=maximum_progress,
        minimum_mll_buffer=2_500.0,
        mll_breached=terminal is CombineTerminal.MLL_BREACH,
        consistency_ok=True,
        best_day_concentration=0.25,
        days_to_target=(eligible_days if terminal is CombineTerminal.PASSED else None),
        maximum_mini_equivalent=2.0,
        maximum_net_directional_exposure=2.0,
        shared_loss_days=1,
        component_contribution={"sleeve_a": net},
        skipped_reasons={},
        risk_allocation_path=(),
        daily_path=(),
    )


def test_account_observation_distinguishes_operational_and_data_censoring() -> None:
    assert (
        classify_account_observation(
            CombineTerminal.TIMEOUT,
            requested_horizon=90,
            available_sessions=120,
        )
        == "OPERATIONAL_HORIZON_NOT_REACHED"
    )
    assert (
        classify_account_observation(
            CombineTerminal.TIMEOUT,
            requested_horizon=90,
            available_sessions=45,
        )
        == "DATA_CENSORED"
    )
    assert (
        classify_account_observation(
            CombineTerminal.TIMEOUT,
            requested_horizon=None,
            available_sessions=120,
        )
        == "DATA_CENSORED"
    )
    assert (
        classify_account_observation(
            CombineTerminal.PASSED,
            requested_horizon=90,
            available_sessions=120,
        )
        == "TARGET_REACHED"
    )
    assert (
        classify_account_observation(
            CombineTerminal.MLL_BREACH,
            requested_horizon=90,
            available_sessions=120,
        )
        == "MLL_BREACHED"
    )


def test_account_horizon_summary_preserves_pass_blocks_and_censoring() -> None:
    days = tuple(range(100, 240))
    episodes = (
        _episode(
            start=100,
            terminal=CombineTerminal.PASSED,
            eligible_days=35,
            net=9_100.0,
            maximum_progress=1.01,
        ),
        _episode(
            start=180,
            terminal=CombineTerminal.TIMEOUT,
            eligible_days=60,
            net=3_000.0,
            maximum_progress=0.5,
        ),
        _episode(
            start=220,
            terminal=CombineTerminal.TIMEOUT,
            eligible_days=20,
            net=1_000.0,
            maximum_progress=0.2,
        ),
    )
    result = _summarize_account_episodes(
        episodes,
        requested_horizon=60,
        common_days=days,
        temporal_blocks=(
            {
                "block_id": "B1",
                "start_day_inclusive": 100,
                "end_day_inclusive": 169,
            },
            {
                "block_id": "B2",
                "start_day_inclusive": 170,
                "end_day_inclusive": 239,
            },
        ),
    )
    assert result["target_reached_count"] == 1
    assert result["operational_horizon_not_reached_count"] == 1
    assert result["data_censored_count"] == 1
    assert result["mll_breached_count"] == 0
    assert result["pass_temporal_blocks"] == ["B1"]
    assert result["maximum_target_progress"] == 1.01
    assert result["episodes"][2]["observation_status"] == "DATA_CENSORED"
