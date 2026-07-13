from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    UnsupportedExactExecution,
    compile_account_policy,
    evaluate_compiled_account_policy,
    matched_observations_from_evaluation,
)
from hydra.economic_evolution.failure_model import derive_failure_vector, failure_scores
from hydra.economic_evolution.schema import AccountPolicyGenome, EconomicRole
from hydra.account_policy.basket import RoutedTrade
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


DAY_NS = 100_000_000_000_000


def _runtime(
    sleeve_id: str,
    market: str,
    *,
    net: float,
    offset: int,
    worst: float = -100.0,
) -> ExactSleeveRuntime:
    events = tuple(
        RoutedTrade(
            component_id=sleeve_id,
            market=market,
            side=1,
            event=TradePathEvent(
                event_id=f"{sleeve_id}:{day}",
                decision_ns=day * DAY_NS + offset,
                exit_ns=day * DAY_NS + offset + 10,
                session_day=day,
                net_pnl=net,
                gross_pnl=net + 10.0,
                worst_unrealized_pnl=worst,
                best_unrealized_pnl=max(net, 0.0) + 50.0,
                quantity=1,
                mini_equivalent=0.1,
                regime="SYNTHETIC_PAST_ONLY",
            ),
        )
        for day in range(140)
    )
    return ExactSleeveRuntime(
        sleeve_id=sleeve_id,
        signal_market=market.removeprefix("M"),
        execution_market=market,
        role=EconomicRole.PRIMARY_ALPHA,
        source_campaign="synthetic",
        specification_hash=(sleeve_id[0] * 64),
        eligible_session_days=tuple(range(140)),
        events=events,
        event_count=len(events),
        net_pnl=sum(row.event.net_pnl for row in events),
        cost_stress_1_5x_net=sum(row.event.net_pnl - 5.0 for row in events),
        maximum_drawdown=0.0,
        best_positive_event_share=1.0 / len(events),
        exit_implementation="EXACT_TIME_EXIT",
    )


def _genome(*, conflict: str = "FIXED_PRIORITY") -> AccountPolicyGenome:
    return AccountPolicyGenome(
        policy_id="account-policy-test",
        sleeve_ids=("alpha-a", "alpha-b"),
        allocation_units=(1, 2),
        maximum_simultaneous_positions=2,
        maximum_mini_equivalent=15,
        conflict_policy=conflict,
        daily_risk_budget=1_500.0,
        daily_profit_lock=2_500.0,
        low_mll_buffer=2_500.0,
        critical_mll_buffer=750.0,
        loss_streak_throttle_after=2,
        mode="COMBINE_RESEARCH",
        source_campaign="synthetic",
    )


def _episode_policy() -> EpisodeStartPolicy:
    return EpisodeStartPolicy(
        maximum_starts=3,
        minimum_spacing_sessions=20,
        minimum_observation_sessions=30,
        maximum_duration_sessions=60,
        regime_balanced=False,
    )


def test_compiled_policy_uses_one_shared_account_and_identical_starts() -> None:
    runtimes = {
        "alpha-a": _runtime("alpha-a", "MES", net=300.0, offset=1),
        "alpha-b": _runtime("alpha-b", "MNQ", net=300.0, offset=2),
    }
    compiled = compile_account_policy(_genome(), runtimes)
    result = evaluate_compiled_account_policy(
        compiled,
        episode_policy=_episode_policy(),
        explicit_start_days=(0, 30, 60),
    )

    assert result.outbound_order_capability is False
    assert result.validated is False
    assert result.episode_start_days == (0, 30, 60)
    assert result.static_base.episode_start_days == result.controlled_base.episode_start_days
    assert result.controlled_base.pass_count == 3
    assert all(
        episode.terminal is CombineTerminal.PASSED
        for episode in result.controlled_base.episodes
    )
    assert all(episode.net_pnl == 9_000.0 for episode in result.controlled_base.episodes)
    # A discrete target can overshoot by more under stress, so terminal net is
    # not monotone.  Time-to-target is the correct matched economic comparison.
    assert result.controlled_stress_1_5x.median_days_to_target > (
        result.controlled_base.median_days_to_target
    )
    assert max(
        row.event.mini_equivalent
        for row in compiled.component_events["alpha-b"]
    ) == pytest.approx(0.2)
    observations = matched_observations_from_evaluation(
        result,
        block_by_start={0: "B0", 30: "B1", 60: "B2"},
    )
    assert [row.block_id for row in observations] == ["B0", "B1", "B2"]
    assert all(row.stressed_net_after_costs > 0.0 for row in observations)


def test_unimplemented_conflict_policy_fails_closed() -> None:
    runtimes = {
        "alpha-a": _runtime("alpha-a", "MES", net=100.0, offset=1),
        "alpha-b": _runtime("alpha-b", "MNQ", net=100.0, offset=2),
    }
    with pytest.raises(UnsupportedExactExecution, match="no preregistered exact"):
        compile_account_policy(_genome(conflict="LOWEST_CORRELATION_FIRST"), runtimes)


def test_failure_vector_targets_observed_account_failure() -> None:
    runtimes = {
        "alpha-a": _runtime("alpha-a", "MES", net=12.0, offset=1),
        "alpha-b": _runtime("alpha-b", "MNQ", net=8.0, offset=2),
    }
    result = evaluate_compiled_account_policy(
        compile_account_policy(_genome(), runtimes),
        episode_policy=_episode_policy(),
        explicit_start_days=(0, 30, 60),
    )
    vector = derive_failure_vector(
        result.policy_id,
        result.controlled_base,
        result.controlled_stress_1_5x,
        minimum_research_events=10,
        minimum_effective_blocks=3,
        expected_payouts=0.0,
    )
    scores = failure_scores(vector)

    assert scores["INSUFFICIENT_TARGET_VELOCITY"] > 0.5
    assert scores["MLL_BREACH"] == 0.0
    assert vector.evaluated_on_identical_parent_child_starts is True
    assert len(vector.evidence_hash) == 64

    changed_stress = replace(
        result.controlled_stress_1_5x,
        episode_start_days=(1, 31, 61),
    )
    with pytest.raises(ValueError, match="identical starts"):
        derive_failure_vector(
            result.policy_id,
            result.controlled_base,
            changed_stress,
        )
