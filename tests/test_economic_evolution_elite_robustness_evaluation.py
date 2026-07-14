from __future__ import annotations

from dataclasses import replace

from hydra.account_policy.basket import RoutedTrade
from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
    EliteRobustnessPolicyPair,
)
from hydra.economic_evolution.account_elite_robustness_evaluation import (
    ELITE_ROBUSTNESS_EVALUATION_VERSION,
    evaluate_elite_robustness_policy_pair,
    evaluate_elite_robustness_policy_pairs,
)
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import EconomicRole, stable_hash
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


DAY_NS = 100_000_000_000_000
COMPONENTS = tuple(f"robustness-component-{index}" for index in range(10))
MARKETS = ("ES", "NQ", "CL", "GC", "RTY", "YM", "MES", "MNQ", "MCL", "MGC")


def _policy(policy_id: str, *, child: int = 0) -> EliteRobustnessPolicy:
    components = COMPONENTS[child:] + COMPONENTS[:child]
    return EliteRobustnessPolicy(
        policy_id=policy_id,
        parent_policy_id="frozen-parent",
        parent_policy_fingerprint="frozen-parent-fingerprint",
        component_ids=components,
        retained_added_sleeve_id=COMPONENTS[-1],
        mutation_family=(
            "FROZEN_0018_PARENT" if child == 0 else "PRIORITY_REALLOCATION"
        ),
        failure_target=(
            "FROZEN_DEVELOPMENT_CONTROL" if child == 0 else "SEQUENCE_PATH_DEPENDENCY"
        ),
        exact_change=(("priority_rotation", child),),
        expected_effect="Frozen deterministic research comparison.",
        high_risk_units=3,
        daily_loss_guard=1_000.0,
        daily_profit_lock=2_250.0,
        critical_buffer=750.0,
        high_zone_buffer=3_750.0,
        high_zone_remaining_target=4_500.0,
        middle_zone_buffer=3_000.0,
        middle_zone_remaining_target=2_250.0,
        middle_risk_units=2,
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=15,
    )


def _pair(index: int) -> EliteRobustnessPolicyPair:
    parent = _policy("frozen-parent")
    child = _policy(f"targeted-child-{index}", child=index)
    return EliteRobustnessPolicyPair(
        pair_id=f"robustness-pair-{index}",
        parent_policy_id=parent.policy_id,
        mutation_family=child.mutation_family,
        failure_target=child.failure_target,
        real_policy=child,
        matched_control_policy=parent,
    )


def _runtimes() -> dict[str, ExactSleeveRuntime]:
    output: dict[str, ExactSleeveRuntime] = {}
    for index, (component, market) in enumerate(zip(COMPONENTS, MARKETS, strict=True)):
        events = tuple(
            RoutedTrade(
                component,
                market,
                1,
                TradePathEvent(
                    event_id=f"{component}-{day}",
                    decision_ns=day * DAY_NS + index * 1_000,
                    exit_ns=day * DAY_NS + index * 1_000 + 100,
                    session_day=day,
                    net_pnl=35.0 + index,
                    gross_pnl=40.0 + index,
                    worst_unrealized_pnl=-25.0,
                    best_unrealized_pnl=50.0 + index,
                    quantity=1,
                    mini_equivalent=1.0,
                    regime="VOLATILITY_NORMAL",
                ),
            )
            for day in range(80)
        )
        output[component] = ExactSleeveRuntime(
            sleeve_id=component,
            signal_market=market,
            execution_market=market,
            role=EconomicRole.PRIMARY_ALPHA,
            source_campaign="TEST_0018_ELITE_ROBUSTNESS",
            specification_hash=stable_hash({"component": component}),
            eligible_session_days=tuple(range(80)),
            events=events,
            event_count=len(events),
            net_pnl=sum(row.event.net_pnl for row in events),
            cost_stress_1_5x_net=sum(row.event.net_pnl - 2.5 for row in events),
            maximum_drawdown=100.0,
            best_positive_event_share=0.05,
            exit_implementation="EXACT_TIME_EXIT",
        )
    return output


def _episode_policy() -> EpisodeStartPolicy:
    return EpisodeStartPolicy(
        maximum_starts=2,
        minimum_spacing_sessions=10,
        minimum_observation_sessions=20,
        maximum_duration_sessions=20,
        regime_balanced=False,
    )


def test_elite_robustness_pair_is_deterministic_and_no_order() -> None:
    pair = _pair(1)
    first = evaluate_elite_robustness_policy_pair(
        pair,
        _runtimes(),
        starts=(0, 20),
        episode_policy=_episode_policy(),
    )
    second = evaluate_elite_robustness_policy_pair(
        pair,
        _runtimes(),
        starts=(0, 20),
        episode_policy=_episode_policy(),
    )
    assert first == second
    assert first["identical_episode_starts"] is True
    assert first["real_evaluation"]["episode_start_days"] == [0, 20]
    assert first["matched_control_evaluation"]["episode_start_days"] == [0, 20]
    assert first["new_data_purchase_count"] == 0
    assert first["q4_access_delta"] == 0
    assert first["orders"] == 0
    assert first["validated"] is False
    assert first["execution_policy_version"] == ELITE_ROBUSTNESS_EVALUATION_VERSION


def test_elite_robustness_reuses_one_parent_control() -> None:
    rows = evaluate_elite_robustness_policy_pairs(
        (_pair(1), _pair(2)),
        _runtimes(),
        starts=(0, 20),
        episode_policy=_episode_policy(),
        worker_count=2,
    )
    assert len(rows) == 2
    assert {row["unique_control_evaluation_count"] for row in rows} == {1}
    assert {row["unique_real_evaluation_count"] for row in rows} == {2}
    assert all(row["control_cache_hit"] for row in rows)


def test_elite_robustness_policy_rejects_order_status_inheritance() -> None:
    policy = _policy("frozen-parent")
    try:
        replace(policy, inherited_status="PAPER_SHADOW_READY")
    except ValueError as error:
        assert "inherit status" in str(error)
    else:
        raise AssertionError("elite robustness child inherited a status")
