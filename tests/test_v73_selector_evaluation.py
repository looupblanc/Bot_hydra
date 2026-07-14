from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.economic_evolution.account_elite_robustness import EliteRobustnessPolicy
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.account_static_parent_basket import (
    StaticParentBasketPolicy,
)
from hydra.economic_evolution.schema import EconomicRole
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.selection.selector_evaluation import (
    SelectorEvaluationError,
    aggregate_design_block_metrics,
    build_frozen_static_account_policy,
    elite_robustness_policy_from_dict,
    evaluate_policy_block,
    static_parent_policy_from_dict,
)


DAY_NS = 100_000_000_000_000
COMPONENT_IDS = tuple(f"component-{index:02d}" for index in range(10))


def _static_policy() -> StaticParentBasketPolicy:
    return StaticParentBasketPolicy(
        policy_id="static-parent-test",
        parent_policy_id="parent-a",
        parent_policy_fingerprint="a" * 64,
        source_parent_ids=("parent-a", "parent-b"),
        component_ids=COMPONENT_IDS,
        retained_added_sleeve_id=COMPONENT_IDS[-1],
        mutation_family="STATIC_PARENT_SYNTHESIS",
        failure_target="TARGET_VELOCITY",
        exact_change=(("assembly_profile", "CONSENSUS_10_ROTATION_0"),),
        expected_effect="Frozen selector-evaluation fixture.",
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
        assembly_profile="CONSENSUS_10_ROTATION_0",
    )


def _elite_policy() -> EliteRobustnessPolicy:
    return EliteRobustnessPolicy(
        policy_id="elite-parent-test",
        parent_policy_id="elite-parent-test",
        parent_policy_fingerprint="b" * 64,
        component_ids=COMPONENT_IDS,
        retained_added_sleeve_id=COMPONENT_IDS[-1],
        mutation_family="FROZEN_0018_PARENT",
        failure_target="FROZEN_PARENT",
        exact_change=(),
        expected_effect="Frozen best-parent baseline fixture.",
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


def _runtime(
    component_id: str,
    component_index: int,
    *,
    days: range = range(120),
    net: float = 5.0,
    session_compliant: bool = True,
) -> ExactSleeveRuntime:
    events = tuple(
        RoutedTrade(
            component_id=component_id,
            market=f"M{component_index:02d}",
            side=1,
            event=TradePathEvent(
                event_id=f"{component_id}:{day}",
                decision_ns=day * DAY_NS + component_index * 1_000,
                exit_ns=day * DAY_NS + component_index * 1_000 + 100,
                session_day=day,
                net_pnl=net,
                gross_pnl=net + 2.0,
                worst_unrealized_pnl=-2.0,
                best_unrealized_pnl=max(net, 0.0),
                quantity=1,
                mini_equivalent=0.1,
                regime="SYNTHETIC_PAST_ONLY",
                session_compliant=session_compliant,
            ),
        )
        for day in days
    )
    return ExactSleeveRuntime(
        sleeve_id=component_id,
        signal_market=f"S{component_index:02d}",
        execution_market=f"M{component_index:02d}",
        role=EconomicRole.PRIMARY_ALPHA,
        source_campaign="synthetic-immutable-ledger",
        specification_hash=hashlib.sha256(component_id.encode()).hexdigest(),
        eligible_session_days=tuple(days),
        events=events,
        event_count=len(events),
        net_pnl=sum(row.event.net_pnl for row in events),
        cost_stress_1_5x_net=sum(
            row.event.net_pnl - 1.0 for row in events
        ),
        maximum_drawdown=0.0,
        best_positive_event_share=1.0 / len(events),
        exit_implementation="EXACT_TIME_EXIT",
    )


def _runtimes(**changes: object) -> dict[str, ExactSleeveRuntime]:
    output = {
        component_id: _runtime(component_id, index)
        for index, component_id in enumerate(COMPONENT_IDS)
    }
    output.update(changes)  # type: ignore[arg-type]
    return output


def test_persisted_policy_loaders_verify_structural_fingerprints() -> None:
    static = _static_policy()
    elite = _elite_policy()

    assert static_parent_policy_from_dict(static.to_dict()) == static
    assert elite_robustness_policy_from_dict(elite.to_dict()) == elite

    tampered = static.to_dict()
    tampered["component_ids"] = list(reversed(tampered["component_ids"]))
    with pytest.raises(SelectorEvaluationError, match="fingerprint drift"):
        static_parent_policy_from_dict(tampered)

    missing = elite.to_dict()
    missing.pop("structural_fingerprint")
    with pytest.raises(SelectorEvaluationError, match="requires its.*fingerprint"):
        elite_robustness_policy_from_dict(missing)


def test_builder_freezes_membership_and_one_integer_micro_risk_tier() -> None:
    source = _static_policy()
    frozen = build_frozen_static_account_policy(source.to_dict(), "1.25x")

    assert frozen.source_policy_fingerprint == source.structural_fingerprint
    assert frozen.basket.component_ids == source.component_ids
    assert frozen.basket.component_priority == source.component_ids
    assert frozen.controller.component_priority == source.component_ids
    assert frozen.controller.risk_label == "1.25x"
    assert frozen.controller.micro_risk_units == 5
    assert frozen.controller.dynamic_buffer_sizing is False
    assert frozen.controller.loss_streak_sizing is False
    assert build_frozen_static_account_policy(
        source, "1.25x"
    ).controller.structural_fingerprint == frozen.controller.structural_fingerprint


def test_block_evaluation_is_deterministic_and_stress_cost_is_applied() -> None:
    policy = _static_policy()
    runtimes = _runtimes()
    kwargs = {
        "risk_level": "0.75x",
        "block_id": "B1",
        "session_days": tuple(range(50)),
        "start_days": (0, 10),
    }

    first = evaluate_policy_block(policy, runtimes, **kwargs)
    second = evaluate_policy_block(
        policy,
        dict(reversed(tuple(runtimes.items()))),
        **kwargs,
    )

    assert first == second
    assert first["micro_risk_units"] == 3
    assert first["episode_count"] == 2
    assert first["normal_pass_count"] == 0
    assert first["stress_pass_count"] == 0
    assert first["stressed_pass_count"] == first["stress_pass_count"]
    assert first["normal_net_usd"] == pytest.approx(12_000.0)
    assert first["stressed_net_usd"] == pytest.approx(9_600.0)
    assert first["normal_net_usd"] > first["stressed_net_usd"]
    assert first["mll_breach_rate"] == 0.0
    assert first["consistency_pass_rate"] == 1.0
    assert first["stressed_target_progress"] == (
        first["stressed_target_progress_median"]
    )
    assert first["mll_breach_count"] == 0
    assert first["consistency"] == first["consistency_pass_rate"]
    assert first["hard_issue_count"] == 0
    assert first["maximum_component_profit_share"] == pytest.approx(0.1)
    assert len(first["design_behavior_fingerprint"]) == 64
    assert first["immutable_ledger_only"] is True
    assert first["underlying_signals_recomputed"] is False


def test_optional_time_to_combine_report_uses_every_frozen_horizon() -> None:
    result = evaluate_policy_block(
        _static_policy(),
        _runtimes(),
        risk_level="0.75x",
        block_id="B1",
        session_days=tuple(range(100)),
        start_days=(0,),
        include_time_to_combine=True,
    )

    assert tuple(result["time_to_combine"]["normal"]) == (
        "20",
        "40",
        "60",
        "90",
        "full_available",
    )
    assert result["time_to_combine"]["normal"]["40"]["pass_count"] == (
        result["normal_pass_count"]
    )
    assert result["time_to_combine"]["stress_1_5x"]["20"][
        "operational_horizon_not_reached_count"
    ] == 1


def test_hard_rule_failure_is_exposed_as_a_selector_integrity_issue() -> None:
    runtimes = _runtimes()
    runtimes[COMPONENT_IDS[0]] = _runtime(
        COMPONENT_IDS[0], 0, session_compliant=False
    )
    result = evaluate_policy_block(
        _static_policy(),
        runtimes,
        risk_level="0.75x",
        block_id="B1",
        session_days=tuple(range(50)),
        start_days=(0,),
    )

    assert result["normal_hard_rule_failure_count"] == 1
    assert result["stressed_hard_rule_failure_count"] == 1
    assert result["hard_issue_count"] == 2


def test_design_aggregation_enforces_heldout_isolation_and_block_concentration() -> None:
    policy = _static_policy()
    runtimes = _runtimes()
    b1 = evaluate_policy_block(
        policy,
        runtimes,
        risk_level="0.75x",
        block_id="B1",
        session_days=tuple(range(50)),
        start_days=(0,),
    )
    b2 = evaluate_policy_block(
        policy,
        runtimes,
        risk_level="0.75x",
        block_id="B2",
        session_days=tuple(range(60, 110)),
        start_days=(60,),
    )
    b3 = replace(_runtime(COMPONENT_IDS[0], 0), net_pnl=999_999.0)
    assert b3.net_pnl == 999_999.0  # held-out mutation is never consulted below

    design = aggregate_design_block_metrics(
        (b2, b1),
        allowed_block_ids=("B1", "B2"),
        heldout_block_id="B3",
    )

    assert design["design_block_ids"] == ["B1", "B2"]
    assert design["heldout_block_id"] == "B3"
    assert design["episode_count"] == 2
    assert design["normal_net_usd"] == pytest.approx(12_000.0)
    assert design["stressed_net_usd"] == pytest.approx(9_600.0)
    assert design["maximum_block_profit_share"] == pytest.approx(0.5)
    assert design["maximum_component_profit_share"] == pytest.approx(0.1)
    assert design["positive_temporal_block_count"] == 2
    assert len(design["design_behavior_fingerprint"]) == 64
    assert design["stressed_pass_count"] == design["stress_pass_count"]
    assert design["stressed_target_progress"] == (
        design["stressed_target_progress_median"]
    )
    assert design["mll_breach_count"] == 0
    assert design["consistency"] == design["consistency_pass_rate"]

    with pytest.raises(SelectorEvaluationError, match="held-out evidence"):
        aggregate_design_block_metrics(
            (b1, b2, {**b1, "block_id": "B3"}),
            allowed_block_ids=("B1", "B2"),
            heldout_block_id="B3",
        )
    with pytest.raises(SelectorEvaluationError, match="cannot be allowed"):
        aggregate_design_block_metrics(
            (b1, b2),
            allowed_block_ids=("B1", "B2", "B3"),
            heldout_block_id="B3",
        )
