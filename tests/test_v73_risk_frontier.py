from __future__ import annotations

from dataclasses import replace

import pytest

import hydra.account_policy.basket as basket_engine
from hydra.account_policy.router import AccountDecisionState, EntryIntent, OpenExposure
from hydra.selection.risk_frontier import (
    FROZEN_RISK_TIERS,
    STATIC_CONFLICT_POLICY,
    StaticIntegerMicroPolicy,
    adapt_static_risk_policy,
    equal_risk_integer_units,
    resolve_static_risk_tier,
    route_static_integer_micro_entry,
    static_risk_router_context,
)


def _source() -> dict[str, object]:
    return {
        "policy_id": "static_parent_basket_test",
        "component_ids": ["alpha", "defensive"],
        "daily_loss_guard": 750.0,
        "daily_profit_lock": 2_000.0,
        "critical_buffer": 800.0,
        "maximum_simultaneous_positions": 2,
        "maximum_mini_equivalent": 1.5,
        "conflict_policy": STATIC_CONFLICT_POLICY,
    }


def _intent(*, component: str = "alpha", market: str = "MNQ") -> EntryIntent:
    return EntryIntent(
        event_id="event-1",
        component_id=component,
        market=market,
        side=1,
        decision_ns=1_000,
        session_day=20260701,
        regime="NEUTRAL",
        base_quantity=1,
        base_mini_equivalent=0.1,
    )


def _state(
    *,
    daily_pnl: float = 0.0,
    buffer: float = 2_000.0,
    losing_days: int = 0,
    exposures: tuple[OpenExposure, ...] = (),
) -> AccountDecisionState:
    return AccountDecisionState(
        balance=150_000.0,
        mll_floor=150_000.0 - buffer,
        mll_buffer=buffer,
        daily_realized_pnl=daily_pnl,
        consecutive_losing_days=losing_days,
        remaining_target=9_000.0,
        open_exposures=exposures,
    )


def test_frontier_is_exactly_the_four_preregistered_integer_micro_tiers() -> None:
    assert [row.label for row in FROZEN_RISK_TIERS] == [
        "0.75x",
        "1.00x",
        "1.25x",
        "1.50x",
    ]
    assert [row.micro_risk_units for row in FROZEN_RISK_TIERS] == [3, 4, 5, 6]
    assert resolve_static_risk_tier(1.25).micro_risk_units == 5
    assert resolve_static_risk_tier("1.50X").micro_risk_units == 6
    with pytest.raises(ValueError, match="outside the frozen frontier"):
        resolve_static_risk_tier(1.15)


def test_adapter_retains_account_guards_and_declares_no_dynamic_sizing() -> None:
    policy = adapt_static_risk_policy(_source(), "1.25x")

    assert isinstance(policy, StaticIntegerMicroPolicy)
    assert policy.component_priority == ("alpha", "defensive")
    assert policy.micro_risk_units == 5
    assert policy.daily_loss_guard == 750.0
    assert policy.daily_profit_lock == 2_000.0
    assert policy.critical_buffer == 800.0
    assert policy.maximum_simultaneous_positions == 2
    assert policy.maximum_mini_equivalent == 1.5
    assert policy.dynamic_buffer_sizing is False
    assert policy.loss_streak_sizing is False
    assert policy.structural_fingerprint == adapt_static_risk_policy(
        _source(), "1.25x"
    ).structural_fingerprint


def test_router_uses_constant_tier_despite_buffer_zone_or_loss_streak() -> None:
    policy = adapt_static_risk_policy(_source(), "1.50x")

    high_buffer = route_static_integer_micro_entry(
        _intent(), _state(buffer=10_000.0, losing_days=0), policy=policy
    )
    low_buffer = route_static_integer_micro_entry(
        _intent(), _state(buffer=801.0, losing_days=99), policy=policy
    )

    assert high_buffer == low_buffer
    assert high_buffer.allow is True
    assert high_buffer.quantity == 6
    assert high_buffer.mini_equivalent == pytest.approx(0.6)
    assert high_buffer.reason == "STATIC_INTEGER_MICRO_UNITS_6"


@pytest.mark.parametrize(
    ("state", "expected_reason"),
    [
        (_state(daily_pnl=-750.0), "DAILY_LOSS_GUARD"),
        (_state(daily_pnl=2_000.0), "DAILY_PROFIT_LOCK"),
        (_state(buffer=800.0), "CRITICAL_MLL_BUFFER"),
        (
            _state(
                exposures=(
                    OpenExposure("other", "MNQ", -1, 0.1, 2_000),
                )
            ),
            "SAME_MARKET_CONFLICT",
        ),
        (
            _state(
                exposures=(
                    OpenExposure("one", "MES", 1, 0.1, 2_000),
                    OpenExposure("two", "MCL", 1, 0.1, 2_000),
                )
            ),
            "MAXIMUM_SIMULTANEOUS_POSITIONS",
        ),
    ],
)
def test_router_retains_frozen_account_guards(
    state: AccountDecisionState, expected_reason: str
) -> None:
    policy = adapt_static_risk_policy(_source(), "1.00x")
    decision = route_static_integer_micro_entry(_intent(), state, policy=policy)
    assert decision.allow is False
    assert decision.reason == expected_reason


def test_router_blocks_risk_tier_that_would_exceed_shared_mini_cap() -> None:
    policy = replace(
        adapt_static_risk_policy(_source(), "1.50x"),
        maximum_mini_equivalent=0.5,
    )
    decision = route_static_integer_micro_entry(
        _intent(), _state(), policy=policy
    )
    assert decision.allow is False
    assert decision.reason == "SHARED_CONTRACT_LIMIT"


def test_equal_risk_units_are_integer_equal_and_conservatively_capped() -> None:
    assert equal_risk_integer_units(
        ("alpha", "defensive"), risk_level="1.50x"
    ) == {"alpha": 6, "defensive": 6}
    assert equal_risk_integer_units(
        ("alpha", "defensive"),
        risk_level="1.50x",
        per_component_unit_caps={"alpha": 6, "defensive": 4},
    ) == {"alpha": 6, "defensive": 4}
    assert equal_risk_integer_units(
        ("alpha", "defensive"),
        risk_level="1.50x",
        base_mini_equivalents={"alpha": 0.1, "defensive": 0.25},
        maximum_mini_equivalent=1.2,
        maximum_simultaneous_positions=2,
    ) == {"alpha": 6, "defensive": 2}


def test_router_context_is_usable_by_account_basket_engine_and_restores() -> None:
    policy = adapt_static_risk_policy(_source(), "0.75x")
    prior = basket_engine.route_entry

    with static_risk_router_context():
        assert basket_engine.route_entry is not prior
        decision = basket_engine.route_entry(_intent(), _state(), policy=policy)
        assert decision.allow is True
        assert decision.quantity == 3

    assert basket_engine.route_entry is prior
