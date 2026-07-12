from __future__ import annotations

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.account_policy.xfa import evaluate_serial_xfa_basket
from hydra.propfirm.combine_episode import TradePathEvent


def _trade(component: str, market: str, day: int, offset: int = 0) -> RoutedTrade:
    decision = day * 1_000_000 + offset
    return RoutedTrade(
        component_id=component,
        market=market,
        side=1,
        event=TradePathEvent(
            event_id=f"{component}:{day}:{offset}",
            decision_ns=decision,
            exit_ns=decision + 100,
            session_day=day,
            net_pnl=250.0,
            gross_pnl=260.0,
            worst_unrealized_pnl=-100.0,
            best_unrealized_pnl=300.0,
            quantity=1,
            mini_equivalent=1.0,
            regime="VOLATILITY_NORMAL",
        ),
    )


def test_xfa_basket_uses_global_serial_routing_and_no_status_inheritance() -> None:
    days = tuple(range(220))
    events = {
        "left": tuple(_trade("left", "ES", day) for day in days),
        "right": tuple(_trade("right", "CL", day, 10) for day in days),
    }
    basket = BasketPolicy(
        policy_id="xfa-basket",
        component_ids=("left", "right"),
        archetype="XFA_PAYOUT",
        component_priority=("left", "right"),
    )

    result = evaluate_serial_xfa_basket(
        events, days, basket=basket, maximum_starts=12
    )

    assert result["routing_policy"] == "GLOBAL_SERIAL_FIXED_PRIORITY"
    assert result["accepted_event_count"] == 220
    assert result["skipped_event_count"] == 220
    assert result["skipped_reasons"] == {"GLOBAL_SERIAL_CONFLICT": 220}
    assert result["inherited_status"] is False
    assert result["outbound_order_capability"] is False
    assert result["rolling_xfa"]["episode_start_count"] > 0
    assert result["rolling_xfa"]["payout_probability"] > 0.0
