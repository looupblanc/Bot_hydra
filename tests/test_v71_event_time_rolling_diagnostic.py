from __future__ import annotations

from hydra.propfirm.combine_episode import TradePathEvent
from hydra.validation.v71_event_time_rolling_diagnostic import (
    serialize_account_events,
)


def _event(event_id: str, start: int, end: int) -> TradePathEvent:
    return TradePathEvent(
        event_id=event_id,
        decision_ns=start,
        exit_ns=end,
        session_day=1,
        net_pnl=10.0,
        gross_pnl=20.0,
        worst_unrealized_pnl=-5.0,
        best_unrealized_pnl=15.0,
        quantity=1,
        mini_equivalent=1.0,
    )


def test_serialized_basket_blocks_overlapping_component_signal() -> None:
    accepted, audit = serialize_account_events(
        {
            "a": (_event("a1", 10, 30),),
            "b": (_event("b1", 20, 40), _event("b2", 40, 50)),
        }
    )
    assert len(accepted) == 2
    assert audit["blocked_conflict_count"] == 1
    assert audit["blocked_by_candidate"] == {"b": 1}
