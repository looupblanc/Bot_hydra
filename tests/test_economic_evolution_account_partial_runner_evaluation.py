from __future__ import annotations

import numpy as np
import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.economic_evolution.account_partial_runner_evaluation import (
    _partial_runner_event_from_parent,
)
from hydra.propfirm.combine_episode import TradePathEvent


def _parent_event() -> RoutedTrade:
    return RoutedTrade(
        component_id="alpha",
        market="MES",
        side=1,
        event=TradePathEvent(
            event_id="alpha:0",
            decision_ns=1,
            exit_ns=10,
            session_day=1,
            net_pnl=14.0,
            gross_pnl=15.0,
            worst_unrealized_pnl=-6.0,
            best_unrealized_pnl=16.0,
            quantity=1,
            mini_equivalent=0.1,
        ),
    )


def test_partial_runner_uses_two_costed_lots_and_conservative_same_bar_path() -> None:
    result = _partial_runner_event_from_parent(
        _parent_event(),
        entry_price=100.0,
        side=1,
        point_value=5.0,
        target_distance=2.0,
        path_highs=np.asarray([101.0, 102.5, 103.0]),
        path_lows=np.asarray([99.0, 98.0, 100.0]),
        target_hit_offset=1,
    )
    # One lot exits at +2 points ($10), one retains the parent's $15 gross;
    # both pay the frozen $1 round turn.
    assert result.event.gross_pnl == pytest.approx(25.0)
    assert result.event.net_pnl == pytest.approx(23.0)
    assert result.event.quantity == 2
    assert result.event.mini_equivalent == pytest.approx(0.2)
    # The target bar also traded below entry. Conservative ordering retains
    # the two-lot adverse excursion before allowing the target fill.
    assert result.event.worst_unrealized_pnl == pytest.approx(-22.0)
    assert result.event.same_bar_ambiguous is True


def test_partial_runner_without_target_hit_equals_two_lot_time_exit() -> None:
    result = _partial_runner_event_from_parent(
        _parent_event(),
        entry_price=100.0,
        side=1,
        point_value=5.0,
        target_distance=4.0,
        path_highs=np.asarray([101.0, 102.0, 103.0]),
        path_lows=np.asarray([99.0, 99.5, 100.0]),
        target_hit_offset=None,
    )
    assert result.event.gross_pnl == pytest.approx(30.0)
    assert result.event.net_pnl == pytest.approx(28.0)
    assert result.event.worst_unrealized_pnl == pytest.approx(-12.0)
    assert result.event.best_unrealized_pnl == pytest.approx(32.0)
