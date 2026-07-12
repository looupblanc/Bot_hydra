from __future__ import annotations

import numpy as np

from hydra.account_policy.basket import RoutedTrade
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.validation.v7_null_tripwire import (
    NullControl,
    SyntheticMarketPath,
    block_shuffle_source_days,
    null_verdict,
    rebuild_counterfactual_trade,
    year_permutation_source_days,
)


def test_daily_block_shuffle_is_deterministic_nonidentity_and_block_preserving() -> None:
    days = np.arange(40, dtype=np.int64)
    first = block_shuffle_source_days(
        days, block_size=5, rng=np.random.default_rng(71001)
    )
    second = block_shuffle_source_days(
        days, block_size=5, rng=np.random.default_rng(71001)
    )

    assert np.array_equal(first, second)
    assert not np.array_equal(first, days)
    assert sorted(first.tolist()) == days.tolist()
    assert all(
        np.all(np.diff(first[index : index + 5]) == 1)
        for index in range(0, len(first), 5)
    )


def test_year_permutation_rotates_whole_year_blocks_without_dropping_days() -> None:
    days = np.asarray(
        [
            int(np.datetime64(value, "D").astype(np.int64))
            for value in (
                "2023-01-03",
                "2023-01-04",
                "2024-01-02",
                "2024-01-03",
            )
        ]
    )

    result = year_permutation_source_days(days)

    assert result.tolist() == [days[2], days[3], days[0], days[1]]
    assert sorted(result.tolist()) == sorted(days.tolist())


def test_counterfactual_trade_preserves_cost_and_frozen_signal_timestamps() -> None:
    timestamp = np.asarray([1, 61, 121, 181], dtype=np.int64) * 1_000_000_000
    path = SyntheticMarketPath(
        market="ES",
        control=NullControl.VOLATILITY_MATCHED_RANDOM_WALK,
        timestamp_ns=timestamp,
        session_day=np.asarray([1, 1, 1, 1]),
        segment_code=np.asarray([7, 7, 7, 7]),
        close=np.asarray([0.0, 1.0, 3.0, 2.0]),
        high=np.asarray([0.5, 1.5, 3.5, 2.5]),
        low=np.asarray([-0.5, 0.5, 2.5, 1.5]),
        path_hash="f" * 64,
    )
    event = TradePathEvent(
        event_id="alpha:0",
        decision_ns=int(timestamp[1]),
        exit_ns=int(timestamp[3] + 60_000_000_000),
        session_day=1,
        net_pnl=80.0,
        gross_pnl=100.0,
        worst_unrealized_pnl=-50.0,
        best_unrealized_pnl=120.0,
        quantity=1,
        mini_equivalent=1.0,
    )
    routed = RoutedTrade("alpha", "ES", 1, event)

    rebuilt = rebuild_counterfactual_trade(
        routed, path, point_value=50.0
    )

    assert rebuilt.event.decision_ns == event.decision_ns
    assert rebuilt.event.exit_ns == event.exit_ns
    assert rebuilt.event.gross_pnl == 50.0
    assert rebuilt.event.net_pnl == 30.0
    assert rebuilt.event.best_unrealized_pnl == 105.0
    assert rebuilt.event.worst_unrealized_pnl == -45.0


def test_null_ratio_threshold_is_frozen_at_point_eight() -> None:
    assert null_verdict(0.5, 0.399)[0] == "GREEN"
    assert null_verdict(0.5, 0.4) == ("ARTEFACT", 0.8)
    assert null_verdict(0.0, 0.0) == ("BLOCKED_UNDEFINED_RATIO", None)
