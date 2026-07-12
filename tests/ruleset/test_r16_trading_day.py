from __future__ import annotations

import pandas as pd

from hydra.propfirm.topstep_150k import (
    InternalRiskOverlay,
    trades_to_topstep_daily,
)
from hydra.propfirm.trading_day import (
    is_allowed_entry_timestamp,
    is_winning_day_locked,
    trading_day_for_timestamp,
)


def test_r16_trading_day_rolls_at_1700_ct_in_winter_and_summer() -> None:
    winter_before = trading_day_for_timestamp(
        pd.Timestamp("2024-01-03T22:59:00Z")
    )
    winter_at = trading_day_for_timestamp(
        pd.Timestamp("2024-01-03T23:00:00Z")
    )
    summer_before = trading_day_for_timestamp(
        pd.Timestamp("2024-07-02T21:59:00Z")
    )
    summer_at = trading_day_for_timestamp(
        pd.Timestamp("2024-07-02T22:00:00Z")
    )

    assert winter_before.trading_day == "2024-01-03"
    assert winter_at.trading_day == "2024-01-04"
    assert summer_before.trading_day == "2024-07-02"
    assert summer_at.trading_day == "2024-07-03"


def test_r16_winning_day_locks_at_1600_ct_and_flatten_is_1510_ct() -> None:
    before_lock = pd.Timestamp("2024-07-02T20:59:00Z")
    at_lock = pd.Timestamp("2024-07-02T21:00:00Z")
    at_flatten = pd.Timestamp("2024-07-02T20:10:00Z")

    assert not is_winning_day_locked(before_lock)
    assert is_winning_day_locked(at_lock)
    assert not is_allowed_entry_timestamp(at_flatten)


def test_legacy_daily_pnl_is_grouped_by_ct_trading_day_not_utc_date() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-03T22:59:00Z",
                    "2024-01-03T23:00:00Z",
                ],
                utc=True,
            )
        }
    )
    trades = [
        {"exit_i": 0, "pnl": 100.0, "mae": -10.0},
        {"exit_i": 1, "pnl": 200.0, "mae": -10.0},
    ]
    daily = trades_to_topstep_daily(
        trades,
        frame,
        InternalRiskOverlay(daily_stop=3000.0, daily_profit_lock=5000.0),
    )

    assert daily["date"].tolist() == ["2024-01-03", "2024-01-04"]
    assert daily["pnl"].tolist() == [100.0, 200.0]
