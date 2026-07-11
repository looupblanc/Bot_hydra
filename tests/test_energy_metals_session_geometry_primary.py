from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.energy_metals_session_geometry_primary import (
    build_session_geometry_events,
    build_session_geometry_table,
    generate_session_geometry_hypotheses,
)


def _bars_for_session(date: str, base: float) -> pd.DataFrame:
    current = pd.Timestamp(date, tz="America/Chicago")
    overnight = pd.date_range(
        current - pd.Timedelta(days=1) + pd.Timedelta(hours=18),
        periods=30,
        freq="1min",
    )
    rth = pd.date_range(current + pd.Timedelta(hours=8), periods=151, freq="1min")
    timestamp = overnight.append(rth).tz_convert("UTC")
    values = base + np.linspace(0.0, 1.8, len(timestamp))
    return pd.DataFrame(
        {
            "timestamp": timestamp,
            "symbol": "CL",
            "active_contract": "CLH4",
            "open": values,
            "high": values + 0.05,
            "low": values - 0.05,
            "close": values + 0.01,
            "volume": np.arange(len(timestamp)) + 1,
        }
    )


def test_population_is_exact_unique_and_balanced() -> None:
    population = generate_session_geometry_hypotheses()

    assert len(population) == 432
    assert len({item["candidate_id"] for item in population}) == 432
    assert len({item["structural_fingerprint"] for item in population}) == 432
    assert {item["market"] for item in population} == {"CL", "GC"}
    assert {item["execution_market"] for item in population} == {"MCL", "MGC"}
    assert {item["feature"] for item in population} == {
        "overnight_displacement",
        "opening_impulse_15",
        "opening_impulse_30",
        "overnight_extreme_position",
        "opening_efficiency_15",
        "opening_volume_surprise_15",
    }


def test_session_features_shift_prior_rth_and_close_opening_windows() -> None:
    raw = pd.concat(
        [_bars_for_session("2024-01-03", 70.0), _bars_for_session("2024-01-04", 72.0)],
        ignore_index=True,
    )

    table = build_session_geometry_table(raw, "CL")

    assert len(table) == 2
    assert pd.isna(table.iloc[0]["prior_close"])
    assert table.iloc[1]["prior_close"] == table.iloc[0]["rth_close"]
    assert table.iloc[1]["overnight_displacement"] == (
        table.iloc[1]["rth_open"] - table.iloc[0]["rth_close"]
    ) / (table.iloc[0]["rth_high"] - table.iloc[0]["rth_low"])
    assert table.iloc[0]["open15_entry_timestamp"] > pd.Timestamp(
        "2024-01-03 08:14", tz="America/Chicago"
    ).tz_convert("UTC")


def test_event_threshold_is_past_only_and_one_per_session() -> None:
    sessions = 16
    timestamps = pd.date_range("2023-01-03T14:01:00Z", periods=sessions, freq="1D")
    table = pd.DataFrame(
        {
            "session_id": timestamps.date.astype(str),
            "symbol": ["MCL"] * sessions,
            "active_contract": ["MCLH3"] * sessions,
            "prior_trend": np.ones(sessions),
            "overnight_displacement": np.arange(1, sessions + 1, dtype=float),
            "opening_impulse_15": np.ones(sessions),
            "overnight_entry_timestamp": timestamps,
            "overnight_entry_price": np.full(sessions, 70.0),
            "overnight_exit_timestamp_30": timestamps + pd.Timedelta(minutes=30),
            "overnight_exit_30": np.full(sessions, 70.2),
            "overnight_long_mae_30": np.full(sessions, -0.1),
            "overnight_short_mae_30": np.full(sessions, -0.3),
        }
    )
    hypothesis = next(
        item
        for item in generate_session_geometry_hypotheses()
        if item["market"] == "CL"
        and item["feature"] == "overnight_displacement"
        and item["policy_direction"] == "continuation"
        and item["quantile"] == 0.65
        and item["horizon"] == 30
        and item["context"] == "none"
    )

    events = build_session_geometry_events(table, {**hypothesis, "market": "MCL"})

    assert events["trading_session_id"].is_unique
    assert events["entry_timestamp"].min() >= timestamps[10]
    assert (events["net_pnl"] < events["gross_pnl"]).all()
