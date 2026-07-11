from __future__ import annotations

import math

import numpy as np
import pandas as pd

from hydra.research.barrier_hazard_primary import (
    PRIMARY_ALPHA,
    add_barrier_features,
    barrier_hazard_metrics,
    generate_barrier_hypotheses,
    resolve_barrier_events,
)


def test_barrier_population_is_exact_balanced_and_deterministic() -> None:
    first = generate_barrier_hypotheses()
    second = generate_barrier_hypotheses()

    assert first == second
    assert len(first) == 144
    assert len({item["structural_fingerprint"] for item in first}) == 144
    assert all(
        sum(item["market"] == market for item in first) == 24
        for market in ("ES", "NQ", "RTY", "YM", "GC", "CL")
    )


def test_barrier_features_and_scale_are_future_invariant() -> None:
    size = 280
    base = pd.DataFrame(
        {
            "contiguous_segment_id": [1] * size,
            "close": 100 + np.sin(np.arange(size) / 9) + np.arange(size) * 0.01,
            "high": 100.5 + np.sin(np.arange(size) / 9) + np.arange(size) * 0.01,
            "low": 99.5 + np.sin(np.arange(size) / 9) + np.arange(size) * 0.01,
        }
    )
    altered = base.copy()
    altered.loc[240:, ["close", "high", "low"]] += 1000

    first = add_barrier_features(base)
    second = add_barrier_features(altered)
    columns = [
        "signed_close_location_persistence_30",
        "range_acceleration_15_120",
        "return_sign_persistence_30",
        "signed_extreme_recovery_60",
        "barrier_range_scale",
    ]
    pd.testing.assert_frame_equal(first.loc[:239, columns], second.loc[:239, columns])


def _event(side: int = 1) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2024-01-02T14:58:00Z"),
                "symbol": "ES",
                "active_contract": "ESH4",
                "trading_session_id": "2024-01-02",
                "contiguous_segment_id": 1,
                "entry_timestamp": pd.Timestamp("2024-01-02T15:00:00Z"),
                "entry_price": 100.0,
                "exit_timestamp": pd.Timestamp("2024-01-02T15:03:00Z"),
                "exit_price": 100.0,
                "side": side,
                "point_value": 50.0,
                "cost": 2.0,
                "holding_horizon_minutes": 3,
                "barrier_range_scale": math.sqrt(5.0),
            }
        ]
    )


def _cache(
    opens: list[float], highs: list[float], lows: list[float], closes: list[float]
) -> dict:
    timestamps = pd.date_range("2024-01-02T15:00:00Z", periods=len(opens), freq="1min")
    return {
        ("ES", "ESH4", "2024-01-02", 1): (
            timestamps.tz_localize(None).to_numpy(dtype="datetime64[ns]").astype(np.int64),
            np.asarray(opens, dtype=float),
            np.asarray(highs, dtype=float),
            np.asarray(lows, dtype=float),
            np.asarray(closes, dtype=float),
        )
    }


def test_barrier_target_first_and_timeout_are_resolved() -> None:
    target = resolve_barrier_events(
        _event(),
        _cache(
            [100, 100.2, 100.4, 100.5],
            [100.5, 101.2, 100.8, 100.7],
            [99.6, 99.8, 100.0, 100.1],
            [100.2, 101.0, 100.5, 100.4],
        ),
        barrier_scale_multiplier=1.0,
    )
    timeout = resolve_barrier_events(
        _event(),
        _cache(
            [100, 100.1, 100.2, 100.3],
            [100.4, 100.5, 100.6, 100.7],
            [99.7, 99.8, 99.9, 100.0],
            [100.1, 100.2, 100.3, 100.4],
        ),
        barrier_scale_multiplier=1.0,
    )

    assert target.iloc[0]["barrier_outcome"] == "TARGET_FIRST"
    assert target.iloc[0]["exit_price"] == 101.0
    assert timeout.iloc[0]["barrier_outcome"] == "TIMEOUT"
    assert timeout.iloc[0]["exit_price"] == 100.3


def test_same_bar_ambiguity_is_stop_first_and_gap_stop_is_worse_fill() -> None:
    ambiguous = resolve_barrier_events(
        _event(),
        _cache(
            [100, 100, 100, 100],
            [102, 100.5, 100.5, 100.5],
            [98, 99.5, 99.5, 99.5],
            [100, 100, 100, 100],
        ),
        barrier_scale_multiplier=1.0,
    )
    gap = resolve_barrier_events(
        _event(),
        _cache(
            [100, 98, 100, 100],
            [100.5, 99, 100.5, 100.5],
            [99.5, 97.5, 99.5, 99.5],
            [100, 98.5, 100, 100],
        ),
        barrier_scale_multiplier=1.0,
    )

    assert ambiguous.iloc[0]["barrier_outcome"] == "STOP_FIRST"
    assert bool(ambiguous.iloc[0]["barrier_ambiguous_stop_first"])
    assert ambiguous.iloc[0]["exit_price"] == 99.0
    assert gap.iloc[0]["barrier_outcome"] == "STOP_FIRST"
    assert gap.iloc[0]["exit_price"] == 98.0


def test_barrier_hazard_exact_null_uses_resolved_events_only() -> None:
    events = pd.DataFrame(
        {
            "barrier_outcome": ["TARGET_FIRST"] * 9
            + ["STOP_FIRST"]
            + ["TIMEOUT"] * 3,
            "barrier_ambiguous_stop_first": [False] * 9 + [True] + [False] * 3,
        }
    )
    metrics = barrier_hazard_metrics(events)

    assert metrics["resolved_barrier_events"] == 10
    assert metrics["target_first_probability"] == 0.9
    assert metrics["ambiguous_stop_first"] == 1
    assert metrics["exact_probability"] <= PRIMARY_ALPHA
