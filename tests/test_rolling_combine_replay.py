from __future__ import annotations

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.rolling_combine_replay import _day_regimes, build_exact_trade_path
from hydra.strategies.turbo_dsl import (
    ComparisonOperator,
    StrategyRole,
    StrategySpec,
)


def _matrix() -> FeatureMatrix:
    rows = 400
    timestamp = np.arange(rows, dtype=np.int64) * 60_000_000_000
    session_day = (np.arange(rows) // 10).astype(np.int32)
    feature = np.zeros(rows, dtype=float)
    feature[::20] = 1.0
    forward = np.full(rows, np.nan, dtype=float)
    forward[:-6] = 2.0
    entry = np.full(rows, 100.0)
    high = np.full(rows, 102.5)
    low = np.full(rows, 99.5)
    arrays = {
        "timestamp_ns": timestamp,
        "decision_ns": timestamp + 60_000_000_000,
        "availability_ns": timestamp + 60_000_000_000,
        "segment_code": np.zeros(rows, dtype=np.int64),
        "session_day": session_day,
        "session_code": np.zeros(rows, dtype=np.int16),
        "entry_price": entry,
        "bar_high": high,
        "bar_low": low,
        "bar_close": np.full(rows, 100.0),
        "feature__path_efficiency": feature,
        "feature__ctx_60m_volatility_expansion": np.ones(rows, dtype=float),
        "forward_move__5": forward,
    }
    for value in arrays.values():
        value.flags.writeable = False
    return FeatureMatrix(
        root=None,  # type: ignore[arg-type]
        manifest={"row_count": rows, "bundle_hash": "synthetic"},
        arrays=arrays,
    )


def _spec() -> StrategySpec:
    return StrategySpec(
        candidate_id="synthetic",
        lineage_id="synthetic-lineage",
        family="market_state_geometry",
        market="NQ",
        timeframe="1m",
        feature="path_efficiency",
        operator=ComparisonOperator.GREATER_EQUAL,
        threshold=1.0,
        side=1,
        holding_events=5,
        point_value=20.0,
        round_turn_cost=10.0,
        role=StrategyRole.COMBINE_PASSER,
    )


def test_exact_trade_path_includes_costs_unrealized_path_and_chronology() -> None:
    path = build_exact_trade_path(
        _spec(), _matrix(), start_inclusive="1970-01-01", end_exclusive="1970-03-01"
    )
    assert path.event_count > 0
    assert all(
        left.exit_ns <= right.decision_ns
        for left, right in zip(path.events, path.events[1:])
    )
    first = path.events[0]
    assert first.gross_pnl == 40.0
    assert first.net_pnl == 30.0
    assert first.worst_unrealized_pnl == -20.0
    assert first.best_unrealized_pnl == 40.0
    assert path.cost_stress_1_5x_net < path.net_pnl
    assert set(path.fold_results) == {
        "2023_h1",
        "2023_h2",
        "2024_q1",
        "2024_q2",
        "2024_q3",
    }


def test_episode_start_regime_uses_only_previous_completed_session() -> None:
    matrix = _matrix()
    session_days = matrix.array("session_day")
    volatility = matrix.array("feature__ctx_60m_volatility_expansion").copy()
    volatility[session_days == 0] = 1.50
    volatility[session_days == 1] = 0.50
    volatility[session_days == 2] = 1.00
    arrays = dict(matrix.arrays)
    arrays["feature__ctx_60m_volatility_expansion"] = volatility
    shifted = FeatureMatrix(root=None, manifest=matrix.manifest, arrays=arrays)  # type: ignore[arg-type]
    selected = np.ones(shifted.row_count, dtype=bool)

    regimes = _day_regimes(shifted, selected)

    assert regimes[0] == "UNKNOWN"
    assert regimes[1] == "VOLATILITY_EXPANSION"
    assert regimes[2] == "VOLATILITY_CONTRACTION"

    # Future observations within day 1 may alter day 2's state, never day 1's
    # already selected start regime.
    changed = volatility.copy()
    changed[session_days == 1] = 2.00
    arrays["feature__ctx_60m_volatility_expansion"] = changed
    changed_matrix = FeatureMatrix(  # type: ignore[arg-type]
        root=None, manifest=matrix.manifest, arrays=arrays
    )
    changed_regimes = _day_regimes(changed_matrix, selected)
    assert changed_regimes[1] == regimes[1]
    assert changed_regimes[2] == "VOLATILITY_EXPANSION"
