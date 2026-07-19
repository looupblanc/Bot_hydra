from __future__ import annotations

import pandas as pd
import pytest

from hydra.research.cl_front_second_term_structure_tripwire import (
    causal_intent,
    frozen_rule_specs,
    next_tradable_open,
    prepare_causal_source_features,
    validate_decision_columns,
)


def _rank_frame(*, offset: float, periods: int = 80) -> pd.DataFrame:
    timestamp = pd.date_range("2024-01-02 14:00:00Z", periods=periods, freq="1min")
    return pd.DataFrame(
        {
            "timestamp": timestamp,
            "available_at": timestamp + pd.Timedelta(minutes=1),
            "session_id": timestamp.strftime("%Y-%m-%d"),
            "close": [70.0 + offset + index * 0.01 + (index % 7) * 0.002 for index in range(periods)],
            "roll_unsafe": False,
        }
    )


def test_rule_lattice_is_bounded_and_unique() -> None:
    rules = frozen_rule_specs()
    assert len(rules) == 8
    assert len({rule.rule_id for rule in rules}) == 8
    assert {rule.mechanism for rule in rules} == {
        "BASIS_RESIDUAL_CONTINUATION",
        "BASIS_RESIDUAL_REVERSION",
    }
    assert {rule.execution_market for rule in rules} == {"MCL.c.0"}
    assert {rule.fill_policy for rule in rules} == {"NEXT_TRADABLE_OPEN"}


def test_appending_a_future_bar_cannot_change_prior_features() -> None:
    front = _rank_frame(offset=0.0)
    second = _rank_frame(offset=0.5)
    before = prepare_causal_source_features(front, second, lookback_minutes=15)
    next_time = front["timestamp"].iloc[-1] + pd.Timedelta(minutes=1)
    extra_front = pd.concat(
        [
            front,
            pd.DataFrame(
                {
                    "timestamp": [next_time],
                    "available_at": [next_time + pd.Timedelta(minutes=1)],
                    "session_id": [next_time.strftime("%Y-%m-%d")],
                    "close": [999.0],
                    "roll_unsafe": [False],
                }
            ),
        ],
        ignore_index=True,
    )
    extra_second = pd.concat(
        [
            second,
            pd.DataFrame(
                {
                    "timestamp": [next_time],
                    "available_at": [next_time + pd.Timedelta(minutes=1)],
                    "session_id": [next_time.strftime("%Y-%m-%d")],
                    "close": [1.0],
                    "roll_unsafe": [False],
                }
            ),
        ],
        ignore_index=True,
    )
    after = prepare_causal_source_features(extra_front, extra_second, lookback_minutes=15)
    pd.testing.assert_frame_equal(before, after.iloc[:-1].reset_index(drop=True))


def test_lookback_is_exact_elapsed_minutes_and_never_crosses_a_gap() -> None:
    front = _rank_frame(offset=0.0, periods=100).drop(index=30).reset_index(drop=True)
    second = _rank_frame(offset=0.5, periods=100).drop(index=30).reset_index(drop=True)
    result = prepare_causal_source_features(front, second, lookback_minutes=15)
    at_45 = result.loc[result["timestamp"].eq(pd.Timestamp("2024-01-02T14:45:00Z"))].iloc[0]
    at_46 = result.loc[result["timestamp"].eq(pd.Timestamp("2024-01-02T14:46:00Z"))].iloc[0]
    assert pd.isna(at_45["basis_change"])
    assert pd.notna(at_46["basis_change"])
    assert int(at_45["causal_segment_id"]) == int(at_46["causal_segment_id"])


def test_delivery_and_spread_volatility_state_are_explicit() -> None:
    front = _rank_frame(offset=0.0, periods=100)
    second = _rank_frame(offset=0.5, periods=100)
    front["rank_contract"] = "front_contract"
    second["rank_contract"] = "second_contract"
    front["days_to_delivery"] = 20.0
    second["days_to_delivery"] = 50.0
    result = prepare_causal_source_features(front, second, lookback_minutes=15)
    assert {
        "front_days_to_delivery",
        "second_days_to_delivery",
        "delivery_tenor_gap_days",
        "roll_distance_adjusted_basis",
        "roll_distance_adjusted_basis_innovation",
        "current_spread_state",
        "front_prior_realized_volatility",
    }.issubset(result.columns)
    assert result["delivery_tenor_gap_days"].eq(30.0).all()


def test_roll_guard_abstains_and_mechanism_directions_are_opposite() -> None:
    rules = frozen_rule_specs()
    continuation = next(rule for rule in rules if rule.mechanism.endswith("CONTINUATION"))
    reversion = next(rule for rule in rules if rule.mechanism.endswith("REVERSION"))
    row = {
        "decision_eligible": True,
        "basis_robust_score_prior_sessions": 3.0,
        "front_residual_return": 0.01,
    }
    assert causal_intent(row, continuation) == 1
    assert causal_intent(row, reversion) == -1
    assert causal_intent({**row, "decision_eligible": False}, continuation) == 0


def test_fill_is_strictly_next_tradable_open() -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-02T14:00:00Z", "2024-01-02T14:01:00Z", "2024-01-02T14:03:00Z"]
            ),
            "open": [70.0, 70.1, 70.3],
        }
    )
    fill = next_tradable_open(bars, decision_bar_timestamp="2024-01-02T14:01:00Z")
    assert fill == {
        "fill_time": pd.Timestamp("2024-01-02T14:03:00Z"),
        "fill_price": 70.3,
    }


def test_future_or_outcome_columns_fail_closed() -> None:
    with pytest.raises(ValueError, match="forbidden"):
        validate_decision_columns(["basis_change", "future_return_label"])
