from __future__ import annotations

import pandas as pd

from hydra.research.option_implied_vrp_regime_switch import simulate_action


def _manifest() -> dict:
    return {
        "opportunity_contract": {"entry_window_minutes": 30, "trigger_offset_ticks": 1},
        "execution_contract": {
            "mandatory_flatten_chicago": "15:00",
            "maximum_holding_minutes": 60,
            "normal_extra_slippage_ticks_per_side": 0,
            "stressed_extra_slippage_ticks_per_side": 1,
            "market_parameters": {
                "ES": {
                    "tick_size": 0.25,
                    "stop_ticks": 8,
                    "target_ticks": 12,
                    "point_value_usd": 50.0,
                    "normal_round_turn_fee_usd": 5.28,
                }
            },
        },
    }


def _rows(*ohlc: tuple[str, float, float, float, float]) -> pd.DataFrame:
    return pd.DataFrame([
        {"timestamp": pd.Timestamp(ts), "open": op, "high": hi, "low": lo, "close": cl,
         "instrument_id": "ESU4"}
        for ts, op, hi, lo, cl in ohlc
    ])


def _opportunity() -> dict:
    return {
        "market": "ES", "session": "2024-09-04",
        "decision_time": "2024-09-04T13:45:00+00:00",
        "range_high": 5000.0, "range_low": 4990.0,
    }


def test_breakout_uses_next_bar_open_and_causal_exit() -> None:
    rows = _rows(
        ("2024-09-04T13:46:00Z", 5000.0, 5000.5, 4999.75, 5000.25),
        ("2024-09-04T13:47:00Z", 5000.5, 5000.75, 5000.25, 5000.5),
        ("2024-09-04T13:48:00Z", 5000.5, 5003.5, 5000.5, 5003.25),
    )
    result = simulate_action(_opportunity(), rows, _manifest(), action="BREAKOUT_OCO", scenario="NORMAL")
    assert result["status"] == "EXECUTABLE_COMPLETE"
    assert result["trigger_time"] == "2024-09-04T13:46:00+00:00"
    assert result["entry_time"] == "2024-09-04T13:47:00+00:00"
    assert result["exit_reason"] == "TARGET"


def test_ambiguous_trigger_abstains_without_trade() -> None:
    rows = _rows(("2024-09-04T13:46:00Z", 4995.0, 5001.0, 4989.0, 4995.0))
    result = simulate_action(_opportunity(), rows, _manifest(), action="FADE_EXTENSION", scenario="STRESSED")
    assert result["status"] == "EXECUTABLE_ABSTAIN_AMBIGUOUS"
    assert result["traded"] is False
