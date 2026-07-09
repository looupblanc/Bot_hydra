from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from hydra.data.databento_loader import normalize_ohlcv_frame
from hydra.features.market_state import build_market_state
from hydra.validation.no_leak import assert_no_future_dependency


class NoLookaheadTests(unittest.TestCase):
    def test_market_state_features_do_not_change_when_future_bars_are_mutated(self) -> None:
        raw = _sample_ohlcv()
        ok, reason = assert_no_future_dependency(raw, build_market_state)
        self.assertTrue(ok, reason)

    def test_validator_catches_intentional_future_close_leak(self) -> None:
        raw = _sample_ohlcv()

        def leaky_builder(df: pd.DataFrame) -> pd.DataFrame:
            out = build_market_state(df)
            out["future_close"] = out["close"].shift(-1)
            return out

        ok, reason = assert_no_future_dependency(raw, leaky_builder, feature_columns=["future_close"])
        self.assertFalse(ok)
        self.assertIn("future_dependency_detected", reason)

    def test_normalized_cached_ohlcv_interface_is_causal(self) -> None:
        raw = _sample_ohlcv().drop(columns=["timeframe"])
        normalized = normalize_ohlcv_frame(raw, symbol="ES", timeframe="1m")
        ok, reason = assert_no_future_dependency(normalized, build_market_state)
        self.assertTrue(ok, reason)


def _sample_ohlcv(rows: int = 260) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    timestamp = pd.date_range("2024-01-01", periods=rows, freq="1min", tz="UTC")
    shocks = rng.normal(0.00005, 0.0015, rows)
    close = 4800 * np.exp(np.cumsum(shocks))
    open_ = np.r_[close[0], close[:-1]]
    spread = np.abs(rng.normal(0.0004, 0.0001, rows)) * close
    high = np.maximum(open_, close) + spread
    low = np.minimum(open_, close) - spread
    return pd.DataFrame(
        {
            "timestamp": timestamp,
            "symbol": "ES",
            "timeframe": "1m",
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": rng.integers(100, 1000, rows),
            "session_id": timestamp.date.astype(str),
        }
    )


if __name__ == "__main__":
    unittest.main()
