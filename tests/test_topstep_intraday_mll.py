from __future__ import annotations

import unittest

import pandas as pd

from hydra.propfirm.intraday_mll import conservative_intraday_mll_audit
from hydra.propfirm.scaling_plan import position_limit_ok, xfa_150k_max_mini_equivalent
from hydra.propfirm.trading_day import trading_day_for_timestamp


class IntradayMLLTests(unittest.TestCase):
    def test_unrealized_touch_exact_threshold_breaches(self) -> None:
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-02T14:30:00Z", "2024-01-02T14:31:00Z"], utc=True),
                "open": [100.0, 100.0],
                "high": [100.0, 100.0],
                "low": [10.0, 10.0],
                "close": [100.0, 100.0],
                "volume": [1, 1],
                "symbol": ["ES", "ES"],
            }
        )
        trades = [{"entry_i": 0, "exit_i": 1, "side": 1, "entry_price": 100.0, "pnl": 0.0, "symbol": "ES", "risk_scale": 1.0}]
        result = conservative_intraday_mll_audit(trades, df, 150000.0, 145500.0, 4500.0, 150000.0, forced_liquidation_slippage_bps=0.0)
        self.assertTrue(result.breached)
        self.assertEqual(result.breach_trade_index, 0)
        self.assertLessEqual(result.min_buffer, 0.0)

    def test_unrealized_breach_then_recovery_is_failure(self) -> None:
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-02T14:30:00Z", "2024-01-02T14:31:00Z"], utc=True),
                "open": [100.0, 100.0],
                "high": [120.0, 120.0],
                "low": [0.0, 100.0],
                "close": [120.0, 120.0],
                "volume": [1, 1],
                "symbol": ["ES", "ES"],
            }
        )
        trades = [{"entry_i": 0, "exit_i": 1, "side": 1, "entry_price": 100.0, "pnl": 1000.0, "symbol": "ES", "risk_scale": 1.0}]
        result = conservative_intraday_mll_audit(trades, df, 150000.0, 145500.0, 4500.0, 150000.0, forced_liquidation_slippage_bps=0.0)
        self.assertTrue(result.breached)

    def test_same_bar_stop_target_ambiguity_flagged(self) -> None:
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-02T14:30:00Z"], utc=True),
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1],
                "symbol": ["MES"],
            }
        )
        trades = [{"entry_i": 0, "exit_i": 0, "side": 1, "entry_price": 100.0, "pnl": 25.0, "symbol": "MES", "risk_scale": 1.0}]
        result = conservative_intraday_mll_audit(trades, df, 150000.0, 145500.0, 4500.0, 150000.0)
        self.assertFalse(result.breached)
        self.assertEqual(result.ambiguous_same_bar_count, 1)

    def test_scaling_plan_and_micro_ratio(self) -> None:
        self.assertEqual(xfa_150k_max_mini_equivalent(1499.99), 3)
        self.assertEqual(xfa_150k_max_mini_equivalent(1500.0), 4)
        self.assertTrue(position_limit_ok("MNQ", 30, 3))
        self.assertFalse(position_limit_ok("NQ", 4, 3))

    def test_sunday_session_and_dst_timestamp(self) -> None:
        info = trading_day_for_timestamp(pd.Timestamp("2024-03-10T23:15:00Z"))
        self.assertEqual(info.trading_day, "2024-03-11")
        self.assertFalse(info.after_cutoff)


if __name__ == "__main__":
    unittest.main()

