from __future__ import annotations

import pandas as pd


def execution_price(row: pd.Series, side: int, slippage_bps: float = 0.5) -> float:
    slip = row["close"] * slippage_bps / 10_000
    return float(row["close"] + side * slip)
