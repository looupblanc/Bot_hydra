from __future__ import annotations

import pandas as pd

from hydra.data.synthetic import generate_synthetic_ohlcv


def load_market_data(symbol: str, timeframe: str, synthetic: bool, seed: int, bars: int = 1500) -> pd.DataFrame:
    if synthetic:
        return generate_synthetic_ohlcv(symbol, timeframe, seed, bars)
    raise FileNotFoundError("Real futures data loader is not configured yet. Use --synthetic for smoke tests.")
