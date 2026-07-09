from __future__ import annotations

import pandas as pd

from hydra.data.databento_loader import load_cached_databento_range
from hydra.data.synthetic import generate_synthetic_ohlcv


def load_market_data(
    symbol: str,
    timeframe: str,
    synthetic: bool,
    seed: int,
    bars: int = 1500,
    diagnostic_relaxed: bool = False,
    data_provider: str | None = None,
    dataset: str | None = None,
    schema: str | None = None,
    start: str | None = None,
    end: str | None = None,
    cache_folder: str = "data/cache/databento",
) -> pd.DataFrame:
    if synthetic:
        return generate_synthetic_ohlcv(symbol, timeframe, seed, bars, diagnostic_relaxed)
    if data_provider == "databento":
        if not all([dataset, schema, start, end]):
            raise FileNotFoundError("Databento real-data loader requires dataset, schema, start, and end.")
        df = load_cached_databento_range(
            dataset=str(dataset),
            schema=str(schema),
            symbols=[symbol],
            start=str(start),
            end=str(end),
            cache_folder=cache_folder,
            timeframe=timeframe,
        )
        return df[df["symbol"] == symbol].reset_index(drop=True)
    raise FileNotFoundError("Real futures data loader is not configured. Use --synthetic or --data-provider databento.")
