from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_cache(df: pd.DataFrame, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def read_cache(path: str | Path) -> pd.DataFrame:
    return pd.read_parquet(path)
