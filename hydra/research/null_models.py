from __future__ import annotations

import numpy as np
import pandas as pd


def block_shuffle(series: pd.Series, block_size: int, seed: int = 0) -> pd.Series:
    rng = np.random.default_rng(seed)
    values = series.reset_index(drop=True)
    blocks = [values.iloc[i : i + block_size] for i in range(0, len(values), block_size)]
    order = rng.permutation(len(blocks))
    shuffled = pd.concat([blocks[int(i)] for i in order], ignore_index=True)
    shuffled.index = series.index
    return shuffled


def delayed_null(series: pd.Series, delay: int) -> pd.Series:
    return series.shift(delay)


def session_aware_shuffle(frame: pd.DataFrame, value_col: str, seed: int = 0) -> pd.Series:
    rng = np.random.default_rng(seed)
    out = frame[value_col].copy()
    if "session_id" not in frame.columns:
        return block_shuffle(out, 60, seed)
    for _, idx in frame.groupby("session_id", sort=True).groups.items():
        values = out.loc[idx].to_numpy(copy=True)
        rng.shuffle(values)
        out.loc[idx] = values
    return out
