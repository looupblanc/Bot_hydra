from __future__ import annotations

import numpy as np
import pandas as pd


def matched_event_indices(
    frame: pd.DataFrame,
    event_mask: pd.Series,
    *,
    seed: int = 0,
    max_events: int | None = None,
) -> pd.Index:
    rng = np.random.default_rng(seed)
    working = frame.copy()
    working["_event"] = event_mask.reindex(frame.index).fillna(False).astype(bool)
    if "timestamp" in working.columns:
        timestamps = pd.to_datetime(working["timestamp"], utc=True)
        working["_weekday"] = timestamps.dt.weekday
        working["_phase"] = (timestamps.dt.hour * 60 + timestamps.dt.minute) // 60
    else:
        working["_weekday"] = 0
        working["_phase"] = 0
    if "volatility_decile" not in working.columns:
        returns = working.get("close", pd.Series(index=working.index, dtype=float)).pct_change().abs()
        working["volatility_decile"] = pd.qcut(returns.rank(method="first"), 10, labels=False, duplicates="drop").fillna(0)
    events = working[working["_event"]]
    chosen = []
    for _, event in events.iterrows():
        pool = working[
            (~working["_event"])
            & (working.get("symbol", "") == event.get("symbol", working.get("symbol", "")))
            & (working["_weekday"] == event["_weekday"])
            & (working["_phase"] == event["_phase"])
            & (working["volatility_decile"] == event["volatility_decile"])
        ]
        if pool.empty:
            pool = working[(~working["_event"]) & (working["_phase"] == event["_phase"])]
        if pool.empty:
            continue
        chosen.append(pool.index[int(rng.integers(0, len(pool)))])
        if max_events and len(chosen) >= max_events:
            break
    return pd.Index(chosen)


def opportunity_count_matched_signal(frame: pd.DataFrame, signal: pd.Series, *, seed: int = 0) -> pd.Series:
    event_mask = signal.reindex(frame.index).fillna(0).astype(float).ne(0)
    matched = matched_event_indices(frame, event_mask, seed=seed, max_events=int(event_mask.sum()))
    out = pd.Series(0, index=frame.index, dtype=int)
    if len(matched):
        directions = signal[event_mask].astype(int).to_numpy()
        out.loc[matched] = directions[: len(matched)]
    return out
