from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Mapping

import numpy as np
import pandas as pd

from hydra.data.multitimeframe import (
    resample_closed_bars,
    resample_multi_session_context,
    resample_session_bars,
)


@dataclass(frozen=True)
class MTFCacheBundle:
    frames: Mapping[str, pd.DataFrame]
    transformation_hash: str


def build_closed_mtf_cache(frame: pd.DataFrame) -> MTFCacheBundle:
    """Build every canonical closed-bar context once for one source frame."""
    required = {
        "timestamp",
        "symbol",
        "active_contract",
        "trading_session_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Missing MTF source columns: {missing}")
    source = frame.sort_values(["symbol", "active_contract", "timestamp"]).reset_index(drop=True)
    cutoff = pd.to_datetime(source["timestamp"], utc=True).max() + pd.Timedelta(minutes=1)
    frames: dict[str, pd.DataFrame] = {"1m": source}
    for minutes in (5, 15, 30, 60):
        frames[f"{minutes}m"] = resample_closed_bars(source, minutes, as_of=cutoff)
    sessions = resample_session_bars(source)
    frames["session"] = sessions
    frames["daily"] = resample_multi_session_context(sessions, 1)
    frames["3session"] = resample_multi_session_context(sessions, 3)
    digest = hashlib.sha256()
    for name, value in sorted(frames.items()):
        digest.update(name.encode("utf-8"))
        digest.update(str(len(value)).encode("ascii"))
        if "availability_timestamp" in value:
            timestamps = pd.to_datetime(value["availability_timestamp"], utc=True)
            digest.update(timestamps.astype("int64").to_numpy(dtype=np.int64).tobytes())
    return MTFCacheBundle(frames=frames, transformation_hash=digest.hexdigest())
