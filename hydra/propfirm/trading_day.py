from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from zoneinfo import ZoneInfo

import pandas as pd


CENTRAL = ZoneInfo("America/Chicago")
TRADING_DAY_START = time(17, 0)
SESSION_FLATTEN = time(15, 10)
WINNING_DAY_LOCK = time(16, 0)
# Backward-compatible name used by the existing shadow layer.
TRADING_DAY_CUTOFF = SESSION_FLATTEN


@dataclass(frozen=True)
class TradingDayInfo:
    timestamp_utc: pd.Timestamp
    timestamp_ct: pd.Timestamp
    trading_day: str
    after_cutoff: bool
    winning_day_locked: bool


def trading_day_for_timestamp(ts: pd.Timestamp) -> TradingDayInfo:
    utc = pd.Timestamp(ts)
    if utc.tzinfo is None:
        utc = utc.tz_localize("UTC")
    else:
        utc = utc.tz_convert("UTC")
    ct = utc.tz_convert(CENTRAL)
    local_date = ct.date()
    if ct.time() >= TRADING_DAY_START:
        trading_day = (ct + pd.Timedelta(days=1)).date().isoformat()
    else:
        trading_day = local_date.isoformat()
    after_cutoff = (
        SESSION_FLATTEN <= ct.time() < TRADING_DAY_START
    )
    winning_day_locked = (
        WINNING_DAY_LOCK <= ct.time() < TRADING_DAY_START
    )
    return TradingDayInfo(
        utc, ct, trading_day, after_cutoff, winning_day_locked
    )


def add_trading_day_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ts = pd.to_datetime(out["timestamp"], utc=True)
    infos = [trading_day_for_timestamp(value) for value in ts]
    out["timestamp_ct"] = [info.timestamp_ct for info in infos]
    out["topstep_trading_day"] = [info.trading_day for info in infos]
    out["after_topstep_cutoff"] = [info.after_cutoff for info in infos]
    out["topstep_winning_day_locked"] = [
        info.winning_day_locked for info in infos
    ]
    return out


def is_allowed_entry_timestamp(ts: pd.Timestamp) -> bool:
    return not trading_day_for_timestamp(ts).after_cutoff


def is_winning_day_locked(ts: pd.Timestamp) -> bool:
    return trading_day_for_timestamp(ts).winning_day_locked
