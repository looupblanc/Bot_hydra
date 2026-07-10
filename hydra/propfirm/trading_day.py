from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from zoneinfo import ZoneInfo

import pandas as pd


CENTRAL = ZoneInfo("America/Chicago")
TRADING_DAY_START = time(17, 0)
TRADING_DAY_CUTOFF = time(15, 10)


@dataclass(frozen=True)
class TradingDayInfo:
    timestamp_utc: pd.Timestamp
    timestamp_ct: pd.Timestamp
    trading_day: str
    after_cutoff: bool


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
    after_cutoff = ct.time() > TRADING_DAY_CUTOFF and ct.time() < TRADING_DAY_START
    return TradingDayInfo(utc, ct, trading_day, after_cutoff)


def add_trading_day_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ts = pd.to_datetime(out["timestamp"], utc=True)
    infos = [trading_day_for_timestamp(value) for value in ts]
    out["timestamp_ct"] = [info.timestamp_ct for info in infos]
    out["topstep_trading_day"] = [info.trading_day for info in infos]
    out["after_topstep_cutoff"] = [info.after_cutoff for info in infos]
    return out


def is_allowed_entry_timestamp(ts: pd.Timestamp) -> bool:
    return not trading_day_for_timestamp(ts).after_cutoff

