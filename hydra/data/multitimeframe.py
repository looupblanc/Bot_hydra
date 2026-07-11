from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


SUPPORTED_MINUTE_TIMEFRAMES = (1, 5, 15, 30, 60)


@dataclass(frozen=True)
class TimeframeMetadata:
    timeframe: str
    transformation_version: str = "closed_bar_resampler_v1"
    timestamp_semantics: str = "source_bar_open"


def resample_closed_bars(
    frame: pd.DataFrame,
    minutes: int,
    *,
    as_of: str | pd.Timestamp | None = None,
    group_columns: Iterable[str] = ("symbol", "active_contract"),
) -> pd.DataFrame:
    """Aggregate source-bar-open OHLCV without exposing incomplete bars."""
    if minutes not in SUPPORTED_MINUTE_TIMEFRAMES:
        raise ValueError(f"Unsupported minute timeframe: {minutes}")
    required = {"timestamp", "open", "high", "low", "close", "volume", *group_columns}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Missing resampling columns: {missing}")
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    data = data.sort_values([*group_columns, "timestamp"]).reset_index(drop=True)
    cadence = pd.Timedelta(minutes=minutes)
    data["source_bar_start"] = data["timestamp"].dt.floor(f"{minutes}min")
    data["source_bar_close"] = data["source_bar_start"] + cadence
    keys = [*group_columns, "source_bar_start", "source_bar_close"]
    output = (
        data.groupby(keys, sort=True, dropna=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_row_count=("timestamp", "size"),
            source_last_timestamp=("timestamp", "max"),
        )
        .reset_index()
    )
    output["availability_timestamp"] = output["source_bar_close"]
    output["decision_timestamp"] = output["availability_timestamp"]
    output["source_timeframe"] = "1m"
    output["timeframe"] = f"{minutes}m"
    output["transformation_version"] = "closed_bar_resampler_v1"
    cutoff = pd.Timestamp(as_of) if as_of is not None else data["timestamp"].max() + pd.Timedelta(minutes=1)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    # A higher-timeframe bar is available only after its close. This also drops
    # the last partial bar even if it contains one or more source observations.
    output = output[output["availability_timestamp"] <= cutoff].copy()
    return output.sort_values([*group_columns, "source_bar_close"]).reset_index(drop=True)


def resample_session_bars(
    frame: pd.DataFrame,
    *,
    session_column: str = "trading_session_id",
    group_columns: Iterable[str] = ("symbol", "active_contract"),
) -> pd.DataFrame:
    required = {"timestamp", session_column, "open", "high", "low", "close", "volume", *group_columns}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Missing session columns: {missing}")
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    keys = [*group_columns, session_column]
    output = (
        data.sort_values([*group_columns, "timestamp"])
        .groupby(keys, sort=True, dropna=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_bar_start=("timestamp", "min"),
            source_last_timestamp=("timestamp", "max"),
            source_row_count=("timestamp", "size"),
        )
        .reset_index()
    )
    output["source_bar_close"] = output["source_last_timestamp"] + pd.Timedelta(minutes=1)
    output["availability_timestamp"] = output["source_bar_close"]
    output["decision_timestamp"] = output["availability_timestamp"]
    output["source_timeframe"] = "1m"
    output["timeframe"] = "session"
    output["transformation_version"] = "closed_session_resampler_v1"
    return output


def resample_multi_session_context(
    session_bars: pd.DataFrame,
    sessions: int = 1,
    *,
    group_columns: Iterable[str] = ("symbol", "active_contract"),
) -> pd.DataFrame:
    """Build causal daily or multi-day context from already closed sessions.

    Every output row becomes available only when its newest constituent session
    is complete. Contract grouping prevents a context window from crossing a
    roll, and ``sessions=1`` is the canonical daily representation.
    """
    if sessions < 1:
        raise ValueError("Context sessions must be positive.")
    required = {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source_bar_start",
        "source_bar_close",
        "availability_timestamp",
        *group_columns,
    }
    missing = sorted(required - set(session_bars.columns))
    if missing:
        raise ValueError(f"Missing closed-session columns: {missing}")
    group_keys = list(group_columns)
    pieces: list[pd.DataFrame] = []
    for _keys, group in session_bars.groupby(group_keys, sort=True, dropna=False):
        ordered = group.sort_values("availability_timestamp").reset_index(drop=True).copy()
        ordered["open_context"] = ordered["open"].shift(sessions - 1)
        ordered["high_context"] = ordered["high"].rolling(sessions, min_periods=sessions).max()
        ordered["low_context"] = ordered["low"].rolling(sessions, min_periods=sessions).min()
        ordered["volume_context"] = ordered["volume"].rolling(sessions, min_periods=sessions).sum()
        ordered["context_start"] = ordered["source_bar_start"].shift(sessions - 1)
        ordered = ordered.iloc[sessions - 1 :].copy()
        ordered["open"] = ordered.pop("open_context")
        ordered["high"] = ordered.pop("high_context")
        ordered["low"] = ordered.pop("low_context")
        ordered["volume"] = ordered.pop("volume_context")
        ordered["source_bar_start"] = ordered.pop("context_start")
        ordered["decision_timestamp"] = ordered["availability_timestamp"]
        ordered["source_timeframe"] = "session"
        ordered["timeframe"] = "daily" if sessions == 1 else f"{sessions}session"
        ordered["transformation_version"] = "closed_multi_session_context_v1"
        pieces.append(ordered)
    if not pieces:
        return session_bars.iloc[0:0].copy()
    return (
        pd.concat(pieces, ignore_index=True)
        .sort_values([*group_keys, "availability_timestamp"])
        .reset_index(drop=True)
    )


def join_completed_timeframe(
    decisions: pd.DataFrame,
    higher_timeframe: pd.DataFrame,
    *,
    decision_timestamp: str = "decision_timestamp",
    by: Iterable[str] = ("symbol", "active_contract"),
) -> pd.DataFrame:
    left = decisions.copy()
    right = higher_timeframe.copy()
    left[decision_timestamp] = pd.to_datetime(left[decision_timestamp], utc=True)
    right["availability_timestamp"] = pd.to_datetime(right["availability_timestamp"], utc=True)
    by_columns = list(by)
    left = left.sort_values([decision_timestamp, *by_columns])
    right = right.sort_values(["availability_timestamp", *by_columns])
    joined = pd.merge_asof(
        left,
        right,
        left_on=decision_timestamp,
        right_on="availability_timestamp",
        by=by_columns,
        direction="backward",
        allow_exact_matches=True,
        suffixes=("", "_higher"),
    )
    available = joined["availability_timestamp"].dropna()
    if not (available <= joined.loc[available.index, decision_timestamp]).all():
        raise RuntimeError("Incomplete higher-timeframe bar joined to decision.")
    return joined
