from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class PurgedFold:
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def monthly_purged_folds(timestamps: pd.Series, embargo_minutes: int = 60) -> list[PurgedFold]:
    ts = pd.to_datetime(timestamps, utc=True).sort_values()
    if ts.empty:
        return []
    months = sorted({(value.year, value.month) for value in ts})
    folds: list[PurgedFold] = []
    for year, month in months:
        test_start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
        test_end = test_start + pd.offsets.MonthBegin(1)
        train_start = ts.iloc[0]
        train_end = test_start - pd.Timedelta(minutes=embargo_minutes)
        if train_end > train_start:
            folds.append(PurgedFold(train_start, train_end, test_start, test_end))
    return folds

