from __future__ import annotations

import pandas as pd


def correlation_cluster(candidate_id: str, equity_curve: pd.Series, existing: dict[str, pd.Series], threshold: float = 0.92) -> tuple[str | None, bool]:
    returns = equity_curve.diff().fillna(0.0)
    for other_id, other_curve in existing.items():
        joined = pd.concat([returns, other_curve.diff().fillna(0.0)], axis=1).dropna()
        if len(joined) > 5 and abs(joined.iloc[:, 0].corr(joined.iloc[:, 1])) >= threshold:
            return other_id, True
    return None, False
