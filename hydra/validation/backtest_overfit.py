from __future__ import annotations

import math


def deflated_sharpe_proxy(sharpe: float, trials: float, observations: int) -> float:
    if observations <= 1:
        return 0.0
    penalty = math.sqrt(2.0 * math.log(max(trials, 1.0)) / observations)
    return float(sharpe - penalty)


def probability_backtest_overfit_proxy(train_rank_pct: float, validation_rank_pct: float) -> float:
    train = max(0.0, min(1.0, train_rank_pct))
    validation = max(0.0, min(1.0, validation_rank_pct))
    if train <= 0.20 and validation >= 0.50:
        return min(1.0, 0.5 + validation - train)
    return max(0.0, validation - train)

