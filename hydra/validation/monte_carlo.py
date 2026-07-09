from __future__ import annotations

import numpy as np


def monte_carlo_reshuffle_score(trades: list[dict], seed: int, trials: int = 200) -> float:
    pnls = np.array([t["pnl"] for t in trades], dtype=float)
    if len(pnls) < 2:
        return 0.0
    rng = np.random.default_rng(seed)
    positive = 0
    for _ in range(trials):
        sample = rng.permutation(pnls)
        if sample.sum() > 0 and sample.cumsum().min() > -abs(sample.sum() + 1e-9) * 2:
            positive += 1
    return positive / trials
