from __future__ import annotations

from hydra.backtest.engine import BacktestResult
from hydra.validation.monte_carlo import monte_carlo_reshuffle_score
from hydra.validation.walk_forward import walk_forward_score


def robustness_score(result: BacktestResult, seed: int) -> float:
    wf = walk_forward_score(result.trades)
    mc = monte_carlo_reshuffle_score(result.trades, seed)
    pf = min(result.metrics.get("profit_factor", 0.0) / 2.0, 1.0)
    sharpe = min(max(result.metrics.get("sharpe", 0.0), 0.0) / 2.0, 1.0)
    return float(0.30 * wf + 0.30 * mc + 0.20 * pf + 0.20 * sharpe)


def parameter_sensitivity_placeholder() -> float:
    return 0.5
