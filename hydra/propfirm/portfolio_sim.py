from __future__ import annotations

import pandas as pd

from hydra.propfirm.mll import simulate_trailing_mll


def simulate_portfolio(curves: list[pd.Series], account_size: float, max_loss_limit: float) -> dict:
    if not curves:
        return {"mll_breached": False, "mll_buffer": account_size, "portfolio_drawdown": 0.0}
    portfolio_curve = pd.concat(curves, axis=1).fillna(method="ffill").fillna(0.0).sum(axis=1)
    return simulate_trailing_mll(portfolio_curve, account_size, max_loss_limit)
