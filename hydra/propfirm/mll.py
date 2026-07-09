from __future__ import annotations

import pandas as pd


def simulate_trailing_mll(equity_curve: pd.Series, account_size: float, max_loss_limit: float) -> dict[str, float | bool]:
    account_equity = account_size + equity_curve
    max_equity = account_equity.cummax()
    trailing_floor = (max_equity - max_loss_limit).clip(upper=account_size)
    buffer = account_equity - trailing_floor
    breached = bool((buffer <= 0).any())
    return {
        "mll_breached": breached,
        "mll_buffer": float(buffer.iloc[-1] if len(buffer) else 0.0),
        "min_mll_buffer": float(buffer.min() if len(buffer) else 0.0),
        "max_equity": float(max_equity.max() if len(max_equity) else account_size),
        "portfolio_drawdown": float((max_equity - account_equity).max() if len(account_equity) else 0.0),
    }
