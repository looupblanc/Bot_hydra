from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from hydra.markets.instruments import instrument_spec


@dataclass(frozen=True)
class IntegerHedge:
    left_symbol: str
    right_symbol: str
    left_quantity: int
    right_quantity: int
    theoretical_ratio: float
    executable_ratio: float
    net_dollar_exposure: float
    ratio_error: float
    mini_equivalent_contracts: float


def integer_hedge_ratio(
    *,
    left_symbol: str,
    right_symbol: str,
    theoretical_ratio: float,
    left_price: float,
    right_price: float,
    max_mini_equivalents: float = 15.0,
    prefer_micro: bool = True,
) -> IntegerHedge:
    left_spec = instrument_spec(left_symbol)
    right_spec = instrument_spec(right_symbol)
    max_left = 150 if prefer_micro and left_spec.is_micro else 15
    max_right = 150 if prefer_micro and right_spec.is_micro else 15
    best: tuple[float, int, int, float, float] | None = None
    target = abs(float(theoretical_ratio))
    if not np.isfinite(target) or target <= 0:
        target = 1.0
    for left_qty in range(1, max_left + 1):
        for right_qty in range(1, max_right + 1):
            mini_equiv = _mini_equivalent(left_symbol, left_qty) + _mini_equivalent(right_symbol, right_qty)
            if mini_equiv > max_mini_equivalents:
                continue
            executable = (right_qty * right_spec.point_value * right_price) / max(left_qty * left_spec.point_value * left_price, 1e-9)
            error = abs(executable - target)
            net = left_qty * left_spec.point_value * left_price - target * right_qty * right_spec.point_value * right_price
            score = error + abs(net) / max(left_qty * left_spec.point_value * left_price, 1.0)
            if best is None or score < best[0]:
                best = (score, left_qty, right_qty, executable, mini_equiv)
    if best is None:
        raise ValueError("No executable pair ratio within position limits")
    _, left_qty, right_qty, executable, mini_equiv = best
    net_exposure = left_qty * left_spec.point_value * left_price - target * right_qty * right_spec.point_value * right_price
    return IntegerHedge(
        left_symbol=left_symbol,
        right_symbol=right_symbol,
        left_quantity=int(left_qty),
        right_quantity=int(right_qty),
        theoretical_ratio=float(theoretical_ratio),
        executable_ratio=float(executable),
        net_dollar_exposure=float(net_exposure),
        ratio_error=float(abs(executable - target)),
        mini_equivalent_contracts=float(mini_equiv),
    )


def directional_beta_audit(pnl: pd.Series, left_returns: pd.Series, right_returns: pd.Series) -> dict[str, Any]:
    aligned = pd.concat(
        [
            pd.to_numeric(pnl, errors="coerce").rename("pnl"),
            pd.to_numeric(left_returns, errors="coerce").rename("left"),
            pd.to_numeric(right_returns, errors="coerce").rename("right"),
        ],
        axis=1,
    ).dropna()
    if len(aligned) < 10:
        return {"status": "INSUFFICIENT_EVIDENCE", "left_beta": 0.0, "right_beta": 0.0, "directional_dominance": False}
    left_beta = _beta(aligned["pnl"], aligned["left"])
    right_beta = _beta(aligned["pnl"], aligned["right"])
    corr_left = aligned["pnl"].corr(aligned["left"])
    corr_right = aligned["pnl"].corr(aligned["right"])
    dominance = max(abs(corr_left or 0.0), abs(corr_right or 0.0)) >= 0.65
    return {
        "status": "OK",
        "left_beta": float(left_beta),
        "right_beta": float(right_beta),
        "left_correlation": float(corr_left or 0.0),
        "right_correlation": float(corr_right or 0.0),
        "directional_dominance": bool(dominance),
    }


def _beta(y: pd.Series, x: pd.Series) -> float:
    var = float(x.var())
    if var <= 0:
        return 0.0
    return float(y.cov(x) / var)


def _mini_equivalent(symbol: str, quantity: int) -> float:
    return float(quantity) / 10.0 if instrument_spec(symbol).is_micro else float(quantity)
