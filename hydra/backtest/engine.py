from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from hydra.backtest.costs import round_turn_cost
from hydra.backtest.metrics import max_drawdown, profit_factor, sharpe_approx
from hydra.strategies.dsl import StrategyCandidate
from hydra.strategies.families import signal_for_candidate


@dataclass
class BacktestResult:
    candidate_id: str
    equity_curve: pd.Series
    trades: list[dict]
    metrics: dict[str, float]


def run_backtest(candidate: StrategyCandidate, df: pd.DataFrame, seed: int = 42) -> BacktestResult:
    signals = signal_for_candidate(candidate, df)
    point_value = 5.0 if candidate.symbol.startswith("M") else 50.0
    risk_scale = float(candidate.risk_parameters.get("risk_scale", 1.0))
    hold = int(candidate.risk_parameters.get("holding_period", 8))
    max_position = int(candidate.risk_parameters.get("max_position", 1))
    equity = 0.0
    curve: list[float] = []
    trades: list[dict] = []
    pos = 0
    entry_price = 0.0
    entry_i = 0
    mfe = 0.0
    mae = 0.0
    cost = round_turn_cost(candidate.symbol)
    closes = df["close"].to_numpy(dtype=float)
    signal_values = signals.to_numpy(dtype=float)
    slippage_bps = 0.5

    for i, close in enumerate(closes):
        signal = int(signal_values[i])
        execution_side = signal if signal else (pos or 1)
        price = float(close + execution_side * close * slippage_bps / 10_000)
        if pos:
            open_pnl = (close - entry_price) * pos * point_value * risk_scale
            mfe = max(mfe, open_pnl)
            mae = min(mae, open_pnl)
            should_exit = (i - entry_i >= hold) or (signal == -pos)
            if should_exit:
                pnl = (price - entry_price) * pos * point_value * risk_scale - cost * risk_scale
                equity += pnl
                trades.append({"entry_i": entry_i, "exit_i": i, "side": pos, "pnl": float(pnl), "mfe": float(mfe), "mae": float(mae)})
                pos = 0
                entry_price = 0.0
                mfe = 0.0
                mae = 0.0
        if pos == 0 and signal != 0:
            pos = max(-max_position, min(max_position, signal))
            entry_price = price
            entry_i = i
        curve.append(equity)

    equity_curve = pd.Series(curve, index=df.index, dtype=float)
    pnls = [t["pnl"] for t in trades]
    returns = equity_curve.diff().fillna(0.0)
    wins = [x for x in pnls if x > 0]
    metrics = {
        "net_profit": float(equity_curve.iloc[-1] if len(equity_curve) else 0.0),
        "max_drawdown": max_drawdown(equity_curve) if len(equity_curve) else 0.0,
        "profit_factor": profit_factor(pnls),
        "sharpe": sharpe_approx(returns),
        "trade_count": float(len(trades)),
        "win_rate": float(len(wins) / len(pnls)) if pnls else 0.0,
    }
    return BacktestResult(candidate.candidate_id, equity_curve, trades, metrics)
