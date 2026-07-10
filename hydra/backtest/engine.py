from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from hydra.backtest.costs import round_turn_cost
from hydra.backtest.exits import evaluate_exit_policy
from hydra.backtest.metrics import max_drawdown, profit_factor, sharpe_approx
from hydra.markets.instruments import instrument_spec
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
    point_value = instrument_spec(candidate.symbol).point_value
    risk_scale = float(candidate.risk_parameters.get("risk_scale", 1.0))
    hold = int(candidate.risk_parameters.get("holding_period", 8))
    max_position = int(candidate.risk_parameters.get("max_position", 1))
    exit_policy = str(candidate.risk_parameters.get("exit_policy", "time_stop"))
    risk_dollars = float(candidate.risk_parameters.get("risk_per_trade", 500.0))
    daily_stop = float(candidate.risk_parameters.get("internal_daily_stop", 1000.0))
    daily_profit_lock = float(candidate.risk_parameters.get("daily_profit_lock", 1500.0))
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
    sessions = df["session_id"].astype(str).to_numpy() if "session_id" in df.columns else ["all"] * len(df)
    slippage_bps = 0.5
    daily_pnl: dict[str, float] = {}

    for i, close in enumerate(closes):
        session_id = str(sessions[i])
        day_pnl = daily_pnl.get(session_id, 0.0)
        signal = int(signal_values[i])
        execution_side = signal if signal else (pos or 1)
        price = float(close + execution_side * close * slippage_bps / 10_000)
        if pos:
            open_pnl = (close - entry_price) * pos * point_value * risk_scale
            mfe = max(mfe, open_pnl)
            mae = min(mae, open_pnl)
            exit_decision = evaluate_exit_policy(
                exit_policy,
                open_pnl=open_pnl,
                mfe=mfe,
                mae=mae,
                bars_held=i - entry_i,
                holding_period=hold,
                risk_dollars=risk_dollars,
                daily_pnl=day_pnl,
                daily_stop=daily_stop,
                daily_profit_lock=daily_profit_lock,
                mll_buffer=4500.0 + equity,
            )
            should_exit = exit_decision.should_exit or (signal == -pos)
            if should_exit:
                pnl = (price - entry_price) * pos * point_value * risk_scale - cost * risk_scale
                equity += pnl
                daily_pnl[session_id] = daily_pnl.get(session_id, 0.0) + float(pnl)
                trades.append(
                    {
                        "entry_i": entry_i,
                        "exit_i": i,
                        "side": pos,
                        "pnl": float(pnl),
                        "mfe": float(mfe),
                        "mae": float(mae),
                        "exit_reason": exit_decision.reason,
                        "symbol": candidate.symbol,
                        "point_value": float(point_value),
                        "risk_scale": float(risk_scale),
                        "max_position": int(max_position),
                    }
                )
                pos = 0
                entry_price = 0.0
                mfe = 0.0
                mae = 0.0
        day_pnl = daily_pnl.get(session_id, 0.0)
        can_trade_today = day_pnl > -daily_stop and day_pnl < daily_profit_lock
        if pos == 0 and signal != 0 and can_trade_today:
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
