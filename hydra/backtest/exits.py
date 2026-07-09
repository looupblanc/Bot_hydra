from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExitDecision:
    should_exit: bool
    reason: str


def evaluate_exit_policy(
    policy: str,
    open_pnl: float,
    mfe: float,
    mae: float,
    bars_held: int,
    holding_period: int,
    risk_dollars: float,
    daily_pnl: float,
    daily_stop: float,
    daily_profit_lock: float,
    mll_buffer: float,
) -> ExitDecision:
    risk = max(abs(risk_dollars), 1.0)
    if policy == "time_stop" and bars_held >= holding_period:
        return ExitDecision(True, "time_stop")
    if policy == "fixed_stop_fixed_target":
        if open_pnl <= -risk:
            return ExitDecision(True, "fixed_stop")
        if open_pnl >= risk * 1.8:
            return ExitDecision(True, "fixed_target")
    elif policy == "atr_stop_target":
        if open_pnl <= -risk * 0.9:
            return ExitDecision(True, "atr_stop")
        if open_pnl >= risk * 2.2:
            return ExitDecision(True, "atr_target")
    elif policy == "volatility_scaled_stop_target":
        if open_pnl <= -risk * 0.75:
            return ExitDecision(True, "vol_scaled_stop")
        if open_pnl >= risk * 1.6:
            return ExitDecision(True, "vol_scaled_target")
    elif policy == "break_even_after_R":
        if mfe >= risk and open_pnl <= 0:
            return ExitDecision(True, "breakeven_after_r")
    elif policy == "trailing_after_R":
        if mfe >= risk * 1.5 and open_pnl <= mfe * 0.45:
            return ExitDecision(True, "trailing_after_r")
    elif policy == "session_close_exit":
        if bars_held >= holding_period:
            return ExitDecision(True, "session_close_proxy")
    elif policy == "daily_profit_lock_exit":
        if daily_pnl + open_pnl >= daily_profit_lock:
            return ExitDecision(True, "daily_profit_lock")
    elif policy == "internal_daily_stop_exit":
        if daily_pnl + open_pnl <= -daily_stop:
            return ExitDecision(True, "internal_daily_stop")
    elif policy == "mll_buffer_protection_exit":
        if mll_buffer + open_pnl <= max(750.0, risk * 1.5):
            return ExitDecision(True, "mll_buffer_protection")
    elif policy == "partial_profit_take_then_runner":
        if mfe >= risk * 1.25 and open_pnl <= mfe * 0.35:
            return ExitDecision(True, "runner_trail")
    elif policy == "reduce_size_near_mll_exit":
        if mll_buffer < 1500 and open_pnl < 0:
            return ExitDecision(True, "reduce_size_near_mll")
    if bars_held >= holding_period:
        return ExitDecision(True, "holding_period")
    return ExitDecision(False, "hold")
