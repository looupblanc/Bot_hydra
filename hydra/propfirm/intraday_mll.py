from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from hydra.markets.instruments import instrument_spec
from hydra.propfirm.mll_variants import (
    MllVariant,
    advance_end_of_day_floor,
    advance_intraday_floor,
    normalized_variant,
)


@dataclass(frozen=True)
class IntradayMLLResult:
    breached: bool
    min_buffer: float
    breach_trade_index: int | None
    breach_timestamp: str | None
    ambiguous_same_bar_count: int
    forced_liquidation_slippage: float
    notes: list[str]


def conservative_intraday_mll_audit(
    trades: list[dict[str, Any]],
    df: pd.DataFrame,
    starting_balance: float,
    starting_floor: float,
    mll_distance: float,
    floor_lock: float,
    forced_liquidation_slippage_bps: float = 1.0,
    mll_variant: MllVariant | str = MllVariant.EOD_REALIZED_BALANCE,
) -> IntradayMLLResult:
    if not trades:
        return IntradayMLLResult(False, starting_balance - starting_floor, None, None, 0, 0.0, [])
    frame = df.reset_index(drop=True).copy()
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    balance = float(starting_balance)
    floor = float(starting_floor)
    min_buffer = balance - floor
    notes: list[str] = []
    ambiguous = 0
    total_forced_slippage = 0.0
    variant = normalized_variant(mll_variant)
    previous_session: object | None = None
    for trade_index, trade in enumerate(sorted(trades, key=lambda t: int(t.get("exit_i", 0)))):
        entry_i = max(0, min(int(trade.get("entry_i", 0)), len(frame) - 1))
        exit_i = max(entry_i, min(int(trade.get("exit_i", entry_i)), len(frame) - 1))
        side = int(trade.get("side", 1) or 1)
        symbol = str(trade.get("symbol") or frame.get("symbol", pd.Series(["ES"])).iloc[entry_i])
        spec = instrument_spec(symbol)
        point_value = float(trade.get("point_value", spec.point_value))
        risk_scale = float(trade.get("risk_scale", 1.0))
        entry_price = _entry_price(trade, frame, entry_i)
        path = frame.iloc[entry_i : exit_i + 1]
        session = timestamps.iloc[entry_i].date()
        if previous_session is not None and session != previous_session:
            floor = advance_end_of_day_floor(
                floor,
                closing_balance=balance,
                distance=mll_distance,
                lock=floor_lock,
            )
        previous_session = session
        adverse_price = path["low"].min() if side > 0 else path["high"].max()
        favorable_price = path["high"].max() if side > 0 else path["low"].min()
        worst_unrealized = (float(adverse_price) - entry_price) * side * point_value * risk_scale
        best_unrealized = (float(favorable_price) - entry_price) * side * point_value * risk_scale
        if worst_unrealized < 0 < best_unrealized and entry_i == exit_i:
            ambiguous += 1
        floor = advance_intraday_floor(
            floor,
            live_equity_high=balance + max(best_unrealized, 0.0),
            distance=mll_distance,
            lock=floor_lock,
            variant=variant,
        )
        forced_slippage = abs(entry_price) * forced_liquidation_slippage_bps / 10_000.0 * point_value * risk_scale
        intraday_equity_low = balance + worst_unrealized - forced_slippage
        min_buffer = min(min_buffer, intraday_equity_low - floor)
        if intraday_equity_low <= floor:
            total_forced_slippage += forced_slippage
            return IntradayMLLResult(
                True,
                float(min_buffer),
                trade_index,
                timestamps.iloc[exit_i].isoformat(),
                ambiguous,
                float(total_forced_slippage),
                notes + ["intraday_unrealized_mll_touch_or_breach"],
            )
        pnl = float(trade.get("pnl", 0.0))
        balance += pnl
        floor = advance_intraday_floor(
            floor,
            live_equity_high=balance,
            distance=mll_distance,
            lock=floor_lock,
            variant=variant,
        )
        min_buffer = min(min_buffer, balance - floor)
        if balance <= floor:
            return IntradayMLLResult(
                True,
                float(min_buffer),
                trade_index,
                timestamps.iloc[exit_i].isoformat(),
                ambiguous,
                float(total_forced_slippage),
                notes + ["realized_mll_touch_or_breach"],
            )
    if ambiguous:
        notes.append("same_bar_path_ambiguity_present")
    return IntradayMLLResult(False, float(min_buffer), None, None, ambiguous, float(total_forced_slippage), notes)


def _entry_price(trade: dict[str, Any], frame: pd.DataFrame, entry_i: int) -> float:
    if "entry_price" in trade:
        return float(trade["entry_price"])
    return float(frame["close"].iloc[entry_i])
