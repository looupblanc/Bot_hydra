from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import pandas as pd

from hydra.backtest.costs import round_turn_cost
from hydra.execution.cost_units import leg_cost_breakdown
from hydra.markets.instruments import instrument_spec


class ExecutionMode(str, Enum):
    ATOMIC_CONSERVATIVE = "ATOMIC_CONSERVATIVE"
    LEG_SEQUENTIAL_STRESS = "LEG_SEQUENTIAL_STRESS"
    MIDPOINT_RESEARCH_ONLY = "MIDPOINT_RESEARCH_ONLY"


@dataclass(frozen=True)
class PairLeg:
    symbol: str
    quantity: int
    side: int
    entry_price: float
    exit_price: float
    point_value: float
    commission: float
    slippage: float
    spread_cost: float = 0.0
    forced_liquidation_cost: float = 0.0

    @property
    def gross_pnl(self) -> float:
        return (self.exit_price - self.entry_price) * self.side * self.point_value * self.quantity

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.execution_cost_usd

    @property
    def execution_cost_usd(self) -> float:
        return self.commission + self.slippage + self.spread_cost + self.forced_liquidation_cost

    @property
    def notional_exposure_usd(self) -> float:
        return self.entry_price * self.point_value * self.quantity

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {
            "gross_pnl": self.gross_pnl,
            "net_pnl": self.net_pnl,
            "execution_cost_usd": self.execution_cost_usd,
            "notional_exposure_usd": self.notional_exposure_usd,
            "mark_to_market_movement_usd": self.gross_pnl,
        }


@dataclass(frozen=True)
class TwoLegTrade:
    entry_timestamp: str
    exit_timestamp: str
    left: PairLeg
    right: PairLeg
    mode: str
    legging_risk_pnl: float
    failed_second_leg: bool = False

    @property
    def gross_pnl(self) -> float:
        return self.left.gross_pnl + self.right.gross_pnl

    @property
    def net_pnl(self) -> float:
        return self.left.net_pnl + self.right.net_pnl + self.legging_risk_pnl

    @property
    def execution_cost_usd(self) -> float:
        return self.left.execution_cost_usd + self.right.execution_cost_usd

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_timestamp": self.entry_timestamp,
            "exit_timestamp": self.exit_timestamp,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
            "mode": self.mode,
            "legging_risk_pnl": self.legging_risk_pnl,
            "failed_second_leg": self.failed_second_leg,
            "gross_pnl": self.gross_pnl,
            "net_pnl": self.net_pnl,
            "execution_cost_usd": self.execution_cost_usd,
            "notional_exposure_usd": self.left.notional_exposure_usd + self.right.notional_exposure_usd,
            "mark_to_market_movement_usd": self.gross_pnl,
        }


def build_two_leg_trade(
    *,
    entry_timestamp: Any,
    exit_timestamp: Any,
    left_symbol: str,
    right_symbol: str,
    left_quantity: int,
    right_quantity: int,
    direction: int,
    left_entry: float,
    right_entry: float,
    left_exit: float,
    right_exit: float,
    mode: ExecutionMode = ExecutionMode.ATOMIC_CONSERVATIVE,
    slippage_ticks: float = 1.0,
    spread_ticks: float = 0.0,
    forced_liquidation_ticks: float = 0.0,
    legging_delay_bars: int = 1,
    slippage_bps: float | None = None,
) -> TwoLegTrade:
    left_side = int(direction)
    right_side = -int(direction)
    if slippage_bps is not None:
        slippage_ticks = float(slippage_bps)
    left = _leg(left_symbol, left_quantity, left_side, left_entry, left_exit, slippage_ticks, spread_ticks, forced_liquidation_ticks)
    right = _leg(right_symbol, right_quantity, right_side, right_entry, right_exit, slippage_ticks, spread_ticks, forced_liquidation_ticks)
    legging_risk = 0.0
    failed_second_leg = False
    if mode == ExecutionMode.LEG_SEQUENTIAL_STRESS:
        legging_risk = -float(legging_delay_bars) * instrument_spec(left_symbol).tick_value * float(slippage_ticks) * int(left_quantity)
    elif mode == ExecutionMode.MIDPOINT_RESEARCH_ONLY:
        left = _leg(left_symbol, left_quantity, left_side, left_entry, left_exit, 0.0, 0.0, 0.0)
        right = _leg(right_symbol, right_quantity, right_side, right_entry, right_exit, 0.0, 0.0, 0.0)
    return TwoLegTrade(
        entry_timestamp=_as_utc_iso(entry_timestamp),
        exit_timestamp=_as_utc_iso(exit_timestamp),
        left=left,
        right=right,
        mode=mode.value,
        legging_risk_pnl=float(legging_risk),
        failed_second_leg=failed_second_leg,
    )


def _leg(
    symbol: str,
    quantity: int,
    side: int,
    entry: float,
    exit_: float,
    slippage_ticks: float,
    spread_ticks: float,
    forced_liquidation_ticks: float,
) -> PairLeg:
    spec = instrument_spec(symbol)
    costs = leg_cost_breakdown(
        symbol=symbol,
        quantity=quantity,
        reference_price=entry,
        entry_slippage_ticks=slippage_ticks,
        exit_slippage_ticks=slippage_ticks,
        spread_ticks=spread_ticks,
        forced_liquidation_ticks=forced_liquidation_ticks,
    )
    return PairLeg(
        symbol=symbol,
        quantity=int(quantity),
        side=int(side),
        entry_price=float(entry),
        exit_price=float(exit_),
        point_value=float(spec.point_value),
        commission=float(costs.round_turn_commission_usd),
        slippage=float(costs.slippage_cost_usd),
        spread_cost=float(costs.spread_cost_usd),
        forced_liquidation_cost=float(costs.forced_liquidation_cost_usd),
    )


def _as_utc_iso(value: Any) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()
