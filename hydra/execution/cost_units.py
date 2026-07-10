from __future__ import annotations

from dataclasses import asdict, dataclass

from hydra.backtest.costs import round_turn_cost
from hydra.markets.instruments import instrument_spec


@dataclass(frozen=True)
class LegCostBreakdown:
    symbol: str
    quantity: int
    tick_size: float
    tick_value: float
    point_value: float
    commission_per_side_usd: float
    round_turn_commission_usd: float
    entry_slippage_ticks: float
    exit_slippage_ticks: float
    spread_ticks: float
    forced_liquidation_ticks: float
    slippage_cost_usd: float
    spread_cost_usd: float
    forced_liquidation_cost_usd: float
    execution_cost_usd: float
    notional_exposure_usd: float

    def to_dict(self) -> dict[str, float | int | str]:
        return asdict(self)


def ticks_to_dollars(symbol: str, ticks: float, quantity: int = 1) -> float:
    spec = instrument_spec(symbol)
    return float(ticks) * spec.tick_value * int(quantity)


def points_to_dollars(symbol: str, points: float, quantity: int = 1) -> float:
    spec = instrument_spec(symbol)
    return float(points) * spec.point_value * int(quantity)


def notional_exposure(symbol: str, price: float, quantity: int = 1) -> float:
    spec = instrument_spec(symbol)
    return float(price) * spec.point_value * int(quantity)


def leg_cost_breakdown(
    *,
    symbol: str,
    quantity: int,
    reference_price: float,
    entry_slippage_ticks: float = 1.0,
    exit_slippage_ticks: float = 1.0,
    spread_ticks: float = 0.0,
    forced_liquidation_ticks: float = 0.0,
) -> LegCostBreakdown:
    spec = instrument_spec(symbol)
    quantity = int(quantity)
    round_turn_commission = round_turn_cost(symbol) * quantity
    commission_per_side = round_turn_commission / 2.0
    slippage = ticks_to_dollars(symbol, entry_slippage_ticks + exit_slippage_ticks, quantity)
    spread = ticks_to_dollars(symbol, spread_ticks, quantity)
    forced = ticks_to_dollars(symbol, forced_liquidation_ticks, quantity)
    execution_cost = round_turn_commission + slippage + spread + forced
    return LegCostBreakdown(
        symbol=symbol,
        quantity=quantity,
        tick_size=spec.tick_size,
        tick_value=spec.tick_value,
        point_value=spec.point_value,
        commission_per_side_usd=commission_per_side,
        round_turn_commission_usd=round_turn_commission,
        entry_slippage_ticks=float(entry_slippage_ticks),
        exit_slippage_ticks=float(exit_slippage_ticks),
        spread_ticks=float(spread_ticks),
        forced_liquidation_ticks=float(forced_liquidation_ticks),
        slippage_cost_usd=slippage,
        spread_cost_usd=spread,
        forced_liquidation_cost_usd=forced,
        execution_cost_usd=execution_cost,
        notional_exposure_usd=notional_exposure(symbol, reference_price, quantity),
    )


def legacy_bps_double_counted_cost(
    *,
    symbol: str,
    quantity: int,
    entry_price: float,
    exit_price: float,
    slippage_bps: float,
) -> float:
    """Replicate the old mislabeled bps + double-deduction cost for forensics only."""
    spec = instrument_spec(symbol)
    price_slippage = (abs(float(entry_price)) + abs(float(exit_price))) * float(slippage_bps) / 10_000.0
    dollar_slippage = price_slippage * spec.point_value * int(quantity)
    return round_turn_cost(symbol) * int(quantity) + 2.0 * dollar_slippage
