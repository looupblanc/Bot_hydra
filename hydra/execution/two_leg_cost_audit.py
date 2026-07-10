from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import mean, median
from typing import Any

import numpy as np

from hydra.execution.cost_units import leg_cost_breakdown, legacy_bps_double_counted_cost


@dataclass(frozen=True)
class TwoLegCostAuditRow:
    prototype_id: str
    left_symbol: str
    right_symbol: str
    left_quantity: int
    right_quantity: int
    theoretical_hedge_ratio: float
    executable_hedge_ratio: float
    execution_cost_usd: float
    notional_exposure_usd: float
    mark_to_market_movement_usd: float
    legging_stress_loss_usd: float
    legacy_mislabeled_cost_usd: float
    cost_as_pct_abs_gross: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def audit_two_leg_trade(
    *,
    prototype_id: str,
    left_symbol: str,
    right_symbol: str,
    left_quantity: int,
    right_quantity: int,
    theoretical_hedge_ratio: float,
    executable_hedge_ratio: float,
    left_entry: float,
    right_entry: float,
    left_exit: float,
    right_exit: float,
    direction: int,
    entry_slippage_ticks: float = 1.0,
    exit_slippage_ticks: float = 1.0,
    spread_ticks: float = 0.0,
    forced_liquidation_ticks: float = 0.0,
    legging_stress_loss_usd: float = 0.0,
    legacy_slippage_bps: float = 0.5,
) -> TwoLegCostAuditRow:
    left_cost = leg_cost_breakdown(
        symbol=left_symbol,
        quantity=left_quantity,
        reference_price=left_entry,
        entry_slippage_ticks=entry_slippage_ticks,
        exit_slippage_ticks=exit_slippage_ticks,
        spread_ticks=spread_ticks,
        forced_liquidation_ticks=forced_liquidation_ticks,
    )
    right_cost = leg_cost_breakdown(
        symbol=right_symbol,
        quantity=right_quantity,
        reference_price=right_entry,
        entry_slippage_ticks=entry_slippage_ticks,
        exit_slippage_ticks=exit_slippage_ticks,
        spread_ticks=spread_ticks,
        forced_liquidation_ticks=forced_liquidation_ticks,
    )
    left_mtm = (float(left_exit) - float(left_entry)) * int(direction) * left_cost.point_value * int(left_quantity)
    right_mtm = (float(right_exit) - float(right_entry)) * -int(direction) * right_cost.point_value * int(right_quantity)
    gross = left_mtm + right_mtm
    execution_cost = left_cost.execution_cost_usd + right_cost.execution_cost_usd
    legacy = legacy_bps_double_counted_cost(
        symbol=left_symbol,
        quantity=left_quantity,
        entry_price=left_entry,
        exit_price=left_exit,
        slippage_bps=legacy_slippage_bps,
    ) + legacy_bps_double_counted_cost(
        symbol=right_symbol,
        quantity=right_quantity,
        entry_price=right_entry,
        exit_price=right_exit,
        slippage_bps=legacy_slippage_bps,
    )
    return TwoLegCostAuditRow(
        prototype_id=prototype_id,
        left_symbol=left_symbol,
        right_symbol=right_symbol,
        left_quantity=int(left_quantity),
        right_quantity=int(right_quantity),
        theoretical_hedge_ratio=float(theoretical_hedge_ratio),
        executable_hedge_ratio=float(executable_hedge_ratio),
        execution_cost_usd=float(execution_cost),
        notional_exposure_usd=float(left_cost.notional_exposure_usd + right_cost.notional_exposure_usd),
        mark_to_market_movement_usd=float(gross),
        legging_stress_loss_usd=float(legging_stress_loss_usd),
        legacy_mislabeled_cost_usd=float(legacy),
        cost_as_pct_abs_gross=float(execution_cost / max(abs(gross), 1.0)),
    )


def summarize_cost_audit(rows: list[TwoLegCostAuditRow]) -> dict[str, Any]:
    costs = [row.execution_cost_usd for row in rows]
    legacy = [row.legacy_mislabeled_cost_usd for row in rows]
    quantities = [row.left_quantity + row.right_quantity for row in rows]
    hedge = [row.executable_hedge_ratio for row in rows]
    return {
        "trade_sets": len(rows),
        "corrected_cost": _distribution(costs),
        "legacy_mislabeled_cost": _distribution(legacy),
        "quantity_distribution": _distribution(quantities),
        "hedge_ratio_distribution": _distribution(hedge),
        "contract_mix": _contract_mix(rows),
        "classification": classify_cost_issue(rows),
    }


def classify_cost_issue(rows: list[TwoLegCostAuditRow]) -> str:
    if not rows:
        return "UNRESOLVED"
    corrected = mean(row.execution_cost_usd for row in rows)
    legacy = mean(row.legacy_mislabeled_cost_usd for row in rows)
    quantity_p90 = np.quantile([row.left_quantity + row.right_quantity for row in rows], 0.90)
    if legacy > corrected * 5.0 and quantity_p90 > 20:
        return "MULTIPLE_CAUSES"
    if legacy > corrected * 5.0:
        return "COST_UNIT_BUG"
    if quantity_p90 > 60:
        return "SIZING_BUG"
    if corrected > 500:
        return "GENUINELY_UNECONOMIC_EXECUTION"
    return "STRESS_COST_MISLABELED" if any(row.legging_stress_loss_usd for row in rows) else "UNRESOLVED"


def _distribution(values: list[float]) -> dict[str, float]:
    if not values:
        return {"mean": 0.0, "median": 0.0, "p25": 0.0, "p75": 0.0, "p90": 0.0, "max": 0.0}
    arr = np.asarray(values, dtype=float)
    return {
        "mean": round(float(mean(arr)), 6),
        "median": round(float(median(arr)), 6),
        "p25": round(float(np.quantile(arr, 0.25)), 6),
        "p75": round(float(np.quantile(arr, 0.75)), 6),
        "p90": round(float(np.quantile(arr, 0.90)), 6),
        "max": round(float(np.max(arr)), 6),
    }


def _contract_mix(rows: list[TwoLegCostAuditRow]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        key = f"{row.left_symbol}/{row.right_symbol}"
        out[key] = out.get(key, 0) + 1
    return out
