from __future__ import annotations

from enum import StrEnum


class MllVariant(StrEnum):
    """Versioned interpretations of the Topstep trailing MLL."""

    EOD_REALIZED_BALANCE = "EOD_REALIZED_BALANCE"
    INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST = (
        "INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST"
    )


def normalized_variant(value: MllVariant | str) -> MllVariant:
    return value if isinstance(value, MllVariant) else MllVariant(str(value))


def advance_intraday_floor(
    floor: float,
    *,
    live_equity_high: float,
    distance: float,
    lock: float,
    variant: MllVariant | str,
) -> float:
    """Advance the floor from live equity only for the intraday-HWM variant."""

    if normalized_variant(variant) is MllVariant.EOD_REALIZED_BALANCE:
        return float(floor)
    return float(min(lock, max(floor, live_equity_high - distance)))


def advance_end_of_day_floor(
    floor: float,
    *,
    closing_balance: float,
    distance: float,
    lock: float,
) -> float:
    """Advance an end-of-day floor; it can never fall or exceed its lock."""

    return float(min(lock, max(floor, closing_balance - distance)))


def favorable_first_is_ambiguous(
    *, worst_unrealized_pnl: float, best_unrealized_pnl: float
) -> bool:
    """Whether aggregated extrema do not identify their within-event ordering."""

    return bool(worst_unrealized_pnl < 0.0 < best_unrealized_pnl)


__all__ = [
    "MllVariant",
    "advance_end_of_day_floor",
    "advance_intraday_floor",
    "favorable_first_is_ambiguous",
    "normalized_variant",
]
