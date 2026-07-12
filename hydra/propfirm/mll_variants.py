from __future__ import annotations

from enum import StrEnum


class MllMode(StrEnum):
    """Public V7 configuration values frozen by the mission contract."""

    EOD_LEVEL_RT_BREACH = "eod_level_rt_breach"
    INTRADAY_HWM = "intraday_hwm"


class MllVariant(StrEnum):
    """Legacy internal identifiers retained for replay compatibility."""

    EOD_REALIZED_BALANCE = "EOD_REALIZED_BALANCE"
    INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST = (
        "INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST"
    )


def normalized_mode(value: MllMode | MllVariant | str) -> MllMode:
    if isinstance(value, MllMode):
        return value
    if isinstance(value, MllVariant):
        return (
            MllMode.EOD_LEVEL_RT_BREACH
            if value is MllVariant.EOD_REALIZED_BALANCE
            else MllMode.INTRADAY_HWM
        )
    raw = str(value)
    aliases = {
        MllVariant.EOD_REALIZED_BALANCE.value: MllMode.EOD_LEVEL_RT_BREACH,
        MllVariant.INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST.value: (
            MllMode.INTRADAY_HWM
        ),
    }
    if raw in aliases:
        return aliases[raw]
    return MllMode(raw)


def normalized_variant(value: MllMode | MllVariant | str) -> MllVariant:
    mode = normalized_mode(value)
    return (
        MllVariant.EOD_REALIZED_BALANCE
        if mode is MllMode.EOD_LEVEL_RT_BREACH
        else MllVariant.INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST
    )


def advance_intraday_floor(
    floor: float,
    *,
    live_equity_high: float,
    distance: float,
    lock: float,
    variant: MllMode | MllVariant | str,
) -> float:
    """Advance the floor from live equity only for the intraday-HWM variant."""

    if normalized_mode(variant) is MllMode.EOD_LEVEL_RT_BREACH:
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
    "MllMode",
    "MllVariant",
    "advance_end_of_day_floor",
    "advance_intraday_floor",
    "favorable_first_is_ambiguous",
    "normalized_mode",
    "normalized_variant",
]
