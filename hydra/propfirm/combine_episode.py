from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Iterable, Sequence

from hydra.propfirm.mll_variants import (
    advance_end_of_day_floor,
    advance_intraday_floor,
)
from hydra.propfirm.topstep_150k import Topstep150KConfig


class CombineTerminal(StrEnum):
    PASSED = "PASSED"
    MLL_BREACH = "MLL_BREACH"
    TIMEOUT = "TIMEOUT"
    COMPLIANCE_FAILURE = "COMPLIANCE_FAILURE"


@dataclass(frozen=True, slots=True)
class TradePathEvent:
    event_id: str
    decision_ns: int
    exit_ns: int
    session_day: int
    net_pnl: float
    gross_pnl: float
    worst_unrealized_pnl: float
    best_unrealized_pnl: float
    quantity: int
    mini_equivalent: float
    regime: str = "UNKNOWN"
    session_compliant: bool = True
    contract_limit_compliant: bool = True
    same_bar_ambiguous: bool = False

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id must be non-empty")
        if self.exit_ns < self.decision_ns:
            raise ValueError("event exit cannot precede its decision")
        if self.quantity <= 0 or self.mini_equivalent <= 0:
            raise ValueError("event sizing must be positive")
        for name in (
            "net_pnl",
            "gross_pnl",
            "worst_unrealized_pnl",
            "best_unrealized_pnl",
            "mini_equivalent",
        ):
            if not math.isfinite(float(getattr(self, name))):
                raise ValueError(f"{name} must be finite")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CombineEpisodeResult:
    start_day: int
    end_day: int
    start_regime: str
    terminal: CombineTerminal
    terminal_reason: str
    eligible_days: int
    traded_days: int
    event_count: int
    days_to_target: int | None
    net_pnl: float
    target_progress: float
    required_target: float
    minimum_mll_buffer: float
    mll_breached: bool
    consistency_ok: bool
    best_day_profit: float
    best_day_concentration: float
    contract_limit_compliant: bool
    session_compliant: bool
    same_bar_ambiguous_count: int
    maximum_mini_equivalent: float
    worst_day_loss: float
    max_consecutive_losing_days: int
    daily_path: tuple[dict[str, float | int], ...]

    @property
    def passed(self) -> bool:
        return self.terminal == CombineTerminal.PASSED

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["terminal"] = self.terminal.value
        payload["passed"] = self.passed
        return payload


def run_combine_episode(
    events: Sequence[TradePathEvent],
    eligible_session_days: Sequence[int],
    *,
    start_day: int,
    maximum_duration_days: int = 60,
    config: Topstep150KConfig | None = None,
    maximum_mini_equivalent: float = 15.0,
    start_regime: str | None = None,
) -> CombineEpisodeResult:
    """Replay one immutable account episode in chronological order.

    MLL is checked on each event using conservative unrealized OHLC loss.  Its
    advancement follows the explicitly versioned ``config.mll_variant``.
    """

    rules = config or Topstep150KConfig()
    if maximum_duration_days <= 0:
        raise ValueError("maximum_duration_days must be positive")
    days = tuple(sorted({int(value) for value in eligible_session_days}))
    if start_day not in days:
        raise ValueError("start_day must be an eligible session day")
    start_index = days.index(start_day)
    episode_days = days[start_index : start_index + maximum_duration_days]
    if not episode_days:
        raise ValueError("episode has no eligible session days")
    end_day = int(episode_days[-1])
    ordered = sorted(
        (event for event in events if start_day <= event.session_day <= end_day),
        key=lambda event: (event.session_day, event.decision_ns, event.event_id),
    )
    if any(
        right.decision_ns < left.exit_ns
        for left, right in zip(ordered, ordered[1:], strict=False)
        if left.session_day == right.session_day
    ):
        raise ValueError("episode contains overlapping executable events")

    by_day: dict[int, list[TradePathEvent]] = {}
    for event in ordered:
        by_day.setdefault(int(event.session_day), []).append(event)
    resolved_start_regime = start_regime or next(
        (event.regime for event in ordered if event.session_day >= start_day),
        "NO_EVENT",
    )
    balance = float(rules.combine_starting_balance)
    floor = float(rules.combine_starting_mll)
    minimum_buffer = balance - floor
    daily_pnls: list[float] = []
    daily_path: list[dict[str, float | int]] = []
    event_count = 0
    traded_days = 0
    same_bar_ambiguous = 0
    maximum_size = 0.0
    best_day = 0.0
    required_target = float(rules.combine_profit_target)
    consistency_ok = True
    contract_ok = True
    session_ok = True
    terminal = CombineTerminal.TIMEOUT
    terminal_reason = "maximum_evaluation_duration_reached"
    days_to_target: int | None = None

    for elapsed, day in enumerate(episode_days, start=1):
        day_pnl = 0.0
        dll_triggered = False
        day_events = by_day.get(int(day), [])
        if day_events:
            traded_days += 1
        for event in day_events:
            event_count += 1
            maximum_size = max(maximum_size, float(event.mini_equivalent))
            same_bar_ambiguous += int(event.same_bar_ambiguous)
            event_contract_ok = bool(
                event.contract_limit_compliant
                and event.mini_equivalent <= maximum_mini_equivalent + 1e-12
            )
            contract_ok = contract_ok and event_contract_ok
            session_ok = session_ok and bool(event.session_compliant)
            if not event_contract_ok or not event.session_compliant:
                terminal = CombineTerminal.COMPLIANCE_FAILURE
                terminal_reason = (
                    "contract_limit_violation"
                    if not event_contract_ok
                    else "session_policy_violation"
                )
                break
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance
                + max(float(event.best_unrealized_pnl), 0.0),
                distance=float(rules.combine_max_loss_limit),
                lock=float(rules.combine_starting_balance),
                variant=rules.mll_variant,
            )
            adverse_pnl = min(float(event.worst_unrealized_pnl), 0.0)
            intraday_low = balance + adverse_pnl
            minimum_buffer = min(minimum_buffer, intraday_low - floor)
            if (
                rules.use_optional_daily_loss_limit
                and day_pnl + adverse_pnl
                <= -float(rules.optional_daily_loss_limit)
            ):
                forced_pnl = min(
                    float(event.net_pnl),
                    -float(rules.optional_daily_loss_limit) - day_pnl,
                )
                balance += forced_pnl
                day_pnl += forced_pnl
                minimum_buffer = min(minimum_buffer, balance - floor)
                if balance <= floor:
                    terminal = CombineTerminal.MLL_BREACH
                    terminal_reason = "dll_liquidation_mll_touch_or_breach"
                else:
                    floor = advance_intraday_floor(
                        floor,
                        live_equity_high=balance,
                        distance=float(rules.combine_max_loss_limit),
                        lock=float(rules.combine_starting_balance),
                        variant=rules.mll_variant,
                    )
                    dll_triggered = True
                break
            if intraday_low <= floor:
                terminal = CombineTerminal.MLL_BREACH
                terminal_reason = "intraday_unrealized_mll_touch_or_breach"
                break
            balance += float(event.net_pnl)
            day_pnl += float(event.net_pnl)
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance,
                distance=float(rules.combine_max_loss_limit),
                lock=float(rules.combine_starting_balance),
                variant=rules.mll_variant,
            )
            minimum_buffer = min(minimum_buffer, balance - floor)
            if balance <= floor:
                terminal = CombineTerminal.MLL_BREACH
                terminal_reason = "realized_mll_touch_or_breach"
                break
        daily_pnls.append(day_pnl)
        daily_path.append(
            {
                "session_day": int(day),
                "balance": float(balance),
                "mll_floor": float(floor),
                "day_pnl": float(day_pnl),
                "dll_triggered": dll_triggered,
            }
        )
        if terminal in {
            CombineTerminal.MLL_BREACH,
            CombineTerminal.COMPLIANCE_FAILURE,
        }:
            break

        total_profit = balance - float(rules.combine_starting_balance)
        best_day = max(best_day, day_pnl)
        if (
            best_day
            > rules.combine_profit_target
            * rules.consistency_best_day_max_pct_of_profit_target
        ):
            required_target = max(
                required_target,
                best_day / rules.consistency_best_day_max_pct_of_profit_target,
            )
        concentration = (
            best_day / total_profit if total_profit > 0 and best_day > 0 else 0.0
        )
        consistency_ok = (
            total_profit <= 0
            or concentration
            <= rules.consistency_best_day_max_pct_of_profit_target + 1e-12
        )
        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=float(rules.combine_max_loss_limit),
            lock=float(rules.combine_starting_balance),
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        if (
            total_profit >= required_target
            and consistency_ok
            and traded_days >= int(rules.minimum_pass_days)
        ):
            terminal = CombineTerminal.PASSED
            terminal_reason = "target_consistency_and_minimum_days_satisfied"
            days_to_target = elapsed
            break

    net = balance - float(rules.combine_starting_balance)
    concentration = best_day / net if net > 0 and best_day > 0 else 0.0
    return CombineEpisodeResult(
        start_day=int(start_day),
        end_day=int(daily_path[-1]["session_day"]),
        start_regime=resolved_start_regime,
        terminal=terminal,
        terminal_reason=terminal_reason,
        eligible_days=len(daily_path),
        traded_days=traded_days,
        event_count=event_count,
        days_to_target=days_to_target,
        net_pnl=float(net),
        target_progress=float(net / max(required_target, 1e-12)),
        required_target=float(required_target),
        minimum_mll_buffer=float(minimum_buffer),
        mll_breached=terminal == CombineTerminal.MLL_BREACH,
        consistency_ok=bool(consistency_ok),
        best_day_profit=float(best_day),
        best_day_concentration=float(concentration),
        contract_limit_compliant=bool(contract_ok),
        session_compliant=bool(session_ok),
        same_bar_ambiguous_count=same_bar_ambiguous,
        maximum_mini_equivalent=float(maximum_size),
        worst_day_loss=float(min(daily_pnls, default=0.0)),
        max_consecutive_losing_days=_max_consecutive_losing_days(daily_pnls),
        daily_path=tuple(daily_path),
    )


def events_to_daily_rows(
    events: Iterable[TradePathEvent],
    *,
    start_day: int,
    end_day: int,
) -> list[dict[str, float | int]]:
    grouped: dict[int, list[TradePathEvent]] = {}
    for event in sorted(events, key=lambda item: (item.session_day, item.decision_ns)):
        if start_day <= event.session_day <= end_day:
            grouped.setdefault(int(event.session_day), []).append(event)
    rows: list[dict[str, float | int]] = []
    for day, day_events in sorted(grouped.items()):
        running = 0.0
        worst = 0.0
        for event in day_events:
            worst = min(worst, running + min(event.worst_unrealized_pnl, 0.0))
            running += event.net_pnl
            worst = min(worst, running)
        rows.append(
            {
                "date": day,
                "pnl": float(running),
                "raw_pnl": float(sum(event.gross_pnl for event in day_events)),
                "worst_intraday_pnl": float(worst),
                "trades": len(day_events),
                "skipped_trades": 0,
            }
        )
    return rows


def _max_consecutive_losing_days(values: Sequence[float]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


__all__ = [
    "CombineEpisodeResult",
    "CombineTerminal",
    "TradePathEvent",
    "events_to_daily_rows",
    "run_combine_episode",
]
