from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Literal, Sequence

from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.mll_variants import (
    advance_end_of_day_floor,
    advance_intraday_floor,
)
from hydra.propfirm.payout_cycles import payout_request
from hydra.propfirm.topstep_150k import Topstep150KConfig


class XfaTerminal(StrEnum):
    SURVIVED_WINDOW = "SURVIVED_WINDOW"
    MLL_BREACH = "MLL_BREACH"
    COMPLIANCE_FAILURE = "COMPLIANCE_FAILURE"


@dataclass(frozen=True, slots=True)
class XfaEpisodeResult:
    path: str
    start_day: int
    end_day: int
    terminal: XfaTerminal
    survived: bool
    post_payout_survived: bool
    payout_eligible: bool
    payout_cycles: int
    gross_payout: float
    trader_net_payout: float
    first_payout_day: int | None
    qualifying_winning_days: int
    traded_days: int
    event_count: int
    minimum_mll_buffer: float
    ending_balance: float
    consistency_margin: float
    contract_limit_compliant: bool
    session_compliant: bool
    terminal_reason: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["terminal"] = self.terminal.value
        return payload


def run_xfa_episode(
    events: Sequence[TradePathEvent],
    eligible_session_days: Sequence[int],
    *,
    start_day: int,
    maximum_duration_days: int = 120,
    path: Literal["STANDARD", "CONSISTENCY"] = "STANDARD",
    config: Topstep150KConfig | None = None,
) -> XfaEpisodeResult:
    rules = config or Topstep150KConfig()
    days = tuple(sorted({int(value) for value in eligible_session_days}))
    if start_day not in days:
        raise ValueError("start_day must be eligible")
    if maximum_duration_days <= 0:
        raise ValueError("maximum_duration_days must be positive")
    start_index = days.index(start_day)
    episode_days = days[start_index : start_index + maximum_duration_days]
    if not episode_days:
        raise ValueError("XFA episode has no eligible days")
    end_day = int(episode_days[-1])
    by_day: dict[int, list[TradePathEvent]] = {}
    for event in sorted(events, key=lambda item: (item.session_day, item.decision_ns)):
        if start_day <= event.session_day <= end_day:
            by_day.setdefault(int(event.session_day), []).append(event)

    balance = float(rules.funded_starting_balance)
    floor = float(rules.funded_starting_mll)
    minimum_buffer = balance - floor
    winning_days = 0
    traded_days_since_payout = 0
    total_profit_since_payout = 0.0
    best_day_since_payout = 0.0
    cycles = 0
    gross_payout = 0.0
    trader_net = 0.0
    first_payout_day: int | None = None
    total_qualifying_days = 0
    total_traded_days = 0
    event_count = 0
    contract_ok = True
    session_ok = True
    terminal = XfaTerminal.SURVIVED_WINDOW
    reason = "maximum_forward_window_completed"

    for elapsed, day in enumerate(episode_days, start=1):
        day_events = by_day.get(int(day), [])
        day_pnl = 0.0
        if day_events:
            total_traded_days += 1
            traded_days_since_payout += 1
        for event in day_events:
            event_count += 1
            allowed = _xfa_max_mini_equivalent(balance)
            event_contract_ok = bool(
                event.contract_limit_compliant
                and event.mini_equivalent <= allowed + 1e-12
            )
            contract_ok = contract_ok and event_contract_ok
            session_ok = session_ok and event.session_compliant
            if not event_contract_ok or not event.session_compliant:
                terminal = XfaTerminal.COMPLIANCE_FAILURE
                reason = (
                    "xfa_scaling_plan_violation"
                    if not event_contract_ok
                    else "session_policy_violation"
                )
                break
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance + max(event.best_unrealized_pnl, 0.0),
                distance=abs(float(rules.funded_starting_mll)),
                lock=0.0,
                variant=rules.mll_variant,
            )
            adverse_pnl = min(event.worst_unrealized_pnl, 0.0)
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
                    terminal = XfaTerminal.MLL_BREACH
                    reason = "dll_liquidation_mll_touch_or_breach"
                else:
                    floor = advance_intraday_floor(
                        floor,
                        live_equity_high=balance,
                        distance=abs(float(rules.funded_starting_mll)),
                        lock=0.0,
                        variant=rules.mll_variant,
                    )
                break
            if intraday_low <= floor:
                terminal = XfaTerminal.MLL_BREACH
                reason = "intraday_unrealized_mll_touch_or_breach"
                break
            balance += event.net_pnl
            day_pnl += event.net_pnl
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance,
                distance=abs(float(rules.funded_starting_mll)),
                lock=0.0,
                variant=rules.mll_variant,
            )
            minimum_buffer = min(minimum_buffer, balance - floor)
            if balance <= floor:
                terminal = XfaTerminal.MLL_BREACH
                reason = "realized_mll_touch_or_breach"
                break
        if terminal != XfaTerminal.SURVIVED_WINDOW:
            break

        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=abs(float(rules.funded_starting_mll)),
            lock=0.0,
        )
        total_profit_since_payout += day_pnl
        best_day_since_payout = max(best_day_since_payout, day_pnl)
        if day_pnl >= rules.payout_winning_day_min_profit:
            winning_days += 1
            total_qualifying_days += 1
        consistency_ratio = (
            best_day_since_payout / total_profit_since_payout
            if total_profit_since_payout > 0 and best_day_since_payout > 0
            else math.inf
        )
        if path == "STANDARD":
            eligible = winning_days >= rules.payout_eligibility_winning_days
            cap = rules.payout_cap
        else:
            eligible = (
                traded_days_since_payout >= 3
                and consistency_ratio
                <= rules.funded_consistency_largest_day_max_pct_of_total_profit
                + 1e-12
            )
            cap = 6000.0
        if eligible and balance > 0:
            payout = payout_request(
                balance,
                cap=cap,
                split=rules.profit_split_trader,
                pct=rules.payout_max_pct_of_balance,
            )
            if payout.eligible:
                if first_payout_day is None:
                    first_payout_day = elapsed
                gross_payout += payout.gross_payout
                trader_net += payout.trader_net
                balance -= payout.gross_payout
                floor = 0.0
                cycles += 1
                winning_days = 0
                traded_days_since_payout = 0
                # Consistency is measured over the new payout cycle.  The
                # residual account balance is not profit earned since the
                # just-completed payout and must not be carried into the next
                # cycle's denominator.
                total_profit_since_payout = 0.0
                best_day_since_payout = 0.0

    consistency_ratio = (
        best_day_since_payout / total_profit_since_payout
        if total_profit_since_payout > 0 and best_day_since_payout > 0
        else 0.0
    )
    threshold = (
        1.0
        if path == "STANDARD"
        else rules.funded_consistency_largest_day_max_pct_of_total_profit
    )
    return XfaEpisodeResult(
        path=f"XFA_{path}",
        start_day=int(start_day),
        end_day=int(episode_days[-1]),
        terminal=terminal,
        survived=terminal == XfaTerminal.SURVIVED_WINDOW,
        post_payout_survived=bool(
            cycles > 0 and terminal != XfaTerminal.MLL_BREACH
        ),
        payout_eligible=cycles > 0,
        payout_cycles=cycles,
        gross_payout=float(gross_payout),
        trader_net_payout=float(trader_net),
        first_payout_day=first_payout_day,
        qualifying_winning_days=total_qualifying_days,
        traded_days=total_traded_days,
        event_count=event_count,
        minimum_mll_buffer=float(minimum_buffer),
        ending_balance=float(balance),
        consistency_margin=float(threshold - consistency_ratio),
        contract_limit_compliant=contract_ok,
        session_compliant=session_ok,
        terminal_reason=reason,
    )


def _xfa_max_mini_equivalent(balance: float) -> float:
    if balance < 1500.0:
        return 3.0
    if balance < 2000.0:
        return 4.0
    if balance < 3000.0:
        return 5.0
    if balance < 4500.0:
        return 10.0
    return 15.0


__all__ = ["XfaEpisodeResult", "XfaTerminal", "run_xfa_episode"]
