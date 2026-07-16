"""Bounded XFA-only post-payout policy frontier.

The Combine outcome and underlying sleeve trades are inputs, never recomputed.
Each replay begins at a frozen XFA transition and changes only payout request
semantics plus the account-level risk applied *after* an executed payout.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_to_xfa import (
    FrozenRiskProfile,
    RuleSnapshot,
    XfaTerminal,
    _calendar_gap,
    _market_root,
    _parse_session_date,
    _priority,
    _scale_trade,
    official_rule_snapshot_2026_07_15,
)
from hydra.propfirm.mll_variants import advance_end_of_day_floor
from hydra.propfirm.xfa_source_tape import XfaSourceTape


POST_PAYOUT_POLICY_SCHEMA = "hydra_xfa_post_payout_policy_v1"
POST_PAYOUT_RESULT_SCHEMA = "hydra_xfa_post_payout_result_v1"
POST_PAYOUT_EVENT_SCHEMA = "hydra_xfa_post_payout_event_v1"
POST_PAYOUT_FRONTIER_VERSION = "hydra_xfa_post_payout_frontier_v1"
FROZEN_XFA_TRANSITION_SCHEMA = "hydra_frozen_xfa_transition_v1"
_STRESSED_TAPE_CACHE: dict[
    str, dict[str, tuple[RoutedTrade, ...]]
] = {}
_PREPARED_TRADE_CACHE: dict[
    tuple[str, str, str, int, int], tuple[RoutedTrade, ...]
] = {}


class RequestTiming(StrEnum):
    EARLIEST_ELIGIBLE_CLIPPED = "EARLIEST_ELIGIBLE_CLIPPED"
    FULL_TARGET_BUFFER_SAFE = "FULL_TARGET_BUFFER_SAFE"


class PayoutAmountMode(StrEnum):
    OFFICIAL_MAX = "OFFICIAL_MAX"
    HALF_ALLOWED = "HALF_ALLOWED"
    MINIMUM_125 = "MINIMUM_125"


class RecoveryCondition(StrEnum):
    HOLD_REDUCED_RISK = "HOLD_REDUCED_RISK"
    RECOVER_TO_LAST_PRE_PAYOUT_BALANCE = "RECOVER_TO_LAST_PRE_PAYOUT_BALANCE"


class DllScenario(StrEnum):
    NO_DLL = "NO_DLL"
    OPTIONAL_3000_SESSION_STOP = "OPTIONAL_3000_SESSION_STOP"


class FrontierRole(StrEnum):
    HARVEST = "HARVEST"
    BALANCED = "BALANCED"
    LONGEVITY = "LONGEVITY"


@dataclass(frozen=True, slots=True)
class FrozenXfaTransition:
    """Immutable hand-off from a previously sealed successful Combine path."""

    book_id: str
    scenario: str
    combine_start_id: str
    combine_start_day: int
    xfa_start_day: int
    combine_path_hash: str
    combine_terminal: str = "TARGET_REACHED"
    schema: str = FROZEN_XFA_TRANSITION_SCHEMA

    def __post_init__(self) -> None:
        if not self.book_id or not self.combine_start_id or not self.combine_path_hash:
            raise ValueError("frozen XFA transition provenance is incomplete")
        if self.scenario not in {"NORMAL", "STRESSED", "STRESSED_1_5X"}:
            raise ValueError("frozen XFA transition scenario is unsupported")
        if self.combine_terminal != "TARGET_REACHED":
            raise ValueError("only a sealed successful Combine may start XFA")
        if self.xfa_start_day < self.combine_start_day:
            raise ValueError("XFA cannot start before the frozen Combine path")
        if self.schema != FROZEN_XFA_TRANSITION_SCHEMA:
            raise ValueError("frozen XFA transition schema drift")

    @property
    def fingerprint(self) -> str:
        return stable_hash(self.to_dict(include_fingerprint=False))

    @property
    def transition_id(self) -> str:
        return "xfa_transition_" + self.fingerprint[:24]

    def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
        payload = asdict(self)
        if include_fingerprint:
            payload.update(
                {
                    "transition_id": self.transition_id,
                    "fingerprint": self.fingerprint,
                }
            )
        return payload


@dataclass(frozen=True, slots=True)
class XfaPostPayoutPolicy:
    book_id: str
    path: str
    role: FrontierRole
    request_timing: RequestTiming
    payout_amount_mode: PayoutAmountMode
    retained_buffer_usd: float
    post_payout_risk_scale: float
    recovery_condition: RecoveryCondition
    dll_scenario: DllScenario
    policy_version: str = POST_PAYOUT_FRONTIER_VERSION
    schema: str = POST_PAYOUT_POLICY_SCHEMA

    def __post_init__(self) -> None:
        if not self.book_id:
            raise ValueError("post-payout book_id is required")
        if self.path not in {"XFA_STANDARD", "XFA_CONSISTENCY"}:
            raise ValueError("post-payout path must be frozen Standard or Consistency")
        if self.post_payout_risk_scale not in {0.25, 0.5, 0.75, 1.0}:
            raise ValueError("post-payout risk scale is outside the frozen frontier")
        if self.retained_buffer_usd not in {0.0, 1_000.0, 2_000.0}:
            raise ValueError("retained buffer is outside the frozen frontier")
        expected = {
            FrontierRole.HARVEST: (
                RequestTiming.EARLIEST_ELIGIBLE_CLIPPED,
                PayoutAmountMode.OFFICIAL_MAX,
                0.0,
                RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE,
            ),
            FrontierRole.BALANCED: (
                RequestTiming.FULL_TARGET_BUFFER_SAFE,
                PayoutAmountMode.HALF_ALLOWED,
                1_000.0,
                RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE,
            ),
            FrontierRole.LONGEVITY: (
                RequestTiming.FULL_TARGET_BUFFER_SAFE,
                PayoutAmountMode.MINIMUM_125,
                2_000.0,
                RecoveryCondition.HOLD_REDUCED_RISK,
            ),
        }[self.role]
        actual = (
            self.request_timing,
            self.payout_amount_mode,
            float(self.retained_buffer_usd),
            self.recovery_condition,
        )
        if actual != expected:
            raise ValueError("post-payout role tuple drifted from preregistration")

    @property
    def policy_id(self) -> str:
        return "xfa_post_" + self.fingerprint[:24]

    @property
    def fingerprint(self) -> str:
        return stable_hash(self._payload())

    def _payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["role"] = self.role.value
        payload["request_timing"] = self.request_timing.value
        payload["payout_amount_mode"] = self.payout_amount_mode.value
        payload["recovery_condition"] = self.recovery_condition.value
        payload["dll_scenario"] = self.dll_scenario.value
        return payload

    def to_dict(self) -> dict[str, Any]:
        return {
            **self._payload(),
            "policy_id": self.policy_id,
            "fingerprint": self.fingerprint,
        }


@dataclass(frozen=True, slots=True)
class XfaPostPayoutResult:
    transition_id: str
    transition_fingerprint: str
    scenario: str
    combine_start_id: str
    policy_id: str
    book_id: str
    path: str
    start_day: int
    end_day: int
    terminal: XfaTerminal
    terminal_reason: str
    requested_horizon_days: int
    observed_days: int
    traded_days: int
    event_count: int
    accepted_event_count: int
    skipped_event_count: int
    payout_cycles: int
    gross_payout: float
    trader_net_payout: float
    first_payout_day: int | None
    ending_balance: float
    ending_mll_floor: float
    minimum_mll_buffer: float
    post_payout_survived: bool
    post_payout_censored: bool
    post_payout_observed_days: int
    dll_trigger_count: int
    payout_events: tuple[Mapping[str, Any], ...]
    daily_ledger: tuple[Mapping[str, Any], ...]
    component_contribution: Mapping[str, float]
    result_hash: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["terminal"] = self.terminal.value
        payload["payout_events"] = [dict(row) for row in self.payout_events]
        payload["daily_ledger"] = [dict(row) for row in self.daily_ledger]
        payload["component_contribution"] = dict(
            sorted(self.component_contribution.items())
        )
        return payload


@dataclass(slots=True)
class _Position:
    trade: RoutedTrade
    quantity: int
    mini_equivalent: float
    net_pnl: float
    gross_pnl: float
    worst_unrealized_pnl: float


def preregistered_post_payout_frontier(
    book_id: str, *, path: str = "XFA_CONSISTENCY"
) -> tuple[XfaPostPayoutPolicy, ...]:
    """Return the exact small 3 x 4 x 2 frontier: 24 policies per book."""

    role_contract = {
        FrontierRole.HARVEST: (
            RequestTiming.EARLIEST_ELIGIBLE_CLIPPED,
            PayoutAmountMode.OFFICIAL_MAX,
            0.0,
            RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE,
        ),
        FrontierRole.BALANCED: (
            RequestTiming.FULL_TARGET_BUFFER_SAFE,
            PayoutAmountMode.HALF_ALLOWED,
            1_000.0,
            RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE,
        ),
        FrontierRole.LONGEVITY: (
            RequestTiming.FULL_TARGET_BUFFER_SAFE,
            PayoutAmountMode.MINIMUM_125,
            2_000.0,
            RecoveryCondition.HOLD_REDUCED_RISK,
        ),
    }
    values = []
    for role in FrontierRole:
        timing, amount, buffer, recovery = role_contract[role]
        for risk_scale in (0.25, 0.5, 0.75, 1.0):
            for dll in (DllScenario.NO_DLL, DllScenario.OPTIONAL_3000_SESSION_STOP):
                values.append(
                    XfaPostPayoutPolicy(
                        book_id=book_id,
                        path=path,
                        role=role,
                        request_timing=timing,
                        payout_amount_mode=amount,
                        retained_buffer_usd=buffer,
                        post_payout_risk_scale=risk_scale,
                        recovery_condition=recovery,
                        dll_scenario=dll,
                    )
                )
    return tuple(values)


def run_xfa_only_from_transition(
    tape: XfaSourceTape,
    *,
    basket: BasketPolicy,
    frozen_xfa_profile: FrozenRiskProfile,
    transition: FrozenXfaTransition,
    policy: XfaPostPayoutPolicy,
    horizon_days: int = 120,
    rule_snapshot: RuleSnapshot | None = None,
) -> XfaPostPayoutResult:
    """Replay only XFA from an already-frozen Combine-to-XFA transition."""

    if basket.policy_id != policy.book_id:
        raise ValueError("post-payout policy does not reference the frozen book")
    if basket.policy_id != transition.book_id:
        raise ValueError("frozen XFA transition references another book")
    if transition.xfa_start_day not in tape.eligible_session_days:
        raise ValueError("frozen XFA transition is absent from the source calendar")
    if horizon_days < 1:
        raise ValueError("XFA-only horizon must be positive")
    if not frozen_xfa_profile.clip_to_xfa_scaling_plan:
        raise ValueError("frozen XFA profile must retain official scaling limits")
    rules = rule_snapshot or official_rule_snapshot_2026_07_15()
    events = (
        _stressed_events_for_tape(tape)
        if transition.scenario != "NORMAL"
        else {key: tuple(value) for key, value in tape.events.items()}
    )
    prepared_trades = _prepared_trade_timeline(
        tape=tape,
        component_events=events,
        basket=basket,
        scenario=transition.scenario,
        start_day=int(transition.xfa_start_day),
        horizon=int(horizon_days),
    )
    return _run_post_payout_path(
        events,
        tape.eligible_session_days,
        basket=basket,
        profile=frozen_xfa_profile,
        rules=rules,
        transition=transition,
        start_day=int(transition.xfa_start_day),
        horizon=int(horizon_days),
        policy=policy,
        prepared_trades=prepared_trades,
    )


def _run_post_payout_path(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_days: Sequence[int],
    *,
    basket: BasketPolicy,
    profile: FrozenRiskProfile,
    rules: RuleSnapshot,
    start_day: int,
    horizon: int,
    policy: XfaPostPayoutPolicy,
    transition: FrozenXfaTransition,
    prepared_trades: Sequence[RoutedTrade] | None = None,
) -> XfaPostPayoutResult:
    days = tuple(sorted({int(value) for value in eligible_days}))
    start_index = days.index(start_day)
    episode_days = days[start_index : start_index + horizon]
    trades = (
        tuple(prepared_trades)
        if prepared_trades is not None
        else _filter_and_sort_trades(
            component_events,
            basket=basket,
            first_day=episode_days[0],
            last_day=episode_days[-1],
        )
    )
    by_day: dict[int, list[RoutedTrade]] = defaultdict(list)
    for row in trades:
        by_day[int(row.event.session_day)].append(row)

    balance = float(rules.xfa_starting_balance)
    floor = float(rules.xfa_starting_floor)
    minimum_buffer = balance - floor
    winning_days = 0
    traded_days_cycle = 0
    total_profit_cycle = 0.0
    best_day_cycle = 0.0
    cycle_start_balance = balance
    payout_cycles = 0
    gross_payout = 0.0
    trader_net = 0.0
    first_payout_day: int | None = None
    last_payout_elapsed: int | None = None
    recovery_balance: float | None = None
    reduced_risk_active = False
    traded_days = 0
    event_count = 0
    accepted = 0
    skipped = 0
    dll_triggers = 0
    contribution: dict[str, float] = defaultdict(float)
    ledger: list[dict[str, Any]] = []
    payout_events: list[dict[str, Any]] = []
    terminal: XfaTerminal | None = None
    terminal_reason = ""
    last_activity_day = int(start_day)
    calendar_auditable = _parse_session_date(start_day) is not None

    for elapsed, day in enumerate(episode_days, start=1):
        gap = _calendar_gap(last_activity_day, int(day))
        calendar_auditable = calendar_auditable and gap is not None
        if gap is not None and gap > rules.inactivity_calendar_days:
            terminal = XfaTerminal.INACTIVITY_RISK
            terminal_reason = "more_than_30_calendar_days_without_xfa_trading_activity"
            ledger.append(
                _terminal_day(day, balance, floor, terminal, "INACTIVITY")
            )
            break

        if (
            reduced_risk_active
            and policy.recovery_condition
            is RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE
            and recovery_balance is not None
            and balance >= recovery_balance - 1e-12
        ):
            reduced_risk_active = False
        session_risk_scale = (
            policy.post_payout_risk_scale if reduced_risk_active else 1.0
        )
        session_multiplier = profile.risk_multiplier * session_risk_scale
        opening_balance = balance
        floor_open = floor
        session_limit = min(
            float(profile.maximum_mini_equivalent),
            rules.xfa_session_limit(opening_balance),
        )
        restricted_limits = {
            market: min(
                float(profile.maximum_mini_equivalent),
                rules.xfa_session_limit(opening_balance, market),
            )
            for market in rules.restricted_market_roots
        }
        open_positions: dict[str, _Position] = {}
        day_pnl = 0.0
        day_accepted = 0
        day_skipped = 0
        day_worst_equity = balance
        dll_triggered = False
        actions: list[tuple[int, int, int, str, RoutedTrade]] = []
        for trade in by_day.get(int(day), ()):
            priority = _priority(basket, trade.component_id)
            actions.append((trade.event.decision_ns, 1, priority, trade.event.event_id, trade))
            actions.append((trade.event.exit_ns, 0, priority, trade.event.event_id, trade))
        actions.sort(key=lambda row: (row[0], row[1], row[2], row[3]))

        for _timestamp, kind, _rank, event_id, base_trade in actions:
            if dll_triggered:
                if kind == 1:
                    skipped += 1
                    day_skipped += 1
                continue
            if kind == 0:
                position = open_positions.pop(event_id, None)
                if position is None:
                    continue
                balance += position.net_pnl
                day_pnl += position.net_pnl
                contribution[position.trade.component_id] += position.net_pnl
                minimum_buffer = min(minimum_buffer, balance - floor)
                day_worst_equity = min(day_worst_equity, balance)
                if balance <= floor:
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "realized_xfa_mll_touch_or_breach"
                    break
                if _dll_enabled(policy) and day_pnl <= -3_000.0:
                    dll_triggered = True
                    dll_triggers += 1
                    open_positions.clear()
                continue

            event_count += 1
            scaled_trade = _scale_trade(base_trade, session_multiplier)
            event = scaled_trade.event
            if not event.session_compliant or not event.contract_limit_compliant:
                terminal = XfaTerminal.HARD_RULE_FAILURE
                terminal_reason = (
                    "session_close_or_trading_hours_violation"
                    if not event.session_compliant
                    else "source_contract_limit_violation"
                )
                break
            if len(open_positions) >= min(
                basket.maximum_simultaneous_positions,
                profile.maximum_simultaneous_positions,
            ):
                skipped += 1
                day_skipped += 1
                continue
            if profile.same_market_exclusive and any(
                value.trade.market == scaled_trade.market
                and value.trade.event.exit_ns > event.decision_ns
                for value in open_positions.values()
            ):
                skipped += 1
                day_skipped += 1
                continue
            used = sum(value.mini_equivalent for value in open_positions.values())
            available = max(0.0, session_limit - used)
            root = _market_root(scaled_trade.market)
            if root in rules.restricted_market_roots:
                restricted_used = sum(
                    value.mini_equivalent
                    for value in open_positions.values()
                    if _market_root(value.trade.market) == root
                )
                available = min(
                    available,
                    max(0.0, restricted_limits[root] - restricted_used),
                )
            position = _position_with_limit(scaled_trade, available)
            if position is None:
                skipped += 1
                day_skipped += 1
                continue
            open_positions[event_id] = position
            accepted += 1
            day_accepted += 1
            conservative_loss = sum(
                min(value.worst_unrealized_pnl, 0.0)
                for value in open_positions.values()
            )
            intraday_low = balance + conservative_loss
            day_worst_equity = min(day_worst_equity, intraday_low)
            minimum_buffer = min(minimum_buffer, intraday_low - floor)
            if intraday_low <= floor:
                terminal = XfaTerminal.MLL_BREACHED
                terminal_reason = "intraday_unrealized_xfa_mll_touch_or_breach"
                break
            if _dll_enabled(policy) and day_pnl + conservative_loss <= -3_000.0:
                forced = -3_000.0 - day_pnl
                balance += forced
                day_pnl = -3_000.0
                open_positions.clear()
                dll_triggered = True
                dll_triggers += 1
                minimum_buffer = min(minimum_buffer, balance - floor)
                if balance <= floor:
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "dll_liquidation_mll_touch_or_breach"
                    break

        if terminal is None and open_positions:
            terminal = XfaTerminal.HARD_RULE_FAILURE
            terminal_reason = "open_position_remaining_after_session_close"
        traded = day_accepted > 0
        if traded:
            traded_days += 1
            traded_days_cycle += 1
            last_activity_day = int(day)
        if terminal is not None:
            ledger.append(
                {
                    **_terminal_day(day, balance, floor, terminal, terminal_reason),
                    "opening_balance": opening_balance,
                    "mll_floor_open": floor_open,
                    "day_pnl": day_pnl,
                    "accepted_events": day_accepted,
                    "skipped_events": day_skipped,
                    "dll_triggered": dll_triggered,
                    "session_risk_scale": session_risk_scale,
                }
            )
            break

        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=rules.maximum_loss_limit,
            lock=0.0,
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        if day_pnl >= rules.xfa_standard_winning_day_minimum:
            winning_days += 1
        total_profit_cycle += day_pnl
        best_day_cycle = max(best_day_cycle, day_pnl)
        consistency_ratio = (
            best_day_cycle / total_profit_cycle
            if total_profit_cycle > 0.0 and best_day_cycle > 0.0
            else math.inf
        )
        if policy.path == "XFA_STANDARD":
            payout_eligible = winning_days >= rules.xfa_standard_winning_days and (
                payout_cycles == 0
                or balance - cycle_start_balance
                >= rules.later_standard_cycle_minimum_profit - 1e-12
            )
            payout_cap = rules.standard_payout_cap
        else:
            payout_eligible = bool(
                traded_days_cycle >= rules.xfa_consistency_traded_days
                and total_profit_cycle > 0.0
                and consistency_ratio <= rules.xfa_consistency_limit + 1e-12
            )
            payout_cap = rules.consistency_payout_cap

        request = _payout_request(
            balance=balance,
            mll_floor=floor,
            eligible=payout_eligible and not dll_triggered,
            official_cap=payout_cap,
            minimum=rules.minimum_payout,
            policy=policy,
        )
        payout_gross = float(request["gross_payout"])
        payout_net = payout_gross * rules.trader_profit_split
        if payout_gross > 0.0:
            pre_payout_balance = balance
            if first_payout_day is None:
                first_payout_day = elapsed
            gross_payout += payout_gross
            trader_net += payout_net
            balance -= payout_gross
            floor = 0.0
            payout_cycles += 1
            last_payout_elapsed = elapsed
            reduced_risk_active = policy.post_payout_risk_scale < 1.0
            recovery_balance = pre_payout_balance
            winning_days = 0
            traded_days_cycle = 0
            total_profit_cycle = 0.0
            best_day_cycle = 0.0
            cycle_start_balance = balance
            payout_event = {
                "schema": POST_PAYOUT_EVENT_SCHEMA,
                "policy_id": policy.policy_id,
                "book_id": policy.book_id,
                "scenario": transition.scenario,
                "combine_start_id": transition.combine_start_id,
                "transition_id": transition.transition_id,
                "xfa_path": policy.path,
                "payout_cycle": payout_cycles,
                "eligibility_timestamp": int(day),
                "eligible_account_balance": pre_payout_balance,
                "gross_payout_request": float(request["target_gross"]),
                "balance_fraction_limit": float(request["balance_fraction_limit"]),
                "account_size_payout_cap": float(payout_cap),
                "payout_split": float(rules.trader_profit_split),
                "pre_payout_balance": pre_payout_balance,
                "gross_payout": payout_gross,
                "trader_net_payout": payout_net,
                "costs_or_fees": 0.0,
                "post_payout_balance": balance,
                "retained_buffer_usd": policy.retained_buffer_usd,
                "post_payout_risk_scale": policy.post_payout_risk_scale,
                "mll_before_payout": float(request["mll_before_payout"]),
                "mll_after_payout": floor,
                "reset_marker": True,
            }
            payout_event["event_fingerprint"] = stable_hash(payout_event)
            payout_events.append(payout_event)

        ledger.append(
            {
                "session_day": int(day),
                "opening_balance": opening_balance,
                "closing_balance": balance,
                "mll_floor_open": floor_open,
                "mll_floor_close": floor,
                "day_pnl": day_pnl,
                "worst_intraday_equity": day_worst_equity,
                "traded": traded,
                "accepted_events": day_accepted,
                "skipped_events": day_skipped,
                "dll_triggered": dll_triggered,
                "session_risk_scale": session_risk_scale,
                "payout_eligible": payout_eligible,
                "payout_requested": payout_gross > 0.0,
                "gross_payout": payout_gross,
                "trader_net_payout": payout_net,
                "payout_cycles": payout_cycles,
                "retained_buffer_usd": policy.retained_buffer_usd,
                "reduced_risk_active_next_session": reduced_risk_active,
                "terminal": None,
            }
        )

    if terminal is None:
        if len(episode_days) < horizon:
            terminal = XfaTerminal.DATA_CENSORED
            terminal_reason = "available_chronology_ended_before_xfa_horizon"
        else:
            terminal = XfaTerminal.SURVIVED_HORIZON
            terminal_reason = "frozen_xfa_horizon_survived"
    post_days = (
        0 if last_payout_elapsed is None else max(0, len(ledger) - last_payout_elapsed)
    )
    payload = {
        "schema": POST_PAYOUT_RESULT_SCHEMA,
        "transition_id": transition.transition_id,
        "transition_fingerprint": transition.fingerprint,
        "scenario": transition.scenario,
        "combine_start_id": transition.combine_start_id,
        "policy_id": policy.policy_id,
        "book_id": policy.book_id,
        "path": policy.path,
        "start_day": start_day,
        "end_day": int(ledger[-1]["session_day"]),
        "terminal": terminal.value,
        "terminal_reason": terminal_reason,
        "requested_horizon_days": horizon,
        "observed_days": len(ledger),
        "traded_days": traded_days,
        "event_count": event_count,
        "accepted_event_count": accepted,
        "skipped_event_count": skipped,
        "payout_cycles": payout_cycles,
        "gross_payout": gross_payout,
        "trader_net_payout": trader_net,
        "first_payout_day": first_payout_day,
        "ending_balance": balance,
        "ending_mll_floor": floor,
        "minimum_mll_buffer": minimum_buffer,
        "post_payout_survived": bool(
            payout_cycles > 0
            and post_days > 0
            and terminal is XfaTerminal.SURVIVED_HORIZON
        ),
        "post_payout_censored": bool(
            payout_cycles > 0 and terminal is XfaTerminal.DATA_CENSORED
        ),
        "post_payout_observed_days": post_days,
        "dll_trigger_count": dll_triggers,
        "payout_events": payout_events,
        "daily_ledger": ledger,
        "component_contribution": dict(sorted(contribution.items())),
    }
    result_hash = stable_hash(payload)
    return XfaPostPayoutResult(
        transition_id=transition.transition_id,
        transition_fingerprint=transition.fingerprint,
        scenario=transition.scenario,
        combine_start_id=transition.combine_start_id,
        policy_id=policy.policy_id,
        book_id=policy.book_id,
        path=policy.path,
        start_day=start_day,
        end_day=int(payload["end_day"]),
        terminal=terminal,
        terminal_reason=terminal_reason,
        requested_horizon_days=horizon,
        observed_days=len(ledger),
        traded_days=traded_days,
        event_count=event_count,
        accepted_event_count=accepted,
        skipped_event_count=skipped,
        payout_cycles=payout_cycles,
        gross_payout=float(gross_payout),
        trader_net_payout=float(trader_net),
        first_payout_day=first_payout_day,
        ending_balance=float(balance),
        ending_mll_floor=float(floor),
        minimum_mll_buffer=float(minimum_buffer),
        post_payout_survived=bool(payload["post_payout_survived"]),
        post_payout_censored=bool(payload["post_payout_censored"]),
        post_payout_observed_days=post_days,
        dll_trigger_count=dll_triggers,
        payout_events=tuple(payout_events),
        daily_ledger=tuple(ledger),
        component_contribution=dict(sorted(contribution.items())),
        result_hash=result_hash,
    )


def _payout_request(
    *,
    balance: float,
    mll_floor: float,
    eligible: bool,
    official_cap: float,
    minimum: float,
    policy: XfaPostPayoutPolicy,
) -> dict[str, float]:
    if not eligible or balance <= 0.0:
        return {
            "target_gross": 0.0,
            "gross_payout": 0.0,
            "balance_fraction_limit": max(0.0, balance * 0.50),
            "mll_before_payout": float(mll_floor),
        }
    official_allowed = min(balance * 0.50, official_cap)
    if policy.payout_amount_mode is PayoutAmountMode.OFFICIAL_MAX:
        target = official_allowed
    elif policy.payout_amount_mode is PayoutAmountMode.HALF_ALLOWED:
        target = official_allowed * 0.50
    else:
        target = minimum
    buffer_safe = max(0.0, balance - policy.retained_buffer_usd)
    if policy.request_timing is RequestTiming.EARLIEST_ELIGIBLE_CLIPPED:
        gross = min(target, buffer_safe)
    else:
        gross = target if target <= buffer_safe + 1e-12 else 0.0
    if gross < minimum - 1e-12:
        gross = 0.0
    return {
        "target_gross": float(target),
        "gross_payout": float(gross),
        "balance_fraction_limit": float(balance * 0.50),
        "mll_before_payout": float(mll_floor),
    }


def _position_with_limit(trade: RoutedTrade, available: float) -> _Position | None:
    event = trade.event
    per_contract = event.mini_equivalent / event.quantity
    if per_contract <= 0.0:
        return None
    quantity = min(
        event.quantity, int(math.floor((available + 1e-12) / per_contract))
    )
    if quantity < 1:
        return None
    ratio = quantity / event.quantity
    return _Position(
        trade=trade,
        quantity=quantity,
        mini_equivalent=float(event.mini_equivalent * ratio),
        net_pnl=float(event.net_pnl * ratio),
        gross_pnl=float(event.gross_pnl * ratio),
        worst_unrealized_pnl=float(event.worst_unrealized_pnl * ratio),
    )


def _stressed_events(
    values: Mapping[str, Sequence[RoutedTrade]],
) -> dict[str, tuple[RoutedTrade, ...]]:
    output: dict[str, tuple[RoutedTrade, ...]] = {}
    for component_id, rows in values.items():
        stressed = []
        for row in rows:
            event = row.event
            base_cost = max(0.0, event.gross_pnl - event.net_pnl)
            extra = 0.5 * base_cost
            stressed.append(
                replace(
                    row,
                    event=replace(
                        event,
                        event_id=f"{event.event_id}:portfolio_cost_stress_1_5x",
                        net_pnl=float(event.net_pnl - extra),
                        worst_unrealized_pnl=float(
                            event.worst_unrealized_pnl - extra
                        ),
                        best_unrealized_pnl=float(
                            event.best_unrealized_pnl - extra
                        ),
                    ),
                )
            )
        output[component_id] = tuple(stressed)
    return output


def _stressed_events_for_tape(
    tape: XfaSourceTape,
) -> dict[str, tuple[RoutedTrade, ...]]:
    """Materialize immutable 1.5x-cost events once per sealed source tape."""

    cached = _STRESSED_TAPE_CACHE.get(tape.tape_hash)
    if cached is not None:
        return cached
    stressed = _stressed_events(tape.events)
    # The operating study binds one source tape.  Refuse unbounded process
    # growth if this helper is reused by a future diagnostic.
    if len(_STRESSED_TAPE_CACHE) >= 2:
        _STRESSED_TAPE_CACHE.clear()
    _STRESSED_TAPE_CACHE[tape.tape_hash] = stressed
    return stressed


def _prepared_trade_timeline(
    *,
    tape: XfaSourceTape,
    component_events: Mapping[str, Sequence[RoutedTrade]],
    basket: BasketPolicy,
    scenario: str,
    start_day: int,
    horizon: int,
) -> tuple[RoutedTrade, ...]:
    key = (tape.tape_hash, basket.policy_id, scenario, start_day, horizon)
    cached = _PREPARED_TRADE_CACHE.get(key)
    if cached is not None:
        return cached
    days = tuple(sorted({int(value) for value in tape.eligible_session_days}))
    start_index = days.index(start_day)
    episode_days = days[start_index : start_index + horizon]
    prepared = _filter_and_sort_trades(
        component_events,
        basket=basket,
        first_day=episode_days[0],
        last_day=episode_days[-1],
    )
    if len(_PREPARED_TRADE_CACHE) >= 4_096:
        _PREPARED_TRADE_CACHE.clear()
    _PREPARED_TRADE_CACHE[key] = prepared
    return prepared


def _filter_and_sort_trades(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    *,
    basket: BasketPolicy,
    first_day: int,
    last_day: int,
) -> tuple[RoutedTrade, ...]:
    selected = set(basket.component_ids)
    return tuple(
        sorted(
            (
                row
                for component_id, values in component_events.items()
                if component_id in selected
                for row in values
                if first_day <= row.event.session_day <= last_day
            ),
            key=lambda row: (
                row.event.session_day,
                row.event.decision_ns,
                _priority(basket, row.component_id),
                row.event.event_id,
            ),
        )
    )


def _dll_enabled(policy: XfaPostPayoutPolicy) -> bool:
    return policy.dll_scenario is DllScenario.OPTIONAL_3000_SESSION_STOP


def _terminal_day(
    day: int,
    balance: float,
    floor: float,
    terminal: XfaTerminal,
    reason: str,
) -> dict[str, Any]:
    return {
        "session_day": int(day),
        "opening_balance": balance,
        "closing_balance": balance,
        "mll_floor_open": floor,
        "mll_floor_close": floor,
        "day_pnl": 0.0,
        "accepted_events": 0,
        "skipped_events": 0,
        "payout_requested": False,
        "terminal": terminal.value,
        "terminal_reason": reason,
    }


__all__ = [
    "DllScenario",
    "FROZEN_XFA_TRANSITION_SCHEMA",
    "FrontierRole",
    "FrozenXfaTransition",
    "POST_PAYOUT_EVENT_SCHEMA",
    "POST_PAYOUT_FRONTIER_VERSION",
    "POST_PAYOUT_POLICY_SCHEMA",
    "POST_PAYOUT_RESULT_SCHEMA",
    "PayoutAmountMode",
    "RecoveryCondition",
    "RequestTiming",
    "XfaPostPayoutPolicy",
    "XfaPostPayoutResult",
    "preregistered_post_payout_frontier",
    "run_xfa_only_from_transition",
]
