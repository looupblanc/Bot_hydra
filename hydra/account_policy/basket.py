from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    OpenExposure,
    route_entry,
    static_route_entry,
)
from hydra.account_policy.schema import BasketPolicy, ControllerPolicy
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent
from hydra.propfirm.mll_variants import (
    advance_end_of_day_floor,
    advance_intraday_floor,
)
from hydra.propfirm.rolling_combine import (
    EpisodeStartPolicy,
    select_episode_starts,
)
from hydra.propfirm.topstep_150k import Topstep150KConfig


@dataclass(frozen=True, slots=True)
class RoutedTrade:
    component_id: str
    market: str
    side: int
    event: TradePathEvent

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "market": self.market,
            "side": self.side,
            "event": self.event.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RoutedTrade":
        event = dict(value["event"])
        return cls(
            component_id=str(value["component_id"]),
            market=str(value["market"]),
            side=int(value["side"]),
            event=TradePathEvent(**event),
        )


@dataclass(frozen=True, slots=True)
class AccountPolicyEpisode:
    policy_id: str
    start_day: int
    end_day: int
    terminal: CombineTerminal
    terminal_reason: str
    eligible_days: int
    traded_days: int
    accepted_events: int
    skipped_events: int
    conflict_count: int
    net_pnl: float
    total_cost: float
    target_progress: float
    maximum_target_progress: float
    minimum_mll_buffer: float
    mll_breached: bool
    consistency_ok: bool
    best_day_concentration: float
    days_to_target: int | None
    maximum_mini_equivalent: float
    maximum_net_directional_exposure: float
    shared_loss_days: int
    component_contribution: dict[str, float]
    skipped_reasons: dict[str, int]
    risk_allocation_path: tuple[dict[str, Any], ...]
    daily_path: tuple[dict[str, Any], ...]

    @property
    def passed(self) -> bool:
        return self.terminal is CombineTerminal.PASSED

    def to_dict(self, *, include_paths: bool = False) -> dict[str, Any]:
        row = asdict(self)
        row["terminal"] = self.terminal.value
        row["passed"] = self.passed
        if not include_paths:
            row.pop("risk_allocation_path", None)
            row.pop("daily_path", None)
        return row


@dataclass(frozen=True, slots=True)
class AccountPolicyRollingSummary:
    policy_id: str
    policy_kind: str
    episode_start_days: tuple[int, ...]
    episode_start_count: int
    effective_block_count: int
    pass_count: int
    pass_rate: float
    mll_breach_count: int
    mll_breach_rate: float
    timeout_rate: float
    compliance_failure_count: int
    target_progress_p25: float
    target_progress_median: float
    target_progress_p75: float
    maximum_target_progress: float
    median_days_to_target: float | None
    days_per_thousand_progress: float | None
    projected_days_to_target: float | None
    minimum_mll_buffer: float
    consistency_pass_rate: float
    median_episode_net_pnl: float
    median_best_day_concentration: float
    median_shared_loss_days: float
    conflict_rate: float
    accepted_event_count: int
    skipped_event_count: int
    component_contribution: dict[str, float]
    terminal_distribution: dict[str, int]
    episodes: tuple[AccountPolicyEpisode, ...]

    def to_dict(self, *, include_episodes: bool = False) -> dict[str, Any]:
        row = asdict(self)
        if include_episodes:
            row["episodes"] = [episode.to_dict() for episode in self.episodes]
        else:
            row.pop("episodes", None)
        row["episode_start_days"] = list(self.episode_start_days)
        return row


@dataclass(slots=True)
class _OpenPosition:
    routed: RoutedTrade
    quantity: int
    mini_equivalent: float
    net_pnl: float
    gross_pnl: float
    worst_unrealized_pnl: float
    best_unrealized_pnl: float


def run_shared_account_episode(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_session_days: Sequence[int],
    *,
    basket: BasketPolicy,
    start_day: int,
    maximum_duration_days: int,
    controller: ControllerPolicy | None = None,
    config: Topstep150KConfig | None = None,
) -> AccountPolicyEpisode:
    rules = config or Topstep150KConfig()
    days = tuple(sorted({int(day) for day in eligible_session_days}))
    if start_day not in days:
        raise ValueError("start day is not eligible")
    start_index = days.index(start_day)
    episode_days = days[start_index : start_index + maximum_duration_days]
    if not episode_days:
        raise ValueError("episode has no days")
    selected_ids = set(basket.component_ids)
    trades = sorted(
        (
            trade
            for component_id, values in component_events.items()
            if component_id in selected_ids
            for trade in values
            if start_day <= trade.event.session_day <= episode_days[-1]
        ),
        key=lambda item: (
            item.event.session_day,
            item.event.decision_ns,
            _priority_index(basket, item.component_id),
            item.event.event_id,
        ),
    )
    by_day: dict[int, list[RoutedTrade]] = defaultdict(list)
    for trade in trades:
        by_day[int(trade.event.session_day)].append(trade)

    balance = float(rules.combine_starting_balance)
    floor = float(rules.combine_starting_mll)
    minimum_buffer = balance - floor
    required_target = float(rules.combine_profit_target)
    best_day = 0.0
    maximum_progress = 0.0
    consecutive_losing_days = 0
    accepted = skipped = conflicts = traded_days = 0
    max_mini = max_direction = total_cost = 0.0
    terminal = CombineTerminal.TIMEOUT
    terminal_reason = "maximum_evaluation_duration_reached"
    days_to_target: int | None = None
    contribution: dict[str, float] = defaultdict(float)
    skipped_reasons: Counter[str] = Counter()
    allocation: list[dict[str, Any]] = []
    daily_path: list[dict[str, Any]] = []
    shared_loss_days = 0

    for elapsed, day in enumerate(episode_days, start=1):
        open_positions: dict[str, _OpenPosition] = {}
        day_pnl = 0.0
        dll_triggered = False
        day_components: dict[str, float] = defaultdict(float)
        day_traded = False
        actions: list[tuple[int, int, int, str, RoutedTrade]] = []
        for trade in by_day.get(int(day), ()):
            priority = _priority_index(basket, trade.component_id)
            actions.append(
                (
                    trade.event.decision_ns,
                    1,
                    priority,
                    trade.event.event_id,
                    trade,
                )
            )
            actions.append(
                (
                    trade.event.exit_ns,
                    0,
                    priority,
                    trade.event.event_id,
                    trade,
                )
            )
        if basket.policy_version.startswith("hydra_account_policy_v7_2"):
            actions.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        else:
            # Preserve the frozen V6 event-ID ordering for historical replay.
            actions.sort(key=lambda item: (item[0], item[1], item[3]))
        for timestamp, kind, _priority, event_id, trade in actions:
            if kind == 0:
                position = open_positions.pop(event_id, None)
                if position is None:
                    continue
                balance += position.net_pnl
                day_pnl += position.net_pnl
                floor = advance_intraday_floor(
                    floor,
                    live_equity_high=balance,
                    distance=float(rules.combine_max_loss_limit),
                    lock=float(rules.combine_starting_balance),
                    variant=rules.resolved_mll_mode,
                )
                contribution[trade.component_id] += position.net_pnl
                day_components[trade.component_id] += position.net_pnl
                minimum_buffer = min(minimum_buffer, balance - floor)
                if balance <= floor:
                    terminal = CombineTerminal.MLL_BREACH
                    terminal_reason = "realized_mll_touch_or_breach"
                    break
                if (
                    rules.use_optional_daily_loss_limit
                    and day_pnl <= -float(rules.optional_daily_loss_limit)
                ):
                    dll_triggered = True
                    open_positions.clear()
                    break
                continue

            event = trade.event
            if not event.session_compliant or not event.contract_limit_compliant:
                terminal = CombineTerminal.COMPLIANCE_FAILURE
                terminal_reason = (
                    "session_policy_violation"
                    if not event.session_compliant
                    else "component_contract_limit_violation"
                )
                break
            exposures = tuple(
                OpenExposure(
                    component_id=position.routed.component_id,
                    market=position.routed.market,
                    side=position.routed.side,
                    mini_equivalent=position.mini_equivalent,
                    exit_ns=position.routed.event.exit_ns,
                )
                for position in open_positions.values()
            )
            state = AccountDecisionState(
                balance=balance,
                mll_floor=floor,
                mll_buffer=balance - floor,
                daily_realized_pnl=day_pnl,
                consecutive_losing_days=consecutive_losing_days,
                remaining_target=max(
                    0.0,
                    required_target
                    - (balance - float(rules.combine_starting_balance)),
                ),
                open_exposures=exposures,
            )
            intent = EntryIntent(
                event_id=event.event_id,
                component_id=trade.component_id,
                market=trade.market,
                side=trade.side,
                decision_ns=event.decision_ns,
                session_day=event.session_day,
                regime=event.regime,
                base_quantity=event.quantity,
                base_mini_equivalent=event.mini_equivalent,
            )
            decision = (
                route_entry(intent, state, policy=controller)
                if controller is not None
                else static_route_entry(
                    intent,
                    state,
                    policy_id=basket.policy_id,
                    component_priority=basket.component_priority
                    or basket.component_ids,
                    maximum_simultaneous_positions=basket.maximum_simultaneous_positions,
                    maximum_mini_equivalent=basket.maximum_mini_equivalent,
                )
            )
            allocation.append(
                {
                    "decision_ns": timestamp,
                    "session_day": int(day),
                    "component_id": trade.component_id,
                    **decision.to_dict(),
                }
            )
            if not decision.allow:
                skipped += 1
                skipped_reasons[decision.reason] += 1
                conflicts += int(
                    "CONFLICT" in decision.reason
                    or (
                        basket.policy_version.startswith(
                            "hydra_account_policy_v7_2"
                        )
                        and decision.reason
                        == "MAXIMUM_SIMULTANEOUS_POSITIONS"
                    )
                )
                continue
            ratio = decision.quantity / event.quantity
            position = _OpenPosition(
                routed=trade,
                quantity=decision.quantity,
                mini_equivalent=decision.mini_equivalent,
                net_pnl=float(event.net_pnl * ratio),
                gross_pnl=float(event.gross_pnl * ratio),
                worst_unrealized_pnl=float(event.worst_unrealized_pnl * ratio),
                best_unrealized_pnl=float(event.best_unrealized_pnl * ratio),
            )
            open_positions[event_id] = position
            accepted += 1
            day_traded = True
            total_cost += max(0.0, position.gross_pnl - position.net_pnl)
            total_mini = sum(item.mini_equivalent for item in open_positions.values())
            directional = sum(
                item.mini_equivalent * item.routed.side
                for item in open_positions.values()
            )
            max_mini = max(max_mini, total_mini)
            max_direction = max(max_direction, abs(directional))
            conservative_open_loss = sum(
                min(item.worst_unrealized_pnl, 0.0)
                for item in open_positions.values()
            )
            conservative_open_gain = sum(
                max(item.best_unrealized_pnl, 0.0)
                for item in open_positions.values()
            )
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance + conservative_open_gain,
                distance=float(rules.combine_max_loss_limit),
                lock=float(rules.combine_starting_balance),
                variant=rules.resolved_mll_mode,
            )
            intraday_low = balance + conservative_open_loss
            minimum_buffer = min(minimum_buffer, intraday_low - floor)
            if (
                rules.use_optional_daily_loss_limit
                and day_pnl + conservative_open_loss
                <= -float(rules.optional_daily_loss_limit)
            ):
                forced_total = min(
                    sum(item.net_pnl for item in open_positions.values()),
                    -float(rules.optional_daily_loss_limit) - day_pnl,
                )
                weights = {
                    item.routed.component_id: abs(item.worst_unrealized_pnl)
                    for item in open_positions.values()
                }
                weight_total = sum(weights.values()) or float(len(weights) or 1)
                for component_id, weight in weights.items():
                    share = forced_total * (weight or 1.0) / weight_total
                    contribution[component_id] += share
                    day_components[component_id] += share
                balance += forced_total
                day_pnl += forced_total
                open_positions.clear()
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
                        variant=rules.resolved_mll_mode,
                    )
                    dll_triggered = True
                break
            if intraday_low <= floor:
                terminal = CombineTerminal.MLL_BREACH
                terminal_reason = "correlated_open_position_mll_touch_or_breach"
                break
        if day_traded:
            traded_days += 1
        negative_components = sum(value < 0.0 for value in day_components.values())
        if negative_components >= 2:
            shared_loss_days += 1
        if terminal in {
            CombineTerminal.MLL_BREACH,
            CombineTerminal.COMPLIANCE_FAILURE,
        }:
            daily_path.append(
                {
                    "session_day": int(day),
                    "balance": balance,
                    "mll_floor": floor,
                    "day_pnl": day_pnl,
                    "dll_triggered": dll_triggered,
                }
            )
            break
        best_day = max(best_day, day_pnl)
        total_profit = balance - float(rules.combine_starting_balance)
        if (
            best_day
            > rules.combine_profit_target
            * rules.consistency_best_day_max_pct_of_profit_target
        ):
            required_target = max(
                required_target,
                best_day
                / rules.consistency_best_day_max_pct_of_profit_target,
            )
        maximum_progress = max(
            maximum_progress, total_profit / max(required_target, 1.0)
        )
        concentration = best_day / total_profit if total_profit > 0 else 0.0
        consistency_ok = bool(
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
        consecutive_losing_days = (
            consecutive_losing_days + 1 if day_pnl < 0 else 0
        )
        daily_path.append(
            {
                "session_day": int(day),
                "balance": balance,
                "mll_floor": floor,
                "day_pnl": day_pnl,
                "dll_triggered": dll_triggered,
            }
        )
        if (
            total_profit >= required_target
            and consistency_ok
            and traded_days >= int(rules.minimum_pass_days)
        ):
            terminal = CombineTerminal.PASSED
            terminal_reason = "shared_target_consistency_and_minimum_days_satisfied"
            days_to_target = elapsed
            break

    net = balance - float(rules.combine_starting_balance)
    concentration = best_day / net if net > 0 else 0.0
    consistency_ok = bool(
        net > 0
        and concentration
        <= rules.consistency_best_day_max_pct_of_profit_target + 1e-12
    )
    return AccountPolicyEpisode(
        policy_id=controller.controller_id if controller else basket.policy_id,
        start_day=int(start_day),
        end_day=int(daily_path[-1]["session_day"]),
        terminal=terminal,
        terminal_reason=terminal_reason,
        eligible_days=len(daily_path),
        traded_days=traded_days,
        accepted_events=accepted,
        skipped_events=skipped,
        conflict_count=conflicts,
        net_pnl=float(net),
        total_cost=float(total_cost),
        target_progress=float(net / max(required_target, 1.0)),
        maximum_target_progress=float(maximum_progress),
        minimum_mll_buffer=float(minimum_buffer),
        mll_breached=terminal is CombineTerminal.MLL_BREACH,
        consistency_ok=consistency_ok,
        best_day_concentration=float(concentration),
        days_to_target=days_to_target,
        maximum_mini_equivalent=float(max_mini),
        maximum_net_directional_exposure=float(max_direction),
        shared_loss_days=shared_loss_days,
        component_contribution=dict(sorted(contribution.items())),
        skipped_reasons=dict(sorted(skipped_reasons.items())),
        risk_allocation_path=tuple(allocation),
        daily_path=tuple(daily_path),
    )


def evaluate_account_policy(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_session_days: Sequence[int],
    *,
    basket: BasketPolicy,
    controller: ControllerPolicy | None = None,
    episode_policy: EpisodeStartPolicy | None = None,
    explicit_start_days: Sequence[int] | None = None,
    config: Topstep150KConfig | None = None,
) -> AccountPolicyRollingSummary:
    policy = episode_policy or EpisodeStartPolicy()
    day_regimes = _past_only_day_regimes(component_events, eligible_session_days)
    starts = tuple(int(day) for day in explicit_start_days) if explicit_start_days else select_episode_starts(
        eligible_session_days, day_regimes=day_regimes, policy=policy
    )
    if not starts:
        raise ValueError("account policy needs at least one episode start")
    episodes = tuple(
        run_shared_account_episode(
            component_events,
            eligible_session_days,
            basket=basket,
            controller=controller,
            start_day=start,
            maximum_duration_days=policy.maximum_duration_sessions,
            config=config,
        )
        for start in starts
    )
    terminals = Counter(episode.terminal.value for episode in episodes)
    progress = np.asarray([episode.target_progress for episode in episodes], dtype=float)
    maximum_progress = np.asarray(
        [episode.maximum_target_progress for episode in episodes], dtype=float
    )
    net = np.asarray([episode.net_pnl for episode in episodes], dtype=float)
    buffer = np.asarray(
        [episode.minimum_mll_buffer for episode in episodes], dtype=float
    )
    concentration = np.asarray(
        [episode.best_day_concentration for episode in episodes], dtype=float
    )
    passing_days = [
        float(episode.days_to_target)
        for episode in episodes
        if episode.days_to_target is not None
    ]
    positive_velocity = [
        episode.net_pnl / max(episode.eligible_days, 1)
        for episode in episodes
        if episode.net_pnl > 0
    ]
    median_velocity = float(np.median(positive_velocity)) if positive_velocity else 0.0
    component_contribution: dict[str, float] = defaultdict(float)
    for episode in episodes:
        for component_id, value in episode.component_contribution.items():
            component_contribution[component_id] += value / len(episodes)
    accepted = sum(episode.accepted_events for episode in episodes)
    skipped = sum(episode.skipped_events for episode in episodes)
    return AccountPolicyRollingSummary(
        policy_id=controller.controller_id if controller else basket.policy_id,
        policy_kind=(controller.kind.value if controller else basket.kind.value),
        episode_start_days=starts,
        episode_start_count=len(episodes),
        effective_block_count=_effective_blocks(
            starts, eligible_session_days, policy.maximum_duration_sessions
        ),
        pass_count=terminals[CombineTerminal.PASSED.value],
        pass_rate=terminals[CombineTerminal.PASSED.value] / len(episodes),
        mll_breach_count=terminals[CombineTerminal.MLL_BREACH.value],
        mll_breach_rate=terminals[CombineTerminal.MLL_BREACH.value] / len(episodes),
        timeout_rate=terminals[CombineTerminal.TIMEOUT.value] / len(episodes),
        compliance_failure_count=terminals[
            CombineTerminal.COMPLIANCE_FAILURE.value
        ],
        target_progress_p25=float(np.percentile(progress, 25)),
        target_progress_median=float(np.median(progress)),
        target_progress_p75=float(np.percentile(progress, 75)),
        maximum_target_progress=float(np.max(maximum_progress)),
        median_days_to_target=(
            float(np.median(passing_days)) if passing_days else None
        ),
        days_per_thousand_progress=(
            float(1000.0 / median_velocity) if median_velocity > 0 else None
        ),
        projected_days_to_target=(
            float(9000.0 / median_velocity) if median_velocity > 0 else None
        ),
        minimum_mll_buffer=float(np.min(buffer)),
        consistency_pass_rate=float(
            np.mean([episode.consistency_ok for episode in episodes])
        ),
        median_episode_net_pnl=float(np.median(net)),
        median_best_day_concentration=float(np.median(concentration)),
        median_shared_loss_days=float(
            np.median([episode.shared_loss_days for episode in episodes])
        ),
        conflict_rate=float(
            sum(episode.conflict_count for episode in episodes)
            / max(accepted + skipped, 1)
        ),
        accepted_event_count=accepted,
        skipped_event_count=skipped,
        component_contribution=dict(sorted(component_contribution.items())),
        terminal_distribution=dict(sorted(terminals.items())),
        episodes=episodes,
    )


def _past_only_day_regimes(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_days: Sequence[int],
) -> dict[int, str]:
    by_day: dict[int, list[str]] = defaultdict(list)
    for values in component_events.values():
        for trade in values:
            by_day[trade.event.session_day].append(trade.event.regime)
    output: dict[int, str] = {}
    previous = "UNKNOWN"
    for day in sorted({int(value) for value in eligible_days}):
        output[day] = previous
        values = by_day.get(day, ())
        if values:
            previous = Counter(values).most_common(1)[0][0]
    return output


def _priority_index(basket: BasketPolicy, component_id: str) -> int:
    priority = basket.component_priority or basket.component_ids
    return priority.index(component_id)


def _effective_blocks(
    starts: Sequence[int], days: Sequence[int], duration: int
) -> int:
    positions = {day: index for index, day in enumerate(sorted(set(days)))}
    retained = 0
    next_position = -1
    for start in starts:
        position = positions[int(start)]
        if position < next_position:
            continue
        retained += 1
        next_position = position + duration
    return retained


__all__ = [
    "AccountPolicyEpisode",
    "AccountPolicyRollingSummary",
    "RoutedTrade",
    "evaluate_account_policy",
    "run_shared_account_episode",
]
