"""Chronological active-risk-pool replay for causal sleeve trajectories.

Unlike the legacy event-summary adapter, this replay advances unrealized PnL
and the MLL one available bar at a time.  A future trade-wide MAE/MFE is never
applied at entry and therefore cannot influence a later governor decision.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_pool_replay import (
    AccountPolicyEpisode,
    AccountPolicyRollingSummary,
)
from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    route_active_risk_entry,
)
from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    OpenExposure,
)
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.mll_variants import (
    advance_end_of_day_floor,
    advance_intraday_floor,
)
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)


CAUSAL_ACCOUNT_REPLAY_VERSION = "hydra_causal_active_pool_account_replay_v1"


@dataclass(slots=True)
class _OpenPosition:
    trajectory: CausalTradeTrajectory
    quantity: int
    mini_equivalent: float
    ratio: float
    current_unrealized: float
    current_worst: float
    current_best: float


def _is_completed(trajectory: CausalTradeTrajectory) -> bool:
    return bool(getattr(trajectory, "completed", True))


def _censor_time(trajectory: CausalTradeTrajectory) -> int | None:
    value = getattr(trajectory, "censor_time_ns", None)
    return int(value) if value is not None else None


def _open_unrealized(
    positions: Mapping[str, _OpenPosition], *, bound: str = "current"
) -> float:
    attribute = {
        "current": "current_unrealized",
        "worst": "current_worst",
        "best": "current_best",
    }[bound]
    return float(sum(float(getattr(row, attribute)) for row in positions.values()))


def _live_equity(balance: float, positions: Mapping[str, _OpenPosition]) -> float:
    return float(balance + _open_unrealized(positions))


def run_causal_shared_account_episode(
    component_trajectories: Mapping[str, Sequence[CausalTradeTrajectory]],
    eligible_session_days: Sequence[int],
    *,
    policy: ActiveRiskPoolPolicy,
    start_day: int,
    maximum_duration_days: int,
    config: Topstep150KConfig | None = None,
) -> AccountPolicyEpisode:
    """Replay one account episode in true event/availability order."""

    rules = config or Topstep150KConfig()
    if maximum_duration_days <= 0:
        raise ValueError("causal account duration must be positive")
    days = tuple(sorted({int(day) for day in eligible_session_days}))
    if int(start_day) not in days:
        raise ValueError("causal account start is not eligible")
    start_index = days.index(int(start_day))
    episode_days = days[start_index : start_index + int(maximum_duration_days)]
    if not episode_days:
        raise ValueError("causal account episode has no observable days")
    selected = set(policy.component_ids)
    trajectories = sorted(
        (
            row
            for component_id, values in component_trajectories.items()
            if component_id in selected
            for row in values
            if int(start_day) <= row.event.session_day <= episode_days[-1]
        ),
        key=lambda row: (
            row.event.decision_ns,
            _priority(policy, row.component_id),
            row.event.event_id,
        ),
    )
    by_day: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
    for row in trajectories:
        by_day[int(row.event.session_day)].append(row)

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
        day_components: dict[str, float] = defaultdict(float)
        day_traded = False
        day_costs = 0.0
        day_conflicts = 0
        day_max_mini = 0.0
        day_max_direction = 0.0
        action_times: set[int] = set()
        marks: dict[int, list[tuple[CausalTradeTrajectory, CausalTradeMark]]] = (
            defaultdict(list)
        )
        entries: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
        exits: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
        censors: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
        for trajectory in by_day.get(int(day), ()):
            entries[int(trajectory.event.decision_ns)].append(trajectory)
            action_times.add(int(trajectory.event.decision_ns))
            if _is_completed(trajectory):
                exits[int(trajectory.event.exit_ns)].append(trajectory)
                action_times.add(int(trajectory.event.exit_ns))
            else:
                censor_time = _censor_time(trajectory)
                if censor_time is None:
                    raise ValueError("causal censored trajectory has no censor boundary")
                censors[censor_time].append(trajectory)
                action_times.add(censor_time)
            for mark in trajectory.marks:
                marks[int(mark.availability_time_ns)].append((trajectory, mark))
                action_times.add(int(mark.availability_time_ns))

        for timestamp in sorted(action_times):
            # The just-completed bar is available before orders at this same
            # boundary.  Aggregate all current-bar marks before advancing MLL.
            for trajectory, mark in marks.get(timestamp, ()):
                position = open_positions.get(trajectory.event.event_id)
                if position is None:
                    continue
                position.current_unrealized = float(
                    mark.current_unrealized_pnl * position.ratio
                )
                position.current_worst = float(
                    mark.worst_unrealized_pnl * position.ratio
                )
                position.current_best = float(
                    mark.best_unrealized_pnl * position.ratio
                )
            if marks.get(timestamp) and open_positions:
                floor = advance_intraday_floor(
                    floor,
                    live_equity_high=balance
                    + _open_unrealized(open_positions, bound="best"),
                    distance=float(rules.combine_max_loss_limit),
                    lock=float(rules.combine_starting_balance),
                    variant=rules.resolved_mll_mode,
                )
                conservative_low = balance + _open_unrealized(
                    open_positions, bound="worst"
                )
                minimum_buffer = min(
                    minimum_buffer, conservative_low - floor
                )
                if conservative_low <= floor:
                    terminal = CombineTerminal.MLL_BREACH
                    terminal_reason = "causal_current_bar_mll_touch_or_breach"
                    (
                        balance,
                        day_pnl,
                    ) = _force_liquidate_at_current_bound(
                        open_positions,
                        balance=balance,
                        day_pnl=day_pnl,
                        contribution=contribution,
                        day_components=day_components,
                        bound="worst",
                    )
                    minimum_buffer = min(minimum_buffer, balance - floor)
                    break

            # Fills at a boundary close existing trades before admitting new
            # entries, preserving the frozen non-overlap equality contract.
            for trajectory in sorted(
                exits.get(timestamp, ()),
                key=lambda row: (
                    _priority(policy, row.component_id),
                    row.event.event_id,
                ),
            ):
                position = open_positions.pop(trajectory.event.event_id, None)
                if position is None:
                    continue
                realized = float(trajectory.event.net_pnl * position.ratio)
                balance += realized
                day_pnl += realized
                contribution[trajectory.component_id] += realized
                day_components[trajectory.component_id] += realized
                floor = advance_intraday_floor(
                    floor,
                    live_equity_high=balance,
                    distance=float(rules.combine_max_loss_limit),
                    lock=float(rules.combine_starting_balance),
                    variant=rules.resolved_mll_mode,
                )
                minimum_buffer = min(minimum_buffer, balance - floor)
                if balance <= floor:
                    terminal = CombineTerminal.MLL_BREACH
                    terminal_reason = "causal_realized_mll_touch_or_breach"
                    break
            if terminal is CombineTerminal.MLL_BREACH:
                break

            # A causally filled trajectory whose required future bar is absent
            # remains an open economic path.  It is not flattened or converted
            # into a losing trade; the episode stops with explicit data censoring
            # at the first unavailable boundary.
            censored_open = [
                trajectory
                for trajectory in censors.get(timestamp, ())
                if trajectory.event.event_id in open_positions
            ]
            if censored_open:
                terminal = CombineTerminal.TIMEOUT
                terminal_reason = "CENSORED_FUTURE_COVERAGE"
                break

            for trajectory in sorted(
                entries.get(timestamp, ()),
                key=lambda row: (
                    _priority(policy, row.component_id),
                    row.event.event_id,
                ),
            ):
                event = trajectory.event
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
                        component_id=position.trajectory.component_id,
                        market=position.trajectory.market,
                        side=position.trajectory.side,
                        mini_equivalent=position.mini_equivalent,
                        exit_ns=position.trajectory.event.exit_ns,
                    )
                    for position in open_positions.values()
                )
                live_equity = _live_equity(balance, open_positions)
                state = AccountDecisionState(
                    balance=live_equity,
                    mll_floor=floor,
                    mll_buffer=live_equity - floor,
                    daily_realized_pnl=day_pnl,
                    consecutive_losing_days=consecutive_losing_days,
                    remaining_target=max(
                        0.0,
                        required_target
                        - (live_equity - float(rules.combine_starting_balance)),
                    ),
                    open_exposures=exposures,
                )
                intent = EntryIntent(
                    event_id=event.event_id,
                    component_id=trajectory.component_id,
                    market=trajectory.market,
                    side=trajectory.side,
                    decision_ns=event.decision_ns,
                    session_day=event.session_day,
                    regime=event.regime,
                    base_quantity=event.quantity,
                    base_mini_equivalent=event.mini_equivalent,
                )
                decision = route_active_risk_entry(intent, state, policy=policy)
                decision_row = {
                    "event_id": event.event_id,
                    "decision_ns": int(timestamp),
                    "exit_ns": int(event.exit_ns),
                    "session_day": int(day),
                    "component_id": trajectory.component_id,
                    "base_quantity": int(event.quantity),
                    "base_mini_equivalent": float(event.mini_equivalent),
                    "account_replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
                    **decision.to_dict(),
                }
                requested = int(decision_row.get("requested_quantity", event.quantity))
                suppressed = max(0, requested - int(decision.quantity))
                completed = _is_completed(trajectory)
                decision_row.update(
                    {
                        "foregone_expected_pnl": None,
                        "foregone_expected_pnl_status": (
                            "UNAVAILABLE_NO_FROZEN_PRE_OUTCOME_ESTIMATE"
                        ),
                        "foregone_realized_pnl_ex_post": (
                            float(event.net_pnl * suppressed / max(event.quantity, 1))
                            if completed
                            else None
                        ),
                        "foregone_realized_pnl_status": (
                            "OBSERVED_COMPLETE_PATH"
                            if completed
                            else "CENSORED_FUTURE_COVERAGE"
                        ),
                        "foregone_realized_pnl_used_for_routing": False,
                    }
                )
                allocation.append(decision_row)
                if not decision.allow:
                    skipped += 1
                    skipped_reasons[decision.reason] += 1
                    conflicts += int("CONFLICT" in decision.reason)
                    day_conflicts += int("CONFLICT" in decision.reason)
                    continue
                ratio = float(decision.quantity / event.quantity)
                immediate_cost = max(0.0, event.gross_pnl - event.net_pnl) * ratio
                initial_unrealized = float(
                    getattr(trajectory, "initial_unrealized_pnl", -immediate_cost)
                    * ratio
                )
                open_positions[event.event_id] = _OpenPosition(
                    trajectory=trajectory,
                    quantity=int(decision.quantity),
                    mini_equivalent=float(decision.mini_equivalent),
                    ratio=ratio,
                    current_unrealized=initial_unrealized,
                    current_worst=initial_unrealized,
                    current_best=initial_unrealized,
                )
                accepted += 1
                day_traded = True
                total_cost += immediate_cost
                day_costs += immediate_cost
                total_mini = sum(
                    position.mini_equivalent for position in open_positions.values()
                )
                directional = sum(
                    position.mini_equivalent * position.trajectory.side
                    for position in open_positions.values()
                )
                max_mini = max(max_mini, total_mini)
                max_direction = max(max_direction, abs(directional))
                day_max_mini = max(day_max_mini, total_mini)
                day_max_direction = max(day_max_direction, abs(directional))
                live_equity = _live_equity(balance, open_positions)
                floor = advance_intraday_floor(
                    floor,
                    live_equity_high=live_equity,
                    distance=float(rules.combine_max_loss_limit),
                    lock=float(rules.combine_starting_balance),
                    variant=rules.resolved_mll_mode,
                )
                minimum_buffer = min(minimum_buffer, live_equity - floor)
                if live_equity <= floor:
                    terminal = CombineTerminal.MLL_BREACH
                    terminal_reason = "causal_entry_cost_mll_touch_or_breach"
                    (
                        balance,
                        day_pnl,
                    ) = _force_liquidate_at_current_bound(
                        open_positions,
                        balance=balance,
                        day_pnl=day_pnl,
                        contribution=contribution,
                        day_components=day_components,
                        bound="current",
                    )
                    minimum_buffer = min(minimum_buffer, balance - floor)
                    break
            if terminal in {
                CombineTerminal.MLL_BREACH,
                CombineTerminal.COMPLIANCE_FAILURE,
            } or terminal_reason == "CENSORED_FUTURE_COVERAGE":
                break

        if day_traded:
            traded_days += 1
        if sum(value < 0.0 for value in day_components.values()) >= 2:
            shared_loss_days += 1
        if terminal in {
            CombineTerminal.MLL_BREACH,
            CombineTerminal.COMPLIANCE_FAILURE,
        } or terminal_reason == "CENSORED_FUTURE_COVERAGE":
            unrealized = _open_unrealized(open_positions)
            maximum_progress = max(
                maximum_progress,
                (
                    balance
                    + unrealized
                    - float(rules.combine_starting_balance)
                )
                / max(required_target, 1.0),
            )
            censored_components = {
                position.trajectory.component_id: float(position.current_unrealized)
                for position in open_positions.values()
            }
            row_components = dict(day_components)
            for component_id, value in censored_components.items():
                row_components[component_id] = (
                    float(row_components.get(component_id, 0.0)) + value
                )
            daily_path.append(
                _daily_row(
                    day=int(day),
                    balance=balance,
                    floor=floor,
                    starting_balance=float(rules.combine_starting_balance),
                    required_target=required_target,
                    day_pnl=day_pnl + unrealized,
                    best_day=max(best_day, day_pnl),
                    minimum_buffer=minimum_buffer,
                    day_costs=day_costs,
                    day_conflicts=day_conflicts,
                    day_max_mini=day_max_mini,
                    day_max_direction=day_max_direction,
                    day_components=row_components,
                    open_positions=len(open_positions),
                    unrealized_pnl=unrealized,
                    consistency_limit=float(
                        rules.consistency_best_day_max_pct_of_profit_target
                    ),
                )
            )
            break
        if open_positions:
            terminal = CombineTerminal.COMPLIANCE_FAILURE
            terminal_reason = "causal_session_end_open_position"
            daily_path.append(
                _daily_row(
                    day=int(day),
                    balance=balance,
                    floor=floor,
                    starting_balance=float(rules.combine_starting_balance),
                    required_target=required_target,
                    day_pnl=day_pnl,
                    best_day=max(best_day, day_pnl),
                    minimum_buffer=minimum_buffer,
                    day_costs=day_costs,
                    day_conflicts=day_conflicts,
                    day_max_mini=day_max_mini,
                    day_max_direction=day_max_direction,
                    day_components=day_components,
                    open_positions=len(open_positions),
                    unrealized_pnl=_open_unrealized(open_positions),
                    consistency_limit=float(
                        rules.consistency_best_day_max_pct_of_profit_target
                    ),
                )
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
        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=float(rules.combine_max_loss_limit),
            lock=float(rules.combine_starting_balance),
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        consecutive_losing_days = (
            consecutive_losing_days + 1 if day_pnl < 0.0 else 0
        )
        daily_path.append(
            _daily_row(
                day=int(day),
                balance=balance,
                floor=floor,
                starting_balance=float(rules.combine_starting_balance),
                required_target=required_target,
                day_pnl=day_pnl,
                best_day=best_day,
                minimum_buffer=minimum_buffer,
                day_costs=day_costs,
                day_conflicts=day_conflicts,
                day_max_mini=day_max_mini,
                day_max_direction=day_max_direction,
                day_components=day_components,
                open_positions=0,
                unrealized_pnl=0.0,
                consistency_limit=float(
                    rules.consistency_best_day_max_pct_of_profit_target
                ),
            )
        )
        concentration = best_day / total_profit if total_profit > 0.0 else 0.0
        consistency_ok = bool(
            total_profit <= 0.0
            or concentration
            <= rules.consistency_best_day_max_pct_of_profit_target + 1e-12
        )
        if (
            total_profit >= required_target
            and consistency_ok
            and traded_days >= int(rules.minimum_pass_days)
        ):
            terminal = CombineTerminal.PASSED
            terminal_reason = "causal_shared_target_consistency_and_minimum_days"
            days_to_target = elapsed
            break

    ending_unrealized = _open_unrealized(open_positions) if "open_positions" in locals() else 0.0
    net = balance + ending_unrealized - float(rules.combine_starting_balance)
    reported_contribution = dict(contribution)
    if ending_unrealized:
        for position in open_positions.values():
            component_id = position.trajectory.component_id
            reported_contribution[component_id] = float(
                reported_contribution.get(component_id, 0.0)
                + position.current_unrealized
            )
    concentration = best_day / net if net > 0.0 else 0.0
    consistency_ok = bool(
        net <= 0.0
        or concentration
        <= rules.consistency_best_day_max_pct_of_profit_target + 1e-12
    )
    return AccountPolicyEpisode(
        policy_id=policy.policy_id,
        start_day=int(start_day),
        end_day=int(episode_days[min(len(daily_path), len(episode_days)) - 1]),
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
        component_contribution=dict(sorted(reported_contribution.items())),
        skipped_reasons=dict(sorted(skipped_reasons.items())),
        risk_allocation_path=tuple(allocation),
        daily_path=tuple(daily_path),
    )


def evaluate_causal_account_policy(
    component_trajectories: Mapping[str, Sequence[CausalTradeTrajectory]],
    eligible_session_days: Sequence[int],
    *,
    policy: ActiveRiskPoolPolicy,
    episode_policy: EpisodeStartPolicy | None = None,
    explicit_start_days: Sequence[int] | None = None,
    config: Topstep150KConfig | None = None,
) -> AccountPolicyRollingSummary:
    replay_policy = episode_policy or EpisodeStartPolicy()
    starts = (
        tuple(int(day) for day in explicit_start_days)
        if explicit_start_days
        else select_episode_starts(eligible_session_days, policy=replay_policy)
    )
    if not starts:
        raise ValueError("causal account policy needs starts")
    episodes = tuple(
        run_causal_shared_account_episode(
            component_trajectories,
            eligible_session_days,
            policy=policy,
            start_day=start,
            maximum_duration_days=replay_policy.maximum_duration_sessions,
            config=config,
        )
        for start in starts
    )
    return _summary(policy, starts, episodes)


def _summary(
    policy: ActiveRiskPoolPolicy,
    starts: Sequence[int],
    episodes: Sequence[AccountPolicyEpisode],
) -> AccountPolicyRollingSummary:
    terminals = Counter(row.terminal.value for row in episodes)
    progress = np.asarray([row.target_progress for row in episodes], dtype=float)
    maximum_progress = np.asarray(
        [row.maximum_target_progress for row in episodes], dtype=float
    )
    net = np.asarray([row.net_pnl for row in episodes], dtype=float)
    buffers = np.asarray([row.minimum_mll_buffer for row in episodes], dtype=float)
    concentration = np.asarray(
        [row.best_day_concentration for row in episodes], dtype=float
    )
    passing_days = [
        float(row.days_to_target)
        for row in episodes
        if row.days_to_target is not None
    ]
    velocities = [
        row.net_pnl / max(row.eligible_days, 1)
        for row in episodes
        if row.net_pnl > 0.0
    ]
    median_velocity = float(np.median(velocities)) if velocities else 0.0
    accepted = sum(row.accepted_events for row in episodes)
    skipped = sum(row.skipped_events for row in episodes)
    contribution: dict[str, float] = defaultdict(float)
    for episode in episodes:
        for component_id, value in episode.component_contribution.items():
            contribution[component_id] += float(value)
    count = len(episodes)
    return AccountPolicyRollingSummary(
        policy_id=policy.policy_id,
        policy_kind=CAUSAL_ACCOUNT_REPLAY_VERSION,
        episode_start_days=tuple(int(day) for day in starts),
        episode_start_count=count,
        effective_block_count=0,
        pass_count=terminals[CombineTerminal.PASSED.value],
        pass_rate=terminals[CombineTerminal.PASSED.value] / count,
        mll_breach_count=terminals[CombineTerminal.MLL_BREACH.value],
        mll_breach_rate=terminals[CombineTerminal.MLL_BREACH.value] / count,
        timeout_rate=terminals[CombineTerminal.TIMEOUT.value] / count,
        compliance_failure_count=terminals[
            CombineTerminal.COMPLIANCE_FAILURE.value
        ],
        target_progress_p25=float(np.quantile(progress, 0.25)),
        target_progress_median=float(np.median(progress)),
        target_progress_p75=float(np.quantile(progress, 0.75)),
        maximum_target_progress=float(np.max(maximum_progress, initial=0.0)),
        median_days_to_target=(
            float(np.median(passing_days)) if passing_days else None
        ),
        days_per_thousand_progress=(
            float(1000.0 / median_velocity) if median_velocity > 0.0 else None
        ),
        projected_days_to_target=(
            float(9000.0 / median_velocity) if median_velocity > 0.0 else None
        ),
        minimum_mll_buffer=float(np.min(buffers, initial=4500.0)),
        consistency_pass_rate=float(
            sum(row.consistency_ok for row in episodes) / count
        ),
        median_episode_net_pnl=float(np.median(net)),
        median_best_day_concentration=float(np.median(concentration)),
        median_shared_loss_days=float(
            np.median([row.shared_loss_days for row in episodes])
        ),
        conflict_rate=float(sum(row.conflict_count for row in episodes) / max(accepted + skipped, 1)),
        accepted_event_count=accepted,
        skipped_event_count=skipped,
        component_contribution=dict(sorted(contribution.items())),
        terminal_distribution=dict(sorted(terminals.items())),
        episodes=tuple(episodes),
    )


def _priority(policy: ActiveRiskPoolPolicy, component_id: str) -> int:
    return policy.component_priority.index(component_id)


def _force_liquidate_at_current_bound(
    positions: dict[str, _OpenPosition],
    *,
    balance: float,
    day_pnl: float,
    contribution: dict[str, float],
    day_components: dict[str, float],
    bound: str,
) -> tuple[float, float]:
    """Materialize the economic state used to declare an MLL breach.

    The old adapter set a terminal flag while leaving both balance and sleeve
    attribution untouched.  That made a breached account appear to have zero
    net PnL.  Here the conservative *current-bar* bound which caused the breach
    is also the explicit forced-liquidation accounting basis.  It is never a
    future trade-wide MAE.
    """

    attribute = {
        "current": "current_unrealized",
        "worst": "current_worst",
    }.get(bound)
    if attribute is None:
        raise ValueError("unsupported causal liquidation bound")
    forced_total = 0.0
    for position in positions.values():
        value = float(getattr(position, attribute))
        component_id = position.trajectory.component_id
        contribution[component_id] += value
        day_components[component_id] += value
        forced_total += value
    positions.clear()
    return float(balance + forced_total), float(day_pnl + forced_total)


def _daily_row(
    *,
    day: int,
    balance: float,
    floor: float,
    starting_balance: float,
    required_target: float,
    day_pnl: float,
    best_day: float,
    minimum_buffer: float,
    day_costs: float,
    day_conflicts: int,
    day_max_mini: float,
    day_max_direction: float,
    day_components: Mapping[str, float],
    open_positions: int,
    unrealized_pnl: float,
    consistency_limit: float,
) -> dict[str, Any]:
    realized = float(balance - starting_balance)
    concentration = float(best_day / realized) if realized > 0.0 else 0.0
    consistency_ok = bool(realized <= 0.0 or concentration <= consistency_limit + 1e-12)
    return {
        "session_day": int(day),
        "balance": float(balance),
        "mll_floor": float(floor),
        "mll_buffer": float(balance + unrealized_pnl - floor),
        "minimum_mll_buffer": float(
            min(minimum_buffer, balance + unrealized_pnl - floor)
        ),
        "day_pnl": float(day_pnl),
        "realized_pnl": realized,
        "unrealized_pnl": float(unrealized_pnl),
        "costs": float(day_costs),
        "target_progress": float(
            (realized + unrealized_pnl) / max(required_target, 1.0)
        ),
        "consistency": concentration,
        "consistency_ok": consistency_ok,
        "conflicts": {"count": int(day_conflicts)},
        "exposure": {
            "maximum_mini_equivalent": float(day_max_mini),
            "maximum_net_directional": float(day_max_direction),
        },
        "component_attribution": dict(sorted(day_components.items())),
        "open_positions": int(open_positions),
    }


__all__ = [
    "CAUSAL_ACCOUNT_REPLAY_VERSION",
    "evaluate_causal_account_policy",
    "run_causal_shared_account_episode",
]
