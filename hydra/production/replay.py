from __future__ import annotations

from collections import Counter, defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import replace
from multiprocessing import get_context
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import (
    AccountPolicyRollingSummary,
    RoutedTrade,
    evaluate_account_policy,
    run_shared_account_episode,
)
from hydra.account_policy.schema import BasketPolicy
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.production.mll_accounting import (
    realize_correlated_open_position_mll_breach,
)
from hydra.production.policy_factory import ProductionPolicy
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.mll_variants import advance_end_of_day_floor, advance_intraday_floor
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.propfirm.topstep_150k import Topstep150KConfig


PRODUCTION_REPLAY_VERSION = "hydra_account_policy_v7_2_production_replay_v1"


def evaluate_policy(
    policy: ProductionPolicy,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    horizon: int,
    stress_cost_multiplier: float = 1.5,
    eligible_days_by_start: Mapping[int, Sequence[int]] | None = None,
) -> dict[str, Any]:
    if horizon < 1:
        raise ValueError("horizon must be positive")
    if stress_cost_multiplier < 1.0:
        raise ValueError("cost stress cannot be below normal costs")
    missing = [value for value in policy.sleeve_ids if value not in runtimes]
    if missing:
        raise ValueError(f"production policy has missing runtimes: {missing}")
    selected = {value: runtimes[value] for value in policy.sleeve_ids}
    eligible = _common_days(selected.values())
    if not eligible:
        raise ValueError("production policy has no common eligible sessions")
    transformed, transform_audit = transform_policy_events(
        policy,
        selected,
        eligible_days_by_start=eligible_days_by_start,
    )
    if any(not transformed.get(value) for value in policy.sleeve_ids):
        raise ValueError("production routing removed an entire component")
    basket = BasketPolicy(
        policy_id=policy.policy_id,
        component_ids=policy.sleeve_ids,
        archetype=f"ECONOMIC_PRODUCTION::{policy.mechanism}",
        maximum_simultaneous_positions=policy.maximum_simultaneous_positions,
        maximum_mini_equivalent=policy.maximum_mini_equivalent,
        conflict_policy=policy.conflict_policy,
        component_priority=policy.component_priority,
        policy_version=PRODUCTION_REPLAY_VERSION,
    )
    episode_policy = EpisodeStartPolicy(
        maximum_starts=len(starts),
        minimum_spacing_sessions=1,
        minimum_observation_sessions=horizon,
        maximum_duration_sessions=horizon,
        regime_balanced=False,
    )
    valid_starts = tuple(int(value) for value in starts if value in set(eligible))
    if not valid_starts:
        raise ValueError("production policy has no valid explicit starts")
    normal = (
        evaluate_account_policy(
            transformed,
            eligible,
            basket=basket,
            episode_policy=episode_policy,
            explicit_start_days=valid_starts,
        )
        if eligible_days_by_start is None
        else _evaluate_isolated_blocks(
            transformed,
            basket=basket,
            starts=valid_starts,
            eligible_days_by_start=eligible_days_by_start,
            horizon=horizon,
        )
    )
    stressed = {
        key: tuple(_restress(row, stress_cost_multiplier) for row in values)
        for key, values in transformed.items()
    }
    stress = (
        evaluate_account_policy(
            stressed,
            eligible,
            basket=basket,
            episode_policy=episode_policy,
            explicit_start_days=valid_starts,
        )
        if eligible_days_by_start is None
        else _evaluate_isolated_blocks(
            stressed,
            basket=basket,
            starts=valid_starts,
            eligible_days_by_start=eligible_days_by_start,
            horizon=horizon,
        )
    )
    normal_rows = [
        _episode_row(
            policy, row, scenario="NORMAL", horizon=horizon, events=transformed
        )
        for row in normal.episodes
    ]
    stress_rows = [
        _episode_row(
            policy, row, scenario="STRESSED_1_5X", horizon=horizon, events=stressed
        )
        for row in stress.episodes
    ]
    return {
        "schema": "hydra_production_policy_replay_v1",
        "replay_version": PRODUCTION_REPLAY_VERSION,
        "policy": policy.to_dict(),
        "horizon_trading_days": horizon,
        "episode_start_days": list(valid_starts),
        "normal": normal.to_dict(include_episodes=False),
        "stressed_1_5x": stress.to_dict(include_episodes=False),
        "normal_episodes": normal_rows,
        "stressed_episodes": stress_rows,
        "censoring_audit": {
            "normal": _censoring_summary(normal_rows),
            "stressed_1_5x": _censoring_summary(stress_rows),
        },
        "transform_audit": transform_audit,
        "source_runtime_hashes": {
            key: value.specification_hash for key, value in sorted(selected.items())
        },
        "exact_account_chronology": True,
        "dynamic_loss_streak_ratchet": False,
        "development_only": True,
        "validated": False,
        "broker_connections": 0,
        "orders": 0,
    }


def transform_policy_events(
    policy: ProductionPolicy,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    eligible_days_by_start: Mapping[int, Sequence[int]] | None = None,
) -> tuple[dict[str, tuple[RoutedTrade, ...]], dict[str, Any]]:
    scaled = {
        key: tuple(_scale(row, policy.risk_micro_units) for row in runtime.events)
        for key, runtime in runtimes.items()
    }
    before = {key: len(values) for key, values in scaled.items()}
    route = dict(policy.route_parameters)
    mechanism = policy.mechanism
    if eligible_days_by_start is not None:
        calendars = sorted(
            {
                tuple(sorted({int(day) for day in days}))
                for days in eligible_days_by_start.values()
            }
        )
        merged: dict[str, list[RoutedTrade]] = defaultdict(list)
        block_audit: list[dict[str, Any]] = []
        for calendar in calendars:
            allowed = set(calendar)
            subset = {
                key: tuple(
                    row for row in values if int(row.event.session_day) in allowed
                )
                for key, values in scaled.items()
            }
            routed = _apply_policy_route(mechanism, route, subset)
            for key, values in routed.items():
                merged[key].extend(values)
            block_audit.append(
                {
                    "first_day": calendar[0],
                    "last_day": calendar[-1],
                    "event_count_after": {
                        key: len(values) for key, values in sorted(routed.items())
                    },
                }
            )
        output = {
            key: tuple(
                sorted(values, key=lambda row: (row.event.decision_ns, row.event.event_id))
            )
            for key, values in merged.items()
        }
    else:
        output = _apply_policy_route(mechanism, route, scaled)
        block_audit = []
    after = {key: len(values) for key, values in output.items()}
    return output, {
        "mechanism": mechanism,
        "route_parameters": dict(route),
        "event_count_before": dict(sorted(before.items())),
        "event_count_after": dict(sorted(after.items())),
        "temporal_block_state_resets": block_audit,
        "future_outcomes_read": False,
        "underlying_source_signals_changed": False,
        "static_risk_micro_units": policy.risk_micro_units,
    }


def _apply_policy_route(
    mechanism: str,
    route: Mapping[str, Any],
    scaled: Mapping[str, Sequence[RoutedTrade]],
) -> dict[str, tuple[RoutedTrade, ...]]:
    if mechanism == "REGIME_GATED_SLEEVES":
        output = {
            key: _past_only_trailing_net_gate(
                values,
                lookback=int(route["lookback_closed_trades"]),
                minimum=int(route["minimum_closed_trades"]),
            )
            for key, values in scaled.items()
        }
    elif mechanism == "OPPORTUNITY_DENSITY":
        output = _past_only_opportunity_density_gate(
            scaled,
            lookback_minutes=int(route["lookback_minutes"]),
            minimum_sources=int(route["minimum_independent_sources"]),
        )
    elif mechanism == "MARKET_ROLE_ROTATION":
        output = _past_only_market_rotation(
            scaled,
            lookback_sessions=int(route["lookback_sessions"]),
            active_market_count=int(route["active_market_count"]),
        )
    elif mechanism == "NEW_MICRO_EDGE_ASSEMBLY":
        output = {
            key: _past_only_quality_veto(
                values,
                lookback=int(route["lookback_closed_trades"]),
                minimum=int(route["minimum_closed_trades"]),
                maximum_loss_share=float(route["maximum_trailing_loss_share"]),
            )
            for key, values in scaled.items()
        }
    else:
        output = {key: tuple(values) for key, values in scaled.items()}
    return output


def common_episode_starts(
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    maximum_starts: int,
    horizon: int,
    minimum_spacing_sessions: int,
) -> tuple[int, ...]:
    days = _common_days(runtimes.values())
    return select_episode_starts(
        days,
        policy=EpisodeStartPolicy(
            maximum_starts=maximum_starts,
            minimum_spacing_sessions=minimum_spacing_sessions,
            minimum_observation_sessions=horizon,
            maximum_duration_sessions=horizon,
            regime_balanced=True,
        ),
    )


def evaluate_policy_batch_parallel(
    policies: Sequence[ProductionPolicy],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    horizon: int,
    worker_count: int,
    batch_size: int = 2,
    eligible_days_by_start: Mapping[int, Sequence[int]] | None = None,
) -> Iterable[list[dict[str, Any]]]:
    """Yield completed exact replay batches while three workers remain busy."""

    if worker_count < 1 or batch_size < 1:
        raise ValueError("worker and batch counts must be positive")
    batches = [
        list(policies[index : index + batch_size])
        for index in range(0, len(policies), batch_size)
    ]
    if worker_count == 1:
        for batch in batches:
            output: list[dict[str, Any]] = []
            for policy in batch:
                try:
                    output.append(
                        evaluate_policy(
                            policy,
                            runtimes,
                            starts=starts,
                            horizon=horizon,
                            eligible_days_by_start=eligible_days_by_start,
                        )
                    )
                except (ValueError, RuntimeError) as exc:
                    output.append(
                        {
                            "schema": "hydra_production_policy_replay_error_v1",
                            "policy": policy.to_dict(),
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                            "development_only": True,
                        }
                    )
            yield output
        return
    context = get_context("spawn")
    with ProcessPoolExecutor(
        max_workers=worker_count,
        mp_context=context,
        initializer=_initialize_worker,
        initargs=(
            runtimes,
            tuple(int(value) for value in starts),
            int(horizon),
            eligible_days_by_start,
        ),
    ) as pool:
        indexed = iter(enumerate(batches))
        maximum_in_flight = max(1, 2 * worker_count)
        futures: dict[Any, int] = {}
        for _ in range(min(maximum_in_flight, len(batches))):
            index, batch = next(indexed)
            futures[pool.submit(_fork_batch, [row.to_dict() for row in batch])] = index
        completed: dict[int, list[dict[str, Any]]] = {}
        next_index = 0
        exhausted = len(futures) == len(batches)
        while futures:
            done, _pending = wait(tuple(futures), return_when=FIRST_COMPLETED)
            for future in done:
                completed[futures.pop(future)] = future.result()
            while next_index in completed:
                yield completed.pop(next_index)
                next_index += 1
            while not exhausted and len(futures) + len(completed) < maximum_in_flight:
                try:
                    index, batch = next(indexed)
                except StopIteration:
                    exhausted = True
                    break
                futures[pool.submit(_fork_batch, [row.to_dict() for row in batch])] = index


def rank_exact_replays(
    rows: Sequence[Mapping[str, Any]], *, limit: int
) -> tuple[str, ...]:
    """Transparent Pareto-oriented ordering for successive halving."""

    eligible = [
        row
        for row in rows
        if float(row["normal"]["median_episode_net_pnl"]) > 0.0
        and float(row["stressed_1_5x"]["median_episode_net_pnl"]) > 0.0
        and float(row["normal"]["mll_breach_rate"]) <= 0.10
        and float(row["stressed_1_5x"]["mll_breach_rate"]) <= 0.10
    ]
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in eligible:
        groups[str(row["policy"]["mechanism"])].append(row)
    for values in groups.values():
        values.sort(
            key=lambda row: (
                -int(row["stressed_1_5x"]["pass_count"]),
                -int(row["normal"]["pass_count"]),
                -float(
                    row.get("censoring_audit", {})
                    .get("stressed_1_5x", {})
                    .get(
                        "uncensored_target_progress_median",
                        row["stressed_1_5x"]["target_progress_median"],
                    )
                ),
                -float(row["normal"]["target_progress_p25"]),
                -float(row["stressed_1_5x"]["median_episode_net_pnl"]),
                float(row["stressed_1_5x"]["mll_breach_rate"]),
                float(
                    row.get("censoring_audit", {})
                    .get("stressed_1_5x", {})
                    .get("censored_rate", 0.0)
                ),
                -float(row["stressed_1_5x"]["consistency_pass_rate"]),
                len(row["policy"]["sleeve_ids"]),
                str(row["policy"]["policy_id"]),
            )
        )
    output: list[str] = []
    keys = sorted(groups)
    cursor = 0
    while keys and len(output) < limit:
        key = keys[cursor % len(keys)]
        values = groups[key]
        output.append(str(values.pop(0)["policy"]["policy_id"]))
        if not values:
            keys.remove(key)
            cursor = 0
        else:
            cursor += 1
    return tuple(output)


def failure_vector(row: Mapping[str, Any]) -> str:
    normal = row["normal"]
    stress = row["stressed_1_5x"]
    if float(stress["mll_breach_rate"]) > 0.10:
        return "MLL_BREACH"
    if float(stress["median_episode_net_pnl"]) <= 0.0:
        return "COST_FRAGILITY"
    if float(stress["consistency_pass_rate"]) < 0.75:
        return "CONSISTENCY_FAILURE"
    if float(stress["target_progress_median"]) < 0.35:
        return "TARGET_TOO_SLOW"
    if int(stress["accepted_event_count"]) < 48:
        return "INSUFFICIENT_OPPORTUNITIES"
    contribution = stress.get("component_contribution") or {}
    positive = sum(max(float(value), 0.0) for value in contribution.values())
    share = max((max(float(value), 0.0) for value in contribution.values()), default=0.0) / max(
        positive, 1e-12
    )
    if share > 0.65:
        return "OVER_CONCENTRATION"
    if float(normal["conflict_rate"]) > 0.20:
        return "SIGNAL_CONFLICT"
    if int(normal["pass_count"]) == 0 and float(normal["target_progress_median"]) < 0.70:
        return "TARGET_TOO_SLOW"
    return "NO_INCREMENTAL_VALUE"


def _censoring_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    censored = [
        row
        for row in rows
        if row["terminal_classification"]
        in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"}
    ]
    uncensored = [row for row in rows if row not in censored]
    return {
        "episode_count": len(rows),
        "censored_count": len(censored),
        "censored_rate": len(censored) / max(len(rows), 1),
        "data_censored_count": sum(
            row["terminal_classification"] == "DATA_CENSORED" for row in rows
        ),
        "operational_horizon_not_reached_count": sum(
            row["terminal_classification"] == "OPERATIONAL_HORIZON_NOT_REACHED"
            for row in rows
        ),
        "uncensored_target_progress_median": (
            float(np.median([float(row["target_progress"]) for row in uncensored]))
            if uncensored
            else -1_000_000_000.0
        ),
    }


def _evaluate_isolated_blocks(
    events: Mapping[str, Sequence[RoutedTrade]],
    *,
    basket: BasketPolicy,
    starts: Sequence[int],
    eligible_days_by_start: Mapping[int, Sequence[int]],
    horizon: int,
) -> AccountPolicyRollingSummary:
    episodes = tuple(
        realize_correlated_open_position_mll_breach(
            run_shared_account_episode(
                events,
                tuple(int(day) for day in eligible_days_by_start[int(start)]),
                basket=basket,
                start_day=int(start),
                maximum_duration_days=horizon,
            ),
            events,
        )
        for start in starts
    )
    terminals = Counter(row.terminal.value for row in episodes)
    progress = np.asarray([row.target_progress for row in episodes], dtype=float)
    maximum_progress = np.asarray(
        [row.maximum_target_progress for row in episodes], dtype=float
    )
    net = np.asarray([row.net_pnl for row in episodes], dtype=float)
    passing_days = [float(row.days_to_target) for row in episodes if row.days_to_target]
    positive_velocity = [
        row.net_pnl / max(row.eligible_days, 1) for row in episodes if row.net_pnl > 0
    ]
    median_velocity = float(np.median(positive_velocity)) if positive_velocity else 0.0
    contribution: dict[str, float] = defaultdict(float)
    for episode in episodes:
        for component_id, value in episode.component_contribution.items():
            contribution[component_id] += float(value) / len(episodes)
    accepted = sum(row.accepted_events for row in episodes)
    skipped = sum(row.skipped_events for row in episodes)
    return AccountPolicyRollingSummary(
        policy_id=basket.policy_id,
        policy_kind=basket.kind.value,
        episode_start_days=tuple(int(value) for value in starts),
        episode_start_count=len(episodes),
        effective_block_count=len(
            {tuple(int(day) for day in eligible_days_by_start[int(start)]) for start in starts}
        ),
        pass_count=terminals[CombineTerminal.PASSED.value],
        pass_rate=terminals[CombineTerminal.PASSED.value] / len(episodes),
        mll_breach_count=terminals[CombineTerminal.MLL_BREACH.value],
        mll_breach_rate=terminals[CombineTerminal.MLL_BREACH.value] / len(episodes),
        timeout_rate=terminals[CombineTerminal.TIMEOUT.value] / len(episodes),
        compliance_failure_count=terminals[CombineTerminal.COMPLIANCE_FAILURE.value],
        target_progress_p25=float(np.percentile(progress, 25)),
        target_progress_median=float(np.median(progress)),
        target_progress_p75=float(np.percentile(progress, 75)),
        maximum_target_progress=float(np.max(maximum_progress)),
        median_days_to_target=float(np.median(passing_days)) if passing_days else None,
        days_per_thousand_progress=1000.0 / median_velocity if median_velocity > 0 else None,
        projected_days_to_target=9000.0 / median_velocity if median_velocity > 0 else None,
        minimum_mll_buffer=min(float(row.minimum_mll_buffer) for row in episodes),
        consistency_pass_rate=float(np.mean([row.consistency_ok for row in episodes])),
        median_episode_net_pnl=float(np.median(net)),
        median_best_day_concentration=float(
            np.median([row.best_day_concentration for row in episodes])
        ),
        median_shared_loss_days=float(
            np.median([row.shared_loss_days for row in episodes])
        ),
        conflict_rate=sum(row.conflict_count for row in episodes) / max(accepted + skipped, 1),
        accepted_event_count=accepted,
        skipped_event_count=skipped,
        component_contribution=dict(sorted(contribution.items())),
        terminal_distribution=dict(sorted(terminals.items())),
        episodes=episodes,
    )


def _episode_row(
    policy: ProductionPolicy,
    episode: Any,
    *,
    scenario: str,
    horizon: int,
    events: Mapping[str, Sequence[RoutedTrade]],
) -> dict[str, Any]:
    row = episode.to_dict(include_paths=True)
    row["daily_path"] = _enrich_daily_path(episode, events)
    if episode.terminal is CombineTerminal.PASSED:
        terminal = "TARGET_REACHED"
    elif episode.terminal is CombineTerminal.MLL_BREACH:
        terminal = "MLL_BREACHED"
    elif episode.terminal is CombineTerminal.COMPLIANCE_FAILURE:
        terminal = "HARD_RULE_FAILURE"
    elif int(episode.eligible_days) < horizon:
        terminal = "DATA_CENSORED"
    else:
        terminal = "OPERATIONAL_HORIZON_NOT_REACHED"
    row.update(
        {
            "campaign_id": policy.source_campaign,
            "policy_id": policy.policy_id,
            "scenario": scenario,
            "horizon_trading_days": horizon,
            "terminal_classification": terminal,
            "censored": terminal == "DATA_CENSORED",
            "research_horizon_timeout_is_failure": False,
            "development_only": True,
        }
    )
    return row


def _enrich_daily_path(
    episode: Any,
    events: Mapping[str, Sequence[RoutedTrade]],
) -> list[dict[str, Any]]:
    rules = Topstep150KConfig()
    event_lookup: dict[tuple[str, int], list[RoutedTrade]] = defaultdict(list)
    for component_id, values in events.items():
        for routed in values:
            event_lookup[(component_id, int(routed.event.decision_ns))].append(routed)
    decisions_by_day: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for raw in episode.risk_allocation_path:
        decisions_by_day[int(raw["session_day"])].append(dict(raw))
    running_balance = float(rules.combine_starting_balance)
    running_floor = float(rules.combine_starting_mll)
    required_target = float(rules.combine_profit_target)
    best_day = 0.0
    cumulative_cost = 0.0
    output: list[dict[str, Any]] = []
    for raw_day in episode.daily_path:
        day = int(raw_day["session_day"])
        routing = sorted(
            decisions_by_day.get(day, ()),
            key=lambda row: (int(row["decision_ns"]), str(row["component_id"])),
        )
        accepted: list[tuple[dict[str, Any], RoutedTrade]] = []
        for decision in routing:
            if not bool(decision["allow"]):
                continue
            matches = event_lookup[
                (str(decision["component_id"]), int(decision["decision_ns"]))
            ]
            if len(matches) != 1:
                raise ValueError("routing decision does not resolve to one immutable trade")
            accepted.append((decision, matches[0]))
        actions: list[tuple[int, int, dict[str, Any], RoutedTrade]] = []
        for decision, routed in accepted:
            actions.append((int(routed.event.exit_ns), 0, decision, routed))
            actions.append((int(routed.event.decision_ns), 1, decision, routed))
        actions.sort(
            key=lambda row: (
                row[0],
                row[1],
                episode.policy_id,
                row[3].event.event_id,
            )
        )
        open_positions: dict[str, tuple[RoutedTrade, float, float, float, float]] = {}
        attribution: dict[str, float] = defaultdict(float)
        day_cost = 0.0
        day_minimum_buffer = running_balance - running_floor
        day_maximum_mini = 0.0
        day_maximum_direction = 0.0
        for _timestamp, kind, decision, routed in actions:
            event = routed.event
            if kind == 0:
                position = open_positions.pop(event.event_id, None)
                if position is None:
                    continue
                _source, net, _worst, _best, _mini = position
                running_balance += net
                attribution[routed.component_id] += net
                running_floor = advance_intraday_floor(
                    running_floor,
                    live_equity_high=running_balance,
                    distance=float(rules.combine_max_loss_limit),
                    lock=float(rules.combine_starting_balance),
                    variant=rules.resolved_mll_mode,
                )
                day_minimum_buffer = min(
                    day_minimum_buffer, running_balance - running_floor
                )
                continue
            ratio = int(decision["quantity"]) / int(event.quantity)
            net = float(event.net_pnl * ratio)
            gross = float(event.gross_pnl * ratio)
            worst = float(event.worst_unrealized_pnl * ratio)
            best = float(event.best_unrealized_pnl * ratio)
            mini = float(decision["mini_equivalent"])
            open_positions[event.event_id] = (routed, net, worst, best, mini)
            day_cost += max(0.0, gross - net)
            total_mini = sum(value[4] for value in open_positions.values())
            directional = sum(
                value[4] * value[0].side for value in open_positions.values()
            )
            day_maximum_mini = max(day_maximum_mini, total_mini)
            day_maximum_direction = max(day_maximum_direction, abs(directional))
            running_floor = advance_intraday_floor(
                running_floor,
                live_equity_high=running_balance
                + sum(max(value[3], 0.0) for value in open_positions.values()),
                distance=float(rules.combine_max_loss_limit),
                lock=float(rules.combine_starting_balance),
                variant=rules.resolved_mll_mode,
            )
            intraday_low = running_balance + sum(
                min(value[2], 0.0) for value in open_positions.values()
            )
            day_minimum_buffer = min(
                day_minimum_buffer, intraday_low - running_floor
            )
            if intraday_low <= running_floor:
                break
        authoritative_balance = float(raw_day["balance"])
        day_pnl = float(raw_day["day_pnl"])
        delta = day_pnl - sum(attribution.values())
        if abs(delta) > 1e-9:
            component = (
                next(iter(attribution))
                if attribution
                else (str(routing[-1]["component_id"]) if routing else next(iter(events)))
            )
            attribution[component] += delta
        running_balance = authoritative_balance
        running_floor = float(raw_day["mll_floor"])
        day_minimum_buffer = min(
            day_minimum_buffer, running_balance - running_floor
        )
        cumulative_cost += day_cost
        best_day = max(best_day, day_pnl)
        total_profit = running_balance - float(rules.combine_starting_balance)
        if (
            best_day
            > rules.combine_profit_target
            * rules.consistency_best_day_max_pct_of_profit_target
        ):
            required_target = max(
                required_target,
                best_day / rules.consistency_best_day_max_pct_of_profit_target,
            )
        concentration = best_day / total_profit if total_profit > 0 else 0.0
        consistency = bool(
            total_profit <= 0
            or concentration
            <= rules.consistency_best_day_max_pct_of_profit_target + 1e-12
        )
        output.append(
            {
                **dict(raw_day),
                "realized_pnl": total_profit,
                "unrealized_pnl": 0.0,
                "costs": day_cost,
                "cumulative_costs": cumulative_cost,
                "closing_mll_buffer": running_balance - running_floor,
                "minimum_mll_buffer": day_minimum_buffer,
                "target_progress": total_profit / max(required_target, 1.0),
                "consistency_ok": consistency,
                "component_attribution": dict(sorted(attribution.items())),
                "conflicts": [
                    row
                    for row in routing
                    if "CONFLICT" in str(row["reason"])
                    or row["reason"] == "MAXIMUM_SIMULTANEOUS_POSITIONS"
                ],
                "routing_decisions": routing,
                "exposure": {
                    "maximum_mini_equivalent": day_maximum_mini,
                    "maximum_net_directional": day_maximum_direction,
                },
            }
        )
    if output:
        output[-1]["consistency_ok"] = bool(episode.consistency_ok)
        output[-1]["target_progress"] = float(episode.target_progress)
    if abs(sum(float(row["costs"]) for row in output) - float(episode.total_cost)) > 1e-6:
        raise ValueError("derived daily cost path does not reconcile with frozen episode")
    if output and abs(
        min(float(row["minimum_mll_buffer"]) for row in output)
        - float(episode.minimum_mll_buffer)
    ) > 1e-6:
        raise ValueError("derived daily MLL path does not reconcile with frozen episode")
    return output


def _scale(trade: RoutedTrade, micro_units: int) -> RoutedTrade:
    if micro_units not in {3, 4, 5, 6}:
        raise ValueError("production risk must use the frozen micro-unit frontier")
    event = trade.event
    return replace(
        trade,
        event=replace(
            event,
            event_id=f"{event.event_id}:static_micro_units_{micro_units}",
            net_pnl=float(event.net_pnl * micro_units),
            gross_pnl=float(event.gross_pnl * micro_units),
            worst_unrealized_pnl=float(event.worst_unrealized_pnl * micro_units),
            best_unrealized_pnl=float(event.best_unrealized_pnl * micro_units),
            quantity=int(event.quantity * micro_units),
            mini_equivalent=float(event.mini_equivalent * micro_units),
        ),
    )


def _restress(trade: RoutedTrade, multiplier: float) -> RoutedTrade:
    event = trade.event
    base_cost = max(0.0, float(event.gross_pnl - event.net_pnl))
    extra = (multiplier - 1.0) * base_cost
    if extra <= 0.0:
        return trade
    return replace(
        trade,
        event=replace(
            event,
            event_id=f"{event.event_id}:cost_stress_{multiplier:g}",
            net_pnl=float(event.net_pnl - extra),
            worst_unrealized_pnl=float(event.worst_unrealized_pnl - extra),
            best_unrealized_pnl=float(event.best_unrealized_pnl - extra),
        ),
    )


def _past_only_trailing_net_gate(
    events: Sequence[RoutedTrade], *, lookback: int, minimum: int
) -> tuple[RoutedTrade, ...]:
    ordered = sorted(events, key=lambda row: (row.event.decision_ns, row.event.event_id))
    closed: deque[RoutedTrade] = deque()
    pending: list[RoutedTrade] = []
    accepted: list[RoutedTrade] = []
    for current in ordered:
        newly_closed = [row for row in pending if row.event.exit_ns < current.event.decision_ns]
        pending = [row for row in pending if row.event.exit_ns >= current.event.decision_ns]
        for row in sorted(newly_closed, key=lambda value: (value.event.exit_ns, value.event.event_id)):
            closed.append(row)
            while len(closed) > lookback:
                closed.popleft()
        if len(closed) >= minimum and sum(row.event.net_pnl for row in closed) > 0.0:
            accepted.append(current)
        pending.append(current)
    return tuple(accepted)


def _past_only_quality_veto(
    events: Sequence[RoutedTrade],
    *,
    lookback: int,
    minimum: int,
    maximum_loss_share: float,
) -> tuple[RoutedTrade, ...]:
    """Causal micro-edge assembly veto using only already closed source trades."""

    ordered = sorted(events, key=lambda row: (row.event.decision_ns, row.event.event_id))
    closed: deque[RoutedTrade] = deque()
    pending: list[RoutedTrade] = []
    accepted: list[RoutedTrade] = []
    for current in ordered:
        newly_closed = [row for row in pending if row.event.exit_ns < current.event.decision_ns]
        pending = [row for row in pending if row.event.exit_ns >= current.event.decision_ns]
        for row in sorted(newly_closed, key=lambda value: (value.event.exit_ns, value.event.event_id)):
            closed.append(row)
            while len(closed) > lookback:
                closed.popleft()
        if len(closed) >= minimum:
            trailing_net = sum(row.event.net_pnl for row in closed)
            loss_share = sum(row.event.net_pnl < 0.0 for row in closed) / len(closed)
            if trailing_net > 0.0 and loss_share <= maximum_loss_share:
                accepted.append(current)
        pending.append(current)
    return tuple(accepted)


def _past_only_opportunity_density_gate(
    events: Mapping[str, Sequence[RoutedTrade]],
    *,
    lookback_minutes: int,
    minimum_sources: int,
) -> dict[str, tuple[RoutedTrade, ...]]:
    timeline = sorted(
        (row for values in events.values() for row in values),
        key=lambda row: (row.event.decision_ns, row.component_id, row.event.event_id),
    )
    window: deque[RoutedTrade] = deque()
    retained: dict[str, list[RoutedTrade]] = defaultdict(list)
    lookback_ns = int(lookback_minutes * 60 * 1_000_000_000)
    cursor = 0
    while cursor < len(timeline):
        decision_ns = timeline[cursor].event.decision_ns
        end = cursor + 1
        while end < len(timeline) and timeline[end].event.decision_ns == decision_ns:
            end += 1
        simultaneous = timeline[cursor:end]
        while window and window[0].event.decision_ns < decision_ns - lookback_ns:
            window.popleft()
        sources = {row.component_id for row in window}
        sources.update(row.component_id for row in simultaneous)
        if len(sources) >= minimum_sources:
            for current in simultaneous:
                retained[current.component_id].append(current)
        window.extend(simultaneous)
        cursor = end
    return {key: tuple(retained.get(key, ())) for key in events}


def _past_only_market_rotation(
    events: Mapping[str, Sequence[RoutedTrade]],
    *,
    lookback_sessions: int,
    active_market_count: int,
) -> dict[str, tuple[RoutedTrade, ...]]:
    by_day: dict[int, list[RoutedTrade]] = defaultdict(list)
    for values in events.values():
        for row in values:
            by_day[int(row.event.session_day)].append(row)
    history: deque[tuple[int, str, float, str]] = deque()
    retained: dict[str, list[RoutedTrade]] = defaultdict(list)
    days = sorted(by_day)
    closed_events = sorted(
        (row for values in by_day.values() for row in values),
        key=lambda row: (row.event.exit_ns, row.component_id, row.event.event_id),
    )
    closed_cursor = 0
    for day in days:
        current = sorted(
            by_day[day], key=lambda row: (row.event.decision_ns, row.component_id, row.event.event_id)
        )
        day_open = min(row.event.decision_ns for row in current)
        while (
            closed_cursor < len(closed_events)
            and closed_events[closed_cursor].event.exit_ns < day_open
        ):
            row = closed_events[closed_cursor]
            history.append(
                (
                    int(row.event.session_day),
                    row.market,
                    float(row.event.net_pnl),
                    row.event.event_id,
                )
            )
            closed_cursor += 1
        while history and history[0][0] < day - lookback_sessions * 3:
            history.popleft()
        recent_days = sorted({row[0] for row in history})[-lookback_sessions:]
        score: dict[str, float] = defaultdict(float)
        for prior_day, market, pnl, _event_id in history:
            if prior_day in recent_days:
                score[market] += pnl
        available = sorted({row.market for row in current})
        active = set(
            sorted(available, key=lambda market: (-score.get(market, 0.0), market))[
                :active_market_count
            ]
        )
        for row in current:
            if row.market in active:
                retained[row.component_id].append(row)
    return {key: tuple(retained.get(key, ())) for key in events}


def _common_days(runtimes: Iterable[ExactSleeveRuntime]) -> tuple[int, ...]:
    values = list(runtimes)
    if not values:
        return ()
    common = set(values[0].eligible_session_days)
    for value in values[1:]:
        common.intersection_update(value.eligible_session_days)
    return tuple(sorted(common))


_FORK_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_FORK_STARTS: tuple[int, ...] = ()
_FORK_HORIZON = 60
_FORK_ELIGIBLE_DAYS_BY_START: Mapping[int, Sequence[int]] | None = None


def _initialize_worker(
    runtimes: Mapping[str, ExactSleeveRuntime],
    starts: Sequence[int],
    horizon: int,
    eligible_days_by_start: Mapping[int, Sequence[int]] | None,
) -> None:
    global _FORK_RUNTIMES, _FORK_STARTS, _FORK_HORIZON, _FORK_ELIGIBLE_DAYS_BY_START
    _FORK_RUNTIMES = runtimes
    _FORK_STARTS = tuple(int(value) for value in starts)
    _FORK_HORIZON = int(horizon)
    _FORK_ELIGIBLE_DAYS_BY_START = eligible_days_by_start


def _fork_batch(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        policy = ProductionPolicy.from_dict(row)
        try:
            output.append(
                evaluate_policy(
                    policy,
                    _FORK_RUNTIMES,
                    starts=_FORK_STARTS,
                    horizon=_FORK_HORIZON,
                    eligible_days_by_start=_FORK_ELIGIBLE_DAYS_BY_START,
                )
            )
        except (ValueError, RuntimeError) as exc:
            output.append(
                {
                    "schema": "hydra_production_policy_replay_error_v1",
                    "policy": policy.to_dict(),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "development_only": True,
                }
            )
    return output


__all__ = [
    "PRODUCTION_REPLAY_VERSION",
    "common_episode_starts",
    "evaluate_policy",
    "evaluate_policy_batch_parallel",
    "failure_vector",
    "rank_exact_replays",
    "transform_policy_events",
]
