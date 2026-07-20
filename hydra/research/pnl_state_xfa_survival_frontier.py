"""Bounded post-payout frontier for the sealed PnL-state XFA diagnostic.

The input is the immutable 71-transition/142-alternative diagnostic.  Market
features, sleeve signals and Combine paths are never replayed.  Only the
already-canonical causal XFA trajectory events are read, hash-reconciled and
replayed under a small pre-registered post-payout policy frontier.

Standard and Consistency remain alternative products.  Their values are never
added and this development diagnostic cannot promote an evidence tier.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.account_size_xfa import (
    AccountSizeXfaRules,
    FrozenAccountSizeXfaHandoff,
    _force_liquidate,
    _position_with_limit,
    _priority,
    _validate_trajectories,
    freeze_account_size_xfa_handoff,
    load_account_size_xfa_rules,
)
from hydra.propfirm.combine_to_xfa import XfaTerminal
from hydra.propfirm.mll_variants import advance_end_of_day_floor
from hydra.propfirm.xfa_post_payout import (
    DllScenario,
    FrontierRole,
    PayoutAmountMode,
    RecoveryCondition,
    RequestTiming,
    XfaPostPayoutPolicy,
    preregistered_post_payout_frontier,
)
from hydra.research.causal_sleeve_replay import CausalTradeTrajectory
from hydra.research.pnl_state_xfa_diagnostic import (
    SCHEMA as SOURCE_SCHEMA,
    _continuation_days,
    _continuation_trajectories,
    _load_context,
)
from hydra.research.pnl_state_risk_frontier import _inside, _read_json


SCHEMA = "hydra_pnl_state_xfa_survival_frontier_v2"
SOURCE_RESULT_HASH = (
    "6a1f474355b18797e1b9efecff34692f3b6d2edeac31ac9207694c6f651ac85c"
)
DEFAULT_SOURCE = Path(
    "reports/economic_evolution/pnl_state_xfa_diagnostic_v1/"
    "xfa_all_clean_handoffs.json"
)
PROFILE_COUNT_PER_CELL = 12
SURVIVAL_CHECKPOINTS = (30, 60, 90)


class PnLStateXfaSurvivalFrontierError(RuntimeError):
    """The bounded diagnostic cannot reconcile its immutable inputs."""


@dataclass(slots=True)
class _FrontierPosition:
    trajectory: CausalTradeTrajectory
    quantity: int
    mini_equivalent: float
    ratio: float
    current_unrealized: float
    current_worst: float


def build_pnl_state_xfa_survival_frontier(
    root: str | Path,
    *,
    source_path: str | Path = DEFAULT_SOURCE,
) -> dict[str, Any]:
    """Evaluate the frozen survival frontier without touching Combine evidence."""

    project = Path(root).resolve()
    source = _read_json(_inside(project, source_path))
    _verify_source(source)
    eligible = _eligible_cells(source)
    if len(eligible) != 7:
        raise PnLStateXfaSurvivalFrontierError(
            f"eligible policy/horizon/path cell count drift: {len(eligible)} != 7"
        )
    policy_ids = {str(row["policy_id"]) for row in eligible}
    context = _load_context(project, policy_ids)
    transition_by_id = {
        str(row["transition_id"]): dict(row) for row in source["transitions"]
    }
    engine_by_id = {
        str(row["transition_id"]): dict(row)
        for row in source["alternative_results"]
    }
    if set(transition_by_id) != set(engine_by_id):
        raise PnLStateXfaSurvivalFrontierError("transition/engine identity drift")

    source_receipts: dict[tuple[str, str], dict[str, Any]] = {}
    evaluation_rows: list[dict[str, Any]] = []
    baseline_mismatches: list[dict[str, Any]] = []
    canonical_payout_events: list[dict[str, Any]] = []
    payout_event_fingerprints: set[str] = set()

    for cell in eligible:
        policy_id = str(cell["policy_id"])
        horizon = int(cell["horizon_trading_days"])
        path = str(cell["path"])
        path_name = f"XFA_{path}"
        prepared = context["prepared"][policy_id]
        transitions = [
            row
            for row in source["transitions"]
            if str(row["policy_id"]) == policy_id
            and int(row["horizon_trading_days"]) == horizon
        ]
        if len(transitions) != int(cell["combine_pass_count"]):
            raise PnLStateXfaSurvivalFrontierError(
                "eligible cell Combine transition denominator drift"
            )
        policies = tuple(
            row
            for row in preregistered_post_payout_frontier(
                policy_id, path=path_name
            )
            if row.dll_scenario is DllScenario.NO_DLL
        )
        if len(policies) != PROFILE_COUNT_PER_CELL:
            raise PnLStateXfaSurvivalFrontierError("frontier profile count drift")

        for transition in transitions:
            transition_id = str(transition["transition_id"])
            source_engine = engine_by_id[transition_id]
            rules = load_account_size_xfa_rules(
                prepared.account_label,
                snapshot_path=(
                    project / "config/rulesets/topstep_official_2026-07-19.json"
                ),
            )
            handoff = freeze_account_size_xfa_handoff(
                candidate_id=policy_id,
                combine_book_hash=str(transition["combine_book_hash"]),
                component_priority=prepared.baseline_policy.component_ids,
                rules=rules,
                risk_multiplier=1.0,
                maximum_simultaneous_positions=(
                    prepared.baseline_policy.maximum_concurrent_sleeves
                ),
                maximum_mini_equivalent=min(
                    float(prepared.baseline_policy.maximum_mini_equivalent),
                    float(rules.combine_maximum_mini_equivalent),
                ),
                same_market_exclusive=True,
                profile_id=(
                    f"{policy_id}:pnl-state-xfa-static-1x:"
                    f"{prepared.account_label}"
                ),
            )
            if handoff.handoff_hash != str(
                source_engine["handoff"]["handoff_hash"]
            ):
                raise PnLStateXfaSurvivalFrontierError("frozen XFA handoff drift")
            xfa_days = _continuation_days(
                context["calendar"],
                after_day=int(transition["combine_end_day"]),
                unavailable=prepared.unavailable_days,
            )
            if len(xfa_days) != int(transition["available_xfa_days"]):
                raise PnLStateXfaSurvivalFrontierError(
                    "available XFA chronology drift"
                )
            trajectories = _continuation_trajectories(
                prepared.trajectories["NORMAL"],
                start_day=xfa_days[0],
                end_day=xfa_days[-1],
            )
            source_event_hash = stable_hash(
                {
                    "eligible_session_days": list(xfa_days),
                    "component_trajectories": {
                        key: [row.to_dict() for row in trajectories[key]]
                        for key in sorted(trajectories)
                    },
                }
            )
            if source_event_hash != str(source_engine["source_trajectory_hash"]):
                raise PnLStateXfaSurvivalFrontierError(
                    "canonical XFA trajectory hash drift"
                )
            source_receipts.setdefault(
                (transition_id, path),
                {
                    "transition_id": transition_id,
                    "policy_id": policy_id,
                    "horizon_trading_days": horizon,
                    "path_alternative_evaluated": path,
                    "source_trajectory_hash": source_event_hash,
                    "source_engine_result_hash": source_engine["result_hash"],
                    "component_count": len(trajectories),
                    "event_count": sum(len(rows) for rows in trajectories.values()),
                    "eligible_session_day_count": len(xfa_days),
                    "market_or_signal_replay_performed": False,
                    "combine_replay_performed": False,
                },
            )
            for policy in policies:
                result = _run_frontier_path(
                    trajectories,
                    xfa_days,
                    handoff=handoff,
                    rules=rules,
                    transition_id=transition_id,
                    policy=policy,
                    start_day=int(transition["xfa_start_day"]),
                    horizon=int(source["requested_xfa_horizon_days"]),
                )
                evaluation_rows.append(
                    {
                        "policy_id": policy_id,
                        "horizon_trading_days": horizon,
                        "path": path,
                        "combine_attempt_count": int(cell["combine_attempt_count"]),
                        "transition_id": transition_id,
                        "frontier_policy": policy.to_dict(),
                        "result": _compact_result(result),
                    }
                )
                for event in result["payout_events"]:
                    fingerprint = str(event["event_fingerprint"])
                    if fingerprint in payout_event_fingerprints:
                        raise PnLStateXfaSurvivalFrontierError(
                            "duplicate canonical frontier payout event"
                        )
                    payout_event_fingerprints.add(fingerprint)
                    canonical_payout_events.append(event)
                if _is_official_baseline(policy):
                    differences = _baseline_differences(
                        result, source_engine["alternatives"][path]
                    )
                    if differences:
                        baseline_mismatches.append(
                            {
                                "transition_id": transition_id,
                                "path": path,
                                "differences": differences,
                            }
                        )

    if baseline_mismatches:
        raise PnLStateXfaSurvivalFrontierError(
            "official XFA baseline did not reconcile exactly"
        )
    aggregates = _aggregate_evaluations(evaluation_rows)
    _assign_pareto(aggregates)
    selected = _select_profiles(aggregates)
    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_BOUNDED_XFA_POST_PAYOUT_DEVELOPMENT_DIAGNOSTIC",
        "source_result_hash": source["result_hash"],
        "source_transition_count": int(
            source["counts"]["clean_normal_combine_transition_count"]
        ),
        "source_alternative_path_count": int(
            source["counts"]["alternative_path_count"]
        ),
        "scoped_unique_transition_count": len(
            {str(row["transition_id"]) for row in evaluation_rows}
        ),
        "scoped_path_transition_count": len(evaluation_rows)
        // PROFILE_COUNT_PER_CELL,
        "eligible_cell_count": len(eligible),
        "eligible_cells": eligible,
        "source_event_receipts": [
            source_receipts[key] for key in sorted(source_receipts)
        ],
        "frontier_contract": {
            "roles": [role.value for role in FrontierRole],
            "post_payout_risk_scales": [0.25, 0.5, 0.75, 1.0],
            "profiles_per_cell": PROFILE_COUNT_PER_CELL,
            "dll_scenario": DllScenario.NO_DLL.value,
            "standard_and_consistency_are_alternatives": True,
            "sum_standard_and_consistency_ev_allowed": False,
            "data_censoring_preserved": True,
        },
        "evaluation_count": len(evaluation_rows),
        "evaluation_count_by_path": {
            path: sum(str(row["path"]) == path for row in evaluation_rows)
            for path in ("STANDARD", "CONSISTENCY")
        },
        "evaluations": evaluation_rows,
        "aggregates": aggregates,
        "pareto_selected_profiles": selected,
        "canonical_payout_events": canonical_payout_events,
        "canonical_payout_event_count": len(canonical_payout_events),
        "canonical_payout_event_unique_fingerprint_count": len(
            payout_event_fingerprints
        ),
        "baseline_reconciliation": {
            "status": "EXACT",
            "comparison_count": len(source_receipts),
            "mismatch_count": 0,
        },
        "invariants": {
            "market_signals_replayed": False,
            "combine_paths_replayed": False,
            "standard_consistency_values_added": False,
            "source_data_censoring_preserved": True,
            "promotion_or_confirmation_claimed": False,
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "database_writes": 0,
            "registry_writes": 0,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_DIAGNOSTIC_ONLY",
        "promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


def _eligible_cells(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = [
        dict(row)
        for row in source["aggregates"]
        if int(row["first_payout_count"]) > 0
        and float(row["minimum_mll_buffer_usd"]) >= 0.0
    ]
    values.sort(
        key=lambda row: (
            str(row["policy_id"]),
            int(row["horizon_trading_days"]),
            str(row["path"]),
        )
    )
    return values


def _is_official_baseline(policy: XfaPostPayoutPolicy) -> bool:
    return bool(
        policy.role is FrontierRole.HARVEST
        and policy.post_payout_risk_scale == 1.0
        and policy.dll_scenario is DllScenario.NO_DLL
    )


def _run_frontier_path(
    component_trajectories: Mapping[str, Sequence[CausalTradeTrajectory]],
    eligible_days: Sequence[int],
    *,
    handoff: FrozenAccountSizeXfaHandoff,
    rules: AccountSizeXfaRules,
    transition_id: str,
    policy: XfaPostPayoutPolicy,
    start_day: int,
    horizon: int,
) -> dict[str, Any]:
    """Exact event replay with a frozen account overlay after each payout."""

    path = policy.path.removeprefix("XFA_")
    if path not in {"STANDARD", "CONSISTENCY"}:
        raise PnLStateXfaSurvivalFrontierError("unsupported XFA path")
    if policy.book_id != handoff.book.candidate_id:
        raise PnLStateXfaSurvivalFrontierError("frontier policy/book drift")
    days = tuple(sorted({int(day) for day in eligible_days}))
    if start_day not in days or horizon < 1:
        raise PnLStateXfaSurvivalFrontierError("invalid XFA chronology")
    components = handoff.book.component_priority
    if set(component_trajectories) != set(components):
        raise PnLStateXfaSurvivalFrontierError("XFA membership drift")
    _validate_trajectories(component_trajectories, components, start_day=start_day)
    start_index = days.index(start_day)
    episode_days = days[start_index : start_index + horizon]
    last_day = int(episode_days[-1])
    trajectories = sorted(
        (
            row
            for component_id, values in component_trajectories.items()
            if component_id in set(components)
            for row in values
            if start_day <= int(row.event.session_day) <= last_day
        ),
        key=lambda row: (
            int(row.event.decision_ns),
            _priority(components, row.component_id),
            row.event.event_id,
        ),
    )
    by_day: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
    for row in trajectories:
        by_day[int(row.event.session_day)].append(row)

    balance = float(rules.xfa_starting_balance)
    floor = float(rules.xfa_starting_floor)
    minimum_buffer = balance - floor
    cycle_winning_days = 0
    cycle_traded_days = 0
    cycle_profit = 0.0
    cycle_best_day = 0.0
    cycle_start_balance = balance
    payout_cycles = 0
    first_payout_day: int | None = None
    last_payout_elapsed: int | None = None
    recovery_balance: float | None = None
    reduced_risk_active = False
    gross_payout = trader_net_payout = 0.0
    traded_days = accepted = skipped = 0
    maximum_size = 0.0
    contribution: dict[str, float] = defaultdict(float)
    ledger: list[dict[str, Any]] = []
    payout_events: list[dict[str, Any]] = []
    terminal: XfaTerminal | None = None
    terminal_reason = ""

    for elapsed, day in enumerate(episode_days, start=1):
        if (
            reduced_risk_active
            and policy.recovery_condition
            is RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE
            and recovery_balance is not None
            and balance >= recovery_balance - 1e-12
        ):
            reduced_risk_active = False
        session_scale = (
            float(policy.post_payout_risk_scale) if reduced_risk_active else 1.0
        )
        opening_balance = balance
        opening_floor = floor
        session_limit = min(
            float(handoff.profile.maximum_mini_equivalent),
            float(rules.session_limit(opening_balance)),
        )
        open_positions: dict[str, Any] = {}
        day_pnl = 0.0
        day_accepted = day_skipped = 0
        day_worst_equity = balance
        action_times: set[int] = set()
        marks: dict[int, list[tuple[CausalTradeTrajectory, Any]]] = defaultdict(list)
        entries: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
        exits: dict[int, list[CausalTradeTrajectory]] = defaultdict(list)
        for trajectory in by_day.get(int(day), ()):
            entries[int(trajectory.event.decision_ns)].append(trajectory)
            exits[int(trajectory.event.exit_ns)].append(trajectory)
            action_times.add(int(trajectory.event.decision_ns))
            action_times.add(int(trajectory.event.exit_ns))
            for mark in trajectory.marks:
                marks[int(mark.availability_time_ns)].append((trajectory, mark))
                action_times.add(int(mark.availability_time_ns))

        for timestamp in sorted(action_times):
            for trajectory, mark in marks.get(timestamp, ()):
                position = open_positions.get(trajectory.event.event_id)
                if position is None:
                    continue
                current = (
                    mark.current_unrealized_pnl
                    if mark.current_unrealized_pnl is not None
                    else mark.worst_unrealized_pnl
                )
                position.current_unrealized = float(current * position.ratio)
                position.current_worst = float(
                    mark.worst_unrealized_pnl * position.ratio
                )
            if marks.get(timestamp) and open_positions:
                conservative_low = balance + sum(
                    min(position.current_worst, 0.0)
                    for position in open_positions.values()
                )
                day_worst_equity = min(day_worst_equity, conservative_low)
                minimum_buffer = min(minimum_buffer, conservative_low - floor)
                if conservative_low <= floor:
                    forced = _force_liquidate(open_positions, contribution)
                    balance += forced
                    day_pnl += forced
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "causal_current_bar_xfa_mll_touch_or_breach"
                    break

            for trajectory in sorted(
                exits.get(timestamp, ()),
                key=lambda row: (
                    _priority(components, row.component_id),
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
                day_worst_equity = min(day_worst_equity, balance)
                minimum_buffer = min(minimum_buffer, balance - floor)
                if balance <= floor:
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "causal_realized_xfa_mll_touch_or_breach"
                    break
            if terminal is not None:
                break

            for trajectory in sorted(
                entries.get(timestamp, ()),
                key=lambda row: (
                    _priority(components, row.component_id),
                    row.event.event_id,
                ),
            ):
                event = trajectory.event
                if not event.session_compliant or not event.contract_limit_compliant:
                    terminal = XfaTerminal.HARD_RULE_FAILURE
                    terminal_reason = (
                        "session_policy_violation"
                        if not event.session_compliant
                        else "source_contract_limit_violation"
                    )
                    break
                if len(open_positions) >= handoff.book.maximum_simultaneous_positions:
                    skipped += 1
                    day_skipped += 1
                    continue
                if handoff.book.same_market_exclusive and any(
                    value.trajectory.market == trajectory.market
                    for value in open_positions.values()
                ):
                    skipped += 1
                    day_skipped += 1
                    continue
                used = sum(
                    value.mini_equivalent for value in open_positions.values()
                )
                market_limit = min(
                    session_limit,
                    float(rules.session_limit(opening_balance, trajectory.market)),
                )
                available = max(
                    0.0, min(session_limit - used, market_limit - used)
                )
                position = _position_with_limit(
                    trajectory,
                    available=available,
                    risk_multiplier=(
                        float(handoff.profile.risk_multiplier) * session_scale
                    ),
                )
                if position is None:
                    skipped += 1
                    day_skipped += 1
                    continue
                open_positions[event.event_id] = position
                accepted += 1
                day_accepted += 1
                maximum_size = max(
                    maximum_size,
                    sum(
                        value.mini_equivalent for value in open_positions.values()
                    ),
                )
                live_equity = balance + sum(
                    value.current_unrealized for value in open_positions.values()
                )
                day_worst_equity = min(day_worst_equity, live_equity)
                minimum_buffer = min(minimum_buffer, live_equity - floor)
                if live_equity <= floor:
                    forced = _force_liquidate(open_positions, contribution)
                    balance += forced
                    day_pnl += forced
                    terminal = XfaTerminal.MLL_BREACHED
                    terminal_reason = "causal_entry_cost_xfa_mll_touch_or_breach"
                    break
            if terminal is not None:
                break

        if terminal is None and open_positions:
            terminal = XfaTerminal.HARD_RULE_FAILURE
            terminal_reason = "open_position_remaining_after_session_close"
        traded = day_accepted > 0
        if traded:
            traded_days += 1
            cycle_traded_days += 1
        if terminal is not None:
            ledger.append(
                {
                    "session_day": int(day),
                    "opening_balance": opening_balance,
                    "closing_balance": balance,
                    "mll_floor_open": opening_floor,
                    "mll_floor_close": floor,
                    "day_pnl": day_pnl,
                    "worst_intraday_equity": day_worst_equity,
                    "traded": traded,
                    "accepted_events": day_accepted,
                    "skipped_events": day_skipped,
                    "session_risk_scale": session_scale,
                    "terminal": terminal.value,
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
            cycle_winning_days += 1
        cycle_profit += day_pnl
        cycle_best_day = max(cycle_best_day, day_pnl)
        consistency_ratio = (
            cycle_best_day / cycle_profit
            if cycle_profit > 0.0 and cycle_best_day > 0.0
            else math.inf
        )
        if path == "STANDARD":
            eligible = cycle_winning_days >= rules.xfa_standard_winning_days and (
                payout_cycles == 0
                or balance - cycle_start_balance
                >= rules.later_standard_cycle_minimum_profit - 1e-12
            )
            payout_cap = float(rules.standard_payout_cap)
        else:
            eligible = bool(
                cycle_traded_days >= rules.xfa_consistency_traded_days
                and cycle_profit > 0.0
                and consistency_ratio <= rules.xfa_consistency_limit + 1e-12
            )
            payout_cap = float(rules.consistency_payout_cap)

        request = _payout_request(
            balance=balance,
            floor=floor,
            eligible=eligible,
            official_cap=payout_cap,
            minimum=float(rules.minimum_payout),
            payout_fraction=float(rules.payout_fraction),
            policy=policy,
        )
        payout_gross = float(request["gross_payout"])
        payout_net = payout_gross * float(rules.trader_profit_split)
        pre_payout_balance = balance
        if payout_gross > 0.0:
            payout_cycles += 1
            if first_payout_day is None:
                first_payout_day = elapsed
            gross_payout += payout_gross
            trader_net_payout += payout_net
            balance -= payout_gross
            floor = float(rules.mll_floor_after_first_payout)
            minimum_buffer = min(minimum_buffer, balance - floor)
            last_payout_elapsed = elapsed
            reduced_risk_active = policy.post_payout_risk_scale < 1.0
            recovery_balance = pre_payout_balance
            cycle_winning_days = 0
            cycle_traded_days = 0
            cycle_profit = 0.0
            cycle_best_day = 0.0
            cycle_start_balance = balance
            event = {
                "schema": "hydra_pnl_state_xfa_survival_payout_event_v1",
                "transition_id": transition_id,
                "policy_id": policy.policy_id,
                "source_policy_id": handoff.book.candidate_id,
                "xfa_path": path,
                "payout_cycle": payout_cycles,
                "eligibility_session_day": int(day),
                "eligible_account_balance": pre_payout_balance,
                "gross_payout_request": float(request["target_gross"]),
                "payout_balance_fraction_limit": float(
                    request["balance_fraction_limit"]
                ),
                "account_size_payout_cap": payout_cap,
                "retained_buffer_usd": float(policy.retained_buffer_usd),
                "gross_payout": payout_gross,
                "payout_split": float(rules.trader_profit_split),
                "trader_net_payout": payout_net,
                "pre_payout_balance": pre_payout_balance,
                "post_payout_balance": balance,
                "mll_before_payout": float(request["mll_before_payout"]),
                "mll_after_payout": floor,
                "post_payout_risk_scale": float(policy.post_payout_risk_scale),
                "reset_marker": True,
            }
            event["event_fingerprint"] = stable_hash(event)
            payout_events.append(event)

        ledger.append(
            {
                "session_day": int(day),
                "opening_balance": opening_balance,
                "closing_balance": balance,
                "mll_floor_open": opening_floor,
                "mll_floor_close": floor,
                "day_pnl": day_pnl,
                "worst_intraday_equity": day_worst_equity,
                "traded": traded,
                "accepted_events": day_accepted,
                "skipped_events": day_skipped,
                "session_risk_scale": session_scale,
                "payout_eligible": eligible,
                "gross_payout": payout_gross,
                "trader_net_payout": payout_net,
                "pre_payout_balance": pre_payout_balance,
                "post_payout_balance": balance,
                "payout_reset_marker": payout_gross > 0.0,
                "terminal": None,
            }
        )

    if terminal is None:
        if len(episode_days) < horizon:
            terminal = XfaTerminal.DATA_CENSORED
            terminal_reason = "available_chronology_ended_before_frozen_xfa_horizon"
        else:
            terminal = XfaTerminal.SURVIVED_HORIZON
            terminal_reason = "frozen_xfa_horizon_survived"
    post_days = (
        0
        if last_payout_elapsed is None
        else max(0, len(ledger) - last_payout_elapsed)
    )
    core = {
        "transition_id": transition_id,
        "frontier_policy_id": policy.policy_id,
        "path": path,
        "terminal": terminal.value,
        "terminal_reason": terminal_reason,
        "start_day": start_day,
        "end_day": int(ledger[-1]["session_day"]),
        "requested_horizon_days": horizon,
        "observed_days": len(ledger),
        "traded_days": traded_days,
        "accepted_event_count": accepted,
        "skipped_event_count": skipped,
        "payout_cycles": payout_cycles,
        "first_payout_day": first_payout_day,
        "first_payout_count": int(first_payout_day is not None),
        "gross_payout": gross_payout,
        "trader_net_payout": trader_net_payout,
        "ending_balance": balance,
        "ending_mll_floor": floor,
        "minimum_mll_buffer": minimum_buffer,
        "maximum_mini_equivalent": maximum_size,
        "post_payout_observed_days": post_days,
        "post_payout_survived": bool(
            payout_cycles > 0 and terminal is XfaTerminal.SURVIVED_HORIZON
        ),
        "post_payout_censored": bool(
            payout_cycles > 0 and terminal is XfaTerminal.DATA_CENSORED
        ),
        "payout_events": payout_events,
        "component_contribution": dict(sorted(contribution.items())),
        "daily_ledger": ledger,
    }
    return {**core, "result_hash": stable_hash(core)}


def _payout_request(
    *,
    balance: float,
    floor: float,
    eligible: bool,
    official_cap: float,
    minimum: float,
    payout_fraction: float,
    policy: XfaPostPayoutPolicy,
) -> dict[str, float]:
    fraction_limit = (
        max(0.0, balance) * payout_fraction if eligible else 0.0
    )
    allowed = min(fraction_limit, official_cap) if eligible else 0.0
    if policy.payout_amount_mode is PayoutAmountMode.OFFICIAL_MAX:
        desired = allowed
    elif policy.payout_amount_mode is PayoutAmountMode.HALF_ALLOWED:
        desired = 0.5 * allowed
    else:
        desired = min(minimum, allowed)
    buffer_limit = max(0.0, balance - float(policy.retained_buffer_usd))
    target = min(desired, buffer_limit)
    if (
        policy.request_timing is RequestTiming.FULL_TARGET_BUFFER_SAFE
        and target + 1e-12 < desired
    ):
        target = 0.0
    gross = target if target >= minimum - 1e-12 else 0.0
    return {
        "balance_fraction_limit": fraction_limit,
        "target_gross": desired,
        "gross_payout": gross,
        "mll_before_payout": floor,
    }


def _compact_result(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value[key]
        for key in (
            "transition_id",
            "frontier_policy_id",
            "path",
            "terminal",
            "terminal_reason",
            "start_day",
            "end_day",
            "requested_horizon_days",
            "observed_days",
            "traded_days",
            "accepted_event_count",
            "skipped_event_count",
            "payout_cycles",
            "first_payout_day",
            "first_payout_count",
            "gross_payout",
            "trader_net_payout",
            "ending_balance",
            "ending_mll_floor",
            "minimum_mll_buffer",
            "maximum_mini_equivalent",
            "post_payout_observed_days",
            "post_payout_survived",
            "post_payout_censored",
            "component_contribution",
            "result_hash",
        )
    }


def _baseline_differences(
    actual: Mapping[str, Any], expected: Mapping[str, Any]
) -> dict[str, Any]:
    fields = (
        "terminal",
        "start_day",
        "end_day",
        "requested_horizon_days",
        "observed_days",
        "traded_days",
        "accepted_event_count",
        "skipped_event_count",
        "payout_cycles",
        "first_payout_day",
        "gross_payout",
        "trader_net_payout",
        "ending_balance",
        "ending_mll_floor",
        "minimum_mll_buffer",
        "post_payout_survived",
        "maximum_mini_equivalent",
    )
    differences = {}
    for field in fields:
        left = actual.get(field)
        right = expected.get(field)
        if isinstance(left, float) or isinstance(right, float):
            equal = math.isclose(
                float(left), float(right), rel_tol=1e-12, abs_tol=1e-8
            )
        else:
            equal = left == right
        if not equal:
            differences[field] = {"source": right, "frontier": left}
    return differences


def _aggregate_evaluations(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        policy = row["frontier_policy"]
        grouped[
            (
                str(row["policy_id"]),
                int(row["horizon_trading_days"]),
                str(row["path"]),
                str(policy["policy_id"]),
            )
        ].append(row)
    output = []
    for (policy_id, horizon, path, frontier_id), values in sorted(grouped.items()):
        results = [row["result"] for row in values]
        attempts = int(values[0]["combine_attempt_count"])
        first_days = [
            int(row["first_payout_day"])
            for row in results
            if row["first_payout_day"] is not None
        ]
        first_count = len(first_days)
        cycles = sum(int(row["payout_cycles"]) for row in results)
        total_net = sum(float(row["trader_net_payout"]) for row in results)
        survival = _survival_summary(results)
        profile = dict(values[0]["frontier_policy"])
        core = {
            "policy_id": policy_id,
            "horizon_trading_days": horizon,
            "path": path,
            "frontier_policy_id": frontier_id,
            "frontier_role": profile["role"],
            "post_payout_risk_scale": profile["post_payout_risk_scale"],
            "request_timing": profile["request_timing"],
            "payout_amount_mode": profile["payout_amount_mode"],
            "retained_buffer_usd": profile["retained_buffer_usd"],
            "combine_attempt_count": attempts,
            "successful_combine_transition_count": len(results),
            "first_payout_count": first_count,
            "first_payout_rate_per_successful_combine": (
                first_count / len(results) if results else 0.0
            ),
            "first_payout_rate_per_new_combine_attempt": (
                first_count / attempts if attempts else 0.0
            ),
            "median_days_to_first_payout": (
                statistics.median(first_days) if first_days else None
            ),
            "payout_cycles_total": cycles,
            "payout_cycles_per_successful_combine": (
                cycles / len(results) if results else 0.0
            ),
            "at_least_two_payout_count": sum(
                int(row["payout_cycles"]) >= 2 for row in results
            ),
            "probability_at_least_two_payouts_per_successful_combine": (
                sum(int(row["payout_cycles"]) >= 2 for row in results) / len(results)
                if results
                else 0.0
            ),
            "trader_net_payout_total_usd": total_net,
            "conditional_trader_net_payout_per_successful_combine_usd": (
                total_net / len(results) if results else 0.0
            ),
            "expected_trader_net_payout_per_new_combine_attempt_usd": (
                total_net / attempts if attempts else 0.0
            ),
            "mll_breach_count": sum(
                str(row["terminal"]) == XfaTerminal.MLL_BREACHED.value
                for row in results
            ),
            "mll_breach_rate_per_successful_combine": (
                sum(
                    str(row["terminal"]) == XfaTerminal.MLL_BREACHED.value
                    for row in results
                )
                / len(results)
                if results
                else 0.0
            ),
            "minimum_mll_buffer_usd": min(
                float(row["minimum_mll_buffer"]) for row in results
            ),
            "closure_before_first_payout_count": sum(
                row["first_payout_day"] is None
                and str(row["terminal"])
                in {
                    XfaTerminal.MLL_BREACHED.value,
                    XfaTerminal.HARD_RULE_FAILURE.value,
                    XfaTerminal.INACTIVITY_RISK.value,
                }
                for row in results
            ),
            "terminal_distribution": dict(
                sorted(Counter(str(row["terminal"]) for row in results).items())
            ),
            "post_payout_survival": survival,
            "alternative_value_not_additive": True,
            "evidence_role": "VIEWED_DEVELOPMENT_DIAGNOSTIC_ONLY",
        }
        output.append({**core, "aggregate_hash": stable_hash(core)})
    return output


def _survival_summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    first = [row for row in results if row["first_payout_day"] is not None]
    output: dict[str, Any] = {
        "first_payout_path_count": len(first),
        "checkpoint_unit": "TRADING_DAYS_AFTER_FIRST_PAYOUT",
        "checkpoints": {},
    }
    for checkpoint in SURVIVAL_CHECKPOINTS:
        survived = failed = censored = 0
        for row in first:
            post_days = int(row["post_payout_observed_days"])
            terminal = str(row["terminal"])
            if terminal in {
                XfaTerminal.MLL_BREACHED.value,
                XfaTerminal.HARD_RULE_FAILURE.value,
                XfaTerminal.INACTIVITY_RISK.value,
            } and post_days < checkpoint:
                failed += 1
            elif post_days >= checkpoint:
                survived += 1
            else:
                censored += 1
        evaluable = survived + failed
        output["checkpoints"][str(checkpoint)] = {
            "survived_count": survived,
            "failed_before_checkpoint_count": failed,
            "data_censored_before_checkpoint_count": censored,
            "evaluable_count": evaluable,
            "survival_rate_among_evaluable": (
                survived / evaluable if evaluable else None
            ),
            "demonstrated_survival_rate_all_first_payout_paths": (
                survived / len(first) if first else None
            ),
        }
    return output


def _pareto_vector(row: Mapping[str, Any]) -> tuple[float, ...]:
    checkpoints = row["post_payout_survival"]["checkpoints"]
    return (
        float(row["expected_trader_net_payout_per_new_combine_attempt_usd"]),
        float(row["first_payout_rate_per_successful_combine"]),
        float(row["probability_at_least_two_payouts_per_successful_combine"]),
        float(row["payout_cycles_per_successful_combine"]),
        float(checkpoints["30"]["demonstrated_survival_rate_all_first_payout_paths"] or 0.0),
        float(checkpoints["60"]["demonstrated_survival_rate_all_first_payout_paths"] or 0.0),
        float(checkpoints["90"]["demonstrated_survival_rate_all_first_payout_paths"] or 0.0),
        -float(row["mll_breach_rate_per_successful_combine"]),
        float(row["minimum_mll_buffer_usd"]),
    )


def _assign_pareto(rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row["policy_id"]),
                int(row["horizon_trading_days"]),
                str(row["path"]),
            )
        ].append(row)
    for values in grouped.values():
        for row in values:
            vector = _pareto_vector(row)
            row["pareto_nondominated"] = not any(
                other is not row and _dominates(_pareto_vector(other), vector)
                for other in values
            )


def _dominates(left: Sequence[float], right: Sequence[float]) -> bool:
    return all(a >= b - 1e-12 for a, b in zip(left, right)) and any(
        a > b + 1e-12 for a, b in zip(left, right)
    )


def _select_profiles(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row["policy_id"]),
                int(row["horizon_trading_days"]),
                str(row["path"]),
            )
        ].append(row)
    output = []
    role_order = {"LONGEVITY": 0, "BALANCED": 1, "HARVEST": 2}
    for key, values in sorted(grouped.items()):
        nondominated = [row for row in values if row["pareto_nondominated"]]
        selected = sorted(
            nondominated,
            key=lambda row: (
                -float(
                    row["post_payout_survival"]["checkpoints"]["30"][
                        "demonstrated_survival_rate_all_first_payout_paths"
                    ]
                    or 0.0
                ),
                float(row["mll_breach_rate_per_successful_combine"]),
                -float(row["minimum_mll_buffer_usd"]),
                -float(row["expected_trader_net_payout_per_new_combine_attempt_usd"]),
                role_order[str(row["frontier_role"])],
                float(row["post_payout_risk_scale"]),
                str(row["frontier_policy_id"]),
            ),
        )[0]
        output.append(
            {
                "policy_id": key[0],
                "horizon_trading_days": key[1],
                "path": key[2],
                "selected_frontier_policy_id": selected["frontier_policy_id"],
                "selected_role": selected["frontier_role"],
                "selected_post_payout_risk_scale": selected[
                    "post_payout_risk_scale"
                ],
                "selection_is_development_diagnostic_only": True,
                "aggregate_hash": selected["aggregate_hash"],
            }
        )
    return output


def _verify_source(value: Mapping[str, Any]) -> None:
    if value.get("schema") != SOURCE_SCHEMA:
        raise PnLStateXfaSurvivalFrontierError("source schema drift")
    expected = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    if value.get("result_hash") != SOURCE_RESULT_HASH or expected != SOURCE_RESULT_HASH:
        raise PnLStateXfaSurvivalFrontierError("source result hash drift")
    counts = value.get("counts") or {}
    if (
        int(counts.get("clean_normal_combine_transition_count", -1)) != 71
        or int(counts.get("alternative_path_count", -1)) != 142
    ):
        raise PnLStateXfaSurvivalFrontierError("source XFA count drift")


__all__ = [
    "DEFAULT_SOURCE",
    "PnLStateXfaSurvivalFrontierError",
    "SCHEMA",
    "SOURCE_RESULT_HASH",
    "build_pnl_state_xfa_survival_frontier",
]
