"""Production-only accounting for an open-position shared-account MLL breach.

The frozen shared-account simulator detects a correlated adverse excursion while
positions are open, but its historical return object retains the pre-liquidation
cash balance.  Production evidence must not turn the eventual trade result into
the breach liquidation result.  This module reconstructs the already-frozen
routing decisions and realizes the positions that were open at the breach at
their conservative adverse excursion.

The correction is deliberately outside the shared simulator.  It changes no
routing decision and sees trade outcomes only after those decisions are frozen.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import AccountPolicyEpisode, RoutedTrade
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.mll_variants import advance_intraday_floor
from hydra.propfirm.topstep_150k import Topstep150KConfig


CORRELATED_OPEN_POSITION_MLL_REASON = (
    "correlated_open_position_mll_touch_or_breach"
)


class ProductionMLLAccountingError(ValueError):
    """Raised when frozen routing evidence cannot be reconciled exactly."""


@dataclass(frozen=True, slots=True)
class _AcceptedPosition:
    routed: RoutedTrade
    net_pnl: float
    worst_unrealized_pnl: float
    best_unrealized_pnl: float


def realize_correlated_open_position_mll_breach(
    episode: AccountPolicyEpisode,
    component_events: Mapping[str, Sequence[RoutedTrade]],
    *,
    config: Topstep150KConfig | None = None,
) -> AccountPolicyEpisode:
    """Return an evidence-correct MLL episode without changing frozen routing.

    Only ``correlated_open_position_mll_touch_or_breach`` episodes are eligible.
    Every other episode is returned by identity.  The accepted routing rows on
    the terminal day are resolved against the immutable component trade ledger,
    replayed up to the observed breach, and every position still open then is
    liquidated at ``min(worst_unrealized_pnl, 0)``.

    Reapplying the function to an already-corrected episode is an identity
    operation.  Missing, ambiguous, or economically inconsistent evidence fails
    closed instead of fabricating a liquidation.
    """

    if episode.terminal_reason != CORRELATED_OPEN_POSITION_MLL_REASON:
        return episode

    rules = config or Topstep150KConfig()
    _validate_target_episode(episode)
    terminal_rows = tuple(dict(row) for row in episode.daily_path)
    terminal_day = int(terminal_rows[-1]["session_day"])
    accepted = _accepted_terminal_decisions(episode, terminal_day, component_events)
    previous_balance, opening_floor = _terminal_opening_state(
        terminal_rows, rules
    )
    reconstruction = _reconstruct_breach(
        accepted,
        previous_balance=previous_balance,
        opening_floor=opening_floor,
        rules=rules,
    )

    desired_balance = reconstruction.breach_equity
    desired_day_pnl = desired_balance - previous_balance
    desired_net = desired_balance - float(rules.combine_starting_balance)
    required_target, prior_best_day = _required_target_before_terminal_day(
        terminal_rows, rules
    )
    desired_progress = desired_net / max(required_target, 1.0)
    desired_concentration = (
        prior_best_day / desired_net if desired_net > 0.0 else 0.0
    )
    desired_consistency = bool(
        desired_net > 0.0
        and desired_concentration
        <= float(rules.consistency_best_day_max_pct_of_profit_target) + 1e-12
    )

    # The absolute balance/path reconstruction makes a repeated call safe.  Do
    # not allocate a new dataclass when the episode has already been corrected.
    if _already_corrected(
        episode,
        desired_balance=desired_balance,
        desired_day_pnl=desired_day_pnl,
        desired_net=desired_net,
        desired_progress=desired_progress,
        desired_consistency=desired_consistency,
    ):
        return episode

    current_balance = float(terminal_rows[-1]["balance"])
    current_day_pnl = float(terminal_rows[-1]["day_pnl"])
    if not _close(current_balance, previous_balance + current_day_pnl):
        raise ProductionMLLAccountingError(
            "terminal daily balance does not reconcile before MLL liquidation"
        )
    if not _close(episode.net_pnl, current_balance - rules.combine_starting_balance):
        raise ProductionMLLAccountingError(
            "episode net PnL does not reconcile before MLL liquidation"
        )
    contribution = {
        str(component_id): float(value)
        for component_id, value in episode.component_contribution.items()
    }
    if not _close(sum(contribution.values()), float(episode.net_pnl)):
        raise ProductionMLLAccountingError(
            "component contribution does not reconcile before MLL liquidation"
        )
    for component_id, loss in reconstruction.open_loss_by_component.items():
        contribution[component_id] = contribution.get(component_id, 0.0) + loss
    if not _close(sum(contribution.values()), desired_net):
        raise ProductionMLLAccountingError(
            "component contribution does not reconcile after MLL liquidation"
        )

    corrected_terminal = dict(terminal_rows[-1])
    corrected_terminal["balance"] = float(desired_balance)
    corrected_terminal["day_pnl"] = float(desired_day_pnl)
    corrected_terminal["mll_floor"] = float(reconstruction.mll_floor)
    if "target_progress" in corrected_terminal:
        corrected_terminal["target_progress"] = float(desired_progress)
    if "consistency_ok" in corrected_terminal:
        corrected_terminal["consistency_ok"] = desired_consistency
    if "best_day_concentration" in corrected_terminal:
        corrected_terminal["best_day_concentration"] = float(
            desired_concentration
        )
    if "component_attribution" in corrected_terminal:
        corrected_terminal["component_attribution"] = dict(
            sorted(reconstruction.corrected_terminal_contribution.items())
        )
    corrected_daily_path = terminal_rows[:-1] + (corrected_terminal,)

    prior_terminal_shared_loss = int(
        sum(
            value < 0.0
            for value in reconstruction.realized_terminal_contribution.values()
        )
        >= 2
    )
    corrected_terminal_shared_loss = int(
        sum(
            value < 0.0
            for value in reconstruction.corrected_terminal_contribution.values()
        )
        >= 2
    )

    return replace(
        episode,
        net_pnl=float(desired_net),
        target_progress=float(desired_progress),
        consistency_ok=desired_consistency,
        best_day_concentration=float(desired_concentration),
        shared_loss_days=(
            int(episode.shared_loss_days)
            - prior_terminal_shared_loss
            + corrected_terminal_shared_loss
        ),
        component_contribution=dict(sorted(contribution.items())),
        daily_path=corrected_daily_path,
    )


@dataclass(frozen=True, slots=True)
class _BreachReconstruction:
    breach_equity: float
    mll_floor: float
    open_loss_by_component: dict[str, float]
    realized_terminal_contribution: dict[str, float]
    corrected_terminal_contribution: dict[str, float]


def _validate_target_episode(episode: AccountPolicyEpisode) -> None:
    if episode.terminal is not CombineTerminal.MLL_BREACH or not episode.mll_breached:
        raise ProductionMLLAccountingError(
            "correlated open-position MLL reason requires an MLL terminal"
        )
    if not episode.daily_path:
        raise ProductionMLLAccountingError(
            "correlated open-position MLL episode has no daily path"
        )


def _accepted_terminal_decisions(
    episode: AccountPolicyEpisode,
    terminal_day: int,
    component_events: Mapping[str, Sequence[RoutedTrade]],
) -> tuple[tuple[int, Mapping[str, Any], RoutedTrade], ...]:
    lookup: dict[tuple[str, int, int], list[RoutedTrade]] = defaultdict(list)
    for mapping_component_id, values in component_events.items():
        for routed in values:
            if str(mapping_component_id) != routed.component_id:
                raise ProductionMLLAccountingError(
                    "component ledger key disagrees with RoutedTrade.component_id"
                )
            lookup[
                (
                    routed.component_id,
                    int(routed.event.decision_ns),
                    int(routed.event.session_day),
                )
            ].append(routed)

    accepted: list[tuple[int, Mapping[str, Any], RoutedTrade]] = []
    for decision_index, raw in enumerate(episode.risk_allocation_path):
        if int(raw["session_day"]) != terminal_day or not bool(raw["allow"]):
            continue
        component_id = str(raw["component_id"])
        decision_ns = int(raw["decision_ns"])
        matches = lookup[(component_id, decision_ns, terminal_day)]
        if len(matches) != 1:
            raise ProductionMLLAccountingError(
                "accepted routing decision does not resolve to exactly one immutable trade"
            )
        quantity = int(raw["quantity"])
        if quantity <= 0:
            raise ProductionMLLAccountingError(
                "accepted routing decision has non-positive quantity"
            )
        accepted.append((decision_index, raw, matches[0]))
    if not accepted:
        raise ProductionMLLAccountingError(
            "correlated open-position MLL episode has no accepted terminal-day route"
        )
    return tuple(accepted)


def _terminal_opening_state(
    daily_path: Sequence[Mapping[str, Any]], rules: Topstep150KConfig
) -> tuple[float, float]:
    if len(daily_path) == 1:
        return (
            float(rules.combine_starting_balance),
            float(rules.combine_starting_mll),
        )
    return (
        float(daily_path[-2]["balance"]),
        float(daily_path[-2]["mll_floor"]),
    )


def _reconstruct_breach(
    accepted: Sequence[tuple[int, Mapping[str, Any], RoutedTrade]],
    *,
    previous_balance: float,
    opening_floor: float,
    rules: Topstep150KConfig,
) -> _BreachReconstruction:
    actions: list[
        tuple[int, int, int, str, Mapping[str, Any], RoutedTrade]
    ] = []
    for decision_index, decision, routed in accepted:
        actions.append(
            (
                int(routed.event.exit_ns),
                0,
                decision_index,
                routed.event.event_id,
                decision,
                routed,
            )
        )
        actions.append(
            (
                int(routed.event.decision_ns),
                1,
                decision_index,
                routed.event.event_id,
                decision,
                routed,
            )
        )
    # Routing-decision order is the frozen priority tiebreak for simultaneous
    # entries; exits precede entries exactly as in the production V7.2 replay.
    actions.sort(key=lambda row: (row[0], row[1], row[2], row[3]))

    balance = float(previous_balance)
    floor = float(opening_floor)
    open_positions: dict[tuple[str, str, int], _AcceptedPosition] = {}
    realized: dict[str, float] = defaultdict(float)
    for _timestamp, kind, _index, _event_id, decision, routed in actions:
        event = routed.event
        position_key = (
            routed.component_id,
            event.event_id,
            int(event.decision_ns),
        )
        if kind == 0:
            position = open_positions.pop(position_key, None)
            if position is None:
                continue
            balance += position.net_pnl
            realized[routed.component_id] += position.net_pnl
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance,
                distance=float(rules.combine_max_loss_limit),
                lock=float(rules.combine_starting_balance),
                variant=rules.resolved_mll_mode,
            )
            continue

        ratio = int(decision["quantity"]) / int(event.quantity)
        position = _AcceptedPosition(
            routed=routed,
            net_pnl=float(event.net_pnl * ratio),
            worst_unrealized_pnl=float(event.worst_unrealized_pnl * ratio),
            best_unrealized_pnl=float(event.best_unrealized_pnl * ratio),
        )
        open_positions[position_key] = position
        floor = advance_intraday_floor(
            floor,
            live_equity_high=balance
            + sum(
                max(item.best_unrealized_pnl, 0.0)
                for item in open_positions.values()
            ),
            distance=float(rules.combine_max_loss_limit),
            lock=float(rules.combine_starting_balance),
            variant=rules.resolved_mll_mode,
        )
        breach_equity = balance + sum(
            min(item.worst_unrealized_pnl, 0.0)
            for item in open_positions.values()
        )
        if breach_equity > floor:
            continue

        open_losses: dict[str, float] = defaultdict(float)
        for item in open_positions.values():
            open_losses[item.routed.component_id] += min(
                item.worst_unrealized_pnl, 0.0
            )
        terminal_contribution = dict(realized)
        for component_id, loss in open_losses.items():
            terminal_contribution[component_id] = (
                terminal_contribution.get(component_id, 0.0) + loss
            )
        return _BreachReconstruction(
            breach_equity=float(breach_equity),
            mll_floor=float(floor),
            open_loss_by_component=dict(sorted(open_losses.items())),
            realized_terminal_contribution=dict(sorted(realized.items())),
            corrected_terminal_contribution=dict(
                sorted(terminal_contribution.items())
            ),
        )

    raise ProductionMLLAccountingError(
        "frozen accepted routing decisions do not reproduce the recorded MLL breach"
    )


def _required_target_before_terminal_day(
    daily_path: Sequence[Mapping[str, Any]], rules: Topstep150KConfig
) -> tuple[float, float]:
    required = float(rules.combine_profit_target)
    best_day = 0.0
    consistency_limit = float(
        rules.consistency_best_day_max_pct_of_profit_target
    )
    for row in daily_path[:-1]:
        best_day = max(best_day, float(row["day_pnl"]))
        if best_day > float(rules.combine_profit_target) * consistency_limit:
            required = max(required, best_day / consistency_limit)
    return required, best_day


def _already_corrected(
    episode: AccountPolicyEpisode,
    *,
    desired_balance: float,
    desired_day_pnl: float,
    desired_net: float,
    desired_progress: float,
    desired_consistency: bool,
) -> bool:
    terminal = episode.daily_path[-1]
    if not (
        _close(float(terminal["balance"]), desired_balance)
        and _close(float(terminal["day_pnl"]), desired_day_pnl)
        and _close(float(episode.net_pnl), desired_net)
        and _close(float(episode.target_progress), desired_progress)
        and bool(episode.consistency_ok) is desired_consistency
        and _close(sum(episode.component_contribution.values()), desired_net)
    ):
        return False
    return True


def _close(left: float, right: float) -> bool:
    return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-9)


__all__ = [
    "CORRELATED_OPEN_POSITION_MLL_REASON",
    "ProductionMLLAccountingError",
    "realize_correlated_open_position_mll_breach",
]
