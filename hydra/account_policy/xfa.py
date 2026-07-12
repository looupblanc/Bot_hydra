from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_fitness import xfa_payout_fitness
from hydra.propfirm.payout_episode import evaluate_rolling_xfa
from hydra.propfirm.topstep_150k import Topstep150KConfig


def evaluate_serial_xfa_basket(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_session_days: Sequence[int],
    *,
    basket: BasketPolicy,
    maximum_starts: int = 12,
    config: Topstep150KConfig | None = None,
) -> dict[str, Any]:
    """Evaluate a frozen XFA version using one globally serial open position.

    Serial routing is deliberately conservative: it eliminates correlated
    intraday-MAE ambiguity across component markets before the existing exact
    XFA payout simulator is used. It is a separately frozen XFA policy, not an
    inheritance of the Combine basket's status.
    """

    priority = basket.component_priority or basket.component_ids
    rank = {component_id: index for index, component_id in enumerate(priority)}
    ordered = sorted(
        (
            trade
            for component_id in basket.component_ids
            for trade in component_events.get(component_id, ())
        ),
        key=lambda trade: (
            trade.event.decision_ns,
            rank[trade.component_id],
            trade.event.event_id,
        ),
    )
    accepted: list[TradePathEvent] = []
    skipped: Counter[str] = Counter()
    active_exit = -1
    for trade in ordered:
        event = trade.event
        if event.decision_ns < active_exit:
            skipped["GLOBAL_SERIAL_CONFLICT"] += 1
            continue
        accepted.append(event)
        active_exit = event.exit_ns
    if not accepted:
        raise ValueError("XFA basket has no executable serial events")
    regimes = _past_only_day_regimes(accepted, eligible_session_days)
    summary = evaluate_rolling_xfa(
        accepted,
        eligible_session_days,
        day_regimes=regimes,
        maximum_starts=maximum_starts,
        config=config,
    )
    fitness = xfa_payout_fitness(
        summary,
        complexity=float(len(basket.component_ids)),
    )
    return {
        "policy_id": basket.policy_id,
        "xfa_policy_id": f"{basket.policy_id}::GLOBAL_SERIAL_XFA_V1",
        "basket": basket.to_dict(),
        "routing_policy": "GLOBAL_SERIAL_FIXED_PRIORITY",
        "accepted_event_count": len(accepted),
        "skipped_event_count": sum(skipped.values()),
        "skipped_reasons": dict(sorted(skipped.items())),
        "rolling_xfa": summary.to_dict(),
        "xfa_fitness": fitness.to_dict(),
        "inherited_status": False,
        "outbound_order_capability": False,
    }


def _past_only_day_regimes(
    events: Sequence[TradePathEvent], eligible_days: Sequence[int]
) -> dict[int, str]:
    observed: dict[int, list[str]] = {}
    for event in events:
        observed.setdefault(int(event.session_day), []).append(event.regime)
    output: dict[int, str] = {}
    previous = "UNKNOWN"
    for day in sorted({int(value) for value in eligible_days}):
        output[day] = previous
        values = observed.get(day, ())
        if values:
            previous = Counter(values).most_common(1)[0][0]
    return output


__all__ = ["evaluate_serial_xfa_basket"]
