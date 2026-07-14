from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.production.policy_factory import ComponentCandidate


def materialize_component_evidence(
    campaign_id: str,
    runtimes: Mapping[str, ExactSleeveRuntime],
    components: Mapping[str, ComponentCandidate],
    matrices: Mapping[str, FeatureMatrix],
) -> dict[str, list[dict[str, Any]]]:
    """Convert immutable exact component runtimes into the V1 evidence ledgers."""

    signals: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for component_id, runtime in sorted(runtimes.items()):
        candidate = components[component_id]
        sleeve = candidate.sleeve
        matrix = matrices[runtime.signal_market]
        decisions = matrix.array("decision_ns")
        entry_prices = matrix.array("entry_price")
        point_value = float(instrument_spec(runtime.execution_market).point_value)
        for routed in runtime.events:
            event = routed.event
            index = int(np.searchsorted(decisions, event.decision_ns))
            if index >= len(decisions) or int(decisions[index]) != int(event.decision_ns):
                raise ValueError(f"component event lacks exact feature row: {event.event_id}")
            entry_price = float(entry_prices[index])
            quantity = int(event.quantity)
            side = "LONG" if routed.side > 0 else "SHORT"
            exit_price = entry_price + float(event.gross_pnl) / (
                float(routed.side) * point_value * quantity
            )
            costs = float(event.gross_pnl - event.net_pnl)
            event_time = _ns_iso(int(event.decision_ns))
            exit_time = _ns_iso(int(event.exit_ns))
            common = {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "trade_id": event.event_id,
            }
            signals.append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "signal_id": event.event_id,
                    "event_time": event_time,
                    "market": runtime.signal_market,
                    "contract": runtime.execution_market,
                    "timeframe": sleeve.timeframe,
                    "signal": {
                        "side": int(routed.side),
                        "trigger_feature": sleeve.trigger_feature,
                        "session_code": sleeve.session_code,
                        "closed_or_past_only": True,
                    },
                    "sizing": float(quantity),
                    "stop": None,
                    "target": None,
                    "veto": False,
                    "component_role": runtime.role.value,
                }
            )
            entries.append(
                {
                    **common,
                    "entry_time": event_time,
                    "market": runtime.signal_market,
                    "contract": runtime.execution_market,
                    "side": side,
                    "quantity": quantity,
                    "entry_price": entry_price,
                    "sizing": float(quantity),
                    "stop_price": None,
                    "target_price": None,
                }
            )
            exits.append(
                {
                    **common,
                    "exit_time": exit_time,
                    "exit_price": float(exit_price),
                    "exit_reason": f"EXACT_TIME_EXIT_{sleeve.holding_bars}",
                }
            )
            trades.append(
                {
                    **common,
                    "entry_time": event_time,
                    "exit_time": exit_time,
                    "market": runtime.signal_market,
                    "contract": runtime.execution_market,
                    "side": side,
                    "quantity": quantity,
                    "entry_price": entry_price,
                    "exit_price": float(exit_price),
                    "gross_pnl": float(event.gross_pnl),
                    "costs": costs,
                    "net_pnl": float(event.net_pnl),
                }
            )
    return {
        "component_signals": signals,
        "component_entries": entries,
        "component_exits": exits,
        "component_trades": trades,
    }


def _ns_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat().replace(
        "+00:00", "Z"
    )


__all__ = ["materialize_component_evidence"]
