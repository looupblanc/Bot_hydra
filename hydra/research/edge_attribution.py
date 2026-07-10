from __future__ import annotations

from typing import Any


def infer_edge_components(candidate_row: dict[str, Any]) -> dict[str, Any]:
    family = str(candidate_row.get("family") or "")
    components = []
    if "divergence" in family:
        components.append("cross_market_residual_confirmation")
    if "opening_range" in family:
        components.append("opening_session_state")
    if "volatility" in family:
        components.append("volatility_state_transition")
    if "vwap" in family:
        components.append("vwap_exhaustion_context")
    if not components:
        components.append("generic_regime_condition")
    return {
        "candidate_id": candidate_row.get("candidate_id"),
        "hypothesized_components": components,
        "likely_failure_regime": _failure_regime(candidate_row),
        "falsification_test": "ablate_component_and_require_non_degraded_oos_or_mll_path",
    }


def _failure_regime(candidate_row: dict[str, Any]) -> str:
    reason = str(candidate_row.get("rejection_reason") or "")
    if "mll" in reason:
        return "tail_loss_or_adverse_excursion"
    if "target" in reason:
        return "insufficient_profit_velocity"
    if "oos" in reason or "split" in reason:
        return "regime_instability"
    if "fragile" in reason:
        return "sequence_order_dependence"
    return "unknown_or_mixed"

