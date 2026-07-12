from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


def _bounded(value: float, scale: float) -> float:
    return max(0.0, min(float(value) / max(scale, 1e-12), 1.0))


@dataclass(frozen=True)
class CandidateValue:
    combine_utility: float
    xfa_utility: float
    defensive_utility: float
    portfolio_utility: float
    target_velocity: float
    mll_buffer: float
    consistency_margin: float
    cost_resilience: float
    forward_opportunity_frequency: float
    expected_account_utility: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_value(candidate: Mapping[str, Any]) -> CandidateValue:
    role = str(candidate.get("role") or "")
    topstep = dict(candidate.get("topstep") or {})
    combine = dict(topstep.get("combine") or {})
    xfa_standard = dict(topstep.get("xfa_standard") or {})
    xfa_consistency = dict(topstep.get("xfa_consistency") or {})
    events = max(int(candidate.get("events") or 0), 0)
    net = float(candidate.get("net_pnl") or 0.0)
    drawdown = max(float(candidate.get("maximum_drawdown") or 0.0), 0.0)
    stressed = float(candidate.get("cost_stress_1_5x_net") or 0.0)
    combine_utility = (
        0.45 * float(bool(topstep.get("path_candidate")))
        + 0.30 * _bounded(float(combine.get("min_mll_buffer") or 0.0), 4_500.0)
        + 0.25 * _bounded(net, 15_000.0)
    )
    payout_cycles = max(
        int(xfa_standard.get("payout_cycles_survived") or 0),
        int(xfa_consistency.get("payout_cycles_survived") or 0),
    )
    xfa_utility = (
        0.55 * _bounded(payout_cycles, 4.0)
        + 0.20 * float(bool(xfa_standard.get("survived") or xfa_consistency.get("survived")))
        + 0.25 * _bounded(net, 10_000.0)
    )
    defensive_utility = 0.6 * _bounded(net / max(drawdown, 1.0), 8.0) + 0.4 * _bounded(
        float(combine.get("min_mll_buffer") or 0.0), 4_500.0
    )
    portfolio_utility = 0.5 * defensive_utility + 0.5 * _bounded(events, 120.0)
    primary = {
        "COMBINE_PASSER": combine_utility,
        "XFA_PAYOUT": xfa_utility,
        "DEFENSIVE": defensive_utility,
        "PORTFOLIO_ONLY": portfolio_utility,
    }.get(role, max(combine_utility, xfa_utility, defensive_utility))
    return CandidateValue(
        combine_utility=combine_utility,
        xfa_utility=xfa_utility,
        defensive_utility=defensive_utility,
        portfolio_utility=portfolio_utility,
        target_velocity=_bounded(net / max(events, 1), 500.0),
        mll_buffer=_bounded(float(combine.get("min_mll_buffer") or 0.0), 4_500.0),
        consistency_margin=1.0 - min(float(combine.get("best_day_pct_of_total_profit") or 1.0), 1.0),
        cost_resilience=_bounded(stressed, max(abs(net), 1.0)),
        forward_opportunity_frequency=_bounded(events, 120.0),
        expected_account_utility=primary,
    )
