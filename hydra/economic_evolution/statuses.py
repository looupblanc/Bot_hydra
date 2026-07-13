from __future__ import annotations

from typing import Any, Mapping, Protocol


class _CombineSummary(Protocol):
    pass_count: int
    mll_breach_rate: float
    consistency_pass_rate: float


class _StressSummary(Protocol):
    median_episode_net_pnl: float


class _AccountEvaluation(Protocol):
    controlled_base: _CombineSummary
    controlled_stress_1_5x: _StressSummary


RESEARCH_FALLBACK_STATUSES = frozenset(
    {
        "ACCOUNT_POLICY_DIAGNOSTIC_ONLY",
        "ACCOUNT_POLICY_RESEARCH_CANDIDATE",
    }
)


def rolling_research_status(
    result: _AccountEvaluation,
    gate: Mapping[str, Any],
    *,
    fallback_status: str,
) -> str:
    """Promote a real Combine path, otherwise preserve the upstream status.

    This helper is prospective.  The hash-bound pilot-0001 implementation is
    deliberately left unchanged and its mislabeled rows are reconciled in a
    separate immutable report.
    """

    if fallback_status not in RESEARCH_FALLBACK_STATUSES:
        raise ValueError(f"invalid rolling fallback status: {fallback_status}")
    base = result.controlled_base
    stress = result.controlled_stress_1_5x
    passed = bool(
        base.pass_count >= int(gate["minimum_pass_count"])
        and base.mll_breach_rate <= float(gate["maximum_mll_breach_rate"])
        and stress.median_episode_net_pnl
        > float(gate["minimum_stressed_median_net"])
        and base.consistency_pass_rate
        >= float(gate["minimum_consistency_pass_rate"])
    )
    return "COMBINE_PATH_CANDIDATE" if passed else fallback_status


__all__ = ["RESEARCH_FALLBACK_STATUSES", "rolling_research_status"]
