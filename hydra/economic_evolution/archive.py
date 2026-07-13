from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class BehavioralDescriptor:
    market: str
    session: str
    timeframe: str
    direction_balance: str
    trade_frequency: str
    holding_horizon: str
    volatility_regime: str
    trend_range_behavior: str
    pnl_skew: str
    drawdown_shape: str
    loss_clustering: str
    target_velocity: str
    mll_usage: str
    cost_sensitivity: str
    correlation_cluster: str
    account_role: str

    @property
    def niche(self) -> tuple[str, ...]:
        return (
            self.market,
            self.session,
            self.timeframe,
            self.trade_frequency,
            self.holding_horizon,
            self.volatility_regime,
            self.target_velocity,
            self.mll_usage,
            self.cost_sensitivity,
            self.correlation_cluster,
            self.account_role,
        )


@dataclass(frozen=True, slots=True)
class ParetoObjectives:
    stressed_net_pnl: float
    target_progress: float
    target_velocity: float
    combine_pass_rate_diagnostic: float
    mll_breach_rate: float
    consistency_rate: float
    xfa_survival_rate: float
    expected_payouts: float
    total_cost: float
    complexity: float


@dataclass(frozen=True, slots=True)
class ArchiveEntry:
    policy_id: str
    family: str
    lineage_id: str
    descriptor: BehavioralDescriptor
    objectives: ParetoObjectives
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ArchiveDecision:
    accepted: bool
    reason: str
    removed_policy_ids: tuple[str, ...] = ()


class ParetoQualityDiversityArchive:
    """Pareto archive with niche capacity and feasible soft diversity caps."""

    def __init__(self, *, maximum_per_niche: int = 3) -> None:
        if maximum_per_niche < 1:
            raise ValueError("maximum_per_niche must be positive")
        self.maximum_per_niche = maximum_per_niche
        self._entries: dict[tuple[str, ...], list[ArchiveEntry]] = {}
        self._policy_ids: set[str] = set()

    @property
    def entries(self) -> tuple[ArchiveEntry, ...]:
        return tuple(
            row
            for niche in sorted(self._entries)
            for row in sorted(self._entries[niche], key=lambda value: value.policy_id)
        )

    def insert(self, entry: ArchiveEntry) -> ArchiveDecision:
        if entry.policy_id in self._policy_ids:
            return ArchiveDecision(False, "DUPLICATE_POLICY_ID")
        niche = list(self._entries.get(entry.descriptor.niche, ()))
        if any(dominates(existing.objectives, entry.objectives) for existing in niche):
            return ArchiveDecision(False, "PARETO_DOMINATED_IN_NICHE")
        dominated = [
            existing
            for existing in niche
            if dominates(entry.objectives, existing.objectives)
        ]
        for existing in dominated:
            niche.remove(existing)
            self._policy_ids.remove(existing.policy_id)
        niche.append(entry)
        self._policy_ids.add(entry.policy_id)
        removed = [row.policy_id for row in dominated]
        if len(niche) > self.maximum_per_niche:
            crowding = _crowding_distance(niche)
            weakest = min(
                niche,
                key=lambda row: (crowding[row.policy_id], row.policy_id),
            )
            niche.remove(weakest)
            self._policy_ids.remove(weakest.policy_id)
            removed.append(weakest.policy_id)
            if weakest.policy_id == entry.policy_id:
                self._entries[entry.descriptor.niche] = niche
                return ArchiveDecision(False, "NICHE_CROWDING_REJECTION", tuple(removed))
        self._entries[entry.descriptor.niche] = niche
        return ArchiveDecision(True, "PARETO_NICHE_ACCEPTED", tuple(removed))

    def summary(self) -> dict[str, Any]:
        entries = self.entries
        return {
            "entry_count": len(entries),
            "niche_count": len([rows for rows in self._entries.values() if rows]),
            "markets": _counts(row.descriptor.market for row in entries),
            "sessions": _counts(row.descriptor.session for row in entries),
            "timeframes": _counts(row.descriptor.timeframe for row in entries),
            "roles": _counts(row.descriptor.account_role for row in entries),
            "families": _counts(row.family for row in entries),
            "lineage_count": len({row.lineage_id for row in entries}),
            "maximum_diagnostic_pass_rate": max(
                (row.objectives.combine_pass_rate_diagnostic for row in entries),
                default=0.0,
            ),
            "minimum_mll_breach_rate": min(
                (row.objectives.mll_breach_rate for row in entries), default=0.0
            ),
        }


def dominates(left: ParetoObjectives, right: ParetoObjectives) -> bool:
    left_values = _objective_vector(left)
    right_values = _objective_vector(right)
    return all(a >= b - 1e-12 for a, b in zip(left_values, right_values, strict=True)) and any(
        a > b + 1e-12 for a, b in zip(left_values, right_values, strict=True)
    )


def _objective_vector(value: ParetoObjectives) -> tuple[float, ...]:
    return (
        value.stressed_net_pnl,
        value.target_progress,
        value.target_velocity,
        value.combine_pass_rate_diagnostic,
        -value.mll_breach_rate,
        value.consistency_rate,
        value.xfa_survival_rate,
        value.expected_payouts,
        -value.total_cost,
        -value.complexity,
    )


def _crowding_distance(entries: Iterable[ArchiveEntry]) -> dict[str, float]:
    values = list(entries)
    distance = {row.policy_id: 0.0 for row in values}
    if len(values) <= 2:
        return {row.policy_id: float("inf") for row in values}
    vectors = {row.policy_id: _objective_vector(row.objectives) for row in values}
    for dimension in range(len(next(iter(vectors.values())))):
        ordered = sorted(values, key=lambda row: vectors[row.policy_id][dimension])
        low = vectors[ordered[0].policy_id][dimension]
        high = vectors[ordered[-1].policy_id][dimension]
        distance[ordered[0].policy_id] = float("inf")
        distance[ordered[-1].policy_id] = float("inf")
        scale = max(high - low, 1e-12)
        for index in range(1, len(ordered) - 1):
            if distance[ordered[index].policy_id] == float("inf"):
                continue
            previous = vectors[ordered[index - 1].policy_id][dimension]
            following = vectors[ordered[index + 1].policy_id][dimension]
            distance[ordered[index].policy_id] += (following - previous) / scale
    return distance


def _counts(values: Iterable[str]) -> dict[str, int]:
    output: dict[str, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items()))


__all__ = [
    "ArchiveDecision",
    "ArchiveEntry",
    "BehavioralDescriptor",
    "ParetoObjectives",
    "ParetoQualityDiversityArchive",
    "dominates",
]
