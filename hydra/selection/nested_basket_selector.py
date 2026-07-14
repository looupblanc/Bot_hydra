from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


class SelectorError(ValueError):
    """Raised when a frozen selector cannot make an auditable decision."""


@dataclass(frozen=True, slots=True)
class ParetoObjective:
    field: str
    direction: str

    def __post_init__(self) -> None:
        if not self.field:
            raise SelectorError("Pareto objective field is required")
        if self.direction not in {"maximize", "minimize"}:
            raise SelectorError("Pareto direction must be maximize or minimize")


@dataclass(frozen=True, slots=True)
class SelectionDecision:
    primary: dict[str, Any]
    backup: dict[str, Any] | None
    eligible_count: int
    hard_rejection_count: int
    behavioral_clone_rejection_count: int
    pareto_frontier_count: int
    pareto_frontier_ids: tuple[str, ...]
    hard_rejections: tuple[dict[str, Any], ...]
    clone_groups: tuple[dict[str, Any], ...]
    deterministic_order: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "primary": self.primary,
            "backup": self.backup,
            "eligible_count": self.eligible_count,
            "hard_rejection_count": self.hard_rejection_count,
            "behavioral_clone_rejection_count": (
                self.behavioral_clone_rejection_count
            ),
            "pareto_frontier_count": self.pareto_frontier_count,
            "pareto_frontier_ids": list(self.pareto_frontier_ids),
            "hard_rejections": list(self.hard_rejections),
            "clone_groups": list(self.clone_groups),
            "deterministic_order": list(self.deterministic_order),
        }


def select_pareto_champion(
    records: Sequence[Mapping[str, Any]],
    *,
    objectives: Sequence[ParetoObjective],
    maximum_mll_breach_rate: float,
    maximum_component_profit_share: float,
    backup_maximum_component_jaccard: float = 0.80,
) -> SelectionDecision:
    """Apply frozen hard filters, design-only clone collapse, and Pareto order.

    The objective list is also the deterministic lexicographic tie-break order.
    No weighted or learned score is constructed.
    """

    if not records:
        raise SelectorError("selector requires at least one design record")
    if not objectives or len({row.field for row in objectives}) != len(objectives):
        raise SelectorError("selector objectives must be non-empty and unique")
    if not 0.0 <= maximum_mll_breach_rate <= 1.0:
        raise SelectorError("MLL tolerance must be a probability")
    if not 0.0 < maximum_component_profit_share <= 1.0:
        raise SelectorError("component concentration limit is invalid")
    if not 0.0 <= backup_maximum_component_jaccard < 1.0:
        raise SelectorError("backup Jaccard limit must be in [0, 1)")

    normalized = [_normalize_record(row, objectives) for row in records]
    identifiers = [str(row["variant_id"]) for row in normalized]
    if len(set(identifiers)) != len(identifiers):
        raise SelectorError("selector variant IDs must be unique")

    eligible: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    for row in normalized:
        reasons = _hard_rejection_reasons(
            row,
            maximum_mll_breach_rate=maximum_mll_breach_rate,
            maximum_component_profit_share=maximum_component_profit_share,
        )
        if reasons:
            rejections.append(
                {"variant_id": row["variant_id"], "reasons": reasons}
            )
        else:
            eligible.append(row)
    if not eligible:
        raise SelectorError("no selector variant passed frozen hard requirements")

    clone_buckets: dict[str, list[dict[str, Any]]] = {}
    for row in eligible:
        clone_buckets.setdefault(str(row["design_behavior_fingerprint"]), []).append(
            row
        )
    clone_groups: list[dict[str, Any]] = []
    representatives: list[dict[str, Any]] = []
    for fingerprint, members in sorted(clone_buckets.items()):
        ordered = sorted(members, key=lambda row: _lexicographic_key(row, objectives))
        representative = ordered[0]
        representatives.append(representative)
        clone_groups.append(
            {
                "design_behavior_fingerprint": fingerprint,
                "representative_variant_id": representative["variant_id"],
                "member_variant_ids": [row["variant_id"] for row in ordered],
            }
        )

    frontier = [
        row
        for row in representatives
        if not any(
            _dominates(other, row, objectives)
            for other in representatives
            if other["variant_id"] != row["variant_id"]
        )
    ]
    ordered_frontier = sorted(
        frontier, key=lambda row: _lexicographic_key(row, objectives)
    )
    ordered_all = sorted(
        representatives, key=lambda row: _lexicographic_key(row, objectives)
    )
    primary = ordered_frontier[0]
    backup = next(
        (
            row
            for row in ordered_all
            if row["variant_id"] != primary["variant_id"]
            and row["design_behavior_fingerprint"]
            != primary["design_behavior_fingerprint"]
            and _component_jaccard(row, primary)
            <= backup_maximum_component_jaccard
        ),
        None,
    )
    return SelectionDecision(
        primary=primary,
        backup=backup,
        eligible_count=len(eligible),
        hard_rejection_count=len(rejections),
        behavioral_clone_rejection_count=len(eligible) - len(representatives),
        pareto_frontier_count=len(ordered_frontier),
        pareto_frontier_ids=tuple(
            str(row["variant_id"]) for row in ordered_frontier
        ),
        hard_rejections=tuple(rejections),
        clone_groups=tuple(clone_groups),
        deterministic_order=tuple(str(row["variant_id"]) for row in ordered_all),
    )


def pareto_frontier(
    records: Sequence[Mapping[str, Any]],
    objectives: Sequence[ParetoObjective],
) -> tuple[dict[str, Any], ...]:
    normalized = [_normalize_record(row, objectives) for row in records]
    return tuple(
        row
        for row in normalized
        if not any(
            _dominates(other, row, objectives)
            for other in normalized
            if other["variant_id"] != row["variant_id"]
        )
    )


def _normalize_record(
    record: Mapping[str, Any], objectives: Sequence[ParetoObjective]
) -> dict[str, Any]:
    row = dict(record)
    required = {
        "variant_id",
        "policy_id",
        "component_ids",
        "design_behavior_fingerprint",
        "normal_net_usd",
        "stressed_net_usd",
        "mll_breach_rate",
        "hard_issue_count",
        "maximum_component_profit_share",
    } | {objective.field for objective in objectives}
    missing = sorted(required - set(row))
    if missing:
        raise SelectorError(f"selector record is incomplete: {missing}")
    if not isinstance(row["component_ids"], (list, tuple)) or not row[
        "component_ids"
    ]:
        raise SelectorError("selector record needs frozen component membership")
    for objective in objectives:
        try:
            row[objective.field] = float(row[objective.field])
        except (TypeError, ValueError) as exc:
            raise SelectorError(
                f"objective {objective.field} is not numeric"
            ) from exc
    return row


def _hard_rejection_reasons(
    row: Mapping[str, Any],
    *,
    maximum_mll_breach_rate: float,
    maximum_component_profit_share: float,
) -> list[str]:
    reasons: list[str] = []
    if float(row["normal_net_usd"]) <= 0.0:
        reasons.append("NORMAL_NET_NONPOSITIVE")
    if float(row["stressed_net_usd"]) <= 0.0:
        reasons.append("STRESSED_NET_NONPOSITIVE")
    if int(row["hard_issue_count"]) != 0:
        reasons.append("HARD_EXECUTION_OR_INTEGRITY_ISSUE")
    if float(row["mll_breach_rate"]) > maximum_mll_breach_rate:
        reasons.append("MLL_TOLERANCE_EXCEEDED")
    if (
        float(row["maximum_component_profit_share"])
        > maximum_component_profit_share
    ):
        reasons.append("COMPONENT_DOMINATION")
    return reasons


def _dominates(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    objectives: Sequence[ParetoObjective],
) -> bool:
    weakly_better = True
    strictly_better = False
    for objective in objectives:
        a = float(left[objective.field])
        b = float(right[objective.field])
        if objective.direction == "maximize":
            weakly_better &= a >= b
            strictly_better |= a > b
        else:
            weakly_better &= a <= b
            strictly_better |= a < b
        if not weakly_better:
            return False
    return strictly_better


def _lexicographic_key(
    row: Mapping[str, Any], objectives: Sequence[ParetoObjective]
) -> tuple[Any, ...]:
    values: list[Any] = []
    for objective in objectives:
        value = float(row[objective.field])
        values.append(-value if objective.direction == "maximize" else value)
    values.append(str(row["variant_id"]))
    return tuple(values)


def _component_jaccard(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    a = set(str(value) for value in left["component_ids"])
    b = set(str(value) for value in right["component_ids"])
    return len(a & b) / max(1, len(a | b))


__all__ = [
    "ParetoObjective",
    "SelectionDecision",
    "SelectorError",
    "pareto_frontier",
    "select_pareto_champion",
]
