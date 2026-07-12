from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, slots=True)
class PolicyArchiveEntry:
    policy_id: str
    policy_kind: str
    niche: tuple[str, ...]
    score: float
    pass_rate: float
    mll_breach_rate: float
    target_progress: float
    component_ids: tuple[str, ...]
    payload: Mapping[str, Any]


class AccountPolicyArchive:
    """Small deterministic MAP-Elites archive for account policies."""

    def __init__(self, *, maximum_per_niche: int = 2) -> None:
        if maximum_per_niche < 1:
            raise ValueError("maximum_per_niche must be positive")
        self.maximum_per_niche = maximum_per_niche
        self._entries: dict[tuple[str, ...], list[PolicyArchiveEntry]] = {}

    def insert(self, entry: PolicyArchiveEntry) -> bool:
        values = list(self._entries.get(entry.niche, ()))
        if any(row.policy_id == entry.policy_id for row in values):
            return False
        values.append(entry)
        values.sort(
            key=lambda row: (
                -row.pass_rate,
                row.mll_breach_rate,
                -row.target_progress,
                -row.score,
                row.policy_id,
            )
        )
        accepted = entry in values[: self.maximum_per_niche]
        self._entries[entry.niche] = values[: self.maximum_per_niche]
        return accepted

    @property
    def entries(self) -> tuple[PolicyArchiveEntry, ...]:
        return tuple(
            row
            for niche in sorted(self._entries)
            for row in self._entries[niche]
        )

    def summary(self) -> dict[str, Any]:
        entries = self.entries
        return {
            "niche_count": len(self._entries),
            "entry_count": len(entries),
            "policy_kinds": _counts(row.policy_kind for row in entries),
            "basket_sizes": _counts(str(len(row.component_ids)) for row in entries),
            "maximum_pass_rate": max((row.pass_rate for row in entries), default=0.0),
            "minimum_mll_breach_rate": min(
                (row.mll_breach_rate for row in entries), default=0.0
            ),
        }


def _counts(values: Iterable[str]) -> dict[str, int]:
    output: dict[str, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items()))


__all__ = ["AccountPolicyArchive", "PolicyArchiveEntry"]
