from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    EconomicRole,
    SleeveSpec,
    deterministic_id,
)


ALLOCATION_PROFILES = (
    (1, 1, 1, 1),
    (2, 1, 1, 1),
    (1, 2, 1, 1),
    (2, 2, 1, 1),
    (3, 1, 1, 1),
    (1, 1, 2, 1),
)
CONFLICT_POLICIES = (
    "FIXED_PRIORITY",
    "LOWEST_CORRELATION_FIRST",
    "LOWEST_MLL_USAGE_FIRST",
)
ASSEMBLY_METHODS = (
    "BEAM_COMPATIBILITY_SEARCH",
    "EVOLUTIONARY_COMPATIBLE_ASSEMBLY",
    "PORTFOLIO_ROLE_SYNTHESIS",
)


@dataclass(frozen=True, slots=True)
class AssemblyInput:
    sleeve: SleeveSpec
    behavioral_cluster: str
    priority_score: float
    cost_per_opportunity: float
    approximate_event_count: int
    hard_invalidated: bool = False

    def __post_init__(self) -> None:
        if not self.behavioral_cluster:
            raise ValueError("behavioral_cluster must be non-empty")
        if not -1.0 <= self.priority_score <= 1.0:
            raise ValueError("priority score must be in [-1,1]")
        if self.cost_per_opportunity < 0.0:
            raise ValueError("cost cannot be negative")
        if self.approximate_event_count < 0:
            raise ValueError("event count cannot be negative")


@dataclass(frozen=True, slots=True)
class AssemblyPopulation:
    campaign_id: str
    requested_count: int
    policies: tuple[AccountPolicyGenome, ...]
    rejected_duplicate_count: int
    rejected_incompatible_count: int
    methods: dict[str, int]

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "requested_count": self.requested_count,
            "policy_count": len(self.policies),
            "rejected_duplicate_count": self.rejected_duplicate_count,
            "rejected_incompatible_count": self.rejected_incompatible_count,
            "methods": dict(sorted(self.methods.items())),
        }


def generate_account_policy_population(
    candidates: Sequence[AssemblyInput],
    *,
    campaign_id: str,
    count: int,
) -> AssemblyPopulation:
    """Generate bounded account policies using three complementary methods."""

    eligible = tuple(
        sorted(
            (row for row in candidates if not row.hard_invalidated),
            key=lambda row: (-row.priority_score, row.sleeve.sleeve_id),
        )
    )
    if len(eligible) < 2:
        raise ValueError("account assembly requires at least two eligible sleeves")
    if count < 1:
        raise ValueError("policy count must be positive")
    policies: list[AccountPolicyGenome] = []
    seen: set[str] = set()
    duplicate = 0
    incompatible = 0
    methods: dict[str, int] = {}
    maximum_attempts = max(count * 80, 1_000)
    for attempt in range(maximum_attempts):
        if len(policies) >= count:
            break
        method = ASSEMBLY_METHODS[attempt % len(ASSEMBLY_METHODS)]
        size = 2 + (_choice(campaign_id, attempt, "size") % min(3, len(eligible) - 1))
        selected = _select(method, eligible, size=size, campaign_id=campaign_id, attempt=attempt)
        if len(selected) != size or not _compatible(selected):
            incompatible += 1
            continue
        profile = ALLOCATION_PROFILES[
            _choice(campaign_id, attempt, "allocation") % len(ALLOCATION_PROFILES)
        ][:size]
        conflict = CONFLICT_POLICIES[
            _choice(campaign_id, attempt, "conflict") % len(CONFLICT_POLICIES)
        ]
        maximum_positions = 1 + (
            _choice(campaign_id, attempt, "positions") % min(size, 3)
        )
        payload = {
            "method": method,
            "sleeves": [row.sleeve.sleeve_id for row in selected],
            "allocation": profile,
            "maximum_positions": maximum_positions,
            "conflict": conflict,
            "daily_risk": (750.0, 1_250.0, 1_750.0)[
                _choice(campaign_id, attempt, "daily_risk") % 3
            ],
            "daily_lock": (1_500.0, 2_250.0, 3_000.0)[
                _choice(campaign_id, attempt, "daily_lock") % 3
            ],
            "low_buffer": (2_250.0, 3_000.0, 3_750.0)[
                _choice(campaign_id, attempt, "low_buffer") % 3
            ],
            "critical_buffer": (750.0, 1_125.0, 1_500.0)[
                _choice(campaign_id, attempt, "critical_buffer") % 3
            ],
            "loss_streak": (2, 3, 4)[
                _choice(campaign_id, attempt, "loss_streak") % 3
            ],
            "campaign": campaign_id,
        }
        if payload["critical_buffer"] > payload["low_buffer"]:
            incompatible += 1
            continue
        policy = AccountPolicyGenome(
            policy_id=deterministic_id("account_policy", payload),
            sleeve_ids=tuple(row.sleeve.sleeve_id for row in selected),
            allocation_units=tuple(int(value) for value in profile),
            maximum_simultaneous_positions=maximum_positions,
            maximum_mini_equivalent=min(15, sum(profile) * 2),
            conflict_policy=conflict,
            daily_risk_budget=float(payload["daily_risk"]),
            daily_profit_lock=float(payload["daily_lock"]),
            low_mll_buffer=float(payload["low_buffer"]),
            critical_mll_buffer=float(payload["critical_buffer"]),
            loss_streak_throttle_after=int(payload["loss_streak"]),
            mode="COMBINE_RESEARCH",
            source_campaign=campaign_id,
        )
        if policy.structural_fingerprint in seen:
            duplicate += 1
            continue
        seen.add(policy.structural_fingerprint)
        policies.append(policy)
        methods[method] = methods.get(method, 0) + 1
    return AssemblyPopulation(
        campaign_id=campaign_id,
        requested_count=count,
        policies=tuple(policies),
        rejected_duplicate_count=duplicate,
        rejected_incompatible_count=incompatible,
        methods=methods,
    )


def _select(
    method: str,
    candidates: Sequence[AssemblyInput],
    *,
    size: int,
    campaign_id: str,
    attempt: int,
) -> tuple[AssemblyInput, ...]:
    if method == "BEAM_COMPATIBILITY_SEARCH":
        start = _choice(campaign_id, attempt, "beam") % len(candidates)
        ordered = tuple(candidates[start:]) + tuple(candidates[:start])
        return _greedy_distinct(ordered, size)
    if method == "PORTFOLIO_ROLE_SYNTHESIS":
        by_role: dict[EconomicRole, list[AssemblyInput]] = {}
        for row in candidates:
            by_role.setdefault(row.sleeve.role, []).append(row)
        roles = sorted(by_role, key=lambda value: value.value)
        start = _choice(campaign_id, attempt, "role") % len(roles)
        selected: list[AssemblyInput] = []
        for offset in range(len(roles)):
            role = roles[(start + offset) % len(roles)]
            choices = by_role[role]
            candidate = choices[
                _choice(campaign_id, attempt + offset, role.value) % len(choices)
            ]
            if _can_add(selected, candidate):
                selected.append(candidate)
            if len(selected) == size:
                break
        return tuple(selected)
    ranked = sorted(
        candidates,
        key=lambda row: (
            _choice(campaign_id, attempt, row.sleeve.sleeve_id),
            row.sleeve.sleeve_id,
        ),
    )
    return _greedy_distinct(ranked, size)


def _greedy_distinct(
    candidates: Iterable[AssemblyInput], size: int
) -> tuple[AssemblyInput, ...]:
    selected: list[AssemblyInput] = []
    for candidate in candidates:
        if _can_add(selected, candidate):
            selected.append(candidate)
        if len(selected) == size:
            break
    return tuple(selected)


def _can_add(selected: Sequence[AssemblyInput], candidate: AssemblyInput) -> bool:
    if any(row.sleeve.sleeve_id == candidate.sleeve.sleeve_id for row in selected):
        return False
    if any(row.behavioral_cluster == candidate.behavioral_cluster for row in selected):
        return False
    same_market = sum(row.sleeve.market == candidate.sleeve.market for row in selected)
    if same_market >= 2:
        return False
    return True


def _compatible(selected: Sequence[AssemblyInput]) -> bool:
    if len(selected) < 2:
        return False
    if len({row.behavioral_cluster for row in selected}) != len(selected):
        return False
    roles = {row.sleeve.role for row in selected}
    if len(roles) == 1 and len({row.sleeve.market for row in selected}) == 1:
        return False
    cost = sum(row.cost_per_opportunity for row in selected)
    return cost <= 100.0


def _choice(campaign_id: str, index: int, name: str) -> int:
    digest = hashlib.sha256(f"{campaign_id}|{index}|{name}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


__all__ = [
    "ALLOCATION_PROFILES",
    "ASSEMBLY_METHODS",
    "AssemblyInput",
    "AssemblyPopulation",
    "generate_account_policy_population",
]
