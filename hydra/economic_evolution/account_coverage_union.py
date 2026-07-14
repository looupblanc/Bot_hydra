from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.role_aware_account import (
    RoleAwareComponent,
    _select_components,
)
from hydra.economic_evolution.schema import deterministic_id, stable_hash


COVERAGE_UNION_CLASS_ID = "CROSS_MARKET_SESSION_COVERAGE_UNION_V1"
COVERAGE_UNION_HYPOTHESIS = (
    "A static union of ten to twelve positive-cost-resilient sleeves spanning "
    "distinct market/session cells should raise accepted opportunity density "
    "and target velocity over event-count-matched concentrated unions without "
    "increasing per-signal risk."
)
COVERAGE_UNION_LIMITS: dict[str, Any] = {
    "daily_loss_guard": 1_000.0,
    "daily_profit_lock": 2_250.0,
    "critical_buffer": 750.0,
    "maximum_simultaneous_positions": 3,
    "maximum_mini_equivalent": 15,
    "risk_units": 1,
}


@dataclass(frozen=True, slots=True)
class CoverageUnionPolicy:
    policy_id: str
    component_ids: tuple[str, ...]
    daily_loss_guard: float
    daily_profit_lock: float
    critical_buffer: float
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    risk_units: int
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("coverage-union policy ID is required")
        if not 10 <= len(self.component_ids) <= 12:
            raise ValueError("coverage union requires ten to twelve sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("coverage-union sleeves must be unique")
        if self.daily_loss_guard != COVERAGE_UNION_LIMITS["daily_loss_guard"]:
            raise ValueError("coverage-union daily loss guard drift")
        if self.daily_profit_lock != COVERAGE_UNION_LIMITS["daily_profit_lock"]:
            raise ValueError("coverage-union daily profit lock drift")
        if self.critical_buffer != COVERAGE_UNION_LIMITS["critical_buffer"]:
            raise ValueError("coverage-union critical buffer drift")
        if self.maximum_simultaneous_positions != 3:
            raise ValueError("coverage-union concurrency drift")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("coverage-union contract limit is invalid")
        if self.risk_units != 1:
            raise ValueError("coverage union cannot increase per-signal risk")
        if self.version != 1 or self.inherited_status is not None:
            raise ValueError("coverage-union candidates cannot inherit status")

    @property
    def controller_id(self) -> str:
        return self.policy_id

    @property
    def basket_policy_id(self) -> str:
        return f"{self.policy_id}::BASKET"

    @property
    def component_priority(self) -> tuple[str, ...]:
        return self.component_ids

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.ADAPTIVE_CONTROLLER

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload())

    def structural_payload(self) -> dict[str, Any]:
        return {
            "schema": "hydra_coverage_union_policy_v1",
            "component_ids": list(self.component_ids),
            **dict(COVERAGE_UNION_LIMITS),
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["kind"] = self.kind.value
        row["structural_fingerprint"] = self.structural_fingerprint
        return row


@dataclass(frozen=True, slots=True)
class CoverageUnionPolicyPair:
    pair_id: str
    real_policy: CoverageUnionPolicy
    matched_control_policy: CoverageUnionPolicy
    real_market_count: int
    real_session_count: int
    real_market_session_cell_count: int
    control_market_count: int
    control_session_count: int
    control_market_session_cell_count: int
    real_source_event_count: int
    control_source_event_count: int

    def __post_init__(self) -> None:
        if len(self.real_policy.component_ids) != len(
            self.matched_control_policy.component_ids
        ):
            raise ValueError("coverage pair must retain union size")
        if set(self.real_policy.component_ids) == set(
            self.matched_control_policy.component_ids
        ):
            raise ValueError("coverage control membership must differ")
        if self.real_market_count < 4 or self.real_session_count < 4:
            raise ValueError("real coverage union lacks market/session breadth")
        if self.control_market_count > 2:
            raise ValueError("coverage control is not market concentrated")
        denominator = max(self.real_source_event_count, 1)
        if abs(self.real_source_event_count - self.control_source_event_count) / denominator > 0.15:
            raise ValueError("coverage control event count is not matched")
        if _limits(self.real_policy) != _limits(self.matched_control_policy):
            raise ValueError("coverage pair account limits differ")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "union_size": len(self.real_policy.component_ids),
            "real_market_count": self.real_market_count,
            "real_session_count": self.real_session_count,
            "real_market_session_cell_count": self.real_market_session_cell_count,
            "control_market_count": self.control_market_count,
            "control_session_count": self.control_session_count,
            "control_market_session_cell_count": self.control_market_session_cell_count,
            "real_source_event_count": self.real_source_event_count,
            "control_source_event_count": self.control_source_event_count,
            "source_event_count_relative_delta": abs(
                self.real_source_event_count - self.control_source_event_count
            )
            / max(self.real_source_event_count, 1),
            "same_union_size": True,
            "same_account_limits": True,
            "same_per_signal_risk": True,
            "different_membership": True,
        }


@dataclass(frozen=True, slots=True)
class CoverageUnionPopulation:
    campaign_id: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[CoverageUnionPolicyPair, ...]
    duplicate_rejection_count: int
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[CoverageUnionPolicy, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[CoverageUnionPolicy, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": COVERAGE_UNION_CLASS_ID,
            "component_count": len(self.components),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "structurally_distinct_policy_count": len(
                {row.real_policy.structural_fingerprint for row in self.pairs}
            ),
            "markets": sorted({row.sleeve.market for row in self.components}),
            "sessions": sorted(
                {row.sleeve.session_code for row in self.components}
            ),
            "union_sizes": _counts(
                len(row.real_policy.component_ids) for row in self.pairs
            ),
            "event_count_matched_pair_count": len(self.pairs),
            "real_minimum_market_count": min(
                row.real_market_count for row in self.pairs
            ),
            "real_minimum_session_count": min(
                row.real_session_count for row in self.pairs
            ),
            "control_maximum_market_count": max(
                row.control_market_count for row in self.pairs
            ),
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "manifest_hash": self.manifest_hash,
            "per_signal_risk_units": 1,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def route_coverage_union_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: CoverageUnionPolicy,
) -> RoutingDecision:
    if intent.component_id not in set(policy.component_ids):
        return _blocked(policy, "COMPONENT_NOT_IN_FROZEN_UNION")
    if state.daily_realized_pnl <= -policy.daily_loss_guard:
        return _blocked(policy, "DAILY_LOSS_GUARD")
    if state.daily_realized_pnl >= policy.daily_profit_lock:
        return _blocked(policy, "DAILY_PROFIT_LOCK")
    if state.mll_buffer <= policy.critical_buffer:
        return _blocked(policy, "CRITICAL_MLL_BUFFER")
    if len(state.open_exposures) >= policy.maximum_simultaneous_positions:
        return _blocked(policy, "MAXIMUM_SIMULTANEOUS_POSITIONS")
    if any(
        row.market == intent.market and row.exit_ns > intent.decision_ns
        for row in state.open_exposures
    ):
        return _blocked(policy, "SAME_MARKET_CONFLICT")
    quantity = int(intent.base_quantity * policy.risk_units)
    mini = float(intent.base_mini_equivalent * policy.risk_units)
    current = sum(row.mini_equivalent for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        True,
        quantity,
        mini,
        "STATIC_COVERAGE_UNION_ACCEPT",
        policy.policy_id,
    )


def generate_coverage_union_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
) -> CoverageUnionPopulation:
    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if policy_pair_count < 64:
        raise ValueError("coverage-union synthesis requires at least 64 pairs")
    if maximum_components < 24:
        raise ValueError("coverage-union component bank is too small")
    if seed_archive.get("development_only") is not True:
        raise ValueError("coverage union requires a development seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive coverage unions")
    if (seed_archive.get("governance") or {}).get("status_inheritance") is not False:
        raise ValueError("coverage-union seed status inheritance is forbidden")

    components = _select_components(
        seed_archive,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )
    if len(components) < 24:
        raise ValueError("insufficient positive-net coverage components")
    pairs, duplicates = _generate_pairs(
        components,
        campaign_id=campaign_id,
        count=policy_pair_count,
    )
    payload = {
        "schema": "hydra_coverage_union_population_v1",
        "campaign_id": campaign_id,
        "class_id": COVERAGE_UNION_CLASS_ID,
        "component_behavioral_fingerprints": [
            row.sleeve.behavioral_fingerprint for row in components
        ],
        "pairs": [
            {
                "pair_id": row.pair_id,
                "real": row.real_policy.structural_fingerprint,
                "control": row.matched_control_policy.structural_fingerprint,
                "real_events": row.real_source_event_count,
                "control_events": row.control_source_event_count,
            }
            for row in pairs
        ],
        "limits": dict(COVERAGE_UNION_LIMITS),
        "real_minimum_markets": 4,
        "real_minimum_sessions": 4,
        "control_maximum_markets": 2,
        "maximum_event_count_relative_delta": 0.15,
        "same_union_size": True,
        "same_account_limits": True,
        "same_per_signal_risk": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return CoverageUnionPopulation(
        campaign_id=campaign_id,
        components=components,
        pairs=pairs,
        duplicate_rejection_count=duplicates,
        manifest_hash=stable_hash(payload),
    )


def _generate_pairs(
    components: Sequence[RoleAwareComponent],
    *,
    campaign_id: str,
    count: int,
) -> tuple[tuple[CoverageUnionPolicyPair, ...], int]:
    pairs: list[CoverageUnionPolicyPair] = []
    seen_real: set[str] = set()
    seen_control: set[str] = set()
    duplicates = 0
    markets = sorted({row.sleeve.market for row in components})
    for attempt in range(max(count * 300, 50_000)):
        if len(pairs) == count:
            break
        size = 10 + attempt % 3
        real_members = _diverse_membership(
            components,
            size=size,
            campaign_id=campaign_id,
            attempt=attempt,
        )
        real_ids = tuple(row.sleeve.sleeve_id for row in real_members)
        real_key = stable_hash(sorted(real_ids))
        if real_key in seen_real:
            duplicates += 1
            continue
        real_events = sum(row.event_count for row in real_members)
        control_members = _matched_concentrated_membership(
            components,
            size=size,
            campaign_id=campaign_id,
            attempt=attempt,
            target_events=real_events,
            markets=markets,
        )
        if control_members is None:
            continue
        control_ids = tuple(row.sleeve.sleeve_id for row in control_members)
        control_key = stable_hash(sorted(control_ids))
        if control_key in seen_control or set(control_ids) == set(real_ids):
            duplicates += 1
            continue
        real = _policy(
            real_ids,
            policy_id=deterministic_id(
                "coverage_union_real",
                {
                    "campaign": campaign_id,
                    "class": COVERAGE_UNION_CLASS_ID,
                    "membership": real_key,
                    "limits": COVERAGE_UNION_LIMITS,
                },
            ),
        )
        control = _policy(
            control_ids,
            policy_id=deterministic_id(
                "coverage_union_control",
                {
                    "campaign": campaign_id,
                    "class": COVERAGE_UNION_CLASS_ID,
                    "membership": control_key,
                    "limits": COVERAGE_UNION_LIMITS,
                },
            ),
        )
        pair = CoverageUnionPolicyPair(
            pair_id=deterministic_id(
                "coverage_union_pair",
                {"campaign": campaign_id, "real": real_key, "control": control_key},
            ),
            real_policy=real,
            matched_control_policy=control,
            real_market_count=_market_count(real_members),
            real_session_count=_session_count(real_members),
            real_market_session_cell_count=_cell_count(real_members),
            control_market_count=_market_count(control_members),
            control_session_count=_session_count(control_members),
            control_market_session_cell_count=_cell_count(control_members),
            real_source_event_count=real_events,
            control_source_event_count=sum(row.event_count for row in control_members),
        )
        pairs.append(pair)
        seen_real.add(real_key)
        seen_control.add(control_key)
    if len(pairs) != count:
        raise RuntimeError(f"generated {len(pairs)} of {count} coverage pairs")
    return tuple(sorted(pairs, key=lambda row: row.pair_id)), duplicates


def _diverse_membership(
    components: Sequence[RoleAwareComponent],
    *,
    size: int,
    campaign_id: str,
    attempt: int,
) -> tuple[RoleAwareComponent, ...]:
    remaining = list(components)
    selected: list[RoleAwareComponent] = []
    while remaining and len(selected) < size:
        markets = {row.sleeve.market for row in selected}
        sessions = {row.sleeve.session_code for row in selected}
        cells = {(row.sleeve.market, row.sleeve.session_code) for row in selected}
        remaining.sort(
            key=lambda row: (
                (row.sleeve.market, row.sleeve.session_code) in cells,
                row.sleeve.market in markets,
                row.sleeve.session_code in sessions,
                stable_hash(
                    [campaign_id, "diverse", attempt, row.sleeve.sleeve_id]
                ),
                row.sleeve.sleeve_id,
            )
        )
        selected.append(remaining.pop(0))
    return tuple(selected)


def _matched_concentrated_membership(
    components: Sequence[RoleAwareComponent],
    *,
    size: int,
    campaign_id: str,
    attempt: int,
    target_events: int,
    markets: Sequence[str],
) -> tuple[RoleAwareComponent, ...] | None:
    candidates: list[tuple[float, str, tuple[RoleAwareComponent, ...]]] = []
    for offset in range(len(markets)):
        first = markets[(attempt + offset) % len(markets)]
        for second in markets:
            if second == first:
                continue
            pool = [
                row
                for row in components
                if row.sleeve.market in {first, second}
            ]
            if len(pool) < size:
                continue
            for salt in range(12):
                ranked = sorted(
                    pool,
                    key=lambda row: (
                        stable_hash(
                            [
                                campaign_id,
                                "concentrated_control",
                                attempt,
                                offset,
                                salt,
                                row.sleeve.sleeve_id,
                            ]
                        ),
                        row.sleeve.sleeve_id,
                    ),
                )
                selected = tuple(ranked[:size])
                events = sum(row.event_count for row in selected)
                delta = abs(events - target_events) / max(target_events, 1)
                candidates.append(
                    (delta, stable_hash(sorted(row.sleeve.sleeve_id for row in selected)), selected)
                )
    if not candidates:
        return None
    delta, _fingerprint, best = min(candidates, key=lambda row: (row[0], row[1]))
    return best if delta <= 0.15 else None


def _policy(
    component_ids: tuple[str, ...], *, policy_id: str
) -> CoverageUnionPolicy:
    return CoverageUnionPolicy(
        policy_id=policy_id,
        component_ids=component_ids,
        daily_loss_guard=float(COVERAGE_UNION_LIMITS["daily_loss_guard"]),
        daily_profit_lock=float(COVERAGE_UNION_LIMITS["daily_profit_lock"]),
        critical_buffer=float(COVERAGE_UNION_LIMITS["critical_buffer"]),
        maximum_simultaneous_positions=int(
            COVERAGE_UNION_LIMITS["maximum_simultaneous_positions"]
        ),
        maximum_mini_equivalent=int(
            COVERAGE_UNION_LIMITS["maximum_mini_equivalent"]
        ),
        risk_units=int(COVERAGE_UNION_LIMITS["risk_units"]),
    )


def _limits(policy: CoverageUnionPolicy) -> tuple[Any, ...]:
    return (
        policy.daily_loss_guard,
        policy.daily_profit_lock,
        policy.critical_buffer,
        policy.maximum_simultaneous_positions,
        policy.maximum_mini_equivalent,
        policy.risk_units,
    )


def _market_count(rows: Sequence[RoleAwareComponent]) -> int:
    return len({row.sleeve.market for row in rows})


def _session_count(rows: Sequence[RoleAwareComponent]) -> int:
    return len({row.sleeve.session_code for row in rows})


def _cell_count(rows: Sequence[RoleAwareComponent]) -> int:
    return len({(row.sleeve.market, row.sleeve.session_code) for row in rows})


def _blocked(policy: CoverageUnionPolicy, reason: str) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.policy_id)


def _counts(values: Sequence[Any] | Any) -> dict[str, int]:
    output: dict[str, int] = {}
    for value in values:
        key = str(value)
        output[key] = output.get(key, 0) + 1
    return dict(sorted(output.items()))


__all__ = [
    "COVERAGE_UNION_CLASS_ID",
    "COVERAGE_UNION_HYPOTHESIS",
    "COVERAGE_UNION_LIMITS",
    "CoverageUnionPolicy",
    "CoverageUnionPolicyPair",
    "CoverageUnionPopulation",
    "generate_coverage_union_population",
    "route_coverage_union_entry",
]
