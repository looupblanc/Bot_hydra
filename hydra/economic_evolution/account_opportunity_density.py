from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.economic_evolution.role_aware_account import (
    RoleAwareComponent,
    _eligible_membership,
    _select_components,
)
from hydra.economic_evolution.schema import deterministic_id, stable_hash


OPPORTUNITY_DENSITY_CLASS_ID = (
    "CONTEMPORANEOUS_CROSS_MARKET_OPPORTUNITY_DENSITY_V1"
)
OPPORTUNITY_DENSITY_HYPOTHESIS = (
    "Same-direction opportunities on different futures markets observed in the "
    "preceding hour identify a shared participation state in which two bounded "
    "risk units should improve target velocity over a degree-matched source-"
    "graph permutation without unacceptable MLL or consistency deterioration."
)
OPPORTUNITY_DENSITY_LIMITS: dict[str, Any] = {
    "trailing_signal_window_ns": 60 * 60 * 1_000_000_000,
    "minimum_confirming_different_markets": 1,
    "base_risk_units": 1,
    "confirmed_risk_units": 2,
    "daily_loss_guard": 1_000.0,
    "daily_profit_lock": 2_250.0,
    "critical_buffer": 750.0,
    "maximum_simultaneous_positions": 3,
    "maximum_mini_equivalent": 15,
}


@dataclass(frozen=True, slots=True)
class SignalObservation:
    component_id: str
    market: str
    side: int
    decision_ns: int

    def __post_init__(self) -> None:
        if not self.component_id or not self.market:
            raise ValueError("signal observation identity is required")
        if self.side not in {-1, 1}:
            raise ValueError("signal observation side must be -1 or 1")
        if self.decision_ns < 0:
            raise ValueError("signal observation timestamp must be nonnegative")


@dataclass(frozen=True, slots=True)
class OpportunityDensityPolicy:
    policy_id: str
    component_ids: tuple[str, ...]
    confirmation_sources: tuple[tuple[str, tuple[str, ...]], ...]
    trailing_signal_window_ns: int
    minimum_confirming_different_markets: int
    base_risk_units: int
    confirmed_risk_units: int
    daily_loss_guard: float
    daily_profit_lock: float
    critical_buffer: float
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("opportunity-density policy ID is required")
        if not 6 <= len(self.component_ids) <= 8:
            raise ValueError("opportunity-density policy requires six to eight sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("opportunity-density sleeves must be unique")
        graph = self.source_graph
        if set(graph) != set(self.component_ids):
            raise ValueError("confirmation graph must cover every sleeve")
        if any(not values for values in graph.values()):
            raise ValueError("every sleeve requires a confirmation source")
        if any(
            value not in set(self.component_ids)
            for values in graph.values()
            for value in values
        ):
            raise ValueError("confirmation graph references an absent sleeve")
        if self.trailing_signal_window_ns != OPPORTUNITY_DENSITY_LIMITS[
            "trailing_signal_window_ns"
        ]:
            raise ValueError("signal window is outside the frozen mechanism")
        if self.minimum_confirming_different_markets != 1:
            raise ValueError("confirmation count is outside the frozen mechanism")
        if (self.base_risk_units, self.confirmed_risk_units) != (1, 2):
            raise ValueError("risk units are outside the frozen mechanism")
        if not 1 <= self.maximum_simultaneous_positions <= 3:
            raise ValueError("opportunity-density concurrency is invalid")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("maximum mini equivalent must be in [1,15]")
        if not 0.0 < self.critical_buffer <= 4_500.0:
            raise ValueError("critical MLL buffer is invalid")
        if self.version != 1 or self.inherited_status is not None:
            raise ValueError("opportunity-density candidates cannot inherit status")

    @property
    def basket_policy_id(self) -> str:
        return f"{self.policy_id}::BASKET"

    @property
    def source_graph(self) -> dict[str, tuple[str, ...]]:
        return {str(key): tuple(values) for key, values in self.confirmation_sources}

    @property
    def source_degree_multiset(self) -> tuple[int, ...]:
        return tuple(sorted(len(values) for values in self.source_graph.values()))

    @property
    def source_id_multiset(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                value
                for values in self.source_graph.values()
                for value in values
            )
        )

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload())

    def structural_payload(self) -> dict[str, Any]:
        return {
            "schema": "hydra_opportunity_density_policy_v1",
            "component_ids": list(self.component_ids),
            "confirmation_sources": [
                [key, list(values)] for key, values in self.confirmation_sources
            ],
            **{
                key: value
                for key, value in OPPORTUNITY_DENSITY_LIMITS.items()
            },
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["confirmation_sources"] = [
            [key, list(values)] for key, values in self.confirmation_sources
        ]
        row["source_degree_multiset"] = list(self.source_degree_multiset)
        row["source_id_multiset"] = list(self.source_id_multiset)
        row["structural_fingerprint"] = self.structural_fingerprint
        return row


@dataclass(frozen=True, slots=True)
class OpportunityDensityPolicyPair:
    pair_id: str
    real_policy: OpportunityDensityPolicy
    matched_control_policy: OpportunityDensityPolicy
    membership_hash: str

    def __post_init__(self) -> None:
        if self.real_policy.component_ids != self.matched_control_policy.component_ids:
            raise ValueError("density pair must retain ordered sleeve membership")
        if (
            self.real_policy.source_degree_multiset
            != self.matched_control_policy.source_degree_multiset
            or self.real_policy.source_id_multiset
            != self.matched_control_policy.source_id_multiset
        ):
            raise ValueError("density control must retain graph degree and source multisets")
        if (
            self.real_policy.confirmation_sources
            == self.matched_control_policy.confirmation_sources
        ):
            raise ValueError("density control graph must be distinct")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "membership_hash": self.membership_hash,
            "identical_ordered_membership": True,
            "identical_component_event_paths": True,
            "same_confirmation_graph_degree_multiset": True,
            "same_confirmation_source_id_multiset": True,
            "same_account_limits": True,
        }


@dataclass(frozen=True, slots=True)
class OpportunityDensityPopulation:
    campaign_id: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[OpportunityDensityPolicyPair, ...]
    duplicate_rejection_count: int
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[OpportunityDensityPolicy, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[OpportunityDensityPolicy, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": OPPORTUNITY_DENSITY_CLASS_ID,
            "component_count": len(self.components),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "markets": sorted({row.sleeve.market for row in self.components}),
            "sessions": sorted(
                {row.sleeve.session_code for row in self.components}
            ),
            "sleeve_counts": _counts(
                len(row.real_policy.component_ids) for row in self.pairs
            ),
            "same_ordered_membership_pair_count": len(self.pairs),
            "same_graph_degree_multiset_pair_count": len(self.pairs),
            "same_graph_source_multiset_pair_count": len(self.pairs),
            "unique_membership_count": len(
                {row.membership_hash for row in self.pairs}
            ),
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "manifest_hash": self.manifest_hash,
            "past_only_signal_observations": True,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def route_opportunity_density_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: OpportunityDensityPolicy,
    signal_histories: Mapping[str, Sequence[SignalObservation]],
) -> RoutingDecision:
    if intent.component_id not in policy.source_graph:
        return _blocked(policy, "COMPONENT_NOT_IN_FROZEN_MEMBERSHIP")
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

    lower = intent.decision_ns - policy.trailing_signal_window_ns
    confirming_markets: set[str] = set()
    for source_id in policy.source_graph[intent.component_id]:
        history = signal_histories.get(source_id, ())
        if not history:
            continue
        observation = history[-1]
        if observation.decision_ns > intent.decision_ns:
            raise ValueError("future opportunity observation reached router")
        if (
            observation.decision_ns >= lower
            and observation.side == intent.side
            and observation.market != intent.market
        ):
            confirming_markets.add(observation.market)
    confirmed = (
        len(confirming_markets)
        >= policy.minimum_confirming_different_markets
    )
    units = policy.confirmed_risk_units if confirmed else policy.base_risk_units
    quantity = int(intent.base_quantity * units)
    mini = float(intent.base_mini_equivalent * units)
    current = sum(row.mini_equivalent for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        True,
        quantity,
        mini,
        "CROSS_MARKET_DENSITY_CONFIRMED_SCALE"
        if confirmed
        else "ISOLATED_OPPORTUNITY_BASE_RISK",
        policy.policy_id,
    )


def generate_opportunity_density_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
    minimum_markets: int = 3,
    minimum_sessions: int = 3,
) -> OpportunityDensityPopulation:
    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if policy_pair_count < 64:
        raise ValueError("opportunity-density synthesis requires at least 64 pairs")
    if maximum_components < 16:
        raise ValueError("component bank is too small for density synthesis")
    if seed_archive.get("development_only") is not True:
        raise ValueError("density generation requires a development seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive density synthesis")
    if (seed_archive.get("governance") or {}).get("status_inheritance") is not False:
        raise ValueError("seed status inheritance must be disabled")

    components = _select_components(
        seed_archive,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )
    if len(components) < 16:
        raise ValueError("insufficient positive-net distinct components")
    pairs, duplicate_rejections = _generate_pairs(
        components,
        campaign_id=campaign_id,
        count=policy_pair_count,
        minimum_markets=minimum_markets,
        minimum_sessions=minimum_sessions,
    )
    manifest_payload = {
        "schema": "hydra_opportunity_density_population_v1",
        "campaign_id": campaign_id,
        "class_id": OPPORTUNITY_DENSITY_CLASS_ID,
        "component_behavioral_fingerprints": [
            row.sleeve.behavioral_fingerprint for row in components
        ],
        "pairs": [
            {
                "pair_id": row.pair_id,
                "membership_hash": row.membership_hash,
                "real": row.real_policy.structural_fingerprint,
                "matched_control": (
                    row.matched_control_policy.structural_fingerprint
                ),
            }
            for row in pairs
        ],
        "opportunity_density_limits": dict(OPPORTUNITY_DENSITY_LIMITS),
        "same_ordered_membership_within_pair": True,
        "same_component_event_paths_within_pair": True,
        "same_graph_degree_multiset_within_pair": True,
        "same_graph_source_multiset_within_pair": True,
        "control_difference": (
            "DETERMINISTIC_WITHIN_MEMBERSHIP_CONFIRMATION_SOURCE_GRAPH_PERMUTATION"
        ),
        "past_only_signal_observations": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "same_class_0012_rescue": False,
        "outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return OpportunityDensityPopulation(
        campaign_id=campaign_id,
        components=components,
        pairs=pairs,
        duplicate_rejection_count=duplicate_rejections,
        manifest_hash=stable_hash(manifest_payload),
    )


def _generate_pairs(
    components: Sequence[RoleAwareComponent],
    *,
    campaign_id: str,
    count: int,
    minimum_markets: int,
    minimum_sessions: int,
) -> tuple[tuple[OpportunityDensityPolicyPair, ...], int]:
    pairs: list[OpportunityDensityPolicyPair] = []
    seen_memberships: set[str] = set()
    seen_real: set[str] = set()
    seen_control: set[str] = set()
    duplicates = 0
    for attempt in range(max(count * 120, 30_000)):
        if len(pairs) == count:
            break
        size = 6 + (attempt % 3)
        ranked = sorted(
            components,
            key=lambda row: (
                stable_hash(
                    [campaign_id, "density_membership", attempt, row.sleeve.sleeve_id]
                ),
                row.sleeve.sleeve_id,
            ),
        )
        members = tuple(ranked[:size])
        if not _eligible_membership(
            members,
            minimum_markets=minimum_markets,
            minimum_sessions=minimum_sessions,
            minimum_roles=2,
        ):
            continue
        membership_hash = stable_hash(
            sorted(row.sleeve.behavioral_fingerprint for row in members)
        )
        if membership_hash in seen_memberships:
            duplicates += 1
            continue
        ordered = tuple(
            sorted(
                members,
                key=lambda row: (
                    stable_hash(
                        [campaign_id, membership_hash, "priority", row.sleeve.sleeve_id]
                    ),
                    row.sleeve.sleeve_id,
                ),
            )
        )
        graph = _confirmation_graph(
            ordered, campaign_id=campaign_id, membership_hash=membership_hash
        )
        if graph is None:
            continue
        control_graph = _permuted_graph(
            graph, campaign_id=campaign_id, membership_hash=membership_hash
        )
        component_ids = tuple(row.sleeve.sleeve_id for row in ordered)
        real = _policy(
            component_ids,
            graph,
            policy_id=deterministic_id(
                "opportunity_density_policy",
                {
                    "campaign": campaign_id,
                    "membership": membership_hash,
                    "graph": graph,
                    "limits": OPPORTUNITY_DENSITY_LIMITS,
                },
            ),
        )
        control = _policy(
            component_ids,
            control_graph,
            policy_id=deterministic_id(
                "opportunity_density_control",
                {
                    "campaign": campaign_id,
                    "membership": membership_hash,
                    "graph": control_graph,
                    "limits": OPPORTUNITY_DENSITY_LIMITS,
                },
            ),
        )
        if (
            real.structural_fingerprint in seen_real
            or control.structural_fingerprint in seen_control
            or real.structural_fingerprint == control.structural_fingerprint
        ):
            duplicates += 1
            continue
        pairs.append(
            OpportunityDensityPolicyPair(
                pair_id=deterministic_id(
                    "opportunity_density_pair",
                    {
                        "campaign": campaign_id,
                        "membership": membership_hash,
                        "real": real.structural_fingerprint,
                        "control": control.structural_fingerprint,
                    },
                ),
                real_policy=real,
                matched_control_policy=control,
                membership_hash=membership_hash,
            )
        )
        seen_memberships.add(membership_hash)
        seen_real.add(real.structural_fingerprint)
        seen_control.add(control.structural_fingerprint)
    if len(pairs) != count:
        raise RuntimeError(
            f"only {len(pairs)} distinct opportunity-density pairs for {count} requested"
        )
    return tuple(pairs), duplicates


def _confirmation_graph(
    members: Sequence[RoleAwareComponent],
    *,
    campaign_id: str,
    membership_hash: str,
) -> tuple[tuple[str, tuple[str, ...]], ...] | None:
    output: list[tuple[str, tuple[str, ...]]] = []
    for target in members:
        sources = sorted(
            (
                row
                for row in members
                if row.sleeve.sleeve_id != target.sleeve.sleeve_id
                and row.sleeve.market != target.sleeve.market
                and row.sleeve.side == target.sleeve.side
            ),
            key=lambda row: (
                stable_hash(
                    [
                        campaign_id,
                        membership_hash,
                        target.sleeve.sleeve_id,
                        row.sleeve.sleeve_id,
                    ]
                ),
                row.sleeve.sleeve_id,
            ),
        )
        if not sources:
            return None
        output.append(
            (
                target.sleeve.sleeve_id,
                tuple(row.sleeve.sleeve_id for row in sources[:3]),
            )
        )
    return tuple(output)


def _permuted_graph(
    graph: tuple[tuple[str, tuple[str, ...]], ...],
    *,
    campaign_id: str,
    membership_hash: str,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    targets = tuple(target for target, _values in graph)
    degrees = tuple(len(values) for _target, values in graph)
    flat = tuple(value for _target, values in graph for value in values)
    for attempt in range(512):
        rotated = tuple(
            value
            for _index, value in sorted(
                enumerate(flat),
                key=lambda row: (
                    stable_hash(
                        [
                            campaign_id,
                            membership_hash,
                            "graph_permutation",
                            attempt,
                            row[0],
                            row[1],
                        ]
                    ),
                    row[0],
                ),
            )
        )
        cursor = 0
        rows: list[tuple[str, tuple[str, ...]]] = []
        valid = True
        for target, degree in zip(targets, degrees, strict=True):
            values = tuple(rotated[cursor : cursor + degree])
            cursor += degree
            if len(set(values)) != len(values):
                valid = False
                break
            rows.append((target, values))
        candidate = tuple(rows)
        if valid and candidate != graph:
            return candidate
    raise RuntimeError("unable to construct a distinct degree-matched graph control")


def _policy(
    component_ids: tuple[str, ...],
    graph: tuple[tuple[str, tuple[str, ...]], ...],
    *,
    policy_id: str,
) -> OpportunityDensityPolicy:
    return OpportunityDensityPolicy(
        policy_id=policy_id,
        component_ids=component_ids,
        confirmation_sources=graph,
        **OPPORTUNITY_DENSITY_LIMITS,
    )


def _blocked(policy: OpportunityDensityPolicy, reason: str) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.policy_id)


def _counts(values: Sequence[Any] | Any) -> dict[Any, int]:
    output: dict[Any, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items(), key=lambda row: str(row[0])))


__all__ = [
    "OPPORTUNITY_DENSITY_CLASS_ID",
    "OPPORTUNITY_DENSITY_HYPOTHESIS",
    "OPPORTUNITY_DENSITY_LIMITS",
    "OpportunityDensityPolicy",
    "OpportunityDensityPolicyPair",
    "OpportunityDensityPopulation",
    "SignalObservation",
    "generate_opportunity_density_population",
    "route_opportunity_density_entry",
]
