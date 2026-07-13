from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import (
    EconomicRole,
    FailureDimension,
    SleeveSpec,
    deterministic_id,
    stable_hash,
)


ROLE_AWARE_CLASS_ID = "ROLE_AWARE_OPPORTUNITY_POOL_ALLOCATOR_V1"
ROLE_AWARE_HYPOTHESIS = (
    "On identical frozen sleeve membership and account limits, assigning a "
    "bounded risk-unit multiset and conflict priority by preregistered economic "
    "role should improve stressed account expectancy and target progress over "
    "a deterministic within-membership permutation without increasing MLL risk."
)
ROLE_ORDER = (
    EconomicRole.TARGET_ACCELERATOR,
    EconomicRole.PRIMARY_ALPHA,
    EconomicRole.SECONDARY_ALPHA,
    EconomicRole.SESSION_DIVERSIFIER,
    EconomicRole.MARKET_DIVERSIFIER,
    EconomicRole.CONSISTENCY_SMOOTHER,
    EconomicRole.MLL_STABILIZER,
    EconomicRole.DEFENSIVE_SWITCH,
    EconomicRole.XFA_COMPONENT,
    EconomicRole.PAYOUT_STABILIZER,
)
PROFILES = (
    "ROLE_BALANCED_POOL",
    "ROLE_TARGET_VELOCITY_POOL",
    "ROLE_MLL_CONSTRAINED_POOL",
)
_ROLE_RANK = {role: index for index, role in enumerate(ROLE_ORDER)}


@dataclass(frozen=True, slots=True)
class RoleAwareAccountPolicyGenome:
    """Campaign-local genome that preserves every legacy frozen schema."""

    policy_id: str
    sleeve_ids: tuple[str, ...]
    allocation_units: tuple[int, ...]
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    conflict_policy: str
    daily_risk_budget: float
    daily_profit_lock: float
    low_mll_buffer: float
    critical_mll_buffer: float
    loss_streak_throttle_after: int
    mode: str
    source_campaign: str
    parent_policy_ids: tuple[str, ...] = ()
    mutation_target: FailureDimension | None = None
    version: int = 1
    inherited_status: None = None

    def __post_init__(self) -> None:
        if not self.policy_id or not self.source_campaign:
            raise ValueError("policy and campaign IDs are required")
        if not 6 <= len(self.sleeve_ids) <= 8:
            raise ValueError("role-aware policy must contain six to eight sleeves")
        if len(set(self.sleeve_ids)) != len(self.sleeve_ids):
            raise ValueError("role-aware policy sleeves must be unique")
        if len(self.allocation_units) != len(self.sleeve_ids):
            raise ValueError("allocation units must match sleeves")
        if any(value not in {1, 2, 3, 4} for value in self.allocation_units):
            raise ValueError("allocation units must use the bounded discrete set")
        if not 1 <= self.maximum_simultaneous_positions <= len(self.sleeve_ids):
            raise ValueError("maximum simultaneous positions is inconsistent")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("maximum mini equivalent must be in [1,15]")
        if self.conflict_policy != "FIXED_PRIORITY":
            raise ValueError("role-aware exact executor requires fixed priority")
        if not 0.0 < self.daily_risk_budget <= 3_000.0:
            raise ValueError("daily risk budget must be in (0,3000]")
        if not 0.0 < self.daily_profit_lock <= 9_000.0:
            raise ValueError("daily profit lock must be in (0,9000]")
        if not 0.0 < self.critical_mll_buffer <= self.low_mll_buffer <= 4_500.0:
            raise ValueError("MLL buffer thresholds are invalid")
        if self.loss_streak_throttle_after not in {2, 3, 4, 5}:
            raise ValueError("loss streak throttle must use the bounded set")
        if self.mode != "COMBINE_RESEARCH":
            raise ValueError("role-aware campaign is Combine research only")
        if self.parent_policy_ids:
            raise ValueError("role-aware candidates cannot inherit parent status")
        if self.version != 1:
            raise ValueError("role-aware policy version must be frozen at one")

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.structural_payload())

    def structural_payload(self) -> dict[str, Any]:
        return {
            "schema": "hydra_role_aware_account_policy_v1",
            "sleeve_ids": list(self.sleeve_ids),
            "allocation_units": list(self.allocation_units),
            "maximum_simultaneous_positions": self.maximum_simultaneous_positions,
            "maximum_mini_equivalent": self.maximum_mini_equivalent,
            "conflict_policy": self.conflict_policy,
            "daily_risk_budget": float(self.daily_risk_budget).hex(),
            "daily_profit_lock": float(self.daily_profit_lock).hex(),
            "low_mll_buffer": float(self.low_mll_buffer).hex(),
            "critical_mll_buffer": float(self.critical_mll_buffer).hex(),
            "loss_streak_throttle_after": self.loss_streak_throttle_after,
            "mode": self.mode,
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["sleeve_ids"] = list(self.sleeve_ids)
        value["allocation_units"] = list(self.allocation_units)
        value["parent_policy_ids"] = list(self.parent_policy_ids)
        value["mutation_target"] = (
            self.mutation_target.value if self.mutation_target else None
        )
        value["structural_fingerprint"] = self.structural_fingerprint
        return value


@dataclass(frozen=True, slots=True)
class RoleAwareComponent:
    sleeve: SleeveSpec
    net_pnl: float
    stressed_net_pnl: float
    event_count: int
    incremental_status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "sleeve_id": self.sleeve.sleeve_id,
            "behavioral_fingerprint": self.sleeve.behavioral_fingerprint,
            "market": self.sleeve.market,
            "session_code": self.sleeve.session_code,
            "role": self.sleeve.role.value,
            "mechanism": self.sleeve.trigger_feature,
            "net_pnl": self.net_pnl,
            "stress_1_5x_net_pnl": self.stressed_net_pnl,
            "event_count": self.event_count,
            "incremental_status": self.incremental_status,
        }


@dataclass(frozen=True, slots=True)
class RoleAwarePolicyPair:
    pair_id: str
    real_policy: RoleAwareAccountPolicyGenome
    matched_control_policy: RoleAwareAccountPolicyGenome
    profile: str
    membership_hash: str

    def __post_init__(self) -> None:
        if set(self.real_policy.sleeve_ids) != set(
            self.matched_control_policy.sleeve_ids
        ):
            raise ValueError("role-aware pair must keep identical sleeve membership")
        if sorted(self.real_policy.allocation_units) != sorted(
            self.matched_control_policy.allocation_units
        ):
            raise ValueError("role-aware pair must keep the risk-unit multiset")
        if _account_limits(self.real_policy) != _account_limits(
            self.matched_control_policy
        ):
            raise ValueError("role-aware pair must keep identical account limits")
        if self.real_policy.sleeve_ids == self.matched_control_policy.sleeve_ids:
            raise ValueError("matched control must permute frozen priority")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "profile": self.profile,
            "membership_hash": self.membership_hash,
            "identical_sleeve_membership": True,
            "same_total_risk_units": (
                sum(self.real_policy.allocation_units)
                == sum(self.matched_control_policy.allocation_units)
            ),
            "same_risk_unit_multiset": True,
            "identical_account_limits": True,
            "real_priority": list(self.real_policy.sleeve_ids),
            "matched_control_priority": list(
                self.matched_control_policy.sleeve_ids
            ),
            "real_allocation_units": list(self.real_policy.allocation_units),
            "matched_control_allocation_units": list(
                self.matched_control_policy.allocation_units
            ),
        }


@dataclass(frozen=True, slots=True)
class RoleAwareAccountPopulation:
    campaign_id: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[RoleAwarePolicyPair, ...]
    prior_policy_rejection_count: int
    duplicate_rejection_count: int
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[RoleAwareAccountPolicyGenome, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[RoleAwareAccountPolicyGenome, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": ROLE_AWARE_CLASS_ID,
            "component_count": len(self.components),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "markets": sorted({row.sleeve.market for row in self.components}),
            "sessions": sorted(
                {row.sleeve.session_code for row in self.components}
            ),
            "roles": sorted(
                {row.sleeve.role.value for row in self.components}
            ),
            "profiles": _counts(row.profile for row in self.pairs),
            "sleeve_counts": _counts(
                len(row.real_policy.sleeve_ids) for row in self.pairs
            ),
            "same_membership_pair_count": sum(
                set(row.real_policy.sleeve_ids)
                == set(row.matched_control_policy.sleeve_ids)
                for row in self.pairs
            ),
            "same_risk_unit_multiset_pair_count": sum(
                sorted(row.real_policy.allocation_units)
                == sorted(row.matched_control_policy.allocation_units)
                for row in self.pairs
            ),
            "prior_policy_rejection_count": self.prior_policy_rejection_count,
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "manifest_hash": self.manifest_hash,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "validated": False,
        }


def generate_role_aware_account_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
    minimum_markets: int = 3,
    minimum_sessions: int = 3,
    minimum_roles: int = 3,
) -> RoleAwareAccountPopulation:
    """Generate same-membership role-aware policies and permutation controls.

    The generator reads only the frozen development seed.  It never opens
    market features or predecessor outcome files.  Membership, account limits
    and the risk-unit multiset are identical inside each pair; only the frozen
    role mapping versus a deterministic role-blind permutation changes.
    """

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if policy_pair_count < 64:
        raise ValueError("role-aware synthesis requires at least 64 pairs")
    if maximum_components < 16:
        raise ValueError("component bank is too small for role-aware synthesis")
    if seed_archive.get("development_only") is not True:
        raise ValueError("role-aware generation requires a development seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive generation")
    governance = seed_archive.get("governance") or {}
    if governance.get("status_inheritance") is not False:
        raise ValueError("seed status inheritance must be disabled")

    components = _select_components(
        seed_archive,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
    )
    if len(components) < 16:
        raise ValueError("insufficient positive-net distinct components")
    if len({row.sleeve.market for row in components}) < minimum_markets:
        raise ValueError("role-aware synthesis lacks market coverage")
    if len({row.sleeve.session_code for row in components}) < minimum_sessions:
        raise ValueError("role-aware synthesis lacks session coverage")
    if len({row.sleeve.role for row in components}) < minimum_roles:
        raise ValueError("role-aware synthesis lacks role coverage")

    blocked = _prior_policy_fingerprints(seed_archive)
    pairs, prior_rejections, duplicate_rejections = _generate_pairs(
        components,
        campaign_id=campaign_id,
        count=policy_pair_count,
        blocked=blocked,
        minimum_markets=minimum_markets,
        minimum_sessions=minimum_sessions,
        minimum_roles=minimum_roles,
    )
    manifest_payload = {
        "schema": "hydra_role_aware_account_population_v1",
        "campaign_id": campaign_id,
        "class_id": ROLE_AWARE_CLASS_ID,
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
                "profile": row.profile,
            }
            for row in pairs
        ],
        "same_membership_within_pair": True,
        "same_risk_unit_multiset_within_pair": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "same_class_0009_rescue": False,
        "new_market_outcomes_seen_during_generation": False,
    }
    return RoleAwareAccountPopulation(
        campaign_id=campaign_id,
        components=components,
        pairs=pairs,
        prior_policy_rejection_count=prior_rejections,
        duplicate_rejection_count=duplicate_rejections,
        manifest_hash=stable_hash(manifest_payload),
    )


def _select_components(
    seed_archive: Mapping[str, Any],
    *,
    maximum_components: int,
    minimum_component_events: int,
) -> tuple[RoleAwareComponent, ...]:
    pools: dict[str, list[RoleAwareComponent]] = {}
    seen_behavior: set[str] = set()
    for raw in seed_archive.get("sleeves") or ():
        sleeve = _sleeve_from_dict(raw["specification"])
        evidence = raw.get("development_evidence") or {}
        net = float(evidence.get("net_pnl") or 0.0)
        stressed = float(evidence.get("cost_stress_1_5x_net") or 0.0)
        events = int(evidence.get("events") or 0)
        if (
            sleeve.behavioral_fingerprint in seen_behavior
            or net <= 0.0
            or stressed <= 0.0
            or events < minimum_component_events
        ):
            continue
        seen_behavior.add(sleeve.behavioral_fingerprint)
        pools.setdefault(sleeve.market, []).append(
            RoleAwareComponent(
                sleeve=sleeve,
                net_pnl=net,
                stressed_net_pnl=stressed,
                event_count=events,
                incremental_status=str(
                    evidence.get("incremental_status") or ""
                ),
            )
        )
    for rows in pools.values():
        rows.sort(
            key=lambda row: (
                row.incremental_status != "MICRO_EDGE_USEFUL",
                -row.stressed_net_pnl,
                -row.event_count,
                row.sleeve.sleeve_id,
            )
        )

    output: list[RoleAwareComponent] = []
    markets = sorted(pools)
    cursor = 0
    while markets and len(output) < maximum_components:
        market = markets[cursor % len(markets)]
        if not pools[market]:
            markets.remove(market)
            cursor = 0
            continue
        output.append(pools[market].pop(0))
        cursor += 1
    return tuple(output)


def _generate_pairs(
    components: Sequence[RoleAwareComponent],
    *,
    campaign_id: str,
    count: int,
    blocked: set[str],
    minimum_markets: int,
    minimum_sessions: int,
    minimum_roles: int,
) -> tuple[tuple[RoleAwarePolicyPair, ...], int, int]:
    pairs: list[RoleAwarePolicyPair] = []
    seen_real: set[str] = set()
    seen_control: set[str] = set()
    seen_membership_profile: set[str] = set()
    prior_rejections = 0
    duplicate_rejections = 0
    for attempt in range(max(count * 100, 20_000)):
        if len(pairs) == count:
            break
        size = 6 + (attempt % 3)
        profile = PROFILES[(attempt // 3) % len(PROFILES)]
        ranked = sorted(
            components,
            key=lambda row: (
                stable_hash(
                    [campaign_id, "membership", attempt, row.sleeve.sleeve_id]
                ),
                row.sleeve.sleeve_id,
            ),
        )
        members = tuple(ranked[:size])
        if not _eligible_membership(
            members,
            minimum_markets=minimum_markets,
            minimum_sessions=minimum_sessions,
            minimum_roles=minimum_roles,
        ):
            continue
        membership_hash = stable_hash(
            sorted(row.sleeve.behavioral_fingerprint for row in members)
        )
        membership_profile = stable_hash([membership_hash, profile])
        if membership_profile in seen_membership_profile:
            duplicate_rejections += 1
            continue
        real = _role_aware_policy(
            members,
            profile=profile,
            campaign_id=campaign_id,
            membership_hash=membership_hash,
        )
        control = _permutation_control_policy(
            members,
            real_policy=real,
            profile=profile,
            campaign_id=campaign_id,
            membership_hash=membership_hash,
        )
        real_fingerprint = real.structural_fingerprint
        control_fingerprint = control.structural_fingerprint
        if real_fingerprint in blocked or control_fingerprint in blocked:
            prior_rejections += 1
            continue
        if (
            real_fingerprint in seen_real
            or control_fingerprint in seen_control
            or real_fingerprint == control_fingerprint
        ):
            duplicate_rejections += 1
            continue
        pair_id = deterministic_id(
            "role_aware_account_pair",
            {
                "campaign": campaign_id,
                "membership_hash": membership_hash,
                "profile": profile,
                "real": real_fingerprint,
                "control": control_fingerprint,
            },
        )
        pairs.append(
            RoleAwarePolicyPair(
                pair_id=pair_id,
                real_policy=real,
                matched_control_policy=control,
                profile=profile,
                membership_hash=membership_hash,
            )
        )
        seen_real.add(real_fingerprint)
        seen_control.add(control_fingerprint)
        seen_membership_profile.add(membership_profile)
    if len(pairs) != count:
        raise RuntimeError(
            f"only {len(pairs)} distinct role-aware pairs for {count} requested"
        )
    return tuple(pairs), prior_rejections, duplicate_rejections


def _eligible_membership(
    members: Sequence[RoleAwareComponent],
    *,
    minimum_markets: int,
    minimum_sessions: int,
    minimum_roles: int,
) -> bool:
    market_counts = _counts(row.sleeve.market for row in members)
    return (
        len({row.sleeve.market for row in members}) >= minimum_markets
        and len({row.sleeve.session_code for row in members})
        >= minimum_sessions
        and len({row.sleeve.role for row in members}) >= minimum_roles
        and len({row.sleeve.trigger_feature for row in members}) >= 3
        and max(market_counts.values()) <= 3
    )


def _role_aware_policy(
    members: Sequence[RoleAwareComponent],
    *,
    profile: str,
    campaign_id: str,
    membership_hash: str,
) -> RoleAwareAccountPolicyGenome:
    ordered = tuple(
        sorted(
            members,
            key=lambda row: (
                _ROLE_RANK[row.sleeve.role],
                stable_hash(
                    [campaign_id, membership_hash, row.sleeve.sleeve_id]
                ),
                row.sleeve.sleeve_id,
            ),
        )
    )
    allocations, limits = _profile(len(ordered), profile)
    sleeve_ids = tuple(row.sleeve.sleeve_id for row in ordered)
    return RoleAwareAccountPolicyGenome(
        policy_id=deterministic_id(
            "role_aware_account_policy",
            {
                "campaign": campaign_id,
                "class": ROLE_AWARE_CLASS_ID,
                "membership": membership_hash,
                "profile": profile,
                "priority": sleeve_ids,
                "allocations": allocations,
            },
        ),
        sleeve_ids=sleeve_ids,
        allocation_units=allocations,
        source_campaign=campaign_id,
        mutation_target=FailureDimension.INSUFFICIENT_TARGET_VELOCITY,
        **limits,
    )


def _permutation_control_policy(
    members: Sequence[RoleAwareComponent],
    *,
    real_policy: RoleAwareAccountPolicyGenome,
    profile: str,
    campaign_id: str,
    membership_hash: str,
) -> RoleAwareAccountPolicyGenome:
    ordered = tuple(
        sorted(
            members,
            key=lambda row: (
                stable_hash(
                    [
                        campaign_id,
                        membership_hash,
                        "role_blind_control",
                        row.sleeve.sleeve_id,
                    ]
                ),
                row.sleeve.sleeve_id,
            ),
        )
    )
    sleeve_ids = tuple(row.sleeve.sleeve_id for row in ordered)
    if sleeve_ids == real_policy.sleeve_ids:
        sleeve_ids = sleeve_ids[1:] + sleeve_ids[:1]
    offset = int(
        stable_hash([campaign_id, membership_hash, profile, "risk_rotation"])[
            :16
        ],
        16,
    ) % len(real_policy.allocation_units)
    allocations = (
        real_policy.allocation_units[offset:]
        + real_policy.allocation_units[:offset]
    )
    if allocations == real_policy.allocation_units and len(set(allocations)) > 1:
        allocations = allocations[1:] + allocations[:1]
    return RoleAwareAccountPolicyGenome(
        policy_id=deterministic_id(
            "role_blind_permutation_control",
            {
                "campaign": campaign_id,
                "class": ROLE_AWARE_CLASS_ID,
                "membership": membership_hash,
                "profile": profile,
                "priority": sleeve_ids,
                "allocations": allocations,
            },
        ),
        sleeve_ids=sleeve_ids,
        allocation_units=allocations,
        maximum_simultaneous_positions=(
            real_policy.maximum_simultaneous_positions
        ),
        maximum_mini_equivalent=real_policy.maximum_mini_equivalent,
        conflict_policy=real_policy.conflict_policy,
        daily_risk_budget=real_policy.daily_risk_budget,
        daily_profit_lock=real_policy.daily_profit_lock,
        low_mll_buffer=real_policy.low_mll_buffer,
        critical_mll_buffer=real_policy.critical_mll_buffer,
        loss_streak_throttle_after=real_policy.loss_streak_throttle_after,
        mode=real_policy.mode,
        source_campaign=campaign_id,
        mutation_target=FailureDimension.INSUFFICIENT_TARGET_VELOCITY,
    )


def _profile(size: int, profile: str) -> tuple[tuple[int, ...], dict[str, Any]]:
    if profile == "ROLE_BALANCED_POOL":
        allocations = (2, 2) + (1,) * (size - 2)
        limits = {
            "maximum_simultaneous_positions": min(3, size),
            "maximum_mini_equivalent": 8,
            "conflict_policy": "FIXED_PRIORITY",
            "daily_risk_budget": 1_000.0,
            "daily_profit_lock": 1_800.0,
            "low_mll_buffer": 3_000.0,
            "critical_mll_buffer": 1_500.0,
            "loss_streak_throttle_after": 3,
            "mode": "COMBINE_RESEARCH",
        }
    elif profile == "ROLE_TARGET_VELOCITY_POOL":
        allocations = (3, 3, 2, 2) + (1,) * (size - 4)
        limits = {
            "maximum_simultaneous_positions": min(4, size),
            "maximum_mini_equivalent": 12,
            "conflict_policy": "FIXED_PRIORITY",
            "daily_risk_budget": 1_500.0,
            "daily_profit_lock": 2_250.0,
            "low_mll_buffer": 3_000.0,
            "critical_mll_buffer": 1_500.0,
            "loss_streak_throttle_after": 3,
            "mode": "COMBINE_RESEARCH",
        }
    elif profile == "ROLE_MLL_CONSTRAINED_POOL":
        allocations = (2,) + (1,) * (size - 1)
        limits = {
            "maximum_simultaneous_positions": min(2, size),
            "maximum_mini_equivalent": 6,
            "conflict_policy": "FIXED_PRIORITY",
            "daily_risk_budget": 700.0,
            "daily_profit_lock": 1_500.0,
            "low_mll_buffer": 3_250.0,
            "critical_mll_buffer": 1_750.0,
            "loss_streak_throttle_after": 2,
            "mode": "COMBINE_RESEARCH",
        }
    else:
        raise ValueError(f"unsupported role-aware profile: {profile}")
    return allocations, limits


def _account_limits(policy: RoleAwareAccountPolicyGenome) -> tuple[Any, ...]:
    return (
        policy.maximum_simultaneous_positions,
        policy.maximum_mini_equivalent,
        policy.conflict_policy,
        policy.daily_risk_budget,
        policy.daily_profit_lock,
        policy.low_mll_buffer,
        policy.critical_mll_buffer,
        policy.loss_streak_throttle_after,
        policy.mode,
    )


def _prior_policy_fingerprints(seed_archive: Mapping[str, Any]) -> set[str]:
    output: set[str] = set()
    for row in seed_archive.get("policies") or ():
        fingerprint = (row.get("policy") or {}).get("structural_fingerprint")
        if fingerprint:
            output.add(str(fingerprint))
    for row in seed_archive.get("mutations") or ():
        fingerprint = (row.get("child_policy") or {}).get(
            "structural_fingerprint"
        )
        if fingerprint:
            output.add(str(fingerprint))
    return output


def _sleeve_from_dict(value: Mapping[str, Any]) -> SleeveSpec:
    return SleeveSpec(
        sleeve_id=str(value["sleeve_id"]),
        component_ids=tuple(str(row) for row in value["component_ids"]),
        market=str(value["market"]),
        execution_market=str(value["execution_market"]),
        timeframe=str(value["timeframe"]),
        session_code=int(value["session_code"]),
        trigger_feature=str(value["trigger_feature"]),
        trigger_operator=str(value["trigger_operator"]),
        trigger_quantile=float(value["trigger_quantile"]),
        context_feature=(
            None
            if value.get("context_feature") is None
            else str(value["context_feature"])
        ),
        context_operator=(
            None
            if value.get("context_operator") is None
            else str(value["context_operator"])
        ),
        context_quantile=(
            None
            if value.get("context_quantile") is None
            else float(value["context_quantile"])
        ),
        side=int(value["side"]),
        holding_bars=int(value["holding_bars"]),
        exit_style=str(value["exit_style"]),
        role=EconomicRole(str(value["role"])),
        source_campaign=str(value["source_campaign"]),
        lineage_id=str(value["lineage_id"]),
        version=int(value.get("version") or 1),
    )


def _counts(values: Sequence[Any] | Any) -> dict[Any, int]:
    output: dict[Any, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items(), key=lambda row: str(row[0])))


__all__ = [
    "PROFILES",
    "ROLE_AWARE_CLASS_ID",
    "ROLE_AWARE_HYPOTHESIS",
    "ROLE_ORDER",
    "RoleAwareAccountPopulation",
    "RoleAwareAccountPolicyGenome",
    "RoleAwareComponent",
    "RoleAwarePolicyPair",
    "generate_role_aware_account_population",
]
