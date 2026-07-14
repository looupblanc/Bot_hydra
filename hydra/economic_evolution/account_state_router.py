from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.account_state_evaluation import (
    AccountStatePolicyPair,
    AccountStateRoutingPolicy,
)
from hydra.economic_evolution.role_aware_account import (
    RoleAwareComponent,
    _eligible_membership,
    _select_components,
)
from hydra.economic_evolution.schema import deterministic_id, stable_hash


ACCOUNT_STATE_CLASS_ID = "ACCOUNT_STATE_CONDITIONAL_ROLE_ROUTER_V1"
ACCOUNT_STATE_HYPOTHESIS = (
    "On identical frozen sleeve membership, event paths, state thresholds and "
    "account limits, routing risk according to a sleeve's preregistered role "
    "and past-only account state should improve stressed expectancy and target "
    "progress over a deterministic within-membership role-label permutation "
    "without worsening MLL or consistency."
)
ACCOUNT_STATE_LIMITS: dict[str, Any] = {
    "daily_loss_guard": 1_000.0,
    "daily_profit_lock": 1_500.0,
    "critical_buffer": 750.0,
    "protect_buffer": 2_250.0,
    "accelerate_buffer": 4_000.0,
    "accelerate_remaining_target": 6_000.0,
    "loss_streak_protect_after": 2,
    "balanced_maximum_positions": 2,
    "accelerate_maximum_positions": 3,
    "protect_maximum_positions": 1,
    "accelerate_risk_units": 2,
    "maximum_mini_equivalent": 15,
}


@dataclass(frozen=True, slots=True)
class AccountStateRouterPopulation:
    campaign_id: str
    components: tuple[RoleAwareComponent, ...]
    pairs: tuple[AccountStatePolicyPair, ...]
    duplicate_rejection_count: int
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[AccountStateRoutingPolicy, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[AccountStateRoutingPolicy, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": ACCOUNT_STATE_CLASS_ID,
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
            "sleeve_counts": _counts(
                len(row.real_policy.component_ids) for row in self.pairs
            ),
            "same_ordered_membership_pair_count": sum(
                row.real_policy.component_ids
                == row.matched_control_policy.component_ids
                for row in self.pairs
            ),
            "same_role_multiset_pair_count": sum(
                sorted(row.real_policy.component_role_map.values())
                == sorted(
                    row.matched_control_policy.component_role_map.values()
                )
                for row in self.pairs
            ),
            "same_state_limits_pair_count": len(self.pairs),
            "unique_membership_count": len(
                {row.membership_hash for row in self.pairs}
            ),
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "manifest_hash": self.manifest_hash,
            "past_only_account_state": True,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "outcomes_seen_during_generation": False,
            "outbound_order_capability": False,
            "validated": False,
        }


def generate_account_state_router_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
    minimum_markets: int = 3,
    minimum_sessions: int = 3,
    minimum_roles: int = 3,
) -> AccountStateRouterPopulation:
    """Generate fixed state routers with within-membership role controls.

    Generation reads only the frozen development seed.  It never opens market
    features, predecessor results or proof windows.  Real and control policies
    share ordered membership, event paths, thresholds, action/risk multisets
    and account limits; the only difference is the deterministic role map.
    """

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if policy_pair_count < 64:
        raise ValueError("account-state synthesis requires at least 64 pairs")
    if maximum_components < 16:
        raise ValueError("component bank is too small for state routing")
    if seed_archive.get("development_only") is not True:
        raise ValueError("account-state generation requires a development seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive state routing")
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
        raise ValueError("state routing lacks market coverage")
    if len({row.sleeve.session_code for row in components}) < minimum_sessions:
        raise ValueError("state routing lacks session coverage")
    if len({row.sleeve.role for row in components}) < minimum_roles:
        raise ValueError("state routing lacks role coverage")

    pairs, duplicate_rejections = _generate_pairs(
        components,
        campaign_id=campaign_id,
        count=policy_pair_count,
        minimum_markets=minimum_markets,
        minimum_sessions=minimum_sessions,
        minimum_roles=minimum_roles,
    )
    manifest_payload = {
        "schema": "hydra_account_state_router_population_v1",
        "campaign_id": campaign_id,
        "class_id": ACCOUNT_STATE_CLASS_ID,
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
        "account_state_limits": dict(ACCOUNT_STATE_LIMITS),
        "same_ordered_membership_within_pair": True,
        "same_component_event_paths_within_pair": True,
        "same_state_thresholds_within_pair": True,
        "same_action_and_risk_multisets_within_pair": True,
        "same_account_limits_within_pair": True,
        "control_difference": "DETERMINISTIC_ROLE_LABEL_PERMUTATION",
        "past_only_state_inputs": True,
        "new_candidate_ids": True,
        "status_inheritance": False,
        "same_class_0010_rescue": False,
        "new_market_outcomes_seen_during_generation": False,
        "outbound_order_capability": False,
    }
    return AccountStateRouterPopulation(
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
    minimum_roles: int,
) -> tuple[tuple[AccountStatePolicyPair, ...], int]:
    pairs: list[AccountStatePolicyPair] = []
    seen_memberships: set[str] = set()
    seen_real: set[str] = set()
    seen_control: set[str] = set()
    duplicate_rejections = 0
    for attempt in range(max(count * 100, 20_000)):
        if len(pairs) == count:
            break
        size = 6 + (attempt % 3)
        ranked = sorted(
            components,
            key=lambda row: (
                stable_hash(
                    [campaign_id, "account_state_membership", attempt, row.sleeve.sleeve_id]
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
        if membership_hash in seen_memberships:
            duplicate_rejections += 1
            continue
        ordered = tuple(
            sorted(
                members,
                key=lambda row: (
                    stable_hash(
                        [
                            campaign_id,
                            membership_hash,
                            "shared_component_priority",
                            row.sleeve.sleeve_id,
                        ]
                    ),
                    row.sleeve.sleeve_id,
                ),
            )
        )
        component_ids = tuple(row.sleeve.sleeve_id for row in ordered)
        real_roles = tuple(
            (row.sleeve.sleeve_id, row.sleeve.role.value) for row in ordered
        )
        control_roles = _permuted_roles(
            component_ids,
            real_roles,
            campaign_id=campaign_id,
            membership_hash=membership_hash,
        )
        real = _policy(
            component_ids,
            real_roles,
            policy_id=deterministic_id(
                "account_state_role_router",
                {
                    "campaign": campaign_id,
                    "class": ACCOUNT_STATE_CLASS_ID,
                    "membership": membership_hash,
                    "role_map": real_roles,
                    "limits": ACCOUNT_STATE_LIMITS,
                },
            ),
        )
        control = _policy(
            component_ids,
            control_roles,
            policy_id=deterministic_id(
                "account_state_role_permutation_control",
                {
                    "campaign": campaign_id,
                    "class": ACCOUNT_STATE_CLASS_ID,
                    "membership": membership_hash,
                    "role_map": control_roles,
                    "limits": ACCOUNT_STATE_LIMITS,
                },
            ),
        )
        if (
            real.structural_fingerprint in seen_real
            or control.structural_fingerprint in seen_control
            or real.structural_fingerprint == control.structural_fingerprint
        ):
            duplicate_rejections += 1
            continue
        pair = AccountStatePolicyPair(
            pair_id=deterministic_id(
                "account_state_router_pair",
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
        pairs.append(pair)
        seen_memberships.add(membership_hash)
        seen_real.add(real.structural_fingerprint)
        seen_control.add(control.structural_fingerprint)
    if len(pairs) != count:
        raise RuntimeError(
            f"only {len(pairs)} distinct account-state pairs for {count} requested"
        )
    return tuple(pairs), duplicate_rejections


def _permuted_roles(
    component_ids: Sequence[str],
    real_roles: Sequence[tuple[str, str]],
    *,
    campaign_id: str,
    membership_hash: str,
) -> tuple[tuple[str, str], ...]:
    values = tuple(value for _component, value in real_roles)
    offset = 1 + (
        int(
            stable_hash(
                [campaign_id, membership_hash, "matched_role_rotation"]
            )[:16],
            16,
        )
        % (len(values) - 1)
    )
    rotated = values[offset:] + values[:offset]
    output = tuple(zip(component_ids, rotated, strict=True))
    if output == tuple(real_roles):
        rotated = values[1:] + values[:1]
        output = tuple(zip(component_ids, rotated, strict=True))
    if output == tuple(real_roles):
        raise RuntimeError("unable to construct a distinct role permutation")
    return output


def _policy(
    component_ids: tuple[str, ...],
    roles: tuple[tuple[str, str], ...],
    *,
    policy_id: str,
) -> AccountStateRoutingPolicy:
    return AccountStateRoutingPolicy(
        policy_id=policy_id,
        component_ids=component_ids,
        component_roles=roles,
        **ACCOUNT_STATE_LIMITS,
    )


def _counts(values: Sequence[Any] | Any) -> dict[Any, int]:
    output: dict[Any, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items(), key=lambda row: str(row[0])))


__all__ = [
    "ACCOUNT_STATE_CLASS_ID",
    "ACCOUNT_STATE_HYPOTHESIS",
    "ACCOUNT_STATE_LIMITS",
    "AccountStateRouterPopulation",
    "generate_account_state_router_population",
]
