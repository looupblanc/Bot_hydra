from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    EconomicRole,
    FailureDimension,
    SleeveSpec,
    deterministic_id,
    stable_hash,
)


CROSS_SESSION_CLASS_ID = "CROSS_SESSION_ACCOUNT_COMPLEMENTARITY_SYNTHESIS_V1"
CROSS_SESSION_HYPOTHESIS = (
    "Behaviorally distinct positive-net sleeves operating in different markets "
    "and sessions should raise account target velocity more efficiently than "
    "risk-matched portfolios concentrated in one market or one session, because "
    "their opportunity clocks and loss paths are less synchronous."
)
PROFILES = (
    "EQUAL_RISK_COMPLEMENTARY",
    "TARGET_VELOCITY_COMPLEMENTARY",
    "MLL_PROTECTIVE_COMPLEMENTARY",
)


@dataclass(frozen=True, slots=True)
class SelectedAccountComponent:
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
class AccountPolicyPair:
    pair_id: str
    real_policy: AccountPolicyGenome
    matched_control_policy: AccountPolicyGenome
    profile: str
    control_basis: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "profile": self.profile,
            "control_basis": self.control_basis,
            "identical_account_parameters": (
                _account_parameters(self.real_policy)
                == _account_parameters(self.matched_control_policy)
            ),
        }


@dataclass(frozen=True, slots=True)
class CrossSessionAccountPopulation:
    campaign_id: str
    components: tuple[SelectedAccountComponent, ...]
    pairs: tuple[AccountPolicyPair, ...]
    prior_policy_rejection_count: int
    duplicate_rejection_count: int
    manifest_hash: str

    @property
    def real_policies(self) -> tuple[AccountPolicyGenome, ...]:
        return tuple(row.real_policy for row in self.pairs)

    @property
    def matched_control_policies(self) -> tuple[AccountPolicyGenome, ...]:
        return tuple(row.matched_control_policy for row in self.pairs)

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": CROSS_SESSION_CLASS_ID,
            "component_count": len(self.components),
            "real_policy_count": len(self.pairs),
            "matched_control_policy_count": len(self.pairs),
            "markets": sorted({row.sleeve.market for row in self.components}),
            "sessions": sorted(
                {row.sleeve.session_code for row in self.components}
            ),
            "roles": sorted({row.sleeve.role.value for row in self.components}),
            "profiles": _counts(row.profile for row in self.pairs),
            "control_bases": _counts(row.control_basis for row in self.pairs),
            "prior_policy_rejection_count": self.prior_policy_rejection_count,
            "duplicate_rejection_count": self.duplicate_rejection_count,
            "manifest_hash": self.manifest_hash,
            "new_candidate_ids": True,
            "status_inheritance": False,
            "validated": False,
        }


def generate_cross_session_account_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    policy_pair_count: int = 512,
    maximum_components: int = 48,
    minimum_component_events: int = 20,
    maximum_components_per_market: int = 16,
    maximum_components_per_session: int = 18,
    maximum_components_per_mechanism: int = 8,
) -> CrossSessionAccountPopulation:
    """Freeze account-first multi-session policies and matched controls.

    Generation reads only the predecessor's development archive.  It does not
    open market features or evaluate any new policy outcome.  Real policies
    contain three or four distinct sleeves spanning markets and sessions.  The
    paired control keeps every account parameter fixed but concentrates sleeve
    membership in one market or one session.
    """

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if policy_pair_count < 64:
        raise ValueError("account-first synthesis requires at least 64 pairs")
    if maximum_components < 12:
        raise ValueError("component bank is too small for complementarity")
    if seed_archive.get("development_only") is not True:
        raise ValueError("cross-session generation requires a development seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive generation")
    governance = seed_archive.get("governance") or {}
    if governance.get("status_inheritance") is not False:
        raise ValueError("seed status inheritance must be disabled")

    components = _select_components(
        seed_archive,
        maximum_components=maximum_components,
        minimum_component_events=minimum_component_events,
        maximum_components_per_market=maximum_components_per_market,
        maximum_components_per_session=maximum_components_per_session,
        maximum_components_per_mechanism=maximum_components_per_mechanism,
    )
    if len(components) < 12:
        raise ValueError("insufficient positive-net distinct components")
    if len({row.sleeve.market for row in components}) < 3:
        raise ValueError("account synthesis requires at least three markets")
    if len({row.sleeve.session_code for row in components}) < 3:
        raise ValueError("account synthesis requires at least three sessions")

    blocked = _prior_policy_fingerprints(seed_archive)
    real_candidates, prior_rejections, duplicate_rejections = _real_candidates(
        components,
        campaign_id=campaign_id,
        count=policy_pair_count,
        blocked=blocked,
    )
    control_combinations = _control_combinations(components)
    pairs, control_duplicates = _pair_controls(
        real_candidates,
        components,
        control_combinations=control_combinations,
        campaign_id=campaign_id,
        blocked=blocked,
    )
    if len(pairs) != policy_pair_count:
        raise RuntimeError(
            f"only {len(pairs)} complete policy pairs for {policy_pair_count} requested"
        )

    manifest_payload = {
        "schema": "hydra_cross_session_account_population_v1",
        "campaign_id": campaign_id,
        "class_id": CROSS_SESSION_CLASS_ID,
        "component_behavioral_fingerprints": [
            row.sleeve.behavioral_fingerprint for row in components
        ],
        "pairs": [
            {
                "pair_id": row.pair_id,
                "real": row.real_policy.structural_fingerprint,
                "matched_control": row.matched_control_policy.structural_fingerprint,
                "profile": row.profile,
                "control_basis": row.control_basis,
            }
            for row in pairs
        ],
        "new_candidate_ids": True,
        "status_inheritance": False,
        "same_class_0008_rescue": False,
        "new_market_outcomes_seen_during_generation": False,
    }
    return CrossSessionAccountPopulation(
        campaign_id=campaign_id,
        components=components,
        pairs=pairs,
        prior_policy_rejection_count=prior_rejections,
        duplicate_rejection_count=duplicate_rejections + control_duplicates,
        manifest_hash=stable_hash(manifest_payload),
    )


def _select_components(
    seed_archive: Mapping[str, Any],
    *,
    maximum_components: int,
    minimum_component_events: int,
    maximum_components_per_market: int,
    maximum_components_per_session: int,
    maximum_components_per_mechanism: int,
) -> tuple[SelectedAccountComponent, ...]:
    pools: dict[str, list[SelectedAccountComponent]] = {}
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
            SelectedAccountComponent(
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

    output: list[SelectedAccountComponent] = []
    market_counts: dict[str, int] = {}
    session_counts: dict[int, int] = {}
    mechanism_counts: dict[str, int] = {}
    markets = sorted(pools)
    cursor = 0
    while markets and len(output) < maximum_components:
        market = markets[cursor % len(markets)]
        candidate_index = next(
            (
                index
                for index, row in enumerate(pools[market])
                if market_counts.get(market, 0) < maximum_components_per_market
                and session_counts.get(row.sleeve.session_code, 0)
                < maximum_components_per_session
                and mechanism_counts.get(row.sleeve.trigger_feature, 0)
                < maximum_components_per_mechanism
            ),
            None,
        )
        if candidate_index is None:
            markets.remove(market)
            cursor = 0
            continue
        row = pools[market].pop(candidate_index)
        output.append(row)
        market_counts[market] = market_counts.get(market, 0) + 1
        session = row.sleeve.session_code
        session_counts[session] = session_counts.get(session, 0) + 1
        mechanism = row.sleeve.trigger_feature
        mechanism_counts[mechanism] = mechanism_counts.get(mechanism, 0) + 1
        cursor += 1
    return tuple(output)


def _real_candidates(
    components: Sequence[SelectedAccountComponent],
    *,
    campaign_id: str,
    count: int,
    blocked: set[str],
) -> tuple[tuple[tuple[AccountPolicyGenome, str], ...], int, int]:
    selected: list[tuple[AccountPolicyGenome, str]] = []
    prior_rejections = 0
    duplicate_rejections = 0
    seen: set[str] = set()
    for attempt in range(max(count * 40, 4_000)):
        if len(selected) == count:
            break
        size = 3 + (attempt % 2)
        ranked = sorted(
            components,
            key=lambda row: (
                stable_hash(
                    [campaign_id, "real_members", attempt, row.sleeve.sleeve_id]
                ),
                row.sleeve.sleeve_id,
            ),
        )
        members: list[SelectedAccountComponent] = []
        for row in ranked:
            if (
                sum(value.sleeve.market == row.sleeve.market for value in members)
                >= 2
            ):
                continue
            members.append(row)
            if len(members) == size:
                break
        if len(members) != size or not _is_complementary(members):
            continue
        profile = PROFILES[attempt % len(PROFILES)]
        policy = _policy(
            members,
            profile=profile,
            campaign_id=campaign_id,
            matched_control=False,
        )
        fingerprint = policy.structural_fingerprint
        if fingerprint in blocked:
            prior_rejections += 1
            continue
        if fingerprint in seen:
            duplicate_rejections += 1
            continue
        seen.add(fingerprint)
        selected.append((policy, profile))
    if len(selected) != count:
        raise RuntimeError(
            f"only {len(selected)} distinct real account policies for {count} requested"
        )
    return (
        tuple(selected),
        prior_rejections,
        duplicate_rejections,
    )


def _control_combinations(
    components: Sequence[SelectedAccountComponent],
) -> dict[int, tuple[tuple[SelectedAccountComponent, ...], ...]]:
    output: dict[int, tuple[tuple[SelectedAccountComponent, ...], ...]] = {}
    for size in (3, 4):
        rows = [
            members
            for members in itertools.combinations(components, size)
            if len({row.sleeve.market for row in members}) == 1
            or len({row.sleeve.session_code for row in members}) == 1
        ]
        rows.sort(
            key=lambda members: stable_hash(
                [row.sleeve.sleeve_id for row in members]
            )
        )
        output[size] = tuple(rows)
    return output


def _pair_controls(
    real_candidates: Sequence[tuple[AccountPolicyGenome, str]],
    components: Sequence[SelectedAccountComponent],
    *,
    control_combinations: Mapping[
        int, Sequence[tuple[SelectedAccountComponent, ...]]
    ],
    campaign_id: str,
    blocked: set[str],
) -> tuple[tuple[AccountPolicyPair, ...], int]:
    evidence = {row.sleeve.sleeve_id: row for row in components}
    used_controls: set[str] = set()
    pairs: list[AccountPolicyPair] = []
    duplicate_rejections = 0
    for real, profile in real_candidates:
        real_events = sum(evidence[key].event_count for key in real.sleeve_ids)
        choices = control_combinations[len(real.sleeve_ids)]
        if not choices:
            raise RuntimeError("no concentrated matched-control combination")
        start = int(
            stable_hash([campaign_id, real.policy_id, "control_start"])[:16], 16
        ) % len(choices)
        best: tuple[float, str, AccountPolicyGenome, str] | None = None
        for offset in range(len(choices)):
            members = choices[(start + offset) % len(choices)]
            control = _matched_control_policy(
                members,
                real_policy=real,
                profile=profile,
                campaign_id=campaign_id,
            )
            fingerprint = control.structural_fingerprint
            if fingerprint in blocked or fingerprint in used_controls:
                duplicate_rejections += 1
                continue
            if set(control.sleeve_ids) == set(real.sleeve_ids):
                duplicate_rejections += 1
                continue
            control_events = sum(row.event_count for row in members)
            event_distance = abs(control_events - real_events) / max(real_events, 1)
            control_basis = (
                "SAME_MARKET_CONCENTRATED"
                if len({row.sleeve.market for row in members}) == 1
                else "SAME_SESSION_CONCENTRATED"
            )
            rank = stable_hash(
                [campaign_id, real.policy_id, fingerprint]
            )
            candidate = (event_distance, rank, control, control_basis)
            if best is None or candidate[:2] < best[:2]:
                best = candidate
            if offset >= 127 and best is not None:
                break
        if best is None:
            raise RuntimeError(f"no unique matched control for {real.policy_id}")
        _, _, control, control_basis = best
        used_controls.add(control.structural_fingerprint)
        pair_id = deterministic_id(
            "cross_session_pair",
            {
                "campaign": campaign_id,
                "real": real.structural_fingerprint,
                "control": control.structural_fingerprint,
            },
        )
        pairs.append(
            AccountPolicyPair(
                pair_id=pair_id,
                real_policy=real,
                matched_control_policy=control,
                profile=profile,
                control_basis=control_basis,
            )
        )
    return tuple(pairs), duplicate_rejections


def _is_complementary(
    members: Sequence[SelectedAccountComponent],
) -> bool:
    return (
        len({row.sleeve.market for row in members}) >= 2
        and len({row.sleeve.session_code for row in members}) >= 2
        and len({row.sleeve.role for row in members}) >= 2
        and len({row.sleeve.trigger_feature for row in members}) >= 2
        and max(_counts(row.sleeve.market for row in members).values()) <= 2
    )


def _policy(
    members: Sequence[SelectedAccountComponent],
    *,
    profile: str,
    campaign_id: str,
    matched_control: bool,
    pair_key: str | None = None,
) -> AccountPolicyGenome:
    sleeve_ids = tuple(row.sleeve.sleeve_id for row in members)
    if profile == "EQUAL_RISK_COMPLEMENTARY":
        allocations = (1,) * len(members)
        maximum_positions, maximum_mini = min(2, len(members)), 6
        daily_risk, daily_profit = 750.0, 1_500.0
        low_buffer, critical_buffer, loss_streak = 3_000.0, 1_500.0, 3
    elif profile == "TARGET_VELOCITY_COMPLEMENTARY":
        preferred = max(
            range(len(members)),
            key=lambda index: (
                members[index].sleeve.role
                in {EconomicRole.TARGET_ACCELERATOR, EconomicRole.PRIMARY_ALPHA},
                members[index].stressed_net_pnl,
                members[index].event_count,
                members[index].sleeve.sleeve_id,
            ),
        )
        allocations = tuple(
            2 if index == preferred else 1 for index in range(len(members))
        )
        maximum_positions, maximum_mini = min(3, len(members)), 10
        daily_risk, daily_profit = 1_250.0, 2_250.0
        low_buffer, critical_buffer, loss_streak = 3_000.0, 1_500.0, 3
    elif profile == "MLL_PROTECTIVE_COMPLEMENTARY":
        allocations = (1,) * len(members)
        maximum_positions, maximum_mini = min(2, len(members)), 5
        daily_risk, daily_profit = 600.0, 1_200.0
        low_buffer, critical_buffer, loss_streak = 3_250.0, 1_750.0, 2
    else:
        raise ValueError(f"unsupported account profile: {profile}")
    payload = {
        "campaign": campaign_id,
        "class": CROSS_SESSION_CLASS_ID,
        "sleeves": sleeve_ids,
        "profile": profile,
        "matched_control": matched_control,
        "pair_key": pair_key,
    }
    return AccountPolicyGenome(
        policy_id=deterministic_id(
            "cross_session_control_policy"
            if matched_control
            else "cross_session_account_policy",
            payload,
        ),
        sleeve_ids=sleeve_ids,
        allocation_units=allocations,
        maximum_simultaneous_positions=maximum_positions,
        maximum_mini_equivalent=maximum_mini,
        conflict_policy="FIXED_PRIORITY",
        daily_risk_budget=daily_risk,
        daily_profit_lock=daily_profit,
        low_mll_buffer=low_buffer,
        critical_mll_buffer=critical_buffer,
        loss_streak_throttle_after=loss_streak,
        mode="COMBINE_RESEARCH",
        source_campaign=campaign_id,
        mutation_target=FailureDimension.INSUFFICIENT_TARGET_VELOCITY,
    )


def _matched_control_policy(
    members: Sequence[SelectedAccountComponent],
    *,
    real_policy: AccountPolicyGenome,
    profile: str,
    campaign_id: str,
) -> AccountPolicyGenome:
    sleeve_ids = tuple(row.sleeve.sleeve_id for row in members)
    payload = {
        "campaign": campaign_id,
        "class": CROSS_SESSION_CLASS_ID,
        "sleeves": sleeve_ids,
        "profile": profile,
        "matched_control": True,
        "pair_key": real_policy.policy_id,
        "account_parameters": _account_parameters(real_policy),
    }
    return AccountPolicyGenome(
        policy_id=deterministic_id("cross_session_control_policy", payload),
        sleeve_ids=sleeve_ids,
        allocation_units=real_policy.allocation_units,
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


def _account_parameters(policy: AccountPolicyGenome) -> tuple[Any, ...]:
    return (
        policy.allocation_units,
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
    "CROSS_SESSION_CLASS_ID",
    "CROSS_SESSION_HYPOTHESIS",
    "AccountPolicyPair",
    "CrossSessionAccountPopulation",
    "SelectedAccountComponent",
    "generate_cross_session_account_population",
]
