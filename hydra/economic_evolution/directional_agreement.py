from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    ComponentKind,
    ComponentSpec,
    EconomicRole,
    FailureDimension,
    FeatureDependency,
    PortType,
    SleeveSpec,
    deterministic_id,
    stable_hash,
)


AGREEMENT_CLASS_ID = "DIRECTIONAL_CONTEXT_AGREEMENT_TRADE_VETO_V1"
AGREEMENT_HYPOTHESIS = (
    "After an independently specified event trigger, counterparties still "
    "unwinding inventory against an already established closed-bar directional "
    "state pay continuation sleeves; vetoing triggers opposed to that state "
    "should improve net excursion after costs without increasing turnover."
)
CONTEXT_FEATURES = (
    "ctx_60m_return",
    "ctx_30m_return",
    "ctx_15m_return",
    "ctx_5m_return",
)


@dataclass(frozen=True, slots=True)
class SelectedAgreementSource:
    source: SleeveSpec
    net_pnl: float
    stressed_net_pnl: float
    event_count: int
    incremental_status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_sleeve_id": self.source.sleeve_id,
            "source_behavioral_fingerprint": self.source.behavioral_fingerprint,
            "market": self.source.market,
            "execution_market": self.source.execution_market,
            "session_code": self.source.session_code,
            "mechanism": self.source.trigger_feature,
            "role": self.source.role.value,
            "net_pnl": self.net_pnl,
            "stress_1_5x_net_pnl": self.stressed_net_pnl,
            "event_count": self.event_count,
            "incremental_status": self.incremental_status,
        }


@dataclass(frozen=True, slots=True)
class DirectionalAgreementPopulation:
    campaign_id: str
    sources: tuple[SelectedAgreementSource, ...]
    components: tuple[ComponentSpec, ...]
    real_sleeves: tuple[SleeveSpec, ...]
    matched_null_sleeves: tuple[SleeveSpec, ...]
    policies: tuple[AccountPolicyGenome, ...]
    policy_archetypes: tuple[tuple[str, str], ...]
    source_by_candidate: tuple[tuple[str, str], ...]
    horizon_by_candidate: tuple[tuple[str, str], ...]
    candidate_manifest_hash: str

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "class_id": AGREEMENT_CLASS_ID,
            "source_count": len(self.sources),
            "component_count": len(self.components),
            "real_sleeve_count": len(self.real_sleeves),
            "matched_null_sleeve_count": len(self.matched_null_sleeves),
            "account_policy_count": len(self.policies),
            "markets": sorted({row.source.market for row in self.sources}),
            "sessions": sorted({row.source.session_code for row in self.sources}),
            "mechanisms": sorted(
                {row.source.trigger_feature for row in self.sources}
            ),
            "context_horizons": sorted(set(dict(self.horizon_by_candidate).values())),
            "policy_archetype_counts": _counts(
                dict(self.policy_archetypes).values()
            ),
            "candidate_manifest_hash": self.candidate_manifest_hash,
            "status_inheritance": False,
            "validated": False,
        }


def generate_directional_agreement_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    excluded_source_sleeve_ids: Sequence[str],
    maximum_sources: int = 24,
    maximum_sources_per_market: int = 5,
    maximum_sources_per_market_session: int = 2,
    maximum_sources_per_market_mechanism: int = 2,
    minimum_source_events: int = 24,
    contexts_per_source: int = 2,
    agreement_quantile: float = 0.65,
    policy_count: int = 256,
) -> DirectionalAgreementPopulation:
    """Generate a new closed-bar trade-veto class without outcome feedback.

    Each selected development source receives two structurally distinct context
    horizons.  The real candidate keeps only triggers aligned with the source
    direction.  Its paired family null uses the same source and horizon but the
    opposite directional state.  Existing source behavior is rejected before a
    new identity can be emitted.
    """

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if seed_archive.get("development_only") is not True:
        raise ValueError("agreement generation requires a development-only seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive generation")
    if seed_archive.get("governance", {}).get("status_inheritance") is not False:
        raise ValueError("seed status inheritance must be disabled")
    if contexts_per_source != 2:
        raise ValueError("0008 freezes exactly two context graphs per source")
    if not 0.5 < agreement_quantile < 0.9:
        raise ValueError("agreement quantile must stay in the bounded range")
    if maximum_sources < 4 or policy_count < 1:
        raise ValueError("agreement population is too small")

    seed_sleeves = tuple(
        _sleeve_from_dict(row["specification"])
        for row in seed_archive.get("sleeves") or ()
    )
    seed_behavior = {row.behavioral_fingerprint for row in seed_sleeves}
    sources = _select_sources(
        seed_archive,
        excluded=frozenset(str(value) for value in excluded_source_sleeve_ids),
        maximum_sources=maximum_sources,
        maximum_sources_per_market=maximum_sources_per_market,
        maximum_sources_per_market_session=maximum_sources_per_market_session,
        maximum_sources_per_market_mechanism=maximum_sources_per_market_mechanism,
        minimum_source_events=minimum_source_events,
    )
    if len({row.source.market for row in sources}) < 2:
        raise ValueError("agreement population needs at least two markets")

    components: dict[str, ComponentSpec] = {}
    real_sleeves: list[SleeveSpec] = []
    null_sleeves: list[SleeveSpec] = []
    source_by_candidate: list[tuple[str, str]] = []
    horizon_by_candidate: list[tuple[str, str]] = []
    for selected in sources:
        source = selected.source
        variants = _context_variants(
            source,
            seed_behavior=seed_behavior,
            contexts_per_source=contexts_per_source,
            agreement_quantile=agreement_quantile,
            campaign_id=campaign_id,
        )
        for context_feature in variants:
            real_operator, real_quantile = _aligned_context(
                source.side, agreement_quantile
            )
            null_operator, null_quantile = _opposed_context(
                source.side, agreement_quantile
            )
            real_components = _agreement_components(
                source,
                campaign_id=campaign_id,
                context_feature=context_feature,
                context_operator=real_operator,
                context_quantile=real_quantile,
                matched_null=False,
            )
            null_components = _agreement_components(
                source,
                campaign_id=campaign_id,
                context_feature=context_feature,
                context_operator=null_operator,
                context_quantile=null_quantile,
                matched_null=True,
            )
            for component in (*real_components, *null_components):
                components.setdefault(component.component_id, component)
            real = _agreement_sleeve(
                source,
                component_ids=tuple(row.component_id for row in real_components),
                campaign_id=campaign_id,
                context_feature=context_feature,
                context_operator=real_operator,
                context_quantile=real_quantile,
                matched_null=False,
            )
            null = _agreement_sleeve(
                source,
                component_ids=tuple(row.component_id for row in null_components),
                campaign_id=campaign_id,
                context_feature=context_feature,
                context_operator=null_operator,
                context_quantile=null_quantile,
                matched_null=True,
            )
            if (
                real.behavioral_fingerprint in seed_behavior
                or null.behavioral_fingerprint in seed_behavior
            ):
                raise RuntimeError("agreement generation emitted a seed clone")
            real_sleeves.append(real)
            null_sleeves.append(null)
            for candidate in (real, null):
                source_by_candidate.append((candidate.sleeve_id, source.sleeve_id))
                horizon_by_candidate.append((candidate.sleeve_id, context_feature))

    source_map = dict(source_by_candidate)
    policies, archetypes = _generate_policies(
        tuple(real_sleeves),
        source_by_candidate=source_map,
        campaign_id=campaign_id,
        policy_count=policy_count,
    )
    manifest_payload = {
        "schema": "hydra_directional_agreement_population_v1",
        "campaign_id": campaign_id,
        "class_id": AGREEMENT_CLASS_ID,
        "source_ids": [row.source.sleeve_id for row in sources],
        "real_sleeves": [row.structural_fingerprint for row in real_sleeves],
        "matched_null_sleeves": [
            row.structural_fingerprint for row in null_sleeves
        ],
        "source_by_candidate": sorted(source_by_candidate),
        "horizon_by_candidate": sorted(horizon_by_candidate),
        "policies": [row.structural_fingerprint for row in policies],
        "policy_archetypes": list(archetypes),
        "status_inheritance": False,
        "source_outcomes_from_0007_used": False,
    }
    return DirectionalAgreementPopulation(
        campaign_id=campaign_id,
        sources=sources,
        components=tuple(sorted(components.values(), key=lambda row: row.component_id)),
        real_sleeves=tuple(real_sleeves),
        matched_null_sleeves=tuple(null_sleeves),
        policies=policies,
        policy_archetypes=archetypes,
        source_by_candidate=tuple(sorted(source_by_candidate)),
        horizon_by_candidate=tuple(sorted(horizon_by_candidate)),
        candidate_manifest_hash=stable_hash(manifest_payload),
    )


def _select_sources(
    seed_archive: Mapping[str, Any],
    *,
    excluded: frozenset[str],
    maximum_sources: int,
    maximum_sources_per_market: int,
    maximum_sources_per_market_session: int,
    maximum_sources_per_market_mechanism: int,
    minimum_source_events: int,
) -> tuple[SelectedAgreementSource, ...]:
    pools: dict[str, list[SelectedAgreementSource]] = {}
    seen_behavior: set[str] = set()
    for raw in seed_archive.get("sleeves") or ():
        source = _sleeve_from_dict(raw["specification"])
        evidence = raw["development_evidence"]
        net = float(evidence.get("net_pnl") or 0.0)
        stressed = float(evidence.get("cost_stress_1_5x_net") or 0.0)
        events = int(evidence.get("events") or 0)
        if (
            source.sleeve_id in excluded
            or source.behavioral_fingerprint in seen_behavior
            or net <= 0.0
            or stressed <= 0.0
            or events < minimum_source_events
        ):
            continue
        seen_behavior.add(source.behavioral_fingerprint)
        pools.setdefault(source.market, []).append(
            SelectedAgreementSource(
                source=source,
                net_pnl=net,
                stressed_net_pnl=stressed,
                event_count=events,
                incremental_status=str(
                    evidence.get("incremental_status") or ""
                ),
            )
        )
    for values in pools.values():
        values.sort(
            key=lambda row: (
                row.incremental_status != "MICRO_EDGE_USEFUL",
                -row.stressed_net_pnl,
                -row.event_count,
                row.source.sleeve_id,
            )
        )

    selected: list[SelectedAgreementSource] = []
    market_counts: dict[str, int] = {}
    session_counts: dict[tuple[str, int], int] = {}
    mechanism_counts: dict[tuple[str, str], int] = {}
    markets = sorted(pools)
    cursor = 0
    while markets and len(selected) < maximum_sources:
        market = markets[cursor % len(markets)]
        index = next(
            (
                offset
                for offset, row in enumerate(pools[market])
                if market_counts.get(market, 0) < maximum_sources_per_market
                and session_counts.get((market, row.source.session_code), 0)
                < maximum_sources_per_market_session
                and mechanism_counts.get((market, row.source.trigger_feature), 0)
                < maximum_sources_per_market_mechanism
            ),
            None,
        )
        if index is None:
            markets.remove(market)
            cursor = 0
            continue
        row = pools[market].pop(index)
        selected.append(row)
        market_counts[market] = market_counts.get(market, 0) + 1
        session_key = (market, row.source.session_code)
        mechanism_key = (market, row.source.trigger_feature)
        session_counts[session_key] = session_counts.get(session_key, 0) + 1
        mechanism_counts[mechanism_key] = mechanism_counts.get(mechanism_key, 0) + 1
        cursor += 1
    return tuple(selected)


def _context_variants(
    source: SleeveSpec,
    *,
    seed_behavior: set[str],
    contexts_per_source: int,
    agreement_quantile: float,
    campaign_id: str,
) -> tuple[str, ...]:
    operator, quantile = _aligned_context(source.side, agreement_quantile)
    output: list[str] = []
    for feature in CONTEXT_FEATURES:
        candidate = _agreement_sleeve(
            source,
            component_ids=("structural_probe",),
            campaign_id=campaign_id,
            context_feature=feature,
            context_operator=operator,
            context_quantile=quantile,
            matched_null=False,
        )
        if candidate.behavioral_fingerprint in seed_behavior:
            continue
        output.append(feature)
        if len(output) == contexts_per_source:
            break
    if len(output) != contexts_per_source:
        raise ValueError(
            f"source {source.sleeve_id} has insufficient non-clone contexts"
        )
    return tuple(output)


def _aligned_context(side: int, quantile: float) -> tuple[str, float]:
    return ("GT", quantile) if side == 1 else ("LT", 1.0 - quantile)


def _opposed_context(side: int, quantile: float) -> tuple[str, float]:
    return ("LT", 1.0 - quantile) if side == 1 else ("GT", quantile)


def _agreement_components(
    source: SleeveSpec,
    *,
    campaign_id: str,
    context_feature: str,
    context_operator: str,
    context_quantile: float,
    matched_null: bool,
) -> tuple[ComponentSpec, ...]:
    variant = "OPPOSED_DIRECTION_MATCHED_NULL" if matched_null else "ALIGNED_DIRECTION_REAL"
    timeframe = context_feature.split("_", 2)[1]
    scope = f"session_{source.session_code}"
    common = {
        "mechanism_family": AGREEMENT_CLASS_ID,
        "economic_hypothesis": AGREEMENT_HYPOTHESIS,
        "market_scope": (source.market,),
        "timeframe": timeframe,
        "session_scope": scope,
        "role": source.role,
        "parent_component_ids": source.component_ids,
        "source_campaign": campaign_id,
    }

    def identity(kind: str) -> str:
        return deterministic_id(
            "agreement_component",
            {
                "campaign": campaign_id,
                "source": source.sleeve_id,
                "kind": kind,
                "context_feature": context_feature,
                "context_operator": context_operator,
                "context_quantile": context_quantile,
                "variant": variant,
            },
        )

    context = ComponentSpec(
        component_id=identity("CONTEXT"),
        kind=ComponentKind.CONTEXT,
        input_types=(PortType.FEATURE_SCALAR,),
        output_type=PortType.MARKET_STATE,
        feature_dependencies=(
            FeatureDependency(context_feature, source.market, timeframe),
        ),
        parameters=(
            ("operator", context_operator),
            ("quantile", context_quantile),
            ("variant", variant),
        ),
        failure_target=FailureDimension.UNSTABLE_TEMPORAL_TRANSFER,
        **common,
    )
    trigger = ComponentSpec(
        component_id=identity("TRIGGER"),
        kind=ComponentKind.TRIGGER,
        input_types=(PortType.FEATURE_SCALAR, PortType.MARKET_STATE),
        output_type=PortType.EVENT_TRIGGER,
        feature_dependencies=(
            FeatureDependency(source.trigger_feature, source.market, source.timeframe),
            FeatureDependency(context_feature, source.market, timeframe),
        ),
        parameters=(
            ("source_operator", source.trigger_operator),
            ("source_quantile", source.trigger_quantile),
            ("context_variant", variant),
        ),
        failure_target=FailureDimension.INSUFFICIENT_TARGET_VELOCITY,
        **common,
    )
    direction = ComponentSpec(
        component_id=identity("DIRECTION"),
        kind=ComponentKind.DIRECTION,
        input_types=(PortType.EVENT_TRIGGER,),
        output_type=PortType.DIRECTION,
        parameters=(("side", source.side),),
        failure_target=FailureDimension.HIDDEN_DIRECTIONAL_BETA,
        **common,
    )
    veto = ComponentSpec(
        component_id=identity("TRADE_VETO"),
        kind=ComponentKind.TRADE_VETO,
        input_types=(PortType.MARKET_STATE,),
        output_type=PortType.ELIGIBILITY,
        parameters=(
            ("alignment", "OPPOSED" if matched_null else "ALIGNED"),
            ("closed_bar_only", True),
        ),
        failure_target=FailureDimension.NULL_INDISTINGUISHABLE,
        **common,
    )
    time_exit = ComponentSpec(
        component_id=identity("TIME_EXIT"),
        kind=ComponentKind.TIME_EXIT,
        input_types=(PortType.EVENT_TRIGGER,),
        output_type=PortType.EXIT_POLICY,
        parameters=(("holding_bars", source.holding_bars),),
        failure_target=FailureDimension.WEAK_COST_MARGIN,
        **common,
    )
    role = ComponentSpec(
        component_id=identity("PORTFOLIO_ROLE"),
        kind=ComponentKind.PORTFOLIO_ROLE,
        input_types=(PortType.EVENT_TRIGGER,),
        output_type=PortType.PORTFOLIO_ROLE,
        parameters=(("role", source.role.value),),
        failure_target=FailureDimension.REDUNDANT_PORTFOLIO_ROLE,
        **common,
    )
    return context, trigger, direction, veto, time_exit, role


def _agreement_sleeve(
    source: SleeveSpec,
    *,
    component_ids: tuple[str, ...],
    campaign_id: str,
    context_feature: str,
    context_operator: str,
    context_quantile: float,
    matched_null: bool,
) -> SleeveSpec:
    payload = {
        "campaign": campaign_id,
        "class": AGREEMENT_CLASS_ID,
        "source": source.sleeve_id,
        "context_feature": context_feature,
        "context_operator": context_operator,
        "context_quantile": context_quantile,
        "matched_null": matched_null,
    }
    return SleeveSpec(
        sleeve_id=deterministic_id(
            "agreement_null_sleeve" if matched_null else "agreement_sleeve",
            payload,
        ),
        component_ids=component_ids,
        market=source.market,
        execution_market=source.execution_market,
        timeframe=f"1m|{context_feature.split('_', 2)[1]}",
        session_code=source.session_code,
        trigger_feature=source.trigger_feature,
        trigger_operator=source.trigger_operator,
        trigger_quantile=source.trigger_quantile,
        context_feature=context_feature,
        context_operator=context_operator,
        context_quantile=context_quantile,
        side=source.side,
        holding_bars=source.holding_bars,
        exit_style="TIME_ONLY",
        role=source.role,
        source_campaign=campaign_id,
        lineage_id=deterministic_id(
            "agreement_lineage",
            {
                "class": AGREEMENT_CLASS_ID,
                "source_lineage": source.lineage_id,
                "context": context_feature,
            },
        ),
    )


def _generate_policies(
    sleeves: tuple[SleeveSpec, ...],
    *,
    source_by_candidate: Mapping[str, str],
    campaign_id: str,
    policy_count: int,
) -> tuple[tuple[AccountPolicyGenome, ...], tuple[tuple[str, str], ...]]:
    candidates: list[tuple[str, tuple[SleeveSpec, ...], str]] = []
    profiles = (
        "EQUAL_RISK_DISPERSED",
        "TARGET_VELOCITY_TILTED",
        "CONSISTENCY_TILTED",
    )
    for size in (2, 3, 4):
        for members in itertools.combinations(sleeves, size):
            if len({source_by_candidate[row.sleeve_id] for row in members}) != size:
                continue
            if len({row.market for row in members}) < 2:
                continue
            if len({row.session_code for row in members}) < 2 and len(
                {row.market for row in members}
            ) < 3:
                continue
            if max(_counts(row.trigger_feature for row in members).values()) > 2:
                continue
            if len({row.role for row in members}) < 2:
                continue
            if len({row.context_feature for row in members}) < 2:
                continue
            for profile in profiles:
                key = stable_hash(
                    {
                        "campaign": campaign_id,
                        "members": [row.sleeve_id for row in members],
                        "profile": profile,
                    }
                )
                candidates.append((key, members, profile))
    candidates.sort(key=lambda row: row[0])
    policies: list[AccountPolicyGenome] = []
    archetypes: list[tuple[str, str]] = []
    seen: set[str] = set()
    for _, members, profile in candidates:
        policy = _policy(members, profile=profile, campaign_id=campaign_id)
        if policy.structural_fingerprint in seen:
            continue
        seen.add(policy.structural_fingerprint)
        policies.append(policy)
        archetypes.append((policy.policy_id, profile))
        if len(policies) == policy_count:
            break
    if len(policies) < policy_count:
        raise ValueError(
            f"only {len(policies)} distinct agreement policies for {policy_count} requested"
        )
    return tuple(policies), tuple(archetypes)


def _policy(
    members: tuple[SleeveSpec, ...], *, profile: str, campaign_id: str
) -> AccountPolicyGenome:
    sleeve_ids = tuple(row.sleeve_id for row in members)
    if profile == "EQUAL_RISK_DISPERSED":
        allocations = (1,) * len(members)
        maximum_positions, maximum_mini = min(2, len(members)), 6
        daily_risk, daily_profit = 750.0, 1_500.0
        low_buffer, critical_buffer, loss_streak = 3_000.0, 1_500.0, 3
    elif profile == "TARGET_VELOCITY_TILTED":
        preferred = next(
            (
                index
                for index, row in enumerate(members)
                if row.role
                in {EconomicRole.TARGET_ACCELERATOR, EconomicRole.PRIMARY_ALPHA}
            ),
            0,
        )
        allocations = tuple(
            2 if index == preferred else 1 for index in range(len(members))
        )
        maximum_positions, maximum_mini = min(3, len(members)), 10
        daily_risk, daily_profit = 1_000.0, 2_250.0
        low_buffer, critical_buffer, loss_streak = 3_000.0, 1_500.0, 3
    elif profile == "CONSISTENCY_TILTED":
        allocations = (1,) * len(members)
        maximum_positions, maximum_mini = min(2, len(members)), 5
        daily_risk, daily_profit = 600.0, 1_200.0
        low_buffer, critical_buffer, loss_streak = 3_250.0, 1_750.0, 2
    else:
        raise ValueError(f"unsupported agreement profile: {profile}")
    payload = {
        "campaign": campaign_id,
        "class": AGREEMENT_CLASS_ID,
        "sleeves": sleeve_ids,
        "allocations": allocations,
        "profile": profile,
    }
    return AccountPolicyGenome(
        policy_id=deterministic_id("agreement_policy", payload),
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
        mutation_target=FailureDimension.UNSTABLE_TEMPORAL_TRANSFER,
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
    return output


__all__ = [
    "AGREEMENT_CLASS_ID",
    "AGREEMENT_HYPOTHESIS",
    "CONTEXT_FEATURES",
    "DirectionalAgreementPopulation",
    "SelectedAgreementSource",
    "generate_directional_agreement_population",
]
