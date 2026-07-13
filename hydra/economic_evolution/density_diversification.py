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


DENSITY_CLASS_ID = "INDEPENDENT_OPPORTUNITY_DENSITY_CONSISTENCY_ASSEMBLY_V1"
DENSITY_HYPOTHESIS = (
    "Behaviorally distinct market and session sleeves that each retain positive "
    "stressed development economics can create more temporally dispersed daily "
    "account expectancy, improve consistency and raise independent evidence "
    "density without consuming additional MLL buffer."
)


@dataclass(frozen=True, slots=True)
class SelectedDensitySource:
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
class DensityDiversificationPopulation:
    campaign_id: str
    sources: tuple[SelectedDensitySource, ...]
    components: tuple[ComponentSpec, ...]
    real_sleeves: tuple[SleeveSpec, ...]
    matched_null_sleeves: tuple[SleeveSpec, ...]
    policies: tuple[AccountPolicyGenome, ...]
    policy_archetypes: tuple[tuple[str, str], ...]
    source_by_candidate: tuple[tuple[str, str], ...]
    candidate_manifest_hash: str

    def summary(self) -> dict[str, Any]:
        markets = sorted({row.source.market for row in self.sources})
        sessions = sorted({row.source.session_code for row in self.sources})
        mechanisms = sorted({row.source.trigger_feature for row in self.sources})
        return {
            "campaign_id": self.campaign_id,
            "class_id": DENSITY_CLASS_ID,
            "source_count": len(self.sources),
            "component_count": len(self.components),
            "real_sleeve_count": len(self.real_sleeves),
            "matched_null_sleeve_count": len(self.matched_null_sleeves),
            "account_policy_count": len(self.policies),
            "markets": markets,
            "sessions": sessions,
            "mechanisms": mechanisms,
            "policy_archetype_counts": _counts(dict(self.policy_archetypes).values()),
            "candidate_manifest_hash": self.candidate_manifest_hash,
            "status_inheritance": False,
            "validated": False,
        }


def generate_density_diversification_population(
    seed_archive: Mapping[str, Any],
    *,
    campaign_id: str,
    excluded_source_sleeve_ids: Sequence[str],
    maximum_sources: int = 24,
    maximum_sources_per_market: int = 5,
    maximum_sources_per_market_session: int = 2,
    maximum_sources_per_market_mechanism: int = 2,
    minimum_source_events: int = 24,
    density_quantile: float = 0.65,
    policy_count: int = 192,
) -> DensityDiversificationPopulation:
    """Build one outcome-frozen class reformulation from development components.

    Source selection may use the explicitly development-only seed evidence, but
    candidate construction never reads the new campaign's feature values, PnL or
    account outcomes.  Every real sleeve receives a new past-only opportunity
    density context and a new identity.  The mirrored low-density sleeve is a
    preregistered matched family null and can never enter an account policy.
    """

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if seed_archive.get("development_only") is not True:
        raise ValueError("density assembly requires a development-only seed")
    if seed_archive.get("proof_window_consumed") is not False:
        raise ValueError("proof-consuming seeds cannot drive development assembly")
    if seed_archive.get("governance", {}).get("status_inheritance") is not False:
        raise ValueError("seed status inheritance must be disabled")
    if not 0.5 < density_quantile < 0.9:
        raise ValueError("density quantile must remain in the frozen bounded range")
    if maximum_sources < 4 or policy_count < 1:
        raise ValueError("density population is too small for a substantive decision")

    selected = _select_sources(
        seed_archive,
        excluded=frozenset(str(value) for value in excluded_source_sleeve_ids),
        maximum_sources=maximum_sources,
        maximum_sources_per_market=maximum_sources_per_market,
        maximum_sources_per_market_session=maximum_sources_per_market_session,
        maximum_sources_per_market_mechanism=maximum_sources_per_market_mechanism,
        minimum_source_events=minimum_source_events,
        density_quantile=density_quantile,
    )
    if len({row.source.market for row in selected}) < 2:
        raise ValueError("density assembly needs at least two source markets")

    components: dict[str, ComponentSpec] = {}
    real_sleeves: list[SleeveSpec] = []
    null_sleeves: list[SleeveSpec] = []
    source_by_candidate: list[tuple[str, str]] = []
    for row in selected:
        gate_feature = _density_feature(row.source)
        real_components = _density_components(
            row.source,
            campaign_id=campaign_id,
            gate_feature=gate_feature,
            gate_operator="GT",
            gate_quantile=density_quantile,
            matched_null=False,
        )
        null_components = _density_components(
            row.source,
            campaign_id=campaign_id,
            gate_feature=gate_feature,
            gate_operator="LT",
            gate_quantile=1.0 - density_quantile,
            matched_null=True,
        )
        for component in (*real_components, *null_components):
            components.setdefault(component.component_id, component)
        real = _density_sleeve(
            row.source,
            component_ids=tuple(value.component_id for value in real_components),
            campaign_id=campaign_id,
            gate_feature=gate_feature,
            gate_operator="GT",
            gate_quantile=density_quantile,
            matched_null=False,
        )
        null = _density_sleeve(
            row.source,
            component_ids=tuple(value.component_id for value in null_components),
            campaign_id=campaign_id,
            gate_feature=gate_feature,
            gate_operator="LT",
            gate_quantile=1.0 - density_quantile,
            matched_null=True,
        )
        if real.behavioral_fingerprint == row.source.behavioral_fingerprint:
            raise RuntimeError("density reformulation reproduced its source behavior")
        real_sleeves.append(real)
        null_sleeves.append(null)
        source_by_candidate.extend(
            ((real.sleeve_id, row.source.sleeve_id), (null.sleeve_id, row.source.sleeve_id))
        )

    policies, archetypes = _generate_policies(
        tuple(real_sleeves), campaign_id=campaign_id, policy_count=policy_count
    )
    manifest_payload = {
        "schema": "hydra_density_diversification_population_v1",
        "campaign_id": campaign_id,
        "class_id": DENSITY_CLASS_ID,
        "source_ids": [row.source.sleeve_id for row in selected],
        "real_sleeves": [row.structural_fingerprint for row in real_sleeves],
        "matched_null_sleeves": [row.structural_fingerprint for row in null_sleeves],
        "policies": [row.structural_fingerprint for row in policies],
        "policy_archetypes": list(archetypes),
        "status_inheritance": False,
    }
    return DensityDiversificationPopulation(
        campaign_id=campaign_id,
        sources=selected,
        components=tuple(sorted(components.values(), key=lambda row: row.component_id)),
        real_sleeves=tuple(real_sleeves),
        matched_null_sleeves=tuple(null_sleeves),
        policies=policies,
        policy_archetypes=archetypes,
        source_by_candidate=tuple(sorted(source_by_candidate)),
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
    density_quantile: float,
) -> tuple[SelectedDensitySource, ...]:
    pools: dict[str, list[SelectedDensitySource]] = {}
    seen_behavior: set[str] = set()
    for raw in seed_archive.get("sleeves") or ():
        spec = _sleeve_from_dict(raw["specification"])
        evidence = raw["development_evidence"]
        if spec.sleeve_id in excluded or spec.behavioral_fingerprint in seen_behavior:
            continue
        net = float(evidence.get("net_pnl") or 0.0)
        stressed = float(evidence.get("cost_stress_1_5x_net") or 0.0)
        events = int(evidence.get("events") or 0)
        if net <= 0.0 or stressed <= 0.0 or events < minimum_source_events:
            continue
        feature = _density_feature(spec)
        if (
            spec.context_feature == feature
            and spec.context_operator == "GT"
            and spec.context_quantile == density_quantile
        ):
            continue
        seen_behavior.add(spec.behavioral_fingerprint)
        pools.setdefault(spec.market, []).append(
            SelectedDensitySource(
                source=spec,
                net_pnl=net,
                stressed_net_pnl=stressed,
                event_count=events,
                incremental_status=str(evidence.get("incremental_status") or ""),
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

    selected: list[SelectedDensitySource] = []
    market_counts: dict[str, int] = {}
    market_session_counts: dict[tuple[str, int], int] = {}
    market_mechanism_counts: dict[tuple[str, str], int] = {}
    markets = sorted(pools)
    cursor = 0
    while markets and len(selected) < maximum_sources:
        market = markets[cursor % len(markets)]
        candidate_index = next(
            (
                index
                for index, row in enumerate(pools[market])
                if market_counts.get(market, 0) < maximum_sources_per_market
                and market_session_counts.get((market, row.source.session_code), 0)
                < maximum_sources_per_market_session
                and market_mechanism_counts.get((market, row.source.trigger_feature), 0)
                < maximum_sources_per_market_mechanism
            ),
            None,
        )
        if candidate_index is None:
            markets.remove(market)
            cursor = 0
            continue
        row = pools[market].pop(candidate_index)
        selected.append(row)
        market_counts[market] = market_counts.get(market, 0) + 1
        session_key = (market, row.source.session_code)
        mechanism_key = (market, row.source.trigger_feature)
        market_session_counts[session_key] = market_session_counts.get(session_key, 0) + 1
        market_mechanism_counts[mechanism_key] = market_mechanism_counts.get(mechanism_key, 0) + 1
        cursor += 1
    return tuple(selected)


def _density_components(
    source: SleeveSpec,
    *,
    campaign_id: str,
    gate_feature: str,
    gate_operator: str,
    gate_quantile: float,
    matched_null: bool,
) -> tuple[ComponentSpec, ...]:
    suffix = "MATCHED_LOW_DENSITY_NULL" if matched_null else "HIGH_DENSITY_REAL"
    scope = f"session_{source.session_code}"
    common = {
        "mechanism_family": DENSITY_CLASS_ID,
        "economic_hypothesis": DENSITY_HYPOTHESIS,
        "market_scope": (source.market,),
        "timeframe": source.timeframe,
        "session_scope": scope,
        "role": source.role,
        "parent_component_ids": source.component_ids,
        "source_campaign": campaign_id,
    }

    def identity(kind: str) -> str:
        return deterministic_id(
            "density_component",
            {
                "campaign": campaign_id,
                "source": source.sleeve_id,
                "kind": kind,
                "gate_feature": gate_feature,
                "gate_operator": gate_operator,
                "gate_quantile": gate_quantile,
                "variant": suffix,
            },
        )

    context = ComponentSpec(
        component_id=identity("CONTEXT"),
        kind=ComponentKind.CONTEXT,
        input_types=(PortType.FEATURE_SCALAR,),
        output_type=PortType.MARKET_STATE,
        feature_dependencies=(FeatureDependency(gate_feature, source.market, source.timeframe),),
        parameters=(
            ("operator", gate_operator),
            ("quantile", gate_quantile),
            ("variant", suffix),
        ),
        failure_target=FailureDimension.INSUFFICIENT_STATISTICAL_POWER,
        **common,
    )
    trigger = ComponentSpec(
        component_id=identity("TRIGGER"),
        kind=ComponentKind.TRIGGER,
        input_types=(PortType.FEATURE_SCALAR, PortType.MARKET_STATE),
        output_type=PortType.EVENT_TRIGGER,
        feature_dependencies=(
            FeatureDependency(source.trigger_feature, source.market, source.timeframe),
            FeatureDependency(gate_feature, source.market, source.timeframe),
        ),
        parameters=(
            ("source_operator", source.trigger_operator),
            ("source_quantile", source.trigger_quantile),
            ("density_variant", suffix),
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
        failure_target=FailureDimension.CONSISTENCY_RULE_FAILURE,
        **common,
    )
    return context, trigger, direction, time_exit, role


def _density_sleeve(
    source: SleeveSpec,
    *,
    component_ids: tuple[str, ...],
    campaign_id: str,
    gate_feature: str,
    gate_operator: str,
    gate_quantile: float,
    matched_null: bool,
) -> SleeveSpec:
    payload = {
        "campaign": campaign_id,
        "class": DENSITY_CLASS_ID,
        "source": source.sleeve_id,
        "gate_feature": gate_feature,
        "gate_operator": gate_operator,
        "gate_quantile": gate_quantile,
        "matched_null": matched_null,
    }
    prefix = "density_null_sleeve" if matched_null else "density_sleeve"
    return SleeveSpec(
        sleeve_id=deterministic_id(prefix, payload),
        component_ids=component_ids,
        market=source.market,
        execution_market=source.execution_market,
        timeframe=source.timeframe,
        session_code=source.session_code,
        trigger_feature=source.trigger_feature,
        trigger_operator=source.trigger_operator,
        trigger_quantile=source.trigger_quantile,
        context_feature=gate_feature,
        context_operator=gate_operator,
        context_quantile=gate_quantile,
        side=source.side,
        holding_bars=source.holding_bars,
        exit_style="TIME_ONLY",
        role=source.role,
        source_campaign=campaign_id,
        lineage_id=deterministic_id(
            "density_lineage",
            {"class": DENSITY_CLASS_ID, "source_lineage": source.lineage_id},
        ),
    )


def _generate_policies(
    sleeves: tuple[SleeveSpec, ...], *, campaign_id: str, policy_count: int
) -> tuple[tuple[AccountPolicyGenome, ...], tuple[tuple[str, str], ...]]:
    candidates: list[tuple[str, tuple[SleeveSpec, ...], str]] = []
    profiles = (
        "EQUAL_RISK_DISPERSED",
        "TARGET_VELOCITY_TILTED",
        "CONSISTENCY_TILTED",
    )
    for size in (2, 3, 4):
        for members in itertools.combinations(sleeves, size):
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
        genome = _policy(members, profile=profile, campaign_id=campaign_id)
        if genome.structural_fingerprint in seen:
            continue
        seen.add(genome.structural_fingerprint)
        policies.append(genome)
        archetypes.append((genome.policy_id, profile))
        if len(policies) >= policy_count:
            break
    if len(policies) < policy_count:
        raise ValueError(
            f"only {len(policies)} distinct density policies for requested {policy_count}"
        )
    return tuple(policies), tuple(archetypes)


def _policy(
    members: tuple[SleeveSpec, ...], *, profile: str, campaign_id: str
) -> AccountPolicyGenome:
    sleeve_ids = tuple(row.sleeve_id for row in members)
    if profile == "EQUAL_RISK_DISPERSED":
        allocations = (1,) * len(members)
        maximum_positions = min(2, len(members))
        maximum_mini = 6
        daily_risk = 750.0
        daily_profit = 1_500.0
        low_buffer = 3_000.0
        critical_buffer = 1_500.0
        loss_streak = 3
    elif profile == "TARGET_VELOCITY_TILTED":
        preferred = next(
            (
                index
                for index, row in enumerate(members)
                if row.role in {EconomicRole.TARGET_ACCELERATOR, EconomicRole.PRIMARY_ALPHA}
            ),
            0,
        )
        allocations = tuple(2 if index == preferred else 1 for index in range(len(members)))
        maximum_positions = min(3, len(members))
        maximum_mini = 10
        daily_risk = 1_000.0
        daily_profit = 2_250.0
        low_buffer = 3_000.0
        critical_buffer = 1_500.0
        loss_streak = 3
    elif profile == "CONSISTENCY_TILTED":
        allocations = (1,) * len(members)
        maximum_positions = min(2, len(members))
        maximum_mini = 5
        daily_risk = 600.0
        daily_profit = 1_200.0
        low_buffer = 3_250.0
        critical_buffer = 1_750.0
        loss_streak = 2
    else:
        raise ValueError(f"unsupported density policy profile: {profile}")
    payload = {
        "campaign": campaign_id,
        "class": DENSITY_CLASS_ID,
        "sleeves": sleeve_ids,
        "allocations": allocations,
        "profile": profile,
    }
    return AccountPolicyGenome(
        policy_id=deterministic_id("density_policy", payload),
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
        mutation_target=FailureDimension.INSUFFICIENT_STATISTICAL_POWER,
    )


def _density_feature(source: SleeveSpec) -> str:
    return (
        "ctx_60m_volatility_expansion"
        if source.trigger_feature == "past_participation"
        else "past_participation"
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
            None if value.get("context_feature") is None else str(value["context_feature"])
        ),
        context_operator=(
            None if value.get("context_operator") is None else str(value["context_operator"])
        ),
        context_quantile=(
            None if value.get("context_quantile") is None else float(value["context_quantile"])
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
    "DENSITY_CLASS_ID",
    "DENSITY_HYPOTHESIS",
    "DensityDiversificationPopulation",
    "SelectedDensitySource",
    "generate_density_diversification_population",
]
