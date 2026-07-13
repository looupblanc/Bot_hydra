from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from hydra.economic_evolution.schema import (
    ComponentKind,
    ComponentSpec,
    EconomicRole,
    FeatureDependency,
    PortType,
    SleeveSpec,
    deterministic_id,
)


DEFAULT_MARKET_PAIRS: Mapping[str, str] = {
    "ES": "MES",
    "NQ": "MNQ",
    "RTY": "M2K",
    "YM": "MYM",
    "GC": "MGC",
    "CL": "MCL",
}

TRIGGER_FEATURES = (
    "old_region_reentry",
    "directional_pressure_without_progress",
    "shared_loss_risk_state",
    "failed_expansion",
    "extreme_dwell",
    "rv_short_long_ratio",
    "past_return_60",
    "past_volatility",
    "past_participation",
)

CONTEXT_FEATURES = (
    "ctx_5m_return",
    "ctx_5m_volatility_expansion",
    "ctx_15m_return",
    "ctx_15m_volatility_expansion",
    "ctx_30m_return",
    "ctx_30m_volatility_expansion",
    "ctx_60m_return",
    "ctx_60m_volatility_expansion",
)

TRIGGER_VARIANTS = (
    ("GT", 0.65),
    ("GT", 0.75),
    ("GT", 0.85),
    ("LT", 0.35),
    ("LT", 0.25),
    ("LT", 0.15),
)

CONTEXT_VARIANTS: tuple[tuple[str | None, str | None, float | None], ...] = (
    (None, None, None),
    *tuple(
        (feature, operator, quantile)
        for feature in CONTEXT_FEATURES
        for operator, quantile in (
            ("GT", 0.65),
            ("GT", 0.75),
            ("LT", 0.35),
            ("LT", 0.25),
        )
    ),
)

SESSIONS = (-1, 0, 1, 2)
HOLDING_HORIZONS = (5, 15, 30, 60)
EXIT_STYLES = (
    "TIME_ONLY",
    "VOL_STOP_TIME_EXIT",
    "STRUCTURAL_INVALIDATION_TIME_EXIT",
    "BOUNDED_TARGET_WITH_RUNNER",
)


FEATURE_MECHANISMS: Mapping[str, tuple[str, str]] = {
    "old_region_reentry": (
        "AUCTION_REENTRY",
        "Participants trapped outside a previously accepted region pay when price re-enters that region and their inventory is unwound.",
    ),
    "directional_pressure_without_progress": (
        "EFFORT_WITHOUT_PROGRESS",
        "Aggressive directional effort without displacement reveals passive opposition whose later release or reversal can create a cost-bearing move.",
    ),
    "shared_loss_risk_state": (
        "SHARED_LOSS_HAZARD",
        "Crowded market states concentrate losses across sleeves, so avoiding them can improve account utility even without standalone alpha.",
    ),
    "failed_expansion": (
        "FAILED_EXPANSION",
        "Participants entering a failed volatility expansion must exit when continuation does not arrive, creating a bounded unwind.",
    ),
    "extreme_dwell": (
        "EXTREME_DWELL",
        "Extended dwell at an auction extreme separates acceptance from rejection and changes continuation versus reversal hazard.",
    ),
    "rv_short_long_ratio": (
        "VOLATILITY_TRANSITION",
        "A change in short-versus-long realized volatility alters the inventory horizon and opportunity density of liquidity providers.",
    ),
    "past_return_60": (
        "INVENTORY_TREND_STATE",
        "Persistent past-only displacement leaves directional inventory that can continue or unwind depending on the accompanying closed context.",
    ),
    "past_volatility": (
        "VOLATILITY_CAPACITY",
        "Past-only volatility changes both executable excursion and adverse-selection cost, creating state-dependent economic capacity.",
    ),
    "past_participation": (
        "PARTICIPATION_TRANSITION",
        "A participation transition changes the density of informed and forced flow, altering target velocity and execution cost.",
    ),
}


@dataclass(frozen=True, slots=True)
class GeneratedPopulation:
    campaign_id: str
    raw_proposal_count: int
    components: tuple[ComponentSpec, ...]
    sleeves: tuple[SleeveSpec, ...]
    duplicate_proposal_count: int
    rejected_incompatible_count: int
    candidate_manifest_hash: str

    @property
    def unique_sleeve_count(self) -> int:
        return len(self.sleeves)

    @property
    def duplicate_rejection_rate(self) -> float:
        return (
            self.duplicate_proposal_count / self.raw_proposal_count
            if self.raw_proposal_count
            else 0.0
        )

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "raw_proposal_count": self.raw_proposal_count,
            "component_count": len(self.components),
            "unique_sleeve_count": len(self.sleeves),
            "duplicate_proposal_count": self.duplicate_proposal_count,
            "duplicate_rejection_rate": self.duplicate_rejection_rate,
            "rejected_incompatible_count": self.rejected_incompatible_count,
            "candidate_manifest_hash": self.candidate_manifest_hash,
        }


def generate_structural_population(
    *,
    campaign_id: str,
    raw_proposal_count: int,
    market_pairs: Mapping[str, str] = DEFAULT_MARKET_PAIRS,
) -> GeneratedPopulation:
    """Generate a balanced deterministic typed population without market outcomes.

    Candidate selection uses a hash-indexed product rather than a threshold grid.
    Repeated proposals are retained in the raw count but rejected before replay by
    a behavioral fingerprint.  No feature values, PnL or future target is read.
    """

    if not campaign_id.strip():
        raise ValueError("campaign_id must be non-empty")
    if raw_proposal_count < 1:
        raise ValueError("raw_proposal_count must be positive")
    if not market_pairs:
        raise ValueError("at least one verified market pair is required")
    markets = tuple(sorted(market_pairs))
    component_by_semantic: dict[str, ComponentSpec] = {}
    sleeves: list[SleeveSpec] = []
    seen_behavior: set[str] = set()
    duplicates = 0
    incompatible = 0

    for proposal_index in range(raw_proposal_count):
        choices = _hashed_choices(campaign_id, proposal_index)
        market = markets[choices[0] % len(markets)]
        trigger_feature = TRIGGER_FEATURES[choices[1] % len(TRIGGER_FEATURES)]
        trigger_operator, trigger_quantile = TRIGGER_VARIANTS[
            choices[2] % len(TRIGGER_VARIANTS)
        ]
        context_feature, context_operator, context_quantile = CONTEXT_VARIANTS[
            choices[3] % len(CONTEXT_VARIANTS)
        ]
        side = (-1, 1)[choices[4] % 2]
        session_code = SESSIONS[choices[5] % len(SESSIONS)]
        holding = HOLDING_HORIZONS[choices[6] % len(HOLDING_HORIZONS)]
        exit_style = EXIT_STYLES[choices[7] % len(EXIT_STYLES)]
        if not _compatible_configuration(
            trigger_feature=trigger_feature,
            context_feature=context_feature,
            holding=holding,
            exit_style=exit_style,
        ):
            incompatible += 1
            continue
        role = _role_for(trigger_feature, market, session_code, exit_style)
        components = _components_for_sleeve(
            campaign_id=campaign_id,
            market=market,
            trigger_feature=trigger_feature,
            trigger_operator=trigger_operator,
            trigger_quantile=trigger_quantile,
            context_feature=context_feature,
            context_operator=context_operator,
            context_quantile=context_quantile,
            side=side,
            session_code=session_code,
            holding=holding,
            exit_style=exit_style,
            role=role,
        )
        component_ids: list[str] = []
        for component in components:
            existing = component_by_semantic.get(component.semantic_fingerprint)
            if existing is None:
                component_by_semantic[component.semantic_fingerprint] = component
                existing = component
            component_ids.append(existing.component_id)
        sleeve_payload = {
            "components": component_ids,
            "market": market,
            "execution_market": market_pairs[market],
            "timeframe": _timeframe_for(context_feature),
            "session": session_code,
            "trigger": [trigger_feature, trigger_operator, trigger_quantile],
            "context": [context_feature, context_operator, context_quantile],
            "side": side,
            "holding": holding,
            "exit_style": exit_style,
            "role": role.value,
            "campaign": campaign_id,
        }
        sleeve = SleeveSpec(
            sleeve_id=deterministic_id("sleeve", sleeve_payload),
            component_ids=tuple(component_ids),
            market=market,
            execution_market=market_pairs[market],
            timeframe=_timeframe_for(context_feature),
            session_code=session_code,
            trigger_feature=trigger_feature,
            trigger_operator=trigger_operator,
            trigger_quantile=trigger_quantile,
            context_feature=context_feature,
            context_operator=context_operator,
            context_quantile=context_quantile,
            side=side,
            holding_bars=holding,
            exit_style=exit_style,
            role=role,
            source_campaign=campaign_id,
            lineage_id=deterministic_id(
                "lineage",
                {
                    "family": FEATURE_MECHANISMS[trigger_feature][0],
                    "market": market,
                    "context": context_feature,
                    "side": side,
                },
            ),
        )
        if sleeve.behavioral_fingerprint in seen_behavior:
            duplicates += 1
            continue
        seen_behavior.add(sleeve.behavioral_fingerprint)
        sleeves.append(sleeve)

    sleeves.sort(key=lambda value: (value.behavioral_fingerprint, value.sleeve_id))
    components = tuple(
        sorted(component_by_semantic.values(), key=lambda value: value.component_id)
    )
    manifest_hash = hashlib.sha256(
        "|".join(value.structural_fingerprint for value in sleeves).encode("ascii")
    ).hexdigest()
    return GeneratedPopulation(
        campaign_id=campaign_id,
        raw_proposal_count=raw_proposal_count,
        components=components,
        sleeves=tuple(sleeves),
        duplicate_proposal_count=duplicates,
        rejected_incompatible_count=incompatible,
        candidate_manifest_hash=manifest_hash,
    )


def _hashed_choices(campaign_id: str, index: int) -> tuple[int, ...]:
    digest = hashlib.sha256(f"{campaign_id}|{index}".encode("utf-8")).digest()
    return tuple(int.from_bytes(digest[offset : offset + 4], "big") for offset in range(0, 32, 4))


def _compatible_configuration(
    *,
    trigger_feature: str,
    context_feature: str | None,
    holding: int,
    exit_style: str,
) -> bool:
    if context_feature is not None and context_feature.removeprefix("ctx_").startswith(
        trigger_feature
    ):
        return False
    if exit_style == "BOUNDED_TARGET_WITH_RUNNER" and holding < 15:
        return False
    if trigger_feature == "shared_loss_risk_state" and exit_style == "BOUNDED_TARGET_WITH_RUNNER":
        return False
    return True


def _role_for(
    feature: str, market: str, session_code: int, exit_style: str
) -> EconomicRole:
    if feature == "shared_loss_risk_state":
        return EconomicRole.MLL_STABILIZER
    if feature == "past_volatility":
        return EconomicRole.CONSISTENCY_SMOOTHER
    if session_code == 2:
        return EconomicRole.SESSION_DIVERSIFIER
    if market in {"GC", "CL"}:
        return EconomicRole.MARKET_DIVERSIFIER
    if exit_style == "BOUNDED_TARGET_WITH_RUNNER":
        return EconomicRole.TARGET_ACCELERATOR
    return EconomicRole.PRIMARY_ALPHA


def _components_for_sleeve(
    *,
    campaign_id: str,
    market: str,
    trigger_feature: str,
    trigger_operator: str,
    trigger_quantile: float,
    context_feature: str | None,
    context_operator: str | None,
    context_quantile: float | None,
    side: int,
    session_code: int,
    holding: int,
    exit_style: str,
    role: EconomicRole,
) -> tuple[ComponentSpec, ...]:
    family, hypothesis = FEATURE_MECHANISMS[trigger_feature]
    output: list[ComponentSpec] = []
    context_id: str | None = None
    if context_feature is not None:
        context_payload = {
            "kind": "CONTEXT",
            "market": market,
            "feature": context_feature,
            "operator": context_operator,
            "quantile": context_quantile,
        }
        context_id = deterministic_id("component", context_payload)
        output.append(
            ComponentSpec(
                component_id=context_id,
                kind=ComponentKind.CONTEXT,
                input_types=(PortType.FEATURE_SCALAR,),
                output_type=PortType.MARKET_STATE,
                mechanism_family="CLOSED_MULTI_TIMEFRAME_CONTEXT",
                economic_hypothesis=(
                    "Closed higher-timeframe state changes which participant inventory "
                    "is exposed to the lower-timeframe event."
                ),
                market_scope=(market,),
                timeframe=_timeframe_for(context_feature),
                session_scope=_session_name(session_code),
                role=role,
                feature_dependencies=(
                    FeatureDependency(
                        name=context_feature,
                        market=market,
                        timeframe=_timeframe_for(context_feature),
                    ),
                ),
                parameters=(
                    ("operator", str(context_operator)),
                    ("quantile", float(context_quantile)),
                ),
                source_campaign=campaign_id,
            )
        )
    trigger_payload = {
        "kind": "TRIGGER",
        "market": market,
        "feature": trigger_feature,
        "operator": trigger_operator,
        "quantile": trigger_quantile,
        "context": context_id,
    }
    trigger_id = deterministic_id("component", trigger_payload)
    output.append(
        ComponentSpec(
            component_id=trigger_id,
            kind=ComponentKind.TRIGGER,
            input_types=(
                (PortType.FEATURE_SCALAR, PortType.MARKET_STATE)
                if context_id
                else (PortType.FEATURE_SCALAR,)
            ),
            output_type=PortType.EVENT_TRIGGER,
            mechanism_family=family,
            economic_hypothesis=hypothesis,
            market_scope=(market,),
            timeframe="1m",
            session_scope=_session_name(session_code),
            role=role,
            feature_dependencies=(
                FeatureDependency(name=trigger_feature, market=market, timeframe="1m"),
            ),
            parameters=(
                ("operator", trigger_operator),
                ("quantile", trigger_quantile),
            ),
            source_campaign=campaign_id,
        )
    )
    direction_id = deterministic_id(
        "component",
        {"kind": "DIRECTION", "trigger": trigger_id, "side": side},
    )
    output.append(
        ComponentSpec(
            component_id=direction_id,
            kind=ComponentKind.DIRECTION,
            input_types=(PortType.EVENT_TRIGGER,),
            output_type=PortType.DIRECTION,
            mechanism_family=family,
            economic_hypothesis=hypothesis,
            market_scope=(market,),
            timeframe="1m",
            session_scope=_session_name(session_code),
            role=role,
            parameters=(("side", side),),
            source_campaign=campaign_id,
        )
    )
    eligibility_id = deterministic_id(
        "component",
        {"kind": "ELIGIBILITY", "market": market, "session": session_code},
    )
    output.append(
        ComponentSpec(
            component_id=eligibility_id,
            kind=ComponentKind.ELIGIBILITY,
            input_types=(PortType.MARKET_STATE,),
            output_type=PortType.ELIGIBILITY,
            mechanism_family="SESSION_OPPORTUNITY_BUDGET",
            economic_hypothesis=(
                "Session-specific participant mandates create different opportunity "
                "and adverse-selection densities."
            ),
            market_scope=(market,),
            timeframe="session",
            session_scope=_session_name(session_code),
            role=role,
            parameters=(("session_code", session_code),),
            source_campaign=campaign_id,
        )
    )
    output.extend(
        _exit_components(
            campaign_id=campaign_id,
            market=market,
            family=family,
            hypothesis=hypothesis,
            trigger_id=trigger_id,
            direction_id=direction_id,
            holding=holding,
            exit_style=exit_style,
            role=role,
            session_code=session_code,
        )
    )
    sizing_id = deterministic_id(
        "component",
        {"kind": "SIZING", "policy": "MLL_BUFFER_BOUNDED_MICRO_FIRST"},
    )
    output.append(
        ComponentSpec(
            component_id=sizing_id,
            kind=ComponentKind.SIZING,
            input_types=(PortType.ACCOUNT_STATE,),
            output_type=PortType.POSITION_SIZE,
            mechanism_family="ACCOUNT_BUFFER_SIZING",
            economic_hypothesis=(
                "Risk scaled to available MLL buffer preserves future opportunity "
                "capacity while retaining positive-net states."
            ),
            market_scope=(market,),
            timeframe="account_state",
            session_scope="ALL",
            role=role,
            parameters=(("policy", "MLL_BUFFER_BOUNDED_MICRO_FIRST"),),
            source_campaign=campaign_id,
        )
    )
    role_id = deterministic_id(
        "component", {"kind": "PORTFOLIO_ROLE", "trigger": trigger_id, "role": role.value}
    )
    output.append(
        ComponentSpec(
            component_id=role_id,
            kind=ComponentKind.PORTFOLIO_ROLE,
            input_types=(PortType.EVENT_TRIGGER,),
            output_type=PortType.PORTFOLIO_ROLE,
            mechanism_family="ACCOUNT_ROLE_ASSIGNMENT",
            economic_hypothesis=(
                "Explicit account roles allow incremental value and redundancy to be "
                "measured against matched policies."
            ),
            market_scope=(market,),
            timeframe="account_state",
            session_scope="ALL",
            role=role,
            parameters=(("role", role.value),),
            source_campaign=campaign_id,
        )
    )
    return tuple(output)


def _exit_components(
    *,
    campaign_id: str,
    market: str,
    family: str,
    hypothesis: str,
    trigger_id: str,
    direction_id: str,
    holding: int,
    exit_style: str,
    role: EconomicRole,
    session_code: int,
) -> tuple[ComponentSpec, ...]:
    common = {
        "mechanism_family": family,
        "economic_hypothesis": hypothesis,
        "market_scope": (market,),
        "timeframe": "1m",
        "session_scope": _session_name(session_code),
        "role": role,
        "source_campaign": campaign_id,
    }
    time_id = deterministic_id(
        "component",
        {"kind": "TIME_EXIT", "trigger": trigger_id, "holding": holding},
    )
    output = [
        ComponentSpec(
            component_id=time_id,
            kind=ComponentKind.TIME_EXIT,
            input_types=(PortType.EVENT_TRIGGER,),
            output_type=PortType.EXIT_POLICY,
            parameters=(("holding_bars", holding),),
            **common,
        )
    ]
    if exit_style in {"VOL_STOP_TIME_EXIT", "STRUCTURAL_INVALIDATION_TIME_EXIT"}:
        stop_id = deterministic_id(
            "component",
            {
                "kind": "STOP",
                "trigger": trigger_id,
                "direction": direction_id,
                "style": exit_style,
            },
        )
        output.append(
            ComponentSpec(
                component_id=stop_id,
                kind=ComponentKind.STOP,
                input_types=(
                    PortType.EVENT_TRIGGER,
                    PortType.DIRECTION,
                    PortType.FEATURE_SCALAR,
                ),
                output_type=PortType.EXIT_POLICY,
                feature_dependencies=(
                    FeatureDependency(name="past_volatility", market=market, timeframe="1m"),
                ),
                parameters=(("style", exit_style),),
                **common,
            )
        )
    if exit_style == "BOUNDED_TARGET_WITH_RUNNER":
        target_id = deterministic_id(
            "component",
            {"kind": "TARGET", "trigger": trigger_id, "style": exit_style},
        )
        output.append(
            ComponentSpec(
                component_id=target_id,
                kind=ComponentKind.TARGET,
                input_types=(
                    PortType.EVENT_TRIGGER,
                    PortType.DIRECTION,
                    PortType.FEATURE_SCALAR,
                ),
                output_type=PortType.EXIT_POLICY,
                feature_dependencies=(
                    FeatureDependency(name="past_volatility", market=market, timeframe="1m"),
                ),
                parameters=(("style", exit_style),),
                **common,
            )
        )
    return tuple(output)


def _timeframe_for(context_feature: str | None) -> str:
    if context_feature is None:
        return "1m"
    return context_feature.split("_", 2)[1]


def _session_name(code: int) -> str:
    return {-1: "ALL", 0: "OPEN", 1: "MIDDLE", 2: "LATE"}[code]


def component_counts_by_kind(
    components: Iterable[ComponentSpec],
) -> dict[str, int]:
    output: dict[str, int] = {}
    for component in components:
        key = component.kind.value
        output[key] = output.get(key, 0) + 1
    return dict(sorted(output.items()))


__all__ = [
    "CONTEXT_FEATURES",
    "DEFAULT_MARKET_PAIRS",
    "EXIT_STYLES",
    "GeneratedPopulation",
    "HOLDING_HORIZONS",
    "SESSIONS",
    "TRIGGER_FEATURES",
    "component_counts_by_kind",
    "generate_structural_population",
]
