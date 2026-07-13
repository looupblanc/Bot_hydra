from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.economic_evolution.generator import generate_structural_population
from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    ComponentKind,
    ComponentSpec,
    EconomicRole,
    FeatureDependency,
    PortType,
    SleeveSpec,
    reject_duplicate_fingerprints,
)


def _context() -> ComponentSpec:
    return ComponentSpec(
        component_id="component_context",
        kind=ComponentKind.CONTEXT,
        input_types=(PortType.FEATURE_SCALAR,),
        output_type=PortType.MARKET_STATE,
        mechanism_family="TEST_CONTEXT",
        economic_hypothesis="Closed context changes the paying participant set.",
        market_scope=("ES",),
        timeframe="15m",
        session_scope="OPEN",
        role=EconomicRole.PRIMARY_ALPHA,
        feature_dependencies=(
            FeatureDependency("ctx_15m_return", "ES", "15m"),
        ),
        parameters=(("operator", "GT"), ("quantile", 0.75)),
    )


def test_component_interfaces_and_feature_availability_fail_closed() -> None:
    component = _context()
    assert len(component.structural_fingerprint) == 64
    assert component.inherited_status is None
    with pytest.raises(ValueError, match="invalid interface"):
        replace(component, output_type=PortType.DIRECTION)
    with pytest.raises(ValueError, match="closed or past-only"):
        FeatureDependency(
            "ctx_15m_return", "ES", "15m", availability="FUTURE_BAR"
        )
    with pytest.raises(ValueError, match="at most four"):
        replace(
            component,
            parameters=(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)),
        )


def test_behavioral_fingerprint_rejects_role_and_micro_clone_inflation() -> None:
    population = generate_structural_population(
        campaign_id="fingerprint_test", raw_proposal_count=200
    )
    sleeve = population.sleeves[0]
    clone = replace(
        sleeve,
        sleeve_id="renamed_clone",
        execution_market="ES",
        role=EconomicRole.SECONDARY_ALPHA,
    )
    assert clone.structural_fingerprint != sleeve.structural_fingerprint
    assert clone.behavioral_fingerprint == sleeve.behavioral_fingerprint
    retained, duplicate_indices = reject_duplicate_fingerprints(
        (sleeve, clone), semantic=True
    )
    assert retained == (sleeve,)
    assert duplicate_indices == (1,)


def test_account_policy_rejects_unbounded_or_duplicate_construction() -> None:
    policy = AccountPolicyGenome(
        policy_id="policy",
        sleeve_ids=("s1", "s2"),
        allocation_units=(1, 2),
        maximum_simultaneous_positions=2,
        maximum_mini_equivalent=6,
        conflict_policy="FIXED_PRIORITY",
        daily_risk_budget=1_250.0,
        daily_profit_lock=2_250.0,
        low_mll_buffer=3_000.0,
        critical_mll_buffer=1_125.0,
        loss_streak_throttle_after=3,
        mode="COMBINE_RESEARCH",
        source_campaign="test",
    )
    assert len(policy.structural_fingerprint) == 64
    assert policy.inherited_status is None
    with pytest.raises(ValueError, match="unique"):
        replace(policy, sleeve_ids=("s1", "s1"))
    with pytest.raises(ValueError, match="bounded discrete"):
        replace(policy, allocation_units=(1, 99))
    with pytest.raises(ValueError, match=r"\[1,15\]"):
        replace(policy, maximum_mini_equivalent=16)

    eight = tuple(f"sleeve_{index}" for index in range(8))
    expanded = replace(
        policy,
        sleeve_ids=eight,
        allocation_units=(1,) * 8,
        maximum_simultaneous_positions=4,
    )
    assert len(expanded.sleeve_ids) == 8
    with pytest.raises(ValueError, match="one to eight"):
        replace(
            expanded,
            sleeve_ids=(*eight, "sleeve_8"),
            allocation_units=(1,) * 9,
        )


def test_sleeve_requires_closed_bounded_execution_fields() -> None:
    population = generate_structural_population(
        campaign_id="sleeve_validation", raw_proposal_count=100
    )
    sleeve: SleeveSpec = population.sleeves[0]
    with pytest.raises(ValueError, match="canonical frozen horizon"):
        replace(sleeve, holding_bars=17)
    with pytest.raises(ValueError, match="context fields"):
        replace(sleeve, context_feature="ctx_15m_return", context_operator=None)
