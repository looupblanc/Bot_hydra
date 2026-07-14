from __future__ import annotations

from hydra.economic_evolution.account_coverage_three_zone import (
    THREE_ZONE_LIMITS,
    CoverageThreeZonePolicy,
)
from hydra.economic_evolution.account_coverage_three_zone_evaluation import (
    THREE_ZONE_POLICY_VERSION,
)
from hydra.economic_evolution.account_coverage_union_evaluation import (
    CoverageUnionBasketPolicy,
)


def test_three_zone_reuses_exact_shared_account_basket_contract() -> None:
    components = tuple(f"c{i}" for i in range(12))
    policy = CoverageThreeZonePolicy(
        policy_id="real",
        parent_policy_id="parent",
        component_ids=components,
        high_risk_units=3,
        **dict(THREE_ZONE_LIMITS),
    )
    basket = CoverageUnionBasketPolicy(
        policy_id=policy.basket_policy_id,
        component_ids=components,
        archetype="GREEN_COVERAGE_UNION_THREE_ZONE_SIZING",
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=components,
    )
    assert basket.component_ids == policy.component_ids
    assert basket.maximum_simultaneous_positions == 3
    assert THREE_ZONE_POLICY_VERSION == "hydra_coverage_three_zone_policy_v1"
