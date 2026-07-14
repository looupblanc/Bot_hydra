from __future__ import annotations

from hydra.economic_evolution.account_coverage_sizing import (
    BUFFER_SIZING_LIMITS,
    CoverageSizingPolicy,
)
from hydra.economic_evolution.account_coverage_sizing_evaluation import (
    COVERAGE_SIZING_POLICY_VERSION,
)
from hydra.economic_evolution.account_coverage_union_evaluation import (
    CoverageUnionBasketPolicy,
)


def test_coverage_sizing_reuses_exact_broad_shared_account_basket() -> None:
    components = tuple(f"c{i}" for i in range(12))
    policy = CoverageSizingPolicy(
        policy_id="real",
        parent_policy_id="parent",
        component_ids=components,
        accelerate_risk_units=2,
        **dict(BUFFER_SIZING_LIMITS),
    )
    basket = CoverageUnionBasketPolicy(
        policy_id=policy.basket_policy_id,
        component_ids=components,
        archetype="GREEN_COVERAGE_UNION_BUFFER_AWARE_SIZING",
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=components,
    )
    assert basket.component_ids == policy.component_ids
    assert basket.maximum_simultaneous_positions == 3
    assert COVERAGE_SIZING_POLICY_VERSION == "hydra_coverage_sizing_policy_v1"
