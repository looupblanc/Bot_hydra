from __future__ import annotations

from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_coverage_union_evaluation import (
    CoverageUnionBasketPolicy,
)


def test_coverage_union_basket_supports_broad_static_membership() -> None:
    components = tuple(f"c{index}" for index in range(12))
    basket = CoverageUnionBasketPolicy(
        policy_id="coverage_basket",
        component_ids=components,
        archetype="CROSS_MARKET_SESSION_COVERAGE_UNION",
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=components,
    )
    assert basket.kind is AccountPolicyKind.STATIC_BASKET
    assert len(basket.component_ids) == 12
    assert basket.to_dict()["kind"] == "STATIC_ACCOUNT_BASKET"
