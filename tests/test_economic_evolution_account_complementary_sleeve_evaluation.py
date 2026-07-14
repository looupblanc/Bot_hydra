from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    COMPLEMENTARY_SLEEVE_POLICY_VERSION,
    ComplementarySleeveBasketPolicy,
)


def test_complementary_sleeve_evaluator_is_versioned() -> None:
    assert (
        COMPLEMENTARY_SLEEVE_POLICY_VERSION
        == "hydra_complementary_sleeve_policy_v1"
    )


def test_complementary_basket_accepts_frozen_thirteen_sleeves() -> None:
    components = tuple(f"c{i}" for i in range(13))
    basket = ComplementarySleeveBasketPolicy(
        policy_id="basket",
        component_ids=components,
        archetype="GREEN_THREE_ZONE_COMPLEMENTARY_SLEEVE",
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=components,
    )
    assert len(basket.component_ids) == 13
    assert basket.maximum_simultaneous_positions == 3
