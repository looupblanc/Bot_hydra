from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    COMPLEMENTARY_SLEEVE_POLICY_VERSION,
)


def test_complementary_sleeve_evaluator_is_versioned() -> None:
    assert (
        COMPLEMENTARY_SLEEVE_POLICY_VERSION
        == "hydra_complementary_sleeve_policy_v1"
    )
