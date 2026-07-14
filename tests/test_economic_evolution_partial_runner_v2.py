from hydra.economic_evolution.account_partial_runner_evaluation_v2 import (
    MUTATED_PARTIAL_RUNNER_SLEEVE_IDS,
)
from hydra.research.economic_evolution_partial_runner_campaign_v2 import (
    PARTIAL_RUNNER_ENGINE_VERSION_V2,
)


def test_partial_runner_v2_scopes_volatility_exit_to_frozen_five_sleeves() -> None:
    assert MUTATED_PARTIAL_RUNNER_SLEEVE_IDS == {
        "sleeve_65bad2088913fc9fca0a145d",
        "sleeve_9f99649247c698bf206d0507",
        "sleeve_b8e98b3a73cacb8a105a3116",
        "sleeve_c5da4b5a67abadeb7d68eabe",
        "sleeve_fe3e0de298753596c2459cf4",
    }
    assert PARTIAL_RUNNER_ENGINE_VERSION_V2 == "hydra_partial_runner_campaign_v2"
