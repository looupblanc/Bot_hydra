from hydra.research.economic_evolution_partial_runner_campaign import (
    PARTIAL_RUNNER_ENGINE_VERSION,
)


def test_partial_runner_campaign_engine_is_versioned() -> None:
    assert PARTIAL_RUNNER_ENGINE_VERSION == "hydra_partial_runner_campaign_v1"
