from hydra.research.economic_evolution_opportunity_replacement_campaign import (
    OPPORTUNITY_REPLACEMENT_ENGINE_VERSION,
)


def test_opportunity_replacement_campaign_is_versioned() -> None:
    assert (
        OPPORTUNITY_REPLACEMENT_ENGINE_VERSION
        == "hydra_opportunity_replacement_campaign_v1"
    )
