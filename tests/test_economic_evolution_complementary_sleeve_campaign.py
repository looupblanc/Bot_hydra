from hydra.research.economic_evolution_complementary_sleeve_campaign import (
    COMPLEMENTARY_SLEEVE_ENGINE_VERSION,
)


def test_complementary_sleeve_campaign_is_manifest_driven() -> None:
    assert (
        COMPLEMENTARY_SLEEVE_ENGINE_VERSION
        == "hydra_complementary_sleeve_campaign_v1"
    )
