from __future__ import annotations

from hydra.research.economic_evolution_coverage_three_zone_campaign import (
    THREE_ZONE_ENGINE_VERSION,
)


def test_three_zone_campaign_is_versioned_and_manifest_driven() -> None:
    assert THREE_ZONE_ENGINE_VERSION == "hydra_coverage_three_zone_campaign_v1"
