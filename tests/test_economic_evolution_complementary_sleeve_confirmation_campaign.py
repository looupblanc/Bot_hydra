from __future__ import annotations

import json
from pathlib import Path

from hydra.research.economic_evolution_complementary_sleeve_confirmation_campaign import (
    FROZEN_POPULATION_CAMPAIGN_ID,
    _generate_frozen_population,
)


ROOT = Path(__file__).resolve().parents[1]


def test_confirmation_reuses_exact_0017_policy_identities() -> None:
    seed = json.loads(
        (
            ROOT
            / "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"
        ).read_text()
    )
    population = _generate_frozen_population(
        seed,
        campaign_id="confirmation_0018",
        parent_campaign_id="hydra_economic_evolution_three_zone_sizing_0016",
        coverage_parent_campaign_id=(
            "hydra_economic_evolution_coverage_union_0014"
        ),
        policy_pair_count=512,
        maximum_components=48,
        minimum_component_events=20,
    )
    assert population.campaign_id == FROZEN_POPULATION_CAMPAIGN_ID
    assert population.manifest_hash == (
        "6c84ababed3f8c331cbb3e892eca211510e4cc10b3b163a7701395d083835781"
    )
