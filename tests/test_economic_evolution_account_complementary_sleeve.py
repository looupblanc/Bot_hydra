from __future__ import annotations

from hydra.economic_evolution.account_complementary_sleeve import (
    generate_complementary_sleeve_population,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


def test_complementary_population_adds_exactly_one_frozen_sleeve() -> None:
    seed = load_and_verify_seed_archive(
        "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"
    )
    kwargs = {
        "campaign_id": "campaign-0017",
        "parent_campaign_id": "hydra_economic_evolution_three_zone_sizing_0016",
        "sizing_parent_campaign_id": (
            "hydra_economic_evolution_buffer_sizing_0015"
        ),
        "coverage_parent_campaign_id": (
            "hydra_economic_evolution_coverage_union_0014"
        ),
        "policy_pair_count": 512,
        "maximum_components": 48,
        "minimum_component_events": 20,
    }
    first = generate_complementary_sleeve_population(seed, **kwargs)
    second = generate_complementary_sleeve_population(seed, **kwargs)
    assert first.manifest_hash == second.manifest_hash
    assert len(first.pairs) == 512
    assert len({row.parent_policy_id for row in first.pairs}) == 512
    assert all(
        len(row.real_policy.component_ids)
        == len(row.matched_control_policy.component_ids) + 1
        for row in first.pairs
    )
    assert all(
        row.real_policy.component_ids[-1] == row.added_sleeve_id
        for row in first.pairs
    )
    assert all(row.real_policy.high_risk_units == 3 for row in first.pairs)
    assert first.summary()["duplicate_control_definition_count"] == 0
