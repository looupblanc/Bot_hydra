from hydra.economic_evolution.account_opportunity_replacement import (
    PARENT_POPULATION_MANIFEST_HASH,
    generate_opportunity_replacement_population,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


def test_opportunity_replacement_changes_exactly_one_frozen_component() -> None:
    seed = load_and_verify_seed_archive(
        "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"
    )
    population = generate_opportunity_replacement_population(
        seed,
        campaign_id="campaign-0020",
        parent_campaign_id="hydra_economic_evolution_three_zone_sizing_0016",
        sizing_parent_campaign_id="hydra_economic_evolution_buffer_sizing_0015",
        coverage_parent_campaign_id=(
            "hydra_economic_evolution_coverage_union_0014"
        ),
        policy_pair_count=512,
        maximum_components=48,
        minimum_component_events=20,
    )
    assert population.parent_population_manifest_hash == PARENT_POPULATION_MANIFEST_HASH
    assert len(population.pairs) == 512
    assert len({row.real_policy.structural_fingerprint for row in population.pairs}) == 512
    for row in population.pairs:
        assert len(row.real_policy.component_ids) == len(
            row.matched_control_policy.component_ids
        )
        assert set(row.real_policy.component_ids) ^ set(
            row.matched_control_policy.component_ids
        ) == {row.removed_sleeve_id, row.replacement_sleeve_id}
        assert (
            row.real_policy.component_ids[-1]
            == row.retained_complementary_sleeve_id
        )
