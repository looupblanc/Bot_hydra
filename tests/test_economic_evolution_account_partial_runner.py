from __future__ import annotations

from hydra.economic_evolution.account_partial_runner import (
    MATCHED_CONTROL_EXIT,
    PARTIAL_RUNNER_EXIT,
    PARENT_POPULATION_MANIFEST_HASH,
    generate_partial_runner_population,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


def test_partial_runner_population_changes_only_frozen_exit_representation() -> None:
    seed = load_and_verify_seed_archive(
        "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"
    )
    kwargs = {
        "campaign_id": "campaign-0019",
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
    first = generate_partial_runner_population(seed, **kwargs)
    second = generate_partial_runner_population(seed, **kwargs)
    assert first.manifest_hash == second.manifest_hash
    assert first.parent_population_manifest_hash == PARENT_POPULATION_MANIFEST_HASH
    assert len(first.pairs) == 512
    assert len({row.parent_policy_id for row in first.pairs}) == 512
    assert len({row.real_policy.structural_fingerprint for row in first.pairs}) == 512
    assert all(
        row.real_policy.component_ids == row.matched_control_policy.component_ids
        for row in first.pairs
    )
    assert all(
        row.real_policy.exit_representation == PARTIAL_RUNNER_EXIT
        and row.matched_control_policy.exit_representation == MATCHED_CONTROL_EXIT
        for row in first.pairs
    )
    assert all(
        row.real_policy.mutated_sleeve_id == row.real_policy.component_ids[-1]
        for row in first.pairs
    )
    assert first.summary()["duplicate_control_definition_count"] == 0
    assert first.summary()["status_inheritance"] is False
