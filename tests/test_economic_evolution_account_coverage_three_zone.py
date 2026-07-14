from __future__ import annotations

from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_coverage_three_zone import (
    THREE_ZONE_LIMITS,
    CoverageThreeZonePolicy,
    generate_coverage_three_zone_population,
    route_coverage_three_zone_entry,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


def _policy(high_units: int) -> CoverageThreeZonePolicy:
    return CoverageThreeZonePolicy(
        policy_id=f"policy-{high_units}",
        parent_policy_id="parent",
        component_ids=tuple(f"c{i}" for i in range(10)),
        high_risk_units=high_units,
        **dict(THREE_ZONE_LIMITS),
    )


def test_three_zone_router_changes_only_frozen_high_zone() -> None:
    intent = EntryIntent("e", "c0", "ES", 1, 1, 1, "RTH", 1, 1.0)
    high = AccountDecisionState(0.0, -4500.0, 4500.0, 0.0, 0, 9000.0, ())
    middle = AccountDecisionState(3500.0, -1000.0, 3500.0, 0.0, 0, 5500.0, ())
    base = AccountDecisionState(7000.0, 2500.0, 4500.0, 0.0, 0, 2000.0, ())
    assert route_coverage_three_zone_entry(
        intent, high, policy=_policy(3)
    ).quantity == 3
    assert route_coverage_three_zone_entry(
        intent, high, policy=_policy(2)
    ).quantity == 2
    assert route_coverage_three_zone_entry(
        intent, middle, policy=_policy(3)
    ).quantity == 2
    assert route_coverage_three_zone_entry(
        intent, middle, policy=_policy(2)
    ).quantity == 2
    assert route_coverage_three_zone_entry(
        intent, base, policy=_policy(3)
    ).quantity == 1


def test_three_zone_population_is_deterministic_and_preserves_0015_parent() -> None:
    seed = load_and_verify_seed_archive(
        "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"
    )
    kwargs = {
        "campaign_id": "campaign-0016",
        "parent_campaign_id": "hydra_economic_evolution_buffer_sizing_0015",
        "coverage_parent_campaign_id": (
            "hydra_economic_evolution_coverage_union_0014"
        ),
        "policy_pair_count": 512,
        "maximum_components": 48,
        "minimum_component_events": 20,
    }
    first = generate_coverage_three_zone_population(seed, **kwargs)
    second = generate_coverage_three_zone_population(seed, **kwargs)
    assert first.manifest_hash == second.manifest_hash
    assert len(first.pairs) == 512
    assert len({row.parent_policy_id for row in first.pairs}) == 512
    assert all(row.real_policy.high_risk_units == 3 for row in first.pairs)
    assert all(
        row.matched_control_policy.high_risk_units == 2
        for row in first.pairs
    )
    assert first.summary()["duplicate_control_definition_count"] == 0
