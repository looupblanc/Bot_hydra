from __future__ import annotations

from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_coverage_sizing import (
    BUFFER_SIZING_LIMITS,
    CoverageSizingPolicy,
    generate_coverage_sizing_population,
    route_coverage_sizing_entry,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


def _policy(units: int) -> CoverageSizingPolicy:
    return CoverageSizingPolicy(
        policy_id=f"policy-{units}",
        parent_policy_id="parent",
        component_ids=tuple(f"c{i}" for i in range(10)),
        accelerate_risk_units=units,
        **dict(BUFFER_SIZING_LIMITS),
    )


def test_buffer_sizing_accelerates_only_inside_frozen_safe_state() -> None:
    intent = EntryIntent("e", "c0", "ES", 1, 1, 1, "RTH", 1, 1.0)
    safe = AccountDecisionState(0.0, -4500.0, 4500.0, 0.0, 0, 9000.0, ())
    late = AccountDecisionState(7000.0, 2500.0, 4500.0, 0.0, 0, 2000.0, ())
    danger = AccountDecisionState(-1600.0, -4500.0, 2900.0, 0.0, 0, 9000.0, ())
    assert route_coverage_sizing_entry(intent, safe, policy=_policy(2)).quantity == 2
    assert route_coverage_sizing_entry(intent, late, policy=_policy(2)).quantity == 1
    assert route_coverage_sizing_entry(intent, danger, policy=_policy(2)).quantity == 1
    assert route_coverage_sizing_entry(intent, safe, policy=_policy(1)).quantity == 1


def test_sizing_population_is_deterministic_and_has_unique_parents() -> None:
    seed = load_and_verify_seed_archive(
        "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"
    )
    kwargs = {
        "campaign_id": "campaign-0015",
        "parent_campaign_id": "hydra_economic_evolution_coverage_union_0014",
        "policy_pair_count": 512,
        "maximum_components": 48,
        "minimum_component_events": 20,
    }
    first = generate_coverage_sizing_population(seed, **kwargs)
    second = generate_coverage_sizing_population(seed, **kwargs)
    assert first.manifest_hash == second.manifest_hash
    assert len(first.pairs) == 512
    assert len({row.parent_policy_id for row in first.pairs}) == 512
    assert all(row.real_policy.accelerate_risk_units == 2 for row in first.pairs)
    assert all(
        row.matched_control_policy.accelerate_risk_units == 1
        for row in first.pairs
    )
    assert first.summary()["duplicate_control_definition_count"] == 0
