from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.economic_evolution.account_state_router import (
    ACCOUNT_STATE_CLASS_ID,
    ACCOUNT_STATE_LIMITS,
    AccountStateRouterPopulation,
    generate_account_state_router_population,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN_ID = "hydra_economic_evolution_account_state_router_0011"


def _seed() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/seeds/"
            "persistent_0003_successor_seed.json"
        ).read_text(encoding="utf-8")
    )


@pytest.fixture(scope="module")
def population() -> AccountStateRouterPopulation:
    return generate_account_state_router_population(
        _seed(), campaign_id=CAMPAIGN_ID, policy_pair_count=96
    )


def test_population_is_deterministic_and_every_membership_is_unique(
    population: AccountStateRouterPopulation,
) -> None:
    repeated = generate_account_state_router_population(
        _seed(), campaign_id=CAMPAIGN_ID, policy_pair_count=96
    )
    assert population.manifest_hash == repeated.manifest_hash
    assert population.summary()["class_id"] == ACCOUNT_STATE_CLASS_ID
    assert population.summary()["real_policy_count"] == 96
    assert population.summary()["unique_membership_count"] == 96
    assert len({row.real_policy.policy_id for row in population.pairs}) == 96
    assert len(
        {row.matched_control_policy.policy_id for row in population.pairs}
    ) == 96


def test_pairs_hold_ordered_membership_state_limits_and_role_multiset(
    population: AccountStateRouterPopulation,
) -> None:
    for pair in population.pairs:
        real = pair.real_policy
        control = pair.matched_control_policy
        assert real.component_ids == control.component_ids
        assert real.component_roles != control.component_roles
        assert sorted(real.component_role_map.values()) == sorted(
            control.component_role_map.values()
        )
        for key, expected in ACCOUNT_STATE_LIMITS.items():
            assert getattr(real, key) == expected
            assert getattr(control, key) == expected
        assert real.outbound_order_capability is False
        assert control.outbound_order_capability is False


def test_component_bank_is_positive_stressed_and_behaviorally_unique(
    population: AccountStateRouterPopulation,
) -> None:
    assert len(population.components) == 48
    assert all(row.net_pnl > 0.0 for row in population.components)
    assert all(row.stressed_net_pnl > 0.0 for row in population.components)
    assert all(row.event_count >= 20 for row in population.components)
    assert len(
        {row.sleeve.behavioral_fingerprint for row in population.components}
    ) == len(population.components)


def test_memberships_are_diverse_and_role_control_is_not_priority_control(
    population: AccountStateRouterPopulation,
) -> None:
    by_id = {row.sleeve.sleeve_id: row for row in population.components}
    for pair in population.pairs:
        members = [by_id[value] for value in pair.real_policy.component_ids]
        assert len(members) in {6, 7, 8}
        assert len({row.sleeve.market for row in members}) >= 3
        assert len({row.sleeve.session_code for row in members}) >= 3
        assert len({row.sleeve.role for row in members}) >= 3
        assert pair.real_policy.component_priority == (
            pair.matched_control_policy.component_priority
        )


def test_full_population_manifest_is_frozen_by_generator() -> None:
    population = generate_account_state_router_population(
        _seed(), campaign_id=CAMPAIGN_ID, policy_pair_count=512
    )
    assert population.manifest_hash == (
        "da61d2638cb9d0f4e95c8dc7d6fffe73e8faed9a422f4add95767e9fc6369301"
    )
    assert population.summary()["same_ordered_membership_pair_count"] == 512
    assert population.summary()["same_role_multiset_pair_count"] == 512


def test_generation_rejects_proof_or_status_inheritance() -> None:
    proof_seed = _seed()
    proof_seed["proof_window_consumed"] = True
    with pytest.raises(ValueError, match="proof-consuming"):
        generate_account_state_router_population(
            proof_seed, campaign_id=CAMPAIGN_ID, policy_pair_count=64
        )

    status_seed = _seed()
    status_seed["governance"]["status_inheritance"] = True
    with pytest.raises(ValueError, match="inheritance"):
        generate_account_state_router_population(
            status_seed, campaign_id=CAMPAIGN_ID, policy_pair_count=64
        )
