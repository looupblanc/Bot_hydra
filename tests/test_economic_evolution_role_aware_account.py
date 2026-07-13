from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.economic_evolution.role_aware_account import (
    ROLE_AWARE_CLASS_ID,
    ROLE_ORDER,
    RoleAwareAccountPolicyGenome,
    RoleAwareAccountPopulation,
    generate_role_aware_account_population,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN_ID = "hydra_economic_evolution_role_aware_account_allocator_0010"


def _seed() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/seeds/"
            "persistent_0003_successor_seed.json"
        ).read_text(encoding="utf-8")
    )


@pytest.fixture(scope="module")
def population() -> RoleAwareAccountPopulation:
    return generate_role_aware_account_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=96,
    )


def test_role_aware_population_is_deterministic_and_structurally_distinct(
    population: RoleAwareAccountPopulation,
) -> None:
    repeated = generate_role_aware_account_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=96,
    )
    assert population.manifest_hash == repeated.manifest_hash
    assert population.summary()["class_id"] == ROLE_AWARE_CLASS_ID
    assert population.summary()["real_policy_count"] == 96
    assert population.summary()["matched_control_policy_count"] == 96
    assert len({row.real_policy.policy_id for row in population.pairs}) == 96
    assert len(
        {row.matched_control_policy.policy_id for row in population.pairs}
    ) == 96


def test_pairs_hold_membership_limits_and_risk_multiset_fixed(
    population: RoleAwareAccountPopulation,
) -> None:
    for pair in population.pairs:
        real = pair.real_policy
        control = pair.matched_control_policy
        assert set(real.sleeve_ids) == set(control.sleeve_ids)
        assert real.sleeve_ids != control.sleeve_ids
        assert sorted(real.allocation_units) == sorted(control.allocation_units)
        assert sum(real.allocation_units) == sum(control.allocation_units)
        assert real.maximum_simultaneous_positions == (
            control.maximum_simultaneous_positions
        )
        assert real.maximum_mini_equivalent == control.maximum_mini_equivalent
        assert real.daily_risk_budget == control.daily_risk_budget
        assert real.daily_profit_lock == control.daily_profit_lock
        assert real.low_mll_buffer == control.low_mll_buffer
        assert real.critical_mll_buffer == control.critical_mll_buffer
        assert real.loss_streak_throttle_after == (
            control.loss_streak_throttle_after
        )
        assert pair.to_dict()["identical_account_limits"] is True


def test_real_priority_is_frozen_by_role_and_membership_is_diverse(
    population: RoleAwareAccountPopulation,
) -> None:
    by_id = {row.sleeve.sleeve_id: row for row in population.components}
    role_rank = {role: index for index, role in enumerate(ROLE_ORDER)}
    for pair in population.pairs:
        members = [by_id[value] for value in pair.real_policy.sleeve_ids]
        ranks = [role_rank[row.sleeve.role] for row in members]
        assert ranks == sorted(ranks)
        assert len(members) in {6, 7, 8}
        assert len({row.sleeve.market for row in members}) >= 3
        assert len({row.sleeve.session_code for row in members}) >= 3
        assert len({row.sleeve.role for row in members}) >= 3


def test_component_bank_is_positive_cost_resilient_and_behaviorally_unique(
    population: RoleAwareAccountPopulation,
) -> None:
    assert len(population.components) == 48
    assert all(row.net_pnl > 0.0 for row in population.components)
    assert all(row.stressed_net_pnl > 0.0 for row in population.components)
    assert all(row.event_count >= 20 for row in population.components)
    assert len(
        {row.sleeve.behavioral_fingerprint for row in population.components}
    ) == len(population.components)


def test_full_preregistered_population_has_frozen_manifest() -> None:
    population = generate_role_aware_account_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=512,
    )
    assert population.manifest_hash == (
        "f43aa93d75392232cb69e1a768a3856f1102adc768f5e0d27cfa7ffad347f88a"
    )
    assert population.summary()["same_membership_pair_count"] == 512
    assert population.summary()["same_risk_unit_multiset_pair_count"] == 512


def test_role_aware_genome_is_local_and_bounded_to_six_through_eight() -> None:
    population = generate_role_aware_account_population(
        _seed(), campaign_id=CAMPAIGN_ID, policy_pair_count=64
    )
    policy = population.real_policies[0]
    assert isinstance(policy, RoleAwareAccountPolicyGenome)
    assert 6 <= len(policy.sleeve_ids) <= 8
    payload = policy.to_dict()
    payload["sleeve_ids"] = payload["sleeve_ids"][:5]
    payload["allocation_units"] = payload["allocation_units"][:5]
    payload.pop("structural_fingerprint")
    with pytest.raises(ValueError, match="six to eight"):
        RoleAwareAccountPolicyGenome(**payload)


def test_generation_rejects_proof_consuming_or_status_inheriting_seed() -> None:
    proof_seed = _seed()
    proof_seed["proof_window_consumed"] = True
    with pytest.raises(ValueError, match="proof-consuming"):
        generate_role_aware_account_population(
            proof_seed, campaign_id=CAMPAIGN_ID, policy_pair_count=64
        )

    status_seed = _seed()
    status_seed["governance"]["status_inheritance"] = True
    with pytest.raises(ValueError, match="inheritance"):
        generate_role_aware_account_population(
            status_seed, campaign_id=CAMPAIGN_ID, policy_pair_count=64
        )
