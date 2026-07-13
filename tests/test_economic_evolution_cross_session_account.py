from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.economic_evolution.cross_session_account import (
    CROSS_SESSION_CLASS_ID,
    CrossSessionAccountPopulation,
    generate_cross_session_account_population,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN_ID = "hydra_economic_evolution_cross_session_account_synthesis_0009"


def _seed() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/seeds/"
            "persistent_0003_successor_seed.json"
        ).read_text(encoding="utf-8")
    )


@pytest.fixture(scope="module")
def population() -> CrossSessionAccountPopulation:
    return generate_cross_session_account_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=96,
    )


def test_population_is_account_first_and_deterministic(
    population: CrossSessionAccountPopulation,
) -> None:
    repeated = generate_cross_session_account_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=96,
    )
    assert population.manifest_hash == repeated.manifest_hash
    assert population.summary()["class_id"] == CROSS_SESSION_CLASS_ID
    assert population.summary()["real_policy_count"] == 96
    assert population.summary()["matched_control_policy_count"] == 96
    assert len({row.real_policy.policy_id for row in population.pairs}) == 96
    assert (
        len({row.matched_control_policy.policy_id for row in population.pairs})
        == 96
    )


def test_real_policies_span_markets_sessions_roles_and_mechanisms(
    population: CrossSessionAccountPopulation,
) -> None:
    by_id = {row.sleeve.sleeve_id: row for row in population.components}
    for pair in population.pairs:
        members = [by_id[key] for key in pair.real_policy.sleeve_ids]
        assert len(members) in {3, 4}
        assert len({row.sleeve.market for row in members}) >= 2
        assert len({row.sleeve.session_code for row in members}) >= 2
        assert len({row.sleeve.role for row in members}) >= 2
        assert len({row.sleeve.trigger_feature for row in members}) >= 2


def test_controls_are_concentrated_and_account_parameter_matched(
    population: CrossSessionAccountPopulation,
) -> None:
    by_id = {row.sleeve.sleeve_id: row for row in population.components}
    for pair in population.pairs:
        control_members = [
            by_id[key] for key in pair.matched_control_policy.sleeve_ids
        ]
        assert (
            len({row.sleeve.market for row in control_members}) == 1
            or len({row.sleeve.session_code for row in control_members}) == 1
        )
        assert pair.to_dict()["identical_account_parameters"] is True
        assert set(pair.real_policy.sleeve_ids) != set(
            pair.matched_control_policy.sleeve_ids
        )


def test_generation_does_not_inherit_status_or_reemit_prior_policies(
    population: CrossSessionAccountPopulation,
) -> None:
    seed = _seed()
    blocked = {
        row["policy"]["structural_fingerprint"]
        for row in seed["policies"]
    } | {
        row["child_policy"]["structural_fingerprint"]
        for row in seed["mutations"]
    }
    generated = {
        policy.structural_fingerprint
        for pair in population.pairs
        for policy in (pair.real_policy, pair.matched_control_policy)
    }
    assert generated.isdisjoint(blocked)
    assert all(
        policy.inherited_status is None
        for pair in population.pairs
        for policy in (pair.real_policy, pair.matched_control_policy)
    )


def test_proof_consuming_seed_is_rejected() -> None:
    seed = _seed()
    seed["proof_window_consumed"] = True
    with pytest.raises(ValueError, match="proof-consuming"):
        generate_cross_session_account_population(
            seed, campaign_id=CAMPAIGN_ID, policy_pair_count=64
        )
