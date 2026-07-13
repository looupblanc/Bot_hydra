from __future__ import annotations

import json
from pathlib import Path

from hydra.economic_evolution.directional_agreement import (
    AGREEMENT_CLASS_ID,
    generate_directional_agreement_population,
)


CAMPAIGN_ID = "hydra_economic_evolution_multi_horizon_agreement_0008"
EXCLUDED = (
    "sleeve_56d62ae303bf865ba98c7748",
    "sleeve_66ae033ef4acbc5ea759661d",
    "sleeve_958d5ade0f6a1090ba0d95aa",
    "sleeve_f85e46cd5d5793e6b9042cdb",
)


def _seed() -> dict:
    return json.loads(
        Path(
            "reports/economic_evolution/seeds/"
            "persistent_0003_successor_seed.json"
        ).read_text(encoding="utf-8")
    )


def _population():
    return generate_directional_agreement_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        excluded_source_sleeve_ids=EXCLUDED,
        maximum_sources=24,
        maximum_sources_per_market=5,
        maximum_sources_per_market_session=2,
        maximum_sources_per_market_mechanism=2,
        minimum_source_events=24,
        contexts_per_source=2,
        agreement_quantile=0.65,
        policy_count=256,
    )


def test_agreement_population_is_deterministic_and_substantive() -> None:
    first = _population()
    second = _population()
    assert first.candidate_manifest_hash == second.candidate_manifest_hash
    assert len(first.sources) == 22
    assert len(first.real_sleeves) == 44
    assert len(first.matched_null_sleeves) == 44
    assert len(first.components) == 528
    assert len(first.policies) == 256
    assert len({row.sleeve_id for row in first.real_sleeves}) == 44
    assert len({row.sleeve_id for row in first.matched_null_sleeves}) == 44
    assert len({row.structural_fingerprint for row in first.policies}) == 256
    assert len({row.source.market for row in first.sources}) == 5
    assert first.summary()["class_id"] == AGREEMENT_CLASS_ID


def test_real_and_null_are_source_horizon_paired_and_directionally_opposed() -> None:
    population = _population()
    source_by_candidate = dict(population.source_by_candidate)
    horizon_by_candidate = dict(population.horizon_by_candidate)
    sources = {row.source.sleeve_id: row.source for row in population.sources}
    real_by_pair = {
        (source_by_candidate[row.sleeve_id], horizon_by_candidate[row.sleeve_id]): row
        for row in population.real_sleeves
    }
    null_by_pair = {
        (source_by_candidate[row.sleeve_id], horizon_by_candidate[row.sleeve_id]): row
        for row in population.matched_null_sleeves
    }
    assert real_by_pair.keys() == null_by_pair.keys()
    assert len(real_by_pair) == 44
    for key, real in real_by_pair.items():
        null = null_by_pair[key]
        source = sources[key[0]]
        assert real.context_feature == null.context_feature == key[1]
        assert real.trigger_feature == null.trigger_feature == source.trigger_feature
        assert real.side == null.side == source.side
        assert real.context_operator != null.context_operator
        if source.side == 1:
            assert (real.context_operator, real.context_quantile) == ("GT", 0.65)
            assert (null.context_operator, null.context_quantile) == ("LT", 0.35)
        else:
            assert (real.context_operator, real.context_quantile) == ("LT", 0.35)
            assert (null.context_operator, null.context_quantile) == ("GT", 0.65)


def test_agreement_candidates_do_not_clone_seed_or_inherit_status() -> None:
    seed = _seed()
    population = _population()
    seed_behavior = {
        row["specification"]["behavioral_fingerprint"]
        for row in seed["sleeves"]
    }
    for sleeve in (*population.real_sleeves, *population.matched_null_sleeves):
        assert sleeve.behavioral_fingerprint not in seed_behavior
        assert sleeve.inherited_status is None
        assert sleeve.source_campaign == CAMPAIGN_ID
    for policy in population.policies:
        assert policy.inherited_status is None
        assert policy.source_campaign == CAMPAIGN_ID


def test_policies_use_real_distinct_sources_markets_and_contexts_only() -> None:
    population = _population()
    real = {row.sleeve_id: row for row in population.real_sleeves}
    null_ids = {row.sleeve_id for row in population.matched_null_sleeves}
    source_by_candidate = dict(population.source_by_candidate)
    for policy in population.policies:
        assert not (set(policy.sleeve_ids) & null_ids)
        members = [real[row] for row in policy.sleeve_ids]
        assert len({source_by_candidate[row.sleeve_id] for row in members}) == len(
            members
        )
        assert len({row.market for row in members}) >= 2
        assert len({row.context_feature for row in members}) >= 2
        assert len({row.role for row in members}) >= 2


def test_class_is_not_the_tombstoned_density_class() -> None:
    assert AGREEMENT_CLASS_ID != (
        "INDEPENDENT_OPPORTUNITY_DENSITY_CONSISTENCY_ASSEMBLY_V1"
    )
