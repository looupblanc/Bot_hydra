from __future__ import annotations

from collections import Counter

from hydra.economic_evolution.assembly import (
    ASSEMBLY_METHODS,
    AssemblyInput,
    generate_account_policy_population,
)
from hydra.economic_evolution.generator import (
    DEFAULT_MARKET_PAIRS,
    component_counts_by_kind,
    generate_structural_population,
)
from hydra.economic_evolution.mutation import propose_directed_mutation
from hydra.economic_evolution.schema import (
    FailureDimension,
    FailureVector,
)


def test_structural_generation_is_deterministic_diverse_and_outcome_free() -> None:
    first = generate_structural_population(
        campaign_id="deterministic_generation", raw_proposal_count=2_000
    )
    second = generate_structural_population(
        campaign_id="deterministic_generation", raw_proposal_count=2_000
    )
    assert first.summary() == second.summary()
    assert [row.to_dict() for row in first.sleeves] == [
        row.to_dict() for row in second.sleeves
    ]
    assert first.unique_sleeve_count > 1_500
    assert len({row.behavioral_fingerprint for row in first.sleeves}) == len(
        first.sleeves
    )
    assert set(row.market for row in first.sleeves) == set(DEFAULT_MARKET_PAIRS)
    assert len({row.role for row in first.sleeves}) >= 5
    kinds = component_counts_by_kind(first.components)
    assert {"CONTEXT", "TRIGGER", "DIRECTION", "TIME_EXIT", "SIZING"} <= set(
        kinds
    )
    manifest = first.sleeves[0].to_dict()
    assert "pnl" not in str(manifest).lower()
    assert "future_move" not in str(manifest).lower()


def test_three_assembly_methods_produce_bounded_distinct_policies() -> None:
    population = generate_structural_population(
        campaign_id="assembly_source", raw_proposal_count=3_000
    )
    selected = population.sleeves[:80]
    inputs = tuple(
        AssemblyInput(
            sleeve=sleeve,
            behavioral_cluster=sleeve.behavioral_fingerprint[:16],
            priority_score=0.8 - index / 1_000.0,
            cost_per_opportunity=4.0 + (index % 3),
            approximate_event_count=50 + index,
        )
        for index, sleeve in enumerate(selected)
    )
    assembled = generate_account_policy_population(
        inputs, campaign_id="assembly_test", count=200
    )
    assert len(assembled.policies) == 200
    assert set(assembled.methods) == set(ASSEMBLY_METHODS)
    assert len({row.structural_fingerprint for row in assembled.policies}) == 200
    assert all(2 <= len(row.sleeve_ids) <= 4 for row in assembled.policies)
    assert all(row.maximum_mini_equivalent <= 15 for row in assembled.policies)
    size_counts = Counter(len(row.sleeve_ids) for row in assembled.policies)
    assert len(size_counts) >= 2


def test_failure_directed_mutation_changes_one_account_dimension() -> None:
    population = generate_structural_population(
        campaign_id="mutation_source", raw_proposal_count=500
    )
    inputs = tuple(
        AssemblyInput(
            sleeve=sleeve,
            behavioral_cluster=sleeve.behavioral_fingerprint[:16],
            priority_score=0.5,
            cost_per_opportunity=5.0,
            approximate_event_count=100,
        )
        for sleeve in population.sleeves[:30]
    )
    parent = generate_account_policy_population(
        inputs, campaign_id="mutation_parent", count=1
    ).policies[0]
    failure = FailureVector(
        policy_id=parent.policy_id,
        scores=(
            (FailureDimension.MLL_BREACH, 0.9),
            (FailureDimension.WEAK_COST_MARGIN, 0.2),
        ),
        evidence_hash="a" * 64,
        evaluated_on_identical_parent_child_starts=True,
    )
    mutation = propose_directed_mutation(
        parent, failure, available_sleeves=population.sleeves
    )
    assert mutation.child_policy is not None
    assert mutation.child_policy.parent_policy_ids == (parent.policy_id,)
    assert mutation.child_policy.inherited_status is None
    assert mutation.child_policy.maximum_simultaneous_positions == (
        parent.maximum_simultaneous_positions - 1
    )
    assert mutation.decision == "REPLAY_ON_IDENTICAL_STARTS"


def test_null_indistinguishable_failure_cannot_receive_parameter_rescue() -> None:
    population = generate_structural_population(
        campaign_id="null_kill_source", raw_proposal_count=500
    )
    inputs = tuple(
        AssemblyInput(
            sleeve=sleeve,
            behavioral_cluster=sleeve.behavioral_fingerprint[:16],
            priority_score=0.5,
            cost_per_opportunity=5.0,
            approximate_event_count=100,
        )
        for sleeve in population.sleeves[:30]
    )
    parent = generate_account_policy_population(
        inputs, campaign_id="null_kill_parent", count=1
    ).policies[0]
    failure = FailureVector(
        policy_id=parent.policy_id,
        scores=((FailureDimension.NULL_INDISTINGUISHABLE, 1.0),),
        evidence_hash="b" * 64,
        evaluated_on_identical_parent_child_starts=True,
    )
    mutation = propose_directed_mutation(
        parent, failure, available_sleeves=population.sleeves
    )
    assert mutation.child_policy is None
    assert mutation.decision == "KILL_OR_CHANGE_REPRESENTATION"
