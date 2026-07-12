from __future__ import annotations

from hydra.account_policy.evolution import ComponentRuntime
from hydra.account_policy.schema import ComponentDescriptor, ComponentRole
from hydra.account_policy.target_velocity import (
    evaluate_target_velocity_outcome,
    generate_target_velocity_mutations,
)
from hydra.research.turbo_exact_replay import spec_to_dict
from hydra.strategies.turbo_dsl import (
    ComparisonOperator,
    StrategyRole,
    StrategySpec,
)


def _component() -> ComponentRuntime:
    spec = StrategySpec(
        candidate_id="parent",
        lineage_id="lineage-parent",
        family="session_transition",
        market="NQ",
        timeframe="5m|15m",
        feature="return_efficiency_15",
        operator=ComparisonOperator.GREATER_THAN,
        threshold=0.4,
        side=1,
        holding_events=15,
        point_value=2.0,
        round_turn_cost=2.4,
        role=StrategyRole.COMBINE_PASSER,
        quantity=2,
    )
    descriptor = ComponentDescriptor(
        component_id="parent",
        specification_hash="spec-parent",
        market="NQ",
        execution_market="MNQ",
        family="session_transition",
        timeframe="5m|15m",
        role=ComponentRole.TARGET_VELOCITY_COMPONENT,
        behavioral_cluster="cluster-parent",
        source_experiment="v5",
        source_result_hash="result-parent",
        net_pnl_after_costs=5000.0,
        cost_stress_net_pnl=3500.0,
        event_count=50,
        rolling_pass_rate=0.05,
        rolling_mll_breach_rate=0.05,
        median_target_progress=0.50,
    )
    return ComponentRuntime(
        descriptor,
        spec_to_dict(spec),
        (),
        tuple(range(200)),
        "TEST",
    )


def _result(
    candidate_id: str,
    *,
    progress: float,
    pass_rate: float = 0.05,
    breach: float = 0.05,
    starts: tuple[int, ...] = (0, 10, 20),
) -> dict:
    return {
        "candidate_id": candidate_id,
        "hard_invalidation": None,
        "exact_trade_path": {
            "event_count": 50,
            "net_pnl": 5000.0,
            "cost_stress_1_5x_net": 3500.0,
        },
        "rolling_combine": {
            "episode_start_days": list(starts),
            "pass_rate": pass_rate,
            "mll_breach_rate": breach,
            "median_target_progress_when_not_passed": progress,
            "consistency_pass_rate": 0.75,
            "minimum_mll_buffer": 1800.0,
            "median_days_to_target": 45.0,
        },
    }


def test_target_velocity_mutation_changes_one_structure_not_contract_size() -> None:
    proposals = generate_target_velocity_mutations(
        [_component()], generation_index=0, maximum=1
    )

    assert len(proposals) == 1
    proposal = proposals[0]
    assert proposal.child.quantity == 2
    assert len(proposal.hypothesis.exact_change) == 1
    assert proposal.hypothesis.inherited_status is None
    assert proposal.child.candidate_id != "parent"


def test_target_velocity_child_requires_same_episode_starts_and_objective_gain() -> None:
    proposal = generate_target_velocity_mutations(
        [_component()], generation_index=0, maximum=1
    )[0]
    outcome = evaluate_target_velocity_outcome(
        proposal,
        parent_result=_result("parent", progress=0.40),
        child_result=_result(proposal.child.candidate_id, progress=0.50),
    )

    assert outcome["decision"] == "KEEP_CHILD"
    assert outcome["target_progress_delta"] > 0.0
    assert outcome["mll_breach_rate_delta"] == 0.0
    assert outcome["inherited_status"] is None


def test_target_velocity_comparison_rejects_different_episode_starts() -> None:
    proposal = generate_target_velocity_mutations(
        [_component()], generation_index=0, maximum=1
    )[0]

    try:
        evaluate_target_velocity_outcome(
            proposal,
            parent_result=_result("parent", progress=0.40),
            child_result=_result(
                proposal.child.candidate_id,
                progress=0.50,
                starts=(1, 11, 21),
            ),
        )
    except ValueError as exc:
        assert "identical episode starts" in str(exc)
    else:
        raise AssertionError("comparison with different starts was accepted")
