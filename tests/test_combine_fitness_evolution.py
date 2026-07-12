from __future__ import annotations

from dataclasses import replace
import json

import pytest

import hydra.research.combine_first_evolution_v5 as combine_v5

from hydra.factory.failure_guided_evolution import (
    configuration_fingerprint,
    constrained_crossover,
    mutation_outcome,
    propose_failure_guided_mutation,
)
from hydra.factory.quality_diversity import (
    ArchiveCandidate,
    ArchiveNiche,
    QualityDiversityArchive,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_fitness import (
    DefensiveAccountEvidence,
    combine_passer_fitness,
    defensive_account_fitness,
    diagnose_combine_failure,
    diagnose_xfa_failure,
    xfa_payout_fitness,
)
from hydra.research.combine_first_evolution_v5 import (
    _scientific_hash_payload,
    _select_targeted_promising_specs,
)
from hydra.mission.calibration_retest_execution import _stable_hash
from hydra.research.turbo_exact_replay import spec_to_dict
from hydra.propfirm.payout_episode import evaluate_rolling_xfa
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, evaluate_rolling_combine
from hydra.strategies.turbo_batch_fingerprint import structural_fingerprint
from hydra.strategies.turbo_dsl import (
    ComparisonOperator,
    StrategyRole,
    StrategySpec,
)


def _event(day: int, net: float, worst: float = -100.0) -> TradePathEvent:
    return TradePathEvent(
        event_id=f"e-{day}",
        decision_ns=day * 1_000_000_000_000,
        exit_ns=day * 1_000_000_000_000 + 1,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=max(net, 0.0),
        quantity=1,
        mini_equivalent=1.0,
    )


def _spec(**changes: object) -> StrategySpec:
    base = StrategySpec(
        candidate_id="parent",
        lineage_id="lineage",
        family="market_state_geometry",
        market="NQ",
        timeframe="1m",
        feature="path_efficiency",
        operator=ComparisonOperator.GREATER_EQUAL,
        threshold=0.8,
        side=1,
        holding_events=15,
        point_value=20.0,
        round_turn_cost=14.5,
        role=StrategyRole.COMBINE_PASSER,
    )
    return replace(base, **changes)


def test_role_specific_fitness_separates_strong_and_weak_combine() -> None:
    days = list(range(180))
    strong_events = [_event(day, 1200.0) for day in range(0, 180, 4)]
    weak_events = [_event(day, 100.0) for day in range(0, 180, 12)]
    policy = EpisodeStartPolicy(
        maximum_starts=12,
        minimum_spacing_sessions=10,
        minimum_observation_sessions=30,
        maximum_duration_sessions=60,
    )
    strong = evaluate_rolling_combine(strong_events, days, policy=policy)
    weak = evaluate_rolling_combine(weak_events, days, policy=policy)
    strong_fitness = combine_passer_fitness(strong, cost_stress_net_pnl=40_000.0)
    weak_fitness = combine_passer_fitness(weak, cost_stress_net_pnl=500.0)
    assert strong_fitness.score > weak_fitness.score
    assert strong_fitness.components["combine_pass_rate"] > 0
    assert strong_fitness.objective == "COMBINE_PASSER_FITNESS"
    assert diagnose_combine_failure(weak, cost_stress_net_pnl=500.0) == "TARGET_NOT_REACHED"


def test_xfa_and_defensive_fitness_do_not_require_combine_role() -> None:
    days = list(range(220))
    events = [_event(day, 200.0, -20.0) for day in range(220)]
    rolling_xfa = evaluate_rolling_xfa(events, days, maximum_starts=8)
    assert rolling_xfa.path_selection_policy == (
        "ONE_AGGREGATE_DEVELOPMENT_POLICY_PER_CANDIDATE"
    )
    assert {
        rolling_xfa.standard_selected_count,
        rolling_xfa.consistency_selected_count,
    } == {0, rolling_xfa.episode_start_count}
    assert set(rolling_xfa.path_summaries) == {
        "XFA_STANDARD",
        "XFA_CONSISTENCY",
    }
    xfa = xfa_payout_fitness(rolling_xfa)
    assert xfa.objective == "XFA_PAYOUT_FITNESS"
    assert xfa.factory_survivor

    defensive = defensive_account_fitness(
        DefensiveAccountEvidence(
            baseline_mll_breach_rate=0.30,
            candidate_mll_breach_rate=0.15,
            baseline_shared_loss_day_rate=0.40,
            candidate_shared_loss_day_rate=0.25,
            baseline_drawdown=4000.0,
            candidate_drawdown=2800.0,
            baseline_target_velocity=1.0,
            candidate_target_velocity=0.90,
            matched_control_probability=0.08,
        )
    )
    assert defensive.objective == "DEFENSIVE_ACCOUNT_FITNESS"
    assert defensive.factory_survivor


def test_xfa_no_payout_does_not_claim_post_payout_survival() -> None:
    days = list(range(220))
    events = [_event(day, 10.0, -5.0) for day in range(0, 220, 20)]

    rolling = evaluate_rolling_xfa(events, days, maximum_starts=8)

    assert rolling.payout_probability == 0.0
    assert rolling.post_payout_survival_rate == 0.0
    assert diagnose_xfa_failure(rolling) == "TARGET_NOT_REACHED"


def test_targeted_promising_seed_selection_round_robins_market_and_role() -> None:
    ranked = [
        _spec(candidate_id=f"nq-{index}", lineage_id=f"nq-lineage-{index}")
        for index in range(8)
    ]
    ranked.extend(
        [
            _spec(
                candidate_id="cl-xfa",
                lineage_id="cl-lineage",
                market="CL",
                point_value=1_000.0,
                role=StrategyRole.XFA_PAYOUT,
            ),
            _spec(
                candidate_id="gc-defensive",
                lineage_id="gc-lineage",
                market="GC",
                point_value=100.0,
                role=StrategyRole.DEFENSIVE,
            ),
        ]
    )

    selected = _select_targeted_promising_specs(ranked, limit=4)

    assert {spec.market for spec in selected} >= {"CL", "GC", "NQ"}
    assert {spec.role for spec in selected} >= {
        StrategyRole.COMBINE_PASSER,
        StrategyRole.XFA_PAYOUT,
        StrategyRole.DEFENSIVE,
    }


def test_scientific_hash_excludes_runtime_and_cache_temperature() -> None:
    left = {
        "schema": "v5",
        "candidates": [{"candidate_id": "a", "pass_rate": 0.25}],
        "feature_store": {
            "source_fingerprint": "source",
            "cache_hits": 6,
            "cache_misses": 0,
        },
        "performance": {"total_seconds": 10.0, "rss_mb": 100.0},
    }
    right = {
        **left,
        "feature_store": {
            **left["feature_store"],
            "cache_hits": 0,
            "cache_misses": 6,
        },
        "performance": {"total_seconds": 20.0, "rss_mb": 200.0},
    }

    assert _stable_hash(_scientific_hash_payload(left)) == _stable_hash(
        _scientific_hash_payload(right)
    )


def test_structural_batch_downsizes_only_to_reported_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted: list[int] = []

    def fake_generator(*_args: object, count: int, **_kwargs: object):
        attempted.append(count)
        if count == 10_000:
            raise combine_v5.TurboFoundryError(
                "INSUFFICIENT_STRUCTURAL_DIVERSITY: family/lineage caps permit "
                "only 8789 of 10000 requested structures"
            )
        return [object()] * count

    monkeypatch.setattr(combine_v5, "generate_turbo_population", fake_generator)
    population, audit = combine_v5._generate_adaptive_population(
        {},
        requested_count=10_000,
        batch_index=0,
        random_seed=1,
        excluded_fingerprints=(),
    )

    assert attempted == [10_000, 8_789]
    assert len(population) == 8_789
    assert audit["downscaled"] is True
    assert audit["selection_uses_future_economics"] is False


def test_prior_v5_structures_and_children_become_tombstones_and_parents(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "reports" / "mission_experiments"
    epoch = root / "combine_first_evolution_v5_epoch_0000"
    epoch.mkdir(parents=True)
    parent = _spec(candidate_id="v5-parent", lineage_id="v5-parent-lineage")
    child = replace(
        parent,
        candidate_id="v5-child",
        lineage_id="v5-child-lineage",
        quantity=2,
    )
    (epoch / "combine_v5_population_manifest.json").write_text(
        json.dumps({"specifications": [spec_to_dict(parent)]}) + "\n",
        encoding="utf-8",
    )
    (epoch / "combine_v5_mutations.json").write_text(
        json.dumps({"proposals": [{"child": spec_to_dict(child)}]}) + "\n",
        encoding="utf-8",
    )
    (epoch / "combine_v5_result.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": child.candidate_id,
                        "status": "PROMISING_RESEARCH_CANDIDATE",
                        "specification": spec_to_dict(child),
                        "role_specific_fitness": {"score": 0.7},
                        "net_pnl": 9_000.0,
                        "maximum_drawdown": 1_000.0,
                        "topstep_path_candidate": False,
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        combine_v5,
        "project_path",
        lambda *parts: tmp_path.joinpath(*parts),
    )

    structural, configurations = combine_v5._historical_v5_fingerprints()
    ranked, audit = combine_v5._load_existing_promising_specs()

    assert structural_fingerprint(parent) in structural
    assert configuration_fingerprint(parent) in configurations
    assert configuration_fingerprint(child) in configurations
    assert any(spec.candidate_id == child.candidate_id for spec in ranked)
    assert audit["v5_source_files"] >= 1


def test_failure_guided_mutation_has_provenance_and_no_status_inheritance() -> None:
    parent = _spec()
    proposal = propose_failure_guided_mutation(
        parent,
        diagnosed_failure="TARGET_NOT_REACHED",
        rolling_summary={
            "median_target_progress_when_not_passed": 0.45,
            "minimum_mll_buffer": 4000.0,
        },
    )
    assert proposal is not None
    assert proposal.hypothesis.inherited_status is None
    assert proposal.hypothesis.parent_candidate_id == parent.candidate_id
    assert proposal.child.candidate_id != parent.candidate_id
    assert proposal.child.quantity > parent.quantity
    assert structural_fingerprint(proposal.child) == structural_fingerprint(parent)
    assert configuration_fingerprint(proposal.child) != configuration_fingerprint(parent)

    outcome = mutation_outcome(
        proposal,
        parent_fitness=0.45,
        child_fitness=0.60,
        parent_pass_rate=0.0,
        child_pass_rate=0.20,
        parent_mll_breach_rate=0.10,
        child_mll_breach_rate=0.10,
    )
    assert outcome.decision == "KEEP_CHILD"
    assert outcome.inherited_status is None
    assert outcome.fitness_delta > 0


def test_constrained_crossover_requires_compatible_components() -> None:
    left = _spec()
    right = _spec(
        candidate_id="right",
        timeframe="1m|30m",
        context_feature="ctx_30m_return",
        context_operator=ComparisonOperator.GREATER_THAN,
        context_threshold=0.0,
    )
    proposal = constrained_crossover(left, right)
    assert proposal is not None
    assert proposal.child.context_feature == "ctx_30m_return"
    assert proposal.hypothesis.inherited_status is None
    assert constrained_crossover(left, replace(right, market="CL")) is None


def test_archive_counts_sizing_configs_as_one_mechanism() -> None:
    niche = ArchiveNiche(
        market_ecology="equity_indices",
        timeframe_profile="1m",
        holding_horizon="SHORT",
        session="0",
        mechanism_family="market_state_geometry",
        portfolio_role="COMBINE_PASSER",
        turnover="MEDIUM",
        behavioral_cluster="NQ:geometry:long",
    )
    parent = _spec()
    scaled = replace(parent, candidate_id="scaled", quantity=2)
    archive = QualityDiversityArchive(
        maximum_family_share=1.0,
        maximum_ecology_share=1.0,
        maximum_lineage_share=1.0,
    )
    first = archive.insert(
        ArchiveCandidate(
            parent.candidate_id,
            structural_fingerprint(parent),
            parent.lineage_id,
            parent.family,
            niche,
            {"combine_fitness": 0.5},
            {},
        )
    )
    duplicate = archive.insert(
        ArchiveCandidate(
            scaled.candidate_id,
            structural_fingerprint(scaled),
            scaled.lineage_id,
            scaled.family,
            niche,
            {"combine_fitness": 0.8},
            {},
        )
    )
    assert first.accepted
    assert not duplicate.accepted
    assert duplicate.reason == "duplicate_structural_fingerprint"
    assert archive.summary()["candidate_count"] == 1
