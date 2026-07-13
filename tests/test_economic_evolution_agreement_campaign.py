from __future__ import annotations

from types import SimpleNamespace

from hydra.research.economic_evolution_agreement_campaign import (
    _aggregate_policy_metrics,
    component_pass,
    component_economic_metrics,
    family_tripwire,
    final_result,
)


def _runtime(*, passed: bool = True):
    return SimpleNamespace(
        event_count=30 if passed else 10,
        net_pnl=100.0 if passed else -10.0,
        cost_stress_1_5x_net=50.0 if passed else -20.0,
        best_positive_event_share=0.2 if passed else 0.9,
        maximum_drawdown=500.0 if passed else 5_000.0,
    )


def _gate() -> dict:
    return {
        "minimum_events": 24,
        "maximum_best_positive_event_share": 0.35,
        "maximum_drawdown_usd": 4_500.0,
        "maximum_null_ratio": 0.8,
        "net_evidence_p_value": 0.05,
    }


def _population():
    return SimpleNamespace(
        real_sleeves=tuple(
            SimpleNamespace(sleeve_id=f"real_{index}") for index in range(4)
        ),
        matched_null_sleeves=tuple(
            SimpleNamespace(sleeve_id=f"null_{index}") for index in range(4)
        ),
    )


def test_component_gate_requires_all_frozen_economic_conditions() -> None:
    assert component_pass(_runtime(), _gate()) is True
    assert component_pass(_runtime(passed=False), _gate()) is False


def test_family_tripwire_uses_frozen_denominators_and_fails_closed_on_missing() -> None:
    population = _population()
    runtimes = {
        "real_0": _runtime(),
        "real_1": _runtime(),
        "null_0": _runtime(),
    }
    result = family_tripwire(population, runtimes, _gate())
    assert result["real_pass_count"] == 2
    assert result["real_candidate_count"] == 4
    assert result["real_exact_replay_missing_count"] == 2
    assert result["null_pass_count"] == 1
    assert result["null_candidate_count"] == 4
    assert result["null_exact_replay_missing_count"] == 3
    assert result["real_pass_rate"] == 0.5
    assert result["null_pass_rate"] == 0.25
    assert result["NULL_RATIO"] == 0.5
    assert result["family_green"] is False
    assert result["verdict"] == "INCOMPLETE_EXACT_REPLAY_FAIL_CLOSED"


def test_family_tripwire_can_turn_green_only_with_complete_exact_replays() -> None:
    population = _population()
    runtimes = {
        **{f"real_{index}": _runtime() for index in range(3)},
        "real_3": _runtime(passed=False),
        "null_0": _runtime(),
        **{f"null_{index}": _runtime(passed=False) for index in range(1, 4)},
    }
    result = family_tripwire(population, runtimes, _gate())
    assert result["real_exact_replay_missing_count"] == 0
    assert result["null_exact_replay_missing_count"] == 0
    assert result["real_pass_count"] == 3
    assert result["null_pass_count"] == 1
    assert result["NULL_RATIO"] == 1 / 3
    assert result["family_green"] is True


def test_null_dominance_blocks_all_account_evaluation() -> None:
    population = _population()
    runtimes = {
        **{f"real_{index}": _runtime() for index in range(2)},
        **{f"real_{index}": _runtime(passed=False) for index in range(2, 4)},
        **{f"null_{index}": _runtime() for index in range(3)},
        "null_3": _runtime(passed=False),
    }
    result = family_tripwire(population, runtimes, _gate())
    assert result["NULL_RATIO"] == 1.5
    assert result["family_green"] is False
    assert result["verdict"] == "ARTEFACT_GEOMETRY_ONLY"


def test_component_metrics_report_all_real_and_matched_null_economics() -> None:
    population = _population()
    runtimes = {
        **{f"real_{index}": _runtime(passed=index < 3) for index in range(4)},
        **{f"null_{index}": _runtime(passed=index == 0) for index in range(4)},
    }
    result = component_economic_metrics(population, runtimes, _gate())
    assert result["real_evaluated_count"] == 4
    assert result["matched_null_evaluated_count"] == 4
    assert result["real_positive_after_stressed_cost_count"] == 3
    assert result["matched_null_positive_after_stressed_cost_count"] == 1
    assert result["real_component_gate_winner_count"] == 3
    assert result["matched_null_component_gate_winner_count"] == 1


def test_policy_metrics_keep_red_family_diagnostics_non_promotional() -> None:
    summary = {
        "episode_start_count": 24,
        "pass_count": 2,
        "pass_rate": 2 / 24,
        "target_progress_median": 0.4,
        "maximum_target_progress": 1.0,
        "mll_breach_rate": 0.0,
        "median_episode_net_pnl": 500.0,
    }
    row = {
        "policy": {"structural_fingerprint": "structure-1"},
        "behavioral_fingerprint": "behavior-1",
        "failure_vectors": ["FAMILY_TRIPWIRE_FAILED", "TARGET_VELOCITY_LOW"],
        "evaluation": {
            "static_base": dict(summary),
            "controlled_base": dict(summary),
            "controlled_stress_1_5x": dict(summary),
        },
        "matched_add_one_leave_one_out_controls": [{}, {}],
    }
    result = _aggregate_policy_metrics([row], family_tripwire_green=False)
    assert result["primary_rolling_combine_episode_count"] == 24
    assert result["all_internal_account_episode_simulation_count"] == 216
    assert result["policies_passing_at_least_one_combine_episode"] == 1
    assert result["behaviorally_distinct_policy_count"] == 1
    assert result["dominant_economic_failure"] == "TARGET_VELOCITY_LOW"
    assert result["targeted_mutations_selected"] == [
        {
            "priority": 1,
            "failure_vector": "TARGET_VELOCITY_LOW",
            "action": "LAUNCH_STRUCTURALLY_DISTINCT_0009_REPRESENTATION",
            "same_class_parameter_rescue": False,
            "status_inheritance": False,
        }
    ]


def test_final_result_never_promotes_development_evidence() -> None:
    prereg = {
        "campaign_id": "hydra_economic_evolution_multi_horizon_agreement_0008",
        "multiplicity": {"reserved_delta_trials": 2_400},
    }
    result = final_result(
        prereg,
        population_summary={"source_count": 22},
        screen_summary={"survivor_count": 12},
        exact_runtime_count=88,
        exact_failure_count=0,
        tripwire={
            "family_green": False,
            "verdict": "ARTEFACT_GEOMETRY_ONLY",
        },
        account_rows=[],
        global_starts=(),
        elapsed_seconds=1.0,
    )
    assert result["scientific_status"] == "ARTEFACT_GEOMETRY_ONLY"
    assert result["account_policy_evaluated_count"] == 0
    assert result["pre_holdout_ready_count"] == 0
    assert result["paper_shadow_ready_count"] == 0
    assert result["governance"]["proof_windows_consumed"] == 0
    assert result["governance"]["new_data_purchase_count"] == 0
    assert result["governance"]["orders"] == 0
