from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_graduation_cohort as cohort


ROOT = Path(__file__).resolve().parents[1]


def _fake_policy(*, concurrency_scaling: str = "PROPORTIONAL") -> SimpleNamespace:
    return SimpleNamespace(
        policy_id="frozen-book",
        concurrency_scaling=SimpleNamespace(value=concurrency_scaling),
        target_protection_mode=SimpleNamespace(value="SCALE_50"),
        maximum_mini_equivalent=5.0,
        nominal_risk_charge_map={"sleeve-a": 125.0},
    )


def _decision_row(*, kind: str = "reduction") -> dict[str, object]:
    if kind == "reduction":
        requested_quantity = 24
        admitted_quantity = 8
        requested_mini = 2.4
        admitted_mini = 0.8
        requested_risk = 300.0
        admitted_risk = 100.0
        status = "SIZE_REDUCED"
        reason = "ACTIVE_POOL_PROPORTIONAL_SIZE_REDUCTION"
        binding = "AGGREGATE_NOMINAL_RISK_LIMIT"
        allow = accepted = True
        rejected = False
        foregone = 160.0
        risk_before = 300.0
        risk_maximum = 400.0
    elif kind == "rejected":
        requested_quantity = 10
        admitted_quantity = 0
        requested_mini = 1.0
        admitted_mini = 0.0
        requested_risk = 125.0
        admitted_risk = 0.0
        status = "REJECTED"
        reason = "DAILY_LOSS_GUARD"
        binding = reason
        allow = accepted = False
        rejected = True
        foregone = -25.0
        risk_before = 100.0
        risk_maximum = 400.0
    else:
        raise AssertionError(kind)
    return {
        "policy_id": "frozen-book",
        "component_id": "sleeve-a",
        "event_id": f"event-{kind}",
        "decision_ns": 1,
        "exit_ns": 2,
        "base_quantity": requested_quantity,
        "base_mini_equivalent": requested_mini,
        "requested_quantity": requested_quantity,
        "requested_mini_equivalent": requested_mini,
        "requested_declared_nominal_risk": requested_risk,
        "quantity": admitted_quantity,
        "mini_equivalent": admitted_mini,
        "admitted_declared_nominal_risk": admitted_risk,
        "decision_status": status,
        "size_reduced": status == "SIZE_REDUCED",
        "conflict_rejected": status == "CONFLICT_REJECTED",
        "contract_limit_rejected": status == "CONTRACT_LIMIT_REJECTED",
        "mll_risk_rejected": status == "MLL_RISK_REJECTED",
        "reason": reason,
        "binding_constraint": binding,
        "emitted": True,
        "allow": allow,
        "accepted": accepted,
        "rejected": rejected,
        "admission_fraction": admitted_quantity / requested_quantity,
        "scaling_factor": admitted_quantity / requested_quantity,
        "risk_before": {
            "open_declared_nominal_risk": risk_before,
            "maximum_admissible_declared_nominal_risk": risk_maximum,
        },
        "risk_after": {
            "open_declared_nominal_risk": risk_before + admitted_risk,
            "maximum_admissible_declared_nominal_risk": risk_maximum,
        },
        "foregone_expected_pnl": None,
        "foregone_expected_pnl_status": (
            "UNAVAILABLE_NO_FROZEN_PRE_OUTCOME_ESTIMATE"
        ),
        "foregone_realized_pnl_ex_post": foregone,
        "foregone_realized_pnl_status": "OBSERVED_COMPLETE_PATH",
        "foregone_realized_pnl_used_for_routing": False,
        "foregone_realized_pnl_available_at_decision": False,
    }


class _FakeEpisode:
    def __init__(self, allocation: tuple[dict[str, object], ...] = ()) -> None:
        self.target_progress = 0.10
        self.net_pnl = 100.0
        self.passed = False
        self.mll_breached = False
        self.minimum_mll_buffer = 2_000.0
        self.consistency_ok = True
        self.best_day_concentration = 0.25
        self.days_to_target = None
        self.component_contribution = {"sleeve-a": 100.0}
        self.daily_path = (
            {
                "session_day": 1,
                "day_pnl": 100.0,
                "exposure": {"maximum_mini_equivalent": 2.5},
            },
        )
        self.maximum_mini_equivalent = 2.5
        self.maximum_net_directional_exposure = 2.5
        self.accepted_events = 1
        self.skipped_events = 0
        self.terminal = SimpleNamespace(value="TIMEOUT")
        self.risk_allocation_path = allocation

    def to_dict(self, *, include_paths: bool = False) -> dict[str, object]:
        return {
            "episode": "fake",
            "include_paths": include_paths,
            "risk_allocation_path": list(self.risk_allocation_path),
        }


def test_hash_bound_audit_excludes_all_consumed_candidates() -> None:
    result = cohort.audit_autonomous_graduation_cohort(ROOT)
    assert result["status"] == "PASS_HASH_BOUND_COHORT_AUDIT"
    assert result["cohort_size"] == 12
    assert result["confirmation_evaluated"] is False
    assert result["confirmation_partition_reads"] == 0
    assert result["q4_access_count_delta"] == 0
    assert result["orders"] == 0
    assert set(result["consumed_2026_candidate_ids"]) == {
        "hazard_2641d5adb7bfee8dca07de2a",
        "hazard_16a744e747cafb88a7e2c83b",
        "hazard_0a569f580a2540474116636c",
        "hazard_10ffb41856432af08259e32b",
        "hazard_16f0da561bc98f2eb7d2efc4",
    }


def test_preflight_receipt_is_hash_bound_and_read_only() -> None:
    provenance_core = {
        "source_commit": "a" * 40,
        "manifest_file_sha256": "b" * 64,
        "adapter_module_sha256": "c" * 64,
        "runner_script_sha256": "d" * 64,
        "audit_protocol_tests_sha256": "e" * 64,
    }
    provenance = {
        **provenance_core,
        "runtime_provenance_hash": stable_hash(provenance_core),
    }
    core = {
        "schema": cohort.PREFLIGHT_SCHEMA,
        "status": "PASS_FROZEN_FULL_COVERAGE_PREFLIGHT",
        "manifest_hash": "a" * 64,
        "frozen_candidates": [],
        "runtime_provenance": provenance,
        "runtime_provenance_hash": provenance["runtime_provenance_hash"],
        "confirmation_partition_reads": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    receipt = {**core, "preflight_hash": stable_hash(core)}
    assert cohort.verify_autonomous_graduation_preflight(receipt) == receipt
    receipt["orders"] = 1
    with pytest.raises(cohort.AutonomousGraduationCohortError):
        cohort.verify_autonomous_graduation_preflight(receipt)


def test_full_coverage_filters_without_changing_requested_denominator() -> None:
    value = {
        "calendar": (1, 2, 3, 4, 5, 6),
        "eligible_session_days": frozenset((1, 2, 3, 4, 5, 6)),
        "censored_session_days": frozenset((4,)),
    }
    starts = {
        5: ((1, "B1"), (2, "B2")),
        10: ((1, "B1"),),
        20: ((1, "B1"),),
    }
    result = cohort._coverage_for_prepared(value, starts)
    assert result["5"]["requested_start_count"] == 2
    assert result["5"]["full_coverage_start_count"] == 0
    assert result["5"]["data_censored_start_count"] == 2
    assert result["10"]["requested_start_count"] == 1
    assert result["10"]["full_coverage_start_count"] == 0
    assert result["5"]["within_horizon_non_overlapping"] is True


def test_development_gate_requires_same_horizon_block_diverse_stressed_passes() -> None:
    summary = {
        "episode_count": 12,
        "pass_count": 3,
        "mll_breach_rate": 0.0,
        "net_total_usd": 100.0,
        "blocks_with_passes": ["B1", "B3"],
        "all_passing_paths_consistency_compliant": True,
        "by_block": {"B1": {"pass_count": 1}, "B3": {"pass_count": 2}},
    }
    summaries = {
        "NORMAL": {str(h): dict(summary) for h in cohort.HORIZONS},
        "STRESSED_1_5X": {str(h): dict(summary) for h in cohort.HORIZONS},
    }
    gate = {
        "minimum_normal_passes_same_horizon": 3,
        "minimum_stressed_passes_same_horizon": 2,
        "minimum_blocks_with_stressed_passes": 2,
        "maximum_stressed_mll_breach_rate": 0.10,
    }
    concentration = {"cleared": True}
    result = cohort._development_gates(summaries, concentration, gate)
    assert all(all(checks.values()) for checks in result.values())
    summaries["STRESSED_1_5X"]["10"]["blocks_with_passes"] = ["B4"]
    result = cohort._development_gates(summaries, concentration, gate)
    assert result["10"]["multiple_stressed_pass_blocks"] is False


def test_legitimate_proportional_24_to_8_reduction_is_fully_attributed() -> None:
    episode = _FakeEpisode((_decision_row(kind="reduction"),))
    result = cohort._governor_allocation_attribution(
        [episode], policy=_fake_policy()
    )
    assert result["requested_quantity_total"] == 24
    assert result["admitted_quantity_total"] == 8
    assert result["size_reduced_count"] == 1
    assert result["size_reduction_binding_counts"] == {
        "AGGREGATE_NOMINAL_RISK_LIMIT": 1
    }
    assert result["foregone_realized_pnl_size_reductions_usd"] == 160.0
    assert result["size_reduction_is_frozen_governor_semantics"] is True


def test_proportional_reduction_fails_under_priority_policy() -> None:
    episode = _FakeEpisode((_decision_row(kind="reduction"),))
    with pytest.raises(cohort.AutonomousGraduationCohortError):
        cohort._governor_allocation_attribution(
            [episode], policy=_fake_policy(concurrency_scaling="PRIORITY")
        )


def test_generic_full_rejection_is_counted_with_foregone_pnl() -> None:
    episode = _FakeEpisode((_decision_row(kind="rejected"),))
    result = cohort._governor_allocation_attribution(
        [episode], policy=_fake_policy()
    )
    assert result["all_governor_rejection_count"] == 1
    assert result["decision_status_counts"] == {"REJECTED": 1}
    assert result["rejection_reason_counts"] == {"DAILY_LOSS_GUARD": 1}
    assert result["foregone_realized_pnl_full_rejections_usd"] == -25.0


def test_rejection_preserves_preexisting_risk_even_just_above_dynamic_maximum() -> None:
    row = _decision_row(kind="rejected")
    row.update(
        {
            "decision_status": "CONFLICT_REJECTED",
            "reason": "SAME_INSTRUMENT_CONFLICT",
            "binding_constraint": "SAME_INSTRUMENT_CONFLICT",
            "conflict_rejected": True,
            "risk_before": {
                "open_declared_nominal_risk": 406.38432255912824,
                "maximum_admissible_declared_nominal_risk": 406.33767082902705,
            },
            "risk_after": {
                "open_declared_nominal_risk": 406.38432255912824,
                "maximum_admissible_declared_nominal_risk": 406.33767082902705,
            },
        }
    )
    result = cohort._governor_allocation_attribution(
        [_FakeEpisode((row,))], policy=_fake_policy()
    )
    assert result["decision_status_counts"] == {"CONFLICT_REJECTED": 1}
    corrupted = dict(row)
    corrupted["risk_after"] = dict(row["risk_after"])
    corrupted["risk_after"]["maximum_admissible_declared_nominal_risk"] = 406.0
    with pytest.raises(cohort.AutonomousGraduationCohortError):
        cohort._governor_allocation_attribution(
            [_FakeEpisode((corrupted,))], policy=_fake_policy()
        )


@pytest.mark.parametrize(
    ("mutation", "value"),
    (
        ("reason", "UNKNOWN_REJECTION"),
        ("binding_constraint", "SHARED_CONTRACT_LIMIT"),
        ("scaling_factor", 0.5),
        ("allow", False),
        ("mll_risk_rejected", True),
        ("requested_declared_nominal_risk", 299.0),
        ("admitted_declared_nominal_risk", 99.0),
        ("policy_id", "other-policy"),
    ),
)
def test_corrupt_reduction_rows_fail_closed(mutation: str, value: object) -> None:
    row = _decision_row(kind="reduction")
    row[mutation] = value
    with pytest.raises(cohort.AutonomousGraduationCohortError):
        cohort._governor_allocation_attribution(
            [_FakeEpisode((row,))], policy=_fake_policy()
        )


def test_missing_governor_field_fails_closed() -> None:
    row = _decision_row(kind="reduction")
    del row["component_id"]
    with pytest.raises(cohort.AutonomousGraduationCohortError):
        cohort._governor_allocation_attribution(
            [_FakeEpisode((row,))], policy=_fake_policy()
        )


def test_policy_aware_summary_uses_frozen_50k_limit_and_complete_blocks() -> None:
    result = cohort._summarize_cohort_episodes(
        [(_FakeEpisode(), "B1")],
        requested_start_count=1,
        data_censored_count=0,
        policy=_fake_policy(),
    )
    assert result["contract_utilization_denominator_mini_equivalent"] == 5.0
    assert result["mean_daily_contract_utilization"] == 0.5
    assert result["by_block"]["B1"]["episode_count"] == 1
    assert result["by_block"]["B2"]["episode_count"] == 0
    assert set(result["by_block"]) == set(cohort.BLOCKS)


def test_policy_aware_summary_rejects_exposure_above_frozen_limit() -> None:
    episode = _FakeEpisode()
    episode.maximum_mini_equivalent = 5.1
    with pytest.raises(cohort.AutonomousGraduationCohortError):
        cohort._summarize_cohort_episodes(
            [(episode, "B1")],
            requested_start_count=1,
            data_censored_count=0,
            policy=_fake_policy(),
        )


def test_actual_book_742_reconstructs_frozen_policy_and_attributes_24_to_8() -> None:
    manifest = cohort._load_cohort_manifest(ROOT / cohort.DEFAULT_MANIFEST)
    artifacts = cohort._load_replay_artifacts(ROOT, manifest)
    prepared = cohort._prepare_all(ROOT, manifest, artifacts)
    value = next(
        row
        for row in prepared
        if row["candidate_id"]
        == "autonomous_marginal_book_74271a65d77ce0c7fe144170"
    )
    assert value["policy"].maximum_mini_equivalent == 5.0
    assert value["frozen_policy_hash"] == value["source_governor_policy_hash"]
    episode = cohort.run_causal_shared_account_episode(
        value["trajectories"]["NORMAL"],
        value["calendar"],
        policy=value["policy"],
        start_day=19541,
        maximum_duration_days=5,
        config=value["config"],
    )
    result = cohort._summarize_cohort_episodes(
        [(episode, "B1")],
        requested_start_count=1,
        data_censored_count=0,
        policy=value["policy"],
    )
    assert result["size_reduced_count"] == 6
    assert result["governor_allocation_attribution"][
        "size_reduction_binding_counts"
    ] == {"AGGREGATE_NOMINAL_RISK_LIMIT": 6}
    assert result["contract_utilization_denominator_mini_equivalent"] == 5.0
    over_dynamic_limit_episode = cohort.run_causal_shared_account_episode(
        value["trajectories"]["NORMAL"],
        value["calendar"],
        policy=value["policy"],
        start_day=19569,
        maximum_duration_days=20,
        config=value["config"],
    )
    over_dynamic_limit = cohort._governor_allocation_attribution(
        [over_dynamic_limit_episode], policy=value["policy"]
    )
    assert over_dynamic_limit["decision_status_counts"]["CONFLICT_REJECTED"] > 0


def test_xfa_contract_never_counts_alternative_paths_without_combine_pass() -> None:
    manifest = cohort._load_cohort_manifest(ROOT / cohort.DEFAULT_MANIFEST)
    assert manifest["xfa_contract"]["trigger"] == (
        "UNIQUE_EXACT_TARGET_REACHED_COMBINE_PATH_ONLY"
    )
    assert manifest["xfa_contract"]["paths_are_alternatives"] is True
    assert manifest["xfa_contract"]["sum_expected_values"] is False
    assert manifest["evaluation_contract"]["stage_48_claimed"] is False
    assert manifest["evaluation_contract"]["stage_96_claimed"] is False


def test_xfa_count_sums_only_materialized_alternative_paths() -> None:
    empty = {
        scenario: {
            str(horizon): {"episode_count": 0, "pass_count": 0}
            for horizon in cohort.HORIZONS
        }
        for scenario in cohort.SCENARIOS
    }
    counts = cohort._evidence_counts(
        [{"summaries": empty}],
        [
            {
                "status": "COMPLETE_DIAGNOSTIC_XFA_ALTERNATIVES",
                "alternative_path_count": 2,
            },
            {
                "status": "XFA_CONTINUATION_UNAVAILABLE_FAIL_CLOSED",
                "alternative_path_count": 0,
            },
        ],
        unique_combine_path_count=2,
    )
    assert counts["unique_xfa_transition_record_count"] == 2
    assert counts["ready_xfa_transition_count"] == 1
    assert counts["fail_closed_xfa_transition_count"] == 1
    assert counts["alternative_xfa_path_count"] == 2


def test_cross_horizon_passes_are_labeled_observations_not_unique_paths() -> None:
    summaries = {
        scenario: {
            str(horizon): {"episode_count": 4, "pass_count": 1}
            for horizon in cohort.HORIZONS
        }
        for scenario in cohort.SCENARIOS
    }
    counts = cohort._evidence_counts(
        [{"summaries": summaries}],
        [],
        unique_combine_path_count=2,
    )
    assert counts["combine_episode_horizon_observation_count_non_independent"] == 24
    assert counts["combine_pass_horizon_observation_count_non_independent"] == 6
    assert counts["unique_combine_path_count"] == 2
    assert "combine_pass_count" not in counts
