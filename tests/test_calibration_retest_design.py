from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pytest

from hydra.mission.calibration_retest import (
    DEFAULT_HISTORICAL_PREREGISTRATION,
    DEFAULT_HISTORICAL_REPORT,
    CalibrationRetestDesignError,
    _group_attacks_by_class,
    run_calibration_affected_atom_retest_design,
)
from hydra.utils.config import project_path


SENSITIVE_IDS = [
    "atom_accepted_price_migration_old_region_reentry_MNQ_60_extreme_v1",
    "atom_effort_vs_progress_directional_pressure_without_progress_MES_60_low_v1",
    "atom_effort_vs_progress_directional_pressure_without_progress_YM_60_moderate_v1",
    "atom_defensive_portfolio_atom_shared_loss_risk_state_MYM_30_moderate_v1",
]
INVARIANT_IDS = [
    "atom_volatility_path_shape_failed_expansion_ES_60_moderate_v1",
    "atom_accepted_price_migration_extreme_dwell_ES_30_moderate_v1",
]


def test_historical_attack_ranking_uses_recalibrated_policy() -> None:
    grouped = _group_attacks_by_class(
        [
            "delayed_signal",
            "sign_flipped_signal",
            "block_shuffled_signal",
            "momentum_baseline",
            "mean_reversion_baseline",
            "session_only_baseline",
            "volatility_only_baseline",
            "opportunity_count_matched_random",
        ],
        family="effort_vs_progress",
    )
    assert set(grouped["ROBUSTNESS_DIAGNOSTIC"]) == {
        "delayed_signal",
        "sign_flipped_signal",
        "block_shuffled_signal",
        "momentum_baseline",
        "mean_reversion_baseline",
        "session_only_baseline",
    }
    assert grouped["HYPOTHESIS_SPECIFIC_MANDATORY"] == ["volatility_only_baseline"]
    assert grouped["FATAL_MANDATORY"] == ["opportunity_count_matched_random"]

    accepted_price = _group_attacks_by_class(
        ["session_only_baseline", "volatility_only_baseline"],
        family="accepted_price_migration",
    )
    assert accepted_price["HYPOTHESIS_SPECIFIC_MANDATORY"] == ["session_only_baseline"]
    assert accepted_price["ROBUSTNESS_DIAGNOSTIC"] == ["volatility_only_baseline"]


def test_design_is_deterministic_and_all_artifact_hashes_verify(tmp_path: Path) -> None:
    first = run_calibration_affected_atom_retest_design(tmp_path / "first", code_commit="test-commit")
    second = run_calibration_affected_atom_retest_design(tmp_path / "second", code_commit="test-commit")
    repeated = run_calibration_affected_atom_retest_design(tmp_path / "first", code_commit="test-commit")

    runtime_path_keys = {"artifacts", "paths", "design_path", "preregistration_path", "report_path"}
    first_without_paths = {key: value for key, value in first.items() if key not in runtime_path_keys}
    second_without_paths = {key: value for key, value in second.items() if key not in runtime_path_keys}
    assert first_without_paths == second_without_paths
    assert repeated["design_hash"] == first["design_hash"]
    assert first["paths"] == {
        "design": first["design_path"],
        "preregistration": first["preregistration_path"],
        "report": first["report_path"],
    }
    for key in ("design_json_path", "preregistration_json_path", "report_path"):
        assert Path(first["artifacts"][key]).read_bytes() == Path(second["artifacts"][key]).read_bytes()

    design_on_disk = json.loads(Path(first["artifacts"]["design_json_path"]).read_text(encoding="utf-8"))
    design_hash = design_on_disk.pop("design_hash")
    assert design_hash == _stable_hash(design_on_disk)
    prereg = json.loads(Path(first["artifacts"]["preregistration_json_path"]).read_text(encoding="utf-8"))
    prereg_hash = prereg.pop("preregistration_hash")
    assert prereg_hash == _stable_hash(prereg)
    for atom in prereg["atoms"]:
        atom_hash = atom.pop("preregistration_hash")
        assert atom_hash == _stable_hash(atom)

    with pytest.raises(CalibrationRetestDesignError, match="Refusing to overwrite immutable"):
        run_calibration_affected_atom_retest_design(tmp_path / "first", code_commit="different-commit")


def test_selection_is_bounded_prioritized_fresh_and_never_inherits_status(tmp_path: Path) -> None:
    result = run_calibration_affected_atom_retest_design(tmp_path, code_commit="test-commit")
    assert result["selection"]["selected_sensitive_historical_atom_ids"] == SENSITIVE_IDS
    assert result["selection"]["selected_invariant_historical_atom_ids"] == INVARIANT_IDS
    assert result["selection"]["historical_atom_retest_count"] == 6
    assert result["selection"]["positive_control_count"] == 5
    assert result["selection"]["negative_control_count"] == 5

    historical_ids = set(SENSITIVE_IDS + INVARIANT_IDS)
    atoms = result["preregistration"]["atoms"]
    new_ids = {atom["atom_id"] for atom in atoms}
    assert len(new_ids) == len(atoms)
    assert not (new_ids & historical_ids)
    for atom in atoms:
        assert atom["version"] == 2
        assert atom["authoring_mode"] == "PREREGISTERED_BEFORE_RETEST"
        assert {"status", "passed", "validation_status"}.isdisjoint(atom)
        assert atom["historical_reference"]["historical_status_is_not_inherited"] is True
        assert atom["decision_contract"]["old_pass_status_inherited"] is False
        atom_without_hash = {key: value for key, value in atom.items() if key != "preregistration_hash"}
        assert atom["preregistration_hash"] == _stable_hash(atom_without_hash)

    roles = [atom["selection_role"] for atom in atoms]
    assert roles.count("CALIBRATION_SENSITIVE_CANDIDATE") == 4
    assert roles.count("CALIBRATION_INVARIANT_OLD_FAILURE") == 2
    assert result["preregistration"]["interpretation_policy"]["retest_pass_implies_atom_validated"] is False


def test_design_replaces_invalid_old_nulls_and_preregisters_causal_execution_contract(tmp_path: Path) -> None:
    result = run_calibration_affected_atom_retest_design(tmp_path, code_commit="test-commit")
    prereg = result["preregistration"]
    validity = prereg["implementation_validity_contract"]
    assert "training fold only" in validity["walk_forward_thresholds"]
    assert "Globex trading days jointly" in validity["clustered_block_uncertainty"]
    assert "explicit instrument/contract identity" in validity["explicit_contract_mapping"]
    assert "Match null opportunities" in validity["true_matched_nulls"]
    assert "direction exactly once" in validity["direction_single_application"]
    assert "Chicago-dated Globex session" in validity["group_safe_forward_target"]

    for atom in prereg["atoms"]:
        policy = atom["attack_policy"]
        assert {"target_leakage", "lookahead", "opportunity_session_volatility_matched_random"} <= set(
            policy["fatal_mandatory"]
        )
        assert policy["hypothesis_specific_mandatory"]
        assert "block_permuted_event_assignment" in policy["robustness_diagnostic"]
        assert "block_shuffled_signal" not in policy["hypothesis_specific_mandatory"]
        assert policy["retired_historical_nulls"]["block_shuffled_signal"].startswith(
            "MECHANICALLY_NONDISCRIMINATIVE"
        )
        assert {"event_time_jitter", "best_event_removed", "cost_stress"} <= set(policy["robustness_diagnostic"])
        assert {"sign_flipped_signal", "delayed_signal"} <= set(policy["robustness_diagnostic"])
        assert policy["informational_only"] == ["placebo_market"]
        assert atom["minimum_useful_effect"] > 0
        assert atom["falsification_criteria"]
        assert atom["cost_envelope"]["strategy_execution_cost_required_at_atom_scope"] is False
        uncertainties = set(atom["historical_implementation_uncertainties_requiring_repair"])
        assert "OLD_BLOCK_SHUFFLE_NULL_MECHANICALLY_NONDISCRIMINATIVE" in uncertainties
        assert "OLD_SESSION_BASELINE_NOT_OPPORTUNITY_OR_VOLATILITY_MATCHED" in uncertainties
        assert "CALENDAR_QUARTER_PROXY_IS_NOT_EXPLICIT_CONTRACT_IDENTITY" in uncertainties

    paired = prereg["paired_retest_groups"]
    assert len(paired) == 1
    assert paired[0]["group_id"] == "paired_effort_without_progress_mes_ym_v1"
    assert len(paired[0]["new_atom_ids"]) == 2

    defensive = next(atom for atom in prereg["atoms"] if atom["family"] == "defensive_portfolio_atom")
    assert defensive["target_variable"] == "future_standardized_tail_loss_hazard"
    assert defensive["effect_unit"] == "absolute_hazard_probability_difference"
    assert defensive["minimum_useful_effect"] == 0.02
    assert defensive["cost_envelope"]["historical_cost_proxy_unit_compatible_with_retest_target"] is False
    assert defensive["hazard_decision_contract"]["account_level_mll_evidence_claimed"] is False
    assert defensive["hazard_decision_contract"]["odds_ratio_confidence_lower_bound_threshold"] == 1.15


def test_ranking_exposes_sensitivity_edig_and_invariant_below_minimum(tmp_path: Path) -> None:
    result = run_calibration_affected_atom_retest_design(tmp_path, code_commit="test-commit")
    ranking = result["historical_decision_ranking"]
    assert len(ranking) == 25
    assert [row["rank"] for row in ranking] == list(range(1, 26))
    assert [row["expected_decision_information_gain"] for row in ranking] == sorted(
        (row["expected_decision_information_gain"] for row in ranking), reverse=True
    )
    assert ranking[0]["historical_atom_id"] == SENSITIVE_IDS[1]
    for row in ranking:
        assert set(row["sensitivity_components"]) == {
            "cost_hurdle_misapplication",
            "attack_policy_over_strictness",
            "sample_size_treatment",
            "scope_error",
        }
        assert 0.0 <= row["calibration_sensitivity_score"] <= 1.0
        assert row["expected_decision_information_gain"] >= 0.0
        assert row["expected_decision_information_gain_components"]["data_cost_usd"] == 0.0

    below_minimum = next(row for row in ranking if row["historical_atom_id"] == INVARIANT_IDS[1])
    assert abs(below_minimum["historical_raw_effect"]) < below_minimum["minimum_useful_effect"]
    assert below_minimum["decision_class"] == "CALIBRATION_INVARIANT_FAILURE"
    direction_opposed = next(row for row in ranking if row["historical_atom_id"] == INVARIANT_IDS[0])
    assert direction_opposed["historical_direction_ok"] is False
    assert direction_opposed["decision_class"] == "CALIBRATION_INVARIANT_FAILURE"


def test_design_fails_closed_on_q4_contamination_or_historical_hash_tampering(tmp_path: Path) -> None:
    report_path = project_path(*Path(DEFAULT_HISTORICAL_REPORT).parts)
    prereg_path = project_path(*Path(DEFAULT_HISTORICAL_PREREGISTRATION).parts)
    report_text = report_path.read_text(encoding="utf-8")
    match = re.search(r"```json\s*(\{.*\})\s*```", report_text, re.DOTALL)
    assert match
    report_payload = json.loads(match.group(1))
    report_payload["q4_access_count"] = 1
    contaminated_report = tmp_path / "contaminated.md"
    contaminated_report.write_text(
        "# contaminated fixture\n\n```json\n"
        + json.dumps(report_payload, indent=2, sort_keys=True)
        + "\n```\n",
        encoding="utf-8",
    )
    with pytest.raises(CalibrationRetestDesignError, match="Q4"):
        run_calibration_affected_atom_retest_design(
            tmp_path / "q4-output",
            historical_report_path=contaminated_report,
            historical_preregistration_path=prereg_path,
        )

    tampered_payload = json.loads(prereg_path.read_text(encoding="utf-8"))
    tampered_payload["atoms"][0]["parameters"]["threshold"] = "tampered_after_preregistration"
    tampered_prereg = tmp_path / "tampered.json"
    tampered_prereg.write_text(json.dumps(tampered_payload), encoding="utf-8")
    with pytest.raises(CalibrationRetestDesignError, match="hash mismatch"):
        run_calibration_affected_atom_retest_design(
            tmp_path / "tamper-output",
            historical_report_path=report_path,
            historical_preregistration_path=tampered_prereg,
        )


def test_design_is_explicitly_zero_q4_zero_network_and_zero_paid_data(tmp_path: Path) -> None:
    result = run_calibration_affected_atom_retest_design(tmp_path, code_commit="test-commit")
    assert result["governance"] == {
        "historical_research_only": True,
        "q4_accessed": False,
        "q4_access_count": 0,
        "latest_permitted_data_end_exclusive": "2024-10-01",
        "network_access": False,
        "paid_data_request_count": 0,
        "incremental_databento_cost_usd": 0.0,
        "broker_or_live_execution": False,
        "frozen_cached_inputs_only": True,
    }
    assert result["experiment_status"] == "COMPLETED_DESIGN_ONLY"
    assert "remains unresolved" in result["unresolved_question"]
    assert result["next_recommended_action"] == "EXECUTE_FRESH_PREREGISTERED_RETESTS_ON_DEVELOPMENT_DATA_ONLY"


def _stable_hash(payload: object) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
