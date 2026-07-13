from __future__ import annotations

from pathlib import Path

from hydra.validation.v72_flow_impact_relaxation_power_audit import (
    EXPECTED_GLOBAL_N_TRIALS,
    run_flow_impact_relaxation_power_audit,
)


def test_g10_power_audit_uses_frozen_calibrated_policy(tmp_path: Path) -> None:
    result = run_flow_impact_relaxation_power_audit(
        project_root=".",
        proof_registry_path="mission/state/proof_registry.json",
        output_dir=tmp_path / "result",
    )
    assert result["candidate_count"] == 4
    assert result["universal_raw_event_threshold_used"] is False
    assert result["calibrated_candidate_specific_policy_used"] is True
    assert sum(result["status_counts"].values()) == 4
    assert result["raw_global_N_trials_at_reservation"] == EXPECTED_GLOBAL_N_TRIALS
    assert result["raw_global_N_trials_current"] >= EXPECTED_GLOBAL_N_TRIALS
    assert result["campaign_effective_N_trials_after_audit_reservation"] == 294.0
    assert result["candidate_nulls_executed"] is False
    assert result["DSR_BH_executed"] is False
    assert result["rolling_combine_executed"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["protected_holdout_access_count_delta"] == 0
    assert result["outbound_order_count"] == 0


def test_g10_power_audit_is_deterministic(tmp_path: Path) -> None:
    first = run_flow_impact_relaxation_power_audit(
        project_root=".",
        proof_registry_path="mission/state/proof_registry.json",
        output_dir=tmp_path / "first",
    )
    second = run_flow_impact_relaxation_power_audit(
        project_root=".",
        proof_registry_path="mission/state/proof_registry.json",
        output_dir=tmp_path / "second",
    )
    assert first["candidate_results"] == second["candidate_results"]
    assert first["status_counts"] == second["status_counts"]
