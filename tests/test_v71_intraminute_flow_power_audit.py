from __future__ import annotations

from pathlib import Path

from hydra.validation.v71_intraminute_flow_power_audit import run_intraminute_flow_power_audit


def test_intraminute_flow_power_audit_uses_calibrated_policy(tmp_path: Path) -> None:
    result = run_intraminute_flow_power_audit(project_root=".", proof_registry_path="mission/state/proof_registry.json", output_dir=tmp_path / "result")
    assert result["candidate_count"] == 2
    assert sum(result["status_counts"].values()) == 2
    assert result["universal_raw_event_threshold_used"] is False
    assert result["calibrated_candidate_specific_policy_used"] is True
    assert result["final_confirmation_power_requirement"] == 0.8
    assert result["candidate_nulls_executed"] is False
    assert result["DSR_BH_executed"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["outbound_order_count"] == 0
