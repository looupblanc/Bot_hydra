from __future__ import annotations

from pathlib import Path

from hydra.validation.v71_cross_clock_flow_power_audit import (
    run_cross_clock_flow_power_audit,
)


def test_cross_clock_power_audit_uses_calibrated_policy(tmp_path: Path) -> None:
    result = run_cross_clock_flow_power_audit(project_root=".", output_dir=tmp_path)
    assert result["candidate_count"] == 2
    assert result["universal_raw_event_threshold_used"] is False
    assert result["calibrated_candidate_specific_policy_used"] is True
    assert sum(result["status_counts"].values()) == 2
    assert result["candidate_nulls_executed"] is False
    assert result["DSR_BH_executed"] is False
    assert result["rolling_combine_executed"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["outbound_order_count"] == 0
