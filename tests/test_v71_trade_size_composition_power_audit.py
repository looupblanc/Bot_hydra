from __future__ import annotations

import shutil
from pathlib import Path

from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.validation.v71_trade_size_composition_power_audit import (
    EXPECTED_GLOBAL_N_TRIALS,
    run_trade_size_composition_power_audit,
)


def test_trade_size_power_audit_uses_calibrated_policy(tmp_path: Path) -> None:
    proof = tmp_path / "proof_registry.json"
    shutil.copyfile("mission/state/proof_registry.json", proof)
    current = multiplicity_trial_count(load_and_verify(proof))
    if current < EXPECTED_GLOBAL_N_TRIALS:
        append_entry(
            proof,
            {
                "event_id": "test_v71_g6_power_reservation",
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": "2026-07-13T00:00:00Z",
                "status": "TEST_RESERVATION",
                "multiplicity": {
                    "previous_N_trials": current,
                    "delta_trials": EXPECTED_GLOBAL_N_TRIALS - current,
                    "cumulative_N_trials": EXPECTED_GLOBAL_N_TRIALS,
                    "method": "test-only copy of the append-only registry",
                },
                "evidence": {"test_only": True},
            },
        )
    result = run_trade_size_composition_power_audit(
        project_root=".",
        proof_registry_path=proof,
        output_dir=tmp_path / "result",
    )
    assert result["candidate_count"] == 2
    assert result["universal_raw_event_threshold_used"] is False
    assert result["calibrated_candidate_specific_policy_used"] is True
    assert sum(result["status_counts"].values()) == 2
    assert result["candidate_nulls_executed"] is False
    assert result["DSR_BH_executed"] is False
    assert result["rolling_combine_executed"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["outbound_order_count"] == 0
