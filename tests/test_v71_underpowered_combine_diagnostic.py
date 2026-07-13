from __future__ import annotations

import shutil
from pathlib import Path

from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.validation.v71_underpowered_combine_diagnostic import (
    EXPECTED_GLOBAL_N_TRIALS,
    run_underpowered_combine_diagnostic,
)


def test_underpowered_combine_diagnostic_is_bounded_and_nonpromotional(
    tmp_path: Path,
) -> None:
    proof = tmp_path / "proof_registry.json"
    shutil.copyfile("mission/state/proof_registry.json", proof)
    current = multiplicity_trial_count(load_and_verify(proof))
    if current < EXPECTED_GLOBAL_N_TRIALS:
        append_entry(
            proof,
            {
                "event_id": "test_v71_underpowered_combine_reservation",
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
    result = run_underpowered_combine_diagnostic(
        project_root=".",
        proof_registry_path=proof,
        output_dir=tmp_path / "result",
    )
    assert result["candidate_count"] == 5
    assert result["episode_start_count"] == 24
    assert result["effective_nonoverlapping_block_count"] == 4
    assert all(
        row["diagnostic_status"] == "PROMISING_UNDERPOWERED_COMBINE_RESEARCH"
        for row in result["candidate_results"].values()
    )
    assert all(
        all(variant["episode_count"] == 24 for variant in row["variants"].values())
        for row in result["candidate_results"].values()
    )
    assert result["final_power_gate_passed_count"] == 0
    assert result["shadow_promotion_authorized"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["broker_or_order_capability"] is False
    assert result["outbound_order_count"] == 0
