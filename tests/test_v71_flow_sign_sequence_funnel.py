from __future__ import annotations

from pathlib import Path

from hydra.validation.v71_flow_sign_sequence_funnel import run_flow_sign_sequence_funnel


def test_flow_sign_sequence_funnel_preserves_stage_order(tmp_path: Path) -> None:
    result = run_flow_sign_sequence_funnel(
        project_root=".",
        proof_registry_path="mission/state/proof_registry.json",
        output_dir=tmp_path / "result",
    )
    assert result["candidate_count"] == 6
    assert result["stage0_valid_novel_count"] == 6
    assert result["candidate_nulls_executed"] is False
    assert result["DSR_BH_executed"] is False
    assert result["rolling_combine_executed"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["protected_holdout_access_count_delta"] == 0
    assert result["outbound_order_count"] == 0
