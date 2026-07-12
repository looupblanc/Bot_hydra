from __future__ import annotations

from pathlib import Path

from hydra.validation.v71_opportunity_density_funnel import (
    run_opportunity_density_funnel,
)


def test_opportunity_density_funnel_preserves_stage_order(
    tmp_path: Path,
) -> None:
    result = run_opportunity_density_funnel(
        project_root=".", output_dir=tmp_path
    )

    assert result["candidate_count"] == 128
    assert result["stage1_pass_count"] <= result["stage0_valid_novel_count"]
    assert result["walk_forward_positive_count"] <= result["stage1_pass_count"]
    assert (
        result["powered_walk_forward_candidate_count"]
        <= result["walk_forward_positive_count"]
    )
    assert result["candidate_nulls_executed"] is False
    assert result["DSR_BH_executed"] is False
    assert result["rolling_combine_executed"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["protected_holdout_access_count_delta"] == 0
    assert result["outbound_order_count"] == 0
    for row in result["candidate_results"]:
        if row["powered_for_DSR_BH"]:
            assert row["walk_forward_positive"] is True
            assert row["walk_forward"]["retained_event_count"] >= 320
        if row["duplicate_of"]:
            assert row["stage0_valid_novel"] is False
