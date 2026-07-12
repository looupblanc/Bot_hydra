from __future__ import annotations

from pathlib import Path

from hydra.validation.v71_event_time_funnel import run_event_time_funnel


def test_event_time_funnel_preserves_stage_order(tmp_path: Path) -> None:
    result = run_event_time_funnel(project_root=".", output_dir=tmp_path)

    assert result["candidate_count"] == 128
    assert result["stage1_pass_count"] <= result["stage0_valid_novel_count"]
    assert result["walk_forward_positive_count"] <= result["stage1_pass_count"]
    assert (
        result["powered_walk_forward_candidate_count"]
        <= result["walk_forward_positive_count"]
    )
    assert result["source_audit"]["cross_chicago_date_count"] == 122
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
