from __future__ import annotations

from pathlib import Path

from hydra.validation.v71_underpowered_combine_selection import (
    build_underpowered_combine_selection,
)


def test_underpowered_combine_selection_is_distinct_and_fully_reconciled(
    tmp_path: Path,
) -> None:
    first = build_underpowered_combine_selection(
        project_root=".", output_dir=tmp_path / "first"
    )
    second = build_underpowered_combine_selection(
        project_root=".", output_dir=tmp_path / "second"
    )
    first_ids = [row["candidate_id"] for row in first["selected_candidates"]]
    second_ids = [row["candidate_id"] for row in second["selected_candidates"]]
    assert first_ids == second_ids
    assert 3 <= len(first_ids) <= 5
    assert len({row["family_id"] for row in first["selected_candidates"]}) == len(
        first_ids
    )
    assert all(
        row["diagnostic_status"] == "PROMISING_UNDERPOWERED_COMBINE_RESEARCH"
        for row in first["selected_candidates"]
    )
    assert first["population_reconciliation"]["after_G6_accounted_count"] == 22
    assert first["population_reconciliation"]["unaccounted_count"] == 0
    assert first["contains_combine_results"] is False
    assert first["shadow_promotion_authorized"] is False
    assert first["new_data_purchase_count"] == 0
    assert first["outbound_order_count"] == 0
