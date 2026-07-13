from __future__ import annotations

from hydra.validation.v72_component_bank import (
    build_behavioral_clusters,
    select_cluster_representatives,
)


def _row(
    candidate_id: str,
    family: str,
    *,
    year_fraction: float,
    stressed: float,
    effective: float,
) -> dict:
    return {
        "candidate_id": candidate_id,
        "family_id": family,
        "stability": {"calendar_year": {"positive_fraction": year_fraction}},
        "cost_results": {"STRESS_2X": {"mean_net": stressed}},
        "effective_sample": {"effective_independent_event_count": effective},
    }


def test_behavioral_clusters_merge_family_and_equivalent_paths() -> None:
    rows = [
        _row("a", "FAMILY_1", year_fraction=1.0, stressed=10.0, effective=20.0),
        _row("b", "FAMILY_1", year_fraction=1.0, stressed=20.0, effective=10.0),
        _row("c", "FAMILY_2", year_fraction=1.0, stressed=30.0, effective=30.0),
        _row("d", "FAMILY_3", year_fraction=0.5, stressed=40.0, effective=40.0),
    ]
    comparisons = [
        {
            "left_candidate_id": "b",
            "right_candidate_id": "c",
            "daily_pnl_correlation_defined": True,
            "daily_pnl_correlation": 0.8,
            "signal_timestamp_jaccard": 0.0,
        },
        {
            "left_candidate_id": "c",
            "right_candidate_id": "d",
            "daily_pnl_correlation_defined": True,
            "daily_pnl_correlation": 0.1,
            "signal_timestamp_jaccard": 0.1,
        },
    ]
    clusters = build_behavioral_clusters(
        rows,
        comparisons,
        maximum_absolute_correlation=0.7,
        maximum_signal_jaccard=0.7,
    )

    assert clusters["a"] == clusters["b"] == clusters["c"]
    assert clusters["d"] != clusters["a"]


def test_primary_selection_is_frozen_lexicographic_and_one_backup() -> None:
    rows = [
        _row("a", "FAMILY", year_fraction=1.0, stressed=10.0, effective=100.0),
        _row("b", "FAMILY", year_fraction=1.0, stressed=20.0, effective=10.0),
        _row("c", "FAMILY", year_fraction=0.5, stressed=100.0, effective=100.0),
    ]
    selections = select_cluster_representatives(
        rows, {"a": "cluster", "b": "cluster", "c": "cluster"}
    )

    assert selections[0]["primary_candidate_id"] == "b"
    assert selections[0]["backup_candidate_id"] == "a"
    assert selections[0]["excluded_additional_member_ids"] == ["c"]
