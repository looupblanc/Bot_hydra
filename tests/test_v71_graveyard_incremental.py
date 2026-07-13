from __future__ import annotations

import shutil
from pathlib import Path

from hydra.research.v7_graveyard import (
    ClassTombstone,
    append_class_tombstone,
    audit_graveyard,
    class_feedback,
)


def test_incremental_graveyard_append_is_class_only_and_idempotent(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "graveyard.db"
    shutil.copy2("mission/state/graveyard.db", destination)
    before = audit_graveyard(destination)
    row = ClassTombstone(
        mechanism_class="v71g5_cross_clock_speed_leadership",
        regime="D1_2023_2024_DATE_MATCHED_BLOCKS",
        death_cause="GEOMETRY_ONLY_NULL_RATIO_GTE_0_8",
        candidate_count=12,
        source_scope="HYDRA_V7_GRAMMAR:hydra_v7_1_cross_clock_speed_leadership_grammar_0005",
        evidence_sha256="ea7755aa5ab60f78298557da422d497d98467457a24a259ff3f3a9919048fc1d",
    )
    first = append_class_tombstone(destination, row)
    second = append_class_tombstone(destination, row)
    assert first["append_status"] == "APPENDED"
    assert second["append_status"] == "ALREADY_PRESENT_IDENTICAL"
    assert first["class_signature_count"] == before["class_signature_count"] + 1
    assert first["indexed_object_count"] == before["indexed_object_count"] + 12
    matched = [
        item
        for item in class_feedback(destination)
        if item["mechanism_class"] == row.mechanism_class
    ]
    assert matched == [
        {
            "mechanism_class": row.mechanism_class,
            "regime": row.regime,
            "death_cause": row.death_cause,
            "candidate_count": 12,
        }
    ]
