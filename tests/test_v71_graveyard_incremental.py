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
        mechanism_class="v71_test_incremental_class",
        regime="TEST_REGIME",
        death_cause="TEST_CLASS_LEVEL_DEATH",
        candidate_count=7,
        source_scope="TEST_ONLY",
        evidence_sha256="1" * 64,
    )
    first = append_class_tombstone(destination, row)
    second = append_class_tombstone(destination, row)
    assert first["append_status"] == "APPENDED"
    assert second["append_status"] == "ALREADY_PRESENT_IDENTICAL"
    assert first["class_signature_count"] == before["class_signature_count"] + 1
    assert first["indexed_object_count"] == before["indexed_object_count"] + 7
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
            "candidate_count": 7,
        }
    ]
