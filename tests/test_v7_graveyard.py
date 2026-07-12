from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from hydra.research.v7_graveyard import (
    audit_graveyard,
    build_graveyard,
    class_feedback,
)


def _sources(tmp_path: Path) -> tuple[Path, Path]:
    registry = tmp_path / "registry.db"
    conn = sqlite3.connect(registry)
    conn.execute(
        "CREATE TABLE candidates(family TEXT,rejection_reason TEXT)"
    )
    conn.executemany(
        "INSERT INTO candidates VALUES(?,?)",
        [("old_a", "no_edge"), ("old_a", "no_edge"), ("old_b", "cost")],
    )
    conn.commit()
    conn.close()
    phase2 = tmp_path / "phase2.json"
    phase2.write_text(
        json.dumps({"verdict": "NULL", "candidate_count": 55}),
        encoding="utf-8",
    )
    return registry, phase2


def test_graveyard_exposes_only_class_level_feedback(tmp_path: Path) -> None:
    registry, phase2 = _sources(tmp_path)
    output = tmp_path / "graveyard.db"

    result = build_graveyard(
        registry_path=registry,
        phase2_result_path=phase2,
        output_path=output,
    )

    assert result["integrity"] == "ok"
    assert result["legacy_indexed_count"] == 115_388
    assert result["indexed_object_count"] == 115_443
    assert result["parameter_level_columns"] == []
    feedback = class_feedback(output)
    assert feedback
    assert set(feedback[0]) == {
        "mechanism_class",
        "regime",
        "death_cause",
        "candidate_count",
    }
    assert all("parameter" not in key for row in feedback for key in row)


def test_graveyard_build_is_deterministic_at_logical_level(tmp_path: Path) -> None:
    registry, phase2 = _sources(tmp_path)
    first = tmp_path / "first.db"
    second = tmp_path / "second.db"

    build_graveyard(
        registry_path=registry,
        phase2_result_path=phase2,
        output_path=first,
    )
    build_graveyard(
        registry_path=registry,
        phase2_result_path=phase2,
        output_path=second,
    )

    assert audit_graveyard(first) == audit_graveyard(second)
    assert class_feedback(first) == class_feedback(second)


def test_graveyard_never_persists_candidate_or_threshold_columns(
    tmp_path: Path,
) -> None:
    registry, phase2 = _sources(tmp_path)
    output = tmp_path / "graveyard.db"
    build_graveyard(
        registry_path=registry,
        phase2_result_path=phase2,
        output_path=output,
    )
    conn = sqlite3.connect(output)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(class_tombstones)")}
    conn.close()

    assert "candidate_id" not in columns
    assert "threshold" not in columns
    assert "parameters_json" not in columns
