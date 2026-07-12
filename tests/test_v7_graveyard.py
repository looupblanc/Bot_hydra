from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from hydra.research.v7_graveyard import (
    GraveyardError,
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


def test_grammar_results_feed_only_class_level_death_causes(tmp_path: Path) -> None:
    registry, phase2 = _sources(tmp_path)
    grammar = tmp_path / "grammar.json"
    grammar.write_text(
        json.dumps(
            {
                "grammar_id": "test_grammar",
                "selected_shadow_queue_candidate_ids": [],
                "candidate_results": [
                    {
                        "candidate_id": "never_persist_me",
                        "specification": {"mechanism_class": "clock_flow"},
                        "stage1_pass": False,
                        "stage2_pass": False,
                        "base": {"expectancy_per_trade": 0.0},
                        "stress_2x": {"expectancy_per_trade": 0.0},
                    },
                    {
                        "candidate_id": "also_private",
                        "specification": {"mechanism_class": "clock_flow"},
                        "stage1_pass": True,
                        "stage2_pass": False,
                        "base": {"expectancy_per_trade": 2.0},
                        "stress_2x": {"expectancy_per_trade": -0.1},
                    },
                    {
                        "candidate_id": "private_three",
                        "specification": {"mechanism_class": "inventory"},
                        "stage1_pass": True,
                        "stage2_pass": True,
                        "base": {"expectancy_per_trade": 3.0},
                        "stress_2x": {"expectancy_per_trade": 1.0},
                        "DSR": {"deflated_z": -0.2},
                        "BH": {"rejected": False},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "graveyard.db"

    result = build_graveyard(
        registry_path=registry,
        phase2_result_path=phase2,
        grammar_result_paths=[grammar],
        output_path=output,
    )

    assert result["new_grammar_tombstone_count"] == 3
    assert result["indexed_object_count"] == 115_446
    feedback = class_feedback(output)
    assert {
        (row["mechanism_class"], row["death_cause"], row["candidate_count"])
        for row in feedback
        if row["mechanism_class"] in {"clock_flow", "inventory"}
    } == {
        ("clock_flow", "INSUFFICIENT_EVENT_COUNT_OR_BASE_ECONOMICS", 1),
        ("clock_flow", "SIM_EXPLOIT", 1),
        ("inventory", "MULTIPLICITY_DEFLATION_FAILURE", 1),
    }
    conn = sqlite3.connect(output)
    stored = " ".join(
        str(value)
        for row in conn.execute("SELECT * FROM class_tombstones")
        for value in row
    )
    conn.close()
    assert "never_persist_me" not in stored
    assert "also_private" not in stored
    assert "private_three" not in stored


def test_graveyard_rejects_tombstoning_promoted_grammar(tmp_path: Path) -> None:
    registry, phase2 = _sources(tmp_path)
    grammar = tmp_path / "grammar.json"
    grammar.write_text(
        json.dumps(
            {
                "grammar_id": "promoted",
                "selected_shadow_queue_candidate_ids": ["survivor"],
                "candidate_results": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(GraveyardError, match="promotions"):
        build_graveyard(
            registry_path=registry,
            phase2_result_path=phase2,
            grammar_result_paths=[grammar],
            output_path=tmp_path / "graveyard.db",
        )
