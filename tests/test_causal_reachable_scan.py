from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from hydra.validation.causal_reachable_scan import (
    CausalReachableScanError,
    ClassificationRule,
    FindingClassification,
    SCAN_FAILED,
    SCAN_PASS,
    ScanScope,
    run_causal_reachable_scan,
    scan_scoped_sources,
    stable_hash,
    verify_causal_reachable_scan,
)


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def real_scan() -> dict:
    return run_causal_reachable_scan(
        repository_root=ROOT,
        created_at=datetime(2026, 7, 17, tzinfo=UTC),
    )


def test_repaired_reachable_surface_has_no_decision_blocker(real_scan: dict) -> None:
    assert real_scan["status"] == SCAN_PASS
    assert real_scan["frozen_inventory"]["book_count"] == 6
    assert real_scan["frozen_inventory"]["sleeve_count"] == 18
    assert real_scan["blocking_findings"] == []
    assert any(
        row["module_path"] == "hydra/research/turbo_feature_builder.py"
        and row["primitive"] == "NEXT_ROW_EXECUTION_PRICE"
        and row["classification"] == "OUTCOME_LABEL_ONLY"
        for row in real_scan["findings"]
    )
    assert real_scan["classification_counts"]["LOOKAHEAD_DEFECT"] == 0
    assert real_scan["classification_counts"]["UNRESOLVED"] == 0
    verify_causal_reachable_scan(real_scan)


def test_unmatched_reachable_primitive_is_unresolved(tmp_path: Path) -> None:
    module = tmp_path / "reachable.py"
    module.write_text(
        "def decide(matrix):\n"
        "    forward = matrix.array(f'forward_move__{5}')\n"
        "    return forward\n",
        encoding="utf-8",
    )
    scopes = (
        ScanScope(
            "reachable.py",
            ("decide",),
            "TEST_DECISION",
            "Synthetic reachable decision function.",
        ),
    )
    findings, coverage = scan_scoped_sources(
        repository_root=tmp_path,
        scopes=scopes,
        rules=(),
    )
    assert coverage[0]["finding_count"] >= 1
    assert {row["classification"] for row in findings} == {"UNRESOLVED"}


def test_explicit_outcome_rule_needs_rationale_and_does_not_hide_scope(
    tmp_path: Path,
) -> None:
    module = tmp_path / "reachable.py"
    module.write_text(
        "def label(matrix):\n"
        "    forward = matrix.array(f'forward_move__{5}')\n"
        "    return forward\n"
        "\n"
        "def unrelated(matrix):\n"
        "    return matrix.array(f'forward_move__{99}')\n",
        encoding="utf-8",
    )
    scope = ScanScope(
        "reachable.py",
        ("label",),
        "TEST_LABEL",
        "Only the label function is reachable.",
    )
    with pytest.raises(CausalReachableScanError, match="rationale"):
        ClassificationRule(
            "bad",
            "reachable.py",
            "label",
            "FORWARD_LABEL_REFERENCE",
            FindingClassification.OUTCOME_LABEL_ONLY,
            "",
        )
    rules = (
        ClassificationRule(
            "label-only",
            "reachable.py",
            "label",
            "FORWARD_LABEL_REFERENCE",
            FindingClassification.OUTCOME_LABEL_ONLY,
            "This synthetic function returns an evaluation label only.",
        ),
        ClassificationRule(
            "label-value",
            "reachable.py",
            "label",
            "FUTURE_VALUE_REFERENCE",
            FindingClassification.OUTCOME_LABEL_ONLY,
            "The value remains inside the same label-only function.",
        ),
    )
    findings, coverage = scan_scoped_sources(
        repository_root=tmp_path,
        scopes=(scope,),
        rules=rules,
    )
    assert findings
    assert {row["classification"] for row in findings} == {"OUTCOME_LABEL_ONLY"}
    assert all(row["function"] == "label" for row in findings)
    assert coverage[0]["functions"] == ["label"]


def test_scan_hash_rejects_tampering(real_scan: dict) -> None:
    changed = dict(real_scan)
    changed["status"] = SCAN_FAILED
    with pytest.raises(CausalReachableScanError, match="hash drift"):
        verify_causal_reachable_scan(changed)

    changed = dict(real_scan)
    changed["status"] = SCAN_FAILED
    changed.pop("scan_hash")
    changed["scan_hash"] = stable_hash(changed)
    with pytest.raises(CausalReachableScanError, match="status drift"):
        verify_causal_reachable_scan(changed)
