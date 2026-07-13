from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from scripts.annotate_v7_boosted_supersession import (
    ANNOTATION_EVENT_ID,
    annotate_supersession,
)


def test_supersession_is_idempotent_and_preserves_trials_and_windows(
    tmp_path: Path,
) -> None:
    proof = tmp_path / "proof_registry.json"
    proof.write_text(
        Path("mission/state/proof_registry.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    before = load_and_verify(proof)
    trials = multiplicity_trial_count(before)
    burned = burned_window_ids(before)
    output = tmp_path / "supersession.json"

    first = annotate_supersession(
        project_root=".", proof_registry_path=proof, output_path=output
    )
    second = annotate_supersession(
        project_root=".", proof_registry_path=proof, output_path=output
    )

    after = load_and_verify(proof)
    assert first == second
    assert multiplicity_trial_count(after) == trials == 266_954
    assert burned_window_ids(after) == burned == ("Q4_2024",)
    assert sum(
        row["event_id"] == ANNOTATION_EVENT_ID for row in after["entries"]
    ) == 1
    assert first["new_data_purchase_count"] == 0
    assert first["q4_access_count_delta"] == 0
    assert first["outbound_order_count"] == 0


def test_supersession_fails_closed_if_results_exist(tmp_path: Path) -> None:
    project = tmp_path / "project"
    report_dir = project / "reports/v7_boosted/tournament_0001"
    report_dir.mkdir(parents=True)
    amendment = Path("MISSION_CONTRACT_AMENDMENT_004_ECONOMIC_EVOLUTION.md")
    (project / amendment.name).write_text(
        amendment.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (report_dir / "unexpected_result.json").write_text(
        json.dumps({"result": True}), encoding="utf-8"
    )

    with pytest.raises(RuntimeError, match="results already exist"):
        annotate_supersession(
            project_root=project,
            proof_registry_path="mission/state/proof_registry.json",
            output_path="report.json",
        )
