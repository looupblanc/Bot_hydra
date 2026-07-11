from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.mission.calibration_retest_v3 import (
    DESIGN_VERSION,
    REQUIRED_MAP_TYPE,
    CalibrationRetestV3Error,
    run_calibration_affected_atom_retest_v3_design,
)
from hydra.mission.experiment_runner import run_experiment


ROOT = Path("/root/hydra-bot")
REPAIR_PATH = ROOT / (
    "reports/mission_experiments/contract_map_date_aware_repair_v1/"
    "contract_map_date_aware_repair.json"
)
INVALID_V2_PATH = ROOT / (
    "reports/mission_experiments/calibration_affected_atom_retest_execution_v1/"
    "calibration_affected_atom_retest_execution.json"
)
MAP_PATH = ROOT / (
    "data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
)
TASK_PATH = Path(__file__).resolve().parents[1] / (
    "reports/engineering/hydra_calibration_retest_v3_20260710.md"
)


def _kwargs() -> dict[str, object]:
    return {
        "contract_map_repair_result_path": REPAIR_PATH,
        "contract_map_repair_result_hash": (
            "a932819f1eb0b72557b39ea867d3e930fd7d9e9dcad3e4cb64e10a0bbe2abb0d"
        ),
        "contract_map_repair_file_sha256": (
            "9137d0850efae03a00c139b9628063a6b7237d4614979491956dca7063e5e1a9"
        ),
        "invalid_v2_execution_result_path": INVALID_V2_PATH,
        "invalid_v2_execution_result_hash": (
            "22123708ac5ce71d89a75b73d7f3b5ee03cfd87d48655f5e28e1d828ddb12de9"
        ),
        "invalid_v2_execution_file_sha256": (
            "34e4f5d937971f277d8b86d64c69e8078bb8ffbb7e5c9ed841a4409a42c75233"
        ),
        "repaired_map_path": MAP_PATH,
        "repaired_map_sha256": (
            "401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda"
        ),
        "repaired_roll_map_hash": (
            "705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208"
        ),
        "engineering_task_path": TASK_PATH,
        "engineering_task_sha256": (
            "2ad1137abe0ee83f7ec1ce21acd48749df7aeed465a48777fe90a9796f606de9"
        ),
        "code_commit": "test-commit",
    }


def test_v3_design_is_deterministic_fresh_and_freezes_repaired_map(tmp_path: Path) -> None:
    first = run_calibration_affected_atom_retest_v3_design(tmp_path / "first", **_kwargs())
    second = run_calibration_affected_atom_retest_v3_design(tmp_path / "second", **_kwargs())
    assert first["design_hash"] == second["design_hash"]
    assert (
        first["preregistration"]["preregistration_hash"]
        == second["preregistration"]["preregistration_hash"]
    )
    assert first["design_version"] == DESIGN_VERSION
    atoms = first["preregistration"]["atoms"]
    ids = {atom["atom_id"] for atom in atoms}
    assert len(atoms) == len(ids) == 6
    assert all(atom_id.endswith("_v3") for atom_id in ids)
    assert all(atom["version"] == 3 for atom in atoms)
    assert all(atom["decision_contract"]["integrity_invalid_v2_status_inherited"] is False for atom in atoms)

    invalid_v2 = json.loads(INVALID_V2_PATH.read_text(encoding="utf-8"))
    invalid_ids = {row["atom_id"] for row in invalid_v2["results"]}
    historical_ids = {row["historical_atom_id"] for row in invalid_v2["results"]}
    assert ids.isdisjoint(invalid_ids | historical_ids)
    manifest_map = first["source"]["development_data_manifest"]["contract_map"]
    assert manifest_map == {
        "path": str(MAP_PATH),
        "sha256": "401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda",
        "size_bytes": MAP_PATH.stat().st_size,
        "roll_map_hash": "705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208",
        "map_type": REQUIRED_MAP_TYPE,
    }
    assert first["governance"]["q4_access_count"] == 0
    assert first["governance"]["incremental_databento_cost_usd"] == 0.0


def test_v3_design_fails_closed_on_repaired_map_hash_change(tmp_path: Path) -> None:
    kwargs = _kwargs()
    kwargs["repaired_map_sha256"] = "0" * 64
    with pytest.raises(CalibrationRetestV3Error, match="repaired roll map"):
        run_calibration_affected_atom_retest_v3_design(tmp_path, **kwargs)


def test_runner_dispatches_v3_design(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake(output_dir: Path, **kwargs: object) -> dict[str, str]:
        captured.update({"output_dir": output_dir, **kwargs})
        return {"scientific_conclusion": "v3-designed"}

    monkeypatch.setattr(
        "hydra.mission.calibration_retest_v3.run_calibration_affected_atom_retest_v3_design",
        fake,
    )
    specification = {"experiment_id": "v3-design", "experiment_type": "calibration_affected_atom_retest_v3_design", **_kwargs()}
    result = run_experiment(specification, output_root=tmp_path)
    assert result["scientific_conclusion"] == "v3-designed"
    assert captured["repaired_roll_map_hash"] == _kwargs()["repaired_roll_map_hash"]


def test_runner_dispatches_v3_execution_with_required_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    def fake(output_dir: Path, **kwargs: object) -> dict[str, str]:
        captured.update({"output_dir": output_dir, **kwargs})
        return {"scientific_conclusion": "v3-executed"}

    monkeypatch.setattr(
        "hydra.mission.calibration_retest_execution.run_calibration_affected_atom_retest_execution",
        fake,
    )
    result = run_experiment(
        {
            "experiment_id": "v3-execution",
            "experiment_type": "calibration_affected_atom_retest_v3_execution",
            "design_preregistration_path": "prereg.json",
            "design_path": "design.json",
            "repaired_map_path": str(MAP_PATH),
            "code_commit": "commit",
        },
        output_root=tmp_path,
    )
    assert result["scientific_conclusion"] == "v3-executed"
    assert captured["required_contract_map_type"] == REQUIRED_MAP_TYPE
    assert captured["expected_design_version"] == DESIGN_VERSION
    assert captured["execution_version"] == "calibration_affected_atom_retest_execution_v3"
