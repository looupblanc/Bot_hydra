from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from hydra.mission.experiment_runner import run_experiment
from hydra.validation.retest_integrity_repair import (
    RetestIntegrityRepairError,
    audit_date_aware_contract_symbols,
    run_validator_integrity_repair_pilot,
)


BASE_COMMIT = "073d8cf9a68b608b234a45ea32374b664186e714"
SOURCE_HASH = ""


def _stable_hash(payload: object) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _write_hashed(path: Path, payload: dict, key: str) -> dict:
    result = dict(payload)
    result[key] = _stable_hash(result)
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def _definition_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_event": "2023-01-01T17:00:00Z",
                "instrument_id": 1,
                "raw_symbol": "DC:XS 16M BLK-CSC Z2",
                "instrument_class": "S",
                "security_type": "FUT",
                "asset": "BLK",
            },
            {
                "ts_event": "2024-03-17T11:04:10Z",
                "instrument_id": 1,
                "raw_symbol": "MESM4",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "MES",
            },
            {
                "ts_event": "2023-01-01T17:03:56Z",
                "instrument_id": 2,
                "raw_symbol": "ESM4",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "ES",
            },
        ]
    )


def _frozen_artifacts(tmp_path: Path) -> dict[str, object]:
    map_path = tmp_path / "roll_map.json"
    contract_map = {
        "map_type": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
        "contracts": [
            {
                "root": "MES",
                "contract": "DC:XS 16M BLK-CSC Z2",
                "instrument_id": "1",
                "active_start": "2024-03-17",
                "active_end": "2024-06-23",
            },
            {
                "root": "ES",
                "contract": "ESM4",
                "instrument_id": "2",
                "active_start": "2024-03-17",
                "active_end": "2024-06-23",
            },
        ],
    }
    map_path.write_text(json.dumps(contract_map), encoding="utf-8")
    map_sha = hashlib.sha256(map_path.read_bytes()).hexdigest()

    positive_targets = ["future_return"] * 45 + ["future_realized_volatility"] * 15 + [
        "future_standardized_tail_loss_hazard"
    ] * 15
    decisions = [
        {
            "control_id": f"positive_{index}",
            "target_kind": target,
            "expected_positive": True,
            "edge_detected": True,
            "status": "RETEST_SUPPORTS_FAMILY_REOPENING",
        }
        for index, target in enumerate(positive_targets)
    ] + [
        {
            "control_id": f"negative_{index}",
            "target_kind": "future_return",
            "expected_positive": False,
            "edge_detected": False,
            "status": "RETEST_FALSIFIED",
        }
        for index in range(75)
    ]
    source_path = tmp_path / "source.json"
    source = _write_hashed(
        source_path,
        {
            "scientific_conclusion": "INVALID_RETEST_INVARIANT_SENTINEL_INSUFFICIENT_NO_DECISION_CHANGE",
            "code_commit": BASE_COMMIT,
            "validator_controls": {
                "pipeline_v2_decisive": {
                    "decisions": decisions,
                    "power_on_meaningful_effects": 1.0,
                    "false_positive_rate": 0.0,
                    "passed": True,
                    "selection_universe_size_each_run": 25,
                    "seed_count": 15,
                }
            },
            "results": [
                {
                    "atom_id": "sentinel-1",
                    "historical_atom_id": "old-1",
                    "selection_role": "CALIBRATION_INVARIANT_OLD_FAILURE",
                    "status": "INVARIANT_CONTROL_INSUFFICIENT",
                    "reason": "full_matched_population_unavailable",
                },
                {
                    "atom_id": "sentinel-2",
                    "historical_atom_id": "old-2",
                    "selection_role": "CALIBRATION_INVARIANT_OLD_FAILURE",
                    "status": "INVARIANT_CONTROL_INSUFFICIENT",
                    "insufficient_gates": ["explicit_contract_replication"],
                },
            ],
            "data_provenance": {
                "contract_map_path": str(map_path),
                "contract_map_sha256": map_sha,
            },
            "governance": {
                "q4_access_count_delta": 0,
                "incremental_databento_spend_usd": 0.0,
                "network_requests": 0,
                "live_or_broker_execution": False,
                "latest_data_end_exclusive": "2024-10-01",
            },
        },
        "result_hash",
    )
    task_path = tmp_path / "task.json"
    task = _write_hashed(
        task_path,
        {
            "schema": "hydra_immutable_engineering_task_v1",
            "immutable_before_implementation": True,
            "selected_branch": "INVALID_VALIDATOR_INTEGRITY_REPAIR",
            "pilot_experiment_type": "validator_integrity_repair_pilot",
            "source_execution_result_hash": source["result_hash"],
            "code_commit": BASE_COMMIT,
        },
        "engineering_task_hash",
    )
    design_path = tmp_path / "design.json"
    design = _write_hashed(
        design_path,
        {
            "selected_branch": "INVALID_VALIDATOR_INTEGRITY_REPAIR",
            "engineering_task_specification": task,
        },
        "design_hash",
    )
    definition_path = tmp_path / "definitions.dbn.zst"
    definition_path.write_bytes(b"cached-definition-fixture")
    return {
        "source_path": source_path,
        "source": source,
        "task_path": task_path,
        "task": task,
        "design_path": design_path,
        "design": design,
        "definition_path": definition_path,
    }


def test_date_aware_audit_isolates_flattened_instrument_id_defect() -> None:
    result = audit_date_aware_contract_symbols(
        {
            "map_type": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
            "contracts": [
                {
                    "root": "MES",
                    "contract": "DC:XS 16M BLK-CSC Z2",
                    "instrument_id": "1",
                    "active_start": "2024-03-17",
                    "active_end": "2024-06-23",
                }
            ],
        },
        _definition_frame(),
    )
    assert result["invalid_frozen_map_symbol_count"] == 1
    assert result["date_aware_valid_future_symbol_count"] == 1
    assert result["date_aware_symbol_change_count"] == 1
    assert result["invalid_or_corrected_segments"][0]["date_aware_definition_symbol"] == "MESM4"


def test_pilot_confirms_map_defect_without_candidate_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frozen = _frozen_artifacts(tmp_path)
    monkeypatch.setattr(
        "hydra.validation.retest_integrity_repair._load_best_cached_definition_frame",
        lambda *_args, **_kwargs: (_definition_frame(), frozen["definition_path"]),
    )
    kwargs = {
        "source_execution_result_path": frozen["source_path"],
        "source_execution_result_hash": frozen["source"]["result_hash"],
        "source_execution_experiment_id": "calibration_affected_atom_retest_execution_v1",
        "source_execution_specification_hash": "spec-hash",
        "post_retest_design_path": frozen["design_path"],
        "engineering_task_path": frozen["task_path"],
        "engineering_task_hash": frozen["task"]["engineering_task_hash"],
        "selected_post_retest_branch": "INVALID_VALIDATOR_INTEGRITY_REPAIR",
        "code_commit": BASE_COMMIT,
    }
    first = run_validator_integrity_repair_pilot(tmp_path / "output", **kwargs)
    second = run_validator_integrity_repair_pilot(tmp_path / "output", **kwargs)
    assert first["result_hash"] == second["result_hash"]
    assert first["integrity_disposition"] == "CONTRACT_MAP_REBUILD_REQUIRED"
    assert first["validator_controls_calibrated"] is True
    assert first["candidate_evidence_rerun_count"] == 0
    assert first["contract_map_integrity_audit"]["invalid_frozen_map_symbol_count"] == 1
    assert first["governance"] == {
        "q4_access_count_delta": 0,
        "incremental_databento_spend_usd": 0.0,
        "network_requests": 0,
        "live_or_broker_execution": False,
        "market_observation_rows_read": 0,
        "cached_definition_metadata_only": True,
    }


def test_pilot_fails_closed_on_task_hash_tampering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frozen = _frozen_artifacts(tmp_path)
    task = json.loads(Path(frozen["task_path"]).read_text(encoding="utf-8"))
    task["selected_branch"] = "SURVIVOR_REPLICATION"
    Path(frozen["task_path"]).write_text(json.dumps(task), encoding="utf-8")
    monkeypatch.setattr(
        "hydra.validation.retest_integrity_repair._load_best_cached_definition_frame",
        lambda *_args, **_kwargs: (_definition_frame(), frozen["definition_path"]),
    )
    with pytest.raises(RetestIntegrityRepairError, match="hash mismatch"):
        run_validator_integrity_repair_pilot(
            tmp_path / "output",
            source_execution_result_path=frozen["source_path"],
            source_execution_result_hash=frozen["source"]["result_hash"],
            source_execution_experiment_id="execution",
            source_execution_specification_hash="spec",
            post_retest_design_path=frozen["design_path"],
            engineering_task_path=frozen["task_path"],
            engineering_task_hash=frozen["task"]["engineering_task_hash"],
            selected_post_retest_branch="INVALID_VALIDATOR_INTEGRITY_REPAIR",
            code_commit=BASE_COMMIT,
        )


def test_experiment_runner_dispatches_integrity_pilot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = {}

    def fake(output_dir: Path, **kwargs):
        captured.update({"output_dir": str(output_dir), **kwargs})
        return {"scientific_conclusion": "diagnosed"}

    monkeypatch.setattr(
        "hydra.validation.retest_integrity_repair.run_validator_integrity_repair_pilot", fake
    )
    result = run_experiment(
        {
            "experiment_id": "pilot",
            "experiment_type": "validator_integrity_repair_pilot",
            "source_execution_result_path": "source.json",
            "source_execution_result_hash": "source-hash",
            "source_execution_experiment_id": "execution",
            "source_execution_specification_hash": "spec-hash",
            "post_retest_design_path": "design.json",
            "engineering_task_path": "task.json",
            "engineering_task_hash": "task-hash",
            "selected_post_retest_branch": "INVALID_VALIDATOR_INTEGRITY_REPAIR",
            "code_commit": BASE_COMMIT,
        },
        output_root=tmp_path,
    )
    assert result["scientific_conclusion"] == "diagnosed"
    assert captured["source_execution_result_hash"] == "source-hash"
    assert captured["selected_post_retest_branch"] == "INVALID_VALIDATOR_INTEGRITY_REPAIR"
