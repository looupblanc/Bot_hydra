from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hydra.mission.post_retest_research import (
    PostRetestResearchDesignError,
    run_post_calibration_retest_research_design,
)
from hydra.mission.experiment_runner import run_experiment


def _stable_hash(payload: object) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _source_result(path: Path, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "calibration_affected_atom_retest_execution_v2",
        "scientific_conclusion": "ZERO_SURVIVAL_PERSISTS_UNDER_CORRECTED_RETEST_PIVOT_RESEARCH_GRAMMAR",
        "code_commit": "source-commit",
        "validator_controls_passed": True,
        "invariant_controls_all_rejected": True,
        "evidence_valid_for_decision_change": True,
        "calibration_sensitive_survivor_count": 0,
        "calibration_sensitive_survivor_ids": [],
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
        },
    }
    payload.update(overrides)
    payload["result_hash"] = _stable_hash(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


@pytest.mark.parametrize(
    ("overrides", "expected_branch", "expected_pilot"),
    [
        (
            {
                "scientific_conclusion": (
                    "CALIBRATION_FALSE_KILLS_PLAUSIBLE_BOUNDED_FAMILIES_MAY_BE_REOPENED_FOR_FRESH_REPLICATION"
                ),
                "calibration_sensitive_survivor_count": 2,
                "calibration_sensitive_survivor_ids": ["fresh-a", "fresh-b"],
            },
            "SURVIVOR_REPLICATION",
            "fresh_survivor_family_replication_pilot",
        ),
        (
            {},
            "ZERO_SURVIVAL_GEOMETRY_PIVOT",
            "counterfactual_market_state_geometry_pilot",
        ),
        (
            {
                "scientific_conclusion": "CALIBRATION_RETEST_INSUFFICIENT_NO_ZERO_SURVIVAL_CONCLUSION",
                "evidence_valid_for_decision_change": True,
            },
            "INSUFFICIENT_POWER_RESOLUTION",
            "calibration_retest_power_resolution_pilot",
        ),
        (
            {
                "scientific_conclusion": "INVALID_VALIDATOR_CALIBRATION_CONTROLS_FAILED_NO_DECISION_CHANGE",
                "validator_controls_passed": False,
                "evidence_valid_for_decision_change": False,
            },
            "INVALID_VALIDATOR_INTEGRITY_REPAIR",
            "validator_integrity_repair_pilot",
        ),
    ],
)
def test_post_retest_design_selects_exactly_one_frozen_branch(
    tmp_path: Path,
    overrides: dict[str, object],
    expected_branch: str,
    expected_pilot: str,
) -> None:
    source_path = tmp_path / "source.json"
    source = _source_result(source_path, **overrides)
    result = run_post_calibration_retest_research_design(
        tmp_path / "output",
        source_execution_result_path=source_path,
        source_execution_result_hash=str(source["result_hash"]),
        source_execution_experiment_id="execution-v1",
        source_execution_specification_hash="execution-spec-hash",
        code_commit="post-design-commit",
    )
    assert result["selected_branch"] == expected_branch
    assert result["pilot_experiment_type"] == expected_pilot
    assert result["pilot_experiment_specification"]["experiment_type"] == expected_pilot
    assert result["pilot_experiment_specification"]["q4_access_allowed"] is False
    assert result["pilot_experiment_specification"]["paid_data_allowed"] is False
    assert result["engineering_task_specification"]["maximum_automatic_retries"] == 2
    assert set(result["paths"]) == {"design", "report", "engineering_task"}

    repeated = run_post_calibration_retest_research_design(
        tmp_path / "output",
        source_execution_result_path=source_path,
        source_execution_result_hash=str(source["result_hash"]),
        source_execution_experiment_id="execution-v1",
        source_execution_specification_hash="execution-spec-hash",
        code_commit="post-design-commit",
    )
    assert repeated["design_hash"] == result["design_hash"]


def test_post_retest_design_rejects_source_hash_or_immutable_output_change(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    source = _source_result(source_path)
    with pytest.raises(PostRetestResearchDesignError, match="hash mismatch"):
        run_post_calibration_retest_research_design(
            tmp_path / "wrong-hash",
            source_execution_result_path=source_path,
            source_execution_result_hash="not-the-frozen-hash",
            source_execution_experiment_id="execution-v1",
            source_execution_specification_hash="execution-spec-hash",
            code_commit="post-design-commit",
        )

    run_post_calibration_retest_research_design(
        tmp_path / "immutable",
        source_execution_result_path=source_path,
        source_execution_result_hash=str(source["result_hash"]),
        source_execution_experiment_id="execution-v1",
        source_execution_specification_hash="execution-spec-hash",
        code_commit="post-design-commit",
    )
    with pytest.raises(PostRetestResearchDesignError, match="Refusing to overwrite immutable"):
        run_post_calibration_retest_research_design(
            tmp_path / "immutable",
            source_execution_result_path=source_path,
            source_execution_result_hash=str(source["result_hash"]),
            source_execution_experiment_id="execution-v1",
            source_execution_specification_hash="execution-spec-hash",
            code_commit="different-post-design-commit",
        )


def test_post_retest_design_rejects_unsafe_source_boundary(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    source = _source_result(
        source_path,
        governance={
            "q4_access_count_delta": 1,
            "latest_data_end_exclusive": "2025-01-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
        },
    )
    with pytest.raises(PostRetestResearchDesignError, match="safe development-data boundary"):
        run_post_calibration_retest_research_design(
            tmp_path / "output",
            source_execution_result_path=source_path,
            source_execution_result_hash=str(source["result_hash"]),
            source_execution_experiment_id="execution-v1",
            source_execution_specification_hash="execution-spec-hash",
            code_commit="post-design-commit",
        )


def test_experiment_runner_dispatches_post_retest_design_handler(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    source = _source_result(source_path)
    result = run_experiment(
        {
            "experiment_id": "post-design-dispatch",
            "experiment_type": "post_calibration_retest_research_design",
            "source_execution_result_path": str(source_path),
            "source_execution_result_hash": str(source["result_hash"]),
            "source_execution_experiment_id": "execution-v1",
            "source_execution_specification_hash": "execution-spec-hash",
            "code_commit": "post-design-commit",
        },
        output_root=tmp_path / "runner-output",
    )
    assert result["selected_branch"] == "ZERO_SURVIVAL_GEOMETRY_PIVOT"
    assert Path(result["paths"]["design"]).is_file()
