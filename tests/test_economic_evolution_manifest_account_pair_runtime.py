from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionRuntimeError,
    _load_and_verify_generic_account_pair_preregistration,
    _load_and_verify_generic_account_pair_result,
)


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def _frozen_config(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    (tmp_path / "MISSION_CONTRACT.md").write_text("test contract\n")
    runner = tmp_path / "scripts/runner.py"
    runner.parent.mkdir()
    runner.write_text("raise SystemExit(0)\n")
    runner_sha = hashlib.sha256(runner.read_bytes()).hexdigest()
    config: dict[str, object] = {
        "schema": "hydra_manifest_account_pair_preregistration_v1",
        "campaign_id": "campaign_test_0014",
        "class_id": "TEST_ACCOUNT_PAIR_CLASS_V1",
        "structural_population": {
            "policy_pair_count": 512,
            "policy_manifest_hash": "population-hash",
        },
        "rolling_episode_policy": {"maximum_starts": 24},
        "compute": {"account_worker_count": 3},
        "data": {"role": "DEVELOPMENT_ONLY_Q4_EXCLUDED"},
        "statuses": {
            "development_only": True,
            "validated_allowed": False,
            "pre_holdout_ready_allowed": False,
            "paper_shadow_ready_allowed": False,
            "status_inheritance": False,
        },
        "governance": {
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "broker_or_orders_allowed": False,
        },
        "implementation_files": {"scripts/runner.py": runner_sha},
        "runtime_manifest": {
            "engine": "manifest_account_pair_v1",
            "runner": "scripts/runner.py",
            "result_schema": "hydra_test_account_pair_result_v1",
            "controller_source_change_required": False,
        },
    }
    config["preregistration_hash"] = stable_hash(config)
    path = tmp_path / "config/v7/campaign.json"
    _write_json(path, config)
    return path, config


def test_generic_account_pair_manifest_and_result_are_fail_closed(
    tmp_path: Path,
) -> None:
    path, config = _frozen_config(tmp_path)
    loaded = _load_and_verify_generic_account_pair_preregistration(path)
    assert loaded["campaign_id"] == "campaign_test_0014"

    result: dict[str, object] = {
        "schema": "hydra_test_account_pair_result_v1",
        "campaign_id": "campaign_test_0014",
        "class_id": "TEST_ACCOUNT_PAIR_CLASS_V1",
        "population": {
            "manifest_hash": "population-hash",
            "real_policy_count": 512,
            "matched_control_policy_count": 512,
        },
        "policy_pair_evaluated_count": 512,
        "account_policy_economics": {
            "primary_rolling_combine_episode_count": 12_288
        },
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "governance": {
            "proof_windows_consumed": 0,
            "new_data_purchase_count": 0,
            "q4_access_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        },
    }
    result["result_sha256"] = stable_hash(result)
    result_path = tmp_path / "reports/result.json"
    _write_json(result_path, result)
    assert _load_and_verify_generic_account_pair_result(result_path, config) == result

    result["governance"]["q4_access_delta"] = 1  # type: ignore[index]
    result["result_sha256"] = stable_hash(
        {key: value for key, value in result.items() if key != "result_sha256"}
    )
    _write_json(result_path, result)
    with pytest.raises(EconomicEvolutionRuntimeError):
        _load_and_verify_generic_account_pair_result(result_path, config)


def test_generic_manifest_rejects_runner_hash_drift(tmp_path: Path) -> None:
    path, _ = _frozen_config(tmp_path)
    (tmp_path / "scripts/runner.py").write_text("raise SystemExit(1)\n")
    with pytest.raises(EconomicEvolutionRuntimeError):
        _load_and_verify_generic_account_pair_preregistration(path)


def test_generic_manifest_accepts_frozen_confirmation_horizon(
    tmp_path: Path,
) -> None:
    path, config = _frozen_config(tmp_path)
    config["rolling_episode_policy"] = {"maximum_starts": 48}
    config["preregistration_hash"] = stable_hash(
        {
            key: value
            for key, value in config.items()
            if key != "preregistration_hash"
        }
    )
    _write_json(path, config)
    loaded = _load_and_verify_generic_account_pair_preregistration(path)
    assert loaded["rolling_episode_policy"]["maximum_starts"] == 48
