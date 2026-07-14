from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.mission.economic_evolution_account_timeline_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
    load_and_verify_account_timeline_terminal_verdict,
)
from hydra.mission.economic_evolution_account_timeline_runtime import (
    CAMPAIGN_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME,
    verify_account_timeline_freeze,
)
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
    _load_and_verify_generic_account_pair_preregistration,
    load_and_verify_manifest_queue,
)
from hydra.mission.economic_evolution_runtime import (
    EconomicEvolutionRuntimeError,
)
from hydra.research.economic_evolution_account_timeline_campaign import (
    load_and_verify_account_timeline_result,
)
from hydra.research.economic_evolution_coverage_union_campaign import (
    load_and_verify_coverage_union_preregistration,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_test_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_test_queue(root: Path, queue: dict[str, object]) -> None:
    queue.pop("queue_hash", None)
    queue["queue_hash"] = stable_hash(queue)
    _write_test_json(
        root / "config/v7/economic_evolution_production_queue_0001.json",
        queue,
    )


def _minimal_test_queue(entry: dict[str, object]) -> dict[str, object]:
    return {
        "schema": "hydra_manifest_campaign_queue_v1",
        "entries": [entry],
        "runtime_policy": {
            "reload_queue_each_controller_step": True,
            "controller_source_change_for_new_manifest": False,
            "single_active_campaign": True,
            "single_authoritative_mission_writer": True,
        },
        "governance": {
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "broker_or_orders_allowed": False,
            "proof_window_consumption_allowed": False,
        },
    }


def _terminal_disable_queue_fixture(
    root: Path,
) -> tuple[dict[str, object], Path, Path]:
    campaign_id = "hydra_test_campaign_0099"
    status = "SELECTOR_PROCEDURE_FALSIFIED"
    reason = "NO_ELIGIBLE_DESIGN_SET_CHAMPION_TEST"
    receipt_path = root / "reports/test/terminal_receipt.json"
    verdict_path = root / "WORM/test-terminal-verdict.json"
    receipt: dict[str, object] = {
        "schema": "hydra_test_terminal_receipt_v1",
        "campaign_id": campaign_id,
        "terminal_status": status,
        "failure_code": reason,
    }
    receipt["receipt_hash"] = stable_hash(receipt)
    _write_test_json(receipt_path, receipt)
    receipt_file_sha256 = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    verdict: dict[str, object] = {
        "schema": "hydra_test_terminal_verdict_v1",
        "campaign_id": campaign_id,
        "terminal_status": status,
        "failure_code": reason,
        "immutable": True,
        "source_terminal_receipt": {
            "path": receipt_path.relative_to(root).as_posix(),
            "file_sha256": receipt_file_sha256,
            "receipt_hash": receipt["receipt_hash"],
        },
    }
    verdict["verdict_hash"] = stable_hash(verdict)
    _write_test_json(verdict_path, verdict)

    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "add", verdict_path.relative_to(root).as_posix()],
        cwd=root,
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=HYDRA Test",
            "-c",
            "user.email=hydra-test@example.invalid",
            "commit",
            "-q",
            "-m",
            "freeze terminal verdict",
        ],
        cwd=root,
        check=True,
    )
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    tag = "worm/test-terminal-verdict"
    subprocess.run(["git", "tag", tag, commit], cwd=root, check=True)

    entry: dict[str, object] = {
        "ordinal": 99,
        "campaign_id": campaign_id,
        "enabled": False,
        "terminal_disable": {
            "status": status,
            "reason": reason,
            "receipt_path": receipt_path.relative_to(root).as_posix(),
            "receipt_file_sha256": receipt_file_sha256,
            "receipt_hash": receipt["receipt_hash"],
            "verdict_path": verdict_path.relative_to(root).as_posix(),
            "verdict_file_sha256": hashlib.sha256(
                verdict_path.read_bytes()
            ).hexdigest(),
            "verdict_hash": verdict["verdict_hash"],
            "verdict_tag": tag,
            "verdict_commit": commit,
            "automatic_retry_allowed": False,
            "evidence_finalization_allowed": False,
        },
    }
    queue = _minimal_test_queue(entry)
    _write_test_queue(root, queue)
    return queue, receipt_path, verdict_path


def test_0012_terminal_verdict_matches_completed_economics() -> None:
    config = verify_account_timeline_freeze(ROOT)
    result = load_and_verify_account_timeline_result(
        ROOT / CAMPAIGN_OUTPUT_RELATIVE_PATH / CAMPAIGN_RESULT_NAME,
        config,
    )
    verdict = load_and_verify_account_timeline_terminal_verdict(
        ROOT,
        result=result,
    )
    assert verdict["terminal_decision"]["verdict"] == (
        "CLASS_TOMBSTONE_EXACT_GRAMMAR"
    )
    assert verdict["result"]["policies_with_at_least_one_combine_pass"] == 0
    assert verdict["graveyard_append"]["parameter_level_feedback"] is False


def test_manifest_queue_is_reloadable_and_campaign_entry_is_frozen() -> None:
    queue = load_and_verify_manifest_queue(ROOT)
    assert queue["runtime_policy"]["reload_queue_each_controller_step"] is True
    assert queue["runtime_policy"]["controller_source_change_for_new_manifest"] is False
    assert queue["governance"]["q4_access_allowed"] is False
    runtime = EconomicEvolutionManifestRuntime(ROOT, ROOT / "mission/state")
    config = runtime._verify_entry(queue["entries"][0])
    assert config["campaign_id"] == NEXT_CAMPAIGN_ID
    assert config["compute"]["account_worker_count"] == 3
    assert config["governance"]["broker_or_orders_allowed"] is False


def test_manifest_queue_validates_terminal_disable_worm_receipt(
    tmp_path: Path,
) -> None:
    _terminal_disable_queue_fixture(tmp_path)

    queue = load_and_verify_manifest_queue(tmp_path)

    terminal_disable = queue["entries"][0]["terminal_disable"]
    assert terminal_disable["status"] == "SELECTOR_PROCEDURE_FALSIFIED"
    assert terminal_disable["reason"] == "NO_ELIGIBLE_DESIGN_SET_CHAMPION_TEST"
    assert terminal_disable["automatic_retry_allowed"] is False
    assert terminal_disable["evidence_finalization_allowed"] is False


def test_manifest_queue_keeps_historical_disabled_entry_compatible(
    tmp_path: Path,
) -> None:
    queue = _minimal_test_queue(
        {"ordinal": 98, "campaign_id": "historical_0098", "enabled": False}
    )
    _write_test_queue(tmp_path, queue)

    loaded = load_and_verify_manifest_queue(tmp_path)

    assert loaded["entries"][0]["enabled"] is False
    assert "terminal_disable" not in loaded["entries"][0]


def test_manifest_queue_rejects_terminal_disable_document_self_hash_drift(
    tmp_path: Path,
) -> None:
    queue, receipt_path, _ = _terminal_disable_queue_fixture(tmp_path)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["terminal_status"] = "SELECTOR_PROCEDURE_GREEN"
    _write_test_json(receipt_path, receipt)
    terminal_disable = queue["entries"][0]["terminal_disable"]
    terminal_disable["receipt_file_sha256"] = hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()
    _write_test_queue(tmp_path, queue)

    with pytest.raises(EconomicEvolutionRuntimeError, match="self-hash drift"):
        load_and_verify_manifest_queue(tmp_path)


def test_manifest_queue_rejects_terminal_disable_campaign_drift(
    tmp_path: Path,
) -> None:
    queue, receipt_path, _ = _terminal_disable_queue_fixture(tmp_path)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt.pop("receipt_hash")
    receipt["campaign_id"] = "different_campaign"
    receipt["receipt_hash"] = stable_hash(receipt)
    _write_test_json(receipt_path, receipt)
    terminal_disable = queue["entries"][0]["terminal_disable"]
    terminal_disable["receipt_file_sha256"] = hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()
    terminal_disable["receipt_hash"] = receipt["receipt_hash"]
    _write_test_queue(tmp_path, queue)

    with pytest.raises(
        EconomicEvolutionRuntimeError, match="campaign or status drift"
    ):
        load_and_verify_manifest_queue(tmp_path)


def test_manifest_queue_rejects_unanchored_terminal_receipt(
    tmp_path: Path,
) -> None:
    queue, _, verdict_path = _terminal_disable_queue_fixture(tmp_path)
    verdict = json.loads(verdict_path.read_text(encoding="utf-8"))
    verdict.pop("verdict_hash")
    verdict["source_terminal_receipt"]["receipt_hash"] = "0" * 64
    verdict["verdict_hash"] = stable_hash(verdict)
    _write_test_json(verdict_path, verdict)
    terminal_disable = queue["entries"][0]["terminal_disable"]
    terminal_disable["verdict_file_sha256"] = hashlib.sha256(
        verdict_path.read_bytes()
    ).hexdigest()
    terminal_disable["verdict_hash"] = verdict["verdict_hash"]
    _write_test_queue(tmp_path, queue)

    with pytest.raises(
        EconomicEvolutionRuntimeError, match="does not anchor the receipt"
    ):
        load_and_verify_manifest_queue(tmp_path)


def test_manifest_queue_rejects_terminal_failure_code_drift(
    tmp_path: Path,
) -> None:
    queue, receipt_path, verdict_path = _terminal_disable_queue_fixture(tmp_path)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt.pop("receipt_hash")
    receipt["failure_code"] = "DIFFERENT_FAILURE"
    receipt["receipt_hash"] = stable_hash(receipt)
    _write_test_json(receipt_path, receipt)
    receipt_file_sha256 = hashlib.sha256(receipt_path.read_bytes()).hexdigest()

    verdict = json.loads(verdict_path.read_text(encoding="utf-8"))
    verdict.pop("verdict_hash")
    verdict["source_terminal_receipt"] = {
        "path": receipt_path.relative_to(tmp_path).as_posix(),
        "file_sha256": receipt_file_sha256,
        "receipt_hash": receipt["receipt_hash"],
    }
    verdict["verdict_hash"] = stable_hash(verdict)
    _write_test_json(verdict_path, verdict)

    terminal_disable = queue["entries"][0]["terminal_disable"]
    terminal_disable["receipt_file_sha256"] = receipt_file_sha256
    terminal_disable["receipt_hash"] = receipt["receipt_hash"]
    terminal_disable["verdict_file_sha256"] = hashlib.sha256(
        verdict_path.read_bytes()
    ).hexdigest()
    terminal_disable["verdict_hash"] = verdict["verdict_hash"]
    _write_test_queue(tmp_path, queue)

    with pytest.raises(EconomicEvolutionRuntimeError, match="failure-code drift"):
        load_and_verify_manifest_queue(tmp_path)


def test_manifest_queue_rejects_terminal_disable_safety_or_worm_drift(
    tmp_path: Path,
) -> None:
    queue, _, _ = _terminal_disable_queue_fixture(tmp_path)
    terminal_disable = queue["entries"][0]["terminal_disable"]
    terminal_disable["automatic_retry_allowed"] = True
    _write_test_queue(tmp_path, queue)

    with pytest.raises(EconomicEvolutionRuntimeError, match="safety boundary"):
        load_and_verify_manifest_queue(tmp_path)

    terminal_disable["automatic_retry_allowed"] = False
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=HYDRA Test",
            "-c",
            "user.email=hydra-test@example.invalid",
            "commit",
            "--allow-empty",
            "-q",
            "-m",
            "unrelated successor",
        ],
        cwd=tmp_path,
        check=True,
    )
    terminal_disable["verdict_commit"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True
    ).strip()
    _write_test_queue(tmp_path, queue)

    with pytest.raises(EconomicEvolutionRuntimeError, match="WORM tag drift"):
        load_and_verify_manifest_queue(tmp_path)


def test_manifest_queue_rejects_terminal_disable_tagged_blob_drift(
    tmp_path: Path,
) -> None:
    queue, _, verdict_path = _terminal_disable_queue_fixture(tmp_path)
    verdict = json.loads(verdict_path.read_text(encoding="utf-8"))
    verdict.pop("verdict_hash")
    verdict["post_tag_mutation"] = True
    verdict["verdict_hash"] = stable_hash(verdict)
    _write_test_json(verdict_path, verdict)
    terminal_disable = queue["entries"][0]["terminal_disable"]
    terminal_disable["verdict_file_sha256"] = hashlib.sha256(
        verdict_path.read_bytes()
    ).hexdigest()
    terminal_disable["verdict_hash"] = verdict["verdict_hash"]
    _write_test_queue(tmp_path, queue)

    with pytest.raises(EconomicEvolutionRuntimeError, match="tagged verdict blob"):
        load_and_verify_manifest_queue(tmp_path)


def test_manifest_runtime_requires_terminal_0012_predecessor(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    runtime = EconomicEvolutionManifestRuntime(ROOT, state)
    predecessor = {
        "action_type": "ECONOMIC_EVOLUTION_ACCOUNT_TIMELINE_0012_TOMBSTONED",
        "economic_account_timeline_terminal_state": "COMPLETE",
        "economic_account_timeline_parameter_rescue_allowed": False,
        "economic_account_timeline_same_class_relaunch_allowed": False,
        "next_experiment_id": NEXT_CAMPAIGN_ID,
    }
    runtime._verify_first_predecessor(predecessor, NEXT_CAMPAIGN_ID)


def test_production_queue_does_not_authorize_proof_data_or_orders() -> None:
    queue = json.loads(
        (ROOT / "config/v7/economic_evolution_production_queue.json").read_text()
    )
    assert queue["governance"] == {
        "q4_access_allowed": False,
        "new_data_purchase_allowed": False,
        "broker_or_orders_allowed": False,
        "proof_window_consumption_allowed": False,
    }


def test_manifest_retry_budget_is_scoped_to_immutable_revision() -> None:
    original = {
        "campaign_id": "campaign_0017",
        "preregistration_hash": "a" * 64,
    }
    revision = {
        "campaign_id": "campaign_0017",
        "preregistration_hash": "b" * 64,
    }
    assert EconomicEvolutionManifestRuntime._attempt_key(original) != (
        EconomicEvolutionManifestRuntime._attempt_key(revision)
    )


def test_0014_is_a_frozen_generic_account_pair_campaign() -> None:
    path = ROOT / "config/v7/economic_evolution_coverage_union_0014.json"
    campaign = load_and_verify_coverage_union_preregistration(path)
    generic = _load_and_verify_generic_account_pair_preregistration(path)
    assert campaign == generic
    assert campaign["structural_population"]["policy_pair_count"] == 512
    assert campaign["rolling_episode_policy"]["maximum_starts"] == 24
    assert campaign["runtime_manifest"]["engine"] == "manifest_account_pair_v1"
    assert campaign["governance"] == {
        "q4_access_allowed": False,
        "new_data_purchase_allowed": False,
        "network_access_allowed": False,
        "broker_or_orders_allowed": False,
    }
