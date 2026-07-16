from __future__ import annotations

import copy
import inspect
import json
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.compute.result_writer import AtomicResultWriter
import scripts.recover_active_risk_0026_result_once as recovery_module
from scripts.recover_active_risk_0026_result_once import (
    CAMPAIGN_ID,
    ATTEMPT_NAME,
    EXPECTED_GROUPS,
    EXPECTED_HORIZONS,
    EXPECTED_PERSISTED_EPISODE_ROWS,
    MANIFEST_HASH,
    PREFLIGHT_PROOF_NAME,
    PREFLIGHT_RESULT_NAME,
    RECOVERY_RECEIPT_NAME,
    RECOVERY_VERSION,
    SOURCE_COMMIT,
    ControllerHandoffRequired,
    RecoveryError,
    _poll_for_controller_completion,
    _load_result_after_two_preregistered_guards,
    _preflight_validate_and_publish_result,
    _resume_terminalization_only,
    _sha256,
    _write_attempt_marker,
    normalized_terminal_kpis,
    recover_once,
    validate_episode_partition_accounting,
)


def _fixture() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    files: dict[str, Any] = {}
    part_index = 0
    for prefix, contract in EXPECTED_GROUPS.items():
        partitions = int(contract["partition_count"])
        total_rows = int(contract["persisted_rows"])
        quotient, remainder = divmod(total_rows, partitions)
        for batch_index in range(partitions):
            row_count = quotient + int(batch_index < remainder)
            files[f"datasets/episodes/part-{part_index:06d}.jsonl.gz"] = {
                "kind": "dataset_partition",
                "dataset": "episodes",
                "batch_id": f"{prefix}:{batch_index:06d}:episodes",
                "row_count": row_count,
            }
            part_index += 1
    evidence_manifest = {
        "files": files,
        "dataset_row_counts": {"episodes": EXPECTED_PERSISTED_EPISODE_ROWS},
    }
    receipt = {"dataset_row_counts": {"episodes": EXPECTED_PERSISTED_EPISODE_ROWS}}
    summary = {
        "production_counters": {
            "combine_episodes_completed": 35_328,
            "normal_episodes_completed": 17_664,
            "stressed_episodes_completed": 17_664,
        },
        "normal_combine_passes": 2_998,
        "stressed_combine_passes": 2_703,
        "horizon_frontier": {
            "90_TRADING_DAYS": {
                "normal": {"episode_count": 12_288, "pass_count": 2_998},
                "stressed": {"episode_count": 12_288, "pass_count": 2_703},
            }
        },
    }
    return evidence_manifest, receipt, summary


def test_partition_accounting_distinguishes_persisted_horizons_from_work() -> None:
    evidence_manifest, receipt, summary = _fixture()
    observed = validate_episode_partition_accounting(
        frozen_horizons=EXPECTED_HORIZONS,
        evidence_manifest=evidence_manifest,
        evidence_receipt=receipt,
        campaign_summary=summary,
    )
    assert observed["persisted_multi_horizon_episode_rows"] == 152_064
    assert observed["canonical_episode_computations"] == 35_328
    assert observed["outcome_rows_read"] == 0


def test_partition_accounting_fails_closed_on_unknown_batch() -> None:
    evidence_manifest, receipt, summary = _fixture()
    first = next(iter(evidence_manifest["files"].values()))
    first["batch_id"] = "active:neighboring-stage:000000:episodes"
    with pytest.raises(RecoveryError, match="unknown episode batch"):
        validate_episode_partition_accounting(
            frozen_horizons=EXPECTED_HORIZONS,
            evidence_manifest=evidence_manifest,
            evidence_receipt=receipt,
            campaign_summary=summary,
        )


def test_partition_accounting_fails_closed_on_horizon_or_receipt_drift() -> None:
    evidence_manifest, receipt, summary = _fixture()
    with pytest.raises(RecoveryError, match="frozen horizon policy"):
        validate_episode_partition_accounting(
            frozen_horizons=(20, 40, 60, 90),
            evidence_manifest=evidence_manifest,
            evidence_receipt=receipt,
            campaign_summary=summary,
        )
    receipt["dataset_row_counts"]["episodes"] -= 1
    with pytest.raises(RecoveryError, match="persisted episode rows"):
        validate_episode_partition_accounting(
            frozen_horizons=EXPECTED_HORIZONS,
            evidence_manifest=evidence_manifest,
            evidence_receipt=receipt,
            campaign_summary=summary,
        )


def test_terminal_kpis_replace_failed_state_with_sealed_terminal_metrics() -> None:
    prior = {
        "schema": "hydra_economic_production_kpis_v1",
        "campaign_id": "hydra_active_risk_pool_target_velocity_0026",
        "manifest_hash": "old",
        "source_commit": "old",
        "state": "FAILED_CLOSED",
        "rates_per_hour": {"combine_episodes": 1.0},
    }
    prior["kpi_hash"] = stable_hash(prior)
    summary = {
        "governor_proposals_generated": 20_000,
        "unique_policies_screened": 4_096,
        "exact_account_replays": 1_024,
        "policies_promoted_to_96": 32,
        "policies_surviving_96": 8,
        "confirmation_ready_candidate_ids": [f"p{value}" for value in range(8)],
        "positive_stressed_net_count": 256,
        "normal_pass_candidate_count": 256,
        "stressed_pass_candidate_count": 256,
        "best_normal_pass_rate": 0.34375,
        "best_stressed_pass_rate": 0.34375,
        "median_normal_pass_rate": 0.34375,
        "median_stressed_pass_rate": 0.33854,
        "matched_controls_status": "EXECUTED",
        "production_counters": {
            "combine_episodes_completed": 35_328,
            "normal_episodes_completed": 17_664,
            "stressed_episodes_completed": 17_664,
        },
        "production_kpis": {
            "rates_per_hour": {"combine_episodes": 12_340.0},
            "economic_research_wall_clock_fraction": 0.998,
            "cpu_utilization_fraction": 0.599,
            "workers": {"compute": 3, "evidence_writer": 1},
            "duplicate_rejection_rate": 0.2,
            "cache_hit_rate": 1.0,
        },
    }
    observed = normalized_terminal_kpis(
        copy.deepcopy(prior),
        state={"checkpoint_sequence": 217},
        campaign_summary=summary,
        finalized_at_utc="2026-07-15T12:00:00Z",
    )
    assert observed["state"] == "COMPLETE"
    assert observed["combine_episodes_completed"] == 35_328
    assert observed["rates_per_hour"]["combine_episodes"] == 12_340.0
    claimed = observed.pop("kpi_hash")
    assert stable_hash(observed) == claimed


def test_recover_once_reuses_two_guards_before_single_official_publication(
    tmp_path: Path,
) -> None:
    events: list[tuple[str, str]] = []

    class RecordingWriter:
        def __init__(self, root: Path) -> None:
            self.inner = AtomicResultWriter(root)

        def write_json(self, relative_path: str, value: Any) -> Any:
            events.append(("write", Path(relative_path).name))
            return self.inner.write_json(relative_path, value)

    loader_calls = 0

    def result_loader(path: Path, manifest: Any) -> dict[str, Any]:
        nonlocal loader_calls
        loader_calls += 1
        events.append(("load", path.name))
        assert manifest == {"manifest_hash": "frozen"}
        return json.loads(path.read_text(encoding="utf-8"))

    def revalidate() -> None:
        events.append(("guards", "exact-failed-state"))

    def consume(proof: Any) -> None:
        assert proof["preregistered_deep_guard_count"] == 2
        assert proof["additional_deep_guard_performed"] is False
        events.append(("attempt", "consumed"))

    reconcile_calls = 0

    def reconcile(checked_result: Any) -> None:
        nonlocal reconcile_calls
        reconcile_calls += 1
        assert checked_result["result_hash"] == "a" * 64
        assert result_path.is_file()
        events.append(("reconcile", "snapshots"))

    result_path = tmp_path / "economic_production_result.json"
    checked, published_sha256, proof = _preflight_validate_and_publish_result(
        output_dir=tmp_path,
        result_path=result_path,
        result={"schema": "synthetic", "result_hash": "a" * 64},
        manifest={"manifest_hash": "frozen"},
        writer=RecordingWriter(tmp_path),
        result_loader=result_loader,
        proof_payload={
            "sealed_bundle_content_sha256": "b" * 64,
            "sealed_bundle_manifest_sha256": "c" * 64,
            "original_failed_state_hash": "d" * 64,
            "original_failed_kpi_hash": "e" * 64,
        },
        revalidate_guards=revalidate,
        consume_attempt=consume,
        reconcile_snapshots=reconcile,
    )

    assert loader_calls == 1
    assert events == [
        ("write", ".sealed_result_recovery_preflight.json"),
        ("load", ".sealed_result_recovery_preflight.json"),
        ("write", ".sealed_result_recovery_preflight_proof.json"),
        ("guards", "exact-failed-state"),
        ("attempt", "consumed"),
        ("reconcile", "snapshots"),
    ]
    assert reconcile_calls == 1
    assert not (tmp_path / ".sealed_result_recovery_preflight.json").exists()
    assert proof["result_sha256"] == published_sha256
    assert json.loads(result_path.read_text(encoding="utf-8")) == checked
    assert len(published_sha256) == 64

    recover_source = inspect.getsource(recover_once)
    assert recover_source.count(
        "result_loader=_load_result_after_two_preregistered_guards"
    ) == 1
    assert "load_and_verify_production_result" not in recover_source
    assert "deep=True" not in recover_source
    loader_source = inspect.getsource(_load_result_after_two_preregistered_guards)
    assert "verify_evidence_bundle(final_bundle, deep=False)" in loader_source
    assert "deep=True" not in loader_source
    assert "guard_campaign_completion" not in loader_source
    assert recover_source.count("ActiveRiskPoolRun(") == 1
    assert recover_source.count("run._reconcile_completed_result_snapshots(") == 1
    assert "_poll_for_controller_completion(" in recover_source
    assert recover_source.index("controller_completion = _poll_for_controller_completion") < (
        recover_source.index("recovery_receipt =")
    )


def _write_hashed_json(path: Path, value: dict[str, Any], hash_field: str) -> str:
    payload = dict(value)
    payload[hash_field] = stable_hash(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(payload[hash_field])


def test_controller_poll_hands_off_hash_chained_terminal_without_writing(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    failed_state_hash = _write_hashed_json(
        output / "production_state.json",
        {"state": "FAILED_CLOSED"},
        "state_hash",
    )
    failed_kpi_hash = _write_hashed_json(
        output / "production_kpis.json",
        {"state": "FAILED_CLOSED"},
        "kpi_hash",
    )
    recommendation = {
        "action": "CONTINUE_FROZEN_ACTIVE_POOL_FINALISTS",
        "manifest_required": True,
        "q4_access_authorized": False,
        "new_data_purchase_authorized": False,
    }
    result = {
        "result_hash": "a" * 64,
        "autonomous_next_action": recommendation,
    }
    identity = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": MANIFEST_HASH,
        "result_hash": result["result_hash"],
        "recommendation": recommendation,
    }
    handoff = {
        "schema": "hydra_production_successor_handoff_v1",
        "sequence": 1,
        "handoff_id": stable_hash(identity),
        **identity,
        "previous_handoff_hash": None,
        "recorded_at_utc": "2026-07-15T13:00:00+00:00",
        "handoff_state": "WORM_MANIFEST_REQUIRED",
    }
    handoff["handoff_hash"] = stable_hash(handoff)
    controller_path = tmp_path / "economic_evolution_manifest_runtime.json"
    controller_path.write_text(
        json.dumps(
            {
                "schema": "hydra_manifest_campaign_runtime_v1",
                "state": "SUCCESSOR_HANDOFF_RECORDED",
                "production_successor_handoffs": [handoff],
            }
        ),
        encoding="utf-8",
    )
    state_before = (output / "production_state.json").read_bytes()
    kpis_before = (output / "production_kpis.json").read_bytes()

    with pytest.raises(ControllerHandoffRequired) as error:
        _poll_for_controller_completion(
            output_dir=output,
            controller_runtime_state_path=controller_path,
            result=result,
            original_failed_state_hash=failed_state_hash,
            original_failed_kpi_hash=failed_kpi_hash,
            timeout_seconds=0.0,
        )
    assert error.value.payload["state"] == (
        "CONTROLLER_ACCEPTED_RESULT_SNAPSHOTS_NOT_COMPLETE"
    )
    terminal = error.value.payload["controller_terminal_observation"]
    assert terminal["terminal_source"] == "PERSISTENT_CONTROLLER_SUCCESSOR_HANDOFF"
    assert terminal["handoff_hash"] == handoff["handoff_hash"]
    assert json.loads((output / "production_state.json").read_text())["state"] == (
        "FAILED_CLOSED"
    )
    assert (output / "production_state.json").read_bytes() == state_before
    assert (output / "production_kpis.json").read_bytes() == kpis_before


def test_controller_poll_accepts_only_complete_state_and_kpis(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    state_hash = _write_hashed_json(
        output / "production_state.json",
        {
            "state": "COMPLETE",
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": MANIFEST_HASH,
            "source_commit": SOURCE_COMMIT,
            "combine_episodes_completed": 35_328,
            "checkpoint_sequence": 218,
        },
        "state_hash",
    )
    kpi_hash = _write_hashed_json(
        output / "production_kpis.json",
        {
            "state": "COMPLETE",
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": MANIFEST_HASH,
            "source_commit": SOURCE_COMMIT,
            "combine_episodes_completed": 35_328,
            "checkpoint_sequence": 218,
        },
        "kpi_hash",
    )
    observed = _poll_for_controller_completion(
        output_dir=output,
        controller_runtime_state_path=tmp_path / "absent-controller-state.json",
        result={"result_hash": "c" * 64, "autonomous_next_action": {}},
        original_failed_state_hash="unused",
        original_failed_kpi_hash="unused",
        timeout_seconds=0.0,
    )
    assert observed == {
        "terminal_source": "PRODUCTION_SNAPSHOTS_COMPLETE",
        "source_truth_complete": True,
        "checkpoint_sequence": 218,
        "state_hash": state_hash,
        "kpi_hash": kpi_hash,
    }


def test_controller_poll_tolerates_transient_complete_failed_snapshot_pair(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    _write_hashed_json(
        output / "production_state.json",
        {
            "state": "COMPLETE",
            "campaign_id": CAMPAIGN_ID,
            "manifest_hash": MANIFEST_HASH,
            "source_commit": SOURCE_COMMIT,
            "combine_episodes_completed": 35_328,
            "checkpoint_sequence": 218,
        },
        "state_hash",
    )
    failed_kpi_hash = _write_hashed_json(
        output / "production_kpis.json",
        {"state": "FAILED_CLOSED"},
        "kpi_hash",
    )
    with pytest.raises(ControllerHandoffRequired) as error:
        _poll_for_controller_completion(
            output_dir=output,
            controller_runtime_state_path=tmp_path / "absent-controller-state.json",
            result={"result_hash": "d" * 64, "autonomous_next_action": {}},
            original_failed_state_hash="unused",
            original_failed_kpi_hash=failed_kpi_hash,
            timeout_seconds=0.0,
        )
    assert error.value.payload["state"] == (
        "RESULT_PUBLISHED_SNAPSHOTS_NOT_COMPLETE"
    )


def test_controller_poll_times_out_with_explicit_read_only_handoff(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    failed_state_hash = _write_hashed_json(
        output / "production_state.json",
        {"state": "FAILED_CLOSED"},
        "state_hash",
    )
    failed_kpi_hash = _write_hashed_json(
        output / "production_kpis.json",
        {"state": "FAILED_CLOSED"},
        "kpi_hash",
    )
    controller_path = tmp_path / "economic_evolution_manifest_runtime.json"
    controller_path.write_text(
        json.dumps(
            {
                "schema": "hydra_manifest_campaign_runtime_v1",
                "state": "RUNNING",
                "production_successor_handoffs": [],
            }
        ),
        encoding="utf-8",
    )
    result = {
        "result_hash": "b" * 64,
        "autonomous_next_action": {
            "action": "CONTINUE_FROZEN_ACTIVE_POOL_FINALISTS",
            "manifest_required": True,
        },
    }
    state_before = (output / "production_state.json").read_bytes()
    kpis_before = (output / "production_kpis.json").read_bytes()
    with pytest.raises(ControllerHandoffRequired) as error:
        _poll_for_controller_completion(
            output_dir=output,
            controller_runtime_state_path=controller_path,
            result=result,
            original_failed_state_hash=failed_state_hash,
            original_failed_kpi_hash=failed_kpi_hash,
            timeout_seconds=0.0,
        )
    assert error.value.payload["state"] == (
        "RESULT_PUBLISHED_SNAPSHOTS_NOT_COMPLETE"
    )
    assert error.value.payload["local_state_or_kpi_write_performed"] is False
    assert (output / "production_state.json").read_bytes() == state_before
    assert (output / "production_kpis.json").read_bytes() == kpis_before


def _failed_recovery_snapshots(output: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    state = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": MANIFEST_HASH,
        "source_commit": SOURCE_COMMIT,
        "state": "FAILED_CLOSED",
        "stage": "EVIDENCE_BUNDLE_ATOMIC_FINALIZE",
        "error_type": "ActiveRiskRuntimeError",
        "error": "active-risk counters diverge from persisted multi-horizon episodes",
        "next_action": "REQUIRE_SPECIFIC_ACTIVE_RISK_RUNTIME_REPAIR",
        "combine_episodes_completed": 35_328,
        "runner_pid": -1,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }
    kpis = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": MANIFEST_HASH,
        "source_commit": SOURCE_COMMIT,
        "state": "FAILED_CLOSED",
        "combine_episodes_completed": 35_328,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }
    _write_hashed_json(output / "production_state.json", state, "state_hash")
    _write_hashed_json(output / "production_kpis.json", kpis, "kpi_hash")
    return (
        json.loads((output / "production_state.json").read_text()),
        json.loads((output / "production_kpis.json").read_text()),
    )


@pytest.mark.parametrize(
    ("candidate_name", "attempt_exists", "partial_snapshot"),
    [
        (PREFLIGHT_RESULT_NAME, False, "NONE"),
        (PREFLIGHT_RESULT_NAME, True, "NONE"),
        ("economic_production_result.json", True, "NONE"),
        ("economic_production_result.json", True, "KPI_COMPLETE"),
        ("economic_production_result.json", True, "STATE_COMPLETE"),
        ("economic_production_result.json", True, "BOTH_COMPLETE"),
        ("economic_production_result.json", True, "FAILED_KPI_DRIFT"),
    ],
)
def test_terminalization_resume_converges_without_second_deep_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    candidate_name: str,
    attempt_exists: bool,
    partial_snapshot: str,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    failed_state, failed_kpis = _failed_recovery_snapshots(output)
    result = {
        "schema": "hydra_economic_production_result_v1",
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": MANIFEST_HASH,
        "source_commit": SOURCE_COMMIT,
        "status": "COMPLETE",
        "evidence_bundle": {
            "bundle_content_sha256": "e" * 64,
            "manifest_sha256": "f" * 64,
        },
        "autonomous_next_action": {
            "action": "CONTINUE_FROZEN_ACTIVE_POOL_FINALISTS",
            "manifest_required": True,
        },
        "sealed_result_recovery": {"episode_accounting": {"canonical": 35_328}},
    }
    result["result_hash"] = stable_hash(result)
    AtomicResultWriter(output).write_json(candidate_name, result)
    candidate_path = output / candidate_name
    proof = {
        "schema": "hydra_active_risk_0026_result_preflight_proof_v1",
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": MANIFEST_HASH,
        "source_commit": SOURCE_COMMIT,
        "recovery_version": RECOVERY_VERSION,
        "result_hash": result["result_hash"],
        "result_sha256": _sha256(candidate_path),
        "recovery_implementation_sha256": _sha256(
            Path(recovery_module.__file__).resolve()
        ),
        "validator": "load_result_after_two_preregistered_guards",
        "preregistered_deep_guard_count": 2,
        "additional_deep_guard_performed": False,
        "validated_at_utc": "2026-07-15T13:00:00Z",
        "sealed_bundle_content_sha256": "e" * 64,
        "sealed_bundle_manifest_sha256": "f" * 64,
        "original_failed_state_hash": failed_state["state_hash"],
        "original_failed_kpi_hash": failed_kpis["kpi_hash"],
    }
    proof["proof_hash"] = stable_hash(proof)
    AtomicResultWriter(output).write_json(PREFLIGHT_PROOF_NAME, proof)
    if attempt_exists:
        _write_attempt_marker(
            attempt_path=output / ATTEMPT_NAME,
            output_dir=output,
            proof=proof,
            original_state_hash=failed_state["state_hash"],
            original_kpi_hash=failed_kpis["kpi_hash"],
            sealed_bundle_content_sha256="e" * 64,
        )

    if partial_snapshot == "KPI_COMPLETE":
        _write_hashed_json(
            output / "production_kpis.json",
            {
                "state": "COMPLETE",
                "campaign_id": CAMPAIGN_ID,
                "manifest_hash": MANIFEST_HASH,
                "source_commit": SOURCE_COMMIT,
                "combine_episodes_completed": 35_328,
                "checkpoint_sequence": 218,
            },
            "kpi_hash",
        )
    elif partial_snapshot == "STATE_COMPLETE":
        _write_hashed_json(
            output / "production_state.json",
            {
                "state": "COMPLETE",
                "campaign_id": CAMPAIGN_ID,
                "manifest_hash": MANIFEST_HASH,
                "source_commit": SOURCE_COMMIT,
                "combine_episodes_completed": 35_328,
                "checkpoint_sequence": 218,
            },
            "state_hash",
        )
    elif partial_snapshot == "BOTH_COMPLETE":
        _write_hashed_json(
            output / "production_state.json",
            {
                "state": "COMPLETE",
                "campaign_id": CAMPAIGN_ID,
                "manifest_hash": MANIFEST_HASH,
                "source_commit": SOURCE_COMMIT,
                "combine_episodes_completed": 35_328,
                "checkpoint_sequence": 218,
            },
            "state_hash",
        )
        _write_hashed_json(
            output / "production_kpis.json",
            {
                "state": "COMPLETE",
                "campaign_id": CAMPAIGN_ID,
                "manifest_hash": MANIFEST_HASH,
                "source_commit": SOURCE_COMMIT,
                "combine_episodes_completed": 35_328,
                "checkpoint_sequence": 218,
            },
            "kpi_hash",
        )
    elif partial_snapshot == "FAILED_KPI_DRIFT":
        drifted_kpis = json.loads(
            (output / "production_kpis.json").read_text()
        )
        drifted_kpis.pop("kpi_hash")
        drifted_kpis["unexpected_mutation"] = True
        _write_hashed_json(
            output / "production_kpis.json",
            drifted_kpis,
            "kpi_hash",
        )

    reconcile_calls = 0

    class FakeRun:
        def __init__(self, **_: Any) -> None:
            pass

        def _reconcile_completed_result_snapshots(self, checked: Any) -> None:
            nonlocal reconcile_calls
            reconcile_calls += 1
            assert checked["result_hash"] == result["result_hash"]
            current_state = json.loads(
                (output / "production_state.json").read_text()
            )
            current_kpis = json.loads(
                (output / "production_kpis.json").read_text()
            )
            if (
                current_state.get("state") == "COMPLETE"
                and current_kpis.get("state") == "COMPLETE"
            ):
                return
            _write_hashed_json(
                output / "production_state.json",
                {
                    "state": "COMPLETE",
                    "campaign_id": CAMPAIGN_ID,
                    "manifest_hash": MANIFEST_HASH,
                    "source_commit": SOURCE_COMMIT,
                    "combine_episodes_completed": 35_328,
                    "checkpoint_sequence": 219,
                },
                "state_hash",
            )
            _write_hashed_json(
                output / "production_kpis.json",
                {
                    "state": "COMPLETE",
                    "campaign_id": CAMPAIGN_ID,
                    "manifest_hash": MANIFEST_HASH,
                    "source_commit": SOURCE_COMMIT,
                    "combine_episodes_completed": 35_328,
                    "checkpoint_sequence": 219,
                },
                "kpi_hash",
            )

    import hydra.production.active_risk_runtime as active_runtime

    monkeypatch.setattr(active_runtime, "ActiveRiskPoolRun", FakeRun)
    monkeypatch.setattr(
        recovery_module,
        "_verify_sealed_evidence_against_proof",
        lambda **_: None,
    )
    if partial_snapshot == "FAILED_KPI_DRIFT":
        with pytest.raises(RecoveryError, match="FAILED_CLOSED KPI changed"):
            _resume_terminalization_only(
                manifest_path=tmp_path / "config/v7/frozen.json",
                manifest={},
                root=tmp_path,
                output_dir=output,
                timeout_seconds=0.0,
            )
        assert reconcile_calls == 0
        return
    receipt = _resume_terminalization_only(
        manifest_path=tmp_path / "config/v7/frozen.json",
        manifest={},
        root=tmp_path,
        output_dir=output,
        timeout_seconds=0.0,
    )
    assert reconcile_calls == 1
    assert receipt["resume_without_deep_validation"] is True
    assert receipt["authoritative_snapshot_writer"] == (
        "BOUNDED_ONE_TIME_RECOVERY_RECONCILER"
    )
    assert (output / "economic_production_result.json").is_file()
    assert not (output / PREFLIGHT_RESULT_NAME).exists()
    assert (output / ATTEMPT_NAME).is_file()
    assert (output / RECOVERY_RECEIPT_NAME).is_file()
    assert "load_and_verify_production_result" not in inspect.getsource(
        _resume_terminalization_only
    )

    # A relaunch after the receipt is durable is observation-only and must not
    # invoke either deep validation or snapshot reconciliation again.
    repeated = _resume_terminalization_only(
        manifest_path=tmp_path / "config/v7/frozen.json",
        manifest={},
        root=tmp_path,
        output_dir=output,
        timeout_seconds=0.0,
    )
    assert repeated == receipt
    assert reconcile_calls == 1
    assert len(list(output.glob("economic_production_result.json"))) == 1
    assert len(list(output.glob(ATTEMPT_NAME))) == 1
    assert len(list(output.glob(RECOVERY_RECEIPT_NAME))) == 1
