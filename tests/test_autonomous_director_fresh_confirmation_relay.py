from __future__ import annotations

from pathlib import Path

import pytest

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_director_runtime as runtime


def _manifest(tmp_path: Path) -> dict:
    output = tmp_path / "reports/economic_evolution/director"
    return {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
        "runtime": {
            "output_dir": str(output.relative_to(tmp_path)),
        },
        "fresh_confirmation": {
            "contract_path": "confirmation/contract.json",
            "acquisition_receipt_path": "confirmation/acquisition.json",
            "feature_receipt_path": "confirmation/features.json",
            "result_path": str(
                (
                    output
                    / "branch_results/post_source_exhaustion/post_composite/"
                    "fresh_confirmation_result.json"
                ).relative_to(tmp_path)
            ),
            "contract_hash": "c" * 64,
        },
    }


def _confirmation_result(*, passed: bool) -> dict:
    candidate = {
        "candidate_id": "hazard_confirmed" if passed else "hazard_failed",
        "tier_c_promoted": passed,
        "tier_c_gate": {"passed": passed},
        "evidence_tier": "C" if passed else "G_CONFIRMATION_FAILED",
    }
    core = {
        "schema": runtime.FRESH_CONFIRMATION_RESULT_SCHEMA,
        "status": "CONFIRMATION_CONSUMED_ONCE",
        "retuning_performed": False,
        "recalibration_performed": False,
        "independent_confirmation_claimed_only_for_gate_passers": True,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "candidate_results": [candidate],
        "tier_c_candidate_ids": [candidate["candidate_id"]] if passed else [],
    }
    return {**core, "result_hash": stable_hash(core)}


def _prior_state(manifest: dict) -> dict:
    return runtime._state_payload(
        manifest,
        sequence=1,
        state="ROBUSTNESS_ACTIVE",
        stage="PRIOR_RELAY_COMPLETE",
        branch_results={},
        next_action="RUN_FRESH_CONFIRMATION_RELAY",
    )


def test_confirmation_verifier_counts_only_exact_gate_passers() -> None:
    result = _confirmation_result(passed=True)

    assert runtime._verify_fresh_confirmation_result(result) == result
    counts = runtime._relay_evidence_counts({"FRESH_CONFIRMATION": result})
    assert counts["tier_c_count"] == 1
    assert counts["tier_c_candidate_ids"] == ["hazard_confirmed"]
    assert counts["tier_f_count"] == 0


def test_confirmation_verifier_rejects_declared_count_or_f_tier_inflation() -> None:
    result = _confirmation_result(passed=True)
    core = dict(result)
    core.pop("result_hash")
    core["tier_c_candidate_ids"] = []
    drifted = {**core, "result_hash": stable_hash(core)}
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="Tier-C count"):
        runtime._verify_fresh_confirmation_result(drifted)

    core = dict(result)
    core.pop("result_hash")
    core["candidate_results"] = [
        {
            **dict(core["candidate_results"][0]),
            "evidence_tier": "F",
        }
    ]
    inflated = {**core, "result_hash": stable_hash(core)}
    with pytest.raises(
        runtime.AutonomousDirectorRuntimeError,
        match="candidate evidence tier drift|Tier-F inflation",
    ):
        runtime._verify_fresh_confirmation_result(inflated)


def test_absent_prewritten_inputs_fail_closed_without_worker_or_idle_loop(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    output = tmp_path / manifest["runtime"]["output_dir"]
    output.mkdir(parents=True)
    live_writer = AtomicResultWriter(output, immutable=False)
    branch_writer = AtomicResultWriter(output / "branch_results")

    state, results, confirmation = runtime._run_fresh_confirmation_relay(
        root=tmp_path,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        prior_state=_prior_state(manifest),
        started=runtime.time.monotonic(),
        heartbeat_seconds=0.01,
        runtime_results={},
    )

    assert confirmation == {}
    assert results == {}
    assert state["stage"] == "FRESH_CONFIRMATION_ACQUISITION_REQUIRED"
    assert state["active_economic_worker_processes"] == 0
    assert state["fresh_confirmation_fail_closed"] is True
    assert state["fresh_confirmation_missing_inputs"] == [
        "acquisition_receipt_path",
        "contract_path",
        "feature_receipt_path",
    ]
    assert state["fresh_confirmation_runtime_network_access"] is False
    assert state["fresh_confirmation_runtime_feature_cache_writes"] == 0


def test_persisted_confirmation_resumes_to_f0_only_for_actual_c_passer(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    output = tmp_path / manifest["runtime"]["output_dir"]
    output.mkdir(parents=True)
    live_writer = AtomicResultWriter(output, immutable=False)
    branch_writer = AtomicResultWriter(output / "branch_results")
    result = _confirmation_result(passed=True)
    envelope = runtime._post_source_envelope(
        manifest,
        lane_id="EXPLOITATION",
        branch_id="FROZEN_TIER_G_FRESH_CONFIRMATION",
        decision=result["status"],
        payload_key="fresh_confirmation_result",
        payload=result,
        next_action="PROVE_F0_FOR_CONFIRMED_TIER_C_BOOKS",
    )
    result_path = Path(manifest["fresh_confirmation"]["result_path"])
    branch_writer.write_json(
        result_path.relative_to(
            Path(manifest["runtime"]["output_dir"]) / "branch_results"
        ),
        envelope,
    )

    state, results, confirmation = runtime._run_fresh_confirmation_relay(
        root=tmp_path,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        prior_state=_prior_state(manifest),
        started=runtime.time.monotonic(),
        heartbeat_seconds=0.01,
        runtime_results={},
    )

    assert confirmation["tier_c_candidate_ids"] == ["hazard_confirmed"]
    assert results["FRESH_CONFIRMATION"] == confirmation
    assert state["stage"] == "FRESH_CONFIRMATION_COMPLETE_TIER_C_GATE_PASSED"
    assert state["independently_confirmed_tier_c_count"] == 1
    assert state["forward_tier_f_count"] == 0
    assert state["next_action"] == "PROVE_F0_FOR_CONFIRMED_TIER_C_BOOKS"


def test_confirmation_paths_cannot_escape_repository(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    manifest["fresh_confirmation"]["contract_path"] = "../escape.json"

    with pytest.raises(
        runtime.AutonomousDirectorRuntimeError,
        match="escapes repository",
    ):
        runtime._fresh_confirmation_manifest_paths(tmp_path, manifest)
