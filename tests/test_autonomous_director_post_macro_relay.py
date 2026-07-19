from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_director_manifest as manifest_contract
from hydra.production import autonomous_director_runtime as runtime


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def _manifest() -> dict:
    return {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
        "compatible_artifact_manifest_hashes": [],
    }


def _source() -> dict:
    core = {
        "schema": "hydra_tier_q_2026_two_stage_economic_result_v2",
        "status": "FINAL_DEVELOPMENT_CONSUMED",
        "role": "FINAL_DEVELOPMENT",
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _card(
    root: Path,
    output: Path,
    index: int,
    *,
    source_mode: str = "GENERATE_READ_ONLY_ONCE",
) -> dict:
    input_path = root / f"input_{index}.json"
    _write(input_path, {"index": index})
    return {
        "lane_id": "EXPLOITATION" if index == 1 else "EXPLORATION",
        "branch_id": f"BRANCH_{index}",
        "relay_key": "POST_MACRO_TIER_Q_2026_FINAL_DEVELOPMENT",
        "worker_kind": "TIER_Q_2026_FINAL_DEVELOPMENT_READ_ONLY",
        "source_mode": source_mode,
        "preexisting_result_hash": (
            _source()["result_hash"]
            if source_mode == "PREEXISTING_HASH_BOUND"
            else None
        ),
        "worker_implementation_file": (
            "hydra/production/tier_q_2026_two_stage_runner.py"
        ),
        "worker_implementation_sha256": "c" * 64,
        "source_result_path": str(
            (output / f"branch_results/post_macro/source_{index}.json").relative_to(root)
        ),
        "relay_result_path": str(
            (output / f"branch_results/post_macro/relay_{index}.json").relative_to(root)
        ),
        "launch_receipt_path": str(
            (output / f"branch_results/post_macro/launch_{index}.json").relative_to(root)
        ),
        "resume_receipt_path": str(
            (output / f"branch_results/post_macro/resume_{index}.json").relative_to(root)
        ),
        "worker_inputs": {
            "decision_card": {
                "path": str(input_path.relative_to(root)),
                "sha256": runtime._file_sha256(input_path),
            }
        },
        "expected_schema": "hydra_tier_q_2026_two_stage_economic_result_v2",
        "allowed_statuses": ["FINAL_DEVELOPMENT_CONSUMED"],
        "expected_fields": {
            "role": "FINAL_DEVELOPMENT",
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "relay_evidence_tier": "E",
        "next_action": "NEXT_DISTINCT_BRANCH",
    }


def test_empty_inventory_creates_no_job_or_shard(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    inventory = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), {"cards": []}
    )
    assert inventory == []
    assert list(output.rglob("*.json")) == []


def test_existing_source_is_relayed_once_and_resume_is_idempotent(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    branch_root = output / "branch_results"
    branch_root.mkdir(parents=True)
    card = _card(
        tmp_path, output, 1, source_mode="PREEXISTING_HASH_BOUND"
    )
    source_path = tmp_path / card["source_result_path"]
    _write(source_path, _source())
    section = {"cards": [card]}

    first = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    assert [row["action"] for row in first] == ["RELAY_SOURCE"]
    runtime._write_post_macro_relay(
        tmp_path,
        _manifest(),
        AtomicResultWriter(branch_root),
        first[0],
        runtime._verify_post_macro_source(_source(), card),
    )

    resumed = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    assert [row["action"] for row in resumed] == ["RELAYED"]
    assert (
        runtime._read_post_macro_relay(
            resumed[0], _manifest(), root=tmp_path
        )
        == _source()
    )
    assert not (tmp_path / card["launch_receipt_path"]).exists()


def test_two_cards_resume_with_one_valid_result_and_one_active_lease(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    cards = [
        _card(
            tmp_path,
            output,
            1,
            source_mode="PREEXISTING_HASH_BOUND",
        ),
        _card(tmp_path, output, 2),
    ]
    _write(tmp_path / cards[0]["source_result_path"], _source())
    section = {"cards": cards}
    first = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    assert [row["action"] for row in first] == ["RELAY_SOURCE", "LAUNCH_ONCE"]
    receipt = runtime._post_macro_launch_receipt(
        _manifest(), first[1], lease_seconds=300
    )
    _write(tmp_path / cards[1]["launch_receipt_path"], receipt)

    resumed = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    assert [row["action"] for row in resumed] == [
        "RELAY_SOURCE",
        "WAITING_FOR_ACTIVE_LEASE",
    ]
    assert resumed[1]["worker_inputs"]


def test_crash_resumes_same_deterministic_attempt_after_lease(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    card = _card(tmp_path, output, 1)
    section = {"cards": [card]}
    first = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    receipt = runtime._post_macro_launch_receipt(
        _manifest(), first[0], lease_seconds=300
    )
    core = dict(receipt)
    core.pop("receipt_hash")
    core["lease_expires_at_utc"] = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    expired = {**core, "receipt_hash": stable_hash(core)}
    _write(tmp_path / card["launch_receipt_path"], expired)

    resumed = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    assert resumed[0]["action"] == "RESUME_EXPIRED_LEASE"
    assert (
        expired["deterministic_attempt_id"]
        == receipt["deterministic_attempt_id"]
    )


def test_second_expired_lease_is_terminal_and_cannot_launch_a_third_time(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    card = _card(tmp_path, output, 1)
    section = {"cards": [card]}
    first = runtime._post_macro_inventory(tmp_path, output, _manifest(), section)[0]
    initial = runtime._post_macro_launch_receipt(
        _manifest(), first, lease_seconds=300
    )
    initial_core = dict(initial)
    initial_core.pop("receipt_hash")
    initial_core["lease_expires_at_utc"] = (
        datetime.now(UTC) - timedelta(seconds=2)
    ).isoformat()
    initial = {**initial_core, "receipt_hash": stable_hash(initial_core)}
    _write(tmp_path / card["launch_receipt_path"], initial)

    resume_item = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )[0]
    assert resume_item["action"] == "RESUME_EXPIRED_LEASE"
    resumed = runtime._post_macro_launch_receipt(
        _manifest(),
        resume_item,
        lease_seconds=300,
        lease_generation=1,
        prior_receipt=initial,
    )
    resumed_core = dict(resumed)
    resumed_core.pop("receipt_hash")
    resumed_core["lease_expires_at_utc"] = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    resumed = {**resumed_core, "receipt_hash": stable_hash(resumed_core)}
    _write(tmp_path / card["resume_receipt_path"], resumed)

    exhausted = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    assert exhausted[0]["action"] == "WAITING_INPUT_LEASE_EXHAUSTED"
    assert runtime._post_macro_launch_batch(exhausted, 2) == []


def test_missing_input_precedes_even_an_active_lease(tmp_path: Path) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    card = _card(tmp_path, output, 1)
    section = {"cards": [card]}
    first = runtime._post_macro_inventory(tmp_path, output, _manifest(), section)[0]
    receipt = runtime._post_macro_launch_receipt(
        _manifest(), first, lease_seconds=300
    )
    _write(tmp_path / card["launch_receipt_path"], receipt)
    (tmp_path / card["worker_inputs"]["decision_card"]["path"]).unlink()

    inventory = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), section
    )
    assert inventory[0]["action"] == "WAITING_INPUT"
    assert inventory[0]["missing_inputs"] == ("decision_card",)


def test_missing_input_waits_without_launch_or_empty_shard(tmp_path: Path) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    card = _card(tmp_path, output, 1)
    (tmp_path / card["worker_inputs"]["decision_card"]["path"]).unlink()
    inventory = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), {"cards": [card]}
    )
    assert inventory[0]["action"] == "WAITING_INPUT"
    assert inventory[0]["missing_inputs"] == ("decision_card",)
    assert not (tmp_path / card["launch_receipt_path"]).exists()


def test_missing_hash_bound_source_never_relaunches_economic_worker(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    card = _card(
        tmp_path, output, 1, source_mode="PREEXISTING_HASH_BOUND"
    )
    inventory = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), {"cards": [card]}
    )

    assert inventory[0]["action"] == "WAITING_INPUT_PREEXISTING_RESULT"
    assert inventory[0]["missing_inputs"] == ("preexisting_source_result",)
    assert runtime._post_macro_launch_batch(inventory, 2) == []


def test_three_card_queue_never_exceeds_two_concurrent_workers(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    cards = [_card(tmp_path, output, index) for index in (1, 2, 3)]
    inventory = runtime._post_macro_inventory(
        tmp_path,
        output,
        _manifest(),
        {"card_capacity": 3, "cards": cards},
    )
    selected = runtime._post_macro_launch_batch(inventory, 2)
    assert len(selected) == 2
    assert [row["action"] for row in inventory].count("QUEUED_FOR_WORKER_SLOT") == 1


def test_worker_contract_exposes_no_writer_or_output_path(tmp_path: Path) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    card = _card(tmp_path, output, 1)
    inventory = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), {"cards": [card]}
    )
    receipt = runtime._post_macro_launch_receipt(
        _manifest(), inventory[0], lease_seconds=300
    )

    assert set(inspect.signature(runtime._post_macro_read_only_worker).parameters) == {
        "root_path",
        "worker_kind",
        "inputs",
    }
    assert "source_result_path" not in inventory[0]["worker_inputs"]
    assert "relay_result_path" not in inventory[0]["worker_inputs"]
    assert "launch_receipt_path" not in inventory[0]["worker_inputs"]
    assert receipt["worker_received_output_or_writer_path"] is False
    assert receipt["parent_only_authoritative_writer"] is True


def test_source_safety_field_drift_fails_closed(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    card = _card(tmp_path, output, 1)
    unsafe = _source()
    core = dict(unsafe)
    core.pop("result_hash")
    core["orders"] = 1
    unsafe = {**core, "result_hash": stable_hash(core)}
    with pytest.raises(runtime.AutonomousDirectorRuntimeError):
        runtime._verify_post_macro_source(unsafe, card)


def test_treasury_unevaluable_status_relays_without_false_alpha_verdict() -> None:
    status = (
        "TREASURY_CURVATURE_RISK_GRANULARITY_BLOCKED_AND_"
        "COVERAGE_UNDERPOWERED"
    )
    worker_kind = "TREASURY_THREE_TENOR_CURVATURE_READ_ONLY"
    manifest_statuses = manifest_contract._POST_MACRO_WORKER_CONTRACTS[
        worker_kind
    ]["statuses"]
    runtime_statuses = runtime._POST_MACRO_WORKER_CONTRACTS[worker_kind][
        "statuses"
    ]
    assert manifest_statuses == runtime_statuses
    assert status in manifest_statuses

    core = {
        "schema": "hydra_treasury_three_tenor_curvature_tripwire_v1",
        "status": status,
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "branch_gate": {
            "economically_evaluable_account_point_count": 0,
            "full_coverage_headline_account_point_count": 0,
            "legal_quantity_account_point_count": 0,
            "promotion_allowed": False,
            "zero_serialized_economic_metrics_are_observed_rates": False,
        },
        "governance": {
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "promotion_allowed": False,
            "tier_q_allowed": False,
        },
    }
    source = {**core, "result_hash": stable_hash(core)}
    card = {
        "worker_kind": worker_kind,
        "expected_schema": core["schema"],
        "allowed_statuses": list(runtime_statuses),
        "expected_fields": {
            "evidence_role": core["evidence_role"],
            "governance.q4_access_count_delta": 0,
            "governance.broker_connections": 0,
            "governance.orders": 0,
            "branch_gate.economically_evaluable_account_point_count": 0,
            "branch_gate.full_coverage_headline_account_point_count": 0,
            "branch_gate.legal_quantity_account_point_count": 0,
            "branch_gate.promotion_allowed": False,
            "branch_gate.zero_serialized_economic_metrics_are_observed_rates": False,
        },
        "source_mode": "PREEXISTING_HASH_BOUND",
        "preexisting_result_hash": source["result_hash"],
    }

    verified = runtime._verify_post_macro_source(source, card)
    assert verified["status"] == status
    assert verified["branch_gate"]["promotion_allowed"] is False
    assert (
        verified["branch_gate"][
            "zero_serialized_economic_metrics_are_observed_rates"
        ]
        is False
    )


def test_pre_spawn_requires_the_frozen_kind_to_implementation_mapping(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    (output / "branch_results").mkdir(parents=True)
    card = _card(tmp_path, output, 1)
    card["worker_implementation_file"] = (
        "hydra/research/cl_front_second_term_structure_economic_runner.py"
    )
    item = runtime._post_macro_inventory(
        tmp_path, output, _manifest(), {"cards": [card]}
    )[0]
    manifest = {
        **_manifest(),
        "implementation_files": {
            card["worker_implementation_file"]: card[
                "worker_implementation_sha256"
            ]
        },
    }
    with pytest.raises(
        runtime.AutonomousDirectorRuntimeError,
        match="implementation mapping drift",
    ):
        runtime._verify_post_macro_pre_spawn(tmp_path, manifest, item)


def test_terminal_economic_projection_preserves_real_denominators() -> None:
    source = json.loads(
        Path(
            "reports/economic_evolution/"
            "autonomous_economic_discovery_director_0035_revision_02/"
            "branch_results/tier_q_2026_two_stage/"
            "final_development_result.json"
        ).read_text(encoding="utf-8")
    )
    summary, halving, failures = runtime._post_macro_terminal_economics(
        source["candidate_results"]
    )
    assert summary["production_counters"] == {
        "serious_exact_account_replays": 5,
        "predeclared_control_policy_replays": 0,
        "combine_episodes_completed": 54,
        "normal_episodes_completed": 27,
        "stressed_episodes_completed": 27,
    }
    assert halving["stage_decisions"][0]["output_count"] == 0
    assert len(failures["by_candidate"]) == 5


def test_crash_after_terminal_result_resumes_by_deep_verification_without_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = {
        **_manifest(),
        "evidence_bundle": {
            "evidence_status": "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION",
            "reconstruction_flag": True,
        },
    }
    action = "ADVANCE_TO_PREAPPENDED_RESEARCH_BOARD_SUCCESSOR"
    state_core = {
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "state": "COMPLETE",
        "stage": "POST_MACRO_BRANCH_PORTFOLIO_TERMINAL_EVIDENCE_SEALED",
        "next_action": action,
    }
    state = {**state_core, "state_hash": stable_hash(state_core)}
    state_path = output / "production_state.json"
    _write(state_path, state)
    result_path = output / "economic_production_result.json"
    _write(result_path, {"terminal_commit_marker": True})
    original_state = state_path.read_bytes()
    original_result = result_path.read_bytes()
    calls: list[tuple[Path, bool]] = []

    def verified(path, frozen_manifest, *, deep_evidence=True):
        assert frozen_manifest is manifest
        calls.append((Path(path), deep_evidence))
        return {
            "autonomous_next_action": {"action": action},
            "evidence_bundle": {
                "evidence_status": (
                    "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
                ),
                "reconstruction_flag": True,
            },
        }

    monkeypatch.setattr(runtime, "load_and_verify_production_result", verified)
    resumed = runtime._load_existing_post_macro_terminal_state(output, manifest)

    assert resumed == state
    assert calls == [(result_path, True)]
    assert state_path.read_bytes() == original_state
    assert result_path.read_bytes() == original_result
