from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_director_runtime as runtime


def _manifest() -> dict[str, Any]:
    return {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
    }


def _legal_result(*, admissible_passes: int = 2) -> dict[str, Any]:
    diagnostic = max(admissible_passes, 3)
    core = {
        "schema": runtime.FROZEN_LEGAL_FRONTIER_SCHEMA,
        "status": "COMPLETE_EXACT_FROZEN_LEGAL_FRONTIER_REPLAY",
        "results": [
            {"exact_policy_id": f"exact_legal_{index}"} for index in range(3)
        ],
        "evidence_bundle_adapter": {
            "sealing_performed": False,
            "authoritative_writer_required_for_sealing": True,
        },
        "counters": {
            "frozen_cell_count": 3,
            "unique_component_count": 4,
            "source_event_rows_reconstructed": 90,
            "exact_account_replays": 72,
            "diagnostic_exact_passes_all_horizons_and_scenarios": diagnostic,
            "admissible_exact_passes_all_horizons_and_scenarios": (
                admissible_passes
            ),
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "authoritative_writes": 0,
        },
        "decision": (
            "EXACT_PASS_OBSERVED_REQUIRES_FROZEN_CHRONOLOGICAL_VALIDATION"
            if admissible_passes
            else "SUMMARY_LEGAL_FRONTIER_PASS_SIGNAL_NOT_CONFIRMED_EXACTLY"
        ),
        "evidence_tier": "E",
        "promotion_status": None,
        "read_only_worker": True,
    }
    return {**core, "result_hash": stable_hash(core)}


def _treasury_waiting() -> dict[str, Any]:
    core = {
        "schema": runtime.TREASURY_CURVE_SCHEMA,
        "status": runtime.TREASURY_CURVE_WAITING_STATUS,
        "decision": runtime.TREASURY_CURVE_WAITING_STATUS,
        "economic_result_created": False,
        "evidence_role": None,
        "rule_specs": [
            {"rule_id": f"curve_rule_{index}"} for index in range(16)
        ],
        "authoritative_writes": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


def _treasury_complete() -> dict[str, Any]:
    candidates = [
        {
            "rule_id": f"curve_rule_{index}",
            "account_matrix": [
                {"account_label": label} for label in ("50K", "100K", "150K")
            ],
        }
        for index in range(16)
    ]
    core = {
        "schema": runtime.TREASURY_CURVE_SCHEMA,
        "status": "COMPLETE_DEVELOPMENT_TRIPWIRE",
        "decision": "CURVE_RELATIVE_VALUE_TRIPWIRE_WEAK",
        "evidence_role": runtime.TREASURY_CURVE_EVIDENCE_ROLE,
        "rule_specs": [
            {"rule_id": f"curve_rule_{index}"} for index in range(16)
        ],
        "candidate_results": candidates,
        "economic_summary": {
            "rule_count": 16,
            "final_development_stressed_episode_count": 48,
            "final_development_stressed_pass_count": 1,
        },
        "authoritative_writes": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


class _ThreadExecutor:
    def __init__(self, *, max_workers: int, mp_context: object) -> None:
        del mp_context
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

    def __enter__(self) -> "_ThreadExecutor":
        return self

    def __exit__(self, *args: object) -> None:
        self._pool.shutdown(wait=True)

    def submit(self, function: Any, payload: dict[str, Any]) -> Any:
        return self._pool.submit(function, payload)


def _patch_relay_io(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "ProcessPoolExecutor", _ThreadExecutor)
    monkeypatch.setattr(runtime, "_publish", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime, "_write_mission_views", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(runtime, "_append_decision_once", lambda *_args: None)
    monkeypatch.setattr(runtime, "_kpis", lambda *_args, **_kwargs: {})


def _run_relay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    prior_state: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    output = tmp_path / "output"
    output.mkdir(exist_ok=True)
    return runtime._run_post_breadth_portfolio_relay(
        root=tmp_path,
        manifest=_manifest(),
        output=output,
        live_writer=object(),
        branch_writer=AtomicResultWriter(output / "branch_results"),
        prior_state=prior_state or {"checkpoint_sequence": 0},
        started=time.monotonic(),
        heartbeat_seconds=0.01,
        runtime_results={},
    )


def test_dispatches_two_read_only_workers_and_persists_only_parent_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_relay_io(monkeypatch)
    calls: list[tuple[str, dict[str, Any]]] = []

    def legal(payload: dict[str, Any]) -> dict[str, Any]:
        calls.append(("legal", payload))
        return _legal_result()

    def treasury(payload: dict[str, Any]) -> dict[str, Any]:
        calls.append(("treasury", payload))
        assert payload["input_contract"] is None
        assert payload["missing_inputs"] == [
            "treasury_acquisition_receipt",
            "treasury_input_contract",
        ]
        return _treasury_waiting()

    monkeypatch.setattr(runtime, "frozen_legal_frontier_worker", legal)
    monkeypatch.setattr(runtime, "_treasury_curve_tripwire_worker", treasury)
    state, results, portfolio = _run_relay(tmp_path, monkeypatch)

    assert {name for name, _payload in calls} == {"legal", "treasury"}
    assert state["stage"] == "POST_BREADTH_PORTFOLIO_DECIDED"
    assert state["active_economic_worker_processes"] == 0
    assert state["post_breadth_worker_limit"] == 2
    assert state["post_breadth_parent_writer_only"] is True
    assert state["post_breadth_legal_exact_account_replay_count"] == 72
    assert state["post_breadth_legal_admissible_pass_count"] == 2
    assert state["post_breadth_treasury_status"] == (
        runtime.TREASURY_CURVE_WAITING_STATUS
    )
    assert portfolio["treasury_curve"]["economic_result_created"] is False
    assert results["POST_BREADTH_LEGAL_EXACT"]["read_only_worker"] is True

    legal_path = (
        tmp_path
        / "output/branch_results/post_breadth_portfolio"
        / "frozen_legal_frontier_exact.json"
    )
    treasury_path = legal_path.with_name("treasury_curve_tripwire.json")
    assert legal_path.is_file()
    assert not treasury_path.exists()
    envelope = json.loads(legal_path.read_text())
    assert envelope["read_only_worker"] is True
    assert envelope["q4_access_count_delta"] == 0
    assert envelope["broker_connections"] == 0
    assert envelope["orders"] == 0


def test_partial_resume_loads_legal_without_dispatch_or_double_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_relay_io(monkeypatch)
    calls = {"legal": 0, "treasury": 0}

    def legal(_payload: dict[str, Any]) -> dict[str, Any]:
        calls["legal"] += 1
        return _legal_result()

    def treasury(_payload: dict[str, Any]) -> dict[str, Any]:
        calls["treasury"] += 1
        return _treasury_waiting()

    monkeypatch.setattr(runtime, "frozen_legal_frontier_worker", legal)
    monkeypatch.setattr(runtime, "_treasury_curve_tripwire_worker", treasury)
    first_state, _results, _portfolio = _run_relay(tmp_path, monkeypatch)
    second_state, _results, _portfolio = _run_relay(
        tmp_path, monkeypatch, prior_state=first_state
    )

    assert calls == {"legal": 1, "treasury": 2}
    assert second_state["post_breadth_legal_exact_account_replay_count"] == 72
    assert second_state["post_breadth_legal_admissible_pass_count"] == 2
    assert second_state["exact_account_replays"] == first_state[
        "exact_account_replays"
    ]
    assert second_state["combine_episodes_completed"] == first_state[
        "combine_episodes_completed"
    ]


def test_resume_rejects_persisted_envelope_hash_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_relay_io(monkeypatch)
    monkeypatch.setattr(
        runtime, "frozen_legal_frontier_worker", lambda _payload: _legal_result()
    )
    monkeypatch.setattr(
        runtime,
        "_treasury_curve_tripwire_worker",
        lambda _payload: _treasury_waiting(),
    )
    state, _results, _portfolio = _run_relay(tmp_path, monkeypatch)
    path = (
        tmp_path
        / "output/branch_results/post_breadth_portfolio"
        / "frozen_legal_frontier_exact.json"
    )
    envelope = json.loads(path.read_text())
    envelope["frozen_legal_frontier_exact"]["counters"][
        "exact_account_replays"
    ] += 1
    path.write_text(json.dumps(envelope))

    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="hash drift"):
        _run_relay(tmp_path, monkeypatch, prior_state=state)


def test_post_breadth_counts_do_not_inflate_historical_runtime_totals() -> None:
    manifest = _manifest()
    base = runtime._state_payload(
        manifest,
        sequence=1,
        state="ROBUSTNESS_ACTIVE",
        stage="BASE",
        branch_results={},
        next_action="BASE",
    )
    augmented = runtime._state_payload(
        manifest,
        sequence=2,
        state="ROBUSTNESS_ACTIVE",
        stage="POST_BREADTH_PORTFOLIO_DECIDED",
        branch_results={
            "POST_BREADTH_LEGAL_EXACT": _legal_result(),
            "POST_BREADTH_TREASURY_CURVE": _treasury_complete(),
        },
        next_action="ADVANCE",
    )

    for field in (
        "policies_proposed",
        "unique_policies_screened",
        "exact_account_replays",
        "combine_episodes_completed",
        "normal_episodes_completed",
        "stressed_episodes_completed",
    ):
        assert augmented[field] == base[field]
    assert augmented["post_breadth_legal_exact_account_replay_count"] == 72
    assert augmented["post_breadth_treasury_rule_count"] == 16
    assert augmented["post_breadth_treasury_final_stressed_episode_count"] == 48


def test_strict_verifiers_reject_worker_side_effects() -> None:
    legal = _legal_result()
    legal["counters"]["q4_access_count_delta"] = 1
    legal["result_hash"] = stable_hash(
        {key: value for key, value in legal.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="safety drift"):
        runtime._verify_frozen_legal_frontier_result(legal)

    treasury = _treasury_complete()
    treasury["orders"] = 1
    treasury["result_hash"] = stable_hash(
        {key: value for key, value in treasury.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="safety counter"):
        runtime._verify_treasury_curve_result(treasury, allow_waiting=False)
