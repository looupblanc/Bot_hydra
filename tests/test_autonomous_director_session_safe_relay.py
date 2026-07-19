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


def _summary(episode_count: int, pass_count: int) -> dict[str, Any]:
    return {
        "episode_count": episode_count,
        "pass_count": pass_count,
        "mll_breach_count": 0,
        "target_progress_median": 0.94,
    }


def _result(
    variant: str, *, signal_gate_passed: bool = True
) -> dict[str, Any]:
    coverage = {"5": 36, "10": 13, "20": 4}
    accounts: dict[str, Any] = {}
    for account_label, account_size in (
        ("50K", 50_000),
        ("100K", 100_000),
        ("150K", 150_000),
    ):
        accounts[account_label] = {
            "policy_id": f"session_safe:{variant}:{account_label}",
            "account_size_usd": account_size,
            "official_market_contract_cap_breach_count_by_component": {
                "component_a": 0,
                "component_b": 0,
            },
            "hard_execution_contract_clean": True,
            "horizon_results": {
                horizon: {
                    "horizon_trading_days": int(horizon),
                    "requested_start_count": count,
                    "full_coverage_start_count": count,
                    "data_censored_start_count": 0,
                    "normal": _summary(
                        count,
                        2 if account_label == "50K" and horizon == "20" else 0,
                    ),
                    "stressed": _summary(
                        count,
                        2 if account_label == "50K" and horizon == "20" else 0,
                    ),
                }
                for horizon, count in coverage.items()
            },
            "promotion_status": None,
            "evidence_tier": "E_EXACT_DEVELOPMENT_TRIPWIRE",
        }
    records = [{"episode_index": index} for index in range(318)]
    checks = {
        "frozen_full_coverage": signal_gate_passed,
        "normal_and_stressed_signal": signal_gate_passed,
    }
    core = {
        "schema": runtime.SESSION_SAFE_FAST_BOOK_SCHEMA,
        "branch_id": runtime.SESSION_SAFE_FAST_BOOK_BRANCH_ID,
        "status": "COMPLETE_SESSION_SAFE_FAST_BOOK_TRIPWIRE",
        "repair_variant": variant,
        "evidence_role": runtime.SESSION_SAFE_FAST_BOOK_EVIDENCE_ROLE,
        "source_policy_id": runtime.SESSION_SAFE_FAST_BOOK_SOURCE_POLICY_ID,
        "status_inherited": False,
        "promotion_status": None,
        "evidence_tier": "E",
        "repair_contract": {
            "variant": variant,
            "scale_factor": 3,
            "outcome_fields_used": False,
            "future_label_eligibility_used": False,
            "mandatory_flatten_local": "15:10",
        },
        "original_session_violation_count_by_component": {
            "component_a": 8,
            "component_b": 0,
        },
        "repaired_session_violation_count_by_component": {
            "component_a": 0,
            "component_b": 0,
        },
        "account_results": accounts,
        "repair_signal_gate": {
            "gate_role": "DEVELOPMENT_TRIPWIRE_ONLY_NO_PROMOTION",
            "checks": checks,
            "passed": signal_gate_passed,
            "gate_hash": stable_hash(checks),
        },
        "evidence_bundle_adapter": {
            "evaluated_policy_records": records,
            "records_hash": stable_hash(records),
            "sealing_performed": False,
            "authoritative_writer_required_for_sealing": True,
        },
        "counters": {
            "cpu_shards_for_this_result": 1,
            "maximum_parallel_cpu_shards": 2,
            "source_event_rows_reconstructed": 666,
            "source_component_count": 3,
            "repaired_session_violation_count": 0,
            "exact_account_replays": 318,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "authoritative_writes": 0,
            "promotion_count": 0,
        },
        "decision": (
            "SESSION_SAFE_REPAIR_SIGNAL_REQUIRES_FROZEN_VALIDATION"
            if signal_gate_passed
            else "SESSION_SAFE_REPAIR_FALSIFIED_AT_TRIPWIRE"
        ),
        "read_only_worker": True,
        "outbound_order_capability": False,
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
    return runtime._run_session_safe_fast_book_relay(
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


def test_two_workers_are_parent_persisted_and_never_promoted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_relay_io(monkeypatch)
    calls: list[str] = []

    def worker(payload: dict[str, Any]) -> dict[str, Any]:
        variant = str(payload["repair_variant"])
        calls.append(variant)
        return _result(variant)

    monkeypatch.setattr(runtime, "session_safe_fast_book_worker", worker)
    state, results, portfolio = _run_relay(tmp_path, monkeypatch)

    assert set(calls) == set(runtime.SESSION_SAFE_REPAIR_VARIANTS)
    assert state["stage"] == (
        "SESSION_SAFE_FAST_BOOK_SIGNAL_REQUIRES_FROZEN_CONFIRMATION"
    )
    assert state["next_action"] == (
        "FREEZE_SESSION_SAFE_REPAIRS_AND_RUN_UNCHANGED_CHRONOLOGICAL_CONFIRMATION_50K_20D"
    )
    assert state["active_economic_worker_processes"] == 0
    assert state["session_safe_worker_limit"] == 2
    assert state["session_safe_parent_writer_only"] is True
    assert state["session_safe_tier_e_result_count"] == 2
    assert state["session_safe_signal_gate_pass_count"] == 2
    assert state["session_safe_exact_account_replay_count"] == 636
    assert state["exact_account_replays"] == 0
    assert state["combine_episodes_completed"] == 0
    assert state["authoritative_tier_g_count"] == 0
    assert state["independently_confirmed_tier_c_count"] == 0
    assert portfolio["evidence_tier"] == "E"
    assert portfolio["promotion_status"] is None
    assert portfolio["confirmation_contract"]["automatic_promotion_allowed"] is False
    assert all(value["promotion_status"] is None for value in results.values())

    artifact_root = (
        tmp_path
        / "output/branch_results/post_breadth_portfolio/session_safe_fast_book"
    )
    assert (artifact_root / "horizon_safe_entry_cutoff.json").is_file()
    assert (artifact_root / "drop_offending_component.json").is_file()
    assert (artifact_root / "portfolio.json").is_file()


def test_partial_resume_dispatches_only_missing_variant_without_double_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_relay_io(monkeypatch)
    calls: list[str] = []

    def worker(payload: dict[str, Any]) -> dict[str, Any]:
        variant = str(payload["repair_variant"])
        calls.append(variant)
        return _result(variant)

    monkeypatch.setattr(runtime, "session_safe_fast_book_worker", worker)
    first_state, _results, _portfolio = _run_relay(tmp_path, monkeypatch)
    artifact_root = (
        tmp_path
        / "output/branch_results/post_breadth_portfolio/session_safe_fast_book"
    )
    (artifact_root / "drop_offending_component.json").unlink()
    (artifact_root / "portfolio.json").unlink()

    second_state, _results, _portfolio = _run_relay(
        tmp_path, monkeypatch, prior_state=first_state
    )
    assert calls.count("HORIZON_SAFE_ENTRY_CUTOFF") == 1
    assert calls.count("DROP_OFFENDING_COMPONENT") == 2
    assert second_state["session_safe_exact_account_replay_count"] == 636
    assert second_state["exact_account_replays"] == first_state[
        "exact_account_replays"
    ]
    assert second_state["combine_episodes_completed"] == first_state[
        "combine_episodes_completed"
    ]


def test_resume_rejects_persisted_result_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_relay_io(monkeypatch)
    monkeypatch.setattr(
        runtime,
        "session_safe_fast_book_worker",
        lambda payload: _result(str(payload["repair_variant"])),
    )
    state, _results, _portfolio = _run_relay(tmp_path, monkeypatch)
    path = (
        tmp_path
        / "output/branch_results/post_breadth_portfolio/session_safe_fast_book"
        / "horizon_safe_entry_cutoff.json"
    )
    envelope = json.loads(path.read_text(encoding="utf-8"))
    envelope["session_safe_fast_book_tripwire"]["counters"][
        "promotion_count"
    ] = 1
    path.write_text(json.dumps(envelope), encoding="utf-8")

    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="hash drift"):
        _run_relay(tmp_path, monkeypatch, prior_state=state)


def test_strict_verifier_rejects_tier_or_safety_inflation() -> None:
    value = _result("HORIZON_SAFE_ENTRY_CUTOFF")
    value["evidence_tier"] = "G"
    value["result_hash"] = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="tier"):
        runtime._verify_session_safe_fast_book_result(
            value, expected_variant="HORIZON_SAFE_ENTRY_CUTOFF"
        )

    value = _result("DROP_OFFENDING_COMPONENT")
    value["counters"]["orders"] = 1
    value["result_hash"] = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="safety"):
        runtime._verify_session_safe_fast_book_result(
            value, expected_variant="DROP_OFFENDING_COMPONENT"
        )
