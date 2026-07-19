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


def _card() -> dict[str, Any]:
    return {"card_hash": "c" * 64}


def _features() -> dict[str, Any]:
    core = {
        "schema": runtime.SESSION_SAFE_CONFIRMATION_FEATURE_SCHEMA,
        "status": "CAUSAL_FEATURE_BUNDLES_READY",
        "decision_card_hash": "c" * 64,
        "source_files": [{"path": "/tmp/source", "sha256": "d" * 64}],
        "contract_map": {"path": "/tmp/map", "sha256": "e" * 64},
        "bundles": {
            market: {
                "path": f"/tmp/{market}",
                "bundle_hash": str(index) * 64,
                "row_count": 100,
                "cache_hit": False,
            }
            for index, market in enumerate(("RTY", "YM", "ES"), start=1)
        },
        "future_outcomes_in_decision_bundle": False,
    }
    return {**core, "result_hash": stable_hash(core)}


def _summary(episode_count: int, pass_count: int) -> dict[str, Any]:
    return {
        "episode_count": episode_count,
        "pass_count": pass_count,
        "mll_breach_count": 0,
        "net_total_usd": 2000.0,
        "mll_breach_rate": 0.0,
        "consistency_compliance_rate": 1.0,
    }


def _result(*, passed: bool = True) -> dict[str, Any]:
    starts = {"5": 4, "10": 2, "20": 1}
    horizons = {
        horizon: {
            "horizon_trading_days": int(horizon),
            "full_coverage_start_count": count,
            "normal": _summary(count, 1 if passed else 0),
            "stressed": _summary(count, 1 if passed else 0),
        }
        for horizon, count in starts.items()
    }
    records: list[dict[str, Any]] = []
    for horizon, count in starts.items():
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            for index in range(count):
                record = {
                    "scenario": scenario,
                    "horizon_trading_days": int(horizon),
                    "start_day": index,
                }
                record["record_hash"] = stable_hash(record)
                records.append(record)
    checks = {
        "normal_pass": passed,
        "stressed_pass": passed,
        "positive_stressed_net": passed,
        "controlled_stressed_mll": True,
        "stressed_passing_consistency": passed,
        "full_coverage": True,
    }
    core = {
        "schema": runtime.SESSION_SAFE_CONFIRMATION_SCHEMA,
        "status": "CONFIRMATION_CONSUMED_ONCE",
        "decision_card_hash": "c" * 64,
        "acquisition_receipt_hash": "f" * 64,
        "account_label": "50K",
        "policy_id": "session_safe_confirmed_book",
        "source_evidence_tier": "E",
        "component_results": {
            "component_a": {"recalibrated": False, "batch_stream_equal": True},
            "component_b": {"recalibrated": False, "batch_stream_equal": True},
        },
        "calendar": {"session_count": 40},
        "horizon_results": horizons,
        "confirmation_gate": {
            "passed": passed,
            "checks": checks,
            "evidence_ceiling": (
                "FRESH_REPLICATION_SUCCESS_TIER_Q_ELIGIBLE_NOT_TIER_C"
            ),
        },
        "resulting_evidence_status": (
            "FRESH_REPLICATION_SUCCESS_TIER_Q_ELIGIBLE_NOT_TIER_C"
            if passed
            else "FRESH_REPLICATION_FAILED_BRANCH_CLOSED"
        ),
        "evidence_bundle_adapter": {
            "evaluated_policy_records": records,
            "records_hash": stable_hash(records),
            "sealing_performed": False,
        },
        "official_rule_snapshot": {},
        "retuning_performed": False,
        "recalibration_performed": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


class _ThreadExecutor:
    def __init__(self, *, max_workers: int, mp_context: object) -> None:
        assert max_workers == 1
        del mp_context
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

    def __enter__(self) -> "_ThreadExecutor":
        return self

    def __exit__(self, *args: object) -> None:
        self._pool.shutdown(wait=True)

    def submit(self, function: Any, payload: dict[str, Any]) -> Any:
        return self._pool.submit(function, payload)


def _patch_io(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "ProcessPoolExecutor", _ThreadExecutor)
    monkeypatch.setattr(runtime, "_publish", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_write_mission_views", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_kpis", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(runtime, "_append_decision_once", lambda *_args: None)
    monkeypatch.setattr(runtime, "load_session_safe_confirmation_card", lambda _path: _card())


def _prepare_card_path(tmp_path: Path) -> None:
    path = tmp_path / "config/research/session_safe_m2k_mym_confirmation_decision_card_v1.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")


def _write_acquisition(tmp_path: Path) -> Path:
    source = tmp_path / "source.parquet"
    contract_map = tmp_path / "contract-map.json"
    source.write_bytes(b"source")
    contract_map.write_text("{}", encoding="utf-8")
    core = {
        "decision_card_hash": "c" * 64,
        "download_status": "DOWNLOADED",
        "feature_build_inputs": {
            "source_files": [{"path": str(source), "sha256": "d" * 64}],
            "contract_map_path": str(contract_map),
            "cache_root": str(tmp_path / "cache"),
        },
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    receipt = {**core, "receipt_hash": stable_hash(core)}
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    return path


def _run(
    tmp_path: Path,
    receipt_path: Path,
    *,
    prior_state: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    output = tmp_path / "output"
    output.mkdir(exist_ok=True)
    return runtime._run_session_safe_m2k_mym_confirmation_relay(
        root=tmp_path,
        manifest=_manifest(),
        output=output,
        live_writer=object(),
        branch_writer=AtomicResultWriter(output / "branch_results"),
        prior_state=prior_state or {"checkpoint_sequence": 0},
        started=time.monotonic(),
        heartbeat_seconds=0.01,
        runtime_results={},
        acquisition_receipt_path=receipt_path,
    )


def test_missing_receipt_waits_without_terminal_economic_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_card_path(tmp_path)
    _patch_io(monkeypatch)
    state, results, waiting = _run(tmp_path, tmp_path / "missing.json")

    assert waiting["status"] == runtime._SESSION_SAFE_CONFIRMATION_WAITING
    assert waiting["economic_result_created"] is False
    assert state["stage"] == runtime._SESSION_SAFE_CONFIRMATION_WAITING
    assert state["active_economic_worker_processes"] == 0
    assert state["session_safe_confirmation_worker_limit"] == 1
    assert state["exact_account_replays"] == 0
    assert state["combine_episodes_completed"] == 0
    assert not results
    assert not (
        tmp_path
        / "output/branch_results/post_breadth_portfolio/session_safe_m2k_mym_confirmation/confirmation_result.json"
    ).exists()


def test_present_input_runs_one_worker_parent_persists_and_resumes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_card_path(tmp_path)
    _patch_io(monkeypatch)
    receipt = _write_acquisition(tmp_path)
    calls = {"features": 0, "worker": 0, "decisions": 0}

    def build(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        calls["features"] += 1
        return _features()

    def worker(_payload: dict[str, Any]) -> dict[str, Any]:
        calls["worker"] += 1
        return _result()

    monkeypatch.setattr(runtime, "build_session_safe_confirmation_features", build)
    monkeypatch.setattr(runtime, "_session_safe_m2k_mym_confirmation_worker", worker)
    monkeypatch.setattr(
        runtime,
        "_append_decision_once",
        lambda *_args: calls.__setitem__("decisions", calls["decisions"] + 1),
    )

    first_state, first_results, confirmation = _run(tmp_path, receipt)
    assert calls == {"features": 1, "worker": 1, "decisions": 1}
    assert confirmation["resulting_evidence_status"].endswith("NOT_TIER_C")
    assert first_state["stage"] == (
        "SESSION_SAFE_M2K_MYM_CONFIRMATION_TIER_Q_ELIGIBLE"
    )
    assert first_state["session_safe_confirmation_tier_q_eligible_count"] == 1
    assert first_state["session_safe_confirmation_automatic_tier_g_count"] == 0
    assert first_state["session_safe_confirmation_automatic_tier_c_count"] == 0
    assert first_state["session_safe_confirmation_automatic_tier_f_count"] == 0
    assert first_state["session_safe_confirmation_50k_20d_normal_pass_count"] == 1
    assert first_state["session_safe_confirmation_50k_20d_stressed_pass_count"] == 1
    assert first_state["exact_account_replays"] == 0
    assert first_state["combine_episodes_completed"] == 0
    assert "POST_BREADTH_SESSION_SAFE_CONFIRMATION" in first_results

    second_state, _results, second = _run(
        tmp_path, receipt, prior_state=first_state
    )
    assert calls == {"features": 1, "worker": 1, "decisions": 1}
    assert second["result_hash"] == confirmation["result_hash"]
    assert second_state["session_safe_confirmation_exact_episode_count"] == 14
    assert second_state["exact_account_replays"] == first_state["exact_account_replays"]
    assert second_state["combine_episodes_completed"] == first_state[
        "combine_episodes_completed"
    ]


def test_failed_confirmation_closes_repair_without_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_card_path(tmp_path)
    _patch_io(monkeypatch)
    receipt = _write_acquisition(tmp_path)
    monkeypatch.setattr(
        runtime, "build_session_safe_confirmation_features", lambda *_a, **_k: _features()
    )
    monkeypatch.setattr(
        runtime,
        "_session_safe_m2k_mym_confirmation_worker",
        lambda _payload: _result(passed=False),
    )
    state, _results, result = _run(tmp_path, receipt)
    assert result["resulting_evidence_status"] == (
        "FRESH_REPLICATION_FAILED_BRANCH_CLOSED"
    )
    assert state["stage"] == "SESSION_SAFE_M2K_MYM_CONFIRMATION_BRANCH_CLOSED"
    assert state["next_action"] == (
        "CLOSE_SESSION_SAFE_REPAIR_AND_DISPATCH_DISTINCT_CAUSAL_BRANCH"
    )
    assert state["session_safe_confirmation_tier_q_eligible_count"] == 0


def test_verifier_rejects_tier_c_or_safety_inflation() -> None:
    value = _result()
    value["resulting_evidence_status"] = "TIER_C"
    value["result_hash"] = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="ceiling"):
        runtime._verify_session_safe_m2k_mym_confirmation_result(value)

    value = _result()
    value["orders"] = 1
    value["result_hash"] = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="safety"):
        runtime._verify_session_safe_m2k_mym_confirmation_result(value)
