from __future__ import annotations

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
        "post_breadth_branch_portfolio": {
            "cross_asset_daily_decision_card_hash": "c" * 64,
        },
    }


def _summary(*, passes: int = 0) -> dict[str, Any]:
    return {
        "episode_count": 11,
        "pass_count": passes,
        "pass_rate": passes / 11,
        "pass_count_by_block": {},
        "blocks_with_passes": [],
        "net_total_usd": 100.0,
        "net_median_usd": 10.0,
        "target_progress_median": 0.1,
        "target_progress_p25": -0.1,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer_usd": 100.0,
        "consistency_compliance_rate": 1.0,
        "all_passing_paths_consistency_compliant": True,
        "terminal_distribution": {"DATA_CENSORED": 11},
    }


def _result() -> dict[str, Any]:
    cell = {
        "candidate_id": "candidate",
        "control": "PRIMARY",
        "account_label": "50K",
        "account_size_usd": 50_000,
        "micro_quantity": 8,
        "mini_equivalent": 0.8,
        "horizon_trading_days": 20,
        "full_coverage_start_count": 11,
        "normal": _summary(),
        "stressed": _summary(),
    }
    checks = {
        "headline_horizon": True,
        "normal_passes": False,
        "stressed_passes": False,
        "passing_block_diversity": False,
        "controlled_stressed_mll": True,
        "positive_stressed_net": True,
        "passing_consistency": True,
        "beats_direction_flip": True,
    }
    core = {
        "schema": runtime.CLEAN_CROSS_ASSET_DAILY_SCHEMA,
        "branch_id": runtime.CLEAN_CROSS_ASSET_DAILY_BRANCH_ID,
        "status": "CROSS_ASSET_DAILY_DIRECTION_TRANSFER_FALSIFIED",
        "evidence_role": "VIEWED_DEVELOPMENT_TRIPWIRE_ONLY",
        "candidate_id": "candidate",
        "source_bindings": {"decision_card_hash": "c" * 64},
        "event_count": 28,
        "calendar_session_count": 234,
        "start_counts": {"5": 46, "10": 23, "20": 11},
        "account_cell_count": 180,
        "best_observed_primary_cell": cell,
        "best_safe_primary_cell": cell,
        "best_primary_cell": cell,
        "matched_direction_flip_cell": {**cell, "control": "DIRECTION_FLIP"},
        "frozen_gate": {"passed": False, "checks": checks},
        "cells": [],
        "promotion_status": None,
        "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        "independent_confirmation_claimed": False,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": "REALLOCATE_TO_NON_OHLCV_REPRESENTATION",
    }
    return {**core, "result_hash": stable_hash(core)}


class _ThreadExecutor:
    def __init__(self, *, max_workers: int, mp_context: object) -> None:
        assert max_workers == 1
        del mp_context
        self._pool = ThreadPoolExecutor(max_workers=1)

    def __enter__(self) -> "_ThreadExecutor":
        return self

    def __exit__(self, *args: object) -> None:
        self._pool.shutdown(wait=True)

    def submit(self, function: Any, *args: Any) -> Any:
        return self._pool.submit(function, *args)


def _run(
    tmp_path: Path, *, prior_state: dict[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    output = tmp_path / "output"
    output.mkdir(exist_ok=True)
    return runtime._run_clean_cross_asset_daily_tripwire_relay(
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


def test_relay_runs_once_parent_persists_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    card = tmp_path / "config/research/clean_cross_asset_daily_tripwire_decision_card_v1.json"
    card.parent.mkdir(parents=True)
    card.write_text("{}", encoding="utf-8")
    calls = {"worker": 0, "decision": 0}
    monkeypatch.setattr(runtime, "ProcessPoolExecutor", _ThreadExecutor)
    monkeypatch.setattr(runtime, "_publish", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_write_mission_views", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_kpis", lambda *_args, **_kwargs: {})

    def worker(*_args: Any) -> dict[str, Any]:
        calls["worker"] += 1
        return _result()

    monkeypatch.setattr(runtime, "_clean_cross_asset_daily_tripwire_worker", worker)
    monkeypatch.setattr(
        runtime,
        "_append_decision_once",
        lambda *_args: calls.__setitem__("decision", calls["decision"] + 1),
    )

    first_state, first_results, first = _run(tmp_path)
    assert calls == {"worker": 1, "decision": 1}
    assert first_state["stage"] == "CROSS_ASSET_DAILY_DIRECTION_TRANSFER_FALSIFIED"
    assert first_state["cross_asset_daily_best_safe_stressed_pass_count"] == 0
    assert first_state["cross_asset_daily_promotion_count"] == 0
    assert "POST_BREADTH_CLEAN_CROSS_ASSET_DAILY" in first_results

    second_state, _second_results, second = _run(
        tmp_path, prior_state=first_state
    )
    assert calls == {"worker": 1, "decision": 1}
    assert second["result_hash"] == first["result_hash"]
    assert second_state["cross_asset_daily_event_count"] == 28


def test_verifier_rejects_promotion_or_safety_inflation() -> None:
    value = _result()
    value["promotion_status"] = "Q"
    value["result_hash"] = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="ceiling"):
        runtime._verify_clean_cross_asset_daily_tripwire_result(value)

    value = _result()
    value["orders"] = 1
    value["result_hash"] = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    with pytest.raises(runtime.AutonomousDirectorRuntimeError, match="safety"):
        runtime._verify_clean_cross_asset_daily_tripwire_result(value)
