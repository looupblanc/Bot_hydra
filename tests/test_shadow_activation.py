from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from hydra.mission.calibration_retest_execution import _stable_hash
from hydra.pipelines.shadow_pipeline import (
    ShadowPipelineIntegrityError,
    registry_entry_from_activation,
    tick_shadow_pipeline,
)
from hydra.shadow.activation import (
    ShadowActivationError,
    audit_zero_order_surface,
    run_immutable_shadow_activation,
    run_ym_shadow_activation,
)
from hydra.shadow.specification import ShadowSpecification


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _configuration(path: Path) -> ShadowSpecification:
    specification = ShadowSpecification(
        strategy_id="strategy_open_gap_continuation_YM_v1",
        strategy_version="v1",
        feature_versions=("gap_state_v1",),
        markets=("YM", "MYM"),
        timeframes=("1m", "session"),
        session_rules={"timezone": "America/Chicago"},
        entry_rules={"decision": "08:31"},
        exit_rules={"holding_minutes": 60},
        sizing={"instrument": "MYM", "contracts": 1},
        costs={"round_turn_usd": 3.0},
        stale_data_seconds=120,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=86400,
        maximum_exposure=1.0,
        simulated_mll_floor=-4500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=("stale_data", "mll_floor"),
        logging={"events": True},
        reconciliation={"startup": "fail_closed"},
        source_manifest_hash="a" * 64,
    )
    specification.write_immutable(path)
    return specification


def test_activation_and_pipeline_are_immutable_and_fail_closed(tmp_path: Path) -> None:
    task = tmp_path / "task.md"
    task.write_text("immutable shadow task\n", encoding="utf-8")
    configuration_path = tmp_path / "configuration.json"
    specification = _configuration(configuration_path)
    strict = {
        "candidate_id": "strategy_open_gap_continuation_YM_v1",
        "shadow_activation_eligible": True,
        "hard_invalidations": [],
        "candidates": [
            {
                "candidate_id": "strategy_open_gap_continuation_YM_v1",
                "status": "SHADOW_RESEARCH_CANDIDATE",
                "topstep": {"path_candidate": True},
            }
        ],
    }
    strict["result_hash"] = _stable_hash(strict)
    strict_path = tmp_path / "strict.json"
    strict_path.write_text(json.dumps(strict, sort_keys=True) + "\n", encoding="utf-8")
    surface = tmp_path / "safe_surface.py"
    surface.write_text("def virtual_fill():\n    return 0\n", encoding="utf-8")

    result = run_ym_shadow_activation(
        tmp_path / "output",
        engineering_task_path=task,
        engineering_task_sha256=_sha(task),
        strict_result_path=strict_path,
        strict_result_sha256=_sha(strict_path),
        strict_result_hash=strict["result_hash"],
        shadow_configuration_path=configuration_path,
        shadow_configuration_sha256=_sha(configuration_path),
        shadow_configuration_hash=specification.configuration_hash,
        code_commit="test",
        code_surface_paths=[surface],
    )
    entry = registry_entry_from_activation(result)
    state_dir = tmp_path / "state"
    waiting = tick_shadow_pipeline(
        state_dir,
        {result["candidate_id"]: entry},
        now=datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc),
    )

    assert result["shadow_active"] == 1
    assert result["paper_shadow_ready"] == 0
    assert result["activation_manifest"]["outbound_orders_enabled"] is False
    assert waiting["shadow_research_active"] == 1
    assert waiting["candidates"][result["candidate_id"]]["runtime_state"] == "WAITING_FOR_FRESH_FORWARD_DATA"
    assert waiting["outbound_orders"] == 0

    feed_dir = state_dir / "forward_data"
    feed_dir.mkdir(parents=True)
    (feed_dir / f"{result['candidate_id']}.heartbeat.json").write_text(
        json.dumps({"latest_completed_bar_at_utc": "2026-07-11T11:59:30+00:00"}),
        encoding="utf-8",
    )
    ready = tick_shadow_pipeline(
        state_dir,
        {result["candidate_id"]: entry},
        now=datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc),
    )
    assert ready["candidates"][result["candidate_id"]]["runtime_state"] == "READY_FOR_VIRTUAL_SIGNALS"

    configuration_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ShadowPipelineIntegrityError, match="changed"):
        tick_shadow_pipeline(state_dir, {result["candidate_id"]: entry})


def test_order_surface_audit_detects_submission_capability(tmp_path: Path) -> None:
    unsafe = tmp_path / "unsafe.py"
    unsafe.write_text("def submit_order():\n    return True\n", encoding="utf-8")

    audit = audit_zero_order_surface([unsafe])

    assert not audit["passed"]
    assert audit["violations"][0]["reason"] == "prohibited_function:submit_order"


def test_generic_activation_requires_official_shadow_candidate_and_hashes(
    tmp_path: Path,
) -> None:
    candidate_id = "strategy_barrier_hazard_NQ_v1"
    task = tmp_path / "task.md"
    task.write_text("immutable generic activation\n", encoding="utf-8")
    configuration_path = tmp_path / "configuration.json"
    specification = ShadowSpecification(
        strategy_id=candidate_id,
        strategy_version="v1",
        feature_versions=("barrier_hazard_v1",),
        markets=("MNQ",),
        timeframes=("1m", "15m"),
        session_rules={"timezone": "America/Chicago"},
        entry_rules={"decision": "completed_bar"},
        exit_rules={"same_bar_policy": "stop_first"},
        sizing={"instrument": "MNQ", "contracts": 1},
        costs={"round_turn_usd": 3.0},
        stale_data_seconds=120,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=3600,
        maximum_exposure=1.0,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=("stale_data", "mll_floor"),
        logging={"events": True},
        reconciliation={"startup": "fail_closed"},
        source_manifest_hash="b" * 64,
    )
    specification.write_immutable(configuration_path)
    source = {
        "scientific_conclusion": "BARRIER_HAZARD_SHADOW_CANDIDATE_FOUND",
        "candidates": [
            {
                "candidate_id": candidate_id,
                "status": "SHADOW_RESEARCH_CANDIDATE",
                "net_pnl": 1000.0,
                "micro_net_pnl": 100.0,
                "admission": {
                    "permits_zero_risk_shadow": True,
                    "fatal_reasons": [],
                },
                "shadow_evidence": {
                    "hard_invalidations": [],
                    "account_mll_safe": True,
                    "deterministic_signals": True,
                    "realtime_features_available": True,
                    "observability_complete": True,
                },
                "topstep": {"path_candidate": False},
            }
        ],
        "shadow_configurations": [
            {
                "candidate_id": candidate_id,
                "configuration_hash": specification.configuration_hash,
                "outbound_orders_enabled": False,
            }
        ],
    }
    source["result_hash"] = _stable_hash(source)
    source_path = tmp_path / "source.json"
    source_path.write_text(json.dumps(source, sort_keys=True) + "\n", encoding="utf-8")
    surface = tmp_path / "surface.py"
    surface.write_text("def virtual_fill():\n    return 0\n", encoding="utf-8")

    result = run_immutable_shadow_activation(
        tmp_path / "output",
        engineering_task_path=task,
        engineering_task_sha256=_sha(task),
        source_result_path=source_path,
        source_result_sha256=_sha(source_path),
        source_result_hash=source["result_hash"],
        candidate_id=candidate_id,
        shadow_configuration_path=configuration_path,
        shadow_configuration_sha256=_sha(configuration_path),
        shadow_configuration_hash=specification.configuration_hash,
        code_commit="test",
        code_surface_paths=[surface],
    )

    assert result["candidate_id"] == candidate_id
    assert result["candidates"][0]["status"] == "SHADOW_ACTIVE"
    assert result["activation_manifest"]["outbound_orders_enabled"] is False
    assert registry_entry_from_activation(result)["candidate_id"] == candidate_id

    source["candidates"][0]["status"] = "PROMISING_RESEARCH_CANDIDATE"
    source["result_hash"] = _stable_hash(
        {key: value for key, value in source.items() if key != "result_hash"}
    )
    source_path.write_text(json.dumps(source, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(ShadowActivationError):
        run_immutable_shadow_activation(
            tmp_path / "rejected",
            engineering_task_path=task,
            engineering_task_sha256=_sha(task),
            source_result_path=source_path,
            source_result_sha256=_sha(source_path),
            source_result_hash=source["result_hash"],
            candidate_id=candidate_id,
            shadow_configuration_path=configuration_path,
            shadow_configuration_sha256=_sha(configuration_path),
            shadow_configuration_hash=specification.configuration_hash,
            code_commit="test",
            code_surface_paths=[surface],
        )
