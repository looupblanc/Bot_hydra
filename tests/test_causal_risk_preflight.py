from __future__ import annotations

import hashlib
import json

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.causal_risk_preflight import (
    CausalRiskPreflightError,
    _sealed_preflight_manifest_is_compatible,
    executable_micro_quantity,
    risk_scale_gate,
    scale_causal_trajectory,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.causal_sleeve_replay import CausalTradeMark, CausalTradeTrajectory


def _trajectory() -> CausalTradeTrajectory:
    event = TradePathEvent(
        event_id="sleeve:test:1",
        decision_ns=120,
        exit_ns=180,
        session_day=1,
        net_pnl=10.0,
        gross_pnl=12.0,
        worst_unrealized_pnl=-4.0,
        best_unrealized_pnl=15.0,
        quantity=1,
        mini_equivalent=0.1,
    )
    return CausalTradeTrajectory(
        component_id="sleeve:test",
        market="MES",
        side=1,
        event=event,
        marks=(
            CausalTradeMark(
                availability_time_ns=180,
                worst_unrealized_pnl=-4.0,
                best_unrealized_pnl=15.0,
                current_unrealized_pnl=10.0,
            ),
        ),
        initial_unrealized_pnl=-2.0,
    )


def test_normalized_frontier_has_exact_whole_micro_mapping() -> None:
    assert [executable_micro_quantity(level) for level in (0.75, 1, 1.25, 1.5)] == [3, 4, 5, 6]
    with pytest.raises(CausalRiskPreflightError):
        executable_micro_quantity(1.1)


def test_trajectory_scaling_preserves_causal_identity_and_scales_economics() -> None:
    source = _trajectory()
    scaled = scale_causal_trajectory(source, executable_quantity_multiplier=5)
    assert scaled.event.event_id == source.event.event_id
    assert scaled.event.decision_ns == source.event.decision_ns
    assert scaled.event.exit_ns == source.event.exit_ns
    assert scaled.event.quantity == 5
    assert scaled.event.mini_equivalent == pytest.approx(0.5)
    assert scaled.event.net_pnl == pytest.approx(50.0)
    assert scaled.event.gross_pnl == pytest.approx(60.0)
    assert scaled.marks[0].worst_unrealized_pnl == pytest.approx(-20.0)
    assert scaled.marks[0].current_unrealized_pnl == pytest.approx(50.0)
    assert scaled.initial_unrealized_pnl == pytest.approx(-10.0)


def test_risk_gate_requires_both_cost_scenarios_and_positive_stress() -> None:
    base = {
        "preflight_policy_id": "pass",
        "normal": {"pass_count": 3, "mll_breach_rate": 0.10},
        "stressed": {"pass_count": 2, "net_total": 1.0, "mll_breach_rate": 0.10},
    }
    result = risk_scale_gate([base])
    assert result["status"] == "RISK_SCALE_ONLY_SURVIVORS_FOUND"
    assert result["survivor_ids"] == ["pass"]
    failed = {
        **base,
        "preflight_policy_id": "failed",
        "stressed": {"pass_count": 2, "net_total": 0.0, "mll_breach_rate": 0.10},
    }
    result = risk_scale_gate([failed])
    assert result["status"] == "RISK_SCALE_ONLY_FALSIFIED"


def test_sealed_preflight_accepts_only_anchored_kpi_repair(tmp_path) -> None:
    old_sha = "a" * 64
    receipt = {
        "classification": "TECHNICAL_STAGE3_KPI_INVALID_ROW_AGGREGATION_DEFECT",
        "scientific_status": "NO_ECONOMIC_SEMANTICS_CHANGE",
        "repair_scope": {"completed_stage3_batch_recomputed": False},
        "multiplicity": {"multiplicity_delta": 0},
        "preserved_preflight": {
            "path": (
                "reports/economic_evolution/causal_target_velocity_0028/"
                "preflight/risk_frontier_preflight_result.json"
            ),
            "file_sha256": "c" * 64,
            "result_hash": "d" * 64,
            "manifest_sha256": old_sha,
            "recomputed": False,
        },
    }
    receipt["repair_record_hash"] = stable_hash(receipt)
    receipt_path = tmp_path / "repair.json"
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    receipt_sha = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    manifest = {
        "technical_repair": {
            "classification": "TECHNICAL_STAGE3_KPI_INVALID_ROW_AGGREGATION_DEFECT",
            "economic_semantics_changed": False,
            "population_or_selection_changed": False,
            "risk_threshold_or_control_changed": False,
            "completed_evidence_recomputed": False,
            "completed_stage3_batch_reused_unchanged": True,
            "new_multiplicity_reservation_required": False,
            "supersedes_manifest_file_sha256": old_sha,
            "repair_receipt": {
                "path": "repair.json",
                "file_sha256": receipt_sha,
                "repair_record_hash": receipt["repair_record_hash"],
            },
        }
    }

    assert _sealed_preflight_manifest_is_compatible(
        existing_manifest_sha256=old_sha,
        existing_result_file_sha256="c" * 64,
        existing_result_hash="d" * 64,
        current_manifest_sha256="b" * 64,
        manifest=manifest,
        root=tmp_path,
    )
    manifest["technical_repair"]["completed_evidence_recomputed"] = True
    assert not _sealed_preflight_manifest_is_compatible(
        existing_manifest_sha256=old_sha,
        existing_result_file_sha256="c" * 64,
        existing_result_hash="d" * 64,
        current_manifest_sha256="b" * 64,
        manifest=manifest,
        root=tmp_path,
    )
