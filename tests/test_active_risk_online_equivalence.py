from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.shadow.active_risk_online_equivalence import (
    ActiveRiskOnlineEquivalenceError,
    CanonicalBarOrderGuard,
    EQUIVALENCE_SCHEMA,
    ENGINE_VERSION,
    FrozenSleeveOnlineEngine,
    ONLINE_OFFLINE_EQUIVALENCE_FAILED_CLOSED,
    stable_hash,
    verify_active_risk_online_equivalence_proof,
)
from hydra.shadow.active_risk_package import FrozenSignalBinding
from hydra.research.turbo_feature_builder import FEATURE_BUNDLE_VERSION, FEATURE_DAG_HASH


H = "a" * 64


def _binding() -> FrozenSignalBinding:
    return FrozenSignalBinding(
        sleeve_id="sleeve_test",
        trigger_feature="past_return_5",
        trigger_operator="GT",
        trigger_threshold=1.0,
        context_feature=None,
        context_operator=None,
        context_threshold=None,
        calibration_start="2023-01-01",
        calibration_end_exclusive="2023-07-01",
        trigger_finite_observation_count=100,
        context_finite_observation_count=None,
        source_execution_fingerprint=H,
        source_cheap_screen_path="reports/source.json",
        source_cheap_screen_sha256=H,
        source_cheap_screen_row_sha256=H,
        feature_matrix_manifest_path="data/features/manifest.json",
        feature_matrix_manifest_sha256=H,
        feature_matrix_schema="hydra_canonical_feature_store_v2",
        feature_matrix_bundle_hash=H,
        feature_matrix_source_data_sha256=H,
        feature_matrix_roll_map_sha256=H,
        feature_matrix_market="ES",
        feature_matrix_execution_market="MES",
        feature_bundle_version=FEATURE_BUNDLE_VERSION,
        feature_dag_hash=FEATURE_DAG_HASH,
        trigger_array_sha256=H,
        context_array_sha256=None,
        session_day_array_sha256=H,
        session_code_array_sha256=H,
    )


def _spec() -> SimpleNamespace:
    return SimpleNamespace(
        sleeve_id="sleeve_test",
        side=1,
        session_code=0,
        holding_bars=5,
    )


def _feature_record(index: int, *, trigger: float = 2.0, horizon=True) -> dict:
    return {
        "matrix_fingerprint": H,
        "row_index": index,
        "timestamp_ns": index * 60_000_000_000,
        "decision_ns": (index + 1) * 60_000_000_000,
        "availability_ns": (index + 1) * 60_000_000_000,
        "segment_code": 1,
        "session_day": 19_400,
        "session_code": 0,
        "contract_code": 0,
        "entry_price": 100.0,
        "bar_open": 99.0,
        "bar_high": 101.0,
        "bar_low": 98.0,
        "bar_close": 100.0,
        "trigger_value": trigger,
        "context_value": None,
        "horizon_available": horizon,
    }


def test_scalar_engine_is_record_ordered_and_restart_exact() -> None:
    engine = FrozenSleeveOnlineEngine(_spec(), _binding(), H)
    first = engine.process_record(_feature_record(0))
    assert len(first) == 1
    assert first[0]["signal_id"].startswith("sleeve_test:00000:")
    checkpoint = engine.checkpoint()
    resumed = FrozenSleeveOnlineEngine.from_checkpoint(
        _spec(), _binding(), H, checkpoint
    )
    assert resumed.process_record(_feature_record(1)) == ()
    with pytest.raises(ActiveRiskOnlineEquivalenceError, match="non-contiguous"):
        resumed.process_record(_feature_record(3))


def test_historical_horizon_mask_fails_closed() -> None:
    engine = FrozenSleeveOnlineEngine(_spec(), _binding(), H)
    assert engine.process_record(_feature_record(0, horizon=False)) == ()


def test_bar_guard_rejects_duplicate_gap_and_bad_roll() -> None:
    row = {
        "timestamp_ns": 60_000_000_000,
        "availability_ns": 120_000_000_000,
        "observed_ns": 120_000_000_000,
        "segment_code": 1,
        "session_day": 20_000,
        "contract": "ESU6",
        "close": 100.0,
    }
    guard = CanonicalBarOrderGuard()
    assert guard.process(row) == "ACCEPTED"
    with pytest.raises(ActiveRiskOnlineEquivalenceError, match="DUPLICATE_BAR"):
        guard.process(row)

    gap = CanonicalBarOrderGuard()
    gap.process(row)
    with pytest.raises(ActiveRiskOnlineEquivalenceError, match="MISSING_INTERVAL"):
        gap.process(
            {
                **row,
                "timestamp_ns": 180_000_000_000,
                "availability_ns": 240_000_000_000,
                "observed_ns": 240_000_000_000,
            }
        )

    roll = CanonicalBarOrderGuard()
    roll.process(row)
    with pytest.raises(
        ActiveRiskOnlineEquivalenceError, match="CONTRACT_ROLL_WITHOUT_SEGMENT"
    ):
        roll.process(
            {
                **row,
                "timestamp_ns": 120_000_000_000,
                "availability_ns": 180_000_000_000,
                "observed_ns": 180_000_000_000,
                "contract": "ESZ6",
            }
        )


def test_verifier_rejects_fail_closed_receipt(tmp_path: Path) -> None:
    proof = {
        "schema": EQUIVALENCE_SCHEMA,
        "engine_version": ENGINE_VERSION,
        "status": ONLINE_OFFLINE_EQUIVALENCE_FAILED_CLOSED,
        "mismatch_count": 0,
    }
    proof["proof_hash"] = stable_hash(proof)
    path = tmp_path / "proof.json"
    path.write_text(json.dumps(proof), encoding="utf-8")
    with pytest.raises(
        ActiveRiskOnlineEquivalenceError, match="equivalence is not proven"
    ):
        verify_active_risk_online_equivalence_proof(
            path,
            repository_root=tmp_path,
            expected_package_manifest_hash=H,
            expected_package_ids=(),
        )
