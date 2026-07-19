from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

import hydra.evidence.causal_target_velocity_adapter as adapter_module
from hydra.evidence import REQUIRED_DATASETS, iter_evidence_records, verify_evidence_bundle
from hydra.evidence.causal_target_velocity_adapter import (
    CausalTargetVelocityEvidenceError,
    adapt_exact_hazard_replay,
    finalize_causal_target_velocity_evidence_bundle,
    reconstruct_exact_hazard_replay,
)
from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.causal_sleeve_replay import CENSORED_FUTURE_COVERAGE
from hydra.research.causal_target_velocity import (
    CalibratedHazardCandidate,
    HazardCandidate,
    calibrate_candidate,
    discover_intents_batch,
    exact_sleeve_replay,
    observe_outcomes,
    stable_hash,
)


MINUTE = 60_000_000_000


def _candidate() -> HazardCandidate:
    return HazardCandidate(
        market="CL",
        execution_market="MCL",
        mechanism="PARTICIPATION_DENSITY",
        cross_asset_reference_market=None,
        timeframe="1m",
        session_code=0,
        trigger_feature="past_participation",
        trigger_operator="GT",
        trigger_quantile=0.65,
        context_feature="rv_short_long_ratio",
        context_operator="GT",
        context_quantile=0.50,
        direction_rule="PAST_RETURN_CONTINUATION",
        favorable_r=0.5,
        adverse_r=0.5,
        horizon=5,
        risk_level=1.0,
        cooldown_minutes=5,
    )


def _matrix(*, rows: int = 80, direction: int = -1) -> FeatureMatrix:
    timestamp = np.arange(rows, dtype=np.int64) * MINUTE
    trigger = np.ones(rows, dtype=float)
    if rows > 45:
        trigger[45] = 10.0
    opens = np.full(rows, 100.0, dtype=float)
    highs = np.full(rows, 100.02, dtype=float)
    lows = np.full(rows, 99.98, dtype=float)
    if rows > 47:
        if direction > 0:
            highs[47] = 101.0
        else:
            lows[47] = 99.0
    arrays = {
        "timestamp_ns": timestamp,
        "decision_ns": timestamp + MINUTE,
        "availability_ns": timestamp + MINUTE,
        "session_day": np.full(rows, 19_724, dtype=np.int32),
        "session_code": np.zeros(rows, dtype=np.int8),
        "segment_code": np.ones(rows, dtype=np.int32),
        "contract_code": np.ones(rows, dtype=np.int32),
        "bar_open": opens,
        "bar_high": highs,
        "bar_low": lows,
        "bar_close": np.full(rows, 100.0, dtype=float),
        "feature__past_participation": trigger,
        "feature__rv_short_long_ratio": np.concatenate(
            (np.ones(min(rows, 40)), np.full(max(0, rows - 40), 2.0))
        ),
        "feature__past_return_60": np.full(rows, 0.01 * direction, dtype=float),
        "feature__past_volatility": np.full(rows, 0.001, dtype=float),
    }
    return FeatureMatrix(
        root=None,  # type: ignore[arg-type]
        manifest={
            "row_count": rows,
            "bundle_hash": "a" * 64,
            "key": {"market": "CL"},
        },
        arrays=arrays,
    )


def _calibrated(matrix: FeatureMatrix) -> CalibratedHazardCandidate:
    return calibrate_candidate(
        _candidate(),
        matrix,
        calibration_end_exclusive_ns=40 * MINUTE,
        minimum_observations=20,
    )


def _replay(*, rows: int = 80):
    matrix = _matrix(rows=rows)
    calibrated = _calibrated(matrix)
    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    events = observe_outcomes(calibrated, matrix, intents)
    return exact_sleeve_replay(
        calibrated,
        events,
        eligible_session_days=[19_724],
    )


def _manifest() -> dict[str, object]:
    return {
        "campaign_id": "hydra_causal_target_velocity_0028_test",
        "class_id": "TARGET_BEFORE_ADVERSE_EXCURSION_HAZARD_OPPORTUNITY_DENSITY_V1",
        "source_commit": "d" * 40,
        "created_at_utc": "2026-07-17T00:00:00Z",
        "manifest_hash": "e" * 64,
        "seeds": [0],
    }


def _policy(replay) -> dict[str, object]:
    policy_id = replay.candidate.candidate_id
    payload = {
        "policy_id": policy_id,
        "component_ids": [policy_id],
        "static_risk_tier": 1.0,
        "policy_version": "CAUSAL_HAZARD_STANDALONE_V1",
    }
    payload["structural_fingerprint"] = stable_hash(
        {key: value for key, value in payload.items() if key != "policy_id"}
    )
    return payload


def _episode(replay, *, scenario: str) -> dict[str, object]:
    component_id = replay.candidate.candidate_id
    costs = 2.0 if scenario == "NORMAL" else 3.0
    return {
        "policy_id": component_id,
        "start_day": 19_724,
        "end_day": 19_724,
        "terminal": "PASSED",
        "terminal_reason": "profit_target_reached",
        "eligible_days": 1,
        "traded_days": 1,
        "total_cost": costs,
        "net_pnl": 9_000.0,
        "target_progress": 1.0,
        "maximum_target_progress": 1.0,
        "minimum_mll_buffer": 4_000.0,
        "consistency_ok": True,
        "best_day_concentration": 0.4,
        "days_to_target": 1,
        "component_contribution": {component_id: 9_000.0},
        "accepted_events": 1,
        "skipped_events": 0,
        "risk_allocation_path": [],
        "daily_path": [
            {
                "session_day": 19_724,
                "balance": 159_000.0,
                "mll_floor": 154_500.0,
                "mll_buffer": 4_500.0,
                "minimum_mll_buffer": 4_000.0,
                "day_pnl": 9_000.0,
                "realized_pnl": 9_000.0,
                "unrealized_pnl": 0.0,
                "costs": costs,
                "target_progress": 1.0,
                "consistency": 0.4,
                "consistency_ok": True,
                "conflicts": {},
                "exposure": {"maximum_mini_equivalent": 1.0},
                "component_attribution": {component_id: 9_000.0},
                "open_positions": 0,
            }
        ],
    }


def _records(replay) -> list[dict[str, object]]:
    policy_id = replay.candidate.candidate_id
    return [
        {
            "policy_id": policy_id,
            "episode_id": "episode_001",
            "scenario": scenario,
            "horizon": "90_TRADING_DAYS",
            "temporal_block": "B1",
            "episode": _episode(replay, scenario=scenario),
        }
        for scenario in ("NORMAL", "STRESSED_1_5X")
    ]


def _expected_hashes(replay) -> dict[str, str]:
    return {
        "decision_hash": replay.decision_hash,
        "normal_event_hash": replay.normal_event_hash,
        "stressed_event_hash": replay.stressed_event_hash,
        "normal_trajectory_hash": replay.normal_trajectory_hash,
        "stressed_trajectory_hash": replay.stressed_trajectory_hash,
        "fill_policy_hash": replay.fill_policy_hash,
    }


def test_reconstructs_worker_mapping_and_rejects_hash_drift() -> None:
    replay = _replay()
    restored = reconstruct_exact_hazard_replay(
        candidate_payload=replay.candidate.payload,
        event_mappings=[row.to_dict() for row in replay.events],
        eligible_session_days=replay.eligible_session_days,
        expected_hashes=_expected_hashes(replay),
    )
    assert _expected_hashes(restored) == _expected_hashes(replay)
    drift = _expected_hashes(replay)
    drift["decision_hash"] = "0" * 64
    with pytest.raises(
        CausalTargetVelocityEvidenceError,
        match="worker/coordinator exact hazard replay mismatch",
    ):
        reconstruct_exact_hazard_replay(
            candidate_payload=replay.candidate.payload,
            event_mappings=[row.to_dict() for row in replay.events],
            eligible_session_days=replay.eligible_session_days,
            expected_hashes=drift,
        )


def test_adapts_filled_censor_without_fabricating_exit_or_trade() -> None:
    replay = _replay(rows=47)
    assert len(replay.events) == 1
    assert replay.events[0].outcome == CENSORED_FUTURE_COVERAGE
    assert replay.events[0].fill_time_ns is not None
    adapted = adapt_exact_hazard_replay(replay)
    assert adapted.signal_count == 1
    assert adapted.censored_signal_count == 1
    assert adapted.completed_trade_count == 0
    assert adapted.signals[0].fill_time_ns is not None
    assert adapted.signals[0].exit_fill_time_ns is None
    assert adapted.normal_events == ()
    assert adapted.normal_censored_trajectories == ()


def test_deep_seals_all_required_datasets_with_event_direction(
    tmp_path: Path,
) -> None:
    replay = _replay()
    policy = _policy(replay)
    policy_id = replay.candidate.candidate_id
    receipt = finalize_causal_target_velocity_evidence_bundle(
        base_dir=tmp_path / "payload",
        lightweight_manifest_path=tmp_path / "receipt.json",
        campaign_manifest=_manifest(),
        exact_replays={policy_id: replay},
        policies={policy_id: policy},
        evaluated_policy_records=_records(replay),
        data_fingerprints={"cached_feature_matrix:CL": "c" * 64},
        provenance={
            "access_ledger_sha256": "b" * 64,
            "recorded_at_utc": "2026-07-17T00:01:00Z",
            "market_data_role": "PRE_FREEZE_DEVELOPMENT_CACHE",
            "immutable_checksums": {"manifest": "e" * 64},
        },
    )
    manifest = verify_evidence_bundle(receipt.bundle_path, deep=True)
    assert set(manifest["dataset_row_counts"]) == set(REQUIRED_DATASETS)
    assert all(manifest["dataset_row_counts"][name] > 0 for name in REQUIRED_DATASETS)
    entries = list(iter_evidence_records(receipt.bundle_path, "component_entries"))
    trades = list(iter_evidence_records(receipt.bundle_path, "component_trades"))
    signals = list(iter_evidence_records(receipt.bundle_path, "component_signals"))
    assert entries[0]["side"] == "SHORT"
    assert trades[0]["side"] == "SHORT"
    assert signals[0]["signal"]["direction"] == -1
    assert signals[0]["raw_feature_values_embedded"] is False
    assert receipt.dataset_row_counts["episodes"] == 2


def test_seals_authoritative_reconstruction_with_explicit_provenance_flag(
    tmp_path: Path,
) -> None:
    replay = _replay()
    policy_id = replay.candidate.candidate_id
    receipt = finalize_causal_target_velocity_evidence_bundle(
        base_dir=tmp_path / "payload",
        lightweight_manifest_path=tmp_path / "receipt.json",
        campaign_manifest=_manifest(),
        exact_replays={policy_id: replay},
        policies={policy_id: _policy(replay)},
        evaluated_policy_records=_records(replay),
        data_fingerprints={"cached_feature_matrix:CL": "c" * 64},
        provenance={
            "access_ledger_sha256": "b" * 64,
            "recorded_at_utc": "2026-07-17T00:01:00Z",
            "market_data_role": "VIEWED_FINAL_DEVELOPMENT_RECONSTRUCTION",
            "immutable_checksums": {"manifest": "e" * 64},
        },
        evidence_status="AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION",
    )

    manifest = verify_evidence_bundle(receipt.bundle_path, deep=True)
    provenance = list(iter_evidence_records(receipt.bundle_path, "provenance"))
    summary_path = (
        Path(receipt.bundle_path)
        / manifest["compact_outputs"]["campaign_summary"]["relative_path"]
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert receipt.evidence_status == "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
    assert receipt.reconstruction_flag is True
    assert manifest["reconstruction_flag"] is True
    assert provenance[0]["reconstruction_flag"] is True
    assert summary["evidence_status"] == (
        "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
    )
    assert summary["reconstruction_flag"] is True
    assert summary["fresh_development_evidence"] is False


def test_releases_terminal_collections_before_finalize_and_uses_shallow_followups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    replay = _replay()
    policy = _policy(replay)
    policy_id = replay.candidate.candidate_id
    chronology: list[str] = []
    verification_modes: list[bool] = []
    guard_modes: list[bool] = []
    released_sizes: dict[str, int] = {}
    gc_calls = 0
    original_release = adapter_module._release_terminal_evidence_accumulators
    original_finalize = adapter_module.EvidenceBundleWriter.finalize
    original_verify = adapter_module.verify_evidence_bundle
    original_guard = adapter_module.guard_campaign_completion

    def counted_gc() -> int:
        nonlocal gc_calls
        gc_calls += 1
        return 0

    def release(**kwargs) -> None:
        original_release(**kwargs)
        released_sizes.update(
            {
                "rows": len(kwargs["accumulator"].rows),
                "seen_hashes": len(kwargs["seen_hashes"]),
                "scenario_coverage": len(kwargs["scenario_coverage"]),
                "observed_base_keys": len(kwargs["observed_base_keys"]),
            }
        )
        chronology.append("released")

    def finalize(writer, **kwargs):
        assert chronology == ["released"]
        chronology.append("finalize")
        return original_finalize(writer, **kwargs)

    def shallow_verify(bundle_path, *, deep: bool = True):
        verification_modes.append(deep)
        return original_verify(bundle_path, deep=deep)

    def shallow_guard(
        requested_status,
        bundle_path,
        *,
        campaign_id=None,
        deep: bool = True,
    ):
        guard_modes.append(deep)
        return original_guard(
            requested_status,
            bundle_path,
            campaign_id=campaign_id,
            deep=deep,
        )

    monkeypatch.setattr(adapter_module.gc, "collect", counted_gc)
    monkeypatch.setattr(
        adapter_module,
        "_release_terminal_evidence_accumulators",
        release,
    )
    monkeypatch.setattr(adapter_module.EvidenceBundleWriter, "finalize", finalize)
    monkeypatch.setattr(adapter_module, "verify_evidence_bundle", shallow_verify)
    monkeypatch.setattr(adapter_module, "guard_campaign_completion", shallow_guard)

    finalize_causal_target_velocity_evidence_bundle(
        base_dir=tmp_path / "payload",
        lightweight_manifest_path=tmp_path / "receipt.json",
        campaign_manifest=_manifest(),
        exact_replays={policy_id: replay},
        policies={policy_id: policy},
        evaluated_policy_records=_records(replay),
        data_fingerprints={"cached_feature_matrix:CL": "c" * 64},
        provenance={
            "access_ledger_sha256": "b" * 64,
            "recorded_at_utc": "2026-07-17T00:01:00Z",
            "market_data_role": "PRE_FREEZE_DEVELOPMENT_CACHE",
            "immutable_checksums": {"manifest": "e" * 64},
        },
    )

    assert chronology == ["released", "finalize"]
    assert released_sizes == {
        "rows": 0,
        "seen_hashes": 0,
        "scenario_coverage": 0,
        "observed_base_keys": 0,
    }
    assert gc_calls == 1
    assert verification_modes == [False]
    assert guard_modes == [False]


def test_refuses_summary_only_completion(tmp_path: Path) -> None:
    replay = _replay()
    policy_id = replay.candidate.candidate_id
    with pytest.raises(
        CausalTargetVelocityEvidenceError,
        match="summary-only completion is forbidden",
    ):
        finalize_causal_target_velocity_evidence_bundle(
            base_dir=tmp_path / "payload",
            lightweight_manifest_path=tmp_path / "receipt.json",
            campaign_manifest=_manifest(),
            exact_replays={policy_id: replay},
            policies={policy_id: _policy(replay)},
            evaluated_policy_records=[],
            data_fingerprints={"cached_feature_matrix:CL": "c" * 64},
            provenance={},
        )
