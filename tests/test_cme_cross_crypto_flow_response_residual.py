from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.research.cme_cross_crypto_flow_response_residual import (
    Candidate,
    _candidate_id,
    _candidates,
    _simulate_signals,
    audit_inputs,
)
from scripts.acquire_cme_cross_crypto_flow_response_residual import (
    _read_manifest,
    _requests,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/research/cme_cross_crypto_flow_response_residual_v1.json"


def test_manifest_is_self_hashed_pre_q4_and_sequential() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    claimed = payload.pop("manifest_hash")
    assert stable_hash(payload) == claimed
    data = payload["data_contract"]
    assert data["q4_2024_access"] is False
    assert data["tranche_a"]["end_exclusive"] == "2024-01-01"
    assert data["tranche_b_conditional"]["end_exclusive"] == "2024-10-01"
    assert data["tranche_b_conditional"]["acquire_only_after_validation_gate"] is True


def test_tranche_a_requests_and_crypto_contract_cap_are_frozen() -> None:
    manifest = _read_manifest(ROOT)
    requests = _requests(manifest)
    assert set(requests) == {"tbbo", "definition"}
    assert all(row["symbols"] == ["MBT.c.0", "MET.c.0"] for row in requests.values())
    assert all(row["end"] == "2024-01-01" for row in requests.values())
    assert manifest["candidate_lattice"]["proposal_count"] == 24
    candidates = _candidates(manifest)
    assert len(candidates) == 24
    assert len({_candidate_id(candidate, manifest) for candidate in candidates}) == 24
    assert manifest["account_contract"]["maximum_contracts_by_account"] == {
        "50K": 5,
        "100K": 10,
        "150K": 15,
    }


def test_frozen_costs_reconcile_and_remain_under_authority() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    data = payload["data_contract"]
    for key in ("tranche_a", "tranche_b_conditional"):
        estimated = sum(
            row["estimated_cost_usd"] for row in data[key]["official_estimates"].values()
        )
        assert abs(estimated - data[key]["official_total_cost_usd"]) < 1e-12
    budget = payload["budget"]
    assert (
        budget["cumulative_actual_before_usd"]
        + data["tranche_a"]["official_total_cost_usd"]
        + data["tranche_b_conditional"]["official_total_cost_usd"]
        <= budget["authoritative_cumulative_cap_usd"]
    )


def test_receipt_raw_files_and_ledgers_reconcile() -> None:
    audit = audit_inputs(ROOT)
    assert audit["receipt"]["download_status"] == "DOWNLOADED"
    assert set(audit["files"]) == {"tbbo", "definition"}
    assert audit["receipt"]["q4_access_count_delta"] == 0


def test_simulator_skips_equal_timestamp_and_enters_strictly_later() -> None:
    manifest = _read_manifest(ROOT)
    times = pd.to_datetime(
        [
            "2023-06-01T14:00:00Z",
            "2023-06-01T14:00:00Z",
            "2023-06-01T14:00:01Z",
            "2023-06-01T14:00:02Z",
            "2023-06-01T14:00:03Z",
        ],
        utc=True,
    )
    frame = pd.DataFrame(
        {
            "ts_recv": times,
            "session_day": [pd.Timestamp("2023-06-01", tz="America/Chicago")] * 5,
            "instrument_id": [7] * 5,
            "bid": [99.0, 99.0, 100.0, 108.0, 108.0],
            "ask": [100.0, 100.0, 101.0, 109.0, 109.0],
            "bid_size": [2.0] * 5,
            "ask_size": [2.0] * 5,
            "prior_range": [4.0] * 5,
            "role": ["VALIDATION"] * 5,
        }
    )
    candidate = Candidate("MBT", "CROSS_FLOW_CONFIRMATION_CONTINUATION", 50, 5)
    events = _simulate_signals(
        frame,
        candidate,
        np.asarray([0], dtype=np.int64),
        np.asarray([1], dtype=np.int8),
        {"tick_size": 1.0, "point_value_usd": 1.0, "tick_value_usd": 1.0},
        manifest,
        control="PRIMARY",
    )
    assert len(events) == 1
    assert pd.Timestamp(events[0]["entry_time"]) == times[2]
    assert pd.Timestamp(events[0]["entry_time"]) > pd.Timestamp(events[0]["decision_time"])
    assert events[0]["exit_reason"] == "TARGET"
    assert events[0]["candidate_id"] == _candidate_id(candidate, manifest)
    assert events[0]["event_window_trade_count"] == 50
    assert events[0]["holding_minutes"] == 5
    assert len(events[0]["event_hash"]) == 64


def test_missing_strictly_later_entry_is_persisted_as_censored() -> None:
    manifest = _read_manifest(ROOT)
    timestamp = pd.Timestamp("2023-06-01T14:00:00Z")
    frame = pd.DataFrame(
        {
            "ts_recv": [timestamp],
            "session_day": [pd.Timestamp("2023-06-01", tz="America/Chicago")],
            "instrument_id": [7],
            "bid": [99.0],
            "ask": [100.0],
            "bid_size": [2.0],
            "ask_size": [2.0],
            "prior_range": [4.0],
            "role": ["VALIDATION"],
        }
    )
    candidate = Candidate("MBT", "CROSS_FLOW_CONFIRMATION_CONTINUATION", 50, 5)
    events = _simulate_signals(
        frame,
        candidate,
        np.asarray([0], dtype=np.int64),
        np.asarray([1], dtype=np.int8),
        {"tick_size": 1.0, "point_value_usd": 1.0, "tick_value_usd": 1.0},
        manifest,
        control="PRIMARY",
    )
    assert len(events) == 1
    assert events[0]["outcome_state"] == "DATA_CENSORED"
    assert events[0]["censor_reason"] == "MISSING_STRICTLY_LATER_ENTRY"
    assert len(events[0]["event_hash"]) == 64
