from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.economic_evolution.schema import stable_hash
from hydra.research.cftc_grain_positioning_crowding import (
    CHICAGO,
    Candidate,
    _candidates,
    _controls_beaten,
    _cot_direction,
    _load_cot,
    _rolling_percentile,
    _session_context,
    _simulate_session,
    audit_inputs,
)
from scripts.acquire_cftc_grain_positioning_crowding import _query, _read_manifest, acquire


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/research/cftc_grain_positioning_crowding_tripwire_v1.json"


def test_manifest_is_self_hashed_and_pre_q4() -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    claimed = payload.pop("manifest_hash")
    assert stable_hash(payload) == claimed
    assert payload["candidate_lattice"]["proposal_count"] == 48
    assert payload["cftc_data_contract"]["q4_2024_access"] is False
    assert payload["frozen_price_input"]["q4_2024_access"] is False
    assert payload["governance"]["new_databento_spend_usd"] == 0.0


def test_query_is_futures_only_and_exactly_three_grain_markets() -> None:
    manifest = _read_manifest(ROOT)
    _url, params = _query(manifest)
    where = params["$where"]
    assert "futonly_or_combined='FutOnly'" in where
    assert all(code in where for code in ("001602", "002602", "005602"))
    assert "2024-09-24T23:59:59.999" in where
    assert params["$limit"] == "5000"


def test_frozen_price_inputs_reconcile() -> None:
    manifest = _read_manifest(ROOT)
    price = manifest["frozen_price_input"]
    assert sha256_file(ROOT / price["ohlcv_path"]) == price["ohlcv_sha256"]
    assert sha256_file(ROOT / price["definition_path"]) == price["definition_sha256"]


def test_publication_delay_is_conservative_and_shutdown_is_excluded() -> None:
    manifest = _read_manifest(ROOT)
    causal = manifest["publication_causality"]
    assert causal["conservative_actionable_time"].startswith("REPORT_DATE_PLUS_8")
    assert causal["available_at_must_be_lte_decision_time"] is True
    assert causal["future_label_eligibility"] is False
    assert causal["excluded_disrupted_report_dates"] == {
        "start": "2018-12-18",
        "end_inclusive": "2019-03-05",
        "reason": (
            "2018-2019 federal shutdown publication backlog; exact historical "
            "release timestamps are unavailable."
        ),
    }
    cot, _receipt = _load_cot(audit_inputs(ROOT))
    first = cot["ZC"].iloc[0]
    available_local = pd.Timestamp(first["available_at"]).tz_convert("America/Chicago")
    assert (available_local.date() - pd.Timestamp(first["report_date"]).date()).days == 8
    assert (available_local.hour, available_local.minute) == (8, 30)


def test_acquisition_receipt_has_zero_spend_and_no_execution_side_effects() -> None:
    result = acquire(ROOT, execute=False)
    assert result["download_status"] == "DOWNLOADED"
    assert result["actual_cost_usd"] == 0.0
    assert result["q4_access_count_delta"] == 0
    assert result["broker_connections"] == 0
    assert result["orders"] == 0
    assert result["audit"]["row_count"] == 1056
    audit_inputs(ROOT)


def test_candidate_lattice_is_unique_and_complete() -> None:
    manifest = _read_manifest(ROOT)
    candidates = _candidates(manifest)
    assert len(candidates) == 48
    assert len(set(candidates)) == 48


def test_rolling_percentile_is_strictly_prior_only() -> None:
    base = pd.Series(np.arange(40, dtype=float))
    changed = base.copy()
    changed.iloc[30:] = -999.0
    original = _rolling_percentile(base, 52, 5)
    perturbed = _rolling_percentile(changed, 52, 5)
    pd.testing.assert_series_equal(original.iloc[:30], perturbed.iloc[:30])
    assert original.iloc[5] == 1.0


def test_cot_direction_contract_is_bounded() -> None:
    row = {
        "managed_percentile": 0.95,
        "managed_change": -0.01,
        "divergence_percentile": 0.97,
        "managed_change_abs_percentile": 0.93,
    }
    assert _cot_direction(row, Candidate("ZC", "CROWDING_CONTINUATION", 0.9, 1.5)) == 1
    assert _cot_direction(row, Candidate("ZC", "CROWDING_UNWIND", 0.9, 1.5)) == -1
    assert _cot_direction(row, Candidate("ZC", "PRODUCER_MANAGED_DIVERGENCE", 0.9, 1.5)) == -1
    assert _cot_direction(row, Candidate("ZC", "POSITION_CHANGE_MOMENTUM", 0.9, 1.5)) == -1


def test_session_entry_is_strictly_after_decision_and_same_bar_is_stop_first() -> None:
    manifest = _read_manifest(ROOT)
    local = pd.date_range("2023-06-07 08:30", "2023-06-07 13:19", freq="1min", tz="America/Chicago")
    times = local.tz_convert("UTC")
    size = len(times)
    frame = pd.DataFrame(
        {
            "ts_event": times,
            "instrument_id": [7] * size,
            "open": [100.0] * size,
            "high": [101.0] * size,
            "low": [99.5] * size,
            "close": [100.5] * size,
        }
    )
    entry_row = int(np.flatnonzero((local.hour == 8) & (local.minute == 46))[0])
    frame.loc[entry_row, ["open", "high", "low", "close"]] = [101.0, 110.0, 90.0, 101.0]
    context = _session_context(frame)
    assert context is not None
    assert context["decision_time"] == pd.Timestamp("2023-06-07 13:45:00Z")
    assert context["entry_time"] > context["decision_time"]
    context.update(
        {
            "role": "FINAL_DEVELOPMENT",
            "session_day": pd.Timestamp("2023-06-07", tz="America/Chicago"),
            "cot_report_date": pd.Timestamp("2023-05-30", tz="UTC"),
            "cot_available_at": pd.Timestamp("2023-06-07 13:30", tz="UTC"),
        }
    )
    result = _simulate_session(
        frame,
        context,
        1,
        Candidate("ZC", "CROWDING_CONTINUATION", 0.9, 1.5),
        {"tick_size": 0.25, "point_value_usd": 50.0, "tick_value_usd": 12.5},
        manifest,
        control="PRIMARY",
    )
    assert result["exit_reason"] == "STOP_FIRST"
    assert result["entry_time"] > result["decision_time"]
    assert result["exit_time"] == pd.Timestamp("2023-06-07 13:47:00Z").isoformat()
    assert result["stressed_entry_price"] == result["entry_price"] + 0.25
    assert result["stressed_stop_price"] == result["stop_price"] + 0.25
    assert result["minimum_open_pnl_stressed_usd"] > -100.0


def test_session_context_fails_closed_on_missing_opening_bar_or_mixed_contract() -> None:
    local = pd.date_range("2023-06-07 08:30", "2023-06-07 13:19", freq="1min", tz=CHICAGO)
    frame = pd.DataFrame(
        {
            "ts_event": local.tz_convert("UTC"),
            "instrument_id": [7] * len(local),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
        }
    )
    missing = frame.loc[local.strftime("%H:%M") != "08:44"].reset_index(drop=True)
    assert _session_context(missing) is None
    mixed = frame.copy()
    mixed.loc[mixed.index[-1], "instrument_id"] = 8
    assert _session_context(mixed) is None


def test_undercovered_or_empty_control_is_never_beaten() -> None:
    primary = {"event_count": 30, "stressed_net_per_event_usd": 10.0}
    empty = {
        "empty": {
            "roles": {
                "VALIDATION": {"event_count": 0, "stressed_net_per_event_usd": None}
            }
        }
    }
    assert _controls_beaten(primary, empty, "VALIDATION", 30) == (False, False)
    covered = {
        "control": {
            "roles": {
                "VALIDATION": {"event_count": 30, "stressed_net_per_event_usd": 5.0}
            }
        }
    }
    assert _controls_beaten(primary, covered, "VALIDATION", 30) == (True, True)
