from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.research.soybean_crush_structural_value_router import (
    CONTROLS,
    LEGS,
    Candidate,
    SoybeanCrushRouterError,
    _align_session,
    _assert_continuous_membership,
    _assert_timestamp_bounds,
    _candidate_id,
    _candidates,
    _control_route,
    _controls_beaten,
    _feature_frame,
    _gate,
    _next_entry,
    _path,
    _prior_robust_z,
    _raw_contract,
    _simulate,
    _summary,
    _valid_triplet,
    audit_inputs,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "config/research/soybean_crush_structural_value_router_v1.json"


def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _leg_frame(leg: str, timestamps: list[str], prices: list[float]) -> pd.DataFrame:
    ts = pd.to_datetime(timestamps, utc=True)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "available_at": ts + pd.Timedelta(minutes=1),
            "instrument_key": [f"{leg}-1"] * len(ts),
            "raw_contract": [f"{leg}F4"] * len(ts),
            "triplet_key": ["2024:JAN"] * len(ts),
            "open": prices,
            "high": np.asarray(prices) + 1.0,
            "low": np.asarray(prices) - 1.0,
            "close": prices,
            "volume": [1] * len(ts),
        }
    )


def test_manifest_hash_and_frozen_lattice() -> None:
    payload = manifest()
    core = dict(payload)
    claimed = core.pop("manifest_hash")
    assert stable_hash(core) == claimed
    candidates = _candidates(payload)
    assert len(candidates) == 24
    assert len({_candidate_id(candidate, payload) for candidate in candidates}) == 24
    assert {candidate.execution_leg for candidate in candidates} == {"ZS", "ZM", "ZL"}


def test_explicit_triplet_map_fails_closed() -> None:
    payload = manifest()
    assert _raw_contract("ZSX4", "ZS") == ("NOV", 2004)
    valid, key = _valid_triplet({"ZS": "ZSX4", "ZM": "ZMV4", "ZL": "ZLV4"}, payload)
    assert valid and key == "2004:OCT"
    valid, key = _valid_triplet({"ZS": "ZSX4", "ZM": "ZMZ4", "ZL": "ZLZ4"}, payload)
    assert valid and key == "2004:DEC"
    assert _valid_triplet({"ZS": "ZSH4", "ZM": "ZMK4", "ZL": "ZLK4"}, payload) == (False, None)
    assert _valid_triplet(
        {"ZS": "ZSX2024", "ZM": "ZMV2024", "ZL": "ZLV2024"},
        payload,
        session_day=date(2024, 9, 1),
    ) == (True, "2024:OCT")
    assert _valid_triplet(
        {"ZS": "ZSX2004", "ZM": "ZMV2004", "ZL": "ZLV2004"},
        payload,
        session_day=date(2024, 9, 1),
    ) == (False, None)
    with pytest.raises(SoybeanCrushRouterError):
        _raw_contract("BAD", "ZS")


def test_alignment_uses_only_completed_backward_asof_bars() -> None:
    sessions = {
        "ZS": _leg_frame("ZS", ["2024-01-03T14:30:00Z", "2024-01-03T14:31:00Z"], [100.0, 101.0]),
        "ZM": _leg_frame("ZM", ["2024-01-03T14:30:00Z"], [200.0]),
        "ZL": _leg_frame("ZL", ["2024-01-03T14:29:00Z", "2024-01-03T14:31:00Z"], [50.0, 51.0]),
    }
    aligned = _align_session(sessions, maximum_staleness_minutes=2)
    at_1431 = aligned.loc[aligned["decision_time"].eq(pd.Timestamp("2024-01-03T14:31:00Z"))].iloc[0]
    assert at_1431["timestamp_ZS"] == pd.Timestamp("2024-01-03T14:30:00Z")
    assert at_1431["timestamp_ZM"] == pd.Timestamp("2024-01-03T14:30:00Z")
    assert at_1431["timestamp_ZL"] == pd.Timestamp("2024-01-03T14:29:00Z")
    assert all(
        aligned[f"timestamp_{leg}"].add(pd.Timedelta(minutes=1)).le(aligned["decision_time"]).all()
        for leg in ("ZS", "ZM", "ZL")
    )


def test_future_mutation_cannot_change_prior_robust_score() -> None:
    values = pd.Series(np.linspace(-2.0, 3.0, 100))
    original = _prior_robust_z(values, minimum=10, window=20)
    changed = values.copy()
    changed.iloc[80:] = 1_000_000.0
    replay = _prior_robust_z(changed, minimum=10, window=20)
    pd.testing.assert_series_equal(original.iloc[:80], replay.iloc[:80])


def test_frozen_input_time_bounds_are_fail_closed() -> None:
    _assert_timestamp_bounds(pd.Series(pd.to_datetime(["2018-01-02", "2024-09-30"], utc=True)))
    with pytest.raises(SoybeanCrushRouterError, match="pre-Q4"):
        _assert_timestamp_bounds(pd.Series(pd.to_datetime(["2024-10-01"], utc=True)))
    with pytest.raises(SoybeanCrushRouterError, match="pre-Q4"):
        _assert_timestamp_bounds(pd.Series(pd.to_datetime(["2018-01-01"], utc=True)))


def test_next_entry_is_strictly_after_decision_and_stress_replays_path() -> None:
    timestamp = pd.date_range("2024-01-03T15:00:00Z", periods=4, freq="1min")
    bars = pd.DataFrame(
        {
            "timestamp": timestamp,
            "instrument_key": ["ZS-1"] * 4,
            "open": [100.0, 100.0, 100.0, 100.0],
            "high": [100.5, 102.5, 103.0, 103.0],
            "low": [99.5, 99.5, 99.5, 99.5],
            "close": [100.0, 102.0, 102.5, 102.5],
        }
    )
    decision = pd.Timestamp("2024-01-03T15:00:00Z")
    assert _next_entry(bars, decision) == 1
    normal = _path(bars, 1, 1, 1.0, 1.5, 30, 0.25, 50.0, 0)
    stressed = _path(bars, 1, 1, 1.0, 1.5, 30, 0.25, 50.0, 1)
    assert normal is not None and stressed is not None
    assert normal["entry_price"] == 100.0
    assert stressed["entry_price"] == 100.25
    assert stressed["stop_price"] == 99.25
    assert stressed["target_price"] == 101.75
    assert stressed["gross_pnl_usd"] < normal["gross_pnl_usd"]


def test_overnight_entry_flattens_on_following_logical_session_day() -> None:
    timestamp = pd.date_range("2024-01-04T01:00:00Z", periods=3, freq="1min")
    bars = pd.DataFrame(
        {
            "timestamp": timestamp,
            "instrument_key": ["ZS-1"] * 3,
            "open": [100.0] * 3,
            "high": [100.25] * 3,
            "low": [99.75] * 3,
            "close": [100.0] * 3,
        }
    )
    replay = _path(bars, 0, 1, 1.0, 1.5, 30, 0.25, 50.0, 0)
    assert replay is not None
    assert replay["outcome_status"] == "CENSORED_FUTURE_COVERAGE"
    assert replay["exit_reason"] == "CENSORED_FUTURE_COVERAGE"
    assert replay["exit_time"] is None
    assert replay["exit_price"] is None
    assert replay["gross_pnl_usd"] is None


def test_complete_time_horizon_may_flatten_and_levels_are_executable_ticks() -> None:
    timestamp = pd.date_range("2024-01-03T15:00:00Z", periods=32, freq="1min")
    bars = pd.DataFrame(
        {
            "timestamp": timestamp,
            "instrument_key": ["ZS-1"] * len(timestamp),
            "open": [100.0] * len(timestamp),
            "high": [100.2] * len(timestamp),
            "low": [99.8] * len(timestamp),
            "close": [100.0] * len(timestamp),
        }
    )
    replay = _path(bars, 0, 1, 1.03, 1.5, 30, 0.25, 50.0, 0)
    assert replay is not None
    assert replay["outcome_status"] == "COMPLETED"
    assert replay["exit_reason"] == "TIME_OR_SESSION_FLATTEN"
    assert replay["stop_price"] == 98.75
    assert replay["target_price"] == 101.75
    assert replay["stop_price"] / 0.25 == round(replay["stop_price"] / 0.25)
    assert replay["target_price"] / 0.25 == round(replay["target_price"] / 0.25)


def test_feature_lookback_uses_elapsed_minutes_and_never_crosses_pause() -> None:
    first = pd.date_range("2024-01-03T13:00:00Z", periods=20, freq="1min")
    second = pd.date_range("2024-01-03T14:30:00Z", periods=90, freq="1min")
    decision = first.append(second)
    x = np.arange(len(decision), dtype=float)
    frame = pd.DataFrame({"decision_time": decision})
    for leg, base, scale in (("ZS", 1000.0, 1.0), ("ZM", 300.0, 0.3), ("ZL", 50.0, 0.02)):
        close = base + scale * x + np.sin(x / (3.0 + scale))
        frame[f"close_{leg}"] = close
        frame[f"high_{leg}"] = close + scale
        frame[f"low_{leg}"] = close - scale
    features = _feature_frame(
        frame,
        15,
        {
            "ZS": {"tick_size": 0.25},
            "ZM": {"tick_size": 0.10},
            "ZL": {"tick_size": 0.01},
        },
    )
    start_second = len(first)
    assert np.isnan(features.loc[start_second, "crush_change"])
    assert np.isnan(features.loc[start_second + 14, "crush_change"])
    assert np.isfinite(features.loc[start_second + 15, "crush_change"])
    tail = features.iloc[-20:]
    normalized = pd.DataFrame(
        {
            leg: tail[f"residual_{leg}"] / tail[f"residual_{leg}"].abs().max()
            for leg in LEGS
        }
    ).dropna()
    assert not normalized.empty
    assert normalized.nunique(axis=1).gt(1).any()


def test_continuous_symbology_must_match_each_new_data_bar() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["ZM.c.0", "ZM.c.0"],
            "timestamp": pd.to_datetime(
                ["2024-01-02T15:00:00Z", "2024-02-02T15:00:00Z"], utc=True
            ),
            "instrument_key": ["11", "22"],
        }
    )
    mapping = {
        "ZM.c.0": [
            (pd.Timestamp("2024-01-01", tz="UTC"), pd.Timestamp("2024-02-01", tz="UTC"), "11"),
            (pd.Timestamp("2024-02-01", tz="UTC"), pd.Timestamp("2024-03-01", tz="UTC"), "22"),
        ]
    }
    _assert_continuous_membership(frame, mapping)
    changed = frame.copy()
    changed.loc[1, "instrument_key"] = "11"
    with pytest.raises(SoybeanCrushRouterError, match="continuous symbology"):
        _assert_continuous_membership(changed, mapping)


def test_random_routing_is_causal_deterministic_and_routes_a_real_leg() -> None:
    event = {
        "candidate_id": "candidate",
        "session_day": "2024-01-03",
        "decision_time": "2024-01-03T15:00:00+00:00",
        "role": "VALIDATION",
        "executed_leg": "ZS",
        "direction": 1,
    }
    route = _control_route(event, {}, "SESSION_AND_EXPOSURE_MATCHED_RANDOM_ROUTING")
    assert route[0] in LEGS
    assert route[1] in {-1, 1}
    contaminated = {**event, "event_hash": "outcome-dependent", "stressed_net_usd": 1e9}
    assert _control_route(contaminated, {}, "SESSION_AND_EXPOSURE_MATCHED_RANDOM_ROUTING") == route


def test_minimum_open_pnl_stops_at_actual_stop_fill() -> None:
    timestamp = pd.date_range("2024-01-03T15:00:00Z", periods=2, freq="1min")
    bars = pd.DataFrame(
        {
            "timestamp": timestamp,
            "instrument_key": ["ZS-1"] * 2,
            "open": [100.0, 100.0],
            "high": [100.25, 100.25],
            "low": [90.0, 90.0],
            "close": [95.0, 95.0],
        }
    )
    replay = _path(bars, 0, 1, 1.0, 1.5, 30, 0.25, 50.0, 0)
    assert replay is not None
    assert replay["exit_reason"] == "STOP_FIRST"
    assert replay["exit_price"] == 99.0
    assert replay["minimum_open_pnl_usd"] == -50.0


def test_empty_or_undercovered_controls_never_pass() -> None:
    primary = {"event_count": 30, "stressed_net_per_event_usd": 10.0}
    controls = {
        name: {"event_count": 30, "stressed_net_per_event_usd": 0.0}
        for name in CONTROLS
    }
    assert _controls_beaten(primary, controls, 30) == (True, True)
    controls[CONTROLS[0]] = {"event_count": 29, "stressed_net_per_event_usd": -100.0}
    assert _controls_beaten(primary, controls, 30) == (False, False)
    controls.pop(CONTROLS[1])
    assert _controls_beaten(primary, controls, 30) == (False, False)


def test_event_provenance_and_fee_inclusive_50k_mll_are_persisted() -> None:
    payload = manifest()
    candidate = Candidate("ZS", "CRUSH_EXPANSION_CONTINUATION_ROUTER", 15, 1.5)
    decision = pd.Timestamp("2024-01-03T15:00:00Z")
    row: dict[str, object] = {
        "decision_time": decision,
        "crush_margin": 1.0,
        "crush_change": 0.5,
        "crush_score": 2.0,
    }
    for leg, price in (("ZS", 100.0), ("ZM", 300.0), ("ZL", 50.0)):
        row.update(
            {
                f"timestamp_{leg}": decision - pd.Timedelta(minutes=1),
                f"change_{leg}": 0.25,
                f"residual_{leg}": 0.1,
                f"residual_score_{leg}": 1.0,
                f"risk_{leg}": 50.0,
                f"triplet_key_{leg}": "2024:JAN",
                f"instrument_key_{leg}": f"{leg}-1",
                f"raw_contract_{leg}": f"{leg}F2024",
                f"close_{leg}": price,
            }
        )
    row["residual_score_ZM"] = np.nan
    session = pd.DataFrame(
        {
            "timestamp": [decision + pd.Timedelta(minutes=1)],
            "instrument_key": ["ZS-1"],
            "open": [100.0],
            "high": [100.25],
            "low": [0.0],
            "close": [50.0],
        }
    )
    event = _simulate(
        candidate,
        row,
        session,
        1,
        "DISCOVERY",
        date(2024, 1, 3),
        {"tick_size": 0.25, "tick_value_usd": 12.5, "point_value_usd": 50.0},
        payload,
        "PRIMARY",
    )
    assert event is not None
    assert event["feature_hash"] == stable_hash(event["causal_feature_values"])
    assert set(event["causal_feature_values"]["leg_context"]) == set(LEGS)
    assert event["causal_feature_values"]["leg_context"]["ZM"]["residual_score"] is None
    assert event["minimum_event_equity_stressed_usd"] < -2_000.0
    summary = _summary([event], reference_mll_usd=2_000.0)
    assert summary["event_level_50k_mll_breach_count"] == 1
    assert summary["minimum_event_equity_stressed_usd"] == event[
        "minimum_event_equity_stressed_usd"
    ]


def test_event_level_mll_breach_is_a_hard_selection_gate() -> None:
    payload = manifest()
    summary = {
        "event_count": 80,
        "independent_session_count": 80,
        "stressed_net_usd": 1_000.0,
        "stressed_edge_to_cost_ratio": 2.0,
        "maximum_single_trade_positive_profit_share": 0.1,
        "maximum_positive_day_profit_share": 0.1,
        "event_level_50k_mll_breach_count": 0,
    }
    assert _gate(summary, payload, discovery=True)
    assert not _gate(
        {**summary, "event_level_50k_mll_breach_count": 1},
        payload,
        discovery=True,
    )


def test_audit_waits_for_governed_receipt(tmp_path: Path) -> None:
    config = tmp_path / "config/research"
    config.mkdir(parents=True)
    payload = manifest()
    (config / MANIFEST_PATH.name).write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(SoybeanCrushRouterError, match="required input missing"):
        audit_inputs(tmp_path)
