from __future__ import annotations

from copy import deepcopy
import hashlib
import sqlite3
from types import SimpleNamespace

import numpy as np
import pandas as pd

from hydra.research.intraday_rates_shock_cross_asset_tripwire import (
    CANONICAL_MECHANISM_CLASS,
    CANONICAL_SUBCLASSES,
    REQUIRED_ADJACENT_TOMBSTONE_REVIEWS,
    IntradayRatesShockTripwireError,
    _assert_no_cemetery_resurrection,
    _assert_no_structural_resurrection,
    _role_calendars,
    _split_starts_by_coverage,
    _summarize,
    _trade_path_event,
    _zero_safety_counters,
    build_rule_events,
    load_decision_card,
)
from hydra.production.fresh_confirmation_lane import non_overlapping_starts


def test_frozen_decision_card_is_tier_e_and_fail_closed() -> None:
    card = load_decision_card()
    experiment = card["smallest_decisive_falsification_experiment"]
    assert card["selected_branch"] == (
        "INTRADAY_RATES_SHOCK_CROSS_ASSET_REPRICING_TRIPWIRE_V1"
    )
    assert experiment["status_ceiling"] == "TIER_E_EXECUTABLE_DIAGNOSTIC"
    assert experiment["promotion_allowed"] is False
    assert experiment["q4_access_allowed"] is False
    assert experiment["data_purchase_allowed"] is False
    assert experiment["broker_allowed"] is False
    assert experiment["orders_allowed"] is False


def test_rule_event_uses_only_completed_source_bar_and_next_target_open() -> None:
    card = load_decision_card()
    experiment = deepcopy(card["smallest_decisive_falsification_experiment"])
    experiment.update(
        {
            "execution_markets": ["MNQ"],
            "source_lookback_minutes": [5],
            "mechanisms": ["DURATION_DIRECTION_CONTINUATION"],
            "holding_minutes": [15],
        }
    )
    timestamps = pd.date_range("2023-01-03T12:50:00Z", periods=80, freq="1min")
    source = pd.DataFrame(
        {
            "timestamp": timestamps,
            "contract": "ZNH3",
            "roll_segment_id": "ZN:segment:1",
            "return_5": 0.0,
            "prior_sigma_5": 0.001,
        }
    )
    source.loc[20, "return_5"] = 0.003
    target = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": "MNQ",
            "active_contract": "MNQH3",
            "open": np.arange(len(timestamps), dtype=float) + 10000.0,
            "high": np.arange(len(timestamps), dtype=float) + 10002.0,
            "low": np.arange(len(timestamps), dtype=float) + 9998.0,
            "close": np.arange(len(timestamps), dtype=float) + 10001.0,
            "return_5": 0.0001,
            "prior_sigma_5": 0.001,
        }
    )

    proposals, by_rule = build_rule_events(source, target, experiment=experiment)
    assert len(proposals) == 1
    events = by_rule[proposals[0]["candidate_id"]]
    assert len(events) == 1
    event = events[0]
    signal_bar = timestamps[20]
    assert event["available_at_ns"] == int((signal_bar + pd.Timedelta(minutes=1)).value)
    assert event["decision_ns"] == event["entry_ns"]
    assert event["entry_ns"] == int(timestamps[21].value)
    assert event["entry_price"] == float(target.iloc[21]["open"])
    assert event["exit_ns"] == int((timestamps[35] + pd.Timedelta(minutes=1)).value)
    assert event["side"] == 1


def test_direction_flip_control_preserves_clock_size_and_cost() -> None:
    row = {
        "event_id": "one",
        "decision_ns": 100,
        "exit_ns": 200,
        "session_day": 20240102,
        "gross_one_micro": 12.0,
        "favorable_one_micro": 20.0,
        "adverse_one_micro": -7.0,
        "normal_cost_one_micro": 3.0,
        "stressed_cost_one_micro": 4.5,
        "session_compliant": True,
    }
    primary = _trade_path_event(
        row, market="MNQ", quantity=4, scenario="STRESSED", direction_flip=False
    )
    control = _trade_path_event(
        row, market="MNQ", quantity=4, scenario="STRESSED", direction_flip=True
    )
    assert primary.decision_ns == control.decision_ns
    assert primary.exit_ns == control.exit_ns
    assert primary.quantity == control.quantity == 4
    assert primary.gross_pnl == -control.gross_pnl
    assert primary.net_pnl == (12.0 - 4.5) * 4
    assert control.net_pnl == (-12.0 - 4.5) * 4
    assert primary.mini_equivalent == control.mini_equivalent


def _coverage_fixture(*, missing_source_day: int | None = None):
    days = pd.bdate_range("2023-01-03", periods=25, tz="America/Chicago")
    target_parts = []
    source_parts = []
    for day_number, day in enumerate(days):
        target_clock = pd.date_range(
            day + pd.Timedelta(hours=6, minutes=59), periods=481, freq="1min"
        ).tz_convert("UTC")
        source_clock = target_clock[:421]
        if missing_source_day is not None and day_number == missing_source_day:
            source_clock = source_clock[:0]
        target_parts.append(
            pd.DataFrame({"timestamp": target_clock, "symbol": "MNQ"})
        )
        source_parts.append(
            pd.DataFrame({"timestamp": source_clock, "symbol": "ZN"})
        )
    raw = pd.concat(target_parts, ignore_index=True)
    source = pd.concat(source_parts, ignore_index=True)
    roll_day = int(days[10].strftime("%Y%m%d"))
    mapped = raw.loc[
        raw["timestamp"].dt.tz_convert("America/Chicago").dt.strftime("%Y%m%d").astype(int)
        != roll_day
    ].copy()
    roll_map = SimpleNamespace(
        unsafe_window_days=0,
        contracts=[SimpleNamespace(root="MNQ", roll_date=days[10].date().isoformat())],
    )
    experiment = {
        "source_market": "ZN",
        "execution_markets": ["MNQ"],
        "eligible_chicago_clock": ["07:00", "14:00"],
        "holding_minutes": [60],
        "horizons_trading_days": [5, 10, 20],
        "temporal_roles": {
            role: [days[0].date().isoformat(), (days[-1] + pd.Timedelta(days=1)).date().isoformat()]
            for role in ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
        },
    }
    return raw, mapped, source, roll_map, experiment, roll_day, days


def test_true_calendar_keeps_roll_day_as_zero_trade() -> None:
    raw, mapped, source, roll_map, experiment, roll_day, _days = _coverage_fixture()
    calendars, coverage = _role_calendars(
        raw, mapped, source, roll_map=roll_map, experiment=experiment
    )
    assert roll_day in calendars["DISCOVERY"]["MNQ"]
    audit = coverage["DISCOVERY"]["MNQ"]
    assert roll_day in audit["planned_roll_zero_trade_days"]
    assert roll_day not in audit["data_censored_days"]
    assert audit["true_session_count"] == 25


def test_unplanned_source_gap_censors_every_intersecting_start() -> None:
    raw, mapped, source, roll_map, experiment, _roll_day, days = _coverage_fixture(
        missing_source_day=2
    )
    calendars, coverage = _role_calendars(
        raw, mapped, source, roll_map=roll_map, experiment=experiment
    )
    calendar = calendars["DISCOVERY"]["MNQ"]
    missing_day = int(days[2].strftime("%Y%m%d"))
    assert missing_day in coverage["DISCOVERY"]["MNQ"]["data_censored_days"]
    starts = non_overlapping_starts(calendar, (5,))[5]
    full, censored = _split_starts_by_coverage(
        starts, calendar, horizon=5, censored_days=[missing_day]
    )
    assert len(starts) == 5
    assert len(censored) == 1
    assert len(full) == 4


def _sha(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_cemetery_class_and_canonical_fingerprint_collisions_fail_closed(tmp_path) -> None:
    database = tmp_path / "graveyard.db"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE class_tombstones (mechanism_class TEXT NOT NULL)"
    )
    connection.execute(
        "INSERT INTO class_tombstones(mechanism_class) VALUES(?)",
        (CANONICAL_MECHANISM_CLASS,),
    )
    connection.commit()
    connection.close()
    audit = {
        "graveyard_path": "graveyard.db",
        "graveyard_sha256_at_selection": _sha(database),
        "exact_mechanism_class_collision_count": 0,
        "mechanism_classes_checked": [CANONICAL_MECHANISM_CLASS, *CANONICAL_SUBCLASSES],
        "adjacent_tombstones_reviewed": sorted(REQUIRED_ADJACENT_TOMBSTONE_REVIEWS),
        "canonical_fingerprint_schema": "hydra_canonical_cross_asset_structure_v1",
        "forbidden_canonical_structural_fingerprints": [],
    }
    with np.testing.assert_raises(IntradayRatesShockTripwireError):
        _assert_no_cemetery_resurrection(tmp_path, {"cemetery_audit": audit})

    connection = sqlite3.connect(database)
    connection.execute("DELETE FROM class_tombstones")
    connection.commit()
    connection.close()
    audit["graveyard_sha256_at_selection"] = _sha(database)
    fingerprint = "a" * 64
    audit["forbidden_canonical_structural_fingerprints"] = [fingerprint]
    clean = _assert_no_cemetery_resurrection(tmp_path, {"cemetery_audit": audit})
    with np.testing.assert_raises(IntradayRatesShockTripwireError):
        _assert_no_structural_resurrection(
            {"cemetery_audit": audit},
            [{"canonical_structural_fingerprint": fingerprint}],
            clean,
        )


def test_safety_counters_and_empty_consistency_are_non_vacuous() -> None:
    assert _zero_safety_counters() == {
        "broker_connections": 0,
        "orders": 0,
        "tier_q_created": 0,
    }
    summary = _summarize([], data_censored_count=3)
    assert summary["episode_count"] == 0
    assert summary["data_censored_count"] == 3
    assert summary["all_passing_paths_consistency_compliant"] is False
