from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from hydra.research.cross_asset_volatility_convexity_tripwire import (
    _match_control_events,
    build_source_composite,
    build_true_session_calendars,
    load_decision_card,
    materialize_frozen_oco_event,
)


def _mechanism(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "oco_lookback_minutes": 15,
        "oco_valid_minutes": 3,
        "stop_range_fraction": 0.5,
        "minimum_stop_ticks": 1,
        "target_r_multiple": 1.0,
        "maximum_holding_minutes": 2,
    }
    value.update(overrides)
    return value


def _causal() -> dict[str, object]:
    return {
        "normal_all_in_cost_per_micro_usd": {"MNQ": 3.0},
        "stressed_all_in_cost_per_micro_usd": {"MNQ": 4.5},
    }


def _proposal() -> dict[str, str]:
    return {
        "candidate_id": "synthetic",
        "execution_market": "MNQ",
        "session_role": "OPEN",
    }


def _row(timestamp: pd.Timestamp) -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=timestamp,
        target_index=0,
        range30=1.0,
        oco_high_15=100.0,
        oco_low_15=90.0,
        active_contract="MNQH4",
        local_minute=540,
        decision_source_score=2.5,
        target_vol_z=-0.5,
        contract_zn="ZNH4",
        contract_tn="TNH4",
    )


def _target(bars: list[tuple[float, float, float]]) -> pd.DataFrame:
    timestamp = pd.date_range("2024-01-02T15:00:00Z", periods=len(bars), freq="1min")
    return pd.DataFrame(
        {
            "timestamp": timestamp,
            "open": [bar[0] for bar in bars],
            "high": [bar[1] for bar in bars],
            "low": [bar[2] for bar in bars],
            "active_contract": "MNQH4",
            "unsafe_roll_window": False,
        }
    )


def test_frozen_card_has_tier_e_ceiling_and_no_external_side_effects() -> None:
    card = load_decision_card()
    governance = card["governance"]
    assert governance["status_ceiling"] == "TIER_E_EXECUTABLE_DIAGNOSTIC"
    assert governance["promotion_allowed"] is False
    assert governance["tier_q_allowed"] is False
    assert governance["q4_access_allowed"] is False
    assert governance["data_purchase_allowed"] is False
    assert governance["maximum_cpu_workers"] == 1


def test_oco_cannot_fill_on_completed_decision_bar() -> None:
    target = _target(
        [
            (99.0, 105.0, 95.0),  # completed decision bar would have crossed
            (99.0, 100.0, 95.0),
            (100.0, 101.0, 99.5),
            (100.0, 100.0, 99.0),
        ]
    )
    event, status = materialize_frozen_oco_event(
        _row(target.iloc[0]["timestamp"]),
        target,
        proposal=_proposal(),
        mechanism=_mechanism(maximum_holding_minutes=1),
        causal=_causal(),
        control="PRIMARY",
    )
    assert status == "TRADE_CREATED"
    assert event is not None
    assert event["entry_ns"] == int(target.iloc[2]["timestamp"].value)
    assert event["entry_ns"] > int(target.iloc[0]["timestamp"].value)


def test_entry_double_touch_abstains() -> None:
    target = _target(
        [
            (95.0, 100.0, 90.0),
            (95.0, 101.0, 89.0),
            (95.0, 100.0, 90.0),
        ]
    )
    event, status = materialize_frozen_oco_event(
        _row(target.iloc[0]["timestamp"]),
        target,
        proposal=_proposal(),
        mechanism=_mechanism(),
        causal=_causal(),
        control="PRIMARY",
    )
    assert event is None
    assert status == "AMBIGUOUS_BOTH_TOUCH_ABSTAIN"


def test_exit_double_touch_is_stop_first() -> None:
    target = _target(
        [
            (95.0, 100.0, 90.0),
            (100.0, 101.0, 99.5),
            (100.0, 100.0, 99.0),
        ]
    )
    event, status = materialize_frozen_oco_event(
        _row(target.iloc[0]["timestamp"]),
        target,
        proposal=_proposal(),
        mechanism=_mechanism(),
        causal=_causal(),
        control="PRIMARY",
    )
    assert status == "TRADE_CREATED"
    assert event is not None
    assert event["exit_reason"] == "STOP_FIRST"
    assert event["same_bar_exit_stop_first"] is True
    assert event["exit_price"] == event["frozen_oco_levels"]["buy_stop"]


def test_missing_interval_is_data_censored_and_roll_is_zero_trade() -> None:
    target = _target(
        [
            (95.0, 100.0, 90.0),
            (100.0, 101.0, 99.5),
            (100.0, 100.0, 99.0),
        ]
    )
    target.loc[1, "timestamp"] += pd.Timedelta(minutes=1)
    event, status = materialize_frozen_oco_event(
        _row(target.iloc[0]["timestamp"]),
        target,
        proposal=_proposal(),
        mechanism=_mechanism(),
        causal=_causal(),
        control="PRIMARY",
    )
    assert event is None
    assert status == "DATA_CENSORED"

    target = _target([(95.0, 100.0, 90.0), (100.0, 101.0, 99.5)])
    target.loc[1, "unsafe_roll_window"] = True
    event, status = materialize_frozen_oco_event(
        _row(target.iloc[0]["timestamp"]),
        target,
        proposal=_proposal(),
        mechanism=_mechanism(),
        causal=_causal(),
        control="PRIMARY",
    )
    assert event is None
    assert status == "ROLL_UNSAFE_ZERO_TRADE"


def test_source_volatility_is_exactly_sign_flip_invariant() -> None:
    sessions = pd.bdate_range("2023-01-03", periods=26)
    frames: dict[str, pd.DataFrame] = {}
    for market, base in (("ZN", 110.0), ("TN", 112.0)):
        rows: list[dict[str, object]] = []
        for session_number, day in enumerate(sessions):
            start = pd.Timestamp(day).tz_localize("UTC") + pd.Timedelta(hours=15)
            for minute in range(20):
                rows.append(
                    {
                        "timestamp": start + pd.Timedelta(minutes=minute),
                        "contract": f"{market}H3",
                        "roll_segment_id": f"{market}:one",
                        "close": base
                        * np.exp((session_number + 1) * (minute**2) * 1e-7),
                    }
                )
        frames[market] = pd.DataFrame(rows)
    source, audit = build_source_composite(
        frames, prior_sessions=20, rv_minutes=15
    )
    assert len(source) > 0
    assert audit["passed"] is True
    assert audit["original_feature_hash"] == audit["source_sign_flipped_feature_hash"]
    assert audit["decision_source_direction_fields"] == []


def test_true_session_calendar_retains_roll_and_censors_gaps() -> None:
    timestamps: list[pd.Timestamp] = []
    symbols: list[str] = []
    for day, count in (("2024-01-01", 5), ("2024-01-02", 5), ("2024-01-03", 3)):
        start = pd.Timestamp(f"{day}T13:00:00Z")
        timestamps.extend(start + pd.Timedelta(minutes=value) for value in range(count))
        symbols.extend(["MNQ"] * count)
    raw_targets = pd.DataFrame({"timestamp": timestamps, "symbol": symbols})
    source_rows: list[dict[str, object]] = []
    for day in (20240101, 20240102, 20240103):
        start = pd.Timestamp(str(day), tz="UTC") + pd.Timedelta(hours=13)
        for value in range(5):
            timestamp = start + pd.Timedelta(minutes=value)
            source_rows.append(
                {
                    "timestamp": timestamp,
                    "session_day": day,
                    "local_minute": 420 + value,
                }
            )
    source = pd.DataFrame(source_rows)
    roll_map = SimpleNamespace(
        unsafe_window_days=0,
        contracts=[SimpleNamespace(root="MNQ", roll_date="2024-01-02")],
    )
    card = {
        "causal_contract": {
            "minimum_observed_minutes_for_complete_session": 4,
            "calendar_inventory_window_chicago": ["07:00", "07:05"],
            "execution_markets": ["MNQ"],
        },
        "chronological_roles": {
            role: ["2024-01-01", "2024-01-04"]
            for role in ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
        },
    }
    calendars, audit = build_true_session_calendars(
        raw_targets,
        source,
        roll_map=roll_map,
        source_roll_days=set(),
        card=card,
    )
    assert calendars["DISCOVERY"]["MNQ"] == (20240101, 20240102, 20240103)
    result = audit["DISCOVERY"]["MNQ"]
    assert result["roll_unsafe_zero_trade_days"] == [20240102]
    assert result["data_censored_days"] == [20240103]


def test_controls_never_reuse_the_primary_decision() -> None:
    primary = (
        {
            "event_id": "primary",
            "block": "B1",
            "session_role": "OPEN",
            "side": 1,
            "local_minute": 500,
            "session_day": 20240102,
            "decision_ns": 100,
        },
    )
    pool = (
        {**primary[0], "event_id": "same-decision"},
        {
            **primary[0],
            "event_id": "matched-placebo",
            "session_day": 20240103,
            "decision_ns": 200,
        },
    )
    matched = _match_control_events(primary, pool, used=set())
    assert [row["event_id"] for row in matched] == ["matched-placebo"]
