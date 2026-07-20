from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

import hydra.research.v73_extreme_recovery_speed_tripwire as v73
from hydra.economic_evolution.schema import stable_hash


def _day(
    date: str,
    *,
    breach: bool,
    both_touch: bool = False,
    minutes: int = 90,
) -> pd.DataFrame:
    start = pd.Timestamp(f"{date}T13:30:00Z")
    rows: list[dict[str, object]] = []
    for position in range(minutes):
        timestamp = start + pd.Timedelta(minutes=position)
        rows.append(
            {
                "product": "ES",
                "contract": "ESU4",
                "calendar_year": 2024,
                "minute_start_ns": int(timestamp.value),
                "source_close_ns": int((timestamp + pd.Timedelta(minutes=1)).value),
                "availability_ns": int((timestamp + pd.Timedelta(minutes=1)).value),
                "first_trade_ns": int((timestamp + pd.Timedelta(seconds=1)).value),
                "open": 100.0,
                "high": 100.5,
                "low": 99.5,
                "close": 100.0,
                "total_volume": 1000.0,
                "signed_aggressor_volume": 100.0,
            }
        )
    if breach and minutes >= 62:
        rows[60].update(
            {"open": 100.25, "high": 102.0, "low": 100.0, "close": 101.5,
             "signed_aggressor_volume": 800.0}
        )
        rows[61].update(
            {"open": 101.5, "high": 101.6, "low": 100.0, "close": 100.25,
             "signed_aggressor_volume": 200.0}
        )
        if minutes >= 63:
            rows[62].update(
                {
                    "open": 100.25,
                    "high": 102.5 if both_touch else 100.5,
                    "low": 95.0 if both_touch else 99.5,
                    "close": 100.0,
                }
            )
    return pd.DataFrame(rows)


def _card() -> dict[str, object]:
    return v73.load_decision_card()


def test_card_is_self_hashed_and_has_hard_tier_ceiling() -> None:
    card = _card()
    core = dict(card)
    claimed = core.pop("card_hash")
    assert stable_hash(core) == claimed == v73.EXPECTED_CARD_HASH
    assert card["governance"]["tier_q_allowed"] is False
    assert card["governance"]["evidence_ceiling"] == "TIER_E_EXECUTABLE_DIAGNOSTIC"
    assert card["governance"]["tier_g_allowed"] is False
    assert card["governance"]["tier_c_allowed"] is False
    assert card["governance"]["q4_access_allowed"] is False
    assert tuple(row["id"] for row in card["controls"]) == v73.CONTROLS


def test_rehashed_tampered_card_still_fails_constant_binding(tmp_path: Path) -> None:
    card = _card()
    card["hypothesis"] = "tampered"
    core = dict(card)
    core.pop("card_hash")
    card["card_hash"] = stable_hash(core)
    path = tmp_path / "card.json"
    path.write_text(json.dumps(card), encoding="utf-8")
    try:
        v73.load_decision_card(path)
    except v73.V73TripwireError as exc:
        assert "hash drift" in str(exc)
    else:
        raise AssertionError("rehash plus content mutation must fail")


def test_audit_only_never_decodes_parquet_or_writes(monkeypatch) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("audit-only decoded parquet")

    monkeypatch.setattr(pd, "read_parquet", forbidden)
    result = v73.audit_only(Path("."))
    assert result["status"] == "AUDIT_ONLY_GREEN_READY_FOR_EXPLICIT_ECONOMIC_REPLAY"
    assert result["pre_q4_manifest_proof"]["passed"] is True
    assert result["parquet_files_decoded"] == 0
    assert result["economic_rows_decoded"] == 0
    assert result["outcomes_read"] == 0
    assert result["authoritative_mission_writes"] == 0
    assert result["economic_replay_started"] is False


def test_signal_waits_for_recovery_close_and_fill_waits_for_next_trade() -> None:
    minute = _day("2024-08-01", breach=True)
    signals = v73.generate_recovery_signals(
        minute,
        candidate_id="synthetic",
        holding_minutes=15,
        card=_card(),
    )
    assert signals
    signal = signals[0]
    recovery = minute.iloc[61]
    assert signal["decision_ns"] == recovery["source_close_ns"]
    events, counts = v73.materialize_recovery_trades(
        minute, signals[:1], holding_minutes=15, card=_card()
    )
    assert counts["TRADE_CREATED"] == 1
    event = events[0]
    assert event["entry_ns"] == minute.iloc[62]["first_trade_ns"]
    assert event["entry_ns"] > signal["decision_ns"]
    assert event["side"] == -1


def test_recovery_signal_is_append_invariant_and_not_retroactively_suppressed() -> None:
    short = _day("2024-08-01", breach=True, minutes=62)
    long = _day("2024-08-01", breach=True, minutes=90)
    short_signals = v73.generate_recovery_signals(
        short, candidate_id="synthetic", holding_minutes=15, card=_card()
    )
    long_signals = v73.generate_recovery_signals(
        long, candidate_id="synthetic", holding_minutes=15, card=_card()
    )
    assert short_signals
    assert short_signals[0] == long_signals[0]
    events, counts = v73.materialize_recovery_trades(
        short, short_signals, holding_minutes=15, card=_card()
    )
    assert not events
    assert counts["CENSORED_FUTURE_COVERAGE"] == 1


def test_prior_window_cannot_cross_chicago_session() -> None:
    prior = _day("2024-08-01", breach=False, minutes=59)
    following = _day("2024-08-02", breach=True, minutes=62).iloc[59:].copy()
    minute = pd.concat([prior, following], ignore_index=True)
    signals = v73.generate_recovery_signals(
        minute, candidate_id="synthetic", holding_minutes=15, card=_card()
    )
    assert signals == ()


def test_same_bar_stop_and_target_is_stop_first() -> None:
    minute = _day("2024-08-01", breach=True, both_touch=True)
    signals = v73.generate_recovery_signals(
        minute, candidate_id="synthetic", holding_minutes=15, card=_card()
    )
    events, _counts = v73.materialize_recovery_trades(
        minute, signals[:1], holding_minutes=15, card=_card()
    )
    assert events[0]["exit_reason"] == "STOP_FIRST"
    assert events[0]["same_bar_exit_stop_first"] is True
    assert events[0]["exit_price"] == events[0]["stop_price"]


def test_controls_are_deterministic_and_preserve_frozen_geometry() -> None:
    minute = pd.concat(
        [
            _day("2024-08-02", breach=True),
            _day("2024-08-03", breach=False),
        ],
        ignore_index=True,
    )
    signals = v73.generate_recovery_signals(
        minute, candidate_id="synthetic", holding_minutes=15, card=_card()
    )
    primary, _ = v73.materialize_recovery_trades(
        minute, signals[:1], holding_minutes=15, card=_card()
    )
    flipped, _ = v73.build_direction_flip_control(
        minute, primary, holding_minutes=15, card=_card()
    )
    assert len(flipped) == len(primary) == 1
    assert flipped[0]["entry_ns"] == primary[0]["entry_ns"]
    assert flipped[0]["side"] == -primary[0]["side"]
    assert flipped[0]["risk_points"] == primary[0]["risk_points"]

    first, _ = v73.build_session_timing_control(
        minute, primary, holding_minutes=15, card=_card()
    )
    second, _ = v73.build_session_timing_control(
        minute, primary, holding_minutes=15, card=_card()
    )
    assert first == second
    assert len(first) == 1
    source_clock = pd.Timestamp(primary[0]["entry_ns"], unit="ns", tz="UTC").tz_convert(
        "America/Chicago"
    ).strftime("%H:%M")
    target_clock = pd.Timestamp(first[0]["entry_ns"], unit="ns", tz="UTC").tz_convert(
        "America/Chicago"
    ).strftime("%H:%M")
    assert source_clock == target_clock


def test_direction_flip_control_suppresses_its_own_overlaps() -> None:
    minute = _day("2024-08-02", breach=True)
    # The primary short stops immediately, while its flipped long survives to
    # the time exit.  A later source event must therefore be suppressed by the
    # control's own path, not by the primary path.
    minute.loc[62, "high"] = 102.5
    signals = v73.generate_recovery_signals(
        minute, candidate_id="synthetic", holding_minutes=15, card=_card()
    )
    primary, _ = v73.materialize_recovery_trades(
        minute, signals[:1], holding_minutes=15, card=_card()
    )
    assert primary[0]["exit_reason"] == "STOP"
    later = dict(primary[0])
    later["event_id"] = "synthetic:later"
    later["decision_ns"] = int(minute.iloc[65]["minute_start_ns"])
    later["entry_minute_start_ns"] = int(minute.iloc[65]["minute_start_ns"])
    later["entry_ns"] = int(minute.iloc[65]["first_trade_ns"])
    flipped, counts = v73.build_direction_flip_control(
        minute, (primary[0], later), holding_minutes=15, card=_card()
    )
    assert len(flipped) == 1
    assert counts["DIRECTION_FLIP_OVERLAP_SUPPRESSED"] == 1


def test_underpowered_branch_cannot_manufacture_tier_q() -> None:
    result = v73._branch_gate([], {"passed": False}, _card())
    assert result["passed"] is False
    assert result["tier_q_candidate_ids"] == []
    assert result["tier_g_or_c_claimed"] is False
    assert result["status"] == _card()["power_preflight"]["when_underpowered"]


def test_decision_maxes_entire_feature_window_and_entry_is_strictly_later() -> None:
    minute = _day("2024-08-02", breach=True)
    late = int(minute.iloc[61]["source_close_ns"]) + 30_000_000_000
    minute.loc[10, "availability_ns"] = late
    signals = v73.generate_recovery_signals(
        minute, candidate_id="synthetic", holding_minutes=15, card=_card()
    )
    assert signals[0]["decision_ns"] == late
    events, counts = v73.materialize_recovery_trades(
        minute, signals[:1], holding_minutes=15, card=_card()
    )
    assert counts["TRADE_CREATED"] == 1
    assert events[0]["entry_ns"] > late
    assert events[0]["entry_minute_start_ns"] == minute.iloc[63]["minute_start_ns"]


def test_role_calendar_keeps_exact_full_and_censored_denominators() -> None:
    full = _day("2024-08-02", breach=False, minutes=400)
    incomplete = _day("2024-08-03", breach=False, minutes=399)
    calendars, coverage = v73.build_role_calendars(
        pd.concat([full, incomplete], ignore_index=True), card=_card()
    )
    assert calendars["VALIDATION"]["ES"] == (20240802, 20240803)
    cell = coverage["VALIDATION"]["ES"]
    assert cell["full_coverage_days"] == [20240802]
    assert cell["data_censored_days"] == [20240803]
    assert cell["exact_required_minutes"] == 400
    assert cell["preregistered_calendar_day_count"] == 2


def test_static_parent_is_exact_fixed_holding_event_reference() -> None:
    minute = _day("2024-08-02", breach=False)
    frame = v73._prepare_minutes(minute, card=_card())
    lookup = v73._minute_lookup(frame)
    decision = int(minute.iloc[60]["source_close_ns"])
    signal = {
        "signal_id": "parent",
        "candidate_id": "parent",
        "contract": "ESU4",
        "side": 1,
        "decision_ns": decision,
        "feature_snapshot_hash": "0" * 64,
    }
    event, status = v73._materialize_fixed_parent_signal(
        lookup, signal, holding_minutes=15, card=_card()
    )
    assert status == "TRADE_CREATED"
    assert event is not None
    assert event["exit_reason"] == "LEGACY_FIXED_HOLDING_EXIT"
    assert event["account_control_eligible"] is False
    assert "stop_price" not in event and "target_price" not in event


def test_static_parent_requires_canonical_es_mes_alignment_and_only_15m() -> None:
    es = _day("2024-08-02", breach=False, minutes=140)
    es.loc[100:104, "high"] = 102.0
    es.loc[100:104, "close"] = 100.0
    es.loc[100:104, "signed_aggressor_volume"] = 800.0
    mes = _day("2024-08-02", breach=False, minutes=140).copy()
    mes["product"] = "MES"
    mes["contract"] = "MESU4"
    aligned = pd.concat([es, mes], ignore_index=True)
    parent, _ = v73.build_static_parent_control(
        aligned, holding_minutes=15, card=_card()
    )
    assert len(parent) == 1
    assert parent[0]["exit_reason"] == "LEGACY_FIXED_HOLDING_EXIT"
    assert (
        parent[0]["exit_minute_start_ns"]
        - parent[0]["entry_minute_start_ns"]
        == 15 * v73.MINUTE_NS
    )

    mes_missing_trigger_block = mes.drop(index=range(100, 105))
    misaligned = pd.concat([es, mes_missing_trigger_block], ignore_index=True)
    excluded, _ = v73.build_static_parent_control(
        misaligned, holding_minutes=15, card=_card()
    )
    assert excluded == ()

    try:
        v73.build_static_parent_control(
            aligned, holding_minutes=30, card=_card()
        )
    except v73.V73TripwireError as exc:
        assert "must remain 15m" in str(exc)
    else:
        raise AssertionError("canonical parent cannot be reconstructed at 30m")


def test_control_power_is_role_local_and_fails_when_final_control_is_missing() -> None:
    card = _card()

    def rows(day: int, count: int) -> tuple[dict[str, object], ...]:
        return tuple(
            {"session_day": day, "event_id": f"{day}:{index}"}
            for index in range(count)
        )

    primary = rows(20230803, 20) + rows(20240805, 8) + rows(20240820, 8)
    complete = tuple(primary)
    missing_final = rows(20230803, 20) + rows(20240805, 8)
    event_sets = {
        "candidate": {
            v73.PRIMARY: primary,
            "STATIC_EXTREME_REJECTION_PARENT": complete,
            "DIRECTION_FLIP_SAME_EVENTS": complete,
            "SESSION_MATCHED_TIMING_NULL": missing_final,
        }
    }
    power = v73._power_preflight(event_sets, card)
    assert power["passed"] is False
    assert (
        power["candidates"]["candidate"]["control_power"]
        ["SESSION_MATCHED_TIMING_NULL"]["FINAL_DEVELOPMENT"]
        is False
    )


def test_result_persistence_is_idempotent_and_never_overwrites(tmp_path: Path) -> None:
    result = {"result_hash": "a" * 64, "status": "TEST"}
    first = v73.persist_economic_result(
        tmp_path, result, output_root="reports/v73"
    )
    second = v73.persist_economic_result(
        tmp_path, result, output_root="reports/v73"
    )
    assert first == second
    try:
        v73.persist_economic_result(
            tmp_path,
            {"result_hash": "b" * 64, "status": "DIFFERENT"},
            output_root="reports/v73",
        )
    except v73.V73TripwireError as exc:
        assert "refusing to overwrite" in str(exc)
    else:
        raise AssertionError("divergent overwrite must fail closed")


def test_script_safe_default_is_audit_only() -> None:
    source = Path("scripts/run_v73_extreme_recovery_speed_tripwire.py").read_text(
        encoding="utf-8"
    )
    assert "if args.run_economic" in source
    assert "else:" in source
    assert "payload = audit_only" in source
    assert hashlib.sha256(source.encode()).hexdigest()
