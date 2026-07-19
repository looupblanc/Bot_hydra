from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hydra.research import treasury_three_tenor_curvature_tripwire as tripwire


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _root_frame(
    root_name: str,
    *,
    periods: int = 500,
    start: str = "2024-01-03T14:00:00Z",
    phase: float = 0.0,
) -> pd.DataFrame:
    timestamp = pd.date_range(start, periods=periods, freq="1min")
    index = np.arange(periods, dtype=float)
    base = {"ZT": 102.0, "ZF": 108.0, "ZN": 112.0, "ZB": 120.0}[root_name]
    close = (
        base
        + 0.002 * index
        + 0.04 * np.sin(index / (11.0 + phase))
        + 0.015 * np.cos(index / (5.0 + phase))
    )
    return pd.DataFrame(
        {
            "timestamp": timestamp,
            "symbol": root_name,
            "contract": f"{root_name}H4",
            "delivery_month": "202403",
            "open": close - 0.002,
            "high": close + 0.01,
            "low": close - 0.01,
            "close": close,
            "volume": 100,
            "session_id": "2024-01-03",
            "instrument_id": f"{root_name}:1",
            "roll_segment_id": f"{root_name}:segment:1",
        }
    )


def _prepared_execution_frame(*, gap_index: int | None = None) -> pd.DataFrame:
    triangle = tripwire.TRIANGLES[0]
    periods = 190
    timestamp = pd.date_range("2024-01-03T14:00:00Z", periods=periods, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamp,
            "available_at": timestamp + pd.Timedelta(minutes=1),
            "session_id": "2024-01-03",
            "session_day": pd.Timestamp("2024-01-03").date().toordinal(),
            "local_minute": np.arange(8 * 60, 8 * 60 + periods),
            "temporal_role": "VALIDATION",
            "contract_segment": 1,
            "roll_unsafe": False,
            "decision_eligible": False,
            "short_return": 0.001,
            "belly_return": 0.002,
            "long_return": 0.0005,
            "prior_beta_short": 0.5,
            "prior_beta_long": 0.5,
            "curvature_residual": 0.00125,
            "prior_only_curvature_z": 0.0,
            "prior_belly_volatility": 0.001,
            "nearest_adjacent_slope_return": 0.001,
            "session_full_coverage": True,
        }
    )
    for root_name, price in (("ZT", 102.0), ("ZF", 108.0), ("ZN", 112.0)):
        frame[f"{root_name}_contract"] = f"{root_name}H4"
        frame[f"{root_name}_delivery_month"] = "202403"
        frame[f"{root_name}_roll_segment_id"] = f"{root_name}:segment:1"
        frame[f"{root_name}_open"] = price
        frame[f"{root_name}_high"] = price + 0.005
        frame[f"{root_name}_low"] = price - 0.005
        frame[f"{root_name}_close"] = price
    signal_index = 10
    frame.loc[signal_index - 1, "prior_only_curvature_z"] = 1.0
    frame.loc[signal_index, "prior_only_curvature_z"] = 2.5
    frame.loc[signal_index, "decision_eligible"] = True
    # Reversion is short.  A lower low on the second post-entry bar reaches
    # its target; the fill remains the following tradable open.
    frame.loc[signal_index + 2, "ZF_low"] = 107.90
    if gap_index is not None:
        frame.loc[gap_index:, "timestamp"] += pd.Timedelta(minutes=1)
        frame["available_at"] = frame["timestamp"] + pd.Timedelta(minutes=1)
    return frame


def test_frozen_lattice_is_exactly_eight_unique_outight_rules() -> None:
    rules = tripwire.frozen_rule_specs()
    assert len(rules) == 8
    assert len({row.rule_id for row in rules}) == 8
    assert {row.triangle_id for row in rules} == {"ZT_ZF_ZN", "ZF_ZN_ZB"}
    assert {row.source_return_minutes for row in rules} == {15, 60}
    assert {row.holding_minutes for row in rules} == {30, 120}
    assert {row.fill_policy for row in rules} == {"NEXT_TRADABLE_BELLY_OPEN"}


def test_real_audit_reads_metadata_not_outcomes(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_decode(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("economic Parquet rows were decoded")

    monkeypatch.setattr(pd, "read_parquet", forbidden_decode)
    audit = tripwire.audit_inputs(PROJECT_ROOT)
    assert audit["status"] == "READY_FOR_ROOT_ECONOMIC_REPLAY_AUTHORIZATION"
    assert audit["rule_count"] == 8
    assert audit["control_count"] == 4
    assert audit["headline_gate_horizon_trading_days"] == 20
    assert audit["economic_outcome_rows_read"] == 0
    assert audit["parquet_row_groups_decoded"] == 0
    assert audit["q4_access_count_delta"] == 0
    assert audit["network_requests"] == 0
    assert audit["registry_writes"] == 0
    assert audit["cemetery_writes"] == 0


def test_replay_requires_exact_root_authorization_before_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def forbidden_audit(*_args: object, **_kwargs: object) -> object:
        calls.append("audit")
        raise AssertionError("audit should not be reached")

    monkeypatch.setattr(tripwire, "audit_inputs", forbidden_audit)
    with pytest.raises(tripwire.TreasuryCurvatureError, match="authorization absent"):
        tripwire.run_economic_tripwire(PROJECT_ROOT, authorization="NO")
    assert calls == []


def test_appending_future_rows_cannot_change_prior_curvature_features() -> None:
    triangle = tripwire.TRIANGLES[0]
    short = _root_frame("ZT", phase=0.0)
    belly = _root_frame("ZF", phase=1.0)
    long = _root_frame("ZN", phase=2.0)
    arguments = {
        "triangle": triangle,
        "source_return_minutes": 15,
        "beta_window_bars": 60,
        "beta_minimum_bars": 20,
        "normalization_window_bars": 60,
        "normalization_minimum_bars": 20,
    }
    before, _ = tripwire.prepare_curvature_features(short, belly, long, **arguments)
    extended = []
    for frame, root_name in ((short, "ZT"), (belly, "ZF"), (long, "ZN")):
        row = frame.iloc[-1].copy()
        row["timestamp"] = frame["timestamp"].iloc[-1] + pd.Timedelta(minutes=1)
        row[["open", "high", "low", "close"]] = [999.0, 999.1, 998.9, 999.0]
        extended.append(pd.concat([frame, row.to_frame().T], ignore_index=True))
    after, _ = tripwire.prepare_curvature_features(*extended, **arguments)
    pd.testing.assert_frame_equal(
        before,
        after.iloc[:-1].reset_index(drop=True),
        check_dtype=False,
    )


def test_future_or_outcome_decision_fields_fail_closed() -> None:
    short = _root_frame("ZT")
    short["future_label"] = 1
    with pytest.raises(tripwire.TreasuryCurvatureError, match="forbidden"):
        tripwire.prepare_curvature_features(
            short,
            _root_frame("ZF", phase=1.0),
            _root_frame("ZN", phase=2.0),
            triangle=tripwire.TRIANGLES[0],
            source_return_minutes=15,
            beta_window_bars=60,
            beta_minimum_bars=20,
            normalization_window_bars=60,
            normalization_minimum_bars=20,
        )


def test_primary_fill_and_exit_are_causal_and_tick_executable() -> None:
    frame = _prepared_execution_frame()
    rule = next(
        row
        for row in tripwire.frozen_rule_specs()
        if row.triangle_id == "ZT_ZF_ZN"
        and row.mechanism.endswith("REVERSION")
        and row.holding_minutes == 30
    )
    replay = tripwire._replay_primary_path(
        frame,
        triangle=tripwire.TRIANGLES[0],
        rule=rule,
        signal_index=10,
        direction=-1,
    )
    event = replay["event"]
    assert event is not None
    assert event["session_compliant"] is True
    assert pd.Timestamp(event["signal_time"]) < pd.Timestamp(event["entry_time"])
    assert pd.Timestamp(event["exit_trigger_bar_time"]) < pd.Timestamp(event["exit_time"])
    tick = tripwire.TREASURY_SPECS["ZF"].tick_size_points
    for key in ("entry_open", "exit_open", "path_low", "path_high"):
        assert float(event[key]) / tick == pytest.approx(round(float(event[key]) / tick))
    normal = tripwire._scenario_events(
        [event],
        execution_root="ZF",
        quantity=1,
        maximum_contracts=5,
        scenario="NORMAL",
    )
    stressed = tripwire._scenario_events(
        [event],
        execution_root="ZF",
        quantity=1,
        maximum_contracts=5,
        scenario="STRESSED_1_5X",
    )
    tripwire._require_scenario_identity(normal, stressed)
    assert stressed[0].net_pnl < normal[0].net_pnl

    broken = frame.copy()
    broken.loc[int(event["exit_index"]), "session_id"] = "2024-01-04"
    noncompliant = tripwire._raw_path_event(
        broken,
        triangle=tripwire.TRIANGLES[0],
        rule=rule,
        signal_index=10,
        entry_index=int(event["entry_index"]),
        exit_index=int(event["exit_index"]),
        direction=-1,
        control="PRIMARY",
        opportunity_id="x",
        declared_stop_distance_points=float(event["declared_stop_distance_points"]),
        exit_reason="TARGET",
        same_bar_ambiguous=False,
        trigger_index=12,
    )
    assert noncompliant["session_compliant"] is False


def test_missing_path_interval_preserves_decision_as_censored() -> None:
    frame = _prepared_execution_frame(gap_index=12)
    rule = next(
        row
        for row in tripwire.frozen_rule_specs()
        if row.triangle_id == "ZT_ZF_ZN"
        and row.mechanism.endswith("REVERSION")
        and row.holding_minutes == 30
    )
    replay = tripwire._replay_primary_path(
        frame,
        triangle=tripwire.TRIANGLES[0],
        rule=rule,
        signal_index=10,
        direction=-1,
    )
    assert replay["event"] is None
    assert replay["decision"]["outcome_status"] == "DATA_CENSORED"
    assert replay["decision"]["signal_time"] == frame.at[10, "timestamp"].isoformat()


def test_controls_are_opportunity_path_duty_and_role_matched() -> None:
    frame = _prepared_execution_frame()
    rule = next(
        row
        for row in tripwire.frozen_rule_specs()
        if row.triangle_id == "ZT_ZF_ZN"
        and row.mechanism.endswith("REVERSION")
        and row.holding_minutes == 30
    )
    primary, rows, decisions = tripwire._build_matched_control_paths(
        frame, triangle=tripwire.TRIANGLES[0], rule=rule
    )
    assert len(primary) == 1
    assert len(rows["PRIMARY_MATCHED"]) == 1
    receipt = tripwire._control_matching_receipt(rows)
    assert receipt["passed"] is True
    assert len({len(value) for value in rows.values()}) == 1
    assert {
        row["control"] for row in decisions if row["outcome_status"] == "EXECUTABLE_COMPLETE"
    } == set(tripwire.CONTROLS)


def test_headline_gate_never_double_counts_diagnostic_p10() -> None:
    def summary(*, passes: int, net: float) -> dict[str, object]:
        return {
            "episodes": 3,
            "passes": passes,
            "pass_rate": passes / 3,
            "mll_breaches": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 1.0,
            "all_passing_paths_consistency_compliant": bool(passes),
            "net_total_usd": net,
            "net_median_usd": net / 3,
            "target_progress_median": net / 3000,
            "target_progress_p25": 0.0,
            "minimum_mll_buffer_usd": 1500.0,
            "median_days_to_target": 10.0 if passes else None,
            "terminal_distribution": {},
            "start_days": [1, 21, 41],
            "coverage_status": "FULL_COVERAGE",
        }

    controls: dict[str, object] = {}
    for control in tripwire.ACCOUNT_CONTROL_KEYS:
        scenarios: dict[str, object] = {}
        for scenario in tripwire.SCENARIOS:
            roles: dict[str, object] = {}
            for role in tripwire.ROLES:
                roles[role] = {
                    "5": summary(passes=0, net=0.0),
                    "10": summary(passes=3, net=9000.0),
                    "20": summary(passes=0, net=0.0),
                }
            scenarios[scenario] = roles
        controls[control] = scenarios
    point = {
        "integer_quantity": 1,
        "controls": controls,
        "paired_headline_deltas": tripwire._paired_headline_deltas(controls),
        "final_stressed_profit_concentration": {
            "maximum_single_trade_share": 0.0,
            "maximum_single_day_share": 0.0,
        },
    }
    card = tripwire._read_json(PROJECT_ROOT / tripwire.DEFAULT_CARD)
    gate = tripwire._candidate_gate(point, card=card)
    assert gate["passed"] is False
    assert gate["headline_horizon_trading_days"] == 20
    assert gate["headline_final_normal_passes"] == 0
    assert gate["headline_final_stressed_passes"] == 0


def test_card_governance_forbids_q4_writes_and_promotion() -> None:
    card = tripwire._read_json(PROJECT_ROOT / tripwire.DEFAULT_CARD)
    tripwire._validate_card(card)
    assert card["governance"]["tier_ceiling"] == "E"
    assert card["governance"]["tier_q_allowed"] is False
    assert card["governance"]["q4_access_allowed"] is False
    assert card["governance"]["registry_or_cemetery_write_allowed"] is False
    assert card["account_frontier"]["headline_gate_horizon_trading_days"] == 20
    assert card["account_frontier"]["diagnostic_horizons_trading_days"] == [5, 10]


def test_three_legs_require_the_exact_same_delivery_month() -> None:
    short = _root_frame("ZT", phase=0.0)
    belly = _root_frame("ZF", phase=1.0)
    long = _root_frame("ZN", phase=2.0)
    belly.loc[250, "delivery_month"] = "202406"
    prepared, audit = tripwire.prepare_curvature_features(
        short,
        belly,
        long,
        triangle=tripwire.TRIANGLES[0],
        source_return_minutes=15,
        beta_window_bars=60,
        beta_minimum_bars=20,
        normalization_window_bars=60,
        normalization_minimum_bars=20,
    )
    assert len(prepared) == len(short) - 1
    assert audit["delivery_mismatch_rows_excluded"] == 1
    delivery = prepared[["ZT_delivery_month", "ZF_delivery_month", "ZN_delivery_month"]]
    assert delivery.astype(str).nunique(axis=1).eq(1).all()
    assert prepared["session_full_coverage"].eq(False).all()


def test_early_target_is_complete_without_full_time_horizon_but_time_exit_is_not() -> None:
    frame = _prepared_execution_frame().iloc[:20].copy()
    rule = next(
        row
        for row in tripwire.frozen_rule_specs()
        if row.triangle_id == "ZT_ZF_ZN"
        and row.mechanism.endswith("REVERSION")
        and row.holding_minutes == 30
    )
    early = tripwire._replay_primary_path(
        frame,
        triangle=tripwire.TRIANGLES[0],
        rule=rule,
        signal_index=10,
        direction=-1,
    )
    assert early["event"] is not None
    assert early["event"]["exit_reason"] == "TARGET"

    no_trigger = frame.copy()
    no_trigger.loc[:, "ZF_low"] = 107.995
    no_trigger.loc[:, "ZF_high"] = 108.005
    censored = tripwire._replay_primary_path(
        no_trigger,
        triangle=tripwire.TRIANGLES[0],
        rule=rule,
        signal_index=10,
        direction=-1,
    )
    assert censored["event"] is None
    assert censored["decision"]["outcome_status"] == "DATA_CENSORED"
    assert censored["decision"]["censor_reason"] == "CENSORED_FUTURE_COVERAGE_TIME_EXIT"


def test_exit_bar_extrema_are_excluded_because_exit_is_open_only() -> None:
    frame = _prepared_execution_frame()
    frame.loc[13, "ZF_high"] = 999.0
    frame.loc[13, "ZF_low"] = 1.0
    rule = next(
        row
        for row in tripwire.frozen_rule_specs()
        if row.triangle_id == "ZT_ZF_ZN"
        and row.mechanism.endswith("REVERSION")
        and row.holding_minutes == 30
    )
    replay = tripwire._replay_primary_path(
        frame,
        triangle=tripwire.TRIANGLES[0],
        rule=rule,
        signal_index=10,
        direction=-1,
    )
    event = replay["event"]
    assert event is not None
    assert event["exit_index"] == 13
    assert event["path_high"] < 999.0
    assert event["path_low"] > 1.0


def test_quantity_freeze_uses_discovery_only() -> None:
    base = {
        "opportunity_id": "d",
        "temporal_role": "DISCOVERY",
        "declared_stop_distance_points": 0.05,
    }
    validation = {
        "opportunity_id": "v",
        "temporal_role": "VALIDATION",
        "declared_stop_distance_points": 50.0,
    }
    rule_snapshot, _ = tripwire.exact._load_rule_snapshot(
        PROJECT_ROOT / "config/rulesets/topstep_official_2026-07-19.json"
    )
    first = tripwire._freeze_discovery_sizing(
        [base, validation],
        execution_root="ZF",
        account_rule=rule_snapshot["50K"],
        risk_fraction=0.1,
    )
    changed = deepcopy(validation)
    changed["declared_stop_distance_points"] = 5000.0
    second = tripwire._freeze_discovery_sizing(
        [base, changed],
        execution_root="ZF",
        account_rule=rule_snapshot["50K"],
        risk_fraction=0.1,
    )
    assert first == second
    assert first["validation_or_final_inputs_used"] is False
    assert first["discovery_event_count"] == 1


def test_candidate_power_is_a_hard_gate() -> None:
    def summary(passes: int, net: float) -> dict[str, object]:
        return {
            "passes": passes,
            "mll_breach_rate": 0.0,
            "net_total_usd": net,
            "target_progress_median": 1.0,
            "target_progress_p25": 0.5,
            "all_passing_paths_consistency_compliant": True,
            "start_days": [1],
        }

    controls: dict[str, object] = {}
    for control in tripwire.ACCOUNT_CONTROL_KEYS:
        controls[control] = {
            scenario: {
                role: {
                    "20": summary(
                        3 if control in {"PRIMARY", "PRIMARY_MATCHED"} else 0,
                        1000.0 if control in {"PRIMARY", "PRIMARY_MATCHED"} else -1.0,
                    )
                }
                for role in tripwire.ROLES
            }
            for scenario in tripwire.SCENARIOS
        }
    point = {
        "integer_quantity": 1,
        "controls": controls,
        "paired_headline_deltas": tripwire._paired_headline_deltas(controls),
        "final_stressed_profit_concentration": {
            "maximum_single_trade_share": 0.2,
            "maximum_single_day_share": 0.2,
        },
    }
    card = tripwire._read_json(PROJECT_ROOT / tripwire.DEFAULT_CARD)
    gate = tripwire._candidate_gate(
        point, card=card, candidate_power_passed=False
    )
    assert gate["passed"] is False
    assert gate["checks"]["candidate_power_preflight"] is False


def test_primary_complete_is_not_truncated_to_common_control_coverage() -> None:
    frame = _prepared_execution_frame().iloc[:18].copy()
    rule = next(
        row
        for row in tripwire.frozen_rule_specs()
        if row.triangle_id == "ZT_ZF_ZN"
        and row.mechanism.endswith("REVERSION")
        and row.holding_minutes == 30
    )
    primary, matched, _ = tripwire._build_matched_control_paths(
        frame, triangle=tripwire.TRIANGLES[0], rule=rule
    )
    assert len(primary) == 1
    assert len(matched["PRIMARY_MATCHED"]) == 0
    assert all(len(rows) == 0 for rows in matched.values())


def test_required_session_window_masks_missing_minute_and_censors_start() -> None:
    minutes = np.arange(tripwire.COVERAGE_START_MINUTE, tripwire.COVERAGE_END_MINUTE + 1)
    timestamp = pd.date_range(
        "2024-01-03T12:19:00Z", periods=len(minutes), freq="1min"
    )
    frame = pd.DataFrame(
        {
            "timestamp": timestamp,
            "session_id": "2024-01-03",
            "local_minute": minutes,
            "roll_unsafe": False,
            "delivery_mismatch_session": False,
        }
    )
    assert tripwire._session_full_coverage_mask(frame).all()
    missing = frame.drop(index=100).reset_index(drop=True)
    assert not tripwire._session_full_coverage_mask(missing).any()


def test_out_of_scope_warmup_sessions_are_excluded_without_dropping_in_scope_censure() -> None:
    card = tripwire._read_json(PROJECT_ROOT / tripwire.DEFAULT_CARD)
    timestamps = pd.Series(
        pd.to_datetime(["2022-12-30T12:00:00Z", "2023-01-03T12:00:00Z"], utc=True)
    )
    assert tripwire._role_scope_mask(timestamps, card).tolist() == [False, True]

    frame = pd.DataFrame({"timestamp": [timestamps.iloc[1]]})
    frame.attrs["canonical_session_calendar"] = [
        {
            "session_id": "2022-12-30",
            "session_day": pd.Timestamp("2022-12-30").date().toordinal(),
            "session_full_coverage": True,
        },
        {
            "session_id": "2023-01-03",
            "session_day": pd.Timestamp("2023-01-03").date().toordinal(),
            "session_full_coverage": False,
            "coverage_reason": "ROOT_SESSION_ABSENT_BEFORE_INNER_JOIN",
        },
    ]
    tripwire._attach_canonical_calendar_roles(frame, card)
    calendar = frame.attrs["canonical_session_calendar"]
    assert [row["session_id"] for row in calendar] == ["2023-01-03"]
    assert calendar[0]["temporal_role"] == "DISCOVERY"
    assert calendar[0]["session_full_coverage"] is False


def test_timing_control_is_five_bars_after_primary_entry_and_slope_sign_matches_card() -> None:
    frame = _prepared_execution_frame()
    rule = next(
        row
        for row in tripwire.frozen_rule_specs()
        if row.triangle_id == "ZT_ZF_ZN"
        and row.mechanism.endswith("REVERSION")
        and row.holding_minutes == 30
    )
    primary, matched, _ = tripwire._build_matched_control_paths(
        frame, triangle=tripwire.TRIANGLES[0], rule=rule
    )
    assert matched["TIMING_DELAY_5_BARS"][0]["entry_index"] == (
        primary[0]["entry_index"] + 5
    )
    row = {"belly_return": 0.01, "nearest_adjacent_slope_return": 0.02}
    continuation = tripwire._control_direction(
        row,
        primary_direction=-1,
        mechanism="CURVATURE_RESIDUAL_CONTINUATION",
        control="NEAREST_ADJACENT_SLOPE",
    )
    reversion = tripwire._control_direction(
        row,
        primary_direction=1,
        mechanism="CURVATURE_RESIDUAL_REVERSION",
        control="NEAREST_ADJACENT_SLOPE",
    )
    assert continuation == 1
    assert reversion == -1


def test_stressed_all_in_cost_is_exactly_one_and_a_half_normal() -> None:
    normal = tripwire._scenario_cost_contract("ZN", "NORMAL")
    stressed = tripwire._scenario_cost_contract("ZN", "STRESSED_1_5X")
    assert stressed["scenario_all_in_cost_one_contract_usd"] == pytest.approx(
        1.5 * normal["scenario_all_in_cost_one_contract_usd"]
    )
    assert normal["tick_executable_slippage_ticks_per_side"] == 1
    assert stressed["tick_executable_slippage_ticks_per_side"] == 1


def test_concentration_uses_only_full_coverage_headline_days() -> None:
    from hydra.propfirm.combine_episode import TradePathEvent

    def event(day: int, pnl: float) -> TradePathEvent:
        return TradePathEvent(
            event_id=f"e:{day}",
            decision_ns=day,
            exit_ns=day + 1,
            session_day=day,
            net_pnl=pnl,
            gross_pnl=pnl,
            worst_unrealized_pnl=min(0.0, pnl),
            best_unrealized_pnl=max(0.0, pnl),
            quantity=1,
            mini_equivalent=1.0,
            regime="FINAL_DEVELOPMENT:PRIMARY",
        )

    result = tripwire._headline_profit_concentration(
        [event(1, 10.0), event(2, 10_000.0)],
        traversed_session_days={1},
    )
    assert result["maximum_single_trade_share"] == 1.0


def test_account_matrix_persists_episode_ledgers_and_real_censures() -> None:
    days = pd.bdate_range("2024-01-02", periods=40)
    frame = pd.DataFrame(
        {
            "session_id": [str(day.date()) for day in days],
            "session_day": [day.date().toordinal() for day in days],
            "temporal_role": ["FINAL_DEVELOPMENT"] * 40,
            "session_full_coverage": [False] + [True] * 39,
        }
    )
    rules, _ = tripwire.exact._load_rule_snapshot(
        PROJECT_ROOT / "config/rulesets/topstep_official_2026-07-19.json"
    )
    matrix = tripwire._account_role_matrix(
        frame,
        {scenario: () for scenario in tripwire.SCENARIOS},
        account_rule=rules["50K"],
    )
    headline = matrix["NORMAL"]["FINAL_DEVELOPMENT"]["20"]
    assert headline["all_start_count"] == 2
    assert headline["full_coverage_start_count"] == 1
    assert headline["data_censored_start_count"] == 1
    assert len(headline["episode_ledger"]) == 1
    assert len(headline["episode_ledger_hash"]) == 64
    assert headline["censored_start_ledger"][0]["status"] == "DATA_CENSORED"
