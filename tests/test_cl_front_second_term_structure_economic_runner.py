from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hydra.production import autonomous_exact_replay as exact
from hydra.research import cl_front_second_term_structure_economic_runner as runner


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_real_audit_is_read_only_tier_e_and_pre_q4() -> None:
    audit = runner.audit_tripwire_inputs(PROJECT_ROOT)
    assert audit["status"] == "READY_FOR_BOUNDED_ECONOMIC_REPLAY"
    assert audit["receipt_hash"] == "048337824527fddf660214b13582723f31f3a6c9a8a6e4627ad06919f6228add"
    assert audit["rule_count"] == 8
    assert audit["control_count"] == 4
    assert audit["latest_data_end_exclusive"] == "2024-10-01"
    assert audit["q4_rows"] == 0
    assert audit["promotion_allowed"] is False
    assert audit["tier_ceiling"] == "E"
    assert audit["network_requests"] == 0
    assert audit["writes"] == 0
    assert audit["session_decision_contract"]["decision_window_chicago"] == {
        "start": "07:00",
        "end_exclusive": "14:00",
        "start_minute_inclusive": 7 * 60,
        "end_minute_exclusive": 14 * 60,
    }
    assert audit["session_decision_contract"]["causal_warmup_minutes"] == 60
    assert audit["session_decision_contract"]["source_prior_return_count"] == 60
    assert audit["session_decision_contract"]["source_warmup_price_observations"] == 61
    assert audit["session_decision_contract"]["required_source_coverage_chicago"] == {
        "start": "05:59",
        "end_exclusive": "14:00",
    }
    assert audit["session_decision_contract"]["required_target_coverage_chicago"] == {
        "start": "06:00",
        "end_inclusive": "15:10",
    }


def test_invalid_receipt_stops_before_any_economic_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fail_audit(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise runner.CLTermStructureEconomicError("receipt invalid")

    def forbidden_loader(*_args: object, **_kwargs: object) -> object:
        calls.append("economic_loader")
        raise AssertionError("economic loader was reached")

    monkeypatch.setattr(runner, "audit_tripwire_inputs", fail_audit)
    monkeypatch.setattr(runner, "_load_frozen_front_and_target", forbidden_loader)
    with pytest.raises(runner.CLTermStructureEconomicError, match="receipt invalid"):
        runner.run_tripwire(PROJECT_ROOT)
    assert calls == []


def test_dynamic_account_replay_uses_current_buffer_and_exact_combine_rules() -> None:
    rules, _receipt = exact._load_rule_snapshot(
        PROJECT_ROOT / "config/rulesets/topstep_official_2026-07-19.json"
    )
    config = exact._account_config(rules["50K"])
    events = [
        {
            "event_id": f"e{index}",
            "session_day": day,
            "decision_ns": index,
            "gross_one_micro": 800.0,
            "favorable_one_micro": 100.0,
            "adverse_one_micro": -50.0,
            "stop_risk_one_micro": 100.0,
        }
        for index, day in enumerate((20240102, 20240103))
    ]
    result = runner._run_dynamic_episode(
        events,
        (20240102, 20240103, 20240104, 20240105, 20240108),
        start_day=20240102,
        horizon=5,
        config=config,
        account_label="50K",
        micro_cap=30,
        risk_fraction=0.1,
        scenario="STRESSED",
    )
    assert result["terminal"] == "PASSED"
    assert result["days_to_target"] == 2
    assert result["maximum_micro_quantity"] == 2
    assert result["consistency_ok"] is True
    assert result["minimum_mll_buffer"] > 0.0


def test_causal_event_enters_and_exits_only_on_later_opens() -> None:
    timestamps = pd.date_range("2024-01-03T14:00:00Z", periods=70, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": 70.0,
            "high": 70.1,
            "low": 69.9,
            "close": 70.0,
            "session_day": 20240103,
            "local_minute": 8 * 60 + 0 + pd.Series(range(70)),
            "roll_unsafe": False,
        }
    )
    # The first executable bar after 15:00 touches the long target.  Exit is
    # assigned only to the following bar open, never to the touch bar itself.
    frame.loc[61, "high"] = 73.0
    target = runner._TargetIndex(frame)
    rule = next(value for value in runner.frozen_rule_specs() if value.holding_minutes == 30)
    event = runner._replay_at_timestamp(
        target,
        pd.Timestamp("2024-01-03T15:00:00Z"),
        1,
        rule,
        control="PRIMARY",
        source_feature_hash="a" * 64,
    )
    assert event is not None
    assert pd.Timestamp(event["signal_time"]) < pd.Timestamp(event["decision_time"])
    assert pd.Timestamp(event["entry_time"]) == pd.Timestamp(event["decision_time"])
    assert pd.Timestamp(event["exit_time"]) > pd.Timestamp(frame.loc[61, "timestamp"])
    assert pd.Timestamp(event["exit_trigger_available_at"]) == pd.Timestamp(
        event["exit_time"]
    )
    assert pd.Timestamp(event["exit_trigger_bar_time"]) < pd.Timestamp(
        event["exit_time"]
    )
    assert round(float(event["stop_price"]) / 0.01) == pytest.approx(
        float(event["stop_price"]) / 0.01
    )
    assert round(float(event["target_price"]) / 0.01) == pytest.approx(
        float(event["target_price"]) / 0.01
    )
    assert event["session_compliant"] is True


def test_time_exit_holds_exactly_frozen_number_of_bars() -> None:
    timestamps = pd.date_range("2024-01-03T14:00:00Z", periods=100, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": 70.0,
            "high": 70.01,
            "low": 69.99,
            "close": 70.0,
            "session_day": 20240103,
            "local_minute": 8 * 60 + pd.Series(range(100)),
            "roll_unsafe": False,
        }
    )
    target = runner._TargetIndex(frame)
    rule = next(value for value in runner.frozen_rule_specs() if value.holding_minutes == 30)
    event = runner._replay_at_timestamp(
        target,
        pd.Timestamp("2024-01-03T15:00:00Z"),
        1,
        rule,
        control="PRIMARY",
        source_feature_hash="c" * 64,
    )
    assert event["exit_reason"] == "TIME"
    assert pd.Timestamp(event["exit_time"]) - pd.Timestamp(event["entry_time"]) == pd.Timedelta(
        minutes=30
    )


def test_missing_future_bar_preserves_signal_as_data_censored() -> None:
    timestamps = pd.date_range("2024-01-03T14:00:00Z", periods=70, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": 70.0,
            "high": 70.1,
            "low": 69.9,
            "close": 70.0,
            "session_day": 20240103,
            "local_minute": 8 * 60 + pd.Series(range(70)),
            "roll_unsafe": False,
        }
    ).drop(index=66).reset_index(drop=True)
    target = runner._TargetIndex(frame)
    rule = next(value for value in runner.frozen_rule_specs() if value.holding_minutes == 30)
    signal = runner._replay_at_timestamp(
        target,
        pd.Timestamp("2024-01-03T15:00:00Z"),
        1,
        rule,
        control="PRIMARY",
        source_feature_hash="b" * 64,
    )
    assert signal is not None
    assert signal["decision_time"] == "2024-01-03T15:00:00+00:00"
    assert signal["outcome_status"] == "DATA_CENSORED"
    assert signal["censor_reason"] == "CENSORED_FUTURE_COVERAGE_PATH_GAP"
    assert signal["position_opened"] is True
    assert runner._completed_events([signal]) == []
    assert runner._censored_signal_days([signal]) == (20240103,)


def test_missing_warmup_bar_is_data_censored_not_abstention() -> None:
    timestamps = pd.date_range("2024-01-03T14:00:00Z", periods=100, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": 70.0,
            "high": 70.1,
            "low": 69.9,
            "close": 70.0,
            "session_day": 20240103,
            "local_minute": 8 * 60 + pd.Series(range(100)),
            "roll_unsafe": False,
        }
    ).drop(index=17).reset_index(drop=True)
    rule = next(value for value in runner.frozen_rule_specs() if value.holding_minutes == 30)
    signal = runner._replay_at_timestamp(
        runner._TargetIndex(frame),
        pd.Timestamp("2024-01-03T15:00:00Z"),
        1,
        rule,
        control="PRIMARY",
        source_feature_hash="d" * 64,
    )
    assert signal["outcome_status"] == "DATA_CENSORED"
    assert signal["censor_reason"] == "CENSORED_REQUIRED_PRIOR_VOLATILITY_GAP"


def test_decision_clock_is_bound_to_explicit_audited_contract() -> None:
    contract = runner._session_decision_contract(
        runner._read_json(PROJECT_ROOT / "config/research/cl_front_second_term_structure_tripwire_v1.json")
    )
    frame = pd.DataFrame(
        {
            "local_minute_chicago": ["06:59", "07:00", "13:59", "14:00"],
            "close_front": [70.0, 70.1, 70.2, 70.3],
            "log_front_second_basis": [0.01, 0.02, 0.03, 0.04],
            "decision_eligible": [True, True, True, True],
        }
    )
    result = runner._with_control_scores(frame, contract)
    assert result["eligible_source_clock"].tolist() == [False, True, True, False]
    assert result["decision_eligible"].tolist() == [False, True, True, False]


def test_control_overlap_loss_is_detected_instead_of_silently_matched() -> None:
    rows = [
        {"event_id": "a", "decision_ns": 1, "exit_ns": 10},
        {"event_id": "b", "decision_ns": 5, "exit_ns": 7},
        {"event_id": "c", "decision_ns": 11, "exit_ns": 12},
    ]
    kept = runner._nonoverlapping_events(rows)
    assert [row["event_id"] for row in kept] == ["a", "c"]


def test_exact_source_clock_gap_is_censored() -> None:
    full = pd.DataFrame(
        {
            "session_day": 20240103,
            "local_minute": list(range(5 * 60 + 59, 14 * 60)),
        }
    )
    assert runner._required_clock_gap_counts(
        full, start_minute=5 * 60 + 59, end_minute_exclusive=14 * 60
    ) == {20240103: 0}
    missing = full.loc[full["local_minute"].ne(5 * 60 + 59)]
    assert runner._required_clock_gap_counts(
        missing, start_minute=5 * 60 + 59, end_minute_exclusive=14 * 60
    ) == {20240103: 1}


def test_bounded_builder_preserves_decision_ledgers_separately() -> None:
    timestamps = pd.date_range("2024-01-03T14:00:00Z", periods=190, freq="1min")
    target_frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": 70.0,
            "high": 70.1,
            "low": 69.9,
            "close": 70.0,
            "session_day": 20240103,
            "local_minute": 8 * 60 + pd.Series(range(190)),
            "roll_unsafe": False,
        }
    )
    decision_bar = pd.Timestamp("2024-01-03T15:00:00Z")
    features = pd.DataFrame(
        {
            "timestamp": [decision_bar - pd.Timedelta(minutes=1)],
            "available_at": [decision_bar],
            "decision_eligible": [True],
            "basis_robust_score_prior_sessions": [3.0],
            "front_residual_return": [0.01],
            "front_return_score": [3.0],
            "carry_level_score": [3.0],
            "front_days_to_delivery": [15.0],
            "second_days_to_delivery": [45.0],
            "delivery_tenor_gap_days": [30.0],
            "roll_distance_adjusted_basis_innovation": [0.01],
            "current_spread_state": [0.02],
            "front_prior_realized_volatility": [0.001],
        }
    )
    proposals, events, signals = runner._build_all_events(
        {15: features, 60: features}, runner._TargetIndex(target_frame), {}
    )
    assert len(proposals) == 8
    assert set(events) == set(signals)
    assert all("PRIMARY" in controls for controls in signals.values())
    assert all(
        row["outcome_status"] in {"EXECUTABLE_COMPLETE", "DATA_CENSORED", "CAUSAL_ABSTAIN"}
        for controls in signals.values()
        for rows in controls.values()
        for row in rows
    )
    assert all(
        len(controls["PRIMARY"]) == len(controls[control])
        for controls in signals.values()
        for control in runner.CONTROLS[1:]
    )
    assert all(
        len(controls["PRIMARY"]) == len(controls[control])
        for controls in events.values()
        for control in runner.CONTROLS[1:]
    )


def test_branch_gate_never_inherits_promotion() -> None:
    decision = {
        "candidate_id": "x",
        "gate": {
            "passed": True,
            "headline_final_normal_p20_passes": 2,
            "headline_final_stressed_p20_passes": 1,
            "final_stressed_net_usd": 1.0,
        },
        "promotion_status": None,
        "evidence_tier": "E_EXECUTABLE_DIAGNOSTIC",
    }
    result = runner._branch_gate([decision], {"passed": True}, {})
    assert result["status"] == "TERM_STRUCTURE_TRIPWIRE_GREEN_TIER_E"
    assert result["tier_e_candidate_ids"] == ["x"]
    assert decision["promotion_status"] is None


def test_branch_gate_counts_one_headline_horizon_only() -> None:
    decision = {
        "candidate_id": "x",
        "gate": {
            "passed": False,
            "headline_final_normal_p20_passes": 1,
            "headline_final_stressed_p20_passes": 1,
            "final_stressed_net_usd": 1.0,
        },
    }
    result = runner._branch_gate([decision], {"passed": True}, {})
    assert result["headline_final_normal_p20_passes"] == 1
    assert result["headline_final_stressed_p20_passes"] == 1
