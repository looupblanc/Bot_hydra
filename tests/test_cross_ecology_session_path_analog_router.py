from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.bundle import _validate_relational_contract
from hydra.research import cross_ecology_session_path_analog_router as router


ROOT = Path(__file__).resolve().parents[1]


def _outcome(target: bool) -> dict[str, object]:
    return {
        "status": "EXECUTABLE_COMPLETE",
        "target_first": target,
        "fill_time": "2023-01-03T14:35:00+00:00",
        "fill_price": 100.0,
        "exit_time": "2023-01-03T14:36:00+00:00",
        "exit_price": 102.0 if target else 99.0,
        "stop_price": 99.0,
        "target_price": 102.0,
        "terminal": "TARGET" if target else "STOP_FIRST",
        "gross_usd_per_micro": 10.0 if target else -5.0,
        "declared_stop_risk_usd_per_micro": 5.0,
        "mae_points": -1.0,
        "mfe_points": 2.0,
        "normal_cost_usd_per_micro": 3.0,
        "stressed_cost_usd_per_micro": 4.5,
    }


def _account_rule() -> dict[str, object]:
    return {
        "account_label": "50K",
        "account_size_usd": 50_000.0,
        "profit_target_usd": 3_000.0,
        "maximum_loss_limit_usd": 2_000.0,
        "maximum_micro_contracts": 50,
        "maximum_mini_contracts": 5,
        "consistency_target_fraction": 0.5,
        "minimum_trading_days": 2,
        "special_contract_caps": {
            "MCL": {"50K": 30},
            "MGC": {"50K": 30},
        },
    }


def _primary_event() -> dict[str, object]:
    return {
        "event_id": "e1",
        "candidate_id": "c1",
        "session_date": "2023-01-03",
        "temporal_role": "DISCOVERY",
        "market": "MNQ",
        "contract": "MNQH3",
        "decision_clock_local": "08:35",
        "decision_time": pd.Timestamp("2023-01-03T14:35:00Z"),
        "available_at": pd.Timestamp("2023-01-03T14:35:00Z"),
        "direction": 1,
        "permuted_label_direction": -1,
        "own_path_return": -0.01,
        "outcome": _outcome(True),
        "opposite_outcome": _outcome(False),
        "normal_cost_usd_per_micro": 3.0,
        "stressed_cost_usd_per_micro": 4.5,
    }


def test_card_self_hash_and_frozen_lattice() -> None:
    card = router.load_decision_card(ROOT / router.DEFAULT_CARD)
    core = dict(card)
    claimed = core.pop("card_hash")
    assert stable_hash(core) == claimed
    assert card["campaign_id"] == router.CAMPAIGN_ID
    rules = router.frozen_rule_specs()
    assert len(rules) == 6
    assert len({row.rule_id for row in rules}) == 6
    assert {row.panel for row in rules} == set(router.PANELS)
    assert {row.decision_clock_local for row in rules} == set(router.CLOCKS)


def test_economic_runner_rejects_every_inexact_token() -> None:
    with pytest.raises(router.SessionPathAnalogError, match="authorization"):
        router.run_economic_tripwire(ROOT, authorization="not-root-authorized")


def test_exact_authorization_still_requires_committed_production_manifest() -> None:
    with pytest.raises(router.SessionPathAnalogError, match="production manifest"):
        router.run_economic_tripwire(
            ROOT,
            authorization=router.RUN_AUTHORIZATION,
            production_manifest_path=None,
        )


def test_prior_normalization_is_strictly_previous_twenty_sessions() -> None:
    frame = pd.DataFrame(
        {
            "session_date": [f"2023-01-{day:02d}" for day in range(1, 23)],
            "market": ["MNQ"] * 22,
            "decision_clock_local": ["08:35"] * 22,
            "f00": [float(value) for value in range(22)],
        }
    )
    normalized = router.prior_session_normalize(frame, ["f00"], prior_sessions=20)
    assert normalized.loc[:19, "z_f00"].isna().all()
    expected = (20.0 - 9.5) / (14.25 - 4.75)
    assert normalized.loc[20, "z_f00"] == pytest.approx(expected)
    changed = frame.copy()
    changed.loc[21, "f00"] = 999_999.0
    changed_normalized = router.prior_session_normalize(changed, ["f00"], prior_sessions=20)
    assert changed_normalized.loc[20, "z_f00"] == normalized.loc[20, "z_f00"]


def test_runtime_guard_forbids_future_and_non_discovery_library() -> None:
    now = pd.Timestamp("2023-02-01T14:35:00Z")
    with pytest.raises(router.SessionPathAnalogError, match="availability"):
        router.assert_runtime_causality(
            available_at=now + pd.Timedelta(minutes=1),
            decision_time=now,
            query_role="VALIDATION",
            query_day="2023-02-01",
            library_rows=[],
        )
    with pytest.raises(router.SessionPathAnalogError, match="non-Discovery"):
        router.assert_runtime_causality(
            available_at=now,
            decision_time=now,
            query_role="VALIDATION",
            query_day="2023-02-01",
            library_rows=[{"temporal_role": "VALIDATION", "session_date": "2023-01-01"}],
        )
    with pytest.raises(router.SessionPathAnalogError, match="strictly prior"):
        router.assert_runtime_causality(
            available_at=now,
            decision_time=now,
            query_role="DISCOVERY",
            query_day="2023-02-01",
            library_rows=[{"temporal_role": "DISCOVERY", "session_date": "2023-02-01"}],
        )


def test_next_open_fill_is_causal_and_same_bar_is_stop_first() -> None:
    decision = pd.Timestamp("2023-01-03T14:35:00Z")
    path = pd.DataFrame(
        {
            "timestamp": [decision, decision + pd.Timedelta(minutes=1)],
            "open": [100.0, 100.0],
            "high": [103.0, 100.5],
            "low": [99.0, 99.5],
            "close": [101.0, 100.0],
        }
    )
    result = router.next_open_fill(
        path,
        decision_time=decision,
        direction=1,
        stop_distance=1.0,
    )
    assert result["fill_time"] == decision
    assert result["fill_price"] == 100.0
    assert result["terminal"] == "STOP_FIRST"
    assert result["exit_price"] == 99.0


def test_all_three_panels_are_finite_and_eligible_after_warmup() -> None:
    card = json.loads(
        json.dumps(router.load_decision_card(ROOT / router.DEFAULT_CARD))
    )
    records: list[dict[str, object]] = []
    source_days = list(pd.bdate_range("2023-01-03", periods=23))
    first_role_day = source_days[21].strftime("%Y-%m-%d")
    card["chronological_roles"] = [
        {
            "role": "DISCOVERY",
            "start": first_role_day,
            "end": "2023-12-01",
            "candidate_modification_allowed": True,
        },
        {
            "role": "VALIDATION",
            "start": "2023-12-01",
            "end": "2024-01-01",
            "candidate_modification_allowed": False,
        },
        {
            "role": "FINAL_DEVELOPMENT",
            "start": "2024-01-01",
            "end": "2024-02-01",
            "candidate_modification_allowed": False,
        },
    ]
    for day_index, day in enumerate(source_days):
        session_date = day.strftime("%Y-%m-%d")
        local_times = pd.date_range(
            f"{session_date} 07:00", periods=490, freq="min", tz=router.SESSION_TZ
        )
        for market_index, market in enumerate(router.MARKETS):
            base = 100.0 + market_index * 10.0 + day_index * 0.1
            for minute, local_time in enumerate(local_times):
                price = base + minute * 0.001 + market_index * minute * 0.0001
                records.append(
                    {
                        "timestamp": local_time.tz_convert("UTC"),
                        "symbol": market,
                        "open": price,
                        "high": price + 0.25,
                        "low": price - 0.25,
                        "close": price + 0.01,
                        "volume": 100 + minute + market_index,
                        "session_id": session_date,
                        "active_contract": f"{market}H3",
                    }
                )
    features, coverage = router.build_session_features(pd.DataFrame(records), card)
    eligible = features.loc[features["decision_eligible"]]
    assert set(eligible["panel"]) == set(router.PANELS)
    assert {
        panel: len(rows.iloc[0]["feature_vector"])
        for panel, rows in eligible.groupby("panel")
    } == router.PANEL_FEATURE_COUNTS
    assert all(
        all(np.isfinite(value) for value in vector)
        for vector in eligible["feature_vector"]
    )
    assert eligible["session_date"].min() == first_role_day
    assert coverage["pre_role_source_session_count"] == 21
    assert coverage["pre_role_warmup_applied_before_economic_role_filter"] is True


def test_execution_path_tick_gap_contiguity_and_boundaries() -> None:
    decision = pd.Timestamp("2023-01-03T14:35:00Z")

    def path(count: int = 120) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range(decision, periods=count, freq="min"),
                "open": [100.0] * count,
                "high": [100.5] * count,
                "low": [99.5] * count,
                "close": [100.0] * count,
                "active_contract": ["MNQH3"] * count,
                "session_id": ["2023-01-03"] * count,
            }
        )

    complete = router.next_open_fill(
        path(),
        decision_time=decision,
        direction=1,
        stop_distance=0.60,
        tick_size=0.25,
        expected_contract="MNQH3",
        expected_session_date="2023-01-03",
        expected_session_id="2023-01-03",
    )
    assert complete["terminal"] == "TIME_EXIT"
    assert complete["stop_price"] == 99.25
    assert complete["target_price"] == 101.5
    missing = path().drop(index=50).reset_index(drop=True)
    assert router.next_open_fill(
        missing,
        decision_time=decision,
        direction=1,
        stop_distance=0.60,
        tick_size=0.25,
        expected_contract="MNQH3",
        expected_session_date="2023-01-03",
        expected_session_id="2023-01-03",
    )["censor_reason"] == "MISSING_INTERVAL_BEFORE_EXIT"
    roll = path()
    roll.loc[1:, "active_contract"] = "MNQM3"
    assert router.next_open_fill(
        roll,
        decision_time=decision,
        direction=1,
        stop_distance=0.60,
        tick_size=0.25,
        expected_contract="MNQH3",
        expected_session_date="2023-01-03",
        expected_session_id="2023-01-03",
    )["censor_reason"] == "ROLL_BOUNDARY_BEFORE_EXIT"
    session = path()
    session.loc[1:, "session_id"] = "2023-01-04"
    assert router.next_open_fill(
        session,
        decision_time=decision,
        direction=1,
        stop_distance=0.60,
        tick_size=0.25,
        expected_contract="MNQH3",
        expected_session_date="2023-01-03",
        expected_session_id="2023-01-03",
    )["censor_reason"] == "SESSION_ID_BOUNDARY_BEFORE_EXIT"
    gap = path(2)
    gap.loc[1, ["open", "high", "low", "close"]] = [98.75, 99.0, 98.5, 98.8]
    gap_result = router.next_open_fill(
        gap,
        decision_time=decision,
        direction=1,
        stop_distance=0.60,
        tick_size=0.25,
        expected_contract="MNQH3",
        expected_session_date="2023-01-03",
        expected_session_id="2023-01-03",
    )
    assert gap_result["terminal"] == "STOP_GAP_ADVERSE_OPEN"
    assert gap_result["exit_price"] == 98.75


def test_mae_stops_at_actual_exit_bar() -> None:
    decision = pd.Timestamp("2023-01-03T14:35:00Z")
    path = pd.DataFrame(
        {
            "timestamp": pd.date_range(decision, periods=3, freq="min"),
            "open": [100.0, 100.0, 100.0],
            "high": [100.5, 102.0, 101.0],
            "low": [99.8, 99.5, 90.0],
            "close": [100.1, 101.8, 90.0],
        }
    )
    result = router.next_open_fill(
        path,
        decision_time=decision,
        direction=1,
        stop_distance=1.0,
        tick_size=0.25,
    )
    assert result["terminal"] == "TARGET"
    assert result["mae_points"] == pytest.approx(-0.5)


def test_router_uses_discovery_only_and_emits_at_most_one_event_per_day() -> None:
    records = []
    for day_index in range(1, 33):
        day = f"2023-02-{day_index:02d}" if day_index <= 28 else f"2023-03-{day_index - 28:02d}"
        for market_index, market in enumerate(router.MARKETS):
            records.append(
                {
                    "session_date": day,
                    "market": market,
                    "active_contract": f"{market}H3",
                    "temporal_role": "DISCOVERY",
                    "available_at": pd.Timestamp(f"{day}T14:35:00Z"),
                    "decision_time": pd.Timestamp(f"{day}T14:35:00Z"),
                    "feature_vector": [float(market_index), float(day_index) / 100.0],
                    "own_path_return": 0.01,
                    "outcome_-1": _outcome(False),
                    "outcome_+1": _outcome(True),
                }
            )
    rule = router.AnalogRule("synthetic", router.PANELS[0], "08:35", analog_k=25, lcb_threshold=0.0)
    first = router.route_analog_events(records, rule=rule)
    second = router.route_analog_events(records, rule=rule)
    assert len({row["session_date"] for row in first}) == len(first)
    assert all(row["library_role"] == "DISCOVERY_ONLY" for row in first)
    assert all(row["library_latest_day"] < row["session_date"] for row in first)
    assert stable_hash(router._json_safe_events(first)) == stable_hash(router._json_safe_events(second))


def test_future_outcome_censor_preserves_signal_and_censors_account_window() -> None:
    days = [day.strftime("%Y-%m-%d") for day in pd.bdate_range("2023-02-01", periods=30)]
    records: list[dict[str, object]] = []
    censored = {
        "status": "DATA_CENSORED",
        "censor_reason": "INCOMPLETE_FULL_HOLDING_PATH",
        "target_first": None,
        "economic_outcome_materialized": False,
        "declared_stop_risk_usd_per_micro": 5.0,
        "normal_cost_usd_per_micro": 3.0,
        "stressed_cost_usd_per_micro": 4.5,
    }
    for day_index, day in enumerate(days):
        for market_index, market in enumerate(router.MARKETS):
            records.append(
                {
                    "session_date": day,
                    "market": market,
                    "active_contract": f"{market}H3",
                    "temporal_role": "DISCOVERY",
                    "available_at": pd.Timestamp(f"{day}T14:35:00Z"),
                    "decision_time": pd.Timestamp(f"{day}T14:35:00Z"),
                    "feature_vector": [float(market_index), float(day_index) / 100.0],
                    "own_path_return": 0.01,
                    "outcome_-1": _outcome(False),
                    "outcome_+1": (
                        dict(censored) if day_index >= 25 else _outcome(True)
                    ),
                }
            )
    rule = router.AnalogRule(
        "censored-signal",
        router.PANELS[0],
        "08:35",
        analog_k=25,
        lcb_threshold=0.0,
    )
    routed = router.route_analog_events(records, rule=rule)
    assert len(routed) == 5
    assert all(row["outcome_status"] == "DATA_CENSORED" for row in routed)
    assert all(row["economic_outcome_materialized"] is False for row in routed)
    ledgers = router._matched_control_ledgers(routed)
    cell = router._evaluate_account_cell(
        ledgers,
        account_label="50K",
        account_rule=_account_rule(),
        risk_fraction=0.1,
        role_days={"DISCOVERY": days, "VALIDATION": [], "FINAL_DEVELOPMENT": []},
        full_coverage_days={
            "DISCOVERY": set(days),
            "VALIDATION": set(),
            "FINAL_DEVELOPMENT": set(),
        },
    )
    five = cell["evaluations"]["PRIMARY"]["DISCOVERY"]["5"]
    assert five["data_censored_start_count"] == 1
    assert five["censored_start_ledger"][0]["future_outcome_censored_days"] == days[-5:]
    selected = {
        "account_label": "50K",
        "account_rule_snapshot": cell["account_rule_snapshot"],
        "risk_fraction_of_current_mll_buffer": 0.1,
        "cell_hash": cell["cell_hash"],
        "evaluations": cell["evaluations"],
        "selection_role": "DISCOVERY_ONLY",
        "control_quantity_policy": cell["control_quantity_policy"],
        "control_exposure_matched": True,
    }
    card = router.load_decision_card(ROOT / router.DEFAULT_CARD)
    bundle = router._build_candidate_evidence_bundle(
        rule=rule,
        routed=routed,
        control_ledgers=ledgers,
        selected_account_cell=selected,
        card=card,
        source_commit="0" * 40,
        production_manifest_hash="f" * 64,
    )
    assert bundle["routed_event_ledgers"]["PRIMARY"]
    assert bundle["routed_trade_ledgers"]["PRIMARY"]["NORMAL"] == []
    assert bundle["canonical_evidence_material"] is None
    assert bundle["complete"] is False
    assert "PRIMARY:NO_MATERIALIZED_TRADE" in bundle[
        "materialization_exclusion_reasons"
    ]


def test_controls_preserve_exact_opportunity_identity() -> None:
    event = _primary_event()
    ledgers = router._matched_control_ledgers([event])
    assert set(ledgers) == set(router.CONTROLS)
    identities = [
        (rows[0]["session_date"], rows[0]["market"], rows[0]["decision_time"])
        for rows in ledgers.values()
    ]
    assert len(set(identities)) == 1


def test_canonical_calendar_does_not_compress_incomplete_days() -> None:
    days = [f"2023-01-{day:02d}" for day in range(1, 11)]
    ledgers = {control: [] for control in router.CONTROLS}
    full = set(days)
    full.remove(days[2])
    cell = router._evaluate_account_cell(
        ledgers,
        account_label="50K",
        account_rule=_account_rule(),
        risk_fraction=0.1,
        role_days={"DISCOVERY": days, "VALIDATION": [], "FINAL_DEVELOPMENT": []},
        full_coverage_days={"DISCOVERY": full, "VALIDATION": set(), "FINAL_DEVELOPMENT": set()},
    )
    five = cell["evaluations"]["PRIMARY"]["DISCOVERY"]["5"]
    assert five["all_start_count"] == 2
    assert five["full_coverage_start_count"] == 1
    assert five["data_censored_start_count"] == 1
    assert five["censored_start_ledger"][0]["incomplete_session_days"] == [days[2]]
    assert router._actual_prior_session(
        "2023-01-04", ["2023-01-01", "2023-01-02", "2023-01-03"]
    ) == "2023-01-03"


def test_matched_controls_use_primary_decision_time_quantities() -> None:
    ledgers = router._matched_control_ledgers([_primary_event()])
    days = [f"2023-01-{day:02d}" for day in range(3, 8)]
    cell = router._evaluate_account_cell(
        ledgers,
        account_label="50K",
        account_rule=_account_rule(),
        risk_fraction=0.1,
        role_days={"DISCOVERY": days, "VALIDATION": [], "FINAL_DEVELOPMENT": []},
    )
    assert cell["control_exposure_matched"] is True
    primary = cell["evaluations"]["PRIMARY"]["DISCOVERY"]["5"]["NORMAL"][
        "episode_ledger"
    ][0]["quantity_by_event"]
    for control in router.CONTROLS[1:]:
        compared = cell["evaluations"][control]["DISCOVERY"]["5"]["NORMAL"][
            "episode_ledger"
        ][0]["quantity_by_event"]
        assert compared == primary


def test_breaching_trade_is_booked_before_terminal_and_equity_reconciles() -> None:
    event = _primary_event()
    event["normal_cost_usd_per_micro"] = 0.0
    event["stressed_cost_usd_per_micro"] = 0.0
    event["outcome"] = {
        **_outcome(False),
        "gross_usd_per_micro": -100.0,
        "declared_stop_risk_usd_per_micro": 100.0,
        "mae_points": -400.0,
        "mfe_points": 0.0,
    }
    episode = router._replay_dynamic_account_episode(
        [event],
        episode_days=["2023-01-03"],
        scenario="NORMAL",
        account_rule=_account_rule(),
        risk_fraction=0.3,
    )
    assert episode["quantity_by_event"] == {"e1": 6}
    assert episode["terminal"] == "MLL_BREACHED"
    assert episode["net_pnl_usd"] == -600.0
    assert episode["ending_equity_usd"] == 49_400.0
    assert episode["daily_path"][0]["balance"] == 49_400.0
    assert episode["ending_equity_usd"] == 50_000.0 + episode["net_pnl_usd"]


def test_complete_evidence_bundle_contains_identity_and_row_material() -> None:
    event = _primary_event()
    ledgers = router._matched_control_ledgers([event])
    days = [f"2023-01-{day:02d}" for day in range(3, 13)]
    full_days = set(days)
    full_days.remove(days[7])
    account_cell = router._evaluate_account_cell(
        ledgers,
        account_label="50K",
        account_rule=_account_rule(),
        risk_fraction=0.1,
        role_days={"DISCOVERY": days, "VALIDATION": [], "FINAL_DEVELOPMENT": []},
        full_coverage_days={
            "DISCOVERY": full_days,
            "VALIDATION": set(),
            "FINAL_DEVELOPMENT": set(),
        },
    )
    selected = {
        "account_label": "50K",
        "account_rule_snapshot": account_cell["account_rule_snapshot"],
        "risk_fraction_of_current_mll_buffer": 0.1,
        "cell_hash": account_cell["cell_hash"],
        "evaluations": account_cell["evaluations"],
        "selection_role": "DISCOVERY_ONLY",
        "control_quantity_policy": account_cell["control_quantity_policy"],
        "control_exposure_matched": True,
    }
    card = router.load_decision_card(ROOT / router.DEFAULT_CARD)
    rule = router.AnalogRule("c1", router.PANELS[0], "08:35")
    bundle = router._build_candidate_evidence_bundle(
        rule=rule,
        routed=[event],
        control_ledgers=ledgers,
        selected_account_cell=selected,
        card=card,
        source_commit="0" * 40,
        production_manifest_hash="f" * 64,
    )
    core = dict(bundle)
    claimed = core.pop("evidence_bundle_hash")
    assert stable_hash(core) == claimed
    assert bundle["row_counts"]["routed_event_rows"] == 5
    assert bundle["row_counts"]["routed_trade_rows"] == 10
    assert set(bundle["routed_event_ledgers"]) == set(router.CONTROLS)
    for control in router.CONTROLS:
        assert set(bundle["routed_trade_ledgers"][control]) == {
            "NORMAL",
            "STRESSED_1_5X",
        }
    episode = bundle["account_episode_material"]["evaluations"]["PRIMARY"][
        "DISCOVERY"
    ]["5"]["NORMAL"]["episode_ledger"][0]
    assert episode["daily_path"]
    assert episode["quantity_ledger"]
    assert bundle["account_episode_material"]["evaluations"]["PRIMARY"][
        "DISCOVERY"
    ]["5"]["censored_start_ledger"]
    canonical = bundle["canonical_evidence_material"]
    assert canonical["contract"] == "HYDRA_EVIDENCE_BUNDLE_V1"
    assert canonical["identity"]["campaign_id"] == router.CAMPAIGN_ID
    assert canonical["adapter_requires_economic_replay"] is False
    assert canonical["datasets"]["component_trades"]
    assert canonical["datasets"]["account_daily_paths"]
    assert canonical["datasets"]["episodes"]
    assert canonical["censored_start_ledgers"]
    assert all(
        int(row["duration_trading_days"]) > 0 for row in canonical["datasets"]["episodes"]
    )
    assert _validate_relational_contract(
        identity=canonical["identity"], records=canonical["datasets"]
    ) is False
    second = router._build_candidate_evidence_bundle(
        rule=router.AnalogRule("c2", router.PANELS[1], "08:35"),
        routed=[event],
        control_ledgers=ledgers,
        selected_account_cell=selected,
        card=card,
        source_commit="0" * 40,
        production_manifest_hash="f" * 64,
    )["canonical_evidence_material"]
    merged = router._merge_canonical_evidence_materials([canonical, second])
    assert merged["identity"]["campaign_id"] == router.CAMPAIGN_ID
    assert len(merged["identity"]["policy_fingerprints"]) == 10
    assert len(merged["datasets"]["provenance"]) == 1
    assert _validate_relational_contract(
        identity=merged["identity"], records=merged["datasets"]
    ) is False


def test_static_future_scan_rejects_forbidden_transforms() -> None:
    assert router.static_future_dependency_scan("x = series.shift(1)")["passed"]
    assert not router.static_future_dependency_scan("x = series.shift(-1)")["passed"]
    assert not router.static_future_dependency_scan("x = series.bfill()")["passed"]
    assert not router.static_future_dependency_scan("x = series.rolling(5, center=True)")["passed"]


def test_audit_is_metadata_only_and_governance_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("audit decoded economic Parquet rows")

    monkeypatch.setattr(pd, "read_parquet", forbidden)
    result = router.audit_inputs(ROOT)
    assert result["economic_rows_read"] == 0
    assert result["economic_outcomes_read"] == 0
    assert result["parquet_row_groups_decoded"] == 0
    assert result["tier_ceiling"] == "E"
    assert result["tier_q_allowed"] is False
    assert result["promotion_allowed"] is False
    assert result["q4_access_count_delta"] == 0
    assert result["broker_connections"] == 0
    assert result["orders"] == 0
