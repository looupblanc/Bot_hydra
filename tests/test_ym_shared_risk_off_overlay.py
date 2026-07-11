from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.ym_shared_risk_off_overlay import (
    _matched_random_skip_controls,
    _overlay_metrics,
    _shadow_specification_for_child,
    apply_risk_off_overlay,
    build_shared_risk_state,
    past_only_percentile,
)


def test_past_only_percentile_is_prefix_invariant_and_excludes_current_value() -> None:
    values = pd.Series([3.0, 1.0, 2.0, 100.0, -5.0, 8.0])
    complete = past_only_percentile(values, minimum_history=3)
    prefix = past_only_percentile(values.iloc[:4], minimum_history=3)

    pd.testing.assert_series_equal(complete.iloc[:4], prefix)
    assert np.isnan(complete.iloc[2])
    assert complete.iloc[3] == 1.0


def test_shared_risk_state_uses_closed_bars_and_handles_dst() -> None:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2023-01-03", periods=52)
    for symbol_index, symbol in enumerate(("ES", "NQ", "RTY", "YM")):
        counter = 0
        for date in dates:
            local_end = pd.Timestamp(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=8,
                minute=30,
                tz="America/Chicago",
            )
            for timestamp in pd.date_range(local_end - pd.Timedelta(minutes=120), local_end, freq="1min"):
                rows.append(
                    {
                        "timestamp": timestamp.tz_convert("UTC"),
                        "symbol": symbol,
                        "active_contract": f"{symbol}H3",
                        "close": 1000.0 + 100.0 * symbol_index + 0.02 * counter + np.sin(counter / 17),
                    }
                )
                counter += 1
    state = build_shared_risk_state(pd.DataFrame(rows), minimum_history=40)
    usable = state[state["shared_risk_score"].notna()]

    assert len(state) == len(dates)
    assert not usable.empty
    assert state["market_count"].eq(4).all()
    assert state["complete_market_count"].eq(4).all()
    assert (state["source_bar_close"] == state["source_bar_start"] + pd.Timedelta(minutes=1)).all()
    assert (state["availability_timestamp"] == state["decision_timestamp"]).all()
    local_starts = state["source_bar_start"].dt.tz_convert("America/Chicago")
    assert local_starts.dt.hour.eq(8).all()
    assert local_starts.dt.minute.eq(30).all()
    assert len(set(state["source_bar_start"].dt.hour)) == 2  # CST and CDT UTC offsets.


def test_overlay_can_only_remove_parent_events_and_missing_state_fails_closed() -> None:
    decisions = pd.to_datetime(
        ["2024-01-02 14:31:00+00:00", "2024-01-03 14:31:00+00:00", "2024-01-04 14:31:00+00:00"]
    )
    parent = pd.DataFrame(
        {
            "decision_timestamp": decisions,
            "timestamp": decisions - pd.Timedelta(minutes=1),
            "symbol": ["YM"] * 3,
            "net_pnl_60": [100.0, -50.0, 70.0],
            "cost": [14.5] * 3,
            "active_contract": ["YMH4"] * 3,
        }
    )
    state = pd.DataFrame(
        {
            "decision_timestamp": decisions[:2],
            "source_bar_start": decisions[:2] - pd.Timedelta(minutes=1),
            "source_bar_close": decisions[:2],
            "availability_timestamp": decisions[:2],
            "prior_decision_count": [40, 41],
            "mean_past_volatility_percentile": [0.2, 0.9],
            "mean_downside_state_percentile": [0.3, 0.9],
            "cross_market_dispersion_percentile": [0.4, 0.9],
            "shared_risk_score": [0.3, 0.9],
            "transformation_version": ["test", "test"],
        }
    )
    overlaid = apply_risk_off_overlay(parent, state, threshold=0.8)

    assert len(overlaid) == len(parent)
    assert overlaid["retained"].tolist() == [True, False, False]
    assert overlaid["net_pnl_60"].tolist() == parent["net_pnl_60"].tolist()
    assert overlaid["cost"].tolist() == parent["cost"].tolist()


def test_preregistered_defensive_utility_and_random_controls_are_deterministic() -> None:
    timestamps = pd.to_datetime(
        ["2023-07-03", "2024-01-03", "2024-04-03", "2024-07-03"], utc=True
    )
    parent = pd.DataFrame(
        {
            "timestamp": timestamps,
            "event_session_id": ["2023-07-03", "2024-01-03", "2024-04-03", "2024-07-03"],
            "net_pnl_60": [100.0, -200.0, 100.0, 100.0],
            "gross_pnl_60": [110.0, -190.0, 110.0, 110.0],
            "cost": [10.0] * 4,
            "mae_dollars": [-20.0, -250.0, -30.0, -25.0],
        }
    )
    overlay = parent.copy()
    overlay["retained"] = [True, False, True, True]

    metrics = _overlay_metrics(parent, overlay)
    first = _matched_random_skip_controls(parent, overlay, random_seed=7, draws=128)
    second = _matched_random_skip_controls(parent, overlay, random_seed=7, draws=128)

    assert metrics["primary_utility_success"]
    assert metrics["maximum_drawdown_reduction_fraction"] == 1.0
    assert first == second


def test_child_shadow_specification_is_fail_closed_and_orderless() -> None:
    specification = _shadow_specification_for_child(preregistration_hash="a" * 64)
    specification.validate()

    assert not specification.outbound_orders_enabled
    assert specification.entry_rules["activation"] == "shared_risk_score_lt_0.80"
    assert specification.entry_rules["missing_risk_state_policy"] == "fail_closed_skip_signal"
    assert "manual_kill_switch" in specification.kill_conditions
