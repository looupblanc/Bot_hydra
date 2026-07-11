from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.qd_economic_tournament import (
    _benjamini_hochberg,
    _causal_threshold,
    _prepare_execution_cache,
    _round_turn_cost_all,
    _shadow_specification,
    attach_event_mae,
    build_market_path_cache,
    build_prototype_events,
    generate_prototypes,
    select_balanced_elites,
)


def test_population_has_exactly_540_unique_structures() -> None:
    population = generate_prototypes()

    assert len(population) == 540
    assert len({item["candidate_id"] for item in population}) == 540
    assert len({item["structural_fingerprint"] for item in population}) == 540
    assert {item["market"] for item in population} == {"ES", "NQ", "RTY", "YM", "GC", "CL"}
    assert _round_turn_cost_all("CL") == 24.5
    assert _round_turn_cost_all("MCL") == 4.0


def test_causal_threshold_is_prefix_invariant_and_excludes_current_observation() -> None:
    values = pd.Series(np.linspace(0.0, 10.0, 700))
    full = _causal_threshold(values, 0.65)
    prefix = _causal_threshold(values.iloc[:620], 0.65)

    pd.testing.assert_series_equal(full.iloc[:620], prefix)
    assert np.isnan(full.iloc[499])
    expected = float(values.iloc[:500].abs().quantile(0.65))
    assert full.iloc[500] == expected


def test_prototype_execution_is_one_bar_delayed_and_session_bounded() -> None:
    rows: list[dict[str, object]] = []
    global_position = 0
    for session_number, date in enumerate(pd.bdate_range("2023-07-03", periods=5)):
        local_start = pd.Timestamp(
            year=date.year,
            month=date.month,
            day=date.day,
            hour=8,
            minute=30,
            tz="America/Chicago",
        )
        for minute in range(180):
            timestamp = (local_start + pd.Timedelta(minutes=minute)).tz_convert("UTC")
            feature = 0.05 + 0.001 * (global_position % 7)
            if session_number >= 3 and minute in {20, 80}:
                feature = 4.0
            rows.append(
                {
                    "timestamp": timestamp,
                    "symbol": "ES",
                    "active_contract": "ESU3",
                    "trading_session_id": date.strftime("%Y-%m-%d"),
                    "contiguous_segment_id": session_number,
                    "symbol_position": minute,
                    "local_minute": 8 * 60 + 30 + minute,
                    "market_open_minute": 8 * 60 + 30,
                    "market_session_length": 390,
                    "minutes_from_market_open": minute,
                    "open": 4500.0 + global_position * 0.01,
                    "high": 4500.5 + global_position * 0.01,
                    "low": 4499.5 + global_position * 0.01,
                    "close": 4500.1 + global_position * 0.01,
                    "old_region_reentry": feature,
                    "past_return_60": 0.001,
                }
            )
            global_position += 1
    frame = _prepare_execution_cache(pd.DataFrame(rows))
    prototype = next(
        item
        for item in generate_prototypes()
        if item["market"] == "ES"
        and item["feature"] == "old_region_reentry"
        and item["policy_direction"] == "continuation"
        and item["profile"] == "open_q65_h15"
    )
    events = build_prototype_events(frame, prototype)
    events = attach_event_mae(events, frame)

    assert not events.empty
    assert (events["decision_timestamp"] == events["timestamp"] + pd.Timedelta(minutes=1)).all()
    assert (events["entry_timestamp"] == events["decision_timestamp"] + pd.Timedelta(minutes=1)).all()
    assert (events["exit_timestamp"] == events["entry_timestamp"] + pd.Timedelta(minutes=15)).all()
    assert events["active_contract"].eq("ESU3").all()
    assert np.isfinite(events["mae_dollars"]).all()
    assert events.groupby("event_session_id").apply(
        lambda group: (group["entry_timestamp"].iloc[1:].to_numpy() >= group["exit_timestamp"].iloc[:-1].to_numpy()).all(),
        include_groups=False,
    ).all()


def test_market_path_cache_preserves_event_mae_exactly() -> None:
    rows: list[dict[str, object]] = []
    session = "2023-08-01"
    for minute in range(30):
        timestamp = pd.Timestamp("2023-08-01 13:30:00+00:00") + pd.Timedelta(
            minutes=minute
        )
        rows.append(
            {
                "timestamp": timestamp,
                "symbol": "ES",
                "active_contract": "ESU3",
                "trading_session_id": session,
                "contiguous_segment_id": 0,
                "low": 4499.0 - minute * 0.05,
                "high": 4501.0 + minute * 0.05,
            }
        )
    frame = pd.DataFrame(rows)
    events = pd.DataFrame(
        [
            {
                "symbol": "ES",
                "active_contract": "ESU3",
                "trading_session_id": session,
                "contiguous_segment_id": 0,
                "entry_timestamp": rows[5]["timestamp"],
                "exit_timestamp": rows[15]["timestamp"],
                "point_value": 50.0,
                "entry_price": 4500.0,
                "side": 1,
                "cost": 9.0,
            },
            {
                "symbol": "ES",
                "active_contract": "ESU3",
                "trading_session_id": session,
                "contiguous_segment_id": 0,
                "entry_timestamp": rows[15]["timestamp"],
                "exit_timestamp": rows[25]["timestamp"],
                "point_value": 50.0,
                "entry_price": 4500.0,
                "side": -1,
                "cost": 9.0,
            },
        ]
    )

    uncached = attach_event_mae(events, frame)
    cached = attach_event_mae(events, frame, path_cache=build_market_path_cache(frame))

    pd.testing.assert_frame_equal(cached, uncached)


def test_balanced_selection_enforces_actual_share_caps() -> None:
    ecologies = ("equity_indices", "metals", "energy")
    families = (
        "market_state_geometry",
        "distributional_risk_hazard",
        "volatility_state_transition",
        "invariant_price_state",
    )
    markets = ("ES", "NQ", "GC", "MGC", "CL", "MCL")
    survivors = []
    for index in range(48):
        survivors.append(
            {
                "candidate_id": f"candidate_{index}",
                "lineage_id": f"lineage_{index}",
                "market_ecology": ecologies[index % len(ecologies)],
                "mechanism_family": families[index % len(families)],
                "market": markets[index % len(markets)],
                "profile": f"profile_{index}",
                "portfolio_role": "trend" if index % 2 else "reversal",
                "discovery": {
                    "cost_stress_1_5x_net": 1000.0 - index,
                    "net_pnl": 1200.0 - index,
                    "maximum_drawdown": 100.0 + index,
                    "best_positive_event_share": 0.1,
                    "events": 40,
                },
            }
        )
    selected, audit = select_balanced_elites(survivors)

    assert len(selected) <= 24
    assert audit["maximum_ecology_share"] <= 0.35
    assert audit["maximum_family_share"] <= 0.25
    assert audit["maximum_market_share"] <= 0.25
    assert audit["unique_lineages"] == len(selected)


def test_bh_adjustment_and_shadow_spec_are_deterministic_and_orderless() -> None:
    adjusted = _benjamini_hochberg([0.01, 0.04, 0.03, 0.20])
    assert np.allclose(adjusted, [0.04, 0.05333333333333334, 0.05333333333333334, 0.20])
    prototype = generate_prototypes()[0]
    specification = _shadow_specification(prototype, selection_manifest_hash="a" * 64)
    specification.validate()

    assert not specification.outbound_orders_enabled
    assert specification.entry_rules["execution_delay_completed_bars"] == 1
    assert specification.entry_rules["missing_feature_policy"] == "fail_closed_skip_signal"
