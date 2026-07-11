from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.causal_transition_graph import (
    EXPECTED_POPULATION,
    STATES,
    build_source_state_table,
    build_transition_events,
    generate_hypotheses,
    transition_edge_statistics,
    validator_controls,
)


def _session_table(rows: int = 45) -> pd.DataFrame:
    sessions = pd.date_range("2023-01-02", periods=rows, freq="B")
    prior_trend = np.array([(-1.0 if index % 3 == 0 else 1.0) * (1 + index % 4) for index in range(rows)])
    prior_range = np.array([4.0 + index % 7 for index in range(rows)])
    entry = pd.to_datetime(sessions).tz_localize("UTC") + pd.Timedelta(hours=14)
    return pd.DataFrame(
        {
            "session_id": sessions.strftime("%Y-%m-%d"),
            "prior_trend": prior_trend,
            "prior_range": prior_range,
            "rth_high": 100.0 + np.arange(rows) + 2.0,
            "rth_low": 100.0 + np.arange(rows) - 2.0,
            "rth_close": 100.0 + np.arange(rows) + np.sign(prior_trend),
            "active_contract": "YMH3",
            "overnight_entry_timestamp": entry,
            "overnight_entry_price": 100.0 + np.arange(rows),
            "overnight_exit_timestamp_60": entry + pd.Timedelta(minutes=60),
            "overnight_exit_60": 100.5 + np.arange(rows),
            "overnight_long_mae_60": -0.25,
            "overnight_short_mae_60": -0.75,
            "overnight_exit_timestamp_120": entry + pd.Timedelta(minutes=120),
            "overnight_exit_120": 101.0 + np.arange(rows),
            "overnight_long_mae_120": -0.5,
            "overnight_short_mae_120": -1.0,
        }
    )


def test_population_is_exact_deterministic_and_lineage_aware() -> None:
    first = generate_hypotheses()
    second = generate_hypotheses()

    assert first == second
    assert len(first) == EXPECTED_POPULATION == 384
    assert len({row["candidate_id"] for row in first}) == EXPECTED_POPULATION
    assert len({row["structural_fingerprint"] for row in first}) == EXPECTED_POPULATION
    assert len({row["lineage_id"] for row in first}) == 192


def test_source_states_are_six_state_past_only_and_shifted() -> None:
    table = _session_table()
    states = build_source_state_table(table)

    assert set(states["source_state"].dropna()).issubset(STATES)
    valid = states.dropna(subset=["source_state"])
    assert not valid.empty
    assert (
        pd.to_datetime(valid["source_prior_session_id"])
        < pd.to_datetime(valid["session_id"])
    ).all()
    assert (
        pd.to_datetime(valid["state_threshold_history_end_session"])
        < pd.to_datetime(valid["session_id"])
    ).all()


def test_transition_events_use_only_completed_source_state() -> None:
    table = _session_table()
    state_table = build_source_state_table(table).set_index("session_id", drop=False)
    target = table.set_index("session_id", drop=False)
    aligned = target.join(
        state_table[
            [
                "source_prior_session_id",
                "source_state",
                "source_trend_ratio",
                "source_prior_range",
                "source_past_range_median",
                "state_threshold_history_end_session",
            ]
        ],
        how="left",
    )
    selected_state = str(aligned["source_state"].dropna().iloc[0])
    hypothesis = next(
        row
        for row in generate_hypotheses()
        if row["source_market"] == "YM"
        and row["target_market"] == "YM"
        and row["source_state"] == selected_state
        and row["side_name"] == "long"
        and row["horizon"] == 60
    )
    events = build_transition_events(
        {"YM": table}, hypothesis, cache={("YM", "YM"): aligned}
    )

    assert not events.empty
    assert set(events["source_state"]) == {selected_state}
    assert (
        pd.to_datetime(events["source_prior_session_id"])
        < pd.to_datetime(events["trading_session_id"])
    ).all()
    assert (
        pd.to_datetime(events["state_threshold_history_end_session"])
        < pd.to_datetime(events["trading_session_id"])
    ).all()


def test_transition_edge_probabilities_are_laplace_smoothed() -> None:
    events = pd.DataFrame({"gross_pnl": [1.0, 1.0, -1.0]})
    baseline = pd.DataFrame({"gross_pnl": [1.0, -1.0, -1.0, -1.0]})

    result = transition_edge_statistics(events, baseline)

    assert result["laplace_success_probability"] == 3 / 5
    assert result["unconditional_laplace_probability"] == 2 / 6
    assert result["edge_lift"] == (3 / 5) / (2 / 6)


def test_transition_validator_rejects_null_and_detects_injected_effect() -> None:
    controls = validator_controls()

    assert controls["negative_control_passed"] is True
    assert controls["injected_control_passed"] is True
    assert controls["passed"] is True

