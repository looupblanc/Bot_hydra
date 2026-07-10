from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hydra.mission.calibration_retest_execution import (
    FOLDS,
    _add_selected_past_only_features,
    _apply_benjamini_hochberg,
    _finalize_decision,
    _future_target,
    _globex_session_fields,
    _clustered_contract_bootstrap,
    _matching_fields,
    _matching_covariates,
    _trading_day_window,
)
from hydra.atoms.atom_library import add_atom_features


def test_future_return_never_bleeds_across_contract_or_session() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["ES"] * 6,
            "active_contract": ["ESH4", "ESH4", "ESM4", "ESM4", "ESM4", "ESM4"],
            "session_date": ["2024-03-01", "2024-03-01", "2024-03-01", "2024-03-01", "2024-03-02", "2024-03-02"],
            "trading_session_id": ["2024-03-01", "2024-03-01", "2024-03-01", "2024-03-01", "2024-03-02", "2024-03-02"],
            "contiguous_segment_id": ["a", "a", "b", "b", "c", "c"],
            "timestamp": pd.to_datetime(
                [
                    "2024-03-01T10:00Z",
                    "2024-03-01T10:01Z",
                    "2024-03-01T10:02Z",
                    "2024-03-01T10:03Z",
                    "2024-03-02T10:00Z",
                    "2024-03-02T10:01Z",
                ],
                utc=True,
            ),
            "close": [100.0, 101.0, 1000.0, 1010.0, 2000.0, 2020.0],
            "low": [99.0, 100.0, 999.0, 1009.0, 1999.0, 2019.0],
            "past_volatility": [0.01] * 6,
        }
    )
    target = _future_target(frame, 1, defensive=False)
    assert target.iloc[0] == pytest.approx(0.01)
    assert np.isnan(target.iloc[1])
    assert target.iloc[2] == pytest.approx(0.01)
    assert np.isnan(target.iloc[3])
    assert target.iloc[4] == pytest.approx(0.01)
    assert np.isnan(target.iloc[5])


def test_defensive_target_is_binary_hazard_not_signed_mae() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["NQ"] * 3,
            "active_contract": ["NQH4"] * 3,
            "session_date": ["2024-03-01"] * 3,
            "trading_session_id": ["2024-03-01"] * 3,
            "contiguous_segment_id": ["a"] * 3,
            "timestamp": pd.date_range("2024-03-01T10:00Z", periods=3, freq="1min"),
            "close": [100.0, 100.0, 100.0],
            "low": [100.0, 95.0, 100.0],
            "past_volatility": [0.01, 0.01, 0.01],
        }
    )
    target = _future_target(frame, 1, defensive=True)
    assert target.iloc[0] == 1.0
    assert target.iloc[1] == 0.0
    assert np.isnan(target.iloc[2])


def test_future_target_never_crosses_a_missing_bar_segment() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["ES"] * 4,
            "active_contract": ["ESH4"] * 4,
            "trading_session_id": ["2024-03-01"] * 4,
            "contiguous_segment_id": ["a", "a", "b", "b"],
            "timestamp": pd.to_datetime(
                ["2024-03-01T20:58Z", "2024-03-01T20:59Z", "2024-03-01T22:00Z", "2024-03-01T22:01Z"],
                utc=True,
            ),
            "close": [100.0, 101.0, 1000.0, 1010.0],
            "low": [99.0, 100.0, 999.0, 1009.0],
            "past_volatility": [0.01] * 4,
        }
    )
    target = _future_target(frame, 1, defensive=False)
    assert target.iloc[0] == pytest.approx(0.01)
    assert np.isnan(target.iloc[1])
    assert target.iloc[2] == pytest.approx(0.01)
    assert np.isnan(target.iloc[3])


def test_multiplicity_uses_full_selection_universe() -> None:
    rows = [
        {"cluster_one_sided_p_value": 0.001},
        {"cluster_one_sided_p_value": 0.01},
    ]
    _apply_benjamini_hochberg(rows, selection_universe_size=25)
    assert rows[0]["selection_universe_bonferroni_p_value"] == 0.025
    assert rows[1]["selection_universe_bonferroni_p_value"] == 0.25
    assert all(row["selection_universe_size"] == 25 for row in rows)


def test_invariant_control_cannot_be_silently_insufficient() -> None:
    result = {"status": "ATOM_RETEST_INSUFFICIENT_EVIDENCE", "reason": "few clusters"}
    atom = {"selection_role": "CALIBRATION_INVARIANT_OLD_FAILURE"}
    finalized = _finalize_decision(result, atom, validator_controls_passed=True)
    assert finalized["status"] == "INVARIANT_CONTROL_INSUFFICIENT"


def test_full_matched_null_contract_includes_required_confounders() -> None:
    fields = set(_matching_fields("full"))
    assert {
        "symbol",
        "explicit_contract",
        "fold",
        "15_minute_session_phase",
        "volatility_bin",
        "prior_displacement_bin",
        "participation_bin",
        "horizon",
        "causal_past_opportunity_frequency_bin",
    } <= fields
    assert set(_matching_fields("session")) != set(_matching_fields("volatility"))
    assert "15_minute_session_phase" in _matching_fields("session")
    assert "volatility_bin" not in _matching_fields("session")
    assert "volatility_bin" in _matching_fields("volatility")
    assert "15_minute_session_phase" not in _matching_fields("volatility")
    assert _matching_covariates("session") == [
        "session_phase_15m",
        "past_opportunity_frequency",
    ]
    assert _matching_covariates("volatility") == [
        "past_volatility",
        "past_opportunity_frequency",
    ]
    assert set(_matching_covariates("full")) == {
        "past_volatility",
        "past_return_60",
        "past_participation",
        "past_opportunity_frequency",
        "session_phase_15m",
    }


def test_hazard_bootstrap_is_one_joint_event_and_control_day_resample() -> None:
    events = pd.DataFrame(
        {
            "cluster": ["NQ|2024-01-02", "RTY|2024-01-02", "NQ|2024-01-03", "RTY|2024-01-03"],
            "trading_session_id": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "paired_effect": [0.20, 0.10, 0.30, 0.20],
            "target": [1.0, 1.0, 1.0, 1.0],
            "matched_counterfactual_target": [0.2, 0.4, 0.2, 0.4],
            "matched_control_targets": [[0, 0, 0, 0, 1]] * 4,
            "matched_control_session_ids": [
                ["2023-12-20", "2023-12-21", "2023-12-22", "2023-12-26", "2023-12-27"],
                ["2023-12-20", "2023-12-21", "2023-12-22", "2023-12-26", "2023-12-27"],
                ["2023-12-28", "2023-12-29", "2024-01-02", "2024-01-03", "2024-01-04"],
                ["2023-12-28", "2023-12-29", "2024-01-02", "2024-01-03", "2024-01-04"],
            ],
        }
    )
    result = _clustered_contract_bootstrap(
        events, seed=17, repetitions=199, value_column="paired_effect", hazard_pair=True
    )
    assert result["method"] == "joint_two_way_pigeonhole_globex_event_day_and_matched_control_day_bootstrap"
    assert result["joint_two_way_bootstrap_repetitions"] == 199
    assert result["cross_market_sessions_resampled_jointly"] is True
    assert len(result["_draws"]) == 199


def test_globex_session_spans_utc_midnight_but_not_maintenance_gap() -> None:
    timestamps = pd.Series(
        pd.to_datetime(
            [
                "2024-07-01T22:00:00Z",  # 17:00 Chicago: July 2 trading session opens
                "2024-07-01T23:59:00Z",
                "2024-07-02T00:00:00Z",
                "2024-07-02T20:59:00Z",  # 15:59 Chicago, same trading day
                "2024-07-02T22:00:00Z",  # next trading session after maintenance
            ],
            utc=True,
        )
    )
    sessions, phases = _globex_session_fields(timestamps)
    assert sessions.iloc[:4].tolist() == ["2024-07-02"] * 4
    assert sessions.iloc[4] == "2024-07-03"
    assert phases.iloc[0] == 0
    assert phases.iloc[2] > phases.iloc[1]


def test_complete_globex_session_belongs_to_only_one_temporal_fold() -> None:
    timestamps = pd.Series(
        pd.to_datetime(
            [
                "2024-03-31T23:59:00Z",
                "2024-04-01T00:00:00Z",
                "2024-04-01T00:01:00Z",
            ],
            utc=True,
        )
    )
    trading_day, _phase = _globex_session_fields(timestamps)
    assert trading_day.tolist() == ["2024-04-01", "2024-04-01", "2024-04-01"]

    frame = pd.DataFrame({"trading_session_id": trading_day})
    memberships: dict[str, list[bool]] = {}
    for fold_name, _train_start, _train_end, test_start, test_end in FOLDS:
        memberships[fold_name] = _trading_day_window(frame, test_start, test_end).tolist()

    assert memberships["2024_q1"] == [False, False, False]
    assert memberships["2024_q2"] == [True, True, True]
    assert all(
        sum(bool(memberships[fold_name][row]) for fold_name, *_bounds in FOLDS) == 1
        for row in range(len(timestamps))
    )


def test_vectorized_selected_features_match_reference_on_one_contiguous_segment() -> None:
    rng = np.random.default_rng(44)
    rows = 360
    returns = rng.normal(0.0, 0.0005, size=rows)
    close = 5000.0 * np.exp(np.cumsum(returns))
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-08T23:00Z", periods=rows, freq="1min"),
            "symbol": "ES",
            "active_contract": "ESH4",
            "trading_session_id": "2024-01-09",
            "contiguous_segment_id": "2024-01-09|0",
            "feature_group_id": "ES|ESH4|2024-01-09|0",
            "open": close,
            "high": close * 1.0001,
            "low": close * 0.9999,
            "close": close,
            "volume": rng.integers(10, 1000, size=rows),
        }
    )
    reference = add_atom_features(frame.copy())
    actual = _add_selected_past_only_features(frame.copy())
    for column in (
        "old_region_reentry",
        "directional_pressure_without_progress",
        "shared_loss_risk_state",
        "failed_expansion",
        "extreme_dwell",
        "rv_short_long_ratio",
    ):
        left = pd.to_numeric(reference[column], errors="coerce").to_numpy(dtype=float)
        right = pd.to_numeric(actual[column], errors="coerce").to_numpy(dtype=float)
        assert np.array_equal(np.isnan(left), np.isnan(right)), column
        finite = np.isfinite(left) & np.isfinite(right)
        assert np.allclose(left[finite], right[finite], rtol=1e-12, atol=1e-12), column
