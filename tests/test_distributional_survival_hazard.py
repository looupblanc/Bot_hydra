from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from hydra.research.distributional_survival_hazard import (
    FEATURE_COLUMNS,
    _candidate_specification,
    _fold_coefficient_stability,
    build_hazard_dataset,
    rolling_origin_predictions,
    validator_controls,
)


def _session_table(symbol: str, periods: int = 520) -> pd.DataFrame:
    sessions = pd.bdate_range("2023-01-02", periods=periods)
    prior_trend = np.sin(np.arange(periods) / 7.0)
    return pd.DataFrame(
        {
            "session_id": sessions.strftime("%Y-%m-%d"),
            "symbol": symbol,
            "prior_range": 10.0 + np.cos(np.arange(periods) / 9.0),
            "prior_trend": prior_trend,
            "rth_high": 110.0 + prior_trend,
            "rth_low": 100.0 - prior_trend,
            "rth_close": 105.0 + prior_trend,
            "overnight_entry_timestamp": sessions.tz_localize("UTC")
            + pd.Timedelta(hours=14),
            "overnight_long_mae_120": -1.0 - np.abs(prior_trend),
            "overnight_short_mae_120": -0.8 - np.abs(prior_trend) * 0.5,
        }
    )


def test_hazard_dataset_uses_shifted_prior_sessions_and_threshold() -> None:
    tables = {
        symbol: _session_table(symbol)
        for symbol in ("YM", "MYM", "RTY", "M2K", "CL", "MCL", "GC", "MGC")
    }

    dataset = build_hazard_dataset(tables, "YM")

    assert len(dataset) > 400
    assert set(FEATURE_COLUMNS).issubset(dataset)
    for source in ("YM", "RTY", "CL", "GC"):
        assert (
            pd.to_datetime(dataset[f"{source}_prior_session_id"])
            < pd.to_datetime(dataset["session_id"])
        ).all()
    first = dataset.iloc[0]
    source = tables["MYM"].copy()
    source["severity"] = np.maximum(
        -source["overnight_long_mae_120"] * 0.5,
        -source["overnight_short_mae_120"] * 0.5,
    )
    position = source.index[
        source["session_id"].eq(first["session_id"])
    ][0]
    expected = source.loc[: position - 1, "severity"].tail(40).quantile(0.80)
    assert first["past_only_tail_threshold"] == expected


def test_rolling_origin_never_trains_through_validation_boundary() -> None:
    sessions = pd.bdate_range("2023-01-02", "2024-09-30")
    rng = np.random.default_rng(7)
    dataset = pd.DataFrame(
        {
            "session_id": sessions.strftime("%Y-%m-%d"),
            "decision_timestamp": sessions.tz_localize("UTC"),
            "tail_event": (rng.normal(size=len(sessions)) > 0.7).astype(int),
            "tail_severity_dollars": rng.uniform(10, 200, size=len(sessions)),
            **{column: rng.normal(size=len(sessions)) for column in FEATURE_COLUMNS},
        }
    )

    predictions, manifests = rolling_origin_predictions(dataset)

    assert set(predictions["fold"]) == {"2024_q1", "2024_q2", "2024_q3"}
    assert [row["train_end_exclusive"] for row in manifests] == [
        "2024-01-01",
        "2024-04-01",
        "2024-07-01",
    ]
    assert all(row["training_samples_after_purge"] >= 120 for row in manifests)


def test_validator_controls_reject_null_and_detect_injected_signal() -> None:
    controls = validator_controls()

    assert controls["passed"]
    assert 0.40 <= controls["negative_control_auc"] <= 0.60
    assert controls["injected_weak_real_auc"] >= 0.75


def test_coefficient_stability_uses_fold_direction_agreement() -> None:
    positive = {feature: 1.0 for feature in FEATURE_COLUMNS}
    one_flip = dict(positive)
    one_flip[FEATURE_COLUMNS[0]] = -1.0

    stability = _fold_coefficient_stability(
        [{"coefficients": positive}, {"coefficients": one_flip}]
    )

    assert stability == (len(FEATURE_COLUMNS) - 1) / len(FEATURE_COLUMNS)


def test_candidate_ids_are_fresh_role_specific_and_not_shadow() -> None:
    candidates = [_candidate_specification(target) for target in ("YM", "RTY", "CL", "GC")]

    assert len({row["candidate_id"] for row in candidates}) == 4
    assert len({row["structural_fingerprint"] for row in candidates}) == 4
    assert all(row["portfolio_role"] == "defensive_risk_state" for row in candidates)
    task = Path(
        "reports/engineering/hydra_distributional_survival_hazard_20260711.md"
    ).read_text(encoding="utf-8")
    assert "cannot become" in task and "SHADOW_RESEARCH_CANDIDATE" in task
    assert "Q4 and later are prohibited" in task
