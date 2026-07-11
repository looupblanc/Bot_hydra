from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.counterfactual_hazard_primary import (
    PRIMARY_ALPHA,
    _one_sided_binomial_probability,
    _primary_manifest,
    add_counterfactual_features,
    generate_counterfactual_hypotheses,
    match_counterfactual_events,
    paired_hazard_metrics,
)


def test_population_is_exact_balanced_and_deterministic() -> None:
    first = generate_counterfactual_hypotheses()
    second = generate_counterfactual_hypotheses()

    assert first == second
    assert len(first) == 96
    assert len({item["structural_fingerprint"] for item in first}) == 96
    assert all(
        sum(item["market"] == market for item in first) == 16
        for market in ("ES", "NQ", "RTY", "YM", "GC", "CL")
    )


def test_counterfactual_features_are_invariant_to_future_changes() -> None:
    size = 260
    base = pd.DataFrame(
        {
            "contiguous_segment_id": [1] * size,
            "close": 100.0 + np.sin(np.arange(size) / 11.0) + np.arange(size) * 0.01,
        }
    )
    altered = base.copy()
    altered.loc[220:, "close"] += np.linspace(0, 1000, size - 220)

    first = add_counterfactual_features(base)
    second = add_counterfactual_features(altered)

    columns = [
        "path_efficiency_30",
        "failed_displacement_recovery_30_5",
        "range_compression_30_180",
        "accepted_price_migration_30",
    ]
    pd.testing.assert_frame_equal(first.loc[:219, columns], second.loc[:219, columns])


def _matching_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    covariates = {
        "past_volatility": [0.01, 0.011, 0.012, 0.013],
        "past_return_60": [0.001, 0.0011, 0.0012, 0.0013],
        "past_participation": [1.0, 1.1, 1.2, 1.3],
        "past_opportunity_frequency": [0.1, 0.11, 0.12, 0.13],
    }
    treated = pd.DataFrame(
        {
            "symbol": ["ES", "ES"],
            "active_contract": ["ESH4", "ESH4"],
            "session_phase_15m": [40, 41],
            "trading_session_id": ["2024-01-02", "2024-01-03"],
            "entry_timestamp": pd.to_datetime(
                ["2024-01-02T15:00:00Z", "2024-01-03T15:15:00Z"], utc=True
            ),
            "net_pnl": [100.0, -20.0],
            **{key: values[:2] for key, values in covariates.items()},
        }
    )
    controls = pd.DataFrame(
        {
            "symbol": ["ES", "ES", "ES", "ES"],
            "active_contract": ["ESH4"] * 4,
            "session_phase_15m": [40, 41, 42, 43],
            "trading_session_id": [
                "2024-01-04",
                "2024-01-05",
                "2024-01-06",
                "2024-01-07",
            ],
            "entry_timestamp": pd.to_datetime(
                [
                    "2024-01-04T15:00:00Z",
                    "2024-01-05T15:15:00Z",
                    "2024-01-06T15:30:00Z",
                    "2024-01-07T15:45:00Z",
                ],
                utc=True,
            ),
            "net_pnl": [-10.0, 30.0, -40.0, 50.0],
            **covariates,
        }
    )
    return treated, controls


def test_matching_is_deterministic_distinct_and_outcome_blind() -> None:
    treated, controls = _matching_frames()
    first = match_counterfactual_events(treated, controls)
    changed = controls.copy()
    changed["net_pnl"] = [9999.0, -9999.0, 5555.0, -5555.0]
    second = match_counterfactual_events(treated, changed)

    assert first["control_entry_timestamp"].tolist() == second[
        "control_entry_timestamp"
    ].tolist()
    assert first["control_session"].tolist() == second["control_session"].tolist()
    assert (first["treated_session"] != first["control_session"]).all()
    assert first["active_contract"].eq("ESH4").all()
    assert first["pair_id"].is_unique


def test_paired_probability_is_calibrated_on_null_and_powerful_on_uplift() -> None:
    rng = np.random.default_rng(7126)
    null_rejections = []
    power_rejections = []
    for _ in range(500):
        null_positive = int(rng.binomial(120, 0.5))
        null_rejections.append(
            _one_sided_binomial_probability(null_positive, 120) <= PRIMARY_ALPHA
        )
        positive = int(rng.binomial(120, 0.70))
        power_rejections.append(
            _one_sided_binomial_probability(positive, 120) <= PRIMARY_ALPHA
        )

    assert np.mean(null_rejections) <= 0.05
    assert np.mean(power_rejections) >= 0.80


def test_hazard_metrics_and_primary_manifest_freeze_one_test() -> None:
    treated, controls = _matching_frames()
    pairs = match_counterfactual_events(treated, controls)
    metrics = paired_hazard_metrics(pairs)
    hypothesis = generate_counterfactual_hypotheses()[0]
    manifest = _primary_manifest(
        primary=hypothesis,
        ranking=[{"candidate_id": hypothesis["candidate_id"], "eligible": True}],
        archive_hash="a" * 64,
        population_hash="b" * 64,
    )

    assert metrics["pairs"] == 2
    assert manifest["promotion_test_count"] == 1
    assert manifest["candidate_probability_threshold"] == PRIMARY_ALPHA
    assert manifest["selection_data_end_exclusive"] == "2024-01-01"
    assert manifest["q4_access_allowed"] is False
    assert manifest["paper_shadow_ready_allowed"] is False
