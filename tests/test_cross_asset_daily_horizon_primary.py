from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hydra.research.cross_asset_daily_horizon_primary import (
    EXPECTED_POPULATION,
    _bh_adjust,
    _source_features,
    build_daily_feature_cache,
    generate_hypotheses,
)


def test_population_is_exact_unique_and_structurally_bounded() -> None:
    population = generate_hypotheses()

    assert len(population) == EXPECTED_POPULATION == 720
    assert len({row["candidate_id"] for row in population}) == 720
    assert len({row["structural_fingerprint"] for row in population}) == 720
    assert not any(
        row["feature"] == "relative_prior_trend"
        and row["source_market"] == row["target_market"]
        for row in population
    )
    assert {row["market_ecology"] for row in population} == {
        "equity_indices",
        "energy",
        "metals",
    }


def test_source_features_are_lagged_by_a_complete_session() -> None:
    sessions = pd.date_range("2023-01-02", periods=30, freq="D")
    table = pd.DataFrame(
        {
            "session_id": sessions.strftime("%Y-%m-%d"),
            "prior_range": [10.0 + index for index in range(30)],
            "prior_trend": [(-1.0) ** index * 0.5 for index in range(30)],
            "rth_high": [110.0 + index for index in range(30)],
            "rth_low": [100.0 + index for index in range(30)],
            "rth_close": [106.0 + index for index in range(30)],
        }
    )

    features = _source_features(table)

    assert pd.isna(features.iloc[0]["source_prior_session_id"])
    assert features.iloc[1]["source_prior_session_id"] == "2023-01-02"
    assert (
        pd.to_datetime(features.loc[1:, "source_prior_session_id"])
        < pd.to_datetime(features.loc[1:, "session_id"])
    ).all()
    assert pd.isna(features.iloc[9]["source_prior_range_shock_signed"])
    assert pd.notna(features.iloc[10]["source_prior_range_shock_signed"])


def test_bh_adjustment_is_monotone_in_rank_and_never_smaller() -> None:
    raw = [0.01, 0.04, 0.03, 0.20]
    adjusted = _bh_adjust(raw)

    assert adjusted == pytest.approx([0.04, 0.0533333333, 0.0533333333, 0.20])
    assert all(adjusted[index] >= raw[index] for index in range(len(raw)))


def test_feature_cache_builds_every_valid_target_source_feature_niche() -> None:
    sessions = pd.date_range("2023-01-02", periods=30, freq="D")
    table = pd.DataFrame(
        {
            "session_id": sessions.strftime("%Y-%m-%d"),
            "prior_range": [10.0] * 30,
            "prior_trend": [0.5] * 30,
            "rth_high": [110.0] * 30,
            "rth_low": [100.0] * 30,
            "rth_close": [106.0] * 30,
        }
    )
    cache = build_daily_feature_cache(
        {symbol: table.copy() for symbol in ("YM", "RTY", "CL", "GC")}
    )

    assert len(cache) == 60
    assert ("YM", "YM", "relative_prior_trend") not in cache
    assert ("YM", "CL", "relative_prior_trend") in cache
    assert "_feature_value" in cache[("YM", "CL", "source_prior_trend")]


def test_task_freezes_2023_selector_before_2024() -> None:
    task = Path(
        "reports/engineering/hydra_cross_asset_daily_horizon_tournament_20260711.md"
    ).read_text(encoding="utf-8")

    assert "exactly 720 unique structural fingerprints" in task
    assert "select a diversified elite set using only 2023" in task
    assert "Q4 and later: prohibited" in task
    assert "PAPER_SHADOW_READY" in task
