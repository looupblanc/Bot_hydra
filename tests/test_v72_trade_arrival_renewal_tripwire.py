from __future__ import annotations

from pathlib import Path

import pytest

from hydra.research.v72_trade_arrival_renewal import FEATURE_PATH
from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire
from hydra.validation.v72_trade_arrival_renewal_tripwire import (
    INVARIANT_FEATURE_COLUMNS,
    build_trade_arrival_null_world,
    run_trade_arrival_renewal_tripwire,
)
from hydra.validation.v7_d1_new_dataset_tripwire import D1NullControl
from hydra.research.v72_trade_arrival_renewal import (
    load_trade_arrival_renewal_sources,
)


def test_trade_arrival_tripwire_threshold_is_inclusive() -> None:
    verdict, ratio = classify_tripwire(
        real_passes=10,
        real_episodes=100,
        null_passes=8,
        null_episodes=100,
    )
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)


def test_trade_arrival_null_preserves_arrivals_and_recomputes_price() -> None:
    minute, _, _ = load_trade_arrival_renewal_sources(".")
    feature = __import__("pandas").read_parquet(FEATURE_PATH)
    null_minute, null_feature = build_trade_arrival_null_world(
        minute, feature, control=D1NullControl.DAILY_BLOCK_SHUFFLE
    )
    for column in INVARIANT_FEATURE_COLUMNS:
        assert feature[column].equals(null_feature[column])
    assert not minute["close"].equals(null_minute["close"])
    assert not feature["price_progress_points"].equals(
        null_feature["price_progress_points"]
    )


def test_trade_arrival_tripwire_has_frozen_denominators(tmp_path: Path) -> None:
    result = run_trade_arrival_renewal_tripwire(
        project_root=".",
        proof_registry_path="mission/state/proof_registry.json",
        output_dir=tmp_path / "result",
    )
    assert result["real"]["episode_count"] == 480
    assert result["pooled_null"]["episode_count"] == 1440
    assert result["candidate_promotion_authorized"] is False
    assert result["new_data_purchase_count"] == 0
    assert result["q4_access_count_delta"] == 0
    assert result["outbound_order_count"] == 0
