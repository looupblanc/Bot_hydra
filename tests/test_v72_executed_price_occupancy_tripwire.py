from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire
from hydra.validation.v72_executed_price_occupancy_tripwire import (
    _round_rational_ties_lower,
    reconstruct_minute_tick_path,
    run_executed_price_occupancy_tripwire,
)


def test_occupancy_tripwire_threshold_is_inclusive() -> None:
    verdict, ratio = classify_tripwire(
        real_passes=10,
        real_episodes=100,
        null_passes=8,
        null_episodes=100,
    )
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)


def test_rank_residual_reconstruction_is_exact_and_endpoint_bound() -> None:
    result = reconstruct_minute_tick_path(
        np.asarray([400, 403, 404], dtype=np.int64),
        null_open_tick=500,
        null_close_tick=502,
    )
    assert result.tolist() == [500, 502, 502]
    assert result[0] == 500
    assert result[-1] == 502


def test_rational_rounding_uses_lower_tick_on_exact_half() -> None:
    result = _round_rational_ties_lower(
        np.asarray([1, 3, 5, -1], dtype=np.int64), 2
    )
    assert result.tolist() == [0, 1, 2, -1]


def test_occupancy_tripwire_has_frozen_denominators(tmp_path: Path) -> None:
    result = run_executed_price_occupancy_tripwire(
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
