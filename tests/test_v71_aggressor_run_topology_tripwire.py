from __future__ import annotations

import pytest

from hydra.research.v71_aggressor_run_topology import (
    load_aggressor_run_topology_sources,
)
from hydra.validation.v71_aggressor_run_topology_tripwire import (
    _feature_for_null,
    build_aggressor_run_topology_null_world,
)
from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire
from hydra.validation.v7_d1_new_dataset_tripwire import D1NullControl


def test_aggressor_run_tripwire_threshold_is_inclusive() -> None:
    verdict, ratio = classify_tripwire(
        real_passes=10, real_episodes=100, null_passes=8, null_episodes=100
    )
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)


def test_aggressor_run_null_preserves_topology_and_recomputes_price_progress() -> None:
    minute, states, _ = load_aggressor_run_topology_sources(".")
    null = build_aggressor_run_topology_null_world(
        minute, control=D1NullControl.DAILY_BLOCK_SHUFFLE
    )
    for column in ("minute_start_ns", "availability_ns", "contract"):
        assert minute[column].equals(null[column])
    assert not minute["close"].equals(null["close"])
    null_feature = _feature_for_null(states, null)
    assert states["longest_buy_run"].equals(null_feature["longest_buy_run"])
    assert states["longest_sell_run"].equals(null_feature["longest_sell_run"])
    assert not states["last_price"].equals(null_feature["last_price"])
