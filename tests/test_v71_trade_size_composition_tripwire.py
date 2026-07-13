from __future__ import annotations

import pytest

from hydra.research.v71_event_time_grammar import load_event_time_sources
from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire
from hydra.validation.v71_trade_size_composition_tripwire import (
    build_trade_size_composition_null_world,
)
from hydra.validation.v7_d1_new_dataset_tripwire import D1NullControl


def test_trade_size_composition_tripwire_threshold_is_inclusive() -> None:
    verdict, ratio = classify_tripwire(
        real_passes=10,
        real_episodes=100,
        null_passes=8,
        null_episodes=100,
    )
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)


def test_trade_size_null_preserves_trade_observables_and_changes_prices() -> None:
    minute, _, _ = load_event_time_sources(".")
    null = build_trade_size_composition_null_world(
        minute, control=D1NullControl.DAILY_BLOCK_SHUFFLE
    )
    for column in (
        "minute_start_ns",
        "availability_ns",
        "trade_count",
        "total_volume",
        "signed_aggressor_volume",
        "signed_aggressor_fraction",
        "contract",
    ):
        assert minute[column].equals(null[column])
    assert not minute["close"].equals(null["close"])
