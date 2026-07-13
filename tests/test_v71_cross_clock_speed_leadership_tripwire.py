from __future__ import annotations

import pytest

from hydra.research.v71_event_time_grammar import load_event_time_sources
from hydra.validation.v71_cross_clock_speed_leadership_tripwire import (
    build_speed_leadership_null_world,
)
from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire
from hydra.validation.v7_d1_new_dataset_tripwire import D1NullControl


def test_speed_leadership_tripwire_threshold_is_inclusive() -> None:
    verdict, ratio = classify_tripwire(
        real_passes=10,
        real_episodes=100,
        null_passes=8,
        null_episodes=100,
    )
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)


def test_speed_leadership_null_preserves_clock_observables() -> None:
    minute, event, _ = load_event_time_sources(".")
    null_minute, null_event = build_speed_leadership_null_world(
        minute, event, control=D1NullControl.DAILY_BLOCK_SHUFFLE
    )
    for column in (
        "start_event_ns",
        "end_event_ns",
        "availability_ns",
        "duration_seconds",
        "signed_aggressor_volume",
        "total_volume",
        "contract",
        "bar_type",
    ):
        assert event[column].equals(null_event[column])
    assert minute["minute_start_ns"].equals(null_minute["minute_start_ns"])
    assert not minute["close"].equals(null_minute["close"])
