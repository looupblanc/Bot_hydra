from __future__ import annotations

import pytest

from hydra.research.v71_intraminute_flow import load_intraminute_flow_sources
from hydra.validation.v71_intraminute_flow_tripwire import build_intraminute_flow_null_world
from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire
from hydra.validation.v7_d1_new_dataset_tripwire import D1NullControl


def test_intraminute_flow_tripwire_threshold_is_inclusive() -> None:
    verdict, ratio = classify_tripwire(real_passes=10, real_episodes=100, null_passes=8, null_episodes=100)
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)


def test_intraminute_null_preserves_minute_identity_and_changes_prices() -> None:
    minute, _, _ = load_intraminute_flow_sources(".")
    null = build_intraminute_flow_null_world(minute, control=D1NullControl.DAILY_BLOCK_SHUFFLE)
    for column in ("minute_start_ns", "availability_ns", "contract"):
        assert minute[column].equals(null[column])
    assert not minute["close"].equals(null["close"])
