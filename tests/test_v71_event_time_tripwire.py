from __future__ import annotations

import pandas as pd
import pytest

from hydra.validation.v71_event_time_tripwire import (
    _verify_preserved_event_observables,
)
from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire


def test_event_time_tripwire_threshold_is_inclusive() -> None:
    verdict, ratio = classify_tripwire(
        real_passes=5,
        real_episodes=100,
        null_passes=4,
        null_episodes=100,
    )
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)


def test_event_time_null_observables_must_be_identical() -> None:
    source = pd.DataFrame(
        {
            "start_event_ns": [1],
            "end_event_ns": [2],
            "availability_ns": [2],
            "duration_seconds": [1.0e-9],
            "signed_aggressor_volume": [3.0],
            "volume": [4.0],
            "contract": ["ESH6"],
            "bar_type": ["VOLUME_BAR"],
        }
    )
    _verify_preserved_event_observables(source, source.copy())
