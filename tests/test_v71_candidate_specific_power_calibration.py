from __future__ import annotations

import numpy as np

from hydra.calibration.v71_candidate_specific_power_calibration import (
    WORLD_SEMI,
    WORLD_SYNTHETIC,
    _control_sample,
    _one_sided_block_test,
)


def test_candidate_power_control_is_deterministic() -> None:
    days = tuple(np.linspace(-150.0, 150.0, 20) for _ in range(25))
    first = _control_sample(
        world=WORLD_SYNTHETIC,
        event_count=60,
        effect=25.0,
        rng=np.random.default_rng(971101),
        empirical_days=days,
    )
    second = _control_sample(
        world=WORLD_SYNTHETIC,
        event_count=60,
        effect=25.0,
        rng=np.random.default_rng(971101),
        empirical_days=days,
    )
    assert np.array_equal(first, second)


def test_semisynthetic_control_and_block_test() -> None:
    days = tuple(np.arange(20, dtype=float) - 9.5 for _ in range(25))
    sample = _control_sample(
        world=WORLD_SEMI,
        event_count=60,
        effect=50.0,
        rng=np.random.default_rng(971201),
        empirical_days=days,
    )
    assert len(sample) == 60
    assert _one_sided_block_test(sample)
