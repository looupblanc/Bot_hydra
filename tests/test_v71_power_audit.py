from __future__ import annotations

import numpy as np

from hydra.calibration.v71_power_audit import (
    WORLD_SEMI,
    WORLD_SYNTHETIC,
    _evaluate_control_family,
)


def test_power_family_is_deterministic_and_uses_hierarchical_trials() -> None:
    empirical = tuple(np.linspace(-200.0, 200.0, 40) for _ in range(25))
    first = _evaluate_control_family(
        world=WORLD_SYNTHETIC,
        seed=71001,
        event_count=100,
        effect=50.0,
        family_size=32,
        raw_global_trials=247_892,
        empirical_days=empirical,
    )
    second = _evaluate_control_family(
        world=WORLD_SYNTHETIC,
        seed=71001,
        event_count=100,
        effect=50.0,
        family_size=32,
        raw_global_trials=247_892,
        empirical_days=empirical,
    )

    assert first == second
    assert first["DSR_N_trials"] >= 20
    assert sum(first["confusion"].values()) == 32


def test_semisynthetic_control_accepts_empirical_residual_days() -> None:
    empirical = tuple(np.arange(20, dtype=float) - 10.0 for _ in range(25))
    result = _evaluate_control_family(
        world=WORLD_SEMI,
        seed=71002,
        event_count=30,
        effect=0.0,
        family_size=32,
        raw_global_trials=247_892,
        empirical_days=empirical,
    )

    assert result["confusion"]["TP"] == 0
    assert result["confusion"]["FN"] == 0
