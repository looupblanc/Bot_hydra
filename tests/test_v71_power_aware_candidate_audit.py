from __future__ import annotations

import numpy as np

from hydra.validation.v71_power_aware_candidate_audit import (
    block_bootstrap_statistics,
    effective_independent_events,
    exposure_overlap_inflation,
)


def _ledger(count: int, *, overlap: bool = False) -> list[dict[str, int]]:
    rows = []
    for index in range(count):
        start = index * 100 if not overlap else index * 50
        rows.append({"entry_ns": start, "exit_ns": start + 100})
    return rows


def test_effective_sample_penalizes_serial_dependence() -> None:
    ledger = _ledger(60)
    independent = np.asarray([1.0, -1.0] * 30)
    persistent = np.repeat(np.asarray([1.0, -1.0]), 30)
    first = effective_independent_events(ledger, independent)
    second = effective_independent_events(ledger, persistent)
    assert second["effective_independent_event_count"] < first[
        "effective_independent_event_count"
    ]


def test_overlap_inflation_is_detected() -> None:
    assert exposure_overlap_inflation(_ledger(10)) == 1.0
    assert exposure_overlap_inflation(_ledger(10, overlap=True)) > 1.0


def test_block_bootstrap_is_deterministic() -> None:
    blocks = {
        "2023-W01": np.asarray([10.0, 20.0, 30.0]),
        "2023-W02": np.asarray([-5.0, 15.0]),
        "2024-W01": np.asarray([20.0, 25.0]),
        "2024-W02": np.asarray([5.0, 10.0, 15.0]),
    }
    first = block_bootstrap_statistics(
        blocks,
        observed_mean=15.0,
        minimum_effect=12.5,
        shrinkage=0.5,
        draws=1000,
        seed=971003,
    )
    second = block_bootstrap_statistics(
        blocks,
        observed_mean=15.0,
        minimum_effect=12.5,
        shrinkage=0.5,
        draws=1000,
        seed=971003,
    )
    assert first == second
