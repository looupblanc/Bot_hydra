from pathlib import Path

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.event_time_volatility_burst_direct_account import (
    _augment_with_variance_clocks,
)


def _matrix() -> FeatureMatrix:
    rows = 40
    arrays = {
        "bar_open": np.full(rows, 100.0),
        "bar_close": np.r_[np.full(20, 100.1), np.full(20, 99.9)],
        "feature__past_volatility": np.full(rows, 0.001),
        "segment_code": np.r_[np.zeros(20), np.ones(20)].astype(np.int64),
        "session_code": np.zeros(rows, dtype=np.int8),
    }
    return FeatureMatrix(
        root=Path("."),
        manifest={"row_count": rows, "bundle_hash": "synthetic"},
        arrays=arrays,
    )


def test_variance_clock_is_causal_directional_and_resets_by_segment() -> None:
    augmented = _augment_with_variance_clocks(_matrix())
    continuation = augmented.array(
        "feature__event_time_q16_coherent_continuation"
    )
    reversal = augmented.array("feature__event_time_q16_coherent_reversal")

    assert np.flatnonzero(continuation).tolist() == [16, 36]
    assert continuation[16] > 0.0
    assert continuation[36] < 0.0
    assert np.array_equal(reversal, -continuation)
    assert not continuation.flags.writeable


def test_future_rows_cannot_change_prior_variance_clock_decisions() -> None:
    source = _matrix()
    first = _augment_with_variance_clocks(source).array(
        "feature__event_time_q16_fast_continuation"
    ).copy()
    mutated = dict(source.arrays)
    mutated["bar_close"] = np.asarray(mutated["bar_close"]).copy()
    mutated["bar_close"][30:] = 150.0
    changed = FeatureMatrix(source.root, source.manifest, mutated)
    second = _augment_with_variance_clocks(changed).array(
        "feature__event_time_q16_fast_continuation"
    )

    assert np.array_equal(first[:30], second[:30])
