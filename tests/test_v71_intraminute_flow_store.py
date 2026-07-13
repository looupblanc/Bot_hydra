from __future__ import annotations

import numpy as np

from hydra.data.v71_intraminute_flow_store import HalfMinuteAccumulator, _accumulate_chunk


def test_intraminute_accumulator_uses_frozen_half_minute_boundaries() -> None:
    dtype = np.dtype(
        [
            ("ts_event", "<i8"),
            ("size", "<u4"),
            ("side", "S1"),
        ]
    )
    base = 1_700_000_040_000_000_000
    rows = np.array(
        [
            (base + 1_000_000_000, 2, b"B"),
            (base + 29_999_999_999, 3, b"A"),
            (base + 30_000_000_000, 5, b"B"),
            (base + 59_000_000_000, 7, b"A"),
        ],
        dtype=dtype,
    )
    destination: dict[int, HalfMinuteAccumulator] = {}
    _accumulate_chunk(destination, rows)
    assert len(destination) == 1
    value = next(iter(destination.values()))
    assert value.first_trade_count == 2
    assert value.second_trade_count == 2
    assert value.first_total_volume == 5
    assert value.second_total_volume == 12
    assert value.first_signed_flow == -1
    assert value.second_signed_flow == -2


def test_intraminute_accumulator_is_chunk_boundary_invariant() -> None:
    dtype = np.dtype([("ts_event", "<i8"), ("size", "<u4"), ("side", "S1")])
    base = 1_700_000_040_000_000_000
    rows = np.array(
        [
            (base + 2_000_000_000, 2, b"B"),
            (base + 20_000_000_000, 4, b"B"),
            (base + 40_000_000_000, 3, b"A"),
            (base + 50_000_000_000, 1, b"N"),
        ],
        dtype=dtype,
    )
    whole: dict[int, HalfMinuteAccumulator] = {}
    split: dict[int, HalfMinuteAccumulator] = {}
    _accumulate_chunk(whole, rows)
    _accumulate_chunk(split, rows[:2])
    _accumulate_chunk(split, rows[2:])
    assert whole == split
