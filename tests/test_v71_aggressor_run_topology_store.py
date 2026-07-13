from __future__ import annotations

import numpy as np

from hydra.data.v71_aggressor_run_topology_store import (
    RunTopologyAccumulator,
    _accumulate_chunk,
)


def _rows() -> np.ndarray:
    dtype = np.dtype(
        [("ts_event", "<i8"), ("price", "<i8"), ("side", "S1")]
    )
    base = 1_700_000_040_000_000_000
    return np.array(
        [
            (base + 1_000_000_000, 100, b"B"),
            (base + 2_000_000_000, 101, b"B"),
            (base + 3_000_000_000, 101, b"N"),
            (base + 4_000_000_000, 100, b"A"),
            (base + 5_000_000_000, 99, b"A"),
            (base + 6_000_000_000, 98, b"A"),
            (base + 7_000_000_000, 99, b"B"),
        ],
        dtype=dtype,
    )


def test_aggressor_run_topology_is_exact() -> None:
    destination: dict[int, RunTopologyAccumulator] = {}
    _accumulate_chunk(destination, _rows())
    assert len(destination) == 1
    value = next(iter(destination.values()))
    assert value.trade_count == 6
    assert value.neutral_trade_count == 1
    assert value.side_change_count == 2
    assert value.longest_buy_run == 2
    assert value.longest_sell_run == 3
    assert value.first_price == 100
    assert value.last_price == 99


def test_aggressor_run_topology_is_chunk_boundary_invariant() -> None:
    rows = _rows()
    whole: dict[int, RunTopologyAccumulator] = {}
    split: dict[int, RunTopologyAccumulator] = {}
    _accumulate_chunk(whole, rows)
    _accumulate_chunk(split, rows[:1])
    _accumulate_chunk(split, rows[1:5])
    _accumulate_chunk(split, rows[5:])
    assert whole == split
