from __future__ import annotations

import numpy as np

from hydra.data.v72_trade_arrival_renewal_store import (
    ArrivalStreamAccumulator,
    SourceSpec,
)


SOURCE = SourceSpec(
    calendar_year=2024,
    path="unused",
    sha256="0" * 64,
    start_ns=0,
    end_ns=2**63 - 1,
    instrument_id=118,
    contract="ESU4",
)


def _rows() -> np.ndarray:
    dtype = np.dtype(
        [
            ("ts_event", "<i8"),
            ("price", "<i8"),
            ("size", "<u4"),
            ("side", "S1"),
        ]
    )
    base = 1_700_000_040_000_000_000
    scale = 1_000_000_000
    return np.array(
        [
            (base + 1_000_000_000, 100 * scale, 2, b"B"),
            (base + 2_000_000_000, 101 * scale, 3, b"B"),
            (base + 2_000_000_000, 101 * scale, 1, b"N"),
            (base + 12_000_000_000, 100 * scale, 4, b"A"),
            (base + 61_000_000_000, 99 * scale, 5, b"A"),
            (base + 63_000_000_000, 100 * scale, 1, b"B"),
            (base + 66_000_000_000, 101 * scale, 2, b"B"),
        ],
        dtype=dtype,
    )


def _build(parts: list[np.ndarray]):
    stream = ArrivalStreamAccumulator(source=SOURCE)
    for part in parts:
        stream.ingest(part)
    return stream.finish().reset_index(drop=True)


def test_trade_arrival_features_are_exact() -> None:
    frame = _build([_rows()])
    assert len(frame) == 2
    first = frame.iloc[0]
    assert first["trade_count"] == 4
    assert first["total_volume"] == 10
    assert first["signed_volume"] == 1
    assert first["signed_flow_fraction"] == 0.1
    assert first["positive_gap_count"] == 2
    assert first["positive_gap_median_ns"] == 5_500_000_000
    assert first["trade_count_5s_00"] == 3
    assert first["trade_count_5s_02"] == 1
    assert first["maximum_five_second_share"] == 0.75
    assert first["price_progress_points"] == 0.0


def test_trade_arrival_store_is_chunk_boundary_invariant() -> None:
    rows = _rows()
    whole = _build([rows])
    split = _build([rows[:1], rows[1:3], rows[3:5], rows[5:]])
    assert whole.equals(split)
    assert whole["availability_ns"].equals(
        whole["minute_start_ns"] + 60_000_000_000
    )


def test_zero_timestamp_gaps_are_excluded_from_gap_median() -> None:
    first = _build([_rows()]).iloc[0]
    assert first["trade_count"] == 4
    assert first["positive_gap_count"] == 2
