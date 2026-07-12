from __future__ import annotations

from datetime import UTC, datetime

import numpy as np

from hydra.data.v7_d1_event_store import (
    _positive_measure_boundaries,
    _signed_imbalance_boundaries,
    build_event_bar_frame,
)
from hydra.data.v7_trade_feature_store import ContractDefinition


DTYPE = np.dtype(
    [
        ("instrument_id", "<u4"),
        ("ts_event", "<u8"),
        ("ts_recv", "<u8"),
        ("sequence", "<u4"),
        ("price", "<i8"),
        ("size", "<u4"),
        ("action", "S1"),
        ("side", "S1"),
    ]
)


def test_positive_measure_boundaries_do_not_split_trigger_trade() -> None:
    boundaries = _positive_measure_boundaries(
        np.asarray([4.0, 7.0, 5.0, 6.0]), 10.0
    )

    assert boundaries.tolist() == [1, 3]


def test_signed_imbalance_resets_after_first_passage() -> None:
    boundaries = _signed_imbalance_boundaries(
        np.asarray([3, -1, 4, -3, -2, -1]), 5.0
    )

    assert boundaries.tolist() == [2, 4]


def test_event_bar_uses_receive_time_availability_and_reconciles_sides() -> None:
    records = np.array(
        [
            (118, _ns(1), _ns(2), 1, 100_000_000_000, 4, b"T", b"B"),
            (118, _ns(3), _ns(5), 2, 100_250_000_000, 7, b"T", b"A"),
            (118, _ns(6), _ns(7), 3, 100_500_000_000, 5, b"T", b"N"),
            (118, _ns(8), _ns(9), 4, 100_250_000_000, 6, b"T", b"B"),
            (118, _ns(10), _ns(11), 5, 101_000_000_000, 1, b"T", b"N"),
        ],
        dtype=DTYPE,
    )
    definition = ContractDefinition(118, "ES", "ESU4", 0.25, 50.0)

    frame = build_event_bar_frame(
        records,
        definition,
        calendar_year=2024,
        bar_type="VOLUME_BAR",
        threshold=10.0,
    )

    assert len(frame) == 2
    assert frame.iloc[0]["trade_count"] == 2
    assert frame.iloc[0]["total_volume"] == 11
    assert frame.iloc[0]["buy_aggressor_volume"] == 4
    assert frame.iloc[0]["sell_aggressor_volume"] == 7
    assert frame.iloc[0]["availability_ns"] == _ns(5)
    assert frame.iloc[0]["path_length_points"] == 0.25
    assert frame.iloc[1]["unknown_side_volume"] == 5
    assert frame["total_volume"].sum() == 22


def _ns(second: int) -> int:
    return int(
        datetime(2024, 8, 2, 13, 30, second, tzinfo=UTC).timestamp()
        * 1_000_000_000
    )
