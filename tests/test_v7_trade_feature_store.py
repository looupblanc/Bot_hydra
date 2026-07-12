from __future__ import annotations

from datetime import UTC, datetime

import numpy as np

from hydra.data.v7_trade_feature_store import (
    ContractDefinition,
    accumulators_to_frame,
    aggregate_trade_chunks,
)


DTYPE = np.dtype(
    [
        ("instrument_id", "<u4"),
        ("ts_event", "<u8"),
        ("price", "<i8"),
        ("size", "<u4"),
        ("action", "S1"),
        ("side", "S1"),
    ]
)


def _ns(hour: int, minute: int, second: int = 0) -> int:
    return int(datetime(2024, 8, 1, hour, minute, second, tzinfo=UTC).timestamp() * 1e9)


def _contracts() -> dict[int, ContractDefinition]:
    return {
        118: ContractDefinition(118, "ES", "ESU4", 0.25, 50.0),
        7114: ContractDefinition(7114, "MES", "MESU4", 0.25, 5.0),
    }


def test_minute_print_aggregation_uses_documented_aggressor_side() -> None:
    # 13:30 UTC is 08:30 America/Chicago during August.
    chunk = np.array(
        [
            (118, _ns(13, 30, 1), 100_000_000_000, 2, b"T", b"B"),
            (118, _ns(13, 30, 2), 100_250_000_000, 1, b"T", b"A"),
            (118, _ns(13, 30, 3), 100_000_000_000, 3, b"T", b"N"),
        ],
        dtype=DTYPE,
    )

    accumulators, audit = aggregate_trade_chunks([chunk], _contracts())
    frame = accumulators_to_frame(accumulators, _contracts())
    row = frame.iloc[0]

    assert audit["retained_rth_record_count"] == 3
    assert row["buy_aggressor_volume"] == 2
    assert row["sell_aggressor_volume"] == 1
    assert row["unknown_side_volume"] == 3
    assert row["signed_aggressor_volume"] == 1
    assert row["path_length_points"] == 0.5
    assert row["availability_ns"] > row["last_trade_ns"]


def test_chunk_boundary_preserves_path_and_first_last_trade() -> None:
    first = np.array(
        [(118, _ns(13, 30, 1), 100_000_000_000, 1, b"T", b"B")],
        dtype=DTYPE,
    )
    second = np.array(
        [(118, _ns(13, 30, 5), 100_500_000_000, 2, b"T", b"B")],
        dtype=DTYPE,
    )

    accumulators, _ = aggregate_trade_chunks([first, second], _contracts())
    frame = accumulators_to_frame(accumulators, _contracts())
    row = frame.iloc[0]

    assert row["open"] == 100.0
    assert row["close"] == 100.5
    assert row["path_length_points"] == 0.5
    assert row["total_volume"] == 3
    assert row["trade_count"] == 2


def test_non_rth_and_unknown_instruments_are_excluded() -> None:
    chunk = np.array(
        [
            (118, _ns(12, 0), 100_000_000_000, 1, b"T", b"B"),
            (999, _ns(13, 30), 100_000_000_000, 1, b"T", b"B"),
            (7114, _ns(13, 31), 100_000_000_000, 1, b"T", b"B"),
        ],
        dtype=DTYPE,
    )

    accumulators, audit = aggregate_trade_chunks([chunk], _contracts())

    assert len(accumulators) == 1
    assert audit["excluded_instrument_record_count"] == 1
    assert audit["excluded_session_record_count"] == 1
