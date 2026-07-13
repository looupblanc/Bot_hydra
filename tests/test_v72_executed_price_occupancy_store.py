from __future__ import annotations

import math

import numpy as np

from hydra.data.v72_executed_price_occupancy_store import (
    OccupancyStreamAccumulator,
    SourceSpec,
    _compute_mode_migration,
    _rank_occupancy_levels,
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
TICK_RAW = 250_000_000


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
    return np.array(
        [
            (base + 1_000_000_000, 400 * TICK_RAW, 2, b"B"),
            (base + 2_000_000_000, 401 * TICK_RAW, 3, b"B"),
            (base + 2_000_000_000, 401 * TICK_RAW, 1, b"N"),
            (base + 12_000_000_000, 400 * TICK_RAW, 4, b"A"),
            (base + 61_000_000_000, 396 * TICK_RAW, 5, b"A"),
            (base + 63_000_000_000, 400 * TICK_RAW, 1, b"B"),
            (base + 66_000_000_000, 404 * TICK_RAW, 2, b"B"),
        ],
        dtype=dtype,
    )


def _build(parts: list[np.ndarray]):
    stream = OccupancyStreamAccumulator(source=SOURCE)
    for part in parts:
        stream.ingest(part)
    return stream.finish().reset_index(drop=True)


def test_executed_price_occupancy_features_are_exact() -> None:
    frame = _build([_rows()])
    assert len(frame) == 2
    first = frame.iloc[0]
    assert first["trade_count"] == 4
    assert first["total_volume"] == 10
    assert first["buy_volume"] == 5
    assert first["sell_volume"] == 4
    assert first["neutral_volume"] == 1
    assert first["signed_flow_fraction"] == 0.1
    assert first["unique_tick_count"] == 2
    assert first["mode_tick"] == 400
    assert first["second_mode_tick"] == 401
    assert first["mode_volume"] == 6
    assert first["second_mode_volume"] == 4
    assert first["mode_volume_share"] == 0.6
    assert first["top_two_volume_share"] == 1.0
    assert first["second_to_first_mode_ratio"] == 4 / 6
    assert first["mode_signed_flow_fraction"] == -2 / 6
    assert first["adjacent_tick_transition_count"] == 2
    assert first["revisit_count"] == 1
    assert first["revisit_ratio"] == 0.5
    assert first["last_minus_mode_ticks"] == 0
    assert first["maximum_excursion_from_mode_ticks"] == 1
    assert first["maximum_excursion_direction"] == 1
    expected_entropy = -(
        0.6 * math.log(0.6) + 0.4 * math.log(0.4)
    ) / math.log(2)
    assert math.isclose(first["occupancy_entropy"], expected_entropy)


def test_mode_tie_break_uses_exact_vwap_distance_then_lower_tick() -> None:
    mode, second = _rank_occupancy_levels(
        {400: 5, 402: 5}, weighted_tick_volume=4010, total_volume=10
    )
    assert mode == 400
    assert second == 402
    mode, second = _rank_occupancy_levels(
        {400: 5, 402: 5, 405: 1}, weighted_tick_volume=4415, total_volume=11
    )
    assert mode == 402
    assert second == 400


def test_occupancy_store_is_chunk_boundary_invariant() -> None:
    rows = _rows()
    whole = _build([rows])
    split = _build([rows[:1], rows[1:3], rows[3:5], rows[5:]])
    assert whole.equals(split)
    assert whole["availability_ns"].equals(
        whole["minute_start_ns"] + 60_000_000_000
    )


def test_single_level_minute_has_zero_entropy_and_no_second_mode() -> None:
    row = _rows()[:1]
    frame = _build([row])
    first = frame.iloc[0]
    assert first["occupancy_entropy"] == 0.0
    assert math.isnan(first["second_mode_tick"])
    assert first["second_to_first_mode_ratio"] == 0.0
    assert first["revisit_ratio"] == 0.0


def test_mode_migration_requires_same_session_contract_and_contiguous_minute() -> None:
    frame = _build([_rows()])
    frame["mode_migration_ticks"] = _compute_mode_migration(frame)
    assert math.isnan(frame.iloc[0]["mode_migration_ticks"])
    assert frame.iloc[1]["mode_migration_ticks"] == -4

    separated = frame.copy()
    separated.loc[1, "minute_start_ns"] += 60_000_000_000
    separated["mode_migration_ticks"] = _compute_mode_migration(separated)
    assert math.isnan(separated.iloc[1]["mode_migration_ticks"])
