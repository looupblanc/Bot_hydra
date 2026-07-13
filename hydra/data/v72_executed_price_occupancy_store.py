from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


GRAMMAR_PATH = "WORM/v7.2-executed-price-occupancy-grammar-0012-2026-07-13.json"
GRAMMAR_SHA256 = "d0fa4eb200f47e1df9d3323c09f9e0c3729802a001b9c946bdf43824846a4c0c"
OUTPUT_PATH = "data/cache/v7_d1/date_matched_executed_price_occupancy_v1.parquet"
MANIFEST_PATH = "data/manifests/v7_d1_executed_price_occupancy_v1.json"
TRANSFORMATION_VERSION = "hydra_v7_d1_executed_price_occupancy_v1"
MINUTE_NS = 60_000_000_000
PRICE_SCALE = 1_000_000_000
TICK_SIZE = 0.25
TICK_RAW = 250_000_000
EXPECTED_MINUTE_COUNT = 17_200


class V72ExecutedPriceOccupancyStoreError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SourceSpec:
    calendar_year: int
    path: str
    sha256: str
    start_ns: int
    end_ns: int
    instrument_id: int
    contract: str


SOURCES = (
    SourceSpec(
        calendar_year=2023,
        path=(
            "data/cache/databento_v7_d1/"
            "GLBX-MDP3_trades_ES_MES_2023-08-02_2023-09-01.dbn.zst"
        ),
        sha256="d93e5c8bd657a28bff5f05560b845caa3bb04e9252882ddc2751033fedd1c468",
        start_ns=1_690_934_400_000_000_000,
        end_ns=1_693_526_400_000_000_000,
        instrument_id=3445,
        contract="ESU3",
    ),
    SourceSpec(
        calendar_year=2024,
        path=(
            "data/cache/databento_v7_d1/"
            "GLBX-MDP3_trades_ES_MES_2024-08-01_2024-10-01.dbn.zst"
        ),
        sha256="a312d6df524205c2d1632c20f1033ecb74081c98d6608eacc1a9cdb08bcf9b63",
        start_ns=1_722_556_800_000_000_000,
        end_ns=1_725_148_800_000_000_000,
        instrument_id=118,
        contract="ESU4",
    ),
)


@dataclass(slots=True)
class OccupancyMinuteAccumulator:
    minute_start_ns: int | None = None
    trade_count: int = 0
    total_volume: int = 0
    buy_volume: int = 0
    sell_volume: int = 0
    neutral_volume: int = 0
    weighted_tick_volume: int = 0
    first_tick: int | None = None
    last_tick: int | None = None
    previous_tick: int | None = None
    adjacent_tick_transition_count: int = 0
    revisit_count: int = 0
    seen_ticks: set[int] = field(default_factory=set)
    ordered_ticks: list[int] = field(default_factory=list)
    level_volume: dict[int, int] = field(default_factory=dict)
    level_signed_volume: dict[int, int] = field(default_factory=dict)

    def empty(self) -> bool:
        return self.minute_start_ns is None


@dataclass(slots=True)
class OccupancyStreamAccumulator:
    source: SourceSpec
    current: OccupancyMinuteAccumulator = field(
        default_factory=OccupancyMinuteAccumulator
    )
    rows: list[dict[str, Any]] = field(default_factory=list)
    last_event_ns: int = -1

    def ingest(self, records: np.ndarray) -> None:
        _accumulate_records(self, records)

    def finish(self) -> pd.DataFrame:
        self._flush_current()
        return pd.DataFrame(self.rows)

    def _flush_current(self) -> None:
        value = self.current
        if value.empty():
            return
        if (
            value.first_tick is None
            or value.last_tick is None
            or not value.level_volume
            or value.total_volume <= 0
        ):
            raise V72ExecutedPriceOccupancyStoreError(
                "occupancy minute lacks an executable tick path"
            )
        if value.buy_volume + value.sell_volume + value.neutral_volume != value.total_volume:
            raise V72ExecutedPriceOccupancyStoreError(
                "occupancy aggressor volumes do not reconcile"
            )
        if sum(value.level_volume.values()) != value.total_volume:
            raise V72ExecutedPriceOccupancyStoreError(
                "occupancy level volumes do not reconcile"
            )

        mode_tick, second_mode_tick = _rank_occupancy_levels(
            value.level_volume,
            weighted_tick_volume=value.weighted_tick_volume,
            total_volume=value.total_volume,
        )
        mode_volume = value.level_volume[mode_tick]
        second_mode_volume = (
            value.level_volume[second_mode_tick]
            if second_mode_tick is not None
            else 0
        )
        unique_tick_count = len(value.level_volume)
        if unique_tick_count == 1:
            entropy = 0.0
        else:
            shares = np.fromiter(
                value.level_volume.values(), dtype=np.float64
            ) / float(value.total_volume)
            entropy = float(-np.sum(shares * np.log(shares)) / math.log(unique_tick_count))

        maximum_excursion = -1
        maximum_excursion_direction = 0
        for tick in value.ordered_ticks:
            excursion = abs(tick - mode_tick)
            if excursion > maximum_excursion:
                maximum_excursion = excursion
                maximum_excursion_direction = _sign(tick - mode_tick)

        minute_timestamp = pd.Timestamp(value.minute_start_ns, unit="ns", tz="UTC")
        session_date = minute_timestamp.tz_convert("America/Chicago").date().isoformat()
        row = {
            "calendar_year": self.source.calendar_year,
            "session_date": session_date,
            "product": "ES",
            "contract": self.source.contract,
            "instrument_id": self.source.instrument_id,
            "minute_start_ns": int(value.minute_start_ns),
            "source_close_ns": int(value.minute_start_ns) + MINUTE_NS,
            "availability_ns": int(value.minute_start_ns) + MINUTE_NS,
            "trade_count": value.trade_count,
            "total_volume": value.total_volume,
            "buy_volume": value.buy_volume,
            "sell_volume": value.sell_volume,
            "neutral_volume": value.neutral_volume,
            "signed_volume": value.buy_volume - value.sell_volume,
            "signed_flow_fraction": (
                (value.buy_volume - value.sell_volume) / value.total_volume
            ),
            "unique_tick_count": unique_tick_count,
            "occupancy_entropy": entropy,
            "mode_tick": mode_tick,
            "second_mode_tick": (
                float(second_mode_tick) if second_mode_tick is not None else math.nan
            ),
            "mode_volume": mode_volume,
            "second_mode_volume": second_mode_volume,
            "mode_volume_share": mode_volume / value.total_volume,
            "top_two_volume_share": (
                (mode_volume + second_mode_volume) / value.total_volume
            ),
            "second_to_first_mode_ratio": second_mode_volume / mode_volume,
            "mode_signed_flow_fraction": (
                value.level_signed_volume.get(mode_tick, 0) / mode_volume
            ),
            "adjacent_tick_transition_count": value.adjacent_tick_transition_count,
            "revisit_count": value.revisit_count,
            "revisit_ratio": (
                value.revisit_count / value.adjacent_tick_transition_count
                if value.adjacent_tick_transition_count
                else 0.0
            ),
            "first_tick": value.first_tick,
            "last_tick": value.last_tick,
            "low_tick": min(value.level_volume),
            "high_tick": max(value.level_volume),
            "last_minus_mode_ticks": value.last_tick - mode_tick,
            "maximum_excursion_from_mode_ticks": maximum_excursion,
            "maximum_excursion_direction": maximum_excursion_direction,
            "first_price": value.first_tick * TICK_SIZE,
            "last_price": value.last_tick * TICK_SIZE,
            "transformation_version": TRANSFORMATION_VERSION,
        }
        self.rows.append(row)
        self.current = OccupancyMinuteAccumulator()


def build_executed_price_occupancy_store(
    project_root: str | Path = ".",
    *,
    chunk_size: int = 1_000_000,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if _sha256(root / GRAMMAR_PATH) != GRAMMAR_SHA256:
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy grammar WORM drift"
        )
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for source in SOURCES:
        source_path = root / source.path
        if _sha256(source_path) != source.sha256:
            raise V72ExecutedPriceOccupancyStoreError(
                f"executed-price occupancy raw source drift for {source.calendar_year}"
            )
        frame, audit = aggregate_dbn_source(
            source_path, source, chunk_size=chunk_size
        )
        frames.append(frame)
        audits.append(audit)
    output = pd.concat(frames, ignore_index=True).sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    output["mode_migration_ticks"] = _compute_mode_migration(output)
    _validate_frame(output)

    destination = root / OUTPUT_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    output.to_parquet(temporary, index=False, compression="zstd")
    os.replace(temporary, destination)
    output_hash = _sha256(destination)
    manifest = {
        "schema": "hydra_v7_d1_executed_price_occupancy_manifest_v1",
        "transformation_version": TRANSFORMATION_VERSION,
        "preregistration_path": GRAMMAR_PATH,
        "preregistration_sha256": GRAMMAR_SHA256,
        "data_role": "DEVELOPMENT_ONLY",
        "sources": [asdict(source) for source in SOURCES],
        "session_filter": {
            "timezone": "America/Chicago",
            "start_inclusive": "08:30:00",
            "end_exclusive": "15:10:00",
            "weekdays_only": True,
        },
        "feature_policy": {
            "price_tick": "nearest integer quarter-point ES tick",
            "mode_tie_break": "maximum volume, nearest exact volume-weighted mean, then lower tick",
            "second_mode_tie_break": "same ordering after removal of the first mode",
            "occupancy_entropy_normalization": "log(unique_tick_count), zero for one level",
            "revisit": "transition into a previously seen tick after an intervening different tick",
            "revisit_denominator": "adjacent unequal-tick transition count",
            "aggressor_sign": {"B": 1, "A": -1, "N": 0},
            "mode_migration": "current minus prior contiguous completed-minute mode in same contract and CT session",
            "availability": "minute_start_ns+60s",
        },
        "output": {
            "path": OUTPUT_PATH,
            "sha256": output_hash,
            "row_count": len(output),
            "columns": list(output.columns),
            "first_minute_start_ns": int(output["minute_start_ns"].min()),
            "last_minute_start_ns": int(output["minute_start_ns"].max()),
            "contracts": sorted(output["contract"].unique().tolist()),
            "session_count": int(
                output[["calendar_year", "contract", "session_date"]]
                .drop_duplicates()
                .shape[0]
            ),
            "contiguous_mode_migration_count": int(
                output["mode_migration_ticks"].notna().sum()
            ),
        },
        "audits": audits,
        "outcome_or_future_pnl_columns": [],
        "new_data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The store is deterministic and closed-minute only, but occupancy can "
            "still be a transformation of contemporaneous range and volatility; "
            "walk-forward and the frozen class tripwire remain mandatory."
        ),
    }
    manifest_path = root / MANIFEST_PATH
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_manifest = manifest_path.with_name(f".{manifest_path.name}.tmp")
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary_manifest, manifest_path)
    return manifest


def aggregate_dbn_source(
    path: str | Path,
    source: SourceSpec,
    *,
    chunk_size: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    import databento as db

    store = db.DBNStore.from_file(path)
    if str(store.metadata.dataset) != "GLBX.MDP3" or str(store.metadata.schema) != "trades":
        raise V72ExecutedPriceOccupancyStoreError(
            "unexpected executed-price occupancy DBN metadata"
        )
    stream = OccupancyStreamAccumulator(source=source)
    raw_count = bounded_count = retained_count = 0
    invalid_action = excluded_instrument = excluded_session = 0
    for chunk in store.to_ndarray(count=chunk_size):
        if len(chunk) == 0:
            continue
        raw_count += len(chunk)
        ts = np.asarray(chunk["ts_event"], dtype=np.int64)
        bounded = (ts >= source.start_ns) & (ts < source.end_ns)
        bounded_count += int(np.count_nonzero(bounded))
        actions = np.asarray(chunk["action"])
        valid_action = actions == b"T"
        invalid_action += int(np.count_nonzero(bounded & ~valid_action))
        instruments = np.asarray(chunk["instrument_id"], dtype=np.int64)
        valid_instrument = instruments == source.instrument_id
        excluded_instrument += int(
            np.count_nonzero(bounded & valid_action & ~valid_instrument)
        )
        timestamps = pd.to_datetime(ts, unit="ns", utc=True).tz_convert(
            "America/Chicago"
        )
        minutes_ct = np.asarray(timestamps.hour * 60 + timestamps.minute)
        weekdays = np.asarray(timestamps.weekday)
        rth = (
            (weekdays < 5)
            & (minutes_ct >= 8 * 60 + 30)
            & (minutes_ct < 15 * 60 + 10)
        )
        eligible_before_session = bounded & valid_action & valid_instrument
        excluded_session += int(np.count_nonzero(eligible_before_session & ~rth))
        keep = eligible_before_session & rth
        if not np.any(keep):
            continue
        retained = chunk[keep]
        retained_count += len(retained)
        stream.ingest(retained)
    frame = stream.finish()
    if retained_count <= 0 or frame.empty:
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy source retained no observations"
        )
    return frame, {
        "calendar_year": source.calendar_year,
        "contract": source.contract,
        "raw_record_count": raw_count,
        "bounded_record_count": bounded_count,
        "retained_es_rth_record_count": retained_count,
        "invalid_action_record_count": invalid_action,
        "excluded_instrument_record_count": excluded_instrument,
        "excluded_session_record_count": excluded_session,
        "minute_group_count": len(frame),
        "trade_count_reconciled": int(frame["trade_count"].sum()) == retained_count,
        "volume_reconciled": bool(
            (
                frame["buy_volume"]
                + frame["sell_volume"]
                + frame["neutral_volume"]
                == frame["total_volume"]
            ).all()
        ),
    }


def _accumulate_records(
    stream: OccupancyStreamAccumulator,
    records: np.ndarray,
) -> None:
    if len(records) == 0:
        return
    required = {"ts_event", "price", "size", "side"}
    if not required.issubset(records.dtype.names or ()):
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy record fields missing"
        )
    timestamps = np.asarray(records["ts_event"], dtype=np.int64)
    if int(timestamps[0]) < stream.last_event_ns or np.any(np.diff(timestamps) < 0):
        raise V72ExecutedPriceOccupancyStoreError(
            "explicit-contract exchange-event ordering failed"
        )
    sides = np.asarray(records["side"])
    if not np.all(np.isin(sides, [b"A", b"B", b"N"])):
        raise V72ExecutedPriceOccupancyStoreError("unknown aggressor-side code")
    for record in records:
        ts = int(record["ts_event"])
        minute_start = (ts // MINUTE_NS) * MINUTE_NS
        if stream.current.minute_start_ns != minute_start:
            stream._flush_current()
            stream.current.minute_start_ns = minute_start
        current = stream.current
        size = int(record["size"])
        if size <= 0:
            raise V72ExecutedPriceOccupancyStoreError("nonpositive trade size")
        tick = _raw_price_to_tick(int(record["price"]))
        raw_side = bytes(record["side"])
        if current.first_tick is None:
            current.first_tick = tick
        if current.previous_tick is not None and tick != current.previous_tick:
            current.adjacent_tick_transition_count += 1
            if tick in current.seen_ticks:
                current.revisit_count += 1
        current.seen_ticks.add(tick)
        current.previous_tick = tick
        current.last_tick = tick
        current.ordered_ticks.append(tick)
        current.trade_count += 1
        current.total_volume += size
        current.weighted_tick_volume += tick * size
        current.level_volume[tick] = current.level_volume.get(tick, 0) + size
        signed_size = 0
        if raw_side == b"B":
            current.buy_volume += size
            signed_size = size
        elif raw_side == b"A":
            current.sell_volume += size
            signed_size = -size
        else:
            current.neutral_volume += size
        current.level_signed_volume[tick] = (
            current.level_signed_volume.get(tick, 0) + signed_size
        )
    stream.last_event_ns = int(timestamps[-1])


def _raw_price_to_tick(raw_price: int) -> int:
    if raw_price <= 0:
        raise V72ExecutedPriceOccupancyStoreError("nonpositive ES trade price")
    tick = (raw_price + TICK_RAW // 2) // TICK_RAW
    if abs(raw_price - tick * TICK_RAW) > TICK_RAW // 2:
        raise V72ExecutedPriceOccupancyStoreError("ES tick rounding failure")
    return int(tick)


def _rank_occupancy_levels(
    level_volume: dict[int, int],
    *,
    weighted_tick_volume: int,
    total_volume: int,
) -> tuple[int, int | None]:
    if not level_volume or total_volume <= 0:
        raise V72ExecutedPriceOccupancyStoreError("cannot rank empty occupancy levels")
    ranked = sorted(
        level_volume,
        key=lambda tick: (
            -level_volume[tick],
            abs(tick * total_volume - weighted_tick_volume),
            tick,
        ),
    )
    return ranked[0], ranked[1] if len(ranked) > 1 else None


def _compute_mode_migration(frame: pd.DataFrame) -> pd.Series:
    previous_mode = frame.groupby(
        ["calendar_year", "contract", "session_date"], sort=False
    )["mode_tick"].shift(1)
    previous_minute = frame.groupby(
        ["calendar_year", "contract", "session_date"], sort=False
    )["minute_start_ns"].shift(1)
    contiguous = (frame["minute_start_ns"] - previous_minute) == MINUTE_NS
    result = (frame["mode_tick"] - previous_mode).astype(float)
    return result.where(contiguous)


def _validate_frame(frame: pd.DataFrame) -> None:
    if frame.empty or frame.duplicated(
        ["calendar_year", "contract", "minute_start_ns"]
    ).any():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy output identity failure"
        )
    if len(frame) != EXPECTED_MINUTE_COUNT:
        raise V72ExecutedPriceOccupancyStoreError(
            f"executed-price occupancy minute count drift: {len(frame)}"
        )
    if set(frame["contract"].unique()) != {"ESU3", "ESU4"}:
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy explicit-contract drift"
        )
    if not (frame["availability_ns"] == frame["minute_start_ns"] + MINUTE_NS).all():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy availability drift"
        )
    if (frame["trade_count"] <= 0).any() or (frame["total_volume"] <= 0).any():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy empty minute"
        )
    if not frame["signed_flow_fraction"].between(-1.0, 1.0).all():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy signed flow invalid"
        )
    bounded_columns = [
        "occupancy_entropy",
        "mode_volume_share",
        "top_two_volume_share",
        "second_to_first_mode_ratio",
        "revisit_ratio",
    ]
    for column in bounded_columns:
        if not frame[column].between(0.0, 1.0).all():
            raise V72ExecutedPriceOccupancyStoreError(
                f"executed-price occupancy bounded feature invalid: {column}"
            )
    if not frame["mode_signed_flow_fraction"].between(-1.0, 1.0).all():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy mode flow invalid"
        )
    if not (frame["unique_tick_count"] >= 1).all():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy unique-tick count invalid"
        )
    if not (
        frame["revisit_count"] <= frame["adjacent_tick_transition_count"]
    ).all():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy revisit reconciliation failed"
        )
    if not (
        (frame["mode_tick"] >= frame["low_tick"])
        & (frame["mode_tick"] <= frame["high_tick"])
    ).all():
        raise V72ExecutedPriceOccupancyStoreError(
            "executed-price occupancy mode outside traded range"
        )


def _sign(value: int | float) -> int:
    return int(value > 0) - int(value < 0)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "OccupancyMinuteAccumulator",
    "OccupancyStreamAccumulator",
    "SourceSpec",
    "V72ExecutedPriceOccupancyStoreError",
    "_accumulate_records",
    "_rank_occupancy_levels",
    "build_executed_price_occupancy_store",
]
