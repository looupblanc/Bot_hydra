from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from hydra.data.v7_trade_feature_store import (
    ContractDefinition,
    _merge_chunk_groups,
    accumulators_to_frame,
)


PLAN_PATH = "WORM/v7-d1-event-representations-2026-07-12.json"
PLAN_SHA256 = "5a72dd7f991dd62c387b101c857e562b4fca05414b6340e41ec6388e16ea1d5b"
DEFINITION_PATH = (
    "data/cache/contract_maps/"
    "definitions_GLBX-MDP3_2023-01-01_2024-10-01_1faa319bb6354a47.dbn.zst"
)
DEFINITION_SHA256 = "a5374a723fdf442f8c5e31f98af26ff813070a61008d24e46f848df69c819dcc"
MINUTE_OUTPUT = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
EVENT_OUTPUT = "data/cache/v7_d1/date_matched_event_bars_v1.parquet"
MANIFEST_PATH = "data/manifests/v7_d1_date_matched_event_store_v1.json"
TRANSFORMATION_VERSION = "hydra_v7_d1_date_matched_event_store_v1"
PRICE_SCALE = 1_000_000_000.0
MINUTE_NS = 60_000_000_000


class D1EventStoreError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SourceSpec:
    year: int
    path: str
    sha256: str
    start_ns: int
    end_ns: int
    instrument_ids: frozenset[int]


SOURCES = (
    SourceSpec(
        year=2023,
        path=(
            "data/cache/databento_v7_d1/"
            "GLBX-MDP3_trades_ES_MES_2023-08-02_2023-09-01.dbn.zst"
        ),
        sha256="d93e5c8bd657a28bff5f05560b845caa3bb04e9252882ddc2751033fedd1c468",
        start_ns=1_690_934_400_000_000_000,
        end_ns=1_693_526_400_000_000_000,
        instrument_ids=frozenset({3445, 2922}),
    ),
    SourceSpec(
        year=2024,
        path=(
            "data/cache/databento_v7_d1/"
            "GLBX-MDP3_trades_ES_MES_2024-08-01_2024-10-01.dbn.zst"
        ),
        sha256="a312d6df524205c2d1632c20f1033ecb74081c98d6608eacc1a9cdb08bcf9b63",
        start_ns=1_722_556_800_000_000_000,
        end_ns=1_725_148_800_000_000_000,
        instrument_ids=frozenset({118, 7114}),
    ),
)
EXPECTED_CONTRACTS = {
    3445: ("ES", "ESU3", 50.0),
    2922: ("MES", "MESU3", 5.0),
    118: ("ES", "ESU4", 50.0),
    7114: ("MES", "MESU4", 5.0),
}
VOLUME_THRESHOLDS = {"ES": 1000.0, "MES": 250.0}
IMBALANCE_THRESHOLDS = {"ES": 500.0, "MES": 125.0}
DOLLAR_THRESHOLDS = {"ES": 25_000_000.0, "MES": 2_500_000.0}


def build_date_matched_event_store(
    project_root: str | Path = ".",
    *,
    chunk_size: int = 1_000_000,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if _sha256(root / PLAN_PATH) != PLAN_SHA256:
        raise D1EventStoreError("D1 event representation WORM hash mismatch")
    definitions_path = root / DEFINITION_PATH
    if _sha256(definitions_path) != DEFINITION_SHA256:
        raise D1EventStoreError("D1 explicit definitions hash mismatch")
    for source in SOURCES:
        if _sha256(root / source.path) != source.sha256:
            raise D1EventStoreError(f"D1 raw hash mismatch for {source.year}")

    contracts = load_contract_definitions(definitions_path)
    minute_frames: list[pd.DataFrame] = []
    event_frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for source in SOURCES:
        records, audit = load_rth_records(
            root / source.path,
            source,
            chunk_size=chunk_size,
        )
        source_contracts = {
            instrument_id: contracts[instrument_id]
            for instrument_id in source.instrument_ids
        }
        minute_accumulators: dict[tuple[int, int], Any] = {}
        _merge_chunk_groups(minute_accumulators, records)
        minute = accumulators_to_frame(minute_accumulators, source_contracts)
        minute.insert(3, "calendar_year", source.year)
        minute["transformation_version"] = TRANSFORMATION_VERSION
        minute_frames.append(minute)

        for instrument_id in sorted(source.instrument_ids):
            instrument_records = records[records["instrument_id"] == instrument_id]
            definition = source_contracts[instrument_id]
            for bar_type, threshold in (
                ("VOLUME_BAR", VOLUME_THRESHOLDS[definition.product]),
                (
                    "SIGNED_IMBALANCE_BAR",
                    IMBALANCE_THRESHOLDS[definition.product],
                ),
                ("DOLLAR_BAR", DOLLAR_THRESHOLDS[definition.product]),
            ):
                event_frames.append(
                    build_event_bar_frame(
                        instrument_records,
                        definition,
                        calendar_year=source.year,
                        bar_type=bar_type,
                        threshold=threshold,
                    )
                )
        audit["minute_group_count"] = len(minute)
        audit["event_bar_count"] = sum(
            len(frame)
            for frame in event_frames
            if not frame.empty and int(frame["calendar_year"].iloc[0]) == source.year
        )
        audits.append(audit)
        del records, minute_accumulators

    minute_frame = pd.concat(minute_frames, ignore_index=True).sort_values(
        ["minute_start_ns", "instrument_id"], kind="stable"
    )
    event_frame = pd.concat(event_frames, ignore_index=True).sort_values(
        ["start_event_ns", "instrument_id", "bar_type"], kind="stable"
    )
    minute_path = root / MINUTE_OUTPUT
    event_path = root / EVENT_OUTPUT
    minute_path.parent.mkdir(parents=True, exist_ok=True)
    minute_frame.to_parquet(minute_path, index=False, compression="zstd")
    event_frame.to_parquet(event_path, index=False, compression="zstd")
    _validate_outputs(minute_frame, event_frame)

    event_counts = {
        f"{year}:{product}:{bar_type}": int(count)
        for (year, product, bar_type), count in event_frame.groupby(
            ["calendar_year", "product", "bar_type"], sort=True
        ).size().items()
    }
    manifest = {
        "schema": "hydra_v7_d1_date_matched_event_store_manifest_v1",
        "transformation_version": TRANSFORMATION_VERSION,
        "preregistration_path": PLAN_PATH,
        "preregistration_sha256": PLAN_SHA256,
        "data_role": "DEVELOPMENT_ONLY",
        "sources": [
            {
                "year": source.year,
                "path": source.path,
                "sha256": source.sha256,
                "slice_start_ns": source.start_ns,
                "slice_end_ns": source.end_ns,
                "instrument_ids": sorted(source.instrument_ids),
            }
            for source in SOURCES
        ],
        "definitions": {
            "path": DEFINITION_PATH,
            "sha256": DEFINITION_SHA256,
            "contracts": sorted(row.raw_symbol for row in contracts.values()),
        },
        "session_filter": {
            "timezone": "America/Chicago",
            "start_inclusive": "08:30:00",
            "end_exclusive": "15:10:00",
            "weekdays_only": True,
        },
        "thresholds": {
            "volume": VOLUME_THRESHOLDS,
            "signed_imbalance": IMBALANCE_THRESHOLDS,
            "dollar": DOLLAR_THRESHOLDS,
        },
        "minute_output": {
            "path": MINUTE_OUTPUT,
            "sha256": _sha256(minute_path),
            "row_count": len(minute_frame),
        },
        "event_output": {
            "path": EVENT_OUTPUT,
            "sha256": _sha256(event_path),
            "row_count": len(event_frame),
            "counts": event_counts,
        },
        "audits": audits,
        "outcome_or_pnl_columns": [],
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The event bars are deterministic and native to prints, but fixed "
            "thresholds can induce different sampling rates across contracts; "
            "the new-dataset tripwire remains mandatory."
        ),
    }
    manifest_path = root / MANIFEST_PATH
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def load_contract_definitions(path: str | Path) -> dict[int, ContractDefinition]:
    import databento as db

    required = set(EXPECTED_CONTRACTS)
    selected: dict[int, ContractDefinition] = {}
    store = db.DBNStore.from_file(path)
    for chunk in store.to_ndarray(count=100_000):
        mask = np.isin(chunk["instrument_id"], list(required))
        for row in chunk[mask]:
            instrument_id = int(row["instrument_id"])
            product, expected_symbol, expected_value = EXPECTED_CONTRACTS[
                instrument_id
            ]
            raw_symbol = bytes(row["raw_symbol"]).rstrip(b"\x00").decode("ascii")
            definition = ContractDefinition(
                instrument_id=instrument_id,
                product=product,
                raw_symbol=raw_symbol,
                min_price_increment=float(row["min_price_increment"])
                / PRICE_SCALE,
                point_value=float(row["unit_of_measure_qty"]) / PRICE_SCALE,
            )
            if raw_symbol != expected_symbol:
                raise D1EventStoreError("D1 explicit contract symbol drift")
            if not math.isclose(definition.min_price_increment, 0.25) or not math.isclose(
                definition.point_value, expected_value
            ):
                raise D1EventStoreError("D1 contract tick or multiplier drift")
            prior = selected.get(instrument_id)
            if prior is not None and prior != definition:
                raise D1EventStoreError("D1 contract definition drift")
            selected[instrument_id] = definition
    if set(selected) != required:
        raise D1EventStoreError("D1 explicit definitions are incomplete")
    return selected


def load_rth_records(
    path: str | Path,
    source: SourceSpec,
    *,
    chunk_size: int,
) -> tuple[np.ndarray, dict[str, Any]]:
    import databento as db

    store = db.DBNStore.from_file(path)
    if (
        str(store.metadata.dataset) != "GLBX.MDP3"
        or str(store.metadata.schema) != "trades"
    ):
        raise D1EventStoreError("unexpected D1 source metadata")
    retained: list[np.ndarray] = []
    raw_count = bounded_count = rth_count = 0
    invalid_action = invalid_instrument = 0
    last_receive = -1
    for chunk in store.to_ndarray(count=chunk_size):
        if len(chunk) == 0:
            continue
        raw_count += len(chunk)
        receive_field = "ts_recv" if "ts_recv" in chunk.dtype.names else "ts_event"
        receive = np.asarray(chunk[receive_field], dtype=np.int64)
        if int(receive[0]) < last_receive or np.any(np.diff(receive) < 0):
            raise D1EventStoreError("D1 receive-time ordering failed")
        last_receive = int(receive[-1])
        ts = np.asarray(chunk["ts_event"], dtype=np.int64)
        bounded = (ts >= source.start_ns) & (ts < source.end_ns)
        bounded_count += int(np.count_nonzero(bounded))
        action = np.asarray(chunk["action"])
        valid_action = action == b"T"
        invalid_action += int(np.count_nonzero(bounded & ~valid_action))
        instruments = np.asarray(chunk["instrument_id"], dtype=np.int64)
        valid_instrument = np.isin(instruments, list(source.instrument_ids))
        invalid_instrument += int(
            np.count_nonzero(bounded & valid_action & ~valid_instrument)
        )
        sides = np.asarray(chunk["side"])
        if not np.all(np.isin(sides[bounded & valid_action], [b"A", b"B", b"N"])):
            raise D1EventStoreError("unknown D1 aggressor-side code")
        timestamps = pd.to_datetime(ts, unit="ns", utc=True).tz_convert(
            "America/Chicago"
        )
        minutes = np.asarray(timestamps.hour * 60 + timestamps.minute)
        weekdays = np.asarray(timestamps.weekday)
        rth = (
            (weekdays < 5)
            & (minutes >= 8 * 60 + 30)
            & (minutes < 15 * 60 + 10)
        )
        keep = bounded & valid_action & valid_instrument & rth
        rth_count += int(np.count_nonzero(keep))
        if np.any(keep):
            retained.append(np.array(chunk[keep], copy=True))
    if not retained:
        raise D1EventStoreError("D1 matched source retained no RTH records")
    records = np.concatenate(retained)
    ts = np.asarray(records["ts_event"], dtype=np.int64)
    receive_field = "ts_recv" if "ts_recv" in records.dtype.names else "ts_event"
    receive = np.asarray(records[receive_field], dtype=np.int64)
    sequence = (
        np.asarray(records["sequence"], dtype=np.int64)
        if "sequence" in records.dtype.names
        else np.arange(len(records), dtype=np.int64)
    )
    instruments = np.asarray(records["instrument_id"], dtype=np.int64)
    order = np.lexsort((sequence, receive, ts, instruments))
    records = records[order]
    return records, {
        "year": source.year,
        "raw_record_count": raw_count,
        "bounded_record_count": bounded_count,
        "retained_rth_record_count": rth_count,
        "invalid_action_record_count": invalid_action,
        "excluded_instrument_record_count": invalid_instrument,
    }


def build_event_bar_frame(
    records: np.ndarray,
    definition: ContractDefinition,
    *,
    calendar_year: int,
    bar_type: str,
    threshold: float,
) -> pd.DataFrame:
    if len(records) == 0:
        return pd.DataFrame()
    prices = np.asarray(records["price"], dtype=np.float64) / PRICE_SCALE
    sizes = np.asarray(records["size"], dtype=np.int64)
    sides = np.asarray(records["side"])
    signed = np.where(sides == b"B", sizes, np.where(sides == b"A", -sizes, 0))
    if bar_type == "VOLUME_BAR":
        boundaries = _positive_measure_boundaries(sizes.astype(np.float64), threshold)
    elif bar_type == "DOLLAR_BAR":
        measure = prices * definition.point_value * sizes
        boundaries = _positive_measure_boundaries(measure, threshold)
    elif bar_type == "SIGNED_IMBALANCE_BAR":
        boundaries = _signed_imbalance_boundaries(signed, threshold)
    else:
        raise D1EventStoreError(f"unsupported event bar type: {bar_type}")
    if len(boundaries) == 0:
        return pd.DataFrame()
    limit = int(boundaries[-1]) + 1
    records = records[:limit]
    prices = prices[:limit]
    sizes = sizes[:limit]
    sides = sides[:limit]
    signed = signed[:limit]
    starts = np.concatenate(([0], boundaries[:-1] + 1))
    counts = boundaries - starts + 1
    steps = np.concatenate(([0.0], np.abs(np.diff(prices))))
    steps[starts] = 0.0
    total_volume = np.add.reduceat(sizes, starts)
    buy = np.add.reduceat(np.where(sides == b"B", sizes, 0), starts)
    sell = np.add.reduceat(np.where(sides == b"A", sizes, 0), starts)
    unknown = np.add.reduceat(np.where(sides == b"N", sizes, 0), starts)
    signed_volume = np.add.reduceat(signed, starts)
    price_size = np.add.reduceat(prices * sizes, starts)
    receive_field = "ts_recv" if "ts_recv" in records.dtype.names else "ts_event"
    receive = np.asarray(records[receive_field], dtype=np.int64)
    frame = pd.DataFrame(
        {
            "product": definition.product,
            "contract": definition.raw_symbol,
            "instrument_id": definition.instrument_id,
            "calendar_year": calendar_year,
            "bar_type": bar_type,
            "bar_sequence": np.arange(len(boundaries), dtype=np.int64),
            "start_event_ns": np.asarray(records["ts_event"], dtype=np.int64)[starts],
            "end_event_ns": np.asarray(records["ts_event"], dtype=np.int64)[boundaries],
            "availability_ns": np.maximum.reduceat(receive, starts),
            "open": prices[starts],
            "high": np.maximum.reduceat(prices, starts),
            "low": np.minimum.reduceat(prices, starts),
            "close": prices[boundaries],
            "vwap": price_size / total_volume,
            "trade_count": counts,
            "total_volume": total_volume,
            "buy_aggressor_volume": buy,
            "sell_aggressor_volume": sell,
            "unknown_side_volume": unknown,
            "signed_aggressor_volume": signed_volume,
            "price_change_points": prices[boundaries] - prices[starts],
            "path_length_points": np.add.reduceat(steps, starts),
            "threshold": threshold,
            "transformation_version": TRANSFORMATION_VERSION,
        }
    )
    if not np.all(frame["availability_ns"] >= frame["end_event_ns"]):
        raise D1EventStoreError("event bars use unavailable prints")
    return frame


def _positive_measure_boundaries(measure: np.ndarray, threshold: float) -> np.ndarray:
    if threshold <= 0.0 or np.any(measure < 0.0):
        raise D1EventStoreError("positive event measure is invalid")
    cumulative = np.cumsum(measure, dtype=np.float64)
    boundaries: list[int] = []
    base = 0.0
    while True:
        position = int(np.searchsorted(cumulative, base + threshold, side="left"))
        if position >= len(cumulative):
            break
        boundaries.append(position)
        base = float(cumulative[position])
    return np.asarray(boundaries, dtype=np.int64)


def _signed_imbalance_boundaries(
    signed_measure: np.ndarray, threshold: float
) -> np.ndarray:
    if threshold <= 0.0:
        raise D1EventStoreError("signed imbalance threshold is invalid")
    boundaries: list[int] = []
    accumulator = 0.0
    for index, value in enumerate(signed_measure):
        accumulator += float(value)
        if abs(accumulator) >= threshold:
            boundaries.append(index)
            accumulator = 0.0
    return np.asarray(boundaries, dtype=np.int64)


def _validate_outputs(minute: pd.DataFrame, event: pd.DataFrame) -> None:
    if minute.empty or event.empty:
        raise D1EventStoreError("D1 output is empty")
    if set(minute["calendar_year"]) != {2023, 2024} or set(
        event["calendar_year"]
    ) != {2023, 2024}:
        raise D1EventStoreError("D1 date-matched years are incomplete")
    if set(event["bar_type"]) != {
        "VOLUME_BAR",
        "SIGNED_IMBALANCE_BAR",
        "DOLLAR_BAR",
    }:
        raise D1EventStoreError("D1 event representation coverage drift")
    forbidden = {
        column
        for column in [*minute.columns, *event.columns]
        if "pnl" in column.lower()
    }
    if forbidden:
        raise D1EventStoreError("D1 features contain outcomes")
    if not np.all(minute["availability_ns"] > minute["last_trade_ns"]):
        raise D1EventStoreError("D1 minute availability is not closed")
    if not np.all(event["availability_ns"] >= event["end_event_ns"]):
        raise D1EventStoreError("D1 event availability is not closed")


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "D1EventStoreError",
    "SourceSpec",
    "build_date_matched_event_store",
    "build_event_bar_frame",
    "load_contract_definitions",
    "load_rth_records",
]
