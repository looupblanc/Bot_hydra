from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


GRAMMAR_PATH = "WORM/v7.1-intraminute-flow-grammar-0008-2026-07-13.json"
GRAMMAR_SHA256 = "36f5d4f8dd2582979d809925782881fb1e159d23ddfbd50dc6a9d348cf5c18dc"
OUTPUT_PATH = "data/cache/v7_d1/date_matched_intraminute_flow_v1.parquet"
MANIFEST_PATH = "data/manifests/v7_d1_intraminute_flow_v1.json"
TRANSFORMATION_VERSION = "hydra_v7_d1_intraminute_flow_v1"
MINUTE_NS = 60_000_000_000
HALF_MINUTE_NS = 30_000_000_000


class V71IntraminuteFlowStoreError(RuntimeError):
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
        path="data/cache/databento_v7_d1/GLBX-MDP3_trades_ES_MES_2023-08-02_2023-09-01.dbn.zst",
        sha256="d93e5c8bd657a28bff5f05560b845caa3bb04e9252882ddc2751033fedd1c468",
        start_ns=1_690_934_400_000_000_000,
        end_ns=1_693_526_400_000_000_000,
        instrument_id=3445,
        contract="ESU3",
    ),
    SourceSpec(
        calendar_year=2024,
        path="data/cache/databento_v7_d1/GLBX-MDP3_trades_ES_MES_2024-08-01_2024-10-01.dbn.zst",
        sha256="a312d6df524205c2d1632c20f1033ecb74081c98d6608eacc1a9cdb08bcf9b63",
        start_ns=1_722_556_800_000_000_000,
        end_ns=1_725_148_800_000_000_000,
        instrument_id=118,
        contract="ESU4",
    ),
)


@dataclass(slots=True)
class HalfMinuteAccumulator:
    first_trade_count: int = 0
    second_trade_count: int = 0
    first_total_volume: int = 0
    second_total_volume: int = 0
    first_signed_flow: int = 0
    second_signed_flow: int = 0


def build_intraminute_flow_store(
    project_root: str | Path = ".",
    *,
    chunk_size: int = 1_000_000,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if _sha256(root / GRAMMAR_PATH) != GRAMMAR_SHA256:
        raise V71IntraminuteFlowStoreError("intraminute grammar WORM drift")
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for source in SOURCES:
        source_path = root / source.path
        if _sha256(source_path) != source.sha256:
            raise V71IntraminuteFlowStoreError(
                f"intraminute raw source drift for {source.calendar_year}"
            )
        accumulators, audit = aggregate_dbn_source(
            source_path, source, chunk_size=chunk_size
        )
        frames.append(accumulators_to_frame(accumulators, source))
        audits.append(audit)
    frame = pd.concat(frames, ignore_index=True).sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    _validate_frame(frame)
    destination = root / OUTPUT_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    frame.to_parquet(temporary, index=False, compression="zstd")
    os.replace(temporary, destination)
    output_hash = _sha256(destination)
    manifest = {
        "schema": "hydra_v7_d1_intraminute_flow_manifest_v1",
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
            "first_half": "[minute_start,minute_start+30s)",
            "second_half": "[minute_start+30s,minute_start+60s)",
            "aggressor_sign": {"B": 1, "A": -1, "N": 0},
            "availability": "minute_start_ns+60s",
        },
        "output": {
            "path": OUTPUT_PATH,
            "sha256": output_hash,
            "row_count": len(frame),
            "columns": list(frame.columns),
            "first_minute_start_ns": int(frame["minute_start_ns"].min()),
            "last_minute_start_ns": int(frame["minute_start_ns"].max()),
            "contracts": sorted(frame["contract"].unique().tolist()),
        },
        "audits": audits,
        "outcome_or_pnl_columns": [],
        "new_data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Half-minute aggregation is deterministic and outcome-free, but the "
            "30-second boundary can remain an arbitrary clock partition; the permanent "
            "price-null tripwire is mandatory."
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
) -> tuple[dict[int, HalfMinuteAccumulator], dict[str, Any]]:
    import databento as db

    store = db.DBNStore.from_file(path)
    if str(store.metadata.dataset) != "GLBX.MDP3" or str(store.metadata.schema) != "trades":
        raise V71IntraminuteFlowStoreError("unexpected intraminute DBN metadata")
    accumulators: dict[int, HalfMinuteAccumulator] = {}
    raw_count = bounded_count = retained_count = 0
    invalid_action = excluded_instrument = excluded_session = 0
    last_receive = -1
    for chunk in store.to_ndarray(count=chunk_size):
        if len(chunk) == 0:
            continue
        raw_count += len(chunk)
        receive_field = "ts_recv" if "ts_recv" in chunk.dtype.names else "ts_event"
        receive = np.asarray(chunk[receive_field], dtype=np.int64)
        if int(receive[0]) < last_receive or np.any(np.diff(receive) < 0):
            raise V71IntraminuteFlowStoreError("intraminute receive ordering failed")
        last_receive = int(receive[-1])
        ts = np.asarray(chunk["ts_event"], dtype=np.int64)
        bounded = (ts >= source.start_ns) & (ts < source.end_ns)
        bounded_count += int(np.count_nonzero(bounded))
        actions = np.asarray(chunk["action"])
        valid_action = actions == b"T"
        invalid_action += int(np.count_nonzero(bounded & ~valid_action))
        instruments = np.asarray(chunk["instrument_id"], dtype=np.int64)
        valid_instrument = instruments == source.instrument_id
        excluded_instrument += int(np.count_nonzero(bounded & valid_action & ~valid_instrument))
        timestamps = pd.to_datetime(ts, unit="ns", utc=True).tz_convert("America/Chicago")
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
        retained_count += int(np.count_nonzero(keep))
        _accumulate_chunk(accumulators, chunk[keep])
    if retained_count <= 0 or not accumulators:
        raise V71IntraminuteFlowStoreError("intraminute source retained no observations")
    return accumulators, {
        "calendar_year": source.calendar_year,
        "contract": source.contract,
        "raw_record_count": raw_count,
        "bounded_record_count": bounded_count,
        "retained_es_rth_record_count": retained_count,
        "invalid_action_record_count": invalid_action,
        "excluded_instrument_record_count": excluded_instrument,
        "excluded_session_record_count": excluded_session,
        "minute_group_count": len(accumulators),
    }


def _accumulate_chunk(
    destination: dict[int, HalfMinuteAccumulator],
    records: np.ndarray,
) -> None:
    ts = np.asarray(records["ts_event"], dtype=np.int64)
    minute = (ts // MINUTE_NS) * MINUTE_NS
    second_half = (ts - minute) >= HALF_MINUTE_NS
    sizes = np.asarray(records["size"], dtype=np.int64)
    sides = np.asarray(records["side"])
    if not np.all(np.isin(sides, [b"A", b"B", b"N"])):
        raise V71IntraminuteFlowStoreError("unknown aggressor-side code")
    signed = np.where(sides == b"B", sizes, np.where(sides == b"A", -sizes, 0))
    unique, inverse = np.unique(minute, return_inverse=True)
    first = ~second_half
    for index, minute_start in enumerate(unique):
        group = inverse == index
        first_group = group & first
        second_group = group & second_half
        row = destination.setdefault(int(minute_start), HalfMinuteAccumulator())
        row.first_trade_count += int(np.count_nonzero(first_group))
        row.second_trade_count += int(np.count_nonzero(second_group))
        row.first_total_volume += int(np.sum(sizes[first_group]))
        row.second_total_volume += int(np.sum(sizes[second_group]))
        row.first_signed_flow += int(np.sum(signed[first_group]))
        row.second_signed_flow += int(np.sum(signed[second_group]))


def accumulators_to_frame(
    accumulators: dict[int, HalfMinuteAccumulator],
    source: SourceSpec,
) -> pd.DataFrame:
    rows = []
    for minute_start, values in sorted(accumulators.items()):
        rows.append(
            {
                "calendar_year": source.calendar_year,
                "product": "ES",
                "contract": source.contract,
                "instrument_id": source.instrument_id,
                "minute_start_ns": minute_start,
                "availability_ns": minute_start + MINUTE_NS,
                **asdict(values),
                "transformation_version": TRANSFORMATION_VERSION,
            }
        )
    return pd.DataFrame(rows)


def _validate_frame(frame: pd.DataFrame) -> None:
    if frame.empty or frame.duplicated(["calendar_year", "contract", "minute_start_ns"]).any():
        raise V71IntraminuteFlowStoreError("intraminute output identity failure")
    if set(frame["contract"].unique()) != {"ESU3", "ESU4"}:
        raise V71IntraminuteFlowStoreError("intraminute explicit-contract drift")
    if not (frame["availability_ns"] == frame["minute_start_ns"] + MINUTE_NS).all():
        raise V71IntraminuteFlowStoreError("intraminute availability drift")
    counts = frame["first_trade_count"] + frame["second_trade_count"]
    volumes = frame["first_total_volume"] + frame["second_total_volume"]
    if (counts <= 0).any() or (volumes <= 0).any():
        raise V71IntraminuteFlowStoreError("intraminute empty minute")


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "HALF_MINUTE_NS",
    "MINUTE_NS",
    "HalfMinuteAccumulator",
    "SourceSpec",
    "V71IntraminuteFlowStoreError",
    "_accumulate_chunk",
    "accumulators_to_frame",
    "build_intraminute_flow_store",
]
