from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


GRAMMAR_PATH = "WORM/v7.1-aggressor-run-topology-grammar-0009-2026-07-13.json"
GRAMMAR_SHA256 = "05ff83f0fbf902381371d3d840ce7393adadfa8e51d6c75e51a76c12a275bce2"
OUTPUT_PATH = "data/cache/v7_d1/date_matched_aggressor_run_topology_v1.parquet"
MANIFEST_PATH = "data/manifests/v7_d1_aggressor_run_topology_v1.json"
TRANSFORMATION_VERSION = "hydra_v7_d1_aggressor_run_topology_v1"
MINUTE_NS = 60_000_000_000


class V71AggressorRunTopologyStoreError(RuntimeError):
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
class RunTopologyAccumulator:
    trade_count: int = 0
    neutral_trade_count: int = 0
    side_change_count: int = 0
    longest_buy_run: int = 0
    longest_sell_run: int = 0
    tail_side: int = 0
    tail_run: int = 0
    first_price: int | None = None
    last_price: int | None = None


def build_aggressor_run_topology_store(
    project_root: str | Path = ".",
    *,
    chunk_size: int = 1_000_000,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if _sha256(root / GRAMMAR_PATH) != GRAMMAR_SHA256:
        raise V71AggressorRunTopologyStoreError("aggressor-run grammar WORM drift")
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for source in SOURCES:
        source_path = root / source.path
        if _sha256(source_path) != source.sha256:
            raise V71AggressorRunTopologyStoreError(
                f"aggressor-run raw source drift for {source.calendar_year}"
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
        "schema": "hydra_v7_d1_aggressor_run_topology_manifest_v1",
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
            "record_order": "exchange event timestamp after explicit-contract filter",
            "aggressor_sign": {"B": 1, "A": -1, "N": 0},
            "neutral_records": "ignored for runs, retained in neutral count",
            "run_boundary": "side change or completed-minute boundary",
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
        "outcome_or_future_pnl_columns": [],
        "new_data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Run topology is deterministic and uses only completed prints, but minute "
            "price progress is contemporaneous geometry; the permanent null tripwire "
            "must distinguish it from edge."
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
) -> tuple[dict[int, RunTopologyAccumulator], dict[str, Any]]:
    import databento as db

    store = db.DBNStore.from_file(path)
    if str(store.metadata.dataset) != "GLBX.MDP3" or str(store.metadata.schema) != "trades":
        raise V71AggressorRunTopologyStoreError("unexpected aggressor-run DBN metadata")
    accumulators: dict[int, RunTopologyAccumulator] = {}
    raw_count = bounded_count = retained_count = 0
    invalid_action = excluded_instrument = excluded_session = 0
    last_event = -1
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
        records = chunk[keep]
        retained_ts = np.asarray(records["ts_event"], dtype=np.int64)
        if int(retained_ts[0]) < last_event or np.any(np.diff(retained_ts) < 0):
            raise V71AggressorRunTopologyStoreError(
                "explicit-contract exchange-event ordering failed"
            )
        last_event = int(retained_ts[-1])
        retained_count += len(records)
        _accumulate_chunk(accumulators, records)
    if retained_count <= 0 or not accumulators:
        raise V71AggressorRunTopologyStoreError("aggressor-run source retained no observations")
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
    destination: dict[int, RunTopologyAccumulator],
    records: np.ndarray,
) -> None:
    ts = np.asarray(records["ts_event"], dtype=np.int64)
    if np.any(np.diff(ts) < 0):
        raise V71AggressorRunTopologyStoreError("run chunk event ordering failed")
    minute = (ts // MINUTE_NS) * MINUTE_NS
    boundaries = np.flatnonzero(np.r_[True, minute[1:] != minute[:-1], True])
    for left, right in zip(boundaries[:-1], boundaries[1:]):
        _accumulate_minute_group(
            destination.setdefault(int(minute[left]), RunTopologyAccumulator()),
            records[left:right],
        )


def _accumulate_minute_group(
    destination: RunTopologyAccumulator,
    records: np.ndarray,
) -> None:
    prices = np.asarray(records["price"], dtype=np.int64)
    if destination.first_price is None:
        destination.first_price = int(prices[0])
    destination.last_price = int(prices[-1])
    raw_sides = np.asarray(records["side"])
    if not np.all(np.isin(raw_sides, [b"A", b"B", b"N"])):
        raise V71AggressorRunTopologyStoreError("unknown aggressor-side code")
    destination.neutral_trade_count += int(np.count_nonzero(raw_sides == b"N"))
    sides = np.where(raw_sides == b"B", 1, np.where(raw_sides == b"A", -1, 0))
    sides = sides[sides != 0].astype(np.int8, copy=False)
    if len(sides) == 0:
        return
    destination.trade_count += len(sides)
    starts = np.flatnonzero(np.r_[True, sides[1:] != sides[:-1]])
    ends = np.r_[starts[1:], len(sides)]
    run_sides = sides[starts]
    run_lengths = ends - starts
    if destination.tail_side and destination.tail_side == int(run_sides[0]):
        run_lengths[0] += destination.tail_run
    elif destination.tail_side:
        destination.side_change_count += 1
    destination.side_change_count += max(0, len(run_lengths) - 1)
    buy = run_lengths[run_sides == 1]
    sell = run_lengths[run_sides == -1]
    if len(buy):
        destination.longest_buy_run = max(destination.longest_buy_run, int(buy.max()))
    if len(sell):
        destination.longest_sell_run = max(destination.longest_sell_run, int(sell.max()))
    destination.tail_side = int(run_sides[-1])
    destination.tail_run = int(run_lengths[-1])


def accumulators_to_frame(
    accumulators: dict[int, RunTopologyAccumulator],
    source: SourceSpec,
) -> pd.DataFrame:
    rows = []
    for minute_start, values in sorted(accumulators.items()):
        if values.first_price is None or values.last_price is None:
            continue
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
        raise V71AggressorRunTopologyStoreError("aggressor-run output identity failure")
    if set(frame["contract"].unique()) != {"ESU3", "ESU4"}:
        raise V71AggressorRunTopologyStoreError("aggressor-run explicit-contract drift")
    if not (frame["availability_ns"] == frame["minute_start_ns"] + MINUTE_NS).all():
        raise V71AggressorRunTopologyStoreError("aggressor-run availability drift")
    if (frame["trade_count"] <= 0).any():
        raise V71AggressorRunTopologyStoreError("aggressor-run empty minute")
    if (frame[["longest_buy_run", "longest_sell_run"]].max(axis=1) <= 0).any():
        raise V71AggressorRunTopologyStoreError("aggressor-run topology missing")


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "RunTopologyAccumulator",
    "V71AggressorRunTopologyStoreError",
    "_accumulate_chunk",
    "build_aggressor_run_topology_store",
]
