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


GRAMMAR_PATH = "WORM/v7.2-trade-arrival-renewal-grammar-0011-2026-07-13.json"
GRAMMAR_SHA256 = "d69f021bf4de5b4e5a0fe92d318eba9f00b08c80d99cb3941c43daab4a6b10c2"
OUTPUT_PATH = "data/cache/v7_d1/date_matched_trade_arrival_renewal_v1.parquet"
MANIFEST_PATH = "data/manifests/v7_d1_trade_arrival_renewal_v1.json"
TRANSFORMATION_VERSION = "hydra_v7_d1_trade_arrival_renewal_v1"
MINUTE_NS = 60_000_000_000
FIVE_SECONDS_NS = 5_000_000_000
PRICE_SCALE = 1_000_000_000.0
EXPECTED_MINUTE_COUNT = 17_200


class V72TradeArrivalRenewalStoreError(RuntimeError):
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
class ArrivalMinuteAccumulator:
    minute_start_ns: int | None = None
    trade_count: int = 0
    total_volume: int = 0
    signed_volume: int = 0
    first_price_raw: int | None = None
    last_price_raw: int | None = None
    last_event_ns: int | None = None
    five_second_counts: list[int] = field(default_factory=lambda: [0] * 12)
    positive_gaps_ns: list[int] = field(default_factory=list)

    def empty(self) -> bool:
        return self.minute_start_ns is None


@dataclass(slots=True)
class ArrivalStreamAccumulator:
    source: SourceSpec
    current: ArrivalMinuteAccumulator = field(default_factory=ArrivalMinuteAccumulator)
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
        if value.first_price_raw is None or value.last_price_raw is None:
            raise V72TradeArrivalRenewalStoreError("arrival minute lacks a price")
        counts = np.asarray(value.five_second_counts, dtype=np.int64)
        if int(counts.sum()) != value.trade_count:
            raise V72TradeArrivalRenewalStoreError("arrival bin counts do not reconcile")
        shares = counts[counts > 0].astype(float) / float(value.trade_count)
        entropy = float(-np.sum(shares * np.log(shares)) / math.log(12.0))
        positive_gap_median = (
            float(np.median(np.asarray(value.positive_gaps_ns, dtype=np.int64)))
            if len(value.positive_gaps_ns) >= 2
            else math.nan
        )
        row: dict[str, Any] = {
            "calendar_year": self.source.calendar_year,
            "product": "ES",
            "contract": self.source.contract,
            "instrument_id": self.source.instrument_id,
            "minute_start_ns": int(value.minute_start_ns),
            "source_close_ns": int(value.minute_start_ns) + MINUTE_NS,
            "availability_ns": int(value.minute_start_ns) + MINUTE_NS,
            "trade_count": value.trade_count,
            "total_volume": value.total_volume,
            "signed_volume": value.signed_volume,
            "signed_flow_fraction": value.signed_volume / value.total_volume,
            "positive_gap_count": len(value.positive_gaps_ns),
            "positive_gap_median_ns": positive_gap_median,
            "arrival_entropy": entropy,
            "maximum_five_second_share": float(counts.max() / value.trade_count),
            "first_price": value.first_price_raw / PRICE_SCALE,
            "last_price": value.last_price_raw / PRICE_SCALE,
            "price_progress_points": (
                value.last_price_raw - value.first_price_raw
            )
            / PRICE_SCALE,
            "transformation_version": TRANSFORMATION_VERSION,
        }
        row.update(
            {
                f"trade_count_5s_{index:02d}": int(count)
                for index, count in enumerate(counts)
            }
        )
        self.rows.append(row)
        self.current = ArrivalMinuteAccumulator()


def build_trade_arrival_renewal_store(
    project_root: str | Path = ".",
    *,
    chunk_size: int = 1_000_000,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if _sha256(root / GRAMMAR_PATH) != GRAMMAR_SHA256:
        raise V72TradeArrivalRenewalStoreError("trade-arrival grammar WORM drift")
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for source in SOURCES:
        source_path = root / source.path
        if _sha256(source_path) != source.sha256:
            raise V72TradeArrivalRenewalStoreError(
                f"trade-arrival raw source drift for {source.calendar_year}"
            )
        frame, audit = aggregate_dbn_source(
            source_path, source, chunk_size=chunk_size
        )
        frames.append(frame)
        audits.append(audit)
    output = pd.concat(frames, ignore_index=True).sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    _validate_frame(output)

    destination = root / OUTPUT_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    output.to_parquet(temporary, index=False, compression="zstd")
    os.replace(temporary, destination)
    output_hash = _sha256(destination)
    manifest = {
        "schema": "hydra_v7_d1_trade_arrival_renewal_manifest_v1",
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
            "minute_partition": "twelve fixed five-second exchange-event bins",
            "positive_gap": "strictly positive exchange-event timestamp difference inside a completed minute",
            "positive_gap_median_minimum_count": 2,
            "arrival_entropy_normalization": "log(12)",
            "aggressor_sign": {"B": 1, "A": -1, "N": 0},
            "price_scale": PRICE_SCALE,
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
            "eligible_positive_gap_minutes": int(
                output["positive_gap_median_ns"].notna().sum()
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
            "Arrival clustering can encode only the intraday activity curve or generic "
            "volatility; the permanent price-world tripwire and walk-forward transfer "
            "remain mandatory."
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
        raise V72TradeArrivalRenewalStoreError("unexpected trade-arrival DBN metadata")
    stream = ArrivalStreamAccumulator(source=source)
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
        excluded_session += int(
            np.count_nonzero(eligible_before_session & ~rth)
        )
        keep = eligible_before_session & rth
        if not np.any(keep):
            continue
        retained = chunk[keep]
        retained_count += len(retained)
        stream.ingest(retained)
    frame = stream.finish()
    if retained_count <= 0 or frame.empty:
        raise V72TradeArrivalRenewalStoreError(
            "trade-arrival source retained no observations"
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
        "positive_gap_eligible_minute_count": int(
            frame["positive_gap_median_ns"].notna().sum()
        ),
    }


def _accumulate_records(
    stream: ArrivalStreamAccumulator,
    records: np.ndarray,
) -> None:
    if len(records) == 0:
        return
    required = {"ts_event", "price", "size", "side"}
    if not required.issubset(records.dtype.names or ()):
        raise V72TradeArrivalRenewalStoreError("trade-arrival record fields missing")
    timestamps = np.asarray(records["ts_event"], dtype=np.int64)
    if int(timestamps[0]) < stream.last_event_ns or np.any(np.diff(timestamps) < 0):
        raise V72TradeArrivalRenewalStoreError(
            "explicit-contract exchange-event ordering failed"
        )
    sides = np.asarray(records["side"])
    if not np.all(np.isin(sides, [b"A", b"B", b"N"])):
        raise V72TradeArrivalRenewalStoreError("unknown aggressor-side code")
    for record in records:
        ts = int(record["ts_event"])
        minute_start = (ts // MINUTE_NS) * MINUTE_NS
        if stream.current.minute_start_ns != minute_start:
            stream._flush_current()
            stream.current.minute_start_ns = minute_start
        current = stream.current
        price = int(record["price"])
        size = int(record["size"])
        raw_side = bytes(record["side"])
        if size <= 0:
            raise V72TradeArrivalRenewalStoreError("nonpositive trade size")
        if current.first_price_raw is None:
            current.first_price_raw = price
        if current.last_event_ns is not None:
            gap = ts - current.last_event_ns
            if gap < 0:
                raise V72TradeArrivalRenewalStoreError("negative interarrival gap")
            if gap > 0:
                current.positive_gaps_ns.append(gap)
        current.last_event_ns = ts
        current.last_price_raw = price
        current.trade_count += 1
        current.total_volume += size
        if raw_side == b"B":
            current.signed_volume += size
        elif raw_side == b"A":
            current.signed_volume -= size
        bin_index = min(11, int((ts - minute_start) // FIVE_SECONDS_NS))
        current.five_second_counts[bin_index] += 1
    stream.last_event_ns = int(timestamps[-1])


def _validate_frame(frame: pd.DataFrame) -> None:
    if frame.empty or frame.duplicated(
        ["calendar_year", "contract", "minute_start_ns"]
    ).any():
        raise V72TradeArrivalRenewalStoreError("trade-arrival output identity failure")
    if len(frame) != EXPECTED_MINUTE_COUNT:
        raise V72TradeArrivalRenewalStoreError(
            f"trade-arrival minute count drift: {len(frame)}"
        )
    if set(frame["contract"].unique()) != {"ESU3", "ESU4"}:
        raise V72TradeArrivalRenewalStoreError("trade-arrival explicit-contract drift")
    if not (
        frame["availability_ns"] == frame["minute_start_ns"] + MINUTE_NS
    ).all():
        raise V72TradeArrivalRenewalStoreError("trade-arrival availability drift")
    bin_columns = [f"trade_count_5s_{index:02d}" for index in range(12)]
    if not (frame[bin_columns].sum(axis=1) == frame["trade_count"]).all():
        raise V72TradeArrivalRenewalStoreError("trade-arrival bin reconciliation failed")
    if (frame["trade_count"] <= 0).any() or (frame["total_volume"] <= 0).any():
        raise V72TradeArrivalRenewalStoreError("trade-arrival empty minute")
    if not frame["signed_flow_fraction"].between(-1.0, 1.0).all():
        raise V72TradeArrivalRenewalStoreError("trade-arrival signed flow is invalid")
    if not frame["arrival_entropy"].between(0.0, 1.0).all():
        raise V72TradeArrivalRenewalStoreError("trade-arrival entropy is invalid")
    if not frame["maximum_five_second_share"].between(1.0 / 12.0, 1.0).all():
        raise V72TradeArrivalRenewalStoreError("trade-arrival concentration is invalid")


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ArrivalMinuteAccumulator",
    "ArrivalStreamAccumulator",
    "SourceSpec",
    "V72TradeArrivalRenewalStoreError",
    "_accumulate_records",
    "build_trade_arrival_renewal_store",
]
