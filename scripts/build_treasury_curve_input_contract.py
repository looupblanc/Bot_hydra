#!/usr/bin/env python3
"""Build the immutable input contract for the Treasury curve tripwire.

The builder is deliberately isolated from the persistent runtime.  It performs
no network request, purchase, manifest mutation, registry write, Q4 access, or
broker/order action.  It binds already-downloaded continuous OHLCV to explicit
raw contracts through either:

* the continuous -> instrument_id mappings embedded in the OHLCV DBN plus a
  raw Definition DBN, or
* a previously sealed two-stage symbology receipt containing both
  continuous -> instrument_id and instrument_id -> raw_symbol mappings.

Every source bar must have exactly one explicit mapping.  Pair alignment is
audited only where both legs have the same delivery month; mismatched roll
windows are excluded and never forward-filled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash
from hydra.research.curve_relative_value_tripwire import (
    INPUT_SCHEMA,
    OFFICIAL_COST_RECEIPT,
    PAIR_SPECS,
    TREASURY_SPECS,
    _validate_input_contract,
)


SCHEMA = "hydra_treasury_curve_input_builder_v1"
ROLL_RECEIPT_SCHEMA = "hydra_treasury_curve_roll_receipt_v1"
MAPPING_RECEIPT_SCHEMA = "hydra_two_stage_symbology_mapping_receipt_v1"
ROLL_POLICY = "EXPLICIT_CONTRACT_DELIVERY_SYNC_NO_FORWARD_FILL"
ROOTS = tuple(sorted(TREASURY_SPECS))
DATASET = "GLBX.MDP3"
DATA_SCHEMA = "ohlcv-1m"
PROTECTED_Q4_START = pd.Timestamp("2024-10-01", tz="UTC")
MONTH_CODES = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
REQUIRED_OUTPUT_COLUMNS = (
    "timestamp",
    "symbol",
    "contract",
    "delivery_month",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "session_id",
    "instrument_id",
    "roll_segment_id",
)


class TreasuryCurveInputError(RuntimeError):
    """The immutable Treasury input cannot be built without ambiguity."""


@dataclass(frozen=True, slots=True)
class MappingInterval:
    root: str
    instrument_id: str
    start: pd.Timestamp
    end: pd.Timestamp

    def contains(self, timestamp: pd.Timestamp) -> bool:
        return bool(self.start <= timestamp < self.end)

    def to_dict(self) -> dict[str, str]:
        return {
            "root": self.root,
            "instrument_id": self.instrument_id,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
        }


def build_treasury_curve_input_contract(
    *,
    root: str | Path,
    raw_ohlcv_path: str | Path,
    output_dir: str | Path,
    mapping_receipt_path: str | Path | None = None,
    raw_definition_path: str | Path | None = None,
) -> dict[str, Any]:
    """Decode existing DBN inputs and build a tripwire-compatible contract."""

    project = Path(root).resolve()
    raw_path = _inside(project, raw_ohlcv_path, require_file=True)
    out = _inside(project, output_dir, require_file=False)
    if mapping_receipt_path is None and raw_definition_path is None:
        raise TreasuryCurveInputError(
            "either a sealed two-stage mapping receipt or raw Definition DBN is required"
        )
    if mapping_receipt_path is not None and raw_definition_path is not None:
        raise TreasuryCurveInputError(
            "mapping receipt and raw Definition DBN are mutually exclusive inputs"
        )

    db = _import_databento()
    ohlcv_store = db.DBNStore.from_file(raw_path)
    ohlcv = _store_frame(ohlcv_store, price_type="float")
    metadata = _metadata_mapping_payload(ohlcv_store)
    source_receipts: dict[str, Any] = {
        "raw_ohlcv": _source_file_receipt(project, raw_path),
    }

    if mapping_receipt_path is not None:
        receipt_path = _inside(project, mapping_receipt_path, require_file=True)
        mapping = _load_mapping_receipt(receipt_path)
        source_receipts["mapping_receipt"] = _source_file_receipt(project, receipt_path)
    else:
        definition_path = _inside(project, raw_definition_path or "", require_file=True)
        definition_store = db.DBNStore.from_file(definition_path)
        definitions = _store_frame(definition_store, price_type=None)
        mapping = _mapping_from_metadata_and_definitions(metadata, definitions)
        source_receipts["raw_definition"] = _source_file_receipt(
            project, definition_path
        )

    return build_from_frames(
        root=project,
        ohlcv_frame=ohlcv,
        mapping_receipt=mapping,
        output_dir=out,
        source_receipts=source_receipts,
    )


def build_from_frames(
    *,
    root: str | Path,
    ohlcv_frame: pd.DataFrame,
    mapping_receipt: Mapping[str, Any],
    output_dir: str | Path,
    source_receipts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Pure-frame builder used by the DBN wrapper and deterministic tests."""

    project = Path(root).resolve()
    out = _inside(project, output_dir, require_file=False)
    out.mkdir(parents=True, exist_ok=True)
    mapping = _validate_mapping_receipt(mapping_receipt)
    intervals = _parse_continuous_intervals(mapping["continuous_mapping"])
    raw_symbols = _parse_raw_symbol_mapping(mapping["raw_symbol_mapping"])
    sealed_intervals = _seal_mapping_intervals(intervals, raw_symbols)
    normalized = _normalize_ohlcv(ohlcv_frame, intervals, raw_symbols)
    _validate_mapping_bounds(normalized, mapping)

    root_files: list[dict[str, Any]] = []
    root_rolls: dict[str, Any] = {}
    for root_symbol in ROOTS:
        frame = normalized.loc[normalized["symbol"] == root_symbol].copy()
        if frame.empty:
            raise TreasuryCurveInputError(f"no normalized bars for required root {root_symbol}")
        frame, segments = _assign_roll_segments(frame, root_symbol)
        path = out / f"{root_symbol.lower()}_explicit_contract_ohlcv.parquet"
        _persist_parquet_once(path, frame.loc[:, REQUIRED_OUTPUT_COLUMNS])
        root_files.append(
            {
                "path": str(path.relative_to(project)),
                "sha256": _sha256(path),
                "dataset": str(mapping["dataset"]),
                "schema": DATA_SCHEMA,
                "roots": [root_symbol],
                "record_count": int(len(frame)),
            }
        )
        root_rolls[root_symbol] = {
            "record_count": int(len(frame)),
            "contract_count": int(frame["contract"].nunique()),
            "delivery_month_count": int(frame["delivery_month"].nunique()),
            "sealed_mapping_intervals": sealed_intervals[root_symbol],
            "segments": segments,
        }

    pair_audits = _pair_delivery_audits(normalized)
    sources = _canonical_sources(source_receipts or {})
    roll_core: dict[str, Any] = {
        "schema": ROLL_RECEIPT_SCHEMA,
        "policy": ROLL_POLICY,
        "no_forward_fill": True,
        "same_delivery_alignment_only": True,
        "source_mapping_hash": str(mapping["mapping_hash"]),
        "source_receipts": sources,
        "root_rolls": root_rolls,
        "pair_delivery_audits": pair_audits,
        "coverage": {
            "roots": list(ROOTS),
            "record_count": int(len(normalized)),
            "first_timestamp": normalized["timestamp"].min().isoformat(),
            "last_timestamp": normalized["timestamp"].max().isoformat(),
            "unmapped_rows": 0,
            "ambiguous_rows": 0,
        },
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    roll_receipt = {**roll_core, "receipt_hash": stable_hash(roll_core)}
    roll_path = out / "roll_receipt.json"
    _persist_json_once(roll_path, roll_receipt)

    input_core: dict[str, Any] = {
        "schema": INPUT_SCHEMA,
        "q4_excluded": True,
        "files": root_files,
        "roll_receipt": {
            "path": str(roll_path.relative_to(project)),
            "sha256": _sha256(roll_path),
            "policy": ROLL_POLICY,
        },
        "cost_receipt": dict(OFFICIAL_COST_RECEIPT),
    }
    input_contract = {**input_core, "builder_contract_hash": stable_hash(input_core)}
    # Validate against the consumer before the contract itself is sealed.
    validated = _validate_input_contract(project, input_contract)
    input_contract["tripwire_input_contract_hash"] = validated["input_contract_hash"]
    contract_path = out / "input_contract.json"
    _persist_json_once(contract_path, input_contract)

    result_core = {
        "schema": SCHEMA,
        "status": "TREASURY_CURVE_INPUT_CONTRACT_BUILT",
        "input_contract_path": str(contract_path.relative_to(project)),
        "input_contract_file_sha256": _sha256(contract_path),
        "tripwire_input_contract_hash": validated["input_contract_hash"],
        "roll_receipt_path": str(roll_path.relative_to(project)),
        "roll_receipt_sha256": _sha256(roll_path),
        "normalized_record_count": int(len(normalized)),
        "root_record_counts": {
            item["roots"][0]: item["record_count"] for item in root_files
        },
        "pair_delivery_audits": pair_audits,
        "input_contract": input_contract,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**result_core, "result_hash": stable_hash(result_core)}


def _normalize_ohlcv(
    frame: pd.DataFrame,
    intervals: Sequence[MappingInterval],
    raw_symbols: Mapping[str, Sequence[tuple[pd.Timestamp, pd.Timestamp, str]]],
) -> pd.DataFrame:
    source = frame.copy().reset_index(drop=False)
    timestamp_column = _first_column(source, ("timestamp", "ts_event"))
    instrument_column = _first_column(source, ("instrument_id", "symbol"))
    required_prices = ("open", "high", "low", "close", "volume")
    missing = [name for name in required_prices if name not in source.columns]
    if missing:
        raise TreasuryCurveInputError(f"raw OHLCV missing required columns: {missing}")
    source["timestamp"] = pd.to_datetime(source[timestamp_column], utc=True, errors="coerce")
    if source["timestamp"].isna().any():
        raise TreasuryCurveInputError("raw OHLCV contains invalid timestamps")
    source["instrument_id"] = source[instrument_column].astype(str)
    if source.duplicated(["instrument_id", "timestamp"]).any():
        raise TreasuryCurveInputError("raw OHLCV contains duplicate instrument timestamps")
    for column in required_prices:
        source[column] = pd.to_numeric(source[column], errors="coerce")
    if source[list(required_prices)].isna().any().any():
        raise TreasuryCurveInputError("raw OHLCV contains non-numeric values")
    if (source[["open", "high", "low", "close"]] <= 0).any().any():
        raise TreasuryCurveInputError("raw OHLCV contains non-positive prices")
    if (source["volume"] < 0).any():
        raise TreasuryCurveInputError("raw OHLCV contains negative volume")

    intervals_by_id: dict[str, list[MappingInterval]] = {}
    for interval in intervals:
        intervals_by_id.setdefault(interval.instrument_id, []).append(interval)
    mapped: list[dict[str, Any]] = []
    for row in source.itertuples(index=False):
        instrument_id = str(getattr(row, "instrument_id"))
        timestamp = pd.Timestamp(getattr(row, "timestamp"))
        matches = [
            interval
            for interval in intervals_by_id.get(instrument_id, ())
            if interval.contains(timestamp)
        ]
        if len(matches) != 1:
            reason = "uncovered" if not matches else "ambiguous"
            raise TreasuryCurveInputError(
                f"{reason} continuous mapping for instrument={instrument_id} "
                f"timestamp={timestamp.isoformat()}"
            )
        interval = matches[0]
        contract = _resolve_raw_symbol(raw_symbols, instrument_id, timestamp)
        delivery = _delivery_month(contract, interval.root, timestamp)
        mapped.append(
            {
                "timestamp": timestamp,
                "symbol": interval.root,
                "contract": contract,
                "delivery_month": delivery,
                "open": float(getattr(row, "open")),
                "high": float(getattr(row, "high")),
                "low": float(getattr(row, "low")),
                "close": float(getattr(row, "close")),
                "volume": int(getattr(row, "volume")),
                "session_id": _session_id(timestamp),
                "instrument_id": instrument_id,
            }
        )
    out = pd.DataFrame.from_records(mapped)
    if out.empty:
        raise TreasuryCurveInputError("raw OHLCV contains no records")
    if set(out["symbol"]) != set(ROOTS):
        raise TreasuryCurveInputError(
            f"normalized root inventory drift: {sorted(set(out['symbol']))}"
        )
    if out.duplicated(["symbol", "timestamp"]).any():
        raise TreasuryCurveInputError("continuous input maps multiple bars to one root timestamp")
    return out.sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _parse_continuous_intervals(value: Mapping[str, Any]) -> tuple[MappingInterval, ...]:
    by_root: dict[str, list[MappingInterval]] = {root: [] for root in ROOTS}
    for key, raw_rows in dict(value).items():
        root = str(key).split(".", 1)[0].upper()
        if root not in by_root:
            continue
        rows = list(raw_rows or [])
        if not rows:
            raise TreasuryCurveInputError(f"empty continuous mapping for {key}")
        for row in rows:
            data = dict(row)
            start = _utc(data.get("d0", data.get("start_date", data.get("start"))))
            end = _utc(data.get("d1", data.get("end_date", data.get("end"))))
            instrument_id = str(data.get("s", data.get("symbol", "")))
            if not instrument_id or start >= end:
                raise TreasuryCurveInputError(f"invalid continuous interval for {key}: {data}")
            by_root[root].append(MappingInterval(root, instrument_id, start, end))
    if any(not rows for rows in by_root.values()):
        missing = sorted(root for root, rows in by_root.items() if not rows)
        raise TreasuryCurveInputError(f"continuous mapping misses required roots: {missing}")
    output: list[MappingInterval] = []
    for root, rows in sorted(by_root.items()):
        rows.sort(key=lambda item: (item.start, item.end, item.instrument_id))
        for left, right in zip(rows, rows[1:]):
            if right.start < left.end:
                raise TreasuryCurveInputError(
                    f"overlapping continuous intervals for {root}: "
                    f"{left.to_dict()} vs {right.to_dict()}"
                )
        output.extend(rows)
    return tuple(output)


def _parse_raw_symbol_mapping(
    value: Mapping[str, Any],
) -> dict[str, tuple[tuple[pd.Timestamp, pd.Timestamp, str], ...]]:
    minimum = pd.Timestamp("1900-01-01", tz="UTC")
    maximum = pd.Timestamp("2200-01-01", tz="UTC")
    output: dict[str, tuple[tuple[pd.Timestamp, pd.Timestamp, str], ...]] = {}
    for instrument_id, raw in dict(value).items():
        rows: list[tuple[pd.Timestamp, pd.Timestamp, str]] = []
        if isinstance(raw, str):
            rows.append((minimum, maximum, raw.strip()))
        elif isinstance(raw, Mapping):
            data = dict(raw)
            symbol = str(data.get("s", data.get("symbol", data.get("raw_symbol", "")))).strip()
            rows.append(
                (
                    _utc(data.get("d0", data.get("start_date", data.get("start", minimum)))),
                    _utc(data.get("d1", data.get("end_date", data.get("end", maximum)))),
                    symbol,
                )
            )
        else:
            for raw_row in list(raw or []):
                data = dict(raw_row)
                symbol = str(data.get("s", data.get("symbol", data.get("raw_symbol", "")))).strip()
                rows.append(
                    (
                        _utc(data.get("d0", data.get("start_date", data.get("start", minimum)))),
                        _utc(data.get("d1", data.get("end_date", data.get("end", maximum)))),
                        symbol,
                    )
                )
        if not rows or any(not symbol or start >= end for start, end, symbol in rows):
            raise TreasuryCurveInputError(
                f"invalid raw-symbol mapping for instrument {instrument_id}"
            )
        rows.sort(key=lambda item: (item[0], item[1], item[2]))
        for left, right in zip(rows, rows[1:]):
            if right[0] < left[1]:
                raise TreasuryCurveInputError(
                    f"overlapping raw-symbol intervals for instrument {instrument_id}"
                )
        output[str(instrument_id)] = tuple(rows)
    return output


def _resolve_raw_symbol(
    mapping: Mapping[str, Sequence[tuple[pd.Timestamp, pd.Timestamp, str]]],
    instrument_id: str,
    timestamp: pd.Timestamp,
) -> str:
    matches = [
        symbol
        for start, end, symbol in mapping.get(str(instrument_id), ())
        if start <= timestamp < end
    ]
    if len(matches) != 1:
        reason = "missing" if not matches else "ambiguous"
        raise TreasuryCurveInputError(
            f"{reason} raw symbol for instrument={instrument_id} "
            f"timestamp={timestamp.isoformat()}"
        )
    return matches[0]


def _seal_mapping_intervals(
    intervals: Sequence[MappingInterval],
    raw_symbols: Mapping[str, Sequence[tuple[pd.Timestamp, pd.Timestamp, str]]],
) -> dict[str, list[dict[str, Any]]]:
    """Bind every continuous interval to one raw contract without filling gaps."""

    output: dict[str, list[dict[str, Any]]] = {root: [] for root in ROOTS}
    for interval in intervals:
        covers = [
            (start, end, symbol)
            for start, end, symbol in raw_symbols.get(interval.instrument_id, ())
            if start <= interval.start and end >= interval.end
        ]
        if len(covers) != 1:
            reason = "uncovered" if not covers else "ambiguous"
            raise TreasuryCurveInputError(
                f"{reason} raw-symbol coverage for continuous interval "
                f"{interval.to_dict()}"
            )
        contract = covers[0][2]
        reference = interval.start + (interval.end - interval.start) / 2
        core = {
            **interval.to_dict(),
            "contract": contract,
            "delivery_month": _delivery_month(contract, interval.root, reference),
            "forward_filled": False,
        }
        output[interval.root].append(
            {**core, "mapping_interval_hash": stable_hash(core)}
        )
    return output


def _delivery_month(contract: str, expected_root: str, timestamp: pd.Timestamp) -> str:
    match = re.fullmatch(r"([A-Z]{2})([FGHJKMNQUVXZ])(\d{1,4})", contract.upper())
    if match is None or match.group(1) != expected_root:
        raise TreasuryCurveInputError(
            f"raw contract {contract!r} is not an explicit {expected_root} future"
        )
    digits = match.group(3)
    if len(digits) == 4:
        year = int(digits)
    elif len(digits) == 2:
        year = 2000 + int(digits)
    elif len(digits) == 1:
        reference = int(timestamp.year)
        candidates = [
            year
            for year in range(reference - 5, reference + 6)
            if year % 10 == int(digits)
        ]
        distances = [abs(year - reference) for year in candidates]
        if not candidates or distances.count(min(distances)) != 1:
            raise TreasuryCurveInputError(
                f"ambiguous one-digit delivery year in {contract!r} at {timestamp}"
            )
        year = candidates[distances.index(min(distances))]
    else:
        raise TreasuryCurveInputError(f"unsupported delivery year in {contract!r}")
    return f"{year:04d}{MONTH_CODES[match.group(2)]:02d}"


def _assign_roll_segments(
    frame: pd.DataFrame, root: str
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    output = frame.sort_values("timestamp").reset_index(drop=True).copy()
    transition = (
        output[["contract", "delivery_month"]]
        .ne(output[["contract", "delivery_month"]].shift(1))
        .any(axis=1)
    )
    output["roll_segment_id"] = transition.cumsum().map(
        lambda number: f"{root}:segment:{int(number):04d}"
    )
    segments = []
    for segment_id, rows in output.groupby("roll_segment_id", sort=True):
        if rows["contract"].nunique() != 1 or rows["delivery_month"].nunique() != 1:
            raise TreasuryCurveInputError(f"ambiguous roll segment {segment_id}")
        segments.append(
            {
                "segment_id": str(segment_id),
                "contract": str(rows["contract"].iloc[0]),
                "delivery_month": str(rows["delivery_month"].iloc[0]),
                "first_timestamp": rows["timestamp"].min().isoformat(),
                "last_timestamp": rows["timestamp"].max().isoformat(),
                "record_count": int(len(rows)),
                "input_hash": _frame_hash(
                    rows.loc[:, ["timestamp", "instrument_id", "contract", "delivery_month"]]
                ),
            }
        )
    return output, segments


def _pair_delivery_audits(normalized: pd.DataFrame) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for pair in PAIR_SPECS:
        left = normalized.loc[
            normalized["symbol"] == pair.shorter_root,
            ["timestamp", "contract", "delivery_month", "session_id"],
        ].rename(
            columns={
                "contract": "left_contract",
                "delivery_month": "left_delivery_month",
                "session_id": "left_session_id",
            }
        )
        right = normalized.loc[
            normalized["symbol"] == pair.longer_root,
            ["timestamp", "contract", "delivery_month", "session_id"],
        ].rename(
            columns={
                "contract": "right_contract",
                "delivery_month": "right_delivery_month",
                "session_id": "right_session_id",
            }
        )
        merged = left.merge(right, on="timestamp", how="outer", indicator=True, validate="one_to_one")
        both = merged.loc[merged["_merge"] == "both"].copy()
        if both.empty:
            raise TreasuryCurveInputError(f"no timestamp overlap for {pair.pair_id}")
        if not both["left_session_id"].astype(str).eq(both["right_session_id"].astype(str)).all():
            raise TreasuryCurveInputError(f"session identity drift for {pair.pair_id}")
        matched = both.loc[
            both["left_delivery_month"].astype(str)
            == both["right_delivery_month"].astype(str)
        ].copy()
        mismatched = both.loc[
            both["left_delivery_month"].astype(str)
            != both["right_delivery_month"].astype(str)
        ].copy()
        if matched.empty:
            raise TreasuryCurveInputError(
                f"pair {pair.pair_id} has no same-delivery aligned observations"
            )
        aligned_hash = _frame_hash(
            matched.loc[
                :, [
                    "timestamp",
                    "left_contract",
                    "right_contract",
                    "left_delivery_month",
                ]
            ]
        )
        output[pair.pair_id] = {
            "shorter_root": pair.shorter_root,
            "longer_root": pair.longer_root,
            "left_records": int(len(left)),
            "right_records": int(len(right)),
            "timestamp_intersection": int(len(both)),
            "aligned_same_delivery_rows": int(len(matched)),
            "delivery_mismatch_rows_excluded": int(len(mismatched)),
            "left_only_rows_excluded": int((merged["_merge"] == "left_only").sum()),
            "right_only_rows_excluded": int((merged["_merge"] == "right_only").sum()),
            "same_delivery_alignment_hash": aligned_hash,
            "mismatch_fingerprint": _frame_hash(
                mismatched.loc[
                    :, [
                        "timestamp",
                        "left_contract",
                        "right_contract",
                        "left_delivery_month",
                        "right_delivery_month",
                    ]
                ]
            ),
            "forward_fill_rows": 0,
        }
    return output


def _validate_mapping_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    supplied = dict(value)
    claimed = str(supplied.pop("mapping_hash", ""))
    if not claimed or stable_hash(supplied) != claimed:
        raise TreasuryCurveInputError("two-stage mapping receipt hash mismatch")
    if str(supplied.get("dataset")) != DATASET:
        raise TreasuryCurveInputError("mapping receipt dataset drift")
    roots = {str(root).upper() for root in supplied.get("roots") or []}
    if roots != set(ROOTS):
        raise TreasuryCurveInputError("mapping receipt root inventory drift")
    if not supplied.get("continuous_mapping"):
        raise TreasuryCurveInputError("mapping receipt is not a complete two-stage mapping")
    normalized = dict(supplied)
    if not normalized.get("raw_symbol_mapping"):
        intervals = list(normalized.get("contract_intervals") or [])
        if not intervals:
            raise TreasuryCurveInputError(
                "mapping receipt lacks raw_symbol_mapping and contract_intervals"
            )
        raw_by_id: dict[str, list[dict[str, str]]] = {}
        for raw_row in intervals:
            row = dict(raw_row)
            instrument_id = str(row.get("instrument_id", ""))
            raw_symbol = str(row.get("raw_symbol", "")).strip()
            if not instrument_id or not raw_symbol:
                raise TreasuryCurveInputError("invalid sealed contract interval")
            raw_by_id.setdefault(instrument_id, []).append(
                {
                    "d0": str(row.get("d0", "")),
                    "d1": str(row.get("d1", "")),
                    "s": raw_symbol,
                }
            )
        normalized["raw_symbol_mapping"] = raw_by_id
        normalized["raw_symbol_mapping_derivation"] = (
            "SEALED_CONTRACT_INTERVALS_FROM_TWO_STAGE_RECEIPT"
        )
    start = _utc(supplied.get("start"))
    end = _utc(supplied.get("end"))
    if start >= end or end > PROTECTED_Q4_START:
        raise TreasuryCurveInputError("mapping receipt opens protected Q4 or has invalid dates")
    return {**normalized, "mapping_hash": claimed}


def _load_mapping_receipt(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TreasuryCurveInputError(f"cannot decode mapping receipt {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise TreasuryCurveInputError("mapping receipt must be a JSON object")
    return dict(value)


def _metadata_mapping_payload(store: Any) -> dict[str, Any]:
    metadata = getattr(store, "metadata", None)
    mappings = dict(getattr(metadata, "mappings", {}) or {})
    continuous = {
        str(symbol): [
            {
                "d0": _json_time(row.get("start_date")),
                "d1": _json_time(row.get("end_date")),
                "s": str(row.get("symbol", "")),
            }
            for row in rows
        ]
        for symbol, rows in mappings.items()
    }
    start_ns = getattr(metadata, "start", None)
    end_ns = getattr(metadata, "end", None)
    start = pd.Timestamp(int(start_ns), unit="ns", tz="UTC") if start_ns is not None else None
    end = pd.Timestamp(int(end_ns), unit="ns", tz="UTC") if end_ns is not None else None
    return {
        "dataset": str(getattr(metadata, "dataset", "")),
        "schema": str(getattr(metadata, "schema", "")),
        "start": start.isoformat() if start is not None else None,
        "end": end.isoformat() if end is not None else None,
        "continuous_mapping": continuous,
    }


def _mapping_from_metadata_and_definitions(
    metadata: Mapping[str, Any], definitions: pd.DataFrame
) -> dict[str, Any]:
    if str(metadata.get("dataset")) != DATASET or str(metadata.get("schema")) != DATA_SCHEMA:
        raise TreasuryCurveInputError("raw OHLCV metadata dataset/schema drift")
    continuous = dict(metadata.get("continuous_mapping") or {})
    required_ids = {
        str(row.get("s", row.get("symbol", "")))
        for rows in continuous.values()
        for row in rows
    }
    frame = definitions.reset_index(drop=False)
    instrument_column = _first_column(frame, ("instrument_id", "symbol"))
    if "raw_symbol" not in frame.columns:
        raise TreasuryCurveInputError("Definition DBN has no raw_symbol field")
    raw: dict[str, str] = {}
    for instrument_id in sorted(required_ids):
        rows = frame.loc[frame[instrument_column].astype(str) == instrument_id]
        symbols = sorted(
            {
                str(value).strip()
                for value in rows["raw_symbol"].dropna().tolist()
                if str(value).strip()
            }
        )
        if len(symbols) != 1:
            raise TreasuryCurveInputError(
                f"Definition DBN raw symbol is {'missing' if not symbols else 'ambiguous'} "
                f"for instrument {instrument_id}: {symbols}"
            )
        raw[instrument_id] = symbols[0]
    core = {
        "schema": MAPPING_RECEIPT_SCHEMA,
        "dataset": DATASET,
        "start": str(metadata.get("start")),
        "end": str(metadata.get("end")),
        "roots": list(ROOTS),
        "continuous_mapping": continuous,
        "instrument_ids": sorted(required_ids, key=_numeric_sort_key),
        "raw_symbol_mapping": raw,
        "mapping_source": "OHLCV_DBN_METADATA_PLUS_RAW_DEFINITION_DBN",
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _validate_mapping_bounds(normalized: pd.DataFrame, mapping: Mapping[str, Any]) -> None:
    start = _utc(mapping["start"])
    end = _utc(mapping["end"])
    timestamps = pd.to_datetime(normalized["timestamp"], utc=True)
    if timestamps.min() < start or timestamps.max() >= end:
        raise TreasuryCurveInputError("normalized bars escape the sealed half-open interval")
    if (timestamps >= PROTECTED_Q4_START).any():
        raise TreasuryCurveInputError("normalized bars include protected Q4")


def _session_id(timestamp: pd.Timestamp) -> str:
    local = timestamp.tz_convert("America/Chicago")
    trade_date = local.normalize()
    if int(local.hour) >= 17:
        trade_date += pd.Timedelta(days=1)
    return trade_date.date().isoformat()


def _store_frame(store: Any, *, price_type: str | None) -> pd.DataFrame:
    kwargs: dict[str, Any] = {"pretty_ts": True, "map_symbols": False}
    if price_type is not None:
        kwargs["price_type"] = price_type
    frame = store.to_df(**kwargs)
    if not isinstance(frame, pd.DataFrame):
        raise TreasuryCurveInputError("DBN decoder did not return a DataFrame")
    return frame.reset_index()


def _persist_parquet_once(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    frame.to_parquet(
        temporary,
        index=False,
        engine="pyarrow",
        compression="zstd",
        row_group_size=65_536,
    )
    if path.exists():
        if _sha256(path) != _sha256(temporary):
            temporary.unlink(missing_ok=True)
            raise TreasuryCurveInputError(f"immutable Parquet already differs: {path}")
        temporary.unlink()
    else:
        os.replace(temporary, path)


def _persist_json_once(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (_canonical_json(value) + "\n").encode("utf-8")
    if path.exists():
        if path.read_bytes() != payload:
            raise TreasuryCurveInputError(f"immutable JSON already differs: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _canonical_sources(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): dict(item) for key, item in sorted(value.items())}


def _source_file_receipt(project: Path, path: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(project)),
        "sha256": _sha256(path),
        "bytes": int(path.stat().st_size),
    }


def _inside(project: Path, value: str | Path, *, require_file: bool) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (project / path).resolve()
    try:
        resolved.relative_to(project)
    except ValueError as exc:
        raise TreasuryCurveInputError(f"path escapes project: {value}") from exc
    if require_file and not resolved.is_file():
        raise TreasuryCurveInputError(f"required input file is missing: {resolved}")
    return resolved


def _first_column(frame: pd.DataFrame, choices: Iterable[str]) -> str:
    for column in choices:
        if column in frame.columns:
            return column
    raise TreasuryCurveInputError(
        f"required column absent; expected one of {tuple(choices)}, got {list(frame.columns)}"
    )


def _utc(value: Any) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value)
    except Exception as exc:
        raise TreasuryCurveInputError(f"invalid mapping timestamp {value!r}") from exc
    if result.tzinfo is None:
        result = result.tz_localize("UTC")
    else:
        result = result.tz_convert("UTC")
    return result


def _json_time(value: Any) -> str:
    return _utc(value).isoformat()


def _numeric_sort_key(value: str) -> tuple[int, str]:
    try:
        return (0, f"{int(value):020d}")
    except ValueError:
        return (1, value)


def _frame_hash(frame: pd.DataFrame) -> str:
    if frame.empty:
        return hashlib.sha256(b"[]").hexdigest()
    records = []
    for row in frame.itertuples(index=False, name=None):
        records.append(
            [item.isoformat() if isinstance(item, pd.Timestamp) else item for item in row]
        )
    return hashlib.sha256(_canonical_json(records).encode("utf-8")).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the SHA-bound Treasury curve tripwire input contract"
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--raw-ohlcv", required=True)
    parser.add_argument("--output-dir", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--mapping-receipt")
    source.add_argument("--raw-definition")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = build_treasury_curve_input_contract(
        root=args.root,
        raw_ohlcv_path=args.raw_ohlcv,
        output_dir=args.output_dir,
        mapping_receipt_path=args.mapping_receipt,
        raw_definition_path=args.raw_definition,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
