from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd


RAW_PATH = (
    "data/cache/databento_v7_d1/"
    "GLBX-MDP3_trades_ES_MES_2024-08-01_2024-10-01.dbn.zst"
)
RAW_SHA256 = "a312d6df524205c2d1632c20f1033ecb74081c98d6608eacc1a9cdb08bcf9b63"
DEFINITION_PATH = (
    "data/cache/contract_maps/"
    "definitions_GLBX-MDP3_2024-01-01_2024-10-01_dbf3b43b85c57f1e.dbn.zst"
)
DEFINITION_SHA256 = (
    "72d7a5276cdfed7c6e710bd2b7eacdd3131aa1603190589d84224005a5bbc7c9"
)
ACQUISITION_PLAN_SHA256 = (
    "37022f048337f173db9e01997087b998051a4512aa444e46f0f5d31cc45b29f2"
)
OUTPUT_PATH = "data/cache/v7_d1/rth_minute_print_features_v1.parquet"
MANIFEST_PATH = "data/manifests/v7_d1_trades_features_v1.json"
EXPECTED_INSTRUMENT_IDS = frozenset({118, 183_748, 7_114, 42_034_014})
EXPECTED_RAW_SYMBOLS = frozenset({"ESU4", "ESZ4", "MESU4", "MESZ4"})
MINUTE_NS = 60_000_000_000
PRICE_SCALE = 1_000_000_000.0
TRANSFORMATION_VERSION = "hydra_v7_d1_rth_minute_print_features_v1"


class TradeFeatureStoreError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ContractDefinition:
    instrument_id: int
    product: str
    raw_symbol: str
    min_price_increment: float
    point_value: float


@dataclass(slots=True)
class MinuteAccumulator:
    instrument_id: int
    minute_start_ns: int
    first_ts_ns: int
    last_ts_ns: int
    first_price: float
    last_price: float
    high_price: float
    low_price: float
    total_volume: int
    buy_volume: int
    sell_volume: int
    unknown_side_volume: int
    trade_count: int
    price_size_sum: float
    path_length_points: float

    def merge_later(self, other: "MinuteAccumulator") -> None:
        if (
            self.instrument_id != other.instrument_id
            or self.minute_start_ns != other.minute_start_ns
        ):
            raise TradeFeatureStoreError("cannot merge different minute groups")
        if other.first_ts_ns < self.last_ts_ns:
            raise TradeFeatureStoreError("trade chunks are not chronological")
        self.path_length_points += abs(other.first_price - self.last_price)
        self.path_length_points += other.path_length_points
        self.last_ts_ns = other.last_ts_ns
        self.last_price = other.last_price
        self.high_price = max(self.high_price, other.high_price)
        self.low_price = min(self.low_price, other.low_price)
        self.total_volume += other.total_volume
        self.buy_volume += other.buy_volume
        self.sell_volume += other.sell_volume
        self.unknown_side_volume += other.unknown_side_volume
        self.trade_count += other.trade_count
        self.price_size_sum += other.price_size_sum


def build_feature_store(
    project_root: str | Path = ".",
    *,
    chunk_size: int = 1_000_000,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    raw = root / RAW_PATH
    definitions = root / DEFINITION_PATH
    if _sha256(raw) != RAW_SHA256:
        raise TradeFeatureStoreError("D1 raw trades hash mismatch")
    if _sha256(definitions) != DEFINITION_SHA256:
        raise TradeFeatureStoreError("definition cache hash mismatch")
    contract_map = load_contract_definitions(definitions)

    import databento as db

    store = db.DBNStore.from_file(raw)
    metadata = store.metadata
    if str(metadata.dataset) != "GLBX.MDP3" or str(metadata.schema) != "trades":
        raise TradeFeatureStoreError("unexpected D1 DBN metadata")
    accumulators, audit = aggregate_trade_chunks(
        store.to_ndarray(count=chunk_size), contract_map
    )
    frame = accumulators_to_frame(accumulators, contract_map)
    output = root / OUTPUT_PATH
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output, index=False, compression="zstd")
    output_hash = _sha256(output)
    manifest = {
        "schema": "hydra_v7_d1_trade_feature_manifest_v1",
        "transformation_version": TRANSFORMATION_VERSION,
        "data_role": "DEVELOPMENT_ONLY",
        "source": {
            "path": RAW_PATH,
            "sha256": RAW_SHA256,
            "schema": "trades",
            "record_count": audit["source_record_count"],
            "start_inclusive": "2024-08-01T00:00:00Z",
            "end_exclusive": "2024-10-01T00:00:00Z"
        },
        "definitions": {
            "path": DEFINITION_PATH,
            "sha256": DEFINITION_SHA256,
            "explicit_contracts": sorted(
                definition.raw_symbol for definition in contract_map.values()
            )
        },
        "acquisition_plan_sha256": ACQUISITION_PLAN_SHA256,
        "session_filter": {
            "timezone": "America/Chicago",
            "start_inclusive": "08:30:00",
            "end_exclusive": "15:10:00",
            "weekdays_only": True
        },
        "aggressor_side_encoding": {
            "B": "BUY_AGGRESSOR",
            "A": "SELL_AGGRESSOR",
            "N": "UNKNOWN"
        },
        "feature_availability": "minute_start_ns + 60 seconds",
        "feature_columns": list(frame.columns),
        "output": {
            "path": OUTPUT_PATH,
            "sha256": output_hash,
            "row_count": len(frame),
            "first_minute_start_ns": int(frame["minute_start_ns"].min()),
            "last_minute_start_ns": int(frame["minute_start_ns"].max()),
            "products": sorted(frame["product"].unique().tolist()),
            "contracts": sorted(frame["contract"].unique().tolist())
        },
        "audit": audit,
        "outcome_or_pnl_columns": [],
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Minute aggregation retains aggressor imbalance but discards within-minute "
            "queue and exact sweep ordering; event bars must be built separately before "
            "testing hypotheses that depend on sub-minute burst geometry."
        )
    }
    manifest_path = root / MANIFEST_PATH
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def load_contract_definitions(path: str | Path) -> dict[int, ContractDefinition]:
    import databento as db

    store = db.DBNStore.from_file(path)
    selected: dict[int, ContractDefinition] = {}
    for chunk in store.to_ndarray(count=10_000):
        mask = np.isin(chunk["instrument_id"], list(EXPECTED_INSTRUMENT_IDS))
        for row in chunk[mask]:
            instrument_id = int(row["instrument_id"])
            raw_symbol = bytes(row["raw_symbol"]).rstrip(b"\x00").decode("ascii")
            product = "MES" if raw_symbol.startswith("MES") else "ES"
            definition = ContractDefinition(
                instrument_id=instrument_id,
                product=product,
                raw_symbol=raw_symbol,
                min_price_increment=float(row["min_price_increment"])
                / PRICE_SCALE,
                point_value=float(row["unit_of_measure_qty"]) / PRICE_SCALE,
            )
            prior = selected.get(instrument_id)
            if prior is not None and prior != definition:
                raise TradeFeatureStoreError(
                    f"definition drift for instrument {instrument_id}"
                )
            selected[instrument_id] = definition
    if set(selected) != set(EXPECTED_INSTRUMENT_IDS):
        raise TradeFeatureStoreError("explicit contract definitions are incomplete")
    if {row.raw_symbol for row in selected.values()} != set(EXPECTED_RAW_SYMBOLS):
        raise TradeFeatureStoreError("explicit contract symbols changed")
    expected_values = {"ES": 50.0, "MES": 5.0}
    for definition in selected.values():
        if not math.isclose(
            definition.point_value, expected_values[definition.product]
        ) or not math.isclose(definition.min_price_increment, 0.25):
            raise TradeFeatureStoreError("contract multiplier or tick changed")
    return selected


def aggregate_trade_chunks(
    chunks: Iterable[np.ndarray],
    contract_map: Mapping[int, ContractDefinition],
) -> tuple[dict[tuple[int, int], MinuteAccumulator], dict[str, Any]]:
    accumulators: dict[tuple[int, int], MinuteAccumulator] = {}
    source_count = 0
    retained_count = 0
    excluded_session_count = 0
    excluded_instrument_count = 0
    invalid_action_count = 0
    last_global_receive_ts = -1
    retained_chunks: list[np.ndarray] = []
    for chunk in chunks:
        if len(chunk) == 0:
            continue
        source_count += len(chunk)
        ts = np.asarray(chunk["ts_event"], dtype=np.int64)
        receive_field = "ts_recv" if "ts_recv" in chunk.dtype.names else "ts_event"
        receive_ts = np.asarray(chunk[receive_field], dtype=np.int64)
        if int(receive_ts[0]) < last_global_receive_ts or np.any(
            np.diff(receive_ts) < 0
        ):
            raise TradeFeatureStoreError(
                "source trade stream is not receive-time chronological"
            )
        last_global_receive_ts = int(receive_ts[-1])
        actions = np.asarray(chunk["action"])
        valid_action = actions == b"T"
        invalid_action_count += int(np.count_nonzero(~valid_action))
        sides = np.asarray(chunk["side"])
        if not np.all(np.isin(sides, [b"A", b"B", b"N"])):
            raise TradeFeatureStoreError("unknown Databento aggressor-side code")
        instruments = np.asarray(chunk["instrument_id"], dtype=np.int64)
        valid_instrument = np.isin(instruments, list(contract_map))
        excluded_instrument_count += int(np.count_nonzero(~valid_instrument))
        timestamps = pd.to_datetime(ts, unit="ns", utc=True).tz_convert(
            "America/Chicago"
        )
        minutes = np.asarray(timestamps.hour * 60 + timestamps.minute)
        weekdays = np.asarray(timestamps.weekday)
        in_session = (
            (weekdays < 5)
            & (minutes >= 8 * 60 + 30)
            & (minutes < 15 * 60 + 10)
        )
        eligible_before_session = valid_action & valid_instrument
        excluded_session_count += int(
            np.count_nonzero(eligible_before_session & ~in_session)
        )
        keep = eligible_before_session & in_session
        if not np.any(keep):
            continue
        retained_count += int(np.count_nonzero(keep))
        retained_chunks.append(np.array(chunk[keep], copy=True))
    if source_count <= 0 or retained_count <= 0:
        raise TradeFeatureStoreError("trade aggregation retained no observations")
    # GLBX records are delivered in receive-time order. Exchange event timestamps
    # may move backward by a small amount, including across DBN chunk boundaries.
    # Concatenating the already-filtered RTH subset before one event-time sort keeps
    # open/close/path features exact without assuming receive order equals event order.
    retained = np.concatenate(retained_chunks)
    _merge_chunk_groups(accumulators, retained)
    del retained, retained_chunks
    audit = {
        "source_record_count": source_count,
        "retained_rth_record_count": retained_count,
        "excluded_session_record_count": excluded_session_count,
        "excluded_instrument_record_count": excluded_instrument_count,
        "invalid_action_record_count": invalid_action_count,
        "minute_group_count": len(accumulators),
        "unknown_side_volume": sum(
            row.unknown_side_volume for row in accumulators.values()
        ),
        "buy_aggressor_volume": sum(row.buy_volume for row in accumulators.values()),
        "sell_aggressor_volume": sum(
            row.sell_volume for row in accumulators.values()
        )
    }
    if audit["retained_rth_record_count"] != sum(
        row.trade_count for row in accumulators.values()
    ):
        raise TradeFeatureStoreError("retained trade-count reconciliation failed")
    if sum(row.total_volume for row in accumulators.values()) != (
        audit["buy_aggressor_volume"]
        + audit["sell_aggressor_volume"]
        + audit["unknown_side_volume"]
    ):
        raise TradeFeatureStoreError("aggressor volume reconciliation failed")
    return accumulators, audit


def _merge_chunk_groups(
    destination: dict[tuple[int, int], MinuteAccumulator], chunk: np.ndarray
) -> None:
    ts = np.asarray(chunk["ts_event"], dtype=np.int64)
    instruments = np.asarray(chunk["instrument_id"], dtype=np.int64)
    minute_starts = (ts // MINUTE_NS) * MINUTE_NS
    order = np.lexsort((ts, instruments, minute_starts))
    sorted_chunk = chunk[order]
    sorted_ts = ts[order]
    sorted_instruments = instruments[order]
    sorted_minutes = minute_starts[order]
    changes = np.flatnonzero(
        (np.diff(sorted_instruments) != 0) | (np.diff(sorted_minutes) != 0)
    ) + 1
    starts = np.concatenate(([0], changes))
    ends = np.concatenate((changes, [len(sorted_chunk)]))
    for start, end in zip(starts, ends, strict=True):
        group = sorted_chunk[start:end]
        prices = np.asarray(group["price"], dtype=np.float64) / PRICE_SCALE
        sizes = np.asarray(group["size"], dtype=np.int64)
        sides = np.asarray(group["side"])
        accumulator = MinuteAccumulator(
            instrument_id=int(sorted_instruments[start]),
            minute_start_ns=int(sorted_minutes[start]),
            first_ts_ns=int(sorted_ts[start]),
            last_ts_ns=int(sorted_ts[end - 1]),
            first_price=float(prices[0]),
            last_price=float(prices[-1]),
            high_price=float(np.max(prices)),
            low_price=float(np.min(prices)),
            total_volume=int(np.sum(sizes)),
            buy_volume=int(np.sum(sizes[sides == b"B"])),
            sell_volume=int(np.sum(sizes[sides == b"A"])),
            unknown_side_volume=int(np.sum(sizes[sides == b"N"])),
            trade_count=len(group),
            price_size_sum=float(np.sum(prices * sizes)),
            path_length_points=float(np.sum(np.abs(np.diff(prices)))),
        )
        key = (accumulator.instrument_id, accumulator.minute_start_ns)
        prior = destination.get(key)
        if prior is None:
            destination[key] = accumulator
        else:
            prior.merge_later(accumulator)


def accumulators_to_frame(
    accumulators: Mapping[tuple[int, int], MinuteAccumulator],
    contract_map: Mapping[int, ContractDefinition],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key in sorted(accumulators, key=lambda value: (value[1], value[0])):
        accumulator = accumulators[key]
        definition = contract_map[accumulator.instrument_id]
        signed_volume = accumulator.buy_volume - accumulator.sell_volume
        price_change = accumulator.last_price - accumulator.first_price
        efficiency = (
            price_change / accumulator.path_length_points
            if accumulator.path_length_points > 0.0
            else 0.0
        )
        rows.append(
            {
                "product": definition.product,
                "contract": definition.raw_symbol,
                "instrument_id": accumulator.instrument_id,
                "minute_start_ns": accumulator.minute_start_ns,
                "source_close_ns": accumulator.minute_start_ns + MINUTE_NS,
                "availability_ns": accumulator.minute_start_ns + MINUTE_NS,
                "first_trade_ns": accumulator.first_ts_ns,
                "last_trade_ns": accumulator.last_ts_ns,
                "open": accumulator.first_price,
                "high": accumulator.high_price,
                "low": accumulator.low_price,
                "close": accumulator.last_price,
                "vwap": accumulator.price_size_sum / accumulator.total_volume,
                "trade_count": accumulator.trade_count,
                "total_volume": accumulator.total_volume,
                "buy_aggressor_volume": accumulator.buy_volume,
                "sell_aggressor_volume": accumulator.sell_volume,
                "unknown_side_volume": accumulator.unknown_side_volume,
                "signed_aggressor_volume": signed_volume,
                "signed_aggressor_fraction": signed_volume
                / accumulator.total_volume,
                "price_change_points": price_change,
                "path_length_points": accumulator.path_length_points,
                "signed_path_efficiency": efficiency,
                "transformation_version": TRANSFORMATION_VERSION,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise TradeFeatureStoreError("minute feature frame is empty")
    if not np.all(frame["availability_ns"] > frame["last_trade_ns"]):
        raise TradeFeatureStoreError("minute feature availability is not closed-bar")
    if not np.all(frame["source_close_ns"] == frame["availability_ns"]):
        raise TradeFeatureStoreError("feature close and availability drifted")
    return frame


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ContractDefinition",
    "MinuteAccumulator",
    "TradeFeatureStoreError",
    "accumulators_to_frame",
    "aggregate_trade_chunks",
    "build_feature_store",
    "load_contract_definitions",
]
