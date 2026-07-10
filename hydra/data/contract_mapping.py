from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.markets.instruments import instrument_spec
from hydra.utils.config import project_path


CONTRACT_MONTH_CODES = {3: "H", 6: "M", 9: "U", 12: "Z"}
OUTRIGHT_FUTURES_MONTH_CODES = "FGHJKMNQUVXZ"


@dataclass(frozen=True)
class ContractInfo:
    root: str
    contract: str
    month_code: str
    year: int
    expiry_date: str
    last_trade_date: str
    active_start: str
    active_end: str
    roll_date: str
    tick_size: float
    tick_value: float
    point_value: float
    contract_multiplier: float
    is_micro: bool
    instrument_id: str | None = None
    parent_symbol: str | None = None
    continuous_symbol: str | None = None
    activation_time: str | None = None
    deactivation_time: str | None = None
    roll_reason: str | None = None
    transition_uncertainty: str | None = None
    price_discontinuity: float | None = None
    volume_migration_ratio: float | None = None


@dataclass(frozen=True)
class RollMap:
    dataset: str
    schema: str
    map_type: str
    symbols: list[str]
    contracts: list[ContractInfo]
    unsafe_window_days: int
    notes: list[str]
    source_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["roll_map_hash"] = self.roll_map_hash()
        return payload

    def roll_map_hash(self) -> str:
        raw = json.dumps(asdict(self), sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_rule_based_roll_map(
    symbols: list[str],
    *,
    start: str,
    end: str,
    dataset: str = "GLBX.MDP3",
    schema: str = "ohlcv-1m",
    unsafe_window_days: int = 3,
) -> RollMap:
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")
    years = sorted({start_ts.year, end_ts.year})
    contracts: list[ContractInfo] = []
    for symbol in symbols:
        spec = instrument_spec(symbol)
        for year in range(min(years), max(years) + 1):
            quarter_months = [3, 6, 9, 12]
            expiries = {month: third_friday(year, month) for month in quarter_months}
            for index, month in enumerate(quarter_months):
                expiry = expiries[month]
                prev_month = quarter_months[index - 1] if index else 12
                prev_year = year if index else year - 1
                active_start = expiries.get(prev_month) if index else third_friday(prev_year, prev_month)
                active_end = expiry
                if pd.Timestamp(active_end, tz="UTC") < start_ts - pd.Timedelta(days=unsafe_window_days + 10):
                    continue
                if pd.Timestamp(active_start, tz="UTC") >= end_ts + pd.Timedelta(days=unsafe_window_days + 10):
                    continue
                code = CONTRACT_MONTH_CODES[month]
                contract = f"{symbol}{code}{str(year)[-1]}"
                contracts.append(
                    ContractInfo(
                        root=symbol,
                        contract=contract,
                        month_code=code,
                        year=year,
                        expiry_date=expiry.isoformat(),
                        last_trade_date=expiry.isoformat(),
                        active_start=active_start.isoformat(),
                        active_end=active_end.isoformat(),
                        roll_date=expiry.isoformat(),
                        tick_size=spec.tick_size,
                        tick_value=spec.tick_value,
                        point_value=spec.point_value,
                        contract_multiplier=spec.point_value,
                        is_micro=spec.is_micro,
                    )
                )
    return RollMap(
        dataset=dataset,
        schema=schema,
        map_type="RULE_BASED_CME_EQUITY_INDEX_QUARTERLY_PROXY",
        symbols=list(symbols),
        contracts=sorted(contracts, key=lambda item: (item.root, item.active_start, item.contract)),
        unsafe_window_days=unsafe_window_days,
        notes=[
            "Rule-based quarterly CME equity-index mapping used because explicit raw contract metadata was not present in cache.",
            "Use as roll-aware proxy only; explicit Databento definitions should replace this before promotion.",
        ],
    )


def build_explicit_roll_map(
    symbols: list[str],
    *,
    start: str,
    end: str,
    continuous_mapping: dict[str, list[dict[str, Any]]],
    raw_symbol_mapping: dict[str, str],
    definition_records: dict[str, dict[str, Any]],
    definition_history: pd.DataFrame | None = None,
    dataset: str = "GLBX.MDP3",
    schema: str = "ohlcv-1m",
    unsafe_window_days: int = 3,
) -> RollMap:
    contracts: list[ContractInfo] = []
    for root in symbols:
        spec = instrument_spec(root)
        continuous_symbol = f"{root}.c.0"
        for segment in continuous_mapping.get(continuous_symbol, []):
            instrument_id = str(segment["s"])
            if definition_history is not None:
                definition = resolve_date_aware_definition(
                    definition_history,
                    instrument_id=instrument_id,
                    active_start=str(segment["d0"]),
                    root=root,
                )
                raw_symbol = str(definition["raw_symbol"])
            else:
                raw_symbol = raw_symbol_mapping.get(instrument_id, instrument_id)
                definition = definition_records.get(instrument_id, {})
                if not valid_outright_future_symbol(root, raw_symbol):
                    raise ValueError(
                        f"Raw symbol {raw_symbol!r} is not a valid outright futures contract for {root}."
                    )
            month_code = _contract_month_code(root, raw_symbol)
            year = _contract_year(raw_symbol, segment.get("d0", start))
            expiry = _definition_timestamp(definition, "expiration") or third_friday(year, _month_from_code(month_code)).isoformat()
            activation = _definition_timestamp(definition, "activation")
            active_start = str(segment["d0"])
            active_end = str(segment["d1"])
            tick_size = _definition_float(definition, "min_price_increment", spec.tick_size)
            tick_value = spec.tick_value
            point_value = spec.point_value
            roll_date = active_start if pd.Timestamp(active_start) > pd.Timestamp(start) else active_end
            contracts.append(
                ContractInfo(
                    root=root,
                    contract=raw_symbol,
                    month_code=month_code,
                    year=year,
                    expiry_date=str(_as_utc_timestamp(expiry).date()),
                    last_trade_date=str(_as_utc_timestamp(expiry).date()),
                    active_start=active_start,
                    active_end=active_end,
                    roll_date=roll_date,
                    tick_size=tick_size,
                    tick_value=tick_value,
                    point_value=point_value,
                    contract_multiplier=point_value,
                    is_micro=spec.is_micro,
                    instrument_id=instrument_id,
                    parent_symbol=root,
                    continuous_symbol=continuous_symbol,
                    activation_time=activation,
                    deactivation_time=active_end,
                    roll_reason="databento_continuous_front_contract_transition",
                    transition_uncertainty="date_level_symbology_interval",
                )
            )
    return RollMap(
        dataset=dataset,
        schema=schema,
        map_type=(
            "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
            if definition_history is not None
            else "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS"
        ),
        symbols=list(symbols),
        contracts=sorted(contracts, key=lambda item: (item.root, item.active_start, item.contract)),
        unsafe_window_days=unsafe_window_days,
        notes=[
            "Continuous-symbol intervals came from Databento symbology continuous->instrument_id mapping.",
            "Raw symbols came from Databento symbology instrument_id->raw_symbol mapping.",
            "Instrument definitions came from Databento definition schema; OHLCV signals must exclude unsafe roll-transition windows.",
        ],
        source_metadata={
            "period_start": start,
            "period_end": end,
            "continuous_symbols": [f"{symbol}.c.0" for symbol in symbols],
            "definition_record_count": len(definition_records),
            "definition_resolution": (
                "date_aware_end_of_segment_start_day"
                if definition_history is not None
                else "legacy_pre_resolved_raw_symbol_mapping"
            ),
        },
    )


def valid_outright_future_symbol(root: str, symbol: str) -> bool:
    import re

    return bool(
        re.fullmatch(
            rf"{re.escape(str(root))}[{OUTRIGHT_FUTURES_MONTH_CODES}]\d{{1,2}}",
            str(symbol),
        )
    )


def resolve_date_aware_definition(
    definition_history: pd.DataFrame,
    *,
    instrument_id: str,
    active_start: str,
    root: str,
) -> dict[str, Any]:
    required = {
        "ts_event",
        "instrument_id",
        "raw_symbol",
        "instrument_class",
        "security_type",
        "asset",
        "min_price_increment",
    }
    missing = sorted(required - set(definition_history.columns))
    if missing:
        raise ValueError(f"Definition history lacks required columns: {missing}")
    frame = definition_history.copy()
    frame["instrument_id_key"] = frame["instrument_id"].astype(str)
    frame["ts_event"] = pd.to_datetime(frame["ts_event"], utc=True)
    rows = frame[frame["instrument_id_key"] == str(instrument_id)].sort_values("ts_event")
    if rows.empty:
        raise ValueError(f"No definition history for instrument ID {instrument_id}.")
    start = _as_utc_timestamp(active_start)
    cutoff = start.normalize() + pd.Timedelta(days=1)
    available = rows[rows["ts_event"] < cutoff]
    selected_time = (available.iloc[-1] if not available.empty else rows.iloc[0])["ts_event"]
    selected_rows = rows[rows["ts_event"] == selected_time]
    signature_columns = [
        "raw_symbol",
        "instrument_class",
        "security_type",
        "asset",
        "min_price_increment",
        *(["expiration"] if "expiration" in rows.columns else []),
        *(["activation"] if "activation" in rows.columns else []),
    ]
    signatures = selected_rows[signature_columns].astype(str).drop_duplicates()
    if len(signatures) != 1:
        raise ValueError(
            f"Ambiguous definition history for instrument ID {instrument_id} at {selected_time}."
        )
    selected = selected_rows.iloc[-1].to_dict()
    raw_symbol = str(selected.get("raw_symbol") or "")
    if not valid_outright_future_symbol(root, raw_symbol):
        raise ValueError(
            f"Date-aware definition {raw_symbol!r} is not an outright futures symbol for {root}."
        )
    if str(selected.get("instrument_class") or "") != "F":
        raise ValueError(f"Definition {raw_symbol} is not instrument_class=F.")
    if str(selected.get("security_type") or "") != "FUT":
        raise ValueError(f"Definition {raw_symbol} is not security_type=FUT.")
    if str(selected.get("asset") or "") != str(root):
        raise ValueError(f"Definition asset {selected.get('asset')!r} does not equal root {root!r}.")
    spec = instrument_spec(root)
    tick_size = float(selected.get("min_price_increment"))
    if abs(tick_size - float(spec.tick_size)) > 1e-12:
        raise ValueError(
            f"Definition tick {tick_size} for {raw_symbol} differs from root spec {spec.tick_size}."
        )
    selected["ts_event"] = pd.Timestamp(selected_time).isoformat()
    return selected


def repair_roll_map_from_date_aware_definitions(
    frozen: RollMap,
    definition_history: pd.DataFrame,
) -> tuple[RollMap, dict[str, Any]]:
    repaired_contracts: list[ContractInfo] = []
    changes: list[dict[str, Any]] = []
    for contract in frozen.contracts:
        if not contract.instrument_id:
            raise ValueError(f"Contract {contract.contract} has no explicit instrument ID.")
        definition = resolve_date_aware_definition(
            definition_history,
            instrument_id=str(contract.instrument_id),
            active_start=contract.active_start,
            root=contract.root,
        )
        raw_symbol = str(definition["raw_symbol"])
        month_code = _contract_month_code(contract.root, raw_symbol)
        year = _contract_year(raw_symbol, contract.active_start)
        expiration = _definition_timestamp(definition, "expiration")
        if expiration is None:
            raise ValueError(f"Date-aware definition {raw_symbol} has no expiration.")
        expiration_date = str(_as_utc_timestamp(expiration).date())
        activation = _definition_timestamp(definition, "activation")
        tick_size = float(definition["min_price_increment"])
        repaired = replace(
            contract,
            contract=raw_symbol,
            month_code=month_code,
            year=year,
            expiry_date=expiration_date,
            last_trade_date=expiration_date,
            tick_size=tick_size,
            activation_time=activation,
        )
        repaired_contracts.append(repaired)
        if repaired != contract:
            changes.append(
                {
                    "root": contract.root,
                    "instrument_id": contract.instrument_id,
                    "active_start": contract.active_start,
                    "active_end": contract.active_end,
                    "definition_event_time": definition["ts_event"],
                    "old_contract": contract.contract,
                    "new_contract": repaired.contract,
                    "old_tick_size": contract.tick_size,
                    "new_tick_size": repaired.tick_size,
                    "symbol_changed": contract.contract != repaired.contract,
                    "tick_size_changed": abs(contract.tick_size - repaired.tick_size) > 1e-12,
                }
            )
    repaired_map = RollMap(
        dataset=frozen.dataset,
        schema=frozen.schema,
        map_type="EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2",
        symbols=list(frozen.symbols),
        contracts=sorted(repaired_contracts, key=lambda item: (item.root, item.active_start, item.contract)),
        unsafe_window_days=frozen.unsafe_window_days,
        notes=[
            *frozen.notes,
            "Raw symbols and definition fields were resolved date-aware from cached Databento definition history.",
            "The frozen predecessor map remains historical and must not be used for decision-changing evidence.",
        ],
        source_metadata={
            **frozen.source_metadata,
            "definition_resolution": "date_aware_end_of_segment_start_day",
            "predecessor_roll_map_hash": frozen.roll_map_hash(),
        },
    )
    audit = {
        "segment_count": len(frozen.contracts),
        "changed_segment_count": len(changes),
        "symbol_change_count": sum(row["symbol_changed"] for row in changes),
        "tick_size_change_count": sum(row["tick_size_changed"] for row in changes),
        "all_resolved_symbols_valid": all(
            valid_outright_future_symbol(row.root, row.contract) for row in repaired_map.contracts
        ),
        "changes": changes,
    }
    return repaired_map, audit


def active_contract(roll_map: RollMap, symbol: str, timestamp: Any) -> ContractInfo:
    ts = _as_utc_timestamp(timestamp)
    matches = [c for c in roll_map.contracts if c.root == symbol]
    for contract in matches:
        start = pd.Timestamp(contract.active_start, tz="UTC")
        end = pd.Timestamp(contract.active_end, tz="UTC")
        if start <= ts < end:
            return contract
    if not matches:
        raise KeyError(f"No contracts for {symbol}")
    return min(matches, key=lambda c: abs((_as_utc_timestamp(c.active_start) - ts).total_seconds()))


def synchronized_pair_contracts(roll_map: RollMap, symbols: tuple[str, str], timestamp: Any) -> dict[str, str]:
    return {symbol: active_contract(roll_map, symbol, timestamp).contract for symbol in symbols}


def maturity_key(contract: ContractInfo) -> tuple[str, int]:
    return contract.month_code, contract.year


def is_unsafe_roll_window(roll_map: RollMap, symbol: str, timestamp: Any) -> bool:
    ts = _as_utc_timestamp(timestamp).normalize()
    for contract in [c for c in roll_map.contracts if c.root == symbol]:
        roll = pd.Timestamp(contract.roll_date, tz="UTC").normalize()
        if abs((ts - roll).days) <= roll_map.unsafe_window_days:
            return True
    return False


def annotate_contracts(df: pd.DataFrame, roll_map: RollMap) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        out["active_contract"] = []
        out["unsafe_roll_window"] = []
        return out
    out = df.copy()
    timestamps = pd.to_datetime(out["timestamp"], utc=True)
    out["active_contract"] = [
        active_contract(roll_map, str(symbol), ts).contract for symbol, ts in zip(out["symbol"], timestamps, strict=False)
    ]
    out["unsafe_roll_window"] = [
        bool(is_unsafe_roll_window(roll_map, str(symbol), ts)) for symbol, ts in zip(out["symbol"], timestamps, strict=False)
    ]
    return out


def write_roll_map(roll_map: RollMap, folder: str = "data/cache/contract_maps") -> tuple[Path, str]:
    target_dir = project_path(folder)
    target_dir.mkdir(parents=True, exist_ok=True)
    digest = roll_map.roll_map_hash()
    path = target_dir / f"roll_map_{roll_map.dataset.replace('.', '-')}_{roll_map.schema}_{digest[:16]}.json"
    content = json.dumps(roll_map.to_dict(), indent=2, sort_keys=True, default=str) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != content:
        raise RuntimeError(f"Refusing to overwrite divergent roll-map artifact: {path}")
    if not path.exists():
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(path)
    return path, digest


def load_roll_map(path: str | Path) -> RollMap:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    payload.pop("roll_map_hash", None)
    contracts = [ContractInfo(**item) for item in payload.pop("contracts")]
    return RollMap(contracts=contracts, **payload)


def third_friday(year: int, month: int) -> date:
    current = date(year, month, 1)
    friday_count = 0
    while current.month == month:
        if current.weekday() == 4:
            friday_count += 1
            if friday_count == 3:
                return current
        current += timedelta(days=1)
    raise ValueError(f"No third Friday for {year}-{month}")


def _as_utc_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _contract_month_code(root: str, raw_symbol: str) -> str:
    suffix = raw_symbol[len(root) :]
    if not suffix:
        raise ValueError(f"Cannot infer contract month from {raw_symbol}")
    return suffix[0]


def _contract_year(raw_symbol: str, fallback_date: Any) -> int:
    suffix_digits = "".join(ch for ch in raw_symbol if ch.isdigit())
    if suffix_digits:
        digit = int(suffix_digits[-1])
        fallback_year = pd.Timestamp(fallback_date).year
        decade = fallback_year - (fallback_year % 10)
        year = decade + digit
        if year < fallback_year - 5:
            year += 10
        return year
    return pd.Timestamp(fallback_date).year


def _month_from_code(code: str) -> int:
    reverse = {value: key for key, value in CONTRACT_MONTH_CODES.items()}
    if code not in reverse:
        raise ValueError(f"Unsupported futures month code {code}")
    return reverse[code]


def _definition_timestamp(record: dict[str, Any], key: str) -> str | None:
    value = record.get(key)
    if value is None or value == "" or value == 0 or value == "0" or pd.isna(value):
        return None
    try:
        return pd.Timestamp(value, tz="UTC").isoformat()
    except Exception:
        return str(value)


def _definition_float(record: dict[str, Any], key: str, default: float) -> float:
    try:
        value = float(record.get(key, default))
        return value if value > 0 else float(default)
    except (TypeError, ValueError):
        return float(default)
