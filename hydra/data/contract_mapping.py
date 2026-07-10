from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.markets.instruments import instrument_spec
from hydra.utils.config import project_path


CONTRACT_MONTH_CODES = {3: "H", 6: "M", 9: "U", 12: "Z"}


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


@dataclass(frozen=True)
class RollMap:
    dataset: str
    schema: str
    map_type: str
    symbols: list[str]
    contracts: list[ContractInfo]
    unsafe_window_days: int
    notes: list[str]

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
    path.write_text(json.dumps(roll_map.to_dict(), indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path, digest


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
