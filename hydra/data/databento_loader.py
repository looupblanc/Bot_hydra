from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from hydra.utils.config import project_path


MISSING_KEY_MESSAGE = (
    "DATABENTO_API_KEY is not set. Add it to your shell environment or to /root/hydra-bot/.env, "
    "for example: export DATABENTO_API_KEY=your_key_here. Dry-run mode does not require a key."
)

INSTALL_MESSAGE = (
    "The databento Python package is not installed. Install project requirements or run: "
    "pip install databento"
)

REQUIRED_CORE_OHLCV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume", "symbol")
REQUIRED_OHLCV_COLUMNS = ("timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume", "session_id")
SUPPORTED_CONTINUOUS_ROOTS = {
    "ES": "ES.c.0",
    "MES": "MES.c.0",
    "NQ": "NQ.c.0",
    "MNQ": "MNQ.c.0",
    "RTY": "RTY.c.0",
    "M2K": "M2K.c.0",
    "YM": "YM.c.0",
    "MYM": "MYM.c.0",
    "GC": "GC.c.0",
    "MGC": "MGC.c.0",
    "CL": "CL.c.0",
    "MCL": "MCL.c.0",
}
RAW_CONTRACT_RE = re.compile(r"^[A-Z]{1,4}[FGHJKMNQUVXZ][0-9]{1,2}$")


class DatabentoConfigError(ValueError):
    pass


class DatabentoMissingKeyError(RuntimeError):
    pass


class DatabentoDependencyError(RuntimeError):
    pass


class DatabentoCostLimitError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabentoRequest:
    dataset: str
    schema: str
    symbols: list[str]
    api_symbols: list[str]
    symbol_map: dict[str, str]
    start: str
    end: str
    timeframe: str
    stype_in: str
    stype_out: str
    cache_folder: str
    raw_output_path: str
    output_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def request_from_config(
    config: dict[str, Any],
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    schema: str | None = None,
    dataset: str | None = None,
) -> DatabentoRequest:
    data_cfg = config.get("data", {})
    db_cfg = data_cfg.get("databento", {})
    resolved_symbols = symbols or list(db_cfg.get("symbols") or config.get("markets", {}).get("symbols") or [])
    resolved_dataset = dataset or db_cfg.get("dataset")
    resolved_schema = schema or db_cfg.get("schema")
    resolved_start = start or db_cfg.get("start_date")
    resolved_end = end or db_cfg.get("end_date")
    cache_folder = db_cfg.get("cache_folder") or data_cfg.get("cache_folder") or "data/cache/databento"
    timeframe = db_cfg.get("timeframe") or _timeframe_from_schema(str(resolved_schema or ""))
    stype_in = db_cfg.get("stype_in", "raw_symbol")
    stype_out = db_cfg.get("stype_out", "instrument_id")
    symbol_resolution = resolve_symbols([str(s) for s in resolved_symbols], str(stype_in))
    request = DatabentoRequest(
        dataset=str(resolved_dataset or ""),
        schema=str(resolved_schema or ""),
        symbols=symbol_resolution["logical_symbols"],
        api_symbols=symbol_resolution["api_symbols"],
        symbol_map=symbol_resolution["symbol_map"],
        start=str(resolved_start or ""),
        end=str(resolved_end or ""),
        timeframe=str(timeframe),
        stype_in=symbol_resolution["stype_in"],
        stype_out=str(stype_out),
        cache_folder=str(cache_folder),
        raw_output_path=str(_output_path(cache_folder, str(resolved_dataset or ""), str(resolved_schema or ""), symbol_resolution["logical_symbols"], str(resolved_start or ""), str(resolved_end or ""), ".dbn.zst")),
        output_path=str(_output_path(cache_folder, str(resolved_dataset or ""), str(resolved_schema or ""), symbol_resolution["logical_symbols"], str(resolved_start or ""), str(resolved_end or ""), ".parquet")),
    )
    validate_request(request)
    return request


def validate_request(request: DatabentoRequest) -> None:
    missing = []
    if not request.dataset:
        missing.append("dataset")
    if not request.schema:
        missing.append("schema")
    if not request.symbols:
        missing.append("symbols")
    if not request.start:
        missing.append("start_date")
    if not request.end:
        missing.append("end_date")
    if missing:
        raise DatabentoConfigError(f"Missing Databento configuration fields: {', '.join(missing)}")
    if request.start >= request.end:
        raise DatabentoConfigError("Databento start date must be earlier than end date.")
    if request.schema.startswith("ohlcv") and request.timeframe != _timeframe_from_schema(request.schema):
        raise DatabentoConfigError(f"Configured timeframe '{request.timeframe}' does not match schema '{request.schema}'.")


def resolve_symbols(symbols: list[str], configured_stype_in: str = "raw_symbol") -> dict[str, Any]:
    if not symbols:
        return {"logical_symbols": [], "api_symbols": [], "symbol_map": {}, "stype_in": configured_stype_in}
    logical_symbols: list[str] = []
    api_symbols: list[str] = []
    symbol_map: dict[str, str] = {}
    stype_in = configured_stype_in
    resolved_stypes: set[str] = set()
    for raw_symbol in symbols:
        symbol = raw_symbol.strip().upper()
        if not symbol:
            continue
        if symbol in SUPPORTED_CONTINUOUS_ROOTS:
            logical_symbols.append(symbol)
            api_symbol = SUPPORTED_CONTINUOUS_ROOTS[symbol]
            api_symbols.append(api_symbol)
            symbol_map[api_symbol] = symbol
            stype_in = "continuous"
            resolved_stypes.add("continuous")
        elif symbol.endswith(".C.0"):
            root = symbol.split(".", 1)[0]
            if root not in SUPPORTED_CONTINUOUS_ROOTS:
                raise DatabentoConfigError(
                    f"Unsupported continuous Databento symbol '{raw_symbol}'. Supported roots: {', '.join(SUPPORTED_CONTINUOUS_ROOTS)}."
                )
            logical_symbols.append(root)
            api_symbol = SUPPORTED_CONTINUOUS_ROOTS[root]
            api_symbols.append(api_symbol)
            symbol_map[api_symbol] = root
            stype_in = "continuous"
            resolved_stypes.add("continuous")
        elif configured_stype_in == "raw_symbol" and RAW_CONTRACT_RE.match(symbol):
            logical_symbols.append(symbol)
            api_symbols.append(symbol)
            symbol_map[symbol] = symbol
            resolved_stypes.add("raw_symbol")
        else:
            raise DatabentoConfigError(
                f"Unsupported futures symbol '{raw_symbol}'. Use one of {', '.join(SUPPORTED_CONTINUOUS_ROOTS)} "
                "for front continuous contracts, explicit continuous symbols like ES.c.0, or raw contract symbols like ESH4."
            )
    if len(resolved_stypes) > 1:
        raise DatabentoConfigError("Do not mix continuous root symbols and raw contract symbols in one Databento request.")
    if len(set(logical_symbols)) != len(logical_symbols):
        raise DatabentoConfigError("Duplicate symbols after Databento symbol resolution.")
    return {"logical_symbols": logical_symbols, "api_symbols": api_symbols, "symbol_map": symbol_map, "stype_in": stype_in}


def load_api_key() -> str | None:
    load_dotenv(project_path(".env"))
    return os.environ.get("DATABENTO_API_KEY") or None


def validate_environment(dry_run: bool) -> str | None:
    key = load_api_key()
    if not key and not dry_run:
        raise DatabentoMissingKeyError(MISSING_KEY_MESSAGE)
    return key


def dry_run_plan(request: DatabentoRequest) -> dict[str, Any]:
    return {
        "dry_run": True,
        "network_request_made": False,
        "request": request.to_dict(),
        "warning": "Dry-run only: no Databento client was created and no market data request was made.",
    }


def test_connection(config: dict[str, Any], dry_run: bool = True) -> dict[str, Any]:
    request = request_from_config(config)
    if dry_run:
        return dry_run_plan(request)
    key = validate_environment(dry_run=False)
    db = _import_databento()
    client = db.Historical(key)
    datasets = client.metadata.list_datasets()
    return {
        "dry_run": False,
        "network_request_made": True,
        "dataset": request.dataset,
        "dataset_available": request.dataset in datasets,
        "available_dataset_count": len(datasets),
    }


def estimate_request(request: DatabentoRequest, key: str | None = None) -> dict[str, Any]:
    if key is None:
        key = validate_environment(dry_run=False)
    db = _import_databento()
    client = db.Historical(key)
    kwargs = {
        "dataset": request.dataset,
        "start": request.start,
        "end": request.end,
        "symbols": request.api_symbols,
        "schema": request.schema,
        "stype_in": request.stype_in,
    }
    return {
        "record_count": int(client.metadata.get_record_count(**kwargs)),
        "estimated_cost_usd": float(client.metadata.get_cost(**kwargs)),
        "billable_size_bytes": int(client.metadata.get_billable_size(**kwargs)),
    }


def download_historical_ohlcv(
    config: dict[str, Any],
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    schema: str | None = None,
    dataset: str | None = None,
    dry_run: bool = True,
    max_cost_usd: float = 5.0,
) -> dict[str, Any]:
    request = request_from_config(config, symbols=symbols, start=start, end=end, schema=schema, dataset=dataset)
    cached_path = Path(request.output_path)
    if cached_path.exists() and not dry_run:
        cached = load_cached_ohlcv(cached_path, timeframe=request.timeframe)
        stats = validate_ohlcv_frame(cached, timeframe=request.timeframe)
        return {
            "dry_run": False,
            "network_request_made": False,
            "cache_hit": True,
            "request": request.to_dict(),
            "output_path": request.output_path,
            "raw_output_path": request.raw_output_path,
            "validation": stats,
        }
    if dry_run:
        return dry_run_plan(request)
    key = validate_environment(dry_run=False)
    db = _import_databento()
    estimate = estimate_request(request, key)
    if estimate["estimated_cost_usd"] > max_cost_usd:
        raise DatabentoCostLimitError(
            f"Estimated Databento cost ${estimate['estimated_cost_usd']:.2f} exceeds --max-cost-usd ${max_cost_usd:.2f}. "
            "Use a smaller date range or explicitly raise the limit after reviewing the estimate."
        )
    Path(request.output_path).parent.mkdir(parents=True, exist_ok=True)
    client = db.Historical(key)
    store = client.timeseries.get_range(
        dataset=request.dataset,
        start=request.start,
        end=request.end,
        symbols=request.api_symbols,
        schema=request.schema,
        stype_in=request.stype_in,
        stype_out=request.stype_out,
        path=request.raw_output_path,
    )
    df = store.to_df(price_type="float", pretty_ts=True, map_symbols=True)
    normalized = normalize_ohlcv_frame(df, symbol=None, timeframe=request.timeframe, symbol_map=request.symbol_map)
    stats = validate_ohlcv_frame(normalized, timeframe=request.timeframe)
    normalized.to_parquet(request.output_path, index=False)
    return {
        "dry_run": False,
        "network_request_made": True,
        "cache_hit": False,
        "request": request.to_dict(),
        "output_path": request.output_path,
        "raw_output_path": request.raw_output_path,
        "record_count": _safe_record_count(store),
        "estimate": estimate,
        "validation": stats,
    }


def download_historical_raw_only(request: DatabentoRequest, key: str | None = None) -> dict[str, Any]:
    """Download DBN/ZST without converting or inspecting rows.

    This is used for protected lockbox periods where acquisition is allowed but
    programmatic inspection must wait until a freeze manifest authorizes access.
    """
    if key is None:
        key = validate_environment(dry_run=False)
    db = _import_databento()
    raw_path = Path(request.raw_output_path)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    client = db.Historical(key)
    client.timeseries.get_range(
        dataset=request.dataset,
        start=request.start,
        end=request.end,
        symbols=request.api_symbols,
        schema=request.schema,
        stype_in=request.stype_in,
        stype_out=request.stype_out,
        path=request.raw_output_path,
    )
    return {
        "network_request_made": True,
        "raw_only": True,
        "request": request.to_dict(),
        "raw_output_path": request.raw_output_path,
    }


def load_cached_ohlcv(path: str | Path, symbol: str | None = None, timeframe: str = "1m") -> pd.DataFrame:
    source = Path(path)
    if source.suffix == ".csv":
        df = pd.read_csv(source)
    elif source.suffix == ".parquet":
        df = pd.read_parquet(source)
    else:
        raise DatabentoConfigError(f"Unsupported cache format for loader interface: {source.suffix}")
    normalized = normalize_ohlcv_frame(df, symbol=symbol, timeframe=timeframe)
    if symbol:
        normalized = normalized[normalized["symbol"] == symbol].copy()
        if normalized.empty:
            raise DatabentoConfigError(f"Cached OHLCV data contains no rows for symbol '{symbol}'.")
    return normalized.reset_index(drop=True)


def load_cached_databento_range(
    dataset: str,
    schema: str,
    symbols: list[str],
    start: str,
    end: str,
    cache_folder: str = "data/cache/databento",
    timeframe: str | None = None,
) -> pd.DataFrame:
    request = request_from_config(
        {"data": {"databento": {"dataset": dataset, "schema": schema, "symbols": symbols, "start_date": start, "end_date": end, "cache_folder": cache_folder}}},
        symbols=symbols,
        start=start,
        end=end,
        schema=schema,
        dataset=dataset,
    )
    path = Path(request.output_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Databento cache not found at {path}. Run scripts/download_historical_futures.py for the same dataset/schema/symbols/date range first."
        )
    return load_cached_ohlcv(path, timeframe=timeframe or request.timeframe)


def normalize_ohlcv_frame(
    df: pd.DataFrame,
    symbol: str | None,
    timeframe: str,
    symbol_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    out = df.copy()
    rename = {
        "ts_event": "timestamp",
        "open_price": "open",
        "high_price": "high",
        "low_price": "low",
        "close_price": "close",
    }
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
    if "timestamp" not in out.columns and out.index.name:
        out = out.reset_index().rename(columns={out.index.name: "timestamp"})
    if "symbol" not in out.columns:
        if symbol is None:
            raise DatabentoConfigError("OHLCV data is missing symbol and no fallback symbol was supplied.")
        out["symbol"] = symbol
    if symbol_map:
        out["symbol"] = out["symbol"].astype(str).map(lambda value: symbol_map.get(value, value.split(".", 1)[0] if ".c." in value.lower() else value))
    elif symbol is not None:
        out["symbol"] = out["symbol"].fillna(symbol)
    out["timeframe"] = timeframe
    if "session_id" not in out.columns:
        timestamps = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
        out["session_id"] = timestamps.dt.date.astype(str)
    missing = [c for c in REQUIRED_OHLCV_COLUMNS if c not in out.columns]
    if missing:
        raise DatabentoConfigError(f"Cached OHLCV data is missing required columns: {', '.join(missing)}")
    out = out.loc[:, REQUIRED_OHLCV_COLUMNS].copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="raise")
    for col in ("open", "high", "low", "close", "volume"):
        out[col] = pd.to_numeric(out[col], errors="raise")
    return out.sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def validate_ohlcv_frame(df: pd.DataFrame, timeframe: str = "1m") -> dict[str, Any]:
    missing = [c for c in REQUIRED_CORE_OHLCV_COLUMNS if c not in df.columns]
    if missing:
        raise DatabentoConfigError(f"Normalized OHLCV data is missing required columns: {', '.join(missing)}")
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="raise")
    if out["timestamp"].isna().any():
        raise DatabentoConfigError("Normalized OHLCV data contains invalid timestamps.")
    now = pd.Timestamp.now(tz="UTC")
    if out["timestamp"].max() > now:
        raise DatabentoConfigError("Normalized OHLCV data contains timestamps in the future.")
    duplicates = int(out.duplicated(["timestamp", "symbol"]).sum())
    if duplicates:
        raise DatabentoConfigError(f"Normalized OHLCV data contains {duplicates} duplicate timestamp/symbol rows.")
    rows_by_symbol: dict[str, int] = {}
    missing_intervals: dict[str, dict[str, Any]] = {}
    for symbol, group in out.groupby("symbol", sort=True):
        group = group.sort_values("timestamp")
        if not group["timestamp"].is_monotonic_increasing:
            raise DatabentoConfigError(f"Timestamps are not sorted for {symbol}.")
        rows_by_symbol[str(symbol)] = int(len(group))
        gaps = group["timestamp"].diff().dropna()
        expected = pd.Timedelta(minutes=1) if timeframe == "1m" else None
        if expected is not None:
            large_gaps = gaps[gaps > expected]
            missing_intervals[str(symbol)] = {
                "gap_count_gt_1m": int(len(large_gaps)),
                "max_gap_seconds": float(large_gaps.max().total_seconds()) if len(large_gaps) else 0.0,
            }
    return {
        "row_count": int(len(out)),
        "rows_by_symbol": rows_by_symbol,
        "missing_intervals": missing_intervals,
        "duplicate_timestamp_symbol_rows": duplicates,
        "future_timestamp_rows": int((out["timestamp"] > now).sum()),
        "columns": list(out.columns),
    }


def _import_databento():
    try:
        import databento as db
    except ImportError as exc:
        raise DatabentoDependencyError(INSTALL_MESSAGE) from exc
    return db


def _output_path(cache_folder: str, dataset: str, schema: str, symbols: list[str], start: str, end: str, suffix: str) -> Path:
    safe_dataset = _safe_part(dataset)
    safe_schema = _safe_part(schema)
    safe_symbols = "_".join(_safe_part(str(s)) for s in symbols)
    safe_start = _safe_part(start)
    safe_end = _safe_part(end)
    return project_path(cache_folder) / f"{safe_dataset}_{safe_schema}_{safe_symbols}_{safe_start}_{safe_end}{suffix}"


def _safe_part(value: str) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in value).strip("-") or "unset"


def _timeframe_from_schema(schema: str) -> str:
    return schema.rsplit("-", 1)[-1] if "-" in schema else ""


def _safe_record_count(store: Any) -> int | None:
    try:
        return len(store)
    except TypeError:
        return None
