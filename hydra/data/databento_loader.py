from __future__ import annotations

import os
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

REQUIRED_OHLCV_COLUMNS = ("timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume", "session_id")


class DatabentoConfigError(ValueError):
    pass


class DatabentoMissingKeyError(RuntimeError):
    pass


class DatabentoDependencyError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabentoRequest:
    dataset: str
    schema: str
    symbols: list[str]
    start: str
    end: str
    timeframe: str
    stype_in: str
    stype_out: str
    cache_folder: str
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
    request = DatabentoRequest(
        dataset=str(resolved_dataset or ""),
        schema=str(resolved_schema or ""),
        symbols=[str(s) for s in resolved_symbols],
        start=str(resolved_start or ""),
        end=str(resolved_end or ""),
        timeframe=str(timeframe),
        stype_in=str(stype_in),
        stype_out=str(stype_out),
        cache_folder=str(cache_folder),
        output_path=str(_output_path(cache_folder, str(resolved_dataset or ""), str(resolved_schema or ""), resolved_symbols, str(resolved_start or ""), str(resolved_end or ""))),
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


def download_historical_ohlcv(
    config: dict[str, Any],
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    schema: str | None = None,
    dataset: str | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    request = request_from_config(config, symbols=symbols, start=start, end=end, schema=schema, dataset=dataset)
    if dry_run:
        return dry_run_plan(request)
    key = validate_environment(dry_run=False)
    db = _import_databento()
    Path(request.output_path).parent.mkdir(parents=True, exist_ok=True)
    client = db.Historical(key)
    store = client.timeseries.get_range(
        dataset=request.dataset,
        start=request.start,
        end=request.end,
        symbols=request.symbols,
        schema=request.schema,
        stype_in=request.stype_in,
        stype_out=request.stype_out,
        path=request.output_path,
    )
    return {
        "dry_run": False,
        "network_request_made": True,
        "request": request.to_dict(),
        "output_path": request.output_path,
        "record_count": _safe_record_count(store),
    }


def load_cached_ohlcv(path: str | Path, symbol: str, timeframe: str) -> pd.DataFrame:
    source = Path(path)
    if source.suffix == ".csv":
        df = pd.read_csv(source)
    elif source.suffix == ".parquet":
        df = pd.read_parquet(source)
    else:
        raise DatabentoConfigError(f"Unsupported cache format for loader interface: {source.suffix}")
    return normalize_ohlcv_frame(df, symbol=symbol, timeframe=timeframe)


def normalize_ohlcv_frame(df: pd.DataFrame, symbol: str, timeframe: str) -> pd.DataFrame:
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
    out["symbol"] = out.get("symbol", symbol)
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
    return out.sort_values("timestamp").reset_index(drop=True)


def _import_databento():
    try:
        import databento as db
    except ImportError as exc:
        raise DatabentoDependencyError(INSTALL_MESSAGE) from exc
    return db


def _output_path(cache_folder: str, dataset: str, schema: str, symbols: list[str], start: str, end: str) -> Path:
    safe_dataset = _safe_part(dataset)
    safe_schema = _safe_part(schema)
    safe_symbols = "_".join(_safe_part(str(s)) for s in symbols)
    safe_start = _safe_part(start)
    safe_end = _safe_part(end)
    return project_path(cache_folder) / f"{safe_dataset}_{safe_schema}_{safe_symbols}_{safe_start}_{safe_end}.dbn.zst"


def _safe_part(value: str) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in value).strip("-") or "unset"


def _timeframe_from_schema(schema: str) -> str:
    return schema.rsplit("-", 1)[-1] if "-" in schema else ""


def _safe_record_count(store: Any) -> int | None:
    try:
        return len(store)
    except TypeError:
        return None
