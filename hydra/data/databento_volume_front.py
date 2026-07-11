from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.data.acquisition_policy import decide_databento_acquisition, record_download_complete
from hydra.data.budget import DatabentoBudgetConfig, cumulative_spend, sha256_file
from hydra.data.contract_mapping import ContractInfo, RollMap, load_roll_map, write_roll_map
from hydra.data.databento_loader import (
    DatabentoCostLimitError,
    DatabentoRequest,
    estimate_request,
    normalize_ohlcv_frame,
    validate_ohlcv_frame,
)
from hydra.utils.config import project_path


VOLUME_FRONT_MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_VOLUME_RANK_V1"
DEFAULT_ROOTS = ("GC", "MGC")
DEFAULT_START = "2023-01-01"
DEFAULT_END = "2024-10-01"


class VolumeFrontDataError(RuntimeError):
    pass


def volume_front_request(
    *,
    roots: tuple[str, ...] = DEFAULT_ROOTS,
    start: str = DEFAULT_START,
    end: str = DEFAULT_END,
    dataset: str = "GLBX.MDP3",
    schema: str = "ohlcv-1m",
    cache_folder: str = "data/cache/databento",
) -> DatabentoRequest:
    if not roots or any(not str(root).isalpha() for root in roots):
        raise VolumeFrontDataError("Volume-front roots are invalid.")
    if start >= end or end > "2024-10-01":
        raise VolumeFrontDataError("Volume-front request must exclude Q4 2024.")
    api_symbols = [f"{root}.v.0" for root in roots]
    safe_dataset = dataset.replace(".", "-")
    safe_schema = schema.replace("/", "-")
    label = "_".join(symbol.replace(".", "-") for symbol in api_symbols)
    prefix = f"{safe_dataset}_{safe_schema}_{label}_{start}_{end}"
    folder = project_path(cache_folder)
    return DatabentoRequest(
        dataset=dataset,
        schema=schema,
        symbols=api_symbols,
        api_symbols=api_symbols,
        symbol_map={symbol: root for symbol, root in zip(api_symbols, roots)},
        start=start,
        end=end,
        timeframe="1m",
        stype_in="continuous",
        stype_out="instrument_id",
        cache_folder=str(folder),
        raw_output_path=str(folder / f"{prefix}.dbn.zst"),
        output_path=str(folder / f"{prefix}.parquet"),
    )


def acquire_volume_front(
    request: DatabentoRequest,
    *,
    key: str,
    budget: DatabentoBudgetConfig,
    base_roll_map_path: str | Path,
    output_report_dir: str | Path,
    estimate: dict[str, Any] | None = None,
    maximum_cost_usd: float = 5.0,
    minimum_remaining_usd: float = 30.0,
    minimum_rows_per_root: int = 100_000,
) -> dict[str, Any]:
    if request.end > "2024-10-01" or request.api_symbols != ["GC.v.0", "MGC.v.0"]:
        raise VolumeFrontDataError("Frozen volume-front request contract changed.")
    official_estimate = estimate or estimate_request(request, key)
    cost = float(official_estimate["estimated_cost_usd"])
    if cost > maximum_cost_usd:
        raise DatabentoCostLimitError(
            f"Volume-front estimate ${cost:.6f} exceeds cap ${maximum_cost_usd:.2f}."
        )
    _estimated, actual = cumulative_spend(project_path(budget.ledger_path))
    if budget.hard_cap_usd - actual - cost < minimum_remaining_usd:
        raise DatabentoCostLimitError(
            "Volume-front request would violate the protected remaining budget."
        )
    purpose = (
        "repair GC/MGC development ecology with volume-ranked front contracts; "
        "calendar-front GC was event-starved; Q4 excluded"
    )
    decision = decide_databento_acquisition(
        request,
        budget,
        research_purpose=purpose,
        candidate_tier="DATA_REPRESENTATION_REPAIR",
        key=key,
        estimate=official_estimate,
    )
    if decision.reason == "duplicate_request_blocked_by_ledger":
        raise VolumeFrontDataError("Prior paid request exists but verified cache is missing.")
    output_path = Path(request.output_path)
    raw_path = Path(request.raw_output_path)
    network_request_made = False
    if decision.may_download:
        _download_request_atomic(request, key=key)
        network_request_made = True
    if not output_path.is_file() or not raw_path.is_file():
        raise VolumeFrontDataError("Volume-front raw or normalized cache is missing.")
    frame = pd.read_parquet(output_path)
    validation = validate_volume_front_frame(
        frame,
        roots=DEFAULT_ROOTS,
        start=request.start,
        end=request.end,
        minimum_rows_per_root=minimum_rows_per_root,
    )
    mappings = volume_mappings_from_dbn(raw_path)
    base_map = load_roll_map(base_roll_map_path)
    roll_map = build_volume_front_roll_map(
        mappings,
        base_map,
        roots=DEFAULT_ROOTS,
        start=request.start,
        end=request.end,
        data_checksum=sha256_file(output_path),
    )
    map_path, _map_hash = write_roll_map(roll_map)
    if load_roll_map(map_path).roll_map_hash() != roll_map.roll_map_hash():
        raise VolumeFrontDataError("Volume-front roll map does not round-trip.")
    actual_record = None
    if decision.may_download:
        actual_record = record_download_complete(
            request,
            budget,
            decision.request_id,
            decision.estimated_cost_usd,
            decision.estimated_cost_usd,
            purpose,
            "DATA_REPRESENTATION_REPAIR",
        )
    destination = Path(output_report_dir)
    destination.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "gc_mgc_volume_front_data_repair_v1",
        "scientific_conclusion": "GC_VOLUME_FRONT_DEVELOPMENT_REPRESENTATION_REPAIRED",
        "request_id": decision.request_id,
        "request": {
            key_name: value
            for key_name, value in request.to_dict().items()
            if "key" not in key_name.lower()
        },
        "official_estimate": official_estimate,
        "network_request_made": network_request_made,
        "cache_hit": decision.cache_hit,
        "data_path": str(output_path),
        "data_sha256": sha256_file(output_path),
        "raw_path": str(raw_path),
        "raw_sha256": sha256_file(raw_path),
        "roll_map_path": str(map_path),
        "roll_map_hash": roll_map.roll_map_hash(),
        "roll_map_type": roll_map.map_type,
        "validation": validation,
        "actual_spend_usd": (
            float(actual_record.actual_cost_usd or 0.0) if actual_record else 0.0
        ),
        "q4_access_count_delta": 0,
        "api_key_recorded": False,
        "strategy_status_changes": 0,
    }
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "gc_mgc_volume_front_data_repair.json"
    report_path = destination / "gc_mgc_volume_front_data_repair.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {**payload, "result_path": str(result_path), "report_path": str(report_path)}


def validate_volume_front_frame(
    frame: pd.DataFrame,
    *,
    roots: tuple[str, ...],
    start: str,
    end: str,
    minimum_rows_per_root: int,
) -> dict[str, Any]:
    stats = validate_ohlcv_frame(frame, timeframe="1m")
    observed = set(frame["symbol"].astype(str).unique())
    if observed != set(roots):
        raise VolumeFrontDataError(f"Unexpected volume-front symbols: {sorted(observed)}")
    rows = {root: int(frame["symbol"].astype(str).eq(root).sum()) for root in roots}
    if any(count < minimum_rows_per_root for count in rows.values()):
        raise VolumeFrontDataError(f"Volume-front coverage remains insufficient: {rows}")
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    if timestamps.min() < pd.Timestamp(start, tz="UTC") or timestamps.max() >= pd.Timestamp(
        end, tz="UTC"
    ):
        raise VolumeFrontDataError("Volume-front timestamps exceed frozen development period.")
    if not (
        (frame["high"].astype(float) >= frame[["open", "close", "low"]].max(axis=1)).all()
        and (frame["low"].astype(float) <= frame[["open", "close", "high"]].min(axis=1)).all()
    ):
        raise VolumeFrontDataError("Volume-front OHLC relationships are invalid.")
    return {**stats, "rows_by_required_root": rows, "q4_rows": 0}


def volume_mappings_from_dbn(path: str | Path) -> dict[str, list[dict[str, str]]]:
    import databento as db

    store = db.DBNStore.from_file(path)
    return normalize_volume_mappings(store.metadata.mappings)


def normalize_volume_mappings(
    mappings: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, str]]]:
    if not isinstance(mappings, dict):
        raise VolumeFrontDataError("DBN volume symbology mapping has an unsupported shape.")
    output = {
        str(raw_symbol): [
            {
                "d0": str(interval["start_date"]),
                "d1": str(interval["end_date"]),
                "s": str(interval["symbol"]),
            }
            for interval in intervals
        ]
        for raw_symbol, intervals in mappings.items()
    }
    expected = {"GC.v.0", "MGC.v.0"}
    if set(output) != expected or any(not rows for rows in output.values()):
        raise VolumeFrontDataError("DBN volume symbology mapping is incomplete.")
    return output


def build_volume_front_roll_map(
    mappings: dict[str, list[dict[str, str]]],
    base_map: RollMap,
    *,
    roots: tuple[str, ...],
    start: str,
    end: str,
    data_checksum: str,
) -> RollMap:
    crosswalk: dict[tuple[str, str], ContractInfo] = {
        (item.root, str(item.instrument_id)): item
        for item in base_map.contracts
        if item.root in roots and item.instrument_id
    }
    contracts: list[ContractInfo] = []
    for root in roots:
        continuous = f"{root}.v.0"
        intervals = mappings.get(continuous) or []
        for index, interval in enumerate(intervals):
            instrument_id = str(interval["s"])
            base = crosswalk.get((root, instrument_id))
            if base is None:
                raise VolumeFrontDataError(
                    f"No verified definition crosswalk for {root} instrument {instrument_id}."
                )
            active_start, active_end = str(interval["d0"]), str(interval["d1"])
            contracts.append(
                replace(
                    base,
                    active_start=active_start,
                    active_end=active_end,
                    roll_date=active_start if index else active_end,
                    continuous_symbol=continuous,
                    deactivation_time=active_end,
                    roll_reason="databento_previous_day_volume_rank_transition",
                    transition_uncertainty="date_level_previous_day_volume_rank",
                )
            )
    result = RollMap(
        dataset=base_map.dataset,
        schema=base_map.schema,
        map_type=VOLUME_FRONT_MAP_TYPE,
        symbols=list(roots),
        contracts=sorted(
            contracts, key=lambda item: (item.root, item.active_start, item.contract)
        ),
        unsafe_window_days=1,
        notes=[
            "Continuous intervals use Databento volume rank v.0, based on previous-day volume.",
            "Raw contracts, ticks, multipliers and expiries inherit the verified date-aware definition crosswalk.",
            "Unadjusted prices are retained and roll-transition days remain excluded.",
        ],
        source_metadata={
            "period_start": start,
            "period_end": end,
            "continuous_symbols": [f"{root}.v.0" for root in roots],
            "definition_crosswalk_roll_map_hash": base_map.roll_map_hash(),
            "data_sha256": data_checksum,
            "q4_excluded": True,
        },
    )
    if not result.contracts or {item.root for item in result.contracts} != set(roots):
        raise VolumeFrontDataError("Volume-front roll map lacks required roots.")
    return result


def _download_request_atomic(request: DatabentoRequest, *, key: str) -> None:
    import databento as db

    raw_path = Path(request.raw_output_path)
    output_path = Path(request.output_path)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_tmp = raw_path.with_name(f".{raw_path.name}.tmp.dbn.zst")
    parquet_tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    for path in (raw_tmp, parquet_tmp):
        path.unlink(missing_ok=True)
    try:
        client = db.Historical(key)
        store = client.timeseries.get_range(
            dataset=request.dataset,
            start=request.start,
            end=request.end,
            symbols=request.api_symbols,
            schema=request.schema,
            stype_in=request.stype_in,
            stype_out=request.stype_out,
            path=str(raw_tmp),
        )
        frame = store.to_df(price_type="float", pretty_ts=True, map_symbols=True)
        normalized = normalize_ohlcv_frame(
            frame,
            symbol=None,
            timeframe=request.timeframe,
            symbol_map=request.symbol_map,
        )
        validate_volume_front_frame(
            normalized,
            roots=DEFAULT_ROOTS,
            start=request.start,
            end=request.end,
            minimum_rows_per_root=100_000,
        )
        normalized.to_parquet(parquet_tmp, index=False)
        raw_tmp.replace(raw_path)
        parquet_tmp.replace(output_path)
    except Exception:
        raw_tmp.unlink(missing_ok=True)
        parquet_tmp.unlink(missing_ok=True)
        raise


def _stable_hash(payload: dict[str, Any]) -> str:
    import hashlib

    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _write_immutable(path: Path, content: str) -> None:
    if path.exists() and path.read_text(encoding="utf-8") != content:
        raise VolumeFrontDataError(f"Refusing divergent immutable artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(content, encoding="utf-8")


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# GC/MGC Volume-Front Development Data Repair",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Request ID: `{payload['request_id']}`",
            f"- Official estimate USD: `{payload['official_estimate']['estimated_cost_usd']}`",
            f"- Actual incremental spend USD: `{payload['actual_spend_usd']}`",
            f"- Rows: `{payload['validation']['row_count']}`",
            f"- Rows by root: `{payload['validation']['rows_by_required_root']}`",
            f"- Data SHA-256: `{payload['data_sha256']}`",
            f"- Roll map: `{payload['roll_map_hash']}`",
            "- Q4 access delta: `0`",
            "- Strategy status changes: `0`",
            "",
        ]
    )
