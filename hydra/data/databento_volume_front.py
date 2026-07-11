from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.data.acquisition_policy import decide_databento_acquisition, record_download_complete
from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
    cumulative_spend,
    enforce_budget,
    read_ledger,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.contract_mapping import (
    ContractInfo,
    RollMap,
    load_roll_map,
    resolve_date_aware_definition,
    write_roll_map,
)
from hydra.data.databento_loader import (
    DatabentoCostLimitError,
    DatabentoRequest,
    estimate_request,
    normalize_ohlcv_frame,
    validate_ohlcv_frame,
)
from hydra.markets.instruments import instrument_spec
from hydra.utils.config import project_path


VOLUME_FRONT_MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_VOLUME_RANK_V1"
DEFAULT_ROOTS = ("GC", "MGC")
DEFAULT_START = "2023-01-01"
DEFAULT_END = "2024-10-01"
FROZEN_SUPPLEMENTAL_DEFINITION_IDS = ("393", "1974")
MAXIMUM_SUPPLEMENTAL_DEFINITION_COST_USD = 0.005


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
    maximum_definition_cost_usd: float = MAXIMUM_SUPPLEMENTAL_DEFINITION_COST_USD,
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
    supplemental_contracts, definition_audit = ensure_volume_front_definitions(
        mappings,
        base_map,
        key=key,
        budget=budget,
        start=request.start,
        end=request.end,
        maximum_cost_usd=maximum_definition_cost_usd,
        minimum_remaining_usd=minimum_remaining_usd,
    )
    roll_map = build_volume_front_roll_map(
        mappings,
        base_map,
        roots=DEFAULT_ROOTS,
        start=request.start,
        end=request.end,
        data_checksum=sha256_file(output_path),
        supplemental_contracts=supplemental_contracts,
    )
    map_path, _map_hash = write_roll_map(roll_map)
    if load_roll_map(map_path).roll_map_hash() != roll_map.roll_map_hash():
        raise VolumeFrontDataError("Volume-front roll map does not round-trip.")
    actual_record = None
    recovered_interrupted_download = (
        not decision.may_download
        and has_unreconciled_download(budget, decision.request_id)
    )
    if decision.may_download or recovered_interrupted_download:
        actual_record = record_download_complete(
            request,
            budget,
            decision.request_id,
            cost,
            cost,
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
        "network_request_made": bool(
            network_request_made or definition_audit["network_request_made"]
        ),
        "ohlcv_network_request_made": network_request_made,
        "cache_hit": decision.cache_hit,
        "data_path": str(output_path),
        "data_sha256": sha256_file(output_path),
        "raw_path": str(raw_path),
        "raw_sha256": sha256_file(raw_path),
        "roll_map_path": str(map_path),
        "roll_map_hash": roll_map.roll_map_hash(),
        "roll_map_type": roll_map.map_type,
        "supplemental_definitions": definition_audit,
        "validation": validation,
        "actual_spend_usd": (
            float(actual_record.actual_cost_usd or 0.0) if actual_record else 0.0
        )
        + float(definition_audit["actual_spend_usd"]),
        "ohlcv_spend_recovered_after_interrupted_map_build": recovered_interrupted_download,
        "q4_access_count_delta": 0,
        "api_key_recorded": False,
        "strategy_status_changes": 0,
    }
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "gc_mgc_volume_front_data_repair.json"
    report_path = destination / "gc_mgc_volume_front_data_repair.md"
    if result_path.exists():
        existing = json.loads(result_path.read_text(encoding="utf-8"))
        immutable_checks = {
            "data_sha256": payload["data_sha256"],
            "roll_map_hash": payload["roll_map_hash"],
            "q4_access_count_delta": 0,
            "strategy_status_changes": 0,
        }
        if any(existing.get(key) != value for key, value in immutable_checks.items()):
            raise VolumeFrontDataError(
                "Existing volume-front result does not match the verified cache."
            )
        _write_immutable(report_path, _render_report(existing))
        return {
            **existing,
            "result_path": str(result_path),
            "report_path": str(report_path),
        }
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


def missing_volume_definition_segments(
    mappings: dict[str, list[dict[str, str]]],
    base_map: RollMap,
) -> list[dict[str, str]]:
    available = {
        (contract.root, str(contract.instrument_id))
        for contract in base_map.contracts
        if contract.instrument_id
    }
    missing = [
        {
            "root": continuous.split(".", 1)[0],
            "continuous_symbol": continuous,
            "instrument_id": str(interval["s"]),
            "active_start": str(interval["d0"]),
            "active_end": str(interval["d1"]),
        }
        for continuous, intervals in mappings.items()
        for interval in intervals
        if (continuous.split(".", 1)[0], str(interval["s"])) not in available
    ]
    return sorted(
        missing,
        key=lambda row: (row["root"], row["active_start"], row["instrument_id"]),
    )


def ensure_volume_front_definitions(
    mappings: dict[str, list[dict[str, str]]],
    base_map: RollMap,
    *,
    key: str,
    budget: DatabentoBudgetConfig,
    start: str,
    end: str,
    maximum_cost_usd: float,
    minimum_remaining_usd: float,
) -> tuple[list[ContractInfo], dict[str, Any]]:
    import databento as db

    missing = missing_volume_definition_segments(mappings, base_map)
    if not missing:
        return [], {
            "missing_instrument_ids": [],
            "network_request_made": False,
            "cache_hit": True,
            "actual_spend_usd": 0.0,
            "definition_path": None,
            "definition_sha256": None,
        }
    instrument_ids = sorted(
        {row["instrument_id"] for row in missing}, key=lambda value: int(value)
    )
    if tuple(instrument_ids) != FROZEN_SUPPLEMENTAL_DEFINITION_IDS:
        raise VolumeFrontDataError(
            f"Unexpected missing volume-front definitions: {instrument_ids}"
        )
    purpose = (
        "recover explicit definitions 393/1974 required by frozen GC/MGC "
        "volume-front development representation; Q4 excluded"
    )
    request_payload = {
        "dataset": base_map.dataset,
        "schema": "definition",
        "symbols": instrument_ids,
        "stype_in": "instrument_id",
        "start": start,
        "end": end,
        "purpose": purpose,
        "candidate_tier": "DATA_INTEGRITY_RECOVERY",
    }
    request_id = request_id_for(request_payload)
    cache_tag = _stable_hash(request_payload)[:16]
    definition_path = project_path(
        "data",
        "cache",
        "contract_maps",
        f"volume_front_definitions_{base_map.dataset.replace('.', '-')}_{start}_{end}_{cache_tag}.dbn.zst",
    )
    ledger_path = project_path(budget.ledger_path)
    ledger = read_ledger(ledger_path)
    prior = [row for row in ledger if row.get("request_id") == request_id]
    completed = any(
        str(row.get("download_status", "")).startswith("DOWNLOADED")
        for row in prior
    )
    planned = [row for row in prior if row.get("download_status") == "ESTIMATED_ONLY"]
    network_request_made = False
    actual_spend = 0.0
    official_estimate: dict[str, Any] | None = None
    if completed and not definition_path.is_file():
        raise VolumeFrontDataError(
            "Supplemental definition ledger is complete but its cache is missing."
        )
    if not definition_path.is_file():
        client = db.Historical(key)
        kwargs = {
            "dataset": base_map.dataset,
            "start": start,
            "end": end,
            "symbols": instrument_ids,
            "schema": "definition",
            "stype_in": "instrument_id",
        }
        official_estimate = {
            "record_count": int(client.metadata.get_record_count(**kwargs)),
            "estimated_cost_usd": float(client.metadata.get_cost(**kwargs)),
            "billable_size_bytes": int(client.metadata.get_billable_size(**kwargs)),
        }
        cost = float(official_estimate["estimated_cost_usd"])
        if cost > maximum_cost_usd:
            raise DatabentoCostLimitError(
                f"Supplemental definition estimate ${cost:.9f} exceeds ${maximum_cost_usd:.3f}."
            )
        _enforce_effective_remaining_budget(
            budget,
            incremental_cost_usd=cost,
            minimum_remaining_usd=minimum_remaining_usd,
        )
        projected, actual_before = enforce_budget(budget, cost)
        append_spend_record(
            budget,
            DatabentoSpendRecord(
                request_id=request_id,
                timestamp_utc=utc_now(),
                dataset=base_map.dataset,
                schema="definition",
                symbols=instrument_ids,
                stype_in="instrument_id",
                start=start,
                end=end,
                estimated_cost_usd=cost,
                actual_cost_usd=None,
                cumulative_estimated_spend_usd=projected,
                cumulative_actual_spend_usd=actual_before,
                cache_hit=False,
                research_purpose=purpose,
                candidate_tier="DATA_INTEGRITY_RECOVERY",
                approval_mode=AUTO_UNDER_HARD_CAP,
                resulting_file=None,
                checksum=None,
                download_status="ESTIMATED_ONLY",
            ),
        )
        definition_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = definition_path.with_name(f".{definition_path.name}.tmp.dbn.zst")
        temporary.unlink(missing_ok=True)
        try:
            store = client.timeseries.get_range(
                **kwargs,
                stype_out="instrument_id",
                path=str(temporary),
            )
            history = store.to_df(pretty_ts=True, map_symbols=False).reset_index()
            supplemental = supplemental_contracts_from_definitions(missing, history)
            temporary.replace(definition_path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        network_request_made = True
        actual_spend = cost
        _append_definition_completion(
            budget,
            request_id=request_id,
            dataset=base_map.dataset,
            instrument_ids=instrument_ids,
            start=start,
            end=end,
            purpose=purpose,
            definition_path=definition_path,
            actual_cost_usd=cost,
            status="DOWNLOADED",
        )
    else:
        store = db.DBNStore.from_file(definition_path)
        history = store.to_df(pretty_ts=True, map_symbols=False).reset_index()
        supplemental = supplemental_contracts_from_definitions(missing, history)
        if planned and not completed:
            recovered_cost = float(planned[-1].get("estimated_cost_usd") or 0.0)
            if recovered_cost <= 0.0 or recovered_cost > maximum_cost_usd:
                raise VolumeFrontDataError(
                    "Cannot safely reconcile supplemental definition spend."
                )
            actual_spend = recovered_cost
            _append_definition_completion(
                budget,
                request_id=request_id,
                dataset=base_map.dataset,
                instrument_ids=instrument_ids,
                start=start,
                end=end,
                purpose=purpose,
                definition_path=definition_path,
                actual_cost_usd=recovered_cost,
                status="DOWNLOADED_RECOVERED_AFTER_INTERRUPTED_VOLUME_MAP_BUILD",
            )
        elif not completed:
            raise VolumeFrontDataError(
                "Unledgered supplemental definition cache cannot be trusted."
            )
    return supplemental, {
        "request_id": request_id,
        "missing_instrument_ids": instrument_ids,
        "network_request_made": network_request_made,
        "cache_hit": not network_request_made,
        "actual_spend_usd": actual_spend,
        "official_estimate": official_estimate,
        "definition_path": str(definition_path),
        "definition_sha256": sha256_file(definition_path),
        "supplemental_contracts": [contract.contract for contract in supplemental],
    }


def supplemental_contracts_from_definitions(
    missing: list[dict[str, str]], definition_history: pd.DataFrame
) -> list[ContractInfo]:
    contracts: list[ContractInfo] = []
    for segment in missing:
        root = segment["root"]
        definition = resolve_date_aware_definition(
            definition_history,
            instrument_id=segment["instrument_id"],
            active_start=segment["active_start"],
            root=root,
        )
        spec = instrument_spec(root)
        raw_symbol = str(definition["raw_symbol"])
        expiration = pd.Timestamp(definition.get("expiration"))
        if pd.isna(expiration):
            raise VolumeFrontDataError(f"Definition {raw_symbol} lacks expiration.")
        multiplier = float(definition.get("unit_of_measure_qty") or spec.point_value)
        if abs(multiplier - float(spec.point_value)) > 1e-12:
            raise VolumeFrontDataError(
                f"Definition multiplier {multiplier} for {raw_symbol} differs from {spec.point_value}."
            )
        suffix = raw_symbol[len(root) :]
        if len(suffix) < 2:
            raise VolumeFrontDataError(f"Cannot parse definition symbol {raw_symbol}.")
        activation = definition.get("activation")
        contracts.append(
            ContractInfo(
                root=root,
                contract=raw_symbol,
                month_code=suffix[0],
                year=int(expiration.year),
                expiry_date=str(expiration.date()),
                last_trade_date=str(expiration.date()),
                active_start=segment["active_start"],
                active_end=segment["active_end"],
                roll_date=segment["active_start"],
                tick_size=float(definition["min_price_increment"]),
                tick_value=spec.tick_value,
                point_value=spec.point_value,
                contract_multiplier=spec.point_value,
                is_micro=spec.is_micro,
                instrument_id=segment["instrument_id"],
                parent_symbol=root,
                continuous_symbol=segment["continuous_symbol"],
                activation_time=(
                    pd.Timestamp(activation).isoformat()
                    if activation is not None and not pd.isna(activation)
                    else None
                ),
                deactivation_time=segment["active_end"],
                roll_reason="databento_previous_day_volume_rank_transition",
                transition_uncertainty="date_level_previous_day_volume_rank",
            )
        )
    return contracts


def has_unreconciled_download(
    budget: DatabentoBudgetConfig, request_id: str
) -> bool:
    rows = [
        row
        for row in read_ledger(project_path(budget.ledger_path))
        if row.get("request_id") == request_id
    ]
    return any(row.get("download_status") == "ESTIMATED_ONLY" for row in rows) and not any(
        str(row.get("download_status", "")).startswith("DOWNLOADED") for row in rows
    )


def _enforce_effective_remaining_budget(
    budget: DatabentoBudgetConfig,
    *,
    incremental_cost_usd: float,
    minimum_remaining_usd: float,
) -> None:
    rows = read_ledger(project_path(budget.ledger_path))
    completed_ids = {
        str(row.get("request_id"))
        for row in rows
        if str(row.get("download_status", "")).startswith("DOWNLOADED")
    }
    outstanding = sum(
        float(row.get("estimated_cost_usd") or 0.0)
        for row in rows
        if row.get("download_status") == "ESTIMATED_ONLY"
        and str(row.get("request_id")) not in completed_ids
    )
    _estimated, actual = cumulative_spend(project_path(budget.ledger_path))
    effective_actual = actual + outstanding + float(incremental_cost_usd)
    if budget.hard_cap_usd - effective_actual < minimum_remaining_usd:
        raise DatabentoCostLimitError(
            "Supplemental definitions would violate the protected effective remaining budget."
        )


def _append_definition_completion(
    budget: DatabentoBudgetConfig,
    *,
    request_id: str,
    dataset: str,
    instrument_ids: list[str],
    start: str,
    end: str,
    purpose: str,
    definition_path: Path,
    actual_cost_usd: float,
    status: str,
) -> None:
    projected, actual_before = enforce_budget(budget, 0.0)
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=dataset,
            schema="definition",
            symbols=instrument_ids,
            stype_in="instrument_id",
            start=start,
            end=end,
            estimated_cost_usd=0.0,
            actual_cost_usd=actual_cost_usd,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual_before + actual_cost_usd,
            cache_hit=False,
            research_purpose=purpose,
            candidate_tier="DATA_INTEGRITY_RECOVERY",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(definition_path),
            checksum=sha256_file(definition_path),
            download_status=status,
        ),
    )


def build_volume_front_roll_map(
    mappings: dict[str, list[dict[str, str]]],
    base_map: RollMap,
    *,
    roots: tuple[str, ...],
    start: str,
    end: str,
    data_checksum: str,
    supplemental_contracts: list[ContractInfo] | None = None,
) -> RollMap:
    crosswalk: dict[tuple[str, str], ContractInfo] = {
        (item.root, str(item.instrument_id)): item
        for item in [*base_map.contracts, *(supplemental_contracts or [])]
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
