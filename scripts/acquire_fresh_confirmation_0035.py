#!/usr/bin/env python3
from __future__ import annotations

"""One-shot, manifest-bound acquisition for HYDRA 0035 confirmation.

The command is deliberately dry-run unless ``--execute`` is supplied.  It
accepts only the already-frozen YM/MYM/ES 2025 request, re-estimates both bars
and explicit-contract definitions, and records one aggregate spend event and
one immutable blind-validation access event.  Re-running a completed request
only verifies the sealed receipt and cache; it never appends duplicate ledger
rows or downloads the same data again.
"""

import argparse
import fcntl
import json
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
from hydra.data.current_contract_map import build_current_roll_map
from hydra.data.databento_loader import (
    _import_databento,
    load_api_key,
    normalize_ohlcv_frame,
    validate_ohlcv_frame,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.production.fresh_confirmation_lane import (
    CUMULATIVE_HARD_CAP_USD,
    DATASET,
    DATA_ROLE,
    DATA_SCHEMA,
    END,
    FreshConfirmationError,
    START,
    SYMBOLS,
    validate_acquisition_receipt,
)
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


CAMPAIGN_ID = "hydra_autonomous_economic_discovery_director_0035"
CAMPAIGN_MODE = "AUTONOMOUS_ECONOMIC_DISCOVERY_DIRECTOR"
RECEIPT_SCHEMA = "hydra_fresh_confirmation_0035_acquisition_receipt_v1"
REQUEST_PURPOSE = (
    "one-shot frozen Tier-C confirmation for the four causal Tier-G YM books; "
    "2025 data only; parameters immutable"
)
CANDIDATE_TIER = "TIER_G_AWAITING_INDEPENDENT_CONFIRMATION"
ROOTS = ("YM", "MYM", "ES")
Q4_2024_START = date(2024, 10, 1)
Q4_2024_END = date(2025, 1, 1)


class ConfirmationAcquisitionError(RuntimeError):
    """The exact governed acquisition cannot be completed safely."""


def load_frozen_inputs(
    contract_path: str | Path,
    manifest_path: str | Path,
    *,
    expected_manifest_hash: str,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    """Load and hash-check the prewritten contract and current 0035 manifest."""

    manifest_file = Path(manifest_path).resolve()
    manifest = load_and_validate_production_manifest(manifest_file)
    contract_file = Path(contract_path).resolve()
    try:
        contract = json.loads(contract_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfirmationAcquisitionError(
            "frozen fresh-confirmation contract is absent or invalid"
        ) from exc
    if not isinstance(contract, dict):
        raise ConfirmationAcquisitionError("confirmation contract must be a JSON object")
    root = manifest_file.parents[2]
    try:
        contract_file.relative_to(root)
    except ValueError as exc:
        raise ConfirmationAcquisitionError("contract path escapes repository") from exc
    validate_frozen_inputs(
        contract,
        manifest,
        expected_manifest_hash=expected_manifest_hash,
    )
    return root, contract, manifest


def validate_frozen_inputs(
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
    *,
    expected_manifest_hash: str,
) -> dict[str, Any]:
    """Fail closed on any request, budget, role, or manifest drift."""

    frozen = dict(contract)
    claimed_contract_hash = str(frozen.pop("contract_hash", ""))
    if not claimed_contract_hash or stable_hash(frozen) != claimed_contract_hash:
        raise ConfirmationAcquisitionError("confirmation contract hash drift")
    actual_manifest_hash = str(manifest.get("manifest_hash") or "")
    if (
        manifest.get("campaign_id") != CAMPAIGN_ID
        or manifest.get("campaign_mode") != CAMPAIGN_MODE
        or len(actual_manifest_hash) != 64
        or actual_manifest_hash != str(expected_manifest_hash)
    ):
        raise ConfirmationAcquisitionError("current 0035 manifest identity/hash drift")
    payload = dict(manifest)
    payload.pop("manifest_hash", None)
    if stable_hash(payload) != actual_manifest_hash:
        raise ConfirmationAcquisitionError("current 0035 manifest hash does not recompute")

    request = dict(contract.get("data_request") or {})
    expected = {
        "dataset": DATASET,
        "schema": DATA_SCHEMA,
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "data_role": DATA_ROLE,
        "q4_2024_access_allowed": False,
        "broker_or_order_capability": False,
    }
    if any(request.get(key) != value for key, value in expected.items()):
        raise ConfirmationAcquisitionError("frozen confirmation request drift")
    request_core = {key: expected[key] for key in expected}
    if str(request.get("request_hash") or "") != stable_hash(request_core):
        raise ConfirmationAcquisitionError("frozen confirmation request hash drift")
    if float(request.get("cumulative_hard_cap_usd", -1.0)) != CUMULATIVE_HARD_CAP_USD:
        raise ConfirmationAcquisitionError("confirmation cumulative authority drift")
    if float(request.get("additional_authority_usd", -1.0)) != 100.0:
        raise ConfirmationAcquisitionError("confirmation incremental authority drift")
    if _overlaps_q4_2024(START, END):
        raise ConfirmationAcquisitionError("confirmation request overlaps protected Q4")
    if contract.get("status") not in {None, "FROZEN_AWAITING_ACQUISITION"}:
        raise ConfirmationAcquisitionError("confirmation contract is not awaiting acquisition")
    partition = dict(contract.get("data_partition") or {})
    if partition and (
        partition.get("role") != DATA_ROLE
        or partition.get("candidate_modification_allowed") is not False
        or partition.get("recalibration_allowed") is not False
    ):
        raise ConfirmationAcquisitionError("confirmation parameter-freeze contract drift")
    return {
        "contract_hash": claimed_contract_hash,
        "manifest_hash": actual_manifest_hash,
        "request": request,
    }


def acquire_fresh_confirmation(
    *,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
    expected_manifest_hash: str,
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    dbn_store_loader: Callable[[Path], Any] | None = None,
    receipt_path: str | Path | None = None,
) -> dict[str, Any]:
    """Re-estimate and optionally acquire the exact frozen confirmation bundle."""

    project = Path(root).resolve()
    frozen = validate_frozen_inputs(
        contract,
        manifest,
        expected_manifest_hash=expected_manifest_hash,
    )
    cfg = budget or DatabentoBudgetConfig(
        hard_cap_usd=CUMULATIVE_HARD_CAP_USD,
        safety_ceiling_usd=CUMULATIVE_HARD_CAP_USD,
    )
    if (
        float(cfg.hard_cap_usd) != CUMULATIVE_HARD_CAP_USD
        or float(cfg.safety_ceiling_usd) > CUMULATIVE_HARD_CAP_USD
    ):
        raise ConfirmationAcquisitionError("acquisition budget is not bound by the authority")

    bundle_id = request_id_for(
        {
            "contract_hash": frozen["contract_hash"],
            "authorization_manifest_hash": frozen["manifest_hash"],
            "request": _api_bar_request(frozen["request"]),
            "definitions": "EXPLICIT_DATE_AWARE_2025_YM_MYM_ES",
            "purpose": REQUEST_PURPOSE,
        }
    )
    paths = _bundle_paths(project, bundle_id, receipt_path=receipt_path)
    with _optional_lock(paths["lock"], enabled=execute):
        existing = _load_existing_receipt(
            paths["receipt"],
            contract=contract,
            contract_hash=str(frozen["contract_hash"]),
            manifest_hash=str(frozen["manifest_hash"]),
            bundle_id=bundle_id,
            budget=cfg,
            access_ledger=paths["access_ledger"],
        )
        if existing is not None:
            return existing

        symbology = _resolve_explicit_contract_inputs(client)
        requests = {
            "ohlcv": _api_bar_request(frozen["request"]),
            "definition": {
                "dataset": DATASET,
                "schema": "definition",
                "symbols": list(symbology["instrument_ids"]),
                "stype_in": "instrument_id",
                "start": START,
                "end": END,
            },
        }
        estimates = {
            name: float(client.metadata.get_cost(**request))
            for name, request in requests.items()
        }
        if any(value < 0.0 for value in estimates.values()):
            raise ConfirmationAcquisitionError("Databento returned a negative estimate")
        total_estimate = sum(estimates.values())
        projected, actual_before = enforce_budget(cfg, total_estimate)
        if actual_before + total_estimate > CUMULATIVE_HARD_CAP_USD + 1e-12:
            raise ConfirmationAcquisitionError("live estimate exceeds cumulative authority")
        plan = {
            "schema": "hydra_fresh_confirmation_0035_acquisition_plan_v1",
            "bundle_id": bundle_id,
            "contract_hash": frozen["contract_hash"],
            "authorization_manifest_hash": frozen["manifest_hash"],
            "request": dict(frozen["request"]),
            "api_requests": requests,
            "official_live_estimates_usd": estimates,
            "aggregate_live_estimate_usd": total_estimate,
            "cumulative_actual_before_usd": actual_before,
            "projected_cumulative_usd": actual_before + total_estimate,
            "cumulative_hard_cap_usd": CUMULATIVE_HARD_CAP_USD,
            "data_role": DATA_ROLE,
            "access_role": DataRole.BLIND_VALIDATION.value,
            "parameters_mutable": False,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "execute": bool(execute),
        }
        if not execute:
            return {
                **plan,
                "download_status": "DRY_RUN_ONLY",
                "network_data_request_made": False,
            }

        raw_network = _download_once(
            client,
            requests["ohlcv"],
            paths["raw_ohlcv"],
            stype_out="instrument_id",
        )
        definition_network = _download_once(
            client,
            requests["definition"],
            paths["raw_definition"],
            stype_out="instrument_id",
        )
        raw_inventory = [
            {"path": str(paths["raw_ohlcv"]), "sha256": sha256_file(paths["raw_ohlcv"])},
            {
                "path": str(paths["raw_definition"]),
                "sha256": sha256_file(paths["raw_definition"]),
            },
        ]
        _record_spend_once(
            cfg,
            bundle_id=bundle_id,
            estimate=total_estimate,
            projected=projected,
            actual_before=actual_before,
            raw_inventory=raw_inventory,
        )
        candidate_ids = sorted(
            {
                str(row.get("candidate_id"))
                for row in contract.get("tier_g_candidates", ())
                if row.get("candidate_id")
            }
        )
        _record_access_once(
            paths["access_ledger"],
            bundle_id=bundle_id,
            manifest_hash=str(frozen["manifest_hash"]),
            candidate_ids=candidate_ids,
        )

        loader = dbn_store_loader or _default_dbn_store_loader
        _persist_json_once(paths["symbology"], symbology)
        definition_frame = _store_frame(
            loader(paths["raw_definition"]),
            price_type=None,
            map_symbols=False,
        )
        roll_map = build_current_roll_map(
            roots=ROOTS,
            start=START,
            end=END,
            continuous_mapping=symbology["continuous_mapping"],
            raw_symbol_mapping=symbology["raw_symbol_mapping"],
            definition_history=definition_frame,
            dataset=DATASET,
            schema=DATA_SCHEMA,
        )
        _persist_json_once(paths["contract_map"], roll_map.to_dict())

        ohlcv_frame = _store_frame(
            loader(paths["raw_ohlcv"]),
            price_type="float",
            map_symbols=False,
        )
        normalized, validation = _normalize_confirmation_ohlcv(
            ohlcv_frame,
            roll_map=roll_map,
        )
        _persist_parquet_once(paths["parquet"], normalized)
        normalized_hash = sha256_file(paths["parquet"])
        if sha256_file(paths["parquet"]) != normalized_hash:
            raise ConfirmationAcquisitionError("normalized parquet is not stable")

        files = [
            _file_receipt("RAW_DBN_OHLCV", paths["raw_ohlcv"]),
            _file_receipt("RAW_DBN_DEFINITIONS", paths["raw_definition"]),
            _file_receipt("NORMALIZED_PARQUET", paths["parquet"]),
            _file_receipt("EXPLICIT_CONTRACT_MAP", paths["contract_map"]),
            _file_receipt("SYMBOL_RESOLUTION", paths["symbology"]),
        ]
        _estimated_after, actual_after = cumulative_spend(_ledger_path(cfg))
        core = {
            "schema": RECEIPT_SCHEMA,
            "bundle_id": bundle_id,
            "created_at_utc": utc_now(),
            "contract_hash": frozen["contract_hash"],
            "authorization_manifest_hash": frozen["manifest_hash"],
            "request": {
                key: frozen["request"][key]
                for key in ("dataset", "schema", "symbols", "stype_in", "start", "end")
            },
            "actual_cost_usd": total_estimate,
            "cumulative_actual_usd": actual_after,
            "official_cost_breakdown_usd": estimates,
            "download_status": "DOWNLOADED",
            "network_data_request_made": bool(raw_network or definition_network),
            "data_role": DATA_ROLE,
            "access_role": DataRole.BLIND_VALIDATION.value,
            "parameters_mutable": False,
            "files": files,
            "feature_build_inputs": {
                "source_files": [
                    {
                        "path": str(paths["parquet"]),
                        "sha256": normalized_hash,
                        "rows": int(len(normalized)),
                    }
                ],
                "contract_map_path": str(paths["contract_map"]),
                "contract_map_sha256": sha256_file(paths["contract_map"]),
                "cache_root": str(paths["feature_cache"]),
                "request_hash": frozen["request"]["request_hash"],
            },
            "normalization": validation,
            "explicit_contracts": [asdict(row) for row in roll_map.contracts],
            "roll_map_hash": roll_map.roll_map_hash(),
            "spend_ledger_request_id": bundle_id,
            "data_access_marker": bundle_id,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
        receipt = {**core, "receipt_hash": stable_hash(core)}
        # Ensure the public consumer accepts the shape before sealing it.
        validate_acquisition_receipt(contract, receipt)
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _resolve_explicit_contract_inputs(client: Any) -> dict[str, Any]:
    continuous_symbols = list(SYMBOLS)
    continuous_raw = client.symbology.resolve(
        dataset=DATASET,
        symbols=continuous_symbols,
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=START,
        end_date=END,
    )
    continuous = {
        str(symbol): [dict(row) for row in rows]
        for symbol, rows in dict(continuous_raw.get("result") or {}).items()
    }
    if set(continuous) != set(continuous_symbols):
        raise ConfirmationAcquisitionError("continuous symbology resolution is incomplete")
    instrument_ids = sorted(
        {str(row["s"]) for rows in continuous.values() for row in rows},
        key=lambda value: int(value),
    )
    if not instrument_ids:
        raise ConfirmationAcquisitionError("continuous symbology yielded no contracts")
    raw_response = client.symbology.resolve(
        dataset=DATASET,
        symbols=instrument_ids,
        stype_in="instrument_id",
        stype_out="raw_symbol",
        start_date=START,
        end_date=END,
    )
    raw_mapping = {
        str(instrument_id): str(rows[0]["s"])
        for instrument_id, rows in dict(raw_response.get("result") or {}).items()
        if rows
    }
    if set(raw_mapping) != set(instrument_ids):
        raise ConfirmationAcquisitionError("raw-contract symbology is incomplete")
    core = {
        "dataset": DATASET,
        "start": START,
        "end": END,
        "roots": list(ROOTS),
        "continuous_mapping": continuous,
        "instrument_ids": instrument_ids,
        "raw_symbol_mapping": raw_mapping,
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _normalize_confirmation_ohlcv(frame: pd.DataFrame, *, roll_map: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = frame.reset_index()
    if "symbol" not in source.columns:
        if "instrument_id" not in source.columns:
            raise ConfirmationAcquisitionError("raw OHLCV has no instrument identity")
        source = source.rename(columns={"instrument_id": "symbol"})
    symbol_map = {
        **{symbol: symbol.split(".", 1)[0] for symbol in SYMBOLS},
        **{
            str(row.instrument_id): str(row.root)
            for row in roll_map.contracts
            if row.instrument_id is not None
        },
        **{str(row.contract): str(row.root) for row in roll_map.contracts},
    }
    normalized = normalize_ohlcv_frame(
        source,
        symbol=None,
        timeframe="1m",
        symbol_map=symbol_map,
    )
    timestamps = pd.to_datetime(normalized["timestamp"], utc=True)
    if timestamps.min() < pd.Timestamp(START, tz="UTC") or timestamps.max() >= pd.Timestamp(END, tz="UTC"):
        raise ConfirmationAcquisitionError("downloaded bars escape frozen half-open dates")
    observed = set(normalized["symbol"].astype(str))
    if observed != set(ROOTS):
        raise ConfirmationAcquisitionError(
            f"normalized confirmation roots differ from freeze: {sorted(observed)}"
        )
    validation = validate_ohlcv_frame(normalized, timeframe="1m")
    return normalized, validation


def _store_frame(store: Any, *, price_type: str | None, map_symbols: bool) -> pd.DataFrame:
    kwargs: dict[str, Any] = {"pretty_ts": True, "map_symbols": map_symbols}
    if price_type is not None:
        kwargs["price_type"] = price_type
    frame = store.to_df(**kwargs)
    if not isinstance(frame, pd.DataFrame):
        raise ConfirmationAcquisitionError("DBN decoder did not return a DataFrame")
    return frame.reset_index()


def _download_once(
    client: Any,
    request: Mapping[str, Any],
    path: Path,
    *,
    stype_out: str,
) -> bool:
    if path.is_file():
        if path.stat().st_size <= 0:
            raise ConfirmationAcquisitionError(f"empty governed cache file: {path}")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.stem}.{os.getpid()}.tmp.dbn.zst")
    temporary.unlink(missing_ok=True)
    try:
        client.timeseries.get_range(**dict(request), stype_out=stype_out, path=str(temporary))
        if not temporary.is_file() or temporary.stat().st_size <= 0:
            raise ConfirmationAcquisitionError("Databento returned an empty raw file")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return True


def _record_spend_once(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
    projected: float,
    actual_before: float,
    raw_inventory: list[dict[str, str]],
) -> None:
    rows = [row for row in read_ledger(_ledger_path(budget)) if row.get("request_id") == bundle_id]
    if len(rows) > 1:
        raise ConfirmationAcquisitionError("duplicate confirmation spend records")
    if rows:
        row = rows[0]
        if row.get("download_status") != "DOWNLOADED" or abs(float(row.get("actual_cost_usd") or 0.0) - estimate) > 1e-9:
            raise ConfirmationAcquisitionError("existing confirmation spend record drift")
        return
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema=f"{DATA_SCHEMA}+definition",
            symbols=list(SYMBOLS),
            stype_in="continuous+instrument_id",
            start=START,
            end=END,
            estimated_cost_usd=estimate,
            actual_cost_usd=estimate,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual_before + estimate,
            cache_hit=False,
            research_purpose=REQUEST_PURPOSE,
            candidate_tier=CANDIDATE_TIER,
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=raw_inventory[0]["path"],
            checksum=stable_hash(raw_inventory),
            download_status="DOWNLOADED",
        ),
    )


def _record_access_once(
    path: Path,
    *,
    bundle_id: str,
    manifest_hash: str,
    candidate_ids: list[str],
) -> None:
    rows = _jsonl(path)
    matching = [row for row in rows if bundle_id in set(row.get("candidate_ids") or ())]
    if len(matching) > 1:
        raise ConfirmationAcquisitionError("duplicate confirmation data-access records")
    if matching:
        row = matching[0]
        if (
            row.get("data_role") != DataRole.BLIND_VALIDATION.value
            or row.get("parameters_mutable") is not False
            or row.get("freeze_manifest_hash") != manifest_hash
        ):
            raise ConfirmationAcquisitionError("existing confirmation access record drift")
        return
    enforce_data_access(
        period=f"{START}:{END}",
        role=DataRole.BLIND_VALIDATION,
        requesting_module="scripts.acquire_fresh_confirmation_0035",
        candidate_ids=[CAMPAIGN_ID, bundle_id, *candidate_ids],
        reason=REQUEST_PURPOSE,
        freeze_manifest_hash=manifest_hash,
        ledger_path=str(path),
    )


def _load_existing_receipt(
    path: Path,
    *,
    contract: Mapping[str, Any],
    contract_hash: str,
    manifest_hash: str,
    bundle_id: str,
    budget: DatabentoBudgetConfig,
    access_ledger: Path,
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfirmationAcquisitionError("existing confirmation receipt is invalid") from exc
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("bundle_id") != bundle_id
        or receipt.get("contract_hash") != contract_hash
        or receipt.get("authorization_manifest_hash") != manifest_hash
    ):
        raise ConfirmationAcquisitionError("existing confirmation receipt drift")
    for row in receipt.get("files", ()):
        artifact = Path(str(row.get("path") or ""))
        if not artifact.is_file() or sha256_file(artifact) != str(row.get("sha256") or ""):
            raise ConfirmationAcquisitionError("sealed confirmation artifact drift")
    validate_acquisition_receipt(contract, receipt)
    spend = [row for row in read_ledger(_ledger_path(budget)) if row.get("request_id") == bundle_id]
    access = [row for row in _jsonl(access_ledger) if bundle_id in set(row.get("candidate_ids") or ())]
    if len(spend) != 1 or len(access) != 1:
        raise ConfirmationAcquisitionError("sealed receipt ledger cardinality drift")
    if access[0].get("parameters_mutable") is not False:
        raise ConfirmationAcquisitionError("sealed confirmation access became mutable")
    return receipt


def _api_bar_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dataset": request["dataset"],
        "schema": request["schema"],
        "symbols": list(request["symbols"]),
        "stype_in": request["stype_in"],
        "start": request["start"],
        "end": request["end"],
    }


def _bundle_paths(root: Path, bundle_id: str, *, receipt_path: str | Path | None) -> dict[str, Path]:
    base = root / "data/cache/databento/fresh_confirmation_0035" / bundle_id
    output = (
        Path(receipt_path).resolve()
        if receipt_path is not None
        else root
        / "reports/economic_evolution/autonomous_economic_discovery_director_0035_revision_02"
        / "fresh_confirmation_acquisition_receipt.json"
    )
    return {
        "raw_ohlcv": base / "raw_ohlcv.dbn.zst",
        "raw_definition": base / "raw_definitions.dbn.zst",
        "parquet": base / "normalized_ohlcv.parquet",
        "contract_map": base / "explicit_contract_map.json",
        "symbology": base / "symbology_resolution.json",
        "feature_cache": base / "feature_matrices",
        "receipt": output,
        "lock": root / "reports/data_access/fresh_confirmation_0035_acquisition.lock",
        "access_ledger": root / "reports/data_access/data_access_ledger.jsonl",
    }


def _persist_json_once(path: Path, payload: Mapping[str, Any]) -> None:
    content = json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != content:
            raise ConfirmationAcquisitionError(f"refusing divergent cache rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _persist_parquet_once(path: Path, frame: pd.DataFrame) -> None:
    if path.is_file():
        prior = pd.read_parquet(path)
        if stable_hash(_frame_identity(prior)) != stable_hash(_frame_identity(frame)):
            raise ConfirmationAcquisitionError("normalized parquet cache drift")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp.parquet")
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, path)


def _frame_identity(frame: pd.DataFrame) -> dict[str, Any]:
    value = frame.copy()
    if "timestamp" in value:
        value["timestamp"] = pd.to_datetime(value["timestamp"], utc=True).astype(str)
    return {
        "columns": list(value.columns),
        "rows": int(len(value)),
        "hash": stable_hash(value.astype(str).to_dict(orient="records")),
    }


def _file_receipt(kind: str, path: Path) -> dict[str, Any]:
    return {
        "kind": kind,
        "path": str(path),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _default_dbn_store_loader(path: Path) -> Any:
    return _import_databento().DBNStore.from_file(path)


def _ledger_path(budget: DatabentoBudgetConfig) -> Path:
    path = Path(budget.ledger_path)
    return path if path.is_absolute() else project_path(str(path))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _overlaps_q4_2024(start: str, end: str) -> bool:
    return date.fromisoformat(start) < Q4_2024_END and date.fromisoformat(end) > Q4_2024_START


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ConfirmationAcquisitionError("another confirmation acquisition holds the lock") from exc
        yield
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _optional_lock(path: Path, *, enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return
    with _exclusive_lock(path):
        yield


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Acquire the exact frozen HYDRA 0035 confirmation bundle"
    )
    parser.add_argument("--contract", required=True)
    parser.add_argument(
        "--manifest",
        default="config/v7/autonomous_economic_discovery_director_0035.json",
    )
    parser.add_argument("--manifest-hash", required=True)
    parser.add_argument("--receipt")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform the bounded purchase; default only re-estimates official costs",
    )
    args = parser.parse_args()
    root, contract, manifest = load_frozen_inputs(
        args.contract,
        args.manifest,
        expected_manifest_hash=args.manifest_hash,
    )
    key = load_api_key()
    if not key:
        raise ConfirmationAcquisitionError("DATABENTO_API_KEY is required for live cost re-estimation")
    client = _import_databento().Historical(key)
    result = acquire_fresh_confirmation(
        contract=contract,
        manifest=manifest,
        expected_manifest_hash=args.manifest_hash,
        root=root,
        client=client,
        execute=args.execute,
        budget=DatabentoBudgetConfig(
            hard_cap_usd=CUMULATIVE_HARD_CAP_USD,
            safety_ceiling_usd=CUMULATIVE_HARD_CAP_USD,
        ),
        receipt_path=args.receipt,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
