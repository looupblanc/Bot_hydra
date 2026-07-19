#!/usr/bin/env python3
"""Dry-run-first acquisition for the frozen M2K+MYM 2022 replication.

No network data are downloaded unless ``--execute`` is explicit.  The live
Databento estimate is repeated immediately before any download and the request
fails closed above the USD 7 decision-card ceiling or the mission budget.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Mapping

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
from hydra.production.session_safe_m2k_mym_confirmation import (
    DEFAULT_CARD,
    REQUIRED_ROOTS,
    load_decision_card,
    load_decision_card_from_mapping,
)
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access
from scripts.acquire_fresh_confirmation_0035 import (
    _default_dbn_store_loader,
    _download_once,
    _file_receipt,
    _optional_lock,
    _persist_json_once,
    _persist_parquet_once,
    _store_frame,
)


RECEIPT_SCHEMA = "hydra_session_safe_m2k_mym_confirmation_acquisition_v1"
PURPOSE = (
    "one-shot untouched-2022 replication of the frozen session-safe M2K+MYM "
    "50K book; no recalibration"
)
DEFAULT_RECEIPT = Path(
    "reports/data_access/session_safe_m2k_mym_confirmation_acquisition_receipt.json"
)


class SessionSafeAcquisitionError(RuntimeError):
    pass


def estimate_or_acquire(
    *,
    card: Mapping[str, Any],
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    receipt_path: str | Path = DEFAULT_RECEIPT,
    dbn_store_loader: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    project = Path(root).resolve()
    frozen = load_decision_card_from_mapping(card)
    request = dict(frozen["data_request"])
    cfg = budget or DatabentoBudgetConfig()
    symbology = _resolve_inputs(client, request)
    api_requests = {
        "ohlcv": _ohlcv_request(request),
        "definition": {
            "dataset": request["dataset"],
            "schema": "definition",
            "symbols": list(symbology["instrument_ids"]),
            "stype_in": "instrument_id",
            "start": request["request_start_inclusive"],
            "end": request["end_exclusive"],
        },
    }
    bundle_id = request_id_for(
        {
            "decision_card_hash": frozen["card_hash"],
            "api_requests": api_requests,
            "purpose": PURPOSE,
        }
    )
    paths = _paths(project, bundle_id, receipt_path)
    with _optional_lock(paths["lock"], enabled=execute):
        if paths["receipt"].is_file():
            existing = json.loads(paths["receipt"].read_text(encoding="utf-8"))
            _verify_existing(existing, frozen, paths)
            return existing
        _assert_period_still_untouched(project, request)
        estimates = {
            name: float(client.metadata.get_cost(**payload))
            for name, payload in api_requests.items()
        }
        records = {
            name: int(client.metadata.get_record_count(**payload))
            for name, payload in api_requests.items()
        }
        bytes_ = {
            name: int(client.metadata.get_billable_size(**payload))
            for name, payload in api_requests.items()
        }
        total = float(sum(estimates.values()))
        maximum = float(frozen["official_cost_estimate"]["maximum_authorized_by_this_card_usd"])
        if total < 0.0 or total > maximum + 1e-12:
            raise SessionSafeAcquisitionError("live estimate exceeds frozen branch ceiling")
        projected, actual_before = enforce_budget(cfg, total)
        plan = {
            "schema": "hydra_session_safe_m2k_mym_confirmation_acquisition_plan_v1",
            "bundle_id": bundle_id,
            "decision_card_hash": frozen["card_hash"],
            "request": {
                "dataset": request["dataset"],
                "schema": request["schema"],
                "symbols": list(request["symbols"]),
                "stype_in": request["stype_in"],
                "start": request["request_start_inclusive"],
                "end": request["end_exclusive"],
            },
            "api_requests": api_requests,
            "official_live_estimates_usd": estimates,
            "aggregate_live_estimate_usd": total,
            "official_record_counts": records,
            "official_billable_bytes": bytes_,
            "symbology": symbology,
            "cumulative_actual_before_usd": actual_before,
            "projected_cumulative_estimate_usd": projected,
            "branch_cost_ceiling_usd": maximum,
            "data_role": "CONFIRMATION",
            "parameters_mutable": False,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "execute": bool(execute),
        }
        if not execute:
            return {**plan, "download_status": "DRY_RUN_ONLY"}

        ohlcv_network = _download_once(
            client, api_requests["ohlcv"], paths["raw_ohlcv"], stype_out="instrument_id"
        )
        definition_network = _download_once(
            client,
            api_requests["definition"],
            paths["raw_definition"],
            stype_out="instrument_id",
        )
        loader = dbn_store_loader or _default_dbn_store_loader
        _persist_json_once(paths["symbology"], symbology)
        definition = _store_frame(
            loader(paths["raw_definition"]), price_type=None, map_symbols=False
        )
        roll_map = build_current_roll_map(
            roots=REQUIRED_ROOTS,
            start=request["request_start_inclusive"],
            end=request["end_exclusive"],
            continuous_mapping=symbology["continuous_mapping"],
            raw_symbol_mapping=symbology["raw_symbol_mapping"],
            definition_history=definition,
            dataset=request["dataset"],
            schema=request["schema"],
        )
        _persist_json_once(paths["contract_map"], roll_map.to_dict())
        frame = _store_frame(loader(paths["raw_ohlcv"]), price_type="float", map_symbols=False)
        normalized = _normalize(frame, request=request, roll_map=roll_map)
        _persist_parquet_once(paths["parquet"], normalized)
        _record_spend_once(
            cfg,
            bundle_id=bundle_id,
            total=total,
            projected=projected,
            actual_before=actual_before,
            result_path=paths["raw_ohlcv"],
            request=request,
        )
        _record_access_once(
            project / "reports/data_access/data_access_ledger.jsonl",
            bundle_id=bundle_id,
            card_hash=str(frozen["card_hash"]),
            request=request,
        )
        files = [
            _file_receipt("RAW_DBN_OHLCV", paths["raw_ohlcv"]),
            _file_receipt("RAW_DBN_DEFINITIONS", paths["raw_definition"]),
            _file_receipt("NORMALIZED_PARQUET", paths["parquet"]),
            _file_receipt("EXPLICIT_CONTRACT_MAP", paths["contract_map"]),
            _file_receipt("SYMBOL_RESOLUTION", paths["symbology"]),
        ]
        _estimated_after, actual_after = cumulative_spend(project_path(cfg.ledger_path))
        core = {
            "schema": RECEIPT_SCHEMA,
            "bundle_id": bundle_id,
            "created_at_utc": utc_now(),
            "decision_card_hash": frozen["card_hash"],
            "request": plan["request"],
            "actual_cost_usd": total,
            "cumulative_actual_usd": actual_after,
            "official_cost_breakdown_usd": estimates,
            "official_record_counts": records,
            "official_billable_bytes": bytes_,
            "download_status": "DOWNLOADED",
            "network_data_request_made": bool(ohlcv_network or definition_network),
            "data_role": "CONFIRMATION",
            "parameters_mutable": False,
            "files": files,
            "feature_build_inputs": {
                "source_files": [
                    {
                        "path": str(paths["parquet"]),
                        "sha256": sha256_file(paths["parquet"]),
                        "rows": len(normalized),
                    }
                ],
                "contract_map_path": str(paths["contract_map"]),
                "contract_map_sha256": sha256_file(paths["contract_map"]),
                "cache_root": str(paths["feature_cache"]),
            },
            "explicit_contracts": [asdict(row) for row in roll_map.contracts],
            "roll_map_hash": roll_map.roll_map_hash(),
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
        receipt = {**core, "receipt_hash": stable_hash(core)}
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _resolve_inputs(client: Any, request: Mapping[str, Any]) -> dict[str, Any]:
    response = client.symbology.resolve(
        dataset=request["dataset"],
        symbols=list(request["symbols"]),
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=request["request_start_inclusive"],
        end_date=request["end_exclusive"],
    )
    payload = response.to_dict() if hasattr(response, "to_dict") else response
    continuous = {
        str(symbol): [dict(row) for row in rows]
        for symbol, rows in dict(payload.get("result") or {}).items()
    }
    if set(continuous) != set(request["symbols"]):
        raise SessionSafeAcquisitionError("continuous symbology incomplete")
    ids = sorted({str(row["s"]) for rows in continuous.values() for row in rows}, key=int)
    response = client.symbology.resolve(
        dataset=request["dataset"],
        symbols=ids,
        stype_in="instrument_id",
        stype_out="raw_symbol",
        start_date=request["request_start_inclusive"],
        end_date=request["end_exclusive"],
    )
    payload = response.to_dict() if hasattr(response, "to_dict") else response
    # Instrument IDs can be reused across unrelated instruments.  Selecting
    # the first raw-symbol interval is therefore unsafe (e.g. a 2022 futures
    # ID may have mapped to an option earlier in the requested year).  Bind
    # each ID at the start of the exact interval in which the continuous
    # mapping uses it.
    use_start: dict[str, str] = {}
    for rows in continuous.values():
        for row in rows:
            instrument_id = str(row["s"])
            start = str(row["d0"])
            use_start[instrument_id] = min(start, use_start.get(instrument_id, start))
    raw: dict[str, str] = {}
    raw_evidence: dict[str, dict[str, str]] = {}
    for key, rows in dict(payload.get("result") or {}).items():
        instrument_id = str(key)
        target = use_start.get(instrument_id)
        matches = [
            dict(row)
            for row in rows
            if target is not None and str(row["d0"]) <= target < str(row["d1"])
        ]
        if len(matches) == 1:
            raw[instrument_id] = str(matches[0]["s"])
            raw_evidence[instrument_id] = {
                "continuous_use_start": str(target),
                "raw_interval_start": str(matches[0]["d0"]),
                "raw_interval_end": str(matches[0]["d1"]),
                "raw_symbol": str(matches[0]["s"]),
            }
    if set(raw) != set(ids):
        raise SessionSafeAcquisitionError("raw-symbol resolution incomplete")
    core = {
        "dataset": request["dataset"],
        "start": request["request_start_inclusive"],
        "end": request["end_exclusive"],
        "roots": list(REQUIRED_ROOTS),
        "continuous_mapping": continuous,
        "instrument_ids": ids,
        "raw_symbol_mapping": raw,
        "raw_symbol_mapping_evidence": raw_evidence,
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _ohlcv_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dataset": request["dataset"],
        "schema": request["schema"],
        "symbols": list(request["symbols"]),
        "stype_in": request["stype_in"],
        "start": request["request_start_inclusive"],
        "end": request["end_exclusive"],
    }


def _normalize(frame: pd.DataFrame, *, request: Mapping[str, Any], roll_map: Any) -> pd.DataFrame:
    source = frame.reset_index()
    if "symbol" not in source.columns:
        source = source.rename(columns={"instrument_id": "symbol"})
    symbol_map = {
        **{symbol: symbol.split(".", 1)[0] for symbol in request["symbols"]},
        **{
            str(row.instrument_id): str(row.root)
            for row in roll_map.contracts
            if row.instrument_id is not None
        },
        **{str(row.contract): str(row.root) for row in roll_map.contracts},
    }
    normalized = normalize_ohlcv_frame(
        source, symbol=None, timeframe="1m", symbol_map=symbol_map
    )
    times = pd.to_datetime(normalized["timestamp"], utc=True)
    if (
        times.min() < pd.Timestamp(request["request_start_inclusive"], tz="UTC")
        or times.max() >= pd.Timestamp(request["end_exclusive"], tz="UTC")
        or set(normalized["symbol"].astype(str)) != set(REQUIRED_ROOTS)
    ):
        raise SessionSafeAcquisitionError("download escapes frozen roots/dates")
    validate_ohlcv_frame(normalized, timeframe="1m")
    return normalized


def _record_spend_once(
    cfg: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    total: float,
    projected: float,
    actual_before: float,
    result_path: Path,
    request: Mapping[str, Any],
) -> None:
    rows = [row for row in read_ledger(project_path(cfg.ledger_path)) if row.get("request_id") == bundle_id]
    if len(rows) > 1:
        raise SessionSafeAcquisitionError("duplicate spend rows")
    if rows:
        return
    append_spend_record(
        cfg,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=request["dataset"],
            schema=f"{request['schema']}+definition",
            symbols=list(request["symbols"]),
            stype_in="continuous+instrument_id",
            start=request["request_start_inclusive"],
            end=request["end_exclusive"],
            estimated_cost_usd=total,
            actual_cost_usd=total,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual_before + total,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier="TIER_E_FROZEN_FRESH_REPLICATION",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(result_path),
            checksum=sha256_file(result_path),
            download_status="DOWNLOADED",
        ),
    )


def _assert_period_still_untouched(project: Path, request: Mapping[str, Any]) -> None:
    """Fail closed if any later process opened any part of the frozen block."""

    ledger = project / "reports/data_access/data_access_ledger.jsonl"
    if not ledger.is_file():
        return
    target_start = str(request["request_start_inclusive"])
    target_end = str(request["end_exclusive"])
    overlaps: list[str] = []
    for line in ledger.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        row = json.loads(line)
        period = str(row.get("period_accessed") or "")
        if ":" not in period:
            continue
        start, end = period.split(":", 1)
        if start < target_end and end > target_start:
            overlaps.append(period)
    if overlaps:
        raise SessionSafeAcquisitionError(
            f"frozen confirmation period is no longer untouched: {sorted(set(overlaps))}"
        )


def _record_access_once(
    path: Path,
    *,
    bundle_id: str,
    card_hash: str,
    request: Mapping[str, Any],
) -> None:
    rows = []
    if path.is_file():
        rows = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x]
    if any(bundle_id in set(row.get("candidate_ids") or ()) for row in rows):
        return
    enforce_data_access(
        period=f"{request['request_start_inclusive']}:{request['end_exclusive']}",
        role=DataRole.BLIND_VALIDATION,
        requesting_module="scripts.acquire_session_safe_m2k_mym_confirmation",
        candidate_ids=[bundle_id, "session_safe_477b:M2K+MYM"],
        reason=PURPOSE,
        freeze_manifest_hash=card_hash,
        ledger_path=str(path),
    )


def _paths(root: Path, bundle_id: str, receipt_path: str | Path) -> dict[str, Path]:
    base = root / "data/cache/databento/session_safe_m2k_mym_confirmation" / bundle_id
    receipt = Path(receipt_path)
    if not receipt.is_absolute():
        receipt = root / receipt
    return {
        "raw_ohlcv": base / "raw_ohlcv.dbn.zst",
        "raw_definition": base / "raw_definitions.dbn.zst",
        "parquet": base / "normalized_ohlcv.parquet",
        "contract_map": base / "explicit_contract_map.json",
        "symbology": base / "symbology_resolution.json",
        "feature_cache": base / "feature_matrices",
        "lock": base / ".acquisition.lock",
        "receipt": receipt.resolve(),
    }


def _verify_existing(
    receipt: Mapping[str, Any], card: Mapping[str, Any], paths: Mapping[str, Path]
) -> None:
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        not claimed
        or stable_hash(core) != claimed
        or receipt.get("decision_card_hash") != card["card_hash"]
        or receipt.get("bundle_id") != paths["raw_ohlcv"].parent.name
    ):
        raise SessionSafeAcquisitionError("sealed acquisition receipt drift")
    for raw in receipt.get("files", ()):
        row = dict(raw)
        path = Path(str(row.get("path") or ""))
        if not path.is_file() or sha256_file(path) != row.get("sha256"):
            raise SessionSafeAcquisitionError("sealed acquisition file drift")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default=str(DEFAULT_CARD))
    parser.add_argument("--receipt", default=str(DEFAULT_RECEIPT))
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    card_path = Path(args.card)
    if not card_path.is_absolute():
        card_path = root / card_path
    card = load_decision_card(card_path)
    client = _import_databento().Historical(load_api_key())
    result = estimate_or_acquire(
        card=card,
        root=root,
        client=client,
        execute=bool(args.execute),
        receipt_path=args.receipt,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
