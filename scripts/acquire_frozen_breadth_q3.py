#!/usr/bin/env python3
"""Estimate or acquire the exact frozen Q3-2025 breadth bundle.

Dry-run is the default.  ``--execute`` downloads exactly YM/MYM/ES/NQ/RTY
one-minute bars plus their explicit definitions, records one spend/access row,
and seals one immutable receipt.  It never opens Q4, a broker, or an order path.
"""

from __future__ import annotations

import argparse
import json
import os
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
from hydra.production.frozen_breadth_continuation import (
    DATASET,
    DATA_ROLE,
    DATA_SCHEMA,
    END,
    ROOTS,
    START,
    SYMBOLS,
    FrozenBreadthContinuationError,
    validate_acquisition_receipt,
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


RECEIPT_SCHEMA = "hydra_frozen_breadth_q3_acquisition_receipt_v1"
PURPOSE = (
    "single frozen breadth qualifier on untouched Q3-2025 final-development; "
    "no recalibration and evidence ceiling Tier G"
)
ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"
DEFAULT_CONTRACT = (
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/"
    "frozen_breadth_q3_contract.json"
)
DEFAULT_RECEIPT = (
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "frozen_breadth_q3_acquisition_receipt.json"
)


class FrozenBreadthAcquisitionError(RuntimeError):
    pass


def estimate_or_acquire(
    *,
    contract: Mapping[str, Any],
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    receipt_path: str | Path = DEFAULT_RECEIPT,
    dbn_store_loader: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    project = Path(root).resolve()
    frozen = _verify_contract_shape(contract)
    request = dict(frozen["data_request"])
    cfg = budget or DatabentoBudgetConfig()
    bundle_id = request_id_for(
        {
            "contract_hash": frozen["contract_hash"],
            "request": _ohlcv_request(request),
            "definitions": "EXPLICIT_Q3_2025_YM_MYM_ES_NQ_RTY",
            "purpose": PURPOSE,
        }
    )
    paths = _paths(project, bundle_id, receipt_path)
    with _optional_lock(paths["lock"], enabled=execute):
        if paths["receipt"].is_file():
            existing = json.loads(paths["receipt"].read_text(encoding="utf-8"))
            validate_acquisition_receipt(frozen, existing)
            _verify_receipt_files(existing)
            return existing

        symbology = _resolve_inputs(client)
        api_requests = {
            "ohlcv": _ohlcv_request(request),
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
            name: float(client.metadata.get_cost(**payload))
            for name, payload in api_requests.items()
        }
        if any(value < 0.0 for value in estimates.values()):
            raise FrozenBreadthAcquisitionError("negative official cost estimate")
        records = {
            name: int(client.metadata.get_record_count(**payload))
            for name, payload in api_requests.items()
        }
        bytes_ = {
            name: int(client.metadata.get_billable_size(**payload))
            for name, payload in api_requests.items()
        }
        total = float(sum(estimates.values()))
        projected, actual_before = enforce_budget(cfg, total)
        plan = {
            "schema": "hydra_frozen_breadth_q3_acquisition_plan_v1",
            "bundle_id": bundle_id,
            "contract_hash": frozen["contract_hash"],
            "request": request,
            "api_requests": api_requests,
            "official_live_estimates_usd": estimates,
            "aggregate_live_estimate_usd": total,
            "estimated_records": records,
            "estimated_billable_bytes": bytes_,
            "cumulative_actual_before_usd": actual_before,
            "projected_cumulative_estimate_usd": projected,
            "data_role": DATA_ROLE,
            "parameters_mutable": False,
            "evidence_ceiling": "TIER_G_DEVELOPMENT",
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "execute": bool(execute),
        }
        if not execute:
            return {**plan, "download_status": "DRY_RUN_ONLY"}

        ohlcv_network = _download_once(
            client,
            api_requests["ohlcv"],
            paths["raw_ohlcv"],
            stype_out="instrument_id",
        )
        definition_network = _download_once(
            client,
            api_requests["definition"],
            paths["raw_definition"],
            stype_out="instrument_id",
        )
        loader = dbn_store_loader or _default_dbn_store_loader
        _persist_json_once(paths["symbology"], symbology)
        definition_frame = _store_frame(
            loader(paths["raw_definition"]), price_type=None, map_symbols=False
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
        ohlcv = _store_frame(
            loader(paths["raw_ohlcv"]), price_type="float", map_symbols=False
        )
        normalized = _normalize(ohlcv, roll_map=roll_map)
        _persist_parquet_once(paths["parquet"], normalized)
        _record_spend_once(
            cfg,
            bundle_id=bundle_id,
            total=total,
            projected=projected,
            actual_before=actual_before,
            result_path=paths["raw_ohlcv"],
        )
        _record_access_once(
            project / ACCESS_LEDGER,
            bundle_id=bundle_id,
            contract_hash=str(frozen["contract_hash"]),
        )
        files = [
            _file_receipt("RAW_DBN_OHLCV", paths["raw_ohlcv"]),
            _file_receipt("RAW_DBN_DEFINITIONS", paths["raw_definition"]),
            _file_receipt("NORMALIZED_PARQUET", paths["parquet"]),
            _file_receipt("EXPLICIT_CONTRACT_MAP", paths["contract_map"]),
            _file_receipt("SYMBOL_RESOLUTION", paths["symbology"]),
        ]
        _estimated_after, actual_after = cumulative_spend(
            project_path(cfg.ledger_path)
        )
        core = {
            "schema": RECEIPT_SCHEMA,
            "bundle_id": bundle_id,
            "created_at_utc": utc_now(),
            "contract_hash": frozen["contract_hash"],
            "request": {
                key: request[key]
                for key in ("dataset", "schema", "symbols", "stype_in", "start", "end")
            },
            "actual_cost_usd": total,
            "cumulative_actual_usd": actual_after,
            "official_cost_breakdown_usd": estimates,
            "official_record_counts": records,
            "official_billable_bytes": bytes_,
            "download_status": "DOWNLOADED",
            "network_data_request_made": bool(ohlcv_network or definition_network),
            "data_role": DATA_ROLE,
            "parameters_mutable": False,
            "evidence_ceiling": "TIER_G_DEVELOPMENT",
            "files": files,
            "feature_build_inputs": {
                "source_files": [
                    {
                        "path": str(paths["parquet"]),
                        "sha256": sha256_file(paths["parquet"]),
                        "rows": int(len(normalized)),
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
        validate_acquisition_receipt(frozen, receipt)
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _verify_contract_shape(contract: Mapping[str, Any]) -> dict[str, Any]:
    frozen = dict(contract)
    claimed = str(frozen.pop("contract_hash", ""))
    if not claimed or stable_hash(frozen) != claimed:
        raise FrozenBreadthAcquisitionError("contract hash drift")
    request = dict(frozen.get("data_request") or {})
    expected = {
        "dataset": DATASET,
        "schema": DATA_SCHEMA,
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "start": START,
        "end": END,
        "data_role": DATA_ROLE,
    }
    if any(request.get(key) != value for key, value in expected.items()):
        raise FrozenBreadthAcquisitionError("frozen Q3 request drift")
    if (
        frozen.get("tier_c_permitted") is not False
        or frozen.get("data_partition", {}).get("candidate_modification_allowed") is not False
        or START >= END
        or END != "2025-10-01"
    ):
        raise FrozenBreadthAcquisitionError("Q3 role/freeze drift")
    return {**frozen, "contract_hash": claimed}


def _resolve_inputs(client: Any) -> dict[str, Any]:
    response = client.symbology.resolve(
        dataset=DATASET,
        symbols=list(SYMBOLS),
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=START,
        end_date=END,
    )
    continuous = {
        str(symbol): [dict(row) for row in rows]
        for symbol, rows in dict(response.get("result") or {}).items()
    }
    if set(continuous) != set(SYMBOLS):
        raise FrozenBreadthAcquisitionError("continuous symbology incomplete")
    ids = sorted(
        {str(row["s"]) for rows in continuous.values() for row in rows}, key=int
    )
    raw_response = client.symbology.resolve(
        dataset=DATASET,
        symbols=ids,
        stype_in="instrument_id",
        stype_out="raw_symbol",
        start_date=START,
        end_date=END,
    )
    raw = {
        str(key): str(rows[0]["s"])
        for key, rows in dict(raw_response.get("result") or {}).items()
        if rows
    }
    if set(raw) != set(ids):
        raise FrozenBreadthAcquisitionError("raw symbology incomplete")
    core = {
        "dataset": DATASET,
        "start": START,
        "end": END,
        "roots": list(ROOTS),
        "continuous_mapping": continuous,
        "instrument_ids": ids,
        "raw_symbol_mapping": raw,
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _ohlcv_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dataset": request["dataset"],
        "schema": request["schema"],
        "symbols": list(request["symbols"]),
        "stype_in": request["stype_in"],
        "start": request["start"],
        "end": request["end"],
    }


def _normalize(frame: pd.DataFrame, *, roll_map: Any) -> pd.DataFrame:
    source = frame.reset_index()
    if "symbol" not in source.columns:
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
        source, symbol=None, timeframe="1m", symbol_map=symbol_map
    )
    timestamps = pd.to_datetime(normalized["timestamp"], utc=True)
    if timestamps.min() < pd.Timestamp(START, tz="UTC") or timestamps.max() >= pd.Timestamp(END, tz="UTC"):
        raise FrozenBreadthAcquisitionError("download escapes frozen Q3 dates")
    if set(normalized["symbol"].astype(str)) != set(ROOTS):
        raise FrozenBreadthAcquisitionError("download roots differ from freeze")
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
) -> None:
    ledger = project_path(cfg.ledger_path)
    rows = [row for row in read_ledger(ledger) if row.get("request_id") == bundle_id]
    if len(rows) > 1:
        raise FrozenBreadthAcquisitionError("duplicate spend rows")
    if rows:
        return
    append_spend_record(
        cfg,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema=f"{DATA_SCHEMA}+definition",
            symbols=list(SYMBOLS),
            stype_in="continuous+instrument_id",
            start=START,
            end=END,
            estimated_cost_usd=total,
            actual_cost_usd=total,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual_before + total,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier="E_DIAGNOSTIC_TO_TIER_G_FINAL_DEVELOPMENT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(result_path),
            checksum=sha256_file(result_path),
            download_status="DOWNLOADED",
        ),
    )


def _record_access_once(
    path: Path, *, bundle_id: str, contract_hash: str
) -> None:
    rows = []
    if path.is_file():
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    if any(bundle_id in set(row.get("candidate_ids") or ()) for row in rows):
        return
    enforce_data_access(
        period=f"{START}:{END}",
        # The governance enum has no FINAL_DEVELOPMENT member.  BLIND_VALIDATION
        # is the correct immutable access mode; the economic result remains
        # explicitly capped at Tier G by the frozen contract.
        role=DataRole.BLIND_VALIDATION,
        requesting_module="scripts.acquire_frozen_breadth_q3",
        candidate_ids=[bundle_id, "breadth:YM:OPEN:BREADTH_CONFIRMED_CONTINUATION"],
        reason=PURPOSE,
        freeze_manifest_hash=contract_hash,
        ledger_path=str(path),
    )


def _paths(root: Path, bundle_id: str, receipt_path: str | Path) -> dict[str, Path]:
    base = root / "data/cache/databento/frozen_breadth_q3" / bundle_id
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


def _verify_receipt_files(receipt: Mapping[str, Any]) -> None:
    for raw in receipt.get("files", ()):
        row = dict(raw)
        path = Path(str(row.get("path") or ""))
        if not path.is_file() or sha256_file(path) != str(row.get("sha256") or ""):
            raise FrozenBreadthAcquisitionError("sealed acquisition file drift")


def _load_contract(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise FrozenBreadthAcquisitionError("contract must be an object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--contract", default=DEFAULT_CONTRACT)
    parser.add_argument("--receipt", default=DEFAULT_RECEIPT)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    contract_path = Path(args.contract)
    if not contract_path.is_absolute():
        contract_path = root / contract_path
    client = _import_databento().Historical(load_api_key())
    result = estimate_or_acquire(
        contract=_load_contract(contract_path),
        root=root,
        client=client,
        execute=bool(args.execute),
        receipt_path=args.receipt,
    )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
