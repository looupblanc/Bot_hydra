#!/usr/bin/env python3
from __future__ import annotations

"""Acquire the frozen 2026 Tier-Q graduation/confirmation bundle once.

The default command is metadata-only.  ``--execute`` is the only path which
downloads data or appends the existing spend/access ledgers.  The two temporal
roles are fixed in the contract before any 2026 candidate outcome is opened.
"""

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
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
from hydra.data.databento_loader import load_api_key, normalize_ohlcv_frame, validate_ohlcv_frame
from hydra.economic_evolution.schema import stable_hash
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import (
    DataAccessRecord,
    append_access_record,
    current_commit,
    enforce_data_access,
)
from scripts.acquire_fresh_confirmation_0035 import (
    _default_dbn_store_loader,
    _download_once,
    _file_receipt,
    _optional_lock,
    _persist_json_once,
    _persist_parquet_once,
    _store_frame,
)


CAMPAIGN_ID = "hydra_autonomous_economic_discovery_director_0035"
CAMPAIGN_MODE = "AUTONOMOUS_ECONOMIC_DISCOVERY_DIRECTOR"
CONTRACT_SCHEMA = "hydra_tier_q_2026_two_stage_contract_v1"
RECEIPT_SCHEMA = "hydra_tier_q_2026_acquisition_receipt_v1"
DATASET = "GLBX.MDP3"
DATA_SCHEMA = "ohlcv-1m"
START = "2026-01-01"
SPLIT = "2026-05-01"
END = "2026-07-19"
SYMBOLS = (
    "RTY.c.0", "M2K.c.0", "NQ.c.0", "MNQ.c.0", "YM.c.0",
    "MYM.c.0", "ES.c.0", "MES.c.0", "CL.c.0", "MCL.c.0",
)
ROOTS = tuple(symbol.split(".", 1)[0] for symbol in SYMBOLS)
CANDIDATE_IDS = (
    "hazard_2641d5adb7bfee8dca07de2a",
    "hazard_16a744e747cafb88a7e2c83b",
    "hazard_0a569f580a2540474116636c",
    "hazard_10ffb41856432af08259e32b",
    "hazard_16f0da561bc98f2eb7d2efc4",
)
RULE_SNAPSHOT_HASH = "31c322ae58ea51ea0cc87c4714e09d989038aad0f44b60fd53799fbe10873e6f"
REQUEST_PURPOSE = (
    "one-shot immutable 2026 final-development then independent confirmation "
    "for five behaviorally distinct Tier-Q candidates"
)


class TierQ2026AcquisitionError(RuntimeError):
    pass


def load_frozen_inputs(
    contract_path: str | Path,
    manifest_path: str | Path,
    *,
    expected_manifest_hash: str,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    manifest_file = Path(manifest_path).resolve()
    manifest = load_and_validate_production_manifest(manifest_file)
    contract_file = Path(contract_path).resolve()
    try:
        contract = json.loads(contract_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TierQ2026AcquisitionError("frozen 2026 contract is absent or invalid") from exc
    root = manifest_file.parents[2]
    try:
        contract_file.relative_to(root)
    except ValueError as exc:
        raise TierQ2026AcquisitionError("contract path escapes repository") from exc
    validate_frozen_inputs(contract, manifest, expected_manifest_hash=expected_manifest_hash)
    return root, contract, manifest


def validate_frozen_inputs(
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
    *,
    expected_manifest_hash: str,
) -> dict[str, Any]:
    core = dict(contract)
    claimed = str(core.pop("contract_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise TierQ2026AcquisitionError("2026 contract hash drift")
    manifest_hash = str(manifest.get("manifest_hash") or "")
    manifest_core = dict(manifest)
    manifest_core.pop("manifest_hash", None)
    if (
        manifest.get("campaign_id") != CAMPAIGN_ID
        or manifest.get("campaign_mode") != CAMPAIGN_MODE
        or manifest_hash != expected_manifest_hash
        or stable_hash(manifest_core) != manifest_hash
        or contract.get("source_manifest_hash") != manifest_hash
    ):
        raise TierQ2026AcquisitionError("0035 manifest identity/hash drift")
    if contract.get("schema") != CONTRACT_SCHEMA or contract.get("status") != "FROZEN_AWAITING_ACQUISITION":
        raise TierQ2026AcquisitionError("2026 contract status/schema drift")
    if contract.get("official_rule_snapshot_hash") != RULE_SNAPSHOT_HASH:
        raise TierQ2026AcquisitionError("rule snapshot hash drift")

    cohort = list(contract.get("candidate_cohort") or ())
    if [row.get("candidate_id") for row in cohort] != list(CANDIDATE_IDS):
        raise TierQ2026AcquisitionError("candidate cohort drift")
    if any(row.get("prior_evidence_tier") != "Q" for row in cohort):
        raise TierQ2026AcquisitionError("cohort may not inherit a higher tier")
    for row in cohort:
        specification = dict(row.get("frozen_candidate_specification") or {})
        profile = dict(row.get("frozen_account_profile") or {})
        if (
            stable_hash(specification) != row.get("frozen_candidate_specification_hash")
            or row.get("frozen_candidate_specification_hash") != row.get("candidate_fingerprint")
            or stable_hash(profile) != row.get("frozen_account_profile_hash")
            or profile.get("selected_cell_hash") != row.get("selected_cell_hash")
            or profile.get("official_rule_snapshot_hash") != RULE_SNAPSHOT_HASH
            or len(str(row.get("calibration_hash") or "")) != 64
            or len(str(row.get("development_evidence_hash") or "")) != 64
        ):
            raise TierQ2026AcquisitionError("candidate freeze binding drift")

    request = dict(contract.get("data_request") or {})
    expected_request = {
        "dataset": DATASET,
        "schema": DATA_SCHEMA,
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "q4_access_allowed": False,
        "broker_or_order_capability": False,
    }
    if any(request.get(key) != value for key, value in expected_request.items()):
        raise TierQ2026AcquisitionError("frozen 2026 request drift")
    if request.get("request_hash") != stable_hash(expected_request):
        raise TierQ2026AcquisitionError("frozen 2026 request hash drift")

    partitions = list(contract.get("temporal_roles") or ())
    expected_roles = [
        ("FINAL_DEVELOPMENT", START, SPLIT, False, "OPEN_AFTER_ACQUISITION"),
        ("CONFIRMATION", SPLIT, END, False, "SEALED_UNTIL_TIER_G_GATE"),
    ]
    observed_roles = [
        (row.get("role"), row.get("start"), row.get("end"), row.get("retuning_allowed"), row.get("access_state"))
        for row in partitions
    ]
    if observed_roles != expected_roles:
        raise TierQ2026AcquisitionError("temporal-role freeze drift")
    if contract.get("promotion_order") != ["Q", "G", "C"]:
        raise TierQ2026AcquisitionError("evidence-tier order drift")

    budget = dict(contract.get("budget_binding") or {})
    if float(budget.get("cumulative_hard_cap_usd", -1)) != DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD:
        raise TierQ2026AcquisitionError("cumulative budget authority drift")
    if float(budget.get("maximum_live_estimate_usd", -1)) <= 0:
        raise TierQ2026AcquisitionError("live-estimate ceiling is absent")
    if contract.get("outcome_accessed_at_freeze") is not False:
        raise TierQ2026AcquisitionError("2026 outcomes must remain unopened at freeze")
    return {"contract_hash": claimed, "manifest_hash": manifest_hash, "request": request}


def acquire_tier_q_2026(
    *,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
    expected_manifest_hash: str,
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    dbn_store_loader: Any | None = None,
    receipt_path: str | Path | None = None,
) -> dict[str, Any]:
    project = Path(root).resolve()
    frozen = validate_frozen_inputs(contract, manifest, expected_manifest_hash=expected_manifest_hash)
    cfg = budget or DatabentoBudgetConfig()
    if cfg.hard_cap_usd != DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD or cfg.safety_ceiling_usd > cfg.hard_cap_usd:
        raise TierQ2026AcquisitionError("acquisition budget is not bound by authority")
    bundle_id = request_id_for({
        "contract_hash": frozen["contract_hash"],
        "manifest_hash": frozen["manifest_hash"],
        "request": _bar_request(frozen["request"]),
        "definitions": "EXPLICIT_DATE_AWARE_2026_TEN_ROOTS",
    })
    paths = _bundle_paths(project, bundle_id, receipt_path=receipt_path)
    with _optional_lock(paths["lock"], enabled=execute):
        existing = _load_existing_receipt(
            paths["receipt"], contract_hash=frozen["contract_hash"],
            manifest_hash=frozen["manifest_hash"], bundle_id=bundle_id,
            budget=cfg, access_ledger=paths["access_ledger"],
        )
        if existing is not None:
            return existing
        symbology = _resolve_explicit_contract_inputs(client)
        requests = {
            "ohlcv-1m": _bar_request(frozen["request"]),
            "definition": {
                "dataset": DATASET, "schema": "definition",
                "symbols": list(symbology["instrument_ids"]),
                "stype_in": "instrument_id", "start": START, "end": END,
            },
        }
        stats = {name: _metadata_stats(client.metadata, request) for name, request in requests.items()}
        estimate = sum(row["estimated_cost_usd"] for row in stats.values())
        if estimate > float(contract["budget_binding"]["maximum_live_estimate_usd"]) + 1e-12:
            raise TierQ2026AcquisitionError("live estimate exceeds frozen package ceiling")
        projected, actual_before = enforce_budget(cfg, estimate)
        plan = {
            "schema": "hydra_tier_q_2026_acquisition_plan_v1",
            "bundle_id": bundle_id,
            "contract_hash": frozen["contract_hash"],
            "authorization_manifest_hash": frozen["manifest_hash"],
            "api_requests": requests,
            "official_live_metadata": stats,
            "aggregate_live_estimate_usd": estimate,
            "aggregate_record_count": sum(row["record_count"] for row in stats.values()),
            "aggregate_billable_size_bytes": sum(row["billable_size_bytes"] for row in stats.values()),
            "cumulative_actual_before_usd": actual_before,
            "projected_cumulative_usd": actual_before + estimate,
            "cumulative_hard_cap_usd": cfg.hard_cap_usd,
            "temporal_roles": list(contract["temporal_roles"]),
            "parameters_mutable": False,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "execute": bool(execute),
        }
        if not execute:
            return {**plan, "download_status": "DRY_RUN_ONLY", "network_data_request_made": False}

        raw_network = _download_once(client, requests["ohlcv-1m"], paths["raw_ohlcv"], stype_out="instrument_id")
        def_network = _download_once(client, requests["definition"], paths["raw_definition"], stype_out="instrument_id")
        loader = dbn_store_loader or _default_dbn_store_loader
        _persist_json_once(paths["symbology"], symbology)
        definitions = _store_frame(loader(paths["raw_definition"]), price_type=None, map_symbols=False)
        roll_map = build_current_roll_map(
            roots=ROOTS, start=START, end=END,
            continuous_mapping=symbology["continuous_mapping"],
            raw_symbol_mapping=symbology["raw_symbol_mapping"],
            definition_history=definitions, dataset=DATASET, schema=DATA_SCHEMA,
        )
        _persist_json_once(paths["contract_map"], roll_map.to_dict())
        raw_frame = _store_frame(loader(paths["raw_ohlcv"]), price_type="float", map_symbols=False)
        normalized, validation = _normalize(raw_frame, roll_map=roll_map)
        timestamps = pd.to_datetime(normalized["timestamp"], utc=True)
        final_development = normalized.loc[timestamps < pd.Timestamp(SPLIT, tz="UTC")].copy()
        confirmation = normalized.loc[timestamps >= pd.Timestamp(SPLIT, tz="UTC")].copy()
        if final_development.empty or confirmation.empty:
            raise TierQ2026AcquisitionError("a frozen temporal partition is empty")
        _persist_parquet_once(paths["final_development"], final_development)
        _persist_parquet_once(paths["confirmation"], confirmation)
        _record_spend_once(cfg, bundle_id=bundle_id, estimate=estimate, projected=projected,
                           actual_before=actual_before, raw_path=paths["raw_ohlcv"])
        _record_access_once(paths["access_ledger"], bundle_id=bundle_id,
                            manifest_hash=frozen["manifest_hash"])
        files = [
            _file_receipt("RAW_DBN_OHLCV", paths["raw_ohlcv"]),
            _file_receipt("RAW_DBN_DEFINITIONS", paths["raw_definition"]),
            _file_receipt("FINAL_DEVELOPMENT_PARQUET", paths["final_development"]),
            _file_receipt("SEALED_CONFIRMATION_PARQUET", paths["confirmation"]),
            _file_receipt("EXPLICIT_CONTRACT_MAP", paths["contract_map"]),
            _file_receipt("SYMBOL_RESOLUTION", paths["symbology"]),
        ]
        _estimated_after, actual_after = cumulative_spend(_ledger_path(cfg))
        receipt_core = {
            **{key: plan[key] for key in plan if key != "execute"},
            "schema": RECEIPT_SCHEMA,
            "created_at_utc": utc_now(),
            "actual_cost_usd": estimate,
            "cumulative_actual_usd": actual_after,
            "download_status": "DOWNLOADED",
            "network_data_request_made": bool(raw_network or def_network),
            "files": files,
            "normalization": validation,
            "explicit_contracts": [asdict(row) for row in roll_map.contracts],
            "roll_map_hash": roll_map.roll_map_hash(),
            "partition_state": {
                "FINAL_DEVELOPMENT": "AVAILABLE_IMMUTABLE",
                "CONFIRMATION": "SEALED_UNTIL_TIER_G_GATE",
            },
            "outcome_evaluation_performed": False,
        }
        receipt = {**receipt_core, "receipt_hash": stable_hash(receipt_core)}
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _metadata_stats(metadata: Any, request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "estimated_cost_usd": float(metadata.get_cost(**dict(request))),
        "record_count": int(metadata.get_record_count(**dict(request))),
        "billable_size_bytes": int(metadata.get_billable_size(**dict(request))),
    }


def _resolve_explicit_contract_inputs(client: Any) -> dict[str, Any]:
    response = client.symbology.resolve(
        dataset=DATASET, symbols=list(SYMBOLS), stype_in="continuous",
        stype_out="instrument_id", start_date=START, end_date=END,
    )
    continuous = {str(k): [dict(row) for row in v] for k, v in dict(response.get("result") or {}).items()}
    if set(continuous) != set(SYMBOLS):
        raise TierQ2026AcquisitionError("continuous symbology resolution is incomplete")
    instrument_ids = sorted({str(row["s"]) for rows in continuous.values() for row in rows}, key=int)
    raw = client.symbology.resolve(
        dataset=DATASET, symbols=instrument_ids, stype_in="instrument_id",
        stype_out="raw_symbol", start_date=START, end_date=END,
    )
    raw_mapping = {str(k): str(v[0]["s"]) for k, v in dict(raw.get("result") or {}).items() if v}
    if set(raw_mapping) != set(instrument_ids):
        raise TierQ2026AcquisitionError("raw-contract symbology is incomplete")
    core = {
        "dataset": DATASET, "start": START, "end": END, "roots": list(ROOTS),
        "continuous_mapping": continuous, "instrument_ids": instrument_ids,
        "raw_symbol_mapping": raw_mapping,
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _normalize(frame: pd.DataFrame, *, roll_map: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = frame.reset_index()
    if "symbol" not in source.columns:
        if "instrument_id" not in source.columns:
            raise TierQ2026AcquisitionError("raw OHLCV has no instrument identity")
        source = source.rename(columns={"instrument_id": "symbol"})
    symbol_map = {
        **{symbol: symbol.split(".", 1)[0] for symbol in SYMBOLS},
        **{str(row.instrument_id): str(row.root) for row in roll_map.contracts if row.instrument_id is not None},
        **{str(row.contract): str(row.root) for row in roll_map.contracts},
    }
    normalized = normalize_ohlcv_frame(source, symbol=None, timeframe="1m", symbol_map=symbol_map)
    timestamps = pd.to_datetime(normalized["timestamp"], utc=True)
    if timestamps.min() < pd.Timestamp(START, tz="UTC") or timestamps.max() >= pd.Timestamp(END, tz="UTC"):
        raise TierQ2026AcquisitionError("downloaded bars escape frozen dates")
    if set(normalized["symbol"].astype(str)) != set(ROOTS):
        raise TierQ2026AcquisitionError("normalized roots differ from frozen inventory")
    return normalized, validate_ohlcv_frame(normalized, timeframe="1m")


def _bar_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return {key: request[key] for key in ("dataset", "schema", "symbols", "stype_in", "start", "end")}


def _bundle_paths(root: Path, bundle_id: str, *, receipt_path: str | Path | None) -> dict[str, Path]:
    base = root / "data/cache/databento/tier_q_2026_confirmation" / bundle_id
    receipt = Path(receipt_path).resolve() if receipt_path else root / "reports/data_access/tier_q_2026_acquisition_receipt.json"
    return {
        "raw_ohlcv": base / "raw_ohlcv.dbn.zst",
        "raw_definition": base / "raw_definitions.dbn.zst",
        "final_development": base / "final_development_2026.parquet",
        "confirmation": base / "confirmation_2026_sealed.parquet",
        "contract_map": base / "explicit_contract_map.json",
        "symbology": base / "symbology_resolution.json",
        "receipt": receipt,
        "lock": root / "reports/data_access/tier_q_2026_acquisition.lock",
        "access_ledger": root / "reports/data_access/data_access_ledger.jsonl",
    }


def _record_spend_once(
    budget: DatabentoBudgetConfig, *, bundle_id: str, estimate: float,
    projected: float, actual_before: float, raw_path: Path,
) -> None:
    rows = [row for row in read_ledger(_ledger_path(budget)) if row.get("request_id") == bundle_id]
    if len(rows) > 1:
        raise TierQ2026AcquisitionError("duplicate 2026 spend rows")
    if rows:
        if abs(float(rows[0].get("actual_cost_usd") or 0) - estimate) > 1e-9:
            raise TierQ2026AcquisitionError("existing 2026 spend row drift")
        return
    append_spend_record(budget, DatabentoSpendRecord(
        request_id=bundle_id, timestamp_utc=utc_now(), dataset=DATASET,
        schema=f"{DATA_SCHEMA}+definition", symbols=list(SYMBOLS),
        stype_in="continuous+instrument_id", start=START, end=END,
        estimated_cost_usd=estimate, actual_cost_usd=estimate,
        cumulative_estimated_spend_usd=projected,
        cumulative_actual_spend_usd=actual_before + estimate,
        cache_hit=False, research_purpose=REQUEST_PURPOSE,
        candidate_tier="TIER_Q_FROZEN_G_THEN_C_ONE_SHOT",
        approval_mode=AUTO_UNDER_HARD_CAP, resulting_file=str(raw_path),
        checksum=sha256_file(raw_path), download_status="DOWNLOADED",
    ))


def _record_access_once(path: Path, *, bundle_id: str, manifest_hash: str) -> None:
    rows = _jsonl(path)
    matches = [row for row in rows if bundle_id in set(row.get("candidate_ids") or ())]
    expected = {
        (f"{START}:{SPLIT}", DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION.value),
        (f"{SPLIT}:{END}", DataRole.BLIND_VALIDATION.value),
    }
    if len(matches) not in {0, 2}:
        raise TierQ2026AcquisitionError("2026 access ledger cardinality drift")
    if matches:
        actual = {(row.get("period_accessed"), row.get("data_role")) for row in matches}
        if (
            actual != expected
            or any(row.get("freeze_manifest_hash") != manifest_hash for row in matches)
            or any(row.get("parameters_mutable") is not False for row in matches)
        ):
            raise TierQ2026AcquisitionError("existing 2026 access row drift")
        return
    candidate_ids = sorted([CAMPAIGN_ID, bundle_id, *CANDIDATE_IDS])
    append_access_record(
        DataAccessRecord(
            code_commit=current_commit(), process_id=os.getpid(),
            timestamp_utc=utc_now(), period_accessed=f"{START}:{SPLIT}",
            data_role=DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION.value,
            requesting_module="scripts.acquire_tier_q_2026_confirmation",
            candidate_ids=candidate_ids, reason_for_access=REQUEST_PURPOSE,
            freeze_manifest_hash=manifest_hash, parameters_mutable=False,
        ),
        ledger_path=str(path),
    )
    enforce_data_access(
        period=f"{SPLIT}:{END}", role=DataRole.BLIND_VALIDATION,
        requesting_module="scripts.acquire_tier_q_2026_confirmation",
        candidate_ids=candidate_ids,
        reason=REQUEST_PURPOSE, freeze_manifest_hash=manifest_hash,
        ledger_path=str(path),
    )


def _load_existing_receipt(
    path: Path, *, contract_hash: str, manifest_hash: str, bundle_id: str,
    budget: DatabentoBudgetConfig, access_ledger: Path,
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    receipt = json.loads(path.read_text(encoding="utf-8"))
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed or receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("bundle_id") != bundle_id
        or receipt.get("contract_hash") != contract_hash
        or receipt.get("authorization_manifest_hash") != manifest_hash
    ):
        raise TierQ2026AcquisitionError("existing 2026 receipt drift")
    for row in receipt.get("files", ()):
        artifact = Path(str(row.get("path") or ""))
        if not artifact.is_file() or sha256_file(artifact) != row.get("sha256"):
            raise TierQ2026AcquisitionError("sealed 2026 artifact drift")
    spend = [row for row in read_ledger(_ledger_path(budget)) if row.get("request_id") == bundle_id]
    access = [row for row in _jsonl(access_ledger) if bundle_id in set(row.get("candidate_ids") or ())]
    expected_access = {
        (f"{START}:{SPLIT}", DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION.value),
        (f"{SPLIT}:{END}", DataRole.BLIND_VALIDATION.value),
    }
    actual_access = {(row.get("period_accessed"), row.get("data_role")) for row in access}
    if (
        len(spend) != 1 or len(access) != 2 or actual_access != expected_access
        or any(row.get("freeze_manifest_hash") != manifest_hash for row in access)
        or any(row.get("parameters_mutable") is not False for row in access)
    ):
        raise TierQ2026AcquisitionError("sealed 2026 ledger cardinality drift")
    return receipt


def _ledger_path(config: DatabentoBudgetConfig) -> Path:
    path = Path(config.ledger_path)
    return path if path.is_absolute() else project_path(str(path))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run or acquire the frozen HYDRA Tier-Q 2026 package")
    parser.add_argument("--contract", required=True)
    parser.add_argument("--manifest", default="config/v7/autonomous_economic_discovery_director_0035.json")
    parser.add_argument("--manifest-hash", required=True)
    parser.add_argument("--receipt")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    root, contract, manifest = load_frozen_inputs(args.contract, args.manifest, expected_manifest_hash=args.manifest_hash)
    key = load_api_key()
    if not key:
        raise TierQ2026AcquisitionError("DATABENTO_API_KEY is required even for official metadata dry-run")
    from hydra.data.databento_loader import _import_databento
    result = acquire_tier_q_2026(
        contract=contract, manifest=manifest, expected_manifest_hash=args.manifest_hash,
        root=root, client=_import_databento().Historical(key), execute=args.execute,
        receipt_path=args.receipt,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
