#!/usr/bin/env python3
"""Governed acquisition of the frozen Treasury-curve tripwire input.

The command is deliberately dry-run by default.  ``--execute`` is the only
path that can download the exact, predeclared continuous ZT/ZF/ZN/TN/ZB/UB
one-minute bundle.  It fails closed if Databento's live cost, record count, or
billable byte estimate differs from the frozen acquisition contract.

This is an external acquisition utility.  It does not edit a production
manifest, controller, service, registry, or runtime configuration.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any, Mapping

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
from hydra.data.databento_loader import _import_databento, load_api_key
from hydra.economic_evolution.schema import stable_hash
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access
from scripts.acquire_fresh_confirmation_0035 import (
    _download_once,
    _file_receipt,
    _optional_lock,
    _persist_json_once,
)


DATASET = "GLBX.MDP3"
DATA_SCHEMA = "ohlcv-1m"
SYMBOLS = ("ZT.c.0", "ZF.c.0", "ZN.c.0", "TN.c.0", "ZB.c.0", "UB.c.0")
ROOTS = tuple(symbol.split(".", 1)[0] for symbol in SYMBOLS)
STYPE_IN = "continuous"
STYPE_OUT = "instrument_id"
START = "2023-01-03"
END = "2024-10-01"
EXPECTED_LIVE_COST_USD = 9.090267196298
EXPECTED_RECORD_COUNT = 2_489_949
EXPECTED_BILLABLE_BYTES = 139_437_144

# Trading-session-aligned, half-open boundaries.  They are embedded in the
# contract hash before any downloaded outcome can be inspected.
TEMPORAL_ROLES = (
    {
        "role": "DISCOVERY",
        "start": "2023-01-03",
        "end": "2024-01-22",
        "candidate_modification_allowed": True,
    },
    {
        "role": "VALIDATION",
        "start": "2024-01-22",
        "end": "2024-05-28",
        "candidate_modification_allowed": False,
    },
    {
        "role": "FINAL_DEVELOPMENT",
        "start": "2024-05-28",
        "end": "2024-10-01",
        "candidate_modification_allowed": False,
    },
)

PURPOSE = (
    "bounded Treasury-curve information tripwire with chronological roles "
    "frozen before acquisition; no Q4 or protected-data access"
)
RECEIPT_SCHEMA = "hydra_treasury_curve_tripwire_acquisition_receipt_v1"
PLAN_SCHEMA = "hydra_treasury_curve_tripwire_acquisition_plan_v1"
SYMBOLOGY_SCHEMA = "hydra_treasury_curve_causal_symbology_roll_map_v1"
DEFAULT_RECEIPT = (
    "reports/data_access/treasury_curve_tripwire_acquisition_receipt.json"
)
ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"


class TreasuryCurveAcquisitionError(RuntimeError):
    """The exact frozen acquisition cannot be completed safely."""


def frozen_contract() -> dict[str, Any]:
    """Return the complete embedded acquisition contract and its hash."""

    core = {
        "schema": "hydra_treasury_curve_tripwire_contract_v1",
        "status": "FROZEN_AWAITING_EXPLICIT_EXECUTE",
        "data_request": {
            "dataset": DATASET,
            "schema": DATA_SCHEMA,
            "symbols": list(SYMBOLS),
            "stype_in": STYPE_IN,
            "stype_out": STYPE_OUT,
            "start": START,
            "end": END,
            "date_interval": "HALF_OPEN",
        },
        "official_estimate": {
            "cost_usd": EXPECTED_LIVE_COST_USD,
            "records": EXPECTED_RECORD_COUNT,
            "billable_bytes": EXPECTED_BILLABLE_BYTES,
        },
        "temporal_roles": [dict(row) for row in TEMPORAL_ROLES],
        "definition_policy": {
            "requested": False,
            "reason": (
                "Definitions are not required to preserve the immutable continuous "
                "OHLCV tripwire input; any later explicit-contract enrichment must "
                "be separately estimated and frozen before acquisition."
            ),
        },
        "symbology_policy": {
            "required": True,
            "billable_purchase": False,
            "continuous_resolution": "continuous_to_instrument_id",
            "interval_resolution": "instrument_id_to_raw_symbol_per_interval",
            "coverage": "EXACT_HALF_OPEN_NO_GAP_NO_OVERLAP",
            "delivery_contract": "ROOT_QUARTERLY_MONTH_AND_YEAR_VERIFIED",
        },
        "q4_2024_access_allowed": False,
        "protected_data_access_allowed": False,
        "broker_or_order_capability": False,
    }
    return {**core, "contract_hash": stable_hash(core)}


def validate_frozen_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed on any request, estimate, role, or protected-period drift."""

    supplied = dict(contract)
    claimed = str(supplied.pop("contract_hash", ""))
    if not claimed or stable_hash(supplied) != claimed:
        raise TreasuryCurveAcquisitionError("frozen contract hash drift")

    canonical = frozen_contract()
    expected_hash = str(canonical["contract_hash"])
    if claimed != expected_hash or supplied != {
        key: value for key, value in canonical.items() if key != "contract_hash"
    }:
        raise TreasuryCurveAcquisitionError("frozen Treasury request drift")

    roles = [dict(row) for row in supplied.get("temporal_roles", ())]
    if len(roles) != 3:
        raise TreasuryCurveAcquisitionError("temporal-role cardinality drift")
    if roles[0]["start"] != START or roles[-1]["end"] != END:
        raise TreasuryCurveAcquisitionError("temporal roles do not cover request")
    if any(left["end"] != right["start"] for left, right in zip(roles, roles[1:])):
        raise TreasuryCurveAcquisitionError("temporal roles overlap or have a gap")
    if [row["role"] for row in roles] != [
        "DISCOVERY",
        "VALIDATION",
        "FINAL_DEVELOPMENT",
    ]:
        raise TreasuryCurveAcquisitionError("temporal-role order drift")
    if END > "2024-10-01" or supplied.get("q4_2024_access_allowed") is not False:
        raise TreasuryCurveAcquisitionError("request opens protected Q4")
    return {**supplied, "contract_hash": claimed}


def estimate_or_acquire(
    *,
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    receipt_path: str | Path = DEFAULT_RECEIPT,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Re-estimate and optionally acquire exactly the embedded tripwire bundle."""

    project = Path(root).resolve()
    frozen = validate_frozen_contract(
        frozen_contract() if contract is None else contract
    )
    cfg = _bound_budget(project, budget)
    request = _api_request(frozen["data_request"])
    bundle_id = request_id_for(
        {
            "contract_hash": frozen["contract_hash"],
            "request": request,
            "purpose": PURPOSE,
        }
    )
    paths = _paths(project, bundle_id, receipt_path)

    with _optional_lock(paths["lock"], enabled=execute):
        if paths["receipt"].is_file():
            return _load_existing_receipt(
                paths["receipt"],
                bundle_id=bundle_id,
                contract_hash=str(frozen["contract_hash"]),
                budget=cfg,
                access_ledger=paths["access_ledger"],
                raw_path=paths["raw_ohlcv"],
                symbology_path=paths["symbology"],
            )

        # Symbology endpoints are non-billable metadata.  Resolve the complete
        # causal roll map before either the dry-run decision or the OHLCV
        # download, but never persist it from a dry run.
        symbology = _resolve_causal_symbology(client)
        estimate = float(client.metadata.get_cost(**request))
        records = int(client.metadata.get_record_count(**request))
        billable_bytes = int(client.metadata.get_billable_size(**request))
        _verify_official_estimate(estimate, records, billable_bytes)
        projected, actual_before = enforce_budget(cfg, estimate)

        plan = {
            "schema": PLAN_SCHEMA,
            "bundle_id": bundle_id,
            "contract_hash": frozen["contract_hash"],
            "request": request,
            "temporal_roles": frozen["temporal_roles"],
            "definition_policy": frozen["definition_policy"],
            "symbology_policy": frozen["symbology_policy"],
            "symbology_roll_map": symbology,
            "symbology_endpoint_billable_cost_usd": 0.0,
            "official_live_estimate_usd": estimate,
            "official_record_count": records,
            "official_billable_bytes": billable_bytes,
            "cumulative_actual_before_usd": actual_before,
            "projected_cumulative_estimate_usd": projected,
            "authoritative_cumulative_cap_usd": cfg.hard_cap_usd,
            "q4_access_count_delta": 0,
            "protected_data_access_count_delta": 0,
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

        network_request = _download_once(
            client,
            request,
            paths["raw_ohlcv"],
            stype_out=STYPE_OUT,
        )
        _persist_json_once(paths["symbology"], symbology)
        raw = _file_receipt("RAW_DBN_OHLCV", paths["raw_ohlcv"])
        roll_map = _file_receipt("SYMBOLOGY_ROLL_MAP", paths["symbology"])
        _record_spend_once(
            cfg,
            bundle_id=bundle_id,
            estimate=estimate,
            projected=projected,
            actual_before=actual_before,
            raw=raw,
        )
        _record_access_roles_once(
            paths["access_ledger"],
            bundle_id=bundle_id,
            contract_hash=str(frozen["contract_hash"]),
        )
        _estimated_after, actual_after = cumulative_spend(Path(cfg.ledger_path))
        core = {
            "schema": RECEIPT_SCHEMA,
            "bundle_id": bundle_id,
            "created_at_utc": utc_now(),
            "contract_hash": frozen["contract_hash"],
            "request": request,
            "temporal_roles": frozen["temporal_roles"],
            "temporal_roles_hash": stable_hash(frozen["temporal_roles"]),
            "definition_policy": frozen["definition_policy"],
            "symbology_policy": frozen["symbology_policy"],
            "symbology_roll_map": symbology,
            "symbology_roll_map_hash": symbology["mapping_hash"],
            "symbology_endpoint_billable_cost_usd": 0.0,
            "official_live_cost_usd": estimate,
            "official_record_count": records,
            "official_billable_bytes": billable_bytes,
            "actual_cost_usd": estimate,
            "cumulative_actual_usd": actual_after,
            "download_status": "DOWNLOADED",
            "network_data_request_made": bool(network_request),
            "raw_immutable": True,
            "files": [raw, roll_map],
            "spend_ledger_request_id": bundle_id,
            "data_access_markers": [
                _access_marker(bundle_id, row["role"])
                for row in frozen["temporal_roles"]
            ],
            "q4_access_count_delta": 0,
            "protected_data_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "runtime_or_manifest_modified": False,
        }
        receipt = {**core, "receipt_hash": stable_hash(core)}
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _bound_budget(
    project: Path, budget: DatabentoBudgetConfig | None
) -> DatabentoBudgetConfig:
    source = budget or DatabentoBudgetConfig()
    if (
        source.hard_cap_usd > DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD
        or source.safety_ceiling_usd > DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD
        or source.safety_ceiling_usd > source.hard_cap_usd
    ):
        raise TreasuryCurveAcquisitionError("budget exceeds authoritative authority")
    ledger = Path(source.ledger_path)
    summary = Path(source.summary_path)
    return DatabentoBudgetConfig(
        budget_start=source.budget_start,
        hard_cap_usd=source.hard_cap_usd,
        safety_ceiling_usd=source.safety_ceiling_usd,
        ledger_path=str(ledger if ledger.is_absolute() else project / ledger),
        summary_path=str(summary if summary.is_absolute() else project / summary),
    )


def _resolve_causal_symbology(client: Any) -> dict[str, Any]:
    """Resolve and validate a date-aware roll map without purchasing data.

    Instrument IDs may be reused across dates, so the second resolution is
    deliberately performed for every continuous-mapping interval rather than
    flattened into one global ``instrument_id -> raw_symbol`` dictionary.
    """

    continuous_response = client.symbology.resolve(
        dataset=DATASET,
        symbols=list(SYMBOLS),
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=START,
        end_date=END,
    )
    raw_continuous = {
        str(key): list(value)
        for key, value in dict(continuous_response.get("result") or {}).items()
    }
    if set(raw_continuous) != set(SYMBOLS):
        raise TreasuryCurveAcquisitionError("continuous symbology is incomplete")

    normalized_continuous: dict[str, list[dict[str, str]]] = {}
    resolved: list[dict[str, Any]] = []
    raw_call_count = 0
    for symbol in SYMBOLS:
        root = symbol.split(".", 1)[0]
        segments = _normalize_covered_intervals(
            raw_continuous[symbol], start=START, end=END, label=symbol
        )
        normalized_continuous[symbol] = [dict(row) for row in segments]
        for segment in segments:
            instrument_id = str(segment["s"])
            if not instrument_id.isdigit() or int(instrument_id) <= 0:
                raise TreasuryCurveAcquisitionError(
                    f"invalid instrument_id for {symbol}: {instrument_id!r}"
                )
            raw_call_count += 1
            raw_response = client.symbology.resolve(
                dataset=DATASET,
                symbols=[instrument_id],
                stype_in="instrument_id",
                stype_out="raw_symbol",
                start_date=segment["d0"],
                end_date=segment["d1"],
            )
            raw_result = {
                str(key): list(value)
                for key, value in dict(raw_response.get("result") or {}).items()
            }
            if set(raw_result) != {instrument_id}:
                raise TreasuryCurveAcquisitionError(
                    f"raw symbology is incomplete for instrument {instrument_id}"
                )
            raw_segments = _normalize_covered_intervals(
                raw_result[instrument_id],
                start=segment["d0"],
                end=segment["d1"],
                label=f"instrument_id={instrument_id}",
            )
            for raw_segment in raw_segments:
                contract = _parse_delivery_contract(
                    root=root,
                    raw_symbol=str(raw_segment["s"]),
                    active_start=str(raw_segment["d0"]),
                )
                resolved.append(
                    {
                        "root": root,
                        "continuous_symbol": symbol,
                        "instrument_id": instrument_id,
                        "raw_symbol": contract["raw_symbol"],
                        "delivery_month_code": contract["delivery_month_code"],
                        "delivery_month": contract["delivery_month"],
                        "delivery_year": contract["delivery_year"],
                        "d0": raw_segment["d0"],
                        "d1": raw_segment["d1"],
                    }
                )

    resolved.sort(key=lambda row: (row["root"], row["d0"], row["d1"], row["raw_symbol"]))
    coverage: dict[str, dict[str, Any]] = {}
    for root in ROOTS:
        intervals = [row for row in resolved if row["root"] == root]
        _verify_exact_coverage(intervals, start=START, end=END, label=root)
        coverage[root] = {
            "start": START,
            "end": END,
            "gap_count": 0,
            "overlap_count": 0,
            "interval_count": len(intervals),
            "instrument_ids": sorted(
                {row["instrument_id"] for row in intervals}, key=int
            ),
            "raw_symbols": sorted({row["raw_symbol"] for row in intervals}),
            "delivery_months": sorted({row["delivery_month"] for row in intervals}),
        }
    core = {
        "schema": SYMBOLOGY_SCHEMA,
        "dataset": DATASET,
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "roots": list(ROOTS),
        "continuous_symbols": list(SYMBOLS),
        "continuous_mapping": normalized_continuous,
        "contract_intervals": resolved,
        "coverage_by_root": coverage,
        "policy": "EXPLICIT_INTERVAL_SYMBOLOGY_NO_GLOBAL_ID_FLATTENING",
        "continuous_to_instrument_id_call_count": 1,
        "instrument_interval_to_raw_symbol_call_count": raw_call_count,
        "endpoint_billable_cost_usd": 0.0,
        "q4_access_count_delta": 0,
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _normalize_covered_intervals(
    rows: list[Any], *, start: str, end: str, label: str
) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for raw in rows:
        row = dict(raw)
        if not str(row.get("s") or "").strip():
            raise TreasuryCurveAcquisitionError(f"empty symbology value for {label}")
        raw_start = _iso_date(row.get("d0"), f"{label}.d0")
        raw_end = _iso_date(row.get("d1"), f"{label}.d1")
        clipped_start = max(raw_start, start)
        clipped_end = min(raw_end, end)
        if clipped_start < clipped_end:
            normalized.append(
                {"s": str(row["s"]), "d0": clipped_start, "d1": clipped_end}
            )
    normalized.sort(key=lambda row: (row["d0"], row["d1"], row["s"]))
    _verify_exact_coverage(normalized, start=start, end=end, label=label)
    return normalized


def _verify_exact_coverage(
    rows: list[Mapping[str, Any]], *, start: str, end: str, label: str
) -> None:
    if not rows:
        raise TreasuryCurveAcquisitionError(f"symbology coverage is empty for {label}")
    if str(rows[0]["d0"]) != start or str(rows[-1]["d1"]) != end:
        raise TreasuryCurveAcquisitionError(
            f"symbology coverage boundaries drift for {label}"
        )
    for index, row in enumerate(rows):
        d0 = _iso_date(row.get("d0"), f"{label}.d0")
        d1 = _iso_date(row.get("d1"), f"{label}.d1")
        if d0 >= d1:
            raise TreasuryCurveAcquisitionError(
                f"non-positive symbology interval for {label}"
            )
        if index and str(rows[index - 1]["d1"]) != d0:
            raise TreasuryCurveAcquisitionError(
                f"symbology coverage gap or overlap for {label}"
            )


def _parse_delivery_contract(
    *, root: str, raw_symbol: str, active_start: str
) -> dict[str, Any]:
    value = raw_symbol.strip().upper()
    match = re.fullmatch(rf"{re.escape(root)}([HMUZ])(\d{{1,2}})", value)
    if match is None:
        raise TreasuryCurveAcquisitionError(
            f"raw symbol root/month/year mismatch for {root}: {raw_symbol!r}"
        )
    code, digits = match.groups()
    month = {"H": 3, "M": 6, "U": 9, "Z": 12}[code]
    active_year = date.fromisoformat(active_start).year
    if len(digits) == 2:
        year = 2000 + int(digits)
    else:
        decade = active_year - active_year % 10
        candidates = (decade - 10 + int(digits), decade + int(digits), decade + 10 + int(digits))
        year = min(candidates, key=lambda candidate: abs(candidate - active_year))
    active = date.fromisoformat(active_start)
    delivery_distance = (year - active.year) * 12 + month - active.month
    if delivery_distance < 0 or delivery_distance > 6:
        raise TreasuryCurveAcquisitionError(
            f"delivery month/year is not causal front-contract context: {value} at {active_start}"
        )
    return {
        "raw_symbol": value,
        "delivery_month_code": code,
        "delivery_month": f"{year:04d}-{month:02d}",
        "delivery_year": year,
    }


def _iso_date(value: Any, label: str) -> str:
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (TypeError, ValueError) as exc:
        raise TreasuryCurveAcquisitionError(
            f"invalid symbology date for {label}: {value!r}"
        ) from exc


def _api_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dataset": request["dataset"],
        "schema": request["schema"],
        "symbols": list(request["symbols"]),
        "stype_in": request["stype_in"],
        "start": request["start"],
        "end": request["end"],
    }


def _verify_official_estimate(cost: float, records: int, billable_bytes: int) -> None:
    if not math.isfinite(cost) or not math.isclose(
        cost, EXPECTED_LIVE_COST_USD, rel_tol=0.0, abs_tol=1e-12
    ):
        raise TreasuryCurveAcquisitionError(
            f"official cost drift: expected {EXPECTED_LIVE_COST_USD:.12f}, got {cost!r}"
        )
    if records != EXPECTED_RECORD_COUNT:
        raise TreasuryCurveAcquisitionError(
            f"official record-count drift: expected {EXPECTED_RECORD_COUNT}, got {records}"
        )
    if billable_bytes != EXPECTED_BILLABLE_BYTES:
        raise TreasuryCurveAcquisitionError(
            "official billable-size drift: "
            f"expected {EXPECTED_BILLABLE_BYTES}, got {billable_bytes}"
        )


def _record_spend_once(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
    projected: float,
    actual_before: float,
    raw: Mapping[str, Any],
) -> None:
    rows = [
        row
        for row in read_ledger(budget.ledger_path)
        if row.get("request_id") == bundle_id
    ]
    if len(rows) > 1:
        raise TreasuryCurveAcquisitionError("duplicate spend records")
    if rows:
        row = rows[0]
        if (
            row.get("download_status") != "DOWNLOADED"
            or not math.isclose(
                float(row.get("actual_cost_usd") or 0.0),
                estimate,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
            or row.get("checksum") != raw["sha256"]
        ):
            raise TreasuryCurveAcquisitionError("existing spend record drift")
        return
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema=DATA_SCHEMA,
            symbols=list(SYMBOLS),
            stype_in=STYPE_IN,
            start=START,
            end=END,
            estimated_cost_usd=estimate,
            actual_cost_usd=estimate,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual_before + estimate,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier="HYPOTHESIS_TRIPWIRE_INPUT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(raw["path"]),
            checksum=str(raw["sha256"]),
            download_status="DOWNLOADED",
        ),
    )


def _record_access_roles_once(
    path: Path, *, bundle_id: str, contract_hash: str
) -> None:
    rows = _jsonl(path)
    for role in TEMPORAL_ROLES:
        marker = _access_marker(bundle_id, role["role"])
        matching = [
            row for row in rows if marker in set(row.get("candidate_ids") or ())
        ]
        if len(matching) > 1:
            raise TreasuryCurveAcquisitionError("duplicate data-access records")
        expected_role = (
            DataRole.DEVELOPMENT
            if role["role"] == "DISCOVERY"
            else DataRole.BLIND_VALIDATION
        )
        if matching:
            row = matching[0]
            if (
                row.get("data_role") != expected_role.value
                or row.get("parameters_mutable")
                != (expected_role == DataRole.DEVELOPMENT)
                or row.get("freeze_manifest_hash") != contract_hash
            ):
                raise TreasuryCurveAcquisitionError("existing data-access record drift")
            continue
        enforce_data_access(
            period=f"{role['start']}:{role['end']}",
            role=expected_role,
            requesting_module="scripts.acquire_treasury_curve_tripwire",
            candidate_ids=[bundle_id, marker],
            reason=f"{PURPOSE}; frozen economic role={role['role']}",
            freeze_manifest_hash=contract_hash,
            ledger_path=str(path),
        )
        rows = _jsonl(path)


def _access_marker(bundle_id: str, role: str) -> str:
    return f"{bundle_id}:{role}"


def _paths(
    root: Path, bundle_id: str, receipt_path: str | Path
) -> dict[str, Path]:
    base = root / "data/cache/databento/treasury_curve_tripwire" / bundle_id
    receipt = Path(receipt_path)
    if not receipt.is_absolute():
        receipt = root / receipt
    return {
        "raw_ohlcv": base / "raw_ohlcv.dbn.zst",
        "symbology": base / "symbology_roll_map.json",
        "lock": root
        / "reports/data_access/treasury_curve_tripwire_acquisition.lock",
        "access_ledger": root / ACCESS_LEDGER,
        "receipt": receipt.resolve(),
    }


def _load_existing_receipt(
    path: Path,
    *,
    bundle_id: str,
    contract_hash: str,
    budget: DatabentoBudgetConfig,
    access_ledger: Path,
    raw_path: Path,
    symbology_path: Path,
) -> dict[str, Any]:
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TreasuryCurveAcquisitionError("existing receipt is invalid") from exc
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("bundle_id") != bundle_id
        or receipt.get("contract_hash") != contract_hash
        or receipt.get("request") != _api_request(frozen_contract()["data_request"])
        or receipt.get("temporal_roles") != [dict(row) for row in TEMPORAL_ROLES]
        or receipt.get("definition_policy")
        != frozen_contract()["definition_policy"]
        or receipt.get("symbology_policy")
        != frozen_contract()["symbology_policy"]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("raw_immutable") is not True
        or receipt.get("runtime_or_manifest_modified") is not False
    ):
        raise TreasuryCurveAcquisitionError("existing receipt drift")
    _verify_official_estimate(
        float(receipt.get("official_live_cost_usd", float("nan"))),
        int(receipt.get("official_record_count", -1)),
        int(receipt.get("official_billable_bytes", -1)),
    )
    files = list(receipt.get("files") or ())
    if len(files) != 2 or {row.get("kind") for row in files} != {
        "RAW_DBN_OHLCV",
        "SYMBOLOGY_ROLL_MAP",
    }:
        raise TreasuryCurveAcquisitionError("receipt file inventory drift")
    raw = next(dict(row) for row in files if row.get("kind") == "RAW_DBN_OHLCV")
    symbology_file = next(
        dict(row) for row in files if row.get("kind") == "SYMBOLOGY_ROLL_MAP"
    )
    if (
        Path(str(raw.get("path") or "")) != raw_path
        or not raw_path.is_file()
        or raw_path.stat().st_size != int(raw.get("size_bytes") or -1)
        or sha256_file(raw_path) != str(raw.get("sha256") or "")
    ):
        raise TreasuryCurveAcquisitionError("sealed raw artifact drift")
    if (
        Path(str(symbology_file.get("path") or "")) != symbology_path
        or not symbology_path.is_file()
        or symbology_path.stat().st_size
        != int(symbology_file.get("size_bytes") or -1)
        or sha256_file(symbology_path) != str(symbology_file.get("sha256") or "")
    ):
        raise TreasuryCurveAcquisitionError("sealed symbology artifact drift")
    try:
        symbology = json.loads(symbology_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TreasuryCurveAcquisitionError(
            "sealed symbology artifact is invalid"
        ) from exc
    mapping_core = dict(symbology)
    mapping_hash = str(mapping_core.pop("mapping_hash", ""))
    if (
        symbology.get("schema") != SYMBOLOGY_SCHEMA
        or stable_hash(mapping_core) != mapping_hash
        or receipt.get("symbology_roll_map") != symbology
        or receipt.get("symbology_roll_map_hash") != mapping_hash
        or symbology.get("q4_access_count_delta") != 0
    ):
        raise TreasuryCurveAcquisitionError("sealed symbology mapping drift")
    for root in ROOTS:
        intervals = [
            row
            for row in list(symbology.get("contract_intervals") or ())
            if row.get("root") == root
        ]
        _verify_exact_coverage(intervals, start=START, end=END, label=root)
    spend = [
        row
        for row in read_ledger(budget.ledger_path)
        if row.get("request_id") == bundle_id
    ]
    markers = {
        _access_marker(bundle_id, row["role"]) for row in TEMPORAL_ROLES
    }
    access = [
        row
        for row in _jsonl(access_ledger)
        if markers.intersection(set(row.get("candidate_ids") or ()))
    ]
    if len(spend) != 1 or len(access) != len(TEMPORAL_ROLES):
        raise TreasuryCurveAcquisitionError("sealed receipt ledger cardinality drift")
    if (
        spend[0].get("checksum") != raw.get("sha256")
        or not math.isclose(
            float(spend[0].get("actual_cost_usd") or 0.0),
            EXPECTED_LIVE_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise TreasuryCurveAcquisitionError("sealed receipt spend-ledger drift")
    for role in TEMPORAL_ROLES:
        marker = _access_marker(bundle_id, role["role"])
        matching = [
            row for row in access if marker in set(row.get("candidate_ids") or ())
        ]
        expected_role = (
            DataRole.DEVELOPMENT
            if role["role"] == "DISCOVERY"
            else DataRole.BLIND_VALIDATION
        )
        if len(matching) != 1 or (
            matching[0].get("period_accessed") != f"{role['start']}:{role['end']}"
            or matching[0].get("data_role") != expected_role.value
            or matching[0].get("parameters_mutable")
            != (expected_role == DataRole.DEVELOPMENT)
            or matching[0].get("freeze_manifest_hash") != contract_hash
        ):
            raise TreasuryCurveAcquisitionError("sealed receipt data-access drift")
    return receipt


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Estimate or acquire the exact frozen Treasury-curve tripwire"
    )
    parser.add_argument("--root", default=".")
    parser.add_argument("--receipt", default=DEFAULT_RECEIPT)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform the bounded purchase; default only verifies live estimates",
    )
    args = parser.parse_args()
    api_key = load_api_key()
    if not api_key:
        raise TreasuryCurveAcquisitionError(
            "DATABENTO_API_KEY is required for official live cost verification"
        )
    client = _import_databento().Historical(api_key)
    result = estimate_or_acquire(
        root=args.root,
        client=client,
        execute=bool(args.execute),
        receipt_path=args.receipt,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
