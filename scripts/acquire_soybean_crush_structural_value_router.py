#!/usr/bin/env python3
"""Governed ZM/ZL input acquisition for the frozen soybean-crush router.

The default command is metadata-only.  A download is possible only with the
explicit ``--execute`` flag, after the immutable manifest, existing ZS bundle,
official estimates, chronological roles, pre-Q4 boundary and live committed
Databento budget all reconcile.  The append-only spend reservation is written
before raw network I/O so an interrupted two-file download cannot overspend on
retry.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
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


MANIFEST_PATH = "config/research/soybean_crush_structural_value_router_v1.json"
EXPECTED_MANIFEST_HASH = (
    "67c65399dbe9919c47fce22eced7460a3ece876ebb160b7d82e17850601f5827"
)
DATASET = "GLBX.MDP3"
SYMBOLS = ("ZM.c.0", "ZL.c.0")
SCHEMAS = ("ohlcv-1m", "definition")
STYPE_IN = "continuous"
STYPE_OUT = "instrument_id"
START = "2018-01-02"
END = "2024-10-01"
EXPECTED = {
    "ohlcv-1m": {
        "record_count": 1_612_801,
        "billable_size_bytes": 90_316_856,
        "estimated_cost_usd": 5.887988880277,
    },
    "definition": {
        "record_count": 4_230,
        "billable_size_bytes": 1_522_800,
        "estimated_cost_usd": 0.002410970628,
    },
}
EXPECTED_TOTAL_COST_USD = 5.890399850905
HARD_CAP_USD = 200.720719923081
TEMPORAL_ROLES = (
    {
        "role": "DISCOVERY",
        "start": "2018-01-02",
        "end": "2022-01-01",
        "candidate_modification_allowed": True,
    },
    {
        "role": "VALIDATION",
        "start": "2022-01-01",
        "end": "2023-01-01",
        "candidate_modification_allowed": False,
    },
    {
        "role": "FINAL_DEVELOPMENT",
        "start": "2023-01-01",
        "end": "2024-10-01",
        "candidate_modification_allowed": False,
    },
)
PURPOSE = (
    "frozen pre-Q4 soybean-crush structural-value single-leg router input; "
    "ZM/ZL OHLCV-1m and definitions only; no economic outcome, broker, orders, "
    "XFA or promotion"
)
DEFAULT_RECEIPT = (
    "reports/data_access/"
    "soybean_crush_structural_value_router_acquisition_receipt.json"
)
DEFAULT_CACHE_ROOT = "data/cache/databento/soybean_crush_structural_value_router"
DEFAULT_ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"
DEFAULT_LOCAL_LOCK = (
    "reports/data_access/soybean_crush_structural_value_router_acquisition.lock"
)
DEFAULT_GLOBAL_LOCK = "reports/data_access/fresh_confirmation_0035_acquisition.lock"
RECEIPT_SCHEMA = "hydra_soybean_crush_structural_value_router_acquisition_receipt_v1"
SYMBOLOGY_SCHEMA = "hydra_soybean_crush_zm_zl_symbology_v1"


class SoybeanCrushAcquisitionError(RuntimeError):
    """The frozen ZM/ZL acquisition cannot be performed safely."""


def load_and_validate_manifest(
    root: str | Path, path: str | Path = MANIFEST_PATH
) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest_path = Path(path)
    if not manifest_path.is_absolute():
        manifest_path = project / manifest_path
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SoybeanCrushAcquisitionError("frozen manifest unavailable or invalid") from exc

    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if (
        not claimed
        or stable_hash(core) != claimed
        or claimed != EXPECTED_MANIFEST_HASH
    ):
        raise SoybeanCrushAcquisitionError("frozen manifest hash drift")

    data = dict(manifest.get("new_data_contract") or {})
    governance = dict(manifest.get("governance") or {})
    budget = dict(manifest.get("budget") or {})
    account = dict(manifest.get("account_contract") or {})
    if (
        manifest.get("branch_id") != "SOYBEAN_CRUSH_STRUCTURAL_VALUE_ROUTER_V1"
        or data.get("dataset") != DATASET
        or tuple(data.get("symbols") or ()) != SYMBOLS
        or tuple(data.get("schemas") or ()) != SCHEMAS
        or data.get("stype_in") != STYPE_IN
        or data.get("stype_out") != STYPE_OUT
        or data.get("start") != START
        or data.get("end_exclusive") != END
        or data.get("q4_2024_access") is not False
        or data.get("official_estimates") != EXPECTED
        or not math.isclose(
            float(data.get("official_total_cost_usd", float("nan"))),
            EXPECTED_TOTAL_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or manifest.get("chronological_roles")
        != [dict(row) for row in TEMPORAL_ROLES]
        or manifest.get("candidate_lattice", {}).get("proposal_count") != 24
        or account.get("full_board_crush_execution_allowed") is not False
        or governance.get("no_q4_access") is not True
        or governance.get("no_broker") is not True
        or governance.get("no_orders") is not True
        or governance.get("no_live_trading") is not True
        or not math.isclose(
            float(budget.get("authoritative_cumulative_cap_usd", -1.0)),
            HARD_CAP_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(budget.get("maximum_branch_purchase_usd", -1.0)),
            EXPECTED_TOTAL_COST_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise SoybeanCrushAcquisitionError("frozen manifest contract drift")
    if END > "2024-10-01":
        raise SoybeanCrushAcquisitionError("frozen request enters protected Q4")

    roles = list(manifest["chronological_roles"])
    if (
        roles[0]["start"] != START
        or roles[-1]["end"] != END
        or any(left["end"] != right["start"] for left, right in zip(roles, roles[1:]))
    ):
        raise SoybeanCrushAcquisitionError("chronological role gap or overlap")
    _verify_frozen_local_inputs(project, manifest)
    return manifest


def estimate_or_acquire(
    *,
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    manifest_path: str | Path = MANIFEST_PATH,
    receipt_path: str | Path = DEFAULT_RECEIPT,
    cache_root: str | Path = DEFAULT_CACHE_ROOT,
    access_ledger_path: str | Path = DEFAULT_ACCESS_LEDGER,
    local_lock_path: str | Path = DEFAULT_LOCAL_LOCK,
    global_lock_path: str | Path = DEFAULT_GLOBAL_LOCK,
) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = load_and_validate_manifest(project, manifest_path)
    cfg = _bound_budget(project, budget)
    requests = {schema: _request(schema) for schema in SCHEMAS}
    bundle_id = request_id_for(
        {
            "manifest_hash": EXPECTED_MANIFEST_HASH,
            "requests": requests,
            "purpose": PURPOSE,
        }
    )
    paths = _paths(
        project,
        bundle_id=bundle_id,
        receipt_path=receipt_path,
        cache_root=cache_root,
        access_ledger_path=access_ledger_path,
        local_lock_path=local_lock_path,
        global_lock_path=global_lock_path,
    )

    with _optional_lock(paths["global_lock"], enabled=execute):
        with _optional_lock(paths["local_lock"], enabled=execute):
            if paths["receipt"].is_file():
                return _load_existing_receipt(
                    paths["receipt"],
                    bundle_id=bundle_id,
                    manifest=manifest,
                    budget=cfg,
                    paths=paths,
                )

            estimates = _live_estimates(client)
            total_cost = math.fsum(
                float(row["estimated_cost_usd"]) for row in estimates.values()
            )
            if not math.isclose(
                total_cost,
                EXPECTED_TOTAL_COST_USD,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise SoybeanCrushAcquisitionError("combined official cost drift")
            symbology = _resolve_symbology(client)

            actual_before, outstanding_before = _committed_spend(cfg)
            ledger_state = _bundle_ledger_state(
                cfg, bundle_id=bundle_id, estimate=total_cost
            )
            incremental_commitment = 0.0 if ledger_state != "NONE" else total_cost
            effective_cap = min(cfg.hard_cap_usd, cfg.safety_ceiling_usd)
            projected_commitment = (
                actual_before + outstanding_before + incremental_commitment
            )
            if projected_commitment > effective_cap + 1e-12:
                raise SoybeanCrushAcquisitionError(
                    "live committed spend exceeds authoritative budget"
                )

            plan = {
                "schema": "hydra_soybean_crush_structural_value_router_acquisition_plan_v1",
                "bundle_id": bundle_id,
                "manifest_hash": EXPECTED_MANIFEST_HASH,
                "requests": requests,
                "official_estimates": estimates,
                "official_total_cost_usd": total_cost,
                "symbology": symbology,
                "chronological_roles": [dict(row) for row in TEMPORAL_ROLES],
                "cumulative_actual_before_usd": actual_before,
                "outstanding_estimates_before_usd": outstanding_before,
                "estimated_incremental_commitment_usd": incremental_commitment,
                "projected_committed_spend_usd": projected_commitment,
                "authoritative_cumulative_cap_usd": effective_cap,
                "remaining_after_estimate_usd": effective_cap - projected_commitment,
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
                    "market_data_downloaded": False,
                    "files_created": 0,
                    "economic_outcomes_read": 0,
                }

            _reserve_spend_once(cfg, bundle_id=bundle_id, estimate=total_cost)
            network = {
                schema: _download_once(
                    client,
                    request,
                    paths["raw_ohlcv"] if schema == "ohlcv-1m" else paths["raw_definition"],
                    stype_out=STYPE_OUT,
                )
                for schema, request in requests.items()
            }
            _persist_json_once(paths["symbology"], symbology)
            inventory = [
                _file_receipt("RAW_DBN_OHLCV_1M", paths["raw_ohlcv"]),
                _file_receipt("RAW_DBN_DEFINITION", paths["raw_definition"]),
                _file_receipt("CONTINUOUS_SYMBOLOGY", paths["symbology"]),
            ]
            _complete_spend_once(
                cfg,
                bundle_id=bundle_id,
                estimate=total_cost,
                inventory=inventory,
            )
            _record_access_roles_once(
                paths["access_ledger"], bundle_id=bundle_id
            )
            _estimated_after, actual_after = cumulative_spend(cfg.ledger_path)
            receipt_core = {
                **plan,
                "schema": RECEIPT_SCHEMA,
                "completed_at_utc": utc_now(),
                "download_status": "DOWNLOADED",
                "market_data_downloaded": True,
                "network": network,
                "actual_incremental_spend_usd": total_cost,
                "cumulative_actual_after_usd": actual_after,
                "symbology_hash": symbology["mapping_hash"],
                "files": inventory,
                "inventory_hash": stable_hash(inventory),
                "raw_immutable": True,
                "economic_outcomes_read": 0,
                "economic_replay_started": False,
                "promotion_changes": 0,
                "runtime_or_manifest_modified": False,
            }
            receipt_core.pop("execute", None)
            receipt = {
                **receipt_core,
                "receipt_hash": stable_hash(receipt_core),
            }
            _persist_json_once(paths["receipt"], receipt)
            return receipt


def _verify_frozen_local_inputs(project: Path, manifest: Mapping[str, Any]) -> None:
    source = dict(manifest["frozen_existing_zs_input"])
    receipt_path = project / str(source["receipt_path"])
    if (
        not receipt_path.is_file()
        or sha256_file(receipt_path) != source["receipt_file_sha256"]
    ):
        raise SoybeanCrushAcquisitionError("frozen ZS receipt file drift")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SoybeanCrushAcquisitionError("frozen ZS receipt invalid") from exc
    receipt_core = dict(receipt)
    receipt_hash = str(receipt_core.pop("receipt_hash", ""))
    if (
        stable_hash(receipt_core) != receipt_hash
        or receipt_hash != source["receipt_hash"]
        or receipt.get("bundle_id") != source["bundle_id"]
        or receipt.get("download_status") != "DOWNLOADED"
    ):
        raise SoybeanCrushAcquisitionError("frozen ZS receipt semantic drift")
    for kind in ("ohlcv", "definition"):
        artifact = project / str(source[f"{kind}_path"])
        if (
            not artifact.is_file()
            or sha256_file(artifact) != source[f"{kind}_sha256"]
        ):
            raise SoybeanCrushAcquisitionError(f"frozen ZS {kind} input drift")

    rules = dict(manifest["official_rule_evidence"])
    rules_path = project / str(rules["snapshot_path"])
    if (
        not rules_path.is_file()
        or sha256_file(rules_path) != rules["snapshot_file_sha256"]
    ):
        raise SoybeanCrushAcquisitionError("Topstep rule snapshot drift")


def _request(schema: str) -> dict[str, Any]:
    return {
        "dataset": DATASET,
        "symbols": list(SYMBOLS),
        "schema": schema,
        "stype_in": STYPE_IN,
        "start": START,
        "end": END,
    }


def _live_estimates(client: Any) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for schema in SCHEMAS:
        request = _request(schema)
        observed = {
            "record_count": int(client.metadata.get_record_count(**request)),
            "billable_size_bytes": int(client.metadata.get_billable_size(**request)),
            "estimated_cost_usd": float(client.metadata.get_cost(**request)),
        }
        if observed != EXPECTED[schema]:
            raise SoybeanCrushAcquisitionError(f"official estimate drift: {schema}")
        output[schema] = observed
    return output


def _resolve_symbology(client: Any) -> dict[str, Any]:
    response = client.symbology.resolve(
        dataset=DATASET,
        symbols=list(SYMBOLS),
        stype_in=STYPE_IN,
        stype_out=STYPE_OUT,
        start_date=START,
        end_date=END,
    )
    raw = dict(response.get("result") or {})
    if set(raw) != set(SYMBOLS):
        raise SoybeanCrushAcquisitionError("continuous symbology incomplete")
    mappings = {
        symbol: _normalize_intervals(list(raw[symbol]), label=symbol)
        for symbol in SYMBOLS
    }
    instrument_sets = [
        {str(row["s"]) for row in mappings[symbol]} for symbol in SYMBOLS
    ]
    if instrument_sets[0] & instrument_sets[1]:
        raise SoybeanCrushAcquisitionError(
            "ZM and ZL symbology share an instrument identifier"
        )
    core = {
        "schema": SYMBOLOGY_SCHEMA,
        "dataset": DATASET,
        "symbols": list(SYMBOLS),
        "start": START,
        "end_exclusive": END,
        "date_interval": "HALF_OPEN",
        "continuous_mapping": mappings,
        "coverage": {
            symbol: {
                "start": START,
                "end_exclusive": END,
                "interval_count": len(mappings[symbol]),
                "gap_count": 0,
                "overlap_count": 0,
            }
            for symbol in SYMBOLS
        },
        "q4_access_count_delta": 0,
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _normalize_intervals(rows: list[Any], *, label: str) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for raw in rows:
        row = dict(raw)
        start = str(row.get("d0") or row.get("start_date") or "")[:10]
        end = str(row.get("d1") or row.get("end_date") or "")[:10]
        instrument = str(row.get("s") or row.get("symbol") or "")
        clipped_start, clipped_end = max(start, START), min(end, END)
        if clipped_start < clipped_end:
            normalized.append(
                {"s": instrument, "d0": clipped_start, "d1": clipped_end}
            )
    normalized.sort(key=lambda row: (row["d0"], row["d1"], row["s"]))
    if (
        not normalized
        or normalized[0]["d0"] != START
        or normalized[-1]["d1"] != END
    ):
        raise SoybeanCrushAcquisitionError(f"symbology boundary drift: {label}")
    for index, row in enumerate(normalized):
        if not row["s"] or row["d0"] >= row["d1"]:
            raise SoybeanCrushAcquisitionError(f"invalid symbology interval: {label}")
        if index and normalized[index - 1]["d1"] != row["d0"]:
            raise SoybeanCrushAcquisitionError(
                f"symbology gap or overlap: {label}"
            )
    return normalized


def _bound_budget(
    project: Path, budget: DatabentoBudgetConfig | None
) -> DatabentoBudgetConfig:
    source = budget or DatabentoBudgetConfig()
    if (
        not math.isclose(
            source.hard_cap_usd, HARD_CAP_USD, rel_tol=0.0, abs_tol=1e-12
        )
        or source.safety_ceiling_usd > HARD_CAP_USD
        or source.safety_ceiling_usd > source.hard_cap_usd
        or not math.isclose(
            DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
            HARD_CAP_USD,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise SoybeanCrushAcquisitionError("budget authority drift")
    ledger = Path(source.ledger_path)
    summary = Path(source.summary_path)
    return DatabentoBudgetConfig(
        budget_start=source.budget_start,
        hard_cap_usd=source.hard_cap_usd,
        safety_ceiling_usd=source.safety_ceiling_usd,
        ledger_path=str(ledger if ledger.is_absolute() else project / ledger),
        summary_path=str(summary if summary.is_absolute() else project / summary),
    )


def _committed_spend(budget: DatabentoBudgetConfig) -> tuple[float, float]:
    rows = read_ledger(budget.ledger_path)
    actual = math.fsum(float(row.get("actual_cost_usd") or 0.0) for row in rows)
    completed = {
        str(row.get("request_id") or "")
        for row in rows
        if row.get("download_status") == "DOWNLOADED"
    }
    outstanding: dict[str, float] = {}
    for row in rows:
        request_id = str(row.get("request_id") or "")
        if (
            request_id
            and row.get("download_status") == "ESTIMATED_ONLY"
            and request_id not in completed
        ):
            outstanding[request_id] = max(
                outstanding.get(request_id, 0.0),
                float(row.get("estimated_cost_usd") or 0.0),
            )
    return actual, math.fsum(outstanding.values())


def _bundle_ledger_state(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
    inventory_hash: str | None = None,
) -> str:
    rows = [
        row
        for row in read_ledger(budget.ledger_path)
        if row.get("request_id") == bundle_id
    ]
    if not rows:
        return "NONE"
    if len(rows) not in {1, 2}:
        raise SoybeanCrushAcquisitionError("spend journal cardinality drift")
    reservation = rows[0]
    if (
        reservation.get("download_status") != "ESTIMATED_ONLY"
        or reservation.get("dataset") != DATASET
        or reservation.get("symbols") != list(SYMBOLS)
        or not math.isclose(
            float(reservation.get("estimated_cost_usd") or 0.0),
            estimate,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or reservation.get("actual_cost_usd") is not None
        or reservation.get("resulting_file") is not None
        or reservation.get("checksum") is not None
    ):
        raise SoybeanCrushAcquisitionError("spend reservation drift")
    if len(rows) == 1:
        return "RESERVED"
    completion = rows[1]
    if (
        completion.get("download_status") != "DOWNLOADED"
        or completion.get("dataset") != DATASET
        or completion.get("symbols") != list(SYMBOLS)
        or not math.isclose(
            float(completion.get("estimated_cost_usd") or 0.0),
            0.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not math.isclose(
            float(completion.get("actual_cost_usd") or 0.0),
            estimate,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or not completion.get("checksum")
        or (
            inventory_hash is not None
            and completion.get("checksum") != inventory_hash
        )
    ):
        raise SoybeanCrushAcquisitionError("spend completion drift")
    return "COMPLETED"


def _reserve_spend_once(
    budget: DatabentoBudgetConfig, *, bundle_id: str, estimate: float
) -> None:
    if _bundle_ledger_state(budget, bundle_id=bundle_id, estimate=estimate) != "NONE":
        return
    actual, outstanding = _committed_spend(budget)
    effective_cap = min(budget.hard_cap_usd, budget.safety_ceiling_usd)
    projected = actual + outstanding + estimate
    if projected > effective_cap + 1e-12:
        raise SoybeanCrushAcquisitionError("spend reservation exceeds authority")
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema="ohlcv-1m+definition",
            symbols=list(SYMBOLS),
            stype_in=STYPE_IN,
            start=START,
            end=END,
            estimated_cost_usd=estimate,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier="TIER_H_FROZEN_EXPLORATION_INPUT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=None,
            checksum=None,
            download_status="ESTIMATED_ONLY",
        ),
    )


def _complete_spend_once(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimate: float,
    inventory: list[dict[str, Any]],
) -> None:
    inventory_hash = stable_hash(inventory)
    state = _bundle_ledger_state(
        budget,
        bundle_id=bundle_id,
        estimate=estimate,
        inventory_hash=inventory_hash,
    )
    if state == "COMPLETED":
        return
    if state != "RESERVED":
        raise SoybeanCrushAcquisitionError("completion lacks durable spend reservation")
    actual, outstanding = _committed_spend(budget)
    effective_cap = min(budget.hard_cap_usd, budget.safety_ceiling_usd)
    if actual + outstanding > effective_cap + 1e-12:
        raise SoybeanCrushAcquisitionError("completion exceeds committed authority")
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=bundle_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema="ohlcv-1m+definition",
            symbols=list(SYMBOLS),
            stype_in=STYPE_IN,
            start=START,
            end=END,
            estimated_cost_usd=0.0,
            actual_cost_usd=estimate,
            cumulative_estimated_spend_usd=actual + outstanding,
            cumulative_actual_spend_usd=actual + estimate,
            cache_hit=False,
            research_purpose=PURPOSE,
            candidate_tier="TIER_H_FROZEN_EXPLORATION_INPUT",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(inventory[0]["path"]),
            checksum=inventory_hash,
            download_status="DOWNLOADED",
        ),
    )


def _record_access_roles_once(path: Path, *, bundle_id: str) -> None:
    for role in TEMPORAL_ROLES:
        marker = f"{bundle_id}:{role['role']}"
        rows = _jsonl(path)
        matching = [
            row for row in rows if marker in set(row.get("candidate_ids") or ())
        ]
        if len(matching) > 1:
            raise SoybeanCrushAcquisitionError("duplicate data-access role")
        expected_role = (
            DataRole.DEVELOPMENT
            if role["role"] == "DISCOVERY"
            else DataRole.BLIND_VALIDATION
        )
        if matching:
            row = matching[0]
            if (
                row.get("period_accessed") != f"{role['start']}:{role['end']}"
                or row.get("data_role") != expected_role.value
                or row.get("freeze_manifest_hash") != EXPECTED_MANIFEST_HASH
            ):
                raise SoybeanCrushAcquisitionError("existing data-access role drift")
            continue
        enforce_data_access(
            period=f"{role['start']}:{role['end']}",
            role=expected_role,
            requesting_module="scripts.acquire_soybean_crush_structural_value_router",
            candidate_ids=[
                "SOYBEAN_CRUSH_STRUCTURAL_VALUE_ROUTER_V1",
                bundle_id,
                marker,
            ],
            reason=f"{PURPOSE}; frozen role={role['role']}",
            freeze_manifest_hash=EXPECTED_MANIFEST_HASH,
            ledger_path=str(path),
        )


def _load_existing_receipt(
    path: Path,
    *,
    bundle_id: str,
    manifest: Mapping[str, Any],
    budget: DatabentoBudgetConfig,
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SoybeanCrushAcquisitionError("existing receipt invalid") from exc
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        not claimed
        or stable_hash(core) != claimed
        or receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("bundle_id") != bundle_id
        or receipt.get("manifest_hash") != manifest["manifest_hash"]
        or receipt.get("requests") != {schema: _request(schema) for schema in SCHEMAS}
        or receipt.get("chronological_roles")
        != [dict(row) for row in TEMPORAL_ROLES]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("q4_access_count_delta") != 0
        or receipt.get("broker_connections") != 0
        or receipt.get("orders") != 0
        or receipt.get("economic_outcomes_read") != 0
    ):
        raise SoybeanCrushAcquisitionError("existing receipt semantic drift")

    expected_paths = {
        "RAW_DBN_OHLCV_1M": paths["raw_ohlcv"],
        "RAW_DBN_DEFINITION": paths["raw_definition"],
        "CONTINUOUS_SYMBOLOGY": paths["symbology"],
    }
    rows = list(receipt.get("files") or ())
    if (
        len(rows) != len(expected_paths)
        or {str(row.get("kind")) for row in rows} != set(expected_paths)
        or receipt.get("inventory_hash") != stable_hash(rows)
    ):
        raise SoybeanCrushAcquisitionError("existing receipt inventory drift")
    for row in rows:
        artifact = Path(str(row.get("path") or "")).resolve()
        expected = expected_paths[str(row["kind"])].resolve()
        if (
            artifact != expected
            or not artifact.is_file()
            or artifact.stat().st_size != int(row.get("size_bytes") or -1)
            or sha256_file(artifact) != str(row.get("sha256") or "")
        ):
            raise SoybeanCrushAcquisitionError("sealed acquisition artifact drift")
    if _bundle_ledger_state(
        budget,
        bundle_id=bundle_id,
        estimate=EXPECTED_TOTAL_COST_USD,
        inventory_hash=str(receipt["inventory_hash"]),
    ) != "COMPLETED":
        raise SoybeanCrushAcquisitionError("sealed spend journal incomplete")
    for role in TEMPORAL_ROLES:
        marker = f"{bundle_id}:{role['role']}"
        matching = [
            row
            for row in _jsonl(paths["access_ledger"])
            if marker in set(row.get("candidate_ids") or ())
        ]
        if len(matching) != 1:
            raise SoybeanCrushAcquisitionError("sealed access role cardinality drift")
    return receipt


def _paths(
    project: Path,
    *,
    bundle_id: str,
    receipt_path: str | Path,
    cache_root: str | Path,
    access_ledger_path: str | Path,
    local_lock_path: str | Path,
    global_lock_path: str | Path,
) -> dict[str, Path]:
    cache = _resolve(project, cache_root) / bundle_id
    return {
        "raw_ohlcv": cache / "raw_ohlcv.dbn.zst",
        "raw_definition": cache / "raw_definition.dbn.zst",
        "symbology": cache / "continuous_symbology.json",
        "receipt": _resolve(project, receipt_path),
        "access_ledger": _resolve(project, access_ledger_path),
        "local_lock": _resolve(project, local_lock_path),
        "global_lock": _resolve(project, global_lock_path),
    }


def _resolve(project: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project / path


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
        description="Estimate or explicitly acquire frozen ZM/ZL soybean-crush inputs"
    )
    parser.add_argument("--root", default=".")
    parser.add_argument("--manifest", default=MANIFEST_PATH)
    parser.add_argument("--receipt", default=DEFAULT_RECEIPT)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform the governed purchase; default is official metadata only",
    )
    args = parser.parse_args()
    key = load_api_key()
    if not key:
        raise SoybeanCrushAcquisitionError(
            "DATABENTO_API_KEY is required for official metadata verification"
        )
    client = _import_databento().Historical(key)
    result = estimate_or_acquire(
        root=args.root,
        client=client,
        execute=args.execute,
        manifest_path=args.manifest,
        receipt_path=args.receipt,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
