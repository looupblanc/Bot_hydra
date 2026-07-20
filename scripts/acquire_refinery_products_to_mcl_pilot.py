#!/usr/bin/env python3
"""Governed one-shot RB/HO acquisition for the refinery-products MCL pilot."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
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
    _optional_lock,
)


MANIFEST = Path("config/research/refinery_products_to_mcl_pilot_v1.json")
RECEIPT = Path("reports/data_access/refinery_products_to_mcl_acquisition_receipt.json")
LOCAL_LOCK = Path("reports/data_access/refinery_products_to_mcl_acquisition.lock")
# Confirmation and exploration purchases deliberately serialize on this lock.
GLOBAL_LOCK = Path("reports/data_access/fresh_confirmation_0035_acquisition.lock")
CACHE_ROOT = Path("data/cache/databento/refinery_products_to_mcl")
ACCESS_LEDGER = Path("reports/data_access/data_access_ledger.jsonl")
PURPOSE = (
    "bounded pre-Q4 RB+HO refinery-products causal tripwire to MCL; "
    "development roles frozen before outcomes; no broker, orders, Q4, XFA or promotion"
)


class RefineryAcquisitionError(RuntimeError):
    pass


def _read_manifest(root: Path) -> dict[str, Any]:
    path = root / MANIFEST
    manifest = json.loads(path.read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise RefineryAcquisitionError("frozen manifest hash drift")
    data = manifest["data_contract"]
    if (
        data["end_exclusive"] != "2024-10-01"
        or data.get("q4_access") is not False
        or data["symbols"] != ["RB.c.0", "HO.c.0"]
    ):
        raise RefineryAcquisitionError("data contract drift")
    for row in manifest["frozen_inputs"].values():
        artifact = root / row["path"]
        if not artifact.is_file() or sha256_file(artifact) != row["sha256"]:
            raise RefineryAcquisitionError("frozen input drift")
    if manifest["governance"].get("maximum_cpu_workers") != 1:
        raise RefineryAcquisitionError("worker contract drift")
    return manifest


def _requests(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    data = manifest["data_contract"]
    common = {
        "dataset": data["dataset"],
        "symbols": list(data["symbols"]),
        "stype_in": data["stype_in"],
        "start": data["start"],
        "end": data["end_exclusive"],
    }
    return {schema: {**common, "schema": schema} for schema in data["schemas"]}


def _live_estimates(client: Any, manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    expected = manifest["data_contract"]["official_estimates"]
    for schema, request in _requests(manifest).items():
        row = {
            "record_count": int(client.metadata.get_record_count(**request)),
            "billable_size_bytes": int(client.metadata.get_billable_size(**request)),
            "estimated_cost_usd": float(client.metadata.get_cost(**request)),
        }
        frozen = expected[schema]
        if (
            row["record_count"] != int(frozen["record_count"])
            or row["billable_size_bytes"] != int(frozen["billable_size_bytes"])
            or not math.isclose(
                row["estimated_cost_usd"],
                float(frozen["estimated_cost_usd"]),
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            raise RefineryAcquisitionError(f"official estimate drift: {schema}")
        result[schema] = row
    return result


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _schema_request_id(bundle_id: str, schema: str) -> str:
    return request_id_for({"bundle_id": bundle_id, "schema": schema})


def _append_access_once(root: Path, manifest: dict[str, Any], bundle_id: str) -> None:
    path = root / ACCESS_LEDGER
    existing = []
    if path.is_file():
        existing = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    for role in manifest["chronological_roles"]:
        marker = f"{bundle_id}:{role['role']}"
        matches = [row for row in existing if marker in set(row.get("candidate_ids") or ())]
        if len(matches) > 1:
            raise RefineryAcquisitionError("duplicate access-ledger role")
        if matches:
            continue
        data_role = DataRole.DEVELOPMENT if role["role"] == "DISCOVERY" else DataRole.BLIND_VALIDATION
        enforce_data_access(
            period=f"{role['start']}:{role['end']}",
            role=data_role,
            requesting_module="scripts.acquire_refinery_products_to_mcl_pilot",
            candidate_ids=[manifest["branch_id"], bundle_id, marker],
            reason=f"{PURPOSE}; frozen role={role['role']}",
            freeze_manifest_hash=manifest["manifest_hash"],
            ledger_path=str(path),
        )


def acquire(root: Path, *, execute: bool) -> dict[str, Any]:
    root = root.resolve()
    manifest = _read_manifest(root)
    requests = _requests(manifest)
    bundle_id = request_id_for({"manifest_hash": manifest["manifest_hash"], "requests": requests})
    base = root / CACHE_ROOT / bundle_id
    files = {
        "ohlcv-1m": base / "raw_ohlcv.dbn.zst",
        "definition": base / "raw_definition.dbn.zst",
    }
    receipt_path = root / RECEIPT
    if receipt_path.is_file():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        core = dict(receipt)
        claimed = str(core.pop("receipt_hash", ""))
        if stable_hash(core) != claimed or receipt.get("bundle_id") != bundle_id:
            raise RefineryAcquisitionError("existing receipt drift")
        for row in receipt["files"]:
            artifact = Path(row["path"])
            if not artifact.is_file() or sha256_file(artifact) != row["sha256"]:
                raise RefineryAcquisitionError("raw artifact drift")
        return receipt

    key = load_api_key()
    if not key:
        raise RefineryAcquisitionError("DATABENTO_API_KEY unavailable")
    client = _import_databento().Historical(key)
    estimates = _live_estimates(client, manifest)
    total = sum(row["estimated_cost_usd"] for row in estimates.values())
    budget = DatabentoBudgetConfig()
    _estimated_before, actual_before = cumulative_spend(root / budget.ledger_path)
    if actual_before + total > min(budget.hard_cap_usd, budget.safety_ceiling_usd) + 1e-12:
        raise RefineryAcquisitionError("authoritative budget exceeded")
    plan = {
        "schema": "hydra_refinery_products_to_mcl_acquisition_plan_v1",
        "bundle_id": bundle_id,
        "manifest_hash": manifest["manifest_hash"],
        "requests": requests,
        "official_estimates": estimates,
        "official_total_cost_usd": total,
        "cumulative_actual_before_usd": actual_before,
        "projected_actual_usd": actual_before + total,
        "remaining_after_usd": budget.hard_cap_usd - actual_before - total,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    if not execute:
        return {**plan, "download_status": "DRY_RUN_ONLY"}

    # Both locks are non-blocking and fail closed.  The global confirmation
    # lock prevents a stale budget/ledger append against the W2018 purchase.
    with _optional_lock(root / GLOBAL_LOCK, enabled=True):
        with _optional_lock(root / LOCAL_LOCK, enabled=True):
            _estimated_now, actual_now = cumulative_spend(root / budget.ledger_path)
            if actual_now + total > min(budget.hard_cap_usd, budget.safety_ceiling_usd) + 1e-12:
                raise RefineryAcquisitionError("concurrent spend exhausted authority")
            network: dict[str, Any] = {}
            for schema, request in requests.items():
                network[schema] = _download_once(
                    client, request, files[schema], stype_out="instrument_id"
                )
            file_rows = [
                {
                    "kind": schema,
                    "path": str(path.resolve()),
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
                for schema, path in files.items()
            ]
            ledger = read_ledger(root / budget.ledger_path)
            prefix = 0.0
            for row in file_rows:
                schema = str(row["kind"])
                request_id = _schema_request_id(bundle_id, schema)
                matches = [item for item in ledger if item.get("request_id") == request_id]
                if matches:
                    raise RefineryAcquisitionError("pre-existing spend row without sealed receipt")
                cost = float(estimates[schema]["estimated_cost_usd"])
                prefix += cost
                append_spend_record(
                    DatabentoBudgetConfig(
                        ledger_path=str(root / budget.ledger_path),
                        summary_path=str(root / budget.summary_path),
                    ),
                    DatabentoSpendRecord(
                        request_id=request_id,
                        timestamp_utc=utc_now(),
                        dataset=manifest["data_contract"]["dataset"],
                        schema=schema,
                        symbols=list(manifest["data_contract"]["symbols"]),
                        stype_in="continuous+instrument_id",
                        start=manifest["data_contract"]["start"],
                        end=manifest["data_contract"]["end_exclusive"],
                        estimated_cost_usd=cost,
                        actual_cost_usd=cost,
                        cumulative_estimated_spend_usd=actual_now + prefix,
                        cumulative_actual_spend_usd=actual_now + prefix,
                        cache_hit=False,
                        research_purpose=PURPOSE,
                        candidate_tier="TIER_H_EXPLORATION_INPUT",
                        approval_mode=AUTO_UNDER_HARD_CAP,
                        resulting_file=str(row["path"]),
                        checksum=str(row["sha256"]),
                        download_status="DOWNLOADED",
                    ),
                )
            _append_access_once(root, manifest, bundle_id)
            core = {
                **plan,
                "schema": "hydra_refinery_products_to_mcl_acquisition_receipt_v1",
                "download_status": "DOWNLOADED",
                "files": file_rows,
                "network_requests": network,
                "raw_immutable": True,
                "completed_at_utc": utc_now(),
            }
            receipt = {**core, "receipt_hash": stable_hash(core)}
            _atomic_json(receipt_path, receipt)
            return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    print(json.dumps(acquire(Path(args.project_root), execute=args.execute), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
