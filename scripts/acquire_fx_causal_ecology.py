#!/usr/bin/env python3
"""Governed acquisition for the frozen FX causal-ecology exploration pilot."""

from __future__ import annotations

import argparse
import hashlib
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
    enforce_budget,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.databento_loader import _import_databento, load_api_key
from hydra.economic_evolution.schema import stable_hash
from scripts.acquire_fresh_confirmation_0035 import _download_once, _optional_lock


MANIFEST = Path("config/research/fx_causal_ecology_pilot_v1.json")
CACHE_ROOT = Path("data/cache/databento/fx_causal_ecology")
RECEIPT = Path("reports/data_access/fx_causal_ecology_acquisition_receipt.json")
LOCK = Path("reports/data_access/fx_causal_ecology_acquisition.lock")
EXPECTED_COSTS = {"ohlcv-1m": 11.488325148821, "definition": 0.004829350859}
EXPECTED_RECORDS = {"ohlcv-1m": 3_146_810, "definition": 8_473}
MINIMUM_RESERVE_USD = 25.0


class FXAcquisitionError(RuntimeError):
    pass


def canonical_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def load_manifest(root: Path) -> dict[str, Any]:
    path = root / MANIFEST
    manifest = json.loads(path.read_text(encoding="utf-8"))
    claimed = str(manifest.get("manifest_hash") or "")
    core = dict(manifest)
    core.pop("manifest_hash", None)
    if claimed != canonical_hash(core):
        raise FXAcquisitionError("frozen manifest self-hash mismatch")
    data = manifest["data_contract"]
    if data["end_exclusive"] != "2024-10-01" or data.get("q4_access") is not False:
        raise FXAcquisitionError("Q4 boundary drift")
    return manifest


def requests_for(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    data = manifest["data_contract"]
    common = {
        "dataset": data["dataset"],
        "symbols": list(data["symbols"]),
        "stype_in": data["stype_in"],
        "start": data["start"],
        "end": data["end_exclusive"],
    }
    return {schema: {**common, "schema": schema} for schema in data["schemas"]}


def acquire(root: Path, *, execute: bool) -> dict[str, Any]:
    manifest = load_manifest(root)
    requests = requests_for(manifest)
    bundle_id = request_id_for(
        {"manifest_hash": manifest["manifest_hash"], "requests": requests}
    )
    base = root / CACHE_ROOT / bundle_id
    paths = {
        "ohlcv-1m": base / "raw_ohlcv.dbn.zst",
        "definition": base / "raw_definition.dbn.zst",
    }
    receipt_path = root / RECEIPT
    if receipt_path.is_file():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        core = dict(receipt)
        claimed = core.pop("receipt_hash", "")
        if claimed != stable_hash(core) or receipt.get("bundle_id") != bundle_id:
            raise FXAcquisitionError("existing acquisition receipt drift")
        for row in receipt.get("files", []):
            path = Path(row["path"])
            if not path.is_file() or sha256_file(path) != row["sha256"]:
                raise FXAcquisitionError("sealed raw artifact drift")
        return receipt

    db = _import_databento()
    key = load_api_key()
    if not key:
        raise FXAcquisitionError("DATABENTO_API_KEY unavailable")
    client = db.Historical(key)
    live_costs: dict[str, float] = {}
    live_records: dict[str, int] = {}
    for schema, request in requests.items():
        live_costs[schema] = float(client.metadata.get_cost(**request))
        live_records[schema] = int(client.metadata.get_record_count(**request))
        if not math.isclose(live_costs[schema], EXPECTED_COSTS[schema], abs_tol=1e-9):
            raise FXAcquisitionError(f"official {schema} cost drift")
        if live_records[schema] != EXPECTED_RECORDS[schema]:
            raise FXAcquisitionError(f"official {schema} record-count drift")
    total = sum(live_costs.values())
    budget = DatabentoBudgetConfig()
    projected_estimate, actual_before = enforce_budget(budget, total)
    if budget.hard_cap_usd - (actual_before + total) < MINIMUM_RESERVE_USD:
        raise FXAcquisitionError("purchase would violate the frozen USD 25 reserve")
    plan = {
        "schema": "hydra_fx_causal_ecology_acquisition_plan_v1",
        "bundle_id": bundle_id,
        "manifest_hash": manifest["manifest_hash"],
        "requests": requests,
        "official_costs_usd": live_costs,
        "official_records": live_records,
        "total_cost_usd": total,
        "actual_before_usd": actual_before,
        "projected_actual_usd": actual_before + total,
        "projected_estimated_usd": projected_estimate,
        "reserve_after_usd": budget.hard_cap_usd - (actual_before + total),
        "execute": execute,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    if not execute:
        return {**plan, "download_status": "DRY_RUN_ONLY"}

    with _optional_lock(root / LOCK, enabled=True):
        network = {}
        for schema, request in requests.items():
            network[schema] = _download_once(
                client, request, paths[schema], stype_out="instrument_id"
            )
        files = [
            {
                "kind": schema,
                "path": str(path.resolve()),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for schema, path in paths.items()
        ]
        # Re-read after download so another governed purchase cannot make this
        # append claim a stale cumulative total.
        _estimated_now, actual_now = cumulative_spend(root / budget.ledger_path)
        if budget.hard_cap_usd - (actual_now + total) < MINIMUM_RESERVE_USD:
            raise FXAcquisitionError("concurrent spend consumed the frozen reserve")
        append_spend_record(
            budget,
            DatabentoSpendRecord(
                request_id=bundle_id,
                timestamp_utc=utc_now(),
                dataset=manifest["data_contract"]["dataset"],
                schema="ohlcv-1m+definition",
                symbols=list(manifest["data_contract"]["symbols"]),
                stype_in="continuous+instrument_id",
                start=manifest["data_contract"]["start"],
                end=manifest["data_contract"]["end_exclusive"],
                estimated_cost_usd=total,
                actual_cost_usd=total,
                cumulative_estimated_spend_usd=projected_estimate,
                cumulative_actual_spend_usd=actual_now + total,
                cache_hit=False,
                research_purpose="frozen materially distinct FX causal ecology pilot; pre-Q4 only",
                candidate_tier="TIER_H_EXPLORATION_INPUT",
                approval_mode=AUTO_UNDER_HARD_CAP,
                resulting_file=files[0]["path"],
                checksum=stable_hash(files),
                download_status="DOWNLOADED",
            ),
        )
        core = {
            **plan,
            "schema": "hydra_fx_causal_ecology_acquisition_receipt_v1",
            "created_at_utc": utc_now(),
            "download_status": "DOWNLOADED",
            "network_requests": network,
            "files": files,
            "raw_immutable": True,
        }
        receipt = {**core, "receipt_hash": stable_hash(core)}
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = receipt_path.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, receipt_path)
        return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    result = acquire(Path(args.project_root).resolve(), execute=args.execute)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
