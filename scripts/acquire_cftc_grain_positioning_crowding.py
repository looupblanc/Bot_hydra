#!/usr/bin/env python3
"""Acquire the frozen, free CFTC grain positioning input exactly once."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import request_id_for, sha256_file, utc_now
from hydra.economic_evolution.schema import stable_hash
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access
from scripts.acquire_fresh_confirmation_0035 import _optional_lock


MANIFEST = Path("config/research/cftc_grain_positioning_crowding_tripwire_v1.json")
RECEIPT = Path("reports/data_access/cftc_grain_positioning_crowding_receipt.json")
LOCAL_LOCK = Path("reports/data_access/cftc_grain_positioning_crowding.lock")
GLOBAL_LOCK = Path("reports/data_access/fresh_confirmation_0035_acquisition.lock")
CACHE_ROOT = Path("data/cache/cftc/grain_positioning_crowding")
ACCESS_LEDGER = Path("reports/data_access/data_access_ledger.jsonl")
PURPOSE = (
    "free official CFTC disaggregated Futures-Only grain positioning tripwire; "
    "roles and delayed availability frozen before position outcomes were downloaded; "
    "no Databento spend, Q4, broker, orders, account replay, or promotion"
)


class CFTCAcquisitionError(RuntimeError):
    pass


def _read_manifest(root: Path) -> dict[str, Any]:
    manifest = json.loads((root / MANIFEST).read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise CFTCAcquisitionError("frozen manifest hash drift")
    data = manifest["cftc_data_contract"]
    if (
        data["dataset_id"] != "rxbv-e226"
        or data["futonly_or_combined"] != "FutOnly"
        or data["end_inclusive"] != "2024-09-24"
        or data["q4_2024_access"] is not False
        or manifest["governance"]["new_databento_spend_usd"] != 0.0
        or manifest["governance"]["maximum_cpu_workers"] != 1
    ):
        raise CFTCAcquisitionError("data or governance contract drift")
    price = manifest["frozen_price_input"]
    for key in ("ohlcv", "definition"):
        path = root / price[f"{key}_path"]
        if not path.is_file() or sha256_file(path) != price[f"{key}_sha256"]:
            raise CFTCAcquisitionError(f"frozen {key} input drift")
    return manifest


def _query(manifest: dict[str, Any]) -> tuple[str, dict[str, str]]:
    data = manifest["cftc_data_contract"]
    codes = ",".join(f"'{code}'" for code in data["contract_market_codes"].values())
    where = (
        f"futonly_or_combined='{data['futonly_or_combined']}' AND "
        f"cftc_contract_market_code in({codes}) AND "
        f"report_date_as_yyyy_mm_dd between '{data['start']}T00:00:00.000' "
        f"and '{data['end_inclusive']}T23:59:59.999'"
    )
    params = {
        "$select": ",".join(data["selected_fields"]),
        "$where": where,
        "$order": "report_date_as_yyyy_mm_dd ASC,cftc_contract_market_code ASC",
        "$limit": "5000",
    }
    return data["endpoint"] + "?" + urllib.parse.urlencode(params), params


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _download(url: str, output: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "HYDRA-causal-research/1.0"},
        method="GET",
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status != 200:
                raise CFTCAcquisitionError(f"CFTC HTTP status {response.status}")
            temporary.write_bytes(response.read())
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def _audit_csv(path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    data = manifest["cftc_data_contract"]
    if len(rows) != int(data["expected_total_rows"]):
        raise CFTCAcquisitionError(f"unexpected CFTC row count: {len(rows)}")
    if set(rows[0]) != set(data["selected_fields"]):
        raise CFTCAcquisitionError("CFTC field inventory drift")
    codes = set(data["contract_market_codes"].values())
    if any(row["futonly_or_combined"] != "FutOnly" for row in rows):
        raise CFTCAcquisitionError("combined COT row leaked into Futures-Only input")
    if any(row["cftc_contract_market_code"] not in codes for row in rows):
        raise CFTCAcquisitionError("unexpected contract market code")
    counts = Counter(row["cftc_contract_market_code"] for row in rows)
    if set(counts.values()) != {int(data["expected_rows_per_market"])}:
        raise CFTCAcquisitionError(f"CFTC market coverage drift: {dict(counts)}")
    return {
        "row_count": len(rows),
        "row_count_by_contract_market_code": dict(sorted(counts.items())),
        "first_report_date": min(row["report_date_as_yyyy_mm_dd"] for row in rows),
        "last_report_date": max(row["report_date_as_yyyy_mm_dd"] for row in rows),
        "field_count": len(rows[0]),
    }


def _append_access_once(root: Path, manifest: dict[str, Any], bundle_id: str) -> None:
    path = root / ACCESS_LEDGER
    existing = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ] if path.is_file() else []
    for role in manifest["chronological_roles"]:
        marker = f"{bundle_id}:{role['role']}"
        matches = [row for row in existing if marker in set(row.get("candidate_ids") or ())]
        if len(matches) > 1:
            raise CFTCAcquisitionError("duplicate data-role ledger row")
        if matches:
            continue
        data_role = DataRole.DEVELOPMENT if role["role"] == "DISCOVERY" else DataRole.BLIND_VALIDATION
        enforce_data_access(
            period=f"{role['start']}:{role['end']}",
            role=data_role,
            requesting_module="scripts.acquire_cftc_grain_positioning_crowding",
            candidate_ids=[manifest["branch_id"], bundle_id, marker],
            reason=f"{PURPOSE}; frozen role={role['role']}",
            freeze_manifest_hash=manifest["manifest_hash"],
            ledger_path=str(path),
        )


def acquire(root: Path, *, execute: bool) -> dict[str, Any]:
    root = root.resolve()
    manifest = _read_manifest(root)
    url, params = _query(manifest)
    bundle_id = request_id_for({"manifest_hash": manifest["manifest_hash"], "query": params})
    output = root / CACHE_ROOT / bundle_id / "cftc_disaggregated_futures_only_grains.csv"
    receipt_path = root / RECEIPT
    if receipt_path.is_file():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        core = dict(receipt)
        claimed = str(core.pop("receipt_hash", ""))
        if stable_hash(core) != claimed or receipt.get("bundle_id") != bundle_id:
            raise CFTCAcquisitionError("existing receipt drift")
        if not output.is_file() or sha256_file(output) != receipt["raw_sha256"]:
            raise CFTCAcquisitionError("raw CFTC artifact drift")
        return receipt
    plan = {
        "schema": "hydra_cftc_grain_positioning_acquisition_plan_v1",
        "manifest_hash": manifest["manifest_hash"],
        "bundle_id": bundle_id,
        "endpoint": manifest["cftc_data_contract"]["endpoint"],
        "query_parameters": params,
        "output_path": str(output),
        "estimated_cost_usd": 0.0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    if not execute:
        return {**plan, "download_status": "DRY_RUN_ONLY"}
    with _optional_lock(root / GLOBAL_LOCK, enabled=True):
        with _optional_lock(root / LOCAL_LOCK, enabled=True):
            _download(url, output)
            audit = _audit_csv(output, manifest)
            _append_access_once(root, manifest, bundle_id)
            core = {
                **plan,
                "schema": "hydra_cftc_grain_positioning_acquisition_receipt_v1",
                "download_status": "DOWNLOADED",
                "retrieved_at_utc": utc_now(),
                "actual_cost_usd": 0.0,
                "raw_size_bytes": output.stat().st_size,
                "raw_sha256": sha256_file(output),
                "audit": audit,
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
