#!/usr/bin/env python3
"""Governed one-shot acquisition for the frozen CL term-structure tripwire.

The command is a metadata-only dry run unless ``--execute`` is supplied.  It
can acquire only CL.c.1 OHLCV-1m and definition records for the pre-Q4 frozen
interval.  It never downloads outcomes, changes the controller, or writes an
economic result.
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
SYMBOL = "CL.c.1"
FRONT_SYMBOL = "CL.c.0"
STYPE_IN = "continuous"
START = "2023-01-03"
END = "2024-10-01"
REQUEST_HASH = "ecf431248ddd87b2495c952b14a703e6f00005630f47ae402c364e7dff2fc260"
CARD_PATH = "config/research/cl_front_second_term_structure_tripwire_v1.json"
EXPECTED = {
    "ohlcv-1m": {
        "record_count": 556_341,
        "billable_size_bytes": 31_155_096,
        "estimated_cost_usd": 2.031081095338,
    },
    "definition": {
        "record_count": 548,
        "billable_size_bytes": 197_280,
        "estimated_cost_usd": 0.000312343240,
    },
}
TEMPORAL_ROLES = (
    {
        "role": "DISCOVERY",
        "start": "2023-01-03",
        "end": "2024-01-22",
        "fraction": 0.6,
        "candidate_modification_allowed": True,
    },
    {
        "role": "VALIDATION",
        "start": "2024-01-22",
        "end": "2024-05-28",
        "fraction": 0.2,
        "candidate_modification_allowed": False,
    },
    {
        "role": "FINAL_DEVELOPMENT",
        "start": "2024-05-28",
        "end": "2024-10-01",
        "fraction": 0.2,
        "candidate_modification_allowed": False,
    },
)
PURPOSE = (
    "one-shot CL front/second delivery term-structure development tripwire; "
    "pre-Q4 only; no outcome, broker, order, service, controller or DB access"
)
PLAN_SCHEMA = "hydra_cl_term_structure_acquisition_plan_v1"
RECEIPT_SCHEMA = "hydra_cl_term_structure_acquisition_receipt_v1"
SYMBOLOGY_SCHEMA = "hydra_cl_term_structure_symbology_v1"
ACCESS_LEDGER = "reports/data_access/data_access_ledger.jsonl"
DEFAULT_RECEIPT = "reports/data_access/cl_front_second_term_structure_acquisition_receipt.json"


class CLTermStructureAcquisitionError(RuntimeError):
    """The frozen request cannot be estimated or acquired safely."""


def canonical_bundle_request() -> dict[str, Any]:
    return {
        "schema": "hydra_cl_front_second_term_structure_cost_request_v1",
        "dataset": DATASET,
        "symbols": [SYMBOL],
        "stype_in": STYPE_IN,
        "start_inclusive": START,
        "end_exclusive": END,
        "schemas": ["ohlcv-1m", "definition"],
        "purchase": False,
        "q4_access": False,
    }


def frozen_contract(card_hash: str) -> dict[str, Any]:
    core = {
        "schema": "hydra_cl_front_second_term_structure_acquisition_contract_v1",
        "status": "FROZEN_AWAITING_EXPLICIT_EXECUTE",
        "decision_card_hash": card_hash,
        "bundle_request": canonical_bundle_request(),
        "request_hash": REQUEST_HASH,
        "api_requests": [_api_request(schema) for schema in EXPECTED],
        "official_estimates": EXPECTED,
        "temporal_roles": [dict(row) for row in TEMPORAL_ROLES],
        "symbology_policy": {
            "symbols": [FRONT_SYMBOL, SYMBOL],
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "coverage": "EXACT_HALF_OPEN_NO_GAP_NO_OVERLAP",
            "front_and_second_must_never_resolve_to_same_instrument": True,
        },
        "target_binding": {
            "market": "MCL.c.0",
            "fill": "NEXT_TRADABLE_OPEN",
            "roll_guard_true_sessions_each_side": 1,
        },
        "cumulative_hard_cap_usd": DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
        "q4_access_allowed": False,
        "protected_data_access_allowed": False,
        "broker_or_order_capability": False,
        "economic_outcome_downloaded": False,
    }
    return {**core, "contract_hash": stable_hash(core)}


def load_and_validate_card(root: Path, path: str | Path = CARD_PATH) -> dict[str, Any]:
    card_path = Path(path)
    if not card_path.is_absolute():
        card_path = root / card_path
    try:
        card = json.loads(card_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CLTermStructureAcquisitionError("frozen decision card unavailable") from exc
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise CLTermStructureAcquisitionError("decision card hash drift")
    request = dict(card["frozen_inputs"]["required_second_delivery_request"])
    embedded_hash = str(request.pop("request_hash", ""))
    if request != canonical_bundle_request() or embedded_hash != REQUEST_HASH:
        raise CLTermStructureAcquisitionError("decision card request drift")
    if stable_hash(request) != REQUEST_HASH:
        raise CLTermStructureAcquisitionError("canonical request hash drift")
    if card.get("chronological_roles") != [dict(row) for row in TEMPORAL_ROLES]:
        raise CLTermStructureAcquisitionError("chronological role drift")
    if card.get("governance", {}).get("q4_access_allowed") is not False:
        raise CLTermStructureAcquisitionError("decision card opens protected Q4")
    for binding in ("existing_front_and_execution", "existing_front_roll_map"):
        item = card["frozen_inputs"][binding]
        artifact = root / str(item["path"])
        if not artifact.is_file() or sha256_file(artifact) != str(item["sha256"]):
            raise CLTermStructureAcquisitionError(f"frozen input drift: {binding}")
    return card


def validate_frozen_contract(contract: Mapping[str, Any], card_hash: str) -> dict[str, Any]:
    supplied = dict(contract)
    claimed = str(supplied.pop("contract_hash", ""))
    if not claimed or stable_hash(supplied) != claimed:
        raise CLTermStructureAcquisitionError("acquisition contract hash drift")
    expected = frozen_contract(card_hash)
    if dict(contract) != expected:
        raise CLTermStructureAcquisitionError("frozen acquisition contract drift")
    roles = list(contract["temporal_roles"])
    if roles[0]["start"] != START or roles[-1]["end"] != END:
        raise CLTermStructureAcquisitionError("role coverage drift")
    if any(a["end"] != b["start"] for a, b in zip(roles, roles[1:])):
        raise CLTermStructureAcquisitionError("role gap or overlap")
    if END > "2024-10-01" or contract.get("q4_access_allowed") is not False:
        raise CLTermStructureAcquisitionError("request opens protected Q4")
    return dict(contract)


def estimate_or_acquire(
    *,
    root: str | Path,
    client: Any,
    execute: bool,
    budget: DatabentoBudgetConfig | None = None,
    card_path: str | Path = CARD_PATH,
    receipt_path: str | Path = DEFAULT_RECEIPT,
) -> dict[str, Any]:
    project = Path(root).resolve()
    card = load_and_validate_card(project, card_path)
    card_hash = str(card["card_hash"])
    contract = validate_frozen_contract(frozen_contract(card_hash), card_hash)
    bundle_id = request_id_for(
        {"request_hash": REQUEST_HASH, "card_hash": card_hash, "purpose": PURPOSE}
    )
    cfg = _bound_budget(project, budget)
    paths = _paths(project, bundle_id, receipt_path)

    with _optional_lock(paths["lock"], enabled=execute):
        if paths["receipt"].is_file():
            return _load_existing_receipt(
                paths["receipt"],
                bundle_id=bundle_id,
                contract_hash=str(contract["contract_hash"]),
                card_hash=card_hash,
                budget=cfg,
                paths=paths,
            )

        symbology = _resolve_rank_symbology(client)
        estimates = _live_estimates(client)
        _verify_estimates(estimates)
        total_cost = sum(float(row["estimated_cost_usd"]) for row in estimates.values())
        base_estimated, base_actual = _spend_without_bundle(cfg, bundle_id)
        projected = base_estimated + total_cost
        if projected > cfg.hard_cap_usd + 1e-12 or projected > cfg.safety_ceiling_usd + 1e-12:
            raise CLTermStructureAcquisitionError("frozen request exceeds authoritative budget")

        plan = {
            "schema": PLAN_SCHEMA,
            "bundle_id": bundle_id,
            "contract_hash": contract["contract_hash"],
            "decision_card_hash": card_hash,
            "request_hash": REQUEST_HASH,
            "api_requests": contract["api_requests"],
            "official_estimates": estimates,
            "official_total_cost_usd": total_cost,
            "official_total_record_count": sum(int(row["record_count"]) for row in estimates.values()),
            "official_total_billable_bytes": sum(int(row["billable_size_bytes"]) for row in estimates.values()),
            "symbology": symbology,
            "temporal_roles": [dict(row) for row in TEMPORAL_ROLES],
            "cumulative_actual_before_usd": base_actual,
            "projected_cumulative_estimate_usd": projected,
            "authoritative_cumulative_cap_usd": cfg.hard_cap_usd,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "execute": bool(execute),
        }
        if not execute:
            return {
                **plan,
                "download_status": "DRY_RUN_ONLY",
                "market_data_downloaded": False,
                "files_written": 0,
            }

        _download_once(
            client,
            _api_request("ohlcv-1m"),
            paths["raw_ohlcv"],
            stype_out="instrument_id",
        )
        _download_once(
            client,
            _api_request("definition"),
            paths["raw_definition"],
            stype_out="instrument_id",
        )
        _persist_json_once(paths["symbology"], symbology)
        files = [
            _file_receipt("RAW_DBN_OHLCV_1M", paths["raw_ohlcv"]),
            _file_receipt("RAW_DBN_DEFINITION", paths["raw_definition"]),
            _file_receipt("SYMBOLOGY_RESOLUTION", paths["symbology"]),
        ]
        _record_spend_rows_once(
            cfg,
            bundle_id=bundle_id,
            estimates=estimates,
            files=files,
        )
        _record_access_roles_once(
            paths["access_ledger"], bundle_id=bundle_id, card_hash=card_hash
        )
        receipt_core = {
            **plan,
            "schema": RECEIPT_SCHEMA,
            "download_status": "DOWNLOADED",
            "market_data_downloaded": True,
            "files": files,
            "raw_immutable": True,
            "economic_outcomes_generated": False,
            "runtime_or_manifest_modified": False,
            "completed_at_utc": utc_now(),
        }
        receipt = {**receipt_core, "receipt_hash": stable_hash(receipt_core)}
        _persist_json_once(paths["receipt"], receipt)
        return receipt


def _api_request(schema: str) -> dict[str, Any]:
    return {
        "dataset": DATASET,
        "symbols": [SYMBOL],
        "schema": schema,
        "stype_in": STYPE_IN,
        "start": START,
        "end": END,
    }


def _live_estimates(client: Any) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for schema in EXPECTED:
        request = _api_request(schema)
        result[schema] = {
            "record_count": int(client.metadata.get_record_count(**request)),
            "billable_size_bytes": int(client.metadata.get_billable_size(**request)),
            "estimated_cost_usd": float(client.metadata.get_cost(**request)),
        }
    return result


def _verify_estimates(estimates: Mapping[str, Mapping[str, Any]]) -> None:
    if set(estimates) != set(EXPECTED):
        raise CLTermStructureAcquisitionError("official schema estimate missing")
    for schema, expected in EXPECTED.items():
        actual = estimates[schema]
        if int(actual.get("record_count", -1)) != int(expected["record_count"]):
            raise CLTermStructureAcquisitionError(f"official record-count drift: {schema}")
        if int(actual.get("billable_size_bytes", -1)) != int(expected["billable_size_bytes"]):
            raise CLTermStructureAcquisitionError(f"official billable-size drift: {schema}")
        if not math.isclose(
            float(actual.get("estimated_cost_usd", float("nan"))),
            float(expected["estimated_cost_usd"]),
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise CLTermStructureAcquisitionError(f"official cost drift: {schema}")


def _resolve_rank_symbology(client: Any) -> dict[str, Any]:
    response = client.symbology.resolve(
        dataset=DATASET,
        symbols=[FRONT_SYMBOL, SYMBOL],
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=START,
        end_date=END,
    )
    raw = dict(response.get("result") or {})
    if set(raw) != {FRONT_SYMBOL, SYMBOL}:
        raise CLTermStructureAcquisitionError("continuous symbology incomplete")
    normalized = {
        symbol: _normalize_intervals(list(raw[symbol]), label=symbol)
        for symbol in (FRONT_SYMBOL, SYMBOL)
    }
    boundaries = sorted(
        {row[key] for rows in normalized.values() for row in rows for key in ("d0", "d1")}
    )
    collisions: list[str] = []
    for boundary in boundaries[:-1]:
        front = _instrument_at(normalized[FRONT_SYMBOL], boundary)
        second = _instrument_at(normalized[SYMBOL], boundary)
        if front is None or second is None:
            raise CLTermStructureAcquisitionError("rank symbology interval mismatch")
        if front == second:
            collisions.append(boundary)
    if collisions:
        raise CLTermStructureAcquisitionError("front and second ranks resolve to same instrument")
    core = {
        "schema": SYMBOLOGY_SCHEMA,
        "dataset": DATASET,
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "continuous_mapping": normalized,
        "coverage": {
            symbol: {
                "start": START,
                "end": END,
                "interval_count": len(rows),
                "gap_count": 0,
                "overlap_count": 0,
            }
            for symbol, rows in normalized.items()
        },
        "cross_rank_boundary_interval_count": len(boundaries) - 1,
        "same_instrument_interval_count": 0,
        "q4_access_count_delta": 0,
    }
    return {**core, "mapping_hash": stable_hash(core)}


def _normalize_intervals(rows: list[Any], *, label: str) -> list[dict[str, str]]:
    normalized = [
        {"s": str(dict(row)["s"]), "d0": str(dict(row)["d0"])[:10], "d1": str(dict(row)["d1"])[:10]}
        for row in rows
        if max(str(dict(row)["d0"])[:10], START) < min(str(dict(row)["d1"])[:10], END)
    ]
    normalized = [
        {**row, "d0": max(row["d0"], START), "d1": min(row["d1"], END)}
        for row in normalized
    ]
    normalized.sort(key=lambda row: (row["d0"], row["d1"], row["s"]))
    if not normalized or normalized[0]["d0"] != START or normalized[-1]["d1"] != END:
        raise CLTermStructureAcquisitionError(f"symbology boundary drift: {label}")
    for index, row in enumerate(normalized):
        if not row["s"] or row["d0"] >= row["d1"]:
            raise CLTermStructureAcquisitionError(f"invalid symbology interval: {label}")
        if index and normalized[index - 1]["d1"] != row["d0"]:
            raise CLTermStructureAcquisitionError(f"symbology gap or overlap: {label}")
    return normalized


def _instrument_at(rows: list[Mapping[str, str]], boundary: str) -> str | None:
    return next(
        (str(row["s"]) for row in rows if str(row["d0"]) <= boundary < str(row["d1"])),
        None,
    )


def _bound_budget(root: Path, budget: DatabentoBudgetConfig | None) -> DatabentoBudgetConfig:
    cfg = budget or DatabentoBudgetConfig()
    if (
        cfg.hard_cap_usd > DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD
        or cfg.safety_ceiling_usd > DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD
        or cfg.safety_ceiling_usd > cfg.hard_cap_usd
    ):
        raise CLTermStructureAcquisitionError("budget exceeds authoritative authority")
    ledger = Path(cfg.ledger_path)
    summary = Path(cfg.summary_path)
    return DatabentoBudgetConfig(
        budget_start=cfg.budget_start,
        hard_cap_usd=cfg.hard_cap_usd,
        safety_ceiling_usd=cfg.safety_ceiling_usd,
        ledger_path=str(ledger if ledger.is_absolute() else root / ledger),
        summary_path=str(summary if summary.is_absolute() else root / summary),
    )


def _spend_without_bundle(
    budget: DatabentoBudgetConfig, bundle_id: str
) -> tuple[float, float]:
    request_ids = {_schema_request_id(bundle_id, schema) for schema in EXPECTED}
    estimated = 0.0
    actual = 0.0
    for row in read_ledger(budget.ledger_path):
        if row.get("request_id") in request_ids:
            continue
        if row.get("download_status") in {"ESTIMATED_ONLY", "DOWNLOADED", "CACHE_HIT"}:
            estimated += float(row.get("estimated_cost_usd") or 0.0)
        actual += float(row.get("actual_cost_usd") or 0.0)
    return estimated, actual


def _record_spend_rows_once(
    budget: DatabentoBudgetConfig,
    *,
    bundle_id: str,
    estimates: Mapping[str, Mapping[str, Any]],
    files: list[Mapping[str, Any]],
) -> None:
    file_by_schema = {
        "ohlcv-1m": next(row for row in files if row["kind"] == "RAW_DBN_OHLCV_1M"),
        "definition": next(row for row in files if row["kind"] == "RAW_DBN_DEFINITION"),
    }
    base_estimated, base_actual = _spend_without_bundle(budget, bundle_id)
    estimated_prefix = 0.0
    actual_prefix = 0.0
    for schema in EXPECTED:
        estimate = float(estimates[schema]["estimated_cost_usd"])
        estimated_prefix += estimate
        actual_prefix += estimate
        request_id = _schema_request_id(bundle_id, schema)
        matching = [row for row in read_ledger(budget.ledger_path) if row.get("request_id") == request_id]
        if len(matching) > 1:
            raise CLTermStructureAcquisitionError("duplicate spend ledger row")
        artifact = file_by_schema[schema]
        if matching:
            row = matching[0]
            if (
                row.get("download_status") != "DOWNLOADED"
                or row.get("schema") != schema
                or row.get("checksum") != artifact["sha256"]
                or not math.isclose(float(row.get("actual_cost_usd") or 0.0), estimate, abs_tol=1e-12)
            ):
                raise CLTermStructureAcquisitionError("existing spend ledger row drift")
            continue
        append_spend_record(
            budget,
            DatabentoSpendRecord(
                request_id=request_id,
                timestamp_utc=utc_now(),
                dataset=DATASET,
                schema=schema,
                symbols=[SYMBOL],
                stype_in=STYPE_IN,
                start=START,
                end=END,
                estimated_cost_usd=estimate,
                actual_cost_usd=estimate,
                cumulative_estimated_spend_usd=base_estimated + estimated_prefix,
                cumulative_actual_spend_usd=base_actual + actual_prefix,
                cache_hit=False,
                research_purpose=PURPOSE,
                candidate_tier="TIER_H_FROZEN_TRIPWIRE_INPUT",
                approval_mode=AUTO_UNDER_HARD_CAP,
                resulting_file=str(artifact["path"]),
                checksum=str(artifact["sha256"]),
                download_status="DOWNLOADED",
            ),
        )


def _record_access_roles_once(path: Path, *, bundle_id: str, card_hash: str) -> None:
    for role in TEMPORAL_ROLES:
        marker = f"{bundle_id}:{role['role']}"
        rows = _jsonl(path)
        matching = [row for row in rows if marker in set(row.get("candidate_ids") or ())]
        if len(matching) > 1:
            raise CLTermStructureAcquisitionError("duplicate data-access role")
        expected_role = DataRole.DEVELOPMENT if role["role"] == "DISCOVERY" else DataRole.BLIND_VALIDATION
        if matching:
            row = matching[0]
            if (
                row.get("data_role") != expected_role.value
                or row.get("parameters_mutable") != (expected_role == DataRole.DEVELOPMENT)
                or row.get("freeze_manifest_hash") != card_hash
            ):
                raise CLTermStructureAcquisitionError("existing data-access role drift")
            continue
        enforce_data_access(
            period=f"{role['start']}:{role['end']}",
            role=expected_role,
            requesting_module="scripts.acquire_cl_front_second_term_structure_tripwire",
            candidate_ids=["CL_FRONT_SECOND_TERM_STRUCTURE_TRIPWIRE_V1", bundle_id, marker],
            reason=f"{PURPOSE}; frozen economic role={role['role']}",
            freeze_manifest_hash=card_hash,
            ledger_path=str(path),
        )


def _schema_request_id(bundle_id: str, schema: str) -> str:
    return request_id_for({"bundle_id": bundle_id, "request_hash": REQUEST_HASH, "schema": schema})


def _paths(root: Path, bundle_id: str, receipt_path: str | Path) -> dict[str, Path]:
    base = root / "data/cache/databento/cl_front_second_term_structure" / bundle_id
    receipt = Path(receipt_path)
    if not receipt.is_absolute():
        receipt = root / receipt
    return {
        "raw_ohlcv": base / "raw_ohlcv_1m.dbn.zst",
        "raw_definition": base / "raw_definition.dbn.zst",
        "symbology": base / "symbology_resolution.json",
        "receipt": receipt,
        "lock": root / "reports/data_access/cl_front_second_term_structure_acquisition.lock",
        "access_ledger": root / ACCESS_LEDGER,
    }


def _load_existing_receipt(
    path: Path,
    *,
    bundle_id: str,
    contract_hash: str,
    card_hash: str,
    budget: DatabentoBudgetConfig,
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CLTermStructureAcquisitionError("existing receipt is invalid") from exc
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("bundle_id") != bundle_id
        or receipt.get("contract_hash") != contract_hash
        or receipt.get("decision_card_hash") != card_hash
        or receipt.get("request_hash") != REQUEST_HASH
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("economic_outcomes_generated") is not False
    ):
        raise CLTermStructureAcquisitionError("existing receipt drift")
    for row in receipt.get("files", ()):
        artifact = Path(str(row.get("path") or ""))
        if not artifact.is_file() or sha256_file(artifact) != str(row.get("sha256") or ""):
            raise CLTermStructureAcquisitionError("sealed acquisition artifact drift")
    spend_ids = {_schema_request_id(bundle_id, schema) for schema in EXPECTED}
    spend = [row for row in read_ledger(budget.ledger_path) if row.get("request_id") in spend_ids]
    access = [
        row
        for row in _jsonl(paths["access_ledger"])
        if any(f"{bundle_id}:{role['role']}" in set(row.get("candidate_ids") or ()) for role in TEMPORAL_ROLES)
    ]
    if len(spend) != 2 or len(access) != 3:
        raise CLTermStructureAcquisitionError("sealed receipt ledger cardinality drift")
    return receipt


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate or acquire the frozen CL.c.1 term-structure input")
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default=CARD_PATH)
    parser.add_argument("--receipt", default=DEFAULT_RECEIPT)
    parser.add_argument("--execute", action="store_true", help="perform the one-shot download; default is metadata-only")
    args = parser.parse_args()
    key = load_api_key()
    if not key:
        raise CLTermStructureAcquisitionError("DATABENTO_API_KEY is required for official metadata verification")
    client = _import_databento().Historical(key)
    result = estimate_or_acquire(
        root=args.root,
        client=client,
        execute=bool(args.execute),
        card_path=args.card,
        receipt_path=args.receipt,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
