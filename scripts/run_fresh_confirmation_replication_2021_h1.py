#!/usr/bin/env python3
"""One bounded, immutable H1-2021 replication of the strongest clean Tier-G book.

The shared confirmation implementation is reused without changing it.  This
adapter only binds a different, previously unopened half-open date interval
and a singleton candidate inventory.  It never touches the mission database,
registry, controller, broker, or order paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import databento as db

from hydra.data.budget import DatabentoBudgetConfig, cumulative_spend
from hydra.data.databento_loader import load_api_key
from hydra.economic_evolution.schema import stable_hash
from hydra.production import fresh_confirmation_lane as lane
from scripts import acquire_fresh_confirmation_0035 as acquisition


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports/economic_evolution/fresh_confirmation_replication_2021_h1_v1"
CONTRACT_PATH = REPORT_DIR / "contract.json"
RECEIPT_PATH = REPORT_DIR / "acquisition_receipt.json"
FEATURE_RECEIPT_PATH = REPORT_DIR / "feature_receipt.json"
RESULT_PATH = REPORT_DIR / "economic_result.json"
DECISION_REPORT_PATH = REPORT_DIR / "decision_report.json"
SOURCE_CONTRACT_PATH = (
    ROOT
    / "reports/economic_evolution/autonomous_economic_discovery_director_0035_revision_02"
    / "branch_results/post_source_exhaustion/post_composite/fresh_confirmation_contract.json"
)
SOURCE_RESULT_PATH = SOURCE_CONTRACT_PATH.with_name("fresh_confirmation_result.json")
SOURCE_GRADUATION_PATH = SOURCE_CONTRACT_PATH.with_name("tier_g_development_graduation.json")
RECONCILIATION_PATH = (
    ROOT / "reports/economic_evolution/evidence_axis_reconciliation_v1/economic_result.json"
)
MANIFEST_PATH = ROOT / "config/v7/autonomous_economic_discovery_director_0035.json"
ACCESS_LEDGER = ROOT / "reports/data_access/data_access_ledger.jsonl"

CANDIDATE_ID = "hazard_19327ab34a21d623c654a6cc"
START = "2021-01-04"
END = "2021-07-01"
SYMBOLS = ("YM.c.0", "MYM.c.0", "ES.c.0")
ROOTS = ("YM", "MYM", "ES")
PURPOSE = (
    "one-shot untouched H1-2021 replication after immutable Tier-G "
    "requalification of hazard_19327ab34a21d623c654a6cc; no retuning"
)


class ReplicationError(RuntimeError):
    pass


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ReplicationError(f"JSON object required: {path}")
    return value


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    content = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != content:
            raise ReplicationError(f"refusing divergent rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def _patch_shared_adapters() -> None:
    """Bind dates only in this short-lived process; source files stay untouched."""

    lane.START = START
    lane.END = END
    lane.SYMBOLS = SYMBOLS
    lane.TIER_G_IDS = (CANDIDATE_ID,)
    acquisition.START = START
    acquisition.END = END
    acquisition.SYMBOLS = SYMBOLS
    acquisition.ROOTS = ROOTS
    acquisition.REQUEST_PURPOSE = PURPOSE
    acquisition.CANDIDATE_TIER = "TIER_G_REQUALIFIED_AWAITING_REPLICATION"


def _candidate_prior_result(source_result: Mapping[str, Any]) -> dict[str, Any]:
    inner = dict(source_result["fresh_confirmation_result"])
    row = next(
        dict(item)
        for item in inner["candidate_results"]
        if str(item["candidate_id"]) == CANDIDATE_ID
    )
    cell = next(
        dict(item) for item in row["cells"] if int(item["horizon_trading_days"]) == 20
    )
    return {
        "source_result_hash": str(inner["result_hash"]),
        "source_wrapper_result_hash": str(source_result["result_hash"]),
        "period": "2025-01-02:2025-07-01",
        "prior_decision": "TIER_C_GATE_FAILED",
        "matching_horizon_days": 20,
        "full_coverage_start_count": int(cell["full_coverage_start_count"]),
        "normal_pass_count": int(cell["normal"]["pass_count"]),
        "stressed_pass_count": int(cell["stressed"]["pass_count"]),
        "normal_net_usd": float(cell["normal"]["net_total_usd"]),
        "stressed_net_usd": float(cell["stressed"]["net_total_usd"]),
        "normal_mll_breach_rate": float(cell["normal"]["mll_breach_rate"]),
        "stressed_mll_breach_rate": float(cell["stressed"]["mll_breach_rate"]),
        "reclassified_role": "VIEWED_DEVELOPMENT_AFTER_FAILED_CONFIRMATION",
    }


def _assert_untouched() -> dict[str, Any]:
    hits: list[dict[str, Any]] = []
    if ACCESS_LEDGER.is_file():
        for line in ACCESS_LEDGER.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            period = str(row.get("period_accessed") or "")
            parts = period.split(":")
            if len(parts) < 2 or len(parts[0]) < 10 or len(parts[1]) < 10:
                continue
            try:
                left = date.fromisoformat(parts[0][:10])
                right = date.fromisoformat(parts[1][:10])
            except ValueError:
                continue
            if left < date.fromisoformat(END) and right > date.fromisoformat(START):
                text = json.dumps(row, sort_keys=True)
                if any(token in text for token in ("YM", "MYM", "hazard_", CANDIDATE_ID)):
                    hits.append(row)
    if hits:
        raise ReplicationError("H1-2021 YM/MYM candidate evidence is not untouched")
    return {
        "period": f"{START}:{END}",
        "relevant_access_ledger_hit_count": 0,
        "q4_overlap": False,
        "status": "GLOBALLY_UNTOUCHED_FOR_REQUIRED_MARKETS_AND_CANDIDATE",
    }


def _official_stats(client: Any) -> dict[str, Any]:
    continuous = client.symbology.resolve(
        dataset="GLBX.MDP3",
        symbols=list(SYMBOLS),
        stype_in="continuous",
        stype_out="instrument_id",
        start_date=START,
        end_date=END,
    )
    instrument_ids = sorted(
        {
            str(item["s"])
            for rows in dict(continuous.get("result") or {}).values()
            for item in rows
        },
        key=int,
    )
    requests = {
        "ohlcv-1m": {
            "dataset": "GLBX.MDP3",
            "schema": "ohlcv-1m",
            "symbols": list(SYMBOLS),
            "stype_in": "continuous",
            "start": START,
            "end": END,
        },
        "definition": {
            "dataset": "GLBX.MDP3",
            "schema": "definition",
            "symbols": instrument_ids,
            "stype_in": "instrument_id",
            "start": START,
            "end": END,
        },
    }
    parts: dict[str, Any] = {}
    for name, request in requests.items():
        parts[name] = {
            "estimated_cost_usd": float(client.metadata.get_cost(**request)),
            "record_count": int(client.metadata.get_record_count(**request)),
            "billable_size_bytes": int(client.metadata.get_billable_size(**request)),
        }
    return {
        "requests": requests,
        "instrument_ids": instrument_ids,
        "parts": parts,
        "total_estimated_cost_usd": sum(
            float(row["estimated_cost_usd"]) for row in parts.values()
        ),
        "total_record_count": sum(int(row["record_count"]) for row in parts.values()),
        "total_billable_size_bytes": sum(
            int(row["billable_size_bytes"]) for row in parts.values()
        ),
    }


def freeze(client: Any) -> dict[str, Any]:
    untouched = _assert_untouched()
    source_contract = _read(SOURCE_CONTRACT_PATH)
    source_contract_core = dict(source_contract)
    source_contract_hash = str(source_contract_core.pop("contract_hash"))
    if stable_hash(source_contract_core) != source_contract_hash:
        raise ReplicationError("source confirmation contract hash drift")
    source_result = _read(SOURCE_RESULT_PATH)
    graduation_wrapper = _read(SOURCE_GRADUATION_PATH)
    graduation = dict(graduation_wrapper["tier_g_development_graduation"])
    if stable_hash({k: v for k, v in graduation.items() if k != "result_hash"}) != str(
        graduation["result_hash"]
    ):
        raise ReplicationError("source graduation hash drift")
    graduated_book = next(
        dict(row)
        for row in graduation["graduated_development_books"]
        if str(row["candidate_id"]) == CANDIDATE_ID
    )
    reconciliation = _read(RECONCILIATION_PATH)
    if stable_hash({k: v for k, v in reconciliation.items() if k != "result_hash"}) != str(
        reconciliation["result_hash"]
    ):
        raise ReplicationError("evidence-axis reconciliation hash drift")
    clean = {
        str(row["candidate_id"]): dict(row)
        for row in reconciliation["tier_g_development_books"]
    }
    if CANDIDATE_ID not in clean:
        raise ReplicationError("candidate is not in the clean Tier-G inventory")
    candidate = next(
        dict(row)
        for row in source_contract["tier_g_candidates"]
        if str(row["candidate_id"]) == CANDIDATE_ID
    )
    clean_row = clean[CANDIDATE_ID]
    for key in ("graduation_evidence_hash", "candidate_id"):
        if str(candidate[key]) != str(clean_row[key]):
            raise ReplicationError(f"clean candidate binding drift: {key}")
    if int(candidate["selected_development_horizon_days"]) != 20:
        raise ReplicationError("matching confirmation horizon changed")

    stats = _official_stats(client)
    _estimated, current_actual = cumulative_spend(
        ROOT / "reports/data_budget/databento_spend_ledger.jsonl"
    )
    request_core = {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "data_role": "CONFIRMATION",
        "q4_2024_access_allowed": False,
        "broker_or_order_capability": False,
    }
    request = {
        **request_core,
        "request_hash": stable_hash(request_core),
        "frozen_estimated_cost_usd": stats["total_estimated_cost_usd"],
        "prior_cumulative_actual_usd": current_actual,
        "additional_authority_usd": 100.0,
        "cumulative_hard_cap_usd": 200.720719923081,
        "projected_cumulative_usd": current_actual + stats["total_estimated_cost_usd"],
    }
    core = {
        "schema": "hydra_fresh_confirmation_replication_contract_v1",
        "status": "FROZEN_AWAITING_ACQUISITION",
        "candidate_freeze": {
            "candidate_id": CANDIDATE_ID,
            "candidate_fingerprint": candidate["candidate_fingerprint"],
            "frozen_account_policy_hash": candidate["frozen_account_policy_hash"],
            "graduation_evidence_hash": candidate["graduation_evidence_hash"],
            "combine_book_hash": graduated_book["combine_book_hash"],
            "original_freeze_completed_at_utc": "2026-07-19T08:36:14.318387Z",
            "policy_mutated": False,
            "calibration_reused_without_recalibration": True,
        },
        "tier_g_candidates": [candidate],
        "data_request": request,
        "official_cost_matrix": stats,
        "untouched_audit": untouched,
        "data_partition": {
            "role": "CONFIRMATION",
            "replication_attempt": 2,
            "replication_classification": "SECOND_INDEPENDENT_REPLICATION_AFTER_FAILED_CONFIRMATION",
            "entire_post_warmup_block_consumed_once": True,
            "warmup_rule": "FIRST_5_COMMON_COMPLETE_SESSION_DAYS_EXCLUDED",
            "warmup_complete_sessions": 5,
            "evaluation_start_rule": "SIXTH_COMMON_COMPLETE_SESSION_DAY",
            "candidate_modification_allowed": False,
            "recalibration_allowed": False,
        },
        "account_replay_contract": source_contract["account_replay_contract"],
        "tier_c_gate": source_contract["tier_c_gate"],
        "official_rule_snapshot": source_contract["official_rule_snapshot"],
        "official_rule_snapshot_payload": source_contract["official_rule_snapshot_payload"],
        "requalification": {
            "status": "TIER_G_RETAINED_AFTER_FAILED_CONFIRMATION_BECAME_DEVELOPMENT",
            "policy_changed": False,
            "prior_confirmation": _candidate_prior_result(source_result),
            "source_evidence_axis_reconciliation_hash": reconciliation["result_hash"],
            "source_clean_graduation_evidence_hash": clean_row["graduation_evidence_hash"],
            "matching_horizon_days": 20,
            "new_confirmation_gate_changed": False,
            "combined_decision_policy": (
                "PRIOR_2025_FAILURE_REMAINS_EVIDENCE; SECOND_REPLICATION_MUST_PASS_"
                "UNCHANGED_H20_GATE_OR_CLOSE_OVERFIT_CONFIRMATION_FAILURE"
            ),
        },
        "source_contract_hash": source_contract_hash,
        "source_result_hash": source_result["result_hash"],
        "source_evidence_axis_reconciliation_hash": reconciliation["result_hash"],
        "source_manifest_hash": source_contract["source_manifest_hash"],
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "authoritative_writes": 0,
    }
    contract = {**core, "contract_hash": stable_hash(core)}
    _write_once(CONTRACT_PATH, contract)
    return contract


def acquire(client: Any, *, execute: bool) -> dict[str, Any]:
    _assert_untouched()
    contract = _read(CONTRACT_PATH)
    manifest = _read(MANIFEST_PATH)
    manifest_hash = str(manifest["manifest_hash"])
    _patch_shared_adapters()
    return acquisition.acquire_fresh_confirmation(
        contract=contract,
        manifest=manifest,
        expected_manifest_hash=manifest_hash,
        root=ROOT,
        client=client,
        execute=execute,
        budget=DatabentoBudgetConfig(),
        receipt_path=RECEIPT_PATH,
    )


def evaluate() -> dict[str, Any]:
    if RESULT_PATH.exists():
        raise ReplicationError("replication confirmation was already consumed")
    contract = _read(CONTRACT_PATH)
    receipt = _read(RECEIPT_PATH)
    _patch_shared_adapters()
    inputs = dict(receipt["feature_build_inputs"])
    feature_receipt = lane.build_confirmation_feature_bundles(
        contract,
        source_files=inputs["source_files"],
        contract_map_path=inputs["contract_map_path"],
        cache_root=inputs["cache_root"],
    )
    _write_once(FEATURE_RECEIPT_PATH, feature_receipt)
    matrices = lane.open_confirmation_matrices(feature_receipt)
    result = lane.evaluate_fresh_confirmation(
        contract,
        matrices=matrices,
        acquisition_receipt=receipt,
    )
    candidate = dict(result["candidate_results"][0])
    gate = dict(candidate["tier_c_gate"])
    core = {
        "schema": "hydra_fresh_confirmation_replication_result_v1",
        "status": "CONFIRMATION_REPLICATION_CONSUMED_ONCE",
        "decision": (
            "TIER_C_REPLICATION_PASSED"
            if gate["passed"]
            else "OVERFIT_CONFIRMATION_FAILURE_BRANCH_CLOSED"
        ),
        "candidate_id": CANDIDATE_ID,
        "contract_hash": contract["contract_hash"],
        "acquisition_receipt_hash": receipt["receipt_hash"],
        "feature_receipt_hash": feature_receipt["result_hash"],
        "confirmation": result,
        "prior_failed_confirmation_hash": contract["source_result_hash"],
        "retuning_performed": False,
        "recalibration_performed": False,
        "candidate_or_policy_mutated": False,
        "b1_b4_reused": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    wrapped = {**core, "result_hash": stable_hash(core)}
    _write_once(RESULT_PATH, wrapped)
    return wrapped


def seal() -> dict[str, Any]:
    contract = _read(CONTRACT_PATH)
    receipt = _read(RECEIPT_PATH)
    feature = _read(FEATURE_RECEIPT_PATH)
    result = _read(RESULT_PATH)
    for value, key in (
        (contract, "contract_hash"),
        (receipt, "receipt_hash"),
        (feature, "result_hash"),
        (result, "result_hash"),
    ):
        core = dict(value)
        claimed = str(core.pop(key))
        if stable_hash(core) != claimed:
            raise ReplicationError(f"sealed hash drift: {key}")

    reconciliation = _read(RECONCILIATION_PATH)
    source_contract = _read(SOURCE_CONTRACT_PATH)
    source_result = _read(SOURCE_RESULT_PATH)
    source_candidates = {
        str(row["candidate_id"]): dict(row)
        for row in source_contract["tier_g_candidates"]
    }
    prior_rows = {
        str(row["candidate_id"]): dict(row)
        for row in source_result["fresh_confirmation_result"]["candidate_results"]
    }
    clean_rows = {
        str(row["candidate_id"]): dict(row)
        for row in reconciliation["tier_g_development_books"]
    }
    audited: list[dict[str, Any]] = []
    for candidate_id in sorted(clean_rows):
        candidate = source_candidates[candidate_id]
        prior = prior_rows[candidate_id]
        prior_h20 = next(
            dict(row) for row in prior["cells"] if int(row["horizon_trading_days"]) == 20
        )
        audited.append(
            {
                "candidate_id": candidate_id,
                "candidate_fingerprint": candidate["candidate_fingerprint"],
                "frozen_account_policy_hash": candidate["frozen_account_policy_hash"],
                "graduation_evidence_hash": clean_rows[candidate_id]["graduation_evidence_hash"],
                "development_normal_pass_count": clean_rows[candidate_id]["normal_pass_count"],
                "development_stressed_pass_count": clean_rows[candidate_id]["stressed_pass_count"],
                "first_confirmation_2025_h1": {
                    "full_coverage_start_count": prior_h20["full_coverage_start_count"],
                    "normal_pass_count": prior_h20["normal"]["pass_count"],
                    "stressed_pass_count": prior_h20["stressed"]["pass_count"],
                    "normal_net_usd": prior_h20["normal"]["net_total_usd"],
                    "stressed_net_usd": prior_h20["stressed"]["net_total_usd"],
                    "normal_mll_breach_rate": prior_h20["normal"]["mll_breach_rate"],
                    "stressed_mll_breach_rate": prior_h20["stressed"]["mll_breach_rate"],
                    "tier_c_gate_passed": prior["tier_c_gate"]["passed"],
                },
                "second_confirmation_selected": candidate_id == CANDIDATE_ID,
                "terminal_confirmation_status": (
                    "OVERFIT_CONFIRMATION_FAILURE_BRANCH_CLOSED"
                    if candidate_id == CANDIDATE_ID
                    else "G_CONFIRMATION_FAILED_NO_SECOND_REPLICATION_SELECTED"
                ),
            }
        )

    confirmed = result["confirmation"]
    selected = dict(confirmed["candidate_results"][0])
    cells = {
        str(row["horizon_trading_days"]): dict(row) for row in selected["cells"]
    }
    _estimated, actual = cumulative_spend(
        ROOT / "reports/data_budget/databento_spend_ledger.jsonl"
    )
    core = {
        "schema": "hydra_fresh_confirmation_replication_decision_report_v1",
        "status": "SEALED_OVERFIT_CONFIRMATION_FAILURE_BRANCH_CLOSED",
        "economic_verdict": "NO_TIER_C_CANDIDATE_FROM_CLEAN_TIER_G_SET",
        "audited_clean_tier_g_count": len(audited),
        "audited_clean_tier_g": audited,
        "selected_candidate_id": CANDIDATE_ID,
        "second_replication": {
            "period": f"{START}:{END}",
            "account_label": "50K",
            "cells": cells,
            "tier_c_gate": selected["tier_c_gate"],
            "batch_stream_equal": selected["batch_stream_equal"],
            "emitted_intent_count": selected["emitted_intent_count"],
            "completed_event_count": selected["completed_event_count"],
        },
        "artifact_bindings": {
            "contract_hash": contract["contract_hash"],
            "acquisition_receipt_hash": receipt["receipt_hash"],
            "feature_receipt_hash": feature["result_hash"],
            "economic_result_hash": result["result_hash"],
            "raw_ohlcv_sha256": next(
                row["sha256"] for row in receipt["files"] if row["kind"] == "RAW_DBN_OHLCV"
            ),
            "normalized_parquet_sha256": next(
                row["sha256"]
                for row in receipt["files"]
                if row["kind"] == "NORMALIZED_PARQUET"
            ),
            "roll_map_hash": receipt["roll_map_hash"],
        },
        "budget": {
            "incremental_actual_usd": receipt["actual_cost_usd"],
            "cumulative_actual_usd": actual,
            "remaining_authorized_usd": 200.720719923081 - actual,
        },
        "evidence_roles": {
            "B1_B4": "VIEWED_DEVELOPMENT_NOT_REUSED",
            "2025_H1": "VIEWED_DEVELOPMENT_AFTER_FAILED_CONFIRMATION",
            "2025_Q3": "VIEWED_OTHER_BRANCH_NOT_USED",
            "2021_H1": "SECOND_INDEPENDENT_REPLICATION_CONSUMED_ONCE",
            "Q4": "NOT_ACCESSED",
        },
        "candidate_or_policy_mutated": False,
        "retuning_performed": False,
        "recalibration_performed": False,
        "third_confirmation_allowed": False,
        "tier_c_promoted": False,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "next_action": "REALLOCATE_EXPLOITATION_TO_MATERIALLY_DISTINCT_BRANCH",
    }
    report = {**core, "result_hash": stable_hash(core)}
    _write_once(DECISION_REPORT_PATH, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("freeze", "cost", "acquire", "evaluate", "seal"))
    args = parser.parse_args()
    if args.action == "evaluate":
        result = evaluate()
    elif args.action == "seal":
        result = seal()
    else:
        key = load_api_key()
        if not key:
            raise ReplicationError("DATABENTO_API_KEY is unavailable")
        client = db.Historical(key)
        if args.action == "freeze":
            result = freeze(client)
        elif args.action == "cost":
            result = acquire(client, execute=False)
        else:
            result = acquire(client, execute=True)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
