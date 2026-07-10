from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from hydra.data.contract_mapping import (
    load_roll_map,
    repair_roll_map_from_date_aware_definitions,
    valid_outright_future_symbol,
    write_roll_map,
)
from hydra.markets.instruments import instrument_spec


REPAIR_VERSION = "contract_map_date_aware_repair_v1"


class ContractMapDateRepairError(RuntimeError):
    pass


def run_contract_map_date_aware_repair(
    output_dir: str | Path,
    *,
    integrity_pilot_result_path: str | Path,
    integrity_pilot_result_hash: str,
    frozen_contract_map_path: str | Path,
    frozen_contract_map_sha256: str,
    definition_dbn_path: str | Path,
    definition_dbn_sha256: str,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    code_commit: str,
    map_output_folder: str | Path | None = None,
) -> dict[str, Any]:
    """Build one immutable date-aware map from frozen cached metadata only."""
    pilot_path = Path(integrity_pilot_result_path)
    map_path = Path(frozen_contract_map_path)
    dbn_path = Path(definition_dbn_path)
    task_path = Path(engineering_task_path)
    pilot = _load_result(pilot_path, integrity_pilot_result_hash)
    _verify_file(map_path, frozen_contract_map_sha256, "frozen contract map")
    _verify_file(dbn_path, definition_dbn_sha256, "cached definition DBN")
    _verify_file(task_path, engineering_task_sha256, "immutable engineering task")
    if pilot.get("integrity_disposition") != "CONTRACT_MAP_REBUILD_REQUIRED":
        raise ContractMapDateRepairError("Integrity pilot did not authorize a contract-map rebuild.")
    if int((pilot.get("contract_map_integrity_audit") or {}).get("invalid_frozen_map_symbol_count", -1)) != 40:
        raise ContractMapDateRepairError("Frozen pilot no longer reports the preregistered 40 invalid symbols.")
    implementation_commit = _git_commit()
    if code_commit != implementation_commit:
        raise ContractMapDateRepairError("Runtime commit differs from the frozen repair specification.")

    try:
        import databento as db
    except ImportError as exc:
        raise ContractMapDateRepairError("Databento reader is unavailable for cached metadata.") from exc
    definition_frame = db.DBNStore.from_file(dbn_path).to_df(
        pretty_ts=True, map_symbols=False
    ).reset_index()
    frozen = load_roll_map(map_path)
    repaired, repair_audit = repair_roll_map_from_date_aware_definitions(frozen, definition_frame)
    invariants = _compare_invariants(frozen, repaired)
    if not invariants["passed"]:
        raise ContractMapDateRepairError(f"Repair changed protected map invariants: {invariants}")
    if repair_audit["segment_count"] != 141:
        raise ContractMapDateRepairError("Repair segment count differs from the frozen 141-segment task.")
    if repair_audit["symbol_change_count"] != 40:
        raise ContractMapDateRepairError("Repair did not change exactly the 40 invalid frozen symbols.")
    if repair_audit["tick_size_change_count"] != 23:
        raise ContractMapDateRepairError("Repair did not change exactly the 23 invalid frozen tick sizes.")
    if not repair_audit["all_resolved_symbols_valid"]:
        raise ContractMapDateRepairError("At least one repaired symbol is not a valid outright future.")
    tick_validation = _validate_ticks(repaired)
    if not tick_validation["passed"]:
        raise ContractMapDateRepairError(f"Repaired map has invalid ticks: {tick_validation}")

    output_folder = Path(map_output_folder) if map_output_folder is not None else map_path.parent
    repaired_path, repaired_hash = write_roll_map(repaired, folder=str(output_folder))
    repaired_sha256 = _file_sha256(repaired_path)
    if _file_sha256(map_path) != frozen_contract_map_sha256:
        raise ContractMapDateRepairError("Frozen predecessor map changed during repair.")
    if _file_sha256(dbn_path) != definition_dbn_sha256:
        raise ContractMapDateRepairError("Cached definition DBN changed during repair.")

    payload: dict[str, Any] = {
        "schema": REPAIR_VERSION,
        "repair_version": REPAIR_VERSION,
        "scientific_conclusion": "DATE_AWARE_EXPLICIT_CONTRACT_MAP_REPAIRED_AND_INTEGRITY_VALIDATED",
        "repair_status": "COMPLETED_VALIDATED_MAP",
        "interpretation_boundary": (
            "This repairs contract identity metadata only. It reruns no candidate, validates no atom or strategy, "
            "and does not authorize Q4, a lockbox, paid data, or execution."
        ),
        "source": {
            "integrity_pilot_result_path": str(pilot_path),
            "integrity_pilot_result_hash": integrity_pilot_result_hash,
            "frozen_contract_map_path": str(map_path),
            "frozen_contract_map_sha256": frozen_contract_map_sha256,
            "definition_dbn_path": str(dbn_path),
            "definition_dbn_sha256": definition_dbn_sha256,
            "engineering_task_path": str(task_path),
            "engineering_task_sha256": engineering_task_sha256,
            "implementation_commit": implementation_commit,
        },
        "repaired_map": {
            "path": str(repaired_path),
            "roll_map_hash": repaired_hash,
            "sha256": repaired_sha256,
            "map_type": repaired.map_type,
        },
        "repair_audit": repair_audit,
        "protected_invariant_audit": invariants,
        "tick_validation": tick_validation,
        "candidate_evidence_rerun_count": 0,
        "fully_validated_edge_atoms": 0,
        "validated_strategies": 0,
        "required_next_action": "FRESH_CALIBRATION_RETEST_V3_PREREGISTRATION_WITH_NEW_ATOM_IDS",
        "governance": {
            "q4_access_count_delta": 0,
            "incremental_databento_spend_usd": 0.0,
            "network_requests": 0,
            "market_observation_rows_read": 0,
            "live_or_broker_execution": False,
            "frozen_predecessor_preserved": True,
            "cached_definition_metadata_only": True,
        },
    }
    payload["result_hash"] = _stable_hash(payload)
    destination = Path(output_dir)
    json_path = destination / "contract_map_date_aware_repair.json"
    report_path = destination / "contract_map_date_aware_repair.md"
    _write_immutable(json_path, json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {"result_json_path": str(json_path), "report_path": str(report_path)},
        "report_path": str(report_path),
    }


def _compare_invariants(frozen: Any, repaired: Any) -> dict[str, Any]:
    frozen_by_key = {(row.root, row.instrument_id, row.active_start, row.active_end): row for row in frozen.contracts}
    repaired_by_key = {
        (row.root, row.instrument_id, row.active_start, row.active_end): row for row in repaired.contracts
    }
    identity_match = set(frozen_by_key) == set(repaired_by_key)
    drift: list[dict[str, Any]] = []
    preserved_fields = (
        "root",
        "instrument_id",
        "continuous_symbol",
        "active_start",
        "active_end",
        "roll_date",
        "tick_value",
        "point_value",
        "contract_multiplier",
        "is_micro",
        "parent_symbol",
    )
    if identity_match:
        for key in sorted(frozen_by_key):
            old, new = frozen_by_key[key], repaired_by_key[key]
            changed = {
                field: {"old": getattr(old, field), "new": getattr(new, field)}
                for field in preserved_fields
                if getattr(old, field) != getattr(new, field)
            }
            if changed:
                drift.append({"key": list(key), "changed": changed})
    return {
        "passed": bool(
            identity_match
            and not drift
            and frozen.dataset == repaired.dataset
            and frozen.schema == repaired.schema
            and frozen.symbols == repaired.symbols
            and frozen.unsafe_window_days == repaired.unsafe_window_days
        ),
        "identity_key_set_equal": identity_match,
        "preserved_field_drift": drift,
        "dataset_equal": frozen.dataset == repaired.dataset,
        "schema_equal": frozen.schema == repaired.schema,
        "symbols_equal": frozen.symbols == repaired.symbols,
        "unsafe_window_days_equal": frozen.unsafe_window_days == repaired.unsafe_window_days,
    }


def _validate_ticks(roll_map: Any) -> dict[str, Any]:
    failures = []
    for row in roll_map.contracts:
        expected = float(instrument_spec(row.root).tick_size)
        if abs(float(row.tick_size) - expected) > 1e-12:
            failures.append(
                {"root": row.root, "contract": row.contract, "actual": row.tick_size, "expected": expected}
            )
        if not valid_outright_future_symbol(row.root, row.contract):
            failures.append({"root": row.root, "contract": row.contract, "reason": "invalid_symbol"})
    return {"passed": not failures, "validated_contract_count": len(roll_map.contracts), "failures": failures}


def _load_result(path: Path, expected_hash: str) -> dict[str, Any]:
    if not path.is_file():
        raise ContractMapDateRepairError(f"Integrity pilot result is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    stored = str(payload.get("result_hash") or "")
    body = {key: value for key, value in payload.items() if key != "result_hash"}
    if stored != expected_hash or stored != _stable_hash(body):
        raise ContractMapDateRepairError("Integrity pilot result hash mismatch.")
    return payload


def _verify_file(path: Path, expected_hash: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected_hash:
        raise ContractMapDateRepairError(f"Frozen {label} is missing or its checksum changed: {path}")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "UNKNOWN"


def _write_immutable(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") == content:
            return
        raise ContractMapDateRepairError(f"Refusing to overwrite divergent repair artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _render_report(payload: dict[str, Any]) -> str:
    audit = payload["repair_audit"]
    repaired = payload["repaired_map"]
    return (
        "# HYDRA Date-Aware Contract Map Repair\n\n"
        f"- Conclusion: `{payload['scientific_conclusion']}`\n"
        f"- Result hash: `{payload['result_hash']}`\n"
        f"- Repaired map: `{repaired['path']}`\n"
        f"- Repaired map hash: `{repaired['roll_map_hash']}`\n"
        f"- Segments: `{audit['segment_count']}`\n"
        f"- Symbol corrections: `{audit['symbol_change_count']}`\n"
        f"- Tick corrections: `{audit['tick_size_change_count']}`\n"
        "- Candidate reruns: `0`\n"
        "- Q4 / paid data / network / live: `0`\n\n"
        "## Interpretation boundary\n\n"
        f"{payload['interpretation_boundary']}\n"
    )
