from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

from hydra.data.contract_mapping import load_roll_map
from hydra.mission.calibration_retest import (
    _file_sha256,
    _json_text,
    _slug,
    _stable_hash,
    _write_immutable_artifacts,
    run_calibration_affected_atom_retest_design,
)


DESIGN_VERSION = "calibration_affected_atom_retest_design_v3"
PREREGISTRATION_SCHEMA = "edge_atom_calibration_retest_preregistration_v3"
REQUIRED_MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
DESIGN_JSON_NAME = "calibration_affected_atom_retest_v3_design.json"
PREREGISTRATION_JSON_NAME = "calibration_affected_atom_retest_v3_preregistration.json"
REPORT_NAME = "calibration_affected_atom_retest_v3_design.md"


class CalibrationRetestV3Error(RuntimeError):
    pass


def run_calibration_affected_atom_retest_v3_design(
    output_dir: str | Path,
    *,
    contract_map_repair_result_path: str | Path,
    contract_map_repair_result_hash: str,
    contract_map_repair_file_sha256: str,
    invalid_v2_execution_result_path: str | Path,
    invalid_v2_execution_result_hash: str,
    invalid_v2_execution_file_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    code_commit: str,
) -> dict[str, Any]:
    """Create a fresh v3 preregistration on the repaired explicit map."""
    repair_path = Path(contract_map_repair_result_path)
    invalid_path = Path(invalid_v2_execution_result_path)
    map_path = Path(repaired_map_path)
    task_path = Path(engineering_task_path)
    _verify_file(repair_path, contract_map_repair_file_sha256, "contract-map repair result")
    _verify_file(invalid_path, invalid_v2_execution_file_sha256, "invalid v2 execution result")
    _verify_file(map_path, repaired_map_sha256, "repaired roll map")
    _verify_file(task_path, engineering_task_sha256, "immutable v3 engineering task")
    repair = _load_hashed_result(repair_path, contract_map_repair_result_hash)
    invalid_v2 = _load_hashed_result(invalid_path, invalid_v2_execution_result_hash)
    _validate_integrity_predecessors(
        repair,
        invalid_v2,
        map_path=map_path,
        map_sha256=repaired_map_sha256,
        roll_map_hash=repaired_roll_map_hash,
    )
    _verify_runtime_commit(code_commit)

    with tempfile.TemporaryDirectory(prefix="hydra-v3-design-base-") as temporary:
        base = run_calibration_affected_atom_retest_design(
            Path(temporary),
            code_commit=code_commit,
        )

    source = deepcopy(base["source"])
    source["development_data_manifest"]["contract_map"] = {
        "path": str(map_path.resolve()),
        "sha256": repaired_map_sha256,
        "size_bytes": map_path.stat().st_size,
        "roll_map_hash": repaired_roll_map_hash,
        "map_type": REQUIRED_MAP_TYPE,
    }
    source["integrity_repair_precondition"] = {
        "contract_map_repair_result_path": str(repair_path.resolve()),
        "contract_map_repair_result_hash": contract_map_repair_result_hash,
        "contract_map_repair_file_sha256": contract_map_repair_file_sha256,
        "invalid_v2_execution_result_path": str(invalid_path.resolve()),
        "invalid_v2_execution_result_hash": invalid_v2_execution_result_hash,
        "invalid_v2_execution_file_sha256": invalid_v2_execution_file_sha256,
        "invalid_v2_decision_inherited": False,
        "repaired_map_path": str(map_path.resolve()),
        "repaired_map_sha256": repaired_map_sha256,
        "repaired_roll_map_hash": repaired_roll_map_hash,
        "repaired_map_type": REQUIRED_MAP_TYPE,
    }
    source["engineering_task"] = {
        "path": str(task_path.resolve()),
        "sha256": engineering_task_sha256,
    }

    invalid_v2_by_historical = {
        str(row.get("historical_atom_id")): str(row.get("atom_id"))
        for row in invalid_v2.get("results") or []
        if row.get("historical_atom_id") and row.get("atom_id")
    }
    atoms: list[dict[str, Any]] = []
    base_to_v3: dict[str, str] = {}
    for base_atom in base["preregistration"]["atoms"]:
        atom = deepcopy(base_atom)
        base_id = str(atom["atom_id"])
        historical_id = str(atom["historical_reference"]["historical_atom_id"])
        invalid_v2_id = invalid_v2_by_historical.get(historical_id)
        if not invalid_v2_id:
            raise CalibrationRetestV3Error(
                f"Invalid v2 result lacks selected historical atom {historical_id}."
            )
        identifier_payload = {
            "design_version": DESIGN_VERSION,
            "historical_atom_id": historical_id,
            "historical_preregistration_hash": atom["historical_reference"][
                "historical_preregistration_hash"
            ],
            "invalid_v2_execution_result_hash": invalid_v2_execution_result_hash,
            "contract_map_repair_result_hash": contract_map_repair_result_hash,
            "repaired_roll_map_hash": repaired_roll_map_hash,
            "repaired_map_sha256": repaired_map_sha256,
            "code_commit": code_commit,
        }
        digest = _stable_hash(identifier_payload)[:16]
        atom["atom_id"] = (
            f"atom_calibration_retest_{_slug(str(atom['family']))}_"
            f"{_slug(str(atom['feature_key']))}_{digest}_v3"
        )
        atom["version"] = 3
        atom["code_commit"] = code_commit
        atom["historical_reference"].update(
            {
                "integrity_invalid_v2_atom_id": invalid_v2_id,
                "integrity_invalid_v2_result_hash": invalid_v2_execution_result_hash,
                "v2_status_is_not_inherited": True,
            }
        )
        atom["decision_contract"].update(
            {
                "integrity_invalid_v2_status_inherited": False,
                "initial_state": "PREREGISTERED_UNTESTED_V3",
            }
        )
        atom["contract_map_contract"] = {
            "map_path": str(map_path.resolve()),
            "map_sha256": repaired_map_sha256,
            "roll_map_hash": repaired_roll_map_hash,
            "map_type": REQUIRED_MAP_TYPE,
            "date_aware_definition_required": True,
        }
        atom.pop("preregistration_hash", None)
        atom["preregistration_hash"] = _stable_hash(atom)
        base_to_v3[base_id] = str(atom["atom_id"])
        atoms.append(atom)

    preregistration = deepcopy(base["preregistration"])
    preregistration.update(
        {
            "schema": PREREGISTRATION_SCHEMA,
            "design_version": DESIGN_VERSION,
            "code_commit": code_commit,
            "source": source,
            "atoms": atoms,
            "invalid_predecessor_policy": {
                "invalid_v2_result_hash": invalid_v2_execution_result_hash,
                "v2_atom_status_inheritance_allowed": False,
                "v2_effect_inheritance_allowed": False,
                "v2_insufficiency_inheritance_allowed": False,
            },
        }
    )
    for group in preregistration.get("paired_retest_groups") or []:
        group["new_atom_ids"] = sorted(base_to_v3[str(atom_id)] for atom_id in group["new_atom_ids"])
    preregistration.pop("preregistration_hash", None)
    preregistration["preregistration_hash"] = _stable_hash(preregistration)

    design = deepcopy(base)
    for transient in ("artifacts", "paths", "design_path", "preregistration_path", "report_path"):
        design.pop(transient, None)
    design.update(
        {
            "schema": DESIGN_VERSION,
            "design_version": DESIGN_VERSION,
            "experiment_id": "calibration_affected_atom_retest_v3_design",
            "scientific_conclusion": (
                "FRESH_V3_RETEST_PREREGISTERED_ON_DATE_AWARE_MAP_NO_EVIDENCE_INHERITED"
            ),
            "source": source,
            "preregistration": preregistration,
            "integrity_precondition": source["integrity_repair_precondition"],
            "decision_scope": (
                "This v3 design authorizes only a fresh development-data retest on the repaired map. "
                "It validates no atom or strategy and authorizes no Q4 or lockbox access."
            ),
            "unresolved_question": (
                "Whether the selected effects and invariant sentinels become decisive after correcting "
                "contract identity remains unresolved until this exact v3 preregistration executes."
            ),
            "next_recommended_action": "EXECUTE_FRESH_V3_PREREGISTRATION_ON_REPAIRED_MAP",
            "artifact_names": {
                "design_json": DESIGN_JSON_NAME,
                "preregistration_json": PREREGISTRATION_JSON_NAME,
                "report": REPORT_NAME,
            },
        }
    )
    design["selection"]["selected_new_atom_ids"] = [atom["atom_id"] for atom in atoms]
    design.pop("design_hash", None)
    design["design_hash"] = _stable_hash(design)

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    design_path = destination / DESIGN_JSON_NAME
    preregistration_path = destination / PREREGISTRATION_JSON_NAME
    report_path = destination / REPORT_NAME
    _write_immutable_artifacts(
        {
            design_path: _json_text(design),
            preregistration_path: _json_text(preregistration),
            report_path: _render_report(design),
        }
    )
    return {
        **design,
        "artifacts": {
            "design_json_path": str(design_path),
            "preregistration_json_path": str(preregistration_path),
            "report_path": str(report_path),
        },
        "paths": {
            "design": str(design_path),
            "preregistration": str(preregistration_path),
            "report": str(report_path),
        },
        "design_path": str(design_path),
        "preregistration_path": str(preregistration_path),
        "report_path": str(report_path),
    }


def _validate_integrity_predecessors(
    repair: dict[str, Any],
    invalid_v2: dict[str, Any],
    *,
    map_path: Path,
    map_sha256: str,
    roll_map_hash: str,
) -> None:
    if repair.get("scientific_conclusion") != (
        "DATE_AWARE_EXPLICIT_CONTRACT_MAP_REPAIRED_AND_INTEGRITY_VALIDATED"
    ) or repair.get("repair_status") != "COMPLETED_VALIDATED_MAP":
        raise CalibrationRetestV3Error("Contract-map repair result is not a validated predecessor.")
    repaired = repair.get("repaired_map") or {}
    if (
        Path(str(repaired.get("path") or "")).resolve() != map_path.resolve()
        or repaired.get("sha256") != map_sha256
        or repaired.get("roll_map_hash") != roll_map_hash
        or repaired.get("map_type") != REQUIRED_MAP_TYPE
    ):
        raise CalibrationRetestV3Error("Repaired-map provenance differs from the frozen v3 inputs.")
    audit = repair.get("repair_audit") or {}
    if (
        int(audit.get("segment_count", -1)) != 141
        or int(audit.get("symbol_change_count", -1)) != 40
        or int(audit.get("tick_size_change_count", -1)) != 23
        or not (repair.get("protected_invariant_audit") or {}).get("passed")
        or not (repair.get("tick_validation") or {}).get("passed")
    ):
        raise CalibrationRetestV3Error("Contract-map repair audits are incomplete.")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != REQUIRED_MAP_TYPE or roll_map.roll_map_hash() != roll_map_hash:
        raise CalibrationRetestV3Error("Repaired roll-map semantic hash or type mismatch.")
    if invalid_v2.get("scientific_conclusion") != (
        "INVALID_RETEST_INVARIANT_SENTINEL_INSUFFICIENT_NO_DECISION_CHANGE"
    ) or invalid_v2.get("evidence_valid_for_decision_change") is not False:
        raise CalibrationRetestV3Error("Frozen v2 result is not the expected integrity-invalid predecessor.")
    if int(invalid_v2.get("retest_count", -1)) != 6:
        raise CalibrationRetestV3Error("Frozen v2 predecessor does not contain exactly six retests.")


def _load_hashed_result(path: Path, expected_hash: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CalibrationRetestV3Error(f"Frozen result is unreadable: {path}") from exc
    stored_hash = str(payload.get("result_hash") or "")
    body = {key: value for key, value in payload.items() if key != "result_hash"}
    if stored_hash != expected_hash or stored_hash != _stable_hash(body):
        raise CalibrationRetestV3Error(f"Frozen result hash mismatch: {path}")
    return payload


def _verify_file(path: Path, expected_hash: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected_hash:
        raise CalibrationRetestV3Error(f"Frozen {label} is missing or changed: {path}")


def _verify_runtime_commit(code_commit: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", code_commit):
        return
    actual = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
    ).strip()
    if actual != code_commit:
        raise CalibrationRetestV3Error("Runtime Git HEAD differs from the frozen v3 code commit.")
    dirty = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", "hydra", "scripts"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode
    if dirty != 0:
        raise CalibrationRetestV3Error("Tracked runtime code differs from the frozen v3 commit.")


def _render_report(design: dict[str, Any]) -> str:
    prereg = design["preregistration"]
    rows = [
        "# HYDRA Calibration-Affected Atom Retest v3 Design",
        "",
        "Development/falsification data only. Q4, paid data, network, live, and broker paths are prohibited.",
        "",
        f"- Design hash: `{design['design_hash']}`",
        f"- Preregistration hash: `{prereg['preregistration_hash']}`",
        f"- Scientific conclusion: `{design['scientific_conclusion']}`",
        f"- Repaired map hash: `{design['integrity_precondition']['repaired_roll_map_hash']}`",
        f"- Fresh v3 atoms: `{len(prereg['atoms'])}`",
        "",
        "| Role | New v3 atom | Historical atom | Integrity-invalid v2 atom |",
        "|---|---|---|---|",
    ]
    for atom in prereg["atoms"]:
        reference = atom["historical_reference"]
        rows.append(
            f"| {atom['selection_role']} | `{atom['atom_id']}` | "
            f"`{reference['historical_atom_id']}` | `{reference['integrity_invalid_v2_atom_id']}` |"
        )
    rows.extend(["", "## Interpretation boundary", "", design["decision_scope"], ""])
    return "\n".join(rows)
