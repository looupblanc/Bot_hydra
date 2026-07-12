from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from hydra.mission.calibration_retest_execution import _stable_hash
from hydra.promotion.final_cohort import (
    build_final_cohort_manifest,
    validate_final_cohort_manifest,
)
from hydra.shadow.package_factory import build_shadow_package, write_shadow_package


VERSION = "hydra_decision_bridge_v4_preparation"


class DecisionBridgePreparationError(RuntimeError):
    pass


def validate_decision_bridge_preparation_result(result: dict[str, Any]) -> None:
    semantic = {
        key: value
        for key, value in result.items()
        if key not in {"result_hash"}
    }
    if str(result.get("schema")) != VERSION or _stable_hash(semantic) != str(
        result.get("result_hash") or ""
    ):
        raise DecisionBridgePreparationError("Preparation semantic result hash is invalid.")
    if int(result.get("q4_access_count") or 0) != 0 or bool(
        result.get("q4_access_authorized")
    ):
        raise DecisionBridgePreparationError("Preparation crossed the sealed Q4 boundary.")
    manifest_path = Path(str(result.get("cohort_manifest_path") or ""))
    if not manifest_path.is_file() or hashlib.sha256(manifest_path.read_bytes()).hexdigest() != str(
        result.get("cohort_manifest_sha256") or ""
    ):
        raise DecisionBridgePreparationError("Frozen cohort artifact hash mismatch.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_final_cohort_manifest(manifest)
    if str(manifest.get("manifest_hash")) != str(
        result.get("cohort_manifest_hash") or ""
    ):
        raise DecisionBridgePreparationError("Frozen cohort semantic hash mismatch.")
    candidate_ids = sorted(str(value) for value in result.get("candidate_ids") or [])
    packages = dict(result.get("shadow_package_paths") or {})
    package_hashes = dict(result.get("shadow_package_hashes") or {})
    if candidate_ids != sorted(packages) or candidate_ids != sorted(package_hashes):
        raise DecisionBridgePreparationError("Shadow package set is incomplete.")
    for candidate_id in candidate_ids:
        path = Path(str(packages[candidate_id]))
        payload = json.loads(path.read_text(encoding="utf-8"))
        if str(payload.get("package_hash") or "") != str(package_hashes[candidate_id]):
            raise DecisionBridgePreparationError(
                f"Shadow package semantic hash mismatch: {candidate_id}."
            )


def run_decision_bridge_v4_preparation(
    output_dir: str | Path,
    *,
    pre_holdout_manifest_path: str | Path,
    pre_holdout_manifest_sha256: str,
    complete_validation_path: str | Path,
    complete_validation_sha256: str,
    behavioral_clusters_path: str | Path,
    behavioral_clusters_sha256: str,
    policy_path: str | Path,
    policy_sha256: str,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    code_commit: str,
    freeze_timestamp_utc: str,
    q4_access_count: int,
) -> dict[str, Any]:
    sources = {
        str(Path(pre_holdout_manifest_path)): pre_holdout_manifest_sha256,
        str(Path(complete_validation_path)): complete_validation_sha256,
        str(Path(behavioral_clusters_path)): behavioral_clusters_sha256,
        str(Path(policy_path)): policy_sha256,
        str(Path(engineering_task_path)): engineering_task_sha256,
    }
    for raw_path, expected in sources.items():
        path = Path(raw_path)
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise DecisionBridgePreparationError(f"Frozen source hash mismatch: {path}")
    if q4_access_count != 0:
        raise DecisionBridgePreparationError("Q4 access must remain zero during preparation.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise DecisionBridgePreparationError("Preparation worker commit drifted.")
    pre_holdout = json.loads(Path(pre_holdout_manifest_path).read_text(encoding="utf-8"))
    validations = json.loads(Path(complete_validation_path).read_text(encoding="utf-8"))
    clusters = json.loads(Path(behavioral_clusters_path).read_text(encoding="utf-8"))
    validation_by_id = {
        str(row.get("candidate_id")): dict(row) for row in validations
    }
    specifications = dict(pre_holdout.get("specifications") or {})
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    package_records: dict[str, dict[str, Any]] = {}
    package_paths: dict[str, str] = {}
    dossier_paths: dict[str, str] = {}
    for candidate_id in pre_holdout.get("candidate_ids") or []:
        validation = validation_by_id.get(str(candidate_id))
        specification = specifications.get(str(candidate_id))
        if validation is None or not isinstance(specification, dict):
            raise DecisionBridgePreparationError(
                f"Missing frozen validation/specification for {candidate_id}."
            )
        package = build_shadow_package(
            specification,
            validation,
            source_commit=code_commit,
            freeze_timestamp_utc=freeze_timestamp_utc,
            evidence_sha256=complete_validation_sha256,
        )
        machine, dossier = write_shadow_package(package, root / "shadow" / str(candidate_id))
        package_records[str(candidate_id)] = package.to_dict()
        package_paths[str(candidate_id)] = str(machine.resolve())
        dossier_paths[str(candidate_id)] = str(dossier.resolve())
    manifest = build_final_cohort_manifest(
        pre_holdout_manifest=pre_holdout,
        validations=validations,
        behavioral_clusters=clusters,
        package_records=package_records,
        source_commit=code_commit,
        freeze_timestamp_utc=freeze_timestamp_utc,
        policy_path=policy_path,
        policy_sha256=policy_sha256,
        source_artifact_hashes=sources,
        q4_access_count_before=q4_access_count,
    )
    validate_final_cohort_manifest(manifest)
    manifest_path = root / "final_q4_cohort_manifest.json"
    _write_immutable(
        manifest_path, json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n"
    )
    report_path = root / "decision_bridge_preparation_report.md"
    report = (
        "# HYDRA Decision Bridge V4 preparation\n\n"
        f"- Cohort: `{manifest['cohort_id']}`\n"
        f"- Candidates: `{manifest['candidate_count']}`\n"
        f"- Manifest hash: `{manifest['manifest_hash']}`\n"
        "- Q4 access: `0` (still sealed)\n"
        "- Shadow packages: complete, immutable, no broker, no orders\n"
    )
    _write_immutable(report_path, report)
    result: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": "FINAL_Q4_COHORT_AND_SHADOW_PACKAGES_FROZEN_Q4_UNOPENED",
        "cohort_id": manifest["cohort_id"],
        "cohort_manifest_path": str(manifest_path.resolve()),
        "cohort_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "cohort_manifest_hash": manifest["manifest_hash"],
        "candidate_ids": manifest["candidate_ids"],
        "candidate_count": manifest["candidate_count"],
        "candidate_roles": {
            row["candidate_id"]: row["role"] for row in manifest["candidates"]
        },
        "shadow_package_paths": package_paths,
        "shadow_package_dossier_paths": dossier_paths,
        "shadow_package_hashes": {
            candidate_id: row["package_hash"]
            for candidate_id, row in package_records.items()
        },
        "q4_access_count": 0,
        "q4_access_authorized": False,
        "paper_shadow_ready": 0,
        "report_path": str(report_path.resolve()),
        "source_commit": code_commit,
    }
    result["result_hash"] = _stable_hash(result)
    return result


def _write_immutable(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise DecisionBridgePreparationError(f"Immutable output drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
