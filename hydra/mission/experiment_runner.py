from __future__ import annotations

import json
import os
import traceback
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


class UnknownExperimentType(RuntimeError):
    pass


def experiment_worker_entry(experiment: dict[str, Any], result_path: str) -> None:
    """Subprocess entry point: execute research and publish no mission-DB writes."""
    if hasattr(os, "setsid"):
        os.setsid()
    target = Path(result_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        output_root_value = experiment.get("worker_output_root")
        output_root = Path(str(output_root_value)) if output_root_value else None
        result = run_experiment(experiment, output_root=output_root)
        envelope = {
            "ok": True,
            "experiment_id": experiment.get("experiment_id"),
            "specification_hash": experiment.get("specification_hash"),
            "result": result,
        }
    except Exception as exc:
        envelope = {
            "ok": False,
            "experiment_id": experiment.get("experiment_id"),
            "specification_hash": experiment.get("specification_hash"),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(limit=40),
        }
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(envelope, sort_keys=True, default=str), encoding="utf-8")
    os.replace(temporary, target)


def run_experiment(experiment: dict[str, Any], *, output_root: Path | None = None) -> dict[str, Any]:
    """Run a closed, auditable research handler without touching mission SQLite."""
    experiment_type = str(experiment.get("experiment_type") or "")
    experiment_id = str(experiment["experiment_id"])
    root = output_root or project_path("reports", "mission_experiments")
    output_dir = Path(root) / experiment_id
    if experiment_type == "calibration_affected_atom_retest_design":
        from hydra.mission.calibration_retest import run_calibration_affected_atom_retest_design

        return run_calibration_affected_atom_retest_design(
            output_dir,
            historical_report_path=Path(
                experiment.get(
                    "historical_report_path",
                    project_path(
                        "reports",
                        "edge_atom_lab",
                        "edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md",
                    ),
                )
            ),
            historical_preregistration_path=Path(
                experiment.get(
                    "historical_preregistration_path",
                    project_path(
                        "reports",
                        "edge_atom_lab",
                        "edge_atom_preregistration_20260710T101052+0000_edge_atom_discovery_replication_v1_final.json",
                    ),
                )
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "calibration_affected_atom_retest_execution":
        from hydra.mission.calibration_retest_execution import run_calibration_affected_atom_retest_execution

        return run_calibration_affected_atom_retest_execution(
            output_dir,
            design_preregistration_path=Path(str(experiment["design_preregistration_path"])),
            design_path=Path(str(experiment["design_path"])),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "post_calibration_retest_research_design":
        from hydra.mission.post_retest_research import run_post_calibration_retest_research_design

        return run_post_calibration_retest_research_design(
            output_dir,
            source_execution_result_path=Path(str(experiment["source_execution_result_path"])),
            source_execution_result_hash=str(experiment["source_execution_result_hash"]),
            source_execution_experiment_id=str(experiment["source_execution_experiment_id"]),
            source_execution_specification_hash=str(experiment["source_execution_specification_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    raise UnknownExperimentType(f"No approved handler for experiment type {experiment_type!r}.")
