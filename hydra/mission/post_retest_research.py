from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


POST_RETEST_DESIGN_VERSION = "post_calibration_retest_research_design_v1"


class PostRetestResearchDesignError(RuntimeError):
    pass


_BRANCHES: dict[str, dict[str, Any]] = {
    "SURVIVOR_REPLICATION": {
        "pilot_experiment_type": "fresh_survivor_family_replication_pilot",
        "scientific_objective": (
            "Replicate only calibration-sensitive survivor families from zero on explicit contracts, without "
            "inheriting atom pass status."
        ),
        "expected_decision_information_value": 0.92,
        "required_behavior": (
            "Freeze fresh family IDs and thresholds, run temporal and contractual replication, and retain the "
            "family only if candidate-scope nulls and calibrated controls pass."
        ),
        "implementation_path": "hydra/research/fresh_survivor_replication.py",
        "test_path": "tests/test_fresh_survivor_replication.py",
    },
    "ZERO_SURVIVAL_GEOMETRY_PIVOT": {
        "pilot_experiment_type": "counterfactual_market_state_geometry_pilot",
        "scientific_objective": (
            "Pivot away from the falsified atom grammar and test matched counterfactual market-state geometry "
            "without opening any holdout."
        ),
        "expected_decision_information_value": 0.90,
        "required_behavior": (
            "Preregister a small interpretable state geometry, match causal past-only opportunities, compare a "
            "simpler explanation, and map failure surfaces before any strategy assembly."
        ),
        "implementation_path": "hydra/research/counterfactual_state_geometry.py",
        "test_path": "tests/test_counterfactual_state_geometry.py",
    },
    "INSUFFICIENT_POWER_RESOLUTION": {
        "pilot_experiment_type": "calibration_retest_power_resolution_pilot",
        "scientific_objective": (
            "Resolve why the corrected retest was statistically insufficient before changing the economic "
            "hypothesis or generating more candidates."
        ),
        "expected_decision_information_value": 0.96,
        "required_behavior": (
            "Attribute insufficiency to event support, matching coverage, contract coverage, or validator power; "
            "run discriminative controls and do not reinterpret insufficiency as failure or survival."
        ),
        "implementation_path": "hydra/research/calibration_power_resolution.py",
        "test_path": "tests/test_calibration_power_resolution.py",
    },
    "INVALID_VALIDATOR_INTEGRITY_REPAIR": {
        "pilot_experiment_type": "validator_integrity_repair_pilot",
        "scientific_objective": (
            "Repair or falsify the exact validator, sentinel, or implementation integrity defect before any "
            "scientific decision change."
        ),
        "expected_decision_information_value": 1.0,
        "required_behavior": (
            "Reproduce the invalidity with positive and negative controls, isolate one defect, preserve all "
            "governance boundaries, and rerun no candidate evidence until the validator is calibrated."
        ),
        "implementation_path": "hydra/validation/retest_integrity_repair.py",
        "test_path": "tests/test_retest_integrity_repair.py",
    },
}


def run_post_calibration_retest_research_design(
    output_dir: str | Path,
    *,
    source_execution_result_path: str | Path,
    source_execution_result_hash: str,
    source_execution_experiment_id: str,
    source_execution_specification_hash: str,
    code_commit: str,
) -> dict[str, Any]:
    """Choose one immutable next-research branch from a frozen retest result."""
    source_path = Path(source_execution_result_path)
    source = _load_frozen_result(source_path, source_execution_result_hash)
    _verify_safe_source_boundary(source)
    selected_branch = _select_branch(source)
    branch = dict(_BRANCHES[selected_branch])

    destination = Path(output_dir)
    design_path = destination / "post_calibration_retest_research_design.json"
    report_path = destination / "post_calibration_retest_research_design.md"
    engineering_task_path = destination / "post_calibration_retest_engineering_task.json"

    engineering_task: dict[str, Any] = {
        "schema": "hydra_immutable_engineering_task_v1",
        "task_id": f"post_retest_{selected_branch.lower()}_v1",
        "immutable_before_implementation": True,
        "selected_branch": selected_branch,
        "scientific_objective": branch["scientific_objective"],
        "required_behavior": branch["required_behavior"],
        "pilot_experiment_type": branch["pilot_experiment_type"],
        "allowed_paths": [
            branch["implementation_path"],
            branch["test_path"],
            "hydra/mission/experiment_runner.py",
            "hydra/mission/controller.py",
            "tests/test_mission_scheduler.py",
        ],
        "protected_paths": [
            "config/governance",
            "hydra/governance",
            "hydra/validation/data_roles.py",
            "reports/data_access/data_access_ledger.jsonl",
            "reports/data_budget/databento_spend_ledger.jsonl",
            "mission/state/hydra_mission.db",
            "registry/hydra_registry.db",
            "all_Q4_2024_and_future_lockbox_data",
        ],
        "acceptance_tests": [
            "targeted deterministic pilot tests pass",
            "full pytest, no-lookahead, compileall, budget, Q4, lock, and registry checks pass",
            "frozen source result hash and selected branch remain unchanged",
            "deterministic smoke publishes no Q4 access, paid request, or live execution",
            "single mission writer and queue identity are preserved after restart",
        ],
        "rollback_conditions": [
            "any protected invariant weakens",
            "source result or branch selection changes",
            "a Q4, paid-data, live, or destructive path becomes reachable",
            "two implementation attempts fail acceptance",
        ],
        "maximum_automatic_retries": 2,
        "expected_decision_information_value": branch["expected_decision_information_value"],
        "source_execution_result_hash": source_execution_result_hash,
        "code_commit": code_commit,
    }
    engineering_task["engineering_task_hash"] = _stable_hash(engineering_task)

    pilot_specification = {
        "experiment_type": branch["pilot_experiment_type"],
        "priority": 90.0,
        "max_attempts": 3,
        "selected_post_retest_branch": selected_branch,
        "source_execution_experiment_id": source_execution_experiment_id,
        "source_execution_specification_hash": source_execution_specification_hash,
        "source_execution_result_path": str(source_path),
        "source_execution_result_hash": source_execution_result_hash,
        "post_retest_design_path": str(design_path),
        "engineering_task_path": str(engineering_task_path),
        "engineering_task_hash": engineering_task["engineering_task_hash"],
        "code_commit": code_commit,
        "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
        "q4_access_allowed": False,
        "paid_data_allowed": False,
    }

    design: dict[str, Any] = {
        "schema": "post_calibration_retest_research_design_v1",
        "design_version": POST_RETEST_DESIGN_VERSION,
        "immutable_before_pilot_implementation": True,
        "scientific_conclusion": f"POST_RETEST_BRANCH_SELECTED:{selected_branch}",
        "interpretation_boundary": (
            "This artifact selects research work only. It validates no mechanism or strategy, authorizes no "
            "holdout access, and enables no broker execution."
        ),
        "selected_branch": selected_branch,
        "branch_contract": branch,
        "source": {
            "execution_experiment_id": source_execution_experiment_id,
            "execution_specification_hash": source_execution_specification_hash,
            "execution_result_path": str(source_path),
            "execution_result_hash": source_execution_result_hash,
            "execution_scientific_conclusion": source.get("scientific_conclusion"),
            "execution_code_commit": source.get("code_commit"),
        },
        "engineering_task_specification": engineering_task,
        "pilot_experiment_specification": pilot_specification,
        "governance": {
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "live_or_broker_execution": False,
            "parameters_may_not_be_changed_using_holdout_evidence": True,
        },
        "code_commit": code_commit,
    }
    design["design_hash"] = _stable_hash(design)

    artifacts = {
        engineering_task_path: json.dumps(engineering_task, indent=2, sort_keys=True) + "\n",
        design_path: json.dumps(design, indent=2, sort_keys=True) + "\n",
        report_path: _render_report(design),
    }
    _write_immutable_artifacts(artifacts)
    return {
        **design,
        "pilot_experiment_type": branch["pilot_experiment_type"],
        "paths": {
            "design": str(design_path),
            "report": str(report_path),
            "engineering_task": str(engineering_task_path),
        },
        "report_path": str(report_path),
    }


def _load_frozen_result(path: Path, expected_hash: str) -> dict[str, Any]:
    if not path.is_file():
        raise PostRetestResearchDesignError(f"Frozen calibration retest result is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PostRetestResearchDesignError("Frozen calibration retest result root is not an object.")
    stored_hash = str(payload.get("result_hash") or "")
    body = {key: value for key, value in payload.items() if key != "result_hash"}
    calculated_hash = _stable_hash(body)
    if not stored_hash or stored_hash != calculated_hash or stored_hash != str(expected_hash):
        raise PostRetestResearchDesignError("Frozen calibration retest result hash mismatch.")
    return payload


def _verify_safe_source_boundary(source: dict[str, Any]) -> None:
    governance = source.get("governance") or {}
    safe = (
        governance.get("q4_access_count_delta") == 0
        and governance.get("network_requests") == 0
        and float(governance.get("incremental_databento_spend_usd", -1.0)) == 0.0
        and governance.get("live_or_broker_execution") is False
        and governance.get("latest_data_end_exclusive") == "2024-10-01"
    )
    if not safe:
        raise PostRetestResearchDesignError("Frozen retest result violates the safe development-data boundary.")


def _select_branch(source: dict[str, Any]) -> str:
    conclusion = str(source.get("scientific_conclusion") or "")
    survivor_count = int(source.get("calibration_sensitive_survivor_count", 0))
    controls_passed = bool(source.get("validator_controls_passed"))
    evidence_valid = bool(source.get("evidence_valid_for_decision_change"))
    invalid = (
        conclusion.startswith("INVALID_")
        or conclusion.startswith("INTEGRITY_FAIL")
        or not controls_passed
        or not bool(source.get("invariant_controls_all_rejected"))
    )
    if invalid:
        return "INVALID_VALIDATOR_INTEGRITY_REPAIR"
    if "INSUFFICIENT" in conclusion:
        return "INSUFFICIENT_POWER_RESOLUTION"
    if evidence_valid and survivor_count > 0 and conclusion.startswith("CALIBRATION_FALSE_KILLS_PLAUSIBLE"):
        return "SURVIVOR_REPLICATION"
    if (
        evidence_valid
        and survivor_count == 0
        and conclusion == "ZERO_SURVIVAL_PERSISTS_UNDER_CORRECTED_RETEST_PIVOT_RESEARCH_GRAMMAR"
    ):
        return "ZERO_SURVIVAL_GEOMETRY_PIVOT"
    return "INVALID_VALIDATOR_INTEGRITY_REPAIR"


def _write_immutable_artifacts(artifacts: dict[Path, str]) -> None:
    for path, content in artifacts.items():
        if path.exists() and path.read_text(encoding="utf-8") != content:
            raise PostRetestResearchDesignError(f"Refusing to overwrite immutable post-retest artifact: {path}")
    for path, content in artifacts.items():
        if path.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(content, encoding="utf-8")
        os.replace(temporary, path)


def _render_report(design: dict[str, Any]) -> str:
    branch = design["branch_contract"]
    task = design["engineering_task_specification"]
    return (
        "# HYDRA Post-Calibration Retest Research Design\n\n"
        f"- Selected branch: `{design['selected_branch']}`\n"
        f"- Pilot experiment type: `{branch['pilot_experiment_type']}`\n"
        f"- Source result hash: `{design['source']['execution_result_hash']}`\n"
        f"- Design hash: `{design['design_hash']}`\n"
        f"- Engineering task hash: `{task['engineering_task_hash']}`\n"
        "- Q4 access: prohibited\n"
        "- Paid data: prohibited\n"
        "- Live/broker execution: prohibited\n\n"
        "## Scientific objective\n\n"
        f"{branch['scientific_objective']}\n\n"
        "## Interpretation boundary\n\n"
        f"{design['interpretation_boundary']}\n"
    )


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
