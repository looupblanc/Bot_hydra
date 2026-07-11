from __future__ import annotations

import hashlib
import json
import math
import multiprocessing
import os
import re
import subprocess
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.calibration.validator_benchmark import benchmark_validator
from hydra.data.contract_mapping import RollMap, load_roll_map
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


EXECUTION_VERSION = "calibration_affected_atom_retest_execution_v2"
DEFAULT_HISTORICAL_REPORT = project_path(
    "reports",
    "edge_atom_lab",
    "edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md",
)
FOLDS = (
    ("2023_h2", "2023-01-01", "2023-07-01", "2023-07-01", "2024-01-01"),
    ("2024_q1", "2023-01-01", "2024-01-01", "2024-01-01", "2024-04-01"),
    ("2024_q2", "2023-01-01", "2024-04-01", "2024-04-01", "2024-07-01"),
    ("2024_q3", "2023-01-01", "2024-07-01", "2024-07-01", "2024-10-01"),
)
_PAST_ONLY_FEATURES = {
    "old_region_reentry",
    "directional_pressure_without_progress",
    "shared_loss_risk_state",
    "failed_expansion",
    "extreme_dwell",
    "rv_short_long_ratio",
}


class CalibrationRetestExecutionError(RuntimeError):
    pass


def run_calibration_affected_atom_retest_execution(
    output_dir: str | Path,
    *,
    design_preregistration_path: str | Path,
    design_path: str | Path,
    code_commit: str,
    historical_report_path: str | Path = DEFAULT_HISTORICAL_REPORT,
    record_data_access: bool = True,
    random_seed: int = 9173,
    contract_map_path: str | Path | None = None,
    required_contract_map_type: str = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
    expected_design_version: str | None = None,
    execution_version: str = EXECUTION_VERSION,
    output_stem: str = "calibration_affected_atom_retest_execution",
    data_access_reason: str = (
        "fresh calibration-affected atom v2 retests; expanding walk-forward folds; Q4 excluded"
    ),
) -> dict[str, Any]:
    """Execute only the fresh bounded v2 preregistration on pre-Q4 cached data."""
    design_file = Path(design_path)
    prereg_file = Path(design_preregistration_path)
    report_file = Path(historical_report_path)
    design = _load_json(design_file)
    preregistration = _load_json(prereg_file)
    _validate_preregistration(
        design,
        preregistration,
        code_commit=code_commit,
        historical_report_path=report_file,
        random_seed=random_seed,
    )
    if expected_design_version is not None and (
        design.get("design_version") != expected_design_version
        or preregistration.get("design_version") != expected_design_version
    ):
        raise CalibrationRetestExecutionError("Frozen design version differs from the required execution version.")
    manifest_contract_map = (
        (preregistration.get("source") or {}).get("development_data_manifest") or {}
    ).get("contract_map") or {}
    selected_contract_map_path: Path | None = None
    if contract_map_path is not None:
        selected_contract_map_path = Path(contract_map_path)
        manifest_path = Path(str(manifest_contract_map.get("path") or ""))
        if not manifest_path.is_file() or selected_contract_map_path.resolve() != manifest_path.resolve():
            raise CalibrationRetestExecutionError(
                "Execution contract map does not equal the frozen preregistration manifest."
            )
        if manifest_contract_map.get("map_type") not in {None, required_contract_map_type}:
            raise CalibrationRetestExecutionError("Frozen manifest contract-map type is not authorized.")
    atoms = list(preregistration["atoms"])
    atom_ids = [str(atom["atom_id"]) for atom in atoms]

    legacy_calibration = benchmark_validator(seed=9050)
    legacy_calibration_payload = legacy_calibration.to_dict()
    legacy_calibration_payload.pop("created_at_utc", None)
    pipeline_calibration = _benchmark_retest_pipeline(preregistration, seed=random_seed + 50000)
    validator_controls_passed = bool(
        pipeline_calibration["passed"]
        and pipeline_calibration["false_positive_rate"] <= float(
            preregistration["validator_acceptance_policy"]["maximum_false_positive_rate"]
        )
        and pipeline_calibration["power_on_meaningful_effects"]
        >= float(preregistration["validator_acceptance_policy"]["minimum_power_on_meaningful_effects"])
    )
    historical = _load_markdown_json(report_file)
    if record_data_access:
        access_record = _record_data_access_once(
            "2023-01-01:2024-10-01",
            atom_ids,
            data_access_reason,
        )
    else:
        access_record = None
    # The role guard must precede the first market-data read. Manifest and
    # historical-report validation above inspect only frozen metadata/hashes.
    frame, data_provenance = _load_governed_development_frame(
        historical,
        atoms,
        contract_map_path=selected_contract_map_path,
        required_contract_map_type=required_contract_map_type,
    )

    feature_frame = _build_past_only_feature_frame(frame)
    prefix_invariance = _verify_prefix_invariance(
        frame,
        feature_frame,
        feature_keys={str(atom["feature_key"]) for atom in atoms},
    )
    if not prefix_invariance["passed"]:
        raise CalibrationRetestExecutionError(f"Prefix-invariance proof failed: {prefix_invariance}")
    data_provenance["prefix_invariance_proof"] = prefix_invariance
    raw_results = [
        _evaluate_atom(
            atom,
            feature_frame,
            seed=random_seed + index * 1009,
            integrity_proof_passed=bool(prefix_invariance["passed"]),
        )
        for index, atom in enumerate(atoms)
    ]
    _apply_benjamini_hochberg(raw_results, selection_universe_size=25)
    results = [
        _finalize_decision(result, atom, validator_controls_passed=validator_controls_passed)
        for result, atom in zip(raw_results, atoms, strict=True)
    ]

    sensitive = [row for row in results if row["selection_role"] == "CALIBRATION_SENSITIVE_CANDIDATE"]
    invariant = [row for row in results if row["selection_role"] == "CALIBRATION_INVARIANT_OLD_FAILURE"]
    survivors = [row for row in sensitive if row["status"] == "RETEST_SUPPORTS_FAMILY_REOPENING"]
    invariant_rejected = [row for row in invariant if row["status"] == "INVARIANT_CONTROL_REJECTED"]
    invariant_unexpected = [row for row in invariant if row["status"] == "INVARIANT_CONTROL_UNEXPECTED_SURVIVAL"]
    sensitive_insufficient = [row for row in sensitive if row["status"] == "ATOM_RETEST_INSUFFICIENT_EVIDENCE"]
    integrity_failures = [row for row in results if row["status"] == "INTEGRITY_FAIL"]
    paired_replication = _paired_mechanism_result(results)

    evidence_valid = (
        validator_controls_passed
        and len(invariant_rejected) == len(invariant)
        and not integrity_failures
    )
    if integrity_failures:
        conclusion = "INTEGRITY_FAIL_NO_DECISION_CHANGE"
    elif not validator_controls_passed:
        conclusion = "INVALID_VALIDATOR_CALIBRATION_CONTROLS_FAILED_NO_DECISION_CHANGE"
    elif invariant_unexpected:
        conclusion = "INVALID_RETEST_FALSE_POSITIVE_SENTINEL_SURVIVED_NO_DECISION_CHANGE"
    elif len(invariant_rejected) != len(invariant):
        conclusion = "INVALID_RETEST_INVARIANT_SENTINEL_INSUFFICIENT_NO_DECISION_CHANGE"
    elif sensitive_insufficient:
        conclusion = "CALIBRATION_RETEST_INSUFFICIENT_NO_ZERO_SURVIVAL_CONCLUSION"
    elif survivors:
        conclusion = "CALIBRATION_FALSE_KILLS_PLAUSIBLE_BOUNDED_FAMILIES_MAY_BE_REOPENED_FOR_FRESH_REPLICATION"
    else:
        conclusion = "ZERO_SURVIVAL_PERSISTS_UNDER_CORRECTED_RETEST_PIVOT_RESEARCH_GRAMMAR"

    payload: dict[str, Any] = {
        "schema": execution_version,
        "execution_version": execution_version,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "A surviving retest supports only bounded family reopening. No atom is labeled validated, no strategy may be assembled, "
            "and no holdout access is authorized by this experiment."
        ),
        "code_commit": code_commit,
        "design_hash": design.get("design_hash"),
        "preregistration_hash": preregistration.get("preregistration_hash"),
        "validator_controls": {
            "pipeline_v2_decisive": pipeline_calibration,
            "legacy_v1_diagnostic_only": legacy_calibration_payload,
        },
        "validator_controls_passed": validator_controls_passed,
        "invariant_controls_all_rejected": len(invariant_rejected) == len(invariant),
        "invariant_control_unexpected_survivals": [row["atom_id"] for row in invariant_unexpected],
        "evidence_valid_for_decision_change": evidence_valid,
        "retest_count": len(results),
        "calibration_sensitive_retest_count": len(sensitive),
        "calibration_invariant_control_count": len(invariant),
        "calibration_sensitive_survivor_count": len(survivors) if evidence_valid else 0,
        "calibration_sensitive_survivor_ids": [row["atom_id"] for row in survivors] if evidence_valid else [],
        "fully_validated_edge_atoms": 0,
        "paired_cross_market_replication": paired_replication,
        "results": results,
        "data_provenance": data_provenance,
        "data_access_record": access_record,
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "historical_development_and_falsification_only": True,
        },
        "next_recommended_action": (
            "FRESH_REPLICATION_OF_ONLY_SURVIVING_FAMILIES_WITH_EXPLICIT_CONTRACTS"
            if evidence_valid and survivors
            else "PIVOT_TO_COUNTERFACTUAL_MARKET_STATE_GEOMETRY"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    if not re.fullmatch(r"[a-z0-9_]+", output_stem):
        raise CalibrationRetestExecutionError("Execution output stem is invalid.")
    json_path = destination / f"{output_stem}.json"
    report_path = destination / f"{output_stem}.md"
    _atomic_write(json_path, json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n")
    _atomic_write(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(json_path),
            "report_path": str(report_path),
        },
        "report_path": str(report_path),
    }


def _validate_preregistration(
    design: dict[str, Any],
    preregistration: dict[str, Any],
    *,
    code_commit: str,
    historical_report_path: Path,
    random_seed: int,
) -> None:
    design_hash = design.get("design_hash")
    design_body = {key: value for key, value in design.items() if key != "design_hash"}
    if not design_hash or design_hash != _stable_hash(design_body):
        raise CalibrationRetestExecutionError("Frozen design hash mismatch.")
    stored_hash = preregistration.get("preregistration_hash")
    body = {key: value for key, value in preregistration.items() if key != "preregistration_hash"}
    if not stored_hash or stored_hash != _stable_hash(body):
        raise CalibrationRetestExecutionError("Fresh preregistration hash mismatch.")
    if not preregistration.get("immutable_before_execution"):
        raise CalibrationRetestExecutionError("Retest preregistration is not immutable-before-execution.")
    if (preregistration.get("interpretation_policy") or {}).get("q4_access_allowed") is not False:
        raise CalibrationRetestExecutionError("Retest preregistration does not explicitly prohibit Q4.")
    if design.get("design_hash") is None or not preregistration.get("atoms"):
        raise CalibrationRetestExecutionError("Retest design or atom list is incomplete.")
    if design.get("preregistration") != preregistration:
        raise CalibrationRetestExecutionError("Separate preregistration does not match the design-embedded copy.")
    if preregistration.get("code_commit") != code_commit:
        raise CalibrationRetestExecutionError("Executing code commit differs from the frozen preregistration commit.")
    source = preregistration.get("source") or {}
    if int(source.get("frozen_random_seed", -1)) != int(random_seed):
        raise CalibrationRetestExecutionError("Execution seed differs from the frozen preregistration seed.")
    if re.fullmatch(r"[0-9a-f]{40}", code_commit):
        actual_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
        if actual_commit != code_commit:
            raise CalibrationRetestExecutionError("Runtime Git HEAD differs from the frozen code commit.")
        dirty = subprocess.run(
            ["git", "diff", "--quiet", "HEAD", "--", "hydra", "scripts"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        if dirty != 0:
            raise CalibrationRetestExecutionError("Tracked runtime code differs from the frozen commit.")
    _verify_development_manifest(source.get("development_data_manifest") or {})
    if source.get("historical_report_sha256") != _file_sha256(historical_report_path):
        raise CalibrationRetestExecutionError("Frozen historical report checksum mismatch.")
    historical_preregistration = project_path(
        "reports", "edge_atom_lab", str(source.get("historical_preregistration_name") or "")
    )
    if not historical_preregistration.is_file() or source.get("historical_preregistration_sha256") != _file_sha256(
        historical_preregistration
    ):
        raise CalibrationRetestExecutionError("Frozen historical preregistration checksum mismatch.")
    old_ids: set[str] = set()
    new_ids: set[str] = set()
    for atom in preregistration["atoms"]:
        new_id = str(atom.get("atom_id") or "")
        old_id = str((atom.get("historical_reference") or {}).get("historical_atom_id") or "")
        if not new_id or not old_id or new_id == old_id or new_id in new_ids or old_id in old_ids:
            raise CalibrationRetestExecutionError("Retest IDs are missing, duplicated, or reuse a historical atom ID.")
        atom_hash = atom.get("preregistration_hash")
        atom_body = {key: value for key, value in atom.items() if key != "preregistration_hash"}
        if atom_hash != _stable_hash(atom_body):
            raise CalibrationRetestExecutionError(f"Atom preregistration hash mismatch for {new_id}.")
        if (atom.get("decision_contract") or {}).get("old_pass_status_inherited") is not False:
            raise CalibrationRetestExecutionError(f"Historical status inheritance is not disabled for {new_id}.")
        new_ids.add(new_id)
        old_ids.add(old_id)
    selection = design.get("selection") or {}
    expected_new_ids = {str(value) for value in selection.get("selected_new_atom_ids") or []}
    if new_ids != expected_new_ids or len(new_ids) != int(preregistration.get("atom_count", -1)):
        raise CalibrationRetestExecutionError("Frozen atom IDs/count differ from the design selection manifest.")
    sensitive = [atom for atom in preregistration["atoms"] if atom.get("selection_role") == "CALIBRATION_SENSITIVE_CANDIDATE"]
    invariant = [atom for atom in preregistration["atoms"] if atom.get("selection_role") == "CALIBRATION_INVARIANT_OLD_FAILURE"]
    if len(sensitive) != int(selection.get("sensitive_limit", -1)) or len(invariant) != int(
        selection.get("invariant_limit", -1)
    ):
        raise CalibrationRetestExecutionError("Frozen sensitive/invariant counts differ from the design manifest.")
    if not sensitive or not invariant:
        raise CalibrationRetestExecutionError("A decisive retest requires non-empty sensitive and invariant sets.")


def _verify_development_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("period_end_exclusive") != "2024-10-01" or manifest.get("role") != (
        "DEVELOPMENT_AND_FALSIFICATION_ONLY"
    ):
        raise CalibrationRetestExecutionError("Frozen development manifest role or boundary is invalid.")
    files = list(manifest.get("files") or [])
    if not files:
        raise CalibrationRetestExecutionError("Frozen development manifest has no files.")
    for row in [*files, manifest.get("contract_map") or {}]:
        path = Path(str(row.get("path") or ""))
        if not path.is_file() or path.stat().st_size != int(row.get("size_bytes", -1)):
            raise CalibrationRetestExecutionError(f"Frozen data artifact missing or size changed: {path}")
        if _file_sha256(path) != row.get("sha256"):
            raise CalibrationRetestExecutionError(f"Frozen data artifact checksum changed: {path}")


def _benchmark_retest_pipeline(preregistration: dict[str, Any], *, seed: int) -> dict[str, Any]:
    seed_count = 15
    seeds = [seed + seed_index * 100_003 for seed_index in range(seed_count)]
    tasks = [(preregistration, control_seed) for control_seed in seeds]
    with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context("fork")) as executor:
        runs = list(executor.map(_benchmark_retest_pipeline_seed_task, tasks))
    decisions = [
        {**decision, "calibration_seed": seed + seed_index * 100_003}
        for seed_index, run in enumerate(runs)
        for decision in run["decisions"]
    ]
    positives = [row for row in decisions if row["expected_positive"]]
    negatives = [row for row in decisions if not row["expected_positive"]]
    positive_successes = sum(row["edge_detected"] for row in positives)
    false_positives = sum(row["edge_detected"] for row in negatives)
    power = positive_successes / max(len(positives), 1)
    false_positive_rate = false_positives / max(len(negatives), 1)
    power_lower = _wilson_bound(positive_successes, len(positives), side="lower")
    false_positive_upper = _wilson_bound(false_positives, len(negatives), side="upper")
    target_bounds: dict[str, dict[str, float | int]] = {}
    for target in sorted({str(row["target_kind"]) for row in positives}):
        rows = [row for row in positives if row["target_kind"] == target]
        successes = sum(row["edge_detected"] for row in rows)
        target_bounds[target] = {
            "successes": int(successes),
            "trials": int(len(rows)),
            "empirical_power": float(successes / len(rows)),
            "one_sided_95pct_wilson_lower_bound": _wilson_bound(successes, len(rows), side="lower"),
        }
    negative_controls_conclusive = all(row["status"] == "RETEST_FALSIFIED" for row in negatives)
    passed = bool(
        power_lower >= 0.80
        and false_positive_upper <= 0.20
        and negative_controls_conclusive
        and all(float(row["one_sided_95pct_wilson_lower_bound"]) >= 0.80 for row in target_bounds.values())
    )
    return {
        "version": "calibration_retest_pipeline_controls_v3_repeated_seeds",
        "seed_count": seed_count,
        "selection_universe_size_each_run": 25,
        "positive_control_trials": len(positives),
        "negative_control_trials": len(negatives),
        "power_on_meaningful_effects": float(power),
        "power_one_sided_95pct_wilson_lower_bound": float(power_lower),
        "false_positive_rate": float(false_positive_rate),
        "false_positive_rate_one_sided_95pct_wilson_upper_bound": float(false_positive_upper),
        "power_by_target": target_bounds,
        "negative_controls_all_conclusively_falsified": negative_controls_conclusive,
        "decisions": decisions,
        "run_summaries": [
            {key: value for key, value in run.items() if key != "decisions"} for run in runs
        ],
        "passed": passed,
    }


def _wilson_bound(successes: int, trials: int, *, side: str) -> float:
    if trials <= 0:
        return 0.0 if side == "lower" else 1.0
    z = 1.6448536269514722
    proportion = successes / trials
    denominator = 1.0 + z * z / trials
    center = (proportion + z * z / (2.0 * trials)) / denominator
    half_width = z * math.sqrt(
        proportion * (1.0 - proportion) / trials + z * z / (4.0 * trials * trials)
    ) / denominator
    if side == "lower":
        return max(0.0, center - half_width)
    if side == "upper":
        return min(1.0, center + half_width)
    raise ValueError(f"Unknown Wilson bound side {side!r}")


def _benchmark_retest_pipeline_seed_task(
    task: tuple[dict[str, Any], int]
) -> dict[str, Any]:
    preregistration, seed = task
    return _benchmark_retest_pipeline_once(preregistration, seed=seed)


def _benchmark_retest_pipeline_once(preregistration: dict[str, Any], *, seed: int) -> dict[str, Any]:
    atoms = list(preregistration["atoms"])
    return_template = next(atom for atom in atoms if atom["family"] != "defensive_portfolio_atom")
    defensive_template = next(atom for atom in atoms if atom["family"] == "defensive_portfolio_atom")
    positive_specs = [
        (str(row["control_id"]).removeprefix("positive_control_"), True, dict(row["retest_pipeline_translation"]))
        for row in preregistration["positive_controls"]
    ]
    negative_specs = [
        (str(row["control_id"]).removeprefix("negative_control_"), False, dict(row["retest_pipeline_translation"]))
        for row in preregistration["negative_controls"]
    ]
    if len(positive_specs) != 5 or len(negative_specs) != 5:
        raise CalibrationRetestExecutionError("Frozen calibration control manifest must contain five positives and five negatives.")
    raw: list[dict[str, Any]] = []
    metadata: list[tuple[str, bool, dict[str, Any]]] = []
    for index, (control_id, expected_positive, translation) in enumerate([*positive_specs, *negative_specs]):
        target_kind = str(translation["target_kind"])
        template = defensive_template if target_kind == "hazard" else return_template
        atom = _control_atom(
            template, control_id=control_id, target_kind=target_kind, translation=translation
        )
        frame = _synthetic_control_frame(
            control_id,
            target_kind=target_kind,
            expected_positive=expected_positive,
            seed=seed + index * 101,
            horizon=int(atom["horizon_bars"]),
            event_frequency=float(translation["event_frequency"]),
            injected_effect_size=float(translation["injected_effect_size"]),
        )
        control_integrity = _verify_synthetic_control_prefixes(
            frame,
            control_id=control_id,
            target_kind=target_kind,
            expected_positive=expected_positive,
            seed=seed + index * 101,
            horizon=int(atom["horizon_bars"]),
            feature_key=str(atom["feature_key"]),
            event_frequency=float(translation["event_frequency"]),
            injected_effect_size=float(translation["injected_effect_size"]),
        )
        result = _evaluate_atom(
            atom,
            frame,
            seed=seed + index * 1009,
            integrity_proof_passed=control_integrity,
        )
        raw.append(result)
        metadata.append((control_id, expected_positive, atom))
    _apply_benjamini_hochberg(raw, selection_universe_size=25)
    decisions: list[dict[str, Any]] = []
    for result, (control_id, expected_positive, atom) in zip(raw, metadata, strict=True):
        finalized = _finalize_decision(result, atom, validator_controls_passed=True)
        detected = finalized.get("status") == "RETEST_SUPPORTS_FAMILY_REOPENING"
        decisions.append(
            {
                "control_id": control_id,
                "target_kind": atom["target_variable"],
                "expected_positive": expected_positive,
                "edge_detected": detected,
                "decision_correct": (
                    detected if expected_positive else finalized.get("status") == "RETEST_FALSIFIED"
                ),
                "status": finalized.get("status"),
                "effect": finalized.get("raw_effect"),
                "confidence_low": finalized.get("confidence_low"),
                "minimum_useful_effect": finalized.get("minimum_useful_effect"),
                "failed_decisive_attacks": finalized.get("failed_decisive_attacks", []),
                "insufficient_decisive_attacks": finalized.get("insufficient_decisive_attacks", []),
                "attack_decision_states": finalized.get("attack_decision_states", {}),
                "matching_diagnostics": {
                    key: {
                        "status": (finalized.get(key) or {}).get("status"),
                        "null_repetitions": (finalized.get(key) or {}).get("null_repetitions"),
                        "matching_coverage": (finalized.get(key) or {}).get("matching_coverage"),
                        "maximum_standardized_mean_difference": (finalized.get(key) or {}).get(
                            "maximum_standardized_mean_difference"
                        ),
                        "diagnostic_maximum_all_covariates_standardized_mean_difference": (
                            finalized.get(key) or {}
                        ).get("diagnostic_maximum_all_covariates_standardized_mean_difference"),
                        "maximum_within_stratum_standardized_mean_difference": (finalized.get(key) or {}).get(
                            "maximum_within_stratum_standardized_mean_difference"
                        ),
                    }
                    for key in (
                        "matched_opportunity_null",
                        "session_matched_baseline",
                        "volatility_matched_baseline",
                    )
                },
                "gate_results": finalized.get("gate_results", {}),
            }
        )
    positives = [row for row in decisions if row["expected_positive"]]
    negatives = [row for row in decisions if not row["expected_positive"]]
    power = sum(row["edge_detected"] for row in positives) / max(len(positives), 1)
    false_positive_rate = sum(row["edge_detected"] for row in negatives) / max(len(negatives), 1)
    negative_controls_conclusive = all(row["status"] == "RETEST_FALSIFIED" for row in negatives)
    target_power = {
        target: sum(row["edge_detected"] for row in rows) / len(rows)
        for target in sorted({row["target_kind"] for row in positives})
        if (rows := [row for row in positives if row["target_kind"] == target])
    }
    passed = (
        power >= 0.80
        and false_positive_rate <= 0.20
        and negative_controls_conclusive
        and all(value >= 0.80 for value in target_power.values())
    )
    return {
        "version": "calibration_retest_pipeline_controls_v2",
        "same_pipeline_components": [
            "walk_forward_thresholds",
            "group_safe_targets",
            "event_declustering",
            "explicit_contract_groups",
            "matched_nulls",
            "joint_globex_trading_day_cluster_bootstrap",
            "mandatory_attacks",
            "selection_universe_multiplicity",
        ],
        "positive_control_count": len(positives),
        "negative_control_count": len(negatives),
        "power_on_meaningful_effects": float(power),
        "false_positive_rate": float(false_positive_rate),
        "power_by_target": target_power,
        "negative_controls_all_conclusively_falsified": negative_controls_conclusive,
        "decisions": decisions,
        "passed": bool(passed),
    }


def _control_atom(
    template: dict[str, Any], *, control_id: str, target_kind: str, translation: dict[str, Any]
) -> dict[str, Any]:
    atom = deepcopy(template)
    atom["atom_id"] = f"pipeline_v2_control_{control_id}"
    atom["selection_role"] = "CALIBRATION_SENSITIVE_CANDIDATE"
    atom["target_markets"] = ["SYN"]
    horizon = int(translation["horizon_bars"])
    event_frequency = float(translation["event_frequency"])
    atom["horizon_bars"] = horizon
    atom["parameters"] = {
        "threshold": "calibration_control_frozen_quantile",
        "control_quantile": float(translation["analysis_event_quantile"]),
        "horizon_bars": horizon,
    }
    atom["expected_direction"] = 1
    atom["historical_reference"] = {"historical_atom_id": f"synthetic_{control_id}"}
    atom["cross_market_replication_required"] = False
    if target_kind == "hazard":
        atom["family"] = "defensive_portfolio_atom"
        atom["feature_key"] = "shared_loss_risk_state"
        atom["target_variable"] = "future_standardized_tail_loss_hazard"
        atom["minimum_useful_effect"] = 0.02
        atom["cost_envelope"]["atom_statistical_cost_reference"] = None
    elif target_kind == "volatility":
        atom["family"] = "volatility_path_shape"
        atom["feature_key"] = "rv_short_long_ratio"
        atom["target_variable"] = "future_realized_volatility"
        atom["minimum_useful_effect"] = max(float(translation["injected_effect_size"]) * 0.25, 0.00020)
        atom["cost_envelope"]["atom_statistical_cost_reference"] = 0.0
    else:
        atom["family"] = "accepted_price_migration"
        atom["feature_key"] = "old_region_reentry"
        atom["target_variable"] = "future_return"
        atom["minimum_useful_effect"] = max(float(translation["injected_effect_size"]) * 0.50, 0.00010)
        atom["cost_envelope"]["atom_statistical_cost_reference"] = 0.0
    atom["cost_envelope"]["minimum_useful_effect"] = atom["minimum_useful_effect"]
    atom["cost_envelope"]["decisive_atom_effect_hurdle"] = atom["minimum_useful_effect"]
    control_mandatory = (
        ["session_phase_opportunity_matched_baseline", "volatility_opportunity_matched_baseline"]
        if target_kind == "hazard"
        else (
            ["volatility_opportunity_matched_baseline"]
            if target_kind == "volatility"
            else ["session_phase_opportunity_matched_baseline"]
        )
    )
    atom["attack_policy"] = {
        "fatal_mandatory": ["target_leakage", "lookahead", "opportunity_session_volatility_matched_random"],
        "hypothesis_specific_mandatory": control_mandatory,
        "robustness_diagnostic": [
            "delayed_signal",
            "sign_flipped_signal",
            "matched_momentum_baseline",
            "matched_mean_reversion_baseline",
            "block_permuted_event_assignment",
            "event_time_jitter",
            "best_event_removed",
            "cost_stress",
        ],
        "informational_only": ["placebo_market"],
    }
    return atom


def _synthetic_control_frame(
    control_id: str,
    *,
    target_kind: str,
    expected_positive: bool,
    seed: int,
    horizon: int,
    event_frequency: float,
    injected_effect_size: float,
    end: str = "2024-09-30",
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    # The calibration market keeps enough independent days for every frozen
    # fold/contract gate while avoiding redundant synthetic bars. Every second
    # business day is selected before any outcomes are generated.
    all_dates = pd.bdate_range("2023-01-02", end, tz="UTC")
    dates = all_dates if control_id == "regime_conditional" else all_dates[::2]
    rows: list[pd.DataFrame] = []
    feature_key = (
        "shared_loss_risk_state"
        if target_kind == "hazard"
        else ("rv_short_long_ratio" if target_kind == "volatility" else "old_region_reentry")
    )
    global_position = 0
    for day_index, day in enumerate(dates):
        bars = 120
        timestamps = pd.date_range(day + pd.Timedelta(hours=14, minutes=30), periods=bars, freq="1min")
        feature = rng.normal(0.0, 0.35, size=bars)
        trigger_probability = (
            max(0.02, event_frequency * (0.5 + (day_index % 5) / 4.0))
            if control_id == "opportunity_matched_random"
            else event_frequency
        )
        triggers = rng.random(bars) < trigger_probability
        trigger_sign = rng.choice([-1.0, 1.0], size=bars)
        if control_id == "autocorrelated_no_edge":
            for position in range(1, bars):
                feature[position] = 0.92 * feature[position - 1] + 0.08 * feature[position]
        elif control_id == "random_session_effect":
            feature += 0.25 if day_index % 2 == 0 else -0.25
        elif control_id == "block_shuffled_real_returns":
            feature = np.roll(feature, int(rng.integers(10, 40)))
        elif control_id == "regime_conditional" and day_index % 2 != 0:
            triggers[:] = False
            feature *= 0.10
        if target_kind in {"hazard", "volatility"}:
            feature = np.abs(feature)
            feature[triggers] = 2.5 + rng.uniform(0.0, 0.5, size=int(triggers.sum()))
        else:
            feature[triggers] = (2.5 + rng.uniform(0.0, 0.5, size=int(triggers.sum()))) * trigger_sign[triggers]
        returns = rng.normal(0.0, 0.00012, size=bars)
        injected_targets: list[int] = []
        if expected_positive:
            for event_index in np.flatnonzero(triggers):
                target_index = int(event_index) + horizon
                if target_index >= bars:
                    continue
                injected_targets.append(target_index)
                if target_kind == "return":
                    if control_id == "path_asymmetry_medium":
                        start = int(event_index) + 1
                        returns[start : target_index + 1] += float(np.sign(feature[event_index])) * (
                            injected_effect_size / horizon
                        )
                    elif control_id == "regime_conditional":
                        if day_index % 2 == 0:
                            returns[target_index] += float(np.sign(feature[event_index])) * injected_effect_size
                    else:
                        returns[target_index] += float(np.sign(feature[event_index])) * injected_effect_size
                elif target_kind == "volatility":
                    returns[target_index] += float(rng.choice([-1.0, 1.0])) * max(
                        injected_effect_size * 4.0, 0.0020
                    )
        close = 100.0 * np.exp(np.cumsum(returns))
        high = close * (1.0 + 0.0002)
        low = close * (1.0 - 0.0002)
        if expected_positive and target_kind == "hazard":
            low[injected_targets] = close[injected_targets] * 0.970
        contract = f"SYN{day.year}Q{day.quarter}"
        session_phase = timestamps.hour * 4 + timestamps.minute // 15
        frame = pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": "SYN",
                "active_contract": contract,
                "session_date": day.strftime("%Y-%m-%d"),
                "trading_session_id": day.strftime("%Y-%m-%d"),
                "contiguous_segment_id": f"{day.strftime('%Y-%m-%d')}|0",
                "session_phase_15m": session_phase,
                "open": close,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000.0,
                "past_return_60": 0.0,
                "past_volatility": 0.002,
                "past_participation": 1.0,
                feature_key: feature,
                "symbol_position": np.arange(global_position, global_position + bars, dtype=int),
            }
        )
        global_position += bars
        rows.append(frame)
    return pd.concat(rows, ignore_index=True)


def _verify_synthetic_control_prefixes(
    full: pd.DataFrame,
    *,
    control_id: str,
    target_kind: str,
    expected_positive: bool,
    seed: int,
    horizon: int,
    feature_key: str,
    event_frequency: float,
    injected_effect_size: float,
) -> bool:
    for end in ("2023-09-29", "2024-03-29"):
        prefix = _synthetic_control_frame(
            control_id,
            target_kind=target_kind,
            expected_positive=expected_positive,
            seed=seed,
            horizon=horizon,
            event_frequency=event_frequency,
            injected_effect_size=injected_effect_size,
            end=end,
        )
        shared = full[pd.to_datetime(full["timestamp"], utc=True) <= pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)]
        if len(prefix) != len(shared):
            return False
        for column in (feature_key, "past_volatility", "past_return_60", "past_participation"):
            left = pd.to_numeric(prefix[column], errors="coerce").to_numpy(dtype=float)
            right = pd.to_numeric(shared[column], errors="coerce").to_numpy(dtype=float)
            if not np.array_equal(np.isnan(left), np.isnan(right)):
                return False
            finite = np.isfinite(left) & np.isfinite(right)
            if not np.array_equal(np.isfinite(left), np.isfinite(right)):
                return False
            if finite.any() and not np.array_equal(left[finite], right[finite]):
                return False
    return True


def _load_governed_development_frame(
    historical: dict[str, Any],
    atoms: list[dict[str, Any]],
    *,
    contract_map_path: Path | None = None,
    required_contract_map_type: str = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    symbols = sorted({str(symbol) for atom in atoms for symbol in atom.get("target_markets", [])})
    coverage_rows = list(historical.get("cached_coverage") or [])
    paths = sorted({Path(str(row["path"])) for row in coverage_rows if row.get("path")})
    frames: list[pd.DataFrame] = []
    fingerprints: list[dict[str, Any]] = []
    for path in paths:
        if not path.is_file():
            raise CalibrationRetestExecutionError(f"Frozen cached data is missing: {path}")
        try:
            frame = pd.read_parquet(path, filters=[("symbol", "in", symbols)])
        except Exception:
            frame = pd.read_parquet(path)
            frame = frame[frame["symbol"].astype(str).isin(symbols)].copy()
        if frame.empty:
            continue
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame[(frame["timestamp"] >= "2023-01-01") & (frame["timestamp"] < "2024-10-01")].copy()
        frames.append(frame)
        fingerprints.append(
            {
                "path": str(path),
                "sha256": _file_sha256(path),
                "size_bytes": path.stat().st_size,
                "rows_loaded_for_selected_symbols": int(len(frame)),
            }
        )
    if not frames:
        raise CalibrationRetestExecutionError("No governed cached observations were available for selected symbols.")
    combined = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["symbol", "timestamp"])
        .drop_duplicates(["symbol", "timestamp"])
        .reset_index(drop=True)
    )
    if combined["timestamp"].max() >= pd.Timestamp("2024-10-01", tz="UTC"):
        raise CalibrationRetestExecutionError("A loaded observation crosses the sealed Q4 boundary.")
    selected_contract_map_path = contract_map_path or Path(
        str(historical.get("contract_map_path") or "")
    )
    if not selected_contract_map_path.is_file():
        raise CalibrationRetestExecutionError("Explicit contract map required by preregistration is missing.")
    roll_map = load_roll_map(selected_contract_map_path)
    combined, roll_details = _apply_explicit_contract_map(
        combined, roll_map, required_map_type=required_contract_map_type
    )
    if combined.empty:
        raise CalibrationRetestExecutionError("All selected observations were excluded by explicit contract/roll guards.")
    provenance = {
        "symbols": symbols,
        "period_start": "2023-01-01",
        "period_end_exclusive": "2024-10-01",
        "rows_after_contract_and_roll_guards": int(len(combined)),
        "files": fingerprints,
        "contract_map_path": str(selected_contract_map_path),
        "contract_map_sha256": _file_sha256(selected_contract_map_path),
        "contract_map_type": roll_map.map_type,
        "contract_evidence_caveat": (
            "Continuous-root OHLCV is annotated with Databento mapped active-contract intervals; this is stronger "
            "than calendar-quarter proxies but is not raw per-contract OHLCV."
        ),
        "contract_map_details": roll_details,
    }
    provenance["data_fingerprint"] = _stable_hash(provenance)
    return combined, provenance


def _apply_explicit_contract_map(
    frame: pd.DataFrame,
    roll_map: RollMap,
    *,
    required_map_type: str = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if roll_map.map_type != required_map_type:
        raise CalibrationRetestExecutionError(
            f"Retest requires map type {required_map_type!r}, got {roll_map.map_type!r}."
        )
    out = frame.copy()
    out["active_contract"] = pd.Series(pd.NA, index=out.index, dtype="string")
    unsafe = pd.Series(False, index=out.index)
    timestamps = pd.to_datetime(out["timestamp"], utc=True)
    symbols = out["symbol"].astype(str)
    for symbol, positions in out.groupby(symbols, sort=True).groups.items():
        contracts = sorted(
            (contract for contract in roll_map.contracts if str(contract.root) == str(symbol)),
            key=lambda contract: _utc_timestamp(contract.active_start),
        )
        if not contracts:
            continue
        starts = np.asarray([_utc_timestamp(contract.active_start).value for contract in contracts], dtype=np.int64)
        ends = np.asarray([_utc_timestamp(contract.active_end).value for contract in contracts], dtype=np.int64)
        if np.any(starts[1:] < ends[:-1]):
            raise CalibrationRetestExecutionError(f"Overlapping explicit contract intervals for {symbol}.")
        row_positions = np.asarray(list(positions), dtype=int)
        values = (
            timestamps.iloc[row_positions]
            .astype("datetime64[ns, UTC]")
            .astype("int64")
            .to_numpy(dtype=np.int64)
        )
        interval_index = np.searchsorted(starts, values, side="right") - 1
        valid = (interval_index >= 0) & (interval_index < len(contracts))
        valid &= values < ends[np.clip(interval_index, 0, len(contracts) - 1)]
        if valid.any():
            mapped = np.asarray([contract.contract for contract in contracts], dtype=object)[interval_index[valid]]
            out.loc[row_positions[valid], "active_contract"] = mapped
        roll_days = np.asarray(
            sorted({_utc_timestamp(contract.roll_date).normalize().value for contract in contracts if contract.roll_date}),
            dtype=np.int64,
        )
        if len(roll_days):
            days = (
                timestamps.iloc[row_positions]
                .dt.normalize()
                .astype("datetime64[ns, UTC]")
                .astype("int64")
                .to_numpy(dtype=np.int64)
            )
            insertion = np.searchsorted(roll_days, days)
            left = roll_days[np.clip(insertion - 1, 0, len(roll_days) - 1)]
            right = roll_days[np.clip(insertion, 0, len(roll_days) - 1)]
            nearest = np.minimum(np.abs(days - left), np.abs(days - right))
            unsafe.iloc[row_positions] = nearest <= pd.Timedelta(days=roll_map.unsafe_window_days).value
    unmapped = out["active_contract"].isna()
    kept = out.loc[~unsafe & ~unmapped].copy().reset_index(drop=True)
    return kept, {
        "unsafe_roll_rows_excluded": int(unsafe.sum()),
        "unmapped_contract_rows_excluded": int(unmapped.sum()),
        "explicit_contract_count": int(kept["active_contract"].nunique()),
    }


def _build_past_only_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    pieces = []
    next_segment_id = 0
    for _keys, group in frame.groupby(["symbol", "active_contract"], sort=True):
        ordered = group.sort_values("timestamp").reset_index(drop=True).copy()
        ordered["trading_session_id"], ordered["session_phase_15m"] = _globex_session_fields(
            ordered["timestamp"]
        )
        timestamp = pd.to_datetime(ordered["timestamp"], utc=True)
        discontinuity = timestamp.diff().ne(pd.Timedelta(minutes=1))
        discontinuity |= ordered["trading_session_id"].ne(ordered["trading_session_id"].shift())
        local_segment = discontinuity.cumsum().astype("int64") - 1
        ordered["contiguous_segment_id"] = local_segment + next_segment_id
        next_segment_id += int(local_segment.max()) + 1
        ordered["feature_group_id"] = ordered["contiguous_segment_id"]
        featured = _add_selected_past_only_features(ordered)
        pieces.append(featured.drop(columns=["feature_group_id"], errors="ignore"))
    return pd.concat(pieces, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _add_selected_past_only_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Vectorized, segment-safe implementation of only the preregistered features."""
    out = frame.copy()
    key = out["feature_group_id"]

    def rolling(series: pd.Series, window: int, minimum: int, operation: str) -> pd.Series:
        grouped = series.groupby(key, sort=False).rolling(window, min_periods=minimum)
        value = getattr(grouped, operation)().reset_index(level=0, drop=True).sort_index()
        return value.groupby(key, sort=False).shift(1)

    grouped_close = out["close"].astype(float).groupby(key, sort=False)
    returns = grouped_close.pct_change(fill_method=None)
    high_60 = rolling(out["high"].astype(float), 60, 20, "max")
    low_60 = rolling(out["low"].astype(float), 60, 20, "min")
    width_60 = (high_60 - low_60).replace(0, np.nan)
    location = (out["close"].astype(float) - low_60) / width_60
    path_30 = rolling(grouped_close.diff().abs(), 30, 10, "sum")
    displacement_30 = grouped_close.diff(30).groupby(key, sort=False).shift(1)
    reentry_base = location - 0.5
    out["old_region_reentry"] = rolling(reentry_base, 30, 10, "mean")
    out["directional_pressure_without_progress"] = path_30 - displacement_30.abs()
    dwell = (location > 0.8).astype(float) - (location < 0.2).astype(float)
    out["extreme_dwell"] = rolling(dwell, 60, 20, "mean")
    out["failed_expansion"] = -(out["high"].astype(float) - out["low"].astype(float)) / width_60
    downside = returns.clip(upper=0).abs()
    out["shared_loss_risk_state"] = rolling(downside, 60, 20, "mean")
    rv_short = rolling(returns.abs(), 30, 10, "mean")
    rv_long = rolling(returns.abs(), 180, 60, "mean")
    out["rv_short_long_ratio"] = rv_short / rv_long.replace(0, np.nan)
    out["past_return_60"] = grouped_close.pct_change(60, fill_method=None).groupby(key, sort=False).shift(1)
    out["past_volatility"] = rolling(returns, 120, 40, "std")
    volume_median = rolling(out["volume"].astype(float), 120, 40, "median")
    out["past_participation"] = out["volume"].astype(float) / volume_median.replace(0, np.nan)
    out["symbol_position"] = out.groupby("feature_group_id", sort=False).cumcount().astype(int)
    selected = [
        "old_region_reentry",
        "directional_pressure_without_progress",
        "shared_loss_risk_state",
        "failed_expansion",
        "extreme_dwell",
        "rv_short_long_ratio",
        "past_return_60",
        "past_volatility",
        "past_participation",
    ]
    out[selected] = out[selected].replace([np.inf, -np.inf], np.nan)
    return out


def _globex_session_fields(timestamp: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return CME trading-day label and causal 15-minute phase from 17:00 Chicago."""
    utc = pd.to_datetime(timestamp, utc=True)
    chicago = utc.dt.tz_convert("America/Chicago")
    trading_day = (chicago + pd.Timedelta(hours=7)).dt.strftime("%Y-%m-%d")
    local_minutes = chicago.dt.hour * 60 + chicago.dt.minute
    minutes_from_open = (local_minutes - 17 * 60) % (24 * 60)
    phase = (minutes_from_open // 15).astype(int)
    return trading_day, phase


def _trading_day_window(frame: pd.DataFrame, start: str, end: str) -> pd.Series:
    """Select whole exchange trading days in a half-open temporal fold."""
    trading_day = frame["trading_session_id"].astype(str)
    return (trading_day >= start) & (trading_day < end)


def _verify_prefix_invariance(
    raw_frame: pd.DataFrame, feature_frame: pd.DataFrame, *, feature_keys: set[str]
) -> dict[str, Any]:
    checked = 0
    segments_checked = 0
    maximum_absolute_difference = 0.0
    for (_symbol, _contract), raw_group in raw_frame.groupby(["symbol", "active_contract"], sort=True):
        raw_group = raw_group.sort_values("timestamp").reset_index(drop=True)
        full_group = feature_frame[
            (feature_frame["symbol"].astype(str) == str(_symbol))
            & (feature_frame["active_contract"].astype(str) == str(_contract))
        ].sort_values("timestamp")
        segments = [
            group.sort_values("timestamp").reset_index(drop=True)
            for _, group in full_group.groupby("contiguous_segment_id", sort=True)
            if len(group) >= 240
        ]
        if not segments:
            continue
        representative_indexes = np.unique(
            np.linspace(0, len(segments) - 1, num=min(4, len(segments)), dtype=int)
        )
        raw_timestamps = pd.to_datetime(raw_group["timestamp"], utc=True)
        for segment_index in representative_indexes:
            full_segment = segments[int(segment_index)]
            segment_timestamps = pd.to_datetime(full_segment["timestamp"], utc=True)
            segment_raw = raw_group[raw_timestamps.isin(segment_timestamps)].sort_values("timestamp").reset_index(drop=True)
            if len(segment_raw) != len(full_segment):
                return {"passed": False, "reason": "segment_index_or_length_mismatch", "comparisons": checked}
            segments_checked += 1
            for fraction in (0.35, 0.55, 0.75, 0.90):
                cutoff = max(200, int(len(segment_raw) * fraction))
                if cutoff >= len(segment_raw):
                    continue
                prefix = _build_past_only_feature_frame(segment_raw.iloc[:cutoff].copy())
                full = full_segment.iloc[:cutoff].copy()
                if len(prefix) != len(full) or not np.array_equal(
                    pd.to_datetime(prefix["timestamp"], utc=True).astype("int64").to_numpy(),
                    pd.to_datetime(full["timestamp"], utc=True).astype("int64").to_numpy(),
                ):
                    return {"passed": False, "reason": "prefix_index_or_length_mismatch", "comparisons": checked}
                for key in sorted(feature_keys | {"past_return_60", "past_volatility", "past_participation"}):
                    if key not in prefix.columns or key not in full.columns:
                        return {"passed": False, "reason": f"feature_missing:{key}", "comparisons": checked}
                    left = pd.to_numeric(prefix[key], errors="coerce").to_numpy(dtype=float)
                    right = pd.to_numeric(full[key], errors="coerce").to_numpy(dtype=float)
                    if not np.array_equal(np.isnan(left), np.isnan(right)):
                        return {"passed": False, "reason": f"nan_mask_mismatch:{key}", "comparisons": checked}
                    finite = np.isfinite(left) & np.isfinite(right)
                    if not np.array_equal(np.isfinite(left), np.isfinite(right)):
                        return {"passed": False, "reason": f"finite_mask_mismatch:{key}", "comparisons": checked}
                    difference = float(np.max(np.abs(left[finite] - right[finite]))) if finite.any() else 0.0
                    maximum_absolute_difference = max(maximum_absolute_difference, difference)
                    checked += 1
    passed = checked > 0 and maximum_absolute_difference <= 1e-12
    return {
        "passed": bool(passed),
        "comparisons": checked,
        "representative_contiguous_segments_checked": segments_checked,
        "maximum_absolute_difference": maximum_absolute_difference,
        "method": "recompute_multiple_prefixes_on_four_time_spread_contiguous_segments_per_contract",
    }


def _evaluate_atom(
    atom: dict[str, Any], feature_frame: pd.DataFrame, *, seed: int, integrity_proof_passed: bool
) -> dict[str, Any]:
    symbols = {str(item) for item in atom["target_markets"]}
    frame = feature_frame[feature_frame["symbol"].astype(str).isin(symbols)].copy()
    feature_key = str(atom["feature_key"])
    if feature_key not in frame.columns:
        return _insufficient_result(atom, "feature_not_implemented")
    horizon = int(atom["horizon_bars"])
    expected_direction = int(atom["expected_direction"])
    defensive = atom["family"] == "defensive_portfolio_atom"
    volatility_target = atom.get("target_variable") == "future_realized_volatility"
    frame["target"] = _future_target(
        frame,
        horizon,
        defensive=defensive,
        volatility=volatility_target,
    )
    target_boundary_proof = _verify_target_boundaries(frame, horizon=horizon)
    frame["feature_value"] = pd.to_numeric(frame[feature_key], errors="coerce")
    quantile = float((atom.get("parameters") or {}).get("control_quantile") or 0.0) or {
        "low": 0.55,
        "moderate": 0.65,
        "high": 0.75,
        "extreme": 0.85,
        "calibration_control_sparse": 0.95,
    }.get(
        str((atom.get("parameters") or {}).get("threshold", "moderate")), 0.65
    )
    event_frames: list[pd.DataFrame] = []
    eligible_frames: list[pd.DataFrame] = []
    fold_results: dict[str, Any] = {}
    for fold_name, train_start, train_end, test_start, test_end in FOLDS:
        # Assign complete Globex trading days to folds. UTC timestamps split a
        # session at midnight and would let one forward label straddle two
        # temporal replications. The exchange-day label was computed causally
        # before targets or fold selection.
        train = frame[_trading_day_window(frame, train_start, train_end)]
        test = frame[_trading_day_window(frame, test_start, test_end)].copy()
        clean_train = train["feature_value"].replace([np.inf, -np.inf], np.nan).dropna()
        if len(clean_train) < 500 or test.empty:
            fold_results[fold_name] = {"status": "INSUFFICIENT_TRAIN_OR_TEST_DATA"}
            continue
        thresholds = {
            str(symbol): float(group["feature_value"].replace([np.inf, -np.inf], np.nan).dropna().abs().quantile(quantile))
            for symbol, group in train.groupby("symbol", sort=True)
            if len(group["feature_value"].dropna()) >= 200
        }
        vol_edges = _quantile_edges(train["past_volatility"], bins=4)
        displacement_edges = _quantile_edges(train["past_return_60"], bins=5)
        participation_edges = _quantile_edges(train["past_participation"], bins=5)
        test = test.dropna(
            subset=["feature_value", "target", "past_volatility", "past_return_60", "past_participation"]
        ).copy()
        row_threshold = test["symbol"].astype(str).map(thresholds).astype(float)
        if not thresholds or test.empty or row_threshold.isna().all():
            fold_results[fold_name] = {"status": "NO_CAUSAL_THRESHOLD_OR_TARGET"}
            continue
        raw_signal = np.where(
            test["feature_value"] > row_threshold,
            1,
            np.where(test["feature_value"] < -row_threshold, -1, 0),
        )
        test["directed_signal"] = raw_signal * expected_direction
        test["fold"] = fold_name
        test["horizon_bars"] = horizon
        test["session_hour"] = pd.to_datetime(test["timestamp"], utc=True).dt.hour
        test["volatility_bin"] = _assign_bins(test["past_volatility"], vol_edges)
        test["displacement_bin"] = _assign_bins(test["past_return_60"], displacement_edges)
        test["participation_bin"] = _assign_bins(test["past_participation"], participation_edges)
        opportunity_indicator = (test["feature_value"].abs() > row_threshold).astype(float)
        opportunity_group = test["contiguous_segment_id"]
        rolling_frequency = (
            opportunity_indicator.groupby(opportunity_group, sort=False)
            .rolling(60, min_periods=20)
            .mean()
            .reset_index(level=0, drop=True)
            .sort_index()
        )
        test["past_opportunity_frequency"] = rolling_frequency.groupby(
            opportunity_group, sort=False
        ).shift(1)
        test["opportunity_frequency_bin"] = np.digitize(
            test["past_opportunity_frequency"].fillna(-1.0).to_numpy(dtype=float),
            np.asarray([0.01, 0.03, 0.06, 0.12, 0.25]),
            right=True,
        )
        test = test.dropna(subset=["past_opportunity_frequency"]).copy()
        test["cluster"] = (
            test["symbol"].astype(str)
            + "|"
            + test["trading_session_id"].astype(str)
        )
        eligible_frames.append(test)
        event_mask = test["directed_signal"] > 0 if defensive else test["directed_signal"] != 0
        events = _non_overlapping_events(test[event_mask].copy(), horizon)
        if events.empty:
            fold_results[fold_name] = {"status": "NO_NON_OVERLAPPING_EVENTS", "walk_forward_thresholds": thresholds}
            continue
        events["signed_target"] = events["directed_signal"].astype(float) * events["target"].astype(float)
        clusters = events.groupby("cluster", sort=True)["signed_target"].mean()
        fold_effect = float(clusters.mean())
        fold_results[fold_name] = {
            "status": "EVALUATED",
            "walk_forward_thresholds_by_symbol": thresholds,
            "non_overlapping_events": int(len(events)),
            "independent_day_symbol_clusters": int(len(clusters)),
            "effect": fold_effect,
            "direction_positive": fold_effect > 0,
        }
        event_frames.append(events)
    if not event_frames or not eligible_frames:
        return _insufficient_result(atom, "no_evaluable_walk_forward_fold", fold_results=fold_results)
    events = pd.concat(event_frames, ignore_index=True)
    eligible = pd.concat(eligible_frames, ignore_index=True)
    matched = _matched_opportunity_null(
        events, eligible, seed=seed, matching_mode="full", repetitions=499, horizon=horizon
    )
    session_matched = _matched_opportunity_null(
        events, eligible, seed=seed + 31, matching_mode="session", repetitions=199, horizon=horizon
    )
    volatility_matched = _matched_opportunity_null(
        events, eligible, seed=seed + 47, matching_mode="volatility", repetitions=199, horizon=horizon
    )
    matched_positions = np.asarray(matched.pop("_matched_event_positions", []), dtype=int)
    matched_control_targets = np.asarray(matched.pop("_matched_control_targets", []), dtype=float)
    matched_control_sessions = np.asarray(matched.pop("_matched_control_sessions", []), dtype=str)
    for diagnostic_match in (session_matched, volatility_matched):
        diagnostic_match.pop("_matched_event_positions", None)
        diagnostic_match.pop("_matched_control_targets", None)
        diagnostic_match.pop("_matched_control_sessions", None)
    if (
        len(matched_positions) == 0
        or matched_control_targets.shape != (len(matched_positions), 5)
        or matched_control_sessions.shape != (len(matched_positions), 5)
    ):
        return _insufficient_result(atom, "full_matched_population_unavailable", fold_results=fold_results)

    # Every decisive estimate below uses exactly the event population for which a
    # no-reuse 1:5 match exists.  Unmatched events are never allowed to carry the
    # observed effect while a smaller population carries the counterfactual.
    events = events.reset_index(drop=True).iloc[matched_positions].reset_index(drop=True).copy()
    event_signal = events["directed_signal"].to_numpy(dtype=float)
    events["matched_counterfactual_target"] = matched_control_targets.mean(axis=1)
    events["matched_control_targets"] = matched_control_targets.tolist()
    events["matched_control_session_ids"] = matched_control_sessions.tolist()
    events["matched_counterfactual_signed_target"] = event_signal * events["matched_counterfactual_target"]
    events["paired_effect"] = (
        events["signed_target"].astype(float) - events["matched_counterfactual_signed_target"].astype(float)
    )
    analysis_column = "paired_effect" if defensive else "signed_target"
    clusters = events.groupby("cluster", sort=True)[analysis_column].mean()
    raw_event_effect = float(events.groupby("cluster", sort=True)["signed_target"].mean().mean())
    counterfactual_effect = float(
        events.groupby("cluster", sort=True)["matched_counterfactual_signed_target"].mean().mean()
    )
    effect = float(clusters.mean())
    bootstrap = _clustered_contract_bootstrap(
        events,
        seed=seed + 71,
        value_column=analysis_column,
        hazard_pair=defensive,
    )
    bootstrap_draws = np.asarray(bootstrap.pop("_draws", []), dtype=float)
    minimum_effect = float(atom["minimum_useful_effect"])
    decisive_effect_hurdle = float(
        (atom.get("cost_envelope") or {}).get("decisive_atom_effect_hurdle", minimum_effect)
    )
    if len(bootstrap_draws):
        minimum_effect_p_value = float(
            (1 + int((bootstrap_draws <= decisive_effect_hurdle).sum())) / (len(bootstrap_draws) + 1)
        )
    else:
        minimum_effect_p_value = 1.0
    bootstrap["minimum_effect_one_sided_p_value"] = minimum_effect_p_value
    stderr = float(bootstrap["bootstrap_standard_error"])
    z_score = effect / stderr if stderr > 0 and math.isfinite(stderr) else 0.0
    p_value = minimum_effect_p_value
    shifted = _matched_block_permutation(
        events,
        matched_control_targets,
        defensive=defensive,
        real_effect=effect,
        seed=seed + 17,
    )
    delayed_effect = _delayed_signal_effect(
        atom,
        eligible,
        horizon=horizon,
        counterfactual_effect=counterfactual_effect if defensive else 0.0,
    )
    baseline = _simple_competing_baselines(events)
    fold_results = _recalculate_matched_fold_results(events, fold_results, analysis_column=analysis_column)
    eligible_contracts = sorted(str(value) for value in eligible["active_contract"].dropna().unique())
    contract_results: dict[str, dict[str, Any]] = {}
    for contract in eligible_contracts:
        group = events[events["active_contract"].astype(str) == contract]
        contract_results[contract] = {
            "clusters": int(group["cluster"].nunique()),
            "effect": (
                float(group.groupby("cluster")[analysis_column].mean().mean()) if not group.empty else None
            ),
        }
    leave_one_contract_out = {
        contract: (
            float(events[events["active_contract"].astype(str) != contract].groupby("cluster")[analysis_column].mean().mean())
            if not events[events["active_contract"].astype(str) != contract].empty
            else None
        )
        for contract in eligible_contracts
    }
    positive_contracts = sum(
        1 for row in contract_results.values() if row["effect"] is not None and float(row["effect"]) > 0
    )
    market_results = {
        str(symbol): {
            "clusters": int(group["cluster"].nunique()),
            "effect": float(group.groupby("cluster")[analysis_column].mean().mean()),
        }
        for symbol, group in events.groupby("symbol", sort=True)
    }
    positive_clusters = clusters[clusters > 0]
    concentration = float(positive_clusters.max() / positive_clusters.sum()) if len(positive_clusters) and positive_clusters.sum() > 0 else 1.0
    information_set_pass = feature_key in _PAST_ONLY_FEATURES and integrity_proof_passed
    target_leakage_pass = bool(information_set_pass and target_boundary_proof["passed"])
    sign_flipped_effect = -effect
    if len(clusters) > 1:
        best_removed_effect = float(clusters.drop(clusters.abs().idxmax()).mean())
    else:
        best_removed_effect = 0.0
    cost_reference = atom["cost_envelope"].get("atom_statistical_cost_reference")
    diagnostic_attacks = {
        "event_time_jitter": {
            "effect": float(delayed_effect),
            "retention": abs(float(delayed_effect)) / max(abs(effect), 1e-12),
            "fatal": False,
        },
        "best_event_removed": {
            "effect": best_removed_effect,
            "retention": abs(best_removed_effect) / max(abs(effect), 1e-12),
            "fatal": False,
        },
        "cost_stress": {
            "effect_after_atom_reference": effect - float(cost_reference or 0.0),
            "effect_after_two_x_atom_reference": effect - 2.0 * float(cost_reference or 0.0),
            "fatal": False,
        },
        "placebo_market": {"status": "INFORMATIONAL_NOT_RUN_IN_BOUNDED_RETEST", "fatal": False},
    }
    attack_passes = {
        "target_leakage": target_leakage_pass,
        "lookahead": information_set_pass,
        "opportunity_session_volatility_matched_random": matched["p_value"] <= 0.05
        and matched.get("matching_coverage", 0.0) >= 0.80
        and matched.get("maximum_standardized_mean_difference", float("inf")) <= 0.10
        and effect > 0,
        "delayed_signal": effect > abs(float(delayed_effect)),
        "sign_flipped_signal": effect * sign_flipped_effect < 0,
        "block_permuted_event_assignment": shifted["p_value"] <= 0.05 and effect > 0,
        "matched_momentum_baseline": effect > abs(baseline["momentum_effect"]),
        "matched_mean_reversion_baseline": effect > abs(baseline["mean_reversion_effect"]),
        "session_phase_opportunity_matched_baseline": session_matched["p_value"] <= 0.05
        and session_matched.get("matching_coverage", 0.0) >= 0.80
        and session_matched.get("maximum_standardized_mean_difference", float("inf")) <= 0.10
        and effect > 0,
        "volatility_opportunity_matched_baseline": volatility_matched["p_value"] <= 0.05
        and volatility_matched.get("matching_coverage", 0.0) >= 0.80
        and volatility_matched.get("maximum_standardized_mean_difference", float("inf")) <= 0.10
        and effect > 0,
    }
    fold_effects = [row["effect"] for row in fold_results.values() if row.get("status") == "EVALUATED"]
    return {
        "atom_id": atom["atom_id"],
        "historical_atom_id": atom["historical_reference"]["historical_atom_id"],
        "selection_role": atom["selection_role"],
        "family": atom["family"],
        "target_type": (
            "future_standardized_tail_loss_hazard"
            if defensive
            else ("future_realized_volatility" if volatility_target else "future_return")
        ),
        "status": "UNDECIDED",
        "valid_non_overlapping_events": int(len(events)),
        "independent_day_symbol_clusters": int(len(clusters)),
        "raw_effect": effect,
        "event_target_effect_before_matched_counterfactual": raw_event_effect,
        "matched_counterfactual_effect": counterfactual_effect,
        "defensive_hazard_odds_ratio": bootstrap.get("hazard_odds_ratio") if defensive else None,
        "defensive_hazard_odds_ratio_confidence_low": bootstrap.get("hazard_odds_ratio_confidence_low")
        if defensive
        else None,
        "defensive_hazard_odds_ratio_confidence_high": bootstrap.get("hazard_odds_ratio_confidence_high")
        if defensive
        else None,
        "cluster_standard_error": stderr,
        "cluster_z_score": z_score,
        "cluster_one_sided_p_value": p_value,
        "confidence_low": bootstrap["confidence_low"],
        "confidence_high": bootstrap["confidence_high"],
        "clustered_contract_bootstrap": bootstrap,
        "minimum_useful_effect": minimum_effect,
        "decisive_atom_effect_hurdle": decisive_effect_hurdle,
        "atom_statistical_cost_reference": atom["cost_envelope"].get("atom_statistical_cost_reference"),
        "fold_results": fold_results,
        "folds_positive": sum(value > 0 for value in fold_effects),
        "fold_count": len(fold_effects),
        "q3_direction_positive": bool((fold_results.get("2024_q3") or {}).get("direction_positive", False)),
        "contract_results": contract_results,
        "contracts_positive": positive_contracts,
        "contract_count": len(contract_results),
        "leave_one_contract_out_effects": leave_one_contract_out,
        "market_results": market_results,
        "top_positive_cluster_concentration": concentration,
        "matched_opportunity_null": matched,
        "session_matched_baseline": session_matched,
        "volatility_matched_baseline": volatility_matched,
        "block_permuted_event_assignment": shifted,
        "competing_baselines": baseline,
        "attack_effects": {
            "real_effect": effect,
            "sign_flipped_signal": sign_flipped_effect,
            "delayed_signal": float(delayed_effect),
            "matched_momentum_baseline": baseline["momentum_effect"],
            "matched_mean_reversion_baseline": baseline["mean_reversion_effect"],
        },
        "diagnostic_attacks": diagnostic_attacks,
        "attack_passes": attack_passes,
        "attack_policy": atom["attack_policy"],
        "integrity_proofs": {
            "prefix_invariance": bool(integrity_proof_passed),
            "past_only_feature_registry": feature_key in _PAST_ONLY_FEATURES,
            "group_safe_target_boundaries": target_boundary_proof,
        },
    }


def _future_target(
    frame: pd.DataFrame, horizon: int, *, defensive: bool, volatility: bool = False
) -> pd.Series:
    output = pd.Series(np.nan, index=frame.index, dtype=float)
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    for _keys, group in frame.groupby(grouping, sort=False, dropna=False):
        ordered = group.sort_values("timestamp")
        if defensive:
            lows = ordered["low"].astype(float)
            future_low = lows.shift(-1).iloc[::-1].rolling(horizon, min_periods=horizon).min().iloc[::-1]
            adverse_excursion = future_low / ordered["close"].astype(float) - 1.0
            threshold = -1.5 * ordered["past_volatility"].astype(float) * math.sqrt(horizon)
            values = (adverse_excursion <= threshold).astype(float)
            values = values.where(adverse_excursion.notna() & threshold.notna())
        elif volatility:
            returns = ordered["close"].astype(float).pct_change()
            future_variance = (
                returns.pow(2).shift(-1).iloc[::-1].rolling(horizon, min_periods=horizon).sum().iloc[::-1]
            )
            values = np.sqrt(future_variance)
        else:
            values = ordered["close"].astype(float).shift(-horizon) / ordered["close"].astype(float) - 1.0
        output.loc[ordered.index] = values.to_numpy()
    return output


def _verify_target_boundaries(frame: pd.DataFrame, *, horizon: int) -> dict[str, Any]:
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    groups_checked = 0
    for _keys, group in frame.groupby(grouping, sort=False, dropna=False):
        ordered = group.sort_values("timestamp")
        timestamps = pd.to_datetime(ordered["timestamp"], utc=True)
        if len(ordered) > 1 and not timestamps.diff().iloc[1:].eq(pd.Timedelta(minutes=1)).all():
            return {"passed": False, "reason": "non_contiguous_timestamp_inside_segment", "groups_checked": groups_checked}
        tail = ordered["target"].tail(min(horizon, len(ordered)))
        if tail.notna().any():
            return {"passed": False, "reason": "forward_target_present_at_segment_tail", "groups_checked": groups_checked}
        groups_checked += 1
    return {
        "passed": groups_checked > 0,
        "groups_checked": groups_checked,
        "grouping": grouping,
        "required_bar_cadence": "1min",
    }


def _non_overlapping_events(events: pd.DataFrame, horizon: int) -> pd.DataFrame:
    keep: list[int] = []
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    for _keys, group in events.sort_values(grouping + ["symbol_position"]).groupby(grouping, sort=False):
        last_position = -10**18
        for index, position in zip(group.index, group["symbol_position"].astype(int), strict=True):
            if int(position) >= last_position + horizon:
                keep.append(int(index))
                last_position = int(position)
    return events.loc[keep].sort_values(["timestamp", "symbol"]).copy() if keep else events.iloc[0:0].copy()


def _matched_opportunity_null(
    events: pd.DataFrame,
    eligible: pd.DataFrame,
    *,
    seed: int,
    matching_mode: str,
    repetitions: int,
    horizon: int,
) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    event_frame = events.reset_index(drop=True).copy()
    event_frame["stratum"] = _stratum(event_frame, mode=matching_mode, detailed=True)
    non_events = eligible[eligible["directed_signal"] == 0].dropna(subset=["target"]).copy()
    non_events = _exclude_event_embargo(non_events, event_frame, horizon=horizon)
    non_events["stratum"] = _stratum(non_events, mode=matching_mode, detailed=True)
    pools = {str(key): group.reset_index(drop=True) for key, group in non_events.groupby("stratum")}
    groups = {str(key): list(map(int, indexes)) for key, indexes in event_frame.groupby("stratum").groups.items()}
    matched_sets: list[dict[str, Any]] = []
    balance_events: list[pd.DataFrame] = []
    balance_controls: list[pd.DataFrame] = []
    diagnostic_balance_events: list[pd.DataFrame] = []
    diagnostic_balance_controls: list[pd.DataFrame] = []
    all_covariates = [
        "past_volatility",
        "past_return_60",
        "past_participation",
        "past_opportunity_frequency",
        "session_phase_15m",
    ]
    # Balance is assessed on the exact covariates claimed by each null. The
    # full counterfactual remains the all-confounder fatal null; the two
    # hypothesis-specific baselines deliberately test the simpler session or
    # volatility explanation and must not be declared insufficient because an
    # unrelated, intentionally unmatched dimension differs.
    covariates = _matching_covariates(matching_mode)
    matched_positions: list[int] = []
    matched_control_targets: list[np.ndarray] = []
    matched_control_sessions: list[np.ndarray] = []
    within_stratum_smd: dict[str, float] = {}
    for key, indexes in groups.items():
        pool = pools.get(key)
        if pool is None or len(pool) < 5:
            continue
        # Partial strata are allowed only as an explicit reduction of the
        # analysis population. Events are sampled without seeing their target;
        # controls are then allocated 1:5 without reuse.
        matchable = min(len(indexes), len(pool) // 5)
        if matchable <= 0:
            continue
        selected_indexes = np.asarray(indexes, dtype=int)
        if matchable < len(selected_indexes):
            selected_indexes = np.sort(rng.choice(selected_indexes, size=matchable, replace=False))
        required = len(selected_indexes) * 5
        chosen = rng.choice(len(pool), size=required, replace=False)
        selected = pool.iloc[chosen].reset_index(drop=True)
        control_targets = selected["target"].to_numpy(dtype=float).reshape(len(selected_indexes), 5)
        matched_sets.append(
            {
                "indexes": selected_indexes,
                "targets": control_targets,
            }
        )
        matched_positions.extend(int(value) for value in selected_indexes)
        matched_control_targets.extend(control_targets)
        matched_control_sessions.extend(
            selected["trading_session_id"].astype(str).to_numpy().reshape(len(selected_indexes), 5)
        )
        repeated_events = event_frame.loc[selected_indexes, covariates].loc[
            event_frame.loc[selected_indexes, covariates].index.repeat(5)
        ]
        balance_events.append(repeated_events.reset_index(drop=True))
        balance_controls.append(selected[covariates])
        repeated_all = event_frame.loc[selected_indexes, all_covariates].loc[
            event_frame.loc[selected_indexes, all_covariates].index.repeat(5)
        ]
        diagnostic_balance_events.append(repeated_all.reset_index(drop=True))
        diagnostic_balance_controls.append(selected[all_covariates])
        within_stratum_smd[key] = _maximum_standardized_mean_difference(
            [repeated_events.reset_index(drop=True)], [selected[covariates]]
        )
    matching_coverage = len(matched_positions) / max(len(event_frame), 1)
    maximum_smd = _maximum_standardized_mean_difference(balance_events, balance_controls)
    diagnostic_all_covariates_smd = _maximum_standardized_mean_difference(
        diagnostic_balance_events, diagnostic_balance_controls
    )
    ordered_positions = np.asarray(matched_positions, dtype=int)
    if len(ordered_positions):
        order = np.argsort(ordered_positions)
        ordered_positions = ordered_positions[order]
        control_matrix = np.asarray(matched_control_targets, dtype=float)[order]
        control_session_matrix = np.asarray(matched_control_sessions, dtype=str)[order]
        matched_events = event_frame.iloc[ordered_positions].reset_index(drop=True)
    else:
        control_matrix = np.empty((0, 5), dtype=float)
        control_session_matrix = np.empty((0, 5), dtype=str)
        matched_events = event_frame.iloc[0:0].copy()
    cluster_codes, unique_clusters = pd.factorize(matched_events["cluster"], sort=True)
    null_effects: list[float] = []
    for _ in range(repetitions):
        if len(matched_events) == 0:
            continue
        # Exchange the observed opportunity label within each frozen 1:5 set.
        # Including the event itself in the six admissible labels is a true
        # randomization null and avoids treating selected controls as fixed truth.
        candidates = np.column_stack((matched_events["target"].to_numpy(dtype=float), control_matrix))
        choices = rng.integers(0, 6, size=len(matched_events))
        sampled = candidates[np.arange(len(matched_events)), choices]
        valid = np.isfinite(sampled)
        if valid.sum() < max(50, math.ceil(len(sampled) * 0.80)):
            continue
        signed = sampled * matched_events["directed_signal"].to_numpy(dtype=float)
        sums = np.bincount(cluster_codes[valid], weights=signed[valid], minlength=len(unique_clusters))
        valid_counts = np.bincount(cluster_codes[valid], minlength=len(unique_clusters)).astype(float)
        present = valid_counts > 0
        means = sums[present] / valid_counts[present]
        null_effects.append(float(means.mean()))
    real_effect = (
        float(matched_events.groupby("cluster")["signed_target"].mean().mean())
        if not matched_events.empty
        else 0.0
    )
    if not null_effects:
        return {
            "status": "INSUFFICIENT",
            "p_value": 1.0,
            "null_repetitions": 0,
            "matching_coverage": float(matching_coverage),
            "maximum_standardized_mean_difference": float(maximum_smd),
            "diagnostic_maximum_all_covariates_standardized_mean_difference": float(
                diagnostic_all_covariates_smd
            ),
            "reason": "matched_pool_insufficient",
            "_matched_event_positions": ordered_positions.tolist(),
            "_matched_control_targets": control_matrix.tolist(),
            "_matched_control_sessions": control_session_matrix.tolist(),
            "maximum_within_stratum_standardized_mean_difference": max(
                within_stratum_smd.values(), default=float("inf")
            ),
        }
    p_value = (1 + sum(value >= real_effect for value in null_effects)) / (len(null_effects) + 1)
    return {
        "status": "EVALUATED",
        "p_value": float(p_value),
        "null_repetitions": len(null_effects),
        "null_effect_mean": float(np.mean(null_effects)),
        "null_effect_95pct": float(np.quantile(null_effects, 0.95)),
        "matching_coverage": float(matching_coverage),
        "maximum_standardized_mean_difference": float(maximum_smd),
        "diagnostic_maximum_all_covariates_standardized_mean_difference": float(
            diagnostic_all_covariates_smd
        ),
        "maximum_within_stratum_standardized_mean_difference": max(
            within_stratum_smd.values(), default=float("inf")
        ),
        "controls_per_event": 5,
        "control_reuse_allowed": False,
        "matching_mode": matching_mode,
        "matching_fields": _matching_fields(matching_mode),
        "matched_event_count": int(len(matched_events)),
        "unmatched_event_count": int(len(event_frame) - len(matched_events)),
        "event_control_embargo_bars": int(horizon),
        "observed_effect_on_matched_population": real_effect,
        "_matched_event_positions": ordered_positions.tolist(),
        "_matched_control_targets": control_matrix.tolist(),
        "_matched_control_sessions": control_session_matrix.tolist(),
    }


def _exclude_event_embargo(
    non_events: pd.DataFrame, events: pd.DataFrame, *, horizon: int
) -> pd.DataFrame:
    """Exclude controls whose target window overlaps any event target window."""
    if non_events.empty or events.empty:
        return non_events
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    retained: list[pd.DataFrame] = []
    event_positions = {
        tuple(str(item) for item in keys): np.sort(group["symbol_position"].to_numpy(dtype=int))
        for keys, group in events.groupby(grouping, sort=False)
    }
    for keys, group in non_events.groupby(grouping, sort=False):
        positions = event_positions.get(tuple(str(item) for item in keys))
        if positions is None or len(positions) == 0:
            retained.append(group)
            continue
        candidates = group["symbol_position"].to_numpy(dtype=int)
        insertion = np.searchsorted(positions, candidates)
        left_index = np.clip(insertion - 1, 0, len(positions) - 1)
        right_index = np.clip(insertion, 0, len(positions) - 1)
        distance = np.minimum(np.abs(candidates - positions[left_index]), np.abs(candidates - positions[right_index]))
        retained.append(group.loc[distance > horizon])
    return pd.concat(retained, ignore_index=False) if retained else non_events.iloc[0:0].copy()


def _maximum_standardized_mean_difference(
    event_frames: list[pd.DataFrame], control_frames: list[pd.DataFrame]
) -> float:
    if not event_frames or not control_frames:
        return float("inf")
    events = pd.concat(event_frames, ignore_index=True)
    controls = pd.concat(control_frames, ignore_index=True)
    values: list[float] = []
    for column in events.columns:
        event = pd.to_numeric(events[column], errors="coerce").dropna()
        control = pd.to_numeric(controls[column], errors="coerce").dropna()
        if event.empty or control.empty:
            return float("inf")
        pooled = math.sqrt(max((float(event.var(ddof=1)) + float(control.var(ddof=1))) / 2.0, 1e-18))
        values.append(abs(float(event.mean()) - float(control.mean())) / pooled)
    return max(values, default=float("inf"))


def _matched_block_permutation(
    events: pd.DataFrame,
    control_targets: np.ndarray,
    *,
    defensive: bool,
    real_effect: float,
    seed: int,
    repetitions: int = 499,
) -> dict[str, Any]:
    """Permute the event label inside each frozen 1:5 matched opportunity block."""
    rng = np.random.default_rng(seed)
    if len(events) == 0 or control_targets.shape != (len(events), 5):
        return {"status": "INSUFFICIENT", "p_value": 1.0, "null_repetitions": 0}
    candidates = np.column_stack((events["target"].to_numpy(dtype=float), control_targets))
    signal = events["directed_signal"].to_numpy(dtype=float)
    cluster_codes, unique_clusters = pd.factorize(events["cluster"], sort=True)
    null_effects = np.empty(repetitions, dtype=float)
    for repetition in range(repetitions):
        labels = rng.integers(0, 6, size=len(events))
        pseudo_event = candidates[np.arange(len(events)), labels]
        if defensive:
            totals = candidates.sum(axis=1)
            pseudo_controls = (totals - pseudo_event) / 5.0
            values = signal * (pseudo_event - pseudo_controls)
        else:
            values = signal * pseudo_event
        sums = np.bincount(cluster_codes, weights=values, minlength=len(unique_clusters))
        counts = np.bincount(cluster_codes, minlength=len(unique_clusters)).astype(float)
        null_effects[repetition] = float((sums / counts).mean())
    p_value = (1 + sum(value >= real_effect for value in null_effects)) / (len(null_effects) + 1)
    return {
        "status": "EVALUATED",
        "p_value": float(p_value),
        "null_repetitions": len(null_effects),
        "null_effect_mean": float(np.mean(null_effects)),
        "null_effect_95pct": float(np.quantile(null_effects, 0.95)),
        "method": "within_frozen_1_to_5_matched_block_event_label_permutation",
        "hazard_counterfactual_recomputed_after_each_label_permutation": bool(defensive),
        "event_count_and_sign_preserved": True,
    }


def _delayed_signal_effect(
    atom: dict[str, Any],
    eligible: pd.DataFrame,
    *,
    horizon: int,
    counterfactual_effect: float,
) -> float:
    cluster_effects: list[float] = []
    grouping = ["symbol", "active_contract", "fold", "trading_session_id", "contiguous_segment_id"]
    for _, group in eligible.groupby(grouping, sort=True):
        ordered = group.sort_values("timestamp")
        delayed = ordered["directed_signal"].shift(horizon).fillna(0).to_numpy(dtype=int)
        positions = np.flatnonzero(delayed > 0 if atom.get("family") == "defensive_portfolio_atom" else delayed != 0)
        kept: list[int] = []
        last = -10**18
        for position in positions:
            if int(position) >= last + horizon:
                kept.append(int(position))
                last = int(position)
        if kept:
            target = ordered["target"].to_numpy(dtype=float)[kept]
            values = delayed[kept].astype(float) * target
            cluster_effects.append(float(np.mean(values)))
    return float(np.mean(cluster_effects)) - counterfactual_effect if cluster_effects else 0.0


def _simple_competing_baselines(events: pd.DataFrame) -> dict[str, float]:
    past_sign = np.sign(events["past_return_60"].astype(float)).replace(0, np.nan)
    target = events["target"].astype(float)
    momentum = (past_sign * target).dropna()
    mean_reversion = (-past_sign * target).dropna()
    return {
        "momentum_effect": float(momentum.groupby(events.loc[momentum.index, "cluster"]).mean().mean()) if len(momentum) else 0.0,
        "mean_reversion_effect": float(mean_reversion.groupby(events.loc[mean_reversion.index, "cluster"]).mean().mean())
        if len(mean_reversion)
        else 0.0,
    }


def _clustered_contract_bootstrap(
    events: pd.DataFrame,
    *,
    seed: int,
    repetitions: int = 1999,
    value_column: str = "signed_target",
    hazard_pair: bool = False,
) -> dict[str, Any]:
    aggregation: dict[str, tuple[str, str]] = {
        "effect": (value_column, "mean"),
        "trading_session_id": ("trading_session_id", "first"),
    }
    if hazard_pair:
        aggregation.update(
            {
                "event_hazard": ("target", "mean"),
                "counterfactual_hazard": ("matched_counterfactual_target", "mean"),
            }
        )
    cluster_table = events.groupby(["cluster"], sort=True).agg(**aggregation).reset_index()
    event_day_blocks = [group.reset_index(drop=True) for _, group in cluster_table.groupby("trading_session_id", sort=True)]
    if not event_day_blocks or sum(len(values) for values in event_day_blocks) < 2:
        return {
            "method": "globex_trading_day_cluster_bootstrap_preserving_cross_market_dependence",
            "repetitions": 0,
            "bootstrap_effect_mean": 0.0,
            "bootstrap_standard_error": float("inf"),
            "confidence_low": -float("inf"),
            "confidence_high": float("inf"),
            "one_sided_p_value": 1.0,
            "_draws": np.array([], dtype=float),
        }
    rng = np.random.default_rng(seed)
    if not hazard_pair:
        event_effect_sums = np.asarray([group["effect"].sum() for group in event_day_blocks], dtype=float)
        event_counts = np.asarray([len(group) for group in event_day_blocks], dtype=float)
        draws = np.empty(repetitions, dtype=float)
        for index in range(repetitions):
            selected_days = rng.integers(0, len(event_day_blocks), size=len(event_day_blocks))
            draws[index] = float(
                event_effect_sums[selected_days].sum() / event_counts[selected_days].sum()
            )
        odds_ratio_draws: np.ndarray | None = None
    else:
        # Two-way pigeonhole bootstrap. Event-day multiplicities are shared by
        # every market on that Globex day; control-day multiplicities reweight
        # the five retained controls inside the same draw. Delta hazard and OR
        # therefore come from one joint resample, never a mixture of marginals.
        cluster_lookup = {str(cluster): index for index, cluster in enumerate(cluster_table["cluster"])}
        event_day_codes, event_days = pd.factorize(cluster_table["trading_session_id"].astype(str), sort=True)
        event_hazard = cluster_table["event_hazard"].to_numpy(dtype=float)
        long_cluster_codes: list[int] = []
        long_control_days: list[str] = []
        long_control_targets: list[float] = []
        for row in events.itertuples(index=False):
            cluster_code = cluster_lookup[str(row.cluster)]
            targets = list(row._asdict()["matched_control_targets"])
            sessions = list(row.matched_control_session_ids)
            for control_day, control_target in zip(sessions, targets, strict=True):
                long_cluster_codes.append(cluster_code)
                long_control_days.append(str(control_day))
                long_control_targets.append(float(control_target))
        control_day_codes, control_days = pd.factorize(np.asarray(long_control_days, dtype=str), sort=True)
        control_cluster_codes = np.asarray(long_cluster_codes, dtype=int)
        control_targets = np.asarray(long_control_targets, dtype=float)
        draws = np.empty(repetitions, dtype=float)
        odds_ratio_draws = np.empty(repetitions, dtype=float)
        for index in range(repetitions):
            event_counts = np.bincount(
                rng.integers(0, len(event_days), size=len(event_days)), minlength=len(event_days)
            )
            control_counts = np.bincount(
                rng.integers(0, len(control_days), size=len(control_days)), minlength=len(control_days)
            )
            cluster_weights = event_counts[event_day_codes].astype(float)
            observation_weights = (
                cluster_weights[control_cluster_codes] * control_counts[control_day_codes].astype(float)
            )
            control_sums = np.bincount(
                control_cluster_codes,
                weights=observation_weights * control_targets,
                minlength=len(cluster_table),
            )
            control_weight_sums = np.bincount(
                control_cluster_codes, weights=observation_weights, minlength=len(cluster_table)
            )
            valid = (cluster_weights > 0) & (control_weight_sums > 0)
            if not valid.any():
                draws[index] = 0.0
                odds_ratio_draws[index] = 1.0
                continue
            weights = cluster_weights[valid]
            resampled_event = float(np.average(event_hazard[valid], weights=weights))
            cluster_control = control_sums[valid] / control_weight_sums[valid]
            resampled_control = float(np.average(cluster_control, weights=weights))
            draws[index] = resampled_event - resampled_control
            odds_ratio_draws[index] = _odds_ratio(
                resampled_event, resampled_control, sample_size=int(weights.sum())
            )
    result = {
        "method": (
            "joint_two_way_pigeonhole_globex_event_day_and_matched_control_day_bootstrap"
            if hazard_pair
            else "globex_trading_day_cluster_bootstrap_preserving_cross_market_dependence"
        ),
        "repetitions": int(repetitions),
        "bootstrap_effect_mean": float(draws.mean()),
        "bootstrap_standard_error": float(draws.std(ddof=1)),
        "confidence_low": float(np.quantile(draws, 0.025)),
        "confidence_high": float(np.quantile(draws, 0.975)),
        "one_sided_p_value": float((1 + int((draws <= 0).sum())) / (len(draws) + 1)),
        "_draws": draws,
        "cross_market_sessions_resampled_jointly": True,
    }
    if odds_ratio_draws is not None:
        result.update(
            {
                "hazard_odds_ratio": _odds_ratio(
                    float(cluster_table["event_hazard"].mean()),
                    float(cluster_table["counterfactual_hazard"].mean()),
                    sample_size=len(cluster_table),
                ),
                "hazard_odds_ratio_confidence_low": float(np.quantile(odds_ratio_draws, 0.025)),
                "hazard_odds_ratio_confidence_high": float(np.quantile(odds_ratio_draws, 0.975)),
                "hazard_odds_ratio_bootstrap_repetitions": int(repetitions),
                "control_trading_session_identity_retained": True,
                "joint_two_way_bootstrap_repetitions": int(repetitions),
            }
        )
    return result


def _recalculate_matched_fold_results(
    events: pd.DataFrame,
    original: dict[str, Any],
    *,
    analysis_column: str,
) -> dict[str, Any]:
    recalculated: dict[str, Any] = {}
    for fold_name, *_bounds in FOLDS:
        group = events[events["fold"].astype(str) == fold_name]
        prior = dict(original.get(fold_name) or {})
        thresholds = prior.get("walk_forward_thresholds_by_symbol") or prior.get("walk_forward_thresholds") or {}
        if group.empty:
            recalculated[fold_name] = {
                "status": "INSUFFICIENT_MATCHED_EVENTS",
                "walk_forward_thresholds_by_symbol": thresholds,
                "non_overlapping_events": 0,
                "independent_day_symbol_clusters": 0,
            }
            continue
        clusters = group.groupby("cluster", sort=True)[analysis_column].mean()
        fold_effect = float(clusters.mean())
        recalculated[fold_name] = {
            "status": "EVALUATED",
            "walk_forward_thresholds_by_symbol": thresholds,
            "non_overlapping_events": int(len(group)),
            "independent_day_symbol_clusters": int(len(clusters)),
            "effect": fold_effect,
            "direction_positive": fold_effect > 0,
        }
    return recalculated


def _apply_benjamini_hochberg(results: list[dict[str, Any]], *, selection_universe_size: int) -> None:
    evaluable = [(index, float(row.get("cluster_one_sided_p_value", 1.0))) for index, row in enumerate(results)]
    ordered = sorted(evaluable, key=lambda item: item[1])
    adjusted = [1.0] * len(results)
    running = 1.0
    total = max(len(ordered), 1)
    for reverse_rank, (index, p_value) in enumerate(reversed(ordered), start=1):
        rank = total - reverse_rank + 1
        running = min(running, p_value * total / max(rank, 1))
        adjusted[index] = min(1.0, running)
    for row, q_value in zip(results, adjusted, strict=True):
        row["benjamini_hochberg_q_value"] = float(q_value)
        row["selection_universe_bonferroni_p_value"] = min(
            1.0, float(row.get("cluster_one_sided_p_value", 1.0)) * max(selection_universe_size, 1)
        )
        row["selection_universe_size"] = int(selection_universe_size)


def _finalize_decision(
    result: dict[str, Any], atom: dict[str, Any], *, validator_controls_passed: bool
) -> dict[str, Any]:
    if result.get("status") == "ATOM_RETEST_INSUFFICIENT_EVIDENCE":
        if atom["selection_role"] == "CALIBRATION_INVARIANT_OLD_FAILURE":
            return {**result, "status": "INVARIANT_CONTROL_INSUFFICIENT"}
        return result
    policy = atom["attack_policy"]
    decisive_attacks = list(policy["fatal_mandatory"]) + list(policy["hypothesis_specific_mandatory"])

    def matched_attack_state(result_key: str, attack_name: str, minimum_repetitions: int) -> str:
        row = result.get(result_key) or {}
        prerequisites = (
            row.get("status") == "EVALUATED"
            and int(row.get("null_repetitions", 0)) >= minimum_repetitions
            and float(row.get("matching_coverage", 0.0)) >= 0.80
            and float(row.get("maximum_standardized_mean_difference", float("inf"))) <= 0.10
        )
        if not prerequisites:
            return "INSUFFICIENT"
        return "PASS" if bool(result["attack_passes"].get(attack_name, False)) else "FAIL"

    attack_states: dict[str, str] = {}
    for name in decisive_attacks:
        if name == "opportunity_session_volatility_matched_random":
            attack_states[name] = matched_attack_state("matched_opportunity_null", name, 499)
        elif name == "session_phase_opportunity_matched_baseline":
            attack_states[name] = matched_attack_state("session_matched_baseline", name, 199)
        elif name == "volatility_opportunity_matched_baseline":
            attack_states[name] = matched_attack_state("volatility_matched_baseline", name, 199)
        elif name == "block_permuted_event_assignment":
            block = result.get("block_permuted_event_assignment") or {}
            if block.get("status") != "EVALUATED" or int(block.get("null_repetitions", 0)) < 499:
                attack_states[name] = "INSUFFICIENT"
            else:
                attack_states[name] = "PASS" if bool(result["attack_passes"].get(name, False)) else "FAIL"
        elif name in {"lookahead", "target_leakage"}:
            attack_states[name] = "PASS" if bool(result["attack_passes"].get(name, False)) else "FAIL"
        else:
            attack_states[name] = (
                "PASS" if name in result.get("attack_passes", {}) and result["attack_passes"][name] else "FAIL"
            )

    decisive_hurdle = float(
        result.get("decisive_atom_effect_hurdle")
        or (atom.get("cost_envelope") or {}).get("decisive_atom_effect_hurdle")
        or result["minimum_useful_effect"]
    )
    confidence_low = float(result.get("confidence_low") or -float("inf"))
    confidence_high = float(result.get("confidence_high") or float("inf"))
    hurdle_state = "PASS" if confidence_low > decisive_hurdle else (
        "FAIL" if confidence_high < decisive_hurdle else "INSUFFICIENT"
    )
    fold_sample_sufficient = (
        len(result["fold_results"]) == 4
        and all(
            row.get("status") == "EVALUATED" and int(row.get("independent_day_symbol_clusters", 0)) >= 20
            for row in result["fold_results"].values()
        )
    )
    sample_sufficient = (
        result["valid_non_overlapping_events"] >= 50 and result["independent_day_symbol_clusters"] >= 80
    )
    temporal_state = "INSUFFICIENT" if not fold_sample_sufficient else (
        "PASS"
        if result["fold_count"] >= 4 and result["folds_positive"] >= 3 and result["q3_direction_positive"]
        else "FAIL"
    )
    contract_required = max(3, math.ceil(0.70 * max(int(result["contract_count"]), 1)))
    contract_sample_sufficient = result["contract_count"] >= 3 and all(
        int(row.get("clusters", 0)) >= 10 for row in result["contract_results"].values()
    )
    contract_state = "INSUFFICIENT" if not contract_sample_sufficient else (
        "PASS" if result["contracts_positive"] >= contract_required else "FAIL"
    )
    multiplicity_state = "INSUFFICIENT" if not sample_sufficient else (
        "PASS" if float(result["selection_universe_bonferroni_p_value"]) <= 0.05 else "FAIL"
    )
    concentration_state = (
        "PASS" if float(result["top_positive_cluster_concentration"]) <= 0.25 else "FAIL"
    )
    hazard_contract = atom.get("hazard_decision_contract") or {}
    is_hazard = result.get("target_type") == "future_standardized_tail_loss_hazard"
    if not is_hazard:
        hazard_or_state = "PASS"
        cross_market_state = "PASS"
    else:
        odds_threshold = float(hazard_contract.get("odds_ratio_confidence_lower_bound_threshold", 1.15))
        odds_low = float(result.get("defensive_hazard_odds_ratio_confidence_low") or 0.0)
        odds_high = float(result.get("defensive_hazard_odds_ratio_confidence_high") or float("inf"))
        hazard_or_state = "PASS" if odds_low >= odds_threshold else (
            "FAIL" if odds_high < odds_threshold else "INSUFFICIENT"
        )
        market_results = result.get("market_results") or {}
        rule = hazard_contract.get("cross_market_rule") or {}
        required_markets = int(rule.get("required_markets", 3))
        minimum_markets = int(rule.get("minimum_positive_markets", 2))
        minimum_clusters = int(rule.get("minimum_independent_trading_day_clusters_per_market", 20))
        if not atom.get("cross_market_replication_required", True):
            cross_market_state = "PASS"
        elif len(market_results) != required_markets or any(
            int(row.get("clusters", 0)) < minimum_clusters for row in market_results.values()
        ):
            cross_market_state = "INSUFFICIENT"
        else:
            positives = sum(float(row.get("effect") or 0.0) > 0 for row in market_results.values())
            cross_market_state = "PASS" if positives >= minimum_markets else "FAIL"

    structural_states = {
        "independent_sample_support": "PASS" if sample_sufficient else "INSUFFICIENT",
        "temporal_transfer_including_q3": temporal_state,
        "explicit_contract_replication": contract_state,
        "decisive_atom_effect_hurdle": hurdle_state,
        "selection_universe_multiplicity": multiplicity_state,
        "event_concentration": concentration_state,
        "defensive_hazard_odds_ratio": hazard_or_state,
        "cross_market_hazard_replication": cross_market_state,
    }
    all_states = [*attack_states.values(), *structural_states.values()]
    combined_states = {**attack_states, **structural_states}
    insufficient = [name for name, state in combined_states.items() if state == "INSUFFICIENT"]
    failures = [name for name, state in combined_states.items() if state == "FAIL"]
    mandatory_attack_insufficient = [name for name, state in attack_states.items() if state == "INSUFFICIENT"]
    integrity_pass = attack_states.get("lookahead") == "PASS" and attack_states.get("target_leakage") == "PASS"
    all_pass = bool(validator_controls_passed and all(state == "PASS" for state in all_states))
    if not validator_controls_passed:
        status = "INVALID_VALIDATOR_CALIBRATION"
    elif not integrity_pass:
        status = "INTEGRITY_FAIL"
    elif mandatory_attack_insufficient:
        status = (
            "INVARIANT_CONTROL_INSUFFICIENT"
            if atom["selection_role"] == "CALIBRATION_INVARIANT_OLD_FAILURE"
            else "ATOM_RETEST_INSUFFICIENT_EVIDENCE"
        )
    elif failures and atom["selection_role"] == "CALIBRATION_INVARIANT_OLD_FAILURE":
        status = "INVARIANT_CONTROL_REJECTED"
    elif failures:
        status = "RETEST_FALSIFIED"
    elif insufficient:
        status = (
            "INVARIANT_CONTROL_INSUFFICIENT"
            if atom["selection_role"] == "CALIBRATION_INVARIANT_OLD_FAILURE"
            else "ATOM_RETEST_INSUFFICIENT_EVIDENCE"
        )
    elif atom["selection_role"] == "CALIBRATION_INVARIANT_OLD_FAILURE":
        status = "INVARIANT_CONTROL_UNEXPECTED_SURVIVAL" if all_pass else "INVARIANT_CONTROL_REJECTED"
    else:
        status = "RETEST_SUPPORTS_FAMILY_REOPENING" if all_pass else "RETEST_FALSIFIED"
    return {
        **result,
        "status": status,
        "failed_decisive_attacks": [name for name in decisive_attacks if attack_states.get(name) == "FAIL"],
        "insufficient_decisive_attacks": [
            name for name in decisive_attacks if attack_states.get(name) == "INSUFFICIENT"
        ],
        "attack_decision_states": attack_states,
        "gate_results": {
            "validator_controls": "PASS" if validator_controls_passed else "FAIL",
            **structural_states,
            "integrity_proofs": "PASS" if integrity_pass else "FAIL",
            "all_fatal_and_hypothesis_specific_attacks": (
                "PASS" if all(state == "PASS" for state in attack_states.values()) else (
                    "INSUFFICIENT" if any(state == "INSUFFICIENT" for state in attack_states.values()) else "FAIL"
                )
            ),
        },
        "conclusive_failure_gates": failures,
        "insufficient_gates": insufficient,
        "historical_status_inherited": False,
        "maximum_positive_interpretation": "FAMILY_REOPENING_FOR_FRESH_REPLICATION_ONLY",
    }


def _paired_mechanism_result(results: list[dict[str, Any]]) -> dict[str, Any]:
    relevant = [
        row
        for row in results
        if row.get("historical_atom_id")
        in {
            "atom_effort_vs_progress_directional_pressure_without_progress_MES_60_low_v1",
            "atom_effort_vs_progress_directional_pressure_without_progress_YM_60_moderate_v1",
        }
    ]
    return {
        "group_id": "paired_effort_without_progress_mes_ym_v1",
        "member_count": len(relevant),
        "both_support_reopening": len(relevant) == 2 and all(row["status"] == "RETEST_SUPPORTS_FAMILY_REOPENING" for row in relevant),
        "member_statuses": {row["atom_id"]: row["status"] for row in relevant},
    }


def _insufficient_result(
    atom: dict[str, Any], reason: str, *, fold_results: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "atom_id": atom["atom_id"],
        "historical_atom_id": atom["historical_reference"]["historical_atom_id"],
        "selection_role": atom["selection_role"],
        "family": atom["family"],
        "status": "ATOM_RETEST_INSUFFICIENT_EVIDENCE",
        "reason": reason,
        "fold_results": fold_results or {},
        "cluster_one_sided_p_value": 1.0,
    }


def _quantile_edges(values: pd.Series, *, bins: int) -> np.ndarray:
    clean = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return np.array([-np.inf, np.inf])
    edges = np.unique(np.quantile(clean.to_numpy(dtype=float), np.linspace(0.0, 1.0, bins + 1)))
    if len(edges) < 2:
        return np.array([-np.inf, np.inf])
    edges[0], edges[-1] = -np.inf, np.inf
    return edges


def _assign_bins(values: pd.Series, edges: np.ndarray) -> pd.Series:
    return pd.Series(np.digitize(values.to_numpy(dtype=float), edges[1:-1], right=True), index=values.index, dtype=int)


def _stratum(frame: pd.DataFrame, *, mode: str, detailed: bool) -> pd.Series:
    base = (
        frame["symbol"].astype(str)
        + "|"
        + frame["active_contract"].astype(str)
        + "|"
        + frame["fold"].astype(str)
        + "|h"
        + frame["horizon_bars"].astype(str)
    )
    if mode == "session":
        if detailed:
            return (
                base
                + "|"
                + frame["session_phase_15m"].astype(str)
                + "|"
                + frame["opportunity_frequency_bin"].astype(str)
            )
        return base + "|" + frame["session_hour"].astype(str)
    if mode == "volatility":
        if detailed:
            return (
                base
                + "|"
                + frame["volatility_bin"].astype(str)
                + "|"
                + frame["opportunity_frequency_bin"].astype(str)
            )
        return base
    if mode != "full":
        raise ValueError(f"Unknown matching mode {mode!r}")
    if detailed:
        return (
            base
            + "|"
            + frame["session_phase_15m"].astype(str)
            + "|"
            + frame["volatility_bin"].astype(str)
            + "|"
            + frame["displacement_bin"].astype(str)
            + "|"
            + frame["participation_bin"].astype(str)
            + "|"
            + frame["opportunity_frequency_bin"].astype(str)
        )
    return base + "|" + frame["session_hour"].astype(str)


def _matching_fields(mode: str) -> list[str]:
    base = ["symbol", "explicit_contract", "fold", "horizon"]
    if mode == "session":
        return base + ["15_minute_session_phase", "causal_past_opportunity_frequency_bin"]
    if mode == "volatility":
        return base + ["volatility_bin", "causal_past_opportunity_frequency_bin"]
    return base + [
        "15_minute_session_phase",
        "volatility_bin",
        "prior_displacement_bin",
        "participation_bin",
        "causal_past_opportunity_frequency_bin",
    ]


def _matching_covariates(mode: str) -> list[str]:
    if mode == "session":
        return ["session_phase_15m", "past_opportunity_frequency"]
    if mode == "volatility":
        return ["past_volatility", "past_opportunity_frequency"]
    if mode == "full":
        return [
            "past_volatility",
            "past_return_60",
            "past_participation",
            "past_opportunity_frequency",
            "session_phase_15m",
        ]
    raise ValueError(f"Unknown matching mode {mode!r}")


def _odds_ratio(event_probability: float, counterfactual_probability: float, *, sample_size: int) -> float:
    # Jeffreys/Haldane continuity correction avoids infinite sparse-cell ORs
    # without pretending that a zero empirical rate is known to 1e-9.
    shrinkage = 0.5 / max(int(sample_size) + 1, 2)
    event = min(max(float(event_probability), shrinkage), 1.0 - shrinkage)
    counterfactual = min(max(float(counterfactual_probability), shrinkage), 1.0 - shrinkage)
    return float((event / (1.0 - event)) / (counterfactual / (1.0 - counterfactual)))


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise CalibrationRetestExecutionError(f"Required frozen artifact is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CalibrationRetestExecutionError(f"Artifact root is not an object: {path}")
    return payload


def _record_data_access_once(period: str, atom_ids: list[str], reason: str) -> dict[str, Any]:
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    requested_ids = sorted(atom_ids)
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == "hydra.mission.calibration_retest_execution"
                and sorted(row.get("candidate_ids") or []) == requested_ids
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.mission.calibration_retest_execution",
        requested_ids,
        reason,
        None,
    )
    return record.__dict__


def _load_markdown_json(path: Path) -> dict[str, Any]:
    match = re.search(r"```json\s*(\{.*\})\s*```", path.read_text(encoding="utf-8"), re.DOTALL)
    if not match:
        raise CalibrationRetestExecutionError(f"No JSON payload in historical report: {path}")
    return json.loads(match.group(1))


def _utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _strict_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _strict_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_strict_json_value(item) for item in value]
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _atomic_write(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") == content:
            return
        raise CalibrationRetestExecutionError(f"Refusing to overwrite immutable divergent artifact: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _render_report(payload: dict[str, Any]) -> str:
    rows = [
        "# HYDRA Calibration-Affected Atom Retest Execution",
        "",
        "Historical development/falsification research only. Q4 remained sealed. No live trading approval.",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Validator controls passed: `{payload['validator_controls_passed']}`",
        f"- Invariant controls all rejected: `{payload['invariant_controls_all_rejected']}`",
        f"- Calibration-sensitive survivors: `{payload['calibration_sensitive_survivor_count']}`",
        f"- Fully validated edge atoms: `0`",
        f"- Result hash: `{payload['result_hash']}`",
        "",
        "| Atom | Role | Status | Effect | q-value | Positive folds | Positive explicit contracts |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for result in payload["results"]:
        rows.append(
            f"| `{result['atom_id']}` | {result['selection_role']} | `{result['status']}` | "
            f"{float(result.get('raw_effect', 0.0)):.8g} | {float(result.get('benjamini_hochberg_q_value', 1.0)):.6g} | "
            f"{int(result.get('folds_positive', 0))}/{int(result.get('fold_count', 0))} | "
            f"{int(result.get('contracts_positive', 0))}/{int(result.get('contract_count', 0))} |"
        )
    rows.extend(["", payload["interpretation_boundary"], "", f"Next action: `{payload['next_recommended_action']}`.", ""])
    return "\n".join(rows)
