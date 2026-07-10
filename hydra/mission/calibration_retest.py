from __future__ import annotations

import hashlib
import json
import math
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

from hydra.calibration.cost_hurdle_calibration import calibrated_atom_cost_policy
from hydra.calibration.negative_controls import negative_control_specs
from hydra.calibration.positive_controls import positive_control_specs
from hydra.utils.config import project_path


DESIGN_VERSION = "calibration_affected_atom_retest_design_v1"
PREREGISTRATION_SCHEMA = "edge_atom_calibration_retest_preregistration_v1"
DEFAULT_HISTORICAL_REPORT = (
    "reports/edge_atom_lab/"
    "edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md"
)
DEFAULT_HISTORICAL_PREREGISTRATION = (
    "reports/edge_atom_lab/"
    "edge_atom_preregistration_20260710T101052+0000_edge_atom_discovery_replication_v1_final.json"
)
DESIGN_JSON_NAME = "calibration_affected_atom_retest_design.json"
PREREGISTRATION_JSON_NAME = "calibration_affected_atom_retest_preregistration.json"
REPORT_NAME = "calibration_affected_atom_retest_design.md"
MAX_SENSITIVE_RETESTS = 8
MAX_INVARIANT_RETESTS = 4


_SENSITIVE_PRIORITY_IDS = (
    "atom_accepted_price_migration_old_region_reentry_MNQ_60_extreme_v1",
    "atom_effort_vs_progress_directional_pressure_without_progress_MES_60_low_v1",
    "atom_effort_vs_progress_directional_pressure_without_progress_YM_60_moderate_v1",
    "atom_defensive_portfolio_atom_shared_loss_risk_state_MYM_30_moderate_v1",
)
_INVARIANT_PRIORITY_IDS = (
    "atom_volatility_path_shape_failed_expansion_ES_60_moderate_v1",
    "atom_accepted_price_migration_extreme_dwell_ES_30_moderate_v1",
)
_PRIORITY_MULTIPLIERS = {
    _SENSITIVE_PRIORITY_IDS[0]: 1.40,
    _SENSITIVE_PRIORITY_IDS[1]: 1.30,
    _SENSITIVE_PRIORITY_IDS[2]: 1.25,
    _SENSITIVE_PRIORITY_IDS[3]: 1.20,
}
_INVARIANT_PRIORITY_MULTIPLIERS = {
    _INVARIANT_PRIORITY_IDS[0]: 1.30,
    _INVARIANT_PRIORITY_IDS[1]: 1.20,
}
_PAIRED_RETEST_GROUPS = {
    _SENSITIVE_PRIORITY_IDS[1]: "paired_effort_without_progress_mes_ym_v1",
    _SENSITIVE_PRIORITY_IDS[2]: "paired_effort_without_progress_mes_ym_v1",
}


_RETEST_ATTACK_CLASSIFICATION = {
    "target_leakage": "FATAL_MANDATORY",
    "lookahead": "FATAL_MANDATORY",
    "opportunity_session_volatility_matched_random": "FATAL_MANDATORY",
    "delayed_signal": "ROBUSTNESS_DIAGNOSTIC",
    "sign_flipped_signal": "ROBUSTNESS_DIAGNOSTIC",
    "block_permuted_event_assignment": "ROBUSTNESS_DIAGNOSTIC",
    "matched_momentum_baseline": "ROBUSTNESS_DIAGNOSTIC",
    "matched_mean_reversion_baseline": "ROBUSTNESS_DIAGNOSTIC",
    "session_phase_opportunity_matched_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "volatility_opportunity_matched_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "event_time_jitter": "ROBUSTNESS_DIAGNOSTIC",
    "best_event_removed": "ROBUSTNESS_DIAGNOSTIC",
    "cost_stress": "ROBUSTNESS_DIAGNOSTIC",
    "placebo_market": "INFORMATIONAL_ONLY",
}

_FATAL_ATTACKS = tuple(
    name for name, classification in _RETEST_ATTACK_CLASSIFICATION.items() if classification == "FATAL_MANDATORY"
)
_DIAGNOSTIC_ATTACKS = tuple(
    name
    for name, classification in _RETEST_ATTACK_CLASSIFICATION.items()
    if classification == "ROBUSTNESS_DIAGNOSTIC"
)
_INFORMATIONAL_ATTACKS = tuple(
    name for name, classification in _RETEST_ATTACK_CLASSIFICATION.items() if classification == "INFORMATIONAL_ONLY"
)

_FAMILY_MANDATORY_ATTACKS: dict[str, tuple[str, ...]] = {
    "accepted_price_migration": ("session_phase_opportunity_matched_baseline",),
    "effort_vs_progress": ("volatility_opportunity_matched_baseline",),
    "defensive_portfolio_atom": (
        "session_phase_opportunity_matched_baseline",
        "volatility_opportunity_matched_baseline",
    ),
    "volatility_path_shape": ("volatility_opportunity_matched_baseline",),
}

_HISTORICAL_ATTACK_ALIASES = {
    "block_shuffled_signal": "block_permuted_event_assignment",
    "momentum_baseline": "matched_momentum_baseline",
    "mean_reversion_baseline": "matched_mean_reversion_baseline",
    "session_only_baseline": "session_phase_opportunity_matched_baseline",
    "volatility_only_baseline": "volatility_opportunity_matched_baseline",
    "opportunity_count_matched_random": "opportunity_session_volatility_matched_random",
}

class CalibrationRetestDesignError(RuntimeError):
    """Raised when frozen inputs cannot support a governed retest design."""


def run_calibration_affected_atom_retest_design(
    output_dir: str | Path,
    *,
    historical_report_path: str | Path = DEFAULT_HISTORICAL_REPORT,
    historical_preregistration_path: str | Path = DEFAULT_HISTORICAL_PREREGISTRATION,
    code_commit: str = "UNCOMMITTED_ENGINEERING_BUILD",
    sensitive_limit: int = 4,
    invariant_limit: int = 2,
) -> dict[str, Any]:
    """Create a deterministic, zero-data-cost retest design from frozen evidence.

    This function designs and preregisters a bounded retest.  It deliberately
    does not reclassify any historical atom, load market data, access Q4, make a
    network request, or claim that a retested atom passed validation.
    """

    _validate_limits(sensitive_limit, invariant_limit)
    report_path = _resolve_input_path(historical_report_path)
    prereg_path = _resolve_input_path(historical_preregistration_path)
    summary = _load_historical_report(report_path)
    historical_preregistration = _load_json(prereg_path)
    historical_atoms = _validate_frozen_inputs(summary, historical_preregistration)

    source = {
        "historical_report_name": report_path.name,
        "historical_report_sha256": _file_sha256(report_path),
        "historical_preregistration_name": prereg_path.name,
        "historical_preregistration_sha256": _file_sha256(prereg_path),
        "historical_atom_count": len(historical_atoms),
        "historical_detailed_result_count": len(summary["top_atom_results"]),
        "historical_results_without_detailed_rows": len(historical_atoms) - len(summary["top_atom_results"]),
        "historical_code_commit": summary.get("baseline_commit_actual"),
        "development_data_manifest": _development_data_manifest(summary),
        "frozen_random_seed": 9173,
    }
    ranking = _rank_historical_decisions(summary["top_atom_results"], historical_atoms)
    selected_sensitive = _select_with_priorities(
        (row for row in ranking if row["decision_class"] == "CALIBRATION_SENSITIVE"),
        sensitive_limit,
        score_key="expected_decision_information_gain",
        priority_ids=_SENSITIVE_PRIORITY_IDS,
    )
    selected_families = {row["family"] for row in selected_sensitive}
    selected_invariant = _select_with_priorities(
        (row for row in ranking if row["decision_class"] == "CALIBRATION_INVARIANT_FAILURE"),
        invariant_limit,
        score_key="invariant_control_value",
        priority_ids=_INVARIANT_PRIORITY_IDS,
        initially_used_families=selected_families,
    )
    if len(selected_sensitive) != sensitive_limit:
        raise CalibrationRetestDesignError(
            f"Frozen evidence supports only {len(selected_sensitive)} calibration-sensitive retests; "
            f"{sensitive_limit} requested."
        )
    if len(selected_invariant) != invariant_limit:
        raise CalibrationRetestDesignError(
            f"Frozen evidence supports only {len(selected_invariant)} invariant failure controls; "
            f"{invariant_limit} requested."
        )

    selected = [
        *[(row, "CALIBRATION_SENSITIVE_CANDIDATE") for row in selected_sensitive],
        *[(row, "CALIBRATION_INVARIANT_OLD_FAILURE") for row in selected_invariant],
    ]
    preregistration = _build_preregistration(
        selected,
        historical_atoms,
        source=source,
        code_commit=code_commit,
    )
    selected_old_ids = {
        row["historical_reference"]["historical_atom_id"] for row in preregistration["atoms"]
    }
    ranking_with_selection = []
    for row in ranking:
        ranked = deepcopy(row)
        ranked["selected_for_retest"] = row["historical_atom_id"] in selected_old_ids
        ranking_with_selection.append(ranked)

    design: dict[str, Any] = {
        "schema": "calibration_affected_atom_retest_design_v1",
        "design_version": DESIGN_VERSION,
        "experiment_id": "calibration_affected_atom_retest_design",
        "experiment_status": "COMPLETED_DESIGN_ONLY",
        "scientific_conclusion": "BOUNDED_FRESH_RETEST_PREREGISTERED_NO_HISTORICAL_DECISION_INHERITED",
        "decision_scope": (
            "This design can authorize fresh development-data retests only. It cannot validate an atom, "
            "reopen a family, assemble a strategy, or support Q4 access."
        ),
        "source": source,
        "ranking_policy": _ranking_policy(),
        "historical_decision_ranking": ranking_with_selection,
        "selection": {
            "sensitive_limit": sensitive_limit,
            "invariant_limit": invariant_limit,
            "selected_sensitive_historical_atom_ids": [row["historical_atom_id"] for row in selected_sensitive],
            "selected_invariant_historical_atom_ids": [row["historical_atom_id"] for row in selected_invariant],
            "selected_new_atom_ids": [row["atom_id"] for row in preregistration["atoms"]],
            "historical_atom_retest_count": len(preregistration["atoms"]),
            "positive_control_count": len(preregistration["positive_controls"]),
            "negative_control_count": len(preregistration["negative_controls"]),
            "selection_rule": (
                "Frozen independent-audit priorities first, then descending EDIG for calibration-sensitive "
                "candidates and descending invariant-control value for old failures, with family diversity "
                "preferred for unfilled slots and hard count bounds."
            ),
        },
        "preregistration": preregistration,
        "governance": {
            "historical_research_only": True,
            "q4_accessed": False,
            "q4_access_count": 0,
            "latest_permitted_data_end_exclusive": "2024-10-01",
            "network_access": False,
            "paid_data_request_count": 0,
            "incremental_databento_cost_usd": 0.0,
            "broker_or_live_execution": False,
            "frozen_cached_inputs_only": True,
        },
        "unresolved_question": (
            "Whether zero historical survival was mostly real or included false kills remains unresolved "
            "until these fresh preregistered retests and their controls are executed."
        ),
        "next_recommended_action": "EXECUTE_FRESH_PREREGISTERED_RETESTS_ON_DEVELOPMENT_DATA_ONLY",
        "artifact_names": {
            "design_json": DESIGN_JSON_NAME,
            "preregistration_json": PREREGISTRATION_JSON_NAME,
            "report": REPORT_NAME,
        },
    }
    design["design_hash"] = _stable_hash(design)

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    design_json_path = destination / DESIGN_JSON_NAME
    preregistration_json_path = destination / PREREGISTRATION_JSON_NAME
    report_output_path = destination / REPORT_NAME
    _write_immutable_artifacts(
        {
            design_json_path: _json_text(design),
            preregistration_json_path: _json_text(preregistration),
            report_output_path: _render_report(design),
        }
    )

    result = deepcopy(design)
    result["artifacts"] = {
        "design_json_path": str(design_json_path),
        "preregistration_json_path": str(preregistration_json_path),
        "report_path": str(report_output_path),
    }
    result["paths"] = {
        "design": str(design_json_path),
        "preregistration": str(preregistration_json_path),
        "report": str(report_output_path),
    }
    result["design_path"] = str(design_json_path)
    result["preregistration_path"] = str(preregistration_json_path)
    result["report_path"] = str(report_output_path)
    return result


def run_calibration_retest_design(output_dir: str | Path, **kwargs: Any) -> dict[str, Any]:
    """Short integration alias for the canonical mission experiment API."""

    return run_calibration_affected_atom_retest_design(output_dir, **kwargs)


def _development_data_manifest(summary: dict[str, Any]) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for row in summary.get("cached_coverage") or []:
        path = Path(str(row.get("path") or ""))
        if not path.is_file():
            raise CalibrationRetestDesignError(f"Frozen development file is missing: {path}")
        if pd_timestamp := row.get("end"):
            if str(pd_timestamp) >= "2024-10-01":
                # End timestamps on 2024-09-30 are allowed; any date beginning
                # 2024-10-01 or later is protected and cannot enter this design.
                raise CalibrationRetestDesignError(f"Protected-period file cannot be preregistered: {path}")
        files.append(
            {
                "path": str(path.resolve()),
                "sha256": _file_sha256(path),
                "size_bytes": path.stat().st_size,
            }
        )
    contract_map = Path(str(summary.get("contract_map_path") or ""))
    if not contract_map.is_file():
        raise CalibrationRetestDesignError("Frozen explicit contract map is missing.")
    return {
        "role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
        "period_start": "2023-01-01",
        "period_end_exclusive": "2024-10-01",
        "files": sorted(files, key=lambda row: row["path"]),
        "contract_map": {
            "path": str(contract_map.resolve()),
            "sha256": _file_sha256(contract_map),
            "size_bytes": contract_map.stat().st_size,
        },
    }


def _validate_limits(sensitive_limit: int, invariant_limit: int) -> None:
    if not 1 <= sensitive_limit <= MAX_SENSITIVE_RETESTS:
        raise ValueError(f"sensitive_limit must be between 1 and {MAX_SENSITIVE_RETESTS}")
    if not 1 <= invariant_limit <= MAX_INVARIANT_RETESTS:
        raise ValueError(f"invariant_limit must be between 1 and {MAX_INVARIANT_RETESTS}")


def _resolve_input_path(path: str | Path) -> Path:
    candidate = Path(path)
    resolved = candidate if candidate.is_absolute() else project_path(*candidate.parts)
    if not resolved.is_file():
        raise CalibrationRetestDesignError(f"Frozen input does not exist: {resolved}")
    return resolved


def _load_historical_report(path: Path) -> dict[str, Any]:
    match = re.search(r"```json\s*(\{.*\})\s*```", path.read_text(encoding="utf-8"), re.DOTALL)
    if not match:
        raise CalibrationRetestDesignError(f"Historical report has no parseable JSON payload: {path}")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise CalibrationRetestDesignError(f"Historical report JSON is invalid: {path}") from exc
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CalibrationRetestDesignError(f"Frozen JSON is invalid: {path}") from exc
    if not isinstance(payload, dict):
        raise CalibrationRetestDesignError(f"Frozen JSON root must be an object: {path}")
    return payload


def _validate_frozen_inputs(
    summary: dict[str, Any], historical_preregistration: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    if summary.get("q4_access_count") != 0 or summary.get("q4_seal_verification") != "PASSED_NO_Q4_ACCESS":
        raise CalibrationRetestDesignError("Historical report is not eligible: Q4 seal is absent or access count is nonzero.")
    development_end = str((summary.get("development_period") or {}).get("end") or "")
    if not development_end or development_end > "2024-10-01":
        raise CalibrationRetestDesignError("Historical development period crosses the sealed Q4 boundary.")
    if summary.get("status_scope_violations_detected") not in {0, None}:
        raise CalibrationRetestDesignError("Historical report contains evidence-scope violations.")
    detailed = summary.get("top_atom_results")
    atoms = historical_preregistration.get("atoms")
    if not isinstance(detailed, list) or not detailed:
        raise CalibrationRetestDesignError("Historical report contains no detailed atom decisions to rank.")
    if not isinstance(atoms, list) or not atoms:
        raise CalibrationRetestDesignError("Historical preregistration contains no atoms.")
    if historical_preregistration.get("atom_count") != len(atoms):
        raise CalibrationRetestDesignError("Historical preregistration atom count is inconsistent.")

    by_id: dict[str, dict[str, Any]] = {}
    for atom in atoms:
        if not isinstance(atom, dict) or not atom.get("atom_id"):
            raise CalibrationRetestDesignError("Historical preregistration has a malformed atom entry.")
        atom_id = str(atom["atom_id"])
        if atom_id in by_id:
            raise CalibrationRetestDesignError(f"Historical preregistration repeats atom ID {atom_id}.")
        stored_hash = atom.get("preregistration_hash")
        body = {key: value for key, value in atom.items() if key != "preregistration_hash"}
        if stored_hash != _stable_hash(body):
            raise CalibrationRetestDesignError(f"Historical preregistration hash mismatch for {atom_id}.")
        by_id[atom_id] = atom
    missing = sorted({str(row.get("atom_id")) for row in detailed} - set(by_id))
    if missing:
        raise CalibrationRetestDesignError(f"Detailed historical decisions lack preregistrations: {missing}")
    return by_id


def _rank_historical_decisions(
    rows: list[dict[str, Any]], historical_atoms: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    max_adjusted = max(float(row.get("fdr_adjusted_evidence") or 0.0) for row in rows) or 1.0
    ranked = [_score_historical_decision(row, historical_atoms[str(row["atom_id"])], max_adjusted) for row in rows]
    ranked.sort(key=lambda row: (-row["expected_decision_information_gain"], row["historical_atom_id"]))
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return ranked


def _score_historical_decision(
    row: dict[str, Any], historical_atom: dict[str, Any], max_adjusted_evidence: float
) -> dict[str, Any]:
    family = str(row.get("family") or historical_atom.get("family"))
    failed_attacks = tuple(str(item) for item in (row.get("adversarial") or {}).get("attacks_failed", []))
    failed_by_class = _group_attacks_by_class(failed_attacks, family=family)
    fatal_failed = failed_by_class["FATAL_MANDATORY"]
    direction_ok = bool(row.get("direction_ok"))
    failure_reason = str(row.get("failure_reason") or "")
    raw_effect = float(row.get("raw_effect") or 0.0)
    historical_cost = abs(float(row.get("cost_hurdle") or 0.0))
    minimum_effect = abs(float(historical_atom.get("minimum_effect") or 0.0))
    observations = max(int(row.get("valid_observations") or 0), 0)

    raw_to_minimum = abs(raw_effect) / max(minimum_effect, 1e-12)
    cost_dominated = historical_cost > abs(raw_effect) and float(row.get("effect_after_cost_hurdle") or 0.0) < 0
    if (
        failure_reason == "effect_below_cost_or_minimum_effect"
        and direction_ok
        and cost_dominated
        and raw_to_minimum >= 1.0
    ):
        cost_sensitivity = min(1.0, 0.4 * raw_to_minimum)
    else:
        cost_sensitivity = 0.0

    failed_count = len(failed_attacks)
    if failed_count:
        attack_numerator = (
            len(failed_by_class["ROBUSTNESS_DIAGNOSTIC"])
            + 0.25 * len(failed_by_class["HYPOTHESIS_SPECIFIC_MANDATORY"])
            + 0.10 * len(failed_by_class["INFORMATIONAL_ONLY"])
        )
        attack_sensitivity = attack_numerator / failed_count
    else:
        attack_sensitivity = 0.0
    if fatal_failed:
        attack_sensitivity *= 0.10

    sample_reliability = min(1.0, math.log10(max(observations, 10)) / 5.0)
    sample_size_sensitivity = 1.0 - sample_reliability
    scope_sensitivity = 1.0 if family == "defensive_portfolio_atom" else 0.0
    calibration_sensitivity = (
        0.45 * cost_sensitivity
        + 0.35 * attack_sensitivity
        + 0.10 * sample_size_sensitivity
        + 0.10 * scope_sensitivity
    )

    evidence_strength = max(float(row.get("evidence_strength") or 0.0), 0.0)
    validator_power_proxy = min(1.0, evidence_strength / 8.0) * sample_reliability
    fold_count = max(int(row.get("fold_count") or 0), 1)
    contract_count = max(int(row.get("contract_count") or 0), 1)
    market_count = max(int(row.get("market_count") or 0), 1)
    fold_stability = max(0.0, min(1.0, float(row.get("folds_positive") or 0) / fold_count))
    contract_stability = max(0.0, min(1.0, float(row.get("contracts_positive") or 0) / contract_count))
    market_stability = max(0.0, min(1.0, float(row.get("markets_positive") or 0) / market_count))
    adjusted_evidence = max(float(row.get("fdr_adjusted_evidence") or 0.0), 0.0)
    mechanism_value = (
        0.45 * min(1.0, adjusted_evidence / max_adjusted_evidence)
        + 0.30 * fold_stability
        + 0.15 * contract_stability
        + 0.10 * market_stability
    )
    decision_change_probability = min(0.95, 0.10 + 0.80 * calibration_sensitivity)
    if not direction_ok or fatal_failed:
        decision_change_probability *= 0.20
    total_research_cost = 1.0 + 0.50 * sample_size_sensitivity + (0.10 if market_count == 1 else 0.0)
    base_information_gain = (
        calibration_sensitivity * decision_change_probability * mechanism_value / max(total_research_cost, 1e-12)
    )
    independent_audit_priority_multiplier = _PRIORITY_MULTIPLIERS.get(str(row["atom_id"]), 1.0)
    information_gain = base_information_gain * independent_audit_priority_multiplier

    failure_clarity = 1.0 if (not direction_ok or fatal_failed) else 0.25
    invariant_priority_multiplier = _INVARIANT_PRIORITY_MULTIPLIERS.get(str(row["atom_id"]), 1.0)
    invariant_control_value = (
        (1.0 - calibration_sensitivity) * failure_clarity * validator_power_proxy / max(total_research_cost, 1e-12)
    ) * invariant_priority_multiplier
    calibration_defects: list[str] = []
    if cost_sensitivity > 0:
        calibration_defects.append("ATOM_COST_HURDLE_MISAPPLICATION")
    if failed_by_class["ROBUSTNESS_DIAGNOSTIC"]:
        calibration_defects.append("ADVERSARIAL_DIAGNOSTICS_PREVIOUSLY_TREATED_AS_FATAL")
    if sample_size_sensitivity >= 0.20:
        calibration_defects.append("SAMPLE_SIZE_TREATMENT_UNCERTAINTY")
    if scope_sensitivity > 0:
        calibration_defects.append("DEFENSIVE_INFORMATION_SCOPE_COST_ERROR")

    implementation_uncertainties = [
        "CALENDAR_QUARTER_PROXY_IS_NOT_EXPLICIT_CONTRACT_IDENTITY",
        "OVERLAPPING_HORIZON_STANDARD_ERRORS_NOT_CLUSTERED",
        "GLOBAL_THRESHOLD_ESTIMATION_CAN_LEAK_ACROSS_FOLDS",
        "DIRECTION_MAY_HAVE_BEEN_APPLIED_TWICE",
        "FORWARD_RETURN_SHIFT_MAY_BLEED_ACROSS_SYMBOL_OR_CONTRACT_GROUPS",
        "OLD_BLOCK_SHUFFLE_NULL_MECHANICALLY_NONDISCRIMINATIVE",
        "OLD_OPPORTUNITY_NULL_MATCHED_COUNT_ONLY",
        "OLD_SESSION_BASELINE_NOT_OPPORTUNITY_OR_VOLATILITY_MATCHED",
    ]
    if family == "defensive_portfolio_atom":
        implementation_uncertainties.append("DEFENSIVE_HAZARD_HYPOTHESIS_TESTED_AGAINST_MEAN_RETURN_TARGET")

    below_frozen_minimum = abs(raw_effect) < minimum_effect
    if (
        direction_ok
        and failure_reason == "effect_below_cost_or_minimum_effect"
        and not below_frozen_minimum
        and not fatal_failed
        and calibration_sensitivity >= 0.45
    ):
        decision_class = "CALIBRATION_SENSITIVE"
    elif not direction_ok or below_frozen_minimum or fatal_failed or failure_reason in {
        "temporal_replication_below_default_requirement",
        "valid_event_count_below_50",
    }:
        decision_class = "CALIBRATION_INVARIANT_FAILURE"
    else:
        decision_class = "MIXED_OR_LOW_INFORMATION"

    return {
        "historical_atom_id": str(row["atom_id"]),
        "family": family,
        "historical_status": str(row.get("status") or "UNKNOWN"),
        "historical_failure_reason": failure_reason or None,
        "historical_result_hash": _stable_hash(row),
        "decision_class": decision_class,
        "calibration_defects_affecting_old_decision": calibration_defects,
        "historical_implementation_uncertainties_requiring_repair": sorted(implementation_uncertainties),
        "failed_attacks_by_calibrated_class": failed_by_class,
        "sensitivity_components": {
            "cost_hurdle_misapplication": _rounded(cost_sensitivity),
            "attack_policy_over_strictness": _rounded(attack_sensitivity),
            "sample_size_treatment": _rounded(sample_size_sensitivity),
            "scope_error": _rounded(scope_sensitivity),
        },
        "calibration_sensitivity_score": _rounded(calibration_sensitivity),
        "validator_power_proxy": _rounded(validator_power_proxy),
        "expected_decision_information_gain_components": {
            "decision_change_probability": _rounded(decision_change_probability),
            "mechanism_value": _rounded(mechanism_value),
            "total_research_cost": _rounded(total_research_cost),
            "independent_audit_priority_multiplier": _rounded(independent_audit_priority_multiplier),
            "data_cost_usd": 0.0,
            "holdout_contamination_cost": 0.0,
        },
        "expected_decision_information_gain": _rounded(information_gain),
        "invariant_control_value": _rounded(invariant_control_value),
        "historical_observations": observations,
        "historical_raw_effect": raw_effect,
        "historical_cost_hurdle": historical_cost,
        "minimum_useful_effect": minimum_effect,
        "historical_direction_ok": direction_ok,
        "historical_temporal_support": {"positive": int(row.get("folds_positive") or 0), "total": fold_count},
        "historical_contract_support": {
            "positive": int(row.get("contracts_positive") or 0),
            "total": contract_count,
        },
    }


def _group_attacks_by_class(attacks: Iterable[str], *, family: str) -> dict[str, list[str]]:
    grouped = {
        "FATAL_MANDATORY": [],
        "HYPOTHESIS_SPECIFIC_MANDATORY": [],
        "ROBUSTNESS_DIAGNOSTIC": [],
        "INFORMATIONAL_ONLY": [],
        "UNCLASSIFIED": [],
    }
    family_mandatory = set(_FAMILY_MANDATORY_ATTACKS.get(family, ()))
    for attack in attacks:
        calibrated_name = _HISTORICAL_ATTACK_ALIASES.get(attack, attack)
        calibrated_class = _RETEST_ATTACK_CLASSIFICATION.get(calibrated_name, "UNCLASSIFIED")
        if (
            calibrated_class == "HYPOTHESIS_SPECIFIC_MANDATORY"
            and calibrated_name not in family_mandatory
        ):
            calibrated_class = "ROBUSTNESS_DIAGNOSTIC"
        grouped.setdefault(calibrated_class, []).append(attack)
    return {key: sorted(values) for key, values in grouped.items()}


def _select_with_priorities(
    candidates: Iterable[dict[str, Any]],
    limit: int,
    *,
    score_key: str,
    priority_ids: tuple[str, ...],
    initially_used_families: set[str] | None = None,
) -> list[dict[str, Any]]:
    ordered = sorted(candidates, key=lambda row: (-float(row[score_key]), row["historical_atom_id"]))
    selected: list[dict[str, Any]] = []
    used_families = set(initially_used_families or set())
    by_id = {row["historical_atom_id"]: row for row in ordered}
    for atom_id in priority_ids:
        row = by_id.get(atom_id)
        if row is None:
            continue
        selected.append(row)
        used_families.add(row["family"])
        if len(selected) == limit:
            return selected

    already_selected = {row["historical_atom_id"] for row in selected}
    deferred: list[dict[str, Any]] = []
    for row in ordered:
        if row["historical_atom_id"] in already_selected:
            continue
        if row["family"] in used_families:
            deferred.append(row)
            continue
        selected.append(row)
        used_families.add(row["family"])
        if len(selected) == limit:
            return selected
    for row in deferred:
        selected.append(row)
        if len(selected) == limit:
            break
    return selected


def _build_preregistration(
    selected: list[tuple[dict[str, Any], str]],
    historical_atoms: dict[str, dict[str, Any]],
    *,
    source: dict[str, Any],
    code_commit: str,
) -> dict[str, Any]:
    atoms = []
    for ranking_row, selection_role in selected:
        historical = historical_atoms[ranking_row["historical_atom_id"]]
        atom = _fresh_atom_preregistration(
            historical,
            ranking_row,
            selection_role=selection_role,
            source_report_hash=source["historical_report_sha256"],
            code_commit=code_commit,
        )
        atoms.append(atom)

    positive_controls = []
    for spec in positive_control_specs():
        positive_controls.append(
            {
                "control_id": f"positive_control_{spec.edge_id}",
                "control_class": "POSITIVE_CONTROL",
                "specification": spec.to_dict(),
                "retest_pipeline_translation": _control_pipeline_translation(
                    spec.edge_id, spec.to_dict(), positive=True
                ),
                "expected_decision": "KNOWN_INJECTED_EFFECT_DETECTED",
                "falsification_rule": "Control fails if the calibrated validator does not detect the injected effect.",
            }
        )
    negative_controls = []
    for spec in negative_control_specs():
        negative_controls.append(
            {
                "control_id": f"negative_control_{spec.control_id}",
                "control_class": "NEGATIVE_CONTROL",
                "specification": spec.to_dict(),
                "retest_pipeline_translation": _control_pipeline_translation(
                    spec.control_id, spec.to_dict(), positive=False
                ),
                "expected_decision": "NULL_CONTROL_REJECTED",
                "falsification_rule": "Control fails if the calibrated validator reports an edge.",
            }
        )

    paired_groups: dict[str, list[str]] = {}
    for atom in atoms:
        group_id = atom.get("paired_retest_group_id")
        if group_id:
            paired_groups.setdefault(str(group_id), []).append(str(atom["atom_id"]))

    preregistration: dict[str, Any] = {
        "schema": PREREGISTRATION_SCHEMA,
        "design_version": DESIGN_VERSION,
        "immutable_before_execution": True,
        "code_commit": code_commit,
        "source": source,
        "atom_count": len(atoms),
        "atoms": atoms,
        "positive_controls": positive_controls,
        "negative_controls": negative_controls,
        "paired_retest_groups": [
            {
                "group_id": group_id,
                "new_atom_ids": sorted(atom_ids),
                "decision_rule": (
                    "Estimate each market independently under the same causal feature contract; report both "
                    "effects and their heterogeneity. One market may not lend pass status to the other."
                ),
            }
            for group_id, atom_ids in sorted(paired_groups.items())
        ],
        "implementation_validity_contract": _implementation_validity_contract(),
        "validator_acceptance_policy": {
            "maximum_false_positive_rate": 0.20,
            "minimum_power_on_meaningful_effects": 0.80,
            "minimum_precision": 0.80,
            "minimum_recall": 0.80,
            "gate_rule": (
                "If controls fail calibration, atom retest decisions are INVALID_VALIDATOR_CALIBRATION and no "
                "family decision may change."
            ),
        },
        "interpretation_policy": {
            "historical_status_inheritance_allowed": False,
            "retest_pass_implies_atom_validated": False,
            "maximum_positive_interpretation": "RETEST_SUPPORTS_FAMILY_REOPENING_FOR_FRESH_REPLICATION",
            "strategy_assembly_allowed": False,
            "q4_access_allowed": False,
        },
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    return preregistration


def _control_pipeline_translation(
    control_id: str, specification: dict[str, Any], *, positive: bool
) -> dict[str, Any]:
    target_kind = {
        "tail_risk_defensive": "hazard",
        "block_shuffled_real_returns": "hazard",
        "volatility_prediction": "volatility",
        "opportunity_matched_random": "volatility",
    }.get(control_id, "return")
    horizon = int(specification.get("horizon") or 20)
    event_frequency = float(specification.get("event_frequency") or 0.12)
    return {
        "target_kind": target_kind,
        "horizon_bars": horizon,
        "event_frequency": event_frequency,
        "analysis_event_quantile": 0.985 if control_id == "regime_conditional" else 0.97,
        "injected_effect_size": float(specification.get("effect_size") or 0.0) if positive else 0.0,
        "injection_mechanism": (
            {
                "mean_shift_strong": "signed_terminal_return_impulse",
                "path_asymmetry_medium": "signed_return_distributed_across_horizon",
                "tail_risk_defensive": "future_low_tail_breach_on_frozen_feature_events",
                "volatility_prediction": "unsigned_terminal_volatility_impulse",
                "regime_conditional": "signed_terminal_return_impulse_on_alternating_preregistered_sessions",
            }.get(control_id, "no_effect_negative_control")
            if positive
            else "no_future_effect_injection"
        ),
        "hazard_tail_drop_fraction": 0.03 if control_id == "tail_risk_defensive" else 0.0,
        "negative_control_must_be_conclusively_falsified": not positive,
    }


def _fresh_atom_preregistration(
    historical: dict[str, Any],
    ranking_row: dict[str, Any],
    *,
    selection_role: str,
    source_report_hash: str,
    code_commit: str,
) -> dict[str, Any]:
    id_payload = {
        "design_version": DESIGN_VERSION,
        "historical_atom_id": historical["atom_id"],
        "historical_preregistration_hash": historical["preregistration_hash"],
        "source_report_hash": source_report_hash,
        "code_commit": code_commit,
    }
    identifier_digest = _stable_hash(id_payload)[:16]
    family = _slug(str(historical["family"]))
    feature = _slug(str(historical["feature_key"]))
    new_atom_id = f"atom_calibration_retest_{family}_{feature}_{identifier_digest}_v2"
    attack_policy = _attack_policy_for_family(str(historical["family"]))
    target_contract = _target_contract(historical)
    minimum_effect = float(target_contract["minimum_useful_effect"])
    cost_envelope = _cost_envelope(
        ranking_row,
        historical,
        minimum_effect=minimum_effect,
        effect_unit=str(target_contract["effect_unit"]),
    )
    atom: dict[str, Any] = {
        "atom_id": new_atom_id,
        "version": 2,
        "authoring_mode": "PREREGISTERED_BEFORE_RETEST",
        "code_commit": code_commit,
        "selection_role": selection_role,
        "family": historical["family"],
        "feature_key": historical["feature_key"],
        "economic_mechanism": historical["economic_mechanism"],
        "participants": historical["participants"],
        "information_set": historical["information_set"],
        "target_variable": target_contract["target_variable"],
        "expected_direction": target_contract["expected_direction"],
        "direction_semantics": target_contract["direction_semantics"],
        "effect_unit": target_contract["effect_unit"],
        "minimum_useful_effect_rationale": target_contract["minimum_useful_effect_rationale"],
        "hazard_decision_contract": (
            {
                "hazard_definition": target_contract["hazard_definition"],
                "odds_ratio_confidence_lower_bound_threshold": target_contract[
                    "odds_ratio_confidence_lower_bound_threshold"
                ],
                "cross_market_rule": target_contract["cross_market_rule"],
                "inconclusive_rule": target_contract["inconclusive_rule"],
                "account_level_mll_evidence_claimed": False,
            }
            if "hazard_definition" in target_contract
            else None
        ),
        "horizon_bars": historical["horizon_bars"],
        "target_markets": historical["target_markets"],
        "cross_market_replication_required": len(historical["target_markets"]) > 1,
        "favorable_regimes": historical["favorable_regimes"],
        "failure_regimes": historical["failure_regimes"],
        "roll_sensitivity": historical["roll_sensitivity"],
        "parameters": historical["parameters"],
        "max_parameter_degrees": historical["max_parameter_degrees"],
        "primary_null": historical["primary_null"],
        "replication_requirement": historical["replication_requirement"],
        "minimum_useful_effect": minimum_effect,
        "paired_retest_group_id": _PAIRED_RETEST_GROUPS.get(str(historical["atom_id"])),
        "attack_policy": attack_policy,
        "cost_envelope": cost_envelope,
        "falsification_criteria": _falsification_criteria(
            float(cost_envelope["decisive_atom_effect_hurdle"])
        ),
        "implementation_validity_contract": _implementation_validity_contract(),
        "calibration_defects_affecting_old_decision": ranking_row[
            "calibration_defects_affecting_old_decision"
        ],
        "historical_implementation_uncertainties_requiring_repair": ranking_row[
            "historical_implementation_uncertainties_requiring_repair"
        ],
        "historical_reference": {
            "historical_atom_id": historical["atom_id"],
            "historical_preregistration_hash": historical["preregistration_hash"],
            "historical_result_hash": ranking_row["historical_result_hash"],
            "historical_failure_reason": ranking_row["historical_failure_reason"],
            "historical_target_variable": historical["target_variable"],
            "historical_expected_direction": historical["expected_direction"],
            "historical_status_is_not_inherited": True,
        },
        "decision_contract": {
            "initial_state": "PREREGISTERED_UNTESTED",
            "old_pass_status_inherited": False,
            "positive_result_ceiling": "RETEST_SUPPORTS_FAMILY_REOPENING_FOR_FRESH_REPLICATION",
            "diagnostic_attack_failure_alone_is_fatal": False,
            "informational_attack_failure_is_fatal": False,
        },
    }
    atom["preregistration_hash"] = _stable_hash(atom)
    return atom


def _attack_policy_for_family(family: str) -> dict[str, Any]:
    hypothesis_specific = _FAMILY_MANDATORY_ATTACKS.get(
        family,
        (
            "session_phase_opportunity_matched_baseline",
            "volatility_opportunity_matched_baseline",
        ),
    )
    return {
        "policy_version": "calibration_retest_attack_policy_v1",
        "fatal_mandatory": list(_FATAL_ATTACKS),
        "hypothesis_specific_mandatory": list(hypothesis_specific),
        "robustness_diagnostic": list(_DIAGNOSTIC_ATTACKS),
        "informational_only": list(_INFORMATIONAL_ATTACKS),
        "retired_historical_nulls": {
            "block_shuffled_signal": "MECHANICALLY_NONDISCRIMINATIVE_REPLACED_BY_BLOCK_PERMUTED_EVENT_ASSIGNMENT",
            "session_only_baseline": "UNMATCHED_BASELINE_REPLACED_BY_SESSION_OPPORTUNITY_VOLATILITY_MATCHED_BASELINE",
            "opportunity_count_matched_random": "COUNT_ONLY_MATCH_REPLACED_BY_SESSION_VOLATILITY_OPPORTUNITY_MATCHING",
        },
        "decision_rule": (
            "Any fatal or hypothesis-specific mandatory failure falsifies the retest. Diagnostic and "
            "informational outcomes are reported separately and cannot alone kill or pass it."
        ),
    }


def _target_contract(historical: dict[str, Any]) -> dict[str, Any]:
    if historical.get("family") == "defensive_portfolio_atom":
        return {
            "target_variable": "future_standardized_tail_loss_hazard",
            "expected_direction": 1,
            "direction_semantics": (
                "A larger preregistered defensive state score must predict a larger standardized future "
                "tail-loss hazard; direction is applied once in the score, never again in the target."
            ),
            "effect_unit": "absolute_hazard_probability_difference",
            "minimum_useful_effect": 0.02,
            "minimum_useful_effect_rationale": (
                "A two-percentage-point absolute increase in near-horizon standardized tail-loss hazard is "
                "the smallest effect considered useful for later risk-deactivation research."
            ),
            "hazard_definition": (
                "Within the same symbol, explicit contract, Globex trading session, and contiguous one-minute "
                "segment, hazard=1 iff the minimum low over the next h bars is at most entry_close times "
                "(1 - 1.5 * past_120bar_return_std * sqrt(h)); otherwise 0. This is not an MLL replay."
            ),
            "odds_ratio_confidence_lower_bound_threshold": 1.15,
            "cross_market_rule": {
                "required_markets": 3,
                "minimum_positive_markets": 2,
                "minimum_independent_trading_day_clusters_per_market": 20,
            },
            "inconclusive_rule": (
                "If the delta-hazard interval crosses its decisive hurdle, the OR interval crosses 1.15, "
                "or any required market has fewer than 20 independent trading-day clusters, return INSUFFICIENT."
            ),
        }
    return {
        "target_variable": historical["target_variable"],
        "expected_direction": historical["expected_direction"],
        "direction_semantics": (
            "Apply the preregistered direction exactly once when converting the raw feature into an event score."
        ),
        "effect_unit": "signed_future_return",
        "minimum_useful_effect": abs(float(historical.get("minimum_effect") or 0.0)),
        "minimum_useful_effect_rationale": "Preserve the frozen historical economic-effect threshold.",
    }


def _cost_envelope(
    ranking_row: dict[str, Any],
    historical: dict[str, Any],
    *,
    minimum_effect: float,
    effect_unit: str,
) -> dict[str, Any]:
    policy = calibrated_atom_cost_policy()
    historical_cost = abs(float(ranking_row["historical_cost_hurdle"]))
    cost_units_compatible = effect_unit == "signed_future_return"
    atom_reference = historical_cost * policy.atom_statistical_hurdle_multiplier if cost_units_compatible else None
    return {
        "policy_version": policy.policy_version,
        "historical_strategy_like_round_trip_cost_proxy": historical_cost,
        "historical_cost_proxy_unit_compatible_with_retest_target": cost_units_compatible,
        "atom_statistical_hurdle_multiplier": policy.atom_statistical_hurdle_multiplier,
        "atom_statistical_cost_reference": atom_reference,
        "minimum_useful_effect": minimum_effect,
        "decisive_atom_effect_hurdle": (
            max(minimum_effect, float(atom_reference)) if atom_reference is not None else minimum_effect
        ),
        "strategy_execution_cost_required_at_atom_scope": policy.strategy_execution_cost_required,
        "historical_full_round_trip_proxy_is_atom_fatal": False,
        "defensive_atom_cost_mode": policy.defensive_atom_cost_mode,
        "diagnostic_cost_stress_multipliers": [1.0, 2.0],
        "future_strategy_scope_rule": (
            "Any assembled strategy must independently recompute full contract-specific round-trip costs; "
            "atom evidence never satisfies strategy cost resilience."
        ),
    }


def _implementation_validity_contract() -> dict[str, Any]:
    return {
        "policy_version": "calibration_retest_implementation_validity_v2",
        "walk_forward_thresholds": (
            "Estimate every event threshold on the training fold only, freeze it, and apply it to the next fold; "
            "global full-period ranks or quantiles are prohibited."
        ),
        "clustered_block_uncertainty": (
            "Resample complete Globex trading days jointly across markets. For hazard differences, use one joint "
            "two-way pigeonhole bootstrap over event trading days and retained control trading days; iid errors and "
            "contract-by-contract resampling that breaks contemporaneous cross-market shocks are prohibited."
        ),
        "explicit_contract_mapping": (
            "Group by explicit instrument/contract identity and validate roll mappings. Calendar-quarter labels are "
            "not contractual replication evidence."
        ),
        "true_matched_nulls": (
            "Match null opportunities on symbol, explicit contract, session phase, volatility, prior displacement, "
            "participation, frozen horizon, and a causal rolling past-opportunity-frequency bin before random assignment."
        ),
        "block_null_replacement": (
            "Permute the event label only within each frozen no-reuse 1:5 matched opportunity block. For hazard, "
            "recompute the five-control counterfactual after every label permutation."
        ),
        "direction_single_application": (
            "Apply expected direction exactly once. Sign flipping is an explicitly reported algebraic diagnostic, "
            "not an independent mandatory falsification attack."
        ),
        "group_safe_forward_target": (
            "Compute forward returns or hazards only within symbol, explicit contract, Chicago-dated Globex session, "
            "and contiguous one-minute segment; no target may cross maintenance, a missing bar, roll, or session."
        ),
        "defensive_target_alignment": (
            "Defensive atoms in this bounded retest target only the frozen standardized tail-loss hazard, not "
            "mean return and not account-level MLL; they use a hazard-unit minimum useful effect."
        ),
        "q4_and_data_policy": (
            "Use cached development/falsification data ending strictly at 2024-10-01; Q4, network access, and paid "
            "data are prohibited for this retest."
        ),
        "frozen_provenance_and_seed": (
            "Freeze the execution seed, code commit, every development-file path/size/SHA-256, and the explicit "
            "contract-map path/size/SHA-256 in the preregistration; verify all before loading observations."
        ),
        "tri_state_decisions": (
            "Every mandatory attack and structural gate is PASS, FAIL, or INSUFFICIENT. A mandatory attack that "
            "cannot execute makes the retest insufficient; no sentinel may be counted rejected by vacuity."
        ),
    }


def _falsification_criteria(minimum_effect: float) -> list[dict[str, Any]]:
    return [
        {
            "criterion": "future_information_or_target_leakage",
            "severity": "FATAL_MANDATORY",
            "rule": "Falsify on any lookahead, target leakage, or non-causal feature dependency.",
        },
        {
            "criterion": "opportunity_session_volatility_matched_null",
            "severity": "FATAL_MANDATORY",
            "rule": (
                "Falsify if random events matched on symbol, explicit contract, session, volatility, prior "
                "displacement, participation, frozen horizon, and causal past-opportunity frequency explain or "
                "match the effect."
            ),
        },
        {
            "criterion": "hypothesis_specific_mandatory_attacks",
            "severity": "MANDATORY",
            "rule": "Falsify if any preregistered hypothesis-specific competing explanation matches the effect.",
        },
        {
            "criterion": "minimum_useful_directional_effect",
            "severity": "MANDATORY",
            "threshold": minimum_effect,
            "rule": "Falsify if the signed effect is in the wrong direction or below the frozen useful-effect threshold.",
        },
        {
            "criterion": "minimum_event_count",
            "severity": "MANDATORY",
            "threshold": 50,
            "rule": "Return insufficient evidence, never pass, below 50 valid events.",
        },
        {
            "criterion": "temporal_replication",
            "severity": "MANDATORY",
            "threshold": "expected direction in at least 3 meaningful folds",
            "rule": "Return insufficient evidence or falsify according to the frozen replication contract.",
        },
        {
            "criterion": "contract_replication",
            "severity": "MANDATORY",
            "rule": "Do not reopen a family when the effect depends on one contract or roll interval.",
        },
        {
            "criterion": "walk_forward_threshold_integrity",
            "severity": "FATAL_MANDATORY",
            "rule": "Falsify the run if any threshold or normalization uses the evaluated fold or a later fold.",
        },
        {
            "criterion": "clustered_block_uncertainty",
            "severity": "MANDATORY",
            "rule": (
                "Require session-clustered and horizon-aware block uncertainty; iid significance from overlapping "
                "forward returns cannot support a decision."
            ),
        },
        {
            "criterion": "explicit_contract_and_group_safe_target",
            "severity": "FATAL_MANDATORY",
            "rule": (
                "Falsify the run if contract replication uses calendar-quarter proxies or if forward targets bleed "
                "across symbol, contract, roll, or session groups."
            ),
        },
        {
            "criterion": "diagnostic_attacks",
            "severity": "ROBUSTNESS_DIAGNOSTIC",
            "rule": "Report failure regions; diagnostic failure alone neither passes nor falsifies the atom.",
        },
    ]


def _ranking_policy() -> dict[str, Any]:
    return {
        "policy_version": "calibration_retest_edig_policy_v1",
        "calibration_sensitivity_formula": {
            "cost_hurdle_misapplication_weight": 0.45,
            "attack_policy_over_strictness_weight": 0.35,
            "sample_size_treatment_weight": 0.10,
            "scope_error_weight": 0.10,
        },
        "expected_decision_information_gain_formula": (
            "calibration_sensitivity * decision_change_probability * mechanism_value / total_research_cost"
        ),
        "independent_audit_priority": {
            "sensitive_historical_atom_ids": list(_SENSITIVE_PRIORITY_IDS),
            "invariant_historical_atom_ids": list(_INVARIANT_PRIORITY_IDS),
            "purpose": (
                "Prefer the most discriminative clean retest, a paired cross-market mechanism check, a defensive "
                "hazard reformulation, and two failures expected to remain invariant."
            ),
        },
        "research_cost_components": [
            "base_cached_replay_cost",
            "sample_size_uncertainty_cost",
            "single_market_replication_cost",
        ],
        "data_cost_usd": 0.0,
        "holdout_contamination_cost": 0.0,
        "unreported_historical_atoms_policy": (
            "The frozen report exposes detailed rows for only its top results. Atoms without detailed rows "
            "are not rankable and are excluded rather than assigned invented evidence."
        ),
    }


def _render_report(design: dict[str, Any]) -> str:
    selection = design["selection"]
    preregistration = design["preregistration"]
    rows = [
        "# HYDRA Calibration-Affected Atom Retest Design",
        "",
        "Historical research only. No live trading, Q4 access, network request, or paid data acquisition.",
        "",
        f"- Design version: `{design['design_version']}`",
        f"- Design hash: `{design['design_hash']}`",
        f"- Preregistration hash: `{preregistration['preregistration_hash']}`",
        f"- Scientific conclusion: `{design['scientific_conclusion']}`",
        f"- Detailed historical decisions ranked: {len(design['historical_decision_ranking'])}",
        f"- Calibration-sensitive retests selected: {selection['sensitive_limit']}",
        f"- Calibration-invariant old failures selected: {selection['invariant_limit']}",
        f"- Positive controls: {selection['positive_control_count']}",
        f"- Negative controls: {selection['negative_control_count']}",
        "",
        "## Fresh preregistered atoms",
        "",
        "| Role | New atom ID | Historical atom ID | Family |",
        "|---|---|---|---|",
    ]
    for atom in preregistration["atoms"]:
        rows.append(
            f"| {atom['selection_role']} | `{atom['atom_id']}` | "
            f"`{atom['historical_reference']['historical_atom_id']}` | `{atom['family']}` |"
        )
    rows.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            design["decision_scope"],
            "",
            design["unresolved_question"],
            "",
            f"Next action: `{design['next_recommended_action']}`.",
            "",
        ]
    )
    return "\n".join(rows)


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_text(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, default=str) + "\n"


def _write_immutable_artifacts(artifacts: dict[Path, str]) -> None:
    for path, expected in artifacts.items():
        if path.exists() and path.read_text(encoding="utf-8") != expected:
            raise CalibrationRetestDesignError(f"Refusing to overwrite immutable retest artifact: {path}")
    for path, expected in artifacts.items():
        if not path.exists():
            path.write_text(expected, encoding="utf-8")


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _rounded(value: float) -> float:
    return round(float(value), 12)
