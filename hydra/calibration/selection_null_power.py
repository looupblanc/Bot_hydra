from __future__ import annotations

import hashlib
import json
import math
import subprocess
from pathlib import Path
from typing import Any

import numpy as np

from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.equity_open_gap_reversal import _write_immutable
from hydra.research.qd_economic_tournament import _benjamini_hochberg


VERSION = "selection_null_power_calibration_v1"
FAMILY_SIZE = 20
EVENT_COUNTS = (80, 120, 360)
EFFECTS = (0.0, 0.25, 0.40)
REPLICATIONS = 200
NULL_DRAWS = 1024
BLOCK_SIZE = 5
COST_RATIO = 0.05
ADJUSTED_THRESHOLD = 0.20


class SelectionNullCalibrationError(RuntimeError):
    pass


def run_selection_null_power_calibration(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    code_commit: str,
    random_seed: int = 773401,
    replications: int = REPLICATIONS,
    null_draws: int = NULL_DRAWS,
) -> dict[str, Any]:
    task = Path(engineering_task_path)
    if not task.is_file() or hashlib.sha256(task.read_bytes()).hexdigest() != engineering_task_sha256:
        raise SelectionNullCalibrationError("Frozen calibration task is missing or changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise SelectionNullCalibrationError("Worker commit differs from queued specification.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = {
        "schema": "selection_null_power_preregistration_v1",
        "family_size": FAMILY_SIZE,
        "event_counts": EVENT_COUNTS,
        "standardized_net_effects": EFFECTS,
        "replications": replications,
        "null_draws": null_draws,
        "block_size": BLOCK_SIZE,
        "cost_ratio": COST_RATIO,
        "adjusted_probability_threshold": ADJUSTED_THRESHOLD,
        "minimum_useful_power": 0.80,
        "maximum_family_false_admission": 0.05,
        "task_sha256": engineering_task_sha256,
        "random_seed": random_seed,
        "code_commit": code_commit,
        "market_data_access": False,
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "prior_candidate_status_mutation_allowed": False,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "selection_null_power_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )

    conditions = []
    for count_index, event_count in enumerate(EVENT_COUNTS):
        for effect_index, effect in enumerate(EFFECTS):
            condition = simulate_condition(
                event_count=event_count,
                standardized_net_effect=effect,
                replications=replications,
                null_draws=null_draws,
                seed=random_seed + count_index * 100_003 + effect_index * 10_007,
            )
            conditions.append(condition)
    negatives = [item for item in conditions if item["standardized_net_effect"] == 0.0]
    meaningful = [
        item
        for item in conditions
        if item["standardized_net_effect"] == 0.40 and item["event_count"] >= 120
    ]
    family_fpr = max(float(item["family_false_admission_rate"]) for item in negatives)
    useful_power = min(float(item["injected_candidate_power"]) for item in meaningful)
    passed = bool(family_fpr <= 0.05 and useful_power >= 0.80)
    if passed:
        conclusion = "SELECTION_ADJUSTED_NULL_POLICY_CALIBRATED"
        next_action = "KEEP_POLICY_AND_PRIORITIZE_NEW_MECHANISMS_OR_FRESH_CONFIRMATION"
    elif family_fpr > 0.05:
        conclusion = "SELECTION_NULL_POLICY_FALSE_POSITIVE_CONTROL_FAILED"
        next_action = "REPAIR_NULL_POLICY_BEFORE_ANY_NEW_SHADOW_ADMISSION"
    else:
        conclusion = "SELECTION_NULL_POLICY_UNDERPOWERED_FOR_MEANINGFUL_EFFECTS"
        next_action = "PREREGISTER_SINGLE_CANDIDATE_CONFIRMATION_WITH_FRESH_IDS_AND_NO_RETROACTIVE_PROMOTION"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Synthetic calibration changes no historical candidate status. An underpowered result "
            "requires a new confirmation design; it does not authorize relaxing observed p-values."
        ),
        "conditions": conditions,
        "maximum_family_false_admission_rate": family_fpr,
        "minimum_meaningful_effect_power_n120_plus": useful_power,
        "calibration_passed": passed,
        "candidate_count": 0,
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "paper_shadow_ready": 0,
        "governance": {
            "market_data_rows_read": 0,
            "q4_access_count_delta": 0,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
            "prior_statuses_mutated": False,
        },
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "code_commit": code_commit,
        "next_recommended_action": next_action,
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "selection_null_power_result.json"
    report_path = destination / "selection_null_power_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
        },
        "report_path": str(report_path),
    }


def simulate_condition(
    *,
    event_count: int,
    standardized_net_effect: float,
    replications: int,
    null_draws: int,
    seed: int,
) -> dict[str, Any]:
    if event_count % BLOCK_SIZE:
        raise ValueError("Event count must be divisible by the frozen block size.")
    rng = np.random.default_rng(seed)
    injected_admissions = 0
    family_false_admissions = 0
    false_candidate_admissions = 0
    raw_injected_passes = 0
    total_null_candidates = replications * (
        FAMILY_SIZE if standardized_net_effect == 0.0 else FAMILY_SIZE - 1
    )
    for _ in range(replications):
        effects = np.zeros(FAMILY_SIZE, dtype=float)
        if standardized_net_effect > 0:
            effects[0] = standardized_net_effect
        gross = _synthetic_gross_returns(
            rng,
            event_count=event_count,
            net_effects=effects,
        )
        probabilities, net = _family_block_probabilities(
            gross,
            cost=COST_RATIO,
            draws=null_draws,
            rng=rng,
        )
        adjusted = np.asarray(_benjamini_hochberg(probabilities.tolist()))
        sign_flipped_net = -gross.sum(axis=1) - COST_RATIO * event_count
        admitted = (
            (adjusted <= ADJUSTED_THRESHOLD)
            & (net > 0)
            & (sign_flipped_net < 0)
        )
        if standardized_net_effect > 0:
            injected_admissions += int(admitted[0])
            raw_injected_passes += int(probabilities[0] <= ADJUSTED_THRESHOLD and net[0] > 0)
            false = admitted[1:]
        else:
            false = admitted
        family_false_admissions += int(false.any())
        false_candidate_admissions += int(false.sum())
    power = injected_admissions / replications if standardized_net_effect > 0 else 0.0
    family_fpr = family_false_admissions / replications
    candidate_fpr = false_candidate_admissions / max(total_null_candidates, 1)
    return {
        "event_count": event_count,
        "standardized_net_effect": standardized_net_effect,
        "replications": replications,
        "null_draws": null_draws,
        "injected_candidate_power": power,
        "raw_unadjusted_injected_pass_rate": (
            raw_injected_passes / replications if standardized_net_effect > 0 else 0.0
        ),
        "family_false_admission_rate": family_fpr,
        "per_null_candidate_false_admission_rate": candidate_fpr,
        "power_interval_95": _wilson_interval(injected_admissions, replications)
        if standardized_net_effect > 0
        else [0.0, 0.0],
        "family_fpr_interval_95": _wilson_interval(
            family_false_admissions, replications
        ),
    }


def _synthetic_gross_returns(
    rng: np.random.Generator,
    *,
    event_count: int,
    net_effects: np.ndarray,
) -> np.ndarray:
    blocks = event_count // BLOCK_SIZE
    block_shocks = rng.normal(0.0, 0.55, size=(FAMILY_SIZE, blocks))
    event_noise = rng.normal(0.0, math.sqrt(1.0 - 0.55**2), size=(FAMILY_SIZE, event_count))
    correlated = np.repeat(block_shocks, BLOCK_SIZE, axis=1) + event_noise
    # Negative controls have exactly zero gross directional effect before costs,
    # as preregistered. Injected controls receive the requested *net* effect, so
    # only those rows include the cost hurdle in their gross mean.
    gross_effect = np.where(
        net_effects[:, None] > 0,
        net_effects[:, None] + COST_RATIO,
        0.0,
    )
    return correlated + gross_effect


def _family_block_probabilities(
    gross: np.ndarray,
    *,
    cost: float,
    draws: int,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray]:
    family_size, event_count = gross.shape
    block_count = event_count // BLOCK_SIZE
    block_sums = gross.reshape(family_size, block_count, BLOCK_SIZE).sum(axis=2)
    signs = rng.choice(
        np.asarray([-1.0, 1.0], dtype=float),
        size=(draws, family_size, block_count),
    )
    null_gross = np.einsum("dfb,fb->df", signs, block_sums, optimize=True)
    observed_net = gross.sum(axis=1) - cost * event_count
    null_net = null_gross - cost * event_count
    probabilities = (
        1 + np.count_nonzero(null_net >= observed_net[None, :], axis=0)
    ) / (draws + 1)
    return probabilities.astype(float), observed_net.astype(float)


def _wilson_interval(successes: int, trials: int) -> list[float]:
    if trials <= 0:
        return [0.0, 1.0]
    z = 1.959963984540054
    p = successes / trials
    denominator = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / denominator
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * trials)) / trials) / denominator
    return [max(0.0, center - radius), min(1.0, center + radius)]


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Selection-Adjusted Null Power Calibration",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Maximum family false-admission rate: `{payload['maximum_family_false_admission_rate']:.4f}`",
        f"- Minimum meaningful-effect power (n>=120): `{payload['minimum_meaningful_effect_power_n120_plus']:.4f}`",
        f"- Calibration passed: `{payload['calibration_passed']}`",
        "- Historical candidate statuses changed: `0`",
        "- Market/Q4 rows read: `0`",
        "",
        "| Events | Net effect | Adjusted power | Family FPR | Null-candidate FPR |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in payload["conditions"]:
        lines.append(
            f"| {row['event_count']} | {row['standardized_net_effect']:.2f} | "
            f"{row['injected_candidate_power']:.3f} | "
            f"{row['family_false_admission_rate']:.3f} | "
            f"{row['per_null_candidate_false_admission_rate']:.4f} |"
        )
    lines.extend(["", "## Interpretation boundary", "", payload["interpretation_boundary"], ""])
    return "\n".join(lines)
