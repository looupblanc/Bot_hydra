from __future__ import annotations

import hashlib
import json
import math
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from hydra.calibration.selection_null_power import BLOCK_SIZE, COST_RATIO, _wilson_interval
from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.equity_open_gap_reversal import _write_immutable


VERSION = "single_primary_alpha_calibration_v3"
ALPHAS = (0.01, 0.02, 0.025, 0.03, 0.04)
EVENT_COUNTS = (80, 120, 360)
EFFECTS = (0.0, 0.25, 0.40)
REPLICATIONS = 500
NULL_DRAWS = 2048


class SinglePrimaryAlphaError(RuntimeError):
    pass


def run_single_primary_alpha_calibration(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    source_policy_repair_result_path: str | Path,
    source_policy_repair_result_sha256: str,
    source_policy_repair_result_hash: str,
    code_commit: str,
    random_seed: int = 774101,
    replications: int = REPLICATIONS,
    null_draws: int = NULL_DRAWS,
) -> dict[str, Any]:
    task = Path(engineering_task_path)
    source_path = Path(source_policy_repair_result_path)
    _verify(task, engineering_task_sha256, "engineering task")
    _verify(source_path, source_policy_repair_result_sha256, "v2 policy result")
    source = json.loads(source_path.read_text(encoding="utf-8"))
    if (
        source.get("result_hash") != source_policy_repair_result_hash
        or source.get("scientific_conclusion")
        != "NO_PROSPECTIVE_POLICY_MET_BOTH_FPR_AND_POWER"
        or bool(source.get("calibration_passed"))
    ):
        raise SinglePrimaryAlphaError("Policy repair v2 does not authorize v3.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise SinglePrimaryAlphaError("Worker commit differs from queued specification.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = {
        "schema": "single_primary_alpha_preregistration_v3",
        "promotion_primary_count": 1,
        "alphas": ALPHAS,
        "event_counts": EVENT_COUNTS,
        "standardized_net_effects": EFFECTS,
        "replications": replications,
        "null_draws": null_draws,
        "block_size": BLOCK_SIZE,
        "cost_ratio": COST_RATIO,
        "eligibility": {
            "maximum_point_false_admission": 0.05,
            "maximum_upper_wilson_95": 0.07,
            "minimum_power_effect_0_40_n120": 0.80,
        },
        "task_sha256": engineering_task_sha256,
        "source_policy_repair_result_hash": source_policy_repair_result_hash,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "prospective_only": True,
        "historical_status_mutation_allowed": False,
        "market_data_access": False,
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "single_primary_alpha_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )

    conditions = []
    for count_index, event_count in enumerate(EVENT_COUNTS):
        for effect_index, effect in enumerate(EFFECTS):
            conditions.append(
                simulate_primary_condition(
                    event_count=event_count,
                    standardized_net_effect=effect,
                    replications=replications,
                    null_draws=null_draws,
                    seed=random_seed + count_index * 100_003 + effect_index * 10_007,
                )
            )
    summaries = _summaries(conditions)
    eligible = [item for item in summaries if item["constraints_passed"]]
    selected = sorted(
        eligible,
        key=lambda item: (
            -float(item["power_effect_0_25_n120"]),
            -float(item["power_effect_0_40_n80"]),
            float(item["alpha"]),
        ),
    )
    chosen = selected[0] if selected else None
    conclusion = (
        "SINGLE_PRIMARY_ALPHA_CALIBRATED"
        if chosen is not None
        else "SINGLE_PRIMARY_ALPHA_GRID_INSUFFICIENT"
    )
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "This policy is prospective only: one new primary must be selected on an earlier fold "
            "and frozen before one later-fold confirmation. No historical candidate is reclassified."
        ),
        "source_policy_repair": {
            "path": str(source_path),
            "sha256": source_policy_repair_result_sha256,
            "result_hash": source_policy_repair_result_hash,
        },
        "conditions": conditions,
        "alpha_summaries": summaries,
        "selected_alpha": float(chosen["alpha"]) if chosen is not None else None,
        "selected_policy": chosen,
        "calibration_passed": chosen is not None,
        "prospective_policy_contract": (
            {
                "promotion_primary_count": 1,
                "candidate_probability_threshold": float(chosen["alpha"]),
                "selection_source": "earlier_development_fold_only",
                "freeze_before_confirmation": True,
                "confirmation_use_count": 1,
                "archive_elites_are_diagnostic_only": True,
                "new_candidate_id_required": True,
                "historical_reclassification": False,
                "q4_access_authorized": False,
            }
            if chosen is not None
            else None
        ),
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
            "historical_statuses_mutated": False,
        },
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "code_commit": code_commit,
        "next_recommended_action": (
            "BUILD_NEW_SINGLE_PRIMARY_TOURNAMENT_WITH_EARLY_FOLD_SELECTION_AND_LATE_FOLD_CONFIRMATION"
            if chosen is not None
            else "PIVOT_VALIDATION_STATISTIC_WITHOUT_WEAKENING_FALSE_POSITIVE_CEILING"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "single_primary_alpha_result.json"
    report_path = destination / "single_primary_alpha_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {"result_json_path": str(result_path), "report_path": str(report_path)},
        "report_path": str(report_path),
    }


def simulate_primary_condition(
    *,
    event_count: int,
    standardized_net_effect: float,
    replications: int,
    null_draws: int,
    seed: int,
) -> dict[str, Any]:
    if event_count % BLOCK_SIZE:
        raise ValueError("Event count must be divisible by block size.")
    rng = np.random.default_rng(seed)
    admissions = defaultdict(int)
    for _ in range(replications):
        gross = _synthetic_primary_returns(
            rng, event_count=event_count, net_effect=standardized_net_effect
        )
        probability, net, sign_flipped_net = _primary_probability(
            gross, draws=null_draws, rng=rng
        )
        economic = net > 0 and sign_flipped_net < 0
        for alpha in ALPHAS:
            admissions[alpha] += int(probability <= alpha and economic)
    return {
        "event_count": event_count,
        "standardized_net_effect": standardized_net_effect,
        "replications": replications,
        "null_draws": null_draws,
        "alphas": {
            str(alpha): {
                "admission_rate": admissions[alpha] / replications,
                "interval_95": _wilson_interval(admissions[alpha], replications),
            }
            for alpha in ALPHAS
        },
    }


def _synthetic_primary_returns(
    rng: np.random.Generator, *, event_count: int, net_effect: float
) -> np.ndarray:
    blocks = event_count // BLOCK_SIZE
    block_shocks = rng.normal(0.0, 0.55, size=blocks)
    event_noise = rng.normal(0.0, math.sqrt(1.0 - 0.55**2), size=event_count)
    effect = net_effect + COST_RATIO if net_effect > 0 else 0.0
    return np.repeat(block_shocks, BLOCK_SIZE) + event_noise + effect


def _primary_probability(
    gross: np.ndarray, *, draws: int, rng: np.random.Generator
) -> tuple[float, float, float]:
    event_count = len(gross)
    block_sums = gross.reshape(event_count // BLOCK_SIZE, BLOCK_SIZE).sum(axis=1)
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(draws, len(block_sums)))
    null_net = signs @ block_sums - COST_RATIO * event_count
    observed_net = float(gross.sum() - COST_RATIO * event_count)
    probability = float((1 + np.count_nonzero(null_net >= observed_net)) / (draws + 1))
    sign_flipped_net = float(-gross.sum() - COST_RATIO * event_count)
    return probability, observed_net, sign_flipped_net


def _summaries(conditions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries = []
    for alpha in ALPHAS:
        key = str(alpha)
        negative_rows = [row for row in conditions if row["standardized_net_effect"] == 0.0]
        maximum_negative = max(row["alphas"][key]["admission_rate"] for row in negative_rows)
        maximum_upper = max(row["alphas"][key]["interval_95"][1] for row in negative_rows)
        power_040_n120 = _condition_rate(conditions, key, 120, 0.40)
        power_025_n120 = _condition_rate(conditions, key, 120, 0.25)
        power_040_n80 = _condition_rate(conditions, key, 80, 0.40)
        summaries.append(
            {
                "alpha": alpha,
                "maximum_null_false_admission": maximum_negative,
                "maximum_null_upper_wilson_95": maximum_upper,
                "power_effect_0_40_n120": power_040_n120,
                "power_effect_0_25_n120": power_025_n120,
                "power_effect_0_40_n80": power_040_n80,
                "constraints_passed": bool(
                    maximum_negative <= 0.05
                    and maximum_upper <= 0.07
                    and power_040_n120 >= 0.80
                ),
            }
        )
    return summaries


def _condition_rate(
    conditions: list[dict[str, Any]], key: str, event_count: int, effect: float
) -> float:
    return float(
        next(
            row["alphas"][key]["admission_rate"]
            for row in conditions
            if row["event_count"] == event_count
            and row["standardized_net_effect"] == effect
        )
    )


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise SinglePrimaryAlphaError(f"Frozen {label} is missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Single-Primary Alpha Calibration v3",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Selected alpha: `{payload['selected_alpha']}`",
        f"- Calibration passed: `{payload['calibration_passed']}`",
        "- Historical candidate changes: `0`",
        "- Market/Q4 rows read: `0`",
        "",
        "| Alpha | Max null FPR | Max upper 95% | Power d=.40 n=120 | Power d=.25 n=120 | Pass |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["alpha_summaries"]:
        lines.append(
            f"| {row['alpha']:.3f} | {row['maximum_null_false_admission']:.3f} | "
            f"{row['maximum_null_upper_wilson_95']:.3f} | {row['power_effect_0_40_n120']:.3f} | "
            f"{row['power_effect_0_25_n120']:.3f} | {row['constraints_passed']} |"
        )
    lines.extend(["", "## Interpretation boundary", "", payload["interpretation_boundary"], ""])
    return "\n".join(lines)
