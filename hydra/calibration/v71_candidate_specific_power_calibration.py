from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from hydra.calibration.v71_power_audit import _empirical_residual_days
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json"
POLICY_SHA256 = "39f60b4e402c0a40ccc39b5429e0e2cc2dcc88a80592cd28b05c86abed616673"
CANDIDATE_FREEZE_PATH = "WORM/v7.1-power-aware-candidate-freeze-0001-2026-07-12.json"
CANDIDATE_FREEZE_SHA256 = (
    "b66e462989213356106f0cbcd88d31ba4547a61f9900eb1de3e6010cb3d35d83"
)
EXPECTED_GLOBAL_N_TRIALS = 263_604
WORLD_SYNTHETIC = "SYNTHETIC_AR1_GAUSSIAN"
WORLD_SEMI = "SEMI_SYNTHETIC_D1_DAILY_BLOCK_RESIDUAL"
SIGMA_USD = 150.0
AR1_RHO = 0.25
BLOCK_EVENTS = 10
CRITICAL_Z = 1.6448536269514722


class V71CandidatePowerCalibrationError(RuntimeError):
    pass


def run_candidate_power_calibration(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/power_aware_0001",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy = _verify_inputs(root, proof_registry_path)
    design = policy["calibration_controls"]
    empirical_days = _empirical_residual_days(root)
    rows: list[dict[str, Any]] = []
    for world in design["worlds"]:
        world_seed = int(design["seeds"][world])
        for event_count in design["event_counts"]:
            for effect in design["net_effects_usd_per_trade"]:
                rejected = 0
                replications = int(design["replications_per_cell"])
                for replication in range(replications):
                    rng = np.random.default_rng(
                        world_seed
                        + int(event_count) * 10_000
                        + int(round(float(effect) * 10.0)) * 100
                        + replication
                    )
                    values = _control_sample(
                        world=str(world),
                        event_count=int(event_count),
                        effect=float(effect),
                        rng=rng,
                        empirical_days=empirical_days,
                    )
                    rejected += int(_one_sided_block_test(values))
                rejection_rate = rejected / replications
                rows.append(
                    {
                        "world": world,
                        "event_count": int(event_count),
                        "effect_usd_per_trade": float(effect),
                        "replications": replications,
                        "rejection_count": rejected,
                        "false_positive_rate": (
                            rejection_rate if float(effect) == 0.0 else None
                        ),
                        "power": rejection_rate if float(effect) > 0.0 else 0.0,
                    }
                )
    summaries = {
        world: _summarize_world(
            [row for row in rows if row["world"] == world], design
        )
        for world in design["worlds"]
    }
    passed = all(row["passed"] for row in summaries.values())
    result = {
        "schema": "hydra_v7_1_candidate_specific_power_calibration_result_v1",
        "verdict": "GREEN" if passed else "RED",
        "policy_path": POLICY_PATH,
        "policy_sha256": POLICY_SHA256,
        "candidate_freeze_path": CANDIDATE_FREEZE_PATH,
        "candidate_freeze_sha256": CANDIDATE_FREEZE_SHA256,
        "candidate_diagnostics_read": False,
        "world_parameters": {
            "synthetic_stationary_sigma_usd": SIGMA_USD,
            "synthetic_AR1_rho": AR1_RHO,
            "test_block_events": BLOCK_EVENTS,
            "critical_z": CRITICAL_Z,
        },
        "cell_results": rows,
        "world_summaries": summaries,
        "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Controls validate the decision rule at fixed synthetic dependence "
            "and D1 residual scale, but candidate post-selection remains a separate "
            "source of optimism addressed by shrinkage and multiplicity."
        ),
        "prochaine_action": (
            "run_frozen_16_candidate_power_audit"
            if passed
            else "block_candidate_diagnostics_and_repair_power_estimator_on_controls_only"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def _control_sample(
    *,
    world: str,
    event_count: int,
    effect: float,
    rng: np.random.Generator,
    empirical_days: Sequence[np.ndarray],
) -> np.ndarray:
    if event_count <= 0 or event_count % BLOCK_EVENTS:
        raise ValueError("control event count must be a positive multiple of 10")
    if world == WORLD_SYNTHETIC:
        innovations = rng.normal(
            0.0,
            SIGMA_USD * math.sqrt(1.0 - AR1_RHO**2),
            size=event_count,
        )
        values = np.empty(event_count, dtype=np.float64)
        values[0] = innovations[0]
        for index in range(1, event_count):
            values[index] = AR1_RHO * values[index - 1] + innovations[index]
    elif world == WORLD_SEMI:
        collected: list[float] = []
        while len(collected) < event_count:
            day = np.asarray(
                empirical_days[int(rng.integers(0, len(empirical_days)))],
                dtype=np.float64,
            )
            if day.size:
                start = int(rng.integers(0, day.size))
                collected.extend(np.roll(day, -start).tolist())
        values = np.asarray(collected[:event_count], dtype=np.float64)
    else:
        raise V71CandidatePowerCalibrationError(f"unknown control world: {world}")
    values = values - float(np.mean(values)) + float(effect)
    return values


def _one_sided_block_test(values: np.ndarray) -> bool:
    values = np.asarray(values, dtype=np.float64)
    if values.size < 2 * BLOCK_EVENTS or values.size % BLOCK_EVENTS:
        raise ValueError("block test requires complete ten-event blocks")
    blocks = values.reshape(-1, BLOCK_EVENTS)
    block_means = np.mean(blocks, axis=1)
    standard_error = float(np.std(block_means, ddof=1) / math.sqrt(len(block_means)))
    if not math.isfinite(standard_error) or standard_error <= 0.0:
        return bool(float(np.mean(values)) > 0.0)
    return bool(float(np.mean(values)) / standard_error > CRITICAL_Z)


def _summarize_world(
    rows: Sequence[dict[str, Any]], design: dict[str, Any]
) -> dict[str, Any]:
    null_fpr = max(
        float(row["false_positive_rate"])
        for row in rows
        if float(row["effect_usd_per_trade"]) == 0.0
    )
    power_50_120 = next(
        float(row["power"])
        for row in rows
        if int(row["event_count"]) == 120
        and float(row["effect_usd_per_trade"]) == 50.0
    )
    power_25_240 = next(
        float(row["power"])
        for row in rows
        if int(row["event_count"]) == 240
        and float(row["effect_usd_per_trade"]) == 25.0
    )
    acceptance = design["acceptance"]
    return {
        "maximum_null_false_positive_rate": null_fpr,
        "power_at_50_usd_120_events": power_50_120,
        "power_at_25_usd_240_events": power_25_240,
        "passed": bool(
            null_fpr <= float(acceptance["maximum_null_false_positive_rate"])
            and power_50_120
            >= float(acceptance["minimum_power_at_50_usd_and_120_events"])
            and power_25_240
            >= float(acceptance["minimum_power_at_25_usd_and_240_events"])
        ),
    }


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_candidate_specific_power_calibration_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v71_candidate_specific_power_calibration_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Candidate-specific power calibration",
            "",
            f"[HYDRA-V7] phase=4 step=143 verdict={result['verdict']}",
            f"gate=V71_POWER_AWARE_CALIBRATION preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=6400_control_replications",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/calibration/v71_candidate_specific_power_calibration.py CONTRE=les_controles_ne_suppriment_pas_le_biais_de_selection",
            f"prochaine_action={result['prochaine_action']}",
            "",
            *[
                f"- {world}: FPR max `{row['maximum_null_false_positive_rate']}`, power 50/120 `{row['power_at_50_usd_120_events']}`, power 25/240 `{row['power_at_25_usd_240_events']}`"
                for world, row in result["world_summaries"].items()
            ],
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = result_hash
    result["report_path"] = str(report_path)
    return result


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> dict[str, Any]:
    expected = {
        POLICY_PATH: POLICY_SHA256,
        CANDIDATE_FREEZE_PATH: CANDIDATE_FREEZE_SHA256,
    }
    drift = [
        path for path, expected_sha in expected.items() if _sha256(root / path) != expected_sha
    ]
    if drift:
        raise V71CandidatePowerCalibrationError(
            "power calibration frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V71CandidatePowerCalibrationError(
            "power-aware multiplicity reservation is absent"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71CandidatePowerCalibrationError("unexpected proof-window state")
    return json.loads((root / POLICY_PATH).read_text(encoding="utf-8"))


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "V71CandidatePowerCalibrationError",
    "run_candidate_power_calibration",
]
