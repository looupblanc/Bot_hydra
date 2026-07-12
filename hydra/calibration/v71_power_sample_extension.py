from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

from hydra.calibration.v71_power_audit import (
    WORLD_SEMI,
    WORLD_SYNTHETIC,
    _empirical_residual_days,
    _evaluate_control_family,
)
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


PLAN_PATH = "WORM/v7.1-power-sample-size-extension-2026-07-12.json"
PLAN_SHA256 = "8e3df65fa75aa3b64c12a3e35f945711d21480ae59a82ba80478df3dd4bddd1a"
EXPECTED_GLOBAL_N_TRIALS = 261_972


class V71PowerSampleExtensionError(RuntimeError):
    pass


def run_power_sample_extension(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/calibration",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    plan = _verify(root, proof_registry_path)
    empirical = _empirical_residual_days(root)
    rows: list[dict[str, Any]] = []
    for world in plan["control_worlds"]:
        for event_count in plan["event_counts"]:
            for effect in plan["net_effects_usd_per_trade"]:
                confusion: defaultdict[str, int] = defaultdict(int)
                for seed in plan["seeds"]:
                    cell = _evaluate_control_family(
                        world=str(world),
                        seed=int(seed),
                        event_count=int(event_count),
                        effect=float(effect),
                        family_size=int(plan["family_size"]),
                        raw_global_trials=258_132,
                        empirical_days=empirical,
                    )
                    for key, value in cell["confusion"].items():
                        confusion[key] += int(value)
                rows.append(
                    {
                        "world": str(world),
                        "event_count": int(event_count),
                        "effect_usd_per_trade": float(effect),
                        "confusion": dict(confusion),
                        "false_positive_rate": _rate(
                            confusion["FP"], confusion["FP"] + confusion["TN"]
                        ),
                        "power": _rate(
                            confusion["TP"], confusion["TP"] + confusion["FN"]
                        ),
                    }
                )
    summaries = {
        world: _summarize_world(
            [row for row in rows if row["world"] == world], plan
        )
        for world in plan["control_worlds"]
    }
    required = [
        int(summary["minimum_required_event_count"])
        for summary in summaries.values()
        if summary["minimum_required_event_count"] is not None
    ]
    passed = len(required) == len(summaries) and all(
        bool(summary["passed"]) for summary in summaries.values()
    )
    result = {
        "schema": "hydra_v7_1_power_sample_extension_result_v1",
        "verdict": "GREEN" if passed else "RED",
        "plan_path": PLAN_PATH,
        "plan_sha256": PLAN_SHA256,
        "world_summaries": summaries,
        "minimum_required_event_count": max(required) if passed else None,
        "cell_results": rows,
        "raw_global_N_trials_before": 258_132,
        "raw_global_N_trials_after": EXPECTED_GLOBAL_N_TRIALS,
        "thresholds_changed": False,
        "real_candidate_results_read": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "A larger required sample can make D1 unable to decide many rare "
            "mechanisms; those cases must remain INSUFFICIENT_POWER."
        ),
        "next_action": (
            "freeze_power_aware_minimum_and_run_D1_stage0_stage2"
            if passed
            else "block_DSR_BH_and_report_validator_power_failure"
        ),
    }
    return _write(result, root, Path(output_dir))


def _summarize_world(
    rows: list[Mapping[str, Any]], plan: Mapping[str, Any]
) -> dict[str, Any]:
    fpr_limit = float(plan["acceptance"]["null_false_positive_rate_max"])
    power_limit = float(plan["acceptance"]["power_min"])
    null_fpr = max(
        float(row["false_positive_rate"])
        for row in rows
        if float(row["effect_usd_per_trade"]) == 0.0
    )
    eligible = [
        row
        for row in rows
        if float(row["effect_usd_per_trade"])
        == float(plan["acceptance"]["target_effect_usd_per_trade"])
        and float(row["power"]) >= power_limit
    ]
    required = min(int(row["event_count"]) for row in eligible) if eligible else None
    return {
        "null_false_positive_rate": null_fpr,
        "minimum_required_event_count": required,
        "power_at_required_count": (
            next(
                float(row["power"])
                for row in eligible
                if int(row["event_count"]) == required
            )
            if required is not None
            else None
        ),
        "passed": null_fpr <= fpr_limit and required is not None,
    }


def _verify(root: Path, proof_registry_path: str | Path) -> dict[str, Any]:
    if _sha256(root / PLAN_PATH) != PLAN_SHA256:
        raise V71PowerSampleExtensionError("power sample extension WORM drift")
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V71PowerSampleExtensionError("power extension reservation is absent")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71PowerSampleExtensionError("unexpected proof-window state")
    return json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))


def _rate(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _write(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_power_sample_extension_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v71_power_sample_extension_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Power sample-size extension",
            "",
            f"[HYDRA-V7] phase=4 step=111 verdict={result['verdict']}",
            f"gate=V71_POWER_SAMPLE preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=3840_controles",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/calibration/v71_power_sample_extension.py CONTRE=la_taille_requise_peut_depasser_la_capacite_D1",
            f"prochaine_action={result['next_action']}",
            "",
            f"- Minimum requis: `{result['minimum_required_event_count']}` événements",
            *[
                f"- {world}: `{summary}`"
                for world, summary in result["world_summaries"].items()
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


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["V71PowerSampleExtensionError", "run_power_sample_extension"]
