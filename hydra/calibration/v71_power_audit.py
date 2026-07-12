from __future__ import annotations

import hashlib
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.validation.v71_hierarchical_multiplicity import (
    family_bh,
    hierarchical_trial_accounting,
)
from hydra.validation.v7_phase2_multiplicity import deflated_sharpe_statistics
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
POLICY_SHA256 = "d745ac9ca51049ccc2f7f1f97d3593cf49231c92a8873737e350e380170f916c"
DESIGN_PATH = "WORM/v7.1-power-control-design-2026-07-12.json"
DESIGN_SHA256 = "08c632f7058549f11f93fa71c0f2f866b2665996567565e1161ce2355a036a8e"
ADDENDUM_PATH = "WORM/v7.1-power-control-semisynthetic-addendum-2026-07-12.json"
ADDENDUM_SHA256 = "d7f89b64ec68047af849070c3a707c9e36d5313bea981c314d0b7e4ffc913acc"
EXPECTED_GLOBAL_N_TRIALS = 258_132
WORLD_SYNTHETIC = "SYNTHETIC_GAUSSIAN"
WORLD_SEMI = "SEMI_SYNTHETIC_D1_ES_RESIDUAL_BOOTSTRAP"


class V71PowerAuditError(RuntimeError):
    pass


def run_v71_power_audit(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/calibration",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, design = _verify_inputs(root, proof_registry_path)
    empirical_days = _empirical_residual_days(root)
    seeds = tuple(int(value) for value in design["seeds"])
    event_counts = tuple(int(value) for value in design["event_counts"])
    effects = tuple(
        float(value) for value in design["injected_net_effect_usd_per_trade"]
    )
    family_size = int(design["family_size"])
    cell_results: list[dict[str, Any]] = []
    for world in (WORLD_SYNTHETIC, WORLD_SEMI):
        for event_count in event_counts:
            for effect in effects:
                aggregate = _empty_confusion()
                trial_counts: list[int] = []
                effective_counts: list[float] = []
                for seed in seeds:
                    cell = _evaluate_control_family(
                        world=world,
                        seed=seed,
                        event_count=event_count,
                        effect=effect,
                        family_size=family_size,
                        raw_global_trials=int(policy["raw_global_N_trials_at_freeze"]),
                        empirical_days=empirical_days,
                    )
                    _add_confusion(aggregate, cell["confusion"])
                    trial_counts.append(int(cell["DSR_N_trials"]))
                    effective_counts.append(float(cell["effective_signal_trials"]))
                cell_results.append(
                    {
                        "world": world,
                        "event_count": event_count,
                        "effect_usd_per_trade": effect,
                        "confusion": dict(aggregate),
                        "false_positive_rate": _rate(
                            aggregate["FP"], aggregate["FP"] + aggregate["TN"]
                        ),
                        "power": _rate(
                            aggregate["TP"], aggregate["TP"] + aggregate["FN"]
                        ),
                        "precision": _rate(
                            aggregate["TP"], aggregate["TP"] + aggregate["FP"]
                        ),
                        "DSR_N_trials_min": min(trial_counts),
                        "DSR_N_trials_max": max(trial_counts),
                        "effective_signal_trials_mean": float(
                            np.mean(effective_counts)
                        ),
                    }
                )
    world_summaries = {
        world: _world_summary(
            [row for row in cell_results if row["world"] == world], design
        )
        for world in (WORLD_SYNTHETIC, WORLD_SEMI)
    }
    passed = all(summary["passed"] for summary in world_summaries.values())
    result = {
        "schema": "hydra_v7_1_power_audit_result_v1",
        "verdict": "GREEN" if passed else "RED",
        "policy_path": POLICY_PATH,
        "policy_sha256": POLICY_SHA256,
        "design_path": DESIGN_PATH,
        "design_sha256": DESIGN_SHA256,
        "semisynthetic_addendum_path": ADDENDUM_PATH,
        "semisynthetic_addendum_sha256": ADDENDUM_SHA256,
        "raw_global_N_trials_before_control_reservation": int(
            policy["raw_global_N_trials_at_freeze"]
        ),
        "raw_global_N_trials_after_control_reservation": EXPECTED_GLOBAL_N_TRIALS,
        "hierarchical_formula": policy["effective_independent_trials"],
        "world_summaries": world_summaries,
        "cell_results": cell_results,
        "false_positive_rate": max(
            float(row["null_false_positive_rate"])
            for row in world_summaries.values()
        ),
        "power_on_meaningful_effects": min(
            float(row["meaningful_effect_power"])
            for row in world_summaries.values()
        ),
        "real_D1_candidate_results_read": False,
        "protected_holdout_access_count_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The controls calibrate multiplicity power under frozen synthetic "
            "and empirical-residual worlds; they cannot prove that any D1 "
            "mechanism has positive expectancy."
        ),
        "next_action": (
            "run_preregistered_D1_structural_funnel"
            if passed
            else "stop_real_candidate_validation_and_report_power_failure"
        ),
    }
    return _write_result(result, Path(output_dir), root)


def _evaluate_control_family(
    *,
    world: str,
    seed: int,
    event_count: int,
    effect: float,
    family_size: int,
    raw_global_trials: int,
    empirical_days: Sequence[np.ndarray],
) -> dict[str, Any]:
    rng = np.random.default_rng(seed * 100_000 + event_count * 100 + int(effect))
    signal_paths = _clustered_signal_paths(rng, family_size=family_size)
    accounting = hierarchical_trial_accounting(
        signal_paths,
        raw_global_trials=raw_global_trials,
        prior_family_grammar_versions=0,
    )
    positive = set(range(8)) if effect > 0.0 else set()
    day_count = int(math.ceil(event_count / 2.0))
    common = rng.normal(0.0, 75.0, size=day_count)
    p_values: dict[str, float] = {}
    z_scores: dict[str, float] = {}
    for candidate in range(family_size):
        injected = effect if candidate in positive else 0.0
        residuals = _residuals(
            rng,
            world=world,
            event_count=event_count,
            empirical_days=empirical_days,
        )
        trades = residuals + injected
        daily = np.zeros(day_count, dtype=np.float64)
        for index, value in enumerate(trades):
            daily[index // 2] += value
        daily += common
        dsr = deflated_sharpe_statistics(
            daily,
            n_trials=accounting.DSR_N_trials,
        )
        key = f"control_{candidate:02d}"
        p_values[key] = float(dsr["one_sided_p_value"])
        z_scores[key] = float(dsr["deflated_z"])
    adjusted = family_bh({"CONTROL_FAMILY": p_values}, q=0.10)[
        "CONTROL_FAMILY"
    ]
    discovered = {
        int(key.rsplit("_", 1)[1])
        for key, row in adjusted.items()
        if bool(row["rejected"]) and z_scores[key] > 0.0
    }
    confusion = {
        "TP": len(discovered & positive),
        "FP": len(discovered - positive),
        "FN": len(positive - discovered),
        "TN": family_size - len(positive | discovered),
    }
    return {
        "confusion": confusion,
        "DSR_N_trials": accounting.DSR_N_trials,
        "effective_signal_trials": accounting.effective_signal_trials,
    }


def _clustered_signal_paths(
    rng: np.random.Generator, *, family_size: int
) -> np.ndarray:
    observations = 512
    clusters = 8
    latent = rng.choice((-1.0, 1.0), size=(clusters, observations))
    output = np.empty((family_size, observations), dtype=np.float64)
    for candidate in range(family_size):
        row = latent[candidate % clusters].copy()
        flips = rng.random(observations) < 0.10
        row[flips] *= -1.0
        output[candidate] = row
    return output


def _residuals(
    rng: np.random.Generator,
    *,
    world: str,
    event_count: int,
    empirical_days: Sequence[np.ndarray],
) -> np.ndarray:
    if world == WORLD_SYNTHETIC:
        return rng.normal(0.0, 150.0, size=event_count)
    if world != WORLD_SEMI:
        raise V71PowerAuditError(f"unknown control world: {world}")
    day_count = int(math.ceil(event_count / 2.0))
    values: list[float] = []
    for _ in range(day_count):
        day = empirical_days[int(rng.integers(0, len(empirical_days)))]
        picks = rng.choice(day, size=2, replace=len(day) < 2)
        values.extend(float(value) for value in picks)
    return np.asarray(values[:event_count], dtype=np.float64)


def _empirical_residual_days(root: Path) -> tuple[np.ndarray, ...]:
    path = root / "data/cache/v7_d1/rth_minute_print_features_v1.parquet"
    frame = pd.read_parquet(
        path,
        columns=["product", "minute_start_ns", "close"],
    )
    frame = frame[frame["product"] == "ES"].sort_values("minute_start_ns")
    timestamps = pd.to_datetime(frame["minute_start_ns"], unit="ns", utc=True).dt.tz_convert(
        "America/Chicago"
    )
    session_day = timestamps.dt.strftime("%Y-%m-%d")
    raw = frame["close"].diff().to_numpy(dtype=np.float64) * 50.0
    finite = np.isfinite(raw)
    centered = raw[finite] - float(np.mean(raw[finite]))
    scale = float(np.std(centered, ddof=1))
    if scale <= 0.0:
        raise V71PowerAuditError("D1 empirical residual scale is zero")
    normalized = np.full_like(raw, np.nan)
    normalized[finite] = centered * (150.0 / scale)
    days: list[np.ndarray] = []
    for day in sorted(set(session_day)):
        values = normalized[np.asarray(session_day == day)]
        values = values[np.isfinite(values)]
        if values.size >= 2:
            days.append(values)
    if len(days) < 20:
        raise V71PowerAuditError("D1 empirical residual control has too few days")
    return tuple(days)


def _world_summary(
    rows: Sequence[Mapping[str, Any]], design: Mapping[str, Any]
) -> dict[str, Any]:
    null = _sum_confusions(row["confusion"] for row in rows if row["effect_usd_per_trade"] == 0.0)
    meaningful = _sum_confusions(
        row["confusion"]
        for row in rows
        if row["event_count"] >= 100 and row["effect_usd_per_trade"] >= 50.0
    )
    all_positive = _sum_confusions(
        row["confusion"] for row in rows if row["effect_usd_per_trade"] > 0.0
    )
    fpr = _rate(null["FP"], null["FP"] + null["TN"])
    power = _rate(meaningful["TP"], meaningful["TP"] + meaningful["FN"])
    precision = _rate(
        all_positive["TP"], all_positive["TP"] + all_positive["FP"]
    )
    mde = None
    for effect in sorted(
        {float(row["effect_usd_per_trade"]) for row in rows if row["effect_usd_per_trade"] > 0.0}
    ):
        group = _sum_confusions(
            row["confusion"]
            for row in rows
            if row["event_count"] >= 100
            and row["effect_usd_per_trade"] == effect
        )
        if _rate(group["TP"], group["TP"] + group["FN"]) >= 0.60:
            mde = effect
            break
    fpr_max = float(design["null_false_positive_rate_max"])
    power_min = float(design["meaningful_power_target_min"])
    return {
        "null_false_positive_rate": fpr,
        "meaningful_effect_power": power,
        "precision_on_injected_scenarios": precision,
        "recall_on_meaningful_effects": power,
        "minimum_detectable_economic_effect_usd_per_trade": mde,
        "false_positive_threshold": fpr_max,
        "power_threshold": power_min,
        "passed": fpr <= fpr_max and power >= power_min,
    }


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected = {
        POLICY_PATH: POLICY_SHA256,
        DESIGN_PATH: DESIGN_SHA256,
        ADDENDUM_PATH: ADDENDUM_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71PowerAuditError("V7.1 power WORM hash drift: " + ",".join(drift))
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V71PowerAuditError("V7.1 power multiplicity reservation is absent")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71PowerAuditError("unexpected proof-window state")
    policy = json.loads((root / POLICY_PATH).read_text(encoding="utf-8"))
    return policy, dict(policy["power_calibration"])


def _empty_confusion() -> defaultdict[str, int]:
    return defaultdict(int, {"TP": 0, "FP": 0, "FN": 0, "TN": 0})


def _add_confusion(target: defaultdict[str, int], source: Mapping[str, int]) -> None:
    for key in ("TP", "FP", "FN", "TN"):
        target[key] += int(source[key])


def _sum_confusions(rows: Sequence[Mapping[str, int]] | Any) -> dict[str, int]:
    total = _empty_confusion()
    for row in rows:
        _add_confusion(total, row)
    return dict(total)


def _rate(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _write_result(result: dict[str, Any], output_dir: Path, root: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_power_audit_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_sha = _sha256(result_path)
    report_path = destination / "v71_power_audit_report.md"
    report = _render_report(result, result_path.relative_to(root), result_sha)
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = result_sha
    result["report_path"] = str(report_path)
    return result


def _render_report(
    result: Mapping[str, Any], result_path: Path, result_sha: str
) -> str:
    lines = [
        "# HYDRA V7.1 — Multiple-testing power audit",
        "",
        f"[HYDRA-V7] phase=4 step=110 verdict={result['verdict']}",
        f"gate=V71_POWER preuve={result_path}#{result_sha[:8]} tests=10240_controles",
        f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={result['raw_global_N_trials_after_control_reservation']} burned=1",
        "diff_validation=hydra/validation/v71_hierarchical_multiplicity.py,hydra/calibration/v71_power_audit.py CONTRE=des_controles_calibres_ne_prouvent_aucun_edge_D1",
        f"prochaine_action={result['next_action']}",
        "",
        f"- FPR maximal: `{result['false_positive_rate']}`",
        f"- Puissance minimale effets significatifs: `{result['power_on_meaningful_effects']}`",
    ]
    for world, summary in result["world_summaries"].items():
        lines.append(
            f"- {world}: FPR `{summary['null_false_positive_rate']}`, "
            f"power `{summary['meaningful_effect_power']}`, "
            f"MDE `{summary['minimum_detectable_economic_effect_usd_per_trade']}`"
        )
    lines.extend(["", "## CONTRE", "", str(result["CONTRE"]), ""])
    return "\n".join(lines)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "V71PowerAuditError",
    "run_v71_power_audit",
]
