from __future__ import annotations

import hashlib
import json
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from hydra.calibration.selection_null_power import (
    ADJUSTED_THRESHOLD,
    BLOCK_SIZE,
    COST_RATIO,
    EVENT_COUNTS,
    FAMILY_SIZE,
    NULL_DRAWS,
    REPLICATIONS,
    _family_block_probabilities,
    _synthetic_gross_returns,
    _wilson_interval,
)
from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.equity_open_gap_reversal import _write_immutable
from hydra.research.qd_economic_tournament import _benjamini_hochberg


VERSION = "selection_null_policy_repair_v2"
POLICIES = (
    "BH_Q_0_20_BASELINE",
    "BH_Q_0_05",
    "HOLM_FWER_0_05",
    "SINGLE_PRESELECTED_PRIMARY_ALPHA_0_05",
    "FIVE_FAMILY_PRIMARIES_BONFERRONI_0_01",
)
EFFECTS = (0.0, 0.25, 0.40)


class SelectionNullPolicyRepairError(RuntimeError):
    pass


def run_selection_null_policy_repair(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    source_calibration_result_path: str | Path,
    source_calibration_result_sha256: str,
    source_calibration_result_hash: str,
    code_commit: str,
    random_seed: int = 773701,
    replications: int = REPLICATIONS,
    null_draws: int = NULL_DRAWS,
) -> dict[str, Any]:
    task = Path(engineering_task_path)
    source_path = Path(source_calibration_result_path)
    _verify(task, engineering_task_sha256, "engineering task")
    _verify(source_path, source_calibration_result_sha256, "v1 calibration result")
    source = json.loads(source_path.read_text(encoding="utf-8"))
    if (
        source.get("result_hash") != source_calibration_result_hash
        or source.get("scientific_conclusion")
        != "SELECTION_NULL_POLICY_FALSE_POSITIVE_CONTROL_FAILED"
        or bool(source.get("calibration_passed"))
    ):
        raise SelectionNullPolicyRepairError("Calibration v1 does not authorize policy repair.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise SelectionNullPolicyRepairError("Worker commit differs from queued specification.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = {
        "schema": "selection_null_policy_repair_preregistration_v2",
        "policies": POLICIES,
        "family_size": FAMILY_SIZE,
        "event_counts": EVENT_COUNTS,
        "effects": EFFECTS,
        "replications": replications,
        "null_draws": null_draws,
        "block_size": BLOCK_SIZE,
        "cost_ratio": COST_RATIO,
        "selection_rule": [
            "maximum_null_family_fpr_le_0_05",
            "power_effect_0_40_n120_ge_0_80",
            "maximize_power_effect_0_25_n120",
            "maximize_preregistered_promotable_slots",
            "minimize_complexity",
        ],
        "prospective_only": True,
        "historical_status_mutation_allowed": False,
        "source_calibration_result_hash": source_calibration_result_hash,
        "task_sha256": engineering_task_sha256,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "market_data_access": False,
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "selection_null_policy_repair_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )

    conditions = []
    for count_index, event_count in enumerate(EVENT_COUNTS):
        for effect_index, effect in enumerate(EFFECTS):
            conditions.append(
                simulate_policy_condition(
                    event_count=event_count,
                    standardized_net_effect=effect,
                    replications=replications,
                    null_draws=null_draws,
                    seed=random_seed + count_index * 100_003 + effect_index * 10_007,
                )
            )
    summaries = _policy_summaries(conditions)
    eligible = [item for item in summaries if item["calibration_constraints_passed"]]
    selected = sorted(
        eligible,
        key=lambda item: (
            -float(item["power_effect_0_25_n120"]),
            -int(item["promotable_slots"]),
            int(item["complexity_rank"]),
            str(item["policy"]),
        ),
    )
    chosen = selected[0] if selected else None
    conclusion = (
        "SELECTION_NULL_POLICY_REPAIRED_PROSPECTIVELY"
        if chosen is not None
        else "NO_PROSPECTIVE_POLICY_MET_BOTH_FPR_AND_POWER"
    )
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "The chosen policy applies only to future frozen manifests. Existing p-values, "
            "candidate tiers and shadow status remain unchanged. Diagnostic elites cannot inherit "
            "the primary candidate's evidence."
        ),
        "source_calibration": {
            "path": str(source_path),
            "sha256": source_calibration_result_sha256,
            "result_hash": source_calibration_result_hash,
        },
        "conditions": conditions,
        "policy_summaries": summaries,
        "selected_policy": chosen,
        "prospective_policy_contract": (
            {
                "policy": chosen["policy"],
                "promotion_primary_count": int(chosen["promotable_slots"]),
                "primary_selection_period": "earlier_development_fold_only",
                "primary_freeze_required_before_confirmation": True,
                "remaining_elites": "diagnostic_only_no_promotion_from_same_fold",
                "new_candidate_ids_required": True,
                "historical_reclassification": False,
                "q4_access_authorized": False,
            }
            if chosen is not None
            else None
        ),
        "calibration_passed": chosen is not None,
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
            "FREEZE_ONE_NEW_PRIMARY_PER_TOURNAMENT_BEFORE_CONFIRMATION_AND_KEEP_QD_ELITES_DIAGNOSTIC"
            if chosen is not None
            else "PREREGISTER_TIGHTER_SINGLE_PRIMARY_ALPHA_CALIBRATION"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "selection_null_policy_repair_result.json"
    report_path = destination / "selection_null_policy_repair_report.md"
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


def simulate_policy_condition(
    *,
    event_count: int,
    standardized_net_effect: float,
    replications: int,
    null_draws: int,
    seed: int,
) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    admission_counts = defaultdict(int)
    family_false_counts = defaultdict(int)
    for _ in range(replications):
        effects = np.zeros(FAMILY_SIZE, dtype=float)
        if standardized_net_effect > 0:
            effects[0] = standardized_net_effect
        gross = _synthetic_gross_returns(
            rng, event_count=event_count, net_effects=effects
        )
        probabilities, net = _family_block_probabilities(
            gross, cost=COST_RATIO, draws=null_draws, rng=rng
        )
        economic = (net > 0) & (
            (-gross.sum(axis=1) - COST_RATIO * event_count) < 0
        )
        decisions = policy_decisions(probabilities, economic)
        for policy, admitted in decisions.items():
            if standardized_net_effect > 0:
                admission_counts[policy] += int(admitted[0])
                false = admitted.copy()
                false[0] = False
            else:
                false = admitted
            family_false_counts[policy] += int(false.any())
    return {
        "event_count": event_count,
        "standardized_net_effect": standardized_net_effect,
        "replications": replications,
        "policies": {
            policy: {
                "injected_candidate_power": (
                    admission_counts[policy] / replications
                    if standardized_net_effect > 0
                    else 0.0
                ),
                "family_false_admission_rate": family_false_counts[policy]
                / replications,
                "family_fpr_interval_95": _wilson_interval(
                    family_false_counts[policy], replications
                ),
            }
            for policy in POLICIES
        },
    }


def policy_decisions(
    probabilities: np.ndarray, economic: np.ndarray
) -> dict[str, np.ndarray]:
    adjusted_20 = np.asarray(_benjamini_hochberg(probabilities.tolist()))
    decisions: dict[str, np.ndarray] = {
        "BH_Q_0_20_BASELINE": (adjusted_20 <= 0.20) & economic,
        "BH_Q_0_05": (adjusted_20 <= 0.05) & economic,
        "HOLM_FWER_0_05": _holm_rejections(probabilities, 0.05) & economic,
    }
    primary = np.zeros(FAMILY_SIZE, dtype=bool)
    primary[0] = probabilities[0] <= 0.05 and economic[0]
    decisions["SINGLE_PRESELECTED_PRIMARY_ALPHA_0_05"] = primary
    family_primaries = np.zeros(FAMILY_SIZE, dtype=bool)
    family_primaries[:5] = (probabilities[:5] <= 0.01) & economic[:5]
    decisions["FIVE_FAMILY_PRIMARIES_BONFERRONI_0_01"] = family_primaries
    return decisions


def _holm_rejections(probabilities: np.ndarray, alpha: float) -> np.ndarray:
    values = np.asarray(probabilities, dtype=float)
    order = np.argsort(values)
    rejected = np.zeros(len(values), dtype=bool)
    for rank, index in enumerate(order):
        threshold = alpha / (len(values) - rank)
        if values[index] > threshold:
            break
        rejected[index] = True
    return rejected


def _policy_summaries(conditions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    slots = {
        "BH_Q_0_20_BASELINE": 20,
        "BH_Q_0_05": 20,
        "HOLM_FWER_0_05": 20,
        "SINGLE_PRESELECTED_PRIMARY_ALPHA_0_05": 1,
        "FIVE_FAMILY_PRIMARIES_BONFERRONI_0_01": 5,
    }
    complexity = {
        "SINGLE_PRESELECTED_PRIMARY_ALPHA_0_05": 1,
        "FIVE_FAMILY_PRIMARIES_BONFERRONI_0_01": 2,
        "HOLM_FWER_0_05": 3,
        "BH_Q_0_05": 4,
        "BH_Q_0_20_BASELINE": 5,
    }
    summaries = []
    for policy in POLICIES:
        null_fpr = max(
            row["policies"][policy]["family_false_admission_rate"]
            for row in conditions
            if row["standardized_net_effect"] == 0.0
        )
        power_040 = next(
            row["policies"][policy]["injected_candidate_power"]
            for row in conditions
            if row["event_count"] == 120
            and row["standardized_net_effect"] == 0.40
        )
        power_025 = next(
            row["policies"][policy]["injected_candidate_power"]
            for row in conditions
            if row["event_count"] == 120
            and row["standardized_net_effect"] == 0.25
        )
        summaries.append(
            {
                "policy": policy,
                "maximum_null_family_fpr": null_fpr,
                "power_effect_0_40_n120": power_040,
                "power_effect_0_25_n120": power_025,
                "promotable_slots": slots[policy],
                "complexity_rank": complexity[policy],
                "calibration_constraints_passed": bool(
                    null_fpr <= 0.05 and power_040 >= 0.80
                ),
            }
        )
    return summaries


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise SelectionNullPolicyRepairError(f"Frozen {label} is missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Selection Null Policy Repair v2",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Selected prospective policy: `{(payload.get('selected_policy') or {}).get('policy')}`",
        "- Historical candidate changes: `0`",
        "- Market/Q4 rows read: `0`",
        "",
        "| Policy | Max family FPR | Power d=.40 n=120 | Power d=.25 n=120 | Slots | Pass |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in payload["policy_summaries"]:
        lines.append(
            f"| {row['policy']} | {row['maximum_null_family_fpr']:.3f} | "
            f"{row['power_effect_0_40_n120']:.3f} | {row['power_effect_0_25_n120']:.3f} | "
            f"{row['promotable_slots']} | {row['calibration_constraints_passed']} |"
        )
    lines.extend(["", "## Interpretation boundary", "", payload["interpretation_boundary"], ""])
    return "\n".join(lines)
