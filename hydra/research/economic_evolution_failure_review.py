from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Mapping

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.schema import stable_hash
from hydra.utils.time import utc_now_iso


REVIEW_SCHEMA = "hydra_economic_evolution_failure_directed_review_v1"
PREREGISTRATION_SCHEMA = (
    "hydra_economic_evolution_failure_directed_review_preregistration_v1"
)


class EconomicEvolutionFailureReviewError(RuntimeError):
    pass


def load_failure_review_preregistration(path: str | Path) -> dict[str, Any]:
    preregistration_path = Path(path).resolve()
    value = json.loads(preregistration_path.read_text(encoding="utf-8"))
    if value.get("schema") != PREREGISTRATION_SCHEMA:
        raise EconomicEvolutionFailureReviewError(
            "unexpected failure-review preregistration schema"
        )
    semantic = dict(value)
    frozen_hash = str(semantic.pop("preregistration_hash", ""))
    if not frozen_hash or stable_hash(semantic) != frozen_hash:
        raise EconomicEvolutionFailureReviewError(
            "failure-review preregistration hash drift"
        )
    if value.get("retrospective_only") is not True:
        raise EconomicEvolutionFailureReviewError(
            "failure review must declare its retrospective role"
        )
    if value.get("new_statistical_comparisons_allowed") is not False:
        raise EconomicEvolutionFailureReviewError(
            "failure review cannot execute new statistical comparisons"
        )
    if int(value.get("multiplicity_delta") or 0) != 0:
        raise EconomicEvolutionFailureReviewError(
            "retrospective failure review cannot reserve multiplicity"
        )
    for key in (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "network_access_allowed",
        "broker_or_orders_allowed",
        "shadow_admission_allowed",
        "proof_window_consumption_allowed",
        "pre_holdout_promotion_allowed",
        "paper_shadow_promotion_allowed",
        "parameter_rescue_allowed",
        "status_inheritance_allowed",
    ):
        if value.get(key) is not False:
            raise EconomicEvolutionFailureReviewError(
                f"protected action enabled in failure review: {key}"
            )
    candidate = value.get("candidate") or {}
    if not candidate.get("policy_id") or not candidate.get(
        "policy_specification_hash"
    ):
        raise EconomicEvolutionFailureReviewError(
            "failure review requires an immutable candidate identity"
        )
    if value.get("next_research_class", {}).get("new_ids_required") is not True:
        raise EconomicEvolutionFailureReviewError(
            "class reformulation must use new identities"
        )
    project_root = preregistration_path.parents[2]
    for relative, digest in value["implementation_files"].items():
        implementation = project_root / str(relative)
        if not implementation.is_file() or _sha256(implementation) != str(digest):
            raise EconomicEvolutionFailureReviewError(
                f"frozen failure-review implementation drift: {relative}"
            )
    implementation_commit = str(value["implementation_commit"])
    if (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
            cwd=project_root,
            check=False,
        ).returncode
        != 0
    ):
        raise EconomicEvolutionFailureReviewError(
            "failure-review implementation commit is not an ancestor"
        )
    return value


def run_economic_evolution_failure_review(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
) -> dict[str, Any]:
    prereg_path = Path(preregistration_path).resolve()
    root = prereg_path.parents[2]
    prereg = load_failure_review_preregistration(prereg_path)
    writer = AtomicResultWriter(output_dir)
    writer.write_json("preregistration_copy.json", prereg)

    sources: dict[str, Path] = {}
    for key, relative in prereg["source_artifacts"]["paths"].items():
        source = _resolve(root, str(relative))
        expected = str(prereg["source_artifacts"]["sha256"][key])
        if not source.is_file() or _sha256(source) != expected:
            raise EconomicEvolutionFailureReviewError(
                f"frozen failure-review source drift: {key}"
            )
        sources[str(key)] = source

    validation = _load_json(sources["validation_result"])
    profiles = _load_json(sources["profile_results"])
    statistics = _load_json(sources["statistical_validation"])
    controls = _load_json(sources["matched_controls"])
    candidate = prereg["candidate"]
    if (
        validation.get("candidate_id") != candidate["policy_id"]
        or validation.get("candidate_specification_hash")
        != candidate["policy_specification_hash"]
        or validation.get("scientific_status")
        != "EXPENSIVE_VALIDATION_UNDERPOWERED"
        or validation.get("validated") is not False
        or validation.get("independent_confirmation_queue_eligible") is not False
    ):
        raise EconomicEvolutionFailureReviewError(
            "failure-review predecessor is not the frozen underpowered outcome"
        )
    gates = validation.get("gates") or {}
    if not isinstance(gates, dict) or any(
        not isinstance(value, bool) for value in gates.values()
    ):
        raise EconomicEvolutionFailureReviewError(
            "failure-review predecessor gates are invalid"
        )

    policy_row = _find_account_policy(
        sources["source_account_policies"], str(candidate["policy_id"])
    )
    policy = policy_row["policy"]
    if stable_hash(policy) != str(candidate["policy_specification_hash"]):
        raise EconomicEvolutionFailureReviewError(
            "failure-review account-policy specification drift"
        )
    sleeve_ids = tuple(str(value) for value in policy["sleeve_ids"])
    frozen_sleeves = tuple(str(value) for value in candidate["sleeve_ids"])
    if sleeve_ids != frozen_sleeves:
        raise EconomicEvolutionFailureReviewError(
            "failure-review frozen sleeve membership drift"
        )
    exact_components = _find_exact_components(
        sources["exact_components"], set(sleeve_ids)
    )

    base = profiles["CONTROLLED_BASE"]
    stress_1_5x = profiles["CONTROLLED_STRESS_1_5X"]
    stress_2x = profiles["CONTROLLED_STRESS_2X"]
    bootstrap = statistics["block_bootstrap"]
    sign_null = statistics["block_sign_randomization"]
    power = statistics["power_calibration"]
    dsr = statistics["DSR"]
    bh = statistics["BH"]

    failure_scores = {
        "INSUFFICIENT_STATISTICAL_POWER": _clip(
            1.0 - float(power["power_on_minimum_useful_effect"])
        ),
        "INSUFFICIENT_TARGET_VELOCITY": _clip(
            1.0
            - float(stress_1_5x["target_progress_median"])
            / float(prereg["diagnostic_reference"]["useful_target_progress"])
        ),
        "CONSISTENCY_RULE_FAILURE": _clip(
            1.0 - float(stress_1_5x["consistency_pass_rate"])
        ),
        "UNSTABLE_TEMPORAL_TRANSFER": _clip(
            1.0
            - int(stress_1_5x["positive_block_count"])
            / max(int(stress_1_5x["block_count"]), 1)
        ),
        "WEAK_COST_MARGIN": _clip(
            1.0
            - float(stress_2x["pooled_net_pnl"])
            / max(float(base["pooled_net_pnl"]), 1e-12)
        ),
        "MLL_BREACH": float(stress_2x["mll_breach_rate"]),
    }
    ranked_failures = sorted(
        failure_scores,
        key=lambda name: (-failure_scores[name], name),
    )
    if ranked_failures[0] != "INSUFFICIENT_STATISTICAL_POWER":
        raise EconomicEvolutionFailureReviewError(
            "frozen failure decision no longer selects statistical power"
        )

    negative_stressed_components = [
        {
            "sleeve_id": row["sleeve_id"],
            "market": row["signal_market"],
            "execution_market": row["execution_market"],
            "role": row["role"],
            "normal_net_usd": float(row["net_pnl"]),
            "stress_1_5x_net_usd": float(row["cost_stress_1_5x_net"]),
        }
        for row in exact_components
        if float(row["cost_stress_1_5x_net"]) <= 0.0
    ]
    block_rows = [
        {
            "block_id": str(row["block_id"]),
            "net_usd": float(row["net_pnl"]),
            "target_progress": float(row["target_progress"]),
            "consistency_ok": bool(row["consistency_ok"]),
            "mll_breached": bool(row["mll_breached"]),
        }
        for row in stress_1_5x["block_results"]
    ]
    negative_blocks = [row["block_id"] for row in block_rows if row["net_usd"] <= 0.0]

    exact_policy_status = "FROZEN_DEVELOPMENT_UNDERPOWERED_NO_PROOF"
    class_status = "CLASS_REFORMULATION_ALLOWED_NEW_IDS_ONLY"
    next_class = prereg["next_research_class"]
    result: dict[str, Any] = {
        "schema": REVIEW_SCHEMA,
        "review_id": prereg["review_id"],
        "completed_at_utc": utc_now_iso(),
        "retrospective_only": True,
        "new_statistical_comparisons_executed": 0,
        "multiplicity_delta": 0,
        "candidate_id": candidate["policy_id"],
        "candidate_specification_hash": candidate["policy_specification_hash"],
        "candidate_exact_status": exact_policy_status,
        "candidate_validated": False,
        "class_status": class_status,
        "dominant_failure": ranked_failures[0],
        "ranked_failure_dimensions": ranked_failures,
        "failure_scores": failure_scores,
        "observed_evidence": {
            "daily_observations": int(base["daily_observation_count"]),
            "effective_independent_observations": float(
                statistics["effective_sample"]["effective_independent_observations"]
            ),
            "normal_net_usd": float(base["pooled_net_pnl"]),
            "stress_1_5x_net_usd": float(stress_1_5x["pooled_net_pnl"]),
            "stress_2x_net_usd": float(stress_2x["pooled_net_pnl"]),
            "positive_blocks": int(stress_1_5x["positive_block_count"]),
            "block_count": int(stress_1_5x["block_count"]),
            "negative_blocks": negative_blocks,
            "block_results_1_5x": block_rows,
            "median_target_progress_1_5x": float(
                stress_1_5x["target_progress_median"]
            ),
            "maximum_target_progress_1_5x": float(
                stress_1_5x["target_progress_maximum"]
            ),
            "mll_breach_rate_2x": float(stress_2x["mll_breach_rate"]),
            "minimum_mll_buffer_2x_usd": float(
                stress_2x["minimum_mll_buffer"]
            ),
            "consistency_pass_rate_1_5x": float(
                stress_1_5x["consistency_pass_rate"]
            ),
            "bootstrap_confidence_interval_95_usd_per_day": [
                float(value) for value in bootstrap["confidence_interval_95"]
            ],
            "probability_mean_net_positive": float(
                bootstrap["probability_mean_net_positive"]
            ),
            "block_sign_null_p_value": float(sign_null["one_sided_p_value"]),
            "dsr_deflated_z": float(dsr["deflated_z"]),
            "bh_rejected": bool(bh["rejected"]),
            "validator_power": float(power["power_on_minimum_useful_effect"]),
            "static_control_dominates": bool(
                controls["static_control_dominates"]
            ),
            "dominating_leave_one_out_sleeves": list(
                controls["dominating_leave_one_out_sleeves"]
            ),
            "negative_stressed_exact_components": negative_stressed_components,
        },
        "decision": {
            "consume_independent_proof": False,
            "reuse_q4": False,
            "purchase_new_data": False,
            "admit_shadow": False,
            "mutate_exact_policy": False,
            "replay_exact_policy_unchanged": False,
            "remove_tombstones": False,
            "inherit_status": False,
            "class_level_reformulation": True,
            "new_ids_required": True,
        },
        "next_research_class": dict(next_class),
        "next_experiment_id": str(next_class["next_experiment_id"]),
        "next_experiment_state": "WORM_PREREGISTRATION_REQUIRED_BEFORE_OUTCOMES",
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "proof_window_consumed": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "outbound_order_capability": False,
        "CONTRE": prereg["CONTRE"],
    }
    result["result_sha256"] = stable_hash(result)
    report = _render_report(result, prereg)
    writer.write_text("failure_directed_review_report.md", report)
    writer.write_json("failure_directed_review_result.json", result)
    return result


def _render_report(result: Mapping[str, Any], prereg: Mapping[str, Any]) -> str:
    evidence = result["observed_evidence"]
    proof = "reports/economic_evolution/failure_review_0006/failure_directed_review_result.json"
    return "\n".join(
        [
            "[HYDRA-V7] phase=4 step=0006 verdict=NULL",
            f"gate=FAILURE_REVIEW_0006 preuve={proof}#{str(result['result_sha256'])[:8]} tests=retrospective_sans_nouveau_test",
            "budget_llm=usage_API_non_exposee/solde budget_data=87.84738838672598/125 N_trials=452628 burned=1",
            "diff_validation=aucun CONTRE=" + str(result["CONTRE"]).replace(" ", "_"),
            "prochaine_action=" + str(result["next_experiment_id"]),
            "",
            "# Revue failure-directed 0006",
            "",
            f"- Politique exacte : `{result['candidate_exact_status']}`.",
            f"- Cause dominante : `{result['dominant_failure']}`.",
            f"- Net normal / 1,5x / 2x : {evidence['normal_net_usd']:.2f} / {evidence['stress_1_5x_net_usd']:.2f} / {evidence['stress_2x_net_usd']:.2f} USD.",
            f"- Blocs positifs : {evidence['positive_blocks']}/{evidence['block_count']}; bloc(s) negatif(s) : {', '.join(evidence['negative_blocks']) or 'aucun'}.",
            f"- Progression cible mediane 1,5x : {100.0 * evidence['median_target_progress_1_5x']:.2f} %; breach MLL 2x : {100.0 * evidence['mll_breach_rate_2x']:.2f} %.",
            f"- Consistance 1,5x : {100.0 * evidence['consistency_pass_rate_1_5x']:.2f} %.",
            f"- IC bootstrap 95 % quotidien : [{evidence['bootstrap_confidence_interval_95_usd_per_day'][0]:.2f}, {evidence['bootstrap_confidence_interval_95_usd_per_day'][1]:.2f}] USD.",
            f"- p-value null par blocs : {evidence['block_sign_null_p_value']:.6f}; DSR z : {evidence['dsr_deflated_z']:.4f}; puissance : {100.0 * evidence['validator_power']:.1f} %.",
            "",
            "La version exacte est gelee : ni retuning, ni nouvelle preuve, ni Q4. La suite est une reformulation de classe avec nouveaux IDs et WORM avant tout resultat.",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )


def _find_account_policy(path: Path, policy_id: str) -> dict[str, Any]:
    matches = [
        row
        for row in _load_jsonl(path)
        if str(row.get("policy", {}).get("policy_id")) == policy_id
    ]
    if len(matches) != 1:
        raise EconomicEvolutionFailureReviewError(
            f"expected one source account policy, found {len(matches)}"
        )
    return matches[0]


def _find_exact_components(path: Path, sleeve_ids: set[str]) -> list[dict[str, Any]]:
    by_id = {
        str(row["sleeve_id"]): row
        for row in _load_jsonl(path)
        if str(row.get("sleeve_id")) in sleeve_ids
    }
    if set(by_id) != sleeve_ids:
        raise EconomicEvolutionFailureReviewError(
            "failure-review exact component membership drift"
        )
    return [by_id[sleeve_id] for sleeve_id in sorted(sleeve_ids)]


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EconomicEvolutionFailureReviewError(f"expected JSON object: {path}")
    return value


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if any(not isinstance(row, dict) for row in rows):
        raise EconomicEvolutionFailureReviewError(f"invalid JSONL rows: {path}")
    return rows


def _resolve(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise EconomicEvolutionFailureReviewError(
            "failure-review source path escapes project root"
        ) from exc
    return path


def _clip(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EconomicEvolutionFailureReviewError",
    "PREREGISTRATION_SCHEMA",
    "REVIEW_SCHEMA",
    "load_failure_review_preregistration",
    "run_economic_evolution_failure_review",
]
