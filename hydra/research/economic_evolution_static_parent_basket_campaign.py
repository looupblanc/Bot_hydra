from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

import hydra.economic_evolution.account_elite_robustness_evaluation as evaluation
import hydra.research.economic_evolution_elite_robustness_campaign as base
from hydra.economic_evolution.account_static_parent_basket import (
    STATIC_PARENT_BASKET_CLASS_ID,
    STATIC_PARENT_BASKET_DEEP_QUOTAS,
    STATIC_PARENT_BASKET_MUTATION_QUOTAS,
    STATIC_PARENT_BASKET_POLICY_VERSION,
    generate_static_parent_basket_population,
    route_static_parent_basket_entry,
)
from hydra.economic_evolution.account_static_parent_basket_evaluation import (
    STATIC_PARENT_BASKET_EVALUATION_VERSION,
    evaluate_static_parent_basket_policy_pairs,
)
from hydra.economic_evolution.schema import stable_hash


STATIC_PARENT_BASKET_ENGINE_VERSION = "hydra_static_parent_basket_campaign_v1"


def run_static_parent_basket_campaign(*args: Any, **kwargs: Any) -> dict[str, Any]:
    preregistration = Path(kwargs["preregistration_path"]).resolve()
    config = json.loads(preregistration.read_text(encoding="utf-8"))
    root = _project_root(preregistration)
    reference = config["parent_bank"]
    path = root / str(reference["path"])
    if _sha256(path) != str(reference["file_sha256"]):
        raise ValueError("static parent bank file checksum drift")
    parent_bank = json.loads(path.read_text(encoding="utf-8"))
    if parent_bank.get("bank_hash") != reference["semantic_hash"]:
        raise ValueError("static parent bank semantic drift")
    payload = dict(parent_bank)
    claimed = payload.pop("bank_hash", None)
    if claimed != stable_hash(payload):
        raise ValueError("static parent bank internal hash drift")
    with _patched_static_parent_campaign(parent_bank):
        return base.run_elite_robustness_campaign(*args, **kwargs)


@contextmanager
def _patched_static_parent_campaign(
    parent_bank: Mapping[str, Any],
) -> Iterator[None]:
    def generate(*args: Any, **kwargs: Any) -> Any:
        return generate_static_parent_basket_population(
            *args, parent_bank=parent_bank, **kwargs
        )

    replacements: tuple[tuple[object, str, object], ...] = (
        (base, "ELITE_ROBUSTNESS_CLASS_ID", STATIC_PARENT_BASKET_CLASS_ID),
        (base, "ELITE_ROBUSTNESS_ENGINE_VERSION", STATIC_PARENT_BASKET_ENGINE_VERSION),
        (base, "MUTATION_QUOTAS", STATIC_PARENT_BASKET_MUTATION_QUOTAS),
        (base, "DEEP_EVALUATION_QUOTAS", STATIC_PARENT_BASKET_DEEP_QUOTAS),
        (base, "generate_elite_robustness_population", generate),
        (
            base,
            "evaluate_elite_robustness_policy_pairs",
            evaluate_static_parent_basket_policy_pairs,
        ),
        (base, "_next_action", _static_parent_next_action),
        (base, "_report", _static_parent_report),
        (evaluation, "route_elite_robustness_entry", route_static_parent_basket_entry),
        (evaluation, "ELITE_ROBUSTNESS_POLICY_VERSION", STATIC_PARENT_BASKET_POLICY_VERSION),
        (
            evaluation,
            "ELITE_ROBUSTNESS_EVALUATION_VERSION",
            STATIC_PARENT_BASKET_EVALUATION_VERSION,
        ),
    )
    prior = [(owner, name, getattr(owner, name)) for owner, name, _ in replacements]
    for owner, name, value in replacements:
        setattr(owner, name, value)
    try:
        yield
    finally:
        for owner, name, value in reversed(prior):
            setattr(owner, name, value)


def _static_parent_next_action(result: Mapping[str, Any]) -> str:
    if bool(result["family_tripwire"]["family_green"]) and int(
        result["combine_path_diagnostic_count"]
    ):
        return "FREEZE_STATIC_BASKET_SURVIVORS_AND_LAUNCH_96_START_CONFIRMATION"
    if bool(result["family_tripwire"]["family_green"]):
        return "PRESERVE_COMPLEMENTARITY_EVIDENCE_WITHOUT_PROMOTION"
    return "TOMBSTONE_EXACT_STATIC_SYNTHESIS_CLASS_AND_PIVOT_DISTINCT_MECHANISM"


def _static_parent_report(
    result: Mapping[str, Any], prereg: Mapping[str, Any]
) -> str:
    economics = result["account_policy_economics"]
    tripwire = result["family_tripwire"]
    wall = result["wall_clock_accounting"]
    budget = prereg["budget"]
    return "\n".join(
        [
            f"[HYDRA-V7] phase=4 step=0023 verdict={result['report_verdict']}",
            "gate=STATIC_PARENT_BASKET_TRIPWIRE "
            "preuve=reports/economic_evolution/static_parent_basket_0023/"
            "elite_robustness_result.json#PENDING_WRITE tests=targeted",
            f"budget_llm={budget['llm_actual_spend_usd']}/"
            f"{budget['llm_phase_max_usd']} budget_data={budget['actual_spend_usd']}/"
            f"{budget['hard_cap_usd']} N_trials="
            f"{prereg['multiplicity']['expected_global_N_trials_after_reservation']} "
            f"burned={prereg['reporting']['burned_window_count']}",
            "diff_validation="
            "hydra/economic_evolution/account_static_parent_basket_evaluation.py",
            f"CONTRE={prereg['CONTRE']}",
            f"prochaine_action={result['next_action']}",
            "",
            "# HYDRA — static parent basket synthesis 0023",
            "",
            f"- proposals / deep / starts: `{result['population']['proposal_count']}` / "
            f"`{result['policy_pair_evaluated_count']}` / "
            f"`{result['rolling_episode_start_count']}`",
            f"- normal + stressed real episodes: "
            f"`{result['normal_plus_stressed_real_episode_count']}`",
            "- normal/stressed policies with a pass: "
            f"`{economics['policies_passing_at_least_one_combine_episode']}` / "
            f"`{economics['stressed_policies_passing_at_least_one_combine_episode']}`",
            f"- best normal/stressed pass rate: "
            f"`{economics['combine_pass_probability']['maximum']:.4%}` / "
            f"`{economics['stressed_combine_pass_probability']['maximum']:.4%}`",
            f"- median normal/stressed progress: "
            f"`{economics['median_target_progress_distribution']['median']:.4%}` / "
            f"`{economics['stressed_target_progress_distribution']['median']:.4%}`",
            f"- real/control wins: `{tripwire['real_win_count']}` / "
            f"`{tripwire['matched_control_win_count']}`; "
            f"NULL_RATIO=`{tripwire['NULL_RATIO']}`",
            f"- research/admin: `{wall['research_percent']:.2f}%` / "
            f"`{wall['tests_and_reporting_percent']:.2f}%`",
            "- new data / Q4 / broker / orders: `0 / 0 / 0 / 0`",
            "",
            "## CONTRE",
            "",
            str(prereg["CONTRE"]),
            "",
        ]
    )


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise ValueError("project root not found")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


__all__ = [
    "STATIC_PARENT_BASKET_ENGINE_VERSION",
    "run_static_parent_basket_campaign",
]
