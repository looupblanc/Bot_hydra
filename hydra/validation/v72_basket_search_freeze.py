from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from hydra.account_policy.v72_static_basket import generate_static_basket_structures
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.2-pareto-crossfit-account-policy-0001-2026-07-13.json"
POLICY_SHA256 = "94f4ad89a2ae2ea347f1fce4a9cb4682690652429f34e42e72edf79e03da6677"
COMPONENT_BANK_PATH = "WORM/v7.2-component-bank-0001-2026-07-13.json"
COMPONENT_BANK_SHA256 = "36987e68a670345c890e9d7d2d060263a13f1e94928563f777dfdc572773ba4c"
SEARCH_MANIFEST_PATH = "WORM/v7.2-static-basket-search-0001-2026-07-13.json"
EXPECTED_GLOBAL_N_TRIALS_BEFORE = 263_902
RESERVATION_EVENT_ID = "v7_2_static_basket_crossfit_reservation_0001"


class V72BasketSearchFreezeError(RuntimeError):
    pass


def freeze_v72_basket_search(
    *,
    project_root: str | Path = ".",
    output_dir: str | Path = "reports/v7_2/crossfit_0001",
    worm_output_path: str | Path = SEARCH_MANIFEST_PATH,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if _sha256(root / POLICY_PATH) != POLICY_SHA256:
        raise V72BasketSearchFreezeError("V7.2 policy drift")
    if _sha256(root / COMPONENT_BANK_PATH) != COMPONENT_BANK_SHA256:
        raise V72BasketSearchFreezeError("V7.2 component bank drift")
    policy = json.loads((root / POLICY_PATH).read_text(encoding="utf-8"))
    bank = json.loads((root / COMPONENT_BANK_PATH).read_text(encoding="utf-8"))
    if bank.get("basket_results_observed") is not False:
        raise V72BasketSearchFreezeError("component bank is not pre-result")
    structures = generate_static_basket_structures(
        bank["primary_components"],
        minimum_size=int(policy["static_basket_search"]["component_count_minimum"]),
        maximum_size=int(policy["static_basket_search"]["component_count_maximum"]),
    )
    raw_trials = len(structures)
    profile_counts = {
        profile: sum(row.allocation_profile == profile for row in structures)
        for profile in ("UNIT_EQUAL", "TARGET_VELOCITY_TILT")
    }
    if raw_trials != 1_009 or profile_counts != {
        "UNIT_EQUAL": 550,
        "TARGET_VELOCITY_TILT": 459,
    }:
        raise V72BasketSearchFreezeError("static basket structure count drift")
    manifest: dict[str, Any] = {
        "schema": "hydra_v7_2_static_basket_search_manifest_v1",
        "search_id": "hydra_v7_2_static_basket_crossfit_0001",
        "policy_path": POLICY_PATH,
        "policy_sha256": POLICY_SHA256,
        "component_bank_path": COMPONENT_BANK_PATH,
        "component_bank_sha256": COMPONENT_BANK_SHA256,
        "recorded_before_any_basket_evaluation": True,
        "primary_component_count": len(bank["primary_components"]),
        "structure_count": raw_trials,
        "allocation_profile_counts": profile_counts,
        "structures": [row.to_dict() for row in structures],
        "design_only_rules": {
            "component_priority": "individual_STRESS_1_5X_design_net_desc_then_candidate_id_asc",
            "target_velocity_tilt_component": "highest_individual_STRESS_1_5X_design_net_among_frozen_TARGET_VELOCITY_components_then_candidate_id_asc",
            "held_out_information_used": False,
        },
        "rotation_selection": {
            "hard_filter": list(
                policy["static_basket_search"]["selection"]["hard_design_filter"]
            ),
            "pareto_objectives": list(
                policy["static_basket_search"]["selection"]["pareto_objectives"]
            ),
            "pareto_frontier_only": True,
            "frontier_order": [
                "stress_1_5x_account_net_desc",
                "median_maximum_target_progress_desc",
                "mll_breach_rate_asc",
                "consistency_pass_rate_desc",
                "conflict_rate_asc",
                "frozen_basket_hash_asc",
            ],
            "maximum_selected_per_rotation": 3,
            "retuning_after_held_out_result": False,
        },
        "multiplicity": {
            "reservation_event_id": RESERVATION_EVENT_ID,
            "raw_global_N_trials_before": EXPECTED_GLOBAL_N_TRIALS_BEFORE,
            "delta_raw_trials": raw_trials,
            "raw_global_N_trials_after": EXPECTED_GLOBAL_N_TRIALS_BEFORE
            + raw_trials,
            "campaign_inflation_factor": 1.5,
            "campaign_effective_N_trials": raw_trials * 1.5,
        },
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The search contains 1,009 preregistered basket-allocation structures "
            "but only four short independent blocks; cross-fitting controls direct "
            "selection leakage, not the low precision of account-level estimates."
        ),
        "prochaine_action": "commit_manifest_then_append_multiplicity_reservation_then_evaluate_design_blocks",
    }
    manifest["search_manifest_hash"] = _stable_hash(manifest)
    worm_path = Path(worm_output_path)
    if not worm_path.is_absolute():
        worm_path = root / worm_path
    _write_once_json(worm_path, manifest)
    result = {
        **manifest,
        "search_manifest_path": str(worm_path.relative_to(root)),
        "search_manifest_sha256": _sha256(worm_path),
    }
    return _write_report(result, root, Path(output_dir))


def _write_report(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_basket_search_freeze_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v72_basket_search_freeze_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Static basket search freeze",
            "",
            "[HYDRA-V7] phase=4 step=184 verdict=GREEN",
            f"gate=V72_BASKET_SEARCH_FREEZE preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=1009_structures_frozen_before_results",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS_BEFORE} burned=1",
            "diff_validation=hydra/account_policy/basket.py,hydra/account_policy/v72_static_basket.py,hydra/validation/v72_basket_search_freeze.py CONTRE=quatre_blocs_independants_courts",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Composants primaires: `{result['primary_component_count']}`",
            f"- Structures panier/allocation: `{result['structure_count']}`",
            f"- UNIT_EQUAL: `{result['allocation_profile_counts']['UNIT_EQUAL']}`",
            f"- TARGET_VELOCITY_TILT: `{result['allocation_profile_counts']['TARGET_VELOCITY_TILT']}`",
            f"- N_trials effectif campagne: `{result['multiplicity']['campaign_effective_N_trials']}`",
            "- Résultats panier lus: `0`",
            "- Achats data/Q4/ordres: `0/0/0`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    return {
        **result,
        "result_path": str(result_path),
        "result_sha256": result_hash,
        "report_path": str(report_path),
    }


def _write_once_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != serialized:
            raise V72BasketSearchFreezeError("existing WORM basket search manifest drift")
        return
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["V72BasketSearchFreezeError", "freeze_v72_basket_search"]
