#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.v7_boosted_tournament_specs import (
    GRAMMAR_VERSION,
    TOURNAMENT_ID,
    bounded_basket_structures,
    candidate_specs,
    mechanism_families,
    stable_hash,
)


OUTPUT_PATH = "WORM/v7-boosted-dual-track-tournament-0001-2026-07-13.json"
COMPONENT_BANK_PATH = "WORM/v7.2-component-bank-0001-2026-07-13.json"
UNDERPOWERED_SELECTION_PATH = (
    "reports/v7_1/combine_research_0001/"
    "v71_underpowered_combine_selection_manifest.json"
)
G10_POWER_PATH = (
    "reports/v7_2/discovery_0010/"
    "v72_flow_impact_relaxation_power_audit_result.json"
)
G10_NEW_IDS = (
    "v72g10_flow_impact_relaxation_extend_then_fail_reversal_r4_h30",
    "v72g10_flow_impact_relaxation_quiet_passive_extension_continuation_r4_h60",
)


def build_preregistration(project_root: str | Path = ".") -> dict[str, Any]:
    root = Path(project_root).resolve()
    source_commit = _git_head(root)
    component_bank = _load_json(root / COMPONENT_BANK_PATH)
    selection = _load_json(root / UNDERPOWERED_SELECTION_PATH)
    g10_power = _load_json(root / G10_POWER_PATH)
    primary = list(component_bank["primary_components"])
    primary_ids = [str(row["candidate_id"]) for row in primary]
    if len(primary_ids) != 11 or len(set(primary_ids)) != 11:
        raise RuntimeError("frozen V7.2 primary component bank drift")
    power_by_id = {
        str(row["candidate_id"]): row for row in g10_power["candidate_results"]
    }
    if any(power_by_id[candidate_id]["status"] != "PROMISING_UNDERPOWERED" for candidate_id in G10_NEW_IDS):
        raise RuntimeError("G10 underpowered component status drift")
    component_ids = tuple(primary_ids) + G10_NEW_IDS
    role_map = {str(row["candidate_id"]): str(row["role"]) for row in primary}
    role_map[G10_NEW_IDS[0]] = "MLL_PROTECTION"
    role_map[G10_NEW_IDS[1]] = "TARGET_VELOCITY"
    baskets = bounded_basket_structures(
        component_ids,
        new_component_ids=G10_NEW_IDS,
        role_map=role_map,
    )
    existing_diagnostics = tuple(
        str(row["candidate_id"]) for row in selection["selected_candidates"]
    )
    if len(existing_diagnostics) != 5 or len(set(existing_diagnostics)) != 5:
        raise RuntimeError("existing underpowered diagnostic selection drift")
    rolling_candidates = existing_diagnostics + G10_NEW_IDS
    specs = candidate_specs()
    families = mechanism_families()
    if len(specs) != 256 or len(families) != 8:
        raise RuntimeError("boosted mechanism population drift")
    family_rows = []
    for family in families:
        family_specs = [row for row in specs if row.family_id == family.family_id]
        family_rows.append(
            {
                "family_id": family.family_id,
                "economic_hypothesis": family.economic_hypothesis,
                "payer": family.payer,
                "persistence_rationale": family.persistence_rationale,
                "cemetery_distance": family.cemetery_distance,
                "candidate_count": len(family_specs),
                "structure_count": len(family.motifs),
                "bounded_alternatives_per_structure": 4,
                "candidate_ids": [row.candidate_id for row in family_specs],
                "candidate_specification_hashes": {
                    row.candidate_id: row.specification_hash for row in family_specs
                },
            }
        )
    payload: dict[str, Any] = {
        "schema": "hydra_v7_boosted_dual_track_preregistration_v1",
        "tournament_id": TOURNAMENT_ID,
        "grammar_version": GRAMMAR_VERSION,
        "principal_authorization": "HYDRA_BOOSTED_DUAL_TRACK_RESEARCH_MISSION",
        "source_commit": source_commit,
        "created_date_utc": "2026-07-13",
        "frozen_before_new_feature_signal_pnl_or_account_results": True,
        "contract_constraints": {
            "fitness": "net_account_expectancy_after_costs_with_multiplicity_deflation",
            "combine_pass_probability_is_fitness": False,
            "combine_is_downstream_diagnostic": True,
            "no_live_trading": True,
            "broker_connections": 0,
            "outbound_orders": 0,
            "new_data_purchase_authorized": False,
            "q4_access_authorized": False,
            "burned_windows_reusable": False,
            "parameter_feedback_from_forward_or_holdout": False,
        },
        "allocation": {
            "MECHANISM_TOURNAMENT": 0.45,
            "COMBINE_ACCOUNT_SYNTHESIS": 0.45,
            "FORWARD_PROMOTION_INFRASTRUCTURE": 0.10,
            "single_controller": True,
            "single_registry_writer": True,
            "single_mission_database": True,
        },
        "lane_A_mechanism_tournament": {
            "family_count": 8,
            "candidate_count": 256,
            "families": family_rows,
            "all_candidate_specs": [row.to_dict() for row in specs],
            "features": {
                "data_role": "D1_DEVELOPMENT_ONLY",
                "source": "existing_GLBX_MDP3_ES_trades_and_derived_completed_minute_stores",
                "availability": "completed_minute_features_shifted_one_minute",
                "entry": "next_completed_minute_open",
                "contracts": ["ESU3", "ESU4"],
                "session": "RTH",
                "threshold_quantiles": [0.10, 0.25, 0.75, 0.90],
                "rolling_history_minutes": [20, 60],
                "quantile_minimum_periods": 20,
                "threshold_tuning_after_results": False,
            },
            "funnel": {
                "stage0": {
                    "required": [
                        "no_lookahead",
                        "explicit_contract",
                        "deterministic_signal",
                        "feature_availability",
                        "minimum_intended_sample",
                        "no_exact_or_behavioral_duplicate",
                        "no_exact_cemetery_equivalent",
                    ]
                },
                "stage1": {
                    "cost_profile": "STRESS_1_5X",
                    "minimum_nonoverlapping_events": 40,
                    "minimum_positive_early_folds": 2,
                    "early_fold_count": 3,
                    "pooled_expectancy_min_exclusive_usd": 0.0,
                    "maximum_single_day_absolute_pnl_share": 0.35,
                },
                "stage2_walk_forward": {
                    "rolling_origin_fold_count": 4,
                    "fixed_structure_direction_horizon": True,
                    "cost_profile": "STRESS_1_5X",
                    "purge": "candidate_holding_horizon",
                    "embargo_days": 5,
                    "minimum_retained_events": 30,
                    "minimum_positive_folds": 2,
                    "pooled_expectancy_min_exclusive_usd": 0.0,
                    "post_result_tuning": False,
                },
                "stage3_family_tripwire": {
                    "controls": [
                        "DAILY_BLOCK_SHUFFLE",
                        "VOLATILITY_MATCHED_RANDOM_WALK",
                        "YEAR_BLOCK_PERMUTATION",
                    ],
                    "pipeline_identical": True,
                    "real_episode_denominator_per_family": 640,
                    "pooled_null_episode_denominator_per_family": 1920,
                    "NULL_RATIO_green_maximum_exclusive": 0.8,
                    "exact_one_sided_binomial_test": True,
                    "family_tripwire_failure_action": "TOMBSTONE_EXACT_FAMILY_GRAMMAR_NO_PARAMETER_RESCUE",
                },
                "stage4_fragility": {
                    "cost_stress": "STRESS_2X",
                    "best_event_removal": True,
                    "block_concentration": True,
                    "effective_sample_count": True,
                    "SIM_EXPLOIT_when_edge_disappears_at_2X": True,
                },
                "stage5_power": {
                    "final_confirmation_power_minimum": 0.80,
                    "candidate_specific": True,
                    "underpowered_diagnostic_allowed": True,
                    "DSR_and_BH_only_after_positive_WF_green_tripwire_and_power": True,
                    "BH_FDR": 0.10,
                },
                "stage6": "bounded_Rolling_Combine_diagnostic_not_fitness",
                "stage7": "cross_fitted_static_account_synthesis",
            },
        },
        "lane_B_conversion": {
            "rolling_combine": {
                "candidate_ids": list(rolling_candidates),
                "candidate_count": 7,
                "status": "PROMISING_UNDERPOWERED_COMBINE_RESEARCH",
                "validated_claim_allowed": False,
                "episode_starts": 24,
                "independent_temporal_blocks": 4,
                "identical_starts_for_comparisons": True,
                "reporting_horizons_trading_days": [20, 40, 60, 90],
                "full_available_horizon": True,
                "official_time_limit_assumed": False,
                "censored_episode_statuses": [
                    "TARGET_REACHED",
                    "MLL_BREACHED",
                    "DATA_CENSORED",
                    "OPERATIONAL_HORIZON_NOT_REACHED",
                    "HARD_RULE_FAILURE",
                ],
            },
            "component_bank": {
                "source_component_bank_path": COMPONENT_BANK_PATH,
                "source_component_bank_sha256": _sha256(root / COMPONENT_BANK_PATH),
                "existing_primary_count": 11,
                "new_component_ids": list(G10_NEW_IDS),
                "component_ids": list(component_ids),
                "component_roles": role_map,
                "underpowered_components_validated": False,
            },
            "static_baskets": {
                "structure_count": 320,
                "basket_size_minimum": 2,
                "basket_size_maximum": 4,
                "must_include_new_G10_component": True,
                "allocation_profiles": ["UNIT_EQUAL", "TARGET_VELOCITY_TILT"],
                "maximum_simultaneous_positions": 2,
                "continuous_weight_optimization": False,
                "structures": list(baskets),
                "cross_fit": {
                    "method": "leave_one_block_out",
                    "blocks": [
                        {"block_id": "D1_2023_A", "start_date": "2023-08-02", "duration_sessions": 10},
                        {"block_id": "D1_2023_B", "start_date": "2023-08-16", "duration_sessions": 10},
                        {"block_id": "D1_2024_A", "start_date": "2024-08-02", "duration_sessions": 10},
                        {"block_id": "D1_2024_B", "start_date": "2024-08-16", "duration_sessions": 10},
                    ],
                    "design_blocks": 3,
                    "held_out_blocks": 1,
                    "rotations": 4,
                    "maximum_selected_baskets_per_rotation": 3,
                    "retune_after_held_out": False,
                },
                "research_survivor_gate": {
                    "normal_account_net_positive": True,
                    "stressed_account_net_defensible": True,
                    "minimum_useful_unseen_blocks": 2,
                    "maximum_MLL_breach_rate": 0.20,
                    "maximum_MLL_deterioration_vs_safer_parent": 0.10,
                    "must_improve_a_parent": [
                        "pass_probability",
                        "target_progress",
                        "target_velocity",
                        "MLL_survival",
                    ],
                    "parent_dominance_forbidden": True,
                    "single_component_or_block_domination_forbidden": True,
                },
            },
        },
        "lane_C_forward": {
            "append_only_feed_priority": True,
            "fresh_post_freeze_bars_required_for_active_status": True,
            "weekend_bar_fabrication": False,
            "broker_or_order_path": False,
            "data_purchase_in_this_round": False,
            "paper_shadow_ready_from_development_only": False,
        },
        "multiplicity_reservation": {
            "mechanism_structural_trials": 256,
            "mechanism_real_and_null_world_trials": 1024,
            "basket_structure_trials": 320,
            "rolling_diagnostic_trials": 7,
            "delta_raw_trials": 1607,
            "campaign_inflation_factor": 1.5,
            "campaign_effective_N_trials": 2410.5,
            "reserve_before_new_feature_signal_pnl_or_account_results": True,
        },
        "source_artifacts": {
            "component_bank": {
                "path": COMPONENT_BANK_PATH,
                "sha256": _sha256(root / COMPONENT_BANK_PATH),
            },
            "underpowered_selection": {
                "path": UNDERPOWERED_SELECTION_PATH,
                "sha256": _sha256(root / UNDERPOWERED_SELECTION_PATH),
            },
            "g10_power": {
                "path": G10_POWER_PATH,
                "sha256": _sha256(root / G10_POWER_PATH),
            },
        },
        "CONTRE": (
            "All eight families and 320 baskets still use only 43 D1 sessions in "
            "two August blocks. Cross-fitting and family nulls control direct "
            "selection leakage but cannot create independent confirmation power."
        ),
        "prochaine_action": (
            "commit_WORM_then_reserve_1607_trials_before_any_new_result"
        ),
    }
    payload["preregistration_hash"] = stable_hash(payload)
    return payload


def write_preregistration(
    payload: dict[str, Any], project_root: str | Path = "."
) -> Path:
    root = Path(project_root).resolve()
    path = root / OUTPUT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != raw:
            raise RuntimeError(f"WORM preregistration drift: {path}")
        return path
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    return path


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON object required: {path}")
    return payload


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args()
    payload = build_preregistration(args.project_root)
    path = write_preregistration(payload, args.project_root)
    print(
        json.dumps(
            {
                "path": str(path),
                "sha256": _sha256(path),
                "family_count": payload["lane_A_mechanism_tournament"]["family_count"],
                "candidate_count": payload["lane_A_mechanism_tournament"]["candidate_count"],
                "basket_count": payload["lane_B_conversion"]["static_baskets"]["structure_count"],
                "rolling_candidate_count": payload["lane_B_conversion"]["rolling_combine"]["candidate_count"],
                "new_data_purchase_count": 0,
                "q4_access_count_delta": 0,
                "outbound_order_count": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
