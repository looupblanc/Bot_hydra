from __future__ import annotations

import json
import os
import traceback
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


class UnknownExperimentType(RuntimeError):
    pass


def experiment_worker_entry(experiment: dict[str, Any], result_path: str) -> None:
    """Subprocess entry point: execute research and publish no mission-DB writes."""
    if hasattr(os, "setsid"):
        os.setsid()
    target = Path(result_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        output_root_value = experiment.get("worker_output_root")
        output_root = Path(str(output_root_value)) if output_root_value else None
        result = run_experiment(experiment, output_root=output_root)
        envelope = {
            "ok": True,
            "experiment_id": experiment.get("experiment_id"),
            "specification_hash": experiment.get("specification_hash"),
            "result": result,
        }
    except Exception as exc:
        envelope = {
            "ok": False,
            "experiment_id": experiment.get("experiment_id"),
            "specification_hash": experiment.get("specification_hash"),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(limit=40),
        }
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(envelope, sort_keys=True, default=str), encoding="utf-8")
    os.replace(temporary, target)


def run_experiment(experiment: dict[str, Any], *, output_root: Path | None = None) -> dict[str, Any]:
    """Run a closed, auditable research handler without touching mission SQLite."""
    experiment_type = str(experiment.get("experiment_type") or "")
    experiment_id = str(experiment["experiment_id"])
    root = output_root or project_path("reports", "mission_experiments")
    output_dir = Path(root) / experiment_id
    if experiment_type == "calibration_affected_atom_retest_design":
        from hydra.mission.calibration_retest import run_calibration_affected_atom_retest_design

        return run_calibration_affected_atom_retest_design(
            output_dir,
            historical_report_path=Path(
                experiment.get(
                    "historical_report_path",
                    project_path(
                        "reports",
                        "edge_atom_lab",
                        "edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md",
                    ),
                )
            ),
            historical_preregistration_path=Path(
                experiment.get(
                    "historical_preregistration_path",
                    project_path(
                        "reports",
                        "edge_atom_lab",
                        "edge_atom_preregistration_20260710T101052+0000_edge_atom_discovery_replication_v1_final.json",
                    ),
                )
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "calibration_affected_atom_retest_execution":
        from hydra.mission.calibration_retest_execution import run_calibration_affected_atom_retest_execution

        return run_calibration_affected_atom_retest_execution(
            output_dir,
            design_preregistration_path=Path(str(experiment["design_preregistration_path"])),
            design_path=Path(str(experiment["design_path"])),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "post_calibration_retest_research_design":
        from hydra.mission.post_retest_research import run_post_calibration_retest_research_design

        return run_post_calibration_retest_research_design(
            output_dir,
            source_execution_result_path=Path(str(experiment["source_execution_result_path"])),
            source_execution_result_hash=str(experiment["source_execution_result_hash"]),
            source_execution_experiment_id=str(experiment["source_execution_experiment_id"]),
            source_execution_specification_hash=str(experiment["source_execution_specification_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "validator_integrity_repair_pilot":
        from hydra.validation.retest_integrity_repair import run_validator_integrity_repair_pilot

        return run_validator_integrity_repair_pilot(
            output_dir,
            source_execution_result_path=Path(str(experiment["source_execution_result_path"])),
            source_execution_result_hash=str(experiment["source_execution_result_hash"]),
            source_execution_experiment_id=str(experiment["source_execution_experiment_id"]),
            source_execution_specification_hash=str(experiment["source_execution_specification_hash"]),
            post_retest_design_path=Path(str(experiment["post_retest_design_path"])),
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_hash=str(experiment["engineering_task_hash"]),
            selected_post_retest_branch=str(experiment["selected_post_retest_branch"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "contract_map_date_aware_repair":
        from hydra.validation.contract_map_date_repair import run_contract_map_date_aware_repair

        return run_contract_map_date_aware_repair(
            output_dir,
            integrity_pilot_result_path=Path(str(experiment["integrity_pilot_result_path"])),
            integrity_pilot_result_hash=str(experiment["integrity_pilot_result_hash"]),
            frozen_contract_map_path=Path(str(experiment["frozen_contract_map_path"])),
            frozen_contract_map_sha256=str(experiment["frozen_contract_map_sha256"]),
            definition_dbn_path=Path(str(experiment["definition_dbn_path"])),
            definition_dbn_sha256=str(experiment["definition_dbn_sha256"]),
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "calibration_affected_atom_retest_v3_design":
        from hydra.mission.calibration_retest_v3 import (
            run_calibration_affected_atom_retest_v3_design,
        )

        return run_calibration_affected_atom_retest_v3_design(
            output_dir,
            contract_map_repair_result_path=Path(
                str(experiment["contract_map_repair_result_path"])
            ),
            contract_map_repair_result_hash=str(experiment["contract_map_repair_result_hash"]),
            contract_map_repair_file_sha256=str(experiment["contract_map_repair_file_sha256"]),
            invalid_v2_execution_result_path=Path(
                str(experiment["invalid_v2_execution_result_path"])
            ),
            invalid_v2_execution_result_hash=str(
                experiment["invalid_v2_execution_result_hash"]
            ),
            invalid_v2_execution_file_sha256=str(
                experiment["invalid_v2_execution_file_sha256"]
            ),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "calibration_affected_atom_retest_v3_execution":
        from hydra.mission.calibration_retest_execution import (
            run_calibration_affected_atom_retest_execution,
        )
        from hydra.mission.calibration_retest_v3 import DESIGN_VERSION, REQUIRED_MAP_TYPE

        return run_calibration_affected_atom_retest_execution(
            output_dir,
            design_preregistration_path=Path(str(experiment["design_preregistration_path"])),
            design_path=Path(str(experiment["design_path"])),
            code_commit=str(experiment.get("code_commit") or "unknown"),
            contract_map_path=Path(str(experiment["repaired_map_path"])),
            required_contract_map_type=REQUIRED_MAP_TYPE,
            expected_design_version=DESIGN_VERSION,
            execution_version="calibration_affected_atom_retest_execution_v3",
            output_stem="calibration_affected_atom_retest_v3_execution",
            data_access_reason=(
                "fresh calibration-affected atom v3 retest on repaired date-aware map; Q4 excluded"
            ),
        )
    if experiment_type == "path_geometry_candidate_audit":
        from hydra.research.path_geometry_candidate_audit import (
            run_path_geometry_candidate_audit,
        )

        return run_path_geometry_candidate_audit(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "metal_energy_session_transition_pilot":
        from hydra.research.metal_energy_session_transition import run_metal_energy_session_transition_pilot
        return run_metal_energy_session_transition_pilot(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "cross_market_lead_lag_pilot":
        from hydra.research.cross_market_lead_lag import run_cross_market_lead_lag_pilot
        return run_cross_market_lead_lag_pilot(output_dir, engineering_task_path=Path(str(experiment["engineering_task_path"])), engineering_task_sha256=str(experiment["engineering_task_sha256"]), repaired_map_path=Path(str(experiment["repaired_map_path"])), repaired_map_sha256=str(experiment["repaired_map_sha256"]), repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]), code_commit=str(experiment.get("code_commit") or "unknown"))
    if experiment_type == "volatility_transition_pilot":
        from hydra.research.volatility_transition import run_volatility_transition_pilot
        return run_volatility_transition_pilot(output_dir, engineering_task_path=Path(str(experiment["engineering_task_path"])), engineering_task_sha256=str(experiment["engineering_task_sha256"]), repaired_map_path=Path(str(experiment["repaired_map_path"])), repaired_map_sha256=str(experiment["repaired_map_sha256"]), repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]), code_commit=str(experiment.get("code_commit") or "unknown"))
    if experiment_type == "foundry_bootstrap":
        from hydra.foundry.bootstrap import run_foundry_bootstrap

        return run_foundry_bootstrap(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            tournament_preregistration_path=Path(
                str(experiment["tournament_preregistration_path"])
            ),
            tournament_preregistration_sha256=str(
                experiment["tournament_preregistration_sha256"]
            ),
            tournament_report_path=Path(str(experiment["tournament_report_path"])),
            tournament_report_sha256=str(experiment["tournament_report_sha256"]),
            tournament_checkpoint_path=Path(str(experiment["tournament_checkpoint_path"])),
            tournament_checkpoint_sha256=str(experiment["tournament_checkpoint_sha256"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "equity_open_gap_reversal_pilot":
        from hydra.research.equity_open_gap_reversal import (
            run_equity_open_gap_reversal_pilot,
        )

        return run_equity_open_gap_reversal_pilot(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "equity_open_gap_continuation_pilot":
        from hydra.research.equity_open_gap_continuation import (
            run_equity_open_gap_continuation_pilot,
        )

        return run_equity_open_gap_continuation_pilot(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            source_reversal_result_path=Path(
                str(experiment["source_reversal_result_path"])
            ),
            source_reversal_result_sha256=str(
                experiment["source_reversal_result_sha256"]
            ),
            source_reversal_result_hash=str(experiment["source_reversal_result_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "q4_candidate_freeze":
        from hydra.foundry.q4_freeze import run_q4_candidate_freeze

        return run_q4_candidate_freeze(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            source_continuation_result_path=Path(
                str(experiment["source_continuation_result_path"])
            ),
            source_continuation_result_sha256=str(
                experiment["source_continuation_result_sha256"]
            ),
            source_continuation_result_hash=str(
                experiment["source_continuation_result_hash"]
            ),
            source_trade_ledger_path=Path(str(experiment["source_trade_ledger_path"])),
            source_trade_ledger_sha256=str(experiment["source_trade_ledger_sha256"]),
            source_shadow_configuration_path=Path(
                str(experiment["source_shadow_configuration_path"])
            ),
            source_shadow_configuration_sha256=str(
                experiment["source_shadow_configuration_sha256"]
            ),
            source_shadow_configuration_hash=str(
                experiment["source_shadow_configuration_hash"]
            ),
            candidate_id=str(experiment["candidate_id"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
            governance_baseline_commit=str(experiment["governance_baseline_commit"]),
            remaining_databento_budget_usd=float(
                experiment["remaining_databento_budget_usd"]
            ),
        )
    if experiment_type == "opening_direction_hazard_pilot":
        from hydra.research.opening_direction_hazard import (
            run_opening_direction_hazard_pilot,
        )

        return run_opening_direction_hazard_pilot(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "cross_ecology_opening_acceptance_pilot":
        from hydra.research.cross_ecology_opening_acceptance import (
            run_cross_ecology_opening_acceptance_pilot,
        )

        return run_cross_ecology_opening_acceptance_pilot(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "mtf_session_trend_confirmation_pilot":
        from hydra.research.mtf_session_trend_confirmation import (
            run_mtf_session_trend_confirmation_pilot,
        )

        return run_mtf_session_trend_confirmation_pilot(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "rty_ym_relative_value_pilot":
        from hydra.research.rty_ym_relative_value import run_rty_ym_relative_value_pilot

        return run_rty_ym_relative_value_pilot(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "ym_shared_risk_off_overlay":
        from hydra.research.ym_shared_risk_off_overlay import (
            run_ym_shared_risk_off_overlay,
        )

        return run_ym_shared_risk_off_overlay(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            source_parent_result_path=Path(str(experiment["source_parent_result_path"])),
            source_parent_result_sha256=str(experiment["source_parent_result_sha256"]),
            source_parent_result_hash=str(experiment["source_parent_result_hash"]),
            source_parent_trade_ledger_path=Path(
                str(experiment["source_parent_trade_ledger_path"])
            ),
            source_parent_trade_ledger_sha256=str(
                experiment["source_parent_trade_ledger_sha256"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "qd_economic_tournament":
        from hydra.research.qd_economic_tournament import (
            run_qd_economic_tournament,
        )

        return run_qd_economic_tournament(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            selector_task_path=Path(str(experiment["selector_task_path"])),
            selector_task_sha256=str(experiment["selector_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "ym_open_gap_strict_promotion":
        from hydra.research.ym_strict_promotion import run_ym_strict_promotion

        return run_ym_strict_promotion(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            source_parent_result_path=Path(str(experiment["source_parent_result_path"])),
            source_parent_result_sha256=str(experiment["source_parent_result_sha256"]),
            source_parent_result_hash=str(experiment["source_parent_result_hash"]),
            source_parent_trade_ledger_path=Path(
                str(experiment["source_parent_trade_ledger_path"])
            ),
            source_parent_trade_ledger_sha256=str(
                experiment["source_parent_trade_ledger_sha256"]
            ),
            source_freeze_manifest_path=Path(
                str(experiment["source_freeze_manifest_path"])
            ),
            source_freeze_manifest_sha256=str(
                experiment["source_freeze_manifest_sha256"]
            ),
            source_freeze_manifest_hash=str(experiment["source_freeze_manifest_hash"]),
            source_shadow_configuration_path=Path(
                str(experiment["source_shadow_configuration_path"])
            ),
            source_shadow_configuration_sha256=str(
                experiment["source_shadow_configuration_sha256"]
            ),
            source_shadow_configuration_hash=str(
                experiment["source_shadow_configuration_hash"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "ym_immutable_shadow_activation":
        from hydra.shadow.activation import run_ym_shadow_activation

        return run_ym_shadow_activation(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            strict_result_path=Path(str(experiment["strict_result_path"])),
            strict_result_sha256=str(experiment["strict_result_sha256"]),
            strict_result_hash=str(experiment["strict_result_hash"]),
            shadow_configuration_path=Path(
                str(experiment["shadow_configuration_path"])
            ),
            shadow_configuration_sha256=str(
                experiment["shadow_configuration_sha256"]
            ),
            shadow_configuration_hash=str(experiment["shadow_configuration_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "accelerated_context_tournament":
        from hydra.research.accelerated_context_tournament import (
            run_accelerated_context_tournament,
        )

        return run_accelerated_context_tournament(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            selector_task_path=Path(str(experiment["selector_task_path"])),
            selector_task_sha256=str(experiment["selector_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "selection_null_power_calibration":
        from hydra.calibration.selection_null_power import (
            run_selection_null_power_calibration,
        )

        return run_selection_null_power_calibration(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "selection_null_policy_repair":
        from hydra.calibration.selection_null_policy_repair import (
            run_selection_null_policy_repair,
        )

        return run_selection_null_policy_repair(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            source_calibration_result_path=Path(
                str(experiment["source_calibration_result_path"])
            ),
            source_calibration_result_sha256=str(
                experiment["source_calibration_result_sha256"]
            ),
            source_calibration_result_hash=str(
                experiment["source_calibration_result_hash"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "single_primary_alpha_calibration":
        from hydra.calibration.single_primary_alpha import (
            run_single_primary_alpha_calibration,
        )

        return run_single_primary_alpha_calibration(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            source_policy_repair_result_path=Path(
                str(experiment["source_policy_repair_result_path"])
            ),
            source_policy_repair_result_sha256=str(
                experiment["source_policy_repair_result_sha256"]
            ),
            source_policy_repair_result_hash=str(
                experiment["source_policy_repair_result_hash"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "single_primary_context_tournament":
        from hydra.research.single_primary_context_tournament import (
            run_single_primary_context_tournament,
        )

        return run_single_primary_context_tournament(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            selector_task_path=Path(str(experiment["selector_task_path"])),
            selector_task_sha256=str(experiment["selector_task_sha256"]),
            calibrated_policy_result_path=Path(
                str(experiment["calibrated_policy_result_path"])
            ),
            calibrated_policy_result_sha256=str(
                experiment["calibrated_policy_result_sha256"]
            ),
            calibrated_policy_result_hash=str(
                experiment["calibrated_policy_result_hash"]
            ),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "counterfactual_hazard_primary":
        from hydra.research.counterfactual_hazard_primary import (
            run_counterfactual_hazard_primary,
        )

        return run_counterfactual_hazard_primary(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "barrier_hazard_primary":
        from hydra.research.barrier_hazard_primary import (
            run_barrier_hazard_primary,
        )

        return run_barrier_hazard_primary(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            repaired_map_path=Path(str(experiment["repaired_map_path"])),
            repaired_map_sha256=str(experiment["repaired_map_sha256"]),
            repaired_roll_map_hash=str(experiment["repaired_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "immutable_shadow_activation":
        from hydra.shadow.activation import run_immutable_shadow_activation

        return run_immutable_shadow_activation(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            source_result_path=Path(str(experiment["source_result_path"])),
            source_result_sha256=str(experiment["source_result_sha256"]),
            source_result_hash=str(experiment["source_result_hash"]),
            candidate_id=str(experiment["candidate_id"]),
            shadow_configuration_path=Path(
                str(experiment["shadow_configuration_path"])
            ),
            shadow_configuration_sha256=str(
                experiment["shadow_configuration_sha256"]
            ),
            shadow_configuration_hash=str(
                experiment["shadow_configuration_hash"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    raise UnknownExperimentType(f"No approved handler for experiment type {experiment_type!r}.")
