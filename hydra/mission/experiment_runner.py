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
    if experiment_type == "energy_metals_barrier_primary":
        from hydra.research.energy_metals_barrier_primary import (
            run_energy_metals_barrier_primary,
        )

        return run_energy_metals_barrier_primary(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            energy_data_path=Path(str(experiment["energy_data_path"])),
            energy_data_sha256=str(experiment["energy_data_sha256"]),
            energy_map_path=Path(str(experiment["energy_map_path"])),
            energy_map_sha256=str(experiment["energy_map_sha256"]),
            energy_roll_map_hash=str(experiment["energy_roll_map_hash"]),
            metals_data_path=Path(str(experiment["metals_data_path"])),
            metals_data_sha256=str(experiment["metals_data_sha256"]),
            metals_map_path=Path(str(experiment["metals_map_path"])),
            metals_map_sha256=str(experiment["metals_map_sha256"]),
            metals_roll_map_hash=str(experiment["metals_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "energy_metals_session_geometry_primary":
        from hydra.research.energy_metals_session_geometry_primary import (
            run_energy_metals_session_geometry_primary,
        )

        return run_energy_metals_session_geometry_primary(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            energy_data_path=Path(str(experiment["energy_data_path"])),
            energy_data_sha256=str(experiment["energy_data_sha256"]),
            energy_map_path=Path(str(experiment["energy_map_path"])),
            energy_map_sha256=str(experiment["energy_map_sha256"]),
            energy_roll_map_hash=str(experiment["energy_roll_map_hash"]),
            metals_data_path=Path(str(experiment["metals_data_path"])),
            metals_data_sha256=str(experiment["metals_data_sha256"]),
            metals_map_path=Path(str(experiment["metals_map_path"])),
            metals_map_sha256=str(experiment["metals_map_sha256"]),
            metals_roll_map_hash=str(experiment["metals_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "session_geometry_micro_execution_repair":
        from hydra.research.energy_metals_session_execution_repair import (
            run_session_geometry_micro_execution_repair,
        )

        return run_session_geometry_micro_execution_repair(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            source_result_path=Path(str(experiment["source_result_path"])),
            source_result_sha256=str(experiment["source_result_sha256"]),
            source_result_hash=str(experiment["source_result_hash"]),
            source_manifest_path=Path(str(experiment["source_manifest_path"])),
            source_manifest_sha256=str(experiment["source_manifest_sha256"]),
            source_manifest_hash=str(experiment["source_manifest_hash"]),
            source_trade_ledger_path=Path(
                str(experiment["source_trade_ledger_path"])
            ),
            source_trade_ledger_sha256=str(
                experiment["source_trade_ledger_sha256"]
            ),
            energy_data_path=Path(str(experiment["energy_data_path"])),
            energy_data_sha256=str(experiment["energy_data_sha256"]),
            energy_map_path=Path(str(experiment["energy_map_path"])),
            energy_map_sha256=str(experiment["energy_map_sha256"]),
            energy_roll_map_hash=str(experiment["energy_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "session_geometry_micro_shadow_activation":
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
            code_surface_paths=(
                [Path(str(value)) for value in experiment["code_surface_paths"]]
                if experiment.get("code_surface_paths")
                else None
            ),
        )
    if experiment_type == "gc_session_geometry_fresh_primary":
        from hydra.research.gc_session_geometry_fresh_primary import (
            run_gc_session_geometry_fresh_primary,
        )

        return run_gc_session_geometry_fresh_primary(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            source_preregistration_path=Path(
                str(experiment["source_preregistration_path"])
            ),
            source_preregistration_sha256=str(
                experiment["source_preregistration_sha256"]
            ),
            source_freeze_path=Path(str(experiment["source_freeze_path"])),
            source_freeze_sha256=str(experiment["source_freeze_sha256"]),
            metals_data_path=Path(str(experiment["metals_data_path"])),
            metals_data_sha256=str(experiment["metals_data_sha256"]),
            metals_map_path=Path(str(experiment["metals_map_path"])),
            metals_map_sha256=str(experiment["metals_map_sha256"]),
            metals_roll_map_hash=str(experiment["metals_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "cross_asset_daily_horizon_primary":
        from hydra.research.cross_asset_daily_horizon_primary import (
            run_cross_asset_daily_horizon_primary,
        )

        return run_cross_asset_daily_horizon_primary(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            core_data_path=Path(str(experiment["core_data_path"])),
            core_data_sha256=str(experiment["core_data_sha256"]),
            core_map_path=Path(str(experiment["core_map_path"])),
            core_map_sha256=str(experiment["core_map_sha256"]),
            core_roll_map_hash=str(experiment["core_roll_map_hash"]),
            metals_data_path=Path(str(experiment["metals_data_path"])),
            metals_data_sha256=str(experiment["metals_data_sha256"]),
            metals_map_path=Path(str(experiment["metals_map_path"])),
            metals_map_sha256=str(experiment["metals_map_sha256"]),
            metals_roll_map_hash=str(experiment["metals_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "cross_asset_daily_shadow_activation":
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
    if experiment_type == "shadow_shared_account_baskets":
        from hydra.portfolio.shadow_shared_account import (
            run_shadow_shared_account_baskets,
        )

        return run_shadow_shared_account_baskets(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            sources=[dict(row) for row in experiment["sources"]],
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "distributional_survival_hazard":
        from hydra.research.distributional_survival_hazard import (
            run_distributional_survival_hazard,
        )

        return run_distributional_survival_hazard(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            core_data_path=Path(str(experiment["core_data_path"])),
            core_data_sha256=str(experiment["core_data_sha256"]),
            core_map_path=Path(str(experiment["core_map_path"])),
            core_map_sha256=str(experiment["core_map_sha256"]),
            core_roll_map_hash=str(experiment["core_roll_map_hash"]),
            metals_data_path=Path(str(experiment["metals_data_path"])),
            metals_data_sha256=str(experiment["metals_data_sha256"]),
            metals_map_path=Path(str(experiment["metals_map_path"])),
            metals_map_sha256=str(experiment["metals_map_sha256"]),
            metals_roll_map_hash=str(experiment["metals_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "meta_failure_allocation":
        from hydra.research.meta_failure_allocation import (
            run_meta_failure_allocation,
        )

        return run_meta_failure_allocation(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            snapshot=dict(experiment["snapshot"]),
            snapshot_hash=str(experiment["snapshot_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "causal_transition_graph":
        from hydra.research.causal_transition_graph import (
            run_causal_transition_graph,
        )

        return run_causal_transition_graph(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            core_data_path=Path(str(experiment["core_data_path"])),
            core_data_sha256=str(experiment["core_data_sha256"]),
            core_map_path=Path(str(experiment["core_map_path"])),
            core_map_sha256=str(experiment["core_map_sha256"]),
            core_roll_map_hash=str(experiment["core_roll_map_hash"]),
            metals_data_path=Path(str(experiment["metals_data_path"])),
            metals_data_sha256=str(experiment["metals_data_sha256"]),
            metals_map_path=Path(str(experiment["metals_map_path"])),
            metals_map_sha256=str(experiment["metals_map_sha256"]),
            metals_roll_map_hash=str(experiment["metals_roll_map_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "rty_transition_matched_null":
        from hydra.research.rty_transition_matched_null import (
            run_rty_transition_matched_null,
        )

        return run_rty_transition_matched_null(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            source_result_path=Path(str(experiment["source_result_path"])),
            source_result_sha256=str(experiment["source_result_sha256"]),
            source_result_hash=str(experiment["source_result_hash"]),
            source_manifest_path=Path(str(experiment["source_manifest_path"])),
            source_manifest_sha256=str(experiment["source_manifest_sha256"]),
            source_manifest_hash=str(experiment["source_manifest_hash"]),
            source_trade_ledger_path=Path(
                str(experiment["source_trade_ledger_path"])
            ),
            source_trade_ledger_sha256=str(
                experiment["source_trade_ledger_sha256"]
            ),
            core_data_path=Path(str(experiment["core_data_path"])),
            core_data_sha256=str(experiment["core_data_sha256"]),
            core_map_path=Path(str(experiment["core_map_path"])),
            core_map_sha256=str(experiment["core_map_sha256"]),
            core_roll_map_hash=str(experiment["core_roll_map_hash"]),
            metals_data_path=Path(str(experiment["metals_data_path"])),
            metals_data_sha256=str(experiment["metals_data_sha256"]),
            metals_map_path=Path(str(experiment["metals_map_path"])),
            metals_map_sha256=str(experiment["metals_map_sha256"]),
            metals_roll_map_hash=str(experiment["metals_roll_map_hash"]),
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
    if experiment_type == "promising_lineage_mutation":
        from hydra.factory.promising_lineage_mutator import (
            run_promising_lineage_mutation,
        )

        return run_promising_lineage_mutation(
            output_dir,
            source_manifest_path=Path(str(experiment["source_manifest_path"])),
            source_manifest_sha256=str(experiment["source_manifest_sha256"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "portfolio_role_research":
        from hydra.mission.portfolio_mutation_action import (
            run_portfolio_role_research,
        )

        return run_portfolio_role_research(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            sources=[dict(row) for row in experiment["sources"]],
            code_commit=str(experiment.get("code_commit") or "unknown"),
            defensive_control_count=int(
                experiment.get("defensive_control_count") or 4096
            ),
            inclusion_control_count=int(
                experiment.get("inclusion_control_count") or 255
            ),
        )
    if experiment_type == "forward_shadow_feed_audit":
        from hydra.mission.portfolio_mutation_action import run_forward_feed_audit

        return run_forward_feed_audit(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            required_roots=[str(value) for value in experiment["required_roots"]],
            contract_map_dir=Path(str(experiment["contract_map_dir"])),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "post_mutation_successive_halving":
        from hydra.factory.post_mutation_successive_halving import (
            run_post_mutation_successive_halving,
        )

        return run_post_mutation_successive_halving(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            mutation_result_path=Path(str(experiment["mutation_result_path"])),
            mutation_result_sha256=str(experiment["mutation_result_sha256"]),
            mutation_trade_ledger_path=Path(
                str(experiment["mutation_trade_ledger_path"])
            ),
            mutation_trade_ledger_sha256=str(
                experiment["mutation_trade_ledger_sha256"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "post_mutation_shadow_admission":
        from hydra.factory.post_mutation_shadow_admission import (
            run_post_mutation_shadow_admission,
        )

        return run_post_mutation_shadow_admission(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            halving_result_path=Path(str(experiment["halving_result_path"])),
            halving_result_sha256=str(experiment["halving_result_sha256"]),
            halving_result_hash=str(experiment["halving_result_hash"]),
            elite_manifest_path=Path(str(experiment["elite_manifest_path"])),
            elite_manifest_sha256=str(experiment["elite_manifest_sha256"]),
            elite_manifest_hash=str(experiment["elite_manifest_hash"]),
            candidate_evidence_path=Path(str(experiment["candidate_evidence_path"])),
            candidate_evidence_sha256=str(experiment["candidate_evidence_sha256"]),
            parent_source_result_path=Path(
                str(experiment["parent_source_result_path"])
            ),
            parent_source_result_sha256=str(
                experiment["parent_source_result_sha256"]
            ),
            parent_shadow_configuration_path=Path(
                str(experiment["parent_shadow_configuration_path"])
            ),
            parent_shadow_configuration_sha256=str(
                experiment["parent_shadow_configuration_sha256"]
            ),
            parent_shadow_configuration_hash=str(
                experiment["parent_shadow_configuration_hash"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "role_conditioned_structural_epoch":
        from hydra.research.role_conditioned_structural_epoch import (
            run_role_conditioned_structural_epoch,
        )

        return run_role_conditioned_structural_epoch(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            mutation_result_path=Path(str(experiment["mutation_result_path"])),
            mutation_result_sha256=str(experiment["mutation_result_sha256"]),
            mutation_result_hash=str(experiment["mutation_result_hash"]),
            mutation_ledger_path=Path(str(experiment["mutation_ledger_path"])),
            mutation_ledger_sha256=str(experiment["mutation_ledger_sha256"]),
            halving_result_path=Path(str(experiment["halving_result_path"])),
            halving_result_sha256=str(experiment["halving_result_sha256"]),
            halving_result_hash=str(experiment["halving_result_hash"]),
            halving_evidence_path=Path(str(experiment["halving_evidence_path"])),
            halving_evidence_sha256=str(experiment["halving_evidence_sha256"]),
            halving_manifest_path=Path(str(experiment["halving_manifest_path"])),
            halving_manifest_sha256=str(experiment["halving_manifest_sha256"]),
            halving_manifest_hash=str(experiment["halving_manifest_hash"]),
            portfolio_role_result_path=Path(
                str(experiment["portfolio_result_path"])
            ),
            portfolio_role_result_sha256=str(
                experiment["portfolio_result_sha256"]
            ),
            portfolio_role_result_hash=str(experiment["portfolio_result_hash"]),
            meta_result_path=Path(str(experiment["meta_result_path"])),
            meta_result_sha256=str(experiment["meta_result_sha256"]),
            meta_result_hash=str(experiment["meta_result_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
        )
    if experiment_type == "equity_preclose_inventory_dispersion":
        from hydra.research.equity_preclose_inventory_dispersion import (
            run_equity_preclose_inventory_dispersion,
        )

        return run_equity_preclose_inventory_dispersion(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            core_data_paths=[Path(str(value)) for value in experiment["core_data_paths"]],
            core_data_sha256s=[str(value) for value in experiment["core_data_sha256s"]],
            roll_map_path=Path(str(experiment["roll_map_path"])),
            roll_map_sha256=str(experiment["roll_map_sha256"]),
            roll_map_hash=str(experiment["roll_map_hash"]),
            source_role_epoch_result_hash=str(
                experiment["source_role_epoch_result_hash"]
            ),
            code_commit=str(experiment.get("code_commit") or "unknown"),
            record_data_access=bool(experiment.get("record_data_access", True)),
        )
    if experiment_type == "mini_micro_participation_divergence":
        from hydra.research.mini_micro_participation_divergence import (
            run_mini_micro_participation_divergence,
        )

        return run_mini_micro_participation_divergence(
            output_dir,
            engineering_task_path=Path(str(experiment["engineering_task_path"])),
            engineering_task_sha256=str(experiment["engineering_task_sha256"]),
            core_data_paths=[Path(str(value)) for value in experiment["core_data_paths"]],
            core_data_sha256s=[str(value) for value in experiment["core_data_sha256s"]],
            roll_map_path=Path(str(experiment["roll_map_path"])),
            roll_map_sha256=str(experiment["roll_map_sha256"]),
            roll_map_hash=str(experiment["roll_map_hash"]),
            source_preclose_result_hash=str(experiment["source_preclose_result_hash"]),
            code_commit=str(experiment.get("code_commit") or "unknown"),
            record_data_access=bool(experiment.get("record_data_access", True)),
        )
    raise UnknownExperimentType(f"No approved handler for experiment type {experiment_type!r}.")
