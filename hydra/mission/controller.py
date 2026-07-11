from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hydra.calibration.validator_benchmark import benchmark_validator, write_calibration_report
from hydra.governance.kernel import initialize_governance_kernel
from hydra.mission.engineering_runner import detect_engineering_capability
from hydra.mission.experiment_queue import (
    block_experiment,
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    ensure_experiment_schema,
    experiment_counts,
    experiment_record,
    fail_experiment,
    peek_next_experiment,
    queue_size,
    recover_resolved_missing_handler_experiments,
    recover_running_experiments,
    release_experiment_claim_for_shutdown,
    renew_experiment_lease,
)
from hydra.mission.experiment_runner import experiment_worker_entry
from hydra.mission.mission_state import (
    append_event,
    clear_stop,
    connect_state,
    get_kv,
    mission_lock,
    mission_paths,
    set_kv,
    state_snapshot,
    stop_requested,
    write_heartbeat,
)
from hydra.mission.planner import plan_next_action
from hydra.mission.reporting import write_mission_checkpoint, write_mission_summary
from hydra.mission.research_memory import record_decision, record_engineering, record_evidence
from hydra.mission.safety_governor import check_action_allowed
from hydra.pipelines.shadow_pipeline import (
    ShadowPipelineIntegrityError,
    registry_entry_from_activation,
    tick_shadow_pipeline,
)
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


CONTROLLER_VERSION = "autonomous_mission_controller_v2"
DESIGN_EXPERIMENT_ID = "calibration_affected_atom_retest_design_v1"
EXECUTION_EXPERIMENT_ID = "calibration_affected_atom_retest_execution_v1"
POST_RETEST_DESIGN_EXPERIMENT_ID = "post_calibration_retest_research_design_v1"
POST_RETEST_PILOT_EXPERIMENT_ID = "post_calibration_retest_pilot_v1"
CONTRACT_MAP_REPAIR_EXPERIMENT_ID = "contract_map_date_aware_repair_v1"
CONTRACT_MAP_REPAIR_TASK_SHA256 = "92c73632fbff1dcc65de99fdef11b04026189b4033505f82d739f5e7e34216b8"
V3_DESIGN_EXPERIMENT_ID = "calibration_affected_atom_retest_v3_design_v1"
V3_EXECUTION_EXPERIMENT_ID = "calibration_affected_atom_retest_v3_execution_v1"
PATH_GEOMETRY_AUDIT_EXPERIMENT_ID = "path_geometry_candidate_audit_v1"
METAL_ENERGY_PILOT_EXPERIMENT_ID = "metal_energy_session_transition_pilot_v1"
CROSS_MARKET_PILOT_EXPERIMENT_ID = "cross_market_lead_lag_pilot_v2"
VOLATILITY_TRANSITION_PILOT_ID = "volatility_transition_pilot_v1"
FOUNDRY_BOOTSTRAP_EXPERIMENT_ID = "foundry_bootstrap_v1"
EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID = "equity_open_gap_reversal_pilot_v1"
EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID = "equity_open_gap_continuation_pilot_v1"
Q4_CANDIDATE_FREEZE_EXPERIMENT_ID = "q4_candidate_freeze_v1"
OPENING_DIRECTION_HAZARD_EXPERIMENT_ID = "opening_direction_hazard_pilot_v1"
CROSS_ECOLOGY_OPENING_ACCEPTANCE_EXPERIMENT_ID = (
    "cross_ecology_opening_acceptance_pilot_v1"
)
MTF_SESSION_TREND_CONFIRMATION_EXPERIMENT_ID = (
    "mtf_session_trend_confirmation_pilot_v1"
)
RTY_YM_RELATIVE_VALUE_EXPERIMENT_ID = "rty_ym_relative_value_pilot_v1"
YM_SHARED_RISK_OFF_EXPERIMENT_ID = "ym_shared_risk_off_overlay_v1"
QD_ECONOMIC_TOURNAMENT_EXPERIMENT_ID = "qd_economic_tournament_v2"
YM_STRICT_PROMOTION_EXPERIMENT_ID = "ym_open_gap_strict_promotion_v1"
YM_SHADOW_ACTIVATION_EXPERIMENT_ID = "ym_immutable_shadow_activation_v1"
ACCELERATED_CONTEXT_TOURNAMENT_EXPERIMENT_ID = "accelerated_context_tournament_v1"
SELECTION_NULL_POWER_EXPERIMENT_ID = "selection_null_power_calibration_v1"
SELECTION_NULL_POLICY_REPAIR_EXPERIMENT_ID = "selection_null_policy_repair_v2"
SINGLE_PRIMARY_ALPHA_EXPERIMENT_ID = "single_primary_alpha_calibration_v3"
SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID = (
    "single_primary_context_tournament_v1"
)
COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID = "counterfactual_hazard_primary_v1"
BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID = "barrier_hazard_primary_v1"
BARRIER_HAZARD_SHADOW_ACTIVATION_EXPERIMENT_ID = (
    "barrier_hazard_shadow_activation_v1"
)
ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID = (
    "energy_metals_barrier_primary_v1"
)
ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID = (
    "energy_metals_session_geometry_primary_v1"
)
SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID = (
    "session_geometry_micro_execution_repair_v1"
)
SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID = (
    "session_geometry_micro_shadow_activation_v1"
)
GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID = (
    "gc_session_geometry_fresh_primary_v1"
)
CROSS_ASSET_DAILY_EXPERIMENT_ID = "cross_asset_daily_horizon_primary_v1"
CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID = (
    "cross_asset_daily_shadow_activation_v1"
)
SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID = (
    "shadow_shared_account_baskets_v1"
)
V3_TASK_SHA256 = "2ad1137abe0ee83f7ec1ce21acd48749df7aeed465a48777fe90a9796f606de9"
V3_REPAIR_RESULT_HASH = "a932819f1eb0b72557b39ea867d3e930fd7d9e9dcad3e4cb64e10a0bbe2abb0d"
V3_REPAIR_FILE_SHA256 = "9137d0850efae03a00c139b9628063a6b7237d4614979491956dca7063e5e1a9"
V3_INVALID_EXECUTION_RESULT_HASH = "22123708ac5ce71d89a75b73d7f3b5ee03cfd87d48655f5e28e1d828ddb12de9"
V3_INVALID_EXECUTION_FILE_SHA256 = "34e4f5d937971f277d8b86d64c69e8078bb8ffbb7e5c9ed841a4409a42c75233"
PATH_GEOMETRY_TASK_SHA256 = "5b3c795ab658c3d8a5ba799ed1f2e6c95f65daa5a3e0a97ba46599e174127023"
PATH_GEOMETRY_MAP_SHA256 = "401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda"
PATH_GEOMETRY_ROLL_HASH = "705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208"
METAL_ENERGY_TASK_SHA256 = ""
FOUNDRY_TASK_SHA256 = "0cde0fa68f8fb53ee4f3d5560b997af602331e20bfa6978716e814666af78d07"
FOUNDRY_TOURNAMENT_PREREG_SHA256 = "2578377a0623ae1337eef7980bcee6cd30db421810923c4ab6f2d388011960d5"
FOUNDRY_TOURNAMENT_REPORT_SHA256 = "49f38ef88b0142aa769677cb4f6dedb5d05089228ee1abf8f056ec115426ce88"
FOUNDRY_TOURNAMENT_CHECKPOINT_SHA256 = "021ad20268d4b2cd31f36039f831dabeefb85baf44a9b28c12f9da00dc09f1fb"
EQUITY_OPEN_GAP_TASK_SHA256 = "2c76e52c14324bdc8a3e1f4128b08bf433be9b5a18c5e73eeba3a2a7062e2f49"
EQUITY_OPEN_GAP_CONTINUATION_TASK_SHA256 = "06996cb6666a2eb1f03ba66defc1300651f71525597bff858ec876288aaf78bf"
Q4_CANDIDATE_FREEZE_TASK_SHA256 = "42be968728c7dfebc690a6fa0d496305c3ea8f74ed13b64c87302076755100fe"
OPENING_DIRECTION_HAZARD_TASK_SHA256 = "2ad28070ed623b74c86a78647b69bd63b2233de97c290674ed5254a8d4aa7080"
CROSS_ECOLOGY_OPENING_ACCEPTANCE_TASK_SHA256 = "4e2c6e4a5a10249169396a9aac5afc1caae16c591232c117569d4f2dc1acb017"
MTF_SESSION_TREND_CONFIRMATION_TASK_SHA256 = "1358287cba48702049149e0ae37e81bda95990610ded48692facc930898894b1"
RTY_YM_RELATIVE_VALUE_TASK_SHA256 = "eeb031aa4ddbe744a964a0fa1d2ab7340c788bce37fb7025251c179098e243e1"
YM_SHARED_RISK_OFF_TASK_SHA256 = "0b686391803d0f7700c9e166c1bbec4bcb19f79c584963a670bb05adb59e95ac"
YM_SHARED_RISK_OFF_PARENT_RESULT_SHA256 = "b6a501dddd579875088d30c90fe03bb858d02489364fd41d8db48a944e7fe75d"
YM_SHARED_RISK_OFF_PARENT_RESULT_HASH = "5d8935510337b92c89ee4ae00ba472700c9c436fe37aadcb92d50c78cd4f68c3"
YM_SHARED_RISK_OFF_PARENT_LEDGER_SHA256 = "e8f90171ae9efff1dfaca67312e47d05c2dff0200a8ea7a97c911186806cfba3"
QD_ECONOMIC_TOURNAMENT_TASK_SHA256 = "f6f4b91a2d138f816ee1a4f033fa161dbe49c3449830a00f48abf6395e87cc3f"
QD_SELECTOR_V2_TASK_SHA256 = "a38de867f0e711a40ad3d9f044d9b44c54cbf523d9b0447d0f371a953ba09670"
YM_STRICT_PROMOTION_TASK_SHA256 = "81085a66b7452a2a75572c0489b5a255f2826144e65fd84481041465f30d382b"
YM_FREEZE_MANIFEST_SHA256 = "12af2ee1f520207b33b05f539ad0b195f0f69c3304e32924719fab376e2bac21"
YM_FREEZE_MANIFEST_HASH = "6aae37537aa39b0b7ad70d00afd0526b64b9fccfcbf396e0a7941f55300bd62a"
YM_SHADOW_CONFIGURATION_SHA256 = "4cc734a43ae429bb760a7228dcd22e211f5bc925d57cf71b0717323faea3de4d"
YM_SHADOW_CONFIGURATION_HASH = "d8ab9d9741aedd8c4b2ab9609d97124d8d66752873bf53eec24f39a13c23ff10"
YM_SHADOW_ACTIVATION_TASK_SHA256 = "0ba6b7b53e42d77c2362ed361c8eee81ace9198f4714b54432ce97b6fd9333fc"
ACCELERATED_CONTEXT_TASK_SHA256 = "07296001c77726aeb99dcb8b6ac6ea44c2bae1f9276489eb1cf2c0f1adaf5753"
SELECTION_NULL_POWER_TASK_SHA256 = "780fbe3b85473e81e0247777399ac5184d3190f50bddc08a0c3cf8ee4530c7b6"
SELECTION_NULL_POLICY_REPAIR_TASK_SHA256 = "8ec374ea09e4f7f6f6c80b4b16665b2cfa744dd7661203306d84add3d1ade349"
SINGLE_PRIMARY_ALPHA_TASK_SHA256 = "b805c986145cbd0003eb46f512acd5989e9967e898bee0d2cf20b9558f01cb93"
SINGLE_PRIMARY_CONTEXT_TASK_SHA256 = "e66daf691c5a6e6aee54da064aaa8f6e9165f6eac54229ae17d834df38d2839b"
COUNTERFACTUAL_HAZARD_TASK_SHA256 = "d8771ee8af93edffde574c366bbc411d70531acc828726c6bb44607d559b7b79"
BARRIER_HAZARD_TASK_SHA256 = "38b7262566c4f90333993fd335bf02d93add244020867f8f46a5d3a117da8a7f"
BARRIER_SHADOW_ACTIVATION_TASK_SHA256 = "bbf681bd583fc636f48a61da1e3785c05ed8e14b1d81ab03db46b6d80740f7b6"
ENERGY_METALS_BARRIER_TASK_SHA256 = "0363e1032ef3fafe5d5a10580f2028d11671ec2a61a62bdf490bd9aa670e1388"
ENERGY_METALS_DATA_SHA256 = "07b3093ed8ef5888898abc3e531e0b522273a6c2047489b60eb36b33afeaf374"
ENERGY_METALS_VOLUME_DATA_SHA256 = "6bca31351820713016426286de8ae3ce9f0350b6886f780cccc5565fd65da78d"
ENERGY_METALS_VOLUME_MAP_SHA256 = "2ac275f4043ef210afa092be8e7f6676c0409c6e2ec5e41a01aecb37427f3815"
ENERGY_METALS_VOLUME_ROLL_HASH = "01ba149449a494a7a118884813abe10de8845c215b7390dbfbfa9d9dff89de13"
ENERGY_METALS_SESSION_GEOMETRY_TASK_SHA256 = "74ff930eea1b6e5f80143e44179d6cbaa608aac4cede8986bece2d28d199ccb1"
SESSION_GEOMETRY_MICRO_REPAIR_TASK_SHA256 = "1adfb59d86e51f3ef21b0bab94bd57bd06cbaf946f75f9b00916925ae1657378"
SESSION_GEOMETRY_MICRO_SHADOW_TASK_SHA256 = "3f2dcbe971340d3193be8f9d81da482dfd68b34539eb83d07d22a8049b270558"
SESSION_GEOMETRY_PARENT_RESULT_SHA256 = "b4f3158085697e63778849ec2b525f8c74b390fce308d1979a30c1164e4df630"
SESSION_GEOMETRY_PARENT_RESULT_HASH = "651d2a3bfb1d2ab56ac6ccaceaf067a8767389a809e153853f309ceb0ed6f69f"
SESSION_GEOMETRY_PARENT_MANIFEST_SHA256 = "e62d8b03dd74173c66183d9bca25d27006e5c2b799d2c2aa93f544cbb2fd89d8"
SESSION_GEOMETRY_PARENT_MANIFEST_HASH = "f11a6f657e018f2d8b137eddb64cf497dcf63ed0ee17848744667fa968201d96"
SESSION_GEOMETRY_PARENT_LEDGER_SHA256 = "5b3f7bc5a38ec38d5e576dcd4c60f1778410f4426e69cd394a11b33cbe9527e0"
SESSION_GEOMETRY_MICRO_CHILD_ID = (
    "strategy_session_geometry_CL_signal_MCL_execution_overnight_extreme_"
    "position_continuation_q65_h60_prior_trend_agree_v2"
)
GC_SESSION_GEOMETRY_FRESH_TASK_SHA256 = "29829c26099bd86d4d126400d4dec3ce40c15923c1499d2d823734d63a556c02"
SESSION_GEOMETRY_SOURCE_PREREGISTRATION_SHA256 = "aa0db8f5720a091576834e3e4382d691674d2ae1e86f5564418968f290a26d26"
GC_SESSION_GEOMETRY_FRESH_CHILD_ID = (
    "strategy_session_geometry_GC_signal_MGC_execution_overnight_"
    "displacement_reversal_q65_h60_none_v2"
)
CROSS_ASSET_DAILY_TASK_SHA256 = "166bb01e3e2c027873d158cb8f1659e16940228b7ad71922715480ae6a9945a0"
CROSS_ASSET_DAILY_SHADOW_TASK_SHA256 = "f9421885b6f9d1aee060c482c8ca7d5a859a2aaa91f4151ca5fdcc5ce76dc3dc"
CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID = (
    "strategy_daily_cross_CL_to_YM_source_prior_trend_"
    "continuation_q80_h120_v1"
)
SHADOW_SHARED_ACCOUNT_BASKETS_TASK_SHA256 = "8fe0c161e451a0b27d4e9ff0bdaab5b6ad8ed9d66edcd63c2ccd178ca2ffce0c"
SUPPORTED_EXPERIMENT_TYPES = {
    "calibration_affected_atom_retest_design",
    "calibration_affected_atom_retest_execution",
    "post_calibration_retest_research_design",
    "validator_integrity_repair_pilot",
    "contract_map_date_aware_repair",
    "calibration_affected_atom_retest_v3_design",
    "calibration_affected_atom_retest_v3_execution",
    "path_geometry_candidate_audit",
    "metal_energy_session_transition_pilot",
    "cross_market_lead_lag_pilot",
    "volatility_transition_pilot",
    "foundry_bootstrap",
    "equity_open_gap_reversal_pilot",
    "equity_open_gap_continuation_pilot",
    "q4_candidate_freeze",
    "opening_direction_hazard_pilot",
    "cross_ecology_opening_acceptance_pilot",
    "mtf_session_trend_confirmation_pilot",
    "rty_ym_relative_value_pilot",
    "ym_shared_risk_off_overlay",
    "qd_economic_tournament",
    "ym_open_gap_strict_promotion",
    "ym_immutable_shadow_activation",
    "accelerated_context_tournament",
    "selection_null_power_calibration",
    "selection_null_policy_repair",
    "single_primary_alpha_calibration",
    "single_primary_context_tournament",
    "counterfactual_hazard_primary",
    "barrier_hazard_primary",
    "energy_metals_barrier_primary",
    "energy_metals_session_geometry_primary",
    "session_geometry_micro_execution_repair",
    "session_geometry_micro_shadow_activation",
    "gc_session_geometry_fresh_primary",
    "cross_asset_daily_horizon_primary",
    "cross_asset_daily_shadow_activation",
    "shadow_shared_account_baskets",
    "immutable_shadow_activation",
}


class CleanWorkerInterruption(RuntimeError):
    """A controlled stop interrupted research without constituting a failed attempt."""


@dataclass(frozen=True)
class MissionControllerConfig:
    mission_id: str
    baseline_commit: str
    objective_config: str
    remaining_databento_budget_usd: float
    workers: int = 3
    checkpoint_every_minutes: float = 20.0
    persistent: bool = True
    resume: bool = True
    no_live_trading: bool = True
    state_dir: str = "mission/state"
    sleep_seconds: float = 15.0
    max_cycles: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AutonomousMissionController:
    def __init__(self, config: MissionControllerConfig) -> None:
        self.config = config
        self.paths = mission_paths(config.state_dir)
        self._shutdown = False

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        with mission_lock(self.paths):
            conn = connect_state(self.paths)
            try:
                self._initialize(conn)
                cycle_limit = self.config.max_cycles
                loops_this_process = 0
                while not self._shutdown:
                    if stop_requested(self.paths):
                        self._stop_cleanly(conn, "manual_stop_file")
                        return 0
                    action, progressed = self.step(conn)
                    loops_this_process += 1
                    if self._checkpoint_due(conn, action, progressed=progressed):
                        checkpoint = str(self._checkpoint(conn))
                    else:
                        checkpoint = str(get_kv(conn, "last_successful_checkpoint", ""))
                    write_heartbeat(self.paths, self._heartbeat_payload(conn, current_action=action, checkpoint=checkpoint))
                    if self._shutdown or stop_requested(self.paths):
                        reason = "signal" if self._shutdown else "manual_stop_file"
                        self._stop_cleanly(conn, reason)
                        return 0
                    if cycle_limit is not None and loops_this_process >= cycle_limit:
                        set_kv(conn, "last_process_status", "max_cycles_reached")
                        set_kv(conn, "last_shutdown", "clean")
                        return 0
                    if not self.config.persistent:
                        set_kv(conn, "last_shutdown", "clean")
                        return 0
                    time.sleep(self.config.sleep_seconds)
                self._stop_cleanly(conn, "signal")
                return 0
            except Exception as exc:
                set_kv(conn, "service_state", "FAILED")
                set_kv(conn, "last_shutdown", "unclean")
                set_kv(conn, "last_error", f"{type(exc).__name__}:{exc}"[:4000])
                write_heartbeat(
                    self.paths,
                    self._heartbeat_payload(
                        conn,
                        current_action={"action_type": "CONTROLLER_FAILED", "reason": str(exc)},
                        checkpoint=str(get_kv(conn, "last_successful_checkpoint", "")),
                    ),
                )
                raise
            finally:
                conn.close()

    def step(self, conn: Any) -> tuple[dict[str, Any], bool]:
        try:
            self._tick_shadow_pipeline(conn)
        except ShadowPipelineIntegrityError as exc:
            return {
                "action_id": "shadow_pipeline_integrity_blocked",
                "action_type": "INTEGRITY_BLOCKED",
                "rationale": str(exc),
            }, False
        phase = str(get_kv(conn, "current_phase", ""))
        if phase in {"INTEGRITY_BLOCKED", "ENGINEERING_BLOCKED", "EXPERIMENT_BLOCKED"}:
            return {
                "action_id": "blocked_state_requires_external_change",
                "action_type": phase,
                "rationale": str(get_kv(conn, "current_blocker", phase)),
            }, False
        if queue_size(conn) > 0:
            next_experiment = peek_next_experiment(conn)
            if next_experiment is None:
                return {"action_id": "queue_race_retry", "action_type": "RECOVERING"}, False
            experiment_type = str(next_experiment.get("experiment_type") or "")
            try:
                self._check_experiment_allowed(conn, next_experiment)
            except Exception as exc:
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", "EXPERIMENT_GOVERNANCE_GUARD_FAILED")
                set_kv(conn, "last_error", str(exc)[:4000])
                append_event(
                    conn,
                    "experiment_governance_blocked",
                    {"experiment_id": next_experiment.get("experiment_id"), "reason": str(exc)},
                )
                return {
                    "action_id": "experiment_governance_blocked",
                    "action_type": "INTEGRITY_BLOCKED",
                    "rationale": str(exc),
                }, False
            if experiment_type not in SUPPORTED_EXPERIMENT_TYPES:
                blocker = f"MISSING_EXPERIMENT_HANDLER:{experiment_type}"
                first_block = get_kv(conn, "current_blocker") != blocker
                set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
                set_kv(conn, "current_blocker", blocker)
                set_kv(conn, "last_error", f"No approved handler for experiment type {experiment_type!r}.")
                if first_block:
                    append_event(
                        conn,
                        "experiment_handler_missing_preclaim",
                        {
                            "experiment_id": next_experiment.get("experiment_id"),
                            "experiment_type": experiment_type,
                            "queue_status_preserved": "QUEUED",
                        },
                    )
                return {
                    "action_id": "missing_experiment_handler_preclaim",
                    "action_type": "ENGINEERING_BLOCKED",
                    "rationale": blocker,
                }, False
            action = {
                "action_id": "execute_highest_priority_queued_experiment",
                "action_type": "RUN_QUEUED_EXPERIMENT",
                "data_cost": 0.0,
                "expected_decision_information_gain": 1.0,
                "rationale": "A preregistered experiment is durably queued and is the next executable research action.",
            }
            self._record_action(conn, action)
            self._execute_queued_experiment(conn)
            self._record_progress(conn)
            return action, True

        snapshot = state_snapshot(conn)
        action = plan_next_action(snapshot)
        check_action_allowed(
            action,
            baseline_commit=self.config.baseline_commit,
            remaining_budget_usd=float(get_kv(conn, "remaining_databento_budget_usd", self.config.remaining_databento_budget_usd)),
        )
        if action.get("action_type") == "WAIT":
            first_stall = get_kv(conn, "current_phase") != "SCHEDULER_STALLED"
            set_kv(conn, "current_phase", "SCHEDULER_STALLED")
            set_kv(conn, "current_blocker", "NO_EXECUTABLE_ACTION_OR_SCHEDULER_DEADLINE")
            set_kv(conn, "next_wake_at_utc", None)
            if first_stall:
                append_event(conn, "scheduler_stalled", {"reason": action.get("rationale"), "queue": experiment_counts(conn)})
            return {
                **action,
                "action_id": "scheduler_stalled_no_action",
                "rationale": "Mission incomplete with no executable action or legitimate future scientific deadline.",
            }, False

        self._record_action(conn, action)
        self._execute_action(conn, action)
        self._record_progress(conn)
        return action, True

    def _initialize(self, conn: Any) -> None:
        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        clear_stop(self.paths)
        ensure_experiment_schema(conn)
        previous_phase = str(get_kv(conn, "current_phase", ""))
        previous_blocker = get_kv(conn, "current_blocker")
        previous_last_error = get_kv(conn, "last_error")
        blocked_phase = previous_phase in {"INTEGRITY_BLOCKED", "ENGINEERING_BLOCKED", "EXPERIMENT_BLOCKED"}
        resolved_missing_handler_type = self._resolved_missing_handler_type(previous_phase, previous_blocker)
        contract_map_repair_required = bool(
            previous_phase in {"INTEGRITY_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "CONTRACT_MAP_REBUILD_REQUIRED"
        )
        fresh_v3_retest_required = bool(
            previous_phase in {"INTEGRITY_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "FRESH_RETEST_WITH_REPAIRED_MAP_REQUIRED"
        )
        path_geometry_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "V3_INSUFFICIENT_EVIDENCE_RESOLUTION_DESIGN_REQUIRED"
        )
        metal_energy_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "MARKET_ECOLOGY_PIVOT_REQUIRED"
        )
        cross_market_required = bool(previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"} and str(previous_blocker or "") == "MARKET_ECOLOGY_REPRESENTATION_PIVOT_REQUIRED")
        volatility_required = bool(previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"} and str(previous_blocker or "") == "NEW_REPRESENTATION_PIVOT_REQUIRED")
        foundry_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "NEW_REPRESENTATION_PIVOT_REQUIRED"
        )
        equity_open_gap_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "EQUITY_OPEN_GAP_REVERSAL_PILOT_REQUIRED"
        )
        equity_open_gap_continuation_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "EQUITY_OPEN_GAP_CONTINUATION_PILOT_REQUIRED"
        )
        q4_candidate_freeze_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "Q4_FREEZE_PROTOCOL_REQUIRED"
        )
        opening_direction_hazard_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "DISTRIBUTIONAL_OPENING_HAZARD_PILOT_REQUIRED"
        )
        cross_ecology_opening_acceptance_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "CROSS_ECOLOGY_INVARIANT_SEARCH_REQUIRED"
        )
        mtf_session_trend_confirmation_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "MULTITIMEFRAME_SESSION_DAILY_INVARIANT_REQUIRED"
        )
        rty_ym_relative_value_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "RELATIVE_VALUE_OR_DEFENSIVE_PORTFOLIO_REQUIRED"
        )
        ym_shared_risk_off_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "DEFENSIVE_PORTFOLIO_RISK_ENGINE_REQUIRED"
        )
        qd_economic_tournament_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "INVENTED_METHOD_OR_PORTFOLIO_SEARCH_REQUIRED"
        )
        ym_strict_promotion_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            in {
                "INVENTED_METHOD_OR_PORTFOLIO_SEARCH_REQUIRED",
                "QD_TARGETED_CONFIRMATION_AND_YM_STRICT_REPLAY_REQUIRED",
                "QD_SHADOW_ACTIVATION_AND_YM_STRICT_REPLAY_REQUIRED",
                "QD_FAILURE_MAP_AND_YM_STRICT_REPLAY_REQUIRED",
            }
        )
        single_primary_context_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "NEW_SINGLE_PRIMARY_TOURNAMENT_REQUIRED"
        )
        counterfactual_hazard_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "COUNTERFACTUAL_HAZARD_PRIMARY_REQUIRED"
        )
        barrier_hazard_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "DISTRIBUTIONAL_BARRIER_HAZARD_PRIMARY_REQUIRED"
        )
        barrier_shadow_activation_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "BARRIER_HAZARD_SHADOW_ACTIVATION_REQUIRED"
        )
        energy_metals_barrier_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "ENERGY_METALS_ECOLOGY_SEARCH_REQUIRED"
        )
        energy_metals_session_geometry_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "ENERGY_METALS_SESSION_GEOMETRY_REQUIRED"
        )
        session_geometry_micro_repair_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "ENERGY_METALS_SESSION_GEOMETRY_REPLICATION_REQUIRED"
        )
        session_geometry_micro_shadow_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "SESSION_GEOMETRY_MICRO_SHADOW_ACTIVATION_REQUIRED"
        )
        gc_session_geometry_fresh_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "GC_SESSION_GEOMETRY_FRESH_ID_REQUIRED"
        )
        cross_asset_daily_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "") == "CROSS_ASSET_DAILY_HORIZON_REQUIRED"
        )
        cross_asset_daily_shadow_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "CROSS_ASSET_DAILY_SHADOW_ACTIVATION_REQUIRED"
        )
        shadow_shared_account_baskets_required = bool(
            previous_phase in {"ENGINEERING_BLOCKED", "STOPPED_CLEANLY"}
            and str(previous_blocker or "")
            == "PORTFOLIO_BASKET_AND_DISTRIBUTIONAL_SEARCH_REQUIRED"
        )
        recovered_missing_handler_rows = 0
        if resolved_missing_handler_type is not None:
            recovered_missing_handler_rows = recover_resolved_missing_handler_experiments(
                conn, resolved_missing_handler_type
            )
        previous_service_state = get_kv(conn, "service_state")
        previous_shutdown = get_kv(conn, "last_shutdown")
        if previous_service_state in {"RUNNING", "FAILED"} and previous_shutdown not in {"clean", None}:
            set_kv(conn, "crash_count", int(get_kv(conn, "crash_count", 0)) + 1)
        elif get_kv(conn, "crash_count") is None:
            set_kv(conn, "crash_count", 0)
        set_kv(conn, "controller_start_count", int(get_kv(conn, "controller_start_count", 0)) + 1)
        set_kv(conn, "last_shutdown", "unclean")
        if get_kv(conn, "mission_id") is None:
            set_kv(conn, "mission_id", self.config.mission_id)
            set_kv(conn, "objective_version", self.config.objective_config)
            set_kv(conn, "current_phase", "PHASE_0_GOVERNANCE")
            set_kv(conn, "cycle_count", 0)
            set_kv(conn, "progress_sequence", 0)
            set_kv(conn, "validated_mechanisms", 0)
            set_kv(conn, "validated_strategies", 0)
            set_kv(conn, "executable_baskets", 0)
        if get_kv(conn, "progress_sequence") is None:
            set_kv(conn, "progress_sequence", 0)
        if get_kv(conn, "last_progress_at_utc") is None:
            set_kv(conn, "last_progress_at_utc", utc_now_iso())
        set_kv(conn, "service_state", "RUNNING")
        set_kv(conn, "stop_reason", None)
        set_kv(conn, "last_error", None)
        set_kv(conn, "current_phase", "RECOVERING")
        governance = initialize_governance_kernel(
            baseline_commit=self.config.baseline_commit,
            remaining_budget_usd=self.config.remaining_databento_budget_usd,
        )
        actual_spend = float(governance.result.details.get("cumulative_actual_databento_spend_usd", 0.0))
        set_kv(conn, "cumulative_databento_spend_usd", actual_spend)
        set_kv(conn, "remaining_databento_budget_usd", max(100.0 - actual_spend, 0.0))
        set_kv(conn, "q4_access_count", int(governance.result.details.get("q4_access_count", 0)))
        set_kv(conn, "governance_manifest_hash", governance.manifest_hash)
        set_kv(conn, "governance_kernel_path", governance.manifest_path)
        set_kv(conn, "governance_passed", governance.result.passed)
        engineering = detect_engineering_capability()
        set_kv(conn, "autonomous_engineering_capability", engineering.to_dict())
        record_engineering(self.paths, {"mission_id": self.config.mission_id, "capability": engineering.to_dict()})
        recovery = recover_running_experiments(conn)
        set_kv(conn, "latest_recovery", recovery)
        if experiment_counts(conn).get("RUNNING", 0) == 0:
            set_kv(conn, "current_experiment", None)
        self._reconcile_planned_experiment_flags(conn)
        self._reconcile_completed_experiments(conn)
        self._refresh_foundry_candidate_counts(conn)
        contract_map_repair_required = contract_map_repair_required or bool(
            str(get_kv(conn, "current_phase", "")) == "INTEGRITY_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "") == "CONTRACT_MAP_REBUILD_REQUIRED"
        )
        fresh_v3_retest_required = fresh_v3_retest_required or bool(
            str(get_kv(conn, "current_phase", "")) == "INTEGRITY_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "FRESH_RETEST_WITH_REPAIRED_MAP_REQUIRED"
        )
        path_geometry_required = path_geometry_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "V3_INSUFFICIENT_EVIDENCE_RESOLUTION_DESIGN_REQUIRED"
        )
        metal_energy_required = metal_energy_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "") == "MARKET_ECOLOGY_PIVOT_REQUIRED"
        )
        cross_market_required = cross_market_required or bool(str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED" and str(get_kv(conn, "current_blocker") or "") == "MARKET_ECOLOGY_REPRESENTATION_PIVOT_REQUIRED")
        volatility_required = volatility_required or bool(str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED" and str(get_kv(conn, "current_blocker") or "") == "NEW_REPRESENTATION_PIVOT_REQUIRED")
        foundry_required = foundry_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "NEW_REPRESENTATION_PIVOT_REQUIRED"
        )
        equity_open_gap_required = equity_open_gap_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "EQUITY_OPEN_GAP_REVERSAL_PILOT_REQUIRED"
        )
        equity_open_gap_continuation_required = equity_open_gap_continuation_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "EQUITY_OPEN_GAP_CONTINUATION_PILOT_REQUIRED"
        )
        q4_candidate_freeze_required = q4_candidate_freeze_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "") == "Q4_FREEZE_PROTOCOL_REQUIRED"
        )
        opening_direction_hazard_required = opening_direction_hazard_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "DISTRIBUTIONAL_OPENING_HAZARD_PILOT_REQUIRED"
        )
        cross_ecology_opening_acceptance_required = (
            cross_ecology_opening_acceptance_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "CROSS_ECOLOGY_INVARIANT_SEARCH_REQUIRED"
            )
        )
        mtf_session_trend_confirmation_required = (
            mtf_session_trend_confirmation_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "MULTITIMEFRAME_SESSION_DAILY_INVARIANT_REQUIRED"
            )
        )
        rty_ym_relative_value_required = rty_ym_relative_value_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "RELATIVE_VALUE_OR_DEFENSIVE_PORTFOLIO_REQUIRED"
        )
        ym_shared_risk_off_required = ym_shared_risk_off_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "DEFENSIVE_PORTFOLIO_RISK_ENGINE_REQUIRED"
        )
        qd_economic_tournament_required = qd_economic_tournament_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "INVENTED_METHOD_OR_PORTFOLIO_SEARCH_REQUIRED"
        )
        ym_strict_promotion_required = ym_strict_promotion_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            in {
                "QD_TARGETED_CONFIRMATION_AND_YM_STRICT_REPLAY_REQUIRED",
                "QD_SHADOW_ACTIVATION_AND_YM_STRICT_REPLAY_REQUIRED",
                "QD_FAILURE_MAP_AND_YM_STRICT_REPLAY_REQUIRED",
            }
        )
        single_primary_context_required = single_primary_context_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "NEW_SINGLE_PRIMARY_TOURNAMENT_REQUIRED"
        )
        counterfactual_hazard_required = counterfactual_hazard_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "COUNTERFACTUAL_HAZARD_PRIMARY_REQUIRED"
        )
        barrier_hazard_required = barrier_hazard_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "DISTRIBUTIONAL_BARRIER_HAZARD_PRIMARY_REQUIRED"
        )
        barrier_shadow_activation_required = (
            barrier_shadow_activation_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "BARRIER_HAZARD_SHADOW_ACTIVATION_REQUIRED"
            )
        )
        energy_metals_barrier_required = energy_metals_barrier_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "ENERGY_METALS_ECOLOGY_SEARCH_REQUIRED"
        )
        energy_metals_session_geometry_required = (
            energy_metals_session_geometry_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "ENERGY_METALS_SESSION_GEOMETRY_REQUIRED"
            )
        )
        session_geometry_micro_repair_required = (
            session_geometry_micro_repair_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "ENERGY_METALS_SESSION_GEOMETRY_REPLICATION_REQUIRED"
            )
        )
        session_geometry_micro_shadow_required = (
            session_geometry_micro_shadow_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "SESSION_GEOMETRY_MICRO_SHADOW_ACTIVATION_REQUIRED"
            )
        )
        gc_session_geometry_fresh_required = (
            gc_session_geometry_fresh_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "GC_SESSION_GEOMETRY_FRESH_ID_REQUIRED"
            )
        )
        cross_asset_daily_required = cross_asset_daily_required or bool(
            str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
            and str(get_kv(conn, "current_blocker") or "")
            == "CROSS_ASSET_DAILY_HORIZON_REQUIRED"
        )
        cross_asset_daily_shadow_required = (
            cross_asset_daily_shadow_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "CROSS_ASSET_DAILY_SHADOW_ACTIVATION_REQUIRED"
            )
        )
        shadow_shared_account_baskets_required = (
            shadow_shared_account_baskets_required
            or bool(
                str(get_kv(conn, "current_phase", "")) == "ENGINEERING_BLOCKED"
                and str(get_kv(conn, "current_blocker") or "")
                == "PORTFOLIO_BASKET_AND_DISTRIBUTIONAL_SEARCH_REQUIRED"
            )
        )
        contract_map_repair_queued = (
            self._reconcile_contract_map_repair(conn) if contract_map_repair_required else False
        )
        fresh_v3_retest_queued = (
            self._reconcile_fresh_v3_retest(conn) if fresh_v3_retest_required else False
        )
        path_geometry_queued = (
            self._reconcile_path_geometry_audit(conn) if path_geometry_required else False
        )
        metal_energy_queued = self._reconcile_metal_energy_pilot(conn) if metal_energy_required else False
        cross_market_queued = self._reconcile_cross_market_pilot(conn) if cross_market_required else False
        volatility_queued = self._reconcile_volatility_pilot(conn) if volatility_required else False
        foundry_queued = self._reconcile_foundry_bootstrap(conn) if foundry_required else False
        equity_open_gap_queued = (
            self._reconcile_equity_open_gap_pilot(conn) if equity_open_gap_required else False
        )
        equity_open_gap_continuation_queued = (
            self._reconcile_equity_open_gap_continuation_pilot(conn)
            if equity_open_gap_continuation_required
            else False
        )
        q4_candidate_freeze_queued = (
            self._reconcile_q4_candidate_freeze(conn) if q4_candidate_freeze_required else False
        )
        opening_direction_hazard_queued = (
            self._reconcile_opening_direction_hazard(conn)
            if opening_direction_hazard_required
            else False
        )
        cross_ecology_opening_acceptance_queued = (
            self._reconcile_cross_ecology_opening_acceptance(conn)
            if cross_ecology_opening_acceptance_required
            else False
        )
        mtf_session_trend_confirmation_queued = (
            self._reconcile_mtf_session_trend_confirmation(conn)
            if mtf_session_trend_confirmation_required
            else False
        )
        rty_ym_relative_value_queued = (
            self._reconcile_rty_ym_relative_value(conn)
            if rty_ym_relative_value_required
            else False
        )
        ym_shared_risk_off_queued = (
            self._reconcile_ym_shared_risk_off(conn)
            if ym_shared_risk_off_required
            else False
        )
        qd_economic_tournament_queued = (
            self._reconcile_qd_economic_tournament(conn)
            if qd_economic_tournament_required
            else False
        )
        ym_strict_promotion_queued = (
            self._reconcile_ym_strict_promotion(conn)
            if ym_strict_promotion_required
            else False
        )
        single_primary_context_queued = (
            self._reconcile_single_primary_context_tournament(conn)
            if single_primary_context_required
            else False
        )
        counterfactual_hazard_queued = (
            self._reconcile_counterfactual_hazard_primary(conn)
            if counterfactual_hazard_required
            else False
        )
        barrier_hazard_queued = (
            self._reconcile_barrier_hazard_primary(conn)
            if barrier_hazard_required
            else False
        )
        barrier_shadow_activation_queued = (
            self._reconcile_barrier_shadow_activation(conn)
            if barrier_shadow_activation_required
            else False
        )
        energy_metals_barrier_queued = (
            self._reconcile_energy_metals_barrier_primary(conn)
            if energy_metals_barrier_required
            else False
        )
        energy_metals_session_geometry_queued = (
            self._reconcile_energy_metals_session_geometry(conn)
            if energy_metals_session_geometry_required
            else False
        )
        session_geometry_micro_repair_queued = (
            self._reconcile_session_geometry_micro_repair(conn)
            if session_geometry_micro_repair_required
            else False
        )
        session_geometry_micro_shadow_queued = (
            self._reconcile_session_geometry_micro_shadow(conn)
            if session_geometry_micro_shadow_required
            else False
        )
        gc_session_geometry_fresh_queued = (
            self._reconcile_gc_session_geometry_fresh(conn)
            if gc_session_geometry_fresh_required
            else False
        )
        cross_asset_daily_queued = (
            self._reconcile_cross_asset_daily(conn)
            if cross_asset_daily_required
            else False
        )
        cross_asset_daily_shadow_queued = (
            self._reconcile_cross_asset_daily_shadow(conn)
            if cross_asset_daily_shadow_required
            else False
        )
        shadow_shared_account_baskets_queued = (
            self._reconcile_shadow_shared_account_baskets(conn)
            if shadow_shared_account_baskets_required
            else False
        )
        self._reconcile_legacy_plan(conn)
        reconciliation_phase = str(get_kv(conn, "current_phase", ""))
        reconciliation_created_block = reconciliation_phase in {
            "INTEGRITY_BLOCKED",
            "ENGINEERING_BLOCKED",
            "EXPERIMENT_BLOCKED",
        }
        if (
            blocked_phase
            and resolved_missing_handler_type is None
            and not contract_map_repair_queued
            and not fresh_v3_retest_queued
            and not path_geometry_queued
            and not metal_energy_queued
            and not cross_market_queued
            and not volatility_queued
            and not foundry_queued
            and not equity_open_gap_queued
            and not equity_open_gap_continuation_queued
            and not q4_candidate_freeze_queued
            and not opening_direction_hazard_queued
            and not cross_ecology_opening_acceptance_queued
            and not mtf_session_trend_confirmation_queued
            and not rty_ym_relative_value_queued
            and not ym_shared_risk_off_queued
            and not qd_economic_tournament_queued
            and not ym_strict_promotion_queued
            and not single_primary_context_queued
            and not counterfactual_hazard_queued
            and not barrier_hazard_queued
            and not barrier_shadow_activation_queued
            and not energy_metals_barrier_queued
            and not energy_metals_session_geometry_queued
            and not session_geometry_micro_repair_queued
            and not session_geometry_micro_shadow_queued
            and not gc_session_geometry_fresh_queued
            and not cross_asset_daily_queued
            and not cross_asset_daily_shadow_queued
            and not shadow_shared_account_baskets_queued
        ):
            set_kv(conn, "current_phase", previous_phase)
            set_kv(conn, "current_blocker", previous_blocker)
            set_kv(conn, "last_error", previous_last_error)
        elif not reconciliation_created_block:
            set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
            set_kv(conn, "current_blocker", None)
            set_kv(conn, "last_error", None)
        append_event(
            conn,
            "controller_initialized",
            {
                "version": CONTROLLER_VERSION,
                "config": self.config.to_dict(),
                "recovery": recovery,
                "preserved_blocked_phase": (
                    previous_phase if blocked_phase and resolved_missing_handler_type is None else None
                ),
                "resolved_missing_handler_type": resolved_missing_handler_type,
                "requeued_legacy_missing_handler_rows": recovered_missing_handler_rows,
                "contract_map_repair_queued": contract_map_repair_queued,
                "fresh_v3_retest_queued": fresh_v3_retest_queued,
                "path_geometry_queued": path_geometry_queued,
                "metal_energy_queued": metal_energy_queued,
                "cross_market_queued": cross_market_queued,
                "volatility_queued": volatility_queued,
                "foundry_bootstrap_queued": foundry_queued,
                "equity_open_gap_queued": equity_open_gap_queued,
                "equity_open_gap_continuation_queued": equity_open_gap_continuation_queued,
                "q4_candidate_freeze_queued": q4_candidate_freeze_queued,
                "opening_direction_hazard_queued": opening_direction_hazard_queued,
                "cross_ecology_opening_acceptance_queued": cross_ecology_opening_acceptance_queued,
                "mtf_session_trend_confirmation_queued": mtf_session_trend_confirmation_queued,
                "rty_ym_relative_value_queued": rty_ym_relative_value_queued,
                "ym_shared_risk_off_queued": ym_shared_risk_off_queued,
                "qd_economic_tournament_queued": qd_economic_tournament_queued,
                "ym_strict_promotion_queued": ym_strict_promotion_queued,
                "single_primary_context_queued": single_primary_context_queued,
                "counterfactual_hazard_queued": counterfactual_hazard_queued,
                "barrier_hazard_queued": barrier_hazard_queued,
                "barrier_shadow_activation_queued": barrier_shadow_activation_queued,
                "energy_metals_barrier_queued": energy_metals_barrier_queued,
                "energy_metals_session_geometry_queued": energy_metals_session_geometry_queued,
                "session_geometry_micro_repair_queued": session_geometry_micro_repair_queued,
                "session_geometry_micro_shadow_queued": session_geometry_micro_shadow_queued,
                "gc_session_geometry_fresh_queued": gc_session_geometry_fresh_queued,
                "cross_asset_daily_queued": cross_asset_daily_queued,
                "cross_asset_daily_shadow_queued": cross_asset_daily_shadow_queued,
                "shadow_shared_account_baskets_queued": shadow_shared_account_baskets_queued,
                "reconciliation_created_block": reconciliation_phase if reconciliation_created_block else None,
            },
        )

    @staticmethod
    def _resolved_missing_handler_type(previous_phase: str, previous_blocker: Any) -> str | None:
        prefix = "MISSING_EXPERIMENT_HANDLER:"
        blocker = str(previous_blocker or "")
        if previous_phase != "ENGINEERING_BLOCKED" or not blocker.startswith(prefix):
            return None
        experiment_type = blocker[len(prefix) :]
        return experiment_type if experiment_type in SUPPORTED_EXPERIMENT_TYPES else None

    def _reconcile_planned_experiment_flags(self, conn: Any) -> None:
        """Close the enqueue-commit/plan-flag crash window for fixed experiments."""
        for experiment_id, flag in (
            (DESIGN_EXPERIMENT_ID, "bounded_retest_plan_written"),
            (EXECUTION_EXPERIMENT_ID, "calibration_retest_execution_plan_written"),
            (POST_RETEST_DESIGN_EXPERIMENT_ID, "post_retest_research_plan_written"),
            (CONTRACT_MAP_REPAIR_EXPERIMENT_ID, "contract_map_repair_plan_written"),
            (V3_DESIGN_EXPERIMENT_ID, "calibration_retest_v3_design_plan_written"),
            (V3_EXECUTION_EXPERIMENT_ID, "calibration_retest_v3_execution_plan_written"),
            (PATH_GEOMETRY_AUDIT_EXPERIMENT_ID, "path_geometry_audit_plan_written"),
            (METAL_ENERGY_PILOT_EXPERIMENT_ID, "metal_energy_pilot_plan_written"),
            (CROSS_MARKET_PILOT_EXPERIMENT_ID, "cross_market_pilot_plan_written"),
            (VOLATILITY_TRANSITION_PILOT_ID, "volatility_transition_plan_written"),
            (FOUNDRY_BOOTSTRAP_EXPERIMENT_ID, "foundry_bootstrap_plan_written"),
            (EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID, "equity_open_gap_plan_written"),
            (
                EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID,
                "equity_open_gap_continuation_plan_written",
            ),
            (Q4_CANDIDATE_FREEZE_EXPERIMENT_ID, "q4_candidate_freeze_plan_written"),
            (OPENING_DIRECTION_HAZARD_EXPERIMENT_ID, "opening_direction_hazard_plan_written"),
            (
                CROSS_ECOLOGY_OPENING_ACCEPTANCE_EXPERIMENT_ID,
                "cross_ecology_opening_acceptance_plan_written",
            ),
            (
                MTF_SESSION_TREND_CONFIRMATION_EXPERIMENT_ID,
                "mtf_session_trend_confirmation_plan_written",
            ),
            (RTY_YM_RELATIVE_VALUE_EXPERIMENT_ID, "rty_ym_relative_value_plan_written"),
            (YM_SHARED_RISK_OFF_EXPERIMENT_ID, "ym_shared_risk_off_plan_written"),
            (
                QD_ECONOMIC_TOURNAMENT_EXPERIMENT_ID,
                "qd_economic_tournament_plan_written",
            ),
            (YM_STRICT_PROMOTION_EXPERIMENT_ID, "ym_strict_promotion_plan_written"),
            (YM_SHADOW_ACTIVATION_EXPERIMENT_ID, "ym_shadow_activation_plan_written"),
            (
                ACCELERATED_CONTEXT_TOURNAMENT_EXPERIMENT_ID,
                "accelerated_context_tournament_plan_written",
            ),
            (SELECTION_NULL_POWER_EXPERIMENT_ID, "selection_null_power_plan_written"),
            (
                SELECTION_NULL_POLICY_REPAIR_EXPERIMENT_ID,
                "selection_null_policy_repair_plan_written",
            ),
            (SINGLE_PRIMARY_ALPHA_EXPERIMENT_ID, "single_primary_alpha_plan_written"),
            (
                SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID,
                "single_primary_context_plan_written",
            ),
            (
                COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID,
                "counterfactual_hazard_plan_written",
            ),
            (BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID, "barrier_hazard_plan_written"),
            (
                BARRIER_HAZARD_SHADOW_ACTIVATION_EXPERIMENT_ID,
                "barrier_shadow_activation_plan_written",
            ),
            (
                ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID,
                "energy_metals_barrier_plan_written",
            ),
            (
                ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID,
                "energy_metals_session_geometry_plan_written",
            ),
            (
                SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID,
                "session_geometry_micro_repair_plan_written",
            ),
            (
                SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID,
                "session_geometry_micro_shadow_plan_written",
            ),
            (
                GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID,
                "gc_session_geometry_fresh_plan_written",
            ),
            (
                CROSS_ASSET_DAILY_EXPERIMENT_ID,
                "cross_asset_daily_plan_written",
            ),
            (
                CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID,
                "cross_asset_daily_shadow_plan_written",
            ),
            (
                SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID,
                "shadow_shared_account_baskets_plan_written",
            ),
        ):
            record = experiment_record(conn, experiment_id)
            if record is not None:
                set_kv(conn, flag, True)

    def _reconcile_completed_experiments(self, conn: Any) -> None:
        for experiment_id, experiment_type, completion_flag in (
            (DESIGN_EXPERIMENT_ID, "calibration_affected_atom_retest_design", "calibration_retest_design_completed"),
            (EXECUTION_EXPERIMENT_ID, "calibration_affected_atom_retest_execution", "calibration_retest_execution_completed"),
            (
                POST_RETEST_DESIGN_EXPERIMENT_ID,
                "post_calibration_retest_research_design",
                "post_retest_research_design_completed",
            ),
            (
                POST_RETEST_PILOT_EXPERIMENT_ID,
                "validator_integrity_repair_pilot",
                "validator_integrity_repair_pilot_completed",
            ),
            (
                CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
                "contract_map_date_aware_repair",
                "contract_map_date_aware_repair_completed",
            ),
            (
                V3_DESIGN_EXPERIMENT_ID,
                "calibration_affected_atom_retest_v3_design",
                "calibration_retest_v3_design_completed",
            ),
            (
                V3_EXECUTION_EXPERIMENT_ID,
                "calibration_affected_atom_retest_v3_execution",
                "calibration_retest_v3_execution_completed",
            ),
            (
                PATH_GEOMETRY_AUDIT_EXPERIMENT_ID,
                "path_geometry_candidate_audit",
                "path_geometry_candidate_audit_completed",
            ),
            (METAL_ENERGY_PILOT_EXPERIMENT_ID, "metal_energy_session_transition_pilot", "metal_energy_pilot_completed"),
            (CROSS_MARKET_PILOT_EXPERIMENT_ID, "cross_market_lead_lag_pilot", "cross_market_pilot_completed"),
            (VOLATILITY_TRANSITION_PILOT_ID, "volatility_transition_pilot", "volatility_transition_completed"),
            (FOUNDRY_BOOTSTRAP_EXPERIMENT_ID, "foundry_bootstrap", "foundry_bootstrap_completed"),
            (
                EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID,
                "equity_open_gap_reversal_pilot",
                "equity_open_gap_reversal_completed",
            ),
            (
                EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID,
                "equity_open_gap_continuation_pilot",
                "equity_open_gap_continuation_completed",
            ),
            (
                Q4_CANDIDATE_FREEZE_EXPERIMENT_ID,
                "q4_candidate_freeze",
                "q4_candidate_freeze_completed",
            ),
            (
                OPENING_DIRECTION_HAZARD_EXPERIMENT_ID,
                "opening_direction_hazard_pilot",
                "opening_direction_hazard_completed",
            ),
            (
                CROSS_ECOLOGY_OPENING_ACCEPTANCE_EXPERIMENT_ID,
                "cross_ecology_opening_acceptance_pilot",
                "cross_ecology_opening_acceptance_completed",
            ),
            (
                MTF_SESSION_TREND_CONFIRMATION_EXPERIMENT_ID,
                "mtf_session_trend_confirmation_pilot",
                "mtf_session_trend_confirmation_completed",
            ),
            (
                RTY_YM_RELATIVE_VALUE_EXPERIMENT_ID,
                "rty_ym_relative_value_pilot",
                "rty_ym_relative_value_completed",
            ),
            (
                YM_SHARED_RISK_OFF_EXPERIMENT_ID,
                "ym_shared_risk_off_overlay",
                "ym_shared_risk_off_completed",
            ),
            (
                QD_ECONOMIC_TOURNAMENT_EXPERIMENT_ID,
                "qd_economic_tournament",
                "qd_economic_tournament_completed",
            ),
            (
                YM_STRICT_PROMOTION_EXPERIMENT_ID,
                "ym_open_gap_strict_promotion",
                "ym_strict_promotion_completed",
            ),
            (
                YM_SHADOW_ACTIVATION_EXPERIMENT_ID,
                "ym_immutable_shadow_activation",
                "ym_shadow_activation_completed",
            ),
            (
                ACCELERATED_CONTEXT_TOURNAMENT_EXPERIMENT_ID,
                "accelerated_context_tournament",
                "accelerated_context_tournament_completed",
            ),
            (
                SELECTION_NULL_POWER_EXPERIMENT_ID,
                "selection_null_power_calibration",
                "selection_null_power_completed",
            ),
            (
                SELECTION_NULL_POLICY_REPAIR_EXPERIMENT_ID,
                "selection_null_policy_repair",
                "selection_null_policy_repair_completed",
            ),
            (
                SINGLE_PRIMARY_ALPHA_EXPERIMENT_ID,
                "single_primary_alpha_calibration",
                "single_primary_alpha_completed",
            ),
            (
                SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID,
                "single_primary_context_tournament",
                "single_primary_context_completed",
            ),
            (
                COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID,
                "counterfactual_hazard_primary",
                "counterfactual_hazard_completed",
            ),
            (
                BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID,
                "barrier_hazard_primary",
                "barrier_hazard_completed",
            ),
            (
                BARRIER_HAZARD_SHADOW_ACTIVATION_EXPERIMENT_ID,
                "immutable_shadow_activation",
                "barrier_shadow_activation_completed",
            ),
            (
                ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID,
                "energy_metals_barrier_primary",
                "energy_metals_barrier_completed",
            ),
            (
                ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID,
                "energy_metals_session_geometry_primary",
                "energy_metals_session_geometry_completed",
            ),
            (
                SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID,
                "session_geometry_micro_execution_repair",
                "session_geometry_micro_repair_completed",
            ),
            (
                SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID,
                "session_geometry_micro_shadow_activation",
                "session_geometry_micro_shadow_completed",
            ),
            (
                GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID,
                "gc_session_geometry_fresh_primary",
                "gc_session_geometry_fresh_completed",
            ),
            (
                CROSS_ASSET_DAILY_EXPERIMENT_ID,
                "cross_asset_daily_horizon_primary",
                "cross_asset_daily_completed",
            ),
            (
                CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID,
                "cross_asset_daily_shadow_activation",
                "cross_asset_daily_shadow_completed",
            ),
            (
                SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID,
                "shadow_shared_account_baskets",
                "shadow_shared_account_baskets_completed",
            ),
        ):
            record = experiment_record(conn, experiment_id)
            if record is None or record.get("status") != "COMPLETED":
                continue
            result = record.get("result") or {}
            finding = result.get("scientific_conclusion") or "completed_result_recovered_without_claimed_validation"
            reconciliation_id = (
                f"completed:{experiment_id}:{record.get('specification_hash') or result.get('result_hash') or 'unknown'}"
            )
            compact = {
                "experiment_id": experiment_id,
                "experiment_type": experiment_type,
                "completed_at": record.get("completed_at"),
                "specification_hash": record.get("specification_hash"),
                "scientific_conclusion": finding,
                "report_path": result.get("report_path") or (result.get("paths") or {}).get("report"),
            }
            set_kv(conn, completion_flag, True)
            result_key = {
                "calibration_affected_atom_retest_design": "calibration_retest_design_result",
                "calibration_affected_atom_retest_execution": "calibration_retest_execution_result",
                "post_calibration_retest_research_design": "post_retest_research_design_result",
                "validator_integrity_repair_pilot": "validator_integrity_repair_pilot_result",
                "contract_map_date_aware_repair": "contract_map_date_aware_repair_result",
                "calibration_affected_atom_retest_v3_design": "calibration_retest_v3_design_result",
                "calibration_affected_atom_retest_v3_execution": "calibration_retest_v3_execution_result",
                "path_geometry_candidate_audit": "path_geometry_candidate_audit_result",
                "metal_energy_session_transition_pilot": "metal_energy_pilot_result",
                "cross_market_lead_lag_pilot": "cross_market_pilot_result",
                "volatility_transition_pilot": "volatility_transition_result",
                "foundry_bootstrap": "foundry_bootstrap_result",
                "equity_open_gap_reversal_pilot": "equity_open_gap_reversal_result",
                "equity_open_gap_continuation_pilot": "equity_open_gap_continuation_result",
                "q4_candidate_freeze": "q4_candidate_freeze_result",
                "opening_direction_hazard_pilot": "opening_direction_hazard_result",
                "cross_ecology_opening_acceptance_pilot": "cross_ecology_opening_acceptance_result",
                "mtf_session_trend_confirmation_pilot": "mtf_session_trend_confirmation_result",
                "rty_ym_relative_value_pilot": "rty_ym_relative_value_result",
                "ym_shared_risk_off_overlay": "ym_shared_risk_off_result",
                "qd_economic_tournament": "qd_economic_tournament_result",
                "ym_open_gap_strict_promotion": "ym_strict_promotion_result",
                "ym_immutable_shadow_activation": "ym_shadow_activation_result",
                "accelerated_context_tournament": "accelerated_context_tournament_result",
                "selection_null_power_calibration": "selection_null_power_result",
                "selection_null_policy_repair": "selection_null_policy_repair_result",
                "single_primary_alpha_calibration": "single_primary_alpha_result",
                "single_primary_context_tournament": "single_primary_context_result",
                "counterfactual_hazard_primary": "counterfactual_hazard_result",
                "barrier_hazard_primary": "barrier_hazard_result",
                "immutable_shadow_activation": "barrier_shadow_activation_result",
                "energy_metals_barrier_primary": "energy_metals_barrier_result",
                "energy_metals_session_geometry_primary": "energy_metals_session_geometry_result",
                "session_geometry_micro_execution_repair": "session_geometry_micro_repair_result",
                "session_geometry_micro_shadow_activation": "session_geometry_micro_shadow_result",
                "gc_session_geometry_fresh_primary": "gc_session_geometry_fresh_result",
                "cross_asset_daily_horizon_primary": "cross_asset_daily_result",
                "cross_asset_daily_shadow_activation": "cross_asset_daily_shadow_result",
                "shadow_shared_account_baskets": "shadow_shared_account_baskets_result",
            }[experiment_type]
            set_kv(conn, result_key, compact)
            set_kv(conn, "latest_completed_experiment", compact)
            set_kv(conn, "latest_scientific_finding", finding)
            set_kv(conn, "current_experiment", None)
            if experiment_type == "calibration_affected_atom_retest_execution":
                validated = int(result.get("fully_validated_edge_atoms", 0))
                if validated > 0:
                    set_kv(conn, "validated_mechanisms", max(int(get_kv(conn, "validated_mechanisms", 0)), validated))
                    set_kv(conn, "milestone", "M2_FIRST_VALIDATED_MECHANISM")
            elif experiment_type == "post_calibration_retest_research_design":
                self._queue_post_retest_pilot(conn, result)
            elif experiment_type == "validator_integrity_repair_pilot":
                disposition = str(result.get("integrity_disposition") or "INTEGRITY_AUDIT_INCONCLUSIVE")
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", disposition)
                set_kv(
                    conn,
                    "last_error",
                    f"Validator-integrity pilot requires controlled resolution: {disposition}",
                )
            elif experiment_type == "contract_map_date_aware_repair":
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", "FRESH_RETEST_WITH_REPAIRED_MAP_REQUIRED")
                set_kv(
                    conn,
                    "last_error",
                    "Repaired map is valid; a separate fresh preregistration with new atom IDs is required.",
                )
            elif experiment_type == "calibration_affected_atom_retest_v3_design":
                if not self._queue_v3_execution(conn, record):
                    set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
                    set_kv(conn, "current_blocker", "V3_EXECUTION_SPECIFICATION_INCOMPLETE")
                    set_kv(conn, "last_error", "Completed v3 design cannot produce a frozen execution spec.")
            elif experiment_type == "calibration_affected_atom_retest_v3_execution":
                self._route_v3_execution_result(conn, result)
            elif experiment_type == "path_geometry_candidate_audit":
                self._route_path_geometry_result(conn, result)
            elif experiment_type == "metal_energy_session_transition_pilot":
                self._route_metal_energy_result(conn, result)
            elif experiment_type == "cross_market_lead_lag_pilot":
                self._route_cross_market_result(conn, result)
            elif experiment_type == "volatility_transition_pilot":
                self._route_volatility_result(conn, result)
            elif experiment_type == "foundry_bootstrap":
                self._route_foundry_bootstrap_result(conn, result)
            elif experiment_type == "equity_open_gap_reversal_pilot":
                self._route_equity_open_gap_result(conn, result)
            elif experiment_type == "equity_open_gap_continuation_pilot":
                self._route_equity_open_gap_continuation_result(conn, result)
            elif experiment_type == "q4_candidate_freeze":
                self._route_q4_candidate_freeze_result(conn, result)
            elif experiment_type == "opening_direction_hazard_pilot":
                self._route_opening_direction_hazard_result(conn, result)
            elif experiment_type == "cross_ecology_opening_acceptance_pilot":
                self._route_cross_ecology_opening_acceptance_result(conn, result)
            elif experiment_type == "mtf_session_trend_confirmation_pilot":
                self._route_mtf_session_trend_confirmation_result(conn, result)
            elif experiment_type == "rty_ym_relative_value_pilot":
                self._route_rty_ym_relative_value_result(conn, result)
            elif experiment_type == "ym_shared_risk_off_overlay":
                self._route_ym_shared_risk_off_result(conn, result)
            elif experiment_type == "qd_economic_tournament":
                self._route_qd_economic_tournament_result(conn, result)
            elif experiment_type == "ym_open_gap_strict_promotion":
                self._route_ym_strict_promotion_result(conn, result)
            elif experiment_type == "ym_immutable_shadow_activation":
                self._route_ym_shadow_activation_result(conn, result)
            elif experiment_type == "accelerated_context_tournament":
                self._route_accelerated_context_tournament_result(conn, result)
            elif experiment_type == "selection_null_power_calibration":
                self._route_selection_null_power_result(conn, result)
            elif experiment_type == "selection_null_policy_repair":
                self._route_selection_null_policy_repair_result(conn, result)
            elif experiment_type == "single_primary_alpha_calibration":
                self._route_single_primary_alpha_result(conn, result)
            elif experiment_type == "single_primary_context_tournament":
                self._route_single_primary_context_result(conn, result)
            elif experiment_type == "counterfactual_hazard_primary":
                self._route_counterfactual_hazard_result(conn, result)
            elif experiment_type == "barrier_hazard_primary":
                self._route_barrier_hazard_result(conn, result)
            elif experiment_type == "immutable_shadow_activation":
                self._route_barrier_shadow_activation_result(conn, result)
            elif experiment_type == "energy_metals_barrier_primary":
                self._route_energy_metals_barrier_result(conn, result)
            elif experiment_type == "energy_metals_session_geometry_primary":
                self._route_energy_metals_session_geometry_result(conn, result)
            elif experiment_type == "session_geometry_micro_execution_repair":
                self._route_session_geometry_micro_repair_result(conn, result)
            elif experiment_type == "session_geometry_micro_shadow_activation":
                self._route_session_geometry_micro_shadow_result(conn, result)
            elif experiment_type == "gc_session_geometry_fresh_primary":
                self._route_gc_session_geometry_fresh_result(conn, result)
            elif experiment_type == "cross_asset_daily_horizon_primary":
                self._route_cross_asset_daily_result(conn, result)
            elif experiment_type == "cross_asset_daily_shadow_activation":
                self._route_cross_asset_daily_shadow_result(conn, result)
            elif experiment_type == "shadow_shared_account_baskets":
                self._route_shadow_shared_account_baskets_result(conn, result)
            if not self._evidence_reconciliation_exists(reconciliation_id):
                record_evidence(
                    self.paths,
                    {
                        "reconciliation_id": reconciliation_id,
                        "scope": "EXPERIMENT",
                        "experiment_id": experiment_id,
                        "experiment_type": experiment_type,
                        "status": "COMPLETED",
                        "result": result,
                    },
                )
            if not self._event_reconciliation_exists(conn, reconciliation_id):
                append_event(
                    conn,
                    "completed_experiment_reconciled",
                    {**compact, "reconciliation_id": reconciliation_id},
                )

    def _queue_post_retest_pilot(self, conn: Any, result: dict[str, Any]) -> None:
        pilot = result.get("pilot_experiment_specification")
        if not isinstance(pilot, dict) or not pilot.get("experiment_type"):
            raise RuntimeError("Post-retest design did not expose a complete pilot experiment specification.")
        pilot_type = str(pilot["experiment_type"])
        enqueue_experiment(conn, POST_RETEST_PILOT_EXPERIMENT_ID, pilot)
        set_kv(
            conn,
            "post_retest_pilot_selected",
            {
                "experiment_id": POST_RETEST_PILOT_EXPERIMENT_ID,
                "experiment_type": pilot_type,
                "branch": result.get("selected_branch"),
                "engineering_task_path": (result.get("paths") or {}).get("engineering_task"),
                "status": "QUEUED",
            },
        )
        if pilot_type not in SUPPORTED_EXPERIMENT_TYPES:
            set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
            set_kv(conn, "current_blocker", f"MISSING_EXPERIMENT_HANDLER:{pilot_type}")
            set_kv(
                conn,
                "last_error",
                f"Post-retest branch selected; approved pilot handler {pilot_type!r} must be implemented.",
            )

    def _reconcile_contract_map_repair(self, conn: Any) -> bool:
        existing = experiment_record(conn, CONTRACT_MAP_REPAIR_EXPERIMENT_ID)
        if existing is not None:
            return str(existing.get("status")) in {"QUEUED", "RUNNING"}
        pilot = experiment_record(conn, POST_RETEST_PILOT_EXPERIMENT_ID)
        result = (pilot or {}).get("result") or {}
        if (pilot or {}).get("status") != "COMPLETED" or result.get("integrity_disposition") != (
            "CONTRACT_MAP_REBUILD_REQUIRED"
        ):
            return False
        contract_audit = result.get("contract_map_integrity_audit") or {}
        artifacts = result.get("artifacts") or {}
        task_path = project_path(
            "reports", "engineering", "hydra_contract_map_date_repair_20260710_v2.md"
        )
        if not task_path.is_file() or hashlib.sha256(task_path.read_bytes()).hexdigest() != (
            CONTRACT_MAP_REPAIR_TASK_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CONTRACT_MAP_REPAIR_TASK_HASH_MISMATCH")
            set_kv(conn, "last_error", "Immutable contract-map repair task is missing or changed.")
            return False
        specification = {
            "experiment_type": "contract_map_date_aware_repair",
            "priority": 100.0,
            "max_attempts": 3,
            "integrity_pilot_result_path": str(artifacts.get("result_json_path") or ""),
            "integrity_pilot_result_hash": str(result.get("result_hash") or ""),
            "frozen_contract_map_path": str(contract_audit.get("frozen_contract_map_path") or ""),
            "frozen_contract_map_sha256": str(contract_audit.get("frozen_contract_map_sha256") or ""),
            "definition_dbn_path": str(contract_audit.get("definition_dbn_path") or ""),
            "definition_dbn_sha256": str(contract_audit.get("definition_dbn_sha256") or ""),
            "engineering_task_path": str(task_path),
            "engineering_task_sha256": CONTRACT_MAP_REPAIR_TASK_SHA256,
            "code_commit": self._git_commit(),
            "data_role": "CACHED_DEFINITION_METADATA_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "market_observation_read_allowed": False,
        }
        if not all(
            specification[key]
            for key in (
                "integrity_pilot_result_path",
                "integrity_pilot_result_hash",
                "frozen_contract_map_path",
                "frozen_contract_map_sha256",
                "definition_dbn_path",
                "definition_dbn_sha256",
            )
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CONTRACT_MAP_REPAIR_SOURCE_INCOMPLETE")
            set_kv(conn, "last_error", "Integrity pilot lacks frozen map-repair source metadata.")
            return False
        enqueue_experiment(conn, CONTRACT_MAP_REPAIR_EXPERIMENT_ID, specification)
        set_kv(conn, "contract_map_repair_plan_written", True)
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": CONTRACT_MAP_REPAIR_EXPERIMENT_ID,
                "experiment_type": "contract_map_date_aware_repair",
                "status": "QUEUED",
                "reason": "Confirmed contract-map date-flattening integrity defect.",
            },
        )
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "last_error", None)
        return True

    def _reconcile_fresh_v3_retest(self, conn: Any) -> bool:
        execution = experiment_record(conn, V3_EXECUTION_EXPERIMENT_ID)
        if execution is not None:
            if str(execution.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return False
        design = experiment_record(conn, V3_DESIGN_EXPERIMENT_ID)
        if design is not None:
            if design.get("status") == "COMPLETED":
                return self._queue_v3_execution(conn, design)
            if str(design.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return False

        repair = experiment_record(conn, CONTRACT_MAP_REPAIR_EXPERIMENT_ID)
        invalid_execution = experiment_record(conn, EXECUTION_EXPERIMENT_ID)
        repair_result = (repair or {}).get("result") or {}
        invalid_result = (invalid_execution or {}).get("result") or {}
        if (
            (repair or {}).get("status") != "COMPLETED"
            or repair_result.get("result_hash") != V3_REPAIR_RESULT_HASH
            or (invalid_execution or {}).get("status") != "COMPLETED"
            or invalid_result.get("result_hash") != V3_INVALID_EXECUTION_RESULT_HASH
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "V3_FROZEN_PREDECESSOR_MISMATCH")
            set_kv(conn, "last_error", "Fresh v3 retest predecessors are missing or have changed.")
            return False
        task_path = project_path(
            "reports", "engineering", "hydra_calibration_retest_v3_20260710.md"
        )
        if not task_path.is_file() or hashlib.sha256(task_path.read_bytes()).hexdigest() != (
            V3_TASK_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "V3_ENGINEERING_TASK_HASH_MISMATCH")
            set_kv(conn, "last_error", "Immutable v3 engineering task is missing or changed.")
            return False
        repair_artifacts = repair_result.get("artifacts") or {}
        invalid_artifacts = invalid_result.get("artifacts") or {}
        repaired_map = repair_result.get("repaired_map") or {}
        specification = {
            "experiment_type": "calibration_affected_atom_retest_v3_design",
            "priority": 100.0,
            "max_attempts": 3,
            "contract_map_repair_result_path": str(
                repair_artifacts.get("result_json_path") or ""
            ),
            "contract_map_repair_result_hash": V3_REPAIR_RESULT_HASH,
            "contract_map_repair_file_sha256": V3_REPAIR_FILE_SHA256,
            "invalid_v2_execution_result_path": str(
                invalid_artifacts.get("result_json_path") or ""
            ),
            "invalid_v2_execution_result_hash": V3_INVALID_EXECUTION_RESULT_HASH,
            "invalid_v2_execution_file_sha256": V3_INVALID_EXECUTION_FILE_SHA256,
            "repaired_map_path": str(repaired_map.get("path") or ""),
            "repaired_map_sha256": str(repaired_map.get("sha256") or ""),
            "repaired_roll_map_hash": str(repaired_map.get("roll_map_hash") or ""),
            "engineering_task_path": str(task_path),
            "engineering_task_sha256": V3_TASK_SHA256,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_DESIGN_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "market_observation_read_allowed": False,
        }
        required = (
            "contract_map_repair_result_path",
            "invalid_v2_execution_result_path",
            "repaired_map_path",
            "repaired_map_sha256",
            "repaired_roll_map_hash",
        )
        if not all(specification[key] for key in required):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "V3_SOURCE_SPECIFICATION_INCOMPLETE")
            set_kv(conn, "last_error", "Fresh v3 design lacks frozen predecessor metadata.")
            return False
        enqueue_experiment(conn, V3_DESIGN_EXPERIMENT_ID, specification)
        set_kv(conn, "calibration_retest_v3_design_plan_written", True)
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": V3_DESIGN_EXPERIMENT_ID,
                "experiment_type": "calibration_affected_atom_retest_v3_design",
                "status": "QUEUED",
                "reason": (
                    "Highest-EDIG resolution of the integrity-conditioned v2 decision using new IDs "
                    "and the repaired date-aware map."
                ),
            },
        )
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "last_error", None)
        return True

    def _queue_v3_execution(self, conn: Any, design: dict[str, Any]) -> bool:
        existing = experiment_record(conn, V3_EXECUTION_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        result = design.get("result") or {}
        if result.get("scientific_conclusion") != (
            "FRESH_V3_RETEST_PREREGISTERED_ON_DATE_AWARE_MAP_NO_EVIDENCE_INHERITED"
        ):
            return False
        paths = result.get("paths") or result.get("artifacts") or {}
        design_path = paths.get("design") or result.get("design_path")
        preregistration_path = paths.get("preregistration") or result.get("preregistration_path")
        manifest = (result.get("source") or {}).get("development_data_manifest") or {}
        repaired_map = manifest.get("contract_map") or {}
        if not design_path or not preregistration_path or not repaired_map.get("path"):
            return False
        specification = {
            "experiment_type": "calibration_affected_atom_retest_v3_execution",
            "priority": 99.0,
            "max_attempts": 3,
            "design_path": str(design_path),
            "design_preregistration_path": str(preregistration_path),
            "design_hash": str(result.get("design_hash") or ""),
            "preregistration_hash": str(
                (result.get("preregistration") or {}).get("preregistration_hash") or ""
            ),
            "repaired_map_path": str(repaired_map.get("path")),
            "repaired_map_sha256": str(repaired_map.get("sha256") or ""),
            "repaired_roll_map_hash": str(repaired_map.get("roll_map_hash") or ""),
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
        }
        if not all(
            specification[key]
            for key in (
                "design_hash",
                "preregistration_hash",
                "repaired_map_sha256",
                "repaired_roll_map_hash",
            )
        ):
            return False
        enqueue_experiment(conn, V3_EXECUTION_EXPERIMENT_ID, specification)
        set_kv(conn, "calibration_retest_v3_execution_plan_written", True)
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": V3_EXECUTION_EXPERIMENT_ID,
                "experiment_type": "calibration_affected_atom_retest_v3_execution",
                "status": "QUEUED",
                "reason": "Execute the immutable v3 preregistration on the repaired map.",
            },
        )
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "last_error", None)
        return True

    def _reconcile_path_geometry_audit(self, conn: Any) -> bool:
        existing = experiment_record(conn, PATH_GEOMETRY_AUDIT_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task_path = project_path("reports", "engineering", "hydra_candidate_path_geometry_audit_20260711.md")
        map_path = project_path("data", "cache", "contract_maps", "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json")
        if not task_path.is_file() or hashlib.sha256(task_path.read_bytes()).hexdigest() != PATH_GEOMETRY_TASK_SHA256:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "PATH_GEOMETRY_TASK_HASH_MISMATCH")
            set_kv(conn, "last_error", "Immutable path-geometry task is missing or changed.")
            return False
        specification = {
            "experiment_type": "path_geometry_candidate_audit",
            "priority": 100.0,
            "max_attempts": 2,
            "engineering_task_path": str(task_path),
            "engineering_task_sha256": PATH_GEOMETRY_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
        }
        enqueue_experiment(conn, PATH_GEOMETRY_AUDIT_EXPERIMENT_ID, specification)
        set_kv(conn, "path_geometry_audit_plan_written", True)
        set_kv(conn, "current_research_experiment_selected", {
            "experiment": PATH_GEOMETRY_AUDIT_EXPERIMENT_ID,
            "experiment_type": "path_geometry_candidate_audit",
            "status": "QUEUED",
            "reason": "Highest-priority candidate-level audit after the historical screen and defensive identifiability freeze.",
        })
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_metal_energy_pilot(self, conn: Any) -> bool:
        existing = experiment_record(conn, METAL_ENERGY_PILOT_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path("reports", "engineering", "hydra_metal_energy_session_transition_20260711.md")
        task_hash = hashlib.sha256(task.read_bytes()).hexdigest() if task.is_file() else ""
        if not task_hash:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED"); set_kv(conn, "current_blocker", "METAL_ENERGY_TASK_MISSING"); return False
        map_path = project_path("data", "cache", "contract_maps", "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json")
        spec = {"experiment_type": "metal_energy_session_transition_pilot", "priority": 100.0, "max_attempts": 2, "engineering_task_path": str(task), "engineering_task_sha256": task_hash, "repaired_map_path": str(map_path), "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256, "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH, "code_commit": self._git_commit(), "q4_access_allowed": False, "paid_data_allowed": False, "network_allowed": False, "live_or_broker_allowed": False, "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY"}
        enqueue_experiment(conn, METAL_ENERGY_PILOT_EXPERIMENT_ID, spec); set_kv(conn, "metal_energy_pilot_plan_written", True); self._clear_resolved_resume_block(conn); return True

    def _reconcile_cross_market_pilot(self, conn: Any) -> bool:
        existing = experiment_record(conn, CROSS_MARKET_PILOT_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}: self._clear_resolved_resume_block(conn); return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path("reports", "engineering", "hydra_cross_market_lead_lag_20260711.md")
        if not task.is_file(): return False
        mp = project_path("data", "cache", "contract_maps", "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json")
        spec={"experiment_type":"cross_market_lead_lag_pilot","priority":100.0,"max_attempts":2,"engineering_task_path":str(task),"engineering_task_sha256":hashlib.sha256(task.read_bytes()).hexdigest(),"repaired_map_path":str(mp),"repaired_map_sha256":PATH_GEOMETRY_MAP_SHA256,"repaired_roll_map_hash":PATH_GEOMETRY_ROLL_HASH,"code_commit":self._git_commit(),"q4_access_allowed":False,"paid_data_allowed":False,"network_allowed":False,"live_or_broker_allowed":False}
        enqueue_experiment(conn,CROSS_MARKET_PILOT_EXPERIMENT_ID,spec); set_kv(conn,"cross_market_pilot_plan_written",True); self._clear_resolved_resume_block(conn); return True

    def _reconcile_volatility_pilot(self, conn: Any) -> bool:
        existing = experiment_record(conn, VOLATILITY_TRANSITION_PILOT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}: self._clear_resolved_resume_block(conn); return True
            return str(existing.get("status")) == "COMPLETED"
        task=project_path("reports","engineering","hydra_volatility_transition_20260711.md")
        if not task.is_file(): return False
        mp=project_path("data","cache","contract_maps","roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json")
        spec={"experiment_type":"volatility_transition_pilot","priority":100.0,"max_attempts":2,"engineering_task_path":str(task),"engineering_task_sha256":hashlib.sha256(task.read_bytes()).hexdigest(),"repaired_map_path":str(mp),"repaired_map_sha256":PATH_GEOMETRY_MAP_SHA256,"repaired_roll_map_hash":PATH_GEOMETRY_ROLL_HASH,"code_commit":self._git_commit(),"q4_access_allowed":False,"paid_data_allowed":False,"network_allowed":False,"live_or_broker_allowed":False}
        enqueue_experiment(conn,VOLATILITY_TRANSITION_PILOT_ID,spec);set_kv(conn,"volatility_transition_plan_written",True);self._clear_resolved_resume_block(conn);return True

    def _reconcile_foundry_bootstrap(self, conn: Any) -> bool:
        existing = experiment_record(conn, FOUNDRY_BOOTSTRAP_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path("reports", "engineering", "hydra_foundry_core_20260711.md")
        runtime_root = project_path()
        tournament_preregistration = runtime_root / (
            "reports/edge_atom_lab/"
            "edge_atom_preregistration_20260711T053018+0000_governed_strategy_tournament_20_v1.json"
        )
        tournament_report = runtime_root / (
            "reports/edge_atom_lab/"
            "edge_atom_lab_20260711T053018+0000_governed_strategy_tournament_20_v1.md"
        )
        tournament_checkpoint = runtime_root / (
            "reports/checkpoints/edge_atom_lab/"
            "edge_atom_checkpoint_20260711T053018+0000_governed_strategy_tournament_20_v1.md"
        )
        # Isolated engineering worktrees do not copy runtime evidence; the
        # deployed service always resolves it from the canonical repository.
        if not tournament_report.is_file():
            canonical = Path("/root/hydra-bot")
            tournament_preregistration = canonical / tournament_preregistration.relative_to(runtime_root)
            tournament_report = canonical / tournament_report.relative_to(runtime_root)
            tournament_checkpoint = canonical / tournament_checkpoint.relative_to(runtime_root)
        frozen = {
            task: FOUNDRY_TASK_SHA256,
            tournament_preregistration: FOUNDRY_TOURNAMENT_PREREG_SHA256,
            tournament_report: FOUNDRY_TOURNAMENT_REPORT_SHA256,
            tournament_checkpoint: FOUNDRY_TOURNAMENT_CHECKPOINT_SHA256,
        }
        changed = [str(path) for path, digest in frozen.items() if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest]
        if changed:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "FOUNDRY_FROZEN_SOURCE_MISMATCH")
            set_kv(conn, "last_error", f"Foundry frozen sources missing or changed: {changed}")
            return False
        specification = {
            "experiment_type": "foundry_bootstrap",
            "priority": 110.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": FOUNDRY_TASK_SHA256,
            "tournament_preregistration_path": str(tournament_preregistration),
            "tournament_preregistration_sha256": FOUNDRY_TOURNAMENT_PREREG_SHA256,
            "tournament_report_path": str(tournament_report),
            "tournament_report_sha256": FOUNDRY_TOURNAMENT_REPORT_SHA256,
            "tournament_checkpoint_path": str(tournament_checkpoint),
            "tournament_checkpoint_sha256": FOUNDRY_TOURNAMENT_CHECKPOINT_SHA256,
            "code_commit": self._git_commit(),
            "data_role": "FROZEN_DEVELOPMENT_EVIDENCE_AND_SYNTHETIC_CONTROLS_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
        }
        enqueue_experiment(conn, FOUNDRY_BOOTSTRAP_EXPERIMENT_ID, specification)
        set_kv(conn, "foundry_bootstrap_plan_written", True)
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": FOUNDRY_BOOTSTRAP_EXPERIMENT_ID,
                "experiment_type": "foundry_bootstrap",
                "status": "QUEUED",
                "reason": (
                    "Reconcile the zero-survivor direct tournament, calibrate shadow semantics, "
                    "and prove MTF/QD/fail-closed shadow infrastructure before new production."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_equity_open_gap_pilot(self, conn: Any) -> bool:
        existing = experiment_record(conn, EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_equity_open_gap_reversal_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot") / map_path.relative_to(project_path())
        frozen = {
            task: EQUITY_OPEN_GAP_TASK_SHA256,
            map_path: PATH_GEOMETRY_MAP_SHA256,
        }
        changed = [
            str(path)
            for path, digest in frozen.items()
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest
        ]
        if changed:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "EQUITY_OPEN_GAP_FROZEN_SOURCE_MISMATCH")
            set_kv(conn, "last_error", f"Frozen gap-pilot source missing or changed: {changed}")
            return False
        specification = {
            "experiment_type": "equity_open_gap_reversal_pilot",
            "priority": 109.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": EQUITY_OPEN_GAP_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.94,
        }
        enqueue_experiment(conn, EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID, specification)
        set_kv(conn, "equity_open_gap_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_A_DIRECT_STATE_MACHINE")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID,
                "experiment_type": "equity_open_gap_reversal_pilot",
                "status": "QUEUED",
                "reason": (
                    "Highest EDIG direct sparse strategy after the zero-survivor tournament: "
                    "one daily event, four mini/micro contractual pairs, $0 data cost, and "
                    "candidate-level shadow/MLL evidence."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_equity_open_gap_continuation_pilot(self, conn: Any) -> bool:
        existing = experiment_record(conn, EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        reversal_record = experiment_record(conn, EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID)
        reversal = (reversal_record or {}).get("result") or {}
        if (
            (reversal_record or {}).get("status") != "COMPLETED"
            or reversal.get("scientific_conclusion")
            != "EQUITY_OPEN_GAP_REVERSAL_FALSIFIED_OR_INSUFFICIENT"
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CONTINUATION_SOURCE_REVERSAL_INVALID")
            set_kv(
                conn,
                "last_error",
                "Fresh continuation pilot requires the frozen negative reversal result.",
            )
            return False
        reversal_path = Path(
            str((reversal.get("artifacts") or {}).get("result_json_path") or "")
        )
        reversal_hash = str(reversal.get("result_hash") or "")
        if not reversal_path.is_file() or not reversal_hash:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CONTINUATION_SOURCE_ARTIFACT_MISSING")
            set_kv(conn, "last_error", "Frozen reversal result path/hash is unavailable.")
            return False
        task = project_path(
            "reports", "engineering", "hydra_equity_open_gap_continuation_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot") / map_path.relative_to(project_path())
        frozen = {
            task: EQUITY_OPEN_GAP_CONTINUATION_TASK_SHA256,
            map_path: PATH_GEOMETRY_MAP_SHA256,
        }
        changed = [
            str(path)
            for path, digest in frozen.items()
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest
        ]
        if changed:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CONTINUATION_FROZEN_SOURCE_MISMATCH")
            set_kv(conn, "last_error", f"Continuation frozen source changed: {changed}")
            return False
        specification = {
            "experiment_type": "equity_open_gap_continuation_pilot",
            "priority": 108.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": EQUITY_OPEN_GAP_CONTINUATION_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "source_reversal_result_path": str(reversal_path),
            "source_reversal_result_sha256": hashlib.sha256(
                reversal_path.read_bytes()
            ).hexdigest(),
            "source_reversal_result_hash": reversal_hash,
            "source_reversal_specification_hash": (reversal_record or {}).get(
                "specification_hash"
            ),
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.97,
            "inherits_reversal_status": False,
        }
        enqueue_experiment(
            conn, EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID, specification
        )
        set_kv(conn, "equity_open_gap_continuation_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_A_TARGETED_MUTATION")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID,
                "experiment_type": "equity_open_gap_continuation_pilot",
                "status": "QUEUED",
                "reason": (
                    "The preregistered opposite-sign control changed the directional decision "
                    "on three markets; fresh IDs and candidate-level evidence test it without inheritance."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_q4_candidate_freeze(self, conn: Any) -> bool:
        existing = experiment_record(conn, Q4_CANDIDATE_FREEZE_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        continuation_record = experiment_record(
            conn, EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID
        )
        continuation = (continuation_record or {}).get("result") or {}
        eligible = list(continuation.get("q4_freeze_eligible_candidate_ids") or [])
        candidates = {
            str(row.get("candidate_id")): row
            for row in continuation.get("candidates") or []
        }
        ranked = sorted(
            (candidates[item] for item in eligible if item in candidates),
            key=lambda row: (
                float(
                    (row.get("null_evidence") or {}).get(
                        "family_adjusted_probability", 1.0
                    )
                ),
                -int(row.get("supportive_temporal_folds", 0)),
                -float(row.get("net_pnl", 0.0)),
                str(row.get("candidate_id")),
            ),
        )
        if (continuation_record or {}).get("status") != "COMPLETED" or not ranked:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "Q4_FREEZE_ELIGIBLE_SOURCE_MISSING")
            set_kv(conn, "last_error", "No frozen continuation candidate is eligible for Q4 freeze.")
            return False
        candidate_id = str(ranked[0]["candidate_id"])
        artifacts = continuation.get("artifacts") or {}
        result_path = Path(str(artifacts.get("result_json_path") or ""))
        trade_ledger = Path(str(artifacts.get("trade_ledger_path") or ""))
        configurations = {
            str(row.get("candidate_id")): row
            for row in continuation.get("shadow_configurations") or []
        }
        configuration = configurations.get(candidate_id) or {}
        configuration_path = Path(str(configuration.get("path") or ""))
        task = project_path(
            "reports", "engineering", "hydra_q4_candidate_freeze_20260711.md"
        )
        frozen = {
            task: Q4_CANDIDATE_FREEZE_TASK_SHA256,
            result_path: hashlib.sha256(result_path.read_bytes()).hexdigest()
            if result_path.is_file()
            else "",
            trade_ledger: hashlib.sha256(trade_ledger.read_bytes()).hexdigest()
            if trade_ledger.is_file()
            else "",
            configuration_path: hashlib.sha256(configuration_path.read_bytes()).hexdigest()
            if configuration_path.is_file()
            else "",
        }
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != Q4_CANDIDATE_FREEZE_TASK_SHA256
            or any(not path.is_file() or not digest for path, digest in frozen.items())
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "Q4_FREEZE_SOURCE_ARTIFACT_MISSING")
            set_kv(conn, "last_error", "Continuation freeze source artifacts are incomplete.")
            return False
        specification = {
            "experiment_type": "q4_candidate_freeze",
            "priority": 120.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": Q4_CANDIDATE_FREEZE_TASK_SHA256,
            "source_continuation_result_path": str(result_path),
            "source_continuation_result_sha256": frozen[result_path],
            "source_continuation_result_hash": str(
                continuation.get("result_hash") or ""
            ),
            "source_trade_ledger_path": str(trade_ledger),
            "source_trade_ledger_sha256": frozen[trade_ledger],
            "source_shadow_configuration_path": str(configuration_path),
            "source_shadow_configuration_sha256": frozen[configuration_path],
            "source_shadow_configuration_hash": str(
                configuration.get("configuration_hash") or ""
            ),
            "candidate_id": candidate_id,
            "code_commit": self._git_commit(),
            "governance_baseline_commit": self.config.baseline_commit,
            "remaining_databento_budget_usd": float(
                get_kv(
                    conn,
                    "remaining_databento_budget_usd",
                    self.config.remaining_databento_budget_usd,
                )
            ),
            "data_role": "METADATA_AND_FROZEN_DEVELOPMENT_EVIDENCE_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "market_data_reads_allowed": False,
            "expected_decision_information_gain": 0.99,
        }
        if not specification["source_continuation_result_hash"] or not specification[
            "source_shadow_configuration_hash"
        ]:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "Q4_FREEZE_HASH_CONTRACT_INCOMPLETE")
            return False
        enqueue_experiment(conn, Q4_CANDIDATE_FREEZE_EXPERIMENT_ID, specification)
        set_kv(conn, "q4_candidate_freeze_plan_written", True)
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": Q4_CANDIDATE_FREEZE_EXPERIMENT_ID,
                "experiment_type": "q4_candidate_freeze",
                "candidate_id": candidate_id,
                "status": "QUEUED",
                "reason": "Mandatory immutable boundary before any one-shot Q4 decision.",
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_opening_direction_hazard(self, conn: Any) -> bool:
        existing = experiment_record(conn, OPENING_DIRECTION_HAZARD_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_opening_direction_hazard_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot") / map_path.relative_to(project_path())
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != OPENING_DIRECTION_HAZARD_TASK_SHA256
            or not map_path.is_file()
            or hashlib.sha256(map_path.read_bytes()).hexdigest()
            != PATH_GEOMETRY_MAP_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "OPENING_HAZARD_FROZEN_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "Opening-hazard task or explicit map changed.")
            return False
        specification = {
            "experiment_type": "opening_direction_hazard_pilot",
            "priority": 107.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": OPENING_DIRECTION_HAZARD_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.93,
        }
        enqueue_experiment(conn, OPENING_DIRECTION_HAZARD_EXPERIMENT_ID, specification)
        set_kv(conn, "opening_direction_hazard_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_B_DISTRIBUTIONAL_HAZARD")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": OPENING_DIRECTION_HAZARD_EXPERIMENT_ID,
                "experiment_type": "opening_direction_hazard_pilot",
                "status": "QUEUED",
                "reason": (
                    "Directional instability is the dominant gap failure surface; rolling-origin "
                    "probabilities test conditional continuation/reversal with abstention."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_cross_ecology_opening_acceptance(self, conn: Any) -> bool:
        existing = experiment_record(
            conn, CROSS_ECOLOGY_OPENING_ACCEPTANCE_EXPERIMENT_ID
        )
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports",
            "engineering",
            "hydra_cross_ecology_opening_acceptance_20260711.md",
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot") / map_path.relative_to(project_path())
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != CROSS_ECOLOGY_OPENING_ACCEPTANCE_TASK_SHA256
            or not map_path.is_file()
            or hashlib.sha256(map_path.read_bytes()).hexdigest()
            != PATH_GEOMETRY_MAP_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CROSS_ECOLOGY_FROZEN_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "Cross-ecology task or explicit map changed.")
            return False
        specification = {
            "experiment_type": "cross_ecology_opening_acceptance_pilot",
            "priority": 106.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": CROSS_ECOLOGY_OPENING_ACCEPTANCE_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.91,
        }
        enqueue_experiment(
            conn, CROSS_ECOLOGY_OPENING_ACCEPTANCE_EXPERIMENT_ID, specification
        )
        set_kv(conn, "cross_ecology_opening_acceptance_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_E_CROSS_ECOLOGY_INVARIANT")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": CROSS_ECOLOGY_OPENING_ACCEPTANCE_EXPERIMENT_ID,
                "experiment_type": "cross_ecology_opening_acceptance_pilot",
                "status": "QUEUED",
                "reason": (
                    "First post-takeover strategy-level test in metals and energy; explicit market "
                    "clocks and contract economics reduce ecology uncertainty at zero data cost."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_mtf_session_trend_confirmation(self, conn: Any) -> bool:
        existing = experiment_record(conn, MTF_SESSION_TREND_CONFIRMATION_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_mtf_session_trend_confirmation_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot") / map_path.relative_to(project_path())
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != MTF_SESSION_TREND_CONFIRMATION_TASK_SHA256
            or not map_path.is_file()
            or hashlib.sha256(map_path.read_bytes()).hexdigest()
            != PATH_GEOMETRY_MAP_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "MTF_SESSION_CONFIRMATION_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "MTF task or explicit map changed.")
            return False
        specification = {
            "experiment_type": "mtf_session_trend_confirmation_pilot",
            "priority": 105.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": MTF_SESSION_TREND_CONFIRMATION_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.92,
        }
        enqueue_experiment(
            conn, MTF_SESSION_TREND_CONFIRMATION_EXPERIMENT_ID, specification
        )
        set_kv(conn, "mtf_session_trend_confirmation_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_E_MTF_INVARIANT")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": MTF_SESSION_TREND_CONFIRMATION_EXPERIMENT_ID,
                "experiment_type": "mtf_session_trend_confirmation_pilot",
                "status": "QUEUED",
                "reason": (
                    "First strategy-level causal join of completed session state, completed 30m "
                    "confirmation and 1m execution across four contractual pairs."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_rty_ym_relative_value(self, conn: Any) -> bool:
        existing = experiment_record(conn, RTY_YM_RELATIVE_VALUE_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_rty_ym_relative_value_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot") / map_path.relative_to(project_path())
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != RTY_YM_RELATIVE_VALUE_TASK_SHA256
            or not map_path.is_file()
            or hashlib.sha256(map_path.read_bytes()).hexdigest()
            != PATH_GEOMETRY_MAP_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "RTY_YM_RELATIVE_VALUE_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "Relative-value task or explicit map changed.")
            return False
        specification = {
            "experiment_type": "rty_ym_relative_value_pilot",
            "priority": 104.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": RTY_YM_RELATIVE_VALUE_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.94,
        }
        enqueue_experiment(conn, RTY_YM_RELATIVE_VALUE_EXPERIMENT_ID, specification)
        set_kv(conn, "rty_ym_relative_value_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_F_RELATIVE_VALUE")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": RTY_YM_RELATIVE_VALUE_EXPERIMENT_ID,
                "experiment_type": "rty_ym_relative_value_pilot",
                "status": "QUEUED",
                "reason": (
                    "Behaviorally distinct two-leg residual with past-only beta, integer micro "
                    "sizing, synchronized fills and explicit two-leg costs."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_ym_shared_risk_off(self, conn: Any) -> bool:
        existing = experiment_record(conn, YM_SHARED_RISK_OFF_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_ym_shared_risk_off_overlay_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        parent_directory = project_path(
            "reports", "mission_experiments", EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID
        )
        parent_result_path = parent_directory / "equity_open_gap_continuation_result.json"
        parent_ledger_path = parent_directory / "equity_open_gap_continuation_trade_ledger.jsonl"
        root = Path("/root/hydra-bot")
        if not map_path.is_file():
            map_path = root / map_path.relative_to(project_path())
        if not parent_result_path.is_file():
            parent_result_path = (
                root
                / "reports"
                / "mission_experiments"
                / EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID
                / parent_result_path.name
            )
        if not parent_ledger_path.is_file():
            parent_ledger_path = (
                root
                / "reports"
                / "mission_experiments"
                / EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID
                / parent_ledger_path.name
            )
        source_valid = False
        if parent_result_path.is_file():
            try:
                parent_payload = json.loads(parent_result_path.read_text(encoding="utf-8"))
                source_valid = (
                    parent_payload.get("result_hash") == YM_SHARED_RISK_OFF_PARENT_RESULT_HASH
                )
            except (OSError, json.JSONDecodeError):
                source_valid = False
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest() != YM_SHARED_RISK_OFF_TASK_SHA256
            or not map_path.is_file()
            or hashlib.sha256(map_path.read_bytes()).hexdigest() != PATH_GEOMETRY_MAP_SHA256
            or not parent_result_path.is_file()
            or hashlib.sha256(parent_result_path.read_bytes()).hexdigest()
            != YM_SHARED_RISK_OFF_PARENT_RESULT_SHA256
            or not parent_ledger_path.is_file()
            or hashlib.sha256(parent_ledger_path.read_bytes()).hexdigest()
            != YM_SHARED_RISK_OFF_PARENT_LEDGER_SHA256
            or not source_valid
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "YM_SHARED_RISK_OFF_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "Frozen parent, defensive task or explicit map changed.")
            return False
        specification = {
            "experiment_type": "ym_shared_risk_off_overlay",
            "priority": 105.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": YM_SHARED_RISK_OFF_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "source_parent_result_path": str(parent_result_path),
            "source_parent_result_sha256": YM_SHARED_RISK_OFF_PARENT_RESULT_SHA256,
            "source_parent_result_hash": YM_SHARED_RISK_OFF_PARENT_RESULT_HASH,
            "source_parent_trade_ledger_path": str(parent_ledger_path),
            "source_parent_trade_ledger_sha256": YM_SHARED_RISK_OFF_PARENT_LEDGER_SHA256,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "q4_lineage_reuse_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.95,
        }
        enqueue_experiment(conn, YM_SHARED_RISK_OFF_EXPERIMENT_ID, specification)
        set_kv(conn, "ym_shared_risk_off_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_H_DEFENSIVE_PORTFOLIO")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": YM_SHARED_RISK_OFF_EXPERIMENT_ID,
                "experiment_type": "ym_shared_risk_off_overlay",
                "status": "QUEUED",
                "reason": (
                    "Causal shared-risk deactivation of the only current shadow/Topstep child; "
                    "it tests drawdown and MLL uncertainty without adding exposure or touching Q4."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_qd_economic_tournament(self, conn: Any) -> bool:
        """Queue the frozen selector-v2 tournament without inspecting 2024 outcomes."""
        existing = experiment_record(conn, QD_ECONOMIC_TOURNAMENT_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_qd_economic_tournament_20260711.md"
        )
        selector_task = project_path(
            "reports", "engineering", "hydra_qd_selector_v2_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        root = Path("/root/hydra-bot")
        if not map_path.is_file():
            map_path = root / "data/cache/contract_maps" / map_path.name
        contracts = (
            (task, QD_ECONOMIC_TOURNAMENT_TASK_SHA256, "tournament task"),
            (selector_task, QD_SELECTOR_V2_TASK_SHA256, "selector v2 task"),
            (map_path, PATH_GEOMETRY_MAP_SHA256, "explicit-contract map"),
        )
        mismatch = [
            label
            for path, expected, label in contracts
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatch:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "QD_TOURNAMENT_FROZEN_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen quality-diversity inputs changed: {', '.join(mismatch)}.",
            )
            return False
        specification = {
            "experiment_type": "qd_economic_tournament",
            "priority": 110.0,
            "max_attempts": 2,
            "engineering_task_path": str(task),
            "engineering_task_sha256": QD_ECONOMIC_TOURNAMENT_TASK_SHA256,
            "selector_task_path": str(selector_task),
            "selector_task_sha256": QD_SELECTOR_V2_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "pipeline": "PROMOTION",
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "selection_data_end_exclusive": "2024-01-01",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.99,
        }
        enqueue_experiment(
            conn, QD_ECONOMIC_TOURNAMENT_EXPERIMENT_ID, specification
        )
        set_kv(conn, "qd_economic_tournament_plan_written", True)
        set_kv(conn, "foundry_current_engine", "ENGINE_J_QUALITY_DIVERSITY")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": QD_ECONOMIC_TOURNAMENT_EXPERIMENT_ID,
                "experiment_type": "qd_economic_tournament",
                "pipeline": "PROMOTION",
                "status": "QUEUED",
                "reason": (
                    "Selector v2 freezes the maximum feasible diversified 2023 elite set "
                    "before unchanged 2024 Q1-Q3 promotion; Q4 and network access stay closed."
                ),
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_ym_strict_promotion(self, conn: Any) -> bool:
        existing = experiment_record(conn, YM_STRICT_PROMOTION_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_ym_strict_promotion_replay_20260711.md"
        )
        map_path = project_path(
            "data", "cache", "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        parent_dir = project_path(
            "reports", "mission_experiments", EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID
        )
        parent_result = parent_dir / "equity_open_gap_continuation_result.json"
        parent_ledger = parent_dir / "equity_open_gap_continuation_trade_ledger.jsonl"
        shadow_configuration = (
            parent_dir / "shadow_configurations" / f"strategy_open_gap_continuation_YM_v1.json"
        )
        freeze_manifest = project_path(
            "reports", "mission_experiments", Q4_CANDIDATE_FREEZE_EXPERIMENT_ID,
            "q4_freeze_manifest_strategy_open_gap_continuation_YM_v1.json",
        )
        root = Path("/root/hydra-bot")
        fallbacks = {
            "map": root / "data/cache/contract_maps" / map_path.name,
            "parent_result": root / "reports/mission_experiments" / EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID / parent_result.name,
            "parent_ledger": root / "reports/mission_experiments" / EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID / parent_ledger.name,
            "shadow": root / "reports/mission_experiments" / EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID / "shadow_configurations" / shadow_configuration.name,
            "freeze": root / "reports/mission_experiments" / Q4_CANDIDATE_FREEZE_EXPERIMENT_ID / freeze_manifest.name,
        }
        if not map_path.is_file():
            map_path = fallbacks["map"]
        if not parent_result.is_file():
            parent_result = fallbacks["parent_result"]
        if not parent_ledger.is_file():
            parent_ledger = fallbacks["parent_ledger"]
        if not shadow_configuration.is_file():
            shadow_configuration = fallbacks["shadow"]
        if not freeze_manifest.is_file():
            freeze_manifest = fallbacks["freeze"]
        contracts = (
            (task, YM_STRICT_PROMOTION_TASK_SHA256, "strict task"),
            (map_path, PATH_GEOMETRY_MAP_SHA256, "explicit map"),
            (parent_result, YM_SHARED_RISK_OFF_PARENT_RESULT_SHA256, "parent result"),
            (parent_ledger, YM_SHARED_RISK_OFF_PARENT_LEDGER_SHA256, "parent ledger"),
            (freeze_manifest, YM_FREEZE_MANIFEST_SHA256, "freeze manifest"),
            (shadow_configuration, YM_SHADOW_CONFIGURATION_SHA256, "shadow configuration"),
        )
        mismatch = [
            label for path, expected, label in contracts
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatch:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "YM_STRICT_PROMOTION_SOURCE_MISMATCH")
            set_kv(conn, "last_error", f"Frozen YM strict sources changed: {', '.join(mismatch)}.")
            return False
        specification = {
            "experiment_type": "ym_open_gap_strict_promotion",
            "priority": 109.0,
            "max_attempts": 2,
            "pipeline": "PROMOTION",
            "engineering_task_path": str(task),
            "engineering_task_sha256": YM_STRICT_PROMOTION_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "source_parent_result_path": str(parent_result),
            "source_parent_result_sha256": YM_SHARED_RISK_OFF_PARENT_RESULT_SHA256,
            "source_parent_result_hash": YM_SHARED_RISK_OFF_PARENT_RESULT_HASH,
            "source_parent_trade_ledger_path": str(parent_ledger),
            "source_parent_trade_ledger_sha256": YM_SHARED_RISK_OFF_PARENT_LEDGER_SHA256,
            "source_freeze_manifest_path": str(freeze_manifest),
            "source_freeze_manifest_sha256": YM_FREEZE_MANIFEST_SHA256,
            "source_freeze_manifest_hash": YM_FREEZE_MANIFEST_HASH,
            "source_shadow_configuration_path": str(shadow_configuration),
            "source_shadow_configuration_sha256": YM_SHADOW_CONFIGURATION_SHA256,
            "source_shadow_configuration_hash": YM_SHADOW_CONFIGURATION_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.98,
        }
        enqueue_experiment(conn, YM_STRICT_PROMOTION_EXPERIMENT_ID, specification)
        set_kv(conn, "ym_strict_promotion_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "QUEUED")
        set_kv(
            conn,
            "current_research_experiment_selected",
            {
                "experiment": YM_STRICT_PROMOTION_EXPERIMENT_ID,
                "experiment_type": "ym_open_gap_strict_promotion",
                "pipeline": "PROMOTION",
                "status": "QUEUED",
                "reason": "Resolve temporal concentration and zero-order shadow safety of the exact frozen YM parent.",
            },
        )
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_ym_shadow_activation(self, conn: Any) -> bool:
        existing = experiment_record(conn, YM_SHADOW_ACTIVATION_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        strict_record = experiment_record(conn, YM_STRICT_PROMOTION_EXPERIMENT_ID)
        if strict_record is None or strict_record.get("status") != "COMPLETED":
            return False
        strict_result = dict(strict_record.get("result") or {})
        if (
            not bool(strict_result.get("shadow_activation_eligible"))
            or list(strict_result.get("hard_invalidations") or [])
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "YM_SHADOW_ACTIVATION_NOT_AUTHORIZED")
            set_kv(conn, "last_error", "Frozen strict result prohibits shadow activation.")
            return False
        task = project_path(
            "reports", "engineering", "hydra_ym_shadow_activation_20260711.md"
        )
        strict_path = Path(
            str((strict_result.get("artifacts") or {}).get("result_json_path") or "")
        )
        shadow_configuration = project_path(
            "reports", "mission_experiments", EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID,
            "shadow_configurations", "strategy_open_gap_continuation_YM_v1.json",
        )
        root = Path("/root/hydra-bot")
        if not strict_path.is_file():
            strict_path = root / "reports/mission_experiments" / YM_STRICT_PROMOTION_EXPERIMENT_ID / "ym_strict_promotion_result.json"
        if not shadow_configuration.is_file():
            shadow_configuration = root / "reports/mission_experiments" / EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID / "shadow_configurations" / shadow_configuration.name
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest() != YM_SHADOW_ACTIVATION_TASK_SHA256
            or not strict_path.is_file()
            or not shadow_configuration.is_file()
            or hashlib.sha256(shadow_configuration.read_bytes()).hexdigest()
            != YM_SHADOW_CONFIGURATION_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "YM_SHADOW_ACTIVATION_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "Strict result, activation task or shadow configuration changed.")
            return False
        specification = {
            "experiment_type": "ym_immutable_shadow_activation",
            "priority": 108.0,
            "max_attempts": 2,
            "pipeline": "SHADOW",
            "engineering_task_path": str(task),
            "engineering_task_sha256": YM_SHADOW_ACTIVATION_TASK_SHA256,
            "strict_result_path": str(strict_path),
            "strict_result_sha256": hashlib.sha256(strict_path.read_bytes()).hexdigest(),
            "strict_result_hash": str(strict_result["result_hash"]),
            "shadow_configuration_path": str(shadow_configuration),
            "shadow_configuration_sha256": YM_SHADOW_CONFIGURATION_SHA256,
            "shadow_configuration_hash": YM_SHADOW_CONFIGURATION_HASH,
            "code_commit": self._git_commit(),
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.99,
        }
        enqueue_experiment(conn, YM_SHADOW_ACTIVATION_EXPERIMENT_ID, specification)
        set_kv(conn, "ym_shadow_activation_plan_written", True)
        set_kv(conn, "shadow_pipeline_status", "ACTIVATION_QUEUED")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_accelerated_context_tournament(self, conn: Any) -> bool:
        existing = experiment_record(conn, ACCELERATED_CONTEXT_TOURNAMENT_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_accelerated_context_tournament_20260711.md"
        )
        selector_task = project_path(
            "reports", "engineering", "hydra_qd_selector_v2_20260711.md"
        )
        map_path = project_path(
            "data", "cache", "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot/data/cache/contract_maps") / map_path.name
        contracts = (
            (task, ACCELERATED_CONTEXT_TASK_SHA256, "accelerated task"),
            (selector_task, QD_SELECTOR_V2_TASK_SHA256, "selector task"),
            (map_path, PATH_GEOMETRY_MAP_SHA256, "explicit map"),
        )
        mismatch = [
            label for path, expected, label in contracts
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatch:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "ACCELERATED_CONTEXT_SOURCE_MISMATCH")
            set_kv(conn, "last_error", f"Frozen accelerated sources changed: {', '.join(mismatch)}.")
            return False
        specification = {
            "experiment_type": "accelerated_context_tournament",
            "priority": 107.0,
            "max_attempts": 2,
            "pipeline": "DISCOVERY",
            "engineering_task_path": str(task),
            "engineering_task_sha256": ACCELERATED_CONTEXT_TASK_SHA256,
            "selector_task_path": str(selector_task),
            "selector_task_sha256": QD_SELECTOR_V2_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.97,
        }
        enqueue_experiment(
            conn, ACCELERATED_CONTEXT_TOURNAMENT_EXPERIMENT_ID, specification
        )
        set_kv(conn, "accelerated_context_tournament_plan_written", True)
        set_kv(conn, "discovery_pipeline_status", "QUEUED")
        set_kv(conn, "foundry_current_engine", "PARALLEL_MULTI_ENGINE_CONTEXT_SEARCH")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_selection_null_power(self, conn: Any) -> bool:
        existing = experiment_record(conn, SELECTION_NULL_POWER_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports", "engineering", "hydra_selection_null_power_calibration_20260711.md"
        )
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != SELECTION_NULL_POWER_TASK_SHA256
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "SELECTION_NULL_POWER_TASK_MISMATCH")
            set_kv(conn, "last_error", "Frozen selection-null calibration task changed.")
            return False
        specification = {
            "experiment_type": "selection_null_power_calibration",
            "priority": 106.0,
            "max_attempts": 2,
            "pipeline": "PROMOTION_VALIDATOR",
            "engineering_task_path": str(task),
            "engineering_task_sha256": SELECTION_NULL_POWER_TASK_SHA256,
            "code_commit": self._git_commit(),
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.96,
        }
        enqueue_experiment(conn, SELECTION_NULL_POWER_EXPERIMENT_ID, specification)
        set_kv(conn, "selection_null_power_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "VALIDATOR_CALIBRATION_QUEUED")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_selection_null_policy_repair(self, conn: Any) -> bool:
        existing = experiment_record(conn, SELECTION_NULL_POLICY_REPAIR_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        source_record = experiment_record(conn, SELECTION_NULL_POWER_EXPERIMENT_ID)
        if source_record is None or source_record.get("status") != "COMPLETED":
            return False
        source = dict(source_record.get("result") or {})
        task = project_path(
            "reports", "engineering", "hydra_selection_null_policy_repair_v2_20260711.md"
        )
        source_path = Path(str((source.get("artifacts") or {}).get("result_json_path") or ""))
        if not source_path.is_file():
            source_path = Path("/root/hydra-bot/reports/mission_experiments") / SELECTION_NULL_POWER_EXPERIMENT_ID / "selection_null_power_result.json"
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != SELECTION_NULL_POLICY_REPAIR_TASK_SHA256
            or not source_path.is_file()
            or source.get("scientific_conclusion")
            != "SELECTION_NULL_POLICY_FALSE_POSITIVE_CONTROL_FAILED"
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "SELECTION_NULL_POLICY_REPAIR_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "Policy-repair task or calibration v1 source changed.")
            return False
        specification = {
            "experiment_type": "selection_null_policy_repair",
            "priority": 105.5,
            "max_attempts": 2,
            "pipeline": "PROMOTION_VALIDATOR",
            "engineering_task_path": str(task),
            "engineering_task_sha256": SELECTION_NULL_POLICY_REPAIR_TASK_SHA256,
            "source_calibration_result_path": str(source_path),
            "source_calibration_result_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "source_calibration_result_hash": str(source["result_hash"]),
            "code_commit": self._git_commit(),
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.99,
        }
        enqueue_experiment(
            conn, SELECTION_NULL_POLICY_REPAIR_EXPERIMENT_ID, specification
        )
        set_kv(conn, "selection_null_policy_repair_plan_written", True)
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_single_primary_alpha(self, conn: Any) -> bool:
        existing = experiment_record(conn, SINGLE_PRIMARY_ALPHA_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        source_record = experiment_record(conn, SELECTION_NULL_POLICY_REPAIR_EXPERIMENT_ID)
        if source_record is None or source_record.get("status") != "COMPLETED":
            return False
        source = dict(source_record.get("result") or {})
        task = project_path(
            "reports", "engineering", "hydra_single_primary_alpha_calibration_v3_20260711.md"
        )
        source_path = Path(str((source.get("artifacts") or {}).get("result_json_path") or ""))
        if not source_path.is_file():
            source_path = Path("/root/hydra-bot/reports/mission_experiments") / SELECTION_NULL_POLICY_REPAIR_EXPERIMENT_ID / "selection_null_policy_repair_result.json"
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest() != SINGLE_PRIMARY_ALPHA_TASK_SHA256
            or not source_path.is_file()
            or source.get("scientific_conclusion")
            != "NO_PROSPECTIVE_POLICY_MET_BOTH_FPR_AND_POWER"
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "SINGLE_PRIMARY_ALPHA_SOURCE_MISMATCH")
            set_kv(conn, "last_error", "Single-primary task or policy v2 source changed.")
            return False
        specification = {
            "experiment_type": "single_primary_alpha_calibration",
            "priority": 105.0,
            "max_attempts": 2,
            "pipeline": "PROMOTION_VALIDATOR",
            "engineering_task_path": str(task),
            "engineering_task_sha256": SINGLE_PRIMARY_ALPHA_TASK_SHA256,
            "source_policy_repair_result_path": str(source_path),
            "source_policy_repair_result_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "source_policy_repair_result_hash": str(source["result_hash"]),
            "code_commit": self._git_commit(),
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.995,
        }
        enqueue_experiment(conn, SINGLE_PRIMARY_ALPHA_EXPERIMENT_ID, specification)
        set_kv(conn, "single_primary_alpha_plan_written", True)
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_single_primary_context_tournament(self, conn: Any) -> bool:
        existing = experiment_record(
            conn, SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID
        )
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        policy_record = experiment_record(conn, SINGLE_PRIMARY_ALPHA_EXPERIMENT_ID)
        if policy_record is None or policy_record.get("status") != "COMPLETED":
            return False
        policy = dict(policy_record.get("result") or {})
        task = project_path(
            "reports",
            "engineering",
            "hydra_single_primary_context_tournament_20260711.md",
        )
        selector_task = project_path(
            "reports", "engineering", "hydra_qd_selector_v2_20260711.md"
        )
        policy_path = Path(
            str((policy.get("artifacts") or {}).get("result_json_path") or "")
        )
        if not policy_path.is_file():
            policy_path = (
                Path("/root/hydra-bot/reports/mission_experiments")
                / SINGLE_PRIMARY_ALPHA_EXPERIMENT_ID
                / "single_primary_alpha_result.json"
            )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot/data/cache/contract_maps") / map_path.name
        contracts = (
            (task, SINGLE_PRIMARY_CONTEXT_TASK_SHA256, "single-primary task"),
            (selector_task, QD_SELECTOR_V2_TASK_SHA256, "selector task"),
            (map_path, PATH_GEOMETRY_MAP_SHA256, "explicit map"),
        )
        mismatch = [
            label
            for path, expected, label in contracts
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        policy_valid = bool(
            policy_path.is_file()
            and policy.get("scientific_conclusion")
            == "SINGLE_PRIMARY_ALPHA_CALIBRATED"
            and policy.get("calibration_passed")
            and float(policy.get("selected_alpha") or 0.0) == 0.03
            and int(
                (policy.get("prospective_policy_contract") or {}).get(
                    "promotion_primary_count", -1
                )
            )
            == 1
        )
        if mismatch or not policy_valid:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "SINGLE_PRIMARY_CONTEXT_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                "Frozen single-primary sources changed: "
                + ", ".join(mismatch or ["calibrated policy"]),
            )
            return False
        specification = {
            "experiment_type": "single_primary_context_tournament",
            "priority": 104.9,
            "max_attempts": 2,
            "pipeline": "PROMOTION",
            "engineering_task_path": str(task),
            "engineering_task_sha256": SINGLE_PRIMARY_CONTEXT_TASK_SHA256,
            "selector_task_path": str(selector_task),
            "selector_task_sha256": QD_SELECTOR_V2_TASK_SHA256,
            "calibrated_policy_result_path": str(policy_path),
            "calibrated_policy_result_sha256": hashlib.sha256(
                policy_path.read_bytes()
            ).hexdigest(),
            "calibrated_policy_result_hash": str(policy["result_hash"]),
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.995,
        }
        enqueue_experiment(
            conn, SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID, specification
        )
        set_kv(conn, "single_primary_context_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "SINGLE_PRIMARY_CONTEXT_QUEUED")
        set_kv(conn, "foundry_current_engine", "CALIBRATED_SINGLE_PRIMARY_CONTEXT")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_counterfactual_hazard_primary(self, conn: Any) -> bool:
        existing = experiment_record(conn, COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(
            conn, SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID
        )
        predecessor_result = dict((predecessor or {}).get("result") or {})
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or predecessor_result.get("scientific_conclusion")
            != "SINGLE_PRIMARY_CONTEXT_CONFIRMATION_FALSIFIED_OR_INSUFFICIENT"
        ):
            return False
        task = project_path(
            "reports",
            "engineering",
            "hydra_counterfactual_hazard_primary_20260711.md",
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot/data/cache/contract_maps") / map_path.name
        contracts = (
            (task, COUNTERFACTUAL_HAZARD_TASK_SHA256, "counterfactual task"),
            (map_path, PATH_GEOMETRY_MAP_SHA256, "explicit map"),
        )
        mismatch = [
            label
            for path, expected, label in contracts
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatch:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "COUNTERFACTUAL_HAZARD_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen counterfactual sources changed: {', '.join(mismatch)}.",
            )
            return False
        specification = {
            "experiment_type": "counterfactual_hazard_primary",
            "priority": 104.8,
            "max_attempts": 2,
            "pipeline": "PROMOTION_AND_DISCOVERY",
            "engineering_task_path": str(task),
            "engineering_task_sha256": COUNTERFACTUAL_HAZARD_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "source_experiment_id": SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID,
            "source_result_hash": str(predecessor_result.get("result_hash") or ""),
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.995,
        }
        enqueue_experiment(
            conn, COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID, specification
        )
        set_kv(conn, "counterfactual_hazard_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "COUNTERFACTUAL_HAZARD_QUEUED")
        set_kv(conn, "discovery_pipeline_status", "COUNTERFACTUAL_HAZARD_QUEUED")
        set_kv(conn, "foundry_current_engine", "COUNTERFACTUAL_HAZARD_MATCHING")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_barrier_hazard_primary(self, conn: Any) -> bool:
        existing = experiment_record(conn, BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(conn, COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID)
        predecessor_result = dict((predecessor or {}).get("result") or {})
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or predecessor_result.get("scientific_conclusion")
            != "COUNTERFACTUAL_HAZARD_NO_EARLY_PRIMARY"
        ):
            return False
        task = project_path(
            "reports", "engineering", "hydra_barrier_hazard_primary_20260711.md"
        )
        map_path = project_path(
            "data",
            "cache",
            "contract_maps",
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json",
        )
        if not map_path.is_file():
            map_path = Path("/root/hydra-bot/data/cache/contract_maps") / map_path.name
        contracts = (
            (task, BARRIER_HAZARD_TASK_SHA256, "barrier task"),
            (map_path, PATH_GEOMETRY_MAP_SHA256, "explicit map"),
        )
        mismatch = [
            label
            for path, expected, label in contracts
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatch:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "BARRIER_HAZARD_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen barrier sources changed: {', '.join(mismatch)}.",
            )
            return False
        specification = {
            "experiment_type": "barrier_hazard_primary",
            "priority": 104.7,
            "max_attempts": 2,
            "pipeline": "PROMOTION_AND_DISCOVERY",
            "engineering_task_path": str(task),
            "engineering_task_sha256": BARRIER_HAZARD_TASK_SHA256,
            "repaired_map_path": str(map_path),
            "repaired_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "repaired_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "source_experiment_id": COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID,
            "source_result_hash": str(predecessor_result.get("result_hash") or ""),
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.995,
        }
        enqueue_experiment(conn, BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID, specification)
        set_kv(conn, "barrier_hazard_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "BARRIER_HAZARD_QUEUED")
        set_kv(conn, "discovery_pipeline_status", "BARRIER_HAZARD_QUEUED")
        set_kv(conn, "foundry_current_engine", "DISTRIBUTIONAL_BARRIER_HAZARD")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_barrier_shadow_activation(self, conn: Any) -> bool:
        existing = experiment_record(
            conn, BARRIER_HAZARD_SHADOW_ACTIVATION_EXPERIMENT_ID
        )
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        source_record = experiment_record(conn, BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID)
        source = dict((source_record or {}).get("result") or {})
        candidate_id = str(source.get("primary_candidate_id") or "")
        candidates = [
            item
            for item in source.get("candidates") or []
            if str(item.get("candidate_id") or "") == candidate_id
        ]
        configurations = [
            item
            for item in source.get("shadow_configurations") or []
            if str(item.get("candidate_id") or "") == candidate_id
        ]
        if (
            (source_record or {}).get("status") != "COMPLETED"
            or source.get("scientific_conclusion")
            != "BARRIER_HAZARD_SHADOW_CANDIDATE_FOUND"
            or len(candidates) != 1
            or candidates[0].get("status") != "SHADOW_RESEARCH_CANDIDATE"
            or not bool(
                (candidates[0].get("admission") or {}).get(
                    "permits_zero_risk_shadow"
                )
            )
            or len(configurations) != 1
        ):
            return False
        task = project_path(
            "reports",
            "engineering",
            "hydra_barrier_shadow_activation_20260711.md",
        )
        source_path = Path(
            str((source.get("artifacts") or {}).get("result_json_path") or "")
        )
        if not source_path.is_file():
            source_path = (
                Path("/root/hydra-bot/reports/mission_experiments")
                / BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID
                / "barrier_hazard_result.json"
            )
        configuration = configurations[0]
        configuration_path = Path(str(configuration.get("path") or ""))
        if not configuration_path.is_file():
            configuration_path = (
                Path("/root/hydra-bot/reports/mission_experiments")
                / BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID
                / "shadow_configurations"
                / f"{candidate_id}.json"
            )
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != BARRIER_SHADOW_ACTIVATION_TASK_SHA256
            or not source_path.is_file()
            or not configuration_path.is_file()
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "BARRIER_SHADOW_ACTIVATION_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                "Barrier source result, activation task or configuration changed.",
            )
            return False
        specification = {
            "experiment_type": "immutable_shadow_activation",
            "priority": 110.0,
            "max_attempts": 2,
            "pipeline": "SHADOW",
            "engineering_task_path": str(task),
            "engineering_task_sha256": BARRIER_SHADOW_ACTIVATION_TASK_SHA256,
            "source_result_path": str(source_path),
            "source_result_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "source_result_hash": str(source["result_hash"]),
            "candidate_id": candidate_id,
            "shadow_configuration_path": str(configuration_path),
            "shadow_configuration_sha256": hashlib.sha256(
                configuration_path.read_bytes()
            ).hexdigest(),
            "shadow_configuration_hash": str(configuration["configuration_hash"]),
            "code_commit": self._git_commit(),
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 1.0,
        }
        enqueue_experiment(
            conn, BARRIER_HAZARD_SHADOW_ACTIVATION_EXPERIMENT_ID, specification
        )
        set_kv(conn, "barrier_shadow_activation_plan_written", True)
        set_kv(conn, "shadow_pipeline_status", "ACTIVATION_QUEUED")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_energy_metals_barrier_primary(self, conn: Any) -> bool:
        existing = experiment_record(
            conn, ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID
        )
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        task = project_path(
            "reports",
            "engineering",
            "hydra_energy_metals_barrier_primary_20260711.md",
        )
        cache_root = project_path("data", "cache")
        energy_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_RTY_M2K_YM_MYM_GC_MGC_CL_MCL_"
            "2023-01-01_2024-10-01.parquet"
        )
        energy_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
        )
        metals_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_GC-v-0_MGC-v-0_"
            "2023-01-01_2024-10-01.parquet"
        )
        metals_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_01ba149449a494a7.json"
        )
        main_cache = Path("/root/hydra-bot/data/cache")
        for name, path in (
            ("energy_data", energy_data),
            ("energy_map", energy_map),
            ("metals_data", metals_data),
            ("metals_map", metals_map),
        ):
            if path.is_file():
                continue
            fallback = main_cache / path.relative_to(cache_root)
            if name == "energy_data":
                energy_data = fallback
            elif name == "energy_map":
                energy_map = fallback
            elif name == "metals_data":
                metals_data = fallback
            else:
                metals_map = fallback
        frozen = (
            (task, ENERGY_METALS_BARRIER_TASK_SHA256, "engineering task"),
            (energy_data, ENERGY_METALS_DATA_SHA256, "energy data"),
            (energy_map, PATH_GEOMETRY_MAP_SHA256, "energy map"),
            (metals_data, ENERGY_METALS_VOLUME_DATA_SHA256, "metals data"),
            (metals_map, ENERGY_METALS_VOLUME_MAP_SHA256, "metals map"),
        )
        mismatches = [
            label
            for path, expected, label in frozen
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatches:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "ENERGY_METALS_FROZEN_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen energy/metals sources changed: {', '.join(mismatches)}.",
            )
            return False
        specification = {
            "experiment_type": "energy_metals_barrier_primary",
            "priority": 105.0,
            "max_attempts": 2,
            "pipeline": "PROMOTION_AND_DISCOVERY",
            "engineering_task_path": str(task),
            "engineering_task_sha256": ENERGY_METALS_BARRIER_TASK_SHA256,
            "energy_data_path": str(energy_data),
            "energy_data_sha256": ENERGY_METALS_DATA_SHA256,
            "energy_map_path": str(energy_map),
            "energy_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "energy_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "metals_data_path": str(metals_data),
            "metals_data_sha256": ENERGY_METALS_VOLUME_DATA_SHA256,
            "metals_map_path": str(metals_map),
            "metals_map_sha256": ENERGY_METALS_VOLUME_MAP_SHA256,
            "metals_roll_map_hash": ENERGY_METALS_VOLUME_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.998,
        }
        enqueue_experiment(
            conn, ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID, specification
        )
        set_kv(conn, "energy_metals_barrier_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "ENERGY_METALS_BARRIER_QUEUED")
        set_kv(conn, "discovery_pipeline_status", "ENERGY_METALS_BARRIER_QUEUED")
        set_kv(conn, "foundry_current_engine", "ENERGY_METALS_BARRIER_HAZARD")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_energy_metals_session_geometry(self, conn: Any) -> bool:
        existing = experiment_record(
            conn, ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID
        )
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(
            conn, ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID
        )
        if (predecessor or {}).get("status") != "COMPLETED":
            return False
        task = project_path(
            "reports",
            "engineering",
            "hydra_energy_metals_session_geometry_primary_20260711.md",
        )
        cache_root = project_path("data", "cache")
        energy_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_RTY_M2K_YM_MYM_GC_MGC_CL_MCL_"
            "2023-01-01_2024-10-01.parquet"
        )
        energy_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
        )
        metals_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_GC-v-0_MGC-v-0_"
            "2023-01-01_2024-10-01.parquet"
        )
        metals_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_01ba149449a494a7.json"
        )
        main_cache = Path("/root/hydra-bot/data/cache")
        resolved = []
        for path in (energy_data, energy_map, metals_data, metals_map):
            resolved.append(
                path
                if path.is_file()
                else main_cache / path.relative_to(cache_root)
            )
        energy_data, energy_map, metals_data, metals_map = resolved
        frozen = (
            (task, ENERGY_METALS_SESSION_GEOMETRY_TASK_SHA256, "engineering task"),
            (energy_data, ENERGY_METALS_DATA_SHA256, "energy data"),
            (energy_map, PATH_GEOMETRY_MAP_SHA256, "energy map"),
            (metals_data, ENERGY_METALS_VOLUME_DATA_SHA256, "metals data"),
            (metals_map, ENERGY_METALS_VOLUME_MAP_SHA256, "metals map"),
        )
        mismatches = [
            label
            for path, expected, label in frozen
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatches:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(
                conn, "current_blocker", "SESSION_GEOMETRY_FROZEN_SOURCE_MISMATCH"
            )
            set_kv(
                conn,
                "last_error",
                f"Frozen session-geometry sources changed: {', '.join(mismatches)}.",
            )
            return False
        specification = {
            "experiment_type": "energy_metals_session_geometry_primary",
            "priority": 105.2,
            "max_attempts": 2,
            "pipeline": "PROMOTION_AND_DISCOVERY",
            "engineering_task_path": str(task),
            "engineering_task_sha256": ENERGY_METALS_SESSION_GEOMETRY_TASK_SHA256,
            "energy_data_path": str(energy_data),
            "energy_data_sha256": ENERGY_METALS_DATA_SHA256,
            "energy_map_path": str(energy_map),
            "energy_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "energy_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "metals_data_path": str(metals_data),
            "metals_data_sha256": ENERGY_METALS_VOLUME_DATA_SHA256,
            "metals_map_path": str(metals_map),
            "metals_map_sha256": ENERGY_METALS_VOLUME_MAP_SHA256,
            "metals_roll_map_hash": ENERGY_METALS_VOLUME_ROLL_HASH,
            "source_experiment_id": ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID,
            "source_result_hash": str(
                ((predecessor or {}).get("result") or {}).get("result_hash") or ""
            ),
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.999,
        }
        enqueue_experiment(
            conn, ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID, specification
        )
        set_kv(conn, "energy_metals_session_geometry_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "SESSION_GEOMETRY_QUEUED")
        set_kv(conn, "discovery_pipeline_status", "SESSION_GEOMETRY_QUEUED")
        set_kv(conn, "foundry_current_engine", "ENERGY_METALS_SESSION_GEOMETRY")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_session_geometry_micro_repair(self, conn: Any) -> bool:
        existing = experiment_record(conn, SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(
            conn, ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID
        )
        source = dict((predecessor or {}).get("result") or {})
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or source.get("result_hash") != SESSION_GEOMETRY_PARENT_RESULT_HASH
            or source.get("scientific_conclusion")
            != "ENERGY_METALS_SESSION_GEOMETRY_PROMISING_BUT_INSUFFICIENT"
        ):
            return False
        source_root = Path(
            "/root/hydra-bot/reports/mission_experiments/"
            "energy_metals_session_geometry_primary_v1"
        )
        source_result = source_root / "session_geometry_result.json"
        source_manifest = source_root / "session_geometry_primary_freeze.json"
        source_ledger = source_root / "session_geometry_trade_ledger.jsonl"
        task = project_path(
            "reports",
            "engineering",
            "hydra_session_geometry_micro_execution_repair_20260711.md",
        )
        cache_root = project_path("data", "cache")
        energy_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_RTY_M2K_YM_MYM_GC_MGC_CL_MCL_"
            "2023-01-01_2024-10-01.parquet"
        )
        energy_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
        )
        if not energy_data.is_file():
            energy_data = Path("/root/hydra-bot/data/cache/databento") / energy_data.name
        if not energy_map.is_file():
            energy_map = Path("/root/hydra-bot/data/cache/contract_maps") / energy_map.name
        frozen = (
            (task, SESSION_GEOMETRY_MICRO_REPAIR_TASK_SHA256, "engineering task"),
            (source_result, SESSION_GEOMETRY_PARENT_RESULT_SHA256, "parent result"),
            (
                source_manifest,
                SESSION_GEOMETRY_PARENT_MANIFEST_SHA256,
                "parent manifest",
            ),
            (source_ledger, SESSION_GEOMETRY_PARENT_LEDGER_SHA256, "parent ledger"),
            (energy_data, ENERGY_METALS_DATA_SHA256, "energy data"),
            (energy_map, PATH_GEOMETRY_MAP_SHA256, "energy map"),
        )
        mismatches = [
            label
            for path, expected, label in frozen
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatches:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "MICRO_EXECUTION_REPAIR_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen micro-execution sources changed: {', '.join(mismatches)}.",
            )
            return False
        specification = {
            "experiment_type": "session_geometry_micro_execution_repair",
            "priority": 106.0,
            "max_attempts": 2,
            "pipeline": "PROMOTION_AND_DISCOVERY",
            "engineering_task_path": str(task),
            "engineering_task_sha256": SESSION_GEOMETRY_MICRO_REPAIR_TASK_SHA256,
            "source_result_path": str(source_result),
            "source_result_sha256": SESSION_GEOMETRY_PARENT_RESULT_SHA256,
            "source_result_hash": SESSION_GEOMETRY_PARENT_RESULT_HASH,
            "source_manifest_path": str(source_manifest),
            "source_manifest_sha256": SESSION_GEOMETRY_PARENT_MANIFEST_SHA256,
            "source_manifest_hash": SESSION_GEOMETRY_PARENT_MANIFEST_HASH,
            "source_trade_ledger_path": str(source_ledger),
            "source_trade_ledger_sha256": SESSION_GEOMETRY_PARENT_LEDGER_SHA256,
            "energy_data_path": str(energy_data),
            "energy_data_sha256": ENERGY_METALS_DATA_SHA256,
            "energy_map_path": str(energy_map),
            "energy_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "energy_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.999,
        }
        enqueue_experiment(conn, SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID, specification)
        set_kv(conn, "session_geometry_micro_repair_plan_written", True)
        set_kv(conn, "promotion_pipeline_status", "MICRO_EXECUTION_REPAIR_QUEUED")
        set_kv(conn, "foundry_current_engine", "SYNCHRONIZED_MICRO_EXECUTION_REPAIR")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_session_geometry_micro_shadow(self, conn: Any) -> bool:
        existing = experiment_record(conn, SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(conn, SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID)
        source = dict((predecessor or {}).get("result") or {})
        candidates = [
            item
            for item in source.get("candidates") or []
            if item.get("candidate_id") == SESSION_GEOMETRY_MICRO_CHILD_ID
        ]
        configurations = [
            item
            for item in source.get("shadow_configurations") or []
            if item.get("candidate_id") == SESSION_GEOMETRY_MICRO_CHILD_ID
        ]
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or source.get("scientific_conclusion")
            != "SYNCHRONIZED_MCL_EXECUTION_SHADOW_CANDIDATE_FOUND"
            or len(candidates) != 1
            or candidates[0].get("status") != "SHADOW_RESEARCH_CANDIDATE"
            or not bool(
                (candidates[0].get("admission") or {}).get(
                    "permits_zero_risk_shadow"
                )
            )
            or len(configurations) != 1
        ):
            return False
        task = project_path(
            "reports",
            "engineering",
            "hydra_session_geometry_micro_shadow_activation_20260711.md",
        )
        source_path = Path(
            str((source.get("artifacts") or {}).get("result_json_path") or "")
        )
        configuration_path = Path(str(configurations[0].get("path") or ""))
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != SESSION_GEOMETRY_MICRO_SHADOW_TASK_SHA256
            or not source_path.is_file()
            or not configuration_path.is_file()
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "MICRO_SHADOW_ACTIVATION_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                "Synchronized-MCL source, activation task or configuration changed.",
            )
            return False
        specification = {
            "experiment_type": "session_geometry_micro_shadow_activation",
            "priority": 110.0,
            "max_attempts": 2,
            "pipeline": "SHADOW",
            "engineering_task_path": str(task),
            "engineering_task_sha256": SESSION_GEOMETRY_MICRO_SHADOW_TASK_SHA256,
            "source_result_path": str(source_path),
            "source_result_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "source_result_hash": str(source["result_hash"]),
            "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
            "shadow_configuration_path": str(configuration_path),
            "shadow_configuration_sha256": hashlib.sha256(
                configuration_path.read_bytes()
            ).hexdigest(),
            "shadow_configuration_hash": str(configurations[0]["configuration_hash"]),
            "code_commit": self._git_commit(),
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 1.0,
        }
        enqueue_experiment(conn, SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID, specification)
        set_kv(conn, "session_geometry_micro_shadow_plan_written", True)
        set_kv(conn, "shadow_pipeline_status", "ACTIVATION_QUEUED")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_gc_session_geometry_fresh(self, conn: Any) -> bool:
        existing = experiment_record(conn, GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(
            conn, SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID
        )
        predecessor_result = dict((predecessor or {}).get("result") or {})
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or predecessor_result.get("scientific_conclusion")
            != "IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED"
            or predecessor_result.get("candidate_id")
            != SESSION_GEOMETRY_MICRO_CHILD_ID
        ):
            return False
        source_root = Path(
            "/root/hydra-bot/reports/mission_experiments/"
            "energy_metals_session_geometry_primary_v1"
        )
        source_preregistration = source_root / "session_geometry_preregistration.json"
        source_freeze = source_root / "session_geometry_primary_freeze.json"
        task = project_path(
            "reports",
            "engineering",
            "hydra_gc_session_geometry_fresh_primary_20260711.md",
        )
        cache_root = project_path("data", "cache")
        metals_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_GC-v-0_MGC-v-0_"
            "2023-01-01_2024-10-01.parquet"
        )
        metals_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_01ba149449a494a7.json"
        )
        if not metals_data.is_file():
            metals_data = Path("/root/hydra-bot/data/cache/databento") / metals_data.name
        if not metals_map.is_file():
            metals_map = Path("/root/hydra-bot/data/cache/contract_maps") / metals_map.name
        frozen = (
            (task, GC_SESSION_GEOMETRY_FRESH_TASK_SHA256, "engineering task"),
            (
                source_preregistration,
                SESSION_GEOMETRY_SOURCE_PREREGISTRATION_SHA256,
                "source preregistration",
            ),
            (
                source_freeze,
                SESSION_GEOMETRY_PARENT_MANIFEST_SHA256,
                "source freeze",
            ),
            (metals_data, ENERGY_METALS_VOLUME_DATA_SHA256, "metals data"),
            (metals_map, ENERGY_METALS_VOLUME_MAP_SHA256, "metals map"),
        )
        mismatches = [
            label
            for path, expected, label in frozen
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatches:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "GC_FRESH_PRIMARY_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen GC fresh-primary sources changed: {', '.join(mismatches)}.",
            )
            return False
        specification = {
            "experiment_type": "gc_session_geometry_fresh_primary",
            "priority": 105.0,
            "max_attempts": 2,
            "pipeline": "DISCOVERY_AND_PROMOTION",
            "engineering_task_path": str(task),
            "engineering_task_sha256": GC_SESSION_GEOMETRY_FRESH_TASK_SHA256,
            "source_preregistration_path": str(source_preregistration),
            "source_preregistration_sha256": (
                SESSION_GEOMETRY_SOURCE_PREREGISTRATION_SHA256
            ),
            "source_freeze_path": str(source_freeze),
            "source_freeze_sha256": SESSION_GEOMETRY_PARENT_MANIFEST_SHA256,
            "metals_data_path": str(metals_data),
            "metals_data_sha256": ENERGY_METALS_VOLUME_DATA_SHA256,
            "metals_map_path": str(metals_map),
            "metals_map_sha256": ENERGY_METALS_VOLUME_MAP_SHA256,
            "metals_roll_map_hash": ENERGY_METALS_VOLUME_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "selection_end_exclusive": "2024-01-01",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.997,
        }
        enqueue_experiment(conn, GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID, specification)
        set_kv(conn, "gc_session_geometry_fresh_plan_written", True)
        set_kv(conn, "discovery_pipeline_status", "GC_FRESH_PRIMARY_QUEUED")
        set_kv(conn, "promotion_pipeline_status", "GC_FRESH_PRIMARY_QUEUED")
        set_kv(conn, "foundry_current_engine", "GC_SESSION_GEOMETRY_FRESH_PRIMARY")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_cross_asset_daily(self, conn: Any) -> bool:
        existing = experiment_record(conn, CROSS_ASSET_DAILY_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(conn, GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID)
        predecessor_result = dict((predecessor or {}).get("result") or {})
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or predecessor_result.get("scientific_conclusion")
            != "GC_SESSION_GEOMETRY_FRESH_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
        ):
            return False
        task = project_path(
            "reports",
            "engineering",
            "hydra_cross_asset_daily_horizon_tournament_20260711.md",
        )
        cache_root = project_path("data", "cache")
        core_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_RTY_M2K_YM_MYM_GC_MGC_CL_MCL_"
            "2023-01-01_2024-10-01.parquet"
        )
        core_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
        )
        metals_data = cache_root / "databento" / (
            "GLBX-MDP3_ohlcv-1m_GC-v-0_MGC-v-0_"
            "2023-01-01_2024-10-01.parquet"
        )
        metals_map = cache_root / "contract_maps" / (
            "roll_map_GLBX-MDP3_ohlcv-1m_01ba149449a494a7.json"
        )
        fallbacks = {
            "core_data": Path("/root/hydra-bot/data/cache/databento")
            / core_data.name,
            "core_map": Path("/root/hydra-bot/data/cache/contract_maps")
            / core_map.name,
            "metals_data": Path("/root/hydra-bot/data/cache/databento")
            / metals_data.name,
            "metals_map": Path("/root/hydra-bot/data/cache/contract_maps")
            / metals_map.name,
        }
        core_data = core_data if core_data.is_file() else fallbacks["core_data"]
        core_map = core_map if core_map.is_file() else fallbacks["core_map"]
        metals_data = (
            metals_data if metals_data.is_file() else fallbacks["metals_data"]
        )
        metals_map = metals_map if metals_map.is_file() else fallbacks["metals_map"]
        frozen = (
            (task, CROSS_ASSET_DAILY_TASK_SHA256, "engineering task"),
            (core_data, ENERGY_METALS_DATA_SHA256, "core data"),
            (core_map, PATH_GEOMETRY_MAP_SHA256, "core map"),
            (metals_data, ENERGY_METALS_VOLUME_DATA_SHA256, "metals data"),
            (metals_map, ENERGY_METALS_VOLUME_MAP_SHA256, "metals map"),
        )
        mismatches = [
            label
            for path, expected, label in frozen
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatches:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CROSS_ASSET_DAILY_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen cross-asset daily sources changed: {', '.join(mismatches)}.",
            )
            return False
        specification = {
            "experiment_type": "cross_asset_daily_horizon_primary",
            "priority": 105.0,
            "max_attempts": 2,
            "pipeline": "DISCOVERY_AND_PROMOTION",
            "engineering_task_path": str(task),
            "engineering_task_sha256": CROSS_ASSET_DAILY_TASK_SHA256,
            "core_data_path": str(core_data),
            "core_data_sha256": ENERGY_METALS_DATA_SHA256,
            "core_map_path": str(core_map),
            "core_map_sha256": PATH_GEOMETRY_MAP_SHA256,
            "core_roll_map_hash": PATH_GEOMETRY_ROLL_HASH,
            "metals_data_path": str(metals_data),
            "metals_data_sha256": ENERGY_METALS_VOLUME_DATA_SHA256,
            "metals_map_path": str(metals_map),
            "metals_map_sha256": ENERGY_METALS_VOLUME_MAP_SHA256,
            "metals_roll_map_hash": ENERGY_METALS_VOLUME_ROLL_HASH,
            "code_commit": self._git_commit(),
            "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
            "selection_end_exclusive": "2024-01-01",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.995,
        }
        enqueue_experiment(conn, CROSS_ASSET_DAILY_EXPERIMENT_ID, specification)
        set_kv(conn, "cross_asset_daily_plan_written", True)
        set_kv(conn, "discovery_pipeline_status", "CROSS_ASSET_DAILY_QUEUED")
        set_kv(conn, "promotion_pipeline_status", "CROSS_ASSET_DAILY_QUEUED")
        set_kv(conn, "foundry_current_engine", "CROSS_ASSET_DAILY_HORIZON")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_cross_asset_daily_shadow(self, conn: Any) -> bool:
        existing = experiment_record(conn, CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID)
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(conn, CROSS_ASSET_DAILY_EXPERIMENT_ID)
        source = dict((predecessor or {}).get("result") or {})
        candidates = [
            row
            for row in source.get("candidates") or []
            if row.get("candidate_id") == CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID
        ]
        configurations = [
            row
            for row in source.get("shadow_configurations") or []
            if row.get("candidate_id") == CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID
        ]
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or source.get("scientific_conclusion")
            != "CROSS_ASSET_DAILY_SHADOW_CANDIDATES_FOUND"
            or len(candidates) != 1
            or candidates[0].get("status") != "SHADOW_RESEARCH_CANDIDATE"
            or not bool(
                (candidates[0].get("admission") or {}).get(
                    "permits_zero_risk_shadow"
                )
            )
            or len(configurations) != 1
        ):
            return False
        task = project_path(
            "reports",
            "engineering",
            "hydra_cross_asset_daily_shadow_activation_20260711.md",
        )
        source_path = Path(
            str((source.get("artifacts") or {}).get("result_json_path") or "")
        )
        configuration_path = Path(str(configurations[0].get("path") or ""))
        if (
            not task.is_file()
            or hashlib.sha256(task.read_bytes()).hexdigest()
            != CROSS_ASSET_DAILY_SHADOW_TASK_SHA256
            or not source_path.is_file()
            or not configuration_path.is_file()
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "CROSS_ASSET_DAILY_SHADOW_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                "Cross-asset daily result, configuration or activation task changed.",
            )
            return False
        specification = {
            "experiment_type": "cross_asset_daily_shadow_activation",
            "priority": 110.0,
            "max_attempts": 2,
            "pipeline": "SHADOW",
            "engineering_task_path": str(task),
            "engineering_task_sha256": CROSS_ASSET_DAILY_SHADOW_TASK_SHA256,
            "source_result_path": str(source_path),
            "source_result_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "source_result_hash": str(source["result_hash"]),
            "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
            "shadow_configuration_path": str(configuration_path),
            "shadow_configuration_sha256": hashlib.sha256(
                configuration_path.read_bytes()
            ).hexdigest(),
            "shadow_configuration_hash": str(
                configurations[0]["configuration_hash"]
            ),
            "code_commit": self._git_commit(),
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 1.0,
        }
        enqueue_experiment(
            conn, CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID, specification
        )
        set_kv(conn, "cross_asset_daily_shadow_plan_written", True)
        set_kv(conn, "shadow_pipeline_status", "ACTIVATION_QUEUED")
        self._clear_resolved_resume_block(conn)
        return True

    def _reconcile_shadow_shared_account_baskets(self, conn: Any) -> bool:
        existing = experiment_record(
            conn, SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID
        )
        if existing is not None:
            if str(existing.get("status")) in {"QUEUED", "RUNNING"}:
                self._clear_resolved_resume_block(conn)
                return True
            return str(existing.get("status")) == "COMPLETED"
        predecessor = experiment_record(conn, CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID)
        predecessor_result = dict((predecessor or {}).get("result") or {})
        registry = dict(get_kv(conn, "shadow_active_registry", {}) or {})
        required_ids = {
            "strategy_open_gap_continuation_YM_v1",
            "strategy_barrier_hazard_NQ_signed_extreme_recovery_60_middle_q65_h30_s100_15m_expansion_v1",
            SESSION_GEOMETRY_MICRO_CHILD_ID,
            CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
        }
        if (
            (predecessor or {}).get("status") != "COMPLETED"
            or predecessor_result.get("scientific_conclusion")
            != "IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED"
            or predecessor_result.get("candidate_id")
            != CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID
            or not required_ids.issubset(registry)
        ):
            return False
        task = project_path(
            "reports",
            "engineering",
            "hydra_shadow_shared_account_baskets_20260711.md",
        )
        root = Path("/root/hydra-bot/reports/mission_experiments")
        sources = [
            {
                "candidate_id": "strategy_open_gap_continuation_YM_v1",
                "result_path": str(
                    root
                    / "ym_open_gap_strict_promotion_v1"
                    / "ym_strict_promotion_result.json"
                ),
                "result_sha256": "17921561a4b464d961bd23f2f469052a89dbc9f4551202a3c4a325a6efca2a31",
                "result_hash": "89c63a68d52a8b3a1277df0cbe8553c2382bf057c8bc2e1fd3ccfb9707c2eecf",
                "ledger_path": str(
                    root
                    / "ym_open_gap_strict_promotion_v1"
                    / "ym_strict_promotion_candidate_ledger.jsonl"
                ),
                "ledger_sha256": "fbb20c9cf5a33f8867b48e0fba8d75a6ebdf083950765187cc5e6fc8a2f63826",
                "expected_2024": {"events": 46, "net_pnl": 652.5},
            },
            {
                "candidate_id": "strategy_barrier_hazard_NQ_signed_extreme_recovery_60_middle_q65_h30_s100_15m_expansion_v1",
                "result_path": str(
                    root / "barrier_hazard_primary_v1" / "barrier_hazard_result.json"
                ),
                "result_sha256": "17c4f1bbae092901e408f1f1d03a15d5afcab358cfd66b64a5145e62858fc553",
                "result_hash": "9243e40d8f08fadec401004f752b0c69bf53800262d5af08a081ea4a075e4bbf",
                "ledger_path": str(
                    root
                    / "barrier_hazard_primary_v1"
                    / "barrier_hazard_trade_ledger.jsonl"
                ),
                "ledger_sha256": "3b8ac95ccebc754c8a87b6b6d4f3f8eb52d20bdbe659b97176f277d186ef8e02",
                "expected_2024": {
                    "events": 68,
                    "net_pnl": 229.8100102579483,
                },
            },
            {
                "candidate_id": SESSION_GEOMETRY_MICRO_CHILD_ID,
                "result_path": str(
                    root
                    / "session_geometry_micro_execution_repair_v1"
                    / "micro_execution_repair_result.json"
                ),
                "result_sha256": "34f7ceaba8128d9491451762e266b422886b1545b637042ef8c49defcc8ec2eb",
                "result_hash": "8336f9231adf63828707b2c31e17d247cfd4ea0d614a330dd83bff0817eceb3b",
                "ledger_path": str(
                    root
                    / "session_geometry_micro_execution_repair_v1"
                    / "micro_execution_repair_trade_ledger.jsonl"
                ),
                "ledger_sha256": "735a01c3f1b1c8585c872d83e5e6986da06fec8ca8424e8dfcc4a30fd4887cb1",
                "expected_2024": {
                    "events": 22,
                    "net_pnl": 223.00000000000847,
                },
            },
            {
                "candidate_id": CROSS_ASSET_DAILY_SHADOW_CANDIDATE_ID,
                "result_path": str(
                    root
                    / "cross_asset_daily_horizon_primary_v1"
                    / "cross_asset_daily_result.json"
                ),
                "result_sha256": "717c088194f9a377c8bc045e9e5b6fcb364f8a8a38209242df5f836505a877a5",
                "result_hash": "a76176fc6619dfb669343c65650e0a5b09f795a1715ec3385c7b59d44069b553",
                "ledger_path": str(
                    root
                    / "cross_asset_daily_horizon_primary_v1"
                    / "cross_asset_daily_trade_ledger.jsonl"
                ),
                "ledger_sha256": "98e5da466bc7e594d781370ab8bc169a44b26757ac545709df4502c055abc01b",
                "expected_2024": {"events": 28, "net_pnl": 513.5},
            },
        ]
        frozen = [(task, SHADOW_SHARED_ACCOUNT_BASKETS_TASK_SHA256, "task")]
        for source in sources:
            frozen.extend(
                [
                    (
                        Path(str(source["result_path"])),
                        str(source["result_sha256"]),
                        f"{source['candidate_id']} result",
                    ),
                    (
                        Path(str(source["ledger_path"])),
                        str(source["ledger_sha256"]),
                        f"{source['candidate_id']} ledger",
                    ),
                ]
            )
        mismatches = [
            label
            for path, expected, label in frozen
            if not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ]
        if mismatches:
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "SHADOW_BASKET_SOURCE_MISMATCH")
            set_kv(
                conn,
                "last_error",
                f"Frozen shadow-basket sources changed: {', '.join(mismatches)}.",
            )
            return False
        specification = {
            "experiment_type": "shadow_shared_account_baskets",
            "priority": 108.0,
            "max_attempts": 2,
            "pipeline": "PORTFOLIO",
            "engineering_task_path": str(task),
            "engineering_task_sha256": SHADOW_SHARED_ACCOUNT_BASKETS_TASK_SHA256,
            "sources": sources,
            "code_commit": self._git_commit(),
            "data_role": "FROZEN_DEVELOPMENT_EVIDENCE_ONLY",
            "development_start": "2024-01-01",
            "development_end_exclusive": "2024-10-01",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "network_allowed": False,
            "live_or_broker_allowed": False,
            "expected_decision_information_gain": 0.99,
        }
        enqueue_experiment(
            conn, SHADOW_SHARED_ACCOUNT_BASKETS_EXPERIMENT_ID, specification
        )
        set_kv(conn, "shadow_shared_account_baskets_plan_written", True)
        set_kv(conn, "portfolio_pipeline_status", "SHARED_ACCOUNT_BASKETS_QUEUED")
        self._clear_resolved_resume_block(conn)
        return True

    @staticmethod
    def _clear_resolved_resume_block(conn: Any) -> None:
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "last_error", None)

    @staticmethod
    def _route_v3_execution_result(conn: Any, result: dict[str, Any]) -> None:
        conclusion = str(result.get("scientific_conclusion") or "")
        evidence_valid = bool(result.get("evidence_valid_for_decision_change"))
        survivor_count = int(result.get("calibration_sensitive_survivor_count") or 0)
        if evidence_valid and survivor_count > 0:
            phase = "ENGINEERING_BLOCKED"
            blocker = "V3_SURVIVOR_REPLICATION_DESIGN_REQUIRED"
        elif conclusion == "ZERO_SURVIVAL_PERSISTS_UNDER_CORRECTED_RETEST_PIVOT_RESEARCH_GRAMMAR":
            phase = "ENGINEERING_BLOCKED"
            blocker = "V3_ZERO_SURVIVAL_GEOMETRY_PIVOT_DESIGN_REQUIRED"
        elif "INSUFFICIENT" in conclusion:
            phase = "ENGINEERING_BLOCKED"
            blocker = "V3_INSUFFICIENT_EVIDENCE_RESOLUTION_DESIGN_REQUIRED"
        elif conclusion.startswith("INVALID_") or conclusion.startswith("INTEGRITY_FAIL"):
            phase = "INTEGRITY_BLOCKED"
            blocker = "V3_RETEST_INTEGRITY_RESOLUTION_REQUIRED"
        else:
            phase = "ENGINEERING_BLOCKED"
            blocker = "V3_RETEST_OUTCOME_ROUTING_REQUIRED"
        set_kv(
            conn,
            "v3_retest_outcome",
            {
                "scientific_conclusion": conclusion,
                "evidence_valid_for_decision_change": evidence_valid,
                "calibration_sensitive_survivor_count": survivor_count,
                "required_next_action": blocker,
            },
        )
        set_kv(conn, "current_phase", phase)
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Fresh v3 result requires its preregistered scientific follow-up; no atom or strategy is validated.",
        )

    @staticmethod
    def _route_path_geometry_result(conn: Any, result: dict[str, Any]) -> None:
        status = str(result.get("candidate_status") or "NOT_VALIDATED")
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        if status == "DEVELOPMENT_SURVIVOR":
            set_kv(conn, "current_blocker", "PATH_GEOMETRY_INDEPENDENT_REPLICATION_REQUIRED")
            set_kv(conn, "last_error", "Candidate survived development gates only; independent replication and Topstep replay remain mandatory.")
        else:
            set_kv(conn, "current_blocker", "MARKET_ECOLOGY_PIVOT_REQUIRED")
            set_kv(conn, "last_error", "Path-geometry candidate failed or lacked evidence; no strategy was validated.")

    @staticmethod
    def _route_metal_energy_result(conn: Any, result: dict[str, Any]) -> None:
        if str(result.get("mechanism_status")) == "DEVELOPMENT_SURVIVOR":
            blocker = "METAL_ENERGY_INDEPENDENT_REPLICATION_REQUIRED"
        else:
            blocker = "MARKET_ECOLOGY_REPRESENTATION_PIVOT_REQUIRED"
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED"); set_kv(conn, "current_blocker", blocker); set_kv(conn, "last_error", "Metal/energy pilot completed; no strategy or mechanism is validated.")

    @staticmethod
    def _route_cross_market_result(conn: Any, result: dict[str, Any]) -> None:
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "NEW_REPRESENTATION_PIVOT_REQUIRED")
        set_kv(conn, "last_error", "Cross-market lead/lag pilot completed; no strategy or mechanism is validated.")

    @staticmethod
    def _route_volatility_result(conn: Any, result: dict[str, Any]) -> None:
        set_kv(conn,"current_phase","ENGINEERING_BLOCKED");set_kv(conn,"current_blocker","NEW_REPRESENTATION_PIVOT_REQUIRED");set_kv(conn,"last_error","Volatility transition pilot completed; no strategy or mechanism is validated.")

    def _route_foundry_bootstrap_result(self, conn: Any, result: dict[str, Any]) -> None:
        if result.get("scientific_conclusion") != (
            "FOUNDRY_CORE_CALIBRATED_TOURNAMENT_RECONCILED"
        ):
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "FOUNDRY_BOOTSTRAP_INVALID")
            set_kv(conn, "last_error", "Foundry bootstrap did not meet its frozen contract.")
            return
        status = result.get("foundry_status") or {}
        for key, value in {
            "strategy_prototypes_generated": status.get("strategy_prototypes_generated", 0),
            "strategies_screened": status.get("strategies_screened", 0),
            "promising_candidates": status.get("promising_candidates", 0),
            "shadow_candidates": status.get("shadow_candidates", 0),
            "paper_shadow_ready_candidates": status.get("paper_shadow_ready", 0),
            "shadow_active_candidates": status.get("shadow_active", 0),
            "mechanisms_represented": status.get("mechanisms_represented", 0),
            "market_ecologies_represented": status.get("market_ecologies_represented", 0),
            "timeframes_represented": status.get("timeframes_represented", 0),
            "strategies_killed": status.get("strategies_killed", 0),
            "lineages_frozen": status.get("lineages_frozen", 0),
            "q4_candidates": status.get("q4_candidates", 0),
            "model_quota_state": status.get("model_quota_state", "UNKNOWN"),
        }.items():
            if key == "model_quota_state":
                set_kv(conn, key, value)
            else:
                set_kv(conn, key, max(int(get_kv(conn, key, 0)), int(value or 0)))
        set_kv(
            conn,
            "foundry_bootstrap_prototype_baseline",
            int(status.get("strategy_prototypes_generated", 0)),
        )
        set_kv(conn, "foundry_core_ready", True)
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "EQUITY_OPEN_GAP_REVERSAL_PILOT_REQUIRED")
        set_kv(
            conn,
            "last_error",
            "Foundry core is calibrated; queue the first distinct low-turnover event strategy pilot.",
        )
        self._reconcile_equity_open_gap_pilot(conn)

    def _route_equity_open_gap_result(self, conn: Any, result: dict[str, Any]) -> None:
        self._update_foundry_candidate_bank(
            conn, result, EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID
        )
        shadow = int(result.get("shadow_candidates", 0))
        candidates = list(result.get("candidates") or [])
        q4_candidates = sum(
            1
            for row in candidates
            if str(row.get("status")) == "SHADOW_RESEARCH_CANDIDATE"
            and not bool((row.get("attacks") or {}).get("event_dominated", True))
        )
        set_kv(conn, "q4_candidates", q4_candidates)
        set_kv(conn, "foundry_current_engine", "ENGINE_A_DIRECT_STATE_MACHINE")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        if q4_candidates:
            blocker = "Q4_FREEZE_PROTOCOL_REQUIRED"
            message = (
                "At least one pre-Q4 shadow-research candidate warrants an immutable "
                "one-shot freeze protocol; Q4 remains unopened."
            )
            set_kv(conn, "milestone", "PRE_Q4_SHADOW_CANDIDATE")
        elif shadow:
            blocker = "SHADOW_CANDIDATE_FAILURE_SURFACE_REQUIRED"
            message = (
                "A zero-risk shadow-research candidate exists but event concentration or another "
                "diagnostic must be resolved before Q4 freeze."
            )
        elif result.get("scientific_conclusion") == (
            "EQUITY_OPEN_GAP_REVERSAL_FALSIFIED_OR_INSUFFICIENT"
        ):
            blocker = "EQUITY_OPEN_GAP_CONTINUATION_PILOT_REQUIRED"
            message = (
                "Direct reversal failed, but its frozen sign-flip control changed the directional "
                "decision; test a fresh continuation formulation without inherited evidence."
            )
        else:
            blocker = "DISTRIBUTIONAL_OPENING_HAZARD_PILOT_REQUIRED"
            message = "Gap reversal outcome requires a distributional opening-state pivot."
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "reason": message,
                "q4_access_authorized": False,
            },
        )
        if blocker == "EQUITY_OPEN_GAP_CONTINUATION_PILOT_REQUIRED":
            self._reconcile_equity_open_gap_continuation_pilot(conn)

    def _route_equity_open_gap_continuation_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, EQUITY_OPEN_GAP_CONTINUATION_EXPERIMENT_ID
        )
        eligible = list(result.get("q4_freeze_eligible_candidate_ids") or [])
        shadow = int(result.get("shadow_candidates", 0))
        set_kv(conn, "q4_candidates", len(eligible))
        set_kv(conn, "foundry_current_engine", "ENGINE_A_TARGETED_MUTATION")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        if eligible:
            blocker = "Q4_FREEZE_PROTOCOL_REQUIRED"
            message = (
                "Fresh continuation evidence produced a non-event-dominated shadow-research "
                "candidate; create an immutable one-shot Q4 freeze before any Q4 read."
            )
            set_kv(conn, "milestone", "PRE_Q4_SHADOW_CANDIDATE")
        elif shadow:
            blocker = "CONTINUATION_FAILURE_SURFACE_REQUIRED"
            message = "Shadow admission exists, but concentration prevents Q4 freeze."
        else:
            blocker = "DISTRIBUTIONAL_OPENING_HAZARD_PILOT_REQUIRED"
            message = (
                "Continuation did not reach shadow admission; pivot to target-before-invalidation hazard."
            )
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {"action": blocker, "reason": message, "q4_access_authorized": False},
        )
        if blocker == "Q4_FREEZE_PROTOCOL_REQUIRED":
            self._reconcile_q4_candidate_freeze(conn)

    def _route_q4_candidate_freeze_result(self, conn: Any, result: dict[str, Any]) -> None:
        if result.get("scientific_conclusion") != "Q4_FREEZE_READY":
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "Q4_FREEZE_INVALID")
            set_kv(conn, "last_error", "Q4 freeze experiment did not meet its immutable contract.")
            return
        set_kv(
            conn,
            "q4_freeze_manifest",
            {
                "candidate_id": result.get("candidate_id"),
                "path": result.get("freeze_manifest_path"),
                "hash": result.get("freeze_manifest_hash"),
                "status": "FROZEN_Q4_UNOPENED",
            },
        )
        accounted_hash = get_kv(conn, "q4_freeze_accounted_result_hash")
        if accounted_hash != result.get("result_hash"):
            set_kv(
                conn,
                "lineages_frozen",
                int(get_kv(conn, "lineages_frozen", 0)) + 1,
            )
            set_kv(conn, "q4_freeze_accounted_result_hash", result.get("result_hash"))
        set_kv(conn, "lineages_frozen", max(int(get_kv(conn, "lineages_frozen", 0)), 1))
        set_kv(conn, "q4_access_blocker", "GOVERNANCE_CHANGE_REQUIRED_FOR_FROZEN_ONE_SHOT_Q4")
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "DISTRIBUTIONAL_OPENING_HAZARD_PILOT_REQUIRED")
        set_kv(
            conn,
            "last_error",
            "Candidate is frozen and Q4 remains sealed; protected governor blocks Q4, while independent research must continue.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": "DISTRIBUTIONAL_OPENING_HAZARD_PILOT_REQUIRED",
                "parallel_blocker": "GOVERNANCE_CHANGE_REQUIRED_FOR_FROZEN_ONE_SHOT_Q4",
                "q4_access_authorized": False,
            },
        )
        self._reconcile_opening_direction_hazard(conn)

    def _route_opening_direction_hazard_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, OPENING_DIRECTION_HAZARD_EXPERIMENT_ID
        )
        set_kv(conn, "foundry_current_engine", "ENGINE_B_DISTRIBUTIONAL_HAZARD")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        eligible = list(result.get("q4_freeze_eligible_candidate_ids") or [])
        if eligible:
            blocker = "DISTINCT_HAZARD_FREEZE_PROTOCOL_REQUIRED"
            message = "A distinct hazard candidate reached shadow research and requires its own freeze lineage."
        elif int(result.get("promising_candidates", 0)) > 0:
            blocker = "CROSS_ECOLOGY_INVARIANT_SEARCH_REQUIRED"
            message = (
                "Hazard policy produced only sparse promising evidence; expand to a different ecology/representation."
            )
        else:
            blocker = "CROSS_ECOLOGY_INVARIANT_SEARCH_REQUIRED"
            message = "Opening hazard was falsified or underpowered; pivot to cross-ecology invariants."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "parallel_blocker": get_kv(conn, "q4_access_blocker"),
                "q4_access_authorized": False,
            },
        )
        if blocker == "CROSS_ECOLOGY_INVARIANT_SEARCH_REQUIRED":
            self._reconcile_cross_ecology_opening_acceptance(conn)

    def _route_cross_ecology_opening_acceptance_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, CROSS_ECOLOGY_OPENING_ACCEPTANCE_EXPERIMENT_ID
        )
        set_kv(conn, "foundry_current_engine", "ENGINE_E_CROSS_ECOLOGY_INVARIANT")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        shadow = int(result.get("shadow_candidates", 0))
        promising = int(result.get("promising_candidates", 0))
        if shadow:
            blocker = "DISTINCT_ECOLOGY_FREEZE_OR_FORWARD_SHADOW_REQUIRED"
            message = "A metal/energy candidate reached shadow research and needs a distinct freeze/forward plan."
        elif promising:
            blocker = "CROSS_ECOLOGY_FAILURE_SURFACE_REQUIRED"
            message = "Cross-ecology evidence is promising but incomplete; map the causal failure surface."
        else:
            blocker = "MULTITIMEFRAME_SESSION_DAILY_INVARIANT_REQUIRED"
            message = "Opening acceptance failed or was insufficient; pivot to session/daily invariants."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "parallel_blocker": get_kv(conn, "q4_access_blocker"),
                "q4_access_authorized": False,
            },
        )
        if blocker == "MULTITIMEFRAME_SESSION_DAILY_INVARIANT_REQUIRED":
            self._reconcile_mtf_session_trend_confirmation(conn)

    def _route_mtf_session_trend_confirmation_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, MTF_SESSION_TREND_CONFIRMATION_EXPERIMENT_ID
        )
        set_kv(conn, "foundry_current_engine", "ENGINE_E_MTF_INVARIANT")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        shadow = int(result.get("shadow_candidates", 0))
        promising = int(result.get("promising_candidates", 0))
        if shadow:
            blocker = "DISTINCT_MTF_FREEZE_OR_FORWARD_SHADOW_REQUIRED"
            message = "An MTF candidate reached shadow research and requires a distinct freeze/forward plan."
        elif promising:
            blocker = "MTF_ABLATION_AND_FAILURE_SURFACE_REQUIRED"
            message = "MTF confirmation is promising but requires its frozen ablation/failure surface."
        else:
            blocker = "RELATIVE_VALUE_OR_DEFENSIVE_PORTFOLIO_REQUIRED"
            message = "MTF trend confirmation failed; pivot to relative-value or defensive account utility."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "parallel_blocker": get_kv(conn, "q4_access_blocker"),
                "q4_access_authorized": False,
            },
        )
        if blocker == "RELATIVE_VALUE_OR_DEFENSIVE_PORTFOLIO_REQUIRED":
            self._reconcile_rty_ym_relative_value(conn)

    @staticmethod
    def _route_rty_ym_relative_value_result(conn: Any, result: dict[str, Any]) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, RTY_YM_RELATIVE_VALUE_EXPERIMENT_ID
        )
        set_kv(conn, "foundry_current_engine", "ENGINE_F_RELATIVE_VALUE")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        shadow = int(result.get("shadow_candidates", 0))
        promising = int(result.get("promising_candidates", 0))
        if shadow:
            blocker = "RELATIVE_VALUE_EXECUTION_OR_FREEZE_REQUIRED"
            message = "A two-leg candidate reached shadow research and needs targeted paired execution/freeze."
        elif promising:
            blocker = "RELATIVE_VALUE_FAILURE_SURFACE_REQUIRED"
            message = "Relative-value evidence is promising but requires failure/execution analysis."
        else:
            blocker = "DEFENSIVE_PORTFOLIO_RISK_ENGINE_REQUIRED"
            message = "The frozen relative-value formulation failed or had no executable events; pivot defensive."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "parallel_blocker": get_kv(conn, "q4_access_blocker"),
                "q4_access_authorized": False,
            },
        )

    @staticmethod
    def _route_ym_shared_risk_off_result(conn: Any, result: dict[str, Any]) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, YM_SHARED_RISK_OFF_EXPERIMENT_ID
        )
        set_kv(conn, "foundry_current_engine", "ENGINE_H_DEFENSIVE_PORTFOLIO")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        shadow = int(result.get("shadow_candidates", 0))
        promising = int(result.get("promising_candidates", 0))
        if shadow:
            blocker = "DEFENSIVE_FORWARD_SHADOW_AND_BASKET_REQUIRED"
            message = "The defensive child reached safe shadow research; start forward evidence and basket replay."
        elif promising:
            blocker = "DEFENSIVE_FAILURE_SURFACE_REQUIRED"
            message = "The defensive child retained utility but needs a frozen failure-surface audit."
        else:
            blocker = "INVENTED_METHOD_OR_PORTFOLIO_SEARCH_REQUIRED"
            message = "The preregistered shared-risk overlay failed; pivot representation instead of tuning it."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "parallel_blocker": get_kv(conn, "q4_access_blocker"),
                "q4_access_authorized": False,
            },
        )

    def _route_qd_economic_tournament_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, QD_ECONOMIC_TOURNAMENT_EXPERIMENT_ID
        )
        set_kv(conn, "foundry_current_engine", "ENGINE_J_QUALITY_DIVERSITY")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "quality_diversity_archive",
            result.get("quality_diversity_archive") or {},
        )
        set_kv(
            conn,
            "qd_stage1_survivors",
            int(result.get("stage1_survivors", 0)),
        )
        set_kv(
            conn,
            "qd_validation_elites",
            int(result.get("validation_elites", 0)),
        )
        shadow = int(result.get("shadow_candidates", 0))
        promising = int(result.get("promising_candidates", 0))
        if shadow:
            blocker = "QD_SHADOW_ACTIVATION_AND_YM_STRICT_REPLAY_REQUIRED"
            message = "Frozen QD candidates may enter zero-order shadow while strict promotion continues."
        elif promising:
            blocker = "QD_TARGETED_CONFIRMATION_AND_YM_STRICT_REPLAY_REQUIRED"
            message = "QD transfer found promising exact versions; preregister confirmations and resolve frozen YM."
        else:
            blocker = "QD_FAILURE_MAP_AND_YM_STRICT_REPLAY_REQUIRED"
            message = "The first feasible QD promotion found no shadow admission; use its failure map and resolve frozen YM."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "PROMOTION",
                "parallel_discovery": True,
                "parallel_shadow": True,
                "q4_access_authorized": False,
            },
        )
        self._reconcile_ym_strict_promotion(conn)

    def _route_ym_strict_promotion_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, YM_STRICT_PROMOTION_EXPERIMENT_ID
        )
        set_kv(conn, "promotion_pipeline_status", "COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        eligible = bool(result.get("shadow_activation_eligible"))
        hard = list(result.get("hard_invalidations") or [])
        if eligible and not hard:
            blocker = "YM_IMMUTABLE_SHADOW_ACTIVATION_REQUIRED"
            message = (
                "YM lacks two-quarter strict confirmation but passes integrity, matched-null, "
                "concentration and risk safety needed for zero-order forward shadow research."
            )
        else:
            blocker = "YM_SHADOW_HARD_INVALIDATION_RESOLUTION_REQUIRED"
            message = f"YM shadow activation is prohibited by hard invalidations: {hard}."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "SHADOW" if eligible else "ENGINEERING",
                "parallel_discovery": True,
                "q4_access_authorized": False,
            },
        )
        if eligible and not hard:
            self._reconcile_ym_shadow_activation(conn)

    def _route_ym_shadow_activation_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, YM_SHADOW_ACTIVATION_EXPERIMENT_ID
        )
        entry = registry_entry_from_activation(result)
        registry = dict(get_kv(conn, "shadow_active_registry", {}) or {})
        candidate_id = str(result["candidate_id"])
        existing = registry.get(candidate_id)
        if existing is not None and existing != entry:
            raise ShadowPipelineIntegrityError(
                "Refusing in-place mutation of an active shadow candidate."
            )
        registry[candidate_id] = entry
        set_kv(conn, "shadow_active_registry", registry)
        set_kv(conn, "shadow_pipeline_status", "RUNNING_FAIL_CLOSED")
        set_kv(conn, "shadow_active_candidates", len(registry))
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "last_error", None)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": "PARALLEL_DISCOVERY_AND_QD_TARGETED_CONFIRMATION",
                "pipeline": "DISCOVERY",
                "shadow_pipeline": "RUNNING_FAIL_CLOSED",
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)
        self._reconcile_accelerated_context_tournament(conn)

    def _route_accelerated_context_tournament_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        AutonomousMissionController._update_foundry_candidate_bank(
            conn, result, ACCELERATED_CONTEXT_TOURNAMENT_EXPERIMENT_ID
        )
        set_kv(conn, "discovery_pipeline_status", "COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "accelerated_tournament_metrics",
            {
                "structural_prototypes": int(result.get("structural_prototypes", 0)),
                "executable_hypotheses": int(result.get("executable_hypotheses", 0)),
                "round1_survivors": int(result.get("round1_survivors", 0)),
                "round2_survivors": int(result.get("round2_survivors", 0)),
                "validation_elites": int(result.get("validation_elites", 0)),
            },
        )
        self._reconcile_selection_null_power(conn)

    def _route_selection_null_power_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        set_kv(conn, "promotion_pipeline_status", "VALIDATOR_CALIBRATION_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "selection_null_power_calibration",
            {
                "conclusion": result.get("scientific_conclusion"),
                "family_false_admission_rate": result.get(
                    "maximum_family_false_admission_rate"
                ),
                "meaningful_effect_power": result.get(
                    "minimum_meaningful_effect_power_n120_plus"
                ),
                "passed": bool(result.get("calibration_passed")),
            },
        )
        blocker = (
            "NEW_MECHANISM_OR_FRESH_CONFIRMATION_REQUIRED"
            if bool(result.get("calibration_passed"))
            else "SELECTION_NULL_POLICY_REPAIR_REQUIRED"
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Validator calibration completed without changing historical candidate status.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "PROMOTION_VALIDATOR",
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        if blocker == "SELECTION_NULL_POLICY_REPAIR_REQUIRED":
            self._reconcile_selection_null_policy_repair(conn)

    def _route_selection_null_policy_repair_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        passed = bool(result.get("calibration_passed"))
        blocker = (
            "NEW_SINGLE_PRIMARY_TOURNAMENT_REQUIRED"
            if passed
            else "TIGHTER_SINGLE_PRIMARY_ALPHA_CALIBRATION_REQUIRED"
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", "Prospective policy comparison completed; historical statuses unchanged.")
        if not passed:
            self._reconcile_single_primary_alpha(conn)

    def _route_single_primary_alpha_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "single_primary_null_policy", result.get("prospective_policy_contract"))
        if bool(result.get("calibration_passed")):
            blocker = "NEW_SINGLE_PRIMARY_TOURNAMENT_REQUIRED"
            message = (
                f"Prospective single-primary alpha {result.get('selected_alpha')} calibrated; "
                "build a new-ID early-fold selection/later-fold confirmation tournament."
            )
        else:
            blocker = "NEW_VALIDATION_STATISTIC_REQUIRED"
            message = "The bounded alpha grid failed; invent a new calibrated statistic."
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(conn, "last_error", message)
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "PROMOTION",
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        if bool(result.get("calibration_passed")):
            self._reconcile_single_primary_context_tournament(conn)

    def _route_single_primary_context_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, SINGLE_PRIMARY_CONTEXT_TOURNAMENT_EXPERIMENT_ID
        )
        primary_id = str(result.get("primary_candidate_id") or "")
        conclusion = str(result.get("scientific_conclusion") or "")
        shadow_count = int(result.get("shadow_candidates") or 0)
        promising_count = int(result.get("promising_candidates") or 0)
        killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
        if (
            primary_id
            and conclusion
            == "SINGLE_PRIMARY_CONTEXT_CONFIRMATION_FALSIFIED_OR_INSUFFICIENT"
            and primary_id not in killed
        ):
            killed.add(primary_id)
            set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
            set_kv(
                conn,
                "strategies_killed",
                int(get_kv(conn, "strategies_killed", 0)) + 1,
            )
        if shadow_count > 0:
            blocker = "SINGLE_PRIMARY_SHADOW_ACTIVATION_REQUIRED"
        elif promising_count > 0:
            blocker = "SINGLE_PRIMARY_NEW_ID_CONFIRMATION_REQUIRED"
        else:
            blocker = "COUNTERFACTUAL_HAZARD_PRIMARY_REQUIRED"
        set_kv(conn, "promotion_pipeline_status", "SINGLE_PRIMARY_CONTEXT_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "single_primary_context_metrics",
            {
                "structural_prototypes": int(result.get("structural_prototypes", 0)),
                "round1_survivors": int(result.get("round1_survivors", 0)),
                "round2_survivors": int(result.get("round2_survivors", 0)),
                "diagnostic_archive_size": int(
                    result.get("diagnostic_archive_size", 0)
                ),
                "primary_candidate_id": primary_id or None,
                "conclusion": conclusion,
            },
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "The exact frozen primary was resolved prospectively; historical or "
            "diagnostic candidates received no inherited status.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "PROMOTION_AND_DISCOVERY",
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)
        if blocker == "COUNTERFACTUAL_HAZARD_PRIMARY_REQUIRED":
            self._reconcile_counterfactual_hazard_primary(conn)

    def _route_counterfactual_hazard_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, COUNTERFACTUAL_HAZARD_PRIMARY_EXPERIMENT_ID
        )
        primary_id = str(result.get("primary_candidate_id") or "")
        conclusion = str(result.get("scientific_conclusion") or "")
        shadow_count = int(result.get("shadow_candidates") or 0)
        promising_count = int(result.get("promising_candidates") or 0)
        killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
        if (
            primary_id
            and conclusion
            == "COUNTERFACTUAL_HAZARD_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
            and primary_id not in killed
        ):
            killed.add(primary_id)
            set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
            set_kv(
                conn,
                "strategies_killed",
                int(get_kv(conn, "strategies_killed", 0)) + 1,
            )
        if shadow_count > 0:
            blocker = "COUNTERFACTUAL_HAZARD_SHADOW_ACTIVATION_REQUIRED"
        elif promising_count > 0:
            blocker = "COUNTERFACTUAL_HAZARD_NEW_ID_REPLICATION_REQUIRED"
        else:
            blocker = "DISTRIBUTIONAL_BARRIER_HAZARD_PRIMARY_REQUIRED"
        set_kv(conn, "promotion_pipeline_status", "COUNTERFACTUAL_HAZARD_COMPLETED")
        set_kv(conn, "discovery_pipeline_status", "COUNTERFACTUAL_HAZARD_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "counterfactual_hazard_metrics",
            {
                "structural_prototypes": int(result.get("structural_prototypes", 0)),
                "round1_survivors": int(result.get("round1_survivors", 0)),
                "round2_survivors": int(result.get("round2_survivors", 0)),
                "diagnostic_archive_size": int(
                    result.get("diagnostic_archive_size", 0)
                ),
                "primary_candidate_id": primary_id or None,
                "conclusion": conclusion,
            },
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Counterfactual positive-outcome hazard completed under its frozen "
            "single-primary contract; archive diagnostics inherited no status.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "PROMOTION_AND_DISCOVERY",
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)
        if blocker == "DISTRIBUTIONAL_BARRIER_HAZARD_PRIMARY_REQUIRED":
            self._reconcile_barrier_hazard_primary(conn)

    def _route_barrier_hazard_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, BARRIER_HAZARD_PRIMARY_EXPERIMENT_ID
        )
        primary_id = str(result.get("primary_candidate_id") or "")
        conclusion = str(result.get("scientific_conclusion") or "")
        shadow_count = int(result.get("shadow_candidates") or 0)
        promising_count = int(result.get("promising_candidates") or 0)
        killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
        if (
            primary_id
            and conclusion == "BARRIER_HAZARD_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
            and primary_id not in killed
        ):
            killed.add(primary_id)
            set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
            set_kv(
                conn,
                "strategies_killed",
                int(get_kv(conn, "strategies_killed", 0)) + 1,
            )
        if shadow_count > 0:
            blocker = "BARRIER_HAZARD_SHADOW_ACTIVATION_REQUIRED"
        elif promising_count > 0:
            blocker = "BARRIER_HAZARD_FRESH_ID_REPLICATION_REQUIRED"
        else:
            blocker = "ENERGY_METALS_ECOLOGY_SEARCH_REQUIRED"
        set_kv(conn, "promotion_pipeline_status", "BARRIER_HAZARD_COMPLETED")
        set_kv(conn, "discovery_pipeline_status", "BARRIER_HAZARD_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "barrier_hazard_metrics",
            {
                "structural_prototypes": int(result.get("structural_prototypes", 0)),
                "round1_survivors": int(result.get("round1_survivors", 0)),
                "round2_survivors": int(result.get("round2_survivors", 0)),
                "diagnostic_archive_size": int(
                    result.get("diagnostic_archive_size", 0)
                ),
                "primary_candidate_id": primary_id or None,
                "conclusion": conclusion,
            },
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Barrier-hazard primary completed under conservative path ordering; "
            "shadow classification does not imply final null or holdout passage.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "SHADOW" if shadow_count else "PROMOTION_AND_DISCOVERY",
                "parallel_discovery": True,
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)
        if blocker == "BARRIER_HAZARD_SHADOW_ACTIVATION_REQUIRED":
            self._reconcile_barrier_shadow_activation(conn)

    def _route_energy_metals_barrier_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, ENERGY_METALS_BARRIER_PRIMARY_EXPERIMENT_ID
        )
        primary_id = str(result.get("primary_candidate_id") or "")
        conclusion = str(result.get("scientific_conclusion") or "")
        shadow_count = int(result.get("shadow_candidates") or 0)
        promising_count = int(result.get("promising_candidates") or 0)
        killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
        if (
            primary_id
            and conclusion
            == "ENERGY_METALS_BARRIER_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
            and primary_id not in killed
        ):
            killed.add(primary_id)
            set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
            set_kv(
                conn,
                "strategies_killed",
                int(get_kv(conn, "strategies_killed", 0)) + 1,
            )
        if shadow_count > 0:
            blocker = "ENERGY_METALS_SHADOW_ACTIVATION_REQUIRED"
        elif promising_count > 0:
            blocker = "ENERGY_METALS_FRESH_ID_REPLICATION_REQUIRED"
        else:
            blocker = "ENERGY_METALS_SESSION_GEOMETRY_REQUIRED"
        set_kv(conn, "promotion_pipeline_status", "ENERGY_METALS_BARRIER_COMPLETED")
        set_kv(conn, "discovery_pipeline_status", "ENERGY_METALS_BARRIER_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "energy_metals_barrier_metrics",
            {
                "structural_prototypes": int(result.get("structural_prototypes", 0)),
                "round1_survivors": int(result.get("round1_survivors", 0)),
                "round2_survivors": int(result.get("round2_survivors", 0)),
                "diagnostic_archive_size": int(
                    result.get("diagnostic_archive_size", 0)
                ),
                "primary_candidate_id": primary_id or None,
                "conclusion": conclusion,
            },
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Energy/metals barrier transfer completed under a frozen single-primary "
            "contract; negative results do not authorize weaker gates.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "SHADOW" if shadow_count else "DISCOVERY",
                "parallel_discovery": True,
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)

    def _route_energy_metals_session_geometry_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, ENERGY_METALS_SESSION_GEOMETRY_EXPERIMENT_ID
        )
        primary_id = str(result.get("primary_candidate_id") or "")
        conclusion = str(result.get("scientific_conclusion") or "")
        shadow_count = int(result.get("shadow_candidates") or 0)
        promising_count = int(result.get("promising_candidates") or 0)
        killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
        if (
            primary_id
            and conclusion == "ENERGY_METALS_SESSION_GEOMETRY_PRIMARY_FALSIFIED"
            and primary_id not in killed
        ):
            killed.add(primary_id)
            set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
            set_kv(
                conn,
                "strategies_killed",
                int(get_kv(conn, "strategies_killed", 0)) + 1,
            )
        if shadow_count > 0:
            blocker = "ENERGY_METALS_SESSION_SHADOW_ACTIVATION_REQUIRED"
        elif promising_count > 0:
            blocker = "ENERGY_METALS_SESSION_GEOMETRY_REPLICATION_REQUIRED"
        else:
            blocker = "CROSS_ASSET_DAILY_HORIZON_REQUIRED"
        set_kv(conn, "promotion_pipeline_status", "SESSION_GEOMETRY_COMPLETED")
        set_kv(conn, "discovery_pipeline_status", "SESSION_GEOMETRY_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(
            conn,
            "energy_metals_session_geometry_metrics",
            {
                "structural_prototypes": int(result.get("structural_prototypes", 0)),
                "round1_survivors": int(result.get("round1_survivors", 0)),
                "round2_survivors": int(result.get("round2_survivors", 0)),
                "diagnostic_archive_size": int(
                    result.get("diagnostic_archive_size", 0)
                ),
                "primary_candidate_id": primary_id or None,
                "conclusion": conclusion,
            },
        )
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Session geometry completed under a frozen primary. A promising lineage "
            "may be reformulated with a fresh ID, but the tested version inherits no shadow pass.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "SHADOW" if shadow_count else "PROMOTION_AND_DISCOVERY",
                "parallel_discovery": True,
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)

    def _route_session_geometry_micro_repair_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, SESSION_GEOMETRY_MICRO_REPAIR_EXPERIMENT_ID
        )
        shadow_count = int(result.get("shadow_candidates") or 0)
        conclusion = str(result.get("scientific_conclusion") or "")
        if (
            shadow_count > 0
            and conclusion == "SYNCHRONIZED_MCL_EXECUTION_SHADOW_CANDIDATE_FOUND"
        ):
            blocker = "SESSION_GEOMETRY_MICRO_SHADOW_ACTIVATION_REQUIRED"
        else:
            blocker = "CROSS_ASSET_DAILY_HORIZON_REQUIRED"
            killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
            if SESSION_GEOMETRY_MICRO_CHILD_ID not in killed:
                killed.add(SESSION_GEOMETRY_MICRO_CHILD_ID)
                set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
                set_kv(
                    conn,
                    "strategies_killed",
                    int(get_kv(conn, "strategies_killed", 0)) + 1,
                )
        set_kv(conn, "promotion_pipeline_status", "MICRO_EXECUTION_REPAIR_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Synchronized MCL execution was recomputed from the immutable CL signal. "
            "A positive result authorizes only zero-order forward shadow research.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "SHADOW" if shadow_count else "DISCOVERY",
                "parallel_discovery": True,
                "shadow_pipeline": get_kv(conn, "shadow_pipeline_status"),
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)
        if blocker == "SESSION_GEOMETRY_MICRO_SHADOW_ACTIVATION_REQUIRED":
            self._reconcile_session_geometry_micro_shadow(conn)

    def _route_session_geometry_micro_shadow_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, SESSION_GEOMETRY_MICRO_SHADOW_EXPERIMENT_ID
        )
        entry = registry_entry_from_activation(result)
        registry = dict(get_kv(conn, "shadow_active_registry", {}) or {})
        candidate_id = str(result["candidate_id"])
        existing = registry.get(candidate_id)
        if existing is not None and existing != entry:
            raise ShadowPipelineIntegrityError(
                "Refusing in-place mutation of synchronized-MCL shadow candidate."
            )
        registry[candidate_id] = entry
        set_kv(conn, "shadow_active_registry", registry)
        set_kv(conn, "shadow_pipeline_status", "RUNNING_FAIL_CLOSED")
        set_kv(conn, "shadow_active_candidates", len(registry))
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "GC_SESSION_GEOMETRY_FRESH_ID_REQUIRED")
        set_kv(
            conn,
            "last_error",
            "Synchronized CL-signal/MCL-execution child is active with zero orders; "
            "discovery continues in a distinct metals session niche.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": "GC_SESSION_GEOMETRY_FRESH_ID_REQUIRED",
                "pipeline": "DISCOVERY",
                "shadow_pipeline": "RUNNING_FAIL_CLOSED",
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)

    def _route_gc_session_geometry_fresh_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, GC_SESSION_GEOMETRY_FRESH_EXPERIMENT_ID
        )
        shadow_count = int(result.get("shadow_candidates") or 0)
        conclusion = str(result.get("scientific_conclusion") or "")
        if (
            shadow_count > 0
            and conclusion
            == "GC_SESSION_GEOMETRY_FRESH_SHADOW_CANDIDATE_FOUND"
        ):
            blocker = "GC_SESSION_GEOMETRY_SHADOW_ACTIVATION_REQUIRED"
        else:
            blocker = "CROSS_ASSET_DAILY_HORIZON_REQUIRED"
            killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
            if GC_SESSION_GEOMETRY_FRESH_CHILD_ID not in killed:
                killed.add(GC_SESSION_GEOMETRY_FRESH_CHILD_ID)
                set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
                set_kv(
                    conn,
                    "strategies_killed",
                    int(get_kv(conn, "strategies_killed", 0)) + 1,
                )
            self._refresh_foundry_candidate_counts(conn)
        candidates = list(result.get("candidates") or [])
        candidate = dict(candidates[0]) if len(candidates) == 1 else {}
        set_kv(
            conn,
            "gc_session_geometry_fresh_metrics",
            {
                "candidate_id": candidate.get("candidate_id"),
                "status": candidate.get("status"),
                "events": int(candidate.get("events") or 0),
                "net_pnl": float(candidate.get("net_pnl") or 0.0),
                "null_probability": float(
                    (candidate.get("null_evidence") or {}).get(
                        "raw_probability", 1.0
                    )
                ),
                "conclusion": conclusion,
            },
        )
        set_kv(conn, "discovery_pipeline_status", "GC_FRESH_PRIMARY_COMPLETED")
        set_kv(conn, "promotion_pipeline_status", "GC_FRESH_PRIMARY_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            (
                "Fresh GC/MGC primary completed prospectively. The exact version "
                "is frozen; a concentration failure is a kill, not a shadow pass."
            ),
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "SHADOW" if shadow_count else "DISCOVERY",
                "parallel_shadow": True,
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)

    def _route_cross_asset_daily_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, CROSS_ASSET_DAILY_EXPERIMENT_ID
        )
        set_kv(
            conn,
            "quality_diversity_archive",
            {
                "engine": "CROSS_ASSET_DAILY_HORIZON",
                "selector_audit": result.get("selector_audit") or {},
                "elite_candidate_ids": result.get("elite_candidate_ids") or [],
                "negative_controls": result.get("negative_controls") or [],
            },
        )
        killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
        newly_killed = 0
        retained_tiers = {
            "PROMISING_RESEARCH_CANDIDATE",
            "ROBUST_RESEARCH_CANDIDATE",
            "SHADOW_RESEARCH_CANDIDATE",
        }
        for candidate in result.get("candidates") or []:
            candidate_id = str(candidate.get("candidate_id") or "")
            if (
                candidate_id
                and str(candidate.get("status") or "") not in retained_tiers
                and candidate_id not in killed
            ):
                killed.add(candidate_id)
                newly_killed += 1
        if newly_killed:
            set_kv(conn, "foundry_killed_candidate_ids", sorted(killed))
            set_kv(
                conn,
                "strategies_killed",
                int(get_kv(conn, "strategies_killed", 0)) + newly_killed,
            )
        self._refresh_foundry_candidate_counts(conn)
        shadow_count = int(result.get("shadow_candidates") or 0)
        if shadow_count > 0:
            blocker = "CROSS_ASSET_DAILY_SHADOW_ACTIVATION_REQUIRED"
        else:
            blocker = "DISTRIBUTIONAL_PORTFOLIO_ROLE_PIVOT_REQUIRED"
        set_kv(
            conn,
            "cross_asset_daily_metrics",
            {
                "structural_prototypes": int(result.get("structural_prototypes", 0)),
                "round1_survivors": int(result.get("round1_survivors", 0)),
                "round2_survivors": int(result.get("round2_survivors", 0)),
                "elite_count": int(result.get("elite_count", 0)),
                "promising_candidates": int(result.get("promising_candidates", 0)),
                "shadow_candidates": shadow_count,
                "topstep_path_candidates": int(
                    result.get("topstep_path_candidates", 0)
                ),
                "performance": result.get("performance") or {},
                "conclusion": result.get("scientific_conclusion"),
            },
        )
        set_kv(conn, "discovery_pipeline_status", "CROSS_ASSET_DAILY_COMPLETED")
        set_kv(conn, "promotion_pipeline_status", "CROSS_ASSET_DAILY_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            "Cross-asset daily elites were frozen on 2023 and replayed unchanged on "
            "2024 Q1-Q3. Only exact calibrated survivors may enter zero-order shadow.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "SHADOW" if shadow_count else "DISCOVERY",
                "parallel_shadow": True,
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)
        if shadow_count > 0:
            self._reconcile_cross_asset_daily_shadow(conn)

    def _route_cross_asset_daily_shadow_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, CROSS_ASSET_DAILY_SHADOW_EXPERIMENT_ID
        )
        entry = registry_entry_from_activation(result)
        registry = dict(get_kv(conn, "shadow_active_registry", {}) or {})
        candidate_id = str(result["candidate_id"])
        existing = registry.get(candidate_id)
        if existing is not None and existing != entry:
            raise ShadowPipelineIntegrityError(
                "Refusing in-place mutation of cross-asset daily shadow candidate."
            )
        registry[candidate_id] = entry
        set_kv(conn, "shadow_active_registry", registry)
        set_kv(conn, "shadow_pipeline_status", "RUNNING_FAIL_CLOSED")
        set_kv(conn, "shadow_active_candidates", len(registry))
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(
            conn,
            "current_blocker",
            "PORTFOLIO_BASKET_AND_DISTRIBUTIONAL_SEARCH_REQUIRED",
        )
        set_kv(
            conn,
            "last_error",
            "Fourth distinct zero-order shadow is active; shared-account baskets and "
            "the next distributional search must now run in parallel.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": "PORTFOLIO_BASKET_AND_DISTRIBUTIONAL_SEARCH_REQUIRED",
                "pipeline": "PORTFOLIO_AND_DISCOVERY",
                "shadow_pipeline": "RUNNING_FAIL_CLOSED",
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)

    def _route_shadow_shared_account_baskets_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        executable = int(result.get("executable_baskets") or 0)
        conclusion = str(result.get("scientific_conclusion") or "")
        if conclusion == "THREE_EXECUTABLE_SHADOW_BASKETS_FOUND" and executable >= 3:
            blocker = "DISTRIBUTIONAL_SURVIVAL_HAZARD_SEARCH_REQUIRED"
        else:
            blocker = "SHARED_ACCOUNT_RISK_REMEDIATION_REQUIRED"
        set_kv(conn, "executable_baskets", executable)
        set_kv(
            conn,
            "shadow_basket_registry",
            {
                row["basket_id"]: {
                    "role": row["role"],
                    "path": row["path"],
                    "configuration_hash": row["configuration_hash"],
                    "outbound_orders_enabled": False,
                }
                for row in result.get("basket_configurations") or []
            },
        )
        set_kv(
            conn,
            "shadow_shared_account_baskets_metrics",
            {
                "basket_count": int(result.get("basket_count") or 0),
                "executable_baskets": executable,
                "selected_baskets": result.get("selected_baskets") or [],
                "manifest_hash": result.get("manifest_hash"),
                "conclusion": conclusion,
            },
        )
        set_kv(conn, "portfolio_pipeline_status", "SHARED_ACCOUNT_BASKETS_COMPLETED")
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", blocker)
        set_kv(
            conn,
            "last_error",
            (
                "Shared-account baskets were recomputed from exact trade ledgers. "
                "They remain zero-order development configurations, not Paper evidence."
            ),
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": blocker,
                "pipeline": "DISCOVERY",
                "portfolio_pipeline": "SHARED_ACCOUNT_BASKETS_COMPLETED",
                "shadow_pipeline": "RUNNING_FAIL_CLOSED",
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)

    def _route_barrier_shadow_activation_result(
        self, conn: Any, result: dict[str, Any]
    ) -> None:
        self._update_foundry_candidate_bank(
            conn, result, BARRIER_HAZARD_SHADOW_ACTIVATION_EXPERIMENT_ID
        )
        entry = registry_entry_from_activation(result)
        registry = dict(get_kv(conn, "shadow_active_registry", {}) or {})
        candidate_id = str(result["candidate_id"])
        existing = registry.get(candidate_id)
        if existing is not None and existing != entry:
            raise ShadowPipelineIntegrityError(
                "Refusing in-place mutation of an active barrier shadow candidate."
            )
        registry[candidate_id] = entry
        set_kv(conn, "shadow_active_registry", registry)
        set_kv(conn, "shadow_pipeline_status", "RUNNING_FAIL_CLOSED")
        set_kv(conn, "shadow_active_candidates", len(registry))
        set_kv(conn, "last_meaningful_progress_at_utc", utc_now_iso())
        set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
        set_kv(conn, "current_blocker", "ENERGY_METALS_ECOLOGY_SEARCH_REQUIRED")
        set_kv(
            conn,
            "last_error",
            "Barrier candidate is active for zero-order forward research; discovery "
            "must now diversify beyond the equity-index ecology.",
        )
        set_kv(
            conn,
            "foundry_next_planned_action",
            {
                "action": "ENERGY_METALS_ECOLOGY_SEARCH_REQUIRED",
                "pipeline": "DISCOVERY",
                "shadow_pipeline": "RUNNING_FAIL_CLOSED",
                "q4_access_authorized": False,
            },
        )
        self._tick_shadow_pipeline(conn)

    @staticmethod
    def _update_foundry_candidate_bank(
        conn: Any, result: dict[str, Any], experiment_id: str
    ) -> None:
        accounted = set(get_kv(conn, "foundry_accounted_experiments", []) or [])
        if experiment_id not in accounted:
            count = int(result.get("candidate_count", 0))
            set_kv(
                conn,
                "strategy_prototypes_generated",
                int(get_kv(conn, "strategy_prototypes_generated", 0)) + count,
            )
            set_kv(
                conn,
                "strategies_screened",
                int(get_kv(conn, "strategies_screened", 0)) + count,
            )
            accounted.add(experiment_id)
            set_kv(conn, "foundry_accounted_experiments", sorted(accounted))
        bank = dict(get_kv(conn, "foundry_candidate_bank", {}) or {})
        for row in result.get("candidates") or []:
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id:
                continue
            bank[candidate_id] = {
                "status": row.get("status"),
                "mechanism_family": row.get("mechanism_family"),
                "primary_market": row.get("primary_market"),
                "execution_market": row.get("execution_market"),
                "net_pnl": row.get("net_pnl"),
                "topstep_path_candidate": bool(
                    (row.get("topstep") or {}).get("path_candidate")
                ),
                "source_experiment": experiment_id,
            }
        set_kv(conn, "foundry_candidate_bank", bank)
        baseline = int(get_kv(conn, "foundry_bootstrap_prototype_baseline", 0))
        candidate_total = baseline + len(bank)
        set_kv(
            conn,
            "strategy_prototypes_generated",
            max(int(get_kv(conn, "strategy_prototypes_generated", 0)), candidate_total),
        )
        set_kv(
            conn,
            "strategies_screened",
            max(int(get_kv(conn, "strategies_screened", 0)), candidate_total),
        )
        AutonomousMissionController._refresh_foundry_candidate_counts(conn)

    @staticmethod
    def _refresh_foundry_candidate_counts(conn: Any) -> None:
        bank = dict(get_kv(conn, "foundry_candidate_bank", {}) or {})
        killed = set(get_kv(conn, "foundry_killed_candidate_ids", []) or [])
        active_rows = {
            candidate_id: row
            for candidate_id, row in bank.items()
            if candidate_id not in killed
        }
        statuses = [str(row.get("status") or "") for row in active_rows.values()]
        promising_tiers = {
            "PROMISING_RESEARCH_CANDIDATE",
            "ROBUST_RESEARCH_CANDIDATE",
            "SHADOW_RESEARCH_CANDIDATE",
            "PAPER_SHADOW_READY",
            "SHADOW_ACTIVE",
            "SHADOW_CONFIRMED",
        }
        shadow_tiers = {
            "SHADOW_RESEARCH_CANDIDATE",
            "PAPER_SHADOW_READY",
            "SHADOW_ACTIVE",
            "SHADOW_CONFIRMED",
        }
        set_kv(
            conn,
            "promising_candidates",
            sum(status in promising_tiers for status in statuses),
        )
        set_kv(
            conn,
            "shadow_candidates",
            sum(status in shadow_tiers for status in statuses),
        )
        set_kv(
            conn,
            "paper_shadow_ready_candidates",
            sum(status == "PAPER_SHADOW_READY" for status in statuses),
        )
        set_kv(
            conn,
            "shadow_active_candidates",
            sum(status == "SHADOW_ACTIVE" for status in statuses),
        )
        set_kv(
            conn,
            "topstep_path_candidates",
            sum(
                bool(row.get("topstep_path_candidate"))
                for row in active_rows.values()
            ),
        )

    def _evidence_reconciliation_exists(self, reconciliation_id: str) -> bool:
        path = self.paths.evidence_ledger
        if not path.exists():
            return False
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("reconciliation_id") == reconciliation_id:
                return True
        return False

    @staticmethod
    def _event_reconciliation_exists(conn: Any, reconciliation_id: str) -> bool:
        rows = conn.execute("SELECT payload FROM events WHERE event_type='completed_experiment_reconciled'").fetchall()
        for (payload_text,) in rows:
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                continue
            if payload.get("reconciliation_id") == reconciliation_id:
                return True
        return False

    def _reconcile_legacy_plan(self, conn: Any) -> None:
        if not get_kv(conn, "bounded_retest_plan_written", False):
            return
        existing = experiment_record(conn, DESIGN_EXPERIMENT_ID)
        created = False
        if existing is None:
            created = enqueue_experiment(conn, DESIGN_EXPERIMENT_ID, self._design_specification())
            existing = experiment_record(conn, DESIGN_EXPERIMENT_ID)
        selected = dict(get_kv(conn, "first_autonomous_experiment_selected", {}) or {})
        selected.update(
            {
                "experiment": DESIGN_EXPERIMENT_ID,
                "experiment_type": "calibration_affected_atom_retest_design",
                "status": "QUEUED" if created else str((existing or {}).get("status", "UNKNOWN")),
                "constraints": "new atom IDs, no inherited results, no Q4, bounded by information value",
            }
        )
        set_kv(conn, "first_autonomous_experiment_selected", selected)

    def _record_action(self, conn: Any, action: dict[str, Any]) -> None:
        snapshot = state_snapshot(conn)
        record_decision(
            self.paths,
            {"mission_id": self.config.mission_id, "cycle": snapshot.get("cycle_count", 0), "selected_action": action},
        )
        append_event(conn, "selected_action", action)
        set_kv(conn, "current_action", action)

    def _record_progress(self, conn: Any) -> None:
        set_kv(conn, "cycle_count", int(get_kv(conn, "cycle_count", 0)) + 1)
        set_kv(conn, "progress_sequence", int(get_kv(conn, "progress_sequence", 0)) + 1)
        set_kv(conn, "last_progress_at_utc", utc_now_iso())
        set_kv(conn, "next_wake_at_utc", self._next_wake_at())

    def _execute_action(self, conn: Any, action: dict[str, Any]) -> None:
        action_type = action.get("action_type")
        if action_type == "RUN_VALIDATOR_CALIBRATION":
            set_kv(conn, "current_phase", "PHASE_2_VALIDATOR_CALIBRATION")
            result = benchmark_validator(
                previous_report="reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md"
            )
            report_path = write_calibration_report(result)
            set_kv(conn, "validator_calibration_passed", result.passed)
            set_kv(conn, "validator_calibration_report", str(report_path))
            set_kv(conn, "false_positive_rate", result.false_positive_rate)
            set_kv(conn, "power_on_meaningful_effects", result.power_on_meaningful_effects)
            set_kv(conn, "previous_zero_pass_cause", result.zero_pass_diagnosis.get("cause"))
            record_evidence(self.paths, {"scope": "VALIDATOR_CALIBRATION", "result": result.to_dict(), "report_path": str(report_path)})
            if result.passed:
                set_kv(conn, "milestone", "M1_VALIDATOR_CALIBRATED")
            else:
                set_kv(conn, "current_blocker", "VALIDATOR_CALIBRATION_FAILED")
        elif action_type == "AUDIT_ZERO_PASS_RESULT":
            set_kv(conn, "current_phase", "PHASE_3_REASSESS_OLD_ATOM_BATCH")
            zero_pass = benchmark_validator(
                previous_report="reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md"
            ).zero_pass_diagnosis
            set_kv(conn, "zero_pass_audited", True)
            set_kv(
                conn,
                "previous_atom_decisions_affected",
                zero_pass.get("cause")
                in {
                    "MULTIPLE_CAUSES_COST_HURDLE_AND_OVERSTRICT_ATTACK_POLICY",
                    "OVERSTRICT_OR_UNCALIBRATED_MANDATORY_ATTACK_POLICY",
                },
            )
            record_evidence(self.paths, {"scope": "ZERO_PASS_AUDIT", "result": zero_pass})
        elif action_type == "PLAN_BOUNDED_RETESTS":
            set_kv(conn, "current_phase", "PHASE_4_AUTONOMOUS_RESEARCH_PLANNING")
            created = enqueue_experiment(conn, DESIGN_EXPERIMENT_ID, self._design_specification())
            set_kv(
                conn,
                "first_autonomous_experiment_selected",
                {
                    "experiment": DESIGN_EXPERIMENT_ID,
                    "experiment_type": "calibration_affected_atom_retest_design",
                    "status": "QUEUED" if created else "ALREADY_PRESENT",
                    "reason": action.get("rationale"),
                    "constraints": "new atom IDs, no inherited results, no Q4, bounded by information value",
                },
            )
            set_kv(conn, "bounded_retest_plan_written", True)
        elif action_type == "PLAN_CALIBRATION_RETEST_EXECUTION":
            design = experiment_record(conn, DESIGN_EXPERIMENT_ID)
            result = (design or {}).get("result") or {}
            paths = result.get("paths") or result.get("artifacts") or {}
            design_path = paths.get("design") or result.get("design_path")
            preregistration_path = paths.get("preregistration") or result.get("preregistration_path")
            if not design_path or not preregistration_path:
                raise RuntimeError("Completed design did not expose design and preregistration artifact paths.")
            specification = {
                "experiment_type": "calibration_affected_atom_retest_execution",
                "priority": 95.0,
                "max_attempts": 3,
                "design_path": str(design_path),
                "design_preregistration_path": str(preregistration_path),
                "code_commit": self._git_commit(),
                "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
                "development_end_exclusive": "2024-10-01",
                "q4_access_allowed": False,
                "paid_data_allowed": False,
            }
            created = enqueue_experiment(conn, EXECUTION_EXPERIMENT_ID, specification)
            set_kv(conn, "calibration_retest_execution_plan_written", True)
            set_kv(
                conn,
                "current_research_experiment_selected",
                {"experiment": EXECUTION_EXPERIMENT_ID, "status": "QUEUED" if created else "ALREADY_PRESENT", "reason": action.get("rationale")},
            )
            set_kv(conn, "current_phase", "PHASE_5_CALIBRATION_AFFECTED_RETEST")
        elif action_type == "PLAN_POST_RETEST_RESEARCH":
            execution = experiment_record(conn, EXECUTION_EXPERIMENT_ID)
            result = (execution or {}).get("result") or {}
            artifacts = result.get("artifacts") or {}
            result_path = artifacts.get("result_json_path")
            result_hash = result.get("result_hash")
            if not result_path or not result_hash:
                raise RuntimeError("Completed calibration retest did not expose a frozen result path and hash.")
            specification = {
                "experiment_type": "post_calibration_retest_research_design",
                "priority": 92.0,
                "max_attempts": 3,
                "source_execution_experiment_id": EXECUTION_EXPERIMENT_ID,
                "source_execution_specification_hash": (execution or {}).get("specification_hash"),
                "source_execution_result_path": str(result_path),
                "source_execution_result_hash": str(result_hash),
                "code_commit": self._git_commit(),
                "data_role": "FROZEN_DEVELOPMENT_EVIDENCE_DECISION_ONLY",
                "q4_access_allowed": False,
                "paid_data_allowed": False,
            }
            created = enqueue_experiment(conn, POST_RETEST_DESIGN_EXPERIMENT_ID, specification)
            set_kv(conn, "post_retest_research_plan_written", True)
            set_kv(
                conn,
                "current_research_experiment_selected",
                {
                    "experiment": POST_RETEST_DESIGN_EXPERIMENT_ID,
                    "status": "QUEUED" if created else "ALREADY_PRESENT",
                    "reason": action.get("rationale"),
                },
            )
            set_kv(conn, "current_phase", "PHASE_6_POST_RETEST_RESEARCH_DESIGN")
        else:
            raise RuntimeError(f"Unsupported mission action {action_type!r}.")

    def _execute_queued_experiment(self, conn: Any) -> None:
        experiment = claim_next_experiment(
            conn,
            claimed_by=f"{self.config.mission_id}:{os.getpid()}",
        )
        if experiment is None:
            return
        experiment_id = str(experiment["experiment_id"])
        experiment_type = str(experiment["experiment_type"])
        try:
            self._check_experiment_allowed(conn, experiment)
        except Exception as exc:
            block_experiment(
                conn,
                experiment_id,
                f"governance_guard:{exc}",
                claim_token=str(experiment["claim_token"]),
            )
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "EXPERIMENT_GOVERNANCE_GUARD_FAILED")
            set_kv(conn, "last_error", str(exc)[:4000])
            return
        if experiment.get("q4_access_allowed") or experiment.get("paid_data_allowed") or "live" in experiment_type.lower():
            reason = "Experiment specification requests a prohibited Q4, paid-data, or live path."
            block_experiment(conn, experiment_id, reason, claim_token=str(experiment["claim_token"]))
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "PROHIBITED_EXPERIMENT_SPECIFICATION")
            set_kv(conn, "last_error", reason)
            return
        set_kv(conn, "current_phase", "RUNNING_EXPERIMENT")
        set_kv(
            conn,
            "current_experiment",
            {
                "experiment_id": experiment_id,
                "experiment_type": experiment_type,
                "specification_hash": experiment.get("specification_hash"),
                "attempt_count": experiment.get("attempt_count"),
                "claim_token": experiment.get("claim_token"),
                "lease_expires_at": experiment.get("lease_expires_at"),
                "started_at_utc": utc_now_iso(),
            },
        )
        set_kv(conn, "last_progress_at_utc", utc_now_iso())
        write_heartbeat(
            self.paths,
            self._heartbeat_payload(
                conn,
                current_action={"action_type": "RUN_QUEUED_EXPERIMENT", "experiment_id": experiment_id},
                checkpoint=str(get_kv(conn, "last_successful_checkpoint", "")),
            ),
        )
        if experiment_type not in SUPPORTED_EXPERIMENT_TYPES:
            exc = RuntimeError(f"No approved handler for experiment type {experiment_type!r}.")
            block_experiment(conn, experiment_id, str(exc), claim_token=str(experiment["claim_token"]))
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "ENGINEERING_BLOCKED")
            set_kv(conn, "current_blocker", f"MISSING_EXPERIMENT_HANDLER:{experiment_type}")
            set_kv(conn, "last_error", str(exc))
            record_evidence(
                self.paths,
                {"scope": "EXPERIMENT", "experiment_id": experiment_id, "status": "BLOCKED", "reason": str(exc)},
            )
            return
        try:
            result = self._run_experiment_with_heartbeat(conn, experiment)
        except CleanWorkerInterruption as exc:
            release_experiment_claim_for_shutdown(
                conn,
                experiment_id,
                claim_token=str(experiment["claim_token"]),
                reason=str(exc),
            )
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "STOPPING")
            set_kv(conn, "current_blocker", None)
            set_kv(conn, "last_error", None)
            append_event(
                conn,
                "experiment_released_for_clean_shutdown",
                {"experiment_id": experiment_id, "reason": str(exc)},
            )
            return
        except ShadowPipelineIntegrityError as exc:
            block_experiment(
                conn,
                experiment_id,
                f"shadow_pipeline_integrity:{exc}",
                claim_token=str(experiment["claim_token"]),
            )
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "SHADOW_PIPELINE_INTEGRITY_FAILURE")
            set_kv(conn, "last_error", str(exc)[:4000])
            return
        except Exception as exc:
            status = fail_experiment(
                conn,
                experiment_id,
                f"{type(exc).__name__}:{exc}",
                retryable=True,
                claim_token=str(experiment["claim_token"]),
            )
            set_kv(conn, "current_experiment", None)
            set_kv(conn, "current_phase", "RETRY_SCHEDULED" if status == "QUEUED" else "EXPERIMENT_BLOCKED")
            set_kv(conn, "current_blocker", None if status == "QUEUED" else f"EXPERIMENT_FAILED:{experiment_id}")
            set_kv(conn, "last_error", f"{type(exc).__name__}:{exc}"[:4000])
            record_evidence(
                self.paths,
                {"scope": "EXPERIMENT", "experiment_id": experiment_id, "status": status, "reason": str(exc)},
            )
            return

        complete_experiment(conn, experiment_id, result, claim_token=str(experiment["claim_token"]))
        self._reconcile_completed_experiments(conn)
        if str(get_kv(conn, "current_phase", "")) not in {
            "INTEGRITY_BLOCKED",
            "ENGINEERING_BLOCKED",
            "EXPERIMENT_BLOCKED",
        }:
            set_kv(conn, "current_blocker", None)
            set_kv(conn, "last_error", None)
            set_kv(conn, "current_phase", "PLANNING_NEXT_ACTION")

    def _check_experiment_allowed(self, conn: Any, experiment: dict[str, Any]) -> None:
        experiment_type = str(experiment.get("experiment_type") or "")
        check_action_allowed(
            {
                "action_type": "Q4_ACCESS"
                if experiment.get("q4_access_allowed")
                else ("LIVE_TRADING" if "live" in experiment_type.lower() else "RUN_RESEARCH_EXPERIMENT"),
                "data_cost": float(experiment.get("data_cost", 0.0)),
            },
            baseline_commit=self.config.baseline_commit,
            remaining_budget_usd=float(
                get_kv(conn, "remaining_databento_budget_usd", self.config.remaining_databento_budget_usd)
            ),
        )
        if experiment.get("q4_access_allowed") or experiment.get("paid_data_allowed") or "live" in experiment_type.lower():
            raise RuntimeError("Experiment specification requests a prohibited Q4, paid-data, or live path.")

    def _run_experiment_with_heartbeat(
        self,
        conn: Any,
        experiment: dict[str, Any],
        *,
        worker_entrypoint: Any = experiment_worker_entry,
    ) -> dict[str, Any]:
        experiment_id = str(experiment["experiment_id"])
        specification_hash = str(experiment["specification_hash"])
        result_dir = self.paths.state_dir / "worker_results"
        result_path = result_dir / f"{experiment_id}_{specification_hash[:16]}.json"
        if result_path.exists():
            envelope = json.loads(result_path.read_text(encoding="utf-8"))
            if (
                envelope.get("ok")
                and envelope.get("experiment_id") == experiment_id
                and envelope.get("specification_hash") == specification_hash
            ):
                return dict(envelope["result"])
            result_path.unlink()

        context = multiprocessing.get_context("spawn")
        worker = context.Process(
            target=worker_entrypoint,
            args=(experiment, str(result_path)),
            name=f"hydra-exp-{experiment_id[:32]}",
        )
        worker.start()
        last_lease_renewal = 0.0
        while worker.is_alive():
            worker.join(timeout=min(max(self.config.sleep_seconds, 1.0), 15.0))
            if self._shutdown or stop_requested(self.paths):
                self._signal_worker_tree(worker, signal.SIGTERM)
                worker.join(timeout=10.0)
                if worker.is_alive():
                    self._signal_worker_tree(worker, signal.SIGKILL)
                    worker.join(timeout=5.0)
                raise CleanWorkerInterruption("experiment_worker_interrupted_for_clean_shutdown")
            now_monotonic = time.monotonic()
            if now_monotonic - last_lease_renewal >= 30.0:
                lease = renew_experiment_lease(
                    conn,
                    experiment_id,
                    str(experiment["claim_token"]),
                    lease_seconds=180.0,
                )
                current = dict(get_kv(conn, "current_experiment", {}) or {})
                current.update({"worker_pid": worker.pid, "lease_expires_at": lease})
                set_kv(conn, "current_experiment", current)
                write_heartbeat(
                    self.paths,
                    self._heartbeat_payload(
                        conn,
                        current_action={
                            "action_type": "RUN_QUEUED_EXPERIMENT",
                            "experiment_id": experiment_id,
                            "worker_pid": worker.pid,
                        },
                        checkpoint=str(get_kv(conn, "last_successful_checkpoint", "")),
                    ),
                )
                try:
                    self._tick_shadow_pipeline(conn)
                except ShadowPipelineIntegrityError:
                    self._signal_worker_tree(worker, signal.SIGTERM)
                    worker.join(timeout=10.0)
                    if worker.is_alive():
                        self._signal_worker_tree(worker, signal.SIGKILL)
                        worker.join(timeout=5.0)
                    raise
                last_lease_renewal = now_monotonic
        worker.join(timeout=1.0)
        if self._shutdown or stop_requested(self.paths):
            raise CleanWorkerInterruption("experiment_worker_interrupted_for_clean_shutdown")
        if not result_path.exists():
            raise RuntimeError(f"experiment_worker_exited_without_result:exitcode={worker.exitcode}")
        envelope = json.loads(result_path.read_text(encoding="utf-8"))
        if envelope.get("experiment_id") != experiment_id or envelope.get("specification_hash") != specification_hash:
            raise RuntimeError("experiment_worker_result_provenance_mismatch")
        if not envelope.get("ok"):
            raise RuntimeError(
                f"worker:{envelope.get('error_type')}:{envelope.get('error')}\n{envelope.get('traceback', '')}"[:8000]
            )
        return dict(envelope["result"])

    @staticmethod
    def _signal_worker_tree(worker: Any, signal_number: int) -> None:
        """Signal an isolated experiment process group without risking the controller group."""
        try:
            process_group = os.getpgid(int(worker.pid))
            if process_group == int(worker.pid):
                os.killpg(process_group, signal_number)
                return
        except (ProcessLookupError, PermissionError, OSError, TypeError, ValueError):
            return
        if signal_number == signal.SIGKILL:
            worker.kill()
        else:
            worker.terminate()

    def _design_specification(self) -> dict[str, Any]:
        return {
            "experiment_type": "calibration_affected_atom_retest_design",
            "priority": 100.0,
            "max_attempts": 3,
            "historical_report_path": "reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md",
            "historical_preregistration_path": "reports/edge_atom_lab/edge_atom_preregistration_20260710T101052+0000_edge_atom_discovery_replication_v1_final.json",
            "code_commit": self._git_commit(),
            "data_role": "HISTORICAL_DEVELOPMENT_EVIDENCE_ONLY",
            "q4_access_allowed": False,
            "paid_data_allowed": False,
            "selection_rule": "bounded_expected_decision_information_gain_with_positive_negative_and_invariant_controls",
        }

    def _tick_shadow_pipeline(self, conn: Any) -> dict[str, Any]:
        registry = dict(get_kv(conn, "shadow_active_registry", {}) or {})
        mission_state_dir = Path(self.config.state_dir).resolve()
        runtime_root = (
            mission_state_dir.parent.parent
            if mission_state_dir.name == "state" and mission_state_dir.parent.name == "mission"
            else mission_state_dir.parent
        )
        state_dir = runtime_root / "shadow" / "state"
        try:
            status = tick_shadow_pipeline(state_dir, registry)
        except ShadowPipelineIntegrityError as exc:
            set_kv(conn, "shadow_pipeline_status", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
            set_kv(conn, "current_blocker", "SHADOW_PIPELINE_INTEGRITY_FAILURE")
            set_kv(conn, "last_error", str(exc)[:4000])
            raise
        set_kv(conn, "shadow_pipeline_status", status["status"])
        set_kv(conn, "shadow_pipeline_runtime", status)
        set_kv(
            conn,
            "shadow_active_candidates",
            int(status.get("shadow_research_active", 0)),
        )
        return status

    def _checkpoint_due(self, conn: Any, action: dict[str, Any], *, progressed: bool) -> bool:
        if progressed:
            return True
        last_epoch = get_kv(conn, "last_checkpoint_epoch", None)
        if last_epoch is None:
            return True
        interval_seconds = max(self.config.checkpoint_every_minutes, 0.1) * 60
        return (time.time() - float(last_epoch)) >= interval_seconds

    def _checkpoint(self, conn: Any) -> Any:
        snapshot = state_snapshot(conn)
        payload = {
            "mission_id": self.config.mission_id,
            "checkpoint_at_utc": utc_now_iso(),
            "snapshot": snapshot,
            "experiments": experiment_counts(conn),
        }
        path = write_mission_checkpoint(self.config.mission_id, payload)
        write_mission_summary(self.config.mission_id, payload)
        set_kv(conn, "last_successful_checkpoint", str(path))
        set_kv(conn, "last_checkpoint_epoch", time.time())
        return path

    def _heartbeat_payload(self, conn: Any, *, current_action: dict[str, Any], checkpoint: str) -> dict[str, Any]:
        snapshot = state_snapshot(conn)
        return {
            "mission_id": self.config.mission_id,
            "controller_version": CONTROLLER_VERSION,
            "current_phase": snapshot.get("current_phase"),
            "scheduler_state": snapshot.get("current_phase"),
            "current_action": current_action,
            "cycle_count": snapshot.get("cycle_count", 0),
            "progress_sequence": snapshot.get("progress_sequence", 0),
            "last_progress_at_utc": snapshot.get("last_progress_at_utc"),
            "next_wake_at_utc": snapshot.get("next_wake_at_utc"),
            "experiment_counts": experiment_counts(conn),
            "queue_size": queue_size(conn),
            "current_experiment": snapshot.get("current_experiment"),
            "latest_completed_experiment": snapshot.get("latest_completed_experiment"),
            "latest_scientific_finding": snapshot.get("latest_scientific_finding"),
            "validated_mechanisms": snapshot.get("validated_mechanisms", 0),
            "validated_strategies": snapshot.get("validated_strategies", 0),
            "executable_baskets": snapshot.get("executable_baskets", 0),
            "foundry_current_engine": snapshot.get("foundry_current_engine"),
            "strategy_prototypes_generated": snapshot.get("strategy_prototypes_generated", 0),
            "strategies_screened": snapshot.get("strategies_screened", 0),
            "promising_candidates": snapshot.get("promising_candidates", 0),
            "shadow_candidates": snapshot.get("shadow_candidates", 0),
            "paper_shadow_ready_candidates": snapshot.get(
                "paper_shadow_ready_candidates", 0
            ),
            "shadow_active_candidates": snapshot.get("shadow_active_candidates", 0),
            "discovery_pipeline_status": snapshot.get(
                "discovery_pipeline_status", "NOT_INITIALIZED"
            ),
            "promotion_pipeline_status": snapshot.get(
                "promotion_pipeline_status", "NOT_INITIALIZED"
            ),
            "shadow_pipeline_status": snapshot.get(
                "shadow_pipeline_status", "NOT_INITIALIZED"
            ),
            "shadow_pipeline_runtime": snapshot.get("shadow_pipeline_runtime", {}),
            "mechanisms_represented": snapshot.get("mechanisms_represented", 0),
            "market_ecologies_represented": snapshot.get(
                "market_ecologies_represented", 0
            ),
            "timeframes_represented": snapshot.get("timeframes_represented", 0),
            "strategies_killed": snapshot.get("strategies_killed", 0),
            "lineages_frozen": snapshot.get("lineages_frozen", 0),
            "topstep_path_candidates": snapshot.get("topstep_path_candidates", 0),
            "q4_candidates": snapshot.get("q4_candidates", 0),
            "model_quota_state": snapshot.get("model_quota_state", "UNKNOWN"),
            "last_meaningful_progress_at_utc": snapshot.get(
                "last_meaningful_progress_at_utc"
            ),
            "foundry_next_planned_action": snapshot.get("foundry_next_planned_action"),
            "current_blocker": snapshot.get("current_blocker"),
            "cumulative_databento_spend_usd": snapshot.get("cumulative_databento_spend_usd"),
            "remaining_databento_budget_usd": snapshot.get("remaining_databento_budget_usd"),
            "q4_access_count": snapshot.get("q4_access_count", 0),
            "latest_checkpoint": checkpoint,
            "latest_commit": self._git_commit(),
            "governance_baseline_commit": self.config.baseline_commit,
            "crash_count": snapshot.get("crash_count", 0),
            "controller_start_count": snapshot.get("controller_start_count", 0),
            "last_error": snapshot.get("last_error"),
        }

    def _stop_cleanly(self, conn: Any, reason: str) -> None:
        set_kv(conn, "stop_reason", reason)
        set_kv(conn, "service_state", "STOPPED_CLEANLY")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "current_phase", "STOPPED_CLEANLY")
        checkpoint = str(self._checkpoint(conn))
        write_heartbeat(
            self.paths,
            self._heartbeat_payload(conn, current_action={"action_type": "STOPPED", "reason": reason}, checkpoint=checkpoint),
        )

    def _next_wake_at(self) -> str:
        return (datetime.now(timezone.utc) + timedelta(seconds=max(self.config.sleep_seconds, 0.0))).replace(microsecond=0).isoformat()

    @staticmethod
    def _git_commit() -> str:
        try:
            return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return "unknown"

    def _handle_signal(self, _signum: int, _frame: Any) -> None:
        self._shutdown = True


def run_controller(config: MissionControllerConfig) -> int:
    return AutonomousMissionController(config).run()
