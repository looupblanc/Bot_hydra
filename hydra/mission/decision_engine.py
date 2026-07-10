from __future__ import annotations

from typing import Any

from hydra.mission.information_value import MissionAction, rank_actions


def generate_actions(state: dict[str, Any]) -> list[MissionAction]:
    actions: list[MissionAction] = []
    if not state.get("validator_calibration_passed"):
        actions.append(
            MissionAction(
                "validator_calibration_suite",
                "RUN_VALIDATOR_CALIBRATION",
                1.0,
                0.95,
                0.90,
                0.10,
                0.0,
                0.0,
                0.05,
                0.05,
                "The latest atom batch had many replications and zero adversarial passes; validator calibration is the dominant uncertainty.",
            )
        )
    if state.get("validator_calibration_passed") and not state.get("zero_pass_audited"):
        actions.append(
            MissionAction(
                "audit_previous_zero_adversarial_pass",
                "AUDIT_ZERO_PASS_RESULT",
                0.90,
                0.85,
                0.75,
                0.05,
                0.0,
                0.0,
                0.05,
                0.03,
                "Calibrated controls can distinguish true negative evidence from over-strict attack/cost policy.",
            )
        )
    if state.get("zero_pass_audited") and not state.get("bounded_retest_plan_written"):
        actions.append(
            MissionAction(
                "plan_calibration_affected_atom_versions",
                "PLAN_BOUNDED_RETESTS",
                0.70,
                0.55,
                0.70,
                0.15,
                0.0,
                0.0,
                0.10,
                0.05,
                "Retest only atom structures whose old decision was calibration-affected, with new IDs and preregistration.",
            )
        )
    return actions


def select_best_action(state: dict[str, Any]) -> MissionAction:
    actions = generate_actions(state)
    if not actions:
        return MissionAction("heartbeat_wait", "WAIT", 0.1, 0.1, 0.1, 0.01, 0.0, 0.0, 0.0, 0.0, "No immediately executable action.")
    return rank_actions(actions)[0]
