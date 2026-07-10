from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RunControlConfig:
    min_runtime_hours: float = 0.0
    max_runtime_hours: float = 6.0
    continue_until_deadline: bool = False
    minimum_cycles: int = 1
    minimum_remediation_children: int = 0
    stop_only_on_valid_quality_target: bool = False
    allow_early_stop_on_exhaustion: bool = False


@dataclass(frozen=True)
class RunControlState:
    elapsed_seconds: float
    cycles_completed: int
    remediation_children_completed: int
    queue_size: int
    eligible_parents: int
    valid_quality_target_reached: bool = False
    critical_integrity_failure: bool = False
    budget_violation: bool = False
    destructive_risk: bool = False
    manual_interruption: bool = False
    proven_work_exhaustion: bool = False
    provisional_quality_target_reached: bool = False
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class StopDecision:
    should_stop: bool
    reason: str
    diagnostics: dict[str, object]


def evaluate_stop(config: RunControlConfig, state: RunControlState) -> StopDecision:
    min_runtime_met = state.elapsed_seconds >= config.min_runtime_hours * 3600.0
    max_runtime_met = state.elapsed_seconds >= config.max_runtime_hours * 3600.0
    min_cycles_met = state.cycles_completed >= config.minimum_cycles
    min_children_met = state.remediation_children_completed >= config.minimum_remediation_children
    diagnostics = {
        "elapsed_seconds": round(state.elapsed_seconds, 2),
        "min_runtime_met": min_runtime_met,
        "max_runtime_met": max_runtime_met,
        "cycles_completed": state.cycles_completed,
        "minimum_cycles": config.minimum_cycles,
        "min_cycles_met": min_cycles_met,
        "remediation_children_completed": state.remediation_children_completed,
        "minimum_remediation_children": config.minimum_remediation_children,
        "min_children_met": min_children_met,
        "queue_size": state.queue_size,
        "eligible_parents": state.eligible_parents,
        "valid_quality_target_reached": state.valid_quality_target_reached,
        "provisional_quality_target_reached": state.provisional_quality_target_reached,
        "proven_work_exhaustion": state.proven_work_exhaustion,
        "continue_until_deadline": config.continue_until_deadline,
    }
    if state.critical_integrity_failure:
        return StopDecision(True, "critical_integrity_failure", diagnostics)
    if state.budget_violation:
        return StopDecision(True, "budget_violation", diagnostics)
    if state.destructive_risk:
        return StopDecision(True, "destructive_risk", diagnostics)
    if state.manual_interruption:
        return StopDecision(True, "manual_interruption", diagnostics)
    if max_runtime_met:
        return StopDecision(True, "max_runtime_reached", diagnostics)
    if state.valid_quality_target_reached and min_runtime_met and min_cycles_met and min_children_met and not config.continue_until_deadline:
        return StopDecision(True, "valid_quality_target_reached", diagnostics)
    if config.stop_only_on_valid_quality_target and state.provisional_quality_target_reached and not state.valid_quality_target_reached:
        diagnostics["provisional_target_ignored"] = True
    if (
        state.proven_work_exhaustion
        and config.allow_early_stop_on_exhaustion
        and min_runtime_met
        and min_cycles_met
        and min_children_met
    ):
        return StopDecision(True, "proven_work_exhaustion", diagnostics)
    return StopDecision(False, "continue", diagnostics)


def validated_quality_target_reached(*, trading_ready: int, q4_passes: int, execution_passes: int, target_units: int) -> bool:
    return trading_ready >= target_units and q4_passes >= target_units and execution_passes >= target_units
