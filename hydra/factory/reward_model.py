from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


DEFAULT_POLICY_CAP = 0.35
DEFAULT_FAMILY_CAP = 0.30
DEFAULT_LINEAGE_CAP = 0.02
DEFAULT_MIN_EXPLORATION = 0.15

STAGE_RANK = {
    "GENERATED": 0,
    "BACKTESTED": 1,
    "COST_ADJUSTED": 2,
    "NO_LOOKAHEAD_PASSED": 3,
    "WALK_FORWARD_PASSED": 4,
    "OOS_PASSED": 5,
    "MONTE_CARLO_PASSED": 6,
    "PARAMETER_SENSITIVITY_PASSED": 7,
    "TOPSTEP_COMBINE_PASSED": 8,
    "FUNDED_XFA_PASSED": 9,
    "PAYOUT_SURVIVAL_PASSED": 10,
    "CORRELATION_PASSED": 11,
    "PORTFOLIO_INTERACTION_PASSED": 12,
    "EXECUTION_READINESS_PASSED": 13,
    "TRADING_READY_CANDIDATE": 14,
}

STATUS_RANK = {
    "DEAD_STRATEGY": 0,
    "PROMISING_NEEDS_MUTATION": 1,
    "ECONOMICALLY_VIABLE": 2,
    "TOPSTEP_NEAR_MISS": 3,
    "TOPSTEP_VIABLE": 4,
    "TRADING_READY_CANDIDATE": 5,
}


@dataclass(frozen=True)
class RewardBreakdown:
    hard_invalid: float = 0.0
    duplicate: float = 0.0
    local_improvement: float = 0.0
    gate_distance_reduction: float = 0.0
    passed_failed_gate: float = 0.0
    q1_core_robust: float = 0.0
    oos_pass: float = 0.0
    q2_confirmation: float = 0.0
    new_behavior_cluster: float = 0.0
    complexity_penalty: float = 0.0
    compute_cost_penalty: float = 0.0
    tail_or_mll_regression: float = 0.0
    provisional_status_penalty: float = 0.0

    @property
    def total(self) -> float:
        return round(sum(asdict(self).values()), 6)

    def to_dict(self) -> dict[str, float]:
        out = asdict(self)
        out["total"] = self.total
        return out


def promotion_aligned_reward(
    parent: dict[str, Any] | None,
    child: dict[str, Any],
    *,
    compute_seconds: float = 0.0,
    duplicate: bool = False,
    new_behavior_cluster: bool = False,
    q2_confirmed: bool = False,
) -> RewardBreakdown:
    if duplicate:
        return RewardBreakdown(duplicate=-0.35, compute_cost_penalty=-_compute_penalty(compute_seconds))
    child_status = str(child.get("validation_status") or child.get("status") or "")
    child_stage = str(child.get("promotion_stage") or "")
    child_score = float(child.get("promotion_score") or 0.0)
    parent_score = float((parent or {}).get("promotion_score") or 0.0)
    child_failed = failed_gate_names(child)
    parent_failed = failed_gate_names(parent or {})
    hard_invalid = -2.0 if child_status == "DEAD_STRATEGY" and any(_is_hard_failure(item) for item in child.get("gate_history", []) or []) else 0.0
    local = max(0.0, min(0.10, child_score - parent_score))
    reduced = max(0, len(parent_failed) - len(child_failed)) * 0.35
    passed = len(parent_failed.intersection(set(_passed_gate_names(child)))) * 0.75
    oos_pass = 4.0 if "OOS" in _passed_gate_names(child) else 0.0
    q1_core = 1.25 if q1_core_robust(child) else 0.0
    q2 = 5.0 if q2_confirmed else 0.0
    cluster = 0.85 if new_behavior_cluster else 0.0
    complexity = -0.08 * max(0, int(child.get("complexity_delta") or 0))
    mll = -1.25 if _mll_regressed(parent, child) else 0.0
    provisional = -0.60 if _provisional_only(child) else 0.0
    return RewardBreakdown(
        hard_invalid=hard_invalid,
        local_improvement=local,
        gate_distance_reduction=reduced,
        passed_failed_gate=passed,
        q1_core_robust=q1_core,
        oos_pass=oos_pass,
        q2_confirmation=q2,
        new_behavior_cluster=cluster,
        complexity_penalty=complexity,
        compute_cost_penalty=-_compute_penalty(compute_seconds),
        tail_or_mll_regression=mll,
        provisional_status_penalty=provisional,
    )


def policy_allocation_caps(
    *,
    policy_cap: float = DEFAULT_POLICY_CAP,
    family_cap: float = DEFAULT_FAMILY_CAP,
    lineage_cap: float = DEFAULT_LINEAGE_CAP,
    min_exploration: float = DEFAULT_MIN_EXPLORATION,
    validated_oos_or_q2_progress: bool = False,
) -> dict[str, float]:
    if validated_oos_or_q2_progress:
        policy_cap = min(0.55, policy_cap + 0.10)
        family_cap = min(0.45, family_cap + 0.05)
        lineage_cap = min(0.05, lineage_cap + 0.01)
    return {
        "max_policy_share": round(policy_cap, 4),
        "max_family_share": round(family_cap, 4),
        "max_lineage_share": round(lineage_cap, 4),
        "minimum_controlled_exploration_share": round(min_exploration, 4),
    }


def reward_direction_is_valid(before: float, after: float, reward: RewardBreakdown) -> bool:
    if after < before and reward.local_improvement > 0:
        return False
    return True


def failed_gate_names(row: dict[str, Any]) -> set[str]:
    return {str(g.get("name")) for g in _safe_gate_history(row) if not bool(g.get("passed"))}


def _passed_gate_names(row: dict[str, Any]) -> set[str]:
    return {str(g.get("name")) for g in _safe_gate_history(row) if bool(g.get("passed"))}


def q1_core_robust(row: dict[str, Any]) -> bool:
    passed = _passed_gate_names(row)
    required = {
        "DATA_INTEGRITY",
        "DUPLICATE_FINGERPRINT",
        "NO_LOOKAHEAD",
        "ECONOMIC_PROFILE",
        "WALK_FORWARD",
        "MONTE_CARLO",
        "PARAMETER_SENSITIVITY",
        "TOPSTEP_COMBINE",
        "FUNDED_XFA",
        "PAYOUT_SURVIVAL",
        "CORRELATION",
        "PORTFOLIO_INTERACTION",
        "EXECUTION_READINESS",
    }
    return required.issubset(passed)


def _safe_gate_history(row: dict[str, Any]) -> list[dict[str, Any]]:
    value = row.get("gate_history")
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    value = row.get("gate_history_json")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _is_hard_failure(gate: dict[str, Any]) -> bool:
    return not bool(gate.get("passed")) and str(gate.get("severity")) == "HARD_FAIL"


def _mll_regressed(parent: dict[str, Any] | None, child: dict[str, Any]) -> bool:
    if not parent:
        return False
    parent_buffer = float(parent.get("combine_min_mll_buffer") or parent.get("mll_buffer") or 0.0)
    child_buffer = float(child.get("combine_min_mll_buffer") or child.get("mll_buffer") or 0.0)
    if child.get("combine_mll_breached"):
        return True
    return child_buffer < parent_buffer - 250.0


def _provisional_only(row: dict[str, Any]) -> bool:
    status = str(row.get("validation_status") or row.get("status") or "")
    return status == "TRADING_READY_CANDIDATE" and not row.get("q4_lockbox_passed")


def _compute_penalty(seconds: float) -> float:
    return min(0.25, max(0.0, seconds) / 600.0)
