from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AblationPlan:
    candidate_id: str
    ablations: list[dict[str, Any]]


def build_ablation_plan(candidate_row: dict[str, Any]) -> AblationPlan:
    ablations = [
        {"component": "session_filter", "test": "remove_or_broaden_session", "cost": "low"},
        {"component": "exit_policy", "test": "compare_time_stop_vs_adaptive_exit", "cost": "medium"},
        {"component": "sizing", "test": "fixed_micro_vs_dynamic_buffer_sizing", "cost": "medium"},
        {"component": "risk_controls", "test": "daily_lock_and_daily_stop_sensitivity", "cost": "low"},
        {"component": "timing", "test": "one_to_three_bar_entry_delay", "cost": "medium"},
        {"component": "costs", "test": "double_commission_and_slippage", "cost": "low"},
    ]
    return AblationPlan(str(candidate_row["candidate_id"]), ablations)

