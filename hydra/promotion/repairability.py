from __future__ import annotations

from typing import Any


def repairability_score(row: dict[str, Any], attribution: dict[str, Any]) -> float:
    if attribution["policy_classification"] == "HARD_INVALID":
        return 0.0
    failed_count = int(attribution["failed_gate_count"])
    promotion = float(row.get("promotion_score") or 0.0)
    topstep = float(row.get("topstep_score") or 0.0)
    economic = float(row.get("economic_score") or 0.0)
    mll_ok = 1.0 if not bool(row.get("combine_mll_breached")) else 0.0
    target_hit = 1.0 if bool(row.get("combine_profit_target_hit")) else 0.0
    one_gate_bonus = 0.20 if failed_count == 1 else 0.0
    two_gate_bonus = 0.10 if failed_count == 2 else 0.0
    penalty = min(failed_count, 8) * 0.04
    score = 0.35 * promotion + 0.25 * topstep + 0.20 * economic + 0.10 * mll_ok + 0.10 * target_hit
    return round(max(0.0, min(1.0, score + one_gate_bonus + two_gate_bonus - penalty)), 6)


def recommended_mutation_class(row: dict[str, Any], attribution: dict[str, Any]) -> str:
    reason = str(attribution.get("primary_reason") or "")
    if "target" in reason:
        return "target_velocity_repair"
    if "mll" in reason:
        return "mll_buffer_risk_repair"
    if "consistency" in reason or "spike" in reason:
        return "consistency_smoothing_repair"
    if "oos" in reason or "split" in reason:
        return "stability_simplification_repair"
    if "reshuffle" in reason or "fragile" in reason:
        return "sequence_fragility_repair"
    if "payout" in reason:
        return "payout_cycle_repair"
    if "correlation" in reason or "duplicate" in reason:
        return "portfolio_role_or_cluster_backup"
    return "diagnostic_retest"

