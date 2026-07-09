from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd


PASS = "PASS"
WARNING = "WARNING"
SOFT_FAIL = "SOFT_FAIL"
HARD_FAIL = "HARD_FAIL"

REJECT = "reject"
MUTATE = "mutate"
RETEST = "retest"
PROMOTE = "promote"


@dataclass(frozen=True)
class GateResult:
    name: str
    passed: bool
    score: float
    reason: str
    severity: str
    recommended_action: str
    failure_mode: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def gate(name: str, passed: bool, score: float, reason: str, severity: str, action: str, failure_mode: str) -> GateResult:
    return GateResult(name, passed, round(float(max(0.0, min(1.0, score))), 6), reason, severity, action, failure_mode)


def strategy_fingerprint(candidate) -> str:
    payload = {
        "family": candidate.family,
        "symbol": candidate.symbol,
        "timeframe": candidate.timeframe,
        "parameters": candidate.parameters,
        "risk_parameters": candidate.risk_parameters,
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def parameter_zone(candidate) -> str:
    payload = {
        "family": candidate.family,
        "symbol": candidate.symbol,
        "risk_bucket": round(float(candidate.risk_parameters.get("risk_scale", 1.0)), 1),
        "hold": int(candidate.risk_parameters.get("holding_period", 0)),
        "exit": candidate.risk_parameters.get("exit_policy", "time_or_opposite"),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def data_integrity_gate(data_validation: dict[str, Any]) -> GateResult:
    bad = data_validation.get("duplicate_timestamp_symbol_rows", 0) or data_validation.get("future_timestamp_rows", 0)
    return gate("DATA_INTEGRITY", not bad, 1.0 if not bad else 0.0, "passed" if not bad else "corrupted_data", HARD_FAIL if bad else PASS, REJECT if bad else PROMOTE, "reject" if bad else "none")


def no_lookahead_gate(leak_ok: bool, reason: str) -> GateResult:
    return gate("NO_LOOKAHEAD", leak_ok, 1.0 if leak_ok else 0.0, reason, PASS if leak_ok else HARD_FAIL, PROMOTE if leak_ok else REJECT, "none" if leak_ok else "reject")


def economic_gate(metrics: dict[str, float], daily: pd.DataFrame) -> GateResult:
    pnl = float(metrics.get("net_profit", 0.0))
    pf = float(metrics.get("profit_factor", 0.0))
    trades = float(metrics.get("trade_count", 0.0))
    worst_day = float(daily["pnl"].min()) if len(daily) else 0.0
    score = 0.35 * _clip01(pnl / 6000.0) + 0.25 * _clip01(pf / 1.4) + 0.20 * _clip01(trades / 80.0) + 0.20 * _clip01(1.0 - abs(min(worst_day, 0.0)) / 2500.0)
    if pnl > 0 and pf >= 1.05 and trades >= 20:
        return gate("ECONOMIC_PROFILE", True, score, "economic_signal_detected", PASS, PROMOTE, "none")
    if pnl > -1000 and trades >= 10:
        return gate("ECONOMIC_PROFILE", False, score, "weak_but_mutatable_economic_profile", SOFT_FAIL, MUTATE, "mutate")
    return gate("ECONOMIC_PROFILE", False, score, "no_economic_signal", HARD_FAIL, REJECT, "reject")


def walk_forward_gate(split_scores: dict[str, float]) -> GateResult:
    if not split_scores:
        return gate("WALK_FORWARD", False, 0.0, "missing_split_scores", SOFT_FAIL, RETEST, "retest")
    passed = sum(1 for value in split_scores.values() if value >= 0.35)
    score = float(np.mean(list(split_scores.values())))
    if passed >= 2:
        return gate("WALK_FORWARD", True, score, "month_to_month_profile_acceptable", PASS, PROMOTE, "none")
    return gate("WALK_FORWARD", False, score, "viable_only_in_one_split", SOFT_FAIL, MUTATE, "mutate")


def oos_gate(split_scores: dict[str, float]) -> GateResult:
    mar = float(split_scores.get("mar", 0.0))
    if mar >= 0.35:
        return gate("OOS", True, mar, "march_oos_acceptable", PASS, PROMOTE, "none")
    return gate("OOS", False, mar, "march_oos_weak", SOFT_FAIL, MUTATE, "mutate")


def monte_carlo_gate(result, seed: int) -> GateResult:
    from hydra.validation.monte_carlo import monte_carlo_reshuffle_score

    score = monte_carlo_reshuffle_score(result.trades, seed, trials=100)
    if score >= 0.45:
        return gate("MONTE_CARLO", True, score, "reshuffle_robustness_acceptable", PASS, PROMOTE, "none")
    if score >= 0.20:
        return gate("MONTE_CARLO", False, score, "reshuffle_robustness_soft_fail", SOFT_FAIL, MUTATE, "mutate")
    return gate("MONTE_CARLO", False, score, "fragile_trade_order", HARD_FAIL, REJECT, "reject")


def parameter_sensitivity_gate(candidate) -> GateResult:
    risk = float(candidate.risk_parameters.get("risk_scale", 1.0))
    hold = int(candidate.risk_parameters.get("holding_period", 8))
    score = 1.0
    if risk > 1.4 or hold <= 1:
        score = 0.25
    elif risk > 1.0:
        score = 0.65
    if score >= 0.65:
        return gate("PARAMETER_SENSITIVITY", True, score, "parameter_zone_not_extreme", PASS, PROMOTE, "none")
    return gate("PARAMETER_SENSITIVITY", False, score, "parameter_zone_too_fragile", SOFT_FAIL, MUTATE, "mutate")


def topstep_combine_gate(evaluation: dict[str, Any]) -> GateResult:
    if evaluation.get("combine_mll_breached"):
        return gate("TOPSTEP_COMBINE", False, 0.0, "combine_mll_breached", HARD_FAIL, MUTATE, "mutate")
    if evaluation.get("topstep_passed"):
        return gate("TOPSTEP_COMBINE", True, 1.0, "combine_passed", PASS, PROMOTE, "none")
    target_ratio = _clip01(float(evaluation.get("adjusted_net_profit", 0.0)) / 9000.0)
    if target_ratio >= 0.60 and evaluation.get("combine_consistency_ok"):
        return gate("TOPSTEP_COMBINE", False, target_ratio, "topstep_near_miss_target_velocity", SOFT_FAIL, MUTATE, "mutate")
    return gate("TOPSTEP_COMBINE", False, target_ratio, "combine_target_not_reached", SOFT_FAIL, MUTATE, "mutate")


def funded_gate(evaluation: dict[str, Any]) -> GateResult:
    if evaluation.get("funded_sim_survived"):
        return gate("FUNDED_XFA", True, 1.0, "funded_survived", PASS, PROMOTE, "none")
    return gate("FUNDED_XFA", False, 0.0, "funded_mll_or_tail_failure", SOFT_FAIL, MUTATE, "mutate")


def payout_gate(evaluation: dict[str, Any]) -> GateResult:
    if evaluation.get("payout_eligible") and not evaluation.get("post_payout_mll_breach"):
        score = _clip01(float(evaluation.get("trader_net_payout", 0.0)) / 4500.0)
        return gate("PAYOUT_SURVIVAL", True, score, "payout_profile_available", PASS, PROMOTE, "none")
    return gate("PAYOUT_SURVIVAL", False, 0.0, "payout_profile_weak", SOFT_FAIL, MUTATE, "mutate")


def duplicate_gate(fingerprint: str, existing_fingerprints: set[str]) -> GateResult:
    duplicate = fingerprint in existing_fingerprints
    return gate("DUPLICATE_FINGERPRINT", not duplicate, 1.0 if not duplicate else 0.0, "duplicate" if duplicate else "unique", HARD_FAIL if duplicate else PASS, REJECT if duplicate else PROMOTE, "reject" if duplicate else "none")


def correlation_gate(max_corr: float) -> GateResult:
    if max_corr >= 0.95:
        return gate("CORRELATION", False, 0.0, "duplicate_or_near_duplicate_equity_curve", HARD_FAIL, REJECT, "reject")
    if max_corr >= 0.85:
        return gate("CORRELATION", False, 0.4, "high_correlation_needs_portfolio_role", SOFT_FAIL, RETEST, "retest")
    return gate("CORRELATION", True, 1.0, "correlation_acceptable", PASS, PROMOTE, "none")


def execution_readiness_gate(candidate, exported: bool) -> GateResult:
    has_limits = all(k in candidate.risk_parameters for k in ("internal_daily_stop", "daily_profit_lock", "max_position"))
    passed = bool(exported and has_limits)
    return gate("EXECUTION_READINESS", passed, 1.0 if passed else 0.25, "ready_config_exported" if passed else "missing_risk_export", PASS if passed else SOFT_FAIL, PROMOTE if passed else RETEST, "none" if passed else "retest")


def portfolio_interaction_gate(evaluation: dict[str, Any], max_corr: float) -> GateResult:
    score = min(float(evaluation.get("topstep_score", 0.0)), 1.0 - max_corr)
    passed = score >= 0.55 and max_corr < 0.85
    return gate("PORTFOLIO_INTERACTION", passed, score, "portfolio_role_acceptable" if passed else "portfolio_role_needs_retest", PASS if passed else SOFT_FAIL, PROMOTE if passed else RETEST, "none" if passed else "retest")


def _clip01(value: float) -> float:
    if np.isnan(value):
        return 0.0
    return float(max(0.0, min(1.0, value)))
