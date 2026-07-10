from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np


TEMPORAL_POLICY_VERSION = "temporal_transfer_policy_v1"


@dataclass(frozen=True)
class TemporalTransferDecision:
    policy_version: str
    candidate_id: str
    status: str
    positive_periods: int
    pooled_net_pnl: float
    third_period_catastrophic: bool
    top_trade_concentration: float
    null_period_passes: int
    pooled_null_passed: bool
    decision_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_temporal_transfer(
    candidate_id: str,
    period_results: dict[str, dict[str, Any]],
    null_decisions: dict[str, dict[str, Any]],
    pooled_null_decision: dict[str, Any],
    *,
    min_total_trades: int = 30,
) -> TemporalTransferDecision:
    period_nets = [float(period_results[key]["net_pnl"]) for key in sorted(period_results)]
    total_trades = sum(int(period_results[key]["trade_count"]) for key in period_results)
    pooled_net = float(sum(period_nets))
    positive_periods = sum(1 for value in period_nets if value > 0)
    worst_period = min(period_nets) if period_nets else 0.0
    third_catastrophic = bool(worst_period < -max(abs(pooled_net), 1.0) * 1.5 and pooled_net > 0)
    trade_pnls = [float(trade["net_pnl"]) for result in period_results.values() for trade in result.get("trades", [])]
    concentration = _top_trade_concentration(trade_pnls, top_n=1)
    null_passes = sum(1 for value in null_decisions.values() if value.get("passed"))
    pooled_null_passed = bool(pooled_null_decision.get("passed"))
    if total_trades < min_total_trades:
        status = "INSUFFICIENT_EVIDENCE"
        reason = "total_trade_count_below_minimum"
    elif positive_periods >= 2 and pooled_net > 0 and not third_catastrophic and concentration <= 0.60 and pooled_null_passed and null_passes >= 2:
        status = "TEMPORAL_TRANSFER_STRONG"
        reason = "positive_in_two_periods_pooled_positive_candidate_nulls_passed"
    elif positive_periods >= 2 and pooled_net >= -500.0 and concentration <= 0.75 and (pooled_null_passed or null_passes >= 2):
        status = "TEMPORAL_TRANSFER_WEAK"
        reason = "mostly_coherent_but_evidence_or_economics_not_strong"
    else:
        status = "TEMPORAL_TRANSFER_FAILED"
        reason = "pooled_negative_or_nulls_failed_or_period_instability"
    return TemporalTransferDecision(
        policy_version=TEMPORAL_POLICY_VERSION,
        candidate_id=candidate_id,
        status=status,
        positive_periods=int(positive_periods),
        pooled_net_pnl=float(round(pooled_net, 6)),
        third_period_catastrophic=third_catastrophic,
        top_trade_concentration=float(round(concentration, 6)),
        null_period_passes=int(null_passes),
        pooled_null_passed=pooled_null_passed,
        decision_reason=reason,
    )


def _top_trade_concentration(trade_pnls: list[float], top_n: int) -> float:
    positives = sorted([value for value in trade_pnls if value > 0], reverse=True)
    total_positive = sum(positives)
    if total_positive <= 0:
        return 0.0
    return float(sum(positives[:top_n]) / total_positive)


def period_metric_summary(trades: list[dict[str, Any]], *, gross_pnl: float, costs: float, net_pnl: float) -> dict[str, Any]:
    pnls = [float(trade["net_pnl"]) for trade in trades]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    daily: dict[str, float] = {}
    for trade in trades:
        date = str(trade["exit_timestamp"])[:10]
        daily[date] = daily.get(date, 0.0) + float(trade["net_pnl"])
    daily_values = list(daily.values())
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "trade_count": len(trades),
        "gross_pnl": float(gross_pnl),
        "commissions": float(costs),
        "slippage": 0.0,
        "net_pnl": float(net_pnl),
        "expectancy": float(np.mean(pnls)) if pnls else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss else (999.0 if gross_profit > 0 else 0.0),
        "win_rate": float(len(wins) / len(pnls)) if pnls else 0.0,
        "average_win": float(np.mean(wins)) if wins else 0.0,
        "average_loss": float(np.mean(losses)) if losses else 0.0,
        "max_drawdown": float(_max_drawdown(pnls)),
        "worst_trade": float(min(pnls)) if pnls else 0.0,
        "best_trade": float(max(pnls)) if pnls else 0.0,
        "worst_day": float(min(daily_values)) if daily_values else 0.0,
        "best_day": float(max(daily_values)) if daily_values else 0.0,
        "losing_streak": int(_max_losing_streak(pnls)),
        "top_1_trade_profit_pct": _top_trade_concentration(pnls, 1),
        "top_3_trade_profit_pct": _top_trade_concentration(pnls, 3),
        "top_5_trade_profit_pct": _top_trade_concentration(pnls, 5),
        "daily_pnl": daily,
    }


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return abs(max_dd)


def _max_losing_streak(pnls: list[float]) -> int:
    current = 0
    best = 0
    for pnl in pnls:
        if pnl < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best
