from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Topstep150KConfig:
    program: str = "topstep"
    account_size: float = 150000.0
    combine_profit_target: float = 9000.0
    combine_max_loss_limit: float = 4500.0
    combine_starting_balance: float = 150000.0
    no_daily_loss_limit: bool = True
    optional_daily_loss_limit: float = 3000.0
    use_optional_daily_loss_limit: bool = False
    consistency_best_day_max_pct_of_profit_target: float = 0.50
    minimum_pass_days: int = 2
    funded_starting_balance: float = 0.0
    funded_starting_mll: float = -4500.0
    payout_eligibility_winning_days: int = 5
    payout_winning_day_min_profit: float = 150.0
    payout_max_pct_of_balance: float = 0.50
    payout_cap: float = 5000.0
    profit_split_trader: float = 0.90
    funded_consistency_enabled: bool = True
    funded_consistency_largest_day_max_pct_of_total_profit: float = 0.40
    internal_daily_stop_enabled: bool = True
    max_losing_days_per_10_trading_days: int = 5
    max_consecutive_losing_days_limit: int = 3

    @property
    def combine_starting_mll(self) -> float:
        return self.combine_starting_balance - self.combine_max_loss_limit


@dataclass(frozen=True)
class InternalRiskOverlay:
    daily_stop: float
    daily_profit_lock: float
    stop_after_daily_target: bool = True
    stop_after_daily_stop: bool = True


@dataclass
class TopstepEvaluation:
    status: str
    rejection_reason: str | None
    topstep_passed: bool
    topstep_score: float
    combine_days_to_pass: int | None
    combine_profit_target_hit: bool
    combine_mll_breached: bool
    combine_min_mll_buffer: float
    combine_best_day_profit: float
    combine_best_day_pct_of_total_profit: float
    combine_consistency_ok: bool
    target_inflation_required: bool
    funded_sim_survived: bool
    payout_eligible: bool
    payout_days_to_eligibility: int | None
    payout_cycles_survived: int
    gross_payout_available: float
    trader_net_payout: float
    post_payout_mll_breach: bool
    internal_daily_stop_used: float
    daily_profit_lock_used: float
    worst_day_loss: float
    max_consecutive_losing_days: int
    winning_days_150_count: int
    days_traded: int
    adjusted_net_profit: float
    skipped_trades: int
    trade_count: int
    pass_split_count: int
    split_scores: dict[str, float]

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


def trades_to_topstep_daily(trades: list[dict], df: pd.DataFrame, overlay: InternalRiskOverlay) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame(
            columns=[
                "date",
                "pnl",
                "raw_pnl",
                "worst_intraday_pnl",
                "trades",
                "skipped_trades",
                "hit_daily_stop",
                "hit_daily_profit_lock",
            ]
        )
    timestamps = pd.to_datetime(df["timestamp"], utc=True).reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for trade in trades:
        exit_i = min(max(int(trade["exit_i"]), 0), len(timestamps) - 1)
        rows.append(
            {
                "date": timestamps.iloc[exit_i].date().isoformat(),
                "pnl": float(trade["pnl"]),
                "mae": float(trade.get("mae", min(0.0, trade["pnl"]))),
            }
        )
    trade_df = pd.DataFrame(rows)
    days: list[dict[str, Any]] = []
    for date, group in trade_df.groupby("date", sort=True):
        adjusted = 0.0
        raw = 0.0
        worst_intraday = 0.0
        skipped = 0
        hit_stop = False
        hit_lock = False
        taken = 0
        for row in group.itertuples(index=False):
            raw += float(row.pnl)
            if overlay.stop_after_daily_stop and adjusted <= -overlay.daily_stop:
                skipped += 1
                hit_stop = True
                continue
            if overlay.stop_after_daily_target and adjusted >= overlay.daily_profit_lock:
                skipped += 1
                hit_lock = True
                continue
            worst_intraday = min(worst_intraday, adjusted + min(float(row.mae), float(row.pnl), 0.0))
            adjusted += float(row.pnl)
            taken += 1
            if adjusted <= -overlay.daily_stop:
                hit_stop = True
            if adjusted >= overlay.daily_profit_lock:
                hit_lock = True
        days.append(
            {
                "date": date,
                "pnl": adjusted,
                "raw_pnl": raw,
                "worst_intraday_pnl": worst_intraday,
                "trades": taken,
                "skipped_trades": skipped,
                "hit_daily_stop": hit_stop,
                "hit_daily_profit_lock": hit_lock,
            }
        )
    return pd.DataFrame(days)


def evaluate_topstep_150k(
    trades: list[dict],
    df: pd.DataFrame,
    config: Topstep150KConfig,
    overlay: InternalRiskOverlay,
    split_daily: dict[str, pd.DataFrame] | None = None,
) -> TopstepEvaluation:
    daily = trades_to_topstep_daily(trades, df, overlay)
    combine = simulate_combine(daily, config)
    funded = simulate_funded_xfa(daily, config)
    split_scores = _split_scores(split_daily or {}, config)
    pass_split_count = sum(1 for value in split_scores.values() if value >= 0.45)
    score = topstep_score(combine, funded, daily, config, split_scores)
    status, reason = topstep_status(combine, funded, daily, score, pass_split_count, config)
    return TopstepEvaluation(
        status=status,
        rejection_reason=reason,
        topstep_passed=bool(combine["passed"]),
        topstep_score=score,
        combine_days_to_pass=combine["days_to_pass"],
        combine_profit_target_hit=bool(combine["profit_target_hit"]),
        combine_mll_breached=bool(combine["mll_breached"]),
        combine_min_mll_buffer=float(combine["min_mll_buffer"]),
        combine_best_day_profit=float(combine["best_day_profit"]),
        combine_best_day_pct_of_total_profit=float(combine["best_day_pct_of_total_profit"]),
        combine_consistency_ok=bool(combine["consistency_ok"]),
        target_inflation_required=bool(combine["target_inflation_required"]),
        funded_sim_survived=bool(funded["survived"]),
        payout_eligible=bool(funded["payout_eligible"]),
        payout_days_to_eligibility=funded["payout_days_to_eligibility"],
        payout_cycles_survived=int(funded["payout_cycles_survived"]),
        gross_payout_available=float(funded["gross_payout_available"]),
        trader_net_payout=float(funded["trader_net_payout"]),
        post_payout_mll_breach=bool(funded["post_payout_mll_breach"]),
        internal_daily_stop_used=float(overlay.daily_stop),
        daily_profit_lock_used=float(overlay.daily_profit_lock),
        worst_day_loss=float(combine["worst_day_loss"]),
        max_consecutive_losing_days=int(combine["max_consecutive_losing_days"]),
        winning_days_150_count=int(funded["winning_days_150_count"]),
        days_traded=int(len(daily)),
        adjusted_net_profit=float(daily["pnl"].sum()) if len(daily) else 0.0,
        skipped_trades=int(daily["skipped_trades"].sum()) if len(daily) else 0,
        trade_count=int(daily["trades"].sum()) if len(daily) else 0,
        pass_split_count=pass_split_count,
        split_scores=split_scores,
    )


def simulate_combine(daily: pd.DataFrame, config: Topstep150KConfig) -> dict[str, Any]:
    balance = config.combine_starting_balance
    floor = config.combine_starting_mll
    min_buffer = config.combine_max_loss_limit
    mll_breached = False
    days_to_pass: int | None = None
    target_hit = False
    best_day = 0.0
    gross_profit = 0.0
    daily_pnls: list[float] = []
    required_target = config.combine_profit_target
    consistency_ok = True
    target_inflation_required = False
    for day_index, row in enumerate(daily.itertuples(index=False), start=1):
        day_pnl = float(row.pnl)
        intraday_low = balance + float(row.worst_intraday_pnl)
        if intraday_low <= floor:
            mll_breached = True
        balance += day_pnl
        if balance <= floor:
            mll_breached = True
        gross_profit = balance - config.combine_starting_balance
        best_day = max(best_day, day_pnl)
        daily_pnls.append(day_pnl)
        if best_day > config.combine_profit_target * config.consistency_best_day_max_pct_of_profit_target:
            target_inflation_required = True
            required_target = max(required_target, best_day / config.consistency_best_day_max_pct_of_profit_target)
        best_day_pct = best_day / gross_profit if gross_profit > 0 and best_day > 0 else 0.0
        consistency_ok = gross_profit <= 0 or best_day_pct <= config.consistency_best_day_max_pct_of_profit_target
        target_hit = gross_profit >= required_target
        min_buffer = min(min_buffer, balance - floor, intraday_low - floor)
        floor = min(config.combine_starting_balance, max(floor, balance - config.combine_max_loss_limit))
        if target_hit and consistency_ok and day_index >= config.minimum_pass_days and not mll_breached and days_to_pass is None:
            days_to_pass = day_index
            break
        if mll_breached:
            break
    total_profit = balance - config.combine_starting_balance
    best_day_pct = best_day / total_profit if total_profit > 0 and best_day > 0 else 0.0
    return {
        "passed": bool(days_to_pass is not None),
        "days_to_pass": days_to_pass,
        "profit_target_hit": bool(target_hit),
        "mll_breached": bool(mll_breached),
        "min_mll_buffer": float(min_buffer),
        "best_day_profit": float(best_day),
        "best_day_pct_of_total_profit": float(best_day_pct),
        "consistency_ok": bool(consistency_ok),
        "target_inflation_required": bool(target_inflation_required),
        "total_profit": float(total_profit),
        "worst_day_loss": float(min(daily_pnls) if daily_pnls else 0.0),
        "max_consecutive_losing_days": _max_consecutive_losing_days(daily_pnls),
    }


def simulate_funded_xfa(daily: pd.DataFrame, config: Topstep150KConfig) -> dict[str, Any]:
    balance = config.funded_starting_balance
    floor = config.funded_starting_mll
    winning_days = 0
    payout_eligible = False
    payout_days_to_eligibility: int | None = None
    payout_cycles = 0
    gross_payout = 0.0
    trader_net = 0.0
    survived = True
    post_payout_breach = False
    after_payout = False
    best_day = 0.0
    total_profit = 0.0
    for day_index, row in enumerate(daily.itertuples(index=False), start=1):
        pnl = float(row.pnl)
        intraday_low = balance + float(row.worst_intraday_pnl)
        if intraday_low <= floor:
            survived = False
            post_payout_breach = bool(after_payout)
            break
        balance += pnl
        total_profit += pnl
        best_day = max(best_day, pnl)
        if balance <= floor:
            survived = False
            post_payout_breach = bool(after_payout)
            break
        floor = min(0.0, max(floor, balance - abs(config.funded_starting_mll)))
        if pnl >= config.payout_winning_day_min_profit:
            winning_days += 1
        consistency_ok = True
        if config.funded_consistency_enabled and total_profit > 0 and best_day > 0:
            consistency_ok = (best_day / total_profit) <= config.funded_consistency_largest_day_max_pct_of_total_profit
        if winning_days >= config.payout_eligibility_winning_days and consistency_ok and balance > 0:
            payout_eligible = True
            if payout_days_to_eligibility is None:
                payout_days_to_eligibility = day_index
            payout = min(balance * config.payout_max_pct_of_balance, config.payout_cap)
            if payout > 0:
                gross_payout += payout
                trader_net += payout * config.profit_split_trader
                balance -= payout
                floor = 0.0
                after_payout = True
                payout_cycles += 1
                winning_days = 0
                best_day = 0.0
                total_profit = balance
    return {
        "survived": bool(survived),
        "payout_eligible": bool(payout_eligible),
        "payout_days_to_eligibility": payout_days_to_eligibility,
        "payout_cycles_survived": int(payout_cycles),
        "gross_payout_available": float(gross_payout),
        "trader_net_payout": float(trader_net),
        "post_payout_mll_breach": bool(post_payout_breach),
        "winning_days_150_count": int((daily["pnl"] >= config.payout_winning_day_min_profit).sum()) if len(daily) else 0,
    }


def topstep_status(
    combine: dict[str, Any],
    funded: dict[str, Any],
    daily: pd.DataFrame,
    score: float,
    pass_split_count: int,
    config: Topstep150KConfig,
) -> tuple[str, str | None]:
    if combine["mll_breached"]:
        return "TOPSTEP_COMBINE_FAILED_MLL", "combine_mll_breached"
    if not combine["profit_target_hit"]:
        return "TOPSTEP_COMBINE_FAILED_TARGET", "combine_profit_target_not_reached"
    if not combine["consistency_ok"]:
        return "TOPSTEP_COMBINE_FAILED_CONSISTENCY", "combine_best_day_concentration_too_high"
    if combine["min_mll_buffer"] < 750:
        return "TOPSTEP_REJECTED_LOW_MLL_BUFFER", "combine_mll_buffer_below_internal_floor"
    if combine["best_day_pct_of_total_profit"] > 0.50:
        return "TOPSTEP_REJECTED_SPIKE_DAY_DEPENDENCY", "best_day_dependency_above_limit"
    if not funded["survived"]:
        return "TOPSTEP_FUNDED_FAILED_MLL", "funded_mll_breached"
    if not funded["payout_eligible"]:
        return "TOPSTEP_REJECTED_BAD_PAYOUT_PROFILE", "funded_payout_not_eligible"
    if pass_split_count < 2:
        return "TOPSTEP_RESEARCH_CANDIDATE", "insufficient_month_to_month_stability"
    if funded["payout_cycles_survived"] > 0 and score >= 0.72 and pass_split_count == 3:
        return "TOPSTEP_PORTFOLIO_CANDIDATE", None
    if funded["payout_cycles_survived"] > 0:
        return "TOPSTEP_PAYOUT_SURVIVED", None
    if funded["payout_eligible"]:
        return "TOPSTEP_PAYOUT_ELIGIBLE", None
    return "TOPSTEP_COMBINE_PASSED", None


def topstep_score(
    combine: dict[str, Any],
    funded: dict[str, Any],
    daily: pd.DataFrame,
    config: Topstep150KConfig,
    split_scores: dict[str, float] | None = None,
) -> float:
    total_profit = float(combine["total_profit"])
    target_score = _clip01(total_profit / config.combine_profit_target)
    pass_bonus = 1.0 if combine["passed"] else 0.0
    days_score = _clip01(1.0 - ((combine["days_to_pass"] or max(len(daily), 1)) / 60.0))
    buffer_score = _clip01(combine["min_mll_buffer"] / config.combine_max_loss_limit)
    worst_day_score = _clip01(1.0 - abs(min(combine["worst_day_loss"], 0.0)) / config.combine_max_loss_limit)
    concentration_score = _clip01(1.0 - max(combine["best_day_pct_of_total_profit"] - 0.25, 0.0) / 0.35)
    winning_day_score = _clip01(funded["winning_days_150_count"] / max(config.payout_eligibility_winning_days, 1))
    payout_score = _clip01(funded["trader_net_payout"] / 4500.0)
    survival_score = 1.0 if funded["survived"] else 0.0
    streak_score = _clip01(1.0 - combine["max_consecutive_losing_days"] / max(config.max_consecutive_losing_days_limit + 1, 1))
    trade_count = float(daily["trades"].sum()) if len(daily) else 0.0
    trade_days = float((daily["trades"] > 0).sum()) if len(daily) else 0.0
    activity_score = _clip01(trade_days / 20.0)
    overtrade_penalty = _clip01(trade_count / 600.0) * 0.08
    undertrade_penalty = _clip01((8.0 - trade_days) / 8.0) * 0.18
    split_score = float(np.mean(list((split_scores or {}).values()))) if split_scores else 0.0
    score = (
        0.12 * pass_bonus
        + 0.14 * target_score
        + 0.08 * days_score
        + 0.16 * buffer_score
        + 0.10 * worst_day_score
        + 0.10 * concentration_score
        + 0.10 * winning_day_score
        + 0.10 * payout_score
        + 0.06 * survival_score
        + 0.06 * streak_score
        + 0.06 * activity_score
        + 0.08 * split_score
        - overtrade_penalty
        - undertrade_penalty
    )
    if total_profit <= 0:
        score *= 0.45
    if combine["mll_breached"] or funded["post_payout_mll_breach"]:
        score -= 0.25
    if not combine["consistency_ok"]:
        score -= 0.15
    return round(_clip01(score), 6)


def _split_scores(split_daily: dict[str, pd.DataFrame], config: Topstep150KConfig) -> dict[str, float]:
    scores: dict[str, float] = {}
    for name, daily in split_daily.items():
        combine = simulate_combine(daily, config)
        funded = simulate_funded_xfa(daily, config)
        scores[name] = topstep_score(combine, funded, daily, config, split_scores={})
    return scores


def _max_consecutive_losing_days(pnls: list[float]) -> int:
    longest = 0
    current = 0
    for pnl in pnls:
        if pnl < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _clip01(value: float) -> float:
    if np.isnan(value):
        return 0.0
    return float(max(0.0, min(1.0, value)))
