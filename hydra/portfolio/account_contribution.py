"""Pure shared-account replay and marginal contribution metrics.

Inputs are explicit trade ledgers.  No registry status is read and no status is
written.  The replay treats each trade's adverse excursion as immediately
available after entry, which is deliberately conservative for shared MLL.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.portfolio.strategy_role import StrategyPool
from hydra.propfirm.topstep_150k import (
    Topstep150KConfig,
    simulate_combine,
    simulate_funded_xfa,
)


ACCOUNT_CONTRIBUTION_VERSION = "account_contribution_trade_ledger_v1"


class AccountContributionError(ValueError):
    pass


@dataclass(frozen=True)
class AccountReplayConfig:
    starting_balance: float = 150_000.0
    mll_distance: float = 4_500.0
    profit_target: float = 9_000.0
    consistency_limit: float = 0.50
    maximum_simultaneous_contracts: int = 15


@dataclass(frozen=True)
class AccountPathMetrics:
    total_net_pnl: float
    total_cost: float
    trading_days: int
    trade_count: int
    losing_days: int
    shared_loss_days: int
    shared_loss_day_fraction: float
    maximum_drawdown: float
    worst_day_loss: float
    tail_loss_p90: float
    min_mll_buffer: float
    mll_breached: bool
    maximum_simultaneous_contracts: int
    contract_limit_breached: bool
    target_reached: bool
    target_before_mll: bool
    time_to_target_days: int | None
    target_progress: float
    target_velocity_dollars_per_day: float
    target_velocity_fraction_per_day: float
    best_day_concentration: float
    consistency_margin: float
    consistency_ok: bool
    execution_cost_burden: float
    payout_cycles_before_ruin: int
    qualifying_day_frequency: float
    xfa_survived: bool
    post_payout_survival: bool
    payout_time_days: int | None
    combine_utility: float
    xfa_utility: float
    defensive_utility: float
    policy_version: str = ACCOUNT_CONTRIBUTION_VERSION

    def utility_for(self, pool: StrategyPool | str) -> float:
        normalized = pool if isinstance(pool, StrategyPool) else StrategyPool(pool)
        if normalized is StrategyPool.COMBINE_PASSER_POOL:
            return self.combine_utility
        if normalized is StrategyPool.XFA_PAYOUT_POOL:
            return self.xfa_utility
        return self.defensive_utility

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AccountContribution:
    candidate_id: str
    target_pool: StrategyPool
    base: AccountPathMetrics
    combined: AccountPathMetrics
    pool_utility_delta: float
    combine_utility_delta: float
    xfa_utility_delta: float
    defensive_utility_delta: float
    net_pnl_delta: float
    min_mll_buffer_delta: float
    maximum_drawdown_reduction: float
    shared_loss_days_reduction: int
    target_velocity_delta: float
    consistency_margin_delta: float
    hard_risk_violation: bool
    candidate_past_only_verified: bool
    evidence_status: str = "ACCOUNT_CONTRIBUTION_MEASURED"
    inherited_status: bool = False
    promotion_eligible: bool = False
    policy_version: str = ACCOUNT_CONTRIBUTION_VERSION

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["target_pool"] = self.target_pool.value
        return row


@dataclass(frozen=True)
class MatchedInclusionControlSummary:
    observed_pool_utility_delta: float
    control_deltas: tuple[float, ...]
    one_sided_p_value: float
    control_count: int
    matching: str
    seed: int
    operational_policy: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CombinePasserPoolUtility:
    path_count: int
    target_before_mll_probability: float
    median_time_to_target_days: float | None
    median_consistency_margin: float
    mean_execution_cost_burden: float
    median_tail_loss_p90: float
    utility: float
    evidence_status: str = "RESEARCH_PATH_AGGREGATE_ONLY"
    promotion_eligible: bool = False


@dataclass(frozen=True)
class XfaPayoutPoolUtility:
    path_count: int
    expected_payout_cycles_before_ruin: float
    mean_qualifying_day_frequency: float
    mll_survival_probability: float
    post_payout_survival_probability: float
    median_payout_time_days: float | None
    utility: float
    evidence_status: str = "RESEARCH_PATH_AGGREGATE_ONLY"
    promotion_eligible: bool = False


@dataclass(frozen=True)
class DefensiveAccountPoolUtility:
    path_count: int
    mean_marginal_account_utility: float
    mean_drawdown_reduction: float
    mean_shared_loss_days_reduction: float
    mean_mll_buffer_delta: float
    utility: float
    evidence_status: str = "RESEARCH_PATH_AGGREGATE_ONLY"
    promotion_eligible: bool = False


_REQUIRED = {
    "trade_id",
    "event_session_id",
    "entry_timestamp",
    "exit_timestamp",
    "net_pnl",
    "mae_dollars",
}


def normalize_trade_ledger(
    ledger: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    strategy_id: str | None = None,
) -> pd.DataFrame:
    """Validate and deterministically normalize a trade ledger."""

    frame = ledger.copy(deep=True) if isinstance(ledger, pd.DataFrame) else pd.DataFrame(list(ledger))
    missing = sorted(_REQUIRED - set(frame.columns))
    if missing:
        raise AccountContributionError(f"Trade ledger missing columns: {missing}")
    if "strategy_id" not in frame:
        if not strategy_id:
            raise AccountContributionError("strategy_id is required in the ledger or argument")
        frame["strategy_id"] = str(strategy_id)
    elif strategy_id and not frame["strategy_id"].astype(str).eq(str(strategy_id)).all():
        raise AccountContributionError("Ledger contains a different strategy_id")
    frame["strategy_id"] = frame["strategy_id"].astype(str)
    frame["trade_id"] = frame["trade_id"].astype(str)
    frame["event_session_id"] = frame["event_session_id"].astype(str)
    if frame[["strategy_id", "trade_id"]].duplicated().any():
        raise AccountContributionError("Duplicate strategy_id/trade_id")
    for column in ("entry_timestamp", "exit_timestamp"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    if (frame["exit_timestamp"] < frame["entry_timestamp"]).any():
        raise AccountContributionError("Trade exits before entry")
    for column in ("availability_timestamp", "source_bar_close"):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
            if (frame[column] > frame["entry_timestamp"]).any():
                raise AccountContributionError(f"Future information in {column}")
    for column in ("net_pnl", "mae_dollars"):
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype(float)
        if not np.isfinite(frame[column]).all():
            raise AccountContributionError(f"Non-finite {column}")
    if (frame["mae_dollars"] > 1e-12).any():
        raise AccountContributionError("mae_dollars must be zero or adverse (negative)")
    if "cost" not in frame:
        frame["cost"] = 0.0
    frame["cost"] = pd.to_numeric(frame["cost"], errors="raise").astype(float)
    if (frame["cost"] < 0).any() or not np.isfinite(frame["cost"]).all():
        raise AccountContributionError("cost must be finite and non-negative")
    if "contracts" not in frame:
        frame["contracts"] = 1
    numeric_contracts = pd.to_numeric(frame["contracts"], errors="raise")
    if (numeric_contracts <= 0).any() or not np.equal(numeric_contracts, np.floor(numeric_contracts)).all():
        raise AccountContributionError("contracts must be positive integers")
    frame["contracts"] = numeric_contracts.astype(int)
    if "underlying" not in frame:
        frame["underlying"] = frame.get("symbol", pd.Series("UNKNOWN", index=frame.index))
    if "side" not in frame:
        frame["side"] = 0.0
    frame["side"] = pd.to_numeric(frame["side"], errors="raise").astype(float)
    return frame.sort_values(
        ["entry_timestamp", "exit_timestamp", "strategy_id", "trade_id"],
        kind="mergesort",
    ).reset_index(drop=True)


def ledger_past_only_verified(ledger: pd.DataFrame) -> bool:
    required = {"availability_timestamp", "source_bar_close", "entry_timestamp"}
    if not required.issubset(ledger.columns) or ledger.empty:
        return False
    return bool(
        ledger["availability_timestamp"].notna().all()
        and ledger["source_bar_close"].notna().all()
        and (ledger["availability_timestamp"] <= ledger["entry_timestamp"]).all()
        and (ledger["source_bar_close"] <= ledger["entry_timestamp"]).all()
    )


def replay_shared_account(
    ledgers: Mapping[str, pd.DataFrame | Iterable[Mapping[str, Any]]],
    *,
    config: AccountReplayConfig | None = None,
) -> AccountPathMetrics:
    cfg = config or AccountReplayConfig()
    normalized = {
        str(strategy_id): normalize_trade_ledger(ledger, strategy_id=str(strategy_id))
        for strategy_id, ledger in sorted(ledgers.items())
    }
    return _replay_normalized_account(normalized, cfg)


def _replay_normalized_account(
    normalized: Mapping[str, pd.DataFrame], cfg: AccountReplayConfig
) -> AccountPathMetrics:
    if not normalized:
        return _empty_metrics(cfg)
    events = pd.concat(normalized.values(), ignore_index=True)
    daily = build_shared_daily_path(events)
    combine_config = Topstep150KConfig(
        combine_starting_balance=cfg.starting_balance,
        combine_max_loss_limit=cfg.mll_distance,
        combine_profit_target=cfg.profit_target,
        consistency_best_day_max_pct_of_profit_target=cfg.consistency_limit,
    )
    combine = simulate_combine(daily, combine_config)
    xfa = simulate_funded_xfa(daily, combine_config)
    pnl = daily["pnl"].astype(float)
    cumulative = pnl.cumsum()
    drawdown = cumulative - cumulative.cummax().clip(lower=0.0)
    total = float(pnl.sum())
    best_day = float(max(0.0, pnl.max())) if len(pnl) else 0.0
    concentration = best_day / total if total > 0.0 else 1.0 if best_day else 0.0
    consistency_margin = float(cfg.consistency_limit - concentration)
    target_progress = total / cfg.profit_target
    days = int(len(daily))
    total_cost = float(events["cost"].sum())
    gross_abs = abs(total + total_cost)
    cost_burden = total_cost / max(gross_abs, total_cost, 1.0)
    tail_quantile = float(pnl.quantile(0.10)) if len(pnl) else 0.0
    tail_loss = abs(min(0.0, tail_quantile))
    simultaneous = maximum_simultaneous_contracts(events)
    target_before_mll = bool(combine["passed"] and not combine["mll_breached"])
    qualifying_frequency = float((pnl >= combine_config.payout_winning_day_min_profit).mean()) if days else 0.0
    post_payout_survival = bool(xfa["survived"] and not xfa["post_payout_mll_breach"])
    common = {
        "target_progress": target_progress,
        "min_mll_buffer": float(combine["min_mll_buffer"]),
        "mll_breached": bool(combine["mll_breached"]),
        "days": days,
        "concentration": concentration,
        "cost_burden": cost_burden,
        "tail_loss": tail_loss,
        "shared_loss_fraction": float(daily["shared_loss_day"].mean()) if days else 0.0,
        "max_drawdown": abs(float(drawdown.min())) if days else 0.0,
    }
    return AccountPathMetrics(
        total_net_pnl=total,
        total_cost=total_cost,
        trading_days=days,
        trade_count=int(len(events)),
        losing_days=int((pnl < 0).sum()),
        shared_loss_days=int(daily["shared_loss_day"].sum()),
        shared_loss_day_fraction=common["shared_loss_fraction"],
        maximum_drawdown=common["max_drawdown"],
        worst_day_loss=float(min(0.0, pnl.min())) if days else 0.0,
        tail_loss_p90=tail_loss,
        min_mll_buffer=common["min_mll_buffer"],
        mll_breached=common["mll_breached"],
        maximum_simultaneous_contracts=simultaneous,
        contract_limit_breached=simultaneous > cfg.maximum_simultaneous_contracts,
        target_reached=bool(combine["profit_target_hit"]),
        target_before_mll=target_before_mll,
        time_to_target_days=combine["days_to_pass"],
        target_progress=target_progress,
        target_velocity_dollars_per_day=total / max(days, 1),
        target_velocity_fraction_per_day=target_progress / max(days, 1),
        best_day_concentration=concentration,
        consistency_margin=consistency_margin,
        consistency_ok=concentration <= cfg.consistency_limit if total > 0 else False,
        execution_cost_burden=cost_burden,
        payout_cycles_before_ruin=int(xfa["payout_cycles_survived"]),
        qualifying_day_frequency=qualifying_frequency,
        xfa_survived=bool(xfa["survived"]),
        post_payout_survival=post_payout_survival,
        payout_time_days=xfa["payout_days_to_eligibility"],
        combine_utility=_combine_utility(common, cfg, combine),
        xfa_utility=_xfa_utility(xfa, qualifying_frequency, days),
        defensive_utility=_defensive_utility(common, cfg),
    )


def compare_account_contribution(
    base_ledgers: Mapping[str, pd.DataFrame | Iterable[Mapping[str, Any]]],
    candidate_id: str,
    candidate_ledger: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    target_pool: StrategyPool | str,
    config: AccountReplayConfig | None = None,
) -> AccountContribution:
    cfg = config or AccountReplayConfig()
    pool = target_pool if isinstance(target_pool, StrategyPool) else StrategyPool(target_pool)
    normalized_base = {
        str(key): normalize_trade_ledger(value, strategy_id=str(key))
        for key, value in sorted(base_ledgers.items())
    }
    normalized_candidate = normalize_trade_ledger(candidate_ledger, strategy_id=candidate_id)
    if candidate_id in normalized_base:
        raise AccountContributionError(f"Candidate already in base account: {candidate_id}")
    base = _replay_normalized_account(normalized_base, cfg)
    combined_ledgers = {**normalized_base, candidate_id: normalized_candidate}
    combined = _replay_normalized_account(combined_ledgers, cfg)
    return _build_contribution(
        candidate_id,
        pool,
        normalized_candidate,
        base,
        combined,
    )


def _build_contribution(
    candidate_id: str,
    pool: StrategyPool,
    normalized_candidate: pd.DataFrame,
    base: AccountPathMetrics,
    combined: AccountPathMetrics,
) -> AccountContribution:
    return AccountContribution(
        candidate_id=candidate_id,
        target_pool=pool,
        base=base,
        combined=combined,
        pool_utility_delta=combined.utility_for(pool) - base.utility_for(pool),
        combine_utility_delta=combined.combine_utility - base.combine_utility,
        xfa_utility_delta=combined.xfa_utility - base.xfa_utility,
        defensive_utility_delta=combined.defensive_utility - base.defensive_utility,
        net_pnl_delta=combined.total_net_pnl - base.total_net_pnl,
        min_mll_buffer_delta=combined.min_mll_buffer - base.min_mll_buffer,
        maximum_drawdown_reduction=base.maximum_drawdown - combined.maximum_drawdown,
        shared_loss_days_reduction=base.shared_loss_days - combined.shared_loss_days,
        target_velocity_delta=(
            combined.target_velocity_dollars_per_day - base.target_velocity_dollars_per_day
        ),
        consistency_margin_delta=combined.consistency_margin - base.consistency_margin,
        hard_risk_violation=bool(
            (not base.mll_breached and combined.mll_breached)
            or (not base.contract_limit_breached and combined.contract_limit_breached)
        ),
        candidate_past_only_verified=ledger_past_only_verified(normalized_candidate),
    )


def matched_random_inclusion_controls(
    base_ledgers: Mapping[str, pd.DataFrame | Iterable[Mapping[str, Any]]],
    candidate_id: str,
    candidate_ledger: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    target_pool: StrategyPool | str,
    control_count: int = 255,
    seed: int = 0,
    config: AccountReplayConfig | None = None,
) -> MatchedInclusionControlSummary:
    if control_count < 1:
        raise AccountContributionError("control_count must be positive")
    normalized_base = {
        str(key): normalize_trade_ledger(value, strategy_id=str(key))
        for key, value in sorted(base_ledgers.items())
    }
    candidate = normalize_trade_ledger(candidate_ledger, strategy_id=candidate_id)
    pool = target_pool if isinstance(target_pool, StrategyPool) else StrategyPool(target_pool)
    cfg = config or AccountReplayConfig()
    if candidate_id in normalized_base:
        raise AccountContributionError(f"Candidate already in base account: {candidate_id}")
    base_metrics = _replay_normalized_account(normalized_base, cfg)
    observed_metrics = _replay_normalized_account(
        {**normalized_base, candidate_id: candidate}, cfg
    )
    observed = _build_contribution(
        candidate_id,
        pool,
        candidate,
        base_metrics,
        observed_metrics,
    )
    universe = pd.concat([*normalized_base.values(), candidate], ignore_index=True)
    sessions = (
        universe.groupby("event_session_id", sort=False)["entry_timestamp"]
        .min()
        .sort_values()
    )
    rng = np.random.default_rng(seed)
    possible_offsets = np.arange(1, max(len(sessions), 2), dtype=int)
    sampled_offsets = (
        rng.choice(possible_offsets, size=control_count).astype(int)
        if len(sessions) > 1
        else np.zeros(control_count, dtype=int)
    )
    # A circular offset fully determines the null ledger.  Cache each unique
    # offset instead of replaying duplicate draws; the requested Monte Carlo
    # sample and p-value remain unchanged.
    delta_by_offset: dict[int, float] = {}
    for offset in sorted({int(value) for value in sampled_offsets}):
        shifted = _circular_shift_sessions(
            candidate, sessions, offset, suffix=f"null_offset_{offset}"
        )
        control_metrics = _replay_normalized_account(
            {**normalized_base, candidate_id: shifted}, cfg
        )
        control = _build_contribution(
            candidate_id,
            pool,
            shifted,
            base_metrics,
            control_metrics,
        )
        delta_by_offset[offset] = float(control.pool_utility_delta)
    deltas = [delta_by_offset[int(offset)] for offset in sampled_offsets]
    exceed = sum(value >= observed.pool_utility_delta - 1e-15 for value in deltas)
    return MatchedInclusionControlSummary(
        observed_pool_utility_delta=float(observed.pool_utility_delta),
        control_deltas=tuple(deltas),
        one_sided_p_value=float((1 + exceed) / (1 + control_count)),
        control_count=control_count,
        matching="circular_session_assignment_preserving_trade_blocks_and_count",
        seed=seed,
    )


def aggregate_combine_passer_utility(
    paths: Sequence[AccountPathMetrics],
) -> CombinePasserPoolUtility:
    """Aggregate independent folds/resamples for the Combine phase only."""

    _require_paths(paths)
    successful_times = [
        float(path.time_to_target_days)
        for path in paths
        if path.target_before_mll and path.time_to_target_days is not None
    ]
    success_probability = float(np.mean([path.target_before_mll for path in paths]))
    median_time = float(np.median(successful_times)) if successful_times else None
    consistency = float(np.median([path.consistency_margin for path in paths]))
    cost = float(np.mean([path.execution_cost_burden for path in paths]))
    tail = float(np.median([path.tail_loss_p90 for path in paths]))
    time_score = (
        0.0
        if median_time is None
        else float(np.clip(1.0 - median_time / 60.0, 0.0, 1.0))
    )
    utility = (
        0.40 * success_probability
        + 0.20 * time_score
        + 0.15 * float(np.clip(consistency / 0.50, -1.0, 1.0))
        + 0.10 * float(np.clip(1.0 - cost, 0.0, 1.0))
        + 0.15 * float(np.clip(1.0 - tail / 4_500.0, -1.0, 1.0))
    )
    return CombinePasserPoolUtility(
        path_count=len(paths),
        target_before_mll_probability=success_probability,
        median_time_to_target_days=median_time,
        median_consistency_margin=consistency,
        mean_execution_cost_burden=cost,
        median_tail_loss_p90=tail,
        utility=float(utility),
    )


def aggregate_xfa_payout_utility(
    paths: Sequence[AccountPathMetrics],
) -> XfaPayoutPoolUtility:
    """Estimate XFA utility across supplied independent folds/resamples."""

    _require_paths(paths)
    payout_times = [
        float(path.payout_time_days)
        for path in paths
        if path.payout_time_days is not None
    ]
    cycles = float(np.mean([path.payout_cycles_before_ruin for path in paths]))
    qualifying = float(np.mean([path.qualifying_day_frequency for path in paths]))
    survival = float(np.mean([path.xfa_survived for path in paths]))
    post_survival = float(np.mean([path.post_payout_survival for path in paths]))
    timing = float(np.median(payout_times)) if payout_times else None
    timing_score = (
        0.0
        if timing is None
        else float(np.clip(1.0 - timing / 60.0, 0.0, 1.0))
    )
    utility = (
        0.35 * min(cycles / 3.0, 1.0)
        + 0.20 * min(qualifying / 0.50, 1.0)
        + 0.20 * survival
        + 0.15 * post_survival
        + 0.10 * timing_score
    )
    return XfaPayoutPoolUtility(
        path_count=len(paths),
        expected_payout_cycles_before_ruin=cycles,
        mean_qualifying_day_frequency=qualifying,
        mll_survival_probability=survival,
        post_payout_survival_probability=post_survival,
        median_payout_time_days=timing,
        utility=float(utility),
    )


def aggregate_defensive_account_utility(
    contributions: Sequence[AccountContribution],
) -> DefensiveAccountPoolUtility:
    """Aggregate marginal defensive utility without requiring alpha economics."""

    if not contributions:
        raise AccountContributionError(
            "At least one independent contribution path is required"
        )
    if any(
        contribution.target_pool is not StrategyPool.DEFENSIVE_ACCOUNT_POOL
        for contribution in contributions
    ):
        raise AccountContributionError(
            "Defensive aggregate only accepts DEFENSIVE_ACCOUNT_POOL paths"
        )
    marginal = float(
        np.mean([item.defensive_utility_delta for item in contributions])
    )
    drawdown = float(
        np.mean([item.maximum_drawdown_reduction for item in contributions])
    )
    shared_losses = float(
        np.mean([item.shared_loss_days_reduction for item in contributions])
    )
    buffer_delta = float(
        np.mean([item.min_mll_buffer_delta for item in contributions])
    )
    utility = (
        0.40 * marginal
        + 0.25 * float(np.tanh(drawdown / 4_500.0))
        + 0.20 * float(np.tanh(shared_losses / 3.0))
        + 0.15 * float(np.tanh(buffer_delta / 4_500.0))
    )
    return DefensiveAccountPoolUtility(
        path_count=len(contributions),
        mean_marginal_account_utility=marginal,
        mean_drawdown_reduction=drawdown,
        mean_shared_loss_days_reduction=shared_losses,
        mean_mll_buffer_delta=buffer_delta,
        utility=float(utility),
    )


def build_shared_daily_path(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(
            columns=(
                "date", "pnl", "raw_pnl", "worst_intraday_pnl", "trades",
                "skipped_trades", "hit_daily_stop", "hit_daily_profit_lock",
                "losing_strategies", "shared_loss_day",
            )
        )
    rows: list[dict[str, Any]] = []
    grouped = {
        str(session_id): day
        for session_id, day in events.groupby("event_session_id", sort=False)
    }
    ordering = events.groupby("event_session_id")["entry_timestamp"].min().sort_values()
    for raw_session_id in ordering.index:
        session_id = str(raw_session_id)
        # Reusing the pre-grouped frame avoids an O(sessions × trades)
        # boolean scan inside every matched-control replay.
        day = grouped[session_id]
        actions: list[tuple[pd.Timestamp, int, str, float, float, int]] = []
        for trade in day.itertuples(index=False):
            key = f"{trade.strategy_id}:{trade.trade_id}"
            actions.append((trade.entry_timestamp, 0, key, min(float(trade.mae_dollars), 0.0), 0.0, int(trade.contracts)))
            actions.append((trade.exit_timestamp, 1, key, 0.0, float(trade.net_pnl), -int(trade.contracts)))
        actions.sort(key=lambda row: (row[0], row[1], row[2]))
        realized = 0.0
        open_mae: dict[str, float] = {}
        worst = 0.0
        for _timestamp, kind, key, mae, pnl_value, _contracts in actions:
            if kind == 0:
                open_mae[key] = mae
            else:
                open_mae.pop(key, None)
                realized += pnl_value
            worst = min(worst, realized + sum(open_mae.values()))
        by_strategy = day.groupby("strategy_id")["net_pnl"].sum()
        losing_strategies = int((by_strategy < 0).sum())
        pnl_value = float(day["net_pnl"].sum())
        rows.append(
            {
                "date": str(session_id),
                "pnl": pnl_value,
                "raw_pnl": pnl_value,
                "worst_intraday_pnl": float(worst),
                "trades": int(len(day)),
                "skipped_trades": 0,
                "hit_daily_stop": False,
                "hit_daily_profit_lock": False,
                "losing_strategies": losing_strategies,
                "shared_loss_day": losing_strategies >= 2,
            }
        )
    return pd.DataFrame(rows)


def maximum_simultaneous_contracts(events: pd.DataFrame) -> int:
    actions: list[tuple[pd.Timestamp, int, str, int]] = []
    for row in events.itertuples(index=False):
        key = f"{row.strategy_id}:{row.trade_id}"
        actions.append((row.entry_timestamp, 0, key, int(row.contracts)))
        actions.append((row.exit_timestamp, 1, key, -int(row.contracts)))
    actions.sort(key=lambda row: (row[0], row[1], row[2]))
    active = maximum = 0
    for _timestamp, _kind, _key, delta in actions:
        active += delta
        maximum = max(maximum, active)
    return maximum


def _require_paths(paths: Sequence[AccountPathMetrics]) -> None:
    if not paths:
        raise AccountContributionError(
            "At least one independent fold or resampled path is required"
        )


def _combine_utility(common: Mapping[str, Any], cfg: AccountReplayConfig, combine: Mapping[str, Any]) -> float:
    progress = float(np.clip(common["target_progress"], -1.0, 1.0))
    buffer_score = float(np.clip(common["min_mll_buffer"] / cfg.mll_distance, -1.0, 1.0))
    consistency = float(np.clip((cfg.consistency_limit - common["concentration"]) / cfg.consistency_limit, -1.0, 1.0))
    cost_score = float(np.clip(1.0 - common["cost_burden"], 0.0, 1.0))
    tail_score = float(np.clip(1.0 - common["tail_loss"] / cfg.mll_distance, -1.0, 1.0))
    time_score = float(np.clip(common["target_progress"] / max(common["days"] / 20.0, 1.0), -1.0, 1.0))
    score = 0.30 * progress + 0.20 * buffer_score + 0.15 * time_score + 0.15 * consistency + 0.10 * cost_score + 0.10 * tail_score
    if common["mll_breached"]:
        score -= 1.0
    if combine["passed"] and not combine["mll_breached"]:
        score += 0.20
    return float(score)


def _xfa_utility(xfa: Mapping[str, Any], qualifying_frequency: float, days: int) -> float:
    cycles = min(float(xfa["payout_cycles_survived"]) / 3.0, 1.0)
    timing = xfa["payout_days_to_eligibility"]
    timing_score = 0.0 if timing is None else float(np.clip(1.0 - (float(timing) - 5.0) / max(days, 5), 0.0, 1.0))
    survival = float(bool(xfa["survived"]))
    post_survival = float(bool(xfa["survived"] and not xfa["post_payout_mll_breach"]))
    score = 0.35 * cycles + 0.20 * min(qualifying_frequency / 0.50, 1.0) + 0.20 * survival + 0.15 * post_survival + 0.10 * timing_score
    if not xfa["survived"]:
        score -= 0.75
    return float(score)


def _defensive_utility(common: Mapping[str, Any], cfg: AccountReplayConfig) -> float:
    buffer_score = float(np.clip(common["min_mll_buffer"] / cfg.mll_distance, -1.0, 1.0))
    drawdown_score = float(np.clip(1.0 - common["max_drawdown"] / cfg.mll_distance, -1.0, 1.0))
    loss_score = 1.0 - float(common["shared_loss_fraction"])
    tail_score = float(np.clip(1.0 - common["tail_loss"] / cfg.mll_distance, -1.0, 1.0))
    consistency = float(np.clip((cfg.consistency_limit - common["concentration"]) / cfg.consistency_limit, -1.0, 1.0))
    score = 0.35 * buffer_score + 0.25 * drawdown_score + 0.20 * loss_score + 0.10 * tail_score + 0.10 * consistency
    if common["mll_breached"]:
        score -= 1.0
    return float(score)


def _empty_metrics(cfg: AccountReplayConfig) -> AccountPathMetrics:
    return AccountPathMetrics(
        total_net_pnl=0.0, total_cost=0.0, trading_days=0, trade_count=0,
        losing_days=0, shared_loss_days=0, shared_loss_day_fraction=0.0,
        maximum_drawdown=0.0, worst_day_loss=0.0, tail_loss_p90=0.0,
        min_mll_buffer=cfg.mll_distance, mll_breached=False,
        maximum_simultaneous_contracts=0, contract_limit_breached=False,
        target_reached=False, target_before_mll=False, time_to_target_days=None,
        target_progress=0.0, target_velocity_dollars_per_day=0.0,
        target_velocity_fraction_per_day=0.0, best_day_concentration=0.0,
        consistency_margin=cfg.consistency_limit, consistency_ok=False,
        execution_cost_burden=0.0, payout_cycles_before_ruin=0,
        qualifying_day_frequency=0.0, xfa_survived=True,
        post_payout_survival=True, payout_time_days=None,
        combine_utility=0.0, xfa_utility=0.35, defensive_utility=1.0,
    )


def _circular_shift_sessions(
    candidate: pd.DataFrame,
    session_anchors: pd.Series,
    offset: int,
    *,
    suffix: str,
) -> pd.DataFrame:
    shifted = candidate.copy(deep=True)
    sessions = list(session_anchors.index)
    anchors = {str(key): pd.Timestamp(value) for key, value in session_anchors.items()}
    positions = {str(key): index for index, key in enumerate(sessions)}
    for session_id, indices in shifted.groupby("event_session_id", sort=False).groups.items():
        source_id = str(session_id)
        source_anchor = anchors.get(source_id, shifted.loc[indices, "entry_timestamp"].min())
        source_position = positions.get(source_id, 0)
        target_id = str(sessions[(source_position + offset) % len(sessions)])
        delta = anchors[target_id] - source_anchor
        shifted.loc[indices, "event_session_id"] = target_id
        for column in ("entry_timestamp", "exit_timestamp", "availability_timestamp", "source_bar_close"):
            if column in shifted:
                shifted.loc[indices, column] = shifted.loc[indices, column] + delta
        shifted.loc[indices, "trade_id"] = shifted.loc[indices, "trade_id"].astype(str) + f":{suffix}"
    return normalize_trade_ledger(shifted)
