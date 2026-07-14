from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import (
    AccountPolicyEpisode,
    RoutedTrade,
    run_shared_account_episode,
)
from hydra.account_policy.schema import BasketPolicy, ControllerPolicy
from hydra.propfirm.censored_combine import (
    CombineObservationStatus,
    classify_observation_status,
)
from hydra.propfirm.topstep_150k import Topstep150KConfig


FROZEN_TRADING_DAY_HORIZONS: tuple[int, ...] = (20, 40, 60, 90)
FULL_AVAILABLE_HORIZON = "full_available"

# The account-policy layer uses the same five scientific observation states as
# the single-component Combine audit.  This alias makes that shared contract
# explicit without creating a second enum that could drift from it.
AccountObservationStatus = CombineObservationStatus


@dataclass(frozen=True, slots=True)
class CensoredAccountPolicyEpisode:
    """One shared-account replay with its scientific observation state.

    ``legacy_episode`` retains the simulator's historical ``TIMEOUT`` terminal
    for reproducibility.  ``observation_status`` is the inference-safe state:
    an internal horizon or the end of available data is censoring, not failure.
    """

    observation_status: AccountObservationStatus
    requested_horizon_days: int | None
    available_horizon_days: int
    observed_days: int
    legacy_episode: AccountPolicyEpisode
    daily_target_progress: tuple[float, ...]

    @property
    def target_reached(self) -> bool:
        return self.observation_status is AccountObservationStatus.TARGET_REACHED

    @property
    def mll_breached(self) -> bool:
        return self.observation_status is AccountObservationStatus.MLL_BREACHED

    @property
    def hard_rule_failed(self) -> bool:
        return self.observation_status is AccountObservationStatus.HARD_RULE_FAILURE

    @property
    def censored(self) -> bool:
        return self.observation_status in {
            AccountObservationStatus.DATA_CENSORED,
            AccountObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED,
        }

    def to_dict(self, *, include_paths: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "observation_status": self.observation_status.value,
            "requested_horizon_days": self.requested_horizon_days,
            "available_horizon_days": self.available_horizon_days,
            "observed_days": self.observed_days,
            "target_reached": self.target_reached,
            "mll_breached": self.mll_breached,
            "hard_rule_failed": self.hard_rule_failed,
            "censored": self.censored,
            "legacy_episode": self.legacy_episode.to_dict(
                include_paths=include_paths
            ),
        }
        if include_paths:
            payload["daily_target_progress"] = list(self.daily_target_progress)
        return payload


@dataclass(frozen=True, slots=True)
class TimeToCombineHorizonSummary:
    """Held-out shared-account evidence at one frozen trading-day horizon."""

    block_id: str | None
    horizon_label: str
    requested_horizon_days: int | None
    episode_count: int
    pass_count: int
    pass_probability: float
    pass_probability_lower_bound: float
    pass_probability_upper_bound: float
    fully_observed_for_horizon_count: int
    pass_probability_among_fully_observed: float | None
    mll_breach_count: int
    mll_breach_probability: float
    mll_breach_probability_lower_bound: float
    mll_breach_probability_upper_bound: float
    hard_rule_failure_count: int
    data_censored_count: int
    operational_horizon_not_reached_count: int
    censored_count: int
    consistency_ok_count: int
    consistency_ok_probability: float
    target_progress_p25: float
    target_progress_median: float
    target_progress_p75: float
    maximum_target_progress_median: float
    net_after_costs_total: float
    net_after_costs_mean: float
    net_after_costs_p25: float
    net_after_costs_median: float
    net_after_costs_p75: float
    total_execution_cost: float
    expected_trading_days_to_pass_conditional: float | None
    median_trading_days_to_pass_conditional: float | None
    time_to_target_days: tuple[int, ...]
    compact_episode_outcomes: tuple[dict[str, Any], ...]
    target_progress_curve: tuple[dict[str, float | int], ...]
    episodes: tuple[CensoredAccountPolicyEpisode, ...]

    @property
    def target_reached_count(self) -> int:
        return self.pass_count

    @property
    def target_reached_probability(self) -> float:
        return self.pass_probability

    def to_dict(self, *, include_episodes: bool = False) -> dict[str, Any]:
        payload = asdict(self)
        payload["target_progress_curve"] = list(self.target_progress_curve)
        payload["time_to_target_days"] = list(self.time_to_target_days)
        payload["compact_episode_outcomes"] = [
            dict(row) for row in self.compact_episode_outcomes
        ]
        payload["target_reached_count"] = self.target_reached_count
        payload["target_reached_probability"] = self.target_reached_probability
        if include_episodes:
            payload["episodes"] = [row.to_dict() for row in self.episodes]
        else:
            payload.pop("episodes", None)
        return payload


def classify_account_observation_status(
    episode: AccountPolicyEpisode,
    *,
    requested_horizon_days: int | None,
    available_horizon_days: int,
) -> AccountObservationStatus:
    """Map a legacy account terminal into the five-state audit contract."""

    return classify_observation_status(
        episode.terminal,
        requested_horizon_days=requested_horizon_days,
        available_horizon_days=available_horizon_days,
    )


def run_censored_shared_account_episode(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_session_days: Sequence[int],
    *,
    basket: BasketPolicy,
    start_day: int,
    horizon_days: int | None,
    controller: ControllerPolicy | None = None,
    config: Topstep150KConfig | None = None,
) -> CensoredAccountPolicyEpisode:
    """Replay one policy inside the supplied block-local chronological days."""

    days = tuple(sorted({int(day) for day in eligible_session_days}))
    if not days:
        raise ValueError("eligible_session_days must be non-empty")
    normalized_start = int(start_day)
    if normalized_start not in days:
        raise ValueError("start_day must be an eligible block-local session day")
    if horizon_days is not None and int(horizon_days) <= 0:
        raise ValueError("horizon_days must be positive or None")

    available = len(days) - days.index(normalized_start)
    requested = None if horizon_days is None else int(horizon_days)
    duration = available if requested is None else min(requested, available)
    episode = run_shared_account_episode(
        component_events,
        days,
        basket=basket,
        controller=controller,
        start_day=normalized_start,
        maximum_duration_days=duration,
        config=config,
    )
    status = classify_account_observation_status(
        episode,
        requested_horizon_days=requested,
        available_horizon_days=available,
    )
    rules = config or Topstep150KConfig()
    return CensoredAccountPolicyEpisode(
        observation_status=status,
        requested_horizon_days=requested,
        available_horizon_days=available,
        observed_days=int(episode.eligible_days),
        legacy_episode=episode,
        daily_target_progress=_daily_target_progress(episode, rules),
    )


def evaluate_time_to_combine(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_session_days: Sequence[int],
    *,
    basket: BasketPolicy,
    start_days: Sequence[int],
    controller: ControllerPolicy | None = None,
    config: Topstep150KConfig | None = None,
    block_id: str | None = None,
    horizons: Sequence[int] = FROZEN_TRADING_DAY_HORIZONS,
) -> dict[str, TimeToCombineHorizonSummary]:
    """Evaluate 20/40/60/90/full horizons without treating survival as failure.

    ``eligible_session_days`` must contain only the current temporal block.  A
    caller cannot silently substitute a post-result horizon policy: the only
    accepted finite horizon tuple is ``FROZEN_TRADING_DAY_HORIZONS``.
    """

    normalized_horizons = tuple(int(value) for value in horizons)
    if normalized_horizons != FROZEN_TRADING_DAY_HORIZONS:
        raise ValueError(
            "horizons must equal the frozen 20/40/60/90 trading-day policy"
        )
    starts = tuple(int(day) for day in start_days)
    if not starts or len(set(starts)) != len(starts):
        raise ValueError("start_days must be non-empty and unique")
    days = tuple(sorted({int(day) for day in eligible_session_days}))
    if not days:
        raise ValueError("eligible_session_days must be non-empty")
    if any(start not in days for start in starts):
        raise ValueError("every start_day must belong to the supplied block")

    output: dict[str, TimeToCombineHorizonSummary] = {}
    requested_horizons: tuple[int | None, ...] = (
        *FROZEN_TRADING_DAY_HORIZONS,
        None,
    )
    for requested in requested_horizons:
        label = FULL_AVAILABLE_HORIZON if requested is None else str(requested)
        episodes = tuple(
            run_censored_shared_account_episode(
                component_events,
                days,
                basket=basket,
                controller=controller,
                start_day=start,
                horizon_days=requested,
                config=config,
            )
            for start in starts
        )
        output[label] = summarize_time_to_combine(
            episodes,
            horizon_label=label,
            requested_horizon_days=requested,
            block_id=block_id,
        )
    return output


def summarize_time_to_combine(
    episodes: Sequence[CensoredAccountPolicyEpisode],
    *,
    horizon_label: str,
    requested_horizon_days: int | None,
    block_id: str | None = None,
) -> TimeToCombineHorizonSummary:
    rows = tuple(episodes)
    if not rows:
        raise ValueError("time-to-Combine summary requires at least one episode")
    if any(row.requested_horizon_days != requested_horizon_days for row in rows):
        raise ValueError("summary episodes must share the requested horizon")

    statuses = Counter(row.observation_status.value for row in rows)
    progress = np.asarray(
        [row.legacy_episode.target_progress for row in rows], dtype=float
    )
    maximum_progress = np.asarray(
        [row.legacy_episode.maximum_target_progress for row in rows], dtype=float
    )
    # AccountPolicyEpisode.net_pnl is already net of the immutable event costs.
    # Subtracting total_cost again would double-count costs.
    net_after_costs = np.asarray(
        [row.legacy_episode.net_pnl for row in rows], dtype=float
    )
    passing_days = [
        float(row.legacy_episode.days_to_target)
        for row in rows
        if row.target_reached and row.legacy_episode.days_to_target is not None
    ]
    count = len(rows)
    data_censored = statuses[AccountObservationStatus.DATA_CENSORED.value]
    operational_censored = statuses[
        AccountObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED.value
    ]
    pass_count = statuses[AccountObservationStatus.TARGET_REACHED.value]
    mll_count = statuses[AccountObservationStatus.MLL_BREACHED.value]
    fully_observed = count - data_censored
    consistency_count = sum(row.legacy_episode.consistency_ok for row in rows)
    return TimeToCombineHorizonSummary(
        block_id=block_id,
        horizon_label=str(horizon_label),
        requested_horizon_days=requested_horizon_days,
        episode_count=count,
        pass_count=pass_count,
        pass_probability=pass_count / count,
        pass_probability_lower_bound=pass_count / count,
        pass_probability_upper_bound=(pass_count + data_censored) / count,
        fully_observed_for_horizon_count=fully_observed,
        pass_probability_among_fully_observed=(
            pass_count / fully_observed if fully_observed else None
        ),
        mll_breach_count=mll_count,
        mll_breach_probability=mll_count / count,
        mll_breach_probability_lower_bound=mll_count / count,
        mll_breach_probability_upper_bound=(mll_count + data_censored) / count,
        hard_rule_failure_count=statuses[
            AccountObservationStatus.HARD_RULE_FAILURE.value
        ],
        data_censored_count=data_censored,
        operational_horizon_not_reached_count=operational_censored,
        censored_count=data_censored + operational_censored,
        consistency_ok_count=consistency_count,
        consistency_ok_probability=consistency_count / count,
        target_progress_p25=float(np.percentile(progress, 25)),
        target_progress_median=float(np.median(progress)),
        target_progress_p75=float(np.percentile(progress, 75)),
        maximum_target_progress_median=float(np.median(maximum_progress)),
        net_after_costs_total=float(np.sum(net_after_costs)),
        net_after_costs_mean=float(np.mean(net_after_costs)),
        net_after_costs_p25=float(np.percentile(net_after_costs, 25)),
        net_after_costs_median=float(np.median(net_after_costs)),
        net_after_costs_p75=float(np.percentile(net_after_costs, 75)),
        total_execution_cost=float(
            sum(row.legacy_episode.total_cost for row in rows)
        ),
        expected_trading_days_to_pass_conditional=(
            float(np.mean(passing_days)) if passing_days else None
        ),
        median_trading_days_to_pass_conditional=(
            float(np.median(passing_days)) if passing_days else None
        ),
        time_to_target_days=tuple(sorted(int(value) for value in passing_days)),
        compact_episode_outcomes=tuple(
            {
                "start_day": row.legacy_episode.start_day,
                "status": row.observation_status.value,
                "available_horizon_days": row.available_horizon_days,
                "observed_days": row.observed_days,
                "days_to_target": row.legacy_episode.days_to_target,
                "net_after_costs": row.legacy_episode.net_pnl,
                "target_progress": row.legacy_episode.target_progress,
                "maximum_target_progress": (
                    row.legacy_episode.maximum_target_progress
                ),
                "mll_breached": row.legacy_episode.mll_breached,
                "consistency_ok": row.legacy_episode.consistency_ok,
            }
            for row in rows
        ),
        target_progress_curve=_target_progress_curve(rows),
        episodes=rows,
    )


def _daily_target_progress(
    episode: AccountPolicyEpisode,
    rules: Topstep150KConfig,
) -> tuple[float, ...]:
    """Reconstruct the simulator's consistency-adjusted target by day."""

    best_day = 0.0
    required_target = float(rules.combine_profit_target)
    values: list[float] = []
    for row in episode.daily_path:
        best_day = max(best_day, float(row["day_pnl"]))
        if (
            best_day
            > float(rules.combine_profit_target)
            * float(rules.consistency_best_day_max_pct_of_profit_target)
        ):
            required_target = max(
                required_target,
                best_day
                / float(rules.consistency_best_day_max_pct_of_profit_target),
            )
        values.append(
            (
                float(row["balance"])
                - float(rules.combine_starting_balance)
            )
            / max(required_target, 1.0)
        )
    if values:
        # The episode's terminal value is authoritative and accounts for every
        # simulator branch, including failures before end-of-day bookkeeping.
        values[-1] = float(episode.target_progress)
    return tuple(values)


def _target_progress_curve(
    episodes: Sequence[CensoredAccountPolicyEpisode],
) -> tuple[dict[str, float | int], ...]:
    maximum_day = max(row.observed_days for row in episodes)
    count = len(episodes)
    curve: list[dict[str, float | int]] = []
    for day in range(1, maximum_day + 1):
        observed = [
            row.daily_target_progress[day - 1]
            for row in episodes
            if len(row.daily_target_progress) >= day
        ]
        target_count = sum(
            row.legacy_episode.days_to_target is not None
            and int(row.legacy_episode.days_to_target) <= day
            for row in episodes
        )
        mll_count = sum(
            row.mll_breached and row.observed_days <= day for row in episodes
        )
        hard_count = sum(
            row.hard_rule_failed and row.observed_days <= day for row in episodes
        )
        censored_count = sum(
            row.censored and row.observed_days <= day for row in episodes
        )
        at_risk = sum(
            row.observed_days >= day
            and not (
                row.legacy_episode.days_to_target is not None
                and int(row.legacy_episode.days_to_target) < day
            )
            and not (
                (row.mll_breached or row.hard_rule_failed)
                and row.observed_days < day
            )
            for row in episodes
        )
        curve.append(
            {
                "trading_day": day,
                "observed_episode_count": len(observed),
                "at_risk": at_risk,
                "target_progress_p25": float(np.percentile(observed, 25)),
                "target_progress_median": float(np.median(observed)),
                "target_progress_p75": float(np.percentile(observed, 75)),
                "pass_cumulative_probability": target_count / count,
                "mll_cumulative_probability": mll_count / count,
                "hard_rule_failure_cumulative_probability": hard_count / count,
                "censoring_cumulative_probability": censored_count / count,
            }
        )
    return tuple(curve)


# Concise aliases for callers that prefer the account-policy vocabulary.
evaluate_account_policy_horizons = evaluate_time_to_combine
run_account_policy_horizon_episode = run_censored_shared_account_episode
summarize_account_policy_horizons = summarize_time_to_combine


__all__ = [
    "AccountObservationStatus",
    "CensoredAccountPolicyEpisode",
    "FROZEN_TRADING_DAY_HORIZONS",
    "FULL_AVAILABLE_HORIZON",
    "TimeToCombineHorizonSummary",
    "classify_account_observation_status",
    "evaluate_account_policy_horizons",
    "evaluate_time_to_combine",
    "run_account_policy_horizon_episode",
    "run_censored_shared_account_episode",
    "summarize_account_policy_horizons",
    "summarize_time_to_combine",
]
