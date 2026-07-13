from __future__ import annotations

import math
from collections import Counter
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.topstep_150k import Topstep150KConfig


class CombineObservationStatus(StrEnum):
    """Scientific observation state, separate from the legacy episode terminal.

    Historical V5/V7 files retain ``TIMEOUT`` for reproducibility.  V7.2 never
    interprets that operational boundary as an official account failure.
    """

    TARGET_REACHED = "TARGET_REACHED"
    MLL_BREACHED = "MLL_BREACHED"
    DATA_CENSORED = "DATA_CENSORED"
    OPERATIONAL_HORIZON_NOT_REACHED = "OPERATIONAL_HORIZON_NOT_REACHED"
    HARD_RULE_FAILURE = "HARD_RULE_FAILURE"


@dataclass(frozen=True, slots=True)
class CensoredCombineEpisode:
    observation_status: CombineObservationStatus
    requested_horizon_days: int | None
    available_horizon_days: int
    observed_days: int
    legacy_result: CombineEpisodeResult

    @property
    def target_reached(self) -> bool:
        return self.observation_status is CombineObservationStatus.TARGET_REACHED

    @property
    def mll_breached(self) -> bool:
        return self.observation_status is CombineObservationStatus.MLL_BREACHED

    @property
    def censored(self) -> bool:
        return self.observation_status in {
            CombineObservationStatus.DATA_CENSORED,
            CombineObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED,
        }

    def to_dict(self, *, include_daily_path: bool = False) -> dict[str, Any]:
        payload = {
            "observation_status": self.observation_status.value,
            "requested_horizon_days": self.requested_horizon_days,
            "available_horizon_days": self.available_horizon_days,
            "observed_days": self.observed_days,
            "target_reached": self.target_reached,
            "mll_breached": self.mll_breached,
            "censored": self.censored,
            "legacy_result": self.legacy_result.to_dict(),
        }
        if not include_daily_path:
            payload["legacy_result"].pop("daily_path", None)
        return payload


@dataclass(frozen=True, slots=True)
class CensoredHorizonSummary:
    horizon_label: str
    requested_horizon_days: int | None
    episode_count: int
    target_reached_count: int
    target_reached_probability: float
    mll_breached_count: int
    mll_breached_probability: float
    data_censored_count: int
    operational_horizon_not_reached_count: int
    hard_rule_failure_count: int
    target_progress_p25: float
    target_progress_median: float
    target_progress_p75: float
    maximum_target_progress_median: float
    net_pnl_p25: float
    net_pnl_median: float
    net_pnl_p75: float
    expected_days_to_pass_conditional: float | None
    median_days_to_pass_conditional: float | None
    median_observed_subscription_months: float
    median_observed_subscription_cost_usd: float
    target_time_curve: tuple[dict[str, float | int], ...]
    episodes: tuple[CensoredCombineEpisode, ...]

    def to_dict(self, *, include_episodes: bool = False) -> dict[str, Any]:
        payload = asdict(self)
        payload["target_time_curve"] = list(self.target_time_curve)
        if include_episodes:
            payload["episodes"] = [row.to_dict() for row in self.episodes]
        else:
            payload.pop("episodes", None)
        return payload


def classify_observation_status(
    terminal: CombineTerminal,
    *,
    requested_horizon_days: int | None,
    available_horizon_days: int,
) -> CombineObservationStatus:
    if terminal is CombineTerminal.PASSED:
        return CombineObservationStatus.TARGET_REACHED
    if terminal is CombineTerminal.MLL_BREACH:
        return CombineObservationStatus.MLL_BREACHED
    if terminal is CombineTerminal.COMPLIANCE_FAILURE:
        return CombineObservationStatus.HARD_RULE_FAILURE
    if terminal is not CombineTerminal.TIMEOUT:
        raise ValueError(f"unsupported Combine terminal: {terminal}")
    if requested_horizon_days is None:
        return CombineObservationStatus.DATA_CENSORED
    if available_horizon_days < requested_horizon_days:
        return CombineObservationStatus.DATA_CENSORED
    return CombineObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED


def run_censored_combine_episode(
    events: Sequence[TradePathEvent],
    eligible_session_days: Sequence[int],
    *,
    start_day: int,
    horizon_days: int | None,
    config: Topstep150KConfig | None = None,
    maximum_mini_equivalent: float = 15.0,
    start_regime: str | None = None,
) -> CensoredCombineEpisode:
    days = tuple(sorted({int(day) for day in eligible_session_days}))
    if start_day not in days:
        raise ValueError("start_day must be an eligible session day")
    if horizon_days is not None and horizon_days <= 0:
        raise ValueError("horizon_days must be positive or None")
    available = len(days) - days.index(int(start_day))
    duration = available if horizon_days is None else min(horizon_days, available)
    if duration <= 0:
        raise ValueError("episode has no observable sessions")
    result = run_combine_episode(
        events,
        days,
        start_day=int(start_day),
        maximum_duration_days=int(duration),
        config=config,
        maximum_mini_equivalent=maximum_mini_equivalent,
        start_regime=start_regime,
    )
    status = classify_observation_status(
        result.terminal,
        requested_horizon_days=horizon_days,
        available_horizon_days=available,
    )
    return CensoredCombineEpisode(
        observation_status=status,
        requested_horizon_days=horizon_days,
        available_horizon_days=available,
        observed_days=int(result.eligible_days),
        legacy_result=result,
    )


def evaluate_censored_combine_horizons(
    events: Sequence[TradePathEvent],
    eligible_session_days: Sequence[int],
    *,
    start_days: Sequence[int],
    horizons: Sequence[int] = (20, 40, 60, 90),
    include_full_available: bool = True,
    config: Topstep150KConfig | None = None,
    maximum_mini_equivalent: float = 15.0,
    subscription_month_days: int = 20,
    subscription_cost_usd: float = 149.0,
) -> dict[str, CensoredHorizonSummary]:
    starts = tuple(int(day) for day in start_days)
    if not starts or len(set(starts)) != len(starts):
        raise ValueError("start_days must be non-empty and unique")
    normalized_horizons = tuple(int(value) for value in horizons)
    if any(value <= 0 for value in normalized_horizons):
        raise ValueError("all reporting horizons must be positive")
    if len(set(normalized_horizons)) != len(normalized_horizons):
        raise ValueError("reporting horizons must be unique")
    if subscription_month_days <= 0 or subscription_cost_usd < 0.0:
        raise ValueError("subscription proxy configuration is invalid")

    output: dict[str, CensoredHorizonSummary] = {}
    requested: tuple[int | None, ...] = normalized_horizons + (
        (None,) if include_full_available else ()
    )
    for horizon in requested:
        label = "full_available" if horizon is None else str(horizon)
        episodes = tuple(
            run_censored_combine_episode(
                events,
                eligible_session_days,
                start_day=start,
                horizon_days=horizon,
                config=config,
                maximum_mini_equivalent=maximum_mini_equivalent,
            )
            for start in starts
        )
        output[label] = summarize_censored_episodes(
            episodes,
            horizon_label=label,
            requested_horizon_days=horizon,
            subscription_month_days=subscription_month_days,
            subscription_cost_usd=subscription_cost_usd,
        )
    return output


def summarize_censored_episodes(
    episodes: Sequence[CensoredCombineEpisode],
    *,
    horizon_label: str,
    requested_horizon_days: int | None,
    subscription_month_days: int = 20,
    subscription_cost_usd: float = 149.0,
) -> CensoredHorizonSummary:
    rows = tuple(episodes)
    if not rows:
        raise ValueError("censored summary requires episodes")
    statuses = Counter(row.observation_status.value for row in rows)
    progress = np.asarray(
        [row.legacy_result.target_progress for row in rows], dtype=float
    )
    maximum_progress = np.asarray(
        [_maximum_progress(row.legacy_result) for row in rows], dtype=float
    )
    net = np.asarray([row.legacy_result.net_pnl for row in rows], dtype=float)
    passing_days = [
        float(row.legacy_result.days_to_target)
        for row in rows
        if row.legacy_result.days_to_target is not None
    ]
    months = np.asarray(
        [math.ceil(row.observed_days / subscription_month_days) for row in rows],
        dtype=float,
    )
    count = len(rows)
    return CensoredHorizonSummary(
        horizon_label=horizon_label,
        requested_horizon_days=requested_horizon_days,
        episode_count=count,
        target_reached_count=statuses[CombineObservationStatus.TARGET_REACHED.value],
        target_reached_probability=(
            statuses[CombineObservationStatus.TARGET_REACHED.value] / count
        ),
        mll_breached_count=statuses[CombineObservationStatus.MLL_BREACHED.value],
        mll_breached_probability=(
            statuses[CombineObservationStatus.MLL_BREACHED.value] / count
        ),
        data_censored_count=statuses[CombineObservationStatus.DATA_CENSORED.value],
        operational_horizon_not_reached_count=statuses[
            CombineObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED.value
        ],
        hard_rule_failure_count=statuses[
            CombineObservationStatus.HARD_RULE_FAILURE.value
        ],
        target_progress_p25=float(np.percentile(progress, 25)),
        target_progress_median=float(np.median(progress)),
        target_progress_p75=float(np.percentile(progress, 75)),
        maximum_target_progress_median=float(np.median(maximum_progress)),
        net_pnl_p25=float(np.percentile(net, 25)),
        net_pnl_median=float(np.median(net)),
        net_pnl_p75=float(np.percentile(net, 75)),
        expected_days_to_pass_conditional=(
            float(np.mean(passing_days)) if passing_days else None
        ),
        median_days_to_pass_conditional=(
            float(np.median(passing_days)) if passing_days else None
        ),
        median_observed_subscription_months=float(np.median(months)),
        median_observed_subscription_cost_usd=float(
            np.median(months) * subscription_cost_usd
        ),
        target_time_curve=_target_time_curve(rows),
        episodes=rows,
    )


def _maximum_progress(result: CombineEpisodeResult) -> float:
    starting_balance = result.daily_path[0]["balance"] - result.daily_path[0]["day_pnl"]
    return max(
        (
            (float(day["balance"]) - float(starting_balance))
            / max(float(result.required_target), 1.0)
            for day in result.daily_path
        ),
        default=0.0,
    )


def _target_time_curve(
    episodes: Sequence[CensoredCombineEpisode],
) -> tuple[dict[str, float | int], ...]:
    maximum_day = max(row.observed_days for row in episodes)
    count = len(episodes)
    curve: list[dict[str, float | int]] = []
    for day in range(1, maximum_day + 1):
        target = sum(
            row.legacy_result.days_to_target is not None
            and int(row.legacy_result.days_to_target) <= day
            for row in episodes
        )
        mll = sum(
            row.observation_status is CombineObservationStatus.MLL_BREACHED
            and row.observed_days <= day
            for row in episodes
        )
        hard = sum(
            row.observation_status is CombineObservationStatus.HARD_RULE_FAILURE
            and row.observed_days <= day
            for row in episodes
        )
        at_risk = sum(
            row.observed_days >= day
            and not (
                row.legacy_result.days_to_target is not None
                and int(row.legacy_result.days_to_target) < day
            )
            and not (
                row.observation_status
                in {
                    CombineObservationStatus.MLL_BREACHED,
                    CombineObservationStatus.HARD_RULE_FAILURE,
                }
                and row.observed_days < day
            )
            for row in episodes
        )
        curve.append(
            {
                "trading_day": day,
                "at_risk": at_risk,
                "target_cumulative_incidence": target / count,
                "mll_cumulative_incidence": mll / count,
                "hard_rule_failure_cumulative_incidence": hard / count,
                "target_not_reached_probability": 1.0 - target / count,
            }
        )
    return tuple(curve)


__all__ = [
    "CensoredCombineEpisode",
    "CensoredHorizonSummary",
    "CombineObservationStatus",
    "classify_observation_status",
    "evaluate_censored_combine_horizons",
    "run_censored_combine_episode",
    "summarize_censored_episodes",
]
