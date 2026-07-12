from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.topstep_150k import Topstep150KConfig


@dataclass(frozen=True, slots=True)
class EpisodeStartPolicy:
    maximum_starts: int = 24
    minimum_spacing_sessions: int = 5
    minimum_observation_sessions: int = 30
    maximum_duration_sessions: int = 60
    regime_balanced: bool = True

    def __post_init__(self) -> None:
        if self.maximum_starts <= 0:
            raise ValueError("maximum_starts must be positive")
        if self.minimum_spacing_sessions <= 0:
            raise ValueError("minimum_spacing_sessions must be positive")
        if self.minimum_observation_sessions <= 0:
            raise ValueError("minimum_observation_sessions must be positive")
        if self.maximum_duration_sessions < self.minimum_observation_sessions:
            raise ValueError(
                "maximum_duration_sessions cannot be below minimum_observation_sessions"
            )


@dataclass(frozen=True, slots=True)
class RollingCombineSummary:
    eligible_episode_start_count: int
    episode_start_count: int
    effective_block_count: int
    episode_start_days: tuple[int, ...]
    pass_count: int
    pass_rate: float
    mll_breach_count: int
    mll_breach_rate: float
    timeout_count: int
    timeout_rate: float
    compliance_failure_count: int
    median_days_to_target: float | None
    p25_days_to_target: float | None
    p75_days_to_target: float | None
    median_target_progress_when_not_passed: float
    median_episode_net_pnl: float
    net_pnl_after_costs_unique_events: float
    minimum_mll_buffer: float
    consistency_pass_rate: float
    median_best_day_concentration: float
    p90_best_day_concentration: float
    contract_limit_compliance_rate: float
    session_compliance_rate: float
    failure_regime_distribution: dict[str, int]
    terminal_distribution: dict[str, int]
    pass_rate_by_regime: dict[str, float]
    account_path_distribution: dict[str, float]
    event_count: int
    same_bar_ambiguous_count: int
    policy: EpisodeStartPolicy
    episodes: tuple[CombineEpisodeResult, ...]

    def to_dict(self, *, include_daily_paths: bool = False) -> dict[str, Any]:
        payload = asdict(self)
        payload["policy"] = asdict(self.policy)
        episode_rows: list[dict[str, Any]] = []
        for episode in self.episodes:
            row = episode.to_dict()
            if not include_daily_paths:
                row.pop("daily_path", None)
            episode_rows.append(row)
        payload["episodes"] = episode_rows
        return payload


def select_episode_starts(
    eligible_session_days: Sequence[int],
    *,
    day_regimes: Mapping[int, str] | None = None,
    policy: EpisodeStartPolicy | None = None,
) -> tuple[int, ...]:
    """Choose deterministic, spaced starts with at most one start per block."""

    selection = policy or EpisodeStartPolicy()
    days = tuple(sorted({int(value) for value in eligible_session_days}))
    usable = len(days) - selection.minimum_observation_sessions + 1
    if usable <= 0:
        return ()
    maximum_by_spacing = 1 + (usable - 1) // selection.minimum_spacing_sessions
    count = min(selection.maximum_starts, maximum_by_spacing)
    edges = np.linspace(0, usable, count + 1, dtype=int)
    regimes = sorted(
        {
            str((day_regimes or {}).get(day, "UNKNOWN"))
            for day in days[:usable]
        }
    )
    selected_indices: list[int] = []
    for block in range(count):
        low = int(edges[block])
        high = max(low + 1, int(edges[block + 1]))
        candidates = list(range(low, min(high, usable)))
        if selected_indices:
            candidates = [
                index
                for index in candidates
                if index - selected_indices[-1] >= selection.minimum_spacing_sessions
            ]
        if not candidates:
            fallback = (
                selected_indices[-1] + selection.minimum_spacing_sessions
                if selected_indices
                else low
            )
            if fallback >= usable:
                continue
            candidates = [fallback]
        desired = regimes[block % len(regimes)] if regimes else "UNKNOWN"
        matching = [
            index
            for index in candidates
            if str((day_regimes or {}).get(days[index], "UNKNOWN")) == desired
        ]
        pool = matching if selection.regime_balanced and matching else candidates
        center = (low + high - 1) / 2.0
        chosen = min(pool, key=lambda index: (abs(index - center), index))
        selected_indices.append(chosen)
    return tuple(days[index] for index in selected_indices)


def evaluate_rolling_combine(
    events: Sequence[TradePathEvent],
    eligible_session_days: Sequence[int],
    *,
    day_regimes: Mapping[int, str] | None = None,
    policy: EpisodeStartPolicy | None = None,
    config: Topstep150KConfig | None = None,
    maximum_mini_equivalent: float = 15.0,
) -> RollingCombineSummary:
    selection = policy or EpisodeStartPolicy()
    starts = select_episode_starts(
        eligible_session_days, day_regimes=day_regimes, policy=selection
    )
    if not starts:
        raise ValueError("rolling Combine requires at least one eligible episode start")
    episodes = tuple(
        run_combine_episode(
            events,
            eligible_session_days,
            start_day=start,
            maximum_duration_days=selection.maximum_duration_sessions,
            config=config,
            maximum_mini_equivalent=maximum_mini_equivalent,
            start_regime=str((day_regimes or {}).get(start, "UNKNOWN")),
        )
        for start in starts
    )
    count = len(episodes)
    day_positions = {
        day: index
        for index, day in enumerate(
            sorted({int(value) for value in eligible_session_days})
        )
    }
    effective_blocks = _non_overlapping_block_count(
        starts,
        day_positions=day_positions,
        duration=selection.maximum_duration_sessions,
    )
    terminal = Counter(episode.terminal.value for episode in episodes)
    failures = Counter(
        episode.start_regime
        for episode in episodes
        if episode.terminal != CombineTerminal.PASSED
    )
    regime_results: dict[str, list[bool]] = defaultdict(list)
    for episode in episodes:
        regime_results[episode.start_regime].append(episode.passed)
    passing_days = [
        float(episode.days_to_target)
        for episode in episodes
        if episode.days_to_target is not None
    ]
    nonpassing_progress = [
        episode.target_progress for episode in episodes if not episode.passed
    ]
    terminal_net = np.asarray([episode.net_pnl for episode in episodes], dtype=float)
    terminal_buffer = np.asarray(
        [episode.minimum_mll_buffer for episode in episodes], dtype=float
    )
    concentrations = np.asarray(
        [episode.best_day_concentration for episode in episodes], dtype=float
    )
    return RollingCombineSummary(
        eligible_episode_start_count=max(
            0,
            len(set(int(value) for value in eligible_session_days))
            - selection.minimum_observation_sessions
            + 1,
        ),
        episode_start_count=count,
        effective_block_count=effective_blocks,
        episode_start_days=starts,
        pass_count=terminal[CombineTerminal.PASSED.value],
        pass_rate=terminal[CombineTerminal.PASSED.value] / count,
        mll_breach_count=terminal[CombineTerminal.MLL_BREACH.value],
        mll_breach_rate=terminal[CombineTerminal.MLL_BREACH.value] / count,
        timeout_count=terminal[CombineTerminal.TIMEOUT.value],
        timeout_rate=terminal[CombineTerminal.TIMEOUT.value] / count,
        compliance_failure_count=terminal[
            CombineTerminal.COMPLIANCE_FAILURE.value
        ],
        median_days_to_target=_percentile_or_none(passing_days, 50),
        p25_days_to_target=_percentile_or_none(passing_days, 25),
        p75_days_to_target=_percentile_or_none(passing_days, 75),
        median_target_progress_when_not_passed=float(
            np.median(nonpassing_progress) if nonpassing_progress else 1.0
        ),
        median_episode_net_pnl=float(np.median(terminal_net)),
        net_pnl_after_costs_unique_events=float(sum(event.net_pnl for event in events)),
        minimum_mll_buffer=float(np.min(terminal_buffer)),
        consistency_pass_rate=float(
            sum(episode.consistency_ok for episode in episodes) / count
        ),
        median_best_day_concentration=float(np.median(concentrations)),
        p90_best_day_concentration=float(np.percentile(concentrations, 90)),
        contract_limit_compliance_rate=float(
            sum(episode.contract_limit_compliant for episode in episodes) / count
        ),
        session_compliance_rate=float(
            sum(episode.session_compliant for episode in episodes) / count
        ),
        failure_regime_distribution=dict(sorted(failures.items())),
        terminal_distribution=dict(sorted(terminal.items())),
        pass_rate_by_regime={
            regime: float(sum(values) / len(values))
            for regime, values in sorted(regime_results.items())
        },
        account_path_distribution={
            "terminal_net_p25": float(np.percentile(terminal_net, 25)),
            "terminal_net_median": float(np.median(terminal_net)),
            "terminal_net_p75": float(np.percentile(terminal_net, 75)),
            "minimum_buffer_p10": float(np.percentile(terminal_buffer, 10)),
            "minimum_buffer_median": float(np.median(terminal_buffer)),
        },
        event_count=len(events),
        same_bar_ambiguous_count=sum(event.same_bar_ambiguous for event in events),
        policy=selection,
        episodes=episodes,
    )


def _percentile_or_none(values: Sequence[float], percentile: float) -> float | None:
    return float(np.percentile(np.asarray(values, dtype=float), percentile)) if values else None


def _non_overlapping_block_count(
    starts: Sequence[int],
    *,
    day_positions: Mapping[int, int],
    duration: int,
) -> int:
    retained = 0
    next_available = -1
    for start in starts:
        position = day_positions[int(start)]
        if position < next_available:
            continue
        retained += 1
        next_available = position + duration
    return retained


__all__ = [
    "EpisodeStartPolicy",
    "RollingCombineSummary",
    "evaluate_rolling_combine",
    "select_episode_starts",
]
