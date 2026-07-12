from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.propfirm.xfa_episode import XfaEpisodeResult, run_xfa_episode


@dataclass(frozen=True, slots=True)
class RollingXfaSummary:
    selected_path: str
    path_selection_policy: str
    episode_start_count: int
    expected_payout_cycles_before_ruin: float
    payout_probability: float
    survival_rate: float
    post_payout_survival_rate: float
    median_first_payout_day: float | None
    median_trader_net_payout: float
    qualifying_day_frequency: float
    minimum_mll_buffer: float
    standard_selected_count: int
    consistency_selected_count: int
    path_summaries: dict[str, dict[str, float | int | None]]
    episodes: tuple[XfaEpisodeResult, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["episodes"] = [episode.to_dict() for episode in self.episodes]
        return payload


def evaluate_rolling_xfa(
    events: Sequence[TradePathEvent],
    eligible_session_days: Sequence[int],
    *,
    day_regimes: Mapping[int, str] | None = None,
    maximum_starts: int = 12,
    config: Topstep150KConfig | None = None,
) -> RollingXfaSummary:
    policy = EpisodeStartPolicy(
        maximum_starts=maximum_starts,
        minimum_spacing_sessions=10,
        minimum_observation_sessions=60,
        maximum_duration_sessions=120,
        regime_balanced=True,
    )
    starts = select_episode_starts(
        eligible_session_days, day_regimes=day_regimes, policy=policy
    )
    if not starts:
        raise ValueError("rolling XFA requires at least one eligible start")
    standard_rows: list[XfaEpisodeResult] = []
    consistency_rows: list[XfaEpisodeResult] = []
    for start in starts:
        standard_rows.append(
            run_xfa_episode(
                events,
                eligible_session_days,
                start_day=start,
                maximum_duration_days=policy.maximum_duration_sessions,
                path="STANDARD",
                config=config,
            )
        )
        consistency_rows.append(
            run_xfa_episode(
                events,
                eligible_session_days,
                start_day=start,
                maximum_duration_days=policy.maximum_duration_sessions,
                path="CONSISTENCY",
                config=config,
            )
        )
    path_rows = {
        "XFA_STANDARD": standard_rows,
        "XFA_CONSISTENCY": consistency_rows,
    }
    path_summaries = {
        name: _path_summary(rows) for name, rows in path_rows.items()
    }
    # A candidate must use one frozen XFA policy across all historical starts.
    # Selecting the better policy independently for each episode is an oracle
    # and overstates payout and survival evidence.
    selected_path = max(
        sorted(path_rows),
        key=lambda name: _path_selection_key(path_summaries[name]),
    )
    selected = path_rows[selected_path]
    count = len(selected)
    payout_days = [
        float(row.first_payout_day)
        for row in selected
        if row.first_payout_day is not None
    ]
    total_traded = sum(row.traded_days for row in selected)
    return RollingXfaSummary(
        selected_path=selected_path,
        path_selection_policy="ONE_AGGREGATE_DEVELOPMENT_POLICY_PER_CANDIDATE",
        episode_start_count=count,
        expected_payout_cycles_before_ruin=float(
            np.mean([row.payout_cycles for row in selected])
        ),
        payout_probability=float(sum(row.payout_cycles > 0 for row in selected) / count),
        survival_rate=float(sum(row.survived for row in selected) / count),
        post_payout_survival_rate=float(
            sum(row.post_payout_survived for row in selected if row.payout_cycles > 0)
            / max(sum(row.payout_cycles > 0 for row in selected), 1)
        ),
        median_first_payout_day=(
            float(np.median(payout_days)) if payout_days else None
        ),
        median_trader_net_payout=float(
            np.median([row.trader_net_payout for row in selected])
        ),
        qualifying_day_frequency=float(
            sum(row.qualifying_winning_days for row in selected)
            / max(total_traded, 1)
        ),
        minimum_mll_buffer=float(
            min(row.minimum_mll_buffer for row in selected)
        ),
        standard_selected_count=count if selected_path == "XFA_STANDARD" else 0,
        consistency_selected_count=(
            count if selected_path == "XFA_CONSISTENCY" else 0
        ),
        path_summaries=path_summaries,
        episodes=tuple(selected),
    )


def _path_summary(
    rows: Sequence[XfaEpisodeResult],
) -> dict[str, float | int | None]:
    payout_days = [
        float(row.first_payout_day)
        for row in rows
        if row.first_payout_day is not None
    ]
    payout_rows = [row for row in rows if row.payout_cycles > 0]
    return {
        "episode_count": len(rows),
        "mean_payout_cycles": float(np.mean([row.payout_cycles for row in rows])),
        "payout_probability": float(
            sum(row.payout_cycles > 0 for row in rows) / max(len(rows), 1)
        ),
        "survival_rate": float(
            sum(row.survived for row in rows) / max(len(rows), 1)
        ),
        "post_payout_survival_rate": float(
            sum(row.post_payout_survived for row in payout_rows)
            / max(len(payout_rows), 1)
        ),
        "median_trader_net_payout": float(
            np.median([row.trader_net_payout for row in rows])
        ),
        "median_first_payout_day": (
            float(np.median(payout_days)) if payout_days else None
        ),
    }


def _path_selection_key(
    summary: Mapping[str, float | int | None],
) -> tuple[float, float, float, float, float]:
    first_day = summary.get("median_first_payout_day")
    return (
        float(summary["mean_payout_cycles"] or 0.0),
        float(summary["survival_rate"] or 0.0),
        float(summary["post_payout_survival_rate"] or 0.0),
        float(summary["median_trader_net_payout"] or 0.0),
        -float(first_day if first_day is not None else 9999.0),
    )


__all__ = ["RollingXfaSummary", "evaluate_rolling_xfa"]
