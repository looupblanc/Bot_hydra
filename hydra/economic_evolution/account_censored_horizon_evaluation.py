from __future__ import annotations

from dataclasses import replace
from typing import Any, Mapping, Sequence

import hydra.economic_evolution.account_elite_robustness_evaluation as shared
from hydra.economic_evolution.account_censored_horizon import (
    CONTROL_HORIZON_SESSIONS,
    DIAGNOSTIC_HORIZON_SESSIONS,
    CensoredHorizonPair,
)
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


CENSORED_HORIZON_EVALUATION_VERSION = "hydra_censored_horizon_evaluation_v1"


def evaluate_censored_horizon_policy_pairs(
    pairs: Sequence[CensoredHorizonPair],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
) -> list[dict[str, Any]]:
    if episode_policy.maximum_duration_sessions != DIAGNOSTIC_HORIZON_SESSIONS:
        raise ValueError("diagnostic horizon drift")
    if worker_count < 1:
        raise ValueError("censored-horizon worker count must be positive")
    ordered = tuple(sorted(pairs, key=lambda row: row.pair_id))
    if len({row.pair_id for row in ordered}) != len(ordered):
        raise ValueError("censored-horizon pairs must be unique")
    control_policy = replace(
        episode_policy,
        minimum_observation_sessions=CONTROL_HORIZON_SESSIONS,
        maximum_duration_sessions=CONTROL_HORIZON_SESSIONS,
    )
    controls = tuple(
        sorted(
            (row.matched_control_policy for row in ordered),
            key=lambda row: row.policy_id,
        )
    )
    real = tuple(
        sorted((row.real_policy for row in ordered), key=lambda row: row.policy_id)
    )
    control_evaluations = shared._evaluate_unique_policies(
        controls,
        runtimes,
        starts=starts,
        episode_policy=control_policy,
        worker_count=worker_count,
    )
    real_evaluations = shared._evaluate_unique_policies(
        real,
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=worker_count,
    )
    rows: list[dict[str, Any]] = []
    for pair in ordered:
        row = shared._pair_result(
            pair,  # type: ignore[arg-type]
            real=real_evaluations[pair.real_policy.policy_id],
            control=control_evaluations[pair.matched_control_policy.policy_id],
            control_reused=False,
        )
        row.update(
            {
                "unique_control_evaluation_count": len(controls),
                "unique_real_evaluation_count": len(real),
                "execution_policy_version": CENSORED_HORIZON_EVALUATION_VERSION,
                "control_horizon_sessions": CONTROL_HORIZON_SESSIONS,
                "diagnostic_horizon_sessions": DIAGNOSTIC_HORIZON_SESSIONS,
                "policy_behavior_changed": False,
                "official_time_limit_claimed": False,
                "development_diagnostic_only": True,
            }
        )
        rows.append(row)
    return rows


__all__ = [
    "CENSORED_HORIZON_EVALUATION_VERSION",
    "evaluate_censored_horizon_policy_pairs",
]
