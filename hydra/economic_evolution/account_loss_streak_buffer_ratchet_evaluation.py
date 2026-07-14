from __future__ import annotations

import gc
from typing import Any, Mapping, Sequence

import hydra.economic_evolution.account_elite_robustness_evaluation as shared
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.account_loss_streak_buffer_ratchet import (
    LossStreakBufferRatchetPair,
)
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


LOSS_STREAK_BUFFER_RATCHET_EVALUATION_VERSION = (
    "hydra_loss_streak_buffer_ratchet_evaluation_v1"
)
DEFAULT_REAL_POLICY_BATCH_SIZE = 48


def evaluate_loss_streak_buffer_ratchet_policy_pairs(
    pairs: Sequence[LossStreakBufferRatchetPair],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
    real_policy_batch_size: int = DEFAULT_REAL_POLICY_BATCH_SIZE,
) -> list[dict[str, Any]]:
    """Evaluate controls once and release real episode paths by micro-batch.

    The legacy paired evaluator retained every full episode object until all
    policies completed.  That is harmless for 200 policies but exceeds an
    eight-gigabyte host at 512.  This function preserves identical scientific
    semantics while converting each bounded real-policy batch to the same
    compact pair rows before releasing its full paths.
    """

    if worker_count < 1 or real_policy_batch_size < 1:
        raise ValueError("ratchet worker and batch sizes must be positive")
    ordered = tuple(sorted(pairs, key=lambda row: row.pair_id))
    if len({row.pair_id for row in ordered}) != len(ordered):
        raise ValueError("ratchet policy pairs must be unique")
    controls = {
        row.matched_control_policy.policy_id: row.matched_control_policy
        for row in ordered
    }
    if len({row.real_policy.policy_id for row in ordered}) != len(ordered):
        raise ValueError("ratchet real policies must be unique")
    control_evaluations = shared._evaluate_unique_policies(
        tuple(sorted(controls.values(), key=lambda row: row.policy_id)),
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=worker_count,
    )
    parent_use_count: dict[str, int] = {}
    for row in ordered:
        parent_use_count[row.parent_policy_id] = (
            parent_use_count.get(row.parent_policy_id, 0) + 1
        )
    output: list[dict[str, Any]] = []
    for offset in range(0, len(ordered), real_policy_batch_size):
        batch = ordered[offset : offset + real_policy_batch_size]
        real_evaluations = shared._evaluate_unique_policies(
            tuple(row.real_policy for row in batch),
            runtimes,
            starts=starts,
            episode_policy=episode_policy,
            worker_count=worker_count,
        )
        for pair in batch:
            row = shared._pair_result(
                pair,  # type: ignore[arg-type]
                real=real_evaluations[pair.real_policy.policy_id],
                control=control_evaluations[pair.matched_control_policy.policy_id],
                control_reused=parent_use_count[pair.parent_policy_id] > 1,
            )
            row["unique_control_evaluation_count"] = len(controls)
            row["unique_real_evaluation_count"] = len(ordered)
            row["memory_bounded_real_policy_batch_size"] = real_policy_batch_size
            row["execution_policy_version"] = (
                LOSS_STREAK_BUFFER_RATCHET_EVALUATION_VERSION
            )
            output.append(row)
        del real_evaluations
        gc.collect()
    if len(output) != len(ordered):
        raise ValueError("ratchet paired evaluation is incomplete")
    return output


__all__ = [
    "DEFAULT_REAL_POLICY_BATCH_SIZE",
    "LOSS_STREAK_BUFFER_RATCHET_EVALUATION_VERSION",
    "evaluate_loss_streak_buffer_ratchet_policy_pairs",
]
