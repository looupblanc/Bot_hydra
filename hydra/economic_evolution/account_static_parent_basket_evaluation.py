from __future__ import annotations

from typing import Any, Mapping, Sequence

from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.account_loss_streak_buffer_ratchet_evaluation import (
    evaluate_loss_streak_buffer_ratchet_policy_pairs,
)
from hydra.economic_evolution.account_static_parent_basket import StaticParentBasketPair
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


STATIC_PARENT_BASKET_EVALUATION_VERSION = (
    "hydra_static_parent_basket_evaluation_v1"
)
DEFAULT_REAL_POLICY_BATCH_SIZE = 32


def evaluate_static_parent_basket_policy_pairs(
    pairs: Sequence[StaticParentBasketPair],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
) -> list[dict[str, Any]]:
    """Reuse the frozen paired-account simulator with bounded memory.

    Controls are exact immutable lead parents and are evaluated once.  Only the
    component membership/priority of the real static basket differs.
    """

    rows = evaluate_loss_streak_buffer_ratchet_policy_pairs(  # type: ignore[arg-type]
        pairs,
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=worker_count,
        real_policy_batch_size=DEFAULT_REAL_POLICY_BATCH_SIZE,
    )
    for row in rows:
        row["execution_policy_version"] = STATIC_PARENT_BASKET_EVALUATION_VERSION
        row["memory_bounded_real_policy_batch_size"] = DEFAULT_REAL_POLICY_BATCH_SIZE
        row["underlying_signals_changed"] = False
    return rows


__all__ = [
    "DEFAULT_REAL_POLICY_BATCH_SIZE",
    "STATIC_PARENT_BASKET_EVALUATION_VERSION",
    "evaluate_static_parent_basket_policy_pairs",
]
