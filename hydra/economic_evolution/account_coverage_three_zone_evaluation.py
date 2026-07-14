from __future__ import annotations

import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Sequence

import hydra.economic_evolution.account_coverage_sizing_evaluation as sizing_eval
from hydra.economic_evolution.account_coverage_three_zone import (
    CoverageThreeZonePolicyPair,
    route_coverage_three_zone_entry,
)
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


THREE_ZONE_POLICY_VERSION = "hydra_coverage_three_zone_policy_v1"
_PAIR_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_PAIR_STARTS: tuple[int, ...] = ()
_PAIR_EPISODE_POLICY: EpisodeStartPolicy | None = None
_ROUTER_BIND_LOCK = threading.RLock()


def evaluate_coverage_three_zone_policy_pairs(
    pairs: Sequence[CoverageThreeZonePolicyPair],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
) -> list[dict[str, Any]]:
    if worker_count < 1:
        raise ValueError("worker count must be positive")
    ordered = sorted(pairs, key=lambda row: row.pair_id)
    control_keys = {_control_cache_key(row, starts=starts) for row in ordered}
    if len(control_keys) != len(ordered):
        raise ValueError("duplicate three-zone controls must be cached upstream")
    if worker_count == 1:
        return [
            evaluate_coverage_three_zone_policy_pair(
                row,
                runtimes,
                starts=starts,
                episode_policy=episode_policy,
            )
            for row in ordered
        ]
    global _PAIR_RUNTIMES, _PAIR_STARTS, _PAIR_EPISODE_POLICY
    _PAIR_RUNTIMES = runtimes
    _PAIR_STARTS = tuple(int(value) for value in starts)
    _PAIR_EPISODE_POLICY = episode_policy
    context = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=worker_count, mp_context=context) as pool:
        rows = list(pool.map(_evaluate_pair_from_fork_state, ordered, chunksize=4))
    _PAIR_RUNTIMES = {}
    _PAIR_STARTS = ()
    _PAIR_EPISODE_POLICY = None
    return sorted(rows, key=lambda row: str(row["pair_id"]))


def _evaluate_pair_from_fork_state(
    pair: CoverageThreeZonePolicyPair,
) -> dict[str, Any]:
    if not _PAIR_RUNTIMES or not _PAIR_STARTS or _PAIR_EPISODE_POLICY is None:
        raise RuntimeError("three-zone worker has no frozen fork state")
    return evaluate_coverage_three_zone_policy_pair(
        pair,
        _PAIR_RUNTIMES,
        starts=_PAIR_STARTS,
        episode_policy=_PAIR_EPISODE_POLICY,
    )


def evaluate_coverage_three_zone_policy_pair(
    pair: CoverageThreeZonePolicyPair,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    # Reuse the already-validated exact account evaluator. Only its routing
    # callback is rebound for this versioned downstream sizing policy.
    with _bound_three_zone_router():
        row = sizing_eval.evaluate_coverage_sizing_policy_pair(  # type: ignore[arg-type]
            pair,
            runtimes,
            starts=starts,
            episode_policy=episode_policy,
        )
    row["control_cache_key"] = _control_cache_key(
        pair,
        starts=row["real_evaluation"]["episode_start_days"],
    )
    row["control_cache_hit"] = False
    row["execution_policy_version"] = THREE_ZONE_POLICY_VERSION
    return row


@contextmanager
def _bound_three_zone_router() -> Iterator[None]:
    with _ROUTER_BIND_LOCK:
        prior = sizing_eval.route_coverage_sizing_entry
        sizing_eval.route_coverage_sizing_entry = (  # type: ignore[assignment]
            route_coverage_three_zone_entry
        )
        try:
            yield
        finally:
            sizing_eval.route_coverage_sizing_entry = prior


def _control_cache_key(
    pair: CoverageThreeZonePolicyPair,
    *,
    starts: Sequence[int],
) -> str:
    return stable_hash(
        {
            "parent_policy_id": pair.parent_policy_id,
            "membership": list(pair.matched_control_policy.component_ids),
            "high_zone_risk_units": 2,
            "middle_zone_risk_units": 2,
            "base_zone_risk_units": 1,
            "starts": [int(value) for value in starts],
            "execution": THREE_ZONE_POLICY_VERSION,
            "costs": [1.0, 1.5],
        }
    )


__all__ = [
    "THREE_ZONE_POLICY_VERSION",
    "evaluate_coverage_three_zone_policy_pair",
    "evaluate_coverage_three_zone_policy_pairs",
]
