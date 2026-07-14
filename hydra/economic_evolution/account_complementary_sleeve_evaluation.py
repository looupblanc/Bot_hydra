from __future__ import annotations

import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Sequence

import hydra.economic_evolution.account_coverage_sizing_evaluation as sizing_eval
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_complementary_sleeve import (
    ComplementarySleevePolicyPair,
    route_complementary_sleeve_entry,
)
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


COMPLEMENTARY_SLEEVE_POLICY_VERSION = "hydra_complementary_sleeve_policy_v1"
_PAIR_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_PAIR_STARTS: tuple[int, ...] = ()
_PAIR_EPISODE_POLICY: EpisodeStartPolicy | None = None
_ROUTER_BIND_LOCK = threading.RLock()


@dataclass(frozen=True, slots=True)
class ComplementarySleeveBasketPolicy:
    """Campaign-local basket contract; shared execution semantics are unchanged."""

    policy_id: str
    component_ids: tuple[str, ...]
    archetype: str
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    conflict_policy: str
    component_priority: tuple[str, ...]
    policy_version: str = COMPLEMENTARY_SLEEVE_POLICY_VERSION

    def __post_init__(self) -> None:
        if not self.policy_id or not 10 <= len(self.component_ids) <= 13:
            raise ValueError("complementary basket identity or breadth is invalid")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("complementary basket components must be unique")
        if self.component_priority != self.component_ids:
            raise ValueError("complementary basket priority must be frozen")
        if self.maximum_simultaneous_positions != 3:
            raise ValueError("complementary basket concurrency drift")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("complementary basket contract limit is invalid")
        if self.conflict_policy != "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE":
            raise ValueError("complementary basket conflict policy drift")

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.STATIC_BASKET


def evaluate_complementary_sleeve_policy_pairs(
    pairs: Sequence[ComplementarySleevePolicyPair],
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
        raise ValueError("duplicate complementary controls must be cached upstream")
    if worker_count == 1:
        return [
            evaluate_complementary_sleeve_policy_pair(
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
    pair: ComplementarySleevePolicyPair,
) -> dict[str, Any]:
    if not _PAIR_RUNTIMES or not _PAIR_STARTS or _PAIR_EPISODE_POLICY is None:
        raise RuntimeError("complementary worker has no frozen fork state")
    return evaluate_complementary_sleeve_policy_pair(
        pair,
        _PAIR_RUNTIMES,
        starts=_PAIR_STARTS,
        episode_policy=_PAIR_EPISODE_POLICY,
    )


def evaluate_complementary_sleeve_policy_pair(
    pair: ComplementarySleevePolicyPair,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    with _bound_complementary_router():
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
    row["execution_policy_version"] = COMPLEMENTARY_SLEEVE_POLICY_VERSION
    return row


@contextmanager
def _bound_complementary_router() -> Iterator[None]:
    with _ROUTER_BIND_LOCK:
        prior = sizing_eval.route_coverage_sizing_entry
        prior_basket = sizing_eval.CoverageUnionBasketPolicy
        sizing_eval.route_coverage_sizing_entry = (  # type: ignore[assignment]
            route_complementary_sleeve_entry
        )
        sizing_eval.CoverageUnionBasketPolicy = (  # type: ignore[assignment]
            ComplementarySleeveBasketPolicy
        )
        try:
            yield
        finally:
            sizing_eval.route_coverage_sizing_entry = prior
            sizing_eval.CoverageUnionBasketPolicy = prior_basket


def _control_cache_key(
    pair: ComplementarySleevePolicyPair,
    *,
    starts: Sequence[int],
) -> str:
    return stable_hash(
        {
            "parent_policy_id": pair.parent_policy_id,
            "membership": list(pair.matched_control_policy.component_ids),
            "high_zone_risk_units": 3,
            "middle_zone_risk_units": 2,
            "base_zone_risk_units": 1,
            "starts": [int(value) for value in starts],
            "execution": COMPLEMENTARY_SLEEVE_POLICY_VERSION,
            "costs": [1.0, 1.5],
        }
    )


__all__ = [
    "COMPLEMENTARY_SLEEVE_POLICY_VERSION",
    "ComplementarySleeveBasketPolicy",
    "evaluate_complementary_sleeve_policy_pair",
    "evaluate_complementary_sleeve_policy_pairs",
]
