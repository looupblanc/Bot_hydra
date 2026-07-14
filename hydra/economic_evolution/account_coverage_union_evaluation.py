from __future__ import annotations

import multiprocessing
import statistics
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Any, Iterator, Mapping, Sequence

import hydra.account_policy.basket as basket_engine
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_coverage_union import (
    CoverageUnionPolicy,
    CoverageUnionPolicyPair,
    route_coverage_union_entry,
)
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    _restress_routed_trade,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


COVERAGE_UNION_POLICY_VERSION = "hydra_coverage_union_policy_v1"
_PAIR_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_PAIR_STARTS: tuple[int, ...] = ()
_PAIR_EPISODE_POLICY: EpisodeStartPolicy | None = None
_ROUTER_PATCH_LOCK = threading.RLock()


@dataclass(frozen=True, slots=True)
class CoverageUnionBasketPolicy:
    policy_id: str
    component_ids: tuple[str, ...]
    archetype: str
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    conflict_policy: str
    component_priority: tuple[str, ...]
    policy_version: str = COVERAGE_UNION_POLICY_VERSION

    def __post_init__(self) -> None:
        if not self.policy_id or not 10 <= len(self.component_ids) <= 12:
            raise ValueError("coverage basket identity or breadth is invalid")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("coverage basket components must be unique")
        if self.component_priority != self.component_ids:
            raise ValueError("coverage basket priority must be frozen")
        if self.maximum_simultaneous_positions != 3:
            raise ValueError("coverage basket concurrency drift")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("coverage basket contract limit is invalid")
        if self.conflict_policy != "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE":
            raise ValueError("coverage basket conflict policy drift")

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.STATIC_BASKET

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["component_priority"] = list(self.component_priority)
        row["kind"] = self.kind.value
        return row


def evaluate_coverage_union_policy_pairs(
    pairs: Sequence[CoverageUnionPolicyPair],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
) -> list[dict[str, Any]]:
    if worker_count < 1:
        raise ValueError("worker count must be positive")
    ordered = sorted(pairs, key=lambda row: row.pair_id)
    if worker_count == 1:
        return [
            evaluate_coverage_union_policy_pair(
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
    pair: CoverageUnionPolicyPair,
) -> dict[str, Any]:
    if not _PAIR_RUNTIMES or not _PAIR_STARTS or _PAIR_EPISODE_POLICY is None:
        raise RuntimeError("coverage-union worker has no frozen fork state")
    return evaluate_coverage_union_policy_pair(
        pair,
        _PAIR_RUNTIMES,
        starts=_PAIR_STARTS,
        episode_policy=_PAIR_EPISODE_POLICY,
    )


def evaluate_coverage_union_policy_pair(
    pair: CoverageUnionPolicyPair,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    real = _evaluate_policy(
        pair.real_policy,
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
    )
    control = _evaluate_policy(
        pair.matched_control_policy,
        runtimes,
        starts=real["episode_start_days"],
        episode_policy=episode_policy,
    )
    if real["episode_start_days"] != control["episode_start_days"]:
        raise ValueError("coverage-union pair used different episode starts")
    real_normal = real["normal"]
    real_stress = real["stress"]
    control_normal = control["normal"]
    control_stress = control["stress"]
    deltas = {
        "normal_median_net_usd": (
            real_normal.median_episode_net_pnl
            - control_normal.median_episode_net_pnl
        ),
        "stressed_median_net_usd": (
            real_stress.median_episode_net_pnl
            - control_stress.median_episode_net_pnl
        ),
        "normal_target_progress": (
            real_normal.target_progress_median
            - control_normal.target_progress_median
        ),
        "stressed_target_progress": (
            real_stress.target_progress_median
            - control_stress.target_progress_median
        ),
        "normal_mll_breach_rate": (
            real_normal.mll_breach_rate - control_normal.mll_breach_rate
        ),
        "stressed_mll_breach_rate": (
            real_stress.mll_breach_rate - control_stress.mll_breach_rate
        ),
        "normal_consistency_pass_rate": (
            real_normal.consistency_pass_rate
            - control_normal.consistency_pass_rate
        ),
        "stressed_consistency_pass_rate": (
            real_stress.consistency_pass_rate
            - control_stress.consistency_pass_rate
        ),
    }
    blocks = _temporal_blocks(real_stress.episodes, count=4)
    contribution = {
        key: max(0.0, float(value))
        for key, value in real_stress.component_contribution.items()
    }
    total = sum(contribution.values())
    maximum_share = (
        max(contribution.values(), default=0.0) / total if total > 0.0 else 1.0
    )
    behavior = stable_hash(
        {
            "starts": real["episode_start_days"],
            "paths": [
                {
                    "terminal": row.terminal.value,
                    "net": round(row.net_pnl, 8),
                    "progress": round(row.target_progress, 10),
                    "mll": row.mll_breached,
                    "consistency": row.consistency_ok,
                }
                for row in real_normal.episodes
            ],
        }
    )
    return {
        **pair.to_dict(),
        "real_policy": pair.real_policy.to_dict(),
        "matched_control_policy": pair.matched_control_policy.to_dict(),
        "identical_episode_starts": True,
        "episode_start_count": len(real["episode_start_days"]),
        "behavioral_fingerprint": behavior,
        "real_evaluation": {
            "episode_start_days": real["episode_start_days"],
            "controlled_base": real_normal.to_dict(),
            "controlled_stress_1_5x": real_stress.to_dict(),
        },
        "matched_control_evaluation": {
            "episode_start_days": control["episode_start_days"],
            "controlled_base": control_normal.to_dict(),
            "controlled_stress_1_5x": control_stress.to_dict(),
        },
        "real_stressed_temporal_blocks": blocks,
        "real_positive_temporal_block_count": sum(
            float(row["median_net_usd"]) > 0.0 for row in blocks
        ),
        "real_maximum_positive_component_share": maximum_share,
        "paired_delta": deltas,
        "development_only": True,
        "validated": False,
        "proof_window_consumed": False,
        "new_data_purchase_count": 0,
        "q4_access_delta": 0,
        "orders": 0,
    }


def _evaluate_policy(
    policy: CoverageUnionPolicy,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    selected = [runtimes[value] for value in policy.component_ids]
    common = set(selected[0].eligible_session_days)
    for runtime in selected[1:]:
        common.intersection_update(runtime.eligible_session_days)
    days = tuple(sorted(common))
    if not days:
        raise ValueError("coverage-union policy has no common session days")
    events = {row.sleeve_id: row.events for row in selected}
    stressed = {
        component_id: tuple(
            _restress_routed_trade(row, cost_stress=1.5) for row in values
        )
        for component_id, values in events.items()
    }
    basket = CoverageUnionBasketPolicy(
        policy_id=policy.basket_policy_id,
        component_ids=policy.component_ids,
        archetype="CROSS_MARKET_SESSION_COVERAGE_UNION",
        maximum_simultaneous_positions=policy.maximum_simultaneous_positions,
        maximum_mini_equivalent=policy.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=policy.component_ids,
    )
    with _patched_coverage_union_router():
        normal = basket_engine.evaluate_account_policy(
            events,
            days,
            basket=basket,  # type: ignore[arg-type]
            controller=policy,  # type: ignore[arg-type]
            episode_policy=episode_policy,
            explicit_start_days=starts,
        )
    with _patched_coverage_union_router():
        stress = basket_engine.evaluate_account_policy(
            stressed,
            days,
            basket=basket,  # type: ignore[arg-type]
            controller=policy,  # type: ignore[arg-type]
            episode_policy=episode_policy,
            explicit_start_days=normal.episode_start_days,
        )
    return {
        "episode_start_days": list(normal.episode_start_days),
        "normal": normal,
        "stress": stress,
    }


@contextmanager
def _patched_coverage_union_router() -> Iterator[None]:
    def route_coverage(
        intent: Any,
        state: Any,
        *,
        policy: CoverageUnionPolicy,
    ) -> Any:
        return route_coverage_union_entry(intent, state, policy=policy)

    with _ROUTER_PATCH_LOCK:
        prior = basket_engine.route_entry
        basket_engine.route_entry = route_coverage  # type: ignore[assignment]
        try:
            yield
        finally:
            basket_engine.route_entry = prior


def _temporal_blocks(
    episodes: Sequence[Any],
    *,
    count: int,
) -> list[dict[str, Any]]:
    ordered = sorted(episodes, key=lambda row: row.start_day)
    output: list[dict[str, Any]] = []
    for index in range(count):
        chunk = ordered[index::count]
        values = [float(row.net_pnl) for row in chunk]
        output.append(
            {
                "block_id": f"B{index + 1}",
                "episode_count": len(chunk),
                "median_net_usd": statistics.median(values) if values else 0.0,
                "pass_count": sum(row.passed for row in chunk),
                "mll_breach_count": sum(row.mll_breached for row in chunk),
            }
        )
    return output


__all__ = [
    "COVERAGE_UNION_POLICY_VERSION",
    "CoverageUnionBasketPolicy",
    "evaluate_coverage_union_policy_pair",
    "evaluate_coverage_union_policy_pairs",
]
