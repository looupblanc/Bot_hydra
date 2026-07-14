from __future__ import annotations

import multiprocessing
import statistics
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Sequence

import hydra.account_policy.basket as basket_engine
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    ComplementarySleeveBasketPolicy,
)
from hydra.economic_evolution.account_elite_robustness import (
    ELITE_ROBUSTNESS_POLICY_VERSION,
    EliteRobustnessPolicy,
    EliteRobustnessPolicyPair,
    route_elite_robustness_entry,
)
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    _restress_routed_trade,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


ELITE_ROBUSTNESS_EVALUATION_VERSION = (
    "hydra_0018_elite_robustness_evaluation_v1"
)
_POLICY_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_POLICY_STARTS: tuple[int, ...] = ()
_POLICY_EPISODE_POLICY: EpisodeStartPolicy | None = None
_ROUTER_PATCH_LOCK = threading.RLock()


def evaluate_elite_robustness_policy_pairs(
    pairs: Sequence[EliteRobustnessPolicyPair],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
) -> list[dict[str, Any]]:
    """Evaluate every child once and every unchanged 0018 parent once.

    Many targeted children intentionally share one parent.  Controls are
    therefore keyed and evaluated once before their immutable results are
    joined back to every comparison.  This prevents the campaign from spending
    most of its account-simulation budget replaying identical controls.
    """

    if worker_count < 1:
        raise ValueError("worker count must be positive")
    ordered = tuple(sorted(pairs, key=lambda row: row.pair_id))
    if len({row.pair_id for row in ordered}) != len(ordered):
        raise ValueError("elite robustness pairs must be unique")
    real = {row.real_policy.policy_id: row.real_policy for row in ordered}
    controls = {
        row.matched_control_policy.policy_id: row.matched_control_policy
        for row in ordered
    }
    if len(real) != len(ordered):
        raise ValueError("elite robustness real policies must be unique")
    work = tuple(
        sorted(
            {**controls, **real}.values(),
            key=lambda row: row.policy_id,
        )
    )
    evaluations = _evaluate_unique_policies(
        work,
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
    rows = [
        _pair_result(
            pair,
            real=evaluations[pair.real_policy.policy_id],
            control=evaluations[pair.matched_control_policy.policy_id],
            control_reused=parent_use_count[pair.parent_policy_id] > 1,
        )
        for pair in ordered
    ]
    for row in rows:
        row["unique_control_evaluation_count"] = len(controls)
        row["unique_real_evaluation_count"] = len(real)
    return rows


def evaluate_elite_robustness_policy_pair(
    pair: EliteRobustnessPolicyPair,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    evaluations = _evaluate_unique_policies(
        (pair.real_policy, pair.matched_control_policy),
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=1,
    )
    row = _pair_result(
        pair,
        real=evaluations[pair.real_policy.policy_id],
        control=evaluations[pair.matched_control_policy.policy_id],
        control_reused=False,
    )
    row["unique_control_evaluation_count"] = 1
    row["unique_real_evaluation_count"] = 1
    return row


def _evaluate_unique_policies(
    policies: Sequence[EliteRobustnessPolicy],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
) -> dict[str, dict[str, Any]]:
    if worker_count == 1:
        rows = [
            _evaluate_policy(
                row,
                runtimes,
                starts=starts,
                episode_policy=episode_policy,
            )
            for row in policies
        ]
    else:
        global _POLICY_RUNTIMES, _POLICY_STARTS, _POLICY_EPISODE_POLICY
        _POLICY_RUNTIMES = runtimes
        _POLICY_STARTS = tuple(int(value) for value in starts)
        _POLICY_EPISODE_POLICY = episode_policy
        context = multiprocessing.get_context("fork")
        with ProcessPoolExecutor(
            max_workers=worker_count, mp_context=context
        ) as pool:
            rows = list(pool.map(_evaluate_policy_from_fork_state, policies, chunksize=2))
        _POLICY_RUNTIMES = {}
        _POLICY_STARTS = ()
        _POLICY_EPISODE_POLICY = None
    output = {str(row["policy_id"]): row for row in rows}
    if len(output) != len(policies):
        raise ValueError("elite robustness unique evaluation cache is incomplete")
    return output


def _evaluate_policy_from_fork_state(
    policy: EliteRobustnessPolicy,
) -> dict[str, Any]:
    if not _POLICY_RUNTIMES or not _POLICY_STARTS or _POLICY_EPISODE_POLICY is None:
        raise RuntimeError("elite robustness worker has no frozen fork state")
    return _evaluate_policy(
        policy,
        _POLICY_RUNTIMES,
        starts=_POLICY_STARTS,
        episode_policy=_POLICY_EPISODE_POLICY,
    )


def _evaluate_policy(
    policy: EliteRobustnessPolicy,
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
        raise ValueError("elite robustness policy has no common session days")
    events = {row.sleeve_id: row.events for row in selected}
    stressed = {
        component_id: tuple(
            _restress_routed_trade(row, cost_stress=1.5) for row in values
        )
        for component_id, values in events.items()
    }
    basket = ComplementarySleeveBasketPolicy(
        policy_id=policy.basket_policy_id,
        component_ids=policy.component_ids,
        archetype="GREEN_0018_ELITE_ROBUSTNESS_EVOLUTION",
        maximum_simultaneous_positions=policy.maximum_simultaneous_positions,
        maximum_mini_equivalent=policy.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=policy.component_ids,
        policy_version=ELITE_ROBUSTNESS_POLICY_VERSION,
    )
    with _patched_elite_robustness_router():
        normal = basket_engine.evaluate_account_policy(
            events,
            days,
            basket=basket,  # type: ignore[arg-type]
            controller=policy,  # type: ignore[arg-type]
            episode_policy=episode_policy,
            explicit_start_days=starts,
        )
    with _patched_elite_robustness_router():
        stress = basket_engine.evaluate_account_policy(
            stressed,
            days,
            basket=basket,  # type: ignore[arg-type]
            controller=policy,  # type: ignore[arg-type]
            episode_policy=episode_policy,
            explicit_start_days=normal.episode_start_days,
        )
    return {
        "policy_id": policy.policy_id,
        "episode_start_days": list(normal.episode_start_days),
        "normal": normal,
        "stress": stress,
    }


def _pair_result(
    pair: EliteRobustnessPolicyPair,
    *,
    real: Mapping[str, Any],
    control: Mapping[str, Any],
    control_reused: bool,
) -> dict[str, Any]:
    if real["episode_start_days"] != control["episode_start_days"]:
        raise ValueError("elite robustness pair used different episode starts")
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
        "control_cache_key": _control_cache_key(
            pair, starts=real["episode_start_days"]
        ),
        "control_cache_hit": control_reused,
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
        "execution_policy_version": ELITE_ROBUSTNESS_EVALUATION_VERSION,
        "development_only": True,
        "validated": False,
        "proof_window_consumed": False,
        "new_data_purchase_count": 0,
        "q4_access_delta": 0,
        "orders": 0,
    }


@contextmanager
def _patched_elite_robustness_router() -> Iterator[None]:
    def route_robustness(
        intent: Any, state: Any, *, policy: EliteRobustnessPolicy
    ) -> Any:
        return route_elite_robustness_entry(intent, state, policy=policy)

    with _ROUTER_PATCH_LOCK:
        prior = basket_engine.route_entry
        basket_engine.route_entry = route_robustness  # type: ignore[assignment]
        try:
            yield
        finally:
            basket_engine.route_entry = prior


def _temporal_blocks(
    episodes: Sequence[Any], *, count: int
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


def _control_cache_key(
    pair: EliteRobustnessPolicyPair, *, starts: Sequence[int]
) -> str:
    return stable_hash(
        {
            "parent_policy_id": pair.parent_policy_id,
            "parent_policy_fingerprint": (
                pair.matched_control_policy.parent_policy_fingerprint
            ),
            "membership": list(pair.matched_control_policy.component_ids),
            "starts": [int(value) for value in starts],
            "execution": ELITE_ROBUSTNESS_EVALUATION_VERSION,
            "costs": [1.0, 1.5],
        }
    )


__all__ = [
    "ELITE_ROBUSTNESS_EVALUATION_VERSION",
    "evaluate_elite_robustness_policy_pair",
    "evaluate_elite_robustness_policy_pairs",
]
