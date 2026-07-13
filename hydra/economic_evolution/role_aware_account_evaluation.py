from __future__ import annotations

import multiprocessing
import statistics
from dataclasses import asdict, dataclass
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Mapping, Sequence

from hydra.account_policy.schema import AccountPolicyKind, ControllerPolicy
from hydra.economic_evolution.account_evaluation import (
    CompiledAccountPolicy,
    ExactSleeveRuntime,
    _scale_routed_trade,
    evaluate_compiled_account_policy,
)
from hydra.economic_evolution.role_aware_account import (
    RoleAwareAccountPolicyGenome,
    RoleAwarePolicyPair,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


ROLE_AWARE_POLICY_VERSION = "hydra_role_aware_account_policy_v1"
_PAIR_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_PAIR_STARTS: tuple[int, ...] = ()
_PAIR_EPISODE_POLICY: EpisodeStartPolicy | None = None


@dataclass(frozen=True, slots=True)
class RoleAwareBasketPolicy:
    """Eight-sleeve basket adapter local to campaign 0010.

    Legacy ``BasketPolicy`` remains unchanged so every historical WORM hash and
    replay stays valid.  The shared-account engine consumes this audited,
    structurally equivalent interface by duck typing.
    """

    policy_id: str
    component_ids: tuple[str, ...]
    archetype: str
    maximum_simultaneous_positions: int
    maximum_mini_equivalent: int
    conflict_policy: str
    component_priority: tuple[str, ...]
    policy_version: str = ROLE_AWARE_POLICY_VERSION

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("role-aware basket ID must be non-empty")
        if not 6 <= len(self.component_ids) <= 8:
            raise ValueError("role-aware basket must contain six to eight components")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("role-aware basket components must be unique")
        if self.component_priority != self.component_ids:
            raise ValueError("role-aware priority must be frozen in component order")
        if not 1 <= self.maximum_simultaneous_positions <= len(self.component_ids):
            raise ValueError("maximum simultaneous positions is inconsistent")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("maximum mini equivalent must be in [1,15]")
        if self.conflict_policy != "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE":
            raise ValueError("role-aware basket requires deterministic priority")

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


def compile_role_aware_account_policy(
    genome: RoleAwareAccountPolicyGenome,
    runtimes: Mapping[str, ExactSleeveRuntime],
) -> CompiledAccountPolicy:
    missing = [value for value in genome.sleeve_ids if value not in runtimes]
    if missing:
        raise ValueError(f"policy references missing runtimes: {missing}")
    selected = [runtimes[value] for value in genome.sleeve_ids]
    common_days = set(selected[0].eligible_session_days)
    for runtime in selected[1:]:
        common_days.intersection_update(runtime.eligible_session_days)
    eligible_days = tuple(sorted(common_days))
    if not eligible_days:
        raise ValueError("role-aware policy has no common chronological days")
    scaled = {
        sleeve_id: tuple(
            _scale_routed_trade(row, units=units, cost_stress=1.0)
            for row in runtime.events
        )
        for sleeve_id, units, runtime in zip(
            genome.sleeve_ids, genome.allocation_units, selected, strict=True
        )
    }
    basket = RoleAwareBasketPolicy(
        policy_id=f"{genome.policy_id}::STATIC",
        component_ids=genome.sleeve_ids,
        archetype="ROLE_AWARE_SAME_MEMBERSHIP_ALLOCATION",
        maximum_simultaneous_positions=genome.maximum_simultaneous_positions,
        maximum_mini_equivalent=genome.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=genome.sleeve_ids,
    )
    controller = ControllerPolicy(
        controller_id=genome.policy_id,
        basket_policy_id=basket.policy_id,
        component_priority=genome.sleeve_ids,
        daily_loss_limit=genome.daily_risk_budget,
        daily_profit_lock=genome.daily_profit_lock,
        loss_streak_derisk_after=genome.loss_streak_throttle_after,
        low_buffer_threshold=genome.low_mll_buffer,
        critical_buffer_threshold=genome.critical_mll_buffer,
        maximum_simultaneous_positions=genome.maximum_simultaneous_positions,
        maximum_mini_equivalent=genome.maximum_mini_equivalent,
        routing_policy="FIXED_PRIORITY_PAST_ONLY",
        policy_version=ROLE_AWARE_POLICY_VERSION,
    )
    return CompiledAccountPolicy(
        genome=genome,  # type: ignore[arg-type]
        basket=basket,  # type: ignore[arg-type]
        controller=controller,
        component_events=scaled,
        eligible_session_days=eligible_days,
        source_runtime_hashes={
            sleeve_id: runtime.specification_hash
            for sleeve_id, runtime in zip(
                genome.sleeve_ids, selected, strict=True
            )
        },
    )


def evaluate_role_aware_policy_pairs(
    pairs: Sequence[RoleAwarePolicyPair],
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
            evaluate_role_aware_policy_pair(
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


def _evaluate_pair_from_fork_state(pair: RoleAwarePolicyPair) -> dict[str, Any]:
    if not _PAIR_RUNTIMES or _PAIR_EPISODE_POLICY is None or not _PAIR_STARTS:
        raise RuntimeError("role-aware worker has no frozen fork state")
    return evaluate_role_aware_policy_pair(
        pair,
        _PAIR_RUNTIMES,
        starts=_PAIR_STARTS,
        episode_policy=_PAIR_EPISODE_POLICY,
    )


def evaluate_role_aware_policy_pair(
    pair: RoleAwarePolicyPair,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    real = evaluate_compiled_account_policy(
        compile_role_aware_account_policy(pair.real_policy, runtimes),
        episode_policy=episode_policy,
        explicit_start_days=starts,
        evaluate_xfa=False,
    )
    control = evaluate_compiled_account_policy(
        compile_role_aware_account_policy(pair.matched_control_policy, runtimes),
        episode_policy=episode_policy,
        explicit_start_days=real.episode_start_days,
        evaluate_xfa=False,
    )
    if real.episode_start_days != control.episode_start_days:
        raise ValueError("role-aware pair did not use identical episode starts")
    real_normal = real.controlled_base
    real_stress = real.controlled_stress_1_5x
    control_normal = control.controlled_base
    control_stress = control.controlled_stress_1_5x
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
    positive_total = sum(contribution.values())
    maximum_share = (
        max(contribution.values(), default=0.0) / positive_total
        if positive_total > 0.0
        else 1.0
    )
    behavior = stable_hash(
        {
            "starts": list(real.episode_start_days),
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
        "episode_start_count": len(real.episode_start_days),
        "behavioral_fingerprint": behavior,
        "real_evaluation": real.to_dict(include_episodes=False),
        "matched_control_evaluation": control.to_dict(include_episodes=False),
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


def _temporal_blocks(episodes: Sequence[Any], *, count: int) -> list[dict[str, Any]]:
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
    "ROLE_AWARE_POLICY_VERSION",
    "RoleAwareBasketPolicy",
    "compile_role_aware_account_policy",
    "evaluate_role_aware_policy_pair",
    "evaluate_role_aware_policy_pairs",
]
