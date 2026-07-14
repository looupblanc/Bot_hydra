from __future__ import annotations

import multiprocessing
import statistics
import threading
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Iterator, Mapping, Sequence

import hydra.account_policy.basket as basket_engine
from hydra.account_policy.router import (
    AccountDecisionState,
    EntryIntent,
    RoutingDecision,
)
from hydra.account_policy.schema import AccountPolicyKind
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    _restress_routed_trade,
)
from hydra.economic_evolution.role_aware_account_evaluation import (
    RoleAwareBasketPolicy,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


ACCOUNT_STATE_POLICY_VERSION = "hydra_account_state_router_v1"
_PAIR_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_PAIR_STARTS: tuple[int, ...] = ()
_PAIR_EPISODE_POLICY: EpisodeStartPolicy | None = None
_ROUTER_PATCH_LOCK = threading.RLock()


class AccountStateMode(StrEnum):
    ACCELERATE = "ACCELERATE"
    BALANCED = "BALANCED"
    PROTECT = "PROTECT"
    LOCKED = "LOCKED"


@dataclass(frozen=True, slots=True)
class AccountStateRoutingPolicy:
    policy_id: str
    component_ids: tuple[str, ...]
    component_roles: tuple[tuple[str, str], ...]
    daily_loss_guard: float
    daily_profit_lock: float
    critical_buffer: float
    protect_buffer: float
    accelerate_buffer: float
    accelerate_remaining_target: float
    loss_streak_protect_after: int
    balanced_maximum_positions: int
    accelerate_maximum_positions: int
    protect_maximum_positions: int
    accelerate_risk_units: int
    maximum_mini_equivalent: int
    policy_version: str = ACCOUNT_STATE_POLICY_VERSION
    outbound_order_capability: bool = False

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("account-state policy ID is required")
        if not 6 <= len(self.component_ids) <= 8:
            raise ValueError("account-state policy requires six to eight sleeves")
        if len(set(self.component_ids)) != len(self.component_ids):
            raise ValueError("account-state component IDs must be unique")
        roles = dict(self.component_roles)
        if set(roles) != set(self.component_ids):
            raise ValueError("account-state roles must map every component once")
        if any(not value for value in roles.values()):
            raise ValueError("account-state component roles must be non-empty")
        if not 0.0 < self.daily_loss_guard <= 3_000.0:
            raise ValueError("daily loss guard is outside frozen bounds")
        if not 0.0 < self.daily_profit_lock <= 4_500.0:
            raise ValueError("daily profit lock is outside frozen bounds")
        if not (
            0.0
            < self.critical_buffer
            < self.protect_buffer
            < self.accelerate_buffer
            <= 4_500.0
        ):
            raise ValueError("account-state buffer thresholds are not ordered")
        if not 0.0 < self.accelerate_remaining_target <= 9_000.0:
            raise ValueError("remaining-target threshold is outside bounds")
        if self.loss_streak_protect_after not in {1, 2, 3}:
            raise ValueError("loss-streak guard is outside bounded set")
        if not (
            1
            <= self.protect_maximum_positions
            <= self.balanced_maximum_positions
            <= self.accelerate_maximum_positions
            <= 3
        ):
            raise ValueError("state concurrency limits are inconsistent")
        if self.accelerate_risk_units not in {1, 2}:
            raise ValueError("acceleration uses one or two integer units")
        if not 1 <= self.maximum_mini_equivalent <= 15:
            raise ValueError("shared contract limit must be in [1,15]")
        if self.outbound_order_capability:
            raise ValueError("account-state research cannot submit orders")

    @property
    def component_role_map(self) -> dict[str, str]:
        return dict(self.component_roles)

    @property
    def component_priority(self) -> tuple[str, ...]:
        return self.component_ids

    @property
    def controller_id(self) -> str:
        return self.policy_id

    @property
    def basket_policy_id(self) -> str:
        return f"{self.policy_id}::BASKET"

    @property
    def kind(self) -> AccountPolicyKind:
        return AccountPolicyKind.ADAPTIVE_CONTROLLER

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["component_ids"] = list(self.component_ids)
        row["component_roles"] = dict(self.component_roles)
        row["kind"] = self.kind.value
        row["structural_fingerprint"] = stable_hash(
            {key: value for key, value in row.items() if key != "structural_fingerprint"}
        )
        return row


@dataclass(frozen=True, slots=True)
class AccountStatePolicyPair:
    pair_id: str
    real_policy: AccountStateRoutingPolicy
    matched_control_policy: AccountStateRoutingPolicy
    membership_hash: str

    def __post_init__(self) -> None:
        real = self.real_policy
        control = self.matched_control_policy
        if real.component_ids != control.component_ids:
            raise ValueError("state-router pair must keep ordered membership")
        if sorted(real.component_role_map.values()) != sorted(
            control.component_role_map.values()
        ):
            raise ValueError("state-router pair must keep the role multiset")
        if real.component_roles == control.component_roles:
            raise ValueError("matched state-router control must permute roles")
        if _policy_limits(real) != _policy_limits(control):
            raise ValueError("state-router pair must keep all state limits")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "membership_hash": self.membership_hash,
            "real_policy_id": self.real_policy.policy_id,
            "matched_control_policy_id": self.matched_control_policy.policy_id,
            "identical_sleeve_membership": True,
            "identical_component_event_paths": True,
            "same_state_thresholds": True,
            "same_action_and_risk_multisets": True,
            "same_account_limits": True,
            "real_component_roles": dict(self.real_policy.component_roles),
            "matched_control_component_roles": dict(
                self.matched_control_policy.component_roles
            ),
        }


def account_state_mode(
    policy: AccountStateRoutingPolicy,
    state: AccountDecisionState,
) -> AccountStateMode:
    if (
        state.daily_realized_pnl <= -policy.daily_loss_guard
        or state.daily_realized_pnl >= policy.daily_profit_lock
        or state.mll_buffer <= policy.critical_buffer
    ):
        return AccountStateMode.LOCKED
    if (
        state.mll_buffer <= policy.protect_buffer
        or state.consecutive_losing_days >= policy.loss_streak_protect_after
    ):
        return AccountStateMode.PROTECT
    if (
        state.mll_buffer >= policy.accelerate_buffer
        and state.remaining_target >= policy.accelerate_remaining_target
    ):
        return AccountStateMode.ACCELERATE
    return AccountStateMode.BALANCED


def route_account_state_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: AccountStateRoutingPolicy,
) -> RoutingDecision:
    roles = policy.component_role_map
    if intent.component_id not in roles:
        return _blocked(policy, "COMPONENT_NOT_IN_FROZEN_MEMBERSHIP")
    mode = account_state_mode(policy, state)
    if mode is AccountStateMode.LOCKED:
        return _blocked(policy, "ACCOUNT_STATE_LOCKED")
    role = roles[intent.component_id]
    if mode is AccountStateMode.PROTECT and role not in {
        "MLL_STABILIZER",
        "CONSISTENCY_SMOOTHER",
        "MARKET_DIVERSIFIER",
        "SESSION_DIVERSIFIER",
    }:
        return _blocked(policy, "PROTECT_MODE_ROLE_VETO")
    limit = {
        AccountStateMode.ACCELERATE: policy.accelerate_maximum_positions,
        AccountStateMode.BALANCED: policy.balanced_maximum_positions,
        AccountStateMode.PROTECT: policy.protect_maximum_positions,
    }[mode]
    if len(state.open_exposures) >= limit:
        return _blocked(policy, f"{mode.value}_MAXIMUM_POSITIONS")
    if any(
        row.market == intent.market and row.exit_ns > intent.decision_ns
        for row in state.open_exposures
    ):
        return _blocked(policy, "SAME_MARKET_CONFLICT")
    units = 1
    if mode is AccountStateMode.ACCELERATE and role in {
        "PRIMARY_ALPHA",
        "SESSION_DIVERSIFIER",
        "MARKET_DIVERSIFIER",
    }:
        units = policy.accelerate_risk_units
    quantity = int(intent.base_quantity * units)
    mini = float(intent.base_mini_equivalent * units)
    current = sum(row.mini_equivalent for row in state.open_exposures)
    if current + mini > policy.maximum_mini_equivalent + 1e-12:
        return _blocked(policy, "SHARED_CONTRACT_LIMIT")
    return RoutingDecision(
        allow=True,
        quantity=quantity,
        mini_equivalent=mini,
        reason=f"{mode.value}_ROLE_{role}_UNITS_{units}",
        policy_id=policy.policy_id,
    )


def evaluate_account_state_policy_pairs(
    pairs: Sequence[AccountStatePolicyPair],
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
            evaluate_account_state_policy_pair(
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


def _evaluate_pair_from_fork_state(pair: AccountStatePolicyPair) -> dict[str, Any]:
    if not _PAIR_RUNTIMES or not _PAIR_STARTS or _PAIR_EPISODE_POLICY is None:
        raise RuntimeError("account-state worker has no frozen fork state")
    return evaluate_account_state_policy_pair(
        pair,
        _PAIR_RUNTIMES,
        starts=_PAIR_STARTS,
        episode_policy=_PAIR_EPISODE_POLICY,
    )


def evaluate_account_state_policy_pair(
    pair: AccountStatePolicyPair,
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
        raise ValueError("state-router pair used different episode starts")
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
    policy: AccountStateRoutingPolicy,
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
        raise ValueError("account-state policy has no common session days")
    events = {row.sleeve_id: row.events for row in selected}
    stressed = {
        component_id: tuple(
            _restress_routed_trade(row, cost_stress=1.5) for row in values
        )
        for component_id, values in events.items()
    }
    basket = RoleAwareBasketPolicy(
        policy_id=policy.basket_policy_id,
        component_ids=policy.component_ids,
        archetype="ACCOUNT_STATE_CONDITIONAL_ROUTER",
        maximum_simultaneous_positions=policy.accelerate_maximum_positions,
        maximum_mini_equivalent=policy.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=policy.component_ids,
        policy_version=ACCOUNT_STATE_POLICY_VERSION,
    )
    with _patched_account_router():
        normal = basket_engine.evaluate_account_policy(
            events,
            days,
            basket=basket,  # type: ignore[arg-type]
            controller=policy,  # type: ignore[arg-type]
            episode_policy=episode_policy,
            explicit_start_days=starts,
        )
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
def _patched_account_router() -> Iterator[None]:
    with _ROUTER_PATCH_LOCK:
        prior = basket_engine.route_entry
        basket_engine.route_entry = route_account_state_entry  # type: ignore[assignment]
        try:
            yield
        finally:
            basket_engine.route_entry = prior


def _blocked(
    policy: AccountStateRoutingPolicy,
    reason: str,
) -> RoutingDecision:
    return RoutingDecision(False, 0, 0.0, reason, policy.policy_id)


def _policy_limits(policy: AccountStateRoutingPolicy) -> tuple[Any, ...]:
    return (
        policy.daily_loss_guard,
        policy.daily_profit_lock,
        policy.critical_buffer,
        policy.protect_buffer,
        policy.accelerate_buffer,
        policy.accelerate_remaining_target,
        policy.loss_streak_protect_after,
        policy.balanced_maximum_positions,
        policy.accelerate_maximum_positions,
        policy.protect_maximum_positions,
        policy.accelerate_risk_units,
        policy.maximum_mini_equivalent,
        policy.policy_version,
    )


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
    "ACCOUNT_STATE_POLICY_VERSION",
    "AccountStateMode",
    "AccountStatePolicyPair",
    "AccountStateRoutingPolicy",
    "account_state_mode",
    "evaluate_account_state_policy_pair",
    "evaluate_account_state_policy_pairs",
    "route_account_state_entry",
]
