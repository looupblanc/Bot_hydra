from __future__ import annotations

import math
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from typing import Any, Iterator, Mapping, Sequence

import numpy as np

import hydra.account_policy.basket as basket_engine
import hydra.economic_evolution.account_coverage_sizing_evaluation as sizing_eval
from hydra.account_policy.basket import RoutedTrade
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    ComplementarySleeveBasketPolicy,
)
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    UnsupportedExactExecution,
    _restress_routed_trade,
    _scale_routed_trade,
    build_exact_sleeve_runtime,
)
from hydra.economic_evolution.account_partial_runner import (
    MATCHED_CONTROL_EXIT,
    PARTIAL_RUNNER_EXIT,
    PartialRunnerPolicy,
    PartialRunnerPolicyPair,
    TARGET_VOLATILITY_MULTIPLE,
    TOTAL_QUANTITY,
)
from hydra.economic_evolution.account_coverage_three_zone import (
    route_coverage_three_zone_entry,
)
from hydra.economic_evolution.schema import EconomicRole, stable_hash
from hydra.economic_evolution.screen import BoundSleeve
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy
from hydra.propfirm.scaling_plan import mini_equivalent
from hydra.research.rolling_combine_replay import _signal_positions


PARTIAL_RUNNER_POLICY_VERSION = "hydra_partial_runner_policy_v1"
_PAIR_RUNTIMES: Mapping[str, "PartialRunnerExactRuntime"] = {}
_PAIR_STARTS: tuple[int, ...] = ()
_PAIR_EPISODE_POLICY: EpisodeStartPolicy | None = None
_ROUTER_BIND_LOCK = threading.RLock()


@dataclass(frozen=True, slots=True)
class PartialRunnerExactRuntime:
    sleeve_id: str
    signal_market: str
    execution_market: str
    role: EconomicRole
    source_campaign: str
    specification_hash: str
    eligible_session_days: tuple[int, ...]
    events: tuple[RoutedTrade, ...]
    control_events: tuple[RoutedTrade, ...]
    partial_runner_events: tuple[RoutedTrade, ...]
    event_count: int
    net_pnl: float
    cost_stress_1_5x_net: float
    maximum_drawdown: float
    best_positive_event_share: float
    partial_runner_net_pnl: float
    partial_runner_cost_stress_1_5x_net: float
    target_hit_count: int
    exit_implementation: str = "CAMPAIGN_LOCAL_EXACT_PARTIAL_RUNNER"

    def __post_init__(self) -> None:
        if self.event_count != len(self.events):
            raise ValueError("partial-runner base event count drift")
        if len(self.control_events) != self.event_count:
            raise ValueError("partial-runner control event count drift")
        if len(self.partial_runner_events) != self.event_count:
            raise ValueError("partial-runner real event count drift")
        for values in (self.events, self.control_events, self.partial_runner_events):
            if any(row.component_id != self.sleeve_id for row in values):
                raise ValueError("partial-runner events lost sleeve identity")

    def to_dict(self, *, include_events: bool = False) -> dict[str, Any]:
        row = asdict(self)
        row["role"] = self.role.value
        row["eligible_session_days"] = list(self.eligible_session_days)
        if include_events:
            for key in ("events", "control_events", "partial_runner_events"):
                row[key] = [value.to_dict() for value in getattr(self, key)]
        else:
            row.pop("events", None)
            row.pop("control_events", None)
            row.pop("partial_runner_events", None)
        return row


def build_partial_runner_exact_runtime(
    bound: BoundSleeve,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str,
    end_exclusive: str,
) -> PartialRunnerExactRuntime:
    base = build_exact_sleeve_runtime(
        bound,
        matrix,
        start_inclusive=start_inclusive,
        end_exclusive=end_exclusive,
    )
    positions = _signal_positions(
        bound.strategy,
        matrix,
        start_inclusive=start_inclusive,
        end_exclusive=end_exclusive,
    )
    if len(positions) != base.event_count:
        raise ValueError("partial-runner signal path differs from exact parent")
    control = tuple(
        _scale_routed_trade(row, units=TOTAL_QUANTITY, cost_stress=1.0)
        for row in base.events
    )
    partial: list[RoutedTrade] = []
    hits = 0
    entry_prices = matrix.array("entry_price")
    highs = matrix.array("bar_high")
    lows = matrix.array("bar_low")
    segments = matrix.array("segment_code")
    volatility = matrix.array("feature__past_volatility")
    spec = instrument_spec(bound.sleeve.execution_market)
    holding = int(bound.strategy.holding_events)
    for raw_position, base_trade in zip(positions, base.events, strict=True):
        position = int(raw_position)
        entry_index = position + 1
        exit_index = position + holding + 1
        if int(segments[entry_index]) != int(segments[exit_index]):
            raise ValueError("partial runner crosses a contract/session segment")
        vol = float(volatility[position])
        if not math.isfinite(vol) or vol <= 0.0:
            raise ValueError("partial runner requires positive past-only volatility")
        entry = float(entry_prices[position])
        raw_distance = (
            abs(entry)
            * vol
            * math.sqrt(float(holding))
            * TARGET_VOLATILITY_MULTIPLE
        )
        target_distance = max(
            spec.tick_size,
            math.ceil(raw_distance / spec.tick_size) * spec.tick_size,
        )
        path_highs = np.asarray(highs[entry_index : exit_index + 1], dtype=float)
        path_lows = np.asarray(lows[entry_index : exit_index + 1], dtype=float)
        target_price = entry + bound.sleeve.side * target_distance
        hit_offsets = np.flatnonzero(
            path_highs >= target_price
            if bound.sleeve.side > 0
            else path_lows <= target_price
        )
        hit_offset = int(hit_offsets[0]) if len(hit_offsets) else None
        hits += int(hit_offset is not None)
        partial.append(
            _partial_runner_event_from_parent(
                base_trade,
                entry_price=entry,
                side=bound.sleeve.side,
                point_value=spec.point_value,
                target_distance=target_distance,
                path_highs=path_highs,
                path_lows=path_lows,
                target_hit_offset=hit_offset,
            )
        )
    partial_events = tuple(partial)
    partial_net = np.asarray(
        [row.event.net_pnl for row in partial_events], dtype=float
    )
    partial_gross = np.asarray(
        [row.event.gross_pnl for row in partial_events], dtype=float
    )
    return PartialRunnerExactRuntime(
        sleeve_id=base.sleeve_id,
        signal_market=base.signal_market,
        execution_market=base.execution_market,
        role=base.role,
        source_campaign=base.source_campaign,
        specification_hash=stable_hash(
            {
                "parent": base.specification_hash,
                "exit": PARTIAL_RUNNER_EXIT,
                "target_volatility_multiple": TARGET_VOLATILITY_MULTIPLE,
                "total_quantity": TOTAL_QUANTITY,
            }
        ),
        eligible_session_days=base.eligible_session_days,
        events=base.events,
        control_events=control,
        partial_runner_events=partial_events,
        event_count=base.event_count,
        net_pnl=base.net_pnl,
        cost_stress_1_5x_net=base.cost_stress_1_5x_net,
        maximum_drawdown=base.maximum_drawdown,
        best_positive_event_share=base.best_positive_event_share,
        partial_runner_net_pnl=float(partial_net.sum()),
        partial_runner_cost_stress_1_5x_net=float(
            (partial_gross - 1.5 * (partial_gross - partial_net)).sum()
        ),
        target_hit_count=hits,
    )


def _partial_runner_event_from_parent(
    parent: RoutedTrade,
    *,
    entry_price: float,
    side: int,
    point_value: float,
    target_distance: float,
    path_highs: np.ndarray,
    path_lows: np.ndarray,
    target_hit_offset: int | None,
) -> RoutedTrade:
    if side not in {-1, 1}:
        raise ValueError("partial runner side must be directional")
    if target_distance <= 0.0 or point_value <= 0.0:
        raise ValueError("partial runner target economics must be positive")
    if not len(path_highs) or len(path_highs) != len(path_lows):
        raise ValueError("partial runner path is incomplete")
    event = parent.event
    per_contract_cost = max(0.0, event.gross_pnl - event.net_pnl)
    if target_hit_offset is None:
        gross = float(event.gross_pnl * TOTAL_QUANTITY)
        worst_gross = float(event.worst_unrealized_pnl + per_contract_cost)
        worst_gross *= TOTAL_QUANTITY
        best_gross = float(event.best_unrealized_pnl + per_contract_cost)
        best_gross *= TOTAL_QUANTITY
        ambiguous = False
    else:
        if not 0 <= target_hit_offset < len(path_highs):
            raise ValueError("partial runner target offset escapes the path")
        target_gross = target_distance * point_value
        runner_gross = float(event.gross_pnl)
        gross = float(target_gross + runner_gross)
        pre_highs = path_highs[: target_hit_offset + 1]
        pre_lows = path_lows[: target_hit_offset + 1]
        post_highs = path_highs[target_hit_offset:]
        post_lows = path_lows[target_hit_offset:]
        if side > 0:
            pre_adverse = float(np.min(pre_lows))
            pre_favorable = float(np.max(pre_highs))
            post_adverse = float(np.min(post_lows))
            post_favorable = float(np.max(post_highs))
            ambiguous = bool(
                path_lows[target_hit_offset] < entry_price
                and path_highs[target_hit_offset] >= entry_price + target_distance
            )
        else:
            pre_adverse = float(np.max(pre_highs))
            pre_favorable = float(np.min(pre_lows))
            post_adverse = float(np.max(post_highs))
            post_favorable = float(np.min(post_lows))
            ambiguous = bool(
                path_highs[target_hit_offset] > entry_price
                and path_lows[target_hit_offset] <= entry_price - target_distance
            )
        pre_worst = (
            (pre_adverse - entry_price) * side * point_value * TOTAL_QUANTITY
        )
        post_worst = target_gross + (
            (post_adverse - entry_price) * side * point_value
        )
        pre_best = (
            (pre_favorable - entry_price) * side * point_value * TOTAL_QUANTITY
        )
        post_best = target_gross + (
            (post_favorable - entry_price) * side * point_value
        )
        worst_gross = float(min(0.0, pre_worst, post_worst))
        best_gross = float(max(0.0, pre_best, post_best))
    total_cost = per_contract_cost * TOTAL_QUANTITY
    updated = replace(
        event,
        event_id=f"{event.event_id}:partial_runner_v1",
        net_pnl=float(gross - total_cost),
        gross_pnl=float(gross),
        worst_unrealized_pnl=float(worst_gross - total_cost),
        best_unrealized_pnl=float(best_gross - total_cost),
        quantity=TOTAL_QUANTITY,
        mini_equivalent=mini_equivalent(parent.market, TOTAL_QUANTITY),
        contract_limit_compliant=bool(
            mini_equivalent(parent.market, TOTAL_QUANTITY) <= 15.0
        ),
        same_bar_ambiguous=bool(event.same_bar_ambiguous or ambiguous),
    )
    return replace(parent, event=updated)


def build_partial_runner_exact_runtimes(
    bound: Mapping[str, BoundSleeve],
    matrices: Mapping[str, FeatureMatrix],
    *,
    start_inclusive: str,
    end_exclusive: str,
    worker_count: int,
) -> tuple[dict[str, PartialRunnerExactRuntime], list[dict[str, str]]]:
    output: dict[str, PartialRunnerExactRuntime] = {}
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(
                build_partial_runner_exact_runtime,
                row,
                matrices[row.sleeve.market],
                start_inclusive=start_inclusive,
                end_exclusive=end_exclusive,
            ): sleeve_id
            for sleeve_id, row in bound.items()
        }
        for future in as_completed(futures):
            sleeve_id = futures[future]
            try:
                output[sleeve_id] = future.result()
            except (ValueError, UnsupportedExactExecution) as exc:
                failures.append(
                    {
                        "sleeve_id": sleeve_id,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
    return dict(sorted(output.items())), sorted(
        failures, key=lambda row: row["sleeve_id"]
    )


def evaluate_partial_runner_policy_pairs(
    pairs: Sequence[PartialRunnerPolicyPair],
    runtimes: Mapping[str, PartialRunnerExactRuntime],
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
        raise ValueError("duplicate partial-runner controls must be cached upstream")
    if worker_count == 1:
        return [
            evaluate_partial_runner_policy_pair(
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


def _evaluate_pair_from_fork_state(pair: PartialRunnerPolicyPair) -> dict[str, Any]:
    if not _PAIR_RUNTIMES or not _PAIR_STARTS or _PAIR_EPISODE_POLICY is None:
        raise RuntimeError("partial-runner worker has no frozen fork state")
    return evaluate_partial_runner_policy_pair(
        pair,
        _PAIR_RUNTIMES,
        starts=_PAIR_STARTS,
        episode_policy=_PAIR_EPISODE_POLICY,
    )


def evaluate_partial_runner_policy_pair(
    pair: PartialRunnerPolicyPair,
    runtimes: Mapping[str, PartialRunnerExactRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    with _bound_partial_runner_evaluator():
        row = sizing_eval.evaluate_coverage_sizing_policy_pair(  # type: ignore[arg-type]
            pair,
            runtimes,  # type: ignore[arg-type]
            starts=starts,
            episode_policy=episode_policy,
        )
    row["control_cache_key"] = _control_cache_key(
        pair,
        starts=row["real_evaluation"]["episode_start_days"],
    )
    row["control_cache_hit"] = False
    row["execution_policy_version"] = PARTIAL_RUNNER_POLICY_VERSION
    return row


def _evaluate_partial_runner_policy(
    policy: PartialRunnerPolicy,
    runtimes: Mapping[str, PartialRunnerExactRuntime],
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
        raise ValueError("partial-runner policy has no common session days")
    events: dict[str, tuple[RoutedTrade, ...]] = {}
    for runtime in selected:
        if runtime.sleeve_id != policy.mutated_sleeve_id:
            events[runtime.sleeve_id] = runtime.events
        elif policy.exit_representation == PARTIAL_RUNNER_EXIT:
            events[runtime.sleeve_id] = runtime.partial_runner_events
        elif policy.exit_representation == MATCHED_CONTROL_EXIT:
            events[runtime.sleeve_id] = runtime.control_events
        else:
            raise ValueError("unregistered partial-runner exit representation")
    stressed = {
        component_id: tuple(
            _restress_routed_trade(row, cost_stress=1.5) for row in values
        )
        for component_id, values in events.items()
    }
    basket = ComplementarySleeveBasketPolicy(
        policy_id=policy.basket_policy_id,
        component_ids=policy.component_ids,
        archetype="GREEN_COMPLEMENTARY_SLEEVE_PARTIAL_RUNNER",
        maximum_simultaneous_positions=policy.maximum_simultaneous_positions,
        maximum_mini_equivalent=policy.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=policy.component_ids,
        policy_version=PARTIAL_RUNNER_POLICY_VERSION,
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
    with _patched_account_router():
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
def _bound_partial_runner_evaluator() -> Iterator[None]:
    with _ROUTER_BIND_LOCK:
        prior_policy = sizing_eval._evaluate_policy
        sizing_eval._evaluate_policy = _evaluate_partial_runner_policy  # type: ignore[assignment]
        try:
            yield
        finally:
            sizing_eval._evaluate_policy = prior_policy


@contextmanager
def _patched_account_router() -> Iterator[None]:
    def route_partial_runner(intent: Any, state: Any, *, policy: Any) -> Any:
        return route_coverage_three_zone_entry(intent, state, policy=policy)

    prior = basket_engine.route_entry
    basket_engine.route_entry = route_partial_runner  # type: ignore[assignment]
    try:
        yield
    finally:
        basket_engine.route_entry = prior


def _control_cache_key(
    pair: PartialRunnerPolicyPair,
    *,
    starts: Sequence[int],
) -> str:
    return stable_hash(
        {
            "parent_policy_id": pair.parent_policy_id,
            "membership": list(pair.matched_control_policy.component_ids),
            "mutated_sleeve_id": pair.mutated_sleeve_id,
            "control_exit": MATCHED_CONTROL_EXIT,
            "total_quantity": TOTAL_QUANTITY,
            "starts": [int(value) for value in starts],
            "execution": PARTIAL_RUNNER_POLICY_VERSION,
            "costs": [1.0, 1.5],
        }
    )


__all__ = [
    "PARTIAL_RUNNER_POLICY_VERSION",
    "PartialRunnerExactRuntime",
    "build_partial_runner_exact_runtime",
    "build_partial_runner_exact_runtimes",
    "evaluate_partial_runner_policy_pair",
    "evaluate_partial_runner_policy_pairs",
]
