from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import (
    AccountPolicyRollingSummary,
    RoutedTrade,
    evaluate_account_policy,
)
from hydra.account_policy.schema import BasketPolicy, ControllerPolicy
from hydra.account_policy.xfa import evaluate_serial_xfa_basket
from hydra.economic_evolution.schema import AccountPolicyGenome, EconomicRole
from hydra.economic_evolution.screen import BoundSleeve
from hydra.economic_evolution.incremental_value import MatchedAccountObservation
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy
from hydra.propfirm.scaling_plan import mini_equivalent
from hydra.research.rolling_combine_replay import ExactTradePath, build_exact_trade_path


ACCOUNT_POLICY_VERSION = "hydra_economic_evolution_account_policy_v1"


class UnsupportedExactExecution(ValueError):
    """Raised when a structural declaration lacks an exact implementation."""


@dataclass(frozen=True, slots=True)
class ExactSleeveRuntime:
    sleeve_id: str
    signal_market: str
    execution_market: str
    role: EconomicRole
    source_campaign: str
    specification_hash: str
    eligible_session_days: tuple[int, ...]
    events: tuple[RoutedTrade, ...]
    event_count: int
    net_pnl: float
    cost_stress_1_5x_net: float
    maximum_drawdown: float
    best_positive_event_share: float
    exit_implementation: str

    def __post_init__(self) -> None:
        if self.event_count != len(self.events):
            raise ValueError("runtime event count does not match its event ledger")
        if self.exit_implementation != "EXACT_TIME_EXIT":
            raise UnsupportedExactExecution("runtime must use an exact exit implementation")
        if any(row.component_id != self.sleeve_id for row in self.events):
            raise ValueError("runtime events must retain their sleeve identity")

    def to_dict(self, *, include_events: bool = False) -> dict[str, Any]:
        row = asdict(self)
        row["role"] = self.role.value
        if include_events:
            row["events"] = [value.to_dict() for value in self.events]
        else:
            row.pop("events", None)
        row["eligible_session_days"] = list(self.eligible_session_days)
        return row


@dataclass(frozen=True, slots=True)
class CompiledAccountPolicy:
    genome: AccountPolicyGenome
    basket: BasketPolicy
    controller: ControllerPolicy
    component_events: dict[str, tuple[RoutedTrade, ...]]
    eligible_session_days: tuple[int, ...]
    source_runtime_hashes: dict[str, str]
    outbound_order_capability: bool = False

    def __post_init__(self) -> None:
        if self.outbound_order_capability:
            raise ValueError("economic evolution account policies cannot submit orders")
        if set(self.component_events) != set(self.genome.sleeve_ids):
            raise ValueError("compiled policy must contain every frozen sleeve exactly once")
        if not self.eligible_session_days:
            raise ValueError("compiled policy has no common chronological session days")


@dataclass(frozen=True, slots=True)
class AccountEvaluationResult:
    policy_id: str
    episode_start_days: tuple[int, ...]
    static_base: AccountPolicyRollingSummary
    controlled_base: AccountPolicyRollingSummary
    controlled_stress_1_5x: AccountPolicyRollingSummary
    xfa: dict[str, Any] | None
    exact_account_chronology: bool = True
    shared_target_and_mll: bool = True
    inherited_status: bool = False
    validated: bool = False
    outbound_order_capability: bool = False

    def __post_init__(self) -> None:
        starts = self.episode_start_days
        if any(
            summary.episode_start_days != starts
            for summary in (
                self.static_base,
                self.controlled_base,
                self.controlled_stress_1_5x,
            )
        ):
            raise ValueError("all account comparisons must use identical episode starts")
        if self.outbound_order_capability:
            raise ValueError("research evaluation cannot expose an order path")

    def to_dict(self, *, include_episodes: bool = False) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "episode_start_days": list(self.episode_start_days),
            "static_base": self.static_base.to_dict(include_episodes=include_episodes),
            "controlled_base": self.controlled_base.to_dict(
                include_episodes=include_episodes
            ),
            "controlled_stress_1_5x": self.controlled_stress_1_5x.to_dict(
                include_episodes=include_episodes
            ),
            "xfa": self.xfa,
            "exact_account_chronology": self.exact_account_chronology,
            "shared_target_and_mll": self.shared_target_and_mll,
            "inherited_status": self.inherited_status,
            "validated": self.validated,
            "outbound_order_capability": self.outbound_order_capability,
        }


def build_exact_sleeve_runtime(
    bound: BoundSleeve,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str,
    end_exclusive: str,
) -> ExactSleeveRuntime:
    """Compile one frozen sleeve into an exact, no-order trade ledger.

    Only the time exit is currently supported by the existing exact bar-path
    engine.  Stop, target and runner declarations fail closed until their
    conservative same-bar ordering is implemented and tested.
    """

    if bound.sleeve.exit_style != "TIME_ONLY":
        raise UnsupportedExactExecution(
            f"{bound.sleeve.exit_style} has no exact conservative executor"
        )
    if bound.strategy.market != bound.sleeve.execution_market:
        raise ValueError("strategy economics must use the frozen execution market")
    path = build_exact_trade_path(
        bound.strategy,
        matrix,
        start_inclusive=start_inclusive,
        end_exclusive=end_exclusive,
    )
    routed = tuple(
        RoutedTrade(
            component_id=bound.sleeve.sleeve_id,
            market=bound.sleeve.execution_market,
            side=bound.sleeve.side,
            event=replace(
                event,
                mini_equivalent=mini_equivalent(
                    bound.sleeve.execution_market, event.quantity
                ),
            ),
        )
        for event in path.events
    )
    return _runtime_from_path(bound, path, routed)


def compile_account_policy(
    genome: AccountPolicyGenome,
    runtimes: Mapping[str, ExactSleeveRuntime],
) -> CompiledAccountPolicy:
    """Compile a typed genome onto the audited shared-account simulator."""

    if genome.conflict_policy != "FIXED_PRIORITY":
        raise UnsupportedExactExecution(
            f"{genome.conflict_policy} has no preregistered exact priority compiler"
        )
    missing = [value for value in genome.sleeve_ids if value not in runtimes]
    if missing:
        raise ValueError(f"policy references missing runtimes: {missing}")
    selected = [runtimes[value] for value in genome.sleeve_ids]
    common_days = set(selected[0].eligible_session_days)
    for runtime in selected[1:]:
        common_days.intersection_update(runtime.eligible_session_days)
    eligible_days = tuple(sorted(common_days))
    scaled: dict[str, tuple[RoutedTrade, ...]] = {}
    source_hashes: dict[str, str] = {}
    for sleeve_id, units, runtime in zip(
        genome.sleeve_ids, genome.allocation_units, selected, strict=True
    ):
        scaled[sleeve_id] = tuple(
            _scale_routed_trade(row, units=units, cost_stress=1.0)
            for row in runtime.events
        )
        source_hashes[sleeve_id] = runtime.specification_hash
    basket = BasketPolicy(
        policy_id=f"{genome.policy_id}::STATIC",
        component_ids=genome.sleeve_ids,
        archetype="ECONOMIC_EVOLUTION_TYPED_ASSEMBLY",
        maximum_simultaneous_positions=genome.maximum_simultaneous_positions,
        maximum_mini_equivalent=genome.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=genome.sleeve_ids,
        policy_version=ACCOUNT_POLICY_VERSION,
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
        policy_version=ACCOUNT_POLICY_VERSION,
    )
    return CompiledAccountPolicy(
        genome=genome,
        basket=basket,
        controller=controller,
        component_events=scaled,
        eligible_session_days=eligible_days,
        source_runtime_hashes=source_hashes,
    )


def evaluate_compiled_account_policy(
    compiled: CompiledAccountPolicy,
    *,
    episode_policy: EpisodeStartPolicy,
    explicit_start_days: Sequence[int] | None = None,
    evaluate_xfa: bool = False,
) -> AccountEvaluationResult:
    """Evaluate static, controlled and stressed paths on identical starts."""

    static = evaluate_account_policy(
        compiled.component_events,
        compiled.eligible_session_days,
        basket=compiled.basket,
        episode_policy=episode_policy,
        explicit_start_days=explicit_start_days,
    )
    starts = static.episode_start_days
    controlled = evaluate_account_policy(
        compiled.component_events,
        compiled.eligible_session_days,
        basket=compiled.basket,
        controller=compiled.controller,
        episode_policy=episode_policy,
        explicit_start_days=starts,
    )
    stressed_events = {
        component_id: tuple(
            _restress_routed_trade(row, cost_stress=1.5) for row in values
        )
        for component_id, values in compiled.component_events.items()
    }
    stressed = evaluate_account_policy(
        stressed_events,
        compiled.eligible_session_days,
        basket=compiled.basket,
        controller=compiled.controller,
        episode_policy=episode_policy,
        explicit_start_days=starts,
    )
    xfa = (
        evaluate_serial_xfa_basket(
            compiled.component_events,
            compiled.eligible_session_days,
            basket=compiled.basket,
        )
        if evaluate_xfa
        else None
    )
    return AccountEvaluationResult(
        policy_id=compiled.genome.policy_id,
        episode_start_days=starts,
        static_base=static,
        controlled_base=controlled,
        controlled_stress_1_5x=stressed,
        xfa=xfa,
    )


def matched_observations_from_evaluation(
    result: AccountEvaluationResult,
    *,
    block_by_start: Mapping[int, str],
) -> tuple[MatchedAccountObservation, ...]:
    """Expose matched base/stress account rows for incremental-value tests."""

    base = {row.start_day: row for row in result.controlled_base.episodes}
    stressed = {
        row.start_day: row for row in result.controlled_stress_1_5x.episodes
    }
    starts = result.episode_start_days
    if set(starts) != set(base) or set(starts) != set(stressed):
        raise ValueError("account episode ledgers are incomplete")
    missing_blocks = [start for start in starts if start not in block_by_start]
    if missing_blocks:
        raise ValueError(f"block provenance missing for starts: {missing_blocks}")
    return tuple(
        MatchedAccountObservation(
            start_id=str(start),
            block_id=str(block_by_start[start]),
            net_after_costs=base[start].net_pnl,
            stressed_net_after_costs=stressed[start].net_pnl,
            target_progress=base[start].target_progress,
            mll_breached=base[start].mll_breached,
            consistency_ok=base[start].consistency_ok,
            shared_loss_days=base[start].shared_loss_days,
            conflict_count=base[start].conflict_count,
            total_cost=base[start].total_cost,
        )
        for start in starts
    )


def _runtime_from_path(
    bound: BoundSleeve,
    path: ExactTradePath,
    events: tuple[RoutedTrade, ...],
) -> ExactSleeveRuntime:
    return ExactSleeveRuntime(
        sleeve_id=bound.sleeve.sleeve_id,
        signal_market=bound.sleeve.market,
        execution_market=bound.sleeve.execution_market,
        role=bound.sleeve.role,
        source_campaign=bound.sleeve.source_campaign,
        specification_hash=bound.execution_fingerprint,
        eligible_session_days=path.eligible_session_days,
        events=events,
        event_count=path.event_count,
        net_pnl=path.net_pnl,
        cost_stress_1_5x_net=path.cost_stress_1_5x_net,
        maximum_drawdown=path.maximum_drawdown,
        best_positive_event_share=path.best_positive_event_share,
        exit_implementation="EXACT_TIME_EXIT",
    )


def _scale_routed_trade(
    trade: RoutedTrade, *, units: int, cost_stress: float
) -> RoutedTrade:
    if units not in {1, 2, 3, 4}:
        raise ValueError("risk units must use the frozen bounded set")
    scaled = replace(
        trade.event,
        event_id=f"{trade.event.event_id}:risk_units_{units}",
        net_pnl=float(trade.event.net_pnl * units),
        gross_pnl=float(trade.event.gross_pnl * units),
        worst_unrealized_pnl=float(trade.event.worst_unrealized_pnl * units),
        best_unrealized_pnl=float(trade.event.best_unrealized_pnl * units),
        quantity=int(trade.event.quantity * units),
        mini_equivalent=float(trade.event.mini_equivalent * units),
    )
    routed = replace(trade, event=scaled)
    return _restress_routed_trade(routed, cost_stress=cost_stress)


def _restress_routed_trade(
    trade: RoutedTrade, *, cost_stress: float
) -> RoutedTrade:
    if cost_stress < 1.0:
        raise ValueError("cost stress cannot be below the frozen base cost")
    event = trade.event
    base_cost = max(0.0, event.gross_pnl - event.net_pnl)
    extra = (cost_stress - 1.0) * base_cost
    if extra == 0.0:
        return trade
    return replace(
        trade,
        event=replace(
            event,
            event_id=f"{event.event_id}:cost_stress_{cost_stress:g}",
            net_pnl=float(event.net_pnl - extra),
            worst_unrealized_pnl=float(event.worst_unrealized_pnl - extra),
            best_unrealized_pnl=float(event.best_unrealized_pnl - extra),
        ),
    )


__all__ = [
    "ACCOUNT_POLICY_VERSION",
    "AccountEvaluationResult",
    "CompiledAccountPolicy",
    "ExactSleeveRuntime",
    "UnsupportedExactExecution",
    "build_exact_sleeve_runtime",
    "compile_account_policy",
    "evaluate_compiled_account_policy",
    "matched_observations_from_evaluation",
]
