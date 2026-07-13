from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.turbo_feature_builder import HORIZONS, feature_names_for_bundle
from hydra.strategies.turbo_batch_fingerprint import structural_fingerprint
from hydra.strategies.turbo_compiler import compile_strategy_batch
from hydra.strategies.turbo_dsl import (
    ComparisonOperator,
    StrategyRole,
    StrategySpec,
)
from hydra.strategies.turbo_vectorized_executor import (
    EventMatrix,
    execute_stage1_vectorized,
)


@dataclass(frozen=True, slots=True)
class CheapScreenPolicy:
    calibration_start: str
    calibration_end_exclusive: str
    screen_start: str
    screen_end_exclusive: str
    minimum_opportunities: int
    stress_cost_multiplier: float
    maximum_best_positive_event_share: float
    maximum_approximate_drawdown: float
    require_nonnegative_half: bool
    micro_batch_size: int = 64

    def __post_init__(self) -> None:
        if self.minimum_opportunities < 1:
            raise ValueError("minimum opportunities must be positive")
        if self.stress_cost_multiplier < 1.0:
            raise ValueError("stress cost multiplier cannot be below one")
        if not 0.0 < self.maximum_best_positive_event_share <= 1.0:
            raise ValueError("concentration threshold must be in (0,1]")
        if self.maximum_approximate_drawdown <= 0.0:
            raise ValueError("drawdown threshold must be positive")
        if self.micro_batch_size < 1:
            raise ValueError("micro batch size must be positive")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BoundSleeve:
    sleeve: SleeveSpec
    strategy: StrategySpec
    execution_fingerprint: str
    trigger_threshold: float
    context_threshold: float | None


@dataclass(frozen=True, slots=True)
class CheapScreenResult:
    policy: CheapScreenPolicy
    proposal_count: int
    bound_count: int
    unique_execution_path_count: int
    execution_cache_hit_count: int
    rows: tuple[dict[str, Any], ...]
    elapsed_seconds: float

    @property
    def survivors(self) -> tuple[dict[str, Any], ...]:
        return tuple(row for row in self.rows if row["cheap_screen_survivor"])

    @property
    def screens_per_second(self) -> float:
        return self.unique_execution_path_count / max(self.elapsed_seconds, 1e-12)

    @property
    def cache_hit_rate(self) -> float:
        return self.execution_cache_hit_count / max(self.bound_count, 1)

    def summary(self) -> dict[str, Any]:
        return {
            "proposal_count": self.proposal_count,
            "bound_count": self.bound_count,
            "unique_execution_path_count": self.unique_execution_path_count,
            "execution_cache_hit_count": self.execution_cache_hit_count,
            "execution_cache_hit_rate": self.cache_hit_rate,
            "survivor_count": len(self.survivors),
            "elapsed_seconds": self.elapsed_seconds,
            "screens_per_second": self.screens_per_second,
            "policy": self.policy.to_dict(),
        }


def run_ultra_cheap_screen(
    sleeves: Sequence[SleeveSpec],
    matrices: Mapping[str, FeatureMatrix],
    *,
    policy: CheapScreenPolicy,
) -> CheapScreenResult:
    """Screen outcome-independent specifications on one frozen development slice.

    Calibration thresholds use only the earlier calibration interval.  Exit-style
    variants sharing an identical cheap time-exit path reuse one vectorized result.
    This stage does not perform walk-forward, nulls, DSR/BH or Combine replay.
    """

    started = time.perf_counter()
    bound_by_market: dict[str, list[BoundSleeve]] = {}
    for market in sorted({row.market for row in sleeves}):
        matrix = matrices.get(market)
        if matrix is None:
            continue
        bound_by_market[market] = list(
            bind_sleeves_to_calibration(
                [row for row in sleeves if row.market == market],
                matrix,
                policy=policy,
            )
        )

    rows: list[dict[str, Any]] = []
    unique_total = 0
    cache_hits = 0
    for market, bound in sorted(bound_by_market.items()):
        by_execution: dict[str, BoundSleeve] = {}
        for row in bound:
            by_execution.setdefault(row.execution_fingerprint, row)
        unique = tuple(
            sorted(by_execution.values(), key=lambda value: value.execution_fingerprint)
        )
        unique_total += len(unique)
        cache_hits += len(bound) - len(unique)
        event_matrix = _screen_event_matrix(matrices[market], policy)
        compiled = compile_strategy_batch(
            [row.strategy for row in unique],
            event_matrix.feature_names,
            event_matrix.holding_horizons,
        )
        result = execute_stage1_vectorized(
            compiled, event_matrix, micro_batch_size=policy.micro_batch_size
        )
        metrics_by_execution: dict[str, dict[str, Any]] = {}
        for index, row in enumerate(unique):
            opportunities = int(result.opportunity_count[index])
            gross = float(result.gross_pnl[index])
            normal_net = float(result.net_pnl[index])
            per_event_cost = float(
                row.strategy.round_turn_cost * row.strategy.quantity
            )
            stressed_net = float(
                gross
                - policy.stress_cost_multiplier * per_event_cost * opportunities
            )
            first = float(result.first_half_net_pnl[index])
            second = float(result.second_half_net_pnl[index])
            concentration = float(result.best_positive_event_share[index])
            drawdown = float(result.approximate_max_drawdown[index])
            finite = bool(
                np.isfinite(
                    [gross, normal_net, stressed_net, first, second, concentration, drawdown]
                ).all()
            )
            survivor = bool(
                finite
                and opportunities >= policy.minimum_opportunities
                and gross > 0.0
                and normal_net > 0.0
                and stressed_net > 0.0
                and concentration <= policy.maximum_best_positive_event_share
                and drawdown <= policy.maximum_approximate_drawdown
                and (
                    not policy.require_nonnegative_half
                    or min(first, second) >= 0.0
                )
            )
            metrics_by_execution[row.execution_fingerprint] = {
                "opportunity_count": opportunities,
                "gross_pnl": gross,
                "net_pnl": normal_net,
                "stressed_net_pnl": stressed_net,
                "mean_net_pnl": _finite_or_none(result.mean_net_pnl[index]),
                "win_rate": _finite_or_none(result.win_rate[index]),
                "best_positive_event_share": concentration,
                "approximate_max_drawdown": drawdown,
                "first_half_net_pnl": first,
                "second_half_net_pnl": second,
                "finite": finite,
                "cheap_screen_survivor": survivor,
                "disposition": (
                    "COMPONENT_INCREMENTAL_VALUE_ELIGIBLE"
                    if survivor
                    else _failure_reason(
                        finite=finite,
                        opportunities=opportunities,
                        gross=gross,
                        net=normal_net,
                        stressed=stressed_net,
                        concentration=concentration,
                        drawdown=drawdown,
                        first=first,
                        second=second,
                        policy=policy,
                    )
                ),
            }
        for row in bound:
            rows.append(
                {
                    "sleeve_id": row.sleeve.sleeve_id,
                    "lineage_id": row.sleeve.lineage_id,
                    "market": row.sleeve.market,
                    "execution_market": row.sleeve.execution_market,
                    "role": row.sleeve.role.value,
                    "structural_fingerprint": row.sleeve.structural_fingerprint,
                    "behavioral_fingerprint": row.sleeve.behavioral_fingerprint,
                    "execution_fingerprint": row.execution_fingerprint,
                    "execution_cache_hit": (
                        row.sleeve.sleeve_id
                        != by_execution[row.execution_fingerprint].sleeve.sleeve_id
                    ),
                    "trigger_threshold": row.trigger_threshold,
                    "context_threshold": row.context_threshold,
                    **metrics_by_execution[row.execution_fingerprint],
                    "validation_scope": "ULTRA_CHEAP_DEVELOPMENT_SCREEN_ONLY",
                    "walk_forward_executed": False,
                    "tripwire_executed": False,
                    "DSR_BH_executed": False,
                    "rolling_combine_executed": False,
                }
            )
    rows.sort(key=lambda row: str(row["sleeve_id"]))
    return CheapScreenResult(
        policy=policy,
        proposal_count=len(sleeves),
        bound_count=sum(len(value) for value in bound_by_market.values()),
        unique_execution_path_count=unique_total,
        execution_cache_hit_count=cache_hits,
        rows=tuple(rows),
        elapsed_seconds=time.perf_counter() - started,
    )


def bind_sleeves_to_calibration(
    sleeves: Sequence[SleeveSpec],
    matrix: FeatureMatrix,
    *,
    policy: CheapScreenPolicy,
) -> tuple[BoundSleeve, ...]:
    day = matrix.array("session_day")
    session = matrix.array("session_code")
    calibration = (
        (day >= _day(policy.calibration_start))
        & (day < _day(policy.calibration_end_exclusive))
        & (session >= 0)
    )
    threshold_cache: dict[tuple[str, float], float] = {}

    def threshold(feature: str, quantile: float) -> float:
        key = (feature, quantile)
        cached = threshold_cache.get(key)
        if cached is not None:
            return cached
        values = matrix.array(f"feature__{feature}")[calibration]
        finite = values[np.isfinite(values)]
        if len(finite) < 100:
            raise ValueError(f"insufficient calibration observations for {feature}")
        value = float(np.quantile(finite, quantile))
        threshold_cache[key] = value
        return value

    output: list[BoundSleeve] = []
    provenance = dict(matrix.manifest.get("provenance") or {})
    point_value = float(provenance["point_value"])
    round_turn_cost = float(provenance["round_turn_cost"])
    for sleeve in sleeves:
        trigger_threshold = threshold(
            sleeve.trigger_feature, sleeve.trigger_quantile
        )
        context_threshold = (
            None
            if sleeve.context_feature is None
            else threshold(sleeve.context_feature, float(sleeve.context_quantile))
        )
        strategy = StrategySpec(
            candidate_id=sleeve.sleeve_id,
            lineage_id=sleeve.lineage_id,
            family=sleeve.trigger_feature,
            market=sleeve.market,
            timeframe=sleeve.timeframe,
            feature=sleeve.trigger_feature,
            operator=_operator(sleeve.trigger_operator),
            threshold=trigger_threshold,
            side=sleeve.side,
            holding_events=sleeve.holding_bars,
            point_value=point_value,
            round_turn_cost=round_turn_cost,
            role=_role(sleeve.role),
            context_feature=sleeve.context_feature,
            context_operator=(
                None
                if sleeve.context_operator is None
                else _operator(sleeve.context_operator)
            ),
            context_threshold=context_threshold,
            session_code=sleeve.session_code,
            quantity=1,
        )
        output.append(
            BoundSleeve(
                sleeve=sleeve,
                strategy=strategy,
                execution_fingerprint=structural_fingerprint(strategy),
                trigger_threshold=trigger_threshold,
                context_threshold=context_threshold,
            )
        )
    return tuple(output)


def _screen_event_matrix(
    matrix: FeatureMatrix, policy: CheapScreenPolicy
) -> EventMatrix:
    day = matrix.array("session_day")
    session = matrix.array("session_code")
    selected = (
        (day >= _day(policy.screen_start))
        & (day < _day(policy.screen_end_exclusive))
        & (session >= 0)
    )
    names = feature_names_for_bundle()
    return EventMatrix.from_arrays(
        feature_names=names,
        holding_horizons=HORIZONS,
        features=np.column_stack(
            [matrix.array(f"feature__{name}")[selected] for name in names]
        ),
        forward_moves=np.vstack(
            [matrix.array(f"forward_move__{horizon}")[selected] for horizon in HORIZONS]
        ),
        decision_ns=matrix.array("decision_ns")[selected],
        availability_ns=matrix.array("availability_ns")[selected],
        session_codes=session[selected],
    )


def _failure_reason(
    *,
    finite: bool,
    opportunities: int,
    gross: float,
    net: float,
    stressed: float,
    concentration: float,
    drawdown: float,
    first: float,
    second: float,
    policy: CheapScreenPolicy,
) -> str:
    if not finite:
        return "HARD_NONFINITE"
    if opportunities < policy.minimum_opportunities:
        return "INSUFFICIENT_OPPORTUNITY_COUNT"
    if gross <= 0.0:
        return "NONPOSITIVE_GROSS_EXPECTANCY"
    if net <= 0.0:
        return "NONPOSITIVE_NET_AFTER_COSTS"
    if stressed <= 0.0:
        return "WEAK_COST_MARGIN"
    if concentration > policy.maximum_best_positive_event_share:
        return "CONCENTRATION"
    if drawdown > policy.maximum_approximate_drawdown:
        return "APPROXIMATE_DRAWDOWN"
    if policy.require_nonnegative_half and min(first, second) < 0.0:
        return "BASIC_TEMPORAL_DISPERSION_FAILURE"
    return "CHEAP_SCREEN_REJECTED"


def _operator(value: str) -> ComparisonOperator:
    return {
        "GT": ComparisonOperator.GREATER_THAN,
        "GE": ComparisonOperator.GREATER_EQUAL,
        "LT": ComparisonOperator.LESS_THAN,
        "LE": ComparisonOperator.LESS_EQUAL,
    }[value]


def _role(value: EconomicRole) -> StrategyRole:
    if value in {EconomicRole.MLL_STABILIZER, EconomicRole.DEFENSIVE_SWITCH}:
        return StrategyRole.DEFENSIVE
    if value in {EconomicRole.XFA_COMPONENT, EconomicRole.PAYOUT_STABILIZER}:
        return StrategyRole.XFA_PAYOUT
    if value in {
        EconomicRole.SESSION_DIVERSIFIER,
        EconomicRole.MARKET_DIVERSIFIER,
        EconomicRole.CONSISTENCY_SMOOTHER,
    }:
        return StrategyRole.PORTFOLIO_ONLY
    return StrategyRole.ALPHA


def _day(value: str) -> int:
    return int(np.datetime64(value, "D").astype(np.int64))


def _finite_or_none(value: float) -> float | None:
    return float(value) if np.isfinite(value) else None


__all__ = [
    "BoundSleeve",
    "CheapScreenPolicy",
    "CheapScreenResult",
    "bind_sleeves_to_calibration",
    "run_ultra_cheap_screen",
]
