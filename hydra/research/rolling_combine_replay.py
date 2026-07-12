from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_fitness import (
    combine_passer_fitness,
    diagnose_combine_failure,
    diagnose_xfa_failure,
    xfa_payout_fitness,
)
from hydra.propfirm.payout_episode import evaluate_rolling_xfa
from hydra.propfirm.rolling_combine import (
    EpisodeStartPolicy,
    evaluate_rolling_combine,
)
from hydra.research.turbo_exact_replay import spec_from_dict, spec_to_dict
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategyRole, StrategySpec


MINUTE_NS = 60_000_000_000
DEVELOPMENT_START = "2023-01-01"
DEVELOPMENT_END_EXCLUSIVE = "2024-10-01"
_WORKER_MATRIX_CACHE: dict[str, FeatureMatrix] = {}


@dataclass(frozen=True, slots=True)
class ExactTradePath:
    candidate_id: str
    event_count: int
    gross_pnl: float
    net_pnl: float
    cost_stress_1_5x_net: float
    maximum_drawdown: float
    best_positive_event_share: float
    eligible_session_days: tuple[int, ...]
    day_regimes: dict[int, str]
    fold_results: dict[str, dict[str, float | int]]
    events: tuple[TradePathEvent, ...]

    def metrics(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("events", None)
        payload["eligible_session_days"] = list(self.eligible_session_days)
        payload["day_regimes"] = {
            str(key): value for key, value in self.day_regimes.items()
        }
        return payload


def build_exact_trade_path(
    spec: StrategySpec,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str = DEVELOPMENT_START,
    end_exclusive: str = DEVELOPMENT_END_EXCLUSIVE,
) -> ExactTradePath:
    positions = _signal_positions(
        spec,
        matrix,
        start_inclusive=start_inclusive,
        end_exclusive=end_exclusive,
    )
    day_array = matrix.array("session_day")
    session = matrix.array("session_code")
    all_mask = (
        (day_array >= _day(start_inclusive))
        & (day_array < _day(end_exclusive))
        & (session >= 0)
    )
    eligible_days = tuple(sorted({int(value) for value in day_array[all_mask]}))
    regimes = _day_regimes(matrix, all_mask)
    events: list[TradePathEvent] = []
    entry_prices = matrix.array("entry_price")
    highs = matrix.array("bar_high")
    lows = matrix.array("bar_low")
    timestamp = matrix.array("timestamp_ns")
    decisions = matrix.array("decision_ns")
    segments = matrix.array("segment_code")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    cost = float(spec.round_turn_cost * spec.quantity)
    for ordinal, raw_position in enumerate(positions):
        position = int(raw_position)
        entry_index = position + 1
        exit_index = position + spec.holding_events + 1
        if exit_index >= matrix.row_count:
            raise ValueError("exact path exit exceeds feature matrix")
        if int(segments[entry_index]) != int(segments[exit_index]):
            raise ValueError("exact path crosses a contiguous contract/session segment")
        entry = float(entry_prices[position])
        path_high = float(np.max(highs[entry_index : exit_index + 1]))
        path_low = float(np.min(lows[entry_index : exit_index + 1]))
        adverse = path_low if spec.side > 0 else path_high
        favorable = path_high if spec.side > 0 else path_low
        worst_gross = (
            (adverse - entry) * spec.side * spec.point_value * spec.quantity
        )
        best_gross = (
            (favorable - entry) * spec.side * spec.point_value * spec.quantity
        )
        gross = (
            float(forward[position])
            * spec.side
            * spec.point_value
            * spec.quantity
        )
        day = int(day_array[position])
        event_id = f"{spec.candidate_id}:{ordinal:05d}:{int(decisions[position])}"
        events.append(
            TradePathEvent(
                event_id=event_id,
                decision_ns=int(decisions[position]),
                exit_ns=int(timestamp[exit_index]) + MINUTE_NS,
                session_day=day,
                net_pnl=float(gross - cost),
                gross_pnl=float(gross),
                worst_unrealized_pnl=float(worst_gross - cost),
                best_unrealized_pnl=float(best_gross - cost),
                quantity=spec.quantity,
                mini_equivalent=float(spec.quantity),
                regime=regimes.get(day, "UNKNOWN"),
                session_compliant=bool(session[position] >= 0),
                contract_limit_compliant=bool(spec.quantity <= 15),
                same_bar_ambiguous=bool(
                    entry_index == exit_index and worst_gross < 0 < best_gross
                ),
            )
        )
    net_values = np.asarray([event.net_pnl for event in events], dtype=float)
    gross_values = np.asarray([event.gross_pnl for event in events], dtype=float)
    if len(net_values):
        equity = np.cumsum(net_values)
        peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:]
        drawdown = float(np.max(peak - equity, initial=0.0))
        positives = net_values[net_values > 0]
        positive_sum = float(positives.sum())
        best_share = float(positives.max() / positive_sum) if positive_sum else 1.0
    else:
        drawdown = 0.0
        best_share = 1.0
    return ExactTradePath(
        candidate_id=spec.candidate_id,
        event_count=len(events),
        gross_pnl=float(gross_values.sum()),
        net_pnl=float(net_values.sum()),
        cost_stress_1_5x_net=float((gross_values - 1.5 * cost).sum()),
        maximum_drawdown=drawdown,
        best_positive_event_share=best_share,
        eligible_session_days=eligible_days,
        day_regimes=regimes,
        fold_results=_fold_results(events),
        events=tuple(events),
    )


def run_rolling_combine_job(payload: Mapping[str, Any]) -> dict[str, Any]:
    matrix_path = str(payload["matrix_path"])
    matrix = _WORKER_MATRIX_CACHE.get(matrix_path)
    if matrix is None:
        matrix = FeatureMatrix.open(matrix_path, mmap=True)
        _WORKER_MATRIX_CACHE[matrix_path] = matrix
    spec = spec_from_dict(dict(payload["specification"]))
    path = build_exact_trade_path(spec, matrix)
    policy = EpisodeStartPolicy(
        maximum_starts=int(payload.get("maximum_episode_starts") or 24),
        minimum_spacing_sessions=int(payload.get("minimum_spacing_sessions") or 5),
        minimum_observation_sessions=int(
            payload.get("minimum_observation_sessions") or 30
        ),
        maximum_duration_sessions=int(
            payload.get("maximum_duration_sessions") or 60
        ),
        regime_balanced=True,
    )
    rolling = evaluate_rolling_combine(
        path.events,
        path.eligible_session_days,
        day_regimes=path.day_regimes,
        policy=policy,
    )
    combine_fitness = combine_passer_fitness(
        rolling,
        cost_stress_net_pnl=path.cost_stress_1_5x_net,
        complexity=_complexity(spec),
    )
    xfa = evaluate_rolling_xfa(
        path.events,
        path.eligible_session_days,
        day_regimes=path.day_regimes,
        maximum_starts=int(payload.get("maximum_xfa_starts") or 12),
    )
    xfa_fitness = xfa_payout_fitness(xfa, complexity=_complexity(spec))
    if spec.role in {StrategyRole.DEFENSIVE, StrategyRole.PORTFOLIO_ONLY, StrategyRole.HAZARD}:
        diagnosis = "DEFENSIVE_ACCOUNT_REPLAY_REQUIRED"
        failure_objective = "DEFENSIVE_ACCOUNT_FITNESS"
    elif spec.role == StrategyRole.XFA_PAYOUT:
        diagnosis = diagnose_xfa_failure(xfa)
        failure_objective = "XFA_PAYOUT_FITNESS"
    else:
        diagnosis = diagnose_combine_failure(
            rolling, cost_stress_net_pnl=path.cost_stress_1_5x_net
        )
        failure_objective = "COMBINE_PASSER_FITNESS"
    return {
        "candidate_id": spec.candidate_id,
        "lineage_id": spec.lineage_id,
        "mechanism_family": spec.family,
        "market": spec.market,
        "timeframe": spec.timeframe,
        "role": spec.role.name,
        "specification": spec_to_dict(spec),
        "exact_trade_path": path.metrics(),
        "rolling_combine": rolling.to_dict(include_daily_paths=False),
        "combine_fitness": combine_fitness.to_dict(),
        "rolling_xfa": xfa.to_dict(),
        "xfa_fitness": xfa_fitness.to_dict(),
        "failure_diagnosis": diagnosis,
        "failure_objective": failure_objective,
        "hard_invalidation": False,
        "q4_access_count_delta": 0,
        "outbound_order_capability": False,
    }


def _signal_positions(
    spec: StrategySpec,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str,
    end_exclusive: str,
) -> np.ndarray:
    feature = matrix.array(f"feature__{spec.feature}")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    session = matrix.array("session_code")
    day = matrix.array("session_day")
    mask = (
        (day >= _day(start_inclusive))
        & (day < _day(end_exclusive))
        & np.isfinite(feature)
        & np.isfinite(forward)
        & _compare(feature, spec.operator, spec.threshold)
    )
    mask &= session == spec.session_code if spec.session_code >= 0 else session >= 0
    if spec.context_feature is not None:
        context = matrix.array(f"feature__{spec.context_feature}")
        mask &= np.isfinite(context) & _compare(
            context,
            spec.context_operator or ComparisonOperator.GREATER_THAN,
            float(spec.context_threshold or 0.0),
        )
    positions = np.flatnonzero(mask)
    if not len(positions):
        return positions.astype(np.int64)
    decisions = matrix.array("decision_ns")
    segments = matrix.array("segment_code")
    retained: list[int] = []
    last_segment = -1
    last_exit = np.iinfo(np.int64).min
    hold_ns = (spec.holding_events + 1) * MINUTE_NS
    for raw in positions:
        index = int(raw)
        segment = int(segments[index])
        if segment != last_segment:
            last_segment = segment
            last_exit = np.iinfo(np.int64).min
        decision = int(decisions[index])
        if decision < last_exit:
            continue
        retained.append(index)
        last_exit = decision + hold_ns
    return np.asarray(retained, dtype=np.int64)


def _day_regimes(matrix: FeatureMatrix, selected: np.ndarray) -> dict[int, str]:
    days = matrix.array("session_day")[selected]
    feature = matrix.array("feature__ctx_60m_volatility_expansion")[selected]
    completed_session_states: dict[int, str] = {}
    ordered_days = [int(value) for value in np.unique(days)]
    for day in ordered_days:
        values = feature[days == day]
        values = values[np.isfinite(values)]
        if not len(values):
            completed_session_states[day] = "UNKNOWN"
            continue
        median = float(np.median(values))
        completed_session_states[day] = (
            "VOLATILITY_EXPANSION"
            if median >= 1.20
            else "VOLATILITY_CONTRACTION"
            if median <= 0.80
            else "VOLATILITY_NORMAL"
        )
    # Episode starts are session-level decisions.  A statistic using every bar
    # of the start session is not available at that boundary, even when the
    # underlying 60-minute feature is itself closed-bar safe.  Shift the
    # completed-session label by one trading session so changing future bars in
    # day D cannot change the regime assigned to the start of day D.
    output: dict[int, str] = {}
    previous_state = "UNKNOWN"
    for day in ordered_days:
        output[day] = previous_state
        previous_state = completed_session_states[day]
    return output


def _fold_results(events: Sequence[TradePathEvent]) -> dict[str, dict[str, float | int]]:
    folds = (
        ("2023_h1", "2023-01-01", "2023-07-01"),
        ("2023_h2", "2023-07-01", "2024-01-01"),
        ("2024_q1", "2024-01-01", "2024-04-01"),
        ("2024_q2", "2024-04-01", "2024-07-01"),
        ("2024_q3", "2024-07-01", "2024-10-01"),
    )
    output: dict[str, dict[str, float | int]] = {}
    for name, start, end in folds:
        values = [
            event.net_pnl
            for event in events
            if _day(start) <= event.session_day < _day(end)
        ]
        output[name] = {
            "events": len(values),
            "net_pnl": float(sum(values)),
            "mean_net_pnl": float(np.mean(values)) if values else 0.0,
        }
    return output


def _compare(
    values: np.ndarray, operator: ComparisonOperator, threshold: float
) -> np.ndarray:
    if operator == ComparisonOperator.GREATER_THAN:
        return values > threshold
    if operator == ComparisonOperator.GREATER_EQUAL:
        return values >= threshold
    if operator == ComparisonOperator.LESS_THAN:
        return values < threshold
    if operator == ComparisonOperator.LESS_EQUAL:
        return values <= threshold
    raise ValueError(f"unsupported operator: {operator}")


def _complexity(spec: StrategySpec) -> float:
    return float(1 + int(spec.context_feature is not None) + int(spec.session_code >= 0))


def _day(value: str) -> int:
    return int(np.datetime64(value, "D").astype(np.int64))


__all__ = [
    "ExactTradePath",
    "build_exact_trade_path",
    "run_rolling_combine_job",
]
