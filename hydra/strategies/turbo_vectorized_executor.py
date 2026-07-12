from __future__ import annotations

from dataclasses import dataclass
import math
from time import perf_counter

import numpy as np

from hydra.strategies.turbo_compiler import CompiledStrategyBatch


def _readonly(array: np.ndarray) -> np.ndarray:
    array.setflags(write=False)
    return array


@dataclass(frozen=True, slots=True)
class EventMatrix:
    """Past-only feature matrix plus explicitly separated future replay targets."""

    feature_names: tuple[str, ...]
    holding_horizons: tuple[int, ...]
    features: np.ndarray
    forward_moves: np.ndarray
    decision_ns: np.ndarray
    availability_ns: np.ndarray
    session_codes: np.ndarray

    @classmethod
    def from_arrays(
        cls,
        *,
        feature_names: tuple[str, ...],
        holding_horizons: tuple[int, ...],
        features: np.ndarray,
        forward_moves: np.ndarray,
        decision_ns: np.ndarray,
        availability_ns: np.ndarray,
        session_codes: np.ndarray | None = None,
    ) -> "EventMatrix":
        feature_values = np.array(features, dtype=np.float64, order="C", copy=True)
        future_values = np.array(forward_moves, dtype=np.float64, order="C", copy=True)
        decisions = np.array(decision_ns, dtype=np.int64, order="C", copy=True)
        availability = np.array(availability_ns, dtype=np.int64, order="C", copy=True)
        sessions = (
            np.full(decisions.shape, -1, dtype=np.int16)
            if session_codes is None
            else np.array(session_codes, dtype=np.int16, order="C", copy=True)
        )

        if feature_values.ndim != 2:
            raise ValueError("features must have shape (events, features)")
        if future_values.ndim != 2:
            raise ValueError("forward_moves must have shape (horizons, events)")
        event_count = feature_values.shape[0]
        if feature_values.shape[1] != len(feature_names):
            raise ValueError("feature_names do not match the feature matrix")
        if future_values.shape != (len(holding_horizons), event_count):
            raise ValueError("holding_horizons do not match the forward matrix")
        if decisions.shape != (event_count,) or availability.shape != (event_count,):
            raise ValueError("timestamps must contain one value per event")
        if sessions.shape != (event_count,):
            raise ValueError("session_codes must contain one value per event")
        if event_count and np.any(decisions[1:] < decisions[:-1]):
            raise ValueError("events must be ordered by decision timestamp")
        if np.any(availability > decisions):
            raise ValueError("feature availability cannot follow the decision timestamp")
        if len(set(feature_names)) != len(feature_names):
            raise ValueError("feature_names must be unique")
        if len(set(holding_horizons)) != len(holding_horizons):
            raise ValueError("holding_horizons must be unique")

        return cls(
            feature_names=feature_names,
            holding_horizons=holding_horizons,
            features=_readonly(feature_values),
            forward_moves=_readonly(future_values),
            decision_ns=_readonly(decisions),
            availability_ns=_readonly(availability),
            session_codes=_readonly(sessions),
        )

    @property
    def event_count(self) -> int:
        return int(self.features.shape[0])


@dataclass(frozen=True, slots=True)
class Stage1BatchResult:
    candidate_ids: tuple[str, ...]
    fingerprints: tuple[str, ...]
    opportunity_count: np.ndarray
    gross_pnl: np.ndarray
    net_pnl: np.ndarray
    mean_net_pnl: np.ndarray
    win_rate: np.ndarray
    best_positive_event_share: np.ndarray
    approximate_max_drawdown: np.ndarray
    first_half_net_pnl: np.ndarray
    second_half_net_pnl: np.ndarray

    def __len__(self) -> int:
        return len(self.candidate_ids)

    def assert_equivalent(self, other: "Stage1BatchResult") -> None:
        if self.candidate_ids != other.candidate_ids or self.fingerprints != other.fingerprints:
            raise AssertionError("candidate ordering or fingerprints differ")
        for field in (
            "opportunity_count",
            "gross_pnl",
            "net_pnl",
            "mean_net_pnl",
            "win_rate",
            "best_positive_event_share",
            "approximate_max_drawdown",
            "first_half_net_pnl",
            "second_half_net_pnl",
        ):
            left = getattr(self, field)
            right = getattr(other, field)
            if not np.array_equal(left, right, equal_nan=True):
                raise AssertionError(f"reference mismatch in {field}")


def _compare(values: np.ndarray, operators: np.ndarray, thresholds: np.ndarray) -> np.ndarray:
    """Compare a strategy-by-event matrix without Python candidate loops."""

    result = np.zeros(values.shape, dtype=np.bool_)
    result |= (operators[:, None] == 1) & (values > thresholds[:, None])
    result |= (operators[:, None] == 2) & (values >= thresholds[:, None])
    result |= (operators[:, None] == -1) & (values < thresholds[:, None])
    result |= (operators[:, None] == -2) & (values <= thresholds[:, None])
    return result & np.isfinite(values)


def _metrics_from_pnl(
    signals: np.ndarray, gross_event_pnl: np.ndarray, costs: np.ndarray
) -> tuple[np.ndarray, ...]:
    net_event_pnl = np.where(signals, gross_event_pnl - costs[:, None], 0.0)
    gross_selected = np.where(signals, gross_event_pnl, 0.0)
    count = signals.sum(axis=1, dtype=np.int64)
    gross = gross_selected.sum(axis=1, dtype=np.float64)
    net = net_event_pnl.sum(axis=1, dtype=np.float64)
    mean = np.divide(net, count, out=np.full(net.shape, np.nan), where=count > 0)
    wins = ((net_event_pnl > 0.0) & signals).sum(axis=1, dtype=np.int64)
    win_rate = np.divide(wins, count, out=np.full(net.shape, np.nan), where=count > 0)
    positive = np.where(net_event_pnl > 0.0, net_event_pnl, 0.0)
    positive_sum = positive.sum(axis=1, dtype=np.float64)
    positive_best = positive.max(axis=1, initial=0.0)
    concentration = np.divide(
        positive_best,
        positive_sum,
        out=np.zeros(net.shape, dtype=np.float64),
        where=positive_sum > 0.0,
    )
    equity = np.cumsum(net_event_pnl, axis=1, dtype=np.float64)
    running_peak = np.maximum.accumulate(
        np.concatenate((np.zeros((len(signals), 1)), equity), axis=1), axis=1
    )[:, 1:]
    drawdown = np.max(running_peak - equity, axis=1, initial=0.0)
    split = signals.shape[1] // 2
    first_half = net_event_pnl[:, :split].sum(axis=1, dtype=np.float64)
    second_half = net_event_pnl[:, split:].sum(axis=1, dtype=np.float64)
    return (
        count,
        gross,
        net,
        mean,
        win_rate,
        concentration,
        drawdown,
        first_half,
        second_half,
    )


def _empty_result(compiled: CompiledStrategyBatch) -> Stage1BatchResult:
    float_values = _readonly(np.empty(0, dtype=np.float64))
    return Stage1BatchResult(
        candidate_ids=compiled.candidate_ids,
        fingerprints=compiled.fingerprints,
        opportunity_count=_readonly(np.empty(0, dtype=np.int64)),
        gross_pnl=float_values,
        net_pnl=float_values,
        mean_net_pnl=float_values,
        win_rate=float_values,
        best_positive_event_share=float_values,
        approximate_max_drawdown=float_values,
        first_half_net_pnl=float_values,
        second_half_net_pnl=float_values,
    )


def execute_stage1_vectorized(
    compiled: CompiledStrategyBatch,
    matrix: EventMatrix,
    *,
    micro_batch_size: int = 512,
) -> Stage1BatchResult:
    """Evaluate many structures against one immutable event matrix."""

    if micro_batch_size <= 0:
        raise ValueError("micro_batch_size must be positive")
    if len(compiled) == 0:
        return _empty_result(compiled)
    if matrix.event_count == 0:
        raise ValueError("event matrix cannot be empty")

    outputs: list[list[np.ndarray]] = [[] for _ in range(9)]
    for start in range(0, len(compiled), micro_batch_size):
        stop = min(start + micro_batch_size, len(compiled))
        feature_values = matrix.features[:, compiled.feature_indices[start:stop]].T
        signals = _compare(
            feature_values,
            compiled.operator_codes[start:stop],
            compiled.thresholds[start:stop],
        )

        context_indices = compiled.context_feature_indices[start:stop]
        has_context = context_indices >= 0
        if np.any(has_context):
            safe_context_indices = np.maximum(context_indices, 0)
            context_values = matrix.features[:, safe_context_indices].T
            context_signals = _compare(
                context_values,
                compiled.context_operator_codes[start:stop],
                compiled.context_thresholds[start:stop],
            )
            signals &= (~has_context[:, None]) | context_signals

        session_codes = compiled.session_codes[start:stop]
        signals &= (session_codes[:, None] < 0) | (
            matrix.session_codes[None, :] == session_codes[:, None]
        )
        horizon_values = matrix.forward_moves[
            compiled.horizon_indices[start:stop], :
        ]
        signals &= np.isfinite(horizon_values)
        gross_event_pnl = (
            horizon_values
            * compiled.sides[start:stop, None]
            * compiled.point_values[start:stop, None]
            * compiled.quantities[start:stop, None]
        )
        metrics = _metrics_from_pnl(
            signals,
            gross_event_pnl,
            compiled.round_turn_costs[start:stop]
            * compiled.quantities[start:stop],
        )
        for collector, values in zip(outputs, metrics, strict=True):
            collector.append(values)

    merged = [_readonly(np.concatenate(parts)) for parts in outputs]
    return Stage1BatchResult(
        candidate_ids=compiled.candidate_ids,
        fingerprints=compiled.fingerprints,
        opportunity_count=merged[0],
        gross_pnl=merged[1],
        net_pnl=merged[2],
        mean_net_pnl=merged[3],
        win_rate=merged[4],
        best_positive_event_share=merged[5],
        approximate_max_drawdown=merged[6],
        first_half_net_pnl=merged[7],
        second_half_net_pnl=merged[8],
    )


def execute_stage1_reference(
    compiled: CompiledStrategyBatch, matrix: EventMatrix
) -> Stage1BatchResult:
    """Intentionally scalar event-loop oracle for determinism and benchmarks.

    This mirrors the candidate/event Python loop that Turbo replaces.  Metrics
    are reduced by the same helper as the vectorized path so any speedup cannot
    hide a change in economic arithmetic.
    """

    if len(compiled) == 0:
        return _empty_result(compiled)
    if matrix.event_count == 0:
        raise ValueError("event matrix cannot be empty")
    outputs: list[list[np.ndarray]] = [[] for _ in range(9)]
    for index in range(len(compiled)):
        signals = np.zeros((1, matrix.event_count), dtype=np.bool_)
        feature_index = int(compiled.feature_indices[index])
        operator_code = int(compiled.operator_codes[index])
        threshold = float(compiled.thresholds[index])
        context_index = int(compiled.context_feature_indices[index])
        context_operator = int(compiled.context_operator_codes[index])
        context_threshold = float(compiled.context_thresholds[index])
        session_code = int(compiled.session_codes[index])
        horizon_index = int(compiled.horizon_indices[index])
        for event_index in range(matrix.event_count):
            feature_value = float(matrix.features[event_index, feature_index])
            if not _scalar_compare(feature_value, operator_code, threshold):
                continue
            if context_index >= 0 and not _scalar_compare(
                float(matrix.features[event_index, context_index]),
                context_operator,
                context_threshold,
            ):
                continue
            if session_code >= 0 and int(matrix.session_codes[event_index]) != session_code:
                continue
            if not math.isfinite(float(matrix.forward_moves[horizon_index, event_index])):
                continue
            signals[0, event_index] = True
        horizon_values = matrix.forward_moves[
            compiled.horizon_indices[index : index + 1], :
        ]
        gross_event_pnl = (
            horizon_values
            * compiled.sides[index]
            * compiled.point_values[index]
            * compiled.quantities[index]
        )
        metrics = _metrics_from_pnl(
            signals,
            gross_event_pnl,
            compiled.round_turn_costs[index : index + 1]
            * compiled.quantities[index : index + 1],
        )
        for collector, values in zip(outputs, metrics, strict=True):
            collector.append(values)

    merged = [_readonly(np.concatenate(parts)) for parts in outputs]
    return Stage1BatchResult(
        candidate_ids=compiled.candidate_ids,
        fingerprints=compiled.fingerprints,
        opportunity_count=merged[0],
        gross_pnl=merged[1],
        net_pnl=merged[2],
        mean_net_pnl=merged[3],
        win_rate=merged[4],
        best_positive_event_share=merged[5],
        approximate_max_drawdown=merged[6],
        first_half_net_pnl=merged[7],
        second_half_net_pnl=merged[8],
    )


def _scalar_compare(value: float, operator: int, threshold: float) -> bool:
    if not math.isfinite(value):
        return False
    if operator == 1:
        return value > threshold
    if operator == 2:
        return value >= threshold
    if operator == -1:
        return value < threshold
    if operator == -2:
        return value <= threshold
    return False


@dataclass(frozen=True, slots=True)
class Stage1Benchmark:
    strategies: int
    events: int
    reference_seconds: float
    vectorized_seconds: float
    speedup: float
    outputs_identical: bool
    reference_candidates_per_second: float
    vectorized_candidates_per_second: float


def benchmark_stage1(
    compiled: CompiledStrategyBatch,
    matrix: EventMatrix,
    *,
    repeats: int = 3,
    micro_batch_size: int = 512,
) -> Stage1Benchmark:
    """Benchmark identical frozen inputs and fail if optimized results diverge."""

    if repeats <= 0:
        raise ValueError("repeats must be positive")
    # Warm the code paths and allocator before collecting the minimum stable time.
    reference = execute_stage1_reference(compiled, matrix)
    vectorized = execute_stage1_vectorized(
        compiled, matrix, micro_batch_size=micro_batch_size
    )
    reference.assert_equivalent(vectorized)

    reference_timings: list[float] = []
    vectorized_timings: list[float] = []
    for _ in range(repeats):
        started = perf_counter()
        reference = execute_stage1_reference(compiled, matrix)
        reference_timings.append(perf_counter() - started)
        started = perf_counter()
        vectorized = execute_stage1_vectorized(
            compiled, matrix, micro_batch_size=micro_batch_size
        )
        vectorized_timings.append(perf_counter() - started)
    reference.assert_equivalent(vectorized)
    reference_seconds = min(reference_timings)
    vectorized_seconds = min(vectorized_timings)
    speedup = (
        reference_seconds / vectorized_seconds
        if vectorized_seconds > 0.0
        else math.inf
    )
    return Stage1Benchmark(
        strategies=len(compiled),
        events=matrix.event_count,
        reference_seconds=reference_seconds,
        vectorized_seconds=vectorized_seconds,
        speedup=speedup,
        outputs_identical=True,
        reference_candidates_per_second=len(compiled) / reference_seconds,
        vectorized_candidates_per_second=len(compiled) / vectorized_seconds,
    )


__all__ = [
    "EventMatrix",
    "Stage1BatchResult",
    "Stage1Benchmark",
    "benchmark_stage1",
    "execute_stage1_reference",
    "execute_stage1_vectorized",
]
