from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategyRole, StrategySpec


MINUTE_NS = 60_000_000_000
VALIDATION_FOLDS = (
    ("2024_q1", "2024-01-01", "2024-04-01"),
    ("2024_q2", "2024-04-01", "2024-07-01"),
    ("2024_q3", "2024-07-01", "2024-10-01"),
)
_WORKER_MATRIX_CACHE: dict[str, FeatureMatrix] = {}


@dataclass(frozen=True)
class ExactReplayBenchmark:
    candidates: int
    rows: int
    reference_seconds: float
    optimized_seconds: float
    speedup: float
    outputs_identical: bool
    reference_candidates_per_second: float
    optimized_candidates_per_second: float


def spec_to_dict(spec: StrategySpec) -> dict[str, Any]:
    payload = asdict(spec)
    payload["operator"] = int(spec.operator)
    payload["role"] = int(spec.role)
    payload["context_operator"] = (
        None if spec.context_operator is None else int(spec.context_operator)
    )
    return payload


def spec_from_dict(payload: Mapping[str, Any]) -> StrategySpec:
    values = dict(payload)
    values["operator"] = ComparisonOperator(int(values["operator"]))
    values["role"] = StrategyRole(int(values["role"]))
    if values.get("context_operator") is not None:
        values["context_operator"] = ComparisonOperator(
            int(values["context_operator"])
        )
    return StrategySpec(**values)


def run_exact_replay_job(payload: Mapping[str, Any]) -> dict[str, Any]:
    matrix_path = str(payload["matrix_path"])
    matrix = _WORKER_MATRIX_CACHE.get(matrix_path)
    if matrix is None:
        matrix = FeatureMatrix.open(matrix_path, mmap=True)
        _WORKER_MATRIX_CACHE[matrix_path] = matrix
    spec = spec_from_dict(dict(payload["specification"]))
    return exact_replay(spec, matrix)


def exact_replay(spec: StrategySpec, matrix: FeatureMatrix) -> dict[str, Any]:
    positions = _optimized_signal_positions(spec, matrix)
    return _metrics(spec, matrix, positions)


def exact_replay_reference(
    spec: StrategySpec, matrix: FeatureMatrix
) -> dict[str, Any]:
    feature = matrix.array(f"feature__{spec.feature}")
    context = (
        None
        if spec.context_feature is None
        else matrix.array(f"feature__{spec.context_feature}")
    )
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    session = matrix.array("session_code")
    day = matrix.array("session_day")
    start = _day("2024-01-01")
    end = _day("2024-10-01")
    candidates: list[int] = []
    for index in range(matrix.row_count):
        if not start <= int(day[index]) < end:
            continue
        if spec.session_code >= 0 and int(session[index]) != spec.session_code:
            continue
        if spec.session_code < 0 and int(session[index]) < 0:
            continue
        if not math.isfinite(float(feature[index])) or not math.isfinite(
            float(forward[index])
        ):
            continue
        if not _scalar_compare(
            float(feature[index]), spec.operator, float(spec.threshold)
        ):
            continue
        if context is not None:
            value = float(context[index])
            if not math.isfinite(value) or not _scalar_compare(
                value,
                spec.context_operator or ComparisonOperator.GREATER_THAN,
                float(spec.context_threshold or 0.0),
            ):
                continue
        candidates.append(index)
    positions = _non_overlapping(np.asarray(candidates, dtype=np.int64), spec, matrix)
    return _metrics(spec, matrix, positions)


def benchmark_exact_replay(
    specs: Sequence[StrategySpec],
    matrix: FeatureMatrix,
    *,
    repeats: int = 2,
) -> ExactReplayBenchmark:
    if not specs or repeats <= 0:
        raise ValueError("Exact replay benchmark requires specs and positive repeats.")
    reference_values = [exact_replay_reference(spec, matrix) for spec in specs]
    optimized_values = [exact_replay(spec, matrix) for spec in specs]
    _assert_same(reference_values, optimized_values)
    reference_times: list[float] = []
    optimized_times: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        reference_values = [exact_replay_reference(spec, matrix) for spec in specs]
        reference_times.append(time.perf_counter() - started)
        started = time.perf_counter()
        optimized_values = [exact_replay(spec, matrix) for spec in specs]
        optimized_times.append(time.perf_counter() - started)
    _assert_same(reference_values, optimized_values)
    reference = min(reference_times)
    optimized = min(optimized_times)
    return ExactReplayBenchmark(
        candidates=len(specs),
        rows=matrix.row_count,
        reference_seconds=reference,
        optimized_seconds=optimized,
        speedup=reference / max(optimized, 1e-12),
        outputs_identical=True,
        reference_candidates_per_second=len(specs) / max(reference, 1e-12),
        optimized_candidates_per_second=len(specs) / max(optimized, 1e-12),
    )


def _optimized_signal_positions(
    spec: StrategySpec, matrix: FeatureMatrix
) -> np.ndarray:
    feature = matrix.array(f"feature__{spec.feature}")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    session = matrix.array("session_code")
    day = matrix.array("session_day")
    mask = (
        (day >= _day("2024-01-01"))
        & (day < _day("2024-10-01"))
        & np.isfinite(feature)
        & np.isfinite(forward)
        & _array_compare(feature, spec.operator, spec.threshold)
    )
    mask &= session == spec.session_code if spec.session_code >= 0 else session >= 0
    if spec.context_feature is not None:
        context = matrix.array(f"feature__{spec.context_feature}")
        mask &= np.isfinite(context) & _array_compare(
            context,
            spec.context_operator or ComparisonOperator.GREATER_THAN,
            float(spec.context_threshold or 0.0),
        )
    return _non_overlapping(np.flatnonzero(mask), spec, matrix)


def _non_overlapping(
    positions: np.ndarray, spec: StrategySpec, matrix: FeatureMatrix
) -> np.ndarray:
    if not len(positions):
        return positions.astype(np.int64, copy=False)
    decisions = matrix.array("decision_ns")
    segments = matrix.array("segment_code")
    keep: list[int] = []
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
        keep.append(index)
        last_exit = decision + hold_ns
    return np.asarray(keep, dtype=np.int64)


def _metrics(
    spec: StrategySpec, matrix: FeatureMatrix, positions: np.ndarray
) -> dict[str, Any]:
    if not len(positions):
        return _empty_metrics(spec)
    move = matrix.array(f"forward_move__{spec.holding_events}")[positions]
    gross = move * spec.side * spec.point_value * spec.quantity
    cost = spec.round_turn_cost * spec.quantity
    net = gross - cost
    equity = np.cumsum(net, dtype=np.float64)
    peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:]
    drawdown = float(np.max(peak - equity, initial=0.0))
    positive = net[net > 0]
    positive_total = float(positive.sum())
    best_share = (
        float(positive.max() / positive_total) if positive_total > 0 else 1.0
    )
    days = matrix.array("session_day")[positions]
    folds: dict[str, dict[str, Any]] = {}
    for name, start, end in VALIDATION_FOLDS:
        selected = (days >= _day(start)) & (days < _day(end))
        values = net[selected]
        folds[name] = {
            "events": int(len(values)),
            "net_pnl": float(values.sum()),
            "mean_net_pnl": float(values.mean()) if len(values) else 0.0,
        }
    mean = float(net.mean())
    catastrophic = bool(
        mean > 0
        and any(
            value["events"] > 0
            and float(value["mean_net_pnl"]) < -2.0 * abs(mean)
            for value in folds.values()
        )
    )
    delayed = _delayed_net(spec, matrix, positions)
    return {
        "candidate_id": spec.candidate_id,
        "lineage_id": spec.lineage_id,
        "family": spec.family,
        "market": spec.market,
        "role": spec.role.name,
        "events": int(len(net)),
        "gross_pnl": float(gross.sum()),
        "net_pnl": float(net.sum()),
        "cost_stress_1_5x_net": float((gross - 1.5 * cost).sum()),
        "maximum_drawdown": drawdown,
        "best_positive_event_share": best_share,
        "supportive_temporal_folds": int(
            sum(float(value["net_pnl"]) > 0 for value in folds.values())
        ),
        "catastrophic_transfer": catastrophic,
        "fold_results": folds,
        "one_bar_delay_net_pnl": delayed,
        "event_gross_pnl": gross.astype(float).tolist(),
        "event_net_pnl": net.astype(float).tolist(),
        "event_session_days": days.astype(int).tolist(),
        "finite": bool(np.isfinite(net).all()),
        "hard_invalidation": False,
        "mll_proxy_safe": drawdown < 3_000.0,
        "status": "EXACT_REPLAY_COMPLETED",
    }


def _delayed_net(
    spec: StrategySpec, matrix: FeatureMatrix, positions: np.ndarray
) -> float:
    delayed = positions + 1
    valid = delayed < matrix.row_count
    delayed = delayed[valid]
    original = positions[valid]
    if not len(delayed):
        return 0.0
    same_segment = matrix.array("segment_code")[delayed] == matrix.array("segment_code")[original]
    moves = matrix.array(f"forward_move__{spec.holding_events}")[delayed[same_segment]]
    moves = moves[np.isfinite(moves)]
    return float(
        (moves * spec.side * spec.point_value * spec.quantity - spec.round_turn_cost * spec.quantity).sum()
    )


def _empty_metrics(spec: StrategySpec) -> dict[str, Any]:
    return {
        "candidate_id": spec.candidate_id,
        "lineage_id": spec.lineage_id,
        "family": spec.family,
        "market": spec.market,
        "role": spec.role.name,
        "events": 0,
        "gross_pnl": 0.0,
        "net_pnl": 0.0,
        "cost_stress_1_5x_net": 0.0,
        "maximum_drawdown": 0.0,
        "best_positive_event_share": 1.0,
        "supportive_temporal_folds": 0,
        "catastrophic_transfer": False,
        "fold_results": {name: {"events": 0, "net_pnl": 0.0, "mean_net_pnl": 0.0} for name, *_ in VALIDATION_FOLDS},
        "one_bar_delay_net_pnl": 0.0,
        "event_gross_pnl": [],
        "event_net_pnl": [],
        "event_session_days": [],
        "finite": True,
        "hard_invalidation": False,
        "mll_proxy_safe": True,
        "status": "EXACT_REPLAY_COMPLETED",
    }


def _array_compare(
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
    raise ValueError(f"Unsupported comparison operator: {operator}")


def _scalar_compare(
    value: float, operator: ComparisonOperator, threshold: float
) -> bool:
    return bool(_array_compare(np.asarray([value]), operator, threshold)[0])


def _day(value: str) -> int:
    return int(np.datetime64(value, "D").astype(np.int64))


def _assert_same(
    reference: Sequence[Mapping[str, Any]], optimized: Sequence[Mapping[str, Any]]
) -> None:
    if len(reference) != len(optimized):
        raise AssertionError("Exact replay benchmark result counts differ.")
    for left, right in zip(reference, optimized, strict=True):
        if left != right:
            raise AssertionError(
                f"Exact replay output diverged for {left.get('candidate_id')}."
            )
