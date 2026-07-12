from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_fitness import combine_passer_fitness
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, evaluate_rolling_combine
from hydra.research.qd_economic_tournament import MARKET_PAIRS, _round_turn_cost_all
from hydra.research.rolling_combine_replay import ExactTradePath
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategyRole


VERSION = "hydra_mechanism_grammar_v6"
MINUTE_NS = 60_000_000_000
DEVELOPMENT_START_DAY = int(np.datetime64("2023-01-01", "D").astype(np.int64))
DEVELOPMENT_END_DAY = int(np.datetime64("2024-10-01", "D").astype(np.int64))
TRAINING_THRESHOLD_END_DAY = int(np.datetime64("2024-01-01", "D").astype(np.int64))
_WORKER_MATRICES: dict[str, FeatureMatrix] = {}


@dataclass(frozen=True, slots=True)
class MechanismCondition:
    feature: str
    operator: ComparisonOperator
    threshold: float

    def __post_init__(self) -> None:
        if not self.feature:
            raise ValueError("condition feature is required")
        if not isinstance(self.operator, ComparisonOperator):
            raise TypeError("condition operator is invalid")
        if not math.isfinite(self.threshold):
            raise ValueError("condition threshold must be finite")

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature": self.feature,
            "operator": int(self.operator),
            "threshold": self.threshold,
        }


@dataclass(frozen=True, slots=True)
class MechanismGraphSpec:
    candidate_id: str
    lineage_id: str
    mechanism_kind: str
    market: str
    timeframe_profile: str
    conditions: tuple[MechanismCondition, ...]
    minimum_conditions: int
    transition_required: bool
    side: int
    holding_events: int
    session_code: int
    quantity: int
    role: StrategyRole
    point_value: float
    round_turn_cost: float
    version: int = 1

    def __post_init__(self) -> None:
        if not self.candidate_id or not self.lineage_id or not self.mechanism_kind:
            raise ValueError("mechanism identity is incomplete")
        if self.market not in MARKET_PAIRS:
            raise ValueError("unknown mechanism market")
        if len(self.conditions) < 2:
            raise ValueError("V6 mechanism graphs require at least two conditions")
        if not 1 <= self.minimum_conditions <= len(self.conditions):
            raise ValueError("minimum_conditions is invalid")
        if self.side not in {-1, 1}:
            raise ValueError("side must be +/-1")
        if self.holding_events not in {5, 15, 30, 60}:
            raise ValueError("holding horizon is not supported")
        if self.session_code < -1 or self.session_code > 2:
            raise ValueError("session code is invalid")
        if self.quantity < 1 or self.quantity > 15:
            raise ValueError("quantity is invalid")

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(_structure_payload(self))

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["conditions"] = [condition.to_dict() for condition in self.conditions]
        row["role"] = int(self.role)
        row["structural_fingerprint"] = self.structural_fingerprint
        return row

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MechanismGraphSpec":
        return cls(
            candidate_id=str(value["candidate_id"]),
            lineage_id=str(value["lineage_id"]),
            mechanism_kind=str(value["mechanism_kind"]),
            market=str(value["market"]),
            timeframe_profile=str(value["timeframe_profile"]),
            conditions=tuple(
                MechanismCondition(
                    feature=str(row["feature"]),
                    operator=ComparisonOperator(int(row["operator"])),
                    threshold=float(row["threshold"]),
                )
                for row in value["conditions"]
            ),
            minimum_conditions=int(value["minimum_conditions"]),
            transition_required=bool(value["transition_required"]),
            side=int(value["side"]),
            holding_events=int(value["holding_events"]),
            session_code=int(value["session_code"]),
            quantity=int(value["quantity"]),
            role=StrategyRole(int(value["role"])),
            point_value=float(value["point_value"]),
            round_turn_cost=float(value["round_turn_cost"]),
            version=int(value.get("version") or 1),
        )


def generate_mechanism_population(
    matrices: Mapping[str, FeatureMatrix],
    *,
    count: int,
    generation_index: int,
    excluded_fingerprints: Iterable[str] = (),
) -> list[MechanismGraphSpec]:
    if count < 1:
        raise ValueError("count must be positive")
    excluded = set(excluded_fingerprints)
    output: list[MechanismGraphSpec] = []
    templates = (
        (
            "MULTITIMEFRAME_TRANSITION",
            ("past_return_60", "ctx_15m_return", "ctx_60m_volatility_expansion"),
            3,
            True,
            "1m|15m|60m",
        ),
        (
            "OPPORTUNITY_DENSITY_STATE",
            ("extreme_dwell", "past_participation", "directional_pressure_without_progress"),
            2,
            False,
            "1m|session",
        ),
        (
            "TARGET_BEFORE_MLL_HAZARD_STATE",
            ("failed_expansion", "past_volatility", "rv_short_long_ratio"),
            2,
            True,
            "1m|30m",
        ),
        (
            "TIMEFRAME_DISAGREEMENT",
            ("ctx_5m_return", "ctx_60m_return", "ctx_30m_volatility_expansion"),
            3,
            False,
            "1m|5m|30m|60m",
        ),
        (
            "PROFITABLE_INACTIVITY_FILTER",
            ("old_region_reentry", "shared_loss_risk_state", "ctx_60m_volatility_expansion"),
            3,
            False,
            "1m|60m|session",
        ),
        (
            "SESSION_SPECIALIST_TRANSITION",
            ("past_participation", "past_return_60", "ctx_15m_return"),
            2,
            True,
            "1m|15m|session",
        ),
    )
    quantiles = {
        market: _training_quantiles(matrix)
        for market, matrix in sorted(matrices.items())
    }
    ordinal = 0
    capacity = count * 12 + 5_000
    while len(output) < count and ordinal < capacity:
        market = sorted(matrices)[ordinal % len(matrices)]
        template = templates[(ordinal // len(matrices)) % len(templates)]
        kind, features, minimum, transition, timeframe = template
        side = 1 if (ordinal // (len(matrices) * len(templates))) % 2 == 0 else -1
        horizon = (5, 15, 30, 60)[(ordinal // 7) % 4]
        session = (-1, 0, 1, 2)[(ordinal // 11) % 4]
        variant = (ordinal // 13) % 3
        conditions: list[MechanismCondition] = []
        for index, feature in enumerate(features):
            low, median, high = quantiles[market][feature]
            if kind == "TIMEFRAME_DISAGREEMENT" and index < 2:
                operator = (
                    ComparisonOperator.GREATER_THAN
                    if (index == 0) == (side > 0)
                    else ComparisonOperator.LESS_THAN
                )
                threshold = 0.0
            elif kind == "PROFITABLE_INACTIVITY_FILTER" and index == 1:
                operator = ComparisonOperator.LESS_THAN
                threshold = (median, high, median)[variant]
            else:
                positive = (index + int(side < 0) + variant) % 2 == 0
                operator = (
                    ComparisonOperator.GREATER_THAN
                    if positive
                    else ComparisonOperator.LESS_THAN
                )
                threshold = (median, high, low)[variant]
            conditions.append(MechanismCondition(feature, operator, threshold))
        role = (
            StrategyRole.COMBINE_PASSER
            if kind
            not in {
                "PROFITABLE_INACTIVITY_FILTER",
                "TARGET_BEFORE_MLL_HAZARD_STATE",
            }
            else StrategyRole.DEFENSIVE
        )
        provisional = {
            "mechanism_kind": kind,
            "market": market,
            "timeframe_profile": timeframe,
            "minimum_conditions": minimum,
            "transition_required": transition,
            "side": side,
            "holding_events": horizon,
            "session_code": session,
            "quantity": 1,
            "point_value": instrument_spec(market).point_value,
            "round_turn_cost": _round_turn_cost_all(market),
            "version": 1,
        }
        fingerprint = stable_hash(
            {
                **provisional,
                "conditions": [row.to_dict() for row in conditions],
                "role": int(role),
            }
        )
        if fingerprint not in excluded:
            candidate = "strategy_v6_grammar_" + fingerprint[:20] + "_v1"
            output.append(
                MechanismGraphSpec(
                    candidate_id=candidate,
                    lineage_id="lineage_v6_grammar_" + fingerprint[20:40],
                    **provisional,
                    conditions=tuple(conditions),
                    role=role,
                )
            )
            excluded.add(fingerprint)
        ordinal += 1
    if len(output) < count:
        raise ValueError(
            f"new V6 grammar capacity exhausted at {len(output)} of {count}"
        )
    return output


def fast_screen_mechanism(
    spec: MechanismGraphSpec, matrix: FeatureMatrix
) -> dict[str, Any]:
    positions = signal_positions(spec, matrix)
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    gross = (
        forward[positions]
        * spec.side
        * spec.point_value
        * spec.quantity
    ) if len(positions) else np.asarray([], dtype=float)
    net = gross - spec.round_turn_cost * spec.quantity
    if len(net):
        equity = np.cumsum(net)
        peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:]
        drawdown = float(np.max(peak - equity, initial=0.0))
    else:
        drawdown = 0.0
    decisions = matrix.array("decision_ns")[positions]
    signature = hashlib.sha256(decisions[:256].tobytes()).hexdigest()
    return {
        "candidate_id": spec.candidate_id,
        "structural_fingerprint": spec.structural_fingerprint,
        "mechanism_kind": spec.mechanism_kind,
        "market": spec.market,
        "event_count": int(len(positions)),
        "net_pnl": float(np.sum(net)),
        "cost_stress_1_5x_net": float(
            np.sum(gross - 1.5 * spec.round_turn_cost * spec.quantity)
        ),
        "maximum_drawdown": drawdown,
        "event_signature": signature,
        "positive_after_costs": bool(len(net) >= 8 and float(np.sum(net)) > 0.0),
    }


def build_mechanism_trade_path(
    spec: MechanismGraphSpec, matrix: FeatureMatrix
) -> ExactTradePath:
    positions = signal_positions(spec, matrix)
    day_array = matrix.array("session_day")
    session = matrix.array("session_code")
    selected = (
        (day_array >= DEVELOPMENT_START_DAY)
        & (day_array < DEVELOPMENT_END_DAY)
        & (session >= 0)
    )
    eligible_days = tuple(sorted({int(value) for value in day_array[selected]}))
    regimes = _past_only_regimes(matrix, selected)
    entry_prices = matrix.array("entry_price")
    highs = matrix.array("bar_high")
    lows = matrix.array("bar_low")
    timestamp = matrix.array("timestamp_ns")
    decisions = matrix.array("decision_ns")
    segments = matrix.array("segment_code")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    cost = spec.round_turn_cost * spec.quantity
    events: list[TradePathEvent] = []
    for ordinal, raw in enumerate(positions):
        position = int(raw)
        entry_index = position + 1
        exit_index = position + spec.holding_events + 1
        if exit_index >= matrix.row_count:
            continue
        if int(segments[entry_index]) != int(segments[exit_index]):
            continue
        entry = float(entry_prices[position])
        high = float(np.max(highs[entry_index : exit_index + 1]))
        low = float(np.min(lows[entry_index : exit_index + 1]))
        adverse = low if spec.side > 0 else high
        favorable = high if spec.side > 0 else low
        gross = float(forward[position]) * spec.side * spec.point_value * spec.quantity
        event_day = int(day_array[position])
        events.append(
            TradePathEvent(
                event_id=f"{spec.candidate_id}:{ordinal:05d}:{int(decisions[position])}",
                decision_ns=int(decisions[position]),
                exit_ns=int(timestamp[exit_index]) + MINUTE_NS,
                session_day=event_day,
                net_pnl=float(gross - cost),
                gross_pnl=float(gross),
                worst_unrealized_pnl=float(
                    (adverse - entry)
                    * spec.side
                    * spec.point_value
                    * spec.quantity
                    - cost
                ),
                best_unrealized_pnl=float(
                    (favorable - entry)
                    * spec.side
                    * spec.point_value
                    * spec.quantity
                    - cost
                ),
                quantity=spec.quantity,
                mini_equivalent=float(spec.quantity),
                regime=regimes.get(event_day, "UNKNOWN"),
                session_compliant=True,
                contract_limit_compliant=spec.quantity <= 15,
                same_bar_ambiguous=False,
            )
        )
    values = np.asarray([event.net_pnl for event in events], dtype=float)
    gross_values = np.asarray([event.gross_pnl for event in events], dtype=float)
    if len(values):
        equity = np.cumsum(values)
        peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:]
        drawdown = float(np.max(peak - equity, initial=0.0))
        positive = values[values > 0]
        positive_sum = float(positive.sum())
        best_share = float(positive.max() / positive_sum) if positive_sum else 1.0
    else:
        drawdown = 0.0
        best_share = 1.0
    return ExactTradePath(
        candidate_id=spec.candidate_id,
        event_count=len(events),
        gross_pnl=float(gross_values.sum()),
        net_pnl=float(values.sum()),
        cost_stress_1_5x_net=float(
            np.sum(gross_values - 1.5 * cost)
        ),
        maximum_drawdown=drawdown,
        best_positive_event_share=best_share,
        eligible_session_days=eligible_days,
        day_regimes=regimes,
        fold_results=_fold_results(events),
        events=tuple(events),
    )


def run_mechanism_exact_job(payload: Mapping[str, Any]) -> dict[str, Any]:
    matrix_path = str(payload["matrix_path"])
    matrix = _WORKER_MATRICES.get(matrix_path)
    if matrix is None:
        matrix = FeatureMatrix.open(matrix_path, mmap=True)
        _WORKER_MATRICES[matrix_path] = matrix
    spec = MechanismGraphSpec.from_dict(payload["specification"])
    path = build_mechanism_trade_path(spec, matrix)
    rolling = evaluate_rolling_combine(
        path.events,
        path.eligible_session_days,
        day_regimes=path.day_regimes,
        policy=EpisodeStartPolicy(
            maximum_starts=int(payload.get("maximum_episode_starts") or 24),
            minimum_spacing_sessions=5,
            minimum_observation_sessions=30,
            maximum_duration_sessions=60,
            regime_balanced=True,
        ),
    )
    fitness = combine_passer_fitness(
        rolling,
        cost_stress_net_pnl=path.cost_stress_1_5x_net,
        complexity=float(len(spec.conditions) + int(spec.transition_required)),
    )
    return {
        "candidate_id": spec.candidate_id,
        "specification": spec.to_dict(),
        "exact_trade_path": path.metrics(),
        "rolling_combine": rolling.to_dict(include_daily_paths=False),
        "combine_fitness": fitness.to_dict(),
        "hard_invalidation": False,
        "outbound_order_capability": False,
        "q4_access_count_delta": 0,
    }


def signal_positions(
    spec: MechanismGraphSpec, matrix: FeatureMatrix
) -> np.ndarray:
    day = matrix.array("session_day")
    session = matrix.array("session_code")
    segment = matrix.array("segment_code")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    base = (
        (day >= DEVELOPMENT_START_DAY)
        & (day < DEVELOPMENT_END_DAY)
        & np.isfinite(forward)
        & (session >= 0)
    )
    if spec.session_code >= 0:
        base &= session == spec.session_code
    votes = np.zeros(matrix.row_count, dtype=np.int16)
    all_conditions = np.ones(matrix.row_count, dtype=bool)
    for condition in spec.conditions:
        values = matrix.array(f"feature__{condition.feature}")
        condition_mask = np.isfinite(values) & _compare(
            values, condition.operator, condition.threshold
        )
        votes += condition_mask
        all_conditions &= condition_mask
    mask = base & (
        votes >= spec.minimum_conditions
        if spec.minimum_conditions < len(spec.conditions)
        else all_conditions
    )
    if spec.transition_required:
        previous = np.roll(mask, 1)
        previous[0] = False
        previous[1:] &= segment[1:] == segment[:-1]
        mask &= ~previous
    candidates = np.flatnonzero(mask)
    if not len(candidates):
        return candidates.astype(np.int64)
    decisions = matrix.array("decision_ns")
    retained: list[int] = []
    last_segment = -1
    last_exit = np.iinfo(np.int64).min
    hold_ns = (spec.holding_events + 1) * MINUTE_NS
    for raw in candidates:
        index = int(raw)
        current_segment = int(segment[index])
        if current_segment != last_segment:
            last_segment = current_segment
            last_exit = np.iinfo(np.int64).min
        decision = int(decisions[index])
        if decision < last_exit:
            continue
        retained.append(index)
        last_exit = decision + hold_ns
    return np.asarray(retained, dtype=np.int64)


def _training_quantiles(matrix: FeatureMatrix) -> dict[str, tuple[float, float, float]]:
    day = matrix.array("session_day")
    mask = (day >= DEVELOPMENT_START_DAY) & (day < TRAINING_THRESHOLD_END_DAY)
    output: dict[str, tuple[float, float, float]] = {}
    features = (
        "past_return_60",
        "ctx_15m_return",
        "ctx_60m_volatility_expansion",
        "extreme_dwell",
        "past_participation",
        "directional_pressure_without_progress",
        "failed_expansion",
        "past_volatility",
        "rv_short_long_ratio",
        "ctx_5m_return",
        "ctx_60m_return",
        "ctx_30m_volatility_expansion",
        "old_region_reentry",
        "shared_loss_risk_state",
    )
    for feature in features:
        values = matrix.array(f"feature__{feature}")[mask]
        values = values[np.isfinite(values)]
        if not len(values):
            output[feature] = (-1.0, 0.0, 1.0)
        else:
            output[feature] = tuple(
                float(value) for value in np.percentile(values, (35, 50, 65))
            )
    return output


def _past_only_regimes(
    matrix: FeatureMatrix, selected: np.ndarray
) -> dict[int, str]:
    days = matrix.array("session_day")[selected]
    values = matrix.array("feature__ctx_60m_volatility_expansion")[selected]
    completed: dict[int, str] = {}
    for day in sorted({int(value) for value in days}):
        sample = values[days == day]
        sample = sample[np.isfinite(sample)]
        median = float(np.median(sample)) if len(sample) else 1.0
        completed[day] = (
            "VOLATILITY_EXPANSION"
            if median >= 1.20
            else "VOLATILITY_CONTRACTION"
            if median <= 0.80
            else "VOLATILITY_NORMAL"
        )
    output: dict[int, str] = {}
    previous = "UNKNOWN"
    for day in sorted(completed):
        output[day] = previous
        previous = completed[day]
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
        low = int(np.datetime64(start, "D").astype(np.int64))
        high = int(np.datetime64(end, "D").astype(np.int64))
        values = [event.net_pnl for event in events if low <= event.session_day < high]
        output[name] = {
            "events": len(values),
            "net_pnl": float(sum(values)),
            "mean_net_pnl": float(np.mean(values)) if values else 0.0,
        }
    return output


def _structure_payload(spec: MechanismGraphSpec) -> dict[str, Any]:
    row = {
        "mechanism_kind": spec.mechanism_kind,
        "market": spec.market,
        "timeframe_profile": spec.timeframe_profile,
        "conditions": [condition.to_dict() for condition in spec.conditions],
        "minimum_conditions": spec.minimum_conditions,
        "transition_required": spec.transition_required,
        "side": spec.side,
        "holding_events": spec.holding_events,
        "session_code": spec.session_code,
        "quantity": spec.quantity,
        "role": int(spec.role),
        "point_value": spec.point_value,
        "round_turn_cost": spec.round_turn_cost,
        "version": spec.version,
    }
    return row


def _compare(
    values: np.ndarray, operator: ComparisonOperator, threshold: float
) -> np.ndarray:
    if operator is ComparisonOperator.GREATER_THAN:
        return values > threshold
    if operator is ComparisonOperator.GREATER_EQUAL:
        return values >= threshold
    if operator is ComparisonOperator.LESS_THAN:
        return values < threshold
    if operator is ComparisonOperator.LESS_EQUAL:
        return values <= threshold
    raise ValueError("unsupported comparison operator")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


__all__ = [
    "MechanismCondition",
    "MechanismGraphSpec",
    "build_mechanism_trade_path",
    "fast_screen_mechanism",
    "generate_mechanism_population",
    "run_mechanism_exact_job",
    "signal_positions",
]
