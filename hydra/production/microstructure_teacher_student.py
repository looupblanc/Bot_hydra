"""Chronological MBO teacher labels and deployable L1/L2 students for 0031.

Teacher states may use a later markout to decide whether a high-information
book event was economically useful.  Students never receive that markout or
MBO-only fields at inference time.  Thresholds and models are fitted on the
discovery role, calibrated on validation, and evaluated once on the frozen
final-development role.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_score, recall_score

from hydra.economic_evolution.schema import stable_hash


TEACHER_FAMILIES = (
    "ABSORPTION",
    "DEPLETION",
    "LIQUIDITY_VACUUM",
    "EXHAUSTION",
    "QUEUE_STATE",
)
DEPLOYABILITY_STATUSES = (
    "L1_DEPLOYABLE",
    "L2_DEPLOYABLE",
    "MBO_TEACHER_ONLY",
    "UNDEPLOYABLE",
)
ROLE_DISCOVERY = "DISCOVERY"
ROLE_VALIDATION = "VALIDATION"
ROLE_FINAL = "FINAL_DEVELOPMENT"

L1_FEATURES = (
    "aggressor_delta",
    "signed_volume",
    "trade_arrival_rate",
    "notional_rate",
    "bbo_imbalance",
    "microprice_deviation",
    "spread_ticks",
    "vwap_distance",
    "vwap_slope",
    "event_volatility",
    "cross_market_flow_alignment",
    "cross_market_microprice_divergence",
)
L2_FEATURES = L1_FEATURES + (
    "depth_3_imbalance",
    "depth_5_imbalance",
    "depth_10_imbalance",
    "depth_slope",
    "depth_convexity",
    "depletion_rate",
    "replenishment_rate",
    "liquidity_gap_ticks",
)
MBO_ONLY_FEATURES = (
    "queue_persistence",
    "order_age_mean",
    "cancel_ahead_rate",
    "cancel_behind_rate",
    "quantity_ahead",
    "ephemeral_liquidity_rate",
)


class TeacherStudentError(RuntimeError):
    """Teacher/student evidence is incomplete, leaky, or temporally invalid."""


@dataclass(frozen=True, slots=True)
class FeatureTable:
    names: tuple[str, ...]
    values: np.ndarray
    decision_ns: np.ndarray
    available_ns: np.ndarray
    roles: np.ndarray
    market: np.ndarray
    future_markout: np.ndarray
    favorable_before_adverse: np.ndarray

    def __post_init__(self) -> None:
        count = len(self.decision_ns)
        if self.values.shape != (count, len(self.names)):
            raise TeacherStudentError("feature table shape drift")
        if any(
            len(value) != count
            for value in (
                self.available_ns,
                self.roles,
                self.market,
                self.future_markout,
                self.favorable_before_adverse,
            )
        ):
            raise TeacherStudentError("feature table inventory drift")
        if np.any(self.available_ns > self.decision_ns):
            raise TeacherStudentError("decision feature is not yet available")
        role_set = set(str(value) for value in np.unique(self.roles))
        if role_set != {ROLE_DISCOVERY, ROLE_VALIDATION, ROLE_FINAL}:
            raise TeacherStudentError("chronological roles are incomplete")
        forbidden = {"future_markout", "favorable_before_adverse", "outcome"}
        if forbidden.intersection(self.names):
            raise TeacherStudentError("outcome labels entered the feature table")


@dataclass(frozen=True, slots=True)
class TeacherLabelSet:
    labels: Mapping[str, np.ndarray]
    discovery_thresholds: Mapping[str, float]
    counts_by_role: Mapping[str, Mapping[str, int]]
    label_hash: str


@dataclass(frozen=True, slots=True)
class FrozenStudent:
    teacher_family: str
    tier: str
    feature_names: tuple[str, ...]
    coefficients: tuple[float, ...]
    intercept: float
    threshold: float
    model_hash: str

    @property
    def deployability_status(self) -> str:
        return f"{self.tier}_DEPLOYABLE"


@dataclass(frozen=True, slots=True)
class StudentResult:
    student: FrozenStudent
    validation: Mapping[str, float]
    final_development: Mapping[str, float]
    useful_final_economics: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "student": asdict(self.student),
            "validation": dict(self.validation),
            "final_development": dict(self.final_development),
            "useful_final_economics": self.useful_final_economics,
        }


def build_mbo_teacher_labels(table: FeatureTable) -> TeacherLabelSet:
    """Create bounded teacher states with thresholds learned on discovery only."""

    required = {
        "aggressor_delta",
        "trade_arrival_rate",
        "price_response_per_signed_contract",
        "depletion_rate",
        "replenishment_rate",
        "microprice_deviation",
        "liquidity_gap_ticks",
        "depth_withdrawal_rate",
        "queue_persistence",
        "quantity_ahead",
        "order_age_mean",
    }
    index = {name: offset for offset, name in enumerate(table.names)}
    missing = required - set(index)
    if missing:
        raise TeacherStudentError(f"MBO teacher features absent: {sorted(missing)}")
    discovery = table.roles == ROLE_DISCOVERY
    if int(discovery.sum()) < 50:
        raise TeacherStudentError("teacher discovery role is too small")

    def column(name: str) -> np.ndarray:
        value = np.asarray(table.values[:, index[name]], dtype=float)
        return np.nan_to_num(value, nan=0.0, posinf=0.0, neginf=0.0)

    thresholds: dict[str, float] = {}
    for name, quantile in (
        ("aggressor_delta_abs_high", 0.80),
        ("arrival_high", 0.80),
        ("response_low", 0.35),
        ("depletion_high", 0.80),
        ("replenishment_high", 0.75),
        ("microprice_high", 0.75),
        ("gap_high", 0.80),
        ("withdrawal_high", 0.80),
        ("queue_persistence_high", 0.70),
        ("quantity_ahead_low", 0.35),
        ("order_age_high", 0.70),
    ):
        source = {
            "aggressor_delta_abs_high": np.abs(column("aggressor_delta")),
            "arrival_high": column("trade_arrival_rate"),
            "response_low": np.abs(column("price_response_per_signed_contract")),
            "depletion_high": column("depletion_rate"),
            "replenishment_high": column("replenishment_rate"),
            "microprice_high": np.abs(column("microprice_deviation")),
            "gap_high": column("liquidity_gap_ticks"),
            "withdrawal_high": column("depth_withdrawal_rate"),
            "queue_persistence_high": column("queue_persistence"),
            "quantity_ahead_low": column("quantity_ahead"),
            "order_age_high": column("order_age_mean"),
        }[name]
        thresholds[name] = float(np.quantile(source[discovery], quantile))

    aggression = column("aggressor_delta")
    aggression_abs = np.abs(aggression)
    response = np.abs(column("price_response_per_signed_contract"))
    depletion = column("depletion_rate")
    replenishment = column("replenishment_rate")
    microprice = np.abs(column("microprice_deviation"))
    gap = column("liquidity_gap_ticks")
    withdrawal = column("depth_withdrawal_rate")
    queue = column("queue_persistence")
    quantity_ahead = column("quantity_ahead")
    order_age = column("order_age_mean")
    arrival = column("trade_arrival_rate")
    markout = np.asarray(table.future_markout, dtype=float)
    finite_outcome = np.isfinite(markout)
    # Economic confirmation is evaluated only after the causal state exists.
    opposite_or_neutral = markout * np.sign(aggression) <= 0
    continuation = markout * np.sign(aggression + column("microprice_deviation")) > 0

    labels = {
        "ABSORPTION": (
            (aggression_abs >= thresholds["aggressor_delta_abs_high"])
            & (response <= thresholds["response_low"])
            & (replenishment >= thresholds["replenishment_high"])
            & (queue >= thresholds["queue_persistence_high"])
            & opposite_or_neutral
            & finite_outcome
        ),
        "DEPLETION": (
            (depletion >= thresholds["depletion_high"])
            & (replenishment < thresholds["replenishment_high"])
            & (microprice >= thresholds["microprice_high"])
            & continuation
            & finite_outcome
        ),
        "LIQUIDITY_VACUUM": (
            (withdrawal >= thresholds["withdrawal_high"])
            & (gap >= thresholds["gap_high"])
            & (microprice >= thresholds["microprice_high"])
            & continuation
            & finite_outcome
        ),
        "EXHAUSTION": (
            (aggression_abs >= thresholds["aggressor_delta_abs_high"])
            & (arrival >= thresholds["arrival_high"])
            & (response <= thresholds["response_low"])
            & opposite_or_neutral
            & finite_outcome
        ),
        "QUEUE_STATE": (
            (quantity_ahead <= thresholds["quantity_ahead_low"])
            & (queue >= thresholds["queue_persistence_high"])
            & (order_age >= thresholds["order_age_high"])
            & np.asarray(table.favorable_before_adverse, dtype=bool)
            & finite_outcome
        ),
    }
    label_arrays = {key: np.asarray(value, dtype=bool) for key, value in labels.items()}
    counts = {
        family: {
            role: int(np.sum(values & (table.roles == role)))
            for role in (ROLE_DISCOVERY, ROLE_VALIDATION, ROLE_FINAL)
        }
        for family, values in label_arrays.items()
    }
    label_hash = stable_hash(
        {
            "thresholds": thresholds,
            "counts": counts,
            "rows": len(table.decision_ns),
            "first_decision_ns": int(table.decision_ns[0]),
            "last_decision_ns": int(table.decision_ns[-1]),
        }
    )
    return TeacherLabelSet(
        labels=label_arrays,
        discovery_thresholds=thresholds,
        counts_by_role=counts,
        label_hash=label_hash,
    )


def train_deployable_students(
    table: FeatureTable,
    teachers: TeacherLabelSet,
    *,
    minimum_final_opportunities: int = 1,
) -> tuple[StudentResult, ...]:
    """Fit L1/L2 logistic students and freeze thresholds on validation only."""

    results: list[StudentResult] = []
    discovery = table.roles == ROLE_DISCOVERY
    validation = table.roles == ROLE_VALIDATION
    final = table.roles == ROLE_FINAL
    for family in TEACHER_FAMILIES:
        labels = np.asarray(teachers.labels[family], dtype=np.int8)
        if len(np.unique(labels[discovery])) < 2:
            continue
        for tier, whitelist in (("L1", L1_FEATURES), ("L2", L2_FEATURES)):
            feature_names = tuple(name for name in whitelist if name in table.names)
            if len(feature_names) < 3:
                continue
            feature_index = [table.names.index(name) for name in feature_names]
            values = np.asarray(table.values[:, feature_index], dtype=float)
            values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
            mean = values[discovery].mean(axis=0)
            scale = values[discovery].std(axis=0)
            scale[scale <= 1e-12] = 1.0
            normalized = (values - mean) / scale
            model = LogisticRegression(
                C=0.25,
                solver="liblinear",
                class_weight="balanced",
                random_state=31_031,
                max_iter=500,
            )
            model.fit(normalized[discovery], labels[discovery])
            probabilities = model.predict_proba(normalized)[:, 1]
            threshold = _validation_threshold(
                probabilities[validation],
                labels[validation],
                np.asarray(table.future_markout[validation], dtype=float),
            )
            frozen = FrozenStudent(
                teacher_family=family,
                tier=tier,
                feature_names=feature_names,
                coefficients=tuple(float(v) for v in model.coef_[0] / scale),
                intercept=float(
                    model.intercept_[0] - np.sum(model.coef_[0] * mean / scale)
                ),
                threshold=threshold,
                model_hash="",
            )
            payload = asdict(frozen)
            payload.pop("model_hash")
            frozen = FrozenStudent(**payload, model_hash=stable_hash(payload))
            validation_metrics = _metrics(
                labels[validation], probabilities[validation], threshold,
                table.future_markout[validation],
            )
            final_metrics = _metrics(
                labels[final], probabilities[final], threshold,
                table.future_markout[final],
            )
            useful = (
                int(final_metrics["predicted_positive_count"]) >= minimum_final_opportunities
                and float(final_metrics["economic_markout_mean"]) > 0.0
                and float(final_metrics["precision"]) > float(final_metrics["base_rate"])
            )
            results.append(
                StudentResult(
                    student=frozen,
                    validation=validation_metrics,
                    final_development=final_metrics,
                    useful_final_economics=useful,
                )
            )
    return tuple(results)


def score_frozen_student(
    student: FrozenStudent,
    *,
    feature_names: Sequence[str],
    values: np.ndarray,
) -> np.ndarray:
    """Evaluate an exported student without MBO teacher fields or outcomes."""

    if student.tier not in {"L1", "L2"}:
        raise TeacherStudentError("only deployable students may be scored")
    allowed = set(L1_FEATURES if student.tier == "L1" else L2_FEATURES)
    if not set(student.feature_names) <= allowed or set(student.feature_names) & set(MBO_ONLY_FEATURES):
        raise TeacherStudentError("student contains an undeployable feature")
    indexes = [tuple(feature_names).index(name) for name in student.feature_names]
    matrix = np.nan_to_num(np.asarray(values[:, indexes], dtype=float))
    logits = matrix @ np.asarray(student.coefficients) + float(student.intercept)
    logits = np.clip(logits, -40.0, 40.0)
    return 1.0 / (1.0 + np.exp(-logits))


def _validation_threshold(
    probabilities: np.ndarray, labels: np.ndarray, markouts: np.ndarray
) -> float:
    candidates = (0.50, 0.60, 0.70, 0.80)
    ranked: list[tuple[float, float]] = []
    for threshold in candidates:
        mask = probabilities >= threshold
        economic = float(np.nanmean(markouts[mask])) if np.any(mask) else -math.inf
        precision = float(np.mean(labels[mask])) if np.any(mask) else 0.0
        ranked.append((economic + precision, threshold))
    return max(ranked, key=lambda value: (value[0], value[1]))[1]


def _metrics(
    labels: np.ndarray,
    probabilities: np.ndarray,
    threshold: float,
    markouts: np.ndarray,
) -> dict[str, float]:
    labels = np.asarray(labels, dtype=np.int8)
    probabilities = np.asarray(probabilities, dtype=float)
    predicted = probabilities >= threshold
    finite = np.isfinite(markouts)
    selected = predicted & finite
    brier = float(np.mean((probabilities - labels) ** 2)) if len(labels) else math.nan
    ece = _expected_calibration_error(labels, probabilities)
    return {
        "row_count": float(len(labels)),
        "positive_count": float(labels.sum()),
        "predicted_positive_count": float(predicted.sum()),
        "base_rate": float(labels.mean()) if len(labels) else math.nan,
        "precision": float(precision_score(labels, predicted, zero_division=0)),
        "recall": float(recall_score(labels, predicted, zero_division=0)),
        "brier_score": brier,
        "expected_calibration_error": ece,
        "economic_markout_mean": (
            float(np.mean(np.asarray(markouts)[selected])) if np.any(selected) else 0.0
        ),
    }


def _expected_calibration_error(labels: np.ndarray, probabilities: np.ndarray) -> float:
    bins = np.linspace(0.0, 1.0, 11)
    total = max(1, len(labels))
    value = 0.0
    for low, high in zip(bins[:-1], bins[1:], strict=True):
        mask = (probabilities >= low) & (
            (probabilities <= high) if high == 1.0 else (probabilities < high)
        )
        if np.any(mask):
            value += (mask.sum() / total) * abs(
                float(probabilities[mask].mean()) - float(labels[mask].mean())
            )
    return float(value)
