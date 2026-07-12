"""Temporally evaluated compute-prioritisation model for Turbo Foundry.

This module is intentionally unable to validate or promote a strategy.  Its
only output is a bounded compute allocation containing an explicit pure-
exploration lane.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler


META_SCREEN_VERSION = "hydra_turbo_meta_screen_v1"
MINIMUM_EXPLORATION_SHARE = 0.20


class MetaScreenError(ValueError):
    pass


@dataclass(frozen=True)
class MetaScreenFit:
    schema: str
    feature_names: tuple[str, ...]
    scaler: StandardScaler
    base_model: LogisticRegression
    calibrator: LogisticRegression | None
    train_count: int
    calibration_count: int
    oos_count: int
    oos_start_index: int
    oos_brier_score: float
    oos_roc_auc: float | None
    oos_recall_at_half_budget: float
    calibration_bins: tuple[dict[str, float | int], ...]
    strategy_evidence: bool = False
    may_validate_or_promote: bool = False

    def predict_proba(self, rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
        matrix = _matrix(rows, self.feature_names)
        scaled = self.scaler.transform(matrix)
        base_logit = self.base_model.decision_function(scaled).reshape(-1, 1)
        if self.calibrator is None:
            return self.base_model.predict_proba(scaled)[:, 1]
        return self.calibrator.predict_proba(base_logit)[:, 1]

    def report(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "feature_names": list(self.feature_names),
            "temporal_split": {
                "train_count": self.train_count,
                "calibration_count": self.calibration_count,
                "oos_count": self.oos_count,
                "oos_start_index": self.oos_start_index,
            },
            "oos": {
                "brier_score": self.oos_brier_score,
                "roc_auc": self.oos_roc_auc,
                "survivor_recall_at_half_budget": self.oos_recall_at_half_budget,
                "calibration_bins": list(self.calibration_bins),
            },
            "interpretation_boundary": {
                "strategy_evidence": False,
                "may_validate_or_promote": False,
                "allocation_only": True,
            },
        }


@dataclass(frozen=True)
class PrioritizationDecision:
    candidate_id: str
    predicted_stage1_probability: float
    lane: str
    rank: int
    strategy_evidence: bool = False


@dataclass(frozen=True)
class MetaScreenAllocation:
    schema: str
    selected: tuple[PrioritizationDecision, ...]
    capacity: int
    exploration_count: int
    exploitation_count: int
    exploration_share: float
    strategy_evidence: bool = False
    may_validate_or_promote: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "capacity": self.capacity,
            "exploration_count": self.exploration_count,
            "exploitation_count": self.exploitation_count,
            "exploration_share": self.exploration_share,
            "strategy_evidence": False,
            "may_validate_or_promote": False,
            "selected": [decision.__dict__ for decision in self.selected],
        }


def fit_temporal_meta_screen(
    rows: Sequence[Mapping[str, Any]],
    *,
    feature_names: Sequence[str],
    target_name: str = "stage1_success",
    minimum_rows: int = 50,
) -> MetaScreenFit:
    """Fit on the past, calibrate later, and report on the latest untouched rows."""

    if len(rows) < minimum_rows:
        raise MetaScreenError(
            f"at least {minimum_rows} temporally ordered rows are required"
        )
    names = tuple(str(name) for name in feature_names)
    if not names or len(names) != len(set(names)):
        raise MetaScreenError("feature_names must be non-empty and unique")
    matrix = _matrix(rows, names)
    targets = np.asarray([int(row[target_name]) for row in rows], dtype=np.int8)
    if not set(np.unique(targets)).issubset({0, 1}):
        raise MetaScreenError("target must be binary")

    count = len(rows)
    train_end = max(20, math.floor(count * 0.60))
    calibration_end = max(train_end + 10, math.floor(count * 0.80))
    if calibration_end >= count:
        raise MetaScreenError("temporal partitions are too small")
    partitions = (
        targets[:train_end],
        targets[train_end:calibration_end],
        targets[calibration_end:],
    )
    if any(len(np.unique(partition)) < 2 for partition in partitions[:2]):
        raise MetaScreenError("training and calibration partitions need both classes")

    scaler = StandardScaler().fit(matrix[:train_end])
    scaled_train = scaler.transform(matrix[:train_end])
    base = LogisticRegression(
        C=0.25,
        class_weight="balanced",
        max_iter=1_000,
        random_state=0,
        solver="lbfgs",
    ).fit(scaled_train, targets[:train_end])

    calibration_logits = base.decision_function(
        scaler.transform(matrix[train_end:calibration_end])
    ).reshape(-1, 1)
    calibrator = LogisticRegression(
        C=1.0,
        class_weight=None,
        max_iter=1_000,
        random_state=0,
        solver="lbfgs",
    ).fit(calibration_logits, targets[train_end:calibration_end])

    oos_matrix = scaler.transform(matrix[calibration_end:])
    oos_logits = base.decision_function(oos_matrix).reshape(-1, 1)
    probabilities = calibrator.predict_proba(oos_logits)[:, 1]
    oos_targets = targets[calibration_end:]
    brier = float(brier_score_loss(oos_targets, probabilities))
    auc = (
        float(roc_auc_score(oos_targets, probabilities))
        if len(np.unique(oos_targets)) == 2
        else None
    )
    recall = _recall_at_budget(oos_targets, probabilities, share=0.50)
    return MetaScreenFit(
        schema=META_SCREEN_VERSION,
        feature_names=names,
        scaler=scaler,
        base_model=base,
        calibrator=calibrator,
        train_count=train_end,
        calibration_count=calibration_end - train_end,
        oos_count=count - calibration_end,
        oos_start_index=calibration_end,
        oos_brier_score=brier,
        oos_roc_auc=auc,
        oos_recall_at_half_budget=recall,
        calibration_bins=_calibration_bins(oos_targets, probabilities),
    )


def prioritize_with_exploration(
    candidates: Sequence[Mapping[str, Any]],
    *,
    fitted: MetaScreenFit,
    capacity: int,
    exploration_share: float = MINIMUM_EXPLORATION_SHARE,
    seed: int = 0,
) -> MetaScreenAllocation:
    """Allocate compute while reserving at least twenty percent for exploration."""

    if not candidates:
        raise MetaScreenError("candidate universe is empty")
    identifiers = [str(row.get("candidate_id") or "") for row in candidates]
    if "" in identifiers or len(identifiers) != len(set(identifiers)):
        raise MetaScreenError("candidate IDs must be present and unique")
    if not 0 < capacity <= len(candidates):
        raise MetaScreenError("capacity must be within the candidate universe")
    if exploration_share < MINIMUM_EXPLORATION_SHARE or exploration_share >= 1.0:
        raise MetaScreenError("exploration_share must be at least 0.20 and below one")

    probabilities = fitted.predict_proba(candidates)
    ranked = sorted(
        range(len(candidates)),
        key=lambda index: (-float(probabilities[index]), identifiers[index]),
    )
    exploration_count = min(capacity, max(1, math.ceil(capacity * exploration_share)))
    exploitation_count = capacity - exploration_count
    exploitation = ranked[:exploitation_count]
    exploitation_set = set(exploitation)
    remainder = [index for index in range(len(candidates)) if index not in exploitation_set]
    # A stable hash gives reproducible exploration without depending on global RNG
    # state or candidate input ordering.
    exploration_ranked = sorted(
        remainder,
        key=lambda index: _exploration_key(seed, identifiers[index]),
    )
    exploration = exploration_ranked[:exploration_count]
    decisions: list[PrioritizationDecision] = []
    for rank, index in enumerate(exploitation, start=1):
        decisions.append(
            PrioritizationDecision(
                candidate_id=identifiers[index],
                predicted_stage1_probability=float(probabilities[index]),
                lane="META_PRIORITY",
                rank=rank,
            )
        )
    for rank, index in enumerate(exploration, start=1):
        decisions.append(
            PrioritizationDecision(
                candidate_id=identifiers[index],
                predicted_stage1_probability=float(probabilities[index]),
                lane="PURE_EXPLORATION",
                rank=rank,
            )
        )
    actual_share = exploration_count / capacity
    return MetaScreenAllocation(
        schema=META_SCREEN_VERSION,
        selected=tuple(decisions),
        capacity=capacity,
        exploration_count=exploration_count,
        exploitation_count=exploitation_count,
        exploration_share=float(actual_share),
    )


def _matrix(
    rows: Sequence[Mapping[str, Any]], feature_names: Sequence[str]
) -> np.ndarray:
    try:
        matrix = np.asarray(
            [[float(row[name]) for name in feature_names] for row in rows],
            dtype=np.float64,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise MetaScreenError("meta-screen features must be complete numeric values") from exc
    if matrix.ndim != 2 or not np.isfinite(matrix).all():
        raise MetaScreenError("meta-screen features must be finite")
    return matrix


def _calibration_bins(
    targets: np.ndarray, probabilities: np.ndarray, *, bin_count: int = 5
) -> tuple[dict[str, float | int], ...]:
    bins: list[dict[str, float | int]] = []
    boundaries = np.linspace(0.0, 1.0, bin_count + 1)
    for index in range(bin_count):
        lower = boundaries[index]
        upper = boundaries[index + 1]
        include = (probabilities >= lower) & (
            probabilities <= upper if index == bin_count - 1 else probabilities < upper
        )
        if not np.any(include):
            continue
        bins.append(
            {
                "lower": float(lower),
                "upper": float(upper),
                "count": int(np.sum(include)),
                "mean_prediction": float(np.mean(probabilities[include])),
                "observed_rate": float(np.mean(targets[include])),
            }
        )
    return tuple(bins)


def _recall_at_budget(
    targets: np.ndarray, probabilities: np.ndarray, *, share: float
) -> float:
    positives = int(np.sum(targets))
    if positives == 0:
        return 0.0
    count = max(1, math.ceil(len(targets) * share))
    selected = np.argsort(-probabilities, kind="stable")[:count]
    return float(np.sum(targets[selected]) / positives)


def _exploration_key(seed: int, candidate_id: str) -> str:
    return hashlib.sha256(f"{seed}:{candidate_id}".encode("utf-8")).hexdigest()
