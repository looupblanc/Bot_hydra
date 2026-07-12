from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np

from hydra.strategies.turbo_batch_fingerprint import batch_fingerprints
from hydra.strategies.turbo_dsl import StrategySpec


def _readonly(array: np.ndarray) -> np.ndarray:
    array.setflags(write=False)
    return array


@dataclass(frozen=True, slots=True)
class CompiledStrategyBatch:
    """Structure-of-arrays encoding consumed without Python objects in hot loops."""

    candidate_ids: tuple[str, ...]
    fingerprints: tuple[str, ...]
    feature_indices: np.ndarray
    operator_codes: np.ndarray
    thresholds: np.ndarray
    context_feature_indices: np.ndarray
    context_operator_codes: np.ndarray
    context_thresholds: np.ndarray
    sides: np.ndarray
    horizon_indices: np.ndarray
    point_values: np.ndarray
    round_turn_costs: np.ndarray
    quantities: np.ndarray
    session_codes: np.ndarray
    role_codes: np.ndarray

    def __len__(self) -> int:
        return len(self.candidate_ids)


def compile_strategy_batch(
    specs: Sequence[StrategySpec],
    feature_names: Sequence[str],
    holding_horizons: Sequence[int],
) -> CompiledStrategyBatch:
    """Compile immutable specifications once into compact NumPy arrays."""

    if len(set(feature_names)) != len(feature_names):
        raise ValueError("feature_names must be unique")
    if len(set(holding_horizons)) != len(holding_horizons):
        raise ValueError("holding_horizons must be unique")

    feature_lookup = {name: index for index, name in enumerate(feature_names)}
    horizon_lookup = {value: index for index, value in enumerate(holding_horizons)}
    missing_features = sorted(
        {
            feature
            for spec in specs
            for feature in (spec.feature, spec.context_feature)
            if feature is not None and feature not in feature_lookup
        }
    )
    if missing_features:
        raise ValueError(f"unknown features: {', '.join(missing_features)}")
    missing_horizons = sorted(
        {spec.holding_events for spec in specs if spec.holding_events not in horizon_lookup}
    )
    if missing_horizons:
        raise ValueError(f"unknown holding horizons: {missing_horizons}")

    context_indices = [
        -1 if spec.context_feature is None else feature_lookup[spec.context_feature]
        for spec in specs
    ]
    context_operators = [
        0 if spec.context_operator is None else int(spec.context_operator) for spec in specs
    ]
    context_thresholds = [
        np.nan if spec.context_threshold is None else spec.context_threshold for spec in specs
    ]

    return CompiledStrategyBatch(
        candidate_ids=tuple(spec.candidate_id for spec in specs),
        fingerprints=tuple(str(value) for value in batch_fingerprints(specs)),
        feature_indices=_readonly(
            np.asarray([feature_lookup[spec.feature] for spec in specs], dtype=np.int16)
        ),
        operator_codes=_readonly(
            np.asarray([int(spec.operator) for spec in specs], dtype=np.int8)
        ),
        thresholds=_readonly(np.asarray([spec.threshold for spec in specs], dtype=np.float64)),
        context_feature_indices=_readonly(np.asarray(context_indices, dtype=np.int16)),
        context_operator_codes=_readonly(np.asarray(context_operators, dtype=np.int8)),
        context_thresholds=_readonly(np.asarray(context_thresholds, dtype=np.float64)),
        sides=_readonly(np.asarray([spec.side for spec in specs], dtype=np.int8)),
        horizon_indices=_readonly(
            np.asarray([horizon_lookup[spec.holding_events] for spec in specs], dtype=np.int16)
        ),
        point_values=_readonly(
            np.asarray([spec.point_value for spec in specs], dtype=np.float64)
        ),
        round_turn_costs=_readonly(
            np.asarray([spec.round_turn_cost for spec in specs], dtype=np.float64)
        ),
        quantities=_readonly(np.asarray([spec.quantity for spec in specs], dtype=np.int16)),
        session_codes=_readonly(
            np.asarray([spec.session_code for spec in specs], dtype=np.int16)
        ),
        role_codes=_readonly(np.asarray([int(spec.role) for spec in specs], dtype=np.int8)),
    )


__all__ = ["CompiledStrategyBatch", "compile_strategy_batch"]
