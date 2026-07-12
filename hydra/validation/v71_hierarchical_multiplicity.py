from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Mapping, Sequence

import numpy as np

from hydra.validation.v7_phase2_multiplicity import benjamini_hochberg


@dataclass(frozen=True, slots=True)
class HierarchicalTrialAccounting:
    raw_global_trials: int
    raw_family_candidates: int
    effective_signal_trials: float
    campaign_inflated_trials: int
    global_search_history_penalty: int
    prior_family_grammar_penalty: int
    DSR_N_trials: int

    def to_dict(self) -> dict[str, int | float]:
        return asdict(self)


def hierarchical_trial_accounting(
    signal_paths: Sequence[Sequence[float]] | np.ndarray,
    *,
    raw_global_trials: int,
    prior_family_grammar_versions: int = 0,
) -> HierarchicalTrialAccounting:
    matrix = np.asarray(signal_paths, dtype=np.float64)
    if matrix.ndim != 2 or matrix.shape[0] < 1 or matrix.shape[1] < 2:
        raise ValueError("signal path matrix must be candidates x observations")
    if raw_global_trials < 1 or prior_family_grammar_versions < 0:
        raise ValueError("invalid hierarchical trial counters")
    centered = matrix - np.mean(matrix, axis=1, keepdims=True)
    norms = np.linalg.norm(centered, axis=1)
    normalized = np.divide(
        centered,
        norms[:, None],
        out=np.zeros_like(centered),
        where=norms[:, None] > 0.0,
    )
    correlation = normalized @ normalized.T
    zero = norms <= 0.0
    correlation[zero, :] = 0.0
    correlation[:, zero] = 0.0
    np.fill_diagonal(correlation, 1.0)
    eigenvalues = np.clip(np.linalg.eigvalsh(correlation), 0.0, None)
    denominator = float(np.sum(eigenvalues * eigenvalues))
    effective = (
        float(np.sum(eigenvalues) ** 2 / denominator)
        if denominator > 0.0
        else 1.0
    )
    effective = min(max(effective, 1.0), float(matrix.shape[0]))
    inflated = int(math.ceil(1.5 * effective))
    global_penalty = int(math.ceil(math.log2(1.0 + raw_global_trials)))
    dsr_trials = max(
        2,
        inflated + global_penalty + int(prior_family_grammar_versions),
    )
    return HierarchicalTrialAccounting(
        raw_global_trials=int(raw_global_trials),
        raw_family_candidates=int(matrix.shape[0]),
        effective_signal_trials=effective,
        campaign_inflated_trials=inflated,
        global_search_history_penalty=global_penalty,
        prior_family_grammar_penalty=int(prior_family_grammar_versions),
        DSR_N_trials=dsr_trials,
    )


def family_bh(
    p_values_by_family: Mapping[str, Mapping[str, float]], *, q: float = 0.10
) -> dict[str, dict[str, dict[str, float | int | bool]]]:
    if not p_values_by_family:
        raise ValueError("family BH requires at least one family")
    return {
        str(family): benjamini_hochberg(values, q=q)
        for family, values in sorted(p_values_by_family.items())
    }


__all__ = [
    "HierarchicalTrialAccounting",
    "family_bh",
    "hierarchical_trial_accounting",
]
