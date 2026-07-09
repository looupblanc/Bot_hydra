from __future__ import annotations

import copy

import numpy as np

from hydra.strategies.dsl import StrategyCandidate


def mutate_candidate(candidate: StrategyCandidate, seed: int) -> StrategyCandidate:
    rng = np.random.default_rng(seed)
    params = copy.deepcopy(candidate.parameters)
    for key, value in list(params.items()):
        if isinstance(value, (int, float)):
            params[key] = float(value) * float(rng.uniform(0.9, 1.1))
    risk = dict(candidate.risk_parameters)
    risk["risk_scale"] = float(risk.get("risk_scale", 1.0)) * float(rng.uniform(0.75, 1.0))
    return StrategyCandidate(
        candidate_id=f"{candidate.candidate_id}_mut{seed % 100000}",
        family=candidate.family,
        symbol=candidate.symbol,
        timeframe=candidate.timeframe,
        parameters=params,
        entry_logic=candidate.entry_logic,
        exit_logic=candidate.exit_logic,
        risk_parameters=risk,
        parent_candidate_id=candidate.candidate_id,
        mutation_type="local_parameter_jitter",
    )
