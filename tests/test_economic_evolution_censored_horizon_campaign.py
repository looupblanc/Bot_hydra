from __future__ import annotations

import hydra.research.economic_evolution_elite_robustness_campaign as base
from hydra.economic_evolution.account_censored_horizon import (
    CENSORED_HORIZON_CLASS_ID,
    generate_censored_horizon_population,
)
from hydra.economic_evolution.account_censored_horizon_evaluation import (
    evaluate_censored_horizon_policy_pairs,
)
from hydra.research.economic_evolution_censored_horizon_campaign import (
    CENSORED_HORIZON_ENGINE_VERSION,
    _patched_censored_horizon_campaign,
)


def test_censored_horizon_campaign_patch_is_bounded_and_restored() -> None:
    prior = (
        base.ELITE_ROBUSTNESS_CLASS_ID,
        base.ELITE_ROBUSTNESS_ENGINE_VERSION,
        base.generate_elite_robustness_population,
        base.evaluate_elite_robustness_policy_pairs,
    )
    with _patched_censored_horizon_campaign():
        assert base.ELITE_ROBUSTNESS_CLASS_ID == CENSORED_HORIZON_CLASS_ID
        assert base.ELITE_ROBUSTNESS_ENGINE_VERSION == CENSORED_HORIZON_ENGINE_VERSION
        assert (
            base.generate_elite_robustness_population
            is generate_censored_horizon_population
        )
        assert (
            base.evaluate_elite_robustness_policy_pairs
            is evaluate_censored_horizon_policy_pairs
        )
    assert (
        base.ELITE_ROBUSTNESS_CLASS_ID,
        base.ELITE_ROBUSTNESS_ENGINE_VERSION,
        base.generate_elite_robustness_population,
        base.evaluate_elite_robustness_policy_pairs,
    ) == prior
