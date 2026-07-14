from __future__ import annotations

import hydra.economic_evolution.account_elite_robustness_evaluation as evaluation
import hydra.research.economic_evolution_elite_robustness_campaign as base
from hydra.economic_evolution.account_loss_streak_buffer_ratchet import (
    LOSS_STREAK_BUFFER_RATCHET_CLASS_ID,
    generate_loss_streak_buffer_ratchet_population,
    route_loss_streak_buffer_ratchet_entry,
)
from hydra.research.economic_evolution_loss_streak_buffer_ratchet_campaign import (
    LOSS_STREAK_BUFFER_RATCHET_ENGINE_VERSION,
    _patched_ratchet_campaign,
)


def test_ratchet_campaign_patch_is_bounded_and_restored() -> None:
    prior = (
        base.ELITE_ROBUSTNESS_CLASS_ID,
        base.ELITE_ROBUSTNESS_ENGINE_VERSION,
        base.generate_elite_robustness_population,
        evaluation.route_elite_robustness_entry,
    )
    with _patched_ratchet_campaign():
        assert base.ELITE_ROBUSTNESS_CLASS_ID == LOSS_STREAK_BUFFER_RATCHET_CLASS_ID
        assert (
            base.ELITE_ROBUSTNESS_ENGINE_VERSION
            == LOSS_STREAK_BUFFER_RATCHET_ENGINE_VERSION
        )
        assert (
            base.generate_elite_robustness_population
            is generate_loss_streak_buffer_ratchet_population
        )
        assert (
            evaluation.route_elite_robustness_entry
            is route_loss_streak_buffer_ratchet_entry
        )
    assert (
        base.ELITE_ROBUSTNESS_CLASS_ID,
        base.ELITE_ROBUSTNESS_ENGINE_VERSION,
        base.generate_elite_robustness_population,
        evaluation.route_elite_robustness_entry,
    ) == prior
