from __future__ import annotations

from dataclasses import dataclass

import hydra.economic_evolution.account_elite_robustness_evaluation as evaluation
import hydra.research.economic_evolution_elite_robustness_campaign as base
from hydra.economic_evolution.account_loss_streak_buffer_ratchet import (
    LOSS_STREAK_BUFFER_RATCHET_CLASS_ID,
    generate_loss_streak_buffer_ratchet_population,
    route_loss_streak_buffer_ratchet_entry,
)
from hydra.economic_evolution.account_loss_streak_buffer_ratchet_evaluation import (
    LOSS_STREAK_BUFFER_RATCHET_EVALUATION_VERSION,
    evaluate_loss_streak_buffer_ratchet_policy_pairs,
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
        base.evaluate_elite_robustness_policy_pairs,
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
            base.evaluate_elite_robustness_policy_pairs
            is evaluate_loss_streak_buffer_ratchet_policy_pairs
        )
        assert (
            evaluation.route_elite_robustness_entry
            is route_loss_streak_buffer_ratchet_entry
        )
    assert (
        base.ELITE_ROBUSTNESS_CLASS_ID,
        base.ELITE_ROBUSTNESS_ENGINE_VERSION,
        base.generate_elite_robustness_population,
        base.evaluate_elite_robustness_policy_pairs,
        evaluation.route_elite_robustness_entry,
    ) == prior


@dataclass(frozen=True)
class _FakePolicy:
    policy_id: str


@dataclass(frozen=True)
class _FakePair:
    pair_id: str
    parent_policy_id: str
    real_policy: _FakePolicy
    matched_control_policy: _FakePolicy


def test_ratchet_evaluator_bounds_real_policy_memory_batches(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_evaluate(policies, *_args, **_kwargs):
        ids = tuple(row.policy_id for row in policies)
        calls.append(ids)
        return {value: {"policy_id": value} for value in ids}

    def fake_pair_result(pair, **_kwargs):
        return {"pair_id": pair.pair_id}

    monkeypatch.setattr(evaluation, "_evaluate_unique_policies", fake_evaluate)
    monkeypatch.setattr(evaluation, "_pair_result", fake_pair_result)
    controls = (_FakePolicy("parent-0"), _FakePolicy("parent-1"))
    pairs = tuple(
        _FakePair(
            f"pair-{index}",
            controls[index % 2].policy_id,
            _FakePolicy(f"real-{index}"),
            controls[index % 2],
        )
        for index in range(5)
    )
    rows = evaluate_loss_streak_buffer_ratchet_policy_pairs(  # type: ignore[arg-type]
        pairs,
        {},
        starts=(1, 2),
        episode_policy=object(),  # type: ignore[arg-type]
        worker_count=3,
        real_policy_batch_size=2,
    )
    assert calls == [
        ("parent-0", "parent-1"),
        ("real-0", "real-1"),
        ("real-2", "real-3"),
        ("real-4",),
    ]
    assert len(rows) == 5
    assert {row["unique_control_evaluation_count"] for row in rows} == {2}
    assert {row["memory_bounded_real_policy_batch_size"] for row in rows} == {2}
    assert {row["execution_policy_version"] for row in rows} == {
        LOSS_STREAK_BUFFER_RATCHET_EVALUATION_VERSION
    }
