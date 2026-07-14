from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_timeline import (
    ACCOUNT_TIMELINE_CLASS_ID,
    ACCOUNT_TIMELINE_LIMITS,
    AccountTimelinePolicy,
    AccountTimelinePolicyPair,
    AccountTimelinePopulation,
    generate_account_timeline_population,
    route_account_timeline_entry,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN_ID = "hydra_economic_evolution_account_timeline_0012"
COMPONENTS = tuple(f"component-{index}" for index in range(6))


def _seed() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/seeds/"
            "persistent_0003_successor_seed.json"
        ).read_text(encoding="utf-8")
    )


def _policy(
    policy_id: str,
    *,
    source_map: tuple[tuple[str, str], ...] | None = None,
) -> AccountTimelinePolicy:
    return AccountTimelinePolicy(
        policy_id=policy_id,
        component_ids=COMPONENTS,
        score_source_map=source_map
        or tuple((component, component) for component in COMPONENTS),
        **ACCOUNT_TIMELINE_LIMITS,
    )


def _state(
    **updates: object,
) -> AccountDecisionState:
    values: dict[str, object] = {
        "balance": 150_000.0,
        "mll_floor": 145_500.0,
        "mll_buffer": 4_500.0,
        "daily_realized_pnl": 0.0,
        "consecutive_losing_days": 0,
        "remaining_target": 9_000.0,
        "open_exposures": (),
    }
    values.update(updates)
    return AccountDecisionState(**values)  # type: ignore[arg-type]


def _intent(component: str = COMPONENTS[0]) -> EntryIntent:
    return EntryIntent(
        event_id=f"event-{component}",
        component_id=component,
        market="ES",
        side=1,
        decision_ns=100,
        session_day=0,
        regime="VOLATILITY_NORMAL",
        base_quantity=1,
        base_mini_equivalent=1.0,
    )


@pytest.fixture(scope="module")
def population() -> AccountTimelinePopulation:
    return generate_account_timeline_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=96,
    )


def test_router_uses_only_completed_timeline_and_symmetric_thresholds() -> None:
    policy = _policy("real")
    warmup = route_account_timeline_entry(
        _intent(),
        _state(),
        policy=policy,
        completed_outcomes={COMPONENTS[0]: (0.8, 0.7, 0.6)},
    )
    positive = route_account_timeline_entry(
        _intent(),
        _state(),
        policy=policy,
        completed_outcomes={COMPONENTS[0]: (0.8, 0.7, 0.6, 0.5)},
    )
    negative = route_account_timeline_entry(
        _intent(),
        _state(),
        policy=policy,
        completed_outcomes={COMPONENTS[0]: (-0.8, -0.7, -0.6, -0.5)},
    )

    assert warmup.allow and warmup.quantity == 1
    assert warmup.reason == "TIMELINE_WARMUP"
    assert positive.allow and positive.quantity == 2
    assert positive.reason == "POSITIVE_COMPLETED_TIMELINE_SCALE"
    assert not negative.allow
    assert negative.reason == "NEGATIVE_COMPLETED_TIMELINE_VETO"


def test_matched_control_reads_permuted_history_not_role_labels() -> None:
    real = _policy("real")
    rotated = tuple(
        (component, COMPONENTS[(index + 1) % len(COMPONENTS)])
        for index, component in enumerate(COMPONENTS)
    )
    control = _policy("control", source_map=rotated)
    pair = AccountTimelinePolicyPair("pair", real, control, "membership")
    histories = {
        COMPONENTS[0]: (0.8, 0.8, 0.8, 0.8),
        COMPONENTS[1]: (-0.8, -0.8, -0.8, -0.8),
    }

    assert route_account_timeline_entry(
        _intent(COMPONENTS[0]),
        _state(),
        policy=pair.real_policy,
        completed_outcomes=histories,
    ).allow
    assert not route_account_timeline_entry(
        _intent(COMPONENTS[0]),
        _state(),
        policy=pair.matched_control_policy,
        completed_outcomes=histories,
    ).allow
    assert "component_roles" not in pair.real_policy.to_dict()


def test_population_is_deterministic_unique_and_role_blind(
    population: AccountTimelinePopulation,
) -> None:
    repeated = generate_account_timeline_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=96,
    )
    summary = population.summary()
    assert population.manifest_hash == repeated.manifest_hash
    assert summary["class_id"] == ACCOUNT_TIMELINE_CLASS_ID
    assert summary["real_policy_count"] == 96
    assert summary["unique_membership_count"] == 96
    assert summary["policy_uses_role_labels"] is False
    assert summary["past_only_completed_shadow_outcomes"] is True
    assert len({row.real_policy.policy_id for row in population.pairs}) == 96
    assert len(
        {row.matched_control_policy.policy_id for row in population.pairs}
    ) == 96


def test_pairs_preserve_membership_history_multiset_and_limits(
    population: AccountTimelinePopulation,
) -> None:
    by_id = {row.sleeve.sleeve_id: row for row in population.components}
    for pair in population.pairs:
        real = pair.real_policy
        control = pair.matched_control_policy
        assert real.component_ids == control.component_ids
        assert real.score_sources == {
            component_id: component_id for component_id in real.component_ids
        }
        assert sorted(control.score_sources.values()) == sorted(real.component_ids)
        assert all(
            target != source for target, source in control.score_source_map
        )
        for key, expected in ACCOUNT_TIMELINE_LIMITS.items():
            assert getattr(real, key) == expected
            assert getattr(control, key) == expected
        members = [by_id[value] for value in real.component_ids]
        assert len({row.sleeve.market for row in members}) >= 3
        assert len({row.sleeve.session_code for row in members}) >= 3
        assert real.outbound_order_capability is False
        assert control.outbound_order_capability is False


def test_component_bank_is_positive_stressed_and_behaviorally_unique(
    population: AccountTimelinePopulation,
) -> None:
    assert len(population.components) == 48
    assert all(row.net_pnl > 0.0 for row in population.components)
    assert all(row.stressed_net_pnl > 0.0 for row in population.components)
    assert all(row.event_count >= 20 for row in population.components)
    assert len(
        {row.sleeve.behavioral_fingerprint for row in population.components}
    ) == len(population.components)


def test_generation_rejects_proof_or_status_inheritance() -> None:
    proof_seed = _seed()
    proof_seed["proof_window_consumed"] = True
    with pytest.raises(ValueError, match="proof-consuming"):
        generate_account_timeline_population(
            proof_seed,
            campaign_id=CAMPAIGN_ID,
            policy_pair_count=64,
        )

    status_seed = _seed()
    status_seed["governance"]["status_inheritance"] = True
    with pytest.raises(ValueError, match="inheritance"):
        generate_account_timeline_population(
            status_seed,
            campaign_id=CAMPAIGN_ID,
            policy_pair_count=64,
        )


def test_full_population_manifest_is_frozen_by_generator() -> None:
    population = generate_account_timeline_population(
        _seed(),
        campaign_id=CAMPAIGN_ID,
        policy_pair_count=512,
    )
    assert population.manifest_hash == (
        "8f4c7017c825aeda02fdc74b217015541cd777c25e558e6a1420378032c9e953"
    )
    assert population.summary()["same_ordered_membership_pair_count"] == 512
    assert population.summary()["same_history_source_multiset_pair_count"] == 512
