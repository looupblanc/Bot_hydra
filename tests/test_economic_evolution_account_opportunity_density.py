from __future__ import annotations

from pathlib import Path

import pytest

from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_opportunity_density import (
    OPPORTUNITY_DENSITY_LIMITS,
    OpportunityDensityPolicy,
    SignalObservation,
    generate_opportunity_density_population,
    route_opportunity_density_entry,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"


def _policy() -> OpportunityDensityPolicy:
    components = ("a", "b", "c", "d", "e", "f")
    graph = tuple(
        (component, (components[(index + 1) % len(components)],))
        for index, component in enumerate(components)
    )
    return OpportunityDensityPolicy(
        policy_id="density_test",
        component_ids=components,
        confirmation_sources=graph,
        **OPPORTUNITY_DENSITY_LIMITS,
    )


def _state() -> AccountDecisionState:
    return AccountDecisionState(
        balance=150_000.0,
        mll_floor=145_500.0,
        mll_buffer=4_500.0,
        daily_realized_pnl=0.0,
        consecutive_losing_days=0,
        remaining_target=9_000.0,
        open_exposures=(),
    )


def _intent() -> EntryIntent:
    return EntryIntent(
        event_id="event_a",
        component_id="a",
        market="ES",
        side=1,
        decision_ns=10_000_000_000_000,
        session_day=20240102,
        regime="RTH",
        base_quantity=1,
        base_mini_equivalent=1.0,
    )


def test_density_router_scales_only_from_past_different_market_confirmation() -> None:
    policy = _policy()
    intent = _intent()
    observation = SignalObservation(
        component_id="b",
        market="NQ",
        side=1,
        decision_ns=intent.decision_ns - 1,
    )
    decision = route_opportunity_density_entry(
        intent,
        _state(),
        policy=policy,
        signal_histories={"b": (observation,)},
    )
    assert decision.allow is True
    assert decision.quantity == 2
    assert decision.reason == "CROSS_MARKET_DENSITY_CONFIRMED_SCALE"


def test_density_router_rejects_future_observation() -> None:
    policy = _policy()
    intent = _intent()
    future = SignalObservation(
        component_id="b",
        market="NQ",
        side=1,
        decision_ns=intent.decision_ns + 1,
    )
    with pytest.raises(ValueError, match="future"):
        route_opportunity_density_entry(
            intent,
            _state(),
            policy=policy,
            signal_histories={"b": (future,)},
        )


def test_full_density_population_is_deterministic_and_degree_matched() -> None:
    seed = load_and_verify_seed_archive(SEED)
    first = generate_opportunity_density_population(
        seed,
        campaign_id="hydra_economic_evolution_opportunity_density_0013",
    )
    repeated = generate_opportunity_density_population(
        seed,
        campaign_id="hydra_economic_evolution_opportunity_density_0013",
    )
    assert first.manifest_hash == repeated.manifest_hash
    assert len(first.pairs) == 512
    assert len(first.components) == 48
    assert len({row.membership_hash for row in first.pairs}) == 512
    assert all(
        row.real_policy.source_degree_multiset
        == row.matched_control_policy.source_degree_multiset
        and row.real_policy.source_id_multiset
        == row.matched_control_policy.source_id_multiset
        and row.real_policy.confirmation_sources
        != row.matched_control_policy.confirmation_sources
        for row in first.pairs
    )
    assert first.summary()["outbound_order_capability"] is False
    assert first.summary()["status_inheritance"] is False
