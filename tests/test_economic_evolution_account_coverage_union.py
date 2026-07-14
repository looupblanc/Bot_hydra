from __future__ import annotations

from pathlib import Path

from hydra.account_policy.router import AccountDecisionState, EntryIntent
from hydra.economic_evolution.account_coverage_union import (
    COVERAGE_UNION_LIMITS,
    CoverageUnionPolicy,
    generate_coverage_union_population,
    route_coverage_union_entry,
)
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "reports/economic_evolution/seeds/persistent_0003_successor_seed.json"


def _policy() -> CoverageUnionPolicy:
    return CoverageUnionPolicy(
        policy_id="coverage_test",
        component_ids=tuple(f"c{index}" for index in range(10)),
        **COVERAGE_UNION_LIMITS,
    )


def test_coverage_union_routes_one_unit_with_frozen_account_guards() -> None:
    policy = _policy()
    state = AccountDecisionState(
        balance=150_000.0,
        mll_floor=145_500.0,
        mll_buffer=4_500.0,
        daily_realized_pnl=0.0,
        consecutive_losing_days=0,
        remaining_target=9_000.0,
        open_exposures=(),
    )
    intent = EntryIntent(
        event_id="event",
        component_id="c0",
        market="ES",
        side=1,
        decision_ns=1,
        session_day=20240102,
        regime="RTH",
        base_quantity=1,
        base_mini_equivalent=1.0,
    )
    decision = route_coverage_union_entry(intent, state, policy=policy)
    assert decision.allow is True
    assert decision.quantity == 1
    assert decision.reason == "STATIC_COVERAGE_UNION_ACCEPT"
    assert policy.controller_id == policy.policy_id


def test_coverage_population_is_structurally_distinct_and_event_matched() -> None:
    seed = load_and_verify_seed_archive(SEED)
    first = generate_coverage_union_population(
        seed,
        campaign_id="coverage_test_campaign",
        policy_pair_count=64,
    )
    repeated = generate_coverage_union_population(
        seed,
        campaign_id="coverage_test_campaign",
        policy_pair_count=64,
    )
    assert first.manifest_hash == repeated.manifest_hash
    assert len(first.pairs) == 64
    assert len({row.real_policy.structural_fingerprint for row in first.pairs}) == 64
    assert all(
        row.real_market_count >= 4
        and row.real_session_count == 4
        and row.control_market_count <= 2
        and abs(row.real_source_event_count - row.control_source_event_count)
        / row.real_source_event_count
        <= 0.15
        for row in first.pairs
    )
    assert first.summary()["outbound_order_capability"] is False
    assert first.summary()["per_signal_risk_units"] == 1
