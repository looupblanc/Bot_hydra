from __future__ import annotations

from hydra.economic_evolution.density_diversification import (
    DENSITY_CLASS_ID,
    generate_density_diversification_population,
)
from hydra.economic_evolution.schema import EconomicRole, SleeveSpec


def _seed() -> dict:
    markets = (("ES", "MES"), ("NQ", "MNQ"), ("YM", "MYM"))
    roles = (
        EconomicRole.PRIMARY_ALPHA,
        EconomicRole.CONSISTENCY_SMOOTHER,
        EconomicRole.SESSION_DIVERSIFIER,
        EconomicRole.MLL_STABILIZER,
    )
    features = (
        "past_return_60",
        "past_volatility",
        "failed_expansion",
        "old_region_reentry",
    )
    rows = []
    for market_index, (market, execution) in enumerate(markets):
        for session in range(4):
            source = SleeveSpec(
                sleeve_id=f"source_{market}_{session}",
                component_ids=(f"component_{market}_{session}",),
                market=market,
                execution_market=execution,
                timeframe="1m",
                session_code=(-1, 0, 1, 2)[session],
                trigger_feature=features[session],
                trigger_operator="GT",
                trigger_quantile=(0.65, 0.75, 0.85, 0.65)[session],
                context_feature=None,
                context_operator=None,
                context_quantile=None,
                side=(-1, 1)[(market_index + session) % 2],
                holding_bars=(5, 15, 30, 60)[session],
                exit_style="TIME_ONLY",
                role=roles[session],
                source_campaign="development_seed",
                lineage_id=f"lineage_{market}_{session}",
            )
            rows.append(
                {
                    "specification": source.to_dict(),
                    "development_evidence": {
                        "net_pnl": 500.0 + market_index * 100 + session * 10,
                        "cost_stress_1_5x_net": 350.0 + market_index * 50 + session * 5,
                        "events": 40 + session,
                        "incremental_status": (
                            "MICRO_EDGE_USEFUL" if session % 2 == 0 else "COMPONENT_RESEARCH_ONLY"
                        ),
                    },
                }
            )
    return {
        "schema": "hydra_economic_evolution_seed_archive_v1",
        "development_only": True,
        "proof_window_consumed": False,
        "governance": {"status_inheritance": False},
        "sleeves": rows,
    }


def test_density_population_is_deterministic_new_and_matched() -> None:
    kwargs = {
        "campaign_id": "density-0007-test",
        "excluded_source_sleeve_ids": ("source_ES_0",),
        "maximum_sources": 9,
        "maximum_sources_per_market": 3,
        "maximum_sources_per_market_session": 1,
        "maximum_sources_per_market_mechanism": 1,
        "policy_count": 20,
    }
    first = generate_density_diversification_population(_seed(), **kwargs)
    second = generate_density_diversification_population(_seed(), **kwargs)

    assert first.candidate_manifest_hash == second.candidate_manifest_hash
    assert first.policies == second.policies
    assert len(first.sources) == 9
    assert len(first.real_sleeves) == len(first.matched_null_sleeves) == 9
    assert len(first.policies) == 20
    assert all(row.source.sleeve_id != "source_ES_0" for row in first.sources)
    assert all(row.inherited_status is None for row in first.real_sleeves)
    assert all(row.inherited_status is None for row in first.policies)

    source_behavior = {row.source.behavioral_fingerprint for row in first.sources}
    assert not source_behavior.intersection(
        row.behavioral_fingerprint for row in first.real_sleeves
    )
    real_by_source = dict(first.source_by_candidate)
    assert set(real_by_source) == {
        row.sleeve_id for row in (*first.real_sleeves, *first.matched_null_sleeves)
    }
    assert {row.context_operator for row in first.real_sleeves} == {"GT"}
    assert {row.context_operator for row in first.matched_null_sleeves} == {"LT"}
    assert {row.context_quantile for row in first.real_sleeves} == {0.65}
    assert {row.context_quantile for row in first.matched_null_sleeves} == {0.35}
    assert all(
        not set(policy.sleeve_ids).intersection(
            row.sleeve_id for row in first.matched_null_sleeves
        )
        for policy in first.policies
    )


def test_density_policies_are_bounded_and_behaviorally_diverse() -> None:
    population = generate_density_diversification_population(
        _seed(),
        campaign_id="density-0007-policy-test",
        excluded_source_sleeve_ids=(),
        maximum_sources=9,
        maximum_sources_per_market=3,
        maximum_sources_per_market_session=1,
        maximum_sources_per_market_mechanism=1,
        policy_count=30,
    )
    sleeves = {row.sleeve_id: row for row in population.real_sleeves}
    archetypes = dict(population.policy_archetypes)

    assert set(archetypes.values()) == {
        "EQUAL_RISK_DISPERSED",
        "TARGET_VELOCITY_TILTED",
        "CONSISTENCY_TILTED",
    }
    assert len({row.structural_fingerprint for row in population.policies}) == 30
    for policy in population.policies:
        members = [sleeves[value] for value in policy.sleeve_ids]
        assert 2 <= len(members) <= 4
        assert len({row.market for row in members}) >= 2
        assert len({row.role for row in members}) >= 2
        assert policy.conflict_policy == "FIXED_PRIORITY"
        assert max(policy.allocation_units) <= 2
        assert policy.maximum_mini_equivalent <= 10
        assert policy.mode == "COMBINE_RESEARCH"


def test_density_components_are_new_typed_class_components() -> None:
    population = generate_density_diversification_population(
        _seed(),
        campaign_id="density-0007-component-test",
        excluded_source_sleeve_ids=(),
        maximum_sources=6,
        maximum_sources_per_market=2,
        maximum_sources_per_market_session=1,
        maximum_sources_per_market_mechanism=1,
        policy_count=8,
    )
    assert population.components
    assert {row.mechanism_family for row in population.components} == {DENSITY_CLASS_ID}
    assert all(row.source_campaign == "density-0007-component-test" for row in population.components)
    assert all(row.parent_component_ids for row in population.components)
    assert all(row.inherited_status is None for row in population.components)
    assert population.summary()["validated"] is False


def test_density_seed_must_be_development_only_and_unburned() -> None:
    seed = _seed()
    seed["proof_window_consumed"] = True
    try:
        generate_density_diversification_population(
            seed,
            campaign_id="density-invalid",
            excluded_source_sleeve_ids=(),
            policy_count=4,
        )
    except ValueError as exc:
        assert "proof-consuming" in str(exc)
    else:
        raise AssertionError("proof-consuming seed was accepted")
