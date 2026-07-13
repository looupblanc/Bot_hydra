from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.economic_evolution.archive import (
    ArchiveEntry,
    BehavioralDescriptor,
    ParetoObjectives,
    ParetoQualityDiversityArchive,
    dominates,
)
from hydra.economic_evolution.incremental_value import (
    IncrementalValuePolicy,
    MatchedAccountObservation,
    evaluate_incremental_value,
)
from hydra.economic_evolution.schema import EconomicRole


POLICY = IncrementalValuePolicy(
    minimum_matched_starts=8,
    minimum_independent_blocks=4,
    minimum_stressed_net_uplift=50.0,
    minimum_target_progress_uplift=0.02,
    minimum_mll_breach_reduction=0.10,
    minimum_consistency_uplift=0.10,
    minimum_shared_loss_day_reduction=1.0,
    maximum_net_sacrifice_for_defensive_role=100.0,
    maximum_cost_increase=50.0,
    minimum_positive_block_fraction=0.50,
)


def _observations(*, included: bool, defensive: bool = False) -> tuple[MatchedAccountObservation, ...]:
    output = []
    for index in range(8):
        block = f"block_{index // 2}"
        baseline_net = 100.0 + index
        if defensive:
            net = baseline_net - 25.0 if included else baseline_net
            breached = False if included else index < 4
            shared_loss = 1 if included else 3
            progress = 0.20 if included else 0.21
        else:
            net = baseline_net + 100.0 if included else baseline_net
            breached = False
            shared_loss = 1
            progress = 0.35 if included else 0.25
        output.append(
            MatchedAccountObservation(
                start_id=f"start_{index}",
                block_id=block,
                net_after_costs=net,
                stressed_net_after_costs=net - 20.0,
                target_progress=progress,
                mll_breached=breached,
                consistency_ok=included or index % 2 == 0,
                shared_loss_days=shared_loss,
                conflict_count=1,
                total_cost=100.0 + (10.0 if included else 0.0),
            )
        )
    return tuple(output)


def test_incremental_alpha_is_useful_without_becoming_validated() -> None:
    result = evaluate_incremental_value(
        "component_alpha",
        EconomicRole.TARGET_ACCELERATOR,
        _observations(included=False),
        _observations(included=True),
        policy=POLICY,
    )
    assert result.status == "MICRO_EDGE_USEFUL"
    assert result.validated is False
    assert result.inherited_status is False
    assert result.median_stressed_net_uplift == 100.0
    assert result.median_target_progress_uplift == pytest.approx(0.10)
    assert result.independent_block_count == 4


def test_defensive_component_can_trade_small_net_for_mll_uplift() -> None:
    result = evaluate_incremental_value(
        "component_defensive",
        EconomicRole.MLL_STABILIZER,
        _observations(included=False, defensive=True),
        _observations(included=True, defensive=True),
        policy=POLICY,
    )
    assert result.status == "MICRO_EDGE_USEFUL"
    assert result.median_stressed_net_uplift == -25.0
    assert result.mll_breach_reduction == 0.5
    assert result.median_shared_loss_day_reduction == 2.0


def test_incremental_comparison_requires_identical_starts_and_blocks() -> None:
    included = list(_observations(included=True))
    included[-1] = replace(included[-1], start_id="different")
    with pytest.raises(ValueError, match="identical starts"):
        evaluate_incremental_value(
            "component",
            EconomicRole.PRIMARY_ALPHA,
            _observations(included=False),
            included,
            policy=POLICY,
        )


def _descriptor(market: str = "ES", role: str = "PRIMARY_ALPHA") -> BehavioralDescriptor:
    return BehavioralDescriptor(
        market=market,
        session="OPEN",
        timeframe="1m|15m",
        direction_balance="BALANCED",
        trade_frequency="MEDIUM",
        holding_horizon="30m",
        volatility_regime="NORMAL",
        trend_range_behavior="MIXED",
        pnl_skew="POSITIVE",
        drawdown_shape="SHALLOW",
        loss_clustering="LOW",
        target_velocity="MEDIUM",
        mll_usage="LOW",
        cost_sensitivity="LOW",
        correlation_cluster="cluster_1",
        account_role=role,
    )


def _objectives(net: float, progress: float, breach: float) -> ParetoObjectives:
    return ParetoObjectives(
        stressed_net_pnl=net,
        target_progress=progress,
        target_velocity=progress,
        combine_pass_rate_diagnostic=0.10,
        mll_breach_rate=breach,
        consistency_rate=0.80,
        xfa_survival_rate=0.70,
        expected_payouts=1.0,
        total_cost=100.0,
        complexity=2.0,
    )


def test_pareto_archive_rejects_dominated_but_keeps_feasible_niches() -> None:
    archive = ParetoQualityDiversityArchive(maximum_per_niche=2)
    strong = ArchiveEntry(
        "strong",
        "family_a",
        "lineage_a",
        _descriptor(),
        _objectives(1_000.0, 0.60, 0.10),
        {},
    )
    weak = ArchiveEntry(
        "weak",
        "family_b",
        "lineage_b",
        _descriptor(),
        _objectives(500.0, 0.30, 0.20),
        {},
    )
    diversifier = ArchiveEntry(
        "diversifier",
        "family_c",
        "lineage_c",
        _descriptor(market="CL", role="MARKET_DIVERSIFIER"),
        _objectives(300.0, 0.20, 0.00),
        {},
    )
    assert dominates(strong.objectives, weak.objectives)
    assert archive.insert(strong).accepted
    assert not archive.insert(weak).accepted
    assert archive.insert(diversifier).accepted
    assert {row.policy_id for row in archive.entries} == {"strong", "diversifier"}
    summary = archive.summary()
    assert summary["niche_count"] == 2
    assert summary["markets"] == {"CL": 1, "ES": 1}
