from __future__ import annotations

from hydra.economic_evolution.policy_evolution import (
    generate_failure_directed_policy_population,
)
from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    EconomicRole,
    FailureDimension,
    SleeveSpec,
)


def _sleeve(index: int) -> SleeveSpec:
    return SleeveSpec(
        sleeve_id=f"sleeve-{index}",
        component_ids=(f"component-{index}",),
        market=("ES", "NQ", "CL", "GC", "YM")[index % 5],
        execution_market=("MES", "MNQ", "MCL", "MGC", "MYM")[index % 5],
        timeframe=("1m", "5m", "15m")[index % 3],
        session_code=(-1, 0, 1, 2)[index % 4],
        trigger_feature="past_return_60",
        trigger_operator="GT",
        trigger_quantile=0.75,
        context_feature=None,
        context_operator=None,
        context_quantile=None,
        side=(-1, 1)[index % 2],
        holding_bars=(5, 15, 30, 60)[index % 4],
        exit_style="TIME_ONLY",
        role=(
            EconomicRole.PRIMARY_ALPHA,
            EconomicRole.SESSION_DIVERSIFIER,
            EconomicRole.MARKET_DIVERSIFIER,
        )[index % 3],
        source_campaign="seed",
        lineage_id=f"lineage-{index}",
    )


def _parent(index: int) -> AccountPolicyGenome:
    return AccountPolicyGenome(
        policy_id=f"parent-{index}",
        sleeve_ids=(f"sleeve-{index}", f"sleeve-{index + 1}"),
        allocation_units=(1, 1),
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=6,
        conflict_policy="FIXED_PRIORITY",
        daily_risk_budget=1_250.0,
        daily_profit_lock=2_250.0,
        low_mll_buffer=3_000.0,
        critical_mll_buffer=1_125.0,
        loss_streak_throttle_after=3,
        mode="COMBINE_RESEARCH",
        source_campaign="seed",
    )


def test_failure_directed_population_is_deterministic_and_account_only() -> None:
    sleeves = tuple(_sleeve(index) for index in range(8))
    parents = (_parent(0), _parent(2))
    failures = {
        row.policy_id: FailureDimension.LONG_RECOVERY_TIME for row in parents
    }

    first = generate_failure_directed_policy_population(
        parents,
        failures,
        sleeves,
        campaign_id="policy-evolution-test",
        count=20,
    )
    second = generate_failure_directed_policy_population(
        parents,
        failures,
        sleeves,
        campaign_id="policy-evolution-test",
        count=20,
    )

    assert first == second
    assert first.children
    assert first.manifest_hash == second.manifest_hash
    assert all(row.inherited_status is None for row in first.children)
    assert all(row.identical_episode_starts_required for row in first.children)
    assert all(row.child_policy.parent_policy_ids for row in first.children)
    assert all(row.child_policy.mutation_target is FailureDimension.LONG_RECOVERY_TIME for row in first.children)
    # Sleeve specifications are referenced, never modified.
    assert {row.sleeve_id for row in sleeves} == {f"sleeve-{i}" for i in range(8)}


def test_null_or_execution_failure_is_not_parameter_repaired() -> None:
    sleeves = tuple(_sleeve(index) for index in range(4))
    parent = _parent(0)
    result = generate_failure_directed_policy_population(
        (parent,),
        {parent.policy_id: FailureDimension.NULL_INDISTINGUISHABLE},
        sleeves,
        campaign_id="no-repair-test",
        count=4,
    )

    assert result.children == ()
    assert result.no_effect_rejection_count >= 4
