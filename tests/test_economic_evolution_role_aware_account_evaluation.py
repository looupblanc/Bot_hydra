from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.account_policy.schema import BasketPolicy
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    evaluate_compiled_account_policy,
)
from hydra.economic_evolution.role_aware_account import (
    generate_role_aware_account_population,
)
from hydra.economic_evolution.role_aware_account_evaluation import (
    RoleAwareBasketPolicy,
    compile_role_aware_account_policy,
)
from hydra.propfirm.rolling_combine import EpisodeStartPolicy


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN_ID = "hydra_economic_evolution_role_aware_account_allocator_0010"


def _seed() -> dict:
    return json.loads(
        (
            PROJECT_ROOT
            / "reports/economic_evolution/seeds/"
            "persistent_0003_successor_seed.json"
        ).read_text(encoding="utf-8")
    )


def test_role_aware_compiler_accepts_eight_without_changing_legacy_schema() -> None:
    population = generate_role_aware_account_population(
        _seed(), campaign_id=CAMPAIGN_ID, policy_pair_count=64
    )
    policy = next(
        value for value in population.real_policies if len(value.sleeve_ids) == 8
    )
    components = {row.sleeve.sleeve_id: row for row in population.components}
    runtimes = {
        sleeve_id: ExactSleeveRuntime(
            sleeve_id=sleeve_id,
            signal_market=components[sleeve_id].sleeve.market,
            execution_market=components[sleeve_id].sleeve.execution_market,
            role=components[sleeve_id].sleeve.role,
            source_campaign=CAMPAIGN_ID,
            specification_hash="a" * 64,
            eligible_session_days=tuple(range(100)),
            events=(),
            event_count=0,
            net_pnl=0.0,
            cost_stress_1_5x_net=0.0,
            maximum_drawdown=0.0,
            best_positive_event_share=0.0,
            exit_implementation="EXACT_TIME_EXIT",
        )
        for sleeve_id in policy.sleeve_ids
    }

    compiled = compile_role_aware_account_policy(policy, runtimes)
    assert isinstance(compiled.basket, RoleAwareBasketPolicy)
    assert len(compiled.basket.component_ids) == 8
    result = evaluate_compiled_account_policy(
        compiled,
        episode_policy=EpisodeStartPolicy(
            maximum_starts=1,
            minimum_spacing_sessions=5,
            minimum_observation_sessions=60,
            maximum_duration_sessions=60,
        ),
        explicit_start_days=(0,),
    )
    assert result.static_base.episode_start_count == 1
    assert result.controlled_base.pass_count == 0
    assert result.outbound_order_capability is False

    with pytest.raises(ValueError, match="one to five"):
        BasketPolicy(
            policy_id="legacy-must-stay-frozen",
            component_ids=policy.sleeve_ids,
            archetype="INVALID_LEGACY_EXPANSION",
            component_priority=policy.sleeve_ids,
        )
