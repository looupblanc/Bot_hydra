from __future__ import annotations

from hydra.propfirm.ruleset_v7 import RuleStatus, load_ruleset
from hydra.propfirm.topstep_150k import Topstep150KConfig


def test_ruleset_is_complete_versioned_and_blocks_unresolved_ticket() -> None:
    ruleset = load_ruleset()

    assert [rule.rule_id for rule in ruleset.rules] == [
        f"R{index}" for index in range(1, 17)
    ]
    assert ruleset.schema == "hydra_topstep_150k_ruleset_v7"
    assert ruleset.deployment_ticket_blockers == ("R2", "R6", "R7", "R11")
    assert not ruleset.deployment_ticket_allowed
    assert ruleset.by_id["R2"].status is RuleStatus.CONFLICT
    assert ruleset.by_id["R6"].status is RuleStatus.ASSUMED
    assert ruleset.by_id["R11"].status is RuleStatus.CONFLICT


def test_ruleset_numeric_contract_matches_simulator_defaults() -> None:
    rules = load_ruleset().by_id
    config = Topstep150KConfig()

    assert config.combine_profit_target == rules["R1"].parameters["profit_target_usd"]
    assert config.combine_max_loss_limit == rules["R2"].parameters["distance_usd"]
    assert config.optional_daily_loss_limit == rules["R3"].parameters["optional_dll_usd"]
    assert config.consistency_best_day_max_pct_of_profit_target == rules["R4"].parameters["best_day_max_fraction"]
    assert rules["R5"].parameters["max_mini_equivalent"] == 15
    assert config.minimum_pass_days == rules["R6"].parameters["minimum_trading_days"]
    assert config.funded_starting_balance == rules["R8"].parameters["starting_balance_usd"]
    assert config.funded_starting_mll == rules["R8"].parameters["starting_floor_usd"]
    assert config.payout_eligibility_winning_days == rules["R9"].parameters["winning_days"]
    assert config.payout_winning_day_min_profit == rules["R9"].parameters["winning_day_min_usd"]
    assert config.payout_cap == rules["R9"].parameters["payout_cap_usd"]
    assert config.funded_consistency_largest_day_max_pct_of_total_profit == rules["R11"].parameters["simulated_largest_day_max_fraction"]
    assert config.profit_split_trader == rules["R12"].parameters["trader_fraction"]
    assert config.trading_timezone == rules["R16"].parameters["timezone"]
    assert (
        config.trading_day_start_local
        == rules["R16"].parameters["trading_day_start_local"]
    )
    assert (
        config.winning_day_lock_local
        == rules["R16"].parameters["winning_day_lock_local"]
    )


def test_ruleset_permanently_forbids_orders_from_research_server() -> None:
    rules = load_ruleset().by_id

    assert rules["R13"].parameters["research_server_order_capability"] is False
    assert rules["R13"].parameters["remote_execution_allowed"] is False
    assert rules["R15"].parameters["hydra_order_count"] == 0
    assert rules["R15"].parameters["fix_tag"] == 1028
