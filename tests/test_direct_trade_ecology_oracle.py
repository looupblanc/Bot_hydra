from hydra.research.direct_trade_ecology_oracle import (
    consolidation_key,
    decision_card,
    oracle_action,
    representative_rank,
)


def test_oracle_is_explicitly_non_deployable_and_non_promotional() -> None:
    card = decision_card()
    assert card["deployable"] is False
    assert card["promotion_eligible"] is False
    assert card["selection_uses_future_outcome"] is True
    assert oracle_action(1.0) == 1.5
    assert oracle_action(0.0) == 0.0
    assert oracle_action(-1.0) == 0.0


def test_opportunity_identity_and_rank_use_no_outcome_field() -> None:
    event = {
        "execution_market": "MNQ",
        "direction": 1,
        "decision_time_ns": 123,
        "stressed_net_pnl": 999.0,
    }
    assert consolidation_key(event) == ("MNQ", 1, 123)
    candidate = {
        "adverse_r": 1.0,
        "favorable_r": 2.0,
        "horizon": 30,
        "trigger_quantile": 0.8,
        "context_quantile": 0.7,
    }
    before = representative_rank(candidate, "a")
    event["stressed_net_pnl"] = -999.0
    assert representative_rank(candidate, "a") == before


def test_categorical_horizons_have_frozen_explicit_order() -> None:
    candidate = {
        "adverse_r": 1.0,
        "favorable_r": 1.0,
        "horizon": "SESSION",
        "trigger_quantile": 0.8,
        "context_quantile": None,
    }
    session = representative_rank(candidate, "a")
    candidate["horizon"] = "OVERNIGHT"
    overnight = representative_rank(candidate, "a")
    assert session > overnight
