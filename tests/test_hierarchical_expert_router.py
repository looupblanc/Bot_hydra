from hydra.research.hierarchical_expert_router import (
    _router_policy,
    hierarchical_posterior,
    niche_key,
    route_actions,
)


def test_hierarchical_shrinkage_pulls_sparse_source_toward_niche() -> None:
    niche, source = hierarchical_posterior(
        global_total=-100.0,
        global_count=100,
        niche_total=200.0,
        niche_count=100,
        source_total=100.0,
        source_count=1,
    )
    assert -1.0 < niche < 2.0
    assert niche < source < 100.0


def test_niche_uses_only_preregistered_source_identity_fields() -> None:
    candidate = {
        "execution_market": "MNQ",
        "mechanism": "EXHAUSTION_REVERSAL",
        "session_code": 1,
        "timeframe": "5m",
        "future_outcome": 999.0,
    }
    assert niche_key(candidate) == ("MNQ", "EXHAUSTION_REVERSAL", 1, "5m")


def test_router_uses_historical_score_only_and_is_chronological() -> None:
    opportunities = [
        {
            "opportunity_id": "first",
            "session_day": 1,
            "decision_time_ns": 10,
            "members": [
                {"candidate_id": "low", "source_score": 1.0},
                {"candidate_id": "high", "source_score": 2.0},
            ],
        },
        {
            "opportunity_id": "later",
            "session_day": 1,
            "decision_time_ns": 20,
            "members": [
                {"candidate_id": "future_winner", "source_score": 100.0}
            ],
        },
    ]
    routed = route_actions(opportunities, daily_budget=1, mode="HIERARCHICAL")
    assert len(routed) == 1
    assert routed[0]["candidate_id"] == "high"
    assert routed[0]["opportunity_id"] == "first"


def test_random_control_is_deterministic_and_exposure_matched() -> None:
    opportunities = [
        {
            "opportunity_id": str(index),
            "session_day": 1,
            "decision_time_ns": index,
            "members": [
                {"candidate_id": "a", "source_score": 1.0},
                {"candidate_id": "b", "source_score": 2.0},
            ],
        }
        for index in range(10)
    ]
    first = route_actions(opportunities, daily_budget=4, mode="RANDOM")
    second = route_actions(opportunities, daily_budget=4, mode="RANDOM")
    assert first == second
    assert len(first) == 4


def test_best_expert_policy_accepts_one_component() -> None:
    rules = {
        "profit_target_usd": 3000.0,
        "maximum_loss_limit_usd": 2000.0,
        "maximum_mini_contracts": 5,
        "consistency_target_fraction": 0.5,
    }
    policy = _router_policy("best", rules, ["expert"], {"expert": 100.0})
    assert policy.maximum_concurrent_sleeves == 1
