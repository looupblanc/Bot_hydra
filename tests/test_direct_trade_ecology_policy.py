from hydra.research.direct_trade_ecology_policy import (
    FEATURE_CONCEPTS,
    action_from_probability,
    safe_features,
)


def test_action_lattice_is_complete_and_frozen() -> None:
    thresholds = (0.4, 0.6, 0.8)
    assert action_from_probability(0.1, thresholds) == 0.0
    assert action_from_probability(0.5, thresholds) == 0.5
    assert action_from_probability(0.7, thresholds) == 1.0
    assert action_from_probability(0.9, thresholds) == 1.5


def test_safe_model_has_exactly_eight_decision_time_concepts() -> None:
    candidate = {
        "execution_market": "MNQ",
        "mechanism": "VOLATILITY_EXPANSION",
        "session_code": 1,
        "timeframe": "5m",
        "favorable_r": 1.5,
        "adverse_r": 0.75,
        "horizon": 30,
        "trigger_quantile": 0.8,
        "context_quantile": 0.6,
        "stressed_full_net": 999999.0,
        "future_move": 123.0,
    }
    features = safe_features(candidate)
    assert tuple(features) == FEATURE_CONCEPTS
    assert "future_move" not in features
    assert "stressed_full_net" not in features


def test_action_thresholds_fail_closed_when_not_ordered() -> None:
    try:
        action_from_probability(0.5, (0.5, 0.5, 0.9))
    except ValueError as exc:
        assert "strictly ordered" in str(exc)
    else:
        raise AssertionError("unordered thresholds were accepted")
