from __future__ import annotations

import numpy as np

from hydra.account_policy.basket import RoutedTrade
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.validation.economic_evolution_expensive_validation import (
    _account_summary_dominates,
    block_sign_randomization_test,
    calibrate_statistical_power,
    effective_independent_observations,
    moving_block_bootstrap_means,
    sign_invert_routed_trade,
)


def _trade() -> RoutedTrade:
    return RoutedTrade(
        component_id="component_a",
        market="MES",
        side=1,
        event=TradePathEvent(
            event_id="event_a",
            decision_ns=1_000,
            exit_ns=2_000,
            session_day=20_000,
            net_pnl=90.0,
            gross_pnl=100.0,
            worst_unrealized_pnl=-60.0,
            best_unrealized_pnl=140.0,
            quantity=1,
            mini_equivalent=0.1,
            regime="CONTROL",
            session_compliant=True,
            contract_limit_compliant=True,
            same_bar_ambiguous=False,
        ),
    )


def test_sign_inversion_preserves_cost_and_reverses_excursion() -> None:
    inverted = sign_invert_routed_trade(_trade())
    assert inverted.side == -1
    assert inverted.event.gross_pnl == -100.0
    assert inverted.event.net_pnl == -110.0
    assert inverted.event.worst_unrealized_pnl == -160.0
    assert inverted.event.best_unrealized_pnl == 40.0
    assert inverted.event.gross_pnl - inverted.event.net_pnl == 10.0


def test_effective_sample_size_penalizes_serial_dependence() -> None:
    independent = effective_independent_observations(
        [1.0, -1.0] * 40, maximum_lag=10
    )
    persistent = effective_independent_observations(
        np.repeat([1.0, -1.0], 40), maximum_lag=10
    )
    assert independent["effective_independent_observations"] == 80.0
    assert 1.0 <= persistent["effective_independent_observations"] < 20.0


def test_moving_block_bootstrap_is_deterministic() -> None:
    values = np.arange(1.0, 31.0)
    first = moving_block_bootstrap_means(
        values, repetitions=100, block_length=5, seed=71
    )
    second = moving_block_bootstrap_means(
        values, repetitions=100, block_length=5, seed=71
    )
    assert np.array_equal(first, second)
    assert first.shape == (100,)


def test_power_calibration_rejects_null_and_detects_large_effect() -> None:
    rng = np.random.default_rng(71005)
    residuals = rng.normal(0.0, 30.0, size=180)
    result = calibrate_statistical_power(
        residuals,
        minimum_useful_daily_net=40.0,
        repetitions=500,
        block_length=5,
        seed=71006,
        dsr_n_trials=100,
        bh_family_size=20,
        bh_fdr_q=0.10,
    )
    assert result["null_false_positive_rate"] <= 0.10
    assert result["power_on_minimum_useful_effect"] >= 0.80


def test_block_sign_null_distinguishes_persistent_positive_path() -> None:
    result = block_sign_randomization_test(
        np.full(120, 25.0),
        repetitions=2_000,
        block_length=5,
        seed=72001,
    )
    assert result["actual_mean_daily_net"] == 25.0
    assert result["one_sided_p_value"] <= 0.05


def test_account_dominance_requires_all_dimensions() -> None:
    incumbent = {
        "pooled_net_pnl": 1_000.0,
        "target_progress_median": 0.50,
        "mll_breach_rate": 0.10,
        "minimum_mll_buffer": 1_500.0,
        "consistency_pass_rate": 0.75,
    }
    superior = {
        **incumbent,
        "pooled_net_pnl": 1_100.0,
    }
    unsafe = {
        **superior,
        "minimum_mll_buffer": 1_000.0,
    }
    assert _account_summary_dominates(superior, incumbent)
    assert not _account_summary_dominates(unsafe, incumbent)
    assert not _account_summary_dominates(incumbent, incumbent)
