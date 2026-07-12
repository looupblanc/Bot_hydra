from __future__ import annotations

import pytest

from hydra.execution.v7_cost_model import (
    CostStress,
    load_cost_model,
    render_cost_model_markdown,
)


def test_v7_cost_model_uses_preregistered_product_and_horizon_costs() -> None:
    model = load_cost_model()

    assert model.round_turn_cost("ES", "5m") == pytest.approx(28.80)
    assert model.round_turn_cost(
        "ES", "5m", stress=CostStress.STRESS_1_5X
    ) == pytest.approx(41.30)
    assert model.round_turn_cost(
        "ES", "5m", stress=CostStress.STRESS_2X
    ) == pytest.approx(53.80)
    assert model.round_turn_cost("MCL", "1m") == pytest.approx(5.54)


def test_sim_exploit_requires_positive_edge_under_two_x_slippage() -> None:
    model = load_cost_model()

    assert model.is_sim_exploit(
        50.0, symbol="ES", horizon="5m", round_turns=1
    )
    assert not model.is_sim_exploit(
        60.0, symbol="ES", horizon="5m", round_turns=1
    )


def test_cost_model_report_contains_every_current_product_and_stress() -> None:
    report = render_cost_model_markdown(load_cost_model())

    for symbol in ("ES", "MES", "NQ", "MNQ", "RTY", "M2K", "YM", "MYM", "CL", "MCL", "GC", "MGC"):
        assert f"| {symbol} |" in report
    assert "Stress 1.5×" in report
    assert "Stress 2×" in report
    assert "SIM_EXPLOIT" in report
    assert "## CONTRE" in report
