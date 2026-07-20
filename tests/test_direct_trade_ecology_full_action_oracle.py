from hydra.research.direct_trade_ecology_full_action_oracle import (
    ACTION_FRONTIER,
    choose_action,
    rounded_contract_quantity,
)


def _rows(values: dict[float, float]) -> dict[str, dict[str, float]]:
    return {
        f"{action:.1f}X": {"stressed_net_pnl_usd": value}
        for action, value in values.items()
    }


def test_contract_rounding_matches_frozen_floor_rule() -> None:
    assert ACTION_FRONTIER == (0.0, 0.5, 1.0, 1.5)
    assert rounded_contract_quantity(3, account_scale=1.0, action=0.0) == 0
    assert rounded_contract_quantity(3, account_scale=1.0, action=0.5) == 1
    assert rounded_contract_quantity(3, account_scale=1.5, action=1.5) == 6


def test_full_action_oracle_can_select_intermediate_tier() -> None:
    rows = _rows({0.0: 0.0, 0.5: 50.0, 1.0: 80.0, 1.5: 70.0})
    assert choose_action(rows, binary=False) == 1.0
    assert choose_action(rows, binary=True) == 1.5


def test_oracle_abstains_when_every_trade_action_loses() -> None:
    rows = _rows({0.0: 0.0, 0.5: -10.0, 1.0: -20.0, 1.5: -30.0})
    assert choose_action(rows, binary=False) == 0.0
    assert choose_action(rows, binary=True) == 0.0
