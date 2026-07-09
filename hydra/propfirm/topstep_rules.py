from __future__ import annotations

from hydra.propfirm.mll import simulate_trailing_mll


def evaluate_topstep_style(equity_curve, config: dict) -> dict:
    return simulate_trailing_mll(equity_curve, config["account_size"], config["max_loss_limit"])
