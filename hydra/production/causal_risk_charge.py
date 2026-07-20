"""Fail-closed validation for account-policy stop-risk charges.

Every account governor must reserve the maximum causal declared stop risk per
mini-equivalent contract.  A governor mode may change admission or scaling
semantics, but it must never replace executable stop risk with an identity or
epsilon charge.
"""

from __future__ import annotations

import math


MINIMUM_CAUSAL_STOP_RISK_CHARGE_USD = 1.0
RISK_CHARGE_CONTRACT = "MAX_CAUSAL_DECLARED_STOP_RISK_PER_MINI_V1"


class CausalRiskChargeError(ValueError):
    """The supplied charge cannot represent executable futures stop risk."""


def require_causal_stop_risk_charge(
    declared_stop_risk_charge_per_mini: float,
    *,
    governor_mode: str,
) -> float:
    """Return a validated causal charge without mode-specific substitution."""

    charge = float(declared_stop_risk_charge_per_mini)
    if not math.isfinite(charge) or charge < MINIMUM_CAUSAL_STOP_RISK_CHARGE_USD:
        raise CausalRiskChargeError(
            "causal declared stop-risk charge must be finite and at least "
            f"{MINIMUM_CAUSAL_STOP_RISK_CHARGE_USD:.2f} USD per mini; "
            f"governor={governor_mode!r}, charge={charge!r}"
        )
    return charge
