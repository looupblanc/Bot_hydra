from __future__ import annotations

import pytest

from hydra.production.causal_risk_charge import (
    CausalRiskChargeError,
    require_causal_stop_risk_charge,
)


@pytest.mark.parametrize(
    "governor_mode",
    ("CONTRACT_ONLY_UNIFORM_SCALE", "CAUSAL_STATIC_STOP_RISK_GOVERNOR"),
)
def test_every_governor_preserves_declared_causal_stop_risk(
    governor_mode: str,
) -> None:
    assert require_causal_stop_risk_charge(
        463.02,
        governor_mode=governor_mode,
    ) == pytest.approx(463.02)


@pytest.mark.parametrize("charge", (0.0, 1e-6, float("nan"), float("inf")))
def test_identity_or_nonfinite_risk_charge_fails_closed(charge: float) -> None:
    with pytest.raises(CausalRiskChargeError, match="causal declared stop-risk"):
        require_causal_stop_risk_charge(
            charge,
            governor_mode="CONTRACT_ONLY_UNIFORM_SCALE",
        )
