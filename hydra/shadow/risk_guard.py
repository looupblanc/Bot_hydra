from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from hydra.shadow.specification import ShadowSpecification


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str


class ShadowRiskGuard:
    def __init__(self, specification: ShadowSpecification) -> None:
        specification.validate()
        self.specification = specification
        self.kill_switch = False

    def activate_kill_switch(self) -> None:
        self.kill_switch = True

    def check(
        self,
        *,
        now: datetime,
        latest_data_at: datetime,
        proposed_exposure: float,
        session_open: bool,
        simulated_mll: float,
        daily_pnl: float,
    ) -> RiskDecision:
        now = now.astimezone(timezone.utc)
        latest_data_at = latest_data_at.astimezone(timezone.utc)
        if self.kill_switch:
            return RiskDecision(False, "kill_switch_active")
        if (now - latest_data_at).total_seconds() > self.specification.stale_data_seconds:
            return RiskDecision(False, "stale_data")
        if not session_open:
            return RiskDecision(False, "session_closed")
        if abs(proposed_exposure) > self.specification.maximum_exposure:
            return RiskDecision(False, "maximum_exposure")
        if simulated_mll <= self.specification.simulated_mll_floor:
            return RiskDecision(False, "simulated_mll_floor")
        if daily_pnl <= -self.specification.internal_daily_risk_limit:
            return RiskDecision(False, "internal_daily_risk_limit")
        return RiskDecision(True, "allowed_virtual_execution")
