from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class CostHurdlePolicy:
    policy_version: str
    atom_statistical_hurdle_multiplier: float
    atom_monetizability_buffer: float
    strategy_execution_cost_required: bool
    defensive_atom_cost_mode: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def calibrated_atom_cost_policy() -> CostHurdlePolicy:
    return CostHurdlePolicy(
        policy_version="atom_cost_hurdle_calibration_v1",
        atom_statistical_hurdle_multiplier=0.10,
        atom_monetizability_buffer=0.0,
        strategy_execution_cost_required=False,
        defensive_atom_cost_mode="risk_information_not_direct_round_trip_cost",
    )


def classify_previous_cost_hurdle_failure(*, effect_after_cost_hurdle: float, raw_effect: float) -> str:
    if effect_after_cost_hurdle < 0 and abs(raw_effect) > 0:
        return "ATOM_COST_HURDLE_OVER_APPLIED_TO_NON_STRATEGY_EVIDENCE"
    return "COST_HURDLE_NOT_PRIMARY_FAILURE"

