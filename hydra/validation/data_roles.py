from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class DataRole(str, Enum):
    DEVELOPMENT = "DEVELOPMENT"
    SECONDARY_DEVELOPMENT_CONFIRMATION = "SECONDARY_DEVELOPMENT_CONFIRMATION"
    DIAGNOSTIC_CONFIRMATION_CONSUMED = "DIAGNOSTIC_CONFIRMATION_CONSUMED"
    CONTAMINATED_DEVELOPMENT = "CONTAMINATED_DEVELOPMENT"
    BLIND_VALIDATION = "BLIND_VALIDATION"
    SEALED_BLIND_HOLDOUT = "SEALED_BLIND_HOLDOUT"
    FINAL_LOCKBOX = "FINAL_LOCKBOX"
    POTENTIAL_FINAL_LOCKBOX = "POTENTIAL_FINAL_LOCKBOX"


@dataclass(frozen=True)
class DataPeriod:
    name: str
    start: str
    end: str
    role: DataRole
    mutable_parameters: bool


DEFAULT_2024_PERIODS = {
    "q1": DataPeriod("q1_2024", "2024-01-01", "2024-03-29", DataRole.DEVELOPMENT, True),
    "q2": DataPeriod("q2_2024", "2024-04-01", "2024-07-01", DataRole.DIAGNOSTIC_CONFIRMATION_CONSUMED, True),
    "q3": DataPeriod("q3_2024", "2024-07-01", "2024-10-01", DataRole.CONTAMINATED_DEVELOPMENT, True),
    "q4": DataPeriod("q4_2024", "2024-10-01", "2025-01-01", DataRole.SEALED_BLIND_HOLDOUT, False),
}


def role_for_period(start: str, end: str) -> DataRole:
    for period in DEFAULT_2024_PERIODS.values():
        if start == period.start and end == period.end:
            return period.role
    raise ValueError(f"Unknown governed data period: {start} to {end}")


def parameters_mutable(role: DataRole) -> bool:
    return role in {
        DataRole.DEVELOPMENT,
        DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION,
        DataRole.DIAGNOSTIC_CONFIRMATION_CONSUMED,
        DataRole.CONTAMINATED_DEVELOPMENT,
    }
