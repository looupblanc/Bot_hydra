from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class DataRole(str, Enum):
    DEVELOPMENT = "DEVELOPMENT"
    SECONDARY_DEVELOPMENT_CONFIRMATION = "SECONDARY_DEVELOPMENT_CONFIRMATION"
    BLIND_VALIDATION = "BLIND_VALIDATION"
    FINAL_LOCKBOX = "FINAL_LOCKBOX"


@dataclass(frozen=True)
class DataPeriod:
    name: str
    start: str
    end: str
    role: DataRole
    mutable_parameters: bool


DEFAULT_2024_PERIODS = {
    "q1": DataPeriod("q1_2024", "2024-01-01", "2024-03-29", DataRole.DEVELOPMENT, True),
    "q2": DataPeriod("q2_2024", "2024-04-01", "2024-07-01", DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION, True),
    "q3": DataPeriod("q3_2024", "2024-07-01", "2024-10-01", DataRole.BLIND_VALIDATION, False),
    "q4": DataPeriod("q4_2024", "2024-10-01", "2025-01-01", DataRole.FINAL_LOCKBOX, False),
}


def role_for_period(start: str, end: str) -> DataRole:
    for period in DEFAULT_2024_PERIODS.values():
        if start == period.start and end == period.end:
            return period.role
    raise ValueError(f"Unknown governed data period: {start} to {end}")


def parameters_mutable(role: DataRole) -> bool:
    return role in {DataRole.DEVELOPMENT, DataRole.SECONDARY_DEVELOPMENT_CONFIRMATION}

