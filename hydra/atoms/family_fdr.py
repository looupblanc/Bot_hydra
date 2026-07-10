from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from typing import Iterable

from hydra.atoms.schema import EdgeAtomHypothesis


FAMILY_FDR_POLICY_VERSION = "hierarchical_family_fdr_v1"


@dataclass(frozen=True)
class FamilyFdrAdjustment:
    family: str
    raw_evidence: float
    family_trial_count: int
    adjusted_evidence: float
    policy_version: str = FAMILY_FDR_POLICY_VERSION

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def family_trial_counts(atoms: Iterable[EdgeAtomHypothesis]) -> dict[str, int]:
    return dict(Counter(atom.family for atom in atoms))


def adjust_evidence(raw_evidence: float, family_trial_count: int) -> float:
    return float(raw_evidence / max(float(family_trial_count) ** 0.5, 1.0))


def family_adjustment(family: str, raw_evidence: float, family_trial_count: int) -> FamilyFdrAdjustment:
    return FamilyFdrAdjustment(
        family=family,
        raw_evidence=float(raw_evidence),
        family_trial_count=int(family_trial_count),
        adjusted_evidence=adjust_evidence(raw_evidence, family_trial_count),
    )

