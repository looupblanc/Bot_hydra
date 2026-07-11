from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ArchiveNiche:
    market_ecology: str
    timeframe_profile: str
    holding_horizon: str
    session: str
    mechanism_family: str
    portfolio_role: str
    turnover: str
    behavioral_cluster: str

    @property
    def key(self) -> str:
        raw = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()[:24]


@dataclass(frozen=True)
class ArchiveCandidate:
    candidate_id: str
    structural_fingerprint: str
    lineage_id: str
    family: str
    niche: ArchiveNiche
    quality: dict[str, float]
    payload: dict[str, Any]


@dataclass(frozen=True)
class ArchiveDecision:
    accepted: bool
    reason: str
    replaced_candidate_id: str | None = None


class QualityDiversityArchive:
    """Small MAP-Elites archive with explicit diversity and lineage controls."""

    def __init__(
        self,
        *,
        niche_capacity: int = 3,
        maximum_family_share: float = 0.25,
        maximum_ecology_share: float = 0.35,
        maximum_lineage_share: float = 0.02,
    ) -> None:
        self.niche_capacity = niche_capacity
        self.maximum_family_share = maximum_family_share
        self.maximum_ecology_share = maximum_ecology_share
        self.maximum_lineage_share = maximum_lineage_share
        self._niches: dict[str, list[ArchiveCandidate]] = defaultdict(list)
        self._fingerprints: set[str] = set()

    @property
    def candidates(self) -> list[ArchiveCandidate]:
        return [item for rows in self._niches.values() for item in rows]

    def insert(self, candidate: ArchiveCandidate) -> ArchiveDecision:
        if candidate.structural_fingerprint in self._fingerprints:
            return ArchiveDecision(False, "duplicate_structural_fingerprint")
        prospective_size = len(self.candidates) + 1
        family = Counter(item.family for item in self.candidates)
        ecology = Counter(item.niche.market_ecology for item in self.candidates)
        lineage = Counter(item.lineage_id for item in self.candidates)
        if family[candidate.family] + 1 > _cap(prospective_size, self.maximum_family_share):
            return ArchiveDecision(False, "family_share_cap")
        if ecology[candidate.niche.market_ecology] + 1 > _cap(
            prospective_size, self.maximum_ecology_share
        ):
            return ArchiveDecision(False, "market_ecology_share_cap")
        if lineage[candidate.lineage_id] + 1 > _cap(
            prospective_size, self.maximum_lineage_share
        ):
            return ArchiveDecision(False, "lineage_share_cap")
        niche = self._niches[candidate.niche.key]
        if len(niche) < self.niche_capacity:
            niche.append(candidate)
            self._fingerprints.add(candidate.structural_fingerprint)
            return ArchiveDecision(True, "niche_capacity_available")
        weakest = min(niche, key=_quality_vector)
        if _quality_vector(candidate) <= _quality_vector(weakest):
            return ArchiveDecision(False, "niche_not_improved")
        niche.remove(weakest)
        self._fingerprints.remove(weakest.structural_fingerprint)
        niche.append(candidate)
        self._fingerprints.add(candidate.structural_fingerprint)
        return ArchiveDecision(True, "niche_elite_replaced", weakest.candidate_id)

    def summary(self) -> dict[str, Any]:
        items = self.candidates
        return {
            "candidate_count": len(items),
            "niche_count": len([rows for rows in self._niches.values() if rows]),
            "families": dict(Counter(item.family for item in items)),
            "market_ecologies": dict(Counter(item.niche.market_ecology for item in items)),
            "lineages": len({item.lineage_id for item in items}),
            "structural_fingerprints": len(self._fingerprints),
        }


def structural_fingerprint(specification: dict[str, Any]) -> str:
    excluded = {"candidate_id", "variant_id", "created_at", "display_name"}
    frozen = {key: value for key, value in specification.items() if key not in excluded}
    raw = json.dumps(frozen, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _cap(size: int, share: float) -> int:
    # A single seed must be able to initialize a niche; the cap becomes strict
    # as the archive grows.
    return max(1, math.ceil(size * share))


def _quality_vector(candidate: ArchiveCandidate) -> tuple[float, ...]:
    ordered = (
        "net_economics",
        "temporal_transfer",
        "cost_resilience",
        "mll_buffer",
        "null_evidence",
        "behavioral_novelty",
        "execution_confidence",
        "portfolio_utility",
    )
    complexity_penalty = -float(candidate.quality.get("complexity", 0.0))
    return tuple(float(candidate.quality.get(key, 0.0)) for key in ordered) + (
        complexity_penalty,
    )
