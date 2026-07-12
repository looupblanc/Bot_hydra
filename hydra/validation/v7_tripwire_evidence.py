from __future__ import annotations

from dataclasses import asdict, dataclass

from scipy.stats import binomtest


NET_EVIDENCE_P_VALUE_MAX = 0.01


class V7TripwireEvidenceError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class V7TripwireEvidence:
    real_passes: int
    real_episodes: int
    null_passes: int
    null_episodes: int
    null_observed_rate: float
    exact_binomial_one_sided_p_value: float
    evidence_strength: str

    def to_dict(self) -> dict[str, int | float | str]:
        return asdict(self)


def exact_tripwire_evidence(
    *,
    real_passes: int,
    real_episodes: int,
    null_passes: int,
    null_episodes: int,
    tripwire_verdict: str,
) -> V7TripwireEvidence:
    counts = (real_passes, real_episodes, null_passes, null_episodes)
    if any(isinstance(value, bool) or int(value) != value for value in counts):
        raise V7TripwireEvidenceError("tripwire counts must be integers")
    if real_episodes <= 0 or null_episodes <= 0:
        raise V7TripwireEvidenceError("tripwire episode counts must be positive")
    if not 0 <= real_passes <= real_episodes:
        raise V7TripwireEvidenceError("real tripwire counts are invalid")
    if not 0 <= null_passes <= null_episodes:
        raise V7TripwireEvidenceError("null tripwire counts are invalid")
    null_rate = null_passes / null_episodes
    p_value = float(
        binomtest(
            real_passes,
            real_episodes,
            p=null_rate,
            alternative="greater",
        ).pvalue
    )
    if tripwire_verdict == "GREEN_NULL_ADJUSTED_BASELINE":
        strength = (
            "VERT_NET"
            if p_value <= NET_EVIDENCE_P_VALUE_MAX
            else "VERT_MINCE"
        )
    elif tripwire_verdict == "ARTEFACT_GEOMETRY_ONLY":
        strength = "ARTEFACT"
    else:
        strength = "INDETERMINE"
    return V7TripwireEvidence(
        real_passes=int(real_passes),
        real_episodes=int(real_episodes),
        null_passes=int(null_passes),
        null_episodes=int(null_episodes),
        null_observed_rate=float(null_rate),
        exact_binomial_one_sided_p_value=p_value,
        evidence_strength=strength,
    )


__all__ = [
    "NET_EVIDENCE_P_VALUE_MAX",
    "V7TripwireEvidence",
    "V7TripwireEvidenceError",
    "exact_tripwire_evidence",
]
