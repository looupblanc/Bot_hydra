from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class PowerEstimate:
    effect_size: float
    sample_size: int
    noise_sigma: float
    approximate_power: float

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


def approximate_power(effect_size: float, sample_size: int, noise_sigma: float, alpha_z: float = 1.96) -> PowerEstimate:
    if sample_size <= 1 or noise_sigma <= 0:
        power = 0.0
    else:
        z = abs(effect_size) / (noise_sigma / (sample_size**0.5))
        power = max(0.0, min(1.0, (z - alpha_z) / max(alpha_z, 1e-9)))
    return PowerEstimate(float(effect_size), int(sample_size), float(noise_sigma), float(power))

