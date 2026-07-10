from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class NegativeControlSpec:
    control_id: str
    control_type: str
    event_frequency: float = 0.12

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def negative_control_specs() -> list[NegativeControlSpec]:
    return [
        NegativeControlSpec("independent_noise", "independent_noise"),
        NegativeControlSpec("autocorrelated_no_edge", "autocorrelated_no_edge"),
        NegativeControlSpec("random_session_effect", "random_session_effect"),
        NegativeControlSpec("block_shuffled_real_returns", "block_shuffled_real_returns"),
        NegativeControlSpec("opportunity_matched_random", "opportunity_matched_random"),
    ]


def apply_negative_signal(frame: pd.DataFrame, spec: NegativeControlSpec, *, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    out = frame.copy()
    if spec.control_type == "random_session_effect":
        phase = out["timestamp"].dt.hour.to_numpy()
        event = (phase % 5 == seed % 5) & (rng.random(len(out)) < spec.event_frequency * 2)
    elif spec.control_type == "block_shuffled_real_returns":
        block = max(10, len(out) // 100)
        base = np.tile(np.r_[np.ones(block), np.zeros(block)], len(out) // (2 * block) + 1)[: len(out)]
        event = np.roll(base, seed % block).astype(bool)
    else:
        event = rng.random(len(out)) < spec.event_frequency
    direction = rng.choice([-1, 1], size=len(out))
    out[f"signal_{spec.control_id}"] = event.astype(int) * direction
    return out

