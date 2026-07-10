from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class StructuralPrototype:
    prototype_id: str
    lane: str
    structural_id: str
    variant_id: str
    components: tuple[str, ...]
    symbol: str
    horizon: int
    threshold_rank: int

    @property
    def logical_fingerprint(self) -> str:
        payload = asdict(self) | {"variant_id": ""}
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:20]


def generate_cluster_aware_prototypes(
    *,
    lanes: list[str],
    lane_components: dict[str, list[str]],
    symbols: list[str],
    max_total: int,
    max_structures: int,
    max_variants_per_structure: int,
) -> list[StructuralPrototype]:
    per_lane_structures = max(1, max_structures // max(len(lanes), 1))
    out: list[StructuralPrototype] = []
    for lane in lanes:
        combos = _component_sets(lane_components[lane])[:per_lane_structures]
        for struct_idx, components in enumerate(combos):
            structural_id = f"{lane}_struct_{struct_idx:02d}"
            for variant_idx in range(max_variants_per_structure):
                if len(out) >= max_total:
                    return out
                symbol = symbols[(struct_idx + variant_idx) % len(symbols)]
                out.append(
                    StructuralPrototype(
                        prototype_id=f"{lane}_{struct_idx:02d}_{variant_idx:02d}",
                        lane=lane,
                        structural_id=structural_id,
                        variant_id=f"variant_{variant_idx:02d}",
                        components=tuple(components),
                        symbol=symbol,
                        horizon=20 + 10 * (variant_idx % 3),
                        threshold_rank=variant_idx,
                    )
                )
    return out


def expected_event_signature(frame: pd.DataFrame, signal: pd.Series, *, max_events: int = 200) -> dict[str, Any]:
    events = signal.fillna(0).astype(int).ne(0)
    timestamps = pd.to_datetime(frame.loc[events, "timestamp"], utc=True, errors="coerce") if "timestamp" in frame.columns else pd.Series(dtype="datetime64[ns, UTC]")
    ids = [item.isoformat() for item in timestamps.head(max_events)]
    raw = json.dumps(ids, separators=(",", ":"))
    return {
        "event_count": int(events.sum()),
        "event_signature": hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24],
        "direction_signature": hashlib.sha256(json.dumps(signal.loc[events].head(max_events).astype(int).tolist()).encode("utf-8")).hexdigest()[:24],
        "session_distribution": dict(timestamps.dt.hour.value_counts().sort_index()) if len(timestamps) else {},
    }


def _component_sets(components: list[str]) -> list[list[str]]:
    out: list[list[str]] = []
    for component in components:
        out.append([component])
    for idx in range(len(components) - 1):
        out.append([components[idx], components[idx + 1]])
    out.append(list(components[:3]))
    return out
