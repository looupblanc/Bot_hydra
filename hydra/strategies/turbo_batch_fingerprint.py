from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Iterable, Sequence

import numpy as np

from hydra.strategies.turbo_dsl import StrategySpec


FINGERPRINT_SCHEMA = "hydra.turbo.strategy.structure.v1"


def _float_token(value: float | None) -> str | None:
    """Use the exact IEEE-754 value rather than locale-sensitive formatting."""

    return None if value is None else float(value).hex()


def structural_payload(spec: StrategySpec) -> dict[str, object]:
    """Return the economic structure, excluding names and claimed role.

    Family, lineage, candidate id, role and execution scaling/costs are
    intentionally absent.  Changing labels, quantity, or mini/micro execution
    around an identical trading rule must not manufacture a new mechanism.
    """

    return {
        "schema": FINGERPRINT_SCHEMA,
        "market": spec.market,
        "timeframe": spec.timeframe,
        "feature": spec.feature,
        "operator": int(spec.operator),
        "threshold": _float_token(spec.threshold),
        "side": spec.side,
        "holding_events": spec.holding_events,
        "context_feature": spec.context_feature,
        "context_operator": (
            None if spec.context_operator is None else int(spec.context_operator)
        ),
        "context_threshold": _float_token(spec.context_threshold),
        "session_code": spec.session_code,
    }


def structural_fingerprint(spec: StrategySpec) -> str:
    encoded = json.dumps(
        structural_payload(spec), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def batch_fingerprints(specs: Sequence[StrategySpec]) -> np.ndarray:
    """Calculate compact deterministic fingerprints ready for ``numpy.unique``."""

    values = [structural_fingerprint(spec) for spec in specs]
    return np.asarray(values, dtype="U64")


@dataclass(frozen=True, slots=True)
class DeduplicatedBatch:
    specs: tuple[StrategySpec, ...]
    fingerprints: tuple[str, ...]
    duplicate_indices: tuple[int, ...]
    tombstoned_indices: tuple[int, ...]


def deduplicate_specs(
    specs: Sequence[StrategySpec], tombstones: Iterable[str] = ()
) -> DeduplicatedBatch:
    """Preserve first occurrence and reject historical structural tombstones."""

    fingerprints = batch_fingerprints(specs)
    tombstone_set = frozenset(tombstones)
    seen: set[str] = set()
    retained_specs: list[StrategySpec] = []
    retained_fingerprints: list[str] = []
    duplicate_indices: list[int] = []
    tombstoned_indices: list[int] = []

    for index, (spec, fingerprint) in enumerate(zip(specs, fingerprints, strict=True)):
        value = str(fingerprint)
        if value in tombstone_set:
            tombstoned_indices.append(index)
            continue
        if value in seen:
            duplicate_indices.append(index)
            continue
        seen.add(value)
        retained_specs.append(spec)
        retained_fingerprints.append(value)

    return DeduplicatedBatch(
        specs=tuple(retained_specs),
        fingerprints=tuple(retained_fingerprints),
        duplicate_indices=tuple(duplicate_indices),
        tombstoned_indices=tuple(tombstoned_indices),
    )


__all__ = [
    "DeduplicatedBatch",
    "FINGERPRINT_SCHEMA",
    "batch_fingerprints",
    "deduplicate_specs",
    "structural_fingerprint",
    "structural_payload",
]
