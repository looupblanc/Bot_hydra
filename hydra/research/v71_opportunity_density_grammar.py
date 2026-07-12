from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.research.v71_event_mechanism_grammar import (
    V71CandidateSpec,
    V71Signal,
    _signals_for_spec,
    _state_matrix,
    load_v71_minute_features,
    signal_path_hash,
)
from hydra.research.v7_graveyard import class_feedback


GRAMMAR_ID = "hydra_v7_1_opportunity_density_grammar_0002"
GRAMMAR_PATH = "WORM/v7.1-opportunity-density-grammar-0002-2026-07-12.json"
GRAMMAR_SHA256 = "ef44e6e72c42b2ed4b7228f3addbd2f182e3e51bcfb619aa4c0a2102db6d3566"
HORIZONS = (5, 15, 30, 60)
RESPONSES = ("CONTINUATION", "REVERSAL")


class V71OpportunityDensityError(RuntimeError):
    pass


def candidate_specs(
    project_root: str | Path = ".",
) -> tuple[V71CandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    rows: list[V71CandidateSpec] = []
    for family in grammar["families"]:
        family_id = str(family["family_id"])
        mechanism_class = "v71g2_" + family_id.lower()
        for motif in family["motifs"]:
            for response in RESPONSES:
                for horizon in HORIZONS:
                    candidate_id = (
                        f"v71g2_{family_id.lower()}_{str(motif).lower()}_"
                        f"{response.lower()}_h{horizon}"
                    )
                    payload = {
                        "grammar_id": GRAMMAR_ID,
                        "grammar_sha256": GRAMMAR_SHA256,
                        "candidate_id": candidate_id,
                        "family_id": family_id,
                        "mechanism_class": mechanism_class,
                        "motif": str(motif),
                        "response_policy": response,
                        "holding_minutes": horizon,
                        "cost_horizon": f"{horizon}m",
                        "product": "ES",
                    }
                    rows.append(
                        V71CandidateSpec(
                            candidate_id=candidate_id,
                            family_id=family_id,
                            mechanism_class=mechanism_class,
                            motif=str(motif),
                            response_policy=response,
                            holding_minutes=horizon,
                            cost_horizon=f"{horizon}m",
                            product="ES",
                            specification_hash=_stable_hash(payload),
                        )
                    )
    counts = Counter(row.family_id for row in rows)
    if len(rows) != 128 or len({row.candidate_id for row in rows}) != 128:
        raise V71OpportunityDensityError(
            "opportunity-density grammar must contain 128 unique candidates"
        )
    if set(counts.values()) != {32} or len(counts) != 4:
        raise V71OpportunityDensityError(
            "opportunity-density family allocation drift"
        )
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def generate_signal_population(
    minute: pd.DataFrame,
    *,
    project_root: str | Path = ".",
    graveyard_path: str | Path | None = "mission/state/graveyard.db",
) -> dict[str, tuple[V71Signal, ...]]:
    specs = candidate_specs(project_root)
    if graveyard_path is not None:
        dead = {
            str(row["mechanism_class"])
            for row in class_feedback(graveyard_path)
        }
        collisions = sorted(
            {row.mechanism_class for row in specs if row.mechanism_class in dead}
        )
        if collisions:
            raise V71OpportunityDensityError(
                "opportunity-density grammar repeats cemetery classes: "
                + ",".join(collisions)
            )
    frame, base = _state_matrix(minute)
    states = _opportunity_density_states(frame, base)
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        mask, direction = states[(spec.family_id, spec.motif)]
        side = direction if spec.response_policy == "CONTINUATION" else -direction
        output[spec.candidate_id] = tuple(
            _signals_for_spec(spec, frame, mask=mask, side=side)
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V71OpportunityDensityError("signal population drift")
    return dict(sorted(output.items()))


def _opportunity_density_states(
    frame: pd.DataFrame,
    base: Mapping[tuple[str, str], tuple[np.ndarray, np.ndarray]],
) -> dict[tuple[str, str], tuple[np.ndarray, np.ndarray]]:
    states: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}

    def union(*keys: tuple[str, str]) -> tuple[np.ndarray, np.ndarray]:
        return _directional_union(*(base[key] for key in keys))

    states[("PARTICIPATION_COVERAGE_UNION", "INTENSITY_OR_ALIGNED_PROGRESS")] = union(
        ("EVENT_PARTICIPATION_STATE", "INTENSITY_BURST"),
        ("COST_RESILIENT_LOW_TURNOVER", "RARE_EFFICIENCY_WITH_FLOW"),
    )
    states[("PARTICIPATION_COVERAGE_UNION", "ACCELERATION_OR_PERSISTENT_FLOW")] = union(
        ("EVENT_PARTICIPATION_STATE", "PARTICIPATION_ACCELERATION"),
        ("FUTURE_PATH_HAZARD", "CONTINUATION_HAZARD_STATE"),
    )
    states[("PARTICIPATION_COVERAGE_UNION", "DISTRIBUTED_OR_BURST_PARTICIPATION")] = union(
        ("EVENT_PARTICIPATION_STATE", "DISTRIBUTED_PARTICIPATION"),
        ("EVENT_PARTICIPATION_STATE", "INTENSITY_BURST"),
    )
    states[("PARTICIPATION_COVERAGE_UNION", "FLOW_OR_GEOMETRY_CONFIRMATION")] = union(
        ("COST_RESILIENT_LOW_TURNOVER", "RARE_EFFICIENCY_WITH_FLOW"),
        ("EVENT_PATH_GEOMETRY", "HIGH_EFFICIENCY_PATH"),
    )

    states[("STATE_TRANSITION_COVERAGE", "EXPANSION_OR_FLOW_ACCELERATION")] = union(
        ("EVENT_STATE_TRANSITIONS", "COMPRESSION_TO_EXPANSION"),
        ("EVENT_STATE_TRANSITIONS", "FLOW_ACCELERATION_TRANSITION"),
    )
    states[("STATE_TRANSITION_COVERAGE", "RECOVERY_OR_FAILED_EXTENSION")] = union(
        ("EVENT_PATH_GEOMETRY", "FAST_RECOVERY_PATH"),
        ("EFFORT_WITHOUT_PROGRESS", "FAILED_EXTENSION"),
    )
    states[("STATE_TRANSITION_COVERAGE", "EFFICIENCY_OR_COMPRESSION_BREAK")] = union(
        ("EVENT_PATH_GEOMETRY", "GEOMETRY_TRANSITION_MOTIF"),
        ("EVENT_STATE_TRANSITIONS", "COMPRESSION_TO_EXPANSION"),
    )
    states[("STATE_TRANSITION_COVERAGE", "HAZARD_OR_GEOMETRY_TRANSITION")] = union(
        ("FUTURE_PATH_HAZARD", "TARGET_PROGRESS_STATE"),
        ("EVENT_PATH_GEOMETRY", "GEOMETRY_TRANSITION_MOTIF"),
    )

    active_mask, active_direction = _directional_union(
        base[("EVENT_PARTICIPATION_STATE", "INTENSITY_BURST")],
        base[("EVENT_PARTICIPATION_STATE", "DISTRIBUTED_PARTICIPATION")],
        base[("EVENT_PATH_GEOMETRY", "HIGH_EFFICIENCY_PATH")],
    )
    local = frame["local_minute"].to_numpy(dtype=int)
    phases = {
        "OPEN_ACTIVE_STATE": (8 * 60 + 30, 9 * 60 + 30),
        "MORNING_ACTIVE_STATE": (9 * 60 + 30, 11 * 60),
        "MIDDAY_ACTIVE_STATE": (11 * 60, 13 * 60),
        "AFTERNOON_ACTIVE_STATE": (13 * 60, 15 * 60),
    }
    for motif, (start, end) in phases.items():
        gate = (local >= start) & (local < end)
        states[("SESSION_PHASE_EVENT_DENSITY", motif)] = (
            active_mask & gate,
            active_direction,
        )

    session = frame["session_day"].astype(str)
    prior_active = (
        pd.Series(active_mask, index=frame.index)
        .groupby(session, sort=False)
        .shift(1)
        .fillna(False)
        .to_numpy(dtype=bool)
    )
    prior_direction = (
        pd.Series(active_direction, index=frame.index)
        .groupby(session, sort=False)
        .shift(1)
        .fillna(0)
        .to_numpy(dtype=np.int8)
    )
    states[("EVENT_CLUSTER_HAZARD", "CLUSTER_START")] = (
        active_mask & ~prior_active,
        active_direction,
    )
    states[("EVENT_CLUSTER_HAZARD", "CLUSTER_PERSISTENCE")] = (
        active_mask & prior_active & (active_direction == prior_direction),
        active_direction,
    )
    states[("EVENT_CLUSTER_HAZARD", "CLUSTER_RELEASE")] = (
        ~active_mask & prior_active,
        prior_direction,
    )
    states[("EVENT_CLUSTER_HAZARD", "CLUSTER_DIRECTION_REVERSAL")] = (
        active_mask & prior_active & (active_direction == -prior_direction),
        active_direction,
    )
    expected = {
        (spec.family_id, spec.motif)
        for spec in candidate_specs(Path(__file__).resolve().parents[2])
    }
    if set(states) != expected:
        raise V71OpportunityDensityError("motif implementation set drift")
    return states


def _directional_union(
    *components: tuple[np.ndarray, np.ndarray],
) -> tuple[np.ndarray, np.ndarray]:
    if not components:
        raise ValueError("directional union requires at least one component")
    size = len(components[0][0])
    direction = np.zeros(size, dtype=np.int8)
    conflict = np.zeros(size, dtype=bool)
    for raw_mask, raw_direction in components:
        mask = np.asarray(raw_mask, dtype=bool)
        component_direction = np.asarray(raw_direction, dtype=np.int8)
        if len(mask) != size or len(component_direction) != size:
            raise V71OpportunityDensityError("component length drift")
        active = mask & (component_direction != 0)
        empty = active & (direction == 0)
        disagree = active & (direction != 0) & (direction != component_direction)
        direction[empty] = component_direction[empty]
        conflict |= disagree
    valid = (direction != 0) & ~conflict
    direction[~valid] = 0
    return valid, direction


def _load_grammar(root: Path) -> dict[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V71OpportunityDensityError("opportunity-density WORM hash mismatch")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("grammar_id") != GRAMMAR_ID:
        raise V71OpportunityDensityError("opportunity-density grammar ID drift")
    return payload


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "GRAMMAR_ID",
    "V71OpportunityDensityError",
    "candidate_specs",
    "generate_signal_population",
    "load_v71_minute_features",
    "signal_path_hash",
]
