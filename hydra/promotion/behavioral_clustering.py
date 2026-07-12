from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class BehavioralProfile:
    candidate_id: str
    exact_signature: str
    economic_key: tuple[Any, ...]
    broad_family_key: tuple[Any, ...]
    account_role_key: tuple[Any, ...]
    daily_pnl: dict[int, float]
    event_timestamps: frozenset[int]
    event_days: frozenset[int]
    tail_loss_days: frozenset[int]
    behavioral_evidence_complete: bool


def build_behavioral_profile(
    candidate: Mapping[str, Any], exact: Mapping[str, Any] | None
) -> BehavioralProfile:
    exact = dict(exact or {})
    specification = dict(candidate.get("specification") or {})
    days = [int(value) for value in exact.get("event_session_days") or []]
    pnl = [float(value) for value in exact.get("event_net_pnl") or []]
    timestamps = [int(value) for value in exact.get("event_timestamp_ns") or []]
    evidence_complete = bool(
        exact.get("behavioral_replay_complete")
        and timestamps
        and len(timestamps) == len(days) == len(pnl)
    )
    daily: dict[int, float] = {}
    for day, value in zip(days, pnl, strict=False):
        daily[day] = daily.get(day, 0.0) + value
    losses = sorted(
        ((value, day) for day, value in daily.items() if value < 0.0),
        key=lambda row: (row[0], row[1]),
    )
    tail_count = max(1, int(np.ceil(len(losses) * 0.20))) if losses else 0
    tail_days = frozenset(day for _value, day in losses[:tail_count])
    exact_payload = {
        "market": candidate.get("primary_market") or candidate.get("market"),
        "execution_market": candidate.get("execution_market"),
        "side": specification.get("side"),
        "holding_events": specification.get("holding_events"),
        "session_code": specification.get("session_code"),
        "days": days,
        "timestamps": timestamps,
        "pnl": [round(value, 8) for value in pnl],
    }
    if not days or len(days) != len(pnl):
        # Missing trade evidence is uncertainty, never evidence that two
        # candidates execute identically.
        exact_payload["unresolved_candidate_id"] = str(
            candidate.get("candidate_id") or ""
        )
    signature = hashlib.sha256(
        json.dumps(exact_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    market = candidate.get("primary_market") or candidate.get("market")
    family = candidate.get("mechanism_family") or candidate.get("family")
    role = candidate.get("role")
    timeframe = candidate.get("timeframe") or specification.get("timeframe")
    economic_key = (
        market,
        family,
        role,
        timeframe,
        specification.get("session_code"),
        specification.get("holding_events"),
        specification.get("side"),
    )
    return BehavioralProfile(
        candidate_id=str(candidate.get("candidate_id") or ""),
        exact_signature=signature,
        economic_key=economic_key,
        broad_family_key=(family, market),
        account_role_key=(role, market, timeframe),
        daily_pnl=daily,
        event_timestamps=frozenset(timestamps),
        event_days=frozenset(daily),
        tail_loss_days=tail_days,
        behavioral_evidence_complete=evidence_complete,
    )


def cluster_candidates(
    candidates: Sequence[Mapping[str, Any]],
    exact_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    profiles = [
        build_behavioral_profile(
            candidate, exact_by_id.get(str(candidate.get("candidate_id") or ""))
        )
        for candidate in candidates
    ]
    parent = list(range(len(profiles)))
    level: dict[tuple[int, int], int] = {}

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int, merge_level: int) -> None:
        lroot, rroot = find(left), find(right)
        if lroot == rroot:
            return
        if rroot < lroot:
            lroot, rroot = rroot, lroot
        parent[rroot] = lroot
        level[(lroot, rroot)] = merge_level

    for left in range(len(profiles)):
        for right in range(left + 1, len(profiles)):
            a, b = profiles[left], profiles[right]
            if (
                a.behavioral_evidence_complete
                and b.behavioral_evidence_complete
                and a.exact_signature == b.exact_signature
            ):
                union(left, right, 1)
                continue
            if a.economic_key != b.economic_key:
                continue
            overlap = (
                _jaccard(a.event_timestamps, b.event_timestamps)
                if a.event_timestamps and b.event_timestamps
                else _jaccard(a.event_days, b.event_days)
            )
            correlation = _daily_correlation(a.daily_pnl, b.daily_pnl)
            tail_overlap = _jaccard(a.tail_loss_days, b.tail_loss_days)
            # Similar average paths do not merge when their worst account days
            # are materially different; that distinction has portfolio value.
            if overlap >= 0.75 and correlation >= 0.85 and tail_overlap >= 0.50:
                union(left, right, 2)

    grouped: dict[int, list[BehavioralProfile]] = {}
    for index, profile in enumerate(profiles):
        grouped.setdefault(find(index), []).append(profile)
    clusters: list[dict[str, Any]] = []
    membership: dict[str, str] = {}
    for members in grouped.values():
        ids = sorted(member.candidate_id for member in members)
        cluster_id = "behavior_" + hashlib.sha256("|".join(ids).encode()).hexdigest()[:20]
        exact_equivalent = len({member.exact_signature for member in members}) == 1
        cluster = {
            "cluster_id": cluster_id,
            "member_ids": ids,
            "member_count": len(ids),
            "level": 1 if exact_equivalent and len(ids) > 1 else 2 if len(ids) > 1 else 3,
            "economic_key": list(members[0].economic_key),
            "broad_family_key": list(members[0].broad_family_key),
            "account_role_key": list(members[0].account_role_key),
            "execution_equivalent": bool(exact_equivalent and len(ids) > 1),
            "complete_behavioral_evidence_count": sum(
                member.behavioral_evidence_complete for member in members
            ),
            "maximum_backups": 2,
        }
        clusters.append(cluster)
        for candidate_id in ids:
            membership[candidate_id] = cluster_id
    clusters.sort(key=lambda row: (str(row["cluster_id"]), row["member_ids"]))
    return clusters, membership


def _jaccard(left: frozenset[int], right: frozenset[int]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    return len(left & right) / max(len(union), 1)


def _daily_correlation(left: Mapping[int, float], right: Mapping[int, float]) -> float:
    common = sorted(set(left) & set(right))
    if len(common) < 3:
        return 0.0
    a = np.asarray([left[day] for day in common], dtype=float)
    b = np.asarray([right[day] for day in common], dtype=float)
    if float(np.std(a)) <= 1e-12 or float(np.std(b)) <= 1e-12:
        return 1.0 if np.allclose(a, b) else 0.0
    return float(np.corrcoef(a, b)[0, 1])
