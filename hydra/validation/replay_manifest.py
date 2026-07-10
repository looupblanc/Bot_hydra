from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from hydra.validation.status_policy import STATUS_POLICY_VERSION, stable_policy_hash


@dataclass(frozen=True)
class FrozenReplayCandidate:
    candidate_id: str
    lane: str
    structural_id: str
    variant_id: str
    symbol: str
    components: tuple[str, ...]
    horizon: int
    threshold_rank: int
    previous_statuses: tuple[str, ...]
    replay_role: str
    sizing: str = "fixed_one_contract"
    max_trades_per_period: int = 80
    cost_model: str = "round_turn_cost_v1"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_replay_manifest(
    *,
    baseline_commit: str,
    candidates: list[FrozenReplayCandidate],
    data_fingerprints: dict[str, str],
    source_report: str,
    decision_policy_version: str = STATUS_POLICY_VERSION,
) -> dict[str, Any]:
    body = {
        "manifest_type": "strict_unsampled_temporal_transfer_replay",
        "source_commit": baseline_commit,
        "source_report": source_report,
        "decision_policy_version": decision_policy_version,
        "parameter_mutation_allowed": False,
        "sizing_optimization_allowed": False,
        "q4_access_allowed": False,
        "candidate_count": len(candidates),
        "candidates": [candidate.to_dict() for candidate in candidates],
        "data_fingerprints": data_fingerprints,
    }
    body["manifest_hash"] = stable_policy_hash(body)
    return body


def write_manifest(manifest: dict[str, Any], path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return str(manifest["manifest_hash"])


def assert_same_candidate_spec(candidate: FrozenReplayCandidate, record: dict[str, Any]) -> None:
    checks = {
        "candidate_id": candidate.candidate_id,
        "lane": candidate.lane,
        "symbol": candidate.symbol,
        "components": list(candidate.components),
        "horizon": candidate.horizon,
        "threshold_rank": candidate.threshold_rank,
        "sizing": candidate.sizing,
        "max_trades_per_period": candidate.max_trades_per_period,
    }
    for key, expected in checks.items():
        if record.get(key) != expected:
            raise ValueError(f"Frozen manifest mismatch for {candidate.candidate_id}: {key} expected {expected!r}, got {record.get(key)!r}")
