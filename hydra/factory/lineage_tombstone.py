from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


class LineageTombstoneViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class LineageTombstone:
    lineage: str
    disposition: str
    reason: str
    blocked_family_names: tuple[str, ...]
    blocked_parameter_markers: tuple[str, ...]
    allowed_reformulation_requirements: tuple[str, ...]

    @classmethod
    def from_file(cls, path: str | Path) -> "LineageTombstone":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            lineage=str(payload["lineage"]),
            disposition=str(payload["disposition"]),
            reason=str(payload["reason"]),
            blocked_family_names=tuple(str(item).lower() for item in payload.get("blocked_family_names", [])),
            blocked_parameter_markers=tuple(str(item).lower() for item in payload.get("blocked_parameter_markers", [])),
            allowed_reformulation_requirements=tuple(str(item) for item in payload.get("allowed_reformulation_requirements", [])),
        )

    def blocks(self, candidate: Any) -> bool:
        record = _candidate_record(candidate)
        family_text = " ".join(
            str(record.get(key) or "").lower()
            for key in ("family", "lane", "name", "parent_family", "lineage", "mutation_lineage", "parent_candidate_id")
        )
        if any(name and name in family_text for name in self.blocked_family_names):
            return not _is_allowed_paired_reformulation(record)
        params = record.get("parameters") or {}
        param_text = json.dumps(params, sort_keys=True, default=str).lower()
        if any(marker in param_text for marker in self.blocked_parameter_markers):
            return not _is_allowed_paired_reformulation(record)
        return False


def load_default_tombstones(folder: str = "config/lineage_tombstones") -> list[LineageTombstone]:
    root = project_path(folder)
    if not root.exists():
        return []
    return [LineageTombstone.from_file(path) for path in sorted(root.glob("*.json"))]


def assert_not_tombstoned(candidate: Any, tombstones: list[LineageTombstone] | None = None) -> None:
    for tombstone in tombstones or load_default_tombstones():
        if tombstone.blocks(candidate):
            record = _candidate_record(candidate)
            raise LineageTombstoneViolation(
                f"Candidate {record.get('candidate_id') or record.get('name') or '<unknown>'} is blocked by "
                f"lineage tombstone {tombstone.lineage}: {tombstone.reason}"
            )


def _candidate_record(candidate: Any) -> dict[str, Any]:
    if isinstance(candidate, dict):
        return dict(candidate)
    return {
        key: getattr(candidate, key)
        for key in ("candidate_id", "family", "lane", "name", "parent_family", "lineage", "mutation_lineage", "parent_candidate_id", "parameters")
        if hasattr(candidate, key)
    }


def _is_allowed_paired_reformulation(record: dict[str, Any]) -> bool:
    params = record.get("parameters") or {}
    text = " ".join(str(record.get(key) or "").lower() for key in ("family", "lane", "name"))
    paired_markers = {
        "left_symbol",
        "right_symbol",
        "hedge_ratio_method",
        "two_leg_execution",
        "pair_validity_required",
        "beta_neutral",
    }
    has_paired_params = paired_markers.issubset(set(params))
    has_reformulated_name = "paired" in text or "beta_neutral" in text or "relative_value" in text
    return bool(has_paired_params and has_reformulated_name)
