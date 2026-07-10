from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path


DEFAULT_TOPSTEP_RULE_PATH = "config/prop_firms/topstep_150k_2026-07-10.yaml"


@dataclass(frozen=True)
class RuleSnapshot:
    path: str
    rule_version_id: str
    verified_at_utc: str
    primary_mode: str
    raw_text: str


def load_topstep_rule_snapshot(path: str = DEFAULT_TOPSTEP_RULE_PATH) -> RuleSnapshot:
    source = project_path(path)
    raw = source.read_text(encoding="utf-8")
    return RuleSnapshot(
        path=str(source),
        rule_version_id=_extract_scalar(raw, "rule_version_id") or "unknown",
        verified_at_utc=_extract_scalar(raw, "verified_at_utc") or "unknown",
        primary_mode=_extract_scalar(raw, "primary_mode") or "unknown",
        raw_text=raw,
    )


def material_assumptions(path: str = DEFAULT_TOPSTEP_RULE_PATH) -> dict[str, Any]:
    snapshot = load_topstep_rule_snapshot(path)
    return {
        "rule_version_id": snapshot.rule_version_id,
        "verified_at_utc": snapshot.verified_at_utc,
        "primary_mode": snapshot.primary_mode,
        "source_path": snapshot.path,
    }


def write_rule_summary(path: str = DEFAULT_TOPSTEP_RULE_PATH, output: str = "reports/lockbox/topstep_rule_snapshot_summary.json") -> Path:
    target = project_path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(material_assumptions(path), indent=2, sort_keys=True), encoding="utf-8")
    return target


def _extract_scalar(raw: str, key: str) -> str | None:
    prefix = f"{key}:"
    for line in raw.splitlines():
        if line.startswith(prefix):
            return line.split(":", 1)[1].strip().strip('"').strip("'")
    return None

