from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from hydra.factory.quality_diversity_selector_v2 import SelectorV2Result


def build_elite_selection_manifest(
    result: SelectorV2Result,
    *,
    population_hash: str,
    selector_task_sha256: str,
    selection_data_end_exclusive: str = "2024-01-01",
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "schema": "quality_diversity_elite_selection_manifest_v2",
        "population_hash": population_hash,
        "selector_task_sha256": selector_task_sha256,
        "selection_data_end_exclusive": selection_data_end_exclusive,
        "selected_candidate_ids": [item["candidate_id"] for item in result.elites],
        "selected_fingerprints": [
            item["structural_fingerprint"] for item in result.elites
        ],
        "negative_control_ids": [
            item["candidate_id"] for item in result.negative_controls
        ],
        "negative_controls_promotion_eligible": False,
        "selector_audit": result.audit,
        "uses_2024_results": False,
        "q4_access_allowed": False,
    }
    manifest["selection_manifest_hash"] = _stable_hash(manifest)
    return manifest


def write_immutable_elite_manifest(path: str | Path, manifest: dict[str, Any]) -> Path:
    target = Path(path)
    content = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if target.exists() and target.read_text(encoding="utf-8") != content:
        raise RuntimeError(f"Refusing divergent elite manifest: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_text(content, encoding="utf-8")
    return target


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()
