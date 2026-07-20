from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acquire_fx_causal_ecology import (
    FXAcquisitionError,
    canonical_hash,
    load_manifest,
    requests_for,
)


def test_frozen_manifest_is_self_hashed_and_pre_q4() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = load_manifest(root)
    assert manifest["data_contract"]["end_exclusive"] == "2024-10-01"
    assert manifest["data_contract"]["q4_access"] is False
    assert [row["role"] for row in manifest["temporal_roles"]] == [
        "DISCOVERY",
        "VALIDATION",
        "FINAL_DEVELOPMENT",
        "CONFIRMATION",
    ]
    assert set(requests_for(manifest)) == {"ohlcv-1m", "definition"}


def test_manifest_drift_fails_closed(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "config/research/fx_causal_ecology_pilot_v1.json"
    manifest = json.loads(source.read_text(encoding="utf-8"))
    manifest["data_contract"]["end_exclusive"] = "2025-01-01"
    core = dict(manifest)
    core.pop("manifest_hash")
    manifest["manifest_hash"] = canonical_hash(core)
    target = tmp_path / "config/research"
    target.mkdir(parents=True)
    (target / source.name).write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(FXAcquisitionError, match="Q4"):
        load_manifest(tmp_path)
