from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.governance.proof_registry import (
    ProofRegistryError,
    append_entry,
    burned_window_ids,
    load_and_verify,
)


def test_bootstrap_registry_is_hash_chained_and_q4_is_burned() -> None:
    registry = load_and_verify("mission/state/proof_registry.json")

    assert registry["entry_count"] == 1
    assert burned_window_ids(registry) == ("Q4_2024",)


def test_proof_window_can_be_appended_then_burned_exactly_once(tmp_path: Path) -> None:
    source = Path("mission/state/proof_registry.json")
    destination = tmp_path / "proof_registry.json"
    destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    common = {
        "event_type": "PROOF_WINDOW_STATUS",
        "recorded_at_utc": "2026-07-12T13:30:00Z",
        "window": {
            "id": "FORWARD_2026_W28",
            "start": "2026-07-06",
            "end_exclusive": "2026-07-13",
        },
        "candidate_ids": ["candidate"],
        "evidence": {"manifest_sha256": "a" * 64},
    }
    append_entry(
        destination,
        {**common, "event_id": "available", "status": "AVAILABLE_CONFIRMATION"},
    )
    append_entry(
        destination,
        {**common, "event_id": "burn", "status": "BURNED"},
    )

    registry = load_and_verify(destination)
    assert registry["entry_count"] == 3
    assert "FORWARD_2026_W28" in burned_window_ids(registry)
    with pytest.raises(ProofRegistryError, match="irreversibly BURNED"):
        append_entry(
            destination,
            {**common, "event_id": "reuse", "status": "AVAILABLE_CONFIRMATION"},
        )


def test_proof_registry_detects_prior_entry_mutation(tmp_path: Path) -> None:
    registry = json.loads(Path("mission/state/proof_registry.json").read_text())
    registry["entries"][0]["status"] = "AVAILABLE_CONFIRMATION"
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(registry), encoding="utf-8")

    with pytest.raises(ProofRegistryError, match="entry hash mismatch"):
        load_and_verify(path)

