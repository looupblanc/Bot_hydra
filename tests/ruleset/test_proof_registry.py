from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.governance.proof_registry import (
    ProofRegistryError,
    append_entry,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
    verify_registry_prefix,
)


def test_bootstrap_registry_is_hash_chained_and_q4_is_burned() -> None:
    registry = load_and_verify("mission/state/proof_registry.json")

    assert registry["entry_count"] >= 1
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


def test_multiplicity_counter_is_append_only_without_consuming_proof_window(
    tmp_path: Path,
) -> None:
    source = Path("mission/state/proof_registry.json")
    destination = tmp_path / "proof_registry.json"
    destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    current = load_and_verify(destination)
    prior = multiplicity_trial_count(current)
    target = prior + 10
    append_entry(
        destination,
        {
            "event_id": "multiplicity-phase2",
            "event_type": "MULTIPLICITY_COUNTER",
            "recorded_at_utc": "2026-07-12T14:40:00Z",
            "multiplicity": {
                "previous_N_trials": prior,
                "delta_trials": 10,
                "cumulative_N_trials": target,
                "method": "WORM preregistered conservative reconstruction",
            },
            "evidence": {"preregistration_sha256": "a" * 64},
        },
    )

    registry = load_and_verify(destination)
    assert multiplicity_trial_count(registry) == target
    assert burned_window_ids(registry) == ("Q4_2024",)
    with pytest.raises(ProofRegistryError, match="previous counter mismatch"):
        append_entry(
            destination,
            {
                "event_id": "bad-counter",
                "event_type": "MULTIPLICITY_COUNTER",
                "recorded_at_utc": "2026-07-12T14:41:00Z",
                "multiplicity": {
                    "previous_N_trials": 0,
                    "delta_trials": 1,
                    "cumulative_N_trials": 1,
                },
            },
        )


def test_manifest_prefix_anchor_survives_only_valid_registry_extension(
    tmp_path: Path,
) -> None:
    source = Path("mission/state/proof_registry.json")
    destination = tmp_path / "proof_registry.json"
    destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    frozen = load_and_verify(destination)
    frozen_count = int(frozen["entry_count"])
    frozen_head = str(frozen["chain_head"])
    prior = multiplicity_trial_count(frozen)
    append_entry(
        destination,
        {
            "event_id": "extension",
            "event_type": "MULTIPLICITY_COUNTER",
            "recorded_at_utc": "2026-07-12T14:42:00Z",
            "multiplicity": {
                "previous_N_trials": prior,
                "delta_trials": 10,
                "cumulative_N_trials": prior + 10,
            },
        },
    )

    extended = load_and_verify(destination)
    verify_registry_prefix(
        extended, entry_count=frozen_count, chain_head=frozen_head
    )
    with pytest.raises(ProofRegistryError, match="prefix chain head mismatch"):
        verify_registry_prefix(
            extended, entry_count=frozen_count, chain_head="f" * 64
        )
