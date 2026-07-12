from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.governance.proof_registry import append_entry, load_and_verify

from hydra.data.v7_manifest import (
    DataManifestError,
    canonical_hash,
    derive_product_cutoffs,
    verify_v7_data_manifest,
)


def test_product_cutoff_uses_latest_actual_record_not_nominal_end() -> None:
    coverage = {
        "ES": [
            {
                "path": "early.dbn.zst",
                "min_timestamp_ns": 0,
                "max_timestamp_ns": 60_000_000_000,
                "record_count": 2,
            },
            {
                "path": "later.dbn.zst",
                "min_timestamp_ns": 120_000_000_000,
                "max_timestamp_ns": 180_000_000_000,
                "record_count": 2,
            },
        ]
    }

    result = derive_product_cutoffs(coverage)["ES"]

    assert result["cutoff_source"] == "later.dbn.zst"
    assert result["cutoff_utc"] == "1970-01-01T00:03:00Z"
    assert result["gap_start_utc"] == "1970-01-01T00:04:00Z"
    assert result["raw_record_count"] == 4


def test_manifest_hash_is_canonical_and_excludes_its_own_field() -> None:
    payload = {"schema": "x", "nested": {"b": 2, "a": 1}}
    digest = canonical_hash(payload)
    payload["manifest_hash"] = digest

    assert canonical_hash(payload) == digest


def test_manifest_verification_rejects_mutation(tmp_path: Path) -> None:
    artifact = tmp_path / "source.json"
    artifact.write_text("{}\n", encoding="utf-8")
    import hashlib

    artifact_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
    payload = {
        "schema": "hydra_v7_data_lake_manifest_v1",
        "artifacts": [
            {
                "path": "source.json",
                "sha256": artifact_hash,
            }
        ],
        "artifact_count": 1,
        "derived_array_count": 0,
        "product_cutoffs": {"ES": {}},
        "proof_roles": {"Q4_2024": "BURNED"},
        "unclassified_market_data_files": [],
    }
    payload["manifest_hash"] = canonical_hash(payload)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    assert verify_v7_data_manifest(tmp_path, manifest)["valid"] is True
    artifact.write_text("mutated\n", encoding="utf-8")
    with pytest.raises(DataManifestError, match="artifact hash mismatch"):
        verify_v7_data_manifest(tmp_path, manifest)


def test_manifest_accepts_valid_append_only_proof_registry_extension(
    tmp_path: Path,
) -> None:
    import hashlib

    proof_source = Path("mission/state/proof_registry.json")
    proof_dir = tmp_path / "mission/state"
    proof_dir.mkdir(parents=True)
    proof_path = proof_dir / "proof_registry.json"
    proof_path.write_text(proof_source.read_text(encoding="utf-8"), encoding="utf-8")
    frozen = load_and_verify(proof_path)
    frozen_hash = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    payload = {
        "schema": "hydra_v7_data_lake_manifest_v1",
        "artifacts": [],
        "artifact_count": 0,
        "derived_array_count": 0,
        "product_cutoffs": {"ES": {}},
        "proof_roles": {"Q4_2024": "BURNED"},
        "unclassified_market_data_files": [],
        "proof_registry_audit": {
            "path": "mission/state/proof_registry.json",
            "sha256": frozen_hash,
            "entry_count": frozen["entry_count"],
            "chain_head": frozen["chain_head"],
        },
    }
    payload["manifest_hash"] = canonical_hash(payload)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    prior = 0
    for row in frozen["entries"]:
        if row.get("event_type") == "MULTIPLICITY_COUNTER":
            prior = int(row["multiplicity"]["cumulative_N_trials"])
    append_entry(
        proof_path,
        {
            "event_id": "valid-extension",
            "event_type": "MULTIPLICITY_COUNTER",
            "recorded_at_utc": "2026-07-12T14:43:00Z",
            "multiplicity": {
                "previous_N_trials": prior,
                "delta_trials": 1,
                "cumulative_N_trials": prior + 1,
            },
        },
    )

    assert verify_v7_data_manifest(tmp_path, manifest)["valid"] is True
