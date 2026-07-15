from __future__ import annotations

import json
import hashlib
import os
import tempfile
from pathlib import Path

import pytest

from hydra.evidence import (
    EvidenceBundleBusy,
    EvidenceBundleError,
    EvidenceBundleWriter,
    EvidenceContractError,
    IncompleteEvidenceBundle,
    guard_campaign_completion,
    iter_evidence_records,
    recover_finalized_evidence_bundle,
    verify_evidence_bundle,
)
import hydra.evidence.bundle as evidence_bundle_module
from hydra.evidence.schema import (
    EVIDENCE_BUNDLE_CONTRACT,
    EVIDENCE_BUNDLE_SCHEMA_VERSION,
    REQUIRED_COMPACT_OUTPUTS,
    REQUIRED_DATASETS,
)


CAMPAIGN_ID = "economic_production_0024"
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
POLICY_ID = "policy_001"
COMPONENT_ID = "component_001"


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _identity() -> dict[str, object]:
    return {
        "campaign_id": CAMPAIGN_ID,
        "grammar_id": "production_kernel_manifest_v1",
        "policy_fingerprints": {POLICY_ID: HASH_A},
        "component_fingerprints": {COMPONENT_ID: HASH_B},
        "source_commit": "d" * 40,
        "data_fingerprints": {"cached_q1_q3": HASH_C},
        "configuration_sha256": HASH_A,
        "seeds": [7, 11],
        "created_at_utc": "2026-07-14T12:00:00Z",
        "expected_coverage": {
            "policy_ids": [POLICY_ID],
            "component_ids": [COMPONENT_ID],
            "required_episode_keys": [
                {
                    "policy_id": POLICY_ID,
                    "episode_id": "episode_001",
                    "horizon": "40D",
                }
            ],
            "allowed_horizons": ["40D"],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": False,
        },
    }


def _records(*, reconstruction: bool = False) -> dict[str, list[dict[str, object]]]:
    signal = {
        "campaign_id": CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "signal_id": "signal_001",
        "event_time": "2024-01-02T14:30:00Z",
        "market": "MES",
        "contract": "MESH4",
        "timeframe": "1m",
        "signal": 1,
        "sizing": 1.0,
        "stop": 4990.0,
        "target": 5020.0,
        "veto": False,
        "component_role": "TARGET_VELOCITY",
    }
    entry = {
        "campaign_id": CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "trade_id": "trade_001",
        "entry_time": "2024-01-02T14:31:00Z",
        "market": "MES",
        "contract": "MESH4",
        "side": "LONG",
        "quantity": 1.0,
        "entry_price": 5000.0,
        "sizing": 1.0,
        "stop_price": 4990.0,
        "target_price": 5020.0,
    }
    exit_row = {
        "campaign_id": CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "trade_id": "trade_001",
        "exit_time": "2024-01-02T15:00:00Z",
        "exit_price": 5005.0,
        "exit_reason": "TARGET_HORIZON_EXIT",
    }
    trade = {
        "campaign_id": CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "trade_id": "trade_001",
        "entry_time": "2024-01-02T14:31:00Z",
        "exit_time": "2024-01-02T15:00:00Z",
        "market": "MES",
        "contract": "MESH4",
        "side": "LONG",
        "quantity": 1.0,
        "entry_price": 5000.0,
        "exit_price": 5005.0,
        "gross_pnl": 25.0,
        "costs": 2.5,
        "net_pnl": 22.5,
    }
    membership = {
        "campaign_id": CAMPAIGN_ID,
        "policy_id": POLICY_ID,
        "component_id": COMPONENT_ID,
        "risk_allocation": 1.0,
        "component_role": "TARGET_VELOCITY",
    }
    episode_rows: list[dict[str, object]] = []
    path_rows: list[dict[str, object]] = []
    for scenario, costs in (("NORMAL", 2.5), ("STRESSED_1_5X", 3.75)):
        episode_rows.append(
            {
                "campaign_id": CAMPAIGN_ID,
                "policy_id": POLICY_ID,
                "episode_id": "episode_001",
                "episode_start": "2024-01-02T00:00:00Z",
                "horizon": "40D",
                "temporal_block": "block_01",
                "duration_trading_days": 1,
                "target_reached": True,
                "mll_breached": False,
                "censored_state": False,
                "cost_scenario": scenario,
                "costs": costs,
                "net_pnl": 25.0 - costs,
                "target_progress": 1.0,
                "minimum_mll_buffer": 2_000.0,
                "consistency_ok": True,
                "days_to_target": 1.0,
                "failure_vector": [],
                "terminal_state": "TARGET_REACHED",
            }
        )
        path_rows.append(
            {
                "campaign_id": CAMPAIGN_ID,
                "policy_id": POLICY_ID,
                "episode_id": "episode_001",
                "horizon": "40D",
                "trading_day": "2024-01-02",
                "cost_scenario": scenario,
                "realized_pnl": 25.0 - costs,
                "unrealized_pnl": 0.0,
                "daily_pnl": 25.0 - costs,
                "equity": 50_000.0 + 25.0 - costs,
                "mll": 50_000.0 + 25.0 - costs - 2_000.0,
                "mll_buffer": 2_000.0,
                "minimum_mll_buffer": 2_000.0,
                "consistency": 1.0,
                "consistency_ok": True,
                "target_progress": 1.0,
                "costs": costs,
                "conflicts": [],
                "exposure": {"MES": 1.0},
                "component_attribution": {COMPONENT_ID: 25.0 - costs},
            }
        )
    provenance = {
        "campaign_id": CAMPAIGN_ID,
        "validator_version": "evidence_bundle_v1",
        "replay_version": "reference_replay_v1",
        "market_data_role": "DEVELOPMENT_ONLY",
        "access_ledger_sha256": HASH_A,
        "reconstruction_flag": reconstruction,
        "immutable_checksums": {
            "configuration": HASH_A,
            "data:cached_q1_q3": HASH_C,
        },
        "recorded_at_utc": "2026-07-14T12:01:00Z",
    }
    return {
        "component_signals": [signal],
        "component_entries": [entry],
        "component_exits": [exit_row],
        "component_trades": [trade],
        "account_policy_membership": [membership],
        "account_daily_paths": path_rows,
        "episodes": episode_rows,
        "provenance": [provenance],
    }


def _populate(writer: EvidenceBundleWriter, *, reconstruction: bool = False) -> None:
    _populate_records(writer, _records(reconstruction=reconstruction))


def _populate_records(
    writer: EvidenceBundleWriter,
    records: dict[str, list[dict[str, object]]],
) -> None:
    for dataset, rows in records.items():
        writer.append_records(dataset, rows, batch_id=f"{dataset}-batch-0000")
    writer.write_compact_output("campaign_summary", {"policies": 1, "episodes": 2})
    writer.write_compact_output("failure_vectors", [])
    writer.write_compact_output("pareto_archive", [{"policy_id": POLICY_ID}])
    writer.write_compact_output(
        "next_campaign_recommendations", {"action": "CONTINUE_SUCCESSIVE_HALVING"}
    )


def _seal(
    tmp_path: Path,
    *,
    reconstruction: bool = False,
    status: str | None = None,
):
    writer = EvidenceBundleWriter.create(tmp_path / "cache", _identity(), writer_id="writer-1")
    _populate(writer, reconstruction=reconstruction)
    receipt = writer.finalize(
        evidence_status=status
        or (
            "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"
            if reconstruction
            else "FRESH_DEVELOPMENT_EVIDENCE"
        ),
        lightweight_manifest_path=tmp_path / "reports" / "receipt.json",
    )
    return receipt


def test_seals_verifies_and_emits_lightweight_receipt(tmp_path: Path) -> None:
    receipt = _seal(tmp_path)
    manifest = verify_evidence_bundle(receipt.bundle_path)

    assert manifest["status"] == "COMPLETE"
    assert manifest["reconstruction_flag"] is False
    assert manifest["dataset_row_counts"]["episodes"] == 2
    assert len(list(iter_evidence_records(receipt.bundle_path, "episodes"))) == 2
    lightweight = json.loads((tmp_path / "reports" / "receipt.json").read_text())
    assert lightweight["manifest_sha256"] == receipt.manifest_sha256
    assert lightweight["large_payloads_git_ignored"] is True
    guarded = guard_campaign_completion(
        "COMPLETE", receipt.bundle_path, campaign_id=CAMPAIGN_ID
    )
    assert guarded is not None and guarded["status"] == "COMPLETE"


def test_summary_only_campaign_cannot_complete(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    for name in (
        "campaign_summary",
        "failure_vectors",
        "pareto_archive",
        "next_campaign_recommendations",
    ):
        writer.write_compact_output(name, {})

    with pytest.raises(IncompleteEvidenceBundle, match="summary-only completion forbidden"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()

    with pytest.raises(IncompleteEvidenceBundle, match="forbidden"):
        guard_campaign_completion("COMPLETE", None, campaign_id=CAMPAIGN_ID)


def test_single_writer_lock_and_resumable_staging(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity(), writer_id="stable-writer")
    writer.append_records(
        "component_signals",
        _records()["component_signals"],
        batch_id="signals-0",
    )
    with pytest.raises(EvidenceBundleBusy):
        EvidenceBundleWriter.resume(tmp_path, CAMPAIGN_ID, writer_id="stable-writer")
    writer.close()

    checkpoint_path = tmp_path / f".{CAMPAIGN_ID}.evidence-v1.staging" / "checkpoint.json"
    stale = json.loads(checkpoint_path.read_text())
    stale["dataset_parts"]["component_signals"] = []
    stale["dataset_row_counts"]["component_signals"] = 0
    checkpoint_path.write_text(json.dumps(stale), encoding="utf-8")

    resumed = EvidenceBundleWriter.resume(tmp_path, CAMPAIGN_ID, writer_id="stable-writer")
    assert resumed.dataset_row_counts["component_signals"] == 1
    duplicate = resumed.append_records(
        "component_signals",
        _records()["component_signals"],
        batch_id="signals-0",
    )
    assert duplicate.part_index == 0
    changed = dict(_records()["component_signals"][0])
    changed["signal"] = -1
    with pytest.raises(EvidenceBundleError, match="different evidence"):
        resumed.append_records("component_signals", [changed], batch_id="signals-0")
    resumed.close()


def test_reconstruction_status_is_bound_to_provenance(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path / "cache", _identity())
    _populate(writer, reconstruction=True)
    with pytest.raises(EvidenceContractError, match="AUTHORITATIVE"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()

    receipt = _seal(tmp_path / "accepted", reconstruction=True)
    manifest = verify_evidence_bundle(receipt.bundle_path)
    assert manifest["reconstruction_flag"] is True
    assert manifest["evidence_status"] == "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION"


def test_checksum_tampering_is_detected(tmp_path: Path) -> None:
    receipt = _seal(tmp_path)
    bundle = Path(receipt.bundle_path)
    part = next((bundle / "datasets" / "component_signals").glob("*.jsonl.gz"))
    part.chmod(0o644)
    with part.open("ab") as handle:
        handle.write(b"tamper")

    with pytest.raises(IncompleteEvidenceBundle, match="size drift"):
        verify_evidence_bundle(bundle, deep=False)


def test_record_contract_rejects_nonfinite_account_evidence(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    bad = dict(_records()["account_daily_paths"][0])
    bad["mll_buffer"] = float("nan")
    with pytest.raises(EvidenceContractError, match="canonical JSON"):
        writer.append_records("account_daily_paths", [bad])
    writer.close()


def test_completion_requires_paired_cost_scenarios_and_paths(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    records["episodes"] = [records["episodes"][0]]
    records["account_daily_paths"] = [records["account_daily_paths"][0]]
    for dataset, rows in records.items():
        writer.append_records(dataset, rows, batch_id=dataset)
    for name in (
        "campaign_summary",
        "failure_vectors",
        "pareto_archive",
        "next_campaign_recommendations",
    ):
        writer.write_compact_output(name, {})

    with pytest.raises(IncompleteEvidenceBundle, match="exactly NORMAL and STRESSED_1_5X"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_completion_rejects_unknown_policy_membership(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    records["account_policy_membership"][0]["policy_id"] = "unfrozen_policy"
    for dataset, rows in records.items():
        writer.append_records(dataset, rows, batch_id=dataset)
    for name in (
        "campaign_summary",
        "failure_vectors",
        "pareto_archive",
        "next_campaign_recommendations",
    ):
        writer.write_compact_output(name, {})

    with pytest.raises(IncompleteEvidenceBundle, match="unknown immutable fingerprint"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("entry_price", "disagrees with entry field entry_price"),
        ("trade_arithmetic", "gross minus costs equals net"),
    ),
)
def test_trade_ledgers_must_reconcile_fields_and_economics(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    if mutation == "entry_price":
        records["component_entries"][0]["entry_price"] = 5_001.0
    else:
        records["component_trades"][0]["net_pnl"] = 24.0
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match=message):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_target_reached_rejects_economically_inconsistent_daily_path(
    tmp_path: Path,
) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    records["episodes"][0]["net_pnl"] = 9_999.0
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match="cumulative daily PnL"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_target_reached_requires_full_progress_and_valid_days(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    records["episodes"][0]["target_progress"] = 0.75
    records["account_daily_paths"][0]["target_progress"] = 0.75
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match="below full target progress"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_intraday_mll_minimum_reconciles_separately_from_closing_buffer(
    tmp_path: Path,
) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    records["episodes"][0]["minimum_mll_buffer"] = 100.0
    records["account_daily_paths"][0]["minimum_mll_buffer"] = 100.0
    _populate_records(writer, records)

    receipt = writer.finalize(
        evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
        lightweight_manifest_path=tmp_path / "receipt.json",
    )

    assert receipt.dataset_row_counts["account_daily_paths"] == 2


def test_mll_terminal_requires_nonpositive_observed_buffer(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    episode = records["episodes"][0]
    episode.update(
        {
            "target_reached": False,
            "mll_breached": True,
            "terminal_state": "MLL_BREACHED",
            "target_progress": 0.5,
            "days_to_target": None,
            "minimum_mll_buffer": 100.0,
        }
    )
    path = records["account_daily_paths"][0]
    path["target_progress"] = 0.5
    path["mll_buffer"] = 100.0
    path["minimum_mll_buffer"] = 100.0
    path["mll"] = float(path["equity"]) - 100.0
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match="positive buffer"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_completion_guard_deeply_rejects_self_consistent_manifest_without_ledgers(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "forged.evidence-v1"
    bundle.mkdir()
    identity_path = bundle / "identity.json"
    identity_path.write_bytes(_canonical_bytes(_identity()))
    identity_sha = hashlib.sha256(identity_path.read_bytes()).hexdigest()
    files = {
        "identity.json": {
            "kind": "identity",
            "sha256": identity_sha,
            "size_bytes": identity_path.stat().st_size,
        }
    }
    core = {
        "contract": EVIDENCE_BUNDLE_CONTRACT,
        "schema_version": EVIDENCE_BUNDLE_SCHEMA_VERSION,
        "status": "COMPLETE",
        "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
        "campaign_id": CAMPAIGN_ID,
        "identity_sha256": identity_sha,
        "reconstruction_flag": False,
        "dataset_row_counts": {name: 1 for name in REQUIRED_DATASETS},
        "compact_outputs": {name: {} for name in REQUIRED_COMPACT_OUTPUTS},
        "files": files,
    }
    manifest = {
        **core,
        "bundle_content_sha256": hashlib.sha256(_canonical_bytes(core)).hexdigest(),
    }
    (bundle / "evidence_bundle_manifest.json").write_bytes(
        _canonical_bytes(manifest)
    )

    with pytest.raises(IncompleteEvidenceBundle):
        guard_campaign_completion("COMPLETE", bundle, campaign_id=CAMPAIGN_ID)


def test_every_fingerprinted_component_requires_full_component_ledgers(
    tmp_path: Path,
) -> None:
    identity = _identity()
    identity["component_fingerprints"]["component_without_trades"] = "9" * 64
    identity["expected_coverage"]["component_ids"].append("component_without_trades")
    records = _records()
    signal = dict(records["component_signals"][0])
    signal.update(
        {
            "component_id": "component_without_trades",
            "signal_id": "signal_without_trade",
        }
    )
    records["component_signals"].append(signal)
    membership = dict(records["account_policy_membership"][0])
    membership["component_id"] = "component_without_trades"
    records["account_policy_membership"].append(membership)
    writer = EvidenceBundleWriter.create(tmp_path, identity)
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match="component_entries evidence"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_every_fingerprinted_policy_requires_declared_base_episodes(
    tmp_path: Path,
) -> None:
    identity = _identity()
    identity["policy_fingerprints"]["policy_without_episode"] = "8" * 64
    coverage = identity["expected_coverage"]
    coverage["policy_ids"].append("policy_without_episode")
    coverage["required_episode_keys"].append(
        {
            "policy_id": "policy_without_episode",
            "episode_id": "missing_episode",
            "horizon": "40D",
        }
    )
    records = _records()
    membership = dict(records["account_policy_membership"][0])
    membership["policy_id"] = "policy_without_episode"
    records["account_policy_membership"].append(membership)
    writer = EvidenceBundleWriter.create(tmp_path, identity)
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match="policies lack episode evidence"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_cost_scenario_names_are_frozen_exactly(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    bad_path = dict(_records()["account_daily_paths"][0])
    bad_path["cost_scenario"] = "ABNORMAL"
    with pytest.raises(EvidenceContractError, match="NORMAL or STRESSED_1_5X"):
        writer.append_records("account_daily_paths", [bad_path])
    writer.close()


def test_account_mll_buffer_must_equal_equity_minus_mll(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    records["account_daily_paths"][0]["mll"] = 999_999.0
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match="equity minus mll"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_component_attribution_requires_known_members_and_terminal_reconciliation(
    tmp_path: Path,
) -> None:
    records = _records()
    records["account_daily_paths"][0]["component_attribution"] = {
        "unknown_component": 22.5
    }
    writer = EvidenceBundleWriter.create(tmp_path / "unknown", _identity())
    _populate_records(writer, records)
    with pytest.raises(IncompleteEvidenceBundle, match="unknown component"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "unknown-receipt.json",
        )
    writer.close()

    records = _records()
    records["account_daily_paths"][0]["component_attribution"] = {
        COMPONENT_ID: 0.0
    }
    writer = EvidenceBundleWriter.create(tmp_path / "sum", _identity())
    _populate_records(writer, records)
    with pytest.raises(IncompleteEvidenceBundle, match="attribution disagrees with net_pnl"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "sum-receipt.json",
        )
    writer.close()


def test_multiday_component_attribution_reconciles_cumulatively(
    tmp_path: Path,
) -> None:
    records = _records()
    paths: list[dict[str, object]] = []
    for episode in records["episodes"]:
        episode["duration_trading_days"] = 2
        episode["days_to_target"] = 2.0
    for original in records["account_daily_paths"]:
        net = float(original["daily_pnl"])
        costs = float(original["costs"])
        first = dict(original)
        first["daily_pnl"] = net / 2.0
        first["realized_pnl"] = net / 2.0
        first["equity"] = 50_000.0 + net / 2.0
        first["mll"] = float(first["equity"]) - 2_000.0
        first["target_progress"] = 0.5
        first["costs"] = costs / 2.0
        first["component_attribution"] = {COMPONENT_ID: net / 2.0}

        second = dict(original)
        second["trading_day"] = "2024-01-03"
        second["daily_pnl"] = net / 2.0
        second["costs"] = costs / 2.0
        second["component_attribution"] = {COMPONENT_ID: net / 2.0}
        paths.extend((first, second))
    records["account_daily_paths"] = paths

    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    _populate_records(writer, records)
    receipt = writer.finalize(
        evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
        lightweight_manifest_path=tmp_path / "receipt.json",
    )
    writer.close()

    assert receipt.dataset_row_counts["account_daily_paths"] == 4


def test_provenance_checksums_are_bound_to_identity(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    records = _records()
    records["provenance"][0]["immutable_checksums"]["configuration"] = "0" * 64
    _populate_records(writer, records)

    with pytest.raises(IncompleteEvidenceBundle, match="disagrees with identity"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_evidence_status_is_closed_enum(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity())
    _populate(writer)
    with pytest.raises(EvidenceContractError, match="evidence_status must be"):
        writer.finalize(
            evidence_status="UNVERIFIED_BUT_COMPLETE",
            lightweight_manifest_path=tmp_path / "receipt.json",
        )
    writer.close()


def test_git_ignore_enforcement_cannot_be_disabled(tmp_path: Path) -> None:
    with pytest.raises(EvidenceContractError, match="may not be disabled"):
        EvidenceBundleWriter.create(
            tmp_path,
            _identity(),
            require_git_ignored=False,
        )
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
        with pytest.raises(EvidenceBundleError, match="must be ignored by Git"):
            EvidenceBundleWriter.create(Path(directory) / "payload", _identity())


def test_resume_cleans_only_owned_regular_atomic_temp_files(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity(), writer_id="stable")
    writer.append_records(
        "component_signals",
        _records()["component_signals"],
        batch_id="signals",
    )
    staging = writer.staging_dir
    writer.close()
    owned = staging / f".checkpoint.json.tmp-999-{'f' * 32}"
    owned.write_text("partial", encoding="utf-8")

    resumed = EvidenceBundleWriter.resume(tmp_path, CAMPAIGN_ID, writer_id="stable")
    assert not owned.exists()
    assert resumed.dataset_row_counts["component_signals"] == 1
    resumed.close()


def test_resume_binds_staging_to_expected_identity(tmp_path: Path) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity(), writer_id="stable")
    writer.close()
    changed = _identity()
    changed["seeds"] = [99]

    with pytest.raises(EvidenceBundleError, match="expected resume identity"):
        EvidenceBundleWriter.resume(
            tmp_path,
            CAMPAIGN_ID,
            writer_id="stable",
            expected_identity=changed,
        )


@pytest.mark.parametrize("unsafe_kind", ("unknown", "symlink"))
def test_resume_rejects_unknown_or_nonregular_debris(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    writer = EvidenceBundleWriter.create(tmp_path, _identity(), writer_id="stable")
    staging = writer.staging_dir
    writer.close()
    if unsafe_kind == "unknown":
        (staging / ".checkpoint.json.tmp-crash").write_text(
            "partial", encoding="utf-8"
        )
        message = "unexpected or unsafe"
    else:
        debris = staging / f".checkpoint.json.tmp-999-{'e' * 32}"
        debris.symlink_to(staging / "checkpoint.json")
        message = "not a regular file"

    with pytest.raises(EvidenceBundleError, match=message):
        EvidenceBundleWriter.resume(tmp_path, CAMPAIGN_ID, writer_id="stable")
    assert not (tmp_path / f"{CAMPAIGN_ID}.evidence-v1").exists()


def test_post_rename_crash_recovers_valid_final_and_projects_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "cache"
    receipt_path = tmp_path / "reports" / "receipt.json"
    writer = EvidenceBundleWriter.create(base, _identity(), writer_id="stable")
    _populate(writer)
    staging = writer.staging_dir
    real_replace = evidence_bundle_module.os.replace

    def crash_after_bundle_rename(source: object, destination: object) -> None:
        real_replace(source, destination)
        if Path(source) == staging:
            raise OSError("simulated crash after atomic bundle rename")

    monkeypatch.setattr(
        evidence_bundle_module.os,
        "replace",
        crash_after_bundle_rename,
    )
    with pytest.raises(OSError, match="simulated crash"):
        writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=receipt_path,
        )
    monkeypatch.setattr(evidence_bundle_module.os, "replace", real_replace)

    final = base / f"{CAMPAIGN_ID}.evidence-v1"
    assert final.is_dir()
    assert not receipt_path.exists()
    receipt_temp = receipt_path.parent / f".receipt.json.tmp-999-{'d' * 32}"
    receipt_temp.parent.mkdir(parents=True, exist_ok=True)
    receipt_temp.write_text("partial", encoding="utf-8")

    recovered = recover_finalized_evidence_bundle(
        base,
        CAMPAIGN_ID,
        lightweight_manifest_path=receipt_path,
        expected_identity=_identity(),
    )
    assert recovered.bundle_path == str(final)
    assert receipt_path.is_file()
    assert not receipt_temp.exists()
    assert all(path.stat().st_mode & 0o222 == 0 for path in final.rglob("*") if path.is_file())
    repeated = recover_finalized_evidence_bundle(
        base,
        CAMPAIGN_ID,
        lightweight_manifest_path=receipt_path,
        expected_identity=_identity(),
    )
    assert repeated == recovered


def test_recovery_fails_closed_on_receipt_drift(tmp_path: Path) -> None:
    receipt = _seal(tmp_path)
    receipt_path = tmp_path / "reports" / "receipt.json"
    drifted = json.loads(receipt_path.read_text(encoding="utf-8"))
    drifted["manifest_sha256"] = "0" * 64
    receipt_path.write_text(json.dumps(drifted), encoding="utf-8")

    with pytest.raises(EvidenceBundleError, match="receipt disagrees"):
        recover_finalized_evidence_bundle(
            Path(receipt.bundle_path).parent,
            CAMPAIGN_ID,
            lightweight_manifest_path=receipt_path,
            expected_identity=_identity(),
        )
