from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from hydra.evidence import iter_evidence_records, verify_evidence_bundle
from hydra.evidence.causal_salvage_adapter import (
    CausalSalvageEvidenceError,
    finalize_causal_salvage_evidence_bundle,
    finalize_causal_salvage_evidence_bundle_streaming,
    materialize_causal_salvage_evidence,
)
from hydra.research.causal_sleeve_replay import (
    CAUSAL_DECISION_KERNEL_VERSION,
    CENSORED_FUTURE_COVERAGE,
    TARGET_OBSERVED,
)


CAMPAIGN_ID = "causal_salvage_adapter_test"
POLICY_ID = "active_pool_test"
SLEEVE_ID = "sleeve_test"
COMPONENT_HASH = "b" * 64
POLICY_HASH = "a" * 64
FILL_HASH = "f" * 64
BASE_NS = 1_700_000_000_000_000_000


def _signal(signal_id: str, *, censored: bool) -> dict[str, object]:
    offset = 600_000_000_000 if censored else 0
    signal_ns = BASE_NS + offset
    fill_ns = None if censored else signal_ns + 120_000_000_000
    exit_ns = None if censored else signal_ns + 420_000_000_000
    return {
        "signal_id": signal_id,
        "sleeve_id": SLEEVE_ID,
        "signal_time_ns": signal_ns,
        "decision_time_ns": signal_ns + 60_000_000_000,
        "order_submit_time_ns": signal_ns + 60_000_000_000,
        "earliest_executable_time_ns": signal_ns + 120_000_000_000,
        "fill_time_ns": fill_ns,
        "raw_entry_open": None if censored else 100.0,
        "normal_entry_fill_price": None if censored else 100.25,
        "stressed_entry_fill_price": None if censored else 100.5,
        "exit_decision_time_ns": fill_ns,
        "exit_order_submit_time_ns": fill_ns,
        "exit_earliest_executable_time_ns": exit_ns,
        "exit_fill_time_ns": exit_ns,
        "raw_exit_open": None if censored else 105.0,
        "normal_exit_fill_price": None if censored else 104.75,
        "stressed_exit_fill_price": None if censored else 104.5,
        "session_day": 19_724,
        "segment_code": 1,
        "contract_code": 17,
        "direction": 1,
        "quantity": 1,
        "outcome_status": CENSORED_FUTURE_COVERAGE if censored else TARGET_OBSERVED,
        "censor_reason": "NO_FUTURE_BAR" if censored else None,
        "trigger_value": 1.5,
        "context_value": 0.5,
        "kernel_version": CAUSAL_DECISION_KERNEL_VERSION,
        "fill_policy_id": "CAUSAL_NEXT_TRADABLE_OPEN_V1",
        "fill_policy_hash": FILL_HASH,
        "fingerprint": ("9" if censored else "8") * 64,
    }


def _replay() -> dict[str, object]:
    observed_id = f"{SLEEVE_ID}:CAUSAL:00001:{BASE_NS}"
    censored_id = f"{SLEEVE_ID}:CAUSAL:00002:{BASE_NS + 600_000_000_000}"
    return {
        "sleeve_id": SLEEVE_ID,
        "signal_count": 2,
        "completed_trade_count": 1,
        "censored_signal_count": 1,
        "signals": [
            _signal(observed_id, censored=False),
            _signal(censored_id, censored=True),
        ],
        "normal_events": [
            {
                "event_id": f"{observed_id}:NORMAL",
                "gross_pnl": 25.0,
                "net_pnl": 23.0,
            }
        ],
        "stressed_events": [
            {
                "event_id": f"{observed_id}:STRESSED_1_5X",
                "gross_pnl": 20.0,
                "net_pnl": 18.0,
            }
        ],
        "decision_hash": "1" * 64,
        "normal_event_hash": "2" * 64,
        "stressed_event_hash": "3" * 64,
        "fill_policy_hash": FILL_HASH,
        "specification_hash": COMPONENT_HASH,
    }


def _identity() -> dict[str, object]:
    return {
        "campaign_id": CAMPAIGN_ID,
        "grammar_id": "causal_salvage_runner_v1",
        "policy_fingerprints": {POLICY_ID: POLICY_HASH},
        "component_fingerprints": {SLEEVE_ID: COMPONENT_HASH},
        "source_commit": "d" * 40,
        "data_fingerprints": {"pre_freeze_cache": "c" * 64},
        "configuration_sha256": "4" * 64,
        "seeds": [17],
        "created_at_utc": "2026-07-17T00:00:00Z",
        "expected_coverage": {
            "policy_ids": [POLICY_ID],
            "component_ids": [SLEEVE_ID],
            "required_episode_keys": [
                {
                    "policy_id": POLICY_ID,
                    "episode_id": "episode_001",
                    "horizon": "90_TRADING_DAYS",
                }
            ],
            "allowed_horizons": ["90_TRADING_DAYS"],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": False,
        },
    }


def _episode(*, scenario: str) -> dict[str, object]:
    costs = 2.0 if scenario == "NORMAL" else 3.0
    return {
        "policy_id": POLICY_ID,
        "start_day": 19_724,
        "terminal": "PASSED",
        "eligible_days": 1,
        "total_cost": costs,
        "net_pnl": 9_000.0,
        "target_progress": 1.0,
        "minimum_mll_buffer": 4_000.0,
        "consistency_ok": True,
        "days_to_target": 1,
        "component_contribution": {SLEEVE_ID: 9_000.0},
        "terminal_reason": "profit_target_reached",
        "accepted_events": 1,
        "skipped_events": 0,
        "risk_allocation_path": [],
        "daily_path": [
            {
                "session_day": 19_724,
                "balance": 159_000.0,
                "mll_floor": 154_500.0,
                "mll_buffer": 4_500.0,
                "minimum_mll_buffer": 4_000.0,
                "day_pnl": 9_000.0,
                "realized_pnl": 9_000.0,
                "unrealized_pnl": 0.0,
                "costs": costs,
                "target_progress": 1.0,
                "consistency": 0.4,
                "consistency_ok": True,
                "conflicts": {},
                "exposure": {"maximum_mini_equivalent": 1.0},
                "component_attribution": {SLEEVE_ID: 9_000.0},
                "open_positions": 0,
            }
        ],
    }


def _evaluated_records() -> list[dict[str, object]]:
    return [
        {
            "policy_id": POLICY_ID,
            "episode_id": "episode_001",
            "scenario": scenario,
            "horizon": "90_TRADING_DAYS",
            "temporal_block": "B1",
            "episode": _episode(scenario=scenario),
        }
        for scenario in ("NORMAL", "STRESSED_1_5X")
    ]


def _policy() -> dict[str, object]:
    return {
        "policy_id": POLICY_ID,
        "component_ids": [SLEEVE_ID],
        "static_risk_tier": 1.0,
        "structural_fingerprint": POLICY_HASH,
    }


def _spec() -> dict[str, object]:
    return {
        "sleeve_id": SLEEVE_ID,
        "market": "ES",
        "execution_market": "MES",
        "timeframe": "5m",
        "role": "TARGET_VELOCITY",
        "side": 1,
        "holding_bars": 5,
    }


def _provenance() -> dict[str, object]:
    return {
        "access_ledger_sha256": "5" * 64,
        "recorded_at_utc": "2026-07-17T00:01:00Z",
        "market_data_role": "PRE_FREEZE_DEVELOPMENT_CACHE",
    }


def test_finalizes_valid_causal_bundle_and_keeps_censor_signal_only(
    tmp_path: Path,
) -> None:
    receipt = finalize_causal_salvage_evidence_bundle(
        base_dir=tmp_path / "payload",
        lightweight_manifest_path=tmp_path / "receipt.json",
        identity=_identity(),
        causal_replays={SLEEVE_ID: _replay()},
        evaluated_policy_records=_evaluated_records(),
        policies={POLICY_ID: _policy()},
        sleeve_specs={SLEEVE_ID: _spec()},
        provenance=_provenance(),
    )

    manifest = verify_evidence_bundle(receipt.bundle_path, deep=True)
    assert manifest["evidence_status"] == "FRESH_DEVELOPMENT_EVIDENCE"
    signals = list(iter_evidence_records(receipt.bundle_path, "component_signals"))
    trades = list(iter_evidence_records(receipt.bundle_path, "component_trades"))
    assert len(signals) == 2
    assert len(trades) == 1
    assert trades[0]["stressed_net_pnl"] == 18.0
    assert signals[0]["decision_time"] != signals[0]["fill_time"]
    censored_id = next(
        row["signal_id"]
        for row in signals
        if row["outcome_status"] == CENSORED_FUTURE_COVERAGE
    )
    assert all(row["trade_id"] != censored_id for row in trades)
    assert manifest["dataset_row_counts"]["account_daily_paths"] == 2
    assert manifest["dataset_row_counts"]["episodes"] == 2


def test_missing_enriched_daily_field_fails_closed() -> None:
    records = deepcopy(_evaluated_records())
    del records[0]["episode"]["daily_path"][0]["component_attribution"]

    with pytest.raises(
        CausalSalvageEvidenceError,
        match="lacks exact enriched fields: component_attribution",
    ):
        materialize_causal_salvage_evidence(
            identity=_identity(),
            causal_replays={SLEEVE_ID: _replay()},
            evaluated_policy_records=records,
            policies={POLICY_ID: _policy()},
            sleeve_specs={SLEEVE_ID: _spec()},
            provenance=_provenance(),
        )


def test_component_without_completed_trade_cannot_claim_complete_bundle() -> None:
    replay = _replay()
    replay["signals"] = [replay["signals"][1]]
    replay["signal_count"] = 1
    replay["completed_trade_count"] = 0
    replay["normal_events"] = []
    replay["stressed_events"] = []

    with pytest.raises(
        CausalSalvageEvidenceError,
        match="requires at least one completed causal trade",
    ):
        materialize_causal_salvage_evidence(
            identity=_identity(),
            causal_replays={SLEEVE_ID: replay},
            evaluated_policy_records=_evaluated_records(),
            policies={POLICY_ID: _policy()},
            sleeve_specs={SLEEVE_ID: _spec()},
            provenance=_provenance(),
        )


def test_filled_censor_persists_entry_without_fabricating_exit_or_trade(
    tmp_path: Path,
) -> None:
    replay = _replay()
    censored = replay["signals"][1]
    censored["fill_time_ns"] = BASE_NS + 720_000_000_000
    censored["raw_entry_open"] = 101.0
    censored["normal_entry_fill_price"] = 101.25
    censored["stressed_entry_fill_price"] = 101.5

    receipt = finalize_causal_salvage_evidence_bundle(
        base_dir=tmp_path / "payload",
        lightweight_manifest_path=tmp_path / "receipt.json",
        identity=_identity(),
        causal_replays={SLEEVE_ID: replay},
        evaluated_policy_records=_evaluated_records(),
        policies={POLICY_ID: _policy()},
        sleeve_specs={SLEEVE_ID: _spec()},
        provenance=_provenance(),
    )

    entries = list(iter_evidence_records(receipt.bundle_path, "component_entries"))
    exits = list(iter_evidence_records(receipt.bundle_path, "component_exits"))
    trades = list(iter_evidence_records(receipt.bundle_path, "component_trades"))
    assert len(entries) == 2
    assert len(exits) == 1
    assert len(trades) == 1
    orphan = next(row for row in entries if row.get("trade_materialized") is False)
    assert orphan["outcome_status"] == CENSORED_FUTURE_COVERAGE
    assert orphan["open_position_unresolved"] is True
    assert orphan["trade_id"] not in {row["trade_id"] for row in exits}
    assert orphan["trade_id"] not in {row["trade_id"] for row in trades}


def test_streaming_seal_deduplicates_identical_records_across_chunks(
    tmp_path: Path,
) -> None:
    records = _evaluated_records()
    receipt = finalize_causal_salvage_evidence_bundle_streaming(
        base_dir=tmp_path / "payload",
        lightweight_manifest_path=tmp_path / "receipt.json",
        identity=_identity(),
        causal_replays={SLEEVE_ID: _replay()},
        evaluated_policy_record_chunks=[
            [records[0]],
            records[0],
            [records[1]],
        ],
        policies={POLICY_ID: _policy()},
        sleeve_specs={SLEEVE_ID: _spec()},
        provenance=_provenance(),
        episode_batch_size=1,
        daily_path_batch_size=1,
    )

    manifest = verify_evidence_bundle(receipt.bundle_path, deep=True)
    assert manifest["dataset_row_counts"]["episodes"] == 2
    assert manifest["dataset_row_counts"]["account_daily_paths"] == 2
    assert manifest["datasets"]["episodes"]["partition_count"] == 2
    assert manifest["datasets"]["account_daily_paths"]["partition_count"] == 2
    output_path = (
        Path(receipt.bundle_path)
        / manifest["compact_outputs"]["campaign_summary"]["relative_path"]
    )
    summary = json.loads(output_path.read_text(encoding="utf-8"))
    assert summary["context"]["streaming_adapter"] is True
    assert summary["context"]["input_episode_record_count"] == 3
    assert len(summary["context"]["input_episode_sequence_sha256"]) == 64
    assert summary["context"]["unique_episode_record_count"] == 2
    assert summary["context"]["duplicate_episode_record_count"] == 1


def test_streaming_seal_rejects_same_key_with_divergent_hash(
    tmp_path: Path,
) -> None:
    records = _evaluated_records()
    divergent = deepcopy(records[0])
    episode = divergent["episode"]
    episode["net_pnl"] = 9_001.0
    episode["component_contribution"][SLEEVE_ID] = 9_001.0
    day = episode["daily_path"][0]
    day["balance"] = 159_001.0
    day["mll_buffer"] = 4_501.0
    day["day_pnl"] = 9_001.0
    day["realized_pnl"] = 9_001.0
    day["component_attribution"][SLEEVE_ID] = 9_001.0

    with pytest.raises(
        CausalSalvageEvidenceError,
        match="duplicate causal episode key has divergent evidence",
    ):
        finalize_causal_salvage_evidence_bundle_streaming(
            base_dir=tmp_path / "payload",
            lightweight_manifest_path=tmp_path / "receipt.json",
            identity=_identity(),
            causal_replays={SLEEVE_ID: _replay()},
            evaluated_policy_record_chunks=[[records[0]], [divergent], [records[1]]],
            policies={POLICY_ID: _policy()},
            sleeve_specs={SLEEVE_ID: _spec()},
            provenance=_provenance(),
            episode_batch_size=1,
            daily_path_batch_size=1,
        )


def test_explicit_future_censor_wins_over_completed_requested_duration() -> None:
    records = deepcopy(_evaluated_records())
    for record in records:
        record["requested_duration_trading_days"] = 1
        episode = record["episode"]
        episode["terminal"] = "TIMEOUT"
        episode["terminal_reason"] = CENSORED_FUTURE_COVERAGE
        episode["net_pnl"] = 90.0
        episode["target_progress"] = 90.0 / 9_000.0
        episode["days_to_target"] = None
        episode["consistency_ok"] = True
        episode["best_day_concentration"] = 0.0
        episode["component_contribution"] = {SLEEVE_ID: 90.0}
        day = episode["daily_path"][0]
        day["balance"] = 150_100.0
        day["mll_floor"] = 145_600.0
        day["mll_buffer"] = 4_490.0
        day["day_pnl"] = 90.0
        day["realized_pnl"] = 100.0
        day["unrealized_pnl"] = -10.0
        day["target_progress"] = 90.0 / 9_000.0
        day["consistency"] = 1.0
        day["consistency_ok"] = False
        day["component_attribution"] = {SLEEVE_ID: 90.0}

    evidence = materialize_causal_salvage_evidence(
        identity=_identity(),
        causal_replays={SLEEVE_ID: _replay()},
        evaluated_policy_records=records,
        policies={POLICY_ID: _policy()},
        sleeve_specs={SLEEVE_ID: _spec()},
        provenance=_provenance(),
    )

    episodes = evidence.records["episodes"]
    assert {row["terminal_state"] for row in episodes} == {
        "DATA_CENSORED"
    }
    assert all(row["consistency_ok"] is False for row in episodes)
    assert all(row["best_day_concentration"] == 1.0 for row in episodes)
    assert all(row["source_episode_consistency_ok"] is True for row in episodes)
    assert all(
        row["consistency_representation_source"]
        == "TERMINAL_REALIZED_ACCOUNT_PATH"
        for row in episodes
    )
    assert all(
        row["equity"] == 150_090.0
        for row in evidence.records["account_daily_paths"]
    )
