from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hydra.data.budget import DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD, DatabentoBudgetConfig, read_ledger
from hydra.economic_evolution.schema import stable_hash
from scripts.acquire_tier_q_2026_confirmation import (
    CANDIDATE_IDS,
    END,
    ROOTS,
    RULE_SNAPSHOT_HASH,
    SPLIT,
    START,
    SYMBOLS,
    TierQ2026AcquisitionError,
    acquire_tier_q_2026,
    validate_frozen_inputs,
)


def _manifest() -> dict:
    core = {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "campaign_mode": "AUTONOMOUS_ECONOMIC_DISCOVERY_DIRECTOR",
    }
    return {**core, "manifest_hash": stable_hash(core)}


def _contract(manifest_hash: str) -> dict:
    request_core = {
        "dataset": "GLBX.MDP3", "schema": "ohlcv-1m", "symbols": list(SYMBOLS),
        "stype_in": "continuous", "stype_out": "instrument_id",
        "start": START, "end": END, "date_interval": "HALF_OPEN",
        "q4_access_allowed": False, "broker_or_order_capability": False,
    }
    cohort = []
    for index, candidate_id in enumerate(CANDIDATE_IDS):
        specification = {"candidate_id": candidate_id, "version": 1}
        selected_cell_hash = stable_hash({"cell": index})
        profile = {
            "selected_cell_hash": selected_cell_hash,
            "official_rule_snapshot_hash": RULE_SNAPSHOT_HASH,
        }
        cohort.append({
            "candidate_id": candidate_id, "prior_evidence_tier": "Q",
            "candidate_fingerprint": stable_hash(specification),
            "frozen_candidate_specification": specification,
            "frozen_candidate_specification_hash": stable_hash(specification),
            "selected_cell_hash": selected_cell_hash,
            "frozen_account_profile": profile,
            "frozen_account_profile_hash": stable_hash(profile),
            "calibration_hash": stable_hash({"calibration": index}),
            "development_evidence_hash": stable_hash({"evidence": index}),
        })
    core = {
        "schema": "hydra_tier_q_2026_two_stage_contract_v1",
        "status": "FROZEN_AWAITING_ACQUISITION",
        "source_manifest_hash": manifest_hash,
        "official_rule_snapshot_hash": RULE_SNAPSHOT_HASH,
        "candidate_cohort": cohort,
        "data_request": {**request_core, "request_hash": stable_hash(request_core)},
        "temporal_roles": [
            {"role": "FINAL_DEVELOPMENT", "start": START, "end": SPLIT,
             "retuning_allowed": False, "access_state": "OPEN_AFTER_ACQUISITION"},
            {"role": "CONFIRMATION", "start": SPLIT, "end": END,
             "retuning_allowed": False, "access_state": "SEALED_UNTIL_TIER_G_GATE"},
        ],
        "promotion_order": ["Q", "G", "C"],
        "budget_binding": {
            "cumulative_hard_cap_usd": DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
            "maximum_live_estimate_usd": 7.0,
        },
        "outcome_accessed_at_freeze": False,
    }
    return {**core, "contract_hash": stable_hash(core)}


class _Metadata:
    def get_cost(self, **kwargs: object) -> float:
        return 0.01 if kwargs["schema"] == "definition" else 1.0

    def get_record_count(self, **kwargs: object) -> int:
        return 10 if kwargs["schema"] == "definition" else 100

    def get_billable_size(self, **kwargs: object) -> int:
        return 1000 if kwargs["schema"] == "definition" else 10000


class _Symbology:
    def resolve(self, **kwargs: object) -> dict:
        symbols = list(kwargs["symbols"])
        if kwargs["stype_in"] == "continuous":
            return {
                "result": {
                    symbol: [{"s": str(index + 1), "d0": START, "d1": END}]
                    for index, symbol in enumerate(symbols)
                }
            }
        return {"result": {symbol: [{"s": f"{ROOTS[int(symbol) - 1]}M6"}] for symbol in symbols}}


class _Timeseries:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_range(self, **kwargs: object) -> None:
        self.calls.append(str(kwargs["schema"]))
        Path(str(kwargs["path"])).write_bytes(f"dbn:{kwargs['schema']}".encode())


class _Client:
    metadata = _Metadata()
    symbology = _Symbology()

    def __init__(self) -> None:
        self.timeseries = _Timeseries()


class _Store:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def to_df(self, **_kwargs: object) -> pd.DataFrame:
        return self.frame.copy()


def _definition_frame() -> pd.DataFrame:
    ticks = {"RTY": .1, "M2K": .1, "NQ": .25, "MNQ": .25, "YM": 1., "MYM": 1.,
             "ES": .25, "MES": .25, "CL": .01, "MCL": .01}
    return pd.DataFrame([
        {
            "ts_event": pd.Timestamp("2026-01-01T00:00:00Z"),
            "instrument_id": str(index + 1), "raw_symbol": f"{root}M6",
            "instrument_class": "F", "security_type": "FUT", "asset": root,
            "min_price_increment": ticks[root], "expiration": "2026-06-20T00:00:00Z",
            "activation": "2025-12-01T00:00:00Z",
        }
        for index, root in enumerate(ROOTS)
    ])


def _ohlcv_frame() -> pd.DataFrame:
    rows = []
    for index, _root in enumerate(ROOTS):
        for timestamp in ("2026-01-02T14:30:00Z", "2026-05-04T14:30:00Z"):
            rows.append({
                "ts_event": pd.Timestamp(timestamp), "instrument_id": str(index + 1),
                "open": 100.0 + index, "high": 101.0 + index,
                "low": 99.0 + index, "close": 100.5 + index, "volume": 10,
            })
    return pd.DataFrame(rows)


def _loader(path: Path) -> _Store:
    return _Store(_definition_frame() if "definitions" in path.name else _ohlcv_frame())


def _budget(root: Path) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        hard_cap_usd=DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
        safety_ceiling_usd=DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
        ledger_path=str(root / "spend.jsonl"), summary_path=str(root / "summary.md"),
    )


def test_contract_binds_manifest_candidates_roles_and_rule_snapshot() -> None:
    manifest = _manifest()
    contract = _contract(manifest["manifest_hash"])
    result = validate_frozen_inputs(contract, manifest, expected_manifest_hash=manifest["manifest_hash"])
    assert result["request"]["symbols"] == list(SYMBOLS)
    assert contract["temporal_roles"][1]["access_state"] == "SEALED_UNTIL_TIER_G_GATE"

    drift = dict(contract)
    drift["official_rule_snapshot_hash"] = "f" * 64
    drift_core = dict(drift)
    drift_core.pop("contract_hash")
    drift["contract_hash"] = stable_hash(drift_core)
    with pytest.raises(TierQ2026AcquisitionError, match="rule snapshot"):
        validate_frozen_inputs(drift, manifest, expected_manifest_hash=manifest["manifest_hash"])


def test_dry_run_is_metadata_only_and_writes_nothing(tmp_path: Path) -> None:
    manifest = _manifest()
    client = _Client()
    result = acquire_tier_q_2026(
        contract=_contract(manifest["manifest_hash"]), manifest=manifest,
        expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
        client=client, execute=False, budget=_budget(tmp_path),
        receipt_path=tmp_path / "receipt.json",
    )
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["aggregate_live_estimate_usd"] == pytest.approx(1.01)
    assert result["aggregate_record_count"] == 110
    assert result["aggregate_billable_size_bytes"] == 11000
    assert client.timeseries.calls == []
    assert not (tmp_path / "receipt.json").exists()
    assert not (tmp_path / "spend.jsonl").exists()
    assert not (tmp_path / "reports/data_access/data_access_ledger.jsonl").exists()


def test_execute_is_one_shot_and_keeps_confirmation_sealed(tmp_path: Path) -> None:
    manifest = _manifest()
    contract = _contract(manifest["manifest_hash"])
    client = _Client()
    receipt = acquire_tier_q_2026(
        contract=contract, manifest=manifest,
        expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
        client=client, execute=True, budget=_budget(tmp_path),
        dbn_store_loader=_loader, receipt_path=tmp_path / "receipt.json",
    )
    assert client.timeseries.calls == ["ohlcv-1m", "definition"]
    assert receipt["partition_state"]["CONFIRMATION"] == "SEALED_UNTIL_TIER_G_GATE"
    assert receipt["outcome_evaluation_performed"] is False
    assert len(read_ledger(tmp_path / "spend.jsonl")) == 1
    access_rows = [
        json.loads(line)
        for line in (tmp_path / "reports/data_access/data_access_ledger.jsonl").read_text().splitlines()
    ]
    assert len(access_rows) == 2
    assert {(row["period_accessed"], row["data_role"]) for row in access_rows} == {
        (f"{START}:{SPLIT}", "SECONDARY_DEVELOPMENT_CONFIRMATION"),
        (f"{SPLIT}:{END}", "BLIND_VALIDATION"),
    }
    assert all(row["parameters_mutable"] is False for row in access_rows)

    second = _Client()
    repeated = acquire_tier_q_2026(
        contract=contract, manifest=manifest,
        expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
        client=second, execute=True, budget=_budget(tmp_path),
        dbn_store_loader=_loader, receipt_path=tmp_path / "receipt.json",
    )
    assert repeated == receipt
    assert second.timeseries.calls == []
    assert len(read_ledger(tmp_path / "spend.jsonl")) == 1
    assert len((tmp_path / "reports/data_access/data_access_ledger.jsonl").read_text().splitlines()) == 2
