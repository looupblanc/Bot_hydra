from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hydra.data.budget import DatabentoBudgetConfig, read_ledger
from hydra.economic_evolution.schema import stable_hash
from hydra.production.fresh_confirmation_lane import (
    CUMULATIVE_HARD_CAP_USD,
    frozen_data_request,
    validate_acquisition_receipt,
)
from scripts.acquire_fresh_confirmation_0035 import (
    ConfirmationAcquisitionError,
    acquire_fresh_confirmation,
    validate_frozen_inputs,
)


MANIFEST_HASH = "a" * 64


def _manifest() -> dict:
    core = {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "campaign_mode": "AUTONOMOUS_ECONOMIC_DISCOVERY_DIRECTOR",
    }
    return {**core, "manifest_hash": stable_hash(core)}


def _contract() -> dict:
    core = {
        "schema": "hydra_fresh_confirmation_contract_v1",
        "status": "FROZEN_AWAITING_ACQUISITION",
        "data_request": frozen_data_request(),
        "data_partition": {
            "role": "CONFIRMATION",
            "candidate_modification_allowed": False,
            "recalibration_allowed": False,
        },
        "tier_g_candidates": [
            {"candidate_id": "hazard_19327ab34a21d623c654a6cc"},
        ],
    }
    return {**core, "contract_hash": stable_hash(core)}


class _Metadata:
    def get_cost(self, **kwargs: object) -> float:
        return 0.1 if kwargs["schema"] == "definition" else 1.5


class _Symbology:
    _continuous = {
        "YM.c.0": [{"s": "1", "d0": "2025-01-02", "d1": "2025-03-15"},
                     {"s": "2", "d0": "2025-03-15", "d1": "2025-07-01"}],
        "MYM.c.0": [{"s": "3", "d0": "2025-01-02", "d1": "2025-03-15"},
                      {"s": "4", "d0": "2025-03-15", "d1": "2025-07-01"}],
        "ES.c.0": [{"s": "5", "d0": "2025-01-02", "d1": "2025-03-15"},
                     {"s": "6", "d0": "2025-03-15", "d1": "2025-07-01"}],
    }
    _raw = {
        "1": "YMH5", "2": "YMM5", "3": "MYMH5", "4": "MYMM5",
        "5": "ESH5", "6": "ESM5",
    }

    def resolve(self, **kwargs: object) -> dict:
        if kwargs["stype_in"] == "continuous":
            return {"result": {symbol: self._continuous[symbol] for symbol in kwargs["symbols"]}}
        return {
            "result": {
                instrument_id: [{"s": self._raw[instrument_id]}]
                for instrument_id in kwargs["symbols"]
            }
        }


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
    roots = {"1": ("YM", "YMH5", 1.0), "2": ("YM", "YMM5", 1.0),
             "3": ("MYM", "MYMH5", 1.0), "4": ("MYM", "MYMM5", 1.0),
             "5": ("ES", "ESH5", 0.25), "6": ("ES", "ESM5", 0.25)}
    rows = []
    for instrument_id, (root, raw, tick) in roots.items():
        rows.append({
            "ts_event": pd.Timestamp("2025-01-01T00:00:00Z"),
            "instrument_id": instrument_id,
            "raw_symbol": raw,
            "instrument_class": "F",
            "security_type": "FUT",
            "asset": root,
            "min_price_increment": tick,
            "expiration": "2025-06-20T00:00:00Z",
            "activation": "2024-12-01T00:00:00Z",
        })
    return pd.DataFrame(rows)


def _ohlcv_frame() -> pd.DataFrame:
    rows = []
    for instrument_id, base in (("1", 43000), ("3", 43000), ("5", 6000)):
        for minute in range(2):
            rows.append({
                "ts_event": pd.Timestamp("2025-01-02T14:30:00Z") + pd.Timedelta(minutes=minute),
                "instrument_id": instrument_id,
                "open": float(base + minute),
                "high": float(base + minute + 1),
                "low": float(base + minute - 1),
                "close": float(base + minute + 0.5),
                "volume": 10 + minute,
            })
    return pd.DataFrame(rows)


def _loader(path: Path) -> _Store:
    return _Store(_definition_frame() if "definitions" in path.name else _ohlcv_frame())


def _budget(root: Path) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        hard_cap_usd=CUMULATIVE_HARD_CAP_USD,
        safety_ceiling_usd=CUMULATIVE_HARD_CAP_USD,
        ledger_path=str(root / "spend.jsonl"),
        summary_path=str(root / "summary.md"),
    )


def test_validation_binds_exact_current_manifest_hash_and_immutable_role() -> None:
    manifest = _manifest()
    result = validate_frozen_inputs(
        _contract(), manifest, expected_manifest_hash=manifest["manifest_hash"]
    )
    assert result["request"]["symbols"] == ["YM.c.0", "MYM.c.0", "ES.c.0"]
    assert result["request"]["start"] == "2025-01-02"
    assert result["request"]["end"] == "2025-07-01"

    with pytest.raises(ConfirmationAcquisitionError, match="manifest identity/hash"):
        validate_frozen_inputs(_contract(), manifest, expected_manifest_hash="f" * 64)


def test_dry_run_reestimates_bars_and_definitions_without_ledger_write(tmp_path: Path) -> None:
    manifest = _manifest()
    client = _Client()
    result = acquire_fresh_confirmation(
        contract=_contract(), manifest=manifest,
        expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
        client=client, execute=False, budget=_budget(tmp_path), dbn_store_loader=_loader,
        receipt_path=tmp_path / "receipt.json",
    )
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["aggregate_live_estimate_usd"] == pytest.approx(1.6)
    assert result["parameters_mutable"] is False
    assert client.timeseries.calls == []
    assert not (tmp_path / "spend.jsonl").exists()
    assert not (tmp_path / "reports/data_access/data_access_ledger.jsonl").exists()


def test_execute_seals_feature_inputs_and_appends_each_ledger_once(tmp_path: Path) -> None:
    manifest = _manifest()
    contract = _contract()
    client = _Client()
    receipt = acquire_fresh_confirmation(
        contract=contract, manifest=manifest,
        expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
        client=client, execute=True, budget=_budget(tmp_path), dbn_store_loader=_loader,
        receipt_path=tmp_path / "receipt.json",
    )

    assert client.timeseries.calls == ["ohlcv-1m", "definition"]
    assert receipt["data_role"] == "CONFIRMATION"
    assert receipt["access_role"] == "BLIND_VALIDATION"
    assert receipt["parameters_mutable"] is False
    assert receipt["actual_cost_usd"] == pytest.approx(1.6)
    assert {row["kind"] for row in receipt["files"]} == {
        "RAW_DBN_OHLCV", "RAW_DBN_DEFINITIONS", "NORMALIZED_PARQUET",
        "EXPLICIT_CONTRACT_MAP", "SYMBOL_RESOLUTION",
    }
    feature = receipt["feature_build_inputs"]
    assert Path(feature["source_files"][0]["path"]).is_file()
    assert Path(feature["contract_map_path"]).is_file()
    assert validate_acquisition_receipt(contract, receipt)["status"] == (
        "ACQUISITION_RECEIPT_RECONCILED"
    )
    assert len(read_ledger(tmp_path / "spend.jsonl")) == 1
    access_path = tmp_path / "reports/data_access/data_access_ledger.jsonl"
    access = [json.loads(line) for line in access_path.read_text().splitlines()]
    assert len(access) == 1
    assert access[0]["data_role"] == "BLIND_VALIDATION"
    assert access[0]["parameters_mutable"] is False
    assert access[0]["freeze_manifest_hash"] == manifest["manifest_hash"]

    second_client = _Client()
    repeated = acquire_fresh_confirmation(
        contract=contract, manifest=manifest,
        expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
        client=second_client, execute=True, budget=_budget(tmp_path),
        dbn_store_loader=_loader, receipt_path=tmp_path / "receipt.json",
    )
    assert repeated == receipt
    assert second_client.timeseries.calls == []
    assert len(read_ledger(tmp_path / "spend.jsonl")) == 1
    assert len(access_path.read_text().splitlines()) == 1


def test_request_or_sealed_artifact_drift_fails_closed(tmp_path: Path) -> None:
    manifest = _manifest()
    contract = _contract()
    client = _Client()
    receipt = acquire_fresh_confirmation(
        contract=contract, manifest=manifest,
        expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
        client=client, execute=True, budget=_budget(tmp_path), dbn_store_loader=_loader,
        receipt_path=tmp_path / "receipt.json",
    )
    Path(receipt["feature_build_inputs"]["source_files"][0]["path"]).write_bytes(b"drift")
    with pytest.raises(ConfirmationAcquisitionError, match="artifact drift"):
        acquire_fresh_confirmation(
            contract=contract, manifest=manifest,
            expected_manifest_hash=manifest["manifest_hash"], root=tmp_path,
            client=_Client(), execute=True, budget=_budget(tmp_path),
            dbn_store_loader=_loader, receipt_path=tmp_path / "receipt.json",
        )
