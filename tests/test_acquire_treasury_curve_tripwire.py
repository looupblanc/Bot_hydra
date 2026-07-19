from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.data.budget import DatabentoBudgetConfig, DatabentoBudgetError, read_ledger
from scripts.acquire_treasury_curve_tripwire import (
    END,
    EXPECTED_BILLABLE_BYTES,
    EXPECTED_LIVE_COST_USD,
    EXPECTED_RECORD_COUNT,
    START,
    SYMBOLS,
    SYMBOLOGY_SCHEMA,
    TreasuryCurveAcquisitionError,
    estimate_or_acquire,
    frozen_contract,
    validate_frozen_contract,
)


_ROLL_BOUNDARIES = (
    ("2023-01-03", "2023-03-20", "H23"),
    ("2023-03-20", "2023-06-19", "M23"),
    ("2023-06-19", "2023-09-18", "U23"),
    ("2023-09-18", "2023-12-18", "Z23"),
    ("2023-12-18", "2024-03-18", "H24"),
    ("2024-03-18", "2024-06-17", "M24"),
    ("2024-06-17", "2024-09-16", "U24"),
    ("2024-09-16", "2024-10-01", "Z24"),
)


class _Metadata:
    def __init__(
        self,
        *,
        cost: float = EXPECTED_LIVE_COST_USD,
        records: int = EXPECTED_RECORD_COUNT,
        billable_bytes: int = EXPECTED_BILLABLE_BYTES,
    ) -> None:
        self.cost = cost
        self.records = records
        self.billable_bytes = billable_bytes
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get_cost(self, **kwargs: object) -> float:
        self.calls.append(("cost", dict(kwargs)))
        return self.cost

    def get_record_count(self, **kwargs: object) -> int:
        self.calls.append(("records", dict(kwargs)))
        return self.records

    def get_billable_size(self, **kwargs: object) -> int:
        self.calls.append(("bytes", dict(kwargs)))
        return self.billable_bytes


class _Timeseries:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_range(self, **kwargs: object) -> None:
        self.calls.append(dict(kwargs))
        Path(str(kwargs["path"])).write_bytes(b"immutable-treasury-curve-dbn")


class _Symbology:
    def __init__(self, *, gap_root: str | None = None, wrong_root: str | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._continuous: dict[str, list[dict[str, str]]] = {}
        self._raw: dict[str, tuple[str, str, str]] = {}
        next_id = 1000
        for symbol in SYMBOLS:
            root = symbol.split(".", 1)[0]
            rows: list[dict[str, str]] = []
            for index, (d0, d1, suffix) in enumerate(_ROLL_BOUNDARIES):
                instrument_id = str(next_id)
                next_id += 1
                row_start = d0
                if gap_root == root and index == 1:
                    row_start = "2023-03-21"
                rows.append({"s": instrument_id, "d0": row_start, "d1": d1})
                raw_root = "ES" if wrong_root == root and index == 0 else root
                self._raw[instrument_id] = (f"{raw_root}{suffix}", row_start, d1)
            self._continuous[symbol] = rows

    def resolve(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        if kwargs["stype_in"] == "continuous":
            return {
                "result": {
                    symbol: self._continuous[str(symbol)]
                    for symbol in kwargs["symbols"]
                }
            }
        result: dict[str, list[dict[str, str]]] = {}
        for value in kwargs["symbols"]:
            instrument_id = str(value)
            raw, d0, d1 = self._raw[instrument_id]
            result[instrument_id] = [{"s": raw, "d0": d0, "d1": d1}]
        return {"result": result}


class _Client:
    def __init__(
        self,
        metadata: _Metadata | None = None,
        symbology: _Symbology | None = None,
    ) -> None:
        self.metadata = metadata or _Metadata()
        self.timeseries = _Timeseries()
        self.symbology = symbology or _Symbology()


def _budget(root: Path, *, cap: float = 200.720719923081) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        hard_cap_usd=cap,
        safety_ceiling_usd=cap,
        ledger_path=str(root / "spend.jsonl"),
        summary_path=str(root / "budget_summary.md"),
    )


def _access_rows(root: Path) -> list[dict[str, object]]:
    path = root / "reports/data_access/data_access_ledger.jsonl"
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def test_contract_freezes_exact_request_roles_and_excludes_q4() -> None:
    contract = validate_frozen_contract(frozen_contract())

    assert contract["data_request"] == {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
    }
    assert contract["official_estimate"] == {
        "cost_usd": EXPECTED_LIVE_COST_USD,
        "records": EXPECTED_RECORD_COUNT,
        "billable_bytes": EXPECTED_BILLABLE_BYTES,
    }
    roles = contract["temporal_roles"]
    assert [row["role"] for row in roles] == [
        "DISCOVERY",
        "VALIDATION",
        "FINAL_DEVELOPMENT",
    ]
    assert roles[0]["start"] == START
    assert roles[-1]["end"] == END == "2024-10-01"
    assert all(left["end"] == right["start"] for left, right in zip(roles, roles[1:]))
    assert contract["q4_2024_access_allowed"] is False
    assert contract["definition_policy"]["requested"] is False
    assert contract["symbology_policy"] == {
        "required": True,
        "billable_purchase": False,
        "continuous_resolution": "continuous_to_instrument_id",
        "interval_resolution": "instrument_id_to_raw_symbol_per_interval",
        "coverage": "EXACT_HALF_OPEN_NO_GAP_NO_OVERLAP",
        "delivery_contract": "ROOT_QUARTERLY_MONTH_AND_YEAR_VERIFIED",
    }


def test_dry_run_checks_official_dimensions_without_writing(tmp_path: Path) -> None:
    client = _Client()
    receipt = tmp_path / "receipt.json"
    result = estimate_or_acquire(
        root=tmp_path,
        client=client,
        execute=False,
        budget=_budget(tmp_path),
        receipt_path=receipt,
    )

    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["official_live_estimate_usd"] == EXPECTED_LIVE_COST_USD
    assert result["official_record_count"] == EXPECTED_RECORD_COUNT
    assert result["official_billable_bytes"] == EXPECTED_BILLABLE_BYTES
    assert result["q4_access_count_delta"] == 0
    assert result["network_data_request_made"] is False
    mapping = result["symbology_roll_map"]
    assert mapping["schema"] == SYMBOLOGY_SCHEMA
    assert mapping["roots"] == ["ZT", "ZF", "ZN", "TN", "ZB", "UB"]
    assert mapping["continuous_to_instrument_id_call_count"] == 1
    assert mapping["instrument_interval_to_raw_symbol_call_count"] == 48
    assert len(mapping["contract_intervals"]) == 48
    assert all(
        row["gap_count"] == row["overlap_count"] == 0
        for row in mapping["coverage_by_root"].values()
    )
    assert result["symbology_endpoint_billable_cost_usd"] == 0.0
    assert client.timeseries.calls == []
    assert [name for name, _request in client.metadata.calls] == [
        "cost",
        "records",
        "bytes",
    ]
    assert len(client.symbology.calls) == 49
    assert client.symbology.calls[0] == {
        "dataset": "GLBX.MDP3",
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start_date": START,
        "end_date": END,
    }
    assert all(
        call["stype_in"] == "instrument_id"
        and call["stype_out"] == "raw_symbol"
        and len(call["symbols"]) == 1
        for call in client.symbology.calls[1:]
    )
    for _name, request in client.metadata.calls:
        assert request["symbols"] == list(SYMBOLS)
        assert request["start"] == START
        assert request["end"] == END
        assert request["schema"] == "ohlcv-1m"
    assert not receipt.exists()
    assert not (tmp_path / "spend.jsonl").exists()
    assert _access_rows(tmp_path) == []
    assert not list(tmp_path.rglob("symbology_roll_map.json"))


@pytest.mark.parametrize(
    ("metadata", "message"),
    [
        (_Metadata(cost=EXPECTED_LIVE_COST_USD + 0.01), "official cost drift"),
        (_Metadata(records=EXPECTED_RECORD_COUNT + 1), "record-count drift"),
        (_Metadata(billable_bytes=EXPECTED_BILLABLE_BYTES + 1), "billable-size drift"),
    ],
)
def test_live_estimate_drift_fails_before_download(
    tmp_path: Path, metadata: _Metadata, message: str
) -> None:
    client = _Client(metadata)
    with pytest.raises(TreasuryCurveAcquisitionError, match=message):
        estimate_or_acquire(
            root=tmp_path,
            client=client,
            execute=True,
            budget=_budget(tmp_path),
            receipt_path=tmp_path / "receipt.json",
        )

    assert client.timeseries.calls == []
    assert not (tmp_path / "spend.jsonl").exists()
    assert _access_rows(tmp_path) == []


@pytest.mark.parametrize(
    ("symbology", "message"),
    [
        (_Symbology(gap_root="ZN"), "coverage gap or overlap"),
        (_Symbology(wrong_root="ZB"), "root/month/year mismatch"),
    ],
)
def test_causal_symbology_defect_fails_before_cost_or_download(
    tmp_path: Path, symbology: _Symbology, message: str
) -> None:
    client = _Client(symbology=symbology)
    with pytest.raises(TreasuryCurveAcquisitionError, match=message):
        estimate_or_acquire(
            root=tmp_path,
            client=client,
            execute=False,
            budget=_budget(tmp_path),
            receipt_path=tmp_path / "receipt.json",
        )
    assert client.metadata.calls == []
    assert client.timeseries.calls == []
    assert not list(tmp_path.rglob("symbology_roll_map.json"))


def test_execute_seals_raw_receipt_and_each_ledger_exactly_once(
    tmp_path: Path,
) -> None:
    client = _Client()
    receipt_path = tmp_path / "receipt.json"
    receipt = estimate_or_acquire(
        root=tmp_path,
        client=client,
        execute=True,
        budget=_budget(tmp_path),
        receipt_path=receipt_path,
    )

    assert len(client.timeseries.calls) == 1
    download = client.timeseries.calls[0]
    assert download["schema"] == "ohlcv-1m"
    assert download["symbols"] == list(SYMBOLS)
    assert download["stype_in"] == "continuous"
    assert download["stype_out"] == "instrument_id"
    assert receipt["actual_cost_usd"] == EXPECTED_LIVE_COST_USD
    assert receipt["official_record_count"] == EXPECTED_RECORD_COUNT
    assert receipt["official_billable_bytes"] == EXPECTED_BILLABLE_BYTES
    assert receipt["raw_immutable"] is True
    assert receipt["definition_policy"]["requested"] is False
    assert receipt["symbology_policy"]["required"] is True
    assert receipt["symbology_endpoint_billable_cost_usd"] == 0.0
    assert receipt["symbology_roll_map"]["schema"] == SYMBOLOGY_SCHEMA
    assert receipt["symbology_roll_map_hash"] == receipt["symbology_roll_map"][
        "mapping_hash"
    ]
    assert receipt["runtime_or_manifest_modified"] is False
    assert {row["kind"] for row in receipt["files"]} == {
        "RAW_DBN_OHLCV",
        "SYMBOLOGY_ROLL_MAP",
    }
    raw = next(row for row in receipt["files"] if row["kind"] == "RAW_DBN_OHLCV")
    symbology_file = next(
        row for row in receipt["files"] if row["kind"] == "SYMBOLOGY_ROLL_MAP"
    )
    assert raw["kind"] == "RAW_DBN_OHLCV"
    assert Path(raw["path"]).is_file()
    assert Path(symbology_file["path"]).is_file()
    persisted_mapping = json.loads(Path(symbology_file["path"]).read_text())
    assert persisted_mapping == receipt["symbology_roll_map"]
    assert len(read_ledger(tmp_path / "spend.jsonl")) == 1

    access = _access_rows(tmp_path)
    assert len(access) == 3
    assert [row["period_accessed"] for row in access] == [
        "2023-01-03:2024-01-22",
        "2024-01-22:2024-05-28",
        "2024-05-28:2024-10-01",
    ]
    assert access[0]["data_role"] == "DEVELOPMENT"
    assert access[0]["parameters_mutable"] is True
    assert all(row["data_role"] == "BLIND_VALIDATION" for row in access[1:])
    assert all(row["parameters_mutable"] is False for row in access[1:])
    assert all(row["freeze_manifest_hash"] == receipt["contract_hash"] for row in access)

    second_client = _Client()
    repeated = estimate_or_acquire(
        root=tmp_path,
        client=second_client,
        execute=True,
        budget=_budget(tmp_path),
        receipt_path=receipt_path,
    )
    assert repeated == receipt
    assert second_client.metadata.calls == []
    assert second_client.timeseries.calls == []
    assert second_client.symbology.calls == []
    assert len(read_ledger(tmp_path / "spend.jsonl")) == 1
    assert len(_access_rows(tmp_path)) == 3


def test_budget_rejection_occurs_before_download(tmp_path: Path) -> None:
    client = _Client()
    with pytest.raises(DatabentoBudgetError, match="exceeds hard cap"):
        estimate_or_acquire(
            root=tmp_path,
            client=client,
            execute=True,
            budget=_budget(tmp_path, cap=8.0),
            receipt_path=tmp_path / "receipt.json",
        )
    assert client.timeseries.calls == []
    assert not (tmp_path / "spend.jsonl").exists()


def test_sealed_raw_drift_fails_closed_without_network(tmp_path: Path) -> None:
    receipt_path = tmp_path / "receipt.json"
    receipt = estimate_or_acquire(
        root=tmp_path,
        client=_Client(),
        execute=True,
        budget=_budget(tmp_path),
        receipt_path=receipt_path,
    )
    raw = next(
        row for row in receipt["files"] if row["kind"] == "RAW_DBN_OHLCV"
    )
    Path(raw["path"]).write_bytes(b"drift")
    client = _Client()
    with pytest.raises(TreasuryCurveAcquisitionError, match="raw artifact drift"):
        estimate_or_acquire(
            root=tmp_path,
            client=client,
            execute=True,
            budget=_budget(tmp_path),
            receipt_path=receipt_path,
        )
    assert client.metadata.calls == []
    assert client.timeseries.calls == []


def test_sealed_symbology_drift_fails_closed_without_network(tmp_path: Path) -> None:
    receipt_path = tmp_path / "receipt.json"
    receipt = estimate_or_acquire(
        root=tmp_path,
        client=_Client(),
        execute=True,
        budget=_budget(tmp_path),
        receipt_path=receipt_path,
    )
    mapping = next(
        row for row in receipt["files"] if row["kind"] == "SYMBOLOGY_ROLL_MAP"
    )
    Path(mapping["path"]).write_text("{}\n")
    client = _Client()
    with pytest.raises(TreasuryCurveAcquisitionError, match="symbology artifact drift"):
        estimate_or_acquire(
            root=tmp_path,
            client=client,
            execute=True,
            budget=_budget(tmp_path),
            receipt_path=receipt_path,
        )
    assert client.metadata.calls == []
    assert client.timeseries.calls == []
    assert client.symbology.calls == []
