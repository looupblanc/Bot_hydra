from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import scripts.acquire_mgc_rates_target_vol_power_extension as acquisition
from hydra.data.budget import DatabentoBudgetConfig
from hydra.economic_evolution.schema import stable_hash


PROJECT = Path(__file__).resolve().parents[1]
CARD = PROJECT / acquisition.CARD_PATH


class _Metadata:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def _row(self, kwargs: dict, method: str):
        schema = str(kwargs["schema"])
        self.calls.append((method, schema))
        assert kwargs == acquisition._request(schema)
        row = acquisition.EXPECTED[schema]
        return {
            "get_cost": row["cost_usd"],
            "get_record_count": row["records"],
            "get_billable_size": row["billable_bytes"],
        }[method]

    def get_cost(self, **kwargs):
        return self._row(kwargs, "get_cost")

    def get_record_count(self, **kwargs):
        return self._row(kwargs, "get_record_count")

    def get_billable_size(self, **kwargs):
        return self._row(kwargs, "get_billable_size")


class _Timeseries:
    def __init__(self) -> None:
        self.calls = 0

    def get_range(self, **kwargs):
        self.calls += 1
        path = Path(kwargs.pop("path"))
        assert kwargs.pop("stype_out") == acquisition.STYPE_OUT
        assert kwargs == acquisition._request(str(kwargs["schema"]))
        path.write_bytes(f"raw:{kwargs['schema']}".encode())
        return SimpleNamespace()


class _Client:
    def __init__(self) -> None:
        self.metadata = _Metadata()
        self.timeseries = _Timeseries()


class _FailSecondDownloadOnce(_Timeseries):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    def get_range(self, **kwargs):
        if not self.failed and self.calls == 1:
            self.calls += 1
            self.failed = True
            raise RuntimeError("simulated second-download interruption")
        return super().get_range(**kwargs)


def _budget(root: Path, *, safety_ceiling: float | None = None) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        hard_cap_usd=acquisition.HARD_CAP_USD,
        safety_ceiling_usd=(
            acquisition.HARD_CAP_USD if safety_ceiling is None else safety_ceiling
        ),
        ledger_path=str(root / "spend.jsonl"),
        summary_path=str(root / "summary.md"),
    )


def test_frozen_card_binds_single_candidate_request_roles_and_hash() -> None:
    card = acquisition.load_and_validate_card(PROJECT, CARD)
    core = dict(card)
    claimed = core.pop("card_hash")
    assert claimed == stable_hash(core)
    assert claimed == acquisition.EXPECTED_CARD_HASH
    assert card["data_contract"]["symbols"] == ["ZN.c.0", "TN.c.0", "MGC.v.0"]
    assert card["data_contract"]["start"] == "2015-09-01"
    assert card["data_contract"]["end"] == "2021-11-04"
    assert card["frozen_candidate"]["candidate_id"] == acquisition.CANDIDATE_ID
    assert card["frozen_candidate"]["candidate_count"] == 1
    assert card["power_thresholds"] == {
        "DISCOVERY": 60,
        "VALIDATION": 12,
        "FINAL_DEVELOPMENT": 20,
        "threshold_relaxation_allowed": False,
        "meeting_expected_session_count_guarantees_event_count": False,
    }


def test_maliciously_rehashed_card_still_fails_closed(tmp_path: Path) -> None:
    card = json.loads(CARD.read_text(encoding="utf-8"))
    card["governance"]["mission_state_writes_allowed"] = True
    core = dict(card)
    core.pop("card_hash")
    card["card_hash"] = stable_hash(core)
    path = tmp_path / "malicious.json"
    path.write_text(json.dumps(card), encoding="utf-8")
    with pytest.raises(acquisition.MGCPowerExtensionError, match="hash drift"):
        acquisition.load_and_validate_card(tmp_path, path)


def test_metadata_only_plan_creates_no_file_or_ledger(tmp_path: Path) -> None:
    client = _Client()
    result = acquisition.estimate_or_acquire(
        root=tmp_path,
        client=client,
        execute=False,
        budget=_budget(tmp_path),
        card_path=CARD,
        receipt_path=tmp_path / "receipt.json",
    )
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["official_total_cost_usd"] == pytest.approx(
        acquisition.EXPECTED_TOTAL_COST_USD, abs=1e-12
    )
    assert result["network_data_request_made"] is False
    assert result["outcomes_read"] == 0
    assert client.timeseries.calls == 0
    assert not (tmp_path / "receipt.json").exists()
    assert not (tmp_path / "spend.jsonl").exists()
    assert not (tmp_path / "reports").exists()


def test_official_estimate_drift_fails_closed(tmp_path: Path) -> None:
    client = _Client()
    original = client.metadata.get_cost

    def drift(**kwargs):
        value = original(**kwargs)
        return value + (0.01 if kwargs["schema"] == "ohlcv-1m" else 0.0)

    client.metadata.get_cost = drift
    with pytest.raises(acquisition.MGCPowerExtensionError, match="estimate drift"):
        acquisition.estimate_or_acquire(
            root=tmp_path,
            client=client,
            execute=False,
            budget=_budget(tmp_path),
            card_path=CARD,
        )


def test_safety_ceiling_is_the_effective_cap(tmp_path: Path) -> None:
    with pytest.raises(acquisition.MGCPowerExtensionError, match="violate reserve"):
        acquisition.estimate_or_acquire(
            root=tmp_path,
            client=_Client(),
            execute=False,
            budget=_budget(tmp_path, safety_ceiling=40.0),
            card_path=CARD,
        )


def _definition_frame(*, tn_symbol: str = "TNH6") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_event": "2015-08-31T00:00:00Z",
                "instrument_id": 101,
                "raw_symbol": "ZNH6",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "ZN",
                "min_price_increment": 0.015625,
                "unit_of_measure_qty": 1000.0,
                "expiration": "2016-03-22T00:00:00Z",
                "activation": "2015-01-01T00:00:00Z",
            },
            {
                "ts_event": "2015-08-31T00:00:00Z",
                "instrument_id": 102,
                "raw_symbol": tn_symbol,
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "TN",
                "min_price_increment": 0.015625,
                "unit_of_measure_qty": 1000.0,
                "expiration": "2016-03-22T00:00:00Z",
                "activation": "2015-01-01T00:00:00Z",
            },
            {
                "ts_event": "2015-08-31T00:00:00Z",
                "instrument_id": 103,
                "raw_symbol": "MGCZ5",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "MGC",
                "min_price_increment": 0.1,
                "unit_of_measure_qty": 10.0,
                "expiration": "2015-12-28T00:00:00Z",
                "activation": "2014-01-01T00:00:00Z",
            },
        ]
    )


def _mappings() -> dict[str, list[dict[str, str]]]:
    return {
        "ZN.c.0": [{"d0": acquisition.START, "d1": acquisition.END, "s": "101"}],
        "TN.c.0": [{"d0": acquisition.START, "d1": acquisition.END, "s": "102"}],
        "MGC.v.0": [{"d0": acquisition.START, "d1": acquisition.END, "s": "103"}],
    }


def test_roll_artifacts_are_definition_sourced_and_delivery_synchronised() -> None:
    mgc, treasury = acquisition.build_roll_artifacts(_mappings(), _definition_frame())
    assert mgc["map_type"] == acquisition.MGC_MAP_TYPE
    assert mgc["symbols"] == ["MGC"]
    assert mgc["contracts"][0]["contract"] == "MGCZ5"
    assert mgc["contracts"][0]["continuous_symbol"] == "MGC.v.0"
    assert mgc["source_metadata"]["definition_sourced"] is True
    assert treasury["delivery_mismatch_count"] == 0
    assert treasury["delivery_sync_intervals"] == [
        {
            "start": acquisition.START,
            "end": acquisition.END,
            "delivery_month": "201603",
            "zn_contract": "ZNH6",
            "zn_instrument_id": "101",
            "tn_contract": "TNH6",
            "tn_instrument_id": "102",
        }
    ]
    core = dict(treasury)
    claimed = core.pop("receipt_hash")
    assert claimed == stable_hash(core)


def test_delivery_mismatch_is_not_guessed_or_forward_filled() -> None:
    with pytest.raises(acquisition.MGCPowerExtensionError, match="same-delivery"):
        acquisition.build_roll_artifacts(
            _mappings(), _definition_frame(tn_symbol="TNM6")
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("unit_of_measure_qty", 11.0), ("min_price_increment", 0.2)],
)
def test_mgc_multiplier_tick_and_tick_value_are_frozen(
    field: str, value: float
) -> None:
    definitions = _definition_frame()
    definitions.loc[definitions["asset"] == "MGC", field] = value
    with pytest.raises(acquisition.MGCPowerExtensionError, match="economics drift"):
        acquisition.build_roll_artifacts(_mappings(), definitions)


def test_future_only_definition_is_rejected() -> None:
    definitions = _definition_frame()
    definitions.loc[definitions["asset"] == "MGC", "ts_event"] = (
        "2015-09-01T00:00:01Z"
    )
    with pytest.raises(acquisition.MGCPowerExtensionError, match="future-only"):
        acquisition.build_roll_artifacts(_mappings(), definitions)


def test_multi_segment_contract_maps_remain_explicit_and_synchronised() -> None:
    boundary = "2018-01-01"
    mappings = {
        "ZN.c.0": [
            {"d0": acquisition.START, "d1": boundary, "s": "101"},
            {"d0": boundary, "d1": acquisition.END, "s": "201"},
        ],
        "TN.c.0": [
            {"d0": acquisition.START, "d1": boundary, "s": "102"},
            {"d0": boundary, "d1": acquisition.END, "s": "202"},
        ],
        "MGC.v.0": [
            {"d0": acquisition.START, "d1": boundary, "s": "103"},
            {"d0": boundary, "d1": acquisition.END, "s": "203"},
        ],
    }
    second = pd.DataFrame(
        [
            {
                "ts_event": "2017-12-31T00:00:00Z",
                "instrument_id": 201,
                "raw_symbol": "ZNH8",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "ZN",
                "min_price_increment": 0.015625,
                "unit_of_measure_qty": 1000.0,
                "expiration": "2018-03-22T00:00:00Z",
                "activation": "2017-01-01T00:00:00Z",
            },
            {
                "ts_event": "2017-12-31T00:00:00Z",
                "instrument_id": 202,
                "raw_symbol": "TNH8",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "TN",
                "min_price_increment": 0.015625,
                "unit_of_measure_qty": 1000.0,
                "expiration": "2018-03-22T00:00:00Z",
                "activation": "2017-01-01T00:00:00Z",
            },
            {
                "ts_event": "2017-12-31T00:00:00Z",
                "instrument_id": 203,
                "raw_symbol": "MGCZ7",
                "instrument_class": "F",
                "security_type": "FUT",
                "asset": "MGC",
                "min_price_increment": 0.1,
                "unit_of_measure_qty": 10.0,
                "expiration": "2018-01-29T00:00:00Z",
                "activation": "2017-01-01T00:00:00Z",
            },
        ]
    )
    mgc, treasury = acquisition.build_roll_artifacts(
        mappings, pd.concat([_definition_frame(), second], ignore_index=True)
    )
    assert len(mgc["contracts"]) == 2
    assert len(treasury["delivery_sync_intervals"]) == 2
    assert treasury["delivery_mismatch_count"] == 0


def _stub_roll_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    mgc_base = {
        "dataset": acquisition.DATASET,
        "map_type": acquisition.MGC_MAP_TYPE,
        "symbols": ["MGC"],
    }
    mgc_core = {**mgc_base, "roll_map_hash": stable_hash(mgc_base)}
    treasury_base = {
        "schema": acquisition.TREASURY_RECEIPT_SCHEMA,
        "forward_fill_rows": 0,
        "q4_access_count_delta": 0,
    }
    treasury_core = {
        **treasury_base,
        "receipt_hash": stable_hash(treasury_base),
    }
    monkeypatch.setattr(
        acquisition,
        "_build_roll_artifacts_from_dbn",
        lambda definition_path, ohlcv_path: (dict(mgc_core), dict(treasury_core)),
    )


def test_execute_is_atomic_ledgered_and_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _Client()
    _stub_roll_artifacts(monkeypatch)
    kwargs = {
        "root": tmp_path,
        "client": client,
        "execute": True,
        "budget": _budget(tmp_path),
        "card_path": CARD,
        "receipt_path": tmp_path / "receipt.json",
    }
    first = acquisition.estimate_or_acquire(**kwargs)
    assert first["download_status"] == "DOWNLOADED"
    assert first["actual_cost_usd"] == pytest.approx(
        acquisition.EXPECTED_TOTAL_COST_USD, abs=1e-12
    )
    assert first["outcomes_read"] == 0
    assert first["q4_access_count_delta"] == 0
    assert first["broker_connections"] == first["orders"] == 0
    assert client.timeseries.calls == 2
    spend = [json.loads(line) for line in (tmp_path / "spend.jsonl").read_text().splitlines()]
    access = [
        json.loads(line)
        for line in (tmp_path / acquisition.ACCESS_LEDGER).read_text().splitlines()
    ]
    assert len(spend) == 2
    assert [row["download_status"] for row in spend] == [
        "ESTIMATED_ONLY",
        "DOWNLOADED",
    ]
    assert spend[0]["actual_cost_usd"] is None
    assert spend[0]["estimated_cost_usd"] == pytest.approx(
        acquisition.EXPECTED_TOTAL_COST_USD, abs=1e-12
    )
    assert spend[1]["actual_cost_usd"] == pytest.approx(
        acquisition.EXPECTED_TOTAL_COST_USD, abs=1e-12
    )
    assert spend[1]["estimated_cost_usd"] == 0.0
    estimated_total, actual_total = acquisition.cumulative_spend(tmp_path / "spend.jsonl")
    assert estimated_total == pytest.approx(
        acquisition.EXPECTED_TOTAL_COST_USD, abs=1e-12
    )
    assert actual_total == pytest.approx(
        acquisition.EXPECTED_TOTAL_COST_USD, abs=1e-12
    )
    assert len(access) == 4

    second = acquisition.estimate_or_acquire(**kwargs)
    assert second == first
    assert client.timeseries.calls == 2
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 2
    assert len((tmp_path / acquisition.ACCESS_LEDGER).read_text().splitlines()) == 4


def test_download_interruption_resumes_from_durable_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_roll_artifacts(monkeypatch)
    client = _Client()
    client.timeseries = _FailSecondDownloadOnce()
    receipt = tmp_path / "receipt.json"
    kwargs = {
        "root": tmp_path,
        "client": client,
        "execute": True,
        "budget": _budget(tmp_path),
        "card_path": CARD,
        "receipt_path": receipt,
    }
    with pytest.raises(RuntimeError, match="second-download interruption"):
        acquisition.estimate_or_acquire(**kwargs)
    spend = [json.loads(line) for line in (tmp_path / "spend.jsonl").read_text().splitlines()]
    assert [row["download_status"] for row in spend] == ["ESTIMATED_ONLY"]
    paths = acquisition._paths(tmp_path, spend[0]["request_id"], receipt)
    assert paths["raw_ohlcv"].is_file()
    assert not paths["raw_definitions"].exists()
    assert not receipt.exists()

    # A corrupt governed partial is never silently accepted or overwritten.
    paths["raw_definitions"].parent.mkdir(parents=True, exist_ok=True)
    paths["raw_definitions"].write_bytes(b"")
    with pytest.raises(acquisition.MGCPowerExtensionError, match="empty governed"):
        acquisition.estimate_or_acquire(**kwargs)
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 1
    paths["raw_definitions"].unlink()

    recovered = acquisition.estimate_or_acquire(**kwargs)
    assert recovered["download_status"] == "DOWNLOADED"
    assert client.timeseries.calls == 3
    spend = [json.loads(line) for line in (tmp_path / "spend.jsonl").read_text().splitlines()]
    assert [row["download_status"] for row in spend] == [
        "ESTIMATED_ONLY",
        "DOWNLOADED",
    ]


def test_post_download_pre_completion_crash_is_idempotently_recovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_roll_artifacts(monkeypatch)
    client = _Client()
    receipt = tmp_path / "receipt.json"
    original = acquisition._complete_spend_once
    crashed = False

    def crash_once(*args, **kwargs):
        nonlocal crashed
        if not crashed:
            crashed = True
            raise RuntimeError("simulated crash before completion journal")
        return original(*args, **kwargs)

    monkeypatch.setattr(acquisition, "_complete_spend_once", crash_once)
    kwargs = {
        "root": tmp_path,
        "client": client,
        "execute": True,
        "budget": _budget(tmp_path),
        "card_path": CARD,
        "receipt_path": receipt,
    }
    with pytest.raises(RuntimeError, match="before completion journal"):
        acquisition.estimate_or_acquire(**kwargs)
    assert client.timeseries.calls == 2
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 1
    recovered = acquisition.estimate_or_acquire(**kwargs)
    assert recovered["download_status"] == "DOWNLOADED"
    assert client.timeseries.calls == 2
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 2
