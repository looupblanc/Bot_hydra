from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from hydra.data.budget import DatabentoBudgetConfig
from hydra.production.session_safe_m2k_mym_confirmation import load_decision_card
from scripts import acquire_session_safe_m2k_mym_confirmation as acquire


ROOT = Path(__file__).resolve().parents[1]


class _Metadata:
    def __init__(self, ohlcv_cost: float = 6.531647741795) -> None:
        self.ohlcv_cost = ohlcv_cost

    def get_cost(self, **request):
        return self.ohlcv_cost if request["schema"] == "ohlcv-1m" else 0.004095800221

    def get_record_count(self, **request):
        return 1_789_108 if request["schema"] == "ohlcv-1m" else 7_186

    def get_billable_size(self, **request):
        return 100_190_048 if request["schema"] == "ohlcv-1m" else 2_586_960


class _Symbology:
    def resolve(self, **request):
        if request["stype_in"] == "continuous":
            return {
                "result": {
                    symbol: [
                        {"d0": "2021-12-20", "d1": "2023-01-01", "s": str(index)}
                    ]
                    for index, symbol in enumerate(request["symbols"], start=1)
                }
            }
        # Include a reused-ID decoy before the interval in which the
        # continuous mapping actually selects the futures contract.
        return {
            "result": {
                symbol: [
                    {"d0": "2021-01-01", "d1": "2021-12-20", "s": "DECOY"},
                    {"d0": "2021-12-20", "d1": "2023-01-01", "s": f"FUT{symbol}"},
                ]
                for symbol in request["symbols"]
            }
        }


class _Client:
    def __init__(self, ohlcv_cost: float = 6.531647741795) -> None:
        self.metadata = _Metadata(ohlcv_cost)
        self.symbology = _Symbology()
        self.timeseries = _NoDownload()


class _NoDownload:
    def get_range(self, **_request):  # pragma: no cover - failure path
        raise AssertionError("dry-run must not download")


def _budget(tmp_path: Path) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        hard_cap_usd=200.720719923081,
        safety_ceiling_usd=200.720719923081,
        ledger_path=str(tmp_path / "ledger.jsonl"),
        summary_path=str(tmp_path / "summary.md"),
    )


def test_dry_run_is_exact_bounded_and_does_not_write(tmp_path: Path) -> None:
    card = load_decision_card(ROOT / acquire.DEFAULT_CARD)
    result = acquire.estimate_or_acquire(
        card=card,
        root=tmp_path,
        client=_Client(),
        execute=False,
        budget=_budget(tmp_path),
        receipt_path=tmp_path / "receipt.json",
    )
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["aggregate_live_estimate_usd"] == pytest.approx(6.535743542016)
    assert result["official_record_counts"]["ohlcv"] == 1_789_108
    assert result["request"]["symbols"] == [
        "RTY.c.0",
        "M2K.c.0",
        "YM.c.0",
        "MYM.c.0",
        "ES.c.0",
    ]
    assert all(
        value.startswith("FUT")
        for value in result["symbology"]["raw_symbol_mapping"].values()
    )
    assert not (tmp_path / "receipt.json").exists()
    assert not (tmp_path / "ledger.jsonl").exists()


def test_live_estimate_above_branch_ceiling_fails_closed(tmp_path: Path) -> None:
    card = load_decision_card(ROOT / acquire.DEFAULT_CARD)
    with pytest.raises(acquire.SessionSafeAcquisitionError):
        acquire.estimate_or_acquire(
            card=card,
            root=tmp_path,
            client=_Client(ohlcv_cost=7.01),
            execute=False,
            budget=_budget(tmp_path),
            receipt_path=tmp_path / "receipt.json",
        )


def test_later_access_to_frozen_period_fails_closed(tmp_path: Path) -> None:
    import json

    ledger = tmp_path / "reports/data_access/data_access_ledger.jsonl"
    ledger.parent.mkdir(parents=True)
    ledger.write_text(
        json.dumps({"period_accessed": "2022-06-01:2022-06-02"}) + "\n",
        encoding="utf-8",
    )
    card = load_decision_card(ROOT / acquire.DEFAULT_CARD)
    with pytest.raises(acquire.SessionSafeAcquisitionError, match="no longer untouched"):
        acquire.estimate_or_acquire(
            card=card,
            root=tmp_path,
            client=_Client(),
            execute=False,
            budget=_budget(tmp_path),
            receipt_path=tmp_path / "receipt.json",
        )
