from __future__ import annotations

from pathlib import Path

import pytest

from hydra.data.budget import DatabentoBudgetConfig
from hydra.economic_evolution.schema import stable_hash
from scripts.acquire_cl_front_second_term_structure_tripwire import (
    EXPECTED,
    REQUEST_HASH,
    CLTermStructureAcquisitionError,
    canonical_bundle_request,
    estimate_or_acquire,
    frozen_contract,
    load_and_validate_card,
    validate_acquisition_receipt,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _Metadata:
    def __init__(self, *, cost_delta: float = 0.0) -> None:
        self.cost_delta = cost_delta
        self.calls: list[tuple[str, str]] = []

    def get_record_count(self, **kwargs: object) -> int:
        schema = str(kwargs["schema"])
        self.calls.append(("records", schema))
        return int(EXPECTED[schema]["record_count"])

    def get_billable_size(self, **kwargs: object) -> int:
        schema = str(kwargs["schema"])
        self.calls.append(("bytes", schema))
        return int(EXPECTED[schema]["billable_size_bytes"])

    def get_cost(self, **kwargs: object) -> float:
        schema = str(kwargs["schema"])
        self.calls.append(("cost", schema))
        return float(EXPECTED[schema]["estimated_cost_usd"]) + self.cost_delta


class _Symbology:
    def __init__(self, *, collision: bool = False, gap: bool = False) -> None:
        self.collision = collision
        self.gap = gap

    def resolve(self, **_kwargs: object) -> dict[str, object]:
        second_first = "100" if self.collision else "200"
        second_start = "2024-01-23" if self.gap else "2024-01-22"
        return {
            "result": {
                "CL.c.0": [
                    {"s": "100", "d0": "2023-01-03", "d1": "2024-01-22"},
                    {"s": "101", "d0": "2024-01-22", "d1": "2024-10-01"},
                ],
                "CL.c.1": [
                    {"s": second_first, "d0": "2023-01-03", "d1": "2024-01-22"},
                    {"s": "201", "d0": second_start, "d1": "2024-10-01"},
                ],
            }
        }


class _Client:
    def __init__(
        self, *, cost_delta: float = 0.0, collision: bool = False, gap: bool = False
    ) -> None:
        self.metadata = _Metadata(cost_delta=cost_delta)
        self.symbology = _Symbology(collision=collision, gap=gap)


def _budget(tmp_path: Path) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        ledger_path=str(tmp_path / "spend.jsonl"),
        summary_path=str(tmp_path / "summary.md"),
    )


def test_card_and_request_are_immutably_bound() -> None:
    card = load_and_validate_card(PROJECT_ROOT)
    assert stable_hash(canonical_bundle_request()) == REQUEST_HASH
    assert card["card_hash"] == "92557d96f4984570e1a2a06ebb1f33ad9e142ee1328a4ba755c0a699474c979e"
    contract = frozen_contract(card["card_hash"])
    assert contract["request_hash"] == REQUEST_HASH
    assert contract["q4_access_allowed"] is False
    assert [row["fraction"] for row in contract["temporal_roles"]] == [0.6, 0.2, 0.2]
    assert contract["target_binding"] == {
        "market": "MCL.c.0",
        "fill": "NEXT_TRADABLE_OPEN",
        "roll_guard_true_sessions_each_side": 1,
    }


def test_real_sealed_receipt_reconciles_exact_ledgers_and_files() -> None:
    result = validate_acquisition_receipt(PROJECT_ROOT)
    assert result["receipt_hash"] == "048337824527fddf660214b13582723f31f3a6c9a8a6e4627ad06919f6228add"
    assert result["bundle_id"] == "20276ee521b4ab025d7b"
    assert result["local_validation"] == {
        "status": "SEALED_ACQUISITION_RECEIPT_VALID",
        "network_requests": 0,
        "writes": 0,
        "spend_row_count": 2,
        "access_role_count": 3,
        "artifact_count": 3,
    }


def test_dry_run_is_metadata_only_and_writes_nothing(tmp_path: Path) -> None:
    client = _Client()
    receipt = tmp_path / "receipt.json"
    result = estimate_or_acquire(
        root=PROJECT_ROOT,
        client=client,
        execute=False,
        budget=_budget(tmp_path),
        receipt_path=receipt,
    )

    assert result["request_hash"] == REQUEST_HASH
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["market_data_downloaded"] is False
    assert result["files_written"] == 0
    assert result["official_total_record_count"] == 556_889
    assert result["official_total_billable_bytes"] == 31_352_376
    assert result["official_total_cost_usd"] == pytest.approx(2.031393438578)
    assert result["symbology"]["same_instrument_interval_count"] == 0
    assert not receipt.exists()
    assert not (tmp_path / "spend.jsonl").exists()
    assert client.metadata.calls == [
        ("records", "ohlcv-1m"),
        ("bytes", "ohlcv-1m"),
        ("cost", "ohlcv-1m"),
        ("records", "definition"),
        ("bytes", "definition"),
        ("cost", "definition"),
    ]


@pytest.mark.parametrize(
    ("client", "message"),
    [
        (_Client(cost_delta=0.01), "official cost drift"),
        (_Client(collision=True), "same instrument"),
        (_Client(gap=True), "gap or overlap"),
    ],
)
def test_cost_and_symbology_drift_fail_closed(
    tmp_path: Path, client: _Client, message: str
) -> None:
    with pytest.raises(CLTermStructureAcquisitionError, match=message):
        estimate_or_acquire(
            root=PROJECT_ROOT,
            client=client,
            execute=False,
            budget=_budget(tmp_path),
            receipt_path=tmp_path / "receipt.json",
        )
    assert not (tmp_path / "spend.jsonl").exists()
