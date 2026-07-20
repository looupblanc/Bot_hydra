from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.data.budget import DatabentoBudgetConfig, sha256_file
from hydra.economic_evolution.schema import stable_hash
import scripts.acquire_soybean_crush_structural_value_router as acquisition
from scripts.acquire_soybean_crush_structural_value_router import (
    END,
    EXPECTED,
    EXPECTED_MANIFEST_HASH,
    EXPECTED_TOTAL_COST_USD,
    START,
    SYMBOLS,
    SoybeanCrushAcquisitionError,
    estimate_or_acquire,
    load_and_validate_manifest,
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
    def __init__(self, *, gap: bool = False, collision: bool = False) -> None:
        self.gap = gap
        self.collision = collision
        self.calls = 0

    def resolve(self, **kwargs: object) -> dict[str, object]:
        self.calls += 1
        assert kwargs == {
            "dataset": "GLBX.MDP3",
            "symbols": list(SYMBOLS),
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "start_date": START,
            "end_date": END,
        }
        second_start = "2022-01-02" if self.gap else "2022-01-01"
        zl_first = "100" if self.collision else "300"
        return {
            "result": {
                "ZM.c.0": [
                    {"s": "100", "d0": START, "d1": "2022-01-01"},
                    {"s": "200", "d0": "2022-01-01", "d1": END},
                ],
                "ZL.c.0": [
                    {"s": zl_first, "d0": START, "d1": "2022-01-01"},
                    {"s": "400", "d0": second_start, "d1": END},
                ],
            }
        }


class _Client:
    def __init__(self, *, cost_delta: float = 0.0, gap: bool = False) -> None:
        self.metadata = _Metadata(cost_delta=cost_delta)
        self.symbology = _Symbology(gap=gap)


def _budget(tmp_path: Path) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        ledger_path=str(tmp_path / "spend.jsonl"),
        summary_path=str(tmp_path / "summary.md"),
    )


def _kwargs(tmp_path: Path) -> dict[str, object]:
    return {
        "root": PROJECT_ROOT,
        "budget": _budget(tmp_path),
        "receipt_path": tmp_path / "receipt.json",
        "cache_root": tmp_path / "cache",
        "access_ledger_path": tmp_path / "access.jsonl",
        "local_lock_path": tmp_path / "local.lock",
        "global_lock_path": tmp_path / "global.lock",
    }


def test_manifest_is_pinned_to_pre_q4_inputs_and_single_leg_router() -> None:
    manifest = load_and_validate_manifest(PROJECT_ROOT)
    assert manifest["manifest_hash"] == EXPECTED_MANIFEST_HASH
    assert stable_hash(
        {key: value for key, value in manifest.items() if key != "manifest_hash"}
    ) == EXPECTED_MANIFEST_HASH
    assert manifest["new_data_contract"]["symbols"] == list(SYMBOLS)
    assert manifest["new_data_contract"]["end_exclusive"] == "2024-10-01"
    assert manifest["new_data_contract"]["official_total_cost_usd"] == pytest.approx(
        EXPECTED_TOTAL_COST_USD
    )
    assert manifest["economic_relationship"]["full_board_crush_contract_count"] == 30
    assert manifest["account_contract"]["full_board_crush_execution_allowed"] is False
    assert manifest["economic_relationship"]["production_policy"] == (
        "EXECUTE_EXACTLY_ONE_ROUTED_LEG_PER_OPPORTUNITY"
    )


def test_rehashed_manifest_mutation_is_rejected(tmp_path: Path) -> None:
    manifest = json.loads(
        (PROJECT_ROOT / acquisition.MANIFEST_PATH).read_text(encoding="utf-8")
    )
    manifest["candidate_lattice"]["past_only_robust_z_thresholds"] = [1.0, 2.0]
    core = dict(manifest)
    core.pop("manifest_hash")
    manifest["manifest_hash"] = stable_hash(core)
    path = tmp_path / "mutated.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SoybeanCrushAcquisitionError, match="manifest hash drift"):
        load_and_validate_manifest(PROJECT_ROOT, path)


def test_dry_run_is_metadata_only_and_checks_live_budget(tmp_path: Path) -> None:
    client = _Client()
    result = estimate_or_acquire(client=client, execute=False, **_kwargs(tmp_path))
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["market_data_downloaded"] is False
    assert result["files_created"] == 0
    assert result["official_total_cost_usd"] == pytest.approx(EXPECTED_TOTAL_COST_USD)
    assert result["remaining_after_estimate_usd"] == pytest.approx(
        200.720719923081 - EXPECTED_TOTAL_COST_USD
    )
    assert result["symbology"]["coverage"]["ZM.c.0"]["gap_count"] == 0
    assert not (tmp_path / "receipt.json").exists()
    assert not (tmp_path / "spend.jsonl").exists()
    assert not (tmp_path / "cache").exists()


@pytest.mark.parametrize(
    ("client", "message"),
    [
        (_Client(cost_delta=0.01), "official estimate drift"),
        (_Client(gap=True), "symbology gap or overlap"),
    ],
)
def test_cost_or_symbology_drift_fails_closed(
    tmp_path: Path, client: _Client, message: str
) -> None:
    with pytest.raises(SoybeanCrushAcquisitionError, match=message):
        estimate_or_acquire(client=client, execute=False, **_kwargs(tmp_path))
    assert not (tmp_path / "spend.jsonl").exists()


def test_explicit_execute_seals_hashes_and_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    download_calls: list[str] = []

    def _fake_download(
        _client: object,
        request: Mapping[str, object],
        path: Path,
        *,
        stype_out: str,
    ) -> bool:
        assert stype_out == "instrument_id"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return False
        schema = str(request["schema"])
        path.write_bytes(f"immutable-{schema}".encode())
        download_calls.append(schema)
        return True

    monkeypatch.setattr(acquisition, "_download_once", _fake_download)
    client = _Client()
    result = estimate_or_acquire(client=client, execute=True, **_kwargs(tmp_path))

    assert result["download_status"] == "DOWNLOADED"
    assert result["actual_incremental_spend_usd"] == pytest.approx(
        EXPECTED_TOTAL_COST_USD
    )
    assert download_calls == ["ohlcv-1m", "definition"]
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 2
    assert len((tmp_path / "access.jsonl").read_text().splitlines()) == 3
    for row in result["files"]:
        assert sha256_file(row["path"]) == row["sha256"]

    again = estimate_or_acquire(client=_Client(), execute=True, **_kwargs(tmp_path))
    assert again["receipt_hash"] == result["receipt_hash"]
    assert download_calls == ["ohlcv-1m", "definition"]
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 2

    raw = next(row for row in result["files"] if row["kind"] == "RAW_DBN_OHLCV_1M")
    Path(raw["path"]).write_bytes(b"tampered")
    with pytest.raises(SoybeanCrushAcquisitionError, match="artifact drift"):
        estimate_or_acquire(client=_Client(), execute=True, **_kwargs(tmp_path))


def test_outstanding_commitments_are_counted_before_purchase(tmp_path: Path) -> None:
    row = {
        "request_id": "other-download",
        "timestamp_utc": "2026-07-20T00:00:00Z",
        "dataset": "GLBX.MDP3",
        "schema": "other",
        "symbols": ["OTHER"],
        "stype_in": "continuous",
        "start": START,
        "end": END,
        "estimated_cost_usd": 196.0,
        "actual_cost_usd": None,
        "cumulative_estimated_spend_usd": 196.0,
        "cumulative_actual_spend_usd": 0.0,
        "cache_hit": False,
        "research_purpose": "test",
        "candidate_tier": "TIER_H",
        "approval_mode": "AUTO_UNDER_HARD_CAP",
        "resulting_file": None,
        "checksum": None,
        "download_status": "ESTIMATED_ONLY",
    }
    (tmp_path / "spend.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    with pytest.raises(SoybeanCrushAcquisitionError, match="authoritative budget"):
        estimate_or_acquire(client=_Client(), execute=False, **_kwargs(tmp_path))
