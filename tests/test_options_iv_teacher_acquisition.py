from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.data.budget import DatabentoBudgetConfig, sha256_file
from hydra.economic_evolution.schema import stable_hash
import hydra.data.options_iv_teacher_acquisition as acquisition
from hydra.data.options_iv_teacher_acquisition import (
    END,
    EXPECTED_ESTIMATES,
    EXPECTED_MANIFEST_HASH,
    EXPECTED_TOTAL_COST_USD,
    MANIFEST_PATH,
    MAX_INCREMENTAL_USD,
    SCHEMAS,
    START,
    SYMBOLS,
    OptionsTeacherAcquisitionError,
    estimate_or_acquire,
    frozen_requests,
    load_and_validate_manifest,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _Metadata:
    def __init__(self, costs: dict[str, float] | None = None) -> None:
        self.costs = costs or {
            schema: float(EXPECTED_ESTIMATES[schema]["estimated_cost_usd"])
            for schema in SCHEMAS
        }
        self.calls: list[tuple[str, str]] = []

    def get_cost(self, **kwargs: object) -> float:
        self.calls.append(("cost", str(kwargs["schema"])))
        return self.costs[str(kwargs["schema"])]

    def get_record_count(self, **kwargs: object) -> int:
        self.calls.append(("records", str(kwargs["schema"])))
        return int(EXPECTED_ESTIMATES[str(kwargs["schema"])]["record_count"])

    def get_billable_size(self, **kwargs: object) -> int:
        self.calls.append(("bytes", str(kwargs["schema"])))
        return int(
            EXPECTED_ESTIMATES[str(kwargs["schema"])]["billable_size_bytes"]
        )


class _Timeseries:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_range(self, **kwargs: object) -> None:
        self.calls.append(dict(kwargs))
        Path(str(kwargs["path"])).write_bytes(
            f"immutable-{kwargs['schema']}".encode()
        )


class _Client:
    def __init__(self, costs: dict[str, float] | None = None) -> None:
        self.metadata = _Metadata(costs)
        self.timeseries = _Timeseries()


def _budget(tmp_path: Path, *, cap: float = 200.720719923081) -> DatabentoBudgetConfig:
    return DatabentoBudgetConfig(
        hard_cap_usd=cap,
        safety_ceiling_usd=cap,
        ledger_path=str(tmp_path / "spend.jsonl"),
        summary_path=str(tmp_path / "summary.md"),
    )


def _kwargs(tmp_path: Path, *, cap: float = 200.720719923081) -> dict[str, object]:
    return {
        "root": PROJECT_ROOT,
        "manifest_path": PROJECT_ROOT / MANIFEST_PATH,
        "budget": _budget(tmp_path, cap=cap),
        "cache_root": tmp_path / "cache",
        "receipt_path": tmp_path / "receipt.json",
        "access_ledger_path": tmp_path / "access.jsonl",
        "lock_path": tmp_path / "acquisition.lock",
    }


def test_contract_is_exact_parent_symbology_and_pre_q4() -> None:
    manifest = load_and_validate_manifest(PROJECT_ROOT)
    assert manifest["manifest_hash"] == EXPECTED_MANIFEST_HASH
    requests = frozen_requests()
    assert tuple(requests) == SCHEMAS
    assert START == "2024-09-03"
    assert END == "2024-10-01"
    assert MAX_INCREMENTAL_USD == 0.70
    for schema, request in requests.items():
        assert request == {
            "dataset": "GLBX.MDP3",
            "schema": schema,
            "symbols": list(SYMBOLS),
            "stype_in": "parent",
            "start": START,
            "end": END,
        }


def test_dry_run_rechecks_official_metadata_but_creates_nothing(tmp_path: Path) -> None:
    client = _Client()
    result = estimate_or_acquire(client=client, execute=False, **_kwargs(tmp_path))
    assert result["download_status"] == "DRY_RUN_ONLY"
    assert result["aggregate_estimated_cost_usd"] == pytest.approx(
        EXPECTED_TOTAL_COST_USD
    )
    assert result["network_data_request_made"] is False
    assert result["files_created"] == 0
    assert len(client.metadata.calls) == 6
    assert client.timeseries.calls == []
    assert not (tmp_path / "spend.jsonl").exists()
    assert not (tmp_path / "receipt.json").exists()
    assert not (tmp_path / "cache").exists()


def test_live_metadata_drift_and_cumulative_cap_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(OptionsTeacherAcquisitionError, match="metadata estimate drift"):
        estimate_or_acquire(
            client=_Client(
                {
                    "statistics": 0.435541570187,
                    "definition": 0.082069896163,
                }
            ),
            execute=False,
            **_kwargs(tmp_path),
        )

    prior = {
        "request_id": "prior",
        "actual_cost_usd": 0.45,
        "estimated_cost_usd": 0.0,
        "download_status": "DOWNLOADED",
    }
    (tmp_path / "spend.jsonl").write_text(json.dumps(prior) + "\n")
    with pytest.raises(OptionsTeacherAcquisitionError, match="mission budget"):
        estimate_or_acquire(
            client=_Client(), execute=False, **_kwargs(tmp_path, cap=0.9)
        )


def test_execute_is_atomic_hashed_and_idempotent(tmp_path: Path) -> None:
    client = _Client()
    result = estimate_or_acquire(client=client, execute=True, **_kwargs(tmp_path))

    assert result["download_status"] == "DOWNLOADED"
    assert result["actual_incremental_spend_usd"] == pytest.approx(
        EXPECTED_TOTAL_COST_USD
    )
    assert result["q4_access_count_delta"] == 0
    assert result["teacher_only"] is True
    assert [row["schema"] for row in client.timeseries.calls] == list(SCHEMAS)
    assert all(row["stype_out"] == "raw_symbol" for row in client.timeseries.calls)
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 2
    assert len((tmp_path / "access.jsonl").read_text().splitlines()) == 3
    for row in result["files"]:
        assert sha256_file(row["path"]) == row["sha256"]

    second = _Client()
    again = estimate_or_acquire(client=second, execute=True, **_kwargs(tmp_path))
    assert again["receipt_hash"] == result["receipt_hash"]
    assert second.metadata.calls == []
    assert second.timeseries.calls == []
    assert len((tmp_path / "spend.jsonl").read_text().splitlines()) == 2


def test_tampered_dbn_or_rehashed_manifest_mutation_is_rejected(tmp_path: Path) -> None:
    result = estimate_or_acquire(client=_Client(), execute=True, **_kwargs(tmp_path))
    Path(result["files"][0]["path"]).write_bytes(b"tampered")
    with pytest.raises(OptionsTeacherAcquisitionError, match="artifact drift"):
        estimate_or_acquire(client=_Client(), execute=True, **_kwargs(tmp_path))

    manifest = json.loads((PROJECT_ROOT / MANIFEST_PATH).read_text(encoding="utf-8"))
    manifest["data_contract"]["stype_out"] = "instrument_id"
    core = dict(manifest)
    core.pop("manifest_hash")
    manifest["manifest_hash"] = stable_hash(core)
    mutated = tmp_path / "mutated_manifest.json"
    mutated.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(OptionsTeacherAcquisitionError, match="manifest hash drift"):
        estimate_or_acquire(
            root=PROJECT_ROOT,
            client=_Client(),
            manifest_path=mutated,
            execute=False,
            budget=_budget(tmp_path),
        )
