from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from hydra.data.budget import DatabentoBudgetConfig, read_ledger
from hydra.data.q4_data_plan import acquire_q4_data, build_q4_data_plan
from hydra.governance.q4_one_shot import AuthorizedQ4Capability


class _Metadata:
    def get_cost(self, **kwargs: object) -> float:
        return 0.02 if kwargs.get("schema") == "definition" else 0.10


class _Symbology:
    def resolve(self, **kwargs: object) -> dict[str, object]:
        result = {}
        for index, symbol in enumerate(kwargs["symbols"], start=1):
            result[symbol] = [
                {"d0": "2024-10-01", "d1": "2025-01-01", "s": str(index)}
            ]
        return {"result": result}


class _Timeseries:
    def get_range(self, **kwargs: object) -> object:
        Path(str(kwargs["path"])).write_bytes(b"mock-dbn")
        return object()


class _Client:
    metadata = _Metadata()
    symbology = _Symbology()
    timeseries = _Timeseries()


def _manifest() -> dict[str, object]:
    return {
        "cohort_id": "cohort",
        "manifest_hash": "b" * 64,
        "q4_access_count_before": 0,
        "candidates": [
            {"primary_market": "NQ", "execution_market": "MNQ"},
            {"primary_market": "CL", "execution_market": "MCL"},
        ],
    }


def _capability() -> AuthorizedQ4Capability:
    marker = hashlib.sha256(f"token:{'b' * 64}:{'a' * 40}".encode()).hexdigest()
    return AuthorizedQ4Capability(
        token_id="token",
        cohort_id="cohort",
        cohort_manifest_hash="b" * 64,
        source_commit="a" * 40,
        consumption_path="/tmp/unused",
        _scope_marker=marker,
    )


def test_q4_plan_audits_cache_records_cost_and_preserves_reserve(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    cached = cache / "GLBX-MDP3_ohlcv-1m_NQ_MNQ_2024-10-01_2025-01-01.dbn.zst"
    cached.write_bytes(b"existing-q4-sealed")
    budget = DatabentoBudgetConfig(
        ledger_path=str(tmp_path / "spend.jsonl"),
        summary_path=str(tmp_path / "summary.md"),
    )
    plan = build_q4_data_plan(
        _manifest(),
        cache_root=cache,
        budget=budget,
        client=_Client(),
        record_request_plans=True,
    )
    assert plan["data_decoded_or_inspected"] is False
    assert plan["official_total_estimated_cost_usd"] == pytest.approx(0.12)
    assert [row["cache_hit"] for row in plan["sources"]] == [True, False]
    planned = read_ledger(tmp_path / "spend.jsonl")
    assert len(planned) == 2
    assert all(row["download_status"] == "ESTIMATED_ONLY" for row in planned)

    receipt = acquire_q4_data(
        plan,
        _capability(),
        budget=budget,
        client=_Client(),
    )
    assert receipt["actual_incremental_cost_usd"] == pytest.approx(0.12)
    assert all(Path(row["path"]).is_file() for row in receipt["sources"])
    assert Path(receipt["definitions"]["path"]).is_file()
    completed = read_ledger(tmp_path / "spend.jsonl")
    assert len([row for row in completed if row["download_status"] == "DOWNLOADED"]) == 2
