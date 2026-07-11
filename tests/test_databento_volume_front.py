from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hydra.data.budget import (
    DatabentoBudgetConfig,
    DatabentoSpendRecord,
    append_spend_record,
)
from hydra.data.contract_mapping import ContractInfo, RollMap
from hydra.data.databento_loader import DatabentoCostLimitError
from hydra.data.databento_volume_front import (
    VOLUME_FRONT_MAP_TYPE,
    VolumeFrontDataError,
    acquire_volume_front,
    build_volume_front_roll_map,
    normalize_volume_mappings,
    validate_volume_front_frame,
    volume_front_request,
)


def _contract(root: str, instrument_id: str, contract: str) -> ContractInfo:
    micro = root == "MGC"
    return ContractInfo(
        root=root,
        contract=contract,
        month_code=contract[len(root)],
        year=2024,
        expiry_date="2024-04-26",
        last_trade_date="2024-04-26",
        active_start="2024-03-01",
        active_end="2024-04-01",
        roll_date="2024-04-01",
        tick_size=0.1,
        tick_value=1.0 if micro else 10.0,
        point_value=10.0 if micro else 100.0,
        contract_multiplier=10.0 if micro else 100.0,
        is_micro=micro,
        instrument_id=instrument_id,
        parent_symbol=root,
        continuous_symbol=f"{root}.c.0",
    )


def test_volume_request_is_distinct_and_q4_guarded(tmp_path: Path) -> None:
    request = volume_front_request(cache_folder=str(tmp_path))

    assert request.api_symbols == ["GC.v.0", "MGC.v.0"]
    assert request.symbol_map == {"GC.v.0": "GC", "MGC.v.0": "MGC"}
    assert "GC-v-0_MGC-v-0" in request.output_path
    assert "GC_MGC" not in Path(request.output_path).name
    with pytest.raises(VolumeFrontDataError):
        volume_front_request(end="2025-01-01", cache_folder=str(tmp_path))


def test_volume_frame_requires_both_roots_and_valid_ohlc() -> None:
    timestamps = pd.date_range("2023-01-03T00:00:00Z", periods=4, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": list(timestamps[:2]) + list(timestamps[2:]),
            "symbol": ["GC", "GC", "MGC", "MGC"],
            "timeframe": ["1m"] * 4,
            "open": [1900.0, 1901.0, 1900.0, 1901.0],
            "high": [1902.0, 1902.0, 1902.0, 1902.0],
            "low": [1899.0, 1900.0, 1899.0, 1900.0],
            "close": [1901.0, 1901.5, 1901.0, 1901.5],
            "volume": [1, 2, 3, 4],
            "session_id": ["2023-01-03"] * 4,
        }
    )
    result = validate_volume_front_frame(
        frame,
        roots=("GC", "MGC"),
        start="2023-01-01",
        end="2024-10-01",
        minimum_rows_per_root=2,
    )

    assert result["rows_by_required_root"] == {"GC": 2, "MGC": 2}
    assert result["q4_rows"] == 0
    with pytest.raises(VolumeFrontDataError):
        validate_volume_front_frame(
            frame[frame.symbol.eq("GC")],
            roots=("GC", "MGC"),
            start="2023-01-01",
            end="2024-10-01",
            minimum_rows_per_root=2,
        )


def test_volume_roll_map_uses_verified_definition_crosswalk() -> None:
    base = RollMap(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        map_type="BASE",
        symbols=["GC", "MGC"],
        contracts=[
            _contract("GC", "101", "GCJ4"),
            _contract("MGC", "201", "MGCJ4"),
        ],
        unsafe_window_days=3,
        notes=[],
    )
    mappings = {
        "GC.v.0": [{"d0": "2024-03-01", "d1": "2024-04-01", "s": "101"}],
        "MGC.v.0": [{"d0": "2024-03-01", "d1": "2024-04-01", "s": "201"}],
    }
    result = build_volume_front_roll_map(
        mappings,
        base,
        roots=("GC", "MGC"),
        start="2023-01-01",
        end="2024-10-01",
        data_checksum="a" * 64,
    )

    assert result.map_type == VOLUME_FRONT_MAP_TYPE
    assert {item.continuous_symbol for item in result.contracts} == {
        "GC.v.0",
        "MGC.v.0",
    }
    assert all(
        item.roll_reason == "databento_previous_day_volume_rank_transition"
        for item in result.contracts
    )
    with pytest.raises(VolumeFrontDataError):
        build_volume_front_roll_map(
            {**mappings, "GC.v.0": [{"d0": "2024-03-01", "d1": "2024-04-01", "s": "999"}]},
            base,
            roots=("GC", "MGC"),
            start="2023-01-01",
            end="2024-10-01",
            data_checksum="a" * 64,
        )


def test_volume_metadata_mapping_normalizes_current_databento_shape() -> None:
    mappings = {
        "GC.v.0": [
            {
                "start_date": "2023-01-01",
                "end_date": "2023-02-01",
                "symbol": "101",
            }
        ],
        "MGC.v.0": [
            {
                "start_date": "2023-01-01",
                "end_date": "2023-02-01",
                "symbol": "201",
            }
        ],
    }

    assert normalize_volume_mappings(mappings) == {
        "GC.v.0": [{"d0": "2023-01-01", "d1": "2023-02-01", "s": "101"}],
        "MGC.v.0": [{"d0": "2023-01-01", "d1": "2023-02-01", "s": "201"}],
    }
    with pytest.raises(VolumeFrontDataError):
        normalize_volume_mappings({"GC.v.0": mappings["GC.v.0"]})


def test_acquisition_rejects_cost_and_reserve_before_download(tmp_path: Path) -> None:
    request = volume_front_request(cache_folder=str(tmp_path / "cache"))
    base = tmp_path / "base.json"
    base.write_text("{}\n", encoding="utf-8")
    budget = DatabentoBudgetConfig(
        ledger_path=str(tmp_path / "ledger.jsonl"),
        summary_path=str(tmp_path / "summary.md"),
    )
    with pytest.raises(DatabentoCostLimitError):
        acquire_volume_front(
            request,
            key="redacted",
            budget=budget,
            base_roll_map_path=base,
            output_report_dir=tmp_path / "report",
            estimate={"estimated_cost_usd": 5.01},
        )

    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id="prior",
            timestamp_utc="2026-07-11T00:00:00Z",
            dataset="GLBX.MDP3",
            schema="ohlcv-1m",
            symbols=["ES"],
            stype_in="continuous",
            start="2023-01-01",
            end="2024-01-01",
            estimated_cost_usd=0.0,
            actual_cost_usd=70.0,
            cumulative_estimated_spend_usd=0.0,
            cumulative_actual_spend_usd=70.0,
            cache_hit=False,
            research_purpose="prior",
            candidate_tier="DATA",
            approval_mode="AUTO_UNDER_HARD_CAP",
            resulting_file=None,
            checksum=None,
            download_status="DOWNLOADED",
        ),
    )
    with pytest.raises(DatabentoCostLimitError):
        acquire_volume_front(
            request,
            key="redacted",
            budget=budget,
            base_roll_map_path=base,
            output_report_dir=tmp_path / "report",
            estimate={"estimated_cost_usd": 4.5},
        )
