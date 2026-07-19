from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.research.curve_relative_value_tripwire import (
    _load_bound_pair_frames,
    _validate_input_contract,
    prepare_pair_frame,
)
from scripts.build_treasury_curve_input_contract import (
    ROOTS,
    TreasuryCurveInputError,
    build_from_frames,
)


def _fixture() -> tuple[pd.DataFrame, dict]:
    timestamps = pd.date_range(
        "2023-03-01T15:00:00Z", periods=5, freq="1D", tz="UTC"
    )
    id_base = {"ZT": 100, "ZF": 200, "ZN": 300, "TN": 400, "ZB": 500, "UB": 600}
    continuous: dict[str, list[dict[str, str]]] = {}
    raw_symbols: dict[str, str] = {}
    rows: list[dict] = []
    for root in ROOTS:
        first_id = str(id_base[root] + 1)
        second_id = str(id_base[root] + 2)
        # ZF rolls one day before the other roots.  This creates a real, bounded
        # delivery mismatch for ZT/ZF and ZF/ZN that must be excluded, not filled.
        boundary = "2023-03-02T00:00:00Z" if root == "ZF" else "2023-03-03T00:00:00Z"
        continuous[f"{root}.c.0"] = [
            {"d0": "2023-03-01T00:00:00Z", "d1": boundary, "s": first_id},
            {"d0": boundary, "d1": "2023-03-07T00:00:00Z", "s": second_id},
        ]
        raw_symbols[first_id] = f"{root}H3"
        raw_symbols[second_id] = f"{root}M3"
        for offset, timestamp in enumerate(timestamps):
            instrument_id = second_id if timestamp >= pd.Timestamp(boundary) else first_id
            price = 100.0 + id_base[root] / 100.0 + offset * 0.01
            rows.append(
                {
                    "ts_event": timestamp,
                    "instrument_id": int(instrument_id),
                    "open": price,
                    "high": price + 0.02,
                    "low": price - 0.02,
                    "close": price + 0.01,
                    "volume": 10 + offset,
                }
            )
    mapping_core = {
        "schema": "toy_two_stage_mapping_v1",
        "dataset": "GLBX.MDP3",
        "start": "2023-03-01T00:00:00Z",
        "end": "2023-03-07T00:00:00Z",
        "roots": list(ROOTS),
        "continuous_mapping": continuous,
        "instrument_ids": sorted(raw_symbols, key=int),
        "raw_symbol_mapping": raw_symbols,
    }
    return pd.DataFrame(rows), {**mapping_core, "mapping_hash": stable_hash(mapping_core)}


def test_roll_mismatch_is_explicitly_excluded_without_forward_fill(tmp_path: Path) -> None:
    frame, mapping = _fixture()
    result = build_from_frames(
        root=tmp_path,
        ohlcv_frame=frame,
        mapping_receipt=mapping,
        output_dir="bound",
    )

    zt_zf = result["pair_delivery_audits"]["ZT_ZF"]
    zf_zn = result["pair_delivery_audits"]["ZF_ZN"]
    assert zt_zf["timestamp_intersection"] == 5
    assert zt_zf["aligned_same_delivery_rows"] == 4
    assert zt_zf["delivery_mismatch_rows_excluded"] == 1
    assert zf_zn["delivery_mismatch_rows_excluded"] == 1
    assert zt_zf["forward_fill_rows"] == 0

    validated = _validate_input_contract(tmp_path, result["input_contract"])
    pair_frames, _receipt = _load_bound_pair_frames(tmp_path, validated)
    prepared, audit = prepare_pair_frame(pair_frames["ZT_ZF"], _pair("ZT_ZF"))
    assert len(prepared) == 4
    assert audit["delivery_mismatch_rows_excluded"] == 1
    assert audit["policy"] == "EXPLICIT_CONTRACT_DELIVERY_SYNC_NO_FORWARD_FILL"

    roll = json.loads((tmp_path / "bound" / "roll_receipt.json").read_text())
    assert roll["no_forward_fill"] is True
    assert roll["same_delivery_alignment_only"] is True


def test_builder_is_byte_deterministic_and_idempotent(tmp_path: Path) -> None:
    frame, mapping = _fixture()
    first = build_from_frames(
        root=tmp_path,
        ohlcv_frame=frame,
        mapping_receipt=mapping,
        output_dir="bound",
    )
    hashes_before = {
        path.name: _sha(path) for path in sorted((tmp_path / "bound").iterdir())
    }
    second = build_from_frames(
        root=tmp_path,
        ohlcv_frame=frame.sample(frac=1.0, random_state=19),
        mapping_receipt=mapping,
        output_dir="bound",
    )
    hashes_after = {
        path.name: _sha(path) for path in sorted((tmp_path / "bound").iterdir())
    }
    assert first == second
    assert hashes_before == hashes_after
    assert first["data_purchase_count"] == 0
    assert first["q4_access_count_delta"] == 0
    assert first["broker_connections"] == 0
    assert first["orders"] == 0


def test_overlapping_continuous_mapping_fails_closed(tmp_path: Path) -> None:
    frame, mapping = _fixture()
    broken_core = {key: value for key, value in mapping.items() if key != "mapping_hash"}
    broken_core["continuous_mapping"] = {
        key: [dict(row) for row in rows]
        for key, rows in broken_core["continuous_mapping"].items()
    }
    broken_core["continuous_mapping"]["ZT.c.0"][1]["d0"] = "2023-03-02T00:00:00Z"
    broken = {**broken_core, "mapping_hash": stable_hash(broken_core)}
    with pytest.raises(TreasuryCurveInputError, match="overlapping continuous intervals"):
        build_from_frames(
            root=tmp_path,
            ohlcv_frame=frame,
            mapping_receipt=broken,
            output_dir="bound",
        )


def test_observed_mapping_gap_fails_closed(tmp_path: Path) -> None:
    frame, mapping = _fixture()
    broken_core = {key: value for key, value in mapping.items() if key != "mapping_hash"}
    broken_core["continuous_mapping"] = {
        key: [dict(row) for row in rows]
        for key, rows in broken_core["continuous_mapping"].items()
    }
    broken_core["continuous_mapping"]["ZN.c.0"][1]["d0"] = "2023-03-04T00:00:00Z"
    broken = {**broken_core, "mapping_hash": stable_hash(broken_core)}
    with pytest.raises(TreasuryCurveInputError, match="uncovered continuous mapping"):
        build_from_frames(
            root=tmp_path,
            ohlcv_frame=frame,
            mapping_receipt=broken,
            output_dir="bound",
        )


def test_sealed_contract_interval_receipt_is_accepted(tmp_path: Path) -> None:
    frame, mapping = _fixture()
    core = {key: value for key, value in mapping.items() if key != "mapping_hash"}
    raw_symbols = core.pop("raw_symbol_mapping")
    core["contract_intervals"] = [
        {
            "root": symbol.split(".", 1)[0],
            "instrument_id": row["s"],
            "raw_symbol": raw_symbols[row["s"]],
            "d0": row["d0"],
            "d1": row["d1"],
        }
        for symbol, rows in core["continuous_mapping"].items()
        for row in rows
    ]
    sealed = {**core, "mapping_hash": stable_hash(core)}
    result = build_from_frames(
        root=tmp_path,
        ohlcv_frame=frame,
        mapping_receipt=sealed,
        output_dir="bound",
    )
    assert result["normalized_record_count"] == 30
    assert result["pair_delivery_audits"]["ZT_ZF"][
        "delivery_mismatch_rows_excluded"
    ] == 1


def _pair(pair_id: str):
    from hydra.research.curve_relative_value_tripwire import PAIR_SPECS

    return next(pair for pair in PAIR_SPECS if pair.pair_id == pair_id)


def _sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()
