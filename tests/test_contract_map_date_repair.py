from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hydra.data.contract_mapping import (
    ContractInfo,
    RollMap,
    repair_roll_map_from_date_aware_definitions,
    resolve_date_aware_definition,
    valid_outright_future_symbol,
    write_roll_map,
)
from hydra.mission.experiment_runner import run_experiment


def _definition(
    *,
    ts_event: str,
    instrument_id: int = 1,
    raw_symbol: str = "ESM4",
    instrument_class: str = "F",
    security_type: str = "FUT",
    asset: str = "ES",
    tick_size: float = 0.25,
) -> dict[str, object]:
    return {
        "ts_event": ts_event,
        "instrument_id": instrument_id,
        "raw_symbol": raw_symbol,
        "instrument_class": instrument_class,
        "security_type": security_type,
        "asset": asset,
        "min_price_increment": tick_size,
        "expiration": "2024-06-21T13:30:00Z",
        "activation": "2023-06-16T13:30:00Z",
    }


def _reused_id_history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _definition(
                ts_event="2023-01-01T17:00:00Z",
                raw_symbol="ZNH3",
                asset="ZN",
                tick_size=0.015625,
            ),
            _definition(ts_event="2024-03-17T11:04:10Z"),
        ]
    )


def _frozen_map() -> RollMap:
    return RollMap(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        map_type="EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
        symbols=["ES"],
        contracts=[
            ContractInfo(
                root="ES",
                contract="ESZ2",
                month_code="Z",
                year=2022,
                expiry_date="2022-12-16",
                last_trade_date="2022-12-16",
                active_start="2024-03-17",
                active_end="2024-06-23",
                roll_date="2024-03-17",
                tick_size=0.5,
                tick_value=12.5,
                point_value=50.0,
                contract_multiplier=50.0,
                is_micro=False,
                instrument_id="1",
                parent_symbol="ES",
                continuous_symbol="ES.c.0",
                activation_time="2022-01-01T00:00:00+00:00",
                deactivation_time="2024-06-23",
                roll_reason="databento_continuous_front_contract_transition",
                transition_uncertainty="date_level_symbology_interval",
            )
        ],
        unsafe_window_days=3,
        notes=["frozen predecessor"],
        source_metadata={"period_start": "2023-01-01"},
    )


def test_date_aware_resolution_uses_definition_at_segment_date_for_reused_id() -> None:
    resolved = resolve_date_aware_definition(
        _reused_id_history(),
        instrument_id="1",
        active_start="2024-03-17",
        root="ES",
    )
    assert resolved["raw_symbol"] == "ESM4"
    assert resolved["ts_event"] == "2024-03-17T11:04:10+00:00"
    assert valid_outright_future_symbol("ES", str(resolved["raw_symbol"]))


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ({"raw_symbol": "NQM4", "asset": "NQ"}, "outright futures symbol"),
        ({"instrument_class": "S"}, "instrument_class=F"),
        ({"security_type": "OPT"}, "security_type=FUT"),
        ({"min_price_increment": 0.5}, "differs from root spec"),
    ],
)
def test_date_aware_resolution_fails_closed_on_invalid_definition(
    replacement: dict[str, object], message: str
) -> None:
    row = _definition(ts_event="2024-03-17T11:04:10Z")
    row.update(replacement)
    with pytest.raises(ValueError, match=message):
        resolve_date_aware_definition(
            pd.DataFrame([row]),
            instrument_id="1",
            active_start="2024-03-17",
            root="ES",
        )


def test_date_aware_resolution_rejects_ambiguous_same_timestamp() -> None:
    first = _definition(ts_event="2024-03-17T11:04:10Z")
    second = _definition(ts_event="2024-03-17T11:04:10Z", raw_symbol="ESU4")
    with pytest.raises(ValueError, match="Ambiguous definition history"):
        resolve_date_aware_definition(
            pd.DataFrame([first, second]),
            instrument_id="1",
            active_start="2024-03-17",
            root="ES",
        )


def test_repair_changes_only_authorized_fields_and_writes_deterministically(
    tmp_path: Path,
) -> None:
    frozen = _frozen_map()
    repaired, audit = repair_roll_map_from_date_aware_definitions(
        frozen, _reused_id_history()
    )
    old = frozen.contracts[0]
    new = repaired.contracts[0]
    assert new.contract == "ESM4"
    assert new.month_code == "M"
    assert new.year == 2024
    assert new.tick_size == 0.25
    assert audit["segment_count"] == 1
    assert audit["symbol_change_count"] == 1
    assert audit["tick_size_change_count"] == 1
    for field in (
        "root",
        "instrument_id",
        "continuous_symbol",
        "active_start",
        "active_end",
        "roll_date",
        "tick_value",
        "point_value",
        "contract_multiplier",
        "is_micro",
        "parent_symbol",
    ):
        assert getattr(new, field) == getattr(old, field)
    first_path, first_hash = write_roll_map(repaired, folder=str(tmp_path))
    second_path, second_hash = write_roll_map(repaired, folder=str(tmp_path))
    assert first_path == second_path
    assert first_hash == second_hash == repaired.roll_map_hash()


def test_experiment_runner_dispatches_contract_map_repair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    def fake(output_dir: Path, **kwargs: object) -> dict[str, str]:
        captured.update({"output_dir": output_dir, **kwargs})
        return {"scientific_conclusion": "repaired"}

    monkeypatch.setattr(
        "hydra.validation.contract_map_date_repair.run_contract_map_date_aware_repair",
        fake,
    )
    result = run_experiment(
        {
            "experiment_id": "repair",
            "experiment_type": "contract_map_date_aware_repair",
            "integrity_pilot_result_path": "pilot.json",
            "integrity_pilot_result_hash": "pilot-hash",
            "frozen_contract_map_path": "map.json",
            "frozen_contract_map_sha256": "map-hash",
            "definition_dbn_path": "definitions.dbn.zst",
            "definition_dbn_sha256": "definition-hash",
            "engineering_task_path": "task.md",
            "engineering_task_sha256": "task-hash",
            "code_commit": "commit",
        },
        output_root=tmp_path,
    )
    assert result["scientific_conclusion"] == "repaired"
    assert captured["integrity_pilot_result_hash"] == "pilot-hash"
    assert captured["definition_dbn_sha256"] == "definition-hash"
