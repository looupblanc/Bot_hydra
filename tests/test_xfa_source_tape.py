from __future__ import annotations

import ast
import gzip
from pathlib import Path

import numpy as np
import pytest

import hydra.propfirm.xfa_source_tape as source_tape
from hydra.propfirm.xfa_source_tape import (
    XFA_SOURCE_TAPE_SCHEMA,
    XfaSourceTape,
    XfaSourceTapeError,
    _FeatureArrays,
    _common_eligible_days,
    _rehydrate_trade,
    write_xfa_source_tape,
)
from hydra.economic_evolution.schema import stable_hash


MINUTE_NS = 60_000_000_000


def _arrays() -> dict[str, _FeatureArrays]:
    start = 1_700_000_000_000_000_000
    clock = np.asarray([start + index * MINUTE_NS for index in range(4)])
    return {
        "MCL": _FeatureArrays(
            market="MCL",
            bundle_hash="feature-bundle-hash",
            decision_ns=clock,
            timestamp_ns=clock,
            entry_price=np.asarray([70.0, 70.0, 70.0, 70.0]),
            bar_high=np.asarray([70.0, 71.0, 72.0, 71.0]),
            bar_low=np.asarray([70.0, 69.0, 70.0, 70.0]),
            segment_code=np.asarray([7, 7, 7, 7]),
            session_day=np.asarray([20231114] * 4),
            session_code=np.asarray([1, 1, 1, 1]),
        )
    }


def _ledger_rows() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    start = 1_700_000_000_000_000_000
    entry_time = "2023-11-14T22:13:20+00:00"
    exit_time = "2023-11-14T22:16:20+00:00"
    entry = {
        "component_id": "sleeve_a",
        "trade_id": "trade_a",
        "entry_time": entry_time,
        "entry_price": 70.0,
        "market": "MCL",
        "contract": "MCL",
        "side": "LONG",
        "quantity": 1,
        "decision_ns": start,
    }
    exit_row = {
        "component_id": "sleeve_a",
        "trade_id": "trade_a",
        "exit_time": exit_time,
        "exit_price": 71.0,
    }
    trade = {
        **entry,
        **exit_row,
        "gross_pnl": 100.0,
        "costs": 3.0,
        "net_pnl": 97.0,
    }
    return trade, entry, exit_row


def test_rehydrate_trade_preserves_identity_economics_and_extrema() -> None:
    trade, entry, exit_row = _ledger_rows()
    routed = _rehydrate_trade(trade, entry, exit_row, _arrays())

    assert routed.component_id == "sleeve_a"
    assert routed.market == "MCL"
    assert routed.side == 1
    assert routed.event.event_id == "trade_a"
    assert routed.event.net_pnl == pytest.approx(97.0)
    assert routed.event.gross_pnl == pytest.approx(100.0)
    assert routed.event.worst_unrealized_pnl == pytest.approx(-103.0)
    assert routed.event.best_unrealized_pnl == pytest.approx(197.0)
    assert routed.event.quantity == 1
    assert routed.event.mini_equivalent == pytest.approx(0.1)
    assert routed.event.session_day == 20231114


def test_rehydrate_trade_fails_closed_on_realized_economic_drift() -> None:
    trade, entry, exit_row = _ledger_rows()
    trade["gross_pnl"] = 99.0

    with pytest.raises(XfaSourceTapeError, match="gross"):
        _rehydrate_trade(trade, entry, exit_row, _arrays())


def test_source_tape_has_no_signal_position_call() -> None:
    tree = ast.parse(Path(source_tape.__file__).read_text(encoding="utf-8"))
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "_signal_positions" not in called_names | called_attributes


def test_common_eligible_days_is_an_exact_intersection(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    path.write_text(
        '{"sleeve_id":"a","eligible_session_days":[20260102,20260105,20260106]}\n'
        '{"sleeve_id":"b","eligible_session_days":[20260105,20260106,20260107]}\n',
        encoding="utf-8",
    )

    assert _common_eligible_days(path, {"a", "b"}) == (20260105, 20260106)


def test_source_tape_write_is_reproducible_atomic_and_refuses_drift(
    tmp_path: Path,
) -> None:
    trade, entry, exit_row = _ledger_rows()
    routed = _rehydrate_trade(trade, entry, exit_row, _arrays())
    payload = {
        "schema": XFA_SOURCE_TAPE_SCHEMA,
        "campaign_id": "campaign_test",
        "events": {"sleeve_a": [routed.to_dict()]},
        "eligible_session_days": [20231114],
        "event_count": 1,
        "component_count": 1,
        "normal_net_pnl": 97.0,
        "normal_gross_pnl": 100.0,
        "source_manifest_sha256": "source-sha",
        "feature_bundle_hashes": {"MCL": "feature-sha"},
    }
    tape = XfaSourceTape(
        schema=XFA_SOURCE_TAPE_SCHEMA,
        campaign_id="campaign_test",
        events={"sleeve_a": (routed,)},
        eligible_session_days=(20231114,),
        event_count=1,
        component_count=1,
        normal_net_pnl=97.0,
        normal_gross_pnl=100.0,
        source_manifest_sha256="source-sha",
        feature_bundle_hashes={"MCL": "feature-sha"},
        tape_hash=stable_hash(payload),
    )

    first = write_xfa_source_tape(tape, tmp_path)
    second = write_xfa_source_tape(tape, tmp_path)
    compressed = (tmp_path / "source_events.jsonl.gz").read_bytes()

    assert first == second
    assert int.from_bytes(compressed[4:8], "little") == 0
    with gzip.open(tmp_path / "source_events.jsonl.gz", "rt") as handle:
        assert sum(1 for _line in handle) == 1
    (tmp_path / "source_events.jsonl.gz").write_bytes(compressed + b"drift")
    with pytest.raises(XfaSourceTapeError, match="drift"):
        write_xfa_source_tape(tape, tmp_path)
