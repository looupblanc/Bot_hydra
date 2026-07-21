from __future__ import annotations

from pathlib import Path

import pandas as pd

import hydra.research.option_settlement_surface_tripwire as tripwire


def _manifest():
    return {
        "structural_opportunities": {"target_stop_multiple": 1.5},
        "causal_and_execution_contract": {
            "mandatory_flatten_local": "15:10",
            "stressed_extra_slippage_ticks_per_side": 1,
            "normal_round_turn_fees_usd": {"ES": 5.28, "NQ": 5.28},
        },
    }


def test_next_open_is_strict_and_double_touch_is_stop_first():
    rows = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-09-04T13:45:00Z", "2024-09-04T13:46:00Z", "2024-09-04T13:47:00Z"], utc=True
            ),
            "open": [100.0, 101.0, 101.0],
            "high": [100.5, 103.0, 101.5],
            "low": [99.5, 99.0, 100.5],
            "close": [100.0, 101.0, 101.0],
            "instrument_id": [7, 7, 7],
        }
    )
    opportunity = {
        "market": "ES", "session": "2024-09-04", "decision_time": "2024-09-04T13:45:00+00:00",
        "direction": 1, "stop_distance": 1.0, "holding_minutes": 2,
    }
    result = tripwire._scenario_trade(opportunity, rows, _manifest(), scenario="NORMAL", direction_flip=False)
    assert result["entry_time"] == "2024-09-04T13:46:00+00:00"
    assert result["entry_price"] == 101.0
    assert result["exit_reason"] == "STOP_FIRST"
    assert result["same_bar_stop_first"] is True


def test_single_class_teacher_is_constant_abstain_at_frozen_threshold():
    rows = [{name: float(index) for index, name in enumerate(tripwire.TEACHER_FEATURES)} for _ in range(12)]
    model = tripwire.fit_frozen_logit(
        rows, [0] * len(rows), features=tripwire.TEACHER_FEATURES, threshold=0.55, regularization_c=1.0
    )
    assert model.model_kind == "DEGENERATE_L2_INTERCEPT_ONLY"
    assert model.positive_labels == 0
    assert not model.trade(rows[0])


def test_heldout_outcomes_are_opened_only_after_durable_model_freeze(tmp_path, monkeypatch):
    opportunity = {
        "opportunity_id": "o1", "role": "DISCOVERY", "session": "2024-09-04", "market": "ES",
        "family": "OPENING_FIFTEEN_MINUTE_DISPLACEMENT_CONTINUATION",
    }
    manifest = {"branch_id": "b", "manifest_hash": "m"}
    monkeypatch.setattr(tripwire, "read_and_audit_inputs", lambda root: {"manifest": manifest, "audit_hash": "a"})
    monkeypatch.setattr(tripwire, "load_surfaces", lambda audit: ({}, {"snapshot_hash": "s"}))
    monkeypatch.setattr(tripwire, "load_futures_bars", lambda audit: ({}, {"rows": 1}))
    monkeypatch.setattr(tripwire, "build_opportunity_inputs", lambda audit, surfaces, bars: [opportunity])
    calls = []

    def outcomes(opportunities, bars, manifest_value, *, roles):
        calls.append(set(roles))
        if "VALIDATION" in roles:
            assert (tmp_path / "model_freeze.json").is_file()
        return {"o1": {}} if roles == frozenset({"DISCOVERY"}) else {}

    monkeypatch.setattr(tripwire, "materialize_outcomes", outcomes)
    model = tripwire.FrozenLogit(("x",), (0.0,), (1.0,), (0.0,), -10.0, 0.5, 1, 0, "DEGENERATE")
    predictions = {"o1": {"teacher_trade": False, "student_trade": False}}
    freeze = {"schema": "freeze", "model_freeze_hash": "f"}
    monkeypatch.setattr(tripwire, "train_and_freeze_models", lambda *args: (model, model, predictions, freeze))
    monkeypatch.setattr(
        tripwire, "evaluate",
        lambda *args: {"status": "OPTION_SETTLEMENT_TEACHER_NO_INCREMENT", "opportunity_counts": {}, "teacher_gate_pass": False, "student_gate_pass": False},
    )
    result = tripwire.run_tripwire(tmp_path, tmp_path)
    assert calls == [{"DISCOVERY"}, {"VALIDATION", "FINAL_DEVELOPMENT"}]
    assert result["evaluation"]["status"] == "OPTION_SETTLEMENT_TEACHER_NO_INCREMENT"

