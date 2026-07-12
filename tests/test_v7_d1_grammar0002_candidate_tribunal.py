from __future__ import annotations

import json
from pathlib import Path

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.research.v7_d1_microstructure_grammar import load_feature_store
from hydra.research.v7_d1_microstructure_grammar_0002 import D1G2Signal, candidate_specs
from hydra.validation.v7_d1_grammar0002_candidate_tribunal import (
    _promotion_gates,
    delay_grammar0002_signals_five_sessions,
)
from hydra.validation.v7_d1_grammar0002_tripwire import build_grammar0002_events


def _frozen_signals() -> dict[str, tuple[D1G2Signal, ...]]:
    payload = json.loads(
        Path(
            "reports/v7/data/d1_microstructure_grammar0002_signal_manifest.json"
        ).read_text(encoding="utf-8")
    )
    return {
        candidate_id: tuple(D1G2Signal(**row) for row in rows)
        for candidate_id, rows in payload["signals"].items()
    }


def test_grammar0002_delayed_null_is_executable_and_powered() -> None:
    minute, _event = load_feature_store(".")
    signals = _frozen_signals()
    delayed = delay_grammar0002_signals_five_sessions(signals, minute)
    specs = {row.candidate_id: row for row in candidate_specs(".")}

    events = build_grammar0002_events(
        minute,
        delayed,
        specs,
        load_cost_model(),
        stress=CostStress.STRESS_1_5X,
    )

    assert set(events) == set(signals)
    assert all(len(events[key]) >= 30 for key in events)
    assert all(len(events[key]) >= 0.5 * len(signals[key]) for key in events)


def test_grammar0002_promotion_excludes_combine_fitness() -> None:
    row = {
        "stage1_pass": True,
        "stress_1_5x": {"expectancy_per_trade": 1.0},
        "SIM_EXPLOIT": False,
        "trajectory_compliance": True,
        "candidate_null_suite": {"passed": True},
        "walk_forward": {"pooled_expectancy_per_trade": 1.0},
        "DSR": {"deflated_z": 1.0},
        "BH": {"rejected": True},
        "combine_diagnostic": {"pass_rate": 1.0},
    }

    gates = _promotion_gates(row)

    assert all(gates.values())
    assert not any("combine" in key.lower() for key in gates)
