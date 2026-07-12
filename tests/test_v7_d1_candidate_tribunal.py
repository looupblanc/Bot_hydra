from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from hydra.research.v7_d1_microstructure_grammar import (
    D1Signal,
    candidate_specs,
    generate_signal_population,
    load_feature_store,
)
from hydra.validation.v7_d1_candidate_tribunal import (
    _delay_signals_five_sessions,
    _d1_event_compliance,
    _promotion_gates,
    _shift_flow_one_prior_session,
    _verify_inputs,
)
from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.validation.v7_d1_new_dataset_tripwire import (
    MINI_EQUIVALENT,
    build_candidate_events,
)


def test_flow_shift_preserves_prices_and_changes_flow() -> None:
    minute, event = load_feature_store(".")

    shifted_minute, shifted_event = _shift_flow_one_prior_session(minute, event)

    assert np.array_equal(shifted_minute["close"], minute["close"])
    assert np.array_equal(shifted_event["close"], event["close"])
    assert not np.array_equal(
        shifted_minute["signed_aggressor_volume"],
        minute["signed_aggressor_volume"],
    )
    assert not np.array_equal(
        shifted_event["signed_aggressor_volume"],
        event["signed_aggressor_volume"],
    )
    assert float((shifted_minute["total_volume"] > 0.0).mean()) > 0.90
    assert float((shifted_event["total_volume"] > 0.0).mean()) > 0.90


def test_shifted_flow_recomputes_frozen_signal_population() -> None:
    minute, event = load_feature_store(".")
    real = generate_signal_population(minute, event)
    shifted_minute, shifted_event = _shift_flow_one_prior_session(minute, event)

    shifted = generate_signal_population(shifted_minute, shifted_event)

    assert set(shifted) == set(real)
    assert any(len(shifted[key]) != len(real[key]) for key in real)
    assert sum(len(rows) for rows in shifted.values()) > 0.50 * sum(
        len(rows) for rows in real.values()
    )


def test_mes_micro_equivalent_is_valid_at_one_contract() -> None:
    assert MINI_EQUIVALENT["MES"] == 0.1
    assert _d1_event_compliance((), "MES")


def test_promotion_gates_exclude_combine_pass_rate() -> None:
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
    assert len(candidate_specs()) == 8


def test_five_session_null_drops_compressed_overlaps_before_replay() -> None:
    minute, event = load_feature_store(".")
    payload = json.loads(
        Path(
            "reports/v7/data/d1_microstructure_grammar0001_signal_manifest.json"
        ).read_text(encoding="utf-8")
    )
    signals = {
        candidate_id: tuple(D1Signal(**row) for row in rows)
        for candidate_id, rows in payload["signals"].items()
    }
    delayed = _delay_signals_five_sessions(signals, minute, event)
    specs = {row.candidate_id: row for row in candidate_specs()}

    events = build_candidate_events(
        minute,
        event,
        delayed,
        specs,
        load_cost_model(),
        stress=CostStress.STRESS_1_5X,
    )

    assert set(events) == set(signals)
    assert all(len(delayed[key]) <= len(signals[key]) for key in signals)


def test_frozen_reservation_remains_valid_after_later_trial_reservations() -> None:
    _verify_inputs(
        Path(".").resolve(),
        {
            "grammar": Path(
                "WORM/v7-d1-microstructure-grammar-0001-2026-07-12.json"
            ).resolve(),
            "tripwire policy": Path(
                "WORM/v7-d1-new-dataset-tripwire-2026-07-12.json"
            ).resolve(),
            "validation policy": Path(
                "WORM/v7-d1-microstructure-validation-policy-2026-07-12.json"
            ).resolve(),
            "execution addendum": Path(
                "WORM/v7-d1-microstructure-execution-addendum-2026-07-12.json"
            ).resolve(),
            "signal manifest": Path(
                "reports/v7/data/d1_microstructure_grammar0001_signal_manifest.json"
            ).resolve(),
            "tripwire result": Path(
                "reports/v7/data/d1_new_dataset_tripwire_result.json"
            ).resolve(),
        },
        "/root/hydra-bot/mission/state/proof_registry.json",
    )
