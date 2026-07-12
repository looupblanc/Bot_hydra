from __future__ import annotations

import numpy as np

from hydra.research.v7_d1_microstructure_grammar import (
    candidate_specs,
    generate_signal_population,
    load_feature_store,
)
from hydra.validation.v7_d1_candidate_tribunal import (
    _d1_event_compliance,
    _promotion_gates,
    _shift_flow_one_prior_session,
)
from hydra.validation.v7_d1_new_dataset_tripwire import MINI_EQUIVALENT


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


def test_shifted_flow_recomputes_frozen_signal_population() -> None:
    minute, event = load_feature_store(".")
    real = generate_signal_population(minute, event)
    shifted_minute, shifted_event = _shift_flow_one_prior_session(minute, event)

    shifted = generate_signal_population(shifted_minute, shifted_event)

    assert set(shifted) == set(real)
    assert any(len(shifted[key]) != len(real[key]) for key in real)


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
