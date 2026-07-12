from __future__ import annotations

from copy import deepcopy

from hydra.validation.v7_frozen_grammar_tribunal import _promotion_gates


def test_promotion_gates_require_economic_null_dsr_and_bh_evidence() -> None:
    row = _passing_row()

    gates = _promotion_gates(row)

    assert all(gates.values())
    assert not any("combine" in key.lower() for key in gates)
    assert "candidate_null_suite_passed" in gates
    assert "DSR_deflated_z_gt_0" in gates
    assert "BH_FDR_10pct_rejected" in gates


def test_candidate_null_failure_cannot_be_rescued_by_combine_diagnostic() -> None:
    row = _passing_row()
    row["candidate_null_suite"]["passed"] = False
    row["combine_diagnostic_not_fitness"] = {"pass_rate": 1.0}

    gates = _promotion_gates(row)

    assert not gates["candidate_null_suite_passed"]
    assert not all(gates.values())


def test_sim_exploit_is_an_irreversible_hard_gate() -> None:
    row = _passing_row()
    row["SIM_EXPLOIT"] = True

    assert not _promotion_gates(row)["SIM_EXPLOIT_2x_survived"]


def _passing_row() -> dict[str, object]:
    return deepcopy(
        {
            "stage1_pass": True,
            "stress_1_5x": {"expectancy_per_trade": 1.0},
            "SIM_EXPLOIT": False,
            "trajectory_compliance": True,
            "walk_forward": {"pooled_expectancy_per_trade": 1.0},
            "candidate_null_suite": {"passed": True},
            "DSR": {"deflated_z": 0.1},
            "BH": {"rejected": True},
        }
    )
