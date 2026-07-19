from __future__ import annotations

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.fresh_confirmation_lane import (
    ADDITIONAL_AUTHORITY_USD,
    BREADTH_SPECIFICATION_ID,
    CUMULATIVE_HARD_CAP_USD,
    FROZEN_ESTIMATED_REQUEST_COST_USD,
    FreshConfirmationError,
    frozen_data_request,
    non_overlapping_starts,
    tier_c_gate,
    validate_acquisition_receipt,
)


def _contract() -> dict:
    core = {
        "schema": "hydra_fresh_confirmation_contract_v1",
        "data_request": frozen_data_request(),
    }
    return {**core, "contract_hash": stable_hash(core)}


def test_request_is_exact_and_budget_is_additional_100() -> None:
    request = frozen_data_request()

    assert request["dataset"] == "GLBX.MDP3"
    assert request["schema"] == "ohlcv-1m"
    assert request["symbols"] == ["YM.c.0", "MYM.c.0", "ES.c.0"]
    assert request["start"] == "2025-01-02"
    assert request["end"] == "2025-07-01"
    assert request["data_role"] == "CONFIRMATION"
    assert request["frozen_estimated_cost_usd"] == FROZEN_ESTIMATED_REQUEST_COST_USD
    assert request["additional_authority_usd"] == ADDITIONAL_AUTHORITY_USD
    assert request["cumulative_hard_cap_usd"] == CUMULATIVE_HARD_CAP_USD
    assert request["q4_2024_access_allowed"] is False


def test_non_overlapping_windows_are_complete_and_deterministic() -> None:
    grid = non_overlapping_starts(tuple(range(100, 145)), (5, 10, 20))

    assert [row[0] for row in grid[5]] == list(range(100, 145, 5))
    assert [row[0] for row in grid[10]] == [100, 110, 120, 130]
    assert [row[0] for row in grid[20]] == [100, 120]
    assert all(row[1] == "CONFIRMATION" for values in grid.values() for row in values)


def test_tier_c_gate_requires_same_frozen_20_day_normal_and_stressed_pass() -> None:
    scenario = {
        "pass_count": 1,
        "net_total_usd": 100.0,
        "mll_breach_rate": 0.0,
        "all_passing_paths_consistency_compliant": True,
    }
    cells = [{
        "horizon_trading_days": 20,
        "full_coverage_start_count": 3,
        "normal": dict(scenario),
        "stressed": dict(scenario),
    }]

    assert tier_c_gate(cells, confirmation_eligible=True)["passed"] is True
    assert tier_c_gate(cells, confirmation_eligible=False)["passed"] is False
    cells[0]["stressed"]["pass_count"] = 0
    assert tier_c_gate(cells, confirmation_eligible=True)["passed"] is False


def test_acquisition_receipt_must_match_request_and_authority() -> None:
    request = frozen_data_request()
    receipt = {
        "request": {key: request[key] for key in (
            "dataset", "schema", "symbols", "stype_in", "start", "end"
        )},
        "actual_cost_usd": 1.80,
        "cumulative_actual_usd": 102.52,
        "files": [{"path": "confirmation.parquet", "sha256": "a" * 64}],
    }

    assert validate_acquisition_receipt(_contract(), receipt)["status"] == (
        "ACQUISITION_RECEIPT_RECONCILED"
    )
    receipt["request"]["end"] = "2025-08-01"
    with pytest.raises(FreshConfirmationError, match="differs from freeze"):
        validate_acquisition_receipt(_contract(), receipt)


def test_breadth_identifier_remains_exact_three_peer_diagnostic() -> None:
    assert BREADTH_SPECIFICATION_ID == (
        "breadth:YM:OPEN:BREADTH_CONFIRMED_CONTINUATION"
    )
