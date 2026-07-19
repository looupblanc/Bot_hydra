from __future__ import annotations

from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import frozen_legal_frontier_replay as exact


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_SOURCE = ROOT / exact.DEFAULT_LEGAL_FEASIBILITY_PATH


@pytest.fixture(scope="module")
def result() -> dict:
    if not REQUIRED_SOURCE.is_file():
        pytest.skip("immutable 0029/0035 economic artifacts are not installed")
    first = exact.run_frozen_legal_frontier_exact_replay(ROOT)
    second = exact.run_frozen_legal_frontier_exact_replay(ROOT)
    assert first == second
    return first


def test_frozen_cell_contract_is_exactly_the_preregistered_shortlist() -> None:
    observed = [
        (
            row.policy_id,
            row.selection_horizon_trading_days,
            row.uniform_quantity_scale,
        )
        for row in exact.FROZEN_CELLS
    ]
    assert observed == [
        ("fast_book_477b40613795d1d45557a83a:w1", 20, 3),
        ("fast_book_7bbfd61968d4122c8bb75f64:w1", 10, 6),
        ("fast_book_bfc9dcbdfb4e4d134b46f0e6:w1", 10, 8),
    ]


def test_real_exact_replay_reconciles_sources_and_falsifies_summary_passes(
    result: dict,
) -> None:
    assert result["schema"] == exact.SCHEMA
    assert result["status"] == "COMPLETE_EXACT_FROZEN_LEGAL_FRONTIER_REPLAY"
    core = dict(result)
    claimed = core.pop("result_hash")
    assert stable_hash(core) == claimed
    assert result["decision"] == (
        "SUMMARY_LEGAL_FRONTIER_PASS_SIGNAL_NOT_CONFIRMED_EXACTLY"
    )
    assert result["counters"] == {
        "frozen_cell_count": 3,
        "unique_component_count": 5,
        "source_event_rows_reconstructed": 1633,
        "exact_account_replays": 262,
        "diagnostic_exact_passes_all_horizons_and_scenarios": 4,
        "admissible_exact_passes_all_horizons_and_scenarios": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_writes": 0,
    }

    by_source = {row["source_policy_id"]: row for row in result["results"]}
    first = by_source["fast_book_477b40613795d1d45557a83a:w1"]
    assert first["horizon_results"]["20"]["normal"]["pass_count"] == 2
    assert first["horizon_results"]["20"]["stressed"]["pass_count"] == 2
    assert first["hard_execution_contract_clean"] is False
    assert sum(first["session_contract_violation_count_by_component"].values()) == 8
    assert first["selection_horizon_summary_delta"]["NORMAL"][
        "screened_pass_count"
    ] == 3

    second = by_source["fast_book_7bbfd61968d4122c8bb75f64:w1"]
    assert second["hard_execution_contract_clean"] is True
    assert second["horizon_results"]["10"]["normal"]["pass_count"] == 0
    assert second["horizon_results"]["10"]["stressed"]["pass_count"] == 0
    assert second["selection_horizon_summary_delta"]["STRESSED_1_5X"][
        "screened_pass_count"
    ] == 4

    third = by_source["fast_book_bfc9dcbdfb4e4d134b46f0e6:w1"]
    assert third["hard_execution_contract_clean"] is False
    assert sum(
        third["official_market_contract_cap_breach_count_by_component"].values()
    ) == 7
    assert third["horizon_results"]["10"]["normal"]["pass_count"] == 0
    assert third["horizon_results"]["10"]["stressed"]["pass_count"] == 0


def test_output_is_evidence_bundle_adapter_ready_and_fail_closed(result: dict) -> None:
    adapter = result["evidence_bundle_adapter"]
    assert adapter["evidence_role"] == "VIEWED_DEVELOPMENT_ONLY"
    assert adapter["sealing_performed"] is False
    assert adapter["authoritative_writer_required_for_sealing"] is True
    assert len(adapter["policies"]) == 3
    assert len(adapter["component_fingerprints"]) == 5
    assert len(adapter["evaluated_policy_records"]) == 262
    payload = dict(adapter)
    claimed = payload.pop("adapter_payload_hash")
    assert stable_hash(payload) == claimed
    assert all(
        policy["outbound_order_capability"] is False
        for policy in adapter["policies"].values()
    )
    assert all(row["promotion_status"] is None for row in result["results"])


def test_frontier_anchor_rejects_a_neighbouring_or_missing_cell() -> None:
    frozen = exact.FROZEN_CELLS[0]
    legal = {
        "uniform_legal_frontier": [
            {
                "policy_id": frozen.policy_id,
                "account_size_usd": 50_000,
                "horizon_trading_days": 20,
                "scale_factor": 4.0,
                "scenario": scenario,
                "legally_executable": True,
            }
            for scenario in exact.SCENARIOS
        ]
    }
    with pytest.raises(
        exact.FrozenLegalFrontierReplayError,
        match="absent or duplicated",
    ):
        exact._legal_frontier_anchor(legal, frozen)
