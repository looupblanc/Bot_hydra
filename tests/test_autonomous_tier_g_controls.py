from __future__ import annotations

from dataclasses import replace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_tier_g_controls as controls
from hydra.production.autonomous_combine_candidate_bank import (
    _verify_generic_concentration_receipt,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)


def _trajectory(
    index: int,
    net: float,
    *,
    scenario: str = "NORMAL",
    day: int | None = None,
) -> CausalTradeTrajectory:
    decision = 1_000_000 + index * 100_000
    exit_time = decision + 50_000
    resolved_day = int(day if day is not None else 100 + index)
    event = TradePathEvent(
        event_id=f"candidate:event_{index}:{scenario}",
        decision_ns=decision,
        exit_ns=exit_time,
        session_day=resolved_day,
        net_pnl=float(net),
        gross_pnl=float(net + 5.0),
        worst_unrealized_pnl=float(min(net, -20.0)),
        best_unrealized_pnl=float(max(net, 20.0)),
        quantity=1,
        mini_equivalent=0.5,
    )
    return CausalTradeTrajectory(
        component_id="candidate",
        market="MYM",
        side=1,
        event=event,
        marks=(
            CausalTradeMark(
                availability_time_ns=decision + 25_000,
                worst_unrealized_pnl=float(min(net / 2.0, -10.0)),
                best_unrealized_pnl=float(max(net / 2.0, 10.0)),
                current_unrealized_pnl=float(net / 4.0),
            ),
            CausalTradeMark(
                availability_time_ns=exit_time,
                worst_unrealized_pnl=float(min(net, -20.0)),
                best_unrealized_pnl=float(max(net, 20.0)),
                current_unrealized_pnl=float(net),
            ),
        ),
    )


def _classified(
    candidate_id: str,
    *,
    passes: int = 2,
    contexts: int = 2,
    block_share: float = 0.5,
    tier_q: bool = True,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "tier_q_contract_cleared": tier_q,
        "computed_development_tier": "Q" if tier_q else "E",
        "authoritative_promotion_status": None,
        "best_safe_cell": {
            "normal": {"pass_count": passes},
            "stressed": {"pass_count": passes},
        },
        "concentration_diagnostic": {
            "positive_pass_context_count": contexts,
            "maximum_block_pass_share": block_share,
        },
    }


def test_candidate_selection_requires_multiple_passes_and_block_diversity() -> None:
    bank = {
        "candidates": [
            _classified("qualified-b"),
            _classified("qualified-a", passes=3, contexts=4, block_share=0.25),
            _classified("one-pass", passes=1),
            _classified("one-block", contexts=1, block_share=1.0),
            _classified("not-q", tier_q=False),
        ]
    }
    selected = controls._select_control_candidates(bank)
    assert [row["candidate_id"] for row in selected] == [
        "qualified-a",
        "qualified-b",
    ]


def test_unique_ledger_concentration_never_counts_rolling_starts() -> None:
    normal = tuple(
        _trajectory(index, net, day=day)
        for index, (net, day) in enumerate(
            ((40.0, 100), (30.0, 100), (20.0, 101), (10.0, 102))
        )
    )
    stressed = tuple(
        replace(row, event=replace(row.event, event_id=row.event.event_id.replace(
            ":NORMAL", ":STRESSED_1_5X"
        )))
        for row in normal
    )
    result = controls._unique_ledger_concentration(normal, stressed)
    assert result["rolling_start_multiplicity"] == 0
    assert result["normal"]["record_count"] == 4
    assert result["normal"]["maximum_single_day_profit_share"] == pytest.approx(
        0.70
    )
    assert result["normal"]["maximum_single_trade_profit_share"] == pytest.approx(
        0.40
    )
    assert result["normal"]["maximum_single_event_profit_share"] == pytest.approx(
        0.40
    )
    assert result["cleared"] is False


def test_circular_shift_is_deterministic_count_and_exposure_matched() -> None:
    source = tuple(_trajectory(index, float(index + 1) * 10.0) for index in range(8))
    shifted = controls._circular_shift_trajectories(
        source,
        offset=3,
        control_id="SHIFT_TEST",
        scenario="NORMAL",
    )
    repeated = controls._circular_shift_trajectories(
        source,
        offset=3,
        control_id="SHIFT_TEST",
        scenario="NORMAL",
    )
    assert controls._exposure_count_match(source, shifted) is True
    assert stable_hash([row.to_dict() for row in shifted]) == stable_hash(
        [row.to_dict() for row in repeated]
    )
    assert shifted[0].event.net_pnl == source[3].event.net_pnl
    assert shifted[0].event.decision_ns == source[0].event.decision_ns
    assert shifted[0].event.exit_ns == source[0].event.exit_ns
    assert shifted[0].event.event_id.endswith(":SHIFT_TEST:NORMAL")


def test_control_offsets_are_bounded_distinct_and_fingerprint_deterministic() -> None:
    first = controls._control_offsets(100, "1" * 64)
    second = controls._control_offsets(100, "1" * 64)
    assert first == second
    assert len(first) == controls.CONTROL_SHIFT_COUNT
    assert len(set(first)) == len(first)
    assert all(0 < value < 100 for value in first)


def test_concentration_receipt_is_self_hashed_and_final_development_only() -> None:
    concentration = {
        "worst_case_maximums": {
            "maximum_single_day_profit_share": 0.2,
            "maximum_single_trade_profit_share": 0.2,
            "maximum_single_event_profit_share": 0.2,
        }
    }
    receipt = controls._concentration_receipt(
        candidate_id="candidate",
        source_exact_result_hash="a" * 64,
        concentration=concentration,
        final_development_evidence_hash="b" * 64,
    )
    claimed = receipt.pop("receipt_hash")
    assert stable_hash(receipt) == claimed
    assert receipt["rolling_start_multiplicity"] == 0
    assert receipt["evidence_role"] == "VIEWED_FINAL_DEVELOPMENT_ONLY"
    compatible = {**receipt, "receipt_hash": claimed}
    verified = _verify_generic_concentration_receipt(
        compatible,
        candidate_id="candidate",
        source_exact_result_hash="a" * 64,
    )
    assert verified["cleared"] is True


def _fake_candidate(candidate_id: str, ready: bool) -> dict[str, object]:
    receipt = controls._concentration_receipt(
        candidate_id=candidate_id,
        source_exact_result_hash="a" * 64,
        concentration={
            "worst_case_maximums": {
                "maximum_single_day_profit_share": 0.2,
                "maximum_single_trade_profit_share": 0.1,
                "maximum_single_event_profit_share": 0.1,
            }
        },
        final_development_evidence_hash="b" * 64,
    )
    return {
        "candidate_id": candidate_id,
        "g_control_ready": ready,
        "concentration_receipt": receipt,
    }


def _fake_shard(index: int, selected_ids: list[str]) -> dict[str, object]:
    inventory = [f"candidate-{value}" for value in range(5)]
    results = [_fake_candidate(candidate_id, candidate_id.endswith("0")) for candidate_id in selected_ids]
    core: dict[str, object] = {
        "schema": controls.SCHEMA,
        "status": "COMPLETE_READ_ONLY_TIER_G_CONTROL_SHARD",
        "source_candidate_bank_hash": "bank",
        "source_exact_composite_hash": "exact",
        "source_manifest_hash": "manifest",
        "frozen_grid": {"hash": "grid"},
        "official_rule_snapshot": {"hash": "rules"},
        "source_bank_receipt": {"hash": "source"},
        "control_contract": {"version": 1},
        "shard": {
            "shard_index": index,
            "shard_count": 2,
            "candidate_inventory_ids": inventory,
            "candidate_inventory_hash": stable_hash(inventory),
            "selected_candidate_ids": selected_ids,
        },
        "candidate_results": results,
        "concentration_receipts": {
            row["candidate_id"]: row["concentration_receipt"] for row in results
        },
        "counts": {
            "exact_account_replay_count": len(results) * 8,
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
    }
    return {**core, "result_hash": stable_hash(core)}


def test_two_shards_compose_without_promotion_or_xfa() -> None:
    shard0 = _fake_shard(0, ["candidate-0", "candidate-2", "candidate-4"])
    shard1 = _fake_shard(1, ["candidate-1", "candidate-3"])
    result = controls.compose_autonomous_tier_g_control_shards([shard1, shard0])
    assert result["schema"] == controls.COMPOSITE_SCHEMA
    assert result["counts"]["selected_candidate_count"] == 5
    assert result["counts"]["authoritative_promotion_count"] == 0
    assert result["counts"]["xfa_paths_started"] == 0
    assert result["counts"]["orders"] == 0
    assert result["candidate_ids"]["g_control_ready"] == ["candidate-0"]


def test_compose_fails_closed_on_authoritative_side_effect() -> None:
    shard = _fake_shard(0, [f"candidate-{value}" for value in range(5)])
    shard["shard"] = {**shard["shard"], "shard_count": 1}
    shard["counts"] = {**shard["counts"], "orders": 1}
    shard["result_hash"] = stable_hash(
        {key: value for key, value in shard.items() if key != "result_hash"}
    )
    with pytest.raises(
        controls.AutonomousTierGControlsError,
        match="read-only Tier-G control invariant failed",
    ):
        controls.compose_autonomous_tier_g_control_shards([shard])
