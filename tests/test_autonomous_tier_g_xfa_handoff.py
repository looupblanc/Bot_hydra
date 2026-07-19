from __future__ import annotations

import copy

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_tier_g_xfa_handoff as handoff
from hydra.production.autonomous_exact_replay import _account_config, _standalone_policy
from hydra.production.autonomous_exact_replay import _summarize_exact_episodes
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)


CANDIDATE = "hazard-test"
RULE_HASH = "6" * 64


def _trajectory(day: int, scenario: str) -> CausalTradeTrajectory:
    decision = day * 1_000_000
    exit_time = decision + 100_000
    net = 1_500.0
    event = TradePathEvent(
        event_id=f"{CANDIDATE}:{day}:{scenario}",
        decision_ns=decision,
        exit_ns=exit_time,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 5.0,
        worst_unrealized_pnl=-50.0,
        best_unrealized_pnl=net + 25.0,
        quantity=1,
        mini_equivalent=0.1,
    )
    return CausalTradeTrajectory(
        component_id=CANDIDATE,
        market="MYM",
        side=1,
        event=event,
        marks=(
            CausalTradeMark(
                availability_time_ns=decision + 50_000,
                worst_unrealized_pnl=-50.0,
                best_unrealized_pnl=750.0,
                current_unrealized_pnl=750.0,
            ),
            CausalTradeMark(
                availability_time_ns=exit_time,
                worst_unrealized_pnl=-50.0,
                best_unrealized_pnl=net + 25.0,
                current_unrealized_pnl=net,
            ),
        ),
    )


def _fixture(*, post_pass_days: int = 3) -> tuple[dict, dict, dict]:
    rule = {
        "account_label": "50K",
        "account_size_usd": 50_000,
        "profit_target_usd": 3_000,
        "maximum_loss_limit_usd": 2_000,
        "maximum_mini_contracts": 5,
        "maximum_micro_contracts": 50,
        "consistency_target_fraction": 0.5,
        "minimum_trading_days": 2,
        "optional_daily_loss_limit_usd": 1_000,
    }
    policy = _standalone_policy(
        CANDIDATE,
        rule,
        tier=1,
        declared_risk_charge_per_mini=1.0,
        account_contract_limit=5.0,
        governor_mode="CONTRACT_ONLY_UNIFORM_SCALE",
    )
    config = _account_config(rule)
    calendar = tuple(range(100, 102 + post_pass_days))
    normal = tuple(_trajectory(day, "NORMAL") for day in calendar)
    stressed = tuple(_trajectory(day, "STRESSED") for day in calendar)
    starts = ((100, "B1"),)

    summaries = {}
    for key, trajectories in (("normal", normal), ("stressed", stressed)):
        episode = run_causal_shared_account_episode(
            {CANDIDATE: trajectories},
            calendar,
            policy=policy,
            start_day=100,
            maximum_duration_days=2,
            config=config,
        )
        assert episode.passed
        summaries[key] = _summarize_exact_episodes(((episode, "B1"),))
    selected_cell = {
        "normal": summaries["normal"],
        "stressed": summaries["stressed"],
    }
    selected_cell_hash = stable_hash({"selected": "cell"})
    frozen_policy = policy.to_dict()
    frozen_policy_hash = stable_hash(frozen_policy)
    prepared = {
        "candidate_id": CANDIDATE,
        "candidate_fingerprint": "1" * 64,
        "source_exact_result_hash": "2" * 64,
        "selected_cell": selected_cell,
        "selected_cell_hash": selected_cell_hash,
        "account_label": "50K",
        "account_size_usd": 50_000,
        "frozen_account_policy": frozen_policy,
        "frozen_account_policy_hash": frozen_policy_hash,
        "official_rule_snapshot_hash": RULE_HASH,
        "source_event_receipt": {
            "relative_path": "events/test.jsonl.gz",
            "record_count": len(calendar),
            "sha256": "3" * 64,
            "uncompressed_sha256": "4" * 64,
        },
        "calendar": calendar,
        "starts": starts,
        "horizon": 2,
        "normal": normal,
        "stressed": stressed,
        "policy": policy,
        "config": config,
    }
    combine_core = {
        "schema": "hydra_single_sleeve_complete_account_policy_v1",
        "candidate_id": CANDIDATE,
        "account_label": "50K",
        "account_size_usd": 50_000,
        "frozen_account_policy_hash": frozen_policy_hash,
    }
    combine_hash = stable_hash(combine_core)
    combine_book = {**combine_core, "combine_book_hash": combine_hash}
    receipt = {
        "candidate_id": CANDIDATE,
        "graduation_status": "GRADUATED_DEVELOPMENT_BOOK",
        "account_label": "50K",
        "account_size_usd": 50_000,
        "selected_cell_hash": selected_cell_hash,
        "frozen_account_policy_hash": frozen_policy_hash,
        "source_exact_result_hash": "2" * 64,
        "combine_book": combine_book,
        "combine_book_hash": combine_hash,
        "normal_economics": {
            "pass_count": 1,
            "episode_path_hash": summaries["normal"]["episode_path_hash"],
        },
        "stressed_economics": {
            "pass_count": 1,
            "episode_path_hash": summaries["stressed"]["episode_path_hash"],
        },
    }
    graduation = {
        "result_hash": "5" * 64,
        "source_candidate_bank_hash": "7" * 64,
        "source_exact_composite_hash": "8" * 64,
        "graduated_development_books": [receipt],
        "candidate_ids": {"graduated_development_books": [CANDIDATE]},
    }
    rule_snapshot = {
        "xfa": {
            "starting_balance_usd": 0,
            "starting_mll_by_account_usd": {"50K": -2_000},
            "mll_floor_after_first_payout_usd": 0,
            "profit_split_trader_fraction": 0.9,
            "scaling_plan_mini_contracts": {
                "50K": [[0, 2], [1_500, 3], [2_000, 5]]
            },
        }
    }
    return prepared, graduation, rule_snapshot


def _build(monkeypatch: pytest.MonkeyPatch, *, post_pass_days: int = 3) -> dict:
    prepared, graduation, rule_snapshot = _fixture(
        post_pass_days=post_pass_days
    )
    monkeypatch.setattr(
        handoff, "verify_tier_g_development_graduation", lambda _value: None
    )
    return handoff.build_tier_g_handoffs_from_prepared(
        [prepared],
        graduation,
        rule_snapshot=rule_snapshot,
        source_manifest_hash="9" * 64,
        frozen_grid={"grid_hash": "a" * 64},
        official_rule_snapshot={"parsed_rule_hash": RULE_HASH},
        source_bank_receipt={"bank_hash": "b" * 64},
        source_exact_composite_hash="8" * 64,
    )


def test_successful_combine_paths_get_exact_post_pass_causal_handoffs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(monkeypatch, post_pass_days=3)
    assert result["counts"]["successful_combine_transition_count"] == 2
    assert result["counts"]["ready_xfa_transition_count"] == 2
    assert result["counts"]["xfa_simulations_started"] == 0
    assert result["combine_outcomes_modified"] is False
    for transition in result["transitions"]:
        assert transition["combine_pass_day"] == 101
        assert transition["xfa_start_day"] == 102
        assert transition["post_pass_completed_trajectory_count"] == 3
        assert transition["fresh_xfa_balance_usd"] == 0.0
        materialized = handoff.materialize_transition_trajectories(
            result, transition["transition_id"]
        )
        assert len(materialized[CANDIDATE]) == 3
        assert all(row.event.session_day > 101 for row in materialized[CANDIDATE])


def test_pass_on_last_eligible_day_fails_closed_without_rewriting_combine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(monkeypatch, post_pass_days=0)
    assert result["counts"]["successful_combine_transition_count"] == 2
    assert result["counts"]["ready_xfa_transition_count"] == 0
    assert result["counts"]["fail_closed_transition_count"] == 2
    assert result["transitions"] == []
    assert all(
        row["failure_reason"] == "NO_ELIGIBLE_SESSION_DAY_AFTER_COMBINE_PASS"
        for row in result["fail_closed_transitions"]
    )


def test_handoff_hash_and_post_pass_slice_tampering_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(monkeypatch)
    tampered = copy.deepcopy(result)
    tampered["source_tapes"][f"{CANDIDATE}:NORMAL"][
        "completed_trajectories"
    ][-1]["event"]["net_pnl"] += 1.0
    tampered["result_hash"] = stable_hash(
        {key: value for key, value in tampered.items() if key != "result_hash"}
    )
    with pytest.raises(
        handoff.AutonomousTierGXfaHandoffError, match="source-tape identity/hash drift"
    ):
        handoff.verify_tier_g_combine_xfa_handoffs(tampered)


def test_xfa_profile_is_frozen_separately_from_combine_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _build(monkeypatch)
    candidate = result["candidate_handoffs"][0]
    profile = candidate["xfa_profile"]
    book = candidate["xfa_book"]
    assert profile["source_combine_book_hash"] == book["source_combine_book_hash"]
    assert profile["risk_multiplier"] == 1.0
    assert profile["scaling_plan_mini_contracts"] == [
        [0.0, 2.0],
        [1500.0, 3.0],
        [2000.0, 5.0],
    ]
    assert profile["standard_and_consistency_are_alternative_paths"] is True
    assert book["fresh_xfa_starts_at_zero"] is True
    assert result["graduation_receipts_modified"] is False
