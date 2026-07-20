from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.portfolio.marginal_contribution_builder import GovernorProfile
from hydra.production import autonomous_marginal_combine_books as books
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.causal_sleeve_replay import CausalTradeMark


DAY = 20_000


def test_bounded_batch_is_deterministic_tier_q_only_and_account_homogeneous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank = _candidate_bank(
        [
            _classified("q-a", account="50K", tier_q=True),
            _classified("q-b", account="50K", tier_q=True),
            _classified("q-c", account="100K", tier_q=True),
            _classified("e-only", account="50K", tier_q=False),
        ]
    )
    composite = {"result_hash": bank["source_composite_result_hash"]}
    context = _context(
        {
            "q-a": _component("q-a", account="50K"),
            "q-b": _component("q-b", account="50K"),
            "q-c": _component("q-c", account="100K"),
        }
    )
    monkeypatch.setattr(
        books,
        "_verified_exact_results",
        lambda _initial, _continuations: (composite, ()),
    )
    monkeypatch.setattr(books, "_prepare_replay_context", lambda *args, **kwargs: context)
    monkeypatch.setattr(books, "_evaluate_policy_spec", _fake_policy_result)

    first = books.build_autonomous_marginal_combine_books(
        ".", bank, {}, requested_book_count=16
    )
    second = books.build_autonomous_marginal_combine_books(
        ".", bank, {}, requested_book_count=16
    )

    assert first == second
    assert first["result_hash"] == stable_hash(
        {key: value for key, value in first.items() if key != "result_hash"}
    )
    assert first["tier_q_component_ids"] == ["q-a", "q-b", "q-c"]
    assert "e-only" not in str(first["proposals"])
    assert first["counts"]["book_proposal_count"] == 4
    assert first["counts"]["standalone_g_ready_count"] == 3
    assert first["counts"]["g_ready_count"] == 4
    assert first["counts"]["authoritative_promotion_count"] == 0
    assert first["counts"]["xfa_paths_started"] == 0
    for row in first["book_results"]:
        assert row["account_label"] == "50K"
        assert 2 <= len(row["component_ids"]) <= 6
        assert row["additional_quantity_scaling"] is False
        assert row["router_static_risk_tier"] == 1.0
        assert row["marginally_accepted"] is True
        assert row["authoritative_promotion_status"] is None


def test_g_ready_reads_heldout_not_design() -> None:
    row = _fake_policy_result(
        {
            "policy_id": "book",
            "policy_spec_hash": "spec",
            "policy_role": "MARGINAL_COMBINE_BOOK_CANDIDATE",
            "account_label": "50K",
            "component_ids": ["a", "b"],
            "component_quantity_tiers": {"a": 2, "b": 3},
            "governor_profile": _profile().__dict__,
        },
        None,
    )
    compact = books._compact_policy_result(row)
    compact["marginally_accepted"] = True
    assert all(books._g_ready_gates(compact).values())

    # Poison only B3/B4.  Strong B1/B2 design evidence must not rescue the gate.
    heldout = compact["summaries_by_role"]["HELD_OUT_DEVELOPMENT"]
    heldout["STRESSED_1_5X"]["5"]["pass_rate"] = 0.0
    heldout["STRESSED_1_5X"]["5"]["pass_count"] = 0
    assert books._g_ready_gates(compact)[
        "stressed_5d_pass_rate_at_least_2pct"
    ] is False


def test_identity_profile_never_substitutes_epsilon_stop_risk() -> None:
    component = _component("contract-only", account="50K")
    component = replace(
        component,
        source_governor_mode="CONTRACT_ONLY_UNIFORM_SCALE",
        declared_risk_charge_per_mini=463.02,
    )
    context = _context({"contract-only": component})
    spec = books._policy_spec(
        account_label="50K",
        members=("contract-only",),
        components=context.components,
        profile=books._identity_profile(),
        policy_role="STANDALONE",
        predecessor_policy_id=None,
    )

    policy = books._active_policy(spec, context)

    assert dict(policy.nominal_risk_charge_per_mini)["contract-only"] == pytest.approx(
        463.02
    )


def test_g_gate_uses_passing_path_consistency_not_timeout_ratio() -> None:
    passed = _summary_episode(
        passed=True,
        consistency_ok=True,
        best_day_concentration=0.50,
        net_pnl=3_000.0,
        daily_pnl=((21_000, 1_500.0), (21_001, 1_500.0)),
    )
    # Topstep expands the target for this unfinished path.  Its near-zero net
    # makes best_day/net enormous, but it is not a failed consistency rule and
    # cannot invalidate the separate, legally compliant pass above.
    timeout = _summary_episode(
        passed=False,
        consistency_ok=False,
        best_day_concentration=1_000.0,
        net_pnl=1.0,
        daily_pnl=((21_002, 1_000.0), (21_003, -999.0)),
    )
    summary = books._summarize_sprint_episodes(
        ((passed, "B3"), (timeout, "B4")),
        requested_start_count=2,
        data_censored_count=0,
    )

    assert summary["consistency_rate"] == pytest.approx(0.50)
    assert summary["passing_consistency_rate"] == pytest.approx(1.0)
    assert summary["all_passing_paths_consistency_compliant"] is True
    assert summary["best_day_concentration_max"] == pytest.approx(1_000.0)
    assert summary["maximum_positive_session_day_aggregate_share"] == pytest.approx(
        1_500.0 / 4_000.0
    )

    row = _fake_policy_result(
        {
            "policy_id": "timeout-safe",
            "policy_spec_hash": "spec",
            "policy_role": "MARGINAL_COMBINE_BOOK_CANDIDATE",
            "account_label": "50K",
            "component_ids": ["a", "b"],
            "component_quantity_tiers": {"a": 2, "b": 2},
            "governor_profile": _profile().__dict__,
        },
        None,
    )
    compact = books._compact_policy_result(row)
    compact["marginally_accepted"] = True
    for scenario in books.SCENARIOS:
        compact["summaries_by_role"]["HELD_OUT_DEVELOPMENT"][scenario]["5"] = (
            deepcopy(summary)
        )
    gates = books._g_ready_gates(compact)

    assert gates["all_passing_paths_consistency_compliant"] is True
    assert gates[
        "daily_concentration_deferred_to_authoritative_unique_ledger_control"
    ] is True
    assert gates["trade_concentration_deferred_to_authoritative_control"] is True


def test_g_gate_does_not_treat_overlapping_session_day_summary_as_authoritative() -> None:
    passed = _summary_episode(
        passed=True,
        consistency_ok=True,
        best_day_concentration=0.50,
        net_pnl=3_000.0,
        daily_pnl=((21_000, 1_500.0), (21_001, 1_500.0)),
    )
    timeout = _summary_episode(
        passed=False,
        consistency_ok=False,
        best_day_concentration=2_000.0,
        net_pnl=1.0,
        # The same session day is economically dominant after rolling paths
        # are grouped by their real session date.
        daily_pnl=((21_000, 2_000.0), (21_002, -1_999.0)),
    )
    summary = books._summarize_sprint_episodes(
        ((passed, "B3"), (timeout, "B4")),
        requested_start_count=2,
        data_censored_count=0,
    )

    assert summary["all_passing_paths_consistency_compliant"] is True
    assert summary["maximum_positive_session_day_aggregate_share"] == pytest.approx(
        3_500.0 / 5_000.0
    )

    row = _fake_policy_result(
        {
            "policy_id": "day-dominated",
            "policy_spec_hash": "spec",
            "policy_role": "MARGINAL_COMBINE_BOOK_CANDIDATE",
            "account_label": "50K",
            "component_ids": ["a", "b"],
            "component_quantity_tiers": {"a": 2, "b": 2},
            "governor_profile": _profile().__dict__,
        },
        None,
    )
    compact = books._compact_policy_result(row)
    compact["marginally_accepted"] = True
    for scenario in books.SCENARIOS:
        compact["summaries_by_role"]["HELD_OUT_DEVELOPMENT"][scenario]["5"] = (
            deepcopy(summary)
        )
    gates = books._g_ready_gates(compact)

    assert gates["all_passing_paths_consistency_compliant"] is True
    assert summary["maximum_positive_session_day_aggregate_share"] > 0.50
    assert gates[
        "daily_concentration_deferred_to_authoritative_unique_ledger_control"
    ] is True


def test_passing_path_consistency_invariant_fails_closed() -> None:
    impossible_pass = _summary_episode(
        passed=True,
        consistency_ok=False,
        best_day_concentration=0.75,
        net_pnl=3_000.0,
        daily_pnl=((21_000, 2_250.0), (21_001, 750.0)),
    )
    summary = books._summarize_sprint_episodes(
        ((impossible_pass, "B3"),),
        requested_start_count=1,
        data_censored_count=0,
    )

    assert summary["passing_episode_count"] == 1
    assert summary["passing_consistency_rate"] == 0.0
    assert summary["all_passing_paths_consistency_compliant"] is False


def test_b1_b2_cell_selection_is_invariant_to_b3_b4_outcomes() -> None:
    candidate = {
        "candidate_id": "causal",
        "frontier": [
            _raw_cell(
                account="50K",
                tier=2,
                design_passes=4,
                heldout_passes=0,
            ),
            _raw_cell(
                account="100K",
                tier=3,
                design_passes=2,
                heldout_passes=20,
            ),
        ],
    }
    inverted = deepcopy(candidate)
    for cell in inverted["frontier"]:
        for scenario in ("normal", "stressed"):
            cell[scenario]["by_block"]["B3"]["pass_count"] = 99
            cell[scenario]["by_block"]["B4"]["pass_count"] = 99
            cell[scenario]["by_block"]["B3"]["net_total_usd"] = 1_000_000.0
            cell[scenario]["by_block"]["B4"]["net_total_usd"] = 1_000_000.0

    selected_left, receipt_left = books._select_b1_b2_safe_cell(candidate)
    selected_right, receipt_right = books._select_b1_b2_safe_cell(inverted)

    assert receipt_left == receipt_right
    assert receipt_left["b3_b4_fields_used"] is False
    assert receipt_left["cell_identity"] == {
        "account_label": "50K",
        "account_size_usd": 50_000,
        "integer_quantity_tier": 2,
        "risk_governor_mode": "CAUSAL_STATIC_STOP_RISK_GOVERNOR",
        "horizon_trading_days": 5,
    }
    assert selected_left["account_label"] == selected_right["account_label"]
    assert selected_left["integer_quantity_tier"] == selected_right[
        "integer_quantity_tier"
    ]


def test_two_shards_are_deterministic_disjoint_and_cover_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank = _candidate_bank(
        [
            _classified("q-a", account="50K", tier_q=True),
            _classified("q-b", account="50K", tier_q=True),
            _classified("q-c", account="50K", tier_q=True),
        ]
    )
    composite = {"result_hash": bank["source_composite_result_hash"]}
    context = _context(
        {
            value: _component(value, account="50K")
            for value in ("q-a", "q-b", "q-c")
        }
    )
    monkeypatch.setattr(
        books,
        "_verified_exact_results",
        lambda _initial, _continuations: (composite, ()),
    )
    monkeypatch.setattr(books, "_prepare_replay_context", lambda *args, **kwargs: context)
    monkeypatch.setattr(books, "_evaluate_policy_spec", _fake_policy_result)

    whole = books.build_autonomous_marginal_combine_books(
        ".", bank, {}, requested_book_count=16
    )
    left = books.build_autonomous_marginal_combine_books(
        ".", bank, {}, requested_book_count=16, shard_index=0, shard_count=2
    )
    right = books.build_autonomous_marginal_combine_books(
        ".", bank, {}, requested_book_count=16, shard_index=1, shard_count=2
    )
    left_again = books.build_autonomous_marginal_combine_books(
        ".", bank, {}, requested_book_count=16, shard_index=0, shard_count=2
    )

    whole_ids = set(whole["shard"]["selected_primary_policy_ids"])
    left_ids = set(left["shard"]["selected_primary_policy_ids"])
    right_ids = set(right["shard"]["selected_primary_policy_ids"])
    assert left == left_again
    assert left["shard"]["proposal_inventory_hash"] == right["shard"][
        "proposal_inventory_hash"
    ] == whole["shard"]["proposal_inventory_hash"]
    assert left_ids.isdisjoint(right_ids)
    assert left_ids | right_ids == whole_ids
    assert len(left_ids) + len(right_ids) == whole["counts"][
        "proposal_inventory_count"
    ]

    composite_result = books.compose_autonomous_marginal_combine_book_shards(
        [right, left]
    )
    composite_again = books.compose_autonomous_marginal_combine_book_shards(
        [left, right]
    )
    assert composite_result == composite_again
    assert [row["policy_id"] for row in composite_result["book_results"]] == whole[
        "shard"
    ]["proposal_inventory_policy_ids"]
    assert composite_result["counts"]["g_ready_count"] == whole["counts"][
        "g_ready_count"
    ]
    assert composite_result["counts"]["standalone_g_ready_count"] == 3


def test_singleton_g_ready_has_no_marginal_parent_requirement() -> None:
    raw = _fake_policy_result(
        books._policy_spec(
            account_label="50K",
            members=("solo",),
            profile=books._identity_profile(),
            components={"solo": _component("solo", account="50K")},
            policy_role="TIER_Q_STANDALONE_REFERENCE",
            predecessor_policy_id=None,
        ),
        None,
    )
    classified = books._classify_singletons({"solo": raw})[0]

    assert classified["g_ready"] is True
    assert classified["marginal_contribution"]["status"] == (
        "NOT_APPLICABLE_SINGLETON"
    )
    assert classified["g_ready_gate_results"][
        "marginal_contribution_not_applicable_singleton"
    ] is True
    assert "marginal_contribution_accepted" not in classified[
        "g_ready_gate_results"
    ]


def test_policy_spec_rejects_mixed_account_sizes() -> None:
    components = {
        "a": _component("a", account="50K"),
        "b": _component("b", account="100K"),
    }
    with pytest.raises(books.AutonomousMarginalCombineBooksError, match="mixes"):
        books._policy_spec(
            account_label="50K",
            members=("a", "b"),
            profile=_profile(),
            components=components,
            policy_role="MARGINAL_COMBINE_BOOK_CANDIDATE",
            predecessor_policy_id=None,
        )


def test_executable_spec_is_invariant_to_candidate_or_predecessor_role() -> None:
    components = {
        "a": _component("a", account="50K"),
        "b": _component("b", account="50K"),
    }
    candidate = books._policy_spec(
        account_label="50K",
        members=("a", "b"),
        profile=_profile(),
        components=components,
        policy_role="MARGINAL_COMBINE_BOOK_CANDIDATE",
        predecessor_policy_id="smaller",
    )
    predecessor = books._policy_spec(
        account_label="50K",
        members=("a", "b"),
        profile=_profile(),
        components=components,
        policy_role="PRECEDING_SMALLER_BOOK_CONTROL",
        predecessor_policy_id=None,
    )

    assert candidate["policy_id"] == predecessor["policy_id"]
    assert candidate["policy_spec_hash"] == predecessor["policy_spec_hash"]
    assert candidate["evaluation_metadata"] != predecessor["evaluation_metadata"]


def test_exact_replay_preserves_materialized_quantity_and_role_split() -> None:
    component = _component(
        "exact",
        account="50K",
        with_trajectory=True,
        integer_tier=3,
    )
    context = _context({"exact": component}, with_grid=True)
    spec = books._policy_spec(
        account_label="50K",
        members=("exact",),
        profile=books._identity_profile(),
        components=context.components,
        policy_role="TIER_Q_STANDALONE_REFERENCE",
        predecessor_policy_id=None,
    )

    result = books._evaluate_policy_spec(spec, context)

    assert result["completed_episode_count"] == 24
    assert result["component_quantity_tiers"] == {"exact": 3}
    assert result["additional_quantity_scaling"] is False
    assert result["router_static_risk_tier"] == 1.0
    assert result["governor_policy"]["static_risk_tier"] == 1.0
    assert result["selection_role_contract"] == {
        "DESIGN": ["B1", "B2"],
        "HELD_OUT_DEVELOPMENT": ["B3", "B4"],
    }
    assert result["episode_evidence"]["record_count"] == 24
    assert result["result_hash"] == stable_hash(
        {key: value for key, value in result.items() if key != "result_hash"}
    )


def test_non_q_bank_returns_no_book_without_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank = _candidate_bank([_classified("e-only", account="50K", tier_q=False)])
    composite = {"result_hash": bank["source_composite_result_hash"]}
    monkeypatch.setattr(
        books,
        "_verified_exact_results",
        lambda _initial, _continuations: (composite, ()),
    )
    monkeypatch.setattr(
        books,
        "_prepare_replay_context",
        lambda *args, **kwargs: pytest.fail("replay context must not be loaded"),
    )

    result = books.build_autonomous_marginal_combine_books(".", bank, {})

    assert result["status"] == "NO_EXACT_MARGINAL_BOOK_BATCH"
    assert result["reason"] == "FEWER_THAN_TWO_TIER_Q_COMPONENTS"
    assert result["counts"]["primary_book_exact_replay_count"] == 0


def _candidate_bank(candidates: list[dict[str, object]]) -> dict[str, object]:
    core = {
        "schema": books.CANDIDATE_BANK_SCHEMA,
        "status": "COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION",
        "source_composite_result_hash": "composite-hash",
        "candidates": candidates,
        "counts": {
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
        },
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _classified(
    candidate_id: str, *, account: str, tier_q: bool
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "candidate_fingerprint": f"fingerprint-{candidate_id}",
        "realized_behavioral_fingerprint": f"behavior-{candidate_id}",
        "qd_cell": f"qd-{candidate_id}",
        "source_exact_result_hash": "exact-hash",
        "best_safe_cell": (
            {
                "account_label": account,
                "account_size_usd": {"50K": 50_000, "100K": 100_000}[account],
                "integer_quantity_tier": 3,
                "risk_governor_mode": "CAUSAL_STATIC_STOP_RISK_GOVERNOR",
                "cell_hash": f"cell-{candidate_id}",
            }
            if tier_q
            else None
        ),
        "tier_q_contract_cleared": tier_q,
        "computed_development_tier": "Q" if tier_q else "E",
        "compact_evidence_bundle": {
            "complete": tier_q,
            "source_manifest_hash": "manifest-hash",
            "frozen_grid_hash": "grid-hash",
            "official_rule_snapshot_hash": "rule-hash",
        },
        "authoritative_promotion_status": None,
    }


def _profile() -> GovernorProfile:
    return GovernorProfile(
        profile_id="profile-1",
        signal_quality_tiers=(1.0,),
        open_risk_ceiling_fraction=1.0,
        daily_loss_budget_fraction=1.0,
        daily_profit_lock_fraction=1.0,
        maximum_concurrent_sleeves=4,
        target_protection_fraction=0.0,
        same_instrument_conflict_policy="priority",
    )


def _component(
    candidate_id: str,
    *,
    account: str,
    with_trajectory: bool = False,
    integer_tier: int = 3,
) -> books._PreparedComponent:
    normal = ()
    stressed = ()
    if with_trajectory:
        normal = (_trajectory(candidate_id, scenario="NORMAL"),)
        stressed = (_trajectory(candidate_id, scenario="STRESSED_1_5X"),)
    return books._PreparedComponent(
        candidate_id=candidate_id,
        candidate_fingerprint=f"fingerprint-{candidate_id}",
        behavioral_fingerprint=f"behavior-{candidate_id}",
        qd_cell=f"qd-{candidate_id}",
        account_label=account,
        account_size_usd={"50K": 50_000, "100K": 100_000}[account],
        integer_quantity_tier=integer_tier,
        source_governor_mode="CAUSAL_STATIC_STOP_RISK_GOVERNOR",
        declared_risk_charge_per_mini=100.0,
        normal_trajectories=normal,
        stressed_trajectories=stressed,
        eligible_session_days=frozenset(range(DAY, DAY + 100)),
        censored_session_days=frozenset(),
        source_receipt={"hash": f"source-{candidate_id}"},
    )


def _trajectory(candidate_id: str, *, scenario: str) -> SimpleNamespace:
    event = TradePathEvent(
        event_id=f"{candidate_id}:{scenario}",
        decision_ns=1,
        exit_ns=2,
        session_day=DAY,
        net_pnl=100.0,
        gross_pnl=105.0,
        worst_unrealized_pnl=-10.0,
        best_unrealized_pnl=110.0,
        quantity=3,
        mini_equivalent=0.3,
    )
    return SimpleNamespace(
        component_id=candidate_id,
        market=f"M-{candidate_id}",
        side=1,
        event=event,
        marks=(
            CausalTradeMark(
                availability_time_ns=2,
                worst_unrealized_pnl=-10.0,
                best_unrealized_pnl=110.0,
                current_unrealized_pnl=100.0,
            ),
        ),
        initial_unrealized_pnl=-5.0,
        completed=True,
        censor_time_ns=None,
        censor_reason=None,
    )


def _context(
    components: dict[str, books._PreparedComponent], *, with_grid: bool = False
) -> books._ReplayContext:
    calendar = tuple(range(DAY, DAY + 100))
    starts = {
        5: ((DAY, "B1"), (DAY + 5, "B2"), (DAY + 10, "B3"), (DAY + 15, "B4")),
        10: ((DAY, "B1"), (DAY + 10, "B2"), (DAY + 20, "B3"), (DAY + 30, "B4")),
        20: ((DAY, "B1"), (DAY + 20, "B2"), (DAY + 40, "B3"), (DAY + 60, "B4")),
    }
    rules = {
        "50K": {
            "account_label": "50K",
            "account_size_usd": 50_000,
            "profit_target_usd": 3_000,
            "maximum_loss_limit_usd": 2_000,
            "maximum_mini_contracts": 5,
            "consistency_target_fraction": 0.5,
            "minimum_trading_days": 2,
            "optional_daily_loss_limit_usd": 1_000,
        },
        "100K": {
            "account_label": "100K",
            "account_size_usd": 100_000,
            "profit_target_usd": 6_000,
            "maximum_loss_limit_usd": 3_000,
            "maximum_mini_contracts": 10,
            "consistency_target_fraction": 0.5,
            "minimum_trading_days": 2,
            "optional_daily_loss_limit_usd": 2_000,
        },
    }
    profiles = (
        _profile(),
        GovernorProfile(
            profile_id="profile-2",
            signal_quality_tiers=(1.0,),
            open_risk_ceiling_fraction=0.75,
            daily_loss_budget_fraction=0.5,
            daily_profit_lock_fraction=0.75,
            maximum_concurrent_sleeves=2,
            target_protection_fraction=0.9,
            same_instrument_conflict_policy="priority",
        ),
        GovernorProfile(
            profile_id="profile-3",
            signal_quality_tiers=(1.0,),
            open_risk_ceiling_fraction=0.5,
            daily_loss_budget_fraction=0.5,
            daily_profit_lock_fraction=0.75,
            maximum_concurrent_sleeves=3,
            target_protection_fraction=0.8,
            same_instrument_conflict_policy="priority",
        ),
        GovernorProfile(
            profile_id="profile-4",
            signal_quality_tiers=(1.0,),
            open_risk_ceiling_fraction=0.25,
            daily_loss_budget_fraction=0.25,
            daily_profit_lock_fraction=0.5,
            maximum_concurrent_sleeves=4,
            target_protection_fraction=0.9,
            same_instrument_conflict_policy="priority",
        ),
    )
    return books._ReplayContext(
        calendar=calendar,
        starts=starts,
        rules=rules,
        governor_profiles=profiles,
        components=components,
        source_manifest_hash="manifest-hash",
        frozen_grid_hash="grid-hash",
        official_rule_snapshot_hash="rule-hash",
    )


def _fake_policy_result(
    spec: dict[str, object], _context: object
) -> dict[str, object]:
    count = len(spec["component_ids"])
    summaries_by_role = {
        "DESIGN": {
            scenario: {
                str(horizon): _summary(
                    count=count,
                    scenario=scenario,
                    horizon=horizon,
                    heldout=False,
                )
                for horizon in (5, 10, 20)
            }
            for scenario in books.SCENARIOS
        },
        "HELD_OUT_DEVELOPMENT": {
            scenario: {
                str(horizon): _summary(
                    count=count,
                    scenario=scenario,
                    horizon=horizon,
                    heldout=True,
                )
                for horizon in (5, 10, 20)
            }
            for scenario in books.SCENARIOS
        },
    }
    summaries = {
        scenario: {
            str(horizon): _summary(
                count=count,
                scenario=scenario,
                horizon=horizon,
                heldout=True,
            )
            for horizon in (5, 10, 20)
        }
        for scenario in books.SCENARIOS
    }
    core = {
        "schema": books.RESULT_SCHEMA,
        "policy_id": spec["policy_id"],
        "policy_spec_hash": spec["policy_spec_hash"],
        "policy_role": "EXACT_COMBINE_ACCOUNT_POLICY",
        "account_label": spec["account_label"],
        "component_ids": list(spec["component_ids"]),
        "component_quantity_tiers": dict(spec["component_quantity_tiers"]),
        "governor_profile_id": spec["governor_profile"]["profile_id"],
        "governor_policy": {"static_risk_tier": 1.0},
        "summaries": summaries,
        "summaries_by_role": summaries_by_role,
        "episode_evidence": {"record_count": 24, "receipt_hash": "episodes"},
        "completed_episode_count": 24,
        "data_censored_episode_count": 0,
        "quantity_tiers_materialized_before_book_replay": True,
        "additional_quantity_scaling": False,
        "router_static_risk_tier": 1.0,
        "selection_role_contract": {
            "DESIGN": ["B1", "B2"],
            "HELD_OUT_DEVELOPMENT": ["B3", "B4"],
        },
        "signal_recomputation_performed": False,
        "registry_writes": 0,
        "database_writes": 0,
        "xfa_paths_started": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _summary(
    *, count: int, scenario: str, horizon: int, heldout: bool
) -> dict[str, object]:
    stressed = scenario == "STRESSED_1_5X"
    pass_rate = min(0.10 * count + (0.02 if not stressed else 0.0), 0.90)
    if heldout:
        pass_rate = min(0.12 * count + (0.02 if not stressed else 0.0), 0.90)
    return {
        "requested_start_count": 20,
        "episode_count": 20,
        "full_coverage_start_count": 20,
        "data_censored_count": 0,
        "pass_count": int(pass_rate * 20),
        "pass_rate": pass_rate,
        "net_total": 1_000.0 * count,
        "net_median": 50.0 * count,
        "target_progress_p25": 0.05 * count,
        "target_progress_median": 0.10 * count,
        "target_progress_p75": 0.20 * count,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer": 1_500.0,
        "consistency_rate": 1.0,
        "passing_episode_count": int(pass_rate * 20),
        "passing_consistency_rate": 1.0,
        "all_passing_paths_consistency_compliant": True,
        "passing_best_day_concentration_max": 0.25,
        "median_days_to_target": float(horizon),
        "block_pass_counts": {"B3": 1, "B4": 1},
        "component_contribution": {
            f"component-{index}": 100.0 for index in range(count)
        },
        "best_day_concentration_max": 0.25,
        "maximum_positive_session_day_aggregate_share": 0.25,
        "positive_session_day_aggregate_profit_denominator": 1_000.0,
        "single_episode_day_observation_domination": False,
        "single_trade_domination": False,
        "single_trade_domination_metric_qualification": (
            "LEGACY_FIELD_IS_EPISODE_DAY_OBSERVATION_CONCENTRATION;"
            "NOT_TRADE_LEVEL_EVIDENCE"
        ),
        "blocks_with_passes": ["B3", "B4"],
        "terminal_distribution": {},
    }


def _summary_episode(
    *,
    passed: bool,
    consistency_ok: bool,
    best_day_concentration: float,
    net_pnl: float,
    daily_pnl: tuple[tuple[int, float], ...],
) -> SimpleNamespace:
    return SimpleNamespace(
        target_progress=net_pnl / 3_000.0,
        net_pnl=net_pnl,
        passed=passed,
        mll_breached=False,
        component_contribution={"a": net_pnl / 2.0, "b": net_pnl / 2.0},
        daily_path=tuple(
            {
                "session_day": session_day,
                "day_pnl": value,
                "exposure": {"maximum_mini_equivalent": 1.0},
            }
            for session_day, value in daily_pnl
        ),
        days_to_target=(2 if passed else None),
        maximum_mini_equivalent=1.0,
        maximum_net_directional_exposure=1.0,
        consistency_ok=consistency_ok,
        minimum_mll_buffer=1_500.0,
        best_day_concentration=best_day_concentration,
        accepted_events=1,
        skipped_events=0,
        terminal=SimpleNamespace(value="PASSED" if passed else "TIMEOUT"),
    )


def _raw_cell(
    *, account: str, tier: int, design_passes: int, heldout_passes: int
) -> dict[str, object]:
    def scenario() -> dict[str, object]:
        return {
            "episode_path_hash": f"path-{account}-{tier}",
            "by_block": {
                "B1": {
                    "episode_count": 10,
                    "pass_count": design_passes,
                    "mll_breach_count": 0,
                    "net_total_usd": 1_000.0,
                },
                "B2": {
                    "episode_count": 10,
                    "pass_count": design_passes,
                    "mll_breach_count": 0,
                    "net_total_usd": 1_000.0,
                },
                "B3": {
                    "episode_count": 10,
                    "pass_count": heldout_passes,
                    "mll_breach_count": 0,
                    "net_total_usd": -1_000.0,
                },
                "B4": {
                    "episode_count": 10,
                    "pass_count": heldout_passes,
                    "mll_breach_count": 0,
                    "net_total_usd": -1_000.0,
                },
            },
        }

    return {
        "candidate_id": "causal",
        "account_label": account,
        "account_size_usd": {"50K": 50_000, "100K": 100_000}[account],
        "integer_quantity_tier": tier,
        "risk_governor_mode": "CAUSAL_STATIC_STOP_RISK_GOVERNOR",
        "horizon_trading_days": 5,
        "legally_executable": True,
        "account_rule_compliant": True,
        "hard_compliance_failure_count": 0,
        "normal": scenario(),
        "stressed": scenario(),
    }
