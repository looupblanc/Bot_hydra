from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_consistency_account_policies as direct
from hydra.production import autonomous_marginal_combine_books as books


def test_frontier_is_bounded_causal_and_account_only() -> None:
    profiles = direct.frozen_consistency_profiles()

    assert len(profiles) == direct.MAXIMUM_PROFILES == 6
    assert len({row.profile_id for row in profiles}) == 6
    assert all(row.maximum_concurrent_sleeves == 1 for row in profiles)
    assert all(row.signal_quality_tiers == (1.0,) for row in profiles)
    serialized = str([row.__dict__ for row in profiles]).lower()
    assert "net_pnl" not in serialized
    assert "outcome" not in serialized
    assert "future" not in serialized


def test_design_ranking_is_invariant_to_heldout_outcomes() -> None:
    weaker = _fake_result(_spec("candidate", "consistency_direct_01"), None)
    stronger = _fake_result(_spec("candidate", "consistency_direct_02"), None)
    stronger["summaries_by_role"]["DESIGN"]["STRESSED_1_5X"]["5"][
        "pass_rate"
    ] = 0.50
    left = direct._design_rank(stronger)

    poisoned = deepcopy(stronger)
    for scenario in books.SCENARIOS:
        for horizon in books.HORIZONS:
            summary = poisoned["summaries_by_role"]["HELD_OUT_DEVELOPMENT"][
                scenario
            ][str(horizon)]
            summary["pass_count"] = 0
            summary["pass_rate"] = 0.0
            summary["net_total"] = -1_000_000.0
            summary["maximum_positive_session_day_aggregate_share"] = 1.0

    assert direct._design_rank(poisoned) == left
    assert direct._design_rank(stronger) > direct._design_rank(weaker)


def test_two_shards_are_deterministic_disjoint_and_composable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank = _bank(("q-a", "q-b", "q-c"))
    context = _context(("q-a", "q-b", "q-c"))
    composite = {"result_hash": bank["source_composite_result_hash"]}
    monkeypatch.setattr(
        books,
        "_verified_exact_results",
        lambda _initial, _continuations: (composite, ()),
    )
    monkeypatch.setattr(books, "_prepare_replay_context", lambda *a, **k: context)
    monkeypatch.setattr(books, "_verify_context_matches_bank", lambda *a, **k: None)
    monkeypatch.setattr(books, "_evaluate_policy_spec", _fake_result)

    whole = direct.build_autonomous_consistency_account_policies(".", bank, {})
    left = direct.build_autonomous_consistency_account_policies(
        ".", bank, {}, shard_index=0, shard_count=2
    )
    right = direct.build_autonomous_consistency_account_policies(
        ".", bank, {}, shard_index=1, shard_count=2
    )
    left_again = direct.build_autonomous_consistency_account_policies(
        ".", bank, {}, shard_index=0, shard_count=2
    )

    assert left == left_again
    left_ids = set(left["shard"]["selected_candidate_ids"])
    right_ids = set(right["shard"]["selected_candidate_ids"])
    assert left_ids.isdisjoint(right_ids)
    assert left_ids | right_ids == set(whole["tier_q_component_ids"])
    assert whole["counts"]["direct_policy_exact_replay_count"] == 18
    assert whole["counts"]["authoritative_promotion_count"] == 0
    assert whole["counts"]["xfa_paths_started"] == 0

    composed = direct.compose_autonomous_consistency_account_policy_shards(
        (right, left)
    )
    composed_again = direct.compose_autonomous_consistency_account_policy_shards(
        (left, right)
    )
    assert composed == composed_again
    assert composed["counts"]["direct_policy_exact_replay_count"] == 18
    assert [
        row["source_candidate_id"] for row in composed["selected_policy_results"]
    ] == list(whole["tier_q_component_ids"])
    assert composed["candidate_ids"] == whole["candidate_ids"]


def test_composer_rejects_tampered_or_write_capable_shard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank = _bank(("q-a",))
    context = _context(("q-a",))
    composite = {"result_hash": bank["source_composite_result_hash"]}
    monkeypatch.setattr(
        books,
        "_verified_exact_results",
        lambda _initial, _continuations: (composite, ()),
    )
    monkeypatch.setattr(books, "_prepare_replay_context", lambda *a, **k: context)
    monkeypatch.setattr(books, "_verify_context_matches_bank", lambda *a, **k: None)
    monkeypatch.setattr(books, "_evaluate_policy_spec", _fake_result)
    shard = direct.build_autonomous_consistency_account_policies(".", bank, {})

    tampered = deepcopy(shard)
    tampered["counts"]["orders"] = 1
    tampered["result_hash"] = stable_hash(
        {key: value for key, value in tampered.items() if key != "result_hash"}
    )
    with pytest.raises(
        direct.AutonomousConsistencyAccountPolicyError, match="safety"
    ):
        direct.compose_autonomous_consistency_account_policy_shards((tampered,))


def test_no_tier_q_candidate_returns_without_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank = _bank(())
    composite = {"result_hash": bank["source_composite_result_hash"]}
    monkeypatch.setattr(
        books,
        "_verified_exact_results",
        lambda _initial, _continuations: (composite, ()),
    )
    monkeypatch.setattr(
        books,
        "_prepare_replay_context",
        lambda *a, **k: pytest.fail("empty Tier-Q bank must not load replay context"),
    )

    result = direct.build_autonomous_consistency_account_policies(".", bank, {})

    assert result["status"] == "NO_BOUNDED_CONSISTENCY_DIRECT_ACCOUNT_SHARD"
    assert result["counts"]["direct_policy_exact_replay_count"] == 0


def _bank(candidate_ids: tuple[str, ...]) -> dict[str, object]:
    candidates = [
        {
            "candidate_id": candidate_id,
            "candidate_fingerprint": f"fingerprint-{candidate_id}",
            "realized_behavioral_fingerprint": f"behavior-{candidate_id}",
            "qd_cell": f"qd-{candidate_id}",
            "source_exact_result_hash": "exact-hash",
            "best_safe_cell": {
                "account_label": "50K",
                "account_size_usd": 50_000,
                "integer_quantity_tier": 3,
                "risk_governor_mode": "CAUSAL_STATIC_STOP_RISK_GOVERNOR",
                "cell_hash": f"cell-{candidate_id}",
            },
            "tier_q_contract_cleared": True,
            "computed_development_tier": "Q",
            "compact_evidence_bundle": {
                "complete": True,
                "source_manifest_hash": "manifest-hash",
                "frozen_grid_hash": "grid-hash",
                "official_rule_snapshot_hash": "rule-hash",
            },
            "authoritative_promotion_status": None,
        }
        for candidate_id in candidate_ids
    ]
    core = {
        "schema": books.CANDIDATE_BANK_SCHEMA,
        "status": "COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION",
        "source_composite_result_hash": "composite-hash",
        "candidates": candidates,
        "counts": {"authoritative_promotion_count": 0, "xfa_paths_started": 0},
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _context(candidate_ids: tuple[str, ...]) -> SimpleNamespace:
    components = {
        candidate_id: SimpleNamespace(
            candidate_id=candidate_id,
            candidate_fingerprint=f"fingerprint-{candidate_id}",
            behavioral_fingerprint=f"behavior-{candidate_id}",
            qd_cell=f"qd-{candidate_id}",
            account_label="50K",
            account_size_usd=50_000,
            integer_quantity_tier=3,
            source_governor_mode="CAUSAL_STATIC_STOP_RISK_GOVERNOR",
            declared_risk_charge_per_mini=100.0,
            normal_trajectories=(),
            stressed_trajectories=(),
            eligible_session_days=frozenset(),
            censored_session_days=frozenset(),
            source_receipt={"hash": f"source-{candidate_id}"},
        )
        for candidate_id in candidate_ids
    }
    return SimpleNamespace(
        components=components,
        source_manifest_hash="manifest-hash",
        frozen_grid_hash="grid-hash",
        official_rule_snapshot_hash="rule-hash",
        design_cell_exclusions={},
    )


def _spec(candidate_id: str, profile_id: str) -> dict[str, object]:
    return {
        "policy_id": f"policy-{candidate_id}-{profile_id}",
        "policy_spec_hash": f"spec-{candidate_id}-{profile_id}",
        "account_label": "50K",
        "component_ids": [candidate_id],
        "component_quantity_tiers": {candidate_id: 3},
        "governor_profile": {"profile_id": profile_id},
    }


def _fake_result(spec: dict[str, object], _context: object) -> dict[str, object]:
    profile_id = str(spec["governor_profile"]["profile_id"])
    strength = 0.20 if profile_id.endswith("02") else 0.10
    summaries_by_role = {
        role: {
            scenario: {
                str(horizon): _summary(
                    component_id=str(spec["component_ids"][0]),
                    pass_rate=strength,
                    blocks=("B1", "B2")
                    if role == "DESIGN"
                    else ("B3", "B4"),
                )
                for horizon in books.HORIZONS
            }
            for scenario in books.SCENARIOS
        }
        for role in ("DESIGN", "HELD_OUT_DEVELOPMENT")
    }
    summaries = {
        scenario: {
            str(horizon): _summary(
                component_id=str(spec["component_ids"][0]),
                pass_rate=strength,
                blocks=("B1", "B2", "B3", "B4"),
            )
            for horizon in books.HORIZONS
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
        "governor_profile_id": profile_id,
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
    *, component_id: str, pass_rate: float, blocks: tuple[str, ...]
) -> dict[str, object]:
    return {
        "requested_start_count": 10,
        "episode_count": 10,
        "full_coverage_start_count": 10,
        "data_censored_count": 0,
        "pass_count": 2,
        "pass_rate": pass_rate,
        "net_total": 3_000.0,
        "net_median": 300.0,
        "target_progress_p25": 0.20,
        "target_progress_median": 0.50,
        "target_progress_p75": 1.0,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer": 1_000.0,
        "consistency_rate": 0.5,
        "passing_episode_count": 2,
        "passing_consistency_rate": 1.0,
        "all_passing_paths_consistency_compliant": True,
        "passing_best_day_concentration_max": 0.50,
        "median_days_to_target": 5.0,
        "block_pass_counts": {block: 1 for block in blocks},
        "blocks_with_passes": list(blocks),
        "component_contribution": {component_id: 3_000.0},
        "best_day_concentration_max": 1_000.0,
        "maximum_positive_session_day_aggregate_share": 0.40,
        "positive_session_day_aggregate_profit_denominator": 5_000.0,
        "single_episode_day_observation_domination": False,
        "single_trade_domination": False,
        "single_trade_domination_metric_qualification": "NOT_TRADE_LEVEL_EVIDENCE",
        "accepted_event_count": 20,
        "skipped_event_count": 5,
        "maximum_mini_equivalent_mean": 1.0,
        "maximum_mini_equivalent_max": 1.0,
        "maximum_net_directional_exposure_mean": 1.0,
        "maximum_net_directional_exposure_max": 1.0,
        "mean_daily_maximum_mini_equivalent": 1.0,
        "mean_daily_contract_utilization": 0.2,
        "terminal_distribution": {"PASSED": 2, "TIMEOUT": 8},
    }
