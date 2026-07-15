from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.active_pool_replay import RoutedTrade
from hydra.economic_evolution.schema import stable_hash
from hydra.production.active_risk_runtime import (
    ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
    ActiveRiskRuntimeError,
    ActiveRiskPoolRun,
    _compact_metric,
    _evidence_rows,
    _evaluate_active_policy,
    _generate_governors,
    _match_random_priority_controls,
    _select_policies,
    _set_worker_state,
)
from hydra.production.portfolio_books import SleeveRecord
from hydra.propfirm.combine_episode import TradePathEvent


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _sleeve(index: int) -> SleeveRecord:
    return SleeveRecord(
        sleeve_id=f"sleeve-{index:02d}",
        immutable_fingerprint=_sha(f"immutable:{index}"),
        behavioral_fingerprint=_sha(f"behavior:{index}"),
        signal_ledger_sha256=_sha(f"signals:{index}"),
        trade_ledger_sha256=_sha(f"trades:{index}"),
        market="ES",
        contract="MES",
        timeframe="5m",
        session="OPEN",
        economic_role="TARGET_VELOCITY",
        source_campaign="sealed-source",
        family_id=f"family-{index}",
    )


def _policy(component: str = "sleeve-00") -> ActiveRiskPoolPolicy:
    return ActiveRiskPoolPolicy(
        policy_id="active-runtime-test",
        component_priority=(component,),
        nominal_risk_charge_per_mini=((component, 2250.0),),
        maximum_concurrent_sleeves=1,
        aggregate_open_risk_ceiling=4500.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=4500.0,
        daily_consistency_profit_guard=9000.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _manifest(end_day: int) -> dict:
    return {
        "temporal_blocks": {
            "blocks": [
                {
                    "block_id": "B1",
                    "start": "1970-01-01",
                    "end": f"1970-01-{end_day + 1:02d}",
                }
            ]
        }
    }


def _passing_rows(component: str, *, final_day: int = 11) -> tuple[RoutedTrade, ...]:
    return tuple(
        RoutedTrade(
            component_id=component,
            market="ES",
            side=1,
            event=TradePathEvent(
                event_id=f"passing-event-{day}",
                session_day=day,
                decision_ns=day * 1_000 + 1,
                exit_ns=day * 1_000 + 2,
                net_pnl=4_600.0 if day < 2 else 1_000.0,
                gross_pnl=4_601.0 if day < 2 else 1_001.0,
                worst_unrealized_pnl=-100.0,
                best_unrealized_pnl=4_700.0 if day < 2 else 1_100.0,
                quantity=1,
                mini_equivalent=0.1,
            ),
        )
        for day in range(final_day + 1)
    )


def test_generator_freezes_structurally_unique_bounded_governors() -> None:
    policies, attempts = _generate_governors(
        tuple(_sleeve(index) for index in range(18)), seed=25, count=128
    )

    assert len(policies) == 128
    assert attempts >= 128
    assert len({row.structural_fingerprint for row in policies}) == 128
    assert all(len(row.component_ids) == 18 for row in policies)
    assert all(row.preserve_sole_sleeve_nominal_risk for row in policies)
    assert all(not row.outbound_order_capability for row in policies)


def test_random_priority_matcher_uses_only_frozen_exposure_signatures() -> None:
    policy = _policy()
    target = {
        "time_weighted_mini_nanoseconds_per_observed_day": 100.0,
        "accepted_event_rate": 0.50,
    }
    specs = []
    rows = []
    for index in range(32):
        control_id = f"random:{index}"
        specs.append(
            {
                "control_id": control_id,
                "matched_policy_id": policy.policy_id,
                "seed": 25_002_600 + index,
                "kind": "ACTIVE",
                "policy": policy.to_dict(),
            }
        )
        rows.append(
            {
                "control_id": control_id,
                "matched_policy_id": policy.policy_id,
                "seed": 25_002_600 + index,
                "exposure_signature": {
                    "time_weighted_mini_nanoseconds_per_observed_day": (
                        101.0 if index == 7 else 140.0 + index
                    ),
                    "accepted_event_rate": 0.51 if index == 7 else 0.80,
                },
            }
        )

    selected, matches = _match_random_priority_controls(
        policies=(policy,),
        metrics_by_policy={
            policy.policy_id: {"exposure_signature": target, "net_total": -999.0}
        },
        screen_specs=specs,
        screen_rows=rows,
    )

    assert selected[0]["control_id"] == "random:7"
    assert matches[0]["matched"] is True
    assert matches[0]["economic_outcomes_used_for_selection"] is False
    assert matches[0]["deltas"]["accepted_event_rate"][
        "relative_delta"
    ] == pytest.approx(0.02)


def test_projected_active_days_use_traded_not_eligible_days() -> None:
    component = "sleeve-00"
    events = tuple(
        RoutedTrade(
            component_id=component,
            market="ES",
            side=1,
            event=TradePathEvent(
                event_id=f"sparse-{day}",
                session_day=day,
                decision_ns=day * 1_000 + 1,
                exit_ns=day * 1_000 + 2,
                net_pnl=450.0,
                gross_pnl=451.0,
                worst_unrealized_pnl=-10.0,
                best_unrealized_pnl=500.0,
                quantity=1,
                mini_equivalent=0.1,
            ),
        )
        for day in (0, 9)
    )
    _set_worker_state(
        {component: events},
        starts=(0,),
        calendars={0: tuple(range(10))},
        full_calendars={0: tuple(range(10))},
        campaign_id="active-days-test",
        blocks=(
            {"block_id": "B1", "start": "1970-01-01", "end": "1970-01-10"},
        ),
    )

    metric = _evaluate_active_policy(
        _policy(),
        horizons=(10,),
        include_stress=False,
        include_evidence=False,
        lifecycle=False,
    )

    summary = metric["normal"]
    assert summary["active_trading_days_values"] == [2]
    assert summary["duration_trading_days_values"] == [10]
    assert summary["target_progress_median"] == pytest.approx(0.1)
    assert summary["projected_active_days_to_target_median"] == pytest.approx(20.0)
    assert summary["projected_calendar_days_to_target_median"] == pytest.approx(100.0)


def test_worker_reports_frozen_horizon_and_causal_risk_utilisation() -> None:
    component = "sleeve-00"
    rows = tuple(
        RoutedTrade(
            component_id=component,
            market="ES",
            side=1,
            event=TradePathEvent(
                event_id=f"event-{day}",
                session_day=day,
                decision_ns=day * 1_000 + 1,
                exit_ns=day * 1_000 + 2,
                net_pnl=1_000.0,
                gross_pnl=1_001.0,
                worst_unrealized_pnl=-100.0,
                best_unrealized_pnl=1_100.0,
                quantity=1,
                mini_equivalent=0.1,
            ),
        )
        for day in range(10)
    )
    _set_worker_state(
        {component: rows},
        starts=(0,),
        calendars={0: tuple(range(10))},
        full_calendars={0: tuple(range(10))},
        campaign_id="active-test",
        blocks=(
            {
                "block_id": "B1",
                "start": "1970-01-01",
                "end": "1970-01-10",
            },
        ),
    )

    metric = _evaluate_active_policy(
        _policy(),
        horizons=(20, "FULL"),
        include_stress=True,
        include_evidence=True,
        lifecycle=False,
    )

    assert metric["normal"]["pass_count"] == 1
    assert metric["stressed"]["pass_count"] == 1
    assert metric["horizons"]["normal"]["20_TRADING_DAYS"]["episode_count"] == 1
    assert metric["risk_utilisation"]["observation_count"] > 0
    assert metric["suppression"]["signals_rejected"] == 0
    assert {row["horizon_label"] for row in metric["evidence_raw"]} == {
        "20_TRADING_DAYS",
        "FULL_CHRONOLOGICAL_HORIZON",
    }


def test_full_pass_xfa_paths_are_sealed_on_matching_authoritative_episodes() -> None:
    component = "sleeve-00"
    rows = _passing_rows(component)
    days = tuple(range(12))
    _set_worker_state(
        {component: rows},
        starts=(0,),
        calendars={0: days},
        full_calendars={0: days},
        campaign_id="active-xfa-evidence-test",
        blocks=(
            {"block_id": "B1", "start": "1970-01-01", "end": "1970-01-12"},
        ),
    )

    metric = _evaluate_active_policy(
        _policy(),
        horizons=(20, "FULL"),
        include_stress=True,
        include_evidence=True,
        lifecycle=True,
    )
    # A cache round trip retains the raw lifecycle; authoritative conversion
    # occurs before compact in the resumable runtime.
    cached = json.loads(json.dumps(metric))
    episodes, _daily = _evidence_rows(cached, _manifest(11))
    compacted = _compact_metric(cached)

    assert metric["xfa_paths_started"] == 2
    assert len(metric["lifecycle_rows"]) == 2
    assert "lifecycle_rows" not in compacted
    assert "evidence_raw" not in compacted
    full = [
        row
        for row in episodes
        if row["horizon"] == "FULL_CHRONOLOGICAL_HORIZON"
    ]
    shorter = [row for row in episodes if row["horizon"] == "20_TRADING_DAYS"]
    assert len(full) == 2
    assert all("active_risk_pool_lifecycle" in row for row in full)
    assert all("active_risk_pool_lifecycle" not in row for row in shorter)
    assert {row["cost_scenario"] for row in full} == {"NORMAL", "STRESSED_1_5X"}
    for episode in full:
        lifecycle = episode["active_risk_pool_lifecycle"]
        assert lifecycle["scenario"] == episode["cost_scenario"]
        assert lifecycle["combine_horizon"] == "FULL_CHRONOLOGICAL_HORIZON"
        assert lifecycle["xfa_overlay_semantics"] == (
            ACTIVE_RISK_XFA_OVERLAY_SEMANTICS
        )
        assert lifecycle["combine_governor_controls_applied_in_xfa"] is False
        assert lifecycle["xfa_profile_frozen_before_replay"] is True
        assert lifecycle["xfa_profile_selected_from_outcomes"] is False
        profile = lifecycle["xfa_profile"]
        projection = lifecycle["xfa_profile_projection"]
        assert profile["fingerprint"] == stable_hash(
            {key: value for key, value in profile.items() if key != "fingerprint"}
        )
        assert projection["risk_multiplier"] == profile["risk_multiplier"]
        assert projection["clip_to_official_xfa_scaling_plan"] is True
        assert projection["same_market_exclusive"] is True
        rules = lifecycle["rule_snapshot"]
        assert rules["fingerprint"] == stable_hash(
            {key: value for key, value in rules.items() if key != "fingerprint"}
        )
        for path_name in ("standard", "consistency"):
            path = lifecycle[path_name]
            assert len(path["daily_ledger"]) == path["observed_days"]
            assert path["observed_days"] > 0
            assert len(path["path_hash"]) == 64
        sealed = dict(lifecycle)
        claimed_sealed_hash = sealed.pop("sealed_lifecycle_sha256")
        assert claimed_sealed_hash == stable_hash(sealed)


def test_active_xfa_evidence_fails_closed_on_cardinality_or_nested_hash_drift() -> None:
    component = "sleeve-00"
    rows = _passing_rows(component)
    days = tuple(range(12))
    _set_worker_state(
        {component: rows},
        starts=(0,),
        calendars={0: days},
        full_calendars={0: days},
        campaign_id="active-xfa-evidence-test",
        blocks=(
            {"block_id": "B1", "start": "1970-01-01", "end": "1970-01-12"},
        ),
    )
    metric = _evaluate_active_policy(
        _policy(),
        horizons=("FULL",),
        include_stress=True,
        include_evidence=True,
        lifecycle=True,
    )

    missing_path = copy.deepcopy(metric)
    missing_path["lifecycle_rows"].pop()
    with pytest.raises(
        ActiveRiskRuntimeError,
        match="FULL-pass/XFA authoritative evidence cardinality drift",
    ):
        _evidence_rows(missing_path, _manifest(11))

    nested_drift = copy.deepcopy(metric)
    raw = nested_drift["lifecycle_rows"][0]
    raw["standard"]["daily_ledger"][0]["closing_balance"] += 1.0
    source = dict(raw)
    source.pop("source_lifecycle_sha256")
    raw["source_lifecycle_sha256"] = stable_hash(source)
    with pytest.raises(ActiveRiskRuntimeError, match="XFA path hash drift"):
        _evidence_rows(nested_drift, _manifest(11))


def test_last_day_full_pass_seals_two_explicit_zero_observation_xfa_paths() -> None:
    component = "sleeve-00"
    rows = _passing_rows(component, final_day=1)
    days = (0, 1)
    _set_worker_state(
        {component: rows},
        starts=(0,),
        calendars={0: days},
        full_calendars={0: days},
        campaign_id="active-xfa-censored-test",
        blocks=(
            {"block_id": "B1", "start": "1970-01-01", "end": "1970-01-02"},
        ),
    )
    metric = _evaluate_active_policy(
        _policy(),
        horizons=("FULL",),
        include_stress=False,
        include_evidence=True,
        lifecycle=True,
    )

    episodes, _daily = _evidence_rows(metric, _manifest(1))

    assert len(episodes) == 1
    lifecycle = episodes[0]["active_risk_pool_lifecycle"]
    assert lifecycle["xfa_start_day"] is None
    for path_name in ("standard", "consistency"):
        path = lifecycle[path_name]
        assert path["terminal"] == "DATA_CENSORED"
        assert path["observed_days"] == 0
        assert path["daily_ledger"] == []


def test_selection_is_transparent_and_deduplicates_actual_behavior() -> None:
    policies = (_policy(),)
    metric = {
        "policy_id": policies[0].policy_id,
        "actual_account_behavior_fingerprint": "behavior-a",
        "normal": {
            "net_total": 100.0,
            "pass_count": 1,
            "pass_rate": 0.25,
            "target_progress_p25": 0.4,
            "target_progress_median": 0.5,
            "net_median": 100.0,
            "mll_breach_rate": 0.0,
            "consistency_rate": 1.0,
            "maximum_block_profit_share": 0.5,
        },
        "stressed": None,
        "risk_utilisation": {"mean": 0.25},
        "suppression": {"signals_rejected": 0},
    }

    selected, decision = _select_policies(
        policies,
        (metric,),
        limit=1,
        require_stress=False,
        stage="TEST",
    )

    assert selected == policies
    assert decision["opaque_score_used"] is False
    assert decision["selected_policy_ids"] == [policies[0].policy_id]


def test_sealed_active_bundle_recovers_result_without_portfolio_forward_anchor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash after seal must preserve active, multi-horizon semantics."""

    import hydra.production.active_risk_runtime as runtime_module

    campaign_id = "hydra_active_risk_pool_target_velocity_test"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}\n", encoding="utf-8")
    source_commit = "a" * 40
    manifest = {
        "campaign_id": campaign_id,
        "manifest_hash": "b" * 64,
        "source_commit": source_commit,
        "runtime": {"result_name": "active_result.json"},
        "evidence_bundle": {
            "destination": "evidence",
            "lightweight_manifest_path": "reports/active_receipt.json",
        },
    }
    final = tmp_path / "evidence" / f"{campaign_id}.evidence-v1"
    outputs = final / "outputs"
    outputs.mkdir(parents=True)
    identity = {
        "campaign_id": campaign_id,
        "source_commit": source_commit,
        "configuration_sha256": runtime_module._sha256(manifest_path),
    }
    (final / "identity.json").write_text(
        json.dumps(identity), encoding="utf-8"
    )
    summary = {
        "schema": "hydra_active_risk_campaign_summary_v1",
        "campaign_id": campaign_id,
        "scientific_status": "ACTIVE_RISK_POOL_TARGET_VELOCITY_IMPROVED_WEAK",
        "confirmation_ready_candidate_ids": [],
        "policies_promoted_to_96": 2,
        "policies_surviving_96": 1,
        "normal_combine_passes": 2,
        "stressed_combine_passes": 1,
        # Persisted counters include five horizons, while these frozen
        # headlines intentionally use only the canonical 90-day horizon.
        "production_counters": {
            "serious_exact_account_replays": 1024,
            "combine_episodes_completed": 100,
            "normal_episodes_completed": 50,
            "stressed_episodes_completed": 50,
        },
        "horizon_frontier": {
            "90_TRADING_DAYS": {
                "normal": {"episode_count": 10, "pass_count": 2},
                "stressed": {"episode_count": 10, "pass_count": 1},
            }
        },
        "matched_controls": {"status": "EXECUTED", "control_count": 3},
    }
    stage_decisions = [
        {"stage": "ACTIVE_POOL_STAGE_3_TO_96", "output_count": 2},
        {"stage": "ACTIVE_POOL_EXPANDED_CONFIRMATION_GATE", "output_count": 1},
    ]
    sealed_outputs = {
        "campaign_summary": summary,
        "failure_vectors": {"schema": "failure-test", "by_policy": {}},
        "pareto_archive": {
            "schema": "hydra_active_risk_pareto_archive_v1",
            "stage_decisions": stage_decisions,
        },
        "next_campaign_recommendations": {
            "recommendation": {
                "action": "CONTINUE_FROZEN_ACTIVE_POOL_FINALISTS"
            }
        },
    }
    for name, payload in sealed_outputs.items():
        (outputs / f"{name}.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    kpis = {"state": "FINALIZING", "combine_episodes_completed": 100}
    kpis["kpi_hash"] = stable_hash(kpis)
    (output_dir / "production_kpis.json").write_text(
        json.dumps(kpis), encoding="utf-8"
    )
    receipt_payload = {
        "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
        "reconstruction_flag": False,
        "bundle_path": str(final),
        "manifest_path": str(final / "manifest.json"),
        "manifest_sha256": "c" * 64,
        "bundle_content_sha256": "d" * 64,
        "dataset_row_counts": {"episodes": 100},
    }
    receipt = SimpleNamespace(to_dict=lambda: dict(receipt_payload))
    monkeypatch.setattr(
        runtime_module,
        "recover_finalized_evidence_bundle",
        lambda *args, **kwargs: receipt,
    )

    written: dict[str, object] = {}
    sequence: list[str] = []
    run = ActiveRiskPoolRun.__new__(ActiveRiskPoolRun)
    run.root = tmp_path
    run.campaign_id = campaign_id
    run.manifest_path = manifest_path
    run.manifest = manifest
    run.output_dir = output_dir
    run.output_writer = SimpleNamespace(
        write_json=lambda name, payload: (
            sequence.append("result_write"),
            written.update(name=name, payload=payload),
        )
    )
    run._reconcile_completed_result_snapshots = lambda result: sequence.append(
        "reconcile"
    )
    run._reconcile_forward_package_anchors = lambda result: pytest.fail(
        "active recovery must not invoke portfolio forward anchoring"
    )

    def fake_load(path: Path, frozen_manifest: dict) -> dict:
        del path, frozen_manifest
        sequence.append("verify")
        return dict(written["payload"])

    monkeypatch.setattr(runtime_module, "load_and_verify_production_result", fake_load)

    recovered = run._recover_sealed_bundle_result()

    assert recovered is not None
    assert recovered["scientific_status"] == summary["scientific_status"]
    assert recovered["matched_controls"] == summary["matched_controls"]
    assert recovered["successive_halving"]["stage_decisions"] == stage_decisions
    assert recovered["economic_results"]["production_counters"][
        "combine_episodes_completed"
    ] == 100
    assert recovered["economic_results"]["horizon_frontier"][
        "90_TRADING_DAYS"
    ]["normal"]["episode_count"] == 10
    assert sequence == ["result_write", "verify", "reconcile"]
