from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.selection.nested_basket_selector import SelectorError
from hydra.selection.time_to_combine import evaluate_time_to_combine
from scripts import run_nested_selector_sprint as runner


DAY_NS = 100_000_000_000_000


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _governance_root(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    root = tmp_path
    _write_jsonl(
        root / "reports/data_budget/databento_spend_ledger.jsonl",
        [
            {
                "timestamp_utc": "2026-07-14T09:00:00Z",
                "cumulative_actual_spend_usd": 87.84738838672598,
            }
        ],
    )
    _write_json(
        root / "mission/state/heartbeat.json",
        {"q4_access_count": 1},
    )
    _write_json(
        root / "mission/state/proof_registry.json",
        {
            "entries": [
                {
                    "event": "Q4_2024_ATOMIC_ACCESS",
                    "proof": "historical-and-burned",
                },
                {"event": "NON_Q4_CONTROL"},
            ]
        },
    )
    baseline = {
        "budget_actual_spend_usd": 87.84738838672598,
        "q4_historical_access_count": 1,
        "q4_proof_entry_count": 1,
    }
    return root, baseline


def test_sprint_governance_baseline_is_exact_and_budget_delta_is_detected(
    tmp_path: Path,
) -> None:
    root, baseline = _governance_root(tmp_path)
    observed = runner._sprint_governance_observation(root)

    runner._validate_sprint_governance_baseline(baseline, observed)
    initial = runner._budget_snapshot(root, baseline=baseline)
    assert initial["actual_spend_delta_usd"] == pytest.approx(0.0, abs=1e-12)
    assert initial["new_purchase_during_sprint"] is False

    drifted = {**baseline, "budget_actual_spend_usd": 87.84738838682598}
    with pytest.raises(runner.NestedSelectorSprintError, match="baseline drift"):
        runner._validate_sprint_governance_baseline(drifted, observed)

    _write_jsonl(
        root / "reports/data_budget/databento_spend_ledger.jsonl",
        [
            {
                "timestamp_utc": "2026-07-14T09:00:00Z",
                "cumulative_actual_spend_usd": baseline[
                    "budget_actual_spend_usd"
                ],
            },
            {
                "timestamp_utc": "2026-07-14T10:00:00Z",
                "cumulative_actual_spend_usd": baseline[
                    "budget_actual_spend_usd"
                ]
                + 0.25,
            },
        ],
    )
    changed = runner._budget_snapshot(root, baseline=baseline)
    assert changed["actual_spend_delta_usd"] == pytest.approx(0.25)
    assert changed["new_purchase_during_sprint"] is True
    assert changed["remaining_usd"] == pytest.approx(
        125.0 - baseline["budget_actual_spend_usd"] - 0.25
    )


def test_q4_delta_detects_either_heartbeat_or_proof_registry_growth(
    tmp_path: Path,
) -> None:
    root, baseline = _governance_root(tmp_path)
    observed = runner._sprint_governance_observation(root)
    runner._validate_sprint_governance_baseline(baseline, observed)

    initial = runner._q4_snapshot(root, baseline=baseline)
    assert initial["q4_access_during_sprint"] == 0
    assert initial["q4_access_authorized"] is False

    _write_json(
        root / "mission/state/proof_registry.json",
        {
            "entries": [
                {"event": "Q4_2024_ATOMIC_ACCESS", "ordinal": 1},
                {"event": "Q4_2024_ATOMIC_ACCESS", "ordinal": 2},
            ]
        },
    )
    proof_growth = runner._q4_snapshot(root, baseline=baseline)
    assert proof_growth["historical_access_count"] == 1
    assert proof_growth["q4_access_during_sprint"] == 1

    _write_json(root / "mission/state/heartbeat.json", {"q4_access_count": 3})
    heartbeat_growth = runner._q4_snapshot(root, baseline=baseline)
    assert heartbeat_growth["q4_access_during_sprint"] == 2

    wrong_count = {**baseline, "q4_proof_entry_count": 0}
    with pytest.raises(runner.NestedSelectorSprintError, match="baseline drift"):
        runner._validate_sprint_governance_baseline(wrong_count, observed)


def _create_terminal_event_database(root: Path) -> None:
    path = root / "mission/state/hydra_mission.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            (
                10,
                "V7_ACTION_TRANSITION",
                json.dumps(
                    {
                        "current": {
                            "manifest_campaign_id": runner.CAMPAIGN_0023_ID,
                            "manifest_campaign_state": "COMPLETE",
                            "manifest_campaign_terminal_state": (
                                "EXACT_CLASS_TOMBSTONED"
                            ),
                            "manifest_campaign_parameter_rescue_allowed": False,
                            "manifest_campaign_same_class_relaunch_allowed": False,
                        }
                    }
                ),
                "2026-07-14T08:00:00Z",
            ),
        )
        connection.commit()
    finally:
        connection.close()


def test_campaign_0023_terminal_event_and_persistence_fail_closed(
    tmp_path: Path,
) -> None:
    _create_terminal_event_database(tmp_path)

    terminal = runner._mission_campaign_terminal_event(tmp_path)
    assert terminal == {
        "event_id": 10,
        "event_type": "V7_ACTION_TRANSITION",
        "created_at": "2026-07-14T08:00:00Z",
        "manifest_campaign_id": runner.CAMPAIGN_0023_ID,
        "manifest_campaign_parameter_rescue_allowed": False,
        "manifest_campaign_same_class_relaunch_allowed": False,
        "manifest_campaign_state": "COMPLETE",
        "manifest_campaign_terminal_state": "EXACT_CLASS_TOMBSTONED",
    }

    campaign_dir = tmp_path / "reports/economic_evolution/campaign_0023"
    diagnostic = {
        "policy_id": runner.DIAGNOSTIC_CHAMPION_ID,
        "status": "DEVELOPMENT_SELECTED_DIAGNOSTIC_CHAMPION",
        "validated": False,
        "promotion_status_inherited": False,
    }
    audit = {
        "schema": "hydra_v73_campaign_0023_terminal_audit_v1",
        "diagnostic_champion": diagnostic,
        "mission_db_terminal_event": terminal,
        "terminal_verdict": "STATIC_PARENT_SYNTHESIS_FALSIFIED",
    }
    runner._persist_campaign_0023_terminal(campaign_dir, audit)
    runner._persist_campaign_0023_terminal(campaign_dir, audit)

    persisted = json.loads(
        (campaign_dir / "campaign_0023_terminal_persistence_v73.json").read_text(
            encoding="utf-8"
        )
    )
    champion = json.loads(
        (
            campaign_dir
            / "development_selected_diagnostic_champion_manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert persisted == audit
    assert champion["status"] == "DEVELOPMENT_SELECTED_DIAGNOSTIC_CHAMPION"
    assert champion["validated"] is False
    assert champion["may_inherit_promotion"] is False
    assert champion["basket_confirmation_ready"] is False
    assert champion["paper_shadow_ready"] is False
    assert champion["manifest_hash"] == stable_hash(
        {key: value for key, value in champion.items() if key != "manifest_hash"}
    )

    empty_root = tmp_path / "missing-terminal"
    path = empty_root / "mission/state/hydra_mission.db"
    path.parent.mkdir(parents=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE events (id INTEGER, event_type TEXT, payload TEXT, created_at TEXT)"
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(
        runner.NestedSelectorSprintError, match="terminal event is absent"
    ):
        runner._mission_campaign_terminal_event(empty_root)


def _rejected_selector_record(
    variant_id: str, *, stressed_passes: int
) -> dict[str, Any]:
    return {
        "variant_id": variant_id,
        "policy_id": variant_id.split("::", maxsplit=1)[0],
        "component_ids": ["A", "B"],
        "risk_label": "RISK_1_00X",
        "design_behavior_fingerprint": f"fingerprint::{variant_id}",
        "normal_net_usd": -1.0,
        "stressed_net_usd": -2.0,
        "mll_breach_rate": 0.0,
        "hard_issue_count": 0,
        "maximum_component_profit_share": 0.50,
        "stressed_combine_pass_count": stressed_passes,
        "normal_combine_pass_count": stressed_passes,
        "stressed_median_target_progress": 0.50,
        "lower_quartile_target_progress": 0.25,
        "stressed_net_pnl": -2.0,
        "consistency": 1.0,
        "component_concentration": 0.50,
        "temporal_block_concentration": 0.50,
        "operational_simplicity": -20.0,
    }


def test_no_admissible_selector_uses_only_deterministic_diagnostic_fallback() -> None:
    records = [
        _rejected_selector_record("policy_b::RISK_1_00X", stressed_passes=0),
        _rejected_selector_record("policy_a::RISK_1_00X", stressed_passes=1),
    ]

    with pytest.raises(SelectorError, match="no selector variant"):
        runner._select(records)

    fallback = runner._select_baseline(records)
    assert fallback.primary["variant_id"] == "policy_a::RISK_1_00X"
    assert fallback.backup is None
    assert fallback.eligible_count == 0
    assert fallback.hard_rejection_count == 2
    assert fallback.pareto_frontier_count == 1
    assert fallback.hard_rejections == (
        {
            "variant_id": "policy_a::RISK_1_00X",
            "reasons": ["BASELINE_DID_NOT_CLEAR_CHAMPION_HARD_GATES"],
        },
        {
            "variant_id": "policy_b::RISK_1_00X",
            "reasons": ["BASELINE_DID_NOT_CLEAR_CHAMPION_HARD_GATES"],
        },
    )


def _round_a_block(
    block_id: str,
    *,
    normal_data_censored: int = 0,
    stressed_data_censored: int = 0,
) -> dict[str, Any]:
    return {
        "policy_id": "selector-finalist",
        "role": "PRIMARY",
        "block_id": block_id,
        "risk_label": "RISK_1_00X",
        "metrics": {
            "episode_count": 24,
            "normal_pass_count": 3,
            "stress_pass_count": 2,
            "mll_breach_count": 0,
            "stressed_net_usd": 1_000.0,
            "stressed_consistency_pass_rate": 0.90,
            "maximum_component_profit_share": 0.30,
            "time_to_combine": {
                "normal": {
                    "40": {"data_censored_count": normal_data_censored}
                },
                "stress_1_5x": {
                    "40": {"data_censored_count": stressed_data_censored}
                },
            },
        },
    }


def test_round_a_censoring_precludes_confirmation_without_counting_failure() -> None:
    rows = [
        _round_a_block(
            "V73_B1", normal_data_censored=1, stressed_data_censored=1
        ),
        _round_a_block("V73_B2"),
        _round_a_block("V73_B3"),
        _round_a_block("V73_B4"),
    ]

    result = runner._round_a_candidate_summary(rows)

    assert result["normal_pass_count"] == 12
    assert result["normal_pass_probability_lower_bound"] == pytest.approx(12 / 96)
    assert result["normal_pass_probability_upper_bound"] == pytest.approx(13 / 96)
    assert result["stressed_pass_count"] == 8
    assert result["stressed_pass_probability_lower_bound"] == pytest.approx(8 / 96)
    assert result["stressed_pass_probability_upper_bound"] == pytest.approx(9 / 96)
    assert result["normal_40_day_data_censored_count"] == 1
    assert result["stressed_40_day_data_censored_count"] == 1
    assert result["censored_survivors_counted_as_failures"] is False
    assert result["checks"]["normal_pass_rate_at_least_10pct"] is None
    assert result["checks"]["stressed_pass_rate_at_least_5pct"] is None
    assert result["status"] == "ROUND_A_CENSORING_PRECLUDES_CONFIRMATION_READY"
    assert result["status"] != "BASKET_CONFIRMATION_READY"


def _trade(day: int, net: float) -> RoutedTrade:
    decision = day * DAY_NS
    return RoutedTrade(
        "component",
        "MES",
        1,
        TradePathEvent(
            event_id=f"component-{day}-{net}",
            decision_ns=decision,
            exit_ns=decision + 1_000,
            session_day=day,
            net_pnl=net,
            gross_pnl=net + 10.0,
            worst_unrealized_pnl=-100.0,
            best_unrealized_pnl=max(net, 0.0),
            quantity=1,
            mini_equivalent=1.0,
            session_compliant=True,
        ),
    )


def _basket() -> BasketPolicy:
    return BasketPolicy(
        policy_id="compact-time-to-combine-test",
        component_ids=("component",),
        archetype="STATIC_CROSS_FIT",
        component_priority=("component",),
        policy_version="hydra_account_policy_v7_2_crossfit_v1",
    )


def test_time_to_combine_compact_rows_and_probability_bounds() -> None:
    summaries = evaluate_time_to_combine(
        {
            "component": (
                _trade(0, 4_500.0),
                _trade(1, 4_500.0),
                _trade(20, 500.0),
            )
        },
        tuple(range(100)),
        basket=_basket(),
        start_days=(0, 20, 90),
        block_id="V73_B1",
    )

    twenty = summaries["20"]
    assert twenty.pass_probability_lower_bound == pytest.approx(1 / 3)
    assert twenty.pass_probability == pytest.approx(1 / 3)
    assert twenty.pass_probability_upper_bound == pytest.approx(2 / 3)
    assert twenty.pass_probability_among_fully_observed == pytest.approx(1 / 2)
    assert twenty.mll_breach_probability_lower_bound == 0.0
    assert twenty.mll_breach_probability == 0.0
    assert twenty.mll_breach_probability_upper_bound == pytest.approx(1 / 3)

    expected_compact_keys = {
        "start_day",
        "status",
        "available_horizon_days",
        "observed_days",
        "days_to_target",
        "net_after_costs",
        "target_progress",
        "maximum_target_progress",
        "mll_breached",
        "consistency_ok",
    }
    assert len(twenty.compact_episode_outcomes) == 3
    assert all(
        set(row) == expected_compact_keys
        for row in twenty.compact_episode_outcomes
    )
    compact_payload = twenty.to_dict()
    assert "episodes" not in compact_payload
    assert len(compact_payload["compact_episode_outcomes"]) == 3

    for summary in summaries.values():
        for lower, point, upper in (
            (
                summary.pass_probability_lower_bound,
                summary.pass_probability,
                summary.pass_probability_upper_bound,
            ),
            (
                summary.mll_breach_probability_lower_bound,
                summary.mll_breach_probability,
                summary.mll_breach_probability_upper_bound,
            ),
        ):
            assert 0.0 <= lower <= point <= upper <= 1.0
        assert len(summary.compact_episode_outcomes) == summary.episode_count
        assert summary.censored_count == (
            summary.data_censored_count
            + summary.operational_horizon_not_reached_count
        )
        for curve_row in summary.target_progress_curve:
            for key in (
                "pass_cumulative_probability",
                "mll_cumulative_probability",
                "hard_rule_failure_cumulative_probability",
                "censoring_cumulative_probability",
            ):
                assert 0.0 <= float(curve_row[key]) <= 1.0
