from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import active_risk_decision_report as report_module
from hydra.production.active_risk_decision_report import (
    ActiveRiskDecisionReportError,
    build_active_risk_decision_report,
    canonical_hash,
    render_markdown,
)


CAMPAIGN = "hydra_active_risk_pool_target_velocity_0026"
HORIZONS = (
    "20_TRADING_DAYS",
    "40_TRADING_DAYS",
    "60_TRADING_DAYS",
    "90_TRADING_DAYS",
    "FULL_CHRONOLOGICAL_HORIZON",
)


def _epoch_day(value: str) -> int:
    return (date.fromisoformat(value) - date(1970, 1, 1)).days


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _summary(*, stressed: bool = False) -> dict[str, object]:
    progress = [0.20, 0.35, 0.55, 1.05]
    net = [1800.0, 3150.0, 4950.0, 9450.0]
    if stressed:
        progress = [value - 0.05 for value in progress]
        net = [value - 450.0 for value in net]
    return {
        "episode_count": 4,
        "pass_count": 1,
        "pass_rate": 0.25,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "censored_episode_count": 3,
        "censoring_rate": 0.75,
        "terminal_distribution": {"DATA_CENSORED": 3, "TARGET_REACHED": 1},
        "net_total": sum(net),
        "net_median": (net[1] + net[2]) / 2.0,
        "net_values": net,
        "target_progress_median": (progress[1] + progress[2]) / 2.0,
        "target_progress_p25": 0.3125 if not stressed else 0.2625,
        "target_progress_values": progress,
        "maximum_target_progress": max(progress),
        "minimum_mll_buffer": 3000.0,
        "consistency_rate": 0.75,
        "consistency_ok_count": 3,
        "duration_trading_days_values": [20, 20, 20, 20],
        "duration_trading_days_median": 20.0,
        "active_trading_days_values": [10, 11, 12, 13],
        "active_trading_days_median": 11.5,
        "calendar_days_values": [25, 26, 27, 28],
        "calendar_days_median": 26.5,
        "days_to_target_values": [18],
        "median_days_to_target": 18.0,
        "projected_active_days_to_target_median": 35.0,
        "projected_calendar_days_to_target_median": 70.0,
        "monthly_subscription_duration_proxy_median": 70.0 / 30.0,
        "pass_block_count": 1,
        "pass_block_ids": ["B1"],
        "by_block_net": {
            "B1": net[0],
            "B2": net[1],
            "B3": net[2],
            "B4": net[3],
        },
        "by_block_target_progress_median": {
            "B1": progress[0],
            "B2": progress[1],
            "B3": progress[2],
            "B4": progress[3],
        },
        "component_contribution": {"sleeve": sum(net)},
        "maximum_block_profit_share": 0.50,
        "maximum_sleeve_profit_share": 0.40,
    }


def _daily(start: int, progress: float) -> list[dict[str, object]]:
    return [
        {
            "session_day": start,
            "target_progress": progress / 2.0,
            "closing_mll_buffer": 4200.0,
        },
        {
            "session_day": start + 1,
            "target_progress": progress,
            "closing_mll_buffer": 3900.0,
        },
    ]


def _raw_rows() -> list[dict[str, object]]:
    starts = [
        _epoch_day("2023-01-02"),
        _epoch_day("2023-02-02"),
        _epoch_day("2023-03-02"),
        _epoch_day("2023-04-03"),
    ]
    progress_by_scenario = {
        "NORMAL": [0.20, 0.35, 0.55, 1.05],
        "STRESSED_1_5X": [0.15, 0.30, 0.50, 1.00],
    }
    output: list[dict[str, object]] = []
    for scenario, values in progress_by_scenario.items():
        for index, (start, progress) in enumerate(zip(starts, values, strict=True)):
            passed = index == 0
            output.append(
                {
                    "campaign_id": CAMPAIGN,
                    "policy_id": "replaced",
                    "scenario": scenario,
                    "horizon_label": "90_TRADING_DAYS",
                    "start_day": start,
                    "end_day": start + 20,
                    "eligible_days": 20,
                    "traded_days": 10 + index,
                    "terminal_classification": (
                        "TARGET_REACHED" if passed else "DATA_CENSORED"
                    ),
                    "passed": passed,
                    "mll_breached": False,
                    "censored": not passed,
                    "consistency_ok": index != 1,
                    "target_progress": progress,
                    "minimum_mll_buffer": 3000.0 + index * 100.0,
                    "net_pnl": progress * 9000.0,
                    "days_to_target": 18 if passed else None,
                    "accepted_events": 8,
                    "skipped_events": 2,
                    "maximum_mini_equivalent": 3.0,
                    "maximum_net_directional_exposure": 2.0,
                    "risk_allocation_path": [
                        {
                            "event_id": f"{scenario}:{start}:entry-a",
                            "quantity": 2,
                            "decision_status": "ACCEPTED",
                        },
                        {
                            "event_id": f"{scenario}:{start}:entry-b",
                            "quantity": 1,
                            "decision_status": "SIZE_REDUCED",
                        },
                    ],
                    "daily_path": _daily(start, progress),
                }
            )
    return output


def _xfa_path(*, net: float) -> dict[str, object]:
    return {
        "observed_days": 20,
        "payout_eligible": True,
        "payout_cycles": 1,
        "trader_net_payout": net,
        "post_payout_survived": True,
        "post_payout_censored": False,
        "minimum_mll_buffer": 2500.0,
        "start_day": 20000,
        "first_payout_day": 20009,
    }


def _candidate(policy_id: str) -> dict[str, object]:
    normal = _summary(stressed=False)
    stressed = _summary(stressed=True)
    raw = _raw_rows()
    for row in raw:
        row["policy_id"] = policy_id
    horizons = {
        "normal": {label: dict(normal) for label in HORIZONS},
        "stressed": {label: dict(stressed) for label in HORIZONS},
    }
    return {
        "schema": "hydra_active_risk_policy_metric_v1",
        "policy_id": policy_id,
        "structural_fingerprint": f"struct-{policy_id}",
        "actual_account_behavior_fingerprint": f"behavior-{policy_id}",
        "normal": normal,
        "stressed": stressed,
        "horizons": horizons,
        "risk_utilisation": {
            "observation_count": 100,
            "mean": 0.40,
            "median": 0.35,
            "p25": 0.10,
            "p75": 0.70,
            "by_active_sleeve_count": {
                "zero": {"observation_count": 20, "mean": 0.0, "median": 0.0},
                "one": {"observation_count": 50, "mean": 0.4, "median": 0.4},
                "two": {"observation_count": 30, "mean": 2 / 3, "median": 0.7},
                "three_or_more": {
                    "observation_count": 0,
                    "mean": 0.0,
                    "median": 0.0,
                },
            },
        },
        "suppression": {
            "signals_emitted": 20,
            "signals_accepted": 16,
            "signals_rejected": 4,
            "decision_status_counts": {
                "ACCEPTED": 14,
                "SIZE_REDUCED": 2,
                "CONFLICT_REJECTED": 4,
            },
            "foregone_realized_pnl_ex_post": 125.0,
        },
        "evidence_raw": raw,
        "lifecycle_rows": [
            {
                "scenario": "NORMAL",
                "standard": _xfa_path(net=900.0),
                "consistency": _xfa_path(net=450.0),
            },
            {
                "scenario": "STRESSED_1_5X",
                "standard": _xfa_path(net=450.0),
                "consistency": _xfa_path(net=225.0),
            },
        ],
    }


def _control(policy_id: str, *, target: float = 0.20) -> dict[str, object]:
    normal = _summary(stressed=False)
    stressed = _summary(stressed=True)
    normal["target_progress_median"] = target + 0.05
    stressed["target_progress_median"] = target
    return {
        "policy_id": policy_id,
        "normal": normal,
        "stressed": stressed,
        "horizons": {
            "normal": {label: dict(normal) for label in HORIZONS},
            "stressed": {label: dict(stressed) for label in HORIZONS},
        },
    }


def _decision(stage: str, selected: list[str]) -> dict[str, object]:
    value = {
        "stage": stage,
        "input_count": 2,
        "eligible_count": len(selected),
        "output_limit": 32,
        "output_count": len(selected),
        "selected_policy_ids": selected,
        "excluded": [],
        "development_only": True,
    }
    value["decision_hash"] = canonical_hash(value)
    return value


def _fixture(tmp_path: Path) -> dict[str, Path]:
    manifest_path = tmp_path / "manifest.json"
    stage3 = tmp_path / "stage3"
    controls_path = tmp_path / "matched_controls.json"
    halving = tmp_path / "halving"
    blocks = [
        ("B1", "2023-01-01", "2023-01-31"),
        ("B2", "2023-02-01", "2023-02-28"),
        ("B3", "2023-03-01", "2023-03-31"),
        ("B4", "2023-04-01", "2023-04-30"),
    ]
    _write(
        manifest_path,
        {
            "campaign_id": CAMPAIGN,
            "temporal_blocks": {
                "blocks": [
                    {
                        "block_id": block_id,
                        "start": start,
                        "end": end,
                        "markets": ["NQ"],
                        "contract_separation": "EXPLICIT",
                    }
                    for block_id, start, end in blocks
                ]
            },
        },
    )
    candidates = [_candidate("candidate-a"), _candidate("candidate-b")]
    for index, candidate in enumerate(candidates):
        rows = [candidate]
        _write(
            stage3 / f"batch_{index:06d}.json",
            {
                "schema": "hydra_active_risk_stage_batch_v1",
                "stage": "stage3",
                "rows": rows,
                "rows_hash": canonical_hash(rows),
            },
        )
    static = _control("control:static", target=0.20)
    standalone = _control("control:standalone", target=0.25)
    equal = _control("control:equal", target=0.30)
    always = _control("control:always", target=0.32)
    random_controls = {
        candidate["policy_id"]: _control(
            f"control:random:{candidate['policy_id']}", target=0.28
        )
        for candidate in candidates
    }
    matches = {
        candidate["policy_id"]: {
            "matched_policy_id": candidate["policy_id"],
            "control_id": random_controls[candidate["policy_id"]]["policy_id"],
            "matched": True,
            "relative_tolerance": 0.05,
            "deltas": {},
            "economic_outcomes_used_for_selection": False,
        }
        for candidate in candidates
    }
    controls = {
        "schema": "hydra_active_risk_matched_controls_v1",
        "campaign_id": CAMPAIGN,
        "static_partition": static,
        "standalone_controls": [standalone],
        "best_standalone": standalone,
        "equal_risk_active_pool": equal,
        "always_on_pooled_governor": always,
        "random_priority_by_policy": random_controls,
        "random_priority_exposure_match_by_policy": matches,
        "matched_controls_status": "EXECUTED_EXPOSURE_MATCHED",
        "random_priority_exposure_matched": True,
        "random_priority_exposure_match_rate": 1.0,
        "random_priority_fixed_seeds": [1, 2],
        "development_only": True,
    }
    controls["controls_hash"] = canonical_hash(controls)
    _write(controls_path, controls)
    _write(halving / "stage3.json", _decision("ACTIVE_POOL_STAGE_3_TO_96", ["candidate-a", "candidate-b"]))
    _write(halving / "stage4.json", _decision("ACTIVE_POOL_EXPANDED_CONFIRMATION_GATE", ["candidate-a"]))
    _write(halving / "stage5.json", _decision("ACTIVE_POOL_EXPANDED_CONFIRMATION_GATE", ["candidate-a"]))
    return {
        "manifest": manifest_path,
        "stage3": stage3,
        "controls": controls_path,
        "halving": halving,
    }


def test_canonical_hash_matches_runtime_stable_hash() -> None:
    value = {"z": [1, 2.5, {"accent": "é"}], "a": True}
    assert canonical_hash(value) == stable_hash(value)


def test_streaming_report_covers_blocks_controls_xfa_and_clusters(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    report = build_active_risk_decision_report(
        manifest_path=paths["manifest"],
        stage3_cache_dir=paths["stage3"],
        matched_controls_path=paths["controls"],
        halving_dir=paths["halving"],
        expected_stage3_count=2,
    )

    assert report["integrity"]["stage3_validated_policy_count"] == 2
    assert report["temporal_blocks"]["results"]["normal"]["B1"]["pass_count"] == 2
    assert report["temporal_blocks"]["results"]["stressed"]["B4"]["episode_count"] == 2
    assert report["risk_utilisation"]["observation_count"] == 200
    assert report["suppression_and_foregone_pnl"]["signals_rejected"] == 8
    assert report["suppression_and_foregone_pnl"]["foregone_realized_pnl_ex_post"] == 250.0

    candidate = report["candidates"][0]
    static_delta = candidate["control_deltas"]["static_partition"]["stressed"]
    assert static_delta["target_progress_median"] == pytest.approx(0.20)
    assert candidate["control_deltas"]["matched_random_priority"]["exposure_matching"]["matched"]

    normal_standard = report["xfa_lifecycle"]["normal"]["standard"]
    normal_consistency = report["xfa_lifecycle"]["normal"]["consistency"]
    stressed_standard = report["xfa_lifecycle"]["stressed"]["standard"]
    assert normal_standard["combine_attempts"] == 8
    assert normal_standard["xfa_paths_started"] == 2
    assert normal_standard["first_payouts"] == 2
    assert normal_standard["expected_trader_payout_per_combine_attempt"] == 225.0
    assert normal_consistency["expected_trader_payout_per_combine_attempt"] == 112.5
    assert stressed_standard["expected_trader_payout_per_combine_attempt"] == 112.5

    clustering = report["posthoc_behavioral_clustering"]
    assert clustering["candidate_count"] == 2
    assert clustering["cluster_count"] == 1
    assert clustering["clusters"][0]["member_ids"] == ["candidate-a", "candidate-b"]
    assert clustering["promotion_or_selection_effect"] is False

    checked = dict(report)
    claimed = checked.pop("report_hash")
    assert canonical_hash(checked) == claimed
    markdown = render_markdown(report)
    assert "Expected trader payout" in markdown
    assert "B1" in markdown
    assert "candidate-a" in markdown


def test_stage3_rows_hash_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000001.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["rows"][0]["normal"]["net_total"] += 1.0
    _write(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match="rows_hash drift"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_same_economics_with_different_routed_trades_split_clusters(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000001.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    for raw in payload["rows"][0]["evidence_raw"]:
        for decision in raw["risk_allocation_path"]:
            decision["event_id"] = "distinct:" + decision["event_id"]
            decision["quantity"] += 5
    payload["rows_hash"] = canonical_hash(payload["rows"])
    _write(cache, payload)

    report = build_active_risk_decision_report(
        manifest_path=paths["manifest"],
        stage3_cache_dir=paths["stage3"],
        matched_controls_path=paths["controls"],
        halving_dir=paths["halving"],
        expected_stage3_count=2,
    )
    clustering = report["posthoc_behavioral_clustering"]
    assert clustering["cluster_count"] == 2
    assert sorted(row["member_count"] for row in clustering["clusters"]) == [1, 1]
    assert (
        clustering["method"]["routing_tuple_jaccard_minimum"] == 0.90
    )


def test_nonpromoted_policy_does_not_retain_behavior_vector_or_routing_signature(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    _write(
        paths["halving"] / "stage3.json",
        _decision("ACTIVE_POOL_STAGE_3_TO_96", ["candidate-a"]),
    )
    report = build_active_risk_decision_report(
        manifest_path=paths["manifest"],
        stage3_cache_dir=paths["stage3"],
        matched_controls_path=paths["controls"],
        halving_dir=paths["halving"],
        expected_stage3_count=2,
    )
    by_id = {row["policy_id"]: row for row in report["candidates"]}
    assert "posthoc_behavior_vector_hash" in by_id["candidate-a"]
    assert "posthoc_routing_tuple_hash" in by_id["candidate-a"]
    assert "posthoc_behavior_vector_hash" not in by_id["candidate-b"]
    assert "posthoc_routing_tuple_hash" not in by_id["candidate-b"]
    assert report["posthoc_behavioral_clustering"]["candidate_count"] == 1


def test_cli_defaults_use_revision_02_manifest_and_report_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    def fake_build(**kwargs: object) -> dict[str, object]:
        captured["build"] = kwargs
        return {"report_hash": "hash"}

    def fake_write(
        report: object, *, json_path: Path, markdown_path: Path
    ) -> None:
        captured["write"] = {
            "report": report,
            "json_path": json_path,
            "markdown_path": markdown_path,
        }

    monkeypatch.setattr(report_module, "build_active_risk_decision_report", fake_build)
    monkeypatch.setattr(report_module, "write_active_risk_decision_report", fake_write)
    assert report_module.main(["--root", str(tmp_path)]) == 0
    build = captured["build"]
    assert isinstance(build, dict)
    assert build["manifest_path"] == (
        tmp_path / "config/v7/active_risk_pool_target_velocity_0026_revision_02.json"
    )
    assert build["matched_controls_path"] == (
        tmp_path
        / "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02"
        / "matched_controls.json"
    )
    written = captured["write"]
    assert isinstance(written, dict)
    assert written["json_path"].parent.name == (
        "active_risk_pool_target_velocity_0026_revision_02"
    )
