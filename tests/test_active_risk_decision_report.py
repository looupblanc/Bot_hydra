from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import EvidenceBundleWriter
from hydra.production import active_risk_decision_report as report_module
from hydra.production.active_risk_decision_report import (
    ActiveRiskDecisionReportError,
    build_active_risk_decision_report,
    canonical_hash,
    render_markdown,
)
from hydra.production.active_risk_runtime import (
    ACTIVE_RISK_XFA_EVIDENCE_SCHEMA,
    ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
    ACTIVE_RISK_XFA_OVERLAY_VERSION,
    _XFA_PATH_HASH_FIELDS,
    _aggregate_horizons,
    _aggregate_suppression,
    _aggregate_utilisation,
    _evidence_rows,
)
from hydra.propfirm.combine_to_xfa import (
    LIFECYCLE_VERSION,
    UNREALIZED_AGGREGATION_SEMANTICS,
    FrozenRiskProfile,
    _zero_observation_xfa_path,
    official_rule_snapshot_2026_07_15,
)


CAMPAIGN = "hydra_active_risk_pool_target_velocity_0026"
SOURCE_COMMIT = "d" * 40
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
    scenario = "STRESSED_1_5X" if stressed else "NORMAL"
    rows = [
        row
        for row in _raw_rows()
        if row["scenario"] == scenario
        and row["horizon_label"] == "90_TRADING_DAYS"
    ]
    return _summary_from_rows(rows)


def _daily(
    start: int,
    progress: float,
    *,
    net_pnl: float,
    minimum_mll_buffer: float,
    consistency_ok: bool,
    routing: list[dict[str, object]],
    total_cost: float,
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for index in range(20):
        fraction = (index + 1) / 20.0
        cumulative = net_pnl * fraction
        balance = 150_000.0 + cumulative
        day_cost = total_cost / 20.0
        output.append(
            {
                "session_day": start + index,
                "realized_pnl": cumulative,
                "unrealized_pnl": 0.0,
                "day_pnl": net_pnl / 20.0,
                "balance": balance,
                "mll_floor": balance - minimum_mll_buffer,
                "closing_mll_buffer": minimum_mll_buffer,
                "minimum_mll_buffer": minimum_mll_buffer,
                "consistency_ok": consistency_ok,
                "target_progress": progress * fraction,
                "costs": day_cost,
                "conflicts": [],
                "exposure": {
                    "maximum_mini_equivalent": 3.0 if index == 0 else 0.0,
                    "maximum_net_directional": 2.0 if index == 0 else 0.0,
                },
                "component_attribution": {"sleeve": net_pnl / 20.0},
                "routing_decisions": routing if index == 0 else [],
                "cumulative_costs": day_cost * (index + 1),
            }
        )
    return output


def _raw_rows() -> list[dict[str, object]]:
    starts = [
        _epoch_day((date(2023, month, 2) + timedelta(days=offset)).isoformat())
        for month in range(1, 5)
        for offset in range(12)
    ]
    base_progress = {
        "NORMAL": (0.20, 0.35, 0.55),
        "STRESSED_1_5X": (0.15, 0.30, 0.50),
    }
    output: list[dict[str, object]] = []
    for horizon_label, duration in (
        ("20_TRADING_DAYS", 20),
        ("40_TRADING_DAYS", 40),
        ("60_TRADING_DAYS", 60),
        ("90_TRADING_DAYS", 90),
        ("FULL_CHRONOLOGICAL_HORIZON", 120),
    ):
        for scenario, progress_cycle in base_progress.items():
            for index, start in enumerate(starts):
                passed = index == 0
                progress = 1.05 if passed else progress_cycle[(index - 1) % 3]
                terminal = (
                    "TARGET_REACHED"
                    if passed
                    else "DATA_CENSORED"
                    if horizon_label == "FULL_CHRONOLOGICAL_HORIZON"
                    else "OPERATIONAL_HORIZON_NOT_REACHED"
                )
                eligible_days = 20
                end_day = start + eligible_days - 1
                minimum_buffer = 3000.0 + (index % 4) * 100.0
                consistency_ok = index % 4 != 1
                net_pnl = progress * 9000.0
                total_cost = 20.0
                routing = [
                    {
                        "event_id": f"{scenario}:{horizon_label}:{start}:entry-a",
                        "component_id": "sleeve",
                        "decision_ns": start * 86_400_000_000_000 + 1,
                        "exit_ns": start * 86_400_000_000_000 + 2,
                        "allow": True,
                        "accepted": True,
                        "mini_equivalent": 2.0,
                        "risk_before": {
                            "utilisation": 0.0,
                            "active_sleeve_count": 0,
                        },
                        "risk_after": {
                            "utilisation": 0.5,
                            "active_sleeve_count": 1,
                        },
                        "quantity": 2,
                        "decision_status": "ACCEPTED",
                    },
                    {
                        "event_id": f"{scenario}:{horizon_label}:{start}:entry-b",
                        "component_id": "sleeve",
                        "decision_ns": start * 86_400_000_000_000 + 3,
                        "exit_ns": start * 86_400_000_000_000 + 4,
                        "allow": True,
                        "accepted": True,
                        "mini_equivalent": 1.0,
                        "risk_before": {
                            "utilisation": 0.5,
                            "active_sleeve_count": 1,
                        },
                        "risk_after": {
                            "utilisation": 0.75,
                            "active_sleeve_count": 2,
                        },
                        "quantity": 1,
                        "decision_status": "SIZE_REDUCED",
                    },
                ]
                output.append({
                    "campaign_id": CAMPAIGN,
                    "policy_id": "replaced",
                    "scenario": scenario,
                    "horizon_label": horizon_label,
                    "horizon_trading_days": duration,
                    "start_day": start,
                    "end_day": end_day,
                    "eligible_days": eligible_days,
                    "traded_days": 1,
                    "terminal_classification": terminal,
                    "passed": passed,
                    "mll_breached": False,
                    "censored": terminal == "DATA_CENSORED",
                    "consistency_ok": consistency_ok,
                    "target_progress": progress,
                    "maximum_target_progress": progress,
                    "minimum_mll_buffer": minimum_buffer,
                    "net_pnl": net_pnl,
                    "total_cost": total_cost,
                    "days_to_target": 18 if passed else None,
                    "component_contribution": {"sleeve": progress * 9000.0},
                    "accepted_events": 2,
                    "skipped_events": 0,
                    "maximum_mini_equivalent": 3.0,
                    "maximum_net_directional_exposure": 2.0,
                    "risk_allocation_path": routing,
                    "daily_path": _daily(
                        start,
                        progress,
                        net_pnl=net_pnl,
                        minimum_mll_buffer=minimum_buffer,
                        consistency_ok=consistency_ok,
                        routing=routing,
                        total_cost=total_cost,
                    ),
                })
    return output


def _quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _block_id(start_day: int) -> str:
    month = (date(1970, 1, 1) + timedelta(days=start_day)).month
    return f"B{month}"


def _summary_from_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    net = [float(row["net_pnl"]) for row in rows]
    progress = [float(row["target_progress"]) for row in rows]
    durations = [int(row["eligible_days"]) for row in rows]
    active = [int(row["traded_days"]) for row in rows]
    calendars = [int(row["end_day"]) - int(row["start_day"]) + 1 for row in rows]
    target_days = [int(row["days_to_target"]) for row in rows if row["days_to_target"] is not None]
    terminals = Counter(str(row["terminal_classification"]) for row in rows)
    by_block_net: dict[str, float] = defaultdict(float)
    by_block_progress: dict[str, list[float]] = defaultdict(list)
    contribution: dict[str, float] = defaultdict(float)
    pass_blocks: set[str] = set()
    for row in rows:
        block = _block_id(int(row["start_day"]))
        by_block_net[block] += float(row["net_pnl"])
        by_block_progress[block].append(float(row["target_progress"]))
        if row["terminal_classification"] == "TARGET_REACHED":
            pass_blocks.add(block)
        for component_id, value in dict(row["component_contribution"]).items():
            contribution[str(component_id)] += float(value)
    projected_active = [days / value for days, value in zip(active, progress, strict=True) if value > 0]
    projected_calendar = [days / value for days, value in zip(calendars, progress, strict=True) if value > 0]
    positive_blocks = sum(max(value, 0.0) for value in by_block_net.values())
    positive_components = sum(max(value, 0.0) for value in contribution.values())
    passes = terminals["TARGET_REACHED"]
    breaches = terminals["MLL_BREACHED"]
    censored = terminals["DATA_CENSORED"] + terminals["OPERATIONAL_HORIZON_NOT_REACHED"]
    return {
        "episode_count": len(rows),
        "pass_count": passes,
        "pass_rate": passes / len(rows),
        "mll_breach_count": breaches,
        "mll_breach_rate": breaches / len(rows),
        "censored_episode_count": censored,
        "censoring_rate": censored / len(rows),
        "terminal_distribution": dict(sorted(terminals.items())),
        "net_total": sum(net),
        "net_median": statistics.median(net),
        "net_values": net,
        "target_progress_median": statistics.median(progress),
        "target_progress_p25": _quantile(progress, 0.25),
        "target_progress_values": progress,
        "maximum_target_progress": max(progress),
        "minimum_mll_buffer": min(float(row["minimum_mll_buffer"]) for row in rows),
        "consistency_rate": sum(bool(row["consistency_ok"]) for row in rows) / len(rows),
        "consistency_ok_count": sum(bool(row["consistency_ok"]) for row in rows),
        "duration_trading_days_values": durations,
        "duration_trading_days_median": statistics.median(durations),
        "active_trading_days_values": active,
        "active_trading_days_median": statistics.median(active),
        "calendar_days_values": calendars,
        "calendar_days_median": statistics.median(calendars),
        "days_to_target_values": target_days,
        "median_days_to_target": statistics.median(target_days) if target_days else None,
        "projected_active_days_to_target_median": statistics.median(projected_active),
        "projected_calendar_days_to_target_median": statistics.median(projected_calendar),
        "monthly_subscription_duration_proxy_median": statistics.median(projected_calendar) / 30.0,
        "pass_block_count": len(pass_blocks),
        "pass_block_ids": sorted(pass_blocks),
        "by_block_net": dict(sorted(by_block_net.items())),
        "by_block_target_progress_median": {
            key: statistics.median(values) for key, values in sorted(by_block_progress.items())
        },
        "component_contribution": dict(sorted(contribution.items())),
        "maximum_block_profit_share": max(by_block_net.values()) / positive_blocks,
        "maximum_sleeve_profit_share": max(contribution.values()) / positive_components,
    }


def _xfa_path(*, path: str, net: float, start_day: int) -> dict[str, object]:
    ledger = [
        {
            "session_day": start_day + index,
            "accepted_events": 8 if index == 0 else 0,
            "skipped_events": 2 if index == 0 else 0,
            "payout_requested": index == 9,
            "gross_payout": net / 0.9 if index == 9 else 0.0,
            "trader_net_payout": net if index == 9 else 0.0,
        }
        for index in range(120)
    ]
    value: dict[str, object] = {
        "path": path,
        "terminal": "SURVIVED_HORIZON",
        "terminal_reason": "completed_frozen_xfa_horizon",
        "start_day": start_day,
        "end_day": start_day + 119,
        "requested_horizon_days": 120,
        "observed_days": 120,
        "traded_days": 10,
        "event_count": 10,
        "accepted_event_count": 8,
        "skipped_event_count": 2,
        "payout_eligible": True,
        "payout_cycles": 1,
        "gross_payout": net / 0.9,
        "trader_net_payout": net,
        "first_payout_day": 10,
        "post_payout_survived": True,
        "post_payout_censored": False,
        "post_payout_observed_days": 110,
        "ending_balance": 3000.0,
        "ending_mll_floor": 0.0,
        "minimum_mll_buffer": 2500.0,
        "qualifying_winning_days": 5,
        "maximum_consistency_ratio": 0.35,
        "maximum_mini_equivalent": 3.0,
        "total_cost": 100.0,
        "skipped_reasons": {"CONFLICT": 2},
        "component_contribution": {"sleeve": net},
        "daily_ledger": ledger,
        "calendar_inactivity_auditable": True,
        "payout_request_policy": "FIRST_ELIGIBLE_DAY_PER_CYCLE",
        "payout_path_selected_from_outcomes": False,
    }
    value["path_hash"] = canonical_hash(
        {field: value[field] for field in _XFA_PATH_HASH_FIELDS}
    )
    return value


def _lifecycle(
    *, policy_id: str, scenario: str, combine_start: int, net: float
) -> dict[str, object]:
    xfa_start = combine_start + 20
    profile = FrozenRiskProfile(profile_id=f"{policy_id}:XFA_PROFILE")
    value: dict[str, object] = {
        "schema": ACTIVE_RISK_XFA_EVIDENCE_SCHEMA,
        "lifecycle_version": LIFECYCLE_VERSION,
        "overlay_version": ACTIVE_RISK_XFA_OVERLAY_VERSION,
        "policy_id": policy_id,
        "scenario": scenario,
        "combine_start_day": combine_start,
        "combine_end_day": combine_start + 19,
        "combine_status": "TARGET_REACHED",
        "combine_horizon": "FULL_CHRONOLOGICAL_HORIZON",
        "xfa_start_day": xfa_start,
        "xfa_horizon_days": 120,
        "xfa_profile": profile.to_dict(),
        "xfa_profile_projection": {
            "risk_multiplier": profile.risk_multiplier,
            "maximum_simultaneous_positions": profile.maximum_simultaneous_positions,
            "maximum_mini_equivalent": profile.maximum_mini_equivalent,
            "clip_to_official_xfa_scaling_plan": profile.clip_to_xfa_scaling_plan,
            "same_market_exclusive": profile.same_market_exclusive,
        },
        "rule_snapshot": official_rule_snapshot_2026_07_15().to_dict(),
        "standard": _xfa_path(path="XFA_STANDARD", net=net, start_day=xfa_start),
        "consistency": _xfa_path(
            path="XFA_CONSISTENCY", net=net / 2.0, start_day=xfa_start
        ),
        "combine_profit_transferred_to_xfa": False,
        "xfa_profile_frozen_before_replay": True,
        "xfa_profile_selected_from_outcomes": False,
        "xfa_overlay_semantics": ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
        "combine_governor_controls_applied_in_xfa": False,
        "payout_path_oracle_used": False,
        "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
        "development_only": True,
    }
    value["source_lifecycle_sha256"] = canonical_hash(value)
    return value


def _candidate(policy_id: str) -> dict[str, object]:
    raw = _raw_rows()
    for row in raw:
        row["policy_id"] = policy_id
    horizons = {
        scenario_key: {
            label: _summary_from_rows(
                [
                    item
                    for item in raw
                    if item["scenario"] == scenario
                    and item["horizon_label"] == label
                ]
            )
            for label in HORIZONS
        }
        for scenario_key, scenario in (
            ("normal", "NORMAL"),
            ("stressed", "STRESSED_1_5X"),
        )
    }
    normal = horizons["normal"]["90_TRADING_DAYS"]
    stressed = horizons["stressed"]["90_TRADING_DAYS"]
    normal_start = _epoch_day("2023-01-02")
    stressed_start = _epoch_day("2023-01-02")
    derived_diagnostics = report_module._derive_canonical_account_diagnostics(
        [value for value in raw if value["horizon_label"] == "90_TRADING_DAYS"]
    )
    return {
        "schema": "hydra_active_risk_policy_metric_v1",
        "policy_id": policy_id,
        "structural_fingerprint": canonical_hash(
            {"policy_id": policy_id, "kind": "structure"}
        ),
        "actual_account_behavior_fingerprint": canonical_hash(
            {"policy_id": policy_id, "kind": "behavior"}
        ),
        "normal": normal,
        "stressed": stressed,
        "horizons": horizons,
        "risk_utilisation": derived_diagnostics["risk_utilisation"],
        "exposure_signature": derived_diagnostics["exposure_signature"],
        "suppression": derived_diagnostics["suppression"],
        "evidence_raw": raw,
        "lifecycle_rows": [
            _lifecycle(
                policy_id=policy_id,
                scenario="NORMAL",
                combine_start=normal_start,
                net=900.0,
            ),
            _lifecycle(
                policy_id=policy_id,
                scenario="STRESSED_1_5X",
                combine_start=stressed_start,
                net=450.0,
            ),
        ],
    }


def _control(policy_id: str, *, target: float = 0.20) -> dict[str, object]:
    normal = _summary(stressed=False)
    stressed = _summary(stressed=True)
    normal["target_progress_median"] = target + 0.05
    stressed["target_progress_median"] = target
    exposure_signature = report_module._derive_canonical_account_diagnostics(
        [
            value
            for value in _raw_rows()
            if value["horizon_label"] == "90_TRADING_DAYS"
        ]
    )["exposure_signature"]
    return {
        "policy_id": policy_id,
        "normal": normal,
        "stressed": stressed,
        "horizons": {
            "normal": {label: dict(normal) for label in HORIZONS},
            "stressed": {label: dict(stressed) for label in HORIZONS},
        },
        "exposure_signature": exposure_signature,
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


def _reseal_lifecycle(value: dict[str, object]) -> None:
    for name in ("standard", "consistency"):
        path = value[name]
        assert isinstance(path, dict)
        path["path_hash"] = canonical_hash(
            {field: path[field] for field in _XFA_PATH_HASH_FIELDS}
        )
    value.pop("source_lifecycle_sha256", None)
    value["source_lifecycle_sha256"] = canonical_hash(value)


def _reseal_batch(path: Path, payload: dict[str, object]) -> None:
    rows = payload["rows"]
    assert isinstance(rows, list)
    payload["rows_hash"] = canonical_hash(rows)
    _write(path, payload)


def _seal_test_evidence_bundle(
    tmp_path: Path,
    *,
    manifest_path: Path,
    economic_results: dict[str, object],
    stage_decisions: list[dict[str, object]],
    candidates: list[dict[str, object]],
) -> dict[str, object]:
    shared_root = tmp_path.parent / "_active_risk_report_bundle"
    shared_receipt_path = shared_root / "evidence_bundle_receipt.json"
    if shared_receipt_path.is_file():
        return json.loads(shared_receipt_path.read_text(encoding="utf-8"))
    policy_ids = tuple(str(candidate["policy_id"]) for candidate in candidates)
    component_id = "sleeve"
    configuration_sha256 = report_module.file_sha256(manifest_path)
    data_sha256 = "c" * 64
    starts = sorted(
        {
            int(row["start_day"])
            for row in _raw_rows()
            if row["horizon_label"] == "90_TRADING_DAYS"
            and row["scenario"] == "NORMAL"
        }
    )
    identity = {
        "campaign_id": CAMPAIGN,
        "grammar_id": "active_risk_pool_target_velocity_v1",
        "policy_fingerprints": {
            str(candidate["policy_id"]): str(candidate["structural_fingerprint"])
            for candidate in candidates
        },
        "component_fingerprints": {
            component_id: canonical_hash({"component_id": component_id})
        },
        "source_commit": SOURCE_COMMIT,
        "data_fingerprints": {"cached_development": data_sha256},
        "configuration_sha256": configuration_sha256,
        "seeds": [250026],
        "created_at_utc": "2026-07-15T00:00:00Z",
        "expected_coverage": {
            "policy_ids": list(policy_ids),
            "component_ids": [component_id],
            "required_episode_keys": [
                {
                    "policy_id": policy_id,
                    "episode_id": f"{policy_id}:{starts[0]}",
                    "horizon": "20_TRADING_DAYS",
                }
                for policy_id in policy_ids
            ],
            "allowed_horizons": list(HORIZONS),
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": True,
        },
    }
    signal = {
        "campaign_id": CAMPAIGN,
        "component_id": component_id,
        "signal_id": "signal-001",
        "event_time": "2023-01-02T14:30:00Z",
        "market": "NQ",
        "contract": "NQH3",
        "timeframe": "1m",
        "signal": 1,
        "sizing": 1.0,
        "stop": 10990.0,
        "target": 11020.0,
        "veto": False,
        "component_role": "TARGET_VELOCITY",
    }
    entry = {
        "campaign_id": CAMPAIGN,
        "component_id": component_id,
        "trade_id": "trade-001",
        "entry_time": "2023-01-02T14:31:00Z",
        "market": "NQ",
        "contract": "NQH3",
        "side": "LONG",
        "quantity": 1.0,
        "entry_price": 11000.0,
        "sizing": 1.0,
        "stop_price": 10990.0,
        "target_price": 11020.0,
    }
    exit_row = {
        "campaign_id": CAMPAIGN,
        "component_id": component_id,
        "trade_id": "trade-001",
        "exit_time": "2023-01-02T15:00:00Z",
        "exit_price": 11005.0,
        "exit_reason": "TARGET_HORIZON_EXIT",
    }
    trade = {
        "campaign_id": CAMPAIGN,
        "component_id": component_id,
        "trade_id": "trade-001",
        "entry_time": entry["entry_time"],
        "exit_time": exit_row["exit_time"],
        "market": entry["market"],
        "contract": entry["contract"],
        "side": entry["side"],
        "quantity": entry["quantity"],
        "entry_price": entry["entry_price"],
        "exit_price": exit_row["exit_price"],
        "gross_pnl": 25.0,
        "costs": 2.5,
        "net_pnl": 22.5,
    }
    memberships = [
        {
            "campaign_id": CAMPAIGN,
            "policy_id": policy_id,
            "component_id": component_id,
            "risk_allocation": 1.0,
            "component_role": "TARGET_VELOCITY",
        }
        for policy_id in policy_ids
    ]
    records = {
        "component_signals": [signal],
        "component_entries": [entry],
        "component_exits": [exit_row],
        "component_trades": [trade],
        "account_policy_membership": memberships,
        "provenance": [
            {
                "campaign_id": CAMPAIGN,
                "validator_version": "evidence_bundle_v1",
                "replay_version": "active_risk_reference_v1",
                "market_data_role": "DEVELOPMENT_ONLY",
                "access_ledger_sha256": "a" * 64,
                "reconstruction_flag": False,
                "immutable_checksums": {
                    "configuration": configuration_sha256,
                    "data:cached_development": data_sha256,
                },
                "recorded_at_utc": "2026-07-15T01:00:00Z",
            }
        ],
    }
    writer = EvidenceBundleWriter.create(
        shared_root / "evidence_cache", identity, writer_id="test-writer"
    )
    for dataset, rows in records.items():
        writer.append_records(dataset, rows, batch_id=f"{dataset}-batch-0000")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for index, candidate in enumerate(candidates):
        episodes, daily_paths = _evidence_rows(candidate, manifest)
        writer.append_records(
            "episodes",
            episodes,
            batch_id=f"active:stage3:{index:06d}:episodes",
        )
        writer.append_records(
            "account_daily_paths",
            daily_paths,
            batch_id=f"active:stage3:{index:06d}:daily",
        )
    writer.write_compact_output("campaign_summary", economic_results)
    writer.write_compact_output("failure_vectors", [])
    writer.write_compact_output(
        "pareto_archive", {"stage_decisions": stage_decisions}
    )
    writer.write_compact_output(
        "next_campaign_recommendations",
        {"recommendation": {"action": "QUEUE_FROZEN_NEXT_ACTION"}},
    )
    receipt = writer.finalize(
        evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
        lightweight_manifest_path=shared_receipt_path,
    )
    return receipt.to_dict()


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
    manifest = {
        "campaign_id": CAMPAIGN,
        "source_commit": SOURCE_COMMIT,
        "episode_starts": {"serious_policy_starts": 48},
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
    }
    manifest["manifest_hash"] = canonical_hash(manifest)
    _write(manifest_path, manifest)
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
            "selected_seed": 1,
            "candidate_signature": dict(candidate["exposure_signature"]),
            "control_signature": dict(
                random_controls[candidate["policy_id"]]["exposure_signature"]
            ),
            "matched": True,
            "relative_tolerance": 0.05,
            "deltas": {
                field: {
                    "candidate": candidate["exposure_signature"][field],
                    "control": random_controls[candidate["policy_id"]][
                        "exposure_signature"
                    ][field],
                    "absolute_delta": 0.0,
                    "relative_delta": 0.0,
                }
                for field in (
                    "time_weighted_mini_nanoseconds_per_observed_day",
                    "accepted_event_rate",
                )
            },
            "selection_key_fields": [
                "time_weighted_mini_nanoseconds_per_observed_day",
                "accepted_event_rate",
            ],
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
        "random_priority_outcomes_used_for_matching": False,
        "development_only": True,
    }
    controls["controls_hash"] = canonical_hash(controls)
    _write(controls_path, controls)
    stage_decisions = [
        _decision("ACTIVE_POOL_STAGE_3_TO_96", ["candidate-a", "candidate-b"]),
        _decision("ACTIVE_POOL_EXPANDED_CONFIRMATION_GATE", ["candidate-a"]),
        _decision("ACTIVE_POOL_EXPANDED_CONFIRMATION_GATE", ["candidate-a"]),
    ]
    for stage_number, decision in zip(range(3, 6), stage_decisions, strict=True):
        _write(halving / f"stage{stage_number}.json", decision)
    economic_results: dict[str, object] = {
        "campaign_id": CAMPAIGN,
        "governor_proposals_generated": 20_000,
        "unique_policies_screened": 4_096,
        "exact_account_replays": 1_024,
        "stage3_policy_count": 2,
        "production_counters": {
            "combine_episodes_completed": 960,
            "normal_episodes_completed": 480,
            "stressed_episodes_completed": 480,
        },
        "identity_audit": {"passed": True},
        "matched_controls": controls,
        "normal_combine_passes": sum(
            int(candidate["normal"]["pass_count"]) for candidate in candidates
        ),
        "stressed_combine_passes": sum(
            int(candidate["stressed"]["pass_count"]) for candidate in candidates
        ),
        "normal_target_progress_median": statistics.median(
            float(candidate["normal"]["target_progress_median"])
            for candidate in candidates
        ),
        "stressed_target_progress_median": statistics.median(
            float(candidate["stressed"]["target_progress_median"])
            for candidate in candidates
        ),
        "stressed_mll_breach_rate_maximum": max(
            float(candidate["stressed"]["mll_breach_rate"])
            for candidate in candidates
        ),
        "risk_utilisation": _aggregate_utilisation(candidates),
        "suppression": _aggregate_suppression(candidates),
        "horizon_frontier": _aggregate_horizons(candidates),
    }
    evidence_receipt = _seal_test_evidence_bundle(
        tmp_path,
        manifest_path=manifest_path,
        economic_results=economic_results,
        stage_decisions=stage_decisions,
        candidates=candidates,
    )
    bundle_path = Path(str(evidence_receipt["bundle_path"]))
    production_state = {
        "campaign_id": CAMPAIGN,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": SOURCE_COMMIT,
        "state": "COMPLETE",
        "stage": "ACTIVE_RISK_CAMPAIGN_COMPLETE",
        "identity_audit_status": "PASS",
        "policies_proposed": 20_000,
        "unique_policies_screened": 4_096,
        "exact_account_replays": 1_024,
        "combine_episodes_completed": 960,
        "normal_episodes_completed": 480,
        "stressed_episodes_completed": 480,
        "evidence_final_path": str(bundle_path),
        "evidence_bundle_path": str(bundle_path),
        "evidence_bundle_manifest_sha256": evidence_receipt["manifest_sha256"],
        "next_action": "QUEUE_FROZEN_NEXT_ACTION",
    }
    production_state["state_hash"] = canonical_hash(production_state)
    state_path = tmp_path / "production_state.json"
    _write(state_path, production_state)
    final_result = {
        "campaign_id": CAMPAIGN,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": SOURCE_COMMIT,
        "status": "COMPLETE",
        "scientific_status": "DEVELOPMENT_COMPLETE",
        "economic_results": economic_results,
        "successive_halving": {"stage_decisions": stage_decisions},
        "matched_controls": controls,
        "evidence_bundle": evidence_receipt,
        "evidence_verification_manifest_sha256": evidence_receipt["manifest_sha256"],
        "autonomous_next_action": {"action": "QUEUE_FROZEN_NEXT_ACTION"},
    }
    final_result["result_hash"] = canonical_hash(final_result)
    result_path = tmp_path / "economic_production_result.json"
    _write(result_path, final_result)
    return {
        "manifest": manifest_path,
        "stage3": stage3,
        "controls": controls_path,
        "halving": halving,
        "state": state_path,
        "result": result_path,
        "bundle": bundle_path,
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
    assert report["integrity"]["exact_48_starts_per_scenario_and_policy"] is True
    normal_90 = report["horizon_distributions"]["normal"]["90_TRADING_DAYS"]
    assert normal_90["pass_rate_raw_lower_bound"] == pytest.approx(2 / 96)
    assert normal_90["pass_rate_evaluable"] == pytest.approx(2 / 96)
    assert normal_90["data_censored_episode_count"] == 0
    assert normal_90["operational_horizon_not_reached_count"] == 94
    normal_full = report["horizon_distributions"]["normal"][
        "FULL_CHRONOLOGICAL_HORIZON"
    ]
    assert normal_full["pass_rate_raw_lower_bound"] == pytest.approx(2 / 96)
    assert normal_full["pass_rate_evaluable"] == 1.0
    assert normal_full["data_censored_episode_count"] == 94
    assert normal_full["operational_horizon_not_reached_count"] == 0
    assert report["temporal_blocks"]["results"]["normal"]["B1"]["pass_count"] == 2
    assert report["temporal_blocks"]["results"]["stressed"]["B4"]["episode_count"] == 24
    assert report["risk_utilisation"]["observation_count"] == 384
    assert report["risk_utilisation"]["scope"].startswith("NORMAL_CANONICAL_90_DAY")
    assert (
        report["risk_utilisation"]["by_active_sleeve_count"]["three_or_more"][
            "policy_median_distribution"
        ]["count"]
        == 0
    )
    assert report["duty_and_exposure_match_evidence"]["policy_count"] == 2
    assert report["suppression_and_foregone_pnl"]["signals_rejected"] == 0
    assert report["suppression_and_foregone_pnl"]["foregone_realized_pnl_ex_post"] == 0.0

    candidate = report["candidates"][0]
    static_delta = candidate["control_deltas"]["static_partition"]["stressed"]
    assert static_delta["target_progress_median"] == pytest.approx(0.10)
    assert candidate["control_deltas"]["matched_random_priority"]["exposure_matching"]["matched"]

    normal_standard = report["xfa_lifecycle"]["normal"]["standard"]
    normal_consistency = report["xfa_lifecycle"]["normal"]["consistency"]
    stressed_standard = report["xfa_lifecycle"]["stressed"]["standard"]
    assert normal_standard["combine_attempts"] == 96
    assert normal_standard["xfa_paths_started"] == 2
    assert normal_standard["first_payouts"] == 2
    assert (
        normal_standard["unconditional_lower_bound"][
            "expected_trader_payout_per_combine_attempt"
        ]
        == 18.75
    )
    assert (
        normal_standard["days_to_first_payout"]["evaluable_only"]["median"]
        == 10.0
    )
    assert (
        normal_consistency["unconditional_lower_bound"][
            "expected_trader_payout_per_combine_attempt"
        ]
        == 9.375
    )
    assert (
        stressed_standard["unconditional_lower_bound"][
            "expected_trader_payout_per_combine_attempt"
        ]
        == 9.375
    )
    assert report["integrity"]["full_pass_xfa_lifecycle_bijection_valid"] is True
    assert (
        report["production_context"]["source"]
        == "DEEP_VERIFIED_EVIDENCE_BUNDLE_AND_TERMINAL_SNAPSHOTS"
    )

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
    assert "overlapping rolling episode starts are not independent" in markdown
    assert "B1" in markdown
    assert "candidate-a" in markdown


def test_hash_validated_terminal_result_and_state_supply_current_context(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    state_path = tmp_path / "production_state.json"
    state = json.loads(paths["state"].read_text(encoding="utf-8"))
    assert "stage3_policy_count" not in state
    state["next_action"] = "CONTINUE_STAGE3_ECONOMIC_REPLAY"
    state.pop("state_hash")
    state["state_hash"] = canonical_hash(state)
    _write(state_path, state)
    report = build_active_risk_decision_report(
        manifest_path=paths["manifest"],
        stage3_cache_dir=paths["stage3"],
        matched_controls_path=paths["controls"],
        halving_dir=paths["halving"],
        expected_stage3_count=2,
        production_state_path=state_path,
    )
    context = report["production_context"]
    assert context["source"] == "DEEP_VERIFIED_EVIDENCE_BUNDLE_AND_TERMINAL_SNAPSHOTS"
    assert context["production_state_available"] is True
    assert context["identity_audit_status"] == "PASS"
    assert context["current_production_funnel"]["governor_proposals_generated"] == 20_000
    assert context["next_autonomous_action"] == {"action": "QUEUE_FROZEN_NEXT_ACTION"}
    assert context["current_bottleneck"] is None


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


def test_xfa_source_hash_corruption_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["rows"][0]["lifecycle_rows"][0]["source_lifecycle_sha256"] = "bad"
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match="source lifecycle hash drift"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_xfa_daily_path_cardinality_corruption_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    lifecycle = payload["rows"][0]["lifecycle_rows"][0]
    lifecycle["standard"]["daily_ledger"].pop()
    lifecycle.pop("source_lifecycle_sha256")
    lifecycle["source_lifecycle_sha256"] = canonical_hash(lifecycle)
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match="daily-ledger cardinality drift"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_xfa_profile_fingerprint_corruption_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    lifecycle = payload["rows"][0]["lifecycle_rows"][0]
    lifecycle["xfa_profile"]["risk_multiplier"] = 1.15
    lifecycle.pop("source_lifecycle_sha256")
    lifecycle["source_lifecycle_sha256"] = canonical_hash(lifecycle)
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match="profile fingerprint drift"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_full_pass_xfa_bijection_corruption_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    lifecycle = payload["rows"][0]["lifecycle_rows"][0]
    lifecycle["combine_start_day"] += 1
    _reseal_lifecycle(lifecycle)
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="lifecycle projection|authoritative bijection drift",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_duplicate_canonical_episode_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    duplicate = dict(payload["rows"][0]["evidence_raw"][0])
    payload["rows"][0]["evidence_raw"].append(duplicate)
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match="duplicate 20_TRADING_DAYS"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_canonical_summary_reconciliation_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["rows"][0]["evidence_raw"][0]["net_pnl"] += 1.0
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="net PnL/daily path drift|net_total drift",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_rehashed_raw_and_daily_economics_cannot_replace_sealed_episode_partition(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    candidate = payload["rows"][0]
    raw = candidate["evidence_raw"][0]
    raw["net_pnl"] += 1.0
    raw["component_contribution"]["sleeve"] += 1.0
    final_day = raw["daily_path"][-1]
    final_day["realized_pnl"] += 1.0
    final_day["day_pnl"] += 1.0
    final_day["balance"] += 1.0
    final_day["component_attribution"]["sleeve"] += 1.0
    scenario = "normal" if raw["scenario"] == "NORMAL" else "stressed"
    horizon = raw["horizon_label"]
    candidate["horizons"][scenario][horizon] = _summary_from_rows(
        [
            value
            for value in candidate["evidence_raw"]
            if value["scenario"] == raw["scenario"]
            and value["horizon_label"] == horizon
        ]
    )
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="diverges from sealed episodes partition",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_rehashed_daily_path_cannot_replace_sealed_daily_partition(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["rows"][0]["evidence_raw"][0]["daily_path"][0]["balance"] += 1.0
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="diverges from sealed account_daily_paths partition",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_rehashed_maximum_target_progress_is_redriven_from_sealed_daily_path(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    candidate = payload["rows"][0]
    raw = candidate["evidence_raw"][0]
    raw["maximum_target_progress"] += 0.25
    summary = candidate["horizons"]["normal"][raw["horizon_label"]]
    summary["maximum_target_progress"] = max(
        value["maximum_target_progress"]
        for value in candidate["evidence_raw"]
        if value["scenario"] == "NORMAL"
        and value["horizon_label"] == raw["horizon_label"]
    )
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="maximum target progress/daily path",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_stage3_cache_index_swap_cannot_retarget_sealed_partitions(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    left = paths["stage3"] / "batch_000000.json"
    right = paths["stage3"] / "batch_000001.json"
    left_payload = json.loads(left.read_text(encoding="utf-8"))
    right_payload = json.loads(right.read_text(encoding="utf-8"))
    _write(left, right_payload)
    _write(right, left_payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="diverges from sealed episodes partition",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


@pytest.mark.parametrize(
    ("field", "expected"),
    (
        ("risk_utilisation", "cached risk_utilisation diverges"),
        ("suppression", "cached suppression diverges"),
        ("exposure_signature", "cached exposure_signature diverges"),
        ("structural_fingerprint", "structural fingerprint diverges"),
    ),
)
def test_rehashed_stage3_top_level_claims_remain_authoritatively_bound(
    tmp_path: Path, field: str, expected: str
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    candidate = payload["rows"][0]
    if field == "risk_utilisation":
        candidate[field]["mean"] += 0.1
    elif field == "suppression":
        candidate[field]["signals_emitted"] += 1
    elif field == "exposure_signature":
        candidate[field]["time_weighted_mini_nanoseconds_per_observed_day"] += 1.0
    else:
        candidate[field] = "f" * 64
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match=expected):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


@pytest.mark.parametrize("mutation", ("risk_p25", "risk_group_median", "suppression_extra"))
def test_rehashed_nested_diagnostic_claims_are_strictly_redriven(
    tmp_path: Path, mutation: str
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    candidate = payload["rows"][0]
    if mutation == "risk_p25":
        candidate["risk_utilisation"]["p25"] += 0.123
        expected = "cached risk_utilisation diverges"
    elif mutation == "risk_group_median":
        candidate["risk_utilisation"]["by_active_sleeve_count"]["one"][
            "median"
        ] += 0.123
        expected = "cached risk_utilisation diverges"
    else:
        candidate["suppression"]["foregone_expected_pnl_status"] = "FABRICATED"
        expected = "cached suppression diverges"
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match=expected):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_unsealed_runtime_behavior_self_hash_is_not_published(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["rows"][0]["actual_account_behavior_fingerprint"] = "f" * 64
    _reseal_batch(cache, payload)
    report = build_active_risk_decision_report(
        manifest_path=paths["manifest"],
        stage3_cache_dir=paths["stage3"],
        matched_controls_path=paths["controls"],
        halving_dir=paths["halving"],
        expected_stage3_count=2,
    )
    candidate = next(
        value for value in report["candidates"] if value["policy_id"] == "candidate-a"
    )
    assert "actual_account_behavior_fingerprint" not in candidate
    assert candidate["cached_actual_account_behavior_fingerprint_status"] == (
        "OMITTED_UNSEALED_ORDER_SENSITIVE_CACHE_SELF_HASH"
    )
    assert candidate["sealed_normalized_account_behavior_fingerprint"] != "f" * 64


def test_noncanonical_horizon_summary_reconciliation_fails_closed(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["rows"][0]["horizons"]["normal"]["40_TRADING_DAYS"][
        "net_total"
    ] += 1.0
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError, match="normal 40_TRADING_DAYS net_total drift"
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_horizon_start_key_coverage_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    rows = payload["rows"][0]["evidence_raw"]
    removed = next(
        row
        for row in rows
        if row["scenario"] == "NORMAL"
        and row["horizon_label"] == "60_TRADING_DAYS"
        and not row["passed"]
    )
    rows.remove(removed)
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError, match="60_TRADING_DAYS episode-start key coverage drift"
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_normal_and_stressed_start_sets_must_match_per_horizon(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    candidate = payload["rows"][0]
    stressed_starts = {
        int(row["start_day"])
        for row in candidate["evidence_raw"]
        if row["scenario"] == "STRESSED_1_5X"
    }
    replaced_start = max(stressed_starts)
    replacement_start = replaced_start + 1
    for row in candidate["evidence_raw"]:
        if row["scenario"] != "STRESSED_1_5X" or int(row["start_day"]) != replaced_start:
            continue
        row["start_day"] = replacement_start
        row["end_day"] = int(row["end_day"]) + 1
        for daily in row["daily_path"]:
            daily["session_day"] = int(daily["session_day"]) + 1
    for horizon in HORIZONS:
        horizon_rows = [
            row
            for row in candidate["evidence_raw"]
            if row["scenario"] == "STRESSED_1_5X"
            and row["horizon_label"] == horizon
        ]
        candidate["horizons"]["stressed"][horizon] = _summary_from_rows(
            horizon_rows
        )
    candidate["stressed"] = dict(
        candidate["horizons"]["stressed"]["90_TRADING_DAYS"]
    )
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="normal/stressed start-set drift",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_exact_48_starts_per_scenario_is_required(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    candidate = payload["rows"][0]
    normal_90 = [
        row
        for row in candidate["evidence_raw"]
        if row["scenario"] == "NORMAL"
        and row["horizon_label"] == "90_TRADING_DAYS"
        and not row["passed"]
    ]
    removed_start = int(normal_90[-1]["start_day"])
    candidate["evidence_raw"] = [
        row
        for row in candidate["evidence_raw"]
        if not (row["scenario"] == "NORMAL" and int(row["start_day"]) == removed_start)
    ]
    for horizon in HORIZONS:
        horizon_rows = [
            row
            for row in candidate["evidence_raw"]
            if row["scenario"] == "NORMAL" and row["horizon_label"] == horizon
        ]
        candidate["horizons"]["normal"][horizon] = _summary_from_rows(horizon_rows)
    candidate["normal"] = dict(candidate["horizons"]["normal"]["90_TRADING_DAYS"])
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match="does not have 48 frozen starts"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_terminal_result_and_state_are_both_required(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    paths["result"].unlink()
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="final result and production state are required",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_production_context_campaign_mismatch_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    state = json.loads(paths["state"].read_text(encoding="utf-8"))
    state["campaign_id"] = "wrong-campaign"
    state.pop("state_hash")
    state["state_hash"] = canonical_hash(state)
    _write(paths["state"], state)
    with pytest.raises(ActiveRiskDecisionReportError, match="production state campaign identity drift"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_evidence_receipt_counts_must_match_deep_verified_bundle(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    result = json.loads(paths["result"].read_text(encoding="utf-8"))
    result["evidence_bundle"]["dataset_row_counts"]["episodes"] += 2
    result.pop("result_hash")
    result["result_hash"] = canonical_hash(result)
    _write(paths["result"], result)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="receipt dataset_row_counts drift",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_terminal_state_episode_counters_must_match_final_result(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    state = json.loads(paths["state"].read_text(encoding="utf-8"))
    state["normal_episodes_completed"] += 1
    state.pop("state_hash")
    state["state_hash"] = canonical_hash(state)
    _write(paths["state"], state)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="production state episode counters diverge from final result",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_terminal_state_bundle_checksum_must_match_receipt(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    state = json.loads(paths["state"].read_text(encoding="utf-8"))
    state["evidence_bundle_manifest_sha256"] = "0" * 64
    state.pop("state_hash")
    state["state_hash"] = canonical_hash(state)
    _write(paths["state"], state)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="production state EvidenceBundle manifest linkage drift",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_final_result_source_commit_must_match_manifest_and_bundle(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    result = json.loads(paths["result"].read_text(encoding="utf-8"))
    result["source_commit"] = "e" * 40
    result.pop("result_hash")
    result["result_hash"] = canonical_hash(result)
    _write(paths["result"], result)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="economic final result source-commit linkage drift",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_xfa_semantic_accounting_corruption_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    lifecycle = payload["rows"][0]["lifecycle_rows"][0]
    lifecycle["standard"]["payout_eligible"] = False
    _reseal_lifecycle(lifecycle)
    _reseal_batch(cache, payload)
    with pytest.raises(ActiveRiskDecisionReportError, match="payout eligibility drift"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_xfa_survived_path_requires_full_requested_horizon(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    lifecycle = payload["rows"][0]["lifecycle_rows"][0]
    standard = lifecycle["standard"]
    standard["daily_ledger"].pop()
    standard["observed_days"] = 119
    standard["end_day"] = int(standard["end_day"]) - 1
    standard["post_payout_observed_days"] = 109
    _reseal_lifecycle(lifecycle)
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError, match="survived-horizon cardinality drift"
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_xfa_event_counts_must_reconcile_to_daily_ledger(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    lifecycle = payload["rows"][0]["lifecycle_rows"][0]
    lifecycle["standard"]["accepted_event_count"] += 1
    lifecycle["standard"]["event_count"] += 1
    _reseal_lifecycle(lifecycle)
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError, match="event ledger accounting drift"
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_xfa_zero_observation_requires_complete_censor_identity() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    path.update(
        {
            "terminal": "DATA_CENSORED",
            "start_day": None,
            "end_day": None,
            "observed_days": 0,
            "traded_days": 0,
            "event_count": 0,
            "accepted_event_count": 0,
            "skipped_event_count": 0,
            "payout_eligible": False,
            "payout_cycles": 0,
            "gross_payout": 0.0,
            "trader_net_payout": 0.0,
            "first_payout_day": None,
            "post_payout_survived": False,
            "post_payout_censored": False,
            "post_payout_observed_days": 0,
            "total_cost": 1.0,
            "skipped_reasons": {},
            "component_contribution": {},
            "daily_ledger": [],
        }
    )
    with pytest.raises(
        ActiveRiskDecisionReportError, match="zero-observation censor identity drift"
    ):
        report_module._validate_xfa_path_accounting(
            path,
            label="zero-observation",
            combine_end_day=19_999,
            xfa_start_day=None,
            rule_snapshot=official_rule_snapshot_2026_07_15().to_dict(),
        )


def test_xfa_start_must_follow_combine_end() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="XFA start is not strictly after Combine end",
    ):
        report_module._validate_xfa_path_accounting(
            path,
            label="chronology",
            combine_end_day=20_000,
            xfa_start_day=20_000,
            rule_snapshot=official_rule_snapshot_2026_07_15().to_dict(),
        )


def test_xfa_daily_ledger_must_be_strictly_chronological() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    ledger = path["daily_ledger"]
    assert isinstance(ledger, list)
    ledger[1], ledger[2] = ledger[2], ledger[1]
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="daily ledger is not strictly chronological",
    ):
        report_module._validate_xfa_path_accounting(
            path,
            label="chronology",
            combine_end_day=19_999,
            xfa_start_day=20_000,
            rule_snapshot=official_rule_snapshot_2026_07_15().to_dict(),
        )


def test_xfa_zero_observation_account_state_must_match_rule_snapshot() -> None:
    frozen_rules = official_rule_snapshot_2026_07_15()
    rules = frozen_rules.to_dict()
    valid = _zero_observation_xfa_path(
        path="STANDARD", horizon=120, rules=frozen_rules
    ).to_dict()
    report_module._validate_xfa_path_accounting(
        valid,
        label="valid-zero-observation",
        combine_end_day=19_999,
        xfa_start_day=None,
        rule_snapshot=rules,
    )
    path = dict(valid)
    path.update(
        {
            "ending_balance": 999_999.0,
        }
    )
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="zero-observation censor identity drift",
    ):
        report_module._validate_xfa_path_accounting(
            path,
            label="zero-observation",
            combine_end_day=19_999,
            xfa_start_day=None,
            rule_snapshot=rules,
        )


def test_xfa_source_rule_failure_reconciles_one_fatal_unclassified_event() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    path.update(
        {
            "terminal": "HARD_RULE_FAILURE",
            "terminal_reason": "source_contract_limit_violation",
            "event_count": 11,
            "post_payout_survived": False,
        }
    )
    report_module._validate_xfa_path_accounting(
        path,
        label="fatal-source-event",
        combine_end_day=19_999,
        xfa_start_day=20_000,
        rule_snapshot=official_rule_snapshot_2026_07_15().to_dict(),
    )


def test_random_exposure_match_corruption_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    controls = json.loads(paths["controls"].read_text(encoding="utf-8"))
    controls["random_priority_exposure_match_by_policy"]["candidate-a"][
        "matched"
    ] = False
    controls.pop("controls_hash")
    controls["controls_hash"] = canonical_hash(controls)
    _write(paths["controls"], controls)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="external matched controls diverge from authoritative EvidenceBundle",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_rehashed_matched_control_economics_drift_fails_authoritative_chain(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    controls = json.loads(paths["controls"].read_text(encoding="utf-8"))
    controls["static_partition"]["normal"]["net_total"] += 1.0
    controls.pop("controls_hash")
    controls["controls_hash"] = canonical_hash(controls)
    _write(paths["controls"], controls)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="external matched controls diverge from authoritative EvidenceBundle",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_halving_decision_hash_is_required(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    decision_path = paths["halving"] / "stage4.json"
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    decision.pop("decision_hash")
    _write(decision_path, decision)
    with pytest.raises(ActiveRiskDecisionReportError, match="lacks required decision_hash"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_halving_output_count_and_selected_ids_must_reconcile(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    decision_path = paths["halving"] / "stage4.json"
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    decision["output_count"] += 1
    decision.pop("decision_hash")
    decision["decision_hash"] = canonical_hash(decision)
    _write(decision_path, decision)
    with pytest.raises(ActiveRiskDecisionReportError, match="count consistency drift"):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_lifecycle_censoring_and_missing_buffer_are_not_coerced() -> None:
    accumulator = report_module.LifecyclePathAccumulator()
    accumulator.add_combine_episode(
        {"terminal_classification": "DATA_CENSORED", "censored": True}
    )
    accumulator.add_combine_episode(
        {"terminal_classification": "TARGET_REACHED", "passed": True}
    )
    accumulator.add_path(
        {
            "terminal": "DATA_CENSORED",
            "observed_days": 0,
            "payout_eligible": False,
            "payout_cycles": 0,
            "trader_net_payout": 0.0,
            "post_payout_survived": False,
            "post_payout_censored": False,
            "minimum_mll_buffer": None,
        }
    )
    value = accumulator.to_dict()
    assert value["zero_observation_xfa_paths"] == 1
    assert value["minimum_mll_buffer"]["missing_count"] == 1
    assert value["minimum_mll_buffer"]["all_nonmissing_paths"]["count"] == 0
    assert (
        value["evaluable_only"]["denominators"][
            "xfa_paths_excluding_censored_or_zero_observation"
        ]
        == 0
    )
    assert (
        value["evaluable_only"][
            "first_payout_probability_per_evaluable_lifecycle_attempt"
        ]
        is None
    )

    payout_then_censored = report_module.LifecyclePathAccumulator()
    payout_then_censored.add_combine_episode(
        {"terminal_classification": "TARGET_REACHED", "passed": True}
    )
    payout_then_censored.add_path(
        {
            "terminal": "DATA_CENSORED",
            "observed_days": 10,
            "payout_eligible": True,
            "payout_cycles": 1,
            "trader_net_payout": 900.0,
            "post_payout_survived": False,
            "post_payout_censored": True,
            "minimum_mll_buffer": 2500.0,
            "first_payout_day": 5,
        }
    )
    observed = payout_then_censored.to_dict()["evaluable_only"]
    assert observed["first_payout_probability_per_evaluable_lifecycle_attempt"] == 1.0
    assert (
        observed[
            "observed_trader_payout_lower_bound_per_first_payout_evaluable_attempt"
        ]
        == 900.0
    )
    assert observed["expected_trader_payout_per_evaluable_lifecycle_attempt"] is None
    assert (
        observed[
            "post_payout_survival_probability_conditional_on_evaluable_first_payout"
        ]
        is None
    )


def test_same_economics_with_different_routed_trades_split_clusters() -> None:
    left = _candidate("candidate-a")
    right = _candidate("candidate-b")
    for raw in right["evidence_raw"]:
        for decision in raw["risk_allocation_path"]:
            decision["event_id"] = "distinct:" + decision["event_id"]
            decision["quantity"] += 5
    left_raw = [
        raw
        for raw in left["evidence_raw"]
        if raw["horizon_label"] == "90_TRADING_DAYS"
    ]
    right_raw = [
        raw
        for raw in right["evidence_raw"]
        if raw["horizon_label"] == "90_TRADING_DAYS"
    ]
    left_vector, left_terminals, _, left_keys, left_routing = (
        report_module._behavior_vector(left_raw)
    )
    right_vector, right_terminals, _, right_keys, right_routing = (
        report_module._behavior_vector(right_raw)
    )
    assert left_keys == right_keys
    similarity = report_module._behavior_similarity(
        left_vector,
        right_vector,
        left_terminals,
        right_terminals,
        left_routing,
        right_routing,
    )
    assert similarity[3] < 0.90
    assert similarity[4] is False


def test_rehashed_halving_selection_drift_fails_authoritative_chain(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    _write(
        paths["halving"] / "stage3.json",
        _decision("ACTIVE_POOL_STAGE_3_TO_96", ["candidate-a"]),
    )
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="external halving decisions diverge from authoritative EvidenceBundle",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


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
    assert build["final_result_path"] == (
        tmp_path
        / "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02"
        / "economic_production_result.json"
    )
    assert build["production_state_path"].name == "production_state.json"
    written = captured["write"]
    assert isinstance(written, dict)
    assert written["json_path"].parent.name == (
        "active_risk_pool_target_velocity_0026_revision_02"
    )
