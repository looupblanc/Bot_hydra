from __future__ import annotations

import inspect
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
from hydra.propfirm.xfa_payout_events import (
    CANONICAL_PAYOUT_RECONCILIATION_SCHEMA,
    CanonicalPayoutEvent,
    PayoutPathReconciliation,
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
                source_prefix = f"sleeve:{scenario}:{horizon_label}:{start}"
                stress_suffix = (
                    ":portfolio_cost_stress_1_5x"
                    if scenario == "STRESSED_1_5X"
                    else ""
                )
                routing = [
                    {
                        "event_id": f"{source_prefix}:entry-a{stress_suffix}",
                        "component_id": "sleeve",
                        "decision_ns": start * 86_400_000_000_000 + 1,
                        "exit_ns": start * 86_400_000_000_000 + 2,
                        "allow": True,
                        "accepted": True,
                        "emitted": True,
                        "rejected": False,
                        "size_reduced": False,
                        "mini_equivalent": 2.0,
                        "base_quantity": 2,
                        "requested_quantity": 2,
                        "scaling_factor": 1.0,
                        "foregone_realized_pnl_ex_post": 0.0,
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
                        "event_id": f"{source_prefix}:entry-b{stress_suffix}",
                        "component_id": "sleeve",
                        "decision_ns": start * 86_400_000_000_000 + 3,
                        "exit_ns": start * 86_400_000_000_000 + 4,
                        "allow": True,
                        "accepted": True,
                        "emitted": True,
                        "rejected": False,
                        "size_reduced": False,
                        "mini_equivalent": 1.0,
                        "base_quantity": 1,
                        "requested_quantity": 1,
                        "scaling_factor": 1.0,
                        "foregone_realized_pnl_ex_post": 0.0,
                        "risk_before": {
                            "utilisation": 0.5,
                            "active_sleeve_count": 1,
                        },
                        "risk_after": {
                            "utilisation": 0.75,
                            "active_sleeve_count": 2,
                        },
                        "quantity": 1,
                        "decision_status": "ACCEPTED",
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
    rules = official_rule_snapshot_2026_07_15()
    qualifying_days = 5 if path == "XFA_STANDARD" else 3
    gross = net / rules.trader_profit_split
    pre_payout_balance = gross / rules.payout_fraction
    day_profit = pre_payout_balance / qualifying_days
    balance = 0.0
    floor = rules.xfa_starting_floor
    ledger: list[dict[str, object]] = []
    winning_days = 0
    traded_days_cycle = 0
    total_profit_cycle = 0.0
    best_day_cycle = 0.0
    cycle_start_balance = balance
    cycles = 0
    minimum_buffer = balance - floor
    for index in range(120):
        opening = balance
        floor_open = floor
        traded = index < qualifying_days
        pnl = day_profit if traded else 0.0
        if traded:
            traded_days_cycle += 1
            winning_days += int(pnl >= rules.xfa_standard_winning_day_minimum)
        total_profit_cycle += pnl
        best_day_cycle = max(best_day_cycle, pnl)
        ratio = (
            best_day_cycle / total_profit_cycle
            if total_profit_cycle > 0.0 and best_day_cycle > 0.0
            else None
        )
        balance += pnl
        floor = min(
            0.0,
            max(floor, balance - rules.maximum_loss_limit),
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        eligible = (
            winning_days >= rules.xfa_standard_winning_days
            if path == "XFA_STANDARD"
            else traded_days_cycle >= rules.xfa_consistency_traded_days
            and total_profit_cycle > 0.0
            and ratio is not None
            and ratio <= rules.xfa_consistency_limit + 1e-12
        )
        execute = eligible and index == qualifying_days - 1
        day_gross = gross if execute else 0.0
        day_net = net if execute else 0.0
        if execute:
            balance -= day_gross
            floor = 0.0
            cycles += 1
            winning_days = 0
            traded_days_cycle = 0
            total_profit_cycle = 0.0
            best_day_cycle = 0.0
            cycle_start_balance = balance
            minimum_buffer = min(minimum_buffer, balance - floor)
        ledger.append(
            {
                "session_day": start_day + index,
                "opening_balance": opening,
                "closing_balance": balance,
                "mll_floor_open": floor_open,
                "mll_floor_close": floor,
                "day_pnl": pnl,
                "worst_intraday_equity": min(opening, opening + pnl),
                "traded": traded,
                "accepted_events": 1 if traded else 0,
                "skipped_events": 0,
                "winning_days_in_cycle": winning_days,
                "traded_days_in_cycle": traded_days_cycle,
                "profit_since_payout": balance - cycle_start_balance,
                "consistency_ratio_before_reset": ratio,
                "payout_eligible": eligible,
                "payout_requested": execute,
                "gross_payout": day_gross,
                "trader_net_payout": day_net,
                "payout_cycles": cycles,
                "post_payout_mll_locked_at_zero": cycles > 0,
                "terminal": None,
            }
        )
    value: dict[str, object] = {
        "path": path,
        "terminal": "SURVIVED_HORIZON",
        "terminal_reason": "completed_frozen_xfa_horizon",
        "start_day": start_day,
        "end_day": start_day + 119,
        "requested_horizon_days": 120,
        "observed_days": 120,
        "traded_days": qualifying_days,
        "event_count": qualifying_days,
        "accepted_event_count": qualifying_days,
        "skipped_event_count": 0,
        "payout_eligible": True,
        "payout_cycles": 1,
        "gross_payout": net / 0.9,
        "trader_net_payout": net,
        "first_payout_day": qualifying_days,
        "post_payout_survived": True,
        "post_payout_censored": False,
        "post_payout_observed_days": 120 - qualifying_days,
        "ending_balance": balance,
        "ending_mll_floor": 0.0,
        "minimum_mll_buffer": minimum_buffer,
        "qualifying_winning_days": qualifying_days,
        "maximum_consistency_ratio": 1.0,
        "maximum_mini_equivalent": 3.0,
        "total_cost": 100.0,
        "skipped_reasons": {},
        "component_contribution": {"sleeve": net},
        "daily_ledger": ledger,
        "calendar_inactivity_auditable": True,
        "payout_request_policy": "EARLIEST_ELIGIBLE_END_OF_DAY",
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
    profile = FrozenRiskProfile(
        profile_id=f"{policy_id}:XFA_PROFILE",
        maximum_simultaneous_positions=1,
    )
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
    runtime_behavior_fingerprint = (
        report_module._runtime_behavior_fingerprint_from_raw(
            [
                value
                for value in raw
                if value["horizon_label"] == "90_TRADING_DAYS"
            ]
        )
    )
    return {
        "schema": "hydra_active_risk_policy_metric_v1",
        "policy_id": policy_id,
        "structural_fingerprint": canonical_hash(
            {"policy_id": policy_id, "kind": "structure"}
        ),
        "actual_account_behavior_fingerprint": runtime_behavior_fingerprint,
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
        "xfa_paths_started": 2,
        "xfa_standard_paths": 2,
        "xfa_consistency_paths": 2,
        "first_payouts": 4,
        "payout_cycles": 4,
        "trader_net_payout": 2025.0,
        "post_payout_survival_count": 4,
        "post_payout_survival_rate": 1.0,
    }


def _sealed_full_pass_episode(
    *, policy_id: str = "candidate-a", scenario: str = "NORMAL"
) -> dict[str, object]:
    manifest = {
        "temporal_blocks": {
            "blocks": [
                {
                    "block_id": f"B{month}",
                    "start": date(2023, month, 1).isoformat(),
                    "end": (
                        date(2023, month + 1, 1) - timedelta(days=1)
                        if month < 12
                        else date(2023, 12, 31)
                    ).isoformat(),
                }
                for month in range(1, 5)
            ]
        }
    }
    episodes, _daily_rows = _evidence_rows(_candidate(policy_id), manifest)
    selected = [
        row
        for row in episodes
        if row["cost_scenario"] == scenario
        and row["horizon"] == "FULL_CHRONOLOGICAL_HORIZON"
        and row["target_reached"] is True
    ]
    assert len(selected) == 1
    return json.loads(json.dumps(selected[0]))


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
    frontier_candidates: list[dict[str, object]] | None = None,
    bundle_root_name: str = "_active_risk_report_bundle",
) -> dict[str, object]:
    shared_root = tmp_path.parent / bundle_root_name
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
    source_events: dict[str, dict[str, object]] = {}
    for raw in candidates[0]["evidence_raw"]:
        routing = raw["risk_allocation_path"]
        assert isinstance(routing, list) and routing
        per_event_net = float(raw["net_pnl"]) / len(routing)
        for decision in routing:
            routed_id = str(decision["event_id"])
            source_id = routed_id.removesuffix(
                ":portfolio_cost_stress_1_5x"
            )
            event = {
                "trade_id": source_id,
                "quantity": int(decision["base_quantity"]),
                "net_pnl": per_event_net,
                "start_day": int(raw["start_day"]),
            }
            prior = source_events.get(source_id)
            if prior is not None:
                assert prior == event
            source_events[source_id] = event
    signals: list[dict[str, object]] = []
    entries: list[dict[str, object]] = []
    exits: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    for source_id, event in sorted(source_events.items()):
        event_date = (
            date(1970, 1, 1) + timedelta(days=int(event["start_day"]))
        ).isoformat()
        entry_time = f"{event_date}T14:30:00Z"
        exit_time = f"{event_date}T15:00:00Z"
        quantity = int(event["quantity"])
        net_pnl = float(event["net_pnl"])
        signals.append(
            {
                "campaign_id": CAMPAIGN,
                "component_id": component_id,
                "signal_id": f"signal:{source_id}",
                "event_time": f"{event_date}T14:29:00Z",
                "market": "NQ",
                "contract": "NQH3",
                "timeframe": "1m",
                "signal": 1,
                "sizing": float(quantity),
                "stop": 10990.0,
                "target": 11020.0,
                "veto": False,
                "component_role": "TARGET_VELOCITY",
            }
        )
        entries.append(
            {
                "campaign_id": CAMPAIGN,
                "component_id": component_id,
                "trade_id": source_id,
                "entry_time": entry_time,
                "market": "NQ",
                "contract": "NQH3",
                "side": "LONG",
                "quantity": quantity,
                "entry_price": 11000.0,
                "sizing": float(quantity),
                "stop_price": 10990.0,
                "target_price": 11020.0,
            }
        )
        exits.append(
            {
                "campaign_id": CAMPAIGN,
                "component_id": component_id,
                "trade_id": source_id,
                "exit_time": exit_time,
                "exit_price": 11005.0,
                "exit_reason": "TARGET_HORIZON_EXIT",
            }
        )
        trades.append(
            {
                "campaign_id": CAMPAIGN,
                "component_id": component_id,
                "trade_id": source_id,
                "entry_time": entry_time,
                "exit_time": exit_time,
                "market": "NQ",
                "contract": "NQH3",
                "side": "LONG",
                "quantity": quantity,
                "entry_price": 11000.0,
                "exit_price": 11005.0,
                "gross_pnl": net_pnl,
                "costs": 0.0,
                "net_pnl": net_pnl,
            }
        )
    candidate_by_id = {
        str(candidate["policy_id"]): candidate for candidate in candidates
    }
    memberships = []
    for policy_id in policy_ids:
        active_risk_policy = {
            "schema": "hydra_active_risk_pool_policy_v1",
            "policy_version": "hydra_active_risk_pool_governor_v1",
            "policy_id": policy_id,
            "structural_fingerprint": candidate_by_id[policy_id][
                "structural_fingerprint"
            ],
            "component_priority": [component_id],
            "nominal_risk_charge_per_mini": {component_id: 2_250.0},
            "same_instrument_conflict_rule": "ALLOW_SAME_DIRECTION",
            "static_risk_tier": 1.0,
            "maximum_concurrent_sleeves": 1,
            "maximum_mini_equivalent": 15,
            "future_outcome_fields_used": False,
            "outbound_order_capability": False,
        }
        memberships.append(
            {
                "campaign_id": CAMPAIGN,
                "policy_id": policy_id,
                "component_id": component_id,
                "risk_allocation": 1.0,
                "component_role": "TARGET_VELOCITY",
                "inactive_sleeve_reserves_risk": False,
                "underlying_signal_mutated": False,
                "active_risk_policy": active_risk_policy,
            }
        )
    records = {
        "component_signals": signals,
        "component_entries": entries,
        "component_exits": exits,
        "component_trades": trades,
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
        "pareto_archive",
        {
            "schema": "hydra_active_risk_pareto_archive_v1",
            "campaign_id": CAMPAIGN,
            "frontier": [
                candidate
                for candidate in (frontier_candidates or candidates)
                if candidate["policy_id"]
                in set(stage_decisions[-1]["selected_policy_ids"])
            ],
            "stage_decisions": stage_decisions,
            "opaque_score_used": False,
        },
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


def _fixture(
    tmp_path: Path,
    *,
    tamper_finalist_frontier_consistency: bool = False,
    tamper_finalist_frontier_behavior: bool = False,
) -> dict[str, Path]:
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
        "sleeve_bank": {
            "member_count": 1,
            "members": [
                {
                    "sleeve_id": "sleeve",
                    "immutable_fingerprint": canonical_hash(
                        {"component_id": "sleeve"}
                    ),
                    "behavioral_fingerprint": canonical_hash(
                        {"component_id": "sleeve", "behavior": 1}
                    ),
                    "signal_ledger_sha256": "1" * 64,
                    "trade_ledger_sha256": "2" * 64,
                    "market": "NQ",
                    "contract": "NQH3",
                    "timeframe": "1m",
                    "session": "OPEN",
                    "source_campaign": "synthetic_source",
                    "role": "TARGET_VELOCITY",
                    "sleeve_specification": {
                        "sleeve_id": "sleeve",
                        "version": 1,
                        "market": "NQ",
                    },
                }
            ],
        },
        "costs": {
            "normal_multiplier": 1.0,
            "stressed_multiplier": 1.5,
        },
        "account_parameters": {
            "starting_balance": 150_000.0,
            "profit_target": 9_000.0,
            "maximum_loss_limit": 4_500.0,
            "maximum_mini_equivalent": 15,
        },
        "lifecycle": {
            "rule_snapshot": official_rule_snapshot_2026_07_15().to_dict(),
            "standard_and_consistency_both_evaluated": True,
            "books_frozen_before_outcomes": True,
        },
        "successive_halving": {
            "frozen_horizons": [20, 40, 60, 90, "FULL"],
            "xfa_profile_projection": {
                "profile_version": "hydra_combine_to_xfa_v1",
                "clip_to_official_scaling_plan": True,
                "same_market_exclusive": True,
                "active_pool_combine_only_controls_applied": False,
                "selected_after_combine_outcome": False,
            },
        },
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
            # Account attempts count policy/start/scenario once.  The bundle
            # persists five horizon rows per Stage-3 attempt (960 rows).
            "combine_episodes_completed": 192,
            "normal_episodes_completed": 96,
            "stressed_episodes_completed": 96,
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
        # The synthetic sealed bundle contains Stage-3 episode partitions only.
        "xfa_paths_started": 4,
        "xfa_standard_paths": 4,
        "xfa_consistency_paths": 4,
        "first_payouts": 8,
        "payout_cycles": 8,
        "trader_net_payout": 4_050.0,
        "post_payout_survival_count": 8,
        "post_payout_survival_rate": 1.0,
        "development_finalist_ids": ["candidate-a"],
        "confirmation_ready_candidate_ids": ["candidate-a"],
    }
    frontier_candidates: list[dict[str, object]] | None = None
    bundle_root_name = "_active_risk_report_bundle"
    if tamper_finalist_frontier_consistency or tamper_finalist_frontier_behavior:
        frontier_candidates = json.loads(json.dumps(candidates))
        finalist = frontier_candidates[0]
        if tamper_finalist_frontier_consistency:
            normal_horizon = finalist["horizons"]["normal"]["90_TRADING_DAYS"]
            normal_horizon["consistency_ok_count"] = int(
                normal_horizon["consistency_ok_count"]
            ) - 1
            normal_horizon["consistency_rate"] = (
                int(normal_horizon["consistency_ok_count"])
                / int(normal_horizon["episode_count"])
            )
            finalist["normal"] = dict(normal_horizon)
        if tamper_finalist_frontier_behavior:
            finalist["actual_account_behavior_fingerprint"] = "f" * 64
        bundle_root_name = (
            "_active_risk_report_bundle_frontier_behavior_tamper"
            if tamper_finalist_frontier_behavior
            else "_active_risk_report_bundle_frontier_tamper"
        )
    evidence_receipt = _seal_test_evidence_bundle(
        tmp_path,
        manifest_path=manifest_path,
        economic_results=economic_results,
        stage_decisions=stage_decisions,
        candidates=candidates,
        frontier_candidates=frontier_candidates,
        bundle_root_name=bundle_root_name,
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
        "combine_episodes_completed": 192,
        "normal_episodes_completed": 96,
        "stressed_episodes_completed": 96,
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
        "sealed_result_recovery": {
            "preregistered_deep_guard_count": 2,
            "additional_deep_guard_performed": False,
            "deep_guard_completion_proof": (
                "EXACT_POST_GUARD_FAILED_CLOSED_COUNTER_ASSERTION"
            ),
        },
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


def test_report_reuses_preregistered_guards_without_third_deep_scan() -> None:
    source = inspect.getsource(report_module._production_context)
    assert "verify_evidence_bundle(bundle_path, deep=False)" in source
    assert "deep=True" not in source


def test_cumulative_behavior_clustering_covers_exact_eight_by_192_path() -> None:
    """Exercise the full 48+48+96 path without heavy EvidenceBundle I/O."""

    candidate_ids = [f"finalist-{index}" for index in range(8)]

    def observation(
        candidate_index: int, scenario: str, start: int
    ) -> report_module.ExpandedBehaviorObservation:
        stage = "stage3" if start < 48 else "stage4" if start < 96 else "stage5"
        family = {
            0: "family-01",
            1: "family-01",
            2: "family-23",
            3: "family-23",
            4: "family-4",
            5: "family-5",
            6: "family-67",
            7: "family-67" if start < 96 else "family-7-stage5",
        }[candidate_index]
        tiny_delta = 0.002 if candidate_index == 3 else 0.0
        stage5_divergence = 0.8 if candidate_index == 7 and start >= 96 else 0.0
        scenario_delta = 0.03 if scenario == "stressed" else 0.0
        varying = (start % 17) / 100.0
        feature_values = (
            0.20 + varying + tiny_delta + stage5_divergence,
            0.35 + varying / 2.0 + tiny_delta + stage5_divergence,
            0.60 - scenario_delta + varying / 3.0 + tiny_delta,
            0.80 - varying / 4.0,
            float(start % 5 != 0),
        )
        terminal_code = int(candidate_index == 7 and start >= 96)
        route = (
            scenario,
            start,
            family,
            f"source:{family}:{scenario}:{start}",
            2,
            1,
            "SIZE_REDUCED" if start % 7 == 0 else "ACCEPTED",
        )
        admitted = (
            scenario,
            start,
            family,
            f"trade:{family}:{scenario}:{start}",
            1,
        )
        exact_payload = {
            "stage": stage,
            "scenario": scenario,
            "start": start,
            "full_trajectory_digest": canonical_hash(
                [feature_values, terminal_code, route, admitted]
            ),
            "routing_and_suppression": route,
            "admitted_trade_and_contribution": admitted,
            "admitted_trade_account_contribution": feature_values[0],
        }
        return report_module.ExpandedBehaviorObservation(
            scenario=scenario,
            start_day=start,
            stage=stage,
            exact_row_hash=canonical_hash(exact_payload),
            feature_values=feature_values,
            terminal_code=terminal_code,
            routing_tuples=frozenset({route}),
            admitted_trade_tuples=frozenset({admitted}),
        )

    observations: dict[
        str, dict[tuple[str, int], report_module.ExpandedBehaviorObservation]
    ] = {}
    for candidate_index, candidate_id in enumerate(candidate_ids):
        ordered = [
            observation(candidate_index, scenario, start)
            for scenario in ("normal", "stressed")
            for start in range(192)
        ]
        if candidate_index == 1:
            ordered.reverse()
        observations[candidate_id] = {
            (row.scenario, row.start_day): row for row in ordered
        }

    expected_stage_counts = {
        "stage3": {"normal": 48, "stressed": 48},
        "stage4": {"normal": 48, "stressed": 48},
        "stage5": {"normal": 96, "stressed": 96},
    }
    profiles = {
        candidate_id: report_module._cumulative_behavior_profile(
            rows, declared_stage_start_counts=expected_stage_counts
        )
        for candidate_id, rows in observations.items()
    }
    assert sum(
        profile["public"]["observation_count"] for profile in profiles.values()
    ) == 3_072
    for profile in profiles.values():
        assert profile["public"]["per_scenario_observation_count"] == {
            "normal": 192,
            "stressed": 192,
        }
        assert profile["public"]["stage_start_counts"] == expected_stage_counts
        assert profile["public"]["observation_count"] == 384

    fingerprint = "authoritative_raw_account_trade_behavior_fingerprint"
    assert profiles["finalist-0"]["public"][fingerprint] == profiles[
        "finalist-1"
    ]["public"][fingerprint]
    assert profiles["finalist-2"]["public"][fingerprint] != profiles[
        "finalist-3"
    ]["public"][fingerprint]
    assert profiles["finalist-6"]["public"][fingerprint] != profiles[
        "finalist-7"
    ]["public"][fingerprint]

    stage3_six = {
        key: row
        for key, row in observations["finalist-6"].items()
        if row.stage == "stage3"
    }
    stage3_seven = {
        key: row
        for key, row in observations["finalist-7"].items()
        if row.stage == "stage3"
    }
    assert report_module._cumulative_behavior_profile(stage3_six)["public"][
        fingerprint
    ] == report_module._cumulative_behavior_profile(stage3_seven)["public"][
        fingerprint
    ]

    candidates = {
        candidate_id: {
            "stressed": {
                "pass_rate": 0.10,
                "target_progress_p25": 0.40,
                "net_total": 1_000.0 - index,
                "mll_breach_rate": 0.0,
            }
        }
        for index, candidate_id in enumerate(candidate_ids)
    }
    clustering = report_module._cumulative_behavior_clusters(profiles, candidates)
    assert clustering["full_192_start_contract_satisfied"] is True
    assert clustering["cluster_count"] == 6
    assert clustering["pairwise_diagnostic_count"] == 28
    assert clustering["expected_pairwise_diagnostic_count"] == 28
    assert clustering["pairwise_coverage_complete"] is True
    assert clustering[
        "complete_link_partition_rederived_from_published_pairwise"
    ] is True
    assert clustering["pairwise_diagnostics"] == sorted(
        clustering["pairwise_diagnostics"],
        key=lambda row: (row["left_policy_id"], row["right_policy_id"]),
    )
    assert canonical_hash(clustering["pairwise_diagnostics"]) == clustering[
        "pairwise_diagnostics_sha256"
    ]
    report_module._validate_cumulative_behavior_clustering_payload(
        clustering, expected_candidate_ids=candidate_ids
    )
    assert {
        frozenset(row["member_ids"]) for row in clustering["clusters"]
    } == {
        frozenset({"finalist-0", "finalist-1"}),
        frozenset({"finalist-2", "finalist-3"}),
        frozenset({"finalist-4"}),
        frozenset({"finalist-5"}),
        frozenset({"finalist-6"}),
        frozenset({"finalist-7"}),
    }
    assert len(
        {profile["public"][fingerprint] for profile in profiles.values()}
    ) == 7
    account_same_routes_different = report_module._cumulative_behavior_similarity(
        profiles["finalist-4"], profiles["finalist-5"]
    )
    assert account_same_routes_different[0] == pytest.approx(1.0)
    assert account_same_routes_different[1] == pytest.approx(0.0)
    assert account_same_routes_different[3] < 0.90
    assert account_same_routes_different[4] < 0.90
    assert account_same_routes_different[5] is False

    hash_tamper = json.loads(json.dumps(clustering))
    hash_tamper["pairwise_diagnostics"][0][
        "account_vector_correlation"
    ] -= 0.01
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="pairwise diagnostics hash drift",
    ):
        report_module._validate_cumulative_behavior_clustering_payload(
            hash_tamper, expected_candidate_ids=candidate_ids
        )

    partition_tamper = json.loads(json.dumps(clustering))
    joined_pair = next(
        row
        for row in partition_tamper["pairwise_diagnostics"]
        if (row["left_policy_id"], row["right_policy_id"])
        == ("finalist-0", "finalist-1")
    )
    joined_pair["routing_jaccard"] = 0.0
    joined_pair["similar"] = False
    partition_tamper["pairwise_diagnostics_sha256"] = canonical_hash(
        partition_tamper["pairwise_diagnostics"]
    )
    split_groups = report_module._complete_link_groups_from_pairwise(
        candidate_ids, partition_tamper["pairwise_diagnostics"]
    )
    assert ["finalist-0"] in split_groups
    assert ["finalist-1"] in split_groups
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="published complete-link partition drift",
    ):
        report_module._validate_cumulative_behavior_clustering_payload(
            partition_tamper, expected_candidate_ids=candidate_ids
        )


def test_episode_dataset_accounting_separates_attempts_from_horizon_rows() -> None:
    bundle = {
        "dataset_row_counts": {"episodes": 152_064},
        "files": {
            "stage2": {
                "kind": "dataset_partition",
                "dataset": "episodes",
                "batch_id": "active:stage2-eliminated:000000:episodes",
                "row_count": 6_144,
            },
            "stage3": {
                "kind": "dataset_partition",
                "dataset": "episodes",
                "batch_id": "active:stage3:000000:episodes",
                "row_count": 122_880,
            },
            "stage4": {
                "kind": "dataset_partition",
                "dataset": "episodes",
                "batch_id": "active:stage4:000000:episodes",
                "row_count": 15_360,
            },
            "stage5": {
                "kind": "dataset_partition",
                "dataset": "episodes",
                "batch_id": "active:stage5:000000:episodes",
                "row_count": 7_680,
            },
        },
    }
    accounting = report_module._episode_dataset_accounting(
        bundle, canonical_attempt_count=35_328
    )
    assert accounting["canonical_account_episode_attempts"] == 35_328
    assert accounting["persisted_multi_horizon_episode_rows"] == 152_064
    assert accounting["per_stage"]["stage5"][
        "derived_canonical_account_attempts"
    ] == 1_536
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="canonical attempt counter diverges",
    ):
        report_module._episode_dataset_accounting(
            bundle, canonical_attempt_count=152_064
        )


def test_campaign_lifecycle_exact_transition_path_payout_semantics() -> None:
    proof = {
        "combine_to_xfa_transition_count": 25_019,
        "alternative_path_count": 50_038,
        "first_payout_path_observation_count": 48_922,
        "payout_cycle_observation_count": 149_254,
        "trader_90_percent_split_cash_observations_before_fees_tax": (
            288_381_857.41232216
        ),
        "post_payout_survival_observation_count": 47_000,
        "transition_key_uniqueness_proved": True,
        "full_episode_key_uniqueness_proved": True,
        "zero_inter_stage_overlap_proved": True,
        "full_pass_lifecycle_bijection_proved": True,
        "exactly_two_alternative_paths_per_transition_proved": True,
        "path_key_uniqueness_proved": True,
        "first_payout_uniqueness_per_path_proved": True,
        "canonical_event_to_summary_reconciliation_proved": True,
        "standard_consistency_alternatives_kept_separate": True,
        "official_rule_snapshot_exact": True,
        "payout_eligibility_amount_and_reset_reexecuted_from_daily_ledger": True,
        "by_scenario_and_path": {
            "normal": {"standard": {}, "consistency": {}},
            "stressed": {"standard": {}, "consistency": {}},
        },
    }
    lifecycle = report_module._sealed_campaign_wide_lifecycle_totals(
        {
            "xfa_paths_started": 25_019,
            "first_payouts": 48_922,
            "payout_cycles": 149_254,
            "trader_net_payout": 288_381_857.41232216,
        },
        sealed_episode_audit=proof,
    )
    audit = lifecycle["transition_and_alternative_path_audit"]
    assert audit["combine_to_xfa_transition_count"] == 25_019
    assert audit["expected_standard_plus_consistency_path_count"] == 50_038
    assert audit["first_payout_path_observation_count"] == 48_922
    assert audit["alternative_paths_without_observed_first_payout"] == 1_116
    assert audit["first_payout_observations_are_combine_to_xfa_transitions"] is False
    assert audit["duplicate_transition_inflation_detected"] is False
    assert audit["duplicate_transition_verdict_basis"] == (
        "PROVED_FROM_DEEP_VERIFIED_EPISODE_PARTITIONS"
    )
    assert audit["probability_denominator_status"].startswith("AVAILABLE_BY_COST")


def test_rehashed_finalist_frontier_cannot_diverge_from_expanded_raw_metrics(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path, tamper_finalist_frontier_consistency=True)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="sealed frontier/raw consistency_ok_count drift",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_legacy_frontier_behavior_hash_must_match_raw_runtime_merge(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path, tamper_finalist_frontier_behavior=True)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="legacy frontier behavior fingerprint cannot be rederived",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
        )


def test_runtime_behavior_hash_preserves_persisted_causal_routing_order() -> None:
    raw = next(
        value
        for value in _raw_rows()
        if value["scenario"] == "NORMAL"
        and value["horizon_label"] == "90_TRADING_DAYS"
    )
    persisted_routing = list(reversed(raw["risk_allocation_path"]))
    raw["risk_allocation_path"] = persisted_routing

    expected = canonical_hash(
        {
            "normal": [
                {
                    "start": int(raw["start_day"]),
                    "terminal": "PASSED",
                    "accepted": int(raw["accepted_events"]),
                    "skipped": int(raw["skipped_events"]),
                    "quantity_path": [
                        [
                            str(decision["event_id"]),
                            int(decision["quantity"]),
                            str(decision["decision_status"]),
                        ]
                        for decision in persisted_routing
                    ],
                }
            ],
            "stressed": [],
        }
    )
    diagnostic_projection_order = canonical_hash(
        {
            "normal": [
                {
                    "start": int(raw["start_day"]),
                    "terminal": "PASSED",
                    "accepted": int(raw["accepted_events"]),
                    "skipped": int(raw["skipped_events"]),
                    "quantity_path": [
                        [
                            str(decision["event_id"]),
                            int(decision["quantity"]),
                            str(decision["decision_status"]),
                        ]
                        for decision in report_module._canonical_daily_routing(raw)
                    ],
                }
            ],
            "stressed": [],
        }
    )

    actual = report_module._runtime_behavior_fingerprint_from_raw([raw])
    assert actual == expected
    assert actual != diagnostic_projection_order


def test_campaign_lifecycle_rejects_duplicate_full_episode_key() -> None:
    accumulator = report_module.CampaignLifecycleAuditAccumulator()
    episode = _sealed_full_pass_episode()
    accumulator.add_full_episode(episode, stage="stage3")
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="duplicate or inter-stage FULL episode",
    ):
        accumulator.add_full_episode(episode, stage="stage3")


def test_campaign_lifecycle_rejects_inter_stage_overlap() -> None:
    accumulator = report_module.CampaignLifecycleAuditAccumulator()
    episode = _sealed_full_pass_episode()
    accumulator.add_full_episode(episode, stage="stage3")
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match=r"stage3/stage4",
    ):
        accumulator.add_full_episode(episode, stage="stage4")


def test_campaign_lifecycle_rejects_missing_alternative_path() -> None:
    accumulator = report_module.CampaignLifecycleAuditAccumulator()
    episode = _sealed_full_pass_episode()
    lifecycle = episode["active_risk_pool_lifecycle"]
    assert isinstance(lifecycle, dict)
    lifecycle.pop("consistency")
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="lifecycle evidence is incomplete: consistency",
    ):
        accumulator.add_full_episode(episode, stage="stage3")


def test_campaign_lifecycle_rejects_summary_raw_total_corruption() -> None:
    proof = {
        "combine_to_xfa_transition_count": 2,
        "alternative_path_count": 2,
        "first_payout_path_observation_count": 2,
        "payout_cycle_observation_count": 2,
        "trader_90_percent_split_cash_observations_before_fees_tax": 1_350.0,
        "post_payout_survival_observation_count": 2,
        "transition_key_uniqueness_proved": True,
        "full_episode_key_uniqueness_proved": True,
        "zero_inter_stage_overlap_proved": True,
        "full_pass_lifecycle_bijection_proved": True,
        "exactly_two_alternative_paths_per_transition_proved": True,
        "path_key_uniqueness_proved": True,
        "first_payout_uniqueness_per_path_proved": True,
        "canonical_event_to_summary_reconciliation_proved": True,
        "standard_consistency_alternatives_kept_separate": True,
        "official_rule_snapshot_exact": True,
        "payout_eligibility_amount_and_reset_reexecuted_from_daily_ledger": True,
    }
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="summary lifecycle counts diverge from episode proof",
    ):
        report_module._sealed_campaign_wide_lifecycle_totals(
            {
                "xfa_paths_started": 1,
                "first_payouts": 2,
                "payout_cycles": 2,
                "trader_net_payout": 1_350.0,
            },
            sealed_episode_audit=proof,
        )


def test_trade_attribution_is_bound_to_sealed_source_quantity_and_pnl() -> None:
    raw = next(
        row
        for row in _candidate("candidate-a")["evidence_raw"]
        if row["scenario"] == "NORMAL"
        and row["horizon_label"] == "90_TRADING_DAYS"
    )
    decisions = raw["risk_allocation_path"]
    assert isinstance(decisions, list)
    source_index = {
        ("sleeve", str(decision["event_id"])): {
            "quantity": int(decision["base_quantity"]),
            "gross_pnl": float(raw["net_pnl"]) / len(decisions),
            "costs": 0.0,
            "net_pnl": float(raw["net_pnl"]) / len(decisions),
        }
        for decision in decisions
    }
    attribution = report_module._trade_attribution_from_routing(
        raw,
        component_trade_index=source_index,
        label="synthetic trade audit",
    )
    assert sum(attribution["component_pnl"].values()) == pytest.approx(
        raw["net_pnl"]
    )
    first_key = next(iter(source_index))
    source_index[first_key]["quantity"] = int(source_index[first_key]["quantity"]) + 1
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="base/source quantity drift",
    ):
        report_module._trade_attribution_from_routing(
            raw,
            component_trade_index=source_index,
            label="synthetic trade audit",
        )


def test_temporal_block_contract_and_nonoverlap_fail_closed() -> None:
    manifest = {
        "campaign_id": CAMPAIGN,
        "temporal_blocks": {
            "blocks": [
                {
                    "block_id": f"B{index}",
                    "start": f"2023-0{index}-01",
                    "end": f"2023-0{index}-28",
                    "markets": ["NQ"],
                    "contract_separation": "EXPLICIT",
                }
                for index in range(1, 5)
            ]
        },
    }
    report_module._block_specs(manifest)
    manifest["temporal_blocks"]["blocks"][1]["start"] = "2023-01-15"
    with pytest.raises(ActiveRiskDecisionReportError, match="source blocks overlap"):
        report_module._block_specs(manifest)


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
    accounting = report["production_context"]["episode_dataset_accounting"]
    assert accounting["canonical_account_episode_attempts"] == 192
    assert accounting["persisted_multi_horizon_episode_rows"] == 960
    assert accounting["per_stage"]["stage3"] == {
        "partition_count": 2,
        "persisted_episode_rows": 960,
        "frozen_horizon_multiplicity": 5,
        "derived_canonical_account_attempts": 192,
    }
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

    stage3_lifecycle = report["stage3_xfa_lifecycle"]
    assert stage3_lifecycle["scope"].startswith("STAGE3_ONLY")
    normal_standard = stage3_lifecycle["normal"]["standard"]
    normal_consistency = stage3_lifecycle["normal"]["consistency"]
    stressed_standard = stage3_lifecycle["stressed"]["standard"]
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
        == 5.0
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
    assert report["integrity"][
        "expanded_finalist_decision_metrics_rederived_from_raw_caches"
    ] is True
    global_lifecycle = report["campaign_wide_sealed_xfa_lifecycle_totals"]
    assert global_lifecycle["scope"] == "CAMPAIGN_WIDE_SEALED_STAGE3_STAGE4_STAGE5"
    assert global_lifecycle["totals"]["xfa_paths_started"] == 4
    assert global_lifecycle["totals"]["xfa_paths_started"] == sum(
        stage3_lifecycle[scenario]["standard"]["xfa_paths_started"]
        for scenario in ("normal", "stressed")
    )
    assert global_lifecycle["totals"]["first_payouts"] == 8
    assert global_lifecycle["transition_and_alternative_path_audit"] == {
        "combine_to_xfa_transition_count": 4,
        "alternative_path_multiplier": 2,
        "expected_standard_plus_consistency_path_count": 8,
        "first_payout_path_observation_count": 8,
        "alternative_paths_without_observed_first_payout": 0,
        "transition_to_alternative_path_identity_valid": True,
        "first_payout_observations_within_alternative_path_bound": True,
        "first_payout_observations_are_combine_to_xfa_transitions": False,
        "first_payouts_above_transition_count_can_be_expected": True,
        "duplicate_transition_inflation_detected": False,
        "legacy_subminimum_marker_count": 0,
        "legacy_subminimum_marker_gross": 0.0,
        "legacy_subminimum_marker_affected_finalist_ids": [],
        "duplicate_transition_verdict_basis": (
            "PROVED_FROM_DEEP_VERIFIED_EPISODE_PARTITIONS"
        ),
        "semantics": (
            "ONE_UNIQUE_COMBINE_TO_XFA_TRANSITION_FANS_OUT_TO_TWO_MUTUALLY_"
            "EXCLUSIVE_DIAGNOSTIC_PATHS;FIRST_PAYOUTS_COUNT_SUCCESSFUL_PATH_"
            "OBSERVATIONS_NOT_UNIQUE_TRANSITIONS_OR_SIMULTANEOUS_REALIZABLE_"
            "PAYOUTS"
        ),
        "probability_denominator_status": (
            "AVAILABLE_BY_COST_SCENARIO_AND_PREDECLARED_PATH_FROM_DEEP_"
            "VERIFIED_EPISODE_PARTITIONS"
        ),
    }
    sealed_proof = global_lifecycle["sealed_episode_lifecycle_proof"]
    assert sealed_proof["full_episode_count"] == 192
    assert sealed_proof["combine_to_xfa_transition_count"] == 4
    assert sealed_proof["alternative_path_count"] == 8
    assert sealed_proof["stage_full_episode_counts"] == {"stage3": 192}
    assert sealed_proof["zero_inter_stage_overlap_proved"] is True
    assert (
        sealed_proof[
            "payout_eligibility_amount_and_reset_reexecuted_from_daily_ledger"
        ]
        is True
    )
    assert sealed_proof["by_scenario_and_path"]["normal"]["standard"][
        "combine_attempts"
    ] == 96
    assert global_lifecycle["optional_path_and_survival_breakdown"] == {
        "xfa_standard_paths": 4,
        "xfa_consistency_paths": 4,
        "post_payout_survival_count": 8,
        "post_payout_survival_rate": 1.0,
    }
    assert (
        report["production_context"]["source"]
        == "TWO_PREREGISTERED_DEEP_GUARDS_REUSED_PLUS_REPORT_RELATIONAL_REDERIVATION"
    )

    clustering = report["posthoc_behavioral_clustering"]
    assert clustering["candidate_count"] == 2
    assert clustering["cluster_count"] == 1
    assert clustering["clusters"][0]["member_ids"] == ["candidate-a", "candidate-b"]
    assert clustering["promotion_or_selection_effect"] is False

    expanded = report["expanded_development_finalists"]
    assert expanded["finalist_count"] == 1
    assert expanded["expanded_matched_controls_status"] == (
        "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED"
    )
    finalist = expanded["rows"][0]
    assert finalist["policy_id"] == "candidate-a"
    assert finalist["starts_per_scenario"] == 48
    assert finalist["effective_independent_source_block_count"] == 4
    assert finalist["expanded_exact_account_behavior_cluster"] == (
        "expanded_exact_account_cluster_01"
    )
    assert finalist["expanded_economic_behavior_cluster"].startswith(
        "expanded_economic_behavior_"
    )
    assert finalist["legacy_frontier_behavior_fingerprint_rederived_exactly"] is True
    assert finalist["cumulative_account_trade_behavior"][
        "authoritative_raw_account_trade_behavior_fingerprint"
    ] == finalist["sealed_cumulative_account_behavior_fingerprint"]
    assert finalist["cumulative_account_trade_behavior"][
        "per_scenario_observation_count"
    ] == {"normal": 48, "stressed": 48}
    assert expanded["cumulative_192_economic_behavior_cluster_count"] == 1
    assert finalist["stage3_matched_control_deltas"]["scope"] == (
        "STAGE3_ONLY_48_MATCHED_STARTS"
    )
    assert finalist["stage3_matched_control_deltas"][
        "matched_starts_per_scenario"
    ] == 48
    assert finalist["stage3_matched_control_deltas"]["expanded_192_status"] == (
        "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED"
    )
    relational = finalist["normal"]["expanded_raw_relational_validation"]
    assert relational["decision_metrics_match"] is True
    assert relational["status"] == (
        "EXACTLY_REDERIVED_FROM_STAGE3_STAGE4_STAGE5_RAW_CACHES"
    )
    assert relational[
        "target_progress_distribution_exact_rederived"
    ]["count"] == 48
    assert relational["days_to_target_distribution_exact_rederived"]["count"] == 1
    assert finalist["normal"]["consistency_ok_count"] == 36
    coverage = finalist["source_block_coverage_exact"]
    assert coverage["common_covered_block_ids"] == ["B1", "B2", "B3", "B4"]
    raw_risk = finalist["risk_utilisation_exact_rederived"]
    assert raw_risk["by_scenario"]["normal"]["observation_count"] == 192
    assert raw_risk["by_scenario"]["stressed"]["observation_count"] == 192
    assert raw_risk["total_all_scenarios"]["observation_count"] == 384
    raw_suppression = finalist["suppression_exact_rederived"]
    assert raw_suppression["by_scenario"]["normal"]["signals_emitted"] == 96
    assert raw_suppression["by_scenario"]["stressed"]["signals_emitted"] == 96
    assert raw_suppression["by_scenario_and_component"]["normal"]["sleeve"][
        "signals_emitted"
    ] == 96
    trade_concentration = finalist["stressed"]["concentration"][
        "trade_concentration"
    ]
    assert trade_concentration["selection_gate_auditable"] is True
    assert trade_concentration["accepted_account_trade_observation_count"] == 96
    assert trade_concentration["unique_immutable_source_trade_count"] == 96
    assert trade_concentration["positive_source_trade_profit_denominator"] > 0.0
    assert finalist["stressed"]["day_concentration_exact"][
        "maximum_positive_session_day_aggregate_share"
    ] > 0.0
    assert set(finalist["stressed"]["market_attribution"]) == {"NQ"}
    assert finalist["stressed"]["concentration"][
        "maximum_market_positive_profit_share"
    ] == 1.0
    stressed_component = finalist["component_contribution_exact_rederived"][
        "stressed"
    ]
    assert stressed_component["scope"] == (
        "COMBINE_CANONICAL_90_DAY_STAGE3_STAGE4_STAGE5_ONLY;"
        "XFA_COMPONENT_ATTRIBUTION_NOT_AGGREGATED"
    )
    assert stressed_component["additive_account_net_reconciliation"] is True
    assert stressed_component["total"] == pytest.approx(
        stressed_component["account_net_pnl_total"]
    )
    assert finalist["stressed"]["component_attribution_scope"] == (
        stressed_component["scope"]
    )
    assert finalist["stressed"][
        "component_attribution_additive_account_net_reconciliation"
    ] is True
    assert "XFA component attribution is not aggregated" in report[
        "known_interpretation_limits"
    ][-1]

    frozen = report["frozen_finalist_policy_specs"]
    assert frozen["finalist_count"] == 1
    frozen_policy = frozen["policy_specs"][0]
    assert frozen_policy["policy_id"] == "candidate-a"
    assert frozen_policy["membership_row_count"] == 1
    assert frozen_policy["membership_rows_all_contain_identical_policy"] is True
    assert frozen_policy["active_risk_policy"]["policy_version"] == (
        "hydra_active_risk_pool_governor_v1"
    )
    frozen_sleeve = frozen_policy["membership"][0]
    assert frozen_sleeve["behavioral_fingerprint"]
    assert frozen_sleeve["signal_ledger_sha256"] == "1" * 64
    assert frozen_sleeve["trade_ledger_sha256"] == "2" * 64
    assert frozen_sleeve["market"] == "NQ"
    assert frozen_sleeve["contract"] == "NQH3"
    assert frozen_sleeve["timeframe"] == "1m"
    assert frozen_sleeve["session"] == "OPEN"
    assert frozen_sleeve["source_campaign"] == "synthetic_source"
    assert frozen_policy["combine_book"]["book"] == "COMBINE_BOOK"
    assert frozen_policy["xfa_standard_book"]["book"] == "XFA_STANDARD_BOOK"
    assert frozen_policy["xfa_consistency_book"]["book"] == (
        "XFA_CONSISTENCY_BOOK"
    )
    assert frozen_policy["xfa_standard_book"]["xfa_profile"]["fingerprint"]
    assert frozen_policy["xfa_standard_book"]["rule_snapshot"] == (
        official_rule_snapshot_2026_07_15().to_dict()
    )

    checked = dict(report)
    claimed = checked.pop("report_hash")
    assert canonical_hash(checked) == claimed
    markdown = render_markdown(report)
    assert "Expected trader payout" in markdown
    assert "Stage-3-only XFA lifecycle" in markdown
    assert "Campaign-wide sealed XFA lifecycle totals" in markdown
    assert "Expanded development finalists" in markdown
    assert "192 attempts versus 960 multi-horizon rows" in markdown
    assert "not one realizable combined trader path" in markdown
    assert "overlapping rolling episode starts are not independent" in markdown
    assert "B1" in markdown
    assert "candidate-a" in markdown


def test_explicit_finalist_cardinality_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="development finalist count is 1, expected 8",
    ):
        build_active_risk_decision_report(
            manifest_path=paths["manifest"],
            stage3_cache_dir=paths["stage3"],
            matched_controls_path=paths["controls"],
            halving_dir=paths["halving"],
            expected_stage3_count=2,
            expected_finalist_count=8,
        )


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
    assert context["source"] == (
        "TWO_PREREGISTERED_DEEP_GUARDS_REUSED_PLUS_REPORT_RELATIONAL_REDERIVATION"
    )
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


def test_valid_but_different_xfa_profile_fails_frozen_book_binding(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    cache = paths["stage3"] / "batch_000000.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    lifecycle = payload["rows"][0]["lifecycle_rows"][0]
    profile = lifecycle["xfa_profile"]
    profile["risk_multiplier"] = 1.15
    profile.pop("fingerprint")
    profile["fingerprint"] = canonical_hash(profile)
    lifecycle["xfa_profile_projection"]["risk_multiplier"] = 1.15
    _reseal_lifecycle(lifecycle)
    _reseal_batch(cache, payload)
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="XFA profile differs from the frozen finalist books",
    ):
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


def test_daily_exposure_tie_order_is_diagnostic_but_bounds_remain_strict() -> None:
    raw = _raw_rows()[0]
    raw["maximum_mini_equivalent"] = 1.2
    raw["maximum_net_directional_exposure"] = 0.9
    for day in raw["daily_path"]:
        day["exposure"]["maximum_mini_equivalent"] = 0.6
        day["exposure"]["maximum_net_directional"] = 0.6

    report_module._validate_raw_daily_derivations(
        raw,
        label="coincident-entry-exit-order",
    )

    raw["maximum_net_directional_exposure"] = 1.3
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="exposure bounds are internally inconsistent",
    ):
        report_module._validate_raw_daily_derivations(
            raw,
            label="invalid-authoritative-exposure",
        )


def test_cached_diagnostic_float_reduction_uses_frozen_tolerance() -> None:
    cached = {
        "count": 12,
        "foregone_realized_pnl_ex_post": 133_617.31499999634,
        "status": {"AVAILABLE": False},
    }
    rederived = {
        "count": 12,
        "foregone_realized_pnl_ex_post": 133_617.31499999630,
        "status": {"AVAILABLE": False},
    }
    assert report_module._nested_evidence_equal(cached, rederived)
    assert report_module._nested_evidence_equal(
        {"time_weighted_exposure": 3_097_133_815_551.5376},
        {"time_weighted_exposure": 3_097_133_815_551.5370},
    )

    rederived["foregone_realized_pnl_ex_post"] += 1e-4
    assert not report_module._nested_evidence_equal(cached, rederived)


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
            policy_id="active_pool_test_zero_observation",
            scenario="NORMAL",
            combine_start_id=19_900,
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
            policy_id="active_pool_test_chronology",
            scenario="NORMAL",
            combine_start_id=19_900,
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
            policy_id="active_pool_test_ledger_chronology",
            scenario="NORMAL",
            combine_start_id=19_900,
            combine_end_day=19_999,
            xfa_start_day=20_000,
            rule_snapshot=official_rule_snapshot_2026_07_15().to_dict(),
        )


def test_xfa_payout_eligibility_cannot_be_more_permissive_than_rules() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    ledger = path["daily_ledger"]
    assert isinstance(ledger, list)
    ledger[0]["payout_eligible"] = True
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match=(
            "canonical payout-event reconciliation failed: "
            "XFA payout request timing drift"
        ),
    ):
        report_module._validate_xfa_path_accounting(
            path,
            label="permissive-payout",
            policy_id="active_pool_test_permissive_payout",
            scenario="NORMAL",
            combine_start_id=19_900,
            combine_end_day=19_999,
            xfa_start_day=20_000,
            rule_snapshot=official_rule_snapshot_2026_07_15().to_dict(),
        )


def test_xfa_pre_payout_mll_floor_cannot_be_artificially_relaxed() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    ledger = path["daily_ledger"]
    assert isinstance(ledger, list)
    ledger[0]["mll_floor_close"] = -4_500.0
    ledger[1]["mll_floor_open"] = -4_500.0
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="end-of-day trailing MLL floor",
    ):
        report_module._validate_xfa_path_accounting(
            path,
            label="relaxed-mll",
            policy_id="active_pool_test_relaxed_mll",
            scenario="NORMAL",
            combine_start_id=19_900,
            combine_end_day=19_999,
            xfa_start_day=20_000,
            rule_snapshot=official_rule_snapshot_2026_07_15().to_dict(),
        )


def test_xfa_terminal_row_must_be_final_and_match_path_terminal() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    ledger = path["daily_ledger"]
    assert isinstance(ledger, list)
    ledger[10]["terminal"] = "MLL_BREACHED"
    path.update(
        {
            "terminal": "MLL_BREACHED",
            "terminal_reason": "synthetic_midstream_terminal",
            "post_payout_survived": False,
        }
    )
    with pytest.raises(
        ActiveRiskDecisionReportError,
        match="terminal row/path chronology drift",
    ):
        report_module._validate_xfa_path_accounting(
            path,
            label="post-mortem",
            policy_id="active_pool_test_terminal_chronology",
            scenario="NORMAL",
            combine_start_id=19_900,
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
        policy_id="active_pool_test_valid_zero_observation",
        scenario="NORMAL",
        combine_start_id=19_900,
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
            policy_id="active_pool_test_invalid_zero_observation",
            scenario="NORMAL",
            combine_start_id=19_900,
            combine_end_day=19_999,
            xfa_start_day=None,
            rule_snapshot=rules,
        )


def test_xfa_source_rule_failure_reconciles_one_fatal_unclassified_event() -> None:
    path = _xfa_path(path="XFA_STANDARD", net=900.0, start_day=20_000)
    ledger = path["daily_ledger"]
    assert isinstance(ledger, list)
    ledger[-1]["terminal"] = "HARD_RULE_FAILURE"
    path.update(
        {
            "terminal": "HARD_RULE_FAILURE",
            "terminal_reason": "source_contract_limit_violation",
            "event_count": 6,
            "post_payout_survived": False,
        }
    )
    report_module._validate_xfa_path_accounting(
        path,
        label="fatal-source-event",
        policy_id="active_pool_test_fatal_source_event",
        scenario="NORMAL",
        combine_start_id=19_900,
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


def _synthetic_payout_reconciliation(
    *,
    policy_id: str,
    scenario: str,
    combine_start_id: int,
    payout_cycles: int,
    trader_net_payout: float,
) -> PayoutPathReconciliation:
    events: list[CanonicalPayoutEvent] = []
    if payout_cycles:
        net_per_cycle = trader_net_payout / payout_cycles
        gross_per_cycle = net_per_cycle / 0.9
        pre_payout_balance = gross_per_cycle / 0.5
        for cycle in range(1, payout_cycles + 1):
            events.append(
                CanonicalPayoutEvent.create(
                    policy_id=policy_id,
                    scenario=scenario,
                    combine_start_id=combine_start_id,
                    xfa_path="XFA_STANDARD",
                    payout_cycle=cycle,
                    eligibility_timestamp=combine_start_id + cycle * 5,
                    eligible_account_balance=pre_payout_balance,
                    gross_payout_request=gross_per_cycle,
                    balance_fraction_limit=gross_per_cycle,
                    account_size_payout_cap=5_000.0,
                    payout_split=0.9,
                    trader_net_payout=net_per_cycle,
                    costs_or_fees=0.0,
                    pre_payout_balance=pre_payout_balance,
                    post_payout_balance=pre_payout_balance - gross_per_cycle,
                    mll_before_payout=0.0,
                    mll_after_payout=0.0,
                    reset_marker=True,
                )
            )
    return PayoutPathReconciliation(
        schema=CANONICAL_PAYOUT_RECONCILIATION_SCHEMA,
        policy_id=policy_id,
        scenario=scenario,
        combine_start_id=str(combine_start_id),
        xfa_path="XFA_STANDARD",
        payout_events=tuple(events),
        legacy_subminimum_marker_amounts=(),
        legacy_subminimum_marker_count=0,
        legacy_subminimum_marker_gross=0.0,
        canonical_gross_payout=sum(
            event.gross_payout_request for event in events
        ),
        canonical_trader_net_payout=sum(
            event.trader_net_payout for event in events
        ),
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
        },
        payout_reconciliation=_synthetic_payout_reconciliation(
            policy_id="active_pool_accumulator_zero_observation",
            scenario="NORMAL",
            combine_start_id=20_000,
            payout_cycles=0,
            trader_net_payout=0.0,
        ),
    )
    value = accumulator.to_dict()
    assert value["zero_observation_xfa_paths"] == 1
    assert value["minimum_mll_buffer"]["missing_count"] == 1
    assert value["minimum_mll_buffer"]["all_nonmissing_paths"]["count"] == 0
    assert value["payout_cycles_by_path"]["all_started_paths"]["count"] == 1
    assert value["payout_cycles_by_path"]["on_censored_paths"]["count"] == 1
    assert (
        value["payout_cycles_by_path"]["before_observed_account_closure"]["count"]
        == 0
    )
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
        },
        payout_reconciliation=_synthetic_payout_reconciliation(
            policy_id="active_pool_accumulator_payout_then_censored",
            scenario="NORMAL",
            combine_start_id=20_100,
            payout_cycles=1,
            trader_net_payout=900.0,
        ),
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
    payout_then_censored_value = payout_then_censored.to_dict()
    assert (
        payout_then_censored_value["payout_cycles_by_path"]["on_censored_paths"][
            "median"
        ]
        == 1.0
    )

    observed_closure = report_module.LifecyclePathAccumulator()
    observed_closure.add_combine_episode(
        {"terminal_classification": "TARGET_REACHED", "passed": True}
    )
    observed_closure.add_path(
        {
            "terminal": "MLL_BREACHED",
            "observed_days": 20,
            "payout_eligible": True,
            "payout_cycles": 2,
            "trader_net_payout": 1800.0,
            "post_payout_survived": False,
            "post_payout_censored": False,
            "minimum_mll_buffer": 0.0,
            "first_payout_day": 5,
        },
        payout_reconciliation=_synthetic_payout_reconciliation(
            policy_id="active_pool_accumulator_observed_closure",
            scenario="NORMAL",
            combine_start_id=20_200,
            payout_cycles=2,
            trader_net_payout=1_800.0,
        ),
    )
    closure_distribution = observed_closure.to_dict()["payout_cycles_by_path"]
    assert closure_distribution["before_observed_account_closure"]["count"] == 1
    assert closure_distribution["before_observed_account_closure"]["median"] == 2.0
    assert closure_distribution["on_censored_paths"]["count"] == 0


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
    assert build["stage4_cache_dir"].name == "stage4_active_batches"
    assert build["stage5_cache_dir"].name == "stage5_active_batches"
    assert build["final_result_path"] == (
        tmp_path
        / "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02"
        / "economic_production_result.json"
    )
    assert build["production_state_path"].name == "production_state.json"
    assert build["expected_finalist_starts_per_scenario"] == 192
    assert build["expected_finalist_count"] == 8
    written = captured["write"]
    assert isinstance(written, dict)
    assert written["json_path"].parent.name == (
        "active_risk_pool_target_velocity_0026_revision_02"
    )
