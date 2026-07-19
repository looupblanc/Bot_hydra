"""Exact read-only account tripwire for one frozen cross-ecology daily signal.

The branch is deliberately narrow.  It does not regenerate the legacy 720-row
grammar or select a new candidate.  It hash-binds the one preselected CL->YM
candidate, reconstructs its MYM path extrema from the source minute bars, and
runs the current exact 50K/100K/150K Combine rules over frozen non-overlapping
5/10/20-day windows.  A direction flip is evaluated on the identical event
clock as a cheap matched control.

All evidence is already-viewed development evidence.  This module performs no
writes and cannot promote a candidate.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_exact_replay as exact
from hydra.production.fresh_confirmation_lane import non_overlapping_starts
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent, run_combine_episode
from hydra.propfirm.scaling_plan import mini_equivalent


SCHEMA = "hydra_clean_cross_asset_daily_tripwire_v1"
BRANCH_ID = "CLEAN_CROSS_ASSET_DAILY_DIRECTION_TRANSFER_TRIPWIRE_V1"
DEFAULT_CARD = Path("config/research/clean_cross_asset_daily_tripwire_decision_card_v1.json")
PRIMARY_ID = "strategy_daily_cross_CL_to_YM_source_prior_trend_continuation_q80_h120_v1"
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
CONTROLS = ("PRIMARY", "DIRECTION_FLIP")


class CleanCrossAssetDailyTripwireError(RuntimeError):
    """The frozen source or causal/account contract drifted."""


def load_decision_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = _read_json(Path(path))
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise CleanCrossAssetDailyTripwireError("decision-card hash drift")
    if (
        card.get("selected_branch") != BRANCH_ID
        or dict(card.get("candidate") or {}).get("candidate_id") != PRIMARY_ID
        or bool(dict(card["smallest_decisive_falsification_experiment"]).get("promotion_allowed"))
        or bool(dict(card["smallest_decisive_falsification_experiment"]).get("q4_access_allowed"))
        or bool(dict(card["smallest_decisive_falsification_experiment"]).get("data_purchase_allowed"))
    ):
        raise CleanCrossAssetDailyTripwireError("decision-card semantic drift")
    return card


def run_clean_cross_asset_daily_tripwire(
    root: str | Path,
    *,
    decision_card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Run the one-candidate exact tripwire without writing authoritative state."""

    project = Path(root).resolve()
    card_path = _inside(project, decision_card_path)
    card = load_decision_card(card_path)
    experiment = dict(card["smallest_decisive_falsification_experiment"])
    source_result_path = _verified(project, experiment, "source_result")
    source_ledger_path = _verified(project, experiment, "source_ledger")
    source_ohlcv_path = _verified(project, experiment, "source_ohlcv")
    rules_path = _verified(project, experiment, "rule_snapshot")

    source_result = _read_json(source_result_path)
    result_core = dict(source_result)
    source_result_hash = str(result_core.pop("result_hash", ""))
    if (
        source_result_hash != str(experiment["source_result_hash"])
        or stable_hash(result_core) != source_result_hash
    ):
        raise CleanCrossAssetDailyTripwireError("source result self-hash drift")
    source_candidate = _one(
        [row for row in source_result.get("candidates") or [] if row.get("candidate_id") == PRIMARY_ID],
        "source candidate",
    )
    if (
        int(source_candidate.get("events", -1)) != int(card["candidate"]["legacy_event_count"])
        or float(source_candidate.get("cost_stress_1_5x_net", -math.inf)) <= 0.0
        or int(source_candidate.get("supportive_temporal_folds", 0)) != 3
    ):
        raise CleanCrossAssetDailyTripwireError("legacy candidate evidence drift")

    rows = _read_primary_rows(source_ledger_path)
    market = _read_mym_bars(source_ohlcv_path, experiment)
    calendar = _calendar(market, experiment)
    starts = non_overlapping_starts(calendar, tuple(experiment["horizons_trading_days"]))
    event_inputs = tuple(_event_input(row, market) for row in rows)
    rules, rule_receipt = exact._load_rule_snapshot(rules_path)
    quantities = tuple(int(value) for value in experiment["micro_quantity_frontier"])
    if quantities != tuple(sorted(set(quantities))) or any(value <= 0 for value in quantities):
        raise CleanCrossAssetDailyTripwireError("micro quantity frontier drift")

    cells: list[dict[str, Any]] = []
    for account_label in experiment["account_sizes"]:
        rule = dict(rules[str(account_label)])
        config = exact._account_config(rule)
        for quantity in quantities:
            if quantity > int(rule["maximum_micro_contracts"]):
                continue
            for control in CONTROLS:
                scenario_events = {
                    scenario: tuple(
                        _trade_path_event(
                            row,
                            quantity=quantity,
                            scenario=scenario,
                            direction_flip=control == "DIRECTION_FLIP",
                        )
                        for row in event_inputs
                    )
                    for scenario in SCENARIOS
                }
                for horizon in experiment["horizons_trading_days"]:
                    outcomes: dict[str, Any] = {}
                    for scenario in SCENARIOS:
                        episodes = [
                            (
                                run_combine_episode(
                                    scenario_events[scenario],
                                    calendar,
                                    start_day=int(start_day),
                                    maximum_duration_days=int(horizon),
                                    config=config,
                                    maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
                                ),
                                _block(int(start_day)),
                            )
                            for start_day, _role in starts[int(horizon)]
                        ]
                        outcomes[scenario] = _summarize(episodes)
                    cells.append(
                        {
                            "candidate_id": PRIMARY_ID,
                            "control": control,
                            "account_label": str(account_label),
                            "account_size_usd": int(rule["account_size_usd"]),
                            "micro_quantity": quantity,
                            "mini_equivalent": float(mini_equivalent("MYM", quantity)),
                            "horizon_trading_days": int(horizon),
                            "full_coverage_start_count": len(starts[int(horizon)]),
                            "normal": outcomes["NORMAL"],
                            "stressed": outcomes["STRESSED_1_5X"],
                        }
                    )

    primary_cells = [row for row in cells if row["control"] == "PRIMARY"]
    best_observed = _best_cell(primary_cells)
    best_safe = _best_safe_cell(
        primary_cells,
        headline_horizon_trading_days=int(
            card["frozen_gate"]["headline_horizon_trading_days"]
        ),
        maximum_mll_breach_rate=float(
            card["frozen_gate"]["maximum_stressed_mll_breach_rate"]
        ),
    )
    matched = _one(
        [
            row
            for row in cells
            if row["control"] == "DIRECTION_FLIP"
            and row["account_label"] == best_safe["account_label"]
            and row["micro_quantity"] == best_safe["micro_quantity"]
            and row["horizon_trading_days"] == best_safe["horizon_trading_days"]
        ],
        "matched direction-flip cell",
    )
    gate = _gate(best_safe, matched, card["frozen_gate"])
    if gate["passed"]:
        status = "CROSS_ASSET_DAILY_TRIPWIRE_PASSED_DEVELOPMENT_ONLY"
        next_action = "FREEZE_EXACT_ACCOUNT_CELL_AND_REQUIRE_GENUINELY_FRESH_CONFIRMATION"
    else:
        status = "CROSS_ASSET_DAILY_DIRECTION_TRANSFER_FALSIFIED"
        next_action = "REALLOCATE_TO_NON_OHLCV_REPRESENTATION"

    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "evidence_role": "VIEWED_DEVELOPMENT_TRIPWIRE_ONLY",
        "candidate_id": PRIMARY_ID,
        "source_bindings": {
            "decision_card_path": str(card_path.relative_to(project)),
            "decision_card_hash": card["card_hash"],
            "source_result_hash": source_result_hash,
            "source_ledger_sha256": experiment["source_ledger_sha256"],
            "source_ohlcv_sha256": experiment["source_ohlcv_sha256"],
            "rule_snapshot": rule_receipt,
        },
        "event_count": len(event_inputs),
        "calendar_session_count": len(calendar),
        "start_counts": {str(key): len(value) for key, value in starts.items()},
        "account_cell_count": len(cells),
        # The observed pass-maximising cell is retained as an unsafe diagnostic.
        # Promotion/gating uses the feasible cell because MLL validity precedes
        # pass probability in the mission's economic ordering.
        "best_observed_primary_cell": best_observed,
        "best_safe_primary_cell": best_safe,
        "best_primary_cell": best_safe,
        "matched_direction_flip_cell": matched,
        "frozen_gate": gate,
        "cells": cells,
        "promotion_status": None,
        "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        "independent_confirmation_claimed": False,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": next_action,
    }
    return {**core, "result_hash": stable_hash(core)}


def _event_input(row: Mapping[str, Any], market: pd.DataFrame) -> dict[str, Any]:
    entry = pd.Timestamp(row["entry_timestamp"])
    exit_time = pd.Timestamp(row["exit_timestamp"])
    if entry.tzinfo is None or exit_time.tzinfo is None or exit_time <= entry:
        raise CleanCrossAssetDailyTripwireError("event timestamps are not causal UTC times")
    path = market.loc[
        market["timestamp"].ge(entry) & market["timestamp"].le(exit_time)
    ]
    if len(path) != 121 or not path["timestamp"].diff().dropna().eq(pd.Timedelta(minutes=1)).all():
        raise CleanCrossAssetDailyTripwireError("event does not have a complete 120-minute path")
    entry_price = float(row["entry_price"])
    exit_price = float(row["exit_price"])
    if not math.isclose(float(path.iloc[0]["open"]), entry_price, abs_tol=1e-9):
        raise CleanCrossAssetDailyTripwireError("ledger entry is not the causal bar open")
    if not math.isclose(float(path.iloc[-1]["close"]), exit_price, abs_tol=1e-9):
        raise CleanCrossAssetDailyTripwireError("ledger exit does not match the frozen close")
    side = int(float(row["side"]))
    if side not in {-1, 1}:
        raise CleanCrossAssetDailyTripwireError("event side is invalid")
    point_value = 0.5
    gross = side * (exit_price - entry_price) * point_value
    favorable = (
        (float(path["high"].max()) - entry_price) * point_value
        if side > 0
        else (entry_price - float(path["low"].min())) * point_value
    )
    adverse = (
        (float(path["low"].min()) - entry_price) * point_value
        if side > 0
        else (entry_price - float(path["high"].max())) * point_value
    )
    base_cost = float(row["cost"])
    if (
        not math.isclose(gross, float(row["gross_pnl"]), abs_tol=1e-8)
        or not math.isclose(gross - base_cost, float(row["net_pnl"]), abs_tol=1e-8)
        or not math.isclose(adverse - base_cost / 2.0, float(row["mae_dollars"]), abs_tol=1e-8)
    ):
        raise CleanCrossAssetDailyTripwireError("raw path does not reconcile to legacy ledger")
    local_exit = exit_time.tz_convert("America/Chicago")
    session_compliant = (local_exit.hour, local_exit.minute) <= (15, 10)
    return {
        "event_id": f"{PRIMARY_ID}:{row['event_session_id']}",
        "session_day": int(str(row["event_session_id"]).replace("-", "")),
        "entry_ns": int(entry.value),
        "exit_ns": int(exit_time.value),
        "side": side,
        "gross_one_micro": gross,
        "favorable_one_micro": favorable,
        "adverse_one_micro": adverse,
        "base_cost_one_micro": base_cost,
        "session_compliant": session_compliant,
    }


def _trade_path_event(
    row: Mapping[str, Any],
    *,
    quantity: int,
    scenario: str,
    direction_flip: bool,
) -> TradePathEvent:
    if scenario not in SCENARIOS:
        raise CleanCrossAssetDailyTripwireError("unknown cost scenario")
    multiplier = 1.5 if scenario == "STRESSED_1_5X" else 1.0
    cost = float(row["base_cost_one_micro"]) * multiplier
    if direction_flip:
        gross = -float(row["gross_one_micro"])
        favorable = -float(row["adverse_one_micro"])
        adverse = -float(row["favorable_one_micro"])
    else:
        gross = float(row["gross_one_micro"])
        favorable = float(row["favorable_one_micro"])
        adverse = float(row["adverse_one_micro"])
    quantity_value = int(quantity)
    return TradePathEvent(
        event_id=f"{row['event_id']}:{'FLIP' if direction_flip else 'PRIMARY'}:{scenario}:q{quantity_value}",
        decision_ns=int(row["entry_ns"]),
        exit_ns=int(row["exit_ns"]),
        session_day=int(row["session_day"]),
        net_pnl=(gross - cost) * quantity_value,
        gross_pnl=gross * quantity_value,
        worst_unrealized_pnl=(adverse - cost / 2.0) * quantity_value,
        best_unrealized_pnl=max((favorable - cost / 2.0) * quantity_value, 0.0),
        quantity=quantity_value,
        mini_equivalent=float(mini_equivalent("MYM", quantity_value)),
        regime="CROSS_ASSET_DAILY_DIRECTION_TRANSFER",
        session_compliant=bool(row["session_compliant"]),
        contract_limit_compliant=True,
    )


def _summarize(values: Sequence[tuple[Any, str]]) -> dict[str, Any]:
    episodes = [value for value, _block_id in values]
    pass_by_block = Counter(
        block_id for value, block_id in values if value.terminal is CombineTerminal.PASSED
    )
    nets = [float(value.net_pnl) for value in episodes]
    progress = [float(value.target_progress) for value in episodes]
    passes = [value for value in episodes if value.terminal is CombineTerminal.PASSED]
    return {
        "episode_count": len(episodes),
        "pass_count": len(passes),
        "pass_rate": len(passes) / max(len(episodes), 1),
        "pass_count_by_block": dict(sorted(pass_by_block.items())),
        "blocks_with_passes": sorted(pass_by_block),
        "net_total_usd": float(sum(nets)),
        "net_median_usd": float(statistics.median(nets)) if nets else 0.0,
        "target_progress_median": float(statistics.median(progress)) if progress else 0.0,
        "target_progress_p25": float(np.percentile(progress, 25)) if progress else 0.0,
        "mll_breach_count": sum(value.mll_breached for value in episodes),
        "mll_breach_rate": sum(value.mll_breached for value in episodes) / max(len(episodes), 1),
        "minimum_mll_buffer_usd": min((float(value.minimum_mll_buffer) for value in episodes), default=0.0),
        "consistency_compliance_rate": sum(value.consistency_ok for value in episodes) / max(len(episodes), 1),
        "all_passing_paths_consistency_compliant": all(value.consistency_ok for value in passes),
        "terminal_distribution": dict(sorted(Counter(value.terminal.value for value in episodes).items())),
    }


def _best_cell(cells: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not cells:
        raise CleanCrossAssetDailyTripwireError("primary account frontier is empty")
    return dict(
        max(
            cells,
            key=lambda row: (
                int(row["stressed"]["pass_count"]),
                int(row["normal"]["pass_count"]),
                -float(row["stressed"]["mll_breach_rate"]),
                float(row["stressed"]["target_progress_median"]),
                float(row["stressed"]["net_total_usd"]),
                -int(row["horizon_trading_days"]),
                -int(row["account_size_usd"]),
                -int(row["micro_quantity"]),
            ),
        )
    )


def _best_safe_cell(
    cells: Sequence[Mapping[str, Any]],
    *,
    headline_horizon_trading_days: int,
    maximum_mll_breach_rate: float,
) -> dict[str, Any]:
    """Return the strongest headline cell after enforcing the MLL contract.

    ``_best_cell`` remains useful for reporting the unconstrained diagnostic
    ceiling.  It must not drive the branch decision when its pass count was
    obtained by breaching the account's loss boundary too often.
    """

    safe = [
        row
        for row in cells
        if int(row["horizon_trading_days"]) == int(headline_horizon_trading_days)
        and float(row["normal"]["mll_breach_rate"]) <= maximum_mll_breach_rate
        and float(row["stressed"]["mll_breach_rate"]) <= maximum_mll_breach_rate
        and bool(row["normal"]["all_passing_paths_consistency_compliant"])
        and bool(row["stressed"]["all_passing_paths_consistency_compliant"])
    ]
    if not safe:
        raise CleanCrossAssetDailyTripwireError(
            "no headline account cell satisfies the frozen MLL/consistency contract"
        )
    return _best_cell(safe)


def _gate(primary: Mapping[str, Any], control: Mapping[str, Any], frozen: Mapping[str, Any]) -> dict[str, Any]:
    normal = dict(primary["normal"])
    stressed = dict(primary["stressed"])
    flip = dict(control["stressed"])
    checks = {
        "headline_horizon": int(primary["horizon_trading_days"]) == int(frozen["headline_horizon_trading_days"]),
        "normal_passes": int(normal["pass_count"]) >= int(frozen["minimum_normal_passes"]),
        "stressed_passes": int(stressed["pass_count"]) >= int(frozen["minimum_stressed_passes"]),
        "passing_block_diversity": len(set(normal["blocks_with_passes"]) & set(stressed["blocks_with_passes"])) >= int(frozen["minimum_passing_temporal_blocks"]),
        "controlled_stressed_mll": float(stressed["mll_breach_rate"]) <= float(frozen["maximum_stressed_mll_breach_rate"]),
        "positive_stressed_net": float(stressed["net_total_usd"]) > 0.0,
        "passing_consistency": bool(stressed["all_passing_paths_consistency_compliant"]),
        "beats_direction_flip": (
            int(stressed["pass_count"]) > int(flip["pass_count"])
            or (
                int(stressed["pass_count"]) == int(flip["pass_count"])
                and float(stressed["net_total_usd"]) > float(flip["net_total_usd"])
            )
        ),
    }
    return {"passed": all(checks.values()), "checks": checks}


def _read_primary_rows(path: Path) -> tuple[dict[str, Any], ...]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    selected = tuple(row for row in rows if row.get("candidate_id") == PRIMARY_ID)
    if len(selected) != 28:
        raise CleanCrossAssetDailyTripwireError("primary ledger event count drift")
    if len({row["event_session_id"] for row in selected}) != len(selected):
        raise CleanCrossAssetDailyTripwireError("primary ledger duplicates a session")
    return selected


def _read_mym_bars(path: Path, experiment: Mapping[str, Any]) -> pd.DataFrame:
    frame = pd.read_parquet(
        path,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "session_id"],
        filters=[("symbol", "==", "MYM")],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.loc[
        frame["timestamp"].ge(pd.Timestamp(experiment["evaluation_start_inclusive"], tz="UTC"))
        & frame["timestamp"].lt(pd.Timestamp(experiment["evaluation_end_exclusive"], tz="UTC"))
    ].sort_values("timestamp").reset_index(drop=True)
    if frame.empty or set(frame["symbol"].astype(str)) != {"MYM"}:
        raise CleanCrossAssetDailyTripwireError("MYM source bars are absent")
    if frame["timestamp"].duplicated().any():
        raise CleanCrossAssetDailyTripwireError("MYM source bars contain duplicate timestamps")
    return frame


def _calendar(frame: pd.DataFrame, experiment: Mapping[str, Any]) -> tuple[int, ...]:
    start = pd.Timestamp(experiment["evaluation_start_inclusive"])
    end = pd.Timestamp(experiment["evaluation_end_exclusive"])
    values = sorted(
        {
            int(value.replace("-", ""))
            for value in frame["session_id"].astype(str)
            if start <= pd.Timestamp(value) < end
        }
    )
    if len(values) < 20:
        raise CleanCrossAssetDailyTripwireError("MYM calendar is too short")
    return tuple(values)


def _block(session_day: int) -> str:
    month = int(str(session_day)[4:6])
    return "2024_Q1" if month <= 3 else "2024_Q2" if month <= 6 else "2024_Q3"


def _verified(project: Path, experiment: Mapping[str, Any], prefix: str) -> Path:
    path = _inside(project, str(experiment[f"{prefix}_path"]))
    if _sha256(path) != str(experiment[f"{prefix}_sha256"]):
        raise CleanCrossAssetDailyTripwireError(f"{prefix} SHA drift")
    return path


def _inside(project: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (project / path).resolve() if not path.is_absolute() else path.resolve()
    if not resolved.is_relative_to(project) or not resolved.is_file():
        raise CleanCrossAssetDailyTripwireError("source path escapes project or is absent")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CleanCrossAssetDailyTripwireError("expected a JSON object")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _one(values: Sequence[Any], label: str) -> Any:
    if len(values) != 1:
        raise CleanCrossAssetDailyTripwireError(f"expected one {label}, got {len(values)}")
    return values[0]


__all__ = [
    "BRANCH_ID",
    "CleanCrossAssetDailyTripwireError",
    "load_decision_card",
    "run_clean_cross_asset_daily_tripwire",
]
