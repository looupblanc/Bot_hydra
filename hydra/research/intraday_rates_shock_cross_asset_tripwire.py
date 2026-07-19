"""Read-only intraday Treasury-shock cross-asset economic tripwire.

The experiment is intentionally bounded and development-only.  It uses the
already acquired explicit-contract ZN minute stream as a causal state source,
executes MNQ or MGC at the next tradable minute open, and evaluates exact
5/10/20-session Combine paths for all configured account sizes.  It writes no
mission state, registry, queue, budget ledger, service file, or order route.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import RollMap, load_roll_map
from hydra.economic_evolution.schema import stable_hash
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import _apply_explicit_contract_map
from hydra.production import autonomous_exact_replay as exact
from hydra.production.fresh_confirmation_lane import non_overlapping_starts
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent, run_combine_episode
from hydra.propfirm.scaling_plan import mini_equivalent


SCHEMA = "hydra_intraday_rates_shock_cross_asset_tripwire_v1"
BRANCH_ID = "INTRADAY_RATES_SHOCK_CROSS_ASSET_REPRICING_TRIPWIRE_V1"
DEFAULT_CARD = Path(
    "config/research/intraday_rates_shock_cross_asset_repricing_tripwire_v1.json"
)
SCENARIOS = ("NORMAL", "STRESSED")
CONTROLS = ("PRIMARY", "DIRECTION_FLIP")
ROLE_ORDER = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
CANONICAL_MECHANISM_CLASS = "intraday_rates_shock_cross_asset_repricing"
CANONICAL_SUBCLASSES = (
    "duration_direction_continuation",
    "target_underreaction_catchup",
    "target_overreaction_reversal",
)
REQUIRED_ADJACENT_TOMBSTONE_REVIEWS = frozenset(
    {
        "synchronous_cross_ecology_macro_completion",
        "cross_ecology_delayed_risk_transfer",
        "v71g5_cross_clock_speed_leadership",
    }
)


class IntradayRatesShockTripwireError(RuntimeError):
    """A frozen source, causal invariant, or account contract drifted."""


def load_decision_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = _read_json(Path(path))
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    experiment = dict(card.get("smallest_decisive_falsification_experiment") or {})
    if not claimed or stable_hash(core) != claimed:
        raise IntradayRatesShockTripwireError("decision-card hash drift")
    if (
        card.get("selected_branch") != BRANCH_ID
        or experiment.get("status_ceiling") != "TIER_E_EXECUTABLE_DIAGNOSTIC"
        or bool(experiment.get("promotion_allowed"))
        or bool(experiment.get("q4_access_allowed"))
        or bool(experiment.get("data_purchase_allowed"))
        or bool(experiment.get("broker_allowed"))
        or bool(experiment.get("orders_allowed"))
    ):
        raise IntradayRatesShockTripwireError("decision-card semantic drift")
    return card


def run_intraday_rates_shock_tripwire(
    root: str | Path,
    *,
    decision_card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Execute the frozen development tripwire and return a self-hashed result."""

    project = Path(root).resolve()
    card_path = _inside(project, decision_card_path)
    card = load_decision_card(card_path)
    experiment = dict(card["smallest_decisive_falsification_experiment"])
    resurrection_audit = _assert_no_cemetery_resurrection(project, card)

    source_path = _verified(project, experiment, "source")
    roll_map_path = _verified(project, experiment, "roll_map")
    rules_path = _verified(project, experiment, "rule_snapshot")
    target_paths = tuple(
        _verified_row(project, row, label="target")
        for row in experiment["target_paths"]
    )

    source = _load_source(source_path, experiment)
    targets, raw_targets, mapping_proof, roll_map = _load_targets(
        target_paths, roll_map_path, experiment
    )
    source_features = _causal_features(
        source,
        group_columns=("roll_segment_id",),
        lookbacks=tuple(int(value) for value in experiment["source_lookback_minutes"]),
        volatility_window=int(experiment["prior_volatility_window_minutes"]),
    )
    target_features = _causal_features(
        targets,
        group_columns=("symbol", "active_contract"),
        lookbacks=tuple(int(value) for value in experiment["source_lookback_minutes"]),
        volatility_window=int(experiment["prior_volatility_window_minutes"]),
    )
    proposals, events_by_rule = build_rule_events(
        source_features,
        target_features,
        experiment=experiment,
    )
    resurrection_audit = _assert_no_structural_resurrection(
        card, proposals, resurrection_audit
    )
    selected = _select_on_discovery(
        proposals,
        events_by_rule,
        experiment=experiment,
    )

    rules, rule_receipt = exact._load_rule_snapshot(rules_path)
    calendars, coverage_audit = _role_calendars(
        raw_targets,
        target_features,
        source,
        roll_map=roll_map,
        experiment=experiment,
    )
    cells = _account_cells(
        selected,
        events_by_rule,
        calendars=calendars,
        coverage_audit=coverage_audit,
        rules=rules,
        experiment=experiment,
    )
    decisions = tuple(
        _candidate_decision(
            proposal,
            events_by_rule[proposal["candidate_id"]],
            cells,
            frozen_gate=card["frozen_gate"],
        )
        for proposal in selected
    )
    positive = tuple(row for row in decisions if row["gate"]["passed"])
    safety_counters = _zero_safety_counters()
    has_unplanned_coverage_gap = any(
        bool(values["data_censored_day_count"])
        for roles in coverage_audit.values()
        for values in roles.values()
    )
    if positive:
        status = "INTRADAY_RATES_SHOCK_TRIPWIRE_POSITIVE_DEVELOPMENT_ONLY"
        next_action = "FREEZE_POSITIVE_TIER_E_CELL_AND_REQUIRE_ONE_FRESH_CONFIRMATION"
    elif has_unplanned_coverage_gap:
        status = "COVERAGE_INCONCLUSIVE_NO_SAFE_PASS_DIAGNOSTIC"
        next_action = "PRESERVE_NO_SAFE_PASS_DIAGNOSTIC_AND_FIX_ONLY_COVERAGE_INPUT"
    else:
        status = "INTRADAY_RATES_SHOCK_CROSS_ASSET_REPRICING_FALSIFIED"
        next_action = str(card["next_materially_distinct_alternative"])

    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "evidence_role": "VIEWED_DEVELOPMENT_TRIPWIRE_ONLY",
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "source_bindings": {
            "decision_card_path": str(card_path.relative_to(project)),
            "decision_card_hash": card["card_hash"],
            "source_sha256": experiment["source_sha256"],
            "target_sha256s": [row["sha256"] for row in experiment["target_paths"]],
            "roll_map_sha256": experiment["roll_map_sha256"],
            "rule_snapshot": rule_receipt,
        },
        "integrity": {
            "available_at_not_after_decision": True,
            "entry_at_next_tradable_open": True,
            "outcomes_not_used_for_discovery_features": True,
            "explicit_target_contract_map": True,
            "roll_unsafe_rows_excluded": mapping_proof["unsafe_roll_rows_excluded"],
            "unmapped_rows_excluded": mapping_proof["unmapped_contract_rows_excluded"],
            "q4_rows": 0,
            "data_purchase_usd": 0.0,
            **safety_counters,
        },
        "resurrection_audit": resurrection_audit,
        "proposal_count": len(proposals),
        "selected_rule_count": len(selected),
        "selected_rules": list(selected),
        "event_counts": {
            candidate_id: len(rows) for candidate_id, rows in sorted(events_by_rule.items())
        },
        "role_calendar_counts": {
            role: {market: len(days) for market, days in sorted(markets.items())}
            for role, markets in calendars.items()
        },
        "coverage_audit": coverage_audit,
        "account_cell_count": len(cells),
        "account_episode_count": int(
            sum(
                int(cell["normal"]["episode_count"])
                + int(cell["stressed"]["episode_count"])
                for cell in cells
            )
        ),
        "candidate_decisions": list(decisions),
        "positive_tier_e_diagnostics": [row["candidate_id"] for row in positive],
        "cells": cells,
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        **safety_counters,
        "next_action": next_action,
    }
    return {**core, "result_hash": stable_hash(core)}


def audit_intraday_rates_shock_coverage(
    root: str | Path,
    *,
    decision_card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Audit calendars and data gaps without generating signals or episodes."""

    project = Path(root).resolve()
    card_path = _inside(project, decision_card_path)
    card = load_decision_card(card_path)
    experiment = dict(card["smallest_decisive_falsification_experiment"])
    source_path = _verified(project, experiment, "source")
    roll_map_path = _verified(project, experiment, "roll_map")
    target_paths = tuple(
        _verified_row(project, row, label="target")
        for row in experiment["target_paths"]
    )
    source = _load_source(source_path, experiment)
    mapped, raw, mapping_proof, roll_map = _load_targets(
        target_paths, roll_map_path, experiment
    )
    calendars, coverage = _role_calendars(
        raw,
        mapped,
        source,
        roll_map=roll_map,
        experiment=experiment,
    )
    censored_market_days = sum(
        int(values["data_censored_day_count"])
        for roles in coverage.values()
        for values in roles.values()
    )
    planned_market_days = sum(
        int(values["planned_roll_zero_trade_count"])
        for roles in coverage.values()
        for values in roles.values()
    )
    core = {
        "schema": "hydra_intraday_rates_shock_coverage_audit_v1",
        "branch_id": BRANCH_ID,
        "decision_card_hash": card["card_hash"],
        "calendar_policy": "RAW_TARGET_TRUE_SESSIONS_ROLL_UNSAFE_ZERO_TRADE",
        "coverage_policy": "UNPLANNED_SOURCE_OR_TARGET_SESSION_GAP_DATA_CENSORED",
        "mapping_proof": mapping_proof,
        "calendar_counts": {
            role: {market: len(days) for market, days in markets.items()}
            for role, markets in calendars.items()
        },
        "coverage": coverage,
        "planned_roll_zero_trade_market_day_count": planned_market_days,
        "data_censored_market_day_count": censored_market_days,
        "has_unplanned_coverage_gap": bool(censored_market_days),
        "economic_replay_performed": False,
        "account_episode_count_delta": 0,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        **_zero_safety_counters(),
    }
    return {**core, "coverage_audit_hash": stable_hash(core)}


def build_rule_events(
    source: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    experiment: Mapping[str, Any],
) -> tuple[tuple[dict[str, Any], ...], dict[str, tuple[dict[str, Any], ...]]]:
    """Build every frozen rule from causal features, with one position at a time."""

    proposals: list[dict[str, Any]] = []
    events_by_rule: dict[str, tuple[dict[str, Any], ...]] = {}
    lookbacks = tuple(int(value) for value in experiment["source_lookback_minutes"])
    mechanisms = tuple(str(value) for value in experiment["mechanisms"])
    holdings = tuple(int(value) for value in experiment["holding_minutes"])
    threshold = float(experiment["source_shock_z_threshold"])
    under = float(experiment["underreaction_ratio_maximum"])
    over = float(experiment["overreaction_ratio_minimum"])
    clock_start = _clock_minutes(experiment["eligible_chicago_clock"][0])
    clock_end = _clock_minutes(experiment["eligible_chicago_clock"][1])

    source_columns = [
        "timestamp",
        "contract",
        "roll_segment_id",
        *[f"return_{value}" for value in lookbacks],
        *[f"prior_sigma_{value}" for value in lookbacks],
    ]
    for market in tuple(str(value) for value in experiment["execution_markets"]):
        target = targets.loc[targets["symbol"].astype(str).eq(market)].copy()
        if target.empty:
            raise IntradayRatesShockTripwireError(f"missing target market {market}")
        target = target.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
        target["target_index"] = np.arange(len(target), dtype=np.int64)
        merged = target.merge(
            source[source_columns],
            on="timestamp",
            how="inner",
            validate="one_to_one",
            suffixes=("_target", "_source"),
        )
        for lookback in lookbacks:
            source_sigma = merged[f"prior_sigma_{lookback}_source"].replace(0.0, np.nan)
            target_sigma = merged[f"prior_sigma_{lookback}_target"].replace(0.0, np.nan)
            source_z = merged[f"return_{lookback}_source"] / source_sigma
            target_z = merged[f"return_{lookback}_target"] / target_sigma
            base = merged.loc[
                source_z.abs().ge(threshold)
                & source_z.notna()
                & target_z.notna()
            ].copy()
            base["source_z"] = source_z.loc[base.index]
            base["target_z"] = target_z.loc[base.index]
            base["source_side"] = np.sign(base["source_z"]).astype(int)
            base["aligned_target_z"] = base["source_side"] * base["target_z"]
            decision_time = pd.to_datetime(base["timestamp"], utc=True) + pd.Timedelta(minutes=1)
            local = decision_time.dt.tz_convert("America/Chicago")
            local_minutes = local.dt.hour * 60 + local.dt.minute
            base = base.loc[local_minutes.between(clock_start, clock_end)].copy()

            for mechanism in mechanisms:
                if mechanism == "DURATION_DIRECTION_CONTINUATION":
                    triggers = base.copy()
                    triggers["side"] = triggers["source_side"]
                elif mechanism == "TARGET_UNDERREACTION_CATCHUP":
                    triggers = base.loc[
                        base["aligned_target_z"] <= under * base["source_z"].abs()
                    ].copy()
                    triggers["side"] = triggers["source_side"]
                elif mechanism == "TARGET_OVERREACTION_REVERSAL":
                    triggers = base.loc[
                        base["aligned_target_z"] >= over * base["source_z"].abs()
                    ].copy()
                    triggers["side"] = -triggers["source_side"]
                else:
                    raise IntradayRatesShockTripwireError("unknown frozen mechanism")
                for holding in holdings:
                    short = {
                        "DURATION_DIRECTION_CONTINUATION": "continuation",
                        "TARGET_UNDERREACTION_CATCHUP": "catchup",
                        "TARGET_OVERREACTION_REVERSAL": "reversal",
                    }[mechanism]
                    candidate_id = (
                        f"rates_shock_{market}_{short}_lb{lookback}_h{holding}_v1"
                    )
                    proposal = {
                        "candidate_id": candidate_id,
                        "mechanism": mechanism,
                        "source_market": "ZN",
                        "execution_market": market,
                        "lookback_minutes": lookback,
                        "holding_minutes": holding,
                        "structural_fingerprint": stable_hash(
                            {
                                "branch": BRANCH_ID,
                                "market": market,
                                "mechanism": mechanism,
                                "lookback": lookback,
                                "holding": holding,
                                "threshold": threshold,
                                "underreaction": under,
                                "overreaction": over,
                            }
                        ),
                        "canonical_structural_fingerprint": stable_hash(
                            {
                                "fingerprint_schema": (
                                    "hydra_canonical_cross_asset_structure_v1"
                                ),
                                "mechanism_class": CANONICAL_MECHANISM_CLASS,
                                "source_market": "ZN",
                                "execution_market": market,
                                "mechanism": mechanism.lower(),
                                "lookback_minutes": lookback,
                                "holding_minutes": holding,
                                "source_shock_z_threshold": threshold,
                                "underreaction_ratio_maximum": under,
                                "overreaction_ratio_minimum": over,
                                "fill_policy": "NEXT_TRADABLE_MINUTE_OPEN",
                            }
                        ),
                    }
                    proposals.append(proposal)
                    events_by_rule[candidate_id] = _materialize_events(
                        triggers,
                        target,
                        proposal=proposal,
                        experiment=experiment,
                    )
    if len(proposals) != len({row["structural_fingerprint"] for row in proposals}):
        raise IntradayRatesShockTripwireError("structural proposal collision")
    if len(proposals) != len(
        {row["canonical_structural_fingerprint"] for row in proposals}
    ):
        raise IntradayRatesShockTripwireError("canonical structural proposal collision")
    return tuple(proposals), events_by_rule


def _materialize_events(
    triggers: pd.DataFrame,
    target: pd.DataFrame,
    *,
    proposal: Mapping[str, Any],
    experiment: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    holding = int(proposal["holding_minutes"])
    market = str(proposal["execution_market"])
    point_value = float(instrument_spec(market).point_value)
    normal_cost = float(experiment["normal_all_in_cost_per_micro_usd"][market])
    stressed_cost = float(experiment["stressed_all_in_cost_per_micro_usd"][market])
    last_exit_ns = -1
    events: list[dict[str, Any]] = []
    for row in triggers.sort_values("timestamp", kind="mergesort").itertuples(index=False):
        entry_index = int(row.target_index) + 1
        exit_index = entry_index + holding - 1
        if entry_index >= len(target) or exit_index >= len(target):
            continue
        path = target.iloc[entry_index : exit_index + 1]
        entry_time = pd.Timestamp(path.iloc[0]["timestamp"])
        decision_time = pd.Timestamp(row.timestamp) + pd.Timedelta(minutes=1)
        exit_available = pd.Timestamp(path.iloc[-1]["timestamp"]) + pd.Timedelta(minutes=1)
        if int(decision_time.value) <= last_exit_ns:
            continue
        if (
            entry_time != decision_time
            or len(path) != holding
            or not path["timestamp"].diff().dropna().eq(pd.Timedelta(minutes=1)).all()
            or path["active_contract"].nunique() != 1
        ):
            continue
        local_exit = exit_available.tz_convert("America/Chicago")
        if (local_exit.hour, local_exit.minute) > (15, 10):
            continue
        side = int(row.side)
        if side not in {-1, 1}:
            raise IntradayRatesShockTripwireError("invalid causal event side")
        entry = float(path.iloc[0]["open"])
        exit_price = float(path.iloc[-1]["close"])
        gross = side * (exit_price - entry) * point_value
        if side > 0:
            favorable = (float(path["high"].max()) - entry) * point_value
            adverse = (float(path["low"].min()) - entry) * point_value
        else:
            favorable = (entry - float(path["low"].min())) * point_value
            adverse = (entry - float(path["high"].max())) * point_value
        local_entry = entry_time.tz_convert("America/Chicago")
        session_day = int(local_entry.strftime("%Y%m%d"))
        event_id = f"{proposal['candidate_id']}:{entry_time.isoformat()}"
        events.append(
            {
                "event_id": event_id,
                "candidate_id": proposal["candidate_id"],
                "session_day": session_day,
                "decision_ns": int(decision_time.value),
                "entry_ns": int(entry_time.value),
                "exit_ns": int(exit_available.value),
                "entry_price": entry,
                "exit_price": exit_price,
                "side": side,
                "gross_one_micro": gross,
                "favorable_one_micro": favorable,
                "adverse_one_micro": adverse,
                "normal_net_one_micro": gross - normal_cost,
                "stressed_net_one_micro": gross - stressed_cost,
                "normal_cost_one_micro": normal_cost,
                "stressed_cost_one_micro": stressed_cost,
                "source_contract": str(row.contract),
                "target_contract": str(path.iloc[0]["active_contract"]),
                "source_z": float(row.source_z),
                "target_z": float(row.target_z),
                "available_at_ns": int(decision_time.value),
                "feature_hash": stable_hash(
                    {
                        "source_z": round(float(row.source_z), 12),
                        "target_z": round(float(row.target_z), 12),
                        "source_contract": str(row.contract),
                        "target_contract": str(path.iloc[0]["active_contract"]),
                        "decision_ns": int(decision_time.value),
                    }
                ),
                "session_compliant": True,
            }
        )
        last_exit_ns = int(exit_available.value)
    return tuple(events)


def _causal_features(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str],
    lookbacks: Sequence[int],
    volatility_window: int,
) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for _key, group in frame.groupby(list(group_columns), sort=True, dropna=False):
        ordered = group.sort_values("timestamp", kind="mergesort").reset_index(drop=True).copy()
        gap = ordered["timestamp"].diff().ne(pd.Timedelta(minutes=1))
        ordered["causal_segment"] = gap.cumsum().astype(int)
        segment_pieces: list[pd.DataFrame] = []
        for _segment, segment in ordered.groupby("causal_segment", sort=True):
            segment = segment.copy()
            close = segment["close"].astype(float)
            one = close.pct_change(fill_method=None)
            prior_one_sigma = one.rolling(
                volatility_window,
                min_periods=max(volatility_window // 2, 20),
            ).std().shift(1)
            for lookback in lookbacks:
                value = int(lookback)
                segment[f"return_{value}"] = close.pct_change(value, fill_method=None)
                segment[f"prior_sigma_{value}"] = prior_one_sigma * math.sqrt(value)
            segment_pieces.append(segment)
        pieces.append(pd.concat(segment_pieces, ignore_index=True))
    output = pd.concat(pieces, ignore_index=True).sort_values(
        [*group_columns, "timestamp"], kind="mergesort"
    )
    return output.reset_index(drop=True)


def _select_on_discovery(
    proposals: Sequence[Mapping[str, Any]],
    events_by_rule: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    experiment: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    start, end = experiment["temporal_roles"]["DISCOVERY"]
    start_day, end_day = _day_int(start), _day_int(end)
    scored: list[dict[str, Any]] = []
    for proposal in proposals:
        events = [
            row
            for row in events_by_rule[str(proposal["candidate_id"])]
            if start_day <= int(row["session_day"]) < end_day
        ]
        stressed = float(sum(float(row["stressed_net_one_micro"]) for row in events))
        normal = float(sum(float(row["normal_net_one_micro"]) for row in events))
        wins = sum(float(row["stressed_net_one_micro"]) > 0.0 for row in events)
        scored.append(
            {
                **dict(proposal),
                "discovery_event_count": len(events),
                "discovery_normal_net_one_micro_usd": normal,
                "discovery_stressed_net_one_micro_usd": stressed,
                "discovery_stressed_win_rate": wins / max(len(events), 1),
            }
        )
    winners: list[dict[str, Any]] = []
    for (_market, _mechanism), group in _group_rows(
        scored, ("execution_market", "mechanism")
    ):
        winners.append(
            max(
                group,
                key=lambda row: (
                    float(row["discovery_stressed_net_one_micro_usd"]),
                    float(row["discovery_normal_net_one_micro_usd"]),
                    int(row["discovery_event_count"]),
                    -int(row["holding_minutes"]),
                    -int(row["lookback_minutes"]),
                ),
            )
        )
    winners.sort(
        key=lambda row: (
            -float(row["discovery_stressed_net_one_micro_usd"]),
            str(row["candidate_id"]),
        )
    )
    return tuple(winners[: int(experiment["maximum_discovery_selected_rules"])])


def _account_cells(
    selected: Sequence[Mapping[str, Any]],
    events_by_rule: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    calendars: Mapping[str, Mapping[str, Sequence[int]]],
    coverage_audit: Mapping[str, Mapping[str, Mapping[str, Any]]],
    rules: Mapping[str, Mapping[str, Any]],
    experiment: Mapping[str, Any],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    horizons = tuple(int(value) for value in experiment["horizons_trading_days"])
    for proposal in selected:
        candidate_id = str(proposal["candidate_id"])
        market = str(proposal["execution_market"])
        base_events = events_by_rule[candidate_id]
        for account_label in experiment["account_sizes"]:
            rule = dict(rules[str(account_label)])
            config = exact._account_config(rule)
            legal_cap = int(rule["maximum_micro_contracts"])
            # The caller does not inject a hidden cap.  MGC's official cap is
            # deterministic in the verified snapshot and bounded explicitly.
            if market == "MGC":
                legal_cap = min(legal_cap, {"50K": 30, "100K": 60, "150K": 90}[str(account_label)])
            quantities = tuple(
                int(value)
                for value in experiment["quantity_frontiers"][market]
                if 0 < int(value) <= legal_cap
            )
            for quantity in quantities:
                for control in CONTROLS:
                    scenario_events = {
                        scenario: tuple(
                            _trade_path_event(
                                row,
                                market=market,
                                quantity=quantity,
                                scenario=scenario,
                                direction_flip=control == "DIRECTION_FLIP",
                            )
                            for row in base_events
                        )
                        for scenario in SCENARIOS
                    }
                    for role in ROLE_ORDER:
                        calendar = tuple(calendars[role][market])
                        starts = non_overlapping_starts(calendar, horizons)
                        for horizon in horizons:
                            full_starts, censored_starts = _split_starts_by_coverage(
                                starts[horizon],
                                calendar,
                                horizon=horizon,
                                censored_days=coverage_audit[role][market][
                                    "data_censored_days"
                                ],
                            )
                            summaries: dict[str, Any] = {}
                            for scenario in SCENARIOS:
                                episodes = [
                                    (
                                        run_combine_episode(
                                            scenario_events[scenario],
                                            calendar,
                                            start_day=int(start_day),
                                            maximum_duration_days=horizon,
                                            config=config,
                                            maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
                                        ),
                                        _block(int(start_day)),
                                    )
                                    for start_day, _label in full_starts
                                ]
                                summaries[scenario] = _summarize(
                                    episodes,
                                    data_censored_count=len(censored_starts),
                                )
                            output.append(
                                {
                                    "candidate_id": candidate_id,
                                    "execution_market": market,
                                    "control": control,
                                    "temporal_role": role,
                                    "account_label": str(account_label),
                                    "account_size_usd": int(rule["account_size_usd"]),
                                    "quantity": quantity,
                                    "mini_equivalent": float(mini_equivalent(market, quantity)),
                                    "horizon_trading_days": horizon,
                                    "total_preregistered_start_count": len(starts[horizon]),
                                    "full_coverage_start_count": len(full_starts),
                                    "data_censored_start_count": len(censored_starts),
                                    "planned_roll_zero_trade_session_count": len(
                                        coverage_audit[role][market][
                                            "planned_roll_zero_trade_days"
                                        ]
                                    ),
                                    "normal": summaries["NORMAL"],
                                    "stressed": summaries["STRESSED"],
                                }
                            )
    return output


def _candidate_decision(
    proposal: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    cells: Sequence[Mapping[str, Any]],
    *,
    frozen_gate: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = str(proposal["candidate_id"])
    own = [row for row in cells if row["candidate_id"] == candidate_id]
    discovery = [
        row
        for row in own
        if row["control"] == "PRIMARY" and row["temporal_role"] == "DISCOVERY"
        and float(row["normal"]["mll_breach_rate"]) <= float(frozen_gate["maximum_stressed_mll_breach_rate"])
        and float(row["stressed"]["mll_breach_rate"]) <= float(frozen_gate["maximum_stressed_mll_breach_rate"])
    ]
    if not discovery:
        raise IntradayRatesShockTripwireError("no safe discovery account cell")
    frozen = _best_cell(discovery)
    matching = {
        role: _one(
            [
                row
                for row in own
                if row["control"] == "PRIMARY"
                and row["temporal_role"] == role
                and _same_cell(row, frozen)
            ],
            f"{candidate_id} {role} frozen cell",
        )
        for role in ROLE_ORDER
    }
    flip = _one(
        [
            row
            for row in own
            if row["control"] == "DIRECTION_FLIP"
            and row["temporal_role"] == "FINAL_DEVELOPMENT"
            and _same_cell(row, frozen)
        ],
        f"{candidate_id} final direction flip",
    )
    final = matching["FINAL_DEVELOPMENT"]
    validation = matching["VALIDATION"]
    discovery_cell = matching["DISCOVERY"]
    final_normal = dict(final["normal"])
    final_stressed = dict(final["stressed"])
    flip_stressed = dict(flip["stressed"])
    common_passing_blocks = set(final_normal["blocks_with_passes"]) & set(
        final_stressed["blocks_with_passes"]
    )
    checks = {
        "positive_discovery_stressed_net": float(discovery_cell["stressed"]["net_total_usd"])
        > float(frozen_gate["minimum_discovery_stressed_net_usd"]),
        "positive_validation_stressed_net": float(validation["stressed"]["net_total_usd"])
        > float(frozen_gate["minimum_validation_stressed_net_usd"]),
        "positive_final_stressed_net": float(final_stressed["net_total_usd"])
        > float(frozen_gate["minimum_final_stressed_net_usd"]),
        "final_normal_passes": int(final_normal["pass_count"])
        >= int(frozen_gate["minimum_final_normal_passes"]),
        "final_stressed_passes": int(final_stressed["pass_count"])
        >= int(frozen_gate["minimum_final_stressed_passes"]),
        "passing_block_diversity": len(common_passing_blocks)
        >= int(frozen_gate["minimum_passing_temporal_blocks"]),
        "controlled_stressed_mll": float(final_stressed["mll_breach_rate"])
        <= float(frozen_gate["maximum_stressed_mll_breach_rate"]),
        "passing_consistency": bool(final_stressed["all_passing_paths_consistency_compliant"]),
        "beats_direction_flip": (
            int(final_stressed["pass_count"]) > int(flip_stressed["pass_count"])
            or (
                int(final_stressed["pass_count"]) == int(flip_stressed["pass_count"])
                and float(final_stressed["net_total_usd"])
                > float(flip_stressed["net_total_usd"])
            )
        ),
    }
    role_event_metrics = {
        role: _event_metrics(events, role, proposal, cells, matching[role])
        for role in ROLE_ORDER
    }
    return {
        "candidate_id": candidate_id,
        "execution_market": proposal["execution_market"],
        "mechanism": proposal["mechanism"],
        "lookback_minutes": proposal["lookback_minutes"],
        "holding_minutes": proposal["holding_minutes"],
        "event_count": len(events),
        "frozen_account_cell": {
            key: frozen[key]
            for key in (
                "account_label",
                "account_size_usd",
                "quantity",
                "mini_equivalent",
                "horizon_trading_days",
            )
        },
        "role_event_metrics": role_event_metrics,
        "discovery_account": discovery_cell,
        "validation_account": validation,
        "final_development_account": final,
        "matched_final_direction_flip": flip,
        "gate": {"passed": all(checks.values()), "checks": checks},
        "evidence_tier": "E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
    }


def _event_metrics(
    events: Sequence[Mapping[str, Any]],
    role: str,
    _proposal: Mapping[str, Any],
    _cells: Sequence[Mapping[str, Any]],
    frozen_cell: Mapping[str, Any],
) -> dict[str, Any]:
    # Role bounds are recovered from the account cell's event calendar through
    # its episode evidence; the direct event summary is therefore global only
    # when the caller lacks role bounds.  Account evidence remains authoritative.
    return {
        "account_normal_net_total_usd": float(frozen_cell["normal"]["net_total_usd"]),
        "account_stressed_net_total_usd": float(frozen_cell["stressed"]["net_total_usd"]),
        "account_normal_passes": int(frozen_cell["normal"]["pass_count"]),
        "account_stressed_passes": int(frozen_cell["stressed"]["pass_count"]),
        "account_episode_count": int(frozen_cell["normal"]["episode_count"]),
        "source_event_count_all_roles": len(events),
        "role": role,
    }


def _trade_path_event(
    row: Mapping[str, Any],
    *,
    market: str,
    quantity: int,
    scenario: str,
    direction_flip: bool,
) -> TradePathEvent:
    if scenario not in SCENARIOS:
        raise IntradayRatesShockTripwireError("unknown cost scenario")
    cost = float(
        row["normal_cost_one_micro"]
        if scenario == "NORMAL"
        else row["stressed_cost_one_micro"]
    )
    if direction_flip:
        gross = -float(row["gross_one_micro"])
        favorable = -float(row["adverse_one_micro"])
        adverse = -float(row["favorable_one_micro"])
    else:
        gross = float(row["gross_one_micro"])
        favorable = float(row["favorable_one_micro"])
        adverse = float(row["adverse_one_micro"])
    value = int(quantity)
    return TradePathEvent(
        event_id=f"{row['event_id']}:{'FLIP' if direction_flip else 'PRIMARY'}:{scenario}:q{value}",
        decision_ns=int(row["decision_ns"]),
        exit_ns=int(row["exit_ns"]),
        session_day=int(row["session_day"]),
        net_pnl=(gross - cost) * value,
        gross_pnl=gross * value,
        worst_unrealized_pnl=(adverse - cost / 2.0) * value,
        best_unrealized_pnl=max((favorable - cost / 2.0) * value, 0.0),
        quantity=value,
        mini_equivalent=float(mini_equivalent(market, value)),
        regime="INTRADAY_RATES_SHOCK_CROSS_ASSET_REPRICING",
        session_compliant=bool(row["session_compliant"]),
        contract_limit_compliant=True,
    )


def _summarize(
    values: Sequence[tuple[Any, str]], *, data_censored_count: int = 0
) -> dict[str, Any]:
    episodes = [value for value, _block_id in values]
    passes = [value for value in episodes if value.terminal is CombineTerminal.PASSED]
    nets = [float(value.net_pnl) for value in episodes]
    progress = [float(value.target_progress) for value in episodes]
    pass_by_block = Counter(
        block_id for value, block_id in values if value.terminal is CombineTerminal.PASSED
    )
    return {
        "episode_count": len(episodes),
        "full_coverage_episode_count": len(episodes),
        "data_censored_count": int(data_censored_count),
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
        "minimum_mll_buffer_usd": min(
            (float(value.minimum_mll_buffer) for value in episodes), default=0.0
        ),
        "consistency_compliance_rate": sum(value.consistency_ok for value in episodes)
        / max(len(episodes), 1),
        "all_passing_paths_consistency_compliant": bool(passes)
        and all(value.consistency_ok for value in passes),
        "terminal_distribution": dict(
            sorted(Counter(value.terminal.value for value in episodes).items())
        ),
    }


def _best_cell(cells: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return dict(
        max(
            cells,
            key=lambda row: (
                int(row["stressed"]["pass_count"]),
                int(row["normal"]["pass_count"]),
                float(row["stressed"]["target_progress_p25"]),
                float(row["stressed"]["target_progress_median"]),
                float(row["stressed"]["net_total_usd"]),
                -float(row["stressed"]["mll_breach_rate"]),
                -int(row["horizon_trading_days"]),
                -int(row["account_size_usd"]),
                -int(row["quantity"]),
            ),
        )
    )


def _same_cell(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return all(
        left[key] == right[key]
        for key in ("account_label", "quantity", "horizon_trading_days")
    )


def _load_source(path: Path, experiment: Mapping[str, Any]) -> pd.DataFrame:
    columns = [
        "timestamp",
        "symbol",
        "contract",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "session_id",
        "roll_segment_id",
    ]
    frame = pd.read_parquet(path, columns=columns)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.loc[
        frame["symbol"].astype(str).eq(str(experiment["source_market"]))
        & frame["timestamp"].ge(pd.Timestamp(experiment["data_start_inclusive"], tz="UTC"))
        & frame["timestamp"].lt(pd.Timestamp(experiment["data_end_exclusive"], tz="UTC"))
    ].sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    if frame.empty or frame["timestamp"].duplicated().any():
        raise IntradayRatesShockTripwireError("source is empty or duplicates timestamps")
    return frame


def _load_targets(
    paths: Sequence[Path],
    roll_map_path: Path,
    experiment: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int], RollMap]:
    markets = tuple(str(value) for value in experiment["execution_markets"])
    columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume", "session_id"]
    pieces: list[pd.DataFrame] = []
    for path in paths:
        frame = pd.read_parquet(path, columns=columns, filters=[("symbol", "in", list(markets))])
        if not frame.empty:
            pieces.append(frame)
    raw = pd.concat(pieces, ignore_index=True)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
    raw = raw.loc[
        raw["symbol"].astype(str).isin(markets)
        & raw["timestamp"].ge(pd.Timestamp(experiment["data_start_inclusive"], tz="UTC"))
        & raw["timestamp"].lt(pd.Timestamp(experiment["data_end_exclusive"], tz="UTC"))
    ].drop_duplicates(["symbol", "timestamp"], keep="first")
    raw = raw.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)
    roll_map = load_roll_map(roll_map_path)
    mapped, proof = _apply_explicit_contract_map(raw, roll_map, required_map_type=MAP_TYPE)
    if set(markets) - set(mapped["symbol"].astype(str)):
        raise IntradayRatesShockTripwireError("target mapping removed a required market")
    if mapped.duplicated(["symbol", "timestamp"]).any():
        raise IntradayRatesShockTripwireError("mapped targets duplicate symbol-time")
    return mapped, raw, proof, roll_map


def _role_calendars(
    raw_targets: pd.DataFrame,
    mapped_targets: pd.DataFrame,
    source: pd.DataFrame,
    *,
    roll_map: RollMap,
    experiment: Mapping[str, Any],
) -> tuple[
    dict[str, dict[str, tuple[int, ...]]],
    dict[str, dict[str, dict[str, Any]]],
]:
    """Build real-session calendars independently from signal eligibility.

    Target roll guards deliberately remove executable rows.  They must not
    remove elapsed Combine sessions: a roll-unsafe session remains in the
    calendar and contributes zero trades.  Unexpected source or target holes
    are instead marked ``DATA_CENSORED`` and excluded from headline episode
    denominators.
    """

    clock_start = _clock_minutes(experiment["eligible_chicago_clock"][0])
    clock_end = _clock_minutes(experiment["eligible_chicago_clock"][1])
    maximum_holding = max(int(value) for value in experiment["holding_minutes"])
    raw_view = _session_minute_view(raw_targets)
    mapped_view = _session_minute_view(mapped_targets)
    source_view = _session_minute_view(source.assign(symbol=experiment["source_market"]))
    target_true_days = {
        str(market): tuple(
            sorted(
                set(
                    int(value)
                    for value in raw_view.loc[
                        raw_view["symbol"].astype(str).eq(str(market))
                        & raw_view["local_minute"].between(clock_start, clock_end),
                        "session_day",
                    ]
                )
            )
        )
        for market in experiment["execution_markets"]
    }
    if any(not days for days in target_true_days.values()):
        raise IntradayRatesShockTripwireError("target true-session calendar is empty")

    raw_times = _timestamp_inventory(
        raw_view.loc[
            raw_view["local_minute"].between(
                clock_start - 1, clock_end + maximum_holding - 1
            )
        ]
    )
    mapped_times = _timestamp_inventory(
        mapped_view.loc[
            mapped_view["local_minute"].between(
                clock_start - 1, clock_end + maximum_holding - 1
            )
        ]
    )
    source_times = _timestamp_inventory(
        source_view.loc[
            source_view["local_minute"].between(clock_start - 1, clock_end - 1)
        ]
    )
    planned_roll_days = {
        str(market): _planned_roll_session_days(roll_map, str(market))
        for market in experiment["execution_markets"]
    }
    output: dict[str, dict[str, tuple[int, ...]]] = {}
    coverage: dict[str, dict[str, dict[str, Any]]] = {}
    for role in ROLE_ORDER:
        start, end = experiment["temporal_roles"][role]
        lower, upper = _day_int(start), _day_int(end)
        output[role] = {}
        coverage[role] = {}
        for market in experiment["execution_markets"]:
            market = str(market)
            role_days = tuple(
                day for day in target_true_days[market] if lower <= day < upper
            )
            planned = tuple(
                day for day in role_days if day in planned_roll_days[market]
            )
            censored_reasons: dict[str, list[str]] = {}
            for day in role_days:
                if day in planned_roll_days[market]:
                    continue
                reasons: list[str] = []
                raw_market = raw_times.get((market, day), set())
                mapped_market = mapped_times.get((market, day), set())
                source_market = source_times.get(
                    (str(experiment["source_market"]), day), set()
                )
                # Databento OHLCV emits no record for a zero-trade minute.  A
                # sparse minute sequence is therefore observable abstention,
                # not proof of a data outage.  A complete missing session is
                # unobservable and must be censored.
                if not raw_market:
                    reasons.append("TARGET_RAW_SESSION_MISSING")
                if not mapped_market:
                    reasons.append("TARGET_MAPPED_SESSION_MISSING")
                if not source_market:
                    reasons.append("SOURCE_SESSION_MISSING")
                if reasons:
                    censored_reasons[str(day)] = reasons
            censored = tuple(int(value) for value in censored_reasons)
            if len(role_days) < max(
                int(value) for value in experiment["horizons_trading_days"]
            ):
                raise IntradayRatesShockTripwireError(f"insufficient {role} calendar for {market}")
            output[role][market] = role_days
            coverage[role][market] = {
                "status": "DATA_CENSORED" if censored else "FULL_COVERAGE",
                "true_session_count": len(role_days),
                "true_session_days": list(role_days),
                "planned_roll_zero_trade_count": len(planned),
                "planned_roll_zero_trade_days": list(planned),
                "data_censored_day_count": len(censored),
                "data_censored_days": list(censored),
                "data_censored_reasons_by_day": censored_reasons,
                "zero_trade_minute_policy": (
                    "ABSENT_OHLCV_MINUTE_IS_OBSERVABLE_NO_TRADE_NOT_DATA_GAP"
                ),
            }
    return output, coverage


def _session_minute_view(frame: pd.DataFrame) -> pd.DataFrame:
    values = frame.loc[:, ["timestamp", "symbol"]].copy()
    values["timestamp"] = pd.to_datetime(values["timestamp"], utc=True)
    local = values["timestamp"].dt.tz_convert("America/Chicago")
    values["session_day"] = local.dt.strftime("%Y%m%d").astype(int)
    values["local_minute"] = local.dt.hour * 60 + local.dt.minute
    values["timestamp_ns"] = values["timestamp"].dt.as_unit("ns").astype("int64")
    return values


def _timestamp_inventory(frame: pd.DataFrame) -> dict[tuple[str, int], set[int]]:
    output: dict[tuple[str, int], set[int]] = {}
    for (market, day), group in frame.groupby(["symbol", "session_day"], sort=True):
        output[(str(market), int(day))] = set(
            int(value) for value in group["timestamp_ns"]
        )
    return output


def _expected_minute_ns(session_day: int, start_minute: int, end_minute: int) -> set[int]:
    if start_minute < 0 or end_minute < start_minute or end_minute >= 24 * 60:
        raise IntradayRatesShockTripwireError("invalid frozen coverage clock")
    text = str(int(session_day))
    midnight = pd.Timestamp(
        f"{text[:4]}-{text[4:6]}-{text[6:8]}", tz="America/Chicago"
    )
    start = midnight + pd.Timedelta(minutes=int(start_minute))
    count = int(end_minute) - int(start_minute) + 1
    return set(
        int(value)
        for value in pd.date_range(start, periods=count, freq="1min")
        .tz_convert("UTC")
        .as_unit("ns")
        .asi8
    )


def _planned_roll_session_days(roll_map: RollMap, market: str) -> set[int]:
    values: set[int] = set()
    for contract in roll_map.contracts:
        if str(contract.root) != str(market) or not contract.roll_date:
            continue
        stamp = pd.Timestamp(contract.roll_date)
        if stamp.tzinfo is None:
            stamp = stamp.tz_localize("UTC")
        else:
            stamp = stamp.tz_convert("UTC")
        for offset in range(-int(roll_map.unsafe_window_days), int(roll_map.unsafe_window_days) + 1):
            day = stamp.normalize() + pd.Timedelta(days=offset)
            values.add(int(day.strftime("%Y%m%d")))
    return values


def _split_starts_by_coverage(
    starts: Sequence[tuple[int, str]],
    calendar: Sequence[int],
    *,
    horizon: int,
    censored_days: Sequence[int],
) -> tuple[tuple[tuple[int, str], ...], tuple[tuple[int, str], ...]]:
    positions = {int(day): index for index, day in enumerate(calendar)}
    censored = {int(day) for day in censored_days}
    full: list[tuple[int, str]] = []
    rejected: list[tuple[int, str]] = []
    for start in starts:
        start_day = int(start[0])
        index = positions[start_day]
        window = {int(day) for day in calendar[index : index + int(horizon)]}
        (rejected if window & censored else full).append((start_day, str(start[1])))
    return tuple(full), tuple(rejected)


def _assert_no_cemetery_resurrection(
    project: Path, card: Mapping[str, Any]
) -> dict[str, Any]:
    audit = dict(card["cemetery_audit"])
    path = _inside(project, audit["graveyard_path"])
    if _sha256(path) != str(audit["graveyard_sha256_at_selection"]):
        raise IntradayRatesShockTripwireError("frozen cemetery SHA drift")
    checked = {
        str(value).lower() for value in audit.get("mechanism_classes_checked", ())
    }
    required = {CANONICAL_MECHANISM_CLASS, *CANONICAL_SUBCLASSES}
    if checked != required:
        raise IntradayRatesShockTripwireError("cemetery mechanism-class audit drift")
    reviewed = {
        str(value).lower() for value in audit.get("adjacent_tombstones_reviewed", ())
    }
    if not REQUIRED_ADJACENT_TOMBSTONE_REVIEWS.issubset(reviewed):
        raise IntradayRatesShockTripwireError("adjacent cemetery review is incomplete")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = tuple(
            str(row[0]).lower()
            for row in connection.execute(
                "SELECT mechanism_class FROM class_tombstones "
                f"WHERE lower(mechanism_class) IN ({','.join('?' for _ in required)})",
                tuple(sorted(required)),
            )
        )
    finally:
        connection.close()
    if rows or int(audit["exact_mechanism_class_collision_count"]) != 0:
        raise IntradayRatesShockTripwireError("exact cemetery class collision")
    return {
        "graveyard_sha256": _sha256(path),
        "mechanism_classes_checked": sorted(required),
        "mechanism_class_collision_count": 0,
        "adjacent_tombstones_reviewed": sorted(reviewed),
    }


def _assert_no_structural_resurrection(
    card: Mapping[str, Any],
    proposals: Sequence[Mapping[str, Any]],
    audit: Mapping[str, Any],
) -> dict[str, Any]:
    cemetery = dict(card["cemetery_audit"])
    fingerprints = tuple(
        str(row.get("canonical_structural_fingerprint") or "") for row in proposals
    )
    if not fingerprints or any(len(value) != 64 for value in fingerprints):
        raise IntradayRatesShockTripwireError("canonical fingerprint is absent")
    if len(fingerprints) != len(set(fingerprints)):
        raise IntradayRatesShockTripwireError("canonical fingerprint collision")
    forbidden = {
        str(value)
        for value in cemetery.get("forbidden_canonical_structural_fingerprints", ())
    }
    collisions = sorted(set(fingerprints) & forbidden)
    if collisions:
        raise IntradayRatesShockTripwireError("tombstoned structural fingerprint collision")
    return {
        **dict(audit),
        "canonical_fingerprint_schema": str(
            cemetery["canonical_fingerprint_schema"]
        ),
        "canonical_fingerprint_count": len(fingerprints),
        "canonical_fingerprint_collision_count": 0,
        "canonical_fingerprint_set_hash": stable_hash(sorted(fingerprints)),
    }


def _verified(project: Path, values: Mapping[str, Any], prefix: str) -> Path:
    path = _inside(project, str(values[f"{prefix}_path"]))
    if _sha256(path) != str(values[f"{prefix}_sha256"]):
        raise IntradayRatesShockTripwireError(f"{prefix} SHA drift")
    return path


def _verified_row(project: Path, row: Mapping[str, Any], *, label: str) -> Path:
    path = _inside(project, str(row["path"]))
    if _sha256(path) != str(row["sha256"]):
        raise IntradayRatesShockTripwireError(f"{label} SHA drift")
    return path


def _inside(project: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (project / path).resolve() if not path.is_absolute() else path.resolve()
    if not resolved.is_relative_to(project) or not resolved.is_file():
        raise IntradayRatesShockTripwireError("source path escapes project or is absent")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise IntradayRatesShockTripwireError("expected a JSON object")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clock_minutes(value: str) -> int:
    hour, minute = (int(item) for item in str(value).split(":"))
    return hour * 60 + minute


def _day_int(value: str) -> int:
    return int(str(value)[:10].replace("-", ""))


def _block(session_day: int) -> str:
    text = str(int(session_day))
    month = int(text[4:6])
    quarter = 1 + (month - 1) // 3
    return f"{text[:4]}_Q{quarter}"


def _one(values: Sequence[Any], label: str) -> Any:
    if len(values) != 1:
        raise IntradayRatesShockTripwireError(f"expected one {label}, got {len(values)}")
    return values[0]


def _group_rows(
    rows: Sequence[Mapping[str, Any]], keys: Sequence[str]
) -> Iterable[tuple[tuple[Any, ...], list[Mapping[str, Any]]]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(tuple(row[key] for key in keys), []).append(row)
    return sorted(grouped.items(), key=lambda item: item[0])


def _zero_safety_counters() -> dict[str, int]:
    return {"broker_connections": 0, "orders": 0, "tier_q_created": 0}


__all__ = [
    "BRANCH_ID",
    "IntradayRatesShockTripwireError",
    "audit_intraday_rates_shock_coverage",
    "build_rule_events",
    "load_decision_card",
    "run_intraday_rates_shock_tripwire",
]
