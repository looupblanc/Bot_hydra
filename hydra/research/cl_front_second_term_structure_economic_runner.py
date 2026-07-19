"""Read-only economic runner for the frozen CL front/second tripwire.

The sealed acquisition receipt is the first gate.  Only after it reconciles do
we decode CL.c.1, join it causally to the already frozen CL.c.0/MCL cache, build
the eight preregistered rules and run exact chronological Topstep account
episodes.  This module has no registry, controller, database, promotion,
broker, order, Q4 or network capability.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash
from hydra.markets.instruments import instrument_spec
from hydra.production import autonomous_exact_replay as exact
from hydra.production.fresh_confirmation_lane import non_overlapping_starts
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.mll_variants import advance_end_of_day_floor, advance_intraday_floor
from hydra.propfirm.scaling_plan import mini_equivalent
from hydra.research.cl_front_second_term_structure_tripwire import (
    CLTermStructureRule,
    causal_intent,
    frozen_rule_specs,
    prepare_causal_source_features,
)
from scripts.acquire_cl_front_second_term_structure_tripwire import (
    CARD_PATH,
    DEFAULT_RECEIPT,
    END,
    FRONT_SYMBOL,
    START,
    SYMBOL,
    validate_acquisition_receipt,
)


SCHEMA = "hydra_cl_front_second_term_structure_economic_tripwire_v1"
AUDIT_SCHEMA = "hydra_cl_front_second_term_structure_runner_audit_v1"
BRANCH_ID = "CL_FRONT_SECOND_TERM_STRUCTURE_INNOVATION_TO_MCL_OUTRIGHT_V1"
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
SCENARIOS = ("NORMAL", "STRESSED")
CONTROLS = (
    "PRIMARY",
    "FRONT_ONLY_RETURN_SHOCK",
    "BASIS_SIGN_FLIP",
    "SESSION_MATCHED_TIMING_NULL",
    "CARRY_LEVEL_ONLY",
)
HORIZONS = (5, 10, 20)
HEADLINE_GATE_HORIZON = max(HORIZONS)
RISK_FRACTIONS = (0.1, 0.2, 0.3)
NORMAL_COST = 4.0
STRESSED_COST = 6.0
TARGET_MARKET = "MCL"
MAX_SELECTED_RULES = 2
MIN_COMPLETE_SESSION_ROWS = 600
SESSION_FLATTEN_MINUTE = 15 * 60 + 10
ELIGIBLE_SOURCE_START_MINUTE = 7 * 60
ELIGIBLE_SOURCE_END_MINUTE = 14 * 60
CAUSAL_WARMUP_MINUTES = 60
SOURCE_WARMUP_PRICE_OBSERVATIONS = CAUSAL_WARMUP_MINUTES + 1
SOURCE_REQUIRED_COVERAGE_START_MINUTE = (
    ELIGIBLE_SOURCE_START_MINUTE - SOURCE_WARMUP_PRICE_OBSERVATIONS
)
TARGET_REQUIRED_COVERAGE_START_MINUTE = (
    ELIGIBLE_SOURCE_START_MINUTE - CAUSAL_WARMUP_MINUTES
)


class CLTermStructureEconomicError(RuntimeError):
    """The frozen economic tripwire cannot run without semantic drift."""


def _session_decision_contract(card: Mapping[str, Any]) -> dict[str, Any]:
    """Bind the explicit Chicago decision clock to the sealed causal card."""

    lookbacks = tuple(
        int(value)
        for value in card["smallest_decisive_falsification_experiment"][
            "source_lookbacks_minutes"
        ]
    )
    causal = card["causal_contract"]
    if (
        max(lookbacks) != CAUSAL_WARMUP_MINUTES
        or causal.get("missing_interval") != "DATA_CENSORED"
        or causal.get("entry")
        != "NEXT_TRADABLE_MCL_BAR_OPEN_AFTER_COMPLETED_DECISION_BAR"
        or causal.get("same_session_flatten_required") is not True
    ):
        raise CLTermStructureEconomicError("session decision contract/card drift")
    core = {
        "schema": "hydra_cl_term_structure_session_decision_contract_v1",
        "frozen_decision_card_hash": str(card["card_hash"]),
        "decision_window_chicago": {
            "start": "07:00",
            "end_exclusive": "14:00",
            "start_minute_inclusive": ELIGIBLE_SOURCE_START_MINUTE,
            "end_minute_exclusive": ELIGIBLE_SOURCE_END_MINUTE,
        },
        "causal_warmup_minutes": CAUSAL_WARMUP_MINUTES,
        "source_prior_return_count": CAUSAL_WARMUP_MINUTES,
        "source_warmup_price_observations": SOURCE_WARMUP_PRICE_OBSERVATIONS,
        "required_source_coverage_chicago": {
            "start": "05:59",
            "end_exclusive": "14:00",
        },
        "required_target_coverage_chicago": {
            "start": "06:00",
            "end_inclusive": "15:10",
        },
        "derivation": {
            "source_lookbacks_minutes_from_card": list(lookbacks),
            "missing_interval_from_card": str(causal["missing_interval"]),
            "entry_from_card": str(causal["entry"]),
            "same_session_flatten_from_card": bool(
                causal["same_session_flatten_required"]
            ),
        },
    }
    return {**core, "contract_hash": stable_hash(core)}


def audit_tripwire_inputs(
    root: str | Path,
    *,
    card_path: str | Path = CARD_PATH,
    receipt_path: str | Path = DEFAULT_RECEIPT,
) -> dict[str, Any]:
    """Verify all bindings without decoding or reading an economic outcome."""

    project = Path(root).resolve()
    receipt = validate_acquisition_receipt(
        project, card_path=card_path, receipt_path=receipt_path
    )
    card_file = _inside(project, card_path)
    card = _read_json(card_file)
    if (
        card.get("selected_branch") != BRANCH_ID
        or card.get("governance", {}).get("tier_ceiling") != "E"
        or card.get("governance", {}).get("tier_q_allowed") is not False
        or card.get("governance", {}).get("promotion_allowed") is not False
        or card.get("governance", {}).get("q4_access_allowed") is not False
        or card.get("governance", {}).get("broker_connection_allowed") is not False
        or card.get("governance", {}).get("orders_allowed") is not False
    ):
        raise CLTermStructureEconomicError("decision-card governance drift")
    if tuple(int(value) for value in card["account_frontier"]["horizons_trading_days"]) != HORIZONS:
        raise CLTermStructureEconomicError("account horizon drift")
    if tuple(float(value) for value in card["account_frontier"]["risk_fraction_of_current_mll_buffer"]) != RISK_FRACTIONS:
        raise CLTermStructureEconomicError("account risk frontier drift")
    if card["chronological_roles"][-1]["end"] != END or END > "2024-10-01":
        raise CLTermStructureEconomicError("protected Q4 boundary drift")
    if len(frozen_rule_specs()) != 8:
        raise CLTermStructureEconomicError("frozen rule cardinality drift")
    session_decision_contract = _session_decision_contract(card)

    bindings: dict[str, Any] = {}
    for name in ("existing_front_and_execution", "existing_front_roll_map", "rule_snapshot"):
        row = dict(card["frozen_inputs"][name])
        artifact = _inside(project, row["path"])
        digest = sha256_file(artifact)
        if digest != str(row["sha256"]):
            raise CLTermStructureEconomicError(f"frozen binding drift: {name}")
        bindings[name] = {
            "path": str(artifact.relative_to(project)),
            "sha256": digest,
            "size_bytes": artifact.stat().st_size,
        }
    acquired = {
        str(row["kind"]): {
            "path": str(Path(row["path"]).resolve().relative_to(project)),
            "sha256": str(row["sha256"]),
            "size_bytes": int(row["size_bytes"]),
        }
        for row in receipt["files"]
    }
    core = {
        "schema": AUDIT_SCHEMA,
        "status": "READY_FOR_BOUNDED_ECONOMIC_REPLAY",
        "branch_id": BRANCH_ID,
        "receipt_hash": receipt["receipt_hash"],
        "bundle_id": receipt["bundle_id"],
        "decision_card_hash": card["card_hash"],
        "request_hash": receipt["request_hash"],
        "acquisition_local_validation": receipt["local_validation"],
        "frozen_bindings": bindings,
        "acquired_bindings": acquired,
        "rule_count": 8,
        "control_count": len(CONTROLS) - 1,
        "account_sizes": list(card["account_frontier"]["account_sizes"]),
        "horizons_trading_days": list(HORIZONS),
        "risk_fractions": list(RISK_FRACTIONS),
        "session_decision_contract": session_decision_contract,
        "latest_data_end_exclusive": END,
        "q4_rows": 0,
        "network_requests": 0,
        "writes": 0,
        "promotion_allowed": False,
        "tier_ceiling": "E",
    }
    return {**core, "audit_hash": stable_hash(core)}


def run_tripwire(
    root: str | Path,
    *,
    card_path: str | Path = CARD_PATH,
    receipt_path: str | Path = DEFAULT_RECEIPT,
) -> dict[str, Any]:
    """Run the complete bounded tripwire and return a self-hashed result."""

    project = Path(root).resolve()
    # This must remain the first operation: no market-data decoder is called
    # until the local receipt, spend ledger and access ledger reconcile.
    audit = audit_tripwire_inputs(project, card_path=card_path, receipt_path=receipt_path)
    card = _read_json(_inside(project, card_path))
    acquired = audit["acquired_bindings"]

    front, target, target_raw, roll_audit = _load_frozen_front_and_target(
        project,
        card,
    )
    second, second_audit, symbology, second_expiry = _load_second_rank(
        project / acquired["RAW_DBN_OHLCV_1M"]["path"],
        project / acquired["RAW_DBN_DEFINITION"]["path"],
        project / acquired["SYMBOLOGY_RESOLUTION"]["path"],
    )
    front, second = _attach_rank_delivery_state(
        front,
        second,
        symbology,
        second_expiry=second_expiry,
        project=project,
        card=card,
    )
    front, second = _attach_rank_roll_guards(front, second, symbology)
    target = _attach_target_roll_guard(target, project, card)

    feature_sets = {
        lookback: _with_control_scores(
            prepare_causal_source_features(
                front,
                second,
                lookback_minutes=lookback,
            ),
            audit["session_decision_contract"],
        )
        for lookback in (15, 60)
    }
    target_index = _TargetIndex(target)
    proposals, event_sets, signal_sets = _build_all_events(feature_sets, target_index, card)
    selected = _select_on_discovery(proposals, event_sets, card)
    power = _power_preflight(selected, signal_sets, card)

    rules_path = project / audit["frozen_bindings"]["rule_snapshot"]["path"]
    account_rules, rule_receipt = exact._load_rule_snapshot(rules_path)
    calendars, coverage = _role_calendars(
        target_raw,
        front,
        second,
        card,
        audit["session_decision_contract"],
    )
    decisions = [
        _evaluate_candidate(
            proposal,
            event_sets[str(proposal["candidate_id"])],
            signal_sets[str(proposal["candidate_id"])],
            calendars=calendars,
            coverage=coverage,
            account_rules=account_rules,
            card=card,
        )
        for proposal in selected
    ]
    branch_gate = _branch_gate(decisions, power, card)
    if branch_gate["status"] == "TERM_STRUCTURE_TRIPWIRE_GREEN_TIER_E":
        next_action = card["branch_rule"]["when_green"]
    elif branch_gate["status"] == "TERM_STRUCTURE_TRIPWIRE_WEAK":
        next_action = card["branch_rule"]["when_weak"]
    elif branch_gate["status"] == "TERM_STRUCTURE_TRIPWIRE_UNDERPOWERED_NO_THRESHOLD_RELAXATION":
        next_action = card["power_preflight"]["when_underpowered"]
    else:
        next_action = card["branch_rule"]["when_falsified"]

    account_cells = sum(int(row["account_cell_count"]) for row in decisions)
    account_episodes = sum(int(row["account_episode_count"]) for row in decisions)
    core = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": branch_gate["status"],
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "source_audit": audit,
        "rule_snapshot": rule_receipt,
        "data_reconstruction": {
            "front_rows": len(front),
            "second_rows": len(second),
            "target_rows": len(target),
            "second_rank": second_audit,
            "front_target": roll_audit,
            "aligned_rows_by_lookback": {
                str(key): len(value) for key, value in feature_sets.items()
            },
            "causal_fill": "NEXT_TRADABLE_MCL_OPEN_AFTER_COMPLETED_DECISION_BAR",
            "causal_exit": "NEXT_TRADABLE_MCL_OPEN_AFTER_STOP_TARGET_TIME_OR_SESSION_FLATTEN_DECISION",
            "same_bar_ambiguous": "STOP_FIRST",
        },
        "proposal_count": len(proposals),
        "selected_rule_count": len(selected),
        "selected_rules": selected,
        "event_counts": {
            candidate: {control: len(rows) for control, rows in sorted(values.items())}
            for candidate, values in sorted(event_sets.items())
        },
        "executable_event_ledger_hashes": {
            candidate: {
                control: stable_hash(list(rows))
                for control, rows in sorted(values.items())
            }
            for candidate, values in sorted(event_sets.items())
        },
        "decision_opportunity_counts": {
            candidate: {
                control: len(rows) for control, rows in sorted(values.items())
            }
            for candidate, values in sorted(signal_sets.items())
        },
        "decision_outcome_status_counts": {
            candidate: {
                control: dict(
                    sorted(Counter(str(row["outcome_status"]) for row in rows).items())
                )
                for control, rows in sorted(values.items())
            }
            for candidate, values in sorted(signal_sets.items())
        },
        "decision_ledger_hashes": {
            candidate: {
                control: stable_hash(list(rows))
                for control, rows in sorted(values.items())
            }
            for candidate, values in sorted(signal_sets.items())
        },
        "power_preflight": power,
        "role_calendar_counts": {role: len(days) for role, days in calendars.items()},
        "coverage_audit": coverage,
        "account_cell_count": account_cells,
        "account_episode_count": account_episodes,
        "candidate_decisions": decisions,
        "branch_gate": branch_gate,
        "governance": {
            "q4_rows": 0,
            "protected_data_access_count_delta": 0,
            "incremental_data_spend_usd": 0.0,
            "broker_connections": 0,
            "orders": 0,
            "mission_database_writes": 0,
            "registry_writes": 0,
            "controller_or_service_changes": 0,
            "promotion_allowed": False,
            "tier_q_allowed": False,
        },
        "implementation_hashes": {
            "economic_runner_sha256": sha256_file(Path(__file__).resolve()),
            "causal_primitive_sha256": sha256_file(
                Path(__file__).with_name("cl_front_second_term_structure_tripwire.py")
            ),
        },
        "next_autonomous_action": next_action,
    }
    return {**core, "result_hash": stable_hash(core)}


def _load_frozen_front_and_target(
    project: Path, card: Mapping[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    path = _inside(project, card["frozen_inputs"]["existing_front_and_execution"]["path"])
    columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume", "session_id"]
    raw = pd.read_parquet(path, columns=columns, filters=[("symbol", "in", ["CL", "MCL"])])
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
    raw = raw.loc[
        raw["timestamp"].ge(pd.Timestamp(START, tz="UTC"))
        & raw["timestamp"].lt(pd.Timestamp(END, tz="UTC"))
    ].copy()
    raw = _session_fields(raw)
    if raw.duplicated(["symbol", "timestamp"]).any():
        raise CLTermStructureEconomicError("front/target cache has duplicate rows")
    front = raw.loc[raw["symbol"].eq("CL")].copy()
    target_raw = raw.loc[raw["symbol"].eq("MCL")].copy()
    if front.empty or target_raw.empty:
        raise CLTermStructureEconomicError("frozen CL/MCL binding is empty")
    front["available_at"] = front["timestamp"] + pd.Timedelta(minutes=1)
    front["roll_unsafe"] = False
    target = target_raw.copy()
    target["roll_unsafe"] = False
    return (
        front,
        target,
        target_raw,
        {
            "cache_sha256": sha256_file(path),
            "front_row_count": len(front),
            "target_row_count": len(target),
            "duplicate_symbol_timestamp_rows": 0,
        },
    )


def _load_second_rank(
    raw_path: Path, definition_path: Path, symbology_path: Path
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, pd.Timestamp]]:
    symbology = _read_json(symbology_path)
    core = dict(symbology)
    claimed = str(core.pop("mapping_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise CLTermStructureEconomicError("second-rank symbology hash drift")
    if symbology.get("same_instrument_interval_count") != 0:
        raise CLTermStructureEconomicError("front and second delivery collide")
    store = _import_databento().DBNStore.from_file(raw_path)
    frame = store.to_df(pretty_ts=True, map_symbols=False, price_type="float").reset_index()
    frame = frame.rename(columns={"ts_event": "timestamp"})
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.loc[
        frame["timestamp"].ge(pd.Timestamp(START, tz="UTC"))
        & frame["timestamp"].lt(pd.Timestamp(END, tz="UTC"))
    ].copy()
    expected = _instrument_by_utc_date(
        frame["timestamp"], symbology["continuous_mapping"][SYMBOL]
    )
    actual = frame["instrument_id"].astype(str)
    mismatch = actual.ne(expected)
    if mismatch.any() or expected.isna().any():
        raise CLTermStructureEconomicError("downloaded CL.c.1 rank mapping mismatch")
    if frame["timestamp"].duplicated().any():
        raise CLTermStructureEconomicError("downloaded CL.c.1 duplicates timestamps")
    frame["symbol"] = SYMBOL
    frame = _session_fields(frame)
    frame["available_at"] = frame["timestamp"] + pd.Timedelta(minutes=1)
    frame["roll_unsafe"] = False
    definitions = (
        _import_databento()
        .DBNStore.from_file(definition_path)
        .to_df(pretty_ts=True, map_symbols=False, price_type="float")
        .reset_index()
    )
    definitions["expiration"] = pd.to_datetime(definitions["expiration"], utc=True)
    expiry_rows = definitions.loc[
        definitions["instrument_id"].astype(str).isin(set(actual)),
        ["instrument_id", "expiration"],
    ].dropna()
    conflicts = expiry_rows.groupby(expiry_rows["instrument_id"].astype(str))["expiration"].nunique()
    if (conflicts > 1).any():
        raise CLTermStructureEconomicError("date-aware definition expiry drift")
    expiry_by_instrument = {
        str(key): pd.Timestamp(group["expiration"].iloc[-1])
        for key, group in expiry_rows.groupby(expiry_rows["instrument_id"].astype(str), sort=True)
    }
    missing_expiry = set(actual) - set(expiry_by_instrument)
    if missing_expiry:
        raise CLTermStructureEconomicError("second-rank expiry definition missing")
    return (
        frame,
        {
            "raw_sha256": sha256_file(raw_path),
            "definition_sha256": sha256_file(definition_path),
            "symbology_sha256": sha256_file(symbology_path),
            "row_count": len(frame),
            "instrument_count": int(actual.nunique()),
            "rank_mapping_mismatch_count": 0,
            "duplicate_timestamp_count": 0,
            "expiry_definition_count": len(expiry_by_instrument),
        },
        symbology,
        expiry_by_instrument,
    )


def _attach_rank_delivery_state(
    front: pd.DataFrame,
    second: pd.DataFrame,
    symbology: Mapping[str, Any],
    *,
    second_expiry: Mapping[str, pd.Timestamp],
    project: Path,
    card: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Attach only known contract identity and calendar expiry distance."""

    front_ids = _instrument_by_utc_date(
        front["timestamp"], symbology["continuous_mapping"][FRONT_SYMBOL]
    )
    second_ids = second["instrument_id"].astype(str)
    roll_path = _inside(
        project, card["frozen_inputs"]["existing_front_roll_map"]["path"]
    )
    roll_map = load_roll_map(roll_path)
    front_expiry = {
        str(row.instrument_id): pd.Timestamp(row.expiry_date, tz="UTC")
        for row in roll_map.contracts
        if row.root == "CL" and row.instrument_id
    }
    if set(front_ids.dropna().astype(str)) - set(front_expiry):
        raise CLTermStructureEconomicError("front-rank expiry definition missing")
    front["rank_contract"] = front_ids.astype(str)
    second["rank_contract"] = second_ids
    front["days_to_delivery"] = [
        (front_expiry[str(instrument)].normalize() - timestamp.normalize()).total_seconds()
        / 86_400.0
        for instrument, timestamp in zip(front_ids, front["timestamp"], strict=True)
    ]
    second["days_to_delivery"] = [
        (second_expiry[str(instrument)].normalize() - timestamp.normalize()).total_seconds()
        / 86_400.0
        for instrument, timestamp in zip(second_ids, second["timestamp"], strict=True)
    ]
    if (
        front["days_to_delivery"].isna().any()
        or second["days_to_delivery"].isna().any()
        or (second["days_to_delivery"] <= 0.0).any()
    ):
        raise CLTermStructureEconomicError("invalid known days-to-delivery state")
    return front, second


def _attach_rank_roll_guards(
    front: pd.DataFrame, second: pd.DataFrame, symbology: Mapping[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    boundaries = {
        str(row["d0"])
        for rank in (FRONT_SYMBOL, SYMBOL)
        for row in symbology["continuous_mapping"][rank]
        if str(row["d0"]) != START
    }
    all_days = sorted(set(front["session_day"]) | set(second["session_day"]))
    unsafe = _true_session_guard_days(front, boundaries, all_days, radius=1)
    for frame in (front, second):
        frame["roll_unsafe"] = frame["session_day"].isin(unsafe)
    return front, second


def _attach_target_roll_guard(
    target: pd.DataFrame, project: Path, card: Mapping[str, Any]
) -> pd.DataFrame:
    path = _inside(project, card["frozen_inputs"]["existing_front_roll_map"]["path"])
    roll_map = load_roll_map(path)
    boundaries = {
        str(row.active_start)[:10]
        for row in roll_map.contracts
        if row.root == TARGET_MARKET and str(row.active_start)[:10] >= START
        and str(row.active_start)[:10] < END
    }
    all_days = sorted(set(target["session_day"]))
    unsafe = _true_session_guard_days(target, boundaries, all_days, radius=1)
    target["roll_unsafe"] = target["session_day"].isin(unsafe)
    return target


def _with_control_scores(
    frame: pd.DataFrame, session_contract: Mapping[str, Any]
) -> pd.DataFrame:
    output = frame.copy()
    output["front_return_score"] = _prior_robust_score(
        np.log(output["close_front"].astype(float)).diff(), output
    )
    output["carry_level_score"] = _prior_robust_score(
        output["log_front_second_basis"].astype(float), output
    )
    local = output["local_minute_chicago"].astype(str).str.split(":", expand=True)
    local_minute = pd.to_numeric(local[0], errors="coerce") * 60 + pd.to_numeric(
        local[1], errors="coerce"
    )
    decision = session_contract["decision_window_chicago"]
    output["eligible_source_clock"] = local_minute.ge(
        int(decision["start_minute_inclusive"])
    ) & local_minute.lt(int(decision["end_minute_exclusive"]))
    output["decision_eligible"] &= output["eligible_source_clock"]
    return output


def _prior_robust_score(values: pd.Series, frame: pd.DataFrame) -> pd.Series:
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    groups = frame.groupby("local_minute_chicago", sort=False).groups
    for indexes in groups.values():
        positions = list(indexes)
        sample = values.loc[positions].astype(float)
        prior = sample.shift(1)
        median = prior.rolling(20, min_periods=10).median()
        q25 = prior.rolling(20, min_periods=10).quantile(0.25)
        q75 = prior.rolling(20, min_periods=10).quantile(0.75)
        scale = (q75 - q25).replace(0.0, np.nan)
        result.loc[positions] = ((sample - median) / scale).to_numpy()
    return result


class _TargetIndex:
    def __init__(self, frame: pd.DataFrame) -> None:
        ordered = frame.sort_values("timestamp", kind="mergesort").reset_index(drop=True).copy()
        self.frame = ordered
        # Pandas 3 may preserve DBN timestamps at microsecond resolution when
        # casting; Timestamp.value is always nanoseconds and matches the event
        # ledger contract used by the account replay.
        self.ns = np.asarray(
            [pd.Timestamp(value).value for value in ordered["timestamp"]],
            dtype=np.int64,
        )

    def first_after(self, timestamp: pd.Timestamp) -> int | None:
        index = int(np.searchsorted(self.ns, int(timestamp.value), side="right"))
        return index if index < len(self.frame) else None

    def at_or_after(self, timestamp: pd.Timestamp) -> int | None:
        index = int(np.searchsorted(self.ns, int(timestamp.value), side="left"))
        return index if index < len(self.frame) else None


def _build_all_events(
    feature_sets: Mapping[int, pd.DataFrame],
    target: _TargetIndex,
    card: Mapping[str, Any],
) -> tuple[
    list[dict[str, Any]],
    dict[str, dict[str, list[dict[str, Any]]]],
    dict[str, dict[str, list[dict[str, Any]]]],
]:
    proposals: list[dict[str, Any]] = []
    event_sets: dict[str, dict[str, list[dict[str, Any]]]] = {}
    signal_sets: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for rule in frozen_rule_specs():
        proposal = {
            **rule.to_dict(),
            "candidate_id": rule.rule_id,
            "candidate_hash": stable_hash(rule.to_dict()),
        }
        proposals.append(proposal)
        features = feature_sets[rule.lookback_minutes]
        primary, primary_signals = _events_for_score(
            features, target, rule, score_kind="PRIMARY"
        )
        flip_signals = [
            _replay_at_timestamp(
                target,
                pd.Timestamp(row["decision_time"]),
                -int(row["side"]),
                rule,
                control="BASIS_SIGN_FLIP",
                source_feature_hash=str(row["feature_hash"]),
                source_score=float(row["source_score"]),
            )
            for row in primary_signals
        ]
        controls = {
            "PRIMARY": primary,
            "BASIS_SIGN_FLIP": _completed_events(flip_signals),
        }
        signals = {
            "PRIMARY": primary_signals,
            "BASIS_SIGN_FLIP": flip_signals,
        }
        for control, score_kind in (
            ("FRONT_ONLY_RETURN_SHOCK", "FRONT_ONLY_RETURN_SHOCK"),
            ("CARRY_LEVEL_ONLY", "CARRY_LEVEL_ONLY"),
        ):
            matched_events, matched_signals = _matched_score_control(
                features, target, rule, primary_signals, score_kind=score_kind
            )
            controls[control] = matched_events
            signals[control] = matched_signals
        timing_signals = _timing_null_control(
            target, rule, primary_signals, card
        )
        controls["SESSION_MATCHED_TIMING_NULL"] = _completed_events(timing_signals)
        signals["SESSION_MATCHED_TIMING_NULL"] = timing_signals
        controls = {
            control: _nonoverlapping_events(rows)
            for control, rows in controls.items()
        }
        event_sets[rule.rule_id] = controls
        signal_sets[rule.rule_id] = signals
    return proposals, event_sets, signal_sets


def _events_for_score(
    features: pd.DataFrame,
    target: _TargetIndex,
    rule: CLTermStructureRule,
    *,
    score_kind: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    next_allowed = -1
    censored_open_session: int | None = None
    for value in features.itertuples(index=False):
        signal_timestamp = pd.Timestamp(value.timestamp)
        decision_time = pd.Timestamp(value.available_at)
        decision_session = _session_day_value(decision_time)
        if censored_open_session is not None and decision_session != censored_open_session:
            censored_open_session = None
        if (
            decision_time.value <= next_allowed
            or censored_open_session == decision_session
            or not bool(value.decision_eligible)
        ):
            continue
        mapping = value._asdict()
        if score_kind == "PRIMARY":
            direction = causal_intent(mapping, rule)
            score = float(value.basis_robust_score_prior_sessions)
        elif score_kind == "FRONT_ONLY_RETURN_SHOCK":
            score = float(value.front_return_score)
            direction = int(np.sign(score)) if np.isfinite(score) and abs(score) >= rule.trigger_score else 0
            if rule.mechanism.endswith("REVERSION"):
                direction *= -1
        elif score_kind == "CARRY_LEVEL_ONLY":
            score = float(value.carry_level_score)
            direction = int(np.sign(score)) if np.isfinite(score) and abs(score) >= rule.trigger_score else 0
            if rule.mechanism.endswith("REVERSION"):
                direction *= -1
        else:
            raise CLTermStructureEconomicError("unknown score lane")
        if direction == 0:
            continue
        feature_core = {
            "signal_timestamp": signal_timestamp.isoformat(),
            "decision_time": decision_time.isoformat(),
            "available_at": pd.Timestamp(value.available_at).isoformat(),
            "score_kind": score_kind,
            "score": score,
            "direction": direction,
            "front_days_to_delivery": float(value.front_days_to_delivery),
            "second_days_to_delivery": float(value.second_days_to_delivery),
            "delivery_tenor_gap_days": float(value.delivery_tenor_gap_days),
            "roll_distance_adjusted_basis_innovation": float(
                value.roll_distance_adjusted_basis_innovation
            ),
            "current_spread_state": float(value.current_spread_state),
            "front_prior_realized_volatility": float(
                value.front_prior_realized_volatility
            ),
            "rule_hash": stable_hash(rule.to_dict()),
        }
        event = _replay_at_timestamp(
            target,
            decision_time,
            direction,
            rule,
            control="PRIMARY" if score_kind == "PRIMARY" else score_kind,
            source_feature_hash=stable_hash(feature_core),
            source_score=score,
        )
        signals.append(event)
        if event["outcome_status"] == "EXECUTABLE_COMPLETE":
            rows.append(event)
            next_allowed = int(event["exit_ns"])
        elif bool(event.get("position_opened")):
            censored_open_session = decision_session
    return rows, signals


def _matched_score_control(
    features: pd.DataFrame,
    target: _TargetIndex,
    rule: CLTermStructureRule,
    primary_signals: Sequence[Mapping[str, Any]],
    *,
    score_kind: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if features["available_at"].duplicated().any():
        raise CLTermStructureEconomicError("control feature timestamps are not unique")
    by_decision = features.set_index("available_at", drop=False)
    score_column = {
        "FRONT_ONLY_RETURN_SHOCK": "front_return_score",
        "CARRY_LEVEL_ONLY": "carry_level_score",
    }.get(score_kind)
    if score_column is None:
        raise CLTermStructureEconomicError("unknown matched score control")
    signals: list[dict[str, Any]] = []
    for primary in primary_signals:
        decision_time = pd.Timestamp(primary["decision_time"])
        if decision_time not in by_decision.index:
            value = _noncomplete_decision(
                rule,
                score_kind,
                decision_time,
                int(primary["side"]),
                stable_hash(
                    {
                        "primary_event_id": primary["event_id"],
                        "score_kind": score_kind,
                        "reason": "CONTROL_FEATURE_TIMESTAMP_MISSING",
                    }
                ),
                0.0,
                status="DATA_CENSORED",
                reason="CONTROL_FEATURE_TIMESTAMP_MISSING",
            )
        else:
            row = by_decision.loc[decision_time]
            score = float(row[score_column])
            if not np.isfinite(score) or score == 0.0:
                value = _noncomplete_decision(
                    rule,
                    score_kind,
                    decision_time,
                    int(primary["side"]),
                    stable_hash(
                        {
                            "primary_event_id": primary["event_id"],
                            "score_kind": score_kind,
                            "score": None if not np.isfinite(score) else score,
                        }
                    ),
                    0.0,
                    status="CAUSAL_ABSTAIN",
                    reason="MATCHED_CONTROL_SCORE_UNAVAILABLE",
                )
            else:
                direction = int(np.sign(score))
                if rule.mechanism.endswith("REVERSION"):
                    direction *= -1
                value = _replay_at_timestamp(
                    target,
                    decision_time,
                    direction,
                    rule,
                    control=score_kind,
                    source_feature_hash=stable_hash(
                        {
                            "primary_event_id": primary["event_id"],
                            "score_kind": score_kind,
                            "score": score,
                        }
                    ),
                    source_score=score,
                )
        value["matched_primary_event_id"] = str(primary["event_id"])
        signals.append(value)
    ordered = sorted(
        signals, key=lambda row: (int(row["decision_ns"]), str(row["event_id"]))
    )
    return _completed_events(ordered), ordered


def _timing_null_control(
    target: _TargetIndex,
    rule: CLTermStructureRule,
    primary: Sequence[Mapping[str, Any]],
    card: Mapping[str, Any],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for role in ROLES:
        rows = [row for row in primary if _role_of_day(int(row["session_day"])) == role]
        if not rows:
            continue
        rotated = rows[1:] + rows[:1]
        for source, timing in zip(rows, rotated, strict=True):
            value = _replay_at_timestamp(
                target,
                pd.Timestamp(timing["decision_time"]),
                int(source["side"]),
                rule,
                control="SESSION_MATCHED_TIMING_NULL",
                source_feature_hash=stable_hash(
                    {
                        "source": source["feature_hash"],
                        "timing": timing["feature_hash"],
                        "policy": "ROLE_LOCAL_CYCLIC_TIMESTAMP_PERMUTATION_V1",
                    }
                ),
            )
            value["matched_primary_event_id"] = str(source["event_id"])
            value["timing_source_event_id"] = str(timing["event_id"])
            output.append(value)
    return sorted(output, key=lambda row: (int(row["decision_ns"]), str(row["event_id"])))


def _completed_events(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in rows
        if str(row.get("outcome_status")) == "EXECUTABLE_COMPLETE"
    ]


def _censored_signal_days(rows: Sequence[Mapping[str, Any]]) -> tuple[int, ...]:
    return tuple(
        sorted(
            {
                int(row["session_day"])
                for row in rows
                if str(row.get("outcome_status")) == "DATA_CENSORED"
            }
        )
    )


def _replay_at_timestamp(
    target: _TargetIndex,
    decision_time: pd.Timestamp,
    side: int,
    rule: CLTermStructureRule,
    *,
    control: str,
    source_feature_hash: str,
    source_score: float = 0.0,
) -> dict[str, Any]:
    # ``decision_time`` is the availability boundary of the completed source
    # bar.  The open stamped at that same boundary is the frozen next-bar open,
    # matching the repository's causal OHLCV convention.
    entry_index = target.at_or_after(decision_time)
    if entry_index is None:
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="DATA_CENSORED", reason="CENSORED_FUTURE_COVERAGE_NO_ENTRY_BAR",
        )
    frame = target.frame
    entry = frame.iloc[entry_index]
    if (
        pd.Timestamp(entry["timestamp"]) != decision_time
    ):
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="DATA_CENSORED", reason="CENSORED_FUTURE_COVERAGE_MISSING_NEXT_BAR",
        )
    if bool(entry["roll_unsafe"]):
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="CAUSAL_ABSTAIN", reason="TARGET_ROLL_GUARD",
        )
    if (
        int(entry["session_day"]) != _session_day_value(decision_time)
        or int(entry["local_minute"]) >= SESSION_FLATTEN_MINUTE - 1
    ):
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="CAUSAL_ABSTAIN", reason="SESSION_ENTRY_GUARD",
        )
    prior_start = max(0, entry_index - CAUSAL_WARMUP_MINUTES)
    prior = frame.iloc[prior_start:entry_index]
    if (
        len(prior) != CAUSAL_WARMUP_MINUTES
        or int(prior["session_day"].nunique()) != 1
        or int(prior.iloc[-1]["session_day"]) != int(entry["session_day"])
        or bool(prior["roll_unsafe"].any())
        or not prior["timestamp"].diff().dropna().eq(pd.Timedelta(minutes=1)).all()
        or pd.Timestamp(prior.iloc[-1]["timestamp"]) + pd.Timedelta(minutes=1)
        != pd.Timestamp(entry["timestamp"])
    ):
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="DATA_CENSORED",
            reason="CENSORED_REQUIRED_PRIOR_VOLATILITY_GAP",
        )
    prior_range = (prior["high"].astype(float) - prior["low"].astype(float)).median()
    if not np.isfinite(prior_range):
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="CAUSAL_ABSTAIN", reason="INSUFFICIENT_PRIOR_VOLATILITY_STATE",
        )
    tick = float(instrument_spec(TARGET_MARKET).tick_size)
    raw_risk = float(np.clip(float(prior_range) * math.sqrt(rule.holding_minutes), 0.25, 1.00))
    risk_unit = math.ceil(raw_risk / tick - 1e-12) * tick
    fill = float(entry["open"])
    stop_distance = math.ceil(rule.stop_r_multiple * risk_unit / tick - 1e-12) * tick
    target_distance = math.ceil(rule.target_r_multiple * risk_unit / tick - 1e-12) * tick
    stop = round(fill - side * stop_distance, 10)
    target_price = round(fill + side * target_distance, 10)
    # Entry occurs at the open stamped ``entry.timestamp``.  The bar stamped
    # entry + H - 1 is the final H-th holding bar; its completed state becomes
    # available at entry + H, where the time-exit fill is executable.
    last_time = pd.Timestamp(entry["timestamp"]) + pd.Timedelta(
        minutes=rule.holding_minutes - 1
    )
    exit_decision_index: int | None = None
    exit_reason = "TIME"
    same_bar = False
    path_indexes: list[int] = []
    for index in range(entry_index, len(frame) - 1):
        row = frame.iloc[index]
        if pd.Timestamp(row["timestamp"]) != pd.Timestamp(entry["timestamp"]) + pd.Timedelta(
            minutes=index - entry_index
        ):
            return _noncomplete_decision(
                rule, control, decision_time, side, source_feature_hash, source_score,
                status="DATA_CENSORED", reason="CENSORED_FUTURE_COVERAGE_PATH_GAP",
                position_opened=True,
            )
        if int(row["session_day"]) != int(entry["session_day"]):
            break
        path_indexes.append(index)
        high, low = float(row["high"]), float(row["low"])
        stop_hit = low <= stop if side > 0 else high >= stop
        target_hit = high >= target_price if side > 0 else low <= target_price
        local_minute = int(row["local_minute"])
        if stop_hit or target_hit:
            same_bar = stop_hit and target_hit
            exit_reason = "STOP_FIRST" if stop_hit else "TARGET"
            exit_decision_index = index
            break
        if pd.Timestamp(row["timestamp"]) >= last_time:
            exit_reason = "TIME"
            exit_decision_index = index
            break
        if local_minute >= SESSION_FLATTEN_MINUTE - 1:
            exit_reason = "SESSION_FLATTEN"
            exit_decision_index = index
            break
    if exit_decision_index is None or exit_decision_index + 1 >= len(frame):
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="DATA_CENSORED", reason="CENSORED_FUTURE_COVERAGE_NO_EXIT_BAR",
            position_opened=True,
        )
    exit_row = frame.iloc[exit_decision_index + 1]
    if int(exit_row["session_day"]) != int(entry["session_day"]):
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="DATA_CENSORED", reason="CENSORED_FUTURE_COVERAGE_EXIT_CROSSES_SESSION",
            position_opened=True,
        )
    path = frame.iloc[path_indexes]
    point = instrument_spec(TARGET_MARKET).point_value
    exit_price = float(exit_row["open"])
    gross = side * (exit_price - fill) * point
    favorable = (
        (max(float(path["high"].max()), exit_price) - fill) * point
        if side > 0
        else (fill - min(float(path["low"].min()), exit_price)) * point
    )
    adverse = (
        (min(float(path["low"].min()), exit_price) - fill) * point
        if side > 0
        else (fill - max(float(path["high"].max()), exit_price)) * point
    )
    exit_trigger = pd.Timestamp(frame.iloc[exit_decision_index]["timestamp"])
    exit_available = exit_trigger + pd.Timedelta(minutes=1)
    if pd.Timestamp(exit_row["timestamp"]) != exit_available:
        return _noncomplete_decision(
            rule, control, decision_time, side, source_feature_hash, source_score,
            status="DATA_CENSORED", reason="CENSORED_FUTURE_COVERAGE_EXIT_FILL_GAP",
            position_opened=True,
        )
    event_core = {
        "rule_id": rule.rule_id,
        "control": control,
        "signal_time": (decision_time - pd.Timedelta(minutes=1)).isoformat(),
        "decision_time": decision_time.isoformat(),
        "order_submit_time": decision_time.isoformat(),
        "earliest_executable_time": pd.Timestamp(entry["timestamp"]).isoformat(),
        "fill_time": pd.Timestamp(entry["timestamp"]).isoformat(),
        "entry_time": pd.Timestamp(entry["timestamp"]).isoformat(),
        "exit_trigger_bar_time": exit_trigger.isoformat(),
        "exit_trigger_available_at": exit_available.isoformat(),
        "exit_decision_time": exit_available.isoformat(),
        "exit_order_submit_time": exit_available.isoformat(),
        "exit_earliest_executable_time": pd.Timestamp(exit_row["timestamp"]).isoformat(),
        "exit_time": pd.Timestamp(exit_row["timestamp"]).isoformat(),
        "side": int(side),
        "feature_hash": source_feature_hash,
    }
    return {
        "event_id": stable_hash(event_core)[:24],
        **event_core,
        "outcome_status": "EXECUTABLE_COMPLETE",
        "censor_reason": None,
        "position_opened": True,
        "decision_ns": int(decision_time.value),
        "entry_ns": int(pd.Timestamp(entry["timestamp"]).value),
        "exit_ns": int(pd.Timestamp(exit_row["timestamp"]).value),
        "session_day": int(entry["session_day"]),
        "block": _block(int(entry["session_day"])),
        "entry_price": fill,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "risk_unit_price": risk_unit,
        "stop_price": stop,
        "target_price": target_price,
        "gross_one_micro": float(gross),
        "favorable_one_micro": float(max(favorable, 0.0)),
        "adverse_one_micro": float(min(adverse, 0.0)),
        "normal_net_one_micro": float(gross - NORMAL_COST),
        "stressed_net_one_micro": float(gross - STRESSED_COST),
        "normal_cost_one_micro": NORMAL_COST,
        "stressed_cost_one_micro": STRESSED_COST,
        "stop_risk_one_micro": float(stop_distance * point + STRESSED_COST),
        "same_bar_exit_stop_first": same_bar,
        "session_compliant": True,
        "source_score": float(source_score),
    }


def _noncomplete_decision(
    rule: CLTermStructureRule,
    control: str,
    decision_time: pd.Timestamp,
    side: int,
    source_feature_hash: str,
    source_score: float,
    *,
    status: str,
    reason: str,
    position_opened: bool = False,
) -> dict[str, Any]:
    core = {
        "rule_id": rule.rule_id,
        "control": control,
        "signal_time": (decision_time - pd.Timedelta(minutes=1)).isoformat(),
        "decision_time": decision_time.isoformat(),
        "order_submit_time": decision_time.isoformat(),
        "side": int(side),
        "feature_hash": source_feature_hash,
        "outcome_status": status,
        "censor_reason": reason,
        "position_opened": bool(position_opened),
    }
    return {
        "event_id": stable_hash(core)[:24],
        **core,
        "decision_ns": int(decision_time.value),
        "session_day": _session_day_value(decision_time),
        "source_score": float(source_score),
    }


def _nonoverlapping_events(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Enforce the frozen one-position account contract for every control."""

    output: list[dict[str, Any]] = []
    last_exit = -1
    for row in sorted(
        rows, key=lambda value: (int(value["decision_ns"]), str(value["event_id"]))
    ):
        if int(row["decision_ns"]) <= last_exit:
            continue
        output.append(dict(row))
        last_exit = int(row["exit_ns"])
    return output


def _select_on_discovery(
    proposals: Sequence[Mapping[str, Any]],
    event_sets: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    card: Mapping[str, Any],
) -> list[dict[str, Any]]:
    lower, upper = _role_bounds(card, "DISCOVERY")
    scored: list[dict[str, Any]] = []
    for proposal in proposals:
        events = [
            row
            for row in event_sets[str(proposal["candidate_id"])]["PRIMARY"]
            if lower <= int(row["session_day"]) < upper
        ]
        scored.append(
            {
                **dict(proposal),
                "discovery_event_count": len(events),
                "discovery_normal_net_one_micro_usd": float(
                    sum(float(row["normal_net_one_micro"]) for row in events)
                ),
                "discovery_stressed_net_one_micro_usd": float(
                    sum(float(row["stressed_net_one_micro"]) for row in events)
                ),
            }
        )
    selected: list[dict[str, Any]] = []
    for mechanism in sorted({str(row["mechanism"]) for row in scored}):
        group = [row for row in scored if row["mechanism"] == mechanism]
        selected.append(
            max(
                group,
                key=lambda row: (
                    float(row["discovery_stressed_net_one_micro_usd"]),
                    float(row["discovery_normal_net_one_micro_usd"]),
                    int(row["discovery_event_count"]),
                    str(row["candidate_id"]),
                ),
            )
        )
    return sorted(selected, key=lambda row: str(row["candidate_id"]))[:MAX_SELECTED_RULES]


def _role_calendars(
    target: pd.DataFrame,
    front: pd.DataFrame,
    second: pd.DataFrame,
    card: Mapping[str, Any],
    session_contract: Mapping[str, Any],
) -> tuple[dict[str, tuple[int, ...]], dict[str, dict[str, Any]]]:
    decision = session_contract["decision_window_chicago"]
    warmup = int(session_contract["causal_warmup_minutes"])
    source_coverage_start = int(decision["start_minute_inclusive"]) - int(
        session_contract["source_warmup_price_observations"]
    )
    target_coverage_start = int(decision["start_minute_inclusive"]) - warmup
    if (
        source_coverage_start != SOURCE_REQUIRED_COVERAGE_START_MINUTE
        or target_coverage_start != TARGET_REQUIRED_COVERAGE_START_MINUTE
        or int(decision["end_minute_exclusive"]) != ELIGIBLE_SOURCE_END_MINUTE
    ):
        raise CLTermStructureEconomicError("session coverage contract drift")
    stream_gaps = {
        "target": _required_clock_gap_counts(
            target,
            start_minute=target_coverage_start,
            end_minute_exclusive=SESSION_FLATTEN_MINUTE + 1,
        ),
        "front": _required_clock_gap_counts(
            front,
            start_minute=source_coverage_start,
            end_minute_exclusive=int(decision["end_minute_exclusive"]),
        ),
        "second": _required_clock_gap_counts(
            second,
            start_minute=source_coverage_start,
            end_minute_exclusive=int(decision["end_minute_exclusive"]),
        ),
    }
    all_days = set(int(value) for value in target["session_day"])
    all_days.update(int(value) for value in front["session_day"])
    all_days.update(int(value) for value in second["session_day"])
    calendars: dict[str, tuple[int, ...]] = {}
    coverage: dict[str, dict[str, Any]] = {}
    for role in ROLES:
        lower, upper = _role_bounds(card, role)
        days = tuple(
            sorted(
                int(day)
                for day in all_days
                if lower <= int(day) < upper
            )
        )
        censored = tuple(
            day
            for day in days
            if any(int(values.get(day, 1)) > 0 for values in stream_gaps.values())
        )
        calendars[role] = days
        coverage[role] = {
            "calendar_day_count": len(days),
            "data_censored_days": list(censored),
            "data_censored_day_count": len(censored),
            "required_clock_windows_chicago": {
                "decision": "07:00<=minute<14:00",
                "front": "05:59<=minute<14:00",
                "second": "05:59<=minute<14:00",
                "target": "06:00<=minute<=15:10",
            },
            "session_decision_contract_hash": str(session_contract["contract_hash"]),
            "missing_required_minutes_by_stream": {
                stream: {
                    str(day): int(values.get(day, 1))
                    for day in days
                    if int(values.get(day, 1)) > 0
                }
                for stream, values in stream_gaps.items()
            },
        }
    return calendars, coverage


def _required_clock_gap_counts(
    frame: pd.DataFrame,
    *,
    start_minute: int,
    end_minute_exclusive: int,
) -> dict[int, int]:
    """Return exact required-minute gaps for every represented session day."""

    expected = set(range(int(start_minute), int(end_minute_exclusive)))
    gaps: dict[int, int] = {}
    for day, values in frame.groupby("session_day", sort=True):
        observed = set(
            int(value)
            for value in pd.to_numeric(values["local_minute"], errors="coerce").dropna()
            if int(start_minute) <= int(value) < int(end_minute_exclusive)
        )
        gaps[int(day)] = len(expected - observed)
    return gaps


def _evaluate_candidate(
    proposal: Mapping[str, Any],
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    signal_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    calendars: Mapping[str, Sequence[int]],
    coverage: Mapping[str, Mapping[str, Any]],
    account_rules: Mapping[str, Mapping[str, Any]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    common_censored_signal_days = tuple(
        sorted(
            {
                int(day)
                for rows in signal_sets.values()
                for day in _censored_signal_days(rows)
            }
        )
    )
    discovery_frontier: list[dict[str, Any]] = []
    for account_label in card["account_frontier"]["account_sizes"]:
        rule = dict(account_rules[str(account_label)])
        config = exact._account_config(rule)
        cap = int(rule["special_contract_caps"]["MCL"][str(account_label)])
        for risk_fraction in RISK_FRACTIONS:
            for horizon in HORIZONS:
                discovery_frontier.append(
                    _evaluate_cell(
                        event_sets["PRIMARY"],
                        calendar=calendars["DISCOVERY"],
                        censored_days=coverage["DISCOVERY"]["data_censored_days"],
                        censored_signal_days=common_censored_signal_days,
                        config=config,
                        account_label=str(account_label),
                        micro_cap=cap,
                        risk_fraction=risk_fraction,
                        horizon=horizon,
                    )
                )
    best_cell = max(discovery_frontier, key=_cell_rank)
    frozen_account = str(best_cell["account_label"])
    frozen_risk = float(best_cell["risk_fraction"])
    rule = dict(account_rules[frozen_account])
    config = exact._account_config(rule)
    cap = int(rule["special_contract_caps"]["MCL"][frozen_account])
    evaluations: dict[str, dict[str, dict[str, Any]]] = {}
    account_episode_count = sum(
        int(row["normal"]["episode_count"]) + int(row["stressed"]["episode_count"])
        for row in discovery_frontier
    )
    account_cell_count = len(discovery_frontier)
    for role in ROLES:
        evaluations[role] = {}
        for control in CONTROLS:
            evaluations[role][control] = {}
            for horizon in HORIZONS:
                cell = _evaluate_cell(
                    event_sets[control],
                    calendar=calendars[role],
                    censored_days=coverage[role]["data_censored_days"],
                    censored_signal_days=common_censored_signal_days,
                    config=config,
                    account_label=frozen_account,
                    micro_cap=cap,
                    risk_fraction=frozen_risk,
                    horizon=horizon,
                )
                evaluations[role][control][str(horizon)] = cell
                account_episode_count += int(cell["normal"]["episode_count"]) + int(
                    cell["stressed"]["episode_count"]
                )
                account_cell_count += 1
    gate = _candidate_gate(
        proposal,
        event_sets,
        signal_sets,
        evaluations,
        card,
        selection_horizon=int(best_cell["horizon_trading_days"]),
    )
    return {
        "candidate_id": proposal["candidate_id"],
        "mechanism": proposal["mechanism"],
        "lookback_minutes": proposal["lookback_minutes"],
        "holding_minutes": proposal["holding_minutes"],
        "frozen_discovery_account_cell": {
            "account_label": frozen_account,
            "account_size_usd": int(rule["account_size_usd"]),
            "risk_fraction": frozen_risk,
            "selection_horizon_trading_days": int(best_cell["horizon_trading_days"]),
            "micro_contract_cap": cap,
        },
        "discovery_frontier": discovery_frontier,
        "evaluations": evaluations,
        "gate": gate,
        "evidence_tier": "E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
        "account_cell_count": account_cell_count,
        "account_episode_count": account_episode_count,
    }


def _evaluate_cell(
    events: Sequence[Mapping[str, Any]],
    *,
    calendar: Sequence[int],
    censored_days: Sequence[int],
    censored_signal_days: Sequence[int],
    config: Any,
    account_label: str,
    micro_cap: int,
    risk_fraction: float,
    horizon: int,
) -> dict[str, Any]:
    starts = non_overlapping_starts(calendar, (horizon,))[horizon]
    positions = {int(day): index for index, day in enumerate(calendar)}
    censored_set = set(int(value) for value in censored_days) | set(
        int(value) for value in censored_signal_days
    )
    full: list[tuple[int, str]] = []
    censored: list[tuple[int, str]] = []
    for start, label in starts:
        index = positions[int(start)]
        window = set(int(day) for day in calendar[index : index + horizon])
        (censored if window & censored_set else full).append((int(start), str(label)))
    summaries: dict[str, Any] = {}
    for scenario in SCENARIOS:
        episodes = [
            (
                _run_dynamic_episode(
                    events,
                    calendar,
                    start_day=start,
                    horizon=horizon,
                    config=config,
                    account_label=account_label,
                    micro_cap=micro_cap,
                    risk_fraction=risk_fraction,
                    scenario=scenario,
                ),
                _block(start),
            )
            for start, _label in full
        ]
        summaries[scenario] = _summarize(episodes, len(censored))
    return {
        "account_label": account_label,
        "account_size_usd": int(config.combine_starting_balance),
        "risk_fraction": risk_fraction,
        "micro_cap": micro_cap,
        "horizon_trading_days": horizon,
        "total_preregistered_start_count": len(starts),
        "full_coverage_start_count": len(full),
        "data_censored_start_count": len(censored),
        "normal": summaries["NORMAL"],
        "stressed": summaries["STRESSED"],
    }


def _run_dynamic_episode(
    events: Sequence[Mapping[str, Any]],
    calendar: Sequence[int],
    *,
    start_day: int,
    horizon: int,
    config: Any,
    account_label: str,
    micro_cap: int,
    risk_fraction: float,
    scenario: str,
) -> dict[str, Any]:
    days = tuple(int(value) for value in calendar)
    start_index = days.index(int(start_day))
    episode_days = days[start_index : start_index + int(horizon)]
    end_day = episode_days[-1]
    selected = sorted(
        (row for row in events if start_day <= int(row["session_day"]) <= end_day),
        key=lambda row: (int(row["session_day"]), int(row["decision_ns"]), str(row["event_id"])),
    )
    by_day: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for event in selected:
        by_day[int(event["session_day"])].append(event)
    balance = float(config.combine_starting_balance)
    floor = float(config.combine_starting_mll)
    minimum_buffer = balance - floor
    required_target = float(config.combine_profit_target)
    best_day = 0.0
    traded_days = 0
    event_count = 0
    daily_values: list[float] = []
    terminal = CombineTerminal.TIMEOUT.value
    reason = "maximum_evaluation_duration_reached"
    days_to_target: int | None = None
    consistency_ok = True
    quantities: list[int] = []
    for elapsed, day in enumerate(episode_days, start=1):
        day_pnl = 0.0
        day_traded = False
        for event in by_day.get(day, []):
            current_buffer = max(balance - floor, 0.0)
            risk_budget = risk_fraction * current_buffer
            per_micro = max(float(event["stop_risk_one_micro"]), 1e-12)
            quantity = min(int(math.floor(risk_budget / per_micro)), int(micro_cap))
            if quantity <= 0:
                continue
            cost = NORMAL_COST if scenario == "NORMAL" else STRESSED_COST
            gross = float(event["gross_one_micro"]) * quantity
            net = (float(event["gross_one_micro"]) - cost) * quantity
            best = max((float(event["favorable_one_micro"]) - cost / 2.0) * quantity, 0.0)
            worst = min((float(event["adverse_one_micro"]) - cost / 2.0) * quantity, 0.0)
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance + best,
                distance=float(config.combine_max_loss_limit),
                lock=float(config.combine_starting_balance),
                variant=config.resolved_mll_mode,
            )
            minimum_buffer = min(minimum_buffer, balance + worst - floor)
            if balance + worst <= floor:
                terminal = CombineTerminal.MLL_BREACH.value
                reason = "intraday_unrealized_mll_touch_or_breach"
                break
            balance += net
            day_pnl += net
            event_count += 1
            day_traded = True
            quantities.append(quantity)
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance,
                distance=float(config.combine_max_loss_limit),
                lock=float(config.combine_starting_balance),
                variant=config.resolved_mll_mode,
            )
            minimum_buffer = min(minimum_buffer, balance - floor)
            if balance <= floor:
                terminal = CombineTerminal.MLL_BREACH.value
                reason = "realized_mll_touch_or_breach"
                break
        daily_values.append(day_pnl)
        traded_days += int(day_traded)
        if terminal == CombineTerminal.MLL_BREACH.value:
            break
        total = balance - float(config.combine_starting_balance)
        best_day = max(best_day, day_pnl)
        if best_day > float(config.combine_profit_target) * float(
            config.consistency_best_day_max_pct_of_profit_target
        ):
            required_target = max(
                required_target,
                best_day / float(config.consistency_best_day_max_pct_of_profit_target),
            )
        concentration = best_day / total if total > 0.0 and best_day > 0.0 else 0.0
        consistency_ok = total <= 0.0 or concentration <= float(
            config.consistency_best_day_max_pct_of_profit_target
        ) + 1e-12
        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=float(config.combine_max_loss_limit),
            lock=float(config.combine_starting_balance),
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        if total >= required_target and consistency_ok and traded_days >= int(config.minimum_pass_days):
            terminal = CombineTerminal.PASSED.value
            reason = "target_consistency_and_minimum_days_satisfied"
            days_to_target = elapsed
            break
    net = balance - float(config.combine_starting_balance)
    concentration = best_day / net if net > 0 and best_day > 0 else 0.0
    return {
        "episode_start_day": int(start_day),
        "horizon_trading_days": int(horizon),
        "terminal": terminal,
        "terminal_reason": reason,
        "net_pnl": net,
        "target_progress": net / max(required_target, 1e-12),
        "minimum_mll_buffer": minimum_buffer,
        "mll_breached": terminal == CombineTerminal.MLL_BREACH.value,
        "consistency_ok": consistency_ok,
        "best_day_concentration": concentration,
        "days_to_target": days_to_target,
        "event_count": event_count,
        "traded_days": traded_days,
        "maximum_micro_quantity": max(quantities, default=0),
        "maximum_mini_equivalent": max(
            (mini_equivalent(TARGET_MARKET, value) for value in quantities), default=0.0
        ),
        "worst_day_loss": min(daily_values, default=0.0),
        "account_label": account_label,
    }


def _summarize(
    values: Sequence[tuple[Mapping[str, Any], str]], data_censored_count: int
) -> dict[str, Any]:
    episodes = [row for row, _block_id in values]
    passes = [row for row in episodes if row["terminal"] == CombineTerminal.PASSED.value]
    nets = [float(row["net_pnl"]) for row in episodes]
    progress = [float(row["target_progress"]) for row in episodes]
    pass_by_block = Counter(
        block for row, block in values if row["terminal"] == CombineTerminal.PASSED.value
    )
    return {
        "episode_count": len(episodes),
        "full_coverage_episode_count": len(episodes),
        "data_censored_count": int(data_censored_count),
        "pass_count": len(passes),
        "pass_start_days": sorted(
            int(row["episode_start_day"]) for row in passes
        ),
        "pass_rate": len(passes) / max(len(episodes), 1),
        "pass_count_by_block": dict(sorted(pass_by_block.items())),
        "blocks_with_passes": sorted(pass_by_block),
        "net_total_usd": float(sum(nets)),
        "net_median_usd": float(statistics.median(nets)) if nets else 0.0,
        "target_progress_median": float(statistics.median(progress)) if progress else 0.0,
        "target_progress_p25": float(np.percentile(progress, 25)) if progress else 0.0,
        "mll_breach_count": sum(bool(row["mll_breached"]) for row in episodes),
        "mll_breach_rate": sum(bool(row["mll_breached"]) for row in episodes) / max(len(episodes), 1),
        "minimum_mll_buffer_usd": min(
            (float(row["minimum_mll_buffer"]) for row in episodes), default=0.0
        ),
        "consistency_compliance_rate": sum(bool(row["consistency_ok"]) for row in episodes) / max(len(episodes), 1),
        "all_passing_paths_consistency_compliant": bool(passes) and all(
            bool(row["consistency_ok"]) for row in passes
        ),
        "median_days_to_target": statistics.median(
            [int(row["days_to_target"]) for row in passes]
        )
        if passes
        else None,
        "terminal_distribution": dict(
            sorted(Counter(str(row["terminal"]) for row in episodes).items())
        ),
    }


def _control_matching_audit(
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    signal_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    evaluations: Mapping[str, Mapping[str, Mapping[str, Mapping[str, Any]]]],
    *,
    holding_minutes: int,
) -> dict[str, Any]:
    """Prove controls use the same causal opportunity, exposure and start sets."""

    roles: dict[str, Any] = {}
    all_matched = True
    for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
        metrics: dict[str, Any] = {}
        for control in CONTROLS:
            decisions = [
                row
                for row in signal_sets[control]
                if _role_of_day(int(row["session_day"])) == role
            ]
            accepted = [
                row
                for row in event_sets[control]
                if _role_of_day(int(row["session_day"])) == role
            ]
            clock = Counter(
                pd.Timestamp(row["decision_time"])
                .tz_convert("America/Chicago")
                .strftime("%H:%M")
                for row in decisions
            )
            starts = {
                str(horizon): {
                    "total": int(
                        evaluations[role][control][str(horizon)][
                            "total_preregistered_start_count"
                        ]
                    ),
                    "full": int(
                        evaluations[role][control][str(horizon)][
                            "full_coverage_start_count"
                        ]
                    ),
                    "censored": int(
                        evaluations[role][control][str(horizon)][
                            "data_censored_start_count"
                        ]
                    ),
                }
                for horizon in HORIZONS
            }
            metrics[control] = {
                "decision_count": len(decisions),
                "decision_clock_histogram": dict(sorted(clock.items())),
                "accepted_event_count": len(accepted),
                "accepted_decision_times": [
                    str(row["decision_time"])
                    for row in sorted(
                        accepted,
                        key=lambda value: (
                            int(value["decision_ns"]),
                            str(value["event_id"]),
                        ),
                    )
                ],
                "ex_ante_open_stop_risk_one_micro_usd": float(
                    sum(float(row["stop_risk_one_micro"]) for row in accepted)
                ),
                "ex_ante_maximum_duty_minutes": len(accepted) * int(holding_minutes),
                "realized_holding_minutes": float(
                    sum(
                        (int(row["exit_ns"]) - int(row["entry_ns"])) / 60_000_000_000
                        for row in accepted
                    )
                ),
                "account_starts": starts,
            }
        primary = metrics["PRIMARY"]
        checks: dict[str, dict[str, bool]] = {}
        for control in CONTROLS[1:]:
            value = metrics[control]
            checks[control] = {
                "decision_count": value["decision_count"] == primary["decision_count"],
                "decision_clock": value["decision_clock_histogram"]
                == primary["decision_clock_histogram"],
                "accepted_path": value["accepted_decision_times"]
                == primary["accepted_decision_times"],
                "accepted_exposure": math.isclose(
                    float(value["ex_ante_open_stop_risk_one_micro_usd"]),
                    float(primary["ex_ante_open_stop_risk_one_micro_usd"]),
                    rel_tol=0.0,
                    abs_tol=1e-9,
                ),
                "ex_ante_duty_cycle": value["ex_ante_maximum_duty_minutes"]
                == primary["ex_ante_maximum_duty_minutes"],
                "account_start_denominators": value["account_starts"]
                == primary["account_starts"],
            }
            all_matched = all_matched and all(checks[control].values())
        roles[role] = {"metrics": metrics, "checks": checks}
    return {"passed": all_matched, "roles": roles}


def _candidate_gate(
    proposal: Mapping[str, Any],
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    signal_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    evaluations: Mapping[str, Mapping[str, Mapping[str, Mapping[str, Any]]]],
    card: Mapping[str, Any],
    *,
    selection_horizon: int,
) -> dict[str, Any]:
    gate = card["frozen_gate"]
    validation = evaluations["VALIDATION"]["PRIMARY"]
    final = evaluations["FINAL_DEVELOPMENT"]["PRIMARY"]
    validation_stressed_net = float(
        validation[str(HEADLINE_GATE_HORIZON)]["stressed"]["net_total_usd"]
    )
    final_stressed_net = float(
        final[str(HEADLINE_GATE_HORIZON)]["stressed"]["net_total_usd"]
    )
    # P10 pass trajectories are a subset of their corresponding P20 paths.
    # Use the single conservative P20 headline so one account path can never
    # satisfy the frozen minimum twice.
    final_normal_passes = int(
        final[str(HEADLINE_GATE_HORIZON)]["normal"]["pass_count"]
    )
    final_stressed_passes = int(
        final[str(HEADLINE_GATE_HORIZON)]["stressed"]["pass_count"]
    )
    final_stressed_cells = [final[str(h)]["stressed"] for h in HORIZONS]
    beats: dict[str, bool] = {}
    for control in CONTROLS[1:]:
        control_final = evaluations["FINAL_DEVELOPMENT"][control]
        primary_progress = float(
            final[str(HEADLINE_GATE_HORIZON)]["stressed"]["target_progress_median"]
        )
        control_progress = float(
            control_final[str(HEADLINE_GATE_HORIZON)]["stressed"]["target_progress_median"]
        )
        control_passes = int(
            control_final[str(HEADLINE_GATE_HORIZON)]["stressed"]["pass_count"]
        )
        beats[control] = (
            primary_progress - control_progress
            >= float(gate["minimum_median_target_progress_uplift_over_each_control"])
            or final_stressed_passes >= control_passes + 1
        )
    lower, upper = _role_bounds(card, "FINAL_DEVELOPMENT")
    final_events = [
        row
        for row in event_sets["PRIMARY"]
        if lower <= int(row["session_day"]) < upper
    ]
    positive = [max(float(row["stressed_net_one_micro"]), 0.0) for row in final_events]
    concentration = max(positive, default=0.0) / max(sum(positive), 1e-12)
    contexts: dict[str, float] = defaultdict(float)
    for row in final_events:
        contexts[str(row["block"])] += float(row["stressed_net_one_micro"])
    control_matching = _control_matching_audit(
        event_sets,
        signal_sets,
        evaluations,
        holding_minutes=int(proposal["holding_minutes"]),
    )
    checks = {
        "positive_validation_stressed": validation_stressed_net > 0.0,
        "positive_final_stressed": final_stressed_net > 0.0,
        "headline_p20_final_normal_passes": final_normal_passes
        >= int(gate["minimum_combined_final_normal_passes"]),
        "headline_p20_final_stressed_passes": final_stressed_passes
        >= int(gate["minimum_combined_final_stressed_passes"]),
        "positive_temporal_contexts": sum(value > 0.0 for value in contexts.values())
        >= int(gate["minimum_positive_temporal_contexts"]),
        "controlled_stressed_mll": max(
            float(row["mll_breach_rate"]) for row in final_stressed_cells
        )
        <= float(gate["maximum_stressed_mll_breach_rate"]),
        "passing_consistency": all(
            int(row["pass_count"]) == 0
            or bool(row["all_passing_paths_consistency_compliant"])
            for row in final_stressed_cells
        ),
        "nonnegative_final_p25": min(
            float(row["target_progress_p25"]) for row in final_stressed_cells
        )
        >= float(gate["minimum_final_stressed_target_progress_p25"]),
        "no_single_trade_domination": concentration
        <= float(gate["maximum_single_trade_profit_concentration"]),
        "beats_all_controls": all(beats.values()),
        "exposure_duty_path_and_start_matched_controls": bool(
            control_matching["passed"]
        ),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "control_beats": beats,
        "validation_stressed_net_usd": validation_stressed_net,
        "final_stressed_net_usd": final_stressed_net,
        "headline_final_normal_p20_passes": final_normal_passes,
        "headline_final_stressed_p20_passes": final_stressed_passes,
        "positive_final_temporal_context_count": sum(value > 0.0 for value in contexts.values()),
        "single_trade_profit_concentration": concentration,
        "control_matching_audit": control_matching,
        "headline_gate_horizon_trading_days": HEADLINE_GATE_HORIZON,
        "selection_horizon_trading_days": selection_horizon,
        "proposal": dict(proposal),
    }


def _power_preflight(
    selected: Sequence[Mapping[str, Any]],
    event_sets: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    thresholds = card["power_preflight"]["minimum_independent_events"]
    candidates: dict[str, Any] = {}
    passed = True
    for proposal in selected:
        candidate = str(proposal["candidate_id"])
        counts: dict[str, int] = {}
        checks: dict[str, bool] = {}
        for role in ROLES:
            lower, upper = _role_bounds(card, role)
            count = sum(
                lower <= int(row["session_day"]) < upper
                for row in event_sets[candidate]["PRIMARY"]
            )
            counts[role] = count
            checks[role] = count >= int(thresholds[role])
        candidates[candidate] = {"event_counts": counts, "checks": checks}
        passed = passed and all(checks.values())
    return {"passed": passed, "thresholds": thresholds, "candidates": candidates}


def _branch_gate(
    decisions: Sequence[Mapping[str, Any]],
    power: Mapping[str, Any],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(power["passed"]):
        return {
            "passed": False,
            "status": "TERM_STRUCTURE_TRIPWIRE_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
            "checks": {"power": False},
        }
    normal_passes = sum(
        int(row["gate"]["headline_final_normal_p20_passes"]) for row in decisions
    )
    stressed_passes = sum(
        int(row["gate"]["headline_final_stressed_p20_passes"]) for row in decisions
    )
    passing = [row for row in decisions if row["gate"]["passed"]]
    if passing:
        status = "TERM_STRUCTURE_TRIPWIRE_GREEN_TIER_E"
    elif any(float(row["gate"]["final_stressed_net_usd"]) > 0.0 for row in decisions):
        status = "TERM_STRUCTURE_TRIPWIRE_WEAK"
    else:
        status = "TERM_STRUCTURE_TRIPWIRE_FALSIFIED"
    return {
        "passed": status == "TERM_STRUCTURE_TRIPWIRE_GREEN_TIER_E",
        "status": status,
        "checks": {
            "power": True,
            "at_least_one_candidate_passed_all_frozen_gates": bool(passing),
        },
        "headline_final_normal_p20_passes": normal_passes,
        "headline_final_stressed_p20_passes": stressed_passes,
        "tier_e_candidate_ids": [str(row["candidate_id"]) for row in passing],
    }


def _cell_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        int(row["stressed"]["pass_count"]),
        int(row["normal"]["pass_count"]),
        float(row["stressed"]["target_progress_p25"]),
        float(row["stressed"]["target_progress_median"]),
        float(row["stressed"]["net_total_usd"]),
        -float(row["stressed"]["mll_breach_rate"]),
        -int(row["horizon_trading_days"]),
        -int(row["account_size_usd"]),
        -float(row["risk_fraction"]),
    )


def _session_fields(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    timestamp = pd.to_datetime(out["timestamp"], utc=True)
    local = timestamp.dt.tz_convert("America/Chicago")
    local_day = local.dt.normalize().dt.tz_localize(None)
    session_date = local_day + pd.to_timedelta((local.dt.hour >= 17).astype(int), unit="D")
    out["timestamp"] = timestamp
    out["session_day"] = session_date.dt.strftime("%Y%m%d").astype(int)
    out["session_id"] = session_date.dt.strftime("%Y-%m-%d")
    out["local_minute"] = local.dt.hour * 60 + local.dt.minute
    return out


def _session_day_value(timestamp: pd.Timestamp) -> int:
    value = timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")
    local = value.tz_convert("America/Chicago")
    day = local.normalize().tz_localize(None)
    if local.hour >= 17:
        day += pd.Timedelta(days=1)
    return int(day.strftime("%Y%m%d"))


def _instrument_by_utc_date(
    timestamps: pd.Series, intervals: Sequence[Mapping[str, Any]]
) -> pd.Series:
    dates = pd.to_datetime(timestamps, utc=True).dt.strftime("%Y-%m-%d")
    result = pd.Series(pd.NA, index=timestamps.index, dtype="string")
    for row in intervals:
        mask = dates.ge(str(row["d0"])) & dates.lt(str(row["d1"]))
        result.loc[mask] = str(row["s"])
    return result


def _true_session_guard_days(
    frame: pd.DataFrame,
    boundaries: set[str],
    all_days: Sequence[int],
    *,
    radius: int,
) -> set[int]:
    ordered = list(sorted(int(value) for value in all_days))
    positions = {day: index for index, day in enumerate(ordered)}
    utc_date = frame["timestamp"].dt.strftime("%Y-%m-%d")
    boundary_days = set(int(value) for value in frame.loc[utc_date.isin(boundaries), "session_day"])
    unsafe: set[int] = set()
    for day in boundary_days:
        index = positions.get(day)
        if index is None:
            continue
        unsafe.update(ordered[max(0, index - radius) : index + radius + 1])
    return unsafe


def _role_bounds(card: Mapping[str, Any], role: str) -> tuple[int, int]:
    row = next(value for value in card["chronological_roles"] if value["role"] == role)
    return int(str(row["start"]).replace("-", "")), int(str(row["end"]).replace("-", ""))


def _role_of_day(day: int) -> str | None:
    if 20230103 <= day < 20240122:
        return "DISCOVERY"
    if 20240122 <= day < 20240528:
        return "VALIDATION"
    if 20240528 <= day < 20241001:
        return "FINAL_DEVELOPMENT"
    return None


def _block(day: int) -> str:
    value = str(day)
    return f"{value[:4]}Q{(int(value[4:6]) - 1) // 3 + 1}"


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    path = path if path.is_absolute() else root / path
    resolved = path.resolve()
    if resolved != root and root not in resolved.parents:
        raise CLTermStructureEconomicError("path escapes project root")
    if not resolved.is_file():
        raise CLTermStructureEconomicError(f"required artifact unavailable: {resolved}")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CLTermStructureEconomicError(f"invalid JSON artifact: {path}") from exc


__all__ = [
    "CLTermStructureEconomicError",
    "audit_tripwire_inputs",
    "run_tripwire",
]
