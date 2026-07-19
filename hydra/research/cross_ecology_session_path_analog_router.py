"""Bounded causal cross-ecology session-path analog router.

The default entry point is metadata-only.  Economic Parquet rows are decoded
only after the exact root authorization token is supplied.  This module has no
mission, registry, cemetery, network, broker, or order writer.
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
import sqlite3
import subprocess
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from hydra.data.contract_mapping import load_roll_map
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.bundle import _validate_relational_contract
from hydra.evidence.schema import EVIDENCE_BUNDLE_CONTRACT, RECORD_SPECS, validate_identity
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import _apply_explicit_contract_map
from hydra.production import autonomous_exact_replay as exact
from hydra.propfirm.mll_variants import advance_end_of_day_floor


SCHEMA = "hydra_cross_ecology_session_path_analog_router_v1"
AUDIT_SCHEMA = "hydra_cross_ecology_session_path_analog_router_audit_v1"
CAMPAIGN_ID = "hydra_cross_ecology_session_path_analog_router_0036"
BRANCH_ID = "CAUSAL_CROSS_ECOLOGY_SESSION_PATH_ANALOG_ROUTER_V1"
DEFAULT_CARD = "config/research/cross_ecology_session_path_analog_router_v1.json"
RUN_AUTHORIZATION = "ROOT_AUTHORIZED_CROSS_ECOLOGY_SESSION_ANALOG_REPLAY_V1"
PRODUCTION_MANIFEST_SCHEMA = "hydra_economic_production_manifest_v1"
PRODUCTION_CAMPAIGN_MODE = "CROSS_ECOLOGY_SESSION_PATH_ANALOG_ROUTER"
CAMPAIGN_ORDINAL = 36
REQUIRED_PRODUCTION_ARTIFACTS = (
    DEFAULT_CARD,
    "hydra/research/cross_ecology_session_path_analog_router.py",
    "scripts/run_cross_ecology_session_path_analog_router.py",
    "tests/test_cross_ecology_session_path_analog_router.py",
)
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
PANELS = (
    "CROSS_ECOLOGY_PRICE_PATH",
    "CROSS_ECOLOGY_PRICE_VOLUME_PATH",
    "CROSS_ECOLOGY_RELATIVE_RANK_PATH",
)
PANEL_FEATURE_COUNTS = {
    "CROSS_ECOLOGY_PRICE_PATH": 36,
    "CROSS_ECOLOGY_PRICE_VOLUME_PATH": 54,
    "CROSS_ECOLOGY_RELATIVE_RANK_PATH": 25,
}
CLOCKS = ("08:35", "10:05")
MARKETS = ("MNQ", "MGC", "MCL", "M2K", "MYM")
CONTROLS = (
    "PRIMARY",
    "OWN_PATH_ONLY",
    "SESSION_MARKET_EXPOSURE_MATCHED_RANDOM",
    "ANALOG_LABEL_PERMUTATION",
    "DIRECTION_FLIP",
)
HORIZONS = (5, 10, 20)
RISK_FRACTIONS = (0.1, 0.2, 0.3)
MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
SESSION_TZ = "America/Chicago"
ZERO_SIDE_EFFECT_COUNTER_FIELDS = (
    "network_requests",
    "data_purchase_count",
    "q4_access_count_delta",
    "broker_connections",
    "orders",
    "mission_database_writes",
    "registry_writes",
    "cemetery_writes",
)


class SessionPathAnalogError(RuntimeError):
    """The bounded router cannot preserve its frozen contract."""


def _zero_side_effect_counters() -> dict[str, int]:
    """Return the explicit closed-world counters required by persistence."""

    return {field: 0 for field in ZERO_SIDE_EFFECT_COUNTER_FIELDS}


def _require_exact_zero_side_effect_counters(
    value: Mapping[str, Any], *, label: str
) -> None:
    for field in ZERO_SIDE_EFFECT_COUNTER_FIELDS:
        counter = value.get(field)
        if type(counter) is not int or counter != 0:
            raise SessionPathAnalogError(
                f"{label} must declare exact integer zero for {field}"
            )


def _closed_result_governance() -> dict[str, Any]:
    return {
        "incremental_data_spend_usd": 0.0,
        **_zero_side_effect_counters(),
        "tier_q_allowed": False,
        "promotion_allowed": False,
    }


@dataclass(frozen=True, slots=True)
class AnalogRule:
    rule_id: str
    panel: str
    decision_clock_local: str
    analog_k: int = 25
    lcb_threshold: float = 0.52

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def frozen_rule_specs() -> tuple[AnalogRule, ...]:
    rules = tuple(
        AnalogRule(
            rule_id=f"cross_ecology_analog_v1:{panel}:{clock.replace(':', '')}",
            panel=panel,
            decision_clock_local=clock,
        )
        for panel in PANELS
        for clock in CLOCKS
    )
    if len(rules) != 6 or len({stable_hash(row.to_dict()) for row in rules}) != 6:
        raise SessionPathAnalogError("frozen rule lattice must contain six unique rules")
    return rules


def load_decision_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = json.loads(Path(path).read_text(encoding="utf-8"))
    claimed = str(card.get("card_hash") or "")
    core = dict(card)
    core.pop("card_hash", None)
    if not claimed or stable_hash(core) != claimed:
        raise SessionPathAnalogError("decision-card self-hash drift")
    if card.get("selected_branch") != BRANCH_ID:
        raise SessionPathAnalogError("decision-card branch drift")
    if card.get("campaign_id") != CAMPAIGN_ID:
        raise SessionPathAnalogError("decision-card campaign identity drift")
    if card.get("governance", {}).get("tier_ceiling") != "E":
        raise SessionPathAnalogError("evidence ceiling drift")
    if card.get("governance", {}).get("tier_q_allowed") is not False:
        raise SessionPathAnalogError("Tier-Q must remain forbidden")
    _require_exact_zero_side_effect_counters(
        card.get("governance", {}), label="decision-card governance"
    )
    if stable_hash(card.get("frozen_inputs")) != str(
        card.get("frozen_input_contract_hash") or ""
    ):
        raise SessionPathAnalogError("frozen input-contract hash drift")
    if len(card["smallest_decisive_falsification_experiment"]["panels"]) != 3:
        raise SessionPathAnalogError("panel lattice drift")
    return card


def audit_inputs(
    root: str | Path,
    *,
    card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Hash and inspect metadata without decoding a single economic row."""

    project = Path(root).resolve()
    card_file = _inside(project, card_path)
    card = load_decision_card(card_file)
    parquet_bindings: list[dict[str, Any]] = []
    required = {"timestamp", "symbol", "open", "high", "low", "close", "volume", "session_id"}
    for row in card["frozen_inputs"]["market_files"]:
        binding = _audit_binding(project, row)
        metadata = pq.ParquetFile(project / binding["path"])
        columns = set(metadata.schema_arrow.names)
        if not required.issubset(columns):
            raise SessionPathAnalogError(
                f"Parquet schema missing {sorted(required - columns)}"
            )
        if int(metadata.metadata.num_rows) != int(row["record_count"]):
            raise SessionPathAnalogError("Parquet metadata row-count drift")
        parquet_bindings.append(
            {
                **binding,
                "record_count": int(metadata.metadata.num_rows),
                "row_group_count": int(metadata.metadata.num_row_groups),
                "symbols_used": list(row["symbols_used"]),
                "row_groups_decoded": 0,
            }
        )
    roll = _audit_binding(project, card["frozen_inputs"]["roll_map"])
    rules = _audit_binding(project, card["frozen_inputs"]["rule_snapshot"])
    access_ledger = _audit_binding(
        project, card["frozen_inputs"]["data_access_ledger"]
    )
    parsed_rules, rule_receipt = exact._load_rule_snapshot(project / rules["path"])
    if set(parsed_rules) != {"50K", "100K", "150K"}:
        raise SessionPathAnalogError("account-size inventory drift")
    if rule_receipt["parsed_rule_hash"] != card["frozen_inputs"]["rule_snapshot"]["parsed_rule_hash"]:
        raise SessionPathAnalogError("parsed rule hash drift")
    roll_map = load_roll_map(project / roll["path"])
    if roll_map.map_type != MAP_TYPE:
        raise SessionPathAnalogError("explicit roll-map type drift")
    cemetery = _audit_cemetery(project, card)
    future_scan = static_future_dependency_scan(Path(__file__).read_text(encoding="utf-8"))
    if not future_scan["passed"]:
        raise SessionPathAnalogError("reachable source contains a forbidden causal construct")
    core = {
        "schema": AUDIT_SCHEMA,
        "status": "PRECOMMIT_PENDING_PRODUCTION_MANIFEST_REQUIRED",
        "branch_id": BRANCH_ID,
        "decision_card_hash": card["card_hash"],
        "decision_card_file_sha256": _sha256(card_file),
        "market_bindings": parquet_bindings,
        "roll_map_binding": roll,
        "rule_snapshot_binding": rules,
        "data_access_ledger_binding": access_ledger,
        "source_commit_status": "PRECOMMIT_PENDING",
        "official_rule_snapshot": rule_receipt,
        "cemetery": cemetery,
        "future_dependency_scan": future_scan,
        "rule_count": len(frozen_rule_specs()),
        "maximum_selected_candidates": 2,
        "economic_rows_read": 0,
        "economic_outcomes_read": 0,
        "parquet_row_groups_decoded": 0,
        **_zero_side_effect_counters(),
        "tier_ceiling": "E",
        "tier_q_allowed": False,
        "promotion_allowed": False,
        "implementation_hashes": {
            "module_sha256": _sha256(Path(__file__).resolve()),
            "cli_sha256": _sha256(project / "scripts/run_cross_ecology_session_path_analog_router.py"),
            "tests_sha256": _sha256(project / "tests/test_cross_ecology_session_path_analog_router.py"),
        },
    }
    return {**core, "audit_hash": stable_hash(core)}


def static_future_dependency_scan(source: str) -> dict[str, Any]:
    """AST guard for forbidden future transforms in reachable Python source."""

    findings: list[dict[str, Any]] = []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return {"passed": False, "findings": [{"kind": "SYNTAX_ERROR", "line": exc.lineno}]}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            name = node.func.attr
            if name in {"bfill", "backfill"}:
                findings.append({"kind": "BACKFILL", "line": node.lineno})
            if name == "shift" and node.args and _negative_literal(node.args[0]):
                findings.append({"kind": "NEGATIVE_SHIFT", "line": node.lineno})
            if name == "rolling":
                for keyword in node.keywords:
                    if keyword.arg == "center" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True:
                        findings.append({"kind": "CENTERED_ROLLING", "line": node.lineno})
            if name == "roll" and len(node.args) >= 2 and _negative_literal(node.args[1]):
                findings.append({"kind": "NEGATIVE_ARRAY_ROLL", "line": node.lineno})
    return {"passed": not findings, "findings": findings}


def _negative_literal(node: ast.AST) -> bool:
    return bool(
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.USub)
        and isinstance(node.operand, ast.Constant)
        and isinstance(node.operand.value, (int, float))
    )


def prior_session_normalize(
    frame: pd.DataFrame,
    feature_columns: Sequence[str],
    *,
    prior_sessions: int = 20,
    minimum_sessions: int | None = None,
) -> pd.DataFrame:
    """Median/IQR normalize using prior rows only, grouped by market and clock."""

    minimum = prior_sessions if minimum_sessions is None else int(minimum_sessions)
    if prior_sessions < 1 or minimum < 1 or minimum > prior_sessions:
        raise SessionPathAnalogError("invalid prior-session normalization window")
    output = frame.copy()
    for column in feature_columns:
        output[f"z_{column}"] = np.nan
    for _, positions in output.groupby(["market", "decision_clock_local"], sort=True).groups.items():
        ordered = list(
            output.loc[list(positions)].sort_values("session_date", kind="mergesort").index
        )
        for offset, index in enumerate(ordered):
            prior = ordered[max(0, offset - prior_sessions) : offset]
            if len(prior) < minimum:
                continue
            history = output.loc[prior, list(feature_columns)].astype(float)
            median = history.median(axis=0)
            iqr = history.quantile(0.75) - history.quantile(0.25)
            scale = iqr.where(iqr.abs() > 1e-12, 1.0)
            current = output.loc[index, list(feature_columns)].astype(float)
            normalized = (current - median) / scale
            for column in feature_columns:
                output.at[index, f"z_{column}"] = float(normalized[column])
    return output


def assert_runtime_causality(
    *,
    available_at: pd.Timestamp,
    decision_time: pd.Timestamp,
    query_role: str,
    query_day: str,
    library_rows: Sequence[Mapping[str, Any]],
) -> None:
    if pd.Timestamp(available_at) > pd.Timestamp(decision_time):
        raise SessionPathAnalogError("feature availability exceeds decision time")
    for row in library_rows:
        if str(row.get("temporal_role")) != "DISCOVERY":
            raise SessionPathAnalogError("analog library contains non-Discovery outcome")
        if query_role == "DISCOVERY" and str(row.get("session_date")) >= str(query_day):
            raise SessionPathAnalogError("Discovery analog label is not strictly prior")


def next_open_fill(
    path: pd.DataFrame,
    *,
    decision_time: pd.Timestamp,
    direction: int,
    stop_distance: float,
    tick_size: float = 0.25,
    expected_contract: str | None = None,
    expected_session_date: str | None = None,
    expected_session_id: str | None = None,
    target_r_multiple: float = 2.0,
    maximum_holding_minutes: int = 120,
) -> dict[str, Any]:
    """Causal next-open entry with complete-path and boundary fail-closed guards."""

    if direction not in {-1, 1} or stop_distance <= 0.0 or tick_size <= 0.0:
        raise SessionPathAnalogError("invalid path request")
    ordered = path.sort_values("timestamp", kind="mergesort")
    future_rows = ordered.loc[pd.to_datetime(ordered["timestamp"], utc=True).ge(pd.Timestamp(decision_time))]
    if future_rows.empty:
        return {"status": "DATA_CENSORED", "censor_reason": "NO_NEXT_TRADABLE_BAR"}
    entry = future_rows.iloc[0]
    fill_time = pd.Timestamp(entry["timestamp"])
    if fill_time != pd.Timestamp(decision_time):
        return {"status": "DATA_CENSORED", "censor_reason": "MISSING_NEXT_TRADABLE_BAR"}
    if expected_contract is not None and str(entry.get("active_contract", "")) != expected_contract:
        return {"status": "DATA_CENSORED", "censor_reason": "ROLL_BOUNDARY_AT_ENTRY"}
    if expected_session_id is not None and str(entry.get("session_id", "")) != expected_session_id:
        return {"status": "DATA_CENSORED", "censor_reason": "SESSION_ID_BOUNDARY_AT_ENTRY"}
    actual_session_date = fill_time.tz_convert(SESSION_TZ).strftime("%Y-%m-%d")
    if expected_session_date is not None and actual_session_date != expected_session_date:
        return {"status": "DATA_CENSORED", "censor_reason": "SESSION_BOUNDARY_AT_ENTRY"}
    fill = _round_to_tick(float(entry["open"]), tick_size, mode="nearest")
    rounded_distance = math.ceil(float(stop_distance) / tick_size - 1e-12) * tick_size
    raw_stop = fill - direction * rounded_distance
    raw_target = fill + direction * rounded_distance * float(target_r_multiple)
    stop = _round_to_tick(raw_stop, tick_size, mode="floor" if direction > 0 else "ceil")
    target = _round_to_tick(raw_target, tick_size, mode="ceil" if direction > 0 else "floor")
    end = fill_time + pd.Timedelta(minutes=int(maximum_holding_minutes))
    traversed = future_rows.loc[pd.to_datetime(future_rows["timestamp"], utc=True).lt(end)]
    if traversed.empty:
        return {"status": "DATA_CENSORED", "censor_reason": "EMPTY_EXECUTION_PATH"}
    mfe = 0.0
    mae = 0.0
    exit_price: float | None = None
    exit_time: pd.Timestamp | None = None
    terminal: str | None = None
    expected_timestamp = fill_time
    for row in traversed.itertuples(index=False):
        row_timestamp = pd.Timestamp(row.timestamp)
        if row_timestamp != expected_timestamp:
            return {"status": "DATA_CENSORED", "censor_reason": "MISSING_INTERVAL_BEFORE_EXIT"}
        row_session_date = row_timestamp.tz_convert(SESSION_TZ).strftime("%Y-%m-%d")
        if expected_session_date is not None and row_session_date != expected_session_date:
            return {"status": "DATA_CENSORED", "censor_reason": "SESSION_BOUNDARY_BEFORE_EXIT"}
        if expected_contract is not None and str(getattr(row, "active_contract", "")) != expected_contract:
            return {"status": "DATA_CENSORED", "censor_reason": "ROLL_BOUNDARY_BEFORE_EXIT"}
        if expected_session_id is not None and str(getattr(row, "session_id", "")) != expected_session_id:
            return {"status": "DATA_CENSORED", "censor_reason": "SESSION_ID_BOUNDARY_BEFORE_EXIT"}
        expected_timestamp += pd.Timedelta(minutes=1)
        open_price = float(row.open)
        high = float(row.high)
        low = float(row.low)
        stop_gap = open_price <= stop if direction > 0 else open_price >= stop
        target_gap = open_price >= target if direction > 0 else open_price <= target
        if stop_gap:
            exit_price = _round_to_tick(open_price, tick_size, mode="nearest")
            exit_time = row_timestamp
            terminal = "STOP_GAP_ADVERSE_OPEN"
            gap_move = direction * (exit_price - fill)
            mfe = max(mfe, gap_move)
            mae = min(mae, gap_move)
            break
        if target_gap:
            exit_price = target
            exit_time = row_timestamp
            terminal = "TARGET_GAP_CONSERVATIVE"
            gap_move = direction * (target - fill)
            mfe = max(mfe, gap_move)
            mae = min(mae, gap_move)
            break
        favorable = (high - fill) if direction > 0 else (fill - low)
        adverse = (low - fill) if direction > 0 else (fill - high)
        mfe = max(mfe, favorable)
        mae = min(mae, adverse)
        stop_hit = low <= stop if direction > 0 else high >= stop
        target_hit = high >= target if direction > 0 else low <= target
        if stop_hit:
            exit_price = stop
            exit_time = row_timestamp + pd.Timedelta(minutes=1)
            terminal = "STOP_FIRST" if target_hit else "STOP"
            break
        if target_hit:
            exit_price = target
            exit_time = row_timestamp + pd.Timedelta(minutes=1)
            terminal = "TARGET"
            break
    if terminal is None:
        if len(traversed) != int(maximum_holding_minutes) or expected_timestamp != end:
            return {"status": "DATA_CENSORED", "censor_reason": "INCOMPLETE_FULL_HOLDING_PATH"}
        last = traversed.iloc[-1]
        exit_price = _round_to_tick(float(last["close"]), tick_size, mode="nearest")
        exit_time = pd.Timestamp(last["timestamp"]) + pd.Timedelta(minutes=1)
        terminal = "TIME_EXIT"
    assert exit_price is not None and exit_time is not None
    return {
        "status": "EXECUTABLE_COMPLETE",
        "fill_time": fill_time,
        "fill_price": fill,
        "exit_time": exit_time,
        "exit_price": exit_price,
        "stop_price": stop,
        "target_price": target,
        "terminal": terminal,
        "gross_points": float(direction * (exit_price - fill)),
        "mfe_points": float(max(0.0, mfe)),
        "mae_points": float(min(0.0, mae)),
        "target_first": terminal in {"TARGET", "TARGET_GAP_CONSERVATIVE"},
    }


def _round_to_tick(value: float, tick_size: float, *, mode: str) -> float:
    scaled = float(value) / float(tick_size)
    if mode == "floor":
        units = math.floor(scaled + 1e-12)
    elif mode == "ceil":
        units = math.ceil(scaled - 1e-12)
    elif mode == "nearest":
        units = math.floor(scaled + 0.5)
    else:
        raise SessionPathAnalogError("unknown tick-rounding mode")
    return float(units * float(tick_size))


def run_economic_tripwire(
    root: str | Path,
    *,
    authorization: str,
    card_path: str | Path = DEFAULT_CARD,
    production_manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run the six-rule development tripwire after exact root authorization."""

    if authorization != RUN_AUTHORIZATION:
        raise SessionPathAnalogError("exact root economic authorization is absent")
    project = Path(root).resolve()
    audit = audit_inputs(project, card_path=card_path)
    card = load_decision_card(_inside(project, card_path))
    if production_manifest_path is None:
        raise SessionPathAnalogError(
            "economic replay requires a validated production manifest"
        )
    production_manifest = _validate_production_manifest(
        project,
        production_manifest_path,
        card=card,
    )
    market_rows, load_audit = _load_market_rows(project, card)
    features, coverage = build_session_features(market_rows, card)
    account_rules, rule_receipt = exact._load_rule_snapshot(
        _inside(project, card["frozen_inputs"]["rule_snapshot"]["path"])
    )
    resolved_source_commit = str(production_manifest["source_commit"])
    decisions = []
    for rule in frozen_rule_specs():
        decisions.append(
            _evaluate_rule(
                rule,
                features=features,
                market_rows=market_rows,
                coverage=coverage,
                card=card,
                account_rules=account_rules,
                source_commit=resolved_source_commit,
                production_manifest_hash=str(
                    production_manifest["production_manifest_hash"]
                ),
            )
        )
    selected = _select_discovery_candidates(decisions)
    selected_ids = {row["candidate_id"] for row in selected}
    for row in decisions:
        row["selected_on_discovery"] = row["candidate_id"] in selected_ids
    power = _power_status(coverage, selected, card)
    gate = _branch_gate(selected, power, card)
    evidence_bundles = {
        str(row["candidate_id"]): row["evidence_bundle"] for row in decisions
    }
    materialized_fragments = [
        bundle["canonical_evidence_material"]
        for bundle in evidence_bundles.values()
        if bundle.get("canonical_evidence_material") is not None
    ]
    canonical_evidence_material = (
        _merge_canonical_evidence_materials(materialized_fragments)
        if materialized_fragments
        else None
    )
    decision_summaries: list[dict[str, Any]] = []
    for row in decisions:
        summary = dict(row)
        bundle = summary.pop("evidence_bundle")
        summary["evidence_bundle_hash"] = bundle["evidence_bundle_hash"]
        decision_summaries.append(summary)
    core = {
        "schema": SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "source_commit": resolved_source_commit,
        "production_manifest": production_manifest,
        "branch_id": BRANCH_ID,
        "status": gate["status"],
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "source_audit": audit,
        "load_audit": load_audit,
        "official_rule_snapshot": rule_receipt,
        "coverage": coverage,
        "rule_specs": [row.to_dict() for row in frozen_rule_specs()],
        "candidate_decisions": decision_summaries,
        "evidence_bundles": evidence_bundles,
        "canonical_evidence_material": canonical_evidence_material,
        "canonical_evidence_status": (
            "DEEP_RELATIONAL_PASS_ADOPTABLE_WITHOUT_REPLAY"
            if canonical_evidence_material is not None
            else "NO_GENUINELY_MATERIALIZED_POLICY_FRAGMENT"
        ),
        "evidence_bundle_hashes": {
            candidate_id: bundle["evidence_bundle_hash"]
            for candidate_id, bundle in evidence_bundles.items()
        },
        "discovery_selected_candidates": [row["candidate_id"] for row in selected],
        "power_preflight": power,
        "branch_gate": gate,
        "governance": _closed_result_governance(),
    }
    return {**core, "result_hash": stable_hash(core)}


def _load_market_rows(
    project: Path, card: Mapping[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    pieces: list[pd.DataFrame] = []
    rows_decoded = 0
    for binding in card["frozen_inputs"]["market_files"]:
        wanted = list(binding["symbols_used"])
        frame = pd.read_parquet(
            _inside(project, binding["path"]),
            columns=["timestamp", "symbol", "open", "high", "low", "close", "volume", "session_id"],
            filters=[("symbol", "in", wanted)],
        )
        rows_decoded += len(frame)
        pieces.append(frame)
    raw = pd.concat(pieces, ignore_index=True)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
    raw = (
        raw.loc[raw["symbol"].isin(MARKETS)]
        .drop_duplicates(["symbol", "timestamp"], keep="first")
        .sort_values(["symbol", "timestamp"], kind="mergesort")
        .reset_index(drop=True)
    )
    roll_map = load_roll_map(
        _inside(project, card["frozen_inputs"]["roll_map"]["path"])
    )
    mapped, map_proof = _apply_explicit_contract_map(raw, roll_map, required_map_type=MAP_TYPE)
    if set(MARKETS) - set(mapped["symbol"].astype(str)):
        raise SessionPathAnalogError("roll guards removed a required market")
    mapped["timestamp"] = pd.to_datetime(mapped["timestamp"], utc=True)
    mapped = mapped.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)
    core = {
        "parquet_rows_decoded": int(rows_decoded),
        "deduplicated_continuous_rows": int(len(raw)),
        "mapped_rows": int(len(mapped)),
        "mapped_rows_by_market": {
            market: int((mapped["symbol"] == market).sum()) for market in MARKETS
        },
        "explicit_roll_map": map_proof,
        "data_start": str(mapped["timestamp"].min()),
        "data_end": str(mapped["timestamp"].max()),
    }
    return mapped, {**core, "load_audit_hash": stable_hash(core)}


def build_session_features(
    frame: pd.DataFrame, card: Mapping[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build causal two-clock joint path features and prior-only normalization."""

    required = {"timestamp", "symbol", "open", "high", "low", "close", "volume", "active_contract"}
    if not required.issubset(frame.columns):
        raise SessionPathAnalogError(f"market frame missing {sorted(required - set(frame.columns))}")
    source = frame.copy()
    source["timestamp"] = pd.to_datetime(source["timestamp"], utc=True)
    source["local_timestamp"] = source["timestamp"].dt.tz_convert(SESSION_TZ)
    source["session_date"] = source["local_timestamp"].dt.strftime("%Y-%m-%d")
    source["local_minute"] = source["local_timestamp"].dt.hour * 60 + source["local_timestamp"].dt.minute
    coverage_window = source.loc[source["local_minute"].between(7 * 60, 14 * 60)]
    counts = coverage_window.groupby(["session_date", "symbol"], sort=True).size().unstack(fill_value=0)
    threshold = int(card["causal_contract"]["minimum_common_session_rows_07_00_TO_14_00"])
    source_union_days = tuple(str(day) for day in counts.index)
    economic_union_days = tuple(
        day for day in source_union_days if _role_for_day(day, card) in ROLES
    )
    common_source_days = tuple(
        str(day)
        for day, row in counts.reindex(columns=MARKETS, fill_value=0).iterrows()
        if bool(row.ge(threshold).all())
    )
    if not common_source_days:
        raise SessionPathAnalogError("no common complete session survives preflight")
    by_market = {
        market: source.loc[source["symbol"].eq(market)].set_index("timestamp", drop=False).sort_index()
        for market in MARKETS
    }
    close_1509: dict[str, dict[str, float]] = {}
    market_session_days: dict[str, tuple[str, ...]] = {}
    for market, rows in by_market.items():
        market_session_days[market] = tuple(sorted(set(rows["session_date"].astype(str))))
        close_1509[market] = {
            str(row.session_date): float(row.close)
            for row in rows.loc[rows["local_minute"].eq(15 * 60 + 9)].itertuples(index=False)
        }
    records: list[dict[str, Any]] = []
    rejected_sessions: Counter[str] = Counter()
    valid_clock_sessions: dict[str, set[str]] = {clock: set() for clock in CLOCKS}
    clock_censor_reasons: dict[tuple[str, str], set[str]] = {}
    for day in common_source_days:
        for clock in CLOCKS:
            decision_local = pd.Timestamp(f"{day} {clock}", tz=SESSION_TZ)
            decision_time = decision_local.tz_convert("UTC")
            market_raw: dict[str, dict[str, Any]] = {}
            valid = True
            for market, rows in by_market.items():
                prior_day = _actual_prior_session(day, market_session_days[market])
                window_start = decision_time - pd.Timedelta(minutes=60)
                window = rows.loc[rows["timestamp"].ge(window_start) & rows["timestamp"].lt(decision_time)]
                if len(window) != 60 or not window["timestamp"].diff().dropna().eq(pd.Timedelta(minutes=1)).all():
                    rejected_sessions["INCOMPLETE_PREDECISION_PATH"] += 1
                    clock_censor_reasons.setdefault((str(day), clock), set()).add(
                        f"{market}:INCOMPLETE_PREDECISION_PATH"
                    )
                    valid = False
                    break
                if prior_day is None or prior_day not in close_1509[market]:
                    rejected_sessions["MISSING_PRIOR_SESSION_ANCHOR"] += 1
                    clock_censor_reasons.setdefault((str(day), clock), set()).add(
                        f"{market}:MISSING_ACTUAL_PRIOR_SESSION_1509_ANCHOR"
                    )
                    valid = False
                    break
                current_open_rows = rows.loc[
                    rows["session_date"].eq(day) & rows["local_minute"].eq(7 * 60)
                ]
                if current_open_rows.empty:
                    rejected_sessions["MISSING_CURRENT_SESSION_OPEN"] += 1
                    clock_censor_reasons.setdefault((str(day), clock), set()).add(
                        f"{market}:MISSING_CURRENT_SESSION_OPEN"
                    )
                    valid = False
                    break
                price_bins = []
                for offset in range(12):
                    chunk = window.iloc[offset * 5 : (offset + 1) * 5]
                    price_bins.append(float(math.log(float(chunk.iloc[-1]["close"]) / float(chunk.iloc[0]["open"]))))
                volume_total = float(window["volume"].sum())
                volume_bins = [
                    float(window.iloc[offset * 10 : (offset + 1) * 10]["volume"].sum() / volume_total)
                    if volume_total > 0.0 else 0.0
                    for offset in range(6)
                ]
                pre30 = window.iloc[-30:]
                market_raw[market] = {
                    "price": price_bins,
                    "volume": volume_bins,
                    "overnight": float(
                        math.log(float(current_open_rows.iloc[0]["open"]) / close_1509[market][prior_day])
                    ),
                    "range30": float(pre30["high"].max() - pre30["low"].min()),
                    "available_at": window.iloc[-1]["timestamp"] + pd.Timedelta(minutes=1),
                    "own_path_return": float(sum(price_bins)),
                    "source_session_id": str(window.iloc[-1]["session_id"]),
                }
            if not valid:
                continue
            valid_clock_sessions[clock].add(str(day))
            matrix = np.asarray([market_raw[m]["price"] for m in MARKETS], dtype=float)
            vol_matrix = np.asarray([market_raw[m]["volume"] for m in MARKETS], dtype=float)
            overnights = np.asarray([market_raw[m]["overnight"] for m in MARKETS], dtype=float)
            for market_index, market in enumerate(MARKETS):
                own_price = matrix[market_index]
                peers = np.delete(matrix, market_index, axis=0)
                own_volume = vol_matrix[market_index]
                peer_volume = np.delete(vol_matrix, market_index, axis=0)
                price_values = np.concatenate(
                    [own_price, np.median(peers, axis=0), np.subtract(*np.percentile(peers, [75, 25], axis=0))]
                )
                price_volume_values = np.concatenate(
                    [price_values, own_volume, np.median(peer_volume, axis=0), np.subtract(*np.percentile(peer_volume, [75, 25], axis=0))]
                )
                ranks = np.asarray(
                    [float(pd.Series(matrix[:, column]).rank(method="average").iloc[market_index] / len(MARKETS)) for column in range(12)]
                )
                relative_values = np.concatenate(
                    [ranks, np.asarray([float(pd.Series(overnights).rank(method="average").iloc[market_index] / len(MARKETS))]), np.std(matrix, axis=0)]
                )
                base = {
                    "session_date": day,
                    "market": market,
                    "decision_clock_local": clock,
                    "decision_time": decision_time,
                    "available_at": market_raw[market]["available_at"],
                    "active_contract": str(
                        by_market[market].loc[by_market[market]["timestamp"].lt(decision_time)].iloc[-1]["active_contract"]
                    ),
                    "own_path_return": market_raw[market]["own_path_return"],
                    "raw_range30": market_raw[market]["range30"],
                    "overnight_displacement": market_raw[market]["overnight"],
                    "source_session_id": market_raw[market]["source_session_id"],
                    "temporal_role": _role_for_day(day, card),
                }
                for panel, values in (
                    (PANELS[0], price_values),
                    (PANELS[1], price_volume_values),
                    (PANELS[2], relative_values),
                ):
                    row = dict(base)
                    row["panel"] = panel
                    for feature_index, value in enumerate(values):
                        row[f"f{feature_index:02d}"] = float(value)
                    records.append(row)
    result = pd.DataFrame(records)
    if result.empty:
        raise SessionPathAnalogError("feature lattice is empty")
    normalized: list[pd.DataFrame] = []
    for panel, rows in result.groupby("panel", sort=True):
        columns = [f"f{index:02d}" for index in range(PANEL_FEATURE_COUNTS[str(panel)])]
        if any(column not in rows.columns for column in columns):
            raise SessionPathAnalogError(f"{panel} feature schema drift")
        featured = prior_session_normalize(rows, columns, prior_sessions=20)
        featured["feature_vector"] = featured.apply(
            lambda row: [float(row[f"z_{column}"]) for column in columns], axis=1
        )
        normalized.append(featured)
    result = pd.concat(normalized, ignore_index=True).sort_values(
        ["panel", "decision_clock_local", "session_date", "market"], kind="mergesort"
    ).reset_index(drop=True)
    stop_lookup: dict[tuple[str, str, str], float] = {}
    unique_ranges = (
        result[["session_date", "market", "decision_clock_local", "raw_range30"]]
        .drop_duplicates(["session_date", "market", "decision_clock_local"])
        .sort_values(["market", "decision_clock_local", "session_date"], kind="mergesort")
    )
    for (market, clock), rows in unique_ranges.groupby(
        ["market", "decision_clock_local"], sort=True
    ):
        history: list[float] = []
        for row in rows.itertuples(index=False):
            prior = history[-20:]
            tick = float(instrument_spec(str(market)).tick_size)
            raw_stop = (
                max(4.0 * tick, 0.5 * float(np.median(prior)))
                if len(prior) >= 20
                else float("nan")
            )
            stop_lookup[(str(row.session_date), str(market), str(clock))] = (
                math.ceil(raw_stop / tick - 1e-12) * tick
                if math.isfinite(raw_stop)
                else float("nan")
            )
            history.append(float(row.raw_range30))
    result["stop_distance"] = result.apply(
        lambda row: stop_lookup[
            (str(row["session_date"]), str(row["market"]), str(row["decision_clock_local"]))
        ],
        axis=1,
    )
    result["decision_eligible"] = result.apply(
        lambda row: bool(
            row["temporal_role"] in ROLES
            and pd.Timestamp(row["available_at"]) <= pd.Timestamp(row["decision_time"])
            and math.isfinite(float(row["stop_distance"]))
            and all(math.isfinite(float(value)) for value in row["feature_vector"])
        ),
        axis=1,
    )
    canonical_calendar: list[dict[str, Any]] = []
    full_set = {str(day) for day in common_source_days}
    for day in economic_union_days:
        row_counts = counts.reindex(columns=MARKETS, fill_value=0).loc[day]
        clock_coverage = {
            clock: bool(day in valid_clock_sessions[clock]) for clock in CLOCKS
        }
        reasons: list[str] = []
        if day not in full_set:
            reasons.extend(
                f"{market}:ROWS_{int(row_counts[market])}_BELOW_{threshold}"
                for market in MARKETS
                if int(row_counts[market]) < threshold
            )
        for clock in CLOCKS:
            reasons.extend(sorted(clock_censor_reasons.get((day, clock), set())))
        canonical_calendar.append(
            {
                "session_date": day,
                "temporal_role": _role_for_day(day, card),
                "raw_rows_by_market": {market: int(row_counts[market]) for market in MARKETS},
                "common_raw_coverage": day in full_set,
                "clock_full_coverage": clock_coverage,
                "coverage_status": "FULL_COVERAGE" if all(clock_coverage.values()) else "DATA_CENSORED",
                "censor_reasons": reasons,
            }
        )
    role_sessions = {
        role: sum(
            row["temporal_role"] == role and row["coverage_status"] == "FULL_COVERAGE"
            for row in canonical_calendar
        )
        for role in ROLES
    }
    canonical_role_sessions = {
        role: sum(row["temporal_role"] == role for row in canonical_calendar)
        for role in ROLES
    }
    core = {
        "common_complete_session_count": sum(
            day in full_set for day in economic_union_days
        ),
        "source_session_count_including_pre_role_warmup": len(source_union_days),
        "source_complete_session_count_including_pre_role_warmup": len(
            common_source_days
        ),
        "pre_role_source_session_count": sum(
            _role_for_day(day, card) == "OUTSIDE_ROLE" for day in source_union_days
        ),
        "pre_role_warmup_applied_before_economic_role_filter": True,
        "common_sessions_by_role": role_sessions,
        "canonical_sessions_by_role": canonical_role_sessions,
        "canonical_calendar": canonical_calendar,
        "canonical_calendar_hash": stable_hash(canonical_calendar),
        "feature_rows": len(result),
        "eligible_feature_rows": int(result["decision_eligible"].sum()),
        "rejected_sessions": dict(sorted(rejected_sessions.items())),
        "normalization_prior_sessions": 20,
        "future_outcomes_used_in_features": False,
    }
    return result, {**core, "coverage_hash": stable_hash(core)}


def _evaluate_rule(
    rule: AnalogRule,
    *,
    features: pd.DataFrame,
    market_rows: pd.DataFrame,
    coverage: Mapping[str, Any],
    card: Mapping[str, Any],
    account_rules: Mapping[str, Mapping[str, Any]],
    source_commit: str,
    production_manifest_hash: str,
) -> dict[str, Any]:
    rows = features.loc[
        features["panel"].eq(rule.panel)
        & features["decision_clock_local"].eq(rule.decision_clock_local)
        & features["decision_eligible"]
    ].copy()
    canonical_calendar = list(coverage["canonical_calendar"])
    role_calendars = {
        role: [
            str(row["session_date"])
            for row in canonical_calendar
            if str(row["temporal_role"]) == role
        ]
        for role in ROLES
    }
    full_coverage_days = {
        role: {
            str(row["session_date"])
            for row in canonical_calendar
            if str(row["temporal_role"]) == role
            and bool(row["clock_full_coverage"].get(rule.decision_clock_local, False))
        }
        for role in ROLES
    }
    outcome_cache: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    records: list[dict[str, Any]] = []
    for row in rows.to_dict("records"):
        enriched = dict(row)
        for direction in (-1, 1):
            key = (str(row["session_date"]), str(row["market"]), rule.decision_clock_local, direction)
            if key not in outcome_cache:
                outcome_cache[key] = _outcome_for_feature_row(
                    market_rows, row, direction=direction, card=card
                )
            enriched[f"outcome_{direction:+d}"] = outcome_cache[key]
        records.append(enriched)
    routed = route_analog_events(records, rule=rule)
    control_ledgers = _matched_control_ledgers(routed)
    frontier: list[dict[str, Any]] = []
    for account_label in ("50K", "100K", "150K"):
        for risk_fraction in RISK_FRACTIONS:
            frontier.append(
                _evaluate_account_cell(
                    control_ledgers,
                    account_label=account_label,
                    account_rule=account_rules[account_label],
                    risk_fraction=risk_fraction,
                    role_days=role_calendars,
                    full_coverage_days=full_coverage_days,
                )
            )
    discovery_choice = _choose_discovery_account_cell(frontier)
    routed_signal_role_counts = Counter(str(row["temporal_role"]) for row in routed)
    materialized_events = [
        row
        for row in routed
        if str(row["outcome"].get("status")) == "EXECUTABLE_COMPLETE"
    ]
    role_event_counts = Counter(
        str(row["temporal_role"]) for row in materialized_events
    )
    discovery_events = [
        row for row in materialized_events if row["temporal_role"] == "DISCOVERY"
    ]
    stressed_discovery_net = float(
        sum(float(row["outcome"]["gross_usd_per_micro"]) - float(row["stressed_cost_usd_per_micro"]) for row in discovery_events)
    )
    context_economics = _stressed_context_economics(routed)
    evidence_bundle = _build_candidate_evidence_bundle(
        rule=rule,
        routed=routed,
        control_ledgers=control_ledgers,
        selected_account_cell=discovery_choice,
        card=card,
        source_commit=source_commit,
        production_manifest_hash=production_manifest_hash,
    )
    compact_frontier = [_compact_account_cell(row) for row in frontier]
    compact_discovery_choice = (
        _compact_account_cell(discovery_choice) if discovery_choice is not None else None
    )
    core = {
        "candidate_id": rule.rule_id,
        "panel": rule.panel,
        "decision_clock_local": rule.decision_clock_local,
        "eligible_query_rows": len(records),
        "routed_event_count": len(routed),
        "materialized_economic_event_count": len(materialized_events),
        "future_outcome_censored_signal_count": len(routed) - len(materialized_events),
        "routed_signals_by_role": {
            role: int(routed_signal_role_counts[role]) for role in ROLES
        },
        "routed_events_by_role": {role: int(role_event_counts[role]) for role in ROLES},
        "routed_events_by_market": dict(sorted(Counter(str(row["market"]) for row in routed).items())),
        "discovery_stressed_net_per_one_micro_usd": stressed_discovery_net,
        "discovery_target_first_rate": float(
            np.mean([bool(row["outcome"]["target_first"]) for row in discovery_events])
        ) if discovery_events else 0.0,
        "stressed_context_economics_per_one_micro": context_economics,
        "account_frontier": compact_frontier,
        "discovery_selected_account_cell": compact_discovery_choice,
        "event_ledger_hash": stable_hash(_json_safe_events(routed)),
        "control_event_counts": {key: len(value) for key, value in control_ledgers.items()},
        "control_opportunity_identity_equal": len({len(value) for value in control_ledgers.values()}) <= 1,
        "evidence_bundle": evidence_bundle,
    }
    return {**core, "candidate_hash": stable_hash(core)}


def route_analog_events(
    records: Sequence[Mapping[str, Any]], *, rule: AnalogRule
) -> list[dict[str, Any]]:
    """Route at most one market/direction per session without current labels."""

    ordered = sorted(records, key=lambda row: (str(row["session_date"]), str(row["market"])))
    discovery = [row for row in ordered if str(row["temporal_role"]) == "DISCOVERY"]
    routed: list[dict[str, Any]] = []
    by_day: dict[str, list[Mapping[str, Any]]] = {}
    for row in ordered:
        by_day.setdefault(str(row["session_date"]), []).append(row)
    for day, queries in sorted(by_day.items()):
        choices: list[
            tuple[
                float,
                str,
                int,
                Mapping[str, Any],
                list[Mapping[str, Any]],
                list[Mapping[str, Any]],
                Mapping[str, Any],
            ]
        ] = []
        for query in queries:
            query_features = {
                key: value
                for key, value in query.items()
                if not str(key).startswith("outcome_")
            }
            query_outcomes = {
                "-1": query["outcome_-1"],
                "+1": query["outcome_+1"],
            }
            role = str(query["temporal_role"])
            target = str(query["market"])
            if role == "DISCOVERY":
                library = [
                    row for row in discovery
                    if str(row["market"]) == target and str(row["session_date"]) < day
                ]
            else:
                library = [row for row in discovery if str(row["market"]) == target]
            assert_runtime_causality(
                available_at=pd.Timestamp(query["available_at"]),
                decision_time=pd.Timestamp(query["decision_time"]),
                query_role=role,
                query_day=day,
                library_rows=library,
            )
            query_vector = np.asarray(query_features["feature_vector"], dtype=float)
            for direction in (-1, 1):
                labeled_library = [
                    row
                    for row in library
                    if row[f"outcome_{direction:+d}"].get("status")
                    == "EXECUTABLE_COMPLETE"
                ]
                if len(labeled_library) < rule.analog_k:
                    continue
                distances = [
                    float(
                        np.linalg.norm(
                            query_vector
                            - np.asarray(row["feature_vector"], dtype=float)
                        )
                    )
                    for row in labeled_library
                ]
                neighbors = [
                    labeled_library[index]
                    for index in np.argsort(
                        np.asarray(distances), kind="mergesort"
                    )[: rule.analog_k]
                ]
                successes = sum(bool(row[f"outcome_{direction:+d}"]["target_first"]) for row in neighbors)
                lcb = _wilson_lower_bound(successes, rule.analog_k)
                if lcb >= rule.lcb_threshold:
                    choices.append(
                        (
                            lcb,
                            target,
                            direction,
                            query_features,
                            neighbors,
                            library,
                            query_outcomes,
                        )
                    )
        if not choices:
            continue
        lcb, target, direction, query, neighbors, library, query_outcomes = max(
            choices,
            key=lambda item: (item[0], item[1], item[2]),
        )
        outcome = dict(query_outcomes[f"{direction:+d}"])
        routed.append(
            {
                "event_id": stable_hash(
                    {"rule": rule.rule_id, "day": day, "market": target, "direction": direction}
                )[:24],
                "candidate_id": rule.rule_id,
                "session_date": day,
                "temporal_role": str(query["temporal_role"]),
                "market": target,
                "contract": str(query["active_contract"]),
                "decision_clock_local": rule.decision_clock_local,
                "decision_time": query["decision_time"],
                "available_at": query["available_at"],
                "direction": direction,
                "own_path_return": float(query["own_path_return"]),
                "analog_lcb": float(lcb),
                "neighbor_count": len(neighbors),
                "library_role": "DISCOVERY_ONLY",
                "library_latest_day": max(str(row["session_date"]) for row in neighbors),
                "permuted_label_direction": _permuted_label_direction(
                    library,
                    query_id=f"{rule.rule_id}:{day}:{target}",
                    sample_size=rule.analog_k,
                ),
                "outcome": outcome,
                "outcome_status": str(outcome.get("status")),
                "economic_outcome_materialized": bool(
                    outcome.get("status") == "EXECUTABLE_COMPLETE"
                ),
                "opposite_outcome": dict(query_outcomes[f"{-direction:+d}"]),
                "normal_cost_usd_per_micro": float(outcome["normal_cost_usd_per_micro"]),
                "stressed_cost_usd_per_micro": float(outcome["stressed_cost_usd_per_micro"]),
            }
        )
    if len({(row["session_date"], row["candidate_id"]) for row in routed}) != len(routed):
        raise SessionPathAnalogError("router emitted more than one position per candidate-day")
    return routed


def _outcome_for_feature_row(
    market_rows: pd.DataFrame,
    row: Mapping[str, Any],
    *,
    direction: int,
    card: Mapping[str, Any],
) -> dict[str, Any]:
    market = str(row["market"])
    decision = pd.Timestamp(row["decision_time"])
    maximum = int(card["causal_contract"]["maximum_holding_minutes"])
    subset = market_rows.loc[
        market_rows["symbol"].eq(market)
        & market_rows["timestamp"].ge(decision)
        & market_rows["timestamp"].lt(decision + pd.Timedelta(minutes=maximum + 1))
    ]
    outcome = next_open_fill(
        subset,
        decision_time=decision,
        direction=direction,
        stop_distance=float(row["stop_distance"]),
        tick_size=float(instrument_spec(market).tick_size),
        expected_contract=str(row["active_contract"]),
        expected_session_date=str(row["session_date"]),
        expected_session_id=str(row["source_session_id"]),
        target_r_multiple=float(card["causal_contract"]["target_r_multiple"]),
        maximum_holding_minutes=maximum,
    )
    point_value = float(instrument_spec(market).point_value)
    normal_cost = float(card["causal_contract"]["normal_all_in_cost_per_micro_usd"][market])
    stressed_cost = normal_cost * float(card["causal_contract"]["stressed_cost_multiplier"])
    outcome.update(
        {
            "declared_stop_risk_usd_per_micro": float(
                row["stop_distance"] * point_value + stressed_cost
            ),
            "normal_cost_usd_per_micro": normal_cost,
            "stressed_cost_usd_per_micro": stressed_cost,
        }
    )
    if outcome.get("status") != "EXECUTABLE_COMPLETE":
        outcome["target_first"] = None
        outcome["economic_outcome_materialized"] = False
        return outcome
    outcome.update(
        {
            "gross_usd_per_micro": float(outcome["gross_points"] * point_value),
            "worst_usd_per_micro": float(outcome["mae_points"] * point_value - stressed_cost),
            "best_usd_per_micro": float(outcome["mfe_points"] * point_value - normal_cost),
            "economic_outcome_materialized": True,
        }
    )
    return outcome


def _matched_control_ledgers(
    primary: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    output = {control: [] for control in CONTROLS}
    for event in primary:
        original = int(event["direction"])
        own = 1 if float(event["own_path_return"]) >= 0.0 else -1
        random_direction = _deterministic_direction(str(event["event_id"]), "RANDOM")
        permuted_direction = int(event["permuted_label_direction"])
        directions = {
            "PRIMARY": original,
            "OWN_PATH_ONLY": own,
            "SESSION_MARKET_EXPOSURE_MATCHED_RANDOM": random_direction,
            "ANALOG_LABEL_PERMUTATION": permuted_direction,
            "DIRECTION_FLIP": -original,
        }
        for control, direction in directions.items():
            row = dict(event)
            row["control"] = control
            row["direction"] = direction
            row["outcome"] = dict(event["outcome"] if direction == original else event["opposite_outcome"])
            row["outcome_status"] = str(row["outcome"].get("status"))
            row["economic_outcome_materialized"] = bool(
                row["outcome"].get("status") == "EXECUTABLE_COMPLETE"
            )
            output[control].append(row)
    identities = {
        control: [(row["session_date"], row["market"], row["decision_time"]) for row in rows]
        for control, rows in output.items()
    }
    if any(value != identities["PRIMARY"] for key, value in identities.items() if key != "PRIMARY"):
        raise SessionPathAnalogError("matched controls changed the opportunity set")
    return output


def _deterministic_direction(event_id: str, salt: str) -> int:
    digest = hashlib.sha256(f"{salt}:{event_id}".encode("utf-8")).digest()
    return 1 if digest[0] & 1 else -1


def _permuted_label_direction(
    library: Sequence[Mapping[str, Any]], *, query_id: str, sample_size: int
) -> int:
    """Assign a deterministic Discovery-label permutation to analog positions."""

    complete_pairs = [
        row
        for row in library
        if row["outcome_+1"].get("status") == "EXECUTABLE_COMPLETE"
        and row["outcome_-1"].get("status") == "EXECUTABLE_COMPLETE"
    ]
    permuted = sorted(
        complete_pairs,
        key=lambda row: hashlib.sha256(
            f"ANALOG_LABEL_PERMUTATION:{query_id}:{row['session_date']}".encode("utf-8")
        ).hexdigest(),
    )[:sample_size]
    if not permuted:
        return _deterministic_direction(query_id, "PERMUTATION_NO_COMPLETE_PAIR")
    long_successes = sum(bool(row["outcome_+1"]["target_first"]) for row in permuted)
    short_successes = sum(bool(row["outcome_-1"]["target_first"]) for row in permuted)
    if long_successes == short_successes:
        return _deterministic_direction(query_id, "PERMUTATION_TIE")
    return 1 if long_successes > short_successes else -1


def _wilson_lower_bound(successes: int, observations: int, z: float = 1.0) -> float:
    if observations <= 0:
        return 0.0
    p = float(successes) / float(observations)
    denominator = 1.0 + z * z / observations
    center = p + z * z / (2.0 * observations)
    radius = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * observations)) / observations)
    return float((center - radius) / denominator)


def _evaluate_account_cell(
    ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    account_label: str,
    account_rule: Mapping[str, Any],
    risk_fraction: float,
    role_days: Mapping[str, Sequence[str]] | None = None,
    full_coverage_days: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    resolved_role_days = (
        {role: list(role_days.get(role, ())) for role in ROLES}
        if role_days is not None
        else {
            role: sorted(
            {
                str(row["session_date"])
                for row in ledgers["PRIMARY"]
                if str(row["temporal_role"]) == role
            }
            )
            for role in ROLES
        }
    )
    resolved_full_coverage = (
        {role: set(full_coverage_days.get(role, set())) for role in ROLES}
        if full_coverage_days is not None
        else {role: set(resolved_role_days[role]) for role in ROLES}
    )
    evaluations: dict[str, Any] = {
        control: {role: {} for role in ROLES} for control in CONTROLS
    }
    exposure_comparisons = 0
    outcome_censored_days = {
        str(row["session_date"])
        for control in CONTROLS
        for row in ledgers[control]
        if str(row["outcome"].get("status")) != "EXECUTABLE_COMPLETE"
    }
    for role in ROLES:
        days = resolved_role_days[role]
        role_ledgers = {
            control: [row for row in ledgers[control] if str(row["temporal_role"]) == role]
            for control in CONTROLS
        }
        for horizon in HORIZONS:
            starts = list(range(0, len(days) - horizon + 1, horizon))
            censored: list[dict[str, Any]] = []
            complete_windows: list[tuple[int, list[str]]] = []
            for position in starts:
                episode_days = list(days[position : position + horizon])
                incomplete = [
                    day
                    for day in episode_days
                    if day not in resolved_full_coverage[role]
                    or day in outcome_censored_days
                ]
                if incomplete:
                    censored.append(
                        {
                            "account_label": account_label,
                            "risk_fraction_of_current_mll_buffer": float(risk_fraction),
                            "temporal_role": role,
                            "start_day": episode_days[0],
                            "end_day": episode_days[-1],
                            "horizon_trading_days": horizon,
                            "status": "DATA_CENSORED",
                            "incomplete_session_days": incomplete,
                            "future_outcome_censored_days": [
                                day for day in episode_days if day in outcome_censored_days
                            ],
                        }
                    )
                else:
                    complete_windows.append((position, episode_days))
            scenario_ledgers: dict[str, dict[str, list[dict[str, Any]]]] = {
                scenario: {control: [] for control in CONTROLS}
                for scenario in ("NORMAL", "STRESSED_1_5X")
            }
            for scenario in ("NORMAL", "STRESSED_1_5X"):
                for _position, episode_days in complete_windows:
                    primary_episode = _replay_dynamic_account_episode(
                        role_ledgers["PRIMARY"],
                        episode_days=episode_days,
                        scenario=scenario,
                        account_rule=account_rule,
                        risk_fraction=risk_fraction,
                    )
                    primary_episode = _decorate_episode_context(
                        primary_episode,
                        control="PRIMARY",
                        role=role,
                        horizon=horizon,
                        scenario=scenario,
                        account_label=account_label,
                        risk_fraction=risk_fraction,
                    )
                    scenario_ledgers[scenario]["PRIMARY"].append(primary_episode)
                    frozen_quantities = dict(primary_episode["quantity_by_event"])
                    for control in CONTROLS[1:]:
                        control_episode = _replay_dynamic_account_episode(
                            role_ledgers[control],
                            episode_days=episode_days,
                            scenario=scenario,
                            account_rule=account_rule,
                            risk_fraction=risk_fraction,
                            frozen_quantities=frozen_quantities,
                        )
                        control_episode = _decorate_episode_context(
                            control_episode,
                            control=control,
                            role=role,
                            horizon=horizon,
                            scenario=scenario,
                            account_label=account_label,
                            risk_fraction=risk_fraction,
                        )
                        for event_id, quantity in control_episode["quantity_by_event"].items():
                            if int(quantity) != int(frozen_quantities.get(event_id, -1)):
                                raise SessionPathAnalogError(
                                    "matched control changed a primary decision-time quantity"
                                )
                            exposure_comparisons += 1
                        scenario_ledgers[scenario][control].append(control_episode)
            for control in CONTROLS:
                cell: dict[str, Any] = {
                    "all_start_count": len(starts),
                    "full_coverage_start_count": len(complete_windows),
                    "data_censored_start_count": len(censored),
                    "censored_start_ledger": [
                        {**row, "control": control} for row in censored
                    ],
                    "censored_start_ledger_hash": stable_hash(
                        [{**row, "control": control} for row in censored]
                    ),
                }
                for scenario in ("NORMAL", "STRESSED_1_5X"):
                    episodes = scenario_ledgers[scenario][control]
                    ledger_hash = stable_hash(episodes)
                    cell[scenario] = {
                        **_summarize_account_episodes(episodes),
                        "episode_ledger": episodes,
                        "episode_ledger_hash": ledger_hash,
                    }
                evaluations[control][role][str(horizon)] = cell
    core = {
        "account_label": account_label,
        "account_rule_snapshot": {
            key: account_rule[key]
            for key in (
                "account_size_usd",
                "profit_target_usd",
                "maximum_loss_limit_usd",
                "maximum_micro_contracts",
                "consistency_target_fraction",
                "minimum_trading_days",
            )
        },
        "risk_fraction_of_current_mll_buffer": float(risk_fraction),
        "evaluations": evaluations,
        "role_event_days": {role: len(days) for role, days in resolved_role_days.items()},
        "exact_mll": True,
        "exact_consistency": True,
        "dynamic_current_buffer_sizing": True,
        "control_quantity_policy": "PRIMARY_DECISION_TIME_QUANTITY_FROZEN_PER_SCENARIO_AND_START",
        "control_exposure_matched": True,
        "exposure_quantity_comparisons": exposure_comparisons,
        "future_outcome_censored_signal_days": sorted(outcome_censored_days),
    }
    return {**core, "cell_hash": stable_hash(core)}


def _replay_dynamic_account_episode(
    events: Sequence[Mapping[str, Any]],
    *,
    episode_days: Sequence[str],
    scenario: str,
    account_rule: Mapping[str, Any],
    risk_fraction: float,
    frozen_quantities: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    start_balance = float(account_rule["account_size_usd"])
    mll = float(account_rule["maximum_loss_limit_usd"])
    base_target = float(account_rule["profit_target_usd"])
    consistency_limit = float(account_rule["consistency_target_fraction"])
    floor = start_balance - mll
    balance = start_balance
    minimum_buffer = mll
    daily_path: list[dict[str, Any]] = []
    passing_day: int | None = None
    terminal = "TIMEOUT"
    consistency_ok = True
    maximum_quantity = 0
    selected_days = set(episode_days)
    by_day = {
        day: sorted(
            [row for row in events if str(row["session_date"]) == day],
            key=lambda row: (pd.Timestamp(row["decision_time"]), str(row["event_id"])),
        )
        for day in episode_days
    }
    positive_trade_pnls: list[float] = []
    market_profit: Counter[str] = Counter()
    quantity_by_event: dict[str, int] = {}
    quantity_ledger: list[dict[str, Any]] = []
    for elapsed, day in enumerate(episode_days, start=1):
        day_pnl = 0.0
        day_gross = 0.0
        day_costs = 0.0
        day_maximum_quantity = 0
        day_component_attribution: Counter[str] = Counter()
        for event in by_day.get(day, []):
            if str(event["outcome"].get("status")) != "EXECUTABLE_COMPLETE":
                raise SessionPathAnalogError(
                    "censored causal signal reached account replay instead of window censor"
                )
            event_id = str(event["event_id"])
            if frozen_quantities is None and (
                day_pnl <= -0.35 * mll or day_pnl >= 0.45 * base_target
            ):
                quantity_by_event[event_id] = 0
                quantity_ledger.append(
                    {
                        "event_id": event_id,
                        "session_date": day,
                        "quantity": 0,
                        "decision": "PRIMARY_DAILY_GUARD_REJECTED",
                    }
                )
                continue
            outcome = event["outcome"]
            market = str(event["market"])
            cost = float(
                event[
                    "normal_cost_usd_per_micro"
                    if scenario == "NORMAL"
                    else "stressed_cost_usd_per_micro"
                ]
            )
            stop_risk = float(outcome["declared_stop_risk_usd_per_micro"])
            current_buffer = max(0.0, balance - floor)
            if frozen_quantities is None:
                quantity = int(
                    math.floor(current_buffer * risk_fraction / max(stop_risk, 1e-12))
                )
                quantity = min(quantity, _micro_contract_cap(market, account_rule))
                quantity_decision = "PRIMARY_DYNAMIC_CURRENT_BUFFER"
            else:
                quantity = int(frozen_quantities.get(event_id, 0))
                quantity_decision = "MATCHED_PRIMARY_FROZEN_QUANTITY"
                if quantity > _micro_contract_cap(market, account_rule):
                    raise SessionPathAnalogError("frozen control quantity exceeds contract cap")
            quantity_by_event[event_id] = quantity
            quantity_ledger.append(
                {
                    "event_id": event_id,
                    "session_date": day,
                    "market": market,
                    "quantity": quantity,
                    "decision": quantity_decision,
                    "current_mll_buffer_usd": float(current_buffer),
                }
            )
            if quantity <= 0:
                continue
            maximum_quantity = max(maximum_quantity, quantity)
            day_maximum_quantity = max(day_maximum_quantity, quantity)
            point_value = float(instrument_spec(market).point_value)
            worst_unit = float(outcome["mae_points"] * point_value - cost)
            best_unit = float(outcome["mfe_points"] * point_value - cost)
            unit_net = float(outcome["gross_usd_per_micro"] - cost)
            trade_pnl = quantity * unit_net
            trade_gross = quantity * float(outcome["gross_usd_per_micro"])
            trade_costs = quantity * cost
            minimum_buffer = min(minimum_buffer, balance + quantity * worst_unit - floor)
            if balance + quantity * worst_unit <= floor:
                balance += trade_pnl
                day_pnl += trade_pnl
                day_gross += trade_gross
                day_costs += trade_costs
                day_component_attribution[str(event["candidate_id"])] += trade_pnl
                minimum_buffer = min(minimum_buffer, balance - floor)
                if trade_pnl > 0.0:
                    positive_trade_pnls.append(float(trade_pnl))
                    market_profit[market] += float(trade_pnl)
                terminal = "MLL_BREACHED"
                minimum_buffer = min(minimum_buffer, 0.0)
                break
            balance += trade_pnl
            day_pnl += trade_pnl
            day_gross += trade_gross
            day_costs += trade_costs
            day_component_attribution[str(event["candidate_id"])] += trade_pnl
            minimum_buffer = min(minimum_buffer, balance - floor)
            if trade_pnl > 0.0:
                positive_trade_pnls.append(float(trade_pnl))
                market_profit[market] += float(trade_pnl)
            if balance <= floor:
                terminal = "MLL_BREACHED"
                break
        total_profit = balance - start_balance
        positive_days_before = [
            float(row["day_pnl"]) for row in daily_path if float(row["day_pnl"]) > 0.0
        ]
        best_day = max([*positive_days_before, max(0.0, day_pnl)], default=0.0)
        required_target = max(
            base_target,
            best_day / consistency_limit if consistency_limit > 0.0 else math.inf,
        )
        consistency_value = best_day / total_profit if total_profit > 0.0 else 0.0
        consistency_ok = total_profit <= 0.0 or consistency_value <= consistency_limit + 1e-12
        daily_path.append(
            {
                "session_date": day,
                "day_pnl": float(day_pnl),
                "gross_pnl": float(day_gross),
                "costs": float(day_costs),
                "cumulative_costs": float(
                    sum(float(value["costs"]) for value in daily_path) + day_costs
                ),
                "realized_pnl": float(total_profit),
                "unrealized_pnl": 0.0,
                "balance": float(balance),
                "mll_floor": float(floor),
                "mll_buffer": float(balance - floor),
                "minimum_mll_buffer": float(minimum_buffer),
                "consistency": float(consistency_value),
                "consistency_ok": bool(consistency_ok),
                "target_progress": float(total_profit / required_target),
                "exposure": {"maximum_micro_contracts": float(day_maximum_quantity)},
                "component_attribution": dict(sorted(day_component_attribution.items())),
                "conflicts": [],
            }
        )
        if terminal == "MLL_BREACHED":
            break
        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=mll,
            lock=start_balance,
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        daily_path[-1]["mll_floor"] = float(floor)
        daily_path[-1]["mll_buffer"] = float(balance - floor)
        daily_path[-1]["minimum_mll_buffer"] = float(minimum_buffer)
        if (
            total_profit >= required_target
            and consistency_ok
            and elapsed >= int(account_rule["minimum_trading_days"])
        ):
            terminal = "TARGET_REACHED"
            passing_day = elapsed
            break
    net = balance - start_balance
    positive_days = [float(row["day_pnl"]) for row in daily_path if float(row["day_pnl"]) > 0.0]
    best_day = max(positive_days, default=0.0)
    required_target = max(base_target, best_day / consistency_limit if consistency_limit > 0.0 else math.inf)
    single_trade = max(positive_trade_pnls, default=0.0) / net if net > 0.0 else 0.0
    single_day = best_day / net if net > 0.0 else 0.0
    single_market = max(market_profit.values(), default=0.0) / net if net > 0.0 else 0.0
    return {
        "terminal": terminal,
        "passed": terminal == "TARGET_REACHED",
        "mll_breached": terminal == "MLL_BREACHED",
        "net_pnl_usd": float(net),
        "gross_pnl_usd": float(sum(float(row["gross_pnl"]) for row in daily_path)),
        "costs_usd": float(sum(float(row["costs"]) for row in daily_path)),
        "target_progress": float(net / required_target) if required_target > 0.0 else 0.0,
        "minimum_mll_buffer_usd": float(minimum_buffer),
        "consistency_ok": bool(consistency_ok),
        "days_to_target": passing_day,
        "maximum_micro_contracts": int(maximum_quantity),
        "single_trade_profit_concentration": float(single_trade),
        "single_day_profit_concentration": float(single_day),
        "single_market_profit_concentration": float(single_market),
        "eligible_session_days": len(selected_days),
        "ending_equity_usd": float(balance),
        "quantity_by_event": quantity_by_event,
        "quantity_ledger": quantity_ledger,
        "quantity_ledger_hash": stable_hash(quantity_ledger),
        "daily_path": daily_path,
        "daily_path_hash": stable_hash(daily_path),
        "matched_primary_quantities_used": frozen_quantities is not None,
    }


def _decorate_episode_context(
    episode: Mapping[str, Any],
    *,
    control: str,
    role: str,
    horizon: int,
    scenario: str,
    account_label: str,
    risk_fraction: float,
) -> dict[str, Any]:
    output = dict(episode)
    daily_path = [dict(row) for row in output["daily_path"]]
    start_day = str(daily_path[0]["session_date"]) if daily_path else "1970-01-01"
    end_day = str(daily_path[-1]["session_date"]) if daily_path else start_day
    episode_id = stable_hash(
        {
            "control": control,
            "role": role,
            "horizon": horizon,
            "account": account_label,
            "risk": risk_fraction,
            "start": start_day,
        }
    )[:24]
    terminal_mapping = {
        "TARGET_REACHED": "TARGET_REACHED",
        "MLL_BREACHED": "MLL_BREACHED",
        "TIMEOUT": "OPERATIONAL_HORIZON_NOT_REACHED",
    }
    output.update(
        {
            "episode_id": episode_id,
            "episode_start": f"{start_day}T00:00:00+00:00",
            "episode_end_day": end_day,
            "horizon": f"{int(horizon)}D",
            "horizon_trading_days": int(horizon),
            "temporal_block": role,
            "cost_scenario": scenario,
            "control": control,
            "account_label": account_label,
            "risk_fraction_of_current_mll_buffer": float(risk_fraction),
            "duration_trading_days": len(daily_path),
            "terminal_state": terminal_mapping[str(output["terminal"])],
            "censored_state": str(output["terminal"]) == "TIMEOUT",
            "failure_vector": {
                "MLL_EXCESS": 1.0 if bool(output["mll_breached"]) else 0.0,
                "INSUFFICIENT_TARGET_VELOCITY": 0.0 if bool(output["passed"]) else 1.0,
            },
            "daily_path": daily_path,
        }
    )
    output["episode_hash"] = stable_hash(output)
    return output


def _micro_contract_cap(market: str, rule: Mapping[str, Any]) -> int:
    cap = int(rule["maximum_micro_contracts"])
    special = dict(rule.get("special_contract_caps") or {})
    label = str(rule.get("account_label") or "")
    if market in {"MCL", "MGC"}:
        value = int(dict(special.get(market) or {}).get(label, cap))
        cap = min(cap, value)
    return max(0, cap)


def _summarize_account_episodes(episodes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not episodes:
        return {
            "episodes": 0,
            "passes": 0,
            "pass_rate": 0.0,
            "mll_breaches": 0,
            "mll_breach_rate": 0.0,
            "net_total_usd": 0.0,
            "target_progress_median": 0.0,
            "target_progress_p25": 0.0,
            "minimum_mll_buffer_usd": None,
            "all_passing_paths_consistency_compliant": False,
            "maximum_single_trade_profit_concentration": 0.0,
            "maximum_single_day_profit_concentration": 0.0,
            "maximum_single_market_profit_concentration": 0.0,
        }
    passed = [row for row in episodes if bool(row["passed"])]
    return {
        "episodes": len(episodes),
        "passes": len(passed),
        "pass_rate": float(len(passed) / len(episodes)),
        "mll_breaches": int(sum(bool(row["mll_breached"]) for row in episodes)),
        "mll_breach_rate": float(np.mean([bool(row["mll_breached"]) for row in episodes])),
        "net_total_usd": float(sum(float(row["net_pnl_usd"]) for row in episodes)),
        "target_progress_median": float(np.median([float(row["target_progress"]) for row in episodes])),
        "target_progress_p25": float(np.percentile([float(row["target_progress"]) for row in episodes], 25)),
        "minimum_mll_buffer_usd": float(min(float(row["minimum_mll_buffer_usd"]) for row in episodes)),
        "all_passing_paths_consistency_compliant": bool(passed) and all(bool(row["consistency_ok"]) for row in passed),
        "maximum_single_trade_profit_concentration": float(max(float(row["single_trade_profit_concentration"]) for row in episodes)),
        "maximum_single_day_profit_concentration": float(max(float(row["single_day_profit_concentration"]) for row in episodes)),
        "maximum_single_market_profit_concentration": float(max(float(row["single_market_profit_concentration"]) for row in episodes)),
        "terminal_distribution": dict(sorted(Counter(str(row["terminal"]) for row in episodes).items())),
        "median_days_to_target": float(np.median([int(row["days_to_target"]) for row in passed])) if passed else None,
    }


def _choose_discovery_account_cell(frontier: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    scored: list[tuple[tuple[float, ...], Mapping[str, Any]]] = []
    for cell in frontier:
        discovery = cell["evaluations"]["PRIMARY"]["DISCOVERY"]
        p10 = discovery["10"]["STRESSED_1_5X"]
        p20 = discovery["20"]["STRESSED_1_5X"]
        score = (
            float(p10["passes"] + p20["passes"]),
            -float(p20["mll_breach_rate"]),
            float(p10["target_progress_median"]),
            float(p20["net_total_usd"]),
            -float(cell["risk_fraction_of_current_mll_buffer"]),
        )
        scored.append((score, cell))
    if not scored:
        return None
    chosen = max(scored, key=lambda row: row[0])[1]
    return {
        "account_label": chosen["account_label"],
        "account_rule_snapshot": chosen["account_rule_snapshot"],
        "risk_fraction_of_current_mll_buffer": chosen["risk_fraction_of_current_mll_buffer"],
        "cell_hash": chosen["cell_hash"],
        "evaluations": chosen["evaluations"],
        "selection_role": "DISCOVERY_ONLY",
        "control_quantity_policy": chosen["control_quantity_policy"],
        "control_exposure_matched": chosen["control_exposure_matched"],
    }


def _compact_account_cell(cell: Mapping[str, Any]) -> dict[str, Any]:
    """Retain exact summaries while moving row material to the EvidenceBundle."""

    compact = dict(cell)
    evaluations: dict[str, Any] = {}
    for control, roles in dict(cell.get("evaluations") or {}).items():
        evaluations[control] = {}
        for role, horizons in dict(roles).items():
            evaluations[control][role] = {}
            for horizon, value in dict(horizons).items():
                horizon_row = dict(value)
                horizon_row.pop("censored_start_ledger", None)
                for scenario in ("NORMAL", "STRESSED_1_5X"):
                    scenario_row = dict(horizon_row.get(scenario) or {})
                    scenario_row.pop("episode_ledger", None)
                    horizon_row[scenario] = scenario_row
                evaluations[control][role][horizon] = horizon_row
    compact["evaluations"] = evaluations
    return compact


def _build_candidate_evidence_bundle(
    *,
    rule: AnalogRule,
    routed: Sequence[Mapping[str, Any]],
    control_ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    selected_account_cell: Mapping[str, Any] | None,
    card: Mapping[str, Any],
    source_commit: str,
    production_manifest_hash: str,
) -> dict[str, Any]:
    """Materialize adoption-grade rows in the same deterministic economic run."""

    event_ledgers = {
        control: _json_safe_events(rows) for control, rows in control_ledgers.items()
    }
    trade_ledgers: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for control, rows in control_ledgers.items():
        trade_ledgers[control] = {}
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            material: list[dict[str, Any]] = []
            for row in rows:
                outcome = dict(row["outcome"])
                if outcome.get("status") != "EXECUTABLE_COMPLETE":
                    continue
                cost = float(
                    row[
                        "normal_cost_usd_per_micro"
                        if scenario == "NORMAL"
                        else "stressed_cost_usd_per_micro"
                    ]
                )
                material.append(
                    {
                        "event_id": str(row["event_id"]),
                        "candidate_id": str(row["candidate_id"]),
                        "control": control,
                        "scenario": scenario,
                        "temporal_role": str(row["temporal_role"]),
                        "session_date": str(row["session_date"]),
                        "market": str(row["market"]),
                        "decision_time": str(row["decision_time"]),
                        "available_at": str(row["available_at"]),
                        "direction": int(row["direction"]),
                        "fill_time": str(outcome.get("fill_time")),
                        "fill_price": outcome.get("fill_price"),
                        "exit_time": str(outcome.get("exit_time")),
                        "exit_price": outcome.get("exit_price"),
                        "stop_price": outcome.get("stop_price"),
                        "target_price": outcome.get("target_price"),
                        "terminal": outcome.get("terminal"),
                        "quantity_basis": "ONE_MICRO_PRE_ACCOUNT_SIZING",
                        "gross_pnl_usd_per_micro": outcome.get("gross_usd_per_micro"),
                        "all_in_cost_usd_per_micro": cost,
                        "net_pnl_usd_per_micro": (
                            float(outcome["gross_usd_per_micro"]) - cost
                            if outcome.get("gross_usd_per_micro") is not None
                            else None
                        ),
                        "worst_excursion_points_through_actual_exit": outcome.get("mae_points"),
                        "best_excursion_points_through_actual_exit": outcome.get("mfe_points"),
                        "target_first": bool(outcome.get("target_first", False)),
                    }
                )
            trade_ledgers[control][scenario] = material
    policy_spec = {
        "campaign_id": CAMPAIGN_ID,
        "production_manifest_hash": production_manifest_hash,
        "rule": rule.to_dict(),
        "execution": card["causal_contract"],
        "account_frontier": card["account_frontier"],
        "controls": card["controls"],
        "frozen_gate": card["frozen_gate"],
        "evidence_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
    }
    account_material = (
        dict(selected_account_cell)
        if selected_account_cell is not None
        else {
            "status": "NO_DISCOVERY_ACCOUNT_CELL",
            "evaluations": {},
        }
    )
    materialization_reasons = _candidate_materialization_reasons(
        event_ledgers=event_ledgers,
        account_material=account_material,
    )
    canonical_material = (
        None
        if materialization_reasons
        else _canonical_evidence_material(
            rule=rule,
            event_ledgers=event_ledgers,
            account_material=account_material,
            card=card,
            source_commit=source_commit,
            production_manifest_hash=production_manifest_hash,
        )
    )
    ledger_hashes = {
        "events": {
            control: stable_hash(rows) for control, rows in event_ledgers.items()
        },
        "trades": {
            control: {
                scenario: stable_hash(rows) for scenario, rows in scenarios.items()
            }
            for control, scenarios in trade_ledgers.items()
        },
        "account_material": stable_hash(account_material),
    }
    core = {
        "schema": "hydra_compact_complete_evidence_bundle_v1",
        "campaign_id": CAMPAIGN_ID,
        "source_commit": source_commit,
        "candidate_id": rule.rule_id,
        "policy_specification": policy_spec,
        "policy_fingerprint": stable_hash(policy_spec),
        "provenance": {
            "campaign_id": CAMPAIGN_ID,
            "source_commit": source_commit,
            "production_manifest_hash": production_manifest_hash,
            "decision_card_hash": str(card["card_hash"]),
            "frozen_input_contract_hash": str(card["frozen_input_contract_hash"]),
            "chronological_roles": card["chronological_roles"],
            "analog_library_role": "DISCOVERY_ONLY",
            "candidate_selection_role": "DISCOVERY_ONLY",
            "economic_replay_count_required_for_adoption": 0,
        },
        "routed_event_ledgers": event_ledgers,
        "routed_trade_ledgers": trade_ledgers,
        "account_episode_material": account_material,
        "canonical_evidence_material": canonical_material,
        "ledger_hashes": ledger_hashes,
        "row_counts": {
            "routed_event_rows": sum(len(rows) for rows in event_ledgers.values()),
            "routed_trade_rows": sum(
                len(rows) for scenarios in trade_ledgers.values() for rows in scenarios.values()
            ),
            "controls": len(event_ledgers),
            "scenarios": 2,
            "canonical_rows": sum(
                len(rows)
                for rows in (
                    canonical_material["datasets"].values()
                    if canonical_material is not None
                    else ()
                )
            ),
        },
        "complete": canonical_material is not None,
        "materialization_status": (
            "DEEP_RELATIONAL_PASS_ADOPTABLE_WITHOUT_REPLAY"
            if canonical_material is not None
            else "NON_MATERIALIZED_FRAGMENT_EXCLUDED"
        ),
        "materialization_exclusion_reasons": materialization_reasons,
        "tier_ceiling": "E",
        "tier_q_allowed": False,
        "promotion_allowed": False,
    }
    return {**core, "evidence_bundle_hash": stable_hash(core)}


def _candidate_materialization_reasons(
    *,
    event_ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    account_material: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not account_material.get("evaluations"):
        reasons.append("NO_SELECTED_ACCOUNT_EPISODE_MATERIAL")
    for control in CONTROLS:
        complete_events = [
            row
            for row in event_ledgers.get(control, ())
            if str(row["outcome"].get("status")) == "EXECUTABLE_COMPLETE"
        ]
        if not complete_events:
            reasons.append(f"{control}:NO_MATERIALIZED_TRADE")
        episodes = []
        for horizons in dict(
            account_material.get("evaluations", {}).get(control, {})
        ).values():
            for cell in dict(horizons).values():
                for scenario in ("NORMAL", "STRESSED_1_5X"):
                    episodes.extend(dict(cell.get(scenario) or {}).get("episode_ledger", []))
        if not episodes:
            reasons.append(f"{control}:NO_FULL_COVERAGE_EPISODE")
    return sorted(set(reasons))


def _canonical_evidence_material(
    *,
    rule: AnalogRule,
    event_ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    account_material: Mapping[str, Any],
    card: Mapping[str, Any],
    source_commit: str,
    production_manifest_hash: str,
) -> dict[str, Any]:
    """Export rows directly consumable by a HYDRA_EVIDENCE_BUNDLE_V1 writer."""

    campaign_id = CAMPAIGN_ID
    component_ids = {
        control: f"cea_{stable_hash({'rule': rule.rule_id, 'control': control})[:24]}"
        for control in CONTROLS
    }
    policy_ids = {
        control: f"ceap_{stable_hash({'rule': rule.rule_id, 'control': control})[:24]}"
        for control in CONTROLS
    }
    datasets: dict[str, list[dict[str, Any]]] = {
        name: [] for name in RECORD_SPECS
    }
    for control in CONTROLS:
        component_id = component_ids[control]
        policy_id = policy_ids[control]
        datasets["account_policy_membership"].append(
            {
                "campaign_id": campaign_id,
                "policy_id": policy_id,
                "component_id": component_id,
                "risk_allocation": 1.0,
                "component_role": "CROSS_ECOLOGY_ANALOG_ROUTER",
            }
        )
        for event in event_ledgers[control]:
            outcome = dict(event["outcome"])
            signal_id = f"sig_{event['event_id']}_{control.lower()}"
            trade_id = f"trd_{event['event_id']}_{control.lower()}"
            side = "LONG" if int(event["direction"]) > 0 else "SHORT"
            datasets["component_signals"].append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "signal_id": signal_id,
                    "event_time": str(event["decision_time"]),
                    "market": str(event["market"]),
                    "contract": str(event["contract"]),
                    "timeframe": "1m",
                    "signal": int(event["direction"]),
                    "sizing": 1.0,
                    "stop": (
                        float(outcome["stop_price"])
                        if outcome.get("stop_price") is not None
                        else None
                    ),
                    "target": (
                        float(outcome["target_price"])
                        if outcome.get("target_price") is not None
                        else None
                    ),
                    "veto": False,
                    "component_role": "CROSS_ECOLOGY_ANALOG_ROUTER",
                    "outcome_status": str(outcome.get("status")),
                    "trade_materialized": bool(
                        outcome.get("status") == "EXECUTABLE_COMPLETE"
                    ),
                }
            )
            if outcome.get("status") != "EXECUTABLE_COMPLETE":
                continue
            datasets["component_entries"].append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "trade_id": trade_id,
                    "entry_time": str(outcome["fill_time"]),
                    "market": str(event["market"]),
                    "contract": str(event["contract"]),
                    "side": side,
                    "quantity": 1.0,
                    "entry_price": float(outcome["fill_price"]),
                    "sizing": 1.0,
                    "stop_price": float(outcome["stop_price"]),
                    "target_price": float(outcome["target_price"]),
                }
            )
            datasets["component_exits"].append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "trade_id": trade_id,
                    "exit_time": str(outcome["exit_time"]),
                    "exit_price": float(outcome["exit_price"]),
                    "exit_reason": str(outcome["terminal"]),
                }
            )
            normal_cost = float(event["normal_cost_usd_per_micro"])
            gross = float(outcome["gross_usd_per_micro"])
            datasets["component_trades"].append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "trade_id": trade_id,
                    "entry_time": str(outcome["fill_time"]),
                    "exit_time": str(outcome["exit_time"]),
                    "market": str(event["market"]),
                    "contract": str(event["contract"]),
                    "side": side,
                    "quantity": 1.0,
                    "entry_price": float(outcome["fill_price"]),
                    "exit_price": float(outcome["exit_price"]),
                    "gross_pnl": gross,
                    "costs": normal_cost,
                    "net_pnl": gross - normal_cost,
                }
            )
    evaluations = dict(account_material.get("evaluations") or {})
    censored_start_ledgers: list[dict[str, Any]] = []
    for control, roles in evaluations.items():
        policy_id = policy_ids[str(control)]
        component_id = component_ids[str(control)]
        for role, horizons in dict(roles).items():
            for horizon, cell in dict(horizons).items():
                for scenario in ("NORMAL", "STRESSED_1_5X"):
                    for episode in dict(cell[scenario]).get("episode_ledger", []):
                        episode_id = str(episode["episode_id"])
                        for day in episode["daily_path"]:
                            attribution = {
                                component_id: float(sum(dict(day["component_attribution"]).values()))
                            } if day["component_attribution"] else {}
                            datasets["account_daily_paths"].append(
                                {
                                    "campaign_id": campaign_id,
                                    "policy_id": policy_id,
                                    "episode_id": episode_id,
                                    "trading_day": str(day["session_date"]),
                                    "cost_scenario": scenario,
                                    "horizon": f"{int(horizon)}D",
                                    "realized_pnl": float(day["realized_pnl"]),
                                    "unrealized_pnl": float(day["unrealized_pnl"]),
                                    "daily_pnl": float(day["day_pnl"]),
                                    "equity": float(day["balance"]),
                                    "mll": float(day["mll_floor"]),
                                    "mll_buffer": float(day["mll_buffer"]),
                                    "minimum_mll_buffer": float(day["minimum_mll_buffer"]),
                                    "consistency": float(day["consistency"]),
                                    "target_progress": float(day["target_progress"]),
                                    "costs": float(day["costs"]),
                                    "conflicts": list(day["conflicts"]),
                                    "consistency_ok": bool(day["consistency_ok"]),
                                    "exposure": dict(day["exposure"]),
                                    "component_attribution": attribution,
                                }
                            )
                        datasets["episodes"].append(
                            {
                                "campaign_id": campaign_id,
                                "policy_id": policy_id,
                                "episode_id": episode_id,
                                "episode_start": str(episode["episode_start"]),
                                "horizon": f"{int(horizon)}D",
                                "temporal_block": str(role),
                                "duration_trading_days": int(episode["duration_trading_days"]),
                                "target_reached": bool(episode["passed"]),
                                "mll_breached": bool(episode["mll_breached"]),
                                "censored_state": bool(episode["censored_state"]),
                                "cost_scenario": scenario,
                                "costs": float(episode["costs_usd"]),
                                "net_pnl": float(episode["net_pnl_usd"]),
                                "target_progress": float(episode["target_progress"]),
                                "minimum_mll_buffer": float(episode["minimum_mll_buffer_usd"]),
                                "consistency_ok": bool(episode["consistency_ok"]),
                                "days_to_target": episode["days_to_target"],
                                "failure_vector": dict(episode["failure_vector"]),
                                "terminal_state": str(episode["terminal_state"]),
                            }
                        )
                for censored in cell.get("censored_start_ledger", []):
                    censored_start_ledgers.append(
                        {
                            **dict(censored),
                            "campaign_id": campaign_id,
                            "policy_id": policy_id,
                            "horizon": f"{int(horizon)}D",
                            "temporal_block": str(role),
                            "canonical_episode_exclusion_reason": (
                                "NO_DAILY_ACCOUNT_PATH_FOR_INCOMPLETE_SESSION_WINDOW"
                            ),
                        }
                    )
    data_fingerprints = {
        str(row["path"]): str(row["sha256"])
        for row in card["frozen_inputs"]["market_files"]
    }
    data_fingerprints[str(card["frozen_inputs"]["roll_map"]["path"])] = str(
        card["frozen_inputs"]["roll_map"]["sha256"]
    )
    data_fingerprints[str(card["frozen_inputs"]["rule_snapshot"]["path"])] = str(
        card["frozen_inputs"]["rule_snapshot"]["sha256"]
    )
    data_fingerprints[str(card["frozen_inputs"]["data_access_ledger"]["path"])] = str(
        card["frozen_inputs"]["data_access_ledger"]["sha256"]
    )
    immutable_checksums = {
        "configuration": str(card["card_hash"]),
        **{
            f"data:{name}": digest for name, digest in data_fingerprints.items()
        },
        "decision_card": str(card["card_hash"]),
        "frozen_input_contract": str(card["frozen_input_contract_hash"]),
        **{
            f"market_file_{index:02d}": str(row["sha256"])
            for index, row in enumerate(card["frozen_inputs"]["market_files"])
        },
        "roll_map": str(card["frozen_inputs"]["roll_map"]["sha256"]),
        "rule_snapshot": str(card["frozen_inputs"]["rule_snapshot"]["sha256"]),
        "data_access_ledger": str(
            card["frozen_inputs"]["data_access_ledger"]["sha256"]
        ),
        "production_manifest": production_manifest_hash,
    }
    datasets["provenance"].append(
        {
            "campaign_id": campaign_id,
            "validator_version": "cross_ecology_session_path_analog_router_v1",
            "replay_version": SCHEMA,
            "market_data_role": "DISCOVERY_VALIDATION_FINAL_DEVELOPMENT_PRE_Q4",
            "access_ledger_sha256": str(
                card["frozen_inputs"]["data_access_ledger"]["sha256"]
            ),
            "reconstruction_flag": False,
            "immutable_checksums": immutable_checksums,
            "recorded_at_utc": "2026-07-19T00:00:00+00:00",
        }
    )
    validated: dict[str, list[dict[str, Any]]] = {}
    for name, rows in datasets.items():
        validated[name] = [
            RECORD_SPECS[name].validate(row, campaign_id=campaign_id) for row in rows
        ]
    required_keys = sorted(
        {
            (row["policy_id"], row["episode_id"], row["horizon"])
            for row in validated["episodes"]
        }
    )
    policy_fingerprints = {
        policy_ids[control]: stable_hash(
            {"rule": rule.to_dict(), "control": control, "account": account_material.get("account_label")}
        )
        for control in CONTROLS
    }
    component_fingerprints = {
        component_ids[control]: stable_hash(
            {"rule": rule.to_dict(), "control": control, "component": True}
        )
        for control in CONTROLS
    }
    identity = {
        "campaign_id": campaign_id,
        "grammar_id": BRANCH_ID,
        "policy_fingerprints": policy_fingerprints,
        "component_fingerprints": component_fingerprints,
        "source_commit": source_commit,
        "data_fingerprints": data_fingerprints,
        "configuration_sha256": str(card["card_hash"]),
        "seeds": [25, 835, 1005],
        "created_at_utc": "2026-07-19T00:00:00+00:00",
        "expected_coverage": {
            "policy_ids": sorted(policy_fingerprints),
            "component_ids": sorted(component_fingerprints),
            "required_episode_keys": [
                {"policy_id": policy, "episode_id": episode, "horizon": horizon}
                for policy, episode, horizon in required_keys
            ],
            "allowed_horizons": ["5D", "10D", "20D"],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": False,
        },
    }
    identity = validate_identity(identity)
    core = {
        "contract": EVIDENCE_BUNDLE_CONTRACT,
        "schema_version": 1,
        "source_audit": _zero_side_effect_counters(),
        "governance": _zero_side_effect_counters(),
        "identity": identity,
        "datasets": validated,
        "dataset_hashes": {
            name: stable_hash(rows) for name, rows in validated.items()
        },
        "censored_source_ledger_preserved": True,
        "censored_start_ledgers": censored_start_ledgers,
        "censored_start_ledger_hash": stable_hash(censored_start_ledgers),
        "deep_relational_validation": "PASS",
        "adapter_requires_economic_replay": False,
    }
    if _validate_relational_contract(identity=identity, records=validated) is not False:
        raise SessionPathAnalogError(
            "fresh canonical fragment unexpectedly reports reconstruction evidence"
        )
    return {**core, "canonical_material_hash": stable_hash(core)}


def _merge_canonical_evidence_materials(
    materials: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Merge candidate fragments into one adoptable campaign EvidenceBundle."""

    if not materials:
        raise SessionPathAnalogError("canonical campaign evidence has no candidate material")
    for index, material in enumerate(materials):
        source_audit = material.get("source_audit")
        governance = material.get("governance")
        if not isinstance(source_audit, Mapping) or not isinstance(governance, Mapping):
            raise SessionPathAnalogError(
                f"canonical evidence fragment {index} lacks closed governance counters"
            )
        _require_exact_zero_side_effect_counters(
            source_audit, label=f"canonical fragment {index} source_audit"
        )
        _require_exact_zero_side_effect_counters(
            governance, label=f"canonical fragment {index} governance"
        )
    identities = [dict(material["identity"]) for material in materials]
    first = identities[0]
    invariant_fields = (
        "campaign_id",
        "grammar_id",
        "source_commit",
        "data_fingerprints",
        "configuration_sha256",
        "seeds",
        "created_at_utc",
    )
    for identity in identities[1:]:
        for field in invariant_fields:
            if identity[field] != first[field]:
                raise SessionPathAnalogError(
                    f"canonical evidence fragments disagree on {field}"
                )
    policy_fingerprints: dict[str, str] = {}
    component_fingerprints: dict[str, str] = {}
    required_episode_keys: list[dict[str, str]] = []
    datasets: dict[str, list[dict[str, Any]]] = {name: [] for name in RECORD_SPECS}
    censored_start_ledgers: list[dict[str, Any]] = []
    provenance_row: dict[str, Any] | None = None
    for material, identity in zip(materials, identities):
        for destination, source in (
            (policy_fingerprints, identity["policy_fingerprints"]),
            (component_fingerprints, identity["component_fingerprints"]),
        ):
            overlap = set(destination) & set(source)
            if overlap:
                raise SessionPathAnalogError(
                    "canonical evidence fragments reuse immutable IDs: "
                    + ", ".join(sorted(overlap))
                )
            destination.update({str(key): str(value) for key, value in source.items()})
        required_episode_keys.extend(
            dict(row) for row in identity["expected_coverage"]["required_episode_keys"]
        )
        for name, rows in dict(material["datasets"]).items():
            if name == "provenance":
                for row in rows:
                    if provenance_row is None:
                        provenance_row = dict(row)
                    elif dict(row) != provenance_row:
                        raise SessionPathAnalogError(
                            "canonical evidence fragments disagree on provenance"
                        )
                continue
            datasets[name].extend(dict(row) for row in rows)
        censored_start_ledgers.extend(
            dict(row) for row in material.get("censored_start_ledgers", [])
        )
    if provenance_row is None:
        raise SessionPathAnalogError("canonical evidence has no provenance row")
    datasets["provenance"] = [provenance_row]
    required_episode_keys = sorted(
        required_episode_keys,
        key=lambda row: (row["policy_id"], row["episode_id"], row["horizon"]),
    )
    merged_identity = validate_identity(
        {
            **first,
            "policy_fingerprints": dict(sorted(policy_fingerprints.items())),
            "component_fingerprints": dict(sorted(component_fingerprints.items())),
            "expected_coverage": {
                **first["expected_coverage"],
                "policy_ids": sorted(policy_fingerprints),
                "component_ids": sorted(component_fingerprints),
                "required_episode_keys": required_episode_keys,
            },
        }
    )
    validated = {
        name: [
            RECORD_SPECS[name].validate(row, campaign_id=CAMPAIGN_ID) for row in rows
        ]
        for name, rows in datasets.items()
    }
    core = {
        "contract": EVIDENCE_BUNDLE_CONTRACT,
        "schema_version": 1,
        "source_audit": _zero_side_effect_counters(),
        "governance": _zero_side_effect_counters(),
        "identity": merged_identity,
        "datasets": validated,
        "dataset_hashes": {
            name: stable_hash(rows) for name, rows in validated.items()
        },
        "candidate_fragment_hashes": [
            str(material["canonical_material_hash"]) for material in materials
        ],
        "censored_source_ledger_preserved": True,
        "censored_start_ledgers": censored_start_ledgers,
        "censored_start_ledger_hash": stable_hash(censored_start_ledgers),
        "deep_relational_validation": "PASS",
        "adapter_requires_economic_replay": False,
    }
    if (
        _validate_relational_contract(identity=merged_identity, records=validated)
        is not False
    ):
        raise SessionPathAnalogError(
            "fresh canonical campaign material unexpectedly reports reconstruction evidence"
        )
    return {**core, "canonical_material_hash": stable_hash(core)}


def _select_discovery_candidates(
    decisions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_clock: dict[str, list[Mapping[str, Any]]] = {}
    for row in decisions:
        by_clock.setdefault(str(row["decision_clock_local"]), []).append(row)
    winners: list[Mapping[str, Any]] = []
    for clock in CLOCKS:
        candidates = by_clock.get(clock, [])
        if not candidates:
            continue
        winners.append(
            max(
                candidates,
                key=lambda row: (
                    float(row["discovery_selected_account_cell"]["evaluations"]["PRIMARY"]["DISCOVERY"]["10"]["STRESSED_1_5X"]["passes"])
                    if row.get("discovery_selected_account_cell") else -1.0,
                    float(row["discovery_stressed_net_per_one_micro_usd"]),
                    float(row["discovery_target_first_rate"]),
                    str(row["candidate_id"]),
                ),
            )
        )
    return [dict(row) for row in winners[:2]]


def _power_status(
    coverage: Mapping[str, Any],
    selected: Sequence[Mapping[str, Any]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    session_minimum = card["power_preflight"]["minimum_common_sessions"]
    event_minimum = card["power_preflight"]["minimum_candidate_events"]
    session_checks = {
        role: int(coverage["common_sessions_by_role"].get(role, 0)) >= int(session_minimum[role])
        for role in ROLES
    }
    candidate_checks = {
        str(row["candidate_id"]): {
            role: int(row["routed_events_by_role"].get(role, 0)) >= int(event_minimum[role])
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        }
        for row in selected
    }
    passed = bool(selected) and all(session_checks.values()) and all(
        all(checks.values()) for checks in candidate_checks.values()
    )
    core = {
        "passed": passed,
        "session_checks": session_checks,
        "candidate_event_checks": candidate_checks,
        "underpowered_status": None if passed else "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
    }
    return {**core, "power_hash": stable_hash(core)}


def _branch_gate(
    selected: Sequence[Mapping[str, Any]],
    power: Mapping[str, Any],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(power["passed"]):
        return {
            "status": "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
            "passed_candidate_ids": [],
            "threshold_relaxation_allowed": False,
            "tier_ceiling": "E",
        }
    decisions: list[dict[str, Any]] = []
    for candidate in selected:
        cell = candidate.get("discovery_selected_account_cell")
        if not cell:
            continue
        evaluations = cell["evaluations"]
        primary = evaluations["PRIMARY"]
        validation10 = primary["VALIDATION"]["10"]
        validation20 = primary["VALIDATION"]["20"]
        final10 = primary["FINAL_DEVELOPMENT"]["10"]
        final20 = primary["FINAL_DEVELOPMENT"]["20"]
        val_stressed = validation20["STRESSED_1_5X"]
        final_stressed = final20["STRESSED_1_5X"]
        final_normal10 = final10["NORMAL"]
        checks: dict[str, bool] = {
            "positive_stressed_validation_net": float(val_stressed["net_total_usd"]) > 0.0,
            "positive_stressed_final_net": float(final_stressed["net_total_usd"]) > 0.0,
            "validation_stressed_p10_or_p20_pass": max(
                validation10["STRESSED_1_5X"]["passes"], validation20["STRESSED_1_5X"]["passes"]
            ) >= 1,
            "final_normal_p10_passes": int(final_normal10["passes"]) >= 2,
            "final_stressed_p10_or_p20_pass": max(
                final10["STRESSED_1_5X"]["passes"], final20["STRESSED_1_5X"]["passes"]
            ) >= 1,
            "controlled_mll": float(final_stressed["mll_breach_rate"]) <= 0.10,
            "passing_consistency": bool(final_stressed["all_passing_paths_consistency_compliant"]),
            "trade_concentration": float(final_stressed["maximum_single_trade_profit_concentration"]) <= 0.50,
            "day_concentration": float(final_stressed["maximum_single_day_profit_concentration"]) <= 0.50,
            "market_concentration": float(final_stressed["maximum_single_market_profit_concentration"]) <= 0.50,
            "multiple_positive_contexts": int(
                candidate["stressed_context_economics_per_one_micro"][
                    "positive_market_or_temporal_context_count"
                ]
            ) >= 2,
        }
        control_checks: dict[str, bool] = {}
        for control in CONTROLS[1:]:
            comparator = evaluations[control]["FINAL_DEVELOPMENT"]["20"]["STRESSED_1_5X"]
            progress_uplift = float(final_stressed["target_progress_median"]) - float(
                comparator["target_progress_median"]
            )
            extra_pass = int(final_stressed["passes"]) > int(comparator["passes"])
            control_checks[control] = bool(
                float(final_stressed["net_total_usd"]) > float(comparator["net_total_usd"])
                and (progress_uplift >= 0.10 or extra_pass)
            )
        checks["matched_controls"] = all(control_checks.values())
        decisions.append(
            {
                "candidate_id": candidate["candidate_id"],
                "passed": all(checks.values()),
                "checks": checks,
                "matched_control_checks": control_checks,
                "account_label": cell["account_label"],
                "risk_fraction": cell["risk_fraction_of_current_mll_buffer"],
            }
        )
    passed = [row["candidate_id"] for row in decisions if row["passed"]]
    return {
        "status": "SESSION_PATH_ANALOG_TIER_E_DIAGNOSTIC_GREEN" if passed else "SESSION_PATH_ANALOG_FALSIFIED",
        "candidate_gates": decisions,
        "passed_candidate_ids": passed,
        "threshold_relaxation_allowed": False,
        "tier_ceiling": "E",
        "tier_q_allowed": False,
        "promotion_allowed": False,
    }


def _role_for_day(day: str, card: Mapping[str, Any]) -> str:
    for row in card["chronological_roles"]:
        if str(row["start"]) <= day < str(row["end"]):
            return str(row["role"])
    return "OUTSIDE_ROLE"


def _actual_prior_session(day: str, market_session_days: Sequence[str]) -> str | None:
    """Return the immediately prior raw trading session, never a surviving subset."""

    prior = [str(value) for value in market_session_days if str(value) < str(day)]
    return prior[-1] if prior else None


def _stressed_context_economics(
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    development = [
        row
        for row in events
        if str(row["temporal_role"]) in {"VALIDATION", "FINAL_DEVELOPMENT"}
        and str(row["outcome"].get("status")) == "EXECUTABLE_COMPLETE"
    ]
    market_net: Counter[str] = Counter()
    for row in development:
        market_net[str(row["market"])] += float(row["outcome"]["gross_usd_per_micro"]) - float(
            row["stressed_cost_usd_per_micro"]
        )
    subblock_net: dict[str, float] = {}
    for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
        role_rows = sorted(
            [row for row in development if str(row["temporal_role"]) == role],
            key=lambda row: str(row["session_date"]),
        )
        unique_days = sorted({str(row["session_date"]) for row in role_rows})
        midpoint = len(unique_days) // 2
        halves = (set(unique_days[:midpoint]), set(unique_days[midpoint:]))
        for index, days in enumerate(halves, start=1):
            subblock_net[f"{role}_H{index}"] = float(
                sum(
                    float(row["outcome"]["gross_usd_per_micro"])
                    - float(row["stressed_cost_usd_per_micro"])
                    for row in role_rows
                    if str(row["session_date"]) in days
                )
            )
    positive_markets = sorted(market for market, value in market_net.items() if value > 0.0)
    positive_subblocks = sorted(key for key, value in subblock_net.items() if value > 0.0)
    return {
        "net_by_market_usd": dict(sorted(market_net.items())),
        "net_by_temporal_subblock_usd": subblock_net,
        "positive_markets": positive_markets,
        "positive_temporal_subblocks": positive_subblocks,
        "positive_market_or_temporal_context_count": max(
            len(positive_markets), len(positive_subblocks)
        ),
        "future_outcome_censored_signal_count": sum(
            str(row["temporal_role"]) in {"VALIDATION", "FINAL_DEVELOPMENT"}
            and str(row["outcome"].get("status")) != "EXECUTABLE_COMPLETE"
            for row in events
        ),
    }


def _json_safe_events(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [json.loads(json.dumps(dict(row), sort_keys=True, default=str, allow_nan=False)) for row in events]


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise SessionPathAnalogError("path escapes repository") from exc
    if not resolved.is_file():
        raise SessionPathAnalogError(f"required file missing: {resolved}")
    return resolved


def _validate_production_manifest(
    project: Path,
    manifest_path: str | Path,
    *,
    card: Mapping[str, Any],
) -> dict[str, Any]:
    """Require a manifest-bound committed implementation before any outcome read."""

    path = _inside(project, manifest_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    from hydra.production.cross_ecology_analog_manifest import (
        validate_cross_ecology_analog_manifest,
    )

    try:
        validate_cross_ecology_analog_manifest(manifest, manifest_path=path)
    except Exception as exc:
        raise SessionPathAnalogError(
            "authoritative production manifest validation failed"
        ) from exc
    claimed_binding = str(manifest.get("manifest_hash") or "")
    binding_core = dict(manifest)
    binding_core.pop("manifest_hash", None)
    if not claimed_binding or stable_hash(binding_core) != claimed_binding:
        raise SessionPathAnalogError("production manifest self-hash drift")
    if manifest.get("schema") != PRODUCTION_MANIFEST_SCHEMA:
        raise SessionPathAnalogError("production manifest schema drift")
    if manifest.get("campaign_id") != CAMPAIGN_ID:
        raise SessionPathAnalogError("production manifest campaign identity drift")
    if manifest.get("campaign_mode") != PRODUCTION_CAMPAIGN_MODE:
        raise SessionPathAnalogError("production manifest campaign mode drift")
    if int(manifest.get("campaign_ordinal", -1)) != CAMPAIGN_ORDINAL:
        raise SessionPathAnalogError("production manifest campaign ordinal drift")
    research_source = manifest.get("research_source")
    if not isinstance(research_source, Mapping):
        raise SessionPathAnalogError("production manifest research_source is absent")
    if (
        research_source.get("decision_card_path") != DEFAULT_CARD
        or research_source.get("decision_card_hash") != card["card_hash"]
        or research_source.get("frozen_input_contract_hash")
        != card["frozen_input_contract_hash"]
        or research_source.get("root_authorization") != RUN_AUTHORIZATION
        or int(research_source.get("maximum_economic_replays", -1)) != 1
    ):
        raise SessionPathAnalogError("production manifest decision-card drift")
    source_commit = str(manifest.get("source_commit") or "")
    if (
        len(source_commit) not in {40, 64}
        or any(value not in "0123456789abcdef" for value in source_commit)
    ):
        raise SessionPathAnalogError(
            "production manifest source_commit must be a full lowercase Git object ID"
        )
    exists = subprocess.run(
        ["git", "cat-file", "-e", f"{source_commit}^{{commit}}"],
        cwd=project,
        check=False,
        capture_output=True,
    )
    if exists.returncode != 0:
        raise SessionPathAnalogError("production source_commit is not a Git commit")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", source_commit, "HEAD"],
        cwd=project,
        check=False,
        capture_output=True,
    )
    if ancestor.returncode != 0:
        raise SessionPathAnalogError(
            "production source_commit is not an ancestor of the live checkout"
        )
    artifacts = manifest.get("implementation_files")
    if not isinstance(artifacts, Mapping):
        raise SessionPathAnalogError("production manifest implementation_files is absent")
    missing_artifacts = set(REQUIRED_PRODUCTION_ARTIFACTS) - set(artifacts)
    if missing_artifacts:
        raise SessionPathAnalogError(
            "production manifest lacks required artifacts: "
            + ", ".join(sorted(missing_artifacts))
        )
    for relative_raw, expected_raw in sorted(artifacts.items()):
        relative = str(relative_raw)
        expected = str(expected_raw or "")
        if len(expected) != 64 or any(value not in "0123456789abcdef" for value in expected):
            raise SessionPathAnalogError(
                f"production manifest lacks exact artifact hash: {relative}"
            )
        current = _sha256(_inside(project, relative))
        if current != expected:
            raise SessionPathAnalogError(
                f"working artifact differs from production manifest: {relative}"
            )
        committed = subprocess.run(
            ["git", "show", f"{source_commit}:{relative}"],
            cwd=project,
            check=False,
            capture_output=True,
        )
        if committed.returncode != 0 or hashlib.sha256(committed.stdout).hexdigest() != expected:
            raise SessionPathAnalogError(
                f"production artifact is not frozen in source_commit: {relative}"
            )
    reservation = manifest.get("multiplicity")
    if not isinstance(reservation, Mapping):
        raise SessionPathAnalogError("production manifest lacks multiplicity reservation")
    reservation_path = _inside(
        project, str(reservation.get("reservation_receipt_path") or "")
    )
    reservation_sha = str(reservation.get("reservation_receipt_sha256") or "")
    if _sha256(reservation_path) != reservation_sha:
        raise SessionPathAnalogError("multiplicity reservation hash drift")
    receipt = json.loads(reservation_path.read_text(encoding="utf-8"))
    if (
        receipt.get("campaign_id") != CAMPAIGN_ID
        or receipt.get("schema") != "hydra_manifest_campaign_multiplicity_v1"
        or int(receipt.get("reserved_delta_trials", -1))
        != int(reservation.get("reserved_delta_trials", -2))
    ):
        raise SessionPathAnalogError("multiplicity reservation receipt drift")
    production_manifest_hash = claimed_binding
    return {
        "schema": PRODUCTION_MANIFEST_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "campaign_ordinal": CAMPAIGN_ORDINAL,
        "path": str(path.relative_to(project)),
        "production_manifest_hash": production_manifest_hash,
        "manifest_file_sha256": _sha256(path),
        "source_commit": source_commit,
        "decision_card_hash": str(card["card_hash"]),
        "implementation_files": {
            str(relative): str(digest)
            for relative, digest in sorted(artifacts.items())
        },
        "multiplicity_reservation": {
            "path": str(reservation_path.relative_to(project)),
            "sha256": reservation_sha,
            "reserved_delta_trials": int(reservation["reserved_delta_trials"]),
        },
        "verified_against_committed_blobs": True,
        "source_commit_is_live_head_ancestor": True,
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _audit_binding(project: Path, row: Mapping[str, Any]) -> dict[str, Any]:
    path = _inside(project, str(row["path"]))
    actual_sha = _sha256(path)
    actual_size = path.stat().st_size
    if actual_sha != str(row["sha256"]):
        raise SessionPathAnalogError(f"input SHA drift: {row['path']}")
    if actual_size != int(row["size_bytes"]):
        raise SessionPathAnalogError(f"input size drift: {row['path']}")
    return {
        "path": str(path.relative_to(project)),
        "sha256": actual_sha,
        "size_bytes": actual_size,
    }


def _audit_cemetery(project: Path, card: Mapping[str, Any]) -> dict[str, Any]:
    expected = card["cemetery_audit"]
    path = _inside(project, expected["graveyard_path"])
    actual_sha = _sha256(path)
    if actual_sha != str(expected["graveyard_sha256_at_selection"]):
        raise SessionPathAnalogError("graveyard changed since branch selection")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        collision = int(
            connection.execute(
                "SELECT COUNT(*) FROM class_tombstones WHERE lower(mechanism_class)=lower(?)",
                (BRANCH_ID,),
            ).fetchone()[0]
        )
    finally:
        connection.close()
    if collision != 0:
        raise SessionPathAnalogError("exact cemetery collision")
    return {
        "path": str(path.relative_to(project)),
        "sha256": actual_sha,
        "exact_collision_count": collision,
        "writes": 0,
    }


__all__ = [
    "AnalogRule",
    "BRANCH_ID",
    "CAMPAIGN_ID",
    "DEFAULT_CARD",
    "RUN_AUTHORIZATION",
    "SessionPathAnalogError",
    "assert_runtime_causality",
    "audit_inputs",
    "build_session_features",
    "frozen_rule_specs",
    "load_decision_card",
    "next_open_fill",
    "prior_session_normalize",
    "route_analog_events",
    "run_economic_tripwire",
    "static_future_dependency_scan",
]
