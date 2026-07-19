"""Isolated Treasury-curve relative-value tripwire.

The module is intentionally disconnected from the persistent mission runtime.
It creates no manifest, registry write, controller hook, or promotion status.
Without a SHA-bound input contract it returns the fail-closed status
``AWAITING_MANIFEST_BOUND_INPUT``.  Once bound, it evaluates sixteen transparent
ZT/ZF/ZN/TN/ZB/UB curve rules with causal next-tradable-open execution.

Future outcomes are never materialised in the decision feature table.  Price
paths after a decision are consumed only by the chronological execution replay.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.production.autonomous_exact_replay import (
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _load_rule_snapshot,
)
from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    TradePathEvent,
    run_combine_episode,
)


SCHEMA = "hydra_curve_relative_value_tripwire_v1"
INPUT_SCHEMA = "hydra_curve_relative_value_input_contract_v1"
BRANCH_ID = "CURVE_RELATIVE_VALUE_TRIPWIRE_V1"
WAITING_STATUS = "AWAITING_MANIFEST_BOUND_INPUT"
EVIDENCE_ROLE = "VIEWED_DEVELOPMENT_TRIPWIRE_ONLY"
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
CONTROLS = ("PRIMARY", "SIGN_FLIP", "TIMING_DELAY_5_BARS")
HOLDING_MINUTES = 30
FEATURE_VOLATILITY_BARS = 60
DISPLACEMENT_BARS = 15
ZSCORE_BARS = 120
RISK_BUDGET_MLL_FRACTION = 0.15
MAXIMUM_RULES = 16

# Authoritative estimate supplied by the director after the branch-exhaustion
# audit.  This module does not purchase or download the sample.
OFFICIAL_COST_RECEIPT = {
    "source": "DATABENTO_HISTORICAL_METADATA_GET_COST",
    "estimated_cost_usd": 9.090267196298,
    "estimated_records": 2_489_949,
    "estimated_bytes": 139_437_144,
    "purchase_performed_by_module": False,
}

COMMISSION_SOURCE_URL = (
    "https://help.topstep.com/en/articles/8284213-topstepx-commissions-and-fees"
)
COMMISSION_RETRIEVED_AT_UTC = "2026-07-19T11:54:38Z"
DIRECT_RENDERED_COMMISSION_VALUES = {
    "ZT": 2.32,
    "ZF": 2.32,
    "ZN": 2.58,
    "TN": 2.62,
    "ZB": 2.76,
    "UB": 2.92,
}
# A separate official live-crawl observation reported each Treasury RT value
# $0.02 higher than the page rendered at the retrieval time above.  Until that
# source conflict is resolved, the larger values are the conservative economic
# inputs and the snapshot explicitly forbids a claim of exact current fees.
APPLIED_CONSERVATIVE_COMMISSION_VALUES = {
    "ZT": 2.34,
    "ZF": 2.34,
    "ZN": 2.60,
    "TN": 2.64,
    "ZB": 2.78,
    "UB": 2.94,
}

FORBIDDEN_DECISION_COLUMN_TOKENS = (
    "future_",
    "forward_",
    "outcome",
    "label",
    "mfe",
    "mae",
    "favorable_first",
    "adverse_first",
    "next_bar",
)


class CurveTripwireError(RuntimeError):
    """The isolated tripwire cannot preserve its immutable contract."""


@dataclass(frozen=True, slots=True)
class TreasurySpec:
    root: str
    tenor_years: float
    tick_size_points: float
    tick_value_usd: float
    point_value_usd: float
    round_turn_commission_usd: float

    def __post_init__(self) -> None:
        expected = self.tick_size_points * self.point_value_usd
        if not math.isclose(expected, self.tick_value_usd, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError(f"tick value drift for {self.root}")
        if self.round_turn_commission_usd <= 0:
            raise ValueError(f"round-turn commission must be positive for {self.root}")

    def round_turn_cost(self, *, stressed: bool) -> float:
        slippage_ticks_per_side = 0.75 if stressed else 0.50
        return float(
            self.round_turn_commission_usd
            + 2.0 * slippage_ticks_per_side * self.tick_value_usd
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


TREASURY_SPECS: dict[str, TreasurySpec] = {
    "ZT": TreasurySpec("ZT", 2.0, 1.0 / 256.0, 7.8125, 2_000.0, 2.34),
    "ZF": TreasurySpec("ZF", 5.0, 1.0 / 128.0, 7.8125, 1_000.0, 2.34),
    "ZN": TreasurySpec("ZN", 10.0, 1.0 / 64.0, 15.625, 1_000.0, 2.60),
    "TN": TreasurySpec("TN", 10.5, 1.0 / 64.0, 15.625, 1_000.0, 2.64),
    "ZB": TreasurySpec("ZB", 30.0, 1.0 / 32.0, 31.25, 1_000.0, 2.78),
    "UB": TreasurySpec("UB", 35.0, 1.0 / 32.0, 31.25, 1_000.0, 2.94),
}


def treasury_commission_snapshot() -> dict[str, Any]:
    """Return the sealed, conservative fee observation used by the tripwire.

    Two reads of the official TopstepX fee table disagreed by USD 0.02 per
    round turn.  Economic replay therefore uses the larger observation and
    records the conflict instead of claiming exact-current-fee provenance.
    """

    core: dict[str, Any] = {
        "schema": "hydra_topstepx_treasury_commission_snapshot_v1",
        "status": "OFFICIAL_SOURCE_CONTENT_CONFLICT_CONSERVATIVE_UPPER_BOUND",
        "source_url": COMMISSION_SOURCE_URL,
        "retrieved_at_utc": COMMISSION_RETRIEVED_AT_UTC,
        "source_page_updated_label": "UPDATED_YESTERDAY_AT_RETRIEVAL",
        "exact_current_fee_claimed": False,
        "economic_cost_policy": (
            "CONSERVATIVE_MAX_OF_CONFLICTING_OFFICIAL_OBSERVATIONS"
        ),
        "direct_rendered_round_turn_usd": dict(
            sorted(DIRECT_RENDERED_COMMISSION_VALUES.items())
        ),
        "independent_live_crawl_round_turn_usd": dict(
            sorted(APPLIED_CONSERVATIVE_COMMISSION_VALUES.items())
        ),
        "applied_round_turn_usd": dict(
            sorted(APPLIED_CONSERVATIVE_COMMISSION_VALUES.items())
        ),
    }
    core["provenance_hash"] = _stable_hash(core)
    return core


@dataclass(frozen=True, slots=True)
class PairSpec:
    pair_id: str
    shorter_root: str
    longer_root: str

    def __post_init__(self) -> None:
        if self.shorter_root not in TREASURY_SPECS or self.longer_root not in TREASURY_SPECS:
            raise ValueError("unsupported Treasury root")
        if (
            TREASURY_SPECS[self.shorter_root].tenor_years
            >= TREASURY_SPECS[self.longer_root].tenor_years
        ):
            raise ValueError("Treasury curve pair must be maturity ordered")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RuleSpec:
    rule_id: str
    pair_id: str
    mechanism: str
    session_role: str
    trigger_z: float
    holding_minutes: int = HOLDING_MINUTES
    stop_risk_multiple: float = 1.0
    target_risk_multiple: float = 2.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


PAIR_SPECS: tuple[PairSpec, ...] = (
    PairSpec("ZT_ZF", "ZT", "ZF"),
    PairSpec("ZF_ZN", "ZF", "ZN"),
    PairSpec("ZN_TN", "ZN", "TN"),
    PairSpec("ZB_UB", "ZB", "UB"),
)


def frozen_rules() -> tuple[RuleSpec, ...]:
    """The complete bounded lattice: 4 pairs x 2 mechanisms x 2 sessions."""

    rules = tuple(
        RuleSpec(
            rule_id=f"curve_v1:{pair.pair_id}:{mechanism}:{session}",
            pair_id=pair.pair_id,
            mechanism=mechanism,
            session_role=session,
            trigger_z=2.00 if mechanism == "REVERSION" else 1.25,
        )
        for pair in PAIR_SPECS
        for mechanism in ("REVERSION", "CONTINUATION")
        for session in ("OPEN", "MID")
    )
    if len(rules) != MAXIMUM_RULES or len({row.rule_id for row in rules}) != len(rules):
        raise CurveTripwireError("the frozen Treasury rule lattice must contain 16 unique rules")
    return rules


def build_curve_relative_value_tripwire(
    root: str | Path,
    *,
    input_contract: Mapping[str, Any] | None = None,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
    pair_frames: Mapping[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    """Return a deterministic waiting receipt or a complete tripwire result.

    ``pair_frames`` exists only for targeted toy tests.  Production execution
    requires ``input_contract`` with SHA-bound data and roll receipts.
    """

    project = Path(root).resolve()
    rules_path = _inside_file(project, rule_snapshot_path)
    account_rules, rule_receipt = _load_rule_snapshot(rules_path)
    if input_contract is None and pair_frames is None:
        return awaiting_manifest_bound_input(rule_receipt)

    if pair_frames is None:
        contract = _validate_input_contract(project, input_contract)
        raw_frames, input_receipt = _load_bound_pair_frames(project, contract)
    else:
        raw_frames = {str(key): value.copy() for key, value in pair_frames.items()}
        if set(raw_frames) != {pair.pair_id for pair in PAIR_SPECS}:
            raise CurveTripwireError("toy pair-frame inventory must cover all frozen pairs")
        input_receipt = {
            "mode": "IN_MEMORY_TOY_FIXTURE",
            "pair_hashes": {
                key: _frame_hash(value) for key, value in sorted(raw_frames.items())
            },
            "cost_receipt": dict(OFFICIAL_COST_RECEIPT),
        }

    prepared_frames: dict[str, pd.DataFrame] = {}
    temporal_contracts: dict[str, dict[str, Any]] = {}
    roll_audits: dict[str, dict[str, Any]] = {}
    for pair in PAIR_SPECS:
        prepared, roll_audit = prepare_pair_frame(raw_frames[pair.pair_id], pair)
        temporal = freeze_temporal_roles(prepared)
        prepared["temporal_role"] = prepared["session_id"].map(
            temporal["role_by_session"]
        )
        if prepared["temporal_role"].isna().any():
            raise CurveTripwireError("temporal roles did not cover the complete pair frame")
        prepared_frames[pair.pair_id] = prepared
        temporal_contracts[pair.pair_id] = temporal
        roll_audits[pair.pair_id] = roll_audit

    candidates = [
        evaluate_rule(
            prepared_frames[rule.pair_id],
            pair=_pair(rule.pair_id),
            rule=rule,
            account_rules=account_rules,
        )
        for rule in frozen_rules()
    ]
    candidates.sort(key=lambda row: str(row["rule_id"]))
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": "COMPLETE_DEVELOPMENT_TRIPWIRE",
        "evidence_role": EVIDENCE_ROLE,
        "authoritative_writes": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "causal_contract": _causal_contract(),
        "official_cost_receipt": dict(OFFICIAL_COST_RECEIPT),
        "official_rule_snapshot": rule_receipt,
        "treasury_commission_snapshot": treasury_commission_snapshot(),
        "input_receipt": input_receipt,
        "treasury_specs": {
            key: value.to_dict() for key, value in sorted(TREASURY_SPECS.items())
        },
        "pair_specs": [row.to_dict() for row in PAIR_SPECS],
        "rule_specs": [row.to_dict() for row in frozen_rules()],
        "temporal_contracts": temporal_contracts,
        "roll_delivery_audits": roll_audits,
        "controls": list(CONTROLS),
        "horizons_trading_days": list(HORIZONS),
        "scenarios": list(SCENARIOS),
        "candidate_results": candidates,
        "economic_summary": economic_summary(candidates),
        "decision": tripwire_decision(candidates),
        "next_action": "DO_NOT_PROMOTE_WITHOUT_MANIFEST_BOUND_UNSEEN_CONFIRMATION",
    }
    core["result_hash"] = _stable_hash(core)
    return core


def awaiting_manifest_bound_input(rule_receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Create the deterministic, non-economic fail-closed receipt."""

    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": WAITING_STATUS,
        "decision": WAITING_STATUS,
        "evidence_role": None,
        "economic_result_created": False,
        "official_cost_receipt": dict(OFFICIAL_COST_RECEIPT),
        "official_rule_snapshot": dict(rule_receipt),
        "treasury_commission_snapshot": treasury_commission_snapshot(),
        "required_input_contract": {
            "schema": INPUT_SCHEMA,
            "q4_excluded": True,
            "required_roots": sorted(TREASURY_SPECS),
            "required_files_fields": [
                "path",
                "sha256",
                "dataset",
                "schema",
                "roots",
                "record_count",
            ],
            "required_roll_receipt_fields": ["path", "sha256", "policy"],
            "roll_policy": "EXPLICIT_CONTRACT_DELIVERY_SYNC_NO_FORWARD_FILL",
            "expected_cost_receipt": dict(OFFICIAL_COST_RECEIPT),
        },
        "rule_specs": [row.to_dict() for row in frozen_rules()],
        "authoritative_writes": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": "BIND_PURCHASED_TREASURY_PATHS_AND_ROLL_RECEIPT_IN_MANIFEST",
    }
    core["result_hash"] = _stable_hash(core)
    return core


def _validate_input_contract(
    project: Path, value: Mapping[str, Any] | None
) -> dict[str, Any]:
    if value is None or str(value.get("schema")) != INPUT_SCHEMA:
        raise CurveTripwireError("manifest-bound Treasury input contract is missing or invalid")
    if value.get("q4_excluded") is not True:
        raise CurveTripwireError("Treasury input contract must explicitly exclude protected Q4")
    files = list(value.get("files") or [])
    if not files:
        raise CurveTripwireError("Treasury input contract contains no immutable files")
    covered: set[str] = set()
    verified_files: list[dict[str, Any]] = []
    total_records = 0
    for receipt in files:
        roots = {str(root).upper() for root in receipt.get("roots") or []}
        covered.update(roots)
        path = _inside_file(project, str(receipt.get("path") or ""))
        digest = _sha256(path)
        if digest != str(receipt.get("sha256") or ""):
            raise CurveTripwireError(f"Treasury input SHA mismatch: {path}")
        records = int(receipt.get("record_count") or 0)
        if records <= 0:
            raise CurveTripwireError("Treasury input record count must be positive")
        total_records += records
        verified_files.append(
            {
                **dict(receipt),
                "path": str(path.relative_to(project)),
                "sha256": digest,
            }
        )
    if covered != set(TREASURY_SPECS):
        raise CurveTripwireError("Treasury input root inventory is incomplete")
    roll = dict(value.get("roll_receipt") or {})
    if roll.get("policy") != "EXPLICIT_CONTRACT_DELIVERY_SYNC_NO_FORWARD_FILL":
        raise CurveTripwireError("unsupported Treasury roll policy")
    roll_path = _inside_file(project, str(roll.get("path") or ""))
    if _sha256(roll_path) != str(roll.get("sha256") or ""):
        raise CurveTripwireError("Treasury roll receipt SHA mismatch")
    cost = dict(value.get("cost_receipt") or {})
    for field in ("estimated_cost_usd", "estimated_records", "estimated_bytes"):
        if not math.isclose(
            float(cost.get(field, -1)),
            float(OFFICIAL_COST_RECEIPT[field]),
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise CurveTripwireError(f"official Databento cost receipt drift: {field}")
    core = {
        "schema": INPUT_SCHEMA,
        "q4_excluded": True,
        "files": verified_files,
        "roll_receipt": {
            **roll,
            "path": str(roll_path.relative_to(project)),
            "sha256": _sha256(roll_path),
        },
        "cost_receipt": cost,
        "declared_record_count": total_records,
    }
    core["input_contract_hash"] = _stable_hash(core)
    return core


def _load_bound_pair_frames(
    project: Path, contract: Mapping[str, Any]
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    columns = [
        "timestamp",
        "symbol",
        "contract",
        "delivery_month",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "session_id",
    ]
    pieces: list[pd.DataFrame] = []
    actual_records = 0
    for receipt in contract["files"]:
        path = _inside_file(project, receipt["path"])
        frame = pd.read_parquet(path, columns=columns)
        pieces.append(frame)
        actual_records += len(frame)
    raw = pd.concat(pieces, ignore_index=True)
    if actual_records != int(contract["declared_record_count"]):
        raise CurveTripwireError("Treasury file receipt record count does not reconcile")
    if set(raw["symbol"].astype(str).str.upper()) != set(TREASURY_SPECS):
        raise CurveTripwireError("Treasury raw root inventory drift")
    outputs = {pair.pair_id: _align_pair(raw, pair) for pair in PAIR_SPECS}
    receipt = {
        "mode": "MANIFEST_BOUND_IMMUTABLE_FILES",
        "input_contract_hash": contract["input_contract_hash"],
        "file_receipts": contract["files"],
        "roll_receipt": contract["roll_receipt"],
        "actual_record_count": actual_records,
        "cost_receipt": contract["cost_receipt"],
    }
    return outputs, receipt


def _align_pair(raw: pd.DataFrame, pair: PairSpec) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for root in (pair.shorter_root, pair.longer_root):
        selected = raw.loc[
            raw["symbol"].astype(str).str.upper() == root,
            [
                "timestamp",
                "contract",
                "delivery_month",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "session_id",
            ],
        ].copy()
        if selected["timestamp"].duplicated().any():
            raise CurveTripwireError(f"duplicate {root} timestamp in bound Treasury input")
        selected = selected.rename(
            columns={column: f"{root}_{column}" for column in selected.columns if column != "timestamp"}
        )
        pieces.append(selected)
    merged = pieces[0].merge(pieces[1], on="timestamp", how="inner", validate="one_to_one")
    left_session = merged[f"{pair.shorter_root}_session_id"].astype(str)
    right_session = merged[f"{pair.longer_root}_session_id"].astype(str)
    if not left_session.eq(right_session).all():
        raise CurveTripwireError(f"session identity drift for {pair.pair_id}")
    merged["session_id"] = left_session
    return merged.drop(
        columns=[f"{pair.shorter_root}_session_id", f"{pair.longer_root}_session_id"]
    ).sort_values("timestamp").reset_index(drop=True)


def prepare_pair_frame(
    frame: pd.DataFrame, pair: PairSpec
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate roll/delivery identity and build causal features by roll segment."""

    forbidden = sorted(
        column
        for column in frame.columns
        if any(token in str(column).lower() for token in FORBIDDEN_DECISION_COLUMN_TOKENS)
    )
    if forbidden:
        raise CurveTripwireError(f"future/outcome columns are physically forbidden: {forbidden}")
    roots = (pair.shorter_root, pair.longer_root)
    required = {"timestamp", "session_id"}
    for root in roots:
        required.update({
            f"{root}_contract",
            f"{root}_delivery_month",
            *(f"{root}_{field}" for field in ("open", "high", "low", "close")),
        })
    missing = sorted(required - set(frame.columns))
    if missing:
        raise CurveTripwireError(f"Treasury pair frame lacks columns: {missing}")
    output = frame.copy().sort_values("timestamp").reset_index(drop=True)
    output["timestamp"] = pd.to_datetime(output["timestamp"], utc=True)
    if output["timestamp"].duplicated().any():
        raise CurveTripwireError("aligned Treasury pair timestamps must be unique")
    before = len(output)
    delivery_left = output[f"{pair.shorter_root}_delivery_month"].astype(str)
    delivery_right = output[f"{pair.longer_root}_delivery_month"].astype(str)
    synchronized = delivery_left.eq(delivery_right)
    output = output.loc[synchronized].copy().reset_index(drop=True)
    if output.empty:
        raise CurveTripwireError("no delivery-synchronized Treasury observations remain")
    for root in roots:
        if output[f"{root}_contract"].astype(str).str.strip().eq("").any():
            raise CurveTripwireError("Treasury contract identity may not be empty")
        for field in ("open", "high", "low", "close"):
            output[f"{root}_{field}"] = pd.to_numeric(
                output[f"{root}_{field}"], errors="coerce"
            )
    output = output.dropna(
        subset=[f"{root}_{field}" for root in roots for field in ("open", "high", "low", "close")]
    ).reset_index(drop=True)
    if output.empty or (output[[f"{root}_close" for root in roots]] <= 0).any(axis=None):
        raise CurveTripwireError("Treasury price inputs are empty or non-positive")

    contract_identity = (
        output[f"{pair.shorter_root}_contract"].astype(str)
        + "|"
        + output[f"{pair.longer_root}_contract"].astype(str)
        + "|"
        + output[f"{pair.shorter_root}_delivery_month"].astype(str)
    )
    output["roll_segment"] = contract_identity.ne(contract_identity.shift(1)).cumsum().astype(int)
    group = output["roll_segment"]
    log_left = np.log(output[f"{pair.shorter_root}_close"].astype(float))
    log_right = np.log(output[f"{pair.longer_root}_close"].astype(float))
    return_left = log_left.groupby(group).diff()
    return_right = log_right.groupby(group).diff()
    volatility_left = _group_rolling_std(return_left, group, FEATURE_VOLATILITY_BARS)
    volatility_right = _group_rolling_std(return_right, group, FEATURE_VOLATILITY_BARS)
    normalized = return_left / volatility_left.clip(lower=1e-9) - return_right / volatility_right.clip(lower=1e-9)
    displacement = _group_rolling_sum(normalized, group, DISPLACEMENT_BARS)
    mean = _group_rolling_mean(displacement, group, ZSCORE_BARS)
    deviation = _group_rolling_std(displacement, group, ZSCORE_BARS)
    output["relative_z"] = (displacement - mean) / deviation.clip(lower=1e-9)
    output["relative_z_delta_5"] = output["relative_z"].groupby(group).diff(5)

    left_spec = TREASURY_SPECS[pair.shorter_root]
    right_spec = TREASURY_SPECS[pair.longer_root]
    dollar_left = output[f"{pair.shorter_root}_close"].astype(float).groupby(group).diff() * left_spec.point_value_usd
    dollar_right = output[f"{pair.longer_root}_close"].astype(float).groupby(group).diff() * right_spec.point_value_usd
    output["left_dollar_sigma_60"] = _group_rolling_std(
        dollar_left, group, FEATURE_VOLATILITY_BARS
    )
    output["right_dollar_sigma_60"] = _group_rolling_std(
        dollar_right, group, FEATURE_VOLATILITY_BARS
    )
    local = output["timestamp"].dt.tz_convert("America/Chicago")
    output["local_minute"] = local.dt.hour * 60 + local.dt.minute
    output["session_day"] = output["session_id"].map(_session_ordinal).astype(int)
    audit_core = {
        "pair_id": pair.pair_id,
        "input_rows": before,
        "delivery_synchronized_rows": len(output),
        "delivery_mismatch_rows_excluded": before - int(synchronized.sum()),
        "roll_segment_count": int(output["roll_segment"].nunique()),
        "policy": "EXPLICIT_CONTRACT_DELIVERY_SYNC_NO_FORWARD_FILL",
        "features_reset_at_roll": True,
        "trades_may_cross_roll": False,
    }
    audit_core["audit_hash"] = _stable_hash(audit_core)
    return output, audit_core


def freeze_temporal_roles(frame: pd.DataFrame) -> dict[str, Any]:
    sessions = tuple(sorted({str(value) for value in frame["session_id"]}))
    if len(sessions) < 15:
        raise CurveTripwireError("at least 15 Treasury sessions are required")
    first = int(math.floor(len(sessions) * 0.60))
    second = int(math.floor(len(sessions) * 0.80))
    assignments = {
        "DISCOVERY": sessions[:first],
        "VALIDATION": sessions[first:second],
        "FINAL_DEVELOPMENT": sessions[second:],
    }
    if any(not assignments[role] for role in ROLES):
        raise CurveTripwireError("all frozen temporal roles must be non-empty")
    role_by_session = {
        session: role for role in ROLES for session in assignments[role]
    }
    core = {
        "assignment": "CHRONOLOGICAL_60_20_20_PRE_OUTCOME",
        "sessions": {role: list(assignments[role]) for role in ROLES},
        "session_count": len(sessions),
    }
    return {**core, "role_by_session": role_by_session, "contract_hash": _stable_hash(core)}


def evaluate_rule(
    frame: pd.DataFrame,
    *,
    pair: PairSpec,
    rule: RuleSpec,
    account_rules: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    signals = _signal_indices(frame, rule)
    accounts: list[dict[str, Any]] = []
    event_counts: dict[str, dict[str, int]] = {}
    for label in ("50K", "100K", "150K"):
        account_rule = account_rules[label]
        controls: dict[str, Any] = {}
        event_counts[label] = {}
        for control in CONTROLS:
            gross = _generate_gross_events(
                frame,
                pair=pair,
                rule=rule,
                account_rule=account_rule,
                signal_indices=signals,
                control=control,
            )
            event_counts[label][control] = len(gross)
            scenarios = {
                scenario: _cost_events(gross, pair=pair, scenario=scenario)
                for scenario in SCENARIOS
            }
            _require_decision_identity(scenarios["NORMAL"], scenarios["STRESSED_1_5X"])
            controls[control] = _account_role_matrix(
                frame, scenarios, account_rule=account_rule
            )
        accounts.append(
            {
                "account_label": label,
                "account_size_usd": int(account_rule["account_size_usd"]),
                "profit_target_usd": float(account_rule["profit_target_usd"]),
                "maximum_loss_limit_usd": float(account_rule["maximum_loss_limit_usd"]),
                "controls": controls,
                "paired_deltas": _paired_control_deltas(controls),
            }
        )
    core = {
        "rule_id": rule.rule_id,
        "pair_id": pair.pair_id,
        "mechanism": rule.mechanism,
        "session_role": rule.session_role,
        "trigger_count": len(signals),
        "event_counts": event_counts,
        "account_matrix": accounts,
    }
    core["candidate_hash"] = _stable_hash(core)
    return core


def _signal_indices(frame: pd.DataFrame, rule: RuleSpec) -> tuple[int, ...]:
    z = frame["relative_z"].astype(float)
    strength = z.abs()
    previous = strength.groupby(frame["roll_segment"]).shift(1)
    selected = strength.ge(rule.trigger_z) & previous.lt(rule.trigger_z)
    if rule.mechanism == "CONTINUATION":
        selected &= (z * frame["relative_z_delta_5"].astype(float)).gt(0.0)
    elif rule.mechanism != "REVERSION":
        raise CurveTripwireError(f"unknown mechanism: {rule.mechanism}")
    if rule.session_role == "OPEN":
        selected &= frame["local_minute"].between(7 * 60 + 20, 9 * 60 + 29)
    elif rule.session_role == "MID":
        selected &= frame["local_minute"].between(9 * 60 + 30, 12 * 60 + 29)
    else:
        raise CurveTripwireError(f"unknown Treasury session role: {rule.session_role}")
    selected &= frame["left_dollar_sigma_60"].gt(0.0)
    selected &= frame["right_dollar_sigma_60"].gt(0.0)
    return tuple(int(value) for value in np.flatnonzero(selected.to_numpy()))


def _generate_gross_events(
    frame: pd.DataFrame,
    *,
    pair: PairSpec,
    rule: RuleSpec,
    account_rule: Mapping[str, Any],
    signal_indices: Sequence[int],
    control: str,
) -> tuple[TradePathEvent, ...]:
    if control not in CONTROLS:
        raise CurveTripwireError(f"unknown matched control: {control}")
    entry_lag = 5 if control == "TIMING_DELAY_5_BARS" else 1
    next_free = -1
    events: list[TradePathEvent] = []
    for signal_index in signal_indices:
        if signal_index <= next_free:
            continue
        entry_index = signal_index + entry_lag
        if not _same_execution_segment(frame, signal_index, entry_index):
            continue
        z = float(frame.at[signal_index, "relative_z"])
        direction = 1 if z > 0.0 else -1
        if rule.mechanism == "REVERSION":
            direction *= -1
        if control == "SIGN_FLIP":
            direction *= -1
        quantities = _causal_quantities(frame, signal_index, pair, account_rule)
        if quantities is None:
            continue
        quantity_a, quantity_b, risk_budget = quantities
        exit_result = _causal_exit(
            frame,
            pair=pair,
            direction=direction,
            quantity_a=quantity_a,
            quantity_b=quantity_b,
            entry_index=entry_index,
            stop_usd=risk_budget * rule.stop_risk_multiple,
            target_usd=risk_budget * rule.target_risk_multiple,
            holding_minutes=rule.holding_minutes,
        )
        if exit_result is None:
            continue
        exit_index, worst, best = exit_result
        gross = _pair_pnl(
            frame,
            pair=pair,
            direction=direction,
            quantity_a=quantity_a,
            quantity_b=quantity_b,
            entry_index=entry_index,
            mark_index=exit_index,
            field="open",
        )
        decision_ns = int(pd.Timestamp(frame.at[signal_index, "timestamp"]).value)
        fill_ns = int(pd.Timestamp(frame.at[entry_index, "timestamp"]).value)
        exit_ns = int(pd.Timestamp(frame.at[exit_index, "timestamp"]).value)
        identity = _stable_hash(
            {
                "rule_id": rule.rule_id,
                "account_label": account_rule["account_label"],
                "control": control,
                "decision_ns": decision_ns,
                "fill_ns": fill_ns,
                "exit_ns": exit_ns,
                "direction": direction,
                "quantity_a": quantity_a,
                "quantity_b": quantity_b,
            }
        )[:20]
        total_quantity = quantity_a + quantity_b
        events.append(
            TradePathEvent(
                event_id=f"{rule.rule_id}:{account_rule['account_label']}:{control}:{identity}",
                decision_ns=decision_ns,
                exit_ns=exit_ns,
                session_day=int(frame.at[signal_index, "session_day"]),
                net_pnl=float(gross),
                gross_pnl=float(gross),
                worst_unrealized_pnl=float(min(0.0, worst, gross)),
                best_unrealized_pnl=float(max(0.0, best, gross)),
                quantity=total_quantity,
                mini_equivalent=float(total_quantity),
                regime=f"{pair.pair_id}:{rule.mechanism}:{rule.session_role}",
                session_compliant=True,
                contract_limit_compliant=total_quantity
                <= int(account_rule["maximum_mini_contracts"]),
                same_bar_ambiguous=False,
            )
        )
        next_free = exit_index
    return tuple(events)


def _causal_quantities(
    frame: pd.DataFrame,
    decision_index: int,
    pair: PairSpec,
    account_rule: Mapping[str, Any],
) -> tuple[int, int, float] | None:
    sigma_left = float(frame.at[decision_index, "left_dollar_sigma_60"])
    sigma_right = float(frame.at[decision_index, "right_dollar_sigma_60"])
    if not all(math.isfinite(value) and value > 0.0 for value in (sigma_left, sigma_right)):
        return None
    hedge_right = max(1, min(4, int(round(sigma_left / sigma_right))))
    base_adverse = max(
        25.0,
        3.0 * math.sqrt(HOLDING_MINUTES) * math.hypot(sigma_left, hedge_right * sigma_right),
    )
    risk_budget = float(account_rule["maximum_loss_limit_usd"]) * RISK_BUDGET_MLL_FRACTION
    groups_by_risk = int(math.floor(risk_budget / base_adverse))
    groups_by_contract = int(
        account_rule["maximum_mini_contracts"] // (1 + hedge_right)
    )
    groups = min(groups_by_risk, groups_by_contract)
    if groups <= 0:
        return None
    return groups, groups * hedge_right, risk_budget


def _causal_exit(
    frame: pd.DataFrame,
    *,
    pair: PairSpec,
    direction: int,
    quantity_a: int,
    quantity_b: int,
    entry_index: int,
    stop_usd: float,
    target_usd: float,
    holding_minutes: int,
) -> tuple[int, float, float] | None:
    deadline = pd.Timestamp(frame.at[entry_index, "timestamp"]) + pd.Timedelta(
        minutes=holding_minutes
    )
    worst = 0.0
    best = 0.0
    cursor = entry_index
    decision: int | None = None
    while cursor + 1 < len(frame) and _same_execution_segment(frame, entry_index, cursor):
        adverse, favorable = _pair_extrema(
            frame,
            pair=pair,
            direction=direction,
            quantity_a=quantity_a,
            quantity_b=quantity_b,
            entry_index=entry_index,
            mark_index=cursor,
        )
        worst = min(worst, adverse)
        best = max(best, favorable)
        close_pnl = _pair_pnl(
            frame,
            pair=pair,
            direction=direction,
            quantity_a=quantity_a,
            quantity_b=quantity_b,
            entry_index=entry_index,
            mark_index=cursor,
            field="close",
        )
        if adverse <= -stop_usd or close_pnl >= target_usd:
            decision = cursor
            break
        if pd.Timestamp(frame.at[cursor, "timestamp"]) >= deadline:
            decision = cursor
            break
        if int(frame.at[cursor, "local_minute"]) >= 15 * 60 + 9:
            decision = cursor
            break
        cursor += 1
    if decision is None or not _same_execution_segment(frame, entry_index, decision + 1):
        return None
    return decision + 1, worst, best


def _pair_pnl(
    frame: pd.DataFrame,
    *,
    pair: PairSpec,
    direction: int,
    quantity_a: int,
    quantity_b: int,
    entry_index: int,
    mark_index: int,
    field: str,
) -> float:
    left = TREASURY_SPECS[pair.shorter_root]
    right = TREASURY_SPECS[pair.longer_root]
    entry_left = float(frame.at[entry_index, f"{pair.shorter_root}_open"])
    entry_right = float(frame.at[entry_index, f"{pair.longer_root}_open"])
    mark_left = float(frame.at[mark_index, f"{pair.shorter_root}_{field}"])
    mark_right = float(frame.at[mark_index, f"{pair.longer_root}_{field}"])
    return float(
        direction * (mark_left - entry_left) * left.point_value_usd * quantity_a
        - direction * (mark_right - entry_right) * right.point_value_usd * quantity_b
    )


def _pair_extrema(
    frame: pd.DataFrame,
    *,
    pair: PairSpec,
    direction: int,
    quantity_a: int,
    quantity_b: int,
    entry_index: int,
    mark_index: int,
) -> tuple[float, float]:
    left = TREASURY_SPECS[pair.shorter_root]
    right = TREASURY_SPECS[pair.longer_root]
    entry_left = float(frame.at[entry_index, f"{pair.shorter_root}_open"])
    entry_right = float(frame.at[entry_index, f"{pair.longer_root}_open"])
    if direction > 0:
        adverse_left = float(frame.at[mark_index, f"{pair.shorter_root}_low"])
        favorable_left = float(frame.at[mark_index, f"{pair.shorter_root}_high"])
        adverse_right = float(frame.at[mark_index, f"{pair.longer_root}_high"])
        favorable_right = float(frame.at[mark_index, f"{pair.longer_root}_low"])
    else:
        adverse_left = float(frame.at[mark_index, f"{pair.shorter_root}_high"])
        favorable_left = float(frame.at[mark_index, f"{pair.shorter_root}_low"])
        adverse_right = float(frame.at[mark_index, f"{pair.longer_root}_low"])
        favorable_right = float(frame.at[mark_index, f"{pair.longer_root}_high"])
    adverse = (
        direction * (adverse_left - entry_left) * left.point_value_usd * quantity_a
        - direction * (adverse_right - entry_right) * right.point_value_usd * quantity_b
    )
    favorable = (
        direction * (favorable_left - entry_left) * left.point_value_usd * quantity_a
        - direction * (favorable_right - entry_right) * right.point_value_usd * quantity_b
    )
    return float(adverse), float(favorable)


def _cost_events(
    events: Sequence[TradePathEvent], *, pair: PairSpec, scenario: str
) -> tuple[TradePathEvent, ...]:
    stressed = scenario == "STRESSED_1_5X"
    left_cost = TREASURY_SPECS[pair.shorter_root].round_turn_cost(stressed=stressed)
    right_cost = TREASURY_SPECS[pair.longer_root].round_turn_cost(stressed=stressed)
    output: list[TradePathEvent] = []
    for event in events:
        # The aggregate event does not retain leg quantities.  Its identity
        # embeds them, while a conservative equal split upper-bounds costs when
        # the hedge leg has more contracts.
        cost = float(event.quantity) * max(left_cost, right_cost)
        output.append(
            replace(
                event,
                event_id=f"{event.event_id}:{scenario}",
                net_pnl=float(event.gross_pnl - cost),
                worst_unrealized_pnl=float(event.worst_unrealized_pnl - cost),
                best_unrealized_pnl=float(event.best_unrealized_pnl - cost),
            )
        )
    return tuple(output)


def _account_role_matrix(
    frame: pd.DataFrame,
    events_by_scenario: Mapping[str, Sequence[TradePathEvent]],
    *,
    account_rule: Mapping[str, Any],
) -> dict[str, Any]:
    config = _account_config(account_rule)
    result: dict[str, Any] = {}
    for scenario in SCENARIOS:
        role_rows: dict[str, Any] = {}
        for role in ROLES:
            sessions = (
                frame.loc[frame["temporal_role"] == role, ["session_id", "session_day"]]
                .drop_duplicates()
                .sort_values("session_id")
            )
            days = tuple(int(value) for value in sessions["session_day"])
            horizon_rows: dict[str, Any] = {}
            for horizon in HORIZONS:
                starts = tuple(
                    days[index]
                    for index in range(0, len(days), horizon)
                    if index + horizon <= len(days)
                )
                episodes = tuple(
                    run_combine_episode(
                        tuple(events_by_scenario[scenario]),
                        days,
                        start_day=start,
                        maximum_duration_days=horizon,
                        config=config,
                        maximum_mini_equivalent=float(account_rule["maximum_mini_contracts"]),
                    )
                    for start in starts
                )
                horizon_rows[str(horizon)] = _episode_summary(episodes)
            role_rows[role] = horizon_rows
        result[scenario] = role_rows
    return result


def _episode_summary(episodes: Sequence[CombineEpisodeResult]) -> dict[str, Any]:
    if not episodes:
        return {
            "episodes": 0,
            "passes": 0,
            "pass_rate": 0.0,
            "mll_breaches": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 0.0,
            "net_total_usd": 0.0,
            "net_median_usd": 0.0,
            "target_progress_median": 0.0,
            "target_progress_p25": 0.0,
            "minimum_mll_buffer_usd": None,
            "median_days_to_target": None,
            "terminal_distribution": {},
        }
    passed = sum(row.passed for row in episodes)
    breached = sum(row.mll_breached for row in episodes)
    target_days = [row.days_to_target for row in episodes if row.days_to_target is not None]
    return {
        "episodes": len(episodes),
        "passes": passed,
        "pass_rate": float(passed / len(episodes)),
        "mll_breaches": breached,
        "mll_breach_rate": float(breached / len(episodes)),
        "consistency_compliance_rate": float(
            sum(row.consistency_ok for row in episodes) / len(episodes)
        ),
        "net_total_usd": float(sum(row.net_pnl for row in episodes)),
        "net_median_usd": float(np.median([row.net_pnl for row in episodes])),
        "target_progress_median": float(np.median([row.target_progress for row in episodes])),
        "target_progress_p25": float(np.percentile([row.target_progress for row in episodes], 25)),
        "minimum_mll_buffer_usd": float(min(row.minimum_mll_buffer for row in episodes)),
        "median_days_to_target": float(np.median(target_days)) if target_days else None,
        "terminal_distribution": dict(
            sorted(Counter(row.terminal.value for row in episodes).items())
        ),
    }


def _paired_control_deltas(controls: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    primary = controls["PRIMARY"]
    for control in ("SIGN_FLIP", "TIMING_DELAY_5_BARS"):
        for scenario in SCENARIOS:
            for role in ROLES:
                for horizon in HORIZONS:
                    observed = primary[scenario][role][str(horizon)]
                    matched = controls[control][scenario][role][str(horizon)]
                    rows.append(
                        {
                            "control": control,
                            "scenario": scenario,
                            "temporal_role": role,
                            "horizon_trading_days": horizon,
                            "pass_rate_delta": float(observed["pass_rate"] - matched["pass_rate"]),
                            "median_target_progress_delta": float(
                                observed["target_progress_median"]
                                - matched["target_progress_median"]
                            ),
                            "net_total_delta_usd": float(
                                observed["net_total_usd"] - matched["net_total_usd"]
                            ),
                        }
                    )
    return rows


def economic_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    scored: list[dict[str, Any]] = []
    total_episodes = 0
    total_passes = 0
    total_mll = 0
    total_trades = 0
    for candidate in candidates:
        total_trades += int(candidate["event_counts"]["50K"]["PRIMARY"])
        for account in candidate["account_matrix"]:
            final = account["controls"]["PRIMARY"]["STRESSED_1_5X"]["FINAL_DEVELOPMENT"]
            for horizon in HORIZONS:
                row = final[str(horizon)]
                total_episodes += int(row["episodes"])
                total_passes += int(row["passes"])
                total_mll += int(row["mll_breaches"])
            twenty = final["20"]
            deltas = [
                row
                for row in account["paired_deltas"]
                if row["scenario"] == "STRESSED_1_5X"
                and row["temporal_role"] == "FINAL_DEVELOPMENT"
                and row["horizon_trading_days"] == 20
            ]
            scored.append(
                {
                    "rule_id": candidate["rule_id"],
                    "account_label": account["account_label"],
                    "stressed_20d_passes": twenty["passes"],
                    "stressed_20d_episodes": twenty["episodes"],
                    "stressed_20d_pass_rate": twenty["pass_rate"],
                    "stressed_20d_net_total_usd": twenty["net_total_usd"],
                    "stressed_20d_target_progress_median": twenty["target_progress_median"],
                    "stressed_20d_mll_breach_rate": twenty["mll_breach_rate"],
                    "minimum_control_net_delta_usd": min(
                        (row["net_total_delta_usd"] for row in deltas), default=0.0
                    ),
                }
            )
    scored.sort(
        key=lambda row: (
            -float(row["stressed_20d_pass_rate"]),
            -float(row["stressed_20d_target_progress_median"]),
            -float(row["stressed_20d_net_total_usd"]),
            str(row["rule_id"]),
            str(row["account_label"]),
        )
    )
    return {
        "rule_count": len(candidates),
        "primary_50k_trade_count": total_trades,
        "final_development_stressed_episode_count": total_episodes,
        "final_development_stressed_pass_count": total_passes,
        "final_development_stressed_mll_breach_count": total_mll,
        "ranked_final_development_points": scored[:12],
    }


def tripwire_decision(candidates: Sequence[Mapping[str, Any]]) -> str:
    weak = False
    for candidate in candidates:
        for account in candidate["account_matrix"]:
            primary = account["controls"]["PRIMARY"]["STRESSED_1_5X"]
            validation = primary["VALIDATION"]["20"]
            final = primary["FINAL_DEVELOPMENT"]["20"]
            relevant_deltas = [
                row
                for row in account["paired_deltas"]
                if row["scenario"] == "STRESSED_1_5X"
                and row["horizon_trading_days"] == 20
                and row["temporal_role"] in {"VALIDATION", "FINAL_DEVELOPMENT"}
            ]
            if (
                validation["net_total_usd"] > 0.0
                and final["net_total_usd"] > 0.0
                and final["mll_breach_rate"] <= 0.10
                and (validation["passes"] > 0 or final["passes"] > 0)
                and all(row["net_total_delta_usd"] > 0.0 for row in relevant_deltas)
            ):
                return "CURVE_RELATIVE_VALUE_TRIPWIRE_GREEN_DEVELOPMENT_ONLY"
            weak = weak or (
                validation["net_total_usd"] > 0.0
                or final["net_total_usd"] > 0.0
                or validation["target_progress_median"] > 0.0
                or final["target_progress_median"] > 0.0
            )
    return "CURVE_RELATIVE_VALUE_TRIPWIRE_WEAK" if weak else "CURVE_RELATIVE_VALUE_TRIPWIRE_FALSIFIED"


def _causal_contract() -> dict[str, Any]:
    return {
        "decision_features": "COMPLETED_OR_PAST_BARS_ONLY",
        "feature_whitelist": [
            "relative_z",
            "relative_z_delta_5",
            "left_dollar_sigma_60",
            "right_dollar_sigma_60",
            "local_minute",
            "roll_segment",
        ],
        "future_outcome_columns_physically_excluded": True,
        "fill": "NEXT_TRADABLE_BAR_OPEN",
        "timing_control_fill": "FIFTH_NEXT_TRADABLE_BAR_OPEN",
        "exit": "NEXT_TRADABLE_BAR_OPEN_AFTER_CAUSAL_EXIT_DECISION",
        "roll_policy": "EXPLICIT_CONTRACT_DELIVERY_SYNC_NO_FORWARD_FILL",
        "normal_stress_decision_identity": True,
    }


def _same_execution_segment(frame: pd.DataFrame, left: int, right: int) -> bool:
    return bool(
        0 <= left < len(frame)
        and 0 <= right < len(frame)
        and str(frame.at[left, "session_id"]) == str(frame.at[right, "session_id"])
        and int(frame.at[left, "roll_segment"]) == int(frame.at[right, "roll_segment"])
    )


def _require_decision_identity(
    normal: Sequence[TradePathEvent], stressed: Sequence[TradePathEvent]
) -> None:
    def identity(row: TradePathEvent) -> tuple[Any, ...]:
        event_id = row.event_id
        for suffix in (":NORMAL", ":STRESSED_1_5X"):
            if event_id.endswith(suffix):
                event_id = event_id[: -len(suffix)]
        return (
            event_id,
            row.decision_ns,
            row.exit_ns,
            row.session_day,
            row.quantity,
            row.mini_equivalent,
        )

    if [identity(row) for row in normal] != [identity(row) for row in stressed]:
        raise CurveTripwireError("normal/stressed decisions or fills diverged")


def _group_rolling_std(values: pd.Series, groups: pd.Series, window: int) -> pd.Series:
    return values.groupby(groups).transform(
        lambda part: part.rolling(window, min_periods=window).std(ddof=0)
    )


def _group_rolling_sum(values: pd.Series, groups: pd.Series, window: int) -> pd.Series:
    return values.groupby(groups).transform(
        lambda part: part.rolling(window, min_periods=window).sum()
    )


def _group_rolling_mean(values: pd.Series, groups: pd.Series, window: int) -> pd.Series:
    return values.groupby(groups).transform(
        lambda part: part.rolling(window, min_periods=window).mean()
    )


def _pair(pair_id: str) -> PairSpec:
    for value in PAIR_SPECS:
        if value.pair_id == pair_id:
            return value
    raise CurveTripwireError(f"unknown Treasury pair: {pair_id}")


def _session_ordinal(value: Any) -> int:
    return int(pd.Timestamp(str(value)).date().toordinal())


def _inside_file(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (path if path.is_absolute() else root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CurveTripwireError(f"path escapes repository: {value}") from exc
    if not resolved.is_file():
        raise CurveTripwireError(f"required bound input is absent: {resolved}")
    return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(value: Mapping[str, Any]) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _frame_hash(frame: pd.DataFrame) -> str:
    normalized = frame.copy()
    for column in normalized.columns:
        if pd.api.types.is_datetime64_any_dtype(normalized[column]):
            normalized[column] = pd.to_datetime(normalized[column], utc=True).astype(str)
    return _stable_hash({"records": normalized.sort_index(axis=1).to_dict(orient="records")})


def write_deterministic_result(payload: Mapping[str, Any], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    temporary = target.with_name(f".{target.name}.tmp")
    temporary.write_bytes(raw)
    temporary.replace(target)
    return target


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--input-contract")
    parser.add_argument("--rule-snapshot", default=str(DEFAULT_RULE_SNAPSHOT))
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args(argv)
    root = Path(arguments.root).resolve()
    contract = None
    if arguments.input_contract:
        contract_path = _inside_file(root, arguments.input_contract)
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    result = build_curve_relative_value_tripwire(
        root,
        input_contract=contract,
        rule_snapshot_path=arguments.rule_snapshot,
    )
    output = Path(arguments.output)
    if not output.is_absolute():
        output = root / output
    write_deterministic_result(result, output)
    print(json.dumps({"status": result["status"], "result_hash": result["result_hash"], "output": str(output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "APPLIED_CONSERVATIVE_COMMISSION_VALUES",
    "BRANCH_ID",
    "COMMISSION_RETRIEVED_AT_UTC",
    "COMMISSION_SOURCE_URL",
    "CONTROLS",
    "CurveTripwireError",
    "DIRECT_RENDERED_COMMISSION_VALUES",
    "OFFICIAL_COST_RECEIPT",
    "PAIR_SPECS",
    "TREASURY_SPECS",
    "WAITING_STATUS",
    "awaiting_manifest_bound_input",
    "build_curve_relative_value_tripwire",
    "freeze_temporal_roles",
    "frozen_rules",
    "prepare_pair_frame",
    "treasury_commission_snapshot",
    "write_deterministic_result",
]
