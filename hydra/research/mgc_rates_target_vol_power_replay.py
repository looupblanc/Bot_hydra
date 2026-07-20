"""One-candidate causal MGC power replay on the governed pre-Q4 extension.

This module is deliberately an adapter around the authoritative volatility-
convexity decision and exact-account replay primitives.  It freezes the one
previously selected MGC candidate and never runs discovery selection.  The
acquisition receipt is reconciled before DBN decoding; the module has no
network, purchase, mission-state, service, registry, broker, order, promotion,
or XFA path.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from collections import Counter
from dataclasses import fields
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from hydra.data.budget import request_id_for, sha256_file
from hydra.data.contract_mapping import ContractInfo, RollMap
from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_exact_replay as exact
from hydra.research import cross_asset_volatility_convexity_tripwire as shared
from scripts import acquire_mgc_rates_target_vol_power_extension as acquisition


SCHEMA = "hydra_mgc_rates_target_vol_power_replay_v1"
AUDIT_SCHEMA = "hydra_mgc_rates_target_vol_power_replay_audit_v1"
BRANCH_ID = "MGC_RATES_TARGET_VOL_POWER_REPLAY_V1"
DEFAULT_CARD = Path("config/research/mgc_rates_target_vol_power_replay_v1.json")
DEFAULT_OUTPUT = Path("reports/research_tripwires/mgc_rates_target_vol_power_replay_v1")
EXPECTED_CARD_HASH = "a42aa4e6d7cc30448d13f0ffb3c6b9c1560e89c0e46d2279300893405e9cf9cc"
CANDIDATE_ID = "volconv_MGC_rates_target_vol_gap_oco_open_v1"
CANDIDATE_FINGERPRINT = "5a4c4cfa065ce3322f4acf0ef0e286b9b672d4d46d1bd7a0fc55df57a828816f"
PARENT_BRANCH = "CROSS_ASSET_VOLATILITY_CONVEXITY_WITHOUT_DIRECTION_TRANSFER_V1"
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
PRIMARY = shared.PRIMARY
CONTROLS = (
    "TARGET_ONLY_DUTY_MATCHED_OCO",
    "SOURCE_SHIFT_5_TRUE_SESSIONS",
    "SESSION_TIME_DIRECTION_EXPOSURE_MATCHED_PLACEBO",
    "SOURCE_MAGNITUDE_PERMUTATION",
)
POWER_THRESHOLDS = {"DISCOVERY": 60, "VALIDATION": 12, "FINAL_DEVELOPMENT": 20}
SESSION_PLACEBO_OFFSET_MINUTES = 45
SESSION_PLACEBO_TOLERANCE_MINUTES = 15


class MGCPowerReplayError(RuntimeError):
    """A frozen input, causal invariant, or accounting invariant failed closed."""


def load_replay_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = _read_json(Path(path))
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    candidate = dict(card.get("frozen_candidate") or {})
    governance = dict(card.get("governance") or {})
    power = dict(card.get("power_preflight") or {})
    causal = dict(card.get("causal_contract") or {})
    mechanism = dict(causal.get("mechanism") or {})
    if (
        not claimed
        or stable_hash(core) != claimed
        or claimed != EXPECTED_CARD_HASH
        or candidate.get("candidate_id") != CANDIDATE_ID
        or candidate.get("structural_fingerprint") != CANDIDATE_FINGERPRINT
        or candidate.get("parent_branch") != PARENT_BRANCH
        or candidate.get("mechanism") != "RATES_TARGET_VOL_GAP_OCO"
        or candidate.get("execution_market") != "MGC"
        or candidate.get("source_markets") != ["ZN", "TN"]
        or candidate.get("session_role") != "OPEN"
        or candidate.get("parameter_mutation_allowed") is not False
        or candidate.get("discovery_reselection_allowed") is not False
        or candidate.get("status_inheritance_allowed") is not False
        or power.get("minimum_independent_events_per_target") != POWER_THRESHOLDS
        or power.get("threshold_relaxation_allowed") is not False
        or causal.get("execution_markets") != ["MGC"]
        or mechanism != _frozen_mechanism()
        or tuple(card.get("controls") or ()) != CONTROLS
        or governance.get("status_ceiling") != "TIER_E_EXECUTABLE_DIAGNOSTIC"
        or governance.get("promotion_allowed") is not False
        or governance.get("tier_q_allowed") is not False
        or governance.get("xfa_allowed") is not False
        or governance.get("q4_access_allowed") is not False
        or governance.get("protected_holdout_access_allowed") is not False
        or governance.get("network_access_allowed") is not False
        or governance.get("data_purchase_allowed") is not False
        or governance.get("broker_connection_allowed") is not False
        or governance.get("orders_allowed") is not False
        or governance.get("maximum_cpu_workers") != 1
        or governance.get("mission_state_writes_allowed") is not False
        or governance.get("service_or_controller_changes_allowed") is not False
        or governance.get("queue_changes_allowed") is not False
    ):
        raise MGCPowerReplayError("replay-card semantic or hash drift")
    roles = card.get("chronological_roles") or {}
    if roles != {
        "WARMUP_ONLY": ["2015-09-01", "2015-10-01"],
        "DISCOVERY": ["2015-10-01", "2019-09-10"],
        "VALIDATION": ["2019-09-10", "2020-06-24"],
        "FINAL_DEVELOPMENT": ["2020-06-24", "2021-11-04"],
    }:
        raise MGCPowerReplayError("chronological role drift")
    if str(card["frozen_inputs"]["data_end_exclusive"]) > "2024-10-01T00:00:00Z":
        raise MGCPowerReplayError("protected Q4 boundary drift")
    _validate_gate_contract(card)
    return card


def audit_replay_inputs(
    root: str | Path,
    *,
    card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Reconcile local inputs without decoding market data or reading outcomes."""

    project = Path(root).resolve()
    card_file = _inside(project, card_path)
    card = load_replay_card(card_file)
    inputs = card["frozen_inputs"]
    acquisition_card_path = _inside(project, inputs["acquisition_card_path"])
    acquisition_card = acquisition.load_and_validate_card(
        project, acquisition_card_path
    )
    if acquisition_card["card_hash"] != inputs["acquisition_card_hash"]:
        raise MGCPowerReplayError("acquisition-card binding drift")
    expected_bundle_id = request_id_for(
        {
            "card_hash": acquisition_card["card_hash"],
            "requests": {
                schema: acquisition._request(schema)  # noqa: SLF001
                for schema in acquisition.EXPECTED
            },
            "candidate_id": CANDIDATE_ID,
            "purpose": acquisition.PURPOSE,
        }
    )
    paths = acquisition._paths(  # noqa: SLF001
        project, expected_bundle_id, inputs["acquisition_receipt_path"]
    )
    try:
        receipt = acquisition._load_existing_receipt(  # noqa: SLF001
            paths["receipt"],
            bundle_id=expected_bundle_id,
            card_hash=str(acquisition_card["card_hash"]),
            budget=acquisition._bound_budget(project, None),  # noqa: SLF001
            paths=paths,
        )
    except Exception as exc:
        raise MGCPowerReplayError("sealed acquisition receipt did not reconcile") from exc
    _validate_receipt_semantics(receipt, expected_bundle_id=expected_bundle_id)
    rule_path = _inside(project, inputs["rule_snapshot_path"])
    rule_sha = sha256_file(rule_path)
    if rule_sha != inputs["rule_snapshot_sha256"]:
        raise MGCPowerReplayError("rule-snapshot SHA drift")
    acquired = {
        str(row["kind"]): {
            "path": str(_receipt_path(project, row["path"]).relative_to(project)),
            "sha256": str(row["sha256"]),
            "size_bytes": int(row["size_bytes"]),
        }
        for row in receipt["files"]
    }
    expected_kinds = {
        "RAW_OHLCV_DBN",
        "RAW_DEFINITION_DBN",
        "MGC_VOLUME_FRONT_ROLL_MAP",
        "ZN_TN_DELIVERY_SYNC_RECEIPT",
    }
    if set(acquired) != expected_kinds:
        raise MGCPowerReplayError("acquisition inventory kind drift")
    core = {
        "schema": AUDIT_SCHEMA,
        "status": "READY_FOR_FROZEN_SINGLE_CANDIDATE_REPLAY",
        "branch_id": BRANCH_ID,
        "card_hash": card["card_hash"],
        "candidate_id": CANDIDATE_ID,
        "candidate_fingerprint": CANDIDATE_FINGERPRINT,
        "bundle_id": expected_bundle_id,
        "acquisition_receipt_hash": receipt["receipt_hash"],
        "acquired_bindings": acquired,
        "rule_snapshot": {
            "path": str(rule_path.relative_to(project)),
            "sha256": rule_sha,
        },
        "chronological_roles": card["chronological_roles"],
        "power_thresholds": POWER_THRESHOLDS,
        "q4_rows": 0,
        "outcomes_read": 0,
        "network_requests": 0,
        "data_purchases": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_state_writes": 0,
    }
    return {**core, "audit_hash": stable_hash(core)}


def run_power_replay(
    root: str | Path,
    *,
    card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Run the single unchanged candidate after the sealed receipt reconciles."""

    started = time.perf_counter()
    project = Path(root).resolve()
    audit = audit_replay_inputs(project, card_path=card_path)
    card = load_replay_card(_inside(project, card_path))
    sources, target, raw_target, roll_map, source_roll_days, decode_audit = (
        _load_governed_market_inputs(project, audit)
    )
    source, sign_audit = shared.build_source_composite(
        sources,
        prior_sessions=int(card["causal_contract"]["normalization_prior_true_sessions"]),
        rv_minutes=int(card["causal_contract"]["source_rv_minutes"]),
    )
    if not bool(sign_audit["passed"]):
        raise MGCPowerReplayError("source-sign flip invariance failed")
    target_features = shared.build_target_features(
        target,
        prior_sessions=int(card["causal_contract"]["normalization_prior_true_sessions"]),
    )
    calendars, coverage = shared.build_true_session_calendars(
        raw_target,
        source,
        roll_map=roll_map,
        source_roll_days=source_roll_days,
        card=card,
    )
    proposal, target_for_events, triggers_by_control, permutation_audit = (
        build_frozen_candidate_trigger_sets(
            source,
            target_features,
            card=card,
            source_roll_days=source_roll_days,
            target_roll_days=shared._target_roll_days(roll_map, "MGC"),  # noqa: SLF001
        )
    )
    trigger_power = trigger_power_preflight(triggers_by_control[PRIMARY], card=card)
    materialized = materialize_after_trigger_power(
        proposal,
        target_for_events,
        triggers_by_control,
        power=trigger_power,
        card=card,
    )
    if materialized is None:
        return _underpowered_result(
            started=started,
            project=project,
            card_path=card_path,
            card=card,
            audit=audit,
            sign_audit=sign_audit,
            decode_audit=decode_audit,
            calendars=calendars,
            coverage=coverage,
            power=trigger_power,
            permutation_audit=permutation_audit,
            trigger_counts={
                control: len(rows)
                for control, rows in sorted(triggers_by_control.items())
            },
        )
    event_sets, construction = materialized
    executable_power = shared._power_preflight(  # noqa: SLF001
        [proposal], {CANDIDATE_ID: event_sets}, card
    )
    power = {
        "passed": bool(trigger_power["passed"]) and bool(executable_power["passed"]),
        "causal_trigger_preflight": trigger_power,
        "executable_event_preflight": executable_power,
        "thresholds": dict(POWER_THRESHOLDS),
    }
    control_power = control_power_audit(event_sets, card=card)
    candidate: dict[str, Any] | None = None
    rule_receipt: dict[str, Any] | None = None
    exact_account_replay_started = False
    if should_run_exact_account_replay(power, control_power):
        rules, rule_receipt = exact._load_rule_snapshot(  # noqa: SLF001
            _inside(project, card["frozen_inputs"]["rule_snapshot_path"])
        )
        exact_account_replay_started = True
        candidate = shared.evaluate_candidate(
            proposal,
            event_sets,
            calendars=calendars,
            coverage=coverage,
            rules=rules,
            card=card,
        )
    branch_gate = power_replay_gate(
        candidate, power=power, control_power=control_power, card=card
    )
    status = str(branch_gate["status"])
    core = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "decision": status,
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_POWER_REPLAY_ONLY",
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
        "tier_q_status": None,
        "xfa_status": None,
        "independent_confirmation_claimed": False,
        "source_bindings": {
            "audit": audit,
            "rule_snapshot": rule_receipt,
            "decision_card_path": str(_inside(project, card_path).relative_to(project)),
            "decision_card_hash": card["card_hash"],
        },
        "integrity": {
            "candidate_count": 1,
            "candidate_id": CANDIDATE_ID,
            "candidate_fingerprint": CANDIDATE_FINGERPRINT,
            "parameter_mutation_count": 0,
            "discovery_reselection_count": 0,
            "source_sign_flip_invariance": sign_audit,
            "decision_bar_fill_count": int(construction["decision_bar_fill_count"]),
            "roll_unsafe_trade_count": int(construction["roll_unsafe_trade_count"]),
            "future_outcome_decision_field_count": 0,
            "q4_row_count": 0,
            "decode_audit": decode_audit,
            "source_magnitude_permutation": permutation_audit,
        },
        "event_construction": construction,
        "event_counts": {
            control: len(rows) for control, rows in sorted(event_sets.items())
        },
        "role_calendar_counts": {
            role: len(calendars[role]["MGC"]) for role in ROLES
        },
        "coverage": coverage,
        "power_preflight": power,
        "control_power": control_power,
        "candidate_result": candidate,
        "exact_account_replay_started": exact_account_replay_started,
        "branch_gate": branch_gate,
        "runtime_seconds": time.perf_counter() - started,
        "q4_access_count_delta": 0,
        "protected_data_access_count_delta": 0,
        "network_requests": 0,
        "data_purchase_count": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_mission_writes": 0,
        "next_action": card["next_branch_rule"][_next_rule_key(status)],
    }
    return {**core, "result_hash": stable_hash(core)}


def _underpowered_result(
    *,
    started: float,
    project: Path,
    card_path: str | Path,
    card: Mapping[str, Any],
    audit: Mapping[str, Any],
    sign_audit: Mapping[str, Any],
    decode_audit: Mapping[str, Any],
    calendars: Mapping[str, Mapping[str, Sequence[int]]],
    coverage: Mapping[str, Any],
    power: Mapping[str, Any],
    permutation_audit: Mapping[str, Any],
    trigger_counts: Mapping[str, int],
) -> dict[str, Any]:
    status = "MGC_POWER_REPLAY_UNDERPOWERED_NO_THRESHOLD_RELAXATION"
    branch_gate = power_replay_gate(
        None, power=power, control_power={"passed": False}, card=card
    )
    core = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "decision": status,
        "evidence_role": "CAUSAL_TRIGGER_POWER_PREFLIGHT_ONLY_NO_OUTCOMES",
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
        "tier_q_status": None,
        "xfa_status": None,
        "independent_confirmation_claimed": False,
        "source_bindings": {
            "audit": audit,
            "rule_snapshot": None,
            "decision_card_path": str(
                _inside(project, card_path).relative_to(project)
            ),
            "decision_card_hash": card["card_hash"],
        },
        "integrity": {
            "candidate_count": 1,
            "candidate_id": CANDIDATE_ID,
            "candidate_fingerprint": CANDIDATE_FINGERPRINT,
            "parameter_mutation_count": 0,
            "discovery_reselection_count": 0,
            "source_sign_flip_invariance": sign_audit,
            "source_magnitude_permutation": permutation_audit,
            "future_outcome_decision_field_count": 0,
            "future_outcome_rows_read": 0,
            "q4_row_count": 0,
            "decode_audit": decode_audit,
        },
        "causal_trigger_counts": dict(trigger_counts),
        "event_construction": None,
        "event_counts": {},
        "role_calendar_counts": {
            role: len(calendars[role]["MGC"]) for role in ROLES
        },
        "coverage": coverage,
        "power_preflight": power,
        "control_power": {
            "passed": False,
            "status": "NOT_RUN_BECAUSE_TRIGGER_POWER_FAILED",
        },
        "candidate_result": None,
        "exact_account_replay_started": False,
        "branch_gate": branch_gate,
        "runtime_seconds": time.perf_counter() - started,
        "q4_access_count_delta": 0,
        "protected_data_access_count_delta": 0,
        "network_requests": 0,
        "data_purchase_count": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_mission_writes": 0,
        "next_action": card["next_branch_rule"]["when_underpowered"],
    }
    return {**core, "result_hash": stable_hash(core)}


def build_frozen_candidate_trigger_sets(
    source: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    card: Mapping[str, Any],
    source_roll_days: set[int],
    target_roll_days: set[int],
) -> tuple[
    dict[str, Any],
    pd.DataFrame,
    dict[str, pd.DataFrame],
    dict[str, Any],
]:
    """Build causal decision triggers without reading any future trade outcome."""

    causal = card["causal_contract"]
    mechanism = dict(causal["mechanism"])
    proposal = _frozen_proposal(mechanism)
    target = targets.loc[targets["symbol"].eq("MGC")].copy()
    target = target.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    merged = target.merge(
        source,
        on=["timestamp", "session_day", "local_minute"],
        how="inner",
        validate="one_to_one",
    )
    merged = shared._add_shifted_source_controls(merged)  # noqa: SLF001
    merged, permutation_audit = _attach_role_hashed_magnitude_permutation(
        merged, card=card
    )
    lower, upper = map(shared._clock_minutes, causal["session_roles_chicago"]["OPEN"])  # noqa: SLF001
    triggers_by_control: dict[str, pd.DataFrame] = {}
    guarded = set(source_roll_days) | set(target_roll_days)
    for control, score_column in (
        (PRIMARY, "rates_vol_score"),
        ("SOURCE_SHIFT_5_TRUE_SESSIONS", "rates_vol_score_shift5"),
        ("SOURCE_MAGNITUDE_PERMUTATION", "rates_vol_score_role_permutation"),
        ("TARGET_ONLY_DUTY_MATCHED_OCO", None),
    ):
        triggers_by_control[control] = shared._candidate_triggers(  # noqa: SLF001
            merged,
            mechanism=mechanism,
            score_column=score_column,
            clock_start=lower,
            clock_end=upper,
            source_roll_days=guarded,
        )
    triggers_by_control["SESSION_TIME_DIRECTION_EXPOSURE_MATCHED_PLACEBO"] = (
        _session_time_placebo_trigger_pool(
            merged,
            mechanism=mechanism,
            clock_start=lower,
            clock_end=upper,
            source_roll_days=guarded,
        )
    )
    return proposal, target, triggers_by_control, permutation_audit


def trigger_power_preflight(
    primary_triggers: pd.DataFrame, *, card: Mapping[str, Any]
) -> dict[str, Any]:
    """Fail cheaply on a causal independent-trigger upper bound.

    The count uses no entry, exit, PnL, MLL, or future target path.  It is an
    optimistic upper bound because a prior open position can only reduce the
    number of executable events.  Therefore failure is decisive for power,
    while success only authorizes event materialization.
    """

    independent = _consolidate_causal_triggers(primary_triggers, card=card)
    counts: dict[str, int] = {}
    checks: dict[str, bool] = {}
    for role in ROLES:
        lower, upper = map(shared._day_int, card["chronological_roles"][role])  # noqa: SLF001
        count = sum(
            lower <= int(row.session_day) < upper
            for row in independent.itertuples(index=False)
        )
        counts[role] = count
        checks[role] = count >= int(POWER_THRESHOLDS[role])
    return {
        "passed": all(checks.values()),
        "count_semantics": "CAUSAL_INDEPENDENT_TRIGGER_UPPER_BOUND_NO_FUTURE_OUTCOME",
        "event_counts": counts,
        "checks": checks,
        "thresholds": dict(POWER_THRESHOLDS),
        "raw_trigger_count": len(primary_triggers),
        "independent_trigger_count": len(independent),
        "outcomes_read": 0,
    }


def materialize_frozen_candidate_event_sets(
    proposal: Mapping[str, Any],
    target: pd.DataFrame,
    triggers_by_control: Mapping[str, pd.DataFrame],
    *,
    card: Mapping[str, Any],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], dict[str, int]]:
    """Materialize outcomes only after the causal trigger-power gate passes."""

    causal = card["causal_contract"]
    mechanism = dict(causal["mechanism"])
    counters: Counter[str] = Counter()
    raw: dict[str, tuple[dict[str, Any], ...]] = {}
    for control in (PRIMARY, *CONTROLS):
        triggers = triggers_by_control[control]
        events, local = shared._materialize_trigger_set(  # noqa: SLF001
            triggers,
            target,
            proposal=proposal,
            mechanism=mechanism,
            causal=causal,
            control=control,
        )
        raw[control] = events
        counters.update(local)
    output = {PRIMARY: raw[PRIMARY]}
    used_control_timestamps: set[tuple[int, int]] = set()
    for control in CONTROLS:
        output[control] = _match_control_events_role_local(
            raw[PRIMARY],
            raw[control],
            used=used_control_timestamps,
            card=card,
            session_placebo=(
                control == "SESSION_TIME_DIRECTION_EXPOSURE_MATCHED_PLACEBO"
            ),
        )
    counters["decision_bar_fill_count"] = 0
    counters["roll_unsafe_trade_count"] = 0
    return output, dict(sorted(counters.items()))


def materialize_after_trigger_power(
    proposal: Mapping[str, Any],
    target: pd.DataFrame,
    triggers_by_control: Mapping[str, pd.DataFrame],
    *,
    power: Mapping[str, Any],
    card: Mapping[str, Any],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], dict[str, int]] | None:
    """The sole outcome-materialization gate, kept explicit for audit/tests."""

    if not bool(power.get("passed")):
        return None
    return materialize_frozen_candidate_event_sets(
        proposal, target, triggers_by_control, card=card
    )


def _attach_role_hashed_magnitude_permutation(
    frame: pd.DataFrame, *, card: Mapping[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Attach a reproducible role-local null without touching the primary lane."""

    output = frame.copy()
    output["rates_vol_score_role_permutation"] = float("nan")
    role_audit: dict[str, Any] = {}
    for role in ROLES:
        lower, upper = map(shared._day_int, card["chronological_roles"][role])  # noqa: SLF001
        mask = (
            output["session_day"].between(lower, upper - 1)
            & output["rates_vol_score"].notna()
        )
        role_frame = output.loc[mask, ["timestamp", "rates_vol_score"]].sort_values(
            "timestamp", kind="mergesort"
        )
        destinations = role_frame.index.to_list()
        count = len(destinations)
        seed_bytes = hashlib.sha256(
            (
                f"{card['card_hash']}|SOURCE_MAGNITUDE_PERMUTATION|{role}"
            ).encode("utf-8")
        ).digest()
        seed = int.from_bytes(seed_bytes, "big")
        if count <= 1:
            donor_positions = list(range(count))
        elif count == 2:
            donor_positions = [1, 0]
        else:
            step = 2 + seed % (count - 2)
            while math.gcd(step, count) != 1 or step == 1:
                step += 1
                if step >= count:
                    step = 2
            offset = (seed >> 64) % count
            donor_positions = [
                int((offset + step * position) % count)
                for position in range(count)
            ]
            if donor_positions == list(range(count)):
                donor_positions = donor_positions[1:] + donor_positions[:1]
        donors = [destinations[position] for position in donor_positions]
        output.loc[destinations, "rates_vol_score_role_permutation"] = (
            output.loc[donors, "rates_vol_score"].to_numpy()
        )
        destination_ns = output.loc[destinations, "timestamp"].astype("int64").to_numpy()
        donor_ns = output.loc[donors, "timestamp"].astype("int64").to_numpy()
        mapping_digest = hashlib.sha256()
        mapping_digest.update(destination_ns.tobytes(order="C"))
        mapping_digest.update(donor_ns.tobytes(order="C"))
        role_audit[role] = {
            "observation_count": count,
            "mapping_hash": mapping_digest.hexdigest(),
            "fixed_point_count": int((destination_ns == donor_ns).sum()),
            "sha256_seed": seed_bytes.hex(),
            "affine_step": None if count <= 2 else step,
            "affine_offset": None if count <= 2 else offset,
            "role_local": True,
            "economic_outcome_fields_used": 0,
            "deployable": False,
        }
    return output, {
        "policy": "NONDEPLOYABLE_ROLE_LOCAL_SHA256_MAGNITUDE_PERMUTATION_NULL",
        "roles": role_audit,
        "permutation_hash": stable_hash(role_audit),
        "primary_column_modified": False,
        "outcomes_read": 0,
    }


def _consolidate_causal_triggers(
    triggers: pd.DataFrame, *, card: Mapping[str, Any]
) -> pd.DataFrame:
    causal = card["causal_contract"]
    reset = pd.Timedelta(minutes=int(causal["event_reset_minutes"]))
    maximum = int(causal["maximum_events_per_session"])
    kept: list[int] = []
    per_day: Counter[int] = Counter()
    last_decision: pd.Timestamp | None = None
    for index, row in triggers.sort_values("timestamp", kind="mergesort").iterrows():
        decision = pd.Timestamp(row["timestamp"]) + pd.Timedelta(minutes=1)
        day = int(row["session_day"])
        if per_day[day] >= maximum:
            continue
        if last_decision is not None and decision - last_decision < reset:
            continue
        kept.append(int(index))
        per_day[day] += 1
        last_decision = decision
    return triggers.loc[kept].sort_values("timestamp", kind="mergesort").copy()


def _session_time_placebo_trigger_pool(
    merged: pd.DataFrame,
    *,
    mechanism: Mapping[str, Any],
    clock_start: int,
    clock_end: int,
    source_roll_days: set[int],
) -> pd.DataFrame:
    """Target-only pool consumed only at a frozen cyclic +45 minute clock."""

    pool = shared._candidate_triggers(  # noqa: SLF001
        merged,
        mechanism=mechanism,
        score_column=None,
        clock_start=clock_start,
        clock_end=clock_end,
        source_roll_days=source_roll_days,
    ).copy()
    pool["control_clock_policy"] = (
        "ROLE_LOCAL_PLUS_45_MINUTE_CYCLIC_WITH_15_MINUTE_TOLERANCE"
    )
    return pool


def _cyclic_session_placebo_minute(
    primary_minute: int, card: Mapping[str, Any]
) -> int:
    lower, upper = map(
        shared._clock_minutes,  # noqa: SLF001
        card["causal_contract"]["session_roles_chicago"]["OPEN"],
    )
    width = upper - lower
    if width <= SESSION_PLACEBO_OFFSET_MINUTES:
        raise MGCPowerReplayError("session placebo clock width drift")
    return lower + (
        (int(primary_minute) - lower + SESSION_PLACEBO_OFFSET_MINUTES) % width
    )


def control_power_audit(
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    card: Mapping[str, Any],
) -> dict[str, Any]:
    """Require role-local one-for-one controls; unmatched controls cannot win."""

    roles = tuple(card["control_power_contract"]["roles"])
    role_results: dict[str, Any] = {}
    passed = True
    for role in roles:
        lower, upper = map(shared._day_int, card["chronological_roles"][role])  # noqa: SLF001
        primary_count = sum(
            lower <= int(row["session_day"]) < upper for row in event_sets[PRIMARY]
        )
        primary_ids = {
            str(row["event_id"])
            for row in event_sets[PRIMARY]
            if lower <= int(row["session_day"]) < upper
        }
        control_counts: dict[str, int] = {}
        checks: dict[str, bool] = {}
        matched_primary_ids: dict[str, list[str]] = {}
        event_fingerprints: dict[str, list[str]] = {}
        for control in CONTROLS:
            control_rows = [
                row
                for row in event_sets[control]
                if lower <= int(row["session_day"]) < upper
            ]
            matched = [
                str(row.get("matched_primary_event_id") or "")
                for row in control_rows
            ]
            control_counts[control] = len(control_rows)
            matched_primary_ids[control] = sorted(matched)
            event_fingerprints[control] = sorted(
                _control_event_fingerprint(row) for row in control_rows
            )
            checks[control] = (
                len(control_rows) == primary_count
                and len(set(matched)) == len(matched)
                and set(matched) == primary_ids
                and all(_event_role(row, card) == role for row in control_rows)
            )
        clone_pairs: list[list[str]] = []
        for index, left in enumerate(CONTROLS):
            left_set = set(event_fingerprints[left])
            for right in CONTROLS[index + 1 :]:
                if left_set & set(event_fingerprints[right]):
                    clone_pairs.append([left, right])
        anti_clone_passed = not clone_pairs
        checks["anti_clone_event_fingerprints"] = anti_clone_passed
        passed = passed and all(checks.values())
        role_results[role] = {
            "primary_count": primary_count,
            "control_counts": control_counts,
            "exact_cardinality_checks": checks,
            "matched_primary_ids": matched_primary_ids,
            "event_fingerprints": event_fingerprints,
            "clone_pairs": clone_pairs,
        }
    return {
        "passed": passed,
        "policy": "EXACT_ROLE_LOCAL_PRIMARY_CARDINALITY_REQUIRED",
        "roles": role_results,
    }


def _control_event_fingerprint(row: Mapping[str, Any]) -> str:
    return stable_hash(
        {
            "matched_primary_event_id": str(row.get("matched_primary_event_id") or ""),
            "decision_ns": int(row["decision_ns"]),
            "entry_ns": int(row["entry_ns"]),
            "exit_ns": int(row["exit_ns"]),
            "side": int(row["side"]),
            "entry_price": float(row["entry_price"]),
            "exit_price": float(row["exit_price"]),
            "target_contract": str(row["target_contract"]),
        }
    )


def _match_control_events_role_local(
    primary: Sequence[Mapping[str, Any]],
    pool: Sequence[Mapping[str, Any]],
    *,
    used: set[tuple[int, int]],
    card: Mapping[str, Any],
    session_placebo: bool = False,
) -> tuple[dict[str, Any], ...]:
    """One-to-one causal-null matching that never crosses evidence roles."""

    available = [dict(row) for row in pool]
    matched: list[dict[str, Any]] = []
    for real in primary:
        role = _event_role(real, card)
        expected_placebo_minute = _cyclic_session_placebo_minute(
            int(real["local_minute"]), card
        )
        candidates = [
            row
            for row in available
            if _event_role(row, card) == role
            and row["block"] == real["block"]
            and row["session_role"] == real["session_role"]
            and int(row["side"]) == int(real["side"])
            and int(row["decision_ns"]) != int(real["decision_ns"])
            and (
                abs(int(row["local_minute"]) - expected_placebo_minute)
                <= SESSION_PLACEBO_TOLERANCE_MINUTES
                if session_placebo
                else abs(int(row["local_minute"]) - int(real["local_minute"]))
                <= 60
            )
            and (int(row["decision_ns"]), int(row["side"])) not in used
        ]
        if not candidates:
            continue
        chosen = min(
            candidates,
            key=lambda row: (
                abs(
                    int(row["local_minute"])
                    - (
                        expected_placebo_minute
                        if session_placebo
                        else int(real["local_minute"])
                    )
                ),
                abs(int(row["session_day"]) - int(real["session_day"])),
                str(row["event_id"]),
            ),
        )
        chosen["matched_primary_event_id"] = str(real["event_id"])
        chosen["matched_evidence_role"] = role
        chosen["session_placebo_offset_minutes"] = (
            SESSION_PLACEBO_OFFSET_MINUTES if session_placebo else None
        )
        matched.append(chosen)
        available.remove(chosen)
        used.add((int(chosen["decision_ns"]), int(chosen["side"])))
    return tuple(matched)


def _event_role(row: Mapping[str, Any], card: Mapping[str, Any]) -> str:
    day = int(row["session_day"])
    for role in ROLES:
        lower, upper = map(shared._day_int, card["chronological_roles"][role])  # noqa: SLF001
        if lower <= day < upper:
            return role
    return "OUTSIDE_ECONOMIC_ROLES"


def power_replay_gate(
    candidate: Mapping[str, Any] | None,
    *,
    power: Mapping[str, Any],
    control_power: Mapping[str, Any],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply the frozen complete single-candidate gate without promotion."""

    if not bool(power.get("passed")):
        return {
            "passed": False,
            "status": "MGC_POWER_REPLAY_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
            "checks": {"power": False},
        }
    if not bool(control_power.get("passed")):
        return {
            "passed": False,
            "status": "MGC_POWER_REPLAY_CONTROL_UNDERPOWERED",
            "checks": {"power": True, "control_power": False},
        }
    if candidate is None:
        raise MGCPowerReplayError(
            "powered and controlled evidence requires exact account replay"
        )
    validation = candidate["evaluations"]["VALIDATION"][PRIMARY]["stressed"]
    final_cell = candidate["evaluations"]["FINAL_DEVELOPMENT"][PRIMARY]
    normal = final_cell["normal"]
    stressed = final_cell["stressed"]
    gate = card["frozen_gate"]
    checks = {
        "power": True,
        "control_power": True,
        "positive_validation_stressed": float(validation["net_total_usd"]) > 0.0,
        "positive_final_stressed": float(stressed["net_total_usd"]) > 0.0,
        "minimum_final_normal_passes": int(normal["pass_count"])
        >= int(gate["minimum_combined_final_normal_passes"]),
        "minimum_final_stressed_passes": int(stressed["pass_count"])
        >= int(gate["minimum_combined_final_stressed_passes"]),
        "candidate_complete_gate": bool(candidate["gate"]["passed"]),
        "controlled_mll": float(stressed["mll_breach_rate"])
        <= float(gate["maximum_stressed_mll_breach_rate"]),
        "passing_consistency": int(stressed["pass_count"]) == 0
        or bool(stressed["all_passing_paths_consistency_compliant"]),
    }
    if all(checks.values()):
        status = "MGC_RATES_TARGET_VOL_GREEN_TIER_E_DIAGNOSTIC"
    elif (
        float(validation["net_total_usd"]) > 0.0
        or float(stressed["net_total_usd"]) > 0.0
        or int(normal["pass_count"]) > 0
        or int(stressed["pass_count"]) > 0
    ):
        status = "MGC_RATES_TARGET_VOL_WEAK_DIAGNOSTIC"
    else:
        status = "MGC_RATES_TARGET_VOL_FALSIFIED"
    return {
        "passed": status == "MGC_RATES_TARGET_VOL_GREEN_TIER_E_DIAGNOSTIC",
        "status": status,
        "checks": checks,
        "final_normal_passes": int(normal["pass_count"]),
        "final_stressed_passes": int(stressed["pass_count"]),
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
        "xfa_status": None,
    }


def should_run_exact_account_replay(
    power: Mapping[str, Any], control_power: Mapping[str, Any]
) -> bool:
    """The exact account engine is unreachable until both cheap gates pass."""

    return bool(power.get("passed")) and bool(control_power.get("passed"))


def persist_replay_artifacts(
    root: str | Path,
    result: Mapping[str, Any],
    *,
    output_root: str | Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    project = Path(root).resolve()
    folder = _inside(project, output_root)
    authorized_literal = project / DEFAULT_OUTPUT
    authorized_folder = authorized_literal.resolve()
    if folder != authorized_folder or authorized_folder != authorized_literal:
        raise MGCPowerReplayError(
            "output path is outside the frozen diagnostic artifact root"
        )
    folder.mkdir(parents=True, exist_ok=True)
    result_path = folder / "economic_result.json"
    report_path = folder / "decision_report.json"
    receipt_path = folder / "evidence_receipt.json"
    report_core = {
        "schema": "hydra_mgc_rates_target_vol_power_replay_report_v1",
        "decision": result["decision"],
        "candidate_id": CANDIDATE_ID,
        "power_preflight": result["power_preflight"],
        "control_power": result["control_power"],
        "event_counts": result["event_counts"],
        "candidate_result": result["candidate_result"],
        "branch_gate": result["branch_gate"],
        "runtime_seconds": result["runtime_seconds"],
        "next_action": result["next_action"],
    }
    report = {**report_core, "report_hash": stable_hash(report_core)}
    _persist_json_once(result_path, result)
    _persist_json_once(report_path, report)
    receipt_core = {
        "schema": "hydra_mgc_rates_target_vol_power_replay_evidence_receipt_v1",
        "candidate_id": CANDIDATE_ID,
        "candidate_fingerprint": CANDIDATE_FINGERPRINT,
        "result_path": str(result_path.relative_to(project)),
        "result_sha256": sha256_file(result_path),
        "result_hash": result["result_hash"],
        "report_path": str(report_path.relative_to(project)),
        "report_sha256": sha256_file(report_path),
        "report_hash": report["report_hash"],
        "tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
        "xfa_status": None,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
    }
    receipt = {**receipt_core, "receipt_hash": stable_hash(receipt_core)}
    _persist_json_once(receipt_path, receipt)
    return {
        "result_path": str(result_path.relative_to(project)),
        "result_sha256": sha256_file(result_path),
        "report_path": str(report_path.relative_to(project)),
        "report_sha256": sha256_file(report_path),
        "receipt_path": str(receipt_path.relative_to(project)),
        "receipt_sha256": sha256_file(receipt_path),
        "receipt_hash": receipt["receipt_hash"],
    }


def _load_governed_market_inputs(
    project: Path,
    audit: Mapping[str, Any],
) -> tuple[
    dict[str, pd.DataFrame],
    pd.DataFrame,
    pd.DataFrame,
    RollMap,
    set[int],
    dict[str, Any],
]:
    acquired = audit["acquired_bindings"]
    raw_path = project / acquired["RAW_OHLCV_DBN"]["path"]
    frame = (
        _import_databento()
        .DBNStore.from_file(raw_path)
        .to_df(pretty_ts=True, map_symbols=False, price_type="float")
        .reset_index()
        .rename(columns={"ts_event": "timestamp"})
    )
    required = {"timestamp", "instrument_id", "open", "high", "low", "close", "volume"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise MGCPowerReplayError(f"OHLCV DBN columns missing: {missing}")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    if (
        frame.empty
        or frame["timestamp"].min() < pd.Timestamp("2015-09-01", tz="UTC")
        or frame["timestamp"].max() >= pd.Timestamp("2021-11-04", tz="UTC")
    ):
        raise MGCPowerReplayError("governed DBN temporal boundary drift")
    mgc_payload = _read_self_hashed_json(
        project / acquired["MGC_VOLUME_FRONT_ROLL_MAP"]["path"], "roll_map_hash"
    )
    sync = _read_self_hashed_json(
        project / acquired["ZN_TN_DELIVERY_SYNC_RECEIPT"]["path"], "receipt_hash"
    )
    roll_map = _roll_map_from_payload(mgc_payload)
    allowed_instrument_ids = {
        str(item.instrument_id) for item in roll_map.contracts
    } | {
        str(item["instrument_id"])
        for market in ("ZN", "TN")
        for item in sync["root_rolls"][market]["contracts"]
    }
    observed_instrument_ids = set(frame["instrument_id"].astype(str))
    if observed_instrument_ids - allowed_instrument_ids:
        raise MGCPowerReplayError("OHLCV DBN contains an unbound instrument ID")
    target = _map_mgc_rows(frame, roll_map)
    sources = {
        market: _map_treasury_rows(frame, sync, market)
        for market in ("ZN", "TN")
    }
    source_roll_days = _source_roll_guard_days(sources)
    target_roll_days = shared._target_roll_days(roll_map, "MGC")  # noqa: SLF001
    target["unsafe_roll_window"] = target["session_day"].isin(target_roll_days)
    raw_target = target[["timestamp", "symbol"]].copy()
    duplicates = {
        market: int(value["timestamp"].duplicated().sum())
        for market, value in {**sources, "MGC": target}.items()
    }
    if any(duplicates.values()):
        raise MGCPowerReplayError("mapped market data contain duplicate timestamps")
    decode = {
        "raw_dbn_sha256": sha256_file(raw_path),
        "raw_row_count": len(frame),
        "mapped_row_counts": {
            "ZN": len(sources["ZN"]),
            "TN": len(sources["TN"]),
            "MGC": len(target),
        },
        "duplicate_timestamp_counts": duplicates,
        "source_delivery_mismatch_intervals_excluded": int(
            sync["delivery_mismatch_count"]
        ),
        "source_roll_guard_day_count": len(source_roll_days),
        "target_roll_guard_day_count": len(target_roll_days),
        "q4_row_count": 0,
    }
    return sources, target, raw_target, roll_map, source_roll_days, decode


def _map_mgc_rows(frame: pd.DataFrame, roll_map: RollMap) -> pd.DataFrame:
    instrument_ids = {str(item.instrument_id) for item in roll_map.contracts}
    work = frame.loc[frame["instrument_id"].astype(str).isin(instrument_ids)].copy()
    dates = work["timestamp"].dt.strftime("%Y-%m-%d")
    actual = work["instrument_id"].astype(str)
    assigned = pd.Series(False, index=work.index)
    expected = pd.Series("", index=work.index, dtype=object)
    contract = pd.Series("", index=work.index, dtype=object)
    segment = pd.Series("", index=work.index, dtype=object)
    for item in roll_map.contracts:
        mask = dates.ge(item.active_start[:10]) & dates.lt(item.active_end[:10])
        if (assigned & mask).any():
            raise MGCPowerReplayError("MGC roll intervals overlap")
        assigned |= mask
        expected.loc[mask] = str(item.instrument_id)
        contract.loc[mask] = item.contract
        segment.loc[mask] = f"MGC:{item.active_start}:{item.active_end}:{item.instrument_id}"
    selected = work.loc[assigned].copy()
    if (
        selected.empty
        or (~assigned).any()
        or actual.loc[assigned].ne(expected.loc[assigned]).any()
    ):
        raise MGCPowerReplayError("MGC volume-front instrument mapping mismatch")
    selected["symbol"] = "MGC"
    selected["active_contract"] = contract.loc[assigned].to_numpy()
    selected["contract"] = selected["active_contract"]
    selected["roll_segment_id"] = segment.loc[assigned].to_numpy()
    local = selected["timestamp"].dt.tz_convert("America/Chicago")
    selected["session_day"] = local.dt.strftime("%Y%m%d").astype(int)
    selected["local_minute"] = local.dt.hour * 60 + local.dt.minute
    return selected.sort_values("timestamp", kind="mergesort").reset_index(drop=True)


def _map_treasury_rows(
    frame: pd.DataFrame, sync: Mapping[str, Any], market: str
) -> pd.DataFrame:
    id_key = f"{market.lower()}_instrument_id"
    contract_key = f"{market.lower()}_contract"
    root_contracts = sync["root_rolls"][market]["contracts"]
    instrument_ids = {str(item["instrument_id"]) for item in root_contracts}
    work = frame.loc[frame["instrument_id"].astype(str).isin(instrument_ids)].copy()
    dates = work["timestamp"].dt.strftime("%Y-%m-%d")
    actual = work["instrument_id"].astype(str)
    assigned = pd.Series(False, index=work.index)
    expected = pd.Series("", index=work.index, dtype=object)
    contract = pd.Series("", index=work.index, dtype=object)
    segment = pd.Series("", index=work.index, dtype=object)
    for item in sync["delivery_sync_intervals"]:
        mask = dates.ge(str(item["start"])[:10]) & dates.lt(str(item["end"])[:10])
        if (assigned & mask).any():
            raise MGCPowerReplayError(f"{market} synchronized intervals overlap")
        assigned |= mask
        expected.loc[mask] = str(item[id_key])
        contract.loc[mask] = str(item[contract_key])
        segment.loc[mask] = (
            f"{market}:{item['start']}:{item['end']}:{item[id_key]}"
        )
    selected = work.loc[assigned].copy()
    if selected.empty or actual.loc[assigned].ne(expected.loc[assigned]).any():
        raise MGCPowerReplayError(f"{market} synchronized delivery mapping mismatch")
    selected["symbol"] = market
    selected["contract"] = contract.loc[assigned].to_numpy()
    selected["roll_segment_id"] = segment.loc[assigned].to_numpy()
    return selected.sort_values("timestamp", kind="mergesort").reset_index(drop=True)


def _source_roll_guard_days(sources: Mapping[str, pd.DataFrame]) -> set[int]:
    all_days = sorted(
        {
            int(value)
            for frame in sources.values()
            for value in frame["timestamp"]
            .dt.tz_convert("America/Chicago")
            .dt.strftime("%Y%m%d")
            .astype(int)
        }
    )
    positions = {day: index for index, day in enumerate(all_days)}
    transitions: set[int] = set()
    for frame in sources.values():
        days = (
            frame["timestamp"]
            .dt.tz_convert("America/Chicago")
            .dt.strftime("%Y%m%d")
            .astype(int)
        )
        changed = frame["roll_segment_id"].ne(frame["roll_segment_id"].shift())
        transitions.update(int(value) for value in days[changed].iloc[1:])
    guarded: set[int] = set()
    for day in transitions:
        index = positions.get(day)
        if index is not None:
            guarded.update(all_days[max(0, index - 1) : min(len(all_days), index + 2)])
    return guarded


def _roll_map_from_payload(payload: Mapping[str, Any]) -> RollMap:
    allowed = {item.name for item in fields(ContractInfo)}
    contracts = [
        ContractInfo(**{key: value for key, value in row.items() if key in allowed})
        for row in payload["contracts"]
    ]
    value = RollMap(
        dataset=str(payload["dataset"]),
        schema=str(payload["schema"]),
        map_type=str(payload["map_type"]),
        symbols=list(payload["symbols"]),
        contracts=contracts,
        unsafe_window_days=int(payload["unsafe_window_days"]),
        notes=list(payload["notes"]),
        source_metadata=dict(payload.get("source_metadata") or {}),
    )
    if (
        value.map_type != acquisition.MGC_MAP_TYPE
        or value.symbols != ["MGC"]
        or value.roll_map_hash() != payload["roll_map_hash"]
    ):
        raise MGCPowerReplayError("MGC roll-map semantic drift")
    return value


def _validate_receipt_semantics(
    receipt: Mapping[str, Any], *, expected_bundle_id: str
) -> None:
    if (
        receipt.get("schema") != acquisition.RECEIPT_SCHEMA
        or receipt.get("bundle_id") != expected_bundle_id
        or receipt.get("candidate_ids") != [CANDIDATE_ID]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("outcomes_read") != 0
        or receipt.get("economic_replay_started") is not False
        or receipt.get("promotion_changes") != 0
        or receipt.get("q4_access_count_delta") != 0
        or receipt.get("protected_data_access_count_delta") != 0
        or receipt.get("broker_connections") != 0
        or receipt.get("orders") != 0
        or receipt.get("power_thresholds") != POWER_THRESHOLDS
        or receipt.get("official_total_cost_usd")
        != acquisition.EXPECTED_TOTAL_COST_USD
    ):
        raise MGCPowerReplayError("acquisition receipt semantic drift")


def _frozen_proposal(mechanism: Mapping[str, Any]) -> dict[str, Any]:
    fingerprint = stable_hash(
        {
            "branch": PARENT_BRANCH,
            "market": "MGC",
            "session_role": "OPEN",
            "mechanism": dict(mechanism),
            "fill": "FROZEN_OCO_NEXT_TRADABLE_BAR",
            "entry_ambiguity": "ABSTAIN",
            "exit_ambiguity": "STOP_FIRST",
        }
    )
    if fingerprint != CANDIDATE_FINGERPRINT:
        raise MGCPowerReplayError("frozen candidate fingerprint drift")
    return {
        "candidate_id": CANDIDATE_ID,
        "mechanism": "RATES_TARGET_VOL_GAP_OCO",
        "source_markets": ["ZN", "TN"],
        "execution_market": "MGC",
        "session_role": "OPEN",
        "structural_fingerprint": fingerprint,
    }


def _frozen_mechanism() -> dict[str, Any]:
    return {
        "mechanism": "RATES_TARGET_VOL_GAP_OCO",
        "source_score_minimum": 1.5,
        "source_target_score_gap_minimum": 1.5,
        "target_range_quantile": None,
        "target_vol_quantile": 0.5,
        "oco_lookback_minutes": 30,
        "oco_valid_minutes": 30,
        "stop_range_fraction": 0.5,
        "minimum_stop_ticks": 4,
        "target_r_multiple": 3.0,
        "maximum_holding_minutes": 60,
    }


def _validate_gate_contract(card: Mapping[str, Any]) -> None:
    if card.get("frozen_gate") != {
        "minimum_distinct_positive_targets": 1,
        "positive_stressed_validation_net_required": True,
        "positive_stressed_final_development_net_required": True,
        "minimum_combined_final_normal_passes": 2,
        "minimum_combined_final_stressed_passes": 1,
        "minimum_positive_quarter_or_market_contexts": 2,
        "maximum_stressed_mll_breach_rate": 0.1,
        "passing_consistency_required": True,
        "minimum_final_stressed_target_progress_p25": 0.0,
        "minimum_median_target_progress_uplift_over_each_control": 0.1,
        "one_extra_pass_can_replace_progress_uplift": True,
        "maximum_single_trade_profit_concentration": 0.5,
        "evidence_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
    }:
        raise MGCPowerReplayError("frozen economic gate drift")
    if card.get("control_power_contract") != {
        "exact_role_local_primary_cardinality_required": True,
        "roles": ["VALIDATION", "FINAL_DEVELOPMENT"],
        "when_unmatched": "MGC_POWER_REPLAY_CONTROL_UNDERPOWERED",
    }:
        raise MGCPowerReplayError("control-power contract drift")
    if card.get("control_implementation_contract") != {
        "TARGET_ONLY_DUTY_MATCHED_OCO": "TARGET_ONLY_POOL_ROLE_LOCAL_ONE_TO_ONE_MATCH",
        "SOURCE_SHIFT_5_TRUE_SESSIONS": "CAUSAL_SAME_CLOCK_LAG_5_TRUE_OBSERVATIONS",
        "SESSION_TIME_DIRECTION_EXPOSURE_MATCHED_PLACEBO": "ROLE_LOCAL_PLUS_45_MINUTE_CYCLIC_TARGET_ONLY_PLACEBO_WITH_15_MINUTE_TOLERANCE",
        "SOURCE_MAGNITUDE_PERMUTATION": "NONDEPLOYABLE_ROLE_LOCAL_SHA256_MAGNITUDE_PERMUTATION_NULL",
        "target_only_and_session_placebo_may_share_decision_timestamp": False,
        "all_control_event_timestamps_disjoint": True,
        "cross_role_matching_allowed": False,
    }:
        raise MGCPowerReplayError("control implementation contract drift")


def _next_rule_key(status: str) -> str:
    if status == "MGC_RATES_TARGET_VOL_GREEN_TIER_E_DIAGNOSTIC":
        return "when_green"
    if status == "MGC_RATES_TARGET_VOL_WEAK_DIAGNOSTIC":
        return "when_weak"
    if status == "MGC_POWER_REPLAY_UNDERPOWERED_NO_THRESHOLD_RELAXATION":
        return "when_underpowered"
    if status == "MGC_POWER_REPLAY_CONTROL_UNDERPOWERED":
        return "when_control_underpowered"
    return "when_falsified"


def _read_self_hashed_json(path: Path, hash_key: str) -> dict[str, Any]:
    payload = _read_json(path)
    core = dict(payload)
    claimed = str(core.pop(hash_key, ""))
    if not claimed or stable_hash(core) != claimed:
        raise MGCPowerReplayError(f"self-hash drift: {path.name}")
    return payload


def _receipt_path(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise MGCPowerReplayError("receipt artifact escapes project root") from exc
    return resolved


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise MGCPowerReplayError("path escapes project root") from exc
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MGCPowerReplayError(f"invalid JSON object: {path}") from exc
    if not isinstance(value, dict):
        raise MGCPowerReplayError(f"JSON object required: {path}")
    return value


def _persist_json_once(path: Path, value: Mapping[str, Any]) -> None:
    payload = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != payload:
            raise MGCPowerReplayError(f"refusing divergent artifact rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(payload, encoding="utf-8")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
