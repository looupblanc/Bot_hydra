"""Causal intraday cross-index dispersion/residual development tripwire.

This bounded branch reuses the immutable ES/NQ/RTY/YM feature matrices and the
canonical next-tradable-open outcome/account kernels.  Eight cells are frozen:
leader continuation or laggard convergence, 5m or 15m, OPEN or MID.  Results
are development-only; this module cannot access Q4, buy data, connect a broker,
place orders, or write the authoritative mission databases/registries.
"""

from __future__ import annotations

import hashlib
import json
import statistics
import time
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import stable_hash
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.production import autonomous_exact_replay as exact
from hydra.production.cross_index_breadth_tripwire import (
    _causal_prior_zscore,
    _date_ns,
    _open_bound_matrix,
)
from hydra.propfirm.combine_episode import (
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.scaling_plan import mini_equivalent
from hydra.research.causal_target_velocity import (
    CalibratedHazardCandidate,
    HazardCandidate,
    HazardIntent,
    HazardOutcome,
    observe_outcomes,
)
from hydra.research.causal_target_velocity import (
    _trade_path_event as _hazard_trade_path_event,
)


SCHEMA = "hydra_intraday_cross_index_dispersion_residual_result_v1"
BRANCH_ID = "INTRADAY_CROSS_INDEX_DISPERSION_RESIDUAL_V1"
DEFAULT_MANIFEST = Path(
    "config/research/intraday_cross_index_dispersion_residual_v1.json"
)
DEFAULT_OUTPUT = Path(
    "reports/research_tripwires/intraday_cross_index_dispersion_residual_v1"
)
STATE_MARKETS = ("ES", "NQ", "RTY", "YM")
EXECUTION_MARKETS = {"ES": "MES", "NQ": "MNQ", "RTY": "M2K", "YM": "MYM"}
MECHANISMS = ("CONTINUATION_LEADER", "CONVERGENCE_LAGGARD")
SESSION_ROLES = {"OPEN": 0, "MID": 1}
CONTROL_PRIMARY = "PRIMARY"
CONTROL_DIRECTION_FLIP = "DIRECTION_FLIP_IDENTICAL_OPPORTUNITY"
CONTROL_RANK_SWAP = "RANK_SWAP_STOP_RISK_MATCHED_OPPORTUNITY"
CONTROL_DELAY = "DELAY_ONE_CLOCK_IDENTICAL_MARKET_SESSION"
CONTROLS = (
    CONTROL_PRIMARY,
    CONTROL_DIRECTION_FLIP,
    CONTROL_RANK_SWAP,
    CONTROL_DELAY,
)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
MINUTE_NS = 60_000_000_000


class DispersionResidualError(RuntimeError):
    """A frozen causal, source, or economic contract drifted."""


@dataclass(frozen=True, slots=True)
class DispersionCell:
    mechanism: str
    timeframe_minutes: int
    session_role: str
    session_code: int

    def __post_init__(self) -> None:
        if self.mechanism not in MECHANISMS:
            raise ValueError("unsupported residual mechanism")
        if self.timeframe_minutes not in {5, 15}:
            raise ValueError("unsupported residual clock")
        if SESSION_ROLES.get(self.session_role) != self.session_code:
            raise ValueError("session role/code mismatch")

    @property
    def cell_id(self) -> str:
        return (
            f"dispersion_residual:{self.mechanism.lower()}:"
            f"{self.timeframe_minutes}m:{self.session_role.lower()}"
        )

    @property
    def fingerprint(self) -> str:
        return stable_hash(asdict(self))


def frozen_cells() -> tuple[DispersionCell, ...]:
    rows = tuple(
        DispersionCell(mechanism, timeframe, role, code)
        for mechanism in MECHANISMS
        for timeframe in (5, 15)
        for role, code in SESSION_ROLES.items()
    )
    if len(rows) != 8 or len({row.fingerprint for row in rows}) != 8:
        raise DispersionResidualError("frozen eight-cell inventory drift")
    return rows


def load_manifest(path: str | Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    payload = _read_json(Path(path))
    core = dict(payload)
    claimed = str(core.pop("manifest_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise DispersionResidualError("manifest hash drift")
    governance = dict(payload.get("governance") or {})
    if (
        payload.get("branch_id") != BRANCH_ID
        or bool(governance.get("q4_access_allowed"))
        or bool(governance.get("data_purchase_allowed"))
        or bool(governance.get("broker_allowed"))
        or bool(governance.get("orders_allowed"))
        or bool(governance.get("database_writes_allowed"))
        or bool(governance.get("registry_writes_allowed"))
        or int(governance.get("maximum_cpu_workers", 0)) != 1
    ):
        raise DispersionResidualError("manifest governance drift")
    contract = dict(payload["frozen_cells"])
    if int(contract["expected_cell_count"]) != len(frozen_cells()):
        raise DispersionResidualError("manifest cell inventory drift")
    return payload


def run_intraday_cross_index_dispersion_residual(
    root: str | Path,
    *,
    manifest_path: str | Path = DEFAULT_MANIFEST,
    cell_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Run the frozen, read-only development experiment."""

    started = time.perf_counter()
    project = Path(root).resolve()
    manifest_file = _inside(project, manifest_path)
    manifest = load_manifest(manifest_file)
    source = dict(manifest["source"])
    source_manifest_path = _inside(project, source["manifest_path"])
    if _sha256(source_manifest_path) != str(source["manifest_sha256"]):
        raise DispersionResidualError("source 0029 manifest SHA drift")
    source_manifest = exact._load_self_hashed_manifest(source_manifest_path)
    bindings = dict(dict(source_manifest["data"])["feature_matrix_bindings"])
    matrices = {
        market: _open_bound_matrix(project, bindings, market)
        for market in STATE_MARKETS
    }
    rule_path = _inside(project, source["rule_snapshot_path"])
    if _sha256(rule_path) != str(source["rule_snapshot_sha256"]):
        raise DispersionResidualError("official rule snapshot SHA drift")
    rules, rule_receipt = exact._load_rule_snapshot(rule_path)
    calendar, starts, grid_receipt = exact._load_frozen_grid(
        project, source_manifest
    )
    evaluation_start_ns = _date_ns(
        str(dict(source_manifest["data"])["evaluation_start_inclusive"])
    )
    evaluation_end_ns = _date_ns(
        str(dict(source_manifest["data"])["evaluation_end_exclusive"])
    )

    inventory = frozen_cells()
    if cell_ids is not None:
        known = {row.cell_id: row for row in inventory}
        requested = tuple(str(value) for value in cell_ids)
        if not requested or len(requested) != len(set(requested)):
            raise DispersionResidualError("invalid cell subset")
        unknown = sorted(set(requested) - set(known))
        if unknown:
            raise DispersionResidualError(f"unknown frozen cells: {unknown}")
        inventory = tuple(known[value] for value in requested)

    snapshots = {
        timeframe: build_dispersion_snapshot(
            matrices,
            timeframe_minutes=timeframe,
            calibration_end_exclusive_ns=evaluation_start_ns,
        )
        for timeframe in (5, 15)
    }
    results: list[dict[str, Any]] = []
    exact_replays = 0
    q4_boundary_ns = _date_ns("2024-10-01")
    q4_row_count = 0
    for cell in inventory:
        decisions, threshold_receipt = build_cell_decisions(
            cell,
            snapshots[cell.timeframe_minutes],
            evaluation_start_ns=evaluation_start_ns,
            evaluation_end_exclusive_ns=evaluation_end_ns,
            manifest=manifest,
        )
        q4_row_count += sum(
            int(int(row["decision_ns"]) >= q4_boundary_ns) for row in decisions
        )
        raw_evidence_sets = {
            control: materialize_control_evidence(
                cell,
                decisions,
                matrices,
                control=control,
                threshold=float(threshold_receipt["threshold"]),
                manifest=manifest,
            )
            for control in CONTROLS
        }
        evidence_sets, pairing_receipt = reconcile_paired_evidence(raw_evidence_sets)
        cells = evaluate_account_frontier(
            evidence_sets,
            rules=rules,
            calendar=calendar,
            starts=starts,
            manifest=manifest,
        )
        exact_replays += sum(int(row["exact_account_replays"]) for row in cells)
        decision = decide_cell(cell, decisions, evidence_sets, cells, manifest=manifest)
        results.append(
            {
                "cell": asdict(cell),
                "cell_id": cell.cell_id,
                "cell_fingerprint": cell.fingerprint,
                "threshold_receipt": threshold_receipt,
                "decision_count": len(decisions),
                "decision_hash": stable_hash(decisions),
                "event_counts": {
                    control: {
                        "emitted": len(rows),
                        "completed": sum(
                            row.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
                            for row in rows
                        ),
                    }
                    for control, rows in evidence_sets.items()
                },
                "paired_evidence_receipt": pairing_receipt,
                "account_cells": cells,
                "decision": decision,
            }
        )

    gate = decide_branch(results)
    decisive = cell_ids is None
    status = gate["status"] if decisive else "NON_DECISIONAL_SUBSET_SMOKE_COMPLETE"
    # The economic rows end at the frozen pre-Q4 evaluation boundary.  Count
    # every emitted decision explicitly; do not rely on a declarative zero.
    if evaluation_end_ns > q4_boundary_ns or q4_row_count:
        raise DispersionResidualError("protected Q4 row entered the experiment")
    counts = {
        "frozen_cell_count": 8,
        "evaluated_cell_count": len(inventory),
        "primary_opportunity_count": sum(row["decision_count"] for row in results),
        "exact_account_replays": exact_replays,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "database_writes": 0,
        "registry_writes": 0,
        "xfa_paths_started": 0,
    }
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "evidence_tier_ceiling": "TIER_Q_DEVELOPMENT_QUALIFIED",
        "independent_confirmation_claimed": False,
        "source_bindings": {
            "manifest_path": str(manifest_file.relative_to(project)),
            "manifest_hash": manifest["manifest_hash"],
            "source_manifest_path": str(source_manifest_path.relative_to(project)),
            "source_manifest_hash": source_manifest["manifest_hash"],
            "feature_matrix_hashes": {
                market: matrices[market].fingerprint for market in STATE_MARKETS
            },
            "rule_snapshot": rule_receipt,
            "frozen_grid": grid_receipt,
        },
        "integrity": {
            "availability_not_after_decision": True,
            "strictly_prior_normalization": True,
            "threshold_frozen_before_evaluation": True,
            "next_tradable_open_fill": True,
            "paired_controls_share_opportunity_clock": True,
            "future_outcome_decision_field_count": 0,
            "q4_row_count": q4_row_count,
            "mission_state_write_count": 0,
        },
        "results": results,
        "gate": gate,
        "counts": counts,
        "promotion_status": None,
        "next_action": gate["next_action"],
        "runtime_seconds": time.perf_counter() - started,
        "completed_at_utc": datetime.now(UTC).isoformat(),
    }
    core["result_hash"] = stable_hash(
        {
            key: value
            for key, value in core.items()
            if key not in {"runtime_seconds", "completed_at_utc"}
        }
    )
    return core


def build_dispersion_snapshot(
    matrices: Mapping[str, FeatureMatrix],
    *,
    timeframe_minutes: int,
    calibration_end_exclusive_ns: int,
) -> dict[str, Any]:
    """Align four causal matrices and derive strictly-prior residual dispersion."""

    if set(matrices) != set(STATE_MARKETS):
        raise ValueError("dispersion requires exactly ES/NQ/RTY/YM")
    if timeframe_minutes not in {5, 15}:
        raise ValueError("dispersion clock must be 5m or 15m")
    common = None
    for market in STATE_MARKETS:
        values = np.asarray(matrices[market].array("timestamp_ns"), dtype=np.int64)
        common = values if common is None else np.intersect1d(common, values)
    assert common is not None
    positions: dict[str, np.ndarray] = {}
    z_columns: list[np.ndarray] = []
    availability_columns: list[np.ndarray] = []
    decision_columns: list[np.ndarray] = []
    session_code_columns: list[np.ndarray] = []
    session_day_columns: list[np.ndarray] = []
    segment_columns: list[np.ndarray] = []
    for market in STATE_MARKETS:
        matrix = matrices[market]
        timestamps = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
        index = np.searchsorted(timestamps, common)
        if np.any(index >= len(timestamps)) or not np.array_equal(timestamps[index], common):
            raise DispersionResidualError("cross-index timestamp alignment drift")
        positions[market] = index
        returns = np.asarray(
            matrix.array(f"feature__ctx_{timeframe_minutes}m_return"), dtype=float
        )
        z_columns.append(_causal_prior_zscore(returns)[index])
        availability_columns.append(
            np.asarray(matrix.array("availability_ns"), dtype=np.int64)[index]
        )
        decision_columns.append(
            np.asarray(matrix.array("decision_ns"), dtype=np.int64)[index]
        )
        session_code_columns.append(
            np.asarray(matrix.array("session_code"), dtype=np.int64)[index]
        )
        session_day_columns.append(
            np.asarray(matrix.array("session_day"), dtype=np.int64)[index]
        )
        segment_columns.append(
            np.asarray(matrix.array("segment_code"), dtype=np.int64)[index]
        )
    available = np.max(np.column_stack(availability_columns), axis=1)
    decision_stack = np.column_stack(decision_columns)
    if np.any(decision_stack != decision_stack[:, :1]):
        raise DispersionResidualError("cross-index decision clocks differ")
    decision = decision_stack[:, 0]
    if np.any(available > decision):
        raise DispersionResidualError("cross-index feature availability violation")
    z = np.column_stack(z_columns)
    z_finite = np.all(np.isfinite(z), axis=1)
    common_factor = np.full(len(z), np.nan, dtype=float)
    common_factor[z_finite] = np.median(z[z_finite], axis=1)
    residual = z - common_factor[:, None]
    dispersion = np.full(len(z), np.nan, dtype=float)
    dispersion[z_finite] = np.max(residual[z_finite], axis=1) - np.min(
        residual[z_finite], axis=1
    )
    finite = z_finite & np.isfinite(dispersion)
    boundary = (((common // MINUTE_NS) + 1) % int(timeframe_minutes)) == 0
    session_stack = np.column_stack(session_code_columns)
    session_day_stack = np.column_stack(session_day_columns)
    if np.any(session_stack != session_stack[:, :1]):
        raise DispersionResidualError("cross-index session roles differ")
    if np.any(session_day_stack != session_day_stack[:, :1]):
        raise DispersionResidualError("cross-index session days differ")
    session_codes = session_stack[:, 0]
    session_days = session_day_stack[:, 0]
    segment_stack = np.column_stack(segment_columns)
    segment_change = np.zeros(len(common), dtype=bool)
    if len(common) > 1:
        segment_change[1:] = np.any(segment_stack[1:] != segment_stack[:-1], axis=1)
        segment_change[1:] |= np.diff(common) != MINUTE_NS
    calibration = finite & boundary & (decision < int(calibration_end_exclusive_ns))
    return {
        "timestamp_ns": common,
        "decision_ns": decision,
        "available_at_ns": available,
        "session_code": session_codes,
        "session_day": session_days,
        "segment_codes": segment_stack,
        "structural_reset": segment_change,
        "structural_epoch": np.cumsum(segment_change, dtype=np.int64),
        "z": z,
        "common_factor": common_factor,
        "residual": residual,
        "dispersion": dispersion,
        "finite": finite,
        "boundary": boundary,
        "calibration_mask": calibration,
        "positions": positions,
        "timeframe_minutes": timeframe_minutes,
        "snapshot_hash": stable_hash(
            {
                "timeframe_minutes": timeframe_minutes,
                "row_count": len(common),
                "first_timestamp_ns": int(common[0]),
                "last_timestamp_ns": int(common[-1]),
                "calibration_row_count": int(np.sum(calibration)),
                "matrix_hashes": {
                    market: matrices[market].fingerprint for market in STATE_MARKETS
                },
            }
        ),
    }


def build_cell_decisions(
    cell: DispersionCell,
    snapshot: Mapping[str, Any],
    *,
    evaluation_start_ns: int,
    evaluation_end_exclusive_ns: int,
    manifest: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    causal = dict(manifest["causal_contract"])
    dispersion = np.asarray(snapshot["dispersion"], dtype=float)
    calibration = np.asarray(snapshot["calibration_mask"], dtype=bool) & (
        np.asarray(snapshot["session_code"], dtype=np.int64) == cell.session_code
    )
    calibration_values = dispersion[calibration]
    if len(calibration_values) < 100:
        raise DispersionResidualError("insufficient pre-evaluation threshold power")
    threshold = float(np.quantile(calibration_values, float(causal["threshold_quantile"])))
    decision_ns = np.asarray(snapshot["decision_ns"], dtype=np.int64)
    common = np.asarray(snapshot["common_factor"], dtype=float)
    residual = np.asarray(snapshot["residual"], dtype=float)
    eligible = (
        np.asarray(snapshot["finite"], dtype=bool)
        & np.asarray(snapshot["boundary"], dtype=bool)
        & (np.asarray(snapshot["session_code"], dtype=np.int64) == cell.session_code)
        & (decision_ns >= int(evaluation_start_ns))
        & (decision_ns < int(evaluation_end_exclusive_ns))
    )
    cooldown_ns = int(causal["cooldown_minutes"]) * MINUTE_NS
    last_decision = -10**30
    active_excursion = False
    last_session_day: int | None = None
    last_epoch: int | None = None
    output: list[dict[str, Any]] = []
    for raw_index in np.flatnonzero(eligible):
        index = int(raw_index)
        session_day = int(np.asarray(snapshot["session_day"])[index])
        epoch = int(np.asarray(snapshot["structural_epoch"])[index])
        structural_reset_now = last_epoch is not None and epoch != last_epoch
        reset = session_day != last_session_day or epoch != last_epoch
        if reset:
            active_excursion = False
        last_session_day = session_day
        last_epoch = epoch
        if structural_reset_now:
            continue
        if float(dispersion[index]) < threshold:
            active_excursion = False
            continue
        if active_excursion:
            continue
        # One dispersion excursion is one opportunity, even if the first
        # observable state is later rejected for ambiguity or missing context.
        active_excursion = True
        if abs(float(common[index])) < float(causal["minimum_absolute_common_z"]):
            continue
        if int(decision_ns[index]) < last_decision + cooldown_ns:
            continue
        side = 1 if float(common[index]) > 0.0 else -1
        aligned = side * residual[index]
        if (
            np.sum(np.isclose(aligned, np.nanmax(aligned), rtol=0.0, atol=1e-12)) != 1
            or np.sum(np.isclose(aligned, np.nanmin(aligned), rtol=0.0, atol=1e-12)) != 1
        ):
            continue
        leader_offset = int(np.argmax(aligned))
        laggard_offset = int(np.argmin(aligned))
        primary_offset = (
            leader_offset if cell.mechanism == "CONTINUATION_LEADER" else laggard_offset
        )
        paired_offset = (
            laggard_offset if cell.mechanism == "CONTINUATION_LEADER" else leader_offset
        )
        state_market = STATE_MARKETS[primary_offset]
        paired_market = STATE_MARKETS[paired_offset]
        delayed_timestamp = int(np.asarray(snapshot["timestamp_ns"])[index]) + (
            cell.timeframe_minutes * MINUTE_NS
        )
        delayed_index = int(
            np.searchsorted(np.asarray(snapshot["timestamp_ns"]), delayed_timestamp)
        )
        delayed_target_row_index: int | None = None
        delayed_decision_ns: int | None = None
        if (
            delayed_index < len(np.asarray(snapshot["timestamp_ns"]))
            and int(np.asarray(snapshot["timestamp_ns"])[delayed_index])
            == delayed_timestamp
            and int(np.asarray(snapshot["session_code"])[delayed_index])
            == cell.session_code
            and int(np.asarray(snapshot["session_day"])[delayed_index])
            == int(np.asarray(snapshot["session_day"])[index])
        ):
            delayed_target_row_index = int(
                snapshot["positions"][state_market][delayed_index]
            )
            delayed_decision_ns = int(decision_ns[delayed_index])
        if delayed_target_row_index is None:
            # Controls must retain an identical opportunity denominator.  A
            # boundary opportunity without its frozen one-clock delay is
            # coverage-censored for every arm, never silently dropped later.
            continue
        row = {
            "cell_id": cell.cell_id,
            "aligned_row_index": index,
            "timestamp_ns": int(np.asarray(snapshot["timestamp_ns"])[index]),
            "available_at_ns": int(np.asarray(snapshot["available_at_ns"])[index]),
            "decision_ns": int(decision_ns[index]),
            "side": side,
            "state_market": state_market,
            "paired_rank_market": paired_market,
            "target_row_index": int(snapshot["positions"][state_market][index]),
            "paired_target_row_index": int(snapshot["positions"][paired_market][index]),
            "delayed_target_row_index": delayed_target_row_index,
            "delayed_decision_ns": delayed_decision_ns,
            "dispersion": float(dispersion[index]),
            "common_factor": float(common[index]),
            "leader_market": STATE_MARKETS[leader_offset],
            "laggard_market": STATE_MARKETS[laggard_offset],
            "residuals": {
                market: float(residual[index, offset])
                for offset, market in enumerate(STATE_MARKETS)
            },
        }
        row["feature_fingerprint"] = stable_hash(row)
        output.append(row)
        last_decision = int(decision_ns[index])
    return output, {
        "threshold_quantile": float(causal["threshold_quantile"]),
        "threshold": threshold,
        "calibration_end_exclusive_ns": int(evaluation_start_ns),
        "calibration_observation_count": len(calibration_values),
        "normalization": causal["normalization"],
        "snapshot_hash": snapshot["snapshot_hash"],
    }


def materialize_control_evidence(
    cell: DispersionCell,
    decisions: Sequence[Mapping[str, Any]],
    matrices: Mapping[str, FeatureMatrix],
    *,
    control: str,
    threshold: float,
    manifest: Mapping[str, Any],
) -> tuple[Any, ...]:
    if control not in CONTROLS:
        raise ValueError("unknown paired control")
    causal = dict(manifest["causal_contract"])
    grouped: dict[str, list[Mapping[str, Any]]] = {market: [] for market in STATE_MARKETS}
    for decision in decisions:
        if control == CONTROL_DELAY and decision.get("delayed_target_row_index") is None:
            continue
        market = str(
            decision["paired_rank_market"]
            if control == CONTROL_RANK_SWAP
            else decision["state_market"]
        )
        grouped[market].append(decision)
    evidence: list[Any] = []
    for market in STATE_MARKETS:
        rows = grouped[market]
        if not rows:
            continue
        matrix = matrices[market]
        candidate = _outcome_candidate(cell, market, control, causal)
        calibrated = CalibratedHazardCandidate(
            candidate=candidate,
            calibration_end_exclusive_ns=min(int(row["decision_ns"]) for row in rows),
            trigger_threshold=float(threshold),
            context_threshold=None,
            finite_trigger_observations=len(rows),
            finite_context_observations=None,
            source_matrix_hash=matrix.fingerprint,
        )
        intents: list[HazardIntent] = []
        for row in rows:
            row_index = int(
                row["paired_target_row_index"]
                if control == CONTROL_RANK_SWAP
                else (
                    row["delayed_target_row_index"]
                    if control == CONTROL_DELAY
                    else row["target_row_index"]
                )
            )
            direction = int(row["side"])
            if control == CONTROL_DIRECTION_FLIP:
                direction = -direction
            event_time_ns = int(matrix.array("timestamp_ns")[row_index])
            decision_time_ns = int(matrix.array("decision_ns")[row_index])
            available_at_ns = int(matrix.array("availability_ns")[row_index])
            if control != CONTROL_DELAY:
                event_time_ns = int(row["timestamp_ns"])
                decision_time_ns = int(row["decision_ns"])
                available_at_ns = int(row["available_at_ns"])
            intents.append(
                HazardIntent(
                    candidate_id=candidate.candidate_id,
                    intent_namespace=f"{BRANCH_ID}:{cell.cell_id}:{control}",
                    evidence_role="VIEWED_DEVELOPMENT_ONLY",
                    control_id=None if control == CONTROL_PRIMARY else control,
                    row_index=row_index,
                    market=market,
                    contract_code=int(matrix.array("contract_code")[row_index]),
                    session_day=int(matrix.array("session_day")[row_index]),
                    session_code=int(matrix.array("session_code")[row_index]),
                    segment_code=int(matrix.array("segment_code")[row_index]),
                    event_time_ns=event_time_ns,
                    available_at_ns=available_at_ns,
                    decision_time_ns=decision_time_ns,
                    order_submit_time_ns=decision_time_ns,
                    entry_intent="MARKETABLE_NEXT_TRADABLE_OPEN",
                    earliest_executable_time_ns=decision_time_ns,
                    direction=direction,
                    feature_fingerprint=str(row["feature_fingerprint"]),
                )
            )
        evidence.extend(observe_outcomes(calibrated, matrix, intents))
    return tuple(sorted(evidence, key=lambda row: (row.decision_time_ns, row.event_id)))


def reconcile_paired_evidence(
    evidence_sets: Mapping[str, Sequence[Any]],
) -> tuple[dict[str, tuple[Any, ...]], dict[str, Any]]:
    """Use one common completed-opportunity denominator for every control.

    Censoring is an outcome-coverage role only: it never changes the decision.
    If any arm lacks future coverage, the opportunity is excluded from every
    arm's economic denominator and remains counted in this receipt.
    """

    if set(evidence_sets) != set(CONTROLS):
        raise DispersionResidualError("paired-control inventory drift")
    indexed: dict[str, dict[str, Any]] = {}
    for control, rows in evidence_sets.items():
        values: dict[str, Any] = {}
        for row in rows:
            key = str(row.feature_fingerprint)
            if key in values:
                raise DispersionResidualError("duplicate paired opportunity")
            values[key] = row
        indexed[control] = values
    emitted_sets = {control: set(rows) for control, rows in indexed.items()}
    first = emitted_sets[CONTROL_PRIMARY]
    if any(values != first for values in emitted_sets.values()):
        raise DispersionResidualError("paired controls changed opportunity inventory")
    complete_keys = {
        key
        for key in first
        if all(
            indexed[control][key].outcome
            != HazardOutcome.CENSORED_FUTURE_COVERAGE
            for control in CONTROLS
        )
    }
    filtered = {
        control: tuple(
            sorted(
                (rows[key] for key in complete_keys),
                key=lambda row: (row.feature_fingerprint, row.event_id),
            )
        )
        for control, rows in indexed.items()
    }
    completed_sets = {
        control: {row.feature_fingerprint for row in rows}
        for control, rows in filtered.items()
    }
    if any(values != complete_keys for values in completed_sets.values()):
        raise DispersionResidualError("paired completed denominator drift")
    primary_clock = {
        key: int(indexed[CONTROL_PRIMARY][key].event_time_ns) for key in first
    }
    return filtered, {
        "emitted_opportunity_count": len(first),
        "common_completed_opportunity_count": len(complete_keys),
        "common_censored_opportunity_count": len(first - complete_keys),
        "control_completed_counts": {
            control: len(rows) for control, rows in filtered.items()
        },
        "opportunity_clock_hash": stable_hash(primary_clock),
        "all_controls_share_opportunity_ids": True,
        "censoring_reconciled_before_economic_comparison": True,
    }


def evaluate_account_frontier(
    evidence_sets: Mapping[str, Sequence[Any]],
    *,
    rules: Mapping[str, Mapping[str, Any]],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    account = dict(manifest["account_evaluation"])
    quantity_frontier = tuple(int(value) for value in account["micro_quantity_frontier"])
    design_blocks = set(str(value) for value in account["design_blocks"])
    heldout_blocks = set(str(value) for value in account["held_out_development_blocks"])
    output: list[dict[str, Any]] = []
    primary_by_opportunity = {
        str(row.feature_fingerprint): row for row in evidence_sets[CONTROL_PRIMARY]
    }
    for control in CONTROLS:
        complete = tuple(
            row
            for row in evidence_sets[control]
            if row.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
        )
        base = {
            scenario: tuple(
                _hazard_trade_path_event(row, scenario=scenario) for row in complete
            )
            for scenario in SCENARIOS
        }
        _require_scenario_identity(base["NORMAL"], base["STRESSED_1_5X"])
        for account_label in account["account_sizes"]:
            rule = dict(rules[str(account_label)])
            config = exact._account_config(rule)
            legal_micro = int(rule["maximum_micro_contracts"])
            legal_mini = float(rule["maximum_mini_contracts"])
            for quantity in quantity_frontier:
                if quantity > legal_micro or quantity / 10.0 > legal_mini + 1e-12:
                    continue
                matched_quantities, risk_errors = _matched_control_quantities(
                    complete,
                    primary_by_opportunity,
                    primary_quantity=quantity,
                    legal_micro=legal_micro,
                )
                scaled = {
                    scenario: tuple(
                        _scale_event(row, matched_quantity)
                        for row, matched_quantity in zip(
                            values, matched_quantities, strict=True
                        )
                    )
                    for scenario, values in base.items()
                }
                _require_scenario_identity(
                    scaled["NORMAL"], scaled["STRESSED_1_5X"]
                )
                for horizon in account["horizons_trading_days"]:
                    episodes: dict[str, list[tuple[Any, str]]] = {
                        scenario: [] for scenario in SCENARIOS
                    }
                    for start_day, block in starts[int(horizon)]:
                        for scenario in SCENARIOS:
                            episode = run_combine_episode(
                                scaled[scenario],
                                calendar,
                                start_day=int(start_day),
                                maximum_duration_days=int(horizon),
                                config=config,
                                maximum_mini_equivalent=legal_mini,
                            )
                            episodes[scenario].append((episode, str(block)))
                    summaries: dict[str, Any] = {}
                    for scenario in SCENARIOS:
                        rows = episodes[scenario]
                        summaries[scenario] = {
                            "design": _summarize(
                                [row for row in rows if row[1] in design_blocks]
                            ),
                            "held_out_development": _summarize(
                                [row for row in rows if row[1] in heldout_blocks]
                            ),
                            "all_development": _summarize(rows),
                        }
                    output.append(
                        {
                            "control": control,
                            "account_label": str(account_label),
                            "account_size_usd": int(rule["account_size_usd"]),
                            "micro_quantity": quantity,
                            "maximum_control_micro_quantity": max(
                                matched_quantities, default=0
                            ),
                            "mini_equivalent": float(quantity / 10.0),
                            "maximum_stop_risk_match_error_fraction": max(
                                risk_errors, default=0.0
                            ),
                            "horizon_trading_days": int(horizon),
                            "completed_event_count": len(complete),
                            "exact_account_replays": sum(len(v) for v in episodes.values()),
                            "normal": summaries["NORMAL"],
                            "stressed": summaries["STRESSED_1_5X"],
                        }
                    )
    return output


def decide_cell(
    cell: DispersionCell,
    decisions: Sequence[Mapping[str, Any]],
    evidence_sets: Mapping[str, Sequence[Any]],
    account_cells: Sequence[Mapping[str, Any]],
    *,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    gate = dict(manifest["frozen_gate"])
    primaries = [
        dict(row)
        for row in account_cells
        if row["control"] == CONTROL_PRIMARY
        and float(row["stressed"]["design"]["mll_breach_rate"])
        <= float(gate["maximum_held_out_stressed_mll_breach_rate"])
    ]
    if not primaries:
        return {"passed": False, "reason": "NO_SAFE_DESIGN_CELL"}
    frozen = max(primaries, key=_design_rank)
    matching = {
        control: _one(
            [
                dict(row)
                for row in account_cells
                if row["control"] == control
                and _same_account_cell(row, frozen)
            ],
            f"{cell.cell_id}:{control}",
        )
        for control in CONTROLS
    }
    primary = matching[CONTROL_PRIMARY]
    normal = dict(primary["normal"]["held_out_development"])
    stressed = dict(primary["stressed"]["held_out_development"])
    controls: dict[str, Any] = {}
    for control in (CONTROL_DIRECTION_FLIP, CONTROL_RANK_SWAP, CONTROL_DELAY):
        summary = dict(matching[control]["stressed"]["held_out_development"])
        controls[control] = {
            "stressed_pass_count_delta": int(stressed["pass_count"])
            - int(summary["pass_count"]),
            "stressed_net_total_usd_delta": float(stressed["net_total_usd"])
            - float(summary["net_total_usd"]),
            "stressed_target_progress_median_delta": float(
                stressed["target_progress_median"]
            )
            - float(summary["target_progress_median"]),
            "maximum_stop_risk_match_error_fraction": float(
                matching[control]["maximum_stop_risk_match_error_fraction"]
            ),
        }
        controls[control]["beaten"] = (
            controls[control]["stressed_pass_count_delta"] >= 0
            and controls[control]["stressed_net_total_usd_delta"] > 0.0
            and controls[control]["stressed_target_progress_median_delta"] >= 0.0
            and controls[control]["maximum_stop_risk_match_error_fraction"]
            <= float(gate["maximum_matched_control_stop_risk_error_fraction"])
        )
    common_pass_blocks = set(normal["blocks_with_passes"]) & set(
        stressed["blocks_with_passes"]
    )
    control_event_counts_equal = len(
        {len(evidence_sets[control]) for control in CONTROLS}
    ) == 1
    heldout_block_positive = all(
        float(dict(stressed["by_block"]).get(block, {}).get("net_total_usd", 0.0))
        > 0.0
        for block in ("B3", "B4")
    )
    checks = {
        "minimum_normal_passes": int(normal["pass_count"])
        >= int(gate["minimum_held_out_normal_passes"]),
        "minimum_stressed_passes": int(stressed["pass_count"])
        >= int(gate["minimum_held_out_stressed_passes"]),
        "normal_stressed_common_passing_block_diversity": len(common_pass_blocks)
        >= int(gate["minimum_held_out_stressed_passing_blocks"]),
        "controlled_mll": float(stressed["mll_breach_rate"])
        <= float(gate["maximum_held_out_stressed_mll_breach_rate"]),
        "positive_stressed_net": float(stressed["net_total_usd"])
        > float(gate["minimum_held_out_stressed_net_usd"]),
        "positive_stressed_net_in_B3_and_B4": heldout_block_positive,
        "passing_consistency": bool(stressed["all_passing_paths_consistency_compliant"]),
        "paired_completed_event_counts_equal": control_event_counts_equal,
        "beats_all_matched_controls": all(row["beaten"] for row in controls.values()),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "frozen_from_design_only": {
            key: frozen[key]
            for key in (
                "account_label",
                "account_size_usd",
                "micro_quantity",
                "mini_equivalent",
                "horizon_trading_days",
            )
        },
        "held_out_normal": normal,
        "held_out_stressed": stressed,
        "matched_control_deltas": controls,
        "paired_opportunity_count": len(decisions),
        "control_event_counts_equal": control_event_counts_equal,
        "evidence_tier": (
            "Q_DEVELOPMENT_QUALIFIED" if all(checks.values()) else "E_EXECUTABLE"
        ),
        "promotion_status": None,
    }


def decide_branch(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    qualifiers = [
        str(row["cell_id"])
        for row in results
        if bool(dict(row["decision"]).get("passed"))
    ]
    weak = [
        str(row["cell_id"])
        for row in results
        if float(
            dict(dict(row["decision"]).get("held_out_stressed") or {}).get(
                "net_total_usd", 0.0
            )
        )
        > 0.0
    ]
    if qualifiers:
        status = "INTRADAY_CROSS_INDEX_DISPERSION_RESIDUAL_GREEN_DEVELOPMENT"
        branch = "GREEN"
    elif weak:
        status = "INTRADAY_CROSS_INDEX_DISPERSION_RESIDUAL_WEAK_DEVELOPMENT"
        branch = "WEAK"
    else:
        status = "INTRADAY_CROSS_INDEX_DISPERSION_RESIDUAL_FALSIFIED"
        branch = "FALSIFIED"
    next_actions = {
        "GREEN": "FREEZE_QUALIFYING_CELL_AND_REQUIRE_ONE_GENUINELY_FRESH_CONFIRMATION",
        "WEAK": "PRESERVE_DIAGNOSTIC_AND_RETURN_TO_MATERIALLY_DISTINCT_BRANCH_BOARD",
        "FALSIFIED": "TOMBSTONE_EXACT_DISPERSION_RESIDUAL_GRAMMAR_AND_SELECT_DISTINCT_BRANCH",
    }
    return {
        "status": status,
        "qualifying_cell_ids": qualifiers,
        "qualifying_cell_count": len(qualifiers),
        "weak_cell_ids": weak,
        "independent_confirmation_claimed": False,
        "authoritative_promotion_count": 0,
        "next_action": next_actions[branch],
    }


def persist_tripwire_result(
    root: str | Path,
    result: Mapping[str, Any],
    *,
    output_root: str | Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    project = Path(root).resolve()
    folder = (project / output_root).resolve()
    try:
        folder.relative_to(project)
    except ValueError as exc:
        raise DispersionResidualError("output escapes project root") from exc
    folder.mkdir(parents=True, exist_ok=True)
    result_path = folder / "economic_result.json"
    report_path = folder / "decision_report.json"
    _atomic_json(result_path, result)
    report_core = {
        "schema": "hydra_intraday_cross_index_dispersion_residual_report_v1",
        "branch_id": BRANCH_ID,
        "status": result["status"],
        "gate": result["gate"],
        "counts": result["counts"],
        "result_hash": result["result_hash"],
        "next_action": result["next_action"],
    }
    report = {**report_core, "report_hash": stable_hash(report_core)}
    _atomic_json(report_path, report)
    return {
        "result_path": str(result_path.relative_to(project)),
        "result_sha256": _sha256(result_path),
        "result_hash": result["result_hash"],
        "report_path": str(report_path.relative_to(project)),
        "report_sha256": _sha256(report_path),
        "report_hash": report["report_hash"],
    }


def _outcome_candidate(
    cell: DispersionCell,
    market: str,
    control: str,
    causal: Mapping[str, Any],
) -> HazardCandidate:
    return HazardCandidate(
        market=market,
        execution_market=EXECUTION_MARKETS[market],
        mechanism=f"DISPERSION_RESIDUAL_{cell.mechanism}_{control}",
        cross_asset_reference_market=None,
        timeframe=f"{cell.timeframe_minutes}m",
        session_code=cell.session_code,
        trigger_feature=f"ctx_{cell.timeframe_minutes}m_return",
        trigger_operator="ABS_GT",
        trigger_quantile=0.85,
        context_feature=None,
        context_operator=None,
        context_quantile=None,
        direction_rule="TRIGGER_SIGN_CONTINUATION",
        favorable_r=float(causal["favorable_r"]),
        adverse_r=float(causal["adverse_r"]),
        horizon=int(causal["maximum_holding_minutes"]),
        risk_level=1.0,
        cooldown_minutes=int(causal["cooldown_minutes"]),
    )


def _matched_control_quantities(
    rows: Sequence[Any],
    primary_by_opportunity: Mapping[str, Any],
    *,
    primary_quantity: int,
    legal_micro: int,
) -> tuple[tuple[int, ...], tuple[float, ...]]:
    quantities: list[int] = []
    errors: list[float] = []
    legal_multiple = max(4, (int(legal_micro) // 4) * 4)
    for row in rows:
        primary = primary_by_opportunity[str(row.feature_fingerprint)]
        primary_risk = (
            float(primary.risk_unit_price)
            * float(primary.adverse_r)
            * float(instrument_spec(primary.execution_market).point_value)
            * int(primary_quantity)
        )
        control_per_micro = (
            float(row.risk_unit_price)
            * float(row.adverse_r)
            * float(instrument_spec(row.execution_market).point_value)
        )
        if primary_risk <= 0.0 or control_per_micro <= 0.0:
            raise DispersionResidualError("nonpositive causal stop risk")
        raw = primary_risk / control_per_micro
        matched = int(4 * round(raw / 4.0))
        matched = min(max(matched, 4), legal_multiple)
        actual = control_per_micro * matched
        quantities.append(matched)
        errors.append(abs(actual - primary_risk) / primary_risk)
    return tuple(quantities), tuple(errors)


def _require_scenario_identity(
    normal: Sequence[TradePathEvent], stressed: Sequence[TradePathEvent]
) -> None:
    def identity(row: TradePathEvent) -> tuple[Any, ...]:
        event_id = str(row.event_id)
        for token in (":NORMAL", ":STRESSED_1_5X"):
            event_id = event_id.replace(token, "")
        return (
            event_id,
            int(row.decision_ns),
            int(row.exit_ns),
            int(row.session_day),
            int(row.quantity),
            float(row.mini_equivalent),
        )

    if [identity(row) for row in normal] != [identity(row) for row in stressed]:
        raise DispersionResidualError("normal/stressed decision identity drift")


def _scale_event(event: TradePathEvent, quantity: int) -> TradePathEvent:
    if quantity <= 0 or quantity % int(event.quantity) != 0:
        raise ValueError("quantity frontier must be an integer multiple of base size")
    factor = quantity / float(event.quantity)
    return replace(
        event,
        event_id=f"{event.event_id}:q{quantity}",
        net_pnl=float(event.net_pnl) * factor,
        gross_pnl=float(event.gross_pnl) * factor,
        worst_unrealized_pnl=float(event.worst_unrealized_pnl) * factor,
        best_unrealized_pnl=float(event.best_unrealized_pnl) * factor,
        quantity=quantity,
        mini_equivalent=float(event.mini_equivalent) * factor,
    )


def _summarize(values: Sequence[tuple[Any, str]]) -> dict[str, Any]:
    episodes = [row for row, _block in values]
    passes = [row for row in episodes if row.terminal is CombineTerminal.PASSED]
    by_block: dict[str, dict[str, Any]] = {}
    for block in sorted({block for _row, block in values}):
        block_rows = [row for row, value in values if value == block]
        by_block[block] = {
            "episode_count": len(block_rows),
            "pass_count": sum(row.terminal is CombineTerminal.PASSED for row in block_rows),
            "mll_breach_count": sum(bool(row.mll_breached) for row in block_rows),
            "net_total_usd": float(sum(float(row.net_pnl) for row in block_rows)),
        }
    nets = [float(row.net_pnl) for row in episodes]
    progress = [float(row.target_progress) for row in episodes]
    return {
        "episode_count": len(episodes),
        "pass_count": len(passes),
        "pass_rate": len(passes) / max(len(episodes), 1),
        "blocks_with_passes": [
            block for block, row in by_block.items() if int(row["pass_count"]) > 0
        ],
        "by_block": by_block,
        "net_total_usd": float(sum(nets)),
        "net_median_usd": float(statistics.median(nets)) if nets else 0.0,
        "target_progress_median": float(statistics.median(progress)) if progress else 0.0,
        "target_progress_p25": float(np.percentile(progress, 25)) if progress else 0.0,
        "mll_breach_count": sum(bool(row.mll_breached) for row in episodes),
        "mll_breach_rate": sum(bool(row.mll_breached) for row in episodes)
        / max(len(episodes), 1),
        "minimum_mll_buffer_usd": min(
            (float(row.minimum_mll_buffer) for row in episodes), default=0.0
        ),
        "consistency_compliance_rate": sum(bool(row.consistency_ok) for row in episodes)
        / max(len(episodes), 1),
        "all_passing_paths_consistency_compliant": bool(passes)
        and all(bool(row.consistency_ok) for row in passes),
        "terminal_distribution": {
            terminal.value: sum(row.terminal is terminal for row in episodes)
            for terminal in CombineTerminal
        },
    }


def _design_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    stressed = dict(row["stressed"]["design"])
    normal = dict(row["normal"]["design"])
    return (
        int(stressed["pass_count"]),
        int(normal["pass_count"]),
        float(stressed["target_progress_p25"]),
        float(stressed["target_progress_median"]),
        float(stressed["net_total_usd"]),
        -float(stressed["mll_breach_rate"]),
        -int(row["horizon_trading_days"]),
        -int(row["account_size_usd"]),
        -int(row["micro_quantity"]),
    )


def _same_account_cell(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return all(
        left[key] == right[key]
        for key in ("account_label", "micro_quantity", "horizon_trading_days")
    )


def _one(values: Sequence[Any], label: str) -> Any:
    if len(values) != 1:
        raise DispersionResidualError(f"expected one {label}, got {len(values)}")
    return values[0]


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise DispersionResidualError("path escapes project root") from exc
    if not resolved.is_file():
        raise DispersionResidualError(f"required file is absent: {resolved}")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


__all__ = [
    "BRANCH_ID",
    "DispersionCell",
    "DispersionResidualError",
    "build_cell_decisions",
    "build_dispersion_snapshot",
    "decide_branch",
    "decide_cell",
    "evaluate_account_frontier",
    "frozen_cells",
    "load_manifest",
    "materialize_control_evidence",
    "persist_tripwire_result",
    "run_intraday_cross_index_dispersion_residual",
]
