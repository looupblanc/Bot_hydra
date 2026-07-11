"""Frozen six-policy account-role structural research epoch.

The capability consumes only immutable development artifacts.  It writes
research evidence and account-policy prototypes; it has no mission, registry,
shadow, market-data, broker, or order dependency.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from hydra.portfolio.account_contribution import AccountReplayConfig, replay_shared_account


POLICY_VERSION = "role_conditioned_structural_epoch_v1"
PREREGISTRATION_SHA256 = "50a202914661f2b34bef1366658e741a55f9bd88c15ffd65aacf633838240948"
DEVELOPMENT_END_EXCLUSIVE = pd.Timestamp("2024-10-01T00:00:00Z")
FIT_END_EXCLUSIVE = pd.Timestamp("2024-01-01T00:00:00Z")
STATUS_CEILING = "PROMISING_RESEARCH_CANDIDATE"
POOLS = (
    "COMBINE_PASSER_POOL",
    "XFA_PAYOUT_POOL",
    "DEFENSIVE_ACCOUNT_POOL",
)
STRUCTURES: tuple[dict[str, Any], ...] = (
    {
        "policy_id": "combine_collision_rank_scheduler_v1",
        "objective_pool": "COMBINE_PASSER_POOL",
        "kind": "collision_rank_scheduler",
        "fit_metric": "target_progress_per_adverse_excursion",
        "reference_contracts": 1,
        "control": "circular_session_shift",
    },
    {
        "policy_id": "combine_prior_mae_micro_budget_v1",
        "objective_pool": "COMBINE_PASSER_POOL",
        "kind": "prior_mae_micro_budget",
        "fit_metric": "per_lineage_adverse_excursion_q90",
        "reference_contracts": 1,
        "control": "circular_session_shift",
    },
    {
        "policy_id": "xfa_realized_qualifying_day_latch_v1",
        "objective_pool": "XFA_PAYOUT_POOL",
        "kind": "realized_qualifying_day_latch",
        "fit_metric": "topstep_realized_qualifying_day_threshold",
        "reference_contracts": 1,
        "control": "circular_session_shift",
    },
    {
        "policy_id": "xfa_prior_qualifying_frequency_scheduler_v1",
        "objective_pool": "XFA_PAYOUT_POOL",
        "kind": "qualifying_frequency_scheduler",
        "fit_metric": "qualifying_frequency_and_payout_progress_per_drawdown",
        "reference_contracts": 1,
        "control": "circular_session_shift",
    },
    {
        "policy_id": "defensive_redundant_collision_suppressor_v1",
        "objective_pool": "DEFENSIVE_ACCOUNT_POOL",
        "kind": "redundant_collision_suppressor",
        "fit_metric": "prior_daily_pnl_correlation_and_drawdown_efficiency",
        "reference_contracts": 1,
        "control": "count_matched_random_suppression",
    },
    {
        "policy_id": "defensive_prior_mae_quantile_throttle_v1",
        "objective_pool": "DEFENSIVE_ACCOUNT_POOL",
        "kind": "prior_mae_quantile_throttle",
        "fit_metric": "prior_completed_mae_and_drawdown_q75",
        "reference_contracts": 2,
        "control": "count_matched_random_suppression",
    },
)


class RoleConditionedEpochError(RuntimeError):
    """An immutable-input or hard research-integrity invariant failed."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _verify_file(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file():
        raise RoleConditionedEpochError(f"Missing frozen {label}: {path}")
    actual = _sha256(path)
    if actual != str(expected_sha256):
        raise RoleConditionedEpochError(
            f"Frozen {label} hash drift: expected {expected_sha256}, observed {actual}"
        )


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RoleConditionedEpochError(f"Invalid frozen {label}: {path}") from exc
    if not isinstance(value, dict):
        raise RoleConditionedEpochError(f"Frozen {label} is not an object")
    return value


def _load_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RoleConditionedEpochError(
                    f"Invalid {label} JSONL line {line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise RoleConditionedEpochError(
                    f"Non-object {label} JSONL line {line_number}"
                )
            values.append(value)
    if not values:
        raise RoleConditionedEpochError(f"Frozen {label} is empty")
    return values


def _verify_standard_semantic_hash(
    value: dict[str, Any], expected_hash: str, *, field: str, label: str
) -> None:
    recorded = str(value.get(field) or "")
    if recorded != str(expected_hash):
        raise RoleConditionedEpochError(f"Frozen {label} semantic hash mismatch")
    payload = dict(value)
    payload.pop(field, None)
    if _canonical_hash(payload) != recorded:
        raise RoleConditionedEpochError(f"Frozen {label} canonical hash drift")


def _verify_halving_semantic_hash(
    value: dict[str, Any], expected_hash: str
) -> None:
    recorded = str(value.get("result_hash") or "")
    if recorded != str(expected_hash):
        raise RoleConditionedEpochError("Frozen halving result semantic hash mismatch")
    payload = dict(value)
    payload.pop("result_hash", None)
    artifacts = dict(payload.get("artifacts") or {})
    payload["artifacts"] = {
        key: str((entry or {}).get("sha256") or "")
        for key, entry in sorted(artifacts.items())
    }
    if _canonical_hash(payload) != recorded:
        raise RoleConditionedEpochError("Frozen halving result canonical hash drift")


def _require_zero_source_capability(value: Mapping[str, Any], label: str) -> None:
    scalar_fields = (
        "q4_access_count",
        "q4_access_count_delta",
        "network_requests",
        "paid_data_requests",
        "broker_connections",
        "outbound_orders",
        "paper_shadow_ready",
        "shadow_research_active",
    )
    for field in scalar_fields:
        if int(value.get(field) or 0) != 0:
            raise RoleConditionedEpochError(f"Frozen {label} reports prohibited {field}")
    for field in (
        "order_capability",
        "outbound_order_capability",
        "live_or_broker_execution",
    ):
        if bool(value.get(field)):
            raise RoleConditionedEpochError(f"Frozen {label} reports prohibited {field}")
    for field in ("incremental_databento_spend_usd",):
        if not math.isclose(float(value.get(field) or 0.0), 0.0, abs_tol=1e-12):
            raise RoleConditionedEpochError(f"Frozen {label} reports prohibited spend")


def _validate_sources(
    *,
    mutation_result: dict[str, Any],
    mutation_result_sha256: str,
    mutation_result_hash: str,
    mutation_ledger_sha256: str,
    halving_result: dict[str, Any],
    halving_result_hash: str,
    halving_evidence_sha256: str,
    halving_manifest: dict[str, Any],
    halving_manifest_hash: str,
    portfolio_result: dict[str, Any],
    portfolio_result_hash: str,
    meta_result: dict[str, Any],
    meta_result_hash: str,
) -> list[str]:
    if mutation_result.get("schema") != "hydra_promising_lineage_mutation_result_v1":
        raise RoleConditionedEpochError("Unexpected mutation-result schema")
    _verify_standard_semantic_hash(
        mutation_result,
        mutation_result_hash,
        field="result_hash",
        label="mutation result",
    )
    if str((mutation_result.get("artifacts") or {}).get("trade_ledger_sha256") or "") != str(
        mutation_ledger_sha256
    ):
        raise RoleConditionedEpochError("Mutation result/ledger provenance mismatch")
    _require_zero_source_capability(mutation_result, "mutation result")

    if halving_result.get("schema") != "hydra_post_mutation_successive_halving_result_v1":
        raise RoleConditionedEpochError("Unexpected halving-result schema")
    _verify_halving_semantic_hash(halving_result, halving_result_hash)
    _require_zero_source_capability(halving_result, "halving result")
    artifacts = dict(halving_result.get("artifacts") or {})
    if str((artifacts.get("candidate_evidence") or {}).get("sha256") or "") != str(
        halving_evidence_sha256
    ):
        raise RoleConditionedEpochError("Halving evidence provenance mismatch")
    if str((artifacts.get("elite_manifest") or {}).get("sha256") or "") == "":
        raise RoleConditionedEpochError("Halving elite manifest provenance missing")
    if str(halving_result.get("source_mutation_result_sha256") or "") != str(
        mutation_result_sha256
    ):
        raise RoleConditionedEpochError("Halving/mutation source mismatch")

    if halving_manifest.get("schema") != "hydra_post_mutation_elite_manifest_v1":
        raise RoleConditionedEpochError("Unexpected halving-manifest schema")
    _verify_standard_semantic_hash(
        halving_manifest,
        halving_manifest_hash,
        field="manifest_hash",
        label="halving manifest",
    )
    _require_zero_source_capability(halving_manifest, "halving manifest")
    selected = sorted(str(value) for value in halving_result.get("selected_candidate_ids") or [])
    manifest_selected = sorted(
        str(value) for value in halving_manifest.get("selected_candidate_ids") or []
    )
    if not selected or selected != manifest_selected:
        raise RoleConditionedEpochError("Halving selected-elite manifest mismatch")
    if int(halving_manifest.get("selected_count") or 0) != len(selected):
        raise RoleConditionedEpochError("Halving selected count mismatch")

    if portfolio_result.get("schema") != "hydra_portfolio_role_research_result_v1":
        raise RoleConditionedEpochError("Unexpected portfolio-role schema")
    _verify_standard_semantic_hash(
        portfolio_result,
        portfolio_result_hash,
        field="result_hash",
        label="portfolio-role result",
    )
    _require_zero_source_capability(portfolio_result, "portfolio-role result")

    if meta_result.get("schema") != "meta_failure_allocation_v1":
        raise RoleConditionedEpochError("Unexpected meta-allocation schema")
    _verify_standard_semantic_hash(
        meta_result,
        meta_result_hash,
        field="result_hash",
        label="meta-allocation result",
    )
    _require_zero_source_capability(
        dict(meta_result.get("governance") or {}), "meta-allocation governance"
    )
    return selected


def _normalize_mutation_ledger(
    rows: Iterable[dict[str, Any]], selected_ids: set[str]
) -> pd.DataFrame:
    required = {
        "candidate_id",
        "parent_candidate_id",
        "timestamp",
        "event_session_id",
        "symbol",
        "active_contract",
        "gross_pnl",
        "cost",
        "net_pnl",
        "mae_dollars",
    }
    frame = pd.DataFrame(list(rows))
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RoleConditionedEpochError(f"Mutation ledger missing columns: {missing}")
    frame = frame[frame["candidate_id"].astype(str).isin(selected_ids)].copy()
    if set(frame["candidate_id"].astype(str)) != selected_ids:
        missing_ids = sorted(selected_ids - set(frame["candidate_id"].astype(str)))
        raise RoleConditionedEpochError(f"Selected elites missing ledger rows: {missing_ids}")
    frame["candidate_id"] = frame["candidate_id"].astype(str)
    frame["parent_candidate_id"] = frame["parent_candidate_id"].astype(str)
    frame["event_session_id"] = frame["event_session_id"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["active_contract"] = frame["active_contract"].astype(str)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="raise")
    if bool((frame["timestamp"] >= DEVELOPMENT_END_EXCLUSIVE).any()):
        raise RoleConditionedEpochError("Q4-or-later event reached role epoch")
    for field in ("availability_timestamp", "source_bar_close", "decision_timestamp"):
        if field in frame:
            frame[field] = pd.to_datetime(frame[field], utc=True, errors="raise")
            if bool((frame[field] > frame["timestamp"]).any()):
                raise RoleConditionedEpochError(f"Future information in {field}")
    for field in ("gross_pnl", "cost", "net_pnl", "mae_dollars"):
        frame[field] = pd.to_numeric(frame[field], errors="raise").astype(float)
        if not np.isfinite(frame[field]).all():
            raise RoleConditionedEpochError(f"Non-finite mutation ledger {field}")
    if bool((frame["cost"] < 0.0).any()) or bool((frame["mae_dollars"] > 1e-9).any()):
        raise RoleConditionedEpochError("Invalid cost or MAE sign")
    if not np.allclose(
        frame["gross_pnl"].to_numpy() - frame["cost"].to_numpy(),
        frame["net_pnl"].to_numpy(),
        atol=1e-8,
        rtol=1e-10,
    ):
        raise RoleConditionedEpochError("Gross minus cost does not reconcile to net")
    keys = ["candidate_id", "timestamp", "event_session_id", "symbol", "active_contract"]
    if bool(frame.duplicated(keys).any()):
        raise RoleConditionedEpochError("Duplicate selected-elite event key")
    frame = frame.sort_values(
        ["timestamp", "event_session_id", "candidate_id", "symbol", "active_contract"],
        kind="mergesort",
    ).reset_index(drop=True)
    frame["holding_minutes"] = frame["candidate_id"].map(_holding_minutes)
    frame["completion_timestamp"] = frame["timestamp"] + pd.to_timedelta(
        frame["holding_minutes"], unit="m"
    )
    if bool((frame["completion_timestamp"] >= DEVELOPMENT_END_EXCLUSIVE).any()):
        raise RoleConditionedEpochError(
            "An event outcome would complete in protected Q4-or-later data"
        )
    frame["row_id"] = [
        _canonical_hash(
            [
                row.candidate_id,
                row.timestamp.isoformat(),
                row.event_session_id,
                row.symbol,
                row.active_contract,
            ]
        )[:24]
        for row in frame.itertuples(index=False)
    ]
    return frame


def _holding_minutes(candidate_id: str) -> int:
    """Conservative deterministic completion horizon from the frozen strategy ID."""

    match = re.search(r"_h(\d+)(?:_|$)", str(candidate_id))
    if match:
        return max(1, int(match.group(1)))
    lowered = str(candidate_id).lower()
    if "daily_cross" in lowered:
        return 120
    if "open_gap" in lowered:
        return 60
    # Unknown structures are never treated as instantaneously completed.
    return 120


def _maximum_drawdown(values: pd.Series | np.ndarray) -> float:
    array = np.asarray(values, dtype=float)
    if not len(array):
        return 0.0
    cumulative = np.cumsum(array)
    peak = np.maximum.accumulate(np.maximum(cumulative, 0.0))
    return abs(float(np.min(cumulative - peak)))


def _score_table(training: pd.DataFrame, mode: str) -> dict[str, float]:
    scores: dict[str, float] = {}
    for candidate_id, group in training.groupby("candidate_id", sort=True):
        ordered = group.sort_values("timestamp", kind="mergesort")
        adverse = max(float(ordered["mae_dollars"].abs().sum()), 1.0)
        drawdown = max(_maximum_drawdown(ordered["net_pnl"]), 1.0)
        net = float(ordered["net_pnl"].sum())
        if mode == "combine":
            score = (net / 9_000.0) / adverse
        elif mode == "xfa":
            daily = ordered.groupby("event_session_id", sort=True)["net_pnl"].sum()
            frequency = float((daily >= 200.0).mean()) if len(daily) else 0.0
            payout_progress = max(net, 0.0) / 5_000.0
            score = frequency + payout_progress / drawdown
        else:
            score = net / drawdown - adverse / max(len(ordered) * 4_500.0, 1.0)
        scores[str(candidate_id)] = float(score)
    return scores


def _ranked_candidates(scores: Mapping[str, float], all_ids: Iterable[str]) -> list[str]:
    return sorted(
        (str(value) for value in all_ids),
        key=lambda candidate_id: (-float(scores.get(candidate_id, -math.inf)), candidate_id),
    )


def _prior_training(frame: pd.DataFrame, timestamp: pd.Timestamp) -> pd.DataFrame:
    return frame[
        (frame["completion_timestamp"] < timestamp)
        & (frame["completion_timestamp"] < FIT_END_EXCLUSIVE)
    ]


def _rank_at(
    frame: pd.DataFrame,
    timestamp: pd.Timestamp,
    mode: str,
    final_scores: Mapping[str, float],
    all_ids: Iterable[str],
) -> list[str]:
    if timestamp >= FIT_END_EXCLUSIVE:
        return _ranked_candidates(final_scores, all_ids)
    return _ranked_candidates(_score_table(_prior_training(frame, timestamp), mode), all_ids)


def _source_end(frame: pd.DataFrame, timestamp: pd.Timestamp) -> pd.Timestamp:
    prior = frame.loc[
        frame["completion_timestamp"] < timestamp, "completion_timestamp"
    ]
    if len(prior):
        return pd.Timestamp(prior.max())
    return timestamp - pd.Timedelta(microseconds=1)


def _daily_correlation_pairs(training: pd.DataFrame) -> list[list[str]]:
    if training.empty:
        return []
    daily = training.pivot_table(
        index="event_session_id",
        columns="candidate_id",
        values="net_pnl",
        aggfunc="sum",
    )
    pairs: list[list[str]] = []
    identifiers = sorted(str(value) for value in daily.columns)
    for left_index, left in enumerate(identifiers):
        for right in identifiers[left_index + 1 :]:
            pair = daily[[left, right]].dropna()
            if len(pair) < 5:
                continue
            correlation = float(pair[left].corr(pair[right]))
            if np.isfinite(correlation) and correlation >= 0.65:
                pairs.append([left, right])
    return pairs


def _prior_risk_states(training: pd.DataFrame) -> list[float]:
    states: list[float] = []
    realized = 0.0
    peak = 0.0
    prior_mae: list[float] = []
    for _, group in training.sort_values("completion_timestamp").groupby(
        "completion_timestamp", sort=True
    ):
        drawdown = max(0.0, peak - realized)
        states.append(float(sum(prior_mae[-5:]) + drawdown))
        prior_mae.extend(abs(float(value)) for value in group["mae_dollars"])
        realized += float(group["net_pnl"].sum())
        peak = max(peak, realized)
    return states


def _fit_parameters(frame: pd.DataFrame) -> dict[str, Any]:
    training = frame[frame["completion_timestamp"] < FIT_END_EXCLUSIVE].copy()
    if training.empty:
        raise RoleConditionedEpochError("No completed 2023 event is available for fitting")
    all_ids = sorted(set(frame["candidate_id"].astype(str)))
    combine_scores = _score_table(training, "combine")
    xfa_scores = _score_table(training, "xfa")
    defensive_scores = _score_table(training, "defensive")
    mae_q90: dict[str, float] = {}
    contract_budgets: dict[str, int] = {}
    for candidate_id in all_ids:
        values = training.loc[
            training["candidate_id"].eq(candidate_id), "mae_dollars"
        ].abs()
        q90 = float(values.quantile(0.90)) if len(values) else 4_500.0
        mae_q90[candidate_id] = max(q90, 1.0)
        contract_budgets[candidate_id] = max(1, min(3, int(1_125.0 // max(q90, 1.0))))
    risk_states = _prior_risk_states(training)
    throttle_q75 = float(np.quantile(risk_states, 0.75)) if risk_states else 4_500.0
    payload: dict[str, Any] = {
        "fit_start": pd.Timestamp(training["timestamp"].min()).isoformat(),
        "fit_end_exclusive": "2024-01-01T00:00:00+00:00",
        "fit_event_count": int(len(training)),
        "fit_candidate_count": len(set(training["candidate_id"].astype(str))),
        "combine_scores": dict(sorted(combine_scores.items())),
        "combine_rank": _ranked_candidates(combine_scores, all_ids),
        "xfa_scores": dict(sorted(xfa_scores.items())),
        "xfa_rank": _ranked_candidates(xfa_scores, all_ids),
        "defensive_scores": dict(sorted(defensive_scores.items())),
        "defensive_rank": _ranked_candidates(defensive_scores, all_ids),
        "mae_q90_by_lineage": dict(sorted(mae_q90.items())),
        "micro_contract_budget_by_lineage": dict(sorted(contract_budgets.items())),
        "throttle_prior_risk_q75": throttle_q75,
        "redundant_candidate_pairs": _daily_correlation_pairs(training),
        "qualifying_day_realized_pnl_threshold": 200.0,
        "maximum_shared_micro_contracts": 15,
        "mll_distance": 4_500.0,
        "2023_decision_fit": "expanding_strictly_prior_completed_events",
        "2024_decision_fit": "frozen_complete_2023_only",
        "current_event_outcome_used": False,
    }
    payload["fit_hash"] = _canonical_hash(payload)
    return payload


def _event_contracts(
    frame: pd.DataFrame,
    structure: Mapping[str, Any],
    fit: Mapping[str, Any],
) -> tuple[np.ndarray, list[str], list[str]]:
    kind = str(structure["kind"])
    reference = int(structure["reference_contracts"])
    contracts = np.full(len(frame), reference, dtype=int)
    reasons = ["REFERENCE_UNCHANGED"] * len(frame)
    source_ends = [
        _source_end(frame, pd.Timestamp(timestamp)).isoformat()
        for timestamp in frame["timestamp"]
    ]
    all_ids = sorted(set(frame["candidate_id"].astype(str)))

    if kind in {"collision_rank_scheduler", "qualifying_frequency_scheduler"}:
        mode = "combine" if kind == "collision_rank_scheduler" else "xfa"
        final_scores = dict(fit[f"{mode}_scores"])
        for timestamp, group in frame.groupby("timestamp", sort=True):
            if len(group) <= 1 or group["event_session_id"].nunique() != 1:
                continue
            rank = _rank_at(frame, pd.Timestamp(timestamp), mode, final_scores, all_ids)
            rank_index = {candidate_id: index for index, candidate_id in enumerate(rank)}
            winner = min(
                group.index,
                key=lambda index: (
                    rank_index[str(frame.at[index, "candidate_id"])],
                    str(frame.at[index, "candidate_id"]),
                ),
            )
            for index in group.index:
                if index != winner:
                    contracts[index] = 0
                    reasons[index] = "SIMULTANEOUS_COLLISION_LOWER_FROZEN_OR_PRIOR_RANK"
                else:
                    reasons[index] = "SIMULTANEOUS_COLLISION_RANK_WINNER"
        return contracts, reasons, source_ends

    if kind == "prior_mae_micro_budget":
        realized = 0.0
        peak = 0.0
        pending: list[int] = []
        for timestamp, group in frame.groupby("timestamp", sort=True):
            timestamp = pd.Timestamp(timestamp)
            completed = [
                index
                for index in pending
                if pd.Timestamp(frame.at[index, "completion_timestamp"]) < timestamp
            ]
            for index in sorted(
                completed,
                key=lambda value: pd.Timestamp(
                    frame.at[value, "completion_timestamp"]
                ),
            ):
                realized += float(frame.at[index, "net_pnl"]) * int(contracts[index])
                peak = max(peak, realized)
            pending = [index for index in pending if index not in set(completed)]
            buffer = max(0.0, 4_500.0 + realized - peak)
            if timestamp >= FIT_END_EXCLUSIVE:
                budgets = dict(fit["micro_contract_budget_by_lineage"])
                mae = dict(fit["mae_q90_by_lineage"])
                rank = list(fit["combine_rank"])
            else:
                prior = _prior_training(frame, timestamp)
                scores = _score_table(prior, "combine")
                rank = _ranked_candidates(scores, all_ids)
                budgets = {}
                mae = {}
                for candidate_id in all_ids:
                    values = prior.loc[
                        prior["candidate_id"].eq(candidate_id), "mae_dollars"
                    ].abs()
                    q90 = float(values.quantile(0.90)) if len(values) else 4_500.0
                    mae[candidate_id] = max(q90, 1.0)
                    budgets[candidate_id] = max(
                        1, min(3, int(1_125.0 // max(q90, 1.0)))
                    )
            rank_index = {candidate_id: index for index, candidate_id in enumerate(rank)}
            estimated_budget = max(0.0, buffer * 0.25)
            used_contracts = 0
            used_adverse = 0.0
            for index in sorted(
                group.index,
                key=lambda value: (
                    rank_index[str(frame.at[value, "candidate_id"])],
                    str(frame.at[value, "candidate_id"]),
                ),
            ):
                candidate_id = str(frame.at[index, "candidate_id"])
                requested = int(budgets.get(candidate_id, 1))
                unit_mae = float(mae.get(candidate_id, 4_500.0))
                risk_cap = max(1, int(max(estimated_budget - used_adverse, 0.0) // unit_mae))
                allowed = max(1, min(requested, risk_cap, 15 - used_contracts))
                contracts[index] = allowed
                used_contracts += allowed
                used_adverse += allowed * unit_mae
                reasons[index] = "PRIOR_MAE_AND_SHARED_MLL_INTEGER_MICRO_BUDGET"
            pending.extend(int(index) for index in group.index)
        return contracts, reasons, source_ends

    if kind == "realized_qualifying_day_latch":
        realized_by_session: dict[str, float] = {}
        pending: list[int] = []
        for timestamp, group in frame.groupby("timestamp", sort=True):
            timestamp = pd.Timestamp(timestamp)
            completed = [
                index
                for index in pending
                if pd.Timestamp(frame.at[index, "completion_timestamp"]) < timestamp
            ]
            for index in sorted(
                completed,
                key=lambda value: pd.Timestamp(
                    frame.at[value, "completion_timestamp"]
                ),
            ):
                session = str(frame.at[index, "event_session_id"])
                realized_by_session[session] = realized_by_session.get(session, 0.0) + (
                    float(frame.at[index, "net_pnl"]) * int(contracts[index])
                )
            pending = [index for index in pending if index not in set(completed)]
            for session_id, session_group in group.groupby("event_session_id", sort=True):
                prior_realized = realized_by_session.get(str(session_id), 0.0)
                if prior_realized >= 200.0:
                    contracts[session_group.index] = 0
                    for index in session_group.index:
                        reasons[index] = "PRIOR_REALIZED_QUALIFYING_DAY_THRESHOLD_LATCHED"
                else:
                    for index in session_group.index:
                        reasons[index] = "QUALIFYING_DAY_THRESHOLD_NOT_YET_REALIZED"
            pending.extend(int(index) for index in group.index)
        return contracts, reasons, source_ends

    if kind == "redundant_collision_suppressor":
        frozen_pairs = {tuple(sorted(pair)) for pair in fit["redundant_candidate_pairs"]}
        final_scores = dict(fit["defensive_scores"])
        for timestamp, group in frame.groupby("timestamp", sort=True):
            if len(group) <= 1 or group["event_session_id"].nunique() != 1:
                continue
            timestamp = pd.Timestamp(timestamp)
            if timestamp >= FIT_END_EXCLUSIVE:
                pairs = frozen_pairs
            else:
                pairs = {
                    tuple(sorted(pair))
                    for pair in _daily_correlation_pairs(_prior_training(frame, timestamp))
                }
            rank = _rank_at(frame, timestamp, "defensive", final_scores, all_ids)
            rank_index = {candidate_id: index for index, candidate_id in enumerate(rank)}
            kept: list[str] = []
            for index in sorted(
                group.index,
                key=lambda value: (
                    rank_index[str(frame.at[value, "candidate_id"])],
                    str(frame.at[value, "candidate_id"]),
                ),
            ):
                candidate_id = str(frame.at[index, "candidate_id"])
                if any(tuple(sorted((candidate_id, other))) in pairs for other in kept):
                    contracts[index] = 0
                    reasons[index] = "REDUNDANT_SIMULTANEOUS_LOWER_PRIOR_RANK_SUPPRESSED"
                else:
                    kept.append(candidate_id)
                    reasons[index] = "NONREDUNDANT_OR_PRIOR_RANK_WINNER_RETAINED"
        return contracts, reasons, source_ends

    if kind == "prior_mae_quantile_throttle":
        realized = 0.0
        peak = 0.0
        prior_mae: list[float] = []
        prior_state_history: list[float] = []
        pending: list[int] = []
        for timestamp, group in frame.groupby("timestamp", sort=True):
            timestamp = pd.Timestamp(timestamp)
            completed = [
                index
                for index in pending
                if pd.Timestamp(frame.at[index, "completion_timestamp"]) < timestamp
            ]
            for index in sorted(
                completed,
                key=lambda value: pd.Timestamp(
                    frame.at[value, "completion_timestamp"]
                ),
            ):
                prior_mae.append(abs(float(frame.at[index, "mae_dollars"])))
                realized += float(frame.at[index, "net_pnl"]) * int(contracts[index])
                peak = max(peak, realized)
            pending = [index for index in pending if index not in set(completed)]
            state = float(sum(prior_mae[-5:]) + max(0.0, peak - realized))
            if timestamp >= FIT_END_EXCLUSIVE:
                threshold = float(fit["throttle_prior_risk_q75"])
            else:
                threshold = (
                    float(np.quantile(prior_state_history, 0.75))
                    if prior_state_history
                    else math.inf
                )
            throttled = state > threshold
            contracts[group.index] = 1 if throttled else 2
            for index in group.index:
                reasons[index] = (
                    "PRIOR_COMPLETED_MAE_DRAWDOWN_ABOVE_FROZEN_Q75_ONE_MICRO"
                    if throttled
                    else "PRIOR_COMPLETED_MAE_DRAWDOWN_WITHIN_Q75_TWO_MICROS"
                )
            prior_state_history.append(state)
            pending.extend(int(index) for index in group.index)
        return contracts, reasons, source_ends

    raise RoleConditionedEpochError(f"Unknown frozen policy kind: {kind}")


def _account_ledgers(
    frame: pd.DataFrame,
    contracts: np.ndarray,
    *,
    cost_multiplier: float,
) -> dict[str, pd.DataFrame]:
    selected = frame.loc[contracts > 0].copy()
    if selected.empty:
        return {}
    selected["contracts"] = contracts[selected.index]
    selected["strategy_id"] = selected["candidate_id"]
    selected["trade_id"] = selected["row_id"]
    selected["entry_timestamp"] = selected["timestamp"]
    selected["exit_timestamp"] = selected["completion_timestamp"]
    selected["cost"] = selected["cost"] * selected["contracts"] * cost_multiplier
    selected["net_pnl"] = (
        selected["gross_pnl"] * selected["contracts"] - selected["cost"]
    )
    selected["mae_dollars"] = selected["mae_dollars"] * selected["contracts"]
    selected["underlying"] = selected["symbol"].str.removeprefix("M")
    selected["side"] = 0.0
    selected["availability_timestamp"] = selected["entry_timestamp"]
    selected["source_bar_close"] = selected["entry_timestamp"]
    columns = (
        "strategy_id",
        "trade_id",
        "event_session_id",
        "entry_timestamp",
        "exit_timestamp",
        "net_pnl",
        "mae_dollars",
        "cost",
        "contracts",
        "underlying",
        "side",
        "availability_timestamp",
        "source_bar_close",
    )
    return {
        str(candidate_id): group.loc[:, columns].reset_index(drop=True)
        for candidate_id, group in selected.groupby("candidate_id", sort=True)
    }


def _replay(
    frame: pd.DataFrame,
    contracts: np.ndarray,
    *,
    cost_multiplier: float,
) -> dict[str, Any]:
    return replay_shared_account(
        _account_ledgers(frame, contracts, cost_multiplier=cost_multiplier),
        config=AccountReplayConfig(maximum_simultaneous_contracts=15),
    ).to_dict()


def _utility(metrics: Mapping[str, Any], pool: str) -> float:
    field = {
        "COMBINE_PASSER_POOL": "combine_utility",
        "XFA_PAYOUT_POOL": "xfa_utility",
        "DEFENSIVE_ACCOUNT_POOL": "defensive_utility",
    }[pool]
    return float(metrics[field])


def _period_masks(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    timestamp = frame["completion_timestamp"]
    return {
        "2023": (timestamp < FIT_END_EXCLUSIVE).to_numpy(),
        "2024_Q1": ((timestamp >= FIT_END_EXCLUSIVE) & (timestamp < pd.Timestamp("2024-04-01", tz="UTC"))).to_numpy(),
        "2024_Q2": ((timestamp >= pd.Timestamp("2024-04-01", tz="UTC")) & (timestamp < pd.Timestamp("2024-07-01", tz="UTC"))).to_numpy(),
        "2024_Q3": ((timestamp >= pd.Timestamp("2024-07-01", tz="UTC")) & (timestamp < DEVELOPMENT_END_EXCLUSIVE)).to_numpy(),
        "POOLED_DEVELOPMENT": np.ones(len(frame), dtype=bool),
    }


def _period_evidence(
    frame: pd.DataFrame,
    observed: np.ndarray,
    reference: np.ndarray,
    pool: str,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for period, mask in _period_masks(frame).items():
        subset = frame.loc[mask].copy().reset_index(drop=True)
        observed_subset = observed[mask]
        reference_subset = reference[mask]
        policy_1x = _replay(subset, observed_subset, cost_multiplier=1.0)
        reference_1x = _replay(subset, reference_subset, cost_multiplier=1.0)
        policy_15x = _replay(subset, observed_subset, cost_multiplier=1.5)
        reference_15x = _replay(subset, reference_subset, cost_multiplier=1.5)
        evidence[period] = {
            "event_count": int(mask.sum()),
            "policy_1_0x": policy_1x,
            "reference_1_0x": reference_1x,
            "policy_1_5x": policy_15x,
            "reference_1_5x": reference_15x,
            "primary_utility_delta_1_0x": _utility(policy_1x, pool)
            - _utility(reference_1x, pool),
            "primary_utility_delta_1_5x": _utility(policy_15x, pool)
            - _utility(reference_15x, pool),
            "net_pnl_delta_1_0x": float(policy_1x["total_net_pnl"])
            - float(reference_1x["total_net_pnl"]),
            "net_pnl_delta_1_5x": float(policy_15x["total_net_pnl"])
            - float(reference_15x["total_net_pnl"]),
        }
    return evidence


def _shift_session_patterns(
    frame: pd.DataFrame, observed: np.ndarray, offset: int
) -> np.ndarray:
    sessions = list(
        frame.groupby("event_session_id", sort=False)["timestamp"].min().sort_values().index
    )
    if len(sessions) <= 1:
        return observed.copy()
    patterns = {
        str(session): observed[
            frame.index[frame["event_session_id"].eq(str(session))].to_numpy()
        ].tolist()
        for session in sessions
    }
    shifted = np.ones(len(frame), dtype=int)
    for target_index, target_session in enumerate(sessions):
        source_session = sessions[(target_index - offset) % len(sessions)]
        source_pattern = patterns[str(source_session)]
        target_rows = frame.index[frame["event_session_id"].eq(str(target_session))].to_numpy()
        for within_index, row_index in enumerate(target_rows):
            shifted[row_index] = int(source_pattern[within_index % len(source_pattern)])
    return shifted


def _matched_controls(
    frame: pd.DataFrame,
    observed: np.ndarray,
    reference: np.ndarray,
    *,
    structure: Mapping[str, Any],
    control_count: int,
) -> dict[str, Any]:
    if control_count < 1:
        raise RoleConditionedEpochError("control_count must be positive")
    pool = str(structure["objective_pool"])
    observed_delta = _control_utility_proxy(frame, observed, pool) - _control_utility_proxy(
        frame, reference, pool
    )
    seed = int(hashlib.sha256(str(structure["policy_id"]).encode()).hexdigest()[:16], 16) % (2**32)
    rng = np.random.default_rng(seed)
    values: list[float] = []
    method = str(structure["control"])
    if method == "circular_session_shift":
        session_count = int(frame["event_session_id"].nunique())
        offsets = rng.integers(1, max(session_count, 2), size=control_count)
        cache: dict[int, float] = {}
        for offset in sorted(set(int(value) for value in offsets)):
            control = _shift_session_patterns(frame, observed, offset)
            cache[offset] = _control_utility_proxy(
                frame, control, pool
            ) - _control_utility_proxy(frame, reference, pool)
        values = [float(cache[int(offset)]) for offset in offsets]
        matching = "circular_session_shift_of_complete_policy_decision_patterns"
    else:
        changed = np.flatnonzero(observed != reference)
        count = len(changed)
        target_contracts = int(np.min(observed[changed])) if count else int(reference[0])
        for _ in range(control_count):
            control = reference.copy()
            if count:
                chosen = rng.choice(len(frame), size=count, replace=False)
                control[chosen] = target_contracts
            values.append(
                _control_utility_proxy(frame, control, pool)
                - _control_utility_proxy(frame, reference, pool)
            )
        matching = "fixed_seed_count_matched_random_policy_changes"
    exceed = sum(value >= observed_delta - 1e-15 for value in values)
    return {
        "method": method,
        "matching": matching,
        "seed": seed,
        "control_count": control_count,
        "changed_event_count": int(np.sum(observed != reference)),
        "observed_primary_utility_delta": observed_delta,
        "one_sided_p_value": float((1 + exceed) / (1 + control_count)),
        "control_delta_mean": float(np.mean(values)),
        "control_delta_quantiles": [
            float(value) for value in np.quantile(values, [0.05, 0.50, 0.95])
        ],
        "control_values_hash": _canonical_hash(values),
        "operational_policy": False,
        "control_utility_proxy_version": "daily_account_role_proxy_v1",
    }


def _control_utility_proxy(
    frame: pd.DataFrame, contracts: np.ndarray, pool: str
) -> float:
    """Fast, preregistered-role control score; primary evidence still uses full replay."""

    active = np.asarray(contracts, dtype=int)
    net = frame["gross_pnl"].to_numpy(dtype=float) * active - frame[
        "cost"
    ].to_numpy(dtype=float) * active
    daily = (
        pd.DataFrame(
            {"session": frame["event_session_id"].astype(str), "net": net}
        )
        .groupby("session", sort=True)["net"]
        .sum()
    )
    if daily.empty:
        return -10.0
    values = daily.to_numpy(dtype=float)
    total = float(values.sum())
    cumulative = np.cumsum(values)
    peak = np.maximum.accumulate(np.maximum(cumulative, 0.0))
    drawdown = abs(float(np.min(cumulative - peak)))
    survived = float(drawdown < 4_500.0)
    tail_loss = abs(min(0.0, float(np.quantile(values, 0.10))))
    best_day = max(0.0, float(values.max()))
    concentration = best_day / total if total > 0.0 else 1.0
    qualifying = float(np.mean(values >= 200.0))
    shared_loss_fraction = float(np.mean(values < 0.0))
    if pool == "COMBINE_PASSER_POOL":
        return float(
            2.0 * float(total >= 9_000.0 and survived)
            + np.clip(total / 9_000.0, -1.0, 1.0)
            + (0.50 - concentration)
            - drawdown / 4_500.0
            - tail_loss / 4_500.0
        )
    if pool == "XFA_PAYOUT_POOL":
        return float(
            min(3.0, math.floor(max(total, 0.0) / 5_000.0))
            + qualifying
            + survived
            + float(total >= 5_000.0 and survived)
            - drawdown / 4_500.0
        )
    return float(
        survived
        - drawdown / 4_500.0
        - shared_loss_fraction
        - tail_loss / 4_500.0
    )


def _core_risk_supported(
    pool: str, policy: Mapping[str, Any], reference: Mapping[str, Any]
) -> bool:
    if bool(policy["mll_breached"]) or bool(policy["contract_limit_breached"]):
        return False
    if pool == "COMBINE_PASSER_POOL":
        return bool(
            float(policy["min_mll_buffer"]) >= float(reference["min_mll_buffer"]) - 1e-9
            and float(policy["tail_loss_p90"]) <= float(reference["tail_loss_p90"]) + 1e-9
        )
    if pool == "XFA_PAYOUT_POOL":
        return bool(
            float(policy["min_mll_buffer"]) >= float(reference["min_mll_buffer"]) - 1e-9
            and (not bool(reference["post_payout_survival"]) or bool(policy["post_payout_survival"]))
        )
    return bool(
        float(policy["maximum_drawdown"]) <= float(reference["maximum_drawdown"]) + 1e-9
        and float(policy["min_mll_buffer"]) >= float(reference["min_mll_buffer"]) - 1e-9
        and int(policy["shared_loss_days"]) <= int(reference["shared_loss_days"])
    )


def _dominance_vector(row: Mapping[str, Any]) -> tuple[float, ...]:
    pooled = row["periods"]["POOLED_DEVELOPMENT"]
    policy = pooled["policy_1_0x"]
    reference = pooled["reference_1_0x"]
    pool = str(row["objective_pool"])
    if pool == "COMBINE_PASSER_POOL":
        return (
            float(pooled["primary_utility_delta_1_0x"]),
            float(policy["target_velocity_dollars_per_day"])
            - float(reference["target_velocity_dollars_per_day"]),
            float(policy["min_mll_buffer"]) - float(reference["min_mll_buffer"]),
            -float(policy["execution_cost_burden"]),
        )
    if pool == "XFA_PAYOUT_POOL":
        return (
            float(pooled["primary_utility_delta_1_0x"]),
            float(policy["payout_cycles_before_ruin"])
            - float(reference["payout_cycles_before_ruin"]),
            float(policy["qualifying_day_frequency"])
            - float(reference["qualifying_day_frequency"]),
            float(policy["min_mll_buffer"]) - float(reference["min_mll_buffer"]),
        )
    return (
        float(pooled["primary_utility_delta_1_0x"]),
        float(reference["maximum_drawdown"]) - float(policy["maximum_drawdown"]),
        float(policy["min_mll_buffer"]) - float(reference["min_mll_buffer"]),
        float(reference["shared_loss_days"]) - float(policy["shared_loss_days"]),
    )


def _mark_pareto(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    archive: dict[str, list[str]] = {}
    for pool in POOLS:
        members = [row for row in rows if row["objective_pool"] == pool and not row["behaviorally_duplicate"]]
        vectors = [_dominance_vector(row) for row in members]
        selected: list[str] = []
        for index, row in enumerate(members):
            dominated = any(
                other != index
                and all(left >= right for left, right in zip(vectors[other], vectors[index]))
                and any(left > right for left, right in zip(vectors[other], vectors[index]))
                for other in range(len(members))
            )
            row["pareto_front"] = not dominated
            if not dominated:
                selected.append(str(row["policy_id"]))
        archive[pool] = sorted(selected)
    return archive


def _atomic_immutable(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise RoleConditionedEpochError(f"Immutable artifact drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _artifact_entry(path: Path) -> dict[str, str]:
    return {"path": str(path.resolve()), "sha256": _sha256(path)}


def _render_report(result: Mapping[str, Any]) -> str:
    lines = [
        "# HYDRA role-conditioned structural epoch",
        "",
        f"- Protocol: `{POLICY_VERSION}`",
        f"- Six frozen account policies evaluated: {result['policy_count']}",
        f"- Nonduplicate structural prototypes: {result['structural_prototypes']}",
        f"- Promising account-role policies: {result['promising_candidate_count']}",
        f"- Behavioral duplicates rejected: {result['behavioral_duplicate_count']}",
        "- Fit: expanding past-only during 2023; frozen complete-2023 parameters in 2024 Q1-Q3",
        "- Cost replays: 1.0x and 1.5x",
        "- Q4/network/paid data/orders: 0",
        "- PAPER_SHADOW_READY / SHADOW_RESEARCH_ACTIVE: 0 / 0",
        "",
        "These are account policies, not new alpha mechanisms and not independent temporal replication.",
        "",
    ]
    for row in result["candidates"]:
        lines.append(
            f"- `{row['policy_id']}`: `{row['status']}`; utility delta "
            f"{row['primary_utility_delta']:.6f}; matched p={row['matched_controls']['one_sided_p_value']:.6f}"
        )
    lines.append("")
    return "\n".join(lines)


def run_role_conditioned_structural_epoch(
    output_dir: Path,
    *,
    engineering_task_path: Path,
    engineering_task_sha256: str,
    mutation_result_path: Path,
    mutation_result_sha256: str,
    mutation_result_hash: str,
    mutation_ledger_path: Path,
    mutation_ledger_sha256: str,
    halving_result_path: Path,
    halving_result_sha256: str,
    halving_result_hash: str,
    halving_evidence_path: Path,
    halving_evidence_sha256: str,
    halving_manifest_path: Path,
    halving_manifest_sha256: str,
    halving_manifest_hash: str,
    portfolio_role_result_path: Path,
    portfolio_role_result_sha256: str,
    portfolio_role_result_hash: str,
    meta_result_path: Path,
    meta_result_sha256: str,
    meta_result_hash: str,
    code_commit: str,
    control_count: int = 63,
) -> dict[str, Any]:
    """Run the preregistered role epoch and write immutable evidence artifacts."""

    paths = {
        "engineering task": Path(engineering_task_path),
        "mutation result": Path(mutation_result_path),
        "mutation ledger": Path(mutation_ledger_path),
        "halving result": Path(halving_result_path),
        "halving evidence": Path(halving_evidence_path),
        "halving manifest": Path(halving_manifest_path),
        "portfolio-role result": Path(portfolio_role_result_path),
        "meta-allocation result": Path(meta_result_path),
    }
    expected = {
        "engineering task": engineering_task_sha256,
        "mutation result": mutation_result_sha256,
        "mutation ledger": mutation_ledger_sha256,
        "halving result": halving_result_sha256,
        "halving evidence": halving_evidence_sha256,
        "halving manifest": halving_manifest_sha256,
        "portfolio-role result": portfolio_role_result_sha256,
        "meta-allocation result": meta_result_sha256,
    }
    for label, path in paths.items():
        _verify_file(path, str(expected[label]), label)
    if engineering_task_sha256 != PREREGISTRATION_SHA256:
        raise RoleConditionedEpochError("Unexpected role-epoch preregistration")
    if not str(code_commit).strip():
        raise RoleConditionedEpochError("code_commit is required")

    mutation_result = _load_json(paths["mutation result"], "mutation result")
    halving_result = _load_json(paths["halving result"], "halving result")
    halving_manifest = _load_json(paths["halving manifest"], "halving manifest")
    portfolio_result = _load_json(paths["portfolio-role result"], "portfolio-role result")
    meta_result = _load_json(paths["meta-allocation result"], "meta-allocation result")
    selected_ids = _validate_sources(
        mutation_result=mutation_result,
        mutation_result_sha256=mutation_result_sha256,
        mutation_result_hash=mutation_result_hash,
        mutation_ledger_sha256=mutation_ledger_sha256,
        halving_result=halving_result,
        halving_result_hash=halving_result_hash,
        halving_evidence_sha256=halving_evidence_sha256,
        halving_manifest=halving_manifest,
        halving_manifest_hash=halving_manifest_hash,
        portfolio_result=portfolio_result,
        portfolio_result_hash=portfolio_role_result_hash,
        meta_result=meta_result,
        meta_result_hash=meta_result_hash,
    )
    if str((halving_result.get("artifacts") or {}).get("elite_manifest", {}).get("sha256") or "") != str(
        halving_manifest_sha256
    ):
        raise RoleConditionedEpochError("Halving result/manifest file hash mismatch")
    evidence_rows = _load_jsonl(paths["halving evidence"], "halving evidence")
    selected_evidence = {
        str(row.get("candidate_id")): row
        for row in evidence_rows
        if bool(row.get("selected_elite"))
    }
    if sorted(selected_evidence) != selected_ids:
        raise RoleConditionedEpochError("Halving selected evidence mismatch")
    if any(str(row.get("status")) != "PROMISING_RESEARCH_CANDIDATE" for row in selected_evidence.values()):
        raise RoleConditionedEpochError("A selected elite lacks promising research evidence")

    frame = _normalize_mutation_ledger(
        _load_jsonl(paths["mutation ledger"], "mutation ledger"), set(selected_ids)
    )
    fit = _fit_parameters(frame)
    candidate_rows: list[dict[str, Any]] = []
    transformed_rows: list[dict[str, Any]] = []
    behavior_owners: dict[str, str] = {}

    for structure in STRUCTURES:
        policy_id = str(structure["policy_id"])
        pool = str(structure["objective_pool"])
        observed, reasons, source_ends = _event_contracts(frame, structure, fit)
        reference = np.full(len(frame), int(structure["reference_contracts"]), dtype=int)
        structural_payload = {
            **dict(structure),
            "protocol": POLICY_VERSION,
            "fit_hash": fit["fit_hash"],
            "selected_candidate_ids": selected_ids,
            "decision_availability": "strictly_prior_completed_events_only",
        }
        structural_fingerprint = _canonical_hash(structural_payload)
        behavior_fingerprint = _canonical_hash(
            [[str(row_id), int(value)] for row_id, value in zip(frame["row_id"], observed)]
        )
        owner = behavior_owners.get(behavior_fingerprint)
        behaviorally_duplicate = owner is not None
        if owner is None:
            behavior_owners[behavior_fingerprint] = policy_id
        periods = _period_evidence(frame, observed, reference, pool)
        pooled = periods["POOLED_DEVELOPMENT"]
        policy_path = pooled["policy_1_0x"]
        reference_path = pooled["reference_1_0x"]
        controls = _matched_controls(
            frame,
            observed,
            reference,
            structure=structure,
            control_count=control_count,
        )
        hard_risk = bool(policy_path["mll_breached"] or policy_path["contract_limit_breached"])
        core_risk = _core_risk_supported(pool, policy_path, reference_path)
        primary_delta = float(pooled["primary_utility_delta_1_0x"])
        promising = bool(
            not behaviorally_duplicate
            and primary_delta > 0.0
            and not hard_risk
            and float(controls["one_sided_p_value"]) <= 0.20
            and core_risk
        )
        status = "PROMISING_RESEARCH_CANDIDATE" if promising else "INSUFFICIENT_EVIDENCE"
        uncertainty: list[str] = []
        if behaviorally_duplicate:
            uncertainty.append("BEHAVIORAL_DUPLICATE_REJECTED")
        if primary_delta <= 0.0:
            uncertainty.append("PRIMARY_ROLE_UTILITY_NOT_IMPROVED")
        if float(controls["one_sided_p_value"]) > 0.20:
            uncertainty.append("MATCHED_CONTROL_NOT_DISCRIMINATIVE")
        if not core_risk:
            uncertainty.append("ROLE_CORE_RISK_METRIC_DETERIORATED")
        if float(pooled["policy_1_5x"]["total_net_pnl"]) <= 0.0:
            uncertainty.append("ONE_POINT_FIVE_COST_NET_NONPOSITIVE")
        row: dict[str, Any] = {
            "candidate_id": policy_id,
            "policy_id": policy_id,
            "status": status,
            "source_statuses_are_evidence_only": True,
            "status_inherited": False,
            "inherited_passes": [],
            "objective_pool": pool,
            "role": "PORTFOLIO",
            "primary_market": "PORTFOLIO",
            "mechanism_family": f"account_policy_{structure['kind']}",
            "account_policy_only": True,
            "independent_alpha_mechanism": False,
            "structural_fingerprint": structural_fingerprint,
            "behavior_fingerprint": behavior_fingerprint,
            "behaviorally_duplicate": behaviorally_duplicate,
            "duplicate_of": owner,
            "fit_hash": fit["fit_hash"],
            "reference_contracts": int(structure["reference_contracts"]),
            "changed_event_count": int(np.sum(observed != reference)),
            "retained_event_count": int(np.sum(observed > 0)),
            "maximum_policy_contracts": int(observed.max()) if len(observed) else 0,
            "periods": periods,
            "primary_utility_delta": primary_delta,
            "matched_controls": controls,
            "hard_risk_violation": hard_risk,
            "core_risk_not_deteriorated": core_risk,
            "uncertainty_flags": sorted(uncertainty),
            "pareto_front": False,
            "q4_access_count": 0,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "order_capability": False,
            "shadow_research_active": False,
            "paper_shadow_ready": False,
            "promotion_eligible": False,
        }
        row["evidence_hash"] = _canonical_hash(row)
        candidate_rows.append(row)
        for index, source in frame.iterrows():
            contract_count = int(observed[index])
            transformed_rows.append(
                {
                    "policy_id": policy_id,
                    "objective_pool": pool,
                    "row_id": str(source["row_id"]),
                    "candidate_id": str(source["candidate_id"]),
                    "parent_candidate_id": str(source["parent_candidate_id"]),
                    "event_session_id": str(source["event_session_id"]),
                    "signal_timestamp": pd.Timestamp(source["timestamp"]).isoformat(),
                    "decision_timestamp": pd.Timestamp(source["timestamp"]).isoformat(),
                    "completion_timestamp": pd.Timestamp(
                        source["completion_timestamp"]
                    ).isoformat(),
                    "holding_minutes": int(source["holding_minutes"]),
                    "state_available_through": source_ends[index],
                    "strictly_prior_policy_state": pd.Timestamp(source_ends[index])
                    < pd.Timestamp(source["timestamp"]),
                    "symbol": str(source["symbol"]),
                    "active_contract": str(source["active_contract"]),
                    "included": contract_count > 0,
                    "micro_contracts": contract_count,
                    "decision_reason": reasons[index],
                    "gross_pnl_1_0x": float(source["gross_pnl"] * contract_count),
                    "cost_1_0x": float(source["cost"] * contract_count),
                    "net_pnl_1_0x": float(
                        source["gross_pnl"] * contract_count
                        - source["cost"] * contract_count
                    ),
                    "cost_1_5x": float(source["cost"] * contract_count * 1.5),
                    "net_pnl_1_5x": float(
                        source["gross_pnl"] * contract_count
                        - source["cost"] * contract_count * 1.5
                    ),
                    "mae_dollars": float(source["mae_dollars"] * contract_count),
                    "current_event_outcome_used_for_decision": False,
                }
            )

    pareto = _mark_pareto(candidate_rows)
    # Evidence hashes include the pre-Pareto row by design.  Pareto membership
    # is an archive property and cannot alter candidate-level evidence.
    manifest_policies = [
        {
            "policy_id": row["policy_id"],
            "objective_pool": row["objective_pool"],
            "structural_fingerprint": row["structural_fingerprint"],
            "behavior_fingerprint": row["behavior_fingerprint"],
            "behaviorally_duplicate": row["behaviorally_duplicate"],
            "status_inherited": False,
            "status_ceiling": STATUS_CEILING,
        }
        for row in candidate_rows
    ]
    source_manifest = {
        "engineering_task": {"path": str(paths["engineering task"]), "sha256": engineering_task_sha256},
        "mutation_result": {"path": str(paths["mutation result"]), "sha256": mutation_result_sha256, "result_hash": mutation_result_hash},
        "mutation_ledger": {"path": str(paths["mutation ledger"]), "sha256": mutation_ledger_sha256},
        "halving_result": {"path": str(paths["halving result"]), "sha256": halving_result_sha256, "result_hash": halving_result_hash},
        "halving_evidence": {"path": str(paths["halving evidence"]), "sha256": halving_evidence_sha256},
        "halving_manifest": {"path": str(paths["halving manifest"]), "sha256": halving_manifest_sha256, "manifest_hash": halving_manifest_hash},
        "portfolio_role_result": {"path": str(paths["portfolio-role result"]), "sha256": portfolio_role_result_sha256, "result_hash": portfolio_role_result_hash},
        "meta_result": {"path": str(paths["meta-allocation result"]), "sha256": meta_result_sha256, "result_hash": meta_result_hash},
    }
    manifest: dict[str, Any] = {
        "schema": "hydra_role_conditioned_policy_manifest_v1",
        "policy_version": POLICY_VERSION,
        "code_commit": str(code_commit),
        "source_artifacts": source_manifest,
        "selected_candidate_ids": selected_ids,
        "policy_count": 6,
        "pool_counts": {pool: 2 for pool in POOLS},
        "policies": manifest_policies,
        "fit_parameters": fit,
        "status_ceiling": STATUS_CEILING,
        "q4_access_count": 0,
        "network_requests": 0,
        "incremental_databento_spend_usd": 0.0,
        "order_capability": False,
        "paper_shadow_ready": 0,
        "shadow_research_active": 0,
    }
    manifest["manifest_hash"] = _canonical_hash(manifest)
    archive: dict[str, Any] = {
        "schema": "hydra_role_conditioned_pareto_archive_v1",
        "policy_version": POLICY_VERSION,
        "niches": pareto,
        "vectors": {
            str(row["policy_id"]): list(_dominance_vector(row)) for row in candidate_rows
        },
        "account_policies_not_alpha_mechanisms": True,
    }
    archive["archive_hash"] = _canonical_hash(archive)

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    evidence_path = output / "role_conditioned_policy_evidence.jsonl"
    ledger_path = output / "role_conditioned_transformed_event_ledger.jsonl"
    manifest_path = output / "role_conditioned_policy_manifest.json"
    archive_path = output / "role_conditioned_pareto_archive.json"
    report_path = output / "role_conditioned_structural_epoch_report.md"
    result_path = output / "role_conditioned_structural_epoch_result.json"
    _atomic_immutable(
        evidence_path,
        b"".join(_canonical_bytes(row) + b"\n" for row in sorted(candidate_rows, key=lambda item: str(item["policy_id"]))),
    )
    _atomic_immutable(
        ledger_path,
        b"".join(
            _canonical_bytes(row) + b"\n"
            for row in sorted(
                transformed_rows,
                key=lambda item: (str(item["policy_id"]), str(item["signal_timestamp"]), str(item["candidate_id"]), str(item["row_id"])),
            )
        ),
    )
    _atomic_immutable(manifest_path, json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False).encode() + b"\n")
    _atomic_immutable(archive_path, json.dumps(archive, indent=2, sort_keys=True, allow_nan=False).encode() + b"\n")
    artifacts = {
        "policy_evidence": _artifact_entry(evidence_path),
        "transformed_event_ledger": _artifact_entry(ledger_path),
        "policy_manifest": _artifact_entry(manifest_path),
        "pareto_archive": _artifact_entry(archive_path),
    }
    status_counts = Counter(str(row["status"]) for row in candidate_rows)
    result: dict[str, Any] = {
        "schema": "hydra_role_conditioned_structural_epoch_result_v1",
        "policy_version": POLICY_VERSION,
        "code_commit": str(code_commit),
        "engineering_task_sha256": engineering_task_sha256,
        "source_artifact_hash": _canonical_hash(source_manifest),
        "selected_candidate_count": len(selected_ids),
        "selected_candidate_ids": selected_ids,
        "policy_count": 6,
        "candidate_count": 6,
        "structural_prototypes": sum(not row["behaviorally_duplicate"] for row in candidate_rows),
        "behavioral_duplicate_count": sum(row["behaviorally_duplicate"] for row in candidate_rows),
        "promising_candidate_count": status_counts["PROMISING_RESEARCH_CANDIDATE"],
        "promising_candidates": status_counts["PROMISING_RESEARCH_CANDIDATE"],
        "insufficient_evidence_count": status_counts["INSUFFICIENT_EVIDENCE"],
        "hard_integrity_rejected": 0,
        "status_counts": dict(sorted(status_counts.items())),
        "matched_control_count": int(6 * control_count),
        "pool_counts": {pool: 2 for pool in POOLS},
        "candidates": sorted(candidate_rows, key=lambda item: str(item["policy_id"])),
        "pareto_candidate_ids_by_pool": pareto,
        "fit_hash": fit["fit_hash"],
        "fitting_scope": "2023_expanding_prior_only_then_frozen_for_2024_Q1_Q3",
        "development_end_exclusive": "2024-10-01T00:00:00+00:00",
        "status_ceiling": STATUS_CEILING,
        "account_policy_prototypes": 6,
        "independent_alpha_mechanisms_created": 0,
        "q4_access_count": 0,
        "network_requests": 0,
        "paid_data_requests": 0,
        "incremental_databento_spend_usd": 0.0,
        "broker_connections": 0,
        "outbound_orders": 0,
        "order_capability": False,
        "paper_shadow_ready": 0,
        "shadow_research_active": 0,
        "artifacts": artifacts,
        "scientific_conclusion": (
            "ROLE_CONDITIONED_ACCOUNT_POLICIES_PROMISING"
            if status_counts["PROMISING_RESEARCH_CANDIDATE"]
            else "ROLE_CONDITIONED_ACCOUNT_POLICY_EVIDENCE_INSUFFICIENT"
        ),
        "next_action": (
            "FREEZE_PROMISING_ROLE_POLICIES_FOR_ROLE_SPECIFIC_PROMOTION"
            if status_counts["PROMISING_RESEARCH_CANDIDATE"]
            else "PIVOT_TO_DISTINCT_MECHANISM_OR_MARKET_ECOLOGY"
        ),
        "interpretation_boundary": (
            "The 2024 grammar is development-influenced. These six structures are account "
            "policies, not independent alpha mechanisms, shadow activations, Paper evidence, "
            "or funded-deployment evidence."
        ),
    }
    # Report content depends on result fields but is itself authenticated by the
    # final result.  The result hash remains path independent.
    _atomic_immutable(report_path, _render_report(result).encode("utf-8"))
    result["artifacts"]["report"] = _artifact_entry(report_path)
    hash_payload = dict(result)
    hash_payload["artifacts"] = {
        key: value["sha256"] for key, value in sorted(result["artifacts"].items())
    }
    result["result_hash"] = _canonical_hash(hash_payload)
    _atomic_immutable(result_path, json.dumps(result, indent=2, sort_keys=True, allow_nan=False).encode() + b"\n")
    returned = dict(result)
    returned["artifacts"] = dict(result["artifacts"])
    returned["artifacts"]["result"] = _artifact_entry(result_path)
    return returned
