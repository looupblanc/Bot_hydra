from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import DatabentoBudgetConfig
from hydra.data.q4_data_plan import acquire_q4_data
from hydra.features.feature_matrix import FeatureMatrix
from hydra.governance.q4_one_shot import AuthorizedQ4Capability
from hydra.mission.calibration_retest_execution import _build_past_only_feature_frame
from hydra.portfolio.account_contribution import (
    compare_account_contribution,
    matched_random_inclusion_controls,
)
from hydra.portfolio.strategy_role import StrategyPool
from hydra.promotion.evidence_conversion import _risk_metrics
from hydra.research.qd_economic_tournament import _prepare_feature_frame
from hydra.research.turbo_exact_replay import _array_compare, _non_overlapping, spec_from_dict
from hydra.research.turbo_feature_builder import _market_arrays
from hydra.validation.q4_atomic_runner import classify_role_specific_q4_result


Q4_START = "2024-10-01"
Q4_END = "2025-01-01"


class Q4MarketEvaluationError(RuntimeError):
    pass


def evaluate_q4_from_data_plan(
    manifest: Mapping[str, Any],
    capability: AuthorizedQ4Capability,
    data_plan: Mapping[str, Any],
    *,
    budget: DatabentoBudgetConfig | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    receipt = acquire_q4_data(
        data_plan,
        capability,
        budget=budget,
        client=client,
    )
    frames, provenance = load_q4_dbn_frames(
        [
            {"path": row["path"], "sha256": row["sha256"]}
            for row in receipt["sources"]
        ],
        capability,
    )
    required_markets = {
        str(candidate.get("primary_market"))
        for candidate in manifest.get("candidates") or []
    }
    if not required_markets <= set(frames):
        raise Q4MarketEvaluationError(
            f"Protected Q4 coverage missing: {sorted(required_markets - set(frames))}"
        )
    _validate_definition_store(receipt["definitions"], capability)
    candidate_results = evaluate_q4_frames(manifest, capability, frames)
    return {
        "candidate_results": candidate_results,
        "run_metadata": {
            "data_acquisition": receipt,
            "data_provenance": provenance,
            "definitions_verified": True,
            "q4_rows_by_root": {
                root: int(len(frame)) for root, frame in sorted(frames.items())
            },
        },
    }


def evaluate_q4_frames(
    manifest: Mapping[str, Any],
    capability: AuthorizedQ4Capability,
    frames: Mapping[str, pd.DataFrame],
) -> list[dict[str, Any]]:
    capability.validate_scope()
    if str(manifest.get("manifest_hash")) != capability.cohort_manifest_hash:
        raise Q4MarketEvaluationError("Q4 frame evaluator capability/manifest mismatch.")
    matrices = {
        market: _frame_to_matrix(frame, market)
        for market, frame in sorted(frames.items())
    }
    preliminary: dict[str, dict[str, Any]] = {}
    for candidate in manifest.get("candidates") or []:
        candidate_id = str(candidate["candidate_id"])
        specification = dict(candidate["specification"])
        spec = spec_from_dict(specification)
        if spec.market not in matrices:
            raise Q4MarketEvaluationError(
                f"Q4 frame missing for frozen market {spec.market}."
            )
        matrix = matrices[spec.market]
        positions = _q4_positions(specification, matrix)
        selected_micro_contracts = int(candidate.get("selected_micro_contracts") or 1)
        risk_candidate = {
            "candidate_id": candidate_id,
            "execution_market": candidate.get("execution_market"),
            "topstep": {"selected_micro_contracts": selected_micro_contracts},
        }
        risk = _risk_metrics(
            risk_candidate,
            spec,
            matrix,
            positions,
            {"complete": True},
        )
        metrics = _metrics_from_risk(risk)
        metrics["feature_matrix_fingerprint"] = matrix.fingerprint
        metrics["candidate_specification_hash"] = candidate["specification_hash"]
        metrics["trade_ledger"] = list(risk.get("account_trade_ledger") or [])
        preliminary[candidate_id] = metrics

    non_defensive = {
        str(candidate["candidate_id"]): preliminary[str(candidate["candidate_id"])][
            "trade_ledger"
        ]
        for candidate in manifest.get("candidates") or []
        if str(candidate.get("role")) not in {"DEFENSIVE", "DEFENSIVE_ACCOUNT", "PORTFOLIO_ONLY"}
    }
    for candidate in manifest.get("candidates") or []:
        candidate_id = str(candidate["candidate_id"])
        if str(candidate.get("role")) not in {
            "DEFENSIVE",
            "DEFENSIVE_ACCOUNT",
            "PORTFOLIO_ONLY",
        }:
            continue
        preliminary[candidate_id]["account_utility"] = _defensive_account_utility(
            non_defensive,
            candidate_id,
            preliminary[candidate_id]["trade_ledger"],
        )

    policy = dict(manifest.get("q4_decision_policy") or {})
    results: list[dict[str, Any]] = []
    for candidate in manifest.get("candidates") or []:
        candidate_id = str(candidate["candidate_id"])
        metrics = preliminary[candidate_id]
        decision = classify_role_specific_q4_result(candidate, metrics, policy)
        results.append(
            {
                "candidate_id": candidate_id,
                "role": candidate["role"],
                "classification": decision["classification"],
                "reasons": decision["reasons"],
                "metrics": metrics,
                "parameters_mutated": False,
                "specification_hash": candidate["specification_hash"],
            }
        )
    return results


def load_q4_dbn_frames(
    sources: Sequence[Mapping[str, Any]],
    capability: AuthorizedQ4Capability,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Decode protected DBN only while holding the non-serializable capability."""

    capability.validate_scope()
    try:
        import databento as db
    except ImportError as exc:
        raise Q4MarketEvaluationError("Databento DBN reader is unavailable.") from exc
    pieces: dict[str, list[pd.DataFrame]] = {}
    provenance: list[dict[str, Any]] = []
    for source in sources:
        path = Path(str(source["path"]))
        expected = str(source.get("sha256") or "")
        if not path.is_file() or (expected and _sha256(path) != expected):
            raise Q4MarketEvaluationError(f"Q4 DBN source missing or changed: {path}")
        store = db.DBNStore.from_file(path)
        mappings = _normalize_mappings(getattr(store.metadata, "mappings", {}) or {})
        instrument_roots, roll_dates = _mapping_roots(mappings)
        frame = store.to_df(price_type="float", pretty_ts=True, map_symbols=False)
        if frame.index.name:
            frame = frame.reset_index()
        frame = frame.rename(columns={"ts_event": "timestamp"})
        required = {"timestamp", "instrument_id", "open", "high", "low", "close", "volume"}
        missing = required - set(frame.columns)
        if missing:
            raise Q4MarketEvaluationError(f"Q4 DBN columns missing: {sorted(missing)}")
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame[
            (frame["timestamp"] >= pd.Timestamp(Q4_START, tz="UTC"))
            & (frame["timestamp"] < pd.Timestamp(Q4_END, tz="UTC"))
        ].copy()
        frame["instrument_id"] = frame["instrument_id"].astype(str)
        frame["symbol"] = frame["instrument_id"].map(instrument_roots)
        if frame["symbol"].isna().any():
            unknown = sorted(frame.loc[frame["symbol"].isna(), "instrument_id"].unique())
            raise Q4MarketEvaluationError(
                f"Q4 instrument IDs lack continuous-root provenance: {unknown[:5]}"
            )
        frame["active_contract"] = frame["symbol"] + ":iid:" + frame["instrument_id"]
        frame = _exclude_roll_guard(frame, roll_dates)
        if frame.duplicated(["symbol", "timestamp"]).any():
            raise Q4MarketEvaluationError("Duplicate Q4 root/timestamp bars detected.")
        if not (
            (frame["high"].astype(float) >= frame[["open", "close", "low"]].max(axis=1)).all()
            and (frame["low"].astype(float) <= frame[["open", "close", "high"]].min(axis=1)).all()
        ):
            raise Q4MarketEvaluationError("Q4 OHLC invariants failed.")
        for root, group in frame.groupby("symbol", sort=True):
            pieces.setdefault(str(root), []).append(
                group[
                    [
                        "timestamp",
                        "symbol",
                        "active_contract",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                    ]
                ].copy()
            )
        provenance.append(
            {
                "path": str(path.resolve()),
                "sha256": _sha256(path),
                "rows_after_q4_and_roll_guards": int(len(frame)),
                "roots": sorted(frame["symbol"].astype(str).unique()),
                "explicit_instrument_ids": int(frame["instrument_id"].nunique()),
            }
        )
    frames = {
        root: pd.concat(groups, ignore_index=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
        for root, groups in sorted(pieces.items())
    }
    return frames, {
        "schema": "hydra_q4_dbn_provenance_v1",
        "sources": provenance,
        "roots": sorted(frames),
        "period": [Q4_START, Q4_END],
        "capability_token_id": capability.token_id,
    }


def _frame_to_matrix(frame: pd.DataFrame, market: str) -> FeatureMatrix:
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    if data.empty or data["timestamp"].min() < pd.Timestamp(Q4_START, tz="UTC"):
        raise Q4MarketEvaluationError(f"Invalid Q4 frame for {market}.")
    if data["timestamp"].max() >= pd.Timestamp(Q4_END, tz="UTC"):
        raise Q4MarketEvaluationError(f"Q4 frame crosses frozen end for {market}.")
    featured = _prepare_feature_frame(_build_past_only_feature_frame(data))
    arrays, _metadata = _market_arrays(featured, market)
    digest = hashlib.sha256()
    array_manifest = {}
    for name, values in sorted(arrays.items()):
        digest.update(name.encode())
        digest.update(str(values.dtype).encode())
        digest.update(np.ascontiguousarray(values).view(np.uint8))
        array_manifest[name] = {"shape": list(values.shape), "dtype": str(values.dtype)}
    fingerprint = digest.hexdigest()
    manifest = {
        "row_count": len(featured),
        "bundle_hash": fingerprint,
        "arrays": array_manifest,
        "market": market,
        "period": [Q4_START, Q4_END],
    }
    return FeatureMatrix(
        root=Path("<protected-q4-memory>"),
        manifest=manifest,
        arrays=arrays,
    )


def _q4_positions(specification: Mapping[str, Any], matrix: FeatureMatrix) -> np.ndarray:
    spec = spec_from_dict(dict(specification))
    feature = matrix.array(f"feature__{spec.feature}")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    session = matrix.array("session_code")
    mask = np.isfinite(feature) & np.isfinite(forward)
    mask &= _array_compare(feature, spec.operator, spec.threshold)
    mask &= session == spec.session_code if spec.session_code >= 0 else session >= 0
    if spec.context_feature is not None:
        context = matrix.array(f"feature__{spec.context_feature}")
        mask &= np.isfinite(context) & _array_compare(
            context,
            spec.context_operator,
            float(spec.context_threshold or 0.0),
        )
    return _non_overlapping(np.flatnonzero(mask), spec, matrix)


def _metrics_from_risk(risk: Mapping[str, Any]) -> dict[str, Any]:
    event_net = np.asarray(risk.get("event_net_pnl") or [], dtype=float)
    daily = {
        str(key): float(value)
        for key, value in dict(risk.get("daily_pnl") or {}).items()
    }
    positive_days = [value for value in daily.values() if value > 0.0]
    positive_total = sum(positive_days)
    best_day_fraction = (
        max(positive_days, default=0.0) / positive_total if positive_total > 0.0 else 1.0
    )
    topstep = dict(risk.get("topstep") or {})
    combine = dict(topstep.get("combine") or {})
    standard = dict(topstep.get("xfa_standard") or {})
    consistency = dict(topstep.get("xfa_consistency") or {})
    hard_failures: list[str] = []
    if not bool(risk.get("complete")):
        hard_failures.append("q4_full_risk_replay_incomplete")
    if bool(risk.get("mll_breached")):
        hard_failures.append("catastrophic_q4_mll_breach")
    if not bool(risk.get("session_flatten_proved")):
        hard_failures.append("q4_session_flatten_violation")
    if not bool(risk.get("contract_limit_proved")):
        hard_failures.append("q4_contract_limit_violation")
    return {
        "events": int(len(event_net)),
        "net_pnl": float(event_net.sum()),
        "event_net_pnl": event_net.tolist(),
        "daily_pnl": daily,
        "best_day_positive_pnl_fraction": float(best_day_fraction),
        "mll_breached": bool(risk.get("mll_breached")),
        "minimum_mll_buffer": float(risk.get("minimum_mll_buffer") or 0.0),
        "target_progress": float(combine.get("total_profit") or 0.0) / 9000.0,
        "qualifying_days": max(
            int(standard.get("winning_days_150_count") or 0),
            int(consistency.get("winning_days_150_count") or 0),
        ),
        "catastrophic_xfa_contradiction": bool(
            not standard.get("survived") and not consistency.get("survived")
        ),
        "topstep": topstep,
        "same_bar_ambiguities": int(risk.get("same_bar_ambiguities") or 0),
        "hard_failures": hard_failures,
    }


def _defensive_account_utility(
    base_ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    candidate_id: str,
    candidate_ledger: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not base_ledgers or not candidate_ledger:
        return {"control_count": 0, "reason": "q4_account_events_insufficient"}
    try:
        contribution = compare_account_contribution(
            base_ledgers,
            candidate_id,
            candidate_ledger,
            target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
        )
        controls = matched_random_inclusion_controls(
            base_ledgers,
            candidate_id,
            candidate_ledger,
            target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
            control_count=63,
            seed=int(hashlib.sha256(candidate_id.encode()).hexdigest()[:8], 16),
        )
        base_velocity = float(contribution.base.target_velocity_dollars_per_day)
        combined_velocity = float(contribution.combined.target_velocity_dollars_per_day)
        loss_fraction = (
            max((base_velocity - combined_velocity) / abs(base_velocity), 0.0)
            if base_velocity != 0.0
            else (0.0 if combined_velocity >= 0.0 else 1.0)
        )
        return {
            "control_count": int(controls.control_count),
            "matched_control_probability": float(controls.one_sided_p_value),
            "maximum_drawdown_reduction": float(
                contribution.maximum_drawdown_reduction
            ),
            "min_mll_buffer_delta": float(contribution.min_mll_buffer_delta),
            "shared_loss_days_reduction": int(contribution.shared_loss_days_reduction),
            "target_velocity_loss_fraction": float(loss_fraction),
            "hard_risk_violation": bool(contribution.hard_risk_violation),
            "base": contribution.base.to_dict(),
            "combined": contribution.combined.to_dict(),
        }
    except Exception as exc:
        return {
            "control_count": 0,
            "reason": f"{type(exc).__name__}:{exc}",
        }


def _normalize_mappings(value: Any) -> dict[str, list[dict[str, str]]]:
    if not isinstance(value, dict):
        raise Q4MarketEvaluationError("DBN symbology mappings are unavailable.")
    output: dict[str, list[dict[str, str]]] = {}
    for symbol, intervals in value.items():
        rows = []
        for interval in intervals or []:
            raw = interval if isinstance(interval, dict) else vars(interval)
            rows.append(
                {
                    "start_date": str(raw.get("start_date") or raw.get("d0") or ""),
                    "end_date": str(raw.get("end_date") or raw.get("d1") or ""),
                    "instrument_id": str(raw.get("symbol") or raw.get("s") or ""),
                }
            )
        output[str(symbol)] = rows
    return output


def _mapping_roots(
    mappings: Mapping[str, Sequence[Mapping[str, str]]]
) -> tuple[dict[str, str], dict[str, set[str]]]:
    instrument_roots: dict[str, str] = {}
    roll_dates: dict[str, set[str]] = {}
    for continuous_symbol, intervals in mappings.items():
        root = str(continuous_symbol).split(".", 1)[0].upper()
        ordered = sorted(intervals, key=lambda row: row["start_date"])
        for index, interval in enumerate(ordered):
            instrument_id = str(interval["instrument_id"])
            existing = instrument_roots.get(instrument_id)
            if existing is not None and existing != root:
                raise Q4MarketEvaluationError("Instrument ID maps to multiple roots.")
            instrument_roots[instrument_id] = root
            if index > 0:
                roll_dates.setdefault(root, set()).add(str(interval["start_date"]))
    if not instrument_roots:
        raise Q4MarketEvaluationError("No explicit Q4 instrument mapping was found.")
    return instrument_roots, roll_dates


def _exclude_roll_guard(
    frame: pd.DataFrame, roll_dates: Mapping[str, set[str]]
) -> pd.DataFrame:
    keep = pd.Series(True, index=frame.index)
    dates = frame["timestamp"].dt.normalize()
    for root, values in roll_dates.items():
        for value in values:
            roll = pd.Timestamp(value, tz="UTC")
            keep &= ~(
                frame["symbol"].eq(root)
                & (dates >= roll - pd.Timedelta(days=1))
                & (dates <= roll + pd.Timedelta(days=1))
            )
    return frame.loc[keep].copy().reset_index(drop=True)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_definition_store(
    receipt: Mapping[str, Any], capability: AuthorizedQ4Capability
) -> None:
    capability.validate_scope()
    path = Path(str(receipt.get("path") or ""))
    if not path.is_file() or _sha256(path) != str(receipt.get("sha256") or ""):
        raise Q4MarketEvaluationError("Q4 definition store is missing or changed.")
    try:
        import databento as db
    except ImportError as exc:
        raise Q4MarketEvaluationError("Databento definition reader unavailable.") from exc
    store = db.DBNStore.from_file(path)
    try:
        record_count = len(store)
    except TypeError:
        record_count = len(store.to_df(pretty_ts=True, map_symbols=False))
    if int(record_count) <= 0:
        raise Q4MarketEvaluationError("Q4 definition store is empty.")
