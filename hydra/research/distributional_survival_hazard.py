from __future__ import annotations

import hashlib
import json
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_volume_front import VOLUME_FRONT_MAP_TYPE
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    _stable_hash,
    _strict_json_value,
)
from hydra.research.cross_asset_daily_horizon_primary import (
    TARGETS,
    _load_tables,
    _source_features,
)
from hydra.research.equity_open_gap_reversal import _write_immutable
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "distributional_survival_hazard_v1"
FEATURE_COLUMNS = tuple(
    f"{source}_{feature}"
    for source in TARGETS
    for feature in (
        "prior_trend",
        "prior_range_shock_signed",
        "prior_close_location",
    )
)
FOLDS = (
    ("2024_q1", "2024-01-01", "2024-04-01"),
    ("2024_q2", "2024-04-01", "2024-07-01"),
    ("2024_q3", "2024-07-01", "2024-10-01"),
)


class DistributionalSurvivalHazardError(RuntimeError):
    pass


def build_hazard_dataset(
    tables: dict[str, pd.DataFrame], target_market: str
) -> pd.DataFrame:
    execution_market = str(TARGETS[target_market]["micro"])
    target = tables[execution_market].sort_values("session_id").copy()
    target = target.set_index("session_id", drop=False)
    feature_frames: list[pd.DataFrame] = []
    for source in TARGETS:
        features = _source_features(tables[source]).set_index("session_id", drop=False)
        renamed = features[
            [
                "source_prior_session_id",
                "source_prior_trend",
                "source_prior_range_shock_signed",
                "source_prior_close_location",
            ]
        ].rename(
            columns={
                "source_prior_session_id": f"{source}_prior_session_id",
                "source_prior_trend": f"{source}_prior_trend",
                "source_prior_range_shock_signed": (
                    f"{source}_prior_range_shock_signed"
                ),
                "source_prior_close_location": f"{source}_prior_close_location",
            }
        )
        feature_frames.append(renamed)
    aligned = target
    for frame in feature_frames:
        aligned = aligned.join(frame, how="left")
    point_value = instrument_spec(execution_market).point_value
    long_adverse = -pd.to_numeric(
        aligned["overnight_long_mae_120"], errors="coerce"
    ) * point_value
    short_adverse = -pd.to_numeric(
        aligned["overnight_short_mae_120"], errors="coerce"
    ) * point_value
    severity = pd.concat([long_adverse, short_adverse], axis=1).max(axis=1)
    threshold = severity.shift(1).rolling(40, min_periods=20).quantile(0.80)
    dataset = pd.DataFrame(
        {
            "session_id": aligned["session_id"],
            "decision_timestamp": aligned["overnight_entry_timestamp"],
            "target_market": target_market,
            "execution_market": execution_market,
            "tail_severity_dollars": severity,
            "past_only_tail_threshold": threshold,
            "tail_event": severity.ge(threshold).astype(float),
            **{
                column: pd.to_numeric(aligned[column], errors="coerce")
                for column in FEATURE_COLUMNS
            },
        }
    )
    required_sessions = [f"{source}_prior_session_id" for source in TARGETS]
    for source, frame in zip(TARGETS, feature_frames):
        dataset[f"{source}_prior_session_id"] = frame[
            f"{source}_prior_session_id"
        ]
    dataset = dataset.dropna(
        subset=[
            "decision_timestamp",
            "tail_severity_dollars",
            "past_only_tail_threshold",
            *required_sessions,
        ]
    ).copy()
    for column in required_sessions:
        if not (
            pd.to_datetime(dataset[column]) < pd.to_datetime(dataset["session_id"])
        ).all():
            raise DistributionalSurvivalHazardError(
                f"Feature source session is not prior: {column}"
            )
    dataset["tail_event"] = dataset["tail_event"].astype(int)
    return dataset.sort_values("decision_timestamp").reset_index(drop=True)


def run_distributional_survival_hazard(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    core_data_path: str | Path,
    core_data_sha256: str,
    core_map_path: str | Path,
    core_map_sha256: str,
    core_roll_map_hash: str,
    metals_data_path: str | Path,
    metals_data_sha256: str,
    metals_map_path: str | Path,
    metals_map_sha256: str,
    metals_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    frozen = (
        (Path(engineering_task_path), engineering_task_sha256, "engineering task"),
        (Path(core_data_path), core_data_sha256, "core data"),
        (Path(core_map_path), core_map_sha256, "core map"),
        (Path(metals_data_path), metals_data_sha256, "metals data"),
        (Path(metals_map_path), metals_map_sha256, "metals map"),
    )
    for path, expected, label in frozen:
        _verify(path, expected, label)
    if len(code_commit) == 40:
        current = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if current != code_commit:
            raise DistributionalSurvivalHazardError(
                "Worker commit differs from queued specification."
            )
    core_map, metals_map = load_roll_map(core_map_path), load_roll_map(metals_map_path)
    if core_map.roll_map_hash() != core_roll_map_hash:
        raise DistributionalSurvivalHazardError("Core roll map changed.")
    if (
        metals_map.map_type != VOLUME_FRONT_MAP_TYPE
        or metals_map.roll_map_hash() != metals_roll_map_hash
    ):
        raise DistributionalSurvivalHazardError("Metals roll map changed.")
    candidates = [_candidate_specification(target) for target in TARGETS]
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration: dict[str, Any] = {
        "schema": VERSION,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "feature_columns": list(FEATURE_COLUMNS),
        "target": {
            "horizon_completed_1m_bars": 120,
            "past_only_threshold_window": 40,
            "past_only_threshold_minimum": 20,
            "tail_quantile": 0.80,
        },
        "model": {
            "type": "l2_logistic",
            "C": 0.10,
            "class_weight": "balanced",
            "max_iter": 1000,
            "median_imputation_fit_on_train_only": True,
            "standardization_fit_on_train_only": True,
        },
        "folds": [list(row) for row in FOLDS],
        "one_session_purge": True,
        "q4_access_allowed": False,
        "network_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "code_commit": code_commit,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "survival_hazard_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once(candidates) if record_data_access else None
    tables, data_provenance = _load_tables(
        Path(core_data_path),
        core_map,
        Path(metals_data_path),
        metals_map,
        "2024-10-01",
    )
    controls = validator_controls()
    model_results: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []
    for candidate in candidates:
        dataset = build_hazard_dataset(tables, str(candidate["target_market"]))
        predictions, fold_models = rolling_origin_predictions(dataset)
        metrics = hazard_metrics(predictions, seed=991301 + len(model_results))
        metrics["coefficient_direction_stability"] = _fold_coefficient_stability(
            fold_models
        )
        robust = bool(
            controls["passed"]
            and metrics["roc_auc"] >= 0.60
            and metrics["brier_skill"] > 0
            and metrics["top_20_hazard_lift"] >= 1.35
            and metrics["supportive_quarters"] >= 2
            and metrics["random_control_probability"] <= 0.10
            and metrics["coefficient_direction_stability"] >= 0.50
        )
        if robust:
            status = "ROBUST_RESEARCH_CANDIDATE"
        elif metrics["roc_auc"] >= 0.58 and metrics["brier_skill"] > 0:
            status = "PROMISING_RESEARCH_CANDIDATE"
        elif metrics["roc_auc"] >= 0.55:
            status = "RAW_ECONOMIC_SIGNAL"
        else:
            status = "RESEARCH_PROTOTYPE"
        model_manifest: dict[str, Any] = {
            "schema": "distributional_survival_hazard_model_manifest_v1",
            "candidate_id": candidate["candidate_id"],
            "preregistration_hash": preregistration["preregistration_hash"],
            "feature_columns": list(FEATURE_COLUMNS),
            "fold_models": fold_models,
            "status": status,
            "direct_alpha_claim": False,
            "shadow_policy_constructed": False,
            "q4_access_allowed": False,
        }
        model_manifest["model_manifest_hash"] = _stable_hash(model_manifest)
        manifest_path = destination / "model_manifests" / (
            f"{candidate['candidate_id']}.json"
        )
        _write_immutable(
            manifest_path,
            json.dumps(model_manifest, indent=2, sort_keys=True) + "\n",
        )
        manifests.append(
            {
                "candidate_id": candidate["candidate_id"],
                "path": str(manifest_path),
                "model_manifest_hash": model_manifest["model_manifest_hash"],
            }
        )
        model_results.append(
            {
                **candidate,
                "status": status,
                "events": int(len(predictions)),
                "net_pnl": 0.0,
                "micro_net_pnl": 0.0,
                "hazard_metrics": metrics,
                "fold_results": metrics["fold_results"],
                "candidate_level_evidence": {
                    "role": "HAZARD_RISK_MODEL",
                    "calibration_controls_passed": controls["passed"],
                    "robust_gate_passed": robust,
                    "direct_alpha_evidence": False,
                    "policy_construction_required": True,
                },
                "topstep": {
                    "path_candidate": False,
                    "reason": "hazard_model_requires_policy_construction",
                },
            }
        )
    statuses = [row["status"] for row in model_results]
    promising = sum(
        status in {"PROMISING_RESEARCH_CANDIDATE", "ROBUST_RESEARCH_CANDIDATE"}
        for status in statuses
    )
    robust_count = statuses.count("ROBUST_RESEARCH_CANDIDATE")
    conclusion = (
        "SURVIVAL_HAZARD_ROBUST_MODELS_FOUND_POLICY_REQUIRED"
        if robust_count
        else "SURVIVAL_HAZARD_INSUFFICIENT_OR_DIAGNOSTIC_ONLY"
    )
    integrity = {
        "candidate_count_exact_four": len(candidates) == 4,
        "unique_fingerprints": len(
            {row["structural_fingerprint"] for row in candidates}
        )
        == 4,
        "validator_controls_passed": bool(controls["passed"]),
        "prior_session_features_only": True,
        "tail_threshold_shifted": True,
        "train_only_preprocessing": True,
        "q4_excluded": True,
        "no_direct_alpha_or_shadow_promotion": not any(
            status in {"SHADOW_RESEARCH_CANDIDATE", "PAPER_SHADOW_READY"}
            for status in statuses
        ),
        "no_network_or_paid_data": True,
        "no_outbound_order_capability": True,
    }
    if not all(integrity.values()):
        raise DistributionalSurvivalHazardError(f"Integrity failed: {integrity}")
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "These are calibrated hazard models, not alpha strategies. A separate exact "
            "trade-policy experiment must prove avoided loss before zero-order shadow."
        ),
        "code_commit": code_commit,
        "candidate_count": 4,
        "structural_prototypes": 4,
        "candidates": model_results,
        "candidate_tier_counts": dict(Counter(statuses)),
        "promising_candidates": int(promising),
        "robust_hazard_models": int(robust_count),
        "shadow_candidates": 0,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": 0,
        "validator_controls": controls,
        "model_manifests": manifests,
        "integrity_proof": integrity,
        "data_provenance": data_provenance,
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "performance": {"total_seconds": time.perf_counter() - started},
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": (
            "CONSTRUCT_DEFENSIVE_POLICY_FROM_HAZARD_MODEL"
            if robust_count
            else "PIVOT_HAZARD_TARGET_OR_MARKET_STATE_REPRESENTATION"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "survival_hazard_result.json"
    report_path = destination / "survival_hazard_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "preregistration_path": str(preregistration_path),
        },
        "report_path": str(report_path),
    }


def rolling_origin_predictions(
    dataset: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    outputs: list[pd.DataFrame] = []
    manifests: list[dict[str, Any]] = []
    timestamps = pd.to_datetime(dataset["decision_timestamp"], utc=True)
    for fold_name, start, end in FOLDS:
        train = dataset[timestamps.lt(start)].copy().iloc[:-1]
        validation = dataset[timestamps.ge(start) & timestamps.lt(end)].copy()
        if len(train) < 120 or validation.empty or train["tail_event"].nunique() < 2:
            continue
        pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        C=0.10,
                        class_weight="balanced",
                        max_iter=1000,
                        random_state=991211,
                    ),
                ),
            ]
        )
        pipeline.fit(train[list(FEATURE_COLUMNS)], train["tail_event"])
        probabilities = pipeline.predict_proba(validation[list(FEATURE_COLUMNS)])[:, 1]
        train_probabilities = pipeline.predict_proba(train[list(FEATURE_COLUMNS)])[:, 1]
        high_risk_threshold = float(np.quantile(train_probabilities, 0.80))
        fold_output = validation[
            [
                "session_id",
                "decision_timestamp",
                "tail_event",
                "tail_severity_dollars",
            ]
        ].copy()
        fold_output["probability"] = probabilities
        fold_output["training_prevalence"] = float(train["tail_event"].mean())
        fold_output["high_risk"] = probabilities >= high_risk_threshold
        fold_output["fold"] = fold_name
        outputs.append(fold_output)
        coefficients = pipeline.named_steps["model"].coef_[0]
        manifests.append(
            {
                "fold": fold_name,
                "train_end_exclusive": start,
                "validation_end_exclusive": end,
                "training_samples_after_purge": len(train),
                "validation_samples": len(validation),
                "training_prevalence": float(train["tail_event"].mean()),
                "high_risk_threshold": high_risk_threshold,
                "coefficients": {
                    feature: float(value)
                    for feature, value in zip(FEATURE_COLUMNS, coefficients)
                },
            }
        )
    if not outputs:
        raise DistributionalSurvivalHazardError("No valid rolling-origin folds.")
    return pd.concat(outputs, ignore_index=True), manifests


def hazard_metrics(predictions: pd.DataFrame, *, seed: int) -> dict[str, Any]:
    y = predictions["tail_event"].to_numpy(dtype=int)
    probabilities = predictions["probability"].to_numpy(dtype=float)
    auc = float(roc_auc_score(y, probabilities)) if len(set(y)) > 1 else 0.5
    brier = float(brier_score_loss(y, probabilities))
    baseline_probability = predictions["training_prevalence"].to_numpy(dtype=float)
    baseline_brier = float(np.mean((y - baseline_probability) ** 2))
    brier_skill = 1.0 - brier / max(baseline_brier, 1e-12)
    high = predictions["high_risk"].to_numpy(dtype=bool)
    base_rate = float(y.mean())
    high_rate = float(y[high].mean()) if high.any() else 0.0
    lift = high_rate / max(base_rate, 1e-12)
    capture = float(y[high].sum() / max(y.sum(), 1))
    severity = predictions["tail_severity_dollars"].to_numpy(dtype=float)
    captured_severity = float(severity[high & (y == 1)].sum())
    rng = np.random.default_rng(seed)
    selected_count = int(high.sum())
    random_captured = np.zeros(4096, dtype=float)
    if selected_count:
        for index in range(len(random_captured)):
            selected = rng.choice(len(y), size=selected_count, replace=False)
            random_captured[index] = severity[selected][y[selected] == 1].sum()
    random_probability = float(
        (1 + np.count_nonzero(random_captured >= captured_severity))
        / (len(random_captured) + 1)
    )
    fold_results: dict[str, Any] = {}
    supportive = 0
    for fold_name, fold in predictions.groupby("fold", sort=True):
        fold_y = fold["tail_event"].to_numpy(dtype=int)
        fold_p = fold["probability"].to_numpy(dtype=float)
        fold_auc = (
            float(roc_auc_score(fold_y, fold_p)) if len(set(fold_y)) > 1 else 0.5
        )
        fold_high = fold["high_risk"].to_numpy(dtype=bool)
        fold_base = float(fold_y.mean())
        fold_lift = (
            float(fold_y[fold_high].mean()) / max(fold_base, 1e-12)
            if fold_high.any()
            else 0.0
        )
        support = fold_auc >= 0.55 and fold_lift >= 1.10
        supportive += int(support)
        fold_results[str(fold_name)] = {
            "samples": len(fold),
            "events": int(fold_y.sum()),
            "roc_auc": fold_auc,
            "top_20_hazard_lift": fold_lift,
            "supportive": support,
        }
    return {
        "samples": len(predictions),
        "tail_events": int(y.sum()),
        "tail_prevalence": base_rate,
        "roc_auc": auc,
        "brier_score": brier,
        "baseline_brier_score": baseline_brier,
        "brier_skill": float(brier_skill),
        "log_loss": float(log_loss(y, probabilities, labels=[0, 1])),
        "expected_calibration_error": _calibration_error(y, probabilities),
        "top_20_hazard_lift": float(lift),
        "top_20_capture_rate": capture,
        "captured_tail_severity_dollars": captured_severity,
        "random_control_probability": random_probability,
        "supportive_quarters": supportive,
        "fold_results": fold_results,
        "coefficient_direction_stability": 0.0,
    }


def validator_controls() -> dict[str, Any]:
    rng = np.random.default_rng(991209)
    x = rng.normal(size=(1200, 4))
    negative_y = rng.integers(0, 2, size=1200)
    signal = 1.2 * x[:, 0] - 0.8 * x[:, 1] + rng.normal(scale=0.8, size=1200)
    positive_y = (signal > 0).astype(int)
    negative_auc = _control_auc(x, negative_y)
    positive_auc = _control_auc(x, positive_y)
    return {
        "negative_control_auc": negative_auc,
        "injected_weak_real_auc": positive_auc,
        "negative_control_passed": 0.40 <= negative_auc <= 0.60,
        "injected_control_passed": positive_auc >= 0.75,
        "passed": bool(
            0.40 <= negative_auc <= 0.60 and positive_auc >= 0.75
        ),
    }


def _control_auc(x: np.ndarray, y: np.ndarray) -> float:
    model = LogisticRegression(C=0.10, max_iter=1000, random_state=991209)
    model.fit(x[:800], y[:800])
    probability = model.predict_proba(x[800:])[:, 1]
    return float(roc_auc_score(y[800:], probability))


def _calibration_error(y: np.ndarray, probabilities: np.ndarray) -> float:
    bins = np.linspace(0.0, 1.0, 6)
    error = 0.0
    for lower, upper in zip(bins[:-1], bins[1:]):
        mask = (probabilities >= lower) & (
            probabilities <= upper if upper == 1.0 else probabilities < upper
        )
        if mask.any():
            error += float(mask.mean()) * abs(
                float(y[mask].mean()) - float(probabilities[mask].mean())
            )
    return float(error)


def _fold_coefficient_stability(fold_models: list[dict[str, Any]]) -> float:
    if len(fold_models) < 2:
        return 0.0
    signs = [
        np.sign([float(row["coefficients"][feature]) for feature in FEATURE_COLUMNS])
        for row in fold_models
    ]
    agreements = [
        float(np.mean(left == right))
        for left, right in zip(signs[:-1], signs[1:])
    ]
    return float(np.mean(agreements))


def _candidate_specification(target: str) -> dict[str, Any]:
    specification = {
        "representation": VERSION,
        "candidate_id": f"hazard_tail_120_{target}_l2_logistic_v1",
        "target_market": target,
        "execution_market": TARGETS[target]["micro"],
        "market_ecology": TARGETS[target]["ecology"],
        "mechanism_family": "distributional_tail_survival_hazard",
        "portfolio_role": "defensive_risk_state",
        "source_timeframe": "completed_prior_RTH_session",
        "target_horizon": "120_completed_1m_bars",
    }
    fingerprint = structural_fingerprint(specification)
    return {
        **specification,
        "structural_fingerprint": fingerprint,
        "lineage_id": f"lineage_hazard_{fingerprint[:20]}",
    }


def _record_access_once(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "rolling-origin distributional tail-hazard research; Q4 excluded"
    module = "hydra.research.distributional_survival_hazard"
    candidate_ids = [row["candidate_id"] for row in candidates]
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == module
                and row.get("candidate_ids") == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        module,
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise DistributionalSurvivalHazardError(
            f"Frozen {label} missing or changed: {path}"
        )


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Distributional Survival-Hazard Search",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Models: `{payload['candidate_count']}`",
        f"- Robust hazard models: `{payload['robust_hazard_models']}`",
        f"- Validator controls: `{payload['validator_controls']['passed']}`",
        "- Direct alpha claims: `0`",
        "- Shadow candidates: `0`",
        "- PAPER_SHADOW_READY: `0`",
        "- Q4 access delta: `0`",
        "- Outbound orders: `0`",
        "",
    ]
    for candidate in payload["candidates"]:
        metrics = candidate["hazard_metrics"]
        lines.extend(
            [
                f"## {candidate['candidate_id']}",
                "",
                f"- Status: `{candidate['status']}`",
                f"- AUC: `{metrics['roc_auc']}`",
                f"- Brier skill: `{metrics['brier_skill']}`",
                f"- Top-bin lift: `{metrics['top_20_hazard_lift']}`",
                f"- Supportive quarters: `{metrics['supportive_quarters']}`",
                f"- Random-control p: `{metrics['random_control_probability']}`",
                "",
            ]
        )
    return "\n".join(lines)
