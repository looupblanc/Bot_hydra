from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.preprocessing import StandardScaler

from hydra.data.contract_mapping import load_roll_map
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _file_sha256,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
)
from hydra.research.equity_open_gap_reversal import (
    FOLDS,
    MAP_TYPE,
    MARKET_PAIRS,
    SOURCE_PREREGISTRATION_SHA256,
    SYMBOLS,
    _evaluate_candidate,
    _integrity_proof,
    _round_turn_cost,
    _write_immutable,
    build_event_table,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "opening_direction_hazard_pilot_v1"
FEATURES = (
    "gap_scaled_q75",
    "opening_body_aligned",
    "opening_range_scaled",
    "opening_volume_ratio",
    "past_continuation_rate",
    "gap_sign",
    "weekday_sin",
    "weekday_cos",
)
MINI_SYMBOLS = tuple(MARKET_PAIRS)


class OpeningDirectionHazardError(RuntimeError):
    pass


def run_opening_direction_hazard_pilot(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 771331,
) -> dict[str, Any]:
    task_path, map_path = Path(engineering_task_path), Path(repaired_map_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(map_path, repaired_map_sha256, "repaired contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise OpeningDirectionHazardError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise OpeningDirectionHazardError("Worker commit differs from queued specification.")
    source_preregistration = project_path(
        "reports",
        "mission_experiments",
        "calibration_affected_atom_retest_v3_design_v1",
        "calibration_affected_atom_retest_v3_preregistration.json",
    )
    if not source_preregistration.is_file():
        source_preregistration = Path(
            "/root/hydra-bot/reports/mission_experiments/"
            "calibration_affected_atom_retest_v3_design_v1/"
            "calibration_affected_atom_retest_v3_preregistration.json"
        )
    _verify(source_preregistration, SOURCE_PREREGISTRATION_SHA256, "development manifest")
    source = json.loads(source_preregistration.read_text(encoding="utf-8"))
    _verify_development_manifest((source.get("source") or {}).get("development_data_manifest") or {})

    preregistration = _preregistration(
        engineering_task_sha256=engineering_task_sha256,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        code_commit=code_commit,
        random_seed=random_seed,
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration_path = destination / "opening_direction_hazard_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    candidate_ids = [f"strategy_opening_direction_hazard_{symbol}_v1" for symbol in MINI_SYMBOLS]
    access = _record_access_once(candidate_ids) if record_data_access else None
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    events = _prepare_features(build_event_table(raw))
    events, rolling_models, permutation_models = _rolling_predictions(
        events, random_seed=random_seed
    )
    final_model = _final_model(events, random_seed=random_seed)
    final_model["leave_one_market_out"] = _leave_one_market_out(
        events, final_model, random_seed=random_seed
    )
    final_model["model_hash"] = _stable_hash(final_model)
    model_path = destination / "opening_direction_hazard_final_model.json"
    _write_immutable(model_path, json.dumps(final_model, indent=2, sort_keys=True) + "\n")
    events = _apply_primary_policy(events)
    integrity = _integrity_proof(events)
    integrity.update(_model_integrity(events, rolling_models))
    if not all(integrity.values()):
        raise OpeningDirectionHazardError(f"Hazard integrity proof failed: {integrity}")

    candidates: list[dict[str, Any]] = []
    for index, (mini, micro) in enumerate(MARKET_PAIRS.items()):
        diagnostics = _policy_diagnostics(events[events["symbol"].eq(mini)].copy())
        candidate = _evaluate_candidate(
            events,
            mini,
            micro,
            preregistration_hash=str(preregistration["preregistration_hash"]),
            random_seed=random_seed + index * 1009,
            candidate_prefix="strategy_opening_direction_hazard",
            mechanism_family="opening_direction_distributional_hazard",
            direction="calibrated_bidirectional_probability",
            parameter_diagnostics_override=diagnostics,
        )
        permutation_net = float(
            events[
                events["symbol"].eq(mini) & events["permutation_primary_event"]
            ]["permutation_net_pnl"].sum()
        )
        candidate["attacks"]["label_permutation_net"] = permutation_net
        candidate["model_evidence"] = _candidate_model_evidence(
            events, mini, rolling_models
        )
        candidate["shadow_evidence"]["shadow_spec_complete"] = True
        candidates.append(candidate)

    for candidate in candidates:
        adjusted = min(
            float(candidate["null_evidence"]["raw_probability"]) * len(candidates), 1.0
        )
        candidate["null_evidence"]["family_adjusted_probability"] = adjusted
        candidate["shadow_evidence"]["null_probability"] = adjusted
        candidate["shadow_evidence"]["candidate_null_pass"] = bool(
            adjusted <= 0.20
            and candidate["attacks"]["sign_flip_net"] < 0.0
            and candidate["attacks"]["label_permutation_net"] <= 0.0
        )
        admission = decide_shadow_admission(ShadowEvidence(**candidate["shadow_evidence"]))
        candidate["admission"] = admission.to_dict()
        candidate["status"] = admission.tier.value

    shadow_directory = destination / "shadow_configurations"
    shadow_configs: list[dict[str, Any]] = []
    for candidate in candidates:
        if not candidate["admission"]["permits_zero_risk_shadow"]:
            continue
        specification = _shadow_specification(
            candidate,
            preregistration_hash=str(preregistration["preregistration_hash"]),
            model_path=model_path,
            model_hash=str(final_model["model_hash"]),
        )
        path = specification.write_immutable(
            shadow_directory / f"{candidate['candidate_id']}.json"
        )
        shadow_configs.append(
            {
                "candidate_id": candidate["candidate_id"],
                "status": candidate["status"],
                "path": str(path),
                "configuration_hash": specification.configuration_hash,
                "outbound_orders_enabled": False,
            }
        )
    ledger_path = destination / "opening_direction_hazard_trade_ledger.jsonl"
    _write_trade_ledger(ledger_path, events)
    statuses = [row["status"] for row in candidates]
    promising_tiers = {
        EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
        EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
        EvidenceTier.PAPER_SHADOW_READY.value,
    }
    shadow_tiers = {
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
        EvidenceTier.PAPER_SHADOW_READY.value,
    }
    promising = sum(status in promising_tiers for status in statuses)
    shadow = sum(status in shadow_tiers for status in statuses)
    paper = statuses.count(EvidenceTier.PAPER_SHADOW_READY.value)
    if paper:
        raise OpeningDirectionHazardError("Pre-Q4 hazard model attempted paper promotion.")
    freeze_eligible = [
        row["candidate_id"]
        for row in candidates
        if row["status"] == EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value
        and not bool(row["attacks"]["event_dominated"])
    ]
    if freeze_eligible:
        conclusion = "OPENING_DIRECTION_HAZARD_Q4_FREEZE_CANDIDATES_FOUND"
        next_action = "FREEZE_DISTINCT_HAZARD_CANDIDATE_OR_FORWARD_SHADOW"
    elif shadow:
        conclusion = "OPENING_DIRECTION_HAZARD_SHADOW_RESEARCH_ONLY"
        next_action = "HAZARD_FAILURE_SURFACE_BEFORE_HOLDOUT"
    elif promising:
        conclusion = "OPENING_DIRECTION_HAZARD_PROMISING_BUT_INSUFFICIENT"
        next_action = "HAZARD_CALIBRATION_OR_FEATURE_ABLATION"
    else:
        conclusion = "OPENING_DIRECTION_HAZARD_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_CROSS_ECOLOGY_INVARIANT_SEARCH"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Rolling-origin distributional evidence only. No in-sample score promotes, Q4 remains "
            "sealed, PAPER_SHADOW_READY is impossible pre-holdout, and no broker/order path exists."
        ),
        "code_commit": code_commit,
        "preregistration_hash": preregistration["preregistration_hash"],
        "preregistration_path": str(preregistration_path),
        "data_provenance": provenance,
        "data_access_record": access,
        "integrity_proof": integrity,
        "rolling_models": rolling_models,
        "permutation_models": permutation_models,
        "final_model": final_model,
        "final_model_path": str(model_path),
        "candidate_count": len(candidates),
        "candidate_tier_counts": dict(pd.Series(statuses).value_counts()),
        "candidates": candidates,
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow),
        "paper_shadow_ready": int(paper),
        "q4_freeze_eligible_candidate_ids": freeze_eligible,
        "topstep_path_candidates": int(
            sum(bool(row["topstep"]["path_candidate"]) for row in candidates)
        ),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "mechanism_families": ["opening_direction_distributional_hazard"],
        "market_ecologies": ["equity_indices"],
        "timeframe_profiles": ["session_reference_1m_execution"],
        "shadow_configurations": shadow_configs,
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": next_action,
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "opening_direction_hazard_result.json"
    report_path = destination / "opening_direction_hazard_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "trade_ledger_path": str(ledger_path),
            "model_path": str(model_path),
            "shadow_configuration_directory": str(shadow_directory),
        },
        "report_path": str(report_path),
    }


def _prepare_features(events: pd.DataFrame) -> pd.DataFrame:
    output = events.copy().sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    signed_move = np.sign(output["gap_points"]) * (
        output["exit_price_60"] - output["entry_price"]
    )
    output["continuation_target"] = (signed_move > 0).astype(float).where(
        output["exit_price_60"].notna() & output["gap_points"].ne(0)
    )
    scale = output["absolute_gap_points"].replace(0, np.nan)
    output["gap_scaled_q75"] = output["absolute_gap_points"] / output[
        "threshold_q75"
    ].replace(0, np.nan)
    output["opening_body_aligned"] = (
        np.sign(output["gap_points"])
        * (output["entry_price"] - output["rth_open_price"])
        / scale
    )
    output["opening_range_scaled"] = (output["high"] - output["low"]) / scale
    past_volume_median = output.groupby("symbol", sort=False)["volume"].transform(
        lambda values: values.shift(1).expanding(min_periods=40).median()
    )
    output["opening_volume_ratio"] = output["volume"] / past_volume_median.replace(0, np.nan)
    output["past_continuation_rate"] = output.groupby("symbol", sort=False)[
        "continuation_target"
    ].transform(lambda values: values.shift(1).expanding(min_periods=40).mean())
    output["gap_sign"] = np.sign(output["gap_points"])
    weekday = pd.to_datetime(output["event_session_id"]).dt.dayofweek.astype(float)
    output["weekday_sin"] = np.sin(2 * np.pi * weekday / 5.0)
    output["weekday_cos"] = np.cos(2 * np.pi * weekday / 5.0)
    output[list(FEATURES)] = output[list(FEATURES)].replace([np.inf, -np.inf], np.nan)
    return output


def _rolling_predictions(
    events: pd.DataFrame, *, random_seed: int
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    output = events.copy()
    output["continuation_probability"] = np.nan
    output["permutation_probability"] = np.nan
    output["model_fold"] = None
    model_rows: list[dict[str, Any]] = []
    permutation_rows: list[dict[str, Any]] = []
    for fold_index, (fold, (start, end)) in enumerate(FOLDS.items()):
        train = output[
            output["symbol"].isin(MINI_SYMBOLS)
            & output["event_session_id"].astype(str).lt(start)
        ].dropna(subset=[*FEATURES, "continuation_target"])
        test_index = output[
            output["event_session_id"].astype(str).ge(start)
            & output["event_session_id"].astype(str).lt(end)
        ].dropna(subset=list(FEATURES)).index
        if len(train) < 120 or train["continuation_target"].nunique() < 2 or len(test_index) == 0:
            raise OpeningDirectionHazardError(
                f"Rolling fold {fold} lacks train/test power: train={len(train)} test={len(test_index)}"
            )
        scaler, model = _fit_model(train, random_seed=random_seed + fold_index)
        probabilities = model.predict_proba(
            scaler.transform(output.loc[test_index, list(FEATURES)])
        )[:, 1]
        output.loc[test_index, "continuation_probability"] = probabilities
        output.loc[test_index, "model_fold"] = fold
        mini_test = output.loc[test_index]
        mini_test = mini_test[mini_test["symbol"].isin(MINI_SYMBOLS)].dropna(
            subset=["continuation_target"]
        )
        mini_prob = output.loc[mini_test.index, "continuation_probability"].to_numpy(dtype=float)
        model_rows.append(
            _model_record(fold, train, mini_test, scaler, model, mini_prob)
        )
        permuted_train = train.copy()
        rng = np.random.default_rng(random_seed + 10000 + fold_index)
        permuted_train["continuation_target"] = rng.permutation(
            permuted_train["continuation_target"].to_numpy()
        )
        perm_scaler, perm_model = _fit_model(
            permuted_train, random_seed=random_seed + 10000 + fold_index
        )
        perm_probability = perm_model.predict_proba(
            perm_scaler.transform(output.loc[test_index, list(FEATURES)])
        )[:, 1]
        output.loc[test_index, "permutation_probability"] = perm_probability
        permutation_rows.append(
            {
                "fold": fold,
                "train_events": int(len(permuted_train)),
                "mean_probability": float(np.mean(perm_probability)),
                "coefficient_l1": float(np.abs(perm_model.coef_[0]).sum()),
            }
        )
    return output, model_rows, permutation_rows


def _fit_model(
    train: pd.DataFrame, *, random_seed: int
) -> tuple[StandardScaler, LogisticRegression]:
    scaler = StandardScaler().fit(train.loc[:, list(FEATURES)])
    model = LogisticRegression(
        C=0.10,
        class_weight="balanced",
        solver="lbfgs",
        max_iter=1000,
        random_state=random_seed,
    ).fit(
        scaler.transform(train.loc[:, list(FEATURES)]),
        train["continuation_target"].astype(int),
    )
    return scaler, model


def _model_record(
    fold: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    scaler: StandardScaler,
    model: LogisticRegression,
    probabilities: np.ndarray,
) -> dict[str, Any]:
    target = test["continuation_target"].astype(int).to_numpy()
    clipped = np.clip(probabilities, 1e-6, 1 - 1e-6)
    return {
        "fold": fold,
        "train_events": int(len(train)),
        "test_events": int(len(test)),
        "train_end_exclusive": str(test["event_session_id"].min()),
        "brier_score": float(brier_score_loss(target, probabilities)),
        "log_loss": float(log_loss(target, clipped, labels=[0, 1])),
        "base_rate": float(target.mean()),
        "mean_probability": float(probabilities.mean()),
        "coefficients": dict(zip(FEATURES, model.coef_[0].astype(float), strict=True)),
        "intercept": float(model.intercept_[0]),
        "scaler_mean": dict(zip(FEATURES, scaler.mean_.astype(float), strict=True)),
        "scaler_scale": dict(zip(FEATURES, scaler.scale_.astype(float), strict=True)),
    }


def _final_model(events: pd.DataFrame, *, random_seed: int) -> dict[str, Any]:
    train = events[
        events["symbol"].isin(MINI_SYMBOLS)
        & events["event_session_id"].astype(str).lt("2024-10-01")
    ].dropna(subset=[*FEATURES, "continuation_target"])
    scaler, model = _fit_model(train, random_seed=random_seed + 50000)
    return {
        "schema": "opening_direction_hazard_model_v1",
        "features": list(FEATURES),
        "model": "standardized_l2_logistic_regression",
        "C": 0.10,
        "class_weight": "balanced",
        "train_events": int(len(train)),
        "train_end_exclusive": "2024-10-01",
        "coefficients": dict(zip(FEATURES, model.coef_[0].astype(float), strict=True)),
        "intercept": float(model.intercept_[0]),
        "scaler_mean": dict(zip(FEATURES, scaler.mean_.astype(float), strict=True)),
        "scaler_scale": dict(zip(FEATURES, scaler.scale_.astype(float), strict=True)),
        "continuation_threshold": 0.62,
        "reversal_threshold": 0.38,
    }


def _leave_one_market_out(
    events: pd.DataFrame, final_model: dict[str, Any], *, random_seed: int
) -> dict[str, Any]:
    full_sign = np.sign([float(final_model["coefficients"][feature]) for feature in FEATURES])
    rows: dict[str, Any] = {}
    for index, omitted in enumerate(MINI_SYMBOLS):
        train = events[
            events["symbol"].isin([item for item in MINI_SYMBOLS if item != omitted])
            & events["event_session_id"].astype(str).lt("2024-10-01")
        ].dropna(subset=[*FEATURES, "continuation_target"])
        scaler, model = _fit_model(train, random_seed=random_seed + 60000 + index)
        signs = np.sign(model.coef_[0])
        rows[omitted] = {
            "train_events": int(len(train)),
            "coefficient_sign_agreement": float((signs == full_sign).mean()),
            "coefficients": dict(zip(FEATURES, model.coef_[0].astype(float), strict=True)),
        }
    return rows


def _apply_primary_policy(events: pd.DataFrame) -> pd.DataFrame:
    output = events.copy()
    gap_direction = np.sign(output["gap_points"]).astype(int)
    probability = output["continuation_probability"]
    side = np.where(
        probability >= 0.62,
        gap_direction,
        np.where(probability <= 0.38, -gap_direction, 0),
    ).astype(int)
    output["side"] = side
    base = (
        (output["past_gap_count"] >= 40)
        & (output["absolute_gap_points"] >= output["threshold_q75"])
        & output["exit_price_60"].notna()
    )
    output["primary_event"] = base & output["side"].ne(0)
    for horizon in (30, 60, 90):
        output[f"gross_pnl_{horizon}"] = (
            output["side"]
            * (output[f"exit_price_{horizon}"] - output["entry_price"])
            * output["point_value"]
        )
        output[f"net_pnl_{horizon}"] = output[f"gross_pnl_{horizon}"] - output["cost"]
    output["delayed_gross_pnl"] = (
        output["side"]
        * (output["delayed_exit_price"] - output["delayed_entry_price"])
        * output["point_value"]
    )
    output["delayed_net_pnl"] = output["delayed_gross_pnl"] - output["cost"]
    long_mae = (output["future_low_60"] - output["entry_price"]) * output["point_value"]
    short_mae = (output["entry_price"] - output["future_high_60"]) * output["point_value"]
    output["mae_dollars"] = np.where(output["side"] > 0, long_mae, short_mae) - output[
        "cost"
    ] / 2
    perm = output["permutation_probability"]
    perm_side = np.where(
        perm >= 0.62, gap_direction, np.where(perm <= 0.38, -gap_direction, 0)
    ).astype(int)
    output["permutation_primary_event"] = base & pd.Series(perm_side, index=output.index).ne(0)
    output["permutation_net_pnl"] = (
        perm_side
        * (output["exit_price_60"] - output["entry_price"])
        * output["point_value"]
        - output["cost"]
    )
    return output


def _policy_diagnostics(events: pd.DataFrame) -> dict[str, Any]:
    results: dict[str, dict[str, Any]] = {}
    gap_direction = np.sign(events["gap_points"]).astype(int)
    base = (
        (events["past_gap_count"] >= 40)
        & (events["absolute_gap_points"] >= events["threshold_q75"])
    )
    for label, upper in (("confidence_058", 0.58), ("confidence_066", 0.66)):
        probability = events["continuation_probability"]
        side = np.where(
            probability >= upper,
            gap_direction,
            np.where(probability <= 1.0 - upper, -gap_direction, 0),
        )
        selected = base & pd.Series(side, index=events.index).ne(0) & events[
            "exit_price_60"
        ].notna()
        net = (
            side
            * (events["exit_price_60"] - events["entry_price"])
            * events["point_value"]
            - events["cost"]
        )
        results[label] = {"events": int(selected.sum()), "net_pnl": float(net[selected].sum())}
    for horizon in (30, 90):
        selected = events["primary_event"] & events[f"exit_price_{horizon}"].notna()
        results[f"primary_confidence_h{horizon}"] = {
            "events": int(selected.sum()),
            "net_pnl": float(events.loc[selected, f"net_pnl_{horizon}"].sum()),
        }
    return {
        "variants": results,
        "positive_neighbor_count": int(sum(row["net_pnl"] > 0 for row in results.values())),
        "diagnostic_only": True,
    }


def _candidate_model_evidence(
    events: pd.DataFrame, symbol: str, models: list[dict[str, Any]]
) -> dict[str, Any]:
    selected = events[
        events["symbol"].eq(symbol) & events["continuation_probability"].notna()
    ].dropna(subset=["continuation_target"])
    probabilities = selected["continuation_probability"].to_numpy(dtype=float)
    target = selected["continuation_target"].astype(int).to_numpy()
    return {
        "rolling_test_events": int(len(selected)),
        "brier_score": float(brier_score_loss(target, probabilities)) if len(selected) else 1.0,
        "base_rate": float(target.mean()) if len(target) else 0.0,
        "mean_probability": float(probabilities.mean()) if len(probabilities) else 0.0,
        "folds": [row["fold"] for row in models],
        "in_sample_promotion_allowed": False,
    }


def _model_integrity(
    events: pd.DataFrame, models: list[dict[str, Any]]
) -> dict[str, bool]:
    predicted = events[events["continuation_probability"].notna()]
    return {
        "rolling_models_all_four_folds": len(models) == 4,
        "model_predictions_only_in_frozen_folds": set(predicted["model_fold"].dropna())
        == set(FOLDS),
        "feature_availability_complete": bool(
            predicted.loc[:, list(FEATURES)].notna().all(axis=None)
        ),
        "probabilities_bounded": bool(
            predicted["continuation_probability"].between(0.0, 1.0).all()
        ),
        "no_q4_predictions": bool(
            predicted["event_session_id"].astype(str).lt("2024-10-01").all()
        ),
        "minimum_training_power": all(int(row["train_events"]) >= 120 for row in models),
    }


def _shadow_specification(
    candidate: dict[str, Any], *, preregistration_hash: str, model_path: Path, model_hash: str
) -> ShadowSpecification:
    micro = str(candidate["execution_market"])
    return ShadowSpecification(
        strategy_id=str(candidate["candidate_id"]),
        strategy_version="v1_pre_q4_shadow_research",
        feature_versions=("opening_direction_hazard_features_v1", f"model_{model_hash}"),
        markets=(micro,),
        timeframes=("1m", "session"),
        session_rules={
            "timezone": "America/Chicago",
            "decision_after_source_bar_close": "08:31",
            "mandatory_flatten_before": "15:10",
        },
        entry_rules={
            "model": "standardized_l2_logistic_regression",
            "model_path": str(model_path),
            "model_hash": model_hash,
            "continuation_probability_gte": 0.62,
            "reversal_probability_lte": 0.38,
            "absolute_gap_past_only_q75": True,
            "minimum_prior_sessions": 40,
            "abstain_between_thresholds": True,
        },
        exit_rules={"holding_completed_1m_bars": 60, "no_overnight": True},
        sizing={"contracts": 1, "instrument": micro, "micro_first": True},
        costs={"round_turn_usd": _round_turn_cost(micro), "slippage_ticks_round_turn": 2},
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=23 * 60 * 60,
        maximum_exposure=0.1,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=(
            "stale_data",
            "missing_model_feature",
            "model_hash_mismatch",
            "duplicate_signal",
            "session_closed",
            "clock_invalid",
            "contract_map_mismatch",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={
            "jsonl": True,
            "features": True,
            "probabilities": True,
            "signals": True,
            "virtual_fills": True,
            "rejections": True,
        },
        reconciliation={
            "startup": "fail_closed",
            "expected_vs_observed_fill": True,
            "position_source": "virtual_only",
        },
        source_manifest_hash=_stable_hash(
            {"preregistration_hash": preregistration_hash, "model_hash": model_hash}
        ),
        outbound_orders_enabled=False,
    )


def _preregistration(
    *,
    engineering_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "opening_direction_hazard_preregistration_v1",
        "candidate_ids": [f"strategy_opening_direction_hazard_{symbol}_v1" for symbol in MINI_SYMBOLS],
        "market_pairs": MARKET_PAIRS,
        "features": list(FEATURES),
        "model": {
            "type": "standardized_l2_logistic_regression",
            "C": 0.10,
            "class_weight": "balanced",
            "minimum_training_events": 120,
        },
        "target": "signed_gap_continuation_at_60_completed_minutes",
        "continuation_threshold": 0.62,
        "reversal_threshold": 0.38,
        "primary_gap_quantile": 0.75,
        "minimum_prior_sessions": 40,
        "folds": FOLDS,
        "costs": {symbol: _round_turn_cost(symbol) for symbol in SYMBOLS},
        "task_sha256": engineering_task_sha256,
        "map_sha256": repaired_map_sha256,
        "roll_map_hash": repaired_roll_map_hash,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "data_end_exclusive": "2024-10-01",
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "paper_shadow_ready_requires_untouched_holdout": True,
    }
    payload["preregistration_hash"] = _stable_hash(payload)
    return payload


def _record_access_once(candidate_ids: list[str]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "rolling-origin opening direction hazard strategy pilot; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == "hydra.research.opening_direction_hazard"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.opening_direction_hazard",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _write_trade_ledger(path: Path, events: pd.DataFrame) -> None:
    selected = events[events["primary_event"]].copy()
    columns = [
        "symbol",
        "active_contract",
        "event_session_id",
        "timestamp",
        "decision_timestamp",
        "model_fold",
        "continuation_probability",
        *FEATURES,
        "side",
        "entry_price",
        "exit_price_60",
        "gross_pnl_60",
        "cost",
        "net_pnl_60",
        "mae_dollars",
    ]
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in selected[columns].to_dict(orient="records")
    ]
    _write_immutable(path, "\n".join(lines) + ("\n" if lines else ""))


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Opening Direction Hazard Pilot",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Candidate tiers: `{payload['candidate_tier_counts']}`",
        f"- Shadow candidates: `{payload['shadow_candidates']}`",
        f"- Q4 freeze eligible: `{payload['q4_freeze_eligible_candidate_ids']}`",
        "- Q4 access: `0`",
        "- Outbound orders: `0`",
        "",
    ]
    for row in payload["candidates"]:
        lines.extend(
            [
                f"## {row['candidate_id']}",
                "",
                f"- Status: `{row['status']}`",
                f"- Events / net: `{row['events']}` / `{row['net_pnl']:.2f}`",
                f"- Supportive folds: `{row['supportive_temporal_folds']}`",
                f"- Adjusted p: `{row['null_evidence']['family_adjusted_probability']:.6f}`",
                f"- Brier: `{row['model_evidence']['brier_score']:.6f}`",
                "",
            ]
        )
    return "\n".join(lines)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected:
        raise OpeningDirectionHazardError(f"Frozen {label} is missing or changed: {path}")
