from __future__ import annotations

import gc
import hashlib
import json
import math
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _build_past_only_feature_frame,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
)
from hydra.research.accelerated_context_tournament import (
    _apply_context,
)
from hydra.research.equity_open_gap_reversal import (
    MAP_TYPE,
    SOURCE_PREREGISTRATION_SHA256,
    _account_replay,
    _write_immutable,
)
from hydra.research.qd_economic_tournament import (
    MARKET_PAIRS,
    SESSION_CLOCKS,
    SYMBOLS,
    THRESHOLD_HISTORY_SESSIONS,
    VALIDATION_FOLDS,
    _causal_session_thresholds,
    _non_overlapping_events,
    _period_events,
    _period_metrics,
    _prepare_execution_cache,
    _prepare_feature_frame,
    _profile_session_mask,
    _round_turn_cost_all,
    _validation_events,
    _validation_metrics,
    attach_event_mae,
    build_market_path_cache,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "counterfactual_hazard_primary_v1"
PRIMARY_ALPHA = 0.03
EARLY_ROUND_1 = ("2023-01-01", "2023-07-01")
EARLY_ROUND_2 = ("2023-07-01", "2024-01-01")
FEATURES = (
    "path_efficiency_30",
    "failed_displacement_recovery_30_5",
    "range_compression_30_180",
    "accepted_price_migration_30",
)
SIGNED_FEATURES = frozenset({"accepted_price_migration_30"})
FEATURE_FAMILIES = {
    "path_efficiency_30": "counterfactual_path_efficiency",
    "failed_displacement_recovery_30_5": "counterfactual_recovery_hazard",
    "range_compression_30_180": "counterfactual_expansion_hazard",
    "accepted_price_migration_30": "counterfactual_acceptance_migration",
}
RECIPES = (
    {
        "name": "open_q65_h15_5m_agree",
        "session": "open",
        "quantile": 0.65,
        "horizon": 15,
        "policy_direction": "continuation",
        "activation_context": "completed_5m_trend_agree",
    },
    {
        "name": "middle_q75_h30_15m_disagree",
        "session": "middle",
        "quantile": 0.75,
        "horizon": 30,
        "policy_direction": "reversal",
        "activation_context": "completed_15m_trend_disagree",
    },
    {
        "name": "late_q65_h30_15m_expansion",
        "session": "late",
        "quantile": 0.65,
        "horizon": 30,
        "policy_direction": "continuation",
        "activation_context": "completed_15m_volatility_expansion",
    },
    {
        "name": "all_q75_h60_30m_agree",
        "session": "all",
        "quantile": 0.75,
        "horizon": 60,
        "policy_direction": "reversal",
        "activation_context": "completed_30m_trend_agree",
    },
)


class CounterfactualHazardPrimaryError(RuntimeError):
    pass


def generate_counterfactual_hypotheses() -> list[dict[str, Any]]:
    hypotheses: list[dict[str, Any]] = []
    for market, execution_market in MARKET_PAIRS.items():
        ecology = (
            "equity_indices"
            if market in {"ES", "NQ", "RTY", "YM"}
            else "metals"
            if market == "GC"
            else "energy"
        )
        for feature in FEATURES:
            for recipe in RECIPES:
                specification = {
                    "representation": VERSION,
                    "market": market,
                    "execution_market": execution_market,
                    "market_ecology": ecology,
                    "feature": feature,
                    "mechanism_family": FEATURE_FAMILIES[feature],
                    **recipe,
                }
                fingerprint = structural_fingerprint(specification)
                hypotheses.append(
                    {
                        **specification,
                        "candidate_id": (
                            f"strategy_cf_hazard_{market}_{feature}_{recipe['name']}_v1"
                        ),
                        "lineage_id": f"lineage_cf_hazard_{fingerprint[:20]}",
                        "structural_fingerprint": fingerprint,
                        "portfolio_role": (
                            "reversal"
                            if recipe["policy_direction"] == "reversal"
                            else "trend"
                        ),
                    }
                )
    hypotheses.sort(key=lambda item: item["candidate_id"])
    return hypotheses


def add_counterfactual_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    key = out["contiguous_segment_id"]
    close = out["close"].astype(float)
    grouped_close = close.groupby(key, sort=False)
    one_bar_change = grouped_close.diff()
    one_bar_return = grouped_close.pct_change(fill_method=None)

    def past_rolling(
        series: pd.Series, window: int, minimum: int, operation: str
    ) -> pd.Series:
        grouped = series.groupby(key, sort=False).rolling(window, min_periods=minimum)
        result = getattr(grouped, operation)().reset_index(level=0, drop=True).sort_index()
        return result.groupby(key, sort=False).shift(1)

    path_length = past_rolling(one_bar_change.abs(), 30, 15, "sum")
    displacement_30 = grouped_close.diff(30).groupby(key, sort=False).shift(1)
    out["path_efficiency_30"] = displacement_30.abs() / path_length.replace(0, np.nan)
    return_30 = grouped_close.pct_change(30, fill_method=None).groupby(
        key, sort=False
    ).shift(1)
    return_5 = grouped_close.pct_change(5, fill_method=None).groupby(
        key, sort=False
    ).shift(1)
    out["failed_displacement_recovery_30_5"] = (
        -np.sign(return_30) * return_5
    ).clip(lower=0)
    short_variation = past_rolling(one_bar_return.abs(), 30, 15, "mean")
    long_variation = past_rolling(one_bar_return.abs(), 180, 60, "mean")
    out["range_compression_30_180"] = short_variation / long_variation.replace(
        0, np.nan
    )
    accepted_now = past_rolling(close, 15, 10, "mean")
    accepted_before = accepted_now.groupby(key, sort=False).shift(15)
    lagged_close = grouped_close.shift(1).abs().replace(0, np.nan)
    out["accepted_price_migration_30"] = (
        accepted_now - accepted_before
    ) / lagged_close
    out[list(FEATURES)] = out[list(FEATURES)].replace([np.inf, -np.inf], np.nan)
    return out


def build_counterfactual_base_events(
    frame: pd.DataFrame,
    hypothesis: dict[str, Any],
    *,
    threshold_override: pd.Series | None = None,
) -> pd.DataFrame:
    data = frame
    feature = str(hypothesis["feature"])
    values = pd.to_numeric(data[feature], errors="coerce")
    quantile = float(hypothesis["quantile"])
    threshold = (
        threshold_override.reindex(data.index)
        if threshold_override is not None
        else _causal_session_thresholds(
            values, data["trading_session_id"], [quantile]
        )[quantile]
    )
    segment = data["contiguous_segment_id"]
    magnitudes = values.abs()
    crossing = (magnitudes >= threshold) & (
        magnitudes.groupby(segment, sort=False).shift(1)
        < threshold.groupby(segment, sort=False).shift(1)
    )
    anchor = (
        np.sign(values)
        if feature in SIGNED_FEATURES
        else np.sign(pd.to_numeric(data["past_return_60"], errors="coerce"))
    )
    policy_sign = 1 if hypothesis["policy_direction"] == "continuation" else -1
    side = pd.Series(anchor * policy_sign, index=data.index, dtype=float)
    horizon = int(hypothesis["horizon"])
    session_mask = _profile_session_mask(
        data, str(hypothesis["session"]), horizon=horizon
    )
    opportunity_frequency = (
        crossing.astype(float)
        .groupby(segment, sort=False)
        .rolling(120, min_periods=30)
        .mean()
        .reset_index(level=0, drop=True)
        .sort_index()
        .groupby(segment, sort=False)
        .shift(1)
    )
    valid_timing = (
        data["next_timestamp"].eq(data["timestamp"] + pd.Timedelta(minutes=1))
        & data["entry_timestamp"].eq(
            data["timestamp"] + pd.Timedelta(minutes=2)
        )
        & data[f"exit_timestamp_{horizon}"].eq(
            data["entry_timestamp"] + pd.Timedelta(minutes=horizon)
        )
    )
    mask = (
        crossing
        & session_mask
        & side.notna()
        & side.ne(0)
        & valid_timing
        & data["entry_price"].notna()
        & data[f"exit_price_{horizon}"].notna()
        & opportunity_frequency.notna()
    )
    columns = [
        "timestamp",
        "symbol",
        "active_contract",
        "trading_session_id",
        "contiguous_segment_id",
        "symbol_position",
        "session_phase_15m",
        "past_volatility",
        "past_return_60",
        "past_participation",
    ]
    selected = data.loc[mask, columns].copy()
    if selected.empty:
        return _empty_counterfactual_events()
    selected["feature_value"] = values.loc[selected.index]
    selected["threshold"] = threshold.loc[selected.index]
    selected["past_opportunity_frequency"] = opportunity_frequency.loc[selected.index]
    selected["side"] = side.loc[selected.index].astype(int)
    selected["decision_timestamp"] = selected["timestamp"] + pd.Timedelta(minutes=1)
    selected["entry_timestamp"] = data.loc[selected.index, "entry_timestamp"]
    selected["entry_price"] = data.loc[selected.index, "entry_price"].astype(float)
    selected["exit_timestamp"] = data.loc[
        selected.index, f"exit_timestamp_{horizon}"
    ]
    selected["exit_price"] = data.loc[
        selected.index, f"exit_price_{horizon}"
    ].astype(float)
    selected["event_session_id"] = selected["trading_session_id"].astype(str)
    selected["point_value"] = _point_value(str(hypothesis["market"]))
    selected["cost"] = _round_turn_cost_all(str(hypothesis["market"]))
    selected["gross_pnl"] = (
        selected["side"]
        * (selected["exit_price"] - selected["entry_price"])
        * selected["point_value"]
    )
    selected["net_pnl"] = selected["gross_pnl"] - selected["cost"]
    selected["mae_dollars"] = np.nan
    selected["holding_horizon_minutes"] = horizon
    selected["profile"] = str(hypothesis["name"])
    selected["feature"] = feature
    selected["activation_context"] = "base_counterfactual_pool"
    return _non_overlapping_events(selected).sort_values("entry_timestamp").reset_index(
        drop=True
    )


def match_counterfactual_events(
    treated: pd.DataFrame, controls: pd.DataFrame
) -> pd.DataFrame:
    if treated.empty or controls.empty:
        return _empty_pairs()
    treatment = treated.copy().sort_values("entry_timestamp").reset_index(drop=True)
    pool = controls.copy().sort_values("entry_timestamp").reset_index(drop=True)
    covariates = [
        "past_volatility",
        "past_return_60",
        "past_participation",
        "past_opportunity_frequency",
    ]
    union = pd.concat([treatment[covariates], pool[covariates]], ignore_index=True)
    center = union.median(numeric_only=True)
    scale = (union.quantile(0.75) - union.quantile(0.25)).replace(0, np.nan)
    scale = scale.fillna(union.std().replace(0, 1.0)).fillna(1.0)
    used: set[int] = set()
    pairs: list[dict[str, Any]] = []
    pool["session_bucket"] = (pool["session_phase_15m"].astype(int) // 8).astype(int)
    for treated_index, event in treatment.iterrows():
        session_bucket = int(event["session_phase_15m"]) // 8
        candidates = pool[
            pool["active_contract"].astype(str).eq(str(event["active_contract"]))
            & pool["session_bucket"].eq(session_bucket)
            & pool["trading_session_id"].astype(str).ne(
                str(event["trading_session_id"])
            )
            & ~pool.index.isin(used)
        ]
        candidates = candidates.dropna(subset=covariates)
        if candidates.empty or any(pd.isna(event[column]) for column in covariates):
            continue
        treated_values = event[covariates].astype(float)
        distances = (((candidates[covariates] - treated_values) / scale) ** 2).sum(
            axis=1
        )
        control_index = int(
            sorted(
                distances.items(),
                key=lambda item: (
                    float(item[1]),
                    str(candidates.loc[item[0], "entry_timestamp"]),
                    int(item[0]),
                ),
            )[0][0]
        )
        control = pool.loc[control_index]
        used.add(control_index)
        row: dict[str, Any] = {
            "pair_id": f"pair_{treated_index:06d}_{control_index:06d}",
            "symbol": str(event["symbol"]),
            "active_contract": str(event["active_contract"]),
            "session_bucket": session_bucket,
            "treated_session": str(event["trading_session_id"]),
            "control_session": str(control["trading_session_id"]),
            "treated_entry_timestamp": event["entry_timestamp"],
            "control_entry_timestamp": control["entry_timestamp"],
            "treated_net_pnl": float(event["net_pnl"]),
            "control_net_pnl": float(control["net_pnl"]),
            "treated_positive": int(float(event["net_pnl"]) > 0),
            "control_positive": int(float(control["net_pnl"]) > 0),
            "distance": float(distances.loc[control_index]),
        }
        for column in covariates:
            row[f"treated_{column}"] = float(event[column])
            row[f"control_{column}"] = float(control[column])
        pairs.append(row)
    return pd.DataFrame(pairs) if pairs else _empty_pairs()


def paired_hazard_metrics(pairs: pd.DataFrame) -> dict[str, Any]:
    if pairs.empty:
        return {
            "pairs": 0,
            "treated_positive_probability": 0.0,
            "control_positive_probability": 0.0,
            "probability_uplift": 0.0,
            "treated_mean_net": 0.0,
            "control_mean_net": 0.0,
            "discordant_pairs": 0,
            "positive_discordant_pairs": 0,
            "paired_probability": 1.0,
            "maximum_standardized_covariate_difference": float("inf"),
        }
    treated_positive = pairs["treated_positive"].astype(int)
    control_positive = pairs["control_positive"].astype(int)
    positive_discordant = int(((treated_positive == 1) & (control_positive == 0)).sum())
    negative_discordant = int(((treated_positive == 0) & (control_positive == 1)).sum())
    discordant = positive_discordant + negative_discordant
    probability = _one_sided_binomial_probability(positive_discordant, discordant)
    balance = []
    for column in (
        "past_volatility",
        "past_return_60",
        "past_participation",
        "past_opportunity_frequency",
    ):
        treated_values = pairs[f"treated_{column}"].astype(float)
        control_values = pairs[f"control_{column}"].astype(float)
        pooled = math.sqrt(
            max((float(treated_values.var()) + float(control_values.var())) / 2.0, 0.0)
        )
        balance.append(
            abs(float(treated_values.mean() - control_values.mean())) / pooled
            if pooled > 0
            else 0.0
        )
    return {
        "pairs": int(len(pairs)),
        "treated_positive_probability": float(treated_positive.mean()),
        "control_positive_probability": float(control_positive.mean()),
        "probability_uplift": float((treated_positive - control_positive).mean()),
        "treated_mean_net": float(pairs["treated_net_pnl"].mean()),
        "control_mean_net": float(pairs["control_net_pnl"].mean()),
        "discordant_pairs": discordant,
        "positive_discordant_pairs": positive_discordant,
        "paired_probability": probability,
        "maximum_standardized_covariate_difference": float(max(balance, default=0.0)),
    }


def _one_sided_binomial_probability(successes: int, trials: int) -> float:
    if trials <= 0:
        return 1.0
    return float(
        sum(math.comb(trials, value) for value in range(successes, trials + 1))
        / (2**trials)
    )


def _point_value(symbol: str) -> float:
    return float(instrument_spec(symbol).point_value)


def _empty_counterfactual_events() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "timestamp",
            "symbol",
            "active_contract",
            "trading_session_id",
            "contiguous_segment_id",
            "symbol_position",
            "session_phase_15m",
            "past_volatility",
            "past_return_60",
            "past_participation",
            "past_opportunity_frequency",
            "feature_value",
            "threshold",
            "side",
            "decision_timestamp",
            "entry_timestamp",
            "entry_price",
            "exit_timestamp",
            "exit_price",
            "event_session_id",
            "point_value",
            "cost",
            "gross_pnl",
            "net_pnl",
            "mae_dollars",
            "holding_horizon_minutes",
            "profile",
            "feature",
            "activation_context",
        ]
    )


def _empty_pairs() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "pair_id",
            "symbol",
            "active_contract",
            "session_bucket",
            "treated_session",
            "control_session",
            "treated_entry_timestamp",
            "control_entry_timestamp",
            "treated_net_pnl",
            "control_net_pnl",
            "treated_positive",
            "control_positive",
            "distance",
        ]
    )


def run_counterfactual_hazard_primary(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    task = Path(engineering_task_path)
    map_path = Path(repaired_map_path)
    _verify(task, engineering_task_sha256, "engineering task")
    _verify(map_path, repaired_map_sha256, "explicit-contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise CounterfactualHazardPrimaryError("Explicit-contract map changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise CounterfactualHazardPrimaryError(
                "Worker commit differs from queued specification."
            )
    hypotheses = generate_counterfactual_hypotheses()
    if len(hypotheses) != 96 or len(
        {item["structural_fingerprint"] for item in hypotheses}
    ) != 96:
        raise CounterfactualHazardPrimaryError("Frozen population drifted.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = _preregistration(
        hypotheses,
        engineering_task_sha256=engineering_task_sha256,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        code_commit=code_commit,
    )
    preregistration_path = destination / "counterfactual_hazard_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    population_path = destination / "counterfactual_hazard_population.json"
    _write_immutable(
        population_path,
        json.dumps(
            {
                "schema": "counterfactual_hazard_population_v1",
                "population_hash": preregistration["population_hash"],
                "hypotheses": hypotheses,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    access = _record_access_once(hypotheses) if record_data_access else None
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
    _verify(source_preregistration, SOURCE_PREREGISTRATION_SHA256, "data manifest")
    source = json.loads(source_preregistration.read_text(encoding="utf-8"))
    _verify_development_manifest(
        (source.get("source") or {}).get("development_data_manifest") or {}
    )
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    latest_source_timestamp = pd.to_datetime(raw["timestamp"], utc=True).max()
    features = add_counterfactual_features(
        _prepare_feature_frame(_build_past_only_feature_frame(raw))
    )
    del raw
    gc.collect()
    full_frames = {
        symbol: _prepare_execution_cache(
            group.sort_values("timestamp").reset_index(drop=True)
        )
        for symbol, group in features.groupby("symbol", sort=True)
    }
    del features
    gc.collect()
    early_frames = {
        symbol: _prepare_execution_cache(
            frame.loc[frame["trading_session_id"].astype(str) < "2024-01-01"]
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        for symbol, frame in full_frames.items()
    }
    if any(
        pd.to_datetime(frame["timestamp"], utc=True).max()
        >= pd.Timestamp("2024-01-01", tz="UTC")
        for frame in early_frames.values()
        if not frame.empty
    ):
        raise CounterfactualHazardPrimaryError("Early selection exposed 2024.")
    preparation_seconds = time.perf_counter() - started

    early_context_cache: dict[tuple[str, int], pd.DataFrame] = {}
    early_sets = _build_event_sets(early_frames, hypotheses, early_context_cache)
    round1_survivors: list[dict[str, Any]] = []
    dispositions: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        base, treated = early_sets[hypothesis["candidate_id"]]
        direct = _period_metrics(_period_events(treated, *EARLY_ROUND_1))
        pairs = _period_pairs(base, treated, *EARLY_ROUND_1)
        hazard = paired_hazard_metrics(pairs)
        passed = bool(
            direct["events"] >= 8
            and direct["net_pnl"] > 0
            and direct["cost_stress_1_5x_net"] > 0
            and hazard["pairs"] >= 5
        )
        row = {
            **hypothesis,
            "round1_direct": direct,
            "round1_hazard": hazard,
            "round1_pass": passed,
        }
        if passed:
            round1_survivors.append(row)
        else:
            dispositions.append(
                {
                    "candidate_id": hypothesis["candidate_id"],
                    "stage": "ROUND1",
                    "reason": _early_failure_reason(direct, hazard, round_number=1),
                }
            )
    round2_survivors: list[dict[str, Any]] = []
    for hypothesis in round1_survivors:
        base, treated = early_sets[hypothesis["candidate_id"]]
        direct = _period_metrics(_period_events(treated, *EARLY_ROUND_2))
        pairs = _period_pairs(base, treated, *EARLY_ROUND_2)
        hazard = paired_hazard_metrics(pairs)
        passed = bool(
            direct["events"] >= 10
            and direct["net_pnl"] > 0
            and direct["cost_stress_1_5x_net"] > 0
            and direct["best_positive_event_share"] <= 0.40
            and hazard["pairs"] >= 8
            and hazard["probability_uplift"] > 0
        )
        row = {
            **hypothesis,
            "round2_direct": direct,
            "round2_hazard": hazard,
            "round2_pass": passed,
        }
        if passed:
            round2_survivors.append(row)
        else:
            dispositions.append(
                {
                    "candidate_id": hypothesis["candidate_id"],
                    "stage": "ROUND2",
                    "reason": _early_failure_reason(direct, hazard, round_number=2),
                }
            )

    micro_hypotheses = [
        {
            **item,
            "market": item["execution_market"],
            "candidate_id": f"{item['candidate_id']}__early_micro",
        }
        for item in round2_survivors
    ]
    early_micro_sets = _build_event_sets(
        early_frames, micro_hypotheses, early_context_cache
    )
    archive, ranking = _build_archive_and_primary_ranking(
        round2_survivors, early_micro_sets
    )
    primary_id = next(
        (row["candidate_id"] for row in ranking if row["eligible"]), None
    )
    primary = next(
        (item for item in archive if item["candidate_id"] == primary_id), None
    )
    archive_manifest = _archive_manifest(
        archive=archive,
        dispositions=dispositions,
        population_hash=preregistration["population_hash"],
    )
    archive_path = destination / "counterfactual_hazard_archive_manifest.json"
    _write_immutable(
        archive_path, json.dumps(archive_manifest, indent=2, sort_keys=True) + "\n"
    )
    primary_manifest = _primary_manifest(
        primary=primary,
        ranking=ranking,
        archive_hash=archive_manifest["archive_manifest_hash"],
        population_hash=preregistration["population_hash"],
    )
    primary_manifest_path = destination / "counterfactual_hazard_primary_freeze.json"
    _write_immutable(
        primary_manifest_path,
        json.dumps(primary_manifest, indent=2, sort_keys=True) + "\n",
    )
    freeze_seconds = time.perf_counter() - started

    candidates: list[dict[str, Any]] = []
    pair_ledger = pd.DataFrame()
    trade_ledger = pd.DataFrame()
    shadow_configurations: list[dict[str, Any]] = []
    if primary is not None:
        full_context_cache: dict[tuple[str, int], pd.DataFrame] = {}
        full_sets = _build_event_sets(full_frames, [primary], full_context_cache)
        base, treated = full_sets[primary["candidate_id"]]
        validation_treated = _validation_events(treated)
        validation_base = _validation_events(base)
        control = _control_pool(validation_base, validation_treated)
        pair_ledger = match_counterfactual_events(validation_treated, control)
        hazard = paired_hazard_metrics(pair_ledger)
        micro_hypothesis = {
            **primary,
            "market": primary["execution_market"],
            "candidate_id": f"{primary['candidate_id']}__micro",
        }
        micro_sets = _build_event_sets(
            full_frames, [micro_hypothesis], full_context_cache
        )
        micro_base, micro_treated = micro_sets[micro_hypothesis["candidate_id"]]
        validation_micro = _validation_events(micro_treated)
        micro_pairs = match_counterfactual_events(
            validation_micro,
            _control_pool(_validation_events(micro_base), validation_micro),
        )
        micro_hazard = paired_hazard_metrics(micro_pairs)
        mini_metrics = _validation_metrics(validation_treated)
        micro_metrics = _validation_metrics(validation_micro)
        path_caches = {
            primary["market"]: build_market_path_cache(
                full_frames[primary["market"]]
            ),
            primary["execution_market"]: build_market_path_cache(
                full_frames[primary["execution_market"]]
            ),
        }
        validation_treated = attach_event_mae(
            validation_treated,
            full_frames[primary["market"]],
            path_cache=path_caches[primary["market"]],
        )
        validation_micro = attach_event_mae(
            validation_micro,
            full_frames[primary["execution_market"]],
            path_cache=path_caches[primary["execution_market"]],
        )
        parameter = _parameter_diagnostics(
            full_frames, primary, full_context_cache
        )
        contract_evidence = bool(
            mini_metrics["net_pnl"] > 0
            and micro_metrics["net_pnl"] > 0
            and micro_metrics["supportive_temporal_folds"] >= 1
            and micro_hazard["probability_uplift"] >= 0
        )
        account = _account_replay(
            validation_micro.rename(columns={"net_pnl": "net_pnl_60"}).copy()
        )
        null_pass = bool(
            hazard["pairs"] >= 12
            and hazard["probability_uplift"] > 0
            and hazard["paired_probability"] <= PRIMARY_ALPHA
            and hazard["maximum_standardized_covariate_difference"] <= 0.50
        )
        evidence = ShadowEvidence(
            candidate_id=primary["candidate_id"],
            data_integrity=True,
            no_lookahead=True,
            deterministic_signals=True,
            net_after_costs=float(mini_metrics["net_pnl"]),
            supportive_temporal_folds=int(mini_metrics["supportive_temporal_folds"]),
            catastrophic_transfer=bool(mini_metrics["catastrophic_transfer"]),
            candidate_null_pass=null_pass,
            null_probability=float(hazard["paired_probability"]),
            parameter_stable=bool(parameter["supportive_neighbor_count"] >= 1),
            contract_evidence=contract_evidence,
            account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
            execution_possible=True,
            realtime_features_available=True,
            shadow_spec_complete=True,
            observability_complete=True,
            untouched_holdout_passed=False,
            sample_size=int(mini_metrics["events"]),
            uncertainty="counterfactual_development_confirmation_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise CounterfactualHazardPrimaryError(
                "Development-only experiment attempted paper promotion."
            )
        candidate = {
            "candidate_id": primary["candidate_id"],
            "lineage_id": primary["lineage_id"],
            "structural_fingerprint": primary["structural_fingerprint"],
            "mechanism_family": primary["mechanism_family"],
            "primary_market": primary["market"],
            "execution_market": primary["execution_market"],
            "market_ecology": primary["market_ecology"],
            "portfolio_role": primary["portfolio_role"],
            "feature": primary["feature"],
            "activation_context": primary["activation_context"],
            "profile": primary["name"],
            "status": admission.tier.value,
            "admission": admission.to_dict(),
            "events": int(mini_metrics["events"]),
            "net_pnl": float(mini_metrics["net_pnl"]),
            "micro_events": int(micro_metrics["events"]),
            "micro_net_pnl": float(micro_metrics["net_pnl"]),
            "supportive_temporal_folds": int(
                mini_metrics["supportive_temporal_folds"]
            ),
            "fold_results": mini_metrics["fold_results"],
            "micro_fold_results": micro_metrics["fold_results"],
            "cost_stress_1_5x_net": float(
                mini_metrics["cost_stress_1_5x_net"]
            ),
            "counterfactual_hazard": hazard,
            "micro_counterfactual_hazard": micro_hazard,
            "null_evidence": {
                "method": "preselected_primary_exact_paired_positive_outcome_test",
                "raw_probability": float(hazard["paired_probability"]),
                "prospective_alpha": PRIMARY_ALPHA,
                "promotion_test_count": 1,
                "passed": null_pass,
            },
            "contract_transfer": {
                "mini": primary["market"],
                "micro": primary["execution_market"],
                "passed": contract_evidence,
            },
            "parameter_diagnostics": parameter,
            "attacks": {
                "best_positive_event_share": float(
                    mini_metrics["best_positive_event_share"]
                ),
                "best_fold_share": float(
                    mini_metrics["best_positive_fold_share"]
                ),
                "event_dominated": bool(mini_metrics["event_dominated"]),
                "one_bar_execution_delay_embedded": True,
                "completed_higher_timeframe_only": True,
                "outcome_blind_matching": True,
            },
            "topstep": account,
            "shadow_evidence": evidence.__dict__,
        }
        candidates.append(candidate)
        if admission.permits_zero_risk_shadow:
            specification = _shadow_specification(
                primary, primary_manifest["primary_manifest_hash"]
            )
            path = specification.write_immutable(
                destination
                / "shadow_configurations"
                / f"{primary['candidate_id']}.json"
            )
            shadow_configurations.append(
                {
                    "candidate_id": primary["candidate_id"],
                    "status": admission.tier.value,
                    "path": str(path),
                    "configuration_hash": specification.configuration_hash,
                    "outbound_orders_enabled": False,
                }
            )
        trade_ledger = pd.concat(
            [
                validation_treated.assign(contract_role="primary_mini"),
                validation_micro.assign(contract_role="primary_micro"),
            ],
            ignore_index=True,
        )

    statuses = [row["status"] for row in candidates]
    promising_tiers = {
        EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
        EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
    }
    promising = sum(status in promising_tiers for status in statuses)
    shadow_count = statuses.count(EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value)
    topstep_count = sum(
        bool((row.get("topstep") or {}).get("path_candidate")) for row in candidates
    )
    pair_path = destination / "counterfactual_hazard_pair_ledger.jsonl"
    trade_path = destination / "counterfactual_hazard_trade_ledger.jsonl"
    _write_dataframe_ledger(pair_path, pair_ledger, ["treated_entry_timestamp", "pair_id"])
    _write_dataframe_ledger(
        trade_path, trade_ledger, ["entry_timestamp", "symbol", "contract_role"]
    )
    integrity = _integrity(
        hypotheses=hypotheses,
        early_frames=early_frames,
        primary=primary,
        primary_manifest=primary_manifest,
        archive=archive,
        pair_ledger=pair_ledger,
        trade_ledger=trade_ledger,
        latest_source_timestamp=latest_source_timestamp,
    )
    if not all(integrity.values()):
        raise CounterfactualHazardPrimaryError(f"Integrity proof failed: {integrity}")
    if shadow_count:
        conclusion = "COUNTERFACTUAL_HAZARD_SHADOW_CANDIDATE_FOUND"
        next_action = "ACTIVATE_COUNTERFACTUAL_SHADOW_AND_REPLICATE"
    elif promising:
        conclusion = "COUNTERFACTUAL_HAZARD_PROMISING_BUT_INSUFFICIENT"
        next_action = "FRESH_ID_REPLICATION_OR_FORWARD_SHADOW_RESEARCH"
    elif primary is None:
        conclusion = "COUNTERFACTUAL_HAZARD_NO_EARLY_PRIMARY"
        next_action = "PIVOT_HAZARD_TARGET_OR_MARKET_ECOLOGY"
    else:
        conclusion = "COUNTERFACTUAL_HAZARD_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
        next_action = "KILL_EXACT_PRIMARY_AND_PIVOT_DISTRIBUTIONAL_TARGET"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Exactly one primary at most was frozen on 2023 before its unchanged 2024 "
            "confirmation. Archive candidates are diagnostic-only. Q4 remained sealed; "
            "PAPER_SHADOW_READY is prohibited."
        ),
        "code_commit": code_commit,
        "candidate_count": len(hypotheses),
        "structural_prototypes": len(hypotheses),
        "round1_survivors": len(round1_survivors),
        "round2_survivors": len(round2_survivors),
        "diagnostic_archive_size": len(archive),
        "promotion_primary_count": int(primary is not None),
        "primary_candidate_id": primary["candidate_id"] if primary else None,
        "candidates": candidates,
        "candidate_tier_counts": dict(Counter(statuses)),
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow_count),
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(topstep_count),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "diagnostic_archive": [item["candidate_id"] for item in archive],
        "dispositions": dispositions,
        "shadow_configurations": shadow_configurations,
        "integrity_proof": integrity,
        "data_provenance": provenance,
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "population_path": str(population_path),
        "archive_manifest_path": str(archive_path),
        "archive_manifest_hash": archive_manifest["archive_manifest_hash"],
        "primary_manifest_path": str(primary_manifest_path),
        "primary_manifest_hash": primary_manifest["primary_manifest_hash"],
        "performance": {
            "feature_preparation_seconds": preparation_seconds,
            "primary_freeze_seconds": freeze_seconds,
            "total_seconds": time.perf_counter() - started,
        },
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
    result_path = destination / "counterfactual_hazard_result.json"
    report_path = destination / "counterfactual_hazard_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "primary_manifest_path": str(primary_manifest_path),
            "archive_manifest_path": str(archive_path),
            "pair_ledger_path": str(pair_path),
            "trade_ledger_path": str(trade_path),
        },
        "report_path": str(report_path),
    }


def _build_event_sets(
    frames: dict[str, pd.DataFrame],
    hypotheses: list[dict[str, Any]],
    context_cache: dict[tuple[str, int], pd.DataFrame],
) -> dict[str, tuple[pd.DataFrame, pd.DataFrame]]:
    output: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    for market in sorted({str(item["market"]) for item in hypotheses}):
        frame = frames[market]
        market_hypotheses = [item for item in hypotheses if item["market"] == market]
        thresholds: dict[tuple[str, float], pd.Series] = {}
        for feature in sorted({str(item["feature"]) for item in market_hypotheses}):
            quantiles = sorted(
                {
                    float(item["quantile"])
                    for item in market_hypotheses
                    if item["feature"] == feature
                }
            )
            values = _causal_session_thresholds(
                frame[feature], frame["trading_session_id"], quantiles
            )
            thresholds.update(
                {(feature, quantile): values[quantile] for quantile in quantiles}
            )
        base_cache: dict[tuple[Any, ...], pd.DataFrame] = {}
        for hypothesis in market_hypotheses:
            key = (
                hypothesis["feature"],
                hypothesis["policy_direction"],
                hypothesis["session"],
                float(hypothesis["quantile"]),
                int(hypothesis["horizon"]),
            )
            if key not in base_cache:
                base_cache[key] = build_counterfactual_base_events(
                    frame,
                    hypothesis,
                    threshold_override=thresholds[
                        (str(hypothesis["feature"]), float(hypothesis["quantile"]))
                    ],
                )
            base = base_cache[key]
            treated = _apply_context(
                base,
                frame,
                str(hypothesis["activation_context"]),
                context_cache,
            )
            output[str(hypothesis["candidate_id"])] = (base, treated)
    return output


def _control_pool(base: pd.DataFrame, treated: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return base.copy()
    treated_keys = {
        (str(row.active_contract), pd.Timestamp(row.entry_timestamp).value)
        for row in treated[["active_contract", "entry_timestamp"]].itertuples(
            index=False
        )
    }
    keep = [
        (str(row.active_contract), pd.Timestamp(row.entry_timestamp).value)
        not in treated_keys
        for row in base[["active_contract", "entry_timestamp"]].itertuples(
            index=False
        )
    ]
    return base.loc[keep].copy().reset_index(drop=True)


def _period_pairs(
    base: pd.DataFrame, treated: pd.DataFrame, start: str, end: str
) -> pd.DataFrame:
    period_base = _period_events(base, start, end)
    period_treated = _period_events(treated, start, end)
    return match_counterfactual_events(
        period_treated, _control_pool(period_base, period_treated)
    )


def _early_failure_reason(
    direct: dict[str, Any], hazard: dict[str, Any], *, round_number: int
) -> str:
    minimum_events = 8 if round_number == 1 else 10
    minimum_pairs = 5 if round_number == 1 else 8
    if direct["events"] < minimum_events or hazard["pairs"] < minimum_pairs:
        return "INSUFFICIENT_SAMPLES_OR_MATCHES"
    if direct["net_pnl"] <= 0:
        return "NEGATIVE_ECONOMICS"
    if direct["cost_stress_1_5x_net"] <= 0:
        return "COST_FRAGILITY"
    if round_number == 2 and direct["best_positive_event_share"] > 0.40:
        return "CONCENTRATION"
    if round_number == 2 and hazard["probability_uplift"] <= 0:
        return "NO_COUNTERFACTUAL_HAZARD_UPLIFT"
    return "UNSPECIFIED_FAILURE"


def _build_archive_and_primary_ranking(
    survivors: list[dict[str, Any]],
    early_micro_sets: dict[str, tuple[pd.DataFrame, pd.DataFrame]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scored: list[dict[str, Any]] = []
    for item in survivors:
        direct = item["round2_direct"]
        hazard = item["round2_hazard"]
        score = (
            float(direct["net_pnl"])
            / max(float(direct["maximum_drawdown"]), 1.0)
            + 4.0 * float(hazard["probability_uplift"])
        )
        scored.append({**item, "archive_score": score})
    scored.sort(
        key=lambda item: (
            -float(item["archive_score"]),
            str(item["structural_fingerprint"]),
        )
    )
    archive: list[dict[str, Any]] = []
    occupied: set[tuple[str, str]] = set()
    for item in scored:
        niche = (str(item["market_ecology"]), str(item["mechanism_family"]))
        if niche in occupied:
            continue
        archive.append(item)
        occupied.add(niche)
        if len(archive) >= 12:
            break
    ranking: list[dict[str, Any]] = []
    for item in archive:
        micro_id = f"{item['candidate_id']}__early_micro"
        micro_base, micro_treated = early_micro_sets[micro_id]
        micro_direct = _period_metrics(
            _period_events(micro_treated, *EARLY_ROUND_2)
        )
        micro_hazard = paired_hazard_metrics(
            _period_pairs(micro_base, micro_treated, *EARLY_ROUND_2)
        )
        direct = item["round2_direct"]
        hazard = item["round2_hazard"]
        eligible = bool(
            direct["net_pnl"] > 0
            and direct["cost_stress_1_5x_net"] > 0
            and micro_direct["net_pnl"] > 0
            and micro_direct["cost_stress_1_5x_net"] > 0
            and hazard["probability_uplift"] > 0
            and micro_hazard["probability_uplift"] >= 0
        )
        minimum_efficiency = min(
            float(direct["net_pnl"])
            / max(float(direct["maximum_drawdown"]), 1.0),
            float(micro_direct["net_pnl"])
            / max(float(micro_direct["maximum_drawdown"]), 1.0),
        )
        ranking.append(
            {
                "candidate_id": item["candidate_id"],
                "eligible": eligible,
                "minimum_mini_micro_net_to_drawdown": minimum_efficiency,
                "minimum_mini_micro_hazard_uplift": min(
                    float(hazard["probability_uplift"]),
                    float(micro_hazard["probability_uplift"]),
                ),
                "mini_direct": direct,
                "mini_hazard": hazard,
                "micro_direct": micro_direct,
                "micro_hazard": micro_hazard,
                "structural_fingerprint": item["structural_fingerprint"],
            }
        )
    ranking.sort(
        key=lambda row: (
            -int(row["eligible"]),
            -float(row["minimum_mini_micro_hazard_uplift"]),
            -float(row["minimum_mini_micro_net_to_drawdown"]),
            str(row["structural_fingerprint"]),
        )
    )
    return archive, ranking


def _archive_manifest(
    *,
    archive: list[dict[str, Any]],
    dispositions: list[dict[str, Any]],
    population_hash: str,
) -> dict[str, Any]:
    payload = {
        "schema": "counterfactual_hazard_archive_manifest_v1",
        "population_hash": population_hash,
        "selection_data_start": "2023-01-01",
        "selection_data_end_exclusive": "2024-01-01",
        "selected_candidate_ids": [item["candidate_id"] for item in archive],
        "selected_structural_fingerprints": [
            item["structural_fingerprint"] for item in archive
        ],
        "diagnostic_only": True,
        "disposition_count": len(dispositions),
        "q4_access_allowed": False,
    }
    payload["archive_manifest_hash"] = _stable_hash(payload)
    return payload


def _primary_manifest(
    *,
    primary: dict[str, Any] | None,
    ranking: list[dict[str, Any]],
    archive_hash: str,
    population_hash: str,
) -> dict[str, Any]:
    payload = {
        "schema": "counterfactual_hazard_primary_freeze_v1",
        "primary_candidate_id": primary["candidate_id"] if primary else None,
        "primary_specification": (
            {
                key: primary[key]
                for key in (
                    "candidate_id",
                    "lineage_id",
                    "structural_fingerprint",
                    "market",
                    "execution_market",
                    "feature",
                    "mechanism_family",
                    "policy_direction",
                    "session",
                    "quantile",
                    "horizon",
                    "activation_context",
                )
            }
            if primary
            else None
        ),
        "early_fold_ranking": ranking,
        "archive_manifest_hash": archive_hash,
        "population_hash": population_hash,
        "promotion_test_count": int(primary is not None),
        "candidate_probability_threshold": PRIMARY_ALPHA,
        "matching_policy": {
            "contract": "exact",
            "session_phase_bucket_2h": "exact",
            "different_trading_session": True,
            "covariates": [
                "past_volatility",
                "past_return_60",
                "past_participation",
                "past_opportunity_frequency",
            ],
            "distance": "outcome_blind_scaled_euclidean",
            "control_reuse": False,
        },
        "selection_data_end_exclusive": "2024-01-01",
        "confirmation_data_start": "2024-01-01",
        "confirmation_data_end_exclusive": "2024-10-01",
        "diagnostic_archive_promotion_eligible": False,
        "q4_access_allowed": False,
        "paper_shadow_ready_allowed": False,
    }
    payload["primary_manifest_hash"] = _stable_hash(payload)
    return payload


def _parameter_diagnostics(
    frames: dict[str, pd.DataFrame],
    primary: dict[str, Any],
    context_cache: dict[tuple[str, int], pd.DataFrame],
) -> dict[str, Any]:
    variants: dict[str, Any] = {}
    for delta in (-0.10, 0.10):
        quantile = min(max(float(primary["quantile"]) + delta, 0.50), 0.90)
        label = f"quantile_{int(round(quantile * 100))}"
        variant = {
            **primary,
            "candidate_id": f"{primary['candidate_id']}__diagnostic_{label}",
            "quantile": quantile,
        }
        base, treated = _build_event_sets(
            frames, [variant], context_cache
        )[variant["candidate_id"]]
        validation = _validation_events(treated)
        hazard = paired_hazard_metrics(
            match_counterfactual_events(
                validation,
                _control_pool(_validation_events(base), validation),
            )
        )
        direct = _validation_metrics(validation)
        variants[label] = {"direct": direct, "hazard": hazard}
    return {
        "diagnostic_only": True,
        "variants": variants,
        "supportive_neighbor_count": int(
            sum(
                row["direct"]["net_pnl"] > 0
                and row["hazard"]["probability_uplift"] > 0
                for row in variants.values()
            )
        ),
    }


def _shadow_specification(
    primary: dict[str, Any], source_manifest_hash: str
) -> ShadowSpecification:
    market = str(primary["execution_market"])
    context = str(primary["activation_context"])
    context_minutes = int(context.split("m_", 1)[0].removeprefix("completed_"))
    return ShadowSpecification(
        strategy_id=str(primary["candidate_id"]),
        strategy_version="v1_counterfactual_hazard_pre_holdout",
        feature_versions=(
            "counterfactual_path_state_features_v1",
            "closed_bar_context_v1",
            "outcome_blind_matching_policy_v1",
        ),
        markets=(market,),
        timeframes=("1m", f"{context_minutes}m"),
        session_rules={
            "timezone": "America/Chicago",
            "market_open_minute": SESSION_CLOCKS[market][0],
            "session_profile": primary["session"],
            "mandatory_flatten_before_session_end": True,
        },
        entry_rules={
            "event": "past_only_counterfactual_state_threshold_crossing",
            "feature": primary["feature"],
            "quantile": primary["quantile"],
            "direction": primary["policy_direction"],
            "activation_context": context,
            "higher_timeframe_availability": "completed_bar_at_or_before_decision",
            "minimum_prior_feature_rows": 500,
            "threshold_history_sessions": THRESHOLD_HISTORY_SESSIONS,
            "execution_delay_completed_bars": 1,
            "missing_feature_or_context_policy": "fail_closed_skip_signal",
        },
        exit_rules={
            "holding_completed_1m_bars": int(primary["horizon"]),
            "no_overnight": True,
        },
        sizing={"contracts": 1, "instrument": market, "micro_first": True},
        costs={
            "round_turn_usd": _round_turn_cost_all(market),
            "slippage_ticks_round_turn": 2,
        },
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=int(primary["horizon"]) * 60,
        maximum_exposure=0.1,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=(
            "stale_data",
            "duplicate_signal",
            "session_closed",
            "clock_invalid",
            "contract_map_mismatch",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={
            "jsonl": True,
            "signals": True,
            "virtual_fills": True,
            "rejections": True,
            "attribution": True,
        },
        reconciliation={
            "startup": "fail_closed",
            "expected_vs_observed_fill": True,
            "position_source": "virtual_only",
        },
        source_manifest_hash=source_manifest_hash,
        outbound_orders_enabled=False,
    )


def _preregistration(
    hypotheses: list[dict[str, Any]],
    *,
    engineering_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
) -> dict[str, Any]:
    payload = {
        "schema": "counterfactual_hazard_preregistration_v1",
        "population_count": len(hypotheses),
        "population_hash": _stable_hash(
            [
                (item["candidate_id"], item["structural_fingerprint"])
                for item in hypotheses
            ]
        ),
        "features": list(FEATURES),
        "markets": list(MARKET_PAIRS),
        "recipes": list(RECIPES),
        "round1": EARLY_ROUND_1,
        "round2": EARLY_ROUND_2,
        "promotion_primary_maximum": 1,
        "primary_alpha": PRIMARY_ALPHA,
        "counterfactual_target": "positive_net_outcome_probability",
        "matching_outcome_blind": True,
        "archive_diagnostic_only": True,
        "engineering_task_sha256": engineering_task_sha256,
        "repaired_map_sha256": repaired_map_sha256,
        "repaired_roll_map_hash": repaired_roll_map_hash,
        "code_commit": code_commit,
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "paper_shadow_ready_allowed": False,
    }
    payload["preregistration_hash"] = _stable_hash(payload)
    return payload


def _integrity(
    *,
    hypotheses: list[dict[str, Any]],
    early_frames: dict[str, pd.DataFrame],
    primary: dict[str, Any] | None,
    primary_manifest: dict[str, Any],
    archive: list[dict[str, Any]],
    pair_ledger: pd.DataFrame,
    trade_ledger: pd.DataFrame,
    latest_source_timestamp: pd.Timestamp,
) -> dict[str, bool]:
    early_only = all(
        frame.empty
        or pd.to_datetime(frame["timestamp"], utc=True).max()
        < pd.Timestamp("2024-01-01", tz="UTC")
        for frame in early_frames.values()
    )
    pairs_distinct = bool(
        pair_ledger.empty
        or (
            pair_ledger["treated_session"].astype(str)
            != pair_ledger["control_session"].astype(str)
        ).all()
    )
    pairs_contract = bool(
        pair_ledger.empty or pair_ledger["active_contract"].notna().all()
    )
    timing = True
    context_timing = True
    contracts = True
    if not trade_ledger.empty:
        timing = bool(
            (
                trade_ledger["decision_timestamp"]
                == trade_ledger["timestamp"] + pd.Timedelta(minutes=1)
            ).all()
            and (
                trade_ledger["entry_timestamp"]
                == trade_ledger["decision_timestamp"] + pd.Timedelta(minutes=1)
            ).all()
        )
        available = trade_ledger["context_availability_timestamp"].notna()
        context_timing = bool(
            available.all()
            and (
                trade_ledger.loc[available, "context_availability_timestamp"]
                <= trade_ledger.loc[available, "decision_timestamp"]
            ).all()
        )
        contracts = bool(trade_ledger["active_contract"].notna().all())
    return {
        "exact_unique_population": len(hypotheses) == 96
        and len({item["structural_fingerprint"] for item in hypotheses}) == 96,
        "balanced_markets": all(
            sum(item["market"] == market for item in hypotheses) == 16
            for market in MARKET_PAIRS
        ),
        "new_representation_features_only": set(
            item["feature"] for item in hypotheses
        )
        == set(FEATURES),
        "early_selection_excludes_2024": early_only,
        "one_primary_maximum": int(primary is not None) <= 1,
        "manifest_precedes_confirmation_semantically": primary_manifest[
            "selection_data_end_exclusive"
        ]
        == "2024-01-01",
        "diagnostic_archive_not_promotion_eligible": bool(
            not primary_manifest["diagnostic_archive_promotion_eligible"]
        ),
        "archive_niches_unique": len(
            {
                (item["market_ecology"], item["mechanism_family"])
                for item in archive
            }
        )
        == len(archive),
        "matched_controls_distinct_sessions": pairs_distinct,
        "matched_controls_explicit_contract": pairs_contract,
        "matching_outcome_blind": True,
        "one_bar_execution_delay": timing,
        "completed_higher_timeframe_only": context_timing,
        "explicit_contracts": contracts,
        "q4_excluded": bool(
            latest_source_timestamp < pd.Timestamp("2024-10-01", tz="UTC")
        ),
        "paper_promotion_disabled": True,
        "no_outbound_order_capability": True,
    }


def _record_access_once(hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_ids = [item["candidate_id"] for item in hypotheses]
    period = "2023-01-01:2024-10-01"
    reason = "counterfactual hazard single-primary tournament; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.counterfactual_hazard_primary"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.counterfactual_hazard_primary",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _write_dataframe_ledger(
    path: Path, frame: pd.DataFrame, sort_columns: list[str]
) -> None:
    if frame.empty:
        _write_immutable(path, "")
        return
    columns = [column for column in sort_columns if column in frame]
    ordered = frame.sort_values(columns) if columns else frame
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in ordered.to_dict("records")
    ]
    _write_immutable(path, "\n".join(lines) + "\n")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise CounterfactualHazardPrimaryError(
            f"Frozen {label} missing or changed: {path}"
        )


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Counterfactual Hazard Primary v1",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Structural prototypes: `{payload['structural_prototypes']}`",
            f"- Round-1 / Round-2 survivors: `{payload['round1_survivors']}` / `{payload['round2_survivors']}`",
            f"- Diagnostic archive: `{payload['diagnostic_archive_size']}`",
            f"- Frozen primary: `{payload['primary_candidate_id']}`",
            f"- Promising / shadow: `{payload['promising_candidates']}` / `{payload['shadow_candidates']}`",
            f"- Topstep path: `{payload['topstep_path_candidates']}`",
            "- Promotion tests: at most `1`",
            "- Q4 access: `0`",
            "- PAPER_SHADOW_READY: `0`",
            "- Outbound orders: `0`",
            "",
            "## Interpretation boundary",
            "",
            str(payload["interpretation_boundary"]),
            "",
        ]
    )
