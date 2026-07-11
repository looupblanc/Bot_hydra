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
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _build_past_only_feature_frame,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
)
from hydra.research.accelerated_context_tournament import _apply_context
from hydra.research.counterfactual_hazard_primary import (
    _one_sided_binomial_probability,
    build_counterfactual_base_events,
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
    _causal_session_thresholds,
    _period_events,
    _period_metrics,
    _prepare_execution_cache,
    _prepare_feature_frame,
    _round_turn_cost_all,
    _validation_events,
    _validation_metrics,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "barrier_hazard_primary_v1"
PRIMARY_ALPHA = 0.03
EARLY_ROUND_1 = ("2023-01-01", "2023-07-01")
EARLY_ROUND_2 = ("2023-07-01", "2024-01-01")
FEATURES = (
    "signed_close_location_persistence_30",
    "range_acceleration_15_120",
    "return_sign_persistence_30",
    "signed_extreme_recovery_60",
)
SIGNED_FEATURES = frozenset(
    {
        "signed_close_location_persistence_30",
        "return_sign_persistence_30",
        "signed_extreme_recovery_60",
    }
)
FEATURE_FAMILIES = {
    "signed_close_location_persistence_30": "accepted_location_hazard",
    "range_acceleration_15_120": "range_expansion_hazard",
    "return_sign_persistence_30": "path_curvature_hazard",
    "signed_extreme_recovery_60": "extreme_recovery_hazard",
}
RECIPES = (
    {
        "name": "open_q65_h15_s075_5m_agree",
        "session": "open",
        "quantile": 0.65,
        "horizon": 15,
        "barrier_scale_multiplier": 0.75,
        "policy_direction": "continuation",
        "activation_context": "completed_5m_trend_agree",
    },
    {
        "name": "open_q75_h30_s100_15m_disagree",
        "session": "open",
        "quantile": 0.75,
        "horizon": 30,
        "barrier_scale_multiplier": 1.00,
        "policy_direction": "reversal",
        "activation_context": "completed_15m_trend_disagree",
    },
    {
        "name": "middle_q65_h30_s100_15m_expansion",
        "session": "middle",
        "quantile": 0.65,
        "horizon": 30,
        "barrier_scale_multiplier": 1.00,
        "policy_direction": "continuation",
        "activation_context": "completed_15m_volatility_expansion",
    },
    {
        "name": "late_q75_h60_s150_30m_agree",
        "session": "late",
        "quantile": 0.75,
        "horizon": 60,
        "barrier_scale_multiplier": 1.50,
        "policy_direction": "continuation",
        "activation_context": "completed_30m_trend_agree",
    },
    {
        "name": "all_q85_h30_s075_5m_disagree",
        "session": "all",
        "quantile": 0.85,
        "horizon": 30,
        "barrier_scale_multiplier": 0.75,
        "policy_direction": "reversal",
        "activation_context": "completed_5m_trend_disagree",
    },
    {
        "name": "all_q65_h60_s125_30m_disagree",
        "session": "all",
        "quantile": 0.65,
        "horizon": 60,
        "barrier_scale_multiplier": 1.25,
        "policy_direction": "reversal",
        "activation_context": "completed_30m_trend_disagree",
    },
)


class BarrierHazardPrimaryError(RuntimeError):
    pass


def generate_barrier_hypotheses() -> list[dict[str, Any]]:
    population: list[dict[str, Any]] = []
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
                    "feature_signed": feature in SIGNED_FEATURES,
                    "mechanism_family": FEATURE_FAMILIES[feature],
                    **recipe,
                }
                fingerprint = structural_fingerprint(specification)
                population.append(
                    {
                        **specification,
                        "candidate_id": (
                            f"strategy_barrier_hazard_{market}_{feature}_{recipe['name']}_v1"
                        ),
                        "lineage_id": f"lineage_barrier_hazard_{fingerprint[:20]}",
                        "structural_fingerprint": fingerprint,
                        "portfolio_role": (
                            "reversal"
                            if recipe["policy_direction"] == "reversal"
                            else "trend"
                        ),
                    }
                )
    population.sort(key=lambda item: item["candidate_id"])
    return population


def add_barrier_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    key = out["contiguous_segment_id"]
    close = out["close"].astype(float)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    grouped_close = close.groupby(key, sort=False)
    returns = grouped_close.pct_change(fill_method=None)

    def past_rolling(
        series: pd.Series, window: int, minimum: int, operation: str
    ) -> pd.Series:
        grouped = series.groupby(key, sort=False).rolling(window, min_periods=minimum)
        value = getattr(grouped, operation)().reset_index(level=0, drop=True).sort_index()
        return value.groupby(key, sort=False).shift(1)

    high_30 = past_rolling(high, 30, 15, "max")
    low_30 = past_rolling(low, 30, 15, "min")
    width_30 = (high_30 - low_30).replace(0, np.nan)
    lagged_close = grouped_close.shift(1)
    location = (lagged_close - low_30) / width_30
    location_sign = np.sign(location - 0.5)
    persistence = past_rolling(location_sign, 30, 15, "mean")
    out["signed_close_location_persistence_30"] = persistence
    true_range = (high - low).abs()
    short_range = past_rolling(true_range, 15, 8, "mean")
    long_range = past_rolling(true_range, 120, 40, "mean")
    out["range_acceleration_15_120"] = short_range / long_range.replace(0, np.nan)
    out["return_sign_persistence_30"] = past_rolling(
        np.sign(returns), 30, 15, "mean"
    )
    high_60 = past_rolling(high, 60, 20, "max")
    low_60 = past_rolling(low, 60, 20, "min")
    width_60 = (high_60 - low_60).replace(0, np.nan)
    location_60 = (lagged_close - low_60) / width_60
    past_return_60 = grouped_close.pct_change(60, fill_method=None).groupby(
        key, sort=False
    ).shift(1)
    out["signed_extreme_recovery_60"] = np.where(
        past_return_60 < 0,
        location_60,
        np.where(past_return_60 > 0, -(1.0 - location_60), np.nan),
    )
    out["barrier_range_scale"] = past_rolling(true_range, 60, 20, "median")
    columns = [*FEATURES, "barrier_range_scale"]
    out[columns] = out[columns].replace([np.inf, -np.inf], np.nan)
    return out


def build_barrier_path_cache(
    frame: pd.DataFrame,
) -> dict[
    tuple[str, str, str, int],
    tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray],
]:
    cache = {}
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    for keys, group in frame.groupby(grouping, sort=False, observed=True):
        ordered = group.sort_values("timestamp")
        cache[(str(keys[0]), str(keys[1]), str(keys[2]), int(keys[3]))] = (
            pd.to_datetime(ordered["timestamp"], utc=True)
            .dt.tz_localize(None)
            .to_numpy(dtype="datetime64[ns]")
            .astype(np.int64),
            ordered["open"].to_numpy(dtype=float),
            ordered["high"].to_numpy(dtype=float),
            ordered["low"].to_numpy(dtype=float),
            ordered["close"].to_numpy(dtype=float),
        )
    return cache


def resolve_barrier_events(
    events: pd.DataFrame,
    path_cache: dict[
        tuple[str, str, str, int],
        tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    ],
    *,
    barrier_scale_multiplier: float,
    entry_delay_bars: int = 0,
) -> pd.DataFrame:
    output = events.copy()
    if output.empty:
        return output.assign(
            barrier_outcome=pd.Series(dtype=str),
            barrier_ambiguous_stop_first=pd.Series(dtype=bool),
            barrier_distance_points=pd.Series(dtype=float),
            mfe_dollars=pd.Series(dtype=float),
        )
    records: list[dict[str, Any]] = []
    for row in output.to_dict("records"):
        key = (
            str(row["symbol"]),
            str(row["active_contract"]),
            str(row["trading_session_id"]),
            int(row["contiguous_segment_id"]),
        )
        timestamps, opens, highs, lows, closes = path_cache[key]
        original_entry = pd.Timestamp(row["entry_timestamp"]).value
        start = int(np.searchsorted(timestamps, original_entry)) + int(entry_delay_bars)
        original_end = int(np.searchsorted(timestamps, pd.Timestamp(row["exit_timestamp"]).value))
        end = min(original_end + int(entry_delay_bars), len(timestamps))
        if start >= end or start >= len(timestamps) or timestamps[start] < original_entry:
            continue
        entry_price = float(opens[start])
        entry_timestamp = pd.Timestamp(int(timestamps[start]), tz="UTC")
        horizon = int(row["holding_horizon_minutes"])
        distance = (
            float(row["barrier_range_scale"])
            * math.sqrt(max(horizon, 1) / 15.0)
            * float(barrier_scale_multiplier)
        )
        if not np.isfinite(distance) or distance <= 0:
            continue
        side = int(row["side"])
        target = entry_price + side * distance
        stop = entry_price - side * distance
        outcome = "TIMEOUT"
        ambiguous = False
        exit_price = float(closes[end - 1])
        exit_timestamp = pd.Timestamp(int(timestamps[end - 1]), tz="UTC")
        favorable_points = 0.0
        adverse_points = 0.0
        for position in range(start, end):
            high = float(highs[position])
            low = float(lows[position])
            open_price = float(opens[position])
            if side > 0:
                target_hit = high >= target
                stop_hit = low <= stop
                favorable_points = max(favorable_points, high - entry_price)
                adverse_points = min(adverse_points, low - entry_price)
            else:
                target_hit = low <= target
                stop_hit = high >= stop
                favorable_points = max(favorable_points, entry_price - low)
                adverse_points = min(adverse_points, entry_price - high)
            if stop_hit:
                outcome = "STOP_FIRST"
                ambiguous = bool(target_hit)
                exit_price = (
                    min(stop, open_price) if side > 0 else max(stop, open_price)
                )
                exit_timestamp = pd.Timestamp(int(timestamps[position]), tz="UTC")
                break
            if target_hit:
                outcome = "TARGET_FIRST"
                exit_price = target
                exit_timestamp = pd.Timestamp(int(timestamps[position]), tz="UTC")
                break
        point_value = float(row["point_value"])
        row.update(
            {
                "entry_timestamp": entry_timestamp,
                "entry_price": entry_price,
                "exit_timestamp": exit_timestamp,
                "exit_price": exit_price,
                "gross_pnl": side * (exit_price - entry_price) * point_value,
                "net_pnl": side * (exit_price - entry_price) * point_value
                - float(row["cost"]),
                "barrier_outcome": outcome,
                "barrier_ambiguous_stop_first": ambiguous,
                "barrier_distance_points": distance,
                "target_price": target,
                "stop_price": stop,
                "mfe_dollars": favorable_points * point_value,
                "mae_dollars": adverse_points * point_value - float(row["cost"]) / 2,
                "entry_delay_bars": int(entry_delay_bars),
            }
        )
        records.append(row)
    return pd.DataFrame(records) if records else output.iloc[0:0].copy()


def barrier_hazard_metrics(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "events": 0,
            "resolved_barrier_events": 0,
            "target_first": 0,
            "stop_first": 0,
            "ambiguous_stop_first": 0,
            "timeouts": 0,
            "target_first_probability": 0.0,
            "exact_probability": 1.0,
            "resolution_rate": 0.0,
        }
    outcomes = events["barrier_outcome"].astype(str)
    target = int(outcomes.eq("TARGET_FIRST").sum())
    stop = int(outcomes.eq("STOP_FIRST").sum())
    resolved = target + stop
    return {
        "events": int(len(events)),
        "resolved_barrier_events": resolved,
        "target_first": target,
        "stop_first": stop,
        "ambiguous_stop_first": int(
            events["barrier_ambiguous_stop_first"].astype(bool).sum()
        ),
        "timeouts": int(outcomes.eq("TIMEOUT").sum()),
        "target_first_probability": float(target / resolved) if resolved else 0.0,
        "exact_probability": _one_sided_binomial_probability(target, resolved),
        "resolution_rate": float(resolved / len(events)),
    }


def run_barrier_hazard_primary(
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
        raise BarrierHazardPrimaryError("Explicit-contract map changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise BarrierHazardPrimaryError(
                "Worker commit differs from queued specification."
            )
    hypotheses = generate_barrier_hypotheses()
    if len(hypotheses) != 144 or len(
        {item["structural_fingerprint"] for item in hypotheses}
    ) != 144:
        raise BarrierHazardPrimaryError("Frozen barrier population drifted.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = _preregistration(
        hypotheses,
        engineering_task_sha256=engineering_task_sha256,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        code_commit=code_commit,
    )
    preregistration_path = destination / "barrier_hazard_preregistration.json"
    population_path = destination / "barrier_hazard_population.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    _write_immutable(
        population_path,
        json.dumps(
            {
                "schema": "barrier_hazard_population_v1",
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
    features = add_barrier_features(
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
        raise BarrierHazardPrimaryError("Early selection exposed 2024.")
    preparation_seconds = time.perf_counter() - started

    early_context_cache: dict[tuple[str, int], pd.DataFrame] = {}
    early_path_caches = {
        market: build_barrier_path_cache(early_frames[market])
        for market in SYMBOLS
    }
    early_events = _build_barrier_event_sets(
        early_frames,
        hypotheses,
        early_context_cache,
        early_path_caches,
    )
    round1_survivors: list[dict[str, Any]] = []
    dispositions: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        events = _period_events(
            early_events[hypothesis["candidate_id"]], *EARLY_ROUND_1
        )
        direct = _period_metrics(events)
        hazard = barrier_hazard_metrics(events)
        passed = bool(
            direct["events"] >= 8
            and direct["net_pnl"] > 0
            and direct["cost_stress_1_5x_net"] > 0
            and hazard["resolved_barrier_events"] >= 5
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
        events = _period_events(
            early_events[hypothesis["candidate_id"]], *EARLY_ROUND_2
        )
        direct = _period_metrics(events)
        hazard = barrier_hazard_metrics(events)
        passed = bool(
            direct["events"] >= 10
            and direct["net_pnl"] > 0
            and direct["cost_stress_1_5x_net"] > 0
            and direct["best_positive_event_share"] <= 0.40
            and hazard["resolved_barrier_events"] >= 8
            and hazard["target_first_probability"] > 0.50
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
    early_micro_events = _build_barrier_event_sets(
        early_frames,
        micro_hypotheses,
        early_context_cache,
        early_path_caches,
    )
    archive, ranking = _build_archive_and_primary_ranking(
        round2_survivors, early_micro_events
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
    archive_path = destination / "barrier_hazard_archive_manifest.json"
    _write_immutable(
        archive_path, json.dumps(archive_manifest, indent=2, sort_keys=True) + "\n"
    )
    primary_manifest = _primary_manifest(
        primary=primary,
        ranking=ranking,
        archive_hash=archive_manifest["archive_manifest_hash"],
        population_hash=preregistration["population_hash"],
    )
    primary_manifest_path = destination / "barrier_hazard_primary_freeze.json"
    _write_immutable(
        primary_manifest_path,
        json.dumps(primary_manifest, indent=2, sort_keys=True) + "\n",
    )
    freeze_seconds = time.perf_counter() - started

    candidates: list[dict[str, Any]] = []
    trade_ledger = pd.DataFrame()
    shadow_configurations: list[dict[str, Any]] = []
    if primary is not None:
        full_context_cache: dict[tuple[str, int], pd.DataFrame] = {}
        needed_markets = {str(primary["market"]), str(primary["execution_market"])}
        full_path_caches = {
            market: build_barrier_path_cache(full_frames[market])
            for market in needed_markets
        }
        mini_events = _build_barrier_event_sets(
            full_frames,
            [primary],
            full_context_cache,
            full_path_caches,
        )[primary["candidate_id"]]
        validation_mini = _validation_events(mini_events)
        micro_hypothesis = {
            **primary,
            "market": primary["execution_market"],
            "candidate_id": f"{primary['candidate_id']}__micro",
        }
        micro_events = _build_barrier_event_sets(
            full_frames,
            [micro_hypothesis],
            full_context_cache,
            full_path_caches,
        )[micro_hypothesis["candidate_id"]]
        validation_micro = _validation_events(micro_events)
        mini_metrics = _validation_metrics(validation_mini)
        micro_metrics = _validation_metrics(validation_micro)
        hazard = barrier_hazard_metrics(validation_mini)
        micro_hazard = barrier_hazard_metrics(validation_micro)
        parameter = _parameter_diagnostics(
            full_frames, primary, full_context_cache, full_path_caches
        )
        delayed_events = _build_barrier_event_sets(
            full_frames,
            [primary],
            full_context_cache,
            full_path_caches,
            entry_delay_bars=1,
        )[primary["candidate_id"]]
        delay_metrics = _validation_metrics(_validation_events(delayed_events))
        contract_evidence = bool(
            mini_metrics["net_pnl"] > 0
            and micro_metrics["net_pnl"] > 0
            and micro_metrics["supportive_temporal_folds"] >= 1
            and micro_hazard["target_first_probability"] >= 0.50
        )
        account = _account_replay(
            validation_micro.rename(columns={"net_pnl": "net_pnl_60"}).copy()
        )
        promotion_null_pass = bool(
            hazard["resolved_barrier_events"] >= 12
            and hazard["target_first_probability"] > 0.50
            and hazard["exact_probability"] <= PRIMARY_ALPHA
        )
        shadow_null_support = bool(
            hazard["resolved_barrier_events"] >= 12
            and hazard["target_first_probability"] > 0.50
            and hazard["exact_probability"] <= 0.20
        )
        evidence = ShadowEvidence(
            candidate_id=primary["candidate_id"],
            data_integrity=True,
            no_lookahead=True,
            deterministic_signals=True,
            net_after_costs=float(mini_metrics["net_pnl"]),
            supportive_temporal_folds=int(mini_metrics["supportive_temporal_folds"]),
            catastrophic_transfer=bool(mini_metrics["catastrophic_transfer"]),
            candidate_null_pass=shadow_null_support,
            null_probability=float(hazard["exact_probability"]),
            parameter_stable=bool(parameter["supportive_neighbor_count"] >= 1),
            contract_evidence=contract_evidence,
            account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
            execution_possible=True,
            realtime_features_available=True,
            shadow_spec_complete=True,
            observability_complete=True,
            untouched_holdout_passed=False,
            sample_size=int(mini_metrics["events"]),
            uncertainty="barrier_hazard_development_confirmation_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise BarrierHazardPrimaryError(
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
            "barrier_hazard": hazard,
            "micro_barrier_hazard": micro_hazard,
            "null_evidence": {
                "method": "preselected_primary_exact_target_before_stop_binomial",
                "null_probability": 0.50,
                "raw_probability": float(hazard["exact_probability"]),
                "prospective_alpha": PRIMARY_ALPHA,
                "promotion_test_count": 1,
                "promotion_passed": promotion_null_pass,
                "shadow_research_support_threshold": 0.20,
                "shadow_research_support_passed": shadow_null_support,
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
                "one_additional_bar_delay_net": float(delay_metrics["net_pnl"]),
                "same_bar_ambiguity_stop_first": True,
                "gap_stop_worse_fill": True,
                "completed_higher_timeframe_only": True,
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
                destination / "shadow_configurations" / f"{primary['candidate_id']}.json"
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
                validation_mini.assign(contract_role="primary_mini"),
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
    trade_path = destination / "barrier_hazard_trade_ledger.jsonl"
    _write_dataframe_ledger(
        trade_path, trade_ledger, ["entry_timestamp", "symbol", "contract_role"]
    )
    integrity = _integrity(
        hypotheses=hypotheses,
        early_frames=early_frames,
        primary=primary,
        primary_manifest=primary_manifest,
        archive=archive,
        trade_ledger=trade_ledger,
        latest_source_timestamp=latest_source_timestamp,
    )
    if not all(integrity.values()):
        raise BarrierHazardPrimaryError(f"Integrity proof failed: {integrity}")
    if shadow_count:
        conclusion = "BARRIER_HAZARD_SHADOW_CANDIDATE_FOUND"
        next_action = "ACTIVATE_BARRIER_SHADOW_AND_REPLICATE"
    elif promising:
        conclusion = "BARRIER_HAZARD_PROMISING_BUT_INSUFFICIENT"
        next_action = "FRESH_ID_REPLICATION_OR_FORWARD_SHADOW_RESEARCH"
    elif primary is None:
        conclusion = "BARRIER_HAZARD_NO_EARLY_PRIMARY"
        next_action = "PIVOT_MARKET_ECOLOGY_OR_DEFENSIVE_HAZARD"
    else:
        conclusion = "BARRIER_HAZARD_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
        next_action = "KILL_EXACT_PRIMARY_AND_PIVOT_PORTFOLIO_ROLE"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Exactly one primary at most was frozen using 2023 before unchanged 2024 "
            "confirmation. Same-bar ambiguity is stop-first. Q4 remained sealed and "
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
    result_path = destination / "barrier_hazard_result.json"
    report_path = destination / "barrier_hazard_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "primary_manifest_path": str(primary_manifest_path),
            "archive_manifest_path": str(archive_path),
            "trade_ledger_path": str(trade_path),
        },
        "report_path": str(report_path),
    }


def _build_barrier_event_sets(
    frames: dict[str, pd.DataFrame],
    hypotheses: list[dict[str, Any]],
    context_cache: dict[tuple[str, int], pd.DataFrame],
    path_caches: dict[
        str,
        dict[
            tuple[str, str, str, int],
            tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray],
        ],
    ],
    *,
    entry_delay_bars: int = 0,
) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
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
            causal = _causal_session_thresholds(
                frame[feature], frame["trading_session_id"], quantiles
            )
            thresholds.update(
                {(feature, quantile): causal[quantile] for quantile in quantiles}
            )
        scale_lookup = frame.set_index(
            ["contiguous_segment_id", "symbol_position"]
        )["barrier_range_scale"]
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
                base = build_counterfactual_base_events(
                    frame,
                    hypothesis,
                    threshold_override=thresholds[
                        (str(hypothesis["feature"]), float(hypothesis["quantile"]))
                    ],
                )
                if not base.empty:
                    lookup_index = pd.MultiIndex.from_arrays(
                        [
                            base["contiguous_segment_id"].astype(int),
                            base["symbol_position"].astype(int),
                        ]
                    )
                    base["barrier_range_scale"] = scale_lookup.reindex(
                        lookup_index
                    ).to_numpy(dtype=float)
                    base = base.dropna(subset=["barrier_range_scale"]).reset_index(
                        drop=True
                    )
                base_cache[key] = base
            treated = _apply_context(
                base_cache[key],
                frame,
                str(hypothesis["activation_context"]),
                context_cache,
            )
            output[str(hypothesis["candidate_id"])] = resolve_barrier_events(
                treated,
                path_caches[market],
                barrier_scale_multiplier=float(
                    hypothesis["barrier_scale_multiplier"]
                ),
                entry_delay_bars=entry_delay_bars,
            )
    return output


def _early_failure_reason(
    direct: dict[str, Any], hazard: dict[str, Any], *, round_number: int
) -> str:
    minimum_events = 8 if round_number == 1 else 10
    minimum_resolved = 5 if round_number == 1 else 8
    if (
        direct["events"] < minimum_events
        or hazard["resolved_barrier_events"] < minimum_resolved
    ):
        return "INSUFFICIENT_EVENTS_OR_RESOLVED_BARRIERS"
    if direct["net_pnl"] <= 0:
        return "NEGATIVE_ECONOMICS"
    if direct["cost_stress_1_5x_net"] <= 0:
        return "COST_FRAGILITY"
    if round_number == 2 and direct["best_positive_event_share"] > 0.40:
        return "CONCENTRATION"
    if round_number == 2 and hazard["target_first_probability"] <= 0.50:
        return "NO_TARGET_BEFORE_STOP_HAZARD"
    return "UNSPECIFIED_FAILURE"


def _build_archive_and_primary_ranking(
    survivors: list[dict[str, Any]],
    early_micro_events: dict[str, pd.DataFrame],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scored = []
    for item in survivors:
        direct = item["round2_direct"]
        hazard = item["round2_hazard"]
        score = (
            float(direct["net_pnl"])
            / max(float(direct["maximum_drawdown"]), 1.0)
            + 4.0 * (float(hazard["target_first_probability"]) - 0.50)
            + float(hazard["resolution_rate"])
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
        occupied.add(niche)
        archive.append(item)
        if len(archive) >= 12:
            break
    ranking: list[dict[str, Any]] = []
    for item in archive:
        micro = _period_events(
            early_micro_events[f"{item['candidate_id']}__early_micro"],
            *EARLY_ROUND_2,
        )
        micro_direct = _period_metrics(micro)
        micro_hazard = barrier_hazard_metrics(micro)
        direct = item["round2_direct"]
        hazard = item["round2_hazard"]
        eligible = bool(
            direct["net_pnl"] > 0
            and direct["cost_stress_1_5x_net"] > 0
            and micro_direct["net_pnl"] > 0
            and micro_direct["cost_stress_1_5x_net"] > 0
            and hazard["target_first_probability"] > 0.50
            and micro_hazard["target_first_probability"] >= 0.50
        )
        ranking.append(
            {
                "candidate_id": item["candidate_id"],
                "eligible": eligible,
                "minimum_target_first_probability": min(
                    float(hazard["target_first_probability"]),
                    float(micro_hazard["target_first_probability"]),
                ),
                "minimum_mini_micro_net_to_drawdown": min(
                    float(direct["net_pnl"])
                    / max(float(direct["maximum_drawdown"]), 1.0),
                    float(micro_direct["net_pnl"])
                    / max(float(micro_direct["maximum_drawdown"]), 1.0),
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
            -float(row["minimum_target_first_probability"]),
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
        "schema": "barrier_hazard_archive_manifest_v1",
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
        "schema": "barrier_hazard_primary_freeze_v1",
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
                    "barrier_scale_multiplier",
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
        "hazard_null_probability": 0.50,
        "same_bar_policy": "STOP_FIRST",
        "stop_gap_policy": "WORSE_OF_STOP_LEVEL_AND_BAR_OPEN",
        "target_gap_policy": "NO_PRICE_IMPROVEMENT_BEYOND_TARGET",
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
    path_caches: dict[
        str,
        dict[
            tuple[str, str, str, int],
            tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray],
        ],
    ],
) -> dict[str, Any]:
    variants: dict[str, Any] = {}
    for delta in (-0.25, 0.25):
        scale = max(float(primary["barrier_scale_multiplier"]) + delta, 0.50)
        label = f"barrier_scale_{int(round(scale * 100))}"
        variant = {
            **primary,
            "candidate_id": f"{primary['candidate_id']}__diagnostic_{label}",
            "barrier_scale_multiplier": scale,
        }
        events = _build_barrier_event_sets(
            frames, [variant], context_cache, path_caches
        )[variant["candidate_id"]]
        validation = _validation_events(events)
        variants[label] = {
            "direct": _validation_metrics(validation),
            "hazard": barrier_hazard_metrics(validation),
        }
    return {
        "diagnostic_only": True,
        "variants": variants,
        "supportive_neighbor_count": int(
            sum(
                row["direct"]["net_pnl"] > 0
                and row["hazard"]["target_first_probability"] > 0.50
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
        strategy_version="v1_barrier_hazard_pre_holdout",
        feature_versions=(
            "past_only_barrier_path_features_v1",
            "closed_bar_context_v1",
            "conservative_symmetric_barrier_execution_v1",
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
            "event": "past_only_barrier_state_threshold_crossing",
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
            "symmetric_target_stop": True,
            "past_range_scale_multiplier": primary["barrier_scale_multiplier"],
            "maximum_holding_completed_1m_bars": int(primary["horizon"]),
            "same_bar_policy": "stop_first",
            "stop_gap_policy": "worse_of_stop_and_open",
            "target_gap_policy": "target_level_no_improvement",
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
            "barrier_resolution": True,
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
        "schema": "barrier_hazard_preregistration_v1",
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
        "hazard_target": "symmetric_target_before_stop",
        "hazard_null_probability": 0.50,
        "same_bar_policy": "STOP_FIRST",
        "stop_gap_policy": "WORSE_OF_STOP_LEVEL_AND_BAR_OPEN",
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
    trade_ledger: pd.DataFrame,
    latest_source_timestamp: pd.Timestamp,
) -> dict[str, bool]:
    early_only = all(
        frame.empty
        or pd.to_datetime(frame["timestamp"], utc=True).max()
        < pd.Timestamp("2024-01-01", tz="UTC")
        for frame in early_frames.values()
    )
    timing = True
    context_timing = True
    contracts = True
    ambiguity = True
    finite = True
    if not trade_ledger.empty:
        timing = bool(
            (
                trade_ledger["decision_timestamp"]
                == trade_ledger["timestamp"] + pd.Timedelta(minutes=1)
            ).all()
            and (trade_ledger["entry_timestamp"] >= trade_ledger["decision_timestamp"]).all()
            and (trade_ledger["exit_timestamp"] >= trade_ledger["entry_timestamp"]).all()
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
        ambiguous_rows = trade_ledger["barrier_ambiguous_stop_first"].astype(bool)
        ambiguity = bool(
            trade_ledger.loc[ambiguous_rows, "barrier_outcome"]
            .astype(str)
            .eq("STOP_FIRST")
            .all()
        )
        finite = bool(
            np.isfinite(trade_ledger["net_pnl"].to_numpy(dtype=float)).all()
            and np.isfinite(
                trade_ledger["barrier_distance_points"].to_numpy(dtype=float)
            ).all()
        )
    return {
        "exact_unique_population": len(hypotheses) == 144
        and len({item["structural_fingerprint"] for item in hypotheses}) == 144,
        "balanced_markets": all(
            sum(item["market"] == market for item in hypotheses) == 24
            for market in MARKET_PAIRS
        ),
        "new_barrier_features_only": set(item["feature"] for item in hypotheses)
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
        "one_bar_or_later_entry": timing,
        "completed_higher_timeframe_only": context_timing,
        "explicit_contracts": contracts,
        "ambiguous_bar_is_stop_first": ambiguity,
        "finite_barrier_paths": finite,
        "q4_excluded": bool(
            latest_source_timestamp < pd.Timestamp("2024-10-01", tz="UTC")
        ),
        "paper_promotion_disabled": True,
        "no_outbound_order_capability": True,
    }


def _record_access_once(hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_ids = [item["candidate_id"] for item in hypotheses]
    period = "2023-01-01:2024-10-01"
    reason = "barrier-hazard single-primary tournament; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.barrier_hazard_primary"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.barrier_hazard_primary",
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
        raise BarrierHazardPrimaryError(f"Frozen {label} missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Barrier-Hazard Primary v1",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Structural prototypes: `{payload['structural_prototypes']}`",
            f"- Round-1 / Round-2 survivors: `{payload['round1_survivors']}` / `{payload['round2_survivors']}`",
            f"- Diagnostic archive: `{payload['diagnostic_archive_size']}`",
            f"- Frozen primary: `{payload['primary_candidate_id']}`",
            f"- Promising / shadow: `{payload['promising_candidates']}` / `{payload['shadow_candidates']}`",
            f"- Topstep path: `{payload['topstep_path_candidates']}`",
            "- Same-bar ambiguity: `STOP_FIRST`",
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
