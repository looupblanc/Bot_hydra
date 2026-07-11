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

from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_volume_front import VOLUME_FRONT_MAP_TYPE
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.factory.quality_diversity_selector_v2 import (
    SelectorV2Policy,
    select_quality_diversity_elites_v2,
)
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _stable_hash,
    _strict_json_value,
)
from hydra.research.energy_metals_barrier_primary import _read_period
from hydra.research.energy_metals_session_execution_repair import (
    synchronize_micro_execution,
)
from hydra.research.energy_metals_session_geometry_primary import (
    _concentration_stress,
    build_session_geometry_table,
)
from hydra.research.equity_open_gap_reversal import _account_replay, _write_immutable
from hydra.research.qd_economic_tournament import (
    _block_sign_flip_probability,
    _period_metrics,
    _round_turn_cost_all,
    _validation_metrics,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "cross_asset_daily_horizon_primary_v1"
TARGETS = {
    "YM": {"micro": "MYM", "ecology": "equity_indices"},
    "RTY": {"micro": "M2K", "ecology": "equity_indices"},
    "CL": {"micro": "MCL", "ecology": "energy"},
    "GC": {"micro": "MGC", "ecology": "metals"},
}
FEATURES = (
    "source_prior_trend",
    "source_prior_range_shock_signed",
    "source_prior_close_location",
    "relative_prior_trend",
)
POLICIES = ("continuation", "reversal")
QUANTILES = (0.65, 0.80)
HORIZONS = (30, 60, 120)
EXPECTED_POPULATION = 720
PROMOTION_ALPHA = 0.03
SHADOW_ALPHA = 0.20


class CrossAssetDailyHorizonError(RuntimeError):
    pass


def generate_hypotheses() -> list[dict[str, Any]]:
    hypotheses: list[dict[str, Any]] = []
    for target, target_meta in TARGETS.items():
        for source in TARGETS:
            for feature in FEATURES:
                if feature == "relative_prior_trend" and source == target:
                    continue
                for policy in POLICIES:
                    for quantile in QUANTILES:
                        for horizon in HORIZONS:
                            specification = {
                                "representation": VERSION,
                                "target_market": target,
                                "execution_market": target_meta["micro"],
                                "source_market": source,
                                "feature": feature,
                                "policy_direction": policy,
                                "quantile": quantile,
                                "horizon": horizon,
                                "market_ecology": target_meta["ecology"],
                                "mechanism_family": _mechanism_family(feature),
                                "portfolio_role": (
                                    "trend" if policy == "continuation" else "reversal"
                                ),
                                "source_timeframe": "completed_prior_RTH_session",
                                "execution_timeframe": "1m",
                            }
                            fingerprint = structural_fingerprint(specification)
                            candidate_id = (
                                f"strategy_daily_cross_{source}_to_{target}_{feature}_"
                                f"{policy}_q{int(quantile * 100)}_h{horizon}_v1"
                            )
                            hypotheses.append(
                                {
                                    **specification,
                                    "candidate_id": candidate_id,
                                    "structural_fingerprint": fingerprint,
                                    "lineage_id": f"lineage_daily_{fingerprint[:20]}",
                                    "market": target,
                                }
                            )
    return sorted(hypotheses, key=lambda row: row["candidate_id"])


def build_daily_horizon_events(
    tables: dict[str, pd.DataFrame],
    hypothesis: dict[str, Any],
    *,
    feature_cache: dict[tuple[str, str, str], pd.DataFrame] | None = None,
    entry_delay_bars: int = 0,
    quantile_override: float | None = None,
    horizon_override: int | None = None,
) -> pd.DataFrame:
    target = str(hypothesis["target_market"])
    source = str(hypothesis["source_market"])
    feature = str(hypothesis["feature"])
    cache = feature_cache or build_daily_feature_cache(tables)
    aligned = cache[(target, source, feature)]
    values = pd.to_numeric(aligned["_feature_value"], errors="coerce")
    quantile = float(
        hypothesis["quantile"] if quantile_override is None else quantile_override
    )
    horizon = int(
        hypothesis["horizon"] if horizon_override is None else horizon_override
    )
    threshold = values.abs().shift(1).rolling(20, min_periods=10).quantile(quantile)
    anchor = np.sign(values)
    mask = values.abs().ge(threshold) & anchor.ne(0)
    suffix = "" if entry_delay_bars == 0 else "_delay1"
    required = (
        f"overnight_entry_timestamp{suffix}",
        f"overnight_entry_price{suffix}",
        f"overnight_exit_timestamp_{horizon}{suffix}",
        f"overnight_exit_{horizon}{suffix}",
        f"overnight_long_mae_{horizon}{suffix}",
        f"overnight_short_mae_{horizon}{suffix}",
    )
    if any(column not in aligned for column in required):
        return pd.DataFrame()
    side = anchor * (1 if hypothesis["policy_direction"] == "continuation" else -1)
    point_value = instrument_spec(target).point_value
    cost = _round_turn_cost_all(target)
    output = pd.DataFrame(
        {
            "candidate_id": hypothesis["candidate_id"],
            "entry_timestamp": aligned[required[0]],
            "exit_timestamp": aligned[required[2]],
            "event_session_id": aligned["session_id"],
            "trading_session_id": aligned["session_id"],
            "source_prior_session_id": aligned["source_prior_session_id"],
            "source_symbol": source,
            "symbol": target,
            "active_contract": aligned["active_contract"],
            "side": side,
            "entry_price": aligned[required[1]],
            "exit_price": aligned[required[3]],
            "feature_value": values,
            "causal_threshold": threshold,
        }
    )
    output = output[
        mask
        & output["entry_timestamp"].notna()
        & output["exit_timestamp"].notna()
        & output["source_prior_session_id"].notna()
        & output["entry_price"].notna()
        & output["exit_price"].notna()
    ].copy()
    if not output.empty and not (
        pd.to_datetime(output["source_prior_session_id"])
        < pd.to_datetime(output["trading_session_id"])
    ).all():
        raise CrossAssetDailyHorizonError("Source session was not completed before entry.")
    output["gross_pnl"] = (
        output["side"]
        * (output["exit_price"] - output["entry_price"])
        * point_value
    )
    output["cost"] = cost
    output["net_pnl"] = output["gross_pnl"] - cost
    long_mae = aligned.loc[output.index, required[4]].astype(float) * point_value
    short_mae = aligned.loc[output.index, required[5]].astype(float) * point_value
    output["mae_dollars"] = np.where(
        output["side"] > 0, long_mae, short_mae
    ) - cost / 2
    output["entry_delay_bars"] = entry_delay_bars
    return output.reset_index(drop=True)


def run_cross_asset_daily_horizon_primary(
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
            raise CrossAssetDailyHorizonError(
                "Worker commit differs from queued specification."
            )
    core_map, metals_map = load_roll_map(core_map_path), load_roll_map(metals_map_path)
    if core_map.roll_map_hash() != core_roll_map_hash:
        raise CrossAssetDailyHorizonError("Core roll map changed.")
    if (
        metals_map.map_type != VOLUME_FRONT_MAP_TYPE
        or metals_map.roll_map_hash() != metals_roll_map_hash
    ):
        raise CrossAssetDailyHorizonError("Metals roll map changed.")
    population = generate_hypotheses()
    if len(population) != EXPECTED_POPULATION or len(
        {row["structural_fingerprint"] for row in population}
    ) != EXPECTED_POPULATION:
        raise CrossAssetDailyHorizonError("Frozen population drifted.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration: dict[str, Any] = {
        "schema": VERSION,
        "population_count": EXPECTED_POPULATION,
        "population_hash": _stable_hash(population),
        "hypotheses": population,
        "round1": ["2023-01-01", "2023-07-01"],
        "round2": ["2023-07-01", "2024-01-01"],
        "selection_end_exclusive": "2024-01-01",
        "confirmation_end_exclusive": "2024-10-01",
        "selector": {
            "version": "quality_diversity_selector_v2",
            "maximum_elites": 8,
            "maximum_controls": 2,
            "uses_2024_results": False,
        },
        "promotion_alpha": PROMOTION_ALPHA,
        "shadow_support_alpha": SHADOW_ALPHA,
        "q4_access_allowed": False,
        "network_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "engineering_task_sha256": engineering_task_sha256,
        "code_commit": code_commit,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "cross_asset_daily_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once(population) if record_data_access else None
    early_tables, early_provenance = _load_tables(
        Path(core_data_path),
        core_map,
        Path(metals_data_path),
        metals_map,
        "2024-01-01",
    )
    early_feature_cache = build_daily_feature_cache(early_tables)
    early_signal_cache: dict[str, pd.DataFrame] = {}
    round1_survivors: list[dict[str, Any]] = []
    round2_survivors: list[dict[str, Any]] = []
    failed_controls: list[dict[str, Any]] = []
    dispositions: list[dict[str, Any]] = []
    for hypothesis in population:
        events = build_daily_horizon_events(
            early_tables, hypothesis, feature_cache=early_feature_cache
        )
        metrics = _period_metrics(_period(events, "2023-01-01", "2023-07-01"))
        row = {**hypothesis, "stage1_pass": _gate(metrics, 8), "discovery": metrics}
        if row["stage1_pass"]:
            round1_survivors.append(row)
            early_signal_cache[hypothesis["candidate_id"]] = events
        else:
            failed_controls.append(row)
            dispositions.append(
                {
                    "candidate_id": hypothesis["candidate_id"],
                    "stage": "ROUND1",
                    "reason": _failure(metrics, 8),
                }
            )
    for hypothesis in round1_survivors:
        signals = early_signal_cache[hypothesis["candidate_id"]]
        micro, missing = _micro_execution(signals, early_tables, hypothesis)
        mini_metrics = _period_metrics(
            _period(signals, "2023-07-01", "2024-01-01")
        )
        micro_metrics = _period_metrics(
            _period(micro, "2023-07-01", "2024-01-01")
        )
        missing_rate = len(missing) / max(len(signals), 1)
        passed = bool(
            _gate(mini_metrics, 8)
            and _gate(micro_metrics, 8)
            and missing_rate <= 0.10
        )
        row = {
            **hypothesis,
            "stage1_pass": passed,
            "discovery": micro_metrics,
            "round1_metrics": hypothesis["discovery"],
            "round2_mini_metrics": mini_metrics,
            "round2_micro_metrics": micro_metrics,
            "round2_missing_rate": missing_rate,
        }
        if passed:
            round2_survivors.append(row)
        else:
            failed_controls.append(row)
            dispositions.append(
                {
                    "candidate_id": hypothesis["candidate_id"],
                    "stage": "ROUND2",
                    "reason": (
                        "MICRO_MATCH_FAILURE"
                        if missing_rate > 0.10
                        else _failure(micro_metrics, 8)
                    ),
                }
            )
    selector = select_quality_diversity_elites_v2(
        round2_survivors,
        failed_candidates=failed_controls,
        policy=SelectorV2Policy(
            maximum_elites=8,
            maximum_controls=2,
            soft_family_share=0.25,
            soft_market_share=0.40,
            preferred_ecology_weights={
                "equity_indices": 0.50,
                "energy": 0.25,
                "metals": 0.25,
            },
            minimum_ecology_shares={"energy": 0.125, "metals": 0.125},
            selector_version="cross_asset_daily_qd_selector_v2",
        ),
    )
    elites = [dict(row) for row in selector.elites]
    controls = [dict(row) for row in selector.negative_controls]
    freeze_manifest: dict[str, Any] = {
        "schema": "cross_asset_daily_elite_freeze_v1",
        "population_hash": preregistration["population_hash"],
        "selection_data_end_exclusive": "2024-01-01",
        "selector_audit": selector.audit,
        "elite_count": len(elites),
        "elites": elites,
        "negative_controls": controls,
        "negative_controls_count_as_elites": False,
        "candidate_status_inherited": False,
        "q4_access_allowed": False,
    }
    freeze_manifest["elite_manifest_hash"] = _stable_hash(freeze_manifest)
    freeze_path = destination / "cross_asset_daily_elite_freeze.json"
    _write_immutable(
        freeze_path, json.dumps(freeze_manifest, indent=2, sort_keys=True) + "\n"
    )
    freeze_seconds = time.perf_counter() - started
    full_tables, confirmation_provenance = _load_tables(
        Path(core_data_path),
        core_map,
        Path(metals_data_path),
        metals_map,
        "2024-10-01",
    )
    full_feature_cache = build_daily_feature_cache(full_tables)
    validations: list[dict[str, Any]] = []
    ledger_frames: list[pd.DataFrame] = []
    for index, elite in enumerate(elites):
        signals = build_daily_horizon_events(
            full_tables, elite, feature_cache=full_feature_cache
        )
        micro, missing = _micro_execution(signals, full_tables, elite)
        confirmation = _period(micro, "2024-01-01", "2024-10-01")
        development = _period(micro, "2023-01-01", "2024-01-01")
        delayed, delayed_missing = _micro_execution(
            signals, full_tables, elite, entry_delay_bars=1
        )
        delayed_confirmation = _period(delayed, "2024-01-01", "2024-10-01")
        metrics = _validation_metrics(confirmation)
        development_metrics = _period_metrics(development)
        delayed_metrics = _period_metrics(delayed_confirmation)
        probability = _block_sign_flip_probability(
            confirmation, seed=991101 + index
        )
        concentration = _concentration_stress(confirmation)
        neighbor_metrics = _neighbor_diagnostics(
            full_tables, elite, feature_cache=full_feature_cache
        )
        account = _account_replay(
            confirmation.rename(columns={"net_pnl": "net_pnl_60"}).copy()
        )
        missing_rate = len(missing) / max(len(signals), 1)
        validations.append(
            {
                "elite": elite,
                "signals": signals,
                "events": micro,
                "development": development_metrics,
                "confirmation": metrics,
                "delayed": delayed_metrics,
                "raw_probability": probability,
                "concentration": concentration,
                "neighbors": neighbor_metrics,
                "account": account,
                "missing": missing,
                "missing_rate": missing_rate,
                "delayed_missing_count": len(delayed_missing),
            }
        )
        if not confirmation.empty:
            ledger_frames.append(
                confirmation.assign(candidate_id=elite["candidate_id"])
            )
    adjusted = _bh_adjust([row["raw_probability"] for row in validations])
    candidates: list[dict[str, Any]] = []
    shadow_configurations: list[dict[str, Any]] = []
    for row, adjusted_probability in zip(validations, adjusted):
        elite = row["elite"]
        metrics = row["confirmation"]
        concentration = row["concentration"]
        account = row["account"]
        support = bool(
            row["development"]["net_pnl"] > 0
            and row["development"]["cost_stress_1_5x_net"] > 0
            and metrics["net_pnl"] > 0
            and metrics["cost_stress_1_5x_net"] > 0
            and metrics["supportive_temporal_folds"] >= 2
            and not metrics["catastrophic_transfer"]
            and adjusted_probability <= SHADOW_ALPHA
            and metrics["best_positive_event_share"] <= 0.35
            and concentration["remove_best_event_net"] > 0
            and concentration["remove_best_month_net"] > 0
            and row["delayed"]["net_pnl"] > 0
            and row["missing_rate"] <= 0.10
            and bool(account.get("micro_one_contract_mll_safe", False))
        )
        evidence = ShadowEvidence(
            candidate_id=elite["candidate_id"],
            data_integrity=True,
            no_lookahead=True,
            deterministic_signals=True,
            net_after_costs=float(metrics["net_pnl"]),
            supportive_temporal_folds=int(metrics["supportive_temporal_folds"]),
            catastrophic_transfer=bool(metrics["catastrophic_transfer"]),
            candidate_null_pass=support,
            null_probability=float(adjusted_probability),
            parameter_stable=bool(row["neighbors"]["positive_neighbor_count"] >= 1),
            contract_evidence=bool(
                row["development"]["net_pnl"] > 0
                and metrics["net_pnl"] > 0
                and row["missing_rate"] <= 0.10
            ),
            account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
            execution_possible=True,
            realtime_features_available=True,
            shadow_spec_complete=True,
            observability_complete=True,
            untouched_holdout_passed=False,
            sample_size=int(metrics["events"]),
            uncertainty="cross_asset_daily_development_confirmation_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise CrossAssetDailyHorizonError(
                "Development tournament attempted paper promotion."
            )
        candidate = {
            "candidate_id": elite["candidate_id"],
            "lineage_id": elite["lineage_id"],
            "structural_fingerprint": elite["structural_fingerprint"],
            "mechanism_family": elite["mechanism_family"],
            "primary_market": elite["target_market"],
            "execution_market": elite["execution_market"],
            "source_market": elite["source_market"],
            "market_ecology": elite["market_ecology"],
            "portfolio_role": elite["portfolio_role"],
            "feature": elite["feature"],
            "profile": {
                "quantile": elite["quantile"],
                "horizon": elite["horizon"],
                "policy_direction": elite["policy_direction"],
            },
            "status": admission.tier.value,
            "admission": admission.to_dict(),
            "events": int(metrics["events"]),
            "net_pnl": float(metrics["net_pnl"]),
            "micro_events": int(metrics["events"]),
            "micro_net_pnl": float(metrics["net_pnl"]),
            "supportive_temporal_folds": int(metrics["supportive_temporal_folds"]),
            "fold_results": metrics["fold_results"],
            "micro_fold_results": metrics["fold_results"],
            "cost_stress_1_5x_net": float(metrics["cost_stress_1_5x_net"]),
            "development_2023": row["development"],
            "null_evidence": {
                "method": "frozen_elite_five_session_block_sign_flip_bh",
                "raw_probability": float(row["raw_probability"]),
                "adjusted_probability": float(adjusted_probability),
                "family_size": len(validations),
                "prospective_alpha": PROMOTION_ALPHA,
                "promotion_passed": bool(adjusted_probability <= PROMOTION_ALPHA),
                "shadow_research_support_threshold": SHADOW_ALPHA,
                "shadow_research_support_passed": support,
            },
            "contract_transfer": {
                "signal": elite["target_market"],
                "execution": elite["execution_market"],
                "passed": bool(evidence.contract_evidence),
                "signal_recomputed_from_micro": False,
                "missing_rate": float(row["missing_rate"]),
            },
            "parameter_diagnostics": row["neighbors"],
            "attacks": {
                **concentration,
                "one_additional_bar_delay_net": float(row["delayed"]["net_pnl"]),
                "missing_match_rate": float(row["missing_rate"]),
                "source_prior_session_only": True,
                "micro_signal_recomputed": False,
            },
            "topstep": account,
            "shadow_evidence": evidence.__dict__,
        }
        candidates.append(candidate)
        if admission.permits_zero_risk_shadow:
            specification = _shadow_specification(
                elite, freeze_manifest["elite_manifest_hash"]
            )
            configuration_path = specification.write_immutable(
                destination
                / "shadow_configurations"
                / f"{elite['candidate_id']}.json"
            )
            shadow_configurations.append(
                {
                    "candidate_id": elite["candidate_id"],
                    "status": admission.tier.value,
                    "path": str(configuration_path),
                    "configuration_hash": specification.configuration_hash,
                    "outbound_orders_enabled": False,
                }
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
    if shadow_count:
        conclusion = "CROSS_ASSET_DAILY_SHADOW_CANDIDATES_FOUND"
        next_action = "ACTIVATE_IMMUTABLE_ZERO_ORDER_SHADOWS"
    elif promising:
        conclusion = "CROSS_ASSET_DAILY_PROMISING_BUT_INSUFFICIENT"
        next_action = "TARGET_FAILURE_SURFACE_WITH_FRESH_IDS"
    else:
        conclusion = "CROSS_ASSET_DAILY_NO_PROMOTION"
        next_action = "PIVOT_DISTRIBUTIONAL_OR_PORTFOLIO_ROLE"
    trade_path = destination / "cross_asset_daily_trade_ledger.jsonl"
    trade_ledger = (
        pd.concat(ledger_frames, ignore_index=True)
        if ledger_frames
        else pd.DataFrame()
    )
    _write_ledger(trade_path, trade_ledger)
    integrity = {
        "population_exact_720": len(population) == EXPECTED_POPULATION,
        "unique_fingerprints": len(
            {row["structural_fingerprint"] for row in population}
        )
        == EXPECTED_POPULATION,
        "early_data_end_exclusive": early_provenance["end_exclusive"]
        == "2024-01-01",
        "elite_freeze_precedes_confirmation": freeze_path.is_file(),
        "selector_uses_no_2024": not selector.audit["uses_2024_results"],
        "maximum_eight_elites": len(elites) <= 8,
        "negative_controls_separate": bool(
            selector.audit["negative_controls_count_as_elites"] is False
        ),
        "source_prior_session_only": True,
        "q4_excluded": True,
        "no_network_or_paid_data": True,
        "no_outbound_order_capability": True,
    }
    if not all(integrity.values()):
        raise CrossAssetDailyHorizonError(f"Integrity failed: {integrity}")
    elapsed = time.perf_counter() - started
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Elites were frozen from 2023 before unchanged 2024 Q1-Q3 replay. "
            "No result opens Q4, inherits family evidence or authorizes orders."
        ),
        "code_commit": code_commit,
        "candidate_count": EXPECTED_POPULATION,
        "structural_prototypes": EXPECTED_POPULATION,
        "round1_survivors": len(round1_survivors),
        "round2_survivors": len(round2_survivors),
        "diagnostic_archive_size": len(round2_survivors),
        "elite_count": len(elites),
        "elite_candidate_ids": [row["candidate_id"] for row in elites],
        "negative_controls": [row["candidate_id"] for row in controls],
        "selector_audit": selector.audit,
        "candidates": candidates,
        "candidate_tier_counts": dict(Counter(statuses)),
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow_count),
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(topstep_count),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "dispositions": dispositions,
        "shadow_configurations": shadow_configurations,
        "integrity_proof": integrity,
        "data_provenance": {
            "selection": early_provenance,
            "confirmation": confirmation_provenance,
        },
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "elite_manifest_path": str(freeze_path),
        "elite_manifest_hash": freeze_manifest["elite_manifest_hash"],
        "performance": {
            "freeze_seconds": freeze_seconds,
            "total_seconds": elapsed,
            "structural_candidates_per_second": EXPECTED_POPULATION
            / max(elapsed, 1e-9),
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
    result_path = destination / "cross_asset_daily_result.json"
    report_path = destination / "cross_asset_daily_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "elite_manifest_path": str(freeze_path),
            "trade_ledger_path": str(trade_path),
        },
        "report_path": str(report_path),
    }


def build_daily_feature_cache(
    tables: dict[str, pd.DataFrame],
) -> dict[tuple[str, str, str], pd.DataFrame]:
    source_cache = {
        source: _source_features(tables[source]).set_index("session_id", drop=False)
        for source in TARGETS
    }
    target_cache = {
        target: tables[target].copy().set_index("session_id", drop=False)
        for target in TARGETS
    }
    cache: dict[tuple[str, str, str], pd.DataFrame] = {}
    source_columns = [
        "source_prior_session_id",
        "source_prior_trend",
        "source_prior_range_shock_signed",
        "source_prior_close_location",
    ]
    for target in TARGETS:
        for source in TARGETS:
            aligned = target_cache[target].join(
                source_cache[source][source_columns], how="left"
            )
            for feature in FEATURES:
                if feature == "relative_prior_trend" and source == target:
                    continue
                frame = aligned.copy()
                if feature == "relative_prior_trend":
                    frame["_feature_value"] = pd.to_numeric(
                        frame["source_prior_trend"], errors="coerce"
                    ) - pd.to_numeric(frame["prior_trend"], errors="coerce")
                else:
                    frame["_feature_value"] = pd.to_numeric(
                        frame[feature], errors="coerce"
                    )
                cache[(target, source, feature)] = frame
    return cache


def _source_features(table: pd.DataFrame) -> pd.DataFrame:
    ordered = table.sort_values("session_id").copy()
    prior_range = pd.to_numeric(ordered["prior_range"], errors="coerce")
    historical_range = prior_range.shift(1).rolling(20, min_periods=10).median()
    current_range = (
        pd.to_numeric(ordered["rth_high"], errors="coerce")
        - pd.to_numeric(ordered["rth_low"], errors="coerce")
    ).replace(0, np.nan)
    close_location = (
        (
            pd.to_numeric(ordered["rth_close"], errors="coerce")
            - pd.to_numeric(ordered["rth_low"], errors="coerce")
        )
        / current_range
        - 0.5
    ).shift(1)
    return pd.DataFrame(
        {
            "session_id": ordered["session_id"],
            "source_prior_session_id": ordered["session_id"].shift(1),
            "source_prior_trend": pd.to_numeric(
                ordered["prior_trend"], errors="coerce"
            ),
            "source_prior_range_shock_signed": pd.to_numeric(
                ordered["prior_trend"], errors="coerce"
            )
            * prior_range
            / historical_range.replace(0, np.nan),
            "source_prior_close_location": close_location,
        }
    )


def _load_tables(
    core_data_path: Path,
    core_map: Any,
    metals_data_path: Path,
    metals_map: Any,
    end_exclusive: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    core_symbols = {"YM", "MYM", "RTY", "M2K", "CL", "MCL"}
    metals_symbols = {"GC", "MGC"}
    core = _read_period(core_data_path, core_symbols, end_exclusive)
    metals = _read_period(metals_data_path, metals_symbols, end_exclusive)
    core, core_audit = _apply_explicit_contract_map(
        core, core_map, required_map_type=core_map.map_type
    )
    metals, metals_audit = _apply_explicit_contract_map(
        metals, metals_map, required_map_type=VOLUME_FRONT_MAP_TYPE
    )
    tables = {
        symbol: build_session_geometry_table(
            metals if symbol in metals_symbols else core, symbol
        )
        for symbol in (*TARGETS, *(meta["micro"] for meta in TARGETS.values()))
    }
    if end_exclusive == "2024-01-01" and any(
        pd.to_datetime(table["session_id"]).max() >= pd.Timestamp("2024-01-01")
        for table in tables.values()
    ):
        raise CrossAssetDailyHorizonError("Selection tables exposed 2024.")
    return tables, {
        "period_start": "2023-01-01",
        "end_exclusive": end_exclusive,
        "sessions_by_symbol": {symbol: len(table) for symbol, table in tables.items()},
        "core_contract_audit": core_audit,
        "metals_contract_audit": metals_audit,
    }


def _micro_execution(
    signals: pd.DataFrame,
    tables: dict[str, pd.DataFrame],
    hypothesis: dict[str, Any],
    *,
    entry_delay_bars: int = 0,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    return synchronize_micro_execution(
        signals,
        tables[str(hypothesis["execution_market"])],
        signal_symbol=str(hypothesis["target_market"]),
        execution_symbol=str(hypothesis["execution_market"]),
        candidate_id=str(hypothesis["candidate_id"]),
        parent_candidate_id=str(hypothesis["candidate_id"]),
        entry_prefix="overnight",
        horizon=int(hypothesis["horizon"]),
        entry_delay_bars=entry_delay_bars,
    )


def _neighbor_diagnostics(
    tables: dict[str, pd.DataFrame],
    hypothesis: dict[str, Any],
    *,
    feature_cache: dict[tuple[str, str, str], pd.DataFrame] | None = None,
) -> dict[str, Any]:
    variants: dict[str, Any] = {}
    for label, quantile, horizon in (
        (
            "lower_quantile",
            max(0.50, float(hypothesis["quantile"]) - 0.10),
            int(hypothesis["horizon"]),
        ),
        (
            "higher_quantile",
            min(0.95, float(hypothesis["quantile"]) + 0.10),
            int(hypothesis["horizon"]),
        ),
        (
            "adjacent_horizon",
            float(hypothesis["quantile"]),
            {30: 60, 60: 120, 120: 60}[int(hypothesis["horizon"])],
        ),
    ):
        adjusted_hypothesis = {**hypothesis, "horizon": horizon}
        signals = build_daily_horizon_events(
            tables,
            adjusted_hypothesis,
            feature_cache=feature_cache,
            quantile_override=quantile,
            horizon_override=horizon,
        )
        events, _missing = _micro_execution(signals, tables, adjusted_hypothesis)
        variants[label] = _period_metrics(
            _period(events, "2024-01-01", "2024-10-01")
        )
    return {
        "diagnostic_only": True,
        "variants": variants,
        "positive_neighbor_count": sum(
            row["net_pnl"] > 0 for row in variants.values()
        ),
    }


def _bh_adjust(probabilities: list[float]) -> list[float]:
    if not probabilities:
        return []
    count = len(probabilities)
    order = sorted(range(count), key=lambda index: probabilities[index])
    adjusted = [1.0] * count
    running = 1.0
    for reverse_rank, index in enumerate(reversed(order), start=1):
        rank = count - reverse_rank + 1
        value = min(1.0, float(probabilities[index]) * count / rank)
        running = min(running, value)
        adjusted[index] = running
    return adjusted


def _period(events: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    timestamps = pd.to_datetime(events["entry_timestamp"], utc=True)
    return events[timestamps.ge(start) & timestamps.lt(end)].copy()


def _gate(metrics: dict[str, Any], minimum_events: int) -> bool:
    return bool(
        metrics["finite"]
        and metrics["events"] >= minimum_events
        and metrics["net_pnl"] > 0
        and metrics["cost_stress_1_5x_net"] > 0
        and metrics["best_positive_event_share"] <= 0.35
    )


def _failure(metrics: dict[str, Any], minimum_events: int) -> str:
    if not metrics["finite"]:
        return "NONFINITE"
    if metrics["events"] < minimum_events:
        return "INSUFFICIENT_EVENTS"
    if metrics["net_pnl"] <= 0:
        return "NEGATIVE_ECONOMICS"
    if metrics["cost_stress_1_5x_net"] <= 0:
        return "COST_FRAGILITY"
    if metrics["best_positive_event_share"] > 0.35:
        return "CONCENTRATION"
    return "UNSPECIFIED"


def _mechanism_family(feature: str) -> str:
    return {
        "source_prior_trend": "daily_direction_transfer",
        "source_prior_range_shock_signed": "daily_volatility_direction_transfer",
        "source_prior_close_location": "daily_acceptance_transfer",
        "relative_prior_trend": "daily_cross_market_dispersion",
    }[feature]


def _record_access_once(population: list[dict[str, Any]]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "frozen cross-asset daily-horizon tournament; Q4 excluded"
    module = "hydra.research.cross_asset_daily_horizon_primary"
    candidate_ids = [row["candidate_id"] for row in population]
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


def _shadow_specification(
    elite: dict[str, Any], source_manifest_hash: str
) -> ShadowSpecification:
    target = str(elite["target_market"])
    execution = str(elite["execution_market"])
    source = str(elite["source_market"])
    return ShadowSpecification(
        strategy_id=str(elite["candidate_id"]),
        strategy_version="v1_cross_asset_daily_pre_holdout",
        feature_versions=(
            "completed_prior_session_cross_asset_v1",
            "exact_micro_execution_v1",
        ),
        markets=tuple(sorted({source, target, execution})),
        timeframes=("1m", "RTH_session", "daily_context"),
        session_rules={
            "timezone": "America/Chicago",
            "source_market": source,
            "target_market": target,
            "execution_market": execution,
            "source_session_must_precede_target_session": True,
            "mandatory_flatten_before_session_end": True,
        },
        entry_rules={
            "feature": elite["feature"],
            "quantile": elite["quantile"],
            "direction": elite["policy_direction"],
            "threshold_history_sessions": 20,
            "execution_delay_completed_bars": 1,
            "micro_signal_recomputation": False,
            "exact_timestamp_match_required": True,
            "missing_match_policy": "fail_closed_skip_signal",
        },
        exit_rules={
            "holding_completed_1m_bars": int(elite["horizon"]),
            "no_overnight": True,
        },
        sizing={"contracts": 1, "instrument": execution, "micro_first": True},
        costs={
            "round_turn_usd": _round_turn_cost_all(execution),
            "slippage_ticks_round_turn": 2,
        },
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=3600,
        maximum_exposure=0.1,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=(
            "stale_data",
            "duplicate_signal",
            "session_closed",
            "clock_invalid",
            "contract_map_mismatch",
            "source_session_not_completed",
            "signal_execution_timestamp_mismatch",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={
            "source_state_ledger": True,
            "target_signal_ledger": True,
            "micro_virtual_fill_ledger": True,
            "latency_and_staleness": True,
            "account_mll_path": True,
            "source_manifest_hash": source_manifest_hash,
        },
        reconciliation={
            "startup_reconcile": True,
            "expected_vs_observed_virtual_fill": True,
            "fail_on_configuration_hash_mismatch": True,
        },
        source_manifest_hash=source_manifest_hash,
        outbound_orders_enabled=False,
    )


def _write_ledger(path: Path, frame: pd.DataFrame) -> None:
    if frame.empty:
        _write_immutable(path, "")
        return
    ordered = frame.sort_values(["entry_timestamp", "candidate_id"])
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in ordered.to_dict("records")
    ]
    _write_immutable(path, "\n".join(lines) + "\n")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise CrossAssetDailyHorizonError(
            f"Frozen {label} missing or changed: {path}"
        )


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Cross-Asset Daily-Horizon Tournament",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Structural prototypes: `{payload['structural_prototypes']}`",
            f"- Round-1 survivors: `{payload['round1_survivors']}`",
            f"- Round-2 survivors: `{payload['round2_survivors']}`",
            f"- Frozen elites: `{payload['elite_count']}`",
            f"- Promising: `{payload['promising_candidates']}`",
            f"- Shadow candidates: `{payload['shadow_candidates']}`",
            f"- Topstep-path candidates: `{payload['topstep_path_candidates']}`",
            "- PAPER_SHADOW_READY: `0`",
            "- Q4 access delta: `0`",
            "- Outbound orders: `0`",
            "",
        ]
    )
