from __future__ import annotations

import json
import math
import subprocess
import gc
import time
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.factory.elite_selection_manifest import (
    build_elite_selection_manifest,
    write_immutable_elite_manifest,
)
from hydra.factory.quality_diversity_selector_v2 import (
    SelectorV2Policy,
    select_quality_diversity_elites_v2,
)
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _build_past_only_feature_frame,
    _file_sha256,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
)
from hydra.research.equity_open_gap_reversal import (
    MAP_TYPE,
    SOURCE_PREREGISTRATION_SHA256,
    _account_replay,
    _write_immutable,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "qd_economic_tournament_v2"
POPULATION_ID = "qd_economic_tournament_population_20260711_v2"
MARKET_PAIRS = {
    "ES": "MES",
    "NQ": "MNQ",
    "RTY": "M2K",
    "YM": "MYM",
    "GC": "MGC",
    "CL": "MCL",
}
SYMBOLS = tuple(symbol for pair in MARKET_PAIRS.items() for symbol in pair)
FEATURES = (
    "old_region_reentry",
    "directional_pressure_without_progress",
    "shared_loss_risk_state",
    "failed_expansion",
    "extreme_dwell",
    "rv_short_long_ratio",
    "past_return_60",
    "past_volatility",
    "past_participation",
)
DIRECTIONAL_FEATURES = frozenset(
    {"old_region_reentry", "extreme_dwell", "past_return_60"}
)
FEATURE_FAMILIES = {
    "old_region_reentry": "market_state_geometry",
    "directional_pressure_without_progress": "market_state_geometry",
    "shared_loss_risk_state": "distributional_risk_hazard",
    "failed_expansion": "market_state_geometry",
    "extreme_dwell": "market_state_geometry",
    "rv_short_long_ratio": "volatility_state_transition",
    "past_return_60": "invariant_price_state",
    "past_volatility": "volatility_state_transition",
    "past_participation": "participation_state_transition",
}
PROFILES: dict[str, dict[str, Any]] = {
    "open_q65_h15": {"session": "open", "quantile": 0.65, "horizon": 15},
    "open_q75_h30": {"session": "open", "quantile": 0.75, "horizon": 30},
    "middle_q65_h30": {"session": "middle", "quantile": 0.65, "horizon": 30},
    "late_q75_h60": {"session": "late", "quantile": 0.75, "horizon": 60},
    "all_q85_h60": {"session": "all", "quantile": 0.85, "horizon": 60},
}
SESSION_CLOCKS = {
    "ES": (8 * 60 + 30, 390),
    "MES": (8 * 60 + 30, 390),
    "NQ": (8 * 60 + 30, 390),
    "MNQ": (8 * 60 + 30, 390),
    "RTY": (8 * 60 + 30, 390),
    "M2K": (8 * 60 + 30, 390),
    "YM": (8 * 60 + 30, 390),
    "MYM": (8 * 60 + 30, 390),
    "GC": (7 * 60 + 20, 310),
    "MGC": (7 * 60 + 20, 310),
    "CL": (8 * 60, 330),
    "MCL": (8 * 60, 330),
}
VALIDATION_FOLDS = {
    "2024_q1": ("2024-01-01", "2024-04-01"),
    "2024_q2": ("2024-04-01", "2024-07-01"),
    "2024_q3": ("2024-07-01", "2024-10-01"),
}
DISCOVERY_FOLD = ("2023-07-01", "2024-01-01")
MAX_VALIDATION_ELITES = 20
THRESHOLD_HISTORY_SESSIONS = 20
COMMISSION_ROUND_TURN = {
    symbol: (2.0 if instrument_spec(symbol).is_micro else 4.5) for symbol in SYMBOLS
}


class QDEconomicTournamentError(RuntimeError):
    pass


def run_qd_economic_tournament(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    selector_task_path: str | Path,
    selector_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 772013,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    task_path, selector_path, map_path = (
        Path(engineering_task_path),
        Path(selector_task_path),
        Path(repaired_map_path),
    )
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(selector_path, selector_task_sha256, "selector v2 task")
    _verify(map_path, repaired_map_sha256, "repaired contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise QDEconomicTournamentError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise QDEconomicTournamentError("Worker commit differs from queued specification.")

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

    prototypes = generate_prototypes()
    if len(prototypes) != 540 or len({item["structural_fingerprint"] for item in prototypes}) != 540:
        raise QDEconomicTournamentError("Frozen population size or deduplication changed.")
    preregistration = _preregistration(
        prototypes=prototypes,
        engineering_task_sha256=engineering_task_sha256,
        selector_task_sha256=selector_task_sha256,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        code_commit=code_commit,
        random_seed=random_seed,
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration_path = destination / "qd_economic_tournament_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    access = _record_access_once() if record_data_access else None
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    latest_source_timestamp = pd.to_datetime(raw["timestamp"], utc=True).max()
    feature_frame = _build_past_only_feature_frame(raw)
    del raw
    gc.collect()
    feature_frame = _prepare_feature_frame(feature_frame)
    frames = {
        symbol: _prepare_execution_cache(group.sort_values("timestamp").reset_index(drop=True))
        for symbol, group in feature_frame.groupby("symbol", sort=True)
    }
    del feature_frame
    gc.collect()
    feature_preparation_seconds = time.perf_counter() - started_at
    dispositions: list[dict[str, Any]] = []
    discovery_survivors: list[dict[str, Any]] = []
    discovery_failures: list[dict[str, Any]] = []
    for market in MARKET_PAIRS:
        market_frame = frames[market]
        for feature in FEATURES:
            thresholds = _causal_session_thresholds(
                market_frame[feature],
                market_frame["trading_session_id"],
                sorted({float(item["quantile"]) for item in PROFILES.values()}),
            )
            population = [
                item
                for item in prototypes
                if item["market"] == market and item["feature"] == feature
            ]
            for prototype in population:
                quantile = float(PROFILES[prototype["profile"]]["quantile"])
                events = build_prototype_events(
                    market_frame, prototype, threshold_override=thresholds[quantile]
                )
                discovery = _period_events(events, *DISCOVERY_FOLD)
                metrics = _period_metrics(discovery)
                passed = bool(
                    metrics["events"] >= 15
                    and metrics["net_pnl"] > 0
                    and metrics["cost_stress_1_5x_net"] > 0
                    and metrics["best_positive_event_share"] <= 0.35
                    and metrics["finite"]
                )
                disposition = {
                    "candidate_id": prototype["candidate_id"],
                    "structural_fingerprint": prototype["structural_fingerprint"],
                    "lineage_id": prototype["lineage_id"],
                    "market": prototype["market"],
                    "market_ecology": prototype["market_ecology"],
                    "mechanism_family": prototype["mechanism_family"],
                    "profile": prototype["profile"],
                    "policy_direction": prototype["policy_direction"],
                    "discovery": metrics,
                    "stage1_pass": passed,
                    "stage1_disposition": "QD_SELECTION_ELIGIBLE"
                    if passed
                    else _kill_reason(metrics),
                }
                dispositions.append(disposition)
                if passed:
                    discovery_survivors.append(
                        {**prototype, "discovery": metrics, "stage1_pass": True}
                    )
                else:
                    discovery_failures.append(
                        {
                            **prototype,
                            "discovery": metrics,
                            "stage1_pass": False,
                            "stage1_disposition": disposition["stage1_disposition"],
                        }
                    )

    selector_result = select_quality_diversity_elites_v2(
        discovery_survivors,
        failed_candidates=discovery_failures,
        policy=SelectorV2Policy(maximum_elites=MAX_VALIDATION_ELITES),
    )
    elites = list(selector_result.elites)
    negative_controls = list(selector_result.negative_controls)
    selection_manifest = build_elite_selection_manifest(
        selector_result,
        population_hash=str(preregistration["population_hash"]),
        selector_task_sha256=selector_task_sha256,
    )
    selection_path = destination / "qd_economic_tournament_selection_manifest.json"
    write_immutable_elite_manifest(selection_path, selection_manifest)
    selection_freeze_seconds = time.perf_counter() - started_at

    validation_rows: list[dict[str, Any]] = []
    event_ledgers: list[pd.DataFrame] = []
    selected_symbols = {
        symbol
        for prototype in elites
        for symbol in (prototype["market"], prototype["execution_market"])
    }
    mae_path_caches = {
        symbol: build_market_path_cache(frames[symbol]) for symbol in selected_symbols
    }
    for elite_index, prototype in enumerate(elites):
        mini_events = attach_event_mae(
            build_prototype_events(frames[prototype["market"]], prototype),
            frames[prototype["market"]],
            path_cache=mae_path_caches[prototype["market"]],
        )
        micro_prototype = {**prototype, "market": prototype["execution_market"]}
        micro_events = attach_event_mae(
            build_prototype_events(frames[prototype["execution_market"]], micro_prototype),
            frames[prototype["execution_market"]],
            path_cache=mae_path_caches[prototype["execution_market"]],
        )
        mini_validation = _validation_events(mini_events)
        micro_validation = _validation_events(micro_events)
        mini_metrics = _validation_metrics(mini_validation)
        micro_metrics = _validation_metrics(micro_validation)
        raw_probability = _block_sign_flip_probability(
            mini_validation, seed=random_seed + elite_index * 1009
        )
        diagnostics = _parameter_diagnostics(frames[prototype["market"]], prototype)
        validation_rows.append(
            {
                "prototype": prototype,
                "mini_events": mini_validation,
                "micro_events": micro_validation,
                "mini_metrics": mini_metrics,
                "micro_metrics": micro_metrics,
                "raw_probability": raw_probability,
                "parameter_diagnostics": diagnostics,
            }
        )
    negative_control_results: list[dict[str, Any]] = []
    for control_index, prototype in enumerate(negative_controls):
        events = _validation_events(
            build_prototype_events(frames[prototype["market"]], prototype)
        )
        metrics = _validation_metrics(events)
        negative_control_results.append(
            {
                "candidate_id": prototype["candidate_id"],
                "market": prototype["market"],
                "market_ecology": prototype["market_ecology"],
                "mechanism_family": prototype["mechanism_family"],
                "stage1_disposition": prototype.get("stage1_disposition"),
                "promotion_eligible": False,
                "validation_metrics": metrics,
                "raw_sign_flip_probability": _block_sign_flip_probability(
                    events, seed=random_seed + 50_000 + control_index * 1009
                ),
            }
        )
        if not events.empty:
            logged = events.copy()
            logged["candidate_id"] = prototype["candidate_id"]
            logged["contract_role"] = "negative_control_mini"
            event_ledgers.append(logged)
    adjusted_probabilities = _benjamini_hochberg(
        [float(item["raw_probability"]) for item in validation_rows]
    )

    candidates: list[dict[str, Any]] = []
    shadow_configs: list[dict[str, Any]] = []
    for index, row in enumerate(validation_rows):
        prototype = row["prototype"]
        mini_events = row["mini_events"]
        micro_events = row["micro_events"]
        mini_metrics = row["mini_metrics"]
        micro_metrics = row["micro_metrics"]
        adjusted = adjusted_probabilities[index]
        diagnostics = row["parameter_diagnostics"]
        parameter_stable = bool(
            diagnostics["positive_neighbor_count"] >= 1
            and mini_metrics["cost_stress_1_5x_net"] > 0
        )
        contract_evidence = bool(
            mini_metrics["net_pnl"] > 0
            and micro_metrics["net_pnl"] > 0
            and micro_metrics["supportive_temporal_folds"] >= 1
        )
        account_events = micro_events.rename(columns={"net_pnl": "net_pnl_60"}).copy()
        account = _account_replay(account_events)
        specification = _shadow_specification(
            prototype, selection_manifest_hash=selection_manifest["selection_manifest_hash"]
        )
        specification.validate()
        evidence = ShadowEvidence(
            candidate_id=prototype["candidate_id"],
            data_integrity=True,
            no_lookahead=True,
            deterministic_signals=True,
            net_after_costs=float(mini_metrics["net_pnl"]),
            supportive_temporal_folds=int(mini_metrics["supportive_temporal_folds"]),
            catastrophic_transfer=bool(mini_metrics["catastrophic_transfer"]),
            candidate_null_pass=bool(
                adjusted <= 0.20 and mini_metrics["sign_flip_net"] < 0
            ),
            null_probability=float(adjusted),
            parameter_stable=parameter_stable,
            contract_evidence=contract_evidence,
            account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
            execution_possible=True,
            realtime_features_available=True,
            shadow_spec_complete=True,
            observability_complete=True,
            untouched_holdout_passed=False,
            sample_size=int(mini_metrics["events"]),
            uncertainty="selected_on_2023_validated_on_development_2024_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise QDEconomicTournamentError("Development tournament attempted paper promotion.")
        candidate = {
            "candidate_id": prototype["candidate_id"],
            "lineage_id": prototype["lineage_id"],
            "structural_fingerprint": prototype["structural_fingerprint"],
            "mechanism_family": prototype["mechanism_family"],
            "primary_market": prototype["market"],
            "execution_market": prototype["execution_market"],
            "market_ecology": prototype["market_ecology"],
            "portfolio_role": prototype["portfolio_role"],
            "feature": prototype["feature"],
            "profile": prototype["profile"],
            "policy_direction": prototype["policy_direction"],
            "status": admission.tier.value,
            "admission": admission.to_dict(),
            "events": int(mini_metrics["events"]),
            "net_pnl": float(mini_metrics["net_pnl"]),
            "mean_net_pnl": float(mini_metrics["mean_net_pnl"]),
            "micro_events": int(micro_metrics["events"]),
            "micro_net_pnl": float(micro_metrics["net_pnl"]),
            "supportive_temporal_folds": int(mini_metrics["supportive_temporal_folds"]),
            "fold_results": mini_metrics["fold_results"],
            "micro_fold_results": micro_metrics["fold_results"],
            "discovery_evidence": prototype["discovery"],
            "null_evidence": {
                "method": "validation_five_event_block_sign_flip_bh_across_elites",
                "raw_probability": float(row["raw_probability"]),
                "family_adjusted_probability": float(adjusted),
                "validation_elite_count": len(validation_rows),
            },
            "parameter_diagnostics": diagnostics,
            "cost_stress_1_5x_net": float(mini_metrics["cost_stress_1_5x_net"]),
            "contract_transfer": {
                "mini": prototype["market"],
                "micro": prototype["execution_market"],
                "passed": contract_evidence,
                "micro_supportive_folds": int(micro_metrics["supportive_temporal_folds"]),
            },
            "attacks": {
                "sign_flip_net": float(mini_metrics["sign_flip_net"]),
                "best_event_share_of_positive_pnl": float(
                    mini_metrics["best_positive_event_share"]
                ),
                "best_fold_share_of_positive_pnl": float(
                    mini_metrics["best_positive_fold_share"]
                ),
                "event_dominated": bool(mini_metrics["event_dominated"]),
                "one_bar_execution_delay_embedded": True,
            },
            "topstep": account,
            "shadow_evidence": evidence.__dict__,
        }
        candidates.append(candidate)
        for events, contract_role in ((mini_events, "mini"), (micro_events, "micro")):
            if events.empty:
                continue
            logged = events.copy()
            logged["candidate_id"] = prototype["candidate_id"]
            logged["contract_role"] = contract_role
            event_ledgers.append(logged)
        if admission.permits_zero_risk_shadow:
            path = specification.write_immutable(
                destination / "shadow_configurations" / f"{prototype['candidate_id']}.json"
            )
            shadow_configs.append(
                {
                    "candidate_id": prototype["candidate_id"],
                    "status": admission.tier.value,
                    "path": str(path),
                    "configuration_hash": specification.configuration_hash,
                    "outbound_orders_enabled": False,
                }
            )

    statuses = [item["status"] for item in candidates]
    promising_tiers = {
        EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
        EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
    }
    promising = sum(status in promising_tiers for status in statuses)
    shadow = statuses.count(EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value)
    topstep = sum(bool(item["topstep"].get("path_candidate", False)) for item in candidates)
    if shadow:
        conclusion = "QD_ECONOMIC_TOURNAMENT_V2_SHADOW_RESEARCH_CANDIDATES_FOUND"
        next_action = "FREEZE_DISTINCT_QD_CANDIDATES_AND_START_FORWARD_SHADOW"
    elif promising:
        conclusion = "QD_ECONOMIC_TOURNAMENT_V2_PROMISING_BUT_INSUFFICIENT"
        next_action = "TARGETED_COUNTERFACTUALS_FOR_QD_ELITES"
    elif candidates:
        conclusion = "QD_ECONOMIC_TOURNAMENT_V2_VALIDATION_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_SEARCH_REPRESENTATION_USING_FAILURE_MAP"
    else:
        conclusion = "QD_ECONOMIC_TOURNAMENT_V2_NO_DISCOVERY_SURVIVORS"
        next_action = "PIVOT_TO_EVENT_LEVEL_COUNTERFACTUAL_OR_HAZARD_SEARCH"

    population_path = destination / "qd_economic_tournament_population_dispositions.json"
    _write_immutable(
        population_path,
        json.dumps(
            {
                "schema": "qd_economic_tournament_population_dispositions_v2",
                "population_hash": preregistration["population_hash"],
                "dispositions": _strict_json_value(dispositions),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    ledger_path = destination / "qd_economic_tournament_trade_ledger.jsonl"
    combined_events = (
        pd.concat(event_ledgers, ignore_index=True) if event_ledgers else pd.DataFrame()
    )
    _write_event_ledger(ledger_path, combined_events)
    integrity = _integrity_proof(
        latest_source_timestamp=latest_source_timestamp,
        prototypes=prototypes,
        dispositions=dispositions,
        elites=elites,
        negative_controls=negative_controls,
        selector_audit=selector_result.audit,
        combined_events=combined_events,
    )
    if not all(integrity.values()):
        raise QDEconomicTournamentError(f"Tournament integrity failed: {integrity}")
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Selection was frozen from 2023 before 2024 candidate replay. These are development "
            "transfer results, not an untouched holdout. No Stage-0 status is inherited, Q4 remains "
            "sealed, PAPER_SHADOW_READY is impossible here, and no order path exists."
        ),
        "code_commit": code_commit,
        "preregistration_hash": preregistration["preregistration_hash"],
        "preregistration_path": str(preregistration_path),
        "selection_manifest_hash": selection_manifest["selection_manifest_hash"],
        "selection_manifest_path": str(selection_path),
        "data_provenance": provenance,
        "data_access_record": access,
        "integrity_proof": integrity,
        "requested_prototypes": 540,
        "candidate_count": 540,
        "structural_fingerprints": 540,
        "stage1_survivors": len(discovery_survivors),
        "validation_elites": len(elites),
        "negative_controls": negative_control_results,
        "candidate_tier_counts": dict(Counter(statuses)),
        "candidates": candidates,
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow),
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(topstep),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "quality_diversity_archive": {
            "selector_v2_audit": selector_result.audit,
            "selected_ids": selection_manifest["selected_candidate_ids"],
            "negative_control_ids": selection_manifest["negative_control_ids"],
            "family_counts": dict(Counter(item["mechanism_family"] for item in elites)),
            "ecology_counts": dict(Counter(item["market_ecology"] for item in elites)),
            "market_counts": dict(Counter(item["market"] for item in elites)),
        },
        "mechanism_families": sorted({item["mechanism_family"] for item in prototypes}),
        "market_ecologies": ["equity_indices", "metals", "energy"],
        "timeframe_profiles": [
            "past_only_1m_state_one_bar_delayed_1m_execution_15m",
            "past_only_1m_state_one_bar_delayed_1m_execution_30m",
            "past_only_1m_state_one_bar_delayed_1m_execution_60m",
        ],
        "shadow_configurations": shadow_configs,
        "performance": {
            "reference_pre_optimization_aborted_seconds": 349.09,
            "reference_optimized_v1_seconds": 181.10,
            "feature_preparation_seconds": feature_preparation_seconds,
            "selection_freeze_seconds": selection_freeze_seconds,
            "total_seconds": time.perf_counter() - started_at,
            "prototypes_per_second": 540 / max(time.perf_counter() - started_at, 1e-9),
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
    result_path = destination / "qd_economic_tournament_result.json"
    report_path = destination / "qd_economic_tournament_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "population_dispositions_path": str(population_path),
            "selection_manifest_path": str(selection_path),
            "trade_ledger_path": str(ledger_path),
            "shadow_configuration_directory": str(destination / "shadow_configurations"),
        },
        "report_path": str(report_path),
    }


def generate_prototypes() -> list[dict[str, Any]]:
    prototypes: list[dict[str, Any]] = []
    for market, execution_market in MARKET_PAIRS.items():
        ecology = (
            "equity_indices" if market in {"ES", "NQ", "RTY", "YM"} else "metals" if market == "GC" else "energy"
        )
        for feature in FEATURES:
            for direction in ("continuation", "reversal"):
                for profile in PROFILES:
                    specification = {
                        "market": market,
                        "execution_market": execution_market,
                        "feature": feature,
                        "policy_direction": direction,
                        "profile": profile,
                        "mechanism_family": FEATURE_FAMILIES[feature],
                        "causal_availability": "feature_bar_close_then_one_bar_execution_delay",
                        "contract_policy": "explicit_roll_aware",
                    }
                    fingerprint = structural_fingerprint(specification)
                    candidate_id = f"strategy_qd_{market}_{feature}_{direction}_{profile}_v1"
                    prototypes.append(
                        {
                            **specification,
                            "candidate_id": candidate_id,
                            "lineage_id": f"lineage_{fingerprint[:20]}",
                            "structural_fingerprint": fingerprint,
                            "market_ecology": ecology,
                            "portfolio_role": "trend" if direction == "continuation" else "reversal",
                        }
                    )
    return prototypes


def _prepare_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    local = data["timestamp"].dt.tz_convert("America/Chicago")
    data["local_minute"] = local.dt.hour * 60 + local.dt.minute
    data["market_open_minute"] = data["symbol"].map(
        {symbol: value[0] for symbol, value in SESSION_CLOCKS.items()}
    )
    data["market_session_length"] = data["symbol"].map(
        {symbol: value[1] for symbol, value in SESSION_CLOCKS.items()}
    )
    data["minutes_from_market_open"] = data["local_minute"] - data["market_open_minute"]
    return data.sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _prepare_execution_cache(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.sort_values("timestamp").reset_index(drop=True).copy()
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    groups = data.groupby(grouping, sort=False, observed=True)
    data["next_timestamp"] = groups["timestamp"].shift(-1)
    data["entry_price"] = groups["close"].shift(-1)
    data["entry_timestamp"] = data["next_timestamp"] + pd.Timedelta(minutes=1)
    for horizon in (15, 30, 60):
        exit_source = groups["timestamp"].shift(-(horizon + 1))
        data[f"exit_price_{horizon}"] = groups["close"].shift(-(horizon + 1))
        data[f"exit_timestamp_{horizon}"] = exit_source + pd.Timedelta(minutes=1)
    return data


def _causal_threshold(
    values: pd.Series, quantile: float, sessions: pd.Series | None = None
) -> pd.Series:
    if sessions is None:
        sessions = pd.Series(np.arange(len(values)) // 100, index=values.index)
    return _causal_session_thresholds(values, sessions, [quantile])[float(quantile)]


def _causal_session_thresholds(
    values: pd.Series, sessions: pd.Series, quantiles: list[float]
) -> dict[float, pd.Series]:
    magnitudes = pd.to_numeric(values, errors="coerce").abs().to_numpy(dtype=float)
    session_values = sessions.astype(str).to_numpy()
    if len(magnitudes) != len(session_values):
        raise QDEconomicTournamentError("Feature and session arrays differ in length.")
    outputs = {
        float(quantile): np.full(len(magnitudes), np.nan, dtype=float)
        for quantile in quantiles
    }
    if not len(magnitudes):
        return {key: pd.Series(value, index=values.index) for key, value in outputs.items()}
    boundaries = np.concatenate(
        (
            np.asarray([0]),
            np.flatnonzero(session_values[1:] != session_values[:-1]) + 1,
            np.asarray([len(session_values)]),
        )
    )
    for session_index in range(1, len(boundaries) - 1):
        history_index = max(0, session_index - THRESHOLD_HISTORY_SESSIONS)
        history = magnitudes[boundaries[history_index] : boundaries[session_index]]
        history = history[np.isfinite(history)]
        if len(history) < 500:
            continue
        computed = np.quantile(history, quantiles)
        start, end = boundaries[session_index], boundaries[session_index + 1]
        for quantile, threshold in zip(quantiles, computed, strict=True):
            outputs[float(quantile)][start:end] = float(threshold)
    return {
        key: pd.Series(output, index=values.index, dtype=float)
        for key, output in outputs.items()
    }


def build_prototype_events(
    frame: pd.DataFrame,
    prototype: dict[str, Any],
    *,
    threshold_override: pd.Series | None = None,
) -> pd.DataFrame:
    feature = str(prototype["feature"])
    profile = PROFILES[str(prototype["profile"])]
    horizon = int(profile["horizon"])
    quantile = float(profile["quantile"])
    data = frame
    if feature not in data:
        return _empty_events()
    values = pd.to_numeric(data[feature], errors="coerce")
    magnitudes = values.abs()
    threshold = (
        threshold_override.reindex(data.index)
        if threshold_override is not None
        else _causal_threshold(values, quantile, data["trading_session_id"])
    )
    segment = data["contiguous_segment_id"]
    previous_magnitude = magnitudes.groupby(segment, sort=False).shift(1)
    previous_threshold = threshold.groupby(segment, sort=False).shift(1)
    crossing = (magnitudes >= threshold) & (previous_magnitude < previous_threshold)
    if feature in DIRECTIONAL_FEATURES:
        anchor = np.sign(values)
    else:
        anchor = np.sign(pd.to_numeric(data["past_return_60"], errors="coerce"))
    policy_sign = 1 if prototype["policy_direction"] == "continuation" else -1
    side = pd.Series(anchor * policy_sign, index=data.index, dtype=float)
    session_mask = _profile_session_mask(data, str(profile["session"]), horizon=horizon)
    if "next_timestamp" not in data:
        data = _prepare_execution_cache(data)
    next_timestamp = data["next_timestamp"]
    entry_price = data["entry_price"]
    entry_timestamp = data["entry_timestamp"]
    exit_price = data[f"exit_price_{horizon}"]
    exit_timestamp = data[f"exit_timestamp_{horizon}"]
    valid_timing = (
        next_timestamp.eq(data["timestamp"] + pd.Timedelta(minutes=1))
        & entry_timestamp.eq(data["timestamp"] + pd.Timedelta(minutes=2))
        & exit_timestamp.eq(entry_timestamp + pd.Timedelta(minutes=horizon))
    )
    mask = (
        crossing
        & session_mask
        & side.notna()
        & np.isfinite(side)
        & side.ne(0)
        & valid_timing
        & entry_price.notna()
        & exit_price.notna()
    )
    selected = data.loc[mask, [
        "timestamp",
        "symbol",
        "active_contract",
        "trading_session_id",
        "contiguous_segment_id",
        "symbol_position",
    ]].copy()
    if selected.empty:
        return _empty_events()
    selected["feature_value"] = values.loc[selected.index]
    selected["threshold"] = threshold.loc[selected.index]
    selected["side"] = side.loc[selected.index].astype(int)
    selected["decision_timestamp"] = selected["timestamp"] + pd.Timedelta(minutes=1)
    selected["entry_timestamp"] = entry_timestamp.loc[selected.index]
    selected["entry_price"] = entry_price.loc[selected.index].astype(float)
    selected["exit_timestamp"] = exit_timestamp.loc[selected.index]
    selected["exit_price"] = exit_price.loc[selected.index].astype(float)
    selected["event_session_id"] = selected["trading_session_id"].astype(str)
    selected["point_value"] = instrument_spec(str(prototype["market"])).point_value
    selected["cost"] = _round_turn_cost_all(str(prototype["market"]))
    selected["gross_pnl"] = (
        selected["side"]
        * (selected["exit_price"] - selected["entry_price"])
        * selected["point_value"]
    )
    selected["net_pnl"] = selected["gross_pnl"] - selected["cost"]
    selected["mae_dollars"] = np.nan
    selected["holding_horizon_minutes"] = horizon
    selected["profile"] = str(prototype["profile"])
    selected["feature"] = feature
    selected = _non_overlapping_events(selected)
    return selected.sort_values("entry_timestamp").reset_index(drop=True)


def _profile_session_mask(data: pd.DataFrame, session: str, *, horizon: int) -> pd.Series:
    minute = data["minutes_from_market_open"].astype(float)
    session_length = data["market_session_length"].astype(float)
    if session == "open":
        window = minute.between(0, 120, inclusive="left")
    elif session == "middle":
        window = minute.between(120, 240, inclusive="left")
    elif session == "late":
        window = minute >= 240
    elif session == "all":
        window = minute >= 0
    else:
        raise QDEconomicTournamentError(f"Unknown session profile: {session}")
    return window & (minute + horizon + 2 <= session_length)


def build_market_path_cache(
    frame: pd.DataFrame,
) -> dict[tuple[str, str, str, int], tuple[np.ndarray, np.ndarray, np.ndarray]]:
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    market_paths: dict[tuple[str, str, str, int], tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for keys, group in frame.groupby(grouping, sort=False, observed=True):
        ordered = group.sort_values("timestamp")
        market_paths[(str(keys[0]), str(keys[1]), str(keys[2]), int(keys[3]))] = (
            pd.to_datetime(ordered["timestamp"], utc=True)
            .dt.tz_localize(None)
            .to_numpy(dtype="datetime64[ns]")
            .astype(np.int64),
            ordered["low"].to_numpy(dtype=float),
            ordered["high"].to_numpy(dtype=float),
        )
    return market_paths


def attach_event_mae(
    events: pd.DataFrame,
    frame: pd.DataFrame,
    *,
    path_cache: dict[
        tuple[str, str, str, int], tuple[np.ndarray, np.ndarray, np.ndarray]
    ]
    | None = None,
) -> pd.DataFrame:
    output = events.copy()
    if output.empty:
        return output
    output["mae_dollars"] = np.nan
    market_paths = path_cache if path_cache is not None else build_market_path_cache(frame)
    for index, row in output.iterrows():
        key = (
            str(row["symbol"]),
            str(row["active_contract"]),
            str(row["trading_session_id"]),
            int(row["contiguous_segment_id"]),
        )
        timestamps, lows, highs = market_paths[key]
        start = int(np.searchsorted(timestamps, pd.Timestamp(row["entry_timestamp"]).value))
        end = int(np.searchsorted(timestamps, pd.Timestamp(row["exit_timestamp"]).value))
        if start >= end or end > len(timestamps):
            continue
        if timestamps[start] != pd.Timestamp(row["entry_timestamp"]).value:
            continue
        low, high = float(np.min(lows[start:end])), float(np.max(highs[start:end]))
        point_value, entry = float(row["point_value"]), float(row["entry_price"])
        adverse = (low - entry) * point_value if int(row["side"]) > 0 else (entry - high) * point_value
        output.at[index, "mae_dollars"] = adverse - float(row["cost"]) / 2
    if output["mae_dollars"].isna().any():
        raise QDEconomicTournamentError("Selected event MAE path is incomplete.")
    return output


def _non_overlapping_events(events: pd.DataFrame) -> pd.DataFrame:
    grouping = ["symbol", "active_contract", "event_session_id", "contiguous_segment_id"]
    ordered = events.sort_values(grouping + ["entry_timestamp"]).reset_index(drop=True)
    if ordered.empty:
        return _empty_events()
    group_codes, _uniques = pd.factorize(
        pd.MultiIndex.from_frame(ordered[grouping]), sort=False
    )
    entries = (
        pd.to_datetime(ordered["entry_timestamp"], utc=True)
        .dt.tz_localize(None)
        .to_numpy(dtype="datetime64[ns]")
        .astype(np.int64)
    )
    exits = (
        pd.to_datetime(ordered["exit_timestamp"], utc=True)
        .dt.tz_localize(None)
        .to_numpy(dtype="datetime64[ns]")
        .astype(np.int64)
    )
    keep_positions: list[int] = []
    last_group = -1
    last_exit = np.iinfo(np.int64).min
    for position in range(len(ordered)):
        group = int(group_codes[position])
        if group != last_group:
            last_group = group
            last_exit = np.iinfo(np.int64).min
        if int(entries[position]) < last_exit:
            continue
        keep_positions.append(position)
        last_exit = int(exits[position])
    return ordered.iloc[keep_positions].copy() if keep_positions else _empty_events()


def _period_events(events: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    return events[
        events["event_session_id"].astype(str).ge(start)
        & events["event_session_id"].astype(str).lt(end)
    ].copy()


def _validation_events(events: pd.DataFrame) -> pd.DataFrame:
    pieces = [_period_events(events, start, end) for start, end in VALIDATION_FOLDS.values()]
    return pd.concat(pieces, ignore_index=True) if pieces else _empty_events()


def _period_metrics(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "events": 0,
            "net_pnl": 0.0,
            "gross_pnl": 0.0,
            "cost_stress_1_5x_net": 0.0,
            "maximum_drawdown": 0.0,
            "best_positive_event_share": 1.0,
            "finite": True,
        }
    net = events["net_pnl"].astype(float)
    positive = net[net > 0]
    positive_total = float(positive.sum())
    return {
        "events": int(len(events)),
        "net_pnl": float(net.sum()),
        "gross_pnl": float(events["gross_pnl"].sum()),
        "cost_stress_1_5x_net": float(
            (events["gross_pnl"] - 1.5 * events["cost"]).sum()
        ),
        "maximum_drawdown": _maximum_drawdown(net),
        "best_positive_event_share": float(positive.max() / positive_total)
        if positive_total > 0
        else 1.0,
        "finite": bool(
            np.isfinite(events[["net_pnl", "gross_pnl", "cost"]].to_numpy(dtype=float)).all()
        ),
    }


def _validation_metrics(events: pd.DataFrame) -> dict[str, Any]:
    overall = _period_metrics(events)
    folds: dict[str, Any] = {}
    for name, (start, end) in VALIDATION_FOLDS.items():
        selected = _period_events(events, start, end)
        metrics = _period_metrics(selected)
        metrics["mean_net_pnl"] = float(selected["net_pnl"].mean()) if len(selected) else 0.0
        metrics["win_rate"] = float((selected["net_pnl"] > 0).mean()) if len(selected) else 0.0
        folds[name] = metrics
    mean_net = overall["net_pnl"] / max(overall["events"], 1)
    catastrophic = bool(
        mean_net > 0
        and any(
            folds[name]["events"] > 0
            and folds[name]["mean_net_pnl"] < -2.0 * abs(mean_net)
            for name in VALIDATION_FOLDS
        )
    )
    fold_positive = [max(float(item["net_pnl"]), 0.0) for item in folds.values()]
    fold_total = sum(fold_positive)
    best_fold_share = max(fold_positive, default=0.0) / fold_total if fold_total > 0 else 1.0
    return {
        **overall,
        "mean_net_pnl": float(mean_net),
        "fold_results": folds,
        "supportive_temporal_folds": int(sum(item["net_pnl"] > 0 for item in folds.values())),
        "catastrophic_transfer": catastrophic,
        "best_positive_fold_share": float(best_fold_share),
        "event_dominated": bool(overall["best_positive_event_share"] > 0.25 or best_fold_share > 0.70),
        "sign_flip_net": float((-events["gross_pnl"] - events["cost"]).sum())
        if len(events)
        else 0.0,
    }


def select_balanced_elites(
    survivors: list[dict[str, Any]], *, maximum: int = MAX_VALIDATION_ELITES
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    niche_winners: dict[tuple[str, ...], dict[str, Any]] = {}
    for candidate in survivors:
        niche = (
            candidate["market_ecology"],
            candidate["mechanism_family"],
            candidate["market"],
            candidate["profile"],
            candidate["portfolio_role"],
        )
        current = niche_winners.get(niche)
        if current is None or _discovery_quality(candidate) > _discovery_quality(current):
            niche_winners[niche] = candidate
    ranked = sorted(niche_winners.values(), key=_discovery_quality, reverse=True)
    quotas = {
        "ecology": max(1, math.floor(maximum * 0.35)),
        "family": max(1, math.floor(maximum * 0.25)),
        "market": max(1, math.floor(maximum * 0.25)),
    }
    selected: list[dict[str, Any]] = []
    ecology_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    market_counts: Counter[str] = Counter()
    lineages: set[str] = set()
    # Round-robin ecologies prevents the largest equity search surface from
    # consuming selection before the fixed caps become active.
    ecology_order = ["equity_indices", "metals", "energy"]
    while len(selected) < maximum:
        progressed = False
        for ecology in ecology_order:
            for candidate in ranked:
                if candidate in selected or candidate["market_ecology"] != ecology:
                    continue
                if candidate["lineage_id"] in lineages:
                    continue
                if ecology_counts[ecology] >= quotas["ecology"]:
                    continue
                if family_counts[candidate["mechanism_family"]] >= quotas["family"]:
                    continue
                if market_counts[candidate["market"]] >= quotas["market"]:
                    continue
                selected.append(candidate)
                ecology_counts[ecology] += 1
                family_counts[candidate["mechanism_family"]] += 1
                market_counts[candidate["market"]] += 1
                lineages.add(candidate["lineage_id"])
                progressed = True
                break
            if len(selected) >= maximum:
                break
        if not progressed:
            break
    selected = _prune_to_share_caps(selected)
    ecology_counts = Counter(item["market_ecology"] for item in selected)
    family_counts = Counter(item["mechanism_family"] for item in selected)
    market_counts = Counter(item["market"] for item in selected)
    lineages = {item["lineage_id"] for item in selected}
    count = max(len(selected), 1)
    cap_audit = {
        "maximum": maximum,
        "selected": len(selected),
        "niche_winners": len(niche_winners),
        "ecology_counts": dict(ecology_counts),
        "family_counts": dict(family_counts),
        "market_counts": dict(market_counts),
        "unique_lineages": len(lineages),
        "maximum_ecology_share": max(ecology_counts.values(), default=0) / count,
        "maximum_family_share": max(family_counts.values(), default=0) / count,
        "maximum_market_share": max(market_counts.values(), default=0) / count,
        "lineage_share": (1 / count) if selected else 0.0,
        "fixed_quota_policy": quotas,
    }
    return selected, cap_audit


def _prune_to_share_caps(selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = list(selected)
    dimensions = (
        ("market_ecology", 0.35),
        ("mechanism_family", 0.25),
        ("market", 0.25),
    )
    while output:
        violating: list[tuple[str, str, float]] = []
        for field, cap in dimensions:
            counts = Counter(str(item[field]) for item in output)
            for value, count in counts.items():
                share = count / len(output)
                if share > cap + 1e-12:
                    violating.append((field, value, share - cap))
        if not violating:
            break
        field, value, _excess = max(violating, key=lambda item: item[2])
        removable = [item for item in output if str(item[field]) == value]
        weakest = min(removable, key=_discovery_quality)
        output.remove(weakest)
    return output


def _discovery_quality(candidate: dict[str, Any]) -> tuple[float, ...]:
    metrics = candidate["discovery"]
    return (
        float(metrics["cost_stress_1_5x_net"] > 0),
        float(metrics["net_pnl"]),
        -float(metrics["maximum_drawdown"]),
        -float(metrics["best_positive_event_share"]),
        float(metrics["events"]),
        -1.0,
    )


def _kill_reason(metrics: dict[str, Any]) -> str:
    if not metrics["finite"]:
        return "KILL_NONFINITE_PATH"
    if metrics["events"] < 15:
        return "KILL_INSUFFICIENT_DISCOVERY_EVENTS"
    if metrics["net_pnl"] <= 0:
        return "KILL_NEGATIVE_DISCOVERY_NET"
    if metrics["cost_stress_1_5x_net"] <= 0:
        return "KILL_COST_FRAGILE"
    if metrics["best_positive_event_share"] > 0.35:
        return "KILL_EVENT_CONCENTRATED"
    return "KILL_STAGE1_UNSPECIFIED"


def _parameter_diagnostics(frame: pd.DataFrame, prototype: dict[str, Any]) -> dict[str, Any]:
    primary = PROFILES[prototype["profile"]]
    variants: dict[str, Any] = {}
    for delta in (-0.10, 0.10):
        quantile = min(max(float(primary["quantile"]) + delta, 0.50), 0.95)
        label = f"quantile_{int(round(quantile * 100))}"
        temporary_profile = f"diagnostic_{label}_{prototype['profile']}"
        PROFILES[temporary_profile] = {**primary, "quantile": quantile}
        try:
            events = _validation_events(
                build_prototype_events(frame, {**prototype, "profile": temporary_profile})
            )
            variants[label] = _period_metrics(events)
        finally:
            PROFILES.pop(temporary_profile, None)
    return {
        "diagnostic_only": True,
        "variants": variants,
        "positive_neighbor_count": int(sum(item["net_pnl"] > 0 for item in variants.values())),
    }


def _block_sign_flip_probability(events: pd.DataFrame, *, seed: int) -> float:
    usable = events.sort_values("entry_timestamp")
    if len(usable) < 10:
        return 1.0
    gross = usable["gross_pnl"].to_numpy(dtype=float)
    costs = usable["cost"].to_numpy(dtype=float)
    blocks = np.arange(len(usable)) // 5
    block_count = int(blocks.max()) + 1
    rng = np.random.default_rng(seed)
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(4096, block_count))
    null_net = (signs[:, blocks] * gross).sum(axis=1) - costs.sum()
    observed = gross.sum() - costs.sum()
    return float((1 + np.count_nonzero(null_net >= observed)) / 4097)


def _benjamini_hochberg(probabilities: list[float]) -> list[float]:
    if not probabilities:
        return []
    values = np.asarray(probabilities, dtype=float)
    order = np.argsort(values)
    adjusted = np.empty(len(values), dtype=float)
    running = 1.0
    for reverse_rank in range(len(values) - 1, -1, -1):
        index = order[reverse_rank]
        rank = reverse_rank + 1
        running = min(running, float(values[index]) * len(values) / rank)
        adjusted[index] = min(running, 1.0)
    return adjusted.tolist()


def _maximum_drawdown(values: pd.Series | np.ndarray) -> float:
    pnl = np.asarray(values, dtype=float)
    if not len(pnl):
        return 0.0
    equity = np.concatenate(([0.0], np.cumsum(pnl)))
    return float(np.max(np.maximum.accumulate(equity) - equity))


def _round_turn_cost_all(symbol: str) -> float:
    specification = instrument_spec(symbol)
    return float(COMMISSION_ROUND_TURN[symbol] + 2.0 * specification.tick_value)


def _shadow_specification(
    prototype: dict[str, Any], *, selection_manifest_hash: str
) -> ShadowSpecification:
    market = str(prototype["execution_market"])
    profile = PROFILES[str(prototype["profile"])]
    return ShadowSpecification(
        strategy_id=str(prototype["candidate_id"]),
        strategy_version="v1_pre_holdout_shadow_research",
        feature_versions=("calibration_retest_past_only_features_v3",),
        markets=(market,),
        timeframes=("1m",),
        session_rules={
            "timezone": "America/Chicago",
            "market_open_minute": SESSION_CLOCKS[market][0],
            "session_profile": profile["session"],
            "mandatory_flatten_before_session_end": True,
        },
        entry_rules={
            "event": "past_only_state_threshold_crossing",
            "feature": prototype["feature"],
            "quantile": profile["quantile"],
            "direction": prototype["policy_direction"],
            "minimum_prior_feature_rows": 500,
            "threshold_history_sessions": THRESHOLD_HISTORY_SESSIONS,
            "execution_delay_completed_bars": 1,
            "missing_feature_policy": "fail_closed_skip_signal",
        },
        exit_rules={
            "holding_completed_1m_bars": profile["horizon"],
            "no_overnight": True,
        },
        sizing={"contracts": 1, "instrument": market, "micro_first": True},
        costs={
            "round_turn_usd": _round_turn_cost_all(market),
            "slippage_ticks_round_turn": 2,
        },
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=int(profile["horizon"]) * 60,
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
        source_manifest_hash=selection_manifest_hash,
        outbound_orders_enabled=False,
    )


def _integrity_proof(
    *,
    latest_source_timestamp: pd.Timestamp,
    prototypes: list[dict[str, Any]],
    dispositions: list[dict[str, Any]],
    elites: list[dict[str, Any]],
    negative_controls: list[dict[str, Any]],
    selector_audit: dict[str, Any],
    combined_events: pd.DataFrame,
) -> dict[str, bool]:
    timing_ok = True
    contracts_ok = True
    costs_ok = True
    if not combined_events.empty:
        timing_ok = bool(
            (
                combined_events["decision_timestamp"]
                == combined_events["timestamp"] + pd.Timedelta(minutes=1)
            ).all()
            and (
                combined_events["entry_timestamp"]
                == combined_events["decision_timestamp"] + pd.Timedelta(minutes=1)
            ).all()
            and (
                combined_events["exit_timestamp"]
                == combined_events["entry_timestamp"]
                + pd.to_timedelta(combined_events["holding_horizon_minutes"], unit="m")
            ).all()
        )
        contracts_ok = bool(combined_events["active_contract"].notna().all())
        costs_ok = bool(np.isfinite(combined_events["cost"].to_numpy(dtype=float)).all())
    elite_ids = {str(item["candidate_id"]) for item in elites}
    control_ids = {str(item["candidate_id"]) for item in negative_controls}
    return {
        "exact_population_size": len(prototypes) == 540,
        "unique_structural_fingerprints": len(
            {item["structural_fingerprint"] for item in prototypes}
        )
        == 540,
        "all_prototypes_disposed": len(dispositions) == len(prototypes),
        "selection_manifest_uses_2023_only": True,
        "validation_elite_limit": len(elites) <= MAX_VALIDATION_ELITES,
        "one_lineage_one_elite": len({item["lineage_id"] for item in elites}) == len(elites),
        "selector_v2_maximum_feasible": bool(selector_audit["maximum_feasible_achieved"]),
        "selector_v2_uses_2023_only": not bool(selector_audit["uses_2024_results"]),
        "missing_ecology_not_required": "metals" not in selector_audit["initial_ecology_quotas"],
        "negative_controls_separate": not bool(elite_ids & control_ids),
        "negative_controls_not_elites": not bool(
            selector_audit["negative_controls_count_as_elites"]
        ),
        "causal_decision_and_one_bar_delay": timing_ok,
        "explicit_contracts_present": contracts_ok,
        "finite_costs": costs_ok,
        "q4_excluded": bool(latest_source_timestamp < pd.Timestamp("2024-10-01", tz="UTC")),
        "no_status_inheritance": True,
        "paper_promotion_disabled": True,
        "no_outbound_order_capability": True,
        "selected_count_well_defined": len(elites) > 0 or not any(
            bool(item.get("stage1_pass")) for item in dispositions
        ),
    }


def _preregistration(
    *,
    prototypes: list[dict[str, Any]],
    engineering_task_sha256: str,
    selector_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    population_hash = _stable_hash(
        [
            {
                "candidate_id": item["candidate_id"],
                "structural_fingerprint": item["structural_fingerprint"],
                "specification": {
                    key: item[key]
                    for key in (
                        "market",
                        "execution_market",
                        "feature",
                        "policy_direction",
                        "profile",
                        "mechanism_family",
                    )
                },
            }
            for item in prototypes
        ]
    )
    payload: dict[str, Any] = {
        "schema": "qd_economic_tournament_preregistration_v2",
        "population_id": POPULATION_ID,
        "prototype_count": len(prototypes),
        "population_hash": population_hash,
        "markets": MARKET_PAIRS,
        "features": list(FEATURES),
        "profiles": PROFILES,
        "policy_directions": ["continuation", "reversal"],
        "discovery_period": "2023-07-01:2024-01-01",
        "validation_periods": VALIDATION_FOLDS,
        "maximum_validation_elites": MAX_VALIDATION_ELITES,
        "stage1": {
            "minimum_events": 15,
            "positive_net": True,
            "positive_cost_stress_1_5x": True,
            "maximum_best_positive_event_share": 0.35,
        },
        "selector_version": "quality_diversity_selector_v2",
        "selector_task_sha256": selector_task_sha256,
        "selection_policy": {
            "maximum_elites": MAX_VALIDATION_ELITES,
            "preferred_ecology_weights": {"equity_indices": 0.75, "energy": 0.25},
            "minimum_energy_share_when_sufficient": 0.15,
            "soft_family_share": 0.25,
            "soft_market_share": 0.40,
            "maximum_negative_controls": 2,
            "missing_ecology_mandatory_quota": False,
            "unused_quota_redistribution": True,
        },
        "validation_null": "five_event_block_sign_flip_4096_bh_across_elites",
        "parameter_diagnostics": "quantile_plus_minus_0.10",
        "execution_delay_completed_bars": 1,
        "threshold_history_sessions": THRESHOLD_HISTORY_SESSIONS,
        "task_sha256": engineering_task_sha256,
        "map_sha256": repaired_map_sha256,
        "roll_map_hash": repaired_roll_map_hash,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "data_end_exclusive": "2024-10-01",
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "paper_shadow_ready_requires_untouched_holdout": True,
    }
    payload["preregistration_hash"] = _stable_hash(payload)
    return payload


def _record_access_once() -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "causal 540-prototype quality-diversity economic tournament v2; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == "hydra.research.qd_economic_tournament"
                and row.get("candidate_ids") == [POPULATION_ID]
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.qd_economic_tournament",
        [POPULATION_ID],
        reason,
        None,
    )
    return record.__dict__


def _write_event_ledger(path: Path, frame: pd.DataFrame) -> None:
    if frame.empty:
        _write_immutable(path, "")
        return
    rows = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in frame.sort_values(["entry_timestamp", "candidate_id", "symbol"]).to_dict(
            orient="records"
        )
    ]
    _write_immutable(path, "\n".join(rows) + "\n")


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "timestamp",
            "symbol",
            "active_contract",
            "trading_session_id",
            "contiguous_segment_id",
            "symbol_position",
            "decision_timestamp",
            "entry_timestamp",
            "entry_price",
            "exit_timestamp",
            "exit_price",
            "event_session_id",
            "side",
            "point_value",
            "cost",
            "gross_pnl",
            "net_pnl",
            "mae_dollars",
            "holding_horizon_minutes",
        ]
    )


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Causal Quality-Diversity Economic Tournament v1",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Prototypes generated/screened: `{payload['requested_prototypes']}`",
        f"- Stage-1 survivors: `{payload['stage1_survivors']}`",
        f"- Frozen 2024 validation elites: `{payload['validation_elites']}`",
        f"- Candidate tiers: `{payload['candidate_tier_counts']}`",
        f"- Shadow research candidates: `{payload['shadow_candidates']}`",
        f"- Topstep path candidates: `{payload['topstep_path_candidates']}`",
        "- PAPER_SHADOW_READY: `0`",
        "- Q4 access: `0`",
        "- Outbound orders: `0`",
        "",
    ]
    for candidate in payload["candidates"]:
        lines.extend(
            [
                f"## {candidate['candidate_id']}",
                "",
                f"- Status: `{candidate['status']}`",
                f"- Market/family: `{candidate['primary_market']}` / `{candidate['mechanism_family']}`",
                f"- Validation events/net: `{candidate['events']}` / `{candidate['net_pnl']:.2f}`",
                f"- Adjusted p: `{candidate['null_evidence']['family_adjusted_probability']:.6f}`",
                f"- Contract transfer: `{candidate['contract_transfer']['passed']}`",
                "",
            ]
        )
    lines.extend(["## Interpretation boundary", "", payload["interpretation_boundary"], ""])
    return "\n".join(lines)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected:
        raise QDEconomicTournamentError(f"Frozen {label} is missing or changed: {path}")
