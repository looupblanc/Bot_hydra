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
from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.cross_asset_daily_horizon_primary import (
    TARGETS,
    _bh_adjust,
    _failure,
    _gate,
    _load_tables,
    _micro_execution,
    _period,
    _source_features,
)
from hydra.research.energy_metals_session_geometry_primary import _concentration_stress
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


VERSION = "causal_transition_graph_v1"
STATES = (
    "DOWN_CALM",
    "DOWN_EXPANSION",
    "BALANCED_CALM",
    "BALANCED_EXPANSION",
    "UP_CALM",
    "UP_EXPANSION",
)
SIDES = ("long", "short")
HORIZONS = (60, 120)
EXPECTED_POPULATION = 384
PROMOTION_ALPHA = 0.03
SHADOW_ALPHA = 0.20


class CausalTransitionGraphError(RuntimeError):
    pass


def generate_hypotheses() -> list[dict[str, Any]]:
    hypotheses: list[dict[str, Any]] = []
    for source in TARGETS:
        for target, target_meta in TARGETS.items():
            relation = "same_market" if source == target else "cross_market"
            for state in STATES:
                for side_name in SIDES:
                    for horizon in HORIZONS:
                        specification = {
                            "representation": VERSION,
                            "source_market": source,
                            "target_market": target,
                            "execution_market": target_meta["micro"],
                            "source_state": state,
                            "side_name": side_name,
                            "horizon": horizon,
                            "market_ecology": target_meta["ecology"],
                            "mechanism_family": f"causal_transition_{relation}_state",
                            "portfolio_role": "state_conditioned_alpha",
                            "source_timeframe": "completed_prior_RTH_session",
                            "execution_timeframe": "1m",
                            "state_count": 6,
                        }
                        fingerprint = structural_fingerprint(specification)
                        lineage_specification = {
                            key: value
                            for key, value in specification.items()
                            if key != "horizon"
                        }
                        lineage = structural_fingerprint(lineage_specification)
                        slug = state.lower()
                        candidate_id = (
                            f"strategy_transition_{source}_to_{target}_{slug}_"
                            f"{side_name}_h{horizon}_v1"
                        )
                        hypotheses.append(
                            {
                                **specification,
                                "candidate_id": candidate_id,
                                "structural_fingerprint": fingerprint,
                                "lineage_id": f"lineage_transition_{lineage[:20]}",
                                "market": target,
                            }
                        )
    return sorted(hypotheses, key=lambda row: row["candidate_id"])


def build_source_state_table(table: pd.DataFrame) -> pd.DataFrame:
    ordered = table.sort_values("session_id").reset_index(drop=True)
    features = _source_features(ordered).reset_index(drop=True)
    trend = pd.to_numeric(features["source_prior_trend"], errors="coerce")
    trend_scale = trend.abs().shift(1).rolling(20, min_periods=10).median()
    trend_ratio = trend / trend_scale.replace(0, np.nan)
    prior_range = pd.to_numeric(ordered["prior_range"], errors="coerce")
    range_scale = prior_range.shift(1).rolling(20, min_periods=10).median()
    expansion = prior_range.ge(range_scale)
    direction = np.select(
        [trend_ratio.le(-0.5), trend_ratio.ge(0.5)],
        ["DOWN", "UP"],
        default="BALANCED",
    )
    state = pd.Series(direction, index=ordered.index, dtype="object") + np.where(
        expansion, "_EXPANSION", "_CALM"
    )
    valid = trend_ratio.notna() & range_scale.notna()
    state = state.where(valid)
    result = pd.DataFrame(
        {
            "session_id": ordered["session_id"],
            "source_prior_session_id": features["source_prior_session_id"],
            "source_state": state,
            "source_trend_ratio": trend_ratio,
            "source_prior_range": prior_range,
            "source_past_range_median": range_scale,
            "state_threshold_history_end_session": ordered["session_id"].shift(1),
        }
    )
    if not set(result["source_state"].dropna().unique()).issubset(STATES):
        raise CausalTransitionGraphError("Unexpected source state generated.")
    return result


def build_transition_cache(
    tables: dict[str, pd.DataFrame],
) -> dict[tuple[str, str], pd.DataFrame]:
    source_states = {
        source: build_source_state_table(tables[source]).set_index(
            "session_id", drop=False
        )
        for source in TARGETS
    }
    target_tables = {
        target: tables[target].copy().set_index("session_id", drop=False)
        for target in TARGETS
    }
    cache: dict[tuple[str, str], pd.DataFrame] = {}
    columns = [
        "source_prior_session_id",
        "source_state",
        "source_trend_ratio",
        "source_prior_range",
        "source_past_range_median",
        "state_threshold_history_end_session",
    ]
    for target in TARGETS:
        for source in TARGETS:
            cache[(target, source)] = target_tables[target].join(
                source_states[source][columns], how="left"
            )
    return cache


def build_transition_events(
    tables: dict[str, pd.DataFrame],
    hypothesis: dict[str, Any],
    *,
    cache: dict[tuple[str, str], pd.DataFrame] | None = None,
    state_override: str | None = None,
    horizon_override: int | None = None,
) -> pd.DataFrame:
    target = str(hypothesis["target_market"])
    source = str(hypothesis["source_market"])
    state = str(state_override or hypothesis["source_state"])
    horizon = int(horizon_override or hypothesis["horizon"])
    aligned = (cache or build_transition_cache(tables))[(target, source)]
    required = (
        "overnight_entry_timestamp",
        "overnight_entry_price",
        f"overnight_exit_timestamp_{horizon}",
        f"overnight_exit_{horizon}",
        f"overnight_long_mae_{horizon}",
        f"overnight_short_mae_{horizon}",
    )
    if any(column not in aligned for column in required):
        return pd.DataFrame()
    side = 1.0 if hypothesis["side_name"] == "long" else -1.0
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
            "state_threshold_history_end_session": aligned[
                "state_threshold_history_end_session"
            ],
            "source_symbol": source,
            "symbol": target,
            "active_contract": aligned["active_contract"],
            "source_state": aligned["source_state"],
            "source_trend_ratio": aligned["source_trend_ratio"],
            "side": side,
            "entry_price": aligned[required[1]],
            "exit_price": aligned[required[3]],
        }
    )
    mask = (
        output["source_state"].eq(state)
        & output["entry_timestamp"].notna()
        & output["exit_timestamp"].notna()
        & output["source_prior_session_id"].notna()
        & output["entry_price"].notna()
        & output["exit_price"].notna()
    )
    output = output[mask].copy()
    if not output.empty:
        source_sessions = pd.to_datetime(output["source_prior_session_id"])
        target_sessions = pd.to_datetime(output["trading_session_id"])
        threshold_sessions = pd.to_datetime(
            output["state_threshold_history_end_session"]
        )
        if not (source_sessions < target_sessions).all():
            raise CausalTransitionGraphError(
                "Source state did not precede target decision session."
            )
        if not (threshold_sessions < target_sessions).all():
            raise CausalTransitionGraphError("State threshold used current session.")
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
    return output.reset_index(drop=True)


def transition_edge_statistics(
    events: pd.DataFrame, baseline_events: pd.DataFrame
) -> dict[str, Any]:
    successes = int((events["gross_pnl"] > 0).sum()) if len(events) else 0
    baseline_successes = (
        int((baseline_events["gross_pnl"] > 0).sum()) if len(baseline_events) else 0
    )
    probability = (successes + 1.0) / (len(events) + 2.0)
    baseline_probability = (baseline_successes + 1.0) / (
        len(baseline_events) + 2.0
    )
    return {
        "edge_events": int(len(events)),
        "edge_successes": successes,
        "laplace_success_probability": float(probability),
        "unconditional_events": int(len(baseline_events)),
        "unconditional_laplace_probability": float(baseline_probability),
        "edge_lift": float(probability / max(baseline_probability, 1e-12)),
    }


def validator_controls(seed: int = 991401) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    state = rng.integers(0, 6, size=2400)
    negative = rng.binomial(1, 0.5, size=len(state))
    injected_probability = np.where(state == 2, 0.70, 0.48)
    injected = rng.binomial(1, injected_probability)

    def maximum_lift(outcome: np.ndarray) -> float:
        baseline = (outcome.sum() + 1.0) / (len(outcome) + 2.0)
        return float(
            max(
                ((outcome[state == index].sum() + 1.0) / ((state == index).sum() + 2.0))
                / baseline
                for index in range(6)
            )
        )

    negative_lift = maximum_lift(negative)
    injected_lift = maximum_lift(injected)
    return {
        "negative_control_maximum_state_lift": negative_lift,
        "injected_weak_real_maximum_state_lift": injected_lift,
        "negative_control_passed": negative_lift <= 1.15,
        "injected_control_passed": injected_lift >= 1.25,
        "passed": bool(negative_lift <= 1.15 and injected_lift >= 1.25),
    }


def run_causal_transition_graph(
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
        current = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        if current != code_commit:
            raise CausalTransitionGraphError(
                "Worker commit differs from queued specification."
            )
    core_map = load_roll_map(core_map_path)
    metals_map = load_roll_map(metals_map_path)
    if core_map.roll_map_hash() != core_roll_map_hash:
        raise CausalTransitionGraphError("Core roll map changed.")
    if (
        metals_map.map_type != VOLUME_FRONT_MAP_TYPE
        or metals_map.roll_map_hash() != metals_roll_map_hash
    ):
        raise CausalTransitionGraphError("Metals roll map changed.")
    population = generate_hypotheses()
    if len(population) != EXPECTED_POPULATION or len(
        {row["structural_fingerprint"] for row in population}
    ) != EXPECTED_POPULATION:
        raise CausalTransitionGraphError("Frozen graph population drifted.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration: dict[str, Any] = {
        "schema": VERSION,
        "population_count": EXPECTED_POPULATION,
        "population_hash": _stable_hash(population),
        "states": list(STATES),
        "hypotheses": population,
        "state_definition": {
            "trend_scale_sessions": 20,
            "trend_scale_shift_sessions": 1,
            "direction_boundary": 0.5,
            "range_scale_sessions": 20,
            "range_scale_shift_sessions": 1,
        },
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
    preregistration_path = destination / "transition_graph_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once(population) if record_data_access else None
    controls = validator_controls()
    if not controls["passed"]:
        raise CausalTransitionGraphError("Transition validator controls failed.")

    early_tables, early_provenance = _load_tables(
        Path(core_data_path), core_map, Path(metals_data_path), metals_map, "2024-01-01"
    )
    early_cache = build_transition_cache(early_tables)
    round1_survivors: list[dict[str, Any]] = []
    round2_survivors: list[dict[str, Any]] = []
    failed_controls: list[dict[str, Any]] = []
    dispositions: list[dict[str, Any]] = []
    early_signals: dict[str, pd.DataFrame] = {}
    for hypothesis in population:
        events = build_transition_events(early_tables, hypothesis, cache=early_cache)
        metrics = _period_metrics(_period(events, "2023-01-01", "2023-07-01"))
        row = {**hypothesis, "stage1_pass": _gate(metrics, 8), "discovery": metrics}
        if row["stage1_pass"]:
            round1_survivors.append(row)
            early_signals[hypothesis["candidate_id"]] = events
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
        signals = early_signals[hypothesis["candidate_id"]]
        micro, missing = _micro_execution(signals, early_tables, hypothesis)
        mini_metrics = _period_metrics(_period(signals, "2023-07-01", "2024-01-01"))
        micro_metrics = _period_metrics(_period(micro, "2023-07-01", "2024-01-01"))
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
            "first_half_metrics": hypothesis["discovery"],
            "second_half_mini_metrics": mini_metrics,
            "second_half_micro_metrics": micro_metrics,
            "second_half_missing_rate": missing_rate,
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
            selector_version="causal_transition_graph_qd_selector_v2",
        ),
    )
    elites = [dict(row) for row in selector.elites]
    controls_selected = [dict(row) for row in selector.negative_controls]
    freeze_manifest: dict[str, Any] = {
        "schema": "causal_transition_graph_elite_freeze_v1",
        "population_hash": preregistration["population_hash"],
        "selection_data_end_exclusive": "2024-01-01",
        "selector_audit": selector.audit,
        "elite_count": len(elites),
        "elites": elites,
        "negative_controls": controls_selected,
        "negative_controls_count_as_elites": False,
        "candidate_status_inherited": False,
        "q4_access_allowed": False,
    }
    freeze_manifest["elite_manifest_hash"] = _stable_hash(freeze_manifest)
    freeze_path = destination / "transition_graph_elite_freeze.json"
    _write_immutable(freeze_path, json.dumps(freeze_manifest, indent=2, sort_keys=True) + "\n")
    freeze_seconds = time.perf_counter() - started

    full_tables, confirmation_provenance = _load_tables(
        Path(core_data_path), core_map, Path(metals_data_path), metals_map, "2024-10-01"
    )
    full_cache = build_transition_cache(full_tables)
    validations: list[dict[str, Any]] = []
    ledgers: list[pd.DataFrame] = []
    for index, elite in enumerate(elites):
        signals = build_transition_events(full_tables, elite, cache=full_cache)
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
        probability = _block_sign_flip_probability(confirmation, seed=991501 + index)
        concentration = _concentration_stress(confirmation)
        neighbors = _neighbor_diagnostics(full_tables, elite, full_cache)
        account = _account_replay(
            confirmation.rename(columns={"net_pnl": "net_pnl_60"}).copy()
        )
        missing_rate = len(missing) / max(len(signals), 1)
        baseline_hypothesis = {**elite, "source_state": elite["source_state"]}
        baseline_signals = _unconditional_events(full_cache, baseline_hypothesis)
        edge = transition_edge_statistics(
            _period(signals, "2023-01-01", "2024-01-01"),
            _period(baseline_signals, "2023-01-01", "2024-01-01"),
        )
        validations.append(
            {
                "elite": elite,
                "confirmation": confirmation,
                "development": development_metrics,
                "metrics": metrics,
                "delayed": delayed_metrics,
                "raw_probability": probability,
                "concentration": concentration,
                "neighbors": neighbors,
                "account": account,
                "missing_rate": missing_rate,
                "delayed_missing_count": len(delayed_missing),
                "edge": edge,
            }
        )
        if not confirmation.empty:
            ledgers.append(confirmation.assign(candidate_id=elite["candidate_id"]))
    adjusted = _bh_adjust([row["raw_probability"] for row in validations])
    candidates: list[dict[str, Any]] = []
    shadow_configurations: list[dict[str, Any]] = []
    for row, adjusted_probability in zip(validations, adjusted):
        elite = row["elite"]
        metrics = row["metrics"]
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
            uncertainty="transition_graph_development_confirmation_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise CausalTransitionGraphError("Development result attempted Paper promotion.")
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
            "profile": {
                "source_state": elite["source_state"],
                "side_name": elite["side_name"],
                "horizon": elite["horizon"],
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
            "transition_edge": row["edge"],
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
                "state_threshold_shifted": True,
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
                destination / "shadow_configurations" / f"{elite['candidate_id']}.json"
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
        conclusion = "CAUSAL_TRANSITION_GRAPH_SHADOW_CANDIDATES_FOUND"
        next_action = "ACTIVATE_TRANSITION_GRAPH_ZERO_ORDER_SHADOWS"
    elif promising:
        conclusion = "CAUSAL_TRANSITION_GRAPH_PROMISING_BUT_INSUFFICIENT"
        next_action = "TARGET_TRANSITION_FAILURE_SURFACE_WITH_FRESH_IDS"
    else:
        conclusion = "CAUSAL_TRANSITION_GRAPH_FALSIFIED_OR_INSUFFICIENT"
        next_action = "TARGETED_PROMISING_LINEAGE_MUTATION_REQUIRED"
    trade_path = destination / "transition_graph_trade_ledger.jsonl"
    trade_ledger = pd.concat(ledgers, ignore_index=True) if ledgers else pd.DataFrame()
    _write_ledger(trade_path, trade_ledger)
    integrity = {
        "population_exact_384": len(population) == EXPECTED_POPULATION,
        "unique_fingerprints": len(
            {row["structural_fingerprint"] for row in population}
        ) == EXPECTED_POPULATION,
        "six_states_only": set(STATES) == set(preregistration["states"]),
        "state_thresholds_shifted": True,
        "early_data_end_exclusive": early_provenance["end_exclusive"] == "2024-01-01",
        "elite_freeze_precedes_confirmation": freeze_path.is_file(),
        "selector_uses_no_2024": not selector.audit["uses_2024_results"],
        "maximum_eight_elites": len(elites) <= 8,
        "negative_controls_separate": selector.audit[
            "negative_controls_count_as_elites"
        ] is False,
        "validator_controls_passed": controls["passed"],
        "q4_excluded": True,
        "no_network_or_paid_data": True,
        "no_outbound_order_capability": True,
    }
    if not all(integrity.values()):
        raise CausalTransitionGraphError(f"Integrity failed: {integrity}")
    elapsed = time.perf_counter() - started
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Six-state edges and elites were selected only on 2023 before unchanged "
            "2024 Q1-Q3 replay. No result opens Q4, inherits evidence, authorizes "
            "orders or confers PAPER_SHADOW_READY."
        ),
        "code_commit": code_commit,
        "candidate_count": EXPECTED_POPULATION,
        "structural_prototypes": EXPECTED_POPULATION,
        "round1_survivors": len(round1_survivors),
        "round2_survivors": len(round2_survivors),
        "diagnostic_archive_size": len(round2_survivors),
        "elite_count": len(elites),
        "elite_candidate_ids": [row["candidate_id"] for row in elites],
        "negative_controls": [row["candidate_id"] for row in controls_selected],
        "selector_audit": selector.audit,
        "validator_controls": controls,
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
            "structural_candidates_per_second": EXPECTED_POPULATION / max(elapsed, 1e-9),
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
    result_path = destination / "transition_graph_result.json"
    report_path = destination / "transition_graph_report.md"
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


def _unconditional_events(
    cache: dict[tuple[str, str], pd.DataFrame], hypothesis: dict[str, Any]
) -> pd.DataFrame:
    aligned = cache[(str(hypothesis["target_market"]), str(hypothesis["source_market"]))]
    horizon = int(hypothesis["horizon"])
    side = 1.0 if hypothesis["side_name"] == "long" else -1.0
    point_value = instrument_spec(str(hypothesis["target_market"])).point_value
    entry = pd.to_numeric(aligned["overnight_entry_price"], errors="coerce")
    exit_price = pd.to_numeric(aligned[f"overnight_exit_{horizon}"], errors="coerce")
    output = pd.DataFrame(
        {
            "entry_timestamp": aligned["overnight_entry_timestamp"],
            "gross_pnl": side * (exit_price - entry) * point_value,
        }
    )
    return output.dropna().reset_index(drop=True)


def _neighbor_diagnostics(
    tables: dict[str, pd.DataFrame],
    hypothesis: dict[str, Any],
    cache: dict[tuple[str, str], pd.DataFrame],
) -> dict[str, Any]:
    state = str(hypothesis["source_state"])
    companion = (
        state.replace("_CALM", "_EXPANSION")
        if state.endswith("_CALM")
        else state.replace("_EXPANSION", "_CALM")
    )
    variants: dict[str, Any] = {}
    for label, state_value, horizon in (
        (
            "adjacent_horizon",
            state,
            120 if int(hypothesis["horizon"]) == 60 else 60,
        ),
        ("volatility_companion_state", companion, int(hypothesis["horizon"])),
    ):
        adjusted = {**hypothesis, "horizon": horizon, "source_state": state_value}
        signals = build_transition_events(
            tables,
            adjusted,
            cache=cache,
            state_override=state_value,
            horizon_override=horizon,
        )
        events, _missing = _micro_execution(signals, tables, adjusted)
        variants[label] = _period_metrics(_period(events, "2024-01-01", "2024-10-01"))
    return {
        "diagnostic_only": True,
        "variants": variants,
        "positive_neighbor_count": sum(
            row["net_pnl"] > 0 for row in variants.values()
        ),
    }


def _shadow_specification(
    elite: dict[str, Any], source_manifest_hash: str
) -> ShadowSpecification:
    source = str(elite["source_market"])
    target = str(elite["target_market"])
    execution = str(elite["execution_market"])
    return ShadowSpecification(
        strategy_id=str(elite["candidate_id"]),
        strategy_version="v1_causal_transition_pre_holdout",
        feature_versions=(
            "completed_prior_session_six_state_v1",
            "past_only_shifted_thresholds_v1",
            "exact_micro_execution_v1",
        ),
        markets=tuple(sorted({source, target, execution})),
        timeframes=("1m", "RTH_session", "daily_context"),
        session_rules={
            "timezone": "America/Chicago",
            "source_session_must_precede_target_session": True,
            "mandatory_flatten_before_session_end": True,
        },
        entry_rules={
            "source_state": elite["source_state"],
            "side": elite["side_name"],
            "trend_boundary": 0.5,
            "threshold_history_sessions": 20,
            "threshold_shift_sessions": 1,
            "execution_delay_completed_bars": 1,
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
            "state_threshold_history_mismatch",
            "signal_execution_timestamp_mismatch",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={
            "source_state_ledger": True,
            "transition_edge_ledger": True,
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


def _record_access_once(population: list[dict[str, Any]]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "frozen six-state causal transition graph; Q4 excluded"
    module = "hydra.research.causal_transition_graph"
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
        raise CausalTransitionGraphError(f"Frozen {label} missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Causal Transition Graph Tournament",
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

