from __future__ import annotations

import gc
import hashlib
import json
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_volume_front import VOLUME_FRONT_MAP_TYPE
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _build_past_only_feature_frame,
    _stable_hash,
    _strict_json_value,
)
from hydra.research.barrier_hazard_primary import (
    EARLY_ROUND_1,
    EARLY_ROUND_2,
    FEATURE_FAMILIES,
    FEATURES,
    PRIMARY_ALPHA,
    RECIPES,
    SIGNED_FEATURES,
    _build_archive_and_primary_ranking,
    _build_barrier_event_sets,
    _early_failure_reason,
    _parameter_diagnostics,
    _shadow_specification,
    add_barrier_features,
    barrier_hazard_metrics,
    build_barrier_path_cache,
)
from hydra.research.equity_open_gap_reversal import _account_replay, _write_immutable
from hydra.research.qd_economic_tournament import (
    _period_events,
    _period_metrics,
    _prepare_execution_cache,
    _prepare_feature_frame,
    _validation_events,
    _validation_metrics,
)
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "energy_metals_barrier_primary_v1"
MARKET_PAIRS = {"CL": "MCL", "GC": "MGC"}
SYMBOLS = ("CL", "MCL", "GC", "MGC")


class EnergyMetalsBarrierPrimaryError(RuntimeError):
    pass


def generate_energy_metals_hypotheses() -> list[dict[str, Any]]:
    population: list[dict[str, Any]] = []
    for market, execution_market in MARKET_PAIRS.items():
        ecology = "energy" if market == "CL" else "metals"
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
                            f"strategy_energy_metals_barrier_{market}_{feature}_"
                            f"{recipe['name']}_v1"
                        ),
                        "lineage_id": f"lineage_energy_metals_{fingerprint[:20]}",
                        "structural_fingerprint": fingerprint,
                        "portfolio_role": (
                            "reversal"
                            if recipe["policy_direction"] == "reversal"
                            else "trend"
                        ),
                    }
                )
    return sorted(population, key=lambda item: item["candidate_id"])


def run_energy_metals_barrier_primary(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    energy_data_path: str | Path,
    energy_data_sha256: str,
    energy_map_path: str | Path,
    energy_map_sha256: str,
    energy_roll_map_hash: str,
    metals_data_path: str | Path,
    metals_data_sha256: str,
    metals_map_path: str | Path,
    metals_map_sha256: str,
    metals_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    contracts = (
        (Path(engineering_task_path), engineering_task_sha256, "engineering task"),
        (Path(energy_data_path), energy_data_sha256, "energy data"),
        (Path(energy_map_path), energy_map_sha256, "energy map"),
        (Path(metals_data_path), metals_data_sha256, "metals data"),
        (Path(metals_map_path), metals_map_sha256, "metals map"),
    )
    for path, expected, label in contracts:
        _verify(path, expected, label)
    energy_map = load_roll_map(energy_map_path)
    metals_map = load_roll_map(metals_map_path)
    if energy_map.roll_map_hash() != energy_roll_map_hash:
        raise EnergyMetalsBarrierPrimaryError("Energy roll-map hash changed.")
    if (
        metals_map.map_type != VOLUME_FRONT_MAP_TYPE
        or metals_map.roll_map_hash() != metals_roll_map_hash
    ):
        raise EnergyMetalsBarrierPrimaryError("Metals volume-front map changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise EnergyMetalsBarrierPrimaryError(
                "Worker commit differs from queued specification."
            )
    hypotheses = generate_energy_metals_hypotheses()
    if len(hypotheses) != 48 or len(
        {item["structural_fingerprint"] for item in hypotheses}
    ) != 48:
        raise EnergyMetalsBarrierPrimaryError("Frozen population drifted.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = {
        "schema": VERSION,
        "population_count": len(hypotheses),
        "population_hash": _stable_hash(hypotheses),
        "hypotheses": hypotheses,
        "early_rounds": [EARLY_ROUND_1, EARLY_ROUND_2],
        "confirmation_folds": ["2024_q1", "2024_q2", "2024_q3"],
        "promotion_primary_count": 1,
        "primary_alpha": PRIMARY_ALPHA,
        "shadow_support_threshold": 0.20,
        "data_hashes": {
            "energy": energy_data_sha256,
            "energy_map": energy_map_sha256,
            "metals": metals_data_sha256,
            "metals_map": metals_map_sha256,
        },
        "engineering_task_sha256": engineering_task_sha256,
        "code_commit": code_commit,
        "q4_access_allowed": False,
        "network_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "energy_metals_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once(hypotheses) if record_data_access else None

    early_frames, early_provenance = _load_feature_frames(
        energy_data_path=Path(energy_data_path),
        energy_map=energy_map,
        metals_data_path=Path(metals_data_path),
        metals_map=metals_map,
        end_exclusive="2024-01-01",
    )
    if any(
        pd.to_datetime(frame["timestamp"], utc=True).max()
        >= pd.Timestamp("2024-01-01", tz="UTC")
        for frame in early_frames.values()
        if not frame.empty
    ):
        raise EnergyMetalsBarrierPrimaryError("Early selection exposed 2024.")
    early_context_cache: dict[tuple[str, int], pd.DataFrame] = {}
    early_path_caches = {
        symbol: build_barrier_path_cache(early_frames[symbol]) for symbol in SYMBOLS
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
        direct, hazard = _period_metrics(events), barrier_hazard_metrics(events)
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
        direct, hazard = _period_metrics(events), barrier_hazard_metrics(events)
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
    primary_manifest = {
        "schema": "energy_metals_single_primary_freeze_v1",
        "population_hash": preregistration["population_hash"],
        "archive_candidate_ids": [item["candidate_id"] for item in archive],
        "ranking": ranking,
        "primary": primary,
        "primary_candidate_id": primary_id,
        "selection_data_end_exclusive": "2024-01-01",
        "promotion_primary_count": int(primary is not None),
        "diagnostics_inherit_status": False,
        "q4_access_allowed": False,
    }
    primary_manifest["primary_manifest_hash"] = _stable_hash(primary_manifest)
    primary_manifest_path = destination / "energy_metals_primary_freeze.json"
    _write_immutable(
        primary_manifest_path,
        json.dumps(primary_manifest, indent=2, sort_keys=True) + "\n",
    )
    freeze_seconds = time.perf_counter() - started
    del early_events, early_micro_events, early_frames
    gc.collect()

    candidates: list[dict[str, Any]] = []
    shadow_configurations: list[dict[str, Any]] = []
    trade_ledger = pd.DataFrame()
    full_provenance: dict[str, Any] | None = None
    if primary is not None:
        full_frames, full_provenance = _load_feature_frames(
            energy_data_path=Path(energy_data_path),
            energy_map=energy_map,
            metals_data_path=Path(metals_data_path),
            metals_map=metals_map,
            end_exclusive="2024-10-01",
        )
        contexts: dict[tuple[str, int], pd.DataFrame] = {}
        needed = {str(primary["market"]), str(primary["execution_market"])}
        path_caches = {
            symbol: build_barrier_path_cache(full_frames[symbol]) for symbol in needed
        }
        mini_events = _build_barrier_event_sets(
            full_frames, [primary], contexts, path_caches
        )[primary["candidate_id"]]
        validation_mini = _validation_events(mini_events)
        micro_hypothesis = {
            **primary,
            "market": primary["execution_market"],
            "candidate_id": f"{primary['candidate_id']}__micro",
        }
        micro_events = _build_barrier_event_sets(
            full_frames, [micro_hypothesis], contexts, path_caches
        )[micro_hypothesis["candidate_id"]]
        validation_micro = _validation_events(micro_events)
        mini_metrics = _validation_metrics(validation_mini)
        micro_metrics = _validation_metrics(validation_micro)
        hazard = barrier_hazard_metrics(validation_mini)
        micro_hazard = barrier_hazard_metrics(validation_micro)
        parameter = _parameter_diagnostics(
            full_frames, primary, contexts, path_caches
        )
        delayed = _build_barrier_event_sets(
            full_frames,
            [primary],
            contexts,
            path_caches,
            entry_delay_bars=1,
        )[primary["candidate_id"]]
        delay_metrics = _validation_metrics(_validation_events(delayed))
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
            uncertainty="energy_metals_development_confirmation_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise EnergyMetalsBarrierPrimaryError(
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
                "raw_probability": float(hazard["exact_probability"]),
                "prospective_alpha": PRIMARY_ALPHA,
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
            config_path = specification.write_immutable(
                destination
                / "shadow_configurations"
                / f"{primary['candidate_id']}.json"
            )
            shadow_configurations.append(
                {
                    "candidate_id": primary["candidate_id"],
                    "status": admission.tier.value,
                    "path": str(config_path),
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
    if shadow_count:
        conclusion = "ENERGY_METALS_BARRIER_SHADOW_CANDIDATE_FOUND"
        next_action = "ACTIVATE_ZERO_ORDER_SHADOW_AND_REPLICATE"
    elif promising:
        conclusion = "ENERGY_METALS_BARRIER_PROMISING_BUT_INSUFFICIENT"
        next_action = "FRESH_ID_ENERGY_METALS_REPLICATION"
    elif primary is None:
        conclusion = "ENERGY_METALS_BARRIER_NO_EARLY_PRIMARY"
        next_action = "PIVOT_ENERGY_METALS_SESSION_DISTRIBUTION"
    else:
        conclusion = "ENERGY_METALS_BARRIER_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
        next_action = "KILL_EXACT_PRIMARY_AND_PIVOT_ECOLOGY_REPRESENTATION"
    trade_path = destination / "energy_metals_trade_ledger.jsonl"
    _write_ledger(trade_path, trade_ledger)
    integrity = {
        "population_exact_48": len(hypotheses) == 48,
        "fresh_unique_fingerprints": len(
            {item["structural_fingerprint"] for item in hypotheses}
        )
        == 48,
        "only_energy_metals": {item["market"] for item in hypotheses}
        == {"CL", "GC"},
        "early_selection_pre_2024": early_provenance["end_exclusive"]
        == "2024-01-01",
        "maximum_one_primary": int(primary is not None) <= 1,
        "primary_manifest_written_before_confirmation": primary_manifest_path.is_file(),
        "diagnostics_inherit_no_status": True,
        "q4_excluded": True,
        "no_network_or_paid_data": True,
        "no_outbound_order_capability": True,
    }
    if not all(integrity.values()):
        raise EnergyMetalsBarrierPrimaryError(f"Integrity failed: {integrity}")
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "One primary at most was frozen from 2023 before unchanged 2024 Q1-Q3. "
            "No archive member inherited evidence; Q4 and PAPER_SHADOW_READY remain prohibited."
        ),
        "code_commit": code_commit,
        "candidate_count": len(hypotheses),
        "structural_prototypes": len(hypotheses),
        "round1_survivors": len(round1_survivors),
        "round2_survivors": len(round2_survivors),
        "diagnostic_archive_size": len(archive),
        "promotion_primary_count": int(primary is not None),
        "primary_candidate_id": primary_id,
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
        "data_provenance": {
            "early": early_provenance,
            "confirmation": full_provenance,
        },
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "primary_manifest_path": str(primary_manifest_path),
        "primary_manifest_hash": primary_manifest["primary_manifest_hash"],
        "performance": {
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
    result_path = destination / "energy_metals_result.json"
    report_path = destination / "energy_metals_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "primary_manifest_path": str(primary_manifest_path),
            "trade_ledger_path": str(trade_path),
        },
        "report_path": str(report_path),
    }


def _load_feature_frames(
    *,
    energy_data_path: Path,
    energy_map: Any,
    metals_data_path: Path,
    metals_map: Any,
    end_exclusive: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    energy = _read_period(energy_data_path, {"CL", "MCL"}, end_exclusive)
    metals = _read_period(metals_data_path, {"GC", "MGC"}, end_exclusive)
    energy, energy_details = _apply_explicit_contract_map(
        energy, energy_map, required_map_type=energy_map.map_type
    )
    metals, metals_details = _apply_explicit_contract_map(
        metals, metals_map, required_map_type=VOLUME_FRONT_MAP_TYPE
    )
    combined = pd.concat([energy, metals], ignore_index=True).sort_values(
        ["symbol", "timestamp"]
    )
    if set(combined["symbol"].astype(str).unique()) != set(SYMBOLS):
        raise EnergyMetalsBarrierPrimaryError("Required ecology symbols are incomplete.")
    features = add_barrier_features(
        _prepare_feature_frame(_build_past_only_feature_frame(combined))
    )
    frames = {
        symbol: _prepare_execution_cache(
            group.sort_values("timestamp").reset_index(drop=True)
        )
        for symbol, group in features.groupby("symbol", sort=True)
    }
    return frames, {
        "period_start": "2023-01-01",
        "end_exclusive": end_exclusive,
        "rows_after_guards": int(len(combined)),
        "rows_by_symbol": {
            str(symbol): int(count)
            for symbol, count in combined.groupby("symbol").size().items()
        },
        "energy_contract_details": energy_details,
        "metals_contract_details": metals_details,
    }


def _read_period(path: Path, symbols: set[str], end_exclusive: str) -> pd.DataFrame:
    try:
        frame = pd.read_parquet(
            path,
            filters=[
                ("symbol", "in", sorted(symbols)),
                ("timestamp", ">=", pd.Timestamp("2023-01-01", tz="UTC")),
                ("timestamp", "<", pd.Timestamp(end_exclusive, tz="UTC")),
            ],
        )
    except Exception:
        frame = pd.read_parquet(path)
        timestamp = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame[
            frame["symbol"].astype(str).isin(symbols)
            & timestamp.ge("2023-01-01")
            & timestamp.lt(end_exclusive)
        ].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    if frame.empty or frame["timestamp"].max() >= pd.Timestamp(
        end_exclusive, tz="UTC"
    ):
        raise EnergyMetalsBarrierPrimaryError(f"Invalid period load from {path}.")
    return frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _record_access_once(hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_ids = [item["candidate_id"] for item in hypotheses]
    period = "2023-01-01:2024-10-01"
    reason = "energy/metals volume-front barrier single-primary; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.energy_metals_barrier_primary"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.energy_metals_barrier_primary",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _write_ledger(path: Path, frame: pd.DataFrame) -> None:
    if frame.empty:
        _write_immutable(path, "")
        return
    ordered = frame.sort_values(
        [column for column in ("entry_timestamp", "symbol", "contract_role") if column in frame]
    )
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in ordered.to_dict("records")
    ]
    _write_immutable(path, "\n".join(lines) + "\n")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise EnergyMetalsBarrierPrimaryError(
            f"Frozen {label} missing or changed: {path}"
        )


def _render_report(payload: dict[str, Any]) -> str:
    primary = next(iter(payload.get("candidates") or []), None)
    lines = [
        "# Energy/Metals Barrier Primary",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Structural prototypes: `{payload['structural_prototypes']}`",
        f"- Round 1 survivors: `{payload['round1_survivors']}`",
        f"- Round 2 survivors: `{payload['round2_survivors']}`",
        f"- Frozen primary: `{payload['primary_candidate_id']}`",
        f"- Shadow candidates: `{payload['shadow_candidates']}`",
        f"- PAPER_SHADOW_READY: `{payload['paper_shadow_ready']}`",
        "- Q4 access delta: `0`",
    ]
    if primary:
        lines.extend(
            [
                f"- Confirmation events: `{primary['events']}`",
                f"- Mini net: `{primary['net_pnl']}`",
                f"- Micro net: `{primary['micro_net_pnl']}`",
                f"- Exact null p: `{primary['null_evidence']['raw_probability']}`",
                f"- Classification: `{primary['status']}`",
            ]
        )
    return "\n".join(lines) + "\n"
