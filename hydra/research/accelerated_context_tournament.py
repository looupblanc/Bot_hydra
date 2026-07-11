from __future__ import annotations

import gc
import hashlib
import json
import subprocess
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.data.multitimeframe import resample_closed_bars
from hydra.factory.elite_selection_manifest import (
    build_elite_selection_manifest,
    write_immutable_elite_manifest,
)
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.factory.quality_diversity_selector_v2 import (
    SelectorV2Policy,
    select_quality_diversity_elites_v2,
)
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.foundry.tournament import run_structural_tournament
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _build_past_only_feature_frame,
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
from hydra.research.qd_economic_tournament import (
    FEATURE_FAMILIES,
    FEATURES,
    MARKET_PAIRS,
    PROFILES,
    SYMBOLS,
    VALIDATION_FOLDS,
    _benjamini_hochberg,
    _block_sign_flip_probability,
    _causal_session_thresholds,
    _period_events,
    _period_metrics,
    _prepare_execution_cache,
    _prepare_feature_frame,
    _round_turn_cost_all,
    _shadow_specification,
    _validation_events,
    _validation_metrics,
    attach_event_mae,
    build_market_path_cache,
    build_prototype_events,
    generate_prototypes,
)
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "accelerated_context_tournament_v1"
POPULATION_ID = "accelerated_context_population_20260711_v1"
STAGE0_COUNT = 5000
EXECUTABLE_COUNT = 300
EXECUTABLE_PER_MARKET = 50
CONTEXTS = (
    "none",
    "completed_5m_trend_agree",
    "completed_5m_trend_disagree",
    "completed_15m_trend_agree",
    "completed_15m_trend_disagree",
    "completed_30m_trend_agree",
    "completed_30m_trend_disagree",
    "completed_60m_trend_agree",
    "completed_60m_trend_disagree",
    "completed_15m_volatility_expansion",
)
ROUND1 = ("2023-01-01", "2023-07-01")
ROUND2 = ("2023-07-01", "2024-01-01")


class AcceleratedContextTournamentError(RuntimeError):
    pass


def run_accelerated_context_tournament(
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
    random_seed: int = 773001,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    task = Path(engineering_task_path)
    selector_task = Path(selector_task_path)
    map_path = Path(repaired_map_path)
    _verify(task, engineering_task_sha256, "engineering task")
    _verify(selector_task, selector_task_sha256, "selector task")
    _verify(map_path, repaired_map_sha256, "explicit-contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise AcceleratedContextTournamentError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise AcceleratedContextTournamentError("Worker commit differs from queued specification.")

    stage0 = run_structural_tournament(STAGE0_COUNT)
    executable = generate_executable_hypotheses()
    if (
        stage0["accepted_prototypes"] != STAGE0_COUNT
        or len(executable) != EXECUTABLE_COUNT
        or len({item["structural_fingerprint"] for item in executable}) != EXECUTABLE_COUNT
    ):
        raise AcceleratedContextTournamentError("Frozen population or deduplication drifted.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = _preregistration(
        stage0=stage0,
        executable=executable,
        engineering_task_sha256=engineering_task_sha256,
        selector_task_sha256=selector_task_sha256,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        code_commit=code_commit,
        random_seed=random_seed,
    )
    preregistration_path = destination / "accelerated_context_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    population_path = destination / "accelerated_context_stage0_population.json"
    _write_immutable(
        population_path,
        json.dumps(
            {
                "schema": "accelerated_context_stage0_population_v1",
                "population_sha256": stage0["population_sha256"],
                "prototypes": stage0["prototypes"],
                "executable_population_hash": preregistration["executable_population_hash"],
                "executable_hypotheses": executable,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    access = _record_access_once(executable) if record_data_access else None
    source_preregistration = project_path(
        "reports", "mission_experiments", "calibration_affected_atom_retest_v3_design_v1",
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
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    latest_source_timestamp = pd.to_datetime(raw["timestamp"], utc=True).max()
    features = _prepare_feature_frame(_build_past_only_feature_frame(raw))
    del raw
    gc.collect()
    frames = {
        symbol: _prepare_execution_cache(group.sort_values("timestamp").reset_index(drop=True))
        for symbol, group in features.groupby("symbol", sort=True)
    }
    del features
    gc.collect()
    preparation_seconds = time.perf_counter() - started

    context_cache: dict[tuple[str, int], pd.DataFrame] = {}
    event_cache = _build_hypothesis_event_cache(frames, executable, context_cache)
    round1_rows: list[dict[str, Any]] = []
    round1_survivors: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for hypothesis in executable:
        metrics = _period_metrics(_period_events(event_cache[hypothesis["candidate_id"]], *ROUND1))
        passed = _round_gate(metrics, minimum_events=8, maximum_concentration=0.45)
        row = {
            **hypothesis,
            "discovery": metrics,
            "stage1_pass": passed,
            "stage1_disposition": "ROUND1_SURVIVOR" if passed else _kill_reason(metrics, 8, 0.45),
        }
        round1_rows.append(row)
        (round1_survivors if passed else failed).append(row)

    round2_survivors: list[dict[str, Any]] = []
    round2_rows: list[dict[str, Any]] = []
    for hypothesis in round1_survivors:
        metrics = _period_metrics(_period_events(event_cache[hypothesis["candidate_id"]], *ROUND2))
        passed = _round_gate(metrics, minimum_events=10, maximum_concentration=0.40)
        row = {
            **hypothesis,
            "round1_discovery": hypothesis["discovery"],
            "discovery": metrics,
            "stage1_pass": passed,
            "stage2_disposition": "SELECTOR_V2_ELIGIBLE" if passed else _kill_reason(metrics, 10, 0.40),
        }
        round2_rows.append(row)
        (round2_survivors if passed else failed).append(row)

    selector_result = select_quality_diversity_elites_v2(
        round2_survivors,
        failed_candidates=failed,
        policy=SelectorV2Policy(maximum_elites=20, maximum_controls=2),
    )
    elites = list(selector_result.elites)
    controls = list(selector_result.negative_controls)
    selection_manifest = build_elite_selection_manifest(
        selector_result,
        population_hash=preregistration["executable_population_hash"],
        selector_task_sha256=selector_task_sha256,
    )
    selection_path = destination / "accelerated_context_selection_manifest.json"
    write_immutable_elite_manifest(selection_path, selection_manifest)
    freeze_seconds = time.perf_counter() - started

    validation_rows: list[dict[str, Any]] = []
    event_ledgers: list[pd.DataFrame] = []
    micro_hypotheses = [
        {**item, "market": item["execution_market"], "candidate_id": f"{item['candidate_id']}__micro"}
        for item in elites
    ]
    micro_events = _build_hypothesis_event_cache(frames, micro_hypotheses, context_cache)
    path_caches = {
        symbol: build_market_path_cache(frames[symbol])
        for symbol in {
            value for item in elites for value in (item["market"], item["execution_market"])
        }
    }
    for index, hypothesis in enumerate(elites):
        mini = attach_event_mae(
            _validation_events(event_cache[hypothesis["candidate_id"]]),
            frames[hypothesis["market"]],
            path_cache=path_caches[hypothesis["market"]],
        )
        micro_key = f"{hypothesis['candidate_id']}__micro"
        micro = attach_event_mae(
            _validation_events(micro_events[micro_key]),
            frames[hypothesis["execution_market"]],
            path_cache=path_caches[hypothesis["execution_market"]],
        )
        validation_rows.append(
            {
                "hypothesis": hypothesis,
                "mini": mini,
                "micro": micro,
                "mini_metrics": _validation_metrics(mini),
                "micro_metrics": _validation_metrics(micro),
                "raw_probability": _block_sign_flip_probability(
                    mini, seed=random_seed + index * 1009
                ),
                "parameter_diagnostics": _parameter_diagnostics_with_context(
                    frames[hypothesis["market"]], hypothesis, context_cache
                ),
            }
        )
    adjusted = _benjamini_hochberg(
        [float(item["raw_probability"]) for item in validation_rows]
    )
    candidates: list[dict[str, Any]] = []
    shadow_configs: list[dict[str, Any]] = []
    for index, row in enumerate(validation_rows):
        hypothesis = row["hypothesis"]
        mini, micro = row["mini"], row["micro"]
        mini_metrics, micro_metrics = row["mini_metrics"], row["micro_metrics"]
        diagnostic = row["parameter_diagnostics"]
        parameter_stable = bool(
            diagnostic["positive_neighbor_count"] >= 1
            and mini_metrics["cost_stress_1_5x_net"] > 0
        )
        contract_evidence = bool(
            mini_metrics["net_pnl"] > 0
            and micro_metrics["net_pnl"] > 0
            and micro_metrics["supportive_temporal_folds"] >= 1
        )
        account = _account_replay(
            micro.rename(columns={"net_pnl": "net_pnl_60"}).copy()
        )
        specification = _context_shadow_specification(
            hypothesis, selection_manifest["selection_manifest_hash"]
        )
        specification.validate()
        evidence = ShadowEvidence(
            candidate_id=hypothesis["candidate_id"],
            data_integrity=True,
            no_lookahead=True,
            deterministic_signals=True,
            net_after_costs=float(mini_metrics["net_pnl"]),
            supportive_temporal_folds=int(mini_metrics["supportive_temporal_folds"]),
            catastrophic_transfer=bool(mini_metrics["catastrophic_transfer"]),
            candidate_null_pass=bool(
                adjusted[index] <= 0.20 and mini_metrics["sign_flip_net"] < 0
            ),
            null_probability=float(adjusted[index]),
            parameter_stable=parameter_stable,
            contract_evidence=contract_evidence,
            account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
            execution_possible=True,
            realtime_features_available=True,
            shadow_spec_complete=True,
            observability_complete=True,
            untouched_holdout_passed=False,
            sample_size=int(mini_metrics["events"]),
            uncertainty="successive_halving_development_only_q4_unopened",
        )
        admission = decide_shadow_admission(evidence)
        if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
            raise AcceleratedContextTournamentError("Development tournament attempted paper promotion.")
        candidate = {
            "candidate_id": hypothesis["candidate_id"],
            "lineage_id": hypothesis["lineage_id"],
            "structural_fingerprint": hypothesis["structural_fingerprint"],
            "parent_structure_fingerprint": hypothesis["parent_structure_fingerprint"],
            "mechanism_family": hypothesis["mechanism_family"],
            "primary_market": hypothesis["market"],
            "execution_market": hypothesis["execution_market"],
            "market_ecology": hypothesis["market_ecology"],
            "portfolio_role": hypothesis["portfolio_role"],
            "feature": hypothesis["feature"],
            "profile": hypothesis["profile"],
            "activation_context": hypothesis["activation_context"],
            "status": admission.tier.value,
            "admission": admission.to_dict(),
            "events": int(mini_metrics["events"]),
            "net_pnl": float(mini_metrics["net_pnl"]),
            "micro_events": int(micro_metrics["events"]),
            "micro_net_pnl": float(micro_metrics["net_pnl"]),
            "supportive_temporal_folds": int(mini_metrics["supportive_temporal_folds"]),
            "fold_results": mini_metrics["fold_results"],
            "micro_fold_results": micro_metrics["fold_results"],
            "round1_evidence": hypothesis.get("round1_discovery"),
            "round2_evidence": hypothesis["discovery"],
            "null_evidence": {
                "method": "five_event_block_sign_flip_bh_across_frozen_elites",
                "raw_probability": float(row["raw_probability"]),
                "family_adjusted_probability": float(adjusted[index]),
                "validation_elite_count": len(validation_rows),
            },
            "parameter_diagnostics": diagnostic,
            "cost_stress_1_5x_net": float(mini_metrics["cost_stress_1_5x_net"]),
            "contract_transfer": {
                "mini": hypothesis["market"],
                "micro": hypothesis["execution_market"],
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
                "completed_higher_timeframe_only": True,
            },
            "topstep": account,
            "shadow_evidence": evidence.__dict__,
        }
        candidates.append(candidate)
        for events, role in ((mini, "mini"), (micro, "micro")):
            if not events.empty:
                logged = events.copy()
                logged["candidate_id"] = hypothesis["candidate_id"]
                logged["contract_role"] = role
                event_ledgers.append(logged)
        if admission.permits_zero_risk_shadow:
            path = specification.write_immutable(
                destination / "shadow_configurations" / f"{hypothesis['candidate_id']}.json"
            )
            shadow_configs.append(
                {
                    "candidate_id": hypothesis["candidate_id"],
                    "status": admission.tier.value,
                    "path": str(path),
                    "configuration_hash": specification.configuration_hash,
                    "outbound_orders_enabled": False,
                }
            )

    control_results = []
    for control in controls:
        control_events = _validation_events(event_cache.get(control["candidate_id"], pd.DataFrame()))
        control_results.append(
            {
                "candidate_id": control["candidate_id"],
                "promotion_eligible": False,
                "validation_metrics": _validation_metrics(control_events)
                if not control_events.empty
                else _period_metrics(control_events),
            }
        )
    statuses = [item["status"] for item in candidates]
    promising_tiers = {
        EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
        EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
    }
    promising = sum(status in promising_tiers for status in statuses)
    shadow_count = statuses.count(EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value)
    topstep_count = sum(bool(item["topstep"].get("path_candidate")) for item in candidates)
    combined = pd.concat(event_ledgers, ignore_index=True) if event_ledgers else pd.DataFrame()
    ledger_path = destination / "accelerated_context_trade_ledger.jsonl"
    _write_ledger(ledger_path, combined)
    integrity = _integrity(
        stage0=stage0,
        executable=executable,
        elites=elites,
        controls=controls,
        selector_audit=selector_result.audit,
        combined_events=combined,
        latest_source_timestamp=latest_source_timestamp,
    )
    if not all(integrity.values()):
        raise AcceleratedContextTournamentError(f"Integrity proof failed: {integrity}")
    if shadow_count:
        conclusion = "ACCELERATED_CONTEXT_TOURNAMENT_SHADOW_CANDIDATES_FOUND"
        next_action = "FREEZE_AND_ACTIVATE_DISTINCT_CONTEXT_SHADOW_CANDIDATES"
    elif promising:
        conclusion = "ACCELERATED_CONTEXT_TOURNAMENT_PROMISING_BUT_INSUFFICIENT"
        next_action = "TARGETED_CONFIRMATION_OR_NEW_REPRESENTATION_FROM_FAILURE_MAP"
    else:
        conclusion = "ACCELERATED_CONTEXT_TOURNAMENT_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_COUNTERFACTUAL_HAZARD_AND_META_RESEARCH_ALLOCATION"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Five thousand structures were generated but only the frozen 300-hypothesis executable "
            "lane was replayed. Selection used 2023 only before 2024 Q1-Q3 development replay. "
            "Q4 is sealed, no status is inherited and PAPER_SHADOW_READY is impossible here."
        ),
        "code_commit": code_commit,
        "candidate_count": STAGE0_COUNT,
        "requested_prototypes": STAGE0_COUNT,
        "structural_prototypes": STAGE0_COUNT,
        "executable_hypotheses": EXECUTABLE_COUNT,
        "round1_survivors": len(round1_survivors),
        "round2_survivors": len(round2_survivors),
        "validation_elites": len(elites),
        "negative_controls": control_results,
        "candidate_tier_counts": dict(Counter(statuses)),
        "candidates": candidates,
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow_count),
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(topstep_count),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "quality_diversity_archive": {
            "stage0": stage0["archive"],
            "stage0_allocations": stage0["allocations"],
            "selector_v2_audit": selector_result.audit,
            "selected_ids": selection_manifest["selected_candidate_ids"],
            "ecology_counts": dict(Counter(item["market_ecology"] for item in elites)),
            "family_counts": dict(Counter(item["mechanism_family"] for item in elites)),
            "context_counts": dict(Counter(item["activation_context"] for item in elites)),
        },
        "mechanism_families": sorted({item["mechanism_family"] for item in executable}),
        "market_ecologies": ["equity_indices", "metals", "energy", "relative_value_stage0"],
        "timeframe_profiles": ["1m", "1m+5m", "1m+15m", "1m+30m", "1m+60m"],
        "shadow_configurations": shadow_configs,
        "integrity_proof": integrity,
        "data_provenance": provenance,
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "selection_manifest_path": str(selection_path),
        "selection_manifest_hash": selection_manifest["selection_manifest_hash"],
        "performance": {
            "feature_preparation_seconds": preparation_seconds,
            "selection_freeze_seconds": freeze_seconds,
            "total_seconds": time.perf_counter() - started,
            "structural_prototypes_per_second": STAGE0_COUNT / max(time.perf_counter() - started, 1e-9),
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
    result_path = destination / "accelerated_context_result.json"
    report_path = destination / "accelerated_context_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "population_path": str(population_path),
            "selection_manifest_path": str(selection_path),
            "trade_ledger_path": str(ledger_path),
        },
        "report_path": str(report_path),
    }


def generate_executable_hypotheses(batch_index: int = 0) -> list[dict[str, Any]]:
    if batch_index < 0:
        raise ValueError("Batch index must be non-negative.")
    bases = generate_prototypes()
    selected: list[dict[str, Any]] = []
    candidate_version = batch_index + 2
    for market in MARKET_PAIRS:
        possibilities = []
        for base in bases:
            if base["market"] != market:
                continue
            for context in CONTEXTS:
                specification = {
                    key: base[key]
                    for key in (
                        "market", "execution_market", "feature", "policy_direction", "profile",
                        "mechanism_family", "market_ecology", "portfolio_role",
                    )
                }
                specification.update(
                    {
                        "activation_context": context,
                        "context_availability": "completed_bar_at_or_before_decision",
                        "execution_delay": "one_completed_1m_bar",
                        "contract_policy": "explicit_roll_aware",
                    }
                )
                fingerprint = structural_fingerprint(specification)
                context_label = context.removeprefix("completed_")
                possibilities.append(
                    {
                        **specification,
                        "candidate_id": (
                            f"strategy_accel_{market}_{base['feature']}_{base['policy_direction']}_"
                            f"{base['profile']}_{context_label}_v{candidate_version}"
                        ),
                        "lineage_id": f"lineage_accel_{fingerprint[:20]}",
                        "structural_fingerprint": fingerprint,
                        "parent_structure_fingerprint": base["structural_fingerprint"],
                    }
                )
        possibilities.sort(key=lambda item: (item["structural_fingerprint"], item["candidate_id"]))
        families = sorted({str(item["mechanism_family"]) for item in possibilities})
        per_family = EXECUTABLE_PER_MARKET // len(families)
        family_offset = batch_index * per_family
        market_selected = [
            item
            for family in families
            for item in [
                row for row in possibilities if row["mechanism_family"] == family
            ][family_offset : family_offset + per_family]
        ]
        if len(market_selected) < EXECUTABLE_PER_MARKET:
            chosen = {item["candidate_id"] for item in market_selected}
            market_selected.extend(
                item
                for item in possibilities
                if item["candidate_id"] not in chosen
            )
        selected.extend(market_selected[:EXECUTABLE_PER_MARKET])
    return sorted(selected, key=lambda item: (item["market"], item["candidate_id"]))


def _build_hypothesis_event_cache(
    frames: dict[str, pd.DataFrame],
    hypotheses: list[dict[str, Any]],
    context_cache: dict[tuple[str, int], pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
    for market in sorted({str(item["market"]) for item in hypotheses}):
        frame = frames[market]
        market_hypotheses = [item for item in hypotheses if item["market"] == market]
        quantiles = sorted({float(PROFILES[item["profile"]]["quantile"]) for item in market_hypotheses})
        thresholds = {
            feature: _causal_session_thresholds(
                frame[feature], frame["trading_session_id"], quantiles
            )
            for feature in sorted({str(item["feature"]) for item in market_hypotheses})
        }
        base_cache: dict[tuple[str, str, str], pd.DataFrame] = {}
        for hypothesis in market_hypotheses:
            base_key = (
                str(hypothesis["feature"]),
                str(hypothesis["policy_direction"]),
                str(hypothesis["profile"]),
            )
            if base_key not in base_cache:
                quantile = float(PROFILES[hypothesis["profile"]]["quantile"])
                base_cache[base_key] = build_prototype_events(
                    frame,
                    hypothesis,
                    threshold_override=thresholds[hypothesis["feature"]][quantile],
                )
            output[hypothesis["candidate_id"]] = _apply_context(
                base_cache[base_key],
                frame,
                str(hypothesis["activation_context"]),
                context_cache,
            )
    return output


def _apply_context(
    events: pd.DataFrame,
    frame: pd.DataFrame,
    context: str,
    cache: dict[tuple[str, int], pd.DataFrame],
) -> pd.DataFrame:
    output = events.copy()
    if output.empty:
        output["context_availability_timestamp"] = pd.NaT
        output["activation_context"] = context
        return output
    if context == "none":
        output["context_availability_timestamp"] = output["timestamp"]
        output["activation_context"] = context
        return output
    minutes = int(context.split("m_", 1)[0].removeprefix("completed_"))
    symbol = str(output["symbol"].iloc[0])
    key = (symbol, minutes)
    if key not in cache:
        cache[key] = _context_bars(frame, minutes)
    pieces = []
    for contract, group in output.groupby("active_contract", sort=True):
        right = cache[key].loc[cache[key]["active_contract"].eq(contract)]
        if right.empty:
            continue
        joined = pd.merge_asof(
            group.sort_values("decision_timestamp"),
            right.sort_values("availability_timestamp"),
            left_on="decision_timestamp",
            right_on="availability_timestamp",
            direction="backward",
            allow_exact_matches=True,
            suffixes=("", "_context"),
        )
        pieces.append(joined)
    if not pieces:
        return output.iloc[0:0].assign(
            context_availability_timestamp=pd.NaT, activation_context=context
        )
    joined = pd.concat(pieces, ignore_index=True)
    available = joined["availability_timestamp"].notna()
    if not (
        joined.loc[available, "availability_timestamp"]
        <= joined.loc[available, "decision_timestamp"]
    ).all():
        raise AcceleratedContextTournamentError("Incomplete higher-timeframe bar joined.")
    if context.endswith("trend_agree"):
        mask = np.sign(joined["context_return"]) == joined["side"]
    elif context.endswith("trend_disagree"):
        mask = (
            np.sign(joined["context_return"]).ne(0)
            & (np.sign(joined["context_return"]) == -joined["side"])
        )
    elif context.endswith("volatility_expansion"):
        mask = joined["context_volatility_expansion"].fillna(False)
    else:
        raise AcceleratedContextTournamentError(f"Unknown context: {context}")
    selected = joined.loc[available & mask].copy()
    selected["context_availability_timestamp"] = selected.pop("availability_timestamp")
    selected["activation_context"] = context
    drop = [column for column in ("context_return", "context_volatility_expansion") if column in selected]
    return selected.drop(columns=drop).sort_values("entry_timestamp").reset_index(drop=True)


def _context_bars(frame: pd.DataFrame, minutes: int) -> pd.DataFrame:
    bars = resample_closed_bars(
        frame[["timestamp", "symbol", "active_contract", "open", "high", "low", "close", "volume"]],
        minutes,
    )
    groups = bars.groupby(["symbol", "active_contract"], sort=False, observed=True)
    previous_close = groups["close"].shift(1)
    bars["context_return"] = bars["close"] / previous_close - 1.0
    bar_range = (bars["high"] - bars["low"]) / previous_close.abs()
    threshold = bar_range.groupby(
        [bars["symbol"], bars["active_contract"]], sort=False
    ).transform(lambda values: values.rolling(20, min_periods=10).quantile(0.75).shift(1))
    bars["context_volatility_expansion"] = bar_range > threshold
    return bars[
        ["active_contract", "availability_timestamp", "context_return", "context_volatility_expansion"]
    ].sort_values(["active_contract", "availability_timestamp"]).reset_index(drop=True)


def _parameter_diagnostics_with_context(
    frame: pd.DataFrame,
    hypothesis: dict[str, Any],
    context_cache: dict[tuple[str, int], pd.DataFrame],
) -> dict[str, Any]:
    primary = PROFILES[hypothesis["profile"]]
    variants = {}
    for delta in (-0.10, 0.10):
        quantile = min(max(float(primary["quantile"]) + delta, 0.50), 0.95)
        label = f"quantile_{int(round(quantile * 100))}"
        temporary = f"accelerated_diagnostic_{label}_{hypothesis['profile']}"
        PROFILES[temporary] = {**primary, "quantile": quantile}
        try:
            thresholds = _causal_session_thresholds(
                frame[hypothesis["feature"]], frame["trading_session_id"], [quantile]
            )[quantile]
            events = build_prototype_events(
                frame, {**hypothesis, "profile": temporary}, threshold_override=thresholds
            )
            filtered = _apply_context(
                events, frame, hypothesis["activation_context"], context_cache
            )
            variants[label] = _period_metrics(_validation_events(filtered))
        finally:
            PROFILES.pop(temporary, None)
    return {
        "diagnostic_only": True,
        "variants": variants,
        "positive_neighbor_count": int(sum(item["net_pnl"] > 0 for item in variants.values())),
    }


def _context_shadow_specification(
    hypothesis: dict[str, Any], selection_manifest_hash: str
):
    base = _shadow_specification(
        hypothesis, selection_manifest_hash=selection_manifest_hash
    )
    context = str(hypothesis["activation_context"])
    timeframes = ("1m",) if context == "none" else ("1m", f"{_context_minutes(context)}m")
    entry_rules = {
        **base.entry_rules,
        "activation_context": context,
        "higher_timeframe_availability": "completed_bar_at_or_before_decision",
        "missing_context_policy": "fail_closed_skip_signal",
    }
    return replace(
        base,
        strategy_version="v2_accelerated_pre_holdout_shadow_research",
        feature_versions=("calibration_retest_past_only_features_v3", "closed_bar_context_v1"),
        timeframes=timeframes,
        entry_rules=entry_rules,
    )


def _context_minutes(context: str) -> int:
    if context == "none":
        return 1
    return int(context.split("m_", 1)[0].removeprefix("completed_"))


def _round_gate(metrics: dict[str, Any], *, minimum_events: int, maximum_concentration: float) -> bool:
    return bool(
        metrics["events"] >= minimum_events
        and metrics["net_pnl"] > 0
        and metrics["cost_stress_1_5x_net"] > 0
        and metrics["best_positive_event_share"] <= maximum_concentration
        and metrics["finite"]
    )


def _kill_reason(metrics: dict[str, Any], minimum_events: int, maximum_concentration: float) -> str:
    if metrics["events"] < minimum_events:
        return "INSUFFICIENT_SAMPLES"
    if metrics["net_pnl"] <= 0:
        return "NEGATIVE_ECONOMICS"
    if metrics["cost_stress_1_5x_net"] <= 0:
        return "COST_FRAGILITY"
    if metrics["best_positive_event_share"] > maximum_concentration:
        return "CONCENTRATION"
    if not metrics["finite"]:
        return "NONFINITE_PATH"
    return "UNSPECIFIED_FAILURE"


def _preregistration(
    *,
    stage0: dict[str, Any],
    executable: list[dict[str, Any]],
    engineering_task_sha256: str,
    selector_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload = {
        "schema": "accelerated_context_preregistration_v1",
        "population_id": POPULATION_ID,
        "stage0_count": STAGE0_COUNT,
        "stage0_population_hash": stage0["population_sha256"],
        "executable_count": EXECUTABLE_COUNT,
        "executable_population_hash": _stable_hash(
            [(item["candidate_id"], item["structural_fingerprint"]) for item in executable]
        ),
        "contexts": CONTEXTS,
        "round1": {"period": ROUND1, "minimum_events": 8, "maximum_concentration": 0.45},
        "round2": {"period": ROUND2, "minimum_events": 10, "maximum_concentration": 0.40},
        "validation_folds": VALIDATION_FOLDS,
        "selector_version": "quality_diversity_selector_v2",
        "maximum_validation_elites": 20,
        "engineering_task_sha256": engineering_task_sha256,
        "selector_task_sha256": selector_task_sha256,
        "repaired_map_sha256": repaired_map_sha256,
        "repaired_roll_map_hash": repaired_roll_map_hash,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "paper_shadow_ready_allowed": False,
    }
    payload["preregistration_hash"] = _stable_hash(payload)
    return payload


def _record_access_once(hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_ids = [item["candidate_id"] for item in hypotheses]
    period = "2023-01-01:2024-10-01"
    reason = "accelerated 5000-structure context tournament bounded executable lane; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.accelerated_context_tournament"
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.accelerated_context_tournament",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _integrity(
    *,
    stage0: dict[str, Any],
    executable: list[dict[str, Any]],
    elites: list[dict[str, Any]],
    controls: list[dict[str, Any]],
    selector_audit: dict[str, Any],
    combined_events: pd.DataFrame,
    latest_source_timestamp: pd.Timestamp,
) -> dict[str, bool]:
    context_timing = True
    execution_timing = True
    if not combined_events.empty:
        contextual = combined_events["activation_context"].ne("none")
        context_timing = bool(
            combined_events.loc[contextual, "context_availability_timestamp"].notna().all()
            and (
                combined_events.loc[contextual, "context_availability_timestamp"]
                <= combined_events.loc[contextual, "decision_timestamp"]
            ).all()
        )
        execution_timing = bool(
            (
                combined_events["decision_timestamp"]
                == combined_events["timestamp"] + pd.Timedelta(minutes=1)
            ).all()
            and (
                combined_events["entry_timestamp"]
                == combined_events["decision_timestamp"] + pd.Timedelta(minutes=1)
            ).all()
        )
    elite_ids = {item["candidate_id"] for item in elites}
    control_ids = {item["candidate_id"] for item in controls}
    return {
        "exact_stage0_population": stage0["accepted_prototypes"] == STAGE0_COUNT,
        "exact_unique_executable_population": len(executable) == EXECUTABLE_COUNT
        and len({item["structural_fingerprint"] for item in executable}) == EXECUTABLE_COUNT,
        "balanced_50_per_market": all(
            sum(item["market"] == market for item in executable) == EXECUTABLE_PER_MARKET
            for market in MARKET_PAIRS
        ),
        "selector_uses_2023_only": not bool(selector_audit["uses_2024_results"]),
        "selector_maximum_feasible": bool(selector_audit["maximum_feasible_achieved"]),
        "negative_controls_separate": not bool(elite_ids & control_ids),
        "one_lineage_one_elite": len({item["lineage_id"] for item in elites}) == len(elites),
        "completed_context_only": context_timing,
        "one_bar_execution_delay": execution_timing,
        "explicit_contracts": combined_events.empty
        or bool(combined_events["active_contract"].notna().all()),
        "q4_excluded": bool(latest_source_timestamp < pd.Timestamp("2024-10-01", tz="UTC")),
        "no_status_inheritance": True,
        "paper_promotion_disabled": True,
        "no_outbound_order_capability": True,
    }


def _write_ledger(path: Path, events: pd.DataFrame) -> None:
    if events.empty:
        _write_immutable(path, "")
        return
    rows = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in events.sort_values(["entry_timestamp", "candidate_id", "symbol"]).to_dict("records")
    ]
    _write_immutable(path, "\n".join(rows) + "\n")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise AcceleratedContextTournamentError(f"Frozen {label} is missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Accelerated Context Tournament v1",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Stage-0 structures: `{payload['structural_prototypes']}`",
            f"- Executable hypotheses: `{payload['executable_hypotheses']}`",
            f"- Round-1 survivors: `{payload['round1_survivors']}`",
            f"- Round-2 survivors: `{payload['round2_survivors']}`",
            f"- Frozen validation elites: `{payload['validation_elites']}`",
            f"- Promising / shadow: `{payload['promising_candidates']}` / `{payload['shadow_candidates']}`",
            f"- Topstep path: `{payload['topstep_path_candidates']}`",
            "- PAPER_SHADOW_READY: `0`",
            "- Q4 access: `0`",
            "- Outbound orders: `0`",
            "",
            "## Interpretation boundary",
            "",
            str(payload["interpretation_boundary"]),
            "",
        ]
    )
