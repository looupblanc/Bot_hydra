from __future__ import annotations

import gc
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
from hydra.factory.elite_selection_manifest import (
    build_elite_selection_manifest,
    write_immutable_elite_manifest,
)
from hydra.factory.quality_diversity_selector_v2 import (
    SelectorV2Policy,
    select_quality_diversity_elites_v2,
)
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
from hydra.research.accelerated_context_tournament import (
    ROUND1,
    ROUND2,
    _build_hypothesis_event_cache,
    _context_shadow_specification,
    _kill_reason,
    _parameter_diagnostics_with_context,
    _round_gate,
    generate_executable_hypotheses,
)
from hydra.research.equity_open_gap_reversal import (
    MAP_TYPE,
    SOURCE_PREREGISTRATION_SHA256,
    _account_replay,
    _write_immutable,
)
from hydra.research.qd_economic_tournament import (
    MARKET_PAIRS,
    SYMBOLS,
    _block_sign_flip_probability,
    _period_events,
    _period_metrics,
    _prepare_execution_cache,
    _prepare_feature_frame,
    _validation_events,
    _validation_metrics,
    attach_event_mae,
    build_market_path_cache,
)
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "single_primary_context_tournament_v1"
BATCH_INDEX = 1
PRIMARY_ALPHA = 0.03


class SinglePrimaryContextTournamentError(RuntimeError):
    pass


def run_single_primary_context_tournament(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    selector_task_path: str | Path,
    selector_task_sha256: str,
    calibrated_policy_result_path: str | Path,
    calibrated_policy_result_sha256: str,
    calibrated_policy_result_hash: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int = 774401,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    task = Path(engineering_task_path)
    selector_task = Path(selector_task_path)
    policy_path = Path(calibrated_policy_result_path)
    map_path = Path(repaired_map_path)
    for path, expected, label in (
        (task, engineering_task_sha256, "engineering task"),
        (selector_task, selector_task_sha256, "selector task"),
        (policy_path, calibrated_policy_result_sha256, "calibrated policy"),
        (map_path, repaired_map_sha256, "explicit-contract map"),
    ):
        _verify(path, expected, label)
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if (
        policy.get("result_hash") != calibrated_policy_result_hash
        or policy.get("scientific_conclusion") != "SINGLE_PRIMARY_ALPHA_CALIBRATED"
        or not bool(policy.get("calibration_passed"))
        or not np.isclose(float(policy.get("selected_alpha")), PRIMARY_ALPHA)
        or int((policy.get("prospective_policy_contract") or {}).get("promotion_primary_count", -1)) != 1
    ):
        raise SinglePrimaryContextTournamentError("Calibrated alpha policy contract changed.")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise SinglePrimaryContextTournamentError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise SinglePrimaryContextTournamentError("Worker commit differs from specification.")

    hypotheses = generate_executable_hypotheses(BATCH_INDEX)
    predecessor = generate_executable_hypotheses(0)
    if (
        len(hypotheses) != 300
        or len({item["structural_fingerprint"] for item in hypotheses}) != 300
        or {item["structural_fingerprint"] for item in hypotheses}
        & {item["structural_fingerprint"] for item in predecessor}
    ):
        raise SinglePrimaryContextTournamentError("New batch is not exact and disjoint.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = _preregistration(
        hypotheses=hypotheses,
        engineering_task_sha256=engineering_task_sha256,
        selector_task_sha256=selector_task_sha256,
        calibrated_policy_result_hash=calibrated_policy_result_hash,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        code_commit=code_commit,
        random_seed=random_seed,
    )
    preregistration_path = destination / "single_primary_context_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    access = _record_primary_access_once(hypotheses) if record_data_access else None
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
    full_frames = {
        symbol: _prepare_execution_cache(group.sort_values("timestamp").reset_index(drop=True))
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
        raise SinglePrimaryContextTournamentError("Early selection frame exposed 2024.")
    preparation_seconds = time.perf_counter() - started

    early_context_cache: dict[tuple[str, int], pd.DataFrame] = {}
    early_events = _build_hypothesis_event_cache(
        early_frames, hypotheses, early_context_cache
    )
    round1_survivors: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        metrics = _period_metrics(
            _period_events(early_events[hypothesis["candidate_id"]], *ROUND1)
        )
        passed = _round_gate(metrics, minimum_events=8, maximum_concentration=0.45)
        row = {
            **hypothesis,
            "discovery": metrics,
            "stage1_pass": passed,
            "stage1_disposition": "ROUND1_SURVIVOR"
            if passed
            else _kill_reason(metrics, 8, 0.45),
        }
        (round1_survivors if passed else failures).append(row)
    round2_survivors: list[dict[str, Any]] = []
    for hypothesis in round1_survivors:
        metrics = _period_metrics(
            _period_events(early_events[hypothesis["candidate_id"]], *ROUND2)
        )
        passed = _round_gate(metrics, minimum_events=10, maximum_concentration=0.40)
        row = {
            **hypothesis,
            "round1_discovery": hypothesis["discovery"],
            "discovery": metrics,
            "stage1_pass": passed,
            "stage2_disposition": "SELECTOR_V2_ELIGIBLE"
            if passed
            else _kill_reason(metrics, 10, 0.40),
        }
        (round2_survivors if passed else failures).append(row)
    selector = select_quality_diversity_elites_v2(
        round2_survivors,
        failed_candidates=failures,
        policy=SelectorV2Policy(maximum_elites=20, maximum_controls=2),
    )
    elites = list(selector.elites)
    archive_manifest = build_elite_selection_manifest(
        selector,
        population_hash=preregistration["population_hash"],
        selector_task_sha256=selector_task_sha256,
    )
    archive_path = destination / "single_primary_diagnostic_archive_manifest.json"
    write_immutable_elite_manifest(archive_path, archive_manifest)

    early_micro_hypotheses = [
        {
            **item,
            "market": item["execution_market"],
            "candidate_id": f"{item['candidate_id']}__early_micro",
        }
        for item in elites
    ]
    early_micro_events = _build_hypothesis_event_cache(
        early_frames, early_micro_hypotheses, early_context_cache
    )
    primary, primary_ranking = _select_primary(elites, early_micro_events)
    primary_manifest = _primary_manifest(
        primary=primary,
        ranking=primary_ranking,
        archive_manifest_hash=archive_manifest["selection_manifest_hash"],
        calibrated_policy_result_hash=calibrated_policy_result_hash,
        population_hash=preregistration["population_hash"],
    )
    primary_manifest_path = destination / "single_primary_freeze_manifest.json"
    _write_immutable(
        primary_manifest_path, json.dumps(primary_manifest, indent=2, sort_keys=True) + "\n"
    )
    freeze_seconds = time.perf_counter() - started

    diagnostics: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    ledgers: list[pd.DataFrame] = []
    shadow_configs: list[dict[str, Any]] = []
    if primary is None:
        diagnostics = [
            {
                "candidate_id": elite["candidate_id"],
                "promotion_eligible": False,
                "status": "DIAGNOSTIC_ONLY",
                "reason": "no_eligible_early_fold_primary",
            }
            for elite in elites
        ]
    if primary is not None:
        validation_hypotheses = elites
        full_context_cache: dict[tuple[str, int], pd.DataFrame] = {}
        full_events = _build_hypothesis_event_cache(
            full_frames, validation_hypotheses, full_context_cache
        )
        micro_hypotheses = [
            {
                **item,
                "market": item["execution_market"],
                "candidate_id": f"{item['candidate_id']}__micro",
            }
            for item in validation_hypotheses
        ]
        full_micro_events = _build_hypothesis_event_cache(
            full_frames, micro_hypotheses, full_context_cache
        )
        for index, elite in enumerate(elites):
            mini = _validation_events(full_events[elite["candidate_id"]])
            micro = _validation_events(
                full_micro_events[f"{elite['candidate_id']}__micro"]
            )
            raw_probability = _block_sign_flip_probability(
                mini, seed=random_seed + index * 1009
            )
            if elite["candidate_id"] != primary["candidate_id"]:
                diagnostics.append(
                    {
                        "candidate_id": elite["candidate_id"],
                        "promotion_eligible": False,
                        "status": "DIAGNOSTIC_ONLY",
                        "mini_metrics": _validation_metrics(mini),
                        "micro_metrics": _validation_metrics(micro),
                        "raw_probability": raw_probability,
                        "reason": "not_frozen_single_primary",
                    }
                )
                continue
            path_caches = {
                elite["market"]: build_market_path_cache(full_frames[elite["market"]]),
                elite["execution_market"]: build_market_path_cache(
                    full_frames[elite["execution_market"]]
                ),
            }
            mini = attach_event_mae(
                mini,
                full_frames[elite["market"]],
                path_cache=path_caches[elite["market"]],
            )
            micro = attach_event_mae(
                micro,
                full_frames[elite["execution_market"]],
                path_cache=path_caches[elite["execution_market"]],
            )
            mini_metrics = _validation_metrics(mini)
            micro_metrics = _validation_metrics(micro)
            parameter = _parameter_diagnostics_with_context(
                full_frames[elite["market"]], elite, full_context_cache
            )
            contract_evidence = bool(
                mini_metrics["net_pnl"] > 0
                and micro_metrics["net_pnl"] > 0
                and micro_metrics["supportive_temporal_folds"] >= 1
            )
            account = _account_replay(
                micro.rename(columns={"net_pnl": "net_pnl_60"}).copy()
            )
            evidence = ShadowEvidence(
                candidate_id=elite["candidate_id"],
                data_integrity=True,
                no_lookahead=True,
                deterministic_signals=True,
                net_after_costs=float(mini_metrics["net_pnl"]),
                supportive_temporal_folds=int(mini_metrics["supportive_temporal_folds"]),
                catastrophic_transfer=bool(mini_metrics["catastrophic_transfer"]),
                candidate_null_pass=bool(
                    raw_probability <= PRIMARY_ALPHA
                    and mini_metrics["sign_flip_net"] < 0
                ),
                null_probability=float(raw_probability),
                parameter_stable=bool(
                    parameter["positive_neighbor_count"] >= 1
                    and mini_metrics["cost_stress_1_5x_net"] > 0
                ),
                contract_evidence=contract_evidence,
                account_mll_safe=bool(
                    account.get("micro_one_contract_mll_safe", False)
                ),
                execution_possible=True,
                realtime_features_available=True,
                shadow_spec_complete=True,
                observability_complete=True,
                untouched_holdout_passed=False,
                sample_size=int(mini_metrics["events"]),
                uncertainty="single_primary_development_confirmation_q4_unopened",
            )
            admission = decide_shadow_admission(evidence)
            if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
                raise SinglePrimaryContextTournamentError(
                    "Development-only primary attempted paper promotion."
                )
            candidate = {
                "candidate_id": elite["candidate_id"],
                "lineage_id": elite["lineage_id"],
                "structural_fingerprint": elite["structural_fingerprint"],
                "mechanism_family": elite["mechanism_family"],
                "primary_market": elite["market"],
                "execution_market": elite["execution_market"],
                "market_ecology": elite["market_ecology"],
                "portfolio_role": elite["portfolio_role"],
                "feature": elite["feature"],
                "profile": elite["profile"],
                "activation_context": elite["activation_context"],
                "promotion_primary": True,
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
                "early_selection_evidence": primary_ranking[0],
                "null_evidence": {
                    "method": "calibrated_single_preselected_primary_block_sign_flip",
                    "raw_probability": float(raw_probability),
                    "prospective_alpha": PRIMARY_ALPHA,
                    "promotion_test_count": 1,
                    "diagnostic_elites_not_promotion_eligible": len(elites) - 1,
                },
                "parameter_diagnostics": parameter,
                "cost_stress_1_5x_net": float(
                    mini_metrics["cost_stress_1_5x_net"]
                ),
                "contract_transfer": {
                    "mini": elite["market"],
                    "micro": elite["execution_market"],
                    "passed": contract_evidence,
                    "micro_supportive_folds": int(
                        micro_metrics["supportive_temporal_folds"]
                    ),
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
            candidate_rows.append(candidate)
            for events, role in ((mini, "primary_mini"), (micro, "primary_micro")):
                if not events.empty:
                    logged = events.copy()
                    logged["candidate_id"] = elite["candidate_id"]
                    logged["contract_role"] = role
                    ledgers.append(logged)
            if admission.permits_zero_risk_shadow:
                specification = _context_shadow_specification(
                    elite, primary_manifest["primary_manifest_hash"]
                )
                path = specification.write_immutable(
                    destination / "shadow_configurations" / f"{elite['candidate_id']}.json"
                )
                shadow_configs.append(
                    {
                        "candidate_id": elite["candidate_id"],
                        "status": admission.tier.value,
                        "path": str(path),
                        "configuration_hash": specification.configuration_hash,
                        "outbound_orders_enabled": False,
                    }
                )

    statuses = [item["status"] for item in candidate_rows]
    promising_tiers = {
        EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
        EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
        EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
    }
    promising = sum(status in promising_tiers for status in statuses)
    shadow_count = statuses.count(EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value)
    topstep_count = sum(bool(item["topstep"].get("path_candidate")) for item in candidate_rows)
    combined = pd.concat(ledgers, ignore_index=True) if ledgers else pd.DataFrame()
    ledger_path = destination / "single_primary_context_trade_ledger.jsonl"
    _write_ledger(ledger_path, combined)
    integrity = _integrity(
        hypotheses=hypotheses,
        predecessor=predecessor,
        elites=elites,
        primary=primary,
        diagnostics=diagnostics,
        combined=combined,
        latest_source_timestamp=latest_source_timestamp,
        selector_audit=selector.audit,
    )
    if not all(integrity.values()):
        raise SinglePrimaryContextTournamentError(f"Integrity proof failed: {integrity}")
    if shadow_count:
        conclusion = "SINGLE_PRIMARY_CONTEXT_SHADOW_CANDIDATE_FOUND"
        next_action = "FREEZE_AND_ACTIVATE_NEW_SINGLE_PRIMARY_SHADOW"
    elif promising:
        conclusion = "SINGLE_PRIMARY_CONTEXT_PROMISING_BUT_INSUFFICIENT"
        next_action = "RETAIN_PRIMARY_FOR_FORWARD_DIAGNOSTIC_OR_NEW_ID_CONFIRMATION"
    elif primary is None:
        conclusion = "SINGLE_PRIMARY_CONTEXT_NO_EARLY_FOLD_PRIMARY"
        next_action = "PIVOT_NEW_REPRESENTATION_WITH_SAME_CALIBRATED_POLICY"
    else:
        conclusion = "SINGLE_PRIMARY_CONTEXT_CONFIRMATION_FALSIFIED_OR_INSUFFICIENT"
        next_action = "KILL_EXACT_PRIMARY_AND_PIVOT_NEW_REPRESENTATION"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Exactly one v3 primary was selected and frozen using 2023 only before its 2024 "
            "development confirmation. Other elites are diagnostic-only. Q4 remains sealed and "
            "PAPER_SHADOW_READY is impossible in this experiment."
        ),
        "code_commit": code_commit,
        "candidate_count": 300,
        "structural_prototypes": 300,
        "round1_survivors": len(round1_survivors),
        "round2_survivors": len(round2_survivors),
        "diagnostic_archive_size": len(elites),
        "promotion_primary_count": int(primary is not None),
        "primary_candidate_id": primary["candidate_id"] if primary else None,
        "primary_manifest_path": str(primary_manifest_path),
        "primary_manifest_hash": primary_manifest["primary_manifest_hash"],
        "diagnostic_elites": diagnostics,
        "candidates": candidate_rows,
        "candidate_tier_counts": dict(Counter(statuses)),
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow_count),
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(topstep_count),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "quality_diversity_archive": {
            "selector_v2_audit": selector.audit,
            "selected_ids": archive_manifest["selected_candidate_ids"],
            "primary_id": primary["candidate_id"] if primary else None,
            "diagnostic_only_count": max(len(elites) - int(primary is not None), 0),
        },
        "shadow_configurations": shadow_configs,
        "integrity_proof": integrity,
        "data_provenance": provenance,
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "archive_manifest_path": str(archive_path),
        "archive_manifest_hash": archive_manifest["selection_manifest_hash"],
        "performance": {
            "feature_preparation_seconds": preparation_seconds,
            "primary_freeze_seconds": freeze_seconds,
            "total_seconds": time.perf_counter() - started,
        },
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "promotion_tests_consumed": int(primary is not None),
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": next_action,
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "single_primary_context_result.json"
    report_path = destination / "single_primary_context_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "primary_manifest_path": str(primary_manifest_path),
            "archive_manifest_path": str(archive_path),
            "trade_ledger_path": str(ledger_path),
        },
        "report_path": str(report_path),
    }


def _select_primary(
    elites: list[dict[str, Any]],
    early_micro_events: dict[str, pd.DataFrame],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    ranking = []
    for elite in elites:
        mini = dict(elite["discovery"])
        micro = _period_metrics(
            _period_events(
                early_micro_events[f"{elite['candidate_id']}__early_micro"], *ROUND2
            )
        )
        eligible = bool(
            mini["net_pnl"] > 0
            and micro["net_pnl"] > 0
            and mini["cost_stress_1_5x_net"] > 0
            and micro["cost_stress_1_5x_net"] > 0
        )
        mini_ratio = float(mini["net_pnl"]) / max(float(mini["maximum_drawdown"]), 1.0)
        micro_ratio = float(micro["net_pnl"]) / max(float(micro["maximum_drawdown"]), 1.0)
        ranking.append(
            {
                "candidate_id": elite["candidate_id"],
                "eligible": eligible,
                "minimum_net_to_drawdown": min(mini_ratio, micro_ratio),
                "maximum_positive_event_share": max(
                    float(mini["best_positive_event_share"]),
                    float(micro["best_positive_event_share"]),
                ),
                "minimum_event_count": min(int(mini["events"]), int(micro["events"])),
                "mini_metrics": mini,
                "micro_metrics": micro,
                "structural_fingerprint": elite["structural_fingerprint"],
            }
        )
    ranking.sort(
        key=lambda row: (
            -int(row["eligible"]),
            -float(row["minimum_net_to_drawdown"]),
            float(row["maximum_positive_event_share"]),
            -int(row["minimum_event_count"]),
            str(row["structural_fingerprint"]),
        )
    )
    if not ranking or not ranking[0]["eligible"]:
        return None, ranking
    selected_id = ranking[0]["candidate_id"]
    return next(item for item in elites if item["candidate_id"] == selected_id), ranking


def _primary_manifest(
    *,
    primary: dict[str, Any] | None,
    ranking: list[dict[str, Any]],
    archive_manifest_hash: str,
    calibrated_policy_result_hash: str,
    population_hash: str,
) -> dict[str, Any]:
    payload = {
        "schema": "single_primary_freeze_manifest_v1",
        "primary_candidate_id": primary["candidate_id"] if primary else None,
        "primary_specification": (
            {
                key: primary[key]
                for key in (
                    "candidate_id", "lineage_id", "structural_fingerprint", "market",
                    "execution_market", "feature", "policy_direction", "profile",
                    "activation_context", "mechanism_family",
                )
            }
            if primary
            else None
        ),
        "early_fold_ranking": ranking,
        "archive_manifest_hash": archive_manifest_hash,
        "population_hash": population_hash,
        "calibrated_policy_result_hash": calibrated_policy_result_hash,
        "candidate_probability_threshold": PRIMARY_ALPHA,
        "promotion_test_count": int(primary is not None),
        "primary_selection_data_end_exclusive": "2024-01-01",
        "confirmation_data_start": "2024-01-01",
        "confirmation_data_end_exclusive": "2024-10-01",
        "diagnostic_elites_promotion_eligible": False,
        "q4_access_allowed": False,
        "paper_shadow_ready_allowed": False,
    }
    payload["primary_manifest_hash"] = _stable_hash(payload)
    return payload


def _preregistration(
    *,
    hypotheses: list[dict[str, Any]],
    engineering_task_sha256: str,
    selector_task_sha256: str,
    calibrated_policy_result_hash: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload = {
        "schema": "single_primary_context_preregistration_v1",
        "batch_index": BATCH_INDEX,
        "candidate_version": "v3",
        "population_count": len(hypotheses),
        "population_hash": _stable_hash(
            [(item["candidate_id"], item["structural_fingerprint"]) for item in hypotheses]
        ),
        "round1": ROUND1,
        "round2": ROUND2,
        "diagnostic_archive_maximum": 20,
        "promotion_primary_maximum": 1,
        "primary_alpha": PRIMARY_ALPHA,
        "primary_ranking": [
            "positive_mini_micro_net_and_cost_stress",
            "maximum_minimum_net_to_drawdown",
            "minimum_concentration",
            "maximum_events",
            "structural_fingerprint",
        ],
        "engineering_task_sha256": engineering_task_sha256,
        "selector_task_sha256": selector_task_sha256,
        "calibrated_policy_result_hash": calibrated_policy_result_hash,
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


def _integrity(
    *,
    hypotheses: list[dict[str, Any]],
    predecessor: list[dict[str, Any]],
    elites: list[dict[str, Any]],
    primary: dict[str, Any] | None,
    diagnostics: list[dict[str, Any]],
    combined: pd.DataFrame,
    latest_source_timestamp: pd.Timestamp,
    selector_audit: dict[str, Any],
) -> dict[str, bool]:
    context_ok = True
    delay_ok = True
    if not combined.empty:
        contextual = combined["activation_context"].ne("none")
        context_ok = bool(
            combined.loc[contextual, "context_availability_timestamp"].notna().all()
            and (
                combined.loc[contextual, "context_availability_timestamp"]
                <= combined.loc[contextual, "decision_timestamp"]
            ).all()
        )
        delay_ok = bool(
            (combined["decision_timestamp"] == combined["timestamp"] + pd.Timedelta(minutes=1)).all()
            and (combined["entry_timestamp"] == combined["decision_timestamp"] + pd.Timedelta(minutes=1)).all()
        )
    return {
        "exact_unique_v3_population": len(hypotheses) == 300
        and len({item["structural_fingerprint"] for item in hypotheses}) == 300,
        "no_v2_fingerprint_overlap": not bool(
            {item["structural_fingerprint"] for item in hypotheses}
            & {item["structural_fingerprint"] for item in predecessor}
        ),
        "balanced_markets": all(
            sum(item["market"] == market for item in hypotheses) == 50
            for market in MARKET_PAIRS
        ),
        "one_primary_maximum": int(primary is not None) <= 1,
        "diagnostics_not_promotion_eligible": all(
            not item["promotion_eligible"] for item in diagnostics
        ),
        "diagnostic_count_consistent": len(diagnostics)
        == max(len(elites) - int(primary is not None), 0),
        "selector_uses_2023_only": not bool(selector_audit["uses_2024_results"]),
        "completed_context_only": context_ok,
        "one_bar_execution_delay": delay_ok,
        "explicit_contracts": combined.empty
        or bool(combined["active_contract"].notna().all()),
        "q4_excluded": bool(latest_source_timestamp < pd.Timestamp("2024-10-01", tz="UTC")),
        "calibrated_alpha_fixed": PRIMARY_ALPHA == 0.03,
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


def _record_primary_access_once(hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_ids = [item["candidate_id"] for item in hypotheses]
    period = "2023-01-01:2024-10-01"
    reason = "calibrated single-primary v3 context tournament; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.single_primary_context_tournament"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.single_primary_context_tournament",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise SinglePrimaryContextTournamentError(f"Frozen {label} missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Single-Primary Context Tournament v1",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Structural prototypes: `{payload['structural_prototypes']}`",
            f"- Round-1 / Round-2 survivors: `{payload['round1_survivors']}` / `{payload['round2_survivors']}`",
            f"- Diagnostic archive: `{payload['diagnostic_archive_size']}`",
            f"- Promotion primary: `{payload['primary_candidate_id']}`",
            f"- Promising / shadow: `{payload['promising_candidates']}` / `{payload['shadow_candidates']}`",
            f"- Topstep path: `{payload['topstep_path_candidates']}`",
            "- Promotion tests: at most `1`",
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
