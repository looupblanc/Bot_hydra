from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

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
    EquityOpenGapReversalError,
    _evaluate_candidate,
    _integrity_proof,
    _record_development_access_once,
    _round_turn_cost,
    _shadow_specification,
    _write_immutable,
    _write_trade_ledger,
    build_event_table,
)
from hydra.utils.config import project_path


VERSION = "equity_open_gap_continuation_pilot_v1"


class EquityOpenGapContinuationError(RuntimeError):
    pass


def run_equity_open_gap_continuation_pilot(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    source_reversal_result_path: str | Path,
    source_reversal_result_sha256: str,
    source_reversal_result_hash: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 771217,
) -> dict[str, Any]:
    task_path = Path(engineering_task_path)
    map_path = Path(repaired_map_path)
    reversal_path = Path(source_reversal_result_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(map_path, repaired_map_sha256, "repaired contract map")
    _verify(reversal_path, source_reversal_result_sha256, "frozen reversal result")
    reversal = json.loads(reversal_path.read_text(encoding="utf-8"))
    if (
        reversal.get("result_hash") != source_reversal_result_hash
        or reversal.get("scientific_conclusion")
        != "EQUITY_OPEN_GAP_REVERSAL_FALSIFIED_OR_INSUFFICIENT"
        or int(reversal.get("shadow_candidates", -1)) != 0
    ):
        raise EquityOpenGapContinuationError("Frozen reversal source does not authorize this pivot.")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise EquityOpenGapContinuationError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise EquityOpenGapContinuationError("Worker commit differs from queued specification.")

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
        source_reversal_result_hash=source_reversal_result_hash,
        source_reversal_result_sha256=source_reversal_result_sha256,
        code_commit=code_commit,
        random_seed=random_seed,
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration_path = destination / "equity_open_gap_continuation_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    candidate_ids = [f"strategy_open_gap_continuation_{symbol}_v1" for symbol in MARKET_PAIRS]
    access = (
        _record_continuation_access_once(candidate_ids) if record_data_access else None
    )
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    events = _convert_to_continuation(build_event_table(raw))
    integrity = _integrity_proof(events)
    if not all(integrity.values()):
        raise EquityOpenGapContinuationError(f"Continuation integrity proof failed: {integrity}")
    candidates = [
        _evaluate_candidate(
            events,
            mini,
            micro,
            preregistration_hash=str(preregistration["preregistration_hash"]),
            random_seed=random_seed + index * 1009,
            candidate_prefix="strategy_open_gap_continuation",
            mechanism_family="equity_rth_open_gap_continuation",
            direction="gap_direction",
        )
        for index, (mini, micro) in enumerate(MARKET_PAIRS.items())
    ]
    for candidate in candidates:
        adjusted = min(
            float(candidate["null_evidence"]["raw_probability"]) * len(candidates), 1.0
        )
        candidate["null_evidence"]["family_adjusted_probability"] = adjusted
        candidate["shadow_evidence"]["null_probability"] = adjusted
        candidate["shadow_evidence"]["candidate_null_pass"] = bool(
            adjusted <= 0.20 and candidate["attacks"]["sign_flip_net"] < 0.0
        )
        admission = decide_shadow_admission(ShadowEvidence(**candidate["shadow_evidence"]))
        candidate["admission"] = admission.to_dict()
        candidate["status"] = admission.tier.value

    shadow_directory = destination / "shadow_configurations"
    shadow_configs: list[dict[str, Any]] = []
    for candidate in candidates:
        if not candidate["admission"]["permits_zero_risk_shadow"]:
            continue
        spec = _shadow_specification(
            candidate, preregistration_hash=str(preregistration["preregistration_hash"])
        )
        path = spec.write_immutable(shadow_directory / f"{candidate['candidate_id']}.json")
        shadow_configs.append(
            {
                "candidate_id": candidate["candidate_id"],
                "status": candidate["status"],
                "path": str(path),
                "configuration_hash": spec.configuration_hash,
                "outbound_orders_enabled": False,
            }
        )
    ledger_path = destination / "equity_open_gap_continuation_trade_ledger.jsonl"
    _write_trade_ledger(ledger_path, events)
    statuses = [row["status"] for row in candidates]
    tier_counts = dict(pd.Series(statuses).value_counts())
    promising = sum(
        status
        in {
            EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
            EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
            EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
            EvidenceTier.PAPER_SHADOW_READY.value,
        }
        for status in statuses
    )
    shadow = sum(
        status
        in {EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value, EvidenceTier.PAPER_SHADOW_READY.value}
        for status in statuses
    )
    paper = statuses.count(EvidenceTier.PAPER_SHADOW_READY.value)
    topstep = sum(bool(row["topstep"]["path_candidate"]) for row in candidates)
    if paper:
        raise EquityOpenGapContinuationError("Pre-Q4 continuation attempted paper promotion.")
    q4_freeze_eligible = [
        row["candidate_id"]
        for row in candidates
        if row["status"] == EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value
        and not bool(row["attacks"]["event_dominated"])
    ]
    if q4_freeze_eligible:
        conclusion = "EQUITY_OPEN_GAP_CONTINUATION_Q4_FREEZE_CANDIDATES_FOUND"
        next_action = "FREEZE_BEST_CONTINUATION_CANDIDATE_FOR_ONE_SHOT_Q4"
    elif shadow:
        conclusion = "EQUITY_OPEN_GAP_CONTINUATION_SHADOW_RESEARCH_ONLY"
        next_action = "MAP_CONTINUATION_FAILURE_SURFACE_BEFORE_Q4"
    elif promising:
        conclusion = "EQUITY_OPEN_GAP_CONTINUATION_PROMISING_BUT_INSUFFICIENT"
        next_action = "TARGETED_CONTINUATION_FAILURE_SURFACE"
    else:
        conclusion = "EQUITY_OPEN_GAP_CONTINUATION_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_DISTRIBUTIONAL_OPENING_HAZARD_MODEL"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Fresh candidate-level development evidence only. No status or probability was inherited "
            "from reversal diagnostics. Mini/micro copies are one mechanism, Q4 remains sealed, "
            "PAPER_SHADOW_READY is impossible pre-holdout, and no order path exists."
        ),
        "code_commit": code_commit,
        "preregistration_hash": preregistration["preregistration_hash"],
        "preregistration_path": str(preregistration_path),
        "source_reversal_result": {
            "path": str(reversal_path),
            "sha256": source_reversal_result_sha256,
            "result_hash": source_reversal_result_hash,
        },
        "data_provenance": provenance,
        "data_access_record": access,
        "integrity_proof": integrity,
        "event_count": int(events["primary_event"].sum()),
        "candidate_count": len(candidates),
        "candidate_tier_counts": tier_counts,
        "candidates": candidates,
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow),
        "paper_shadow_ready": int(paper),
        "q4_freeze_eligible_candidate_ids": q4_freeze_eligible,
        "topstep_path_candidates": int(topstep),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "mechanism_families": ["equity_rth_open_gap_continuation"],
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
    result_path = destination / "equity_open_gap_continuation_result.json"
    report_path = destination / "equity_open_gap_continuation_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "trade_ledger_path": str(ledger_path),
            "shadow_configuration_directory": str(shadow_directory),
        },
        "report_path": str(report_path),
    }


def _convert_to_continuation(events: pd.DataFrame) -> pd.DataFrame:
    output = events.copy()
    output["side"] = -output["side"]
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
    return output


def _preregistration(
    *,
    engineering_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    source_reversal_result_hash: str,
    source_reversal_result_sha256: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "equity_open_gap_continuation_preregistration_v1",
        "strategy_family_id": "equity_rth_open_gap_continuation_20260711_v1",
        "candidate_ids": [f"strategy_open_gap_continuation_{symbol}_v1" for symbol in MARKET_PAIRS],
        "market_pairs": MARKET_PAIRS,
        "direction": "signed_gap_continuation",
        "primary_horizon_minutes": 60,
        "primary_threshold_quantile": 0.75,
        "minimum_prior_sessions": 40,
        "decision_time_chicago": "08:31",
        "folds": FOLDS,
        "costs": {symbol: _round_turn_cost(symbol) for symbol in SYMBOLS},
        "diagnostics": {
            "threshold_quantiles": [0.65, 0.85],
            "holding_minutes": [30, 90],
            "entry_delay_bars": 1,
            "reversal_control": True,
            "block_sign_flip_draws": 4096,
            "family_test_count": 4,
        },
        "task_sha256": engineering_task_sha256,
        "map_sha256": repaired_map_sha256,
        "roll_map_hash": repaired_roll_map_hash,
        "source_reversal_result_hash": source_reversal_result_hash,
        "source_reversal_result_sha256": source_reversal_result_sha256,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "data_end_exclusive": "2024-10-01",
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "inherits_reversal_status": False,
        "paper_shadow_ready_requires_untouched_holdout": True,
    }
    payload["preregistration_hash"] = _stable_hash(payload)
    return payload


def _record_continuation_access_once(candidate_ids: list[str]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "equity RTH open-gap continuation fresh strategy pilot; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.equity_open_gap_continuation"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    # Reuse the same governance primitive while retaining a distinct requesting
    # module and fresh candidate IDs.
    from hydra.validation.data_roles import DataRole
    from hydra.validation.lockbox_guard import enforce_data_access

    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.equity_open_gap_continuation",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Equity RTH Open-Gap Continuation Pilot",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Candidate tiers: `{payload['candidate_tier_counts']}`",
        f"- Shadow research candidates: `{payload['shadow_candidates']}`",
        f"- Q4 freeze eligible: `{payload['q4_freeze_eligible_candidate_ids']}`",
        f"- PAPER_SHADOW_READY: `{payload['paper_shadow_ready']}`",
        f"- Topstep path candidates: `{payload['topstep_path_candidates']}`",
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
                f"- Family-adjusted p: `{row['null_evidence']['family_adjusted_probability']:.6f}`",
                f"- Contract transfer: `{row['contract_transfer']['passed']}`",
                f"- MLL safe: `{row['topstep']['micro_one_contract_mll_safe']}`",
                "",
            ]
        )
    lines.extend(["## Interpretation boundary", "", payload["interpretation_boundary"], ""])
    return "\n".join(lines)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected:
        raise EquityOpenGapContinuationError(f"Frozen {label} is missing or changed: {path}")
