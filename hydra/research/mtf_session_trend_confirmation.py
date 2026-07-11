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
from hydra.markets.instruments import instrument_spec
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
    _attach_path_extremes,
    _evaluate_candidate,
    _integrity_proof,
    _round_turn_cost,
    _write_immutable,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "mtf_session_trend_confirmation_pilot_v1"


class MTFSessionTrendConfirmationError(RuntimeError):
    pass


def run_mtf_session_trend_confirmation_pilot(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 771557,
) -> dict[str, Any]:
    task_path, map_path = Path(engineering_task_path), Path(repaired_map_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(map_path, repaired_map_sha256, "repaired contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise MTFSessionTrendConfirmationError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise MTFSessionTrendConfirmationError("Worker commit differs from queued specification.")
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
    preregistration_path = destination / "mtf_session_trend_confirmation_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    candidate_ids = [f"strategy_mtf_session_confirmation_{symbol}_v1" for symbol in MARKET_PAIRS]
    access = _record_access_once(candidate_ids) if record_data_access else None
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    events = build_mtf_session_confirmation_events(raw)
    integrity = _integrity_proof(events)
    integrity.update(
        {
            "prior_session_closed_before_decision": bool(
                (events["prior_session_availability"] <= events["decision_timestamp"]).all()
            ),
            "prior_session_strictly_previous": bool(
                events["prior_session_id"].ne(events["event_session_id"]).all()
            ),
            "completed_30m_window_only": bool(
                (events["decision_timestamp"] == events["current_open_timestamp"] + pd.Timedelta(minutes=30)).all()
            ),
            "session_and_30m_direction_confirm_primary": bool(
                (
                    events.loc[events["primary_event"], "prior_displacement"]
                    * events.loc[events["primary_event"], "current_opening_displacement"]
                    > 0
                ).all()
            ),
        }
    )
    if not all(integrity.values()):
        raise MTFSessionTrendConfirmationError(f"MTF integrity proof failed: {integrity}")
    candidates: list[dict[str, Any]] = []
    for index, (mini, micro) in enumerate(MARKET_PAIRS.items()):
        diagnostics = _parameter_diagnostics(events[events["symbol"].eq(mini)].copy())
        candidate = _evaluate_candidate(
            events,
            mini,
            micro,
            preregistration_hash=str(preregistration["preregistration_hash"]),
            random_seed=random_seed + index * 1009,
            candidate_prefix="strategy_mtf_session_confirmation",
            mechanism_family="mtf_session_trend_confirmation",
            direction="prior_session_and_current_30m_confirmation",
            parameter_diagnostics_override=diagnostics,
            shadow_spec_complete_override=False,
        )
        spec = _shadow_specification(
            candidate, preregistration_hash=str(preregistration["preregistration_hash"])
        )
        spec.validate()
        candidate["shadow_evidence"]["shadow_spec_complete"] = True
        candidates.append(candidate)
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
    ledger_path = destination / "mtf_session_trend_confirmation_trade_ledger.jsonl"
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
        raise MTFSessionTrendConfirmationError("Pre-Q4 MTF pilot attempted paper promotion.")
    if shadow:
        conclusion = "MTF_SESSION_CONFIRMATION_SHADOW_CANDIDATES_FOUND"
        next_action = "FREEZE_OR_FORWARD_SHADOW_MTF_CANDIDATE"
    elif promising:
        conclusion = "MTF_SESSION_CONFIRMATION_PROMISING_BUT_INSUFFICIENT"
        next_action = "MTF_ABLATION_AND_FAILURE_SURFACE"
    else:
        conclusion = "MTF_SESSION_CONFIRMATION_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_RELATIVE_VALUE_OR_DEFENSIVE_PORTFOLIO_ENGINE"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Fresh causal MTF development evidence only. Q4 remains sealed, PAPER_SHADOW_READY "
            "is impossible pre-holdout, and no order capability exists."
        ),
        "code_commit": code_commit,
        "preregistration_hash": preregistration["preregistration_hash"],
        "preregistration_path": str(preregistration_path),
        "data_provenance": provenance,
        "data_access_record": access,
        "integrity_proof": integrity,
        "candidate_count": len(candidates),
        "candidate_tier_counts": dict(pd.Series(statuses).value_counts()),
        "candidates": candidates,
        "promising_candidates": int(promising),
        "shadow_candidates": int(shadow),
        "paper_shadow_ready": int(paper),
        "topstep_path_candidates": int(
            sum(bool(row["topstep"]["path_candidate"]) for row in candidates)
        ),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "mechanism_families": ["mtf_session_trend_confirmation"],
        "market_ecologies": ["equity_indices"],
        "timeframe_profiles": ["completed_session_state_30m_confirmation_1m_execution"],
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
    result_path = destination / "mtf_session_trend_confirmation_result.json"
    report_path = destination / "mtf_session_trend_confirmation_report.md"
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


def build_mtf_session_confirmation_events(
    frame: pd.DataFrame, *, minimum_history: int = 40
) -> pd.DataFrame:
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    data = (
        data.sort_values(["symbol", "active_contract", "timestamp"])
        .drop_duplicates(["symbol", "active_contract", "timestamp"], keep=False)
        .reset_index(drop=True)
    )
    local = data["timestamp"].dt.tz_convert("America/Chicago")
    data["local_minute"] = local.dt.hour * 60 + local.dt.minute
    data["trading_session_id"] = (local + pd.Timedelta(hours=7)).dt.strftime("%Y-%m-%d")
    rth = data[data["local_minute"].between(8 * 60 + 30, 14 * 60 + 59)].copy()
    sessions = (
        rth.groupby(["symbol", "active_contract", "trading_session_id"], sort=True)
        .agg(
            prior_open=("open", "first"),
            prior_high=("high", "max"),
            prior_low=("low", "min"),
            prior_close=("close", "last"),
            prior_session_start=("timestamp", "min"),
            reference_timestamp=("timestamp", "max"),
            source_row_count=("timestamp", "size"),
        )
        .reset_index()
        .rename(columns={"trading_session_id": "prior_session_id"})
    )
    sessions["prior_session_availability"] = sessions["reference_timestamp"] + pd.Timedelta(
        minutes=1
    )
    width = (sessions["prior_high"] - sessions["prior_low"]).replace(0, np.nan)
    sessions["prior_displacement"] = sessions["prior_close"] - sessions["prior_open"]
    sessions["prior_efficiency"] = sessions["prior_displacement"].abs() / width
    sessions = sessions.sort_values(["symbol", "prior_session_availability"]).reset_index(drop=True)
    for quantile in (0.55, 0.65, 0.75):
        label = int(quantile * 100)
        sessions[f"prior_efficiency_q{label}"] = sessions.groupby("symbol", sort=False)[
            "prior_efficiency"
        ].transform(
            lambda values: values.shift(1).expanding(min_periods=minimum_history).quantile(quantile)
        )
    sessions["prior_history_count"] = sessions.groupby("symbol", sort=False).cumcount()
    current_open = data[data["local_minute"].eq(8 * 60 + 30)][
        ["symbol", "active_contract", "trading_session_id", "timestamp", "open"]
    ].rename(
        columns={
            "trading_session_id": "event_session_id",
            "timestamp": "current_open_timestamp",
            "open": "current_open_price",
        }
    )
    entry = data[data["local_minute"].eq(8 * 60 + 59)][
        ["symbol", "active_contract", "trading_session_id", "timestamp", "close"]
    ].rename(
        columns={
            "trading_session_id": "event_session_id",
            "close": "entry_price",
        }
    )
    events = current_open.merge(
        entry,
        on=["symbol", "active_contract", "event_session_id"],
        how="inner",
        validate="one_to_one",
    )
    events["decision_timestamp"] = events["timestamp"] + pd.Timedelta(minutes=1)
    events["current_opening_displacement"] = events["entry_price"] - events[
        "current_open_price"
    ]
    events["current_opening_absolute"] = events["current_opening_displacement"].abs()
    events = events.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    for quantile in (0.45, 0.55, 0.65):
        label = int(quantile * 100)
        events[f"current_opening_q{label}"] = events.groupby("symbol", sort=False)[
            "current_opening_absolute"
        ].transform(
            lambda values: values.shift(1).expanding(min_periods=minimum_history).quantile(quantile)
        )
    events["current_history_count"] = events.groupby("symbol", sort=False).cumcount()
    events = pd.merge_asof(
        events.sort_values(["decision_timestamp", "symbol", "active_contract"]),
        sessions.sort_values(["prior_session_availability", "symbol", "active_contract"]),
        left_on="decision_timestamp",
        right_on="prior_session_availability",
        by=["symbol", "active_contract"],
        direction="backward",
        allow_exact_matches=True,
    )
    age = events["timestamp"] - events["reference_timestamp"]
    events = events[
        events["reference_timestamp"].notna()
        & events["prior_session_id"].ne(events["event_session_id"])
        & age.between(pd.Timedelta(hours=12), pd.Timedelta(days=4))
    ].copy()
    prices = data[["symbol", "active_contract", "timestamp", "close"]].copy()
    for horizon in (30, 60, 90):
        events[f"exit_timestamp_{horizon}"] = events["timestamp"] + pd.Timedelta(minutes=horizon)
        events = events.merge(
            prices.rename(
                columns={
                    "timestamp": f"exit_timestamp_{horizon}",
                    "close": f"exit_price_{horizon}",
                }
            ),
            on=["symbol", "active_contract", f"exit_timestamp_{horizon}"],
            how="left",
            validate="many_to_one",
        )
    events["delayed_entry_timestamp"] = events["timestamp"] + pd.Timedelta(minutes=1)
    events["delayed_exit_timestamp"] = events["timestamp"] + pd.Timedelta(minutes=61)
    events = events.merge(
        prices.rename(columns={"timestamp": "delayed_entry_timestamp", "close": "delayed_entry_price"}),
        on=["symbol", "active_contract", "delayed_entry_timestamp"],
        how="left",
        validate="many_to_one",
    ).merge(
        prices.rename(columns={"timestamp": "delayed_exit_timestamp", "close": "delayed_exit_price"}),
        on=["symbol", "active_contract", "delayed_exit_timestamp"],
        how="left",
        validate="many_to_one",
    )
    events["gap_points"] = events["prior_displacement"]
    events["absolute_gap_points"] = events["prior_displacement"].abs()
    events["past_gap_count"] = np.minimum(
        events["prior_history_count"], events["current_history_count"]
    ).astype(int)
    events["threshold_q65"] = events["prior_efficiency_q65"]
    events["side"] = np.sign(events["prior_displacement"]).astype(int)
    events["point_value"] = events["symbol"].map(
        {symbol: instrument_spec(symbol).point_value for symbol in SYMBOLS}
    )
    events["cost"] = events["symbol"].map(_round_turn_cost)
    for horizon in (30, 60, 90):
        events[f"gross_pnl_{horizon}"] = (
            events["side"]
            * (events[f"exit_price_{horizon}"] - events["entry_price"])
            * events["point_value"]
        )
        events[f"net_pnl_{horizon}"] = events[f"gross_pnl_{horizon}"] - events["cost"]
    events["delayed_gross_pnl"] = (
        events["side"]
        * (events["delayed_exit_price"] - events["delayed_entry_price"])
        * events["point_value"]
    )
    events["delayed_net_pnl"] = events["delayed_gross_pnl"] - events["cost"]
    events["primary_event"] = (
        (events["past_gap_count"] >= minimum_history)
        & (events["prior_efficiency"] >= events["prior_efficiency_q65"])
        & (events["current_opening_absolute"] >= events["current_opening_q55"])
        & (events["prior_displacement"] * events["current_opening_displacement"] > 0)
        & events["exit_price_60"].notna()
        & events["side"].ne(0)
    )
    return _attach_path_extremes(events.reset_index(drop=True), data, horizon=60).sort_values(
        ["timestamp", "symbol"]
    ).reset_index(drop=True)


def _parameter_diagnostics(events: pd.DataFrame) -> dict[str, Any]:
    variants = {
        "efficiency_q55_opening_q55_h60": ("prior_efficiency_q55", "current_opening_q55", 60),
        "efficiency_q75_opening_q55_h60": ("prior_efficiency_q75", "current_opening_q55", 60),
        "efficiency_q65_opening_q45_h30": ("prior_efficiency_q65", "current_opening_q45", 30),
        "efficiency_q65_opening_q65_h90": ("prior_efficiency_q65", "current_opening_q65", 90),
    }
    results: dict[str, dict[str, Any]] = {}
    alignment = events["prior_displacement"] * events["current_opening_displacement"] > 0
    for label, (efficiency_column, opening_column, horizon) in variants.items():
        selected = (
            (events["past_gap_count"] >= 40)
            & (events["prior_efficiency"] >= events[efficiency_column])
            & (events["current_opening_absolute"] >= events[opening_column])
            & alignment
            & events[f"exit_price_{horizon}"].notna()
        )
        results[label] = {
            "events": int(selected.sum()),
            "net_pnl": float(events.loc[selected, f"net_pnl_{horizon}"].sum()),
        }
    return {
        "variants": results,
        "positive_neighbor_count": int(sum(row["net_pnl"] > 0 for row in results.values())),
        "diagnostic_only": True,
    }


def _shadow_specification(
    candidate: dict[str, Any], *, preregistration_hash: str
) -> ShadowSpecification:
    mini, micro = str(candidate["primary_market"]), str(candidate["execution_market"])
    return ShadowSpecification(
        strategy_id=str(candidate["candidate_id"]),
        strategy_version="v1_pre_q4_shadow_research",
        feature_versions=("closed_rth_session_efficiency_v1", "closed_30m_confirmation_v1"),
        markets=(micro,),
        timeframes=("1m", "30m", "session"),
        session_rules={
            "timezone": "America/Chicago",
            "decision_after_source_bar_close": "09:00",
            "reference_market": mini,
            "mandatory_flatten_before": "15:10",
        },
        entry_rules={
            "prior_session_efficiency_past_only_q65": True,
            "current_30m_displacement_past_only_q55": True,
            "direction_confirmation": True,
            "minimum_prior_sessions": 40,
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
            "incomplete_session_or_30m_bar",
            "duplicate_signal",
            "session_closed",
            "clock_invalid",
            "contract_map_mismatch",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={"jsonl": True, "signals": True, "virtual_fills": True, "rejections": True},
        reconciliation={"startup": "fail_closed", "position_source": "virtual_only"},
        source_manifest_hash=preregistration_hash,
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
        "schema": "mtf_session_trend_confirmation_preregistration_v1",
        "candidate_ids": [f"strategy_mtf_session_confirmation_{symbol}_v1" for symbol in MARKET_PAIRS],
        "market_pairs": MARKET_PAIRS,
        "source_timeframes": ["completed_rth_session", "completed_30m", "1m_execution"],
        "decision_time_chicago": "09:00",
        "primary_horizon_minutes": 60,
        "prior_efficiency_quantile": 0.65,
        "current_opening_quantile": 0.55,
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
    reason = "completed-session and 30m MTF trend confirmation strategy pilot; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.mtf_session_trend_confirmation"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.mtf_session_trend_confirmation",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _write_trade_ledger(path: Path, events: pd.DataFrame) -> None:
    selected = events[events["primary_event"]]
    columns = [
        "symbol",
        "active_contract",
        "event_session_id",
        "prior_session_id",
        "prior_session_availability",
        "current_open_timestamp",
        "timestamp",
        "decision_timestamp",
        "prior_efficiency",
        "prior_efficiency_q65",
        "prior_displacement",
        "current_opening_displacement",
        "current_opening_q55",
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
        "# MTF Session Trend Confirmation Pilot",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Candidate tiers: `{payload['candidate_tier_counts']}`",
        f"- Shadow candidates: `{payload['shadow_candidates']}`",
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
                "",
            ]
        )
    return "\n".join(lines)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected:
        raise MTFSessionTrendConfirmationError(f"Frozen {label} is missing or changed: {path}")
