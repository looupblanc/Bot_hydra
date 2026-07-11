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
    SOURCE_PREREGISTRATION_SHA256,
    _attach_path_extremes,
    _evaluate_candidate,
    _integrity_proof,
    _write_immutable,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "cross_ecology_opening_acceptance_pilot_v1"
MARKET_PAIRS = {"GC": "MGC", "CL": "MCL"}
SYMBOLS = tuple(symbol for pair in MARKET_PAIRS.items() for symbol in pair)
SESSION_CLOCKS = {
    "GC": {"open": (7, 20), "close": (12, 29)},
    "MGC": {"open": (7, 20), "close": (12, 29)},
    "CL": {"open": (8, 0), "close": (13, 29)},
    "MCL": {"open": (8, 0), "close": (13, 29)},
}
COMMISSION_ROUND_TURN = {"GC": 4.50, "MGC": 2.00, "CL": 4.50, "MCL": 2.00}


class CrossEcologyOpeningAcceptanceError(RuntimeError):
    pass


def run_cross_ecology_opening_acceptance_pilot(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 771449,
) -> dict[str, Any]:
    task_path, map_path = Path(engineering_task_path), Path(repaired_map_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(map_path, repaired_map_sha256, "repaired contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise CrossEcologyOpeningAcceptanceError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise CrossEcologyOpeningAcceptanceError("Worker commit differs from queued specification.")
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
    preregistration_path = destination / "cross_ecology_opening_acceptance_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    candidate_ids = [f"strategy_opening_acceptance_{symbol}_v1" for symbol in MARKET_PAIRS]
    access = _record_access_once(candidate_ids) if record_data_access else None
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    events = build_opening_acceptance_events(raw)
    integrity = _integrity_proof(events)
    integrity.update(
        {
            "completed_15m_window_only": bool(
                (events["decision_timestamp"] == events["session_open_timestamp"] + pd.Timedelta(minutes=15)).all()
            ),
            "market_specific_session_clock": bool(
                events.apply(_clock_matches, axis=1).all()
            ),
            "acceptance_alignment_primary": bool(
                (
                    events.loc[events["primary_event"], "gap_points"]
                    * events.loc[events["primary_event"], "opening_displacement"]
                    > 0
                ).all()
            ),
        }
    )
    if not all(integrity.values()):
        raise CrossEcologyOpeningAcceptanceError(f"Cross-ecology integrity failed: {integrity}")
    candidates: list[dict[str, Any]] = []
    for index, (mini, micro) in enumerate(MARKET_PAIRS.items()):
        diagnostics = _parameter_diagnostics(events[events["symbol"].eq(mini)].copy())
        candidate = _evaluate_candidate(
            events,
            mini,
            micro,
            preregistration_hash=str(preregistration["preregistration_hash"]),
            random_seed=random_seed + index * 1009,
            candidate_prefix="strategy_opening_acceptance",
            mechanism_family="cross_ecology_opening_acceptance",
            direction="accepted_opening_displacement",
            parameter_diagnostics_override=diagnostics,
            shadow_spec_complete_override=False,
        )
        custom_spec = _shadow_specification(
            candidate, preregistration_hash=str(preregistration["preregistration_hash"])
        )
        custom_spec.validate()
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
    ledger_path = destination / "cross_ecology_opening_acceptance_trade_ledger.jsonl"
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
        raise CrossEcologyOpeningAcceptanceError("Pre-Q4 ecology pilot attempted paper promotion.")
    if shadow:
        conclusion = "CROSS_ECOLOGY_OPENING_ACCEPTANCE_SHADOW_CANDIDATES_FOUND"
        next_action = "FREEZE_OR_FORWARD_SHADOW_DISTINCT_ECOLOGY_CANDIDATE"
    elif promising:
        conclusion = "CROSS_ECOLOGY_OPENING_ACCEPTANCE_PROMISING_BUT_INSUFFICIENT"
        next_action = "CROSS_ECOLOGY_FAILURE_SURFACE_OR_DAILY_MTF_PIVOT"
    else:
        conclusion = "CROSS_ECOLOGY_OPENING_ACCEPTANCE_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_MULTI_TIMEFRAME_SESSION_DAILY_INVARIANT"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Fresh strategy-level development evidence in metal and energy only. Q4 remains sealed, "
            "PAPER_SHADOW_READY is impossible pre-holdout, and no order capability exists."
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
        "mechanism_families": ["cross_ecology_opening_acceptance"],
        "market_ecologies": ["metals", "energy"],
        "timeframe_profiles": ["session_15m_state_1m_execution"],
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
    result_path = destination / "cross_ecology_opening_acceptance_result.json"
    report_path = destination / "cross_ecology_opening_acceptance_report.md"
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


def build_opening_acceptance_events(
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
    data["local_hour"], data["local_minute"] = local.dt.hour, local.dt.minute
    data["trading_session_id"] = (local + pd.Timedelta(hours=7)).dt.strftime("%Y-%m-%d")
    open_mask = pd.Series(False, index=data.index)
    close_mask = pd.Series(False, index=data.index)
    for symbol, clock in SESSION_CLOCKS.items():
        symbol_mask = data["symbol"].eq(symbol)
        open_mask |= symbol_mask & data["local_hour"].eq(clock["open"][0]) & data[
            "local_minute"
        ].eq(clock["open"][1])
        close_mask |= symbol_mask & data["local_hour"].eq(clock["close"][0]) & data[
            "local_minute"
        ].eq(clock["close"][1])
    opens = data[open_mask].copy().rename(
        columns={
            "timestamp": "session_open_timestamp",
            "open": "session_open_price",
            "trading_session_id": "event_session_id",
        }
    )
    references = data[close_mask][
        ["symbol", "active_contract", "timestamp", "close", "trading_session_id"]
    ].rename(
        columns={
            "timestamp": "reference_timestamp",
            "close": "previous_session_close",
            "trading_session_id": "reference_session_id",
        }
    )
    opens = pd.merge_asof(
        opens.sort_values(["session_open_timestamp", "symbol", "active_contract"]),
        references.sort_values(["reference_timestamp", "symbol", "active_contract"]),
        left_on="session_open_timestamp",
        right_on="reference_timestamp",
        by=["symbol", "active_contract"],
        direction="backward",
        allow_exact_matches=False,
    )
    age = opens["session_open_timestamp"] - opens["reference_timestamp"]
    opens = opens[
        opens["reference_timestamp"].notna()
        & opens["reference_session_id"].ne(opens["event_session_id"])
        & age.between(pd.Timedelta(hours=12), pd.Timedelta(days=4))
    ].copy()
    opens["timestamp"] = opens["session_open_timestamp"] + pd.Timedelta(minutes=14)
    prices = data[["symbol", "active_contract", "timestamp", "close"]].copy()
    entry = prices.rename(columns={"close": "entry_price"})
    opens = opens.merge(
        entry,
        on=["symbol", "active_contract", "timestamp"],
        how="left",
        validate="many_to_one",
    )
    opens["decision_timestamp"] = opens["timestamp"] + pd.Timedelta(minutes=1)
    for horizon in (30, 60, 90):
        opens[f"exit_timestamp_{horizon}"] = opens["timestamp"] + pd.Timedelta(minutes=horizon)
        exits = prices.rename(
            columns={"timestamp": f"exit_timestamp_{horizon}", "close": f"exit_price_{horizon}"}
        )
        opens = opens.merge(
            exits,
            on=["symbol", "active_contract", f"exit_timestamp_{horizon}"],
            how="left",
            validate="many_to_one",
        )
    opens["delayed_entry_timestamp"] = opens["timestamp"] + pd.Timedelta(minutes=1)
    opens["delayed_exit_timestamp"] = opens["timestamp"] + pd.Timedelta(minutes=61)
    opens = opens.merge(
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
    opens["gap_points"] = opens["session_open_price"] - opens["previous_session_close"]
    opens["absolute_gap_points"] = opens["gap_points"].abs()
    opens["opening_displacement"] = opens["entry_price"] - opens["session_open_price"]
    opens = opens.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    for quantile in (0.55, 0.65, 0.75, 0.85):
        label = int(quantile * 100)
        opens[f"threshold_q{label}"] = opens.groupby("symbol", sort=False)[
            "absolute_gap_points"
        ].transform(
            lambda values: values.shift(1).expanding(min_periods=minimum_history).quantile(quantile)
        )
    opens["past_gap_count"] = opens.groupby("symbol", sort=False).cumcount()
    opens["side"] = np.sign(opens["gap_points"]).astype(int)
    opens["point_value"] = opens["symbol"].map(
        {symbol: instrument_spec(symbol).point_value for symbol in SYMBOLS}
    )
    opens["cost"] = opens["symbol"].map(_round_turn_cost)
    for horizon in (30, 60, 90):
        opens[f"gross_pnl_{horizon}"] = (
            opens["side"]
            * (opens[f"exit_price_{horizon}"] - opens["entry_price"])
            * opens["point_value"]
        )
        opens[f"net_pnl_{horizon}"] = opens[f"gross_pnl_{horizon}"] - opens["cost"]
    opens["delayed_gross_pnl"] = (
        opens["side"]
        * (opens["delayed_exit_price"] - opens["delayed_entry_price"])
        * opens["point_value"]
    )
    opens["delayed_net_pnl"] = opens["delayed_gross_pnl"] - opens["cost"]
    opens["primary_event"] = (
        (opens["past_gap_count"] >= minimum_history)
        & (opens["absolute_gap_points"] >= opens["threshold_q65"])
        & (opens["gap_points"] * opens["opening_displacement"] > 0)
        & opens["exit_price_60"].notna()
        & opens["side"].ne(0)
    )
    return _attach_path_extremes(opens, data, horizon=60).sort_values(
        ["timestamp", "symbol"]
    ).reset_index(drop=True)


def _parameter_diagnostics(events: pd.DataFrame) -> dict[str, Any]:
    variants = {
        "threshold_q55_h60": (55, 60),
        "threshold_q75_h60": (75, 60),
        "threshold_q65_h30": (65, 30),
        "threshold_q65_h90": (65, 90),
    }
    results: dict[str, dict[str, Any]] = {}
    alignment = events["gap_points"] * events["opening_displacement"] > 0
    for label, (quantile, horizon) in variants.items():
        selected = (
            (events["past_gap_count"] >= 40)
            & (events["absolute_gap_points"] >= events[f"threshold_q{quantile}"])
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
    clock = SESSION_CLOCKS[micro]
    decision_minutes = clock["open"][0] * 60 + clock["open"][1] + 15
    return ShadowSpecification(
        strategy_id=str(candidate["candidate_id"]),
        strategy_version="v1_pre_q4_shadow_research",
        feature_versions=("cross_ecology_opening_acceptance_v1",),
        markets=(micro,),
        timeframes=("1m", "15m_window", "session"),
        session_rules={
            "timezone": "America/Chicago",
            "decision_minute": decision_minutes,
            "reference_market": mini,
            "mandatory_flatten_before": "15:10",
        },
        entry_rules={
            "event": "gap_and_first_15m_displacement_align",
            "absolute_gap_past_only_q65": True,
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
            "incomplete_15m_window",
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


def _round_turn_cost(symbol: str) -> float:
    spec = instrument_spec(str(symbol))
    return float(COMMISSION_ROUND_TURN[str(symbol)] + 2.0 * spec.tick_value)


def _clock_matches(row: pd.Series) -> bool:
    timestamp = pd.Timestamp(row["session_open_timestamp"]).tz_convert("America/Chicago")
    expected = SESSION_CLOCKS[str(row["symbol"])]["open"]
    return (timestamp.hour, timestamp.minute) == expected


def _preregistration(
    *,
    engineering_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "cross_ecology_opening_acceptance_preregistration_v1",
        "candidate_ids": [f"strategy_opening_acceptance_{symbol}_v1" for symbol in MARKET_PAIRS],
        "market_pairs": MARKET_PAIRS,
        "session_clocks_chicago": SESSION_CLOCKS,
        "opening_window_minutes": 15,
        "primary_horizon_minutes": 60,
        "primary_gap_quantile": 0.65,
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
    reason = "gold and crude opening acceptance strategy pilot; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.cross_ecology_opening_acceptance"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.cross_ecology_opening_acceptance",
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
        "session_open_timestamp",
        "timestamp",
        "decision_timestamp",
        "reference_timestamp",
        "gap_points",
        "opening_displacement",
        "threshold_q65",
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
        "# Cross-Ecology Opening Acceptance Pilot",
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
        raise CrossEcologyOpeningAcceptanceError(f"Frozen {label} is missing or changed: {path}")
