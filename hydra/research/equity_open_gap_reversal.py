from __future__ import annotations

import hashlib
import json
import math
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
from hydra.propfirm.topstep_150k import Topstep150KConfig, simulate_combine
from hydra.propfirm.xfa_consistency import simulate_xfa_consistency
from hydra.propfirm.xfa_standard import simulate_xfa_standard
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "equity_open_gap_reversal_pilot_v1"
MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
MARKET_PAIRS = {"ES": "MES", "NQ": "MNQ", "RTY": "M2K", "YM": "MYM"}
SYMBOLS = tuple(symbol for pair in MARKET_PAIRS.items() for symbol in pair)
FOLDS = {
    "2023_h2": ("2023-07-01", "2024-01-01"),
    "2024_q1": ("2024-01-01", "2024-04-01"),
    "2024_q2": ("2024-04-01", "2024-07-01"),
    "2024_q3": ("2024-07-01", "2024-10-01"),
}
COMMISSION_ROUND_TURN = {
    "ES": 4.50,
    "MES": 2.00,
    "NQ": 4.50,
    "MNQ": 2.00,
    "RTY": 4.50,
    "M2K": 2.00,
    "YM": 4.50,
    "MYM": 2.00,
}
SOURCE_PREREGISTRATION_SHA256 = (
    "d3e6ab3fe77ccb759902bb2241fef8e6203e583259eb25648d739fa751b15e26"
)


class EquityOpenGapReversalError(RuntimeError):
    pass


def run_equity_open_gap_reversal_pilot(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 771103,
) -> dict[str, Any]:
    task_path = Path(engineering_task_path)
    map_path = Path(repaired_map_path)
    _verify_file(task_path, engineering_task_sha256, "engineering task")
    _verify_file(map_path, repaired_map_sha256, "repaired contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise EquityOpenGapReversalError("Repaired explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual_commit != code_commit:
            raise EquityOpenGapReversalError("Worker commit differs from the queued frozen specification.")

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
    _verify_file(source_preregistration, SOURCE_PREREGISTRATION_SHA256, "development manifest")
    source_payload = json.loads(source_preregistration.read_text(encoding="utf-8"))
    _verify_development_manifest(
        (source_payload.get("source") or {}).get("development_data_manifest") or {}
    )

    preregistration = _preregistration(
        engineering_task_sha256=engineering_task_sha256,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        code_commit=code_commit,
        random_seed=random_seed,
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration_path = destination / "equity_open_gap_reversal_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )

    candidate_ids = [f"strategy_open_gap_reversal_{symbol}_v1" for symbol in MARKET_PAIRS]
    access_record = (
        _record_development_access_once(candidate_ids)
        if record_data_access
        else None
    )
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    if pd.to_datetime(raw["timestamp"], utc=True).max() >= pd.Timestamp(
        "2024-10-01", tz="UTC"
    ):
        raise EquityOpenGapReversalError("Sealed Q4 boundary crossed.")

    events = build_event_table(raw)
    integrity = _integrity_proof(events)
    if not all(integrity.values()):
        raise EquityOpenGapReversalError(f"Event construction integrity failed: {integrity}")

    candidates = [
        _evaluate_candidate(
            events,
            mini,
            micro,
            preregistration_hash=str(preregistration["preregistration_hash"]),
            random_seed=random_seed + index * 1009,
        )
        for index, (mini, micro) in enumerate(MARKET_PAIRS.items())
    ]
    # Four market-specific tests share one frozen family. Admission uses a
    # family-wise Bonferroni probability; raw probabilities remain reported.
    for candidate in candidates:
        adjusted = min(float(candidate["null_evidence"]["raw_probability"]) * len(candidates), 1.0)
        candidate["null_evidence"]["family_adjusted_probability"] = adjusted
        candidate["shadow_evidence"]["null_probability"] = adjusted
        candidate["shadow_evidence"]["candidate_null_pass"] = bool(
            adjusted <= 0.20 and candidate["attacks"]["sign_flip_net"] < 0.0
        )
        evidence = ShadowEvidence(**candidate["shadow_evidence"])
        admission = decide_shadow_admission(evidence)
        candidate["admission"] = admission.to_dict()
        candidate["status"] = admission.tier.value

    shadow_directory = destination / "shadow_configurations"
    shadow_configs: list[dict[str, Any]] = []
    for candidate in candidates:
        if not candidate["admission"]["permits_zero_risk_shadow"]:
            continue
        specification = _shadow_specification(
            candidate,
            preregistration_hash=str(preregistration["preregistration_hash"]),
        )
        path = specification.write_immutable(
            shadow_directory / f"{candidate['candidate_id']}.json"
        )
        shadow_configs.append(
            {
                "candidate_id": candidate["candidate_id"],
                "status": candidate["status"],
                "path": str(path),
                "configuration_hash": specification.configuration_hash,
                "outbound_orders_enabled": False,
            }
        )

    ledger_path = destination / "equity_open_gap_reversal_trade_ledger.jsonl"
    _write_trade_ledger(ledger_path, events)
    tier_counts = dict(pd.Series([row["status"] for row in candidates]).value_counts())
    shadow_count = int(
        sum(
            row["status"]
            in {EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value, EvidenceTier.PAPER_SHADOW_READY.value}
            for row in candidates
        )
    )
    paper_count = int(
        sum(row["status"] == EvidenceTier.PAPER_SHADOW_READY.value for row in candidates)
    )
    promising_count = int(
        sum(
            row["status"]
            in {
                EvidenceTier.PROMISING_RESEARCH_CANDIDATE.value,
                EvidenceTier.ROBUST_RESEARCH_CANDIDATE.value,
                EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value,
                EvidenceTier.PAPER_SHADOW_READY.value,
            }
            for row in candidates
        )
    )
    topstep_count = int(sum(bool(row["topstep"]["path_candidate"]) for row in candidates))
    if paper_count:
        # This is unreachable pre-holdout under the calibrated policy and is a
        # guard against accidental semantic regression.
        raise EquityOpenGapReversalError("Pre-Q4 pilot attempted PAPER_SHADOW_READY promotion.")
    if shadow_count:
        conclusion = "EQUITY_OPEN_GAP_REVERSAL_SHADOW_RESEARCH_CANDIDATES_FOUND"
        next_action = "FREEZE_BEST_SHADOW_CANDIDATE_FOR_ONE_SHOT_Q4_DECISION"
    elif promising_count:
        conclusion = "EQUITY_OPEN_GAP_REVERSAL_PROMISING_BUT_INSUFFICIENT"
        next_action = "TARGETED_FAILURE_SURFACE_MUTATION_WITHOUT_Q4"
    else:
        conclusion = "EQUITY_OPEN_GAP_REVERSAL_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_DISTRIBUTIONAL_OPENING_HAZARD_MODEL"

    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Development/falsification evidence only. Market copies are one economic family. "
            "No candidate inherits evidence, no Q4 was read, PAPER_SHADOW_READY is impossible "
            "before a frozen one-shot holdout, and no broker/order path exists."
        ),
        "code_commit": code_commit,
        "preregistration_hash": preregistration["preregistration_hash"],
        "preregistration_path": str(preregistration_path),
        "data_provenance": provenance,
        "data_access_record": access_record,
        "integrity_proof": integrity,
        "event_count": int(events["primary_event"].sum()),
        "candidate_count": len(candidates),
        "candidate_tier_counts": tier_counts,
        "candidates": candidates,
        "promising_candidates": promising_count,
        "shadow_candidates": shadow_count,
        "paper_shadow_ready": paper_count,
        "topstep_path_candidates": topstep_count,
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "mechanism_families": ["equity_rth_open_gap_reversal"],
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
    result_path = destination / "equity_open_gap_reversal_result.json"
    report_path = destination / "equity_open_gap_reversal_report.md"
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


def build_event_table(frame: pd.DataFrame, *, minimum_history: int = 40) -> pd.DataFrame:
    required = {
        "timestamp",
        "symbol",
        "active_contract",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise EquityOpenGapReversalError(f"Missing event-source columns: {missing}")
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    data = (
        data.sort_values(["symbol", "active_contract", "timestamp"])
        .drop_duplicates(["symbol", "active_contract", "timestamp"], keep=False)
        .reset_index(drop=True)
    )
    chicago = data["timestamp"].dt.tz_convert("America/Chicago")
    data["trading_session_id"] = (chicago + pd.Timedelta(hours=7)).dt.strftime("%Y-%m-%d")
    data["local_hour"] = chicago.dt.hour
    data["local_minute"] = chicago.dt.minute

    opens = data[(data["local_hour"] == 8) & (data["local_minute"] == 30)].copy()
    opens = opens.rename(
        columns={
            "open": "rth_open_price",
            "close": "entry_price",
            "trading_session_id": "event_session_id",
        }
    )
    opens["decision_timestamp"] = opens["timestamp"] + pd.Timedelta(minutes=1)
    references = data[(data["local_hour"] == 14) & (data["local_minute"] == 59)][
        ["symbol", "active_contract", "timestamp", "close", "trading_session_id"]
    ].rename(
        columns={
            "timestamp": "reference_timestamp",
            "close": "previous_rth_close",
            "trading_session_id": "reference_session_id",
        }
    )
    opens = pd.merge_asof(
        opens.sort_values(["timestamp", "symbol", "active_contract"]),
        references.sort_values(["reference_timestamp", "symbol", "active_contract"]),
        left_on="timestamp",
        right_on="reference_timestamp",
        by=["symbol", "active_contract"],
        direction="backward",
        allow_exact_matches=False,
    )
    age = opens["timestamp"] - opens["reference_timestamp"]
    opens = opens[
        opens["reference_timestamp"].notna()
        & opens["reference_session_id"].ne(opens["event_session_id"])
        & age.between(pd.Timedelta(hours=12), pd.Timedelta(days=4))
    ].copy()

    prices = data[["symbol", "active_contract", "timestamp", "close"]].copy()
    for horizon in (30, 60, 90):
        opens[f"exit_timestamp_{horizon}"] = opens["timestamp"] + pd.Timedelta(
            minutes=horizon
        )
        exit_prices = prices.rename(
            columns={"timestamp": f"exit_timestamp_{horizon}", "close": f"exit_price_{horizon}"}
        )
        opens = opens.merge(
            exit_prices,
            on=["symbol", "active_contract", f"exit_timestamp_{horizon}"],
            how="left",
            validate="many_to_one",
        )
    opens["delayed_entry_timestamp"] = opens["timestamp"] + pd.Timedelta(minutes=1)
    opens["delayed_exit_timestamp"] = opens["timestamp"] + pd.Timedelta(minutes=61)
    delayed_entry = prices.rename(
        columns={"timestamp": "delayed_entry_timestamp", "close": "delayed_entry_price"}
    )
    delayed_exit = prices.rename(
        columns={"timestamp": "delayed_exit_timestamp", "close": "delayed_exit_price"}
    )
    opens = (
        opens.merge(
            delayed_entry,
            on=["symbol", "active_contract", "delayed_entry_timestamp"],
            how="left",
            validate="many_to_one",
        )
        .merge(
            delayed_exit,
            on=["symbol", "active_contract", "delayed_exit_timestamp"],
            how="left",
            validate="many_to_one",
        )
    )
    opens["gap_points"] = opens["rth_open_price"].astype(float) - opens[
        "previous_rth_close"
    ].astype(float)
    opens["absolute_gap_points"] = opens["gap_points"].abs()
    opens = opens.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    for quantile in (0.65, 0.75, 0.85):
        label = int(round(quantile * 100))
        opens[f"threshold_q{label}"] = opens.groupby("symbol", sort=False)[
            "absolute_gap_points"
        ].transform(
            lambda values: values.shift(1).expanding(min_periods=minimum_history).quantile(quantile)
        )
    opens["past_gap_count"] = opens.groupby("symbol", sort=False).cumcount()
    opens["side"] = -np.sign(opens["gap_points"]).astype(int)
    opens["cost"] = opens["symbol"].map(_round_turn_cost)
    opens["point_value"] = opens["symbol"].map(
        {symbol: instrument_spec(symbol).point_value for symbol in SYMBOLS}
    )
    for horizon in (30, 60, 90):
        gross = (
            opens["side"]
            * (opens[f"exit_price_{horizon}"] - opens["entry_price"])
            * opens["point_value"]
        )
        opens[f"gross_pnl_{horizon}"] = gross
        opens[f"net_pnl_{horizon}"] = gross - opens["cost"]
    opens["delayed_gross_pnl"] = (
        opens["side"]
        * (opens["delayed_exit_price"] - opens["delayed_entry_price"])
        * opens["point_value"]
    )
    opens["delayed_net_pnl"] = opens["delayed_gross_pnl"] - opens["cost"]
    opens["primary_event"] = (
        (opens["past_gap_count"] >= minimum_history)
        & (opens["absolute_gap_points"] >= opens["threshold_q75"])
        & opens["exit_price_60"].notna()
        & opens["side"].ne(0)
    )
    opens = _attach_path_extremes(opens, data, horizon=60)
    return opens.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def _attach_path_extremes(events: pd.DataFrame, data: pd.DataFrame, *, horizon: int) -> pd.DataFrame:
    output = events.copy()
    output["future_low_60"] = np.nan
    output["future_high_60"] = np.nan
    for keys, positions in output.groupby(["symbol", "active_contract"], sort=True).groups.items():
        group = data[
            data["symbol"].astype(str).eq(str(keys[0]))
            & data["active_contract"].astype(str).eq(str(keys[1]))
        ].sort_values("timestamp")
        timestamps = pd.to_datetime(group["timestamp"], utc=True).astype("int64").to_numpy()
        lows = group["low"].astype(float).to_numpy()
        highs = group["high"].astype(float).to_numpy()
        for position in positions:
            event_timestamp = pd.Timestamp(output.at[position, "timestamp"]).value
            start = int(np.searchsorted(timestamps, event_timestamp))
            end = start + horizon
            if start >= len(timestamps) or timestamps[start] != event_timestamp or end >= len(timestamps):
                continue
            expected = event_timestamp + pd.Timedelta(minutes=horizon).value
            if timestamps[end] != expected:
                continue
            output.at[position, "future_low_60"] = float(np.min(lows[start + 1 : end + 1]))
            output.at[position, "future_high_60"] = float(np.max(highs[start + 1 : end + 1]))
    long_mae = (output["future_low_60"] - output["entry_price"]) * output["point_value"]
    short_mae = (output["entry_price"] - output["future_high_60"]) * output["point_value"]
    output["mae_dollars"] = np.where(output["side"] > 0, long_mae, short_mae) - output["cost"] / 2
    return output


def _evaluate_candidate(
    events: pd.DataFrame,
    mini: str,
    micro: str,
    *,
    preregistration_hash: str,
    random_seed: int,
    candidate_prefix: str = "strategy_open_gap_reversal",
    mechanism_family: str = "equity_rth_open_gap_reversal",
    direction: str = "contrarian",
    parameter_diagnostics_override: dict[str, Any] | None = None,
    shadow_spec_complete_override: bool | None = None,
) -> dict[str, Any]:
    mini_all = events[events["symbol"].eq(mini)].copy()
    micro_all = events[events["symbol"].eq(micro)].copy()
    mini_primary = mini_all[mini_all["primary_event"]].copy()
    micro_primary = micro_all[micro_all["primary_event"]].copy()
    fold_results = _fold_results(mini_primary)
    micro_folds = _fold_results(micro_primary)
    transfer_folds = ["2024_q1", "2024_q2", "2024_q3"]
    supportive = sum(fold_results[fold]["net_pnl"] > 0 for fold in transfer_folds)
    micro_supportive = sum(micro_folds[fold]["net_pnl"] > 0 for fold in transfer_folds)
    total_net = float(mini_primary["net_pnl_60"].sum())
    micro_net = float(micro_primary["net_pnl_60"].sum())
    raw_probability = _block_sign_flip_probability(mini_primary, seed=random_seed)
    diagnostics = parameter_diagnostics_override or _parameter_diagnostics(mini_all)
    stressed_net = float(
        (mini_primary["gross_pnl_60"] - 1.5 * mini_primary["cost"]).sum()
    )
    parameter_stable = bool(
        diagnostics["positive_neighbor_count"] >= 3 and stressed_net > 0.0
    )
    contract_evidence = bool(
        total_net > 0.0 and micro_net > 0.0 and micro_supportive >= 1
    )
    account = _account_replay(micro_primary)
    concentration = _concentration(mini_primary, fold_results)
    overall_mean = total_net / max(len(mini_primary), 1)
    transfer_means = [
        fold_results[fold]["mean_net_pnl"]
        for fold in transfer_folds
        if fold_results[fold]["events"] > 0
    ]
    catastrophic = bool(
        overall_mean > 0
        and any(value < -2.0 * abs(overall_mean) for value in transfer_means)
    )
    candidate_id = f"{candidate_prefix}_{mini}_v1"
    shadow_spec_complete = (
        bool(shadow_spec_complete_override)
        if shadow_spec_complete_override is not None
        else _shadow_specification_contract_valid(
            mini=mini,
            micro=micro,
            candidate_id=candidate_id,
            preregistration_hash=preregistration_hash,
            direction=direction,
        )
    )
    shadow_evidence = {
        "candidate_id": candidate_id,
        "hard_invalidations": (),
        "data_integrity": True,
        "no_lookahead": True,
        "deterministic_signals": True,
        "net_after_costs": total_net,
        "supportive_temporal_folds": supportive,
        "catastrophic_transfer": catastrophic,
        "candidate_null_pass": False,
        "null_probability": raw_probability,
        "parameter_stable": parameter_stable,
        "contract_evidence": contract_evidence,
        "account_mll_safe": bool(account["micro_one_contract_mll_safe"]),
        "execution_possible": True,
        "realtime_features_available": True,
        "shadow_spec_complete": shadow_spec_complete,
        "observability_complete": True,
        "untouched_holdout_passed": False,
        "sample_size": int(len(mini_primary)),
        "uncertainty": "development_only_q4_unopened",
    }
    sign_flip_net = float((-mini_primary["gross_pnl_60"] - mini_primary["cost"]).sum())
    delayed_net = float(
        mini_primary.loc[mini_primary["delayed_net_pnl"].notna(), "delayed_net_pnl"].sum()
    )
    return {
        "candidate_id": shadow_evidence["candidate_id"],
        "mechanism_family": mechanism_family,
        "entry_direction": direction,
        "primary_market": mini,
        "execution_market": micro,
        "events": int(len(mini_primary)),
        "net_pnl": total_net,
        "mean_net_pnl": overall_mean,
        "micro_events": int(len(micro_primary)),
        "micro_net_pnl": micro_net,
        "supportive_temporal_folds": supportive,
        "fold_results": fold_results,
        "micro_fold_results": micro_folds,
        "null_evidence": {
            "method": "five_event_block_sign_flip_4096_draws",
            "raw_probability": raw_probability,
            "family_adjusted_probability": None,
        },
        "parameter_diagnostics": diagnostics,
        "cost_stress_1_5x_net": stressed_net,
        "contract_transfer": {
            "mini": mini,
            "micro": micro,
            "passed": contract_evidence,
            "micro_supportive_folds": micro_supportive,
        },
        "attacks": {
            "sign_flip_net": sign_flip_net,
            "one_bar_delay_net": delayed_net,
            "best_event_share_of_positive_pnl": concentration["best_event_share"],
            "best_fold_share_of_positive_pnl": concentration["best_fold_share"],
            "event_dominated": concentration["event_dominated"],
        },
        "topstep": account,
        "shadow_evidence": shadow_evidence,
        "admission": {},
        "status": EvidenceTier.RESEARCH_PROTOTYPE.value,
    }


def _fold_results(events: pd.DataFrame) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for fold, (start, end) in FOLDS.items():
        selected = events[
            events["event_session_id"].astype(str).ge(start)
            & events["event_session_id"].astype(str).lt(end)
        ]
        output[fold] = {
            "events": int(len(selected)),
            "gross_pnl": float(selected["gross_pnl_60"].sum()),
            "costs": float(selected["cost"].sum()),
            "net_pnl": float(selected["net_pnl_60"].sum()),
            "mean_net_pnl": float(selected["net_pnl_60"].mean()) if len(selected) else 0.0,
            "win_rate": float((selected["net_pnl_60"] > 0).mean()) if len(selected) else 0.0,
        }
    return output


def _parameter_diagnostics(events: pd.DataFrame) -> dict[str, Any]:
    variants = {
        "threshold_q65_h60": (0.65, 60),
        "threshold_q85_h60": (0.85, 60),
        "threshold_q75_h30": (0.75, 30),
        "threshold_q75_h90": (0.75, 90),
    }
    results: dict[str, dict[str, Any]] = {}
    for label, (quantile, horizon) in variants.items():
        threshold = events[f"threshold_q{int(quantile * 100)}"]
        mask = (
            (events["past_gap_count"] >= 40)
            & (events["absolute_gap_points"] >= threshold)
            & events[f"exit_price_{horizon}"].notna()
            & events["side"].ne(0)
        )
        selected = events[mask]
        results[label] = {
            "events": int(len(selected)),
            "net_pnl": float(selected[f"net_pnl_{horizon}"].sum()),
        }
    return {
        "variants": results,
        "positive_neighbor_count": int(sum(row["net_pnl"] > 0 for row in results.values())),
        "diagnostic_only": True,
    }


def _block_sign_flip_probability(events: pd.DataFrame, *, seed: int) -> float:
    usable = events.dropna(subset=["gross_pnl_60", "cost"]).sort_values("timestamp")
    if len(usable) < 10:
        return 1.0
    gross = usable["gross_pnl_60"].to_numpy(dtype=float)
    costs = usable["cost"].to_numpy(dtype=float)
    block = np.arange(len(gross)) // 5
    block_count = int(block.max()) + 1
    rng = np.random.default_rng(seed)
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(4096, block_count))
    null_net = (signs[:, block] * gross).sum(axis=1) - costs.sum()
    observed = gross.sum() - costs.sum()
    return float((1 + np.count_nonzero(null_net >= observed)) / (len(null_net) + 1))


def _concentration(
    events: pd.DataFrame, fold_results: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    positive = events.loc[events["net_pnl_60"] > 0, "net_pnl_60"]
    positive_total = float(positive.sum())
    best_event_share = float(positive.max() / positive_total) if positive_total > 0 else 1.0
    fold_positive = [max(float(row["net_pnl"]), 0.0) for row in fold_results.values()]
    fold_total = sum(fold_positive)
    best_fold_share = max(fold_positive, default=0.0) / fold_total if fold_total > 0 else 1.0
    return {
        "best_event_share": best_event_share,
        "best_fold_share": best_fold_share,
        "event_dominated": bool(best_event_share > 0.25 or best_fold_share > 0.70),
    }


def _account_replay(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "rule_version": "topstep_150k_2026-07-10_no_dll_baseline",
            "micro_one_contract_mll_safe": False,
            "path_candidate": False,
            "reason": "no_micro_events",
        }
    daily = (
        events.groupby("event_session_id", sort=True)
        .agg(
            pnl=("net_pnl_60", "sum"),
            raw_pnl=("net_pnl_60", "sum"),
            worst_intraday_pnl=("mae_dollars", "min"),
            trades=("net_pnl_60", "size"),
        )
        .reset_index(names="date")
    )
    daily["skipped_trades"] = 0
    daily["hit_daily_stop"] = False
    daily["hit_daily_profit_lock"] = False
    config = Topstep150KConfig()
    one_micro = simulate_combine(daily, config)
    micro_safe = bool(
        not one_micro["mll_breached"] and float(one_micro["min_mll_buffer"]) >= 1000.0
    )
    scaled = daily.copy()
    scaled[["pnl", "raw_pnl", "worst_intraday_pnl"]] *= 10.0
    combine = simulate_combine(scaled, config)
    standard = simulate_xfa_standard(scaled, mll_distance=config.combine_max_loss_limit)
    consistency = simulate_xfa_consistency(scaled, mll_distance=config.combine_max_loss_limit)
    return {
        "rule_version": "topstep_150k_2026-07-10_no_dll_baseline",
        "micro_one_contract_mll_safe": micro_safe,
        "micro_one_contract_min_mll_buffer": float(one_micro["min_mll_buffer"]),
        "ten_micro_combine": combine,
        "ten_micro_xfa_standard": standard,
        "ten_micro_xfa_consistency": consistency,
        "path_candidate": bool(
            combine["passed"]
            and not combine["mll_breached"]
            and combine["consistency_ok"]
        ),
        "shared_account_portfolio_replay_required": True,
    }


def _shadow_specification_contract_valid(
    *,
    mini: str,
    micro: str,
    candidate_id: str,
    preregistration_hash: str,
    direction: str = "contrarian",
) -> bool:
    try:
        _base_shadow_specification(
            candidate_id=candidate_id,
            mini=mini,
            micro=micro,
            preregistration_hash=preregistration_hash,
            direction=direction,
        ).validate()
        return True
    except ValueError:
        return False


def _shadow_specification(
    candidate: dict[str, Any], *, preregistration_hash: str
) -> ShadowSpecification:
    return _base_shadow_specification(
        candidate_id=str(candidate["candidate_id"]),
        mini=str(candidate["primary_market"]),
        micro=str(candidate["execution_market"]),
        preregistration_hash=preregistration_hash,
        direction=str(candidate.get("entry_direction") or "contrarian"),
    )


def _base_shadow_specification(
    *,
    candidate_id: str,
    mini: str,
    micro: str,
    preregistration_hash: str,
    direction: str = "contrarian",
) -> ShadowSpecification:
    return ShadowSpecification(
        strategy_id=candidate_id,
        strategy_version="v1_pre_q4_shadow_research",
        feature_versions=("rth_open_gap_past_only_expanding_q75_v1",),
        markets=(micro,),
        timeframes=("1m", "session"),
        session_rules={
            "timezone": "America/Chicago",
            "decision_after_source_bar_close": "08:31",
            "mandatory_flatten_before": "15:10",
            "reference_market": mini,
        },
        entry_rules={
            "event": "absolute_open_gap_ge_past_only_expanding_q75",
            "direction": direction,
            "minimum_prior_sessions": 40,
        },
        exit_rules={"holding_completed_1m_bars": 60, "no_overnight": True},
        sizing={"contracts": 1, "instrument": micro, "micro_first": True},
        costs={
            "round_turn_usd": _round_turn_cost(micro),
            "slippage_ticks_round_turn": 2,
        },
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=23 * 60 * 60,
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
        source_manifest_hash=preregistration_hash,
        outbound_orders_enabled=False,
    )


def _round_turn_cost(symbol: str) -> float:
    spec = instrument_spec(str(symbol))
    return float(COMMISSION_ROUND_TURN[str(symbol)] + 2.0 * spec.tick_value)


def _integrity_proof(events: pd.DataFrame) -> dict[str, bool]:
    primary = events[events["primary_event"]]
    return {
        "nonempty_event_source": bool(len(events)),
        "decision_after_source_close": bool(
            (events["decision_timestamp"] == events["timestamp"] + pd.Timedelta(minutes=1)).all()
        ),
        "reference_strictly_past": bool((events["reference_timestamp"] < events["timestamp"]).all()),
        "same_explicit_contract_reference": bool(events["active_contract"].notna().all()),
        "past_only_threshold": bool((primary["past_gap_count"] >= 40).all()),
        "exact_future_horizon": bool(
            (
                primary["exit_timestamp_60"]
                == primary["timestamp"] + pd.Timedelta(minutes=60)
            ).all()
        ),
        "q4_excluded": bool(
            pd.to_datetime(events["timestamp"], utc=True).max()
            < pd.Timestamp("2024-10-01", tz="UTC")
        ),
        "one_event_per_market_session": bool(
            events.groupby(["symbol", "event_session_id"]).size().max() <= 1
        ),
        "finite_primary_costs": bool(
            np.isfinite(primary["cost"].to_numpy(dtype=float)).all()
        ),
    }


def _preregistration(
    *,
    engineering_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "equity_open_gap_reversal_preregistration_v1",
        "strategy_family_id": "equity_rth_open_gap_reversal_20260711_v1",
        "candidate_ids": [
            f"strategy_open_gap_reversal_{symbol}_v1" for symbol in MARKET_PAIRS
        ],
        "market_pairs": MARKET_PAIRS,
        "primary_horizon_minutes": 60,
        "primary_threshold_quantile": 0.75,
        "minimum_prior_sessions": 40,
        "decision_time_chicago": "08:31",
        "reference_bar_open_time_chicago": "14:59",
        "folds": FOLDS,
        "costs": {symbol: _round_turn_cost(symbol) for symbol in SYMBOLS},
        "diagnostics": {
            "threshold_quantiles": [0.65, 0.85],
            "holding_minutes": [30, 90],
            "entry_delay_bars": 1,
            "sign_flip": True,
            "block_sign_flip_draws": 4096,
            "family_test_count": 4,
        },
        "task_sha256": engineering_task_sha256,
        "map_sha256": repaired_map_sha256,
        "roll_map_hash": repaired_roll_map_hash,
        "source_preregistration_sha256": SOURCE_PREREGISTRATION_SHA256,
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


def _record_development_access_once(candidate_ids: list[str]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "equity RTH open-gap reversal strategy-level pilot; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.equity_open_gap_reversal"
                and sorted(row.get("candidate_ids") or []) == sorted(candidate_ids)
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.equity_open_gap_reversal",
        candidate_ids,
        reason,
        None,
    )
    return record.__dict__


def _write_trade_ledger(path: Path, events: pd.DataFrame) -> None:
    selected = events[events["primary_event"]].copy()
    columns = [
        "symbol",
        "active_contract",
        "event_session_id",
        "timestamp",
        "decision_timestamp",
        "reference_timestamp",
        "previous_rth_close",
        "rth_open_price",
        "entry_price",
        "exit_timestamp_60",
        "exit_price_60",
        "side",
        "threshold_q75",
        "gap_points",
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
    rows = [
        "# Equity RTH Open-Gap Reversal Pilot",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Candidate tiers: `{payload['candidate_tier_counts']}`",
        f"- Shadow research candidates: `{payload['shadow_candidates']}`",
        f"- PAPER_SHADOW_READY: `{payload['paper_shadow_ready']}`",
        f"- Topstep path candidates: `{payload['topstep_path_candidates']}`",
        "- Q4 access: `0`",
        "- Outbound orders: `0`",
        "",
        "## Candidate results",
        "",
    ]
    for candidate in payload["candidates"]:
        rows.extend(
            [
                f"### {candidate['candidate_id']}",
                "",
                f"- Status: `{candidate['status']}`",
                f"- Events / net: `{candidate['events']}` / `{candidate['net_pnl']:.2f}`",
                f"- Supportive folds: `{candidate['supportive_temporal_folds']}`",
                f"- Family-adjusted null p: `{candidate['null_evidence']['family_adjusted_probability']:.6f}`",
                f"- Micro transfer: `{candidate['contract_transfer']['passed']}`",
                f"- MLL safe: `{candidate['topstep']['micro_one_contract_mll_safe']}`",
                "",
            ]
        )
    rows.extend(["## Interpretation boundary", "", payload["interpretation_boundary"], ""])
    return "\n".join(rows)


def _verify_file(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected_sha256:
        raise EquityOpenGapReversalError(f"Frozen {label} is missing or changed: {path}")


def _write_immutable(path: Path, content: str) -> None:
    if path.exists() and path.read_text(encoding="utf-8") != content:
        raise EquityOpenGapReversalError(f"Refusing divergent immutable artifact: {path}")
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
