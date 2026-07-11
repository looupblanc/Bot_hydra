from __future__ import annotations

import hashlib
import itertools
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.propfirm.topstep_150k import Topstep150KConfig, simulate_combine
from hydra.research.equity_open_gap_reversal import _write_immutable


VERSION = "shadow_shared_account_baskets_v1"
PERIOD_START = "2024-01-01"
PERIOD_END = "2024-10-01"
YM_ID = "strategy_open_gap_continuation_YM_v1"
NQ_ID = (
    "strategy_barrier_hazard_NQ_signed_extreme_recovery_60_middle_q65_"
    "h30_s100_15m_expansion_v1"
)
MCL_ID = (
    "strategy_session_geometry_CL_signal_MCL_execution_overnight_extreme_"
    "position_continuation_q65_h60_prior_trend_agree_v2"
)
DAILY_ID = (
    "strategy_daily_cross_CL_to_YM_source_prior_trend_"
    "continuation_q80_h120_v1"
)
CANDIDATE_IDS = (YM_ID, NQ_ID, MCL_ID, DAILY_ID)


class ShadowSharedAccountError(RuntimeError):
    pass


def run_shadow_shared_account_baskets(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    sources: list[dict[str, Any]],
    code_commit: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    _verify(Path(engineering_task_path), engineering_task_sha256, "engineering task")
    if len(code_commit) == 40:
        current = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if current != code_commit:
            raise ShadowSharedAccountError(
                "Worker commit differs from queued specification."
            )
    if {str(source.get("candidate_id")) for source in sources} != set(CANDIDATE_IDS):
        raise ShadowSharedAccountError("Source candidate set is incomplete or duplicated.")
    normalized: dict[str, pd.DataFrame] = {}
    provenance: list[dict[str, Any]] = []
    for source in sorted(sources, key=lambda row: str(row["candidate_id"])):
        candidate_id = str(source["candidate_id"])
        result_path = Path(str(source["result_path"]))
        ledger_path = Path(str(source["ledger_path"]))
        _verify(result_path, str(source["result_sha256"]), f"{candidate_id} result")
        _verify(ledger_path, str(source["ledger_sha256"]), f"{candidate_id} ledger")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        if result.get("result_hash") != source.get("result_hash"):
            raise ShadowSharedAccountError(f"Semantic result hash changed: {candidate_id}")
        candidates = [
            row
            for row in result.get("candidates") or []
            if row.get("candidate_id") == candidate_id
        ]
        if (
            len(candidates) != 1
            or candidates[0].get("status") != "SHADOW_RESEARCH_CANDIDATE"
        ):
            raise ShadowSharedAccountError(
                f"Source does not contain one shadow candidate: {candidate_id}"
            )
        rows = [
            json.loads(line)
            for line in ledger_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        events = normalize_candidate_trades(candidate_id, rows)
        expected = dict(source.get("expected_2024") or {})
        if (
            len(events) != int(expected.get("events", -1))
            or not np.isclose(
                float(events["net_pnl"].sum()),
                float(expected.get("net_pnl", np.nan)),
                atol=1e-8,
            )
        ):
            raise ShadowSharedAccountError(
                f"Normalized trade total differs from frozen evidence: {candidate_id}"
            )
        normalized[candidate_id] = events
        provenance.append(
            {
                "candidate_id": candidate_id,
                "result_hash": source["result_hash"],
                "result_sha256": source["result_sha256"],
                "ledger_sha256": source["ledger_sha256"],
                "events": len(events),
                "net_pnl": float(events["net_pnl"].sum()),
            }
        )
    pairwise = pairwise_interactions(normalized)
    evaluations: list[dict[str, Any]] = []
    for size in range(2, len(CANDIDATE_IDS) + 1):
        for candidate_ids in itertools.combinations(sorted(CANDIDATE_IDS), size):
            evaluations.append(
                evaluate_basket(candidate_ids, normalized, pairwise=pairwise)
            )
    selected = select_basket_roles(evaluations)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    configurations: list[dict[str, Any]] = []
    for basket in selected:
        configuration: dict[str, Any] = {
            "schema": "hydra_shadow_shared_account_configuration_v1",
            "basket_id": basket["basket_id"],
            "role": basket["role"],
            "candidate_ids": basket["candidate_ids"],
            "sizing": {candidate_id: 1 for candidate_id in basket["candidate_ids"]},
            "contract_unit": "one_micro_contract_per_active_strategy",
            "shared_account": "TOPSTEP_150K_SIMULATED",
            "shared_mll_distance": 4500.0,
            "internal_daily_risk_limit": 1000.0,
            "maximum_simultaneous_micro_contracts": basket[
                "maximum_simultaneous_contracts"
            ],
            "signal_conflict_policy": "fail_closed_skip_new_same_underlying_opposite_signal",
            "stale_data_policy": "fail_closed_no_signal_no_fill",
            "duplicate_signal_policy": "candidate_and_session_unique",
            "mandatory_flatten": True,
            "outbound_orders_enabled": False,
            "broker_connections_allowed": 0,
            "virtual_execution_only": True,
            "source_result_hashes": {
                row["candidate_id"]: row["result_hash"]
                for row in provenance
                if row["candidate_id"] in basket["candidate_ids"]
            },
        }
        configuration["configuration_hash"] = _stable_hash(configuration)
        path = destination / "basket_configurations" / f"{basket['basket_id']}.json"
        _write_immutable(path, json.dumps(configuration, indent=2, sort_keys=True) + "\n")
        configurations.append(
            {
                "basket_id": basket["basket_id"],
                "role": basket["role"],
                "path": str(path),
                "configuration_hash": configuration["configuration_hash"],
                "outbound_orders_enabled": False,
            }
        )
    executable_count = sum(bool(row["executable"]) for row in selected)
    conclusion = (
        "THREE_EXECUTABLE_SHADOW_BASKETS_FOUND"
        if executable_count >= 3
        else "SHADOW_BASKETS_INSUFFICIENT_OR_RISK_BLOCKED"
    )
    manifest: dict[str, Any] = {
        "schema": VERSION,
        "source_candidates": provenance,
        "period_start": PERIOD_START,
        "period_end_exclusive": PERIOD_END,
        "pairwise_interactions": pairwise,
        "evaluated_subset_count": len(evaluations),
        "basket_evaluations": evaluations,
        "selected_baskets": selected,
        "configurations": configurations,
        "standalone_payouts_summed": False,
        "shared_account_recomputed": True,
        "q4_access_allowed": False,
        "outbound_orders_enabled": False,
        "code_commit": code_commit,
    }
    manifest["manifest_hash"] = _stable_hash(manifest)
    manifest_path = destination / "shadow_shared_account_basket_manifest.json"
    _write_immutable(
        manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Baskets recompute one shared development-period account and authorize only "
            "zero-order forward shadow research. They are not Paper or funded evidence."
        ),
        "code_commit": code_commit,
        "candidate_count": 0,
        "candidates": [],
        "basket_count": len(selected),
        "executable_baskets": executable_count,
        "selected_baskets": selected,
        "pairwise_interactions": pairwise,
        "manifest_hash": manifest["manifest_hash"],
        "manifest_path": str(manifest_path),
        "basket_configurations": configurations,
        "paper_shadow_ready": 0,
        "governance": {
            "q4_access_count_delta": 0,
            "market_data_rows_read": 0,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "performance": {"total_seconds": time.perf_counter() - started},
        "next_recommended_action": (
            "RUN_BASKETS_IN_ZERO_ORDER_SHADOW_AND_CONTINUE_DISTRIBUTIONAL_SEARCH"
            if executable_count >= 3
            else "RESEARCH_SHARED_RISK_REMEDIATION_AND_DISTRIBUTIONAL_EDGE"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "shadow_shared_account_basket_result.json"
    report_path = destination / "shadow_shared_account_basket_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "manifest_path": str(manifest_path),
        },
        "report_path": str(report_path),
    }


def normalize_candidate_trades(
    candidate_id: str, rows: list[dict[str, Any]]
) -> pd.DataFrame:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if candidate_id == YM_ID:
            if row.get("symbol") != "MYM":
                continue
            entry_key, exit_key, pnl_key = (
                "decision_timestamp",
                "exit_timestamp_60",
                "net_pnl_60",
            )
        elif candidate_id == NQ_ID:
            if row.get("contract_role") != "primary_micro":
                continue
            entry_key, exit_key, pnl_key = "entry_timestamp", "exit_timestamp", "net_pnl"
        elif candidate_id == MCL_ID:
            if row.get("candidate_id") != MCL_ID:
                continue
            entry_key, exit_key, pnl_key = "entry_timestamp", "exit_timestamp", "net_pnl"
        elif candidate_id == DAILY_ID:
            if row.get("candidate_id") != DAILY_ID:
                continue
            entry_key, exit_key, pnl_key = "entry_timestamp", "exit_timestamp", "net_pnl"
        else:
            raise ShadowSharedAccountError(f"Unknown candidate: {candidate_id}")
        entry = pd.Timestamp(row[entry_key])
        if not (pd.Timestamp(PERIOD_START, tz="UTC") <= entry < pd.Timestamp(PERIOD_END, tz="UTC")):
            continue
        normalized.append(
            {
                "trade_id": f"{candidate_id}:{len(normalized):04d}",
                "candidate_id": candidate_id,
                "entry_timestamp": entry,
                "exit_timestamp": pd.Timestamp(row[exit_key]),
                "event_session_id": str(
                    row.get("event_session_id") or row.get("trading_session_id")
                ),
                "symbol": str(row["symbol"]),
                "underlying": _underlying(str(row["symbol"])),
                "side": float(row["side"]),
                "cost": float(row.get("cost") or 0.0),
                "net_pnl": float(row[pnl_key]),
                "mae_dollars": float(row.get("mae_dollars") or 0.0),
            }
        )
    frame = pd.DataFrame(normalized)
    if frame.empty:
        raise ShadowSharedAccountError(f"No normalized trades: {candidate_id}")
    if (frame["exit_timestamp"] < frame["entry_timestamp"]).any():
        raise ShadowSharedAccountError(f"Negative holding interval: {candidate_id}")
    return frame.sort_values(["entry_timestamp", "trade_id"]).reset_index(drop=True)


def pairwise_interactions(
    normalized: dict[str, pd.DataFrame]
) -> list[dict[str, Any]]:
    dates = sorted(
        {
            str(date)
            for frame in normalized.values()
            for date in frame["event_session_id"].unique()
        }
    )
    daily = {
        candidate_id: frame.groupby("event_session_id")["net_pnl"]
        .sum()
        .reindex(dates, fill_value=0.0)
        for candidate_id, frame in normalized.items()
    }
    output: list[dict[str, Any]] = []
    for left_id, right_id in itertools.combinations(sorted(normalized), 2):
        left, right = normalized[left_id], normalized[right_id]
        interval_overlap = 0
        conflicts = 0
        for left_row in left.itertuples(index=False):
            overlaps = right[
                (right["entry_timestamp"] < left_row.exit_timestamp)
                & (right["exit_timestamp"] > left_row.entry_timestamp)
            ]
            interval_overlap += len(overlaps)
            conflicts += int(
                (
                    overlaps["underlying"].eq(left_row.underlying)
                    & np.sign(overlaps["side"]).ne(np.sign(left_row.side))
                ).sum()
            )
        left_daily, right_daily = daily[left_id], daily[right_id]
        correlation = float(left_daily.corr(right_daily))
        if not np.isfinite(correlation):
            correlation = 0.0
        shared_loss_days = int(((left_daily < 0) & (right_daily < 0)).sum())
        left_active = left_daily[left_daily.ne(0.0)]
        right_active = right_daily[right_daily.ne(0.0)]
        left_threshold = (
            float(left_active.quantile(0.20)) if len(left_active) else float("-inf")
        )
        right_threshold = (
            float(right_active.quantile(0.20)) if len(right_active) else float("-inf")
        )
        left_tail = left_daily.ne(0.0) & left_daily.le(left_threshold)
        right_tail = right_daily.ne(0.0) & right_daily.le(right_threshold)
        output.append(
            {
                "left": left_id,
                "right": right_id,
                "daily_pnl_correlation": correlation,
                "interval_overlap_count": int(interval_overlap),
                "same_underlying_conflict_count": int(conflicts),
                "shared_loss_days": shared_loss_days,
                "joint_tail_days": int((left_tail & right_tail).sum()),
            }
        )
    return output


def evaluate_basket(
    candidate_ids: tuple[str, ...],
    normalized: dict[str, pd.DataFrame],
    *,
    pairwise: list[dict[str, Any]],
) -> dict[str, Any]:
    events = pd.concat(
        [normalized[candidate_id] for candidate_id in candidate_ids],
        ignore_index=True,
    ).sort_values(["entry_timestamp", "trade_id"])
    daily = conservative_shared_daily_path(events)
    stressed_events = events.copy()
    stressed_events["net_pnl"] -= stressed_events["cost"] * 0.5
    stressed_events["mae_dollars"] -= stressed_events["cost"] * 0.5
    stressed_daily = conservative_shared_daily_path(stressed_events)
    combine = simulate_combine(daily, Topstep150KConfig())
    stressed_combine = simulate_combine(stressed_daily, Topstep150KConfig())
    simultaneous = _maximum_simultaneous(events)
    relevant = [
        row
        for row in pairwise
        if row["left"] in candidate_ids and row["right"] in candidate_ids
    ]
    maximum_correlation = max(
        (abs(float(row["daily_pnl_correlation"])) for row in relevant),
        default=0.0,
    )
    conflicts = sum(int(row["same_underlying_conflict_count"]) for row in relevant)
    cost_stressed_net = float(stressed_events["net_pnl"].sum())
    executable = bool(
        not combine["mll_breached"]
        and float(combine["min_mll_buffer"]) >= 1000.0
        and not stressed_combine["mll_breached"]
        and cost_stressed_net > 0
        and simultaneous <= 15
        and conflicts == 0
    )
    basket_id = "basket_" + _stable_hash(
        {"candidate_ids": list(candidate_ids), "version": VERSION}
    )[:16]
    return {
        "basket_id": basket_id,
        "candidate_ids": list(candidate_ids),
        "candidate_count": len(candidate_ids),
        "total_net_pnl": float(events["net_pnl"].sum()),
        "cost_stress_1_5x_net": cost_stressed_net,
        "shared_loss_days": int((daily["pnl"] < 0).sum()),
        "maximum_simultaneous_contracts": int(simultaneous),
        "same_underlying_conflict_count": int(conflicts),
        "maximum_absolute_daily_correlation": float(maximum_correlation),
        "shared_account_combine": combine,
        "cost_stressed_shared_account_combine": stressed_combine,
        "executable": executable,
        "standalone_payouts_summed": False,
    }


def conservative_shared_daily_path(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for session_id, day in events.groupby("event_session_id", sort=True):
        actions: list[tuple[pd.Timestamp, int, str, float, float]] = []
        for trade in day.itertuples(index=False):
            actions.append(
                (
                    trade.entry_timestamp,
                    0,
                    trade.trade_id,
                    min(float(trade.mae_dollars), 0.0),
                    0.0,
                )
            )
            actions.append(
                (
                    trade.exit_timestamp,
                    1,
                    trade.trade_id,
                    0.0,
                    float(trade.net_pnl),
                )
            )
        actions.sort(key=lambda row: (row[0], row[1], row[2]))
        realized = 0.0
        open_mae: dict[str, float] = {}
        worst = 0.0
        for _timestamp, kind, trade_id, mae, pnl in actions:
            if kind == 0:
                open_mae[trade_id] = mae
            else:
                open_mae.pop(trade_id, None)
                realized += pnl
            worst = min(worst, realized + sum(open_mae.values()))
        rows.append(
            {
                "date": str(session_id),
                "pnl": float(day["net_pnl"].sum()),
                "raw_pnl": float(day["net_pnl"].sum()),
                "worst_intraday_pnl": float(worst),
                "trades": int(len(day)),
                "skipped_trades": 0,
                "hit_daily_stop": False,
                "hit_daily_profit_lock": False,
            }
        )
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def select_basket_roles(evaluations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [row for row in evaluations if row["executable"]]
    if not eligible:
        return []
    role_rankings = (
        (
            "maximum_mll_survival",
            lambda row: (
                -float(row["shared_account_combine"]["min_mll_buffer"]),
                -float(row["cost_stress_1_5x_net"]),
                row["candidate_ids"],
            ),
        ),
        (
            "balanced_progress",
            lambda row: (
                -float(row["cost_stress_1_5x_net"]),
                -float(row["shared_account_combine"]["total_profit"]),
                row["candidate_ids"],
            ),
        ),
        (
            "low_correlation_diversity",
            lambda row: (
                float(row["maximum_absolute_daily_correlation"]),
                -len(row["candidate_ids"]),
                -float(row["cost_stress_1_5x_net"]),
                row["candidate_ids"],
            ),
        ),
    )
    selected: list[dict[str, Any]] = []
    used_baskets: set[str] = set()
    for role, ranking in role_rankings:
        choices = sorted(
            (row for row in eligible if row["basket_id"] not in used_baskets),
            key=ranking,
        )
        if not choices:
            break
        chosen = {**choices[0], "role": role}
        selected.append(chosen)
        used_baskets.add(chosen["basket_id"])
    return selected


def _maximum_simultaneous(events: pd.DataFrame) -> int:
    actions: list[tuple[pd.Timestamp, int]] = []
    for row in events.itertuples(index=False):
        actions.append((row.entry_timestamp, 1))
        actions.append((row.exit_timestamp, -1))
    actions.sort(key=lambda row: (row[0], -row[1]))
    active = maximum = 0
    for _timestamp, delta in actions:
        active += delta
        maximum = max(maximum, active)
    return maximum


def _underlying(symbol: str) -> str:
    return {
        "MYM": "YM",
        "MNQ": "NQ",
        "MCL": "CL",
        "MGC": "GC",
    }.get(symbol, symbol)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ShadowSharedAccountError(f"Frozen {label} missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Shadow Shared-Account Baskets",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Evaluated baskets: `11`",
        f"- Selected baskets: `{payload['basket_count']}`",
        f"- Executable baskets: `{payload['executable_baskets']}`",
        "- Shared-account replay: `true`",
        "- Standalone payouts summed: `false`",
        "- PAPER_SHADOW_READY: `0`",
        "- Q4 access delta: `0`",
        "- Outbound orders: `0`",
        "",
    ]
    for basket in payload["selected_baskets"]:
        lines.extend(
            [
                f"## {basket['role']}",
                "",
                f"- ID: `{basket['basket_id']}`",
                f"- Candidates: `{', '.join(basket['candidate_ids'])}`",
                f"- Net: `{basket['total_net_pnl']}`",
                f"- 1.5x-cost net: `{basket['cost_stress_1_5x_net']}`",
                f"- Minimum MLL buffer: `{basket['shared_account_combine']['min_mll_buffer']}`",
                f"- Executable: `{str(basket['executable']).lower()}`",
                "",
            ]
        )
    return "\n".join(lines)
