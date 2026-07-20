"""Direction-neutral scheduled-release OCO bracket preflight.

The release clock is exogenous and frozen before outcomes.  The policy never
predicts direction: both stop-entry levels are armed from completed pre-release
bars.  One-minute dual-touch ambiguity is charged as an immediate full-stop
loss, so the OHLCV preflight cannot manufacture an optimistic ordering.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.economic_evolution.schema import stable_hash
from hydra.research.fx_causal_ecology import (
    POINT_VALUES,
    ROOTS,
    TICK_SIZES,
    RawTrade,
    _account_score,
    _eligible_days,
    _rule_configs,
    account_frontier,
    build_panel,
    load_inputs,
)


SCHEMA = "hydra_direction_neutral_release_bracket_preflight_v1"
DEFAULT_MANIFEST = Path("config/research/direction_neutral_release_bracket_preflight_v1.json")
ROLE_DATES = {
    "DISCOVERY": ("2023-01-01", "2023-09-01"),
    "VALIDATION": ("2023-09-01", "2024-01-01"),
    "FINAL_DEVELOPMENT": ("2024-01-01", "2024-10-01"),
}


class ReleaseBracketError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReleaseEvent:
    family: str
    release_ns: int
    event_id: str


@dataclass(frozen=True, slots=True)
class BracketPolicy:
    root: str
    release_scope: str
    lookback_minutes: int
    trigger_buffer_fraction: float
    trigger_window_minutes: int
    stop_range_fraction: float
    target_r: float
    holding_minutes: int

    @property
    def policy_id(self) -> str:
        return "release_oco_" + stable_hash(asdict(self))[:20]


@dataclass(frozen=True, slots=True)
class ReplayDiagnostics:
    scheduled_events: int
    eligible_brackets: int
    triggered_trades: int
    no_trigger: int
    dual_touch_losses: int
    missing_lookback: int
    roll_rejected: int


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def load_contract(root: Path) -> tuple[dict[str, Any], tuple[ReleaseEvent, ...], Mapping[str, Any], Mapping[str, Any]]:
    manifest_path = root / DEFAULT_MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if claimed != _canonical_hash(core):
        raise ReleaseBracketError("release bracket manifest hash drift")
    calendar_path = root / manifest["data"]["release_calendar"]
    if sha256_file(calendar_path) != manifest["data"]["release_calendar_sha256"]:
        raise ReleaseBracketError("release calendar hash drift")
    rule_path = root / manifest["official_rule_snapshot"]["path"]
    if sha256_file(rule_path) != manifest["official_rule_snapshot"]["file_sha256"]:
        raise ReleaseBracketError("official rule snapshot hash drift")
    calendar = json.loads(calendar_path.read_text(encoding="utf-8"))
    if calendar.get("verification", {}).get("official_archive_content_mismatch_count") != 0:
        raise ReleaseBracketError("official calendar is not verified")
    events = []
    for ordinal, row in enumerate(calendar["events"]):
        stamp = pd.Timestamp(row["release_utc"])
        if stamp.tzinfo is None:
            raise ReleaseBracketError("release timestamp lacks timezone")
        events.append(
            ReleaseEvent(
                family=str(row["family"]),
                release_ns=int(stamp.value),
                event_id=f"{row['family']}:{stamp.isoformat()}:{ordinal}",
            )
        )
    if any(left.release_ns >= right.release_ns for left, right in zip(events, events[1:])):
        raise ReleaseBracketError("release calendar is not strictly chronological")
    rules = json.loads(rule_path.read_text(encoding="utf-8"))
    return manifest, tuple(events), calendar, rules


def frozen_policies() -> tuple[BracketPolicy, ...]:
    output = []
    for root in ROOTS:
        for scope in ("ALL_RELEASES", "BLS_CPI", "BLS_EMPLOYMENT_SITUATION", "FOMC_STATEMENT"):
            for lookback in (5, 15):
                for buffer in (0.0, 0.25):
                    for trigger_window in (1, 5):
                        for stop_fraction in (0.5, 1.0):
                            for target_r in (1.0, 2.0):
                                for holding in (5, 15, 30):
                                    output.append(
                                        BracketPolicy(
                                            root=root,
                                            release_scope=scope,
                                            lookback_minutes=lookback,
                                            trigger_buffer_fraction=buffer,
                                            trigger_window_minutes=trigger_window,
                                            stop_range_fraction=stop_fraction,
                                            target_r=target_r,
                                            holding_minutes=holding,
                                        )
                                    )
    return tuple(output)


def _events_for_role(
    events: Sequence[ReleaseEvent], role: tuple[str, str], scope: str, *, offset_days: int = 0
) -> tuple[ReleaseEvent, ...]:
    start_ns = int(pd.Timestamp(role[0], tz="UTC").value)
    end_ns = int(pd.Timestamp(role[1], tz="UTC").value)
    delta = offset_days * 86_400_000_000_000
    return tuple(
        ReleaseEvent(event.family, event.release_ns + delta, f"{event.event_id}:offset={offset_days}")
        for event in events
        if start_ns <= event.release_ns + delta < end_ns
        and (scope == "ALL_RELEASES" or scope == event.family)
    )


def materialize_brackets(
    policy: BracketPolicy,
    events: Sequence[ReleaseEvent],
    panel: Mapping[str, Any],
    role: tuple[str, str],
    *,
    event_offset_days: int = 0,
) -> tuple[tuple[RawTrade, ...], ReplayDiagnostics]:
    timestamps: pd.DatetimeIndex = panel["timestamps"]
    timestamp_ns = np.asarray(panel["timestamp_ns"], dtype=np.int64)
    root = policy.root
    tick = TICK_SIZES[root]
    point = POINT_VALUES[root]
    selected = _events_for_role(events, role, policy.release_scope, offset_days=event_offset_days)
    trades: list[RawTrade] = []
    diagnostics = {
        "scheduled_events": len(selected),
        "eligible_brackets": 0,
        "triggered_trades": 0,
        "no_trigger": 0,
        "dual_touch_losses": 0,
        "missing_lookback": 0,
        "roll_rejected": 0,
    }
    last_exit_ns = -1
    for ordinal, event in enumerate(selected):
        release_ns = int(event.release_ns)
        lookback_start = release_ns - policy.lookback_minutes * 60_000_000_000
        left = int(np.searchsorted(timestamp_ns, lookback_start, side="left"))
        right = int(np.searchsorted(timestamp_ns, release_ns, side="left"))
        if right <= left or right >= len(timestamp_ns):
            diagnostics["missing_lookback"] += 1
            continue
        expected = policy.lookback_minutes
        indices = np.arange(left, right, dtype=int)
        recent_ns = timestamp_ns[indices]
        exact = (
            len(indices) == expected
            and recent_ns[0] == lookback_start
            and recent_ns[-1] == release_ns - 60_000_000_000
            and np.all(np.diff(recent_ns) == 60_000_000_000)
        )
        highs = pd.to_numeric(panel["high"][root].iloc[left:right], errors="coerce").to_numpy(float)
        lows = pd.to_numeric(panel["low"][root].iloc[left:right], errors="coerce").to_numpy(float)
        contracts = pd.to_numeric(panel["contract_id"][root].iloc[left:right], errors="coerce").to_numpy(float)
        if not exact or not np.all(np.isfinite(highs + lows + contracts)):
            diagnostics["missing_lookback"] += 1
            continue
        frozen_contract = int(contracts[-1])
        if np.any(contracts.astype(np.int64) != frozen_contract):
            diagnostics["roll_rejected"] += 1
            continue
        pre_high = float(np.max(highs))
        pre_low = float(np.min(lows))
        pre_range = pre_high - pre_low
        if not math.isfinite(pre_range) or pre_range < 2.0 * tick:
            diagnostics["missing_lookback"] += 1
            continue
        upper = math.ceil((pre_high + policy.trigger_buffer_fraction * pre_range) / tick - 1e-10) * tick
        lower = math.floor((pre_low - policy.trigger_buffer_fraction * pre_range) / tick + 1e-10) * tick
        stop_distance = max(policy.stop_range_fraction * pre_range, 2.0 * tick)
        stop_distance = math.ceil(stop_distance / tick - 1e-10) * tick
        trigger_end_ns = release_ns + policy.trigger_window_minutes * 60_000_000_000
        trigger_left = int(np.searchsorted(timestamp_ns, release_ns, side="left"))
        trigger_right = int(np.searchsorted(timestamp_ns, trigger_end_ns, side="left"))
        diagnostics["eligible_brackets"] += 1
        entry_index = None
        direction = 0
        dual_touch = False
        entry_price = math.nan
        for index in range(trigger_left, min(trigger_right, len(timestamp_ns))):
            if int(timestamp_ns[index]) <= last_exit_ns:
                continue
            contract = panel["contract_id"][root].iat[index]
            if pd.isna(contract) or int(contract) != frozen_contract:
                continue
            if bool(panel["flatten"][index]):
                continue
            open_price = float(panel["open"][root].iat[index])
            high = float(panel["high"][root].iat[index])
            low = float(panel["low"][root].iat[index])
            if not math.isfinite(open_price + high + low):
                continue
            up = high >= upper
            down = low <= lower
            if not up and not down:
                continue
            entry_index = index
            dual_touch = bool(up and down)
            if dual_touch:
                direction = 1
                entry_price = max(open_price, upper)
            elif up:
                direction = 1
                entry_price = max(open_price, upper)
            else:
                direction = -1
                entry_price = min(open_price, lower)
            break
        if entry_index is None:
            diagnostics["no_trigger"] += 1
            continue
        diagnostics["triggered_trades"] += 1
        entry_ns = int(timestamp_ns[entry_index])
        session_day = int(panel["session_day"][entry_index])
        if dual_touch:
            diagnostics["dual_touch_losses"] += 1
            exit_index = entry_index
            exit_price = entry_price - direction * stop_distance
            best_raw = 0.0
            worst_raw = -stop_distance
        else:
            stop_price = entry_price - direction * stop_distance
            target_price = entry_price + direction * stop_distance * policy.target_r
            deadline_ns = entry_ns + policy.holding_minutes * 60_000_000_000
            exit_index = None
            exit_price = math.nan
            best_raw = 0.0
            worst_raw = 0.0
            last_valid = None
            for index in range(entry_index, len(timestamp_ns)):
                now_ns = int(timestamp_ns[index])
                contract = panel["contract_id"][root].iat[index]
                if now_ns >= deadline_ns:
                    if not pd.isna(contract) and int(contract) == frozen_contract:
                        candidate = float(panel["open"][root].iat[index])
                        if math.isfinite(candidate):
                            exit_index, exit_price = index, candidate
                    break
                if panel["session_day"][index] != session_day or bool(panel["flatten"][index]):
                    candidate = float(panel["open"][root].iat[index])
                    if math.isfinite(candidate):
                        exit_index, exit_price = index, candidate
                    break
                if pd.isna(contract):
                    continue
                if int(contract) != frozen_contract:
                    if last_valid is not None:
                        exit_index = last_valid
                        exit_price = float(panel["close"][root].iat[last_valid])
                    break
                high = float(panel["high"][root].iat[index])
                low = float(panel["low"][root].iat[index])
                if not math.isfinite(high + low):
                    continue
                last_valid = index
                favorable_move = high - entry_price if direction > 0 else entry_price - low
                adverse_move = low - entry_price if direction > 0 else entry_price - high
                best_raw = max(best_raw, favorable_move)
                worst_raw = min(worst_raw, adverse_move)
                adverse = low <= stop_price if direction > 0 else high >= stop_price
                favorable = high >= target_price if direction > 0 else low <= target_price
                if adverse:
                    exit_index, exit_price = index, stop_price
                    dual_touch = bool(favorable)
                    break
                if favorable:
                    exit_index, exit_price = index, target_price
                    break
            if exit_index is None and last_valid is not None:
                exit_index = last_valid
                exit_price = float(panel["close"][root].iat[last_valid])
            if exit_index is None or not math.isfinite(exit_price):
                diagnostics["missing_lookback"] += 1
                continue
        gross = (exit_price - entry_price) * direction * point
        normal_cost = 8.0 + 2.0 * POINT_VALUES[root] * TICK_SIZES[root]
        stressed_cost = normal_cost * 1.5
        normal_net = gross - normal_cost
        stressed_net = gross - stressed_cost
        trades.append(
            RawTrade(
                trade_id=f"{policy.policy_id}:{event.event_id}:{ordinal}:offset={event_offset_days}",
                root=root,
                direction=direction,
                decision_ns=release_ns,
                entry_ns=entry_ns,
                exit_ns=int(timestamp_ns[exit_index]),
                session_day=session_day,
                entry_price=float(entry_price),
                exit_price=float(exit_price),
                stop_distance=float(stop_distance),
                gross_one_contract=float(gross),
                normal_net_one_contract=float(normal_net),
                stressed_net_one_contract=float(stressed_net),
                normal_worst_one_contract=float(min(worst_raw * point - normal_cost, normal_net)),
                stressed_worst_one_contract=float(min(worst_raw * point - stressed_cost, stressed_net)),
                normal_best_one_contract=float(max(best_raw * point - normal_cost, normal_net)),
                stressed_best_one_contract=float(max(best_raw * point - stressed_cost, stressed_net)),
                same_bar_ambiguous=bool(dual_touch),
            )
        )
        last_exit_ns = int(timestamp_ns[exit_index])
    return tuple(trades), ReplayDiagnostics(**diagnostics)


def summarize_trades(policy: BracketPolicy, trades: Sequence[RawTrade], diagnostics: ReplayDiagnostics) -> dict[str, Any]:
    gross = float(sum(row.gross_one_contract for row in trades))
    normal = float(sum(row.normal_net_one_contract for row in trades))
    stressed = float(sum(row.stressed_net_one_contract for row in trades))
    return {
        "policy_id": policy.policy_id,
        "policy": asdict(policy),
        "trade_count": len(trades),
        "gross_one_contract": gross,
        "normal_net_one_contract": normal,
        "stressed_net_one_contract": stressed,
        "median_stressed_trade": float(np.median([row.stressed_net_one_contract for row in trades])) if trades else 0.0,
        "positive_stressed_trade_rate": float(np.mean([row.stressed_net_one_contract > 0.0 for row in trades])) if trades else 0.0,
        "diagnostics": asdict(diagnostics),
        # Candidate identity must not make two execution-equivalent paths look
        # different.  trade_id embeds policy_id and is deliberately excluded.
        "trade_hash": stable_hash(
            [
                {
                    key: value
                    for key, value in asdict(row).items()
                    if key != "trade_id"
                }
                for row in trades
            ]
        ),
        "score": stressed / max(math.sqrt(len(trades)), 1.0) + 0.05 * normal,
    }


def _select_validation(rows: Sequence[Mapping[str, Any]], limit: int = 24) -> list[dict[str, Any]]:
    selected = []
    seen_hashes: set[str] = set()
    niche_counts: dict[tuple[str, str], int] = {}
    for raw in sorted(rows, key=lambda row: (row["score"], row["policy_id"]), reverse=True):
        row = dict(raw)
        policy = row["policy"]
        niche = (str(policy["root"]), str(policy["release_scope"]))
        if row["trade_count"] < 4 or row["trade_hash"] in seen_hashes or niche_counts.get(niche, 0) >= 3:
            continue
        selected.append(row)
        seen_hashes.add(str(row["trade_hash"]))
        niche_counts[niche] = niche_counts.get(niche, 0) + 1
        if len(selected) >= limit:
            break
    return selected


def _policy_from_dict(value: Mapping[str, Any]) -> BracketPolicy:
    return BracketPolicy(**dict(value))


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists():
        raise ReleaseBracketError(f"immutable artifact already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def run(
    root: str | Path,
    *,
    output_dir: str | Path = "reports/research_tripwires/direction_neutral_release_bracket_preflight_v1",
) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    manifest, events, calendar, rules = load_contract(project)
    _, acquisition, bars = load_inputs(project)
    panel = build_panel(bars)
    output = project / output_dir

    discovery_rows = []
    discovery_trades: dict[str, tuple[RawTrade, ...]] = {}
    policies = frozen_policies()
    for policy in policies:
        trades, diagnostics = materialize_brackets(policy, events, panel, ROLE_DATES["DISCOVERY"])
        row = summarize_trades(policy, trades, diagnostics)
        discovery_rows.append(row)
        discovery_trades[policy.policy_id] = trades
    validation_selection = _select_validation(discovery_rows)
    _write_once(
        output / "validation_selection_freeze.json",
        {
            "schema": SCHEMA + "_validation_selection_freeze",
            "created_at_utc": datetime.now(UTC).isoformat(),
            "manifest_hash": manifest["manifest_hash"],
            "calendar_sha256": manifest["data"]["release_calendar_sha256"],
            "policy_ids": [row["policy_id"] for row in validation_selection],
            "policies": [row["policy"] for row in validation_selection],
            "final_development_opened": False,
        },
    )

    validation_rows = []
    validation_trades: dict[str, tuple[RawTrade, ...]] = {}
    for selected in validation_selection:
        policy = _policy_from_dict(selected["policy"])
        trades, diagnostics = materialize_brackets(policy, events, panel, ROLE_DATES["VALIDATION"])
        row = summarize_trades(policy, trades, diagnostics)
        row["discovery"] = selected
        validation_rows.append(row)
        validation_trades[policy.policy_id] = trades
    eligible = [
        row
        for row in validation_rows
        if row["trade_count"] >= 2
        and row["stressed_net_one_contract"] > 0.0
        and row["discovery"]["stressed_net_one_contract"] > 0.0
    ]
    finalists = _select_validation(eligible, limit=8)
    _write_once(
        output / "finalist_freeze.json",
        {
            "schema": SCHEMA + "_finalist_freeze",
            "created_at_utc": datetime.now(UTC).isoformat(),
            "manifest_hash": manifest["manifest_hash"],
            "policy_ids": [row["policy_id"] for row in finalists],
            "policies": [row["policy"] for row in finalists],
            "final_development_role": ROLE_DATES["FINAL_DEVELOPMENT"],
            "final_development_opened_after_this_freeze": bool(finalists),
        },
    )

    final_rows = []
    configs = _rule_configs(rules)
    for selected in finalists:
        policy = _policy_from_dict(selected["policy"])
        trades, diagnostics = materialize_brackets(policy, events, panel, ROLE_DATES["FINAL_DEVELOPMENT"])
        row = summarize_trades(policy, trades, diagnostics)
        control, control_diagnostics = materialize_brackets(
            policy, events, panel, ROLE_DATES["FINAL_DEVELOPMENT"], event_offset_days=-1
        )
        row["prior_day_control"] = summarize_trades(policy, control, control_diagnostics)
        cells = {}
        days = _eligible_days(panel, ROLE_DATES["FINAL_DEVELOPMENT"])
        for label, (config, maximum) in configs.items():
            for risk in (0.10, 0.20, 0.30):
                cells[f"{label}:{risk:.2f}"] = account_frontier(
                    trades, days, config=config, maximum_contracts=maximum, risk_fraction=risk
                )
        best_cell = max(cells, key=lambda key: _account_score(cells[key]))
        row["account_cells"] = cells
        row["selected_account_cell"] = best_cell
        row["paired_stressed_uplift_vs_prior_day"] = (
            row["stressed_net_one_contract"] - row["prior_day_control"]["stressed_net_one_contract"]
        )
        final_rows.append(row)

    signal_rows = []
    for row in final_rows:
        stress = row["account_cells"][row["selected_account_cell"]]["STRESSED_1_5X"]["horizons"]
        if (
            row["stressed_net_one_contract"] > 0.0
            and row["paired_stressed_uplift_vs_prior_day"] > 0.0
            and sum(item["mll_breaches"] for item in stress.values()) == 0
            and max(item["median_target_progress"] for item in stress.values()) > 0.0
        ):
            signal_rows.append(row)
    gross_positive_final = sum(row["gross_one_contract"] > 0.0 for row in final_rows)
    if signal_rows:
        status = "RELEASE_BRACKET_PREFLIGHT_SIGNAL"
        next_action = "ESTIMATE_TARGETED_TBBO_RELEASE_WINDOWS_WITHOUT_PURCHASE"
    elif final_rows and gross_positive_final:
        status = "RELEASE_BRACKET_PREFLIGHT_WEAK"
        next_action = "TOMBSTONE_DIRECTION_NEUTRAL_RELEASE_BRACKET_REPRESENTATION"
    else:
        status = "RELEASE_BRACKET_PREFLIGHT_FALSIFIED"
        next_action = "TOMBSTONE_DIRECTION_NEUTRAL_RELEASE_BRACKET_REPRESENTATION"
    result = {
        "schema": SCHEMA + "_economic_result",
        "status": status,
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "acquisition_receipt_hash": acquisition["receipt_hash"],
        "calendar": {
            "artifact_id": calendar["artifact_id"],
            "sha256": manifest["data"]["release_calendar_sha256"],
            "event_count": len(events),
        },
        "counts": {
            "proposals": len(policies),
            "validation_policies": len(validation_rows),
            "final_development_policies": len(final_rows),
            "signal_policies": len(signal_rows),
            "tier_q": 0,
            "tier_g": 0,
            "tier_c": 0,
            "data_purchases": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "discovery": sorted(discovery_rows, key=lambda row: row["score"], reverse=True)[:48],
        "validation": validation_rows,
        "final_development": final_rows,
        "signal_policy_ids": [row["policy_id"] for row in signal_rows],
        "confirmation_opened": False,
        "runtime_seconds": time.perf_counter() - started,
        "next_action": next_action,
    }
    result["result_hash"] = stable_hash(result)
    _write_once(output / "economic_result.json", result)
    return result


__all__ = [
    "BracketPolicy",
    "ReleaseBracketError",
    "ReleaseEvent",
    "frozen_policies",
    "load_contract",
    "materialize_brackets",
    "run",
    "summarize_trades",
]
