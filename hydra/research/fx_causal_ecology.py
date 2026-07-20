"""Bounded causal FX-futures ecology pilot.

The branch is intentionally separate from the exhausted equity-index grammar.
Selection is chronological and every account result uses a next-minute-open
entry, a declared ATR stop-risk charge, and the canonical Combine episode
kernel.  Confirmation is read exactly once after a policy freeze is written.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_episode import TradePathEvent, run_combine_episode
from hydra.propfirm.topstep_150k import Topstep150KConfig


ROOTS = ("6E", "6B", "6J", "6A")
POINT_VALUES = {"6E": 125_000.0, "6B": 62_500.0, "6J": 12_500_000.0, "6A": 100_000.0}
TICK_SIZES = {"6E": 0.00005, "6B": 0.0001, "6J": 0.0000005, "6A": 0.0001}
TICK_VALUES = {root: POINT_VALUES[root] * TICK_SIZES[root] for root in ROOTS}
ROUND_TURN_COMMISSION_USD = 8.0
NORMAL_SLIPPAGE_TICKS_PER_SIDE = 1.0
STRESS_MULTIPLIER = 1.5
MECHANISMS = (
    "USD_FACTOR_RESIDUAL_REVERSION",
    "USD_FACTOR_RESIDUAL_CONTINUATION",
    "SESSION_TRANSITION_RESIDUAL",
    "CROSS_SECTIONAL_DISPERSION_BREAKOUT",
)
SESSIONS = ("ASIA_00_06", "LONDON_06_12", "US_12_20")
LOOKBACKS = (15, 60, 240)
Z_THRESHOLDS = (1.0, 1.5, 2.0)
HOLDINGS = (30, 120, 360)
GEOMETRIES = ((0.75, 1.5), (1.0, 1.5), (1.0, 2.0), (1.5, 2.0))
RISK_FRACTIONS = (0.10, 0.20, 0.30)
ACCOUNT_LABELS = ("50K", "100K", "150K")
ROLE_DATES = {
    "DISCOVERY": ("2018-01-02", "2022-01-01"),
    "VALIDATION": ("2022-01-01", "2023-01-01"),
    "FINAL_DEVELOPMENT": ("2023-01-01", "2024-01-01"),
    "CONFIRMATION": ("2024-01-01", "2024-10-01"),
}
ROOT_RE = re.compile(r"^(6E|6B|6J|6A)")


class FXEcologyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BasePolicy:
    mechanism: str
    session: str
    lookback_minutes: int
    z_threshold: float

    @property
    def policy_id(self) -> str:
        return "fx_base_" + stable_hash(asdict(self))[:20]


@dataclass(frozen=True, slots=True)
class ExactPolicy:
    base: BasePolicy
    holding_minutes: int
    stop_atr: float
    target_r: float

    @property
    def policy_id(self) -> str:
        return "fx_exact_" + stable_hash(asdict(self))[:20]


@dataclass(frozen=True, slots=True)
class Opportunity:
    decision_index: int
    root: str
    direction: int
    feature_z: float


@dataclass(frozen=True, slots=True)
class RawTrade:
    trade_id: str
    root: str
    direction: int
    decision_ns: int
    entry_ns: int
    exit_ns: int
    session_day: int
    entry_price: float
    exit_price: float
    stop_distance: float
    gross_one_contract: float
    normal_net_one_contract: float
    stressed_net_one_contract: float
    normal_worst_one_contract: float
    stressed_worst_one_contract: float
    normal_best_one_contract: float
    stressed_best_one_contract: float
    same_bar_ambiguous: bool


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def load_inputs(root: Path) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame]:
    manifest_path = root / "config/research/fx_causal_ecology_pilot_v1.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = core.pop("manifest_hash", "")
    if claimed != _canonical_hash(core):
        raise FXEcologyError("manifest hash drift")
    receipt_path = root / "reports/data_access/fx_causal_ecology_acquisition_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt_core = dict(receipt)
    receipt_hash = receipt_core.pop("receipt_hash", "")
    if receipt_hash != stable_hash(receipt_core):
        raise FXEcologyError("acquisition receipt hash drift")
    raw = next(row for row in receipt["files"] if row["kind"] == "ohlcv-1m")
    definition = next(row for row in receipt["files"] if row["kind"] == "definition")
    raw_path = Path(raw["path"])
    definition_path = Path(definition["path"])
    if sha256_file(raw_path) != raw["sha256"] or sha256_file(definition_path) != definition["sha256"]:
        raise FXEcologyError("immutable FX raw hash drift")
    import databento as db

    frame = db.DBNStore.from_file(raw_path).to_df(
        pretty_ts=True, map_symbols=True, price_type="float"
    ).reset_index()
    definitions = db.DBNStore.from_file(definition_path).to_df(
        pretty_ts=True, map_symbols=True, price_type="float"
    ).reset_index()
    _validate_definitions(frame, definitions)
    return manifest, receipt, normalize_bars(frame)


def _validate_definitions(bars: pd.DataFrame, definitions: pd.DataFrame) -> None:
    required = {"instrument_id", "asset", "unit_of_measure_qty", "min_price_increment"}
    if not required.issubset(definitions.columns):
        raise FXEcologyError("definition metadata is incomplete")
    defined_ids = set(pd.to_numeric(definitions["instrument_id"], errors="raise").astype(int))
    bar_ids = set(pd.to_numeric(bars["instrument_id"], errors="raise").astype(int))
    if not bar_ids.issubset(defined_ids):
        raise FXEcologyError("OHLCV contract lacks a purchased definition")
    for root in ROOTS:
        rows = definitions[definitions["asset"].astype(str) == root]
        if rows.empty:
            raise FXEcologyError(f"definition coverage missing for {root}")
        point_values = pd.to_numeric(rows["unit_of_measure_qty"], errors="coerce").dropna().to_numpy(float)
        tick_sizes = pd.to_numeric(rows["min_price_increment"], errors="coerce").dropna().to_numpy(float)
        if not len(point_values) or not np.allclose(point_values, POINT_VALUES[root], rtol=0.0, atol=1e-9):
            raise FXEcologyError(f"point-value definition drift for {root}")
        # 6A moved from a 0.0001 to a 0.00005 minimum increment during the
        # sample.  The pilot deliberately charges the larger frozen increment
        # on every 6A fill, which is conservative rather than a contract-map
        # mismatch.  No observed contract may have a larger tick than charged.
        if (
            not len(tick_sizes)
            or np.any(tick_sizes <= 0.0)
            or np.any(tick_sizes > TICK_SIZES[root] + 1e-12)
            or not math.isclose(float(np.max(tick_sizes)), TICK_SIZES[root], rel_tol=0.0, abs_tol=1e-12)
        ):
            raise FXEcologyError(f"tick-size definition drift for {root}")


def normalize_bars(frame: pd.DataFrame) -> pd.DataFrame:
    ts_col = next((name for name in ("ts_event", "timestamp", "index") if name in frame), None)
    if ts_col is None or "symbol" not in frame or "instrument_id" not in frame:
        raise FXEcologyError(f"DBN frame columns unsupported: {sorted(frame.columns)}")
    required = {"open", "high", "low", "close", "volume"}
    if not required.issubset(frame.columns):
        raise FXEcologyError("OHLCV columns missing")
    output = frame[[ts_col, "symbol", "instrument_id", *sorted(required)]].copy()
    output.rename(columns={ts_col: "timestamp"}, inplace=True)
    output["timestamp"] = pd.to_datetime(output["timestamp"], utc=True)
    output["raw_symbol"] = output["symbol"].astype(str)
    output["contract_id"] = pd.to_numeric(output["instrument_id"], errors="raise").astype("int64")
    output["root"] = output["raw_symbol"].str.extract(ROOT_RE, expand=False)
    output = output[output["root"].isin(ROOTS)].copy()
    output = output[(output["timestamp"] >= pd.Timestamp("2018-01-02", tz="UTC")) & (output["timestamp"] < pd.Timestamp("2024-10-01", tz="UTC"))]
    output.sort_values(["timestamp", "root"], inplace=True)
    output.drop_duplicates(["timestamp", "root"], keep="last", inplace=True)
    if set(output["root"]) != set(ROOTS):
        raise FXEcologyError("not all frozen currency roots decoded")
    for name in required:
        output[name] = pd.to_numeric(output[name], errors="coerce")
    return output.reset_index(drop=True)


def build_panel(bars: pd.DataFrame) -> dict[str, Any]:
    fields = {}
    for name in ("open", "high", "low", "close", "volume", "raw_symbol", "contract_id"):
        fields[name] = bars.pivot(index="timestamp", columns="root", values=name).reindex(columns=ROOTS)
    timestamps = fields["close"].index
    chicago = timestamps.tz_convert("America/Chicago")
    session_dates = (chicago.normalize() + pd.to_timedelta((chicago.hour >= 17).astype(int), unit="D"))
    session_day = session_dates.strftime("%Y%m%d").astype(int).to_numpy()
    utc_hour = timestamps.hour.to_numpy()
    session = np.full(len(timestamps), "OFF", dtype="U12")
    session[(utc_hour >= 0) & (utc_hour < 6)] = "ASIA_00_06"
    session[(utc_hour >= 6) & (utc_hour < 12)] = "LONDON_06_12"
    session[(utc_hour >= 12) & (utc_hour < 20)] = "US_12_20"
    local_minute = chicago.hour.to_numpy() * 60 + chicago.minute.to_numpy()
    # Topstep's mandatory close interval runs from 15:10 CT until the next
    # futures session opens at 17:00 CT.  Hours after 17:00 belong to the new
    # overnight session and must not inherit the prior close veto.
    flatten = (local_minute >= 15 * 60 + 10) & (local_minute < 17 * 60)
    close = fields["close"].astype(float)
    returns = {}
    for lookback in LOOKBACKS:
        prior = close.shift(lookback)
        exact_clock = (
            timestamps.as_unit("ns").view("i8")
            - np.roll(timestamps.as_unit("ns").view("i8"), lookback)
        ) == lookback * 60_000_000_000
        exact_clock[:lookback] = False
        resolved_return = np.log(close / prior)
        resolved_return.loc[~exact_clock, :] = np.nan
        returns[lookback] = resolved_return
    zscores: dict[int, pd.DataFrame] = {}
    dispersions: dict[int, np.ndarray] = {}
    for lookback, ret in returns.items():
        factor = ret.median(axis=1, skipna=True)
        residual = ret.sub(factor, axis=0)
        mean = residual.rolling(5 * 24 * 60, min_periods=500).mean()
        std = residual.rolling(5 * 24 * 60, min_periods=500).std().replace(0.0, np.nan)
        zscores[lookback] = (residual - mean) / std
        dispersions[lookback] = zscores[lookback].max(axis=1).to_numpy() - zscores[lookback].min(axis=1).to_numpy()
    tr_frames = []
    for root in ROOTS:
        previous = close[root].shift(1)
        tr = pd.concat(
            [
                fields["high"][root] - fields["low"][root],
                (fields["high"][root] - previous).abs(),
                (fields["low"][root] - previous).abs(),
            ],
            axis=1,
        ).max(axis=1)
        tr_frames.append(tr.rolling("60min", min_periods=30).mean().rename(root))
    atr = pd.concat(tr_frames, axis=1).reindex(columns=ROOTS)
    return {
        **fields,
        "timestamps": timestamps,
        "timestamp_ns": timestamps.as_unit("ns").view("i8"),
        "session_day": session_day,
        "session": session,
        "flatten": np.asarray(flatten, dtype=bool),
        "zscores": zscores,
        "dispersion": dispersions,
        "atr": atr,
        # Contract state is metadata, not a filled market price.  Forward
        # propagation is causal and lets a missing-print minute retain the
        # currently mapped contract while every price field remains missing.
        "contract_state": fields["contract_id"].ffill(),
        "panel_hash": stable_hash(
            {
                "row_count": len(timestamps),
                "first": timestamps[0].isoformat(),
                "last": timestamps[-1].isoformat(),
                "bars_hash": stable_hash(
                    bars.groupby("root").size().astype(int).to_dict()
                ),
            }
        ),
    }


def frozen_base_policies() -> tuple[BasePolicy, ...]:
    return tuple(
        BasePolicy(mechanism, session, lookback, threshold)
        for mechanism in MECHANISMS
        for session in SESSIONS
        for lookback in LOOKBACKS
        for threshold in Z_THRESHOLDS
    )


def opportunities(policy: BasePolicy, panel: Mapping[str, Any], *, start: str, end: str) -> tuple[Opportunity, ...]:
    timestamps = panel["timestamps"]
    in_role = (timestamps >= pd.Timestamp(start, tz="UTC")) & (timestamps < pd.Timestamp(end, tz="UTC"))
    in_session = np.asarray(panel["session"]) == policy.session
    z = panel["zscores"][policy.lookback_minutes].to_numpy(dtype=float)
    finite = np.sum(np.isfinite(z), axis=1) >= 3
    magnitude = np.max(np.where(np.isfinite(z), np.abs(z), -np.inf), axis=1)
    eligible = in_role & in_session & finite & (magnitude >= policy.z_threshold)
    if policy.mechanism == "SESSION_TRANSITION_RESIDUAL":
        hours = timestamps.hour.to_numpy()
        minutes = timestamps.minute.to_numpy()
        starts = {"ASIA_00_06": 0, "LONDON_06_12": 6, "US_12_20": 12}
        eligible &= (hours == starts[policy.session]) & (minutes < 30)
    elif policy.mechanism == "CROSS_SECTIONAL_DISPERSION_BREAKOUT":
        dispersion = np.asarray(panel["dispersion"][policy.lookback_minutes], dtype=float)
        eligible &= dispersion > np.roll(dispersion, 5)
    indices = np.flatnonzero(eligible)
    output: list[Opportunity] = []
    last_index = -10**9
    for raw_index in indices:
        index = int(raw_index)
        if index < last_index + 30:
            continue
        row = z[index]
        root_offset = int(np.nanargmax(np.abs(row)))
        contract_state = panel["contract_state"][ROOTS[root_offset]]
        prior_index = index - policy.lookback_minutes
        if (
            prior_index < 0
            or pd.isna(contract_state.iat[index])
            or pd.isna(contract_state.iat[prior_index])
            or int(contract_state.iat[index]) != int(contract_state.iat[prior_index])
        ):
            # Never let a continuous-contract mapping transition become a
            # cross-sectional return feature.
            continue
        residual_sign = 1 if row[root_offset] > 0 else -1
        reversion = policy.mechanism in {"USD_FACTOR_RESIDUAL_REVERSION", "SESSION_TRANSITION_RESIDUAL"}
        direction = -residual_sign if reversion else residual_sign
        output.append(Opportunity(index, ROOTS[root_offset], direction, float(row[root_offset])))
        last_index = index
    return tuple(output)


def cheap_score(policy: BasePolicy, panel: Mapping[str, Any], role: tuple[str, str]) -> dict[str, Any]:
    events = opportunities(policy, panel, start=role[0], end=role[1])
    closes = panel["close"]
    opens = panel["open"]
    values: list[float] = []
    for event in events:
        entry = event.decision_index + 1
        exit_index = min(entry + 120, len(closes) - 1)
        if panel["session_day"][entry] != panel["session_day"][exit_index]:
            continue
        entry_price = float(opens[event.root].iat[entry])
        exit_price = float(closes[event.root].iat[exit_index])
        if not math.isfinite(entry_price + exit_price):
            continue
        gross = (exit_price - entry_price) * event.direction * POINT_VALUES[event.root]
        values.append(gross - _cost_per_contract(event.root, stressed=True))
    net = float(sum(values))
    return {
        "policy": asdict(policy),
        "policy_id": policy.policy_id,
        "opportunity_count": len(events),
        "completed_count": len(values),
        "stressed_net_one_contract": net,
        "median_stressed_trade": float(np.median(values)) if values else 0.0,
        "positive_trade_rate": float(np.mean(np.asarray(values) > 0.0)) if values else 0.0,
        "score": net / max(math.sqrt(len(values)), 1.0),
    }


def materialize_trades(
    policy: ExactPolicy,
    panel: Mapping[str, Any],
    role: tuple[str, str],
    *,
    direction_flip: bool = False,
    timing_offset: int = 0,
    deterministic_random_direction: bool = False,
) -> tuple[RawTrade, ...]:
    events = opportunities(policy.base, panel, start=role[0], end=role[1])
    timestamps = panel["timestamps"]
    output: list[RawTrade] = []
    last_exit = -1
    for ordinal, event in enumerate(events):
        decision = event.decision_index + timing_offset
        if decision <= last_exit or decision + 1 >= len(timestamps):
            continue
        entry = decision + 1
        root = event.root
        direction = -event.direction if direction_flip else event.direction
        if deterministic_random_direction:
            direction = 1 if int(stable_hash([policy.policy_id, decision, root])[:2], 16) % 2 else -1
        # The earliest executable bar is the next observed bar for this
        # contract, not merely the next union-panel row.  Wait no more than ten
        # minutes and never cross the frozen session day.
        deadline_ns = int(timestamps[decision].value) + 10 * 60_000_000_000
        while entry < len(timestamps) and (
            int(timestamps[entry].value) > deadline_ns
            or pd.isna(panel["open"][root].iat[entry])
            or pd.isna(panel["contract_id"][root].iat[entry])
        ):
            if int(timestamps[entry].value) > deadline_ns:
                break
            entry += 1
        if (
            entry >= len(timestamps)
            or int(timestamps[entry].value) > deadline_ns
            or panel["session_day"][decision] != panel["session_day"][entry]
        ):
            continue
        raw_symbol = panel["raw_symbol"][root]
        contract_id = panel["contract_id"][root]
        decision_contract = contract_id.iat[decision]
        entry_contract = contract_id.iat[entry]
        if (
            pd.isna(decision_contract)
            or pd.isna(entry_contract)
            or int(decision_contract) != int(entry_contract)
        ):
            continue
        frozen_contract_id = int(entry_contract)
        entry_price = float(panel["open"][root].iat[entry])
        atr = float(panel["atr"][root].iat[decision])
        if not math.isfinite(entry_price + atr) or atr <= 0.0:
            continue
        stop_distance = atr * policy.stop_atr
        stop_price = entry_price - direction * stop_distance
        target_price = entry_price + direction * stop_distance * policy.target_r
        horizon_ns = int(timestamps[entry].value) + policy.holding_minutes * 60_000_000_000
        maximum = min(int(np.searchsorted(panel["timestamp_ns"], horizon_ns, side="right") - 1), len(timestamps) - 1)
        terminal = None
        same_bar = False
        best_raw = 0.0
        worst_raw = 0.0
        last_observed_index: int | None = None
        for index in range(entry, maximum + 1):
            current_contract = contract_id.iat[index]
            if (
                panel["session_day"][index] != panel["session_day"][entry]
                or bool(panel["flatten"][index])
            ):
                if last_observed_index is not None:
                    terminal = (last_observed_index, float(panel["close"][root].iat[last_observed_index]))
                break
            if pd.isna(current_contract):
                continue
            if int(current_contract) != frozen_contract_id:
                if last_observed_index is not None:
                    terminal = (last_observed_index, float(panel["close"][root].iat[last_observed_index]))
                break
            high = float(panel["high"][root].iat[index])
            low = float(panel["low"][root].iat[index])
            if not math.isfinite(high + low):
                continue
            last_observed_index = index
            favorable = high >= target_price if direction > 0 else low <= target_price
            adverse = low <= stop_price if direction > 0 else high >= stop_price
            favorable_move = (high - entry_price) * direction if direction > 0 else (entry_price - low)
            adverse_move = (low - entry_price) * direction if direction > 0 else (entry_price - high)
            best_raw = max(best_raw, favorable_move)
            worst_raw = min(worst_raw, adverse_move)
            if adverse:
                terminal = (index, stop_price)
                same_bar = bool(favorable)
                break
            if favorable:
                terminal = (index, target_price)
                break
        if terminal is None and last_observed_index is not None:
            terminal = (last_observed_index, float(panel["close"][root].iat[last_observed_index]))
        if terminal is None:
            continue
        exit_index, exit_price = terminal
        if not math.isfinite(exit_price):
            continue
        point = POINT_VALUES[root]
        gross = (exit_price - entry_price) * direction * point
        normal_cost = _cost_per_contract(root, stressed=False)
        stressed_cost = _cost_per_contract(root, stressed=True)
        normal_worst = worst_raw * point - normal_cost
        stressed_worst = worst_raw * point - stressed_cost
        normal_best = best_raw * point - normal_cost
        stressed_best = best_raw * point - stressed_cost
        output.append(
            RawTrade(
                trade_id=f"{policy.policy_id}:{ordinal}",
                root=root,
                direction=direction,
                decision_ns=int(timestamps[decision].value),
                entry_ns=int(timestamps[entry].value),
                exit_ns=int(timestamps[exit_index].value),
                session_day=int(panel["session_day"][entry]),
                entry_price=entry_price,
                exit_price=exit_price,
                stop_distance=stop_distance,
                gross_one_contract=gross,
                normal_net_one_contract=gross - normal_cost,
                stressed_net_one_contract=gross - stressed_cost,
                normal_worst_one_contract=min(normal_worst, gross - normal_cost),
                stressed_worst_one_contract=min(stressed_worst, gross - stressed_cost),
                normal_best_one_contract=max(normal_best, gross - normal_cost),
                stressed_best_one_contract=max(stressed_best, gross - stressed_cost),
                same_bar_ambiguous=same_bar,
            )
        )
        last_exit = exit_index
    return tuple(output)


def _cost_per_contract(root: str, *, stressed: bool) -> float:
    normal = ROUND_TURN_COMMISSION_USD + 2.0 * NORMAL_SLIPPAGE_TICKS_PER_SIDE * TICK_VALUES[root]
    return normal * (STRESS_MULTIPLIER if stressed else 1.0)


def _rule_configs(rule_snapshot: Mapping[str, Any]) -> dict[str, tuple[Topstep150KConfig, float]]:
    result = {}
    for label in ACCOUNT_LABELS:
        rule = rule_snapshot["combine"][label]
        result[label] = (
            Topstep150KConfig(
                account_size=float(rule["account_size_usd"]),
                combine_profit_target=float(rule["profit_target_usd"]),
                combine_max_loss_limit=float(rule["maximum_loss_limit_usd"]),
                combine_starting_balance=float(rule["account_size_usd"]),
                mll_mode="eod_level_rt_breach",
                optional_daily_loss_limit=float(rule["optional_daily_loss_limit_usd"]),
                use_optional_daily_loss_limit=False,
                consistency_best_day_max_pct_of_profit_target=float(rule["consistency_target_fraction"]),
                minimum_pass_days=int(rule["minimum_trading_days"]),
            ),
            float(rule["maximum_mini_contracts"]),
        )
    return result


def account_frontier(trades: Sequence[RawTrade], eligible_days: Sequence[int], *, config: Topstep150KConfig, maximum_contracts: float, risk_fraction: float) -> dict[str, Any]:
    scenarios: dict[str, dict[str, Any]] = {}
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        stressed = scenario != "NORMAL"
        events: list[TradePathEvent] = []
        for trade in trades:
            risk_per_contract = trade.stop_distance * POINT_VALUES[trade.root] + _cost_per_contract(trade.root, stressed=stressed)
            quantity = min(int(maximum_contracts), int(config.combine_max_loss_limit * risk_fraction / max(risk_per_contract, 1e-12)))
            if quantity < 1:
                continue
            events.append(
                TradePathEvent(
                    event_id=f"{trade.trade_id}:{scenario}",
                    decision_ns=trade.entry_ns,
                    exit_ns=trade.exit_ns,
                    session_day=trade.session_day,
                    net_pnl=(trade.stressed_net_one_contract if stressed else trade.normal_net_one_contract) * quantity,
                    gross_pnl=trade.gross_one_contract * quantity,
                    worst_unrealized_pnl=(trade.stressed_worst_one_contract if stressed else trade.normal_worst_one_contract) * quantity,
                    best_unrealized_pnl=(trade.stressed_best_one_contract if stressed else trade.normal_best_one_contract) * quantity,
                    quantity=quantity,
                    mini_equivalent=float(quantity),
                    regime="FX_CAUSAL_ECOLOGY",
                    same_bar_ambiguous=trade.same_bar_ambiguous,
                )
            )
        horizons = {}
        for horizon in (5, 10, 20):
            starts = tuple(eligible_days[::horizon])
            starts = starts[:-1] if starts and eligible_days.index(starts[-1]) + horizon > len(eligible_days) else starts
            episodes = [
                run_combine_episode(
                    events,
                    eligible_days,
                    start_day=int(start),
                    maximum_duration_days=horizon,
                    config=config,
                    maximum_mini_equivalent=maximum_contracts,
                )
                for start in starts
                if eligible_days.index(start) + horizon <= len(eligible_days)
            ]
            horizons[str(horizon)] = _summarize_episodes(episodes)
        scenarios[scenario] = {"event_count": len(events), "horizons": horizons}
    return scenarios


def _summarize_episodes(episodes: Sequence[Any]) -> dict[str, Any]:
    return {
        "full_coverage_starts": len(episodes),
        "passes": sum(row.passed for row in episodes),
        "pass_rate": float(np.mean([row.passed for row in episodes])) if episodes else 0.0,
        "mll_breaches": sum(row.mll_breached for row in episodes),
        "mll_breach_rate": float(np.mean([row.mll_breached for row in episodes])) if episodes else 0.0,
        "consistency_rate": float(np.mean([row.consistency_ok for row in episodes])) if episodes else 0.0,
        "net_pnl": float(sum(row.net_pnl for row in episodes)),
        "median_target_progress": float(np.median([row.target_progress for row in episodes])) if episodes else 0.0,
        "lower_quartile_target_progress": float(np.quantile([row.target_progress for row in episodes], 0.25)) if episodes else 0.0,
        "minimum_mll_buffer": float(min((row.minimum_mll_buffer for row in episodes), default=0.0)),
        "median_days_to_pass": float(np.median([row.days_to_target for row in episodes if row.days_to_target is not None])) if any(row.days_to_target is not None for row in episodes) else None,
        "terminals": {terminal: sum(row.terminal.value == terminal for row in episodes) for terminal in ("PASSED", "MLL_BREACH", "TIMEOUT", "COMPLIANCE_FAILURE")},
    }


def _eligible_days(panel: Mapping[str, Any], role: tuple[str, str]) -> tuple[int, ...]:
    timestamps = panel["timestamps"]
    mask = (timestamps >= pd.Timestamp(role[0], tz="UTC")) & (timestamps < pd.Timestamp(role[1], tz="UTC"))
    return tuple(sorted({int(day) for day in np.asarray(panel["session_day"])[mask]}))


def run(root: str | Path, *, output_dir: str | Path = "reports/research_tripwires/fx_causal_ecology_pilot_v1") -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    manifest, acquisition, bars = load_inputs(project)
    panel = build_panel(bars)
    rules = json.loads((project / manifest["official_rule_snapshot"]["path"]).read_text(encoding="utf-8"))
    configs = _rule_configs(rules)
    output = project / output_dir
    output.mkdir(parents=True, exist_ok=True)

    cheap = [cheap_score(policy, panel, ROLE_DATES["DISCOVERY"]) for policy in frozen_base_policies()]
    cheap.sort(key=lambda row: (row["score"], row["stressed_net_one_contract"]), reverse=True)
    selected_bases: list[BasePolicy] = []
    cell_counts: dict[tuple[str, str], int] = {}
    for row in cheap:
        policy = BasePolicy(**row["policy"])
        cell = (policy.mechanism, policy.session)
        if row["completed_count"] < 40 or cell_counts.get(cell, 0) >= 3:
            continue
        selected_bases.append(policy)
        cell_counts[cell] = cell_counts.get(cell, 0) + 1
        if len(selected_bases) >= 24:
            break

    discovery_exact = []
    for base in selected_bases:
        for holding in HOLDINGS:
            for stop_atr, target_r in GEOMETRIES:
                policy = ExactPolicy(base, holding, stop_atr, target_r)
                trades = materialize_trades(policy, panel, ROLE_DATES["DISCOVERY"])
                stress_net = sum(row.stressed_net_one_contract for row in trades)
                discovery_exact.append(
                    {
                        "policy": {"base": asdict(base), "holding_minutes": holding, "stop_atr": stop_atr, "target_r": target_r},
                        "policy_id": policy.policy_id,
                        "trade_count": len(trades),
                        "normal_net_one_contract": sum(row.normal_net_one_contract for row in trades),
                        "stressed_net_one_contract": stress_net,
                        "median_stressed_trade": float(np.median([row.stressed_net_one_contract for row in trades])) if trades else 0.0,
                        "score": stress_net / max(math.sqrt(len(trades)), 1.0),
                    }
                )
    discovery_exact.sort(key=lambda row: (row["score"], row["stressed_net_one_contract"]), reverse=True)
    discovery_finalists = discovery_exact[:24]

    validation_rows = []
    for row in discovery_finalists:
        policy = _exact_from_dict(row["policy"])
        trades = materialize_trades(policy, panel, ROLE_DATES["VALIDATION"])
        normal = sum(item.normal_net_one_contract for item in trades)
        stressed = sum(item.stressed_net_one_contract for item in trades)
        account_cells = {}
        days = _eligible_days(panel, ROLE_DATES["VALIDATION"])
        for label, (config, maximum) in configs.items():
            for risk in RISK_FRACTIONS:
                account_cells[f"{label}:{risk:.2f}"] = account_frontier(trades, days, config=config, maximum_contracts=maximum, risk_fraction=risk)
        best_cell = max(account_cells, key=lambda key: _account_score(account_cells[key]))
        validation_rows.append(
            {
                "policy": row["policy"],
                "policy_id": policy.policy_id,
                "trade_count": len(trades),
                "normal_net_one_contract": normal,
                "stressed_net_one_contract": stressed,
                "account_cells": account_cells,
                "selected_account_cell": best_cell,
                "score": _account_score(account_cells[best_cell]) + stressed / max(math.sqrt(len(trades)), 1.0),
            }
        )
    validation_rows.sort(key=lambda row: row["score"], reverse=True)
    frozen_finalists = validation_rows[:8]
    finalist_core = {
        "schema": "hydra_fx_causal_ecology_finalist_freeze_v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "manifest_hash": manifest["manifest_hash"],
        "panel_hash": panel["panel_hash"],
        "selection_roles": ["DISCOVERY", "VALIDATION"],
        "final_development_unread_at_freeze": True,
        "confirmation_unread_at_freeze": True,
        "finalists": [
            {"policy": row["policy"], "policy_id": row["policy_id"], "account_cell": row["selected_account_cell"]}
            for row in frozen_finalists
        ],
    }
    finalist_freeze = {**finalist_core, "freeze_hash": stable_hash(finalist_core)}
    _write_once(output / "finalist_freeze.json", finalist_freeze)

    final_rows = [_evaluate_frozen(row, panel, configs, "FINAL_DEVELOPMENT") for row in frozen_finalists]
    tier_q = [row for row in final_rows if _tier_q(row)]
    tier_g = [row for row in tier_q if _tier_g(row)]
    confirmation_freeze_core = {
        "schema": "hydra_fx_causal_ecology_confirmation_freeze_v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "finalist_freeze_hash": finalist_freeze["freeze_hash"],
        "confirmation_role": ROLE_DATES["CONFIRMATION"],
        "evaluated_once": True,
        "candidate_ids": [row["policy_id"] for row in tier_g[:4]],
        "cells": {row["policy_id"]: row["selected_account_cell"] for row in tier_g[:4]},
    }
    confirmation_freeze = {**confirmation_freeze_core, "freeze_hash": stable_hash(confirmation_freeze_core)}
    _write_once(output / "confirmation_freeze.json", confirmation_freeze)
    confirmation_rows = [_evaluate_frozen(row, panel, configs, "CONFIRMATION") for row in tier_g[:4]]
    tier_c = [row for row in confirmation_rows if _tier_c(row)]

    controls = {}
    for row in final_rows:
        policy = _exact_from_dict(row["policy"])
        controls[row["policy_id"]] = {
            "direction_flip": _control_summary(materialize_trades(policy, panel, ROLE_DATES["FINAL_DEVELOPMENT"], direction_flip=True)),
            "session_matched_delay_17m": _control_summary(materialize_trades(policy, panel, ROLE_DATES["FINAL_DEVELOPMENT"], timing_offset=17)),
            "pooled_niche_random_direction": _control_summary(
                materialize_trades(
                    policy,
                    panel,
                    ROLE_DATES["FINAL_DEVELOPMENT"],
                    deterministic_random_direction=True,
                )
            ),
        }
    status = "FX_CAUSAL_ECOLOGY_TIER_C" if tier_c else ("FX_CAUSAL_ECOLOGY_TIER_G_UNCONFIRMED" if tier_g else ("FX_CAUSAL_ECOLOGY_TIER_Q_ONLY" if tier_q else "FX_CAUSAL_ECOLOGY_FALSIFIED"))
    result_core = {
        "schema": "hydra_fx_causal_ecology_pilot_result_v1",
        "status": status,
        "manifest_hash": manifest["manifest_hash"],
        "acquisition_receipt_hash": acquisition["receipt_hash"],
        "panel_hash": panel["panel_hash"],
        "data": {
            "bar_count": len(bars),
            "panel_timestamp_count": len(panel["timestamps"]),
            "first_timestamp": panel["timestamps"][0].isoformat(),
            "last_timestamp": panel["timestamps"][-1].isoformat(),
            "roles": ROLE_DATES,
            "q4_rows": int(np.sum(panel["timestamps"] >= pd.Timestamp("2024-10-01", tz="UTC"))),
        },
        "counts": {
            "base_proposals": len(cheap),
            "selected_base_policies": len(selected_bases),
            "exact_discovery_replays": len(discovery_exact),
            "validation_policies": len(validation_rows),
            "final_development_policies": len(final_rows),
            "tier_q": len(tier_q),
            "tier_g": len(tier_g),
            "confirmation_evaluated_once": len(confirmation_rows),
            "tier_c": len(tier_c),
            "xfa_paths": 0,
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
        },
        "cost_contract": {
            "commission_round_turn_usd": ROUND_TURN_COMMISSION_USD,
            "normal_slippage_ticks_per_side": NORMAL_SLIPPAGE_TICKS_PER_SIDE,
            "stress_multiplier": STRESS_MULTIPLIER,
            "per_contract_normal_costs": {root: _cost_per_contract(root, stressed=False) for root in ROOTS},
            "per_contract_stressed_costs": {root: _cost_per_contract(root, stressed=True) for root in ROOTS},
        },
        "cheap_screen_top": cheap[:24],
        "discovery_exact_top": discovery_finalists,
        "validation": validation_rows,
        "final_development": final_rows,
        "confirmation": confirmation_rows,
        "controls": controls,
        "tier_ids": {
            "Q": [row["policy_id"] for row in tier_q],
            "G": [row["policy_id"] for row in tier_g],
            "C": [row["policy_id"] for row in tier_c],
        },
        "next_action": (
            "FREEZE_TIER_C_AND_PROVE_F0" if tier_c else
            "ONE_MATERIALLY_DISTINCT_REPAIR_IF_INFORMATION_UPLIFT" if tier_q else
            "TOMBSTONE_FX_RESIDUAL_GRAMMAR_START_RELEASE_BRACKET_PREFLIGHT"
        ),
        "runtime_seconds": time.perf_counter() - started,
        "completed_at_utc": datetime.now(UTC).isoformat(),
    }
    result = {**result_core, "result_hash": stable_hash({key: value for key, value in result_core.items() if key not in {"runtime_seconds", "completed_at_utc"}})}
    _write_once(output / "economic_result.json", result)
    return result


def _evaluate_frozen(row: Mapping[str, Any], panel: Mapping[str, Any], configs: Mapping[str, tuple[Topstep150KConfig, float]], role_name: str) -> dict[str, Any]:
    policy = _exact_from_dict(row["policy"])
    trades = materialize_trades(policy, panel, ROLE_DATES[role_name])
    label, risk = str(row["selected_account_cell"]).split(":")
    config, maximum = configs[label]
    account = account_frontier(trades, _eligible_days(panel, ROLE_DATES[role_name]), config=config, maximum_contracts=maximum, risk_fraction=float(risk))
    quarters = {}
    start = pd.Timestamp(ROLE_DATES[role_name][0], tz="UTC")
    end = pd.Timestamp(ROLE_DATES[role_name][1], tz="UTC")
    boundary = start
    ordinal = 1
    while boundary < end:
        next_boundary = min(boundary + pd.DateOffset(months=3), end)
        role = (boundary.strftime("%Y-%m-%d"), next_boundary.strftime("%Y-%m-%d"))
        subset = materialize_trades(policy, panel, role)
        days = _eligible_days(panel, role)
        quarters[f"B{ordinal}"] = account_frontier(subset, days, config=config, maximum_contracts=maximum, risk_fraction=float(risk)) if len(days) >= 20 else None
        boundary = next_boundary
        ordinal += 1
    return {
        "policy": row["policy"],
        "policy_id": policy.policy_id,
        "role": role_name,
        "selected_account_cell": row["selected_account_cell"],
        "trade_count": len(trades),
        "normal_net_one_contract": float(sum(item.normal_net_one_contract for item in trades)),
        "stressed_net_one_contract": float(sum(item.stressed_net_one_contract for item in trades)),
        "account": account,
        "blocks": quarters,
        "trade_hash": stable_hash([asdict(item) for item in trades]),
    }


def _exact_from_dict(value: Mapping[str, Any]) -> ExactPolicy:
    return ExactPolicy(BasePolicy(**dict(value["base"])), int(value["holding_minutes"]), float(value["stop_atr"]), float(value["target_r"]))


def _account_score(cell: Mapping[str, Any]) -> float:
    stress = cell["STRESSED_1_5X"]["horizons"]
    normal = cell["NORMAL"]["horizons"]
    return (
        10_000.0 * stress["5"]["pass_rate"]
        + 5_000.0 * stress["10"]["pass_rate"]
        + 2_000.0 * stress["20"]["pass_rate"]
        + 500.0 * stress["20"]["median_target_progress"]
        + 250.0 * normal["10"]["pass_rate"]
        - 10_000.0 * stress["20"]["mll_breach_rate"]
    )


def _tier_q(row: Mapping[str, Any]) -> bool:
    stress = row["account"]["STRESSED_1_5X"]["horizons"]
    return bool(
        row["stressed_net_one_contract"] > 0.0
        and stress["20"]["mll_breach_rate"] <= 0.10
        and stress["20"]["consistency_rate"] >= 0.90
        and (stress["20"]["passes"] > 0 or stress["20"]["median_target_progress"] >= 0.10)
    )


def _tier_g(row: Mapping[str, Any]) -> bool:
    account = row["account"]
    normal = account["NORMAL"]["horizons"]
    stress = account["STRESSED_1_5X"]["horizons"]
    blocks = 0
    for block in row["blocks"].values():
        if block and (block["NORMAL"]["horizons"]["20"]["passes"] > 0 or block["STRESSED_1_5X"]["horizons"]["20"]["passes"] > 0):
            blocks += 1
    return bool(
        normal["20"]["passes"] >= 2
        and stress["20"]["passes"] >= 1
        and blocks >= 2
        and stress["20"]["mll_breach_rate"] <= 0.10
    )


def _tier_c(row: Mapping[str, Any]) -> bool:
    normal = row["account"]["NORMAL"]["horizons"]
    stress = row["account"]["STRESSED_1_5X"]["horizons"]
    return bool(
        normal["20"]["passes"] >= 1
        and stress["20"]["passes"] >= 1
        and stress["20"]["mll_breach_rate"] <= 0.10
        and row["stressed_net_one_contract"] > 0.0
    )


def _control_summary(trades: Sequence[RawTrade]) -> dict[str, Any]:
    return {
        "trade_count": len(trades),
        "normal_net_one_contract": float(sum(row.normal_net_one_contract for row in trades)),
        "stressed_net_one_contract": float(sum(row.stressed_net_one_contract for row in trades)),
    }


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    serialized = json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != serialized:
            raise FXEcologyError(f"immutable artifact drift: {path}")
        return
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)


__all__ = [
    "BasePolicy",
    "ExactPolicy",
    "FXEcologyError",
    "ROLE_DATES",
    "account_frontier",
    "build_panel",
    "frozen_base_policies",
    "materialize_trades",
    "normalize_bars",
    "opportunities",
    "run",
]
