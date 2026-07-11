"""Frozen equity-index pre-close inventory/dispersion research primary.

The engine is deliberately small: exactly 32 preregistered structures are
screened on 2023, at most four are frozen, and only those frozen structures are
replayed on 2024 Q1--Q3.  It has no shadow, broker, network, paid-data or Q4
capability.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _file_sha256,
    _stable_hash,
    _strict_json_value,
)
from hydra.portfolio.account_contribution import replay_shared_account
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


PROTOCOL = "equity_preclose_inventory_dispersion_primary_v1"
PREREGISTRATION_SHA256 = "5c9fe702eee36b467a5fa0b1c27bc58373829b6f5615e374f9775ff3c42e385c"
DEVELOPMENT_END_EXCLUSIVE = pd.Timestamp("2024-10-01T00:00:00Z")
FIT_END_EXCLUSIVE = pd.Timestamp("2024-01-01T00:00:00Z")
MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
MINI_TO_MICRO = {"ES": "MES", "NQ": "MNQ", "RTY": "M2K", "YM": "MYM"}
MINIS = tuple(MINI_TO_MICRO)
ALL_SYMBOLS = tuple(symbol for pair in MINI_TO_MICRO.items() for symbol in pair)
MECHANISMS = (
    "RESIDUAL_DISPERSION_CONVERGENCE",
    "BROAD_INVENTORY_CONTINUATION",
)
DECISION_MINUTES = (14 * 60 + 15, 14 * 60 + 45)
QUANTILES = (0.70, 0.85)
FOLDS = {
    "2023": ("2023-01-01", "2024-01-01"),
    "2024_q1": ("2024-01-01", "2024-04-01"),
    "2024_q2": ("2024-04-01", "2024-07-01"),
    "2024_q3": ("2024-07-01", "2024-10-01"),
}
COMMISSION_ROUND_TURN = {symbol: 2.0 for symbol in MINI_TO_MICRO.values()}


class EquityPrecloseResearchError(RuntimeError):
    """A frozen-input or hard scientific-integrity invariant failed."""


def _sha256(path: Path) -> str:
    return _file_sha256(Path(path))


def _verify_file(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _sha256(path) != str(expected):
        raise EquityPrecloseResearchError(f"Frozen {label} missing or changed: {path}")


def _canonical_hash(payload: Any) -> str:
    return _stable_hash(_strict_json_value(payload))


def _write_immutable(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise EquityPrecloseResearchError(f"Refusing divergent immutable artifact: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _json_text(payload: Any) -> str:
    return json.dumps(_strict_json_value(payload), indent=2, sort_keys=True, allow_nan=False) + "\n"


def _jsonl_text(rows: Iterable[Mapping[str, Any]]) -> str:
    return "".join(
        json.dumps(_strict_json_value(dict(row)), sort_keys=True, allow_nan=False) + "\n"
        for row in rows
    )


def structural_manifest() -> list[dict[str, Any]]:
    """Return the immutable, outcome-neutral population of exactly 32 structures."""

    rows: list[dict[str, Any]] = []
    for target in MINIS:
        for mechanism in MECHANISMS:
            for minute in DECISION_MINUTES:
                for quantile in QUANTILES:
                    clock = f"{minute // 60:02d}{minute % 60:02d}"
                    short = "convergence" if mechanism.startswith("RESIDUAL") else "continuation"
                    payload = {
                        "protocol": PROTOCOL,
                        "target_market": target,
                        "execution_market": MINI_TO_MICRO[target],
                        "mechanism": mechanism,
                        "decision_minute_chicago": minute,
                        "threshold_quantile": quantile,
                        "exit_minute_chicago": 15 * 60 + 5,
                        "selection_period": "2023_ONLY",
                        "target_pool": "COMBINE_PASSER_POOL",
                        "status_ceiling": "PROMISING_RESEARCH_CANDIDATE",
                    }
                    candidate_id = f"equity_preclose_{target}_{short}_{clock}_q{int(quantile*100)}_v1"
                    rows.append(
                        {
                            "candidate_id": candidate_id,
                            **payload,
                            "structural_fingerprint": _canonical_hash(payload),
                            "status_inherited": False,
                            "inherited_passes": [],
                        }
                    )
    if len(rows) != 32 or len({row["structural_fingerprint"] for row in rows}) != 32:
        raise EquityPrecloseResearchError("Frozen population is not exactly 32 unique structures")
    return rows


def _read_governed_bars(paths: list[Path], roll_map: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    pieces: list[pd.DataFrame] = []
    columns = ["timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume"]
    for path in paths:
        frame = pd.read_parquet(path, columns=columns, filters=[("symbol", "in", list(ALL_SYMBOLS))])
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="raise")
        frame = frame.loc[
            frame["symbol"].astype(str).isin(ALL_SYMBOLS)
            & frame["timeframe"].astype(str).eq("1m")
            & frame["timestamp"].lt(DEVELOPMENT_END_EXCLUSIVE)
        ].copy()
        local = frame["timestamp"].dt.tz_convert("America/Chicago")
        minute = local.dt.hour * 60 + local.dt.minute
        frame = frame.loc[minute.between(8 * 60 + 30, 15 * 60 + 4)].copy()
        pieces.append(frame)
    combined = pd.concat(pieces, ignore_index=True)
    combined = combined.drop_duplicates(["symbol", "timestamp"], keep="first")
    combined = combined.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)
    if combined.empty or combined["timestamp"].ge(DEVELOPMENT_END_EXCLUSIVE).any():
        raise EquityPrecloseResearchError("Development bars are empty or cross the protected boundary")
    mapped, details = _apply_explicit_contract_map(
        combined,
        roll_map,
        required_map_type=MAP_TYPE,
    )
    if mapped.empty or set(MINIS) - set(mapped["symbol"].astype(str)):
        raise EquityPrecloseResearchError("Explicit-contract mapping removed a required signal market")
    mapped["timestamp"] = pd.to_datetime(mapped["timestamp"], utc=True)
    return mapped, {
        "rows_before_contract_guards": int(len(combined)),
        "rows_after_contract_guards": int(len(mapped)),
        **details,
    }


def _session_symbol_rows(bars: pd.DataFrame) -> pd.DataFrame:
    """Collapse 1m bars to causal same-clock signal and future execution rows."""

    frame = bars.copy()
    local = frame["timestamp"].dt.tz_convert("America/Chicago")
    frame["local_minute"] = local.dt.hour * 60 + local.dt.minute
    frame["event_session_id"] = local.dt.strftime("%Y-%m-%d")
    records: list[dict[str, Any]] = []
    for (session, symbol), group in frame.groupby(["event_session_id", "symbol"], sort=True):
        group = group.sort_values("timestamp", kind="mergesort")
        by_minute = {int(row.local_minute): row for row in group.itertuples(index=False)}
        open_row = by_minute.get(8 * 60 + 30)
        exit_row = by_minute.get(15 * 60 + 4)
        if open_row is None or exit_row is None:
            continue
        for decision in DECISION_MINUTES:
            entry = by_minute.get(decision)
            delay1 = by_minute.get(decision + 1)
            delay5 = by_minute.get(decision + 5)
            previous = by_minute.get(decision - 1)
            if any(value is None for value in (entry, delay1, delay5, previous)):
                continue
            prefix = group.loc[group["local_minute"].between(8 * 60 + 30, decision - 1)]
            outcome = group.loc[group["local_minute"].between(decision, 15 * 60 + 4)]
            if len(prefix) != decision - (8 * 60 + 30) or outcome.empty:
                continue
            if str(entry.active_contract) != str(exit_row.active_contract):
                continue
            start = float(open_row.open)
            last = float(previous.close)
            closes = np.concatenate(([start], prefix["close"].to_numpy(dtype=float)))
            path_length = float(np.abs(np.diff(closes)).sum())
            displacement = last / start - 1.0
            efficiency = abs(last - start) / max(path_length, 1e-12)
            source_close = pd.Timestamp(previous.timestamp) + pd.Timedelta(minutes=1)
            decision_ts = pd.Timestamp(entry.timestamp)
            if source_close != decision_ts:
                raise EquityPrecloseResearchError("A feature used an incomplete 1m bar")
            records.append(
                {
                    "event_session_id": str(session),
                    "symbol": str(symbol),
                    "decision_minute": int(decision),
                    "decision_timestamp": decision_ts,
                    "source_bar_start": pd.Timestamp(previous.timestamp),
                    "source_bar_close": source_close,
                    "availability_timestamp": source_close,
                    "active_contract": str(entry.active_contract),
                    "displacement": float(displacement),
                    "path_efficiency": float(np.clip(efficiency, 0.0, 1.0)),
                    "cumulative_volume": float(prefix["volume"].sum()),
                    "session_return": float(float(exit_row.close) / start - 1.0),
                    "entry_price": float(entry.open),
                    "delay_1m_entry_price": float(delay1.open),
                    "delay_5m_entry_price": float(delay5.open),
                    "exit_price": float(exit_row.close),
                    "path_low": float(outcome["low"].min()),
                    "path_high": float(outcome["high"].max()),
                    "exit_timestamp": pd.Timestamp(exit_row.timestamp) + pd.Timedelta(minutes=1),
                }
            )
    out = pd.DataFrame(records)
    if out.empty:
        raise EquityPrecloseResearchError("No complete synchronized RTH sessions")
    return out.sort_values(["event_session_id", "decision_minute", "symbol"]).reset_index(drop=True)


def build_causal_contexts(rows: pd.DataFrame) -> pd.DataFrame:
    """Build cross-market features with all rolling inputs shifted one session."""

    frame = rows.copy().sort_values(["symbol", "decision_minute", "event_session_id"])
    grouped = frame.groupby(["symbol", "decision_minute"], sort=False)
    frame["prior_realized_volatility"] = grouped["session_return"].transform(
        lambda values: values.shift(1).rolling(20, min_periods=10).std(ddof=1)
    )
    frame["prior_same_clock_volume"] = grouped["cumulative_volume"].transform(
        lambda values: values.shift(1).rolling(20, min_periods=10).median()
    )
    frame["participation"] = frame["cumulative_volume"] / frame["prior_same_clock_volume"]

    mini = frame.loc[frame["symbol"].isin(MINIS)].copy()
    micro = frame.loc[frame["symbol"].isin(MINI_TO_MICRO.values())].copy()
    key = ["event_session_id", "decision_minute"]
    wide_disp = mini.pivot(index=key, columns="symbol", values="displacement")
    wide_eff = mini.pivot(index=key, columns="symbol", values="path_efficiency")
    wide_vol = mini.pivot(index=key, columns="symbol", values="prior_realized_volatility")
    wide_part = mini.pivot(index=key, columns="symbol", values="participation")
    complete_index = wide_disp.dropna(subset=list(MINIS)).index
    complete_index = complete_index.intersection(wide_eff.dropna(subset=list(MINIS)).index)
    complete_index = complete_index.intersection(wide_vol.dropna(subset=list(MINIS)).index)
    complete_index = complete_index.intersection(wide_part.dropna(subset=list(MINIS)).index)

    contexts: list[dict[str, Any]] = []
    micro_lookup = {
        (str(row.event_session_id), int(row.decision_minute), str(row.symbol)): row
        for row in micro.itertuples(index=False)
    }
    mini_lookup = {
        (str(row.event_session_id), int(row.decision_minute), str(row.symbol)): row
        for row in mini.itertuples(index=False)
    }
    for session, decision in complete_index:
        displacements = wide_disp.loc[(session, decision), list(MINIS)].astype(float)
        efficiencies = wide_eff.loc[(session, decision), list(MINIS)].astype(float)
        participations = wide_part.loc[(session, decision), list(MINIS)].astype(float)
        volatilities = wide_vol.loc[(session, decision), list(MINIS)].astype(float).clip(lower=1e-8)
        signs = np.sign(displacements.to_numpy(dtype=float))
        for target in MINIS:
            others = [symbol for symbol in MINIS if symbol != target]
            inverse = 1.0 / volatilities.loc[others]
            weights = inverse / inverse.sum()
            common = float((displacements.loc[others] * weights).sum())
            common_sign = float(np.sign(common))
            breadth = float(np.mean(signs == common_sign)) if common_sign else 0.0
            residual = float(displacements.loc[target] - common)
            signal = mini_lookup.get((str(session), int(decision), target))
            execution = micro_lookup.get((str(session), int(decision), MINI_TO_MICRO[target]))
            if signal is None or execution is None:
                continue
            if pd.Timestamp(signal.decision_timestamp) != pd.Timestamp(execution.decision_timestamp):
                raise EquityPrecloseResearchError("Mini/micro timestamps are not synchronized")
            contexts.append(
                {
                    "event_session_id": str(session),
                    "target_market": target,
                    "execution_market": MINI_TO_MICRO[target],
                    "decision_minute": int(decision),
                    "decision_timestamp": pd.Timestamp(signal.decision_timestamp),
                    "source_bar_start": pd.Timestamp(signal.source_bar_start),
                    "source_bar_close": pd.Timestamp(signal.source_bar_close),
                    "availability_timestamp": pd.Timestamp(signal.availability_timestamp),
                    "signal_contract": str(signal.active_contract),
                    "execution_contract": str(execution.active_contract),
                    "target_displacement": float(displacements.loc[target]),
                    "common_displacement": common,
                    "residual_displacement": residual,
                    "dispersion": float(displacements.std(ddof=0)),
                    "breadth": breadth,
                    "all_four_agree": bool(common_sign != 0.0 and np.all(signs == common_sign)),
                    "target_efficiency": float(efficiencies.loc[target]),
                    "mean_efficiency": float(efficiencies.mean()),
                    "target_participation": float(participations.loc[target]),
                    "mean_participation": float(participations.mean()),
                    "prior_realized_volatility": float(volatilities.loc[target]),
                    "common_factor_weight_hash": _canonical_hash(
                        {symbol: float(weights.loc[symbol]) for symbol in others}
                    ),
                    "entry_price": float(execution.entry_price),
                    "delay_1m_entry_price": float(execution.delay_1m_entry_price),
                    "delay_5m_entry_price": float(execution.delay_5m_entry_price),
                    "exit_price": float(execution.exit_price),
                    "path_low": float(execution.path_low),
                    "path_high": float(execution.path_high),
                    "exit_timestamp": pd.Timestamp(execution.exit_timestamp),
                }
            )
    out = pd.DataFrame(contexts).sort_values(
        ["target_market", "decision_minute", "event_session_id"], kind="mergesort"
    )
    if out.empty:
        raise EquityPrecloseResearchError("No complete mini/micro cross-market contexts")
    if (out["source_bar_close"] > out["decision_timestamp"]).any() or (
        out["availability_timestamp"] > out["decision_timestamp"]
    ).any():
        raise EquityPrecloseResearchError("Future feature availability detected")
    grouped_context = out.groupby(["target_market", "decision_minute"], sort=False)
    out["prior_residual_scale"] = grouped_context["residual_displacement"].transform(
        lambda values: values.shift(1).rolling(40, min_periods=20).std(ddof=1)
    )
    out["prior_common_scale"] = grouped_context["common_displacement"].transform(
        lambda values: values.shift(1).rolling(40, min_periods=20).std(ddof=1)
    )
    out["residual_z"] = out["residual_displacement"] / out["prior_residual_scale"].clip(lower=1e-8)
    out["common_z"] = out["common_displacement"] / out["prior_common_scale"].clip(lower=1e-8)
    return out.reset_index(drop=True)


def _score_rows(contexts: pd.DataFrame, specification: Mapping[str, Any]) -> pd.DataFrame:
    selected = contexts.loc[
        contexts["target_market"].eq(str(specification["target_market"]))
        & contexts["decision_minute"].eq(int(specification["decision_minute_chicago"]))
    ].copy()
    mechanism = str(specification["mechanism"])
    if mechanism == "RESIDUAL_DISPERSION_CONVERGENCE":
        selected["score"] = (
            selected["residual_z"].abs()
            * (2.0 - selected["breadth"])
            * (2.0 - selected["target_efficiency"])
        )
        selected["side"] = -np.sign(selected["residual_displacement"])
        selected["structurally_eligible"] = selected["breadth"].lt(1.0)
    else:
        selected["score"] = (
            selected["common_z"].abs()
            * selected["breadth"]
            * selected["mean_efficiency"]
            * selected["target_participation"].clip(lower=0.0, upper=3.0)
        )
        selected["side"] = np.sign(selected["common_displacement"])
        selected["structurally_eligible"] = selected["all_four_agree"]
    selected = selected.replace([np.inf, -np.inf], np.nan).dropna(subset=["score", "side"])
    return selected.loc[selected["side"].ne(0.0)].sort_values("event_session_id").reset_index(drop=True)


def apply_causal_thresholds(
    scored: pd.DataFrame, quantile: float
) -> tuple[pd.DataFrame, float | None]:
    """Fit expanding shifted thresholds in 2023 and freeze them before 2024."""

    frame = scored.copy().sort_values("event_session_id").reset_index(drop=True)
    is_fit = frame["event_session_id"].astype(str).lt("2024-01-01")
    fit_scores = frame.loc[is_fit, "score"]
    frame["threshold"] = np.nan
    frame.loc[is_fit, "threshold"] = fit_scores.shift(1).expanding(min_periods=40).quantile(quantile)
    frozen = float(fit_scores.quantile(quantile)) if len(fit_scores) >= 40 else None
    if frozen is not None:
        frame.loc[~is_fit, "threshold"] = frozen
    frame["threshold_fit_cutoff"] = np.where(
        is_fit,
        frame["event_session_id"].shift(1),
        "2023-12-31",
    )
    event = (
        frame["structurally_eligible"].astype(bool)
        & frame["threshold"].notna()
        & frame["score"].ge(frame["threshold"])
    )
    return frame.loc[event].copy().reset_index(drop=True), frozen


def _round_turn_cost(symbol: str) -> float:
    spec = instrument_spec(symbol)
    return float(COMMISSION_ROUND_TURN[symbol] + 2.0 * spec.tick_value)


def _materialize_events(selected: pd.DataFrame, specification: Mapping[str, Any]) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame()
    frame = selected.copy()
    symbol = str(specification["execution_market"])
    point = instrument_spec(symbol).point_value
    side = frame["side"].astype(float)
    frame["gross_pnl"] = (frame["exit_price"] - frame["entry_price"]) * side * point
    frame["delay_1m_gross_pnl"] = (
        frame["exit_price"] - frame["delay_1m_entry_price"]
    ) * side * point
    frame["delay_5m_gross_pnl"] = (
        frame["exit_price"] - frame["delay_5m_entry_price"]
    ) * side * point
    adverse = np.where(
        side > 0,
        (frame["path_low"] - frame["entry_price"]) * point,
        (frame["entry_price"] - frame["path_high"]) * point,
    )
    frame["cost"] = _round_turn_cost(symbol)
    frame["mae_dollars"] = np.minimum(adverse, 0.0) - 0.5 * frame["cost"]
    frame["net_pnl"] = frame["gross_pnl"] - frame["cost"]
    frame["net_pnl_1_5x"] = frame["gross_pnl"] - 1.5 * frame["cost"]
    frame["net_pnl_2x"] = frame["gross_pnl"] - 2.0 * frame["cost"]
    frame["delay_1m_net_pnl"] = frame["delay_1m_gross_pnl"] - frame["cost"]
    frame["delay_5m_net_pnl"] = frame["delay_5m_gross_pnl"] - frame["cost"]
    frame["candidate_id"] = str(specification["candidate_id"])
    frame["strategy_id"] = frame["candidate_id"]
    frame["trade_id"] = [f"{specification['candidate_id']}:{day}" for day in frame["event_session_id"]]
    frame["contracts"] = 1
    frame["underlying"] = str(specification["target_market"])
    frame["role"] = "alpha"
    frame["target_pool"] = "COMBINE_PASSER_POOL"
    frame["status_inherited"] = False
    frame["active_contract"] = frame["execution_contract"]
    frame["entry_timestamp"] = frame["decision_timestamp"]
    return frame.sort_values("entry_timestamp", kind="mergesort").reset_index(drop=True)


def _maximum_drawdown(values: pd.Series) -> float:
    if values.empty:
        return 0.0
    equity = np.concatenate(([0.0], values.astype(float).cumsum().to_numpy()))
    return float(np.max(np.maximum.accumulate(equity) - equity))


def _period_metrics(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "events": 0,
            "net_pnl": 0.0,
            "net_pnl_1_5x": 0.0,
            "net_pnl_2x": 0.0,
            "maximum_drawdown": 0.0,
            "best_trade_removed_net": 0.0,
            "best_day_removed_net": 0.0,
            "best_month_removed_net": 0.0,
        }
    net = events["net_pnl"].astype(float)
    session = events.groupby("event_session_id", sort=True)["net_pnl"].sum()
    months = events.assign(month=events["event_session_id"].astype(str).str[:7]).groupby("month")["net_pnl"].sum()
    return {
        "events": int(len(events)),
        "gross_pnl": float(events["gross_pnl"].sum()),
        "net_pnl": float(net.sum()),
        "net_pnl_1_5x": float(events["net_pnl_1_5x"].sum()),
        "net_pnl_2x": float(events["net_pnl_2x"].sum()),
        "delay_1m_net_pnl": float(events["delay_1m_net_pnl"].sum()),
        "delay_5m_net_pnl": float(events["delay_5m_net_pnl"].sum()),
        "maximum_drawdown": _maximum_drawdown(net),
        "best_trade_removed_net": float(net.sum() - net.max()),
        "best_day_removed_net": float(net.sum() - session.max()),
        "best_month_removed_net": float(net.sum() - months.max()),
        "best_positive_trade_share": float(net.max() / net[net > 0].sum())
        if (net > 0).any()
        else 1.0,
    }


def _fold_events(events: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    session = events["event_session_id"].astype(str)
    return events.loc[session.ge(start) & session.lt(end)].copy()


def _ledger_for_account(events: pd.DataFrame, *, contracts: int = 1) -> pd.DataFrame:
    columns = [
        "trade_id",
        "event_session_id",
        "entry_timestamp",
        "exit_timestamp",
        "availability_timestamp",
        "source_bar_close",
        "net_pnl",
        "mae_dollars",
        "cost",
        "underlying",
        "side",
    ]
    ledger = events[columns].copy()
    ledger["contracts"] = int(contracts)
    if contracts != 1:
        ledger[["net_pnl", "mae_dollars", "cost"]] *= float(contracts)
        ledger["trade_id"] = ledger["trade_id"].astype(str) + f":x{contracts}"
    return ledger


def _account_evidence(events: pd.DataFrame, candidate_id: str) -> dict[str, Any]:
    if events.empty:
        return {"one_micro": None, "ten_micro": None, "path_candidate": False}
    one = replay_shared_account({candidate_id: _ledger_for_account(events)}).to_dict()
    ten = replay_shared_account({candidate_id: _ledger_for_account(events, contracts=10)}).to_dict()
    return {
        "target_pool": "COMBINE_PASSER_POOL",
        "one_micro": one,
        "ten_micro": ten,
        "path_candidate": bool(ten["target_before_mll"] and ten["consistency_ok"]),
    }


def _block_sign_flip_probability(events: pd.DataFrame, seed: int) -> float:
    if len(events) < 10:
        return 1.0
    gross = events.sort_values("entry_timestamp")["gross_pnl"].to_numpy(dtype=float)
    costs = events.sort_values("entry_timestamp")["cost"].to_numpy(dtype=float)
    blocks = np.arange(len(gross)) // 5
    rng = np.random.default_rng(seed)
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(4096, int(blocks.max()) + 1))
    null = (signs[:, blocks] * gross).sum(axis=1) - costs.sum()
    observed = gross.sum() - costs.sum()
    return float((1 + np.count_nonzero(null >= observed)) / 4097)


def _matched_event_probability(
    contexts: pd.DataFrame,
    events: pd.DataFrame,
    specification: Mapping[str, Any],
    *,
    seed: int,
    draws: int = 511,
) -> float:
    """Count/volatility-matched event null over the same target and clock.

    The null samples structurally eligible opportunities in the same prior-vol
    quartiles and period as the observed events.  It preserves event count and
    applies the preregistered mechanism direction to the sampled opportunity.
    """

    if len(events) < 10:
        return 1.0
    pool = _score_rows(contexts, specification)
    pool = pool.loc[pool["structurally_eligible"].astype(bool)].copy()
    if pool.empty:
        return 1.0
    pool["period"] = np.where(
        pool["event_session_id"].astype(str).lt("2024-01-01"),
        "2023",
        pool["event_session_id"].astype(str).str[:7],
    )
    try:
        pool["volatility_band"] = pd.qcut(
            pool["prior_realized_volatility"], 4, labels=False, duplicates="drop"
        ).fillna(0).astype(int)
    except ValueError:
        pool["volatility_band"] = 0
    observed_keys = events[["event_session_id"]].copy()
    observed_keys["period"] = np.where(
        observed_keys["event_session_id"].astype(str).lt("2024-01-01"),
        "2023",
        observed_keys["event_session_id"].astype(str).str[:7],
    )
    observed_keys = observed_keys.merge(
        pool[["event_session_id", "volatility_band"]].drop_duplicates("event_session_id"),
        on="event_session_id",
        how="left",
    )
    if observed_keys["volatility_band"].isna().any():
        return 1.0
    grouped_pool = {
        key: indices.to_numpy(dtype=int)
        for key, indices in pool.groupby(["period", "volatility_band"], sort=True).groups.items()
    }
    counts = observed_keys.groupby(["period", "volatility_band"]).size().to_dict()
    if any(key not in grouped_pool or len(grouped_pool[key]) < count for key, count in counts.items()):
        return 1.0
    point = instrument_spec(str(specification["execution_market"])).point_value
    cost = _round_turn_cost(str(specification["execution_market"]))
    pool["null_net"] = (
        (pool["exit_price"] - pool["entry_price"]) * pool["side"] * point - cost
    )
    observed = float(events["net_pnl"].sum())
    rng = np.random.default_rng(seed)
    null = np.zeros(draws, dtype=float)
    for draw in range(draws):
        total = 0.0
        for key, count in sorted(counts.items()):
            sampled = rng.choice(grouped_pool[key], size=int(count), replace=False)
            total += float(pool.iloc[sampled]["null_net"].sum())
        null[draw] = total
    return float((1 + np.count_nonzero(null >= observed)) / (draws + 1))


def _bh_family(probabilities: Mapping[str, float], universe: list[str]) -> dict[str, float]:
    values = np.asarray([float(probabilities.get(candidate_id, 1.0)) for candidate_id in universe])
    order = np.argsort(values)
    adjusted = np.ones(len(values), dtype=float)
    running = 1.0
    for reverse in range(len(values) - 1, -1, -1):
        index = int(order[reverse])
        rank = reverse + 1
        running = min(running, float(values[index]) * len(values) / rank)
        adjusted[index] = min(running, 1.0)
    return {candidate_id: float(adjusted[index]) for index, candidate_id in enumerate(universe)}


def select_frozen_elites(stage1: list[dict[str, Any]], maximum: int = 4) -> list[dict[str, Any]]:
    """Deterministic 2023-only max-feasible QD selector with behavior dedupe."""

    ordered = sorted(
        [row for row in stage1 if row.get("stage1_passed")],
        key=lambda row: (-float(row["quality"]), str(row["candidate_id"])),
    )
    selected: list[dict[str, Any]] = []
    mechanism_counts: Counter[str] = Counter()
    clock_counts: Counter[int] = Counter()
    behaviors: set[str] = set()
    for row in ordered:
        mechanism = str(row["mechanism"])
        clock = int(row["decision_minute_chicago"])
        behavior = str(row["behavior_fingerprint"])
        if mechanism_counts[mechanism] >= 2 or clock_counts[clock] >= 2 or behavior in behaviors:
            continue
        selected.append(row)
        mechanism_counts[mechanism] += 1
        clock_counts[clock] += 1
        behaviors.add(behavior)
        if len(selected) >= maximum:
            break
    return selected


def _baseline_evidence(contexts: pd.DataFrame, specification: Mapping[str, Any]) -> dict[str, float]:
    base = contexts.loc[
        contexts["target_market"].eq(str(specification["target_market"]))
        & contexts["decision_minute"].eq(int(specification["decision_minute_chicago"]))
    ].copy()
    base = base.loc[base["event_session_id"].astype(str).lt("2024-10-01")]
    means: dict[str, float] = {}
    for name, side in {
        "time_of_day_long": np.ones(len(base)),
        "target_displacement_only": np.sign(base["target_displacement"].to_numpy(dtype=float)),
        "breadth_only": np.sign(base["common_displacement"].to_numpy(dtype=float)),
    }.items():
        gross = (base["exit_price"] - base["entry_price"]) * side * instrument_spec(
            str(specification["execution_market"])
        ).point_value
        means[name] = float(np.mean(gross - _round_turn_cost(str(specification["execution_market"]))))
    return means


def _event_rows(events: pd.DataFrame) -> list[dict[str, Any]]:
    keep = [
        "candidate_id", "trade_id", "event_session_id", "target_market", "execution_market",
        "signal_contract", "execution_contract", "decision_minute", "decision_timestamp",
        "source_bar_start", "source_bar_close", "availability_timestamp", "exit_timestamp",
        "threshold_fit_cutoff", "score", "threshold", "side", "entry_price", "exit_price",
        "gross_pnl", "cost", "net_pnl", "net_pnl_1_5x", "net_pnl_2x",
        "delay_1m_net_pnl", "delay_5m_net_pnl", "mae_dollars",
        "target_displacement", "common_displacement", "residual_displacement", "dispersion",
        "breadth", "target_efficiency", "target_participation", "common_factor_weight_hash",
        "status_inherited",
    ]
    return [
        {
            key: (value.isoformat() if isinstance(value, pd.Timestamp) else value)
            for key, value in row.items()
        }
        for row in events[keep].to_dict(orient="records")
    ]


def _artifact(path: Path) -> dict[str, str]:
    return {"path": str(path.resolve()), "sha256": _sha256(path)}


def _render_report(result: Mapping[str, Any]) -> str:
    lines = [
        "# HYDRA equity pre-close inventory/dispersion primary",
        "",
        f"- Protocol: `{PROTOCOL}`",
        f"- Frozen structures: {result['structural_prototypes']}",
        f"- 2023 Stage-1 survivors: {result['stage1_survivors']}",
        f"- Frozen elites: {result['frozen_elite_count']}",
        f"- Promising research candidates: {result['promising_candidates']}",
        "- Target pool: `COMBINE_PASSER_POOL` (XFA and defensive utility are not universal gates)",
        "- Q4 / network / paid data / orders / Shadow / Paper: 0",
        "",
    ]
    for candidate in result["candidates"]:
        lines.append(
            f"- `{candidate['candidate_id']}`: `{candidate['status']}`; pooled 1.5x "
            f"{candidate['pooled_metrics']['net_pnl_1_5x']:.2f}; adjusted p "
            f"{candidate['null_evidence']['family_adjusted_probability']:.6f}"
        )
    lines.extend(["", "No result is a holdout result or funded-readiness claim.", ""])
    return "\n".join(lines)


def run_equity_preclose_inventory_dispersion(
    output_dir: str | Path,
    *,
    engineering_task_path: Path,
    engineering_task_sha256: str,
    core_data_paths: list[Path],
    core_data_sha256s: list[str],
    roll_map_path: Path,
    roll_map_sha256: str,
    roll_map_hash: str,
    source_role_epoch_result_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    """Execute the preregistered 32 -> at-most-4 development-only primary."""

    task = Path(engineering_task_path)
    if str(engineering_task_sha256) != PREREGISTRATION_SHA256:
        raise EquityPrecloseResearchError("Unexpected preregistration hash")
    _verify_file(task, engineering_task_sha256, "engineering task")
    if len(core_data_paths) != 5 or len(core_data_sha256s) != 5:
        raise EquityPrecloseResearchError("Exactly five frozen core sources are required")
    for index, (path, digest) in enumerate(zip(core_data_paths, core_data_sha256s, strict=True), 1):
        _verify_file(Path(path), digest, f"core data {index}")
    _verify_file(Path(roll_map_path), roll_map_sha256, "roll map")
    if len(str(source_role_epoch_result_hash)) != 64 or not str(code_commit).strip():
        raise EquityPrecloseResearchError("Frozen predecessor hash and code commit are required")

    roll_map = load_roll_map(roll_map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != str(roll_map_hash):
        raise EquityPrecloseResearchError("Roll-map semantic contract changed")
    population = structural_manifest()
    candidate_ids = [row["candidate_id"] for row in population]
    if record_data_access:
        enforce_data_access(
            "2023-01-01:2024-10-01_EXCLUSIVE",
            DataRole.CONTAMINATED_DEVELOPMENT,
            "hydra.research.equity_preclose_inventory_dispersion",
            candidate_ids,
            "Frozen 2023 selection and unchanged 2024 Q1-Q3 development replay",
            None,
        )

    bars, contract_details = _read_governed_bars([Path(path) for path in core_data_paths], roll_map)
    rows = _session_symbol_rows(bars)
    contexts = build_causal_contexts(rows)
    data_provenance = {
        "files": [
            {"path": str(Path(path).resolve()), "sha256": digest}
            for path, digest in zip(core_data_paths, core_data_sha256s, strict=True)
        ],
        "roll_map_path": str(Path(roll_map_path).resolve()),
        "roll_map_sha256": roll_map_sha256,
        "roll_map_hash": roll_map_hash,
        "contract_details": contract_details,
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE.isoformat(),
    }
    data_provenance["data_fingerprint"] = _canonical_hash(data_provenance)

    stage1: list[dict[str, Any]] = []
    event_cache: dict[str, pd.DataFrame] = {}
    frozen_thresholds: dict[str, float | None] = {}
    for index, specification in enumerate(population):
        scored = _score_rows(contexts, specification)
        selected, threshold = apply_causal_thresholds(scored, float(specification["threshold_quantile"]))
        events = _materialize_events(selected, specification)
        discovery = _fold_events(events, "2023-01-01", "2024-01-01")
        metrics = _period_metrics(discovery)
        account = _account_evidence(discovery, str(specification["candidate_id"]))
        behavior = _canonical_hash(
            [[str(row.event_session_id), float(row.side)] for row in discovery.itertuples()]
        )
        passed = bool(
            metrics["events"] >= 40
            and metrics["net_pnl_1_5x"] > 0.0
            and metrics["best_trade_removed_net"] >= -0.5 * max(metrics["net_pnl"], 0.0)
            and metrics["best_day_removed_net"] >= -0.5 * max(metrics["net_pnl"], 0.0)
            and metrics["best_month_removed_net"] >= -0.5 * max(metrics["net_pnl"], 0.0)
            and bool((account.get("one_micro") or {}).get("mll_breached") is False)
        )
        quality = float(metrics["net_pnl_1_5x"] / (metrics["maximum_drawdown"] + 1.0))
        stage1.append(
            {
                **specification,
                "stage1_metrics": metrics,
                "stage1_passed": passed,
                "quality": quality,
                "behavior_fingerprint": behavior,
                "frozen_2023_threshold": threshold,
                "selection_used_2024_results": False,
                "account_evidence": account,
            }
        )
        event_cache[str(specification["candidate_id"])] = events
        frozen_thresholds[str(specification["candidate_id"])] = threshold

    elites = select_frozen_elites(stage1)
    elite_ids = [str(row["candidate_id"]) for row in elites]
    sign_flip_probabilities: dict[str, float] = {}
    matched_probabilities: dict[str, float] = {}
    raw_probabilities: dict[str, float] = {}
    population_by_id = {str(row["candidate_id"]): row for row in population}
    for index, candidate_id in enumerate(candidate_ids):
        if candidate_id not in elite_ids:
            sign_flip_probabilities[candidate_id] = 1.0
            matched_probabilities[candidate_id] = 1.0
            raw_probabilities[candidate_id] = 1.0
            continue
        sign_flip = _block_sign_flip_probability(event_cache[candidate_id], 71300 + index)
        matched = _matched_event_probability(
            contexts,
            event_cache[candidate_id],
            population_by_id[candidate_id],
            seed=81300 + index,
        )
        sign_flip_probabilities[candidate_id] = sign_flip
        matched_probabilities[candidate_id] = matched
        raw_probabilities[candidate_id] = max(sign_flip, matched)
    adjusted = _bh_family(raw_probabilities, candidate_ids)
    candidates: list[dict[str, Any]] = []
    complete_events: list[pd.DataFrame] = []
    for elite in elites:
        candidate_id = str(elite["candidate_id"])
        events = event_cache[candidate_id]
        complete_events.append(events)
        folds = {
            name: _period_metrics(_fold_events(events, start, end))
            for name, (start, end) in FOLDS.items()
        }
        pooled = _period_metrics(events)
        supportive = sum(value["net_pnl_1_5x"] > 0.0 for value in folds.values())
        positive_fold_net = sum(max(float(value["net_pnl_1_5x"]), 0.0) for value in folds.values())
        catastrophic = any(
            float(value["net_pnl_1_5x"]) < -0.5 * max(positive_fold_net, 1.0)
            for value in folds.values()
        )
        baselines = _baseline_evidence(contexts, elite)
        candidate_mean = pooled["net_pnl"] / max(pooled["events"], 1)
        simpler_explanation_pass = candidate_mean > max(baselines.values(), default=-math.inf)
        account = _account_evidence(events, candidate_id)
        removal_floor = -0.5 * max(float(pooled["net_pnl"]), 0.0)
        removal_pass = all(
            float(pooled[key]) >= removal_floor
            for key in ("best_trade_removed_net", "best_day_removed_net", "best_month_removed_net")
        )
        delay_pass = bool(pooled["delay_1m_net_pnl"] > 0.0 and pooled["delay_5m_net_pnl"] > 0.0)
        promising = bool(
            pooled["net_pnl_1_5x"] > 0.0
            and supportive >= 2
            and not catastrophic
            and adjusted[candidate_id] <= 0.10
            and removal_pass
            and delay_pass
            and simpler_explanation_pass
            and not bool((account.get("one_micro") or {}).get("mll_breached", True))
        )
        uncertainties: list[str] = []
        if pooled["net_pnl_1_5x"] <= 0.0:
            uncertainties.append("POOLED_COST_STRESS_NONPOSITIVE")
        if supportive < 2 or catastrophic:
            uncertainties.append("TEMPORAL_TRANSFER_INSUFFICIENT_OR_CATASTROPHIC")
        if adjusted[candidate_id] > 0.10:
            uncertainties.append("FAMILY_ADJUSTED_NULL_NOT_DISCRIMINATIVE")
        if not removal_pass:
            uncertainties.append("EVENT_DAY_OR_MONTH_CONCENTRATION")
        if not delay_pass:
            uncertainties.append("DELAY_FRAGILITY")
        if not simpler_explanation_pass:
            uncertainties.append("SIMPLER_BASELINE_NOT_BEATEN")
        status = "PROMISING_RESEARCH_CANDIDATE" if promising else "INSUFFICIENT_EVIDENCE"
        candidates.append(
            {
                "candidate_id": candidate_id,
                "status": status,
                "status_inherited": False,
                "inherited_passes": [],
                "role": "alpha",
                "target_pool": "COMBINE_PASSER_POOL",
                "primary_market": elite["target_market"],
                "execution_market": elite["execution_market"],
                "mechanism_family": elite["mechanism"],
                "structural_fingerprint": elite["structural_fingerprint"],
                "behavior_fingerprint": elite["behavior_fingerprint"],
                "frozen_2023_threshold": frozen_thresholds[candidate_id],
                "selection_used_2024_results": False,
                "folds": folds,
                "pooled_metrics": pooled,
                "supportive_temporal_folds": supportive,
                "catastrophic_transfer": catastrophic,
                "null_evidence": {
                    "tests": [
                        "five_session_block_sign_flip",
                        "same_target_clock_period_and_prior_volatility_matched_events",
                    ],
                    "sign_flip_probability": sign_flip_probabilities[candidate_id],
                    "matched_event_probability": matched_probabilities[candidate_id],
                    "raw_probability": raw_probabilities[candidate_id],
                    "family_adjusted_probability": adjusted[candidate_id],
                    "family_size": 32,
                },
                "simpler_baselines_mean_net": baselines,
                "simpler_explanation_passed": simpler_explanation_pass,
                "account_evidence": account,
                "uncertainty": uncertainties,
                "q4_access_count": 0,
                "shadow_research_active": 0,
                "paper_shadow_ready": 0,
                "order_capability": False,
            }
        )

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "equity_preclose_structural_manifest.json"
    stage1_path = output / "equity_preclose_2023_screen.jsonl"
    elite_path = output / "equity_preclose_frozen_elite_manifest.json"
    events_path = output / "equity_preclose_trade_ledger.jsonl"
    evidence_path = output / "equity_preclose_candidate_evidence.jsonl"
    archive_path = output / "equity_preclose_qd_archive.json"
    report_path = output / "equity_preclose_inventory_dispersion_report.md"
    result_path = output / "equity_preclose_inventory_dispersion_result.json"
    elite_manifest = {
        "schema": "hydra_equity_preclose_frozen_elite_manifest_v1",
        "protocol": PROTOCOL,
        "selected_candidate_ids": elite_ids,
        "selected_count": len(elite_ids),
        "selection_period": "2023_ONLY",
        "selection_used_2024_results": False,
        "frozen_thresholds": {candidate_id: frozen_thresholds[candidate_id] for candidate_id in elite_ids},
        "maximum_elites": 4,
        "maximum_per_mechanism": 2,
        "maximum_per_decision_time": 2,
        "q4_access_count": 0,
    }
    elite_manifest["manifest_hash"] = _canonical_hash(elite_manifest)
    archive = {
        "schema": "hydra_equity_preclose_qd_archive_v1",
        "niches": [
            {
                "candidate_id": row["candidate_id"],
                "target": row["target_market"],
                "mechanism": row["mechanism"],
                "clock": row["decision_minute_chicago"],
                "quality": row["quality"],
                "selected": row["candidate_id"] in elite_ids,
            }
            for row in stage1
            if row["stage1_passed"]
        ],
        "pareto_dimensions": ["net_economics", "drawdown", "cost_resilience", "behavioral_novelty"],
    }
    _write_immutable(manifest_path, _json_text({"schema": "hydra_equity_preclose_population_v1", "structures": population}))
    _write_immutable(stage1_path, _jsonl_text(stage1))
    _write_immutable(elite_path, _json_text(elite_manifest))
    all_events = pd.concat(complete_events, ignore_index=True) if complete_events else pd.DataFrame()
    _write_immutable(events_path, _jsonl_text(_event_rows(all_events) if not all_events.empty else []))
    _write_immutable(evidence_path, _jsonl_text(candidates))
    _write_immutable(archive_path, _json_text(archive))
    artifacts = {
        "structural_manifest": _artifact(manifest_path),
        "stage1_screen": _artifact(stage1_path),
        "elite_manifest": _artifact(elite_path),
        "trade_ledger": _artifact(events_path),
        "candidate_evidence": _artifact(evidence_path),
        "qd_archive": _artifact(archive_path),
    }
    status_counts = dict(Counter(row["status"] for row in candidates))
    promising_count = int(status_counts.get("PROMISING_RESEARCH_CANDIDATE", 0))
    result: dict[str, Any] = {
        "schema": "hydra_equity_preclose_inventory_dispersion_result_v1",
        "protocol": PROTOCOL,
        "code_commit": str(code_commit),
        "source_role_epoch_result_hash": str(source_role_epoch_result_hash),
        "scientific_conclusion": (
            "PRECLOSE_PROMISING_CANDIDATES_REQUIRE_FROZEN_PROMOTION"
            if promising_count
            else "PRECLOSE_PRIMARY_INSUFFICIENT_PIVOT_MARKET_ECOLOGY"
        ),
        "candidate_count": 32,
        "structural_prototypes": 32,
        "stage1_survivors": int(sum(bool(row["stage1_passed"]) for row in stage1)),
        "frozen_elite_count": len(elite_ids),
        "promising_candidates": promising_count,
        "promising_candidate_count": promising_count,
        "status_counts": status_counts,
        "candidates": candidates,
        "data_provenance": data_provenance,
        "performance": {
            "bar_rows_after_contract_guards": int(len(bars)),
            "session_symbol_clock_rows": int(len(rows)),
            "cross_market_context_rows": int(len(contexts)),
            "structural_evaluations": 32,
            "2024_full_replays": len(elite_ids),
        },
        "artifacts": artifacts,
        "q4_access_count": 0,
        "network_requests": 0,
        "paid_data_requests": 0,
        "incremental_databento_spend_usd": 0.0,
        "broker_connections": 0,
        "outbound_orders": 0,
        "order_capability": False,
        "shadow_research_active": 0,
        "paper_shadow_ready": 0,
        "status_ceiling": "PROMISING_RESEARCH_CANDIDATE",
    }
    hash_payload = dict(result)
    hash_payload["artifacts"] = {key: value["sha256"] for key, value in sorted(artifacts.items())}
    result["result_hash"] = _canonical_hash(hash_payload)
    _write_immutable(report_path, _render_report(result))
    result["artifacts"]["report"] = _artifact(report_path)
    result_without_path = dict(result)
    _write_immutable(result_path, _json_text(result_without_path))
    result["artifacts"]["result"] = _artifact(result_path)
    return _strict_json_value(result)
