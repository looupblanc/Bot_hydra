"""Frozen mini/micro participation-divergence primary.

Exactly 96 outcome-neutral structures are screened on 2023.  At most eight are
frozen and replayed unchanged through 2024 Q1--Q3.  The module consumes cached
development bars only and cannot access Q4, a network, a broker or Shadow.
"""

from __future__ import annotations

import json
import math
import bisect
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.markets.instruments import instrument_spec
from hydra.research.equity_preclose_inventory_dispersion import (
    ALL_SYMBOLS,
    DEVELOPMENT_END_EXCLUSIVE,
    FOLDS,
    MAP_TYPE,
    MINI_TO_MICRO,
    _account_evidence,
    _artifact,
    _bh_family,
    _block_sign_flip_probability,
    _canonical_hash,
    _event_rows as _preclose_event_rows,
    _file_sha256,
    _json_text,
    _jsonl_text,
    _period_metrics,
    _read_governed_bars,
    _round_turn_cost,
    _strict_json_value,
    _verify_file,
    _write_immutable,
)
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


PROTOCOL = "mini_micro_participation_divergence_primary_v1"
PREREGISTRATION_SHA256 = "75b40efb442b928074af36c9809aad075d56c7b21502b9e9105c3860d51f1bac"
STATES = ("MICRO_DOMINANT", "MINI_DOMINANT")
POLICIES = ("CONTINUATION", "REVERSAL")
QUANTILES = (0.70, 0.85)
HORIZONS = (15, 30, 60)
DECISION_MINIMUM = 9 * 60
DECISION_MAXIMUM = 14 * 60


class MiniMicroParticipationError(RuntimeError):
    """A frozen-input or causal-integrity invariant failed."""


def structural_manifest() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mini, micro in MINI_TO_MICRO.items():
        for state in STATES:
            for policy in POLICIES:
                for quantile in QUANTILES:
                    for horizon in HORIZONS:
                        payload = {
                            "protocol": PROTOCOL,
                            "target_market": mini,
                            "execution_market": micro,
                            "participation_state": state,
                            "return_policy": policy,
                            "threshold_quantile": quantile,
                            "holding_minutes": horizon,
                            "target_pool": "COMBINE_PASSER_POOL",
                            "selection_period": "2023_ONLY",
                            "status_ceiling": "PROMISING_RESEARCH_CANDIDATE",
                        }
                        candidate_id = (
                            f"mini_micro_participation_{mini}_{state.lower()}_"
                            f"{policy.lower()}_q{int(quantile*100)}_h{horizon}_v1"
                        )
                        rows.append(
                            {
                                "candidate_id": candidate_id,
                                **payload,
                                "structural_fingerprint": _canonical_hash(payload),
                                "status_inherited": False,
                                "inherited_passes": [],
                            }
                        )
    if len(rows) != 96 or len({row["structural_fingerprint"] for row in rows}) != 96:
        raise MiniMicroParticipationError("Frozen population must contain 96 unique structures")
    return rows


def _contract_maturity(root: str, contract: str) -> str:
    value = str(contract)
    if not value.startswith(str(root)) or len(value) <= len(str(root)):
        raise MiniMicroParticipationError(f"Invalid explicit contract {contract} for {root}")
    return value[len(str(root)) :]


def resample_closed_five_minute_bars(bars: pd.DataFrame) -> pd.DataFrame:
    """Return only complete Chicago RTH 5m bars with close-time availability."""

    frame = bars.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="raise")
    local = frame["timestamp"].dt.tz_convert("America/Chicago")
    frame["event_session_id"] = local.dt.strftime("%Y-%m-%d")
    frame["local_minute"] = local.dt.hour * 60 + local.dt.minute
    frame = frame.loc[frame["local_minute"].between(8 * 60 + 30, 15 * 60 + 4)].copy()
    frame["bucket_start_minute"] = (
        8 * 60 + 30 + ((frame["local_minute"] - (8 * 60 + 30)) // 5) * 5
    )
    group_keys = ["event_session_id", "symbol", "bucket_start_minute"]
    ordered = frame.sort_values(group_keys + ["timestamp"], kind="mergesort")
    out = (
        ordered.groupby(group_keys, sort=True, observed=True)
        .agg(
            source_bar_start=("timestamp", "first"),
            final_source_start=("timestamp", "last"),
            first_local_minute=("local_minute", "min"),
            last_local_minute=("local_minute", "max"),
            active_contract=("active_contract", "first"),
            active_contract_count=("active_contract", "nunique"),
            open=("open", "first"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_row_count=("timestamp", "size"),
        )
        .reset_index()
    )
    complete = (
        out["source_row_count"].eq(5)
        & out["first_local_minute"].eq(out["bucket_start_minute"])
        & out["last_local_minute"].eq(out["bucket_start_minute"] + 4)
        & out["active_contract_count"].eq(1)
    )
    out = out.loc[complete].copy()
    out["decision_minute"] = out["bucket_start_minute"] + 5
    out["source_bar_close"] = out["final_source_start"] + pd.Timedelta(minutes=1)
    out["availability_timestamp"] = out["source_bar_close"]
    out["return_5m"] = out["close"] / out["open"] - 1.0
    out = out.drop(
        columns=[
            "final_source_start",
            "first_local_minute",
            "last_local_minute",
            "active_contract_count",
        ]
    )
    if out.empty:
        raise MiniMicroParticipationError("No complete 5m bars")
    return out.sort_values(
        ["symbol", "bucket_start_minute", "event_session_id"], kind="mergesort"
    ).reset_index(drop=True)


def build_participation_contexts(bars: pd.DataFrame) -> pd.DataFrame:
    five = resample_closed_five_minute_bars(bars)
    grouped = five.groupby(["symbol", "bucket_start_minute"], sort=False)
    five["past_same_phase_volume_median"] = grouped["volume"].transform(
        lambda values: values.shift(1).rolling(20, min_periods=20).median()
    )
    five["past_same_phase_return_volatility"] = grouped["return_5m"].transform(
        lambda values: values.shift(1).rolling(20, min_periods=20).std(ddof=1)
    )
    five["normalized_participation"] = (
        five["volume"] / five["past_same_phase_volume_median"]
    )
    minute = bars.copy()
    minute["timestamp"] = pd.to_datetime(minute["timestamp"], utc=True)
    local = minute["timestamp"].dt.tz_convert("America/Chicago")
    minute["event_session_id"] = local.dt.strftime("%Y-%m-%d")
    minute["local_minute"] = local.dt.hour * 60 + local.dt.minute
    minute_lookup: dict[tuple[str, str, int], Any] = {
        (str(row.event_session_id), str(row.symbol), int(row.local_minute)): row
        for row in minute.itertuples(index=False)
    }

    contexts: list[dict[str, Any]] = []
    lookup = {
        (str(row.event_session_id), str(row.symbol), int(row.bucket_start_minute)): row
        for row in five.itertuples(index=False)
    }
    sessions_and_buckets = sorted(
        {(str(row.event_session_id), int(row.bucket_start_minute)) for row in five.itertuples()}
    )
    for session, bucket in sessions_and_buckets:
        decision = bucket + 5
        if decision < DECISION_MINIMUM or decision > DECISION_MAXIMUM:
            continue
        for mini, micro in MINI_TO_MICRO.items():
            mini_row = lookup.get((session, mini, bucket))
            micro_row = lookup.get((session, micro, bucket))
            if mini_row is None or micro_row is None:
                continue
            if pd.Timestamp(mini_row.source_bar_close) != pd.Timestamp(micro_row.source_bar_close):
                raise MiniMicroParticipationError("Mini/micro completed bars are not synchronized")
            if _contract_maturity(mini, mini_row.active_contract) != _contract_maturity(
                micro, micro_row.active_contract
            ):
                continue
            if not np.isfinite(mini_row.normalized_participation) or not np.isfinite(
                micro_row.normalized_participation
            ):
                continue
            if not np.isfinite(mini_row.past_same_phase_return_volatility) or not np.isfinite(
                micro_row.past_same_phase_return_volatility
            ):
                continue
            entry = minute_lookup.get((session, micro, decision))
            delay1 = minute_lookup.get((session, micro, decision + 1))
            delay5 = minute_lookup.get((session, micro, decision + 5))
            if any(value is None for value in (entry, delay1, delay5)):
                continue
            if any(
                str(value.active_contract) != str(micro_row.active_contract)
                for value in (entry, delay1, delay5)
            ):
                continue
            row: dict[str, Any] = {
                "event_session_id": session,
                "target_market": mini,
                "execution_market": micro,
                "decision_minute": decision,
                "decision_timestamp": pd.Timestamp(entry.timestamp),
                "source_bar_start": pd.Timestamp(mini_row.source_bar_start),
                "source_bar_close": pd.Timestamp(mini_row.source_bar_close),
                "availability_timestamp": pd.Timestamp(mini_row.availability_timestamp),
                "signal_contract": str(mini_row.active_contract),
                "execution_contract": str(micro_row.active_contract),
                "mini_return_5m": float(mini_row.return_5m),
                "micro_return_5m": float(micro_row.return_5m),
                "mini_normalized_participation": float(mini_row.normalized_participation),
                "micro_normalized_participation": float(micro_row.normalized_participation),
                "participation_divergence": float(
                    np.log1p(micro_row.normalized_participation)
                    - np.log1p(mini_row.normalized_participation)
                ),
                "prior_pair_volatility": float(
                    math.sqrt(
                        float(mini_row.past_same_phase_return_volatility) ** 2
                        + float(micro_row.past_same_phase_return_volatility) ** 2
                    )
                    / math.sqrt(2.0)
                ),
                "sign_agreement": bool(
                    np.sign(mini_row.return_5m) != 0
                    and np.sign(mini_row.return_5m) == np.sign(micro_row.return_5m)
                ),
                "entry_price": float(entry.open),
                "delay_1m_entry_price": float(delay1.open),
                "delay_5m_entry_price": float(delay5.open),
            }
            for horizon in HORIZONS:
                exit_minute = min(decision + horizon - 1, 15 * 60 + 4)
                exit_row = minute_lookup.get((session, micro, exit_minute))
                if exit_row is None or str(exit_row.active_contract) != str(micro_row.active_contract):
                    row[f"exit_price_{horizon}"] = np.nan
                    row[f"path_low_{horizon}"] = np.nan
                    row[f"path_high_{horizon}"] = np.nan
                    row[f"exit_timestamp_{horizon}"] = pd.NaT
                    continue
                path_rows = [
                    minute_lookup.get((session, micro, value))
                    for value in range(decision, exit_minute + 1)
                ]
                if any(value is None for value in path_rows) or any(
                    str(value.active_contract) != str(micro_row.active_contract)
                    for value in path_rows
                    if value is not None
                ):
                    row[f"exit_price_{horizon}"] = np.nan
                    row[f"path_low_{horizon}"] = np.nan
                    row[f"path_high_{horizon}"] = np.nan
                    row[f"exit_timestamp_{horizon}"] = pd.NaT
                    continue
                row[f"exit_price_{horizon}"] = float(exit_row.close)
                row[f"path_low_{horizon}"] = float(min(value.low for value in path_rows))
                row[f"path_high_{horizon}"] = float(max(value.high for value in path_rows))
                row[f"exit_timestamp_{horizon}"] = pd.Timestamp(exit_row.timestamp) + pd.Timedelta(minutes=1)
            contexts.append(row)
    out = pd.DataFrame(contexts)
    if out.empty:
        raise MiniMicroParticipationError("No causal mini/micro contexts")
    if (out["source_bar_close"] != out["decision_timestamp"]).any() or (
        out["availability_timestamp"] > out["decision_timestamp"]
    ).any():
        raise MiniMicroParticipationError("Incomplete 5m bar or future availability")
    return out.sort_values(
        ["target_market", "event_session_id", "decision_minute"], kind="mergesort"
    ).reset_index(drop=True)


def _score_rows(contexts: pd.DataFrame, specification: Mapping[str, Any]) -> pd.DataFrame:
    selected = contexts.loc[
        contexts["target_market"].eq(str(specification["target_market"]))
        & contexts["sign_agreement"].astype(bool)
    ].copy()
    state = str(specification["participation_state"])
    selected["score"] = selected["participation_divergence"].abs()
    selected["state_eligible"] = (
        selected["participation_divergence"].gt(0.0)
        if state == "MICRO_DOMINANT"
        else selected["participation_divergence"].lt(0.0)
    )
    direction = np.sign(selected["mini_return_5m"].to_numpy(dtype=float))
    if str(specification["return_policy"]) == "REVERSAL":
        direction *= -1.0
    selected["side"] = direction
    return selected.loc[selected["side"].ne(0.0)].sort_values(
        ["event_session_id", "decision_minute"], kind="mergesort"
    ).reset_index(drop=True)


def apply_phase_causal_thresholds(
    scored: pd.DataFrame, quantile: float
) -> tuple[pd.DataFrame, dict[str, float]]:
    frame = scored.copy().sort_values(
        ["event_session_id", "decision_minute"], kind="mergesort"
    ).reset_index(drop=True)
    sessions = frame["event_session_id"].astype(str).to_numpy()
    scores = frame["score"].to_numpy(dtype=float)
    is_fit = sessions < "2024-01-01"
    thresholds = np.full(len(frame), np.nan, dtype=float)
    cutoffs = np.full(len(frame), None, dtype=object)
    frozen: dict[str, float] = {}
    for phase, positions in frame.groupby("decision_minute", sort=True).groups.items():
        index = np.asarray(list(positions), dtype=int)
        fit_index = index[is_fit[index]]
        fit_values = scores[fit_index]
        ordered: list[float] = []
        previous_session: str | None = None
        for position, value in zip(fit_index, fit_values, strict=True):
            if len(ordered) >= 20:
                rank = (len(ordered) - 1) * float(quantile)
                lower = int(math.floor(rank))
                upper = int(math.ceil(rank))
                fraction = rank - lower
                thresholds[position] = float(
                    ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction
                )
            cutoffs[position] = previous_session
            bisect.insort(ordered, float(value))
            previous_session = str(sessions[position])
        if len(ordered) >= 20:
            rank = (len(ordered) - 1) * float(quantile)
            lower = int(math.floor(rank))
            upper = int(math.ceil(rank))
            fraction = rank - lower
            threshold = float(
                ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction
            )
            frozen[str(int(phase))] = threshold
            validation_index = index[~is_fit[index]]
            thresholds[validation_index] = threshold
            cutoffs[validation_index] = "2023-12-31"
    frame["threshold"] = thresholds
    frame["threshold_fit_cutoff"] = cutoffs
    event = frame["state_eligible"].astype(bool) & frame["threshold"].notna() & frame["score"].ge(
        frame["threshold"]
    )
    # A causal priority rule, fixed before outcomes, permits one event per session.
    selected = frame.loc[event].sort_values(["event_session_id", "decision_minute"])
    selected = selected.drop_duplicates("event_session_id", keep="first")
    return selected.reset_index(drop=True), frozen


def _materialize_events(selected: pd.DataFrame, specification: Mapping[str, Any]) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame()
    horizon = int(specification["holding_minutes"])
    frame = selected.dropna(
        subset=[f"exit_price_{horizon}", f"path_low_{horizon}", f"path_high_{horizon}"]
    ).copy()
    if frame.empty:
        return frame
    micro = str(specification["execution_market"])
    point = instrument_spec(micro).point_value
    side = frame["side"].astype(float)
    frame["exit_price"] = frame[f"exit_price_{horizon}"]
    frame["path_low"] = frame[f"path_low_{horizon}"]
    frame["path_high"] = frame[f"path_high_{horizon}"]
    frame["exit_timestamp"] = frame[f"exit_timestamp_{horizon}"]
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
    frame["cost"] = _round_turn_cost(micro)
    frame["mae_dollars"] = np.minimum(adverse, 0.0) - 0.5 * frame["cost"]
    frame["net_pnl"] = frame["gross_pnl"] - frame["cost"]
    frame["net_pnl_1_5x"] = frame["gross_pnl"] - 1.5 * frame["cost"]
    frame["net_pnl_2x"] = frame["gross_pnl"] - 2.0 * frame["cost"]
    frame["delay_1m_net_pnl"] = frame["delay_1m_gross_pnl"] - frame["cost"]
    frame["delay_5m_net_pnl"] = frame["delay_5m_gross_pnl"] - frame["cost"]
    candidate_id = str(specification["candidate_id"])
    frame["candidate_id"] = candidate_id
    frame["strategy_id"] = candidate_id
    frame["trade_id"] = [f"{candidate_id}:{day}" for day in frame["event_session_id"]]
    frame["contracts"] = 1
    frame["underlying"] = str(specification["target_market"])
    frame["entry_timestamp"] = frame["decision_timestamp"]
    frame["role"] = "alpha"
    frame["target_pool"] = "COMBINE_PASSER_POOL"
    frame["status_inherited"] = False
    return frame.sort_values("entry_timestamp").reset_index(drop=True)


def _behavior_fingerprint(events: pd.DataFrame) -> str:
    return _canonical_hash(
        [[str(row.event_session_id), int(row.decision_minute), float(row.side)] for row in events.itertuples()]
    )


def select_frozen_elites(stage1: list[dict[str, Any]], maximum: int = 8) -> list[dict[str, Any]]:
    ordered = sorted(
        [row for row in stage1 if row.get("stage1_passed")],
        key=lambda row: (-float(row["quality"]), str(row["candidate_id"])),
    )
    selected: list[dict[str, Any]] = []
    market_count: Counter[str] = Counter()
    behaviors: set[str] = set()
    for row in ordered:
        market = str(row["target_market"])
        behavior = str(row["behavior_fingerprint"])
        if market_count[market] >= 2 or behavior in behaviors:
            continue
        selected.append(row)
        market_count[market] += 1
        behaviors.add(behavior)
        if len(selected) == maximum:
            break
    return selected


def _period_label(values: pd.Series) -> pd.Series:
    text = values.astype(str)
    return pd.Series(
        np.select(
            [
                text.lt("2024-01-01"),
                text.lt("2024-04-01"),
                text.lt("2024-07-01"),
                text.lt("2024-10-01"),
            ],
            ["2023", "2024_q1", "2024_q2", "2024_q3"],
            default="PROTECTED_OR_INVALID",
        ),
        index=values.index,
    )


def _add_matching_cells(frame: pd.DataFrame) -> pd.DataFrame:
    """Add period/phase/past-volatility cells fitted from 2023 covariates only."""

    out = frame.copy()
    out["matching_period"] = _period_label(out["event_session_id"])
    out["volatility_band"] = 0
    for phase, positions in out.groupby("decision_minute", sort=True).groups.items():
        index = list(positions)
        fit = out.loc[
            index,
            ["event_session_id", "prior_pair_volatility"],
        ]
        fit_values = fit.loc[
            fit["event_session_id"].astype(str).lt("2024-01-01"),
            "prior_pair_volatility",
        ].dropna()
        if fit_values.empty:
            continue
        boundaries = np.unique(fit_values.quantile([0.25, 0.50, 0.75]).to_numpy(dtype=float))
        out.loc[index, "volatility_band"] = np.searchsorted(
            boundaries,
            out.loc[index, "prior_pair_volatility"].to_numpy(dtype=float),
            side="right",
        )
    return out


def _prepare_shift_plan(
    contexts: pd.DataFrame,
) -> tuple[pd.DataFrame, list[np.ndarray]]:
    base = _add_matching_cells(contexts.copy().reset_index(drop=True))
    groups: list[np.ndarray] = []
    keys = ["target_market", "decision_minute", "matching_period", "volatility_band"]
    for _key, positions in base.groupby(keys, sort=True).groups.items():
        index = np.asarray(list(positions), dtype=int)
        ordered = index[
            np.argsort(base.loc[index, "event_session_id"].astype(str).to_numpy())
        ]
        groups.append(ordered)
    return base, groups


def _shift_micro_participation(
    contexts: pd.DataFrame | None,
    *,
    rng: np.random.Generator,
    prepared: tuple[pd.DataFrame, list[np.ndarray]] | None = None,
) -> pd.DataFrame:
    """Circularly shift micro participation inside market/phase/period/vol cells."""

    if prepared is None:
        if contexts is None:
            raise MiniMicroParticipationError("A context frame or prepared shift plan is required")
        prepared = _prepare_shift_plan(contexts)
    base, groups = prepared
    shifted = base.copy()
    values = shifted["micro_normalized_participation"].to_numpy(dtype=float).copy()
    for ordered in groups:
        if len(ordered) < 2:
            continue
        offset = int(rng.integers(1, len(ordered)))
        values[ordered] = np.roll(values[ordered], offset)
    shifted["micro_normalized_participation"] = values
    shifted["participation_divergence"] = (
        np.log1p(shifted["micro_normalized_participation"])
        - np.log1p(shifted["mini_normalized_participation"])
    )
    return shifted.drop(columns=["matching_period", "volatility_band"])


def _session_shift_probability(
    events: pd.DataFrame,
    contexts: pd.DataFrame,
    specification: Mapping[str, Any],
    *,
    seed: int,
    draws: int = 127,
) -> float:
    """Rerun the exact causal event selector after participant-flow shifts."""

    if len(events) < 10:
        return 1.0
    observed = float(events["net_pnl"].sum())
    target_contexts = contexts.loc[
        contexts["target_market"].eq(str(specification["target_market"]))
    ].copy()
    shift_plan = _prepare_shift_plan(target_contexts)
    rng = np.random.default_rng(seed)
    exceed = 0
    for _draw in range(draws):
        shifted = _shift_micro_participation(None, rng=rng, prepared=shift_plan)
        scored = _score_rows(shifted, specification)
        selected, _thresholds = apply_phase_causal_thresholds(
            scored, float(specification["threshold_quantile"])
        )
        null_events = _materialize_events(selected, specification)
        null_net = float(null_events["net_pnl"].sum()) if not null_events.empty else 0.0
        exceed += int(null_net >= observed)
    return float((1 + exceed) / (draws + 1))


def _matched_opportunity_probability(
    events: pd.DataFrame,
    contexts: pd.DataFrame,
    specification: Mapping[str, Any],
    *,
    seed: int,
    draws: int = 511,
) -> float:
    """Count-match target, period, phase and prior-volatility opportunities."""

    if len(events) < 10:
        return 1.0
    pool = _add_matching_cells(_score_rows(contexts, specification))
    pool = pool.loc[pool["state_eligible"].astype(bool)].copy()
    horizon = int(specification["holding_minutes"])
    pool = pool.dropna(subset=[f"exit_price_{horizon}"])
    point = instrument_spec(str(specification["execution_market"])).point_value
    pool["null_net"] = (
        (pool[f"exit_price_{horizon}"] - pool["entry_price"]) * pool["side"] * point
        - _round_turn_cost(str(specification["execution_market"]))
    )
    event_keys = events[["event_session_id", "decision_minute"]].merge(
        pool[
            ["event_session_id", "decision_minute", "matching_period", "volatility_band"]
        ].drop_duplicates(["event_session_id", "decision_minute"]),
        on=["event_session_id", "decision_minute"],
        how="left",
    )
    if event_keys[["matching_period", "volatility_band"]].isna().any().any():
        return 1.0
    cell_keys = ["matching_period", "decision_minute", "volatility_band"]
    counts = event_keys.groupby(cell_keys, sort=True).size().to_dict()
    pools = {
        key: np.asarray(list(positions), dtype=int)
        for key, positions in pool.groupby(cell_keys, sort=True).groups.items()
    }
    if any(key not in pools for key in counts):
        return 1.0
    rng = np.random.default_rng(seed)
    observed = float(events["net_pnl"].sum())
    exceed = 0
    for _draw in range(draws):
        used_sessions: set[str] = set()
        total = 0.0
        valid = True
        for key, count in sorted(counts.items()):
            candidates = pools[key].copy()
            rng.shuffle(candidates)
            chosen: list[int] = []
            for position in candidates:
                session = str(pool.loc[position, "event_session_id"])
                if session in used_sessions:
                    continue
                chosen.append(int(position))
                used_sessions.add(session)
                if len(chosen) == int(count):
                    break
            if len(chosen) != int(count):
                valid = False
                break
            total += float(pool.loc[chosen, "null_net"].sum())
        if not valid:
            return 1.0
        exceed += int(total >= observed)
    return float((1 + exceed) / (draws + 1))


def _selector_shift_max_probability(
    contexts: pd.DataFrame,
    population: list[dict[str, Any]],
    observed_best_quality: float,
    *,
    seed: int,
    draws: int = 31,
) -> float:
    """Rerun the 96-member 2023 selector under participant-flow shifts."""

    structural_groups: dict[tuple[str, str, float], list[dict[str, Any]]] = {}
    for specification in population:
        key = (
            str(specification["target_market"]),
            str(specification["participation_state"]),
            float(specification["threshold_quantile"]),
        )
        structural_groups.setdefault(key, []).append(specification)
    rng = np.random.default_rng(seed)
    shift_plan = _prepare_shift_plan(contexts)
    exceed = 0
    for _draw in range(draws):
        shifted = _shift_micro_participation(None, rng=rng, prepared=shift_plan)
        maximum = -math.inf
        for specifications in structural_groups.values():
            representative = specifications[0]
            scored = _score_rows(shifted, representative)
            selected, _thresholds = apply_phase_causal_thresholds(
                scored, float(representative["threshold_quantile"])
            )
            for specification in specifications:
                policy_selected = selected.copy()
                side = np.sign(policy_selected["mini_return_5m"].to_numpy(dtype=float))
                if str(specification["return_policy"]) == "REVERSAL":
                    side *= -1.0
                policy_selected["side"] = side
                events = _materialize_events(policy_selected, specification)
                discovery = events.loc[
                    events["event_session_id"].astype(str).lt("2024-01-01")
                ] if not events.empty else events
                metrics = _period_metrics(discovery)
                if (
                    metrics["events"] >= 35
                    and metrics["net_pnl_1_5x"] > 0.0
                    and metrics["best_trade_removed_net"]
                    >= -0.5 * max(metrics["net_pnl"], 0.0)
                    and metrics["best_day_removed_net"]
                    >= -0.5 * max(metrics["net_pnl"], 0.0)
                    and metrics["best_month_removed_net"]
                    >= -0.5 * max(metrics["net_pnl"], 0.0)
                ):
                    maximum = max(
                        maximum,
                        float(metrics["net_pnl_1_5x"] / (metrics["maximum_drawdown"] + 1.0)),
                    )
        exceed += int(maximum >= observed_best_quality)
    return float((1 + exceed) / (draws + 1))


def _baseline_evidence(
    contexts: pd.DataFrame,
    events: pd.DataFrame,
    specification: Mapping[str, Any],
    candidate_mean: float,
) -> dict[str, Any]:
    """Compare count/phase/period matched causal volume-only selectors."""

    pool = _score_rows(contexts, specification)
    pool = pool.loc[pool["state_eligible"].astype(bool)].copy()
    pool["matching_period"] = _period_label(pool["event_session_id"])
    event_counts = events.assign(
        matching_period=_period_label(events["event_session_id"])
    ).groupby(["matching_period", "decision_minute"], sort=True).size().to_dict()
    horizon = int(specification["holding_minutes"])
    point = instrument_spec(str(specification["execution_market"])).point_value
    pool["baseline_net"] = (
        (pool[f"exit_price_{horizon}"] - pool["entry_price"]) * pool["side"] * point
        - _round_turn_cost(str(specification["execution_market"]))
    )
    scores = {
        "mini_volume_only_mean_net": pool["mini_normalized_participation"].abs(),
        "total_volume_mean_net": (
            pool["mini_normalized_participation"] + pool["micro_normalized_participation"]
        ).abs(),
    }
    means: dict[str, Any] = {}
    for label, score in scores.items():
        candidate_pool = pool.assign(baseline_score=score)
        used_sessions: set[str] = set()
        chosen: list[int] = []
        for key, count in sorted(event_counts.items()):
            period, phase = key
            eligible = candidate_pool.loc[
                candidate_pool["matching_period"].eq(period)
                & candidate_pool["decision_minute"].eq(int(phase))
            ].sort_values(["baseline_score", "event_session_id"], ascending=[False, True])
            added = 0
            for position, row in eligible.iterrows():
                session = str(row["event_session_id"])
                if session in used_sessions:
                    continue
                chosen.append(int(position))
                used_sessions.add(session)
                added += 1
                if added == int(count):
                    break
        means[label] = (
            float(candidate_pool.loc[chosen, "baseline_net"].mean()) if chosen else 0.0
        )
    means["matching"] = "same_market_period_phase_event_count_and_direction_policy"
    means["incremental_information_passed"] = bool(
        candidate_mean
        > max(
            float(means["mini_volume_only_mean_net"]),
            float(means["total_volume_mean_net"]),
        )
    )
    return means


def _event_rows(events: pd.DataFrame) -> list[dict[str, Any]]:
    if events.empty:
        return []
    extra = [
        "mini_return_5m", "micro_return_5m", "mini_normalized_participation",
        "micro_normalized_participation", "participation_divergence", "decision_minute",
    ]
    base = _preclose_event_rows(events.assign(
        residual_displacement=events["participation_divergence"],
        target_displacement=events["mini_return_5m"],
        common_displacement=events["micro_return_5m"],
        dispersion=events["participation_divergence"].abs(),
        breadth=events["sign_agreement"].astype(float),
        target_efficiency=0.0,
        target_participation=events["mini_normalized_participation"],
        common_factor_weight_hash="NOT_APPLICABLE_MINI_MICRO_PAIR",
    ))
    by_trade = events.set_index("trade_id")
    for row in base:
        source = by_trade.loc[row["trade_id"]]
        for key in extra:
            row[key] = source[key]
    return base


def _render_report(result: Mapping[str, Any]) -> str:
    lines = [
        "# HYDRA mini/micro participation-divergence primary",
        "",
        f"- Frozen structures: {result['structural_prototypes']}",
        f"- 2023 survivors: {result['stage1_survivors']}",
        f"- Frozen elites: {result['frozen_elite_count']}",
        f"- Promising candidates: {result['promising_candidates']}",
        "- Role: alpha / COMBINE_PASSER_POOL; XFA and defensive optimality are separate",
        "- Q4, network, spend, broker, orders, Shadow and Paper: 0",
        "",
    ]
    for row in result["candidates"]:
        lines.append(
            f"- `{row['candidate_id']}`: `{row['status']}`, pooled 1.5x "
            f"{row['pooled_metrics']['net_pnl_1_5x']:.2f}, adjusted p "
            f"{row['null_evidence']['family_adjusted_probability']:.6f}"
        )
    lines.extend(["", "No result is a holdout, Paper or funded-readiness claim.", ""])
    return "\n".join(lines)


def run_mini_micro_participation_divergence(
    output_dir: str | Path,
    *,
    engineering_task_path: Path,
    engineering_task_sha256: str,
    core_data_paths: list[Path],
    core_data_sha256s: list[str],
    roll_map_path: Path,
    roll_map_sha256: str,
    roll_map_hash: str,
    source_preclose_result_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    if str(engineering_task_sha256) != PREREGISTRATION_SHA256:
        raise MiniMicroParticipationError("Unexpected preregistration hash")
    _verify_file(Path(engineering_task_path), engineering_task_sha256, "engineering task")
    if len(core_data_paths) != 5 or len(core_data_sha256s) != 5:
        raise MiniMicroParticipationError("Exactly five frozen sources are required")
    for index, (path, digest) in enumerate(zip(core_data_paths, core_data_sha256s, strict=True), 1):
        _verify_file(Path(path), digest, f"core data {index}")
    _verify_file(Path(roll_map_path), roll_map_sha256, "roll map")
    if len(str(source_preclose_result_hash)) != 64 or not str(code_commit).strip():
        raise MiniMicroParticipationError("Frozen predecessor hash and code commit are required")
    roll_map = load_roll_map(roll_map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != str(roll_map_hash):
        raise MiniMicroParticipationError("Roll-map semantic contract changed")

    population = structural_manifest()
    candidate_ids = [row["candidate_id"] for row in population]
    if record_data_access:
        enforce_data_access(
            "2023-01-01:2024-10-01",
            DataRole.CONTAMINATED_DEVELOPMENT,
            "hydra.research.mini_micro_participation_divergence",
            candidate_ids,
            "2023-only selection and unchanged 2024 Q1-Q3 mini/micro replay",
            None,
        )
    bars, contract_details = _read_governed_bars([Path(path) for path in core_data_paths], roll_map)
    contexts = build_participation_contexts(bars)
    provenance = {
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
    provenance["data_fingerprint"] = _canonical_hash(provenance)

    stage1: list[dict[str, Any]] = []
    event_cache: dict[str, pd.DataFrame] = {}
    for specification in population:
        scored = _score_rows(contexts, specification)
        selected, thresholds = apply_phase_causal_thresholds(
            scored, float(specification["threshold_quantile"])
        )
        events = _materialize_events(selected, specification)
        discovery = events.loc[events["event_session_id"].astype(str).lt("2024-01-01")].copy()
        metrics = _period_metrics(discovery)
        account = _account_evidence(discovery, str(specification["candidate_id"]))
        passed = bool(
            metrics["events"] >= 35
            and metrics["net_pnl_1_5x"] > 0.0
            and metrics["best_trade_removed_net"] >= -0.5 * max(metrics["net_pnl"], 0.0)
            and metrics["best_day_removed_net"] >= -0.5 * max(metrics["net_pnl"], 0.0)
            and metrics["best_month_removed_net"] >= -0.5 * max(metrics["net_pnl"], 0.0)
            and bool((account.get("one_micro") or {}).get("mll_breached") is False)
        )
        stage1.append(
            {
                **specification,
                "stage1_metrics": metrics,
                "stage1_passed": passed,
                "quality": float(metrics["net_pnl_1_5x"] / (metrics["maximum_drawdown"] + 1.0)),
                "behavior_fingerprint": _behavior_fingerprint(discovery),
                "frozen_2023_phase_thresholds": thresholds,
                "selection_used_2024_results": False,
                "account_evidence": account,
            }
        )
        event_cache[str(specification["candidate_id"])] = events

    elites = select_frozen_elites(stage1)
    elite_ids = [str(row["candidate_id"]) for row in elites]
    raw: dict[str, float] = {}
    sign_flip: dict[str, float] = {}
    shifted: dict[str, float] = {}
    matched: dict[str, float] = {}
    observed_best_quality = max(
        (float(row["quality"]) for row in elites), default=math.inf
    )
    selector_shift_probability = (
        _selector_shift_max_probability(
            contexts,
            population,
            observed_best_quality,
            seed=91500,
        )
        if elites
        else 1.0
    )
    selector_null_decisive_rejection = bool(selector_shift_probability > 0.10)
    for index, elite in enumerate(elites):
        candidate_id = str(elite["candidate_id"])
        events = event_cache[candidate_id]
        sign_flip[candidate_id] = _block_sign_flip_probability(events, 92000 + index)
        if selector_null_decisive_rejection:
            # Successive halving: these expensive diagnostics cannot reverse a
            # mandatory family-level rejection.  A conservative probability of
            # one prevents promotion and records the explicit information stop.
            shifted[candidate_id] = 1.0
            matched[candidate_id] = 1.0
        else:
            shifted[candidate_id] = _session_shift_probability(
                events, contexts, elite, seed=93000 + index
            )
            matched[candidate_id] = _matched_opportunity_probability(
                events, contexts, elite, seed=94000 + index
            )
        raw[candidate_id] = max(
            sign_flip[candidate_id],
            shifted[candidate_id],
            matched[candidate_id],
            selector_shift_probability,
        )
    adjusted = _bh_family(raw, elite_ids) if elite_ids else {}
    candidates: list[dict[str, Any]] = []
    complete_events: list[pd.DataFrame] = []
    for elite in elites:
        candidate_id = str(elite["candidate_id"])
        events = event_cache[candidate_id]
        complete_events.append(events)
        folds = {
            name: _period_metrics(
                events.loc[
                    events["event_session_id"].astype(str).ge(start)
                    & events["event_session_id"].astype(str).lt(end)
                ]
            )
            for name, (start, end) in FOLDS.items()
        }
        pooled = _period_metrics(events)
        supportive = sum(value["net_pnl_1_5x"] > 0 for value in folds.values())
        positive = sum(max(float(value["net_pnl_1_5x"]), 0.0) for value in folds.values())
        catastrophic = any(float(value["net_pnl_1_5x"]) < -0.5 * max(positive, 1.0) for value in folds.values())
        candidate_mean = pooled["net_pnl"] / max(pooled["events"], 1)
        baselines = _baseline_evidence(contexts, events, elite, candidate_mean)
        account = _account_evidence(events, candidate_id)
        removal_floor = -0.5 * max(float(pooled["net_pnl"]), 0.0)
        removal_pass = all(
            float(pooled[key]) >= removal_floor
            for key in ("best_trade_removed_net", "best_day_removed_net", "best_month_removed_net")
        )
        delay_pass = bool(pooled["delay_1m_net_pnl"] > 0 and pooled["delay_5m_net_pnl"] > 0)
        promising = bool(
            pooled["net_pnl_1_5x"] > 0
            and supportive >= 2
            and not catastrophic
            and adjusted[candidate_id] <= 0.10
            and removal_pass
            and delay_pass
            and bool(baselines["incremental_information_passed"])
            and not bool((account.get("one_micro") or {}).get("mll_breached", True))
        )
        uncertainty: list[str] = []
        if pooled["net_pnl_1_5x"] <= 0:
            uncertainty.append("POOLED_COST_STRESS_NONPOSITIVE")
        if supportive < 2 or catastrophic:
            uncertainty.append("TEMPORAL_TRANSFER_INSUFFICIENT_OR_CATASTROPHIC")
        if adjusted[candidate_id] > 0.10:
            uncertainty.append("MANDATORY_NULL_NOT_DISCRIMINATIVE")
        if not removal_pass:
            uncertainty.append("CONCENTRATION")
        if not delay_pass:
            uncertainty.append("DELAY_FRAGILITY")
        if not baselines["incremental_information_passed"]:
            uncertainty.append("NO_INCREMENTAL_PARTICIPATION_INFORMATION")
        candidates.append(
            {
                "candidate_id": candidate_id,
                "status": "PROMISING_RESEARCH_CANDIDATE" if promising else "INSUFFICIENT_EVIDENCE",
                "status_inherited": False,
                "inherited_passes": [],
                "role": "alpha",
                "target_pool": "COMBINE_PASSER_POOL",
                "primary_market": elite["target_market"],
                "execution_market": elite["execution_market"],
                "mechanism_family": "MINI_MICRO_PARTICIPATION_DIVERGENCE",
                "structural_fingerprint": elite["structural_fingerprint"],
                "behavior_fingerprint": elite["behavior_fingerprint"],
                "selection_used_2024_results": False,
                "folds": folds,
                "pooled_metrics": pooled,
                "supportive_temporal_folds": supportive,
                "catastrophic_transfer": catastrophic,
                "null_evidence": {
                    "block_sign_flip_probability": sign_flip[candidate_id],
                    "session_shift_probability": shifted[candidate_id],
                    "matched_opportunity_probability": matched[candidate_id],
                    "selector_96_to_8_shift_max_probability": selector_shift_probability,
                    "raw_mandatory_probability": raw[candidate_id],
                    "family_adjusted_probability": adjusted[candidate_id],
                    "validation_family_size": len(elite_ids),
                    "selector_rerun_completed": True,
                    "candidate_shift_status": (
                        "NOT_RUN_AFTER_DECISIVE_SELECTOR_MAX_STAT_REJECTION"
                        if selector_null_decisive_rejection
                        else "COMPLETED"
                    ),
                    "matched_opportunity_status": (
                        "NOT_RUN_AFTER_DECISIVE_SELECTOR_MAX_STAT_REJECTION"
                        if selector_null_decisive_rejection
                        else "COMPLETED"
                    ),
                },
                "baseline_evidence": baselines,
                "account_evidence": account,
                "topstep": {
                    "path_candidate": bool(account.get("path_candidate")),
                    "target_pool": "COMBINE_PASSER_POOL",
                    "one_micro": account.get("one_micro"),
                    "ten_micro": account.get("ten_micro"),
                },
                "uncertainty": uncertainty,
                "q4_access_count": 0,
                "shadow_research_active": 0,
                "paper_shadow_ready": 0,
                "order_capability": False,
            }
        )

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "structural_manifest": output / "mini_micro_structural_manifest.json",
        "stage1_screen": output / "mini_micro_2023_screen.jsonl",
        "elite_manifest": output / "mini_micro_frozen_elite_manifest.json",
        "trade_ledger": output / "mini_micro_trade_ledger.jsonl",
        "candidate_evidence": output / "mini_micro_candidate_evidence.jsonl",
        "qd_archive": output / "mini_micro_qd_archive.json",
    }
    elite_manifest = {
        "schema": "hydra_mini_micro_elite_manifest_v1",
        "selected_candidate_ids": elite_ids,
        "selected_count": len(elite_ids),
        "selection_period": "2023_ONLY",
        "selection_used_2024_results": False,
        "maximum_elites": 8,
        "maximum_per_target_market": 2,
        "q4_access_count": 0,
    }
    elite_manifest["manifest_hash"] = _canonical_hash(elite_manifest)
    archive = {
        "schema": "hydra_mini_micro_qd_archive_v1",
        "niches": [
            {
                "candidate_id": row["candidate_id"],
                "target": row["target_market"],
                "state": row["participation_state"],
                "policy": row["return_policy"],
                "horizon": row["holding_minutes"],
                "quality": row["quality"],
                "selected": row["candidate_id"] in elite_ids,
            }
            for row in stage1
            if row["stage1_passed"]
        ],
    }
    _write_immutable(paths["structural_manifest"], _json_text({"schema": "hydra_mini_micro_population_v1", "structures": population}))
    _write_immutable(paths["stage1_screen"], _jsonl_text(stage1))
    _write_immutable(paths["elite_manifest"], _json_text(elite_manifest))
    complete = pd.concat(complete_events, ignore_index=True) if complete_events else pd.DataFrame()
    _write_immutable(paths["trade_ledger"], _jsonl_text(_event_rows(complete)))
    _write_immutable(paths["candidate_evidence"], _jsonl_text(candidates))
    _write_immutable(paths["qd_archive"], _json_text(archive))
    artifacts = {key: _artifact(path) for key, path in paths.items()}
    status_counts = dict(Counter(row["status"] for row in candidates))
    promising_count = int(status_counts.get("PROMISING_RESEARCH_CANDIDATE", 0))
    report_path = output / "mini_micro_participation_divergence_report.md"
    result_path = output / "mini_micro_participation_divergence_result.json"
    result: dict[str, Any] = {
        "schema": "hydra_mini_micro_participation_divergence_result_v1",
        "protocol": PROTOCOL,
        "code_commit": str(code_commit),
        "source_preclose_result_hash": str(source_preclose_result_hash),
        "scientific_conclusion": (
            "MINI_MICRO_PARTICIPATION_CANDIDATES_REQUIRE_FROZEN_PROMOTION"
            if promising_count
            else "MINI_MICRO_PARTICIPATION_PRIMARY_INSUFFICIENT_PIVOT_MECHANISM"
        ),
        "candidate_count": 96,
        "structural_prototypes": 96,
        "stage1_survivors": int(sum(bool(row["stage1_passed"]) for row in stage1)),
        "frozen_elite_count": len(elite_ids),
        "promising_candidates": promising_count,
        "promising_candidate_count": promising_count,
        "status_counts": status_counts,
        "candidates": candidates,
        "topstep_path_candidates": int(
            sum(bool(row["topstep"]["path_candidate"]) for row in candidates)
        ),
        "data_provenance": provenance,
        "performance": {
            "bar_rows_after_contract_guards": int(len(bars)),
            "closed_five_minute_contexts": int(len(contexts)),
            "structural_evaluations": 96,
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
        "report_path": str(report_path.resolve()),
        "result_path": str(result_path.resolve()),
    }
    hash_payload = dict(result)
    hash_payload["artifacts"] = {key: value["sha256"] for key, value in sorted(artifacts.items())}
    result["result_hash"] = _canonical_hash(hash_payload)
    _write_immutable(report_path, _render_report(result))
    result["artifacts"]["report"] = _artifact(report_path)
    _write_immutable(result_path, _json_text(result))
    result["artifacts"]["result"] = _artifact(result_path)
    return _strict_json_value(result)
