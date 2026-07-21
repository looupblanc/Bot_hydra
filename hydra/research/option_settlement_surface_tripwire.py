from __future__ import annotations

"""Bounded option-settlement teacher/student economic tripwire.

The temporal contract is deliberately enforced in the call graph: Discovery
outcomes are materialised first, the teacher and futures-only student are
frozen to disk, and only then may Validation/Final-Development outcomes be
opened.  This module contains no Combine, XFA, broker, order, or Q4 path.
"""

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from hydra.data.options_settlement_surface import (
    SettlementSurfaceSnapshot,
    SurfaceBuildDiagnostics,
    iter_dbn_surface_snapshots,
)
from hydra.economic_evolution.schema import stable_hash


MANIFEST = Path("config/research/option_settlement_surface_teacher_student_tripwire_v1.json")
ROLE_ORDER = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
CHICAGO = "America/Chicago"
MARKET_SPEC = {
    "ES": {"symbol": "ES.c.0", "tick": 0.25, "point_value": 50.0},
    "NQ": {"symbol": "NQ.c.0", "tick": 0.25, "point_value": 20.0},
}
TEACHER_FEATURES = (
    "ATM_STRADDLE_VOL_PROXY",
    "DOWNSIDE_UPSIDE_WING_PREMIUM_SKEW",
    "FRONT_NEXT_TERM_SLOPE",
    "ES_NQ_SURFACE_LEVEL_DIFFERENCE",
)
STUDENT_FEATURES = (
    "PRIOR_SESSION_RETURN",
    "PRIOR_SESSION_DOWNSIDE_SEMIVARIANCE_RATIO",
    "OVERNIGHT_GAP_DIVIDED_BY_PRIOR_RANGE",
    "CAUSAL_ANCHOR_DISPLACEMENT_DIVIDED_BY_ATR",
    "PRIOR_ES_NQ_RETURN_DIVERGENCE",
    "CURRENT_ES_NQ_DISPLACEMENT_DIVERGENCE",
)
IMPLEMENTATION_CONTRACT = {
    "opening_anchor": "08:30_OPEN_TO_08:44_CLOSE_CHICAGO",
    "vwap_anchor": "08:30_TO_09:59_SESSION_VWAP_DISPLACEMENT_CHICAGO",
    "anchor_atr": "MEDIAN_TRUE_RANGE_OF_LAST_60_COMPLETED_1M_BARS",
    "prior_session": "08:30_INCLUSIVE_TO_15:10_EXCLUSIVE_CHICAGO",
    "entry": "FIRST_1M_OPEN_WITH_TS_EVENT_STRICTLY_AFTER_DECISION",
    "stop": "MAX(4_TICKS,MIN(TRAILING_15_BAR_MEDIAN_TR,300_USD/POINT_VALUE))",
    "target": "1.5_TIMES_STOP_DISTANCE",
    "same_bar": "STOP_FIRST",
    "time_exit": "PREDECLARED_FINAL_HOLDING_BAR_CLOSE",
    "stress": "ONE_ADVERSE_TICK_AT_ENTRY_AND_EXIT_PLUS_FROZEN_ROUND_TURN_FEES",
    "surface_use": "PRIOR_LISTED_SESSION_ONLY_AND_AVAILABLE_AT_BEFORE_DECISION",
}


class OptionSettlementTripwireError(RuntimeError):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _inside(root: Path, value: str | Path) -> Path:
    candidate = Path(value)
    path = (candidate if candidate.is_absolute() else root / candidate).resolve()
    if path != root and root not in path.parents:
        raise OptionSettlementTripwireError("path escapes project root")
    if not path.is_file():
        raise OptionSettlementTripwireError(f"required frozen artifact missing: {path}")
    return path


def read_and_audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest_path = _inside(project, MANIFEST)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise OptionSettlementTripwireError("manifest hash drift")
    frozen = manifest["frozen_inputs"]
    paths = {
        "statistics": _inside(project, frozen["statistics_path"]),
        "definitions": _inside(project, frozen["definition_path"]),
        "ohlcv": _inside(project, frozen["underlying_ohlcv_path"]),
        "receipt": _inside(project, frozen["acquisition_receipt_path"]),
        "rules": _inside(project, manifest["official_rule_evidence"]["snapshot_path"]),
    }
    expected = {
        "statistics": frozen["statistics_sha256"],
        "definitions": frozen["definition_sha256"],
        "ohlcv": frozen["underlying_ohlcv_sha256"],
        "receipt": frozen["acquisition_receipt_file_sha256"],
        "rules": manifest["official_rule_evidence"]["snapshot_file_sha256"],
    }
    hashes = {key: _sha256_file(path) for key, path in paths.items()}
    if hashes != expected:
        raise OptionSettlementTripwireError("frozen input hash drift")
    receipt = json.loads(paths["receipt"].read_text(encoding="utf-8"))
    receipt_core = dict(receipt)
    receipt_hash = str(receipt_core.pop("receipt_hash", ""))
    if stable_hash(receipt_core) != receipt_hash or receipt_hash != frozen["acquisition_receipt_hash"]:
        raise OptionSettlementTripwireError("acquisition receipt drift")
    if receipt.get("q4_access_count_delta") != 0 or receipt.get("broker_connections") != 0 or receipt.get("orders") != 0:
        raise OptionSettlementTripwireError("forbidden acquisition state")
    rules = json.loads(paths["rules"].read_text(encoding="utf-8"))
    if rules.get("parsed_rule_hash") != manifest["official_rule_evidence"]["parsed_rule_hash"]:
        raise OptionSettlementTripwireError("official rule snapshot drift")
    if frozen["end_exclusive"] != "2024-10-01" or not manifest["governance"]["no_q4_access"]:
        raise OptionSettlementTripwireError("Q4 boundary drift")
    return {
        "root": project,
        "manifest": manifest,
        "paths": paths,
        "hashes": hashes,
        "audit_hash": stable_hash({"manifest_hash": claimed, "input_hashes": hashes}),
    }


def load_surfaces(audit: Mapping[str, Any]) -> tuple[dict[tuple[str, str], SettlementSurfaceSnapshot], dict[str, Any]]:
    diagnostics = SurfaceBuildDiagnostics()
    snapshots = list(
        iter_dbn_surface_snapshots(
            audit["paths"]["statistics"],
            audit["paths"]["definitions"],
            markets=("ES", "NQ"),
            minimum_pairs_per_term=5,
            diagnostics=diagnostics,
        )
    )
    mapping = {(row.settlement_reference_date, row.market): row for row in snapshots}
    if len(mapping) != len(snapshots) or len(mapping) != 40:
        raise OptionSettlementTripwireError("expected exactly 40 unique market/date surfaces")
    if any(row.status != "COMPLETE_FRONT_NEXT" for row in snapshots):
        raise OptionSettlementTripwireError("incomplete settlement surface")
    return mapping, {
        "snapshot_count": len(snapshots),
        "complete_snapshot_count": sum(row.status == "COMPLETE_FRONT_NEXT" for row in snapshots),
        "snapshot_hash": stable_hash([row.to_dict() for row in snapshots]),
        "diagnostics": asdict(diagnostics),
    }


def load_futures_bars(audit: Mapping[str, Any]) -> tuple[dict[tuple[str, str], pd.DataFrame], dict[str, Any]]:
    import databento as db

    frame = db.DBNStore.from_file(audit["paths"]["ohlcv"]).to_df(
        pretty_ts=True, map_symbols=True, price_type="float"
    ).reset_index()
    frame = frame.rename(columns={"ts_event": "timestamp"})
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.loc[
        frame["symbol"].isin([value["symbol"] for value in MARKET_SPEC.values()])
        & frame["timestamp"].ge(pd.Timestamp("2024-09-03", tz="UTC"))
        & frame["timestamp"].lt(pd.Timestamp("2024-10-01", tz="UTC"))
    ].copy()
    if frame.duplicated(["symbol", "timestamp"]).any():
        raise OptionSettlementTripwireError("duplicate futures minute bars")
    frame["local_timestamp"] = frame["timestamp"].dt.tz_convert(CHICAGO)
    frame["session_date"] = frame["local_timestamp"].dt.date.astype(str)
    frame["local_minute"] = frame["local_timestamp"].dt.strftime("%H:%M")
    output: dict[tuple[str, str], pd.DataFrame] = {}
    for market, spec in MARKET_SPEC.items():
        market_rows = frame.loc[frame["symbol"].eq(spec["symbol"])].copy()
        for day, rows in market_rows.groupby("session_date", sort=True):
            output[(market, str(day))] = rows.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    return output, {
        "rows": len(frame),
        "rows_by_market": {
            market: int(sum(len(rows) for (row_market, _), rows in output.items() if row_market == market))
            for market in MARKET_SPEC
        },
        "first_timestamp": frame["timestamp"].min().isoformat(),
        "last_timestamp": frame["timestamp"].max().isoformat(),
        "q4_rows": int(frame["timestamp"].ge(pd.Timestamp("2024-10-01", tz="UTC")).sum()),
    }


def _rth(rows: pd.DataFrame) -> pd.DataFrame:
    return rows.loc[rows["local_minute"].ge("08:30") & rows["local_minute"].lt("15:10")].copy()


def _true_range(rows: pd.DataFrame) -> pd.Series:
    prior = rows["close"].shift(1)
    return pd.concat(
        [rows["high"] - rows["low"], (rows["high"] - prior).abs(), (rows["low"] - prior).abs()], axis=1
    ).max(axis=1)


def _anchor_state(rows: pd.DataFrame, family: str) -> tuple[pd.Timestamp, float, float, float] | None:
    rth = _rth(rows)
    if family == "OPENING_FIFTEEN_MINUTE_DISPLACEMENT_CONTINUATION":
        causal = rth.loc[rth["local_minute"].ge("08:30") & rth["local_minute"].lt("08:45")]
        decision_minute = "08:45"
        displacement = float(causal.iloc[-1]["close"] - causal.iloc[0]["open"]) if len(causal) == 15 else math.nan
    elif family == "SESSION_VWAP_SIXTY_MINUTE_DISPLACEMENT_CONTINUATION":
        causal = rth.loc[rth["local_minute"].ge("08:30") & rth["local_minute"].lt("10:00")]
        decision_minute = "10:00"
        if len(causal) != 90 or float(causal["volume"].sum()) <= 0:
            return None
        typical = (causal["high"] + causal["low"] + causal["close"]) / 3.0
        displacement = float(causal.iloc[-1]["close"] - np.average(typical, weights=causal["volume"]))
    else:
        raise OptionSettlementTripwireError(f"unknown anchor family: {family}")
    if causal.empty or not math.isfinite(displacement):
        return None
    local_day = causal.iloc[0]["local_timestamp"].date().isoformat()
    decision = pd.Timestamp(f"{local_day} {decision_minute}", tz=CHICAGO).tz_convert("UTC")
    history = rows.loc[rows["timestamp"].lt(decision)].tail(60)
    if len(history) < 60:
        return None
    atr = float(_true_range(history).median())
    trailing15 = float(_true_range(history.tail(16)).tail(15).median())
    if not math.isfinite(atr) or atr <= 0 or not math.isfinite(trailing15) or trailing15 <= 0:
        return None
    return decision, displacement, atr, trailing15


def build_opportunity_inputs(
    audit: Mapping[str, Any],
    surfaces: Mapping[tuple[str, str], SettlementSurfaceSnapshot],
    bars: Mapping[tuple[str, str], pd.DataFrame],
) -> list[dict[str, Any]]:
    manifest = audit["manifest"]
    sessions = [day for role in manifest["chronological_roles"] for day in role["sessions"]]
    role_by_day = {day: role["role"] for role in manifest["chronological_roles"] for day in role["sessions"]}
    previous = {sessions[index]: sessions[index - 1] for index in range(1, len(sessions))}
    families = tuple(manifest["structural_opportunities"]["families"])
    raw: list[dict[str, Any]] = []
    state_by_key: dict[tuple[str, str, str], dict[str, float]] = {}
    prior_return_by_key: dict[tuple[str, str], float] = {}
    for day in sessions[1:]:
        prior_day = previous[day]
        for market in MARKET_SPEC:
            current = bars.get((market, day))
            # A listed settlement date can be a futures RTH closure (Sep-20 in
            # this cache).  The option surface remains the immediately prior
            # listed settlement, while futures-only student context uses the
            # most recent *actual* complete RTH session.
            prior_futures_day = next(
                (
                    candidate
                    for candidate in reversed(sessions[: sessions.index(day)])
                    if (market, candidate) in bars and not _rth(bars[(market, candidate)]).empty
                ),
                None,
            )
            prior = bars.get((market, prior_futures_day)) if prior_futures_day else None
            snapshot = surfaces.get((prior_day, market))
            other_snapshot = surfaces.get((prior_day, "NQ" if market == "ES" else "ES"))
            if current is None or prior is None or snapshot is None or other_snapshot is None:
                continue
            prior_rth = _rth(prior)
            current_rth = _rth(current)
            if prior_rth.empty or current_rth.empty:
                continue
            prior_return = float(prior_rth.iloc[-1]["close"] / prior_rth.iloc[0]["open"] - 1.0)
            prior_return_by_key[(day, market)] = prior_return
            returns = prior_rth["close"].pct_change().dropna().to_numpy(dtype=float)
            total_semivar = float(np.square(returns).sum())
            downside_ratio = float(np.square(returns[returns < 0]).sum() / total_semivar) if total_semivar > 0 else 0.5
            prior_range = float(prior_rth["high"].max() - prior_rth["low"].min())
            gap_ratio = float((current_rth.iloc[0]["open"] - prior_rth.iloc[-1]["close"]) / prior_range) if prior_range > 0 else 0.0
            for family in families:
                state = _anchor_state(current, family)
                if state is None:
                    continue
                decision, displacement, atr, trailing15 = state
                tick = MARKET_SPEC[market]["tick"]
                if abs(displacement) < manifest["structural_opportunities"]["minimum_absolute_displacement_ticks"] * tick:
                    continue
                if pd.Timestamp(snapshot.available_at) > decision:
                    continue
                front = snapshot.front_term
                other_front = other_snapshot.front_term
                assert front is not None and other_front is not None
                state_by_key[(day, market, family)] = {"normalized_displacement": displacement / atr}
                core = {
                    "session": day,
                    "role": role_by_day[day],
                    "prior_session": prior_day,
                    "prior_futures_session": prior_futures_day,
                    "market": market,
                    "family": family,
                    "decision_time": decision.isoformat(),
                    "direction": 1 if displacement > 0 else -1,
                    "anchor_displacement": displacement,
                    "anchor_atr": atr,
                    "trailing_15_median_true_range": trailing15,
                    "stop_distance": max(4.0 * tick, min(trailing15, 300.0 / MARKET_SPEC[market]["point_value"])),
                    "holding_minutes": manifest["structural_opportunities"]["maximum_holding_minutes_by_family"][family],
                    "surface_snapshot_hash": snapshot.snapshot_hash,
                    "surface_available_at": snapshot.available_at,
                    "teacher_features": {
                        "ATM_STRADDLE_VOL_PROXY": front.atm_straddle_vol_proxy,
                        "DOWNSIDE_UPSIDE_WING_PREMIUM_SKEW": front.downside_upside_wing_premium_skew,
                        "FRONT_NEXT_TERM_SLOPE": snapshot.front_next_term_slope,
                        "ES_NQ_SURFACE_LEVEL_DIFFERENCE": (
                            surfaces[(prior_day, "ES")].front_term.atm_straddle_vol_proxy
                            - surfaces[(prior_day, "NQ")].front_term.atm_straddle_vol_proxy
                        ),
                    },
                    "student_features": {
                        "PRIOR_SESSION_RETURN": prior_return,
                        "PRIOR_SESSION_DOWNSIDE_SEMIVARIANCE_RATIO": downside_ratio,
                        "OVERNIGHT_GAP_DIVIDED_BY_PRIOR_RANGE": gap_ratio,
                        "CAUSAL_ANCHOR_DISPLACEMENT_DIVIDED_BY_ATR": displacement / atr,
                    },
                }
                raw.append(core)
    for row in raw:
        day, market, family = row["session"], row["market"], row["family"]
        row["student_features"]["PRIOR_ES_NQ_RETURN_DIVERGENCE"] = (
            prior_return_by_key[(day, "ES")] - prior_return_by_key[(day, "NQ")]
        )
        row["student_features"]["CURRENT_ES_NQ_DISPLACEMENT_DIVERGENCE"] = (
            state_by_key[(day, "ES", family)]["normalized_displacement"]
            - state_by_key[(day, "NQ", family)]["normalized_displacement"]
        )
        values = list(row["teacher_features"].values()) + list(row["student_features"].values())
        if not all(value is not None and math.isfinite(float(value)) for value in values):
            raise OptionSettlementTripwireError("non-finite causal decision feature")
        identity = {key: row[key] for key in ("session", "market", "family", "decision_time", "direction")}
        row["opportunity_id"] = "option_surface_" + stable_hash(identity)[:20]
        row["input_fingerprint"] = stable_hash(
            {**identity, "teacher_features": row["teacher_features"], "student_features": row["student_features"]}
        )
    # The manifest records a capacity, not a fabricated denominator.  A listed
    # session with no complete futures RTH bars (2024-09-20 in this cache) is
    # honestly absent rather than synthesized.
    if len(raw) > int(manifest["structural_opportunities"]["expected_opportunity_capacity"]):
        raise OptionSettlementTripwireError(f"opportunity capacity exceeded: {len(raw)}")
    return sorted(raw, key=lambda row: (row["decision_time"], row["market"], row["family"]))


def _scenario_trade(
    opportunity: Mapping[str, Any],
    rows: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    scenario: str,
    direction_flip: bool,
) -> dict[str, Any]:
    market = str(opportunity["market"])
    spec = MARKET_SPEC[market]
    decision = pd.Timestamp(opportunity["decision_time"])
    direction = -int(opportunity["direction"]) if direction_flip else int(opportunity["direction"])
    later = rows.loc[rows["timestamp"].gt(decision)].copy()
    flatten = pd.Timestamp(f"{opportunity['session']} {manifest['causal_and_execution_contract']['mandatory_flatten_local']}", tz=CHICAGO).tz_convert("UTC")
    later = later.loc[later["timestamp"].lt(flatten)]
    if later.empty:
        return {"status": "DATA_CENSORED", "reason": "NO_NEXT_TRADABLE_OPEN"}
    entry_row = later.iloc[0]
    entry_time = pd.Timestamp(entry_row["timestamp"])
    end = min(entry_time + pd.Timedelta(minutes=int(opportunity["holding_minutes"])), flatten)
    path = later.loc[later["timestamp"].lt(end)].copy()
    if path.empty:
        return {"status": "DATA_CENSORED", "reason": "NO_HOLDING_PATH"}
    if path["instrument_id"].astype(str).nunique() != 1:
        return {"status": "HARD_FAILURE", "reason": "INTRATRADE_CONTRACT_ROLL"}
    stress_ticks = manifest["causal_and_execution_contract"]["stressed_extra_slippage_ticks_per_side"] if scenario == "STRESSED" else 0
    entry = float(entry_row["open"]) + direction * stress_ticks * spec["tick"]
    stop_distance = float(opportunity["stop_distance"])
    stop = entry - direction * stop_distance
    target = entry + direction * stop_distance * float(manifest["structural_opportunities"]["target_stop_multiple"])
    exit_price: float | None = None
    exit_time: pd.Timestamp | None = None
    exit_reason = "TIME"
    same_bar = False
    adverse_prices: list[float] = []
    for row in path.itertuples(index=False):
        stop_hit = float(row.low) <= stop if direction > 0 else float(row.high) >= stop
        target_hit = float(row.high) >= target if direction > 0 else float(row.low) <= target
        if stop_hit or target_hit:
            same_bar = stop_hit and target_hit
            if stop_hit:
                exit_reason = "STOP_FIRST"
                raw_exit = min(stop, float(row.open)) if direction > 0 else max(stop, float(row.open))
            else:
                exit_reason = "TARGET"
                raw_exit = target
            exit_price = raw_exit - direction * stress_ticks * spec["tick"]
            exit_time = pd.Timestamp(row.timestamp)
            if stop_hit:
                # Once the executable stop is reached, later intrabar prices
                # are not part of the account path.  A gap through the stop is
                # already represented by raw_exit.
                adverse_prices.append(exit_price)
            else:
                adverse_prices.append(float(row.low) if direction > 0 else float(row.high))
            break
        adverse_prices.append(float(row.low) if direction > 0 else float(row.high))
    if exit_price is None:
        last = path.iloc[-1]
        exit_price = float(last["close"]) - direction * stress_ticks * spec["tick"]
        exit_time = pd.Timestamp(last["timestamp"]) + pd.Timedelta(minutes=1)
    fee = float(manifest["causal_and_execution_contract"]["normal_round_turn_fees_usd"][market])
    gross = direction * (exit_price - entry) * spec["point_value"]
    net = gross - fee
    adverse_price = min(adverse_prices) if direction > 0 else max(adverse_prices)
    adverse_pnl = direction * (adverse_price - entry) * spec["point_value"] - fee
    mll_buffer = 2000.0 + min(0.0, adverse_pnl)
    return {
        "status": "EXECUTABLE_COMPLETE",
        "scenario": scenario,
        "direction": direction,
        "entry_time": entry_time.isoformat(),
        "exit_time": exit_time.isoformat(),
        "entry_price": entry,
        "exit_price": exit_price,
        "stop_price": stop,
        "target_price": target,
        "exit_reason": exit_reason,
        "same_bar_stop_first": same_bar,
        "gross_pnl": gross,
        "net_pnl": net,
        "minimum_mll_buffer_50k": mll_buffer,
        "mll_breach_50k": mll_buffer <= 0.0,
    }


def materialize_outcomes(
    opportunities: Sequence[Mapping[str, Any]],
    bars: Mapping[tuple[str, str], pd.DataFrame],
    manifest: Mapping[str, Any],
    *,
    roles: frozenset[str],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for opportunity in opportunities:
        if opportunity["role"] not in roles:
            continue
        rows = bars[(str(opportunity["market"]), str(opportunity["session"]))]
        output[str(opportunity["opportunity_id"])] = {
            "NORMAL": _scenario_trade(opportunity, rows, manifest, scenario="NORMAL", direction_flip=False),
            "STRESSED": _scenario_trade(opportunity, rows, manifest, scenario="STRESSED", direction_flip=False),
            "FLIP_NORMAL": _scenario_trade(opportunity, rows, manifest, scenario="NORMAL", direction_flip=True),
            "FLIP_STRESSED": _scenario_trade(opportunity, rows, manifest, scenario="STRESSED", direction_flip=True),
        }
    return output


@dataclass(frozen=True, slots=True)
class FrozenLogit:
    features: tuple[str, ...]
    mean: tuple[float, ...]
    scale: tuple[float, ...]
    coefficient: tuple[float, ...]
    intercept: float
    threshold: float
    training_rows: int
    positive_labels: int
    model_kind: str

    def probability(self, feature_values: Mapping[str, float]) -> float:
        x = np.asarray([float(feature_values[name]) for name in self.features], dtype=float)
        z = (x - np.asarray(self.mean)) / np.asarray(self.scale)
        score = float(np.dot(z, np.asarray(self.coefficient)) + self.intercept)
        return 1.0 / (1.0 + math.exp(-max(-40.0, min(40.0, score))))

    def trade(self, feature_values: Mapping[str, float]) -> bool:
        return self.probability(feature_values) >= self.threshold

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def fit_frozen_logit(
    rows: Sequence[Mapping[str, float]],
    labels: Sequence[int],
    *,
    features: Sequence[str],
    threshold: float,
    regularization_c: float,
) -> FrozenLogit:
    x = np.asarray([[float(row[name]) for name in features] for row in rows], dtype=float)
    y = np.asarray(labels, dtype=int)
    if len(x) == 0 or not np.isfinite(x).all():
        raise OptionSettlementTripwireError("empty or non-finite discovery model matrix")
    mean = x.mean(axis=0)
    scale = x.std(axis=0)
    scale = np.where(scale > 1e-12, scale, 1.0)
    standardized = (x - mean) / scale
    if len(np.unique(y)) == 1:
        probability = (float(y.sum()) + 0.5) / (len(y) + 1.0)
        coefficient = np.zeros(x.shape[1], dtype=float)
        intercept = math.log(probability / (1.0 - probability))
        kind = "DEGENERATE_L2_INTERCEPT_ONLY"
    else:
        model = LogisticRegression(
            C=float(regularization_c), solver="liblinear", random_state=1701, max_iter=2000
        ).fit(standardized, y)
        coefficient = model.coef_[0]
        intercept = float(model.intercept_[0])
        kind = "L2_LOGISTIC_LIBLINEAR"
    return FrozenLogit(
        features=tuple(features), mean=tuple(float(v) for v in mean), scale=tuple(float(v) for v in scale),
        coefficient=tuple(float(v) for v in coefficient), intercept=intercept, threshold=float(threshold),
        training_rows=len(y), positive_labels=int(y.sum()), model_kind=kind,
    )


def train_and_freeze_models(
    opportunities: Sequence[Mapping[str, Any]],
    discovery: Mapping[str, Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> tuple[FrozenLogit, FrozenLogit, dict[str, dict[str, Any]], dict[str, Any]]:
    discovery_rows = [row for row in opportunities if row["role"] == "DISCOVERY"]
    labels = [
        int(discovery[row["opportunity_id"]]["STRESSED"]["exit_reason"] == "TARGET")
        for row in discovery_rows
    ]
    teacher = fit_frozen_logit(
        [row["teacher_features"] for row in discovery_rows], labels, features=TEACHER_FEATURES,
        threshold=manifest["teacher_policy_contract"]["trade_probability_threshold"],
        regularization_c=manifest["teacher_policy_contract"]["regularization_c"],
    )
    teacher_discovery_actions = [int(teacher.trade(row["teacher_features"])) for row in discovery_rows]
    student = fit_frozen_logit(
        [row["student_features"] for row in discovery_rows], teacher_discovery_actions, features=STUDENT_FEATURES,
        threshold=manifest["student_contract"]["trade_probability_threshold"],
        regularization_c=manifest["student_contract"]["regularization_c"],
    )
    predictions = {
        row["opportunity_id"]: {
            "teacher_probability": teacher.probability(row["teacher_features"]),
            "teacher_trade": teacher.trade(row["teacher_features"]),
            "student_probability": student.probability(row["student_features"]),
            "student_trade": student.trade(row["student_features"]),
        }
        for row in opportunities
    }
    teacher_coverage = float(np.mean(teacher_discovery_actions))
    model_core = {
        "schema": "hydra_option_settlement_models_freeze_v1",
        "manifest_hash": manifest["manifest_hash"],
        "implementation_contract": IMPLEMENTATION_CONTRACT,
        "training_role": "DISCOVERY_ONLY",
        "discovery_opportunity_count": len(discovery_rows),
        "discovery_outcome_hash": stable_hash(discovery),
        "teacher": teacher.to_dict(),
        "student": student.to_dict(),
        "teacher_discovery_trade_coverage": teacher_coverage,
        "prediction_hash_all_roles_inputs_only": stable_hash(predictions),
        "validation_or_final_outcome_opened_before_freeze": False,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count": 0,
    }
    return teacher, student, predictions, {**model_core, "model_freeze_hash": stable_hash(model_core)}


def _write_immutable_json(path: Path, value: Mapping[str, Any]) -> str:
    raw = _canonical_bytes(value) + b"\n"
    if path.exists() and path.read_bytes() != raw:
        raise OptionSettlementTripwireError(f"immutable output drift: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(raw)
        temporary.replace(path)
    return hashlib.sha256(raw).hexdigest()


def _random_gate(opportunities: Sequence[Mapping[str, Any]], predictions: Mapping[str, Mapping[str, Any]], policy: str) -> set[str]:
    selected: set[str] = set()
    field = f"{policy}_trade"
    for role in ROLE_ORDER:
        rows = [row for row in opportunities if row["role"] == role]
        count = sum(bool(predictions[row["opportunity_id"]][field]) for row in rows)
        ranked = sorted(rows, key=lambda row: stable_hash({"seed": 73031, "role": role, "id": row["opportunity_id"]}))
        selected.update(row["opportunity_id"] for row in ranked[:count])
    return selected


def _policy_summary(
    opportunities: Sequence[Mapping[str, Any]],
    outcomes: Mapping[str, Mapping[str, Any]],
    selected: set[str],
    *,
    outcome_key: str,
) -> dict[str, Any]:
    rows = [row for row in opportunities if row["opportunity_id"] in outcomes]
    complete = [row for row in rows if outcomes[row["opportunity_id"]][outcome_key]["status"] == "EXECUTABLE_COMPLETE"]
    traded = [row for row in complete if row["opportunity_id"] in selected]
    values = [float(outcomes[row["opportunity_id"]][outcome_key]["net_pnl"]) for row in traded]
    buffers = [float(outcomes[row["opportunity_id"]][outcome_key]["minimum_mll_buffer_50k"]) for row in traded]
    positive = [max(value, 0.0) for value in values]
    positive_total = sum(positive)
    trade_share = max(positive, default=0.0) / positive_total if positive_total > 0 else 0.0
    daily: dict[str, float] = {}
    for row, value in zip(traded, values):
        daily[row["session"]] = daily.get(row["session"], 0.0) + value
    positive_days = [max(value, 0.0) for value in daily.values()]
    day_total = sum(positive_days)
    day_share = max(positive_days, default=0.0) / day_total if day_total > 0 else 0.0
    by_context: dict[str, float] = {}
    for row, value in zip(traded, values):
        key = f"{row['market']}|{row['family']}"
        by_context[key] = by_context.get(key, 0.0) + value
    return {
        "opportunity_count": len(complete), "trade_count": len(traded),
        "trade_coverage": len(traded) / len(complete) if complete else 0.0,
        "gross_pnl": sum(float(outcomes[row["opportunity_id"]][outcome_key]["gross_pnl"]) for row in traded),
        "net_pnl": sum(values), "mean_net_per_opportunity": sum(values) / len(complete) if complete else 0.0,
        "target_count": sum(outcomes[row["opportunity_id"]][outcome_key]["exit_reason"] == "TARGET" for row in traded),
        "stop_count": sum(outcomes[row["opportunity_id"]][outcome_key]["exit_reason"] == "STOP_FIRST" for row in traded),
        "time_count": sum(outcomes[row["opportunity_id"]][outcome_key]["exit_reason"] == "TIME" for row in traded),
        "minimum_mll_buffer_50k": min(buffers, default=2000.0),
        "mll_breach_count_50k": sum(outcomes[row["opportunity_id"]][outcome_key]["mll_breach_50k"] for row in traded),
        "maximum_single_trade_positive_profit_share": trade_share,
        "maximum_single_day_positive_profit_share": day_share,
        "positive_context_count": sum(value > 0 for value in by_context.values()),
        "net_by_context": dict(sorted(by_context.items())),
    }


def evaluate(
    opportunities: Sequence[Mapping[str, Any]],
    outcomes: Mapping[str, Mapping[str, Any]],
    predictions: Mapping[str, Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    all_ids = {row["opportunity_id"] for row in opportunities}
    teacher_ids = {key for key, value in predictions.items() if value["teacher_trade"]}
    student_ids = {key for key, value in predictions.items() if value["student_trade"]}
    random_teacher = _random_gate(opportunities, predictions, "teacher")
    random_student = _random_gate(opportunities, predictions, "student")
    policies = {
        "BASELINE": (all_ids, "STRESSED"), "DIRECTION_FLIP": (all_ids, "FLIP_STRESSED"),
        "TEACHER": (teacher_ids, "STRESSED"), "TEACHER_FLIP": (teacher_ids, "FLIP_STRESSED"),
        "TEACHER_RANDOM": (random_teacher, "STRESSED"), "STUDENT": (student_ids, "STRESSED"),
        "STUDENT_FLIP": (student_ids, "FLIP_STRESSED"), "STUDENT_RANDOM": (random_student, "STRESSED"),
    }
    normal_policies = {
        "BASELINE": (all_ids, "NORMAL"), "DIRECTION_FLIP": (all_ids, "FLIP_NORMAL"),
        "TEACHER": (teacher_ids, "NORMAL"), "TEACHER_FLIP": (teacher_ids, "FLIP_NORMAL"),
        "TEACHER_RANDOM": (random_teacher, "NORMAL"), "STUDENT": (student_ids, "NORMAL"),
        "STUDENT_FLIP": (student_ids, "FLIP_NORMAL"), "STUDENT_RANDOM": (random_student, "NORMAL"),
    }
    by_role: dict[str, dict[str, Any]] = {}
    normal_by_role: dict[str, dict[str, Any]] = {}
    for role in ROLE_ORDER:
        role_rows = [row for row in opportunities if row["role"] == role]
        role_outcomes = {row["opportunity_id"]: outcomes[row["opportunity_id"]] for row in role_rows}
        role_summary = {
            name: _policy_summary(role_rows, role_outcomes, selected, outcome_key=key)
            for name, (selected, key) in policies.items()
        }
        for policy in ("TEACHER", "STUDENT"):
            role_summary[policy]["paired_stressed_uplift_vs_baseline"] = (
                role_summary[policy]["net_pnl"] - role_summary["BASELINE"]["net_pnl"]
            )
            role_summary[policy]["delta_vs_random"] = role_summary[policy]["net_pnl"] - role_summary[f"{policy}_RANDOM"]["net_pnl"]
            role_summary[policy]["delta_vs_direction_flip"] = role_summary[policy]["net_pnl"] - role_summary[f"{policy}_FLIP"]["net_pnl"]
        by_role[role] = role_summary
        normal_summary = {
            name: _policy_summary(role_rows, role_outcomes, selected, outcome_key=key)
            for name, (selected, key) in normal_policies.items()
        }
        for policy in ("TEACHER", "STUDENT"):
            normal_summary[policy]["paired_normal_uplift_vs_baseline"] = (
                normal_summary[policy]["net_pnl"] - normal_summary["BASELINE"]["net_pnl"]
            )
        normal_by_role[role] = normal_summary
    gate = manifest["selection_gate"]
    counts = {role: sum(row["role"] == role for row in opportunities) for role in ROLE_ORDER}
    coverage_ok = (
        len(opportunities) >= gate["minimum_total_independent_opportunities"]
        and counts["VALIDATION"] >= gate["minimum_validation_independent_opportunities"]
        and counts["FINAL_DEVELOPMENT"] >= gate["minimum_final_independent_opportunities"]
    )

    def policy_gate(policy: str, *, require_retention: bool) -> bool:
        validation = by_role["VALIDATION"][policy]
        final = by_role["FINAL_DEVELOPMENT"][policy]
        common = (
            validation["net_pnl"] > 0 and final["net_pnl"] > 0
            and validation["paired_stressed_uplift_vs_baseline"] > 0
            and final["paired_stressed_uplift_vs_baseline"] > 0
            and validation["delta_vs_random"] > 0 and final["delta_vs_random"] > 0
            and validation["delta_vs_direction_flip"] > 0 and final["delta_vs_direction_flip"] > 0
            and validation["mll_breach_count_50k"] + final["mll_breach_count_50k"] == 0
            and max(validation["maximum_single_trade_positive_profit_share"], final["maximum_single_trade_positive_profit_share"]) <= gate["maximum_single_trade_or_day_positive_profit_share"]
            and max(validation["maximum_single_day_positive_profit_share"], final["maximum_single_day_positive_profit_share"]) <= gate["maximum_single_trade_or_day_positive_profit_share"]
            and len({row["market"] for row in opportunities if row["role"] in {"VALIDATION", "FINAL_DEVELOPMENT"} and row["opportunity_id"] in (teacher_ids if policy == "TEACHER" else student_ids)}) >= gate["minimum_distinct_markets_or_anchor_families"]
        )
        if not common or not require_retention:
            return common
        teacher_uplift = sum(by_role[role]["TEACHER"]["paired_stressed_uplift_vs_baseline"] for role in ("VALIDATION", "FINAL_DEVELOPMENT"))
        student_uplift = sum(by_role[role]["STUDENT"]["paired_stressed_uplift_vs_baseline"] for role in ("VALIDATION", "FINAL_DEVELOPMENT"))
        return teacher_uplift > 0 and student_uplift / teacher_uplift >= gate["student_minimum_teacher_uplift_retention_fraction"]

    discovery_teacher_coverage = by_role["DISCOVERY"]["TEACHER"]["trade_coverage"]
    teacher_ok = (
        coverage_ok
        and manifest["teacher_policy_contract"]["minimum_trade_coverage_fraction"] <= discovery_teacher_coverage
        and (1.0 - discovery_teacher_coverage) <= manifest["teacher_policy_contract"]["maximum_abstention_fraction"]
        and policy_gate("TEACHER", require_retention=False)
    )
    student_ok = teacher_ok and policy_gate("STUDENT", require_retention=True)
    if not coverage_ok:
        status = gate["coverage_failure_status"]
    elif not teacher_ok:
        status = gate["teacher_failure_status"]
    elif not student_ok:
        status = gate["student_failure_status"]
    else:
        status = gate["success_status"]
    return {
        "status": status, "tier": "TIER_E" if status == gate["success_status"] else "TERMINAL_TRIPWIRE",
        "opportunity_counts": counts, "policy_results_by_role": by_role,
        "normal_policy_results_by_role": normal_by_role,
        "teacher_gate_pass": teacher_ok, "student_gate_pass": student_ok,
        "combine_replay_count": 0, "xfa_path_count": 0, "broker_connections": 0, "orders": 0, "q4_access_count": 0,
    }


def run_tripwire(root: str | Path, output_dir: str | Path) -> dict[str, Any]:
    audit = read_and_audit_inputs(root)
    output = Path(output_dir).resolve()
    surfaces, surface_audit = load_surfaces(audit)
    bars, bar_audit = load_futures_bars(audit)
    opportunities = build_opportunity_inputs(audit, surfaces, bars)

    # This is the only economic outcome access before the model freeze.
    discovery = materialize_outcomes(
        opportunities, bars, audit["manifest"], roles=frozenset({"DISCOVERY"})
    )
    teacher, student, predictions, model_freeze = train_and_freeze_models(
        opportunities, discovery, audit["manifest"]
    )
    model_freeze_path = output / "model_freeze.json"
    model_freeze_file_sha256 = _write_immutable_json(model_freeze_path, model_freeze)
    if _sha256_file(model_freeze_path) != model_freeze_file_sha256:
        raise OptionSettlementTripwireError("model freeze durability failure")

    # Held-out economics become readable only after the durable model freeze.
    heldout = materialize_outcomes(
        opportunities, bars, audit["manifest"], roles=frozenset({"VALIDATION", "FINAL_DEVELOPMENT"})
    )
    outcomes = {**discovery, **heldout}
    if set(outcomes) != {row["opportunity_id"] for row in opportunities}:
        raise OptionSettlementTripwireError("outcome ledger incomplete")
    evaluation = evaluate(opportunities, outcomes, predictions, audit["manifest"])
    evidence_rows = [
        {
            "opportunity": row, "prediction": predictions[row["opportunity_id"]],
            "outcomes": outcomes[row["opportunity_id"]],
        }
        for row in opportunities
    ]
    evidence_path = output / "opportunity_evidence.jsonl"
    evidence_raw = b"".join(_canonical_bytes(row) + b"\n" for row in evidence_rows)
    if evidence_path.exists() and evidence_path.read_bytes() != evidence_raw:
        raise OptionSettlementTripwireError("immutable opportunity evidence drift")
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    if not evidence_path.exists():
        evidence_path.write_bytes(evidence_raw)
    evidence_sha = hashlib.sha256(evidence_raw).hexdigest()
    result_core = {
        "schema": "hydra_option_settlement_surface_teacher_student_tripwire_result_v1",
        "branch_id": audit["manifest"]["branch_id"], "manifest_hash": audit["manifest"]["manifest_hash"],
        "input_audit_hash": audit["audit_hash"], "surface_audit": surface_audit, "bar_audit": bar_audit,
        "implementation_contract": IMPLEMENTATION_CONTRACT,
        "model_freeze_path": model_freeze_path.name, "model_freeze_file_sha256": model_freeze_file_sha256,
        "model_freeze_hash": model_freeze["model_freeze_hash"], "teacher_model": teacher.to_dict(), "student_model": student.to_dict(),
        "opportunity_evidence_path": evidence_path.name, "opportunity_evidence_sha256": evidence_sha,
        "evaluation": evaluation, "incremental_data_spend_usd": 0.0,
        "frozen_economic_boundaries": {"tier_q_allowed": False, "combine_allowed": False, "xfa_allowed": False},
    }
    result = {**result_core, "result_hash": stable_hash(result_core)}
    result_path = output / "result.json"
    result_file_sha = _write_immutable_json(result_path, result)
    return {**result, "result_path": str(result_path), "result_file_sha256": result_file_sha}
