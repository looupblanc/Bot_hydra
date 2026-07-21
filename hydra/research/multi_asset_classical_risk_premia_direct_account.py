"""Bounded classical risk-premia screen evaluated as complete account policies.

The branch is deliberately small: four asset classes, 32 frozen policies and
daily session-flat execution.  Signals are computed after a completed session,
orders fill at the next session's first tradable open, and every position is
closed before the frozen 13:15 Chicago cutoff.  Stress is advisory; normal
economics are primary while MLL and consistency remain hard constraints.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.combine_episode import TradePathEvent, run_combine_episode
from hydra.propfirm.topstep_150k import Topstep150KConfig


SCHEMA = "hydra_multi_asset_classical_risk_premia_direct_account_v1"
BRANCH_ID = "MULTI_ASSET_CLASSICAL_RISK_PREMIA_DIRECT_ACCOUNT_V1"
BASE_PATH = Path(
    "data/cache/databento/GLBX-MDP3_ohlcv-1m_"
    "RTY_M2K_YM_MYM_GC_MGC_CL_MCL_2023-01-01_2024-10-01.parquet"
)
ZN_PATH = Path(
    "data/cache/databento/treasury_curve_tripwire/110de6f631a3ebf415af/"
    "bound_input/zn_explicit_contract_ohlcv.parquet"
)
ROLE_BOUNDS = {
    "DISCOVERY": ("2023-01-03", "2023-10-02"),
    "VALIDATION": ("2023-10-02", "2024-04-01"),
    "FINAL_DEVELOPMENT": ("2024-04-01", "2024-10-01"),
}


@dataclass(frozen=True, slots=True)
class AssetSpec:
    symbol: str
    asset_class: str
    point_value: float
    tick_value: float
    round_turn_commission: float
    mini_equivalent: float

    @property
    def normal_cost(self) -> float:
        return self.round_turn_commission + 2.0 * self.tick_value


ASSETS = {
    "M2K": AssetSpec("M2K", "EQUITY", 5.0, 0.50, 1.24, 0.10),
    "MYM": AssetSpec("MYM", "EQUITY", 0.50, 0.50, 1.24, 0.10),
    "MGC": AssetSpec("MGC", "METAL", 10.0, 1.00, 1.74, 0.10),
    "MCL": AssetSpec("MCL", "ENERGY", 100.0, 1.00, 1.54, 0.10),
    "ZN": AssetSpec("ZN", "RATES", 1000.0, 15.625, 4.20, 1.00),
}


@dataclass(frozen=True, slots=True)
class Policy:
    family: str
    lookback_sessions: int
    maximum_assets: int
    mll_risk_fraction: float

    @property
    def policy_id(self) -> str:
        return "classical_rp_" + stable_hash(asdict(self))[:20]


def frozen_policies() -> tuple[Policy, ...]:
    rows: list[Policy] = []
    for family in ("TIME_SERIES_TREND", "CROSS_SECTIONAL_STRENGTH"):
        for lookback in (20, 60, 120):
            for maximum_assets in (2, 3):
                for risk in (0.10, 0.20):
                    rows.append(Policy(family, lookback, maximum_assets, risk))
    for lookback in (20, 60):
        for maximum_assets in (2, 3):
            for risk in (0.10, 0.20):
                rows.append(Policy("TREND_STRENGTH_BLEND", lookback, maximum_assets, risk))
    if len(rows) != 32 or len({row.policy_id for row in rows}) != 32:
        raise RuntimeError("frozen policy lattice drift")
    return tuple(rows)


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise RuntimeError(f"immutable artifact drift: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _manifest(root: Path) -> dict[str, Any]:
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "hypothesis": (
            "Medium-horizon trend and relative-strength states across distinct futures "
            "asset classes can produce a low-turnover, session-flat direct account path."
        ),
        "strongest_argument_against": (
            "Mandatory daily flattening can consume the classical premium in costs and "
            "the common sample contains only three chronological eras."
        ),
        "data_roles": ROLE_BOUNDS,
        "inputs": {
            "base": {"path": str(BASE_PATH), "sha256": sha256_file(root / BASE_PATH)},
            "zn": {"path": str(ZN_PATH), "sha256": sha256_file(root / ZN_PATH)},
        },
        "assets": {key: asdict(value) for key, value in ASSETS.items()},
        "policies": [asdict(value) | {"policy_id": value.policy_id} for value in frozen_policies()],
        "execution": {
            "decision": "completed prior session only",
            "fill": "first tradable open of next session",
            "flatten_cutoff": "13:15 America/Chicago",
            "positions_aggregated_to_one_daily_account_event": True,
        },
        "selection": {
            "discovery_only": True,
            "maximum_selected": 4,
            "held_gate": "normal positive in validation and final, stress advisory, no class domination",
            "exact_account_only_after_held_gate": True,
        },
        "account_horizons": [10, 20, 40],
        "account_sizes": ["50K", "100K", "150K"],
        "q4_allowed": False,
        "purchase_allowed": False,
        "broker_allowed": False,
        "orders_allowed": False,
        "xfa_allowed": False,
        "maximum_workers": 1,
    }
    core["manifest_hash"] = stable_hash(core)
    return core


def _session_key(ts: pd.Series) -> pd.Series:
    local = ts.dt.tz_convert("America/Chicago")
    dates = local.dt.normalize()
    dates = dates.where(local.dt.hour >= 17, dates - pd.Timedelta(days=1))
    return dates.dt.strftime("%Y%m%d").astype(int)


def _daily_from_frame(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    part = frame.loc[frame["symbol"].astype(str).eq(symbol)].copy()
    part["timestamp"] = pd.to_datetime(part["timestamp"], utc=True)
    local = part["timestamp"].dt.tz_convert("America/Chicago")
    minute = local.dt.hour * 60 + local.dt.minute
    # Include the overnight session and stop well before Topstep's 15:10 close.
    part = part.loc[(minute >= 17 * 60) | (minute < 13 * 60 + 15)].copy()
    part["session_day"] = _session_key(part["timestamp"])
    part.sort_values("timestamp", inplace=True)
    grouped = part.groupby("session_day", sort=True)
    output = grouped.agg(
        entry_ns=("timestamp", lambda value: int(value.iloc[0].value)),
        exit_ns=("timestamp", lambda value: int(value.iloc[-1].value)),
        entry=("open", "first"),
        exit=("close", "last"),
        high=("high", "max"),
        low=("low", "min"),
        observations=("close", "size"),
    )
    output = output.loc[output["observations"].ge(30)].copy()
    output["symbol"] = symbol
    return output


def load_daily_panel(root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    base = pd.read_parquet(
        root / BASE_PATH,
        columns=("timestamp", "symbol", "open", "high", "low", "close"),
    )
    zn = pd.read_parquet(
        root / ZN_PATH,
        columns=("timestamp", "symbol", "open", "high", "low", "close"),
    )
    parts = [_daily_from_frame(base, symbol) for symbol in ("M2K", "MYM", "MGC", "MCL")]
    parts.append(_daily_from_frame(zn, "ZN"))
    common = sorted(set.intersection(*(set(part.index) for part in parts)))
    panel_rows = []
    for part in parts:
        panel_rows.append(part.loc[common].reset_index())
    panel = pd.concat(panel_rows, ignore_index=True)
    panel = panel.loc[
        panel["session_day"].ge(20230103) & panel["session_day"].lt(20241001)
    ].copy()
    counts = panel.groupby("symbol").size().astype(int).to_dict()
    return panel, {
        "common_session_count": len(common),
        "usable_common_session_count": int(panel["session_day"].nunique()),
        "rows_by_symbol": counts,
        "first_session": int(panel["session_day"].min()),
        "last_session": int(panel["session_day"].max()),
    }


def _role(day: int) -> str | None:
    stamp = pd.Timestamp(str(day))
    for role, (start, end) in ROLE_BOUNDS.items():
        if pd.Timestamp(start) <= stamp < pd.Timestamp(end):
            return role
    return None


def build_features(panel: pd.DataFrame) -> pd.DataFrame:
    tables = {}
    for symbol, frame in panel.groupby("symbol"):
        frame = frame.sort_values("session_day").set_index("session_day").copy()
        frame["cc_return"] = np.log(frame["exit"].astype(float)).diff()
        for lookback in (20, 60, 120):
            frame[f"ret_{lookback}"] = np.log(
                frame["exit"].astype(float) / frame["exit"].astype(float).shift(lookback)
            )
        frame["vol_20"] = frame["cc_return"].rolling(20, min_periods=15).std(ddof=0)
        tables[str(symbol)] = frame
    rows = []
    sessions = sorted(set.intersection(*(set(value.index) for value in tables.values())))
    for day in sessions:
        for symbol, table in tables.items():
            row = table.loc[day].to_dict()
            rows.append({"session_day": int(day), "symbol": symbol, **row})
    return pd.DataFrame(rows).sort_values(["session_day", "symbol"]).reset_index(drop=True)


def _positions_for_day(
    policy: Policy,
    previous: pd.DataFrame,
    *,
    maximum_loss_limit: float,
    maximum_mini_equivalent: float,
) -> dict[str, tuple[int, int]]:
    score_rows = []
    for row in previous.itertuples(index=False):
        ret = float(getattr(row, f"ret_{policy.lookback_sessions}"))
        vol = float(row.vol_20)
        if not math.isfinite(ret) or not math.isfinite(vol) or vol <= 0.0:
            continue
        standardized = ret / (vol * math.sqrt(policy.lookback_sessions))
        score_rows.append((str(row.symbol), standardized, abs(standardized), vol, float(row.exit)))
    if len(score_rows) < 3:
        return {}
    selected: list[tuple[str, int, float, float]] = []
    if policy.family == "TIME_SERIES_TREND":
        for symbol, score, strength, vol, price in sorted(score_rows, key=lambda x: -x[2])[: policy.maximum_assets]:
            selected.append((symbol, 1 if score > 0 else -1, vol, price))
    elif policy.family == "CROSS_SECTIONAL_STRENGTH":
        ordered = sorted(score_rows, key=lambda x: x[1])
        legs = max(1, policy.maximum_assets // 2)
        for symbol, _score, _strength, vol, price in ordered[:legs]:
            selected.append((symbol, -1, vol, price))
        for symbol, _score, _strength, vol, price in ordered[-legs:]:
            selected.append((symbol, 1, vol, price))
    else:
        aligned = [value for value in score_rows if abs(value[1]) >= np.median([x[2] for x in score_rows])]
        for symbol, score, _strength, vol, price in sorted(aligned, key=lambda x: -x[2])[: policy.maximum_assets]:
            selected.append((symbol, 1 if score > 0 else -1, vol, price))
    if not selected:
        return {}
    total_budget = maximum_loss_limit * policy.mll_risk_fraction
    per_asset = total_budget / len(selected)
    output: dict[str, tuple[int, int]] = {}
    used_equiv = 0.0
    for symbol, direction, vol, price in selected:
        spec = ASSETS[symbol]
        one_sigma = max(price * vol * spec.point_value, spec.tick_value)
        quantity = max(1, int(per_asset / max(2.0 * one_sigma, 1.0)))
        allowed = int((maximum_mini_equivalent - used_equiv + 1e-12) / spec.mini_equivalent)
        quantity = min(quantity, max(allowed, 0))
        if quantity > 0:
            output[symbol] = (direction, quantity)
            used_equiv += quantity * spec.mini_equivalent
    return output


def policy_events(
    policy: Policy,
    features: pd.DataFrame,
    *,
    maximum_loss_limit: float,
    maximum_mini_equivalent: float,
    stressed: bool,
    excluded_class: str | None = None,
) -> list[TradePathEvent]:
    by_day = {int(day): frame for day, frame in features.groupby("session_day", sort=True)}
    days = sorted(by_day)
    output: list[TradePathEvent] = []
    for prior_day, day in zip(days, days[1:], strict=False):
        current = by_day[day]
        previous = by_day[prior_day]
        positions = _positions_for_day(
            policy,
            previous,
            maximum_loss_limit=maximum_loss_limit,
            maximum_mini_equivalent=maximum_mini_equivalent,
        )
        if excluded_class:
            positions = {
                symbol: value
                for symbol, value in positions.items()
                if ASSETS[symbol].asset_class != excluded_class
            }
        if not positions:
            continue
        gross = net = worst = best = 0.0
        quantity = 0
        equivalent = 0.0
        classes: set[str] = set()
        for row in current.itertuples(index=False):
            symbol = str(row.symbol)
            if symbol not in positions:
                continue
            direction, qty = positions[symbol]
            spec = ASSETS[symbol]
            pnl = direction * (float(row.exit) - float(row.entry)) * spec.point_value * qty
            adverse = (
                (float(row.low) - float(row.entry))
                if direction > 0
                else (float(row.entry) - float(row.high))
            ) * spec.point_value * qty
            favorable = (
                (float(row.high) - float(row.entry))
                if direction > 0
                else (float(row.entry) - float(row.low))
            ) * spec.point_value * qty
            cost = spec.normal_cost * (1.5 if stressed else 1.0) * qty
            gross += pnl
            net += pnl - cost
            worst += min(adverse, 0.0) - cost
            best += max(favorable, 0.0) - cost
            quantity += qty
            equivalent += qty * spec.mini_equivalent
            classes.add(spec.asset_class)
        if quantity:
            output.append(
                TradePathEvent(
                    event_id=f"{policy.policy_id}:{day}:{'S' if stressed else 'N'}:{excluded_class or 'ALL'}",
                    decision_ns=int(previous["exit_ns"].max()),
                    exit_ns=int(current["exit_ns"].max()),
                    session_day=int(day),
                    net_pnl=float(net),
                    gross_pnl=float(gross),
                    worst_unrealized_pnl=float(worst),
                    best_unrealized_pnl=float(best),
                    quantity=int(quantity),
                    mini_equivalent=float(equivalent),
                    regime="|".join(sorted(classes)),
                    session_compliant=True,
                    contract_limit_compliant=equivalent <= maximum_mini_equivalent + 1e-12,
                )
            )
    return output


def _summary(events: Sequence[TradePathEvent], role: str) -> dict[str, Any]:
    selected = [event for event in events if _role(event.session_day) == role]
    by_class: dict[str, float] = {}
    for event in selected:
        for name in event.regime.split("|"):
            by_class[name] = by_class.get(name, 0.0) + event.net_pnl / max(len(event.regime.split("|")), 1)
    positive = {key: max(value, 0.0) for key, value in by_class.items()}
    total_positive = sum(positive.values())
    return {
        "event_count": len(selected),
        "gross_usd": float(sum(event.gross_pnl for event in selected)),
        "net_usd": float(sum(event.net_pnl for event in selected)),
        "median_daily_net_usd": float(np.median([event.net_pnl for event in selected])) if selected else 0.0,
        "minimum_worst_unrealized_usd": float(min((event.worst_unrealized_pnl for event in selected), default=0.0)),
        "asset_class_contribution_proxy": by_class,
        "largest_positive_class_share": max(positive.values(), default=0.0) / total_positive if total_positive else 1.0,
    }


def _upper_bound(features: pd.DataFrame) -> dict[str, Any]:
    # Outcome-aware sign choice is a non-deployable information ceiling only.
    gross = normal = stress = 0.0
    count = 0
    for row in features.itertuples(index=False):
        spec = ASSETS[str(row.symbol)]
        move = abs(float(row.exit) - float(row.entry)) * spec.point_value
        gross += move
        normal += move - spec.normal_cost
        stress += move - 1.5 * spec.normal_cost
        count += 1
    return {
        "label": "NON_DEPLOYABLE_FULL_OUTCOME_UPPER_BOUND",
        "asset_session_count": count,
        "gross_usd": gross,
        "normal_net_usd": normal,
        "stressed_net_usd": stress,
        "positive_after_stress": stress > 0.0,
    }


def _config(account: str) -> tuple[Topstep150KConfig, float]:
    values = {
        "50K": (50_000.0, 3_000.0, 2_000.0, 5.0),
        "100K": (100_000.0, 6_000.0, 3_000.0, 10.0),
        "150K": (150_000.0, 9_000.0, 4_500.0, 15.0),
    }
    size, target, mll, maximum = values[account]
    return (
        Topstep150KConfig(
            account_size=size,
            combine_starting_balance=size,
            combine_profit_target=target,
            combine_max_loss_limit=mll,
            mll_mode="eod_level_rt_breach",
            no_daily_loss_limit=True,
            use_optional_daily_loss_limit=False,
            minimum_pass_days=2,
        ),
        maximum,
    )


def _episodes(events: Sequence[TradePathEvent], days: Sequence[int], config: Topstep150KConfig, maximum: float) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for horizon in (10, 20, 40):
        starts = list(days[::horizon])
        starts = [day for day in starts if days.index(day) + horizon <= len(days)]
        rows = [
            run_combine_episode(
                events,
                days,
                start_day=int(day),
                maximum_duration_days=horizon,
                config=config,
                maximum_mini_equivalent=maximum,
            )
            for day in starts
        ]
        result[str(horizon)] = {
            "starts": len(rows),
            "passes": sum(row.passed for row in rows),
            "pass_rate": sum(row.passed for row in rows) / len(rows) if rows else 0.0,
            "mll_breaches": sum(row.mll_breached for row in rows),
            "median_target_progress": float(np.median([row.target_progress for row in rows])) if rows else 0.0,
            "lower_quartile_target_progress": float(np.quantile([row.target_progress for row in rows], 0.25)) if rows else 0.0,
            "minimum_mll_buffer": min((row.minimum_mll_buffer for row in rows), default=float(config.combine_max_loss_limit)),
            "consistency_failures": sum(not row.consistency_ok for row in rows),
        }
    return result


def run(root: str | Path, output_dir: str | Path = "reports/research_tripwires/multi_asset_classical_risk_premia_direct_account_v1") -> dict[str, Any]:
    started = time.monotonic()
    root = Path(root).resolve()
    output = (root / output_dir).resolve()
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_core = dict(manifest)
        manifest_hash = str(manifest_core.pop("manifest_hash"))
        if manifest_hash != stable_hash(manifest_core):
            raise RuntimeError("persisted manifest hash drift")
        if manifest.get("branch_id") != BRANCH_ID or len(manifest.get("policies", ())) != 32:
            raise RuntimeError("persisted manifest contract drift")
    else:
        manifest = _manifest(root)
        _write_once(manifest_path, manifest)
    result_path = output / "economic_result.json"
    if result_path.exists():
        persisted = json.loads(result_path.read_text(encoding="utf-8"))
        result_core = dict(persisted)
        result_hash = str(result_core.pop("result_hash"))
        if result_hash != stable_hash(result_core):
            raise RuntimeError("persisted economic result hash drift")
        if persisted.get("manifest_hash") != manifest.get("manifest_hash"):
            raise RuntimeError("persisted economic result manifest drift")
        return persisted
    panel, inventory = load_daily_panel(root)
    features = build_features(panel)
    upper = _upper_bound(features)
    candidates = []
    for policy in frozen_policies():
        normal = policy_events(policy, features, maximum_loss_limit=4500.0, maximum_mini_equivalent=15.0, stressed=False)
        stress = policy_events(policy, features, maximum_loss_limit=4500.0, maximum_mini_equivalent=15.0, stressed=True)
        candidates.append(
            {
                "policy_id": policy.policy_id,
                "spec": asdict(policy),
                "normal": {role: _summary(normal, role) for role in ROLE_BOUNDS},
                "stressed": {role: _summary(stress, role) for role in ROLE_BOUNDS},
            }
        )
    ranked = sorted(
        candidates,
        key=lambda row: (
            -float(row["normal"]["DISCOVERY"]["net_usd"]),
            -float(row["stressed"]["DISCOVERY"]["net_usd"]),
            row["policy_id"],
        ),
    )
    selected = ranked[:4]
    selected_ids = {row["policy_id"] for row in selected}
    exact = []
    for row in selected:
        normal_held = all(float(row["normal"][role]["net_usd"]) > 0.0 for role in ("VALIDATION", "FINAL_DEVELOPMENT"))
        stress_held = all(float(row["stressed"][role]["net_usd"]) > 0.0 for role in ("VALIDATION", "FINAL_DEVELOPMENT"))
        concentration_ok = all(float(row["normal"][role]["largest_positive_class_share"]) <= 0.70 for role in ("VALIDATION", "FINAL_DEVELOPMENT"))
        policy = Policy(**row["spec"])
        leave_one_out = {}
        for asset_class in sorted({spec.asset_class for spec in ASSETS.values()}):
            values = policy_events(policy, features, maximum_loss_limit=4500.0, maximum_mini_equivalent=15.0, stressed=False, excluded_class=asset_class)
            leave_one_out[asset_class] = {role: _summary(values, role) for role in ("VALIDATION", "FINAL_DEVELOPMENT")}
        loo_positive = sum(
            all(float(value[role]["net_usd"]) > 0.0 for role in ("VALIDATION", "FINAL_DEVELOPMENT"))
            for value in leave_one_out.values()
        ) >= 2
        gate = normal_held and concentration_ok and loo_positive
        account = {}
        if gate:
            days = sorted(int(value) for value in features["session_day"].unique())
            for account_name in ("50K", "100K", "150K"):
                config, maximum = _config(account_name)
                account[account_name] = {}
                for scenario, stressed in (("NORMAL", False), ("STRESSED_1_5X", True)):
                    events = policy_events(
                        policy,
                        features,
                        maximum_loss_limit=float(config.combine_max_loss_limit),
                        maximum_mini_equivalent=maximum,
                        stressed=stressed,
                    )
                    account[account_name][scenario] = _episodes(events, days, config, maximum)
        exact.append(
            {
                "policy_id": row["policy_id"],
                "normal_held_positive": normal_held,
                "stressed_advisory_held_positive": stress_held,
                "concentration_ok": concentration_ok,
                "leave_one_asset_class_out": leave_one_out,
                "leave_one_out_positive_count_at_least_two": loo_positive,
                "exact_account_gate": gate,
                "account_results": account,
            }
        )
    exact_count = sum(bool(row["account_results"]) for row in exact)
    any_pass = any(
        cell["passes"] > 0
        for row in exact
        for account in row["account_results"].values()
        for scenario in account.values()
        for cell in scenario.values()
    )
    verdict = (
        "MULTI_ASSET_CLASSICAL_RISK_PREMIA_DIRECT_ACCOUNT_PASSED_DEVELOPMENT_ONLY"
        if any_pass
        else "MULTI_ASSET_CLASSICAL_RISK_PREMIA_DIRECT_ACCOUNT_FALSIFIED_HELD_OUT"
    )
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": verdict,
        "manifest_hash": manifest["manifest_hash"],
        "data_inventory": inventory,
        "non_deployable_upper_bound": upper,
        "policy_count": len(candidates),
        "candidates": candidates,
        "selected_policy_ids": sorted(selected_ids),
        "selected_exact_audits": exact,
        "exact_account_policy_count": exact_count,
        "combine_pass_observed": any_pass,
        "stress_interpretation": "ADVISORY_NOT_PRIMARY",
        "mll_and_consistency": "HARD",
        "leave_one_asset_class_out_performed": True,
        "governance": {
            "data_purchase_usd": 0.0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "xfa_runs": 0,
            "worker_count": 1,
            "numeric_threads_per_worker": 1,
        },
        "runtime_seconds": time.monotonic() - started,
        "next_action": (
            "FREEZE_DEVELOPMENT_POLICY_AND_REQUIRE_FRESH_CONFIRMATION"
            if any_pass
            else "TOMBSTONE_CLASSICAL_RISK_PREMIA_DIRECT_ACCOUNT_NO_NEIGHBOR_TUNING"
        ),
    }
    result["result_hash"] = stable_hash(result)
    _write_once(result_path, result)
    _write_once(
        output / "production_state.json",
        {
            "status": "COMPLETE",
            "verdict": verdict,
            "manifest_hash": manifest["manifest_hash"],
            "result_hash": result["result_hash"],
            "policy_count": len(candidates),
            "exact_account_policy_count": exact_count,
        },
    )
    return result


if __name__ == "__main__":
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    run(Path(__file__).resolve().parents[2])
