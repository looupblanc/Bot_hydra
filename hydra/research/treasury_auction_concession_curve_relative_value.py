"""Bounded pre-auction Treasury concession relative-value tripwire.

This experiment is deliberately narrower than the already terminal Treasury
branches.  It does not consume auction-result fields.  A scheduled auction is
known from its announcement, and the policy observes only the *pre-auction*
price displacement of the auction tenor against one adjacent Treasury-futures
tenor.  A qualifying concession is entered at the next tradable one-minute
open, before the competitive close, and is exited on a frozen time horizon.

The module owns no controller, database, registry, broker, or writer.  It
writes only an isolated development report after first sealing a deterministic
manifest.  Exact Combine replay is conditional on the held economic gate.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_exact_replay import (
    _account_config,
    _load_rule_snapshot,
)
from hydra.propfirm.combine_episode import TradePathEvent, run_combine_episode
from hydra.research.curve_relative_value_tripwire import TREASURY_SPECS


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = Path(
    "reports/research_tripwires/treasury_auction_concession_curve_relative_value_v1"
)
RULE_SNAPSHOT = Path("config/rulesets/topstep_official_2026-07-19.json")
NOTE_INPUT = Path("data/cache/official/treasury_auction_demand_shock_v1/note.json")
BOND_INPUT = Path("data/cache/official/treasury_auction_demand_shock_v1/bond.json")
PRICE_ROOT = Path(
    "data/cache/databento/treasury_curve_tripwire/110de6f631a3ebf415af/bound_input"
)
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
ROLE_BOUNDS = {
    "DISCOVERY": ("2023-01-01", "2024-01-01"),
    "VALIDATION": ("2024-01-01", "2024-05-01"),
    "FINAL_DEVELOPMENT": ("2024-05-01", "2024-10-01"),
}
LOOKBACKS = (30, 60)
THRESHOLDS = (0.0, 0.25, 0.50, 0.75)
HOLDING_MINUTES = (15, 30, 60)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# The target is the auction-tenor proxy.  The hedge is the adjacent maturity
# proxy.  ZN/TN is used for the ten-year auction so both legs remain close to
# the auction point without introducing a three-tenor curvature construction.
TERM_PAIRS: dict[str, tuple[str, str]] = {
    "2-Year": ("ZT", "ZF"),
    "3-Year": ("ZF", "ZT"),
    "5-Year": ("ZF", "ZN"),
    "7-Year": ("ZN", "ZF"),
    "10-Year": ("ZN", "TN"),
    "20-Year": ("ZB", "UB"),
    "30-Year": ("UB", "ZB"),
}
ECOLOGY_BY_TERM = {
    "2-Year": "FRONT",
    "3-Year": "FRONT",
    "5-Year": "FRONT",
    "7-Year": "BELLY",
    "10-Year": "BELLY",
    "20-Year": "LONG",
    "30-Year": "LONG",
}


class AuctionConcessionError(RuntimeError):
    """The bounded tripwire cannot preserve its causal or immutable contract."""


@dataclass(frozen=True, slots=True)
class PolicySpec:
    lookback_minutes: int
    concession_threshold: float
    holding_minutes: int

    @property
    def policy_id(self) -> str:
        return "auction_concession_" + stable_hash(asdict(self))[:20]


def frozen_specs() -> tuple[PolicySpec, ...]:
    specs = tuple(
        PolicySpec(lookback, threshold, holding)
        for lookback in LOOKBACKS
        for threshold in THRESHOLDS
        for holding in HOLDING_MINUTES
    )
    if len(specs) != 24 or len({row.policy_id for row in specs}) != 24:
        raise AuctionConcessionError("the concession lattice must contain 24 unique specs")
    return specs


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(dict(payload), sort_keys=True, indent=2, allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise AuctionConcessionError(f"immutable artifact drift: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _atomic(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(dict(payload), sort_keys=True, indent=2, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _role(day: str) -> str | None:
    for role, (start, end) in ROLE_BOUNDS.items():
        if start <= day < end:
            return role
    return None


def _price_paths(root: Path) -> dict[str, Path]:
    return {
        symbol: root / PRICE_ROOT / f"{symbol.lower()}_explicit_contract_ohlcv.parquet"
        for symbol in sorted({leg for pair in TERM_PAIRS.values() for leg in pair})
    }


def freeze(root: Path, output: Path) -> dict[str, Any]:
    cemetery = {
        "post_result_demand": root
        / "reports/research_tripwires/treasury_auction_demand_shock_causal_v1/causal_result.json",
        "generic_two_leg_curve": root
        / "reports/research_tripwires/curve_relative_value_tripwire_v1/economic_result.json",
        "three_tenor_curvature": root
        / "reports/economic_evolution/autonomous_economic_discovery_director_0035_revision_02/branch_results/post_macro_branch_portfolio/treasury_three_tenor_curvature.json",
    }
    inputs = {
        "note_schedule": root / NOTE_INPUT,
        "bond_schedule": root / BOND_INPUT,
        "official_rule_snapshot": root / RULE_SNAPSHOT,
        **{f"price_{key}": value for key, value in _price_paths(root).items()},
        **{f"cemetery_{key}": value for key, value in cemetery.items()},
    }
    missing = [str(path) for path in inputs.values() if not path.is_file()]
    if missing:
        raise AuctionConcessionError(f"required immutable inputs missing: {missing}")
    cemetery_statuses = {
        "post_result_demand": json.loads(cemetery["post_result_demand"].read_text())["status"],
        "generic_two_leg_curve": json.loads(cemetery["generic_two_leg_curve"].read_text())["decision"],
        "three_tenor_curvature": json.loads(cemetery["three_tenor_curvature"].read_text())["status"],
    }
    decision_core = {
        "schema": "hydra_treasury_auction_concession_curve_rv_decision_v1",
        "status": "FROZEN_BEFORE_ECONOMIC_OUTCOMES",
        "hypothesis": (
            "A scheduled Treasury auction creates a pre-close relative concession in "
            "its maturity proxy that mean-reverts versus one adjacent-tenor hedge after "
            "a causal next-open entry."
        ),
        "strongest_argument_against": (
            "The concession may be an efficient supply-risk premium rather than a "
            "temporary dislocation, and two-leg costs may consume the unwind."
        ),
        "smallest_decisive_test": (
            "105 scheduled pre-Q4 auctions; 24 frozen lookback/threshold/horizon specs; "
            "discovery-only selection; chronological validation and final-development."
        ),
        "distinctness": {
            "post_result_demand": "uses no auction-result demand fields and enters before the result",
            "generic_two_leg_curve": "is event-conditioned on announced auction supply rather than an all-session z-score crossing",
            "three_tenor_curvature": "uses one target and one adjacent hedge, never a three-leg curvature residual",
        },
        "expected_data_cost_usd": 0.0,
        "next_materially_distinct_alternative": "CROSS_ASSET_MONTH_END_REBALANCING_FLOW",
    }
    decision = {**decision_core, "decision_hash": stable_hash(decision_core)}
    _write_once(output / "decision_card.json", decision)
    core = {
        "schema": "hydra_treasury_auction_concession_curve_rv_manifest_v1",
        "status": "FROZEN_BEFORE_ECONOMIC_OUTCOMES",
        "decision_hash": decision["decision_hash"],
        "inputs": {
            key: {"path": str(path.relative_to(root)), "sha256": _sha(path)}
            for key, path in sorted(inputs.items())
        },
        "cemetery_statuses": cemetery_statuses,
        "role_bounds": ROLE_BOUNDS,
        "term_pairs": TERM_PAIRS,
        "policy_specs": [asdict(row) for row in frozen_specs()],
        "policy_count": 24,
        "causal_contract": {
            "schedule_availability": "ANNOUNCEMENT_DATE_STRICTLY_PRECEDES_AUCTION_DECISION",
            "decision": "COMPLETED_1M_BAR_ENDING_FIVE_MINUTES_BEFORE_COMPETITIVE_CLOSE",
            "entry": "FIRST_TRADABLE_1M_OPEN_AT_OR_AFTER_DECISION_TIME_AND_BEFORE_CLOSE",
            "feature": "AUCTION_TARGET_MINUS_ADJACENT_HEDGE_PRE_CLOSE_STANDARDIZED_DISPLACEMENT",
            "action": "LONG_CHEAPENED_AUCTION_TARGET_SHORT_ADJACENT_HEDGE_OR_ABSTAIN",
            "exit": "FROZEN_TIME_EXIT_AT_NEXT_AVAILABLE_OPEN",
            "result_fields_in_decision": False,
        },
        "cheap_gate": {
            "minimum_materialized_events": 100,
            "positive_non_deployable_upper_bound_held_roles": True,
            "positive_selected_stressed_validation": True,
            "positive_selected_stressed_final_development": True,
            "minimum_positive_held_ecologies": 2,
            "maximum_positive_ecology_share": 0.75,
        },
        "selection": "MAX_DISCOVERY_STRESSED_NET_THEN_TRADE_COUNT_THEN_POLICY_ID",
        "exact_account_replay": "ONLY_AFTER_COMPLETE_HELD_GATE",
        "incremental_data_spend_usd": 0.0,
        "q4_access": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa": False,
        "worker_processes": 1,
        "numeric_threads": 1,
    }
    manifest = {**core, "manifest_hash": stable_hash(core)}
    _write_once(output / "manifest.json", manifest)
    return manifest


def _load_schedule(root: Path) -> list[dict[str, Any]]:
    raw: list[dict[str, Any]] = []
    for path in (root / NOTE_INPUT, root / BOND_INPUT):
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, list):
            raise AuctionConcessionError("official Treasury schedule input is not a list")
        raw.extend(value)
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in raw:
        term = str(row.get("term") or row.get("originalSecurityTerm") or "")
        day = str(row.get("auctionDate") or "")[:10]
        role = _role(day)
        closing = str(row.get("closingTimeCompetitive") or "").strip()
        if term not in TERM_PAIRS or role is None or closing not in {"01:00 PM", "11:30 AM"}:
            continue
        announcement_day = str(row.get("announcementDate") or "")[:10]
        if not announcement_day or not announcement_day < day:
            raise AuctionConcessionError("auction schedule was not available before decision")
        hour, minute = (13, 0) if closing == "01:00 PM" else (11, 30)
        auction_time = pd.Timestamp(f"{day} {hour:02d}:{minute:02d}:00", tz=NY).tz_convert(UTC)
        decision_cutoff = auction_time - pd.Timedelta(minutes=5)
        key = (day, term, str(row.get("cusip") or ""))
        selected[key] = {
            "event_id": "auction_schedule_" + stable_hash(key)[:20],
            "auction_date": day,
            "term": term,
            "ecology": ECOLOGY_BY_TERM[term],
            "role": role,
            "target_root": TERM_PAIRS[term][0],
            "hedge_root": TERM_PAIRS[term][1],
            "announcement_date": announcement_day,
            "competitive_close": auction_time,
            "decision_cutoff": decision_cutoff,
        }
    schedule = sorted(selected.values(), key=lambda row: (row["competitive_close"], row["term"], row["event_id"]))
    if len(schedule) < 100:
        raise AuctionConcessionError(f"fewer than 100 announced auction events: {len(schedule)}")
    return schedule


def _load_prices(root: Path, manifest: Mapping[str, Any]) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    for symbol, path in _price_paths(root).items():
        receipt = manifest["inputs"][f"price_{symbol}"]
        if _sha(path) != receipt["sha256"]:
            raise AuctionConcessionError(f"immutable price hash drift for {symbol}")
        frame = pd.read_parquet(
            path,
            columns=(
                "timestamp",
                "contract",
                "delivery_month",
                "open",
                "high",
                "low",
                "close",
                "session_id",
            ),
        ).sort_values("timestamp").reset_index(drop=True)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        if frame["timestamp"].duplicated().any():
            raise AuctionConcessionError(f"duplicate price timestamp for {symbol}")
        outputs[symbol] = frame
    return outputs


def _align_pair(prices: Mapping[str, pd.DataFrame], target: str, hedge: str) -> pd.DataFrame:
    def side(symbol: str, prefix: str) -> pd.DataFrame:
        frame = prices[symbol].copy()
        return frame.rename(
            columns={column: f"{prefix}_{column}" for column in frame.columns if column != "timestamp"}
        )

    output = side(target, "target").merge(
        side(hedge, "hedge"), on="timestamp", how="inner", validate="one_to_one"
    )
    if output.empty:
        raise AuctionConcessionError(f"empty pair alignment: {target}/{hedge}")
    same_delivery = output["target_delivery_month"].astype(str).eq(
        output["hedge_delivery_month"].astype(str)
    )
    output = output.loc[same_delivery].copy().reset_index(drop=True)
    same_session = output["target_session_id"].astype(str).eq(output["hedge_session_id"].astype(str))
    output = output.loc[same_session].copy().reset_index(drop=True)
    if output.empty:
        raise AuctionConcessionError(f"no synchronized-delivery rows: {target}/{hedge}")
    identity = (
        output["target_contract"].astype(str)
        + "|"
        + output["hedge_contract"].astype(str)
        + "|"
        + output["target_delivery_month"].astype(str)
    )
    output["segment"] = identity.ne(identity.shift()).cumsum().astype(int)
    target_delta = output["target_close"].astype(float).groupby(output["segment"]).diff()
    hedge_delta = output["hedge_close"].astype(float).groupby(output["segment"]).diff()
    output["target_dollar_sigma"] = (
        target_delta.mul(TREASURY_SPECS[target].point_value_usd)
        .groupby(output["segment"])
        .rolling(120, min_periods=60)
        .std(ddof=0)
        .reset_index(level=0, drop=True)
    )
    output["hedge_dollar_sigma"] = (
        hedge_delta.mul(TREASURY_SPECS[hedge].point_value_usd)
        .groupby(output["segment"])
        .rolling(120, min_periods=60)
        .std(ddof=0)
        .reset_index(level=0, drop=True)
    )
    return output


def _cost(target: str, hedge: str, target_qty: int, hedge_qty: int, *, stressed: bool) -> float:
    return float(
        target_qty * TREASURY_SPECS[target].round_turn_cost(stressed=stressed)
        + hedge_qty * TREASURY_SPECS[hedge].round_turn_cost(stressed=stressed)
    )


def _path_outcome(
    pair: pd.DataFrame,
    *,
    entry_index: int,
    holding_minutes: int,
    target: str,
    hedge: str,
    target_qty: int,
    hedge_qty: int,
    direction: int,
) -> dict[str, Any] | None:
    entry_time = pair.at[entry_index, "timestamp"]
    exit_target = entry_time + pd.Timedelta(minutes=holding_minutes)
    timestamps = pair["timestamp"].array.as_unit("ns").asi8
    exit_index = int(np.searchsorted(timestamps, exit_target.value, side="left"))
    if exit_index >= len(pair) or exit_index <= entry_index:
        return None
    if int(pair.at[entry_index, "segment"]) != int(pair.at[exit_index, "segment"]):
        return None
    if str(pair.at[entry_index, "target_session_id"]) != str(pair.at[exit_index, "target_session_id"]):
        return None
    target_value = TREASURY_SPECS[target].point_value_usd
    hedge_value = TREASURY_SPECS[hedge].point_value_usd
    entry_target = float(pair.at[entry_index, "target_open"])
    entry_hedge = float(pair.at[entry_index, "hedge_open"])
    exit_target_price = float(pair.at[exit_index, "target_open"])
    exit_hedge_price = float(pair.at[exit_index, "hedge_open"])
    gross = direction * (
        (exit_target_price - entry_target) * target_value * target_qty
        - (exit_hedge_price - entry_hedge) * hedge_value * hedge_qty
    )
    view = pair.iloc[entry_index : exit_index + 1]
    if direction > 0:
        adverse = (
            (view["target_low"].astype(float) - entry_target) * target_value * target_qty
            - (view["hedge_high"].astype(float) - entry_hedge) * hedge_value * hedge_qty
        )
        favorable = (
            (view["target_high"].astype(float) - entry_target) * target_value * target_qty
            - (view["hedge_low"].astype(float) - entry_hedge) * hedge_value * hedge_qty
        )
    else:
        adverse = -(
            (view["target_high"].astype(float) - entry_target) * target_value * target_qty
            - (view["hedge_low"].astype(float) - entry_hedge) * hedge_value * hedge_qty
        )
        favorable = -(
            (view["target_low"].astype(float) - entry_target) * target_value * target_qty
            - (view["hedge_high"].astype(float) - entry_hedge) * hedge_value * hedge_qty
        )
    return {
        "entry_time": entry_time.isoformat(),
        "exit_time": pair.at[exit_index, "timestamp"].isoformat(),
        "gross_usd": float(gross),
        "worst_gross_usd": float(min(0.0, adverse.min(), gross)),
        "best_gross_usd": float(max(0.0, favorable.max(), gross)),
    }


def materialize_events(
    root: Path, manifest: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    schedule = _load_schedule(root)
    prices = _load_prices(root, manifest)
    pair_frames = {
        pair: _align_pair(prices, *pair)
        for pair in sorted(set(TERM_PAIRS.values()))
    }
    events: list[dict[str, Any]] = []
    failures: Counter[str] = Counter()
    for item in schedule:
        pair_key = (str(item["target_root"]), str(item["hedge_root"]))
        pair = pair_frames[pair_key]
        timestamps = pair["timestamp"].array.as_unit("ns").asi8
        last_allowed_bar = item["decision_cutoff"] - pd.Timedelta(minutes=1)
        decision_index = int(np.searchsorted(timestamps, last_allowed_bar.value, side="right") - 1)
        if decision_index < 120:
            failures["INSUFFICIENT_PREHISTORY"] += 1
            continue
        decision_time = pair.at[decision_index, "timestamp"] + pd.Timedelta(minutes=1)
        entry_index = int(np.searchsorted(timestamps, decision_time.value, side="left"))
        if entry_index >= len(pair) or entry_index <= decision_index:
            failures["NO_NEXT_OPEN"] += 1
            continue
        if pair.at[entry_index, "timestamp"] >= item["competitive_close"]:
            failures["ENTRY_NOT_PRE_AUCTION"] += 1
            continue
        if int(pair.at[decision_index, "segment"]) != int(pair.at[entry_index, "segment"]):
            failures["ROLL_BOUNDARY"] += 1
            continue
        sigma_target = float(pair.at[decision_index, "target_dollar_sigma"])
        sigma_hedge = float(pair.at[decision_index, "hedge_dollar_sigma"])
        if not all(math.isfinite(value) and value > 0 for value in (sigma_target, sigma_hedge)):
            failures["INVALID_CAUSAL_SIGMA"] += 1
            continue
        target_qty = 1
        hedge_qty = max(1, min(4, int(round(sigma_target / sigma_hedge))))
        if target_qty + hedge_qty > 5:
            failures["CONTRACT_LIMIT"] += 1
            continue
        scores: dict[str, float] = {}
        valid = True
        for lookback in LOOKBACKS:
            start_target = pair.at[decision_index, "timestamp"] - pd.Timedelta(minutes=lookback)
            start_index = int(np.searchsorted(timestamps, start_target.value, side="right") - 1)
            if start_index < 0 or int(pair.at[start_index, "segment"]) != int(pair.at[decision_index, "segment"]):
                valid = False
                break
            target_move = (
                float(pair.at[decision_index, "target_close"])
                - float(pair.at[start_index, "target_close"])
            ) * TREASURY_SPECS[pair_key[0]].point_value_usd
            hedge_move = (
                float(pair.at[decision_index, "hedge_close"])
                - float(pair.at[start_index, "hedge_close"])
            ) * TREASURY_SPECS[pair_key[1]].point_value_usd
            score = target_move / (sigma_target * math.sqrt(lookback)) - hedge_move / (
                sigma_hedge * math.sqrt(lookback)
            )
            scores[str(lookback)] = float(score)
        if not valid:
            failures["LOOKBACK_CROSSES_ROLL"] += 1
            continue
        outcomes: dict[str, Any] = {}
        for holding in HOLDING_MINUTES:
            long = _path_outcome(
                pair,
                entry_index=entry_index,
                holding_minutes=holding,
                target=pair_key[0],
                hedge=pair_key[1],
                target_qty=target_qty,
                hedge_qty=hedge_qty,
                direction=1,
            )
            short = _path_outcome(
                pair,
                entry_index=entry_index,
                holding_minutes=holding,
                target=pair_key[0],
                hedge=pair_key[1],
                target_qty=target_qty,
                hedge_qty=hedge_qty,
                direction=-1,
            )
            if long is None or short is None:
                valid = False
                break
            normal_cost = _cost(*pair_key, target_qty, hedge_qty, stressed=False)
            stress_cost = _cost(*pair_key, target_qty, hedge_qty, stressed=True)
            outcomes[str(holding)] = {
                "LONG_TARGET": {
                    **long,
                    "normal_net_usd": float(long["gross_usd"] - normal_cost),
                    "stressed_net_usd": float(long["gross_usd"] - stress_cost),
                    "normal_cost_usd": normal_cost,
                    "stressed_cost_usd": stress_cost,
                },
                "SHORT_TARGET": {
                    **short,
                    "normal_net_usd": float(short["gross_usd"] - normal_cost),
                    "stressed_net_usd": float(short["gross_usd"] - stress_cost),
                    "normal_cost_usd": normal_cost,
                    "stressed_cost_usd": stress_cost,
                },
            }
        if not valid:
            failures["INCOMPLETE_EXIT_PATH"] += 1
            continue
        events.append(
            {
                **{
                    key: (value.isoformat() if isinstance(value, pd.Timestamp) else value)
                    for key, value in item.items()
                    if key not in {"decision_cutoff", "competitive_close"}
                },
                "competitive_close": item["competitive_close"].isoformat(),
                "decision_time": decision_time.isoformat(),
                "entry_time": pair.at[entry_index, "timestamp"].isoformat(),
                "target_contract": str(pair.at[entry_index, "target_contract"]),
                "hedge_contract": str(pair.at[entry_index, "hedge_contract"]),
                "target_quantity": target_qty,
                "hedge_quantity": hedge_qty,
                "concession_score": scores,
                "outcomes": outcomes,
                "feature_hash": stable_hash(
                    {
                        "decision_time": decision_time.isoformat(),
                        "target": pair_key[0],
                        "hedge": pair_key[1],
                        "scores": scores,
                        "sigma_target": sigma_target,
                        "sigma_hedge": sigma_hedge,
                    }
                ),
            }
        )
    audit = {
        "scheduled_events": len(schedule),
        "materialized_events": len(events),
        "by_role": {role: sum(row["role"] == role for row in events) for role in ROLES},
        "by_term": dict(sorted(Counter(row["term"] for row in events).items())),
        "by_ecology": dict(sorted(Counter(row["ecology"] for row in events).items())),
        "failures": dict(sorted(failures.items())),
        "future_result_fields_consumed": 0,
        "entry_after_decision_count": sum(row["entry_time"] >= row["decision_time"] for row in events),
        "entry_before_auction_count": sum(row["entry_time"] < row["competitive_close"] for row in events),
    }
    return events, audit


def _summary(values: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not values:
        return {
            "trade_count": 0,
            "gross_usd": 0.0,
            "normal_net_usd": 0.0,
            "stressed_net_usd": 0.0,
            "median_stressed_usd": 0.0,
            "positive_stressed_rate": 0.0,
        }
    return {
        "trade_count": len(values),
        "gross_usd": float(sum(float(row["gross_usd"]) for row in values)),
        "normal_net_usd": float(sum(float(row["normal_net_usd"]) for row in values)),
        "stressed_net_usd": float(sum(float(row["stressed_net_usd"]) for row in values)),
        "median_stressed_usd": float(np.median([float(row["stressed_net_usd"]) for row in values])),
        "positive_stressed_rate": float(
            sum(float(row["stressed_net_usd"]) > 0 for row in values) / len(values)
        ),
    }


def non_deployable_upper_bound(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    for event in events:
        choices = [
            {
                **event["outcomes"][str(holding)][direction],
                "role": event["role"],
                "ecology": event["ecology"],
                "term": event["term"],
                "holding_minutes": holding,
                "direction": direction,
            }
            for holding in HOLDING_MINUTES
            for direction in ("LONG_TARGET", "SHORT_TARGET")
        ]
        selected.append(max(choices, key=lambda row: (float(row["stressed_net_usd"]), -int(row["holding_minutes"]))))
    by_role = {role: _summary([row for row in selected if row["role"] == role]) for role in ROLES}
    by_ecology = {
        ecology: _summary([row for row in selected if row["ecology"] == ecology])
        for ecology in sorted({str(row["ecology"]) for row in selected})
    }
    return {
        "status": "NON_DEPLOYABLE_FULL_OUTCOME_DIRECTION_AND_HORIZON_UPPER_BOUND",
        "event_count": len(selected),
        "aggregate": _summary(selected),
        "by_role": by_role,
        "by_ecology": by_ecology,
        "promotion_allowed": False,
    }


def evaluate_policy(events: Sequence[Mapping[str, Any]], spec: PolicySpec) -> dict[str, Any]:
    trades: list[dict[str, Any]] = []
    for event in events:
        score = float(event["concession_score"][str(spec.lookback_minutes)])
        if score > -spec.concession_threshold:
            continue
        outcome = event["outcomes"][str(spec.holding_minutes)]["LONG_TARGET"]
        trades.append(
            {
                "event_id": event["event_id"],
                "auction_date": event["auction_date"],
                "role": event["role"],
                "term": event["term"],
                "ecology": event["ecology"],
                "decision_time": event["decision_time"],
                "entry_time": outcome["entry_time"],
                "exit_time": outcome["exit_time"],
                "target_root": event["target_root"],
                "hedge_root": event["hedge_root"],
                "target_quantity": event["target_quantity"],
                "hedge_quantity": event["hedge_quantity"],
                "concession_score": score,
                **outcome,
            }
        )
    by_role = {role: _summary([row for row in trades if row["role"] == role]) for role in ROLES}
    by_ecology = {
        ecology: _summary([row for row in trades if row["ecology"] == ecology])
        for ecology in ("FRONT", "BELLY", "LONG")
    }
    core = {
        "policy_id": spec.policy_id,
        "spec": asdict(spec),
        "trade_count": len(trades),
        "by_role": by_role,
        "by_ecology": by_ecology,
        "aggregate": _summary(trades),
    }
    return {**core, "candidate_hash": stable_hash(core), "_trades": trades}


def _selected_policy(candidates: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    return max(
        candidates,
        key=lambda row: (
            float(row["by_role"]["DISCOVERY"]["stressed_net_usd"]),
            int(row["by_role"]["DISCOVERY"]["trade_count"]),
            str(row["policy_id"]),
        ),
    )


def _active_policy_audit(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Keep inactivity from being mistaken for economic evidence.

    The frozen numerical selector is reported exactly as preregistered.  This
    separate validity audit does not change that selector or authorize replay;
    it identifies the best candidate that actually acted in discovery and
    makes the no-trade degeneracy explicit.
    """

    active = [
        row
        for row in candidates
        if int(row["by_role"]["DISCOVERY"]["trade_count"]) > 0
    ]
    best = (
        max(
            active,
            key=lambda row: (
                float(row["by_role"]["DISCOVERY"]["stressed_net_usd"]),
                int(row["by_role"]["DISCOVERY"]["trade_count"]),
                str(row["policy_id"]),
            ),
        )
        if active
        else None
    )
    held_positive = [
        row
        for row in active
        if float(row["by_role"]["VALIDATION"]["stressed_net_usd"]) > 0
        and float(row["by_role"]["FINAL_DEVELOPMENT"]["stressed_net_usd"]) > 0
    ]
    return {
        "inactive_policy_count": len(candidates) - len(active),
        "active_policy_count": len(active),
        "all_active_discovery_stressed_negative": bool(
            active
            and all(
                float(row["by_role"]["DISCOVERY"]["stressed_net_usd"]) < 0
                for row in active
            )
        ),
        "held_stress_positive_active_policy_count": len(held_positive),
        "best_active_policy": (
            {key: value for key, value in best.items() if key != "_trades"}
            if best is not None
            else None
        ),
        "selection_changed": False,
        "promotion_allowed": False,
    }


def held_gate(
    selected: Mapping[str, Any], upper: Mapping[str, Any], materialized_event_count: int
) -> dict[str, Any]:
    held_trades = [
        row
        for row in selected["_trades"]
        if row["role"] in {"VALIDATION", "FINAL_DEVELOPMENT"}
    ]
    ecology_net = {
        ecology: float(sum(float(row["stressed_net_usd"]) for row in held_trades if row["ecology"] == ecology))
        for ecology in ("FRONT", "BELLY", "LONG")
    }
    positive = {key: value for key, value in ecology_net.items() if value > 0}
    positive_total = sum(positive.values())
    maximum_share = max(positive.values(), default=0.0) / positive_total if positive_total > 0 else 1.0
    checks = {
        "minimum_100_materialized_events": materialized_event_count >= 100,
        "upper_bound_validation_positive": float(upper["by_role"]["VALIDATION"]["stressed_net_usd"]) > 0,
        "upper_bound_final_positive": float(upper["by_role"]["FINAL_DEVELOPMENT"]["stressed_net_usd"]) > 0,
        "selected_validation_positive": float(selected["by_role"]["VALIDATION"]["stressed_net_usd"]) > 0,
        "selected_final_positive": float(selected["by_role"]["FINAL_DEVELOPMENT"]["stressed_net_usd"]) > 0,
        "two_positive_held_ecologies": len(positive) >= 2,
        "no_single_ecology_domination": maximum_share <= 0.75,
    }
    return {
        "checks": checks,
        "pass": all(checks.values()),
        "held_ecology_stressed_net_usd": ecology_net,
        "maximum_positive_ecology_share": float(maximum_share),
        "held_trade_count": len(held_trades),
    }


def _exact_account(selected: Mapping[str, Any], root: Path) -> dict[str, Any]:
    rules, receipt = _load_rule_snapshot(root / RULE_SNAPSHOT)
    account = rules["combine"]["50K"]
    config = _account_config(account)
    by_scenario: dict[str, list[TradePathEvent]] = {scenario: [] for scenario in SCENARIOS}
    for row in selected["_trades"]:
        day = pd.Timestamp(row["auction_date"]).date().toordinal()
        for scenario in SCENARIOS:
            net_key = "normal_net_usd" if scenario == "NORMAL" else "stressed_net_usd"
            cost_key = "normal_cost_usd" if scenario == "NORMAL" else "stressed_cost_usd"
            cost = float(row[cost_key])
            by_scenario[scenario].append(
                TradePathEvent(
                    event_id=f"{selected['policy_id']}:{row['event_id']}:{scenario}",
                    decision_ns=pd.Timestamp(row["decision_time"]).value,
                    exit_ns=pd.Timestamp(row["exit_time"]).value,
                    session_day=day,
                    net_pnl=float(row[net_key]),
                    gross_pnl=float(row["gross_usd"]),
                    worst_unrealized_pnl=float(row["worst_gross_usd"] - cost),
                    best_unrealized_pnl=float(row["best_gross_usd"] - cost),
                    quantity=int(row["target_quantity"] + row["hedge_quantity"]),
                    mini_equivalent=float(row["target_quantity"] + row["hedge_quantity"]),
                    regime=f"AUCTION_CONCESSION:{row['ecology']}:{row['term']}",
                    session_compliant=True,
                    contract_limit_compliant=(row["target_quantity"] + row["hedge_quantity"]) <= 5,
                )
            )
    output: dict[str, Any] = {"rule_receipt": receipt, "account_label": "50K", "scenarios": {}}
    for scenario, events in by_scenario.items():
        role_rows: dict[str, Any] = {}
        for role, (start, end) in ROLE_BOUNDS.items():
            days = [
                pd.Timestamp(day).date().toordinal()
                for day in pd.bdate_range(start, pd.Timestamp(end) - pd.Timedelta(days=1))
            ]
            horizons: dict[str, Any] = {}
            for horizon in (5, 10, 20):
                starts = [days[index] for index in range(0, len(days), horizon) if index + horizon <= len(days)]
                episodes = [
                    run_combine_episode(
                        events,
                        days,
                        start_day=start_day,
                        maximum_duration_days=horizon,
                        config=config,
                        maximum_mini_equivalent=5.0,
                    )
                    for start_day in starts
                ]
                horizons[str(horizon)] = {
                    "episodes": len(episodes),
                    "passes": sum(row.passed for row in episodes),
                    "mll_breaches": sum(row.mll_breached for row in episodes),
                    "net_total_usd": float(sum(row.net_pnl for row in episodes)),
                    "median_target_progress": float(np.median([row.target_progress for row in episodes])) if episodes else 0.0,
                    "minimum_mll_buffer_usd": float(min(row.minimum_mll_buffer for row in episodes)) if episodes else None,
                    "terminal_distribution": dict(sorted(Counter(row.terminal.value for row in episodes).items())),
                }
            role_rows[role] = horizons
        output["scenarios"][scenario] = role_rows
    return output


def run(
    root: Path = ROOT, output: Path | None = None
) -> dict[str, Any]:
    project = root.resolve()
    target = (project / (output or OUTPUT)).resolve() if not (output or OUTPUT).is_absolute() else (output or OUTPUT).resolve()
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    manifest = freeze(project, target)
    _atomic(target / "production_state.json", {"status": "MATERIALIZING_CAUSAL_PRE_AUCTION_EVENTS"})
    events, audit = materialize_events(project, manifest)
    event_text = "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in events)
    event_path = target / "causal_event_ledger_revision_01.jsonl"
    if event_path.exists() and event_path.read_text(encoding="utf-8") != event_text:
        raise AuctionConcessionError("immutable event ledger drift")
    event_path.write_text(event_text, encoding="utf-8")
    _atomic(target / "production_state.json", {"status": "CHEAP_GROSS_COST_AND_UPPER_BOUND"})
    upper = non_deployable_upper_bound(events)
    candidates = [evaluate_policy(events, spec) for spec in frozen_specs()]
    selected = _selected_policy(candidates)
    active_audit = _active_policy_audit(candidates)
    gate = held_gate(selected, upper, len(events))
    exact = _exact_account(selected, project) if gate["pass"] else {
        "status": "BLOCKED_BY_HELD_ECONOMIC_GATE",
        "exact_account_replays": 0,
    }
    public_candidates = [{key: value for key, value in row.items() if key != "_trades"} for row in candidates]
    selected_public = {key: value for key, value in selected.items() if key != "_trades"}
    if not gate["pass"]:
        verdict = "TREASURY_AUCTION_CONCESSION_CURVE_RV_FALSIFIED"
    else:
        stress = exact["scenarios"]["STRESSED_1_5X"]
        passes = sum(stress[role][horizon]["passes"] for role in ROLES for horizon in ("5", "10", "20"))
        verdict = (
            "TREASURY_AUCTION_CONCESSION_CURVE_RV_TIER_E_DIAGNOSTIC"
            if passes > 0
            else "TREASURY_AUCTION_CONCESSION_CURVE_RV_WEAK"
        )
    core = {
        "schema": "hydra_treasury_auction_concession_curve_rv_result_v1",
        "status": verdict,
        "evidence_role": "VIEWED_DEVELOPMENT_TRIPWIRE_ONLY",
        "manifest_hash": manifest["manifest_hash"],
        "event_ledger": {
            "path": str(event_path.relative_to(project)),
            "sha256": _sha(event_path),
            "event_count": len(events),
        },
        "causal_audit": audit,
        "non_deployable_upper_bound": upper,
        "policy_count": len(candidates),
        "selection_rule": manifest["selection"],
        "selected_policy": selected_public,
        "active_policy_validity_audit": active_audit,
        "held_gate": gate,
        "candidate_results": public_candidates,
        "exact_account": exact,
        "incremental_data_spend_usd": 0.0,
        "q4_access": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths": 0,
        "tier_q_allowed": False,
        "next_action": (
            "FREEZE_TIER_E_AND_RUN_UNSEEN_CONFIRMATION"
            if verdict.endswith("TIER_E_DIAGNOSTIC")
            else "TOMBSTONE_PRE_AUCTION_ADJACENT_TENOR_CONCESSION_NO_NEIGHBOR_RETRY"
        ),
    }
    result = {**core, "result_hash": stable_hash(core)}
    result_path = target / "economic_result_revision_01.json"
    _write_once(result_path, result)
    _atomic(target / "production_state.json", {
        "status": "COMPLETE",
        "verdict": verdict,
        "result_hash": result["result_hash"],
        "materialized_events": len(events),
        "policy_count": len(candidates),
        "exact_account_replays": 0 if not gate["pass"] else 1,
        "authoritative_result_path": str(result_path.relative_to(project)),
    })
    return result


if __name__ == "__main__":
    print(json.dumps(run(), sort_keys=True))
