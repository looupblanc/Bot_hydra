from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.research.v7_hypothesis_grammar import (
    MARKETS,
    MINUTE_NS,
    V7CandidateSpec,
    V7GrammarError,
    V7MarketBars,
    V7Signal,
    assert_class_distance,
    load_v7_market_bars,
)


GRAMMAR_ID = "hydra_v7_grammar_0004_cross_sectional_scheduled_flow"
PREREGISTRATION_SHA256 = (
    "e2d34f38635a203b1157ad78f541e53aa378f930a24a7735a3eff6020ce6ed74"
)
EQUITY_MARKETS = ("ES", "NQ", "RTY", "YM")


@dataclass(frozen=True, slots=True)
class WindowReturn:
    session_day: int
    start_index: int
    end_index: int
    log_return: float


def candidate_specs() -> tuple[V7CandidateSpec, ...]:
    rows: list[tuple[str, str, str, str, str | None, str, int, str]] = []
    breadth_economic = (
        "Index-arbitrage desks and broad ETF hedgers transmit a strong majority "
        "move from three equity futures into the lagging fourth future after "
        "the opening hour."
    )
    for market in EQUITY_MARKETS:
        rows.append(
            (
                f"v7g4_breadth_catchup_{market}",
                "H17_INDEX_BREADTH_CATCHUP",
                "intraday_index_breadth_catchup",
                market,
                "INDEX_PEERS",
                "consensus",
                299,
                breadth_economic,
            )
        )
    leadership_economic = (
        "An extreme opening NQ-minus-RTY leadership spread is partly "
        "inventory-driven and compresses as cross-index hedgers rebalance "
        "before the close."
    )
    rows.extend(
        [
            (
                "v7g4_leadership_reversion_NQ",
                "H18_INDEX_LEADERSHIP_SPREAD_REVERSION",
                "extreme_index_leadership_spread_reversion",
                "NQ",
                "RTY",
                "opposite_spread",
                269,
                leadership_economic,
            ),
            (
                "v7g4_leadership_reversion_RTY",
                "H18_INDEX_LEADERSHIP_SPREAD_REVERSION",
                "extreme_index_leadership_spread_reversion",
                "RTY",
                "NQ",
                "same_spread",
                269,
                leadership_economic,
            ),
        ]
    )
    gamma_economic = (
        "Friday weekly-option gamma hedging counteracts an unusually large "
        "first-hour ES or NQ move during the remaining cash session."
    )
    for market in ("ES", "NQ"):
        rows.append(
            (
                f"v7g4_friday_gamma_{market}",
                "H19_WEEKLY_OPTION_GAMMA_REVERSION",
                "weekly_option_gamma_hedge_reversion",
                market,
                None,
                "opposite",
                299,
                gamma_economic,
            )
        )
    macro_economic = (
        "When related ecologies confirm a large pre-open macro repricing "
        "synchronously, commercial and allocation hedges continue the target "
        "market move after the equity cash open."
    )
    rows.extend(
        [
            (
                "v7g4_macro_confirmation_CL",
                "H20_SYNCHRONOUS_MACRO_CONFIRMATION",
                "synchronous_cross_ecology_macro_completion",
                "CL",
                "ES",
                "same",
                209,
                macro_economic,
            ),
            (
                "v7g4_macro_confirmation_GC",
                "H20_SYNCHRONOUS_MACRO_CONFIRMATION",
                "synchronous_cross_ecology_macro_completion",
                "GC",
                "ES",
                "risk_off",
                209,
                macro_economic,
            ),
            (
                "v7g4_eia_drift_CL",
                "H21_EIA_INVENTORY_DRIFT",
                "scheduled_energy_inventory_information_drift",
                "CL",
                None,
                "same",
                144,
                "Large Wednesday crude repricing around the nominal EIA "
                "inventory release continues while physical and macro hedgers "
                "digest the inventory surprise.",
            ),
        ]
    )
    specs: list[V7CandidateSpec] = []
    for candidate_id, hypothesis_id, mechanism, market, source, relation, hold, economic in rows:
        payload = {
            "grammar_id": GRAMMAR_ID,
            "candidate_id": candidate_id,
            "hypothesis_id": hypothesis_id,
            "mechanism_class": mechanism,
            "market": market,
            "source_market": source,
            "side_relation": relation,
            "holding_minutes": hold,
            "economic_hypothesis": economic,
            "preregistration_sha256": PREREGISTRATION_SHA256,
        }
        specs.append(
            V7CandidateSpec(
                candidate_id=candidate_id,
                hypothesis_id=hypothesis_id,
                mechanism_class=mechanism,
                market=market,
                source_market=source,
                side_relation=relation,
                holding_minutes=hold,
                economic_hypothesis=economic,
                specification_hash=_stable_hash(payload),
            )
        )
    if len(specs) != 11 or len({row.candidate_id for row in specs}) != 11:
        raise V7GrammarError("grammar 0004 must contain 11 unique structures")
    return tuple(specs)


def generate_signal_population(
    bars_by_market: Mapping[str, V7MarketBars],
    *,
    graveyard_path: str | Path | None = "mission/state/graveyard.db",
) -> dict[str, tuple[V7Signal, ...]]:
    if set(bars_by_market) != set(MARKETS):
        raise V7GrammarError("grammar 0004 requires all six frozen markets")
    specs = {row.candidate_id: row for row in candidate_specs()}
    if graveyard_path is not None:
        assert_class_distance(graveyard_path, tuple(specs.values()))

    first_hour = {
        market: _window_returns(bars_by_market[market], 8 * 60 + 30, 9 * 60 + 30)
        for market in EQUITY_MARKETS
    }
    leadership = {
        market: _window_returns(bars_by_market[market], 8 * 60 + 30, 10 * 60)
        for market in ("NQ", "RTY")
    }
    preopen = {
        market: _window_returns(bars_by_market[market], 7 * 60 + 30, 8 * 60 + 30)
        for market in ("ES", "CL", "GC")
    }
    eia = _window_returns(bars_by_market["CL"], 9 * 60 + 30, 9 * 60 + 35)

    output: dict[str, tuple[V7Signal, ...]] = {}
    for market in EQUITY_MARKETS:
        candidate_id = f"v7g4_breadth_catchup_{market}"
        output[candidate_id] = tuple(
            _breadth_signals(
                specs[candidate_id], bars_by_market, first_hour, target=market
            )
        )
    for market in ("NQ", "RTY"):
        candidate_id = f"v7g4_leadership_reversion_{market}"
        output[candidate_id] = tuple(
            _leadership_signals(
                specs[candidate_id], bars_by_market, leadership, target=market
            )
        )
    for market in ("ES", "NQ"):
        candidate_id = f"v7g4_friday_gamma_{market}"
        output[candidate_id] = tuple(
            _friday_gamma_signals(
                specs[candidate_id], bars_by_market[market], first_hour[market]
            )
        )
    output["v7g4_macro_confirmation_CL"] = tuple(
        _macro_confirmation_signals(
            specs["v7g4_macro_confirmation_CL"],
            bars_by_market,
            preopen,
            target="CL",
        )
    )
    output["v7g4_macro_confirmation_GC"] = tuple(
        _macro_confirmation_signals(
            specs["v7g4_macro_confirmation_GC"],
            bars_by_market,
            preopen,
            target="GC",
        )
    )
    output["v7g4_eia_drift_CL"] = tuple(
        _eia_signals(specs["v7g4_eia_drift_CL"], bars_by_market["CL"], eia)
    )
    if set(output) != set(specs):
        raise V7GrammarError("grammar 0004 population drifted from WORM scope")
    for candidate_id, signals in output.items():
        _validate_signal_sequence(specs[candidate_id], signals, bars_by_market)
    return dict(sorted(output.items()))


def _breadth_signals(
    spec: V7CandidateSpec,
    bars_by_market: Mapping[str, V7MarketBars],
    returns: Mapping[str, Mapping[int, WindowReturn]],
    *,
    target: str,
) -> list[V7Signal]:
    peers = tuple(market for market in EQUITY_MARKETS if market != target)
    days = sorted(set.intersection(*(set(returns[market]) for market in EQUITY_MARKETS)))
    history: list[float] = []
    output: list[V7Signal] = []
    target_bars = bars_by_market[target]
    for day in days:
        peer_values = [returns[market][day].log_return for market in peers]
        peer_abs_median = float(np.median(np.abs(peer_values)))
        threshold = float(np.median(history[-60:])) if len(history) >= 60 else None
        signs = [int(math.copysign(1, value)) for value in peer_values if value != 0.0]
        consensus = 1 if signs.count(1) >= 2 else -1 if signs.count(-1) >= 2 else 0
        target_return = returns[target][day].log_return
        execution = _execution(target_bars, day, 9 * 60 + 30, 9 * 60 + 31, 14 * 60 + 30)
        source_availability = max(
            int(bars_by_market[market].availability_ns[returns[market][day].end_index])
            for market in EQUITY_MARKETS
        )
        if (
            threshold is not None
            and consensus != 0
            and peer_abs_median > threshold
            and abs(target_return) < peer_abs_median
            and execution is not None
            and source_availability <= int(target_bars.decision_ns[execution[0]])
        ):
            output.append(
                _make_signal(
                    spec,
                    target_bars,
                    day,
                    consensus,
                    *execution,
                    availability_ns=source_availability,
                    feature_snapshot={
                        "peer_markets": peers,
                        "peer_returns": peer_values,
                        "peer_abs_median": peer_abs_median,
                        "past_60_median": threshold,
                        "target_return": target_return,
                    },
                )
            )
        history.append(peer_abs_median)
    return output


def _leadership_signals(
    spec: V7CandidateSpec,
    bars_by_market: Mapping[str, V7MarketBars],
    returns: Mapping[str, Mapping[int, WindowReturn]],
    *,
    target: str,
) -> list[V7Signal]:
    days = sorted(set(returns["NQ"]) & set(returns["RTY"]))
    history: list[float] = []
    output: list[V7Signal] = []
    target_bars = bars_by_market[target]
    for day in days:
        spread = returns["NQ"][day].log_return - returns["RTY"][day].log_return
        threshold = float(np.quantile(history[-60:], 0.75)) if len(history) >= 60 else None
        execution = _execution(target_bars, day, 10 * 60, 10 * 60 + 1, 14 * 60 + 30)
        source_availability = max(
            int(bars_by_market[market].availability_ns[returns[market][day].end_index])
            for market in ("NQ", "RTY")
        )
        if (
            threshold is not None
            and abs(spread) > threshold
            and spread != 0.0
            and execution is not None
            and source_availability <= int(target_bars.decision_ns[execution[0]])
        ):
            spread_side = int(math.copysign(1, spread))
            side = -spread_side if target == "NQ" else spread_side
            output.append(
                _make_signal(
                    spec,
                    target_bars,
                    day,
                    side,
                    *execution,
                    availability_ns=source_availability,
                    feature_snapshot={
                        "nq_return": returns["NQ"][day].log_return,
                        "rty_return": returns["RTY"][day].log_return,
                        "spread": spread,
                        "past_60_q75": threshold,
                    },
                )
            )
        history.append(abs(spread))
    return output


def _friday_gamma_signals(
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    returns: Mapping[int, WindowReturn],
) -> list[V7Signal]:
    history: list[float] = []
    output: list[V7Signal] = []
    for day in sorted(returns):
        metric = returns[day]
        if int(bars.local_weekday[metric.end_index]) != 4:
            continue
        threshold = float(np.median(history[-20:])) if len(history) >= 20 else None
        execution = _execution(bars, day, 9 * 60 + 30, 9 * 60 + 31, 14 * 60 + 30)
        if (
            threshold is not None
            and abs(metric.log_return) > threshold
            and metric.log_return != 0.0
            and execution is not None
        ):
            output.append(
                _make_signal(
                    spec,
                    bars,
                    day,
                    -int(math.copysign(1, metric.log_return)),
                    *execution,
                    availability_ns=int(bars.availability_ns[metric.end_index]),
                    feature_snapshot={
                        "first_hour_return": metric.log_return,
                        "prior_20_friday_median": threshold,
                    },
                )
            )
        history.append(abs(metric.log_return))
    return output


def _macro_confirmation_signals(
    spec: V7CandidateSpec,
    bars_by_market: Mapping[str, V7MarketBars],
    returns: Mapping[str, Mapping[int, WindowReturn]],
    *,
    target: str,
) -> list[V7Signal]:
    days = sorted(set(returns["ES"]) & set(returns[target]))
    history_es: list[float] = []
    history_target: list[float] = []
    output: list[V7Signal] = []
    target_bars = bars_by_market[target]
    for day in days:
        es_return = returns["ES"][day].log_return
        target_return = returns[target][day].log_return
        es_threshold = float(np.median(history_es[-60:])) if len(history_es) >= 60 else None
        target_threshold = (
            float(np.median(history_target[-60:])) if len(history_target) >= 60 else None
        )
        relation = es_return * target_return
        relation_pass = relation > 0.0 if target == "CL" else relation < 0.0
        execution = _execution(target_bars, day, 8 * 60 + 30, 8 * 60 + 31, 12 * 60)
        source_availability = max(
            int(bars_by_market["ES"].availability_ns[returns["ES"][day].end_index]),
            int(target_bars.availability_ns[returns[target][day].end_index]),
        )
        if (
            es_threshold is not None
            and target_threshold is not None
            and relation_pass
            and abs(es_return) > es_threshold
            and abs(target_return) > target_threshold
            and target_return != 0.0
            and execution is not None
            and source_availability <= int(target_bars.decision_ns[execution[0]])
        ):
            output.append(
                _make_signal(
                    spec,
                    target_bars,
                    day,
                    int(math.copysign(1, target_return)),
                    *execution,
                    availability_ns=source_availability,
                    feature_snapshot={
                        "es_return": es_return,
                        "target_return": target_return,
                        "es_past_60_median": es_threshold,
                        "target_past_60_median": target_threshold,
                    },
                )
            )
        history_es.append(abs(es_return))
        history_target.append(abs(target_return))
    return output


def _eia_signals(
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    returns: Mapping[int, WindowReturn],
) -> list[V7Signal]:
    history: list[float] = []
    output: list[V7Signal] = []
    for day in sorted(returns):
        metric = returns[day]
        if int(bars.local_weekday[metric.end_index]) != 2:
            continue
        threshold = float(np.median(history[-20:])) if len(history) >= 20 else None
        execution = _execution(bars, day, 9 * 60 + 35, 9 * 60 + 36, 12 * 60)
        if (
            threshold is not None
            and abs(metric.log_return) > threshold
            and metric.log_return != 0.0
            and execution is not None
        ):
            output.append(
                _make_signal(
                    spec,
                    bars,
                    day,
                    int(math.copysign(1, metric.log_return)),
                    *execution,
                    availability_ns=int(bars.availability_ns[metric.end_index]),
                    feature_snapshot={
                        "release_window_return": metric.log_return,
                        "prior_20_wednesday_median": threshold,
                        "holiday_shift_weeks_included_as_noise": True,
                    },
                )
            )
        history.append(abs(metric.log_return))
    return output


def _window_returns(
    bars: V7MarketBars, start_minute: int, end_minute: int
) -> dict[int, WindowReturn]:
    output: dict[int, WindowReturn] = {}
    for day, positions in _day_positions(bars).items():
        window = _exact_window(bars, positions, start_minute, end_minute)
        if window is None:
            continue
        start, end = window
        open_price = float(bars.open[start])
        close_price = float(bars.close[end])
        if open_price <= 0.0 or close_price <= 0.0:
            continue
        output[day] = WindowReturn(
            session_day=day,
            start_index=start,
            end_index=end,
            log_return=float(math.log(close_price / open_price)),
        )
    return output


def _execution(
    bars: V7MarketBars,
    day: int,
    decision_minute: int,
    entry_minute: int,
    exit_minute: int,
) -> tuple[int, int, int] | None:
    positions = _day_positions(bars).get(day)
    if positions is None:
        return None
    decision = _at_minute(bars, positions, decision_minute)
    entry = _at_minute(bars, positions, entry_minute)
    exit_index = _at_minute(bars, positions, exit_minute)
    if decision is None or entry is None or exit_index is None:
        return None
    if not (decision < entry < exit_index) or not _same_segment(bars, decision, exit_index):
        return None
    return decision, entry, exit_index


def _make_signal(
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    day: int,
    side: int,
    decision_index: int,
    entry_index: int,
    exit_index: int,
    *,
    availability_ns: int,
    feature_snapshot: Mapping[str, Any],
) -> V7Signal:
    decision_ns = int(bars.decision_ns[decision_index])
    if availability_ns > decision_ns:
        raise V7GrammarError("grammar 0004 cross-market feature is unavailable")
    return V7Signal(
        candidate_id=spec.candidate_id,
        hypothesis_id=spec.hypothesis_id,
        market=spec.market,
        source_market=spec.source_market,
        session_day=int(day),
        side=int(side),
        decision_ns=decision_ns,
        availability_ns=int(availability_ns),
        entry_index=int(entry_index),
        exit_index=int(exit_index),
        entry_ns=int(bars.timestamp_ns[entry_index]),
        exit_ns=int(bars.timestamp_ns[exit_index] + MINUTE_NS),
        contract_code=int(bars.contract_code[entry_index]),
        segment_code=int(bars.segment_code[entry_index]),
        feature_snapshot_hash=_stable_hash(dict(feature_snapshot)),
    )


def _validate_signal_sequence(
    spec: V7CandidateSpec,
    signals: Sequence[V7Signal],
    bars_by_market: Mapping[str, V7MarketBars],
) -> None:
    bars = bars_by_market[spec.market]
    previous_exit = -1
    identities: set[tuple[int, int]] = set()
    for signal in signals:
        if signal.candidate_id != spec.candidate_id or signal.market != spec.market:
            raise V7GrammarError("grammar 0004 signal identity drift")
        if signal.entry_index <= previous_exit:
            raise V7GrammarError("grammar 0004 signals overlap")
        previous_exit = signal.exit_index
        identity = (signal.session_day, signal.decision_ns)
        if identity in identities:
            raise V7GrammarError("grammar 0004 duplicate signal")
        identities.add(identity)
        if not _same_segment(bars, signal.entry_index, signal.exit_index):
            raise V7GrammarError("grammar 0004 signal crosses contract or gap")
        if int(bars.local_minute[signal.exit_index]) > 15 * 60 + 9:
            raise V7GrammarError("grammar 0004 signal violates flatten")


def _day_positions(bars: V7MarketBars) -> dict[int, np.ndarray]:
    days, starts, counts = np.unique(
        bars.session_day, return_index=True, return_counts=True
    )
    return {
        int(day): np.arange(int(start), int(start + count), dtype=np.int64)
        for day, start, count in zip(days, starts, counts, strict=True)
    }


def _at_minute(
    bars: V7MarketBars, positions: np.ndarray, minute: int
) -> int | None:
    matches = positions[bars.local_minute[positions] == int(minute)]
    return int(matches[0]) if len(matches) == 1 else None


def _exact_window(
    bars: V7MarketBars,
    positions: np.ndarray,
    start_minute: int,
    end_minute: int,
) -> tuple[int, int] | None:
    start = _at_minute(bars, positions, start_minute)
    end = _at_minute(bars, positions, end_minute)
    if start is None or end is None or start >= end:
        return None
    if end - start + 1 != end_minute - start_minute + 1:
        return None
    if not _same_segment(bars, start, end):
        return None
    return start, end


def _same_segment(bars: V7MarketBars, start: int, end: int) -> bool:
    if start < 0 or end >= bars.row_count or start > end:
        return False
    return bool(
        np.all(bars.segment_code[start : end + 1] == bars.segment_code[start])
        and np.all(bars.contract_code[start : end + 1] == bars.contract_code[start])
        and np.all(np.diff(bars.timestamp_ns[start : end + 1]) == MINUTE_NS)
    )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


__all__ = [
    "GRAMMAR_ID",
    "PREREGISTRATION_SHA256",
    "candidate_specs",
    "generate_signal_population",
    "load_v7_market_bars",
]
