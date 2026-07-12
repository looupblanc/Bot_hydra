from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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


GRAMMAR_ID = "hydra_v7_grammar_0003_multiday_calendar_transfer"
PREREGISTRATION_SHA256 = (
    "e2f28a415ebd3676df917df1c37a072ab2829a3df7ef33ad438ec0e4312a31e5"
)
EQUITY_MARKETS = ("ES", "NQ", "RTY", "YM")


@dataclass(frozen=True, slots=True)
class RthSessionMetric:
    session_day: int
    start_index: int
    end_index: int
    open_price: float
    close_price: float
    log_return: float


def candidate_specs() -> tuple[V7CandidateSpec, ...]:
    rows: list[tuple[str, str, str, str, str | None, str, str]] = []
    for market in EQUITY_MARKETS:
        rows.append(
            (
                f"v7g3_turn_month_{market}",
                "H11_TURN_OF_MONTH_INDEX_FLOW",
                "turn_of_month_index_allocation_flow",
                market,
                None,
                "long",
                "Predictable pension, payroll and index cash flows create buy demand around month boundaries.",
            )
        )
    for market in MARKETS:
        rows.append(
            (
                f"v7g3_underreaction_{market}",
                "H12_PRIOR_SESSION_UNDERREACTION",
                "prior_session_information_underreaction",
                market,
                None,
                "same",
                "Large prior-session information and parent orders remain partly unfinished into the next cash session.",
            )
        )
        rows.append(
            (
                f"v7g3_two_day_reversal_{market}",
                "H13_TWO_DAY_LIQUIDITY_REVERSAL",
                "two_day_liquidity_shock_reversal",
                market,
                None,
                "opposite",
                "Temporary inventory accumulated over an extreme two-session move unwinds on the third session.",
            )
        )
    rows.extend(
        [
            (
                "v7g3_transfer_CL_GC",
                "H14_CL_TO_GC_INFLATION_TRANSFER",
                "lagged_energy_to_gold_inflation_transfer",
                "GC",
                "CL",
                "same",
                "Gold allocators complete inflation-hedge adjustment one session after an extreme crude repricing.",
            ),
            (
                "v7g3_transfer_ES_CL",
                "H15_ES_TO_CL_GROWTH_TRANSFER",
                "lagged_equity_to_energy_growth_transfer",
                "CL",
                "ES",
                "same",
                "Energy participants complete macro-demand adjustment one session after an extreme equity repricing.",
            ),
            (
                "v7g3_transfer_ES_GC",
                "H16_ES_TO_GC_RISK_OFF_TRANSFER",
                "lagged_equity_to_gold_risk_transfer",
                "GC",
                "ES",
                "opposite",
                "Safe-haven allocation reaches gold one session after an extreme equity repricing.",
            ),
        ]
    )
    specs: list[V7CandidateSpec] = []
    for candidate_id, hypothesis_id, mechanism, market, source, relation, economic in rows:
        payload = {
            "grammar_id": GRAMMAR_ID,
            "candidate_id": candidate_id,
            "hypothesis_id": hypothesis_id,
            "mechanism_class": mechanism,
            "market": market,
            "source_market": source,
            "side_relation": relation,
            "holding_minutes": 398,
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
                holding_minutes=398,
                economic_hypothesis=economic,
                specification_hash=_stable_hash(payload),
            )
        )
    if len(specs) != 19 or len({row.candidate_id for row in specs}) != 19:
        raise V7GrammarError("grammar 0003 must contain 19 unique structures")
    return tuple(specs)


def generate_signal_population(
    bars_by_market: Mapping[str, V7MarketBars],
    *,
    graveyard_path: str | Path | None = "mission/state/graveyard.db",
) -> dict[str, tuple[V7Signal, ...]]:
    if set(bars_by_market) != set(MARKETS):
        raise V7GrammarError("grammar 0003 requires all six frozen markets")
    specs = {row.candidate_id: row for row in candidate_specs()}
    if graveyard_path is not None:
        assert_class_distance(graveyard_path, tuple(specs.values()))
    metrics = {
        market: _rth_metrics(bars) for market, bars in bars_by_market.items()
    }
    output: dict[str, tuple[V7Signal, ...]] = {}
    for market in EQUITY_MARKETS:
        candidate_id = f"v7g3_turn_month_{market}"
        output[candidate_id] = tuple(
            _turn_month_signals(
                specs[candidate_id], bars_by_market[market], metrics[market]
            )
        )
    for market in MARKETS:
        candidate_id = f"v7g3_underreaction_{market}"
        output[candidate_id] = tuple(
            _underreaction_signals(
                specs[candidate_id], bars_by_market[market], metrics[market]
            )
        )
        candidate_id = f"v7g3_two_day_reversal_{market}"
        output[candidate_id] = tuple(
            _two_day_reversal_signals(
                specs[candidate_id], bars_by_market[market], metrics[market]
            )
        )
    for candidate_id, source, target, relation in (
        ("v7g3_transfer_CL_GC", "CL", "GC", "same"),
        ("v7g3_transfer_ES_CL", "ES", "CL", "same"),
        ("v7g3_transfer_ES_GC", "ES", "GC", "opposite"),
    ):
        output[candidate_id] = tuple(
            _cross_transfer_signals(
                specs[candidate_id],
                source_bars=bars_by_market[source],
                target_bars=bars_by_market[target],
                source_metrics=metrics[source],
                target_metrics=metrics[target],
                relation=relation,
            )
        )
    if set(output) != set(specs):
        raise V7GrammarError("grammar 0003 population drifted from WORM scope")
    for candidate_id, signals in output.items():
        _validate_signal_sequence(specs[candidate_id], signals, bars_by_market)
    return dict(sorted(output.items()))


def _turn_month_signals(
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    metrics: Mapping[int, RthSessionMetric],
) -> list[V7Signal]:
    by_month: dict[tuple[int, int], list[int]] = {}
    for day in sorted(metrics):
        date = _session_date(day)
        by_month.setdefault((date.year, date.month), []).append(day)
    active: set[int] = set()
    for days in by_month.values():
        active.update(days[:3])
        active.add(days[-1])
    output: list[V7Signal] = []
    for day in sorted(active):
        execution = _execution_from_metric(bars, metrics[day])
        if execution is None:
            continue
        decision, entry, exit_index = execution
        output.append(
            _signal(
                spec,
                bars,
                day,
                1,
                decision,
                entry,
                exit_index,
                {
                    "calendar_month": _session_date(day).strftime("%Y-%m"),
                    "calendar_membership": "FIRST_THREE_OR_LAST",
                },
            )
        )
    return output


def _underreaction_signals(
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    metrics: Mapping[int, RthSessionMetric],
) -> list[V7Signal]:
    days = sorted(metrics)
    history: list[float] = []
    output: list[V7Signal] = []
    for prior_day, current_day in zip(days, days[1:], strict=False):
        prior_return = metrics[prior_day].log_return
        threshold = float(np.median(history[-60:])) if len(history) >= 20 else None
        execution = _execution_from_metric(bars, metrics[current_day])
        if (
            threshold is not None
            and abs(prior_return) >= threshold
            and prior_return != 0.0
            and execution is not None
        ):
            decision, entry, exit_index = execution
            output.append(
                _signal(
                    spec,
                    bars,
                    current_day,
                    int(math.copysign(1, prior_return)),
                    decision,
                    entry,
                    exit_index,
                    {
                        "prior_session_day": prior_day,
                        "prior_return": prior_return,
                        "past_median": threshold,
                    },
                )
            )
        history.append(abs(prior_return))
    return output


def _two_day_reversal_signals(
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    metrics: Mapping[int, RthSessionMetric],
) -> list[V7Signal]:
    days = sorted(metrics)
    history: list[float] = []
    output: list[V7Signal] = []
    for index in range(2, len(days)):
        first_day = days[index - 2]
        second_day = days[index - 1]
        current_day = days[index]
        cumulative = metrics[first_day].log_return + metrics[second_day].log_return
        threshold = (
            float(np.quantile(history[-60:], 0.75)) if len(history) >= 30 else None
        )
        execution = _execution_from_metric(bars, metrics[current_day])
        if (
            threshold is not None
            and abs(cumulative) >= threshold
            and cumulative != 0.0
            and execution is not None
        ):
            decision, entry, exit_index = execution
            output.append(
                _signal(
                    spec,
                    bars,
                    current_day,
                    -int(math.copysign(1, cumulative)),
                    decision,
                    entry,
                    exit_index,
                    {
                        "source_days": [first_day, second_day],
                        "two_day_log_return": cumulative,
                        "past_q75": threshold,
                    },
                )
            )
        history.append(abs(cumulative))
    return output


def _cross_transfer_signals(
    spec: V7CandidateSpec,
    *,
    source_bars: V7MarketBars,
    target_bars: V7MarketBars,
    source_metrics: Mapping[int, RthSessionMetric],
    target_metrics: Mapping[int, RthSessionMetric],
    relation: str,
) -> list[V7Signal]:
    common = sorted(set(source_metrics) & set(target_metrics))
    history: list[float] = []
    output: list[V7Signal] = []
    for prior_day, current_day in zip(common, common[1:], strict=False):
        source_return = source_metrics[prior_day].log_return
        threshold = (
            float(np.quantile(history[-60:], 0.75)) if len(history) >= 60 else None
        )
        execution = _execution_from_metric(target_bars, target_metrics[current_day])
        if (
            threshold is not None
            and abs(source_return) >= threshold
            and source_return != 0.0
            and execution is not None
        ):
            side = int(math.copysign(1, source_return))
            if relation == "opposite":
                side *= -1
            decision, entry, exit_index = execution
            output.append(
                _signal(
                    spec,
                    target_bars,
                    current_day,
                    side,
                    decision,
                    entry,
                    exit_index,
                    {
                        "source_market": source_bars.market,
                        "source_session_day": prior_day,
                        "source_return": source_return,
                        "past_q75": threshold,
                        "relation": relation,
                    },
                )
            )
        history.append(abs(source_return))
    return output


def _rth_metrics(bars: V7MarketBars) -> dict[int, RthSessionMetric]:
    output: dict[int, RthSessionMetric] = {}
    for day, positions in _day_positions(bars).items():
        window = _exact_window(bars, positions, 8 * 60 + 30, 15 * 60 + 9)
        if window is None:
            continue
        start, end = window
        open_price = float(bars.open[start])
        close_price = float(bars.close[end])
        if open_price <= 0.0 or close_price <= 0.0:
            continue
        output[day] = RthSessionMetric(
            session_day=day,
            start_index=start,
            end_index=end,
            open_price=open_price,
            close_price=close_price,
            log_return=float(math.log(close_price / open_price)),
        )
    return output


def _execution_from_metric(
    bars: V7MarketBars, metric: RthSessionMetric
) -> tuple[int, int, int] | None:
    decision = int(metric.start_index)
    entry = decision + 1
    exit_index = int(metric.end_index)
    if int(bars.local_minute[entry]) != 8 * 60 + 31:
        return None
    if not _same_segment(bars, decision, exit_index):
        return None
    return decision, entry, exit_index


def _signal(
    spec: V7CandidateSpec,
    bars: V7MarketBars,
    day: int,
    side: int,
    decision_index: int,
    entry_index: int,
    exit_index: int,
    feature_snapshot: Mapping[str, Any],
) -> V7Signal:
    return V7Signal(
        candidate_id=spec.candidate_id,
        hypothesis_id=spec.hypothesis_id,
        market=spec.market,
        source_market=spec.source_market,
        session_day=int(day),
        side=int(side),
        decision_ns=int(bars.decision_ns[decision_index]),
        availability_ns=int(bars.availability_ns[decision_index]),
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
            raise V7GrammarError("grammar 0003 signal identity drift")
        if signal.entry_index <= previous_exit:
            raise V7GrammarError("grammar 0003 signals overlap")
        previous_exit = signal.exit_index
        identity = (signal.session_day, signal.decision_ns)
        if identity in identities:
            raise V7GrammarError("grammar 0003 duplicate signal")
        identities.add(identity)
        if not _same_segment(bars, signal.entry_index, signal.exit_index):
            raise V7GrammarError("grammar 0003 signal crosses contract or gap")
        if int(bars.local_minute[signal.exit_index]) > 15 * 60 + 9:
            raise V7GrammarError("grammar 0003 signal violates flatten")


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
    bars: V7MarketBars, positions: np.ndarray, start_minute: int, end_minute: int
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


def _session_date(session_day: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=session_day)


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
