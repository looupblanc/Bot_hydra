from __future__ import annotations

import hashlib
import json
import math
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


GRAMMAR_ID = "hydra_v7_grammar_0002_session_inventory_risk_premia"
PREREGISTRATION_SHA256 = (
    "2060c042296edc05ce0f39d0289a292bc251a86aea9de33946698e6486c1ca54"
)
EQUITY_MARKETS = ("ES", "NQ", "RTY", "YM")


def candidate_specs() -> tuple[V7CandidateSpec, ...]:
    hypotheses = {
        "H6": (
            "H6_OVERNIGHT_EQUITY_RISK_PREMIUM",
            "overnight_equity_inventory_risk_premium",
            "Equity-index intermediaries earn compensation for warehousing risk while cash markets are closed.",
            "long",
            929,
        ),
        "H7": (
            "H7_OVERNIGHT_INVENTORY_PARTIAL_UNWIND",
            "overnight_inventory_partial_unwind",
            "Cash-session liquidity providers partially unwind unusually large thin-liquidity overnight inventory.",
            "opposite",
            90,
        ),
        "H8": (
            "H8_MIDDAY_LIQUIDITY_REPLENISHMENT",
            "midday_liquidity_replenishment_reversion",
            "Replenished midday passive liquidity reverts an efficient urgent morning displacement.",
            "opposite",
            90,
        ),
        "H9": (
            "H9_AFTERNOON_INVENTORY_REACCELERATION",
            "afternoon_inventory_reacceleration_after_compression",
            "An unfinished morning parent order resumes after a compressed midday pause when afternoon liquidity returns.",
            "same",
            119,
        ),
        "H10": (
            "H10_WEEKLY_ENERGY_INVENTORY_FLOW",
            "weekly_energy_inventory_flow_continuation",
            "Sequential physical and discretionary adjustments continue a high-efficiency weekly CL inventory shock.",
            "same",
            54,
        ),
    }
    rows: list[tuple[str, str, str, str, str, int, str]] = []
    hid, mechanism, economic, relation, hold = hypotheses["H6"]
    for market in EQUITY_MARKETS:
        rows.append(
            (f"v7g2_overnight_premium_{market}", hid, mechanism, market, relation, hold, economic)
        )
    for key, prefix in (
        ("H7", "overnight_unwind"),
        ("H8", "midday_reversion"),
        ("H9", "afternoon_reacceleration"),
    ):
        hid, mechanism, economic, relation, hold = hypotheses[key]
        for market in MARKETS:
            rows.append(
                (f"v7g2_{prefix}_{market}", hid, mechanism, market, relation, hold, economic)
            )
    hid, mechanism, economic, relation, hold = hypotheses["H10"]
    rows.append(
        ("v7g2_weekly_inventory_CL", hid, mechanism, "CL", relation, hold, economic)
    )
    specs: list[V7CandidateSpec] = []
    for candidate_id, hypothesis_id, mechanism, market, relation, hold, economic in rows:
        payload = {
            "grammar_id": GRAMMAR_ID,
            "candidate_id": candidate_id,
            "hypothesis_id": hypothesis_id,
            "mechanism_class": mechanism,
            "market": market,
            "source_market": None,
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
                source_market=None,
                side_relation=relation,
                holding_minutes=hold,
                economic_hypothesis=economic,
                specification_hash=_stable_hash(payload),
            )
        )
    if len(specs) != 23 or len({row.candidate_id for row in specs}) != 23:
        raise V7GrammarError("grammar 0002 must contain 23 unique structures")
    return tuple(specs)


def generate_signal_population(
    bars_by_market: Mapping[str, V7MarketBars],
    *,
    graveyard_path: str | Path | None = "mission/state/graveyard.db",
) -> dict[str, tuple[V7Signal, ...]]:
    if set(bars_by_market) != set(MARKETS):
        raise V7GrammarError("grammar 0002 requires all six frozen markets")
    specs = {row.candidate_id: row for row in candidate_specs()}
    if graveyard_path is not None:
        assert_class_distance(graveyard_path, tuple(specs.values()))
    output: dict[str, tuple[V7Signal, ...]] = {}
    for market in EQUITY_MARKETS:
        candidate_id = f"v7g2_overnight_premium_{market}"
        output[candidate_id] = tuple(
            _overnight_premium_signals(specs[candidate_id], bars_by_market[market])
        )
    for market in MARKETS:
        candidate_id = f"v7g2_overnight_unwind_{market}"
        output[candidate_id] = tuple(
            _overnight_unwind_signals(specs[candidate_id], bars_by_market[market])
        )
        candidate_id = f"v7g2_midday_reversion_{market}"
        output[candidate_id] = tuple(
            _midday_reversion_signals(specs[candidate_id], bars_by_market[market])
        )
        candidate_id = f"v7g2_afternoon_reacceleration_{market}"
        output[candidate_id] = tuple(
            _afternoon_reacceleration_signals(
                specs[candidate_id], bars_by_market[market]
            )
        )
    candidate_id = "v7g2_weekly_inventory_CL"
    output[candidate_id] = tuple(
        _weekly_inventory_signals(specs[candidate_id], bars_by_market["CL"])
    )
    if set(output) != set(specs):
        raise V7GrammarError("grammar 0002 population drifted from WORM scope")
    for candidate_id, signals in output.items():
        _validate_signal_sequence(specs[candidate_id], signals, bars_by_market)
    return dict(sorted(output.items()))


def _overnight_premium_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        # Session-day Tuesday through Friday corresponds to a Monday-through-
        # Thursday 17:00 CT entry; Sunday evening is excluded by construction.
        if _session_weekday(day) not in {1, 2, 3, 4}:
            continue
        decision = _at_minute(bars, positions, 17 * 60)
        entry = _at_minute(bars, positions, 17 * 60 + 1)
        exit_index = _at_minute(bars, positions, 8 * 60 + 29)
        if None in {decision, entry, exit_index}:
            continue
        if not _same_segment(bars, int(decision), int(exit_index)):
            continue
        output.append(
            _signal(
                spec,
                bars,
                day,
                1,
                int(decision),
                int(entry),
                int(exit_index),
                {"fixed_side": "long", "calendar_entry_weekday": _session_weekday(day) - 1},
            )
        )
    return output


def _overnight_unwind_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    history: list[float] = []
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        window = _exact_window(bars, positions, 17 * 60, 8 * 60 + 29)
        entry = _at_minute(bars, positions, 8 * 60 + 31)
        exit_index = _at_minute(bars, positions, 10 * 60)
        if window is None or entry is None or exit_index is None:
            continue
        start, end = window
        if not _same_segment(bars, start, int(exit_index)):
            continue
        displacement = float(bars.close[end] / bars.open[start] - 1.0)
        threshold = float(np.median(history[-20:])) if len(history) >= 20 else None
        if threshold is not None and abs(displacement) >= threshold and displacement != 0.0:
            output.append(
                _signal(
                    spec,
                    bars,
                    day,
                    -int(math.copysign(1, displacement)),
                    end,
                    int(entry),
                    int(exit_index),
                    {"overnight_displacement": displacement, "past_median": threshold},
                )
            )
        history.append(abs(displacement))
    return output


def _midday_reversion_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    history: list[float] = []
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        window = _exact_window(bars, positions, 8 * 60 + 30, 10 * 60 + 59)
        entry = _at_minute(bars, positions, 11 * 60 + 1)
        exit_index = _at_minute(bars, positions, 12 * 60 + 30)
        if window is None or entry is None or exit_index is None:
            continue
        start, end = window
        if not _same_segment(bars, start, int(exit_index)):
            continue
        displacement = float(bars.close[end] / bars.open[start] - 1.0)
        efficiency = _path_efficiency(bars.open[start], bars.close[start : end + 1])
        threshold = float(np.median(history[-20:])) if len(history) >= 20 else None
        if (
            threshold is not None
            and abs(displacement) >= threshold
            and efficiency >= 0.35
            and displacement != 0.0
        ):
            output.append(
                _signal(
                    spec,
                    bars,
                    day,
                    -int(math.copysign(1, displacement)),
                    end,
                    int(entry),
                    int(exit_index),
                    {
                        "morning_displacement": displacement,
                        "path_efficiency": efficiency,
                        "past_median": threshold,
                    },
                )
            )
        history.append(abs(displacement))
    return output


def _afternoon_reacceleration_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    morning_history: list[float] = []
    midday_range_history: list[float] = []
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        morning = _exact_window(bars, positions, 8 * 60 + 30, 10 * 60 + 29)
        midday = _exact_window(bars, positions, 10 * 60 + 30, 12 * 60 + 29)
        entry = _at_minute(bars, positions, 12 * 60 + 31)
        exit_index = _at_minute(bars, positions, 14 * 60 + 30)
        if morning is None or midday is None or entry is None or exit_index is None:
            continue
        morning_start, morning_end = morning
        midday_start, midday_end = midday
        if not _same_segment(bars, morning_start, int(exit_index)):
            continue
        morning_move = float(bars.close[morning_end] - bars.open[morning_start])
        midday_range = float(
            np.max(bars.high[midday_start : midday_end + 1])
            - np.min(bars.low[midday_start : midday_end + 1])
        )
        morning_threshold = (
            float(np.median(morning_history[-20:]))
            if len(morning_history) >= 20
            else None
        )
        range_threshold = (
            float(np.median(midday_range_history[-20:]))
            if len(midday_range_history) >= 20
            else None
        )
        retained_move = float(bars.close[midday_end] - bars.open[morning_start])
        retention_ok = (
            morning_move != 0.0
            and math.copysign(1.0, morning_move) * retained_move
            >= 0.50 * abs(morning_move)
        )
        if (
            morning_threshold is not None
            and range_threshold is not None
            and abs(morning_move) >= morning_threshold
            and midday_range <= range_threshold
            and retention_ok
        ):
            output.append(
                _signal(
                    spec,
                    bars,
                    day,
                    int(math.copysign(1, morning_move)),
                    midday_end,
                    int(entry),
                    int(exit_index),
                    {
                        "morning_move": morning_move,
                        "morning_threshold": morning_threshold,
                        "midday_range": midday_range,
                        "range_threshold": range_threshold,
                        "retained_move": retained_move,
                    },
                )
            )
        morning_history.append(abs(morning_move))
        midday_range_history.append(midday_range)
    return output


def _weekly_inventory_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    history: list[float] = []
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        if _session_weekday(day) != 2:
            continue
        window = _exact_window(bars, positions, 9 * 60 + 30, 9 * 60 + 34)
        entry = _at_minute(bars, positions, 9 * 60 + 36)
        exit_index = _at_minute(bars, positions, 10 * 60 + 30)
        if window is None or entry is None or exit_index is None:
            continue
        start, end = window
        if not _same_segment(bars, start, int(exit_index)):
            continue
        displacement = float(bars.close[end] / bars.open[start] - 1.0)
        efficiency = _path_efficiency(bars.open[start], bars.close[start : end + 1])
        threshold = float(np.median(history[-12:])) if len(history) >= 12 else None
        if (
            threshold is not None
            and abs(displacement) >= threshold
            and efficiency >= 0.50
            and displacement != 0.0
        ):
            output.append(
                _signal(
                    spec,
                    bars,
                    day,
                    int(math.copysign(1, displacement)),
                    end,
                    int(entry),
                    int(exit_index),
                    {
                        "shock_displacement": displacement,
                        "path_efficiency": efficiency,
                        "past_wednesday_median": threshold,
                    },
                )
            )
        history.append(abs(displacement))
    return output


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
        source_market=None,
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
            raise V7GrammarError("grammar 0002 signal identity drift")
        if signal.entry_index <= previous_exit:
            raise V7GrammarError("grammar 0002 candidate signals overlap")
        previous_exit = signal.exit_index
        identity = (signal.session_day, signal.decision_ns)
        if identity in identities:
            raise V7GrammarError("grammar 0002 duplicate signal")
        identities.add(identity)
        if not _same_segment(bars, signal.entry_index, signal.exit_index):
            raise V7GrammarError("grammar 0002 signal crosses contract or gap")
        if int(bars.local_minute[signal.exit_index]) > 15 * 60 + 9:
            raise V7GrammarError("grammar 0002 signal violates session flatten")


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
    expected = (
        end_minute - start_minute + 1
        if end_minute >= start_minute
        else (24 * 60 - start_minute) + end_minute + 1
    )
    if end - start + 1 != expected or not _same_segment(bars, start, end):
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


def _path_efficiency(open_price: float, closes: np.ndarray) -> float:
    path = float(np.sum(np.abs(np.diff(np.concatenate(([open_price], closes))))))
    return abs(float(closes[-1] - open_price)) / path if path > 0.0 else 0.0


def _session_weekday(session_day: int) -> int:
    return (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=session_day)).weekday()


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
