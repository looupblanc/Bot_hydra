from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.research.v7_graveyard import (
    canonical_mechanism_class,
    class_feedback,
)


GRAMMAR_ID = "hydra_v7_grammar_0001_scheduled_inventory_and_hazard"
PREREGISTRATION_SHA256 = (
    "1adeab25abb0f75f067caa523c499536c85d66b1811a500d32a3b6caf30a74cc"
)
MARKETS = ("ES", "NQ", "RTY", "YM", "GC", "CL")
EQUITY_MARKETS = ("ES", "NQ", "RTY", "YM")
MINUTE_NS = 60_000_000_000


class V7GrammarError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class V7CandidateSpec:
    candidate_id: str
    hypothesis_id: str
    mechanism_class: str
    market: str
    source_market: str | None
    side_relation: str
    holding_minutes: int
    economic_hypothesis: str
    specification_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "hypothesis_id": self.hypothesis_id,
            "mechanism_class": self.mechanism_class,
            "market": self.market,
            "source_market": self.source_market,
            "side_relation": self.side_relation,
            "holding_minutes": self.holding_minutes,
            "economic_hypothesis": self.economic_hypothesis,
            "specification_hash": self.specification_hash,
        }


@dataclass(frozen=True, slots=True)
class V7Signal:
    candidate_id: str
    hypothesis_id: str
    market: str
    source_market: str | None
    session_day: int
    side: int
    decision_ns: int
    availability_ns: int
    entry_index: int
    exit_index: int
    entry_ns: int
    exit_ns: int
    contract_code: int
    segment_code: int
    feature_snapshot_hash: str

    def __post_init__(self) -> None:
        if self.side not in {-1, 1}:
            raise ValueError("signal side must be -1 or +1")
        if self.availability_ns > self.decision_ns:
            raise ValueError("signal uses unavailable information")
        if self.entry_ns < self.decision_ns or self.exit_ns <= self.entry_ns:
            raise ValueError("signal execution chronology is invalid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "hypothesis_id": self.hypothesis_id,
            "market": self.market,
            "source_market": self.source_market,
            "session_day": self.session_day,
            "side": self.side,
            "decision_ns": self.decision_ns,
            "availability_ns": self.availability_ns,
            "entry_index": self.entry_index,
            "exit_index": self.exit_index,
            "entry_ns": self.entry_ns,
            "exit_ns": self.exit_ns,
            "contract_code": self.contract_code,
            "segment_code": self.segment_code,
            "feature_snapshot_hash": self.feature_snapshot_hash,
        }


@dataclass(frozen=True, slots=True)
class V7MarketBars:
    market: str
    timestamp_ns: np.ndarray
    decision_ns: np.ndarray
    availability_ns: np.ndarray
    session_day: np.ndarray
    contract_code: np.ndarray
    segment_code: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    local_minute: np.ndarray
    local_weekday: np.ndarray
    bundle_hash: str

    @property
    def row_count(self) -> int:
        return len(self.timestamp_ns)


def candidate_specs() -> tuple[V7CandidateSpec, ...]:
    rows: list[tuple[str, str, str, str, str | None, str, int, str]] = []
    hypotheses = {
        "H1": (
            "H1_SETTLEMENT_REBALANCE_CONTINUATION",
            "scheduled_settlement_rebalance_continuation",
            "Benchmark and market-on-close participants must complete index inventory near settlement, so an unusually efficient late displacement continues until the risk cutoff.",
        ),
        "H2": (
            "H2_REGIONAL_HANDOFF_INVENTORY_RELEASE",
            "regional_handoff_inventory_release",
            "Inventory accumulated during the first Globex region transfers at 02:00 CT and unusually efficient moves continue while incoming participants price the inherited inventory.",
        ),
        "H3": (
            "H3_RANGE_AGE_STOP_CASCADE",
            "range_age_stop_cascade",
            "Repeated tests of a ninety-minute extreme accumulate stops whose first material break continues before liquidity can restock.",
        ),
        "H4": (
            "H4_WEEKEND_INVENTORY_RESET",
            "weekend_inventory_reset",
            "Friday risk mandates distort the final forty minutes and the distortion partially reverses during Monday's first cash-session hour.",
        ),
        "H5": (
            "H5_CROSS_ECOLOGY_DELAYED_RISK_TRANSFER",
            "cross_ecology_delayed_risk_transfer",
            "A large early ES shock forces delayed portfolio hedges into gold and crude when those target markets have not already repriced.",
        ),
    }
    for market in EQUITY_MARKETS:
        hid, mechanism, economic = hypotheses["H1"]
        rows.append(
            (f"v7g1_settlement_{market}", hid, mechanism, market, None, "same", 14, economic)
        )
    for market in MARKETS:
        hid, mechanism, economic = hypotheses["H2"]
        rows.append(
            (f"v7g1_handoff_{market}", hid, mechanism, market, None, "same", 120, economic)
        )
    for market in MARKETS:
        hid, mechanism, economic = hypotheses["H3"]
        rows.append(
            (f"v7g1_range_age_{market}", hid, mechanism, market, None, "break", 30, economic)
        )
    for market in MARKETS:
        hid, mechanism, economic = hypotheses["H4"]
        rows.append(
            (f"v7g1_weekend_{market}", hid, mechanism, market, None, "opposite", 60, economic)
        )
    hid, mechanism, economic = hypotheses["H5"]
    rows.extend(
        [
            ("v7g1_risk_transfer_ES_GC", hid, mechanism, "GC", "ES", "opposite", 60, economic),
            ("v7g1_risk_transfer_ES_CL", hid, mechanism, "CL", "ES", "same", 60, economic),
        ]
    )
    specs = []
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
    if len(specs) != 24 or len({row.candidate_id for row in specs}) != 24:
        raise V7GrammarError("grammar must contain exactly 24 unique structures")
    return tuple(specs)


def load_v7_market_bars(project_root: str | Path) -> dict[str, V7MarketBars]:
    root = Path(project_root).resolve()
    manifest = json.loads((root / "data/manifest.json").read_text(encoding="utf-8"))
    paths: dict[str, Path] = {}
    for artifact in manifest["artifacts"]:
        if artifact.get("kind") != "CANONICAL_FEATURE_MANIFEST":
            continue
        path = root / str(artifact["path"])
        payload = json.loads(path.read_text(encoding="utf-8"))
        key = payload.get("key", {})
        market = str(key.get("market") or "")
        if (
            market in MARKETS
            and key.get("transformation_version")
            == "hydra_turbo_feature_bundle_v3_risk_path"
            and key.get("end_exclusive") == "2024-10-01"
        ):
            paths[market] = path.parent
    if set(paths) != set(MARKETS):
        raise V7GrammarError(f"missing canonical V3 market bundles: {set(MARKETS)-set(paths)}")
    return {
        market: market_bars_from_matrix(market, FeatureMatrix.open(path, mmap=True))
        for market, path in sorted(paths.items())
    }


def market_bars_from_matrix(market: str, matrix: FeatureMatrix) -> V7MarketBars:
    timestamp_ns = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
    timestamps = pd.to_datetime(timestamp_ns, unit="ns", utc=True).tz_convert(
        "America/Chicago"
    )
    local_minute = np.asarray(timestamps.hour * 60 + timestamps.minute, dtype=np.int16)
    local_weekday = np.asarray(timestamps.weekday, dtype=np.int8)
    return V7MarketBars(
        market=market,
        timestamp_ns=timestamp_ns,
        decision_ns=np.asarray(matrix.array("decision_ns"), dtype=np.int64),
        availability_ns=np.asarray(matrix.array("availability_ns"), dtype=np.int64),
        session_day=np.asarray(matrix.array("session_day"), dtype=np.int32),
        contract_code=np.asarray(matrix.array("contract_code"), dtype=np.int16),
        segment_code=np.asarray(matrix.array("segment_code"), dtype=np.int64),
        open=np.asarray(matrix.array("bar_open"), dtype=np.float64),
        high=np.asarray(matrix.array("bar_high"), dtype=np.float64),
        low=np.asarray(matrix.array("bar_low"), dtype=np.float64),
        close=np.asarray(matrix.array("bar_close"), dtype=np.float64),
        local_minute=local_minute,
        local_weekday=local_weekday,
        bundle_hash=matrix.fingerprint,
    )


def generate_signal_population(
    bars_by_market: Mapping[str, V7MarketBars],
    *,
    graveyard_path: str | Path | None = "mission/state/graveyard.db",
) -> dict[str, tuple[V7Signal, ...]]:
    if set(bars_by_market) != set(MARKETS):
        raise V7GrammarError("signal population requires all six frozen markets")
    specs = {row.candidate_id: row for row in candidate_specs()}
    if graveyard_path is not None:
        assert_class_distance(graveyard_path, tuple(specs.values()))
    output: dict[str, tuple[V7Signal, ...]] = {}
    for market in EQUITY_MARKETS:
        candidate_id = f"v7g1_settlement_{market}"
        output[candidate_id] = tuple(
            _settlement_signals(specs[candidate_id], bars_by_market[market])
        )
    for market in MARKETS:
        candidate_id = f"v7g1_handoff_{market}"
        output[candidate_id] = tuple(
            _handoff_signals(specs[candidate_id], bars_by_market[market])
        )
        candidate_id = f"v7g1_range_age_{market}"
        output[candidate_id] = tuple(
            _range_age_signals(specs[candidate_id], bars_by_market[market])
        )
        candidate_id = f"v7g1_weekend_{market}"
        output[candidate_id] = tuple(
            _weekend_signals(specs[candidate_id], bars_by_market[market])
        )
    for target, relation in (("GC", "opposite"), ("CL", "same")):
        candidate_id = f"v7g1_risk_transfer_ES_{target}"
        output[candidate_id] = tuple(
            _risk_transfer_signals(
                specs[candidate_id],
                bars_by_market["ES"],
                bars_by_market[target],
                relation=relation,
            )
        )
    if set(output) != set(specs):
        raise V7GrammarError("generated candidate population drifted from WORM scope")
    for candidate_id, signals in output.items():
        _validate_signal_sequence(specs[candidate_id], signals, bars_by_market)
    return dict(sorted(output.items()))


def _settlement_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    history: list[float] = []
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        window = _exact_window(bars, positions, 14 * 60 + 30, 14 * 60 + 54)
        entry = _at_minute(bars, positions, 14 * 60 + 56)
        exit_index = _at_minute(bars, positions, 15 * 60 + 9)
        if window is None or entry is None or exit_index is None:
            continue
        start, end = window
        if not _same_segment(bars, start, exit_index):
            continue
        displacement = float(bars.close[end] / bars.open[start] - 1.0)
        efficiency = _path_efficiency(bars.open[start], bars.close[start : end + 1])
        threshold = float(np.quantile(history[-60:], 0.75)) if len(history) >= 60 else None
        if threshold is not None and abs(displacement) >= threshold and efficiency >= 0.55 and displacement != 0.0:
            output.append(
                _signal(spec, bars, day, int(math.copysign(1, displacement)), end, entry, exit_index, {"displacement": displacement, "efficiency": efficiency, "threshold": threshold})
            )
        history.append(abs(displacement))
    return output


def _handoff_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    history: list[float] = []
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        start = _at_minute(bars, positions, 17 * 60)
        end = _at_minute(bars, positions, 1 * 60 + 59)
        entry = _at_minute(bars, positions, 2 * 60 + 1)
        exit_index = _at_minute(bars, positions, 4 * 60)
        if None in {start, end, entry, exit_index}:
            continue
        start = int(start)
        end = int(end)
        entry = int(entry)
        exit_index = int(exit_index)
        if not (start < end < entry < exit_index) or not _same_segment(bars, start, exit_index):
            continue
        displacement = float(bars.close[end] / bars.open[start] - 1.0)
        efficiency = _path_efficiency(bars.open[start], bars.close[start : end + 1])
        threshold = float(np.quantile(history[-60:], 0.75)) if len(history) >= 60 else None
        if threshold is not None and abs(displacement) >= threshold and efficiency >= 0.60 and displacement != 0.0:
            output.append(
                _signal(spec, bars, day, int(math.copysign(1, displacement)), end, entry, exit_index, {"displacement": displacement, "efficiency": efficiency, "threshold": threshold})
            )
        history.append(abs(displacement))
    return output


def _range_age_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    tick = instrument_spec(spec.market).tick_size
    range_history: list[float] = []
    output: list[V7Signal] = []
    for day, positions in _day_positions(bars).items():
        rth = positions[(bars.local_minute[positions] >= 8 * 60 + 30) & (bars.local_minute[positions] <= 15 * 60 + 9)]
        if len(rth):
            day_range = float(np.nanmax(bars.high[rth]) - np.nanmin(bars.low[rth]))
        else:
            continue
        atr = float(np.median(range_history[-20:])) if len(range_history) >= 20 else None
        range_history.append(day_range)
        if atr is None or not math.isfinite(atr) or atr <= 0.0:
            continue
        eligible = rth[(bars.local_minute[rth] < 14 * 60 + 30)]
        for position in eligible:
            index = int(position)
            prior = np.arange(index - 90, index, dtype=np.int64)
            if (
                index < 90
                or len(prior) != 90
                or int(bars.session_day[int(prior[0])]) != int(day)
            ):
                continue
            if not _same_segment(bars, int(prior[0]), index + 1):
                continue
            pre_high = float(np.max(bars.high[prior]))
            pre_low = float(np.min(bars.low[prior]))
            upper_touches = prior[(pre_high - bars.high[prior]) <= 2.0 * tick + 1e-12]
            lower_touches = prior[(bars.low[prior] - pre_low) <= 2.0 * tick + 1e-12]
            break_distance = max(2.0 * tick, 0.10 * atr)
            side = 0
            touches: np.ndarray | None = None
            if bars.close[index] > pre_high + break_distance:
                side = 1
                touches = upper_touches
            elif bars.close[index] < pre_low - break_distance:
                side = -1
                touches = lower_touches
            if side == 0 or touches is None or not _three_separated_touches(touches):
                continue
            entry = index + 1
            desired_exit = index + 30
            flatten = _at_minute(bars, positions, 15 * 60 + 9)
            if flatten is None:
                continue
            exit_index = min(desired_exit, int(flatten))
            if entry >= exit_index or not _same_segment(bars, index, exit_index):
                continue
            output.append(
                _signal(spec, bars, day, side, index, entry, exit_index, {"pre_high": pre_high, "pre_low": pre_low, "break_distance": break_distance, "atr20": atr, "touch_count": int(len(touches))})
            )
            break
    return output


def _weekend_signals(
    spec: V7CandidateSpec, bars: V7MarketBars
) -> list[V7Signal]:
    days = _day_positions(bars)
    ordered = sorted(days)
    friday_history: list[float] = []
    friday_state: dict[int, tuple[float, float]] = {}
    for day in ordered:
        positions = days[day]
        weekday = _session_weekday(day)
        if weekday != 4:
            continue
        window = _exact_window(bars, positions, 14 * 60 + 30, 15 * 60 + 9)
        if window is None:
            continue
        start, end = window
        displacement = float(bars.close[end] / bars.open[start] - 1.0)
        threshold = float(np.median(friday_history[-52:])) if len(friday_history) >= 12 else None
        if threshold is not None:
            friday_state[day] = (displacement, threshold)
        friday_history.append(abs(displacement))
    output: list[V7Signal] = []
    for previous_day, day in zip(ordered, ordered[1:], strict=False):
        if _session_weekday(previous_day) != 4 or _session_weekday(day) != 0:
            continue
        state = friday_state.get(previous_day)
        if state is None:
            continue
        displacement, threshold = state
        if abs(displacement) < threshold or displacement == 0.0:
            continue
        positions = days[day]
        decision = _at_minute(bars, positions, 8 * 60 + 29)
        entry = _at_minute(bars, positions, 8 * 60 + 31)
        exit_index = _at_minute(bars, positions, 9 * 60 + 30)
        if None in {decision, entry, exit_index} or not _same_segment(bars, int(decision), int(exit_index)):
            continue
        output.append(
            _signal(spec, bars, day, -int(math.copysign(1, displacement)), int(decision), int(entry), int(exit_index), {"friday_displacement": displacement, "threshold": threshold, "friday_session_day": previous_day})
        )
    return output


def _risk_transfer_signals(
    spec: V7CandidateSpec,
    source: V7MarketBars,
    target: V7MarketBars,
    *,
    relation: str,
) -> list[V7Signal]:
    source_days = _day_positions(source)
    target_days = _day_positions(target)
    common = sorted(set(source_days) & set(target_days))
    source_history: list[float] = []
    target_history: list[float] = []
    output: list[V7Signal] = []
    for day in common:
        source_window = _exact_window(source, source_days[day], 8 * 60 + 30, 8 * 60 + 59)
        target_window = _exact_window(target, target_days[day], 8 * 60 + 30, 8 * 60 + 59)
        target_entry = _at_minute(target, target_days[day], 9 * 60 + 1)
        target_exit = _at_minute(target, target_days[day], 10 * 60)
        if source_window is None or target_window is None or None in {target_entry, target_exit}:
            continue
        source_start, source_end = source_window
        target_start, target_end = target_window
        if not _same_segment(source, source_start, source_end) or not _same_segment(
            target, target_start, target_end
        ):
            continue
        source_move = float(source.close[source_end] / source.open[source_start] - 1.0)
        target_move = float(target.close[target_end] / target.open[target_start] - 1.0)
        source_threshold = float(np.quantile(source_history[-60:], 0.80)) if len(source_history) >= 60 else None
        target_threshold = float(np.quantile(target_history[-60:], 0.80)) if len(target_history) >= 60 else None
        # The WORM contract prohibits the signal itself from crossing a roll,
        # not merely the eventual position.  The observation can still update
        # future past-only thresholds even when this exact trade is excluded.
        executable = _same_segment(target, target_start, int(target_exit))
        if source_threshold is not None and target_threshold is not None and abs(source_move) >= source_threshold and abs(target_move) < 0.50 * target_threshold and source_move != 0.0 and executable:
            side = int(math.copysign(1, source_move))
            if relation == "opposite":
                side *= -1
            output.append(
                _signal(spec, target, day, side, int(target_end), int(target_entry), int(target_exit), {"source_move": source_move, "target_move": target_move, "source_threshold": source_threshold, "target_threshold": target_threshold, "source_availability_ns": int(source.availability_ns[source_end])})
            )
        source_history.append(abs(source_move))
        target_history.append(abs(target_move))
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
    availability = int(bars.availability_ns[decision_index])
    decision = int(bars.decision_ns[decision_index])
    if spec.source_market is not None and "source_availability_ns" in feature_snapshot:
        availability = max(availability, int(feature_snapshot["source_availability_ns"]))
    return V7Signal(
        candidate_id=spec.candidate_id,
        hypothesis_id=spec.hypothesis_id,
        market=spec.market,
        source_market=spec.source_market,
        session_day=int(day),
        side=int(side),
        decision_ns=decision,
        availability_ns=availability,
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
            raise V7GrammarError("signal identity drift")
        if signal.entry_index <= previous_exit:
            raise V7GrammarError("candidate signals overlap")
        previous_exit = signal.exit_index
        identity = (signal.session_day, signal.decision_ns)
        if identity in identities:
            raise V7GrammarError("duplicate candidate signal")
        identities.add(identity)
        if not _same_segment(bars, signal.entry_index, signal.exit_index):
            raise V7GrammarError("signal crosses contract or segment")
        if int(bars.local_minute[signal.exit_index]) > 15 * 60 + 9:
            raise V7GrammarError("signal violates V7 session flatten")


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
    expected = end_minute - start_minute + 1
    if end - start + 1 != expected:
        return None
    timestamps = bars.timestamp_ns[start : end + 1]
    if not bool(np.all(np.diff(timestamps) == MINUTE_NS)) or not _same_segment(bars, start, end):
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


def _three_separated_touches(positions: np.ndarray) -> bool:
    if len(positions) < 3:
        return False
    retained = [int(positions[0])]
    for value in positions[1:]:
        if int(value) - retained[-1] >= 5:
            retained.append(int(value))
        if len(retained) >= 3:
            return True
    return False


def _session_weekday(session_day: int) -> int:
    return (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=session_day)).weekday()


def assert_class_distance(
    graveyard_path: str | Path, specs: Sequence[V7CandidateSpec] | None = None
) -> None:
    dead_classes = {
        canonical_mechanism_class(str(row["mechanism_class"]))
        for row in class_feedback(graveyard_path)
    }
    proposed = specs or candidate_specs()
    collisions = sorted(
        {
            canonical_mechanism_class(row.mechanism_class)
            for row in proposed
            if canonical_mechanism_class(row.mechanism_class) in dead_classes
        }
    )
    if collisions:
        raise V7GrammarError(
            "grammar repeats exact dead mechanism classes: " + ",".join(collisions)
        )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


__all__ = [
    "GRAMMAR_ID",
    "MARKETS",
    "PREREGISTRATION_SHA256",
    "V7CandidateSpec",
    "V7GrammarError",
    "V7MarketBars",
    "V7Signal",
    "candidate_specs",
    "assert_class_distance",
    "generate_signal_population",
    "load_v7_market_bars",
    "market_bars_from_matrix",
]
