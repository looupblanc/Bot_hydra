"""Bounded event-sourced microstructure pilot for HYDRA campaign 0031.

This module is intentionally a *pilot runner*, not another research platform.
It consumes immutable Databento MBO files (or canonical ``MarketEvent`` objects
in tests), reconstructs the book through :class:`MicrostructureEventEngine`,
materialises a causal feature lattice, creates outcome-only MBO teacher labels,
fits deployable L1/L2 students, and evaluates a frozen 24-sleeve bank.

The important boundaries are explicit:

* raw data is referenced by content hash and is never rewritten;
* decision features are persisted separately from future outcome labels;
* both batch and streaming reconstruction call the same event-engine ``step``;
* aggressive fills consume displayed depth after a personal-device latency;
* passive fills require observed contra-aggressor volume to consume quantity
  ahead -- merely touching a limit never fills it;
* 5/10/20-session account windows enter denominators only with full coverage;
* a pilot can only be GREEN on final-development economics from a deployable
  student and three distinct mechanism families.

The runner fails closed when book reconstruction, temporal roles, causal
availability, or the minimum candidate inventory is incomplete.  It does not
manufacture metrics when a purchased sample is too small.
"""

from __future__ import annotations

from collections import defaultdict, deque
from array import array
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import heapq
import json
import math
import os
from pathlib import Path
import random
import resource
import shutil
import tempfile
import time
from collections.abc import Mapping as ABCMapping
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from hydra.economic_evolution.schema import stable_hash
from hydra.production.microstructure_event_engine import (
    BookStateError,
    CompactEventResult,
    EventResult,
    F_SNAPSHOT,
    MarketEvent,
    MicrostructureEventEngine,
    MicrostructureEventEngineError,
)
from hydra.production.microstructure_teacher_student import (
    FeatureTable,
    FrozenStudent,
    L1_FEATURES,
    L2_FEATURES,
    ROLE_DISCOVERY,
    ROLE_FINAL,
    ROLE_VALIDATION,
    StudentResult,
    TeacherLabelSet,
    build_mbo_teacher_labels,
    score_frozen_student,
    train_deployable_students,
)


FOUNDRY_PILOT_VERSION = "hydra_microstructure_order_flow_foundry_pilot_v1"
EVENT_STORE_SCHEMA = "hydra_microstructure_event_store_v1"
PILOT_DECISIONS = (
    "MICROSTRUCTURE_PILOT_GREEN",
    "MICROSTRUCTURE_PILOT_WEAK",
    "MICROSTRUCTURE_PILOT_FALSIFIED",
)
EXPERT_FAMILIES = (
    "ABSORPTION_REVERSAL",
    "INITIATIVE_CONTINUATION",
    "LIQUIDITY_VACUUM_CONTINUATION",
    "EXHAUSTION_REVERSAL",
    "VWAP_ACCEPTANCE_REJECTION",
    "OPENING_DRIVE",
    "CROSS_ASSET_FLOW_DIVERGENCE",
    "QUEUE_REPLENISHMENT",
)
DEPLOYABILITY_TIERS = (
    "L1_DEPLOYABLE",
    "L2_DEPLOYABLE",
    "MBO_TEACHER_ONLY",
    "UNDEPLOYABLE",
)
EXECUTION_PATHS = ("AGGRESSIVE", "PASSIVE")
COST_SCENARIOS = ("NORMAL", "STRESSED_1_5X")
HORIZONS_DAYS = (5, 10, 20)
CONTROL_IDS = (
    "DIRECTION_FLIP",
    "SESSION_MATCHED_TIMING_NULL",
    "EXPOSURE_MATCHED_RANDOM",
)


class FoundryPilotError(RuntimeError):
    """The pilot input or derived evidence is causally/economically invalid."""


@dataclass(frozen=True, slots=True)
class FoundryPilotConfig:
    campaign_id: str = "hydra_microstructure_order_flow_foundry_0031"
    manifest_hash: str = "UNBOUND_TEST_MANIFEST"
    source_commit: str = "0" * 40
    acquisition_receipt_hash: str = "0" * 64
    selected_markets: tuple[str, str] = ("NQ", "YM")
    contracts: Mapping[str, str] = field(
        default_factory=lambda: {"NQ": "NQU4", "YM": "YMU4"}
    )
    chronological_roles: tuple[int, int, int] = (3, 1, 1)
    decision_cadence_ns: int = 1_000_000_000
    outcome_horizon_ns: int = 30_000_000_000
    favorable_ticks: float = 4.0
    adverse_ticks: float = 3.0
    aggressive_latency_ns: int = 25_000_000
    passive_latency_ns: int = 50_000_000
    normal_round_turn_commission_usd: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 3.80, "YM": 3.80, "ES": 3.80}
    )
    stressed_cost_multiplier: float = 1.5
    adverse_slippage_ticks_per_side: float = 2.0
    quantity: int = 1
    combine_profit_target_usd: float = 9_000.0
    combine_mll_usd: float = 4_500.0
    consistency_limit: float = 0.50
    minimum_candidates: int = 20
    maximum_candidates: int = 40
    minimum_useful_families: int = 3
    minimum_final_opportunities: int = 1
    minimum_material_uplift_ratio: float = 1.5
    minimum_material_uplift_points: float = 0.05
    maximum_mll_breach_rate: float = 0.10
    baseline_stressed_target_progress_pct: float = 8.097
    baseline_population_median_stressed_target_progress_pct: float = 0.272
    random_seed: int = 31_031
    tick_size: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 0.25, "YM": 1.0, "ES": 0.25}
    )
    point_value: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 20.0, "YM": 5.0, "ES": 50.0}
    )

    def validate(self) -> None:
        if len(self.selected_markets) != 2 or len(set(self.selected_markets)) != 2:
            raise FoundryPilotError("pilot requires exactly two distinct markets")
        if tuple(self.chronological_roles) != (3, 1, 1):
            raise FoundryPilotError("pilot chronological roles must remain 3/1/1")
        if not 20 <= self.minimum_candidates <= self.maximum_candidates <= 40:
            raise FoundryPilotError("pilot candidate bounds drifted")
        if self.decision_cadence_ns <= 0 or self.outcome_horizon_ns <= 0:
            raise FoundryPilotError("pilot causal horizons must be positive")
        if self.aggressive_latency_ns < 0 or self.passive_latency_ns < 0:
            raise FoundryPilotError("execution latency cannot be negative")
        if not 0 < self.consistency_limit <= 0.5:
            raise FoundryPilotError("Combine consistency contract drifted")
        for market in self.selected_markets:
            if (
                market not in self.contracts
                or market not in self.tick_size
                or market not in self.point_value
                or market not in self.normal_round_turn_commission_usd
            ):
                raise FoundryPilotError(f"market contract economics absent: {market}")


@dataclass(frozen=True, slots=True)
class RawSource:
    market: str
    contract: str
    schema: str
    path: str
    sha256: str
    byte_count: int


@dataclass(frozen=True, slots=True)
class _InstrumentRoute:
    market: str
    contract: str
    instrument_id: str
    start_date: str
    end_date: str


@dataclass(frozen=True, slots=True)
class FeatureSnapshot:
    market: str
    contract: str
    session_id: str
    event_ns: int
    available_ns: int
    decision_ns: int
    event_fingerprint: str
    state_hash: str
    trigger_action: str
    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int
    bid_depth: tuple[tuple[float, int], ...]
    ask_depth: tuple[tuple[float, int], ...]
    last_trade_price: float
    last_trade_side: str
    last_trade_size: int
    names: tuple[str, ...]
    values: tuple[float, ...]
    feature_hash: str


@dataclass(frozen=True, slots=True)
class OutcomeRow:
    feature_hash: str
    market: str
    decision_ns: int
    direction: int
    status: str
    favorable_first: bool
    adverse_first: bool
    time_to_favorable_ns: int | None
    time_to_adverse_ns: int | None
    mfe_ticks: float | None
    mae_ticks: float | None
    future_markout_ticks: float | None


@dataclass(frozen=True, slots=True)
class SleeveSpec:
    sleeve_id: str
    family: str
    variant: str
    market: str
    deployability_tier: str
    execution_path: str
    direction_mode: str
    feature_names: tuple[str, ...]
    score_threshold: float
    quantity: int
    stop_ticks: float
    target_ticks: float
    maximum_holding_seconds: int
    entry_rule: str
    exit_rule: str
    session_rule: str
    contract_rule: str
    feature_hash: str
    model_hash: str

    @property
    def fingerprint(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True, slots=True)
class SignalIntent:
    sleeve_id: str
    signal_id: str
    feature_hash: str
    feature_index: int
    market: str
    session_id: str
    direction: int
    score: float
    signal_time_ns: int
    decision_time_ns: int
    order_submit_time_ns: int
    earliest_executable_time_ns: int
    execution_path: str


@dataclass(frozen=True, slots=True)
class ExecutedTrade:
    sleeve_id: str
    trade_id: str
    signal_id: str
    market: str
    session_id: str
    role: str
    execution_path: str
    direction: int
    requested_quantity: int
    filled_quantity: int
    quantity_ahead: int
    entry_time_ns: int
    exit_time_ns: int
    entry_price: float
    exit_price: float
    stop_price: float
    target_price: float
    exit_reason: str
    gross_pnl_usd: float
    base_slippage_cost_usd: float
    normal_total_cost_usd: float
    stressed_total_cost_usd: float
    normal_costs_usd: float
    normal_net_pnl_usd: float
    stressed_costs_usd: float
    stressed_net_pnl_usd: float
    minimum_unrealized_pnl_usd: float


@dataclass(frozen=True, slots=True)
class CandidateResult:
    sleeve: SleeveSpec
    signals: tuple[SignalIntent, ...]
    trades: tuple[ExecutedTrade, ...]
    economics: Mapping[str, Any]
    episodes: tuple[Mapping[str, Any], ...]
    controls: Mapping[str, Mapping[str, float]]
    serious: bool


@dataclass(frozen=True, slots=True)
class FoundryPilotResult(ABCMapping[str, Any]):
    campaign_id: str
    pilot_status: str
    event_store_status: str
    output_dir: str
    store_receipt: Mapping[str, Any]
    decision_report: Mapping[str, Any]
    evidence_identity: Mapping[str, Any]
    evidence_datasets: Mapping[str, Sequence[Mapping[str, Any]]]
    compact_outputs: Mapping[str, Any]
    event_store_paths: Mapping[str, str]
    runtime_kpis: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "pilot_status": self.pilot_status,
            "event_store_status": self.event_store_status,
            "output_dir": self.output_dir,
            "store_receipt": self.store_receipt,
            "decision_report": self.decision_report,
            "evidence_identity": self.evidence_identity,
            "evidence_datasets": self.evidence_datasets,
            "compact_outputs": self.compact_outputs,
            "event_store_paths": self.event_store_paths,
            "runtime_kpis": self.runtime_kpis,
            "decision": self.pilot_status,
            "event_count": int(self.runtime_kpis.get("event_count", 0)),
            "teacher_event_count": int(self.runtime_kpis.get("teacher_event_count", 0)),
            "students_evaluated": int(self.runtime_kpis.get("students_evaluated", 0)),
            "sleeves_evaluated": int(self.runtime_kpis.get("sleeves_evaluated", 0)),
            "economic_wall_clock_fraction": float(self.runtime_kpis.get("economic_wall_clock_fraction", 0.0)),
            "cpu_utilization_fraction": float(self.runtime_kpis.get("cpu_utilization_fraction", 0.0)),
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self):
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())


_FEATURE_NAMES = (
    "aggressor_delta",
    "signed_volume",
    "trade_arrival_rate",
    "notional_rate",
    "bbo_imbalance",
    "microprice_deviation",
    "spread_ticks",
    "vwap_distance",
    "vwap_slope",
    "event_volatility",
    "cross_market_flow_alignment",
    "cross_market_microprice_divergence",
    "depth_3_imbalance",
    "depth_5_imbalance",
    "depth_10_imbalance",
    "depth_slope",
    "depth_convexity",
    "depletion_rate",
    "replenishment_rate",
    "liquidity_gap_ticks",
    "price_response_per_signed_contract",
    "depth_withdrawal_rate",
    "queue_persistence",
    "quantity_ahead",
    "order_age_mean",
    "cancel_ahead_rate",
    "cancel_behind_rate",
    "ephemeral_liquidity_rate",
    "flow_2s",
    "flow_30s",
    "flow_5m",
    "arrival_2s",
    "arrival_30s",
    "session_elapsed_fraction",
    "opening_drive",
)


@dataclass(slots=True)
class _TradeWindow:
    horizon_ns: int
    rows: deque[tuple[int, int, int, float, float]] = field(default_factory=deque)
    signed_volume: float = 0.0
    volume: float = 0.0
    notional: float = 0.0
    price_sum: float = 0.0
    price_sum_squares: float = 0.0

    def add(self, now: int, direction: int, size: int, price: float) -> None:
        notional = float(size) * price
        self.rows.append((now, direction, size, notional, price))
        self.signed_volume += direction * size
        self.volume += size
        self.notional += notional
        self.price_sum += price
        self.price_sum_squares += price * price

    def evict(self, now: int) -> None:
        while self.rows and now - self.rows[0][0] > self.horizon_ns:
            _, direction, size, notional, price = self.rows.popleft()
            self.signed_volume -= direction * size
            self.volume -= size
            self.notional -= notional
            self.price_sum -= price
            self.price_sum_squares -= price * price

    @property
    def count(self) -> int:
        return len(self.rows)

    @property
    def price_std(self) -> float:
        if self.count <= 2:
            return 0.0
        variance = max(
            0.0,
            self.price_sum_squares / self.count
            - (self.price_sum / self.count) ** 2,
        )
        return math.sqrt(variance)


@dataclass(slots=True)
class _ActionWindow:
    rows: deque[tuple[int, str]] = field(default_factory=deque)
    adds: int = 0
    cancels: int = 0

    def add(self, now: int, action: str) -> None:
        if action not in {"A", "C"}:
            return
        self.rows.append((now, action))
        if action == "A":
            self.adds += 1
        else:
            self.cancels += 1

    def evict(self, now: int, horizon_ns: int = 2_000_000_000) -> None:
        while self.rows and now - self.rows[0][0] > horizon_ns:
            _, action = self.rows.popleft()
            if action == "A":
                self.adds -= 1
            else:
                self.cancels -= 1


@dataclass(slots=True)
class _RollingMarketState:
    trade_2s: _TradeWindow = field(default_factory=lambda: _TradeWindow(2_000_000_000))
    trade_30s: _TradeWindow = field(default_factory=lambda: _TradeWindow(30_000_000_000))
    trade_5m: _TradeWindow = field(default_factory=lambda: _TradeWindow(300_000_000_000))
    actions_2s: _ActionWindow = field(default_factory=_ActionWindow)
    last_snapshot_ns: int = -1
    last_price: float = math.nan
    previous_price: float = math.nan
    last_trade_side: str = "N"
    last_trade_size: int = 0
    previous_bid_depth: int = 0
    previous_ask_depth: int = 0
    order_birth_ns: dict[str, int] = field(default_factory=dict)
    order_size: dict[str, int] = field(default_factory=dict)
    order_birth_sum_ns: int = 0
    maturity_heap: list[tuple[int, str, int]] = field(default_factory=list)
    matured_order_ids: set[str] = field(default_factory=set)
    cancel_count: int = 0
    add_count: int = 0
    fill_count: int = 0
    last_vwap: float = math.nan
    previous_vwap: float = math.nan


@dataclass(frozen=True, slots=True)
class _CompactTape:
    available_ns: np.ndarray
    price: np.ndarray
    size: np.ndarray
    side: np.ndarray
    session_code: np.ndarray
    sessions: tuple[str, ...]

    def __len__(self) -> int:
        return len(self.available_ns)

    def session_at(self, index: int) -> str:
        return self.sessions[int(self.session_code[index])]

    def records(self) -> Iterator[tuple[int, float, int, str, str]]:
        for index in range(len(self)):
            direction = int(self.side[index])
            yield (
                int(self.available_ns[index]),
                float(self.price[index]),
                int(self.size[index]),
                "B" if direction > 0 else "A" if direction < 0 else "N",
                self.session_at(index),
            )


@dataclass(slots=True)
class _TapeBuilder:
    available_ns: array = field(default_factory=lambda: array("q"))
    price: array = field(default_factory=lambda: array("d"))
    size: array = field(default_factory=lambda: array("i"))
    side: array = field(default_factory=lambda: array("b"))
    session_code: array = field(default_factory=lambda: array("H"))
    sessions: list[str] = field(default_factory=list)
    session_index: dict[str, int] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.available_ns)

    def append(self, event: MarketEvent) -> None:
        assert event.price is not None
        code = self.session_index.get(event.session_id)
        if code is None:
            code = len(self.sessions)
            if code >= 65_535:
                raise FoundryPilotError("trade tape session inventory overflow")
            self.sessions.append(event.session_id)
            self.session_index[event.session_id] = code
        self.available_ns.append(event.available_at_ns)
        self.price.append(float(event.price))
        self.size.append(int(event.size))
        self.side.append(1 if event.side == "B" else -1 if event.side == "A" else 0)
        self.session_code.append(code)

    def freeze(self) -> _CompactTape:
        return _CompactTape(
            available_ns=np.frombuffer(self.available_ns, dtype=np.int64).copy(),
            price=np.frombuffer(self.price, dtype=np.float64).copy(),
            size=np.frombuffer(self.size, dtype=np.int32).copy(),
            side=np.frombuffer(self.side, dtype=np.int8).copy(),
            session_code=np.frombuffer(self.session_code, dtype=np.uint16).copy(),
            sessions=tuple(self.sessions),
        )


def run_microstructure_foundry_pilot(
    raw_paths: Mapping[str, str | Path] | Sequence[str | Path],
    output_dir: str | Path,
    *,
    contracts: Sequence[str] | None = None,
    config: FoundryPilotConfig | Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
) -> FoundryPilotResult:
    """Run campaign 0031 from immutable DBN MBO inputs, chunk by chunk."""

    cfg = _bind_config(_coerce_config(config, contracts=contracts), manifest)
    if not isinstance(raw_paths, Mapping):
        path_items = tuple(raw_paths)
        if len(path_items) == 1:
            raw_paths = {market: path_items[0] for market in cfg.selected_markets}
        else:
            raw_paths = dict(zip(cfg.selected_markets, path_items, strict=True))
    sources: list[RawSource] = []
    path_groups: dict[Path, list[str]] = defaultdict(list)
    for market in cfg.selected_markets:
        raw = Path(raw_paths[market]).resolve()
        if not raw.is_file():
            raise FoundryPilotError(f"raw MBO source absent: {market}: {raw}")
        sources.append(
            RawSource(
                market=market,
                contract=cfg.contracts[market],
                schema="mbo",
                path=str(raw),
                sha256=_sha256_file(raw),
                byte_count=raw.stat().st_size,
            )
        )
        path_groups[raw].append(market)
    tagged_sources: list[Iterator[tuple[str, MarketEvent]]] = []
    for raw, markets in sorted(path_groups.items(), key=lambda value: str(value[0])):
        tagged_sources.append(
            iter_dbn_mbo_events_multi(
                raw,
                market_contracts=tuple((market, cfg.contracts[market]) for market in markets),
            )
        )
    merged = _merge_tagged_sources(tagged_sources)
    return _run_pilot(merged, output_dir, cfg=cfg, raw_sources=tuple(sources))


def run_microstructure_foundry_pilot_from_events(
    events_by_market: Mapping[str, Sequence[MarketEvent | Mapping[str, Any]]],
    output_dir: str | Path,
    *,
    config: FoundryPilotConfig | Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
) -> FoundryPilotResult:
    """Synthetic/reference entry point that exercises the exact production path."""

    cfg = _bind_config(_coerce_config(config, contracts=None), manifest)
    iterators: dict[str, Iterator[MarketEvent]] = {}
    sources: list[RawSource] = []
    for market in cfg.selected_markets:
        raw_events = events_by_market.get(market)
        if not raw_events:
            raise FoundryPilotError(f"synthetic event source absent: {market}")
        events = tuple(MarketEvent.from_record(value).validated() for value in raw_events)
        payload_hash = stable_hash([event.to_record() for event in events])
        sources.append(
            RawSource(
                market=market,
                contract=cfg.contracts[market],
                schema="mbo",
                path=f"synthetic://{market}/{payload_hash}",
                sha256=payload_hash,
                byte_count=0,
            )
        )
        iterators[market] = iter(sorted(events, key=lambda value: (value.available_at_ns, value.ts_event_ns, value.sequence)))
    return _run_pilot(
        _merge_market_iterators(iterators),
        output_dir,
        cfg=cfg,
        raw_sources=tuple(sources),
    )


def _bind_config(
    config: FoundryPilotConfig, manifest: Mapping[str, Any] | None
) -> FoundryPilotConfig:
    config.validate()
    if manifest is None:
        return config
    campaign_id = str(manifest.get("campaign_id") or config.campaign_id)
    manifest_hash = stable_hash(manifest)
    if campaign_id != config.campaign_id:
        raise FoundryPilotError("pilot manifest/campaign identity drift")
    return FoundryPilotConfig(**{**asdict(config), "manifest_hash": manifest_hash})


def _coerce_config(
    value: FoundryPilotConfig | Mapping[str, Any] | None,
    *,
    contracts: Sequence[str] | None,
) -> FoundryPilotConfig:
    if value is None:
        if contracts is None:
            return FoundryPilotConfig()
        markets = ("NQ", "YM")
        return FoundryPilotConfig(contracts=dict(zip(markets, contracts, strict=True)))
    if isinstance(value, FoundryPilotConfig):
        return value
    markets = tuple(str(item) for item in value.get("markets", ("NQ", "YM")))
    raw_contracts = tuple(contracts or value.get("contracts", ("NQU4", "YMU4")))
    contract_map = dict(zip(markets, raw_contracts, strict=True))
    account = value.get("account_rule_snapshot")
    account = account if isinstance(account, Mapping) else {}
    baseline = value.get("best_ohlcv_baseline")
    baseline = baseline if isinstance(baseline, Mapping) else {}
    return FoundryPilotConfig(
        campaign_id=str(value.get("campaign_id") or "hydra_microstructure_order_flow_foundry_0031"),
        manifest_hash=str(value.get("manifest_hash") or "UNBOUND_TEST_MANIFEST"),
        source_commit=str(value.get("source_commit") or "0" * 40),
        acquisition_receipt_hash=str(value.get("acquisition_receipt_hash") or "0" * 64),
        selected_markets=(str(markets[0]), str(markets[1])),
        contracts=contract_map,
        combine_profit_target_usd=float(account.get("profit_target_usd", account.get("profit_target", 9_000.0))),
        combine_mll_usd=float(account.get("mll_usd", account.get("maximum_loss_limit_usd", 4_500.0))),
        consistency_limit=float(
            account.get(
                "best_day_consistency_fraction",
                account.get("consistency_fraction", account.get("consistency_limit", 0.50)),
            )
        ),
        baseline_stressed_target_progress_pct=float(
            baseline.get(
                "best_median_stressed_target_progress_pct",
                baseline.get("stressed_target_progress_pct", 8.097),
            )
        ),
        baseline_population_median_stressed_target_progress_pct=float(
            baseline.get("population_median_stressed_target_progress_pct", 0.272)
        ),
    )


def _merge_market_iterators(
    iterators: Mapping[str, Iterator[MarketEvent]],
) -> Iterator[tuple[str, MarketEvent]]:
    heap: list[tuple[int, int, str, int, MarketEvent]] = []
    counters = {market: 0 for market in iterators}
    for market, iterator in iterators.items():
        try:
            event = next(iterator)
        except StopIteration:
            continue
        heap.append((event.available_at_ns, event.ts_event_ns, market, 0, event))
    heapq.heapify(heap)
    while heap:
        _, _, market, _, event = heapq.heappop(heap)
        yield market, event
        counters[market] += 1
        try:
            nxt = next(iterators[market])
        except StopIteration:
            continue
        heapq.heappush(
            heap,
            (nxt.available_at_ns, nxt.ts_event_ns, market, counters[market], nxt),
        )


def _merge_tagged_sources(
    sources: Sequence[Iterator[tuple[str, MarketEvent]]],
) -> Iterator[tuple[str, MarketEvent]]:
    if len(sources) == 1:
        yield from sources[0]
        return
    heap: list[tuple[int, int, str, int, int, MarketEvent]] = []
    counters = [0 for _ in sources]
    for source_index, source in enumerate(sources):
        try:
            market, event = next(source)
        except StopIteration:
            continue
        heap.append(
            (
                event.available_at_ns,
                event.ts_event_ns,
                market,
                source_index,
                0,
                event,
            )
        )
    heapq.heapify(heap)
    while heap:
        _, _, market, source_index, _, event = heapq.heappop(heap)
        yield market, event
        counters[source_index] += 1
        try:
            next_market, next_event = next(sources[source_index])
        except StopIteration:
            continue
        heapq.heappush(
            heap,
            (
                next_event.available_at_ns,
                next_event.ts_event_ns,
                next_market,
                source_index,
                counters[source_index],
                next_event,
            ),
        )


def iter_dbn_mbo_events(
    path: str | Path,
    *,
    market: str,
    contract: str,
    chunk_size: int = 250_000,
) -> Iterator[MarketEvent]:
    """Decode a DBN MBO stream without loading the purchased sample in memory."""

    for routed_market, event in iter_dbn_mbo_events_multi(
        path,
        market_contracts=((market, contract),),
        chunk_size=chunk_size,
    ):
        if routed_market != market:  # pragma: no cover - routing invariant
            raise FoundryPilotError("single-market DBN routing drift")
        yield event


def iter_dbn_mbo_events_multi(
    path: str | Path,
    *,
    market_contracts: Sequence[tuple[str, str]],
    chunk_size: int = 250_000,
) -> Iterator[tuple[str, MarketEvent]]:
    """Decode a combined DBN once and route each instrument exactly once."""

    try:
        import databento as db
    except ImportError as exc:  # pragma: no cover - production environment guard
        raise FoundryPilotError("databento package is required to read RAW_DBN") from exc
    store = db.DBNStore.from_file(Path(path))
    yield from iter_dbn_mbo_events_multi_from_store(
        store,
        market_contracts=market_contracts,
        chunk_size=chunk_size,
    )


def iter_dbn_mbo_events_multi_from_store(
    store: Any,
    *,
    market_contracts: Sequence[tuple[str, str]],
    chunk_size: int = 250_000,
) -> Iterator[tuple[str, MarketEvent]]:
    mappings = getattr(getattr(store, "metadata", None), "mappings", None)
    if mappings is None:
        mappings = getattr(store, "mappings", None)
    routes = _resolve_dbn_instrument_routes(mappings, market_contracts)
    routed_counts = {str(market): 0 for market, _ in market_contracts}
    unknown_instrument_counts: dict[str, int] = defaultdict(int)
    local_sequence = 0
    for chunk in store.to_ndarray(count=int(chunk_size)):
        names = set(chunk.dtype.names or ())
        required = {"ts_event", "publisher_id", "instrument_id", "action", "side", "price", "size"}
        missing = required - names
        if missing:
            raise FoundryPilotError(f"RAW_DBN is not MBO-complete: {sorted(missing)}")
        for row in chunk:
            instrument_id = str(int(row["instrument_id"]))
            action = _enum_char(row["action"])
            if action == "N":
                continue
            side = _enum_char(row["side"])
            ts_event = int(row["ts_event"])
            ts_recv = int(row["ts_recv"]) if "ts_recv" in names else ts_event
            available = max(ts_event, ts_recv)
            event_date = datetime.fromtimestamp(ts_event / 1_000_000_000, tz=UTC).date().isoformat()
            matching_routes = [
                route
                for route in routes.get(instrument_id, ())
                if (not route.start_date or route.start_date <= event_date)
                and (not route.end_date or event_date < route.end_date)
            ]
            if not matching_routes:
                unknown_instrument_counts[instrument_id] += 1
                continue
            if len(matching_routes) != 1:
                raise FoundryPilotError(
                    f"DBN instrument {instrument_id} has ambiguous temporal routing"
                )
            route = matching_routes[0]
            local_sequence += 1
            vendor_sequence = int(row["sequence"]) if "sequence" in names else local_sequence
            price = (
                _dbn_price(row["price"])
                if action in {"A", "M", "T"}
                else None
            )
            order_id = None
            if action in {"A", "M", "C", "F"}:
                if "order_id" not in names:
                    raise FoundryPilotError("MBO order mutation lacks order_id")
                order_id = str(int(row["order_id"]))
            flags = int(row["flags"]) if "flags" in names else 0
            event = MarketEvent(
                ts_event_ns=ts_event,
                available_at_ns=available,
                ts_recv_ns=ts_recv,
                sequence=vendor_sequence,
                publisher_id=str(int(row["publisher_id"])),
                instrument_id=instrument_id,
                action=action,
                side=side,
                price=price,
                size=int(row["size"]),
                order_id=order_id,
                flags=flags,
                session_id=_cme_session_id(ts_event),
                is_snapshot=_dbn_snapshot_flag(flags),
                schema="mbo",
            ).validated()
            routed_counts[route.market] += 1
            yield route.market, event
    if unknown_instrument_counts:
        detail = ", ".join(
            f"{instrument_id}:{count}"
            for instrument_id, count in sorted(unknown_instrument_counts.items())[:10]
        )
        raise FoundryPilotError(
            f"combined RAW_DBN contains unresolved instrument IDs: {detail}"
        )
    missing = [market for market, count in routed_counts.items() if count == 0]
    if missing:
        raise FoundryPilotError(
            "combined RAW_DBN contains no routed events for: " + ", ".join(missing)
        )


def _resolve_dbn_instrument_routes(
    mappings: Any,
    market_contracts: Sequence[tuple[str, str]],
) -> Mapping[str, tuple[_InstrumentRoute, ...]]:
    if not isinstance(mappings, Mapping) or not mappings:
        raise FoundryPilotError("RAW_DBN symbology mappings are unavailable")
    requested = {str(contract).upper(): str(market) for market, contract in market_contracts}
    routes: dict[str, list[_InstrumentRoute]] = defaultdict(list)
    found_contracts: set[str] = set()
    for raw_symbol, intervals in mappings.items():
        contract = str(raw_symbol).upper()
        market = requested.get(contract)
        if market is None:
            continue
        found_contracts.add(contract)
        for interval in intervals or ():
            raw = interval if isinstance(interval, Mapping) else vars(interval)
            instrument_id = str(raw.get("symbol") or raw.get("s") or "")
            if not instrument_id:
                raise FoundryPilotError(
                    f"RAW_DBN symbology interval lacks instrument_id: {contract}"
                )
            routes[instrument_id].append(
                _InstrumentRoute(
                    market=market,
                    contract=contract,
                    instrument_id=instrument_id,
                    start_date=str(raw.get("start_date") or raw.get("d0") or ""),
                    end_date=str(raw.get("end_date") or raw.get("d1") or ""),
                )
            )
    missing = set(requested) - found_contracts
    if missing:
        raise FoundryPilotError(
            "RAW_DBN symbology lacks explicit contracts: "
            + ", ".join(sorted(missing))
        )
    return {instrument_id: tuple(values) for instrument_id, values in routes.items()}


def _enum_char(value: Any) -> str:
    if isinstance(value, (bytes, np.bytes_)):
        return bytes(value).decode("ascii").strip().upper()
    if isinstance(value, str):
        return value.strip().upper()
    number = int(value)
    if 0 <= number <= 255:
        return chr(number).strip().upper()
    return str(value).strip().upper()


def _dbn_price(value: Any) -> float:
    number = float(value)
    # DBN ndarray records carry fixed-point prices at 1e-9 precision.
    if isinstance(value, (int, np.integer)) or abs(number) > 10_000_000:
        number /= 1_000_000_000.0
    if not math.isfinite(number) or number <= 0:
        raise FoundryPilotError("RAW_DBN contains an invalid economic price")
    return number


def _dbn_snapshot_flag(flags: int) -> bool:
    """Databento ``F_SNAPSHOT`` is bit 0x20; action R alone is not a snapshot."""

    return bool(int(flags) & 0x20)


def _cme_session_id(ts_ns: int) -> str:
    value = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC)
    trade_date = value.date() + (timedelta(days=1) if value.hour >= 22 else timedelta())
    return trade_date.isoformat()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_pilot(
    merged_events: Iterable[tuple[str, MarketEvent]],
    output_dir: str | Path,
    *,
    cfg: FoundryPilotConfig,
    raw_sources: tuple[RawSource, ...],
) -> FoundryPilotResult:
    wall_started = time.perf_counter()
    cpu_started = resource.getrusage(resource.RUSAGE_SELF)
    target = Path(output_dir).resolve()
    if target.exists():
        raise FoundryPilotError(f"immutable pilot destination already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    try:
        snapshots, tape_events, reconstruction = _reconstruct_features(
            merged_events, cfg=cfg
        )
        selected_sessions, roles = _freeze_roles(snapshots, cfg=cfg)
        snapshots = tuple(value for value in snapshots if value.session_id in roles)
        tape_events = {
            market: _filter_tape_sessions(rows, roles)
            for market, rows in tape_events.items()
        }
        outcomes = _build_outcomes(snapshots, cfg=cfg)
        table = _teacher_feature_table(snapshots, outcomes, roles)
        teachers = build_mbo_teacher_labels(table)
        students = train_deployable_students(
            table,
            teachers,
            minimum_final_opportunities=cfg.minimum_final_opportunities,
        )
        sleeves, scores = _freeze_sleeves(
            snapshots,
            table,
            teachers,
            students,
            roles,
            cfg=cfg,
        )
        if not cfg.minimum_candidates <= len(sleeves) <= cfg.maximum_candidates:
            raise FoundryPilotError("frozen pilot sleeve count is outside 20--40")

        by_market = _snapshots_by_market(snapshots)
        candidate_results: list[CandidateResult] = []
        for sleeve in sleeves:
            candidate_results.append(
                _evaluate_candidate(
                    sleeve,
                    scores[sleeve.sleeve_id],
                    by_market[sleeve.market],
                    tape_events[sleeve.market],
                    selected_sessions,
                    roles,
                    cfg=cfg,
                )
            )

        report = _decision_report(
            cfg=cfg,
            raw_sources=raw_sources,
            reconstruction=reconstruction,
            selected_sessions=selected_sessions,
            roles=roles,
            snapshots=snapshots,
            outcomes=outcomes,
            teachers=teachers,
            students=students,
            candidates=tuple(candidate_results),
            wall_seconds=max(1e-9, time.perf_counter() - wall_started),
            cpu_seconds=max(
                0.0,
                (resource.getrusage(resource.RUSAGE_SELF).ru_utime - cpu_started.ru_utime)
                + (resource.getrusage(resource.RUSAGE_SELF).ru_stime - cpu_started.ru_stime),
            ),
        )
        event_store_paths = _persist_event_store(
            staging,
            cfg=cfg,
            raw_sources=raw_sources,
            snapshots=snapshots,
            tape_events=tape_events,
            outcomes=outcomes,
            teachers=teachers,
            students=students,
            candidates=tuple(candidate_results),
            report=report,
        )
        receipt = _seal_store_receipt(
            staging,
            cfg=cfg,
            datasets=event_store_paths,
            reconstruction=reconstruction,
            report=report,
        )
        os.replace(staging, target)
        relative_paths = {
            key: str(target / Path(value).relative_to(staging))
            for key, value in event_store_paths.items()
        }
        receipt = json.loads((target / "store_receipt.json").read_text())
        identity, evidence_datasets, compact_outputs = _canonical_evidence_material(
            cfg=cfg,
            raw_sources=raw_sources,
            selected_sessions=selected_sessions,
            roles=roles,
            candidates=tuple(candidate_results),
            report=report,
            event_store_paths=relative_paths,
            store_receipt=receipt,
        )
        total_wall = max(1e-9, time.perf_counter() - wall_started)
        economic_wall = float(report["production_kpis"]["economic_wall_seconds"])
        economic_cpu = float(report["production_kpis"]["economic_cpu_seconds"])
        runtime_kpis = {
            "event_count": int(reconstruction["event_count"]),
            "teacher_event_count": int(
                sum(
                    count
                    for family in teachers.counts_by_role.values()
                    for count in family.values()
                )
            ),
            "students_evaluated": len(students),
            "sleeves_evaluated": len(candidate_results),
            "economic_wall_clock_fraction": min(1.0, economic_wall / total_wall),
            "cpu_utilization_fraction": min(1.0, economic_cpu / max(economic_wall, 1e-9)),
        }
        return FoundryPilotResult(
            campaign_id=cfg.campaign_id,
            pilot_status=str(report["pilot_status"]),
            event_store_status=str(reconstruction["status"]),
            output_dir=str(target),
            store_receipt=receipt,
            decision_report=report,
            evidence_identity=identity,
            evidence_datasets=evidence_datasets,
            compact_outputs=compact_outputs,
            event_store_paths=relative_paths,
            runtime_kpis=runtime_kpis,
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _reconstruct_features(
    merged_events: Iterable[tuple[str, MarketEvent]],
    *,
    cfg: FoundryPilotConfig,
) -> tuple[
    tuple[FeatureSnapshot, ...],
    Mapping[str, _CompactTape],
    Mapping[str, Any],
]:
    engine = MicrostructureEventEngine(
        depth_levels=10,
        strict_contiguous_sequence=False,
        recent_event_limit=16_384,
    )
    rolling = {market: _RollingMarketState() for market in cfg.selected_markets}
    latest: dict[str, FeatureSnapshot] = {}
    snapshots: list[FeatureSnapshot] = []
    tape: dict[str, _TapeBuilder] = {
        market: _TapeBuilder() for market in cfg.selected_markets
    }
    counts = defaultdict(int)
    encountered_streams: dict[str, set[tuple[str, str]]] = {
        market: set() for market in cfg.selected_markets
    }
    initialized_streams: dict[str, set[tuple[str, str]]] = {
        market: set() for market in cfg.selected_markets
    }
    snapshot_started_streams: set[tuple[str, str]] = set()
    stream_ready: dict[tuple[str, str], bool] = {}
    snapshot_resets_by_market = defaultdict(int)
    first_available: int | None = None
    last_available: int | None = None
    for market, event in merged_events:
        if market not in rolling:
            raise FoundryPilotError(f"unexpected market in event stream: {market}")
        if event.available_at_ns < event.ts_event_ns:
            raise FoundryPilotError("event availability violates causal contract")
        authenticated_snapshot = bool(event.flags & F_SNAPSHOT)
        stream_key = (event.publisher_id, event.instrument_id)
        encountered_streams[market].add(stream_key)
        counts[market] += 1
        counts[f"action_{event.action}"] += 1
        first_available = event.available_at_ns if first_available is None else min(first_available, event.available_at_ns)
        last_available = event.available_at_ns if last_available is None else max(last_available, event.available_at_ns)

        # A vendor request can begin during Sunday's Globex session, before the
        # first authenticated daily snapshot at 00:00 UTC.  Those leading raw
        # messages are immutable provenance, but applying a C/M/F/T to an
        # empty state would either fabricate a partial book or fail on an
        # unknown order.  The first authenticated snapshot R is therefore the
        # only permitted state-engine bootstrap for each stream.
        initial_snapshot_reset = authenticated_snapshot and event.action == "R"
        if stream_key not in snapshot_started_streams:
            if not initial_snapshot_reset:
                counts["events_gated_before_initial_snapshot"] += 1
                counts["events_gated_until_snapshot_f_last"] += 1
                continue
            snapshot_started_streams.add(stream_key)
        try:
            compact = engine.step(event, materialize=False)
        except (BookStateError, MicrostructureEventEngineError) as exc:
            raise FoundryPilotError(
                f"order-book reconstruction failed closed at {market} "
                f"sequence={event.sequence}: {exc}"
            ) from exc
        counts["state_engine_events"] += 1
        stream_status = engine.stream_status(*stream_key)
        stream_ready[stream_key] = bool(stream_status["book_ready"])
        snapshot_resets_by_market[market] = max(
            snapshot_resets_by_market[market],
            int(stream_status["snapshot_count"]),
        )
        if stream_status["snapshot_complete"]:
            initialized_streams[market].add(stream_key)
        if compact.duplicate:
            counts["duplicates"] += 1
            continue
        state = rolling[market]
        # Events before the first authenticated snapshot are retained in raw
        # provenance but cannot seed rolling features or tape economics. The
        # snapshot records themselves must update queue-age state so the first
        # post-F_LAST decision starts from the complete book image.
        initialized_before = stream_key in initialized_streams[market]
        if authenticated_snapshot or initialized_before:
            _update_rolling_state(state, event)
        else:
            counts["events_gated_before_initial_snapshot"] += 1

        all_markets_initialized = all(
            encountered_streams[value]
            and encountered_streams[value] <= initialized_streams[value]
            for value in cfg.selected_markets
        )
        all_streams_ready = all(
            stream_ready.get(value, False)
            for values in encountered_streams.values()
            for value in values
        )
        if (
            not all_markets_initialized
            or not all_streams_ready
            or authenticated_snapshot
        ):
            counts["events_gated_until_snapshot_f_last"] += 1
            continue

        if event.action == "T":
            assert event.price is not None
            tape[market].append(event)
        due = (
            state.last_snapshot_ns < 0
            or event.available_at_ns - state.last_snapshot_ns >= cfg.decision_cadence_ns
        )
        if not due:
            continue
        if not isinstance(compact, CompactEventResult):
            raise FoundryPilotError("compact event-engine contract drift")
        full = engine.materialize(compact)
        state.last_snapshot_ns = event.available_at_ns
        view = full.book
        if (
            view.bid_price is None
            or view.ask_price is None
            or view.ask_price <= view.bid_price
            or not math.isfinite(state.last_price)
        ):
            counts["snapshots_without_tradeable_bbo"] += 1
            continue
        feature_values = _causal_feature_values(
            market,
            event,
            full,
            state,
            latest,
            cfg=cfg,
        )
        feature_tuple = tuple(float(feature_values[name]) for name in _FEATURE_NAMES)
        feature_hash = stable_hash(
            {
                "market": market,
                "event_ns": event.ts_event_ns,
                "available_ns": event.available_at_ns,
                "names": _FEATURE_NAMES,
                "values": feature_tuple,
                "event_fingerprint": event.fingerprint,
                "state_hash": full.state_hash,
            }
        )
        item = FeatureSnapshot(
            market=market,
            contract=cfg.contracts[market],
            session_id=event.session_id,
            event_ns=event.ts_event_ns,
            available_ns=event.available_at_ns,
            decision_ns=event.available_at_ns,
            event_fingerprint=event.fingerprint,
            state_hash=full.state_hash,
            trigger_action=event.action,
            bid_price=float(view.bid_price),
            ask_price=float(view.ask_price),
            bid_size=int(view.bid_size),
            ask_size=int(view.ask_size),
            bid_depth=tuple(view.bid_depth),
            ask_depth=tuple(view.ask_depth),
            last_trade_price=float(state.last_price),
            last_trade_side=state.last_trade_side,
            last_trade_size=state.last_trade_size,
            names=_FEATURE_NAMES,
            values=feature_tuple,
            feature_hash=feature_hash,
        )
        snapshots.append(item)
        latest[market] = item
        state.previous_bid_depth = sum(size for _, size in view.bid_depth)
        state.previous_ask_depth = sum(size for _, size in view.ask_depth)
        state.previous_vwap = state.last_vwap
        state.last_vwap = float(full.session.vwap or state.last_price)

    if any(counts[market] == 0 for market in cfg.selected_markets):
        raise FoundryPilotError("one selected market has no reconstructed events")
    missing_initial = {
        market: sorted(encountered_streams[market] - initialized_streams[market])
        for market in cfg.selected_markets
        if not encountered_streams[market]
        or encountered_streams[market] - initialized_streams[market]
    }
    if missing_initial:
        raise FoundryPilotError(
            "authenticated initial snapshot never completed for every selected "
            f"market/instrument: {missing_initial}"
        )
    incomplete_final = {
        market: sorted(
            value
            for value in encountered_streams[market]
            if not stream_ready.get(value, False)
        )
        for market in cfg.selected_markets
        if any(
            not stream_ready.get(value, False)
            for value in encountered_streams[market]
        )
    }
    if incomplete_final:
        raise FoundryPilotError(
            f"RAW_DBN ended before snapshot F_LAST: {incomplete_final}"
        )
    if len(snapshots) < 100:
        raise FoundryPilotError("causal event sample is too small for the bounded pilot")
    if any(len(tape[market]) == 0 for market in cfg.selected_markets):
        raise FoundryPilotError("one selected market has no executable public trades")
    reconstruction = {
        "status": "BOOK_STATE_RECONSTRUCTION_GREEN",
        "engine_schema": "hydra_microstructure_event_engine_v1",
        "strict_source_sequence_monotonicity": True,
        "strict_contiguous_sequence": False,
        "sequence_gap_policy": "SOURCE_FILTERED_STREAM_MONOTONIC_AND_SNAPSHOT_RECOVERY",
        "event_count": int(sum(counts[m] for m in cfg.selected_markets)),
        "events_by_market": {m: int(counts[m]) for m in cfg.selected_markets},
        "action_counts": {key.removeprefix("action_"): int(value) for key, value in counts.items() if key.startswith("action_")},
        "duplicate_count": int(counts["duplicates"]),
        "events_gated_before_initial_snapshot": int(
            counts["events_gated_before_initial_snapshot"]
        ),
        "state_engine_event_count": int(counts["state_engine_events"]),
        "pre_snapshot_state_engine_bypass": True,
        "events_gated_until_snapshot_f_last": int(
            counts["events_gated_until_snapshot_f_last"]
        ),
        "initial_snapshot_complete_by_market": {
            market: len(initialized_streams[market])
            == len(encountered_streams[market])
            and bool(encountered_streams[market])
            for market in cfg.selected_markets
        },
        "snapshot_resets_by_market": {
            market: int(snapshot_resets_by_market[market])
            for market in cfg.selected_markets
        },
        "snapshot_f_last_gating": True,
        "maybe_bad_book_fail_closed": True,
        "snapshot_count": len(snapshots),
        "first_available_ns": first_available,
        "last_available_ns": last_available,
        "final_state_hash": engine.state_hash(),
        "checkpoint_hash": engine.checkpoint()["checkpoint_hash"],
        "batch_stream_single_step": True,
    }
    return tuple(snapshots), {key: value.freeze() for key, value in tape.items()}, reconstruction


def _update_rolling_state(state: _RollingMarketState, event: MarketEvent) -> None:
    now = event.available_at_ns
    state.actions_2s.add(now, event.action)
    state.actions_2s.evict(now)
    if event.action == "T":
        assert event.price is not None
        direction = 1 if event.side == "B" else -1 if event.side == "A" else 0
        for window in (state.trade_2s, state.trade_30s, state.trade_5m):
            window.add(now, direction, event.size, float(event.price))
            window.evict(now)
        state.previous_price = state.last_price
        state.last_price = float(event.price)
        state.last_trade_side = event.side
        state.last_trade_size = int(event.size)
    if event.action == "A" and event.order_id is not None:
        previous = state.order_birth_ns.get(event.order_id)
        if previous is not None:
            state.order_birth_sum_ns -= previous
            state.matured_order_ids.discard(event.order_id)
        state.order_birth_ns[event.order_id] = now
        state.order_size[event.order_id] = event.size
        state.order_birth_sum_ns += now
        heapq.heappush(state.maturity_heap, (now + 2_000_000_000, event.order_id, now))
        state.add_count += 1
    elif event.action == "M" and event.order_id is not None:
        previous = state.order_birth_ns.get(event.order_id)
        if previous is not None:
            state.order_birth_sum_ns -= previous
            state.matured_order_ids.discard(event.order_id)
        state.order_birth_ns[event.order_id] = now
        state.order_size[event.order_id] = event.size
        state.order_birth_sum_ns += now
        heapq.heappush(state.maturity_heap, (now + 2_000_000_000, event.order_id, now))
    elif event.action == "C" and event.order_id is not None:
        current_size = state.order_size.get(event.order_id, 0)
        if 0 < event.size < current_size:
            state.order_size[event.order_id] = current_size - event.size
        else:
            previous = state.order_birth_ns.pop(event.order_id, None)
            state.order_size.pop(event.order_id, None)
            if previous is not None:
                state.order_birth_sum_ns -= previous
            state.matured_order_ids.discard(event.order_id)
        state.cancel_count += 1
    elif event.action == "F":
        state.fill_count += 1
    elif event.action == "R":
        state.order_birth_ns.clear()
        state.order_size.clear()
        state.order_birth_sum_ns = 0
        state.maturity_heap.clear()
        state.matured_order_ids.clear()

    while state.maturity_heap and state.maturity_heap[0][0] <= now:
        _, order_id, birth = heapq.heappop(state.maturity_heap)
        if state.order_birth_ns.get(order_id) == birth:
            state.matured_order_ids.add(order_id)


def _causal_feature_values(
    market: str,
    event: MarketEvent,
    result: EventResult,
    state: _RollingMarketState,
    latest: Mapping[str, FeatureSnapshot],
    *,
    cfg: FoundryPilotConfig,
) -> dict[str, float]:
    now = event.available_at_ns
    tick = cfg.tick_size[market]
    book = result.book
    assert book.bid_price is not None and book.ask_price is not None
    assert book.microprice is not None
    mid = 0.5 * (book.bid_price + book.ask_price)

    for window in (state.trade_2s, state.trade_30s, state.trade_5m):
        window.evict(now)
    flow2 = state.trade_2s.signed_volume
    arrival2 = state.trade_2s.count / 2.0
    flow30 = state.trade_30s.signed_volume
    arrival30 = state.trade_30s.count / 30.0
    notional30 = state.trade_30s.notional / 30.0
    volatility30 = state.trade_30s.price_std
    flow5m = state.trade_5m.signed_volume
    total_bbo = max(1, book.bid_size + book.ask_size)
    bbo_imbalance = (book.bid_size - book.ask_size) / total_bbo

    def imbalance(levels: int) -> float:
        bids = sum(size for _, size in book.bid_depth[:levels])
        asks = sum(size for _, size in book.ask_depth[:levels])
        return (bids - asks) / max(1, bids + asks)

    bid_total = sum(size for _, size in book.bid_depth)
    ask_total = sum(size for _, size in book.ask_depth)
    previous_total = state.previous_bid_depth + state.previous_ask_depth
    current_total = bid_total + ask_total
    depletion = max(0.0, previous_total - current_total) / max(1, previous_total)
    replenishment = max(0.0, current_total - previous_total) / max(1, previous_total)
    withdrawal_events = state.actions_2s.cancels
    add_events = state.actions_2s.adds
    active_order_count = len(state.order_birth_ns)
    queue_persistence = len(state.matured_order_ids) / active_order_count if active_order_count else 0.0
    order_age_mean = (
        (now * active_order_count - state.order_birth_sum_ns)
        / active_order_count
        / 1_000_000_000
        if active_order_count
        else 0.0
    )
    spread_ticks = (book.ask_price - book.bid_price) / tick
    bid_gaps = [abs(book.bid_depth[i][0] - book.bid_depth[i + 1][0]) / tick for i in range(len(book.bid_depth) - 1)]
    ask_gaps = [abs(book.ask_depth[i + 1][0] - book.ask_depth[i][0]) / tick for i in range(len(book.ask_depth) - 1)]
    liquidity_gap = max([spread_ticks, *bid_gaps, *ask_gaps])
    depth_vector = np.asarray(
        [size for _, size in book.bid_depth[:5]] + [size for _, size in book.ask_depth[:5]],
        dtype=float,
    )
    depth_slope = float(np.polyfit(np.arange(len(depth_vector)), depth_vector, 1)[0]) if len(depth_vector) >= 2 else 0.0
    depth_convexity = float(np.std(np.diff(depth_vector))) if len(depth_vector) >= 3 else 0.0
    price_change = 0.0 if not math.isfinite(state.previous_price) else state.last_price - state.previous_price
    response = price_change / flow2 if abs(flow2) > 1e-12 else 0.0
    vwap = float(result.session.vwap or state.last_price)
    vwap_slope = 0.0 if not math.isfinite(state.previous_vwap) else (vwap - state.previous_vwap) / tick
    other_market = next((name for name in cfg.selected_markets if name != market), None)
    other = latest.get(str(other_market))
    other_values = dict(zip(other.names, other.values, strict=True)) if other else {}
    other_flow = float(other_values.get("flow_30s", 0.0))
    flow_alignment = math.copysign(1.0, flow30) * math.copysign(1.0, other_flow) if flow30 and other_flow else 0.0
    other_micro = float(other_values.get("microprice_deviation", 0.0))
    micro_dev = (book.microprice - mid) / tick
    hour = datetime.fromtimestamp(event.ts_event_ns / 1_000_000_000, tz=UTC).hour
    minute = datetime.fromtimestamp(event.ts_event_ns / 1_000_000_000, tz=UTC).minute
    seconds_since_utc_midnight = hour * 3600 + minute * 60
    session_fraction = seconds_since_utc_midnight / 86_400.0
    opening_drive = (state.last_price - vwap) / tick if result.session.trade_count <= 1_000 else 0.0
    return {
        "aggressor_delta": flow30,
        "signed_volume": flow30,
        "trade_arrival_rate": arrival30,
        "notional_rate": notional30,
        "bbo_imbalance": bbo_imbalance,
        "microprice_deviation": micro_dev,
        "spread_ticks": spread_ticks,
        "vwap_distance": (state.last_price - vwap) / tick,
        "vwap_slope": vwap_slope,
        "event_volatility": volatility30 / tick,
        "cross_market_flow_alignment": flow_alignment,
        "cross_market_microprice_divergence": micro_dev - other_micro,
        "depth_3_imbalance": imbalance(3),
        "depth_5_imbalance": imbalance(5),
        "depth_10_imbalance": imbalance(10),
        "depth_slope": depth_slope,
        "depth_convexity": depth_convexity,
        "depletion_rate": depletion,
        "replenishment_rate": replenishment,
        "liquidity_gap_ticks": liquidity_gap,
        "price_response_per_signed_contract": response / tick,
        "depth_withdrawal_rate": withdrawal_events / max(1, withdrawal_events + add_events),
        "queue_persistence": queue_persistence,
        "quantity_ahead": float(min(book.bid_size, book.ask_size)),
        "order_age_mean": order_age_mean,
        "cancel_ahead_rate": float(withdrawal_events) / 2.0,
        "cancel_behind_rate": float(state.cancel_count) / max(1, result.session.event_count),
        "ephemeral_liquidity_rate": float(min(state.add_count, state.cancel_count)) / max(1, state.add_count),
        "flow_2s": flow2,
        "flow_30s": flow30,
        "flow_5m": flow5m,
        "arrival_2s": arrival2,
        "arrival_30s": arrival30,
        "session_elapsed_fraction": session_fraction,
        "opening_drive": opening_drive,
    }


def _freeze_roles(
    snapshots: Sequence[FeatureSnapshot], *, cfg: FoundryPilotConfig
) -> tuple[tuple[str, ...], Mapping[str, str]]:
    sessions_by_market = {
        market: {value.session_id for value in snapshots if value.market == market}
        for market in cfg.selected_markets
    }
    complete = sorted(set.intersection(*(sessions_by_market[m] for m in cfg.selected_markets)))
    required = sum(cfg.chronological_roles)
    if len(complete) < required:
        raise FoundryPilotError(
            f"pilot needs {required} complete common sessions, found {len(complete)}"
        )
    selected = tuple(complete[:required])
    d, v, _ = cfg.chronological_roles
    roles = {
        session: (
            ROLE_DISCOVERY if offset < d else ROLE_VALIDATION if offset < d + v else ROLE_FINAL
        )
        for offset, session in enumerate(selected)
    }
    return selected, roles


def _snapshots_by_market(
    snapshots: Sequence[FeatureSnapshot],
) -> Mapping[str, tuple[FeatureSnapshot, ...]]:
    values: dict[str, list[FeatureSnapshot]] = defaultdict(list)
    for item in snapshots:
        values[item.market].append(item)
    return {
        market: tuple(sorted(rows, key=lambda value: value.decision_ns))
        for market, rows in values.items()
    }


def _filter_tape_sessions(
    tape: _CompactTape, roles: Mapping[str, str]
) -> _CompactTape:
    if tape.sessions and all(session in roles for session in tape.sessions):
        return tape
    selected_codes = [
        offset for offset, session in enumerate(tape.sessions) if session in roles
    ]
    mask = np.isin(tape.session_code, np.asarray(selected_codes, dtype=np.uint16))
    sessions = tuple(session for session in tape.sessions if session in roles)
    lookup = np.zeros(max(1, len(tape.sessions)), dtype=np.uint16)
    for new, old in enumerate(selected_codes):
        lookup[old] = new
    codes = lookup[tape.session_code[mask]]
    return _CompactTape(
        available_ns=tape.available_ns[mask],
        price=tape.price[mask],
        size=tape.size[mask],
        side=tape.side[mask],
        session_code=codes,
        sessions=sessions,
    )


def _build_outcomes(
    snapshots: Sequence[FeatureSnapshot], *, cfg: FoundryPilotConfig
) -> tuple[OutcomeRow, ...]:
    rows: list[OutcomeRow] = []
    by_market = _snapshots_by_market(snapshots)
    for market, values in by_market.items():
        times = np.asarray([value.decision_ns for value in values], dtype=np.int64)
        prices = np.asarray([value.last_trade_price for value in values], dtype=float)
        sessions = np.asarray([value.session_id for value in values], dtype=object)
        session_ends = np.empty(len(values), dtype=np.int64)
        group_start = 0
        while group_start < len(values):
            group_end = group_start + 1
            while group_end < len(values) and sessions[group_end] == sessions[group_start]:
                group_end += 1
            session_ends[group_start:group_end] = group_end
            group_start = group_end
        tick = cfg.tick_size[market]
        for offset, value in enumerate(values):
            feature = dict(zip(value.names, value.values, strict=True))
            raw_direction = feature["aggressor_delta"] + feature["microprice_deviation"]
            direction = 1 if raw_direction >= 0 else -1
            stop = value.last_trade_price - direction * cfg.adverse_ticks * tick
            target = value.last_trade_price + direction * cfg.favorable_ticks * tick
            end = int(np.searchsorted(times, value.decision_ns + cfg.outcome_horizon_ns, side="right"))
            session_end = int(session_ends[offset])
            end = min(end, session_end)
            future = prices[offset + 1 : end]
            future_times = times[offset + 1 : end]
            has_coverage = bool(
                session_end > offset + 1
                and times[session_end - 1]
                >= value.decision_ns + cfg.outcome_horizon_ns
            )
            if not has_coverage or len(future) == 0:
                rows.append(
                    OutcomeRow(
                        feature_hash=value.feature_hash,
                        market=market,
                        decision_ns=value.decision_ns,
                        direction=direction,
                        status="CENSORED_FUTURE_COVERAGE",
                        favorable_first=False,
                        adverse_first=False,
                        time_to_favorable_ns=None,
                        time_to_adverse_ns=None,
                        mfe_ticks=None,
                        mae_ticks=None,
                        future_markout_ticks=None,
                    )
                )
                continue
            favorable_mask = future >= target if direction > 0 else future <= target
            adverse_mask = future <= stop if direction > 0 else future >= stop
            favorable_offsets = np.flatnonzero(favorable_mask)
            adverse_offsets = np.flatnonzero(adverse_mask)
            favorable_index = int(favorable_offsets[0]) if len(favorable_offsets) else None
            adverse_index = int(adverse_offsets[0]) if len(adverse_offsets) else None
            favorable_first = favorable_index is not None and (
                adverse_index is None or favorable_index < adverse_index
            )
            adverse_first = adverse_index is not None and (
                favorable_index is None or adverse_index < favorable_index
            )
            signed_moves = direction * (future - value.last_trade_price) / tick
            rows.append(
                OutcomeRow(
                    feature_hash=value.feature_hash,
                    market=market,
                    decision_ns=value.decision_ns,
                    direction=direction,
                    status=(
                        "FAVORABLE_FIRST"
                        if favorable_first
                        else "ADVERSE_FIRST"
                        if adverse_first
                        else "TIMEOUT"
                    ),
                    favorable_first=favorable_first,
                    adverse_first=adverse_first,
                    time_to_favorable_ns=(
                        None
                        if favorable_index is None
                        else int(future_times[favorable_index] - value.decision_ns)
                    ),
                    time_to_adverse_ns=(
                        None
                        if adverse_index is None
                        else int(future_times[adverse_index] - value.decision_ns)
                    ),
                    mfe_ticks=float(np.max(signed_moves)),
                    mae_ticks=float(np.min(signed_moves)),
                    future_markout_ticks=float(
                        (future[-1] - value.last_trade_price) / tick
                    ),
                )
            )
    return tuple(sorted(rows, key=lambda value: (value.decision_ns, value.market)))


def _teacher_feature_table(
    snapshots: Sequence[FeatureSnapshot],
    outcomes: Sequence[OutcomeRow],
    roles: Mapping[str, str],
) -> FeatureTable:
    outcome_by_hash = {value.feature_hash: value for value in outcomes}
    ordered = tuple(sorted(snapshots, key=lambda value: (value.decision_ns, value.market)))
    if not ordered:
        raise FoundryPilotError("feature lattice is empty")
    values = np.asarray([value.values for value in ordered], dtype=float)
    future_markout = np.asarray(
        [
            math.nan
            if outcome_by_hash[value.feature_hash].future_markout_ticks is None
            else outcome_by_hash[value.feature_hash].future_markout_ticks
            for value in ordered
        ],
        dtype=float,
    )
    favorable = np.asarray(
        [outcome_by_hash[value.feature_hash].favorable_first for value in ordered],
        dtype=bool,
    )
    return FeatureTable(
        names=_FEATURE_NAMES,
        values=values,
        decision_ns=np.asarray([value.decision_ns for value in ordered], dtype=np.int64),
        available_ns=np.asarray([value.available_ns for value in ordered], dtype=np.int64),
        roles=np.asarray([roles[value.session_id] for value in ordered], dtype=object),
        market=np.asarray([value.market for value in ordered], dtype=object),
        future_markout=future_markout,
        favorable_before_adverse=favorable,
    )


_FAMILY_TEACHER = {
    "ABSORPTION_REVERSAL": "ABSORPTION",
    "INITIATIVE_CONTINUATION": "DEPLETION",
    "LIQUIDITY_VACUUM_CONTINUATION": "LIQUIDITY_VACUUM",
    "EXHAUSTION_REVERSAL": "EXHAUSTION",
    "VWAP_ACCEPTANCE_REJECTION": "ABSORPTION",
    "OPENING_DRIVE": "DEPLETION",
    "CROSS_ASSET_FLOW_DIVERGENCE": "EXHAUSTION",
    "QUEUE_REPLENISHMENT": "QUEUE_STATE",
}

_FAMILY_FEATURES = {
    "ABSORPTION_REVERSAL": ("aggressor_delta", "price_response_per_signed_contract", "replenishment_rate", "queue_persistence"),
    "INITIATIVE_CONTINUATION": ("flow_2s", "microprice_deviation", "depletion_rate", "trade_arrival_rate"),
    "LIQUIDITY_VACUUM_CONTINUATION": ("depth_withdrawal_rate", "liquidity_gap_ticks", "microprice_deviation", "flow_2s"),
    "EXHAUSTION_REVERSAL": ("aggressor_delta", "trade_arrival_rate", "price_response_per_signed_contract", "vwap_distance"),
    "VWAP_ACCEPTANCE_REJECTION": ("vwap_distance", "vwap_slope", "flow_30s", "microprice_deviation"),
    "OPENING_DRIVE": ("opening_drive", "arrival_2s", "bbo_imbalance", "microprice_deviation"),
    "CROSS_ASSET_FLOW_DIVERGENCE": ("cross_market_flow_alignment", "cross_market_microprice_divergence", "flow_30s", "event_volatility"),
    "QUEUE_REPLENISHMENT": ("queue_persistence", "replenishment_rate", "quantity_ahead", "order_age_mean"),
}


def _freeze_sleeves(
    snapshots: Sequence[FeatureSnapshot],
    table: FeatureTable,
    teachers: TeacherLabelSet,
    students: Sequence[StudentResult],
    roles: Mapping[str, str],
    *,
    cfg: FoundryPilotConfig,
) -> tuple[tuple[SleeveSpec, ...], Mapping[str, tuple[np.ndarray, np.ndarray]]]:
    ordered = tuple(sorted(snapshots, key=lambda value: (value.decision_ns, value.market)))
    if len(ordered) != len(table.decision_ns):
        raise FoundryPilotError("teacher table and feature lattice diverged")
    values = np.asarray(table.values, dtype=float)
    name_index = {name: offset for offset, name in enumerate(_FEATURE_NAMES)}
    students_by_key = {
        (value.student.teacher_family, value.student.tier): value.student
        for value in students
    }
    sleeves: list[SleeveSpec] = []
    scores: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for family_index, family in enumerate(EXPERT_FAMILIES):
        mbo_raw = _symbolic_family_score(family, values, name_index)
        directions = _family_directions(family, values, name_index)
        for variant_index, (tier, path) in enumerate(
            (("L1_DEPLOYABLE", "AGGRESSIVE"), ("L2_DEPLOYABLE", "AGGRESSIVE"), ("MBO_TEACHER_ONLY", "PASSIVE"))
        ):
            market = cfg.selected_markets[(family_index + variant_index) % 2]
            teacher_family = _FAMILY_TEACHER[family]
            student_tier = "L1" if tier == "L1_DEPLOYABLE" else "L2"
            student = students_by_key.get((teacher_family, student_tier)) if tier != "MBO_TEACHER_ONLY" else None
            if student is not None:
                score_vector = score_frozen_student(
                    student, feature_names=_FEATURE_NAMES, values=values
                )
                model_hash = student.model_hash
                feature_names = student.feature_names
                threshold = student.threshold
            else:
                score_vector = (
                    mbo_raw
                    if tier == "MBO_TEACHER_ONLY"
                    else _deployable_symbolic_score(
                        family,
                        "L1" if tier == "L1_DEPLOYABLE" else "L2",
                        values,
                        name_index,
                    )
                )
                discovery_mask = np.asarray(
                    [roles[item.session_id] == ROLE_DISCOVERY and item.market == market for item in ordered],
                    dtype=bool,
                )
                finite_discovery = score_vector[discovery_mask & np.isfinite(score_vector)]
                if len(finite_discovery) < 10:
                    raise FoundryPilotError(f"discovery score support absent: {family}/{market}")
                threshold = float(np.quantile(finite_discovery, 0.85 + 0.05 * variant_index))
                model_hash = stable_hash(
                    {
                        "family": family,
                        "variant": variant_index,
                        "threshold": threshold,
                        "causal_feature_contract": "available_at<=decision_time",
                    }
                )
                feature_names = (
                    _FAMILY_FEATURES[family]
                    if tier == "MBO_TEACHER_ONLY"
                    else tuple(
                        name
                        for name in (L1_FEATURES if tier == "L1_DEPLOYABLE" else L2_FEATURES)
                        if name in _FEATURE_NAMES
                    )
                )
            stop_ticks = (4.0, 6.0, 5.0)[variant_index]
            target_ticks = (8.0, 12.0, 10.0)[variant_index]
            hold = (30, 60, 45)[variant_index]
            sleeve_id = f"micro_0031_{family.lower()}_{variant_index + 1:02d}_{market.lower()}"
            sleeve = SleeveSpec(
                sleeve_id=sleeve_id,
                family=family,
                variant=("L1_AGGRESSIVE", "L2_AGGRESSIVE", "MBO_PASSIVE")[variant_index],
                market=market,
                deployability_tier=tier,
                execution_path=path,
                direction_mode=("REVERSAL" if family in {"ABSORPTION_REVERSAL", "EXHAUSTION_REVERSAL", "VWAP_ACCEPTANCE_REJECTION", "CROSS_ASSET_FLOW_DIVERGENCE", "QUEUE_REPLENISHMENT"} else "CONTINUATION"),
                feature_names=tuple(feature_names),
                score_threshold=float(threshold),
                quantity=cfg.quantity,
                stop_ticks=stop_ticks,
                target_ticks=target_ticks,
                maximum_holding_seconds=hold,
                entry_rule="score>=frozen_discovery_threshold; decision at available event snapshot",
                exit_rule="first target/stop or frozen maximum duration; aggressive executable depth exit",
                session_rule="flat by session boundary; no carry",
                contract_rule=f"explicit {cfg.contracts[market]}; no roll inside bounded pilot",
                feature_hash=stable_hash({"names": list(feature_names), "contract": "available_at<=decision_time"}),
                model_hash=model_hash,
            )
            sleeves.append(sleeve)
            market_mask = np.asarray([item.market == market for item in ordered], dtype=bool)
            scores[sleeve_id] = (
                np.asarray(score_vector[market_mask], dtype=np.float32),
                np.asarray(directions[market_mask], dtype=np.int8),
            )
    return tuple(sleeves), scores


def _symbolic_family_score(
    family: str, values: np.ndarray, index: Mapping[str, int]
) -> np.ndarray:
    def col(name: str) -> np.ndarray:
        return np.nan_to_num(values[:, index[name]], nan=0.0, posinf=0.0, neginf=0.0)
    flow = np.abs(col("flow_30s"))
    response = np.abs(col("price_response_per_signed_contract"))
    if family == "ABSORPTION_REVERSAL":
        raw = flow * col("replenishment_rate") * (1.0 + col("queue_persistence")) / (1.0 + response)
    elif family == "INITIATIVE_CONTINUATION":
        raw = np.abs(col("flow_2s")) * np.abs(col("microprice_deviation")) * (1.0 + col("depletion_rate"))
    elif family == "LIQUIDITY_VACUUM_CONTINUATION":
        raw = (1.0 + col("liquidity_gap_ticks")) * col("depth_withdrawal_rate") * np.abs(col("microprice_deviation"))
    elif family == "EXHAUSTION_REVERSAL":
        raw = flow * col("trade_arrival_rate") / (1.0 + response)
    elif family == "VWAP_ACCEPTANCE_REJECTION":
        raw = np.abs(col("vwap_distance")) * (1.0 + np.abs(col("vwap_slope"))) * (1.0 + np.abs(col("microprice_deviation")))
    elif family == "OPENING_DRIVE":
        raw = np.abs(col("opening_drive")) * (1.0 + col("arrival_2s")) * (1.0 + np.abs(col("bbo_imbalance")))
    elif family == "CROSS_ASSET_FLOW_DIVERGENCE":
        raw = np.abs(col("cross_market_microprice_divergence")) * (1.0 + (col("cross_market_flow_alignment") <= 0)) * (1.0 + flow)
    elif family == "QUEUE_REPLENISHMENT":
        raw = col("queue_persistence") * (1.0 + col("replenishment_rate")) * (1.0 + col("order_age_mean")) / (1.0 + col("quantity_ahead"))
    else:  # pragma: no cover - frozen family inventory guard
        raise FoundryPilotError(f"unknown expert family: {family}")
    return np.asarray(raw, dtype=float)


def _deployable_symbolic_score(
    family: str,
    tier: str,
    values: np.ndarray,
    index: Mapping[str, int],
) -> np.ndarray:
    """Fallback rules that strictly respect the exported L1/L2 feature tier."""

    def col(name: str) -> np.ndarray:
        return np.nan_to_num(values[:, index[name]], nan=0.0, posinf=0.0, neginf=0.0)

    flow = np.abs(col("flow_30s"))
    micro = np.abs(col("microprice_deviation"))
    arrival = col("trade_arrival_rate")
    spread = col("spread_ticks")
    if family == "ABSORPTION_REVERSAL":
        raw = flow * (1.0 + col("bbo_imbalance") ** 2) / (1.0 + micro)
    elif family == "INITIATIVE_CONTINUATION":
        raw = np.abs(col("flow_2s")) * micro * (1.0 + arrival)
    elif family == "LIQUIDITY_VACUUM_CONTINUATION":
        raw = spread * micro * (1.0 + col("event_volatility"))
    elif family == "EXHAUSTION_REVERSAL":
        raw = flow * arrival / (1.0 + micro)
    elif family == "VWAP_ACCEPTANCE_REJECTION":
        raw = np.abs(col("vwap_distance")) * (1.0 + np.abs(col("vwap_slope")))
    elif family == "OPENING_DRIVE":
        raw = np.abs(col("opening_drive")) * (1.0 + col("arrival_2s"))
    elif family == "CROSS_ASSET_FLOW_DIVERGENCE":
        raw = np.abs(col("cross_market_microprice_divergence")) * (
            1.0 + (col("cross_market_flow_alignment") <= 0)
        )
    elif family == "QUEUE_REPLENISHMENT":
        raw = np.abs(col("bbo_imbalance")) * (1.0 + arrival) / (1.0 + spread)
    else:  # pragma: no cover
        raise FoundryPilotError(f"unknown deployable family: {family}")
    if tier == "L2":
        raw = raw * (
            1.0
            + np.abs(col("depth_5_imbalance"))
            + col("depletion_rate")
            + col("replenishment_rate")
        )
    elif tier != "L1":
        raise FoundryPilotError("deployable symbolic tier drift")
    return np.asarray(raw, dtype=float)


def _family_directions(
    family: str, values: np.ndarray, index: Mapping[str, int]
) -> np.ndarray:
    def signed(name: str) -> np.ndarray:
        raw = np.nan_to_num(values[:, index[name]], nan=0.0)
        return np.where(raw >= 0.0, 1, -1)
    if family in {"ABSORPTION_REVERSAL", "EXHAUSTION_REVERSAL"}:
        return -signed("flow_30s")
    if family == "VWAP_ACCEPTANCE_REJECTION":
        return -signed("vwap_distance")
    if family == "CROSS_ASSET_FLOW_DIVERGENCE":
        return -signed("cross_market_microprice_divergence")
    if family == "QUEUE_REPLENISHMENT":
        return signed("bbo_imbalance")
    if family == "OPENING_DRIVE":
        return signed("opening_drive")
    return signed("microprice_deviation")


def _evaluate_candidate(
    sleeve: SleeveSpec,
    scores: tuple[np.ndarray, np.ndarray],
    snapshots: Sequence[FeatureSnapshot],
    tape_events: _CompactTape,
    sessions: Sequence[str],
    roles: Mapping[str, str],
    *,
    cfg: FoundryPilotConfig,
) -> CandidateResult:
    score_values, directions = scores
    if len(score_values) != len(snapshots) or len(directions) != len(snapshots):
        raise FoundryPilotError("candidate score vector and market lattice diverged")
    intents: list[SignalIntent] = []
    cooldown_until = -1
    for feature_index, (item, score_raw, direction_raw) in enumerate(zip(
        snapshots, score_values, directions, strict=True
    )):
        score = float(score_raw)
        direction = int(direction_raw)
        if (
            not math.isfinite(score)
            or score < sleeve.score_threshold
            or direction not in {-1, 1}
            or item.decision_ns < cooldown_until
        ):
            continue
        latency = cfg.aggressive_latency_ns if sleeve.execution_path == "AGGRESSIVE" else cfg.passive_latency_ns
        signal_id = stable_hash(
            {
                "sleeve_id": sleeve.sleeve_id,
                "feature_hash": item.feature_hash,
                "direction": direction,
                "decision_ns": item.decision_ns,
            }
        )
        intents.append(
            SignalIntent(
                sleeve_id=sleeve.sleeve_id,
                signal_id=signal_id,
                feature_hash=item.feature_hash,
                feature_index=feature_index,
                market=sleeve.market,
                session_id=item.session_id,
                direction=direction,
                score=score,
                signal_time_ns=item.event_ns,
                decision_time_ns=item.decision_ns,
                order_submit_time_ns=item.decision_ns,
                earliest_executable_time_ns=item.decision_ns + latency,
                execution_path=sleeve.execution_path,
            )
        )
        cooldown_until = item.decision_ns + 5_000_000_000
    trades = _execute_intents(
        sleeve,
        tuple(intents),
        snapshots,
        tape_events,
        roles,
        cfg=cfg,
    )
    episodes = _account_episodes(
        sleeve.sleeve_id,
        trades,
        sessions,
        cfg=cfg,
    )
    economics = _candidate_economics(
        sleeve,
        tuple(intents),
        trades,
        episodes,
        roles,
        session_count=len(sessions),
    )
    controls = _matched_controls(
        sleeve,
        tuple(intents),
        snapshots,
        tape_events,
        roles,
        cfg=cfg,
    )
    final = economics["by_role"][ROLE_FINAL]
    serious = bool(
        sleeve.deployability_tier in {"L1_DEPLOYABLE", "L2_DEPLOYABLE"}
        and int(final["trade_count"]) >= cfg.minimum_final_opportunities
        and float(final["stressed_net_usd"]) > 0.0
        and float(economics["mll_breach_rate"]) <= cfg.maximum_mll_breach_rate
    )
    return CandidateResult(
        sleeve=sleeve,
        signals=tuple(intents),
        trades=trades,
        economics=economics,
        episodes=episodes,
        controls=controls,
        serious=serious,
    )


def _execute_intents(
    sleeve: SleeveSpec,
    intents: Sequence[SignalIntent],
    snapshots: Sequence[FeatureSnapshot],
    tape_events: _CompactTape,
    roles: Mapping[str, str],
    *,
    cfg: FoundryPilotConfig,
) -> tuple[ExecutedTrade, ...]:
    times = np.asarray([value.decision_ns for value in snapshots], dtype=np.int64)
    tape_times = tape_events.available_ns
    tick = cfg.tick_size[sleeve.market]
    point_value = cfg.point_value[sleeve.market]
    trades: list[ExecutedTrade] = []
    occupied_until = -1
    for intent in intents:
        if intent.decision_time_ns < occupied_until:
            continue
        placement_index = int(np.searchsorted(times, intent.earliest_executable_time_ns, side="left"))
        if placement_index >= len(snapshots):
            continue
        placement = snapshots[placement_index]
        if placement.session_id != intent.session_id:
            continue
        if sleeve.execution_path == "AGGRESSIVE":
            depth = placement.ask_depth if intent.direction > 0 else placement.bid_depth
            filled, entry = _depth_fill(depth, sleeve.quantity)
            quantity_ahead = 0
            if filled <= 0 or entry is None:
                continue
            entry += intent.direction * cfg.adverse_slippage_ticks_per_side * tick
            entry_time = placement.decision_ns
        else:
            limit = placement.bid_price if intent.direction > 0 else placement.ask_price
            quantity_ahead = placement.bid_size if intent.direction > 0 else placement.ask_size
            start = int(np.searchsorted(tape_times, intent.earliest_executable_time_ns, side="left"))
            passive_deadline = min(
                intent.earliest_executable_time_ns + 10_000_000_000,
                placement.decision_ns + sleeve.maximum_holding_seconds * 1_000_000_000,
            )
            filled, entry_time = _passive_queue_fill(
                tape_events,
                start_index=start,
                deadline_ns=passive_deadline,
                session_id=intent.session_id,
                direction=intent.direction,
                limit_price=limit,
                quantity_ahead=quantity_ahead,
                requested_quantity=sleeve.quantity,
            )
            if filled <= 0:
                # A price touch without observed queue consumption is a non-fill.
                continue
            entry = float(limit)

        stop = entry - intent.direction * sleeve.stop_ticks * tick
        target = entry + intent.direction * sleeve.target_ticks * tick
        start_index = int(np.searchsorted(times, entry_time, side="left"))
        maximum_exit_ns = entry_time + sleeve.maximum_holding_seconds * 1_000_000_000
        exit_index = start_index
        exit_reason = "MAXIMUM_HOLD"
        for index in range(start_index, len(snapshots)):
            item = snapshots[index]
            if item.session_id != intent.session_id:
                exit_index = max(start_index, index - 1)
                exit_reason = "SESSION_FLATTEN"
                break
            exit_index = index
            if intent.direction * (item.last_trade_price - target) >= 0:
                exit_reason = "TARGET"
                break
            if intent.direction * (item.last_trade_price - stop) <= 0:
                exit_reason = "STOP"
                break
            if item.decision_ns >= maximum_exit_ns:
                exit_reason = "MAXIMUM_HOLD"
                break
        exit_snapshot = snapshots[exit_index]
        exit_depth = exit_snapshot.bid_depth if intent.direction > 0 else exit_snapshot.ask_depth
        exit_filled, exit_price = _depth_fill(exit_depth, filled)
        if exit_filled <= 0 or exit_price is None:
            continue
        effective_quantity = min(filled, exit_filled)
        exit_price -= intent.direction * cfg.adverse_slippage_ticks_per_side * tick
        path_prices = np.asarray(
            [value.last_trade_price for value in snapshots[start_index : exit_index + 1]],
            dtype=float,
        )
        signed_path = intent.direction * (path_prices - entry) * point_value * effective_quantity
        minimum_unrealized = float(np.min(signed_path)) if len(signed_path) else 0.0
        gross = intent.direction * (exit_price - entry) * point_value * effective_quantity
        commission = cfg.normal_round_turn_commission_usd[sleeve.market] * effective_quantity
        base_slippage_cost = (
            2.0
            * cfg.adverse_slippage_ticks_per_side
            * tick
            * point_value
            * effective_quantity
        )
        additional_stressed_slippage = (
            base_slippage_cost * (cfg.stressed_cost_multiplier - 1.0)
        )
        normal_cost = commission
        stressed_cost = commission + additional_stressed_slippage
        trade_id = stable_hash(
            {
                "signal_id": intent.signal_id,
                "entry_time_ns": entry_time,
                "exit_time_ns": exit_snapshot.decision_ns,
                "entry_price": entry,
                "exit_price": exit_price,
                "quantity": effective_quantity,
            }
        )
        trades.append(
            ExecutedTrade(
                sleeve_id=sleeve.sleeve_id,
                trade_id=trade_id,
                signal_id=intent.signal_id,
                market=sleeve.market,
                session_id=intent.session_id,
                role=roles[intent.session_id],
                execution_path=sleeve.execution_path,
                direction=intent.direction,
                requested_quantity=sleeve.quantity,
                filled_quantity=effective_quantity,
                quantity_ahead=int(quantity_ahead),
                entry_time_ns=int(entry_time),
                exit_time_ns=int(exit_snapshot.decision_ns),
                entry_price=float(entry),
                exit_price=float(exit_price),
                stop_price=float(stop),
                target_price=float(target),
                exit_reason=exit_reason,
                gross_pnl_usd=float(gross),
                base_slippage_cost_usd=float(base_slippage_cost),
                normal_total_cost_usd=float(base_slippage_cost + commission),
                stressed_total_cost_usd=float(
                    base_slippage_cost * cfg.stressed_cost_multiplier + commission
                ),
                normal_costs_usd=float(normal_cost),
                normal_net_pnl_usd=float(gross - normal_cost),
                stressed_costs_usd=float(stressed_cost),
                stressed_net_pnl_usd=float(gross - stressed_cost),
                minimum_unrealized_pnl_usd=minimum_unrealized,
            )
        )
        occupied_until = exit_snapshot.decision_ns
    return tuple(trades)


def _depth_fill(
    depth: Sequence[tuple[float, int]], quantity: int
) -> tuple[int, float | None]:
    remaining = int(quantity)
    filled = 0
    notional = 0.0
    for price, size in depth:
        take = min(remaining, int(size))
        if take <= 0:
            continue
        filled += take
        remaining -= take
        notional += take * float(price)
        if remaining == 0:
            break
    return filled, (notional / filled if filled else None)


def _passive_queue_fill(
    tape: _CompactTape,
    *,
    start_index: int,
    deadline_ns: int,
    session_id: str,
    direction: int,
    limit_price: float,
    quantity_ahead: int,
    requested_quantity: int,
) -> tuple[int, int]:
    """Conservative queue fill: trade-through volume, never price touch alone."""

    consumed = 0
    for index in range(start_index, len(tape)):
        timestamp = int(tape.available_ns[index])
        if timestamp > deadline_ns or tape.session_at(index) != session_id:
            break
        price = float(tape.price[index])
        contra = int(tape.side[index]) == (-1 if direction > 0 else 1)
        crossed = price <= limit_price if direction > 0 else price >= limit_price
        if not (contra and crossed):
            continue
        consumed += int(tape.size[index])
        executable = max(0, consumed - int(quantity_ahead))
        filled = min(int(requested_quantity), executable)
        if filled > 0:
            return filled, timestamp
    return 0, -1


def _account_episodes(
    sleeve_id: str,
    trades: Sequence[ExecutedTrade],
    sessions: Sequence[str],
    *,
    cfg: FoundryPilotConfig,
) -> tuple[Mapping[str, Any], ...]:
    by_session: dict[str, list[ExecutedTrade]] = defaultdict(list)
    for trade in trades:
        by_session[trade.session_id].append(trade)
    rows: list[Mapping[str, Any]] = []
    for start_offset, start_session in enumerate(sessions):
        for horizon in HORIZONS_DAYS:
            full = start_offset + horizon <= len(sessions)
            included = tuple(sessions[start_offset : min(len(sessions), start_offset + horizon)])
            for scenario in COST_SCENARIOS:
                key = "normal_net_pnl_usd" if scenario == "NORMAL" else "stressed_net_pnl_usd"
                daily = []
                cumulative = 0.0
                trailing_high_eod = 0.0
                minimum_buffer = cfg.combine_mll_usd
                breached = False
                passed = False
                pass_day: int | None = None
                for day_number, session in enumerate(included, start=1):
                    day_trades = sorted(by_session.get(session, ()), key=lambda value: value.exit_time_ns)
                    day_pnl = 0.0
                    day_costs = 0.0
                    for trade in day_trades:
                        value = float(getattr(trade, key))
                        day_costs += float(
                            trade.normal_costs_usd
                            if scenario == "NORMAL"
                            else trade.stressed_costs_usd
                        )
                        equity_before = cumulative + day_pnl
                        minimum_equity = equity_before + min(0.0, trade.minimum_unrealized_pnl_usd)
                        loss_limit = max(-cfg.combine_mll_usd, trailing_high_eod - cfg.combine_mll_usd)
                        minimum_buffer = min(minimum_buffer, minimum_equity - loss_limit)
                        if minimum_equity < loss_limit:
                            breached = True
                        day_pnl += value
                    cumulative += day_pnl
                    trailing_high_eod = max(trailing_high_eod, cumulative)
                    closing_loss_limit = max(
                        -cfg.combine_mll_usd,
                        trailing_high_eod - cfg.combine_mll_usd,
                    )
                    minimum_buffer = min(
                        minimum_buffer, cumulative - closing_loss_limit
                    )
                    daily.append({"session_id": session, "day": day_number, "net_pnl_usd": day_pnl, "cumulative_net_usd": cumulative, "costs_usd": day_costs})
                    positive_days = [max(0.0, value["net_pnl_usd"]) for value in daily]
                    best_day = max(positive_days, default=0.0)
                    consistency_ok = cumulative > 0 and best_day <= cfg.consistency_limit * cumulative + 1e-9
                    if not breached and cumulative >= cfg.combine_profit_target_usd and consistency_ok:
                        passed = True
                        pass_day = day_number
                        break
                best_day = max((max(0.0, value["net_pnl_usd"]) for value in daily), default=0.0)
                consistency_ratio = best_day / cumulative if cumulative > 0 else math.inf
                status = (
                    "DATA_CENSORED"
                    if not full
                    else "MLL_BREACHED"
                    if breached
                    else "TARGET_REACHED"
                    if passed
                    else "FULL_COVERAGE"
                )
                rows.append(
                    {
                        "episode_id": stable_hash({"sleeve_id": sleeve_id, "start": start_session, "horizon": horizon}),
                        "sleeve_id": sleeve_id,
                        "start_session": start_session,
                        "horizon_days": horizon,
                        "scenario": scenario,
                        "coverage_status": status,
                        "full_coverage": full,
                        "target_reached": bool(passed and full),
                        "mll_breached": bool(breached),
                        "days_to_target": pass_day,
                        "net_pnl_usd": cumulative,
                        "costs_usd": float(sum(value["costs_usd"] for value in daily)),
                        "target_progress_pct": 100.0 * cumulative / cfg.combine_profit_target_usd,
                        "minimum_mll_buffer_usd": minimum_buffer,
                        "consistency_ratio": consistency_ratio,
                        "consistency_compliant": bool(consistency_ratio <= cfg.consistency_limit),
                        "daily_path": daily,
                    }
                )
    return tuple(rows)


def _candidate_economics(
    sleeve: SleeveSpec,
    intents: Sequence[SignalIntent],
    trades: Sequence[ExecutedTrade],
    episodes: Sequence[Mapping[str, Any]],
    roles: Mapping[str, str],
    *,
    session_count: int,
) -> Mapping[str, Any]:
    by_role: dict[str, Any] = {}
    for role in (ROLE_DISCOVERY, ROLE_VALIDATION, ROLE_FINAL):
        selected = [value for value in trades if value.role == role]
        by_role[role] = {
            "trade_count": len(selected),
            "normal_net_usd": float(sum(value.normal_net_pnl_usd for value in selected)),
            "stressed_net_usd": float(sum(value.stressed_net_pnl_usd for value in selected)),
            "win_rate": float(np.mean([value.stressed_net_pnl_usd > 0 for value in selected])) if selected else 0.0,
        }
    full = [value for value in episodes if value["full_coverage"]]
    mll_rate = float(np.mean([value["mll_breached"] for value in full])) if full else 0.0
    by_horizon = {}
    for horizon in HORIZONS_DAYS:
        horizon_rows = [value for value in full if value["horizon_days"] == horizon]
        by_horizon[str(horizon)] = {
            scenario: {
                "denominator": sum(value["scenario"] == scenario for value in horizon_rows),
                "pass_count": sum(value["scenario"] == scenario and value["target_reached"] for value in horizon_rows),
                "pass_rate": float(np.mean([value["target_reached"] for value in horizon_rows if value["scenario"] == scenario])) if any(value["scenario"] == scenario for value in horizon_rows) else None,
                "median_target_progress_pct": float(np.median([value["target_progress_pct"] for value in horizon_rows if value["scenario"] == scenario])) if any(value["scenario"] == scenario for value in horizon_rows) else None,
            }
            for scenario in COST_SCENARIOS
        }
    return {
        "sleeve_id": sleeve.sleeve_id,
        "signal_count": len(intents),
        "trade_count": len(trades),
        "fill_rate": len(trades) / len(intents) if intents else 0.0,
        "normal_net_usd": float(sum(value.normal_net_pnl_usd for value in trades)),
        "stressed_net_usd": float(sum(value.stressed_net_pnl_usd for value in trades)),
        "opportunities_per_five_sessions": len(intents) * 5.0 / max(1, session_count),
        "target_before_adverse_rate": float(np.mean([value.exit_reason == "TARGET" for value in trades])) if trades else 0.0,
        "mll_breach_rate": mll_rate,
        "minimum_mll_buffer_usd": min((value["minimum_mll_buffer_usd"] for value in full), default=None),
        "by_role": by_role,
        "by_horizon": by_horizon,
    }


def _matched_controls(
    sleeve: SleeveSpec,
    intents: Sequence[SignalIntent],
    snapshots: Sequence[FeatureSnapshot],
    tape_events: _CompactTape,
    roles: Mapping[str, str],
    *,
    cfg: FoundryPilotConfig,
) -> Mapping[str, Mapping[str, float]]:
    if not intents:
        return {name: {"signal_count": 0.0, "trade_count": 0.0, "stressed_net_usd": 0.0} for name in CONTROL_IDS}
    session_ranges: dict[str, tuple[int, int]] = {}
    start = 0
    while start < len(snapshots):
        end = start + 1
        while end < len(snapshots) and snapshots[end].session_id == snapshots[start].session_id:
            end += 1
        session_ranges[snapshots[start].session_id] = (start, end)
        start = end
    rng = random.Random(cfg.random_seed + int(sleeve.fingerprint[:8], 16))
    controls: dict[str, Mapping[str, float]] = {}
    variants: dict[str, list[SignalIntent]] = {name: [] for name in CONTROL_IDS}
    for offset, intent in enumerate(intents):
        variants["DIRECTION_FLIP"].append(
            SignalIntent(**{**asdict(intent), "signal_id": stable_hash({"control": "flip", "source": intent.signal_id}), "direction": -intent.direction})
        )
        session_start, session_end = session_ranges[intent.session_id]
        session_length = session_end - session_start
        source_local = intent.feature_index - session_start
        timing_row = snapshots[
            session_start
            + (source_local + max(1, session_length // 2)) % session_length
        ]
        random_index = rng.randrange(session_start, session_end)
        random_row = snapshots[random_index]
        for name, selected in (("SESSION_MATCHED_TIMING_NULL", timing_row), ("EXPOSURE_MATCHED_RANDOM", random_row)):
            latency = cfg.aggressive_latency_ns if sleeve.execution_path == "AGGRESSIVE" else cfg.passive_latency_ns
            direction = intent.direction if name == "SESSION_MATCHED_TIMING_NULL" else rng.choice((-1, 1))
            variants[name].append(
                SignalIntent(
                    sleeve_id=sleeve.sleeve_id,
                    signal_id=stable_hash({"control": name, "source": intent.signal_id, "feature": selected.feature_hash}),
                    feature_hash=selected.feature_hash,
                    feature_index=(
                        session_start
                        + (source_local + max(1, session_length // 2)) % session_length
                        if name == "SESSION_MATCHED_TIMING_NULL"
                        else random_index
                    ),
                    market=sleeve.market,
                    session_id=selected.session_id,
                    direction=direction,
                    score=intent.score,
                    signal_time_ns=selected.event_ns,
                    decision_time_ns=selected.decision_ns,
                    order_submit_time_ns=selected.decision_ns,
                    earliest_executable_time_ns=selected.decision_ns + latency,
                    execution_path=sleeve.execution_path,
                )
            )
    for name, control_intents in variants.items():
        ordered = tuple(sorted(control_intents, key=lambda value: value.decision_time_ns))
        executed = _execute_intents(sleeve, ordered, snapshots, tape_events, roles, cfg=cfg)
        controls[name] = {
            "signal_count": float(len(ordered)),
            "trade_count": float(len(executed)),
            "stressed_net_usd": float(sum(value.stressed_net_pnl_usd for value in executed)),
            "normal_net_usd": float(sum(value.normal_net_pnl_usd for value in executed)),
        }
    return controls


def _decision_report(
    *,
    cfg: FoundryPilotConfig,
    raw_sources: Sequence[RawSource],
    reconstruction: Mapping[str, Any],
    selected_sessions: Sequence[str],
    roles: Mapping[str, str],
    snapshots: Sequence[FeatureSnapshot],
    outcomes: Sequence[OutcomeRow],
    teachers: TeacherLabelSet,
    students: Sequence[StudentResult],
    candidates: Sequence[CandidateResult],
    wall_seconds: float,
    cpu_seconds: float,
) -> Mapping[str, Any]:
    all_episodes = [row for value in candidates for row in value.episodes]
    normal = [row for row in all_episodes if row["scenario"] == "NORMAL"]
    stressed = [row for row in all_episodes if row["scenario"] == "STRESSED_1_5X"]
    full = [row for row in all_episodes if row["full_coverage"]]
    full_normal = [row for row in normal if row["full_coverage"]]
    full_stressed = [row for row in stressed if row["full_coverage"]]

    def candidate_horizon(candidate: CandidateResult, horizon: int, scenario: str) -> Mapping[str, Any]:
        return candidate.economics["by_horizon"][str(horizon)][scenario]

    stressed_p5_progress = [
        float(row["median_target_progress_pct"])
        for value in candidates
        if (row := candidate_horizon(value, 5, "STRESSED_1_5X"))["median_target_progress_pct"] is not None
    ]
    normal_p5_rates = [
        float(row["pass_rate"])
        for value in candidates
        if (row := candidate_horizon(value, 5, "NORMAL"))["pass_rate"] is not None
    ]
    stressed_p5_rates = [
        float(row["pass_rate"])
        for value in candidates
        if (row := candidate_horizon(value, 5, "STRESSED_1_5X"))["pass_rate"] is not None
    ]
    useful_families = sorted({value.sleeve.family for value in candidates if value.serious})
    serious_deployable = [value for value in candidates if value.serious]
    best_progress = max(stressed_p5_progress, default=-math.inf)
    uplift_ratio = (
        best_progress / cfg.baseline_stressed_target_progress_pct
        if cfg.baseline_stressed_target_progress_pct > 0 and math.isfinite(best_progress)
        else 0.0
    )
    material_uplift = bool(
        math.isfinite(best_progress)
        and uplift_ratio >= cfg.minimum_material_uplift_ratio
        and best_progress - cfg.baseline_stressed_target_progress_pct
        >= cfg.minimum_material_uplift_points
    )
    final_positive = any(
        float(value.economics["by_role"][ROLE_FINAL]["stressed_net_usd"]) > 0.0
        for value in serious_deployable
    )
    maximum_mll_rate = max(
        (float(value.economics["mll_breach_rate"]) for value in candidates),
        default=0.0,
    )
    green_checks = {
        "material_target_velocity_uplift_over_ohlcv": material_uplift,
        "positive_stressed_final_development_economics": final_positive,
        "three_distinct_useful_mechanism_families": len(useful_families) >= cfg.minimum_useful_families,
        "deployable_serious_sleeve": bool(serious_deployable),
        "acceptable_mll": maximum_mll_rate <= cfg.maximum_mll_breach_rate,
        "final_development_evidence": any(
            int(value.economics["by_role"][ROLE_FINAL]["trade_count"]) > 0
            for value in candidates
        ),
    }
    if all(green_checks.values()):
        status = "MICROSTRUCTURE_PILOT_GREEN"
    elif material_uplift or final_positive or serious_deployable:
        status = "MICROSTRUCTURE_PILOT_WEAK"
    else:
        status = "MICROSTRUCTURE_PILOT_FALSIFIED"

    p5_normal_pass_candidates = sum(
        candidate_horizon(value, 5, "NORMAL")["pass_count"] > 0 for value in candidates
    )
    p5_stressed_pass_candidates = sum(
        candidate_horizon(value, 5, "STRESSED_1_5X")["pass_count"] > 0 for value in candidates
    )
    near_pass = sum(
        candidate_horizon(value, 5, "STRESSED_1_5X")["pass_count"] == 0
        and (
            candidate_horizon(value, 5, "STRESSED_1_5X")["median_target_progress_pct"]
            is not None
            and float(candidate_horizon(value, 5, "STRESSED_1_5X")["median_target_progress_pct"]) >= 60.0
        )
        for value in candidates
    )
    minimum_buffers = [
        float(row["minimum_mll_buffer_usd"])
        for row in full
        if row["minimum_mll_buffer_usd"] is not None
    ]
    kpis = {
        "exact_replay_count": len(candidates),
        "control_replay_count": len(candidates) * len(CONTROL_IDS),
        "normal_episode_count": len(normal),
        "stressed_episode_count": len(stressed),
        "combine_episode_count": len(all_episodes),
        "full_coverage_episode_count": len(full),
        "censored_episode_count": len(all_episodes) - len(full),
        "positive_stressed_count": sum(float(value.economics["stressed_net_usd"]) > 0 for value in candidates),
        "normal_pass_candidate_count": p5_normal_pass_candidates,
        "stressed_pass_candidate_count": p5_stressed_pass_candidates,
        "normal_p5_pass_rate_best": max(normal_p5_rates, default=None),
        "normal_p5_pass_rate_median": float(np.median(normal_p5_rates)) if normal_p5_rates else None,
        "stressed_p5_pass_rate_best": max(stressed_p5_rates, default=None),
        "stressed_p5_pass_rate_median": float(np.median(stressed_p5_rates)) if stressed_p5_rates else None,
        "stressed_p5_target_progress_best_pct": max(stressed_p5_progress, default=None),
        "stressed_p5_target_progress_population_median_pct": float(np.median(stressed_p5_progress)) if stressed_p5_progress else None,
        "mll_breach_rate_min": min((float(value.economics["mll_breach_rate"]) for value in candidates), default=0.0),
        "mll_breach_rate_max": maximum_mll_rate,
        "minimum_mll_buffer_usd_min": min(minimum_buffers, default=None),
        "minimum_mll_buffer_usd_median": float(np.median(minimum_buffers)) if minimum_buffers else None,
        "near_pass_count": near_pass,
        "economic_wall_seconds": wall_seconds,
        "economic_cpu_seconds": cpu_seconds,
        "economic_cpu_to_wall_ratio": cpu_seconds / wall_seconds,
        "exact_replays_per_hour": len(candidates) * 3600.0 / wall_seconds,
        "control_replays_per_hour": len(candidates) * len(CONTROL_IDS) * 3600.0 / wall_seconds,
        "combine_episodes_per_hour": len(all_episodes) * 3600.0 / wall_seconds,
        "feature_snapshots_per_hour": len(snapshots) * 3600.0 / wall_seconds,
    }
    teacher_counts = teachers.counts_by_role
    student_rows = [
        {
            "teacher_family": value.student.teacher_family,
            "tier": value.student.tier,
            "deployability_status": value.student.deployability_status,
            "model_hash": value.student.model_hash,
            "validation": dict(value.validation),
            "final_development": dict(value.final_development),
            "useful_final_economics": value.useful_final_economics,
        }
        for value in students
    ]
    target_before_adverse = {
        market: {
            "denominator": sum(value.market == market and value.status != "CENSORED_FUTURE_COVERAGE" for value in outcomes),
            "favorable_first": sum(value.market == market and value.favorable_first for value in outcomes),
            "rate": (
                sum(value.market == market and value.favorable_first for value in outcomes)
                / max(1, sum(value.market == market and value.status != "CENSORED_FUTURE_COVERAGE" for value in outcomes))
            ),
        }
        for market in cfg.selected_markets
    }
    candidate_rows = []
    for value in candidates:
        candidate_rows.append(
            {
                "sleeve_id": value.sleeve.sleeve_id,
                "fingerprint": value.sleeve.fingerprint,
                "family": value.sleeve.family,
                "market": value.sleeve.market,
                "deployability_tier": value.sleeve.deployability_tier,
                "execution_path": value.sleeve.execution_path,
                "serious": value.serious,
                "economics": dict(value.economics),
                "controls": {key: dict(row) for key, row in value.controls.items()},
            }
        )
    return _json_safe(
        {
            "schema": "hydra_microstructure_order_flow_foundry_pilot_report_v1",
            "campaign_id": cfg.campaign_id,
            "manifest_hash": cfg.manifest_hash,
            "created_at_utc": datetime.now(UTC).isoformat(),
            "pilot_status": status,
            "green_checks": green_checks,
            "market_sources": [asdict(value) for value in raw_sources],
            "event_reconstruction": dict(reconstruction),
            "selected_sessions": list(selected_sessions),
            "chronological_roles": dict(roles),
            "target_before_adverse": target_before_adverse,
            "teacher_counts": teacher_counts,
            "teacher_label_hash": teachers.label_hash,
            "students": student_rows,
            "candidate_mechanism_families": list(EXPERT_FAMILIES),
            "candidate_count": len(candidates),
            "serious_deployable_count": len(serious_deployable),
            "useful_mechanism_families": useful_families,
            "ohlcv_baseline_stressed_target_progress_pct": cfg.baseline_stressed_target_progress_pct,
            "ohlcv_population_median_stressed_target_progress_pct": cfg.baseline_population_median_stressed_target_progress_pct,
            "best_microstructure_stressed_target_progress_pct": None if not math.isfinite(best_progress) else best_progress,
            "target_velocity_uplift_ratio": uplift_ratio,
            "frozen_cost_model": {
                market: {
                    "round_turn_commission_usd": cfg.normal_round_turn_commission_usd[market],
                    "base_slippage_ticks_per_side": cfg.adverse_slippage_ticks_per_side,
                    "tick_value_usd": cfg.tick_size[market] * cfg.point_value[market],
                    "normal_effective_round_turn_cost_usd": (
                        cfg.normal_round_turn_commission_usd[market]
                        + 2.0
                        * cfg.adverse_slippage_ticks_per_side
                        * cfg.tick_size[market]
                        * cfg.point_value[market]
                    ),
                    "stressed_effective_round_turn_cost_usd": (
                        cfg.normal_round_turn_commission_usd[market]
                        + 2.0
                        * cfg.adverse_slippage_ticks_per_side
                        * cfg.stressed_cost_multiplier
                        * cfg.tick_size[market]
                        * cfg.point_value[market]
                    ),
                }
                for market in cfg.selected_markets
            },
            "production_kpis": kpis,
            "candidates": candidate_rows,
            "governance": {
                "live_trading": False,
                "broker_connection": False,
                "orders": False,
                "q4_access": False,
                "mbo_teacher_direct_deployment": False,
                "mass_scale_authorized": status == "MICROSTRUCTURE_PILOT_GREEN",
                "xfa_authorized": False,
            },
        }
    )


def _persist_event_store(
    root: Path,
    *,
    cfg: FoundryPilotConfig,
    raw_sources: Sequence[RawSource],
    snapshots: Sequence[FeatureSnapshot],
    tape_events: Mapping[str, _CompactTape],
    outcomes: Sequence[OutcomeRow],
    teachers: TeacherLabelSet,
    students: Sequence[StudentResult],
    candidates: Sequence[CandidateResult],
    report: Mapping[str, Any],
) -> Mapping[str, Path]:
    datasets: dict[str, Path] = {}

    def persist(name: str, records: Iterable[Mapping[str, Any]]) -> None:
        path = root / "datasets" / name / "part-000000.parquet"
        _write_parquet(path, records)
        datasets[name] = path

    persist("raw_dbn", [asdict(value) for value in raw_sources])
    persist(
        "book_snapshots",
        (
            {
                "market": value.market,
                "contract": value.contract,
                "session_id": value.session_id,
                "event_ns": value.event_ns,
                "available_ns": value.available_ns,
                "event_fingerprint": value.event_fingerprint,
                "state_hash": value.state_hash,
                "bid_price": value.bid_price,
                "ask_price": value.ask_price,
                "bid_size": value.bid_size,
                "ask_size": value.ask_size,
                "bid_depth_json": json.dumps(value.bid_depth, separators=(",", ":")),
                "ask_depth_json": json.dumps(value.ask_depth, separators=(",", ":")),
            }
            for value in snapshots
        ),
    )
    persist(
        "derived_events",
        (
            {"market": market, "available_ns": row[0], "price": row[1], "size": row[2], "side": row[3], "session_id": row[4]}
            for market, tape in tape_events.items()
            for row in tape.records()
        ),
    )
    persist(
        "feature_matrices",
        (
            {
                "feature_hash": value.feature_hash,
                "market": value.market,
                "contract": value.contract,
                "session_id": value.session_id,
                "decision_ns": value.decision_ns,
                "available_ns": value.available_ns,
                **dict(zip(value.names, value.values, strict=True)),
            }
            for value in snapshots
        ),
    )
    ordered = tuple(sorted(snapshots, key=lambda value: (value.decision_ns, value.market)))
    outcome_by_hash = {value.feature_hash: value for value in outcomes}
    persist(
        "outcome_labels",
        (
            {
                **asdict(outcome_by_hash[value.feature_hash]),
                **{f"teacher_{family.lower()}": bool(teachers.labels[family][offset]) for family in teachers.labels},
            }
            for offset, value in enumerate(ordered)
        ),
    )
    persist(
        "student_models",
        [
            {
                "teacher_family": value.student.teacher_family,
                "tier": value.student.tier,
                "model_hash": value.student.model_hash,
                "student_json": json.dumps(_json_safe(value.to_dict()), sort_keys=True, separators=(",", ":")),
            }
            for value in students
        ],
    )
    persist("sleeve_manifests", [asdict(value.sleeve) | {"fingerprint": value.sleeve.fingerprint} for value in candidates])
    persist("signals", [asdict(signal) for value in candidates for signal in value.signals])
    persist("trades", [asdict(trade) for value in candidates for trade in value.trades])
    persist("episodes", [{key: val for key, val in row.items() if key != "daily_path"} for value in candidates for row in value.episodes])
    persist(
        "account_daily_paths",
        [
            {"episode_id": row["episode_id"], "sleeve_id": row["sleeve_id"], "scenario": row["scenario"], **daily}
            for value in candidates
            for row in value.episodes
            for daily in row["daily_path"]
        ],
    )
    persist(
        "matched_controls",
        [
            {"sleeve_id": value.sleeve.sleeve_id, "control_id": control_id, **dict(metrics)}
            for value in candidates
            for control_id, metrics in value.controls.items()
        ],
    )
    report_path = root / "decision_report.json"
    _write_json(report_path, report)
    datasets["decision_report"] = report_path
    schema_path = root / "event_store_schema.json"
    _write_json(
        schema_path,
        {
            "schema": EVENT_STORE_SCHEMA,
            "campaign_id": cfg.campaign_id,
            "layers": ["RAW_DBN", "BOOK_SNAPSHOTS", "DERIVED_EVENTS", "FEATURE_MATRICES", "OUTCOME_LABELS"],
            "labels_physically_separate": True,
            "compression": "parquet_zstd",
            "feature_availability_contract": "available_at<=decision_time",
        },
    )
    datasets["event_store_schema"] = schema_path
    return datasets


def _canonical_evidence_material(
    *,
    cfg: FoundryPilotConfig,
    raw_sources: Sequence[RawSource],
    selected_sessions: Sequence[str],
    roles: Mapping[str, str],
    candidates: Sequence[CandidateResult],
    report: Mapping[str, Any],
    event_store_paths: Mapping[str, str],
    store_receipt: Mapping[str, Any],
) -> tuple[
    Mapping[str, Any],
    Mapping[str, Sequence[Mapping[str, Any]]],
    Mapping[str, Any],
]:
    """Build the bounded canonical EvidenceBundle material expected by V17.

    Raw event/book/feature matrices stay in immutable Parquet and are bound by
    hash in provenance.  Only bounded sleeve/account ledgers are returned to
    ``EvidenceBundleWriter.append_records``.
    """

    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    configuration_sha = (
        cfg.manifest_hash
        if len(cfg.manifest_hash) == 64 and all(ch in "0123456789abcdef" for ch in cfg.manifest_hash)
        else stable_hash(asdict(cfg))
    )
    source_commit = (
        cfg.source_commit
        if len(cfg.source_commit) in {40, 64} and all(ch in "0123456789abcdef" for ch in cfg.source_commit)
        else "0" * 40
    )
    policy_fingerprints = {value.sleeve.sleeve_id: value.sleeve.fingerprint for value in candidates}
    component_fingerprints = dict(policy_fingerprints)
    data_fingerprints = {f"raw:{value.market}:{value.contract}": value.sha256 for value in raw_sources}

    required_episode_keys: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for candidate in candidates:
        for row in candidate.episodes:
            key = (
                candidate.sleeve.sleeve_id,
                str(row["episode_id"]),
                f"{int(row['horizon_days'])}D",
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            required_episode_keys.append(
                {"policy_id": key[0], "episode_id": key[1], "horizon": key[2]}
            )
    identity = {
        "campaign_id": cfg.campaign_id,
        "grammar_id": "event_sourced_lob_teacher_student_moe_v1",
        "policy_fingerprints": policy_fingerprints,
        "component_fingerprints": component_fingerprints,
        "source_commit": source_commit,
        "data_fingerprints": data_fingerprints,
        "configuration_sha256": configuration_sha,
        "seeds": [cfg.random_seed],
        "created_at_utc": now,
        "expected_coverage": {
            "policy_ids": list(policy_fingerprints),
            "component_ids": list(component_fingerprints),
            "required_episode_keys": required_episode_keys,
            "allowed_horizons": ["5D", "10D", "20D"],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": False,
        },
    }

    signals: list[Mapping[str, Any]] = []
    entries: list[Mapping[str, Any]] = []
    exits: list[Mapping[str, Any]] = []
    trades: list[Mapping[str, Any]] = []
    memberships: list[Mapping[str, Any]] = []
    episode_rows: list[Mapping[str, Any]] = []
    account_paths: list[Mapping[str, Any]] = []
    for candidate in candidates:
        sleeve = candidate.sleeve
        trade_by_signal = {value.signal_id: value for value in candidate.trades}
        for signal in candidate.signals:
            matched = trade_by_signal.get(signal.signal_id)
            signals.append(
                {
                    "campaign_id": cfg.campaign_id,
                    "component_id": sleeve.sleeve_id,
                    "signal_id": signal.signal_id,
                    "event_time": _ns_iso(signal.signal_time_ns),
                    "market": sleeve.market,
                    "contract": cfg.contracts[sleeve.market],
                    "timeframe": "EVENT",
                    "signal": signal.direction,
                    "sizing": float(sleeve.quantity),
                    "stop": None if matched is None else matched.stop_price,
                    "target": None if matched is None else matched.target_price,
                    "veto": False,
                    "component_role": sleeve.family,
                }
            )
        for trade in candidate.trades:
            side = "LONG" if trade.direction > 0 else "SHORT"
            entries.append(
                {
                    "campaign_id": cfg.campaign_id,
                    "component_id": sleeve.sleeve_id,
                    "trade_id": trade.trade_id,
                    "entry_time": _ns_iso(trade.entry_time_ns),
                    "market": sleeve.market,
                    "contract": cfg.contracts[sleeve.market],
                    "side": side,
                    "quantity": float(trade.filled_quantity),
                    "entry_price": trade.entry_price,
                    "sizing": float(trade.filled_quantity),
                    "stop_price": trade.stop_price,
                    "target_price": trade.target_price,
                }
            )
            exits.append(
                {
                    "campaign_id": cfg.campaign_id,
                    "component_id": sleeve.sleeve_id,
                    "trade_id": trade.trade_id,
                    "exit_time": _ns_iso(trade.exit_time_ns),
                    "exit_price": trade.exit_price,
                    "exit_reason": trade.exit_reason,
                }
            )
            trades.append(
                {
                    "campaign_id": cfg.campaign_id,
                    "component_id": sleeve.sleeve_id,
                    "trade_id": trade.trade_id,
                    "entry_time": _ns_iso(trade.entry_time_ns),
                    "exit_time": _ns_iso(trade.exit_time_ns),
                    "market": sleeve.market,
                    "contract": cfg.contracts[sleeve.market],
                    "side": side,
                    "quantity": float(trade.filled_quantity),
                    "entry_price": trade.entry_price,
                    "exit_price": trade.exit_price,
                    "gross_pnl": trade.gross_pnl_usd,
                    "costs": trade.normal_costs_usd,
                    "net_pnl": trade.normal_net_pnl_usd,
                }
            )
        memberships.append(
            {
                "campaign_id": cfg.campaign_id,
                "policy_id": sleeve.sleeve_id,
                "component_id": sleeve.sleeve_id,
                "risk_allocation": 1.0,
                "component_role": sleeve.family,
            }
        )
        for row in candidate.episodes:
            full = bool(row["full_coverage"])
            terminal = (
                "DATA_CENSORED"
                if not full
                else "MLL_BREACHED"
                if row["mll_breached"]
                else "TARGET_REACHED"
                if row["target_reached"]
                else "OPERATIONAL_HORIZON_NOT_REACHED"
            )
            episode_rows.append(
                {
                    "campaign_id": cfg.campaign_id,
                    "policy_id": sleeve.sleeve_id,
                    "episode_id": row["episode_id"],
                    "episode_start": f"{row['start_session']}T00:00:00Z",
                    "horizon": f"{int(row['horizon_days'])}D",
                    "temporal_block": roles[str(row["start_session"])],
                    "duration_trading_days": len(row["daily_path"]),
                    "target_reached": terminal == "TARGET_REACHED",
                    "mll_breached": terminal == "MLL_BREACHED",
                    "censored_state": terminal in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"},
                    "cost_scenario": row["scenario"],
                    "costs": row["costs_usd"],
                    "net_pnl": row["net_pnl_usd"],
                    "target_progress": row["target_progress_pct"],
                    "minimum_mll_buffer": float(row["minimum_mll_buffer_usd"]),
                    "consistency_ok": bool(row["consistency_compliant"]),
                    "days_to_target": None if row["days_to_target"] is None else float(row["days_to_target"]),
                    "failure_vector": [] if terminal == "TARGET_REACHED" else [terminal],
                    "terminal_state": terminal,
                }
            )
            trailing_high = 0.0
            best_day = 0.0
            for daily in row["daily_path"]:
                cumulative = float(daily["cumulative_net_usd"])
                trailing_high = max(trailing_high, cumulative)
                mll_level = 150_000.0 + max(0.0, trailing_high) - cfg.combine_mll_usd
                equity = 150_000.0 + cumulative
                buffer = equity - mll_level
                best_day = max(best_day, max(0.0, float(daily["net_pnl_usd"])))
                consistency = best_day / cumulative if cumulative > 0 else 1.0
                account_paths.append(
                    {
                        "campaign_id": cfg.campaign_id,
                        "policy_id": sleeve.sleeve_id,
                        "episode_id": row["episode_id"],
                        "horizon": f"{int(row['horizon_days'])}D",
                        "trading_day": daily["session_id"],
                        "cost_scenario": row["scenario"],
                        "realized_pnl": cumulative,
                        "unrealized_pnl": 0.0,
                        "daily_pnl": daily["net_pnl_usd"],
                        "equity": equity,
                        "mll": mll_level,
                        "mll_buffer": buffer,
                        "minimum_mll_buffer": min(buffer, float(row["minimum_mll_buffer_usd"])),
                        "consistency": consistency,
                        "consistency_ok": consistency <= cfg.consistency_limit,
                        "target_progress": 100.0 * cumulative / cfg.combine_profit_target_usd,
                        "costs": daily["costs_usd"],
                        "conflicts": [],
                        "exposure": {sleeve.market: 0.0},
                        "component_attribution": {sleeve.sleeve_id: daily["net_pnl_usd"]},
                    }
                )

    access_hash = (
        cfg.acquisition_receipt_hash
        if len(cfg.acquisition_receipt_hash) == 64
        and all(ch in "0123456789abcdef" for ch in cfg.acquisition_receipt_hash)
        else stable_hash([asdict(value) for value in raw_sources])
    )
    provenance = [
        {
            "campaign_id": cfg.campaign_id,
            "validator_version": FOUNDRY_PILOT_VERSION,
            "replay_version": "hydra_microstructure_event_engine_v1",
            "market_data_role": "CHRONOLOGICAL_60_20_20_DEVELOPMENT",
            "access_ledger_sha256": access_hash,
            "reconstruction_flag": False,
            "immutable_checksums": {
                "configuration": configuration_sha,
                "event_store_receipt": str(store_receipt["receipt_hash"]),
                **{f"data:{key}": value for key, value in data_fingerprints.items()},
            },
            "recorded_at_utc": now,
        }
    ]
    datasets = {
        "component_signals": signals,
        "component_entries": entries,
        "component_exits": exits,
        "component_trades": trades,
        "account_policy_membership": memberships,
        "account_daily_paths": account_paths,
        "episodes": episode_rows,
        "provenance": provenance,
    }
    kpis = dict(report["production_kpis"])
    compact = {
        "campaign_summary": {
            "decision": report["pilot_status"],
            "candidate_count": len(candidates),
            "serious_candidate_count": report["serious_deployable_count"],
            "event_store_paths": dict(event_store_paths),
            "event_store_receipt_hash": store_receipt["receipt_hash"],
            **kpis,
        },
        "failure_vectors": [
            {
                "sleeve_id": value.sleeve.sleeve_id,
                "failure": (
                    "NONE"
                    if value.serious
                    else "NO_FINAL_DEPLOYABLE_STRESSED_ECONOMIC_UTILITY"
                ),
            }
            for value in candidates
        ],
        "pareto_archive": [
            {
                "sleeve_id": value.sleeve.sleeve_id,
                "family": value.sleeve.family,
                "deployability_tier": value.sleeve.deployability_tier,
                "stressed_net_usd": value.economics["stressed_net_usd"],
                "mll_breach_rate": value.economics["mll_breach_rate"],
                "serious": value.serious,
            }
            for value in candidates
        ],
        "next_campaign_recommendations": {
            "action": {
                "MICROSTRUCTURE_PILOT_GREEN": "SCALE_QUALIFIED_MICROSTRUCTURE_BANK",
                "MICROSTRUCTURE_PILOT_WEAK": "ONE_TARGETED_IMPROVEMENT_WAVE_NO_PURCHASE",
                "MICROSTRUCTURE_PILOT_FALSIFIED": "STOP_MICROSTRUCTURE_EXPANSION",
            }[str(report["pilot_status"])]
        },
    }
    return identity, datasets, compact


def _ns_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat().replace("+00:00", "Z")


def _seal_store_receipt(
    root: Path,
    *,
    cfg: FoundryPilotConfig,
    datasets: Mapping[str, Path],
    reconstruction: Mapping[str, Any],
    report: Mapping[str, Any],
) -> Mapping[str, Any]:
    inventory = {
        name: {
            "path": str(path.relative_to(root)),
            "sha256": _sha256_file(path),
            "byte_count": path.stat().st_size,
        }
        for name, path in sorted(datasets.items())
    }
    payload = {
        "schema": "hydra_microstructure_event_store_receipt_v1",
        "campaign_id": cfg.campaign_id,
        "manifest_hash": cfg.manifest_hash,
        "pilot_version": FOUNDRY_PILOT_VERSION,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "status": reconstruction["status"],
        "pilot_status": report["pilot_status"],
        "datasets": inventory,
        "raw_rewrite_allowed": False,
        "outcome_labels_physically_separate": True,
    }
    receipt = {**payload, "receipt_hash": stable_hash(payload)}
    _write_json(root / "store_receipt.json", receipt)
    return receipt


def _write_parquet(
    path: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    batch_size: int = 25_000,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    arrow_schema: pa.Schema | None = None
    batch: list[Mapping[str, Any]] = []
    try:
        for value in records:
            batch.append(_json_safe(dict(value)))
            if len(batch) < batch_size:
                continue
            table = pa.Table.from_pylist(batch, schema=arrow_schema)
            if writer is None:
                arrow_schema = table.schema
                writer = pq.ParquetWriter(
                    temporary,
                    table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            writer.write_table(table)
            batch.clear()
        if batch or writer is None:
            if writer is None and not batch:
                batch = [{"_empty": True}]
            table = pa.Table.from_pylist(batch, schema=arrow_schema)
            if writer is None:
                arrow_schema = table.schema
                writer = pq.ParquetWriter(
                    temporary,
                    table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    os.replace(temporary, path)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(_json_safe(value), sort_keys=True, indent=2) + "\n")
    os.replace(temporary, path)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, np.bool_):
        return bool(value)
    return value


__all__ = [
    "CONTROL_IDS",
    "DEPLOYABILITY_TIERS",
    "EVENT_STORE_SCHEMA",
    "EXECUTION_PATHS",
    "EXPERT_FAMILIES",
    "FOUNDRY_PILOT_VERSION",
    "FoundryPilotConfig",
    "FoundryPilotError",
    "FoundryPilotResult",
    "iter_dbn_mbo_events",
    "iter_dbn_mbo_events_multi",
    "iter_dbn_mbo_events_multi_from_store",
    "run_microstructure_foundry_pilot",
    "run_microstructure_foundry_pilot_from_events",
]
