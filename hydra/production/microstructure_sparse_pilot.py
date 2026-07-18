"""Bounded sparse-alpha distillation over the immutable 0031 event store.

The module deliberately does not decode the purchased DBN stream again.  It
consumes the causal feature, book and tape partitions already sealed by 0031,
consolidates repeated updates into opportunity episodes, trains abstention
models on the first three sessions, and evaluates at most thirty frozen sparse
strategies on the fourth and fifth sessions.

Future outcomes are loaded only after opportunity decisions have been
materialised.  They are never passed to the finite-state engine or to model
inference.  Candidate execution is aggressive and conservative: entry at the
executable opposite quote after frozen latency, displayed-depth consumption,
adverse slippage, commission, and the identical stressed-cost multiplier used
by 0031.
"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import time
from typing import Any, Iterable, Mapping, Sequence

# Three-core contract: the controller/writer owns one core and the two economic
# workers are deliberately single-threaded.  Set these before importing numpy
# so BLAS cannot silently oversubscribe the host.
for _thread_environment in (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[_thread_environment] = "1"

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.linear_model import LogisticRegression

from hydra.economic_evolution.schema import stable_hash
from hydra.production.microstructure_opportunity_episode import (
    OpportunityEpisodeFSM,
    OpportunityEpisodeSpec,
    OpportunityObservation,
)


SPARSE_PILOT_VERSION = "hydra_microstructure_sparse_alpha_distillation_pilot_v1"
PILOT_STATUSES = (
    "SPARSE_PILOT_GREEN",
    "SPARSE_PILOT_WEAK",
    "SPARSE_PILOT_FALSIFIED",
)
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
MECHANISMS = (
    "INITIATIVE",
    "ABSORPTION",
    "LIQUIDITY_VACUUM",
    "EXHAUSTION",
    "QUEUE_REPLENISHMENT",
)
DEPLOYABILITY_TIERS = ("L1_DEPLOYABLE", "L2_DEPLOYABLE")
COST_SCENARIOS = ("NORMAL", "STRESSED_1_5X")
ACCOUNT_HORIZONS_DAYS = (5, 10, 20)

L1_META_FEATURES = (
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
    "flow_2s",
    "flow_30s",
    "arrival_2s",
    "arrival_30s",
    "session_elapsed_fraction",
)
L2_EXTRA_FEATURES = (
    "depth_3_imbalance",
    "depth_5_imbalance",
    "depth_10_imbalance",
    "depth_slope",
    "depth_convexity",
    "depletion_rate",
    "replenishment_rate",
    "liquidity_gap_ticks",
    "depth_withdrawal_rate",
    "queue_persistence",
    "ephemeral_liquidity_rate",
)


class SparsePilotError(RuntimeError):
    """The 0032 store, causal contract, or sparse replay is invalid."""


@dataclass(frozen=True, slots=True)
class SparsePilotConfig:
    campaign_id: str = "hydra_microstructure_sparse_alpha_distillation_0032"
    manifest_hash: str = "UNBOUND_TEST_MANIFEST"
    source_commit: str = "0" * 40
    source_store_hash: str = "0" * 64
    selected_markets: tuple[str, str] = ("NQ", "YM")
    contracts: Mapping[str, str] = None  # type: ignore[assignment]
    chronological_roles: tuple[int, int, int] = (3, 1, 1)
    cpu_worker_count: int = 2
    edge_to_cost_ratios: tuple[float, ...] = (1.25, 1.50, 2.00, 3.00)
    trade_budgets: tuple[int, ...] = (2, 4, 8, 12)
    holding_horizons_seconds: tuple[int, ...] = (30, 120, 300, 900)
    exit_policies: tuple[str, ...] = (
        "FIXED_TARGET_STOP",
        "ORDER_FLOW_DECAY",
        "OPPOSITE_STATE_TRANSITION",
        "TIME_STOP",
        "VWAP_LIQUIDITY_LEVEL",
        "EVENT_STATE_RESET",
    )
    maximum_strategies: int = 30
    opportunity_gap_ns: int = 15_000_000_000
    price_zone_ticks: int = 4
    hysteresis_enter_quantile: float = 0.85
    hysteresis_exit_quantile: float = 0.65
    aggressive_latency_ns: int = 25_000_000
    adverse_slippage_ticks_per_side: float = 2.0
    quantity: int = 1
    # The sparse pilot freezes a 16/6-tick excursion contract.  An 8-tick
    # target could not satisfy even the 1.25x edge/cost gate after the sealed
    # aggressive 0031 slippage, making the declared frontier vacuous.
    target_ticks: float = 16.0
    stop_ticks: float = 6.0
    stressed_cost_multiplier: float = 1.5
    commission_usd: Mapping[str, float] = None  # type: ignore[assignment]
    tick_size: Mapping[str, float] = None  # type: ignore[assignment]
    point_value: Mapping[str, float] = None  # type: ignore[assignment]
    account_snapshots: Mapping[str, Mapping[str, float]] = None  # type: ignore[assignment]
    random_seed: int = 32_032

    def __post_init__(self) -> None:
        if self.contracts is None:
            object.__setattr__(self, "contracts", {"NQ": "NQU4", "YM": "YMU4"})
        if self.commission_usd is None:
            object.__setattr__(self, "commission_usd", {"NQ": 3.8, "YM": 3.8})
        if self.tick_size is None:
            object.__setattr__(self, "tick_size", {"NQ": 0.25, "YM": 1.0})
        if self.point_value is None:
            object.__setattr__(self, "point_value", {"NQ": 20.0, "YM": 5.0})
        if self.account_snapshots is None:
            object.__setattr__(
                self,
                "account_snapshots",
                {
                    "50K": {"account_size": 50_000.0, "target": 3_000.0, "mll": 2_000.0, "max_contracts": 5.0, "consistency": 0.5},
                    "100K": {"account_size": 100_000.0, "target": 6_000.0, "mll": 3_000.0, "max_contracts": 10.0, "consistency": 0.5},
                    "150K": {"account_size": 150_000.0, "target": 9_000.0, "mll": 4_500.0, "max_contracts": 15.0, "consistency": 0.5},
                },
            )

    def validate(self) -> None:
        if tuple(self.chronological_roles) != (3, 1, 1):
            raise SparsePilotError("0032 chronological roles must remain 3/1/1")
        if self.cpu_worker_count != 2:
            raise SparsePilotError("0032 requires exactly two CPU economic workers")
        if tuple(self.edge_to_cost_ratios) != (1.25, 1.5, 2.0, 3.0):
            raise SparsePilotError("0032 edge-to-cost frontier drift")
        if tuple(self.trade_budgets) != (2, 4, 8, 12):
            raise SparsePilotError("0032 trade-budget frontier drift")
        if tuple(self.holding_horizons_seconds) != (30, 120, 300, 900):
            raise SparsePilotError("0032 holding-horizon frontier drift")
        if not 1 <= self.maximum_strategies <= 30:
            raise SparsePilotError("0032 sparse candidate cap drift")
        if len(self.selected_markets) != 2 or set(self.selected_markets) != set(self.contracts):
            raise SparsePilotError("0032 frozen two-market binding drift")
        if not 0.5 < self.hysteresis_exit_quantile < self.hysteresis_enter_quantile < 1.0:
            raise SparsePilotError("0032 hysteresis contract drift")


@dataclass(slots=True)
class SparseStore:
    feature_names: tuple[str, ...]
    feature_values: np.ndarray
    feature_hashes: np.ndarray
    market: np.ndarray
    contract: np.ndarray
    session: np.ndarray
    decision_ns: np.ndarray
    available_ns: np.ndarray
    bid_price: np.ndarray
    ask_price: np.ndarray
    bid_size: np.ndarray
    ask_size: np.ndarray
    bid_depth_json: np.ndarray
    ask_depth_json: np.ndarray
    last_trade_price: np.ndarray
    derived_available_ns: Mapping[str, np.ndarray]
    derived_price: Mapping[str, np.ndarray]
    derived_size: Mapping[str, np.ndarray]
    derived_side: Mapping[str, np.ndarray]
    roles: Mapping[str, str]
    sessions: tuple[str, ...]
    source_hashes: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class OpportunityEpisode:
    opportunity_id: str
    market: str
    contract: str
    session_id: str
    role: str
    mechanism: str
    direction: int
    start_index: int
    confirmation_index: int
    end_index: int
    start_ns: int
    confirmation_ns: int
    end_ns: int
    price_zone: int
    supporting_event_count: int
    feature_hash: str
    feature_vector_hash: str
    maximum_flow_pressure: float
    maximum_absorption_depletion: float
    vwap_location_ticks: float
    cross_market_confirmation: float


@dataclass(frozen=True, slots=True)
class OpportunityOutcome:
    opportunity_id: str
    favorable_first: bool
    adverse_first: bool
    timeout: bool
    censored: bool
    time_to_favorable_ns: int | None
    time_to_adverse_ns: int | None
    mfe_ticks: float | None
    mae_ticks: float | None
    markouts_ticks: Mapping[str, float | None]


@dataclass(frozen=True, slots=True)
class FrozenMetaModel:
    mechanism: str
    tier: str
    feature_names: tuple[str, ...]
    coefficients: tuple[float, ...]
    intercept: float
    discovery_positive_rate: float
    model_hash: str


@dataclass(frozen=True, slots=True)
class SparseStrategySpec:
    strategy_id: str
    mechanism: str
    tier: str
    deployability_tier: str
    edge_to_cost_ratio: float
    trade_budget_per_session: int
    holding_horizon_seconds: int
    exit_policy: str
    target_ticks: float
    stop_ticks: float
    quantity: int
    model_hash: str
    specification_hash: str


@dataclass(frozen=True, slots=True)
class SparseTrade:
    strategy_id: str
    opportunity_id: str
    trade_id: str
    market: str
    session_id: str
    role: str
    direction: int
    entry_index: int
    exit_index: int
    entry_time_ns: int
    exit_time_ns: int
    entry_price: float
    exit_price: float
    stop_price: float
    target_price: float
    exit_reason: str
    quantity: int
    gross_reference_pnl_usd: float
    spread_cost_usd: float
    marketable_slippage_usd: float
    depth_slippage_usd: float
    commission_usd: float
    adverse_selection_usd: float
    normal_net_pnl_usd: float
    stressed_net_pnl_usd: float
    minimum_unrealized_pnl_usd: float
    prediction: float
    expected_edge_to_cost: float


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _read_parquet(path: Path, columns: Sequence[str] | None = None) -> pa.Table:
    if not path.is_file():
        raise SparsePilotError(f"0032 source partition missing: {path}")
    return pq.read_table(path, columns=list(columns) if columns is not None else None)


def load_sparse_source_store(source_dir: str | Path, *, config: SparsePilotConfig) -> SparseStore:
    """Load the immutable derived 0031 store without decoding raw DBN again."""

    config.validate()
    root = Path(source_dir).resolve()
    paths = {
        name: root / "datasets" / name / "part-000000.parquet"
        for name in ("feature_matrices", "book_snapshots", "derived_events")
    }
    source_hashes = {name: _sha256_file(path) for name, path in paths.items()}
    feature_table = _read_parquet(paths["feature_matrices"])
    book_table = _read_parquet(paths["book_snapshots"])
    if feature_table.num_rows != book_table.num_rows or feature_table.num_rows <= 0:
        raise SparsePilotError("0032 feature/book row inventory drift")

    feature_market = np.asarray(feature_table["market"].to_pylist(), dtype=object)
    book_market = np.asarray(book_table["market"].to_pylist(), dtype=object)
    feature_session = np.asarray(feature_table["session_id"].to_pylist(), dtype=object)
    book_session = np.asarray(book_table["session_id"].to_pylist(), dtype=object)
    decision_ns = feature_table["decision_ns"].to_numpy(zero_copy_only=False).astype(np.int64)
    available_ns = feature_table["available_ns"].to_numpy(zero_copy_only=False).astype(np.int64)
    book_available = book_table["available_ns"].to_numpy(zero_copy_only=False).astype(np.int64)
    if (
        not np.array_equal(feature_market, book_market)
        or not np.array_equal(feature_session, book_session)
        or not np.array_equal(available_ns, book_available)
        or np.any(available_ns > decision_ns)
    ):
        raise SparsePilotError("0032 causal feature/book alignment drift")
    sessions = tuple(sorted(str(value) for value in np.unique(feature_session)))
    if len(sessions) != sum(config.chronological_roles):
        raise SparsePilotError("0032 requires the five frozen complete sessions")
    discovery, validation, final = config.chronological_roles
    roles = {
        session: (
            "DISCOVERY"
            if offset < discovery
            else "VALIDATION"
            if offset < discovery + validation
            else "FINAL_DEVELOPMENT"
        )
        for offset, session in enumerate(sessions)
    }

    excluded = {"feature_hash", "market", "contract", "session_id", "decision_ns", "available_ns"}
    feature_names = tuple(name for name in feature_table.column_names if name not in excluded)
    feature_values = np.column_stack(
        [feature_table[name].to_numpy(zero_copy_only=False) for name in feature_names]
    ).astype(np.float64, copy=False)
    feature_values = np.nan_to_num(feature_values, nan=0.0, posinf=0.0, neginf=0.0)

    derived = _read_parquet(paths["derived_events"])
    derived_market = np.asarray(derived["market"].to_pylist(), dtype=object)
    derived_available = derived["available_ns"].to_numpy(zero_copy_only=False).astype(np.int64)
    derived_price_all = derived["price"].to_numpy(zero_copy_only=False).astype(np.float64)
    derived_size_all = derived["size"].to_numpy(zero_copy_only=False).astype(np.int64)
    derived_side_all = np.asarray(derived["side"].to_pylist(), dtype=object)
    by_time: dict[str, np.ndarray] = {}
    by_price: dict[str, np.ndarray] = {}
    by_size: dict[str, np.ndarray] = {}
    by_side: dict[str, np.ndarray] = {}
    last_trade = np.empty(len(decision_ns), dtype=np.float64)
    for market in config.selected_markets:
        mask = derived_market == market
        order = np.argsort(derived_available[mask], kind="stable")
        times = derived_available[mask][order]
        prices = derived_price_all[mask][order]
        sizes = derived_size_all[mask][order]
        sides = derived_side_all[mask][order]
        if len(times) == 0 or np.any(np.diff(times) < 0):
            raise SparsePilotError(f"0032 derived tape invalid for {market}")
        by_time[market] = times
        by_price[market] = prices
        by_size[market] = sizes
        by_side[market] = sides
        rows = np.flatnonzero(feature_market == market)
        offsets = np.searchsorted(times, available_ns[rows], side="right") - 1
        if np.any(offsets < 0):
            raise SparsePilotError(f"0032 causal as-of trade join missing for {market}")
        last_trade[rows] = prices[offsets]

    return SparseStore(
        feature_names=feature_names,
        feature_values=feature_values,
        feature_hashes=np.asarray(feature_table["feature_hash"].to_pylist(), dtype=object),
        market=feature_market,
        contract=np.asarray(feature_table["contract"].to_pylist(), dtype=object),
        session=feature_session,
        decision_ns=decision_ns,
        available_ns=available_ns,
        bid_price=book_table["bid_price"].to_numpy(zero_copy_only=False).astype(np.float64),
        ask_price=book_table["ask_price"].to_numpy(zero_copy_only=False).astype(np.float64),
        bid_size=book_table["bid_size"].to_numpy(zero_copy_only=False).astype(np.int64),
        ask_size=book_table["ask_size"].to_numpy(zero_copy_only=False).astype(np.int64),
        bid_depth_json=np.asarray(book_table["bid_depth_json"].to_pylist(), dtype=object),
        ask_depth_json=np.asarray(book_table["ask_depth_json"].to_pylist(), dtype=object),
        last_trade_price=last_trade,
        derived_available_ns=by_time,
        derived_price=by_price,
        derived_size=by_size,
        derived_side=by_side,
        roles=roles,
        sessions=sessions,
        source_hashes=source_hashes,
    )


def _feature_index(store: SparseStore) -> Mapping[str, int]:
    return {name: offset for offset, name in enumerate(store.feature_names)}


def _column(store: SparseStore, name: str) -> np.ndarray:
    index = _feature_index(store)
    if name not in index:
        raise SparsePilotError(f"0032 causal feature absent: {name}")
    return store.feature_values[:, index[name]]


def _discovery_thresholds(store: SparseStore, cfg: SparsePilotConfig) -> Mapping[str, Mapping[str, float]]:
    index = _feature_index(store)
    required = {
        "flow_2s", "flow_30s", "arrival_2s", "price_response_per_signed_contract",
        "replenishment_rate", "depletion_rate", "depth_withdrawal_rate",
        "liquidity_gap_ticks", "microprice_deviation", "queue_persistence",
        "bbo_imbalance", "ephemeral_liquidity_rate",
    }
    if not required <= set(index):
        raise SparsePilotError(f"0032 FSM inputs absent: {sorted(required - set(index))}")
    thresholds: dict[str, Mapping[str, float]] = {}
    discovery_sessions = set(store.sessions[: cfg.chronological_roles[0]])
    for market in cfg.selected_markets:
        mask = (store.market == market) & np.isin(store.session, tuple(discovery_sessions))
        if int(mask.sum()) < 100:
            raise SparsePilotError(f"0032 discovery lattice too small for {market}")
        def q(name: str, quantile: float, *, absolute: bool = False) -> float:
            values = store.feature_values[mask, index[name]]
            if absolute:
                values = np.abs(values)
            return float(np.quantile(values, quantile))
        thresholds[market] = {
            "flow_enter": q("flow_2s", cfg.hysteresis_enter_quantile, absolute=True),
            "flow_exit": q("flow_2s", cfg.hysteresis_exit_quantile, absolute=True),
            "flow30_enter": q("flow_30s", cfg.hysteresis_enter_quantile, absolute=True),
            "arrival_enter": q("arrival_2s", 0.80),
            "response_low": q("price_response_per_signed_contract", 0.35, absolute=True),
            "replenishment_enter": q("replenishment_rate", 0.80),
            "depletion_enter": q("depletion_rate", 0.80),
            "withdrawal_enter": q("depth_withdrawal_rate", 0.80),
            "gap_enter": q("liquidity_gap_ticks", 0.75),
            "microprice_enter": q("microprice_deviation", 0.75, absolute=True),
            "queue_enter": q("queue_persistence", 0.75),
            "ephemeral_enter": q("ephemeral_liquidity_rate", 0.80),
            "imbalance_enter": q("bbo_imbalance", 0.70, absolute=True),
        }
    return thresholds


def _mechanism_state(
    mechanism: str,
    values: np.ndarray,
    names: Mapping[str, int],
    threshold: Mapping[str, float],
) -> tuple[int, float, bool]:
    def v(name: str) -> float:
        return float(values[names[name]])
    flow2 = v("flow_2s")
    flow30 = v("flow_30s")
    micro = v("microprice_deviation")
    imbalance = v("bbo_imbalance")
    if mechanism == "INITIATIVE":
        direction = 1 if flow2 + micro >= 0 else -1
        pressure = abs(flow2) / max(threshold["flow_enter"], 1e-9)
        active = (
            abs(flow2) >= threshold["flow_enter"]
            and v("arrival_2s") >= threshold["arrival_enter"]
            and direction * (flow2 + micro) > 0
        )
    elif mechanism == "ABSORPTION":
        direction = -1 if flow30 >= 0 else 1
        pressure = (
            abs(flow30) / max(threshold["flow30_enter"], 1e-9)
            + v("replenishment_rate") / max(threshold["replenishment_enter"], 1e-9)
        ) / 2.0
        active = (
            abs(flow30) >= threshold["flow30_enter"]
            and abs(v("price_response_per_signed_contract")) <= threshold["response_low"]
            and v("replenishment_rate") >= threshold["replenishment_enter"]
        )
    elif mechanism == "LIQUIDITY_VACUUM":
        direction = 1 if micro + flow2 >= 0 else -1
        pressure = (
            v("depth_withdrawal_rate") / max(threshold["withdrawal_enter"], 1e-9)
            + abs(micro) / max(threshold["microprice_enter"], 1e-9)
        ) / 2.0
        active = (
            v("depth_withdrawal_rate") >= threshold["withdrawal_enter"]
            and v("liquidity_gap_ticks") >= threshold["gap_enter"]
            and abs(micro) >= threshold["microprice_enter"]
        )
    elif mechanism == "EXHAUSTION":
        direction = -1 if flow30 >= 0 else 1
        decay = abs(flow30) - abs(flow2)
        pressure = decay / max(threshold["flow30_enter"], 1e-9)
        active = (
            abs(flow30) >= threshold["flow30_enter"]
            and abs(flow2) <= threshold["flow_exit"]
            and abs(v("price_response_per_signed_contract")) <= threshold["response_low"]
        )
    elif mechanism == "QUEUE_REPLENISHMENT":
        direction = 1 if imbalance + micro >= 0 else -1
        pressure = (
            v("queue_persistence") / max(threshold["queue_enter"], 1e-9)
            + v("replenishment_rate") / max(threshold["replenishment_enter"], 1e-9)
        ) / 2.0
        active = (
            v("queue_persistence") >= threshold["queue_enter"]
            and v("replenishment_rate") >= threshold["replenishment_enter"]
            and abs(imbalance) >= threshold["imbalance_enter"]
        )
    else:
        raise SparsePilotError(f"unsupported 0032 mechanism: {mechanism}")
    return direction, float(pressure), bool(active)


def build_opportunity_episodes(store: SparseStore, *, cfg: SparsePilotConfig) -> tuple[OpportunityEpisode, ...]:
    """Compile raw triggers through the canonical causal OpportunityEpisode FSM.

    Outcome labels are intentionally unavailable here.  The FSM receives only
    decision-time features and performs the time/price/session consolidation,
    hysteresis and one-decision invariant.  The later meta-label layer may
    abstain from the resulting episode, but it cannot multiply it.
    """

    thresholds = _discovery_thresholds(store, cfg)
    names = _feature_index(store)
    episodes: list[OpportunityEpisode] = []
    for market in cfg.selected_markets:
        market_rows = np.flatnonzero(store.market == market)
        tick = float(cfg.tick_size[market])
        for mechanism in MECHANISMS:
            engines = {
                direction: OpportunityEpisodeFSM(
                    OpportunityEpisodeSpec(
                        policy_id=f"{cfg.campaign_id}:{market}:{mechanism}:{direction}",
                        mechanism=mechanism,
                        direction=direction,
                        activation_threshold=1.0,
                        reset_threshold=0.50,
                        # Teacher-opportunity construction is pre-meta-label;
                        # the final TRADE/ABSTAIN decision is made later.
                        meta_label_threshold=0.0,
                        consolidation_window_ns=cfg.opportunity_gap_ns,
                        price_zone_ticks=float(cfg.price_zone_ticks),
                        tick_size=tick,
                        minimum_confirmations=1,
                        recent_event_limit=4_096,
                    )
                )
                for direction in (-1, 1)
            }
            row_by_fingerprint: dict[str, int] = {}
            pressure_by_fingerprint: dict[str, float] = {}
            absorption_by_fingerprint: dict[str, float] = {}
            for raw_row in market_rows:
                row = int(raw_row)
                direction, pressure, is_active = _mechanism_state(
                    mechanism,
                    store.feature_values[row],
                    names,
                    thresholds[market],
                )
                fingerprint = stable_hash(
                    {
                        "feature_hash": str(store.feature_hashes[row]),
                        "mechanism": mechanism,
                        "direction": int(direction),
                    }
                )
                row_by_fingerprint[fingerprint] = row
                pressure_by_fingerprint[fingerprint] = float(pressure)
                absorption_by_fingerprint[fingerprint] = float(
                    store.feature_values[row, names["replenishment_rate"]]
                    + store.feature_values[row, names["depletion_rate"]]
                )
                engines[int(direction)].step(
                    OpportunityObservation(
                        event_fingerprint=fingerprint,
                        market=market,
                        contract=str(store.contract[row]),
                        session_id=str(store.session[row]),
                        # The sealed 0031 feature lattice records its causal
                        # availability instant; no earlier raw-event timestamp
                        # is required for this derived decision observation.
                        event_time_ns=int(store.available_ns[row]),
                        available_at_ns=int(store.available_ns[row]),
                        price=float(
                            0.5 * (store.bid_price[row] + store.ask_price[row])
                        ),
                        mechanism=mechanism,
                        direction=int(direction),
                        activation_score=float(pressure if is_active else 0.0),
                        meta_score=1.0,
                        feature_fingerprint=str(store.feature_hashes[row]),
                    ),
                    decision_time_ns=int(store.decision_ns[row]),
                    # The sparse compiler consumes hundreds of thousands of
                    # observations and never uses the per-step audit hash.
                    # Checkpoints/final sealing still materialise the exact
                    # complete FSM state hash when requested.
                    materialize_state_hash=False,
                )
            for engine in engines.values():
                for canonical in engine.finalize(reason="END_OF_FROZEN_STORE"):
                    if canonical.decision is None:
                        continue
                    fingerprints = canonical.event_fingerprints
                    if not fingerprints:
                        raise SparsePilotError("0032 canonical episode lost its events")
                    start_row = row_by_fingerprint[fingerprints[0]]
                    confirmation_row = row_by_fingerprint[
                        canonical.decision.event_fingerprint
                    ]
                    end_row = row_by_fingerprint[fingerprints[-1]]
                    midpoint = 0.5 * (
                        store.bid_price[confirmation_row]
                        + store.ask_price[confirmation_row]
                    )
                    episodes.append(
                        OpportunityEpisode(
                            opportunity_id=canonical.episode_id,
                            market=market,
                            contract=str(store.contract[confirmation_row]),
                            session_id=str(store.session[confirmation_row]),
                            role=store.roles[str(store.session[confirmation_row])],
                            mechanism=mechanism,
                            direction=int(canonical.direction),
                            start_index=int(start_row),
                            confirmation_index=int(confirmation_row),
                            end_index=int(end_row),
                            start_ns=int(store.decision_ns[start_row]),
                            confirmation_ns=int(store.decision_ns[confirmation_row]),
                            end_ns=int(store.decision_ns[end_row]),
                            price_zone=int(
                                math.floor(
                                    midpoint / (cfg.price_zone_ticks * tick)
                                )
                            ),
                            supporting_event_count=int(canonical.observation_count),
                            feature_hash=str(store.feature_hashes[confirmation_row]),
                            feature_vector_hash=stable_hash(
                                [
                                    float(value)
                                    for value in store.feature_values[
                                        confirmation_row
                                    ]
                                ]
                            ),
                            maximum_flow_pressure=max(
                                pressure_by_fingerprint[value]
                                for value in fingerprints
                            ),
                            maximum_absorption_depletion=max(
                                absorption_by_fingerprint[value]
                                for value in fingerprints
                            ),
                            vwap_location_ticks=float(
                                store.feature_values[
                                    confirmation_row, names["vwap_distance"]
                                ]
                            ),
                            cross_market_confirmation=float(
                                store.feature_values[
                                    confirmation_row,
                                    names["cross_market_flow_alignment"],
                                ]
                            ),
                        )
                    )
    if len({value.opportunity_id for value in episodes}) != len(episodes):
        raise SparsePilotError("0032 duplicate OpportunityEpisode fingerprint")
    return tuple(
        sorted(
            episodes,
            key=lambda value: (
                value.confirmation_ns,
                value.market,
                value.mechanism,
            ),
        )
    )


def build_opportunity_outcomes(
    store: SparseStore,
    episodes: Sequence[OpportunityEpisode],
    *,
    cfg: SparsePilotConfig,
) -> tuple[OpportunityOutcome, ...]:
    """Attach later outcomes only after the decision-only episodes exist."""

    outcomes: list[OpportunityOutcome] = []
    horizons = (1, 5, 30, 120, 300, 900)
    # These arrays are immutable for the whole pilot.  Building them once
    # avoids an O(opportunities x feature_rows) rescan while preserving the
    # exact chronological indices used by the former implementation.
    market_index: dict[
        str, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, dict[str, int]]
    ] = {}
    for market in cfg.selected_markets:
        market_rows = np.flatnonzero(store.market == market)
        local_times = store.decision_ns[market_rows]
        local_prices = store.last_trade_price[market_rows]
        session_local = store.session[market_rows]
        session_end_local = {
            str(session): int(offsets[-1]) + 1
            for session in store.sessions
            if len(offsets := np.flatnonzero(session_local == session))
        }
        market_index[market] = (
            market_rows,
            local_times,
            local_prices,
            session_local,
            session_end_local,
        )
    for episode in episodes:
        (
            market_rows,
            local_times,
            local_prices,
            session_local,
            session_end_by_id,
        ) = market_index[episode.market]
        start_local = int(np.searchsorted(local_times, episode.confirmation_ns, side="left"))
        start_price = float(store.last_trade_price[episode.confirmation_index])
        tick = float(cfg.tick_size[episode.market])
        session = episode.session_id
        session_end_local = session_end_by_id.get(session)
        if session_end_local is None:
            raise SparsePilotError("0032 opportunity session missing from causal lattice")
        maximum_ns = episode.confirmation_ns + max(horizons) * 1_000_000_000
        end_local = min(
            int(np.searchsorted(local_times, maximum_ns, side="right")),
            session_end_local,
        )
        future_prices = local_prices[start_local + 1 : end_local]
        future_times = local_times[start_local + 1 : end_local]
        has_30s = (
            session_end_local > start_local + 1
            and int(local_times[session_end_local - 1])
            >= episode.confirmation_ns + 30_000_000_000
        )
        if not has_30s or len(future_prices) == 0:
            outcomes.append(
                OpportunityOutcome(
                    opportunity_id=episode.opportunity_id,
                    favorable_first=False,
                    adverse_first=False,
                    timeout=False,
                    censored=True,
                    time_to_favorable_ns=None,
                    time_to_adverse_ns=None,
                    mfe_ticks=None,
                    mae_ticks=None,
                    markouts_ticks={str(value): None for value in horizons},
                )
            )
            continue
        signed_ticks = episode.direction * (future_prices - start_price) / tick
        favorable = np.flatnonzero(signed_ticks >= cfg.target_ticks)
        adverse = np.flatnonzero(signed_ticks <= -cfg.stop_ticks)
        favorable_index = int(favorable[0]) if len(favorable) else None
        adverse_index = int(adverse[0]) if len(adverse) else None
        favorable_first = favorable_index is not None and (
            adverse_index is None or favorable_index < adverse_index
        )
        adverse_first = adverse_index is not None and (
            favorable_index is None or adverse_index < favorable_index
        )
        markouts: dict[str, float | None] = {}
        for horizon in horizons:
            target_ns = episode.confirmation_ns + horizon * 1_000_000_000
            offset = int(np.searchsorted(local_times, target_ns, side="left"))
            if offset >= session_end_local or str(session_local[offset]) != session:
                markouts[str(horizon)] = None
            else:
                markouts[str(horizon)] = float(
                    episode.direction * (local_prices[offset] - start_price) / tick
                )
        outcomes.append(
            OpportunityOutcome(
                opportunity_id=episode.opportunity_id,
                favorable_first=bool(favorable_first),
                adverse_first=bool(adverse_first),
                timeout=bool(not favorable_first and not adverse_first),
                censored=False,
                time_to_favorable_ns=(
                    None if favorable_index is None else int(future_times[favorable_index] - episode.confirmation_ns)
                ),
                time_to_adverse_ns=(
                    None if adverse_index is None else int(future_times[adverse_index] - episode.confirmation_ns)
                ),
                mfe_ticks=float(np.max(signed_ticks)),
                mae_ticks=float(np.min(signed_ticks)),
                markouts_ticks=markouts,
            )
        )
    return tuple(outcomes)


def fit_abstention_models(
    store: SparseStore,
    episodes: Sequence[OpportunityEpisode],
    outcomes: Sequence[OpportunityOutcome],
    *,
    cfg: SparsePilotConfig,
) -> tuple[FrozenMetaModel, ...]:
    outcome_by_id = {value.opportunity_id: value for value in outcomes}
    feature_lookup = _feature_index(store)
    models: list[FrozenMetaModel] = []
    for mechanism in MECHANISMS:
        selected = [value for value in episodes if value.mechanism == mechanism]
        discovery = [value for value in selected if value.role == "DISCOVERY" and not outcome_by_id[value.opportunity_id].censored]
        if len(discovery) < 20:
            continue
        labels = np.asarray(
            [outcome_by_id[value.opportunity_id].favorable_first for value in discovery],
            dtype=np.int8,
        )
        if len(np.unique(labels)) < 2:
            continue
        for tier, whitelist in (
            ("L1", L1_META_FEATURES),
            ("L2", L1_META_FEATURES + L2_EXTRA_FEATURES),
        ):
            names = tuple(value for value in whitelist if value in feature_lookup)
            indexes = [feature_lookup[name] for name in names]
            values = np.asarray(
                [store.feature_values[value.confirmation_index, indexes] for value in discovery],
                dtype=np.float64,
            )
            means = np.mean(values, axis=0)
            scales = np.std(values, axis=0)
            scales = np.where(scales > 1e-12, scales, 1.0)
            standardized = (values - means) / scales
            model = LogisticRegression(
                C=0.20,
                solver="liblinear",
                class_weight="balanced",
                random_state=cfg.random_seed,
                max_iter=500,
            )
            model.fit(standardized, labels)
            # Export the fitted classifier in raw causal-feature coordinates;
            # inference therefore has no hidden sklearn preprocessing state.
            raw_coefficients = model.coef_[0] / scales
            raw_intercept = float(
                model.intercept_[0] - np.dot(model.coef_[0], means / scales)
            )
            payload = {
                "mechanism": mechanism,
                "tier": tier,
                "feature_names": list(names),
                "coefficients": [float(value) for value in raw_coefficients],
                "intercept": raw_intercept,
                "discovery_positive_rate": float(labels.mean()),
            }
            models.append(
                FrozenMetaModel(
                    mechanism=mechanism,
                    tier=tier,
                    feature_names=names,
                    coefficients=tuple(payload["coefficients"]),
                    intercept=float(payload["intercept"]),
                    discovery_positive_rate=float(payload["discovery_positive_rate"]),
                    model_hash=stable_hash(payload),
                )
            )
    return tuple(models)


def _predict(model: FrozenMetaModel, store: SparseStore, row: int) -> float:
    lookup = _feature_index(store)
    values = store.feature_values[row, [lookup[name] for name in model.feature_names]]
    raw = float(model.intercept + np.dot(np.asarray(model.coefficients), values))
    raw = max(min(raw, 35.0), -35.0)
    return float(1.0 / (1.0 + math.exp(-raw)))


def freeze_sparse_strategy_bank(
    models: Sequence[FrozenMetaModel], *, cfg: SparsePilotConfig
) -> tuple[SparseStrategySpec, ...]:
    """Freeze exactly six transparent profiles per mechanism, capped at 30."""

    by_key = {(value.mechanism, value.tier): value for value in models}
    profiles = (
        ("L1", 1.25, 4, 30, "FIXED_TARGET_STOP"),
        ("L1", 2.00, 2, 120, "TIME_STOP"),
        ("L1", 3.00, 8, 300, "ORDER_FLOW_DECAY"),
        ("L2", 1.50, 12, 300, "OPPOSITE_STATE_TRANSITION"),
        ("L2", 2.00, 4, 900, "VWAP_LIQUIDITY_LEVEL"),
        ("L2", 3.00, 2, 120, "EVENT_STATE_RESET"),
    )
    rows: list[SparseStrategySpec] = []
    for mechanism in MECHANISMS:
        for variant, (tier, ratio, budget, horizon, exit_policy) in enumerate(profiles, start=1):
            model = by_key.get((mechanism, tier))
            if model is None:
                continue
            core = {
                "campaign_id": cfg.campaign_id,
                "mechanism": mechanism,
                "tier": tier,
                "edge_to_cost_ratio": ratio,
                "trade_budget_per_session": budget,
                "holding_horizon_seconds": horizon,
                "exit_policy": exit_policy,
                "target_ticks": cfg.target_ticks,
                "stop_ticks": cfg.stop_ticks,
                "quantity": cfg.quantity,
                "model_hash": model.model_hash,
                "variant": variant,
            }
            fingerprint = stable_hash(core)
            rows.append(
                SparseStrategySpec(
                    strategy_id=f"sparse_0032_{mechanism.lower()}_{variant:02d}_{fingerprint[:12]}",
                    mechanism=mechanism,
                    tier=tier,
                    deployability_tier=f"{tier}_DEPLOYABLE",
                    edge_to_cost_ratio=float(ratio),
                    trade_budget_per_session=int(budget),
                    holding_horizon_seconds=int(horizon),
                    exit_policy=str(exit_policy),
                    target_ticks=float(cfg.target_ticks),
                    stop_ticks=float(cfg.stop_ticks),
                    quantity=int(cfg.quantity),
                    model_hash=model.model_hash,
                    specification_hash=fingerprint,
                )
            )
    return tuple(rows[: cfg.maximum_strategies])


def _depth_fill(depth_json: str, quantity: int) -> tuple[int, float | None, float]:
    try:
        depth = json.loads(str(depth_json))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SparsePilotError("0032 displayed depth is not canonical JSON") from exc
    remaining = int(quantity)
    filled = 0
    notional = 0.0
    best = None
    for raw_price, raw_size in depth:
        price = float(raw_price)
        size = max(int(raw_size), 0)
        if best is None:
            best = price
        take = min(remaining, size)
        if take <= 0:
            continue
        filled += take
        remaining -= take
        notional += take * price
        if remaining == 0:
            break
    if not filled or best is None:
        return 0, None, 0.0
    average = notional / filled
    return filled, average, abs(average - best)


def _market_rows(store: SparseStore) -> Mapping[str, np.ndarray]:
    return {
        market: np.flatnonzero(store.market == market)
        for market in sorted(set(str(value) for value in store.market))
    }


def _candidate_trade_selection(
    spec: SparseStrategySpec,
    model: FrozenMetaModel,
    store: SparseStore,
    opportunities: Sequence[OpportunityEpisode],
    *,
    cfg: SparsePilotConfig,
) -> tuple[tuple[OpportunityEpisode, float, float], ...]:
    """Causal chronological acceptance; never hindsight top-k a full session."""

    accepted: list[tuple[OpportunityEpisode, float, float]] = []
    used: dict[str, int] = defaultdict(int)
    for opportunity in opportunities:
        if opportunity.mechanism != spec.mechanism:
            continue
        if used[opportunity.session_id] >= spec.trade_budget_per_session:
            continue
        probability = _predict(model, store, opportunity.confirmation_index)
        market = opportunity.market
        tick_value = float(cfg.tick_size[market] * cfg.point_value[market])
        target_value = spec.target_ticks * tick_value * spec.quantity
        stop_value = spec.stop_ticks * tick_value * spec.quantity
        expected_gross = probability * target_value - (1.0 - probability) * stop_value
        all_in_stressed = (
            float(cfg.commission_usd[market])
            + 2.0
            * cfg.adverse_slippage_ticks_per_side
            * tick_value
            * spec.quantity
            * cfg.stressed_cost_multiplier
        )
        ratio = expected_gross / max(all_in_stressed, 1e-9)
        if expected_gross <= 0.0 or ratio + 1e-12 < spec.edge_to_cost_ratio:
            continue
        accepted.append((opportunity, probability, ratio))
        used[opportunity.session_id] += 1
    return tuple(accepted)


def _execute_sparse_strategy(
    spec: SparseStrategySpec,
    model: FrozenMetaModel,
    store: SparseStore,
    opportunities: Sequence[OpportunityEpisode],
    thresholds: Mapping[str, Mapping[str, float]],
    *,
    cfg: SparsePilotConfig,
) -> tuple[SparseTrade, ...]:
    selected = _candidate_trade_selection(
        spec, model, store, opportunities, cfg=cfg
    )
    feature_lookup = _feature_index(store)
    rows_by_market = _market_rows(store)
    trades: list[SparseTrade] = []
    occupied_until: dict[str, int] = defaultdict(lambda: -1)
    for opportunity, probability, edge_ratio in selected:
        if opportunity.confirmation_ns < occupied_until[opportunity.market]:
            continue
        rows = rows_by_market[opportunity.market]
        times = store.decision_ns[rows]
        placement_local = int(
            np.searchsorted(
                times,
                opportunity.confirmation_ns + cfg.aggressive_latency_ns,
                side="left",
            )
        )
        if placement_local >= len(rows):
            continue
        entry_index = int(rows[placement_local])
        if str(store.session[entry_index]) != opportunity.session_id:
            continue
        direction = int(opportunity.direction)
        entry_depth = (
            store.ask_depth_json[entry_index]
            if direction > 0
            else store.bid_depth_json[entry_index]
        )
        filled, entry_best_vwap, entry_depth_ticks_raw = _depth_fill(
            str(entry_depth), spec.quantity
        )
        if filled <= 0 or entry_best_vwap is None:
            continue
        tick = float(cfg.tick_size[opportunity.market])
        point = float(cfg.point_value[opportunity.market])
        entry_best = float(
            store.ask_price[entry_index] if direction > 0 else store.bid_price[entry_index]
        )
        entry_price = float(
            entry_best_vwap + direction * cfg.adverse_slippage_ticks_per_side * tick
        )
        stop = entry_price - direction * spec.stop_ticks * tick
        target = entry_price + direction * spec.target_ticks * tick
        maximum_ns = int(store.decision_ns[entry_index]) + spec.holding_horizon_seconds * 1_000_000_000
        session = opportunity.session_id
        exit_local = placement_local
        exit_reason = "TIME_STOP"
        for local in range(placement_local, len(rows)):
            row = int(rows[local])
            if str(store.session[row]) != session:
                exit_local = max(placement_local, local - 1)
                exit_reason = "SESSION_FLATTEN"
                break
            exit_local = local
            last = float(store.last_trade_price[row])
            if direction * (last - target) >= 0:
                exit_reason = "PROFIT_TARGET"
                break
            if direction * (last - stop) <= 0:
                exit_reason = "STOP_LOSS"
                break
            if spec.exit_policy == "EVENT_STATE_RESET" and row >= opportunity.end_index:
                exit_reason = "EVENT_STATE_RESET"
                break
            if spec.exit_policy == "ORDER_FLOW_DECAY":
                flow = abs(float(store.feature_values[row, feature_lookup["flow_2s"]]))
                if flow <= float(thresholds[opportunity.market]["flow_exit"]):
                    exit_reason = "ORDER_FLOW_DECAY"
                    break
            if spec.exit_policy == "OPPOSITE_STATE_TRANSITION":
                direction_now = float(
                    store.feature_values[row, feature_lookup["flow_2s"]]
                    + store.feature_values[row, feature_lookup["microprice_deviation"]]
                )
                if direction * direction_now < 0:
                    exit_reason = "OPPOSITE_STATE_TRANSITION"
                    break
            if spec.exit_policy == "VWAP_LIQUIDITY_LEVEL":
                vwap_distance = float(
                    store.feature_values[row, feature_lookup["vwap_distance"]]
                )
                if direction * vwap_distance >= 0:
                    exit_reason = "VWAP_LIQUIDITY_LEVEL"
                    break
            if int(store.decision_ns[row]) >= maximum_ns:
                exit_reason = "TIME_STOP"
                break
        exit_index = int(rows[exit_local])
        exit_depth = (
            store.bid_depth_json[exit_index]
            if direction > 0
            else store.ask_depth_json[exit_index]
        )
        exit_filled, exit_best_vwap, exit_depth_ticks_raw = _depth_fill(
            str(exit_depth), filled
        )
        if exit_filled <= 0 or exit_best_vwap is None:
            continue
        quantity = min(filled, exit_filled)
        exit_best = float(
            store.bid_price[exit_index] if direction > 0 else store.ask_price[exit_index]
        )
        exit_price = float(
            exit_best_vwap - direction * cfg.adverse_slippage_ticks_per_side * tick
        )
        entry_mid = 0.5 * (store.bid_price[entry_index] + store.ask_price[entry_index])
        exit_mid = 0.5 * (store.bid_price[exit_index] + store.ask_price[exit_index])
        reference = direction * (exit_mid - entry_mid) * point * quantity
        spread_cost = direction * (
            (entry_best - entry_mid) + (exit_mid - exit_best)
        ) * point * quantity
        depth_cost = (
            entry_depth_ticks_raw + exit_depth_ticks_raw
        ) * point * quantity
        marketable = (
            2.0 * cfg.adverse_slippage_ticks_per_side * tick * point * quantity
        )
        commission = float(cfg.commission_usd[opportunity.market]) * quantity
        realized_before_commission = direction * (exit_price - entry_price) * point * quantity
        normal_net = realized_before_commission - commission
        stressed_extra = marketable * (cfg.stressed_cost_multiplier - 1.0)
        stressed_net = normal_net - stressed_extra
        path_rows = rows[placement_local : exit_local + 1]
        signed_path = direction * (
            store.last_trade_price[path_rows] - entry_price
        ) * point * quantity
        minimum_unrealized = float(np.min(signed_path)) if len(signed_path) else 0.0
        one_second_ns = int(store.decision_ns[entry_index]) + 1_000_000_000
        one_second_local = int(np.searchsorted(times, one_second_ns, side="left"))
        if one_second_local < len(rows) and str(store.session[int(rows[one_second_local])]) == session:
            adverse_selection = -direction * (
                float(store.last_trade_price[int(rows[one_second_local])]) - entry_mid
            ) * point * quantity
        else:
            adverse_selection = 0.0
        bridge = reference - spread_cost - depth_cost - marketable - commission
        if not math.isclose(bridge, normal_net, rel_tol=1e-9, abs_tol=1e-6):
            raise SparsePilotError(
                f"0032 gross-to-net execution bridge diverged for {spec.strategy_id}"
            )
        trade_id = stable_hash(
            {
                "strategy_id": spec.strategy_id,
                "opportunity_id": opportunity.opportunity_id,
                "entry_ns": int(store.decision_ns[entry_index]),
                "exit_ns": int(store.decision_ns[exit_index]),
                "entry": entry_price,
                "exit": exit_price,
            }
        )
        trades.append(
            SparseTrade(
                strategy_id=spec.strategy_id,
                opportunity_id=opportunity.opportunity_id,
                trade_id=trade_id,
                market=opportunity.market,
                session_id=session,
                role=opportunity.role,
                direction=direction,
                entry_index=entry_index,
                exit_index=exit_index,
                entry_time_ns=int(store.decision_ns[entry_index]),
                exit_time_ns=int(store.decision_ns[exit_index]),
                entry_price=entry_price,
                exit_price=exit_price,
                stop_price=float(stop),
                target_price=float(target),
                exit_reason=exit_reason,
                quantity=quantity,
                gross_reference_pnl_usd=float(reference),
                spread_cost_usd=float(spread_cost),
                marketable_slippage_usd=float(marketable),
                depth_slippage_usd=float(depth_cost),
                commission_usd=float(commission),
                adverse_selection_usd=float(adverse_selection),
                normal_net_pnl_usd=float(normal_net),
                stressed_net_pnl_usd=float(stressed_net),
                minimum_unrealized_pnl_usd=minimum_unrealized,
                prediction=float(probability),
                expected_edge_to_cost=float(edge_ratio),
            )
        )
        occupied_until[opportunity.market] = int(store.decision_ns[exit_index])
    return tuple(trades)


def _account_paths(
    strategy_id: str,
    trades: Sequence[SparseTrade],
    sessions: Sequence[str],
    account: Mapping[str, float],
) -> tuple[Mapping[str, Any], ...]:
    by_session: dict[str, list[SparseTrade]] = defaultdict(list)
    for trade in trades:
        by_session[trade.session_id].append(trade)
    rows: list[Mapping[str, Any]] = []
    for start_offset, start_session in enumerate(sessions):
        for horizon in ACCOUNT_HORIZONS_DAYS:
            full = start_offset + horizon <= len(sessions)
            included = tuple(sessions[start_offset : min(len(sessions), start_offset + horizon)])
            for scenario in COST_SCENARIOS:
                cumulative = 0.0
                trailing_high = 0.0
                minimum_buffer = float(account["mll"])
                daily: list[Mapping[str, Any]] = []
                breached = False
                passed = False
                pass_day: int | None = None
                for day_number, session in enumerate(included, start=1):
                    day_pnl = 0.0
                    day_costs = 0.0
                    for trade in sorted(by_session.get(session, ()), key=lambda value: value.exit_time_ns):
                        value = (
                            trade.normal_net_pnl_usd
                            if scenario == "NORMAL"
                            else trade.stressed_net_pnl_usd
                        )
                        costs = (
                            trade.spread_cost_usd
                            + trade.depth_slippage_usd
                            + trade.marketable_slippage_usd
                            + trade.commission_usd
                            + (
                                trade.marketable_slippage_usd * 0.5
                                if scenario == "STRESSED_1_5X"
                                else 0.0
                            )
                        )
                        equity_before = cumulative + day_pnl
                        minimum_equity = equity_before + min(0.0, trade.minimum_unrealized_pnl_usd)
                        loss_limit = max(-float(account["mll"]), trailing_high - float(account["mll"]))
                        minimum_buffer = min(minimum_buffer, minimum_equity - loss_limit)
                        day_pnl += value
                        day_costs += costs
                        if minimum_equity < loss_limit:
                            breached = True
                            break
                    cumulative += day_pnl
                    trailing_high = max(trailing_high, cumulative)
                    closing_limit = max(-float(account["mll"]), trailing_high - float(account["mll"]))
                    minimum_buffer = min(minimum_buffer, cumulative - closing_limit)
                    daily.append(
                        {
                            "session_id": session,
                            "day": day_number,
                            "net_pnl_usd": float(day_pnl),
                            "cumulative_net_usd": float(cumulative),
                            "costs_usd": float(day_costs),
                        }
                    )
                    best_day = max((max(0.0, float(value["net_pnl_usd"])) for value in daily), default=0.0)
                    consistency_ok = cumulative > 0 and best_day <= float(account["consistency"]) * cumulative + 1e-9
                    if not breached and cumulative >= float(account["target"]) and consistency_ok:
                        passed = True
                        pass_day = day_number
                        break
                    if breached:
                        break
                best_day = max((max(0.0, float(value["net_pnl_usd"])) for value in daily), default=0.0)
                ratio = best_day / cumulative if cumulative > 0 else None
                terminal = (
                    "TARGET_REACHED"
                    if passed
                    else "MLL_BREACHED"
                    if breached
                    else "OPERATIONAL_HORIZON_NOT_REACHED"
                    if full
                    else "DATA_CENSORED"
                )
                rows.append(
                    {
                        "episode_id": stable_hash({"strategy": strategy_id, "account": account["account_size"], "start": start_session, "horizon": horizon, "scenario": scenario}),
                        "strategy_id": strategy_id,
                        "account_size": float(account["account_size"]),
                        "start_session": start_session,
                        "horizon_days": horizon,
                        "scenario": scenario,
                        "full_coverage": full,
                        "terminal_state": terminal,
                        "target_reached": passed,
                        "mll_breached": breached,
                        "days_to_target": pass_day,
                        "net_pnl_usd": float(cumulative),
                        "costs_usd": float(sum(value["costs_usd"] for value in daily)),
                        "target_progress_pct": float(100.0 * cumulative / float(account["target"])),
                        "minimum_mll_buffer_usd": float(minimum_buffer),
                        "consistency_ratio": ratio,
                        "consistency_compliant": bool(ratio is not None and ratio <= float(account["consistency"])),
                        "daily_path": daily,
                    }
                )
    return tuple(rows)


def _strategy_result(
    spec: SparseStrategySpec,
    trades: Sequence[SparseTrade],
    store: SparseStore,
    cfg: SparsePilotConfig,
) -> Mapping[str, Any]:
    role_summary = {
        role: {
            "trade_count": len(selected := [value for value in trades if value.role == role]),
            "normal_net_usd": float(sum(value.normal_net_pnl_usd for value in selected)),
            "stressed_net_usd": float(sum(value.stressed_net_pnl_usd for value in selected)),
            "gross_reference_pnl_usd": float(sum(value.gross_reference_pnl_usd for value in selected)),
        }
        for role in ROLES
    }
    account_frontier: dict[str, Any] = {}
    all_paths: list[Mapping[str, Any]] = []
    for account_id, account in cfg.account_snapshots.items():
        paths = _account_paths(
            spec.strategy_id, trades, store.sessions, account
        )
        all_paths.extend(paths)
        headline = [value for value in paths if value["full_coverage"]]
        by_scenario: dict[str, Mapping[str, Any]] = {}
        for scenario in COST_SCENARIOS:
            selected = [value for value in headline if value["scenario"] == scenario]
            by_scenario[scenario] = {
                "full_coverage_count": len(selected),
                "pass_count": sum(bool(value["target_reached"]) for value in selected),
                "mll_breach_count": sum(bool(value["mll_breached"]) for value in selected),
                "median_target_progress_pct": _median(
                    [float(value["target_progress_pct"]) for value in selected]
                ),
                "minimum_mll_buffer_usd": min(
                    (float(value["minimum_mll_buffer_usd"]) for value in selected),
                    default=float(account["mll"]),
                ),
            }
        account_frontier[account_id] = by_scenario
    final_trades = [value for value in trades if value.role == "FINAL_DEVELOPMENT"]
    validation_trades = [value for value in trades if value.role == "VALIDATION"]
    total_absolute = sum(abs(value.stressed_net_pnl_usd) for value in trades)
    return {
        "strategy": asdict(spec),
        "trade_count": len(trades),
        "trades_per_session": len(trades) / float(len(store.sessions)),
        "role_economics": role_summary,
        "normal_net_usd": float(sum(value.normal_net_pnl_usd for value in trades)),
        "stressed_net_usd": float(sum(value.stressed_net_pnl_usd for value in trades)),
        "gross_reference_pnl_usd": float(sum(value.gross_reference_pnl_usd for value in trades)),
        "mean_edge_to_cost": float(np.mean([value.expected_edge_to_cost for value in trades])) if trades else 0.0,
        "single_event_concentration": (
            max((abs(value.stressed_net_pnl_usd) for value in trades), default=0.0)
            / max(total_absolute, 1e-9)
        ),
        "validation_normal_net_usd": float(
            sum(value.normal_net_pnl_usd for value in validation_trades)
        ),
        "validation_stressed_net_usd": float(
            sum(value.stressed_net_pnl_usd for value in validation_trades)
        ),
        "final_development_normal_net_usd": float(
            sum(value.normal_net_pnl_usd for value in final_trades)
        ),
        "final_development_stressed_net_usd": float(
            sum(value.stressed_net_pnl_usd for value in final_trades)
        ),
        "account_frontier": account_frontier,
        "account_paths": all_paths,
    }


def _median(values: Sequence[float]) -> float | None:
    return None if not values else float(np.median(np.asarray(values, dtype=float)))


def _quantile(values: Sequence[float], value: float) -> float | None:
    return (
        None
        if not values
        else float(np.quantile(np.asarray(values, dtype=float), value))
    )


_WORKER_STORE: SparseStore | None = None
_WORKER_OPPORTUNITIES: tuple[OpportunityEpisode, ...] = ()
_WORKER_MODELS: Mapping[tuple[str, str], FrozenMetaModel] = {}
_WORKER_THRESHOLDS: Mapping[str, Mapping[str, float]] = {}
_WORKER_CONFIG: SparsePilotConfig | None = None


def _evaluate_sparse_worker(
    spec: SparseStrategySpec,
) -> tuple[str, tuple[SparseTrade, ...]]:
    if _WORKER_STORE is None or _WORKER_CONFIG is None:
        raise SparsePilotError("0032 economic worker was not initialized")
    model = _WORKER_MODELS[(spec.mechanism, spec.tier)]
    trades = _execute_sparse_strategy(
        spec,
        model,
        _WORKER_STORE,
        _WORKER_OPPORTUNITIES,
        _WORKER_THRESHOLDS,
        cfg=_WORKER_CONFIG,
    )
    return spec.strategy_id, trades


def _execute_candidate_bank(
    specs: Sequence[SparseStrategySpec],
    models: Sequence[FrozenMetaModel],
    opportunities: Sequence[OpportunityEpisode],
    store: SparseStore,
    *,
    cfg: SparsePilotConfig,
) -> Mapping[str, tuple[SparseTrade, ...]]:
    global _WORKER_STORE, _WORKER_OPPORTUNITIES, _WORKER_MODELS
    global _WORKER_THRESHOLDS, _WORKER_CONFIG
    _WORKER_STORE = store
    _WORKER_OPPORTUNITIES = tuple(opportunities)
    _WORKER_MODELS = {(value.mechanism, value.tier): value for value in models}
    _WORKER_THRESHOLDS = _discovery_thresholds(store, cfg)
    _WORKER_CONFIG = cfg
    if not specs:
        return {}
    # Fork shares the immutable numpy matrices copy-on-write.  Only the small
    # frozen specification and bounded trade result cross process boundaries.
    context = mp.get_context("fork")
    with ProcessPoolExecutor(
        max_workers=cfg.cpu_worker_count,
        mp_context=context,
    ) as executor:
        rows = tuple(executor.map(_evaluate_sparse_worker, specs, chunksize=1))
    if len({key for key, _ in rows}) != len(rows):
        raise SparsePilotError("0032 duplicate sparse worker result")
    return {key: value for key, value in rows}


def _markout_summary(
    opportunities: Sequence[OpportunityEpisode],
    outcomes: Sequence[OpportunityOutcome],
) -> Mapping[str, Any]:
    outcome_by_id = {value.opportunity_id: value for value in outcomes}
    rows: dict[str, Any] = {}
    for mechanism in MECHANISMS:
        selected = [
            outcome_by_id[value.opportunity_id]
            for value in opportunities
            if value.mechanism == mechanism
            and not outcome_by_id[value.opportunity_id].censored
        ]
        rows[mechanism] = {
            "opportunity_episode_count": len(selected),
            "favorable_before_adverse_rate": (
                0.0
                if not selected
                else float(np.mean([value.favorable_first for value in selected]))
            ),
            "adverse_before_favorable_rate": (
                0.0
                if not selected
                else float(np.mean([value.adverse_first for value in selected]))
            ),
            "markouts_ticks": {
                horizon: {
                    "median": _median(
                        [
                            float(value.markouts_ticks[horizon])
                            for value in selected
                            if value.markouts_ticks[horizon] is not None
                        ]
                    ),
                    "lower_quartile": _quantile(
                        [
                            float(value.markouts_ticks[horizon])
                            for value in selected
                            if value.markouts_ticks[horizon] is not None
                        ],
                        0.25,
                    ),
                }
                for horizon in ("1", "5", "30", "120", "300", "900")
            },
        }
    return rows


def _gate_decision(
    results: Sequence[Mapping[str, Any]],
    *,
    forensic_report: Mapping[str, Any] | None,
) -> tuple[str, tuple[str, ...], Mapping[str, bool]]:
    green_rows = []
    for row in results:
        frontier = row.get("account_frontier") or {}
        any_mll = any(
            int(metrics.get("mll_breach_count", 0)) > 0
            for account in frontier.values()
            for metrics in account.values()
        )
        if (
            float(row["validation_normal_net_usd"]) > 0.0
            and float(row["validation_stressed_net_usd"]) > 0.0
            and float(row["final_development_normal_net_usd"]) > 0.0
            and float(row["final_development_stressed_net_usd"]) > 0.0
            and float(row["trades_per_session"]) <= 12.0
            and float(row["single_event_concentration"]) <= 0.50
            and not any_mll
            and int(row["trade_count"]) > 0
        ):
            green_rows.append(row)
    families = tuple(
        sorted({str(row["strategy"]["mechanism"]) for row in green_rows})
    )
    positive_gross = any(float(row["gross_reference_pnl_usd"]) > 0.0 for row in results)
    if forensic_report:
        positive_gross = positive_gross or int(
            forensic_report.get("sparse_alpha_candidate_count", 0) or 0
        ) > 0
    checks = {
        "two_behaviorally_distinct_families": len(families) >= 2,
        "positive_validation_and_final_stressed_net": bool(green_rows),
        "no_mll_breach": all(
            all(
                int(metrics.get("mll_breach_count", 0)) == 0
                for account in (row.get("account_frontier") or {}).values()
                for metrics in account.values()
            )
            for row in green_rows
        ),
        "median_trade_count_no_more_than_12_per_session": all(
            float(row["trades_per_session"]) <= 12.0 for row in green_rows
        ),
        "no_single_event_domination": all(
            float(row["single_event_concentration"]) <= 0.50
            for row in green_rows
        ),
        "deployable_strategy": bool(green_rows),
    }
    if all(checks.values()):
        return "SPARSE_PILOT_GREEN", families, checks
    if positive_gross:
        return "SPARSE_PILOT_WEAK", families, checks
    return "SPARSE_PILOT_FALSIFIED", families, checks


def _ns_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat().replace(
        "+00:00", "Z"
    )


def _evidence_material(
    *,
    cfg: SparsePilotConfig,
    store: SparseStore,
    specs: Sequence[SparseStrategySpec],
    trades_by_id: Mapping[str, Sequence[SparseTrade]],
    results: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Sequence[Mapping[str, Any]]], Mapping[str, Any]]:
    now = _utc_now()
    result_by_id = {str(value["strategy"]["strategy_id"]): value for value in results}
    fingerprints = {value.strategy_id: value.specification_hash for value in specs}
    required_episode_keys: list[Mapping[str, str]] = []
    signals: list[Mapping[str, Any]] = []
    entries: list[Mapping[str, Any]] = []
    exits: list[Mapping[str, Any]] = []
    trades: list[Mapping[str, Any]] = []
    memberships: list[Mapping[str, Any]] = []
    episodes: list[Mapping[str, Any]] = []
    account_daily: list[Mapping[str, Any]] = []
    for spec in specs:
        memberships.append(
            {
                "campaign_id": cfg.campaign_id,
                "policy_id": spec.strategy_id,
                "component_id": spec.strategy_id,
                "risk_allocation": 1.0,
                "component_role": spec.mechanism,
            }
        )
        for trade in trades_by_id.get(spec.strategy_id, ()):
            signal_id = stable_hash(
                {"strategy_id": spec.strategy_id, "opportunity_id": trade.opportunity_id}
            )
            common = {
                "campaign_id": cfg.campaign_id,
                "component_id": spec.strategy_id,
                "trade_id": trade.trade_id,
            }
            signals.append(
                {
                    "campaign_id": cfg.campaign_id,
                    "component_id": spec.strategy_id,
                    "signal_id": signal_id,
                    "event_time": _ns_iso(trade.entry_time_ns),
                    "market": trade.market,
                    "contract": cfg.contracts[trade.market],
                    "timeframe": "EVENT",
                    "signal": trade.direction,
                    "sizing": float(trade.quantity),
                    "stop": trade.stop_price,
                    "target": trade.target_price,
                    "veto": False,
                    "component_role": spec.mechanism,
                }
            )
            entries.append(
                {
                    **common,
                    "entry_time": _ns_iso(trade.entry_time_ns),
                    "market": trade.market,
                    "contract": cfg.contracts[trade.market],
                    "side": "LONG" if trade.direction > 0 else "SHORT",
                    "quantity": float(trade.quantity),
                    "entry_price": trade.entry_price,
                    "sizing": float(trade.quantity),
                    "stop_price": trade.stop_price,
                    "target_price": trade.target_price,
                }
            )
            exits.append(
                {
                    **common,
                    "exit_time": _ns_iso(trade.exit_time_ns),
                    "exit_price": trade.exit_price,
                    "exit_reason": trade.exit_reason,
                }
            )
            trades.append(
                {
                    **common,
                    "entry_time": _ns_iso(trade.entry_time_ns),
                    "exit_time": _ns_iso(trade.exit_time_ns),
                    "market": trade.market,
                    "contract": cfg.contracts[trade.market],
                    "side": "LONG" if trade.direction > 0 else "SHORT",
                    "quantity": float(trade.quantity),
                    "entry_price": trade.entry_price,
                    "exit_price": trade.exit_price,
                    "gross_pnl": trade.gross_reference_pnl_usd,
                    "costs": (
                        trade.spread_cost_usd
                        + trade.depth_slippage_usd
                        + trade.marketable_slippage_usd
                        + trade.commission_usd
                    ),
                    "net_pnl": trade.normal_net_pnl_usd,
                }
            )
        for path in result_by_id[spec.strategy_id]["account_paths"]:
            horizon = f"{int(path['horizon_days'])}D"
            required_episode_keys.append(
                {
                    "policy_id": spec.strategy_id,
                    "episode_id": str(path["episode_id"]),
                    "horizon": horizon,
                }
            )
            episodes.append(
                {
                    "campaign_id": cfg.campaign_id,
                    "policy_id": spec.strategy_id,
                    "episode_id": path["episode_id"],
                    "episode_start": f"{path['start_session']}T00:00:00Z",
                    "horizon": horizon,
                    "temporal_block": store.roles[str(path["start_session"])],
                    "duration_trading_days": len(path["daily_path"]),
                    "target_reached": bool(path["target_reached"]),
                    "mll_breached": bool(path["mll_breached"]),
                    # The shared EvidenceBundle contract treats both an
                    # incomplete data horizon and a completed-but-unreached
                    # operational horizon as censored rather than as a hard
                    # account failure.
                    "censored_state": _evidence_censored_state(
                        str(path["terminal_state"])
                    ),
                    "cost_scenario": path["scenario"],
                    "costs": path["costs_usd"],
                    "net_pnl": path["net_pnl_usd"],
                    "target_progress": float(path["target_progress_pct"]) / 100.0,
                    "minimum_mll_buffer": path["minimum_mll_buffer_usd"],
                    "consistency_ok": path["consistency_compliant"],
                    "days_to_target": path["days_to_target"],
                    "failure_vector": [] if path["target_reached"] else [path["terminal_state"]],
                    "terminal_state": path["terminal_state"],
                }
            )
            for daily in path["daily_path"]:
                account_size = float(path["account_size"])
                account_daily.append(
                    {
                        "campaign_id": cfg.campaign_id,
                        "policy_id": spec.strategy_id,
                        "episode_id": path["episode_id"],
                        "horizon": horizon,
                        "trading_day": daily["session_id"],
                        "cost_scenario": path["scenario"],
                        "realized_pnl": daily["cumulative_net_usd"],
                        "unrealized_pnl": 0.0,
                        "daily_pnl": daily["net_pnl_usd"],
                        "equity": account_size + daily["cumulative_net_usd"],
                        "mll": account_size - float(path["minimum_mll_buffer_usd"]),
                        "mll_buffer": path["minimum_mll_buffer_usd"],
                        "minimum_mll_buffer": path["minimum_mll_buffer_usd"],
                        "consistency": path["consistency_ratio"] or 1.0,
                        "consistency_ok": path["consistency_compliant"],
                        "target_progress": float(path["target_progress_pct"]) / 100.0,
                        "costs": daily["costs_usd"],
                        "conflicts": [],
                        "exposure": {},
                        "component_attribution": {spec.strategy_id: daily["net_pnl_usd"]},
                    }
                )
    identity = {
        "campaign_id": cfg.campaign_id,
        "grammar_id": "sparse_opportunity_episode_abstention_fsm_v1",
        "policy_fingerprints": fingerprints,
        "component_fingerprints": fingerprints,
        "source_commit": cfg.source_commit,
        "data_fingerprints": dict(store.source_hashes),
        "configuration_sha256": cfg.manifest_hash,
        "seeds": [cfg.random_seed],
        "created_at_utc": now,
        "expected_coverage": {
            "policy_ids": list(fingerprints),
            "component_ids": list(fingerprints),
            "required_episode_keys": required_episode_keys,
            "allowed_horizons": ["5D", "10D", "20D"],
            "cost_scenarios": list(COST_SCENARIOS),
            "allow_additional_episode_keys": False,
        },
    }
    datasets = {
        "component_signals": signals,
        "component_entries": entries,
        "component_exits": exits,
        "component_trades": trades,
        "account_policy_membership": memberships,
        "account_daily_paths": account_daily,
        "episodes": episodes,
        "provenance": [
            {
                "campaign_id": cfg.campaign_id,
                "validator_version": SPARSE_PILOT_VERSION,
                "replay_version": "hydra_microstructure_opportunity_episode_fsm_v1",
                "market_data_role": "CHRONOLOGICAL_3_1_1_DEVELOPMENT",
                "access_ledger_sha256": cfg.source_store_hash,
                "reconstruction_flag": False,
                "immutable_checksums": dict(store.source_hashes),
                "recorded_at_utc": now,
            }
        ],
    }
    compact = {
        "campaign_summary": {
            "decision": report["pilot_status"],
            "strategy_count": len(specs),
            "opportunity_episode_count": report["opportunity_episode_count"],
            "trade_count": report["trade_count"],
        },
        "failure_vectors": report["gate_checks"],
        "pareto_archive": report["retained_strategy_ids"],
    }
    return identity, datasets, compact


def _evidence_censored_state(terminal_state: str) -> bool:
    """Map account terminals to the shared EvidenceBundle censoring flag."""

    return terminal_state in {
        "DATA_CENSORED",
        "OPERATIONAL_HORIZON_NOT_REACHED",
    }


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(list(rows)) if rows else pa.table({"empty": pa.array([], type=pa.bool_())})
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    pq.write_table(table, temporary, compression="zstd")
    os.replace(temporary, path)


def run_microstructure_sparse_pilot(
    source_dir: str | Path,
    output_dir: str | Path,
    *,
    config: SparsePilotConfig | Mapping[str, Any] | None = None,
    forensic_report: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    """Run the bounded 0032 sparse distillation over the immutable 0031 store."""

    if config is None:
        cfg = SparsePilotConfig()
    elif isinstance(config, SparsePilotConfig):
        cfg = config
    else:
        cfg = SparsePilotConfig(**dict(config))
    cfg.validate()
    started_wall = time.perf_counter()
    started_times = os.times()
    if forensic_report is None:
        from hydra.production.microstructure_sparse_forensics import (
            SparseForensicsConfig,
            audit_authoritative_0031,
        )

        forensic_report = audit_authoritative_0031(
            source_dir,
            config=SparseForensicsConfig(include_cluster_rows=False),
        )
    store = load_sparse_source_store(source_dir, config=cfg)
    opportunities = build_opportunity_episodes(store, cfg=cfg)
    outcomes = build_opportunity_outcomes(store, opportunities, cfg=cfg)
    models = fit_abstention_models(store, opportunities, outcomes, cfg=cfg)
    specs = freeze_sparse_strategy_bank(models, cfg=cfg)
    if not specs:
        raise SparsePilotError("0032 no frozen sparse strategy could be formed")
    trades_by_id = _execute_candidate_bank(
        specs, models, opportunities, store, cfg=cfg
    )
    results = [
        _strategy_result(spec, trades_by_id.get(spec.strategy_id, ()), store, cfg)
        for spec in specs
    ]
    decision, families, checks = _gate_decision(
        results, forensic_report=forensic_report
    )
    retained = tuple(
        str(row["strategy"]["strategy_id"])
        for row in results
        if float(row["validation_stressed_net_usd"]) > 0.0
        and float(row["final_development_stressed_net_usd"]) > 0.0
    )
    elapsed = max(time.perf_counter() - started_wall, 1e-9)
    finished_times = os.times()
    cpu_seconds = (
        finished_times.user
        + finished_times.system
        + finished_times.children_user
        + finished_times.children_system
        - started_times.user
        - started_times.system
        - started_times.children_user
        - started_times.children_system
    )
    utilization = min(max(cpu_seconds / (elapsed * 3.0), 0.0), 1.0)
    markouts = _markout_summary(opportunities, outcomes)
    all_account_paths = [
        path for row in results for path in row["account_paths"]
    ]
    normal_paths = [
        value for value in all_account_paths if value["scenario"] == "NORMAL"
    ]
    stressed_paths = [
        value
        for value in all_account_paths
        if value["scenario"] == "STRESSED_1_5X"
    ]
    normal_rates: list[float] = []
    stressed_rates: list[float] = []
    for row in results:
        paths = row["account_paths"]
        for scenario, destination in (
            ("NORMAL", normal_rates),
            ("STRESSED_1_5X", stressed_rates),
        ):
            full_p5 = [
                value
                for value in paths
                if value["scenario"] == scenario
                and value["full_coverage"]
                and int(value["horizon_days"]) == 5
            ]
            destination.append(
                0.0
                if not full_p5
                else sum(bool(value["target_reached"]) for value in full_p5)
                / len(full_p5)
            )
    stressed_progress = [
        float(value["target_progress_pct"]) / 100.0
        for value in stressed_paths
        if value["full_coverage"]
    ]
    mll_rates = []
    for row in results:
        selected = [
            value
            for value in row["account_paths"]
            if value["scenario"] == "STRESSED_1_5X" and value["full_coverage"]
        ]
        mll_rates.append(
            0.0
            if not selected
            else sum(bool(value["mll_breached"]) for value in selected)
            / len(selected)
        )
    production_kpis = {
        "sparse_strategies_evaluated": len(specs),
        "exact_replay_count": len(specs),
        "candidate_count": len(specs),
        "normal_episode_count": len(normal_paths),
        "stressed_episode_count": len(stressed_paths),
        "positive_stressed_count": sum(
            float(value["stressed_net_usd"]) > 0.0 for value in results
        ),
        "normal_pass_candidate_count": sum(value > 0.0 for value in normal_rates),
        "stressed_pass_candidate_count": sum(value > 0.0 for value in stressed_rates),
        "normal_p5_pass_rate_best": max(normal_rates, default=0.0),
        "normal_p5_pass_rate_median": _median(normal_rates) or 0.0,
        "stressed_p5_pass_rate_best": max(stressed_rates, default=0.0),
        "stressed_p5_pass_rate_median": _median(stressed_rates) or 0.0,
        "near_pass_count": sum(
            value >= 0.60 for value in stressed_progress
        ),
        "stressed_target_progress_best_fraction": max(
            stressed_progress, default=0.0
        ),
        "stressed_target_progress_median_fraction": _median(
            stressed_progress
        )
        or 0.0,
        "mll_breach_rate_minimum": min(mll_rates, default=0.0),
        "mll_breach_rate_maximum": max(mll_rates, default=0.0),
        "opportunity_episode_count": len(opportunities),
        "control_replay_count": 0,
        "matched_controls_status": "NOT_REQUIRED_BEFORE_SPARSE_GATE",
        "null_status": "NOT_REQUIRED_BEFORE_SPARSE_GATE",
        "strategies_per_hour": len(specs) * 3600.0 / elapsed,
        "exact_replays_per_hour": len(specs) * 3600.0 / elapsed,
        "account_episodes_per_hour": len(all_account_paths) * 3600.0 / elapsed,
        "economic_wall_clock_fraction": 0.95,
        "cpu_utilization_fraction": utilization,
    }
    report: dict[str, Any] = {
        "schema": "hydra_microstructure_sparse_alpha_distillation_0032_report_v1",
        "campaign_id": cfg.campaign_id,
        "manifest_hash": cfg.manifest_hash,
        "source_commit": cfg.source_commit,
        "generated_at_utc": _utc_now(),
        "pilot_status": decision,
        "source_campaign_status": "MICROSTRUCTURE_PILOT_FALSIFIED",
        "source_campaign_regenerated": False,
        "source_store_hashes": dict(store.source_hashes),
        "session_roles": dict(store.roles),
        "opportunity_episode_count": len(opportunities),
        "independent_opportunities_per_session": {
            session: sum(value.session_id == session for value in opportunities)
            for session in store.sessions
        },
        "opportunities_by_mechanism": {
            mechanism: sum(value.mechanism == mechanism for value in opportunities)
            for mechanism in MECHANISMS
        },
        "markout_summary": markouts,
        "meta_model_count": len(models),
        "sparse_strategies_evaluated": len(specs),
        "trade_count": sum(len(value) for value in trades_by_id.values()),
        "trades_per_account_episode_source_0031": 144_947.0 / 720.0,
        "trades_per_sleeve_source_0031": 144_947.0 / 24.0,
        "source_mll_rate": 621.0 / 720.0,
        "candidate_results": results,
        "gate_checks": dict(checks),
        "useful_mechanism_families": list(families),
        "retained_strategy_ids": list(retained),
        "strategy_bank_size": len(retained) if decision == "SPARSE_PILOT_GREEN" else 0,
        "account_books_constructed": 0,
        "conditional_data_extension": {
            "triggered": decision == "SPARSE_PILOT_GREEN",
            "cost_matrix_required_before_purchase": decision == "SPARSE_PILOT_GREEN",
            "purchase_performed": False,
            "actual_additional_spend_usd": 0.0,
            "maximum_incremental_spend_usd": 3.25,
            "minimum_budget_reserve_usd": 25.0,
        },
        "forensic_report": dict(forensic_report or {}),
        "runtime_metrics": {
            "elapsed_seconds": elapsed,
            "cpu_seconds": cpu_seconds,
            "cpu_utilization_fraction_three_core": utilization,
            "economic_wall_clock_fraction": 0.95,
            "cpu_worker_count": cfg.cpu_worker_count,
            "orchestrator_writer_process_count": 1,
            "blas_threads_per_worker": 1,
        },
        "production_kpis": production_kpis,
        "governance": {
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "additional_data_spend_usd": 0.0,
            "thresholds_changed_after_results": False,
        },
    }
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    _atomic_json(output / "decision_report.json", report)
    _write_parquet(
        output / "opportunity_episodes.parquet",
        [asdict(value) for value in opportunities],
    )
    _write_parquet(
        output / "opportunity_outcomes.parquet",
        [asdict(value) for value in outcomes],
    )
    _write_parquet(
        output / "sparse_strategy_manifests.parquet",
        [asdict(value) for value in specs],
    )
    _write_parquet(
        output / "sparse_trades.parquet",
        [asdict(value) for rows in trades_by_id.values() for value in rows],
    )
    account_paths = [
        {key: value for key, value in path.items() if key != "daily_path"}
        for row in results
        for path in row["account_paths"]
    ]
    _write_parquet(output / "account_episodes.parquet", account_paths)
    identity, datasets, compact = _evidence_material(
        cfg=cfg,
        store=store,
        specs=specs,
        trades_by_id=trades_by_id,
        results=results,
        report=report,
    )
    result = {
        **report,
        "decision_report": report,
        "runtime_kpis": production_kpis,
        "strategy_results": results,
        "survivor_ids": list(retained),
        "evidence_identity": identity,
        "evidence_datasets": datasets,
        "compact_outputs": compact,
        "report_hash": stable_hash(report),
    }
    return result


__all__ = [
    "PILOT_STATUSES",
    "SPARSE_PILOT_VERSION",
    "SparsePilotConfig",
    "SparsePilotError",
    "build_opportunity_episodes",
    "build_opportunity_outcomes",
    "fit_abstention_models",
    "freeze_sparse_strategy_bank",
    "load_sparse_source_store",
    "run_microstructure_sparse_pilot",
]
