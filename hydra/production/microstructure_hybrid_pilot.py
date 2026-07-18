"""Bounded hybrid structural/order-flow pilot for campaign 0033.

The pilot deliberately reuses two immutable sources with different roles:

* clean 0028 event ledgers define the structural opportunity, direction,
  target, stop, horizon and the A0 development baseline;
* the sealed 0031 feature/book/tape store supplies only causal execution
  context for the paired A2--A5 counterfactual actions.

No direction is synthesised.  Candidate selection is performed on the three
discovery sessions only; validation and final-development rows are opened only
after each policy is frozen.  A4 is a conservative passive-queue diagnostic
and can never be selected into the primary policy bank.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import math
import multiprocessing as mp
from pathlib import Path
import resource
import time
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import stable_hash
from hydra.production.microstructure_sparse_pilot import (
    SparsePilotConfig,
    SparseStore,
    load_sparse_source_store,
)


HYBRID_PILOT_VERSION = "hydra_microstructure_hybrid_pilot_v1"
PILOT_STATUSES = (
    "HYBRID_OVERLAY_GREEN",
    "HYBRID_OVERLAY_WEAK",
    "HYBRID_OVERLAY_FALSIFIED",
)
SESSION_ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
ACTION_IDS = (
    "A0_BASELINE_IMMEDIATE",
    "A1_ABSTAIN",
    "A2_WAIT_CONFIRM",
    "A3_PULLBACK_MARKETABLE_LIMIT",
    "A4_PASSIVE_JOIN",
    "A5_EARLY_INVALIDATION",
)
PROMOTABLE_ACTIONS = frozenset(
    {
        "A0_BASELINE_IMMEDIATE",
        "A1_ABSTAIN",
        "A2_WAIT_CONFIRM",
        "A3_PULLBACK_MARKETABLE_LIMIT",
        "A5_EARLY_INVALIDATION",
    }
)


class HybridPilotError(RuntimeError):
    """A frozen 0028/0031 input or the bounded paired contract is invalid."""


@dataclass(frozen=True, slots=True)
class HybridPilotConfig:
    campaign_id: str = "hydra_hybrid_structural_alpha_order_flow_0033"
    manifest_hash: str = "UNBOUND_TEST_MANIFEST"
    source_commit: str = "0" * 40
    selected_markets: tuple[str, str] = ("NQ", "YM")
    selected_sessions: tuple[str, ...] = (
        "2024-07-08",
        "2024-07-09",
        "2024-07-10",
        "2024-07-11",
        "2024-07-12",
    )
    chronological_roles: tuple[int, int, int] = (3, 1, 1)
    maximum_anchors: int = 24
    expected_active_anchors: int = 22
    maximum_policies: int = 20
    cpu_worker_count: int = 2
    risk_tiers: tuple[float, float, float] = (0.50, 1.00, 1.50)
    aggressive_latency_ns: int = 25_000_000
    wait_confirmation_ns: int = 2_000_000_000
    pullback_deadline_ns: int = 10_000_000_000
    passive_deadline_ns: int = 10_000_000_000
    pullback_improvement_ticks: float = 1.0
    early_invalidation_fraction: float = 0.25
    early_invalidation_grace_ns: int = 2_000_000_000
    normal_adverse_slippage_ticks: float = 2.0
    stressed_adverse_slippage_ticks: float = 3.0
    commission_per_micro_round_trip_usd: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 1.24, "YM": 1.24}
    )
    tick_size: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 0.25, "YM": 1.0}
    )
    micro_point_value: Mapping[str, float] = field(
        default_factory=lambda: {"NQ": 2.0, "YM": 0.5}
    )
    maximum_micro_contracts: int = 150
    account_target_usd: float = 9_000.0
    account_mll_usd: float = 4_500.0
    maximum_opportunity_profit_concentration: float = 0.50
    minimum_green_policies: int = 2
    minimum_green_anchor_families: int = 2
    minimum_weak_policies: int = 1
    no_data_purchase: bool = True
    q4_access_authorized: bool = False
    broker_connections: int = 0
    orders: int = 0

    def validate(self) -> None:
        if self.selected_markets != ("NQ", "YM"):
            raise HybridPilotError("0033 market binding must remain NQ/YM")
        if self.chronological_roles != (3, 1, 1):
            raise HybridPilotError("0033 chronological roles must remain 3/1/1")
        if len(self.selected_sessions) != 5 or tuple(sorted(self.selected_sessions)) != self.selected_sessions:
            raise HybridPilotError("0033 requires five ordered complete sessions")
        if self.risk_tiers != (0.50, 1.00, 1.50):
            raise HybridPilotError("0033 risk tiers must remain 0.50/1.00/1.50")
        if not 1 <= self.maximum_anchors <= 24 or self.expected_active_anchors > self.maximum_anchors:
            raise HybridPilotError("0033 structural-anchor cap is invalid")
        if not 1 <= self.maximum_policies <= 20:
            raise HybridPilotError("0033 policy cap is invalid")
        if self.cpu_worker_count != 2:
            raise HybridPilotError("0033 requires exactly two economic workers")
        if self.account_target_usd <= 0 or self.account_mll_usd <= 0:
            raise HybridPilotError("0033 account target/MLL is invalid")
        if not 0.0 < self.maximum_opportunity_profit_concentration <= 1.0:
            raise HybridPilotError("0033 opportunity-domination limit is invalid")
        if min(
            self.aggressive_latency_ns,
            self.wait_confirmation_ns,
            self.pullback_deadline_ns,
            self.passive_deadline_ns,
        ) < 0:
            raise HybridPilotError("0033 latency/deadline frontier is invalid")
        if not self.no_data_purchase or self.q4_access_authorized or self.broker_connections or self.orders:
            raise HybridPilotError("0033 is purchase/Q4/broker/order fail-closed")
        for market in self.selected_markets:
            if min(
                float(self.tick_size.get(market, 0.0)),
                float(self.micro_point_value.get(market, 0.0)),
                float(self.commission_per_micro_round_trip_usd.get(market, 0.0)),
            ) <= 0:
                raise HybridPilotError(f"0033 economics missing for {market}")

    @property
    def roles(self) -> Mapping[str, str]:
        discovery, validation, _final = self.chronological_roles
        return {
            session: (
                "DISCOVERY"
                if offset < discovery
                else "VALIDATION"
                if offset < discovery + validation
                else "FINAL_DEVELOPMENT"
            )
            for offset, session in enumerate(self.selected_sessions)
        }


@dataclass(frozen=True, slots=True)
class StructuralOpportunityEpisode:
    opportunity_id: str
    anchor_id: str
    anchor_fingerprint: str
    mechanism: str
    market: str
    execution_market: str
    timeframe: str
    session_id: str
    role: str
    direction: int
    event_time_ns: int
    available_at_ns: int
    decision_time_ns: int
    order_submit_time_ns: int
    earliest_executable_time_ns: int
    baseline_fill_time_ns: int
    baseline_exit_time_ns: int | None
    raw_fill_price: float
    normal_fill_price: float
    stressed_fill_price: float
    raw_exit_price: float | None
    stop_price: float
    target_price: float
    maximum_horizon: str
    quantity: int
    baseline_normal_net_pnl: float | None
    baseline_stressed_net_pnl: float | None
    baseline_normal_minimum_unrealized_pnl: float | None
    baseline_stressed_minimum_unrealized_pnl: float | None
    baseline_outcome: str
    source_reference_censored: bool
    source_fill_policy_id: str
    source_fill_policy_hash: str
    feature_fingerprint: str
    causal_fingerprint: str
    source_event_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class HybridActionSpec:
    action_id: str
    risk_tier: float
    promotion_eligible: bool
    passive_side_lane: bool
    action_hash: str

    @property
    def key(self) -> str:
        return f"{self.action_id}:{self.risk_tier:.2f}"


@dataclass(frozen=True, slots=True)
class _ScenarioExecution:
    fill_status: str
    quantity: int
    fill_time_ns: int | None
    exit_time_ns: int | None
    fill_price: float | None
    exit_price: float | None
    gross_pnl_usd: float
    costs_usd: float
    net_pnl_usd: float
    minimum_unrealized_pnl_usd: float
    exit_reason: str
    quantity_ahead: int = 0
    observed_contra_volume: int = 0


@dataclass(frozen=True, slots=True)
class PairedActionOutcome:
    paired_group_id: str
    opportunity_id: str
    anchor_id: str
    mechanism: str
    market: str
    execution_market: str
    quantity_unit: str
    execution_book_quantity_unit: str
    micro_per_mini_ratio: int
    session_id: str
    role: str
    direction: int
    action_id: str
    risk_tier: float
    promotion_eligible: bool
    passive_side_lane: bool
    causal_quality_score: float
    joined_feature_hash: str
    joined_decision_time_ns: int
    feature_join_lag_ns: int
    normal: Mapping[str, Any]
    stressed: Mapping[str, Any]
    baseline_normal_net_pnl: float
    baseline_stressed_net_pnl: float
    source_0028_normal_net_pnl: float | None
    source_0028_stressed_net_pnl: float | None
    source_0028_normal_fill_price: float
    source_0028_stressed_fill_price: float
    normal_delta_vs_a0_usd: float
    stressed_delta_vs_a0_usd: float
    normal_delta_vs_source_0028_usd: float | None
    stressed_delta_vs_source_0028_usd: float | None
    development_fill_model_id: str
    counterfactual_fill_model_id: str
    normal_fill_price_delta_vs_a0: float | None
    stressed_fill_price_delta_vs_a0: float | None
    normal_fill_price_delta_vs_source_0028: float | None
    stressed_fill_price_delta_vs_source_0028: float | None
    causal_fingerprint: str
    outcome_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise HybridPilotError(f"required immutable input is absent: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise HybridPilotError(f"JSON object required: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise HybridPilotError(f"required immutable input is absent: {path}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise HybridPilotError(f"JSONL row is not an object: {path}:{line_number}")
            rows.append(value)
    return rows


def _session_date(row: Mapping[str, Any]) -> str:
    if "session_day" in row:
        return (date(1970, 1, 1) + timedelta(days=int(row["session_day"]))).isoformat()
    value = row.get("session_id")
    if value is None:
        raise HybridPilotError("0028 event has no session_day/session_id")
    return str(value)


def _close(left: float, right: float, *, tolerance: float = 1e-9) -> bool:
    return math.isclose(float(left), float(right), rel_tol=1e-10, abs_tol=tolerance)


def _episode_from_source(
    row: Mapping[str, Any],
    population: Mapping[str, Any],
    *,
    config: HybridPilotConfig,
) -> StructuralOpportunityEpisode:
    candidate_id = str(row.get("candidate_id") or "")
    if candidate_id != str(population.get("candidate_id") or ""):
        raise HybridPilotError("0028 event/candidate identity mismatch")
    candidate = dict(population.get("candidate") or {})
    market = str(row.get("market") or "")
    if market not in config.selected_markets or market != str(candidate.get("market") or ""):
        raise HybridPilotError("0028 event/candidate market mismatch")
    session_id = _session_date(row)
    if session_id not in config.roles:
        raise HybridPilotError("0028 event is outside frozen 0033 sessions")
    direction = int(row.get("direction", 0))
    if direction not in {-1, 1}:
        raise HybridPilotError("0028 event direction is invalid")
    intent = str(row.get("entry_intent") or "")
    if (direction > 0 and "LONG" not in intent) or (direction < 0 and "SHORT" not in intent):
        raise HybridPilotError("0028 event direction was not preserved by its intent")
    available = int(row["available_at_ns"])
    decision = int(row["decision_time_ns"])
    submit = int(row["order_submit_time_ns"])
    earliest = int(row["earliest_executable_time_ns"])
    fill_time = int(row["fill_time_ns"])
    raw_exit_time = row.get("outcome_time_ns")
    exit_time = None if raw_exit_time is None else int(raw_exit_time)
    if not (available <= decision <= submit <= earliest <= fill_time) or (
        exit_time is not None and fill_time > exit_time
    ):
        raise HybridPilotError("0028 event violates causal time ordering")
    horizon = str(row["maximum_horizon"])
    candidate_horizon = str(candidate.get("horizon", ""))
    if horizon != candidate_horizon or (
        horizon != "SESSION" and (not horizon.isdigit() or int(horizon) <= 0)
    ):
        raise HybridPilotError("0028 event horizon drift")
    raw_fill = float(row["raw_fill_price"])
    target = float(row["favorable_price"])
    stop = float(row["adverse_price"])
    if direction * (target - raw_fill) <= 0 or direction * (stop - raw_fill) >= 0:
        raise HybridPilotError("0028 target/stop geometry is invalid")
    if not _close(float(row["favorable_r"]), float(candidate.get("favorable_r", math.nan))):
        raise HybridPilotError("0028 favorable boundary drift")
    if not _close(float(row["adverse_r"]), float(candidate.get("adverse_r", math.nan))):
        raise HybridPilotError("0028 adverse boundary drift")
    causal = {
        "anchor_id": candidate_id,
        "anchor_fingerprint": str(population.get("structural_fingerprint") or ""),
        "mechanism": str(candidate.get("mechanism") or "UNKNOWN"),
        "market": market,
        "timeframe": str(row.get("timeframe") or candidate.get("timeframe") or ""),
        "session_id": session_id,
        "direction": direction,
        "event_time_ns": int(row["event_time_ns"]),
        "available_at_ns": available,
        "decision_time_ns": decision,
        "order_submit_time_ns": submit,
        "earliest_executable_time_ns": earliest,
        "target_price": target,
        "stop_price": stop,
        "maximum_horizon": horizon,
        "quantity": int(row["quantity"]),
        "feature_fingerprint": str(row["feature_fingerprint"]),
    }
    source_censored = str(row.get("outcome") or "") == "CENSORED_FUTURE_COVERAGE"

    def optional_float(name: str) -> float | None:
        value = row.get(name)
        return None if value is None else float(value)

    return StructuralOpportunityEpisode(
        opportunity_id=str(row["event_id"]),
        anchor_id=candidate_id,
        anchor_fingerprint=str(population.get("structural_fingerprint") or ""),
        mechanism=str(candidate.get("mechanism") or "UNKNOWN"),
        market=market,
        execution_market=str(row.get("execution_market") or candidate.get("execution_market") or ""),
        timeframe=str(row.get("timeframe") or candidate.get("timeframe") or ""),
        session_id=session_id,
        role=config.roles[session_id],
        direction=direction,
        event_time_ns=int(row["event_time_ns"]),
        available_at_ns=available,
        decision_time_ns=decision,
        order_submit_time_ns=submit,
        earliest_executable_time_ns=earliest,
        baseline_fill_time_ns=fill_time,
        baseline_exit_time_ns=exit_time,
        raw_fill_price=raw_fill,
        normal_fill_price=float(row["normal_fill_price"]),
        stressed_fill_price=float(row["stressed_fill_price"]),
        raw_exit_price=optional_float("raw_exit_price"),
        stop_price=stop,
        target_price=target,
        maximum_horizon=horizon,
        quantity=int(row["quantity"]),
        baseline_normal_net_pnl=optional_float("normal_net_pnl"),
        baseline_stressed_net_pnl=optional_float("stressed_net_pnl"),
        baseline_normal_minimum_unrealized_pnl=optional_float("normal_worst_unrealized_pnl"),
        baseline_stressed_minimum_unrealized_pnl=optional_float("stressed_worst_unrealized_pnl"),
        baseline_outcome=str(row["outcome"]),
        source_reference_censored=source_censored,
        source_fill_policy_id=str(row["fill_policy_id"]),
        source_fill_policy_hash=str(row["fill_policy_hash"]),
        feature_fingerprint=str(row["feature_fingerprint"]),
        causal_fingerprint=stable_hash(causal),
        source_event_hash=stable_hash(dict(row)),
    )


def load_structural_opportunities(
    anchor_population_path: str | Path,
    anchor_event_root: str | Path,
    clean_result_path: str | Path,
    *,
    config: HybridPilotConfig,
) -> tuple[tuple[StructuralOpportunityEpisode, ...], dict[str, Any]]:
    """Load only pre-existing clean 0028 NQ/YM anchors active in the five sessions."""

    config.validate()
    population_path = Path(anchor_population_path).resolve()
    event_root = Path(anchor_event_root).resolve()
    result_path = Path(clean_result_path).resolve()
    population_rows = _read_jsonl(population_path)
    population: dict[str, dict[str, Any]] = {}
    for row in population_rows:
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id or candidate_id in population:
            raise HybridPilotError("0028 population candidate identity is missing/duplicated")
        population[candidate_id] = row
    result = _read_json(result_path)
    clean_ids = list((result.get("economic_results") or {}).get("clean_useful_sleeve_ids") or ())
    if not clean_ids:
        raise HybridPilotError("0028 clean useful sleeve inventory is absent")

    selected: list[str] = []
    episodes: list[StructuralOpportunityEpisode] = []
    event_hashes: dict[str, str] = {}
    for raw_id in clean_ids:
        candidate_id = str(raw_id)
        source = population.get(candidate_id)
        if source is None:
            raise HybridPilotError(f"clean 0028 candidate absent from population: {candidate_id}")
        market = str((source.get("candidate") or {}).get("market") or "")
        if market not in config.selected_markets:
            continue
        event_path = event_root / f"{candidate_id}.jsonl"
        if not event_path.is_file():
            raise HybridPilotError(f"clean 0028 event ledger absent: {candidate_id}")
        source_rows = _read_jsonl(event_path)
        active_rows = [row for row in source_rows if _session_date(row) in config.roles]
        if not active_rows:
            continue
        if len(selected) >= config.maximum_anchors:
            raise HybridPilotError("clean 0028 active anchor count exceeds frozen cap")
        selected.append(candidate_id)
        event_hashes[candidate_id] = _sha256_file(event_path)
        episodes.extend(_episode_from_source(row, source, config=config) for row in active_rows)
    if len(selected) != config.expected_active_anchors:
        raise HybridPilotError(
            f"clean active 0028 anchor inventory drift: {len(selected)} != {config.expected_active_anchors}"
        )
    opportunity_ids = [value.opportunity_id for value in episodes]
    if len(opportunity_ids) != len(set(opportunity_ids)):
        raise HybridPilotError("0028 structural opportunity identity collision")
    episodes.sort(key=lambda value: (value.decision_time_ns, value.anchor_id, value.opportunity_id))
    provenance = {
        "selected_anchor_ids": selected,
        "selected_anchor_count": len(selected),
        "opportunity_count": len(episodes),
        "population_sha256": _sha256_file(population_path),
        "clean_result_sha256": _sha256_file(result_path),
        "event_file_sha256": event_hashes,
        "selection_rule": "CLEAN_USEFUL_0028_AND_NQ_YM_AND_ANY_EVENT_IN_FROZEN_FIVE_SESSIONS",
        "selection_uses_0033_outcomes": False,
        "direction_synthesis_allowed": False,
    }
    return tuple(episodes), provenance


def freeze_action_lattice(config: HybridPilotConfig) -> tuple[HybridActionSpec, ...]:
    config.validate()
    raw: list[tuple[str, float, bool, bool]] = [
        ("A0_BASELINE_IMMEDIATE", 1.0, True, False),
        ("A1_ABSTAIN", 0.0, True, False),
    ]
    for action_id in ACTION_IDS[2:]:
        for tier in config.risk_tiers:
            raw.append(
                (
                    action_id,
                    tier,
                    action_id in PROMOTABLE_ACTIONS,
                    action_id == "A4_PASSIVE_JOIN",
                )
            )
    return tuple(
        HybridActionSpec(
            action_id=action_id,
            risk_tier=float(tier),
            promotion_eligible=promotion,
            passive_side_lane=passive,
            action_hash=stable_hash(
                {
                    "campaign_id": config.campaign_id,
                    "action_id": action_id,
                    "risk_tier": tier,
                    "promotion_eligible": promotion,
                    "passive_side_lane": passive,
                }
            ),
        )
        for action_id, tier, promotion, passive in raw
    )


def _scaled_quantity(episode: StructuralOpportunityEpisode, tier: float, config: HybridPilotConfig) -> int:
    quantity = int(math.floor(episode.quantity * tier + 0.5))
    return min(max(quantity, 1), config.maximum_micro_contracts)


def _depth_fill(depth_json: str, quantity: int) -> tuple[int, float | None]:
    try:
        depth = json.loads(str(depth_json))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise HybridPilotError("0031 displayed depth is not canonical JSON") from exc
    remaining = int(quantity)
    filled = 0
    notional = 0.0
    for raw_price, raw_size in depth:
        take = min(remaining, max(0, int(raw_size)))
        if take <= 0:
            continue
        filled += take
        remaining -= take
        notional += take * float(raw_price)
        if remaining <= 0:
            break
    return filled, (notional / filled if filled else None)


def _market_rows(store: SparseStore) -> dict[str, np.ndarray]:
    return {market: np.flatnonzero(store.market == market) for market in sorted(set(store.market))}


def _decision_context(
    episode: StructuralOpportunityEpisode,
    store: SparseStore,
    rows: np.ndarray,
    *,
    config: HybridPilotConfig,
) -> tuple[int, str, int, float]:
    """Strict causal as-of join of one 0028 decision to the sealed 0031 store."""

    times = store.decision_ns[rows]
    local = int(np.searchsorted(times, episode.decision_time_ns, side="right")) - 1
    if local < 0:
        raise HybridPilotError("0031 has no causal context before an 0028 decision")
    row = int(rows[local])
    if (
        str(store.session[row]) != episode.session_id
        or int(store.available_ns[row]) > episode.decision_time_ns
        or int(store.decision_ns[row]) > episode.decision_time_ns
    ):
        raise HybridPilotError("0028->0031 causal context join failed closed")
    lookup = {name: index for index, name in enumerate(store.feature_names)}
    required = ("flow_2s", "flow_30s", "bbo_imbalance", "microprice_deviation", "spread_ticks")
    if any(name not in lookup for name in required):
        raise HybridPilotError("0031 causal quality features are incomplete")
    direction = episode.direction
    flow2 = float(store.feature_values[row, lookup["flow_2s"]])
    flow30 = float(store.feature_values[row, lookup["flow_30s"]])
    imbalance = float(store.feature_values[row, lookup["bbo_imbalance"]])
    micro = float(store.feature_values[row, lookup["microprice_deviation"]])
    spread = max(0.0, float(store.feature_values[row, lookup["spread_ticks"]]))
    tick = float(config.tick_size[episode.market])
    quality = (
        direction * flow2 / (1.0 + abs(flow30))
        + direction * imbalance
        + direction * micro / max(tick, 1e-12)
        - 0.05 * spread
    )
    return (
        row,
        str(store.feature_hashes[row]),
        int(episode.decision_time_ns - store.decision_ns[row]),
        float(quality),
    )


def _entry_aggressive(
    episode: StructuralOpportunityEpisode,
    action: HybridActionSpec,
    store: SparseStore,
    rows: np.ndarray,
    *,
    slippage_ticks: float,
    config: HybridPilotConfig,
) -> tuple[int, int, float, str, int, int] | None:
    times = store.decision_ns[rows]
    base_local = int(np.searchsorted(times, episode.earliest_executable_time_ns + config.aggressive_latency_ns, side="left"))
    if base_local >= len(rows) or str(store.session[int(rows[base_local])]) != episode.session_id:
        return None
    direction = episode.direction
    tick = float(config.tick_size[episode.market])
    quantity = _scaled_quantity(episode, action.risk_tier, config)
    chosen_local = base_local
    if action.action_id == "A5_EARLY_INVALIDATION":
        # Frozen pre-entry invalidation: a causal contradiction at placement
        # cancels the order while the account is still flat.  A5 never enters
        # and then retroactively calls an early exit an invalidation.
        base_row = int(rows[base_local])
        lookup = {name: index for index, name in enumerate(store.feature_names)}
        flow = float(store.feature_values[base_row, lookup["flow_2s"]])
        micro = float(store.feature_values[base_row, lookup["microprice_deviation"]])
        if direction * flow <= 0.0 and direction * micro <= 0.0:
            return None
    elif action.action_id == "A2_WAIT_CONFIRM":
        chosen_local = int(np.searchsorted(times, times[base_local] + config.wait_confirmation_ns, side="left"))
        if chosen_local >= len(rows) or str(store.session[int(rows[chosen_local])]) != episode.session_id:
            return None
        base_row, chosen_row = int(rows[base_local]), int(rows[chosen_local])
        base_mid = 0.5 * (store.bid_price[base_row] + store.ask_price[base_row])
        chosen_mid = 0.5 * (store.bid_price[chosen_row] + store.ask_price[chosen_row])
        feature_lookup = {name: index for index, name in enumerate(store.feature_names)}
        flow = float(store.feature_values[chosen_row, feature_lookup["flow_2s"]])
        if direction * (chosen_mid - base_mid) < 0.0 or direction * flow <= 0.0:
            return None
    elif action.action_id == "A3_PULLBACK_MARKETABLE_LIMIT":
        base_row = int(rows[base_local])
        base_quote = float(store.ask_price[base_row] if direction > 0 else store.bid_price[base_row])
        base_executable = base_quote + direction * slippage_ticks * tick
        deadline = times[base_local] + config.pullback_deadline_ns
        stop_local = int(np.searchsorted(times, deadline, side="right"))
        chosen_local = -1
        for local in range(base_local, min(stop_local, len(rows))):
            row = int(rows[local])
            if str(store.session[row]) != episode.session_id:
                break
            quote = float(store.ask_price[row] if direction > 0 else store.bid_price[row])
            executable = quote + direction * slippage_ticks * tick
            improvement = direction * (base_executable - executable)
            if improvement + 1e-12 >= config.pullback_improvement_ticks * tick:
                chosen_local = local
                break
        if chosen_local < 0:
            return None
    row = int(rows[chosen_local])
    # The 0028 quantities are MNQ/MYM micros while the sealed 0031 book is
    # NQ/YM mini depth.  Comparing the micro quantity directly with displayed
    # mini size would be a unit error.  Promotable actions therefore use the
    # causal BBO plus the frozen adverse slippage, and retain the exact micro
    # quantity.  The 10:1 bridge is persisted in every outcome.
    quote = float(store.ask_price[row] if direction > 0 else store.bid_price[row])
    fill_price = float(quote + direction * slippage_ticks * tick)
    return row, quantity, fill_price, "AGGRESSIVE_BBO_MICRO", 0, 0


def _entry_passive(
    episode: StructuralOpportunityEpisode,
    action: HybridActionSpec,
    store: SparseStore,
    rows: np.ndarray,
    *,
    config: HybridPilotConfig,
) -> tuple[int, int, float, str, int, int] | None:
    times = store.decision_ns[rows]
    local = int(np.searchsorted(times, episode.earliest_executable_time_ns + config.aggressive_latency_ns, side="left"))
    if local >= len(rows):
        return None
    row = int(rows[local])
    if str(store.session[row]) != episode.session_id:
        return None
    direction = episode.direction
    quantity = _scaled_quantity(episode, action.risk_tier, config)
    limit = float(store.bid_price[row] if direction > 0 else store.ask_price[row])
    ahead = int(store.bid_size[row] if direction > 0 else store.ask_size[row])
    tape_times = store.derived_available_ns[episode.market]
    tape_prices = store.derived_price[episode.market]
    tape_sizes = store.derived_size[episode.market]
    tape_sides = store.derived_side[episode.market]
    start = int(np.searchsorted(tape_times, store.decision_ns[row], side="left"))
    session_rows = rows[store.session[rows] == episode.session_id]
    session_end = int(store.decision_ns[int(session_rows[-1])])
    deadline = min(int(store.decision_ns[row]) + config.passive_deadline_ns, session_end)
    consumed = 0
    filled = 0
    fill_time = -1
    contra_side = "A" if direction > 0 else "B"
    for index in range(start, len(tape_times)):
        timestamp = int(tape_times[index])
        if timestamp > deadline:
            break
        price = float(tape_prices[index])
        crossed = price <= limit if direction > 0 else price >= limit
        if str(tape_sides[index]) != contra_side or not crossed:
            continue
        consumed += int(tape_sizes[index])
        # Queue and tape sizes are mini contracts; the candidate quantity is
        # micro contracts.  This diagnostic teacher-only lane uses the frozen
        # 10 micros per mini equivalence explicitly.
        executable = min(quantity, max(0, consumed - ahead) * 10)
        if executable > filled:
            filled = executable
            fill_time = timestamp
        if filled >= quantity:
            break
    if filled <= 0:
        return None
    entry_local = int(np.searchsorted(times, fill_time, side="left"))
    if entry_local >= len(rows):
        return None
    return int(rows[entry_local]), filled, limit, "PASSIVE_TRADE_THROUGH_QUEUE", ahead, consumed


def _simulate_scenario(
    episode: StructuralOpportunityEpisode,
    action: HybridActionSpec,
    store: SparseStore,
    rows: np.ndarray,
    *,
    slippage_ticks: float,
    config: HybridPilotConfig,
) -> _ScenarioExecution:
    if action.action_id == "A1_ABSTAIN":
        return _ScenarioExecution("ABSTAINED", 0, None, None, None, None, 0.0, 0.0, 0.0, 0.0, "ABSTAIN")
    if action.action_id == "A4_PASSIVE_JOIN":
        entry = _entry_passive(episode, action, store, rows, config=config)
    else:
        entry = _entry_aggressive(
            episode, action, store, rows, slippage_ticks=slippage_ticks, config=config
        )
    if entry is None:
        reason = "PASSIVE_NO_FILL" if action.action_id == "A4_PASSIVE_JOIN" else "ACTION_NO_FILL"
        return _ScenarioExecution(reason, 0, None, None, None, None, 0.0, 0.0, 0.0, 0.0, reason)
    entry_row, quantity, entry_price, fill_status, ahead, consumed = entry
    direction = episode.direction
    market = episode.market
    tick = float(config.tick_size[market])
    point = float(config.micro_point_value[market])
    target_distance = direction * (episode.target_price - episode.raw_fill_price)
    stop_distance = -direction * (episode.stop_price - episode.raw_fill_price)
    target = entry_price + direction * target_distance
    stop = entry_price - direction * stop_distance
    local_rows = rows[np.searchsorted(store.decision_ns[rows], store.decision_ns[entry_row], side="left") :]
    same = local_rows[store.session[local_rows] == episode.session_id]
    horizon_ns = (
        int(store.decision_ns[int(same[-1])])
        if episode.maximum_horizon == "SESSION"
        else int(store.decision_ns[entry_row])
        + int(episode.maximum_horizon) * 60_000_000_000
    )
    same = same[store.decision_ns[same] <= horizon_ns]
    if len(same) == 0:
        return _ScenarioExecution("ACTION_NO_EXIT", 0, None, None, None, None, 0.0, 0.0, 0.0, 0.0, "NO_EXIT")
    prices = store.last_trade_price[same]
    target_hits = direction * (prices - target) >= 0
    stop_hits = direction * (prices - stop) <= 0
    trigger = target_hits | stop_hits
    reason = "MAXIMUM_HORIZON"
    exit_offset = len(same) - 1
    if np.any(trigger):
        exit_offset = int(np.flatnonzero(trigger)[0])
        reason = "TARGET" if bool(target_hits[exit_offset]) else "STOP"
    exit_row = int(same[exit_offset])
    # Every exit, including a target-triggered exit, pays the same frozen BBO
    # and adverse-slippage contract.  Entry and exit quantity are identical;
    # no residual is silently discarded because the 0031 depth is in minis.
    exit_quote = float(store.bid_price[exit_row] if direction > 0 else store.ask_price[exit_row])
    exit_price = float(exit_quote - direction * slippage_ticks * tick)
    gross = direction * (exit_price - entry_price) * point * quantity
    commission = float(config.commission_per_micro_round_trip_usd[market]) * quantity
    net = gross - commission
    mark_rows = same[: exit_offset + 1]
    liquidation_quotes = (
        store.bid_price[mark_rows] if direction > 0 else store.ask_price[mark_rows]
    ) - direction * slippage_ticks * tick
    path = direction * (liquidation_quotes - entry_price) * point * quantity - commission
    minimum = float(min(0.0, float(np.min(path)))) if len(path) else 0.0
    return _ScenarioExecution(
        fill_status,
        quantity,
        int(store.decision_ns[entry_row]),
        int(store.decision_ns[exit_row]),
        float(entry_price),
        float(exit_price),
        float(gross),
        commission,
        float(net),
        minimum,
        reason,
        ahead,
        consumed,
    )


def _evaluate_paired_actions_serial(
    episodes: Sequence[StructuralOpportunityEpisode],
    store: SparseStore,
    *,
    config: HybridPilotConfig,
) -> tuple[PairedActionOutcome, ...]:
    """Evaluate every frozen action on the identical structural opportunity."""

    lattice = freeze_action_lattice(config)
    rows_by_market = _market_rows(store)
    output: list[PairedActionOutcome] = []
    baseline_action = next(
        action for action in lattice if action.action_id == "A0_BASELINE_IMMEDIATE"
    )
    for episode in episodes:
        rows = rows_by_market.get(episode.market)
        if rows is None or len(rows) == 0:
            raise HybridPilotError(f"0031 execution context absent for {episode.market}")
        context_row, context_hash, join_lag, quality = _decision_context(
            episode, store, rows, config=config
        )
        pair_id = stable_hash(
            {"opportunity_id": episode.opportunity_id, "causal_fingerprint": episode.causal_fingerprint}
        )
        # A0 is executed through the same sealed 0031 context and cost model as
        # every overlay.  The immutable 0028 ledger remains a separate
        # development reference, never the paired comparator.
        a0_normal = _simulate_scenario(
            episode,
            baseline_action,
            store,
            rows,
            slippage_ticks=config.normal_adverse_slippage_ticks,
            config=config,
        )
        a0_stressed = _simulate_scenario(
            episode,
            baseline_action,
            store,
            rows,
            slippage_ticks=config.stressed_adverse_slippage_ticks,
            config=config,
        )
        for action in lattice:
            if action.action_id == baseline_action.action_id:
                normal, stressed = a0_normal, a0_stressed
            else:
                normal = _simulate_scenario(
                    episode,
                    action,
                    store,
                    rows,
                    slippage_ticks=config.normal_adverse_slippage_ticks,
                    config=config,
                )
                stressed = _simulate_scenario(
                    episode,
                    action,
                    store,
                    rows,
                    slippage_ticks=config.stressed_adverse_slippage_ticks,
                    config=config,
                )
            counterfactual_model = "0031_CAUSAL_BBO_MICRO_EXECUTION_V1"
            normal_payload = asdict(normal)
            stressed_payload = asdict(stressed)
            material = {
                "paired_group_id": pair_id,
                "opportunity_id": episode.opportunity_id,
                "action_hash": action.action_hash,
                "normal": normal_payload,
                "stressed": stressed_payload,
            }
            output.append(
                PairedActionOutcome(
                    paired_group_id=pair_id,
                    opportunity_id=episode.opportunity_id,
                    anchor_id=episode.anchor_id,
                    mechanism=episode.mechanism,
                    market=episode.market,
                    execution_market=episode.execution_market,
                    quantity_unit="MICRO_CONTRACT",
                    execution_book_quantity_unit="MINI_CONTRACT",
                    micro_per_mini_ratio=10,
                    session_id=episode.session_id,
                    role=episode.role,
                    direction=episode.direction,
                    action_id=action.action_id,
                    risk_tier=action.risk_tier,
                    promotion_eligible=action.promotion_eligible,
                    passive_side_lane=action.passive_side_lane,
                    causal_quality_score=quality,
                    joined_feature_hash=context_hash,
                    joined_decision_time_ns=int(store.decision_ns[context_row]),
                    feature_join_lag_ns=join_lag,
                    normal=normal_payload,
                    stressed=stressed_payload,
                    baseline_normal_net_pnl=a0_normal.net_pnl_usd,
                    baseline_stressed_net_pnl=a0_stressed.net_pnl_usd,
                    source_0028_normal_net_pnl=episode.baseline_normal_net_pnl,
                    source_0028_stressed_net_pnl=episode.baseline_stressed_net_pnl,
                    source_0028_normal_fill_price=episode.normal_fill_price,
                    source_0028_stressed_fill_price=episode.stressed_fill_price,
                    normal_delta_vs_a0_usd=float(normal.net_pnl_usd - a0_normal.net_pnl_usd),
                    stressed_delta_vs_a0_usd=float(stressed.net_pnl_usd - a0_stressed.net_pnl_usd),
                    normal_delta_vs_source_0028_usd=(
                        None
                        if episode.baseline_normal_net_pnl is None
                        else float(normal.net_pnl_usd - episode.baseline_normal_net_pnl)
                    ),
                    stressed_delta_vs_source_0028_usd=(
                        None
                        if episode.baseline_stressed_net_pnl is None
                        else float(stressed.net_pnl_usd - episode.baseline_stressed_net_pnl)
                    ),
                    development_fill_model_id=episode.source_fill_policy_id,
                    counterfactual_fill_model_id=counterfactual_model,
                    normal_fill_price_delta_vs_a0=(
                        None
                        if normal.fill_price is None or a0_normal.fill_price is None
                        else float(normal.fill_price - a0_normal.fill_price)
                    ),
                    stressed_fill_price_delta_vs_a0=(
                        None
                        if stressed.fill_price is None or a0_stressed.fill_price is None
                        else float(stressed.fill_price - a0_stressed.fill_price)
                    ),
                    normal_fill_price_delta_vs_source_0028=(
                        None
                        if normal.fill_price is None
                        else float(normal.fill_price - episode.normal_fill_price)
                    ),
                    stressed_fill_price_delta_vs_source_0028=(
                        None
                        if stressed.fill_price is None
                        else float(stressed.fill_price - episode.stressed_fill_price)
                    ),
                    causal_fingerprint=episode.causal_fingerprint,
                    outcome_hash=stable_hash(material),
                )
            )
    expected = len(episodes) * len(lattice)
    if len(output) != expected:
        raise HybridPilotError("paired action denominator drift")
    by_pair: dict[str, set[str]] = {}
    for row in output:
        by_pair.setdefault(row.paired_group_id, set()).add(f"{row.action_id}:{row.risk_tier:.2f}")
    expected_keys = {value.key for value in lattice}
    if any(keys != expected_keys for keys in by_pair.values()):
        raise HybridPilotError("same-anchor action lattice is incomplete")
    return tuple(output)


_FORK_STORE: SparseStore | None = None
_FORK_CONFIG: HybridPilotConfig | None = None


def _evaluate_paired_actions_fork_chunk(
    episodes: tuple[StructuralOpportunityEpisode, ...],
) -> tuple[PairedActionOutcome, ...]:
    if _FORK_STORE is None or _FORK_CONFIG is None:
        raise HybridPilotError("0033 fork worker was not initialized")
    return _evaluate_paired_actions_serial(
        episodes, _FORK_STORE, config=_FORK_CONFIG
    )


def evaluate_paired_actions(
    episodes: Sequence[StructuralOpportunityEpisode],
    store: SparseStore,
    *,
    config: HybridPilotConfig,
) -> tuple[PairedActionOutcome, ...]:
    """Evaluate paired actions with exactly two deterministic fork workers.

    The immutable NumPy store is inherited read-only by the workers.  Small
    fixture calls stay serial, while the real 72-opportunity campaign uses
    both CPU workers and restores the canonical episode/action order before
    returning.
    """

    if len(episodes) < 2:
        return _evaluate_paired_actions_serial(episodes, store, config=config)
    try:
        context = mp.get_context("fork")
    except ValueError as exc:  # pragma: no cover - Linux production contract
        raise HybridPilotError("0033 deterministic fork workers unavailable") from exc
    chunks = (tuple(episodes[::2]), tuple(episodes[1::2]))
    global _FORK_STORE, _FORK_CONFIG
    _FORK_STORE, _FORK_CONFIG = store, config
    try:
        with context.Pool(processes=config.cpu_worker_count) as pool:
            parts = pool.map(_evaluate_paired_actions_fork_chunk, chunks)
    finally:
        _FORK_STORE = None
        _FORK_CONFIG = None
    combined = [row for part in parts for row in part]
    episode_order = {
        row.opportunity_id: ordinal for ordinal, row in enumerate(episodes)
    }
    action_order = {
        action.key: ordinal
        for ordinal, action in enumerate(freeze_action_lattice(config))
    }
    combined.sort(
        key=lambda row: (
            episode_order[row.opportunity_id],
            action_order[f"{row.action_id}:{row.risk_tier:.2f}"],
        )
    )
    expected = len(episodes) * len(freeze_action_lattice(config))
    if len(combined) != expected:
        raise HybridPilotError("0033 parallel paired-action denominator drift")
    return tuple(combined)


def _aggregate(rows: Sequence[PairedActionOutcome]) -> dict[str, Any]:
    normal = [float(row.normal["net_pnl_usd"]) for row in rows]
    stressed = [float(row.stressed["net_pnl_usd"]) for row in rows]
    return {
        "opportunity_count": len(rows),
        "normal_fill_count": sum(int(row.normal["quantity"]) > 0 for row in rows),
        "stressed_fill_count": sum(int(row.stressed["quantity"]) > 0 for row in rows),
        "normal_net_usd": float(math.fsum(normal)),
        "stressed_net_usd": float(math.fsum(stressed)),
        "normal_delta_vs_a0_usd": float(math.fsum(row.normal_delta_vs_a0_usd for row in rows)),
        "stressed_delta_vs_a0_usd": float(math.fsum(row.stressed_delta_vs_a0_usd for row in rows)),
        "normal_delta_vs_source_0028_usd": float(
            math.fsum(
                row.normal_delta_vs_source_0028_usd
                for row in rows
                if row.normal_delta_vs_source_0028_usd is not None
            )
        ),
        "stressed_delta_vs_source_0028_usd": float(
            math.fsum(
                row.stressed_delta_vs_source_0028_usd
                for row in rows
                if row.stressed_delta_vs_source_0028_usd is not None
            )
        ),
        "source_0028_noncensored_count": sum(
            row.source_0028_normal_net_pnl is not None
            and row.source_0028_stressed_net_pnl is not None
            for row in rows
        ),
        "minimum_normal_unrealized_pnl_usd": min((float(row.normal["minimum_unrealized_pnl_usd"]) for row in rows), default=0.0),
        "minimum_stressed_unrealized_pnl_usd": min((float(row.stressed["minimum_unrealized_pnl_usd"]) for row in rows), default=0.0),
    }


def _chronological_account_path(
    rows: Sequence[PairedActionOutcome],
    scenario: str,
    *,
    config: HybridPilotConfig,
) -> dict[str, Any]:
    """Conservatively combine event executions into one chronological account.

    The compact execution records retain the worst unrealized point for each
    trade, not its exact timestamp.  At entry we therefore charge that worst
    unrealized value until exit.  Concurrent trades are summed, which is a
    conservative realized-plus-unrealized MLL path and never a standalone
    per-trade shortcut.
    """

    if scenario not in {"normal", "stressed"}:
        raise HybridPilotError("unknown account scenario")
    ordered = sorted(rows, key=lambda row: (row.session_id, row.opportunity_id))
    timeline: list[tuple[int, int, PairedActionOutcome]] = []
    fills: list[PairedActionOutcome] = []
    for row in ordered:
        execution = row.normal if scenario == "normal" else row.stressed
        if int(execution["quantity"]) <= 0:
            continue
        if execution["fill_time_ns"] is None or execution["exit_time_ns"] is None:
            raise HybridPilotError("filled action lacks a chronological exit")
        fills.append(row)
        # Exit is processed before a new entry at the same timestamp.
        timeline.append((int(execution["fill_time_ns"]), 1, row))
        timeline.append((int(execution["exit_time_ns"]), 0, row))
    timeline.sort(key=lambda item: (item[0], item[1], item[2].opportunity_id))

    realized = 0.0
    trailing_high = 0.0
    minimum_buffer = float(config.account_mll_usd)
    active_unrealized: dict[str, float] = {}
    mll_breached = False
    target_reached = False
    target_time_ns: int | None = None
    maximum_concurrent = 0
    daily: dict[str, dict[str, float]] = {
        session: {"net": 0.0, "costs": 0.0}
        for session in sorted({row.session_id for row in ordered})
    }
    for timestamp, event_type, row in timeline:
        execution = row.normal if scenario == "normal" else row.stressed
        session = row.session_id
        if event_type == 0:
            active_unrealized.pop(row.opportunity_id, None)
            realized += float(execution["net_pnl_usd"])
            daily[session]["net"] += float(execution["net_pnl_usd"])
            daily[session]["costs"] += float(execution["costs_usd"])
        else:
            active_unrealized[row.opportunity_id] = float(
                execution["minimum_unrealized_pnl_usd"]
            )
        maximum_concurrent = max(maximum_concurrent, len(active_unrealized))
        unrealized = float(math.fsum(active_unrealized.values()))
        equity = realized + unrealized
        trailing_high = max(trailing_high, realized, equity)
        floor = trailing_high - config.account_mll_usd
        buffer = equity - floor
        minimum_buffer = min(minimum_buffer, buffer)
        if buffer < -1e-9:
            mll_breached = True
        if not target_reached and realized >= config.account_target_usd:
            target_reached = True
            target_time_ns = timestamp

    positive_events = [
        float((row.normal if scenario == "normal" else row.stressed)["net_pnl_usd"])
        for row in fills
        if float((row.normal if scenario == "normal" else row.stressed)["net_pnl_usd"]) > 0.0
    ]
    positive_total = float(math.fsum(positive_events))
    concentration = max(positive_events, default=0.0) / positive_total if positive_total > 0 else 0.0
    positive_days = [value["net"] for value in daily.values() if value["net"] > 0.0]
    positive_day_total = float(math.fsum(positive_days))
    consistency_ratio = (
        max(positive_days, default=0.0) / positive_day_total
        if positive_day_total > 0.0
        else 0.0
    )
    consistency_ok = (not target_reached) or consistency_ratio <= 0.5 + 1e-12
    sessions = sorted({row.session_id for row in ordered})
    days_to_target = None
    if target_time_ns is not None and sessions:
        target_session = datetime.fromtimestamp(target_time_ns / 1e9, tz=UTC).date().isoformat()
        days_to_target = float(
            1
            + sum(
                1
                for session in sessions
                if session < target_session
            )
        )
    denominator = len(rows)
    return {
        "opportunity_denominator": denominator,
        "fill_count": len(fills),
        "activity_rate": len(fills) / denominator if denominator else 0.0,
        "net_pnl_usd": realized,
        "costs_usd": float(
            math.fsum(
                float((row.normal if scenario == "normal" else row.stressed)["costs_usd"])
                for row in fills
            )
        ),
        "target_reached": target_reached,
        "target_progress": realized / config.account_target_usd,
        "mll_breached": mll_breached,
        "minimum_mll_buffer_usd": minimum_buffer,
        "minimum_mll_buffer_fraction": minimum_buffer / config.account_mll_usd,
        "maximum_concurrent_positions": maximum_concurrent,
        "opportunity_profit_concentration": concentration,
        "consistency_ratio": consistency_ratio,
        "consistency_ok": consistency_ok,
        "days_to_target": days_to_target,
        "daily_path": [
            {
                "session_id": session,
                "net_pnl_usd": value["net"],
                "costs_usd": value["costs"],
            }
            for session, value in sorted(daily.items())
        ],
    }


def _policy_role_result(
    rows: Sequence[PairedActionOutcome],
    *,
    config: HybridPilotConfig,
) -> dict[str, Any]:
    summary = _aggregate(rows)
    by_anchor = {
        anchor_id: [row for row in rows if row.anchor_id == anchor_id]
        for anchor_id in sorted({row.anchor_id for row in rows})
    }
    anchor_paths = {
        anchor_id: {
            "normal": _chronological_account_path(values, "normal", config=config),
            "stressed": _chronological_account_path(values, "stressed", config=config),
        }
        for anchor_id, values in by_anchor.items()
    }

    def independent_summary(scenario: str) -> dict[str, Any]:
        key = "normal" if scenario == "normal" else "stressed"
        fills = [
            row
            for row in rows
            if int((row.normal if key == "normal" else row.stressed)["quantity"]) > 0
        ]
        positive = [
            float((row.normal if key == "normal" else row.stressed)["net_pnl_usd"])
            for row in fills
            if float((row.normal if key == "normal" else row.stressed)["net_pnl_usd"]) > 0.0
        ]
        positive_sum = float(math.fsum(positive))
        paths = [value[key] for value in anchor_paths.values()]
        return {
            # These are independent sleeve paths.  They are not summed into a
            # shared account because 0033 did not preregister a conflict or
            # concurrent-exposure governor for overlapping anchors.
            "evidence_scope": "INDEPENDENT_ANCHOR_SLEEVES_NO_BOOK_ASSEMBLY",
            "opportunity_denominator": len(rows),
            "fill_count": len(fills),
            "activity_rate": len(fills) / len(rows) if rows else 0.0,
            "net_pnl_usd": float(
                math.fsum(
                    float((row.normal if key == "normal" else row.stressed)["net_pnl_usd"])
                    for row in rows
                )
            ),
            "costs_usd": float(
                math.fsum(
                    float((row.normal if key == "normal" else row.stressed)["costs_usd"])
                    for row in fills
                )
            ),
            "target_reached_anchor_count": sum(bool(path["target_reached"]) for path in paths),
            "mll_breached": any(bool(path["mll_breached"]) for path in paths),
            "minimum_mll_buffer_usd": min(
                (float(path["minimum_mll_buffer_usd"]) for path in paths),
                default=config.account_mll_usd,
            ),
            "opportunity_profit_concentration": (
                max(positive, default=0.0) / positive_sum if positive_sum > 0.0 else 0.0
            ),
            "consistency_ok": all(bool(path["consistency_ok"]) for path in paths),
            "anchor_path_count": len(paths),
        }

    return {
        **summary,
        "normal_account": independent_summary("normal"),
        "stressed_account": independent_summary("stressed"),
        "anchor_account_paths": anchor_paths,
        "overlapping_anchor_book_assembly_authorized": False,
    }


def freeze_and_evaluate_policies(
    outcomes: Sequence[PairedActionOutcome],
    *,
    config: HybridPilotConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Freeze 20 global contextual overlays from discovery data only.

    Each overlay uses the same causal L1/L2 quality score for every anchor.
    Below its discovery-frozen threshold it selects A1 (abstain); otherwise it
    selects one fixed promotable execution action/risk tier.  This prevents
    anchor-by-anchor outcome selection on the tiny held-out sessions.
    """

    by_key = {
        (row.opportunity_id, row.action_id, round(row.risk_tier, 8)): row
        for row in outcomes
    }

    def paired_stressed_attribution(
        selected_rows: Sequence[PairedActionOutcome], role: str
    ) -> dict[str, Any]:
        attribution = {
            "avoiding_trade_usd": 0.0,
            "timing_usd": 0.0,
            "execution_usd": 0.0,
            "risk_tier_usd": 0.0,
        }
        improved = harmed = abstained = 0
        role_rows = [row for row in selected_rows if row.role == role]
        total = 0.0
        for row in role_rows:
            baseline = by_key[
                (row.opportunity_id, "A0_BASELINE_IMMEDIATE", 1.0)
            ]
            selected_net = float(row.stressed["net_pnl_usd"])
            baseline_net = float(baseline.stressed["net_pnl_usd"])
            delta = selected_net - baseline_net
            total += delta
            improved += int(delta > 1e-12)
            harmed += int(delta < -1e-12)
            abstained += int(row.action_id == "A1_ABSTAIN")
            if row.action_id == "A1_ABSTAIN":
                attribution["avoiding_trade_usd"] += delta
                continue
            unit = by_key.get((row.opportunity_id, row.action_id, 1.0), row)
            unit_net = float(unit.stressed["net_pnl_usd"])
            action_delta = unit_net - baseline_net
            risk_delta = selected_net - unit_net
            if row.action_id == "A5_EARLY_INVALIDATION":
                attribution["avoiding_trade_usd"] += action_delta
            elif row.action_id == "A2_WAIT_CONFIRM":
                attribution["timing_usd"] += action_delta
            elif row.action_id == "A3_PULLBACK_MARKETABLE_LIMIT":
                attribution["execution_usd"] += action_delta
            attribution["risk_tier_usd"] += risk_delta
        reconciled = float(math.fsum(attribution.values()))
        if not math.isclose(reconciled, total, rel_tol=0.0, abs_tol=1e-8):
            raise HybridPilotError("0033 paired uplift attribution does not reconcile")
        denominator = len(role_rows)
        return {
            "opportunity_count": denominator,
            "paired_stressed_uplift_usd": float(total),
            **{key: float(value) for key, value in attribution.items()},
            "abstained_fraction": abstained / denominator if denominator else 0.0,
            "improved_fraction": improved / denominator if denominator else 0.0,
            "harmed_fraction": harmed / denominator if denominator else 0.0,
            "reconciliation_delta_usd": float(total - reconciled),
        }
    opportunity_rows: dict[str, PairedActionOutcome] = {}
    for row in outcomes:
        opportunity_rows.setdefault(row.opportunity_id, row)
    discovery_quality = sorted(
        row.causal_quality_score
        for row in opportunity_rows.values()
        if row.role == "DISCOVERY"
    )
    if not discovery_quality:
        raise HybridPilotError("0033 discovery quality denominator is zero")
    quantile_contract = (0.35, 0.65)
    thresholds = [float(np.quantile(discovery_quality, value)) for value in quantile_contract]
    active_frontier = [("A0_BASELINE_IMMEDIATE", 1.0)] + [
        (action_id, tier)
        for action_id in (
            "A2_WAIT_CONFIRM",
            "A3_PULLBACK_MARKETABLE_LIMIT",
            "A5_EARLY_INVALIDATION",
        )
        for tier in config.risk_tiers
    ]
    candidate_rows: list[dict[str, Any]] = []
    for quantile_value, threshold in zip(quantile_contract, thresholds):
        for action_id, tier in active_frontier:
            fingerprint = stable_hash(
                {
                    "campaign_id": config.campaign_id,
                    "mapping": "GLOBAL_CAUSAL_QUALITY_THRESHOLD_V1",
                    "quality_quantile": quantile_value,
                    "quality_threshold": threshold,
                    "below_threshold_action": "A1_ABSTAIN",
                    "active_action": action_id,
                    "risk_tier": tier,
                    "selection_role": "DISCOVERY_ONLY",
                }
            )
            candidate_rows.append(
                {
                    "candidate_id": f"overlay_{fingerprint[:20]}",
                    "policy_fingerprint": fingerprint,
                    "mapping_scope": "GLOBAL_ALL_22_ANCHORS",
                    "quality_score_id": "CAUSAL_L1_L2_ASOF_SCORE_V1",
                    "quality_quantile": quantile_value,
                    "quality_threshold": threshold,
                    "below_threshold_action": "A1_ABSTAIN",
                    "active_action_id": action_id,
                    "active_risk_tier": tier,
                    "threshold_discovery_denominator": len(discovery_quality),
                    "selection_uses_validation_or_final": False,
                    "deployable_l1_l2": True,
                }
            )
    if len(candidate_rows) != min(20, config.maximum_policies):
        raise HybridPilotError("0033 frozen global overlay count drift")

    policy_results: list[dict[str, Any]] = []
    for ordinal, frozen in enumerate(candidate_rows, start=1):
        selected_rows: list[PairedActionOutcome] = []
        selections: list[dict[str, Any]] = []
        for opportunity_id, reference in sorted(
            opportunity_rows.items(),
            key=lambda item: (item[1].session_id, item[1].opportunity_id),
        ):
            if reference.causal_quality_score < float(frozen["quality_threshold"]):
                action_id, tier = "A1_ABSTAIN", 0.0
            else:
                action_id = str(frozen["active_action_id"])
                tier = float(frozen["active_risk_tier"])
            selected = by_key.get((opportunity_id, action_id, round(tier, 8)))
            if selected is None:
                raise HybridPilotError("0033 contextual overlay action is absent")
            selected_rows.append(selected)
            selections.append(
                {
                    "opportunity_id": opportunity_id,
                    "role": selected.role,
                    "quality_score": selected.causal_quality_score,
                    "selected_action_id": action_id,
                    "selected_risk_tier": tier,
                    "outcome_hash": selected.outcome_hash,
                }
            )
        by_role = {
            role: _policy_role_result(
                [row for row in selected_rows if row.role == role], config=config
            )
            for role in SESSION_ROLES
        }
        paired_attribution = {
            role: paired_stressed_attribution(selected_rows, role)
            for role in SESSION_ROLES
        }
        heldout_roles = ("VALIDATION", "FINAL_DEVELOPMENT")
        heldout_improvement = all(
            by_role[role][f"{scenario}_delta_vs_a0_usd"] > 0.0
            for role in heldout_roles
            for scenario in ("normal", "stressed")
        )
        heldout_positive = all(
            by_role[role][f"{scenario}_account"]["net_pnl_usd"] > 0.0
            for role in heldout_roles
            for scenario in ("normal", "stressed")
        )
        heldout_active = all(
            by_role[role][f"{scenario}_account"]["activity_rate"] >= 0.20
            and by_role[role][f"{scenario}_account"]["fill_count"] >= 2
            for role in heldout_roles
            for scenario in ("normal", "stressed")
        )
        heldout_mll = all(
            not by_role[role][f"{scenario}_account"]["mll_breached"]
            for role in heldout_roles
            for scenario in ("normal", "stressed")
        )
        heldout_concentration = all(
            by_role[role][f"{scenario}_account"]["opportunity_profit_concentration"]
            <= config.maximum_opportunity_profit_concentration + 1e-12
            for role in heldout_roles
            for scenario in ("normal", "stressed")
        )
        defensible_anchor_sleeves: list[dict[str, Any]] = []
        for anchor_id in sorted({row.anchor_id for row in selected_rows}):
            anchor_rows = [row for row in selected_rows if row.anchor_id == anchor_id]
            role_summaries = {
                role: _aggregate([row for row in anchor_rows if row.role == role])
                for role in heldout_roles
            }
            if all(
                role_summaries[role]["opportunity_count"] > 0
                and role_summaries[role]["normal_net_usd"] > 0.0
                and role_summaries[role]["stressed_net_usd"] > 0.0
                and role_summaries[role]["normal_delta_vs_a0_usd"] > 0.0
                and role_summaries[role]["stressed_delta_vs_a0_usd"] > 0.0
                for role in heldout_roles
            ):
                defensible_anchor_sleeves.append(
                    {
                        "anchor_id": anchor_id,
                        "mechanism": anchor_rows[0].mechanism,
                        "role_results": role_summaries,
                    }
                )
        mechanisms = sorted(
            {row["mechanism"] for row in defensible_anchor_sleeves}
        )
        policy_results.append(
            {
                "policy_id": f"hybrid_0033_{ordinal:02d}_{frozen['policy_fingerprint'][:16]}",
                "policy_fingerprint": frozen["policy_fingerprint"],
                "mapping_scope": frozen["mapping_scope"],
                "mechanism": str(frozen["active_action_id"]),
                "active_action_id": frozen["active_action_id"],
                "active_risk_tier": frozen["active_risk_tier"],
                "quality_quantile": frozen["quality_quantile"],
                "quality_threshold": frozen["quality_threshold"],
                "below_threshold_action": "A1_ABSTAIN",
                "frozen_from_role": "DISCOVERY",
                "selection_uses_validation_or_final": False,
                "deployable_l1_l2": bool(frozen["deployable_l1_l2"]),
                "role_results": by_role,
                "paired_stressed_uplift_attribution": paired_attribution,
                "selected_actions": selections,
                "defensible_heldout_anchor_sleeves": defensible_anchor_sleeves,
                "benefiting_heldout_anchor_mechanisms": mechanisms,
                "validation_and_final_positive": heldout_positive,
                "validation_and_final_improve_a0": heldout_improvement,
                "validation_and_final_non_inactive": heldout_active,
                "validation_and_final_mll_safe": heldout_mll,
                "validation_and_final_concentration_safe": heldout_concentration,
                "heldout_denominators_nonzero": all(
                    by_role[role]["opportunity_count"] > 0 for role in heldout_roles
                ),
            }
        )
    return candidate_rows, policy_results


def decide_hybrid_gate(
    policy_results: Sequence[Mapping[str, Any]],
    *,
    config: HybridPilotConfig,
) -> tuple[str, dict[str, Any]]:
    defensible = [
        row
        for row in policy_results
        if bool(row["validation_and_final_positive"])
        and bool(row["validation_and_final_improve_a0"])
        and bool(row["validation_and_final_non_inactive"])
        and bool(row["validation_and_final_mll_safe"])
        and bool(row["validation_and_final_concentration_safe"])
        and bool(row["heldout_denominators_nonzero"])
        and bool(row["deployable_l1_l2"])
    ]
    families = {
        str(mechanism)
        for row in defensible
        for mechanism in row["benefiting_heldout_anchor_mechanisms"]
    }
    if len(defensible) >= config.minimum_green_policies and len(families) >= config.minimum_green_anchor_families:
        status = "HYBRID_OVERLAY_GREEN"
    elif any(bool(row["validation_and_final_improve_a0"]) for row in policy_results):
        status = "HYBRID_OVERLAY_WEAK"
    else:
        status = "HYBRID_OVERLAY_FALSIFIED"
    checks = {
        "defensible_policy_count": len(defensible),
        "defensible_policy_ids": [str(row["policy_id"]) for row in defensible],
        "distinct_defensible_mechanism_count": len(families),
        "validation_final_positive_required": True,
        "paired_improvement_over_a0_required": True,
        "chronological_mll_no_breach_required": True,
        "maximum_opportunity_profit_concentration": config.maximum_opportunity_profit_concentration,
        "minimum_activity_rate_per_heldout_role": 0.20,
        "minimum_fills_per_heldout_role": 2,
        "heldout_denominators": {
            role: sorted(
                {
                    int(row["role_results"][role]["opportunity_count"])
                    for row in policy_results
                }
            )
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        },
        "passive_side_lane_promotion_allowed": False,
        "thresholds_selected_after_results": False,
    }
    return status, checks


def _passive_side_lane_summary(
    outcomes: Sequence[PairedActionOutcome],
) -> dict[str, Any]:
    passive = [row for row in outcomes if row.action_id == "A4_PASSIVE_JOIN"]
    tiers = sorted({row.risk_tier for row in passive})
    choices: list[dict[str, Any]] = []
    for tier in tiers:
        rows = [
            row
            for row in passive
            if math.isclose(row.risk_tier, tier, rel_tol=0.0, abs_tol=1e-12)
        ]
        choices.append(
            {
                "risk_tier": tier,
                "discovery": _aggregate(
                    [row for row in rows if row.role == "DISCOVERY"]
                ),
            }
        )
    choices.sort(
        key=lambda row: (
            float(row["discovery"]["stressed_net_usd"]),
            -float(row["risk_tier"]),
        ),
        reverse=True,
    )
    selected_tier = float(choices[0]["risk_tier"]) if choices else 1.0
    selected = [
        row
        for row in passive
        if math.isclose(
            row.risk_tier, selected_tier, rel_tol=0.0, abs_tol=1e-12
        )
    ]
    role_results = {
        role: _aggregate([row for row in selected if row.role == role])
        for role in SESSION_ROLES
    }
    heldout_positive = all(
        role_results[role][f"{scenario}_net_usd"] > 0.0
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        for scenario in ("normal", "stressed")
    )
    return {
        "status": (
            "PASSIVE_EXECUTION_SALVAGE_DIAGNOSTIC_POSITIVE"
            if heldout_positive
            else "PASSIVE_EXECUTION_SALVAGE_FALSIFIED"
        ),
        "deployability_tier": "MBO_TEACHER_ONLY",
        "promotion_allowed": False,
        "selection_role": "DISCOVERY_ONLY",
        "selected_risk_tier": selected_tier,
        "role_results": role_results,
        "filled_path_count": sum(
            int(row.stressed["quantity"]) > 0 for row in selected
        ),
        "opportunity_count": len(selected),
        "micro_order_observed_on_mini_book": True,
        "queue_reality_for_execution_market_proven": False,
    }


def _ns_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat().replace(
        "+00:00", "Z"
    )


def _canonical_evidence_material(
    *,
    config: HybridPilotConfig,
    store: SparseStore,
    source_episodes: Sequence[StructuralOpportunityEpisode],
    outcomes: Sequence[PairedActionOutcome],
    policies: Sequence[Mapping[str, Any]],
    provenance: Mapping[str, Any],
    pilot_status: str,
    gate_checks: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Project 0033 material into the mandatory eight-dataset contract."""

    source_by_opportunity = {row.opportunity_id: row for row in source_episodes}
    outcome_by_hash = {row.outcome_hash: row for row in outcomes}
    configuration_hash = (
        config.manifest_hash
        if len(config.manifest_hash) == 64
        and all(char in "0123456789abcdef" for char in config.manifest_hash)
        else stable_hash(asdict(config))
    )
    data_fingerprints = {
        **{f"0031:{name}": str(digest) for name, digest in store.source_hashes.items()},
        "0028:population": str(provenance["population_sha256"]),
        "0028:clean_result": str(provenance["clean_result_sha256"]),
        **{
            f"0028:event:{name}": str(digest)
            for name, digest in provenance["event_file_sha256"].items()
        },
    }
    if any(
        len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
        for value in data_fingerprints.values()
    ):
        raise HybridPilotError("0033 immutable source fingerprint is not SHA-256")

    signals: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    memberships: list[dict[str, Any]] = []
    account_daily: list[dict[str, Any]] = []
    episode_rows: list[dict[str, Any]] = []
    policy_fingerprints: dict[str, str] = {}
    component_fingerprints: dict[str, str] = {}
    required_episode_keys: list[dict[str, str]] = []

    for policy in policies:
        policy_id = str(policy["policy_id"])
        selected = [
            outcome_by_hash[str(value["outcome_hash"])]
            for value in policy["selected_actions"]
        ]
        executed_anchors = {
            row.anchor_id
            for row in selected
            if int(row.normal["quantity"]) > 0 or int(row.stressed["quantity"]) > 0
        }
        if not executed_anchors:
            continue
        policy_fingerprints[policy_id] = str(policy["policy_fingerprint"])
        component_ids: dict[str, str] = {}
        for anchor_id in sorted(executed_anchors):
            component_id = f"{policy_id}.{anchor_id}"
            component_ids[anchor_id] = component_id
            anchor = next(row for row in source_episodes if row.anchor_id == anchor_id)
            component_fingerprints[component_id] = stable_hash(
                {
                    "policy_fingerprint": policy["policy_fingerprint"],
                    "anchor_fingerprint": anchor.anchor_fingerprint,
                    "mapping": "GLOBAL_CAUSAL_QUALITY_THRESHOLD_V1",
                }
            )
            memberships.append(
                {
                    "campaign_id": config.campaign_id,
                    "policy_id": policy_id,
                    "component_id": component_id,
                    "risk_allocation": float(policy["active_risk_tier"]),
                    "component_role": anchor.mechanism,
                }
            )

        for row in selected:
            component_id = component_ids.get(row.anchor_id)
            if component_id is None:
                continue
            source = source_by_opportunity[row.opportunity_id]
            signal_id = stable_hash(
                {"policy_id": policy_id, "opportunity_id": row.opportunity_id}
            )
            signals.append(
                {
                    "campaign_id": config.campaign_id,
                    "component_id": component_id,
                    "signal_id": signal_id,
                    "event_time": _ns_iso(source.decision_time_ns),
                    "market": source.market,
                    "contract": source.execution_market,
                    "timeframe": source.timeframe,
                    "signal": row.direction,
                    "sizing": float(row.normal["quantity"]),
                    "stop": source.stop_price,
                    "target": source.target_price,
                    "veto": int(row.normal["quantity"]) <= 0,
                    "component_role": source.mechanism,
                    "available_at": _ns_iso(source.available_at_ns),
                    "decision_time": _ns_iso(source.decision_time_ns),
                    "action_id": row.action_id,
                    "risk_tier": row.risk_tier,
                    "fill_policy_id": row.counterfactual_fill_model_id,
                    "fill_policy_hash": stable_hash(row.counterfactual_fill_model_id),
                    "execution_market": source.execution_market,
                    "micro_per_mini_ratio": row.micro_per_mini_ratio,
                }
            )
            execution = row.normal if int(row.normal["quantity"]) > 0 else row.stressed
            if int(execution["quantity"]) <= 0:
                continue
            trade_id = signal_id
            fill_price = float(execution["fill_price"])
            target_distance = row.direction * (source.target_price - source.raw_fill_price)
            stop_distance = -row.direction * (source.stop_price - source.raw_fill_price)
            shifted_target = fill_price + row.direction * target_distance
            shifted_stop = fill_price - row.direction * stop_distance
            common = {
                "campaign_id": config.campaign_id,
                "component_id": component_id,
                "trade_id": trade_id,
            }
            entries.append(
                {
                    **common,
                    "entry_time": _ns_iso(int(execution["fill_time_ns"])),
                    "market": source.market,
                    "contract": source.execution_market,
                    "side": "LONG" if row.direction > 0 else "SHORT",
                    "quantity": float(execution["quantity"]),
                    "entry_price": fill_price,
                    "sizing": float(execution["quantity"]),
                    "stop_price": shifted_stop,
                    "target_price": shifted_target,
                }
            )
            exits.append(
                {
                    **common,
                    "exit_time": _ns_iso(int(execution["exit_time_ns"])),
                    "exit_price": float(execution["exit_price"]),
                    "exit_reason": str(execution["exit_reason"]),
                }
            )
            trades.append(
                {
                    **common,
                    "entry_time": _ns_iso(int(execution["fill_time_ns"])),
                    "exit_time": _ns_iso(int(execution["exit_time_ns"])),
                    "market": source.market,
                    "contract": source.execution_market,
                    "side": "LONG" if row.direction > 0 else "SHORT",
                    "quantity": float(execution["quantity"]),
                    "entry_price": fill_price,
                    "exit_price": float(execution["exit_price"]),
                    "gross_pnl": float(execution["gross_pnl_usd"]),
                    "costs": float(execution["costs_usd"]),
                    "net_pnl": float(execution["net_pnl_usd"]),
                }
            )

        for anchor_id in sorted({row.anchor_id for row in selected}):
            for role in SESSION_ROLES:
                role_rows = [
                    row
                    for row in selected
                    if row.anchor_id == anchor_id and row.role == role
                ]
                if not role_rows:
                    continue
                episode_id = f"{policy_id}.{anchor_id}.{role}"
                required_episode_keys.append(
                    {
                        "policy_id": policy_id,
                        "episode_id": episode_id,
                        "horizon": "ROLE_WINDOW",
                    }
                )
                component_id = component_ids.get(anchor_id)
                for scenario_key, scenario_name in (
                    ("normal", "NORMAL"),
                    ("stressed", "STRESSED_1_5X"),
                ):
                    path = _chronological_account_path(
                        role_rows, scenario_key, config=config
                    )
                    terminal = (
                        "MLL_BREACHED"
                        if path["mll_breached"]
                        else "TARGET_REACHED"
                        if path["target_reached"]
                        else "OPERATIONAL_HORIZON_NOT_REACHED"
                    )
                    episode_rows.append(
                        {
                            "campaign_id": config.campaign_id,
                            "policy_id": policy_id,
                            "episode_id": episode_id,
                            "episode_start": f"{min(row.session_id for row in role_rows)}T00:00:00Z",
                            "horizon": "ROLE_WINDOW",
                            "temporal_block": role,
                            "duration_trading_days": len(path["daily_path"]),
                            "target_reached": bool(path["target_reached"]),
                            "mll_breached": bool(path["mll_breached"]),
                            "censored_state": terminal == "OPERATIONAL_HORIZON_NOT_REACHED",
                            "cost_scenario": scenario_name,
                            "costs": float(path["costs_usd"]),
                            "net_pnl": float(path["net_pnl_usd"]),
                            "target_progress": float(path["target_progress"]),
                            "minimum_mll_buffer": float(path["minimum_mll_buffer_usd"]),
                            "consistency_ok": bool(path["consistency_ok"]),
                            "days_to_target": path["days_to_target"],
                            "failure_vector": [] if path["target_reached"] else [terminal],
                            "terminal_state": terminal,
                        }
                    )
                    cumulative = 0.0
                    trailing_high = 0.0
                    running_minimum = float(config.account_mll_usd)
                    for day in path["daily_path"]:
                        cumulative += float(day["net_pnl_usd"])
                        trailing_high = max(trailing_high, cumulative)
                        equity = 150_000.0 + cumulative
                        mll = 150_000.0 + trailing_high - config.account_mll_usd
                        buffer = equity - mll
                        running_minimum = min(
                            running_minimum,
                            buffer,
                            float(path["minimum_mll_buffer_usd"]),
                        )
                        attribution = (
                            {}
                            if component_id is None
                            else {component_id: float(day["net_pnl_usd"])}
                        )
                        account_daily.append(
                            {
                                "campaign_id": config.campaign_id,
                                "policy_id": policy_id,
                                "episode_id": episode_id,
                                "trading_day": day["session_id"],
                                "cost_scenario": scenario_name,
                                "horizon": "ROLE_WINDOW",
                                "realized_pnl": cumulative,
                                "unrealized_pnl": 0.0,
                                "daily_pnl": float(day["net_pnl_usd"]),
                                "equity": equity,
                                "mll": mll,
                                "mll_buffer": buffer,
                                "minimum_mll_buffer": running_minimum,
                                "consistency": float(path["consistency_ratio"]),
                                "target_progress": cumulative / config.account_target_usd,
                                "costs": float(day["costs_usd"]),
                                "conflicts": [],
                                "consistency_ok": bool(path["consistency_ok"]),
                                "exposure": {},
                                "component_attribution": attribution,
                            }
                        )

    if source_episodes and (not policy_fingerprints or not component_fingerprints):
        raise HybridPilotError("0033 canonical evidence has no executable policy")
    decision_ns = getattr(store, "decision_ns", ())
    created_at = (
        _ns_iso(int(max(decision_ns)))
        if len(decision_ns)
        else "1970-01-01T00:00:00Z"
    )
    identity = {
        "campaign_id": config.campaign_id,
        "grammar_id": "global_causal_quality_hybrid_overlay_v1",
        "policy_fingerprints": policy_fingerprints,
        "component_fingerprints": component_fingerprints,
        "source_commit": config.source_commit,
        "data_fingerprints": data_fingerprints,
        "configuration_sha256": configuration_hash,
        "seeds": [33_0033],
        "created_at_utc": created_at,
        "expected_coverage": {
            "policy_ids": list(policy_fingerprints),
            "component_ids": list(component_fingerprints),
            "required_episode_keys": required_episode_keys,
            "allowed_horizons": ["ROLE_WINDOW"],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
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
        "episodes": episode_rows,
        "provenance": [
            {
                "campaign_id": config.campaign_id,
                "validator_version": HYBRID_PILOT_VERSION,
                "replay_version": "0031_CAUSAL_BBO_MICRO_EXECUTION_V1",
                "market_data_role": "CHRONOLOGICAL_3_1_1_DEVELOPMENT",
                "access_ledger_sha256": stable_hash(data_fingerprints),
                "reconstruction_flag": False,
                "immutable_checksums": {
                    "configuration": configuration_hash,
                    **{f"data:{name}": digest for name, digest in data_fingerprints.items()},
                },
                "recorded_at_utc": created_at,
            }
        ],
    }
    compact = {
        "campaign_summary": {
            "pilot_status": pilot_status,
            "structural_opportunity_count": len(source_episodes),
            "source_0028_noncensored_count": sum(
                not row.source_reference_censored for row in source_episodes
            ),
            "hybrid_evaluable_count": len(source_episodes),
            "global_overlay_count": len(policies),
            "evidence_policy_count": len(policy_fingerprints),
            "gate_checks": dict(gate_checks),
        },
        "failure_vectors": {
            "NO_DEFENSIBLE_PAIRED_POLICY": int(
                not int(gate_checks["defensible_policy_count"])
            ),
            "CAUSALITY_DEFECT": 0,
        },
        "pareto_archive": [
            {
                "policy_id": row["policy_id"],
                "active_action_id": row["active_action_id"],
                "active_risk_tier": row["active_risk_tier"],
                "quality_threshold": row["quality_threshold"],
                "validation": row["role_results"]["VALIDATION"],
                "final_development": row["role_results"]["FINAL_DEVELOPMENT"],
            }
            for row in policies
        ],
        "next_campaign_recommendations": {
            "action": (
                "FREEZE_HYBRID_SURVIVORS_FOR_EXPANDED_DEVELOPMENT"
                if pilot_status == "HYBRID_OVERLAY_GREEN"
                else "PRESERVE_INFORMATION_AND_STOP_BOUNDED_HYBRID_OVERLAY"
                if pilot_status == "HYBRID_OVERLAY_WEAK"
                else "TOMBSTONE_EXACT_HYBRID_OVERLAY_CLASS"
            ),
            "new_data_purchase_authorized": False,
            "q4_access_authorized": False,
        },
    }
    return identity, datasets, compact


def _write_material(output_dir: Path, name: str, value: Any) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / name
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise HybridPilotError(f"immutable 0033 pilot material differs: {path}")
        return
    path.write_text(encoded, encoding="utf-8")


def run_microstructure_hybrid_pilot(
    source_store_dir: str | Path,
    anchor_population_path: str | Path,
    anchor_event_root: str | Path,
    clean_result_path: str | Path,
    output_dir: str | Path,
    *,
    config: HybridPilotConfig | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the bounded 0033 pilot and return runtime/EvidenceBundle material."""

    cfg = (
        config
        if isinstance(config, HybridPilotConfig)
        else HybridPilotConfig(**dict(config or {}))
    )
    cfg.validate()
    wall_started = time.perf_counter()
    cpu_started = resource.getrusage(resource.RUSAGE_SELF)
    children_started = resource.getrusage(resource.RUSAGE_CHILDREN)
    episodes, anchor_provenance = load_structural_opportunities(
        anchor_population_path,
        anchor_event_root,
        clean_result_path,
        config=cfg,
    )
    sparse_cfg = SparsePilotConfig(
        campaign_id=cfg.campaign_id,
        manifest_hash=cfg.manifest_hash,
        source_commit=cfg.source_commit,
        selected_markets=cfg.selected_markets,
        chronological_roles=cfg.chronological_roles,
    )
    store = load_sparse_source_store(source_store_dir, config=sparse_cfg)
    if store.sessions != cfg.selected_sessions:
        raise HybridPilotError("0031 store session order differs from 0033 freeze")
    outcomes = evaluate_paired_actions(episodes, store, config=cfg)
    candidate_results, policy_results = freeze_and_evaluate_policies(outcomes, config=cfg)
    pilot_status, gate_checks = decide_hybrid_gate(policy_results, config=cfg)
    lattice = freeze_action_lattice(cfg)
    passive = [row for row in outcomes if row.passive_side_lane]
    passive_filled = sum(int(row.stressed["quantity"]) > 0 for row in passive)
    economic_elapsed = max(time.perf_counter() - wall_started, 1e-9)
    cpu_after = resource.getrusage(resource.RUSAGE_SELF)
    children_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_seconds = (
        cpu_after.ru_utime
        + cpu_after.ru_stime
        + children_after.ru_utime
        + children_after.ru_stime
        - cpu_started.ru_utime
        - cpu_started.ru_stime
        - children_started.ru_utime
        - children_started.ru_stime
    )

    evidence_identity, evidence_datasets, compact_outputs = _canonical_evidence_material(
        config=cfg,
        store=store,
        source_episodes=episodes,
        outcomes=outcomes,
        policies=policy_results,
        provenance=anchor_provenance,
        pilot_status=pilot_status,
        gate_checks=gate_checks,
    )
    # The canonical ledgers contain only executable selected-policy rows.  The
    # full paired counterfactual lattice remains an immutable compact output so
    # every A0--A5 delta can be reconciled on the identical opportunity.
    compact_outputs["paired_counterfactual_outcomes"] = [
        row.to_dict() for row in outcomes
    ]
    compact_outputs["passive_side_lane"] = _passive_side_lane_summary(outcomes)
    total_elapsed = max(time.perf_counter() - wall_started, 1e-9)
    compact_outputs["failure_vectors"]["PASSIVE_NON_FILL"] = (
        len(passive) - passive_filled
    )
    role_denominators = {
        role: sum(row.role == role for row in episodes) for role in SESSION_ROLES
    }
    source_bridge = {
        "normal_net_delta_usd": float(
            math.fsum(
                row.normal_delta_vs_source_0028_usd
                for row in outcomes
                if row.action_id == "A0_BASELINE_IMMEDIATE"
                and row.normal_delta_vs_source_0028_usd is not None
            )
        ),
        "stressed_net_delta_usd": float(
            math.fsum(
                row.stressed_delta_vs_source_0028_usd
                for row in outcomes
                if row.action_id == "A0_BASELINE_IMMEDIATE"
                and row.stressed_delta_vs_source_0028_usd is not None
            )
        ),
        "source_noncensored_denominator": sum(
            not row.source_reference_censored for row in episodes
        ),
        "hybrid_evaluable_denominator": len(episodes),
        "development_fill_model": "CAUSAL_NEXT_TRADABLE_OPEN_V1",
        "paired_fill_model": "0031_CAUSAL_BBO_MICRO_EXECUTION_V1",
    }
    best_overlay = max(
        policy_results,
        key=lambda row: math.fsum(
            float(
                row["role_results"][role]["stressed_delta_vs_a0_usd"]
            )
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        ),
        default=None,
    )
    production_kpis = {
        "structural_anchor_count": len(anchor_provenance["selected_anchor_ids"]),
        "structural_opportunity_count": len(episodes),
        "paired_action_outcome_count": len(outcomes),
        "paired_actions_per_opportunity": len(lattice),
        "candidate_result_count": len(candidate_results),
        "policy_result_count": len(policy_results),
        "passive_side_lane_count": len(passive),
        "passive_fill_count": passive_filled,
        "passive_side_lane_status": compact_outputs["passive_side_lane"][
            "status"
        ],
        "defensible_policy_count": gate_checks["defensible_policy_count"],
        "role_opportunity_denominators": role_denominators,
        "source_0028_noncensored_count": source_bridge["source_noncensored_denominator"],
        "hybrid_evaluable_opportunity_count": source_bridge["hybrid_evaluable_denominator"],
        "control_replay_count": len(episodes) * 2,
        "matched_controls_status": "PAIRED_COMPLETE",
        "paired_uplift": {
            "best_validation_stressed_delta_usd": max(
                (
                    float(row["role_results"]["VALIDATION"]["stressed_delta_vs_a0_usd"])
                    for row in policy_results
                ),
                default=0.0,
            ),
            "best_final_stressed_delta_usd": max(
                (
                    float(row["role_results"]["FINAL_DEVELOPMENT"]["stressed_delta_vs_a0_usd"])
                    for row in policy_results
                ),
                default=0.0,
            ),
            "best_policy_id": (
                None if best_overlay is None else best_overlay["policy_id"]
            ),
                "best_policy_attribution": (
                    {}
                    if best_overlay is None
                    else best_overlay.get("paired_stressed_uplift_attribution", {})
                ),
        },
        "fill_model_bridge": source_bridge,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "orders": 0,
    }
    runtime_metrics = {
        "elapsed_seconds": total_elapsed,
        "cpu_seconds": cpu_seconds,
        "cpu_utilization_fraction": min(
            max(cpu_seconds / (economic_elapsed * 3.0), 0.0), 1.0
        ),
        "economic_worker_utilization_fraction": min(
            max(
                cpu_seconds
                / (economic_elapsed * float(cfg.cpu_worker_count)),
                0.0,
            ),
            1.0,
        ),
        "economic_wall_clock_fraction": min(
            max(economic_elapsed / total_elapsed, 0.0), 1.0
        ),
        "cpu_worker_count": cfg.cpu_worker_count,
        "source_store_cache_hit_rate": 1.0,
    }
    result = {
        "campaign_id": cfg.campaign_id,
        "pilot_status": pilot_status,
        "candidate_results": candidate_results,
        "policy_results": policy_results,
        "evidence_identity": evidence_identity,
        "evidence_datasets": evidence_datasets,
        "compact_outputs": compact_outputs,
        "production_kpis": production_kpis,
        "runtime_metrics": runtime_metrics,
        "gate_checks": gate_checks,
    }
    target = Path(output_dir).resolve()
    _write_material(target, "hybrid_pilot_summary.json", {key: value for key, value in result.items() if key != "evidence_datasets"})
    _write_material(target, "hybrid_evidence_material.json", {"identity": evidence_identity, "datasets": evidence_datasets, "outputs": compact_outputs})
    return result


__all__ = [
    "ACTION_IDS",
    "HYBRID_PILOT_VERSION",
    "HybridActionSpec",
    "HybridPilotConfig",
    "HybridPilotError",
    "PILOT_STATUSES",
    "PROMOTABLE_ACTIONS",
    "PairedActionOutcome",
    "StructuralOpportunityEpisode",
    "decide_hybrid_gate",
    "evaluate_paired_actions",
    "freeze_action_lattice",
    "freeze_and_evaluate_policies",
    "load_structural_opportunities",
    "run_microstructure_hybrid_pilot",
]
