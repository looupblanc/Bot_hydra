"""Causal target-velocity search primitives for campaign 0028.

The module deliberately separates *decisions* from *outcome labels*.  The
decision rule receives only a completed feature row whose ``available_at`` is
not later than ``decision_time``.  Barrier traversal happens afterwards in
``observe_outcome`` and missing traversal data censors an already emitted
signal; it can never make the signal disappear.

Batch screening and one-record streaming call the same
``FrozenHazardDecisionRule.evaluate`` method.  The high-throughput batch path
passes NumPy arrays to it, while the streaming adapter passes scalars.  This is
the small shared decision path required by the causal salvage contract; fills
remain the frozen next-tradable-open model from
``hydra.research.causal_sleeve_replay``.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.scaling_plan import mini_equivalent
from hydra.research.causal_sleeve_replay import (
    CAUSAL_FILL_POLICY_ID,
    CENSORED_FUTURE_COVERAGE,
    MINUTE_NS,
    CausalFillPolicy,
    CausalTradeMark,
    CausalTradeTrajectory,
)


ENGINE_VERSION = "causal_target_velocity_v1"
EVENT_SCHEMA = "hydra_causal_target_before_adverse_event_v1"
SCREEN_SCHEMA = "hydra_causal_target_velocity_screen_v1"

FAVORABLE_R_LEVELS = (0.5, 1.0, 1.5, 2.0)
ADVERSE_R_LEVELS = (0.5, 0.75, 1.0)
LABEL_HORIZONS: tuple[int | str, ...] = (5, 15, 30, 60, "SESSION", "OVERNIGHT")
STATIC_RISK_LEVELS = (0.75, 1.0, 1.25, 1.5)
RISK_LEVEL_TO_MICRO_UNITS = {0.75: 3, 1.0: 4, 1.25: 5, 1.5: 6}
QUANTILES = (0.55, 0.65, 0.75, 0.85)
CONTEXT_QUANTILES = (0.35, 0.50, 0.65)
# -2 is the cached overnight/outside-RTH role.  -1 remains the aggregate
# ANY_RTH selector and must never absorb overnight rows.
SESSION_CODES = (-2, -1, 0, 1, 2)
TIMEFRAME_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60}
CROSS_ASSET_REFERENCE_MARKETS = {
    "ES": "NQ",
    "NQ": "ES",
    "RTY": "ES",
    "YM": "ES",
    "CL": "ES",
    "GC": "ES",
}

# These are already materialized, past/current-only columns in the immutable
# campaign feature matrices.  No future-outcome array is reachable here.
MECHANISM_RECIPES: tuple[Mapping[str, Any], ...] = (
    {
        "mechanism": "COMPRESSION_TO_EXPANSION",
        "trigger_feature": "rv_short_long_ratio",
        "trigger_operator": "LT",
        "context_feature": "ctx_5m_volatility_expansion",
        "context_operator": "GT",
        "direction_rule": "PAST_RETURN_CONTINUATION",
    },
    {
        "mechanism": "DISPLACEMENT_ACCELERATION",
        "trigger_feature": "ctx_5m_return",
        "trigger_operator": "ABS_GT",
        "context_feature": "ctx_30m_return",
        "context_operator": "SAME_SIGN",
        "direction_rule": "TRIGGER_SIGN_CONTINUATION",
    },
    {
        "mechanism": "RANGE_BREAKOUT_WITH_ROOM",
        "trigger_feature": "old_region_reentry",
        "trigger_operator": "ABS_GT",
        "context_feature": "past_volatility",
        "context_operator": "GT",
        "direction_rule": "PAST_RETURN_CONTINUATION",
    },
    {
        "mechanism": "PARTICIPATION_DENSITY",
        "trigger_feature": "past_participation",
        "trigger_operator": "GT",
        "context_feature": "rv_short_long_ratio",
        "context_operator": "GT",
        "direction_rule": "PAST_RETURN_CONTINUATION",
    },
    {
        "mechanism": "INDEPENDENT_SOURCE_DENSITY",
        "trigger_feature": "past_participation",
        "trigger_operator": "GT",
        "context_feature": "ctx_5m_volatility_expansion",
        "context_operator": "GT",
        "direction_rule": "PAST_RETURN_CONTINUATION",
    },
    {
        "mechanism": "MULTI_TIMEFRAME_ALIGNMENT",
        "trigger_feature": "ctx_15m_return",
        "trigger_operator": "ABS_GT",
        "context_feature": "ctx_60m_return",
        "context_operator": "SAME_SIGN",
        "direction_rule": "TRIGGER_SIGN_CONTINUATION",
    },
    {
        "mechanism": "CROSS_ASSET_STATE",
        "trigger_feature": "ctx_15m_return",
        "trigger_operator": "ABS_GT",
        "context_feature": "cross_asset_ctx_15m_return",
        "context_operator": "SAME_SIGN",
        "direction_rule": "TRIGGER_SIGN_CONTINUATION",
    },
    {
        "mechanism": "EXHAUSTION_REVERSAL",
        "trigger_feature": "extreme_dwell",
        "trigger_operator": "GT",
        "context_feature": "past_return_60",
        "context_operator": "ABS_GT",
        "direction_rule": "PAST_RETURN_REVERSAL",
    },
    {
        "mechanism": "FAILED_CONTINUATION_REVERSAL",
        "trigger_feature": "failed_expansion",
        "trigger_operator": "GT",
        "context_feature": "ctx_15m_return",
        "context_operator": "ABS_GT",
        "direction_rule": "CONTEXT_SIGN_REVERSAL",
    },
    {
        "mechanism": "DIRECTIONAL_PRESSURE_RELEASE",
        "trigger_feature": "directional_pressure_without_progress",
        "trigger_operator": "GT",
        "context_feature": "past_participation",
        "context_operator": "GT",
        "direction_rule": "PAST_RETURN_REVERSAL",
    },
)


class HazardOutcome(StrEnum):
    FAVORABLE_FIRST = "FAVORABLE_FIRST"
    ADVERSE_FIRST = "ADVERSE_FIRST"
    NEITHER_REACHED = "NEITHER_REACHED"
    CENSORED_FUTURE_COVERAGE = CENSORED_FUTURE_COVERAGE


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class HazardCandidate:
    market: str
    execution_market: str
    mechanism: str
    cross_asset_reference_market: str | None
    timeframe: str
    session_code: int
    trigger_feature: str
    trigger_operator: str
    trigger_quantile: float
    context_feature: str | None
    context_operator: str | None
    context_quantile: float | None
    direction_rule: str
    favorable_r: float
    adverse_r: float
    horizon: int | str
    risk_level: float
    cooldown_minutes: int
    version: int = 1

    def __post_init__(self) -> None:
        if self.favorable_r not in FAVORABLE_R_LEVELS:
            raise ValueError("unpreregistered favorable barrier")
        if self.adverse_r not in ADVERSE_R_LEVELS:
            raise ValueError("unpreregistered adverse barrier")
        if self.horizon not in LABEL_HORIZONS:
            raise ValueError("unpreregistered label horizon")
        if self.risk_level not in STATIC_RISK_LEVELS:
            raise ValueError("unpreregistered static risk level")
        if self.trigger_quantile not in QUANTILES:
            raise ValueError("unpreregistered trigger quantile")
        if self.context_feature is None:
            if self.context_operator is not None or self.context_quantile is not None:
                raise ValueError("context-free candidate has context parameters")
        elif (
            self.context_operator is None
            or self.context_quantile not in CONTEXT_QUANTILES
        ):
            raise ValueError("context candidate lacks a frozen quantile/operator")
        if self.session_code not in SESSION_CODES:
            raise ValueError("invalid frozen session code")
        if (self.session_code == -2) != (self.horizon == "OVERNIGHT"):
            raise ValueError(
                "overnight session role and overnight holding horizon must be paired"
            )
        if self.timeframe not in TIMEFRAME_MINUTES:
            raise ValueError("invalid executable decision timeframe")
        if self.cooldown_minutes < 1:
            raise ValueError("candidate cooldown must be positive")
        if self.mechanism == "CROSS_ASSET_STATE":
            if (
                self.cross_asset_reference_market is None
                or self.cross_asset_reference_market == self.market
            ):
                raise ValueError("cross-asset candidate lacks a distinct reference market")
        elif self.cross_asset_reference_market is not None:
            raise ValueError("non-cross-asset candidate declares a reference market")

    @property
    def payload(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def structural_fingerprint(self) -> str:
        return stable_hash(self.payload)

    @property
    def candidate_id(self) -> str:
        return f"hazard_{self.structural_fingerprint[:24]}"

    @property
    def decision_fingerprint(self) -> str:
        """Identity of event timing before outcome/risk variations."""

        return stable_hash(
            {
                key: value
                for key, value in self.payload.items()
                if key
                not in {
                    "favorable_r",
                    "adverse_r",
                    "horizon",
                    "risk_level",
                }
            }
        )

    @property
    def behavioral_fingerprint(self) -> str:
        """Risk scaling is not counted as independent signal behavior."""

        return stable_hash(
            {
                key: value
                for key, value in self.payload.items()
                if key != "risk_level"
            }
        )

    def executable_specification(self) -> dict[str, Any]:
        return {
            "schema": "hydra_causal_target_velocity_sleeve_v1",
            "candidate_id": self.candidate_id,
            "market": self.market,
            "execution_market": self.execution_market,
            "cross_asset_reference_market": self.cross_asset_reference_market,
            "timeframe": self.timeframe,
            "session_code": self.session_code,
            "session_role": {
                -2: "OVERNIGHT_OUTSIDE_RTH",
                -1: "ANY_RTH",
                0: "OPEN",
                1: "MID_SESSION",
                2: "LATE_CLOSE",
            }[self.session_code],
            "entry": "DECISION_AFTER_COMPLETED_BAR_NEXT_TRADABLE_OPEN",
            "sizing": {
                "static_risk_level": self.risk_level,
                "micro_units": RISK_LEVEL_TO_MICRO_UNITS[self.risk_level],
            },
            "stop": f"{self.adverse_r:.2f}R_RESTING_ADVERSE_BARRIER",
            "target": f"{self.favorable_r:.2f}R_RESTING_FAVORABLE_BARRIER",
            "maximum_holding_time": self.horizon,
            "session_handling": (
                "FROZEN_SESSION_ONLY"
                if self.horizon != "OVERNIGHT"
                else "FROZEN_OVERNIGHT_WHEN_CONTIGUOUS_CONTRACT_COVERAGE_EXISTS"
            ),
            "fill_policy": CAUSAL_FILL_POLICY_ID,
            "structural_fingerprint": self.structural_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class CalibratedHazardCandidate:
    candidate: HazardCandidate
    calibration_end_exclusive_ns: int
    trigger_threshold: float
    context_threshold: float | None
    finite_trigger_observations: int
    finite_context_observations: int | None
    source_matrix_hash: str

    @property
    def fingerprint(self) -> str:
        return stable_hash(
            {
                "candidate": self.candidate.payload,
                "calibration_end_exclusive_ns": self.calibration_end_exclusive_ns,
                "trigger_threshold": self.trigger_threshold,
                "context_threshold": self.context_threshold,
                "finite_trigger_observations": self.finite_trigger_observations,
                "finite_context_observations": self.finite_context_observations,
                "source_matrix_hash": self.source_matrix_hash,
            }
        )


@dataclass(frozen=True, slots=True)
class HazardIntent:
    candidate_id: str
    intent_namespace: str
    evidence_role: str
    control_id: str | None
    row_index: int
    market: str
    contract_code: int
    session_day: int
    session_code: int
    segment_code: int
    event_time_ns: int
    available_at_ns: int
    decision_time_ns: int
    order_submit_time_ns: int
    entry_intent: str
    earliest_executable_time_ns: int
    direction: int
    feature_fingerprint: str

    @property
    def fingerprint(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True, slots=True)
class HazardEventEvidence:
    event_id: str
    candidate_id: str
    intent_namespace: str
    evidence_role: str
    control_id: str | None
    market: str
    execution_market: str
    contract_code: int
    timeframe: str
    session_day: int
    session_code: int
    segment_code: int
    event_time_ns: int
    available_at_ns: int
    decision_time_ns: int
    order_submit_time_ns: int
    entry_intent: str
    earliest_executable_time_ns: int
    fill_time_ns: int | None
    raw_fill_price: float | None
    normal_fill_price: float | None
    stressed_fill_price: float | None
    direction: int
    quantity: int
    risk_unit_price: float
    favorable_r: float
    adverse_r: float
    favorable_price: float | None
    adverse_price: float | None
    maximum_horizon: int | str
    outcome: str
    outcome_time_ns: int | None
    time_to_favorable_minutes: int | None
    time_to_adverse_minutes: int | None
    maximum_favorable_excursion_r: float | None
    maximum_adverse_excursion_r: float | None
    raw_exit_price: float | None
    exit_fill_semantics: str
    normal_net_pnl: float | None
    stressed_net_pnl: float | None
    normal_worst_unrealized_pnl: float | None
    stressed_worst_unrealized_pnl: float | None
    normal_best_unrealized_pnl: float | None
    stressed_best_unrealized_pnl: float | None
    normal_initial_unrealized_pnl: float | None
    stressed_initial_unrealized_pnl: float | None
    normal_marks: tuple[CausalTradeMark, ...]
    stressed_marks: tuple[CausalTradeMark, ...]
    same_bar_ambiguous: bool
    censor_reason: str | None
    feature_fingerprint: str
    fill_policy_id: str
    fill_policy_hash: str

    @property
    def fingerprint(self) -> str:
        return stable_hash(asdict(self))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": EVENT_SCHEMA,
            **asdict(self),
            "event_fingerprint": self.fingerprint,
        }


@dataclass(frozen=True, slots=True)
class HazardScreenResult:
    candidate_id: str
    candidate_fingerprint: str
    calibration_fingerprint: str
    emitted_event_count: int
    completed_event_count: int
    censored_event_count: int
    favorable_first_count: int
    adverse_first_count: int
    neither_count: int
    same_bar_ambiguous_count: int
    favorable_before_adverse_rate: float
    adverse_before_favorable_rate: float
    median_time_to_favorable_minutes: float | None
    median_time_to_adverse_minutes: float | None
    median_mfe_r: float | None
    median_mae_r: float | None
    normal_net_pnl: float
    stressed_net_pnl: float
    eligible_session_count: int
    emitted_session_count: int
    independent_events_per_20_sessions: float
    stressed_target_contribution_per_20_sessions: float
    stressed_expected_mll_consumption_per_20_sessions: float
    cost_adjusted_target_velocity: float
    day_concentration: float
    block_concentration: float | None
    market_concentration: float
    session_concentration: float
    event_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {"schema": SCREEN_SCHEMA, **asdict(self)}


@dataclass(frozen=True, slots=True)
class ExactHazardSleeveReplay:
    candidate: HazardCandidate
    events: tuple[HazardEventEvidence, ...]
    normal_events: tuple[TradePathEvent, ...]
    stressed_events: tuple[TradePathEvent, ...]
    normal_trajectories: tuple[CausalTradeTrajectory, ...]
    stressed_trajectories: tuple[CausalTradeTrajectory, ...]
    eligible_session_days: tuple[int, ...]
    decision_hash: str
    normal_event_hash: str
    stressed_event_hash: str
    normal_trajectory_hash: str
    stressed_trajectory_hash: str
    fill_policy_hash: str


class FrozenHazardDecisionRule:
    """One decision primitive shared by NumPy batch and scalar streaming."""

    def __init__(self, calibrated: CalibratedHazardCandidate) -> None:
        self.calibrated = calibrated

    def evaluate(
        self,
        *,
        trigger: float | np.ndarray,
        context: float | np.ndarray | None,
        past_return: float | np.ndarray,
        session_code: int | np.ndarray,
    ) -> tuple[bool | np.ndarray, int | np.ndarray]:
        candidate = self.calibrated.candidate
        trigger_array = np.asarray(trigger, dtype=float)
        return_array = np.asarray(past_return, dtype=float)
        session_array = np.asarray(session_code)
        eligible = np.isfinite(trigger_array) & np.isfinite(return_array)
        if candidate.session_code == -2:
            eligible &= session_array == -2
        elif candidate.session_code >= 0:
            eligible &= session_array == candidate.session_code
        else:
            # -1 is explicitly ANY_RTH, never ANY_SESSION.
            eligible &= session_array >= 0
        eligible &= _compare(
            trigger_array,
            candidate.trigger_operator,
            self.calibrated.trigger_threshold,
            reference=None,
        )
        context_array: np.ndarray | None = None
        if candidate.context_feature is not None:
            if context is None:
                eligible &= False
            else:
                context_array = np.asarray(context, dtype=float)
                eligible &= np.isfinite(context_array)
                eligible &= _compare(
                    context_array,
                    str(candidate.context_operator),
                    float(self.calibrated.context_threshold),
                    reference=trigger_array,
                )
        direction = _direction(
            candidate.direction_rule,
            trigger=trigger_array,
            context=context_array,
            past_return=return_array,
        )
        direction = np.where(np.isfinite(direction), direction, 0.0)
        eligible &= direction != 0
        if np.ndim(trigger_array) == 0:
            return bool(eligible), int(direction)
        return eligible, direction.astype(np.int8, copy=False)


class CausalHazardStreamingDecisionKernel:
    """Minimal streaming adapter used to prove batch-decision equality."""

    def __init__(self, calibrated: CalibratedHazardCandidate) -> None:
        self.rule = FrozenHazardDecisionRule(calibrated)
        self.last_segment: int | None = None
        self.prior_eligible = False
        self.cooldown_until_ns = -1

    def step(
        self,
        *,
        trigger: float,
        context: float | None,
        past_return: float,
        session_code: int,
        segment_code: int,
        timestamp_ns: int,
    ) -> tuple[bool, int]:
        if not _is_completed_timeframe_boundary(
            int(timestamp_ns), self.rule.calibrated.candidate.timeframe
        ):
            return False, 0
        eligible, direction = self.rule.evaluate(
            trigger=trigger,
            context=context,
            past_return=past_return,
            session_code=session_code,
        )
        if self.last_segment != int(segment_code):
            self.prior_eligible = False
            self.cooldown_until_ns = -1
            self.last_segment = int(segment_code)
        emit = bool(
            eligible
            and not self.prior_eligible
            and int(timestamp_ns) >= self.cooldown_until_ns
        )
        self.prior_eligible = bool(eligible)
        if emit:
            self.cooldown_until_ns = int(timestamp_ns) + (
                self.rule.calibrated.candidate.cooldown_minutes * MINUTE_NS
            )
        return emit, int(direction)


def generate_structural_proposals(
    market_pairs: Mapping[str, str],
    *,
    minimum_count: int = 20_000,
) -> tuple[HazardCandidate, ...]:
    """Generate a deterministic, bounded grammar and return diverse proposals.

    The cartesian source is larger than the requested production population.
    Sorting by semantic hash before truncation prevents market/mechanism order
    from silently concentrating the first 20,000 candidates.
    """

    if minimum_count < 1:
        raise ValueError("proposal target must be positive")
    dimensions: tuple[Sequence[Any], ...] = (
        tuple(sorted(market_pairs.items())),
        MECHANISM_RECIPES,
        ("1m", "5m", "15m", "30m", "60m"),
        SESSION_CODES,
        QUANTILES,
        CONTEXT_QUANTILES,
        FAVORABLE_R_LEVELS,
        ADVERSE_R_LEVELS,
        LABEL_HORIZONS,
    )
    total = math.prod(len(values) for values in dimensions)
    if total < minimum_count:
        raise ValueError(
            f"bounded grammar contains {total} < {minimum_count} proposals"
        )
    # Walk the mixed-radix product with a coprime stride.  This produces a
    # deterministic, balanced population without materializing the >400k
    # source cartesian population merely to retain 20k rows.
    stride = 104_729
    while math.gcd(stride, total) != 1:
        stride += 2
    proposals: list[HazardCandidate] = []
    seen: set[str] = set()
    for ordinal in range(total):
        linear = (17_071 + ordinal * stride) % total
        resolved: list[Any] = []
        remainder = linear
        for values in reversed(dimensions):
            remainder, offset = divmod(remainder, len(values))
            resolved.append(values[offset])
        (
            market_pair,
            recipe,
            timeframe,
            session_code,
            trigger_quantile,
            context_quantile,
            favorable_r,
            adverse_r,
            horizon,
        ) = reversed(resolved)
        market, execution_market = market_pair
        if (session_code == -2) != (horizon == "OVERNIGHT"):
            continue
        base = {
            "market": market,
            "execution_market": execution_market,
            "mechanism": recipe["mechanism"],
            "cross_asset_reference_market": (
                CROSS_ASSET_REFERENCE_MARKETS[market]
                if recipe["mechanism"] == "CROSS_ASSET_STATE"
                else None
            ),
            "timeframe": timeframe,
            "session_code": session_code,
            "trigger_feature": recipe["trigger_feature"],
            "trigger_operator": recipe["trigger_operator"],
            "trigger_quantile": trigger_quantile,
            "context_feature": recipe["context_feature"],
            "context_operator": recipe["context_operator"],
            "context_quantile": context_quantile,
            "direction_rule": recipe["direction_rule"],
            "favorable_r": favorable_r,
            "adverse_r": adverse_r,
            "horizon": horizon,
        }
        seed = int(stable_hash(base)[:8], 16)
        candidate = HazardCandidate(
            **base,
            risk_level=STATIC_RISK_LEVELS[seed % len(STATIC_RISK_LEVELS)],
            cooldown_minutes=max(5, _numeric_horizon(horizon)),
        )
        if candidate.structural_fingerprint in seen:
            continue
        seen.add(candidate.structural_fingerprint)
        proposals.append(candidate)
        if len(proposals) == minimum_count:
            break
    if len(proposals) != minimum_count:
        raise RuntimeError("deterministic proposal sampler exhausted unexpectedly")
    return tuple(proposals)


def deduplicate_for_event_screen(
    proposals: Sequence[HazardCandidate],
    *,
    minimum_unique: int = 4_096,
    maximum_unique: int | None = None,
) -> tuple[HazardCandidate, ...]:
    """Reject structural, semantic, and risk-only clones deterministically."""

    structural: set[str] = set()
    behavioral: set[str] = set()
    retained: list[HazardCandidate] = []
    for candidate in sorted(proposals, key=lambda row: row.structural_fingerprint):
        if candidate.structural_fingerprint in structural:
            continue
        if candidate.behavioral_fingerprint in behavioral:
            continue
        structural.add(candidate.structural_fingerprint)
        behavioral.add(candidate.behavioral_fingerprint)
        retained.append(candidate)
        if maximum_unique is not None and len(retained) >= maximum_unique:
            break
    if len(retained) < minimum_unique:
        raise ValueError(
            f"behavioral deduplication retained {len(retained)} < {minimum_unique}"
        )
    return tuple(retained)


def calibrate_candidate(
    candidate: HazardCandidate,
    matrix: FeatureMatrix,
    *,
    calibration_end_exclusive_ns: int,
    minimum_observations: int = 100,
) -> CalibratedHazardCandidate:
    """Freeze thresholds exclusively from rows before the evaluation period."""

    timestamps = matrix.array("timestamp_ns")
    available = matrix.array("availability_ns")
    decisions = matrix.array("decision_ns")
    trigger = matrix.array(f"feature__{candidate.trigger_feature}")
    before = (
        (timestamps < int(calibration_end_exclusive_ns))
        & (available <= decisions)
        & (decisions <= int(calibration_end_exclusive_ns))
        & np.isfinite(trigger)
    )
    trigger_values = trigger[before]
    if len(trigger_values) < minimum_observations:
        raise ValueError("insufficient causal trigger calibration history")
    trigger_threshold = _quantile_threshold(
        trigger_values, candidate.trigger_operator, candidate.trigger_quantile
    )
    context_threshold: float | None = None
    context_count: int | None = None
    if candidate.context_feature is not None:
        context = matrix.array(f"feature__{candidate.context_feature}")
        context_values = context[before & np.isfinite(context)]
        context_count = int(len(context_values))
        if context_count < minimum_observations:
            raise ValueError("insufficient causal context calibration history")
        context_threshold = _quantile_threshold(
            context_values,
            str(candidate.context_operator),
            float(candidate.context_quantile),
        )
    return CalibratedHazardCandidate(
        candidate=candidate,
        calibration_end_exclusive_ns=int(calibration_end_exclusive_ns),
        trigger_threshold=float(trigger_threshold),
        context_threshold=context_threshold,
        finite_trigger_observations=int(len(trigger_values)),
        finite_context_observations=context_count,
        source_matrix_hash=matrix.fingerprint,
    )


def with_availability_safe_cross_asset_feature(
    primary: FeatureMatrix,
    reference: FeatureMatrix,
    *,
    reference_feature: str = "ctx_15m_return",
    output_feature: str = "cross_asset_ctx_15m_return",
    maximum_staleness_minutes: int = 5,
) -> FeatureMatrix:
    """Attach the latest *already available* reference-market descriptor.

    ``searchsorted(..., side='right') - 1`` is an as-of join on availability,
    not on future event time.  A reference row unavailable at the primary
    decision becomes NaN and can only suppress the current decision honestly.
    """

    primary_decision = primary.array("decision_ns")
    reference_available = reference.array("availability_ns")
    primary_market = str(dict(primary.manifest.get("key") or {}).get("market") or "")
    reference_market = str(
        dict(reference.manifest.get("key") or {}).get("market") or ""
    )
    if not primary_market or not reference_market or primary_market == reference_market:
        raise ValueError("cross-asset view requires two explicit distinct market identities")
    if np.any(reference_available[1:] < reference_available[:-1]):
        raise ValueError("reference availability is not chronological")
    if maximum_staleness_minutes < 0:
        raise ValueError("cross-asset staleness bound cannot be negative")
    positions = np.searchsorted(
        reference_available, primary_decision, side="right"
    ) - 1
    joined = np.full(primary.row_count, np.nan, dtype=np.float64)
    valid = positions >= 0
    valid_indices = np.flatnonzero(valid)
    fresh = (
        primary_decision[valid_indices]
        - reference_available[positions[valid_indices]]
        <= maximum_staleness_minutes * MINUTE_NS
    )
    valid[valid_indices[~fresh]] = False
    source = reference.array(f"feature__{reference_feature}")
    joined[valid] = source[positions[valid]]
    joined.flags.writeable = False
    arrays = dict(primary.arrays)
    arrays[f"feature__{output_feature}"] = joined
    manifest = dict(primary.manifest)
    manifest["bundle_hash"] = stable_hash(
        {
            "schema": "hydra_availability_safe_cross_asset_view_v1",
            "primary_matrix_hash": primary.fingerprint,
            "reference_matrix_hash": reference.fingerprint,
            "primary_market": primary_market,
            "reference_market": reference_market,
            "reference_feature": reference_feature,
            "output_feature": output_feature,
            "maximum_staleness_minutes": maximum_staleness_minutes,
            "join_contract": "LATEST_REFERENCE_AVAILABLE_AT_OR_BEFORE_PRIMARY_DECISION",
        }
    )
    return FeatureMatrix(root=primary.root, manifest=manifest, arrays=arrays)


def discover_intents_batch(
    calibrated: CalibratedHazardCandidate,
    matrix: FeatureMatrix,
    *,
    evaluation_start_ns: int,
    evaluation_end_exclusive_ns: int,
) -> tuple[HazardIntent, ...]:
    """Vectorized causal decision screen followed by deterministic cooldown."""

    candidate = calibrated.candidate
    timestamp = matrix.array("timestamp_ns")
    decision = matrix.array("decision_ns")
    available = matrix.array("availability_ns")
    sessions = matrix.array("session_code")
    days = matrix.array("session_day")
    segments = matrix.array("segment_code")
    contracts = matrix.array("contract_code")
    trigger = matrix.array(f"feature__{candidate.trigger_feature}")
    context = (
        None
        if candidate.context_feature is None
        else matrix.array(f"feature__{candidate.context_feature}")
    )
    past_return = matrix.array("feature__past_return_60")
    rule = FrozenHazardDecisionRule(calibrated)
    eligible, direction = rule.evaluate(
        trigger=trigger,
        context=context,
        past_return=past_return,
        session_code=sessions,
    )
    scope = (
        (timestamp >= int(evaluation_start_ns))
        & (timestamp < int(evaluation_end_exclusive_ns))
        & (available <= decision)
        & (decision <= int(evaluation_end_exclusive_ns))
    )
    boundary = _completed_timeframe_boundary_mask(timestamp, candidate.timeframe)
    eligible = np.asarray(eligible, dtype=bool) & scope & boundary
    evaluated = np.flatnonzero(scope & boundary)
    evaluated_eligible = eligible[evaluated]
    previous_evaluated = np.zeros(len(evaluated), dtype=bool)
    if len(evaluated) > 1:
        previous_evaluated[1:] = evaluated_eligible[:-1] & (
            segments[evaluated[1:]] == segments[evaluated[:-1]]
        )
    crossings = evaluated[evaluated_eligible & ~previous_evaluated]
    retained = _cooldown_indices(
        crossings,
        timestamp=timestamp,
        segment=segments,
        cooldown_ns=candidate.cooldown_minutes * MINUTE_NS,
    )
    intents: list[HazardIntent] = []
    for raw_index in retained:
        index = int(raw_index)
        if not (
            int(available[index]) <= int(decision[index])
            and int(decision[index]) >= int(timestamp[index]) + MINUTE_NS
        ):
            raise ValueError("decision row violates availability contract")
        earliest = _next_executable_boundary(
            int(decision[index]), int(available[index])
        )
        feature_payload = {
            "trigger_feature": candidate.trigger_feature,
            "trigger_value": float(trigger[index]),
            "trigger_threshold": calibrated.trigger_threshold,
            "context_feature": candidate.context_feature,
            "context_value": (
                None if context is None else _finite_or_none(context[index])
            ),
            "context_threshold": calibrated.context_threshold,
            "past_return_60": float(past_return[index]),
            "matrix_hash": matrix.fingerprint,
        }
        intents.append(
            HazardIntent(
                candidate_id=candidate.candidate_id,
                intent_namespace=candidate.candidate_id,
                evidence_role="CANDIDATE",
                control_id=None,
                row_index=index,
                market=candidate.market,
                contract_code=int(contracts[index]),
                session_day=int(days[index]),
                session_code=int(sessions[index]),
                segment_code=int(segments[index]),
                event_time_ns=int(timestamp[index]),
                available_at_ns=int(available[index]),
                decision_time_ns=int(decision[index]),
                order_submit_time_ns=int(decision[index]),
                entry_intent=(
                    "ENTER_LONG_NEXT_TRADABLE_OPEN"
                    if int(direction[index]) > 0
                    else "ENTER_SHORT_NEXT_TRADABLE_OPEN"
                ),
                earliest_executable_time_ns=earliest,
                direction=int(direction[index]),
                feature_fingerprint=stable_hash(feature_payload),
            )
        )
    return tuple(intents)


def frozen_eligible_session_calendar(
    candidate: HazardCandidate,
    matrix: FeatureMatrix,
    *,
    evaluation_start_ns: int,
    evaluation_end_exclusive_ns: int,
) -> tuple[int, ...]:
    """Return the full pre-outcome calendar for the candidate session role."""

    timestamp = matrix.array("timestamp_ns")
    decision = matrix.array("decision_ns")
    availability = matrix.array("availability_ns")
    sessions = matrix.array("session_code")
    days = matrix.array("session_day")
    if candidate.session_code == -2:
        role = sessions == -2
    elif candidate.session_code == -1:
        role = sessions >= 0
    else:
        role = sessions == candidate.session_code
    mask = (
        (timestamp >= int(evaluation_start_ns))
        & (timestamp < int(evaluation_end_exclusive_ns))
        & (availability <= decision)
        & role
    )
    return tuple(sorted({int(value) for value in days[mask]}))


def discover_intents_streaming(
    calibrated: CalibratedHazardCandidate,
    matrix: FeatureMatrix,
    *,
    evaluation_start_ns: int,
    evaluation_end_exclusive_ns: int,
) -> tuple[tuple[int, int], ...]:
    """Reference streaming decisions for batch/stream parity tests."""

    candidate = calibrated.candidate
    timestamp = matrix.array("timestamp_ns")
    decision = matrix.array("decision_ns")
    available = matrix.array("availability_ns")
    sessions = matrix.array("session_code")
    segments = matrix.array("segment_code")
    trigger = matrix.array(f"feature__{candidate.trigger_feature}")
    context = (
        None
        if candidate.context_feature is None
        else matrix.array(f"feature__{candidate.context_feature}")
    )
    past_return = matrix.array("feature__past_return_60")
    kernel = CausalHazardStreamingDecisionKernel(calibrated)
    output: list[tuple[int, int]] = []
    scope = np.flatnonzero(
        (timestamp >= int(evaluation_start_ns))
        & (timestamp < int(evaluation_end_exclusive_ns))
    )
    for raw_index in scope:
        index = int(raw_index)
        if int(available[index]) > int(decision[index]):
            raise ValueError("decision row violates availability contract")
        emitted, direction = kernel.step(
            trigger=float(trigger[index]),
            context=(None if context is None else float(context[index])),
            past_return=float(past_return[index]),
            session_code=int(sessions[index]),
            segment_code=int(segments[index]),
            timestamp_ns=int(timestamp[index]),
        )
        if emitted:
            output.append((index, direction))
    return tuple(output)


def observe_outcomes(
    calibrated: CalibratedHazardCandidate,
    matrix: FeatureMatrix,
    intents: Sequence[HazardIntent],
) -> tuple[HazardEventEvidence, ...]:
    """Attach post-decision target-before-adverse outcomes to emitted intents."""

    candidate = calibrated.candidate
    fill_policy = CausalFillPolicy()
    cost_horizon = min(60, _numeric_horizon(candidate.horizon))
    payload = fill_policy.resolved_payload(candidate.execution_market, cost_horizon)
    fill_hash = stable_hash(payload)
    instrument = instrument_spec(candidate.execution_market)
    quantity = RISK_LEVEL_TO_MICRO_UNITS[candidate.risk_level]
    normal_ticks = float(payload["normal_slippage_ticks_per_side"])
    stressed_ticks = float(payload["stressed_slippage_ticks_per_side"])
    commission = float(payload["commission_round_turn_usd"]) * quantity
    timestamp = matrix.array("timestamp_ns")
    availability = matrix.array("availability_ns")
    opens = matrix.array("bar_open")
    highs = matrix.array("bar_high")
    lows = matrix.array("bar_low")
    closes = matrix.array("bar_close")
    segments = matrix.array("segment_code")
    contracts = matrix.array("contract_code")
    days = matrix.array("session_day")
    volatility = matrix.array("feature__past_volatility")
    evidence: list[HazardEventEvidence] = []
    for ordinal, intent in enumerate(intents, start=1):
        entry_index = int(
            np.searchsorted(timestamp, int(intent.earliest_executable_time_ns))
        )
        if (
            entry_index >= len(timestamp)
            or int(timestamp[entry_index]) != int(intent.earliest_executable_time_ns)
            or not _same_entry_path(
            intent, entry_index, segments=segments, contracts=contracts, days=days,
            allow_overnight=candidate.horizon == "OVERNIGHT",
            )
        ):
            evidence.append(
                _censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason="NEXT_TRADABLE_OPEN_UNOBSERVED_OR_PATH_CHANGED",
                    fill_policy_hash=fill_hash,
                )
            )
            continue
        raw_entry = float(opens[entry_index])
        if not math.isfinite(raw_entry):
            evidence.append(
                _censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason="NEXT_TRADABLE_OPEN_NONFINITE",
                    fill_policy_hash=fill_hash,
                )
            )
            continue
        normal_entry = raw_entry + (
            intent.direction * normal_ticks * instrument.tick_size
        )
        stressed_entry = raw_entry + (
            intent.direction * stressed_ticks * instrument.tick_size
        )
        signal_volatility = float(volatility[intent.row_index])
        risk_unit = max(
            2.0 * float(instrument.tick_size),
            abs(raw_entry) * max(signal_volatility, 0.0) * math.sqrt(15.0),
        )
        if not math.isfinite(risk_unit) or risk_unit <= 0:
            evidence.append(
                _censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason="CAUSAL_RISK_UNIT_UNAVAILABLE",
                    fill_policy_hash=fill_hash,
                )
            )
            continue
        favorable_price = normal_entry + intent.direction * candidate.favorable_r * risk_unit
        adverse_price = normal_entry - intent.direction * candidate.adverse_r * risk_unit
        observed = _traverse_barriers(
            candidate,
            intent,
            entry_index=entry_index,
            normal_entry=normal_entry,
            favorable_price=favorable_price,
            adverse_price=adverse_price,
            risk_unit=risk_unit,
            timestamp=timestamp,
            availability=availability,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            segments=segments,
            contracts=contracts,
            days=days,
        )
        if observed["outcome"] == HazardOutcome.CENSORED_FUTURE_COVERAGE:
            evidence.append(
                _censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason=str(observed["censor_reason"]),
                    fill_policy_hash=fill_hash,
                    fill_time_ns=int(timestamp[entry_index]),
                    raw_fill=raw_entry,
                    normal_fill=normal_entry,
                    stressed_fill=stressed_entry,
                    risk_unit=risk_unit,
                    favorable_price=favorable_price,
                    adverse_price=adverse_price,
                )
            )
            continue
        raw_exit = float(observed["raw_exit_price"])
        normal_exit = raw_exit - intent.direction * normal_ticks * instrument.tick_size
        stressed_exit = raw_exit - intent.direction * stressed_ticks * instrument.tick_size
        point_value = float(instrument.point_value)
        normal_net = (
            (normal_exit - normal_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        stressed_net = (
            (stressed_exit - stressed_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        normal_adverse = (
            float(observed["worst_raw_price"]) - normal_entry
        ) * intent.direction * point_value * quantity - commission
        stressed_adverse = (
            float(observed["worst_raw_price"]) - stressed_entry
        ) * intent.direction * point_value * quantity - commission
        normal_favorable = (
            float(observed["best_raw_price"]) - normal_entry
        ) * intent.direction * point_value * quantity - commission
        stressed_favorable = (
            float(observed["best_raw_price"]) - stressed_entry
        ) * intent.direction * point_value * quantity - commission
        normal_initial = (
            (raw_entry - normal_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        stressed_initial = (
            (raw_entry - stressed_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        normal_marks = _economic_marks(
            observed["path_indices"],
            entry=normal_entry,
            direction=intent.direction,
            quantity=quantity,
            point_value=point_value,
            commission=commission,
            availability=availability,
            highs=highs,
            lows=lows,
            closes=closes,
            terminal_net=normal_net,
            barrier_outcome=(
                str(observed["outcome"])
                if str(observed["exit_fill_semantics"]).startswith("RESTING_")
                else None
            ),
            terminal_raw_exit=raw_exit,
        )
        stressed_marks = _economic_marks(
            observed["path_indices"],
            entry=stressed_entry,
            direction=intent.direction,
            quantity=quantity,
            point_value=point_value,
            commission=commission,
            availability=availability,
            highs=highs,
            lows=lows,
            closes=closes,
            terminal_net=stressed_net,
            barrier_outcome=(
                str(observed["outcome"])
                if str(observed["exit_fill_semantics"]).startswith("RESTING_")
                else None
            ),
            terminal_raw_exit=raw_exit,
        )
        if (
            not normal_marks
            or normal_marks[-1].availability_time_ns
            != int(observed["outcome_time_ns"])
            or stressed_marks[-1].availability_time_ns
            != int(observed["outcome_time_ns"])
        ):
            raise ValueError("causal hazard mark path does not terminate at exit boundary")
        evidence.append(
            HazardEventEvidence(
                event_id=f"{intent.intent_namespace}:{ordinal:07d}",
                candidate_id=candidate.candidate_id,
                intent_namespace=intent.intent_namespace,
                evidence_role=intent.evidence_role,
                control_id=intent.control_id,
                market=candidate.market,
                execution_market=candidate.execution_market,
                contract_code=int(intent.contract_code),
                timeframe=candidate.timeframe,
                session_day=int(intent.session_day),
                session_code=int(intent.session_code),
                segment_code=int(intent.segment_code),
                event_time_ns=int(intent.event_time_ns),
                available_at_ns=int(intent.available_at_ns),
                decision_time_ns=int(intent.decision_time_ns),
                order_submit_time_ns=int(intent.order_submit_time_ns),
                entry_intent=intent.entry_intent,
                earliest_executable_time_ns=int(intent.earliest_executable_time_ns),
                fill_time_ns=int(timestamp[entry_index]),
                raw_fill_price=raw_entry,
                normal_fill_price=float(normal_entry),
                stressed_fill_price=float(stressed_entry),
                direction=int(intent.direction),
                quantity=quantity,
                risk_unit_price=float(risk_unit),
                favorable_r=float(candidate.favorable_r),
                adverse_r=float(candidate.adverse_r),
                favorable_price=float(favorable_price),
                adverse_price=float(adverse_price),
                maximum_horizon=candidate.horizon,
                outcome=str(observed["outcome"]),
                outcome_time_ns=int(observed["outcome_time_ns"]),
                time_to_favorable_minutes=observed["time_to_favorable_minutes"],
                time_to_adverse_minutes=observed["time_to_adverse_minutes"],
                maximum_favorable_excursion_r=float(observed["mfe_r"]),
                maximum_adverse_excursion_r=float(observed["mae_r"]),
                raw_exit_price=raw_exit,
                exit_fill_semantics=str(observed["exit_fill_semantics"]),
                normal_net_pnl=float(normal_net),
                stressed_net_pnl=float(stressed_net),
                normal_worst_unrealized_pnl=float(
                    min(normal_initial, normal_adverse)
                ),
                stressed_worst_unrealized_pnl=float(
                    min(stressed_initial, stressed_adverse)
                ),
                normal_best_unrealized_pnl=float(
                    max(normal_initial, normal_favorable)
                ),
                stressed_best_unrealized_pnl=float(
                    max(stressed_initial, stressed_favorable)
                ),
                normal_initial_unrealized_pnl=float(normal_initial),
                stressed_initial_unrealized_pnl=float(stressed_initial),
                normal_marks=normal_marks,
                stressed_marks=stressed_marks,
                same_bar_ambiguous=bool(observed["same_bar_ambiguous"]),
                censor_reason=None,
                feature_fingerprint=intent.feature_fingerprint,
                fill_policy_id=fill_policy.policy_id,
                fill_policy_hash=fill_hash,
            )
        )
    return tuple(evidence)


def screen_result(
    calibrated: CalibratedHazardCandidate,
    events: Sequence[HazardEventEvidence],
    *,
    eligible_session_days: Sequence[int],
    block_by_session_day: Mapping[int, str] | None = None,
) -> HazardScreenResult:
    complete = [
        row
        for row in events
        if row.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
    ]
    favorable = [row for row in complete if row.outcome == HazardOutcome.FAVORABLE_FIRST]
    adverse = [row for row in complete if row.outcome == HazardOutcome.ADVERSE_FIRST]
    neither = [row for row in complete if row.outcome == HazardOutcome.NEITHER_REACHED]
    denominator = len(complete)
    days = {int(value) for value in eligible_session_days}
    if any(row.session_day not in days for row in events):
        raise ValueError("event falls outside the frozen eligible session calendar")
    emitted_days = {row.session_day for row in events}
    normal_net = float(sum(float(row.normal_net_pnl or 0.0) for row in complete))
    stressed_net = float(sum(float(row.stressed_net_pnl or 0.0) for row in complete))
    session_count = len(days)
    per20 = len(events) * 20.0 / session_count if session_count else 0.0
    target_per20 = stressed_net * 20.0 / session_count if session_count else 0.0
    adverse_consumption = -sum(
        min(0.0, float(row.stressed_worst_unrealized_pnl or 0.0))
        for row in complete
    )
    adverse_per20 = adverse_consumption * 20.0 / session_count if session_count else 0.0
    day_counts = _counts(row.session_day for row in events)
    block_counts = (
        None
        if block_by_session_day is None
        else _counts(
            block_by_session_day[row.session_day]
            for row in events
            if row.session_day in block_by_session_day
        )
    )
    return HazardScreenResult(
        candidate_id=calibrated.candidate.candidate_id,
        candidate_fingerprint=calibrated.candidate.structural_fingerprint,
        calibration_fingerprint=calibrated.fingerprint,
        emitted_event_count=len(events),
        completed_event_count=len(complete),
        censored_event_count=len(events) - len(complete),
        favorable_first_count=len(favorable),
        adverse_first_count=len(adverse),
        neither_count=len(neither),
        same_bar_ambiguous_count=sum(row.same_bar_ambiguous for row in complete),
        favorable_before_adverse_rate=len(favorable) / denominator if denominator else 0.0,
        adverse_before_favorable_rate=len(adverse) / denominator if denominator else 0.0,
        median_time_to_favorable_minutes=_median_optional(
            row.time_to_favorable_minutes for row in favorable
        ),
        median_time_to_adverse_minutes=_median_optional(
            row.time_to_adverse_minutes for row in adverse
        ),
        median_mfe_r=_median_optional(row.maximum_favorable_excursion_r for row in complete),
        median_mae_r=_median_optional(row.maximum_adverse_excursion_r for row in complete),
        normal_net_pnl=normal_net,
        stressed_net_pnl=stressed_net,
        eligible_session_count=session_count,
        emitted_session_count=len(emitted_days),
        independent_events_per_20_sessions=per20,
        stressed_target_contribution_per_20_sessions=target_per20,
        stressed_expected_mll_consumption_per_20_sessions=adverse_per20,
        cost_adjusted_target_velocity=(target_per20 / 9000.0),
        day_concentration=_maximum_share(day_counts),
        block_concentration=(None if block_counts is None else _maximum_share(block_counts)),
        market_concentration=1.0 if events else 0.0,
        session_concentration=_maximum_share(_counts(row.session_code for row in events)),
        event_hash=stable_hash([row.to_dict() for row in events]),
    )


def exact_sleeve_replay(
    calibrated: CalibratedHazardCandidate,
    events: Sequence[HazardEventEvidence],
    *,
    eligible_session_days: Sequence[int],
) -> ExactHazardSleeveReplay:
    candidate = calibrated.candidate
    eligible_days = tuple(sorted({int(value) for value in eligible_session_days}))
    eligible_day_set = set(eligible_days)
    if any(row.session_day not in eligible_day_set for row in events):
        raise ValueError("event falls outside the frozen eligible session calendar")
    complete = tuple(
        row for row in events if row.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
    )
    normal = tuple(_trade_path_event(row, scenario="NORMAL") for row in complete)
    stressed = tuple(_trade_path_event(row, scenario="STRESSED_1_5X") for row in complete)
    normal_trajectories = tuple(
        _hazard_trajectory(row, event, scenario="NORMAL")
        for row, event in zip(complete, normal, strict=True)
    )
    stressed_trajectories = tuple(
        _hazard_trajectory(row, event, scenario="STRESSED_1_5X")
        for row, event in zip(complete, stressed, strict=True)
    )
    return ExactHazardSleeveReplay(
        candidate=candidate,
        events=tuple(events),
        normal_events=normal,
        stressed_events=stressed,
        normal_trajectories=normal_trajectories,
        stressed_trajectories=stressed_trajectories,
        eligible_session_days=eligible_days,
        decision_hash=stable_hash(
            [
                {
                    "event_id": row.event_id,
                    "decision_time_ns": row.decision_time_ns,
                    "direction": row.direction,
                    "feature_fingerprint": row.feature_fingerprint,
                }
                for row in events
            ]
        ),
        normal_event_hash=stable_hash([row.to_dict() for row in normal]),
        stressed_event_hash=stable_hash([row.to_dict() for row in stressed]),
        normal_trajectory_hash=stable_hash(
            [row.to_dict() for row in normal_trajectories]
        ),
        stressed_trajectory_hash=stable_hash(
            [row.to_dict() for row in stressed_trajectories]
        ),
        fill_policy_hash=(events[0].fill_policy_hash if events else CausalFillPolicy().fingerprint),
    )


def realized_behavioral_fingerprint(
    events: Sequence[HazardEventEvidence],
) -> str:
    """Hash actual decisions/trades, never the proposal specification."""

    return stable_hash(
        [
            {
                "event_time_ns": row.event_time_ns,
                "decision_time_ns": row.decision_time_ns,
                "fill_time_ns": row.fill_time_ns,
                "outcome_time_ns": row.outcome_time_ns,
                "market": row.market,
                "contract_code": row.contract_code,
                "session_day": row.session_day,
                "session_code": row.session_code,
                "direction": row.direction,
                "quantity": row.quantity,
                "outcome": row.outcome,
                "normal_net_pnl": row.normal_net_pnl,
                "stressed_net_pnl": row.stressed_net_pnl,
                "censor_reason": row.censor_reason,
            }
            for row in events
            if row.evidence_role == "CANDIDATE"
        ]
    )


def matched_random_intents(
    calibrated: CalibratedHazardCandidate,
    matrix: FeatureMatrix,
    observed: Sequence[HazardIntent],
    *,
    evaluation_start_ns: int,
    evaluation_end_exclusive_ns: int,
    seed: int,
) -> tuple[HazardIntent, ...]:
    """Day/session/direction/duty/exposure matched random-event control.

    The control has the same candidate risk and holding contract, an identical
    event count in every day/session/direction cell, valid next-open coverage,
    and the same non-overlap cooldown.  Sharing the observed session days also
    makes any externally frozen temporal-block assignment identical.
    """

    if not observed:
        return ()
    candidate = calibrated.candidate
    timestamp = matrix.array("timestamp_ns")
    decision = matrix.array("decision_ns")
    available = matrix.array("availability_ns")
    sessions = matrix.array("session_code")
    days = matrix.array("session_day")
    segments = matrix.array("segment_code")
    contracts = matrix.array("contract_code")
    required_groups = _counts(
        (row.session_day, row.session_code, row.direction) for row in observed
    )
    rng = np.random.default_rng(int(seed))
    output: list[HazardIntent] = []
    selected_by_segment: dict[int, list[int]] = {}
    observed_indices = {row.row_index for row in observed}
    control_id = "matched_random_" + stable_hash(
        {
            "candidate_id": candidate.candidate_id,
            "seed": int(seed),
            "source_intents": [row.fingerprint for row in observed],
            "contract": "DAY_SESSION_DIRECTION_DUTY_EXPOSURE_NEXT_OPEN_MATCHED_V1",
        }
    )[:24]
    namespace = f"{candidate.candidate_id}:{control_id}"
    for (session_day, session_code, required_direction), count in sorted(
        required_groups.items()
    ):
        initial = np.flatnonzero(
            (timestamp >= int(evaluation_start_ns))
            & (timestamp < int(evaluation_end_exclusive_ns))
            & (days == int(session_day))
            & (sessions == int(session_code))
            & (available <= decision)
            & _completed_timeframe_boundary_mask(timestamp, candidate.timeframe)
        )
        eligible = np.asarray(
            [
                int(index)
                for index in initial
                if int(index) not in observed_indices
                and _row_has_valid_next_open(
                    int(index),
                    timestamp=timestamp,
                    decision=decision,
                    available=available,
                    segments=segments,
                    contracts=contracts,
                    days=days,
                    allow_overnight=candidate.horizon == "OVERNIGHT",
                )
            ],
            dtype=np.int64,
        )
        if len(eligible) < count:
            raise ValueError("insufficient fully matched random null population")
        chosen_rows: list[int] = []
        cooldown_ns = candidate.cooldown_minutes * MINUTE_NS
        for raw_index in rng.permutation(eligible):
            index = int(raw_index)
            key = int(segments[index])
            event_time = int(timestamp[index])
            if any(
                abs(event_time - prior_time) < cooldown_ns
                for prior_time in selected_by_segment.get(key, ())
            ):
                continue
            chosen_rows.append(index)
            selected_by_segment.setdefault(key, []).append(event_time)
            if len(chosen_rows) == count:
                break
        if len(chosen_rows) != count:
            raise ValueError("insufficient non-overlapping matched random null population")
        chosen = np.sort(np.asarray(chosen_rows, dtype=np.int64))
        for raw_index in chosen:
            index = int(raw_index)
            direction = int(required_direction)
            feature_fingerprint = stable_hash(
                {
                    "control": "DAY_SESSION_DIRECTION_DUTY_EXPOSURE_MATCHED_RANDOM",
                    "control_id": control_id,
                    "seed": int(seed),
                    "row_index": index,
                    "source_matrix_hash": matrix.fingerprint,
                }
            )
            output.append(
                HazardIntent(
                    candidate_id=candidate.candidate_id,
                    intent_namespace=namespace,
                    evidence_role="MATCHED_RANDOM_CONTROL",
                    control_id=control_id,
                    row_index=index,
                    market=candidate.market,
                    contract_code=int(contracts[index]),
                    session_day=int(days[index]),
                    session_code=int(sessions[index]),
                    segment_code=int(segments[index]),
                    event_time_ns=int(timestamp[index]),
                    available_at_ns=int(available[index]),
                    decision_time_ns=int(decision[index]),
                    order_submit_time_ns=int(decision[index]),
                    entry_intent=(
                        "ENTER_LONG_NEXT_TRADABLE_OPEN"
                        if direction > 0
                        else "ENTER_SHORT_NEXT_TRADABLE_OPEN"
                    ),
                    earliest_executable_time_ns=_next_executable_boundary(
                        int(decision[index]), int(available[index])
                    ),
                    direction=direction,
                    feature_fingerprint=feature_fingerprint,
                )
            )
    return tuple(sorted(output, key=lambda row: (row.event_time_ns, row.fingerprint)))


def direction_flipped_intents(
    intents: Sequence[HazardIntent],
) -> tuple[HazardIntent, ...]:
    """Exposure- and timing-identical direction-flipped control."""

    control_id = "direction_flipped_" + stable_hash(
        [row.fingerprint for row in intents]
    )[:24]
    return tuple(
        HazardIntent(
            **{
                **asdict(row),
                "intent_namespace": f"{row.candidate_id}:{control_id}",
                "evidence_role": "DIRECTION_FLIPPED_CONTROL",
                "control_id": control_id,
                "direction": -row.direction,
                "entry_intent": (
                    "ENTER_LONG_NEXT_TRADABLE_OPEN"
                    if -row.direction > 0
                    else "ENTER_SHORT_NEXT_TRADABLE_OPEN"
                ),
                "feature_fingerprint": stable_hash(
                    {
                        "control": "DIRECTION_FLIPPED",
                        "source_intent_fingerprint": row.fingerprint,
                    }
                ),
            }
        )
        for row in intents
    )


def _traverse_barriers(
    candidate: HazardCandidate,
    intent: HazardIntent,
    *,
    entry_index: int,
    normal_entry: float,
    favorable_price: float,
    adverse_price: float,
    risk_unit: float,
    timestamp: np.ndarray,
    availability: np.ndarray,
    opens: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    segments: np.ndarray,
    contracts: np.ndarray,
    days: np.ndarray,
) -> dict[str, Any]:
    entry_time = int(timestamp[entry_index])
    deadline = entry_time + _numeric_horizon(candidate.horizon) * MINUTE_NS
    best = normal_entry
    worst = normal_entry
    prior_timestamp = entry_time - MINUTE_NS
    final_index: int | None = None
    path_indices: list[int] = []
    for index in range(entry_index, len(timestamp)):
        ts = int(timestamp[index])
        if (
            int(segments[index]) != intent.segment_code
            or int(contracts[index]) != intent.contract_code
        ):
            return _censor_observation("HOLDING_PATH_SEGMENT_CONTRACT_CHANGED")
        if int(days[index]) != intent.session_day and candidate.horizon != "OVERNIGHT":
            if candidate.horizon == "SESSION" and final_index is not None:
                raw_exit = float(closes[final_index])
                if not math.isfinite(raw_exit):
                    return _censor_observation("SESSION_FLATTEN_CLOSE_NONFINITE")
                return _observed_neither(
                    intent,
                    normal_entry=normal_entry,
                    risk_unit=risk_unit,
                    best=best,
                    worst=worst,
                    raw_exit=raw_exit,
                    outcome_time_ns=int(availability[final_index]),
                    exit_fill_semantics=(
                        "PREDECLARED_SESSION_CLOSE_FLATTEN_WITH_FROZEN_SLIPPAGE"
                    ),
                    path_indices=path_indices,
                )
            return _censor_observation("SESSION_ENDED_BEFORE_FROZEN_HORIZON")
        # Validate the complete holding path before accepting an exact
        # deadline open.  A deadline row after missing interior minutes cannot
        # prove that neither barrier was touched inside the gap.
        if index > entry_index and ts != prior_timestamp + MINUTE_NS:
            return _censor_observation("HOLDING_PATH_MISSING_INTERVAL")
        # A fixed-horizon time exit is submitted in advance and fills at the
        # open whose timestamp equals the exact deadline.  Its OHLC must not be
        # inspected before that open fill.
        if candidate.horizon != "SESSION" and ts >= deadline:
            if ts != deadline or not math.isfinite(float(opens[index])):
                return _censor_observation("MAX_HORIZON_NEXT_OPEN_UNOBSERVED")
            return _observed_neither(
                intent,
                normal_entry=normal_entry,
                risk_unit=risk_unit,
                best=best,
                worst=worst,
                raw_exit=float(opens[index]),
                outcome_time_ns=ts,
                exit_fill_semantics="MAX_HORIZON_NEXT_TRADABLE_OPEN",
                path_indices=path_indices,
            )
        if not all(math.isfinite(float(value)) for value in (opens[index], highs[index], lows[index], closes[index])):
            return _censor_observation("HOLDING_PATH_NONFINITE_OHLC")
        prior_timestamp = ts
        high = float(highs[index])
        low = float(lows[index])
        path_indices.append(index)
        prior_best = best
        prior_worst = worst
        if intent.direction > 0:
            favorable_hit = high >= favorable_price
            adverse_hit = low <= adverse_price
            best = max(best, high)
            worst = min(worst, low)
        else:
            favorable_hit = low <= favorable_price
            adverse_hit = high >= adverse_price
            best = min(best, low)
            worst = max(worst, high)
        elapsed = int((ts - int(timestamp[entry_index])) // MINUTE_NS)
        if favorable_hit or adverse_hit:
            # When OHLC cannot order both touches, use the adverse result.  This
            # is deterministic and conservative, never an optimistic label.
            adverse_first = bool(adverse_hit)
            raw_exit = adverse_price if adverse_first else favorable_price
            terminal_best = prior_best if adverse_first else favorable_price
            terminal_worst = adverse_price if adverse_first else worst
            return {
                "outcome": (
                    HazardOutcome.ADVERSE_FIRST
                    if adverse_first
                    else HazardOutcome.FAVORABLE_FIRST
                ),
                "outcome_time_ns": int(availability[index]),
                "time_to_favorable_minutes": (None if adverse_first else elapsed),
                "time_to_adverse_minutes": (elapsed if adverse_first else None),
                "mfe_r": max(
                    0.0,
                    (terminal_best - normal_entry) * intent.direction / risk_unit,
                ),
                "mae_r": max(
                    0.0,
                    (normal_entry - terminal_worst) * intent.direction / risk_unit,
                ),
                "raw_exit_price": raw_exit,
                "best_raw_price": terminal_best,
                "worst_raw_price": terminal_worst,
                "same_bar_ambiguous": bool(favorable_hit and adverse_hit),
                "path_indices": tuple(path_indices),
                "exit_fill_semantics": (
                    "RESTING_ADVERSE_BARRIER_INTRABAR_CONSERVATIVE"
                    if adverse_first
                    else "RESTING_FAVORABLE_BARRIER_INTRABAR"
                ),
                "censor_reason": None,
            }
        final_index = index
    return _censor_observation(
        "SESSION_TRANSITION_OR_MAX_HORIZON_EXIT_NOT_OBSERVED_BEFORE_DATA_END"
    )


def _observed_neither(
    intent: HazardIntent,
    *,
    normal_entry: float,
    risk_unit: float,
    best: float,
    worst: float,
    raw_exit: float,
    outcome_time_ns: int,
    exit_fill_semantics: str,
    path_indices: Sequence[int],
) -> dict[str, Any]:
    return {
        "outcome": HazardOutcome.NEITHER_REACHED,
        "outcome_time_ns": int(outcome_time_ns),
        "time_to_favorable_minutes": None,
        "time_to_adverse_minutes": None,
        "mfe_r": max(0.0, (best - normal_entry) * intent.direction / risk_unit),
        "mae_r": max(0.0, (normal_entry - worst) * intent.direction / risk_unit),
        "raw_exit_price": float(raw_exit),
        "best_raw_price": best,
        "worst_raw_price": worst,
        "same_bar_ambiguous": False,
        "path_indices": tuple(int(value) for value in path_indices),
        "exit_fill_semantics": exit_fill_semantics,
        "censor_reason": None,
    }


def _economic_marks(
    path_indices: Sequence[int],
    *,
    entry: float,
    direction: int,
    quantity: int,
    point_value: float,
    commission: float,
    availability: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    terminal_net: float,
    barrier_outcome: str | None,
    terminal_raw_exit: float,
) -> tuple[CausalTradeMark, ...]:
    marks: list[CausalTradeMark] = []
    for ordinal, raw_index in enumerate(path_indices):
        index = int(raw_index)
        adverse_price = float(lows[index]) if direction > 0 else float(highs[index])
        favorable_price = float(highs[index]) if direction > 0 else float(lows[index])
        worst = (
            (adverse_price - entry)
            * direction
            * point_value
            * quantity
            - commission
        )
        best = (
            (favorable_price - entry)
            * direction
            * point_value
            * quantity
            - commission
        )
        current = (
            (float(closes[index]) - entry)
            * direction
            * point_value
            * quantity
            - commission
        )
        if barrier_outcome is not None and ordinal == len(path_indices) - 1:
            current = float(terminal_net)
            barrier_mark = (
                (float(terminal_raw_exit) - entry)
                * direction
                * point_value
                * quantity
                - commission
            )
            if barrier_outcome == HazardOutcome.ADVERSE_FIRST:
                # Once the resting stop fills, later high/low values in that
                # minute do not belong to the account path.
                worst = min(float(terminal_net), float(barrier_mark))
                best = float(terminal_net)
            elif barrier_outcome == HazardOutcome.FAVORABLE_FIRST:
                # Preserve the conservative pre-target adverse extreme (the
                # stop was not touched), but cap upside at the filled target.
                best = max(float(terminal_net), float(barrier_mark))
        marks.append(
            CausalTradeMark(
                availability_time_ns=int(availability[index]),
                worst_unrealized_pnl=float(worst),
                best_unrealized_pnl=float(best),
                current_unrealized_pnl=float(current),
            )
        )
    return tuple(marks)


def _censored_event(
    candidate: HazardCandidate,
    intent: HazardIntent,
    *,
    ordinal: int,
    quantity: int,
    reason: str,
    fill_policy_hash: str,
    fill_time_ns: int | None = None,
    raw_fill: float | None = None,
    normal_fill: float | None = None,
    stressed_fill: float | None = None,
    risk_unit: float = 0.0,
    favorable_price: float | None = None,
    adverse_price: float | None = None,
) -> HazardEventEvidence:
    return HazardEventEvidence(
        event_id=f"{intent.intent_namespace}:{ordinal:07d}",
        candidate_id=candidate.candidate_id,
        intent_namespace=intent.intent_namespace,
        evidence_role=intent.evidence_role,
        control_id=intent.control_id,
        market=candidate.market,
        execution_market=candidate.execution_market,
        contract_code=int(intent.contract_code),
        timeframe=candidate.timeframe,
        session_day=int(intent.session_day),
        session_code=int(intent.session_code),
        segment_code=int(intent.segment_code),
        event_time_ns=int(intent.event_time_ns),
        available_at_ns=int(intent.available_at_ns),
        decision_time_ns=int(intent.decision_time_ns),
        order_submit_time_ns=int(intent.order_submit_time_ns),
        entry_intent=intent.entry_intent,
        earliest_executable_time_ns=int(intent.earliest_executable_time_ns),
        fill_time_ns=fill_time_ns,
        raw_fill_price=raw_fill,
        normal_fill_price=normal_fill,
        stressed_fill_price=stressed_fill,
        direction=int(intent.direction),
        quantity=int(quantity),
        risk_unit_price=float(risk_unit),
        favorable_r=float(candidate.favorable_r),
        adverse_r=float(candidate.adverse_r),
        favorable_price=favorable_price,
        adverse_price=adverse_price,
        maximum_horizon=candidate.horizon,
        outcome=HazardOutcome.CENSORED_FUTURE_COVERAGE,
        outcome_time_ns=None,
        time_to_favorable_minutes=None,
        time_to_adverse_minutes=None,
        maximum_favorable_excursion_r=None,
        maximum_adverse_excursion_r=None,
        raw_exit_price=None,
        exit_fill_semantics="UNFILLED_CENSORED",
        normal_net_pnl=None,
        stressed_net_pnl=None,
        normal_worst_unrealized_pnl=None,
        stressed_worst_unrealized_pnl=None,
        normal_best_unrealized_pnl=None,
        stressed_best_unrealized_pnl=None,
        normal_initial_unrealized_pnl=None,
        stressed_initial_unrealized_pnl=None,
        normal_marks=(),
        stressed_marks=(),
        same_bar_ambiguous=False,
        censor_reason=reason,
        feature_fingerprint=intent.feature_fingerprint,
        fill_policy_id=CAUSAL_FILL_POLICY_ID,
        fill_policy_hash=fill_policy_hash,
    )


def _trade_path_event(row: HazardEventEvidence, *, scenario: str) -> TradePathEvent:
    if row.outcome == HazardOutcome.CENSORED_FUTURE_COVERAGE:
        raise ValueError("censored labels are not executable completed trades")
    if row.fill_time_ns is None or row.outcome_time_ns is None:
        raise ValueError("completed hazard event lacks fill/exit time")
    normal = scenario == "NORMAL"
    net = row.normal_net_pnl if normal else row.stressed_net_pnl
    worst = (
        row.normal_worst_unrealized_pnl
        if normal
        else row.stressed_worst_unrealized_pnl
    )
    best = (
        row.normal_best_unrealized_pnl
        if normal
        else row.stressed_best_unrealized_pnl
    )
    if net is None or worst is None or best is None:
        raise ValueError("completed hazard event lacks economic evidence")
    if row.raw_fill_price is None or row.raw_exit_price is None:
        raise ValueError("completed hazard event lacks raw price evidence")
    gross = (
        (float(row.raw_exit_price) - float(row.raw_fill_price))
        * int(row.direction)
        * float(instrument_spec(row.execution_market).point_value)
        * int(row.quantity)
    )
    return TradePathEvent(
        event_id=f"{row.event_id}:{scenario}",
        decision_ns=int(row.fill_time_ns),
        exit_ns=int(row.outcome_time_ns),
        session_day=int(row.session_day),
        net_pnl=float(net),
        gross_pnl=float(gross),
        worst_unrealized_pnl=float(worst),
        best_unrealized_pnl=float(best),
        quantity=int(row.quantity),
        mini_equivalent=float(mini_equivalent(row.execution_market, row.quantity)),
        regime="CAUSAL_TARGET_VELOCITY",
        session_compliant=True,
        contract_limit_compliant=True,
        same_bar_ambiguous=bool(row.same_bar_ambiguous),
    )


def _hazard_trajectory(
    row: HazardEventEvidence,
    event: TradePathEvent,
    *,
    scenario: str,
) -> CausalTradeTrajectory:
    normal = scenario == "NORMAL"
    marks = row.normal_marks if normal else row.stressed_marks
    initial = (
        row.normal_initial_unrealized_pnl
        if normal
        else row.stressed_initial_unrealized_pnl
    )
    if initial is None or not marks:
        raise ValueError("completed hazard event lacks chronological mark evidence")
    return CausalTradeTrajectory(
        component_id=row.candidate_id,
        market=row.execution_market,
        side=int(row.direction),
        event=event,
        marks=marks,
        initial_unrealized_pnl=float(initial),
    )


def _compare(
    value: np.ndarray,
    operator: str,
    threshold: float,
    *,
    reference: np.ndarray | None,
) -> np.ndarray:
    if operator == "GT":
        return value > threshold
    if operator == "LT":
        return value < threshold
    if operator == "ABS_GT":
        return np.abs(value) > abs(threshold)
    if operator == "SAME_SIGN":
        if reference is None:
            raise ValueError("SAME_SIGN context requires trigger reference")
        return (np.sign(value) == np.sign(reference)) & (np.abs(value) > abs(threshold))
    raise ValueError(f"unsupported causal operator: {operator}")


def _direction(
    rule: str,
    *,
    trigger: np.ndarray,
    context: np.ndarray | None,
    past_return: np.ndarray,
) -> np.ndarray:
    if rule == "PAST_RETURN_CONTINUATION":
        return np.sign(past_return)
    if rule == "PAST_RETURN_REVERSAL":
        return -np.sign(past_return)
    if rule == "TRIGGER_SIGN_CONTINUATION":
        return np.sign(trigger)
    if rule == "CONTEXT_SIGN_REVERSAL":
        if context is None:
            raise ValueError("context-sign direction requires context")
        return -np.sign(context)
    raise ValueError(f"unsupported causal direction rule: {rule}")


def _quantile_threshold(values: np.ndarray, operator: str, quantile: float) -> float:
    if operator in {"ABS_GT", "SAME_SIGN"}:
        values = np.abs(values)
    resolved_quantile = 1.0 - quantile if operator == "LT" else quantile
    return float(np.quantile(values, resolved_quantile))


def _cooldown_indices(
    indices: np.ndarray,
    *,
    timestamp: np.ndarray,
    segment: np.ndarray,
    cooldown_ns: int,
) -> np.ndarray:
    retained: list[int] = []
    next_time: dict[int, int] = {}
    for raw in indices:
        index = int(raw)
        key = int(segment[index])
        if int(timestamp[index]) < next_time.get(key, -1):
            continue
        retained.append(index)
        next_time[key] = int(timestamp[index]) + int(cooldown_ns)
    return np.asarray(retained, dtype=np.int64)


def _same_entry_path(
    intent: HazardIntent,
    entry_index: int,
    *,
    segments: np.ndarray,
    contracts: np.ndarray,
    days: np.ndarray,
    allow_overnight: bool,
) -> bool:
    return bool(
        int(segments[entry_index]) == intent.segment_code
        and int(contracts[entry_index]) == intent.contract_code
        and (allow_overnight or int(days[entry_index]) == intent.session_day)
    )


def _row_has_valid_next_open(
    row_index: int,
    *,
    timestamp: np.ndarray,
    decision: np.ndarray,
    available: np.ndarray,
    segments: np.ndarray,
    contracts: np.ndarray,
    days: np.ndarray,
    allow_overnight: bool,
) -> bool:
    earliest = _next_executable_boundary(
        int(decision[row_index]), int(available[row_index])
    )
    entry_index = int(np.searchsorted(timestamp, earliest))
    return bool(
        entry_index < len(timestamp)
        and int(timestamp[entry_index]) == earliest
        and int(segments[entry_index]) == int(segments[row_index])
        and int(contracts[entry_index]) == int(contracts[row_index])
        and (allow_overnight or int(days[entry_index]) == int(days[row_index]))
    )


def _next_executable_boundary(decision_ns: int, available_ns: int) -> int:
    raw = max(int(decision_ns), int(available_ns))
    return int(((raw + MINUTE_NS - 1) // MINUTE_NS) * MINUTE_NS)


def _completed_timeframe_boundary_mask(
    timestamp_ns: np.ndarray, timeframe: str
) -> np.ndarray:
    """True only after a completed frozen timeframe bar is available.

    The one-minute source row represents ``[timestamp, timestamp + 1m)``.
    UTC-aligned higher-timeframe descriptors therefore become actionable only
    when that completed interval ends on a 5/15/30/60-minute boundary.
    """

    minutes = TIMEFRAME_MINUTES[timeframe]
    if minutes == 1:
        return np.ones(len(timestamp_ns), dtype=bool)
    return ((timestamp_ns // MINUTE_NS) + 1) % minutes == 0


def _is_completed_timeframe_boundary(timestamp_ns: int, timeframe: str) -> bool:
    minutes = TIMEFRAME_MINUTES[timeframe]
    return bool(minutes == 1 or ((int(timestamp_ns) // MINUTE_NS) + 1) % minutes == 0)


def _numeric_horizon(value: int | str) -> int:
    if isinstance(value, int):
        return value
    if value == "SESSION":
        return 390
    if value == "OVERNIGHT":
        return 960
    raise ValueError(f"unknown frozen horizon: {value}")


def _censor_observation(reason: str) -> dict[str, Any]:
    return {"outcome": HazardOutcome.CENSORED_FUTURE_COVERAGE, "censor_reason": reason}


def _counts(values: Iterable[Any]) -> dict[Any, int]:
    output: dict[Any, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return output


def _maximum_share(counts: Mapping[Any, int]) -> float:
    total = sum(counts.values())
    return max(counts.values(), default=0) / total if total else 0.0


def _median_optional(values: Iterable[float | int | None]) -> float | None:
    finite = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return float(np.median(finite)) if finite else None


def _finite_or_none(value: float) -> float | None:
    resolved = float(value)
    return resolved if math.isfinite(resolved) else None


__all__ = [
    "ADVERSE_R_LEVELS",
    "CausalHazardStreamingDecisionKernel",
    "CalibratedHazardCandidate",
    "CROSS_ASSET_REFERENCE_MARKETS",
    "ENGINE_VERSION",
    "ExactHazardSleeveReplay",
    "FAVORABLE_R_LEVELS",
    "FrozenHazardDecisionRule",
    "HazardCandidate",
    "HazardEventEvidence",
    "HazardIntent",
    "HazardOutcome",
    "HazardScreenResult",
    "LABEL_HORIZONS",
    "MECHANISM_RECIPES",
    "SESSION_CODES",
    "STATIC_RISK_LEVELS",
    "calibrate_candidate",
    "deduplicate_for_event_screen",
    "direction_flipped_intents",
    "discover_intents_batch",
    "discover_intents_streaming",
    "exact_sleeve_replay",
    "frozen_eligible_session_calendar",
    "generate_structural_proposals",
    "matched_random_intents",
    "observe_outcomes",
    "realized_behavioral_fingerprint",
    "screen_result",
    "with_availability_safe_cross_asset_feature",
]
