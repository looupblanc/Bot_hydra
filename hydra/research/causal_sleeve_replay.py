"""Availability-safe causal replay for the frozen HYDRA sleeves.

The module owns one stateful decision/fill kernel.  Historical batch replay and
one-record-at-a-time replay both feed immutable :class:`CausalBarRecord`
instances to :meth:`CausalSleeveStreamingKernel.step`; neither wrapper indexes
future rows.  Missing entry or exit coverage is recorded after the signal was
emitted and can never retroactively veto that signal.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_pool_replay import RoutedTrade
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import SleeveSpec
from hydra.execution.v7_cost_model import default_preregistration_path, load_cost_model
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.scaling_plan import mini_equivalent
from hydra.shadow.active_risk_package import FrozenSignalBinding


MINUTE_NS = 60_000_000_000
CAUSAL_DECISION_KERNEL_VERSION = "hydra_frozen_sleeve_causal_kernel_v2"
CAUSAL_FILL_POLICY_ID = "CAUSAL_NEXT_TRADABLE_OPEN_V1"
CENSORED_FUTURE_COVERAGE = "CENSORED_FUTURE_COVERAGE"
TARGET_OBSERVED = "TARGET_OUTCOME_OBSERVED"


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


def _file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _optional_finite(value: float | None) -> float | None:
    if value is None:
        return None
    resolved = float(value)
    return resolved if math.isfinite(resolved) else None


@dataclass(frozen=True, slots=True)
class CausalFillPolicy:
    """Frozen fill contract; its resolved hash also binds product costs."""

    policy_id: str = CAUSAL_FILL_POLICY_ID
    signal_basis: str = "COMPLETED_BAR_T_AVAILABILITY_TIME"
    order_submit_basis: str = "DECISION_AVAILABILITY_TIME"
    entry_reference: str = "NEXT_EXPECTED_TRADABLE_BAR_OPEN"
    exit_reference: str = "FROZEN_HOLDING_HORIZON_BAR_OPEN"
    normal_slippage_basis: str = "FROZEN_V7_HOLDING_HORIZON_SCHEDULE"
    stressed_slippage_multiplier: float = 1.5
    commission_basis: str = "FROZEN_V7_ROUND_TURN_COMMISSION"
    missing_coverage_action: str = CENSORED_FUTURE_COVERAGE
    interpolate_missing_bars: bool = False

    def __post_init__(self) -> None:
        if self.policy_id != CAUSAL_FILL_POLICY_ID:
            raise ValueError("causal fill-policy identity drift")
        if self.normal_slippage_basis != "FROZEN_V7_HOLDING_HORIZON_SCHEDULE":
            raise ValueError("causal slippage basis must remain the frozen V7 schedule")
        if not math.isclose(self.stressed_slippage_multiplier, 1.5):
            raise ValueError("stressed causal slippage multiplier must remain 1.5x")
        if self.interpolate_missing_bars:
            raise ValueError("causal fill policy cannot interpolate missing bars")

    @property
    def fingerprint(self) -> str:
        """Generic policy hash, including the authoritative cost-file hash."""

        return _stable_hash(
            {
                "policy": asdict(self),
                "cost_configuration_sha256": _file_sha256(
                    default_preregistration_path()
                ),
            }
        )

    def resolved_payload(
        self, execution_market: str, holding_bars: int
    ) -> dict[str, Any]:
        costs = load_cost_model()
        product = costs.products[execution_market]
        instrument = instrument_spec(execution_market)
        holding_horizon = f"{int(holding_bars)}m"
        try:
            normal_ticks = float(
                costs.base_slippage_ticks_per_side_by_horizon[holding_horizon]
            )
        except KeyError as exc:
            raise ValueError(
                "frozen V7 costs do not define the causal holding horizon: "
                f"{holding_horizon}"
            ) from exc
        stressed_ticks = normal_ticks * self.stressed_slippage_multiplier
        return {
            "policy": asdict(self),
            "execution_market": execution_market,
            "holding_horizon": holding_horizon,
            "normal_slippage_ticks_per_side": normal_ticks,
            "stressed_slippage_ticks_per_side": stressed_ticks,
            "stressed_slippage_multiplier": float(
                self.stressed_slippage_multiplier
            ),
            "commission_round_turn_usd": float(
                product.commission_round_turn_usd
            ),
            "tick_size": float(instrument.tick_size),
            "tick_value": float(instrument.tick_value),
            "point_value": float(instrument.point_value),
            "cost_source": costs.source,
            "cost_source_checked_utc": costs.source_checked_utc,
            "cost_configuration_path": "config/v7/phase0_g0_preregistration.json",
            "cost_configuration_sha256": _file_sha256(
                default_preregistration_path()
            ),
        }

    def resolved_fingerprint(self, execution_market: str, holding_bars: int) -> str:
        return _stable_hash(self.resolved_payload(execution_market, holding_bars))


@dataclass(frozen=True, slots=True)
class CausalOrderCheckpoint:
    last_timestamp_ns: int | None = None
    last_segment_code: int | None = None
    last_contract_code: int | None = None
    last_record_fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CausalOrderCheckpoint":
        return cls(
            last_timestamp_ns=_optional_int(value.get("last_timestamp_ns")),
            last_segment_code=_optional_int(value.get("last_segment_code")),
            last_contract_code=_optional_int(value.get("last_contract_code")),
            last_record_fingerprint=(
                None
                if value.get("last_record_fingerprint") is None
                else str(value["last_record_fingerprint"])
            ),
        )


class CausalInputOrderGuard:
    """Fail closed on late/altered bars and ignore only exact duplicates."""

    def __init__(self, checkpoint: CausalOrderCheckpoint | None = None) -> None:
        value = checkpoint or CausalOrderCheckpoint()
        self.last_timestamp_ns = value.last_timestamp_ns
        self.last_segment_code = value.last_segment_code
        self.last_contract_code = value.last_contract_code
        self.last_record_fingerprint = value.last_record_fingerprint

    def accept(
        self,
        *,
        timestamp_ns: int,
        segment_code: int,
        contract_code: int,
        record_fingerprint: str | None = None,
    ) -> bool:
        if self.last_timestamp_ns is not None:
            if int(timestamp_ns) < int(self.last_timestamp_ns):
                raise ValueError("OUT_OF_ORDER_CAUSAL_BAR")
            if int(timestamp_ns) == int(self.last_timestamp_ns):
                identity_equal = (
                    int(segment_code) == int(self.last_segment_code)
                    and int(contract_code) == int(self.last_contract_code)
                )
                fingerprint_equal = (
                    record_fingerprint is None
                    or self.last_record_fingerprint is None
                    or str(record_fingerprint) == str(self.last_record_fingerprint)
                )
                if identity_equal and fingerprint_equal:
                    return False
                raise ValueError("ALTERED_DUPLICATE_OR_TIMESTAMP_COLLISION")
        self.last_timestamp_ns = int(timestamp_ns)
        self.last_segment_code = int(segment_code)
        self.last_contract_code = int(contract_code)
        self.last_record_fingerprint = (
            None if record_fingerprint is None else str(record_fingerprint)
        )
        return True

    def checkpoint(self) -> CausalOrderCheckpoint:
        return CausalOrderCheckpoint(
            self.last_timestamp_ns,
            self.last_segment_code,
            self.last_contract_code,
            self.last_record_fingerprint,
        )


@dataclass(frozen=True, slots=True)
class CausalBarRecord:
    timestamp_ns: int
    decision_ns: int
    availability_ns: int
    session_day: int
    session_code: int
    segment_code: int
    contract_code: int
    bar_open: float
    bar_high: float
    bar_low: float
    bar_close: float
    trigger_value: float
    context_value: float | None
    regime_value: float | None

    def __post_init__(self) -> None:
        if self.decision_ns < self.timestamp_ns + MINUTE_NS:
            raise ValueError("causal decision precedes completed one-minute bar")
        if self.availability_ns < self.decision_ns:
            raise ValueError("causal availability precedes decision")

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp_ns": int(self.timestamp_ns),
            "decision_ns": int(self.decision_ns),
            "availability_ns": int(self.availability_ns),
            "session_day": int(self.session_day),
            "session_code": int(self.session_code),
            "segment_code": int(self.segment_code),
            "contract_code": int(self.contract_code),
            "bar_open": _optional_finite(self.bar_open),
            "bar_high": _optional_finite(self.bar_high),
            "bar_low": _optional_finite(self.bar_low),
            "bar_close": _optional_finite(self.bar_close),
            "trigger_value": _optional_finite(self.trigger_value),
            "context_value": _optional_finite(self.context_value),
            "regime_value": _optional_finite(self.regime_value),
        }

    @property
    def fingerprint(self) -> str:
        return _stable_hash(self.to_dict())


@dataclass(frozen=True, slots=True)
class CausalSignalEvidence:
    signal_id: str
    sleeve_id: str
    signal_time_ns: int
    decision_time_ns: int
    order_submit_time_ns: int
    earliest_executable_time_ns: int
    fill_time_ns: int | None
    raw_entry_open: float | None
    normal_entry_fill_price: float | None
    stressed_entry_fill_price: float | None
    exit_decision_time_ns: int | None
    exit_order_submit_time_ns: int | None
    exit_earliest_executable_time_ns: int | None
    exit_fill_time_ns: int | None
    raw_exit_open: float | None
    normal_exit_fill_price: float | None
    stressed_exit_fill_price: float | None
    session_day: int
    segment_code: int
    contract_code: int
    direction: int
    quantity: int
    outcome_status: str
    censor_reason: str | None
    censor_time_ns: int | None
    trigger_value: float
    context_value: float | None
    kernel_version: str
    fill_policy_id: str
    fill_policy_hash: str

    @property
    def fingerprint(self) -> str:
        return _stable_hash(asdict(self))

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "event_fingerprint": self.fingerprint}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CausalSignalEvidence":
        payload = dict(value)
        payload.pop("event_fingerprint", None)
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class CausalTradeMark:
    """One completed-bar valuation: current close plus conservative OHLC bounds."""

    availability_time_ns: int
    worst_unrealized_pnl: float
    best_unrealized_pnl: float
    current_unrealized_pnl: float | None = None

    def __post_init__(self) -> None:
        for field in ("worst_unrealized_pnl", "best_unrealized_pnl"):
            if not math.isfinite(float(getattr(self, field))):
                raise ValueError(f"{field} must be finite")
        if self.current_unrealized_pnl is not None and not math.isfinite(
            float(self.current_unrealized_pnl)
        ):
            raise ValueError("current unrealized PnL must be finite")
        if self.worst_unrealized_pnl > self.best_unrealized_pnl + 1e-12:
            raise ValueError("causal mark worst/best bounds inverted")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CausalTradeMark":
        return cls(
            availability_time_ns=int(value["availability_time_ns"]),
            worst_unrealized_pnl=float(value["worst_unrealized_pnl"]),
            best_unrealized_pnl=float(value["best_unrealized_pnl"]),
            current_unrealized_pnl=(
                None
                if value.get("current_unrealized_pnl") is None
                else float(value["current_unrealized_pnl"])
            ),
        )


@dataclass(frozen=True, slots=True)
class CausalTradeTrajectory:
    component_id: str
    market: str
    side: int
    event: TradePathEvent
    marks: tuple[CausalTradeMark, ...]
    initial_unrealized_pnl: float = 0.0

    def __post_init__(self) -> None:
        if not self.marks:
            raise ValueError("causal trajectory must contain current-bar marks")
        if any(
            right.availability_time_ns <= left.availability_time_ns
            for left, right in zip(self.marks, self.marks[1:])
        ):
            raise ValueError("causal trajectory marks must be strictly chronological")
        if self.marks[-1].availability_time_ns != self.event.exit_ns:
            raise ValueError("last causal mark must precede exit at the same boundary")
        if not math.isfinite(float(self.initial_unrealized_pnl)):
            raise ValueError("initial unrealized PnL must be finite")

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "market": self.market,
            "side": self.side,
            "event": self.event.to_dict(),
            "marks": [row.to_dict() for row in self.marks],
            "initial_unrealized_pnl": self.initial_unrealized_pnl,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CausalTradeTrajectory":
        return cls(
            component_id=str(value["component_id"]),
            market=str(value["market"]),
            side=int(value["side"]),
            event=TradePathEvent(**dict(value["event"])),
            marks=tuple(CausalTradeMark.from_mapping(row) for row in value["marks"]),
            initial_unrealized_pnl=float(value.get("initial_unrealized_pnl", 0.0)),
        )


@dataclass(frozen=True, slots=True)
class CausalCensoredTrajectory:
    """A real entry whose exact future exit became unobservable.

    ``event`` is sized and uses the last observed close solely as an explicit
    censor-liquidation valuation.  It is never a completed trade or a claimed
    executable exit; account replay must terminate at ``censor_time_ns``.
    """

    component_id: str
    market: str
    side: int
    event: TradePathEvent
    marks: tuple[CausalTradeMark, ...]
    initial_unrealized_pnl: float
    terminal_status: str
    censor_time_ns: int
    censor_reason: str
    completed: bool = False
    liquidation_basis: str = "LAST_OBSERVED_CLOSE_NOT_AN_EXECUTED_EXIT"

    def __post_init__(self) -> None:
        if self.terminal_status != CENSORED_FUTURE_COVERAGE or self.completed:
            raise ValueError("censored trajectory terminal semantics drift")
        if self.censor_time_ns < self.event.decision_ns:
            raise ValueError("censor cannot precede the entry")
        if any(
            right.availability_time_ns <= left.availability_time_ns
            for left, right in zip(self.marks, self.marks[1:])
        ):
            raise ValueError("censored marks must be strictly chronological")
        if not math.isfinite(float(self.initial_unrealized_pnl)):
            raise ValueError("censored initial unrealized PnL must be finite")

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "market": self.market,
            "side": self.side,
            "event": self.event.to_dict(),
            "marks": [row.to_dict() for row in self.marks],
            "initial_unrealized_pnl": self.initial_unrealized_pnl,
            "terminal_status": self.terminal_status,
            "censor_time_ns": self.censor_time_ns,
            "censor_reason": self.censor_reason,
            "completed": self.completed,
            "liquidation_basis": self.liquidation_basis,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CausalCensoredTrajectory":
        return cls(
            component_id=str(value["component_id"]),
            market=str(value["market"]),
            side=int(value["side"]),
            event=TradePathEvent(**dict(value["event"])),
            marks=tuple(CausalTradeMark.from_mapping(row) for row in value["marks"]),
            initial_unrealized_pnl=float(value["initial_unrealized_pnl"]),
            terminal_status=str(value["terminal_status"]),
            censor_time_ns=int(value["censor_time_ns"]),
            censor_reason=str(value["censor_reason"]),
            completed=bool(value.get("completed", False)),
            liquidation_basis=str(
                value.get(
                    "liquidation_basis",
                    "LAST_OBSERVED_CLOSE_NOT_AN_EXECUTED_EXIT",
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class CausalSleeveReplay:
    sleeve_id: str
    signal_count: int
    completed_trade_count: int
    censored_signal_count: int
    eligible_session_days: tuple[int, ...]
    normal_events: tuple[TradePathEvent, ...]
    stressed_events: tuple[TradePathEvent, ...]
    normal_trajectories: tuple[CausalTradeTrajectory, ...]
    stressed_trajectories: tuple[CausalTradeTrajectory, ...]
    normal_censored_trajectories: tuple[CausalCensoredTrajectory, ...]
    stressed_censored_trajectories: tuple[CausalCensoredTrajectory, ...]
    signals: tuple[CausalSignalEvidence, ...]
    decision_hash: str
    normal_event_hash: str
    stressed_event_hash: str
    normal_censored_trajectory_hash: str
    stressed_censored_trajectory_hash: str
    fill_policy_hash: str
    specification_hash: str

    def __post_init__(self) -> None:
        if self.signal_count != len(self.signals):
            raise ValueError("causal signal count drift")
        if self.completed_trade_count != len(self.normal_events):
            raise ValueError("causal completed-trade count drift")
        if len(self.normal_events) != len(self.stressed_events):
            raise ValueError("normal/stressed causal event count drift")
        if len(self.normal_events) != len(self.normal_trajectories) or len(
            self.stressed_events
        ) != len(self.stressed_trajectories):
            raise ValueError("causal event/trajectory count drift")
        if len(self.normal_censored_trajectories) != len(
            self.stressed_censored_trajectories
        ):
            raise ValueError("normal/stressed censored trajectory count drift")
        if self.censored_signal_count != sum(
            row.outcome_status == CENSORED_FUTURE_COVERAGE for row in self.signals
        ):
            raise ValueError("causal censor count drift")

    def to_dict(self, *, include_events: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema": "hydra_causal_sleeve_replay_v2",
            "sleeve_id": self.sleeve_id,
            "signal_count": self.signal_count,
            "completed_trade_count": self.completed_trade_count,
            "censored_signal_count": self.censored_signal_count,
            "filled_censored_trajectory_count": len(
                self.normal_censored_trajectories
            ),
            "eligible_session_days": list(self.eligible_session_days),
            "decision_hash": self.decision_hash,
            "normal_event_hash": self.normal_event_hash,
            "stressed_event_hash": self.stressed_event_hash,
            "normal_censored_trajectory_hash": self.normal_censored_trajectory_hash,
            "stressed_censored_trajectory_hash": self.stressed_censored_trajectory_hash,
            "fill_policy_hash": self.fill_policy_hash,
            "specification_hash": self.specification_hash,
        }
        if include_events:
            payload["signals"] = [row.to_dict() for row in self.signals]
            payload["normal_events"] = [row.to_dict() for row in self.normal_events]
            payload["stressed_events"] = [row.to_dict() for row in self.stressed_events]
            payload["normal_trajectories"] = [
                row.to_dict() for row in self.normal_trajectories
            ]
            payload["stressed_trajectories"] = [
                row.to_dict() for row in self.stressed_trajectories
            ]
            payload["normal_censored_trajectories"] = [
                row.to_dict() for row in self.normal_censored_trajectories
            ]
            payload["stressed_censored_trajectories"] = [
                row.to_dict() for row in self.stressed_censored_trajectories
            ]
        return payload


class FrozenSleeveDecisionKernel:
    """The single authoritative, future-label-free decision rule."""

    def __init__(self, spec: SleeveSpec, binding: FrozenSignalBinding) -> None:
        if spec.sleeve_id != binding.sleeve_id:
            raise ValueError("causal kernel sleeve/binding identity drift")
        self.spec = spec
        self.binding = binding

    def eligible(
        self,
        *,
        trigger_value: float,
        context_value: float | None,
        session_code: int,
    ) -> bool:
        if not math.isfinite(float(trigger_value)):
            return False
        if self.spec.session_code >= 0:
            if int(session_code) != int(self.spec.session_code):
                return False
        elif int(session_code) < 0:
            return False
        if not _compare(
            float(trigger_value),
            self.binding.trigger_operator,
            float(self.binding.trigger_threshold),
        ):
            return False
        if self.binding.context_feature is None:
            return True
        if context_value is None or not math.isfinite(float(context_value)):
            return False
        return _compare(
            float(context_value),
            str(self.binding.context_operator),
            float(self.binding.context_threshold),
        )


@dataclass(slots=True)
class _PendingSignal:
    common: dict[str, Any]
    expected_entry_ns: int
    source_session_day: int
    source_segment_code: int
    source_contract_code: int
    regime: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "_PendingSignal":
        return cls(
            common=dict(value["common"]),
            expected_entry_ns=int(value["expected_entry_ns"]),
            source_session_day=int(value["source_session_day"]),
            source_segment_code=int(value["source_segment_code"]),
            source_contract_code=int(value["source_contract_code"]),
            regime=str(value["regime"]),
        )


@dataclass(slots=True)
class _OpenTrade:
    pending: _PendingSignal
    fill_ns: int
    raw_entry_open: float
    normal_entry: float
    stressed_entry: float
    expected_exit_ns: int
    last_bar_timestamp_ns: int | None
    normal_marks: list[CausalTradeMark]
    stressed_marks: list[CausalTradeMark]

    def to_dict(self) -> dict[str, Any]:
        return {
            "pending": self.pending.to_dict(),
            "fill_ns": self.fill_ns,
            "raw_entry_open": self.raw_entry_open,
            "normal_entry": self.normal_entry,
            "stressed_entry": self.stressed_entry,
            "expected_exit_ns": self.expected_exit_ns,
            "last_bar_timestamp_ns": self.last_bar_timestamp_ns,
            "normal_marks": [row.to_dict() for row in self.normal_marks],
            "stressed_marks": [row.to_dict() for row in self.stressed_marks],
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "_OpenTrade":
        return cls(
            pending=_PendingSignal.from_mapping(value["pending"]),
            fill_ns=int(value["fill_ns"]),
            raw_entry_open=float(value["raw_entry_open"]),
            normal_entry=float(value["normal_entry"]),
            stressed_entry=float(value["stressed_entry"]),
            expected_exit_ns=int(value["expected_exit_ns"]),
            last_bar_timestamp_ns=_optional_int(value.get("last_bar_timestamp_ns")),
            normal_marks=[
                CausalTradeMark.from_mapping(row) for row in value["normal_marks"]
            ],
            stressed_marks=[
                CausalTradeMark.from_mapping(row) for row in value["stressed_marks"]
            ],
        )


@dataclass(frozen=True, slots=True)
class CausalSleeveCheckpoint:
    payload: Mapping[str, Any]
    checkpoint_hash: str

    def __post_init__(self) -> None:
        if _stable_hash(dict(self.payload)) != self.checkpoint_hash:
            raise ValueError("causal sleeve checkpoint hash drift")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "hydra_causal_sleeve_checkpoint_v1",
            "payload": dict(self.payload),
            "checkpoint_hash": self.checkpoint_hash,
        }

    @classmethod
    def create(cls, payload: Mapping[str, Any]) -> "CausalSleeveCheckpoint":
        frozen = json.loads(json.dumps(payload, sort_keys=True, allow_nan=False))
        return cls(payload=frozen, checkpoint_hash=_stable_hash(frozen))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CausalSleeveCheckpoint":
        if value.get("schema") != "hydra_causal_sleeve_checkpoint_v1":
            raise ValueError("unsupported causal sleeve checkpoint schema")
        return cls(
            payload=dict(value["payload"]),
            checkpoint_hash=str(value["checkpoint_hash"]),
        )


class CausalSleeveStreamingKernel:
    """Checkpointable one-bar causal sleeve replay state machine."""

    def __init__(
        self,
        spec: SleeveSpec,
        binding: FrozenSignalBinding,
        *,
        fill_policy: CausalFillPolicy | None = None,
        checkpoint: CausalSleeveCheckpoint | Mapping[str, Any] | None = None,
    ) -> None:
        self.spec = spec
        self.binding = binding
        self.decision_kernel = FrozenSleeveDecisionKernel(spec, binding)
        self.fill_policy = fill_policy or CausalFillPolicy()
        self.fill_policy_payload = self.fill_policy.resolved_payload(
            spec.execution_market, spec.holding_bars
        )
        self.fill_policy_hash = _stable_hash(self.fill_policy_payload)
        self.instrument = instrument_spec(spec.execution_market)
        self.normal_slippage_ticks_per_side = float(
            self.fill_policy_payload["normal_slippage_ticks_per_side"]
        )
        self.stressed_slippage_ticks_per_side = float(
            self.fill_policy_payload["stressed_slippage_ticks_per_side"]
        )
        self.commission = float(
            self.fill_policy_payload["commission_round_turn_usd"]
        )
        self.specification_hash = _stable_hash(
            {
                "kernel_version": CAUSAL_DECISION_KERNEL_VERSION,
                "resolved_fill_policy": self.fill_policy_payload,
                "sleeve": spec.to_dict(),
                "binding": binding.to_dict(),
            }
        )
        self.order_guard = CausalInputOrderGuard()
        self.ordinal = 0
        self.pending: _PendingSignal | None = None
        self.open_trade: _OpenTrade | None = None
        self.blocked_path_identity: tuple[int, int, int] | None = None
        self.signals: list[CausalSignalEvidence] = []
        self.normal_trajectories: list[CausalTradeTrajectory] = []
        self.stressed_trajectories: list[CausalTradeTrajectory] = []
        self.normal_censored: list[CausalCensoredTrajectory] = []
        self.stressed_censored: list[CausalCensoredTrajectory] = []
        self.eligible_days: set[int] = set()
        self.regime_day: int | None = None
        self.regime_values: list[float] = []
        self.prior_regime = "UNKNOWN"
        self.last_availability_ns: int | None = None
        if checkpoint is not None:
            value = (
                checkpoint
                if isinstance(checkpoint, CausalSleeveCheckpoint)
                else CausalSleeveCheckpoint.from_mapping(checkpoint)
            )
            self._restore(value)

    @classmethod
    def from_checkpoint(
        cls,
        spec: SleeveSpec,
        binding: FrozenSignalBinding,
        checkpoint: CausalSleeveCheckpoint | Mapping[str, Any],
        *,
        fill_policy: CausalFillPolicy | None = None,
    ) -> "CausalSleeveStreamingKernel":
        return cls(
            spec,
            binding,
            fill_policy=fill_policy,
            checkpoint=checkpoint,
        )

    def step(self, record: CausalBarRecord) -> bool:
        """Consume exactly one completed bar; return false only for an exact duplicate."""

        if not self.order_guard.accept(
            timestamp_ns=record.timestamp_ns,
            segment_code=record.segment_code,
            contract_code=record.contract_code,
            record_fingerprint=record.fingerprint,
        ):
            return False
        self.last_availability_ns = int(record.availability_ns)
        self._advance_regime_day(record)
        if record.session_code >= 0:
            self.eligible_days.add(int(record.session_day))
        identity = _path_identity(record)
        if self.blocked_path_identity is not None and identity != self.blocked_path_identity:
            self.blocked_path_identity = None

        # At this bar's open: scheduled exits precede scheduled entries.
        self._resolve_open_exit_boundary(record)
        self._resolve_pending_entry_boundary(record)

        # The current OHLC becomes available only now, after its open fills.
        self._mark_open_position(record)

        earliest_entry = _next_executable_boundary(record)
        position_allows_renewal = self.open_trade is None or (
            self.open_trade.expected_exit_ns == earliest_entry
        )
        if (
            self.pending is None
            and position_allows_renewal
            and self.blocked_path_identity != identity
            and all(
                math.isfinite(float(value))
                for value in (record.bar_high, record.bar_low, record.bar_close)
            )
            and self.decision_kernel.eligible(
                trigger_value=record.trigger_value,
                context_value=record.context_value,
                session_code=record.session_code,
            )
        ):
            self.ordinal += 1
            signal_id = (
                f"{self.spec.sleeve_id}:CAUSAL:{self.ordinal:05d}:"
                f"{record.availability_ns}"
            )
            common = {
                "signal_id": signal_id,
                "sleeve_id": self.spec.sleeve_id,
                "signal_time_ns": int(record.availability_ns),
                "decision_time_ns": int(record.availability_ns),
                "order_submit_time_ns": int(record.availability_ns),
                "earliest_executable_time_ns": int(earliest_entry),
                "session_day": int(record.session_day),
                "segment_code": int(record.segment_code),
                "contract_code": int(record.contract_code),
                "direction": int(self.spec.side),
                "quantity": 1,
                "trigger_value": float(record.trigger_value),
                "context_value": _optional_finite(record.context_value),
                "kernel_version": CAUSAL_DECISION_KERNEL_VERSION,
                "fill_policy_id": self.fill_policy.policy_id,
                "fill_policy_hash": self.fill_policy_hash,
            }
            self.pending = _PendingSignal(
                common=common,
                expected_entry_ns=int(earliest_entry),
                source_session_day=int(record.session_day),
                source_segment_code=int(record.segment_code),
                source_contract_code=int(record.contract_code),
                regime=self.prior_regime,
            )
        return True

    def checkpoint(self) -> CausalSleeveCheckpoint:
        payload = {
            "kernel_version": CAUSAL_DECISION_KERNEL_VERSION,
            "sleeve_id": self.spec.sleeve_id,
            "specification_hash": self.specification_hash,
            "fill_policy_hash": self.fill_policy_hash,
            "order_checkpoint": self.order_guard.checkpoint().to_dict(),
            "ordinal": self.ordinal,
            "pending": None if self.pending is None else self.pending.to_dict(),
            "open_trade": (
                None if self.open_trade is None else self.open_trade.to_dict()
            ),
            "blocked_path_identity": (
                None
                if self.blocked_path_identity is None
                else list(self.blocked_path_identity)
            ),
            "signals": [row.to_dict() for row in self.signals],
            "normal_trajectories": [
                row.to_dict() for row in self.normal_trajectories
            ],
            "stressed_trajectories": [
                row.to_dict() for row in self.stressed_trajectories
            ],
            "normal_censored_trajectories": [
                row.to_dict() for row in self.normal_censored
            ],
            "stressed_censored_trajectories": [
                row.to_dict() for row in self.stressed_censored
            ],
            "eligible_days": sorted(self.eligible_days),
            "regime_day": self.regime_day,
            "regime_values": list(self.regime_values),
            "prior_regime": self.prior_regime,
            "last_availability_ns": self.last_availability_ns,
        }
        return CausalSleeveCheckpoint.create(payload)

    def finalize(self) -> CausalSleeveReplay:
        """Return an immutable replay without mutating resumable kernel state."""

        clone = CausalSleeveStreamingKernel.from_checkpoint(
            self.spec,
            self.binding,
            self.checkpoint(),
            fill_policy=self.fill_policy,
        )
        if clone.open_trade is not None:
            clone._censor_open(
                reason="FROZEN_EXIT_BAR_NOT_OBSERVED_BEFORE_INPUT_END",
                censor_time_ns=clone.open_trade.expected_exit_ns,
            )
        if clone.pending is not None:
            clone._censor_pending(
                reason="NEXT_ENTRY_BAR_NOT_OBSERVED_BEFORE_INPUT_END",
                censor_time_ns=clone.pending.expected_entry_ns,
            )
        normal_events = tuple(row.event for row in clone.normal_trajectories)
        stressed_events = tuple(row.event for row in clone.stressed_trajectories)
        signals = tuple(clone.signals)
        normal_censored = tuple(clone.normal_censored)
        stressed_censored = tuple(clone.stressed_censored)
        return CausalSleeveReplay(
            sleeve_id=self.spec.sleeve_id,
            signal_count=len(signals),
            completed_trade_count=len(normal_events),
            censored_signal_count=sum(
                row.outcome_status == CENSORED_FUTURE_COVERAGE for row in signals
            ),
            eligible_session_days=tuple(sorted(clone.eligible_days)),
            normal_events=normal_events,
            stressed_events=stressed_events,
            normal_trajectories=tuple(clone.normal_trajectories),
            stressed_trajectories=tuple(clone.stressed_trajectories),
            normal_censored_trajectories=normal_censored,
            stressed_censored_trajectories=stressed_censored,
            signals=signals,
            decision_hash=_stable_hash([row.to_dict() for row in signals]),
            normal_event_hash=_stable_hash([row.to_dict() for row in normal_events]),
            stressed_event_hash=_stable_hash(
                [row.to_dict() for row in stressed_events]
            ),
            normal_censored_trajectory_hash=_stable_hash(
                [row.to_dict() for row in normal_censored]
            ),
            stressed_censored_trajectory_hash=_stable_hash(
                [row.to_dict() for row in stressed_censored]
            ),
            fill_policy_hash=self.fill_policy_hash,
            specification_hash=self.specification_hash,
        )

    def _restore(self, checkpoint: CausalSleeveCheckpoint) -> None:
        payload = checkpoint.payload
        if _stable_hash(dict(payload)) != checkpoint.checkpoint_hash:
            raise ValueError("causal sleeve checkpoint mutated after validation")
        if payload.get("kernel_version") != CAUSAL_DECISION_KERNEL_VERSION:
            raise ValueError("causal checkpoint kernel-version drift")
        if payload.get("sleeve_id") != self.spec.sleeve_id:
            raise ValueError("causal checkpoint sleeve identity drift")
        if payload.get("specification_hash") != self.specification_hash:
            raise ValueError("causal checkpoint specification drift")
        if payload.get("fill_policy_hash") != self.fill_policy_hash:
            raise ValueError("causal checkpoint fill-policy drift")
        self.order_guard = CausalInputOrderGuard(
            CausalOrderCheckpoint.from_mapping(payload["order_checkpoint"])
        )
        self.ordinal = int(payload["ordinal"])
        self.pending = (
            None
            if payload.get("pending") is None
            else _PendingSignal.from_mapping(payload["pending"])
        )
        self.open_trade = (
            None
            if payload.get("open_trade") is None
            else _OpenTrade.from_mapping(payload["open_trade"])
        )
        blocked = payload.get("blocked_path_identity")
        self.blocked_path_identity = (
            None if blocked is None else tuple(int(value) for value in blocked)
        )
        self.signals = [
            CausalSignalEvidence.from_mapping(row) for row in payload["signals"]
        ]
        self.normal_trajectories = [
            CausalTradeTrajectory.from_mapping(row)
            for row in payload["normal_trajectories"]
        ]
        self.stressed_trajectories = [
            CausalTradeTrajectory.from_mapping(row)
            for row in payload["stressed_trajectories"]
        ]
        self.normal_censored = [
            CausalCensoredTrajectory.from_mapping(row)
            for row in payload["normal_censored_trajectories"]
        ]
        self.stressed_censored = [
            CausalCensoredTrajectory.from_mapping(row)
            for row in payload["stressed_censored_trajectories"]
        ]
        self.eligible_days = {int(day) for day in payload["eligible_days"]}
        self.regime_day = _optional_int(payload.get("regime_day"))
        self.regime_values = [float(value) for value in payload["regime_values"]]
        self.prior_regime = str(payload["prior_regime"])
        self.last_availability_ns = _optional_int(payload.get("last_availability_ns"))

    def _advance_regime_day(self, record: CausalBarRecord) -> None:
        if self.regime_day is None:
            self.regime_day = int(record.session_day)
        elif int(record.session_day) != self.regime_day:
            self.prior_regime = _classify_regime(self.regime_values)
            self.regime_day = int(record.session_day)
            self.regime_values = []
        value = _optional_finite(record.regime_value)
        if value is not None:
            self.regime_values.append(value)

    def _resolve_open_exit_boundary(self, record: CausalBarRecord) -> None:
        opened = self.open_trade
        if opened is None:
            return
        if record.timestamp_ns < opened.expected_exit_ns:
            return
        path_valid = _record_matches_pending_path(record, opened.pending)
        contiguous = (
            opened.last_bar_timestamp_ns is not None
            and record.timestamp_ns == opened.last_bar_timestamp_ns + MINUTE_NS
        )
        if (
            record.timestamp_ns == opened.expected_exit_ns
            and path_valid
            and contiguous
            and math.isfinite(record.bar_open)
        ):
            self._complete_open(record)
            return
        reason = (
            "FROZEN_EXIT_BAR_MISSING_NONCONTIGUOUS_OR_ROLLED"
            if record.timestamp_ns != opened.expected_exit_ns or not contiguous
            else "FROZEN_EXIT_BAR_SESSION_CONTRACT_SEGMENT_OR_ROLL_CHANGED"
        )
        self._censor_open(reason=reason, censor_time_ns=opened.expected_exit_ns)

    def _resolve_pending_entry_boundary(self, record: CausalBarRecord) -> None:
        pending = self.pending
        if pending is None:
            return
        if record.timestamp_ns < pending.expected_entry_ns:
            return
        if (
            record.timestamp_ns == pending.expected_entry_ns
            and _record_matches_pending_path(record, pending)
            and math.isfinite(record.bar_open)
        ):
            normal_ticks = self.normal_slippage_ticks_per_side
            stressed_ticks = self.stressed_slippage_ticks_per_side
            normal_entry = record.bar_open + (
                self.spec.side * normal_ticks * self.instrument.tick_size
            )
            stressed_entry = record.bar_open + (
                self.spec.side * stressed_ticks * self.instrument.tick_size
            )
            self.open_trade = _OpenTrade(
                pending=pending,
                fill_ns=int(record.timestamp_ns),
                raw_entry_open=float(record.bar_open),
                normal_entry=float(normal_entry),
                stressed_entry=float(stressed_entry),
                expected_exit_ns=int(
                    record.timestamp_ns + int(self.spec.holding_bars) * MINUTE_NS
                ),
                last_bar_timestamp_ns=None,
                normal_marks=[],
                stressed_marks=[],
            )
            self.pending = None
            return
        reason = (
            "NEXT_ENTRY_BAR_MISSING_NONCONTIGUOUS_OR_ROLLED"
            if record.timestamp_ns != pending.expected_entry_ns
            else "NEXT_ENTRY_BAR_SESSION_CONTRACT_SEGMENT_OR_ROLL_CHANGED"
        )
        self._censor_pending(reason=reason, censor_time_ns=pending.expected_entry_ns)

    def _mark_open_position(self, record: CausalBarRecord) -> None:
        opened = self.open_trade
        if opened is None or record.timestamp_ns >= opened.expected_exit_ns:
            return
        expected = (
            opened.fill_ns
            if opened.last_bar_timestamp_ns is None
            else opened.last_bar_timestamp_ns + MINUTE_NS
        )
        if (
            record.timestamp_ns != expected
            or not _record_matches_pending_path(record, opened.pending)
        ):
            self._censor_open(
                reason="HOLDING_PATH_GAP_SESSION_CONTRACT_OR_ROLL",
                censor_time_ns=expected,
            )
            return
        if not all(
            math.isfinite(float(value))
            for value in (record.bar_high, record.bar_low, record.bar_close)
        ):
            self._censor_open(
                reason="HOLDING_BAR_OHLC_MISSING",
                censor_time_ns=int(record.availability_ns),
            )
            return
        opened.normal_marks.append(
            _mark(
                self.spec,
                entry=opened.normal_entry,
                commission=self.commission,
                record=record,
            )
        )
        opened.stressed_marks.append(
            _mark(
                self.spec,
                entry=opened.stressed_entry,
                commission=self.commission,
                record=record,
            )
        )
        opened.last_bar_timestamp_ns = int(record.timestamp_ns)

    def _complete_open(self, record: CausalBarRecord) -> None:
        opened = self.open_trade
        if opened is None:
            raise RuntimeError("no causal position to complete")
        normal_exit = record.bar_open - (
            self.spec.side
            * self.normal_slippage_ticks_per_side
            * self.instrument.tick_size
        )
        stressed_exit = record.bar_open - (
            self.spec.side
            * self.stressed_slippage_ticks_per_side
            * self.instrument.tick_size
        )
        normal = _completed_trajectory(
            opened,
            self.spec,
            raw_exit_open=float(record.bar_open),
            exit_price=float(normal_exit),
            commission=self.commission,
            scenario="NORMAL",
            marks=tuple(opened.normal_marks),
        )
        stressed = _completed_trajectory(
            opened,
            self.spec,
            raw_exit_open=float(record.bar_open),
            exit_price=float(stressed_exit),
            commission=self.commission,
            scenario="STRESSED_1_5X",
            marks=tuple(opened.stressed_marks),
        )
        self.signals.append(
            _signal_evidence(
                opened.pending,
                fill_time_ns=opened.fill_ns,
                raw_entry_open=opened.raw_entry_open,
                normal_entry=opened.normal_entry,
                stressed_entry=opened.stressed_entry,
                expected_exit_ns=opened.expected_exit_ns,
                exit_fill_time_ns=int(record.timestamp_ns),
                raw_exit_open=float(record.bar_open),
                normal_exit=float(normal_exit),
                stressed_exit=float(stressed_exit),
                outcome_status=TARGET_OBSERVED,
                censor_reason=None,
                censor_time_ns=None,
            )
        )
        self.normal_trajectories.append(normal)
        self.stressed_trajectories.append(stressed)
        self.open_trade = None

    def _censor_pending(self, *, reason: str, censor_time_ns: int) -> None:
        pending = self.pending
        if pending is None:
            return
        self.signals.append(
            _signal_evidence(
                pending,
                fill_time_ns=None,
                raw_entry_open=None,
                normal_entry=None,
                stressed_entry=None,
                expected_exit_ns=None,
                exit_fill_time_ns=None,
                raw_exit_open=None,
                normal_exit=None,
                stressed_exit=None,
                outcome_status=CENSORED_FUTURE_COVERAGE,
                censor_reason=reason,
                censor_time_ns=int(censor_time_ns),
            )
        )
        self.pending = None

    def _censor_open(self, *, reason: str, censor_time_ns: int) -> None:
        opened = self.open_trade
        if opened is None:
            return
        self.signals.append(
            _signal_evidence(
                opened.pending,
                fill_time_ns=opened.fill_ns,
                raw_entry_open=opened.raw_entry_open,
                normal_entry=opened.normal_entry,
                stressed_entry=opened.stressed_entry,
                expected_exit_ns=opened.expected_exit_ns,
                exit_fill_time_ns=None,
                raw_exit_open=None,
                normal_exit=None,
                stressed_exit=None,
                outcome_status=CENSORED_FUTURE_COVERAGE,
                censor_reason=reason,
                censor_time_ns=int(censor_time_ns),
            )
        )
        self.normal_censored.append(
            _censored_trajectory(
                opened,
                self.spec,
                commission=self.commission,
                scenario="NORMAL",
                marks=tuple(opened.normal_marks),
                censor_time_ns=int(censor_time_ns),
                reason=reason,
            )
        )
        self.stressed_censored.append(
            _censored_trajectory(
                opened,
                self.spec,
                commission=self.commission,
                scenario="STRESSED_1_5X",
                marks=tuple(opened.stressed_marks),
                censor_time_ns=int(censor_time_ns),
                reason=reason,
            )
        )
        self.blocked_path_identity = (
            opened.pending.source_session_day,
            opened.pending.source_segment_code,
            opened.pending.source_contract_code,
        )
        self.open_trade = None


def replay_causal_sleeve_batch(
    spec: SleeveSpec,
    binding: FrozenSignalBinding,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str = "2023-01-01",
    end_exclusive: str = "2024-10-01",
    fill_policy: CausalFillPolicy | None = None,
) -> CausalSleeveReplay:
    """Historical batch replay implemented as a loop over the public step."""

    kernel = CausalSleeveStreamingKernel(
        spec, binding, fill_policy=fill_policy or CausalFillPolicy()
    )
    for record in iter_causal_bar_records(
        binding,
        matrix,
        start_inclusive=start_inclusive,
        end_exclusive=end_exclusive,
    ):
        kernel.step(record)
    return kernel.finalize()


def replay_causal_sleeve_streaming(
    spec: SleeveSpec,
    binding: FrozenSignalBinding,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str = "2023-01-01",
    end_exclusive: str = "2024-10-01",
    fill_policy: CausalFillPolicy | None = None,
) -> CausalSleeveReplay:
    """One-record-at-a-time replay through the checkpointable public kernel."""

    kernel = CausalSleeveStreamingKernel(
        spec, binding, fill_policy=fill_policy or CausalFillPolicy()
    )
    records = iter_causal_bar_records(
        binding,
        matrix,
        start_inclusive=start_inclusive,
        end_exclusive=end_exclusive,
    )
    for record in records:
        kernel.step(record)
    return kernel.finalize()


def iter_causal_bar_records(
    binding: FrozenSignalBinding,
    matrix: FeatureMatrix,
    *,
    start_inclusive: str = "2023-01-01",
    end_exclusive: str = "2024-10-01",
) -> Iterator[CausalBarRecord]:
    """Yield immutable records without exposing future rows to ``step``."""

    trigger = matrix.array(f"feature__{binding.trigger_feature}")
    context = (
        None
        if binding.context_feature is None
        else matrix.array(f"feature__{binding.context_feature}")
    )
    regime = matrix.array("feature__ctx_60m_volatility_expansion")
    timestamp = matrix.array("timestamp_ns")
    decision = matrix.array("decision_ns")
    availability = matrix.array("availability_ns")
    session = matrix.array("session_code")
    days = matrix.array("session_day")
    segments = matrix.array("segment_code")
    contracts = matrix.array("contract_code")
    opens = matrix.array("bar_open")
    highs = matrix.array("bar_high")
    lows = matrix.array("bar_low")
    closes = matrix.array("bar_close")
    in_scope = (
        (days >= _day(start_inclusive))
        & (days < _day(end_exclusive))
        & (session >= 0)
    )
    for raw_index in np.flatnonzero(in_scope):
        index = int(raw_index)
        yield CausalBarRecord(
            timestamp_ns=int(timestamp[index]),
            decision_ns=int(decision[index]),
            availability_ns=int(availability[index]),
            session_day=int(days[index]),
            session_code=int(session[index]),
            segment_code=int(segments[index]),
            contract_code=int(contracts[index]),
            bar_open=float(opens[index]),
            bar_high=float(highs[index]),
            bar_low=float(lows[index]),
            bar_close=float(closes[index]),
            trigger_value=float(trigger[index]),
            context_value=(None if context is None else float(context[index])),
            regime_value=float(regime[index]),
        )


def causal_runtime(
    replay: CausalSleeveReplay,
    spec: SleeveSpec,
    *,
    scenario: str = "NORMAL",
) -> ExactSleeveRuntime:
    if scenario not in {"NORMAL", "STRESSED_1_5X"}:
        raise ValueError("unsupported causal runtime scenario")
    source = replay.normal_events if scenario == "NORMAL" else replay.stressed_events
    routed = tuple(
        RoutedTrade(
            component_id=spec.sleeve_id,
            market=spec.execution_market,
            side=spec.side,
            event=TradePathEvent(
                **{
                    **event.to_dict(),
                    "mini_equivalent": mini_equivalent(
                        spec.execution_market, event.quantity
                    ),
                }
            ),
        )
        for event in source
    )
    net = np.asarray([row.event.net_pnl for row in routed], dtype=float)
    equity = np.cumsum(net) if len(net) else np.asarray([], dtype=float)
    peak = (
        np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:]
        if len(equity)
        else np.asarray([], dtype=float)
    )
    drawdown = float(np.max(peak - equity, initial=0.0)) if len(equity) else 0.0
    positive = net[net > 0.0]
    positive_sum = float(positive.sum())
    best_share = float(positive.max() / positive_sum) if positive_sum else 1.0
    return ExactSleeveRuntime(
        sleeve_id=spec.sleeve_id,
        signal_market=spec.market,
        execution_market=spec.execution_market,
        role=spec.role,
        source_campaign="hydra_causal_salvage_sprint_0027",
        specification_hash=replay.specification_hash,
        eligible_session_days=replay.eligible_session_days,
        events=routed,
        event_count=len(routed),
        net_pnl=float(net.sum()),
        cost_stress_1_5x_net=float(sum(row.net_pnl for row in replay.stressed_events)),
        maximum_drawdown=drawdown,
        best_positive_event_share=best_share,
        exit_implementation="EXACT_TIME_EXIT",
    )


def _mark(
    spec: SleeveSpec,
    *,
    entry: float,
    commission: float,
    record: CausalBarRecord,
) -> CausalTradeMark:
    point = instrument_spec(spec.execution_market).point_value
    adverse = record.bar_low if spec.side > 0 else record.bar_high
    favorable = record.bar_high if spec.side > 0 else record.bar_low
    return CausalTradeMark(
        availability_time_ns=int(record.availability_ns),
        worst_unrealized_pnl=float(
            (adverse - entry) * spec.side * point - commission
        ),
        best_unrealized_pnl=float(
            (favorable - entry) * spec.side * point - commission
        ),
        current_unrealized_pnl=float(
            (record.bar_close - entry) * spec.side * point - commission
        ),
    )


def _completed_trajectory(
    opened: _OpenTrade,
    spec: SleeveSpec,
    *,
    raw_exit_open: float,
    exit_price: float,
    commission: float,
    scenario: str,
    marks: tuple[CausalTradeMark, ...],
) -> CausalTradeTrajectory:
    entry = opened.normal_entry if scenario == "NORMAL" else opened.stressed_entry
    initial = _initial_unrealized(spec, raw_open=opened.raw_entry_open, entry=entry, commission=commission)
    point_value = instrument_spec(spec.execution_market).point_value
    gross = (raw_exit_open - opened.raw_entry_open) * spec.side * point_value
    net = (exit_price - entry) * spec.side * point_value - commission
    event = TradePathEvent(
        event_id=f"{opened.pending.common['signal_id']}:{scenario}",
        decision_ns=int(opened.fill_ns),
        exit_ns=int(opened.expected_exit_ns),
        session_day=int(opened.pending.source_session_day),
        net_pnl=float(net),
        gross_pnl=float(gross),
        worst_unrealized_pnl=float(
            min([initial, *(row.worst_unrealized_pnl for row in marks)])
        ),
        best_unrealized_pnl=float(
            max([initial, *(row.best_unrealized_pnl for row in marks)])
        ),
        quantity=1,
        mini_equivalent=float(mini_equivalent(spec.execution_market, 1)),
        regime=opened.pending.regime,
        session_compliant=True,
        contract_limit_compliant=True,
        same_bar_ambiguous=False,
    )
    return CausalTradeTrajectory(
        component_id=spec.sleeve_id,
        market=spec.execution_market,
        side=spec.side,
        event=event,
        marks=marks,
        initial_unrealized_pnl=float(initial),
    )


def _censored_trajectory(
    opened: _OpenTrade,
    spec: SleeveSpec,
    *,
    commission: float,
    scenario: str,
    marks: tuple[CausalTradeMark, ...],
    censor_time_ns: int,
    reason: str,
) -> CausalCensoredTrajectory:
    entry = opened.normal_entry if scenario == "NORMAL" else opened.stressed_entry
    initial = _initial_unrealized(
        spec,
        raw_open=opened.raw_entry_open,
        entry=entry,
        commission=commission,
    )
    last_net = (
        float(marks[-1].current_unrealized_pnl)
        if marks and marks[-1].current_unrealized_pnl is not None
        else float(initial)
    )
    entry_slippage_cost = (
        (entry - opened.raw_entry_open)
        * spec.side
        * instrument_spec(spec.execution_market).point_value
    )
    event = TradePathEvent(
        event_id=f"{opened.pending.common['signal_id']}:{scenario}:CENSORED",
        decision_ns=int(opened.fill_ns),
        exit_ns=max(int(opened.fill_ns), int(censor_time_ns)),
        session_day=int(opened.pending.source_session_day),
        net_pnl=last_net,
        gross_pnl=float(last_net + entry_slippage_cost + commission),
        worst_unrealized_pnl=float(
            min([initial, *(row.worst_unrealized_pnl for row in marks)])
        ),
        best_unrealized_pnl=float(
            max([initial, *(row.best_unrealized_pnl for row in marks)])
        ),
        quantity=1,
        mini_equivalent=float(mini_equivalent(spec.execution_market, 1)),
        regime=opened.pending.regime,
        session_compliant=True,
        contract_limit_compliant=True,
        same_bar_ambiguous=False,
    )
    return CausalCensoredTrajectory(
        component_id=spec.sleeve_id,
        market=spec.execution_market,
        side=spec.side,
        event=event,
        marks=marks,
        initial_unrealized_pnl=float(initial),
        terminal_status=CENSORED_FUTURE_COVERAGE,
        censor_time_ns=int(censor_time_ns),
        censor_reason=reason,
    )


def _signal_evidence(
    pending: _PendingSignal,
    *,
    fill_time_ns: int | None,
    raw_entry_open: float | None,
    normal_entry: float | None,
    stressed_entry: float | None,
    expected_exit_ns: int | None,
    exit_fill_time_ns: int | None,
    raw_exit_open: float | None,
    normal_exit: float | None,
    stressed_exit: float | None,
    outcome_status: str,
    censor_reason: str | None,
    censor_time_ns: int | None,
) -> CausalSignalEvidence:
    return CausalSignalEvidence(
        **pending.common,
        fill_time_ns=fill_time_ns,
        raw_entry_open=raw_entry_open,
        normal_entry_fill_price=normal_entry,
        stressed_entry_fill_price=stressed_entry,
        exit_decision_time_ns=fill_time_ns,
        exit_order_submit_time_ns=fill_time_ns,
        exit_earliest_executable_time_ns=expected_exit_ns,
        exit_fill_time_ns=exit_fill_time_ns,
        raw_exit_open=raw_exit_open,
        normal_exit_fill_price=normal_exit,
        stressed_exit_fill_price=stressed_exit,
        outcome_status=outcome_status,
        censor_reason=censor_reason,
        censor_time_ns=censor_time_ns,
    )


def _initial_unrealized(
    spec: SleeveSpec,
    *,
    raw_open: float,
    entry: float,
    commission: float,
) -> float:
    return float(
        (raw_open - entry)
        * spec.side
        * instrument_spec(spec.execution_market).point_value
        - commission
    )


def _next_executable_boundary(record: CausalBarRecord) -> int:
    raw = max(int(record.decision_ns), int(record.availability_ns))
    return int(((raw + MINUTE_NS - 1) // MINUTE_NS) * MINUTE_NS)


def _record_matches_pending_path(
    record: CausalBarRecord, pending: _PendingSignal
) -> bool:
    return bool(
        int(record.session_day) == pending.source_session_day
        and int(record.segment_code) == pending.source_segment_code
        and int(record.contract_code) == pending.source_contract_code
    )


def _path_identity(record: CausalBarRecord) -> tuple[int, int, int]:
    return (
        int(record.session_day),
        int(record.segment_code),
        int(record.contract_code),
    )


def _classify_regime(values: Sequence[float]) -> str:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    if not finite:
        return "UNKNOWN"
    median = float(np.median(np.asarray(finite, dtype=float)))
    return (
        "VOLATILITY_EXPANSION"
        if median >= 1.20
        else "VOLATILITY_CONTRACTION"
        if median <= 0.80
        else "VOLATILITY_NORMAL"
    )


def _compare(value: float, operator: str, threshold: float) -> bool:
    normalized = str(operator).upper()
    if normalized in {"GT", "GREATER_THAN"}:
        return value > threshold
    if normalized in {"GE", "GREATER_EQUAL"}:
        return value >= threshold
    if normalized in {"LT", "LESS_THAN"}:
        return value < threshold
    if normalized in {"LE", "LESS_EQUAL"}:
        return value <= threshold
    raise ValueError(f"unsupported causal comparison operator: {operator}")


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _day(value: str) -> int:
    return int(np.datetime64(value, "D").astype(np.int64))


__all__ = [
    "CAUSAL_DECISION_KERNEL_VERSION",
    "CAUSAL_FILL_POLICY_ID",
    "CENSORED_FUTURE_COVERAGE",
    "TARGET_OBSERVED",
    "CausalBarRecord",
    "CausalCensoredTrajectory",
    "CausalFillPolicy",
    "CausalInputOrderGuard",
    "CausalOrderCheckpoint",
    "CausalSignalEvidence",
    "CausalSleeveCheckpoint",
    "CausalSleeveReplay",
    "CausalSleeveStreamingKernel",
    "CausalTradeMark",
    "CausalTradeTrajectory",
    "FrozenSleeveDecisionKernel",
    "causal_runtime",
    "iter_causal_bar_records",
    "replay_causal_sleeve_batch",
    "replay_causal_sleeve_streaming",
]
