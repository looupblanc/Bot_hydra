"""Causal opportunity-episode consolidation for microstructure research.

The state machine in this module deliberately knows nothing about economic
outcomes.  It consumes only observations that were available at decision time,
consolidates repeated observations for one frozen mechanism/direction into a
single price/time episode, and emits at most one transparent decision per
episode.  Outcome labelling belongs to a later, physically separate pass.

Batch replay is intentionally only a loop over :meth:`OpportunityEpisodeFSM.step`.
The checkpoint contains the duplicate ledger and the active episode, so a
controlled restart cannot emit the same decision twice.
"""

from __future__ import annotations

import math
from collections import OrderedDict
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

from hydra.economic_evolution.schema import stable_hash


FSM_SCHEMA = "hydra_microstructure_opportunity_episode_fsm_v1"
CHECKPOINT_SCHEMA = "hydra_microstructure_opportunity_episode_checkpoint_v1"
DECISION_ACTIONS = ("ENTER", "ABSTAIN")

_OBSERVATION_FIELDS = frozenset(
    {
        "event_fingerprint",
        "market",
        "contract",
        "session_id",
        "event_time_ns",
        "available_at_ns",
        "price",
        "mechanism",
        "direction",
        "activation_score",
        "meta_score",
        "feature_fingerprint",
    }
)
_FORBIDDEN_DECISION_FIELDS = frozenset(
    {
        "label",
        "outcome",
        "future_label",
        "future_outcome",
        "future_markout",
        "future_markout_ticks",
        "future_return",
        "favorable_first",
        "adverse_first",
        "favorable_before_adverse",
        "target_reached",
        "mll_breached",
        "exit_price",
        "realized_pnl",
        "net_pnl",
        "days_to_target",
    }
)


class OpportunityEpisodeError(RuntimeError):
    """Base class for an invalid opportunity-episode operation."""


class OpportunityCausalityError(OpportunityEpisodeError):
    """An unavailable or outcome-bearing observation reached the FSM."""


class OpportunityOrderError(OpportunityEpisodeError):
    """A non-duplicate observation regressed in causal decision time."""


class OpportunityDuplicateConflict(OpportunityEpisodeError):
    """One event fingerprint was reused with different causal contents."""


class OpportunityCheckpointError(OpportunityEpisodeError):
    """A checkpoint is corrupt or belongs to another frozen policy."""


@dataclass(frozen=True, slots=True)
class OpportunityEpisodeSpec:
    """Frozen mechanism/direction and consolidation policy.

    ``activation_threshold`` and ``reset_threshold`` implement hysteresis.
    ``meta_label_threshold`` is applied to a causal inference score already
    available on the observation; the FSM never receives the later label used
    to train that inference model.
    """

    policy_id: str
    mechanism: str
    direction: int
    activation_threshold: float
    reset_threshold: float
    meta_label_threshold: float
    consolidation_window_ns: int
    price_zone_ticks: float
    tick_size: float
    minimum_confirmations: int = 1
    recent_event_limit: int = 4_096

    def __post_init__(self) -> None:
        if not self.policy_id.strip() or not self.mechanism.strip():
            raise ValueError("policy_id and mechanism are required")
        if self.direction not in {-1, 1}:
            raise ValueError("direction must be -1 or +1")
        if not all(
            math.isfinite(float(value))
            for value in (
                self.activation_threshold,
                self.reset_threshold,
                self.meta_label_threshold,
                self.price_zone_ticks,
                self.tick_size,
            )
        ):
            raise ValueError("opportunity thresholds must be finite")
        if self.reset_threshold >= self.activation_threshold:
            raise ValueError("reset_threshold must be below activation_threshold")
        if not 0.0 <= self.meta_label_threshold <= 1.0:
            raise ValueError("meta_label_threshold must be a probability")
        if self.consolidation_window_ns <= 0:
            raise ValueError("consolidation_window_ns must be positive")
        if self.price_zone_ticks < 0.0 or self.tick_size <= 0.0:
            raise ValueError("price zone and tick size are invalid")
        if self.minimum_confirmations <= 0:
            raise ValueError("minimum_confirmations must be positive")
        if self.recent_event_limit <= 0:
            raise ValueError("recent_event_limit must be positive")

    @property
    def fingerprint(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True, slots=True)
class OpportunityObservation:
    """One causal mechanism observation presented to the state machine."""

    event_fingerprint: str
    market: str
    contract: str
    session_id: str
    event_time_ns: int
    available_at_ns: int
    price: float
    mechanism: str
    direction: int
    activation_score: float
    meta_score: float
    feature_fingerprint: str

    @classmethod
    def from_record(
        cls, value: OpportunityObservation | Mapping[str, Any]
    ) -> OpportunityObservation:
        if isinstance(value, cls):
            value.validate()
            return value
        if not isinstance(value, Mapping):
            raise TypeError("opportunity observation must be a mapping")
        keys = {str(key) for key in value}
        lowered = {key.lower() for key in keys}
        forbidden = sorted(
            key
            for key in lowered
            if key in _FORBIDDEN_DECISION_FIELDS
            or key.startswith("future_")
            or key.endswith("_label")
            or key.endswith("_outcome")
        )
        if forbidden:
            raise OpportunityCausalityError(
                f"future/outcome fields cannot enter step: {forbidden}"
            )
        unknown = sorted(keys - _OBSERVATION_FIELDS)
        missing = sorted(_OBSERVATION_FIELDS - keys)
        if unknown or missing:
            raise ValueError(
                f"opportunity observation field drift; missing={missing}, "
                f"unknown={unknown}"
            )
        observation = cls(
            event_fingerprint=str(value["event_fingerprint"]),
            market=str(value["market"]),
            contract=str(value["contract"]),
            session_id=str(value["session_id"]),
            event_time_ns=int(value["event_time_ns"]),
            available_at_ns=int(value["available_at_ns"]),
            price=float(value["price"]),
            mechanism=str(value["mechanism"]),
            direction=int(value["direction"]),
            activation_score=float(value["activation_score"]),
            meta_score=float(value["meta_score"]),
            feature_fingerprint=str(value["feature_fingerprint"]),
        )
        observation.validate()
        return observation

    def validate(self) -> None:
        required = (
            self.event_fingerprint,
            self.market,
            self.contract,
            self.session_id,
            self.mechanism,
            self.feature_fingerprint,
        )
        if any(not value.strip() for value in required):
            raise ValueError("opportunity observation identity is incomplete")
        if self.direction not in {-1, 1}:
            raise ValueError("observation direction must be -1 or +1")
        if self.event_time_ns < 0 or self.available_at_ns < self.event_time_ns:
            raise OpportunityCausalityError(
                "observation availability precedes its event time"
            )
        if not math.isfinite(self.price) or self.price <= 0.0:
            raise ValueError("observation price must be finite and positive")
        if not math.isfinite(self.activation_score):
            raise ValueError("activation_score must be finite")
        if not math.isfinite(self.meta_score) or not 0.0 <= self.meta_score <= 1.0:
            raise ValueError("meta_score must be a finite probability")

    @property
    def record_hash(self) -> str:
        return stable_hash(asdict(self))

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class OpportunityDecision:
    episode_id: str
    policy_id: str
    action: str
    reason: str
    mechanism: str
    direction: int
    decision_time_ns: int
    event_fingerprint: str
    feature_fingerprint: str
    activation_score: float
    meta_score: float
    confirmation_count: int
    decision_hash: str

    def to_record(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> OpportunityDecision:
        decision = cls(**dict(value))
        payload = asdict(decision)
        claimed = payload.pop("decision_hash")
        if decision.action not in DECISION_ACTIONS or stable_hash(payload) != claimed:
            raise OpportunityCheckpointError("opportunity decision hash drift")
        return decision


@dataclass(frozen=True, slots=True)
class OpportunityEpisode:
    episode_id: str
    policy_id: str
    mechanism: str
    direction: int
    market: str
    contract: str
    session_id: str
    started_at_ns: int
    ended_at_ns: int
    anchor_price: float
    last_price: float
    observation_count: int
    confirmation_count: int
    event_fingerprints: tuple[str, ...]
    decision: OpportunityDecision | None
    terminal_reason: str
    episode_hash: str

    def to_record(self) -> dict[str, Any]:
        value = asdict(self)
        return value

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> OpportunityEpisode:
        payload = dict(value)
        raw_decision = payload.get("decision")
        payload["decision"] = (
            None
            if raw_decision is None
            else OpportunityDecision.from_record(raw_decision)
        )
        payload["event_fingerprints"] = tuple(payload["event_fingerprints"])
        episode = cls(**payload)
        material = episode.to_record()
        claimed = material.pop("episode_hash")
        if stable_hash(material) != claimed:
            raise OpportunityCheckpointError("opportunity episode hash drift")
        return episode


@dataclass(frozen=True, slots=True)
class OpportunityStepResult:
    observation_fingerprint: str
    transition: str
    duplicate: bool
    consolidated: bool
    reset_reason: str | None
    decision: OpportunityDecision | None
    # ``None`` is an explicit high-throughput mode: the transition is fully
    # applied, but the caller deferred the O(state-size) audit hash.  The hash
    # remains available from ``OpportunityEpisodeFSM.state_hash`` and is always
    # materialised by checkpoints.  Default step behaviour still returns it.
    state_hash: str | None

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class _ActiveEpisode:
    episode_id: str
    market: str
    contract: str
    session_id: str
    started_at_ns: int
    last_decision_time_ns: int
    anchor_price: float
    last_price: float
    observation_count: int
    confirmation_count: int
    event_fingerprints: list[str]
    decision: OpportunityDecision | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "market": self.market,
            "contract": self.contract,
            "session_id": self.session_id,
            "started_at_ns": self.started_at_ns,
            "last_decision_time_ns": self.last_decision_time_ns,
            "anchor_price": self.anchor_price,
            "last_price": self.last_price,
            "observation_count": self.observation_count,
            "confirmation_count": self.confirmation_count,
            "event_fingerprints": list(self.event_fingerprints),
            "decision": None if self.decision is None else self.decision.to_record(),
        }

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> _ActiveEpisode:
        return cls(
            episode_id=str(value["episode_id"]),
            market=str(value["market"]),
            contract=str(value["contract"]),
            session_id=str(value["session_id"]),
            started_at_ns=int(value["started_at_ns"]),
            last_decision_time_ns=int(value["last_decision_time_ns"]),
            anchor_price=float(value["anchor_price"]),
            last_price=float(value["last_price"]),
            observation_count=int(value["observation_count"]),
            confirmation_count=int(value["confirmation_count"]),
            event_fingerprints=[str(item) for item in value["event_fingerprints"]],
            decision=(
                None
                if value.get("decision") is None
                else OpportunityDecision.from_record(value["decision"])
            ),
        )


class OpportunityEpisodeFSM:
    """Deterministic causal FSM for one frozen mechanism and direction."""

    def __init__(
        self,
        spec: OpportunityEpisodeSpec,
        *,
        checkpoint: Mapping[str, Any] | None = None,
    ) -> None:
        self.spec = spec
        self._active: _ActiveEpisode | None = None
        self._episodes: list[OpportunityEpisode] = []
        self._recent: OrderedDict[str, str] = OrderedDict()
        self._last_decision_time_ns: int | None = None
        if checkpoint is not None:
            self._load_checkpoint(checkpoint, expected_spec=spec)

    @property
    def episodes(self) -> tuple[OpportunityEpisode, ...]:
        return tuple(self._episodes)

    @property
    def decisions(self) -> tuple[OpportunityDecision, ...]:
        completed = [
            episode.decision
            for episode in self._episodes
            if episode.decision is not None
        ]
        if self._active is not None and self._active.decision is not None:
            completed.append(self._active.decision)
        return tuple(completed)

    @property
    def state_hash(self) -> str:
        return stable_hash(self._state_material())

    def step(
        self,
        observation: OpportunityObservation | Mapping[str, Any],
        *,
        decision_time_ns: int | None = None,
        materialize_state_hash: bool = True,
    ) -> OpportunityStepResult:
        """Consume one causal observation and possibly emit one decision.

        No outcome argument exists.  Mappings are strict and fail closed if a
        future/outcome field is present.  All validation occurs before state
        mutation.
        """

        item = OpportunityObservation.from_record(observation)
        if item.mechanism != self.spec.mechanism or item.direction != self.spec.direction:
            raise ValueError(
                "observation mechanism/direction does not match frozen FSM spec"
            )
        decision_time = (
            item.available_at_ns
            if decision_time_ns is None
            else int(decision_time_ns)
        )
        if decision_time < item.available_at_ns:
            raise OpportunityCausalityError(
                "observation was used before available_at"
            )

        previous_hash = self._recent.get(item.event_fingerprint)
        if previous_hash is not None:
            if previous_hash != item.record_hash:
                raise OpportunityDuplicateConflict(
                    "event fingerprint was reused with different causal contents"
                )
            return OpportunityStepResult(
                observation_fingerprint=item.event_fingerprint,
                transition="DUPLICATE",
                duplicate=True,
                consolidated=False,
                reset_reason=None,
                decision=None,
                state_hash=(self.state_hash if materialize_state_hash else None),
            )
        if (
            self._last_decision_time_ns is not None
            and decision_time < self._last_decision_time_ns
        ):
            raise OpportunityOrderError(
                "non-duplicate observation regressed in decision time"
            )

        boundary = self._boundary_reason(item, decision_time)
        reset_reason: str | None = None
        if boundary is not None:
            self._close_active(ended_at_ns=decision_time, reason=boundary)
            reset_reason = boundary

        decision: OpportunityDecision | None = None
        consolidated = False
        transition = "IGNORED_BELOW_ACTIVATION"
        if self._active is None:
            if item.activation_score >= self.spec.activation_threshold:
                self._active = self._start_episode(item, decision_time)
                decision = self._maybe_decide(item, decision_time)
                transition = (
                    "BOUNDARY_AND_DECISION_EMITTED"
                    if boundary is not None and decision is not None
                    else "BOUNDARY_AND_EPISODE_STARTED"
                    if boundary is not None
                    else "DECISION_EMITTED"
                    if decision is not None
                    else "EPISODE_STARTED"
                )
        else:
            consolidated = True
            self._append_observation(item, decision_time)
            if item.activation_score <= self.spec.reset_threshold:
                self._close_active(
                    ended_at_ns=decision_time,
                    reason="RESET_THRESHOLD",
                )
                reset_reason = "RESET_THRESHOLD"
                transition = "EPISODE_RESET"
            else:
                if item.activation_score >= self.spec.activation_threshold:
                    assert self._active is not None
                    self._active.confirmation_count += 1
                    decision = self._maybe_decide(item, decision_time)
                transition = (
                    "DECISION_EMITTED"
                    if decision is not None
                    else "EPISODE_CONSOLIDATED"
                )

        self._last_decision_time_ns = decision_time
        self._remember(item)
        return OpportunityStepResult(
            observation_fingerprint=item.event_fingerprint,
            transition=transition,
            duplicate=False,
            consolidated=consolidated,
            reset_reason=reset_reason,
            decision=decision,
            state_hash=(self.state_hash if materialize_state_hash else None),
        )

    def process_batch(
        self,
        observations: Iterable[OpportunityObservation | Mapping[str, Any]],
        *,
        materialize_state_hash: bool = True,
    ) -> tuple[OpportunityStepResult, ...]:
        """Replay a batch strictly through the same one-observation step."""

        return tuple(
            self.step(
                observation,
                materialize_state_hash=materialize_state_hash,
            )
            for observation in observations
        )

    @classmethod
    def replay_batch(
        cls,
        spec: OpportunityEpisodeSpec,
        observations: Iterable[OpportunityObservation | Mapping[str, Any]],
        *,
        materialize_state_hash: bool = True,
    ) -> tuple[OpportunityEpisodeFSM, tuple[OpportunityStepResult, ...]]:
        engine = cls(spec)
        return engine, engine.process_batch(
            observations,
            materialize_state_hash=materialize_state_hash,
        )

    def finalize(
        self,
        *,
        final_time_ns: int | None = None,
        reason: str = "END_OF_INPUT",
    ) -> tuple[OpportunityEpisode, ...]:
        """Close the current episode once and return the complete immutable set."""

        if not str(reason).strip():
            raise ValueError("finalize reason is required")
        if self._active is None:
            return self.episodes
        terminal_time = (
            self._active.last_decision_time_ns
            if final_time_ns is None
            else int(final_time_ns)
        )
        if terminal_time < self._active.last_decision_time_ns:
            raise OpportunityOrderError("finalize time precedes the active episode")
        self._close_active(ended_at_ns=terminal_time, reason=str(reason))
        return self.episodes

    def checkpoint(self) -> dict[str, Any]:
        payload = {
            "schema": CHECKPOINT_SCHEMA,
            "fsm_schema": FSM_SCHEMA,
            "spec": asdict(self.spec),
            "spec_fingerprint": self.spec.fingerprint,
            "active": None if self._active is None else self._active.to_record(),
            "episodes": [episode.to_record() for episode in self._episodes],
            "recent": list(self._recent.items()),
            "last_decision_time_ns": self._last_decision_time_ns,
            "state_hash": self.state_hash,
        }
        return {**payload, "checkpoint_hash": stable_hash(payload)}

    @classmethod
    def restore(cls, checkpoint: Mapping[str, Any]) -> OpportunityEpisodeFSM:
        raw_spec = checkpoint.get("spec")
        if not isinstance(raw_spec, Mapping):
            raise OpportunityCheckpointError("checkpoint spec is absent")
        spec = OpportunityEpisodeSpec(**dict(raw_spec))
        return cls(spec, checkpoint=checkpoint)

    def _load_checkpoint(
        self,
        checkpoint: Mapping[str, Any],
        *,
        expected_spec: OpportunityEpisodeSpec,
    ) -> None:
        payload = dict(checkpoint)
        claimed = str(payload.pop("checkpoint_hash", ""))
        if not claimed or stable_hash(payload) != claimed:
            raise OpportunityCheckpointError("checkpoint hash drift")
        if (
            payload.get("schema") != CHECKPOINT_SCHEMA
            or payload.get("fsm_schema") != FSM_SCHEMA
            or payload.get("spec_fingerprint") != expected_spec.fingerprint
            or payload.get("spec") != asdict(expected_spec)
        ):
            raise OpportunityCheckpointError("checkpoint policy identity drift")
        raw_active = payload.get("active")
        self._active = (
            None
            if raw_active is None
            else _ActiveEpisode.from_record(raw_active)
        )
        self._episodes = [
            OpportunityEpisode.from_record(value)
            for value in payload.get("episodes", ())
        ]
        self._recent = OrderedDict(
            (str(key), str(value)) for key, value in payload.get("recent", ())
        )
        self._last_decision_time_ns = (
            None
            if payload.get("last_decision_time_ns") is None
            else int(payload["last_decision_time_ns"])
        )
        if self.state_hash != payload.get("state_hash"):
            raise OpportunityCheckpointError("restored FSM state hash drift")

    def _boundary_reason(
        self, item: OpportunityObservation, decision_time_ns: int
    ) -> str | None:
        active = self._active
        if active is None:
            return None
        if item.market != active.market:
            return "MARKET_BOUNDARY"
        if item.contract != active.contract:
            return "CONTRACT_BOUNDARY"
        if item.session_id != active.session_id:
            return "SESSION_BOUNDARY"
        if decision_time_ns - active.started_at_ns > self.spec.consolidation_window_ns:
            return "TEMPORAL_BOUNDARY"
        zone = self.spec.price_zone_ticks * self.spec.tick_size
        if abs(item.price - active.anchor_price) > zone + 1e-12:
            return "PRICE_ZONE_BOUNDARY"
        return None

    def _start_episode(
        self, item: OpportunityObservation, decision_time_ns: int
    ) -> _ActiveEpisode:
        episode_id = stable_hash(
            {
                "policy_id": self.spec.policy_id,
                "spec_fingerprint": self.spec.fingerprint,
                "event_fingerprint": item.event_fingerprint,
                "market": item.market,
                "contract": item.contract,
                "session_id": item.session_id,
                "decision_time_ns": decision_time_ns,
                "anchor_price": item.price,
            }
        )
        return _ActiveEpisode(
            episode_id=episode_id,
            market=item.market,
            contract=item.contract,
            session_id=item.session_id,
            started_at_ns=decision_time_ns,
            last_decision_time_ns=decision_time_ns,
            anchor_price=item.price,
            last_price=item.price,
            observation_count=1,
            confirmation_count=1,
            event_fingerprints=[item.event_fingerprint],
        )

    def _append_observation(
        self, item: OpportunityObservation, decision_time_ns: int
    ) -> None:
        assert self._active is not None
        self._active.last_decision_time_ns = decision_time_ns
        self._active.last_price = item.price
        self._active.observation_count += 1
        self._active.event_fingerprints.append(item.event_fingerprint)

    def _maybe_decide(
        self, item: OpportunityObservation, decision_time_ns: int
    ) -> OpportunityDecision | None:
        active = self._active
        assert active is not None
        if (
            active.decision is not None
            or active.confirmation_count < self.spec.minimum_confirmations
        ):
            return None
        action = (
            "ENTER"
            if item.meta_score >= self.spec.meta_label_threshold
            else "ABSTAIN"
        )
        reason = (
            "META_LABEL_ACCEPTED"
            if action == "ENTER"
            else "META_LABEL_ABSTAINED"
        )
        payload = {
            "episode_id": active.episode_id,
            "policy_id": self.spec.policy_id,
            "action": action,
            "reason": reason,
            "mechanism": self.spec.mechanism,
            "direction": self.spec.direction,
            "decision_time_ns": decision_time_ns,
            "event_fingerprint": item.event_fingerprint,
            "feature_fingerprint": item.feature_fingerprint,
            "activation_score": item.activation_score,
            "meta_score": item.meta_score,
            "confirmation_count": active.confirmation_count,
        }
        active.decision = OpportunityDecision(
            **payload,
            decision_hash=stable_hash(payload),
        )
        return active.decision

    def _close_active(self, *, ended_at_ns: int, reason: str) -> None:
        active = self._active
        if active is None:
            return
        payload: dict[str, Any] = {
            "episode_id": active.episode_id,
            "policy_id": self.spec.policy_id,
            "mechanism": self.spec.mechanism,
            "direction": self.spec.direction,
            "market": active.market,
            "contract": active.contract,
            "session_id": active.session_id,
            "started_at_ns": active.started_at_ns,
            "ended_at_ns": ended_at_ns,
            "anchor_price": active.anchor_price,
            "last_price": active.last_price,
            "observation_count": active.observation_count,
            "confirmation_count": active.confirmation_count,
            "event_fingerprints": tuple(active.event_fingerprints),
            "decision": active.decision,
            "terminal_reason": reason,
        }
        serialized = {
            **payload,
            "decision": (
                None
                if active.decision is None
                else active.decision.to_record()
            ),
        }
        self._episodes.append(
            OpportunityEpisode(**payload, episode_hash=stable_hash(serialized))
        )
        self._active = None

    def _remember(self, item: OpportunityObservation) -> None:
        self._recent[item.event_fingerprint] = item.record_hash
        self._recent.move_to_end(item.event_fingerprint)
        while len(self._recent) > self.spec.recent_event_limit:
            self._recent.popitem(last=False)

    def _state_material(self) -> dict[str, Any]:
        return {
            "schema": FSM_SCHEMA,
            "spec_fingerprint": self.spec.fingerprint,
            "active": None if self._active is None else self._active.to_record(),
            "episode_hashes": [episode.episode_hash for episode in self._episodes],
            "recent": list(self._recent.items()),
            "last_decision_time_ns": self._last_decision_time_ns,
        }


__all__ = [
    "CHECKPOINT_SCHEMA",
    "DECISION_ACTIONS",
    "FSM_SCHEMA",
    "OpportunityCheckpointError",
    "OpportunityCausalityError",
    "OpportunityDecision",
    "OpportunityDuplicateConflict",
    "OpportunityEpisode",
    "OpportunityEpisodeError",
    "OpportunityEpisodeFSM",
    "OpportunityEpisodeSpec",
    "OpportunityObservation",
    "OpportunityOrderError",
    "OpportunityStepResult",
]
