"""Causal shared active-risk-pool routing for immutable strategy sleeves.

The router never reserves risk for an inactive sleeve.  Risk is charged only
for positions present in :class:`~hydra.account_policy.router.AccountDecisionState`
and for the entry currently being considered.  Campaign source ledgers do not
contain availability-safe stop prices, so this module deliberately uses a
frozen *declared nominal risk charge* per mini-equivalent.  It never substitutes
future adverse excursion or realized trade PnL for entry-time risk.

The return type is duck-compatible with ``RoutingDecision`` and carries the
additional conservation and suppression fields required by the active-pool
audit.  Existing account replay can therefore consume ``allow``, ``quantity``,
``mini_equivalent``, ``reason`` and ``to_dict`` without signal mutation.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping

from hydra.account_policy.router import AccountDecisionState, EntryIntent


ACTIVE_RISK_POOL_POLICY_VERSION = "hydra_active_risk_pool_governor_v1"
ACTIVE_RISK_POOL_SCHEMA = "hydra_active_risk_pool_policy_v1"
# The cached sleeve ledgers are expressed in whole micro-contract quantities.
# Fractional multipliers would therefore collapse to the same executable size
# for the overwhelmingly common one-micro entry.  Freeze an auditable integer
# frontier instead; 1x is the identity/standalone profile and 4x remains no
# more aggressive than the 3 * 1.30 effective maximum evaluated in campaign
# 0025.
STATIC_RISK_FRONTIER = (1.0, 2.0, 3.0, 4.0)


class ActiveRiskPoolError(ValueError):
    """An active-pool declaration cannot be executed safely."""


class ConcurrencyScaling(StrEnum):
    """How a new entry behaves when only part of the pool remains."""

    PROPORTIONAL = "PROPORTIONAL"
    PRIORITY = "PRIORITY"


class SameInstrumentConflictRule(StrEnum):
    """Entry-time conflict rules implementable without mutating an open sleeve.

    ``PRIORITY`` means the position already admitted by the frozen event and
    component ordering keeps the instrument.  ``ALLOW_SAME_DIRECTION`` permits
    gross same-direction coexistence but still rejects an opposing signal.
    Neither rule silently closes or rewrites an existing sleeve position.
    """

    PRIORITY = "PRIORITY"
    ALLOW_SAME_DIRECTION = "ALLOW_SAME_DIRECTION"


class TargetProtectionMode(StrEnum):
    NONE = "NONE"
    SCALE_75 = "SCALE_75"
    SCALE_50 = "SCALE_50"
    LOCK_NEW_ENTRIES = "LOCK_NEW_ENTRIES"

    @property
    def multiplier(self) -> float:
        return {
            TargetProtectionMode.NONE: 1.0,
            TargetProtectionMode.SCALE_75: 0.75,
            TargetProtectionMode.SCALE_50: 0.50,
            TargetProtectionMode.LOCK_NEW_ENTRIES: 0.0,
        }[self]


CONCURRENCY_SCALING_MODES = tuple(row.value for row in ConcurrencyScaling)
SAME_INSTRUMENT_CONFLICT_RULES = tuple(
    row.value for row in SameInstrumentConflictRule
)
TARGET_PROTECTION_MODES = tuple(row.value for row in TargetProtectionMode)


class ActivePoolDecisionStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    SIZE_REDUCED = "SIZE_REDUCED"
    REJECTED = "REJECTED"
    CONFLICT_REJECTED = "CONFLICT_REJECTED"
    CONTRACT_LIMIT_REJECTED = "CONTRACT_LIMIT_REJECTED"
    MLL_RISK_REJECTED = "MLL_RISK_REJECTED"


@dataclass(frozen=True, slots=True)
class ActiveRiskPoolPolicy:
    """Immutable bounded governor declaration.

    ``nominal_risk_charge_per_mini`` is an entry-time accounting proxy and must
    be frozen before outcomes.  It is not claimed to be actual stop risk.
    """

    policy_id: str
    component_priority: tuple[str, ...]
    nominal_risk_charge_per_mini: tuple[tuple[str, float], ...]
    maximum_concurrent_sleeves: int
    aggregate_open_risk_ceiling: float
    maximum_mll_buffer_fraction: float
    protected_mll_buffer: float
    maximum_mini_equivalent: float
    concurrency_scaling: ConcurrencyScaling
    same_instrument_conflict_rule: SameInstrumentConflictRule
    daily_loss_guard: float
    daily_consistency_profit_guard: float
    target_protection_distance: float
    target_protection_mode: TargetProtectionMode
    static_risk_tier: float
    preserve_sole_sleeve_nominal_risk: bool = True
    policy_version: str = ACTIVE_RISK_POOL_POLICY_VERSION
    outbound_order_capability: bool = False

    def __post_init__(self) -> None:
        if not self.policy_id.strip():
            raise ActiveRiskPoolError("active-pool policy ID is required")
        if not self.component_priority or len(set(self.component_priority)) != len(
            self.component_priority
        ):
            raise ActiveRiskPoolError("component priority must be non-empty and unique")
        charges = self.nominal_risk_charge_map
        if len(self.nominal_risk_charge_per_mini) != len(charges):
            raise ActiveRiskPoolError("nominal risk charge components must be unique")
        if set(charges) != set(self.component_priority):
            raise ActiveRiskPoolError("nominal risk charges must cover membership exactly")
        if tuple(charges) != self.component_priority:
            raise ActiveRiskPoolError(
                "nominal risk charges must follow frozen component priority"
            )
        if any(not math.isfinite(value) or value <= 0.0 for value in charges.values()):
            raise ActiveRiskPoolError("nominal risk charges must be finite and positive")
        if not 1 <= self.maximum_concurrent_sleeves <= len(self.component_priority):
            raise ActiveRiskPoolError("maximum concurrent sleeves is outside membership")
        if (
            not math.isfinite(self.aggregate_open_risk_ceiling)
            or self.aggregate_open_risk_ceiling <= 0.0
        ):
            raise ActiveRiskPoolError("aggregate nominal risk ceiling must be positive")
        if not 0.0 < self.maximum_mll_buffer_fraction <= 1.0:
            raise ActiveRiskPoolError("MLL-buffer risk fraction must be in (0,1]")
        if not math.isfinite(self.protected_mll_buffer) or self.protected_mll_buffer < 0.0:
            raise ActiveRiskPoolError("protected MLL buffer must be finite and nonnegative")
        if not 0.0 < self.maximum_mini_equivalent <= 15.0:
            raise ActiveRiskPoolError("shared mini-equivalent limit must be in (0,15]")
        if not isinstance(self.concurrency_scaling, ConcurrencyScaling):
            raise ActiveRiskPoolError("concurrency scaling mode is not frozen")
        if not isinstance(
            self.same_instrument_conflict_rule, SameInstrumentConflictRule
        ):
            raise ActiveRiskPoolError("same-instrument conflict rule is not frozen")
        if not isinstance(self.target_protection_mode, TargetProtectionMode):
            raise ActiveRiskPoolError("target-protection mode is not frozen")
        if not 0.0 < self.daily_loss_guard <= 4_500.0:
            raise ActiveRiskPoolError("daily loss guard must be in (0,4500]")
        if not 0.0 < self.daily_consistency_profit_guard <= 9_000.0:
            raise ActiveRiskPoolError("daily consistency guard must be in (0,9000]")
        if not 0.0 <= self.target_protection_distance <= 9_000.0:
            raise ActiveRiskPoolError("target-protection distance must be in [0,9000]")
        if float(self.static_risk_tier) not in STATIC_RISK_FRONTIER:
            raise ActiveRiskPoolError("risk tier escaped the frozen discrete frontier")
        if not self.preserve_sole_sleeve_nominal_risk:
            raise ActiveRiskPoolError("inactive-sleeve capital reservation is forbidden")
        if self.policy_version != ACTIVE_RISK_POOL_POLICY_VERSION:
            raise ActiveRiskPoolError("active-pool policy version drift")
        if self.outbound_order_capability:
            raise ActiveRiskPoolError("research governor cannot submit orders")

    @property
    def controller_id(self) -> str:
        """Duck-typed identifier used by the shared account simulator."""

        return self.policy_id

    @property
    def component_ids(self) -> tuple[str, ...]:
        """Frozen sleeve membership exposed to the shared replay engine."""

        return self.component_priority

    @property
    def maximum_simultaneous_positions(self) -> int:
        return self.maximum_concurrent_sleeves

    @property
    def conflict_policy(self) -> str:
        return self.same_instrument_conflict_rule.value

    @property
    def nominal_risk_charge_map(self) -> dict[str, float]:
        return {
            str(component_id): float(charge)
            for component_id, charge in self.nominal_risk_charge_per_mini
        }

    @property
    def structural_fingerprint(self) -> str:
        payload = self.to_dict(include_fingerprint=False)
        # Policy IDs are provenance labels, not executable semantics.  Leaving
        # them in this hash would make structural deduplication vacuous.
        payload.pop("policy_id", None)
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
        row = asdict(self)
        row["schema"] = ACTIVE_RISK_POOL_SCHEMA
        row["component_priority"] = list(self.component_priority)
        row["nominal_risk_charge_per_mini"] = {
            component_id: charge
            for component_id, charge in self.nominal_risk_charge_per_mini
        }
        row["concurrency_scaling"] = self.concurrency_scaling.value
        row["same_instrument_conflict_rule"] = (
            self.same_instrument_conflict_rule.value
        )
        row["target_protection_mode"] = self.target_protection_mode.value
        row["risk_measure"] = "DECLARED_NOMINAL_RISK_UTILISATION"
        row["actual_stop_risk_available"] = False
        row["future_outcome_fields_used"] = False
        if include_fingerprint:
            row["structural_fingerprint"] = self.structural_fingerprint
        return row

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ActiveRiskPoolPolicy":
        return policy_from_mapping(value)


@dataclass(frozen=True, slots=True)
class ActiveRiskUtilisation:
    active_sleeve_count: int
    open_declared_nominal_risk: float
    maximum_admissible_declared_nominal_risk: float
    utilisation: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_measure": "DECLARED_NOMINAL_RISK_UTILISATION",
            "actual_stop_risk_available": False,
            "active_sleeve_count": self.active_sleeve_count,
            "open_declared_nominal_risk": self.open_declared_nominal_risk,
            "maximum_admissible_declared_nominal_risk": (
                self.maximum_admissible_declared_nominal_risk
            ),
            "utilisation": self.utilisation,
        }


@dataclass(frozen=True, slots=True)
class ActivePoolRoutingDecision:
    """RoutingDecision-compatible result with explicit suppression provenance."""

    allow: bool
    quantity: int
    mini_equivalent: float
    reason: str
    policy_id: str
    decision_status: ActivePoolDecisionStatus
    requested_quantity: int
    requested_mini_equivalent: float
    requested_declared_nominal_risk: float
    admitted_declared_nominal_risk: float
    risk_before: ActiveRiskUtilisation
    risk_after: ActiveRiskUtilisation
    scaling_factor: float
    binding_constraint: str | None = None

    @property
    def emitted(self) -> bool:
        return True

    @property
    def accepted(self) -> bool:
        return self.allow

    @property
    def rejected(self) -> bool:
        return not self.allow

    @property
    def size_reduced(self) -> bool:
        return self.decision_status is ActivePoolDecisionStatus.SIZE_REDUCED

    @property
    def admission_fraction(self) -> float:
        if self.requested_quantity <= 0:
            return 0.0
        return float(self.quantity / self.requested_quantity)

    def to_dict(self) -> dict[str, Any]:
        return {
            "allow": self.allow,
            "quantity": self.quantity,
            "mini_equivalent": self.mini_equivalent,
            "reason": self.reason,
            "policy_id": self.policy_id,
            "decision_status": self.decision_status.value,
            "emitted": True,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "size_reduced": self.size_reduced,
            "conflict_rejected": (
                self.decision_status is ActivePoolDecisionStatus.CONFLICT_REJECTED
            ),
            "contract_limit_rejected": (
                self.decision_status
                is ActivePoolDecisionStatus.CONTRACT_LIMIT_REJECTED
            ),
            "mll_risk_rejected": (
                self.decision_status is ActivePoolDecisionStatus.MLL_RISK_REJECTED
            ),
            "requested_quantity": self.requested_quantity,
            "requested_mini_equivalent": self.requested_mini_equivalent,
            "requested_declared_nominal_risk": (
                self.requested_declared_nominal_risk
            ),
            "admitted_declared_nominal_risk": self.admitted_declared_nominal_risk,
            "scaling_factor": self.scaling_factor,
            "admission_fraction": self.admission_fraction,
            "binding_constraint": self.binding_constraint,
            "risk_before": self.risk_before.to_dict(),
            "risk_after": self.risk_after.to_dict(),
            "foregone_realized_pnl_available_at_decision": False,
        }


def active_risk_utilisation(
    policy: ActiveRiskPoolPolicy,
    state: AccountDecisionState,
    *,
    additional_component_id: str | None = None,
    additional_mini_equivalent: float = 0.0,
) -> ActiveRiskUtilisation:
    """Return causal declared-risk utilisation for the current account state."""

    charges = policy.nominal_risk_charge_map
    open_risk = 0.0
    active = set()
    for exposure in state.open_exposures:
        try:
            charge = charges[exposure.component_id]
        except KeyError as exc:
            raise ActiveRiskPoolError(
                "open exposure is absent from frozen active-pool membership"
            ) from exc
        if (
            not math.isfinite(exposure.mini_equivalent)
            or exposure.mini_equivalent < 0.0
        ):
            raise ActiveRiskPoolError("open mini-equivalent exposure cannot be negative")
        active.add(exposure.component_id)
        open_risk += float(exposure.mini_equivalent) * charge
    if additional_component_id is not None and additional_mini_equivalent > 0.0:
        try:
            charge = charges[additional_component_id]
        except KeyError as exc:
            raise ActiveRiskPoolError(
                "additional exposure is absent from frozen membership"
            ) from exc
        active.add(additional_component_id)
        open_risk += float(additional_mini_equivalent) * charge
    maximum = _maximum_admissible_declared_risk(policy, state)
    utilisation = open_risk / maximum if maximum > 0.0 else None
    return ActiveRiskUtilisation(
        active_sleeve_count=len(active),
        open_declared_nominal_risk=float(open_risk),
        maximum_admissible_declared_nominal_risk=float(maximum),
        utilisation=float(utilisation) if utilisation is not None else None,
    )


def route_active_risk_pool_entry(
    intent: EntryIntent,
    state: AccountDecisionState,
    *,
    policy: ActiveRiskPoolPolicy,
) -> ActivePoolRoutingDecision:
    """Route one emitted entry using only information available at decision time."""

    before = active_risk_utilisation(policy, state)
    if intent.component_id not in policy.component_priority:
        return _rejected(
            policy,
            before,
            ActivePoolDecisionStatus.REJECTED,
            "COMPONENT_NOT_IN_FROZEN_MEMBERSHIP",
        )
    if (
        intent.base_quantity < 1
        or not math.isfinite(intent.base_mini_equivalent)
        or intent.base_mini_equivalent <= 0.0
    ):
        return _rejected(
            policy,
            before,
            ActivePoolDecisionStatus.REJECTED,
            "INVALID_NOMINAL_ENTRY_SIZE",
        )
    mini_per_quantity = float(intent.base_mini_equivalent) / float(
        intent.base_quantity
    )
    requested_quantity = max(
        1,
        int(
            math.floor(
                float(intent.base_quantity) * float(policy.static_risk_tier)
                + 1e-12
            )
        ),
    )
    requested_mini = requested_quantity * mini_per_quantity
    charge = policy.nominal_risk_charge_map[intent.component_id]
    requested_risk = requested_mini * charge
    requested_audit = {
        "requested_quantity": requested_quantity,
        "requested_mini_equivalent": requested_mini,
        "requested_declared_nominal_risk": requested_risk,
    }
    if state.daily_realized_pnl <= -policy.daily_loss_guard:
        return _rejected(
            policy,
            before,
            ActivePoolDecisionStatus.REJECTED,
            "DAILY_LOSS_GUARD",
            **requested_audit,
        )
    if state.daily_realized_pnl >= policy.daily_consistency_profit_guard:
        return _rejected(
            policy,
            before,
            ActivePoolDecisionStatus.REJECTED,
            "DAILY_CONSISTENCY_GUARD",
            **requested_audit,
        )
    if state.mll_buffer <= policy.protected_mll_buffer:
        return _rejected(
            policy,
            before,
            ActivePoolDecisionStatus.MLL_RISK_REJECTED,
            "PROTECTED_MLL_BUFFER_REACHED",
            **requested_audit,
        )

    live_exposures = tuple(
        row for row in state.open_exposures if row.exit_ns > intent.decision_ns
    )
    active_ids = {row.component_id for row in live_exposures}
    if (
        intent.component_id not in active_ids
        and len(active_ids) >= policy.maximum_concurrent_sleeves
    ):
        return _rejected(
            policy,
            before,
            ActivePoolDecisionStatus.REJECTED,
            "MAXIMUM_CONCURRENT_SLEEVES",
            **requested_audit,
        )

    same_market = tuple(row for row in live_exposures if row.market == intent.market)
    if same_market:
        conflict = (
            policy.same_instrument_conflict_rule
            is SameInstrumentConflictRule.PRIORITY
            or any(row.side != intent.side for row in same_market)
        )
        if conflict:
            return _rejected(
                policy,
                before,
                ActivePoolDecisionStatus.CONFLICT_REJECTED,
                "SAME_INSTRUMENT_CONFLICT",
                **requested_audit,
            )

    target_multiplier = 1.0
    if (
        policy.target_protection_distance > 0.0
        and state.remaining_target <= policy.target_protection_distance
    ):
        target_multiplier = policy.target_protection_mode.multiplier
        if target_multiplier <= 0.0:
            return _rejected(
                policy,
                before,
                ActivePoolDecisionStatus.REJECTED,
                "TARGET_PROTECTION_LOCK",
                **requested_audit,
            )

    desired_quantity = max(
        1,
        int(math.floor(requested_quantity * target_multiplier + 1e-12)),
    )

    current_mini = sum(float(row.mini_equivalent) for row in live_exposures)
    available_mini = max(0.0, policy.maximum_mini_equivalent - current_mini)
    available_risk = max(
        0.0,
        before.maximum_admissible_declared_nominal_risk
        - before.open_declared_nominal_risk,
    )
    max_by_contract = int(math.floor(available_mini / mini_per_quantity + 1e-12))
    risk_per_quantity = mini_per_quantity * charge
    max_by_risk = int(math.floor(available_risk / risk_per_quantity + 1e-12))

    if policy.concurrency_scaling is ConcurrencyScaling.PRIORITY:
        if max_by_contract < desired_quantity:
            return _rejected(
                policy,
                before,
                ActivePoolDecisionStatus.CONTRACT_LIMIT_REJECTED,
                "SHARED_CONTRACT_LIMIT",
                **requested_audit,
            )
        if max_by_risk < desired_quantity:
            return _rejected(
                policy,
                before,
                ActivePoolDecisionStatus.MLL_RISK_REJECTED,
                "AGGREGATE_NOMINAL_RISK_LIMIT",
                **requested_audit,
            )
        admitted_quantity = desired_quantity
    else:
        admitted_quantity = min(desired_quantity, max_by_contract, max_by_risk)
        if admitted_quantity < 1:
            status, reason = (
                (
                    ActivePoolDecisionStatus.CONTRACT_LIMIT_REJECTED,
                    "SHARED_CONTRACT_LIMIT",
                )
                if max_by_contract <= max_by_risk
                else (
                    ActivePoolDecisionStatus.MLL_RISK_REJECTED,
                    "AGGREGATE_NOMINAL_RISK_LIMIT",
                )
            )
            return _rejected(
                policy,
                before,
                status,
                reason,
                **requested_audit,
            )

    admitted_mini = admitted_quantity * mini_per_quantity
    admitted_risk = admitted_mini * charge
    after = active_risk_utilisation(
        policy,
        state,
        additional_component_id=intent.component_id,
        additional_mini_equivalent=admitted_mini,
    )
    reduced = admitted_quantity < requested_quantity
    binding_constraint: str | None = None
    reason = "ACTIVE_POOL_NOMINAL_RISK_PRESERVED"
    if reduced:
        if admitted_quantity < desired_quantity:
            binding_constraint = (
                "SHARED_CONTRACT_LIMIT"
                if max_by_contract <= max_by_risk
                else "AGGREGATE_NOMINAL_RISK_LIMIT"
            )
        elif target_multiplier < 1.0:
            binding_constraint = "TARGET_PROTECTION"
        reason = (
            "TARGET_PROTECTION_SIZE_REDUCTION"
            if target_multiplier < 1.0 and admitted_quantity == desired_quantity
            else "ACTIVE_POOL_PROPORTIONAL_SIZE_REDUCTION"
        )
    return ActivePoolRoutingDecision(
        allow=True,
        quantity=admitted_quantity,
        mini_equivalent=float(admitted_mini),
        reason=reason,
        policy_id=policy.policy_id,
        decision_status=(
            ActivePoolDecisionStatus.SIZE_REDUCED
            if reduced
            else ActivePoolDecisionStatus.ACCEPTED
        ),
        requested_quantity=requested_quantity,
        requested_mini_equivalent=float(requested_mini),
        requested_declared_nominal_risk=float(requested_risk),
        admitted_declared_nominal_risk=float(admitted_risk),
        risk_before=before,
        risk_after=after,
        scaling_factor=float(admitted_quantity / intent.base_quantity),
        binding_constraint=binding_constraint,
    )


def _maximum_admissible_declared_risk(
    policy: ActiveRiskPoolPolicy,
    state: AccountDecisionState,
) -> float:
    available_buffer = max(0.0, state.mll_buffer - policy.protected_mll_buffer)
    buffer_bound = available_buffer * policy.maximum_mll_buffer_fraction
    return float(min(policy.aggregate_open_risk_ceiling, buffer_bound))


def _rejected(
    policy: ActiveRiskPoolPolicy,
    before: ActiveRiskUtilisation,
    status: ActivePoolDecisionStatus,
    reason: str,
    *,
    requested_quantity: int = 0,
    requested_mini_equivalent: float = 0.0,
    requested_declared_nominal_risk: float = 0.0,
) -> ActivePoolRoutingDecision:
    return ActivePoolRoutingDecision(
        allow=False,
        quantity=0,
        mini_equivalent=0.0,
        reason=reason,
        policy_id=policy.policy_id,
        decision_status=status,
        requested_quantity=requested_quantity,
        requested_mini_equivalent=float(requested_mini_equivalent),
        requested_declared_nominal_risk=float(requested_declared_nominal_risk),
        admitted_declared_nominal_risk=0.0,
        risk_before=before,
        risk_after=before,
        scaling_factor=0.0,
        binding_constraint=reason,
    )


def policy_from_mapping(value: Mapping[str, Any]) -> ActiveRiskPoolPolicy:
    """Load a manifest policy without accepting undeclared enum values."""

    charges = value["nominal_risk_charge_per_mini"]
    if isinstance(charges, Mapping):
        priority = tuple(str(row) for row in value["component_priority"])
        charge_rows = tuple(
            (component_id, float(charges[component_id]))
            for component_id in priority
        )
    else:
        charge_rows = tuple((str(row[0]), float(row[1])) for row in charges)
    return ActiveRiskPoolPolicy(
        policy_id=str(value["policy_id"]),
        component_priority=tuple(str(row) for row in value["component_priority"]),
        nominal_risk_charge_per_mini=charge_rows,
        maximum_concurrent_sleeves=int(value["maximum_concurrent_sleeves"]),
        aggregate_open_risk_ceiling=float(value["aggregate_open_risk_ceiling"]),
        maximum_mll_buffer_fraction=float(value["maximum_mll_buffer_fraction"]),
        protected_mll_buffer=float(value["protected_mll_buffer"]),
        maximum_mini_equivalent=float(value["maximum_mini_equivalent"]),
        concurrency_scaling=ConcurrencyScaling(str(value["concurrency_scaling"])),
        same_instrument_conflict_rule=SameInstrumentConflictRule(
            str(value["same_instrument_conflict_rule"])
        ),
        daily_loss_guard=float(value["daily_loss_guard"]),
        daily_consistency_profit_guard=float(
            value["daily_consistency_profit_guard"]
        ),
        target_protection_distance=float(value["target_protection_distance"]),
        target_protection_mode=TargetProtectionMode(
            str(value["target_protection_mode"])
        ),
        static_risk_tier=float(value["static_risk_tier"]),
        preserve_sole_sleeve_nominal_risk=bool(
            value.get("preserve_sole_sleeve_nominal_risk", True)
        ),
        policy_version=str(
            value.get("policy_version", ACTIVE_RISK_POOL_POLICY_VERSION)
        ),
        outbound_order_capability=bool(value.get("outbound_order_capability", False)),
    )


# Concise runtime-facing aliases.  The longer names remain descriptive in
# evidence schemas while these names keep integration call sites readable.
ActiveRiskRoutingDecision = ActivePoolRoutingDecision
route_active_risk_entry = route_active_risk_pool_entry


__all__ = [
    "ACTIVE_RISK_POOL_POLICY_VERSION",
    "ACTIVE_RISK_POOL_SCHEMA",
    "STATIC_RISK_FRONTIER",
    "CONCURRENCY_SCALING_MODES",
    "SAME_INSTRUMENT_CONFLICT_RULES",
    "TARGET_PROTECTION_MODES",
    "ActivePoolDecisionStatus",
    "ActivePoolRoutingDecision",
    "ActiveRiskRoutingDecision",
    "ActiveRiskPoolError",
    "ActiveRiskPoolPolicy",
    "ActiveRiskUtilisation",
    "ConcurrencyScaling",
    "SameInstrumentConflictRule",
    "TargetProtectionMode",
    "active_risk_utilisation",
    "policy_from_mapping",
    "route_active_risk_pool_entry",
    "route_active_risk_entry",
]
