from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any


ATOM_STATUS = {
    "ATOM_PROPOSED",
    "ATOM_PREREGISTERED",
    "ATOM_VALID",
    "ATOM_REPLICATED_TEMPORALLY",
    "ATOM_REPLICATED_CROSS_MARKET",
    "ATOM_REPLICATED_CONTRACTUALLY",
    "ATOM_ADVERSARIAL_PASS",
    "ATOM_VALIDATED",
    "ATOM_INSUFFICIENT_EVIDENCE",
    "ATOM_FALSIFIED",
}


@dataclass(frozen=True)
class EdgeAtomHypothesis:
    atom_id: str
    family: str
    feature_key: str
    economic_mechanism: str
    participants: str
    information_set: str
    target_variable: str
    expected_direction: int
    horizon_bars: int
    target_markets: tuple[str, ...]
    favorable_regimes: str
    failure_regimes: str
    transaction_cost_hurdle: float
    roll_sensitivity: str
    minimum_effect: float
    primary_null: str
    mandatory_nulls: tuple[str, ...]
    replication_requirement: str
    falsification_rule: str
    max_parameter_degrees: int
    timestamp_utc: str
    code_commit: str
    authoring_mode: str = "PREREGISTERED_BEFORE_TEST"
    version: int = 1
    parameters: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def preregistration_hash(self) -> str:
        raw = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AtomTestResult:
    atom_id: str
    family: str
    status: str
    valid_observations: int
    state_frequency: float
    raw_effect: float
    cost_hurdle: float
    effect_after_cost_hurdle: float
    confidence_low: float
    confidence_high: float
    direction_ok: bool
    folds_positive: int
    fold_count: int
    markets_positive: int
    market_count: int
    contracts_positive: int
    contract_count: int
    top_event_concentration: float
    evidence_strength: float
    fdr_adjusted_evidence: float
    simplest_competing_explanation: str
    failure_reason: str | None
    provenance: dict[str, Any]
    fold_results: dict[str, Any]
    market_results: dict[str, Any]
    contract_results: dict[str, Any]
    adversarial: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AssembledStrategySpec:
    strategy_id: str
    atom_ids: tuple[str, ...]
    primary_family: str
    structure: str
    markets: tuple[str, ...]
    max_atoms: int = 3
    version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
