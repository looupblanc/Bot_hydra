from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from hydra.atoms.schema import AtomTestResult, EdgeAtomHypothesis


REPLICATION_POLICY_VERSION = "atom_replication_policy_v1"


@dataclass(frozen=True)
class AtomReplicationResult:
    atom_id: str
    policy_version: str
    temporal_pass: bool
    contract_pass: bool
    cross_market_required: bool
    cross_market_pass: bool
    fold_count: int
    folds_positive: int
    contract_count: int
    contracts_positive: int
    market_count: int
    markets_positive: int
    decision_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def replicate_atom(atom: EdgeAtomHypothesis, result: AtomTestResult) -> AtomReplicationResult:
    temporal_required = min(3, max(result.fold_count, 1))
    contract_required = min(2, max(result.contract_count, 1))
    cross_market_required = len(atom.target_markets) > 1 or atom.family in {
        "cross_market_risk_transfer",
        "contract_roll_invariant_relative_state",
    }
    temporal_pass = result.fold_count >= temporal_required and result.folds_positive >= temporal_required
    contract_pass = result.contract_count >= contract_required and result.contracts_positive >= contract_required
    if cross_market_required:
        cross_market_pass = result.market_count >= 2 and result.markets_positive >= 2
    else:
        cross_market_pass = True
    if not temporal_pass:
        reason = "temporal_replication_failed"
    elif not contract_pass:
        reason = "contract_replication_failed"
    elif not cross_market_pass:
        reason = "cross_market_replication_failed"
    else:
        reason = "replication_policy_passed"
    return AtomReplicationResult(
        atom_id=atom.atom_id,
        policy_version=REPLICATION_POLICY_VERSION,
        temporal_pass=bool(temporal_pass),
        contract_pass=bool(contract_pass),
        cross_market_required=bool(cross_market_required),
        cross_market_pass=bool(cross_market_pass),
        fold_count=int(result.fold_count),
        folds_positive=int(result.folds_positive),
        contract_count=int(result.contract_count),
        contracts_positive=int(result.contracts_positive),
        market_count=int(result.market_count),
        markets_positive=int(result.markets_positive),
        decision_reason=reason,
    )

