from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Any

from hydra.atoms.schema import AssembledStrategySpec, AtomTestResult, EdgeAtomHypothesis
from hydra.strategy.assembly_constraints import reject_duplicate_structures, validate_strategy_atoms


ASSEMBLY_POLICY_VERSION = "sparse_strategy_assembly_v1"


@dataclass(frozen=True)
class StrategyAssemblyDecision:
    strategy: AssembledStrategySpec | None
    accepted: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if self.strategy is not None:
            out["strategy"] = self.strategy.to_dict()
        return out


def assemble_sparse_strategies(
    atoms: list[EdgeAtomHypothesis],
    results: dict[str, AtomTestResult],
    *,
    max_strategies: int,
    max_atoms_per_strategy: int = 3,
) -> tuple[list[AssembledStrategySpec], list[StrategyAssemblyDecision]]:
    validated = [
        atom
        for atom in atoms
        if atom.atom_id in results
        and results[atom.atom_id].status == "ATOM_VALIDATED"
        and bool(results[atom.atom_id].provenance.get("passed"))
    ]
    decisions: list[StrategyAssemblyDecision] = []
    specs: list[AssembledStrategySpec] = []
    for atom in validated:
        spec = _spec_for((atom,), "single_validated_entry_atom")
        validate_strategy_atoms(spec.atom_ids, max_atoms=max_atoms_per_strategy)
        specs.append(spec)
        decisions.append(StrategyAssemblyDecision(spec, True, "single_validated_atom_accepted"))
    by_family: dict[str, list[EdgeAtomHypothesis]] = {}
    for atom in validated:
        by_family.setdefault(atom.family, []).append(atom)
    families = sorted(by_family)
    for i, family_a in enumerate(families):
        for family_b in families[i + 1 :]:
            if len(specs) >= max_strategies:
                break
            atom_a = by_family[family_a][0]
            atom_b = by_family[family_b][0]
            spec = _spec_for((atom_a, atom_b), "entry_atom_plus_independent_regime_or_risk_atom")
            validate_strategy_atoms(spec.atom_ids, max_atoms=max_atoms_per_strategy)
            specs.append(spec)
            decisions.append(StrategyAssemblyDecision(spec, True, "two_validated_atoms_distinct_families_accepted"))
    specs = reject_duplicate_structures(specs)[:max_strategies]
    if not validated:
        decisions.append(StrategyAssemblyDecision(None, False, "no_fully_validated_atoms_available_for_assembly"))
    return specs, decisions


def _spec_for(atoms: tuple[EdgeAtomHypothesis, ...], structure: str) -> AssembledStrategySpec:
    atom_ids = tuple(atom.atom_id for atom in atoms)
    digest = hashlib.sha256("|".join(atom_ids + (structure,)).encode("utf-8")).hexdigest()[:12]
    markets = tuple(sorted({market for atom in atoms for market in atom.target_markets}))
    return AssembledStrategySpec(
        strategy_id=f"strategy_{digest}",
        atom_ids=atom_ids,
        primary_family=atoms[0].family,
        structure=structure,
        markets=markets,
    )

