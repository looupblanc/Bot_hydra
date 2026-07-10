from __future__ import annotations

from collections import Counter

from hydra.atoms.schema import AssembledStrategySpec


def validate_strategy_atoms(atom_ids: tuple[str, ...], *, max_atoms: int = 3) -> None:
    if not atom_ids:
        raise ValueError("A sparse strategy requires at least one validated atom.")
    if len(atom_ids) > max_atoms:
        raise ValueError(f"Sparse strategy cannot use more than {max_atoms} atoms.")
    if len(set(atom_ids)) != len(atom_ids):
        raise ValueError("Sparse strategy cannot include duplicate atoms.")


def structural_key(spec: AssembledStrategySpec) -> tuple[str, tuple[str, ...], str]:
    return (spec.primary_family, tuple(sorted(spec.atom_ids)), spec.structure)


def reject_duplicate_structures(specs: list[AssembledStrategySpec]) -> list[AssembledStrategySpec]:
    seen: set[tuple[str, tuple[str, ...], str]] = set()
    out: list[AssembledStrategySpec] = []
    for spec in specs:
        key = structural_key(spec)
        if key in seen:
            continue
        seen.add(key)
        out.append(spec)
    return out


def family_share_ok(specs: list[AssembledStrategySpec], *, max_family_share: float) -> bool:
    if not specs:
        return True
    counts = Counter(spec.primary_family for spec in specs)
    return max(counts.values()) / len(specs) <= max_family_share

