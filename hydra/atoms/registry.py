from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hydra.atoms.schema import EdgeAtomHypothesis


def write_preregistration(atoms: list[EdgeAtomHypothesis], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "edge_atom_preregistration_v1",
        "atom_count": len(atoms),
        "atoms": [atom.to_dict() | {"preregistration_hash": atom.preregistration_hash} for atom in atoms],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def load_preregistration(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return list(payload.get("atoms", []))


def write_tombstone(tombstone: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(tombstone, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def tombstone_blocks_id(tombstones: list[dict[str, Any]], identifier: str) -> bool:
    for tombstone in tombstones:
        blocked = set(tombstone.get("blocked_ids", [])) | set(tombstone.get("blocked_formulations", []))
        if identifier in blocked:
            return True
    return False
