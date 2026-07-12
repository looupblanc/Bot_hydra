from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping


GENESIS_HASH = "0" * 64
PROOF_WINDOW_EVENT = "PROOF_WINDOW_STATUS"
MULTIPLICITY_EVENT = "MULTIPLICITY_COUNTER"
SUPPORTED_EVENT_TYPES = frozenset({PROOF_WINDOW_EVENT, MULTIPLICITY_EVENT})


class ProofRegistryError(RuntimeError):
    pass


def canonical_hash(entry_without_hash: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        entry_without_hash, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def load_and_verify(path: str | Path) -> dict[str, Any]:
    registry = json.loads(Path(path).read_text(encoding="utf-8"))
    if registry.get("schema") != "hydra_proof_registry_v1":
        raise ProofRegistryError("unsupported proof registry schema")
    entries = list(registry.get("entries") or [])
    if int(registry.get("entry_count", -1)) != len(entries):
        raise ProofRegistryError("proof registry entry count mismatch")
    previous = GENESIS_HASH
    multiplicity = 0
    event_ids: set[str] = set()
    for position, raw in enumerate(entries):
        entry = dict(raw)
        stored = str(entry.pop("entry_hash", ""))
        if entry.get("previous_hash") != previous:
            raise ProofRegistryError(
                f"proof chain previous hash mismatch at entry {position}"
            )
        calculated = canonical_hash(entry)
        if stored != calculated:
            raise ProofRegistryError(
                f"proof chain entry hash mismatch at entry {position}"
            )
        event_id = str(entry.get("event_id") or "")
        if not event_id or event_id in event_ids:
            raise ProofRegistryError("proof event IDs must be non-empty and unique")
        event_ids.add(event_id)
        event_type = str(entry.get("event_type") or "")
        if event_type not in SUPPORTED_EVENT_TYPES:
            raise ProofRegistryError(f"unsupported proof event type: {event_type}")
        if event_type == PROOF_WINDOW_EVENT:
            window = entry.get("window")
            if not isinstance(window, Mapping) or not str(window.get("id") or ""):
                raise ProofRegistryError("proof-window event requires a window ID")
        else:
            counter = entry.get("multiplicity")
            if not isinstance(counter, Mapping):
                raise ProofRegistryError("multiplicity event requires counter payload")
            prior = int(counter.get("previous_N_trials", -1))
            delta = int(counter.get("delta_trials", -1))
            cumulative = int(counter.get("cumulative_N_trials", -1))
            if prior != multiplicity:
                raise ProofRegistryError("multiplicity previous counter mismatch")
            if delta <= 0 or cumulative != prior + delta:
                raise ProofRegistryError("multiplicity counter must increase exactly")
            multiplicity = cumulative
        previous = stored
    if str(registry.get("chain_head")) != previous:
        raise ProofRegistryError("proof registry chain head mismatch")
    return registry


def burned_window_ids(registry: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(entry["window"]["id"])
                for entry in registry.get("entries", ())
                if entry.get("event_type") == PROOF_WINDOW_EVENT
                and entry.get("status") == "BURNED"
            }
        )
    )


def multiplicity_trial_count(registry: Mapping[str, Any]) -> int:
    count = 0
    for entry in registry.get("entries", ()):
        if entry.get("event_type") == MULTIPLICITY_EVENT:
            count = int(entry["multiplicity"]["cumulative_N_trials"])
    return count


def verify_registry_prefix(
    registry: Mapping[str, Any], *, entry_count: int, chain_head: str
) -> None:
    entries = list(registry.get("entries") or [])
    if entry_count < 0 or entry_count > len(entries):
        raise ProofRegistryError("manifest proof prefix length is impossible")
    expected = GENESIS_HASH if entry_count == 0 else str(
        entries[entry_count - 1].get("entry_hash") or ""
    )
    if expected != str(chain_head):
        raise ProofRegistryError("manifest proof prefix chain head mismatch")


def append_entry(path: str | Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    destination = Path(path)
    registry = load_and_verify(destination)
    if "entry_hash" in payload or "previous_hash" in payload:
        raise ProofRegistryError("caller cannot provide chain hashes")
    event_type = str(payload.get("event_type") or "")
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise ProofRegistryError(f"unsupported proof event type: {event_type}")
    if event_type == PROOF_WINDOW_EVENT:
        window = payload.get("window")
        if not isinstance(window, Mapping) or not str(window.get("id") or ""):
            raise ProofRegistryError("proof-window event requires a window ID")
        window_id = str(window["id"])
        if window_id in burned_window_ids(registry):
            raise ProofRegistryError(
                f"proof window is irreversibly BURNED: {window_id}"
            )
    else:
        if "window" in payload:
            raise ProofRegistryError("multiplicity events cannot consume a window")
        counter = payload.get("multiplicity")
        if not isinstance(counter, Mapping):
            raise ProofRegistryError("multiplicity event requires counter payload")
        prior = multiplicity_trial_count(registry)
        if int(counter.get("previous_N_trials", -1)) != prior:
            raise ProofRegistryError("multiplicity previous counter mismatch")
        delta = int(counter.get("delta_trials", -1))
        cumulative = int(counter.get("cumulative_N_trials", -1))
        if delta <= 0 or cumulative != prior + delta:
            raise ProofRegistryError("multiplicity counter must increase exactly")
    if any(
        str(entry.get("event_id")) == str(payload.get("event_id"))
        for entry in registry["entries"]
    ):
        raise ProofRegistryError("duplicate proof event ID")
    entry = dict(payload)
    entry["previous_hash"] = str(registry["chain_head"])
    entry_hash = canonical_hash(entry)
    entry["entry_hash"] = entry_hash
    updated = dict(registry)
    updated["entries"] = [*registry["entries"], entry]
    updated["entry_count"] = len(updated["entries"])
    updated["chain_head"] = entry_hash
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(
        json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, destination)
    load_and_verify(destination)
    return entry


__all__ = [
    "GENESIS_HASH",
    "ProofRegistryError",
    "append_entry",
    "burned_window_ids",
    "canonical_hash",
    "load_and_verify",
    "multiplicity_trial_count",
    "verify_registry_prefix",
]
