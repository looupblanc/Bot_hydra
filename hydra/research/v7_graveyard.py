from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "hydra_v7_class_graveyard_v2"
LEGACY_PROTOTYPE_COUNT = 115_388
PHASE2_BASKET_COUNT = 55
ARB_INTRA_PRODUCT_CLASS = "ARB_INTRA_PRODUIT"
ARB_INTRA_PRODUCT_DEATH_CAUSE = (
    "SIM_EXPLOIT_ADJACENT_STALE_QUOTE_LATENCY"
)
_CLASS_ALIASES = {
    "mini_micro_aggressor_participation_divergence": ARB_INTRA_PRODUCT_CLASS,
}


class GraveyardError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ClassTombstone:
    mechanism_class: str
    regime: str
    death_cause: str
    candidate_count: int
    source_scope: str
    evidence_sha256: str

    @property
    def signature_hash(self) -> str:
        payload = {
            "mechanism_class": self.mechanism_class,
            "regime": self.regime,
            "death_cause": self.death_cause,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()


def build_graveyard(
    *,
    registry_path: str | Path,
    phase2_result_path: str | Path,
    output_path: str | Path,
    grammar_result_paths: Iterable[str | Path] = (),
) -> dict[str, Any]:
    registry = Path(registry_path)
    phase2 = Path(phase2_result_path)
    if not registry.is_file() or not phase2.is_file():
        raise GraveyardError("graveyard sources are missing")
    phase2_payload = json.loads(phase2.read_text(encoding="utf-8"))
    if phase2_payload.get("verdict") != "NULL" or int(
        phase2_payload.get("candidate_count", -1)
    ) != PHASE2_BASKET_COUNT:
        raise GraveyardError("Phase 2 null evidence is not the frozen 55-basket scope")
    grammar_results = tuple(Path(path) for path in grammar_result_paths)
    if any(not path.is_file() for path in grammar_results):
        raise GraveyardError("grammar result source is missing")
    source_rows = _registry_class_rows(registry)
    registered_count = sum(row.candidate_count for row in source_rows)
    if registered_count > LEGACY_PROTOTYPE_COUNT:
        raise GraveyardError("registry count exceeds inherited prototype counter")
    residual = LEGACY_PROTOTYPE_COUNT - registered_count
    tombstones = list(source_rows)
    if residual:
        tombstones.append(
            ClassTombstone(
                mechanism_class="UNREGISTERED_HISTORICAL_PROTOTYPES",
                regime="UNATTRIBUTED_LEGACY",
                death_cause="SCREENED_NOT_REGISTRY_PERSISTED",
                candidate_count=residual,
                source_scope="mission.strategy_prototypes_generated",
                evidence_sha256=_sha256(registry),
            )
        )
    tombstones.append(
        ClassTombstone(
            mechanism_class="V6_STATIC_ACCOUNT_BASKET",
            regime="DEVELOPMENT_2023_TO_2024Q3",
            death_cause="MULTIPLICITY_BH_NOT_REJECTED",
            candidate_count=PHASE2_BASKET_COUNT,
            source_scope="HYDRA_V7_PHASE2",
            evidence_sha256=_sha256(phase2),
        )
    )
    grammar_rows: list[ClassTombstone] = []
    for grammar_result in grammar_results:
        grammar_rows.extend(_grammar_class_rows(grammar_result))
    tombstones.extend(grammar_rows)
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    if temporary.exists():
        temporary.unlink()
    _write_database(
        temporary,
        tombstones,
        metadata={
            "schema_version": SCHEMA_VERSION,
            "registry_sha256": _sha256(registry),
            "phase2_sha256": _sha256(phase2),
            "legacy_prototype_count": str(LEGACY_PROTOTYPE_COUNT),
            "registered_candidate_count": str(registered_count),
            "unregistered_residual_count": str(residual),
            "phase2_new_tombstone_count": str(PHASE2_BASKET_COUNT),
            "grammar_result_count": str(len(grammar_results)),
            "grammar_new_tombstone_count": str(
                sum(row.candidate_count for row in grammar_rows)
            ),
            "grammar_result_sha256s": json.dumps(
                [_sha256(path) for path in grammar_results],
                separators=(",", ":"),
            ),
            "parameter_feedback_permitted": "false",
        },
    )
    os.replace(temporary, destination)
    audit = audit_graveyard(destination)
    audit.update(
        {
            "path": str(destination),
            "sha256": _sha256(destination),
            "registered_candidate_count": registered_count,
            "unregistered_residual_count": residual,
            "legacy_indexed_count": LEGACY_PROTOTYPE_COUNT,
            "new_phase2_tombstone_count": PHASE2_BASKET_COUNT,
            "new_grammar_tombstone_count": sum(
                row.candidate_count for row in grammar_rows
            ),
        }
    )
    return audit


def audit_graveyard(path: str | Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
    try:
        integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise GraveyardError(f"graveyard integrity failed: {integrity}")
        schema = dict(conn.execute("SELECT key,value FROM metadata"))
        if schema.get("schema_version") != SCHEMA_VERSION:
            raise GraveyardError("graveyard schema version mismatch")
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(class_tombstones)")
        }
        forbidden = {
            name
            for name in columns
            if "parameter" in name.lower()
            or name.lower() in {"candidate_id", "threshold", "score"}
        }
        if forbidden:
            raise GraveyardError(
                "graveyard leaks parameter-level feedback: " + ",".join(forbidden)
            )
        row = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(candidate_count),0) FROM class_tombstones"
        ).fetchone()
        causes = {
            str(cause): int(count)
            for cause, count in conn.execute(
                "SELECT death_cause,SUM(candidate_count) FROM class_tombstones "
                "GROUP BY death_cause ORDER BY death_cause"
            )
        }
        return {
            "integrity": integrity,
            "class_signature_count": int(row[0]),
            "indexed_object_count": int(row[1]),
            "death_cause_counts": causes,
            "parameter_level_columns": [],
            "generator_feedback_scope": [
                "mechanism_class",
                "regime",
                "death_cause",
                "candidate_count",
            ],
        }
    finally:
        conn.close()


def class_feedback(path: str | Path) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT mechanism_class,regime,death_cause,candidate_count "
            "FROM class_tombstones ORDER BY mechanism_class,regime,death_cause"
        ).fetchall()
        return tuple(dict(row) for row in rows)
    finally:
        conn.close()


def append_class_tombstone(
    path: str | Path,
    tombstone: ClassTombstone,
) -> dict[str, Any]:
    """Append one class-level death signature without exposing candidate feedback."""

    destination = Path(path)
    audit_graveyard(destination)
    if tombstone.candidate_count <= 0 or not tombstone.mechanism_class.strip():
        raise GraveyardError("incremental tombstone is invalid")
    conn = sqlite3.connect(destination)
    try:
        conn.execute("BEGIN IMMEDIATE")
        prior = conn.execute(
            "SELECT mechanism_class,regime,death_cause,candidate_count,"
            "source_scope,evidence_sha256 FROM class_tombstones "
            "WHERE signature_hash=?",
            (tombstone.signature_hash,),
        ).fetchone()
        if prior is not None:
            expected = (
                tombstone.mechanism_class,
                tombstone.regime,
                tombstone.death_cause,
                tombstone.candidate_count,
                tombstone.source_scope,
                tombstone.evidence_sha256,
            )
            if tuple(prior) != expected:
                raise GraveyardError(
                    "incremental tombstone signature already exists with different evidence"
                )
            conn.rollback()
            result = audit_graveyard(destination)
            result["append_status"] = "ALREADY_PRESENT_IDENTICAL"
            result["signature_hash"] = tombstone.signature_hash
            return result
        conn.execute(
            "INSERT INTO class_tombstones("
            "signature_hash,mechanism_class,regime,death_cause,candidate_count,"
            "source_scope,evidence_sha256) VALUES(?,?,?,?,?,?,?)",
            (
                tombstone.signature_hash,
                tombstone.mechanism_class,
                tombstone.regime,
                tombstone.death_cause,
                tombstone.candidate_count,
                tombstone.source_scope,
                tombstone.evidence_sha256,
            ),
        )
        prior_count_row = conn.execute(
            "SELECT value FROM metadata WHERE key='incremental_tombstone_count'"
        ).fetchone()
        prior_count = int(prior_count_row[0]) if prior_count_row else 0
        conn.execute(
            "INSERT INTO metadata(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("incremental_tombstone_count", str(prior_count + 1)),
        )
        prior_head_row = conn.execute(
            "SELECT value FROM metadata WHERE key='incremental_chain_head'"
        ).fetchone()
        prior_head = str(prior_head_row[0]) if prior_head_row else "0" * 64
        conn.execute(
            "INSERT INTO metadata(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (
                "incremental_chain_head",
                hashlib.sha256(
                    (
                        prior_head
                        + tombstone.signature_hash
                        + tombstone.evidence_sha256
                    ).encode("ascii")
                ).hexdigest(),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    result = audit_graveyard(destination)
    result["append_status"] = "APPENDED"
    result["signature_hash"] = tombstone.signature_hash
    return result


def verify_class_tombstone(
    path: str | Path,
    tombstone: ClassTombstone,
) -> dict[str, Any]:
    """Verify one completed append without mutating a later graveyard state.

    Terminal campaign runtimes are replayed on every controller step.  Once a
    receipt exists, later campaigns may legitimately append other class-level
    tombstones, so recovery must verify the immutable row rather than require
    the global table totals to remain frozen forever.
    """

    destination = Path(path)
    audit = audit_graveyard(destination)
    conn = sqlite3.connect(
        f"file:{destination.resolve()}?mode=ro",
        uri=True,
    )
    try:
        prior = conn.execute(
            "SELECT mechanism_class,regime,death_cause,candidate_count,"
            "source_scope,evidence_sha256 FROM class_tombstones "
            "WHERE signature_hash=?",
            (tombstone.signature_hash,),
        ).fetchone()
    finally:
        conn.close()
    expected = (
        tombstone.mechanism_class,
        tombstone.regime,
        tombstone.death_cause,
        tombstone.candidate_count,
        tombstone.source_scope,
        tombstone.evidence_sha256,
    )
    if prior is None:
        raise GraveyardError("completed incremental tombstone is missing")
    if tuple(prior) != expected:
        raise GraveyardError(
            "completed incremental tombstone differs from its receipt"
        )
    audit["append_status"] = "ALREADY_PRESENT_IDENTICAL"
    audit["signature_hash"] = tombstone.signature_hash
    return audit


def canonical_mechanism_class(mechanism_class: str) -> str:
    normalized = str(mechanism_class).strip()
    return _CLASS_ALIASES.get(normalized, normalized)


def _registry_class_rows(path: Path) -> tuple[ClassTombstone, ...]:
    conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT family,COALESCE(NULLIF(rejection_reason,''),'UNRESOLVED_LEGACY'),"
            "COUNT(*) FROM candidates GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall()
    finally:
        conn.close()
    source_hash = _sha256(path)
    return tuple(
        ClassTombstone(
            mechanism_class=str(family),
            regime="UNATTRIBUTED_LEGACY",
            death_cause=str(cause).upper(),
            candidate_count=int(count),
            source_scope="registry.hydra_registry.candidates",
            evidence_sha256=source_hash,
        )
        for family, cause, count in rows
    )


def _grammar_class_rows(path: Path) -> tuple[ClassTombstone, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    grammar_id = str(payload.get("grammar_id", "")).strip()
    candidate_results = payload.get("candidate_results")
    if not grammar_id or not isinstance(candidate_results, list):
        raise GraveyardError("invalid frozen grammar result")
    if payload.get("selected_shadow_queue_candidate_ids"):
        raise GraveyardError("graveyard cannot tombstone a grammar with promotions")
    source_hash = _sha256(path)
    grouped: dict[tuple[str, str, str], int] = {}
    for result in candidate_results:
        if not isinstance(result, dict):
            raise GraveyardError("invalid grammar candidate result")
        specification = result.get("specification")
        if not isinstance(specification, dict):
            raise GraveyardError("grammar result lacks class specification")
        mechanism_class = canonical_mechanism_class(
            str(specification.get("mechanism_class", ""))
        )
        if not mechanism_class:
            raise GraveyardError("grammar result has empty mechanism class")
        if mechanism_class == ARB_INTRA_PRODUCT_CLASS:
            regime = "DEVELOPMENT_IS_TO_WF_COLLAPSE"
            cause = ARB_INTRA_PRODUCT_DEATH_CAUSE
        else:
            regime = "DEVELOPMENT_2023_TO_2024Q3"
            cause = _contract_death_cause(result)
        grouped[(mechanism_class, regime, cause)] = (
            grouped.get((mechanism_class, regime, cause), 0) + 1
        )
    return tuple(
        ClassTombstone(
            mechanism_class=mechanism_class,
            regime=regime,
            death_cause=cause,
            candidate_count=count,
            source_scope=f"HYDRA_V7_GRAMMAR:{grammar_id}",
            evidence_sha256=source_hash,
        )
        for (mechanism_class, regime, cause), count in sorted(grouped.items())
    )


def _contract_death_cause(result: Mapping[str, Any]) -> str:
    base = result.get("base")
    stress_2x = result.get("stress_2x")
    base_expectancy = (
        float(base.get("expectancy_per_trade", 0.0))
        if isinstance(base, Mapping)
        else 0.0
    )
    stress_2x_expectancy = (
        float(stress_2x.get("expectancy_per_trade", 0.0))
        if isinstance(stress_2x, Mapping)
        else 0.0
    )
    if base_expectancy > 0.0 and stress_2x_expectancy <= 0.0:
        return "SIM_EXPLOIT"
    if not bool(result.get("stage1_pass")):
        return "INSUFFICIENT_EVENT_COUNT_OR_BASE_ECONOMICS"
    if not bool(result.get("stage2_pass")):
        return "COST_OR_RULESET_FAILURE"
    null_suite = result.get("candidate_null_suite")
    if isinstance(null_suite, Mapping) and not bool(null_suite.get("passed")):
        return "CANDIDATE_NULL_FAILURE"
    dsr = result.get("DSR")
    bh = result.get("BH")
    dsr_positive = isinstance(dsr, Mapping) and float(
        dsr.get("deflated_z", -1.0e12)
    ) > 0.0
    bh_rejected = isinstance(bh, Mapping) and bool(bh.get("rejected"))
    if not dsr_positive or not bh_rejected:
        return "MULTIPLICITY_DEFLATION_FAILURE"
    return "GRAMMAR_TRIPWIRE_OR_PROMOTION_FAILURE"


def _write_database(
    path: Path,
    tombstones: Iterable[ClassTombstone],
    *,
    metadata: Mapping[str, str],
) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            PRAGMA journal_mode=DELETE;
            PRAGMA synchronous=FULL;
            CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE class_tombstones (
                signature_hash TEXT PRIMARY KEY,
                mechanism_class TEXT NOT NULL,
                regime TEXT NOT NULL,
                death_cause TEXT NOT NULL,
                candidate_count INTEGER NOT NULL CHECK(candidate_count >= 0),
                source_scope TEXT NOT NULL,
                evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256) = 64),
                UNIQUE(mechanism_class,regime,death_cause)
            ) WITHOUT ROWID;
            CREATE INDEX class_tombstones_lookup
                ON class_tombstones(mechanism_class,regime,death_cause);
            """
        )
        conn.executemany(
            "INSERT INTO metadata(key,value) VALUES(?,?)",
            sorted((str(key), str(value)) for key, value in metadata.items()),
        )
        combined: dict[str, ClassTombstone] = {}
        for row in tombstones:
            key = row.signature_hash
            prior = combined.get(key)
            if prior is None:
                combined[key] = row
            else:
                combined[key] = ClassTombstone(
                    mechanism_class=row.mechanism_class,
                    regime=row.regime,
                    death_cause=row.death_cause,
                    candidate_count=prior.candidate_count + row.candidate_count,
                    source_scope=prior.source_scope + "+" + row.source_scope,
                    evidence_sha256=hashlib.sha256(
                        (prior.evidence_sha256 + row.evidence_sha256).encode("ascii")
                    ).hexdigest(),
                )
        conn.executemany(
            "INSERT INTO class_tombstones("
            "signature_hash,mechanism_class,regime,death_cause,candidate_count,"
            "source_scope,evidence_sha256"
            ") VALUES(?,?,?,?,?,?,?)",
            [
                (
                    row.signature_hash,
                    row.mechanism_class,
                    row.regime,
                    row.death_cause,
                    row.candidate_count,
                    row.source_scope,
                    row.evidence_sha256,
                )
                for row in sorted(
                    combined.values(),
                    key=lambda item: (
                        item.mechanism_class,
                        item.regime,
                        item.death_cause,
                    ),
                )
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ARB_INTRA_PRODUCT_CLASS",
    "ARB_INTRA_PRODUCT_DEATH_CAUSE",
    "ClassTombstone",
    "GraveyardError",
    "SCHEMA_VERSION",
    "audit_graveyard",
    "append_class_tombstone",
    "build_graveyard",
    "canonical_mechanism_class",
    "class_feedback",
]
