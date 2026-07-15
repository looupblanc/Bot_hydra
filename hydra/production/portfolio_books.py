"""Deterministic portfolio-book pair generation for Combine and XFA research.

The generator assembles immutable sleeve ledgers into two account-policy books:
one for reaching the Combine target and one for surviving/paying out in XFA.
It changes only membership, small discrete risk units, and conflict handling.
Underlying signals, entries, exits, and trade paths remain checksum-bound and
cannot be mutated by this module.
"""

from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


SLEEVE_RECORD_SCHEMA = "hydra_portfolio_sleeve_record_v1"
BOOK_PAIR_SCHEMA = "hydra_portfolio_book_pair_v1"
BOOK_GENERATOR_MANIFEST_SCHEMA = "hydra_portfolio_book_generator_manifest_v1"
BOOK_GENERATION_RESULT_SCHEMA = "hydra_portfolio_book_generation_result_v1"
GENERATOR_VERSION = "hydra_portfolio_book_generator_v1"
CONFLICT_POLICIES = ("PRIORITY", "NET_TO_FLAT", "REJECT_BOTH")
COMBINE_ALLOCATION_UNITS = (1, 2, 3)
XFA_ALLOCATION_UNITS = (1, 2)
STATIC_RISK_FRONTIER = (0.75, 1.0, 1.15, 1.3)
MINIMUM_BOOK_PAIR_TARGET = 20_000
MINIMUM_BEHAVIORALLY_NOVEL_FRACTION = 0.20

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class PortfolioBookError(ValueError):
    """A portfolio book declaration or generated population is invalid."""


def stable_hash(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PortfolioBookError("portfolio book payload is not canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class SleeveRecord:
    """Immutable executable sleeve identity backed by signal and trade ledgers."""

    sleeve_id: str
    immutable_fingerprint: str
    behavioral_fingerprint: str
    signal_ledger_sha256: str
    trade_ledger_sha256: str
    market: str
    contract: str
    timeframe: str
    session: str
    economic_role: str
    source_campaign: str
    family_id: str
    evidence_complete: bool = True
    development_only: bool = True
    inherited_status: bool = False
    signal_mutation_allowed: bool = False

    def __post_init__(self) -> None:
        for name in (
            "sleeve_id",
            "market",
            "contract",
            "timeframe",
            "session",
            "economic_role",
            "source_campaign",
            "family_id",
        ):
            value = str(getattr(self, name))
            if not value or (name == "sleeve_id" and _SAFE_ID.fullmatch(value) is None):
                raise PortfolioBookError(f"invalid SleeveRecord field: {name}")
        for name in (
            "immutable_fingerprint",
            "behavioral_fingerprint",
            "signal_ledger_sha256",
            "trade_ledger_sha256",
        ):
            if _SHA256.fullmatch(str(getattr(self, name))) is None:
                raise PortfolioBookError(f"{name} must be a lowercase SHA-256")
        if (
            not self.evidence_complete
            or not self.development_only
            or self.inherited_status
            or self.signal_mutation_allowed
        ):
            raise PortfolioBookError("unsafe or incomplete immutable sleeve record")

    @property
    def record_fingerprint(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SLEEVE_RECORD_SCHEMA,
            "sleeve_id": self.sleeve_id,
            "immutable_fingerprint": self.immutable_fingerprint,
            "behavioral_fingerprint": self.behavioral_fingerprint,
            "signal_ledger_sha256": self.signal_ledger_sha256,
            "trade_ledger_sha256": self.trade_ledger_sha256,
            "market": self.market,
            "contract": self.contract,
            "timeframe": self.timeframe,
            "session": self.session,
            "economic_role": self.economic_role,
            "source_campaign": self.source_campaign,
            "family_id": self.family_id,
            "evidence_complete": self.evidence_complete,
            "development_only": self.development_only,
            "inherited_status": self.inherited_status,
            "signal_mutation_allowed": self.signal_mutation_allowed,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SleeveRecord":
        if value.get("schema") not in {None, SLEEVE_RECORD_SCHEMA}:
            raise PortfolioBookError("SleeveRecord schema drift")
        return cls(
            sleeve_id=str(value["sleeve_id"]),
            immutable_fingerprint=str(value["immutable_fingerprint"]),
            behavioral_fingerprint=str(value["behavioral_fingerprint"]),
            signal_ledger_sha256=str(value["signal_ledger_sha256"]),
            trade_ledger_sha256=str(value["trade_ledger_sha256"]),
            market=str(value["market"]),
            contract=str(value["contract"]),
            timeframe=str(value["timeframe"]),
            session=str(value["session"]),
            economic_role=str(value["economic_role"]),
            source_campaign=str(value["source_campaign"]),
            family_id=str(value["family_id"]),
            evidence_complete=bool(value.get("evidence_complete", True)),
            development_only=bool(value.get("development_only", True)),
            inherited_status=bool(value.get("inherited_status", False)),
            signal_mutation_allowed=bool(value.get("signal_mutation_allowed", False)),
        )


@dataclass(frozen=True, slots=True)
class BookPair:
    """Immutable Combine/XFA book pair with checksum-bound source sleeves."""

    pair_id: str
    combine_sleeve_ids: tuple[str, ...]
    combine_allocation_units: tuple[int, ...]
    combine_risk_tier: float
    xfa_sleeve_ids: tuple[str, ...]
    xfa_allocation_units: tuple[int, ...]
    xfa_risk_tier: float
    conflict_policy: str
    source_bindings: tuple[tuple[str, str, str, str, str], ...]
    structural_fingerprint: str
    behavioral_fingerprint: str
    behaviorally_novel: bool
    generator_seed: int
    proposal_index: int
    generator_version: str = GENERATOR_VERSION
    signals_mutated: bool = False
    entries_or_exits_mutated: bool = False
    status_inherited: bool = False
    development_only: bool = True
    broker_connections: int = 0
    orders: int = 0

    def __post_init__(self) -> None:
        if self.generator_version != GENERATOR_VERSION:
            raise PortfolioBookError("book generator version drift")
        if self.conflict_policy not in CONFLICT_POLICIES:
            raise PortfolioBookError("unknown book conflict policy")
        if not 2 <= len(self.combine_sleeve_ids) <= 6:
            raise PortfolioBookError("Combine book must contain 2-6 sleeves")
        if not 1 <= len(self.xfa_sleeve_ids) <= 6:
            raise PortfolioBookError("XFA book must contain 1-6 sleeves")
        if (
            len(set(self.combine_sleeve_ids)) != len(self.combine_sleeve_ids)
            or len(set(self.xfa_sleeve_ids)) != len(self.xfa_sleeve_ids)
        ):
            raise PortfolioBookError("a book may not repeat a sleeve")
        if len(self.combine_allocation_units) != len(self.combine_sleeve_ids) or len(
            self.xfa_allocation_units
        ) != len(self.xfa_sleeve_ids):
            raise PortfolioBookError("book sleeve/allocation cardinality drift")
        if any(value not in COMBINE_ALLOCATION_UNITS for value in self.combine_allocation_units):
            raise PortfolioBookError("Combine book escaped discrete allocation units")
        if any(value not in XFA_ALLOCATION_UNITS for value in self.xfa_allocation_units):
            raise PortfolioBookError("XFA book escaped discrete allocation units")
        if (
            float(self.combine_risk_tier) not in STATIC_RISK_FRONTIER
            or float(self.xfa_risk_tier) not in STATIC_RISK_FRONTIER
        ):
            raise PortfolioBookError("portfolio book escaped the static risk frontier")
        if sum(self.combine_allocation_units) > 12 or sum(self.xfa_allocation_units) > 8:
            raise PortfolioBookError("portfolio book total allocation is excessive")
        if self.proposal_index < 0 or isinstance(self.generator_seed, bool):
            raise PortfolioBookError("invalid deterministic generator provenance")
        if (
            self.signals_mutated
            or self.entries_or_exits_mutated
            or self.status_inherited
            or not self.development_only
            or self.broker_connections
            or self.orders
        ):
            raise PortfolioBookError("unsafe portfolio book declaration")
        binding_ids = [row[0] for row in self.source_bindings]
        expected_ids = set(self.combine_sleeve_ids) | set(self.xfa_sleeve_ids)
        if binding_ids != sorted(expected_ids) or len(binding_ids) != len(set(binding_ids)):
            raise PortfolioBookError("book source bindings do not match membership")
        for binding in self.source_bindings:
            if len(binding) != 5 or any(
                _SHA256.fullmatch(str(value)) is None for value in binding[1:]
            ):
                raise PortfolioBookError("invalid checksum-bound sleeve binding")
        expected_structural = stable_hash(self._structural_payload())
        expected_behavioral = stable_hash(self._behavioral_payload())
        if self.structural_fingerprint != expected_structural:
            raise PortfolioBookError("book structural fingerprint drift")
        if self.behavioral_fingerprint != expected_behavioral:
            raise PortfolioBookError("book behavioral fingerprint drift")
        if self.pair_id != f"portfolio_book_pair_{expected_structural[:24]}":
            raise PortfolioBookError("book pair ID drift")

    @classmethod
    def create(
        cls,
        *,
        combine_sleeves: Sequence[SleeveRecord],
        combine_allocation_units: Sequence[int],
        combine_risk_tier: float = 1.0,
        xfa_sleeves: Sequence[SleeveRecord],
        xfa_allocation_units: Sequence[int],
        xfa_risk_tier: float = 1.0,
        conflict_policy: str,
        behaviorally_novel: bool,
        generator_seed: int,
        proposal_index: int,
    ) -> "BookPair":
        if conflict_policy in {"NET_TO_FLAT", "REJECT_BOTH"}:
            combine = sorted(
                zip(combine_sleeves, combine_allocation_units, strict=True),
                key=lambda row: row[0].sleeve_id,
            )
            xfa = sorted(
                zip(xfa_sleeves, xfa_allocation_units, strict=True),
                key=lambda row: row[0].sleeve_id,
            )
        else:
            combine = list(zip(combine_sleeves, combine_allocation_units, strict=True))
            xfa = list(zip(xfa_sleeves, xfa_allocation_units, strict=True))
        combine_ids = tuple(row[0].sleeve_id for row in combine)
        combine_units = tuple(int(row[1]) for row in combine)
        xfa_ids = tuple(row[0].sleeve_id for row in xfa)
        xfa_units = tuple(int(row[1]) for row in xfa)
        by_id = {
            row.sleeve_id: row
            for row in (*[value[0] for value in combine], *[value[0] for value in xfa])
        }
        bindings = tuple(
            (
                sleeve_id,
                by_id[sleeve_id].immutable_fingerprint,
                by_id[sleeve_id].behavioral_fingerprint,
                by_id[sleeve_id].signal_ledger_sha256,
                by_id[sleeve_id].trade_ledger_sha256,
            )
            for sleeve_id in sorted(by_id)
        )
        provisional = object.__new__(cls)
        object.__setattr__(provisional, "combine_sleeve_ids", combine_ids)
        object.__setattr__(provisional, "combine_allocation_units", combine_units)
        object.__setattr__(provisional, "combine_risk_tier", float(combine_risk_tier))
        object.__setattr__(provisional, "xfa_sleeve_ids", xfa_ids)
        object.__setattr__(provisional, "xfa_allocation_units", xfa_units)
        object.__setattr__(provisional, "xfa_risk_tier", float(xfa_risk_tier))
        object.__setattr__(provisional, "conflict_policy", conflict_policy)
        object.__setattr__(provisional, "source_bindings", bindings)
        structural = stable_hash(provisional._structural_payload())
        behavioral = stable_hash(provisional._behavioral_payload())
        return cls(
            pair_id=f"portfolio_book_pair_{structural[:24]}",
            combine_sleeve_ids=combine_ids,
            combine_allocation_units=combine_units,
            combine_risk_tier=float(combine_risk_tier),
            xfa_sleeve_ids=xfa_ids,
            xfa_allocation_units=xfa_units,
            xfa_risk_tier=float(xfa_risk_tier),
            conflict_policy=conflict_policy,
            source_bindings=bindings,
            structural_fingerprint=structural,
            behavioral_fingerprint=behavioral,
            behaviorally_novel=bool(behaviorally_novel),
            generator_seed=int(generator_seed),
            proposal_index=int(proposal_index),
        )

    def _structural_payload(self) -> dict[str, Any]:
        binding = {row[0]: row for row in self.source_bindings}
        return {
            "schema": BOOK_PAIR_SCHEMA,
            "combine": [
                [
                    sleeve_id,
                    units,
                    binding[sleeve_id][1],
                    binding[sleeve_id][3],
                    binding[sleeve_id][4],
                ]
                for sleeve_id, units in zip(
                    self.combine_sleeve_ids,
                    self.combine_allocation_units,
                    strict=True,
                )
            ],
            "combine_risk_tier": self.combine_risk_tier,
            "xfa": [
                [
                    sleeve_id,
                    units,
                    binding[sleeve_id][1],
                    binding[sleeve_id][3],
                    binding[sleeve_id][4],
                ]
                for sleeve_id, units in zip(
                    self.xfa_sleeve_ids,
                    self.xfa_allocation_units,
                    strict=True,
                )
            ],
            "xfa_risk_tier": self.xfa_risk_tier,
            "conflict_policy": self.conflict_policy,
            "underlying_signals_mutated": False,
        }

    def _behavioral_payload(self) -> dict[str, Any]:
        behavior = {row[0]: row[2] for row in self.source_bindings}
        combine = [
            [behavior[sleeve_id], units]
            for sleeve_id, units in zip(
                self.combine_sleeve_ids,
                self.combine_allocation_units,
                strict=True,
            )
        ]
        xfa = [
            [behavior[sleeve_id], units]
            for sleeve_id, units in zip(
                self.xfa_sleeve_ids,
                self.xfa_allocation_units,
                strict=True,
            )
        ]
        if self.conflict_policy in {"NET_TO_FLAT", "REJECT_BOTH"}:
            combine.sort()
            xfa.sort()
        return {
            "schema": "hydra_portfolio_book_behavior_v1",
            "combine": combine,
            "combine_risk_tier": self.combine_risk_tier,
            "xfa": xfa,
            "xfa_risk_tier": self.xfa_risk_tier,
            "conflict_policy": self.conflict_policy,
        }

    def verify_immutable_sources(self, sleeves: Mapping[str, SleeveRecord]) -> bool:
        for sleeve_id, immutable, behavioral, signal, trade in self.source_bindings:
            sleeve = sleeves.get(sleeve_id)
            if sleeve is None or (
                sleeve.immutable_fingerprint,
                sleeve.behavioral_fingerprint,
                sleeve.signal_ledger_sha256,
                sleeve.trade_ledger_sha256,
            ) != (immutable, behavioral, signal, trade):
                raise PortfolioBookError(f"immutable sleeve binding drift: {sleeve_id}")
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": BOOK_PAIR_SCHEMA,
            "pair_id": self.pair_id,
            "combine_sleeve_ids": list(self.combine_sleeve_ids),
            "combine_allocation_units": list(self.combine_allocation_units),
            "combine_risk_tier": self.combine_risk_tier,
            "xfa_sleeve_ids": list(self.xfa_sleeve_ids),
            "xfa_allocation_units": list(self.xfa_allocation_units),
            "xfa_risk_tier": self.xfa_risk_tier,
            "conflict_policy": self.conflict_policy,
            "source_bindings": [list(row) for row in self.source_bindings],
            "structural_fingerprint": self.structural_fingerprint,
            "behavioral_fingerprint": self.behavioral_fingerprint,
            "behaviorally_novel": self.behaviorally_novel,
            "generator_seed": self.generator_seed,
            "proposal_index": self.proposal_index,
            "generator_version": self.generator_version,
            "signals_mutated": self.signals_mutated,
            "entries_or_exits_mutated": self.entries_or_exits_mutated,
            "status_inherited": self.status_inherited,
            "development_only": self.development_only,
            "broker_connections": self.broker_connections,
            "orders": self.orders,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "BookPair":
        if value.get("schema") != BOOK_PAIR_SCHEMA:
            raise PortfolioBookError("BookPair schema drift")
        return cls(
            pair_id=str(value["pair_id"]),
            combine_sleeve_ids=tuple(str(row) for row in value["combine_sleeve_ids"]),
            combine_allocation_units=tuple(
                int(row) for row in value["combine_allocation_units"]
            ),
            combine_risk_tier=float(value["combine_risk_tier"]),
            xfa_sleeve_ids=tuple(str(row) for row in value["xfa_sleeve_ids"]),
            xfa_allocation_units=tuple(int(row) for row in value["xfa_allocation_units"]),
            xfa_risk_tier=float(value["xfa_risk_tier"]),
            conflict_policy=str(value["conflict_policy"]),
            source_bindings=tuple(
                tuple(str(item) for item in row) for row in value["source_bindings"]
            ),
            structural_fingerprint=str(value["structural_fingerprint"]),
            behavioral_fingerprint=str(value["behavioral_fingerprint"]),
            behaviorally_novel=bool(value["behaviorally_novel"]),
            generator_seed=int(value["generator_seed"]),
            proposal_index=int(value["proposal_index"]),
            generator_version=str(value.get("generator_version") or ""),
            signals_mutated=bool(value.get("signals_mutated", False)),
            entries_or_exits_mutated=bool(
                value.get("entries_or_exits_mutated", False)
            ),
            status_inherited=bool(value.get("status_inherited", False)),
            development_only=bool(value.get("development_only", True)),
            broker_connections=int(value.get("broker_connections", 0)),
            orders=int(value.get("orders", 0)),
        )


@dataclass(frozen=True, slots=True)
class PortfolioBookGeneratorSpec:
    seed: int
    unique_pair_target: int = MINIMUM_BOOK_PAIR_TARGET
    combine_sleeve_minimum: int = 2
    combine_sleeve_maximum: int = 6
    xfa_sleeve_minimum: int = 1
    xfa_sleeve_maximum: int = 6
    combine_allocation_units: tuple[int, ...] = COMBINE_ALLOCATION_UNITS
    xfa_allocation_units: tuple[int, ...] = XFA_ALLOCATION_UNITS
    risk_frontier: tuple[float, ...] = STATIC_RISK_FRONTIER
    conflict_policies: tuple[str, ...] = CONFLICT_POLICIES
    combine_total_allocation_maximum: int = 12
    xfa_total_allocation_maximum: int = 8
    minimum_behaviorally_novel_fraction: float = (
        MINIMUM_BEHAVIORALLY_NOVEL_FRACTION
    )
    reference_book_behavioral_fingerprints: tuple[str, ...] = ()
    excluded_structural_fingerprints: tuple[str, ...] = ()
    maximum_attempt_multiplier: int = 100
    structural_deduplication: bool = True
    behavioral_deduplication: bool = True
    mutate_underlying_signals: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise PortfolioBookError("portfolio generator seed must be an integer")
        if self.unique_pair_target < MINIMUM_BOOK_PAIR_TARGET:
            raise PortfolioBookError("portfolio generator target must be at least 20,000")
        if (self.combine_sleeve_minimum, self.combine_sleeve_maximum) != (2, 6):
            raise PortfolioBookError("Combine book size policy must remain 2-6")
        if (self.xfa_sleeve_minimum, self.xfa_sleeve_maximum) != (1, 6):
            raise PortfolioBookError("XFA book size policy must remain 1-6")
        if self.combine_allocation_units != COMBINE_ALLOCATION_UNITS:
            raise PortfolioBookError("Combine allocation frontier drift")
        if self.xfa_allocation_units != XFA_ALLOCATION_UNITS:
            raise PortfolioBookError("XFA allocation frontier drift")
        if tuple(float(value) for value in self.risk_frontier) != STATIC_RISK_FRONTIER:
            raise PortfolioBookError("portfolio static risk frontier drift")
        if (
            not self.conflict_policies
            or len(set(self.conflict_policies)) != len(self.conflict_policies)
            or not set(self.conflict_policies).issubset(CONFLICT_POLICIES)
        ):
            raise PortfolioBookError(
                "conflict policies must be a non-empty declared subset"
            )
        if (
            self.combine_total_allocation_maximum != 12
            or self.xfa_total_allocation_maximum != 8
        ):
            raise PortfolioBookError("book allocation cap drift")
        if not 0.20 <= self.minimum_behaviorally_novel_fraction <= 1.0:
            raise PortfolioBookError("behavioral novelty floor must be at least 20%")
        if self.maximum_attempt_multiplier < 2:
            raise PortfolioBookError("generation attempt bound is too small")
        if (
            not self.structural_deduplication
            or not self.behavioral_deduplication
            or self.mutate_underlying_signals
        ):
            raise PortfolioBookError("unsafe portfolio generation declaration")
        for values, label in (
            (self.reference_book_behavioral_fingerprints, "behavioral reference"),
            (self.excluded_structural_fingerprints, "structural exclusion"),
        ):
            if len(set(values)) != len(values) or any(
                _SHA256.fullmatch(str(value)) is None for value in values
            ):
                raise PortfolioBookError(f"invalid {label} fingerprints")

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> "PortfolioBookGeneratorSpec":
        value = manifest.get("portfolio_books", manifest)
        if not isinstance(value, Mapping):
            raise PortfolioBookError("portfolio_books manifest object is required")
        claimed = value.get("manifest_hash")
        if claimed is not None:
            payload = dict(value)
            payload.pop("manifest_hash", None)
            if stable_hash(payload) != str(claimed):
                raise PortfolioBookError("portfolio book manifest hash drift")
        if value.get("schema") != BOOK_GENERATOR_MANIFEST_SCHEMA:
            raise PortfolioBookError("portfolio book generator schema drift")
        combine = _mapping(value, "combine_book")
        xfa = _mapping(value, "xfa_book")
        dedup = _mapping(value, "deduplication")
        novelty = _mapping(value, "behavioral_novelty")
        governance = _mapping(value, "governance")
        if any(
            governance.get(key) is not False
            for key in (
                "underlying_signal_mutation_allowed",
                "entry_exit_mutation_allowed",
                "status_inheritance_allowed",
                "broker_connection_allowed",
                "orders_allowed",
            )
        ):
            raise PortfolioBookError("unsafe portfolio-book governance declaration")
        if (
            novelty.get("pre_replay_basis")
            != "SEMANTIC_SLEEVE_COMPOSITION_PREDICTION_ONLY"
            or novelty.get("stage1_observed_account_trajectory_deduplication")
            is not True
            or novelty.get("cross_campaign_account_path_novelty_claimed") is not False
        ):
            raise PortfolioBookError(
                "portfolio behavioral evidence semantics are not explicit"
            )
        return cls(
            seed=int(value["seed"]),
            unique_pair_target=int(value["unique_pair_target"]),
            combine_sleeve_minimum=int(combine["sleeve_minimum"]),
            combine_sleeve_maximum=int(combine["sleeve_maximum"]),
            xfa_sleeve_minimum=int(xfa["sleeve_minimum"]),
            xfa_sleeve_maximum=int(xfa["sleeve_maximum"]),
            combine_allocation_units=tuple(int(row) for row in combine["allocation_units"]),
            xfa_allocation_units=tuple(int(row) for row in xfa["allocation_units"]),
            risk_frontier=tuple(float(row) for row in value["risk_frontier"]),
            conflict_policies=tuple(str(row) for row in value["conflict_policies"]),
            combine_total_allocation_maximum=int(combine["total_allocation_maximum"]),
            xfa_total_allocation_maximum=int(xfa["total_allocation_maximum"]),
            minimum_behaviorally_novel_fraction=float(novelty["minimum_fraction"]),
            reference_book_behavioral_fingerprints=tuple(
                str(row) for row in novelty.get("reference_book_fingerprints") or ()
            ),
            excluded_structural_fingerprints=tuple(
                str(row) for row in dedup.get("excluded_structural_fingerprints") or ()
            ),
            maximum_attempt_multiplier=int(value.get("maximum_attempt_multiplier", 100)),
            structural_deduplication=bool(dedup.get("structural", False)),
            behavioral_deduplication=bool(dedup.get("behavioral", False)),
            mutate_underlying_signals=bool(
                governance.get("underlying_signal_mutation_allowed", True)
            ),
        )

    def to_manifest(self) -> dict[str, Any]:
        value = {
            "schema": BOOK_GENERATOR_MANIFEST_SCHEMA,
            "seed": self.seed,
            "unique_pair_target": self.unique_pair_target,
            "combine_book": {
                "sleeve_minimum": self.combine_sleeve_minimum,
                "sleeve_maximum": self.combine_sleeve_maximum,
                "allocation_units": list(self.combine_allocation_units),
                "total_allocation_maximum": self.combine_total_allocation_maximum,
            },
            "xfa_book": {
                "sleeve_minimum": self.xfa_sleeve_minimum,
                "sleeve_maximum": self.xfa_sleeve_maximum,
                "allocation_units": list(self.xfa_allocation_units),
                "total_allocation_maximum": self.xfa_total_allocation_maximum,
            },
            "conflict_policies": list(self.conflict_policies),
            "risk_frontier": list(self.risk_frontier),
            "deduplication": {
                "structural": self.structural_deduplication,
                "behavioral": self.behavioral_deduplication,
                "excluded_structural_fingerprints": list(
                    self.excluded_structural_fingerprints
                ),
            },
            "behavioral_novelty": {
                "minimum_fraction": self.minimum_behaviorally_novel_fraction,
                "reference_book_fingerprints": list(
                    self.reference_book_behavioral_fingerprints
                ),
                "pre_replay_basis": (
                    "SEMANTIC_SLEEVE_COMPOSITION_PREDICTION_ONLY"
                ),
                "stage1_observed_account_trajectory_deduplication": True,
                "cross_campaign_account_path_novelty_claimed": False,
            },
            "maximum_attempt_multiplier": self.maximum_attempt_multiplier,
            "governance": {
                "underlying_signal_mutation_allowed": False,
                "entry_exit_mutation_allowed": False,
                "status_inheritance_allowed": False,
                "broker_connection_allowed": False,
                "orders_allowed": False,
            },
        }
        value["manifest_hash"] = stable_hash(value)
        return value


@dataclass(frozen=True, slots=True)
class PortfolioBookGenerationResult:
    spec: PortfolioBookGeneratorSpec
    pairs: tuple[BookPair, ...]
    attempted_proposals: int
    structural_duplicate_rejections: int
    behavioral_duplicate_rejections: int
    excluded_structural_rejections: int
    behaviorally_novel_count: int
    population_hash: str

    def __post_init__(self) -> None:
        if len(self.pairs) != self.spec.unique_pair_target:
            raise PortfolioBookError("portfolio book result missed its frozen target")
        if len({row.structural_fingerprint for row in self.pairs}) != len(self.pairs):
            raise PortfolioBookError("structural duplicate escaped generation")
        if len({row.behavioral_fingerprint for row in self.pairs}) != len(self.pairs):
            raise PortfolioBookError("behavioral duplicate escaped generation")
        observed_novel = sum(row.behaviorally_novel for row in self.pairs)
        if observed_novel != self.behaviorally_novel_count:
            raise PortfolioBookError("behavioral novelty count drift")
        if self.behaviorally_novel_fraction < self.spec.minimum_behaviorally_novel_fraction:
            raise PortfolioBookError("behaviorally novel book fraction is below 20%")
        expected_hash = stable_hash(
            {
                "generator_manifest_hash": self.spec.to_manifest()["manifest_hash"],
                "structural_fingerprints": [
                    row.structural_fingerprint for row in self.pairs
                ],
                "behavioral_fingerprints": [
                    row.behavioral_fingerprint for row in self.pairs
                ],
            }
        )
        if self.population_hash != expected_hash:
            raise PortfolioBookError("portfolio book population hash drift")

    @property
    def behaviorally_novel_fraction(self) -> float:
        return self.behaviorally_novel_count / len(self.pairs)

    @property
    def duplicate_rejection_rate(self) -> float:
        duplicates = (
            self.structural_duplicate_rejections
            + self.behavioral_duplicate_rejections
        )
        return duplicates / max(self.attempted_proposals, 1)

    def to_dict(self, *, include_pairs: bool = False) -> dict[str, Any]:
        value = {
            "schema": BOOK_GENERATION_RESULT_SCHEMA,
            "generator_version": GENERATOR_VERSION,
            "generator_manifest_hash": self.spec.to_manifest()["manifest_hash"],
            "unique_pair_count": len(self.pairs),
            "attempted_proposals": self.attempted_proposals,
            "structural_duplicate_rejections": self.structural_duplicate_rejections,
            "behavioral_duplicate_rejections": self.behavioral_duplicate_rejections,
            "excluded_structural_rejections": self.excluded_structural_rejections,
            "behaviorally_novel_count": self.behaviorally_novel_count,
            "behaviorally_novel_fraction": self.behaviorally_novel_fraction,
            "duplicate_rejection_rate": self.duplicate_rejection_rate,
            "underlying_signal_mutation_count": 0,
            "broker_connections": 0,
            "orders": 0,
            "population_hash": self.population_hash,
        }
        if include_pairs:
            value["pairs"] = [row.to_dict() for row in self.pairs]
        return value


def generate_portfolio_book_pairs(
    sleeves: Sequence[SleeveRecord | Mapping[str, Any]],
    spec: PortfolioBookGeneratorSpec | Mapping[str, Any],
) -> PortfolioBookGenerationResult:
    """Generate a deterministic, structurally and behaviorally unique population."""

    resolved_spec = (
        spec
        if isinstance(spec, PortfolioBookGeneratorSpec)
        else PortfolioBookGeneratorSpec.from_manifest(spec)
    )
    records = tuple(
        row if isinstance(row, SleeveRecord) else SleeveRecord.from_mapping(row)
        for row in sleeves
    )
    if len(records) < 6:
        raise PortfolioBookError("at least six immutable sleeves are required")
    by_id = {row.sleeve_id: row for row in records}
    if len(by_id) != len(records):
        raise PortfolioBookError("sleeve bank contains duplicate IDs")
    immutable = {row.immutable_fingerprint for row in records}
    if len(immutable) != len(records):
        raise PortfolioBookError("sleeve bank contains immutable clones")

    rng = random.Random(resolved_spec.seed)
    identifiers = sorted(by_id)
    reference_behaviors = set(resolved_spec.reference_book_behavioral_fingerprints)
    excluded_structural = set(resolved_spec.excluded_structural_fingerprints)
    seen_structural: set[str] = set()
    seen_behavioral: set[str] = set()
    output: list[BookPair] = []
    structural_duplicates = behavioral_duplicates = excluded = 0
    maximum_attempts = (
        resolved_spec.unique_pair_target * resolved_spec.maximum_attempt_multiplier
    )
    for proposal_index in range(maximum_attempts):
        conflict = resolved_spec.conflict_policies[
            proposal_index % len(resolved_spec.conflict_policies)
        ]
        combine_size = resolved_spec.combine_sleeve_minimum + (
            proposal_index
            % (
                resolved_spec.combine_sleeve_maximum
                - resolved_spec.combine_sleeve_minimum
                + 1
            )
        )
        xfa_size = resolved_spec.xfa_sleeve_minimum + (
            (proposal_index // 5)
            % (resolved_spec.xfa_sleeve_maximum - resolved_spec.xfa_sleeve_minimum + 1)
        )
        combine_ids = rng.sample(identifiers, combine_size)
        xfa_ids = rng.sample(identifiers, xfa_size)
        combine_units = _bounded_units(
            rng,
            combine_size,
            resolved_spec.combine_allocation_units,
            resolved_spec.combine_total_allocation_maximum,
        )
        xfa_units = _bounded_units(
            rng,
            xfa_size,
            resolved_spec.xfa_allocation_units,
            resolved_spec.xfa_total_allocation_maximum,
        )
        combine_risk_tier = resolved_spec.risk_frontier[
            proposal_index % len(resolved_spec.risk_frontier)
        ]
        xfa_risk_tier = resolved_spec.risk_frontier[
            (proposal_index // len(resolved_spec.risk_frontier))
            % len(resolved_spec.risk_frontier)
        ]
        pair = BookPair.create(
            combine_sleeves=[by_id[value] for value in combine_ids],
            combine_allocation_units=combine_units,
            combine_risk_tier=combine_risk_tier,
            xfa_sleeves=[by_id[value] for value in xfa_ids],
            xfa_allocation_units=xfa_units,
            xfa_risk_tier=xfa_risk_tier,
            conflict_policy=conflict,
            behaviorally_novel=True,
            generator_seed=resolved_spec.seed,
            proposal_index=proposal_index,
        )
        if pair.structural_fingerprint in excluded_structural:
            excluded += 1
            continue
        if pair.structural_fingerprint in seen_structural:
            structural_duplicates += 1
            continue
        if pair.behavioral_fingerprint in seen_behavioral:
            behavioral_duplicates += 1
            continue
        novel = pair.behavioral_fingerprint not in reference_behaviors
        if not novel:
            pair = BookPair.create(
                combine_sleeves=[by_id[value] for value in combine_ids],
                combine_allocation_units=combine_units,
                combine_risk_tier=combine_risk_tier,
                xfa_sleeves=[by_id[value] for value in xfa_ids],
                xfa_allocation_units=xfa_units,
                xfa_risk_tier=xfa_risk_tier,
                conflict_policy=conflict,
                behaviorally_novel=False,
                generator_seed=resolved_spec.seed,
                proposal_index=proposal_index,
            )
        seen_structural.add(pair.structural_fingerprint)
        seen_behavioral.add(pair.behavioral_fingerprint)
        output.append(pair)
        if len(output) >= resolved_spec.unique_pair_target:
            break
    if len(output) < resolved_spec.unique_pair_target:
        raise PortfolioBookError(
            "bounded generator could not produce 20,000 distinct book pairs"
        )
    novel_count = sum(row.behaviorally_novel for row in output)
    if novel_count / len(output) < resolved_spec.minimum_behaviorally_novel_fraction:
        raise PortfolioBookError("generated behavioral novelty is below the frozen floor")
    population_hash = stable_hash(
        {
            "generator_manifest_hash": resolved_spec.to_manifest()["manifest_hash"],
            "structural_fingerprints": [row.structural_fingerprint for row in output],
            "behavioral_fingerprints": [row.behavioral_fingerprint for row in output],
        }
    )
    result = PortfolioBookGenerationResult(
        spec=resolved_spec,
        pairs=tuple(output),
        attempted_proposals=output[-1].proposal_index + 1,
        structural_duplicate_rejections=structural_duplicates,
        behavioral_duplicate_rejections=behavioral_duplicates,
        excluded_structural_rejections=excluded,
        behaviorally_novel_count=novel_count,
        population_hash=population_hash,
    )
    for pair in result.pairs:
        pair.verify_immutable_sources(by_id)
    return result


def _bounded_units(
    rng: random.Random,
    size: int,
    levels: Sequence[int],
    maximum: int,
) -> tuple[int, ...]:
    values = [int(rng.choice(levels)) for _ in range(size)]
    while sum(values) > maximum:
        largest = max(values)
        index = values.index(largest)
        position = levels.index(largest)
        if position == 0:
            raise PortfolioBookError("allocation cap is incompatible with book size")
        values[index] = int(levels[position - 1])
    return tuple(values)


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    row = value.get(key)
    if not isinstance(row, Mapping):
        raise PortfolioBookError(f"missing portfolio manifest object: {key}")
    return row


__all__ = [
    "BOOK_GENERATION_RESULT_SCHEMA",
    "BOOK_GENERATOR_MANIFEST_SCHEMA",
    "BOOK_PAIR_SCHEMA",
    "COMBINE_ALLOCATION_UNITS",
    "CONFLICT_POLICIES",
    "GENERATOR_VERSION",
    "MINIMUM_BEHAVIORALLY_NOVEL_FRACTION",
    "MINIMUM_BOOK_PAIR_TARGET",
    "PortfolioBookError",
    "PortfolioBookGenerationResult",
    "PortfolioBookGeneratorSpec",
    "SLEEVE_RECORD_SCHEMA",
    "STATIC_RISK_FRONTIER",
    "SleeveRecord",
    "BookPair",
    "XFA_ALLOCATION_UNITS",
    "generate_portfolio_book_pairs",
    "stable_hash",
]
