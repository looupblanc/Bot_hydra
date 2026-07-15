from __future__ import annotations

import copy
import hashlib
from dataclasses import FrozenInstanceError, replace

import pytest

from hydra.production.portfolio_books import (
    COMBINE_ALLOCATION_UNITS,
    CONFLICT_POLICIES,
    MINIMUM_BOOK_PAIR_TARGET,
    STATIC_RISK_FRONTIER,
    XFA_ALLOCATION_UNITS,
    BookPair,
    PortfolioBookError,
    PortfolioBookGenerationResult,
    PortfolioBookGeneratorSpec,
    SleeveRecord,
    generate_portfolio_book_pairs,
    stable_hash,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sleeve(index: int, *, prefix: str = "sleeve") -> SleeveRecord:
    sleeve_id = f"{prefix}-{index:02d}"
    return SleeveRecord(
        sleeve_id=sleeve_id,
        immutable_fingerprint=_sha(f"immutable:{sleeve_id}"),
        behavioral_fingerprint=_sha(f"behavior:{index}"),
        signal_ledger_sha256=_sha(f"signal-ledger:{sleeve_id}"),
        trade_ledger_sha256=_sha(f"trade-ledger:{sleeve_id}"),
        market=("ES", "NQ", "CL", "GC")[index % 4],
        contract=("MES", "MNQ", "MCL", "MGC")[index % 4],
        timeframe=("5m", "15m", "30m")[index % 3],
        session=("OPEN", "MID", "CLOSE", "OVERNIGHT")[index % 4],
        economic_role=("VELOCITY", "DEFENSIVE", "DIVERSIFIER")[index % 3],
        source_campaign=f"campaign-{18 + index % 4:04d}",
        family_id=f"family-{index % 5}",
    )


@pytest.fixture(scope="module")
def sleeve_bank() -> tuple[SleeveRecord, ...]:
    return tuple(_sleeve(index) for index in range(10))


@pytest.fixture(scope="module")
def generated_population(
    sleeve_bank: tuple[SleeveRecord, ...],
) -> PortfolioBookGenerationResult:
    return generate_portfolio_book_pairs(
        sleeve_bank,
        PortfolioBookGeneratorSpec(seed=25_000_401),
    )


def _rehash_manifest(value: dict) -> None:
    value.pop("manifest_hash", None)
    value["manifest_hash"] = stable_hash(value)


def test_sleeve_and_six_member_books_are_immutable_and_checksum_bound(
    sleeve_bank: tuple[SleeveRecord, ...],
) -> None:
    pair = BookPair.create(
        combine_sleeves=sleeve_bank[:6],
        combine_allocation_units=(1, 2, 1, 2, 3, 1),
        xfa_sleeves=sleeve_bank[4:10],
        xfa_allocation_units=(1, 1, 2, 1, 1, 2),
        conflict_policy="PRIORITY",
        behaviorally_novel=True,
        generator_seed=250_001,
        proposal_index=0,
    )

    assert len(pair.combine_sleeve_ids) == 6
    assert len(pair.xfa_sleeve_ids) == 6
    assert pair.signals_mutated is False
    assert pair.entries_or_exits_mutated is False
    assert pair.status_inherited is False
    assert pair.broker_connections == pair.orders == 0
    assert pair.verify_immutable_sources({row.sleeve_id: row for row in sleeve_bank})
    assert BookPair.from_mapping(pair.to_dict()) == pair
    assert SleeveRecord.from_mapping(sleeve_bank[0].to_dict()) == sleeve_bank[0]

    with pytest.raises(FrozenInstanceError):
        pair.conflict_policy = "REJECT_BOTH"  # type: ignore[misc]

    changed = replace(
        sleeve_bank[0],
        signal_ledger_sha256=_sha("a-mutated-signal-ledger"),
    )
    changed_bank = {row.sleeve_id: row for row in sleeve_bank}
    changed_bank[changed.sleeve_id] = changed
    with pytest.raises(PortfolioBookError, match="immutable sleeve binding drift"):
        pair.verify_immutable_sources(changed_bank)


def test_manifest_round_trip_freezes_sizes_allocations_conflicts_and_signals() -> None:
    spec = PortfolioBookGeneratorSpec(seed=250_002)
    manifest = spec.to_manifest()

    assert PortfolioBookGeneratorSpec.from_manifest(manifest) == spec
    assert manifest["combine_book"]["sleeve_maximum"] == 6
    assert manifest["xfa_book"]["sleeve_maximum"] == 6
    assert tuple(manifest["risk_frontier"]) == STATIC_RISK_FRONTIER
    assert tuple(manifest["conflict_policies"]) == CONFLICT_POLICIES

    priority_only = PortfolioBookGeneratorSpec(
        seed=250_002,
        conflict_policies=("PRIORITY",),
    )
    assert PortfolioBookGeneratorSpec.from_manifest(
        priority_only.to_manifest()
    ).conflict_policies == ("PRIORITY",)

    capped_at_five = copy.deepcopy(manifest)
    capped_at_five["combine_book"]["sleeve_maximum"] = 5
    _rehash_manifest(capped_at_five)
    with pytest.raises(PortfolioBookError, match="must remain 2-6"):
        PortfolioBookGeneratorSpec.from_manifest(capped_at_five)

    mutating = copy.deepcopy(manifest)
    mutating["governance"]["underlying_signal_mutation_allowed"] = True
    _rehash_manifest(mutating)
    with pytest.raises(PortfolioBookError, match="unsafe portfolio-book governance"):
        PortfolioBookGeneratorSpec.from_manifest(mutating)

    with pytest.raises(PortfolioBookError, match="at least 20,000"):
        PortfolioBookGeneratorSpec(
            seed=250_003,
            unique_pair_target=MINIMUM_BOOK_PAIR_TARGET - 1,
        )
    with pytest.raises(PortfolioBookError, match="non-empty declared subset"):
        PortfolioBookGeneratorSpec(seed=250_003, conflict_policies=())
    with pytest.raises(PortfolioBookError, match="non-empty declared subset"):
        PortfolioBookGeneratorSpec(
            seed=250_003,
            conflict_policies=("PRIORITY", "UNIMPLEMENTED"),
        )
    with pytest.raises(PortfolioBookError, match="static risk frontier"):
        PortfolioBookGeneratorSpec(seed=250_003, risk_frontier=(1.0, 1.5))


def test_behavioral_fingerprint_detects_structurally_distinct_clones(
    sleeve_bank: tuple[SleeveRecord, ...],
) -> None:
    clones = tuple(
        replace(
            sleeve_bank[index],
            sleeve_id=f"clone-{index:02d}",
            immutable_fingerprint=_sha(f"clone-immutable:{index}"),
            signal_ledger_sha256=_sha(f"clone-signal:{index}"),
            trade_ledger_sha256=_sha(f"clone-trade:{index}"),
        )
        for index in range(3)
    )
    original = BookPair.create(
        combine_sleeves=sleeve_bank[:2],
        combine_allocation_units=(2, 1),
        xfa_sleeves=sleeve_bank[2:3],
        xfa_allocation_units=(1,),
        conflict_policy="NET_TO_FLAT",
        behaviorally_novel=True,
        generator_seed=250_004,
        proposal_index=0,
    )
    clone = BookPair.create(
        combine_sleeves=clones[:2],
        combine_allocation_units=(2, 1),
        xfa_sleeves=clones[2:3],
        xfa_allocation_units=(1,),
        conflict_policy="NET_TO_FLAT",
        behaviorally_novel=False,
        generator_seed=250_004,
        proposal_index=1,
    )

    assert original.structural_fingerprint != clone.structural_fingerprint
    assert original.behavioral_fingerprint == clone.behavioral_fingerprint


def test_generator_produces_20k_unique_deterministic_signal_immutable_pairs(
    sleeve_bank: tuple[SleeveRecord, ...],
    generated_population: PortfolioBookGenerationResult,
) -> None:
    before = tuple(row.to_dict() for row in sleeve_bank)
    first = generated_population
    first_behavior = first.pairs[0].behavioral_fingerprint
    second = generate_portfolio_book_pairs(
        sleeve_bank,
        PortfolioBookGeneratorSpec(
            seed=25_000_401,
            reference_book_behavioral_fingerprints=(first_behavior,),
        ),
    )

    assert len(first.pairs) == len(second.pairs) == MINIMUM_BOOK_PAIR_TARGET
    assert [row.pair_id for row in first.pairs] == [
        row.pair_id for row in second.pairs
    ]
    assert len({row.structural_fingerprint for row in first.pairs}) == 20_000
    assert len({row.behavioral_fingerprint for row in first.pairs}) == 20_000
    assert second.pairs[0].behaviorally_novel is False
    assert second.behaviorally_novel_fraction >= 0.20
    assert {row.conflict_policy for row in first.pairs} == set(CONFLICT_POLICIES)
    assert {len(row.combine_sleeve_ids) for row in first.pairs} == set(range(2, 7))
    assert {len(row.xfa_sleeve_ids) for row in first.pairs} == set(range(1, 7))
    assert {row.combine_risk_tier for row in first.pairs} == set(
        STATIC_RISK_FRONTIER
    )
    assert {row.xfa_risk_tier for row in first.pairs} == set(STATIC_RISK_FRONTIER)
    assert all(
        set(row.combine_allocation_units) <= set(COMBINE_ALLOCATION_UNITS)
        and set(row.xfa_allocation_units) <= set(XFA_ALLOCATION_UNITS)
        and sum(row.combine_allocation_units) <= 12
        and sum(row.xfa_allocation_units) <= 8
        and not row.signals_mutated
        and not row.entries_or_exits_mutated
        for row in first.pairs
    )
    assert tuple(row.to_dict() for row in sleeve_bank) == before
    assert first.to_dict()["underlying_signal_mutation_count"] == 0
