from __future__ import annotations

import pandas as pd
import pytest

from hydra.atoms.adversarial_validator import MANDATORY_ATTACKS, adversarial_validate_atom
from hydra.atoms.hypothesis_generator import generate_edge_atom_hypotheses
from hydra.atoms.registry import tombstone_blocks_id
from hydra.atoms.replication_engine import replicate_atom
from hydra.atoms.schema import AtomTestResult, EdgeAtomHypothesis
from hydra.portfolio.account_utility import expected_account_utility
from hydra.strategy.sparse_assembler import assemble_sparse_strategies
from hydra.validation.evidence_scope import ComputationMode, EvidenceScope, can_promote_scope
from hydra.validation.lockbox_guard import LockboxViolation
from hydra.validation.promotion_contract import evidence_can_support_scope
from hydra.validation.status_provenance import assert_not_stale, make_status_provenance
from scripts.run_edge_atom_discovery_lab import guard_no_q4_period


def test_component_evidence_cannot_promote_to_atom_scope() -> None:
    provenance = make_status_provenance(
        status="COMPONENT_PASS",
        scope=EvidenceScope.COMPONENT,
        payload={"component": "x"},
        code_commit="abc",
        data_fingerprint="data",
        validation_version="v1",
        policy_version="p1",
        computation_mode=ComputationMode.FULL,
        evidence_strength=1.0,
        passed=True,
    )
    assert not evidence_can_support_scope(provenance, EvidenceScope.EDGE_ATOM)
    assert not can_promote_scope(EvidenceScope.COMPONENT, EvidenceScope.EDGE_ATOM, newly_executed_validation=False)


def test_unknown_or_proxy_evidence_is_never_passed() -> None:
    provenance = make_status_provenance(
        status="ATOM_VALIDATED",
        scope=EvidenceScope.EDGE_ATOM,
        payload={"atom": "a"},
        code_commit="abc",
        data_fingerprint="data",
        validation_version="v1",
        policy_version="p1",
        computation_mode=ComputationMode.PROXY,
        evidence_strength=10.0,
        passed=True,
    )
    assert provenance.passed is False


def test_stale_validation_is_rejected() -> None:
    provenance = make_status_provenance(
        status="ATOM_VALIDATED",
        scope=EvidenceScope.EDGE_ATOM,
        payload={"atom": "a"},
        code_commit="abc",
        data_fingerprint="data",
        validation_version="v1",
        policy_version="p1",
        computation_mode=ComputationMode.FULL,
        evidence_strength=10.0,
        passed=True,
    )
    with pytest.raises(ValueError):
        assert_not_stale(provenance, current_input_hash="different", current_validation_version="v1")


def test_preregistered_atoms_respect_family_caps_and_cross_market_targets() -> None:
    atoms = generate_edge_atom_hypotheses(
        markets=["ES", "NQ", "RTY", "GC"],
        code_commit="abc",
        max_atoms=40,
        max_family_share=0.20,
        max_variants=4,
        seed=1,
    )
    assert len(atoms) == 40
    by_family = {}
    for atom in atoms:
        by_family[atom.family] = by_family.get(atom.family, 0) + 1
        assert atom.authoring_mode == "PREREGISTERED_BEFORE_TEST"
    assert max(by_family.values()) <= 8
    cross_market = [atom for atom in atoms if atom.family == "cross_market_risk_transfer"]
    assert cross_market
    assert all(len(atom.target_markets) >= 2 for atom in cross_market)


def test_tombstone_blocks_renamed_formulation_identifier() -> None:
    tombstones = [{"blocked_ids": ["candidate_a"], "blocked_formulations": ["overnight_inventory_rth_resolution"]}]
    assert tombstone_blocks_id(tombstones, "candidate_a")
    assert tombstone_blocks_id(tombstones, "overnight_inventory_rth_resolution")
    assert not tombstone_blocks_id(tombstones, "fresh_atom_hypothesis")


def test_q4_access_guard_blocks_holdout_boundary() -> None:
    guard_no_q4_period("2024-07-01", "2024-10-01")
    with pytest.raises(LockboxViolation):
        guard_no_q4_period("2024-09-01", "2024-10-02")


def test_adversarial_validator_reports_candidate_level_attacks() -> None:
    frame = _synthetic_frame()
    atom = _atom(feature_key="feature", markets=("ES",))
    result = adversarial_validate_atom(atom, frame, seed=11)
    assert set(result.attacks_attempted) == set(MANDATORY_ATTACKS)
    assert "real_effect" in result.details
    assert result.details["event_count"] >= 50


def test_replication_requires_temporal_and_contract_support() -> None:
    atom = _atom(feature_key="feature", markets=("ES",))
    result = AtomTestResult(
        atom_id=atom.atom_id,
        family=atom.family,
        status="ATOM_VALID",
        valid_observations=100,
        state_frequency=0.5,
        raw_effect=0.001,
        cost_hurdle=0.0001,
        effect_after_cost_hurdle=0.0009,
        confidence_low=0.0001,
        confidence_high=0.002,
        direction_ok=True,
        folds_positive=1,
        fold_count=3,
        markets_positive=1,
        market_count=1,
        contracts_positive=1,
        contract_count=3,
        top_event_concentration=0.1,
        evidence_strength=2.0,
        fdr_adjusted_evidence=1.0,
        simplest_competing_explanation="none",
        failure_reason=None,
        provenance={"passed": True},
        fold_results={},
        market_results={},
        contract_results={},
        adversarial={},
    )
    replication = replicate_atom(atom, result)
    assert not replication.temporal_pass
    assert not replication.contract_pass


def test_sparse_assembler_accepts_only_fully_validated_atoms() -> None:
    atom = _atom(feature_key="feature", markets=("ES",))
    nonvalidated = AtomTestResult(
        atom.atom_id,
        atom.family,
        "ATOM_VALID",
        100,
        0.5,
        0.001,
        0.0001,
        0.0009,
        0.0,
        0.1,
        True,
        3,
        3,
        1,
        1,
        2,
        2,
        0.1,
        2.0,
        1.0,
        "none",
        None,
        {"passed": True},
        {},
        {},
        {},
        {},
    )
    strategies, decisions = assemble_sparse_strategies([atom], {atom.atom_id: nonvalidated}, max_strategies=5)
    assert strategies == []
    assert decisions[-1].reason == "no_fully_validated_atoms_available_for_assembly"


def test_account_utility_is_probability_and_penalty_based_not_payout_sum() -> None:
    utility = expected_account_utility(
        strategy_id="s1",
        combine_pass_probability=0.5,
        mll_survival_probability=0.8,
        consistency_probability=0.7,
        xfa_survival_probability=0.6,
        first_payout_probability=0.4,
        repeat_payout_probability=0.2,
        shared_loss_day_penalty=0.1,
        tail_overlap_penalty=0.1,
        execution_cost_penalty=0.1,
        operational_complexity_penalty=0.1,
    )
    assert -1.0 <= utility.expected_utility <= 1.0
    assert not hasattr(utility, "standalone_payout_sum")


def _synthetic_frame() -> pd.DataFrame:
    timestamps = pd.date_range("2024-01-01", periods=240, freq="1min", tz="UTC")
    close = pd.Series(range(240), dtype=float) * 0.25 + 5000.0
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": "ES",
            "open": close,
            "high": close + 0.25,
            "low": close - 0.25,
            "close": close,
            "volume": 100,
            "feature": pd.Series(range(240), dtype=float),
        }
    )


def _atom(feature_key: str, markets: tuple[str, ...]) -> EdgeAtomHypothesis:
    return EdgeAtomHypothesis(
        atom_id="atom_test_v1",
        family="session_inventory_acceptance",
        feature_key=feature_key,
        economic_mechanism="test mechanism",
        participants="test participants",
        information_set="past-only test data",
        target_variable="future_return",
        expected_direction=1,
        horizon_bars=5,
        target_markets=markets,
        favorable_regimes="test",
        failure_regimes="test",
        transaction_cost_hurdle=0.0,
        roll_sensitivity="none",
        minimum_effect=0.0,
        primary_null="matched_random",
        mandatory_nulls=MANDATORY_ATTACKS,
        replication_requirement="test",
        falsification_rule="test",
        max_parameter_degrees=1,
        timestamp_utc="2026-07-10T00:00:00+00:00",
        code_commit="abc",
        parameters={"threshold": "low"},
    )
