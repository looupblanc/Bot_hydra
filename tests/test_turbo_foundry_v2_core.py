from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.qd_economic_tournament import FEATURES, MARKET_PAIRS
from hydra.research.turbo_feature_builder import CONTEXT_MINUTES, HORIZONS
from hydra.research.turbo_foundry_v2 import (
    CONTEXTS,
    TurboFoundryError,
    _population_coverage,
    _quality_diversity_cap,
    _stage1_event_matrix,
    generate_turbo_population,
)
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategySpec


def _matrix(market: str, rows: int = 2_000) -> FeatureMatrix:
    rng = np.random.default_rng(sum(map(ord, market)))
    days = np.datetime64("2023-01-02", "D").astype(np.int64) + np.arange(rows) // 20
    decisions = (
        np.datetime64("2023-01-02T14:30", "ns").astype(np.int64)
        + np.arange(rows, dtype=np.int64) * 60_000_000_000
    )
    arrays: dict[str, np.ndarray] = {
        "timestamp_ns": decisions - 60_000_000_000,
        "decision_ns": decisions,
        "availability_ns": decisions,
        "segment_code": np.arange(rows) // 20,
        "session_day": days.astype(np.int32),
        "session_code": (np.arange(rows) % 3).astype(np.int16),
        "contract_code": np.zeros(rows, dtype=np.int16),
        "entry_price": np.full(rows, 100.0),
    }
    for feature in FEATURES:
        arrays[f"feature__{feature}"] = rng.normal(size=rows)
    for minutes in CONTEXT_MINUTES:
        arrays[f"feature__ctx_{minutes}m_return"] = rng.normal(size=rows)
        arrays[f"feature__ctx_{minutes}m_volatility_expansion"] = rng.integers(
            0, 2, size=rows
        ).astype(float)
    for horizon in HORIZONS:
        arrays[f"forward_move__{horizon}"] = rng.normal(size=rows)
    return FeatureMatrix(
        root=Path("."),
        manifest={"row_count": rows, "bundle_hash": market},
        arrays=arrays,
    )


def test_turbo_population_is_deterministic_unique_and_respects_caps():
    matrices = {market: _matrix(market) for market in MARKET_PAIRS}
    left = generate_turbo_population(
        matrices, count=500, batch_index=0, random_seed=77
    )
    right = generate_turbo_population(
        matrices, count=500, batch_index=0, random_seed=77
    )
    assert [row.candidate_id for row in left] == [row.candidate_id for row in right]
    assert len({row.candidate_id for row in left}) == 500
    coverage = _population_coverage(left)
    assert coverage["maximum_family_share"] <= 0.25
    assert set(coverage["mechanism_families"].values()) == {100}
    assert coverage["maximum_ecology_share"] <= 0.40
    assert coverage["maximum_lineage_share"] <= 0.02
    assert set(coverage["market_ecologies"]) == {"equity_indices", "metals", "energy"}


def test_stage1_matrix_contains_only_closed_development_rows():
    matrix = _matrix("ES", rows=8_000)
    stage1 = _stage1_event_matrix(matrix)
    assert stage1.event_count > 0
    assert (stage1.availability_ns <= stage1.decision_ns).all()
    assert set(np.unique(stage1.session_codes)) <= {0, 1, 2}


def test_expanded_mtf_grammar_covers_direction_and_volatility_states():
    context_states = {
        (feature, operator, timeframe)
        for feature, operator, _threshold, timeframe in CONTEXTS
        if feature is not None
    }
    for minutes in (5, 15, 30, 60):
        assert (
            f"ctx_{minutes}m_return",
            ComparisonOperator.GREATER_THAN,
            f"1m|{minutes}m",
        ) in context_states
        assert (
            f"ctx_{minutes}m_return",
            ComparisonOperator.LESS_THAN,
            f"1m|{minutes}m",
        ) in context_states
        assert (
            f"ctx_{minutes}m_volatility_expansion",
            ComparisonOperator.GREATER_EQUAL,
            f"1m|{minutes}m",
        ) in context_states
        assert (
            f"ctx_{minutes}m_volatility_expansion",
            ComparisonOperator.LESS_THAN,
            f"1m|{minutes}m",
        ) in context_states


def test_missing_metal_ecology_redistributes_quota_before_stage1():
    matrices = {
        market: _matrix(market)
        for market in MARKET_PAIRS
        if market != "GC"
    }
    population = generate_turbo_population(
        matrices, count=400, batch_index=0, random_seed=78
    )
    coverage = _population_coverage(population)
    assert len(population) == 400
    assert coverage["ecology_cap_relaxed_for_feasibility"]
    assert coverage["missing_ecologies"] == ["metals"]
    assert coverage["market_ecologies"] == {
        "energy": 200,
        "equity_indices": 200,
    }


def test_sparse_ecology_family_intersection_uses_feasible_strict_allocation():
    specs: list[StrategySpec] = []
    inventory = {
        "ES": {family: 40 for family in ("a", "b", "c", "d", "e")},
        "CL": {"a": 40, "b": 40, "c": 5, "d": 5, "e": 5},
    }
    # This ordering reproduces the old failure: the one-pass selector spent the
    # global a/b family caps in equities before it could fill the energy quota.
    ordered_cells = [
        ("ES", "a"),
        ("ES", "b"),
        ("CL", "a"),
        ("CL", "b"),
        ("CL", "c"),
        ("CL", "d"),
        ("CL", "e"),
        ("ES", "c"),
        ("ES", "d"),
        ("ES", "e"),
    ]
    for market, family in ordered_cells:
        for index in range(inventory[market][family]):
            specs.append(
                StrategySpec(
                    candidate_id=f"{market}_{family}_{index}",
                    lineage_id=f"lineage_{market}_{family}_{index}",
                    family=family,
                    market=market,
                    timeframe="1m",
                    feature="past_return_60",
                    operator=ComparisonOperator.GREATER_EQUAL,
                    threshold=float(index),
                    side=1,
                    holding_events=5,
                    point_value=1.0,
                    round_turn_cost=1.0,
                )
            )

    first = _quality_diversity_cap(specs, count=100)
    second = _quality_diversity_cap(specs, count=100)
    coverage = _population_coverage(first)

    assert [spec.candidate_id for spec in first] == [
        spec.candidate_id for spec in second
    ]
    assert len(first) == 100
    assert coverage["market_ecologies"] == {
        "energy": 50,
        "equity_indices": 50,
    }
    assert coverage["maximum_family_share"] <= 0.25
    assert coverage["maximum_lineage_share"] <= 0.02


def test_infeasible_ecology_quota_relaxes_without_relaxing_family_or_lineage():
    specs: list[StrategySpec] = []
    for market, per_family in (("CL", 6), ("ES", 40)):
        for family in ("a", "b", "c", "d", "e"):
            for index in range(per_family):
                specs.append(
                    StrategySpec(
                        candidate_id=f"relaxed_{market}_{family}_{index}",
                        lineage_id=f"relaxed_lineage_{market}_{family}_{index}",
                        family=family,
                        market=market,
                        timeframe="1m",
                        feature="past_return_60",
                        operator=ComparisonOperator.GREATER_EQUAL,
                        threshold=float(index),
                        side=1,
                        holding_events=5,
                        point_value=1.0,
                        round_turn_cost=1.0,
                    )
                )

    selected = _quality_diversity_cap(specs, count=100)
    coverage = _population_coverage(selected)

    assert len(selected) == 100
    assert coverage["market_ecologies"] == {
        "energy": 30,
        "equity_indices": 70,
    }
    assert coverage["ecology_cap_relaxed_for_feasibility"]
    assert coverage["maximum_family_share"] <= 0.25
    assert coverage["maximum_lineage_share"] <= 0.02


def test_family_capacity_is_not_silently_relaxed():
    specs = [
        StrategySpec(
            candidate_id=f"insufficient_{index}",
            lineage_id=f"insufficient_lineage_{index}",
            family=("a", "b", "c")[index % 3],
            market="ES",
            timeframe="1m",
            feature="past_return_60",
            operator=ComparisonOperator.GREATER_EQUAL,
            threshold=float(index),
            side=1,
            holding_events=5,
            point_value=1.0,
            round_turn_cost=1.0,
        )
        for index in range(120)
    ]

    with pytest.raises(TurboFoundryError, match="family/lineage caps permit only 75"):
        _quality_diversity_cap(specs, count=100)


def test_lineage_cannot_span_multiple_ecology_family_cells():
    shared = {
        "lineage_id": "shared_lineage",
        "timeframe": "1m",
        "feature": "past_return_60",
        "operator": ComparisonOperator.GREATER_EQUAL,
        "threshold": 0.0,
        "side": 1,
        "holding_events": 5,
        "point_value": 1.0,
        "round_turn_cost": 1.0,
    }
    specs = [
        StrategySpec(
            candidate_id="candidate_equity",
            family="family_a",
            market="ES",
            **shared,
        ),
        StrategySpec(
            candidate_id="candidate_energy",
            family="family_b",
            market="CL",
            **shared,
        ),
    ]

    with pytest.raises(TurboFoundryError, match="Lineage spans multiple"):
        _quality_diversity_cap(specs, count=2)
