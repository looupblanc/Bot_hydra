from __future__ import annotations

from pathlib import Path

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.promotion.behavioral_clustering import cluster_candidates
from hydra.promotion.evidence_conversion import (
    _complete_candidate_validation,
    _finalize_decision,
    _period_positions,
)
from hydra.promotion.evidence_debt import build_evidence_debt_record
from hydra.promotion.evidence_gap import evidence_gaps


def _candidate(candidate_id: str, *, role: str = "COMBINE_PASSER") -> dict:
    return {
        "candidate_id": candidate_id,
        "lineage_id": f"lineage_{candidate_id}",
        "mechanism_family": "invariant_price_state",
        "primary_market": "ES",
        "execution_market": "MES",
        "timeframe": "1m|15m",
        "role": role,
        "status": "SHADOW_RESEARCH_CANDIDATE",
        "events": 20,
        "net_pnl": 5_000.0,
        "maximum_drawdown": 500.0,
        "cost_stress_1_5x_net": 4_500.0,
        "one_bar_delay_net_pnl": 4_000.0,
        "supportive_temporal_folds": 3,
        "candidate_null": {"raw_probability": 0.01, "family_adjusted_probability": 0.04},
        "contract_transfer": {"passed": True},
        "delay_resilience": {"passed": True},
        "parameter_neighborhood": {"passed": True},
        "topstep": {
            "selected_micro_contracts": 1,
            "path_candidate": role == "COMBINE_PASSER",
            "combine": {
                "min_mll_buffer": 4_000.0,
                "best_day_pct_of_total_profit": 0.30,
            },
            "xfa_standard": {"payout_cycles_survived": 2, "survived": True},
            "xfa_consistency": {"payout_cycles_survived": 1, "survived": True},
        },
        "specification": {
            "candidate_id": candidate_id,
            "lineage_id": f"lineage_{candidate_id}",
            "family": "invariant_price_state",
            "market": "ES",
            "timeframe": "1m|15m",
            "feature": "past_return_60",
            "operator": 2,
            "threshold": 0.0,
            "side": 1,
            "holding_events": 5,
            "point_value": 50.0,
            "round_turn_cost": 14.5,
            "role": 2 if role == "COMBINE_PASSER" else 3,
            "context_feature": "ctx_15m_return",
            "context_operator": 1,
            "context_threshold": 0.0,
            "session_code": 0,
            "quantity": 1,
            "version": 1,
        },
    }


def _exact(pnl: list[float], days: list[int]) -> dict:
    return {
        "event_net_pnl": pnl,
        "event_gross_pnl": [value + 14.5 for value in pnl],
        "event_session_days": days,
        "event_timestamp_ns": [int(day) * 86_400_000_000_000 for day in days],
        "behavioral_replay_complete": True,
        "events": len(pnl),
        "finite": True,
    }


def test_evidence_debt_is_deterministic_transparent_and_role_specific() -> None:
    candidate = _candidate("candidate_a")
    exact = _exact([100.0] * 20, list(range(19_400, 19_420)))
    left = build_evidence_debt_record(
        candidate, exact, cluster_id="cluster_a", cluster_size=1
    ).to_dict()
    right = build_evidence_debt_record(
        candidate, exact, cluster_id="cluster_a", cluster_size=1
    ).to_dict()

    assert left == right
    assert left["evidence_conversion_priority"] > 0.0
    assert left["priority_components"]["expected_account_utility"] > 0.0
    assert left["estimated_closure_cost"]["total_research_cost"] > 0.0
    missing = {row["name"] for row in left["missing_evidence"]}
    assert "intraday_unrealized_mll" in missing
    assert "sealed_q4_decision" in missing
    assert "fresh_forward_evidence" in missing
    assert "matched_account_utility_control" not in missing


def test_evidence_gaps_never_treat_future_or_holdout_as_present() -> None:
    candidate = _candidate("candidate_a")
    gaps = {gap.name: gap for gap in evidence_gaps(candidate, _exact([1.0] * 20, list(range(20))))}
    assert gaps["sealed_q4_decision"].missing
    assert gaps["fresh_forward_evidence"].missing
    assert gaps["exact_temporal_replay"].missing is False


def test_behavioral_clustering_merges_exact_clone_but_separates_tail_path() -> None:
    candidates = [_candidate("a"), _candidate("b"), _candidate("c")]
    days = list(range(100, 120))
    clone = _exact([100.0, -20.0] * 10, days)
    distinct_tail = _exact(
        [-300.0 if index in {5, 15} else 120.0 for index in range(20)], days
    )
    clusters, membership = cluster_candidates(
        candidates,
        {"a": clone, "b": clone, "c": distinct_tail},
    )

    assert membership["a"] == membership["b"]
    assert membership["c"] != membership["a"]
    assert len(clusters) == 2
    clone_cluster = next(row for row in clusters if "a" in row["member_ids"])
    assert clone_cluster["execution_equivalent"]


def _matrix(rows: int = 20_000) -> FeatureMatrix:
    fold_starts = (
        "2023-04-03",
        "2024-01-03",
        "2024-04-03",
        "2024-07-03",
    )
    block = rows // len(fold_starts)
    timestamp_parts = []
    day_parts = []
    segment_parts = []
    for fold, value in enumerate(fold_starts):
        count = block if fold < len(fold_starts) - 1 else rows - block * fold
        local = np.arange(count, dtype=np.int64)
        timestamp_parts.append(
            np.datetime64(f"{value}T13:30", "ns").astype(np.int64)
            + local * 60_000_000_000
        )
        day_parts.append(
            np.datetime64(value, "D").astype(np.int64) + local // 390
        )
        segment_parts.append(fold * 10_000 + local // 390)
    timestamp = np.concatenate(timestamp_parts)
    days = np.concatenate(day_parts).astype(np.int32)
    segments = np.concatenate(segment_parts).astype(np.int64)
    close = 4_000.0 + np.arange(rows, dtype=float) * 0.05
    forward = np.full(rows, 1.0, dtype=float)
    # A governed forward move never crosses a session, roll or contiguous
    # segment.  The synthetic folds are not exact multiples of 390 rows, so a
    # global modulo mask would leave a handful of impossible cross-fold exits
    # and would (correctly) fail the session-flatten proof.
    horizon_exit = np.arange(rows, dtype=np.int64) + 6
    valid_exit = horizon_exit < rows
    comparable = np.flatnonzero(valid_exit)
    valid_exit[comparable] &= (
        segments[comparable] == segments[horizon_exit[comparable]]
    ) & (days[comparable] == days[horizon_exit[comparable]])
    forward[~valid_exit] = np.nan
    arrays = {
        "timestamp_ns": timestamp,
        "decision_ns": timestamp + 60_000_000_000,
        "availability_ns": timestamp + 60_000_000_000,
        "segment_code": segments,
        "session_day": days,
        "session_code": np.zeros(rows, dtype=np.int16),
        "contract_code": np.zeros(rows, dtype=np.int16),
        "entry_price": close + 0.05,
        "bar_open": close - 0.02,
        "bar_high": close + 0.30,
        "bar_low": close - 0.20,
        "bar_close": close,
        "feature__past_return_60": np.ones(rows, dtype=float),
        "feature__past_volatility": np.ones(rows, dtype=float),
        "feature__ctx_15m_return": np.ones(rows, dtype=float),
        "forward_move__5": forward,
    }
    return FeatureMatrix(
        root=Path("."),
        manifest={"row_count": rows, "bundle_hash": "test"},
        arrays=arrays,
    )


def test_complete_validation_distinguishes_economic_risk_and_promotion() -> None:
    candidate = _candidate("candidate_a")
    candidate["candidate_null"]["global_adjusted_probability"] = 0.01
    matrix = _matrix()
    from hydra.research.turbo_exact_replay import spec_from_dict

    spec = spec_from_dict(candidate["specification"])
    positions = _period_positions(spec, matrix)
    assert len(positions) >= 15
    result = _complete_candidate_validation(
        candidate,
        _exact([50.0] * 20, list(range(20))),
        {"ES": matrix},
        seed=7,
    )

    assert result["stage_completion"]["full_economic_replay"]
    assert result["stage_completion"]["full_risk_replay"]
    assert result["risk"]["mll_breached"] is False
    assert result["stage_completion"]["full_promotion_validation"] is False
    assert result["q4_access_count"] == 0


def test_intraday_mll_breach_is_a_hard_promotion_failure() -> None:
    candidate = _candidate("candidate_mll")
    candidate["candidate_null"]["global_adjusted_probability"] = 0.01
    row = _complete_candidate_validation(
        candidate,
        _exact([50.0] * 20, list(range(20))),
        {"ES": _matrix()},
        seed=11,
    )
    row["risk"]["mll_breached"] = True
    row["candidate_null"]["matched_opportunity_probability"] = 0.01
    row["candidate_null"]["matched_opportunity_null"] = {
        "complete": True,
        "draws": 1024,
        "probability": 0.01,
    }
    row["account_interaction"] = {
        "complete": True,
        "behaviorally_distinct": True,
        "account_hard_risk_violation": True,
    }
    _finalize_decision(row)

    assert row["decision"] == "PROMOTION_FAILED"
    assert "catastrophic_mll_breach" in row["hard_failure_reasons"]
