from __future__ import annotations

from pathlib import Path

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.production.cross_index_breadth_tripwire import (
    FAMILIES,
    INDEX_MARKETS,
    build_cross_index_breadth_view,
    decide_tripwire_gate,
    frozen_contract,
    frozen_specifications,
)


MINUTE_NS = 60_000_000_000


def _matrix(
    market: str,
    values: np.ndarray,
    *,
    availability_offset_minutes: int = 1,
    session_day: np.ndarray | None = None,
    session_code: np.ndarray | None = None,
) -> FeatureMatrix:
    count = len(values)
    timestamps = np.arange(count, dtype=np.int64) * MINUTE_NS
    days = (
        np.full(count, 19_541, dtype=np.int64)
        if session_day is None
        else np.asarray(session_day, dtype=np.int64)
    )
    sessions = (
        np.zeros(count, dtype=np.int8)
        if session_code is None
        else np.asarray(session_code, dtype=np.int8)
    )
    availability = timestamps + availability_offset_minutes * MINUTE_NS
    arrays = {
        "timestamp_ns": timestamps,
        "decision_ns": timestamps + MINUTE_NS,
        "availability_ns": availability,
        "session_day": days,
        "session_code": sessions,
        "segment_code": np.ones(count, dtype=np.int64),
        "contract_code": np.ones(count, dtype=np.int64),
        "feature__ctx_15m_return": np.asarray(values, dtype=float),
        "feature__past_return_60": np.asarray(values, dtype=float),
    }
    for value in arrays.values():
        value.flags.writeable = False
    return FeatureMatrix(
        root=Path("/synthetic") / market,
        manifest={
            "row_count": count,
            "bundle_hash": f"synthetic-{market}",
            "key": {"market": market},
        },
        arrays=arrays,
    )


def test_frozen_inventory_is_exactly_sixteen_material_specs() -> None:
    rows = frozen_specifications()

    assert len(rows) == 16
    assert len({row.fingerprint for row in rows}) == 16
    assert {row.target_market for row in rows} == set(INDEX_MARKETS)
    assert {row.family for row in rows} == set(FAMILIES)
    assert all(row.target_market not in row.reference_markets for row in rows)
    assert all(len(row.reference_markets) == 3 for row in rows)
    assert all("catchup" not in row.family.lower() for row in rows)

    contract = frozen_contract()
    assert contract["threshold_grid"] is False
    assert contract["risk_grid"] is False
    assert contract["promotion_authorized"] is False
    assert contract["xfa_authorized"] is False
    assert contract["account_label"] == "50K"
    assert contract["design_blocks"] == ["B1", "B2"]
    assert contract["validation_blocks"] == ["B3"]
    assert contract["final_development_blocks"] == ["B4"]


def test_breadth_join_is_asof_same_session_and_target_excluded() -> None:
    count = 45
    target = np.ones(count, dtype=float)
    nq = np.full(count, 0.2, dtype=float)
    rty = np.full(count, 0.3, dtype=float)
    ym = np.full(count, 0.4, dtype=float)

    # Boundary 14 is not unanimous, boundary 29 enters unanimous positive,
    # boundary 44 flips unanimously negative while the target stays positive.
    nq[:15] = -0.2
    nq[30:] = -0.2
    rty[30:] = -0.3
    ym[30:] = -0.4

    primary = _matrix("ES", target)
    references = {
        "NQ": _matrix("NQ", nq),
        "RTY": _matrix("RTY", rty),
        "YM": _matrix("YM", ym),
    }
    view = build_cross_index_breadth_view(primary, references)

    transition = view.array("feature__cross_index_breadth_transition")
    failed = view.array("feature__cross_index_failed_progress_transition")
    assert not np.isfinite(transition[14])
    assert transition[29] > 0.0
    assert transition[44] < 0.0
    assert not np.isfinite(failed[29])
    assert failed[44] < 0.0
    assert view.manifest["cross_index_breadth_receipt"]["target_excluded"] is True
    assert set(
        view.manifest["cross_index_breadth_receipt"]["reference_matrix_hashes"]
    ) == {"NQ", "RTY", "YM"}

    # No reference availability is later than the effective input timestamp.
    assert np.all(view.array("availability_ns") <= view.array("decision_ns"))


def test_breadth_join_rejects_cross_session_reference_carry() -> None:
    count = 30
    target = _matrix("ES", np.ones(count, dtype=float))
    wrong_day = np.full(count, 19_540, dtype=np.int64)
    references = {
        "NQ": _matrix("NQ", np.ones(count), session_day=wrong_day),
        "RTY": _matrix("RTY", np.ones(count)),
        "YM": _matrix("YM", np.ones(count)),
    }

    view = build_cross_index_breadth_view(target, references)

    assert not np.any(
        np.isfinite(view.array("feature__cross_index_breadth_consensus"))
    )
    assert not np.any(
        np.isfinite(view.array("feature__cross_index_breadth_transition"))
    )


def _gate_result(
    specification_id: str,
    *,
    target: str,
    family: str,
    behavior_hash: str,
    pass_count: int = 2,
) -> dict:
    scenario = {
        "pass_count": pass_count,
        "pass_rate": 0.25 if pass_count else 0.0,
        "blocks_with_passes": ["B3", "B4"] if pass_count else [],
        "net_total_usd": 2_500.0,
        "mll_breach_rate": 0.0,
        "all_passing_paths_consistency_compliant": bool(pass_count),
    }
    return {
        "specification": {
            "specification_id": specification_id,
            "target_market": target,
            "family": family,
            "session_role": "OPEN",
        },
        "primary": {
            "realized_behavior_hash": behavior_hash,
            "cells": [
                {
                    "horizon_trading_days": 10,
                    "normal": dict(scenario),
                    "stressed": dict(scenario),
                }
            ],
        },
        "control_deltas": {"10": {"controls_beaten": 2}},
    }


def test_gate_is_green_only_for_distinct_families_and_non_nq_ym_target() -> None:
    rows = [
        _gate_result(
            "es-cont",
            target="ES",
            family="BREADTH_CONFIRMED_CONTINUATION",
            behavior_hash="behavior-a",
        ),
        _gate_result(
            "nq-rev",
            target="NQ",
            family="BREADTH_FAILED_PROGRESS_REVERSAL",
            behavior_hash="behavior-b",
        ),
    ]

    decision = decide_tripwire_gate(rows)

    assert decision["status"] == "CROSS_INDEX_BREADTH_TRIPWIRE_GREEN_DEVELOPMENT_ONLY"
    assert decision["authoritative_promotion_count"] == 0
    assert decision["independent_confirmation_claimed"] is False
    assert decision["xfa_paths_started"] == 0

    nq_ym_only = [dict(rows[0]), dict(rows[1])]
    nq_ym_only[0] = _gate_result(
        "ym-cont",
        target="YM",
        family="BREADTH_CONFIRMED_CONTINUATION",
        behavior_hash="behavior-a",
    )
    assert decide_tripwire_gate(nq_ym_only)["green"] is False


def test_gate_fails_closed_without_same_cell_normal_and_stressed_passes() -> None:
    row = _gate_result(
        "es-cont",
        target="ES",
        family="BREADTH_CONFIRMED_CONTINUATION",
        behavior_hash="behavior-a",
        pass_count=0,
    )

    decision = decide_tripwire_gate([row])

    assert decision["green"] is False
    assert decision["authoritative_promotion_count"] == 0
    assert decision["xfa_paths_started"] == 0
