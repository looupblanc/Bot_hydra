from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.intraday_cross_index_dispersion_residual import (
    CONTROL_DELAY,
    CONTROL_DIRECTION_FLIP,
    CONTROL_PRIMARY,
    CONTROL_RANK_SWAP,
    CONTROLS,
    DispersionCell,
    DispersionResidualError,
    _matched_control_quantities,
    _outcome_candidate,
    build_cell_decisions,
    build_dispersion_snapshot,
    decide_cell,
    frozen_cells,
    load_manifest,
)


MINUTE_NS = 60_000_000_000


def _matrix(market: str, values: np.ndarray, *, session_code: int = 0) -> FeatureMatrix:
    count = len(values)
    timestamps = np.arange(count, dtype=np.int64) * MINUTE_NS
    arrays = {
        "timestamp_ns": timestamps,
        "decision_ns": timestamps + MINUTE_NS,
        "availability_ns": timestamps + MINUTE_NS,
        "session_day": np.full(count, 19_541, dtype=np.int64),
        "session_code": np.full(count, session_code, dtype=np.int8),
        "segment_code": np.ones(count, dtype=np.int64),
        "contract_code": np.ones(count, dtype=np.int16),
        "feature__ctx_5m_return": np.asarray(values, dtype=float),
        "feature__ctx_15m_return": np.asarray(values, dtype=float) * 0.8,
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


def test_manifest_freezes_exactly_eight_cells_and_no_external_actions() -> None:
    manifest = load_manifest()
    cells = frozen_cells()

    assert len(cells) == 8
    assert len({row.fingerprint for row in cells}) == 8
    assert {row.mechanism for row in cells} == {
        "CONTINUATION_LEADER",
        "CONVERGENCE_LAGGARD",
    }
    assert {row.timeframe_minutes for row in cells} == {5, 15}
    assert {row.session_role for row in cells} == {"OPEN", "MID"}
    governance = manifest["governance"]
    assert governance["q4_access_allowed"] is False
    assert governance["data_purchase_allowed"] is False
    assert governance["broker_allowed"] is False
    assert governance["orders_allowed"] is False
    assert governance["maximum_cpu_workers"] == 1


def test_snapshot_is_strictly_causal_and_fails_on_session_drift() -> None:
    count = 700
    x = np.arange(count, dtype=float)
    matrices = {
        "ES": _matrix("ES", np.sin(x / 9.0) * 0.001),
        "NQ": _matrix("NQ", np.cos(x / 11.0) * 0.0015),
        "RTY": _matrix("RTY", np.sin(x / 13.0 + 0.4) * 0.0012),
        "YM": _matrix("YM", np.cos(x / 15.0 + 0.2) * 0.0008),
    }
    snapshot = build_dispersion_snapshot(
        matrices,
        timeframe_minutes=5,
        calibration_end_exclusive_ns=500 * MINUTE_NS,
    )

    assert len(snapshot["timestamp_ns"]) == count
    assert np.all(snapshot["available_at_ns"] <= snapshot["decision_ns"])
    assert snapshot["calibration_mask"][500] == 0
    assert snapshot["snapshot_hash"]

    broken = dict(matrices)
    broken["YM"] = _matrix("YM", np.cos(x / 15.0), session_code=1)
    with pytest.raises(DispersionResidualError, match="session roles differ"):
        build_dispersion_snapshot(
            broken,
            timeframe_minutes=5,
            calibration_end_exclusive_ns=500 * MINUTE_NS,
        )


def _decision_snapshot() -> dict:
    count = 150
    dispersion = np.full(count, 1.0)
    dispersion[:100] = np.linspace(0.1, 2.0, 100)
    # Two excursions; repeated above-threshold rows must not multiply actions.
    dispersion[110:114] = 5.0
    dispersion[120:123] = 6.0
    residual = np.tile(np.array([0.2, -1.4, 0.8, 0.4]), (count, 1))
    return {
        "timestamp_ns": np.arange(count, dtype=np.int64) * MINUTE_NS,
        "decision_ns": (np.arange(count, dtype=np.int64) + 1) * MINUTE_NS,
        "available_at_ns": (np.arange(count, dtype=np.int64) + 1) * MINUTE_NS,
        "session_code": np.zeros(count, dtype=np.int8),
        "session_day": np.full(count, 19_541, dtype=np.int64),
        "structural_epoch": np.zeros(count, dtype=np.int64),
        "finite": np.ones(count, dtype=bool),
        "boundary": np.ones(count, dtype=bool),
        "calibration_mask": np.arange(count) < 100,
        "dispersion": dispersion,
        "common_factor": np.full(count, -0.5),
        "residual": residual,
        "positions": {
            market: np.arange(count, dtype=np.int64)
            for market in ("ES", "NQ", "RTY", "YM")
        },
        "snapshot_hash": "synthetic-snapshot",
    }


def test_excursion_state_machine_emits_once_and_negative_side_routes_correctly() -> None:
    manifest = load_manifest()
    manifest = deepcopy(manifest)
    manifest["causal_contract"]["cooldown_minutes"] = 5
    leader = DispersionCell("CONTINUATION_LEADER", 5, "OPEN", 0)
    laggard = DispersionCell("CONVERGENCE_LAGGARD", 5, "OPEN", 0)
    snapshot = _decision_snapshot()

    leader_rows, _receipt = build_cell_decisions(
        leader,
        snapshot,
        evaluation_start_ns=101 * MINUTE_NS,
        evaluation_end_exclusive_ns=141 * MINUTE_NS,
        manifest=manifest,
    )
    laggard_rows, _receipt = build_cell_decisions(
        laggard,
        snapshot,
        evaluation_start_ns=101 * MINUTE_NS,
        evaluation_end_exclusive_ns=141 * MINUTE_NS,
        manifest=manifest,
    )

    assert len(leader_rows) == len(laggard_rows) == 2
    assert {row["side"] for row in leader_rows + laggard_rows} == {-1}
    # common<0 => aligned=-residual: NQ is the downside leader; RTY is laggard.
    assert {row["state_market"] for row in leader_rows} == {"NQ"}
    assert {row["state_market"] for row in laggard_rows} == {"RTY"}


def test_ambiguous_rank_tie_abstains_for_entire_excursion() -> None:
    snapshot = _decision_snapshot()
    snapshot["residual"] = np.tile(np.array([-1.0, -1.0, 0.5, 0.5]), (150, 1))
    manifest = deepcopy(load_manifest())
    manifest["causal_contract"]["cooldown_minutes"] = 5
    rows, _receipt = build_cell_decisions(
        DispersionCell("CONTINUATION_LEADER", 5, "OPEN", 0),
        snapshot,
        evaluation_start_ns=101 * MINUTE_NS,
        evaluation_end_exclusive_ns=141 * MINUTE_NS,
        manifest=manifest,
    )
    assert rows == []


def test_dynamic_market_outcomes_split_by_instrument_and_rank_control_matches_stop_risk() -> None:
    cell = DispersionCell("CONTINUATION_LEADER", 5, "OPEN", 0)
    causal = load_manifest()["causal_contract"]
    expected = {"ES": "MES", "NQ": "MNQ", "RTY": "M2K", "YM": "MYM"}
    assert {
        market: _outcome_candidate(cell, market, CONTROL_PRIMARY, causal).execution_market
        for market in expected
    } == expected

    primary = SimpleNamespace(
        feature_fingerprint="one",
        risk_unit_price=10.0,
        adverse_r=1.0,
        execution_market="MES",
    )
    rank = SimpleNamespace(
        feature_fingerprint="one",
        risk_unit_price=10.0,
        adverse_r=1.0,
        execution_market="MNQ",
    )
    quantities, errors = _matched_control_quantities(
        [rank], {"one": primary}, primary_quantity=8, legal_micro=50
    )
    # MES point value 5 versus MNQ 2: 8 MES micros match 20 MNQ micros.
    assert quantities == (20,)
    assert errors == (0.0,)


def _summary(*, passes: int, net: float, p25: float) -> dict:
    by_block = {
        "B3": {"pass_count": passes // 2, "net_total_usd": net / 2},
        "B4": {"pass_count": passes - passes // 2, "net_total_usd": net / 2},
    }
    return {
        "episode_count": 12,
        "pass_count": passes,
        "pass_rate": passes / 12,
        "blocks_with_passes": ["B3", "B4"] if passes >= 2 else [],
        "by_block": by_block,
        "net_total_usd": net,
        "net_median_usd": net / 12,
        "target_progress_median": p25 + 0.1,
        "target_progress_p25": p25,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer_usd": 1200.0,
        "consistency_compliance_rate": 1.0,
        "all_passing_paths_consistency_compliant": passes > 0,
        "terminal_distribution": {},
    }


def _account_cell(control: str, *, design_passes: int, heldout_net: float) -> dict:
    design = _summary(passes=design_passes, net=1000.0, p25=0.2)
    heldout = _summary(passes=4, net=heldout_net, p25=0.4)
    return {
        "control": control,
        "account_label": "50K",
        "account_size_usd": 50_000,
        "micro_quantity": 8,
        "maximum_control_micro_quantity": 8,
        "mini_equivalent": 0.8,
        "maximum_stop_risk_match_error_fraction": 0.0,
        "horizon_trading_days": 10,
        "normal": {"design": design, "held_out_development": heldout},
        "stressed": {"design": design, "held_out_development": heldout},
    }


def test_account_cell_is_selected_only_from_B1_B2_and_gate_checks_paired_controls() -> None:
    manifest = load_manifest()
    cell = DispersionCell("CONTINUATION_LEADER", 5, "OPEN", 0)
    rows = [_account_cell(CONTROL_PRIMARY, design_passes=3, heldout_net=3000.0)]
    rows.extend(
        _account_cell(control, design_passes=3, heldout_net=1000.0)
        for control in (CONTROL_DIRECTION_FLIP, CONTROL_RANK_SWAP, CONTROL_DELAY)
    )
    evidence = {control: (1, 2, 3) for control in CONTROLS}
    first = decide_cell(cell, [{"id": 1}], evidence, rows, manifest=manifest)
    assert first["passed"] is True
    assert first["frozen_from_design_only"]["micro_quantity"] == 8

    # Held-out perturbation cannot change the preselected account cell.
    perturbed = deepcopy(rows)
    for row in perturbed:
        row["normal"]["held_out_development"]["net_total_usd"] *= 100
        row["stressed"]["held_out_development"]["net_total_usd"] *= 100
    second = decide_cell(cell, [{"id": 1}], evidence, perturbed, manifest=manifest)
    assert second["frozen_from_design_only"] == first["frozen_from_design_only"]
