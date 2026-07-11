from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hydra.research.energy_metals_session_execution_repair import (
    synchronize_micro_execution,
)
from hydra.research.gc_session_geometry_fresh_primary import (
    CANDIDATE_ID,
    SOURCE_ID,
    SOURCE_MANIFEST_HASH,
    SOURCE_POPULATION_HASH,
    SOURCE_PREREGISTRATION_HASH,
    GCSessionGeometryFreshPrimaryError,
    _verify_source_selection,
    candidate_specification,
)


def test_fresh_gc_candidate_has_new_identity_and_micro_semantics() -> None:
    candidate = candidate_specification()

    assert candidate["candidate_id"] == CANDIDATE_ID
    assert candidate["source_diagnostic_id"] == SOURCE_ID
    assert candidate["signal_market"] == "GC"
    assert candidate["execution_market"] == "MGC"
    assert candidate["signal_semantics"] == "GC_SIGNAL_MGC_SYNCHRONIZED_EXECUTION"
    assert len(candidate["structural_fingerprint"]) == 64


def test_source_selection_requires_first_pre_2024_metals_diagnostic() -> None:
    fingerprint = "a" * 64
    hypothesis = {
        "candidate_id": SOURCE_ID,
        "structural_fingerprint": fingerprint,
        "market": "GC",
        "execution_market": "MGC",
        "feature": "overnight_displacement",
        "policy_direction": "reversal",
        "quantile": 0.65,
        "horizon": 60,
        "context": "none",
    }
    preregistration = {
        "preregistration_hash": SOURCE_PREREGISTRATION_HASH,
        "population_hash": SOURCE_POPULATION_HASH,
        "hypotheses": [hypothesis],
    }
    freeze = {
        "primary_manifest_hash": SOURCE_MANIFEST_HASH,
        "population_hash": SOURCE_POPULATION_HASH,
        "selection_data_end_exclusive": "2024-01-01",
        "diagnostics_inherit_status": False,
        "archive_candidate_ids": ["strategy_session_geometry_CL_x", SOURCE_ID],
        "primary_candidate_id": "strategy_session_geometry_CL_x",
        "ranking": [
            {
                "candidate_id": "strategy_session_geometry_CL_x",
                "structural_fingerprint": "b" * 64,
            },
            {"candidate_id": SOURCE_ID, "structural_fingerprint": fingerprint},
        ],
    }

    assert _verify_source_selection(preregistration, freeze) == hypothesis
    freeze["selection_data_end_exclusive"] = "2024-10-01"
    with pytest.raises(GCSessionGeometryFreshPrimaryError):
        _verify_source_selection(preregistration, freeze)


def test_generic_micro_synchronization_uses_mgc_multiplier_and_exact_time() -> None:
    timestamp = pd.Timestamp("2024-01-03T14:11:00Z")
    signals = pd.DataFrame(
        [
            {
                "trading_session_id": "2024-01-03",
                "entry_timestamp": timestamp,
                "side": -1.0,
            },
            {
                "trading_session_id": "2024-01-04",
                "entry_timestamp": timestamp + pd.Timedelta(days=1),
                "side": 1.0,
            },
        ]
    )
    execution = pd.DataFrame(
        [
            {
                "session_id": "2024-01-03",
                "active_contract": "MGCG4",
                "overnight_entry_timestamp": timestamp,
                "overnight_entry_price": 2050.0,
                "overnight_exit_timestamp_60": timestamp + pd.Timedelta(minutes=60),
                "overnight_exit_60": 2048.0,
                "overnight_long_mae_60": -3.0,
                "overnight_short_mae_60": -1.0,
                "overnight_entry_timestamp_delay1": timestamp
                + pd.Timedelta(minutes=1),
                "overnight_entry_price_delay1": 2049.9,
                "overnight_exit_timestamp_60_delay1": timestamp
                + pd.Timedelta(minutes=61),
                "overnight_exit_60_delay1": 2048.1,
                "overnight_long_mae_60_delay1": -2.9,
                "overnight_short_mae_60_delay1": -1.1,
            }
        ]
    )

    events, missing = synchronize_micro_execution(
        signals,
        execution,
        signal_symbol="GC",
        execution_symbol="MGC",
        candidate_id=CANDIDATE_ID,
        parent_candidate_id=SOURCE_ID,
        entry_prefix="overnight",
        horizon=60,
    )

    assert len(events) == 1
    assert events.iloc[0]["candidate_id"] == CANDIDATE_ID
    assert events.iloc[0]["signal_symbol"] == "GC"
    assert events.iloc[0]["symbol"] == "MGC"
    assert events.iloc[0]["gross_pnl"] == pytest.approx(20.0)
    assert events.iloc[0]["net_pnl"] < events.iloc[0]["gross_pnl"]
    assert not bool(events.iloc[0]["signal_recomputed_from_micro"])
    assert missing == [
        {"session_id": "2024-01-04", "reason": "missing_mgc_session"}
    ]


def test_task_freezes_the_exact_candidate_before_confirmation() -> None:
    task = Path(
        "reports/engineering/hydra_gc_session_geometry_fresh_primary_20260711.md"
    ).read_text(encoding="utf-8")

    assert CANDIDATE_ID in task
    assert "selection data end: 2024-01-01 exclusive" in task
    assert "Q4 and later data: prohibited" in task
    assert "PAPER_SHADOW_READY" in task
