from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hydra.data.multitimeframe import (
    join_completed_timeframe,
    resample_closed_bars,
    resample_multi_session_context,
    resample_session_bars,
)
from hydra.factory.quality_diversity import (
    ArchiveCandidate,
    ArchiveNiche,
    QualityDiversityArchive,
    structural_fingerprint,
)
from hydra.foundry.bootstrap import _archive_smoke, _calibration, _multitimeframe_smoke, _shadow_smoke
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.foundry.tournament import run_structural_tournament
from hydra.mission.controller import (
    AutonomousMissionController,
    EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID,
    MissionControllerConfig,
)
from hydra.mission.experiment_queue import experiment_record
from hydra.mission.experiment_runner import run_experiment
from hydra.mission.mission_state import connect_state, get_kv, mission_paths
from hydra.research.equity_open_gap_reversal import (
    _block_sign_flip_probability,
    _round_turn_cost,
    build_event_table,
)
from hydra.research.opening_direction_hazard import FEATURES, _prepare_features
from hydra.foundry.q4_freeze import Q4CandidateFreezeError, _validate_candidate
from hydra.shadow.runner import ShadowRunner
from hydra.shadow.signal_bus import ShadowSignal
from hydra.shadow.specification import ShadowSpecification


def _valid_evidence(**updates: object) -> ShadowEvidence:
    values: dict[str, object] = {
        "candidate_id": "candidate",
        "data_integrity": True,
        "no_lookahead": True,
        "deterministic_signals": True,
        "net_after_costs": 100.0,
        "supportive_temporal_folds": 2,
        "candidate_null_pass": True,
        "null_probability": 0.03,
        "parameter_stable": True,
        "contract_evidence": True,
        "account_mll_safe": True,
        "execution_possible": True,
        "realtime_features_available": True,
        "shadow_spec_complete": True,
        "observability_complete": True,
        "untouched_holdout_passed": True,
        "sample_size": 50,
    }
    values.update(updates)
    return ShadowEvidence(**values)


def test_shadow_admission_separates_hard_failure_uncertainty_and_readiness() -> None:
    assert decide_shadow_admission(_valid_evidence()).tier == EvidenceTier.PAPER_SHADOW_READY
    weak = decide_shadow_admission(
        _valid_evidence(
            supportive_temporal_folds=1,
            null_probability=0.10,
            sample_size=20,
            untouched_holdout_passed=False,
        )
    )
    assert weak.tier == EvidenceTier.SHADOW_RESEARCH_CANDIDATE
    assert weak.permits_zero_risk_shadow
    fatal = decide_shadow_admission(_valid_evidence(hard_invalidations=("lookahead",)))
    assert fatal.tier == EvidenceTier.SHADOW_REJECTED
    assert not fatal.permits_zero_risk_shadow
    assert not fatal.permits_broker_orders
    pre_holdout = decide_shadow_admission(_valid_evidence(untouched_holdout_passed=False))
    assert pre_holdout.tier == EvidenceTier.SHADOW_RESEARCH_CANDIDATE


def test_shadow_policy_calibration_has_low_fpr_and_useful_power() -> None:
    result = _calibration()
    assert result["passed"] is True
    assert result["false_positive_rate"] <= 0.05
    assert result["weak_real_shadow_admission_power"] >= 0.80
    assert result["strong_paper_shadow_power"] >= 0.80


def test_closed_bar_resampling_never_exposes_partial_higher_bar() -> None:
    timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=13, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": "ES",
            "active_contract": "ESH4",
            "open": range(13),
            "high": range(1, 14),
            "low": range(-1, 12),
            "close": [value + 0.5 for value in range(13)],
            "volume": 1.0,
        }
    )
    bars = resample_closed_bars(frame, 5, as_of="2024-01-01T00:12:00Z")
    assert bars["source_bar_close"].tolist() == [
        pd.Timestamp("2024-01-01T00:05:00Z"),
        pd.Timestamp("2024-01-01T00:10:00Z"),
    ]
    decisions = pd.DataFrame(
        {
            "symbol": ["ES"],
            "active_contract": ["ESH4"],
            "decision_timestamp": [pd.Timestamp("2024-01-01T00:09:00Z")],
        }
    )
    joined = join_completed_timeframe(decisions, bars)
    assert joined.loc[0, "source_bar_close"] == pd.Timestamp("2024-01-01T00:05:00Z")


def test_multitimeframe_dst_smoke_is_deterministic() -> None:
    assert _multitimeframe_smoke() == _multitimeframe_smoke()
    assert _multitimeframe_smoke()["passed"] is True


def test_daily_and_multi_session_context_only_use_closed_contract_sessions() -> None:
    timestamps = pd.to_datetime(
        ["2024-03-08T21:58:00Z", "2024-03-08T21:59:00Z", "2024-03-11T20:58:00Z", "2024-03-11T20:59:00Z"],
        utc=True,
    )
    source = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": "ES",
            "active_contract": "ESH4",
            "trading_session_id": ["2024-03-08", "2024-03-08", "2024-03-11", "2024-03-11"],
            "open": [100.0, 101.0, 103.0, 104.0],
            "high": [101.0, 102.0, 104.0, 105.0],
            "low": [99.0, 100.0, 102.0, 103.0],
            "close": [101.0, 102.0, 104.0, 105.0],
            "volume": 1.0,
        }
    )
    sessions = resample_session_bars(source)
    daily = resample_multi_session_context(sessions, 1)
    context = resample_multi_session_context(sessions, 2)
    assert len(daily) == 2
    assert len(context) == 1
    assert context.loc[0, "open"] == 100.0
    assert context.loc[0, "close"] == 105.0
    assert context.loc[0, "availability_timestamp"] == sessions.iloc[-1]["availability_timestamp"]


def test_quality_diversity_rejects_clones_and_caps_lineages() -> None:
    archive = QualityDiversityArchive()
    niche = ArchiveNiche("equity", "1m-15m", "intraday", "rth", "hazard", "alpha", "low", "c1")
    spec = {"family": "hazard", "entry": "a"}
    candidate = ArchiveCandidate(
        "one", structural_fingerprint(spec), "lineage_one", "hazard", niche, {"net_economics": 1.0}, spec
    )
    assert archive.insert(candidate).accepted
    assert not archive.insert(candidate).accepted
    same_lineage = ArchiveCandidate(
        "two",
        structural_fingerprint({"family": "hazard", "entry": "b"}),
        "lineage_one",
        "geometry",
        ArchiveNiche("metal", "5m-60m", "session", "globex", "geometry", "defensive", "moderate", "c2"),
        {"net_economics": 2.0},
        {},
    )
    assert not archive.insert(same_lineage).accepted
    assert _archive_smoke()["passed"] is True


def test_stage0_tournament_is_diverse_deterministic_and_claims_no_edge() -> None:
    first = run_structural_tournament(504)
    second = run_structural_tournament(504)
    assert first["accepted_prototypes"] == 504
    assert first["rejected_prototypes"] == 0
    assert first["population_sha256"] == second["population_sha256"]
    assert len(first["allocations"]["engines"]) == 9
    assert len(first["allocations"]["market_ecologies"]) == 4
    assert max(first["allocations"]["families"].values()) / 504 <= 0.25
    assert max(first["allocations"]["market_ecologies"].values()) / 504 <= 0.35
    assert first["economic_claims"] == first["promotions"] == 0


def _synthetic_open_gap_frame(symbol: str = "ES", sessions: int = 50) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    previous_close = 5000.0
    for index, day in enumerate(pd.bdate_range("2024-01-02", periods=sessions)):
        local_start = pd.Timestamp(day.date(), tz="America/Chicago") + pd.Timedelta(hours=8, minutes=30)
        gap = float((index % 7) + 1) * (1 if index % 2 else -1)
        for minute in range(92):
            local = local_start + pd.Timedelta(minutes=minute)
            price = previous_close + gap * max(0.0, 1.0 - minute / 60.0)
            rows.append(
                {
                    "timestamp": local.tz_convert("UTC"),
                    "symbol": symbol,
                    "active_contract": f"{symbol}H4",
                    "open": price,
                    "high": price + 0.25,
                    "low": price - 0.25,
                    "close": price - np.sign(gap) * 0.10,
                    "volume": 100.0,
                }
            )
        close_local = pd.Timestamp(day.date(), tz="America/Chicago") + pd.Timedelta(
            hours=14, minutes=59
        )
        previous_close += 0.5
        rows.append(
            {
                "timestamp": close_local.tz_convert("UTC"),
                "symbol": symbol,
                "active_contract": f"{symbol}H4",
                "open": previous_close,
                "high": previous_close + 0.25,
                "low": previous_close - 0.25,
                "close": previous_close,
                "volume": 100.0,
            }
        )
    return pd.DataFrame(rows)


def test_open_gap_events_use_only_past_threshold_and_exact_closed_horizon() -> None:
    full_source = _synthetic_open_gap_frame()
    full = build_event_table(full_source, minimum_history=2)
    prefix_source = full_source[full_source["timestamp"] < pd.Timestamp("2024-02-01", tz="UTC")]
    prefix = build_event_table(prefix_source, minimum_history=2)
    shared = full[full["timestamp"].isin(prefix["timestamp"])].set_index("timestamp")
    prefix = prefix.set_index("timestamp")
    pd.testing.assert_series_equal(
        shared.loc[prefix.index, "threshold_q75"], prefix["threshold_q75"], check_names=False
    )
    primary = full[full["primary_event"]]
    assert len(primary) > 0
    assert (primary["reference_timestamp"] < primary["timestamp"]).all()
    assert (
        primary["exit_timestamp_60"]
        == primary["timestamp"] + pd.Timedelta(minutes=60)
    ).all()
    assert (primary["decision_timestamp"] > primary["timestamp"]).all()
    assert _round_turn_cost("MES") == 4.5


def test_open_gap_block_null_is_deterministic() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=30, freq="D", tz="UTC"),
            "gross_pnl_60": np.linspace(-20.0, 80.0, 30),
            "cost": 5.0,
        }
    )
    assert _block_sign_flip_probability(frame, seed=9) == _block_sign_flip_probability(
        frame, seed=9
    )


def test_hazard_features_are_prefix_invariant_and_past_only() -> None:
    source = _synthetic_open_gap_frame(sessions=70)
    full = _prepare_features(build_event_table(source, minimum_history=2))
    cutoff = pd.Timestamp("2024-03-15", tz="UTC")
    prefix = _prepare_features(
        build_event_table(source[source["timestamp"] < cutoff], minimum_history=2)
    )
    shared = full[full["timestamp"].isin(prefix["timestamp"])].set_index("timestamp")
    prefix = prefix.set_index("timestamp")
    for feature in FEATURES:
        pd.testing.assert_series_equal(
            shared.loc[prefix.index, feature], prefix[feature], check_names=False
        )


def test_q4_freeze_requires_complete_non_dominated_shadow_evidence() -> None:
    candidate = {
        "status": "SHADOW_RESEARCH_CANDIDATE",
        "net_pnl": 100.0,
        "supportive_temporal_folds": 1,
        "shadow_evidence": {
            "catastrophic_transfer": False,
            "candidate_null_pass": True,
            "parameter_stable": True,
            "contract_evidence": True,
            "account_mll_safe": True,
            "shadow_spec_complete": True,
            "hard_invalidations": [],
            "untouched_holdout_passed": False,
        },
        "attacks": {"event_dominated": False},
    }
    _validate_candidate(candidate)
    candidate["attacks"]["event_dominated"] = True
    with pytest.raises(Q4CandidateFreezeError, match="not_event_dominated"):
        _validate_candidate(candidate)


def test_foundry_bootstrap_routes_exactly_once_to_gap_pilot(tmp_path: Path) -> None:
    paths = mission_paths(str(tmp_path / "state"))
    conn = connect_state(paths)
    controller = AutonomousMissionController(
        MissionControllerConfig(
            mission_id="test",
            baseline_commit="test",
            objective_config="test",
            remaining_databento_budget_usd=77.0,
            persistent=False,
            state_dir=str(paths.state_dir),
        )
    )
    result = {
        "scientific_conclusion": "FOUNDRY_CORE_CALIBRATED_TOURNAMENT_RECONCILED",
        "foundry_status": {
            "strategy_prototypes_generated": 696,
            "strategies_screened": 696,
            "mechanisms_represented": 19,
            "market_ecologies_represented": 4,
            "timeframes_represented": 8,
            "model_quota_state": "AVAILABLE_FOR_ENGINEERING",
        },
    }
    try:
        controller._route_foundry_bootstrap_result(conn, result)
        controller._route_foundry_bootstrap_result(conn, result)
        record = experiment_record(conn, EQUITY_OPEN_GAP_REVERSAL_EXPERIMENT_ID)
        assert record is not None and record["status"] == "QUEUED"
        assert record["specification"]["q4_access_allowed"] is False
        assert record["specification"]["paid_data_allowed"] is False
        assert get_kv(conn, "current_phase") == "PLANNING_NEXT_ACTION"
    finally:
        conn.close()


def test_experiment_runner_dispatches_open_gap_pilot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    def fake(output_dir: Path, **kwargs: object) -> dict[str, str]:
        captured.update({"output_dir": output_dir, **kwargs})
        return {"scientific_conclusion": "controlled"}

    monkeypatch.setattr(
        "hydra.research.equity_open_gap_reversal.run_equity_open_gap_reversal_pilot",
        fake,
    )
    result = run_experiment(
        {
            "experiment_id": "gap",
            "experiment_type": "equity_open_gap_reversal_pilot",
            "engineering_task_path": "task.md",
            "engineering_task_sha256": "task-hash",
            "repaired_map_path": "map.json",
            "repaired_map_sha256": "map-hash",
            "repaired_roll_map_hash": "roll-hash",
            "code_commit": "commit",
        },
        output_root=tmp_path,
    )
    assert result["scientific_conclusion"] == "controlled"
    assert captured["repaired_roll_map_hash"] == "roll-hash"


def _specification(**updates: object) -> ShadowSpecification:
    values: dict[str, object] = {
        "strategy_id": "s1",
        "strategy_version": "v1",
        "feature_versions": ("f1",),
        "markets": ("MES",),
        "timeframes": ("1m",),
        "session_rules": {"flatten": "15:10"},
        "entry_rules": {"event": "test"},
        "exit_rules": {"bars": 10},
        "sizing": {"contracts": 1},
        "costs": {"round_turn": 4.5},
        "stale_data_seconds": 90,
        "expected_update_seconds": 60,
        "duplicate_signal_window_seconds": 60,
        "maximum_exposure": 1.0,
        "simulated_mll_floor": -2500.0,
        "internal_daily_risk_limit": 800.0,
        "kill_conditions": ("stale_data",),
        "logging": {"jsonl": True},
        "reconciliation": {"startup": "fail_closed"},
        "source_manifest_hash": "abc",
    }
    values.update(updates)
    return ShadowSpecification(**values)


def test_shadow_runner_is_virtual_duplicate_safe_and_fail_closed() -> None:
    spec = _specification()
    runner = ShadowRunner(spec)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    signal = ShadowSignal("s1", "MES", 1, 1, now, now, 5000.0)
    kwargs = dict(
        now=now,
        latest_data_at=now,
        market_price=5000.0,
        proposed_exposure=1.0,
        session_open=True,
        simulated_mll=-100.0,
        daily_pnl=0.0,
        slippage_per_unit=0.25,
        round_turn_cost=4.5,
    )
    assert runner.process(signal, **kwargs)["status"] == "VIRTUAL_FILLED"
    assert runner.process(signal, **kwargs)["reason"] == "duplicate_signal"
    stale = ShadowSignal("s1", "MES", -1, 1, now + timedelta(minutes=2), now, 5000.0)
    assert runner.process(
        stale, **{**kwargs, "now": now + timedelta(minutes=2), "latest_data_at": now}
    )["reason"] == "stale_data"
    assert not hasattr(runner.execution, "submit_order")
    assert _shadow_smoke()["passed"] is True


def test_shadow_spec_rejects_order_capability_and_credentials() -> None:
    with pytest.raises(ValueError, match="outbound"):
        _specification(outbound_orders_enabled=True).validate()
    with pytest.raises(ValueError, match="Prohibited"):
        _specification(logging={"api_key": "forbidden"}).validate()
