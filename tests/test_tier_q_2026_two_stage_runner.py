from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.production import tier_q_2026_two_stage_runner as runner


def _micro_candidate() -> tuple[runner.HazardCandidate, runner.CalibratedHazardCandidate]:
    candidate = runner.HazardCandidate(
        market="NQ",
        execution_market="MNQ",
        mechanism="COMPRESSION_TO_EXPANSION",
        cross_asset_reference_market=None,
        timeframe="1m",
        session_code=0,
        trigger_feature="rv_short_long_ratio",
        trigger_operator="LT",
        trigger_quantile=0.55,
        context_feature=None,
        context_operator=None,
        context_quantile=None,
        direction_rule="PAST_RETURN_CONTINUATION",
        favorable_r=0.5,
        adverse_r=0.5,
        horizon=5,
        risk_level=0.75,
        cooldown_minutes=1,
    )
    calibrated = runner.CalibratedHazardCandidate(
        candidate=candidate,
        calibration_end_exclusive_ns=0,
        trigger_threshold=1.0,
        context_threshold=None,
        finite_trigger_observations=100,
        finite_context_observations=None,
        source_matrix_hash="source-matrix",
    )
    return candidate, calibrated


def _micro_matrix(
    *,
    contracts: tuple[int, ...] = (7, 7, 7, 7),
    days: tuple[int, ...] = (0, 0, 0, 0),
    volatility: float = 0.02,
) -> runner.FeatureMatrix:
    minute = runner.hazard.MINUTE_NS
    # The missing timestamp at +1m represents no micro trade, not a fabricated
    # zero-volume bar.  +2m is therefore the first real tradable open.
    timestamp = np.asarray((0, 2 * minute, 4 * minute, 6 * minute), dtype=np.int64)
    arrays = {
        "timestamp_ns": timestamp,
        "availability_ns": timestamp + minute,
        "contract_code": np.asarray(contracts, dtype=np.int16),
        "segment_code": np.asarray((10, 11, 12, 13), dtype=np.int64),
        "session_day": np.asarray(days, dtype=np.int32),
        "session_code": np.zeros(4, dtype=np.int16),
        "bar_open": np.asarray((99.0, 100.0, 100.1, 100.2)),
        "bar_high": np.asarray((99.1, 100.1, 100.2, 100.3)),
        "bar_low": np.asarray((98.9, 99.9, 100.0, 100.1)),
        "bar_close": np.asarray((99.0, 100.0, 100.1, 100.2)),
        "feature__past_volatility": np.full(4, volatility, dtype=np.float64),
    }
    return runner.FeatureMatrix(
        root=Path("."),
        manifest={"row_count": 4, "bundle_hash": "micro-execution-matrix"},
        arrays=arrays,
    )


def _source_intent(candidate: runner.HazardCandidate) -> runner.HazardIntent:
    minute = runner.hazard.MINUTE_NS
    return runner.HazardIntent(
        candidate_id=candidate.candidate_id,
        intent_namespace=candidate.candidate_id,
        evidence_role="CANDIDATE",
        control_id=None,
        row_index=123,
        market="NQ",
        contract_code=99,
        session_day=0,
        session_code=0,
        segment_code=99,
        event_time_ns=0,
        available_at_ns=minute,
        decision_time_ns=minute,
        order_submit_time_ns=minute,
        entry_intent="ENTER_LONG_NEXT_TRADABLE_OPEN",
        earliest_executable_time_ns=minute,
        direction=1,
        feature_fingerprint="frozen-source-feature",
    )


def _contract() -> dict:
    core = {
        "schema": runner.CONTRACT_SCHEMA,
        "status": "FROZEN_AWAITING_ACQUISITION",
        "promotion_order": ["Q", "G", "C"],
        "outcome_accessed_at_freeze": False,
        "candidate_cohort": [{"candidate_id": "candidate_a"}],
        "temporal_roles": [
            {
                "role": runner.FINAL_DEVELOPMENT,
                "start": "2026-01-01",
                "end": "2026-05-01",
                "retuning_allowed": False,
            },
            {
                "role": runner.CONFIRMATION,
                "start": "2026-05-01",
                "end": "2026-07-19",
                "retuning_allowed": False,
            },
        ],
    }
    return {**core, "contract_hash": stable_hash(core)}


def _summary(*, passes: int, net: float, blocks: tuple[str, ...]) -> dict:
    return {
        "pass_count": passes,
        "net_total_usd": net,
        "mll_breach_rate": 0.0,
        "episode_path_hash": stable_hash([passes, net, blocks]),
        "passing_paths_consistency_compliant": passes > 0,
        "by_block": {
            block: {"pass_count": 1 if index < passes else 0}
            for index, block in enumerate(blocks)
        },
    }


def _concentration(*, share: float = 0.25) -> dict:
    return {
        "cleared": share <= 0.5,
        "worst_case_maximums": {
            "maximum_single_day_profit_share": share,
            "maximum_single_trade_profit_share": share,
            "maximum_single_event_profit_share": share,
        },
    }


def test_tier_g_gate_requires_passes_in_two_contexts() -> None:
    thresholds = {
        "minimum_normal_passes": 2,
        "minimum_stressed_passes": 2,
        "minimum_positive_temporal_contexts": 2,
        "maximum_stressed_mll_breach_rate": 0.1,
        "single_trade_or_day_profit_share_maximum": 0.5,
        "no_retuning": True,
    }
    normal = _summary(passes=2, net=1000.0, blocks=("FD_A", "FD_B"))
    stressed = _summary(passes=2, net=500.0, blocks=("FD_A", "FD_B"))
    result = runner.tier_g_gate(normal, stressed, _concentration(), thresholds)
    assert result["passed"] is True
    assert result["resulting_tier"] == "G"

    one_context = _summary(passes=2, net=500.0, blocks=("FD_A", "FD_A_COPY"))
    one_context["by_block"]["FD_A_COPY"]["pass_count"] = 0
    failed = runner.tier_g_gate(normal, one_context, _concentration(), thresholds)
    assert failed["passed"] is False
    assert failed["checks"]["minimum_positive_temporal_contexts"] is False


def test_tier_c_gate_is_prior_g_and_one_shot_concentration_bound() -> None:
    thresholds = {
        "minimum_normal_passes": 1,
        "minimum_stressed_passes": 1,
        "maximum_stressed_mll_breach_rate": 0.1,
        "single_trade_or_day_profit_share_maximum": 0.5,
        "no_retuning": True,
    }
    normal = _summary(passes=1, net=500.0, blocks=("C",))
    stressed = _summary(passes=1, net=250.0, blocks=("C",))
    passed = runner.tier_c_gate(
        normal,
        stressed,
        _concentration(),
        thresholds,
        prior_tier_g=True,
        selected_horizon_matches=True,
        full_coverage_start_count=2,
    )
    assert passed["passed"] is True
    assert passed["resulting_tier"] == "C"

    failed = runner.tier_c_gate(
        normal,
        stressed,
        _concentration(share=0.75),
        thresholds,
        prior_tier_g=True,
        selected_horizon_matches=True,
        full_coverage_start_count=2,
    )
    assert failed["passed"] is False
    assert failed["checks"]["concentration_complete_and_controlled"] is False


def test_confirmation_stays_sealed_until_self_hashed_tier_g_result() -> None:
    contract = _contract()
    try:
        runner._authorized_confirmation_ids(contract, None)
    except runner.TierQTwoStageError as exc:
        assert "sealed" in str(exc)
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("confirmation opened without final development")

    candidate = {
        "candidate_id": "candidate_a",
        "promotion_gate": {"passed": True},
        "resulting_evidence_tier": "G",
    }
    core = {
        "schema": runner.STAGE_SCHEMA,
        "role": runner.FINAL_DEVELOPMENT,
        "contract_hash": contract["contract_hash"],
        "candidate_results": [candidate],
        "tier_g_candidate_ids": ["candidate_a"],
        "retuning_performed": False,
        "recalibration_performed": False,
    }
    result = {**core, "result_hash": stable_hash(core)}
    assert runner._authorized_confirmation_ids(contract, result) == ("candidate_a",)

    drift = dict(result)
    drift["tier_g_candidate_ids"] = []
    try:
        runner._authorized_confirmation_ids(contract, drift)
    except runner.TierQTwoStageError as exc:
        assert "receipt drift" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("mutated final-development result authorized confirmation")


def test_non_overlapping_starts_never_cross_frozen_blocks() -> None:
    jan_1 = runner._date_ns("2026-01-01") // runner.DAY_NS
    days = tuple(jan_1 + value for value in range(12))
    starts = runner._non_overlapping_role_starts(
        days,
        blocks=(
            {"block_id": "A", "start": "2026-01-01", "end": "2026-01-07"},
            {"block_id": "B", "start": "2026-01-07", "end": "2026-01-13"},
        ),
        horizon=3,
    )
    assert starts == (
        (jan_1, "A"),
        (jan_1 + 3, "A"),
        (jan_1 + 6, "B"),
        (jan_1 + 9, "B"),
    )


def test_real_receipt_final_development_never_reads_sealed_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[1]
    contract = json.loads(
        (root / "config/research/tier_q_2026_two_stage_confirmation_v1.json").read_text()
    )
    receipt = json.loads(
        (root / "reports/data_access/tier_q_2026_acquisition_receipt.json").read_text()
    )
    assert receipt["receipt_hash"] == (
        "6e04534226a7e60c408db9576091c75a787d5428f440bed355cb9d7485093607"
    )
    original = runner._sha256
    opened: list[str] = []

    def guarded(path: Path) -> str:
        opened.append(str(path))
        if path.name == "confirmation_2026_sealed.parquet":
            raise AssertionError("sealed confirmation artifact was opened before Tier G")
        return original(path)

    monkeypatch.setattr(runner, "_sha256", guarded)
    verified = runner.verify_acquisition_receipt(
        contract, receipt, open_role=runner.FINAL_DEVELOPMENT
    )
    assert "SEALED_CONFIRMATION_PARQUET" in verified["_inventory"]
    assert not any("confirmation_2026_sealed.parquet" in value for value in opened)
    assert any("final_development_2026.parquet" in value for value in opened)


def test_real_final_development_coverage_excludes_partial_split_session() -> None:
    root = Path(__file__).resolve().parents[1]
    frame = pd.read_parquet(
        root
        / "data/cache/databento/tier_q_2026_confirmation/97a80942156d15b9801d/"
        "final_development_2026.parquet"
    )
    audit = runner.audit_final_development_coverage(frame)
    assert audit["raw_row_count"] == 1_136_722
    assert audit["true_trading_day_count"] == 85
    assert audit["partial_split_session"]["session_day"] == "2026-05-01"
    assert audit["partial_split_session"]["eligible_as_episode_start"] is False
    assert audit["good_friday_energy"]["CL_rows"] == 0
    assert audit["good_friday_energy"]["MCL_rows"] == 0
    assert audit["maximum_theoretical_non_overlapping_starts_before_warmup"] == {
        "5": 17,
        "10": 8,
        "20": 4,
    }


def test_sparse_micro_gap_uses_first_real_micro_open_and_preserves_signal() -> None:
    candidate, calibrated = _micro_candidate()
    events, receipt = runner._remap_and_observe_execution_outcomes(
        calibrated,
        _micro_matrix(),
        (_source_intent(candidate),),
    )
    assert len(events) == 1
    event = events[0]
    assert event.feature_fingerprint == "frozen-source-feature"
    assert event.direction == 1
    assert event.decision_time_ns == runner.hazard.MINUTE_NS
    assert event.execution_market == "MNQ"
    assert event.contract_code == 7
    assert event.fill_time_ns == 2 * runner.hazard.MINUTE_NS
    assert event.raw_fill_price == 100.0
    # The path later reaches the end of this tiny fixture.  The already-filled
    # signal remains present and is censored; it is never silently suppressed.
    assert str(event.outcome) == "CENSORED_FUTURE_COVERAGE"
    assert event.censor_reason == "EXECUTION_DATA_END_BEFORE_TARGET_STOP_OR_EXIT"
    assert receipt["source_intent_count"] == 1
    assert receipt["execution_remapped_count"] == 1
    assert receipt["execution_mapping_censored_count"] == 0
    assert receipt["execution_fill_price_check_count"] == 1
    assert receipt["execution_fill_price_mismatch_count"] == 0


@pytest.mark.parametrize(
    ("direction", "favorable", "adverse", "expected_target", "expected_stop"),
    (
        (1, 100.26, 99.74, 100.50, 99.50),
        (-1, 99.74, 100.26, 99.50, 100.50),
        (1, 100.25, 99.75, 100.25, 99.75),
        (-1, 99.75, 100.25, 99.75, 100.25),
        (1, 100.25000000000001, 99.74999999999999, 100.25, 99.75),
    ),
)
def test_micro_barriers_quantize_outward_and_keep_aligned_levels(
    direction: int,
    favorable: float,
    adverse: float,
    expected_target: float,
    expected_stop: float,
) -> None:
    target, stop = runner._outward_tick_barriers(
        direction=direction,
        tick_size=0.25,
        favorable_price=favorable,
        adverse_price=adverse,
    )
    assert target == expected_target
    assert stop == expected_stop


def test_micro_barrier_raw_exit_is_proven_tick_aligned() -> None:
    candidate, calibrated = _micro_candidate()
    events, receipt = runner._remap_and_observe_execution_outcomes(
        calibrated,
        _micro_matrix(volatility=0.001),
        (_source_intent(candidate),),
    )
    assert len(events) == 1
    assert events[0].raw_exit_price is not None
    assert runner._is_tick_aligned(events[0].raw_exit_price, 0.25)
    assert receipt["barrier_quantization_rule"]["rule_id"] == (
        runner.BARRIER_QUANTIZATION_RULE_ID
    )
    assert receipt["barrier_quantization_rule_hash"] == (
        runner.BARRIER_QUANTIZATION_RULE_HASH
    )
    assert receipt["barrier_tick_alignment_mismatch_count"] == 0
    assert receipt["raw_exit_tick_alignment_check_count"] == 1
    assert receipt["raw_exit_tick_alignment_mismatch_count"] == 0


@pytest.mark.parametrize(
    ("contracts", "days", "reason"),
    (
        ((7, 8, 8, 8), (0, 0, 0, 0), "EXECUTION_CONTRACT_CHANGED_BEFORE_FILL"),
        ((7, 7, 7, 7), (0, 1, 1, 1), "EXECUTION_SESSION_CHANGED_BEFORE_FILL"),
    ),
)
def test_sparse_micro_remap_censors_roll_or_session_before_fill(
    contracts: tuple[int, ...],
    days: tuple[int, ...],
    reason: str,
) -> None:
    candidate, calibrated = _micro_candidate()
    events, receipt = runner._remap_and_observe_execution_outcomes(
        calibrated,
        _micro_matrix(contracts=contracts, days=days),
        (_source_intent(candidate),),
    )
    assert len(events) == 1
    assert str(events[0].outcome) == "CENSORED_FUTURE_COVERAGE"
    assert events[0].censor_reason == reason
    assert events[0].fill_time_ns is None
    assert receipt["source_intent_count"] == 1
    assert receipt["execution_mapping_censored_count"] == 1


def test_selected_horizon_excludes_any_start_containing_censored_event() -> None:
    calendar = tuple(range(1, 11))
    replay = SimpleNamespace(
        eligible_session_days=calendar,
        events=(
            SimpleNamespace(
                session_day=3,
                outcome="CENSORED_FUTURE_COVERAGE",
            ),
        ),
    )
    coverage = runner._selected_horizon_coverage(
        replay,
        calendar,
        ((1, "FD_A"), (6, "FD_B")),
        horizon=5,
    )
    assert coverage["full"] == ((6, "FD_B"),)
    assert len(coverage["data_censored_starts"]) == 1
    rejected = coverage["data_censored_starts"][0]
    assert rejected["start_day"] == 1
    assert rejected["coverage_state"] == "DATA_CENSORED"
    assert rejected["reasons"] == ["CENSORED_EVENT_IN_WINDOW"]
    assert rejected["censored_session_days"] == [3]
