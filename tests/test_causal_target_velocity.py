from __future__ import annotations

import numpy as np

from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.causal_target_velocity import (
    CalibratedHazardCandidate,
    HazardCandidate,
    HazardOutcome,
    calibrate_candidate,
    deduplicate_for_event_screen,
    direction_flipped_intents,
    discover_intents_batch,
    discover_intents_streaming,
    exact_sleeve_replay,
    generate_structural_proposals,
    matched_random_intents,
    observe_outcomes,
    screen_result,
    with_availability_safe_cross_asset_feature,
)


MINUTE = 60_000_000_000


def _candidate(**overrides: object) -> HazardCandidate:
    payload: dict[str, object] = {
        "market": "CL",
        "execution_market": "MCL",
        "mechanism": "PARTICIPATION_DENSITY",
        "cross_asset_reference_market": None,
        "timeframe": "1m",
        "session_code": 0,
        "trigger_feature": "past_participation",
        "trigger_operator": "GT",
        "trigger_quantile": 0.65,
        "context_feature": "rv_short_long_ratio",
        "context_operator": "GT",
        "context_quantile": 0.50,
        "direction_rule": "PAST_RETURN_CONTINUATION",
        "favorable_r": 0.5,
        "adverse_r": 0.5,
        "horizon": 5,
        "risk_level": 1.0,
        "cooldown_minutes": 5,
    }
    payload.update(overrides)
    return HazardCandidate(**payload)  # type: ignore[arg-type]


def _matrix(*, ambiguous: bool = False, truncate_after_entry: bool = False) -> FeatureMatrix:
    count = 80
    timestamp = np.arange(count, dtype=np.int64) * MINUTE
    trigger = np.full(count, 1.0, dtype=float)
    trigger[40:] = 1.0
    trigger[45] = 10.0
    trigger[46:] = 1.0
    context = np.full(count, 2.0, dtype=float)
    context[:40] = 1.0
    returns = np.full(count, 0.01, dtype=float)
    opens = np.full(count, 100.0, dtype=float)
    highs = np.full(count, 100.02, dtype=float)
    lows = np.full(count, 99.98, dtype=float)
    closes = np.full(count, 100.0, dtype=float)
    if ambiguous:
        highs[46] = 101.0
        lows[46] = 99.0
    else:
        highs[47] = 101.0
    arrays = {
        "timestamp_ns": timestamp,
        "decision_ns": timestamp + MINUTE,
        "availability_ns": timestamp + MINUTE,
        "session_day": np.full(count, 20240102, dtype=np.int32),
        "session_code": np.zeros(count, dtype=np.int8),
        "segment_code": np.ones(count, dtype=np.int32),
        "contract_code": np.ones(count, dtype=np.int32),
        "bar_open": opens,
        "bar_high": highs,
        "bar_low": lows,
        "bar_close": closes,
        "feature__past_participation": trigger,
        "feature__rv_short_long_ratio": context,
        "feature__past_return_60": returns,
        "feature__past_volatility": np.full(count, 0.001, dtype=float),
        "feature__ctx_15m_return": np.arange(count, dtype=float),
    }
    if truncate_after_entry:
        arrays = {name: value[:47] for name, value in arrays.items()}
        count = 47
    return FeatureMatrix(
        root=None,  # type: ignore[arg-type]
        manifest={
            "row_count": count,
            "bundle_hash": "a" * 64,
            "key": {"market": "CL"},
        },
        arrays=arrays,
    )


def _calibrated(matrix: FeatureMatrix, candidate: HazardCandidate | None = None) -> CalibratedHazardCandidate:
    return calibrate_candidate(
        candidate or _candidate(),
        matrix,
        calibration_end_exclusive_ns=40 * MINUTE,
        minimum_observations=20,
    )


def test_bounded_population_supports_production_counts_and_is_diverse() -> None:
    proposals = generate_structural_proposals(
        {
            "CL": "MCL",
            "ES": "MES",
            "GC": "MGC",
            "NQ": "MNQ",
            "RTY": "M2K",
            "YM": "MYM",
        },
        minimum_count=20_000,
    )
    assert len(proposals) == 20_000
    assert len({row.structural_fingerprint for row in proposals}) == 20_000
    unique = deduplicate_for_event_screen(
        proposals, minimum_unique=4_096, maximum_unique=4_096
    )
    assert len(unique) == 4_096
    assert len({row.market for row in unique}) == 6
    assert len({row.mechanism for row in unique}) == 10
    assert len({row.horizon for row in unique}) == 6


def test_calibration_and_decisions_never_require_future_arrays() -> None:
    matrix = _matrix()
    calibrated = _calibrated(matrix)
    # The synthetic matrix intentionally has no entry_price or forward_move__*
    # fields.  A future-dependent decision path would fail here.
    batch = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    streaming = discover_intents_streaming(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    assert [(row.row_index, row.direction) for row in batch] == list(streaming)
    assert [(row.row_index, row.direction) for row in batch] == [(45, 1)]
    assert batch[0].available_at_ns <= batch[0].decision_time_ns
    assert batch[0].earliest_executable_time_ns == 46 * MINUTE


def test_same_bar_touch_is_ambiguous_and_adverse_first() -> None:
    matrix = _matrix(ambiguous=True)
    calibrated = _calibrated(matrix)
    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    events = observe_outcomes(calibrated, matrix, intents)
    assert len(events) == 1
    event = events[0]
    assert event.outcome == HazardOutcome.ADVERSE_FIRST
    assert event.same_bar_ambiguous is True
    assert event.fill_time_ns == event.earliest_executable_time_ns
    assert event.normal_net_pnl is not None and event.normal_net_pnl < 0
    assert event.stressed_net_pnl is not None and event.stressed_net_pnl < event.normal_net_pnl


def test_timeframe_is_an_executable_completed_bar_gate_in_batch_and_stream() -> None:
    matrix = _matrix()
    trigger = matrix.array("feature__past_participation").copy()
    trigger[44] = 10.0  # one-minute bar completes exactly at minute 45
    arrays = dict(matrix.arrays)
    arrays["feature__past_participation"] = trigger
    matrix = FeatureMatrix(root=matrix.root, manifest=matrix.manifest, arrays=arrays)
    calibrated = _calibrated(matrix, _candidate(timeframe="5m"))
    batch = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    streaming = discover_intents_streaming(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    assert [(row.row_index, row.direction) for row in batch] == [(44, 1)]
    assert [(row.row_index, row.direction) for row in batch] == list(streaming)


def test_missing_future_coverage_censors_but_preserves_signal() -> None:
    matrix = _matrix(truncate_after_entry=True)
    calibrated = _calibrated(matrix)
    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=47 * MINUTE,
    )
    assert len(intents) == 1
    events = observe_outcomes(calibrated, matrix, intents)
    assert len(events) == 1
    assert events[0].outcome == HazardOutcome.CENSORED_FUTURE_COVERAGE
    assert events[0].fill_time_ns == 46 * MINUTE
    assert events[0].normal_net_pnl is None


def test_session_horizon_uses_predeclared_causal_session_flatten() -> None:
    matrix = _matrix()
    days = matrix.array("session_day").copy()
    days[60:] = 20240103
    arrays = dict(matrix.arrays)
    arrays["session_day"] = days
    arrays["bar_high"] = np.full(matrix.row_count, 100.02, dtype=float)
    matrix = FeatureMatrix(root=matrix.root, manifest=matrix.manifest, arrays=arrays)
    calibrated = _calibrated(
        matrix,
        _candidate(horizon="SESSION", cooldown_minutes=390),
    )
    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    events = observe_outcomes(calibrated, matrix, intents)
    assert len(events) == 1
    assert events[0].outcome == HazardOutcome.NEITHER_REACHED
    assert events[0].exit_fill_semantics == (
        "PREDECLARED_SESSION_CLOSE_FLATTEN_WITH_FROZEN_SLIPPAGE"
    )
    assert events[0].outcome_time_ns == 60 * MINUTE


def test_screen_exact_replay_and_matched_controls_are_deterministic() -> None:
    matrix = _matrix()
    calibrated = _calibrated(matrix)
    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
    )
    events = observe_outcomes(calibrated, matrix, intents)
    frozen_calendar = (20240102, 20240103, 20240104, 20240105)
    result = screen_result(
        calibrated, events, eligible_session_days=frozen_calendar
    )
    exact = exact_sleeve_replay(
        calibrated, events, eligible_session_days=frozen_calendar
    )
    assert result.emitted_event_count == 1
    assert result.eligible_session_count == 4
    assert result.emitted_session_count == 1
    assert result.independent_events_per_20_sessions == 5.0
    assert result.favorable_first_count == 1
    assert len(exact.normal_events) == len(exact.stressed_events) == 1
    assert exact.normal_events[0].decision_ns == 46 * MINUTE
    flipped = direction_flipped_intents(intents)
    assert flipped[0].direction == -intents[0].direction
    random_a = matched_random_intents(
        calibrated,
        matrix,
        intents,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
        seed=28,
    )
    random_b = matched_random_intents(
        calibrated,
        matrix,
        intents,
        evaluation_start_ns=40 * MINUTE,
        evaluation_end_exclusive_ns=70 * MINUTE,
        seed=28,
    )
    assert [row.fingerprint for row in random_a] == [row.fingerprint for row in random_b]
    assert len(random_a) == len(intents)
    assert random_a[0].session_day == intents[0].session_day
    assert random_a[0].session_code == intents[0].session_code
    assert random_a[0].direction == intents[0].direction
    assert random_a[0].intent_namespace != intents[0].intent_namespace
    assert random_a[0].control_id is not None
    random_events = observe_outcomes(calibrated, matrix, random_a)
    flipped_events = observe_outcomes(calibrated, matrix, flipped)
    assert random_events[0].event_id != events[0].event_id
    assert flipped_events[0].event_id != events[0].event_id


def test_risk_only_variants_are_not_independent_behavior() -> None:
    variants = tuple(_candidate(risk_level=value) for value in (0.75, 1.0, 1.25, 1.5))
    assert len({row.structural_fingerprint for row in variants}) == 4
    assert len({row.behavioral_fingerprint for row in variants}) == 1
    try:
        deduplicate_for_event_screen(variants, minimum_unique=2)
    except ValueError as exc:
        assert "retained 1 < 2" in str(exc)
    else:  # pragma: no cover - fail loudly if clone control is weakened
        raise AssertionError("risk-only clones counted as independent behavior")


def test_cross_asset_asof_join_never_uses_unavailable_reference_row() -> None:
    primary = _matrix()
    reference = _matrix()
    arrays = dict(reference.arrays)
    arrays["availability_ns"] = reference.array("availability_ns") + MINUTE
    reference = FeatureMatrix(
        root=None,  # type: ignore[arg-type]
        manifest={
            "row_count": reference.row_count,
            "bundle_hash": "b" * 64,
            "key": {"market": "ES"},
        },
        arrays=arrays,
    )
    joined = with_availability_safe_cross_asset_feature(primary, reference)
    feature = joined.array("feature__cross_asset_ctx_15m_return")
    assert np.isnan(feature[0])
    # At primary decision minute 2, only reference row 0 (available minute 2)
    # exists.  Reference row 1 is not silently pulled backward.
    assert feature[1] == 0.0
    assert feature[10] == 9.0
