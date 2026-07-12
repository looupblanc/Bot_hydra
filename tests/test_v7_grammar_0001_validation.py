from __future__ import annotations

import numpy as np
import pytest

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.research.v7_hypothesis_grammar import (
    V7CandidateSpec,
    V7MarketBars,
    V7Signal,
)
from hydra.validation.v7_grammar_0001_validation import (
    _synthetic_market_bars,
    horizon_cost_bucket,
    null_ratio_verdict,
    signal_to_event,
)
from hydra.validation.v7_null_tripwire import NullControl, SyntheticMarketPath


def test_horizon_cost_bucket_is_frozen_and_monotone() -> None:
    assert [horizon_cost_bucket(value) for value in (1, 5, 15, 30, 60, 61)] == [
        "1m",
        "5m",
        "15m",
        "30m",
        "60m",
        "session",
    ]


def test_signal_outcome_uses_exact_cost_and_conservative_path() -> None:
    bars = _bars()
    spec = _spec()
    signal = V7Signal(
        candidate_id=spec.candidate_id,
        hypothesis_id=spec.hypothesis_id,
        market="ES",
        source_market=None,
        session_day=1,
        side=1,
        decision_ns=60_000_000_000,
        availability_ns=60_000_000_000,
        entry_index=1,
        exit_index=3,
        entry_ns=60_000_000_000,
        exit_ns=240_000_000_000,
        contract_code=1,
        segment_code=1,
        feature_snapshot_hash="a" * 64,
    )

    event = signal_to_event(
        signal, spec, bars, load_cost_model(), stress=CostStress.BASE
    )

    # Entry 100, exit 103, ES point value 50 => 150 gross.
    assert event.gross_pnl == 150.0
    assert event.net_pnl < event.gross_pnl
    assert event.worst_unrealized_pnl < 0.0
    assert event.best_unrealized_pnl > event.net_pnl
    assert event.same_bar_ambiguous is True


def test_stressed_cost_never_improves_net_event() -> None:
    bars = _bars()
    spec = _spec()
    signal = V7Signal(
        candidate_id=spec.candidate_id,
        hypothesis_id=spec.hypothesis_id,
        market="ES",
        source_market=None,
        session_day=1,
        side=1,
        decision_ns=60_000_000_000,
        availability_ns=60_000_000_000,
        entry_index=1,
        exit_index=3,
        entry_ns=60_000_000_000,
        exit_ns=240_000_000_000,
        contract_code=1,
        segment_code=1,
        feature_snapshot_hash="a" * 64,
    )
    model = load_cost_model()

    base = signal_to_event(signal, spec, bars, model, stress=CostStress.BASE)
    stress = signal_to_event(
        signal, spec, bars, model, stress=CostStress.STRESS_2X
    )

    assert stress.net_pnl < base.net_pnl
    assert stress.gross_pnl == base.gross_pnl


def test_null_tripwire_verdict_is_preregistered() -> None:
    assert null_ratio_verdict(0.0, 0.0) == (
        "BLOCKED_UNDERPOWERED_GRAMMAR_TRIPWIRE",
        None,
    )
    assert null_ratio_verdict(0.10, 0.09)[0] == (
        "ARTEFACT_GEOMETRY_ONLY_KILL_GRAMMAR"
    )
    verdict, ratio = null_ratio_verdict(0.10, 0.02)
    assert verdict == "GREEN_NULL_ADJUSTED_BASELINE"
    assert ratio == pytest.approx(0.2)


def test_synthetic_return_path_is_rebased_to_positive_tradeable_prices() -> None:
    real = _bars()
    path = SyntheticMarketPath(
        market="ES",
        control=NullControl.VOLATILITY_MATCHED_RANDOM_WALK,
        timestamp_ns=real.timestamp_ns,
        session_day=real.session_day,
        segment_code=real.segment_code,
        close=np.asarray([0.0, -1.0, 1.0, 2.0, -0.5]),
        high=np.asarray([0.5, -0.5, 1.5, 2.5, 0.0]),
        low=np.asarray([-0.5, -1.5, 0.5, 1.5, -1.0]),
        path_hash="d" * 64,
    )

    synthetic = _synthetic_market_bars(real, path)

    assert np.all(synthetic.open > 0.0)
    assert np.all(synthetic.close > 0.0)
    assert np.all(synthetic.high >= np.maximum(synthetic.open, synthetic.close))
    assert np.all(synthetic.low <= np.minimum(synthetic.open, synthetic.close))


def _spec() -> V7CandidateSpec:
    return V7CandidateSpec(
        candidate_id="candidate",
        hypothesis_id="H",
        mechanism_class="class",
        market="ES",
        source_market=None,
        side_relation="same",
        holding_minutes=15,
        economic_hypothesis="hypothesis",
        specification_hash="b" * 64,
    )


def _bars() -> V7MarketBars:
    timestamp = np.arange(5, dtype=np.int64) * 60_000_000_000
    return V7MarketBars(
        market="ES",
        timestamp_ns=timestamp,
        decision_ns=timestamp + 60_000_000_000,
        availability_ns=timestamp + 60_000_000_000,
        session_day=np.ones(5, dtype=np.int32),
        contract_code=np.ones(5, dtype=np.int16),
        segment_code=np.ones(5, dtype=np.int64),
        open=np.asarray([99.0, 100.0, 101.0, 102.0, 103.0]),
        high=np.asarray([100.0, 101.0, 104.0, 105.0, 104.0]),
        low=np.asarray([98.0, 99.0, 98.0, 101.0, 102.0]),
        close=np.asarray([99.5, 100.5, 102.0, 103.0, 103.5]),
        local_minute=np.asarray([500, 501, 502, 503, 504], dtype=np.int16),
        local_weekday=np.zeros(5, dtype=np.int8),
        bundle_hash="c" * 64,
    )
