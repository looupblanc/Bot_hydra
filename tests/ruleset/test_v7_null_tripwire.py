from __future__ import annotations

import gzip
import hashlib
import json

import numpy as np

from hydra.account_policy.basket import RoutedTrade
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.validation.v7_null_tripwire import (
    NullControl,
    SyntheticMarketPath,
    block_shuffle_source_days,
    build_synthetic_market_path,
    null_verdict,
    rebuild_counterfactual_trade,
    year_permutation_source_days,
)
from hydra.validation.v7_null_tripwire import _write_result


def test_daily_block_shuffle_is_deterministic_nonidentity_and_block_preserving() -> None:
    days = np.arange(40, dtype=np.int64)
    first = block_shuffle_source_days(
        days, block_size=5, rng=np.random.default_rng(71001)
    )
    second = block_shuffle_source_days(
        days, block_size=5, rng=np.random.default_rng(71001)
    )

    assert np.array_equal(first, second)
    assert not np.array_equal(first, days)
    assert sorted(first.tolist()) == days.tolist()
    assert all(
        np.all(np.diff(first[index : index + 5]) == 1)
        for index in range(0, len(first), 5)
    )


def test_year_permutation_rotates_whole_year_blocks_without_dropping_days() -> None:
    days = np.asarray(
        [
            int(np.datetime64(value, "D").astype(np.int64))
            for value in (
                "2023-01-03",
                "2023-01-04",
                "2024-01-02",
                "2024-01-03",
            )
        ]
    )

    result = year_permutation_source_days(days)

    assert result.tolist() == [days[2], days[3], days[0], days[1]]
    assert sorted(result.tolist()) == sorted(days.tolist())


def test_counterfactual_trade_preserves_cost_and_frozen_signal_timestamps() -> None:
    timestamp = np.asarray([1, 61, 121, 181], dtype=np.int64) * 1_000_000_000
    path = SyntheticMarketPath(
        market="ES",
        control=NullControl.VOLATILITY_MATCHED_RANDOM_WALK,
        timestamp_ns=timestamp,
        session_day=np.asarray([1, 1, 1, 1]),
        segment_code=np.asarray([7, 7, 7, 7]),
        close=np.asarray([0.0, 1.0, 3.0, 2.0]),
        high=np.asarray([0.5, 1.5, 3.5, 2.5]),
        low=np.asarray([-0.5, 0.5, 2.5, 1.5]),
        path_hash="f" * 64,
    )
    event = TradePathEvent(
        event_id="alpha:0",
        decision_ns=int(timestamp[1]),
        exit_ns=int(timestamp[3] + 60_000_000_000),
        session_day=1,
        net_pnl=80.0,
        gross_pnl=100.0,
        worst_unrealized_pnl=-50.0,
        best_unrealized_pnl=120.0,
        quantity=1,
        mini_equivalent=1.0,
    )
    routed = RoutedTrade("alpha", "ES", 1, event)

    rebuilt = rebuild_counterfactual_trade(
        routed, path, point_value=50.0
    )

    assert rebuilt.event.decision_ns == event.decision_ns
    assert rebuilt.event.exit_ns == event.exit_ns
    assert rebuilt.event.gross_pnl == 50.0
    assert rebuilt.event.net_pnl == 30.0
    assert rebuilt.event.best_unrealized_pnl == 105.0
    assert rebuilt.event.worst_unrealized_pnl == -45.0


def test_null_ratio_threshold_is_frozen_at_point_eight() -> None:
    assert null_verdict(0.5, 0.399)[0] == "GREEN"
    assert null_verdict(0.5, 0.4) == ("ARTEFACT", 0.8)
    assert null_verdict(0.0, 0.0) == ("BLOCKED_UNDEFINED_RATIO", None)


def test_synthetic_market_path_is_seed_deterministic_and_source_immutable() -> None:
    days = np.repeat(np.arange(19000, 19012, dtype=np.int32), 3)
    rows = len(days)
    close = np.arange(rows, dtype=np.float64) + np.sin(np.arange(rows))
    arrays = {
        "timestamp_ns": np.arange(rows, dtype=np.int64) * 60_000_000_000,
        "session_day": days,
        "session_code": np.tile(np.asarray([0, 1, 2], dtype=np.int16), 12),
        "segment_code": days.astype(np.int64),
        "bar_open": close - 0.1,
        "bar_high": close + 0.4,
        "bar_low": close - 0.5,
        "bar_close": close,
    }
    matrix = FeatureMatrix(
        root=None,  # type: ignore[arg-type]
        manifest={"row_count": rows, "bundle_hash": "test"},
        arrays=arrays,
    )
    original = close.copy()

    first = build_synthetic_market_path(
        "ES",
        matrix,
        control=NullControl.DAILY_BLOCK_SHUFFLE,
        seed=71002,
        block_size=2,
    )
    second = build_synthetic_market_path(
        "ES",
        matrix,
        control=NullControl.DAILY_BLOCK_SHUFFLE,
        seed=71002,
        block_size=2,
    )

    assert first.path_hash == second.path_hash
    assert np.array_equal(first.close, second.close)
    assert not np.array_equal(first.close, original)
    assert np.array_equal(arrays["bar_close"], original)


def test_full_object_evidence_is_deterministically_compressed(tmp_path) -> None:
    result = {
        "experiment_id": "test",
        "verdict": "GREEN",
        "NULL_RATIO": 0.1,
        "threshold": 0.8,
        "real": {"pass_rate": 0.5, "episode_count": 2},
        "pooled_null": {"pass_rate": 0.05, "episode_count": 6},
        "controls": {},
        "object_results": {"alpha": {"real": {"pass_rate": 0.5}}},
        "CONTRE": "conditional null",
    }

    persisted = _write_result(result, tmp_path)
    summary = json.loads((tmp_path / "null_tripwire_result.json").read_text())
    compressed = (tmp_path / "null_tripwire_full_evidence.json.gz").read_bytes()
    full = gzip.decompress(compressed)

    assert "object_results" not in summary
    assert summary["full_detail_evidence"]["object_result_count"] == 1
    assert hashlib.sha256(full).hexdigest() == persisted[
        "full_detail_evidence"
    ]["uncompressed_sha256"]
    assert json.loads(full)["object_results"] == result["object_results"]
