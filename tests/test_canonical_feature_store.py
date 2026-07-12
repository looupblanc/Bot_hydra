from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from hydra.features.canonical_store import CanonicalFeatureKey, CanonicalFeatureStore
from hydra.features.mtf_cache import build_closed_mtf_cache
from hydra.research.turbo_feature_builder import _session_days


def _key(source: str = "a" * 64) -> CanonicalFeatureKey:
    return CanonicalFeatureKey(
        market="ES",
        explicit_contract_scope="ESH3",
        start_inclusive="2023-01-01",
        end_exclusive="2023-07-01",
        source_data_sha256=source,
        roll_map_hash="b" * 64,
        transformation_version="turbo_features_v2",
        feature_dag_hash="c" * 64,
        timeframes=("1m", "5m", "15m", "30m", "60m", "session", "daily"),
    )


def test_feature_store_is_content_addressed_atomic_and_mmap_read_only(tmp_path):
    store = CanonicalFeatureStore(tmp_path)
    arrays = {"feature": np.arange(8, dtype=np.float64), "side": np.ones(8, dtype=np.int8)}
    first = store.put(_key(), arrays, provenance={"q4_access": 0})
    second = store.put(_key(), arrays, provenance={"q4_access": 0})
    assert not first.cache_hit
    assert second.cache_hit
    assert first.bundle_hash == second.bundle_hash
    matrix = store.get(_key())
    assert matrix is not None and matrix.row_count == 8
    assert not matrix.array("feature").flags.writeable
    with pytest.raises(ValueError):
        matrix.array("feature")[0] = 99.0
    assert not list(tmp_path.glob(".*"))


def test_feature_store_invalidates_on_source_hash(tmp_path):
    store = CanonicalFeatureStore(tmp_path)
    arrays = {"feature": np.arange(3, dtype=np.float32)}
    left = store.put(_key("a" * 64), arrays, provenance={})
    right = store.put(_key("d" * 64), arrays, provenance={})
    assert left.path != right.path


def test_feature_store_detects_manifest_tampering(tmp_path):
    store = CanonicalFeatureStore(tmp_path)
    result = store.put(_key(), {"x": np.arange(4)}, provenance={})
    manifest = result.path / "manifest.json"
    payload = json.loads(manifest.read_text())
    payload["row_count"] = 9
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="hash"):
        store.get(_key())


def test_closed_mtf_cache_exposes_only_completed_bars():
    timestamps = pd.date_range("2024-01-02T14:30:00Z", periods=7, freq="min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": "ES",
            "active_contract": "ESH4",
            "trading_session_id": "2024-01-02",
            "open": np.arange(7, dtype=float),
            "high": np.arange(7, dtype=float) + 1,
            "low": np.arange(7, dtype=float) - 1,
            "close": np.arange(7, dtype=float) + 0.5,
            "volume": 1.0,
        }
    )
    bundle = build_closed_mtf_cache(frame)
    assert set(("1m", "5m", "15m", "30m", "60m", "session", "daily")) <= set(bundle.frames)
    assert (bundle.frames["5m"]["availability_timestamp"] <= timestamps[-1] + pd.Timedelta(minutes=1)).all()
    # The 14:35--14:40 bar is still incomplete at the 14:37 as-of cutoff.
    assert len(bundle.frames["5m"]) == 1


def test_session_day_encoding_is_independent_of_pandas_timestamp_resolution():
    values = pd.Series(["2023-01-02", "2024-09-30"])
    encoded = _session_days(values)
    assert encoded.tolist() == [
        int(np.datetime64("2023-01-02", "D").astype(np.int64)),
        int(np.datetime64("2024-09-30", "D").astype(np.int64)),
    ]
