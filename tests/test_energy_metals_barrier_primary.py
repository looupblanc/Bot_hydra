from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hydra.research.barrier_hazard_primary import generate_barrier_hypotheses
from hydra.research.energy_metals_barrier_primary import (
    EnergyMetalsBarrierPrimaryError,
    _read_period,
    generate_energy_metals_hypotheses,
)


def test_energy_metals_population_is_exact_fresh_and_diverse() -> None:
    population = generate_energy_metals_hypotheses()
    predecessor = [
        item
        for item in generate_barrier_hypotheses()
        if item["market"] in {"CL", "GC"}
    ]

    assert len(population) == 48
    assert len({item["candidate_id"] for item in population}) == 48
    assert len({item["structural_fingerprint"] for item in population}) == 48
    assert {item["market"] for item in population} == {"CL", "GC"}
    assert {item["execution_market"] for item in population} == {"MCL", "MGC"}
    assert {item["market_ecology"] for item in population} == {"energy", "metals"}
    assert {item["mechanism_family"] for item in population} == {
        "accepted_location_hazard",
        "range_expansion_hazard",
        "path_curvature_hazard",
        "extreme_recovery_hazard",
    }
    assert not (
        {item["structural_fingerprint"] for item in population}
        & {item["structural_fingerprint"] for item in predecessor}
    )


def test_period_reader_excludes_confirmation_from_early_selection(
    tmp_path: Path,
) -> None:
    path = tmp_path / "development.parquet"
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2023-12-29T12:00:00Z", "2024-01-02T12:00:00Z"], utc=True
            ),
            "symbol": ["GC", "GC"],
            "timeframe": ["1m", "1m"],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.05, 2.05],
            "volume": [10, 20],
            "session_id": ["2023-12-29", "2024-01-02"],
        }
    )
    frame.to_parquet(path, index=False)

    early = _read_period(path, {"GC"}, "2024-01-01")

    assert len(early) == 1
    assert early.iloc[0]["timestamp"] < pd.Timestamp("2024-01-01", tz="UTC")
    with pytest.raises(EnergyMetalsBarrierPrimaryError):
        _read_period(path, {"MGC"}, "2024-01-01")
