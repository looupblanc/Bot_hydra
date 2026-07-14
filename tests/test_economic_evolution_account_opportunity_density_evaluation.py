from __future__ import annotations

from dataclasses import dataclass

from hydra.economic_evolution.account_opportunity_density_evaluation import (
    _build_signal_index,
)


@dataclass(frozen=True)
class _Event:
    decision_ns: int


@dataclass(frozen=True)
class _Trade:
    market: str
    side: int
    event: _Event


def test_signal_index_is_chronological_and_contains_no_outcomes() -> None:
    index = _build_signal_index(
        {
            "a": (
                _Trade("ES", 1, _Event(20)),
                _Trade("ES", -1, _Event(10)),
            )
        }
    )
    times, observations = index["a"]
    assert times == (10, 20)
    assert tuple(row.side for row in observations) == (-1, 1)
    assert all(not hasattr(row, "net_pnl") for row in observations)
