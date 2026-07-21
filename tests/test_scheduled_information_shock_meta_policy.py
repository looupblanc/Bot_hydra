from __future__ import annotations

from copy import deepcopy

from hydra.research.scheduled_information_shock_meta_policy import (
    _trailing_normalize,
    frozen_specs,
)


def _row(index: int, value: float) -> dict[str, object]:
    return {
        "event_id": f"event-{index}",
        "ecology": "EIA_NG_STORAGE",
        "timestamp": f"2020-01-{index + 1:02d}T00:00:00Z",
        "surprise_raw": value,
        "liquidity_raw": value + 1.0,
        "volatility_raw": value + 2.0,
        "response_raw": value + 3.0,
    }


def test_frozen_meta_policy_lattice_is_bounded_and_unique() -> None:
    specs = frozen_specs()

    assert len(specs) == 24
    assert len({spec.policy_id for spec in specs}) == 24


def test_trailing_normalization_cannot_see_future_event() -> None:
    baseline = [_row(index, float(index + 1)) for index in range(8)]
    changed = deepcopy(baseline)
    changed[-1]["surprise_raw"] = 1_000_000.0

    normalized = _trailing_normalize(baseline)
    normalized_changed = _trailing_normalize(changed)

    for left, right in zip(normalized[:-1], normalized_changed[:-1], strict=True):
        assert left["surprise_z"] == right["surprise_z"]
    assert normalized[5]["feature_ready"] is False
    assert normalized[6]["feature_ready"] is True
