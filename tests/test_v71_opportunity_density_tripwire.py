from __future__ import annotations

import pytest

from hydra.validation.v71_opportunity_density_tripwire import classify_tripwire


def test_opportunity_density_tripwire_decision_is_frozen() -> None:
    verdict, ratio = classify_tripwire(
        real_passes=8,
        real_episodes=160,
        null_passes=10,
        null_episodes=480,
    )
    assert verdict == "GREEN_NULL_ADJUSTED_BASELINE"
    assert ratio == pytest.approx(5.0 / 12.0)

    verdict, ratio = classify_tripwire(
        real_passes=5,
        real_episodes=100,
        null_passes=4,
        null_episodes=100,
    )
    assert verdict == "ARTEFACT_GEOMETRY_ONLY"
    assert ratio == pytest.approx(0.8)

    assert classify_tripwire(
        real_passes=0,
        real_episodes=100,
        null_passes=0,
        null_episodes=300,
    ) == ("BLOCKED_UNDERPOWERED", None)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"real_passes": 1, "real_episodes": 0, "null_passes": 1, "null_episodes": 1},
        {"real_passes": 2, "real_episodes": 1, "null_passes": 1, "null_episodes": 1},
        {"real_passes": 1, "real_episodes": 1, "null_passes": 2, "null_episodes": 1},
    ],
)
def test_opportunity_density_tripwire_rejects_invalid_counts(
    kwargs: dict[str, int],
) -> None:
    with pytest.raises(ValueError):
        classify_tripwire(**kwargs)
