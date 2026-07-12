from __future__ import annotations

import pytest

from hydra.validation.v7_tripwire_evidence import (
    V7TripwireEvidenceError,
    exact_tripwire_evidence,
)


def test_exact_tripwire_publishes_raw_counts_and_thin_green() -> None:
    evidence = exact_tripwire_evidence(
        real_passes=8,
        real_episodes=160,
        null_passes=10,
        null_episodes=480,
        tripwire_verdict="GREEN_NULL_ADJUSTED_BASELINE",
    )

    assert evidence.real_passes == 8
    assert evidence.real_episodes == 160
    assert evidence.null_passes == 10
    assert evidence.null_episodes == 480
    assert evidence.exact_binomial_one_sided_p_value == pytest.approx(
        0.019602241567404557
    )
    assert evidence.evidence_strength == "VERT_MINCE"


def test_exact_tripwire_labels_clear_green_without_changing_ratio_gate() -> None:
    evidence = exact_tripwire_evidence(
        real_passes=9144,
        real_episodes=25680,
        null_passes=1359,
        null_episodes=77040,
        tripwire_verdict="GREEN_NULL_ADJUSTED_BASELINE",
    )

    assert evidence.exact_binomial_one_sided_p_value < 1.0e-100
    assert evidence.evidence_strength == "VERT_NET"


def test_tripwire_rejects_invalid_counts() -> None:
    with pytest.raises(V7TripwireEvidenceError):
        exact_tripwire_evidence(
            real_passes=2,
            real_episodes=1,
            null_passes=0,
            null_episodes=1,
            tripwire_verdict="GREEN_NULL_ADJUSTED_BASELINE",
        )
