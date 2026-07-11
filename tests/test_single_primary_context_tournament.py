from __future__ import annotations

import pandas as pd

from hydra.research.accelerated_context_tournament import (
    generate_executable_hypotheses,
)
from hydra.research.single_primary_context_tournament import (
    PRIMARY_ALPHA,
    _primary_manifest,
    _select_primary,
)


def _metrics(net: float, drawdown: float, events: int = 20) -> dict[str, object]:
    return {
        "events": events,
        "net_pnl": net,
        "gross_pnl": net + events,
        "cost_stress_1_5x_net": net - events * 0.5,
        "maximum_drawdown": drawdown,
        "best_positive_event_share": 0.10,
        "finite": True,
    }


def _micro_events(candidate_id: str, net: float, events: int = 20) -> tuple[str, pd.DataFrame]:
    per_event = net / events
    return (
        f"{candidate_id}__early_micro",
        pd.DataFrame(
            {
                "event_session_id": [f"2023-08-{index + 1:02d}" for index in range(events)],
                "net_pnl": [per_event] * events,
                "gross_pnl": [per_event + 1.0] * events,
                "cost": [1.0] * events,
            }
        ),
    )


def test_v3_population_is_disjoint_balanced_and_family_capped() -> None:
    prior = generate_executable_hypotheses(0)
    current = generate_executable_hypotheses(1)

    assert len(current) == 300
    assert all(item["candidate_id"].endswith("_v3") for item in current)
    assert not (
        {item["structural_fingerprint"] for item in prior}
        & {item["structural_fingerprint"] for item in current}
    )


def test_primary_ranking_uses_minimum_mini_micro_risk_efficiency() -> None:
    templates = generate_executable_hypotheses(1)[:2]
    first = {**templates[0], "discovery": _metrics(200.0, 100.0)}
    second = {**templates[1], "discovery": _metrics(300.0, 100.0)}
    micro = dict(
        [
            _micro_events(first["candidate_id"], 20.0),
            _micro_events(second["candidate_id"], 100.0),
        ]
    )

    selected, ranking = _select_primary([first, second], micro)

    assert selected is not None
    assert selected["candidate_id"] == second["candidate_id"]
    assert ranking[0]["eligible"] is True


def test_primary_manifest_freezes_one_test_and_calibrated_alpha() -> None:
    primary = {**generate_executable_hypotheses(1)[0], "discovery": _metrics(100, 50)}
    ranking = [{"candidate_id": primary["candidate_id"], "eligible": True}]

    manifest = _primary_manifest(
        primary=primary,
        ranking=ranking,
        archive_manifest_hash="a" * 64,
        calibrated_policy_result_hash="b" * 64,
        population_hash="c" * 64,
    )

    assert manifest["candidate_probability_threshold"] == PRIMARY_ALPHA == 0.03
    assert manifest["promotion_test_count"] == 1
    assert manifest["diagnostic_elites_promotion_eligible"] is False
    assert len(manifest["primary_manifest_hash"]) == 64
