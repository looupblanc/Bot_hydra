from __future__ import annotations

import pytest

from hydra.factory.ecology_allocation import (
    EcologyAllocationPolicy,
    feasible_ecology_quotas,
)
from hydra.factory.elite_selection_manifest import build_elite_selection_manifest
from hydra.factory.quality_diversity_selector_v2 import (
    SelectorLeakageError,
    SelectorV2Policy,
    select_quality_diversity_elites_v2,
)


FAMILIES = (
    "market_state_geometry",
    "distributional_risk_hazard",
    "volatility_state_transition",
    "invariant_price_state",
    "participation_state_transition",
)


def _candidate(
    index: int,
    ecology: str,
    *,
    market: str | None = None,
    passed: bool = True,
) -> dict[str, object]:
    default_market = {
        "equity_indices": ("ES", "NQ", "RTY", "YM")[index % 4],
        "energy": "CL",
        "metals": "GC",
    }[ecology]
    return {
        "candidate_id": f"candidate_{ecology}_{index:03d}",
        "lineage_id": f"lineage_{ecology}_{index:03d}",
        "structural_fingerprint": f"fingerprint_{ecology}_{index:03d}",
        "market_ecology": ecology,
        "market": market or default_market,
        "mechanism_family": FAMILIES[index % len(FAMILIES)],
        "profile": f"profile_{index % 5}",
        "portfolio_role": "trend" if index % 2 else "reversal",
        "stage1_pass": passed,
        "discovery": {
            "events": 20 + index,
            "net_pnl": 1000.0 - index,
            "cost_stress_1_5x_net": 800.0 - index,
            "maximum_drawdown": 100.0 + index,
            "best_positive_event_share": 0.10 + index / 10_000,
            "finite": True,
        },
    }


def test_one_surviving_ecology_is_feasible_and_fills_maximum() -> None:
    survivors = [_candidate(index, "equity_indices") for index in range(30)]
    result = select_quality_diversity_elites_v2(survivors)

    assert len(result.elites) == 20
    assert result.audit["maximum_feasible_achieved"]
    assert result.audit["ecology_counts"] == {"equity_indices": 20}


def test_two_ecologies_redistribute_sparse_unused_quota() -> None:
    survivors = [_candidate(index, "equity_indices") for index in range(30)]
    survivors += [_candidate(100 + index, "energy") for index in range(3)]
    result = select_quality_diversity_elites_v2(survivors)

    assert len(result.elites) == 20
    assert result.audit["ecology_counts"]["energy"] == 3
    assert result.audit["ecology_counts"]["equity_indices"] == 17
    assert result.audit["maximum_feasible_achieved"]


def test_no_metal_survivors_keeps_metal_controls_separate() -> None:
    survivors = [_candidate(index, "equity_indices") for index in range(74)]
    survivors += [_candidate(200 + index, "energy") for index in range(13)]
    failures = [_candidate(400 + index, "metals", passed=False) for index in range(8)]
    result = select_quality_diversity_elites_v2(
        survivors, failed_candidates=failures
    )

    assert len(result.elites) == 20
    assert result.audit["ecology_counts"] == {"energy": 5, "equity_indices": 15}
    assert len(result.negative_controls) == 2
    assert all(item["market_ecology"] == "metals" for item in result.negative_controls)
    assert not ({item["candidate_id"] for item in result.elites} & {item["candidate_id"] for item in result.negative_controls})
    assert not result.audit["negative_controls_count_as_elites"]


def test_quota_allocator_never_requires_missing_ecology() -> None:
    candidates = [_candidate(index, "energy") for index in range(7)]
    quotas = feasible_ecology_quotas(candidates, EcologyAllocationPolicy(maximum_elites=20))

    assert quotas == {"energy": 7}
    assert "metals" not in quotas
    assert "equity_indices" not in quotas


def test_selection_is_deterministic_and_maximum_feasible() -> None:
    survivors = [_candidate(index, "equity_indices") for index in range(9)]
    survivors += [_candidate(100 + index, "energy") for index in range(6)]
    first = select_quality_diversity_elites_v2(survivors)
    second = select_quality_diversity_elites_v2(list(reversed(survivors)))

    assert [item["candidate_id"] for item in first.elites] == [
        item["candidate_id"] for item in second.elites
    ]
    assert len(first.elites) == 15
    assert first.audit["maximum_feasible_achieved"]


def test_duplicate_fingerprint_cannot_inflate_elites() -> None:
    survivors = [_candidate(index, "equity_indices") for index in range(5)]
    duplicate = dict(survivors[0])
    duplicate["candidate_id"] = "renamed_clone"
    duplicate["lineage_id"] = "renamed_clone_lineage"
    result = select_quality_diversity_elites_v2([*survivors, duplicate])

    assert len(result.elites) == 5
    assert result.audit["unique_eligible_survivors"] == 5


def test_selector_rejects_any_hidden_2024_or_validation_field() -> None:
    candidate = _candidate(1, "equity_indices")
    candidate["validation_net_2024"] = 999999.0

    with pytest.raises(SelectorLeakageError):
        select_quality_diversity_elites_v2([candidate])


def test_manifest_freezes_elites_controls_and_no_future_use() -> None:
    survivors = [_candidate(index, "equity_indices") for index in range(20)]
    failures = [_candidate(100 + index, "metals", passed=False) for index in range(3)]
    result = select_quality_diversity_elites_v2(survivors, failed_candidates=failures)
    manifest = build_elite_selection_manifest(
        result,
        population_hash="a" * 64,
        selector_task_sha256="b" * 64,
    )

    assert len(manifest["selected_candidate_ids"]) == 20
    assert len(manifest["negative_control_ids"]) == 2
    assert not manifest["negative_controls_promotion_eligible"]
    assert not manifest["uses_2024_results"]
    assert len(manifest["selection_manifest_hash"]) == 64


def test_soft_caps_relax_only_to_reach_maximum_feasible_set() -> None:
    survivors = [_candidate(index, "energy", market="CL") for index in range(20)]
    policy = SelectorV2Policy(maximum_elites=20)
    result = select_quality_diversity_elites_v2(survivors, policy=policy)

    assert len(result.elites) == 20
    assert result.audit["soft_cap_relaxations"]
    assert result.audit["maximum_feasible_achieved"]
