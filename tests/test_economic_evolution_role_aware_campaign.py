from __future__ import annotations

from hydra.research.economic_evolution_role_aware_campaign import (
    role_aware_final_result,
    role_aware_paired_tripwire,
)


def _tripwire_gate() -> dict:
    return {
        "minimum_policy_pairs": 64,
        "minimum_informative_pairs": 48,
        "minimum_stressed_median_net_delta_usd": 100.0,
        "minimum_stressed_target_progress_delta": 0.01,
        "maximum_stressed_mll_breach_rate_deterioration": 0.02,
        "maximum_stressed_consistency_pass_rate_deterioration": 0.05,
        "maximum_NULL_RATIO": 0.8,
        "exact_one_sided_binomial_p_value": 0.05,
    }


def _pair_row(
    *, real_win: bool, index: int = 0, consistency_delta: float = 0.0
) -> dict:
    direction = 1.0 if real_win else -1.0
    return {
        "pair_id": f"pair-{index}",
        "paired_delta": {
            "normal_median_net_usd": direction * 150.0,
            "stressed_median_net_usd": direction * 150.0,
            "normal_target_progress": direction * 0.02,
            "stressed_target_progress": direction * 0.02,
            "normal_mll_breach_rate": 0.0,
            "stressed_mll_breach_rate": 0.0,
            "normal_consistency_pass_rate": consistency_delta,
            "stressed_consistency_pass_rate": consistency_delta,
        },
    }


def test_role_aware_tripwire_requires_same_membership_account_enrichment() -> None:
    rows = [_pair_row(real_win=index < 90, index=index) for index in range(100)]
    result = role_aware_paired_tripwire(rows, _tripwire_gate())
    assert result["real_win_count"] == 90
    assert result["matched_control_win_count"] == 10
    assert result["NULL_RATIO"] == 1 / 9
    assert result["median_stressed_net_delta_usd"] == 150.0
    assert result["median_stressed_target_progress_delta"] == 0.02
    assert result["family_green"] is True
    assert result["evidence_strength"] == "VERT_NET"


def test_role_aware_tripwire_rejects_control_dominance() -> None:
    rows = [_pair_row(real_win=index < 40, index=index) for index in range(100)]
    result = role_aware_paired_tripwire(rows, _tripwire_gate())
    assert result["real_win_count"] == 40
    assert result["matched_control_win_count"] == 60
    assert result["NULL_RATIO"] == 1.5
    assert result["family_green"] is False
    assert result["verdict"] == "ARTEFACT_GEOMETRY_ONLY"


def test_role_aware_tripwire_enforces_consistency_deterioration() -> None:
    rows = [
        _pair_row(
            real_win=index < 90,
            index=index,
            consistency_delta=-0.10 if index < 90 else 0.10,
        )
        for index in range(100)
    ]
    result = role_aware_paired_tripwire(rows, _tripwire_gate())
    assert result["real_win_count"] == 0
    assert result["family_green"] is False


def _summary(*, net: float, progress: float, passes: int = 0) -> dict:
    return {
        "episode_start_count": 24,
        "pass_count": passes,
        "pass_rate": passes / 24,
        "target_progress_median": progress,
        "maximum_target_progress": max(progress, 0.0),
        "mll_breach_rate": 0.0,
        "median_episode_net_pnl": net,
        "consistency_pass_rate": 0.75,
        "compliance_failure_count": 0,
    }


def _complete_pair_row() -> dict:
    real = _summary(net=500.0, progress=0.25, passes=1)
    control = _summary(net=100.0, progress=0.1)
    return {
        **_pair_row(real_win=True),
        "real_policy": {
            "policy_id": "real-1",
            "structural_fingerprint": "real-structure-1",
        },
        "matched_control_policy": {
            "policy_id": "control-1",
            "structural_fingerprint": "control-structure-1",
        },
        "behavioral_fingerprint": "behavior-1",
        "real_evaluation": {
            "controlled_base": dict(real),
            "controlled_stress_1_5x": dict(real),
        },
        "matched_control_evaluation": {
            "controlled_base": dict(control),
            "controlled_stress_1_5x": dict(control),
        },
        "real_positive_temporal_block_count": 3,
        "real_maximum_positive_component_share": 0.5,
    }


def test_final_result_keeps_development_evidence_non_promotional() -> None:
    prereg = {
        "campaign_id": (
            "hydra_economic_evolution_role_aware_account_allocator_0010"
        ),
        "account_gate": {
            "minimum_normal_median_net_usd": 0.0,
            "minimum_stressed_median_net_usd": 0.0,
            "minimum_median_target_progress": 0.15,
            "maximum_mll_breach_rate": 0.2,
            "minimum_consistency_pass_rate": 0.5,
            "minimum_positive_temporal_blocks": 2,
            "maximum_positive_component_share": 0.65,
            "minimum_matched_control_net_delta_usd": 100.0,
            "minimum_combine_path_pass_count": 1,
        },
        "multiplicity": {"reserved_delta_trials": 3_600},
    }
    result = role_aware_final_result(
        prereg,
        population_summary={"manifest_hash": "manifest", "real_policy_count": 1},
        screen_summary={"survivor_count": 1},
        exact_runtime_count=8,
        exact_failure_count=0,
        pair_rows=[_complete_pair_row()],
        starts=tuple(range(24)),
        tripwire={
            "family_green": True,
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "evidence_strength": "VERT_NET",
        },
        elapsed_seconds=2.0,
        phase_seconds={"paired_account_replay": 1.5},
    )
    assert result["combine_path_diagnostic_count"] == 1
    assert result["pre_holdout_ready_count"] == 0
    assert result["paper_shadow_ready_count"] == 0
    assert result["governance"]["proof_windows_consumed"] == 0
    assert result["governance"]["new_data_purchase_count"] == 0
    assert result["governance"]["orders"] == 0
