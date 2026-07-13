from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from hydra.economic_evolution.density_diversification import (
    generate_density_diversification_population,
)
from hydra.economic_evolution.schema import (
    AccountPolicyGenome,
    FailureDimension,
    stable_hash,
)
from hydra.research.economic_evolution_density_campaign import (
    DensityDiversificationCampaignError,
    _binomial_tail,
    _component_pass,
    _family_tripwire,
    _temporal_blocks,
    _without,
    load_and_verify_density_preregistration,
    load_and_verify_density_result,
)

from tests.test_economic_evolution_density_diversification import _seed


def _runtime(*, event_count: int, normal: float, stress: float, concentration: float = 0.1):
    return SimpleNamespace(
        event_count=event_count,
        net_pnl=normal,
        cost_stress_1_5x_net=stress,
        best_positive_event_share=concentration,
        maximum_drawdown=1_000.0,
    )


def _gate() -> dict:
    return {
        "minimum_events": 24,
        "maximum_best_positive_event_share": 0.35,
        "maximum_drawdown_usd": 4_500.0,
        "maximum_null_ratio": 0.8,
        "net_evidence_p_value": 0.05,
    }


def test_family_tripwire_reports_raw_counts_ratio_and_exact_p_value() -> None:
    population = generate_density_diversification_population(
        _seed(),
        campaign_id="density-tripwire-test",
        excluded_source_sleeve_ids=(),
        maximum_sources=6,
        maximum_sources_per_market=2,
        maximum_sources_per_market_session=1,
        maximum_sources_per_market_mechanism=1,
        policy_count=6,
    )
    runtimes = {}
    for row in population.real_sleeves:
        runtimes[row.sleeve_id] = _runtime(event_count=40, normal=100, stress=75)
    for index, row in enumerate(population.matched_null_sleeves):
        runtimes[row.sleeve_id] = _runtime(
            event_count=40,
            normal=100 if index == 0 else -10,
            stress=75 if index == 0 else -20,
        )

    result = _family_tripwire(population, runtimes, _gate())
    assert result["real_pass_count"] == 6
    assert result["null_pass_count"] == 1
    assert result["NULL_RATIO"] == 1 / 6
    assert result["family_green"] is True
    assert 0.0 <= result["exact_one_sided_binomial_p_value"] <= 1.0
    assert result["thresholds_changed_after_outcome"] is False


def test_family_tripwire_keeps_frozen_denominator_when_exact_replay_is_missing() -> None:
    population = generate_density_diversification_population(
        _seed(),
        campaign_id="density-tripwire-missing-test",
        excluded_source_sleeve_ids=(),
        maximum_sources=6,
        maximum_sources_per_market=2,
        maximum_sources_per_market_session=1,
        maximum_sources_per_market_mechanism=1,
        policy_count=6,
    )
    runtimes = {
        row.sleeve_id: _runtime(event_count=40, normal=100, stress=75)
        for row in population.real_sleeves[:-1]
    }
    result = _family_tripwire(population, runtimes, _gate())

    assert result["real_pass_count"] == 5
    assert result["real_candidate_count"] == 6
    assert result["real_exact_replay_missing_count"] == 1
    assert result["null_candidate_count"] == 6
    assert result["null_exact_replay_missing_count"] == 6
    assert result["real_pass_rate"] == 5 / 6
    assert result["null_pass_rate"] == 0.0


def test_component_gate_requires_stressed_economics_and_concentration() -> None:
    gate = _gate()
    assert _component_pass(_runtime(event_count=30, normal=10, stress=5), gate)
    assert not _component_pass(_runtime(event_count=20, normal=10, stress=5), gate)
    assert not _component_pass(_runtime(event_count=30, normal=10, stress=-1), gate)
    assert not _component_pass(
        _runtime(event_count=30, normal=10, stress=5, concentration=0.5), gate
    )


def test_leave_one_out_control_keeps_frozen_risk_rules() -> None:
    policy = AccountPolicyGenome(
        policy_id="density-policy",
        sleeve_ids=("a", "b", "c"),
        allocation_units=(2, 1, 1),
        maximum_simultaneous_positions=3,
        maximum_mini_equivalent=10,
        conflict_policy="FIXED_PRIORITY",
        daily_risk_budget=1_000.0,
        daily_profit_lock=2_250.0,
        low_mll_buffer=3_000.0,
        critical_mll_buffer=1_500.0,
        loss_streak_throttle_after=3,
        mode="COMBINE_RESEARCH",
        source_campaign="density-0007",
        mutation_target=FailureDimension.INSUFFICIENT_STATISTICAL_POWER,
    )
    control = _without(policy, "a")
    assert control.sleeve_ids == ("b", "c")
    assert control.allocation_units == (1, 1)
    assert control.maximum_simultaneous_positions == 2
    assert control.daily_risk_budget == policy.daily_risk_budget
    assert control.daily_profit_lock == policy.daily_profit_lock
    assert control.parent_policy_ids == (policy.policy_id,)


def test_temporal_blocks_are_contiguous_and_complete() -> None:
    episodes = [
        SimpleNamespace(
            start_day=100 + index,
            net_pnl=float(index - 3),
            target_progress=float(index) / 10,
            mll_breached=index == 7,
            consistency_ok=index % 2 == 0,
        )
        for index in range(8)
    ]
    blocks = _temporal_blocks(tuple(reversed(episodes)), count=4)
    assert len(blocks) == 4
    assert [row["start_count"] for row in blocks] == [2, 2, 2, 2]
    assert [row["first_start_day"] for row in blocks] == [100, 102, 104, 106]
    assert blocks[-1]["mll_breach_rate"] == 0.5


def test_preregistration_and_result_verifiers_fail_closed(tmp_path: Path) -> None:
    (tmp_path / "MISSION_CONTRACT.md").write_text("contract", encoding="utf-8")
    implementation = tmp_path / "implementation.py"
    implementation.write_text("VALUE = 1\n", encoding="utf-8")
    digest = __import__("hashlib").sha256(implementation.read_bytes()).hexdigest()
    prereg = {
        "schema": "hydra_density_diversification_preregistration_v1",
        "campaign_id": "hydra_economic_evolution_density_diversification_0007",
        "class_id": "INDEPENDENT_OPPORTUNITY_DENSITY_CONSISTENCY_ASSEMBLY_V1",
        "implementation_files": {"implementation.py": digest},
        "q4_access_allowed": False,
        "new_data_purchase_allowed": False,
        "broker_or_orders_allowed": False,
    }
    prereg["preregistration_hash"] = stable_hash(prereg)
    path = tmp_path / "prereg.json"
    path.write_text(json.dumps(prereg), encoding="utf-8")
    assert load_and_verify_density_preregistration(path)["campaign_id"].endswith("0007")
    implementation.write_text("VALUE = 2\n", encoding="utf-8")
    try:
        load_and_verify_density_preregistration(path)
    except DensityDiversificationCampaignError as exc:
        assert "checksum drift" in str(exc)
    else:
        raise AssertionError("implementation drift was accepted")

    result = {
        "campaign_id": prereg["campaign_id"],
        "validated": False,
        "governance": {
            "proof_windows_consumed": 0,
            "new_data_purchase_count": 0,
            "q4_access_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "outbound_order_capability": False,
        },
    }
    result["result_sha256"] = stable_hash(result)
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(result), encoding="utf-8")
    assert load_and_verify_density_result(result_path, prereg)["validated"] is False
    result["governance"]["orders"] = 1
    result["result_sha256"] = stable_hash(
        {key: value for key, value in result.items() if key != "result_sha256"}
    )
    result_path.write_text(json.dumps(result), encoding="utf-8")
    try:
        load_and_verify_density_result(result_path, prereg)
    except DensityDiversificationCampaignError as exc:
        assert "governance drift" in str(exc)
    else:
        raise AssertionError("order-capable result was accepted")


def test_exact_binomial_tail_boundaries() -> None:
    assert _binomial_tail(0, 10, 0.5) == 1.0
    assert _binomial_tail(1, 10, 0.0) == 0.0
    assert _binomial_tail(10, 10, 1.0) == 1.0
