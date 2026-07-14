from __future__ import annotations

import copy
import random

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.halving import (
    ProductionHalvingError,
    aggregate_policy_evidence,
    build_compact_outputs,
    build_final_result_payload,
    build_leave_one_block_out_plan,
    complete_leave_one_block_out,
    development_decision,
    pareto_select,
    select_stage4_survivors,
    select_stage5_survivors,
)


BLOCKS = ("B1", "B2", "B3", "B4")


def _policy(
    policy_id: str,
    sleeves: tuple[str, ...],
    *,
    risk: float = 1.0,
    micro: int = 4,
    behavior: str | None = None,
) -> dict[str, object]:
    return {
        "policy_id": policy_id,
        "mechanism": "FIXED_STATIC_RISK_FRONTIER",
        "sleeve_ids": list(sleeves),
        "component_priority": list(sleeves),
        "risk_level": risk,
        "risk_micro_units": micro,
        "maximum_simultaneous_positions": min(2, len(sleeves)),
        "maximum_mini_equivalent": 15,
        "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        "route_parameters": [],
        "parent_policy_ids": [],
        "structural_fingerprint": stable_hash({"policy": policy_id}),
        "behavioral_fingerprint": behavior or stable_hash({"behavior": policy_id}),
        "source_campaign": "campaign_0024",
        "development_only": True,
        "validated": False,
    }


def _episode(
    policy_id: str,
    block: str,
    scenario: str,
    index: int,
    *,
    terminal: str = "TARGET_REACHED",
    net: float = 100.0,
    progress: float = 1.0,
    attribution: dict[str, float] | None = None,
    contribution_field: bool = False,
) -> dict[str, object]:
    row: dict[str, object] = {
        "campaign_id": "campaign_0024",
        "policy_id": policy_id,
        "episode_id": f"{policy_id}:{block}:{index}",
        "episode_start": "2024-01-01T00:00:00Z",
        "horizon": "60_TRADING_DAYS",
        "temporal_block": block,
        "duration_trading_days": 60,
        "target_reached": terminal == "TARGET_REACHED",
        "mll_breached": terminal == "MLL_BREACHED",
        "censored_state": terminal
        in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"},
        "cost_scenario": scenario,
        "costs": 10.0,
        "net_pnl": net,
        "target_progress": progress,
        "minimum_mll_buffer": 2_000.0,
        "consistency_ok": True,
        "days_to_target": 20.0 if terminal == "TARGET_REACHED" else None,
        "terminal_state": terminal,
    }
    if attribution is not None:
        row[
            "component_contribution" if contribution_field else "component_attribution"
        ] = attribution
    return row


def _full_rows(
    policy: dict[str, object],
    *,
    net: float,
    progress: float,
) -> list[dict[str, object]]:
    sleeves = list(policy["sleeve_ids"])
    weights = {sleeve: net / len(sleeves) for sleeve in sleeves}
    rows: list[dict[str, object]] = []
    for block in BLOCKS:
        for scenario, multiplier in (("NORMAL", 1.1), ("STRESSED_1_5X", 1.0)):
            rows.append(
                _episode(
                    str(policy["policy_id"]),
                    block,
                    scenario,
                    0,
                    net=net * multiplier,
                    progress=progress,
                    attribution={key: value * multiplier for key, value in weights.items()},
                    contribution_field=True,
                )
            )
    return rows


def _predeclared_baseline_bank(
    *,
    components: tuple[str, ...] = ("c1", "c2", "c3"),
    risks: tuple[tuple[float, int], ...] = ((1.0, 4), (1.25, 5)),
    seeds: tuple[int, ...] = (11, 12, 13, 24002401, 24002402),
    sizes: tuple[int, ...] = (2,),
) -> list[dict[str, object]]:
    bank: list[dict[str, object]] = []
    for risk, micro in risks:
        for component in components:
            row = _policy(
                f"parent_{component}_{str(risk).replace('.', '_')}",
                (component,),
                risk=risk,
                micro=micro,
            )
            row["baseline_role"] = "BEST_PARENT_CANDIDATE"
            bank.append(row)
        for size in sizes:
            equal = _policy(
                f"equal_{size}_{str(risk).replace('.', '_')}",
                tuple(sorted(components)[:size]),
                risk=risk,
                micro=micro,
            )
            equal["baseline_role"] = "EQUAL_RISK"
            bank.append(equal)
            for seed in seeds:
                chosen = tuple(random.Random(seed).sample(sorted(components), size))
                row = _policy(
                    f"random_{seed}_{size}_{str(risk).replace('.', '_')}",
                    chosen,
                    risk=risk,
                    micro=micro,
                )
                row["baseline_role"] = "RANDOM_SELECTION"
                row["random_seed"] = seed
                bank.append(row)
    return bank


def test_censoring_is_reported_not_failed_and_component_alias_is_accepted() -> None:
    policy = _policy("p", ("c1", "c2"))
    rows: list[dict[str, object]] = []
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        rows.extend(
            [
                _episode(
                    "p",
                    "B1",
                    scenario,
                    1,
                    net=200.0,
                    attribution={"c1": 120.0, "c2": 80.0},
                    contribution_field=True,
                ),
                _episode(
                    "p",
                    "B1",
                    scenario,
                    2,
                    terminal="DATA_CENSORED",
                    net=150.0,
                    progress=0.8,
                    attribution={"c1": 75.0, "c2": 75.0},
                    contribution_field=True,
                ),
                _episode(
                    "p",
                    "B1",
                    scenario,
                    3,
                    terminal="MLL_BREACHED",
                    net=-50.0,
                    progress=-0.1,
                    attribution={"c1": -25.0, "c2": -25.0},
                    contribution_field=True,
                ),
            ]
        )
    metrics = aggregate_policy_evidence(policy, rows, block_ids=("B1",))
    stress = metrics["stressed_1_5x"]

    assert stress["episode_count"] == 3
    assert stress["evaluable_episode_count"] == 2
    assert stress["censored_episode_count"] == 1
    assert stress["evaluable_pass_rate"] == pytest.approx(0.5)
    assert stress["observed_pass_fraction"] == pytest.approx(1 / 3)
    assert stress["censoring_rate"] == pytest.approx(1 / 3)
    assert stress["observed_net_total"] == 300.0
    assert metrics["component_attribution_complete"] is True
    assert metrics["maximum_component_positive_profit_share"] < 0.65
    assert metrics["censored_pass_rate_policy"].startswith("EXCLUDE")


def test_duplicate_episode_key_fails_closed() -> None:
    policy = _policy("p", ("c1",))
    row = _episode("p", "B1", "NORMAL", 1)
    rows = [row, copy.deepcopy(row), _episode("p", "B1", "STRESSED_1_5X", 1)]
    with pytest.raises(ProductionHalvingError, match="duplicate frozen episode"):
        aggregate_policy_evidence(policy, rows, block_ids=("B1",))


def test_pareto_reports_censoring_and_does_not_use_opaque_score() -> None:
    fast = _policy("fast", ("a", "b"))
    censored = _policy("censored", ("c", "d"))
    fast_rows = _full_rows(fast, net=100.0, progress=0.9)
    censored_rows = _full_rows(censored, net=100.0, progress=0.9)
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        censored_rows.append(
            _episode(
                "censored",
                "B1",
                scenario,
                99,
                terminal="DATA_CENSORED",
                net=0.0,
                progress=0.9,
                attribution={"c": 0.0, "d": 0.0},
            )
        )
    metrics = [
        aggregate_policy_evidence(fast, fast_rows, block_ids=BLOCKS),
        aggregate_policy_evidence(censored, censored_rows, block_ids=BLOCKS),
    ]
    decision = pareto_select(metrics, limit=1, stage="TEST")

    assert decision["selected_policy_ids"] == ["fast"]
    assert decision["opaque_score_used"] is False
    assert "stressed_censoring_rate:MIN" in decision["pareto_dimensions"]
    ranked = {row["policy_id"]: row for row in decision["ranked_candidates"]}
    assert ranked["censored"]["ranking_metrics"]["stressed_censoring_rate"] > 0


def test_stage4_rejects_incomplete_attribution_and_caps_outputs() -> None:
    policies = [_policy(f"p{i}", (f"a{i}", f"b{i}")) for i in range(18)]
    rows = [_full_rows(policy, net=100.0 + i, progress=0.8) for i, policy in enumerate(policies)]
    flattened = [row for group in rows for row in group]
    metrics = [
        aggregate_policy_evidence(policy, flattened, block_ids=BLOCKS)
        for policy in policies
    ]
    incomplete = copy.deepcopy(metrics[0])
    incomplete["policy_id"] = "incomplete"
    incomplete["behavioral_fingerprint"] = "incomplete"
    incomplete["component_attribution_complete"] = False
    incomplete["maximum_component_positive_profit_share"] = None
    decision = select_stage4_survivors([*metrics, incomplete])

    assert decision["output_count"] == 16
    excluded = {row["policy_id"]: row["reasons"] for row in decision["excluded"]}
    assert "INCOMPLETE_COMPONENT_ATTRIBUTION" in excluded["incomplete"]
    with pytest.raises(ProductionHalvingError, match="cap is 16"):
        select_stage4_survivors(metrics, limit=17)


def test_lobo_plan_does_not_read_its_held_out_block_and_freezes_baselines() -> None:
    champion = _policy("champion", ("c1", "c2"), risk=1.25, micro=5)
    runner_up = _policy("runner_up", ("c2", "c3"))
    candidate_rows = _full_rows(champion, net=120.0, progress=0.9) + _full_rows(
        runner_up, net=80.0, progress=0.7
    )
    baseline_bank = _predeclared_baseline_bank(
        seeds=(24002401, 24002402),
    )
    baseline_rows: list[dict[str, object]] = []
    for index, policy in enumerate(baseline_bank):
        baseline_rows.extend(
            _full_rows(policy, net=30.0 + index / 10, progress=0.4 + index / 1000)
        )
    kwargs = {
        "predeclared_baseline_policies": baseline_bank,
        "baseline_design_episode_rows": baseline_rows,
        "block_ids": BLOCKS,
        "random_seeds": (24002401, 24002402),
    }
    original = build_leave_one_block_out_plan(
        (champion, runner_up), candidate_rows, **kwargs
    )
    mutated_rows = copy.deepcopy(candidate_rows)
    for row in mutated_rows:
        if row["temporal_block"] == "B1" and row["policy_id"] == "runner_up":
            row["net_pnl"] = 1_000_000.0
            row["target_progress"] = 100.0
    mutated = build_leave_one_block_out_plan(
        (champion, runner_up), mutated_rows, **kwargs
    )
    original_b1 = next(row for row in original["folds"] if row["held_out_block"] == "B1")
    mutated_b1 = next(row for row in mutated["folds"] if row["held_out_block"] == "B1")

    assert original_b1 == mutated_b1
    assert original_b1["champion_policy"]["policy_id"] == "champion"
    assert original_b1["selected_risk_level"] == 1.25
    assert original_b1["held_out_outcomes_inspected"] is False
    baselines = original_b1["baselines"]
    assert len(baselines["deterministic_equal_risk"]["sleeve_ids"]) == 2
    assert len(baselines["fixed_seed_random_selection"]) == 2
    assert baselines["held_out_outcomes_used"] is False
    assert baselines["all_policy_ids_predeclared_before_outcomes"] is True
    assert {
        row["policy_id"] for row in baselines["fixed_seed_random_selection"]
    }.issubset({row["policy_id"] for row in baseline_bank})
    again = build_leave_one_block_out_plan(
        (champion, runner_up), candidate_rows, **kwargs
    )
    assert original == again


def test_complete_lobo_uses_only_held_out_evidence_and_can_be_green() -> None:
    champion = _policy("champion", ("c1", "c2"), risk=1.25, micro=5)
    runner_up = _policy("runner_up", ("c2", "c3"))
    candidate_rows = _full_rows(champion, net=120.0, progress=0.9) + _full_rows(
        runner_up, net=80.0, progress=0.7
    )
    baseline_bank = _predeclared_baseline_bank(seeds=(11, 12, 13))
    baseline_rows = [
        row
        for index, policy in enumerate(baseline_bank)
        for row in _full_rows(policy, net=20.0 + index / 100, progress=0.4)
    ]
    plan = build_leave_one_block_out_plan(
        (champion, runner_up),
        candidate_rows,
        predeclared_baseline_policies=baseline_bank,
        baseline_design_episode_rows=baseline_rows,
        block_ids=BLOCKS,
        random_seeds=(11, 12, 13),
    )
    result = complete_leave_one_block_out(plan, candidate_rows, baseline_rows)

    assert result["selector_decision"]["status"] == "SELECTOR_PROCEDURE_GREEN"
    assert result["headline_held_out_selector"]["stressed_1_5x"]["pass_count"] == 4
    assert result["headline_held_out_selector"]["stressed_pass_block_count"] == 4
    assert result["held_out_blocks_used_exactly_once"] is True
    assert all(not row["retuning_after_holdout"] for row in result["folds"])


def test_development_threshold_uses_observed_fraction_and_requires_96_starts() -> None:
    metrics = {
        "policy_id": "p",
        "normal": {
            "episode_count": 96,
            "observed_pass_fraction": 0.10,
            "pass_rate": 0.50,
        },
        "stressed_1_5x": {
            "episode_count": 96,
            "observed_pass_fraction": 0.05,
            "pass_rate": 0.50,
            "observed_net_total": 100.0,
            "mll_breach_rate": 0.01,
            "consistency_rate": 0.90,
        },
        "stressed_pass_block_count": 2,
        "maximum_block_pass_share": 0.50,
        "component_attribution_complete": True,
        "maximum_component_positive_profit_share": 0.60,
    }
    criteria = {
        "minimum_normal_pass_rate": 0.10,
        "minimum_stressed_pass_rate": 0.05,
        "minimum_stressed_net": 0.0,
        "maximum_mll_breach_rate": 0.10,
        "minimum_positive_blocks": 2,
        "maximum_block_pass_share": 0.50,
        "maximum_component_profit_share": 0.65,
    }
    decision = development_decision(metrics, criteria=criteria, minimum_starts=96)

    assert decision["criteria_satisfied"] is True
    assert decision["status"] == "BASKET_CONFIRMATION_READY"
    assert "CUMULATIVE_INCIDENCE" in decision["pass_threshold_definition"]
    assert decision["censored_starts_classified_as_failures"] is False
    metrics["normal"]["observed_pass_fraction"] = 0.09
    assert not development_decision(
        metrics, criteria=criteria, minimum_starts=96
    )["criteria_satisfied"]


def test_stage5_cap_compact_outputs_and_terminal_payload_guards() -> None:
    policy = _policy("p", ("c1", "c2"))
    rows: list[dict[str, object]] = []
    for block in BLOCKS:
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            for index in range(24):
                rows.append(
                    _episode(
                        "p",
                        block,
                        scenario,
                        index,
                        net=10.0,
                        progress=1.0,
                        attribution={"c1": 5.0, "c2": 5.0},
                    )
                )
    metrics = aggregate_policy_evidence(policy, rows, block_ids=BLOCKS)
    criteria = {
        "minimum_normal_pass_rate": 0.10,
        "minimum_stressed_pass_rate": 0.05,
        "minimum_stressed_net": 0.0,
        "maximum_mll_breach_rate": 0.10,
        "minimum_positive_blocks": 2,
        "maximum_block_pass_share": 0.50,
        "maximum_component_profit_share": 0.65,
    }
    stage5 = select_stage5_survivors((metrics,), criteria=criteria)
    assert stage5["selected_policy_ids"] == ["p"]
    with pytest.raises(ProductionHalvingError, match="cap is 4"):
        select_stage5_survivors((metrics,), criteria=criteria, limit=5)

    dev = development_decision(metrics, criteria=criteria, minimum_starts=96)
    compact = build_compact_outputs(
        campaign_id="campaign_0024",
        metrics=(metrics,),
        stage_decisions=(stage5,),
        crossfit_result=None,
        development_decisions=(dev,),
    )
    assert set(compact) == {
        "campaign_summary",
        "failure_vectors",
        "pareto_archive",
        "next_campaign_recommendations",
    }
    assert compact["campaign_summary"]["confirmation_ready_candidate_ids"] == ["p"]

    manifest = {
        "campaign_id": "campaign_0024",
        "manifest_hash": "m" * 64,
        "source_commit": "c" * 40,
    }
    receipt = {
        "bundle_path": "/tmp/bundle",
        "manifest_path": "/tmp/bundle/evidence_bundle_manifest.json",
        "manifest_sha256": "a" * 64,
        "bundle_content_sha256": "b" * 64,
        "dataset_row_counts": {"episodes": 192},
        "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
        "reconstruction_flag": False,
    }
    result = build_final_result_payload(
        manifest=manifest,
        kpis={"policies_proposed": 20_000},
        economic_results={"confirmation_ready_candidate_ids": ["p"]},
        successive_halving=stage5,
        matched_controls={"status": "COMPLETE"},
        failure_vectors=compact["failure_vectors"],
        evidence_receipt=receipt,
        autonomous_next_action=compact["next_campaign_recommendations"]["recommendation"],
    )
    claimed = result.pop("result_hash")
    assert claimed == stable_hash(result)
    assert result["status"] == "COMPLETE"
    assert result["independently_confirmed"] is False
    assert result["q4_access_delta"] == 0
    bad = dict(receipt, reconstruction_flag=True)
    with pytest.raises(ProductionHalvingError, match="reconstruction"):
        build_final_result_payload(
            manifest=manifest,
            kpis={},
            economic_results={},
            successive_halving={},
            matched_controls={},
            failure_vectors={},
            evidence_receipt=bad,
            autonomous_next_action={},
        )
