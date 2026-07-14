from __future__ import annotations

from hydra.selection.nested_basket_selector import (
    ParetoObjective,
    select_pareto_champion,
)


OBJECTIVES = (
    ParetoObjective("stress_pass_count", "maximize"),
    ParetoObjective("normal_pass_count", "maximize"),
    ParetoObjective("stressed_target_progress_median", "maximize"),
    ParetoObjective("stressed_target_progress_p25", "maximize"),
    ParetoObjective("stressed_net_usd", "maximize"),
    ParetoObjective("mll_breach_rate", "minimize"),
    ParetoObjective("consistency_pass_rate", "maximize"),
    ParetoObjective("maximum_component_profit_share", "minimize"),
    ParetoObjective("maximum_block_profit_share", "minimize"),
    ParetoObjective("operational_complexity", "minimize"),
)


def _row(identifier: str, **changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "variant_id": identifier,
        "policy_id": identifier.split("::")[0],
        "component_ids": [f"{identifier}-a", f"{identifier}-b"],
        "design_behavior_fingerprint": identifier,
        "normal_net_usd": 100.0,
        "stressed_net_usd": 90.0,
        "stress_pass_count": 1,
        "normal_pass_count": 1,
        "stressed_target_progress_median": 0.5,
        "stressed_target_progress_p25": 0.3,
        "mll_breach_rate": 0.0,
        "hard_issue_count": 0,
        "consistency_pass_rate": 1.0,
        "maximum_component_profit_share": 0.4,
        "maximum_block_profit_share": 0.4,
        "operational_complexity": 2,
    }
    row.update(changes)
    return row


def test_selector_collapses_design_only_clones_and_is_deterministic() -> None:
    rows = [
        _row("p1::1", design_behavior_fingerprint="clone"),
        _row("p2::1", design_behavior_fingerprint="clone"),
        _row(
            "p3::1",
            stress_pass_count=2,
            normal_pass_count=2,
            stressed_target_progress_median=0.6,
            stressed_target_progress_p25=0.4,
            stressed_net_usd=120.0,
        ),
    ]
    decision = select_pareto_champion(
        list(reversed(rows)),
        objectives=OBJECTIVES,
        maximum_mll_breach_rate=0.10,
        maximum_component_profit_share=0.65,
    )
    assert decision.primary["variant_id"] == "p3::1"
    assert decision.behavioral_clone_rejection_count == 1
    clone = next(row for row in decision.clone_groups if len(row["member_variant_ids"]) == 2)
    assert clone["representative_variant_id"] == "p1::1"


def test_selector_hard_filters_positive_costs_integrity_mll_and_concentration() -> None:
    rows = [
        _row("good"),
        _row("normal", normal_net_usd=0.0),
        _row("stress", stressed_net_usd=-1.0),
        _row("issue", hard_issue_count=1),
        _row("mll", mll_breach_rate=0.11),
        _row("concentration", maximum_component_profit_share=0.66),
    ]
    decision = select_pareto_champion(
        rows,
        objectives=OBJECTIVES,
        maximum_mll_breach_rate=0.10,
        maximum_component_profit_share=0.65,
    )
    assert decision.primary["variant_id"] == "good"
    assert decision.hard_rejection_count == 5
    reasons = {reason for row in decision.hard_rejections for reason in row["reasons"]}
    assert reasons == {
        "NORMAL_NET_NONPOSITIVE",
        "STRESSED_NET_NONPOSITIVE",
        "HARD_EXECUTION_OR_INTEGRITY_ISSUE",
        "MLL_TOLERANCE_EXCEEDED",
        "COMPONENT_DOMINATION",
    }


def test_backup_must_be_behaviorally_and_structurally_distinct() -> None:
    primary = _row(
        "primary",
        component_ids=["a", "b", "c", "d"],
        stress_pass_count=3,
    )
    near_clone = _row(
        "near",
        component_ids=["a", "b", "c", "x"],
        stress_pass_count=2,
    )
    distinct = _row(
        "distinct",
        component_ids=["u", "v", "w", "x"],
        stress_pass_count=1,
    )
    decision = select_pareto_champion(
        [near_clone, distinct, primary],
        objectives=OBJECTIVES,
        maximum_mll_breach_rate=0.10,
        maximum_component_profit_share=0.65,
        backup_maximum_component_jaccard=0.50,
    )
    assert decision.primary["variant_id"] == "primary"
    assert decision.backup is not None
    assert decision.backup["variant_id"] == "distinct"
