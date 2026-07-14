from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
)
from hydra.selection.fold_candidate_compiler import (
    ACCEPTED_PARENT_GROUP_COUNT,
    EXPECTED_CANDIDATE_COUNT,
    FoldCandidateCompilerError,
    compile_fold_candidate_bank,
    frozen_compiler_policy,
)


BLOCKS = ("V73_B1", "V73_B2", "V73_B3")


def _component_stats() -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for index in range(48):
        component_id = f"sleeve_{index:02d}"
        output[component_id] = {
            "component_id": component_id,
            "signal_market": ("ES", "NQ", "YM", "CL")[index % 4],
            "execution_market": ("MES", "MNQ", "MYM", "MCL")[index % 4],
            "role": ("PRIMARY_ALPHA", "SESSION_DIVERSIFIER")[index % 2],
            "design_blocks": {
                block_id: {
                    "event_count": 3 + ((index + block_index) % 7),
                    "normal_net_usd": float(
                        80 + 5 * index - 11 * block_index + (index % 3)
                    ),
                    "stressed_net_usd": float(
                        60 + 4 * index - 13 * block_index + (index % 5)
                    ),
                }
                for block_index, block_id in enumerate(BLOCKS)
            },
        }
    return output


def _parents() -> tuple[EliteRobustnessPolicy, ...]:
    component_ids = tuple(f"sleeve_{index:02d}" for index in range(48))
    rows: list[EliteRobustnessPolicy] = []
    # Coprime step and separated starts provide overlapping but sufficiently
    # wide frozen memberships for every two-parent union to fill size 13.
    for parent_index in range(8):
        membership = tuple(
            component_ids[(parent_index * 5 + offset * 7) % len(component_ids)]
            for offset in range(13)
        )
        rows.append(
            EliteRobustnessPolicy(
                policy_id=f"parent_{parent_index}",
                parent_policy_id=f"parent_{parent_index}",
                parent_policy_fingerprint=f"source_{parent_index}",
                component_ids=membership,
                retained_added_sleeve_id=membership[0],
                mutation_family="FROZEN_0018_PARENT",
                failure_target="TEST_FIXED_PARENT",
                exact_change=(("parent_index", parent_index),),
                expected_effect="Frozen test parent specification.",
                high_risk_units=3,
                daily_loss_guard=1_000.0,
                daily_profit_lock=2_000.0,
                critical_buffer=500.0 + parent_index,
                high_zone_buffer=4_000.0,
                high_zone_remaining_target=8_000.0,
                middle_zone_buffer=2_000.0,
                middle_zone_remaining_target=4_000.0,
                middle_risk_units=2,
                maximum_simultaneous_positions=3,
                maximum_mini_equivalent=15,
            )
        )
    return tuple(rows)


def _compile(
    stats: dict[str, dict[str, Any]] | None = None,
    parents: tuple[EliteRobustnessPolicy, ...] | None = None,
):
    return compile_fold_candidate_bank(
        component_design_stats=stats or _component_stats(),
        parents=parents or _parents(),
        design_block_ids=BLOCKS,
        compiler_policy=frozen_compiler_policy(),
    )


def test_bounded_compiler_emits_exactly_20_groups_and_80_structures() -> None:
    compiled = _compile()

    assert len(compiled.policies) == EXPECTED_CANDIDATE_COUNT == 80
    assert compiled.audit["enumerated_parent_group_count"] == 154
    assert compiled.audit["accepted_parent_group_count"] == (
        ACCEPTED_PARENT_GROUP_COUNT
    )
    assert compiled.audit["membership_size_counts"] == {
        "10": 20,
        "11": 20,
        "12": 20,
        "13": 20,
    }
    assert len({row.structural_fingerprint for row in compiled.policies}) == 80
    accepted = [
        row
        for row in compiled.audit["parent_group_audit"]
        if row["disposition"] == "ACCEPTED"
    ]
    assert len(accepted) == 20
    assert all(len(row["candidate_policy_ids"]) == 4 for row in accepted)
    assert compiled.audit["proposal_memberships_read"] is False
    assert compiled.audit["parent_full_period_outcomes_read"] is False
    assert compiled.audit["held_out_block_read"] is False


def test_top_level_heldout_bait_cannot_change_fold_bank() -> None:
    clean = _component_stats()
    baited = _component_stats()
    for index, row in enumerate(baited.values()):
        row["held_out_stressed_net_usd"] = float((1 if index % 2 else -1) * 1e12)
        row["campaign_0023_full_period_rank"] = 48 - index

    first = _compile(clean)
    second = _compile(baited)

    assert [row.to_dict() for row in first.policies] == [
        row.to_dict() for row in second.policies
    ]
    assert first.audit["design_evidence_hash"] == second.audit["design_evidence_hash"]
    assert second.audit["ignored_top_level_component_fields"] == [
        "campaign_0023_full_period_rank",
        "held_out_stressed_net_usd",
    ]


def test_extra_block_is_rejected_instead_of_silently_read() -> None:
    stats = _component_stats()
    for row in stats.values():
        row["design_blocks"]["V73_B4"] = {
            "event_count": 999,
            "normal_net_usd": 1e12,
            "stressed_net_usd": 1e12,
        }

    with pytest.raises(
        FoldCandidateCompilerError, match="exactly the explicit design blocks"
    ):
        _compile(stats)


def test_parent_input_order_and_parent_component_order_do_not_rank_candidates() -> None:
    parents = _parents()
    reordered = tuple(
        replace(row, component_ids=tuple(reversed(row.component_ids)))
        for row in reversed(parents)
    )

    first = _compile(parents=parents)
    second = _compile(parents=reordered)

    assert [row.component_ids for row in first.policies] == [
        row.component_ids for row in second.policies
    ]
    assert [row.source_parent_ids for row in first.policies] == [
        row.source_parent_ids for row in second.policies
    ]
    assert first.audit["parent_component_order_used"] is False


def test_design_stat_change_can_change_design_ranked_bank() -> None:
    baseline = _compile()
    changed = _component_stats()
    for block_id in BLOCKS:
        changed["sleeve_00"]["design_blocks"][block_id][
            "stressed_net_usd"
        ] = 1e8
        changed["sleeve_00"]["design_blocks"][block_id]["normal_net_usd"] = 1e8

    reranked = _compile(changed)

    assert baseline.audit["design_evidence_hash"] != reranked.audit[
        "design_evidence_hash"
    ]
    assert [row.component_ids for row in baseline.policies] != [
        row.component_ids for row in reranked.policies
    ]


def test_policy_ids_are_semantic_and_do_not_encode_fold_evidence_hash() -> None:
    baseline = _compile()
    same_ranking_new_evidence = _component_stats()
    for row in same_ranking_new_evidence.values():
        for block in row["design_blocks"].values():
            block["event_count"] += 1

    rerun = _compile(same_ranking_new_evidence)

    assert baseline.audit["design_evidence_hash"] != rerun.audit[
        "design_evidence_hash"
    ]
    assert [row.component_ids for row in baseline.policies] == [
        row.component_ids for row in rerun.policies
    ]
    assert [row.policy_id for row in baseline.policies] == [
        row.policy_id for row in rerun.policies
    ]


def test_compiler_policy_is_hash_frozen_and_tamper_rejected() -> None:
    policy = frozen_compiler_policy()
    policy["accepted_parent_group_count"] = 21

    with pytest.raises(FoldCandidateCompilerError, match="policy drift"):
        compile_fold_candidate_bank(
            component_design_stats=_component_stats(),
            parents=_parents(),
            design_block_ids=BLOCKS,
            compiler_policy=policy,
        )
