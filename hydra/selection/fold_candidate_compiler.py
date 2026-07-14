"""Bounded fold-local compiler for the HYDRA V7.3 selector experiment.

The compiler is deliberately not a basket grammar.  Its universe is frozen to
the 48 immutable component ledgers and the eight already-frozen parent
specifications.  For one outer fold it enumerates the finite 154 parent groups
of size two through four, derives every ordering from the three design blocks,
and accepts the first 20 groups whose four executable structures are distinct.

Campaign-0023 proposal memberships and full-period parent outcome fields are
not arguments to this module.  Parent component membership is structural input;
parent ordering is ignored and parent/consensus ranking is recomputed solely
from the explicitly named design blocks.
"""

from __future__ import annotations

import itertools
import math
import statistics
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
)
from hydra.economic_evolution.account_static_parent_basket import (
    StaticParentBasketPolicy,
)
from hydra.economic_evolution.schema import stable_hash


COMPILER_VERSION = "hydra_v73_fold_local_candidate_compiler_v1"
COMPILER_MANIFEST_SCHEMA = "hydra_v73_fold_local_candidate_compiler_policy_v1"
COMPILATION_AUDIT_SCHEMA = "hydra_v73_fold_local_candidate_compilation_audit_v1"
EXPECTED_COMPONENT_COUNT = 48
EXPECTED_PARENT_COUNT = 8
OUTER_DESIGN_BLOCK_COUNT = 3
FINAL_DEVELOPMENT_BLOCK_COUNT = 4
PARENT_GROUP_SIZES: tuple[int, ...] = (2, 3, 4)
MEMBERSHIP_SIZES: tuple[int, ...] = (10, 11, 12, 13)
ACCEPTED_PARENT_GROUP_COUNT = 20
EXPECTED_ENUMERATED_GROUP_COUNT = 154
EXPECTED_CANDIDATE_COUNT = 80


class FoldCandidateCompilerError(ValueError):
    """Raised when candidate construction could cross a frozen boundary."""


@dataclass(frozen=True, slots=True)
class FoldCandidateCompilation:
    """The 80 frozen policies and a complete, hash-sealed construction audit."""

    policies: tuple[StaticParentBasketPolicy, ...]
    audit: Mapping[str, Any]

    def __post_init__(self) -> None:
        if len(self.policies) != EXPECTED_CANDIDATE_COUNT:
            raise FoldCandidateCompilerError("fold candidate count drift")
        if self.audit.get("audit_hash") != _self_hash(self.audit, "audit_hash"):
            raise FoldCandidateCompilerError("fold candidate audit hash drift")


def frozen_compiler_policy() -> dict[str, Any]:
    """Return the exact compiler policy that must be preregistered."""

    payload: dict[str, Any] = {
        "schema": COMPILER_MANIFEST_SCHEMA,
        "compiler_version": COMPILER_VERSION,
        "component_ledger_count": EXPECTED_COMPONENT_COUNT,
        "frozen_parent_count": EXPECTED_PARENT_COUNT,
        "outer_design_block_count": OUTER_DESIGN_BLOCK_COUNT,
        "final_development_block_count": FINAL_DEVELOPMENT_BLOCK_COUNT,
        "parent_group_sizes": list(PARENT_GROUP_SIZES),
        "enumerated_parent_group_count": EXPECTED_ENUMERATED_GROUP_COUNT,
        "accepted_parent_group_count": ACCEPTED_PARENT_GROUP_COUNT,
        "membership_sizes": list(MEMBERSHIP_SIZES),
        "candidate_structure_count": EXPECTED_CANDIDATE_COUNT,
        "parent_lead_ranking": [
            "positive_stressed_design_block_count_DESC",
            "stressed_block_floor_net_DESC",
            "stressed_block_median_net_DESC",
            "stressed_design_net_DESC",
            "normal_design_net_DESC",
            "design_event_count_DESC",
            "parent_policy_id_ASC",
        ],
        "consensus_component_ranking": [
            "parent_vote_count_DESC",
            "positive_stressed_design_block_count_DESC",
            "stressed_block_floor_net_DESC",
            "stressed_block_median_net_DESC",
            "stressed_design_net_DESC",
            "normal_design_net_DESC",
            "design_event_count_DESC",
            "component_id_ASC",
        ],
        "parent_group_ranking": [
            "supports_all_membership_sizes_TRUE",
            "positive_stressed_design_block_count_DESC",
            "stressed_block_floor_net_DESC",
            "stressed_block_median_net_DESC",
            "stressed_design_net_DESC",
            "normal_design_net_DESC",
            "maximum_positive_component_stress_share_ASC",
            "design_event_count_DESC",
            "parent_policy_ids_ASC",
        ],
        "group_ranking_membership_size": 13,
        "lead_retained_sleeve_is_mandatory": True,
        "parent_component_order_used": False,
        "proposal_memberships_allowed": False,
        "full_period_component_outcomes_allowed": False,
        "full_period_parent_outcomes_allowed": False,
        "held_out_block_fields_allowed": False,
        "structural_collision_policy": (
            "SKIP_GROUP_IF_ANY_OF_FOUR_EXECUTABLE_STRUCTURES_ALREADY_ACCEPTED"
        ),
        "continuous_weights_or_optimization": False,
    }
    payload["policy_hash"] = stable_hash(payload)
    return payload


def compile_fold_candidate_bank(
    *,
    component_design_stats: Mapping[str, Mapping[str, Any]],
    parents: Sequence[EliteRobustnessPolicy],
    design_block_ids: Sequence[str],
    compiler_policy: Mapping[str, Any],
) -> FoldCandidateCompilation:
    """Compile exactly 80 policies without reading a held-out outcome.

    ``component_design_stats`` must contain an exact ``design_blocks`` mapping
    for each of the 48 immutable components: three blocks for an outer fold or
    all four for the conditional final-development application.  Only those
    per-block rows and the component identity/provenance fields are projected
    into the compiler.  Any tempting top-level aggregate or outcome field is
    ignored and recorded as such in the audit.
    """

    _validate_compiler_policy(compiler_policy)
    blocks = _validate_design_blocks(design_block_ids)
    stats, ignored_fields = _normalize_component_stats(
        component_design_stats, design_block_ids=blocks
    )
    parent_rows = _normalize_parents(parents, component_ids=set(stats))

    group_rows: list[dict[str, Any]] = []
    for group_size in PARENT_GROUP_SIZES:
        for group in itertools.combinations(parent_rows, group_size):
            group_rows.append(_prepare_group(group, stats=stats, blocks=blocks))
    if len(group_rows) != EXPECTED_ENUMERATED_GROUP_COUNT:
        raise FoldCandidateCompilerError("parent-group enumeration drift")
    group_rows.sort(key=lambda row: row["rank_key"])

    accepted_policies: list[StaticParentBasketPolicy] = []
    accepted_execution_structures: set[str] = set()
    group_audit: list[dict[str, Any]] = []
    accepted_count = 0
    for rank_ordinal, prepared in enumerate(group_rows, start=1):
        policies = (
            _build_group_policies(
                prepared,
                stats=stats,
                blocks=blocks,
                design_evidence_hash=_design_evidence_hash(stats, blocks),
            )
            if prepared["supports_all_membership_sizes"]
            else ()
        )
        structure_hashes = tuple(_execution_structure_hash(row) for row in policies)
        duplicate_hashes = sorted(
            value for value in structure_hashes if value in accepted_execution_structures
        )
        within_group_collision = len(set(structure_hashes)) != len(structure_hashes)
        accepted = bool(
            prepared["supports_all_membership_sizes"]
            and accepted_count < ACCEPTED_PARENT_GROUP_COUNT
            and not duplicate_hashes
            and not within_group_collision
        )
        if not prepared["supports_all_membership_sizes"]:
            disposition = "INSUFFICIENT_PARENT_UNION_FOR_SIZE_13"
        elif accepted:
            accepted_count += 1
            accepted_policies.extend(policies)
            accepted_execution_structures.update(structure_hashes)
            disposition = "ACCEPTED"
        elif accepted_count >= ACCEPTED_PARENT_GROUP_COUNT:
            disposition = "NOT_REACHED_AFTER_FIXED_QUOTA"
        else:
            disposition = "STRUCTURAL_COLLISION_SKIPPED"
        group_audit.append(
            {
                "enumerated_rank": rank_ordinal,
                "parent_policy_ids": list(prepared["parent_policy_ids"]),
                "lead_policy_id": prepared["lead"].policy_id,
                "lead_retained_added_sleeve_id": prepared[
                    "lead"
                ].retained_added_sleeve_id,
                "lead_rank_metrics": dict(prepared["lead_metrics"]),
                "group_rank_metrics": dict(prepared["group_metrics"]),
                "supports_all_membership_sizes": bool(
                    prepared["supports_all_membership_sizes"]
                ),
                "consensus_priority": list(prepared["consensus_priority"]),
                "candidate_policy_ids": [row.policy_id for row in policies],
                "candidate_memberships": {
                    str(len(row.component_ids)): list(row.component_ids)
                    for row in policies
                },
                "candidate_structural_fingerprints": [
                    row.structural_fingerprint for row in policies
                ],
                "execution_structure_hashes": list(structure_hashes),
                "duplicate_execution_structure_hashes": duplicate_hashes,
                "disposition": disposition,
                "accepted_group_ordinal": accepted_count if accepted else None,
            }
        )
    if accepted_count != ACCEPTED_PARENT_GROUP_COUNT:
        raise FoldCandidateCompilerError(
            f"only {accepted_count} collision-free parent groups were available"
        )
    if len(accepted_policies) != EXPECTED_CANDIDATE_COUNT:
        raise FoldCandidateCompilerError("candidate structure quota is incomplete")
    if len({_execution_structure_hash(row) for row in accepted_policies}) != len(
        accepted_policies
    ):
        raise FoldCandidateCompilerError("accepted executable structures are not unique")

    evidence_hash = _design_evidence_hash(stats, blocks)
    audit: dict[str, Any] = {
        "schema": COMPILATION_AUDIT_SCHEMA,
        "compiler_version": COMPILER_VERSION,
        "compiler_policy_hash": compiler_policy["policy_hash"],
        "design_block_ids": list(blocks),
        "design_evidence_hash": evidence_hash,
        "component_ledger_count": len(stats),
        "component_ids": sorted(stats),
        "frozen_parent_count": len(parent_rows),
        "frozen_parent_ids": [row.policy_id for row in parent_rows],
        "enumerated_parent_group_count": len(group_rows),
        "accepted_parent_group_count": accepted_count,
        "candidate_structure_count": len(accepted_policies),
        "membership_size_counts": {
            str(size): sum(len(row.component_ids) == size for row in accepted_policies)
            for size in MEMBERSHIP_SIZES
        },
        "component_stat_fields_read": [
            "component_id",
            "signal_market",
            "execution_market",
            "role",
            "design_blocks.<explicit_design_block_id>.event_count",
            "design_blocks.<explicit_design_block_id>.normal_net_usd",
            "design_blocks.<explicit_design_block_id>.stressed_net_usd",
        ],
        "ignored_top_level_component_fields": sorted(ignored_fields),
        "parent_structural_fields_read": [
            "policy_id",
            "structural_fingerprint",
            "component_ids_as_unordered_membership",
            "retained_added_sleeve_id",
            "account_guard_and_limit_fields",
        ],
        "proposal_memberships_read": False,
        "parent_full_period_outcomes_read": False,
        "held_out_block_read": False,
        "parent_component_order_used": False,
        "continuous_weights_or_optimization": False,
        "accepted_policy_ids": [row.policy_id for row in accepted_policies],
        "parent_group_audit": group_audit,
    }
    audit["audit_hash"] = stable_hash(audit)
    return FoldCandidateCompilation(tuple(accepted_policies), audit)


def _validate_compiler_policy(value: Mapping[str, Any]) -> None:
    expected = frozen_compiler_policy()
    if dict(value) != expected:
        raise FoldCandidateCompilerError("fold candidate compiler policy drift")


def _validate_design_blocks(values: Sequence[str]) -> tuple[str, ...]:
    rows = tuple(str(value) for value in values)
    if (
        len(rows) not in {OUTER_DESIGN_BLOCK_COUNT, FINAL_DEVELOPMENT_BLOCK_COUNT}
        or len(set(rows)) != len(rows)
        or any(not value.strip() for value in rows)
    ):
        raise FoldCandidateCompilerError(
            "exactly three outer-fold or four final-development blocks are required"
        )
    return tuple(sorted(rows))


def _normalize_component_stats(
    values: Mapping[str, Mapping[str, Any]],
    *,
    design_block_ids: Sequence[str],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    if len(values) != EXPECTED_COMPONENT_COUNT:
        raise FoldCandidateCompilerError("compiler requires the frozen 48-component ledger")
    expected_blocks = set(design_block_ids)
    output: dict[str, dict[str, Any]] = {}
    ignored_fields: set[str] = set()
    read_fields = {
        "component_id",
        "signal_market",
        "execution_market",
        "role",
        "design_blocks",
    }
    for lookup_id, raw in sorted(values.items()):
        component_id = str(raw.get("component_id", lookup_id))
        if component_id != str(lookup_id) or not component_id.strip():
            raise FoldCandidateCompilerError("component design-stat identity drift")
        raw_blocks = raw.get("design_blocks")
        if not isinstance(raw_blocks, Mapping) or set(raw_blocks) != expected_blocks:
            raise FoldCandidateCompilerError(
                f"{component_id} must contain exactly the explicit design blocks"
            )
        blocks: dict[str, dict[str, float | int]] = {}
        for block_id in design_block_ids:
            block = raw_blocks[block_id]
            if not isinstance(block, Mapping):
                raise FoldCandidateCompilerError("component block statistics are malformed")
            required = {"event_count", "normal_net_usd", "stressed_net_usd"}
            if set(block) != required:
                raise FoldCandidateCompilerError(
                    f"{component_id}.{block_id} design-stat fields drift"
                )
            event_count = block["event_count"]
            if isinstance(event_count, bool) or int(event_count) != event_count or event_count < 0:
                raise FoldCandidateCompilerError("design event count must be non-negative")
            normal = float(block["normal_net_usd"])
            stressed = float(block["stressed_net_usd"])
            if not math.isfinite(normal) or not math.isfinite(stressed):
                raise FoldCandidateCompilerError("design economics must be finite")
            blocks[block_id] = {
                "event_count": int(event_count),
                "normal_net_usd": normal,
                "stressed_net_usd": stressed,
            }
        output[component_id] = {
            "component_id": component_id,
            "signal_market": str(raw.get("signal_market", "")),
            "execution_market": str(raw.get("execution_market", "")),
            "role": str(raw.get("role", "")),
            "design_blocks": blocks,
        }
        ignored_fields.update(set(raw) - read_fields)
    return output, ignored_fields


def _normalize_parents(
    parents: Sequence[EliteRobustnessPolicy], *, component_ids: set[str]
) -> tuple[EliteRobustnessPolicy, ...]:
    rows = tuple(sorted(parents, key=lambda row: row.policy_id))
    if len(rows) != EXPECTED_PARENT_COUNT or len({row.policy_id for row in rows}) != len(rows):
        raise FoldCandidateCompilerError("compiler requires eight unique frozen parents")
    if any(not set(row.component_ids).issubset(component_ids) for row in rows):
        raise FoldCandidateCompilerError("frozen parent escaped the 48-component ledger")
    return rows


def _prepare_group(
    group: Sequence[EliteRobustnessPolicy],
    *,
    stats: Mapping[str, Mapping[str, Any]],
    blocks: Sequence[str],
) -> dict[str, Any]:
    lead_rows = [
        (_membership_metrics(set(row.component_ids), stats=stats, blocks=blocks), row)
        for row in group
    ]
    lead_rows.sort(key=lambda row: _lead_rank_key(row[0], row[1].policy_id))
    lead_metrics, lead = lead_rows[0]
    votes = Counter(component for row in group for component in set(row.component_ids))
    priority = tuple(
        sorted(
            votes,
            key=lambda component_id: _component_consensus_rank_key(
                component_id, votes=votes, stats=stats, blocks=blocks
            ),
        )
    )
    supports_all = len(priority) >= max(MEMBERSHIP_SIZES)
    canonical = (
        _membership_from_priority(
            priority,
            size=max(MEMBERSHIP_SIZES),
            mandatory=lead.retained_added_sleeve_id,
            votes=votes,
            stats=stats,
            blocks=blocks,
        )
        if supports_all
        else priority
    )
    group_metrics = _membership_metrics(set(canonical), stats=stats, blocks=blocks)
    parent_ids = tuple(sorted(row.policy_id for row in group))
    return {
        "parents": tuple(group),
        "parent_policy_ids": parent_ids,
        "lead": lead,
        "lead_metrics": lead_metrics,
        "votes": votes,
        "consensus_priority": priority,
        "group_metrics": group_metrics,
        "supports_all_membership_sizes": supports_all,
        "rank_key": (
            int(not supports_all),
            *_group_rank_key(group_metrics, parent_ids),
        ),
    }


def _build_group_policies(
    prepared: Mapping[str, Any],
    *,
    stats: Mapping[str, Mapping[str, Any]],
    blocks: Sequence[str],
    design_evidence_hash: str,
) -> tuple[StaticParentBasketPolicy, ...]:
    lead: EliteRobustnessPolicy = prepared["lead"]
    votes: Counter[str] = prepared["votes"]
    parent_ids = tuple(prepared["parent_policy_ids"])
    policies: list[StaticParentBasketPolicy] = []
    for size in MEMBERSHIP_SIZES:
        components = _membership_from_priority(
            prepared["consensus_priority"],
            size=size,
            mandatory=lead.retained_added_sleeve_id,
            votes=votes,
            stats=stats,
            blocks=blocks,
        )
        profile = f"CONSENSUS_{size}_V73_FOLD_LOCAL"
        identity = stable_hash(
            {
                "compiler_version": COMPILER_VERSION,
                "component_priority": list(components),
                "lead_structural_fingerprint": lead.structural_fingerprint,
                "daily_loss_guard": float(lead.daily_loss_guard).hex(),
                "daily_profit_lock": float(lead.daily_profit_lock).hex(),
                "critical_buffer": float(lead.critical_buffer).hex(),
                "maximum_simultaneous_positions": (
                    lead.maximum_simultaneous_positions
                ),
                "maximum_mini_equivalent": lead.maximum_mini_equivalent,
                "assembly_profile": profile,
                "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
            }
        )[:24]
        policies.append(
            StaticParentBasketPolicy(
                policy_id=f"v73_fold_static_basket_{identity}",
                parent_policy_id=lead.policy_id,
                parent_policy_fingerprint=lead.structural_fingerprint,
                source_parent_ids=parent_ids,
                component_ids=components,
                retained_added_sleeve_id=lead.retained_added_sleeve_id,
                mutation_family="STATIC_PARENT_SYNTHESIS",
                failure_target="NESTED_SELECTOR_PROCEDURE_VALIDATION",
                exact_change=(
                    ("assembly_profile", profile),
                    ("compiler_version", COMPILER_VERSION),
                    ("design_evidence_hash", design_evidence_hash),
                    ("source_parent_ids", parent_ids),
                ),
                expected_effect=(
                    "Fold-local consensus membership ranked only on excluded-design "
                    "component-ledger statistics; no inherited status."
                ),
                high_risk_units=lead.high_risk_units,
                daily_loss_guard=lead.daily_loss_guard,
                daily_profit_lock=lead.daily_profit_lock,
                critical_buffer=lead.critical_buffer,
                high_zone_buffer=lead.high_zone_buffer,
                high_zone_remaining_target=lead.high_zone_remaining_target,
                middle_zone_buffer=lead.middle_zone_buffer,
                middle_zone_remaining_target=lead.middle_zone_remaining_target,
                middle_risk_units=lead.middle_risk_units,
                maximum_simultaneous_positions=lead.maximum_simultaneous_positions,
                maximum_mini_equivalent=lead.maximum_mini_equivalent,
                assembly_profile=profile,
                inherited_status=None,
            )
        )
    return tuple(policies)


def _membership_from_priority(
    priority: Sequence[str],
    *,
    size: int,
    mandatory: str,
    votes: Counter[str],
    stats: Mapping[str, Mapping[str, Any]],
    blocks: Sequence[str],
) -> tuple[str, ...]:
    if len(priority) < size:
        raise FoldCandidateCompilerError("parent union cannot fill frozen membership size")
    selected = list(priority[:size])
    if mandatory not in selected:
        if mandatory not in priority:
            raise FoldCandidateCompilerError("lead retained sleeve escaped parent union")
        selected[-1] = mandatory
    return tuple(
        sorted(
            set(selected),
            key=lambda component_id: _component_consensus_rank_key(
                component_id, votes=votes, stats=stats, blocks=blocks
            ),
        )
    )


def _component_metrics(
    component_id: str,
    *,
    stats: Mapping[str, Mapping[str, Any]],
    blocks: Sequence[str],
) -> dict[str, Any]:
    row = stats[component_id]
    stressed_by_block = [
        float(row["design_blocks"][block_id]["stressed_net_usd"])
        for block_id in blocks
    ]
    return {
        "positive_stressed_design_block_count": sum(value > 0.0 for value in stressed_by_block),
        "stressed_block_floor_net": min(stressed_by_block),
        "stressed_block_median_net": float(statistics.median(stressed_by_block)),
        "stressed_design_net": sum(stressed_by_block),
        "normal_design_net": sum(
            float(row["design_blocks"][block_id]["normal_net_usd"])
            for block_id in blocks
        ),
        "design_event_count": sum(
            int(row["design_blocks"][block_id]["event_count"])
            for block_id in blocks
        ),
    }


def _membership_metrics(
    component_ids: set[str],
    *,
    stats: Mapping[str, Mapping[str, Any]],
    blocks: Sequence[str],
) -> dict[str, Any]:
    stressed_by_block = [
        sum(
            float(stats[value]["design_blocks"][block_id]["stressed_net_usd"])
            for value in component_ids
        )
        for block_id in blocks
    ]
    component_stress = [
        _component_metrics(value, stats=stats, blocks=blocks)["stressed_design_net"]
        for value in component_ids
    ]
    positive = [max(0.0, float(value)) for value in component_stress]
    positive_total = sum(positive)
    return {
        "positive_stressed_design_block_count": sum(value > 0.0 for value in stressed_by_block),
        "stressed_block_floor_net": min(stressed_by_block),
        "stressed_block_median_net": float(statistics.median(stressed_by_block)),
        "stressed_design_net": sum(stressed_by_block),
        "normal_design_net": sum(
            float(stats[value]["design_blocks"][block_id]["normal_net_usd"])
            for value in component_ids
            for block_id in blocks
        ),
        "maximum_positive_component_stress_share": (
            max(positive, default=0.0) / positive_total if positive_total > 0.0 else 1.0
        ),
        "design_event_count": sum(
            int(stats[value]["design_blocks"][block_id]["event_count"])
            for value in component_ids
            for block_id in blocks
        ),
        "component_count": len(component_ids),
    }


def _lead_rank_key(metrics: Mapping[str, Any], policy_id: str) -> tuple[Any, ...]:
    return (
        -int(metrics["positive_stressed_design_block_count"]),
        -float(metrics["stressed_block_floor_net"]),
        -float(metrics["stressed_block_median_net"]),
        -float(metrics["stressed_design_net"]),
        -float(metrics["normal_design_net"]),
        -int(metrics["design_event_count"]),
        policy_id,
    )


def _component_consensus_rank_key(
    component_id: str,
    *,
    votes: Counter[str],
    stats: Mapping[str, Mapping[str, Any]],
    blocks: Sequence[str],
) -> tuple[Any, ...]:
    metrics = _component_metrics(component_id, stats=stats, blocks=blocks)
    return (
        -int(votes[component_id]),
        -int(metrics["positive_stressed_design_block_count"]),
        -float(metrics["stressed_block_floor_net"]),
        -float(metrics["stressed_block_median_net"]),
        -float(metrics["stressed_design_net"]),
        -float(metrics["normal_design_net"]),
        -int(metrics["design_event_count"]),
        component_id,
    )


def _group_rank_key(
    metrics: Mapping[str, Any], parent_ids: Sequence[str]
) -> tuple[Any, ...]:
    return (
        -int(metrics["positive_stressed_design_block_count"]),
        -float(metrics["stressed_block_floor_net"]),
        -float(metrics["stressed_block_median_net"]),
        -float(metrics["stressed_design_net"]),
        -float(metrics["normal_design_net"]),
        float(metrics["maximum_positive_component_stress_share"]),
        -int(metrics["design_event_count"]),
        tuple(parent_ids),
    )


def _execution_structure_hash(policy: StaticParentBasketPolicy) -> str:
    """Hash only fields that can change the V7.3 static account replay."""

    return stable_hash(
        {
            "component_priority": list(policy.component_ids),
            "daily_loss_guard": float(policy.daily_loss_guard).hex(),
            "daily_profit_lock": float(policy.daily_profit_lock).hex(),
            "critical_buffer": float(policy.critical_buffer).hex(),
            "maximum_simultaneous_positions": policy.maximum_simultaneous_positions,
            "maximum_mini_equivalent": policy.maximum_mini_equivalent,
            "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        }
    )


def _design_evidence_hash(
    stats: Mapping[str, Mapping[str, Any]], blocks: Sequence[str]
) -> str:
    return stable_hash(
        {
            "design_block_ids": list(blocks),
            "component_design_stats": [stats[value] for value in sorted(stats)],
        }
    )


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return stable_hash(payload)


__all__ = [
    "ACCEPTED_PARENT_GROUP_COUNT",
    "COMPILATION_AUDIT_SCHEMA",
    "COMPILER_MANIFEST_SCHEMA",
    "COMPILER_VERSION",
    "EXPECTED_CANDIDATE_COUNT",
    "FoldCandidateCompilation",
    "FoldCandidateCompilerError",
    "MEMBERSHIP_SIZES",
    "PARENT_GROUP_SIZES",
    "compile_fold_candidate_bank",
    "frozen_compiler_policy",
]
