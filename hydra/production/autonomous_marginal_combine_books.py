"""Bounded marginal Combine-book evaluation over the exact 0029 Tier-Q bank.

The module is intentionally read-only.  It consumes the classification emitted
by :mod:`hydra.production.autonomous_combine_candidate_bank`, reconstructs only
the admitted immutable 0029 sleeves, and replays deterministic two-to-six
component books through the authoritative causal shared-account engine.

Membership and governor selection use B1/B2 only.  B3/B4 remain held-out
development folds and are used solely by the frozen G-ready classification.
No result here mutates the registry, promotes a candidate, starts XFA, accesses
protected data, or creates an order route.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    reconstruct_exact_hazard_replay,
)
from hydra.portfolio.marginal_contribution_builder import (
    BookProposal,
    ExactBookEvaluation,
    GovernorProfile,
    MarginalContributionThresholds,
    SleeveSummary,
    assess_exact_marginal_contribution,
    build_marginal_book_proposals,
)
from hydra.production.autonomous_combine_candidate_bank import (
    SCHEMA as CANDIDATE_BANK_SCHEMA,
)
from hydra.production.autonomous_exact_continuation import (
    _require_diagnostic_safety,
    _unwrap_exact_result,
    _verify_continuation_result,
    _verify_exact_result,
    compose_remaining_0029_exact_results,
)
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    HORIZONS,
    _account_config,
    _apply_session_contract,
    _declared_stop_risk_charge_per_mini,
    _inside,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _read_verified_event_evidence,
    _require_scenario_identity,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.causal_risk_charge import (
    RISK_CHARGE_CONTRACT,
    require_causal_stop_risk_charge,
)
from hydra.production.fast_pass_runtime_helpers import (
    _book_component_key,
    _governor_profiles,
    _role_horizon_summary,
    _sprint_metrics,
    _summarize_sprint_episodes,
)
from hydra.research.causal_target_velocity import HazardOutcome


SCHEMA = "hydra_autonomous_marginal_combine_books_v1"
COMPOSITE_SCHEMA = "hydra_autonomous_marginal_combine_book_shards_v1"
POLICY_SCHEMA = "hydra_autonomous_marginal_combine_policy_v1"
RESULT_SCHEMA = "hydra_autonomous_marginal_combine_policy_result_v1"

DESIGN_BLOCKS = ("B1", "B2")
HELD_OUT_DEVELOPMENT_BLOCKS = ("B3", "B4")
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
MAXIMUM_BOOK_BATCH = 256
MAXIMUM_COMPONENTS = 6


class AutonomousMarginalCombineBooksError(RuntimeError):
    """The bounded book evaluation cannot preserve its frozen invariants."""


@dataclass(frozen=True, slots=True)
class _PreparedComponent:
    candidate_id: str
    candidate_fingerprint: str
    behavioral_fingerprint: str
    qd_cell: str
    account_label: str
    account_size_usd: int
    integer_quantity_tier: int
    source_governor_mode: str
    declared_risk_charge_per_mini: float
    normal_trajectories: tuple[Any, ...]
    stressed_trajectories: tuple[Any, ...]
    eligible_session_days: frozenset[int]
    censored_session_days: frozenset[int]
    source_receipt: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _ReplayContext:
    calendar: tuple[int, ...]
    starts: Mapping[int, tuple[tuple[int, str], ...]]
    rules: Mapping[str, Mapping[str, Any]]
    governor_profiles: tuple[GovernorProfile, ...]
    components: Mapping[str, _PreparedComponent]
    source_manifest_hash: str
    frozen_grid_hash: str
    official_rule_snapshot_hash: str
    design_cell_exclusions: Mapping[str, str] = field(default_factory=dict)


def build_autonomous_marginal_combine_books(
    root: str | Path,
    candidate_bank: Mapping[str, Any],
    initial_exact_result: Mapping[str, Any],
    continuation_results: Sequence[Mapping[str, Any]] = (),
    *,
    requested_book_count: int = MAXIMUM_BOOK_BATCH,
    maximum_components: int = MAXIMUM_COMPONENTS,
    beam_width: int | None = None,
    shard_index: int = 0,
    shard_count: int = 1,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Build and exactly replay one deterministic, bounded Tier-Q book batch.

    The function performs no writes.  Returned mappings contain only JSON-safe
    values and carry a stable result hash; running it twice against the same
    immutable evidence produces the same result.
    """

    if not 0 <= int(requested_book_count) <= MAXIMUM_BOOK_BATCH:
        raise AutonomousMarginalCombineBooksError(
            "requested book count must be in [0,256]"
        )
    if not 2 <= int(maximum_components) <= MAXIMUM_COMPONENTS:
        raise AutonomousMarginalCombineBooksError(
            "book membership must remain between two and six components"
        )
    if int(shard_count) not in {1, 2} or not 0 <= int(shard_index) < int(
        shard_count
    ):
        raise AutonomousMarginalCombineBooksError(
            "deterministic shard contract requires shard_count 1/2 and a valid index"
        )

    bank = _verify_candidate_bank(candidate_bank)
    composite, exact_results = _verified_exact_results(
        initial_exact_result, continuation_results
    )
    if str(bank["source_composite_result_hash"]) != str(composite["result_hash"]):
        raise AutonomousMarginalCombineBooksError(
            "candidate bank and exact composite provenance differ"
        )

    tier_q_rows = tuple(
        sorted(
            (
                dict(row)
                for row in bank.get("candidates", ())
                if row.get("tier_q_contract_cleared") is True
            ),
            key=lambda row: str(row["candidate_id"]),
        )
    )
    _require_tier_q_rows(tier_q_rows)
    if int(requested_book_count) == 0 or len(tier_q_rows) < 2:
        return _empty_result(
            bank=bank,
            composite=composite,
            tier_q_rows=tier_q_rows,
            requested_book_count=int(requested_book_count),
            shard_index=int(shard_index),
            shard_count=int(shard_count),
            reason=(
                "ZERO_BOOKS_REQUESTED"
                if int(requested_book_count) == 0
                else "FEWER_THAN_TWO_TIER_Q_COMPONENTS"
            ),
        )

    context = _prepare_replay_context(
        Path(root).resolve(),
        tier_q_rows,
        exact_results,
        fast_pass_manifest_path=fast_pass_manifest_path,
        rule_snapshot_path=rule_snapshot_path,
    )
    _verify_context_matches_bank(context, tier_q_rows)
    if len(context.components) < 2:
        return _empty_result(
            bank=bank,
            composite=composite,
            tier_q_rows=tier_q_rows,
            requested_book_count=int(requested_book_count),
            shard_index=int(shard_index),
            shard_count=int(shard_count),
            reason="FEWER_THAN_TWO_B1_B2_SAFE_TIER_Q_COMPONENTS",
            design_cell_exclusions=context.design_cell_exclusions,
        )

    singleton_results = _evaluate_singletons(context)
    standalone_results = _classify_singletons(singleton_results)
    standalone_g_ready_ids = sorted(
        str(row["policy_id"])
        for row in standalone_results
        if row["g_ready"] is True
    )
    proposal_inventory = _bounded_proposals(
        context,
        singleton_results,
        requested_count=int(requested_book_count),
        maximum_components=int(maximum_components),
        beam_width=beam_width,
    )
    inventory_rows = _proposal_inventory(proposal_inventory)
    inventory_hash = stable_hash(inventory_rows)
    proposals = [
        proposal
        for position, proposal in enumerate(proposal_inventory)
        if position % int(shard_count) == int(shard_index)
    ]
    evaluated, primary = _evaluate_proposals(
        proposals,
        context=context,
        singleton_results=singleton_results,
    )

    pass_counters = _pass_counters(primary)
    g_ready_ids = sorted(
        str(row["policy_id"]) for row in primary if row["g_ready"] is True
    )
    marginal_ids = sorted(
        str(row["policy_id"])
        for row in primary
        if row["marginally_accepted"] is True
    )
    supporting = [
        _compact_policy_result(row)
        for row in sorted(evaluated.values(), key=lambda row: str(row["policy_id"]))
    ]
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "COMPLETE_BOUNDED_EXACT_MARGINAL_COMBINE_BOOK_BATCH",
        "source_candidate_bank_hash": str(bank["result_hash"]),
        "source_composite_result_hash": str(composite["result_hash"]),
        "source_manifest_hash": context.source_manifest_hash,
        "frozen_grid_hash": context.frozen_grid_hash,
        "official_rule_snapshot_hash": context.official_rule_snapshot_hash,
        "selection_contract": {
            "admissible_input_tier": "Q",
            "design_blocks": list(DESIGN_BLOCKS),
            "held_out_development_blocks": list(
                HELD_OUT_DEVELOPMENT_BLOCKS
            ),
            "membership_and_marginal_selection_uses": "B1_B2_ONLY",
            "g_ready_evaluation_uses": "B3_B4_ONLY",
            "maximum_books": MAXIMUM_BOOK_BATCH,
            "minimum_components": 2,
            "maximum_components": int(maximum_components),
            "no_quantity_rescaling_inside_book": True,
            "router_static_risk_tier": 1.0,
            "no_mutation": True,
            "no_authoritative_promotion": True,
            "no_xfa": True,
        },
        "shard": {
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
            "partition_rule": "DETERMINISTIC_INVENTORY_POSITION_MODULO_SHARD_COUNT_V1",
            "proposal_inventory_count": len(proposal_inventory),
            "proposal_inventory_hash": inventory_hash,
            "proposal_inventory_policy_ids": [
                str(row["runtime_policy_id"]) for row in inventory_rows
            ],
            "selected_primary_policy_ids": [
                str(row["policy_id"]) for row in primary
            ],
            "ranking_recomputed_after_sharding": False,
        },
        "tier_q_component_ids": sorted(context.components),
        "design_cell_exclusions": dict(sorted(context.design_cell_exclusions.items())),
        "component_freeze": [
            _component_receipt(context.components[candidate_id])
            for candidate_id in sorted(context.components)
        ],
        "standalone_results": standalone_results,
        "proposals": [row["proposal"] for row in primary],
        "book_results": primary,
        "supporting_policy_results": supporting,
        "counts": {
            "tier_q_component_count": len(context.components),
            "account_size_group_count": len(
                {row.account_label for row in context.components.values()}
            ),
            "book_proposal_count": len(proposals),
            "proposal_inventory_count": len(proposal_inventory),
            "primary_book_exact_replay_count": len(primary),
            "supporting_policy_exact_replay_count": len(evaluated),
            "completed_episode_count": sum(
                int(row.get("completed_episode_count", 0)) for row in supporting
            ),
            "data_censored_episode_count": sum(
                int(row.get("data_censored_episode_count", 0))
                for row in supporting
            ),
            "marginally_accepted_count": len(marginal_ids),
            "standalone_g_ready_count": len(standalone_g_ready_ids),
            "g_ready_count": len(g_ready_ids),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "broker_connections": 0,
            "orders": 0,
            **pass_counters,
        },
        "candidate_ids": {
            "marginally_accepted": marginal_ids,
            "standalone_g_ready": standalone_g_ready_ids,
            "g_ready": g_ready_ids,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "next_action": (
            "PERSIST_G_READY_CLASSIFICATIONS_WITHOUT_PROMOTION_AND_RUN_REQUIRED_CONTROLS"
            if g_ready_ids or standalone_g_ready_ids
            else "CONTINUE_NEXT_BOUNDED_TIER_Q_MARGINAL_BOOK_BATCH_OR_DISTINCT_BRANCH"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def compose_autonomous_marginal_combine_book_shards(
    shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reconcile complete deterministic shards without replaying economics."""

    values = [_verify_shard_result(value) for value in shards]
    if not values:
        raise AutonomousMarginalCombineBooksError("at least one shard is required")
    declared_count = int(dict(values[0]["shard"])["shard_count"])
    if declared_count not in {1, 2} or len(values) != declared_count:
        raise AutonomousMarginalCombineBooksError(
            "shard set is incomplete for its frozen shard count"
        )
    shared_fields = (
        "source_candidate_bank_hash",
        "source_composite_result_hash",
        "source_manifest_hash",
        "frozen_grid_hash",
        "official_rule_snapshot_hash",
        "selection_contract",
        "tier_q_component_ids",
        "design_cell_exclusions",
        "component_freeze",
        "standalone_results",
    )
    for field_name in shared_fields:
        expected = stable_hash(values[0].get(field_name))
        if any(stable_hash(value.get(field_name)) != expected for value in values[1:]):
            raise AutonomousMarginalCombineBooksError(
                f"shard shared field differs: {field_name}"
            )
    inventory_hash = str(dict(values[0]["shard"])["proposal_inventory_hash"])
    inventory_ids = tuple(
        str(value)
        for value in dict(values[0]["shard"])["proposal_inventory_policy_ids"]
    )
    if len(inventory_ids) != len(set(inventory_ids)):
        raise AutonomousMarginalCombineBooksError(
            "proposal inventory contains duplicate policy IDs"
        )
    indexes: set[int] = set()
    selected_sets: list[set[str]] = []
    for value in values:
        shard = dict(value["shard"])
        index = int(shard["shard_index"])
        if (
            int(shard["shard_count"]) != declared_count
            or str(shard["proposal_inventory_hash"]) != inventory_hash
            or tuple(str(row) for row in shard["proposal_inventory_policy_ids"])
            != inventory_ids
            or int(shard["proposal_inventory_count"]) != len(inventory_ids)
            or index in indexes
        ):
            raise AutonomousMarginalCombineBooksError(
                "shard inventory/provenance/index drift"
            )
        indexes.add(index)
        selected = {
            str(row) for row in shard["selected_primary_policy_ids"]
        }
        if selected != {str(row["policy_id"]) for row in value["book_results"]}:
            raise AutonomousMarginalCombineBooksError(
                "shard selected IDs differ from book results"
            )
        selected_sets.append(selected)
    if indexes != set(range(declared_count)):
        raise AutonomousMarginalCombineBooksError("shard indexes are incomplete")
    for left_index, left in enumerate(selected_sets):
        if any(left & right for right in selected_sets[left_index + 1 :]):
            raise AutonomousMarginalCombineBooksError(
                "primary book policy appears in more than one shard"
            )
    if set().union(*selected_sets) != set(inventory_ids):
        raise AutonomousMarginalCombineBooksError(
            "shard union does not exhaust the frozen proposal inventory"
        )

    book_by_id = _deduplicate_rows(values, "book_results")
    proposal_by_id: dict[str, dict[str, Any]] = {}
    for value in values:
        for raw in value["proposals"]:
            row = dict(raw)
            policy_id = str(row["runtime_policy_id"])
            previous = proposal_by_id.get(policy_id)
            if previous is not None and stable_hash(previous) != stable_hash(row):
                raise AutonomousMarginalCombineBooksError(
                    f"proposal collision across shards: {policy_id}"
                )
            proposal_by_id[policy_id] = row
    if set(book_by_id) != set(proposal_by_id) or set(book_by_id) != set(inventory_ids):
        raise AutonomousMarginalCombineBooksError(
            "composite proposal/book inventory reconciliation failed"
        )
    supporting_by_id = _deduplicate_rows(values, "supporting_policy_results")
    ordered_books = [book_by_id[policy_id] for policy_id in inventory_ids]
    ordered_proposals = [proposal_by_id[policy_id] for policy_id in inventory_ids]
    supporting = [supporting_by_id[key] for key in sorted(supporting_by_id)]
    marginal_ids = sorted(
        str(row["policy_id"])
        for row in ordered_books
        if row["marginally_accepted"] is True
    )
    g_ready_ids = sorted(
        str(row["policy_id"]) for row in ordered_books if row["g_ready"] is True
    )
    standalone = list(values[0]["standalone_results"])
    standalone_g_ready = sorted(
        str(row["policy_id"]) for row in standalone if row["g_ready"] is True
    )
    counts = {
        "tier_q_component_count": len(values[0]["tier_q_component_ids"]),
        "account_size_group_count": int(
            dict(values[0]["counts"])["account_size_group_count"]
        ),
        "proposal_inventory_count": len(inventory_ids),
        "book_proposal_count": len(inventory_ids),
        "primary_book_exact_replay_count": len(ordered_books),
        "supporting_policy_exact_replay_count": len(supporting),
        "completed_episode_count": sum(
            int(row.get("completed_episode_count", 0)) for row in supporting
        ),
        "data_censored_episode_count": sum(
            int(row.get("data_censored_episode_count", 0)) for row in supporting
        ),
        "marginally_accepted_count": len(marginal_ids),
        "standalone_g_ready_count": len(standalone_g_ready),
        "g_ready_count": len(g_ready_ids),
        "authoritative_promotion_count": 0,
        "xfa_paths_started": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "broker_connections": 0,
        "orders": 0,
        **_pass_counters(ordered_books),
    }
    core: dict[str, Any] = {
        "schema": COMPOSITE_SCHEMA,
        "status": "COMPLETE_RECONCILED_MARGINAL_COMBINE_BOOK_SHARDS",
        "source_candidate_bank_hash": values[0]["source_candidate_bank_hash"],
        "source_composite_result_hash": values[0]["source_composite_result_hash"],
        "source_manifest_hash": values[0]["source_manifest_hash"],
        "frozen_grid_hash": values[0]["frozen_grid_hash"],
        "official_rule_snapshot_hash": values[0]["official_rule_snapshot_hash"],
        "selection_contract": values[0]["selection_contract"],
        "tier_q_component_ids": values[0]["tier_q_component_ids"],
        "design_cell_exclusions": values[0]["design_cell_exclusions"],
        "component_freeze": values[0]["component_freeze"],
        "standalone_results": standalone,
        "proposal_inventory": {
            "count": len(inventory_ids),
            "hash": inventory_hash,
            "policy_ids": list(inventory_ids),
        },
        "shard_receipts": [
            {
                "shard_index": int(dict(row["shard"])["shard_index"]),
                "result_hash": str(row["result_hash"]),
                "selected_primary_policy_ids": list(
                    dict(row["shard"])["selected_primary_policy_ids"]
                ),
            }
            for row in sorted(
                values, key=lambda row: int(dict(row["shard"])["shard_index"])
            )
        ],
        "proposals": ordered_proposals,
        "book_results": ordered_books,
        "supporting_policy_results": supporting,
        "counts": counts,
        "candidate_ids": {
            "marginally_accepted": marginal_ids,
            "standalone_g_ready": standalone_g_ready,
            "g_ready": g_ready_ids,
        },
        "promotion_status": None,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "independent_confirmation_claimed": False,
        "next_action": (
            "PERSIST_G_READY_CLASSIFICATIONS_WITHOUT_PROMOTION_AND_RUN_REQUIRED_CONTROLS"
            if g_ready_ids or standalone_g_ready
            else "CONTINUE_NEXT_BOUNDED_TIER_Q_MARGINAL_BOOK_BATCH_OR_DISTINCT_BRANCH"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def _verify_shard_result(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    if (
        not claimed
        or stable_hash(row) != claimed
        or value.get("schema") != SCHEMA
        or value.get("status")
        != "COMPLETE_BOUNDED_EXACT_MARGINAL_COMBINE_BOOK_BATCH"
    ):
        raise AutonomousMarginalCombineBooksError(
            "marginal Combine shard identity/hash drift"
        )
    return dict(value)


def _deduplicate_rows(
    shards: Sequence[Mapping[str, Any]], field_name: str
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for shard in shards:
        for raw in shard[field_name]:
            row = dict(raw)
            policy_id = str(row.get("policy_id") or "")
            result_hash = str(row.get("result_hash") or "")
            if not policy_id or not result_hash:
                raise AutonomousMarginalCombineBooksError(
                    f"{field_name} row lacks policy identity/hash"
                )
            prior = output.get(policy_id)
            if prior is not None and (
                str(prior.get("result_hash")) != result_hash
                or stable_hash(prior) != stable_hash(row)
            ):
                raise AutonomousMarginalCombineBooksError(
                    f"policy result collision across shards: {policy_id}"
                )
            output[policy_id] = row
    return output


def _verify_candidate_bank(value: Mapping[str, Any]) -> dict[str, Any]:
    bank = dict(value)
    claimed = str(bank.pop("result_hash", ""))
    if (
        not claimed
        or stable_hash(bank) != claimed
        or value.get("schema") != CANDIDATE_BANK_SCHEMA
        or value.get("status")
        != "COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION"
    ):
        raise AutonomousMarginalCombineBooksError(
            "autonomous Combine candidate-bank identity/hash drift"
        )
    counts = dict(value.get("counts") or {})
    forbidden = (
        "authoritative_promotion_count",
        "xfa_paths_started",
    )
    if any(int(counts.get(key, 0)) for key in forbidden) or any(
        int(value.get(key, 0))
        for key in (
            "q4_access_count_delta",
            "data_purchase_count",
            "broker_connections",
            "orders",
        )
    ):
        raise AutonomousMarginalCombineBooksError(
            "candidate bank contains a forbidden side effect"
        )
    return dict(value)


def _verified_exact_results(
    initial: Mapping[str, Any], continuations: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    composite = compose_remaining_0029_exact_results(initial, continuations)
    results = [_unwrap_exact_result(initial)]
    for value in continuations:
        wrapper = _verify_continuation_result(value)
        results.append(dict(wrapper["exact_result"]))
    for result in results:
        _verify_exact_result(result)
        _require_diagnostic_safety(result)
    return dict(composite), tuple(results)


def _require_tier_q_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    ids = [str(row.get("candidate_id") or "") for row in rows]
    if len(ids) != len(set(ids)) or any(not value for value in ids):
        raise AutonomousMarginalCombineBooksError(
            "Tier-Q candidate identities are missing or duplicated"
        )
    for row in rows:
        if (
            row.get("tier_q_contract_cleared") is not True
            or row.get("computed_development_tier") != "Q"
            or not isinstance(row.get("best_safe_cell"), Mapping)
            or not bool(
                dict(row.get("compact_evidence_bundle") or {}).get("complete")
            )
            or row.get("authoritative_promotion_status") is not None
        ):
            raise AutonomousMarginalCombineBooksError(
                "non-Tier-Q or status-inherited candidate entered book assembly"
            )


def _prepare_replay_context(
    project: Path,
    tier_q_rows: Sequence[Mapping[str, Any]],
    exact_results: Sequence[Mapping[str, Any]],
    *,
    fast_pass_manifest_path: str | Path,
    rule_snapshot_path: str | Path,
) -> _ReplayContext:
    manifest_file = _inside(project, fast_pass_manifest_path)
    rule_file = _inside(project, rule_snapshot_path)
    manifest = _load_self_hashed_manifest(manifest_file)
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(rule_file)
    profiles = _governor_profiles(manifest)
    bank_entries, _bank_receipt = _load_banks(project)
    exact_by_hash = {str(row["result_hash"]): dict(row) for row in exact_results}
    if len(exact_by_hash) != len(exact_results):
        raise AutonomousMarginalCombineBooksError(
            "duplicate exact-result hash in source artifacts"
        )

    components: dict[str, _PreparedComponent] = {}
    design_exclusions: dict[str, str] = {}
    for classified in tier_q_rows:
        candidate_id = str(classified["candidate_id"])
        source_hash = str(classified["source_exact_result_hash"])
        source = exact_by_hash.get(source_hash)
        if source is None:
            raise AutonomousMarginalCombineBooksError(
                f"Tier-Q source exact result is absent: {candidate_id}"
            )
        exact_candidates = [
            dict(row)
            for row in source.get("results", ())
            if str(row.get("candidate_id")) == candidate_id
        ]
        if len(exact_candidates) != 1:
            raise AutonomousMarginalCombineBooksError(
                f"Tier-Q exact candidate is absent or duplicated: {candidate_id}"
            )
        exact_candidate = exact_candidates[0]
        selected_pair = _select_b1_b2_safe_cell(exact_candidate)
        if selected_pair is None:
            design_exclusions[candidate_id] = (
                "NO_B1_B2_SAFE_NORMAL_AND_STRESSED_CELL"
            )
            continue
        exact_cell, design_selection = selected_pair
        account_label = str(exact_cell["account_label"])
        if account_label not in rules:
            raise AutonomousMarginalCombineBooksError(
                f"unsupported Tier-Q account label: {account_label}"
            )
        bundle = dict(classified["compact_evidence_bundle"])
        matching_entries = [
            dict(entry)
            for entry in bank_entries
            if str(entry.get("candidate_id")) == candidate_id
            and str(entry.get("candidate_fingerprint"))
            == str(classified["candidate_fingerprint"])
            and str(dict(entry.get("event_evidence") or {}).get("sha256"))
            == str(bundle.get("source_event_file_sha256"))
        ]
        semantic_hashes = {
            stable_hash(
                {
                    key: row.get(key)
                    for key in (
                        "candidate_id",
                        "candidate",
                        "candidate_fingerprint",
                        "realized_behavioral_fingerprint",
                        "qd_cell",
                        "eligible_session_days",
                        "event_evidence",
                        "exact_hashes",
                    )
                }
            )
            for row in matching_entries
        }
        if not matching_entries or len(semantic_hashes) != 1:
            raise AutonomousMarginalCombineBooksError(
                f"immutable 0029 bank evidence differs for {candidate_id}"
            )
        entry = min(matching_entries, key=lambda row: int(row.get("_source_wave", 0)))
        events, event_receipt = _read_verified_event_evidence(
            project, dict(entry["event_evidence"])
        )
        replay = reconstruct_exact_hazard_replay(
            candidate_payload=entry["candidate"],
            event_mappings=events,
            eligible_session_days=entry["eligible_session_days"],
            expected_hashes=entry["exact_hashes"],
        )
        normal, normal_violations = _apply_session_contract(
            replay.normal_trajectories
        )
        stressed, stressed_violations = _apply_session_contract(
            replay.stressed_trajectories
        )
        if normal_violations or stressed_violations or normal_violations != stressed_violations:
            raise AutonomousMarginalCombineBooksError(
                f"Tier-Q session contract drift: {candidate_id}"
            )
        tier = int(exact_cell["integer_quantity_tier"])
        scaled_normal = tuple(
            scale_causal_trajectory(row, executable_quantity_multiplier=tier)
            for row in normal
        )
        scaled_stressed = tuple(
            scale_causal_trajectory(row, executable_quantity_multiplier=tier)
            for row in stressed
        )
        _require_scenario_identity(scaled_normal, scaled_stressed)
        risk_charge = _declared_stop_risk_charge_per_mini(
            events, entry["candidate"]
        )
        censored = frozenset(
            int(row.session_day)
            for row in replay.events
            if str(getattr(row.outcome, "value", row.outcome))
            == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
        )
        components[candidate_id] = _PreparedComponent(
            candidate_id=candidate_id,
            candidate_fingerprint=str(classified["candidate_fingerprint"]),
            behavioral_fingerprint=str(
                classified["realized_behavioral_fingerprint"]
            ),
            qd_cell=str(classified["qd_cell"]),
            account_label=account_label,
            account_size_usd=int(exact_cell["account_size_usd"]),
            integer_quantity_tier=tier,
            source_governor_mode=str(exact_cell["risk_governor_mode"]),
            declared_risk_charge_per_mini=float(risk_charge),
            normal_trajectories=scaled_normal,
            stressed_trajectories=scaled_stressed,
            eligible_session_days=frozenset(
                int(value) for value in replay.eligible_session_days
            ),
            censored_session_days=censored,
            source_receipt={
                "source_exact_result_hash": source_hash,
                "source_candidate_result_hash": exact_candidate.get(
                    "candidate_result_hash"
                ),
                "source_event_file_sha256": event_receipt["sha256"],
                "source_event_content_sha256": event_receipt[
                    "uncompressed_sha256"
                ],
                # This hash and every executable selector field are derived
                # only from B1/B2.  The full source-cell hash is provenance,
                # never a selector or policy input.
                "selected_cell_hash": design_selection["selection_hash"],
                "selected_cell_identity": design_selection["cell_identity"],
                "design_metrics": design_selection["design_metrics"],
                "source_full_cell_hash": stable_hash(exact_cell),
                "selection_outcome_role": "B1_B2_DESIGN_ONLY",
                "quantity_scaling_applied_exactly_once": True,
            },
        )
    _assert_unique_component_event_ids(components)
    return _ReplayContext(
        calendar=tuple(int(value) for value in calendar),
        starts={
            int(horizon): tuple((int(day), str(block)) for day, block in values)
            for horizon, values in starts.items()
        },
        rules=rules,
        governor_profiles=profiles,
        components=components,
        source_manifest_hash=str(manifest["manifest_hash"]),
        frozen_grid_hash=str(grid_receipt["grid_hash"]),
        official_rule_snapshot_hash=str(rule_receipt["parsed_rule_hash"]),
        design_cell_exclusions=design_exclusions,
    )


def _verify_context_matches_bank(
    context: _ReplayContext, tier_q_rows: Sequence[Mapping[str, Any]]
) -> None:
    tier_q_ids = {str(row["candidate_id"]) for row in tier_q_rows}
    if (
        set(context.components) & set(context.design_cell_exclusions)
        or set(context.components) | set(context.design_cell_exclusions)
        != tier_q_ids
    ):
        raise AutonomousMarginalCombineBooksError(
            "prepared/excluded component inventory differs from Tier-Q bank"
        )
    for row in tier_q_rows:
        if str(row["candidate_id"]) not in context.components:
            continue
        bundle = dict(row["compact_evidence_bundle"])
        if (
            str(bundle.get("source_manifest_hash")) != context.source_manifest_hash
            or str(bundle.get("frozen_grid_hash")) != context.frozen_grid_hash
            or str(bundle.get("official_rule_snapshot_hash"))
            != context.official_rule_snapshot_hash
        ):
            raise AutonomousMarginalCombineBooksError(
                "Tier-Q source manifest/rule/grid hash drift"
            )


def _select_b1_b2_safe_cell(
    candidate: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Freeze an executable cell using B1/B2 fields and nothing else.

    The exact 0029 summaries retain block-level episode/pass/MLL/net metrics.
    They do not retain block-level consistency paths, so consistency is not
    invented here.  It is evaluated exactly when the selected quantity is
    replayed.  B3/B4 values and aggregate pass rates are deliberately never
    read by this selector.
    """

    eligible: list[tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]] = []
    for raw in candidate.get("frontier", ()):
        cell = dict(raw)
        if (
            cell.get("legally_executable") is not True
            or cell.get("account_rule_compliant") is not True
            or int(cell.get("hard_compliance_failure_count", 0)) != 0
            or not isinstance(cell.get("normal"), Mapping)
            or not isinstance(cell.get("stressed"), Mapping)
        ):
            continue
        normal = _design_block_metrics(dict(cell["normal"]))
        stressed = _design_block_metrics(dict(cell["stressed"]))
        paths_complete = bool(
            dict(cell["normal"]).get("episode_path_hash")
            and dict(cell["stressed"]).get("episode_path_hash")
        )
        safe = bool(
            paths_complete
            and normal["episode_count"] > 0
            and stressed["episode_count"] > 0
            and normal["pass_count"] > 0
            and stressed["pass_count"] > 0
            and stressed["net_total_usd"] > 0.0
            and stressed["mll_breach_rate"] <= 0.10
        )
        if not safe:
            continue
        identity = {
            "account_label": str(cell["account_label"]),
            "account_size_usd": int(cell["account_size_usd"]),
            "integer_quantity_tier": int(cell["integer_quantity_tier"]),
            "risk_governor_mode": str(cell["risk_governor_mode"]),
            "horizon_trading_days": int(cell["horizon_trading_days"]),
        }
        design_metrics = {"normal": normal, "stressed": stressed}
        # Deterministic ordering matches the previous economic preference but
        # every outcome-dependent term below is built solely from B1/B2.
        rank = (
            float(stressed["pass_rate"]),
            float(normal["pass_rate"]),
            -float(stressed["mll_breach_rate"]),
            -int(identity["horizon_trading_days"]),
            float(stressed["net_total_usd"]),
            -int(identity["account_size_usd"]),
            -int(identity["integer_quantity_tier"]),
            stable_hash(identity),
        )
        selection_core = {
            "schema": "hydra_b1_b2_safe_cell_selection_v1",
            "candidate_id": str(candidate["candidate_id"]),
            "selection_role": "DESIGN",
            "selection_blocks": list(DESIGN_BLOCKS),
            "cell_identity": identity,
            "design_metrics": design_metrics,
            "b3_b4_fields_used": False,
            "aggregate_summary_fields_used": False,
        }
        receipt = {
            **selection_core,
            "selection_hash": stable_hash(selection_core),
        }
        eligible.append((rank, cell, receipt))
    if not eligible:
        return None
    _rank, selected, receipt = max(eligible, key=lambda value: value[0])
    return selected, receipt


def _design_block_metrics(scenario: Mapping[str, Any]) -> dict[str, Any]:
    by_block = dict(scenario.get("by_block") or {})
    selected = [dict(by_block.get(block) or {}) for block in DESIGN_BLOCKS]
    episode_count = sum(int(row.get("episode_count", 0)) for row in selected)
    pass_count = sum(int(row.get("pass_count", 0)) for row in selected)
    mll_count = sum(int(row.get("mll_breach_count", 0)) for row in selected)
    return {
        "episode_count": episode_count,
        "pass_count": pass_count,
        "pass_rate": pass_count / episode_count if episode_count else 0.0,
        "mll_breach_count": mll_count,
        "mll_breach_rate": mll_count / episode_count if episode_count else 0.0,
        "net_total_usd": sum(
            float(row.get("net_total_usd", 0.0)) for row in selected
        ),
    }


def _identity_profile() -> GovernorProfile:
    return GovernorProfile(
        profile_id="tier_q_selected_quantity_identity_v1",
        signal_quality_tiers=(1.0,),
        open_risk_ceiling_fraction=1.0,
        daily_loss_budget_fraction=1.0,
        daily_profit_lock_fraction=1.0,
        maximum_concurrent_sleeves=1,
        target_protection_fraction=0.0,
        same_instrument_conflict_policy="priority",
    )


def _evaluate_singletons(
    context: _ReplayContext,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    identity = _identity_profile()
    for candidate_id in sorted(context.components):
        component = context.components[candidate_id]
        spec = _policy_spec(
            account_label=component.account_label,
            members=(candidate_id,),
            profile=identity,
            components=context.components,
            policy_role="TIER_Q_STANDALONE_REFERENCE",
            predecessor_policy_id=None,
        )
        output[candidate_id] = _evaluate_policy_spec(spec, context)
    return output


def _classify_singletons(
    singleton_results: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for candidate_id, raw in sorted(singleton_results.items()):
        row = _compact_policy_result(raw)
        row["source_candidate_id"] = candidate_id
        row["classification_role"] = "TIER_Q_STANDALONE_REFERENCE"
        row["marginal_contribution"] = {
            "status": "NOT_APPLICABLE_SINGLETON",
            "reason": "A_ONE_SLEEVE_ACCOUNT_POLICY_HAS_NO_SMALLER_BOOK_PARENT",
        }
        gates = _g_ready_gates(row, singleton=True)
        row["g_ready_gate_results"] = gates
        row["g_ready"] = all(gates.values())
        row["computed_development_tier"] = (
            "G_READY" if row["g_ready"] else "Q_STANDALONE_DIAGNOSTIC"
        )
        row["authoritative_promotion_status"] = None
        row["result_hash"] = stable_hash(
            {key: value for key, value in row.items() if key != "result_hash"}
        )
        output.append(row)
    return output


def _bounded_proposals(
    context: _ReplayContext,
    singleton_results: Mapping[str, Mapping[str, Any]],
    *,
    requested_count: int,
    maximum_components: int,
    beam_width: int | None,
) -> list[tuple[str, BookProposal]]:
    by_account: dict[str, list[SleeveSummary]] = defaultdict(list)
    for candidate_id, component in sorted(context.components.items()):
        result = singleton_results[candidate_id]
        by_account[component.account_label].append(
            SleeveSummary(
                sleeve_id=candidate_id,
                qd_cell=component.qd_cell,
                behavioral_fingerprint=component.behavioral_fingerprint,
                metrics=_sprint_metrics(
                    result["summaries_by_role"]["DESIGN"]["STRESSED_1_5X"]
                ),
            )
        )
    group_proposals: dict[str, list[BookProposal]] = {}
    for account_label, sleeves in sorted(by_account.items()):
        if len(sleeves) < 2:
            continue
        group_proposals[account_label] = build_marginal_book_proposals(
            sleeves,
            context.governor_profiles,
            requested_count=requested_count,
            maximum_sleeves=maximum_components,
            beam_width=beam_width,
            maximum_members_per_qd_cell=2,
        )
    output: list[tuple[str, BookProposal]] = []
    depth = 0
    while len(output) < requested_count:
        progressed = False
        for account_label in sorted(group_proposals):
            rows = group_proposals[account_label]
            if depth < len(rows):
                output.append((account_label, rows[depth]))
                progressed = True
                if len(output) >= requested_count:
                    break
        if not progressed:
            break
        depth += 1
    return output


def _proposal_inventory(
    proposals: Sequence[tuple[str, BookProposal]],
) -> list[dict[str, Any]]:
    rows = [
        {
            "position": position,
            "account_label": account_label,
            "source_proposal_id": proposal.book_id,
            "runtime_policy_id": _policy_id(
                account_label,
                proposal.governor_profile_id,
                proposal.sleeve_ids,
            ),
            "component_ids": list(proposal.sleeve_ids),
            "predecessor_component_ids": list(proposal.predecessor_sleeve_ids),
            "governor_profile_id": proposal.governor_profile_id,
            "structural_fingerprint": proposal.structural_fingerprint,
        }
        for position, (account_label, proposal) in enumerate(proposals)
    ]
    identifiers = [str(row["runtime_policy_id"]) for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise AutonomousMarginalCombineBooksError(
            "deterministic proposal inventory contains duplicate policy IDs"
        )
    return rows


def _evaluate_proposals(
    proposals: Sequence[tuple[str, BookProposal]],
    *,
    context: _ReplayContext,
    singleton_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    profile_by_id = {row.profile_id: row for row in context.governor_profiles}
    evaluated: dict[str, dict[str, Any]] = {
        str(row["policy_id"]): dict(row) for row in singleton_results.values()
    }
    primary: list[dict[str, Any]] = []
    for account_label, proposal in proposals:
        profile = profile_by_id.get(proposal.governor_profile_id)
        if profile is None:
            raise AutonomousMarginalCombineBooksError(
                "proposal references an unknown frozen governor"
            )
        if any(
            context.components[value].account_label != account_label
            for value in proposal.sleeve_ids
        ):
            raise AutonomousMarginalCombineBooksError(
                "book proposal mixes account sizes"
            )
        candidate_spec = _policy_spec(
            account_label=account_label,
            members=proposal.sleeve_ids,
            profile=profile,
            components=context.components,
            policy_role="MARGINAL_COMBINE_BOOK_CANDIDATE",
            predecessor_policy_id=_policy_id(
                account_label,
                profile.profile_id,
                proposal.predecessor_sleeve_ids,
            ),
        )
        predecessor_spec = _policy_spec(
            account_label=account_label,
            members=proposal.predecessor_sleeve_ids,
            profile=profile,
            components=context.components,
            policy_role="PRECEDING_SMALLER_BOOK_CONTROL",
            predecessor_policy_id=None,
        )
        for spec in (predecessor_spec, candidate_spec):
            policy_id = str(spec["policy_id"])
            if policy_id not in evaluated:
                evaluated[policy_id] = _evaluate_policy_spec(spec, context)
        candidate_result = evaluated[str(candidate_spec["policy_id"])]
        predecessor_result = evaluated[str(predecessor_spec["policy_id"])]
        best_component_id = max(
            proposal.sleeve_ids,
            key=lambda value: _book_component_key(
                singleton_results[value]["summaries_by_role"]["DESIGN"][
                    "STRESSED_1_5X"
                ]
            ),
        )
        best_component = singleton_results[best_component_id]
        decision = assess_exact_marginal_contribution(
            ExactBookEvaluation(
                book_id=str(candidate_result["policy_id"]),
                sleeve_ids=tuple(proposal.sleeve_ids),
                metrics=_sprint_metrics(
                    candidate_result["summaries_by_role"]["DESIGN"][
                        "STRESSED_1_5X"
                    ]
                ),
            ),
            ExactBookEvaluation(
                book_id=str(predecessor_result["policy_id"]),
                sleeve_ids=tuple(proposal.predecessor_sleeve_ids),
                metrics=_sprint_metrics(
                    predecessor_result["summaries_by_role"]["DESIGN"][
                        "STRESSED_1_5X"
                    ]
                ),
            ),
            ExactBookEvaluation(
                book_id=str(best_component["policy_id"]),
                sleeve_ids=(best_component_id,),
                metrics=_sprint_metrics(
                    best_component["summaries_by_role"]["DESIGN"][
                        "STRESSED_1_5X"
                    ]
                ),
            ),
            thresholds=MarginalContributionThresholds(),
        )
        result = _compact_policy_result(candidate_result)
        result.update(
            {
                "classification_role": "MARGINAL_COMBINE_BOOK_CANDIDATE",
                "proposal": {
                    **asdict(proposal),
                    "account_label": account_label,
                    "runtime_policy_id": candidate_result["policy_id"],
                },
                "best_component_id": best_component_id,
                "best_component_policy_id": best_component["policy_id"],
                "predecessor_policy_id": predecessor_result["policy_id"],
                "marginal_contribution": asdict(decision),
                "marginally_accepted": bool(decision.accepted),
            }
        )
        gates = _g_ready_gates(result)
        result["g_ready_gate_results"] = gates
        result["g_ready"] = all(gates.values())
        result["computed_development_tier"] = (
            "G_READY" if result["g_ready"] else "Q_BOOK_DIAGNOSTIC"
        )
        result["authoritative_promotion_status"] = None
        result["result_hash"] = stable_hash(
            {key: value for key, value in result.items() if key != "result_hash"}
        )
        primary.append(result)
    return evaluated, primary


def _policy_id(
    account_label: str, profile_id: str, members: Sequence[str]
) -> str:
    return "autonomous_marginal_book_" + stable_hash(
        {
            "account_label": str(account_label),
            "governor_profile_id": str(profile_id),
            "component_ids": list(members),
        }
    )[:24]


def _policy_spec(
    *,
    account_label: str,
    members: Sequence[str],
    profile: GovernorProfile,
    components: Mapping[str, _PreparedComponent],
    policy_role: str,
    predecessor_policy_id: str | None,
) -> dict[str, Any]:
    component_ids = tuple(str(value) for value in members)
    if not 1 <= len(component_ids) <= MAXIMUM_COMPONENTS:
        raise AutonomousMarginalCombineBooksError(
            "supporting policy membership escaped [1,6]"
        )
    if len(set(component_ids)) != len(component_ids):
        raise AutonomousMarginalCombineBooksError(
            "supporting policy contains duplicate components"
        )
    if any(value not in components for value in component_ids) or any(
        components[value].account_label != account_label for value in component_ids
    ):
        raise AutonomousMarginalCombineBooksError(
            "policy membership is missing or mixes account sizes"
        )
    executable_core: dict[str, Any] = {
        "schema": POLICY_SCHEMA,
        "policy_id": _policy_id(account_label, profile.profile_id, component_ids),
        "account_label": account_label,
        "component_ids": list(component_ids),
        "component_quantity_tiers": {
            value: int(components[value].integer_quantity_tier)
            for value in component_ids
        },
        "component_source_governors": {
            value: str(components[value].source_governor_mode)
            for value in component_ids
        },
        "risk_charge_contract": RISK_CHARGE_CONTRACT,
        "governor_profile": asdict(profile),
        "quantity_tiers_materialized_before_book_replay": True,
        "book_static_risk_tier": 1.0,
        "additional_quantity_scaling": False,
        "signal_recomputation_performed": False,
    }
    # Candidate/predecessor/singleton are evaluation roles, not executable
    # semantics.  The same account policy can occupy different graph roles in
    # different shards and must retain one canonical identity and result hash.
    return {
        **executable_core,
        "policy_spec_hash": stable_hash(executable_core),
        "evaluation_metadata": {
            "policy_role": str(policy_role),
            "predecessor_policy_id": predecessor_policy_id,
        },
    }


def _active_policy(
    spec: Mapping[str, Any], context: _ReplayContext
) -> ActiveRiskPoolPolicy:
    members = tuple(str(value) for value in spec["component_ids"])
    profile = dict(spec["governor_profile"])
    rule = dict(context.rules[str(spec["account_label"])])
    target = float(rule["profit_target_usd"])
    mll = float(rule["maximum_loss_limit_usd"])
    identity = str(profile["profile_id"]) == _identity_profile().profile_id
    charges = []
    for value in members:
        component = context.components[value]
        charge = require_causal_stop_risk_charge(
            component.declared_risk_charge_per_mini,
            governor_mode=component.source_governor_mode,
        )
        charges.append((value, charge))
    return ActiveRiskPoolPolicy(
        policy_id=str(spec["policy_id"]),
        component_priority=members,
        nominal_risk_charge_per_mini=tuple(charges),
        maximum_concurrent_sleeves=min(
            int(profile["maximum_concurrent_sleeves"]), len(members)
        ),
        aggregate_open_risk_ceiling=mll
        * float(profile["open_risk_ceiling_fraction"]),
        maximum_mll_buffer_fraction=float(
            profile["open_risk_ceiling_fraction"]
        ),
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=mll * float(profile["daily_loss_budget_fraction"]),
        daily_consistency_profit_guard=target
        * float(rule["consistency_target_fraction"])
        * float(profile["daily_profit_lock_fraction"]),
        target_protection_distance=(
            0.0
            if identity
            else target * (1.0 - float(profile["target_protection_fraction"]))
        ),
        target_protection_mode=(
            TargetProtectionMode.NONE if identity else TargetProtectionMode.SCALE_50
        ),
        # The candidate's exact whole-contract Tier-Q quantity has already been
        # materialised in its immutable trajectory.  A second multiplier would
        # manufacture a different policy.
        static_risk_tier=1.0,
    )


def _evaluate_policy_spec(
    spec: Mapping[str, Any], context: _ReplayContext
) -> dict[str, Any]:
    members = tuple(str(value) for value in spec["component_ids"])
    account_label = str(spec["account_label"])
    if any(context.components[value].account_label != account_label for value in members):
        raise AutonomousMarginalCombineBooksError("replay mixes account sizes")
    policy = _active_policy(spec, context)
    config = _account_config(context.rules[account_label])
    trajectories = {
        "NORMAL": {
            value: context.components[value].normal_trajectories for value in members
        },
        "STRESSED_1_5X": {
            value: context.components[value].stressed_trajectories for value in members
        },
    }
    unavailable = set()
    calendar_set = set(context.calendar)
    for value in members:
        component = context.components[value]
        unavailable.update(calendar_set.difference(component.eligible_session_days))
        unavailable.update(component.censored_session_days)

    summaries: dict[str, dict[str, Any]] = {
        scenario: {} for scenario in SCENARIOS
    }
    by_block_values: dict[
        str, dict[str, dict[str, list[tuple[Any, str]]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    by_block_censored: Counter[tuple[str, str, str]] = Counter()
    evidence_receipts: list[dict[str, Any]] = []
    index = {day: offset for offset, day in enumerate(context.calendar)}
    for scenario in SCENARIOS:
        for horizon in HORIZONS:
            values: list[tuple[Any, str]] = []
            censored = 0
            for start_day, block in context.starts[horizon]:
                if start_day not in index:
                    raise AutonomousMarginalCombineBooksError(
                        "frozen start is absent from account calendar"
                    )
                start_offset = index[start_day]
                window = context.calendar[start_offset : start_offset + horizon]
                if len(window) != horizon or any(day in unavailable for day in window):
                    censored += 1
                    by_block_censored[(block, scenario, str(horizon))] += 1
                    continue
                episode = run_causal_shared_account_episode(
                    trajectories[scenario],
                    context.calendar,
                    policy=policy,
                    start_day=start_day,
                    maximum_duration_days=horizon,
                    config=config,
                )
                values.append((episode, block))
                by_block_values[block][scenario][str(horizon)].append(
                    (episode, block)
                )
                full = episode.to_dict(include_paths=True)
                evidence_receipts.append(
                    {
                        "scenario": scenario,
                        "horizon_trading_days": horizon,
                        "start_day": start_day,
                        "temporal_block": block,
                        "terminal": episode.terminal.value,
                        "passed": bool(episode.passed),
                        "mll_breached": bool(episode.mll_breached),
                        "net_pnl": float(episode.net_pnl),
                        "target_progress": float(episode.target_progress),
                        "episode_path_hash": stable_hash(full),
                    }
                )
            summaries[scenario][str(horizon)] = _summarize_sprint_episodes(
                values,
                requested_start_count=len(context.starts[horizon]),
                data_censored_count=censored,
            )

    blocks = sorted(
        {
            block
            for values in context.starts.values()
            for _day, block in values
        }
    )
    if set(blocks) != set(DESIGN_BLOCKS) | set(HELD_OUT_DEVELOPMENT_BLOCKS):
        raise AutonomousMarginalCombineBooksError(
            "frozen block inventory differs from B1/B2/B3/B4"
        )
    summaries_by_role: dict[str, Any] = {}
    for role, role_blocks in (
        ("DESIGN", DESIGN_BLOCKS),
        ("HELD_OUT_DEVELOPMENT", HELD_OUT_DEVELOPMENT_BLOCKS),
    ):
        summaries_by_role[role] = {}
        for scenario in SCENARIOS:
            summaries_by_role[role][scenario] = {}
            for horizon in HORIZONS:
                values = [
                    value
                    for block in role_blocks
                    for value in by_block_values[block][scenario][str(horizon)]
                ]
                censored = sum(
                    by_block_censored[(block, scenario, str(horizon))]
                    for block in role_blocks
                )
                summaries_by_role[role][scenario][str(horizon)] = (
                    _summarize_sprint_episodes(
                        values,
                        requested_start_count=sum(
                            block in role_blocks
                            for _day, block in context.starts[horizon]
                        ),
                        data_censored_count=censored,
                    )
                )
    core: dict[str, Any] = {
        "schema": RESULT_SCHEMA,
        "policy_id": str(spec["policy_id"]),
        "policy_spec_hash": str(spec["policy_spec_hash"]),
        "policy_role": "EXACT_COMBINE_ACCOUNT_POLICY",
        "account_label": account_label,
        "component_ids": list(members),
        "component_quantity_tiers": dict(spec["component_quantity_tiers"]),
        "governor_profile_id": str(dict(spec["governor_profile"])["profile_id"]),
        "governor_policy": policy.to_dict(),
        "summaries": summaries,
        "summaries_by_role": summaries_by_role,
        "episode_evidence": {
            "record_count": len(evidence_receipts),
            "receipt_hash": stable_hash(evidence_receipts),
        },
        "completed_episode_count": len(evidence_receipts),
        "data_censored_episode_count": int(sum(by_block_censored.values())),
        "quantity_tiers_materialized_before_book_replay": True,
        "additional_quantity_scaling": False,
        "router_static_risk_tier": float(policy.static_risk_tier),
        "selection_role_contract": {
            "DESIGN": list(DESIGN_BLOCKS),
            "HELD_OUT_DEVELOPMENT": list(HELD_OUT_DEVELOPMENT_BLOCKS),
        },
        "signal_recomputation_performed": False,
        "registry_writes": 0,
        "database_writes": 0,
        "xfa_paths_started": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _g_ready_gates(
    row: Mapping[str, Any], *, singleton: bool = False
) -> dict[str, bool]:
    normal5 = _role_horizon_summary(row, "HELD_OUT_DEVELOPMENT", "NORMAL", 5)
    stressed5 = _role_horizon_summary(
        row, "HELD_OUT_DEVELOPMENT", "STRESSED_1_5X", 5
    )
    normal10 = _role_horizon_summary(row, "HELD_OUT_DEVELOPMENT", "NORMAL", 10)
    stressed10 = _role_horizon_summary(
        row, "HELD_OUT_DEVELOPMENT", "STRESSED_1_5X", 10
    )
    contribution = {
        str(key): max(float(value), 0.0)
        for key, value in dict(stressed5.get("component_contribution") or {}).items()
    }
    total = sum(contribution.values())
    maximum_sleeve_share = max(contribution.values(), default=0.0) / total if total else 0.0
    passed_blocks = set(normal5.get("blocks_with_passes") or ()) | set(
        stressed5.get("blocks_with_passes") or ()
    )
    gates = {
        "tier_q_components_only": all(
            str(value).strip() for value in row.get("component_ids", ())
        ),
        "normal_5d_pass_rate_at_least_5pct": float(
            normal5.get("pass_rate", 0.0)
        )
        >= 0.05,
        "stressed_5d_pass_rate_at_least_2pct": float(
            stressed5.get("pass_rate", 0.0)
        )
        >= 0.02,
        "normal_10d_pass_rate_at_least_10pct": float(
            normal10.get("pass_rate", 0.0)
        )
        >= 0.10,
        "stressed_10d_pass_rate_at_least_5pct": float(
            stressed10.get("pass_rate", 0.0)
        )
        >= 0.05,
        "positive_stressed_5d_economics": float(stressed5.get("net_total", 0.0))
        > 0.0,
        "mll_breach_at_most_10pct": max(
            float(normal5.get("mll_breach_rate", 1.0)),
            float(stressed5.get("mll_breach_rate", 1.0)),
        )
        <= 0.10,
        # Topstep expands the required profit target when an unfinished path's
        # best day exceeds 50%; that path is not a hard consistency failure.
        # Every actual pass must, however, satisfy the exact frozen rule.
        "all_passing_paths_consistency_compliant": bool(
            normal5.get("all_passing_paths_consistency_compliant", False)
            and stressed5.get("all_passing_paths_consistency_compliant", False)
        ),
        "passes_in_two_held_out_blocks": len(passed_blocks) >= 2,
        # Rolling starts reuse a given session day with different account
        # histories.  Summing those observations is a useful diagnostic but is
        # not an independent daily-concentration oracle.  The authoritative
        # gate must therefore run on the canonical unique trade/day ledger.
        "daily_concentration_deferred_to_authoritative_unique_ledger_control": True,
        # No trade-level claim is made from the account-summary daily rows.
        # Exact trade concentration remains mandatory before authoritative
        # promotion, while this read-only result remains a pre-control gate.
        "trade_concentration_deferred_to_authoritative_control": True,
        "no_single_sleeve_domination": len(row.get("component_ids", ())) <= 1
        or maximum_sleeve_share <= 0.65,
    }
    if singleton:
        gates["marginal_contribution_not_applicable_singleton"] = True
    else:
        gates["marginal_contribution_accepted"] = (
            row.get("marginally_accepted") is True
        )
    return gates


def _compact_policy_result(value: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "schema",
        "policy_id",
        "policy_spec_hash",
        "policy_role",
        "account_label",
        "component_ids",
        "component_quantity_tiers",
        "governor_profile_id",
        "governor_policy",
        "summaries",
        "summaries_by_role",
        "episode_evidence",
        "completed_episode_count",
        "data_censored_episode_count",
        "quantity_tiers_materialized_before_book_replay",
        "additional_quantity_scaling",
        "router_static_risk_tier",
        "selection_role_contract",
        "signal_recomputation_performed",
        "registry_writes",
        "database_writes",
        "xfa_paths_started",
        "result_hash",
    )
    return {key: value.get(key) for key in keys}


def _component_receipt(value: _PreparedComponent) -> dict[str, Any]:
    core = {
        "candidate_id": value.candidate_id,
        "candidate_fingerprint": value.candidate_fingerprint,
        "behavioral_fingerprint": value.behavioral_fingerprint,
        "qd_cell": value.qd_cell,
        "account_label": value.account_label,
        "account_size_usd": value.account_size_usd,
        "integer_quantity_tier": value.integer_quantity_tier,
        "source_governor_mode": value.source_governor_mode,
        "declared_risk_charge_per_mini": value.declared_risk_charge_per_mini,
        "source_receipt": dict(value.source_receipt),
        "quantity_scaling_applied_exactly_once": True,
    }
    return {**core, "component_freeze_hash": stable_hash(core)}


def _pass_counters(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, int] = {}
    for role in ("ALL", "HELD_OUT_DEVELOPMENT"):
        for scenario in SCENARIOS:
            for horizon in HORIZONS:
                if role == "ALL":
                    summaries = [
                        dict(dict(row["summaries"])[scenario][str(horizon)])
                        for row in rows
                    ]
                else:
                    summaries = [
                        _role_horizon_summary(row, role, scenario, horizon)
                        for row in rows
                    ]
                key = (
                    f"{role.lower()}_{scenario.lower()}_{horizon}d_pass_count"
                )
                output[key] = sum(int(value.get("pass_count", 0)) for value in summaries)
    return output


def _assert_unique_component_event_ids(
    components: Mapping[str, _PreparedComponent]
) -> None:
    for scenario in ("normal_trajectories", "stressed_trajectories"):
        owners: dict[str, str] = {}
        for candidate_id, component in sorted(components.items()):
            for trajectory in getattr(component, scenario):
                event_id = str(trajectory.event.event_id)
                owner = owners.get(event_id)
                if owner is not None and owner != candidate_id:
                    raise AutonomousMarginalCombineBooksError(
                        f"cross-component event ID collision: {event_id}"
                    )
                owners[event_id] = candidate_id


def _empty_result(
    *,
    bank: Mapping[str, Any],
    composite: Mapping[str, Any],
    tier_q_rows: Sequence[Mapping[str, Any]],
    requested_book_count: int,
    shard_index: int,
    shard_count: int,
    reason: str,
    design_cell_exclusions: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    core = {
        "schema": SCHEMA,
        "status": "NO_EXACT_MARGINAL_BOOK_BATCH",
        "reason": reason,
        "source_candidate_bank_hash": str(bank["result_hash"]),
        "source_composite_result_hash": str(composite["result_hash"]),
        "tier_q_component_ids": sorted(
            str(row["candidate_id"]) for row in tier_q_rows
        ),
        "requested_book_count": int(requested_book_count),
        "design_cell_exclusions": dict(sorted((design_cell_exclusions or {}).items())),
        "shard": {
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
            "proposal_inventory_count": 0,
            "proposal_inventory_hash": stable_hash([]),
            "proposal_inventory_policy_ids": [],
            "selected_primary_policy_ids": [],
            "ranking_recomputed_after_sharding": False,
        },
        "proposals": [],
        "standalone_results": [],
        "book_results": [],
        "supporting_policy_results": [],
        "counts": {
            "tier_q_component_count": len(tier_q_rows),
            "book_proposal_count": 0,
            "primary_book_exact_replay_count": 0,
            "supporting_policy_exact_replay_count": 0,
            "marginally_accepted_count": 0,
            "standalone_g_ready_count": 0,
            "g_ready_count": 0,
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "promotion_status": None,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
    }
    return {**core, "result_hash": stable_hash(core)}


__all__ = [
    "AutonomousMarginalCombineBooksError",
    "COMPOSITE_SCHEMA",
    "MAXIMUM_BOOK_BATCH",
    "SCHEMA",
    "build_autonomous_marginal_combine_books",
    "compose_autonomous_marginal_combine_book_shards",
]
