"""Read-only Tier-G controls over the five exact multi-pass 0029 candidates.

This module closes the concentration-evidence gap without counting the same
trade repeatedly through overlapping rolling starts.  It reconstructs each
candidate's immutable causal trajectory once, reproduces the exact selected
account cell, measures day/trade/event concentration on that unique ledger and
runs a small deterministic circular-label-shift control suite.

Circular shifts are explicitly synthetic, non-deployable temporal-alignment
nulls.  They preserve event count, direction, quantity and exposure timestamps
but permute already-observed outcome paths.  They cannot promote a candidate by
themselves.  Every returned status remains viewed final-development evidence;
the module writes no registry/database, starts no XFA path and exposes no order
route.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    reconstruct_exact_hazard_replay,
)
from hydra.production import autonomous_marginal_combine_books as books
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _apply_session_contract,
    _candidate_coverage,
    _declared_stop_risk_charge_per_mini,
    _inside,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _read_verified_event_evidence,
    _require_scenario_identity,
    _standalone_policy,
    _summarize_exact_episodes,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.causal_risk_charge import require_causal_stop_risk_charge
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)
from hydra.research.causal_target_velocity import HazardOutcome


SCHEMA = "hydra_autonomous_tier_g_controls_v1"
COMPOSITE_SCHEMA = "hydra_autonomous_tier_g_control_shards_v1"
CONCENTRATION_RECEIPT_SCHEMA = "hydra_candidate_concentration_receipt_v1"
MAXIMUM_CANDIDATES = 5
MAXIMUM_CONCENTRATION_SHARE = 0.50
MAXIMUM_BLOCK_PASS_SHARE = 0.75
CONTROL_SHIFT_COUNT = 3


class AutonomousTierGControlsError(RuntimeError):
    """Tier-G control evidence cannot be established safely."""


def build_autonomous_tier_g_controls(
    root: str | Path,
    candidate_bank: Mapping[str, Any],
    initial_exact_result: Mapping[str, Any],
    continuation_results: Sequence[Mapping[str, Any]] = (),
    *,
    shard_index: int = 0,
    shard_count: int = 1,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Build one deterministic candidate shard without authoritative writes."""

    if int(shard_count) not in {1, 2} or not 0 <= int(shard_index) < int(
        shard_count
    ):
        raise AutonomousTierGControlsError(
            "Tier-G controls require shard_count 1/2 and valid index"
        )
    bank = books._verify_candidate_bank(candidate_bank)
    composite, exact_results = books._verified_exact_results(
        initial_exact_result, continuation_results
    )
    if str(bank["source_composite_result_hash"]) != str(composite["result_hash"]):
        raise AutonomousTierGControlsError(
            "candidate bank and exact composite provenance differ"
        )
    candidates = _select_control_candidates(bank)
    if len(candidates) != MAXIMUM_CANDIDATES:
        raise AutonomousTierGControlsError(
            "authoritative multi-pass Tier-Q control inventory is not exactly five"
        )

    project = Path(root).resolve()
    manifest = _load_self_hashed_manifest(
        _inside(project, fast_pass_manifest_path)
    )
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(
        _inside(project, rule_snapshot_path)
    )
    bank_entries, bank_receipt = _load_banks(project)
    exact_by_hash = {str(row["result_hash"]): dict(row) for row in exact_results}
    if len(exact_by_hash) != len(exact_results):
        raise AutonomousTierGControlsError("duplicate exact-result source hash")

    inventory_ids = tuple(str(row["candidate_id"]) for row in candidates)
    inventory_hash = stable_hash(list(inventory_ids))
    selected = tuple(
        row
        for position, row in enumerate(candidates)
        if position % int(shard_count) == int(shard_index)
    )
    results: list[dict[str, Any]] = []
    exact_replay_count = 0
    for classified in selected:
        prepared = _prepare_candidate(
            project=project,
            classified=classified,
            exact_by_hash=exact_by_hash,
            bank_entries=bank_entries,
            calendar=calendar,
            starts=starts,
            rules=rules,
        )
        evaluated = _evaluate_prepared_candidate(prepared)
        exact_replay_count += int(evaluated["exact_account_replay_count"])
        results.append(evaluated)

    ready = sorted(
        str(row["candidate_id"])
        for row in results
        if row["g_control_ready"] is True
    )
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "COMPLETE_READ_ONLY_TIER_G_CONTROL_SHARD",
        "branch_id": "EXACT_TIER_Q_UNIQUE_LEDGER_G_CONTROLS",
        "source_candidate_bank_hash": str(bank["result_hash"]),
        "source_exact_composite_hash": str(composite["result_hash"]),
        "source_manifest_hash": str(manifest["manifest_hash"]),
        "frozen_grid": grid_receipt,
        "official_rule_snapshot": rule_receipt,
        "source_bank_receipt": bank_receipt,
        "control_contract": {
            "candidate_maximum": MAXIMUM_CANDIDATES,
            "candidate_selection": (
                "TIER_Q_AND_PAIRED_PASSES_GE_2_AND_BLOCK_CONTEXTS_GE_2_"
                "AND_BLOCK_PASS_SHARE_LE_075"
            ),
            "unique_ledger_denominator": (
                "ONE_RECONSTRUCTED_COMPLETED_CAUSAL_TRAJECTORY_PER_EVENT"
            ),
            "maximum_day_trade_event_profit_share": (
                MAXIMUM_CONCENTRATION_SHARE
            ),
            "control_type": "CIRCULAR_OUTCOME_PATH_SHIFT_SYNTHETIC_NULL",
            "control_shift_count": CONTROL_SHIFT_COUNT,
            "control_is_deployable": False,
            "control_can_promote": False,
            "identity_best_parent_required": True,
        },
        "shard": {
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
            "partition_rule": "SORTED_CANDIDATE_POSITION_MODULO_SHARD_COUNT_V1",
            "candidate_inventory_ids": list(inventory_ids),
            "candidate_inventory_hash": inventory_hash,
            "selected_candidate_ids": [str(row["candidate_id"]) for row in selected],
        },
        "candidate_results": results,
        "concentration_receipts": {
            str(row["candidate_id"]): dict(row["concentration_receipt"])
            for row in results
        },
        "counts": {
            "source_candidate_count": len(inventory_ids),
            "selected_candidate_count": len(selected),
            "exact_account_replay_count": exact_replay_count,
            "synthetic_control_count": len(selected) * CONTROL_SHIFT_COUNT,
            "g_control_ready_count": len(ready),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "candidate_ids": {"g_control_ready": ready},
        "evidence_role": "VIEWED_FINAL_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "next_action": (
            "AUTHORITATIVE_WRITER_MAY_RECLASSIFY_G_READY_WITHOUT_CONFIRMATION_CLAIM"
            if ready
            else "PRESERVE_TIER_Q_AND_CONTINUE_DISTINCT_ECONOMIC_BRANCH"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def compose_autonomous_tier_g_control_shards(
    shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reconcile deterministic read-only shards without replaying economics."""

    values = [_verify_shard(row) for row in shards]
    if not values:
        raise AutonomousTierGControlsError(
            "at least one Tier-G control shard is required"
        )
    declared = int(dict(values[0]["shard"])["shard_count"])
    if declared not in {1, 2} or len(values) != declared:
        raise AutonomousTierGControlsError("Tier-G control shard set is incomplete")
    shared = (
        "source_candidate_bank_hash",
        "source_exact_composite_hash",
        "source_manifest_hash",
        "frozen_grid",
        "official_rule_snapshot",
        "source_bank_receipt",
        "control_contract",
    )
    for field in shared:
        expected = stable_hash(values[0][field])
        if any(stable_hash(row[field]) != expected for row in values[1:]):
            raise AutonomousTierGControlsError(
                f"Tier-G control shard shared field differs: {field}"
            )
    inventory_ids = tuple(
        str(row) for row in dict(values[0]["shard"])["candidate_inventory_ids"]
    )
    inventory_hash = str(dict(values[0]["shard"])["candidate_inventory_hash"])
    indexes: set[int] = set()
    selected_sets: list[set[str]] = []
    by_candidate: dict[str, dict[str, Any]] = {}
    for value in values:
        shard = dict(value["shard"])
        index = int(shard["shard_index"])
        selected = {str(row) for row in shard["selected_candidate_ids"]}
        if (
            index in indexes
            or int(shard["shard_count"]) != declared
            or str(shard["candidate_inventory_hash"]) != inventory_hash
            or tuple(str(row) for row in shard["candidate_inventory_ids"])
            != inventory_ids
        ):
            raise AutonomousTierGControlsError("Tier-G shard identity drift")
        indexes.add(index)
        selected_sets.append(selected)
        observed = {str(row["candidate_id"]) for row in value["candidate_results"]}
        if observed != selected:
            raise AutonomousTierGControlsError(
                "Tier-G shard result inventory differs from selection"
            )
        for row in value["candidate_results"]:
            candidate_id = str(row["candidate_id"])
            if candidate_id in by_candidate:
                raise AutonomousTierGControlsError(
                    "candidate appears in multiple Tier-G control shards"
                )
            by_candidate[candidate_id] = dict(row)
    if indexes != set(range(declared)):
        raise AutonomousTierGControlsError("Tier-G shard indexes are incomplete")
    for position, left in enumerate(selected_sets):
        if any(left & right for right in selected_sets[position + 1 :]):
            raise AutonomousTierGControlsError("Tier-G control shards overlap")
    if set().union(*selected_sets) != set(inventory_ids):
        raise AutonomousTierGControlsError("Tier-G shard union is incomplete")
    ordered = [by_candidate[candidate_id] for candidate_id in inventory_ids]
    ready = sorted(
        str(row["candidate_id"]) for row in ordered if row["g_control_ready"] is True
    )
    core: dict[str, Any] = {
        "schema": COMPOSITE_SCHEMA,
        "status": "COMPLETE_RECONCILED_TIER_G_CONTROL_SHARDS",
        **{field: values[0][field] for field in shared},
        "shard_receipts": [
            {
                "shard_index": int(dict(row["shard"])["shard_index"]),
                "result_hash": str(row["result_hash"]),
                "selected_candidate_ids": list(
                    dict(row["shard"])["selected_candidate_ids"]
                ),
            }
            for row in sorted(
                values, key=lambda item: int(dict(item["shard"])["shard_index"])
            )
        ],
        "candidate_results": ordered,
        "concentration_receipts": {
            str(row["candidate_id"]): dict(row["concentration_receipt"])
            for row in ordered
        },
        "counts": {
            "source_candidate_count": len(inventory_ids),
            "selected_candidate_count": len(ordered),
            "exact_account_replay_count": sum(
                int(dict(row["counts"])["exact_account_replay_count"])
                for row in values
            ),
            "synthetic_control_count": len(ordered) * CONTROL_SHIFT_COUNT,
            "g_control_ready_count": len(ready),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "candidate_ids": {"g_control_ready": ready},
        "evidence_role": "VIEWED_FINAL_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "next_action": (
            "AUTHORITATIVE_WRITER_MAY_RECLASSIFY_G_READY_WITHOUT_CONFIRMATION_CLAIM"
            if ready
            else "PRESERVE_TIER_Q_AND_CONTINUE_DISTINCT_ECONOMIC_BRANCH"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def tier_g_controls_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Pickle-safe adapter for an economic worker."""

    return build_autonomous_tier_g_controls(
        str(payload["root"]),
        dict(payload["candidate_bank"]),
        dict(payload["initial_exact_result"]),
        tuple(dict(row) for row in payload.get("continuation_results", ())),
        shard_index=int(payload.get("shard_index", 0)),
        shard_count=int(payload.get("shard_count", 1)),
        fast_pass_manifest_path=str(
            payload.get("fast_pass_manifest_path", DEFAULT_FAST_PASS_MANIFEST)
        ),
        rule_snapshot_path=str(
            payload.get("rule_snapshot_path", DEFAULT_RULE_SNAPSHOT)
        ),
    )


def _select_control_candidates(bank: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    selected: list[dict[str, Any]] = []
    for raw in bank.get("candidates", ()):
        row = dict(raw)
        cell = dict(row.get("best_safe_cell") or {})
        normal = dict(cell.get("normal") or {})
        stressed = dict(cell.get("stressed") or {})
        concentration = dict(row.get("concentration_diagnostic") or {})
        maximum_block_share = concentration.get("maximum_block_pass_share")
        if (
            row.get("tier_q_contract_cleared") is True
            and row.get("computed_development_tier") == "Q"
            and int(normal.get("pass_count", 0)) >= 2
            and int(stressed.get("pass_count", 0)) >= 2
            and int(concentration.get("positive_pass_context_count", 0)) >= 2
            and maximum_block_share is not None
            and float(maximum_block_share) <= MAXIMUM_BLOCK_PASS_SHARE
            and row.get("authoritative_promotion_status") is None
        ):
            selected.append(row)
    return tuple(sorted(selected, key=lambda row: str(row["candidate_id"])))


def _prepare_candidate(
    *,
    project: Path,
    classified: Mapping[str, Any],
    exact_by_hash: Mapping[str, Mapping[str, Any]],
    bank_entries: Sequence[Mapping[str, Any]],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    rules: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_id = str(classified["candidate_id"])
    source_hash = str(classified["source_exact_result_hash"])
    source = exact_by_hash.get(source_hash)
    if source is None:
        raise AutonomousTierGControlsError(
            f"exact source is absent for {candidate_id}"
        )
    candidates = [
        dict(row)
        for row in source.get("results", ())
        if str(row.get("candidate_id")) == candidate_id
    ]
    if len(candidates) != 1:
        raise AutonomousTierGControlsError(
            f"exact candidate is absent or duplicated: {candidate_id}"
        )
    exact_candidate = candidates[0]
    compact_cell = dict(classified["best_safe_cell"])
    cell_hash = str(compact_cell["cell_hash"])
    cells = [
        dict(row)
        for row in exact_candidate.get("frontier", ())
        if stable_hash(row) == cell_hash
    ]
    if len(cells) != 1:
        raise AutonomousTierGControlsError(
            f"selected exact cell hash is absent or duplicated: {candidate_id}"
        )
    cell = cells[0]
    account_label = str(cell["account_label"])
    if account_label not in rules:
        raise AutonomousTierGControlsError(
            f"unsupported account label for {candidate_id}"
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
        raise AutonomousTierGControlsError(
            f"immutable bank evidence differs for {candidate_id}"
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
    normal, normal_violations = _apply_session_contract(replay.normal_trajectories)
    stressed, stressed_violations = _apply_session_contract(
        replay.stressed_trajectories
    )
    if normal_violations or stressed_violations:
        raise AutonomousTierGControlsError(
            f"session contract violation in {candidate_id}"
        )
    tier = int(cell["integer_quantity_tier"])
    scaled_normal = tuple(
        scale_causal_trajectory(row, executable_quantity_multiplier=tier)
        for row in normal
    )
    scaled_stressed = tuple(
        scale_causal_trajectory(row, executable_quantity_multiplier=tier)
        for row in stressed
    )
    _require_scenario_identity(scaled_normal, scaled_stressed)
    declared_risk = _declared_stop_risk_charge_per_mini(events, entry["candidate"])
    risk_charge = require_causal_stop_risk_charge(
        declared_risk,
        governor_mode=str(cell["risk_governor_mode"]),
    )
    rule = dict(rules[account_label])
    policy = _standalone_policy(
        candidate_id,
        rule,
        tier=tier,
        declared_risk_charge_per_mini=float(risk_charge),
        account_contract_limit=float(cell["maximum_mini_contracts"]),
        governor_mode=str(cell["risk_governor_mode"]),
    )
    if stable_hash(policy.to_dict()) != stable_hash(cell["account_policy"]):
        raise AutonomousTierGControlsError(
            f"selected account policy drift: {candidate_id}"
        )
    frozen_account_policy = policy.to_dict()
    markets = sorted(
        {
            str(row.market)
            for row in (*scaled_normal, *scaled_stressed)
            if str(row.market).strip()
        }
    )
    if not markets:
        raise AutonomousTierGControlsError(
            f"selected account policy market inventory is absent: {candidate_id}"
        )
    coverage = _candidate_coverage(replay, calendar, starts)
    horizon = int(cell["horizon_trading_days"])
    selected_starts = tuple(coverage["starts"][horizon])
    if (
        len(selected_starts) != int(cell["full_coverage_start_count"])
        or int(coverage["censored"][horizon])
        != int(cell["data_censored_start_count"])
    ):
        raise AutonomousTierGControlsError(
            f"selected-cell coverage drift: {candidate_id}"
        )
    censored_days = frozenset(
        int(row.session_day)
        for row in replay.events
        if str(getattr(row.outcome, "value", row.outcome))
        == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
    )
    return {
        "candidate_id": candidate_id,
        "candidate_fingerprint": str(classified["candidate_fingerprint"]),
        "behavioral_fingerprint": str(
            classified["realized_behavioral_fingerprint"]
        ),
        "qd_cell": str(classified["qd_cell"]),
        "source_exact_result_hash": source_hash,
        "source_manifest_hash": str(
            dict(source["source_manifest"])["manifest_hash"]
        ),
        "frozen_grid_hash": str(dict(source["frozen_grid"])["grid_hash"]),
        "official_rule_snapshot_hash": str(
            dict(source["official_rule_snapshot"])["parsed_rule_hash"]
        ),
        "source_candidate_result_hash": str(
            exact_candidate["candidate_result_hash"]
        ),
        "selected_cell": cell,
        "selected_cell_hash": cell_hash,
        "account_label": account_label,
        "account_size_usd": int(cell["account_size_usd"]),
        "markets": markets,
        "market_inventory_hash": stable_hash(markets),
        "frozen_account_policy": frozen_account_policy,
        "frozen_account_policy_hash": stable_hash(frozen_account_policy),
        "complete_account_policy": True,
        "policy_sleeve_ids": [candidate_id],
        "horizon": horizon,
        "calendar": tuple(int(day) for day in calendar),
        "starts": selected_starts,
        "normal": scaled_normal,
        "stressed": scaled_stressed,
        "eligible_session_days": frozenset(
            int(day) for day in replay.eligible_session_days
        ),
        "censored_session_days": censored_days,
        "policy": policy,
        "config": _account_config(rule),
        "source_event_receipt": event_receipt,
        "source_event_record_count": len(events),
        "source_block_concentration": dict(
            classified["concentration_diagnostic"]
        ),
    }


def _evaluate_prepared_candidate(prepared: Mapping[str, Any]) -> dict[str, Any]:
    candidate_id = str(prepared["candidate_id"])
    identity = _replay_pair(
        candidate_id=candidate_id,
        normal=prepared["normal"],
        stressed=prepared["stressed"],
        calendar=prepared["calendar"],
        starts=prepared["starts"],
        horizon=int(prepared["horizon"]),
        policy=prepared["policy"],
        config=prepared["config"],
    )
    source_cell = dict(prepared["selected_cell"])
    identity_reconciled = all(
        stable_hash(identity[scenario.lower()])
        == stable_hash(source_cell[scenario.lower()])
        for scenario in ("NORMAL", "STRESSED")
    )
    if not identity_reconciled:
        raise AutonomousTierGControlsError(
            f"identity/best-parent replay does not reconcile: {candidate_id}"
        )

    concentration = _unique_ledger_concentration(
        prepared["normal"], prepared["stressed"]
    )
    shifts = _control_offsets(
        len(prepared["normal"]), str(prepared["candidate_fingerprint"])
    )
    controls: list[dict[str, Any]] = []
    for position, offset in enumerate(shifts, start=1):
        control_id = f"CIRCULAR_SHIFT_{position:02d}_OFFSET_{offset}"
        normal_control = _circular_shift_trajectories(
            prepared["normal"], offset=offset, control_id=control_id,
            scenario="NORMAL",
        )
        stressed_control = _circular_shift_trajectories(
            prepared["stressed"], offset=offset, control_id=control_id,
            scenario="STRESSED_1_5X",
        )
        _require_scenario_identity(normal_control, stressed_control)
        normal_match = _exposure_count_match(prepared["normal"], normal_control)
        stressed_match = _exposure_count_match(
            prepared["stressed"], stressed_control
        )
        if not normal_match or not stressed_match:
            raise AutonomousTierGControlsError(
                f"synthetic control exposure mismatch: {candidate_id}/{control_id}"
            )
        replay = _replay_pair(
            candidate_id=candidate_id,
            normal=normal_control,
            stressed=stressed_control,
            calendar=prepared["calendar"],
            starts=prepared["starts"],
            horizon=int(prepared["horizon"]),
            policy=prepared["policy"],
            config=prepared["config"],
        )
        control_core = {
            "control_id": control_id,
            "control_type": "CIRCULAR_OUTCOME_PATH_SHIFT_SYNTHETIC_NULL",
            "synthetic_non_deployable": True,
            "may_promote_candidate": False,
            "offset": offset,
            "event_count": len(normal_control),
            "exposure_count_matched": True,
            "normal": replay["normal"],
            "stressed": replay["stressed"],
            "normal_control_ledger_hash": stable_hash(
                [row.to_dict() for row in normal_control]
            ),
            "stressed_control_ledger_hash": stable_hash(
                [row.to_dict() for row in stressed_control]
            ),
        }
        controls.append(
            {**control_core, "control_hash": stable_hash(control_core)}
        )

    stressed_passes = [int(row["stressed"]["pass_count"]) for row in controls]
    stressed_nets = [float(row["stressed"]["net_total_usd"]) for row in controls]
    observed_stressed = dict(identity["stressed"])
    control_comparison = {
        "observed_stressed_pass_count": int(observed_stressed["pass_count"]),
        "median_synthetic_stressed_pass_count": float(
            statistics.median(stressed_passes)
        ),
        "observed_stressed_net_usd": float(
            observed_stressed["net_total_usd"]
        ),
        "median_synthetic_stressed_net_usd": float(
            statistics.median(stressed_nets)
        ),
        "pass_count_not_worse_than_median_synthetic": int(
            observed_stressed["pass_count"]
        )
        >= statistics.median(stressed_passes),
        "stressed_net_not_worse_than_median_synthetic": float(
            observed_stressed["net_total_usd"]
        )
        >= statistics.median(stressed_nets),
        "interpretation": (
            "SYNTHETIC_TEMPORAL_ALIGNMENT_CONTROL_ONLY_NOT_ALPHA_CONFIRMATION"
        ),
    }
    evidence_core = {
        "schema": "hydra_tier_g_final_development_evidence_v1",
        "candidate_id": candidate_id,
        "candidate_fingerprint": str(prepared["candidate_fingerprint"]),
        "behavioral_fingerprint": str(prepared["behavioral_fingerprint"]),
        "qd_cell": str(prepared["qd_cell"]),
        "source_exact_result_hash": str(prepared["source_exact_result_hash"]),
        "source_manifest_hash": str(prepared["source_manifest_hash"]),
        "frozen_grid_hash": str(prepared["frozen_grid_hash"]),
        "official_rule_snapshot_hash": str(
            prepared["official_rule_snapshot_hash"]
        ),
        "source_candidate_result_hash": str(
            prepared["source_candidate_result_hash"]
        ),
        "selected_cell_hash": str(prepared["selected_cell_hash"]),
        "account_label": str(prepared["account_label"]),
        "account_size_usd": int(prepared["account_size_usd"]),
        "market_inventory_hash": str(prepared["market_inventory_hash"]),
        "frozen_account_policy_hash": str(
            prepared["frozen_account_policy_hash"]
        ),
        "policy_sleeve_inventory_hash": stable_hash(
            prepared["policy_sleeve_ids"]
        ),
        "source_event_receipt": dict(prepared["source_event_receipt"]),
        "normal_unique_ledger_hash": concentration["normal"]["ledger_hash"],
        "stressed_unique_ledger_hash": concentration["stressed"]["ledger_hash"],
        "identity_normal_episode_path_hash": identity["normal"][
            "episode_path_hash"
        ],
        "identity_stressed_episode_path_hash": identity["stressed"][
            "episode_path_hash"
        ],
        "concentration_hash": concentration["concentration_hash"],
        "control_hashes": [row["control_hash"] for row in controls],
        "control_comparison_hash": stable_hash(control_comparison),
        "source_block_concentration_hash": stable_hash(
            prepared["source_block_concentration"]
        ),
        "frozen_gate_thresholds": {
            "maximum_day_trade_event_profit_share": (
                MAXIMUM_CONCENTRATION_SHARE
            ),
            "maximum_block_pass_share": MAXIMUM_BLOCK_PASS_SHARE,
            "minimum_normal_passes": 2,
            "minimum_stressed_passes": 2,
            "minimum_temporal_contexts": 2,
            "synthetic_control_comparison": "NOT_WORSE_THAN_MEDIAN",
        },
        "evidence_role": "VIEWED_FINAL_DEVELOPMENT_ONLY",
        "independent_confirmation_claimed": False,
    }
    final_hash = stable_hash(evidence_core)
    receipt = _concentration_receipt(
        candidate_id=candidate_id,
        source_exact_result_hash=str(prepared["source_exact_result_hash"]),
        concentration=concentration,
        final_development_evidence_hash=final_hash,
    )
    source_concentration = dict(prepared["source_block_concentration"])
    gates = {
        "tier_q_source": True,
        "identity_best_parent_reconciled": identity_reconciled,
        "multiple_normal_and_stressed_passes": int(identity["normal"]["pass_count"])
        >= 2
        and int(identity["stressed"]["pass_count"]) >= 2,
        "multiple_temporal_contexts": int(
            source_concentration["positive_pass_context_count"]
        )
        >= 2,
        "block_concentration_le_75pct": float(
            source_concentration["maximum_block_pass_share"]
        )
        <= MAXIMUM_BLOCK_PASS_SHARE,
        "unique_ledger_day_trade_event_concentration_le_50pct": bool(
            concentration["cleared"]
        ),
        "synthetic_controls_complete_and_exposure_matched": len(controls)
        == CONTROL_SHIFT_COUNT
        and all(row["exposure_count_matched"] for row in controls),
        "not_worse_than_median_synthetic_pass_count": bool(
            control_comparison["pass_count_not_worse_than_median_synthetic"]
        ),
        "not_worse_than_median_synthetic_stressed_net": bool(
            control_comparison["stressed_net_not_worse_than_median_synthetic"]
        ),
        "final_development_evidence_hash_complete": len(final_hash) == 64,
    }
    ready = all(gates.values())
    core: dict[str, Any] = {
        "candidate_id": candidate_id,
        "candidate_fingerprint": str(prepared["candidate_fingerprint"]),
        "behavioral_fingerprint": str(prepared["behavioral_fingerprint"]),
        "qd_cell": str(prepared["qd_cell"]),
        "account_label": str(prepared["account_label"]),
        "account_size_usd": int(prepared["account_size_usd"]),
        "markets": list(prepared["markets"]),
        "market_inventory_hash": str(prepared["market_inventory_hash"]),
        "frozen_account_policy": dict(prepared["frozen_account_policy"]),
        "frozen_account_policy_hash": str(
            prepared["frozen_account_policy_hash"]
        ),
        "complete_account_policy": True,
        "policy_sleeve_ids": list(prepared["policy_sleeve_ids"]),
        "selected_horizon_trading_days": int(prepared["horizon"]),
        "selected_cell_hash": str(prepared["selected_cell_hash"]),
        "source_exact_result_hash": str(prepared["source_exact_result_hash"]),
        "identity_best_parent": {
            "control_role": "IDENTITY_AND_STANDALONE_BEST_PARENT",
            "reconciled": identity_reconciled,
            "normal": identity["normal"],
            "stressed": identity["stressed"],
        },
        "unique_ledger_concentration": concentration,
        "synthetic_controls": controls,
        "control_comparison": control_comparison,
        "final_development_evidence": {
            **evidence_core,
            "final_development_evidence_hash": final_hash,
        },
        "concentration_receipt": receipt,
        "g_control_gate_results": gates,
        "g_control_ready": ready,
        "computed_development_tier": (
            "G_CONTROL_READY" if ready else "Q_CONTROL_EVALUATED"
        ),
        "authoritative_promotion_status": None,
        "independent_confirmation_claimed": False,
        "exact_account_replay_count": (
            len(prepared["starts"]) * 2 * (1 + CONTROL_SHIFT_COUNT)
        ),
        "xfa_paths_started": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _replay_pair(
    *,
    candidate_id: str,
    normal: Sequence[CausalTradeTrajectory],
    stressed: Sequence[CausalTradeTrajectory],
    calendar: Sequence[int],
    starts: Sequence[tuple[int, str]],
    horizon: int,
    policy: Any,
    config: Any,
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for scenario, trajectories in (("normal", normal), ("stressed", stressed)):
        episodes = [
            (
                run_causal_shared_account_episode(
                    {candidate_id: trajectories},
                    calendar,
                    policy=policy,
                    start_day=int(start_day),
                    maximum_duration_days=int(horizon),
                    config=config,
                ),
                str(block),
            )
            for start_day, block in starts
        ]
        output[scenario] = _summarize_exact_episodes(episodes)
    return output


def _unique_ledger_concentration(
    normal: Sequence[CausalTradeTrajectory],
    stressed: Sequence[CausalTradeTrajectory],
) -> dict[str, Any]:
    if not normal or len(normal) != len(stressed):
        raise AutonomousTierGControlsError(
            "unique normal/stressed trajectory ledger is empty or unmatched"
        )
    result = {
        "normal": _scenario_ledger_concentration(normal),
        "stressed": _scenario_ledger_concentration(stressed),
    }
    maximums = {
        metric: max(float(result[scenario][metric]) for scenario in result)
        for metric in (
            "maximum_single_day_profit_share",
            "maximum_single_trade_profit_share",
            "maximum_single_event_profit_share",
        )
    }
    cleared = all(value <= MAXIMUM_CONCENTRATION_SHARE for value in maximums.values())
    core = {
        "denominator": "UNIQUE_COMPLETED_CAUSAL_TRAJECTORY_LEDGER",
        "event_trade_mapping": "ONE_COMPLETED_TRAJECTORY_PER_EVENT",
        "rolling_start_multiplicity": 0,
        "maximum_allowed_profit_share": MAXIMUM_CONCENTRATION_SHARE,
        "normal": result["normal"],
        "stressed": result["stressed"],
        "worst_case_maximums": maximums,
        "cleared": cleared,
    }
    return {**core, "concentration_hash": stable_hash(core)}


def _scenario_ledger_concentration(
    trajectories: Sequence[CausalTradeTrajectory],
) -> dict[str, Any]:
    ordered = tuple(
        sorted(
            trajectories,
            key=lambda row: (row.event.decision_ns, row.event.event_id),
        )
    )
    event_ids = [_base_event_id(row.event.event_id) for row in ordered]
    if len(event_ids) != len(set(event_ids)):
        raise AutonomousTierGControlsError(
            "unique trajectory ledger contains a duplicate event"
        )
    positive = [max(float(row.event.net_pnl), 0.0) for row in ordered]
    total_positive = sum(positive)
    day_profit: dict[int, float] = defaultdict(float)
    day_loss: dict[int, float] = defaultdict(float)
    for row in ordered:
        day_profit[int(row.event.session_day)] += max(float(row.event.net_pnl), 0.0)
        day_loss[int(row.event.session_day)] += min(float(row.event.net_pnl), 0.0)
    trade_share = (
        max(positive, default=0.0) / total_positive if total_positive > 0.0 else 1.0
    )
    day_share = (
        max(day_profit.values(), default=0.0) / total_positive
        if total_positive > 0.0
        else 1.0
    )
    losses = [abs(min(float(row.event.net_pnl), 0.0)) for row in ordered]
    total_loss = sum(losses)
    result = {
        "record_count": len(ordered),
        "unique_event_count": len(event_ids),
        "unique_trade_count": len(event_ids),
        "unique_session_day_count": len({row.event.session_day for row in ordered}),
        "positive_profit_total_usd": float(total_positive),
        "net_total_usd": float(sum(row.event.net_pnl for row in ordered)),
        "maximum_single_day_profit_share": float(day_share),
        "maximum_single_trade_profit_share": float(trade_share),
        "maximum_single_event_profit_share": float(trade_share),
        "maximum_single_trade_loss_share": (
            max(losses, default=0.0) / total_loss if total_loss > 0.0 else 0.0
        ),
        "maximum_single_day_loss_share": (
            max((abs(value) for value in day_loss.values()), default=0.0)
            / total_loss
            if total_loss > 0.0
            else 0.0
        ),
        "ledger_hash": stable_hash([row.to_dict() for row in ordered]),
        "event_inventory_hash": stable_hash(event_ids),
    }
    if not all(
        math.isfinite(float(value)) and 0.0 <= float(value) <= 1.0
        for key, value in result.items()
        if key.endswith("_share")
    ):
        raise AutonomousTierGControlsError("invalid unique-ledger concentration")
    return result


def _control_offsets(event_count: int, fingerprint: str) -> tuple[int, ...]:
    if event_count < 4:
        raise AutonomousTierGControlsError(
            "at least four events are required for circular controls"
        )
    base = (
        event_count // 4,
        event_count // 2,
        (3 * event_count) // 4,
    )
    jitter = int(fingerprint[:8], 16) % max(1, event_count // 20)
    offsets: list[int] = []
    for value in base:
        candidate = (int(value) + jitter) % event_count
        if candidate == 0:
            candidate = 1
        while candidate in offsets:
            candidate = (candidate + 1) % event_count or 1
        offsets.append(candidate)
    return tuple(offsets)


def _circular_shift_trajectories(
    trajectories: Sequence[CausalTradeTrajectory],
    *,
    offset: int,
    control_id: str,
    scenario: str,
) -> tuple[CausalTradeTrajectory, ...]:
    ordered = tuple(
        sorted(
            trajectories,
            key=lambda row: (row.event.decision_ns, row.event.event_id),
        )
    )
    if not 0 < int(offset) < len(ordered):
        raise AutonomousTierGControlsError("circular control offset is invalid")
    output: list[CausalTradeTrajectory] = []
    for index, recipient in enumerate(ordered):
        donor = ordered[(index + int(offset)) % len(ordered)]
        recipient_marks = recipient.marks
        donor_marks = donor.marks
        marks: list[CausalTradeMark] = []
        for mark_index, recipient_mark in enumerate(recipient_marks):
            donor_index = round(
                mark_index * (len(donor_marks) - 1) / max(len(recipient_marks) - 1, 1)
            )
            donor_mark = donor_marks[donor_index]
            marks.append(
                replace(
                    donor_mark,
                    availability_time_ns=recipient_mark.availability_time_ns,
                )
            )
        base_id = _base_event_id(recipient.event.event_id)
        event = replace(
            recipient.event,
            event_id=f"{base_id}:{control_id}:{scenario}",
            net_pnl=float(donor.event.net_pnl),
            gross_pnl=float(donor.event.gross_pnl),
            worst_unrealized_pnl=float(donor.event.worst_unrealized_pnl),
            best_unrealized_pnl=float(donor.event.best_unrealized_pnl),
        )
        output.append(
            replace(
                recipient,
                event=event,
                marks=tuple(marks),
                initial_unrealized_pnl=float(donor.initial_unrealized_pnl),
            )
        )
    return tuple(output)


def _exposure_count_match(
    source: Sequence[CausalTradeTrajectory],
    control: Sequence[CausalTradeTrajectory],
) -> bool:
    def exposure(row: CausalTradeTrajectory) -> tuple[Any, ...]:
        return (
            row.component_id,
            row.market,
            row.side,
            row.event.decision_ns,
            row.event.exit_ns,
            row.event.session_day,
            row.event.quantity,
            row.event.mini_equivalent,
            len(row.marks),
            tuple(mark.availability_time_ns for mark in row.marks),
        )

    left = sorted(source, key=lambda row: (row.event.decision_ns, row.event.event_id))
    right = sorted(control, key=lambda row: (row.event.decision_ns, row.event.event_id))
    return len(left) == len(right) and [exposure(row) for row in left] == [
        exposure(row) for row in right
    ]


def _base_event_id(value: str) -> str:
    output = str(value)
    for scenario in (":NORMAL", ":STRESSED_1_5X"):
        if output.endswith(scenario):
            return output[: -len(scenario)]
    return output


def _concentration_receipt(
    *,
    candidate_id: str,
    source_exact_result_hash: str,
    concentration: Mapping[str, Any],
    final_development_evidence_hash: str,
) -> dict[str, Any]:
    maximums = dict(concentration["worst_case_maximums"])
    core = {
        "schema": CONCENTRATION_RECEIPT_SCHEMA,
        "candidate_id": candidate_id,
        "source_exact_result_hash": source_exact_result_hash,
        "maximum_single_day_profit_share": float(
            maximums["maximum_single_day_profit_share"]
        ),
        "maximum_single_trade_profit_share": float(
            maximums["maximum_single_trade_profit_share"]
        ),
        "maximum_single_event_profit_share": float(
            maximums["maximum_single_event_profit_share"]
        ),
        "maximum_allowed_share": MAXIMUM_CONCENTRATION_SHARE,
        "unique_ledger_denominator": True,
        "rolling_start_multiplicity": 0,
        "final_development_evidence_hash": final_development_evidence_hash,
        "evidence_role": "VIEWED_FINAL_DEVELOPMENT_ONLY",
    }
    return {**core, "receipt_hash": stable_hash(core)}


def _verify_shard(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    if row.get("schema") != SCHEMA or row.get("status") != (
        "COMPLETE_READ_ONLY_TIER_G_CONTROL_SHARD"
    ):
        raise AutonomousTierGControlsError("Tier-G shard schema/status mismatch")
    expected = stable_hash(
        {key: item for key, item in row.items() if key != "result_hash"}
    )
    if str(row.get("result_hash")) != expected:
        raise AutonomousTierGControlsError("Tier-G shard result hash drift")
    counts = dict(row.get("counts") or {})
    for field in (
        "authoritative_promotion_count",
        "xfa_paths_started",
        "registry_writes",
        "database_writes",
        "q4_access_count_delta",
        "data_purchase_count",
        "broker_connections",
        "orders",
    ):
        if int(counts.get(field, -1)) != 0:
            raise AutonomousTierGControlsError(
                f"read-only Tier-G control invariant failed: {field}"
            )
    return row


__all__ = [
    "AutonomousTierGControlsError",
    "COMPOSITE_SCHEMA",
    "CONTROL_SHIFT_COUNT",
    "MAXIMUM_CANDIDATES",
    "MAXIMUM_CONCENTRATION_SHARE",
    "SCHEMA",
    "build_autonomous_tier_g_controls",
    "compose_autonomous_tier_g_control_shards",
    "tier_g_controls_worker",
]
