"""Honest read-only bank of development policies with observed Combine passes.

The bank is deliberately weaker than a graduated bank.  It records exact
development evidence for standalone candidates and marginally useful books,
deduplicates immutable specifications and episode behaviour, and never writes
an authoritative promotion or starts XFA.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_combine_candidate_bank import (
    SCHEMA as CANDIDATE_BANK_SCHEMA,
)
from hydra.production.autonomous_marginal_combine_books import COMPOSITE_SCHEMA


SCHEMA = "hydra_autonomous_combine_pass_observed_development_bank_v1"
MINIMUM_BANK_TARGET = 50
MAXIMUM_BANK_CAPACITY = 100
HORIZONS = (5, 10, 20)
MAXIMUM_TIER_Q_MLL_BREACH_RATE = 0.10


class AutonomousCombinePassBankError(RuntimeError):
    """Source evidence cannot support an honest pass-observed bank."""


def build_autonomous_combine_pass_observed_bank(
    candidate_bank_value: Mapping[str, Any],
    marginal_books_value: Mapping[str, Any],
    *,
    capacity: int = MAXIMUM_BANK_CAPACITY,
) -> dict[str, Any]:
    """Build a 50--100 capacity, non-promotional development bank.

    ``capacity`` is a ceiling, not a quota.  Missing candidates are reported as
    a shortage and are never replaced with duplicate or non-passing policies.
    """

    if not MINIMUM_BANK_TARGET <= int(capacity) <= MAXIMUM_BANK_CAPACITY:
        raise AutonomousCombinePassBankError(
            "pass-observed bank capacity must be between 50 and 100"
        )
    candidate_bank = _unwrap_source(
        candidate_bank_value, key="candidate_bank", schema=CANDIDATE_BANK_SCHEMA
    )
    marginal_books = _unwrap_source(
        marginal_books_value,
        key="marginal_book_composite",
        schema=COMPOSITE_SCHEMA,
    )
    _require_read_only_source(candidate_bank, "candidate bank")
    _require_read_only_source(marginal_books, "marginal book composite")

    eligible: list[dict[str, Any]] = []
    tier_q_component_ids = {
        str(value.get("candidate_id") or "")
        for value in candidate_bank.get("candidates", ())
        if value.get("tier_q_contract_cleared") is True
    }
    exact_observed_count = 0
    for value in candidate_bank.get("candidates", ()):
        row = _standalone_entry(value, candidate_bank)
        if row is not None:
            exact_observed_count += 1
            eligible.append(row)

    accepted_book_count = 0
    rejected_non_marginal_pass_book_count = 0
    for value in marginal_books.get("book_results", ()):
        has_pass = _book_has_paired_pass(value)
        if has_pass and value.get("marginally_accepted") is not True:
            rejected_non_marginal_pass_book_count += 1
            continue
        row = (
            _book_entry(
                value,
                marginal_books,
                tier_q_component_ids=tier_q_component_ids,
            )
            if has_pass
            else None
        )
        if row is not None:
            accepted_book_count += 1
            eligible.append(row)

    eligible.sort(key=_entry_rank)
    retained: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    structural_owner: dict[str, str] = {}
    behavioral_owner: dict[str, str] = {}
    for entry in eligible:
        policy_id = str(entry["policy_id"])
        structural = str(entry["fingerprints"]["policy_spec_hash"])
        behavioral = str(entry["fingerprints"]["episode_behavior_hash"])
        duplicate_of = structural_owner.get(structural)
        reason = "DUPLICATE_POLICY_SPEC"
        fingerprint = structural
        if duplicate_of is None:
            duplicate_of = behavioral_owner.get(behavioral)
            reason = "DUPLICATE_EPISODE_BEHAVIOR"
            fingerprint = behavioral
        if duplicate_of is not None:
            exclusions.append(
                {
                    "policy_id": policy_id,
                    "reason": reason,
                    "retained_policy_id": duplicate_of,
                    "duplicate_fingerprint": fingerprint,
                }
            )
            continue
        structural_owner[structural] = policy_id
        behavioral_owner[behavioral] = policy_id
        retained.append(entry)

    deduplicated_count = len(retained)
    overflow = retained[int(capacity) :]
    retained = retained[: int(capacity)]
    exclusions.extend(
        {
            "policy_id": str(entry["policy_id"]),
            "reason": "HONEST_CAPACITY_CEILING",
            "retained_policy_id": None,
            "duplicate_fingerprint": None,
        }
        for entry in overflow
    )

    tier_counts = {
        tier: sum(entry["evidence_tier"] == tier for entry in retained)
        for tier in ("E", "Q")
    }
    source_counts = {
        source: sum(entry["source_kind"] == source for entry in retained)
        for source in ("EXACT_STANDALONE", "MARGINALLY_ACCEPTED_BOOK")
    }
    observed = len(retained)
    shortage = max(MINIMUM_BANK_TARGET - observed, 0)
    core = {
        "schema": SCHEMA,
        "status": (
            "COMBINE_PASS_OBSERVED_DEVELOPMENT_BANK_TARGET_REACHED"
            if shortage == 0
            else "COMBINE_PASS_OBSERVED_DEVELOPMENT_BANK_SHORTAGE"
        ),
        "source_candidate_bank_hash": str(candidate_bank["result_hash"]),
        "source_marginal_book_composite_hash": str(marginal_books["result_hash"]),
        "capacity_contract": {
            "minimum_target": MINIMUM_BANK_TARGET,
            "maximum_capacity": int(capacity),
            "capacity_is_not_a_quota": True,
            "unsafe_or_duplicate_fill_prohibited": True,
        },
        "policies": retained,
        "policy_ids": [str(entry["policy_id"]) for entry in retained],
        "counts": {
            "eligible_exact_standalone_count": exact_observed_count,
            "eligible_marginal_book_count": accepted_book_count,
            "rejected_non_marginal_pass_book_count": (
                rejected_non_marginal_pass_book_count
            ),
            "eligible_before_deduplication_count": len(eligible),
            "deduplicated_eligible_count": deduplicated_count,
            "duplicate_exclusion_count": sum(
                str(value["reason"]).startswith("DUPLICATE")
                for value in exclusions
            ),
            "capacity_exclusion_count": len(overflow),
            "bank_policy_count": observed,
            "shortage_to_minimum_target": shortage,
            "tier_e_count": tier_counts["E"],
            "tier_q_count": tier_counts["Q"],
            "exact_standalone_count": source_counts["EXACT_STANDALONE"],
            "marginally_accepted_book_count": source_counts[
                "MARGINALLY_ACCEPTED_BOOK"
            ],
            "authoritative_promotion_count": 0,
            "tier_g_count": 0,
            "xfa_paths_started": 0,
        },
        "deduplication": {
            "policy_spec_hash_unique": True,
            "episode_behavior_hash_unique": True,
            "behavior_contract": "CANONICAL_ACCOUNT_EPISODE_SUMMARIES_V1",
            "excluded_identity_metadata": [
                "policy_id",
                "policy_spec_hash",
                "component_ids",
                "legacy_episode_path_hash",
                "legacy_episode_receipt_hash",
                "source_metadata",
            ],
            "exclusions": exclusions,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "allowed_evidence_tiers": ["E", "Q"],
        "tier_g_claimed": False,
        "independent_confirmation_claimed": False,
        "promotion_status": None,
        "xfa_paths_started": 0,
        "database_writes": 0,
        "registry_writes": 0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": (
            "RUN_FROZEN_CONTROLS_AND_CONCENTRATION_WITHOUT_PROMOTION"
            if shortage == 0
            else "REPLENISH_WITH_MATERIALLY_DISTINCT_PASS_OBSERVED_POLICIES"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def _standalone_entry(
    value: Mapping[str, Any], _source: Mapping[str, Any]
) -> dict[str, Any] | None:
    if value.get("authoritative_promotion_status") is not None:
        raise AutonomousCombinePassBankError(
            "standalone source carries an authoritative promotion"
        )
    if not bool(dict(value.get("observed_passes") or {}).get(
        "normal_and_stressed_same_cell"
    )):
        return None
    tier_q = value.get("tier_q_contract_cleared") is True
    selected_cell_key = "best_safe_cell" if tier_q else "best_observed_pass_cell"
    cell = dict(value.get(selected_cell_key) or {})
    if not _cell_has_paired_pass(cell):
        raise AutonomousCombinePassBankError(
            f"standalone paired-pass flag has no valid {selected_cell_key}"
        )
    candidate_id = _required(value, "candidate_id")
    policy_spec_hash = _required(value, "candidate_fingerprint")
    normal_hash = _required(dict(cell.get("normal") or {}), "episode_path_hash")
    stressed_hash = _required(
        dict(cell.get("stressed") or {}), "episode_path_hash"
    )
    horizon = int(cell.get("horizon_trading_days", 0))
    if horizon not in HORIZONS:
        raise AutonomousCombinePassBankError("standalone pass horizon is not frozen")
    horizons = {str(value): _unavailable_exact_horizon(value) for value in HORIZONS}
    horizons[str(horizon)] = {
        "evaluation_status": "OBSERVED_EXACT_DEVELOPMENT_CELL",
        "normal_and_stressed_pass_observed": True,
        "overall": {
            "normal": _compact_scenario(dict(cell["normal"])),
            "stressed": _compact_scenario(dict(cell["stressed"])),
        },
        "held_out_development": None,
        "held_out_status": "NOT_SEPARATELY_AVAILABLE_IN_COMPACT_EXACT_BANK",
    }
    behavior_hash = _canonical_episode_behavior_hash(
        account_label=str(cell.get("account_label") or ""),
        account_size_usd=cell.get("account_size_usd"),
        horizons=horizons,
    )
    tier = "Q" if tier_q else "E"
    return _entry_core(
        policy_id=candidate_id,
        source_kind="EXACT_STANDALONE",
        tier=tier,
        account_label=str(cell.get("account_label") or ""),
        account_size_usd=cell.get("account_size_usd"),
        components=[candidate_id],
        horizons=horizons,
        role_contract={
            "overall_role": "VIEWED_DEVELOPMENT_ONLY",
            "design_blocks": None,
            "held_out_development_blocks": None,
            "held_out_partition_available": False,
        },
        tier_q_gate_results=dict(value.get("tier_q_gate_results") or {}),
        policy_spec_hash=policy_spec_hash,
        episode_behavior_hash=behavior_hash,
        source_episode_receipt_hashes=[normal_hash, stressed_hash],
        source_result_hash=_required(value, "source_exact_result_hash"),
        extra_fingerprints={
            "primary_evidence_cell": selected_cell_key,
            "realized_behavioral_fingerprint": value.get(
                "realized_behavioral_fingerprint"
            ),
            "qd_cell": value.get("qd_cell"),
            "compact_bundle_hash": dict(
                value.get("compact_evidence_bundle") or {}
            ).get("bundle_hash"),
        },
    )


def _book_entry(
    value: Mapping[str, Any],
    source: Mapping[str, Any],
    *,
    tier_q_component_ids: set[str],
) -> dict[str, Any]:
    if value.get("authoritative_promotion_status") is not None or any(
        int(value.get(key, 0) or 0)
        for key in ("xfa_paths_started", "database_writes", "registry_writes")
    ):
        raise AutonomousCombinePassBankError(
            "book source carries a forbidden promotion or side effect"
        )
    if value.get("marginally_accepted") is not True:
        raise AutonomousCombinePassBankError("book lacks accepted marginal value")
    policy_id = _required(value, "policy_id")
    spec_hash = _required(value, "policy_spec_hash")
    evidence = dict(value.get("episode_evidence") or {})
    receipt_hash = _required(evidence, "receipt_hash")
    horizons: dict[str, Any] = {}
    for horizon in HORIZONS:
        key = str(horizon)
        overall = _book_pair(value, "summaries", key)
        heldout = _book_role_pair(value, "HELD_OUT_DEVELOPMENT", key)
        horizons[key] = {
            "evaluation_status": "EXACT_CHRONOLOGICAL_BOOK_REPLAY",
            "normal_and_stressed_pass_observed": _pair_has_pass(overall),
            "overall": overall,
            "held_out_development": heldout,
            "held_out_normal_and_stressed_pass_observed": _pair_has_pass(heldout),
        }
    if not any(
        value["normal_and_stressed_pass_observed"] for value in horizons.values()
    ):
        raise AutonomousCombinePassBankError("included book has no paired pass")
    role = dict(value.get("selection_role_contract") or {})
    components = [str(item) for item in value.get("component_ids", ())]
    q_gates = _recalculate_book_tier_q_gates(
        value,
        horizons=horizons,
        components=components,
        tier_q_component_ids=tier_q_component_ids,
        evidence=evidence,
    )
    tier = "Q" if all(q_gates.values()) else "E"
    behavior_hash = _canonical_episode_behavior_hash(
        account_label=str(value.get("account_label") or ""),
        account_size_usd=_account_size(str(value.get("account_label") or "")),
        horizons=horizons,
    )
    return _entry_core(
        policy_id=policy_id,
        source_kind="MARGINALLY_ACCEPTED_BOOK",
        tier=tier,
        account_label=str(value.get("account_label") or ""),
        account_size_usd=_account_size(str(value.get("account_label") or "")),
        components=components,
        horizons=horizons,
        role_contract={
            "overall_role": str(source.get("evidence_role") or ""),
            "design_blocks": list(role.get("DESIGN") or ()),
            "held_out_development_blocks": list(
                role.get("HELD_OUT_DEVELOPMENT") or ()
            ),
            "held_out_partition_available": True,
        },
        tier_q_gate_results=q_gates,
        policy_spec_hash=spec_hash,
        episode_behavior_hash=behavior_hash,
        source_episode_receipt_hashes=[receipt_hash],
        source_result_hash=_required(value, "result_hash"),
        extra_fingerprints={
            "source_episode_receipt_hash": receipt_hash,
            "component_set_hash": stable_hash(sorted(value.get("component_ids", ()))),
            "governor_profile_id": value.get("governor_profile_id"),
            "marginal_contribution_hash": stable_hash(
                dict(value.get("marginal_contribution") or {})
            ),
        },
    )


def _recalculate_book_tier_q_gates(
    value: Mapping[str, Any],
    *,
    horizons: Mapping[str, Any],
    components: Sequence[str],
    tier_q_component_ids: set[str],
    evidence: Mapping[str, Any],
) -> dict[str, bool]:
    safe_pairs = []
    for horizon in HORIZONS:
        pair = dict(dict(horizons.get(str(horizon)) or {}).get("overall") or {})
        stressed = dict(pair.get("stressed") or {})
        if (
            _pair_has_pass(pair)
            and float(stressed.get("net_total_usd") or -math.inf) > 0.0
            and stressed.get("mll_breach_rate") is not None
            and float(stressed["mll_breach_rate"])
            <= MAXIMUM_TIER_Q_MLL_BREACH_RATE
            and all(
                int(dict(pair.get(scenario) or {}).get("episode_count") or 0) > 0
                for scenario in ("normal", "stressed")
            )
        ):
            safe_pairs.append(pair)
    accounting_valid = bool(
        int(evidence.get("record_count") or 0) > 0
        and evidence.get("receipt_hash")
        and int(value.get("completed_episode_count") or 0)
        == int(evidence.get("record_count") or 0)
        and value.get("signal_recomputation_performed") is False
        and value.get("quantity_tiers_materialized_before_book_replay") is True
        and value.get("additional_quantity_scaling") is False
    )
    return {
        "causal_accounting_valid": accounting_valid,
        "positive_stressed_economics": bool(safe_pairs),
        "acceptable_mll": bool(safe_pairs),
        "useful_target_velocity": bool(safe_pairs),
        "tier_q_components_only": bool(
            components
            and all(component in tier_q_component_ids for component in components)
        ),
        "compact_evidence_bundle_complete": bool(
            value.get("policy_spec_hash")
            and evidence.get("receipt_hash")
            and safe_pairs
        ),
        "behavioral_uniqueness_enforced_by_bank": True,
    }


def _canonical_episode_behavior_hash(
    *,
    account_label: str,
    account_size_usd: Any,
    horizons: Mapping[str, Any],
) -> str:
    """Hash policy-independent episode outcomes available in compact evidence.

    Legacy episode/path receipts include ``policy_id`` and component metadata.
    They remain recorded as provenance, but are deliberately excluded from the
    behavioural identity.  This conservative compact canonicalisation can
    merge economically identical policies even when their legacy receipt
    hashes differ.
    """

    canonical_horizons: dict[str, Any] = {}
    for horizon in HORIZONS:
        value = dict(horizons.get(str(horizon)) or {})
        overall = value.get("overall")
        heldout = value.get("held_out_development")
        if overall is None and heldout is None:
            continue
        canonical_horizons[str(horizon)] = {
            "overall": _canonical_pair(dict(overall or {})),
            "held_out_development": (
                _canonical_pair(dict(heldout))
                if isinstance(heldout, Mapping)
                else None
            ),
        }
    if not account_label or not canonical_horizons:
        raise AutonomousCombinePassBankError(
            "canonical episode behaviour lacks account or horizon evidence"
        )
    return stable_hash(
        {
            "account_label": account_label,
            "account_size_usd": account_size_usd,
            "horizons": canonical_horizons,
        }
    )


def _canonical_pair(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        scenario: {
            key: item
            for key, item in dict(value.get(scenario) or {}).items()
            if key != "episode_path_hash"
        }
        for scenario in ("normal", "stressed")
    }


def _entry_core(
    *,
    policy_id: str,
    source_kind: str,
    tier: str,
    account_label: str,
    account_size_usd: Any,
    components: Sequence[str],
    horizons: Mapping[str, Any],
    role_contract: Mapping[str, Any],
    tier_q_gate_results: Mapping[str, Any],
    policy_spec_hash: str,
    episode_behavior_hash: str,
    source_episode_receipt_hashes: Sequence[str],
    source_result_hash: str,
    extra_fingerprints: Mapping[str, Any],
) -> dict[str, Any]:
    if tier not in {"E", "Q"}:
        raise AutonomousCombinePassBankError("bank tier must be E or Q")
    if not account_label or not components:
        raise AutonomousCombinePassBankError("policy account/components incomplete")
    return {
        "policy_id": policy_id,
        "source_kind": source_kind,
        "classification_status": "COMBINE_PASS_OBSERVED_DEVELOPMENT",
        "evidence_tier": tier,
        "account": {
            "label": account_label,
            "account_size_usd": account_size_usd,
        },
        "components": list(components),
        "horizons": dict(horizons),
        "evidence_roles": dict(role_contract),
        "tier_q_gate_results": dict(tier_q_gate_results),
        "tier_q_contract_cleared": tier == "Q",
        "fingerprints": {
            "policy_spec_hash": policy_spec_hash,
            "episode_behavior_hash": episode_behavior_hash,
            "episode_behavior_hash_contract": (
                "CANONICAL_ACCOUNT_EPISODE_SUMMARIES_V1"
            ),
            "source_episode_receipt_hashes": list(source_episode_receipt_hashes),
            "source_result_hash": source_result_hash,
            **dict(extra_fingerprints),
        },
        "authoritative_promotion_status": None,
        "tier_g_claimed": False,
        "independent_confirmation_claimed": False,
        "xfa_paths_started": 0,
    }


def _book_has_paired_pass(value: Mapping[str, Any]) -> bool:
    return any(
        _pair_has_pass(_book_pair(value, "summaries", str(horizon)))
        for horizon in HORIZONS
    )


def _cell_has_paired_pass(value: Mapping[str, Any]) -> bool:
    return (
        int(dict(value.get("normal") or {}).get("pass_count", 0)) > 0
        and int(dict(value.get("stressed") or {}).get("pass_count", 0)) > 0
    )


def _book_pair(
    value: Mapping[str, Any], summary_key: str, horizon: str
) -> dict[str, Any]:
    summaries = dict(value.get(summary_key) or {})
    return {
        "normal": _compact_scenario(
            dict(dict(summaries.get("NORMAL") or {}).get(horizon) or {})
        ),
        "stressed": _compact_scenario(
            dict(dict(summaries.get("STRESSED_1_5X") or {}).get(horizon) or {})
        ),
    }


def _book_role_pair(
    value: Mapping[str, Any], role: str, horizon: str
) -> dict[str, Any]:
    roles = dict(value.get("summaries_by_role") or {})
    role_summaries = dict(roles.get(role) or {})
    return {
        "normal": _compact_scenario(
            dict(dict(role_summaries.get("NORMAL") or {}).get(horizon) or {})
        ),
        "stressed": _compact_scenario(
            dict(
                dict(role_summaries.get("STRESSED_1_5X") or {}).get(horizon)
                or {}
            )
        ),
    }


def _pair_has_pass(value: Mapping[str, Any]) -> bool:
    return (
        int(dict(value.get("normal") or {}).get("pass_count", 0)) > 0
        and int(dict(value.get("stressed") or {}).get("pass_count", 0)) > 0
    )


def _compact_scenario(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "episode_count": _first(value, "episode_count"),
        "requested_start_count": _first(value, "requested_start_count"),
        "full_coverage_start_count": _first(value, "full_coverage_start_count"),
        "data_censored_count": _first(
            value, "data_censored_count", "data_censored_start_count"
        ),
        "pass_count": int(_first(value, "pass_count", default=0)),
        "pass_rate": float(_first(value, "pass_rate", default=0.0)),
        "net_total_usd": _first(value, "net_total_usd", "net_total"),
        "net_median_usd": _first(value, "net_median_usd", "net_median"),
        "mll_breach_count": _first(value, "mll_breach_count"),
        "mll_breach_rate": _first(value, "mll_breach_rate"),
        "minimum_mll_buffer_usd": _first(
            value, "minimum_mll_buffer_usd", "minimum_mll_buffer"
        ),
        "consistency_rate": _first(
            value, "consistency_compliance_rate", "consistency_rate"
        ),
        "target_progress_p25": _first(value, "target_progress_p25"),
        "target_progress_median": _first(value, "target_progress_median"),
        "target_progress_p75": _first(value, "target_progress_p75"),
        "median_days_to_target": _first(value, "median_days_to_target"),
        "accepted_event_count": _first(value, "accepted_event_count"),
        "skipped_event_count": _first(value, "skipped_event_count"),
        "best_day_concentration_max": _first(
            value, "best_day_concentration_max"
        ),
        "maximum_mini_equivalent_max": _first(
            value, "maximum_mini_equivalent_max"
        ),
        "maximum_mini_equivalent_mean": _first(
            value, "maximum_mini_equivalent_mean"
        ),
        "maximum_net_directional_exposure_max": _first(
            value, "maximum_net_directional_exposure_max"
        ),
        "maximum_net_directional_exposure_mean": _first(
            value, "maximum_net_directional_exposure_mean"
        ),
        "mean_daily_contract_utilization": _first(
            value, "mean_daily_contract_utilization"
        ),
        "mean_daily_maximum_mini_equivalent": _first(
            value, "mean_daily_maximum_mini_equivalent"
        ),
        "terminal_distribution": dict(value.get("terminal_distribution") or {}),
        "block_pass_counts": dict(value.get("block_pass_counts") or {}),
        "blocks_with_passes": list(value.get("blocks_with_passes") or ()),
        "single_trade_domination": _first(value, "single_trade_domination"),
        "episode_path_hash": value.get("episode_path_hash"),
    }


def _unavailable_exact_horizon(horizon: int) -> dict[str, Any]:
    return {
        "evaluation_status": "NOT_AVAILABLE_IN_COMPACT_EXACT_BANK",
        "normal_and_stressed_pass_observed": False,
        "overall": None,
        "held_out_development": None,
        "held_out_status": "NOT_SEPARATELY_AVAILABLE_IN_COMPACT_EXACT_BANK",
        "horizon_trading_days": horizon,
    }


def _entry_rank(value: Mapping[str, Any]) -> tuple[Any, ...]:
    horizons = list(dict(value.get("horizons") or {}).values())
    heldout_pairs = sum(
        _pair_has_pass(dict(item.get("held_out_development") or {}))
        for item in horizons
    )
    total_stressed_passes = sum(
        int(
            dict(dict(item.get("overall") or {}).get("stressed") or {}).get(
                "pass_count", 0
            )
        )
        for item in horizons
    )
    mll = min(
        (
            float(value)
            for item in horizons
            for value in [
                dict(dict(item.get("overall") or {}).get("stressed") or {}).get(
                    "mll_breach_rate"
                )
            ]
            if value is not None
        ),
        default=math.inf,
    )
    return (
        0 if value.get("evidence_tier") == "Q" else 1,
        -heldout_pairs,
        -total_stressed_passes,
        mll,
        str(value.get("policy_id") or ""),
    )


def _unwrap_source(
    value: Mapping[str, Any], *, key: str, schema: str
) -> dict[str, Any]:
    source = dict(value)
    if source.get("schema") == schema:
        _verify_self_hash(source)
        return source
    nested = source.get(key)
    if not isinstance(nested, Mapping):
        raise AutonomousCombinePassBankError(f"missing {key} source payload")
    _verify_self_hash(source)
    payload = dict(nested)
    if payload.get("schema") != schema:
        raise AutonomousCombinePassBankError(f"unexpected {key} schema")
    _verify_self_hash(payload)
    return payload


def _verify_self_hash(value: Mapping[str, Any]) -> None:
    claimed = str(value.get("result_hash") or "")
    core = {key: item for key, item in value.items() if key != "result_hash"}
    if not claimed or stable_hash(core) != claimed:
        raise AutonomousCombinePassBankError("source result hash mismatch")


def _require_read_only_source(value: Mapping[str, Any], label: str) -> None:
    forbidden = (
        "authoritative_promotion_count",
        "xfa_paths_started",
        "database_writes",
        "registry_writes",
        "broker_connections",
        "orders",
    )
    counts = dict(value.get("counts") or {})
    if value.get("promotion_status") is not None or any(
        int(value.get(key, counts.get(key, 0)) or 0) for key in forbidden
    ):
        raise AutonomousCombinePassBankError(f"{label} is not read-only")


def _required(value: Mapping[str, Any], key: str) -> str:
    result = str(value.get(key) or "")
    if not result:
        raise AutonomousCombinePassBankError(f"missing immutable {key}")
    return result


def _account_size(label: str) -> int | None:
    text = label.strip().upper()
    if text.endswith("K"):
        try:
            return int(float(text[:-1]) * 1000)
        except ValueError:
            return None
    return None


def _first(value: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in value:
            return value[key]
    return default


__all__ = [
    "AutonomousCombinePassBankError",
    "MAXIMUM_BANK_CAPACITY",
    "MINIMUM_BANK_TARGET",
    "SCHEMA",
    "build_autonomous_combine_pass_observed_bank",
]
