"""Pure, fail-closed Tier-G development graduation.

The input to this module is the reconciled, read-only output produced by
``autonomous_tier_g_controls``.  Graduation is a candidate-level development
classification only.  It is neither independent confirmation nor forward
evidence, and it does not start an XFA path or write an authoritative status.

A single immutable sleeve plus its exact selected account-policy cell is a
complete account policy.  It therefore does not need to be wrapped in a
multi-sleeve portfolio merely to receive an honest development-book receipt.
The selected-cell hash binds the causal sleeve, integer quantity, account
governor and exact rule snapshot that were replayed by the source control
module.

The module deliberately does not invent a market inventory, an XFA book, an
XFA profile, successful Combine path ledgers, confirmation, or forward state.
Those require separate evidence.  All returned objects are ordinary mappings;
there are no filesystem, registry, database, broker, order, Q4 or data-purchase
side effects.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_tier_g_controls import (
    COMPOSITE_SCHEMA as SOURCE_COMPOSITE_SCHEMA,
    CONTROL_SHIFT_COUNT,
    MAXIMUM_BLOCK_PASS_SHARE,
    MAXIMUM_CONCENTRATION_SHARE,
)


SCHEMA = "hydra_autonomous_tier_g_development_graduation_v1"
RECEIPT_SCHEMA = "hydra_graduated_development_book_receipt_v1"
BOOK_SCHEMA = "hydra_single_sleeve_complete_account_policy_v1"
BLOCK_RECEIPT_SCHEMA = "hydra_tier_g_block_diversity_receipt_v1"
MLL_RECEIPT_SCHEMA = "hydra_tier_g_mll_receipt_v1"
CONSISTENCY_RECEIPT_SCHEMA = "hydra_tier_g_consistency_receipt_v1"

SOURCE_STATUS = "COMPLETE_RECONCILED_TIER_G_CONTROL_SHARDS"
GRADUATION_STATUS = "GRADUATED_DEVELOPMENT_BOOK"
EVIDENCE_ROLE = "VIEWED_FINAL_DEVELOPMENT_ONLY"
MAXIMUM_MLL_BREACH_RATE = 0.10
MINIMUM_PASSES_PER_SCENARIO = 2
MINIMUM_PASS_CONTEXTS = 2

_ACCOUNT_SIZES = {"50K": 50_000, "100K": 100_000, "150K": 150_000}
_REQUIRED_CONTROL_GATES = (
    "tier_q_source",
    "identity_best_parent_reconciled",
    "multiple_normal_and_stressed_passes",
    "multiple_temporal_contexts",
    "block_concentration_le_75pct",
    "unique_ledger_day_trade_event_concentration_le_50pct",
    "synthetic_controls_complete_and_exposure_matched",
    "not_worse_than_median_synthetic_pass_count",
    "not_worse_than_median_synthetic_stressed_net",
    "final_development_evidence_hash_complete",
)
_FORBIDDEN_SOURCE_COUNTERS = (
    "authoritative_promotion_count",
    "xfa_paths_started",
    "registry_writes",
    "database_writes",
    "q4_access_count_delta",
    "data_purchase_count",
    "broker_connections",
    "orders",
)


class AutonomousTierGGraduationError(RuntimeError):
    """Tier-G development graduation evidence is absent or inconsistent."""


def build_graduated_development_books(
    control_composite: Mapping[str, Any],
) -> dict[str, Any]:
    """Return deterministic development-book receipts without side effects.

    Malformed or internally inconsistent source evidence raises immediately.
    A structurally valid candidate that fails an additional development gate
    (for example MLL or pass/consistency reconciliation) remains ungraduated
    and is reported with explicit reasons.
    """

    source = _verify_control_composite(control_composite)
    candidate_results = sorted(
        (dict(row) for row in source["candidate_results"]),
        key=lambda row: str(row["candidate_id"]),
    )
    verified = [_verify_candidate_result(row) for row in candidate_results]

    declared_ready = sorted(
        str(value)
        for value in dict(source["candidate_ids"])["g_control_ready"]
    )
    observed_ready = sorted(
        str(row["candidate_id"])
        for row in verified
        if row["g_control_ready"] is True
    )
    if declared_ready != observed_ready:
        raise AutonomousTierGGraduationError(
            "source G-control-ready inventory does not reconcile"
        )

    receipts: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    for row in verified:
        gate = _graduation_gate(row)
        if gate["cleared"]:
            receipt = _build_receipt(source, row, gate)
            verify_graduated_development_book_receipt(receipt)
            receipts.append(receipt)
        else:
            exclusions.append(
                {
                    "candidate_id": str(row["candidate_id"]),
                    "retained_evidence_tier": "Q",
                    "reason_codes": list(gate["reason_codes"]),
                    "source_candidate_control_result_hash": str(
                        row["result_hash"]
                    ),
                }
            )

    receipt_hashes = [str(row["result_hash"]) for row in receipts]
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "COMPLETE_READ_ONLY_TIER_G_DEVELOPMENT_GRADUATION",
        "source_tier_g_control_composite_hash": str(source["result_hash"]),
        "source_candidate_bank_hash": str(source["source_candidate_bank_hash"]),
        "source_exact_composite_hash": str(source["source_exact_composite_hash"]),
        "source_manifest_hash": str(source["source_manifest_hash"]),
        "graduation_contract": {
            "status": GRADUATION_STATUS,
            "evidence_role": EVIDENCE_ROLE,
            "minimum_normal_passes": MINIMUM_PASSES_PER_SCENARIO,
            "minimum_stressed_passes": MINIMUM_PASSES_PER_SCENARIO,
            "minimum_paired_pass_contexts": MINIMUM_PASS_CONTEXTS,
            "maximum_block_pass_share": MAXIMUM_BLOCK_PASS_SHARE,
            "maximum_unique_day_trade_event_profit_share": (
                MAXIMUM_CONCENTRATION_SHARE
            ),
            "maximum_mll_breach_rate": MAXIMUM_MLL_BREACH_RATE,
            "consistency_contract": (
                "EVERY_PASSED_EPISODE_MUST_BE_WITHIN_THE_EXACT_"
                "CONSISTENCY_COMPLIANT_EPISODE_COUNT"
            ),
            "control_comparison": "NOT_WORSE_THAN_MEDIAN_SYNTHETIC_NULL",
            "single_sleeve_is_complete_account_policy": True,
            "independent_confirmation_required_for_tier_c": True,
            "confirmation_or_forward_claimed": False,
            "xfa_allowed_in_this_module": False,
        },
        "graduated_development_books": receipts,
        "graduation_receipt_hashes": receipt_hashes,
        "not_graduated": exclusions,
        "candidate_ids": {
            "graduated_development_books": [
                str(row["candidate_id"]) for row in receipts
            ],
            "retained_tier_q": [
                str(row["candidate_id"]) for row in exclusions
            ],
        },
        "counts": {
            "source_candidate_count": len(verified),
            "source_g_control_ready_count": len(observed_ready),
            "graduated_development_book_count": len(receipts),
            "retained_tier_q_count": len(exclusions),
            "authoritative_promotion_writes": 0,
            "independent_confirmations": 0,
            "forward_packages": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "evidence_tier": "G_DEVELOPMENT_ONLY",
        "evidence_role": EVIDENCE_ROLE,
        "independent_confirmation_claimed": False,
        "forward_claimed": False,
        "xfa_status": "NOT_STARTED",
        "next_action": (
            "AUTHORITATIVE_PARENT_MAY_PERSIST_TIER_G_DEVELOPMENT_RECEIPTS_"
            "THEN_PREPARE_SEPARATE_CONFIRMATION"
            if receipts
            else "PRESERVE_TIER_Q_WITHOUT_GRADUATION"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def verify_graduated_development_book_receipt(
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify one receipt independently of the enclosing batch."""

    row = dict(receipt)
    claimed = str(row.pop("result_hash", ""))
    if not _is_sha256(claimed) or stable_hash(row) != claimed:
        raise AutonomousTierGGraduationError(
            "graduated development-book receipt hash drift"
        )
    if (
        receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("graduation_status") != GRADUATION_STATUS
        or receipt.get("evidence_tier") != "G"
        or receipt.get("evidence_role") != EVIDENCE_ROLE
        or receipt.get("tier_g_gate_cleared") is not True
        or receipt.get("complete_account_policy") is not True
        or receipt.get("independent_confirmation_claimed") is not False
        or receipt.get("confirmation_status") != "NOT_EVALUATED"
        or receipt.get("forward_status") != "NOT_STARTED"
        or receipt.get("xfa_status") != "NOT_STARTED"
        or receipt.get("frozen_before_confirmation") is not True
        or receipt.get("frozen_before_xfa") is not True
        or receipt.get("authoritative_write_performed") is not False
    ):
        raise AutonomousTierGGraduationError(
            "receipt overstates or omits its development-only status"
        )
    candidate_id = str(receipt.get("candidate_id") or "")
    sleeves = receipt.get("sleeve_ids")
    if (
        not candidate_id
        or not isinstance(sleeves, list)
        or sleeves != [candidate_id]
        or int(receipt.get("sleeve_count", -1)) != 1
        or receipt.get("policy_cardinality") != "SINGLE_SLEEVE"
    ):
        raise AutonomousTierGGraduationError(
            "single-sleeve complete account-policy identity drift"
        )
    account_label = str(receipt.get("account_label") or "")
    if (
        account_label not in _ACCOUNT_SIZES
        or int(receipt.get("account_size_usd", -1))
        != _ACCOUNT_SIZES[account_label]
    ):
        raise AutonomousTierGGraduationError("account label/size binding drift")
    markets = receipt.get("markets")
    if (
        not isinstance(markets, list)
        or not markets
        or markets != sorted(set(str(item) for item in markets))
        or any(not str(item).strip() for item in markets)
        or str(receipt.get("market_inventory_hash")) != stable_hash(markets)
        or not _is_sha256(receipt.get("frozen_account_policy_hash"))
    ):
        raise AutonomousTierGGraduationError(
            "receipt market/account-policy binding is incomplete"
        )
    for field in (
        "source_tier_g_control_composite_hash",
        "source_candidate_control_result_hash",
        "candidate_fingerprint",
        "behavioral_fingerprint",
        "selected_cell_hash",
        "combine_book_hash",
        "graduation_evidence_hash",
        "final_development_evidence_hash",
    ):
        if not _is_sha256(receipt.get(field)):
            raise AutonomousTierGGraduationError(
                f"graduation receipt hash is absent: {field}"
            )
    book = _verify_self_hashed_nested(
        receipt.get("combine_book"),
        hash_field="combine_book_hash",
        schema=BOOK_SCHEMA,
        label="combine book",
    )
    if (
        str(book["combine_book_hash"]) != str(receipt["combine_book_hash"])
        or str(book["candidate_id"]) != candidate_id
        or book.get("complete_account_policy") is not True
        or list(book.get("sleeve_ids") or ()) != [candidate_id]
        or list(book.get("markets") or ()) != markets
        or str(book.get("market_inventory_hash"))
        != str(receipt["market_inventory_hash"])
        or str(book.get("frozen_account_policy_hash"))
        != str(receipt["frozen_account_policy_hash"])
        or not isinstance(book.get("frozen_account_policy"), Mapping)
        or str(book.get("frozen_account_policy_hash"))
        != stable_hash(book.get("frozen_account_policy"))
    ):
        raise AutonomousTierGGraduationError("combine-book receipt binding drift")
    block = _verify_self_hashed_nested(
        receipt.get("block_diversity_receipt"),
        hash_field="receipt_hash",
        schema=BLOCK_RECEIPT_SCHEMA,
        label="block-diversity receipt",
    )
    mll = _verify_self_hashed_nested(
        receipt.get("mll_receipt"),
        hash_field="receipt_hash",
        schema=MLL_RECEIPT_SCHEMA,
        label="MLL receipt",
    )
    consistency = _verify_self_hashed_nested(
        receipt.get("consistency_receipt"),
        hash_field="receipt_hash",
        schema=CONSISTENCY_RECEIPT_SCHEMA,
        label="consistency receipt",
    )
    normal = _verify_episode_summary(receipt.get("normal_economics"), "NORMAL")
    stressed = _verify_episode_summary(
        receipt.get("stressed_economics"), "STRESSED"
    )
    expected_block = _block_diversity_receipt(candidate_id, normal, stressed)
    expected_mll = _mll_receipt(candidate_id, normal, stressed)
    expected_consistency = _consistency_receipt(candidate_id, normal, stressed)
    if any(
        stable_hash(observed) != stable_hash(expected)
        for observed, expected in (
            (block, expected_block),
            (mll, expected_mll),
            (consistency, expected_consistency),
        )
    ) or not all(value.get("cleared") is True for value in (block, mll, consistency)):
        raise AutonomousTierGGraduationError(
            "graduation receipt embeds an uncleared exact gate"
        )
    concentration = receipt.get("concentration_receipt")
    if not isinstance(concentration, Mapping):
        raise AutonomousTierGGraduationError(
            "graduation concentration receipt is absent"
        )
    concentration_core = dict(concentration)
    concentration_hash = str(concentration_core.pop("receipt_hash", ""))
    if (
        not _is_sha256(concentration_hash)
        or stable_hash(concentration_core) != concentration_hash
        or concentration.get("schema")
        != "hydra_candidate_concentration_receipt_v1"
        or str(concentration.get("candidate_id")) != candidate_id
        or concentration.get("unique_ledger_denominator") is not True
        or int(concentration.get("rolling_start_multiplicity", -1)) != 0
        or float(concentration.get("maximum_allowed_share", -1.0))
        != MAXIMUM_CONCENTRATION_SHARE
        or any(
            _finite(concentration.get(field), field)
            > MAXIMUM_CONCENTRATION_SHARE
            for field in (
                "maximum_single_day_profit_share",
                "maximum_single_trade_profit_share",
                "maximum_single_event_profit_share",
            )
        )
    ):
        raise AutonomousTierGGraduationError(
            "graduation concentration receipt does not reconcile"
        )
    binding = receipt.get("graduation_evidence_binding")
    if (
        not isinstance(binding, Mapping)
        or stable_hash(binding) != str(receipt["graduation_evidence_hash"])
        or str(binding.get("candidate_id")) != candidate_id
        or str(binding.get("source_tier_g_control_composite_hash"))
        != str(receipt["source_tier_g_control_composite_hash"])
        or str(binding.get("source_candidate_control_result_hash"))
        != str(receipt["source_candidate_control_result_hash"])
        or str(binding.get("combine_book_hash"))
        != str(receipt["combine_book_hash"])
        or str(binding.get("market_inventory_hash"))
        != str(receipt["market_inventory_hash"])
        or str(binding.get("frozen_account_policy_hash"))
        != str(receipt["frozen_account_policy_hash"])
        or str(binding.get("final_development_evidence_hash"))
        != str(receipt["final_development_evidence_hash"])
        or str(binding.get("normal_episode_path_hash"))
        != str(normal["episode_path_hash"])
        or str(binding.get("stressed_episode_path_hash"))
        != str(stressed["episode_path_hash"])
        or str(binding.get("block_diversity_receipt_hash"))
        != str(block["receipt_hash"])
        or str(binding.get("mll_receipt_hash")) != str(mll["receipt_hash"])
        or str(binding.get("consistency_receipt_hash"))
        != str(consistency["receipt_hash"])
        or str(binding.get("concentration_receipt_hash"))
        != concentration_hash
        or binding.get("evidence_role") != EVIDENCE_ROLE
    ):
        raise AutonomousTierGGraduationError(
            "graduation evidence binding does not reconcile"
        )
    if receipt.get("graduation_gate_results") != {
        "source_g_control_ready": True,
        "all_source_control_gates_true": True,
        "exact_identity_reconciled": True,
        "multiple_normal_and_stressed_passes": True,
        "paired_block_diversity_cleared": True,
        "mll_cleared": True,
        "consistency_cleared": True,
        "unique_ledger_concentration_cleared": True,
        "control_comparison_cleared": True,
        "positive_stressed_economics": True,
        "final_development_hash_reconciled": True,
        "complete_account_policy_frozen": True,
    }:
        raise AutonomousTierGGraduationError(
            "graduation gate inventory is incomplete"
        )
    for field in (
        "authoritative_promotion_writes",
        "independent_confirmations",
        "forward_packages",
        "xfa_paths_started",
        "registry_writes",
        "database_writes",
        "q4_access_count_delta",
        "data_purchase_count",
        "broker_connections",
        "orders",
    ):
        if int(receipt.get(field, -1)) != 0:
            raise AutonomousTierGGraduationError(
                f"graduation receipt contains forbidden activity: {field}"
            )
    return dict(receipt)


def verify_tier_g_development_graduation(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify the deterministic batch and every embedded receipt."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    if (
        not _is_sha256(claimed)
        or stable_hash(row) != claimed
        or value.get("schema") != SCHEMA
        or value.get("status")
        != "COMPLETE_READ_ONLY_TIER_G_DEVELOPMENT_GRADUATION"
    ):
        raise AutonomousTierGGraduationError(
            "Tier-G development graduation batch identity/hash drift"
        )
    receipts = value.get("graduated_development_books")
    exclusions = value.get("not_graduated")
    if not isinstance(receipts, list) or not isinstance(exclusions, list):
        raise AutonomousTierGGraduationError(
            "graduation batch candidate inventories are absent"
        )
    verified = [verify_graduated_development_book_receipt(item) for item in receipts]
    ids = [str(item["candidate_id"]) for item in verified]
    if ids != sorted(ids) or len(ids) != len(set(ids)):
        raise AutonomousTierGGraduationError(
            "graduated candidate inventory is unordered or duplicated"
        )
    if list(value.get("graduation_receipt_hashes") or ()) != [
        str(item["result_hash"]) for item in verified
    ]:
        raise AutonomousTierGGraduationError(
            "graduation receipt-hash inventory drift"
        )
    counts = dict(value.get("counts") or {})
    if (
        int(counts.get("graduated_development_book_count", -1)) != len(verified)
        or int(counts.get("retained_tier_q_count", -1)) != len(exclusions)
        or int(counts.get("source_candidate_count", -1))
        != len(verified) + len(exclusions)
    ):
        raise AutonomousTierGGraduationError("graduation batch counts drift")
    for field in (
        "authoritative_promotion_writes",
        "independent_confirmations",
        "forward_packages",
        "xfa_paths_started",
        "registry_writes",
        "database_writes",
        "q4_access_count_delta",
        "data_purchase_count",
        "broker_connections",
        "orders",
    ):
        if int(counts.get(field, -1)) != 0:
            raise AutonomousTierGGraduationError(
                f"graduation batch contains forbidden activity: {field}"
            )
    return dict(value)


def _verify_control_composite(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    if (
        not _is_sha256(claimed)
        or stable_hash(row) != claimed
        or value.get("schema") != SOURCE_COMPOSITE_SCHEMA
        or value.get("status") != SOURCE_STATUS
    ):
        raise AutonomousTierGGraduationError(
            "source Tier-G control composite identity/hash drift"
        )
    if value.get("evidence_role") != EVIDENCE_ROLE:
        raise AutonomousTierGGraduationError(
            "source controls are not frozen final-development evidence"
        )
    if (
        value.get("independent_confirmation_claimed") is not False
        or value.get("promotion_status") is not None
    ):
        raise AutonomousTierGGraduationError(
            "source controls contain inherited confirmation or promotion"
        )
    counts = dict(value.get("counts") or {})
    for field in _FORBIDDEN_SOURCE_COUNTERS:
        if int(counts.get(field, -1)) != 0:
            raise AutonomousTierGGraduationError(
                f"source control composite has a forbidden side effect: {field}"
            )
    for field in (
        "source_candidate_bank_hash",
        "source_exact_composite_hash",
        "source_manifest_hash",
    ):
        if not _is_sha256(value.get(field)):
            raise AutonomousTierGGraduationError(
                f"source composite provenance hash is absent: {field}"
            )
    contract = dict(value.get("control_contract") or {})
    if (
        int(contract.get("candidate_maximum", -1)) != 5
        or float(contract.get("maximum_day_trade_event_profit_share", -1.0))
        != MAXIMUM_CONCENTRATION_SHARE
        or int(contract.get("control_shift_count", -1)) != CONTROL_SHIFT_COUNT
        or contract.get("control_is_deployable") is not False
        or contract.get("control_can_promote") is not False
        or contract.get("identity_best_parent_required") is not True
    ):
        raise AutonomousTierGGraduationError("source control contract drift")
    candidates = value.get("candidate_results")
    receipts = value.get("concentration_receipts")
    if not isinstance(candidates, list) or not isinstance(receipts, Mapping):
        raise AutonomousTierGGraduationError(
            "source control candidate or concentration inventory is absent"
        )
    ids = [str(row.get("candidate_id") or "") for row in candidates]
    if (
        not ids
        or any(not item for item in ids)
        or len(ids) != len(set(ids))
        or set(receipts) != set(ids)
        or int(counts.get("source_candidate_count", -1)) != len(ids)
        or int(counts.get("selected_candidate_count", -1)) != len(ids)
    ):
        raise AutonomousTierGGraduationError(
            "source control candidate inventory does not reconcile"
        )
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        if stable_hash(receipts[candidate_id]) != stable_hash(
            candidate.get("concentration_receipt")
        ):
            raise AutonomousTierGGraduationError(
                "source composite concentration inventory drift"
            )
    if int(counts.get("exact_account_replay_count", -1)) != sum(
        int(candidate.get("exact_account_replay_count", -1))
        for candidate in candidates
    ) or int(counts.get("synthetic_control_count", -1)) != sum(
        len(candidate.get("synthetic_controls") or ()) for candidate in candidates
    ):
        raise AutonomousTierGGraduationError(
            "source control economic counters do not reconcile"
        )
    declared_ready = dict(value.get("candidate_ids") or {}).get("g_control_ready")
    if not isinstance(declared_ready, list) or len(declared_ready) != len(
        set(str(item) for item in declared_ready)
    ):
        raise AutonomousTierGGraduationError(
            "source G-control-ready inventory is invalid"
        )
    if int(counts.get("g_control_ready_count", -1)) != len(declared_ready):
        raise AutonomousTierGGraduationError("source G-control-ready count drift")
    return dict(value)


def _verify_candidate_result(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    if not _is_sha256(claimed) or stable_hash(row) != claimed:
        raise AutonomousTierGGraduationError(
            "candidate Tier-G control result hash drift"
        )
    candidate_id = str(value.get("candidate_id") or "")
    if not candidate_id:
        raise AutonomousTierGGraduationError("candidate identity is absent")
    for field in (
        "candidate_fingerprint",
        "behavioral_fingerprint",
        "selected_cell_hash",
        "source_exact_result_hash",
    ):
        if not _is_sha256(value.get(field)):
            raise AutonomousTierGGraduationError(
                f"candidate provenance hash is absent: {candidate_id}/{field}"
            )
    account_label = str(value.get("account_label") or "")
    markets = value.get("markets")
    frozen_policy = value.get("frozen_account_policy")
    if (
        account_label not in _ACCOUNT_SIZES
        or int(value.get("account_size_usd", -1)) != _ACCOUNT_SIZES[account_label]
        or not isinstance(markets, list)
        or not markets
        or markets != sorted(set(str(item) for item in markets))
        or any(not str(item).strip() for item in markets)
        or str(value.get("market_inventory_hash")) != stable_hash(markets)
        or not isinstance(frozen_policy, Mapping)
        or not frozen_policy
        or str(value.get("frozen_account_policy_hash"))
        != stable_hash(frozen_policy)
        or value.get("complete_account_policy") is not True
        or value.get("policy_sleeve_ids") != [candidate_id]
    ):
        raise AutonomousTierGGraduationError(
            f"candidate account/market/policy specification is incomplete: {candidate_id}"
        )
    if int(value.get("selected_horizon_trading_days", 0)) <= 0:
        raise AutonomousTierGGraduationError(
            f"candidate horizon is invalid: {candidate_id}"
        )
    for field in (
        "xfa_paths_started",
        "registry_writes",
        "database_writes",
        "broker_connections",
        "orders",
    ):
        if int(value.get(field, -1)) != 0:
            raise AutonomousTierGGraduationError(
                f"candidate control contains forbidden activity: {candidate_id}/{field}"
            )
    if (
        value.get("authoritative_promotion_status") is not None
        or value.get("independent_confirmation_claimed") is not False
    ):
        raise AutonomousTierGGraduationError(
            f"candidate inherited a forbidden status: {candidate_id}"
        )

    identity = dict(value.get("identity_best_parent") or {})
    if (
        identity.get("control_role") != "IDENTITY_AND_STANDALONE_BEST_PARENT"
        or identity.get("reconciled") is not True
    ):
        raise AutonomousTierGGraduationError(
            f"identity/best-parent evidence is absent: {candidate_id}"
        )
    normal = _verify_episode_summary(identity.get("normal"), "NORMAL")
    stressed = _verify_episode_summary(identity.get("stressed"), "STRESSED")
    if int(normal["episode_count"]) != int(stressed["episode_count"]):
        raise AutonomousTierGGraduationError(
            f"normal/stressed episode count differs: {candidate_id}"
        )

    concentration = _verify_concentration(
        value.get("unique_ledger_concentration"), candidate_id
    )
    controls = _verify_synthetic_controls(value.get("synthetic_controls"))
    comparison = _verify_control_comparison(
        value.get("control_comparison"), identity, controls
    )
    final = _verify_final_development_evidence(
        value.get("final_development_evidence"),
        candidate=value,
        identity=identity,
        concentration=concentration,
        controls=controls,
        comparison=comparison,
    )
    _verify_concentration_receipt(
        value.get("concentration_receipt"),
        candidate_id=candidate_id,
        source_exact_result_hash=str(value["source_exact_result_hash"]),
        concentration=concentration,
        final_development_evidence_hash=str(
            final["final_development_evidence_hash"]
        ),
    )
    gates = value.get("g_control_gate_results")
    if not isinstance(gates, Mapping) or any(
        not isinstance(gates.get(field), bool) for field in _REQUIRED_CONTROL_GATES
    ):
        raise AutonomousTierGGraduationError(
            f"candidate G-control gate inventory is incomplete: {candidate_id}"
        )
    ready = bool(value.get("g_control_ready"))
    expected_ready = all(bool(gates[field]) for field in _REQUIRED_CONTROL_GATES)
    if (
        ready != expected_ready
        or value.get("computed_development_tier")
        != ("G_CONTROL_READY" if ready else "Q_CONTROL_EVALUATED")
    ):
        raise AutonomousTierGGraduationError(
            f"candidate G-control-ready classification drift: {candidate_id}"
        )
    return dict(value)


def _graduation_gate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    identity = dict(candidate["identity_best_parent"])
    normal = dict(identity["normal"])
    stressed = dict(identity["stressed"])
    block = _block_diversity_receipt(
        str(candidate["candidate_id"]), normal, stressed
    )
    mll = _mll_receipt(str(candidate["candidate_id"]), normal, stressed)
    consistency = _consistency_receipt(
        str(candidate["candidate_id"]), normal, stressed
    )
    gates = dict(candidate["g_control_gate_results"])
    checks = {
        "source_g_control_ready": candidate["g_control_ready"] is True,
        "all_source_control_gates_true": all(
            bool(gates[field]) for field in _REQUIRED_CONTROL_GATES
        ),
        "exact_identity_reconciled": identity.get("reconciled") is True,
        "multiple_normal_and_stressed_passes": int(normal["pass_count"])
        >= MINIMUM_PASSES_PER_SCENARIO
        and int(stressed["pass_count"]) >= MINIMUM_PASSES_PER_SCENARIO,
        "paired_block_diversity_cleared": block["cleared"] is True,
        "mll_cleared": mll["cleared"] is True,
        "consistency_cleared": consistency["cleared"] is True,
        "unique_ledger_concentration_cleared": bool(
            dict(candidate["unique_ledger_concentration"])["cleared"]
        ),
        "control_comparison_cleared": bool(
            dict(candidate["control_comparison"])[
                "pass_count_not_worse_than_median_synthetic"
            ]
        )
        and bool(
            dict(candidate["control_comparison"])[
                "stressed_net_not_worse_than_median_synthetic"
            ]
        ),
        "positive_stressed_economics": float(stressed["net_total_usd"]) > 0.0,
        "final_development_hash_reconciled": _is_sha256(
            dict(candidate["final_development_evidence"])[
                "final_development_evidence_hash"
            ]
        ),
        "complete_account_policy_frozen": _is_sha256(
            candidate["selected_cell_hash"]
        )
        and _is_sha256(candidate["market_inventory_hash"])
        and _is_sha256(candidate["frozen_account_policy_hash"])
        and candidate.get("complete_account_policy") is True
        and candidate.get("policy_sleeve_ids") == [candidate["candidate_id"]],
    }
    reason_codes = [name.upper() for name, cleared in checks.items() if not cleared]
    return {
        "cleared": not reason_codes,
        "reason_codes": reason_codes,
        "checks": checks,
        "block_diversity_receipt": block,
        "mll_receipt": mll,
        "consistency_receipt": consistency,
    }


def _build_receipt(
    source: Mapping[str, Any],
    candidate: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    account_label = str(candidate["account_label"])
    final = dict(candidate["final_development_evidence"])
    concentration_receipt = dict(candidate["concentration_receipt"])
    book_core = {
        "schema": BOOK_SCHEMA,
        "candidate_id": candidate_id,
        "policy_type": "SINGLE_SLEEVE_COMPLETE_ACCOUNT_POLICY",
        "complete_account_policy": True,
        "sleeve_ids": [candidate_id],
        "sleeve_count": 1,
        "candidate_fingerprint": str(candidate["candidate_fingerprint"]),
        "behavioral_fingerprint": str(candidate["behavioral_fingerprint"]),
        "qd_cell": str(candidate["qd_cell"]),
        "account_label": account_label,
        "account_size_usd": _ACCOUNT_SIZES[account_label],
        "markets": list(candidate["markets"]),
        "market_inventory_hash": str(candidate["market_inventory_hash"]),
        "frozen_account_policy": dict(candidate["frozen_account_policy"]),
        "frozen_account_policy_hash": str(
            candidate["frozen_account_policy_hash"]
        ),
        "selected_horizon_trading_days": int(
            candidate["selected_horizon_trading_days"]
        ),
        "selected_cell_hash": str(candidate["selected_cell_hash"]),
        "account_policy_binding": (
            "EXACT_SELECTED_CELL_HASH_BINDS_CAUSAL_SLEEVE_INTEGER_QUANTITY_"
            "ACCOUNT_GOVERNOR_AND_RULE_SNAPSHOT"
        ),
        "source_candidate_control_result_hash": str(candidate["result_hash"]),
        "source_candidate_result_hash": str(final["source_candidate_result_hash"]),
        "source_event_receipt": dict(final["source_event_receipt"]),
        "immutable": True,
    }
    book = {**book_core, "combine_book_hash": stable_hash(book_core)}
    block = dict(gate["block_diversity_receipt"])
    mll = dict(gate["mll_receipt"])
    consistency = dict(gate["consistency_receipt"])
    evidence_core = {
        "candidate_id": candidate_id,
        "source_tier_g_control_composite_hash": str(source["result_hash"]),
        "source_candidate_control_result_hash": str(candidate["result_hash"]),
        "combine_book_hash": str(book["combine_book_hash"]),
        "market_inventory_hash": str(candidate["market_inventory_hash"]),
        "frozen_account_policy_hash": str(
            candidate["frozen_account_policy_hash"]
        ),
        "final_development_evidence_hash": str(
            final["final_development_evidence_hash"]
        ),
        "normal_episode_path_hash": str(
            dict(candidate["identity_best_parent"])["normal"]["episode_path_hash"]
        ),
        "stressed_episode_path_hash": str(
            dict(candidate["identity_best_parent"])["stressed"]["episode_path_hash"]
        ),
        "block_diversity_receipt_hash": str(block["receipt_hash"]),
        "mll_receipt_hash": str(mll["receipt_hash"]),
        "consistency_receipt_hash": str(consistency["receipt_hash"]),
        "concentration_receipt_hash": str(concentration_receipt["receipt_hash"]),
        "control_comparison_hash": str(final["control_comparison_hash"]),
        "evidence_role": EVIDENCE_ROLE,
    }
    graduation_evidence_hash = stable_hash(evidence_core)
    gate_results = {
        key: bool(value) for key, value in gate["checks"].items()
    }
    if not all(gate_results.values()):
        raise AutonomousTierGGraduationError(
            f"attempted to build an uncleared receipt: {candidate_id}"
        )
    identity = dict(candidate["identity_best_parent"])
    core: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "candidate_id": candidate_id,
        "evidence_tier": "G",
        "graduation_status": GRADUATION_STATUS,
        "promotion_status": "TIER_G",
        "tier_g_gate_cleared": True,
        "evidence_role": EVIDENCE_ROLE,
        "complete_account_policy": True,
        "policy_cardinality": "SINGLE_SLEEVE",
        "sleeve_count": 1,
        "sleeve_ids": [candidate_id],
        "candidate_fingerprint": str(candidate["candidate_fingerprint"]),
        "behavioral_fingerprint": str(candidate["behavioral_fingerprint"]),
        "qd_cell": str(candidate["qd_cell"]),
        "account_label": account_label,
        "account_size_usd": _ACCOUNT_SIZES[account_label],
        "markets": list(candidate["markets"]),
        "market_inventory_hash": str(candidate["market_inventory_hash"]),
        "frozen_account_policy_hash": str(
            candidate["frozen_account_policy_hash"]
        ),
        "selected_horizon_trading_days": int(
            candidate["selected_horizon_trading_days"]
        ),
        "selected_cell_hash": str(candidate["selected_cell_hash"]),
        "combine_book": book,
        "combine_book_hash": str(book["combine_book_hash"]),
        "source_tier_g_control_composite_hash": str(source["result_hash"]),
        "source_candidate_control_result_hash": str(candidate["result_hash"]),
        "source_candidate_bank_hash": str(source["source_candidate_bank_hash"]),
        "source_exact_composite_hash": str(source["source_exact_composite_hash"]),
        "source_exact_result_hash": str(candidate["source_exact_result_hash"]),
        "source_manifest_hash": str(source["source_manifest_hash"]),
        "final_development_evidence_hash": str(
            final["final_development_evidence_hash"]
        ),
        "graduation_evidence_hash": graduation_evidence_hash,
        "graduation_evidence_binding": evidence_core,
        "normal_economics": dict(identity["normal"]),
        "stressed_economics": dict(identity["stressed"]),
        "block_diversity_receipt": block,
        "mll_receipt": mll,
        "consistency_receipt": consistency,
        "concentration_receipt": concentration_receipt,
        "control_comparison": dict(candidate["control_comparison"]),
        "graduation_gate_results": gate_results,
        "frozen_before_confirmation": True,
        "frozen_before_xfa": True,
        "independent_confirmation_claimed": False,
        "confirmation_status": "NOT_EVALUATED",
        "forward_status": "NOT_STARTED",
        "xfa_status": "NOT_STARTED",
        "xfa_book_hash": None,
        "xfa_profile_hash": None,
        "authoritative_write_performed": False,
        "authoritative_promotion_writes": 0,
        "independent_confirmations": 0,
        "forward_packages": 0,
        "xfa_paths_started": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": "SEPARATE_FRESH_CONFIRMATION_REQUIRED_FOR_TIER_C",
    }
    return {**core, "result_hash": stable_hash(core)}


def _verify_episode_summary(value: Any, scenario: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AutonomousTierGGraduationError(
            f"{scenario} exact account summary is absent"
        )
    row = dict(value)
    episodes = _nonnegative_int(row.get("episode_count"), f"{scenario} episodes")
    passes = _nonnegative_int(row.get("pass_count"), f"{scenario} passes")
    breaches = _nonnegative_int(
        row.get("mll_breach_count"), f"{scenario} MLL breaches"
    )
    if episodes <= 0 or passes > episodes or breaches > episodes:
        raise AutonomousTierGGraduationError(
            f"{scenario} exact account counts are invalid"
        )
    _require_ratio(row.get("pass_rate"), passes, episodes, f"{scenario} pass rate")
    _require_ratio(
        row.get("mll_breach_rate"), breaches, episodes, f"{scenario} MLL rate"
    )
    consistency_rate = _finite(row.get("consistency_compliance_rate"), scenario)
    if not 0.0 <= consistency_rate <= 1.0:
        raise AutonomousTierGGraduationError(
            f"{scenario} consistency rate is invalid"
        )
    compliant = consistency_rate * episodes
    if not math.isclose(compliant, round(compliant), abs_tol=1e-9):
        raise AutonomousTierGGraduationError(
            f"{scenario} consistency count is non-integral"
        )
    for field in (
        "net_total_usd",
        "net_median_usd",
        "target_progress_p25",
        "target_progress_median",
        "minimum_mll_buffer_usd",
    ):
        _finite(row.get(field), f"{scenario}/{field}")
    if not _is_sha256(row.get("episode_path_hash")):
        raise AutonomousTierGGraduationError(
            f"{scenario} episode-path hash is absent"
        )
    terminal = row.get("terminal_distribution")
    if not isinstance(terminal, Mapping) or any(
        _nonnegative_int(value, f"{scenario} terminal count") < 0
        for value in terminal.values()
    ):
        raise AutonomousTierGGraduationError(
            f"{scenario} terminal distribution is invalid"
        )
    if (
        sum(int(value) for value in terminal.values()) != episodes
        or int(terminal.get("PASSED", 0)) != passes
    ):
        raise AutonomousTierGGraduationError(
            f"{scenario} terminal distribution does not reconcile"
        )
    blocks = row.get("by_block")
    if not isinstance(blocks, Mapping) or not blocks:
        raise AutonomousTierGGraduationError(
            f"{scenario} temporal-block evidence is absent"
        )
    block_episodes = block_passes = block_breaches = 0
    for block_id, raw in blocks.items():
        if not str(block_id) or not isinstance(raw, Mapping):
            raise AutonomousTierGGraduationError(
                f"{scenario} temporal-block evidence is invalid"
            )
        block = dict(raw)
        count = _nonnegative_int(block.get("episode_count"), "block episodes")
        block_pass = _nonnegative_int(block.get("pass_count"), "block passes")
        block_breach = _nonnegative_int(
            block.get("mll_breach_count"), "block breaches"
        )
        if block_pass > count or block_breach > count:
            raise AutonomousTierGGraduationError(
                f"{scenario} temporal-block counts are invalid"
            )
        _finite(block.get("net_total_usd"), "block net")
        _finite(block.get("target_progress_median"), "block target progress")
        block_episodes += count
        block_passes += block_pass
        block_breaches += block_breach
    if (block_episodes, block_passes, block_breaches) != (
        episodes,
        passes,
        breaches,
    ):
        raise AutonomousTierGGraduationError(
            f"{scenario} block summaries do not reconcile"
        )
    for field in (
        "requested_quantity_total",
        "admitted_quantity_total",
        "size_reduced_count",
        "risk_or_contract_rejection_count",
    ):
        _nonnegative_int(row.get(field), f"{scenario}/{field}")
    if row.get("silent_size_reduction") is not False:
        raise AutonomousTierGGraduationError(
            f"{scenario} contains a silent size reduction"
        )
    return row


def _verify_concentration(value: Any, candidate_id: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AutonomousTierGGraduationError(
            f"unique-ledger concentration is absent: {candidate_id}"
        )
    row = dict(value)
    claimed = str(row.pop("concentration_hash", ""))
    if not _is_sha256(claimed) or stable_hash(row) != claimed:
        raise AutonomousTierGGraduationError(
            f"unique-ledger concentration hash drift: {candidate_id}"
        )
    if (
        value.get("denominator") != "UNIQUE_COMPLETED_CAUSAL_TRAJECTORY_LEDGER"
        or value.get("event_trade_mapping")
        != "ONE_COMPLETED_TRAJECTORY_PER_EVENT"
        or int(value.get("rolling_start_multiplicity", -1)) != 0
        or float(value.get("maximum_allowed_profit_share", -1.0))
        != MAXIMUM_CONCENTRATION_SHARE
    ):
        raise AutonomousTierGGraduationError(
            f"unique-ledger denominator drift: {candidate_id}"
        )
    scenarios: dict[str, Mapping[str, Any]] = {}
    for scenario in ("normal", "stressed"):
        raw = value.get(scenario)
        if not isinstance(raw, Mapping):
            raise AutonomousTierGGraduationError(
                f"unique-ledger scenario is absent: {candidate_id}/{scenario}"
            )
        for field in ("ledger_hash", "event_inventory_hash"):
            if not _is_sha256(raw.get(field)):
                raise AutonomousTierGGraduationError(
                    f"unique-ledger hash is absent: {candidate_id}/{scenario}/{field}"
                )
        for field in (
            "maximum_single_day_profit_share",
            "maximum_single_trade_profit_share",
            "maximum_single_event_profit_share",
            "maximum_single_trade_loss_share",
            "maximum_single_day_loss_share",
        ):
            share = _finite(raw.get(field), f"concentration/{scenario}/{field}")
            if not 0.0 <= share <= 1.0:
                raise AutonomousTierGGraduationError(
                    f"unique-ledger share is invalid: {candidate_id}/{field}"
                )
        scenarios[scenario] = raw
    expected_maximums = {
        field: max(float(scenarios[name][field]) for name in scenarios)
        for field in (
            "maximum_single_day_profit_share",
            "maximum_single_trade_profit_share",
            "maximum_single_event_profit_share",
        )
    }
    observed = dict(value.get("worst_case_maximums") or {})
    if any(
        not _float_equal(observed.get(field), expected)
        for field, expected in expected_maximums.items()
    ):
        raise AutonomousTierGGraduationError(
            f"unique-ledger worst-case concentration drift: {candidate_id}"
        )
    cleared = all(
        value <= MAXIMUM_CONCENTRATION_SHARE
        for value in expected_maximums.values()
    )
    if value.get("cleared") is not cleared:
        raise AutonomousTierGGraduationError(
            f"unique-ledger concentration decision drift: {candidate_id}"
        )
    return dict(value)


def _verify_synthetic_controls(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) != CONTROL_SHIFT_COUNT:
        raise AutonomousTierGGraduationError(
            "synthetic control inventory is incomplete"
        )
    output: list[dict[str, Any]] = []
    ids: set[str] = set()
    offsets: set[int] = set()
    for raw in value:
        if not isinstance(raw, Mapping):
            raise AutonomousTierGGraduationError("synthetic control is not an object")
        row = dict(raw)
        claimed = str(row.pop("control_hash", ""))
        if not _is_sha256(claimed) or stable_hash(row) != claimed:
            raise AutonomousTierGGraduationError("synthetic control hash drift")
        control_id = str(raw.get("control_id") or "")
        offset = _nonnegative_int(raw.get("offset"), "synthetic control offset")
        if (
            not control_id
            or control_id in ids
            or offset <= 0
            or offset in offsets
            or raw.get("control_type")
            != "CIRCULAR_OUTCOME_PATH_SHIFT_SYNTHETIC_NULL"
            or raw.get("synthetic_non_deployable") is not True
            or raw.get("may_promote_candidate") is not False
            or raw.get("exposure_count_matched") is not True
            or _nonnegative_int(raw.get("event_count"), "control event count") <= 0
            or not _is_sha256(raw.get("normal_control_ledger_hash"))
            or not _is_sha256(raw.get("stressed_control_ledger_hash"))
        ):
            raise AutonomousTierGGraduationError(
                "synthetic control identity/exposure contract drift"
            )
        _verify_episode_summary(raw.get("normal"), f"{control_id}/NORMAL")
        _verify_episode_summary(raw.get("stressed"), f"{control_id}/STRESSED")
        ids.add(control_id)
        offsets.add(offset)
        output.append(dict(raw))
    return output


def _verify_control_comparison(
    value: Any,
    identity: Mapping[str, Any],
    controls: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AutonomousTierGGraduationError("control comparison is absent")
    row = dict(value)
    observed = dict(identity["stressed"])
    median_passes = float(
        statistics.median(int(dict(item["stressed"])["pass_count"]) for item in controls)
    )
    median_net = float(
        statistics.median(float(dict(item["stressed"])["net_total_usd"]) for item in controls)
    )
    expected = {
        "observed_stressed_pass_count": int(observed["pass_count"]),
        "median_synthetic_stressed_pass_count": median_passes,
        "observed_stressed_net_usd": float(observed["net_total_usd"]),
        "median_synthetic_stressed_net_usd": median_net,
        "pass_count_not_worse_than_median_synthetic": int(observed["pass_count"])
        >= median_passes,
        "stressed_net_not_worse_than_median_synthetic": float(
            observed["net_total_usd"]
        )
        >= median_net,
        "interpretation": (
            "SYNTHETIC_TEMPORAL_ALIGNMENT_CONTROL_ONLY_NOT_ALPHA_CONFIRMATION"
        ),
    }
    if row != expected:
        raise AutonomousTierGGraduationError(
            "synthetic-control comparison does not reconcile"
        )
    return row


def _verify_final_development_evidence(
    value: Any,
    *,
    candidate: Mapping[str, Any],
    identity: Mapping[str, Any],
    concentration: Mapping[str, Any],
    controls: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AutonomousTierGGraduationError(
            "final-development evidence is absent"
        )
    row = dict(value)
    claimed = str(row.pop("final_development_evidence_hash", ""))
    if not _is_sha256(claimed) or stable_hash(row) != claimed:
        raise AutonomousTierGGraduationError(
            "final-development evidence hash drift"
        )
    if (
        value.get("schema") != "hydra_tier_g_final_development_evidence_v1"
        or str(value.get("candidate_id")) != str(candidate["candidate_id"])
        or str(value.get("candidate_fingerprint"))
        != str(candidate["candidate_fingerprint"])
        or str(value.get("behavioral_fingerprint"))
        != str(candidate["behavioral_fingerprint"])
        or str(value.get("selected_cell_hash"))
        != str(candidate["selected_cell_hash"])
        or str(value.get("account_label")) != str(candidate["account_label"])
        or int(value.get("account_size_usd", -1))
        != int(candidate["account_size_usd"])
        or str(value.get("market_inventory_hash"))
        != str(candidate["market_inventory_hash"])
        or str(value.get("frozen_account_policy_hash"))
        != str(candidate["frozen_account_policy_hash"])
        or str(value.get("policy_sleeve_inventory_hash"))
        != stable_hash(candidate["policy_sleeve_ids"])
        or value.get("evidence_role") != EVIDENCE_ROLE
        or value.get("independent_confirmation_claimed") is not False
        or str(value.get("normal_unique_ledger_hash"))
        != str(dict(concentration["normal"])["ledger_hash"])
        or str(value.get("stressed_unique_ledger_hash"))
        != str(dict(concentration["stressed"])["ledger_hash"])
        or str(value.get("identity_normal_episode_path_hash"))
        != str(dict(identity["normal"])["episode_path_hash"])
        or str(value.get("identity_stressed_episode_path_hash"))
        != str(dict(identity["stressed"])["episode_path_hash"])
        or str(value.get("concentration_hash"))
        != str(concentration["concentration_hash"])
        or list(value.get("control_hashes") or ())
        != [str(item["control_hash"]) for item in controls]
        or str(value.get("control_comparison_hash")) != stable_hash(comparison)
    ):
        raise AutonomousTierGGraduationError(
            "final-development evidence provenance does not reconcile"
        )
    for field in (
        "source_exact_result_hash",
        "source_manifest_hash",
        "frozen_grid_hash",
        "official_rule_snapshot_hash",
        "source_candidate_result_hash",
        "source_block_concentration_hash",
    ):
        if not _is_sha256(value.get(field)):
            raise AutonomousTierGGraduationError(
                f"final-development provenance hash is absent: {field}"
            )
    event = value.get("source_event_receipt")
    if (
        not isinstance(event, Mapping)
        or not str(event.get("relative_path") or "")
        or _nonnegative_int(event.get("record_count"), "event record count") <= 0
        or not _is_sha256(event.get("sha256"))
        or not _is_sha256(event.get("uncompressed_sha256"))
    ):
        raise AutonomousTierGGraduationError(
            "final-development source-event receipt is incomplete"
        )
    thresholds = dict(value.get("frozen_gate_thresholds") or {})
    if thresholds != {
        "maximum_day_trade_event_profit_share": MAXIMUM_CONCENTRATION_SHARE,
        "maximum_block_pass_share": MAXIMUM_BLOCK_PASS_SHARE,
        "minimum_normal_passes": MINIMUM_PASSES_PER_SCENARIO,
        "minimum_stressed_passes": MINIMUM_PASSES_PER_SCENARIO,
        "minimum_temporal_contexts": MINIMUM_PASS_CONTEXTS,
        "synthetic_control_comparison": "NOT_WORSE_THAN_MEDIAN",
    }:
        raise AutonomousTierGGraduationError(
            "final-development gate thresholds drift"
        )
    return dict(value)


def _verify_concentration_receipt(
    value: Any,
    *,
    candidate_id: str,
    source_exact_result_hash: str,
    concentration: Mapping[str, Any],
    final_development_evidence_hash: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AutonomousTierGGraduationError(
            "candidate concentration receipt is absent"
        )
    row = dict(value)
    claimed = str(row.pop("receipt_hash", ""))
    maximums = dict(concentration["worst_case_maximums"])
    if (
        not _is_sha256(claimed)
        or stable_hash(row) != claimed
        or value.get("schema") != "hydra_candidate_concentration_receipt_v1"
        or str(value.get("candidate_id")) != candidate_id
        or str(value.get("source_exact_result_hash")) != source_exact_result_hash
        or value.get("unique_ledger_denominator") is not True
        or int(value.get("rolling_start_multiplicity", -1)) != 0
        or float(value.get("maximum_allowed_share", -1.0))
        != MAXIMUM_CONCENTRATION_SHARE
        or str(value.get("final_development_evidence_hash"))
        != final_development_evidence_hash
        or value.get("evidence_role") != EVIDENCE_ROLE
        or any(
            not _float_equal(value.get(field), maximums[field])
            for field in (
                "maximum_single_day_profit_share",
                "maximum_single_trade_profit_share",
                "maximum_single_event_profit_share",
            )
        )
    ):
        raise AutonomousTierGGraduationError(
            "candidate concentration receipt does not reconcile"
        )
    return dict(value)


def _block_diversity_receipt(
    candidate_id: str,
    normal: Mapping[str, Any],
    stressed: Mapping[str, Any],
) -> dict[str, Any]:
    normal_blocks = dict(normal["by_block"])
    stressed_blocks = dict(stressed["by_block"])
    block_ids = sorted(set(normal_blocks) | set(stressed_blocks))
    if set(normal_blocks) != set(stressed_blocks):
        raise AutonomousTierGGraduationError(
            f"normal/stressed temporal-block inventories differ: {candidate_id}"
        )
    paired = {
        block_id: min(
            int(dict(normal_blocks[block_id])["pass_count"]),
            int(dict(stressed_blocks[block_id])["pass_count"]),
        )
        for block_id in block_ids
    }
    total = sum(paired.values())
    contexts = [block_id for block_id in block_ids if paired[block_id] > 0]
    maximum_share = max(paired.values(), default=0) / total if total else 1.0
    core = {
        "schema": BLOCK_RECEIPT_SCHEMA,
        "candidate_id": candidate_id,
        "block_ids": block_ids,
        "normal_passes_by_block": {
            block_id: int(dict(normal_blocks[block_id])["pass_count"])
            for block_id in block_ids
        },
        "stressed_passes_by_block": {
            block_id: int(dict(stressed_blocks[block_id])["pass_count"])
            for block_id in block_ids
        },
        "paired_passes_by_block": paired,
        "paired_pass_count": total,
        "positive_paired_pass_contexts": contexts,
        "positive_paired_pass_context_count": len(contexts),
        "maximum_paired_block_pass_share": float(maximum_share),
        "minimum_context_count": MINIMUM_PASS_CONTEXTS,
        "maximum_allowed_block_pass_share": MAXIMUM_BLOCK_PASS_SHARE,
        "cleared": len(contexts) >= MINIMUM_PASS_CONTEXTS
        and maximum_share <= MAXIMUM_BLOCK_PASS_SHARE,
    }
    return {**core, "receipt_hash": stable_hash(core)}


def _mll_receipt(
    candidate_id: str,
    normal: Mapping[str, Any],
    stressed: Mapping[str, Any],
) -> dict[str, Any]:
    core = {
        "schema": MLL_RECEIPT_SCHEMA,
        "candidate_id": candidate_id,
        "maximum_allowed_breach_rate": MAXIMUM_MLL_BREACH_RATE,
        "normal": {
            "episode_count": int(normal["episode_count"]),
            "breach_count": int(normal["mll_breach_count"]),
            "breach_rate": float(normal["mll_breach_rate"]),
            "minimum_buffer_usd": float(normal["minimum_mll_buffer_usd"]),
        },
        "stressed": {
            "episode_count": int(stressed["episode_count"]),
            "breach_count": int(stressed["mll_breach_count"]),
            "breach_rate": float(stressed["mll_breach_rate"]),
            "minimum_buffer_usd": float(stressed["minimum_mll_buffer_usd"]),
        },
        "cleared": float(normal["mll_breach_rate"])
        <= MAXIMUM_MLL_BREACH_RATE
        and float(stressed["mll_breach_rate"]) <= MAXIMUM_MLL_BREACH_RATE,
    }
    return {**core, "receipt_hash": stable_hash(core)}


def _consistency_receipt(
    candidate_id: str,
    normal: Mapping[str, Any],
    stressed: Mapping[str, Any],
) -> dict[str, Any]:
    def scenario(row: Mapping[str, Any]) -> dict[str, Any]:
        episodes = int(row["episode_count"])
        rate = float(row["consistency_compliance_rate"])
        compliant = int(round(rate * episodes))
        passes = int(row["pass_count"])
        return {
            "episode_count": episodes,
            "pass_count": passes,
            "compliance_rate": rate,
            "compliant_episode_count": compliant,
            "all_passes_covered_by_compliant_episode_count": compliant >= passes,
        }

    normal_row = scenario(normal)
    stressed_row = scenario(stressed)
    core = {
        "schema": CONSISTENCY_RECEIPT_SCHEMA,
        "candidate_id": candidate_id,
        "normal": normal_row,
        "stressed": stressed_row,
        "cleared": bool(
            normal_row["all_passes_covered_by_compliant_episode_count"]
            and stressed_row["all_passes_covered_by_compliant_episode_count"]
        ),
    }
    return {**core, "receipt_hash": stable_hash(core)}


def _verify_self_hashed_nested(
    value: Any,
    *,
    hash_field: str,
    schema: str,
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AutonomousTierGGraduationError(f"{label} is absent")
    row = dict(value)
    claimed = str(row.pop(hash_field, ""))
    if (
        not _is_sha256(claimed)
        or stable_hash(row) != claimed
        or value.get("schema") != schema
    ):
        raise AutonomousTierGGraduationError(f"{label} identity/hash drift")
    return dict(value)


def _nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise AutonomousTierGGraduationError(f"{label} is not an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise AutonomousTierGGraduationError(f"{label} is not an integer") from exc
    if parsed < 0 or isinstance(value, float) and not value.is_integer():
        raise AutonomousTierGGraduationError(f"{label} is invalid")
    return parsed


def _finite(value: Any, label: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise AutonomousTierGGraduationError(f"{label} is not numeric") from exc
    if not math.isfinite(parsed):
        raise AutonomousTierGGraduationError(f"{label} is not finite")
    return parsed


def _require_ratio(value: Any, numerator: int, denominator: int, label: str) -> None:
    observed = _finite(value, label)
    expected = numerator / denominator
    if not math.isclose(observed, expected, rel_tol=0.0, abs_tol=1e-12):
        raise AutonomousTierGGraduationError(f"{label} does not reconcile")


def _float_equal(value: Any, expected: float) -> bool:
    try:
        observed = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(observed) and math.isclose(
        observed, float(expected), rel_tol=0.0, abs_tol=1e-12
    )


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


__all__ = [
    "AutonomousTierGGraduationError",
    "BOOK_SCHEMA",
    "GRADUATION_STATUS",
    "MAXIMUM_MLL_BREACH_RATE",
    "RECEIPT_SCHEMA",
    "SCHEMA",
    "build_graduated_development_books",
    "verify_graduated_development_book_receipt",
    "verify_tier_g_development_graduation",
]
