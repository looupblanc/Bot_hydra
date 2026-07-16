"""Build the immutable HYDRA Operating Package V1.

The package is deliberately a research/forward-observation manifest.  It binds
already-frozen complete account books; it cannot resize sleeves, combine books,
connect a broker, or make Standard and Consistency XFA paths additive.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.production.active_risk_report_seal import (
    verify_active_risk_decision_report_seal,
)
from hydra.production.frozen_book_selection import (
    verify_frozen_book_selection_seal,
)
from hydra.shadow.active_risk_binding_loader import (
    verify_active_risk_shadow_export,
)
from hydra.shadow.active_risk_forward_boundary import (
    validate_active_risk_forward_boundary,
)
from hydra.shadow.forward_feed_manifest import (
    validate_forward_boundary_manifest,
)


SCHEMA = "hydra_operating_package_v1"
ROLE_RULE_VERSION = "hydra_operating_role_rule_v1"
SEAL_RECEIPT_SCHEMA = "hydra_operating_package_v1_seal_receipt"
PRIMARY_IDS = (
    "active_pool_070e391c7586ba1fac2f5494",
    "active_pool_186a4177401aab223b0a21fa",
    "active_pool_14e275fa8d869c28b1f27f78",
    "active_pool_2287bfb0b1c6f07930150102",
    "active_pool_2377af7025aadf9aaf456a7e",
)
BACKUP_ID = "active_pool_014dffb40e99814612d78c51"
ALL_BOOK_IDS = (*PRIMARY_IDS, BACKUP_ID)
ROLE_IDS = {
    "CORE_BOOK": "active_pool_186a4177401aab223b0a21fa",
    "SAFETY_BOOK": "active_pool_14e275fa8d869c28b1f27f78",
    "DIVERSIFIER_BOOK": "active_pool_2287bfb0b1c6f07930150102",
    "BACKUP_BOOK": BACKUP_ID,
}


class OperatingPackageError(RuntimeError):
    pass


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=str
        ).encode("utf-8")
    ).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_operating_package_v1(
    *,
    project_root: str | Path,
    selection_path: str | Path,
    decision_report_path: str | Path,
    forward_root: str | Path,
    redundancy_audit: Mapping[str, Any],
    post_payout_frontier: Mapping[str, Any],
    forward_boundary: Mapping[str, Any],
    source_commit: str,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    selection_file = _resolve(root, selection_path)
    decision_file = _resolve(root, decision_report_path)
    shadow_root = _resolve(root, forward_root)
    selection = _json(selection_file)
    report = _json(decision_file)
    report_seal = verify_active_risk_decision_report_seal(decision_file.parent)
    selection_seal = verify_frozen_book_selection_seal(
        selection_file.parent,
        report_dir=decision_file.parent,
    )
    _verify_selection(selection)
    _validate_redundancy_audit(redundancy_audit)
    rows = {
        str(row["policy_id"]): row
        for row in report["expanded_development_finalists"]["rows"]
    }
    if not set(ALL_BOOK_IDS).issubset(rows):
        raise OperatingPackageError("Decision report lacks a retained book.")
    selected = {
        str(row["policy_id"]): row for row in selection["selected_books"]
    }
    if tuple(selected) != ALL_BOOK_IDS:
        raise OperatingPackageError("Frozen selection identity/order changed.")

    frontier_books = {
        str(row["policy_id"]): row
        for row in post_payout_frontier.get("books", ())
    }
    _validate_post_payout_frontier(post_payout_frontier)
    if set(frontier_books) != set(ALL_BOOK_IDS):
        raise OperatingPackageError("Post-payout frontier is incomplete.")

    book_rows: list[dict[str, Any]] = []
    canonical_membership: list[dict[str, Any]] | None = None
    canonical_membership_sha: str | None = None
    package_shas: set[str] = set()
    for policy_id in ALL_BOOK_IDS:
        selection_row = selected[policy_id]
        package_path = shadow_root / policy_id / "shadow_package.json"
        package = _json(package_path)
        export_receipt = verify_active_risk_shadow_export(package_path.parent)
        _verify_shadow_package(package, policy_id)
        package_sha = sha256_file(package_path)
        package_shas.add(package_sha)
        membership = list(
            selection_row["frozen_policy_specification"]["membership"]
        )
        membership_sha = str(selection_row["membership_sha256"])
        if canonical_membership is None:
            canonical_membership = membership
            canonical_membership_sha = membership_sha
        elif membership_sha != canonical_membership_sha or membership != canonical_membership:
            raise OperatingPackageError(
                "Retained books unexpectedly differ in frozen sleeve inventory."
            )
        decision = rows[policy_id]
        xfa = decision["expanded_standard_consistency_xfa_lifecycle_exact"]
        standard = xfa["stressed"]["standard"]
        consistency = xfa["stressed"]["consistency"]
        standard_ev = float(
            standard["unconditional_lower_bound"][
                "expected_trader_payout_per_combine_attempt"
            ]
        )
        consistency_ev = float(
            consistency["unconditional_lower_bound"][
                "expected_trader_payout_per_combine_attempt"
            ]
        )
        chosen_path = (
            "CONSISTENCY" if consistency_ev > standard_ev else "STANDARD"
        )
        selected_profile = frontier_books[policy_id].get("selected_profile")
        if not isinstance(selected_profile, Mapping):
            raise OperatingPackageError(
                f"Post-payout profile missing for {policy_id}."
            )
        active = selection_row["frozen_policy_specification"]["active_risk_policy"]
        _cross_check_shadow_and_selection(
            package=package,
            selection_row=selection_row,
            export_receipt=export_receipt,
        )
        expected_profile_path = f"XFA_{chosen_path}"
        if (
            str(selected_profile.get("book_id") or "") != policy_id
            or str(selected_profile.get("path") or "") != expected_profile_path
            or str(selected_profile.get("selected_xfa_path") or "")
            != expected_profile_path
        ):
            raise OperatingPackageError(
                f"Post-payout profile/path binding drift: {policy_id}."
            )
        selected_profile_payload = dict(selected_profile)
        selected_profile_payload["derived_per_new_combine_attempt"] = {
            scenario: _post_payout_per_attempt(
                selected_profile["summary"][profile_scenario],
                combine_pass_rate=float(decision[decision_scenario]["pass_rate"]),
                combine_denominator=int(decision[decision_scenario]["episode_count"]),
            )
            for scenario, profile_scenario, decision_scenario in (
                ("normal", "normal", "normal"),
                ("stressed_1_5x", "stressed_1_5x", "stressed"),
            )
        }
        book_rows.append(
            {
                "policy_id": policy_id,
                "selection_role": selection_row["selection_role"],
                "operating_role": next(
                    (name for name, value in ROLE_IDS.items() if value == policy_id),
                    "RESERVE_ALTERNATIVE_BOOK",
                ),
                "economic_behavior_cluster": selection_row[
                    "expanded_economic_behavior_cluster"
                ],
                "candidate_specification_hash": package[
                    "candidate_specification_hash"
                ],
                "shadow_package": {
                    "path": str(package_path.relative_to(root)),
                    "file_sha256": package_sha,
                    "package_hash": package["package_hash"],
                    "freeze_timestamp_utc": package["freeze_timestamp_utc"],
                    "role": package["role"],
                },
                "book_hashes": {
                    "active_risk_policy_sha256": selection_row[
                        "active_risk_policy_sha256"
                    ],
                    "combine_book_sha256": selection_row["combine_book_sha256"],
                    "xfa_standard_book_sha256": selection_row[
                        "xfa_standard_book_sha256"
                    ],
                    "xfa_consistency_book_sha256": selection_row[
                        "xfa_consistency_book_sha256"
                    ],
                    "membership_sha256": membership_sha,
                },
                "combine_account_profile": {
                    "profit_target_usd": 9000.0,
                    "maximum_loss_limit_usd": 4500.0,
                    "maximum_mini_equivalent": 15,
                    "governor_maximum_mini_equivalent": float(
                        active["maximum_mini_equivalent"]
                    ),
                    "maximum_concurrent_sleeves": int(
                        active["maximum_concurrent_sleeves"]
                    ),
                    "aggregate_open_risk_ceiling_usd": float(
                        active["aggregate_open_risk_ceiling"]
                    ),
                    "protected_mll_buffer_usd": float(
                        active["protected_mll_buffer"]
                    ),
                    "daily_consistency_profit_guard_usd": float(
                        active["daily_consistency_profit_guard"]
                    ),
                    "same_instrument_conflict_rule": active[
                        "same_instrument_conflict_rule"
                    ],
                    "static_risk_tier": float(active["static_risk_tier"]),
                },
                "combine_evidence": {
                    "normal": _combine_metrics(decision["normal"]),
                    "stressed_1_5x": _combine_metrics(decision["stressed"]),
                    "independent_confirmation": False,
                },
                "xfa_path_comparison_stressed": {
                    "standard": _xfa_metrics(standard),
                    "consistency": _xfa_metrics(consistency),
                    "paths_are_alternative_not_additive": True,
                },
                "selected_xfa_path": chosen_path,
                "selected_post_payout_profile": selected_profile_payload,
                "outbound_order_capability": False,
                "broker_connectivity": False,
            }
        )

    role_reasons = {
        "CORE_BOOK": (
            "Highest stressed book net and highest corrected stressed "
            "Consistency EV per new Combine attempt; positive evidence in B1/B3/B4."
        ),
        "SAFETY_BOOK": (
            "Strongest observed minimum MLL buffer among primaries, positive stressed "
            "economics, acceptable passes, and lower exact-trade overlap with Core."
        ),
        "DIVERSIFIER_BOOK": (
            "Most distinct surviving primary governor subject to positive stressed "
            "economics and observed Consistency post-payout survival; diversification "
            "is routing-relative, not independent alpha."
        ),
        "BACKUP_BOOK": (
            "Frozen backup from a distinct economic-behavior cluster and lowest overall "
            "trajectory overlap; weaker economics are retained explicitly."
        ),
    }
    observed = _utc(created_at or datetime.now(timezone.utc)).isoformat()
    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "package_version": 1,
        "created_at_utc": observed,
        "source_commit": str(source_commit),
        "research_only": True,
        "development_selected": True,
        "independently_confirmed": False,
        "paper_shadow_ready": False,
        "broker_connections": 0,
        "outbound_orders": 0,
        "outbound_order_capability": False,
        "q4_access_authorized": False,
        "book_semantics": {
            "complete_books_are_alternatives": True,
            "complete_books_may_be_stacked_on_one_account": False,
            "eventual_account_uses_exactly_one_frozen_book": True,
            "new_simulation_required_before_any_book_merge": True,
        },
        "role_rule": {
            "version": ROLE_RULE_VERSION,
            "outcome_fields_from_forward_used": False,
            "core": "MAX_STRESSED_COMBINE_ECONOMICS_THEN_XFA_EV_WITH_POSITIVE_BLOCK_DIVERSITY",
            "safety": "MAX_MINIMUM_MLL_BUFFER_WITH_ACCEPTABLE_PASS_RATE_AND_NONCLONE_GOVERNOR",
            "diversifier": "MIN_BEHAVIOR_OVERLAP_SUBJECT_TO_POSITIVE_STRESS_AND_POST_PAYOUT_UTILITY",
            "backup": "PREVIOUSLY_FROZEN_BACKUP_FROM_DISTINCT_ECONOMIC_CLUSTER",
            "assignments": [
                {
                    "role": role,
                    "policy_id": policy_id,
                    "reason": role_reasons[role],
                }
                for role, policy_id in ROLE_IDS.items()
            ],
        },
        "retained_book_count": len(book_rows),
        "books": book_rows,
        "canonical_sleeve_inventory": {
            "sleeve_count": len(canonical_membership or ()),
            "membership_sha256": canonical_membership_sha,
            "shared_by_all_six_books": True,
            "members": canonical_membership,
        },
        "redundancy_audit": dict(redundancy_audit),
        "post_payout_frontier": {
            "result_hash": post_payout_frontier.get("result_hash"),
            "source_event_tape_sha256": post_payout_frontier.get(
                "source_event_tape_sha256"
            ),
            "profile_families": [
                "XFA_HARVEST_PROFILE",
                "XFA_BALANCED_PROFILE",
                "XFA_LONGEVITY_PROFILE",
            ],
            "selected_profiles_frozen_before_forward": True,
        },
        "data_acquisition_authorization": {
            "authorization_basis": (
                "DIRECT_USER_DIRECTIVE_HYDRA_OPERATING_PACKAGE_V1_2026_07_16"
            ),
            "scope": "STRICTLY_POST_FREEZE_APPEND_ONLY_REQUIRED_INSTRUMENTS_ONLY",
            "maximum_total_incremental_spend_usd": 10.0,
            "minimum_remaining_budget_reserve_usd": 25.0,
            "initial_incremental_spend_usd": float(
                (forward_boundary.get("latest_update") or {}).get(
                    "incremental_spend_usd", 0.0
                )
            ),
            "broad_historical_purchase_authorized": False,
            "q4_access_authorized": False,
            "broker_or_order_authorized": False,
            "active_shadow_packages_remain_data_purchase_incapable": True,
            "acquisition_is_external_to_book_execution_boundary": True,
        },
        "forward_data_binding": dict(forward_boundary),
        "forward_gates": {
            "checkpoint_a": {
                "minimum_sessions": 10,
                "minimum_effective_trades_alternative": 20,
                "requirements": [
                    "SIGNAL_FIDELITY",
                    "CONSERVATIVE_FILL_REALISM",
                    "NO_HARD_RULE_VIOLATION",
                    "NO_DATA_OR_IMPLEMENTATION_DRIFT",
                ],
            },
            "checkpoint_b": {
                "minimum_sessions": 20,
                "approximate_effective_trades": 40,
                "requirements": [
                    "POSITIVE_NET_AFTER_FROZEN_COSTS",
                    "NO_MLL_BREACH",
                    "ACCEPTABLE_CONSISTENCY",
                    "NO_DAY_OR_TRADE_DOMINATION",
                    "MORE_THAN_ONE_OBSERVABLE_REGIME",
                ],
            },
            "checkpoint_c": {
                "policy": "EXISTING_PROMOTION_POLICY_UNCHANGED",
                "launch_count": 1,
                "backup_count": 1,
                "threshold_reduction_after_observation": False,
            },
            "failed_book_action": "FREEZE_OR_RETIRE_WITHOUT_RETUNING_ON_OWN_FORWARD_DATA",
        },
        "designations": {
            "COMBINE_LAUNCH_CANDIDATE": ROLE_IDS["CORE_BOOK"],
            "XFA_LAUNCH_CANDIDATE": ROLE_IDS["CORE_BOOK"],
            "FORWARD_BACKUP": ROLE_IDS["BACKUP_BOOK"],
            "status": "RESEARCH_SELECTIONS_PENDING_APPEND_ONLY_FORWARD_GATES",
        },
        "b2_specialist_lane": {
            "maximum_compute_fraction": 0.10,
            "frozen_books_mutable": False,
            "status": "RESERVED_NO_SPECIALIST_ADMITTED_NOT_BLOCKING_FORWARD",
            "admission_requires_positive_b2_low_overlap_and_new_book_simulation": True,
            "current_specialist_count": 0,
            "broad_discovery_authorized": False,
        },
        "source_artifacts": {
            "selection": {
                "path": str(selection_file.relative_to(root)),
                "sha256": sha256_file(selection_file),
                "selection_manifest_hash": selection["selection_manifest_hash"],
            },
            "decision_report": {
                "path": str(decision_file.relative_to(root)),
                "sha256": sha256_file(decision_file),
                "report_hash": report["report_hash"],
                "seal_receipt_hash": report_seal["receipt_hash"],
            },
            "selection_seal_receipt_hash": selection_seal["receipt_hash"],
            "post_payout_frontier": {
                "path": (
                    "reports/operating/hydra_operating_package_v1/"
                    "xfa_post_payout_frontier.json"
                ),
                "result_hash": post_payout_frontier["result_hash"],
            },
            "unique_shadow_package_file_sha256_count": len(package_shas),
        },
        "current_evidence_status": "FORWARD_SHADOW_CANDIDATE_WAITING_FOR_GENUINE_POST_FREEZE_BARS",
        "next_promotion_requirement": "PASS_FORWARD_CHECKPOINT_A_WITHOUT_IMPLEMENTATION_DRIFT",
    }
    payload["manifest_hash"] = stable_hash(payload)
    validate_operating_package_v1(payload)
    _validate_bound_sources(payload, project_root=root)
    return payload


def validate_operating_package_v1(value: Mapping[str, Any]) -> None:
    payload = dict(value)
    claimed = str(payload.pop("manifest_hash", ""))
    if not claimed or stable_hash(payload) != claimed:
        raise OperatingPackageError("Operating-package hash mismatch.")
    if payload.get("schema") != SCHEMA or int(payload.get("package_version", 0)) != 1:
        raise OperatingPackageError("Unsupported operating-package contract.")
    if payload.get("research_only") is not True or payload.get(
        "development_selected"
    ) is not True:
        raise OperatingPackageError("Operating package may only contain research selections.")
    if payload.get("independently_confirmed") is not False or payload.get(
        "paper_shadow_ready"
    ) is not False:
        raise OperatingPackageError("Development evidence inherited a validation status.")
    if any(int(payload.get(key, -1)) != 0 for key in ("broker_connections", "outbound_orders")):
        raise OperatingPackageError("Operating package exposes broker/order state.")
    if payload.get("outbound_order_capability") is not False:
        raise OperatingPackageError("Operating package cannot submit orders.")
    if payload.get("q4_access_authorized") is not False:
        raise OperatingPackageError("Operating package cannot access Q4.")
    semantics = payload.get("book_semantics") or {}
    if semantics.get("complete_books_may_be_stacked_on_one_account") is not False:
        raise OperatingPackageError("Complete book stacking is prohibited.")
    if semantics != {
        "complete_books_are_alternatives": True,
        "complete_books_may_be_stacked_on_one_account": False,
        "eventual_account_uses_exactly_one_frozen_book": True,
        "new_simulation_required_before_any_book_merge": True,
    }:
        raise OperatingPackageError("Complete-book operating semantics drifted.")
    books = list(payload.get("books") or ())
    identifiers = [str(row.get("policy_id") or "") for row in books]
    if tuple(identifiers) != ALL_BOOK_IDS or len(set(identifiers)) != 6:
        raise OperatingPackageError("Retained book identity/order drifted.")
    for row in books:
        if row.get("selected_xfa_path") not in {"STANDARD", "CONSISTENCY"}:
            raise OperatingPackageError("Exactly one XFA alternative must be selected.")
        if row.get("outbound_order_capability") is not False or row.get(
            "broker_connectivity"
        ) is not False:
            raise OperatingPackageError("A retained book gained execution capability.")
        comparison = row.get("xfa_path_comparison_stressed") or {}
        if comparison.get("paths_are_alternative_not_additive") is not True:
            raise OperatingPackageError("XFA alternatives became additive.")
        standard_ev = float(
            comparison["standard"][
                "expected_trader_payout_per_new_combine_attempt_usd"
            ]
        )
        consistency_ev = float(
            comparison["consistency"][
                "expected_trader_payout_per_new_combine_attempt_usd"
            ]
        )
        expected_path = "CONSISTENCY" if consistency_ev > standard_ev else "STANDARD"
        if row.get("selected_xfa_path") != expected_path:
            raise OperatingPackageError("Selected XFA alternative is not reproducible.")
        profile = row.get("selected_post_payout_profile") or {}
        if profile.get("book_id") != row.get("policy_id") or profile.get(
            "path"
        ) != f"XFA_{expected_path}":
            raise OperatingPackageError("Selected post-payout profile drifted.")
        for scenario, combine_key in (
            ("normal", "normal"),
            ("stressed_1_5x", "stressed_1_5x"),
        ):
            combine = row["combine_evidence"][combine_key]
            expected = _post_payout_per_attempt(
                profile["summary"][scenario],
                combine_pass_rate=float(combine["pass_rate"]),
                combine_denominator=int(combine["starts"]),
            )
            if (profile.get("derived_per_new_combine_attempt") or {}).get(
                scenario
            ) != expected:
                raise OperatingPackageError(
                    "Post-payout per-attempt derivation drifted."
                )
    assignments = payload["role_rule"]["assignments"]
    if payload["role_rule"].get("version") != ROLE_RULE_VERSION or len(assignments) != 4:
        raise OperatingPackageError("Operating role rule drifted.")
    role_map = {str(row["role"]): str(row["policy_id"]) for row in assignments}
    if role_map != ROLE_IDS:
        raise OperatingPackageError("Frozen role assignment drifted.")
    audit = payload.get("redundancy_audit") or {}
    _validate_redundancy_audit(audit)
    inventory = payload.get("canonical_sleeve_inventory") or {}
    if int(inventory.get("sleeve_count", 0)) != 18 or inventory.get(
        "shared_by_all_six_books"
    ) is not True:
        raise OperatingPackageError("Canonical 18-sleeve inventory is incomplete.")
    acquisition = payload.get("data_acquisition_authorization") or {}
    if (
        acquisition.get("authorization_basis")
        != "DIRECT_USER_DIRECTIVE_HYDRA_OPERATING_PACKAGE_V1_2026_07_16"
        or float(acquisition.get("maximum_total_incremental_spend_usd", -1.0))
        != 10.0
        or float(acquisition.get("minimum_remaining_budget_reserve_usd", -1.0))
        != 25.0
        or acquisition.get("broad_historical_purchase_authorized") is not False
        or acquisition.get("q4_access_authorized") is not False
        or acquisition.get("broker_or_order_authorized") is not False
    ):
        raise OperatingPackageError("Bounded append-only data authority drifted.")


def write_operating_package(path: str | Path, payload: Mapping[str, Any]) -> Path:
    validate_operating_package_v1(payload)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    if target.exists():
        if target.read_text(encoding="utf-8") != content:
            raise OperatingPackageError(f"Immutable operating-package drift: {target}")
        return target
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def verify_operating_package_seal(
    output_dir: str | Path,
    *,
    project_root: str | Path,
) -> dict[str, Any]:
    """Verify the receipt-last publication and all executable bindings."""

    root = Path(project_root).resolve()
    output = Path(output_dir).resolve()
    receipt_path = output / "OPERATING_PACKAGE_V1_seal_receipt.json"
    manifest_path = output / "OPERATING_PACKAGE_V1.json"
    report_path = output / "OPERATING_PACKAGE_V1.md"
    receipt = _json(receipt_path)
    body = dict(receipt)
    claimed = str(body.pop("receipt_hash", ""))
    if (
        receipt.get("schema") != SEAL_RECEIPT_SCHEMA
        or not claimed
        or stable_hash(body) != claimed
        or receipt.get("publication_contract")
        != {
            "manifest_and_report_written_before_receipt": True,
            "receipt_is_commit_marker": True,
            "immutable": True,
        }
    ):
        raise OperatingPackageError("Operating-package seal receipt drifted.")
    artifacts = receipt.get("artifacts")
    expected = {
        "OPERATING_PACKAGE_V1.json": manifest_path,
        "OPERATING_PACKAGE_V1.md": report_path,
    }
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(expected):
        raise OperatingPackageError("Operating-package receipt is incomplete.")
    for name, path in expected.items():
        metadata = artifacts[name]
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("relative_path") != name
            or not path.is_file()
            or int(metadata.get("size_bytes", -1)) != path.stat().st_size
            or str(metadata.get("sha256") or "") != sha256_file(path)
        ):
            raise OperatingPackageError(
                f"Operating-package sealed artifact drifted: {name}."
            )
    manifest = _json(manifest_path)
    validate_operating_package_v1(manifest)
    if (
        receipt.get("manifest_hash") != manifest.get("manifest_hash")
        or receipt.get("manifest_sha256") != sha256_file(manifest_path)
        or receipt.get("report_sha256") != sha256_file(report_path)
        or receipt.get("source_commit") != manifest.get("source_commit")
    ):
        raise OperatingPackageError("Operating-package receipt binding drifted.")
    _validate_bound_sources(manifest, project_root=root)
    return receipt


def _validate_post_payout_frontier(value: Mapping[str, Any]) -> None:
    semantic = dict(value)
    claimed = str(semantic.pop("result_hash", ""))
    if (
        value.get("schema") != "hydra_xfa_post_payout_frontier_result_v1"
        or not claimed
        or stable_hash(semantic) != claimed
        or int(value.get("book_count", 0)) != 6
        or int(value.get("profiles_per_book", 0)) != 24
        or int(value.get("transition_count", 0)) != 2_028
        or int(value.get("alternative_reference_xfa_path_count", 0)) != 4_056
        or int(value.get("frontier_evaluation_count", 0)) != 48_672
        or len(str(value.get("source_event_tape_sha256") or "")) != 64
    ):
        raise OperatingPackageError("Post-payout frontier contract/hash drifted.")
    invariants = value.get("invariants") or {}
    required_true = {
        "market_signals_replayed",
        "combine_paths_replayed",
        "evidence_raw_decoded",
    }
    if any(invariants.get(key) is not False for key in required_true):
        raise OperatingPackageError("Post-payout frontier replayed frozen evidence.")
    for key in (
        "book_limit_respected",
        "profiles_per_book_exact",
        "baseline_reconciliation_exact",
        "standard_consistency_alternatives_not_added",
        "no_broker",
        "no_orders",
        "no_q4_access",
        "no_data_purchase",
    ):
        if invariants.get(key) is not True:
            raise OperatingPackageError(f"Post-payout invariant failed: {key}.")
    books = list(value.get("books") or ())
    if {str(row.get("policy_id") or "") for row in books} != set(ALL_BOOK_IDS):
        raise OperatingPackageError("Post-payout frontier book set drifted.")
    for book in books:
        policy_id = str(book.get("policy_id") or "")
        if (
            int(book.get("transition_count", 0)) != 338
            or book.get("selected_xfa_path") != "CONSISTENCY"
            or (book.get("baseline_reconciliation") or {}).get("status") != "EXACT"
            or int(
                (book.get("baseline_reconciliation") or {}).get(
                    "matched_transition_count", 0
                )
            )
            != 338
        ):
            raise OperatingPackageError(f"Post-payout book evidence drifted: {policy_id}.")
        profiles = list(book.get("profiles") or ())
        if len(profiles) != 24:
            raise OperatingPackageError(f"Post-payout profile count drifted: {policy_id}.")
        by_id: dict[str, Mapping[str, Any]] = {}
        for profile in profiles:
            policy = profile.get("policy") or {}
            profile_id = str(policy.get("policy_id") or "")
            fingerprint = str(policy.get("fingerprint") or "")
            policy_body = dict(policy)
            policy_body.pop("policy_id", None)
            policy_body.pop("fingerprint", None)
            if (
                not profile_id
                or profile_id in by_id
                or policy.get("book_id") != policy_id
                or policy.get("path") != "XFA_CONSISTENCY"
                or stable_hash(policy_body) != fingerprint
                or profile_id != "xfa_post_" + fingerprint[:24]
            ):
                raise OperatingPackageError(
                    f"Post-payout policy fingerprint drifted: {policy_id}."
                )
            by_id[profile_id] = profile
        selected = book.get("selected_profile") or {}
        selected_id = str(selected.get("policy_id") or "")
        source = by_id.get(selected_id)
        if (
            source is None
            or selected.get("book_id") != policy_id
            or selected.get("path") != "XFA_CONSISTENCY"
            or selected.get("selected_xfa_path") != "XFA_CONSISTENCY"
            or source.get("selection_eligible") is not True
            or int(source.get("pareto_layer") or 0) != 1
            or selected.get("summary") != source.get("summary")
        ):
            raise OperatingPackageError(
                f"Selected post-payout profile is not reproducible: {policy_id}."
            )


def _validate_redundancy_audit(value: Mapping[str, Any]) -> None:
    semantic = dict(value)
    claimed = str(semantic.pop("audit_hash", ""))
    if (
        value.get("schema") != "hydra_operating_redundancy_audit_v1"
        or not claimed
        or stable_hash(semantic) != claimed
        or len(value.get("pairwise_redundancy") or ()) != 15
        or (value.get("inventory") or {}).get("sleeve_count") != 18
        or (value.get("inventory") or {}).get("shared_by_all_six_books") is not True
    ):
        raise OperatingPackageError("Operating redundancy audit drifted.")
    roles = {
        str(row.get("policy_id") or ""): str(
            row.get("derived_operating_role") or ""
        )
        for row in value.get("roles") or ()
    }
    expected = {
        ROLE_IDS["CORE_BOOK"]: "CORE",
        ROLE_IDS["SAFETY_BOOK"]: "SAFETY_BUFFER",
        ROLE_IDS["DIVERSIFIER_BOOK"]: "DIVERSIFIER_GOVERNOR_RELATIVE",
        ROLE_IDS["BACKUP_BOOK"]: "BACKUP_DIVERSE",
    }
    if set(roles) != set(ALL_BOOK_IDS) or any(
        roles.get(policy_id) != role for policy_id, role in expected.items()
    ):
        raise OperatingPackageError("Operating role audit drifted.")


def _cross_check_shadow_and_selection(
    *,
    package: Mapping[str, Any],
    selection_row: Mapping[str, Any],
    export_receipt: Mapping[str, Any],
) -> None:
    provenance = package.get("evidence_provenance") or {}
    mapping = {
        "active_risk_policy_sha256": "active_risk_policy_sha256",
        "combine_book_sha256": "combine_book_sha256",
        "membership_sha256": "membership_sha256",
        "xfa_standard_book_sha256": "xfa_standard_book_sha256",
        "xfa_consistency_book_sha256": "xfa_consistency_book_sha256",
        "entry_hash": "selection_entry_hash",
    }
    if any(
        str(selection_row.get(selection_key) or "")
        != str(provenance.get(provenance_key) or "")
        for selection_key, provenance_key in mapping.items()
    ):
        raise OperatingPackageError("Forward package differs from frozen selection.")
    if (
        export_receipt.get("policy_id") != package.get("candidate_id")
        or export_receipt.get("package_hash") != package.get("package_hash")
        or export_receipt.get("freeze_timestamp_utc")
        != package.get("freeze_timestamp_utc")
    ):
        raise OperatingPackageError("Forward export receipt binding drifted.")


def _validate_bound_sources(
    package: Mapping[str, Any], *, project_root: Path
) -> None:
    binding = package.get("forward_data_binding") or {}
    active_meta = binding.get("active_risk_boundary") or {}
    ingestion_meta = binding.get("ingestion_boundary") or {}
    active_path = _resolve(project_root, str(active_meta.get("path") or ""))
    ingestion_path = _resolve(
        project_root, str(ingestion_meta.get("path") or "")
    )
    if (
        not active_path.is_file()
        or sha256_file(active_path) != active_meta.get("sha256")
        or not ingestion_path.is_file()
        or sha256_file(ingestion_path) != ingestion_meta.get("sha256")
    ):
        raise OperatingPackageError("Forward boundary bytes drifted.")
    active = _json(active_path)
    ingestion = _json(ingestion_path)
    validate_active_risk_forward_boundary(active, repository_root=project_root)
    validate_forward_boundary_manifest(ingestion)
    active_rows = {
        str(row["candidate_id"]): row for row in active.get("candidates", ())
    }
    ingestion_rows = {
        str(row["candidate_id"]): row
        for row in ingestion.get("candidates", ())
    }
    books = {str(row["policy_id"]): row for row in package.get("books", ())}
    if set(active_rows) != set(ALL_BOOK_IDS) or set(ingestion_rows) != set(ALL_BOOK_IDS):
        raise OperatingPackageError("Forward boundary candidate set drifted.")
    for policy_id in ALL_BOOK_IDS:
        active_row = active_rows[policy_id]
        ingestion_row = ingestion_rows[policy_id]
        book = books[policy_id]
        if (
            active_row.get("package_sha256")
            != book["shadow_package"]["file_sha256"]
            or ingestion_row.get("configuration_sha256")
            != active_row.get("package_sha256")
            or ingestion_row.get("configuration_hash")
            != active_row.get("package_hash")
            or ingestion_row.get("freeze_timestamp_utc")
            != active_row.get("freeze_timestamp_utc")
            or sorted(ingestion_row.get("required_roots") or ())
            != sorted(active_row.get("required_roots") or ())
        ):
            raise OperatingPackageError(
                f"Forward boundary projection drifted: {policy_id}."
            )
    frontier = package.get("source_artifacts", {}).get("post_payout_frontier") or {}
    frontier_path = _resolve(project_root, str(frontier.get("path") or ""))
    if not frontier_path.is_file():
        raise OperatingPackageError("Bound post-payout frontier is absent.")
    frontier_value = _json(frontier_path)
    _validate_post_payout_frontier(frontier_value)
    if frontier_value.get("result_hash") != frontier.get("result_hash"):
        raise OperatingPackageError("Bound post-payout frontier drifted.")
    event_tape = frontier_value.get("canonical_payout_event_tape") or {}
    event_path = _resolve(project_root, str(event_tape.get("path") or ""))
    if (
        not event_path.is_file()
        or sha256_file(event_path) != event_tape.get("sha256")
        or int(event_tape.get("event_count", -1))
        != int(event_tape.get("unique_event_fingerprint_count", -2))
        or int(event_tape.get("event_count", 0)) <= 0
    ):
        raise OperatingPackageError("Canonical post-payout event tape drifted.")
    for label, boundary_key in (
        ("latest_update", "ingestion_boundary"),
        ("latest_processor", "active_risk_boundary"),
    ):
        metadata = binding.get(label) or {}
        result_path = _resolve(project_root, str(metadata.get("path") or ""))
        if (
            not result_path.is_file()
            or sha256_file(result_path) != metadata.get("sha256")
        ):
            raise OperatingPackageError(f"Bound forward result drifted: {label}.")
        result = _json(result_path)
        result_body = dict(result)
        result_hash = str(result_body.pop("result_hash", ""))
        if not result_hash or stable_hash(result_body) != result_hash:
            raise OperatingPackageError(f"Forward result hash drifted: {label}.")
        expected_boundary = binding.get(boundary_key) or {}
        if (
            result.get("boundary_manifest_sha256") != expected_boundary.get("sha256")
            or int(result.get("broker_connections", -1)) != 0
            or int(result.get("outbound_orders", -1)) != 0
            or int(result.get("q4_access_delta", -1)) != 0
        ):
            raise OperatingPackageError(f"Forward result safety drifted: {label}.")
        if label == "latest_processor" and (
            int(result.get("account_mutations", -1)) != 0
            or int(result.get("signals_emitted", -1)) != 0
            or int(result.get("virtual_fills_created", -1)) != 0
        ):
            raise OperatingPackageError(
                "Unproven forward signal engine changed account state."
            )


def _combine_metrics(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "starts": int(value["episode_count"]),
        "passes": int(value["pass_count"]),
        "pass_rate": float(value["pass_rate"]),
        "net_pnl_usd": float(value["net_total"]),
        "minimum_mll_buffer_usd": float(value["minimum_mll_buffer"]),
        "mll_breach_rate": float(value["mll_breach_rate"]),
        "pass_blocks": list(value["pass_block_ids"]),
    }


def _xfa_metrics(value: Mapping[str, Any]) -> dict[str, Any]:
    unconditional = value["unconditional_lower_bound"]
    days = value["days_to_first_payout"]["all_observed_first_payouts"]
    return {
        "path_count": int(value["xfa_paths_started"]),
        "first_payout_count": int(value["first_payouts"]),
        "first_payout_rate_conditional_on_combine": float(
            unconditional["first_payout_probability_conditional_on_combine_pass"]
        ),
        "median_days_to_first_payout": days["median"],
        "payout_cycles": int(value["payout_cycles"]),
        "trader_net_payout_usd": float(value["trader_net_payout"]),
        "expected_trader_payout_per_successful_combine_usd": float(
            unconditional["expected_trader_payout_per_successful_combine"]
        ),
        "expected_trader_payout_per_new_combine_attempt_usd": float(
            unconditional["expected_trader_payout_per_combine_attempt"]
        ),
        "post_payout_survival_count": int(value["post_payout_survival_count"]),
        "post_payout_survival_rate_conditional_on_first_payout": float(
            unconditional["post_payout_survival_probability_conditional_on_first_payout"]
        ),
        "closure_before_first_payout_count": int(
            value["closure_before_first_payout_count"]
        ),
    }


def _post_payout_per_attempt(
    summary: Mapping[str, Any],
    *,
    combine_pass_rate: float,
    combine_denominator: int,
) -> dict[str, Any]:
    return {
        "method": (
            "SELECTED_PROFILE_CONDITIONAL_TRANSITION_METRIC_TIMES_"
            "FROZEN_192_START_COMBINE_PASS_RATE"
        ),
        "combine_start_denominator": combine_denominator,
        "combine_pass_rate": combine_pass_rate,
        "expected_trader_net_payout_usd": float(
            summary["expected_trader_net_payout_per_transition"]
        )
        * combine_pass_rate,
        "probability_first_payout": float(summary["first_payout_rate"])
        * combine_pass_rate,
        "probability_at_least_two_payouts": float(
            summary["probability_at_least_two_payouts"]
        )
        * combine_pass_rate,
        "expected_payout_cycles": float(summary["payout_cycles_per_transition"])
        * combine_pass_rate,
        "standard_and_consistency_added": False,
    }


def _verify_selection(value: Mapping[str, Any]) -> None:
    semantic = dict(value)
    claimed = str(semantic.pop("selection_manifest_hash", ""))
    if not claimed or stable_hash(semantic) != claimed:
        raise OperatingPackageError("Frozen selection manifest hash mismatch.")
    if value.get("schema") != "hydra_frozen_book_selection_v1":
        raise OperatingPackageError("Unsupported frozen selection.")
    if int(value.get("primary_count", 0)) != 5 or int(value.get("backup_count", 0)) != 1:
        raise OperatingPackageError("Frozen selection is not five plus one.")


def _verify_shadow_package(value: Mapping[str, Any], policy_id: str) -> None:
    semantic = dict(value)
    claimed = str(semantic.pop("package_hash", ""))
    if not claimed or stable_hash(semantic) != claimed:
        raise OperatingPackageError(f"Shadow package hash mismatch: {policy_id}")
    if value.get("candidate_id") != policy_id or value.get("role") != "FORWARD_SHADOW_CANDIDATE":
        raise OperatingPackageError(f"Shadow package identity/status drift: {policy_id}")
    if value.get("broker_connectivity") is not False or value.get(
        "outbound_order_capability"
    ) is not False:
        raise OperatingPackageError(f"Shadow package can execute: {policy_id}")
    policy = value.get("data_policy") or {}
    if policy.get("append_only") is not True or policy.get("post_freeze_only") is not True:
        raise OperatingPackageError(f"Shadow package is not append-only: {policy_id}")


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OperatingPackageError(f"Invalid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise OperatingPackageError(f"Artifact is not an object: {path}")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise OperatingPackageError("Timezone-aware package timestamp required.")
    return value.astimezone(timezone.utc)


__all__ = [
    "ALL_BOOK_IDS",
    "BACKUP_ID",
    "PRIMARY_IDS",
    "ROLE_IDS",
    "ROLE_RULE_VERSION",
    "SCHEMA",
    "SEAL_RECEIPT_SCHEMA",
    "OperatingPackageError",
    "build_operating_package_v1",
    "sha256_file",
    "stable_hash",
    "validate_operating_package_v1",
    "verify_operating_package_seal",
    "write_operating_package",
]
