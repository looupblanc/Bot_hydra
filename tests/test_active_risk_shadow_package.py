from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import hydra.shadow.active_risk_package as active_package_module

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.production.portfolio_books import SleeveRecord, stable_hash
from hydra.shadow.active_risk_package import (
    ACTIVE_RISK_COMBINE_HORIZONS,
    ACTIVE_RISK_PACKAGE_ROLE,
    ACTIVE_RISK_XFA_HORIZON_DAYS,
    ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
    ActiveRiskShadowPackageError,
    FrozenSignalBinding,
    build_active_risk_shadow_package,
    reconstruct_active_risk_shadow_package,
)
from hydra.shadow.package_factory import PACKAGE_SCHEMA, write_shadow_package
from hydra.shadow.active_risk_binding_loader import (
    ActiveRiskBindingLoaderError,
    EXPORT_AUDIT_NAME,
    seal_active_risk_shadow_export,
    verify_active_risk_shadow_export,
)
from hydra.shadow.active_risk_forward_boundary import (
    ActiveRiskForwardBoundaryError,
    build_active_risk_forward_boundary,
    validate_active_risk_forward_boundary,
)
from hydra.shadow.active_risk_forward_processor import (
    ActiveRiskForwardProcessorError,
    run_active_risk_forward_processor,
)
from hydra.shadow.contract_resolver import ContractResolution, ResolvedContract
from hydra.shadow.forward_bar_store import ForwardBar, ForwardBarStore
from hydra.shadow.forward_feed_manifest import write_manifest
from hydra.research.turbo_feature_builder import FEATURE_BUNDLE_VERSION, FEATURE_DAG_HASH


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sources() -> tuple[
    dict[str, SleeveSpec],
    dict[str, SleeveRecord],
    dict[str, str],
]:
    specs: dict[str, SleeveSpec] = {}
    records: dict[str, SleeveRecord] = {}
    components: dict[str, str] = {}
    markets = (("ES", "MES"), ("NQ", "MNQ"), ("CL", "MCL"))
    sessions = {
        -1: "ALL_ELIGIBLE",
        0: "OPEN",
        1: "MID_SESSION",
        2: "LATE_CLOSE",
    }
    for index in range(18):
        sleeve_id = f"sleeve-{index:02d}"
        market, contract = markets[index % len(markets)]
        session_code = (-1, 0, 1, 2)[index % 4]
        spec = SleeveSpec(
            sleeve_id=sleeve_id,
            component_ids=(f"component-{index:02d}",),
            market=market,
            execution_market=contract,
            timeframe=("15m", "30m", "60m")[index % 3],
            session_code=session_code,
            trigger_feature=f"past_feature_{index:02d}",
            trigger_operator="GT" if index % 2 == 0 else "LT",
            trigger_quantile=0.10 + index * 0.04,
            context_feature="ctx_15m_return",
            context_operator="GT",
            context_quantile=0.65,
            side=1 if index % 2 == 0 else -1,
            holding_bars=(5, 15, 30, 60)[index % 4],
            exit_style="TIME_ONLY",
            role=EconomicRole.PRIMARY_ALPHA,
            source_campaign="hydra_economic_production_0024",
            lineage_id=f"lineage-{index:02d}",
        )
        immutable = _sha(f"immutable:{sleeve_id}")
        record = SleeveRecord(
            sleeve_id=sleeve_id,
            immutable_fingerprint=immutable,
            behavioral_fingerprint=spec.behavioral_fingerprint,
            signal_ledger_sha256=_sha(f"signals:{sleeve_id}"),
            trade_ledger_sha256=_sha(f"trades:{sleeve_id}"),
            market=market,
            contract=contract,
            timeframe=spec.timeframe,
            session=sessions[session_code],
            economic_role=(
                spec.role.value if index % 2 == 0 else "TARGET_VELOCITY"
            ),
            source_campaign=spec.source_campaign,
            family_id=spec.lineage_id,
        )
        specs[sleeve_id] = spec
        records[sleeve_id] = record
        components[sleeve_id] = immutable
    return specs, records, components


def _signal_bindings(
    specs: dict[str, SleeveSpec], records: dict[str, SleeveRecord]
) -> dict[str, FrozenSignalBinding]:
    return {
        sleeve_id: FrozenSignalBinding(
            sleeve_id=sleeve_id,
            trigger_feature=spec.trigger_feature,
            trigger_operator=spec.trigger_operator,
            trigger_threshold=0.001 + index * 0.0001,
            context_feature=spec.context_feature,
            context_operator=spec.context_operator,
            context_threshold=(-0.001 - index * 0.0001),
            calibration_start="2023-01-01",
            calibration_end_exclusive="2023-07-01",
            trigger_finite_observation_count=10_000 + index,
            context_finite_observation_count=9_000 + index,
            source_execution_fingerprint=records[sleeve_id].immutable_fingerprint,
            source_cheap_screen_path=(
                f"reports/economic_evolution/source/cheap_screen_{index:02d}.jsonl"
            ),
            source_cheap_screen_sha256=_sha(f"screen:{sleeve_id}"),
            source_cheap_screen_row_sha256=_sha(f"screen-row:{sleeve_id}"),
            feature_matrix_manifest_path=(
                f"data/cache/economic_evolution/features/{index:064x}/manifest.json"
            ),
            feature_matrix_manifest_sha256=_sha(f"manifest:{sleeve_id}"),
            feature_matrix_schema="hydra_canonical_feature_store_v2",
            feature_matrix_bundle_hash=_sha(f"matrix:{sleeve_id}"),
            feature_matrix_source_data_sha256=_sha(f"source:{sleeve_id}"),
            feature_matrix_roll_map_sha256=_sha(f"roll:{sleeve_id}"),
            feature_matrix_market=spec.market,
            feature_matrix_execution_market=spec.execution_market,
            feature_bundle_version=FEATURE_BUNDLE_VERSION,
            feature_dag_hash=FEATURE_DAG_HASH,
            trigger_array_sha256=_sha(f"trigger-array:{sleeve_id}"),
            context_array_sha256=_sha(f"context-array:{sleeve_id}"),
            session_day_array_sha256=_sha(f"day-array:{sleeve_id}"),
            session_code_array_sha256=_sha(f"session-array:{sleeve_id}"),
        )
        for index, (sleeve_id, spec) in enumerate(specs.items())
    }


def _policy(
    ids: tuple[str, ...], *, policy_id: str = "active_pool_test_finalist"
) -> ActiveRiskPoolPolicy:
    return ActiveRiskPoolPolicy(
        policy_id=policy_id,
        component_priority=ids,
        nominal_risk_charge_per_mini=tuple((row, 250.0) for row in ids),
        maximum_concurrent_sleeves=4,
        aggregate_open_risk_ceiling=1800.0,
        maximum_mll_buffer_fraction=0.40,
        protected_mll_buffer=400.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=(
            SameInstrumentConflictRule.ALLOW_SAME_DIRECTION
        ),
        daily_loss_guard=1500.0,
        daily_consistency_profit_guard=3500.0,
        target_protection_distance=1200.0,
        target_protection_mode=TargetProtectionMode.SCALE_50,
        static_risk_tier=2.0,
    )


def _selection_chain(
    policy: ActiveRiskPoolPolicy,
    specs: dict[str, SleeveSpec],
    records: dict[str, SleeveRecord],
) -> tuple[dict[str, object], dict[str, object], str]:
    membership = [
        {
            "component_id": sleeve_id,
            "component_role": records[sleeve_id].economic_role,
            "risk_allocation": 1.0 / len(specs),
            "immutable_fingerprint": records[sleeve_id].immutable_fingerprint,
            "behavioral_fingerprint": records[sleeve_id].behavioral_fingerprint,
            "signal_ledger_sha256": records[sleeve_id].signal_ledger_sha256,
            "trade_ledger_sha256": records[sleeve_id].trade_ledger_sha256,
            "market": records[sleeve_id].market,
            "contract": records[sleeve_id].contract,
            "timeframe": records[sleeve_id].timeframe,
            "session": records[sleeve_id].session,
            "source_campaign": records[sleeve_id].source_campaign,
            "sleeve_specification": specs[sleeve_id].to_dict(),
        }
        for sleeve_id in sorted(specs)
    ]
    policy_sha = stable_hash(policy.to_dict())
    membership_sha = stable_hash(membership)
    combine_book = {
        "book": "COMBINE_BOOK",
        "policy_id": policy.policy_id,
        "active_risk_policy_sha256": policy_sha,
        "membership_sha256": membership_sha,
        "account_parameters": {
            "starting_balance": 150_000.0,
            "profit_target": 9_000.0,
            "maximum_loss_limit": 4_500.0,
            "maximum_mini_equivalent": 15,
            "maximum_simultaneous_positions": 3,
            "consistency_rule": "TOPSTEP_150K_CONFIGURED",
            "session_constraints": "FROZEN_SOURCE_COMPONENT",
            "dynamic_loss_streak_ratchet": False,
            "unrealized_aggregation_semantics": (
                active_package_module.UNREALIZED_AGGREGATION_SEMANTICS
            ),
            "timestamp_exact_combined_unrealized_claimed": False,
        },
        "costs": {
            "normal_multiplier": 1.0,
            "stressed_multiplier": 1.5,
            "source_component_costs_frozen": True,
            "retune_after_outcomes": False,
        },
        "frozen_horizons": [20, 40, 60, 90, "FULL"],
        "underlying_sleeve_logic_mutated": False,
    }
    profile = active_package_module._static_xfa_profile(policy).to_dict()
    xfa_common = {
        "policy_id": policy.policy_id,
        "source_combine_book_sha256": stable_hash(combine_book),
        "membership_sha256": membership_sha,
        "xfa_profile": profile,
        "rule_snapshot": active_package_module.official_rule_snapshot_2026_07_15().to_dict(),
        "overlay_semantics": ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
        "combine_profit_transferred_to_xfa": False,
        "combine_governor_controls_applied_in_xfa": False,
        "book_frozen_before_outcomes": True,
        "selected_after_combine_outcome": False,
    }
    standard_book = {"book": "XFA_STANDARD_BOOK", **xfa_common}
    consistency_book = {"book": "XFA_CONSISTENCY_BOOK", **xfa_common}
    declaration: dict[str, object] = {
        "policy_id": policy.policy_id,
        "structural_fingerprint": policy.structural_fingerprint,
        "active_risk_policy": policy.to_dict(),
        "active_risk_policy_sha256": policy_sha,
        "membership": membership,
        "membership_sha256": membership_sha,
        "combine_book": combine_book,
        "combine_book_sha256": stable_hash(combine_book),
        "xfa_standard_book": standard_book,
        "xfa_standard_book_sha256": stable_hash(standard_book),
        "xfa_consistency_book": consistency_book,
        "xfa_consistency_book_sha256": stable_hash(consistency_book),
    }
    entry: dict[str, object] = {
        "policy_id": policy.policy_id,
        "selection_role": "PRIMARY",
        "expanded_exact_account_behavior_cluster": "expanded_exact_account_cluster_01",
        "expanded_economic_behavior_cluster": "expanded_economic_behavior_cluster_01",
        "status": ACTIVE_RISK_PACKAGE_ROLE,
        "structural_fingerprint": policy.structural_fingerprint,
        "active_risk_policy_sha256": policy_sha,
        "membership_sha256": membership_sha,
        "combine_book_sha256": declaration["combine_book_sha256"],
        "xfa_standard_book_sha256": declaration["xfa_standard_book_sha256"],
        "xfa_consistency_book_sha256": declaration[
            "xfa_consistency_book_sha256"
        ],
        "frozen_policy_specification": declaration,
        "frozen_policy_specification_sha256": stable_hash(declaration),
    }
    entry["entry_hash"] = stable_hash(entry)
    selection: dict[str, object] = {
        "schema": "hydra_frozen_book_selection_v1",
        "campaign_id": "hydra_active_risk_pool_target_velocity_0026",
        "selection_completed_at_utc": "2026-07-15T12:01:00Z",
        "source_decision_report": {
            "report_hash": "f" * 64,
            "json_sha256": "d" * 64,
            "seal_receipt_hash": "9" * 64,
            "seal_receipt_sha256": "8" * 64,
            "sealed_at_utc": "2026-07-15T12:00:00Z",
            "seal_receipt_name": "decision_report_revision_02_seal_receipt.json",
            "report_json_name": "decision_report_revision_02.json",
            "verified_before_selection": True,
        },
        "evidence_status": "DEVELOPMENT_ONLY_NOT_INDEPENDENT_CONFIRMATION",
        "maximum_candidate_status": ACTIVE_RISK_PACKAGE_ROLE,
        "paper_shadow_ready_assigned": False,
        "forward_contract": {
            "append_only_post_freeze_bars_required": True,
            "no_broker": True,
            "no_orders": True,
            "no_q4_access": True,
            "no_new_data_purchase": True,
            "paper_shadow_ready_prohibited_from_this_evidence": True,
        },
        "selected_books": [entry],
    }
    selection["selection_manifest_hash"] = stable_hash(selection)
    selection_bytes = (
        json.dumps(
            selection,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    return declaration, selection, hashlib.sha256(selection_bytes).hexdigest()


def _rehash_selection_chain(
    declaration: dict[str, object], selection: dict[str, object]
) -> str:
    combine_sha = stable_hash(declaration["combine_book"])
    declaration["combine_book_sha256"] = combine_sha
    for book_name, hash_name in (
        ("xfa_standard_book", "xfa_standard_book_sha256"),
        ("xfa_consistency_book", "xfa_consistency_book_sha256"),
    ):
        declaration[book_name]["source_combine_book_sha256"] = combine_sha
        declaration[hash_name] = stable_hash(declaration[book_name])
    entry = selection["selected_books"][0]
    entry["active_risk_policy_sha256"] = declaration["active_risk_policy_sha256"]
    entry["membership_sha256"] = declaration["membership_sha256"]
    entry["combine_book_sha256"] = declaration["combine_book_sha256"]
    entry["xfa_standard_book_sha256"] = declaration["xfa_standard_book_sha256"]
    entry["xfa_consistency_book_sha256"] = declaration[
        "xfa_consistency_book_sha256"
    ]
    entry["frozen_policy_specification"] = declaration
    entry["frozen_policy_specification_sha256"] = stable_hash(declaration)
    entry.pop("entry_hash", None)
    entry["entry_hash"] = stable_hash(entry)
    selection.pop("selection_manifest_hash", None)
    selection["selection_manifest_hash"] = stable_hash(selection)
    payload = (
        json.dumps(
            selection,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _build(*, policy_id: str = "active_pool_test_finalist"):
    specs, records, components = _sources()
    bindings = _signal_bindings(specs, records)
    policy = _policy(tuple(specs), policy_id=policy_id)
    declaration, selection, selection_sha = _selection_chain(
        policy, specs, records
    )
    package = build_active_risk_shadow_package(
        policy,
        specs,
        records,
        components,
        bindings,
        source_commit="a" * 40,
        decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
        selection_completed_at_utc="2026-07-15T12:01:00Z",
        freeze_timestamp_utc="2026-07-15T12:02:00Z",
        campaign_manifest_sha256="b" * 64,
        evidence_receipt_sha256="e" * 64,
        evidence_manifest_sha256="f" * 64,
        evidence_bundle_sha256="c" * 64,
        decision_report_sha256="d" * 64,
        selection_manifest_sha256=selection_sha,
        selection_seal_receipt_hash="a" * 64,
        frozen_finalist_declaration=declaration,
        selection_manifest=selection,
    )
    return policy, specs, records, components, package


def test_active_risk_package_reconstructs_exact_eighteen_sleeves(tmp_path) -> None:
    policy, specs, records, _, package = _build()
    payload = package.to_dict()

    assert payload["schema"] == PACKAGE_SCHEMA
    assert payload["role"] == ACTIVE_RISK_PACKAGE_ROLE
    assert payload["broker_connectivity"] is False
    assert payload["outbound_order_capability"] is False
    assert payload["observability"]["paper_shadow_ready"] is False
    assert payload["evidence_provenance"]["paper_shadow_ready"] is False
    assert payload["sizing_policy"]["combine_book"]["policy"] == policy.to_dict()
    assert payload["sizing_policy"]["combine_book"]["horizons"] == list(
        ACTIVE_RISK_COMBINE_HORIZONS
    )
    assert len(payload["feature_contract"]["immutable_sleeves"]) == 18
    assert payload["feature_contract"][
        "forward_threshold_recalibration_prohibited"
    ] is True

    machine, _ = write_shadow_package(package, tmp_path / "forward")
    disk = json.loads(machine.read_text(encoding="utf-8"))
    reconstructed = reconstruct_active_risk_shadow_package(disk)

    assert reconstructed.combine_policy == policy
    assert dict(reconstructed.sleeve_specs) == specs
    assert dict(reconstructed.sleeve_records) == records
    assert set(reconstructed.frozen_signal_bindings) == set(specs)
    assert reconstructed.xfa_profile.risk_multiplier == policy.static_risk_tier
    assert reconstructed.package.package_hash == package.package_hash


def test_xfa_books_are_frozen_separate_alternatives() -> None:
    policy, _, _, _, package = _build()
    books = package.sizing_policy["xfa_books"]

    assert set(books) == {"STANDARD", "CONSISTENCY"}
    assert (
        package.sizing_policy["xfa_paths_are_mutually_exclusive_alternatives"]
        is True
    )
    for path in ("STANDARD", "CONSISTENCY"):
        assert books[path]["path"] == path
        assert books[path]["sleeve_ids"] == list(policy.component_priority)
        assert books[path]["horizon_days"] == ACTIVE_RISK_XFA_HORIZON_DAYS
        assert books[path]["overlay_semantics"] == ACTIVE_RISK_XFA_OVERLAY_SEMANTICS
        assert books[path]["combine_governor_controls_applied"] is False
        assert books[path]["profile_selected_from_outcomes"] is False
        assert books[path]["combine_profit_transferred"] is False
        assert books[path]["fresh_balance_usd"] == 0.0


def test_forward_guards_and_provenance_are_fail_closed() -> None:
    _, _, _, _, package = _build()
    data = package.data_policy
    provenance = package.evidence_provenance

    assert data["post_freeze_only"] is True
    assert data["first_eligible_bar_close_operator"] == "STRICTLY_GREATER_THAN_FREEZE"
    assert data["duplicate_bars_rejected"] is True
    assert data["duplicate_source_sequences_rejected"] is True
    assert data["missing_intervals_rejected"] is True
    assert data["out_of_order_bars_rejected"] is True
    assert data["contract_mismatches_rejected"] is True
    assert data["q4_access_authorized"] is False
    assert data["new_data_purchase_authorized"] is False
    assert package.virtual_fill_policy["mode"] == "CONSERVATIVE_VIRTUAL_ONLY"
    assert package.virtual_fill_policy["real_order_submission"] is False
    assert provenance["campaign_manifest_sha256"] == "b" * 64
    assert provenance["evidence_bundle_sha256"] == "c" * 64
    assert provenance["decision_report_sha256"] == "d" * 64
    assert provenance["selection_manifest_sha256"] == hashlib.sha256(
        (
            json.dumps(
                provenance["selection_manifest"],
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    ).hexdigest()


def test_forward_export_receipt_binds_full_audit_and_package(tmp_path) -> None:
    policy, _, _, _, package = _build()
    binding_rows = []
    for sleeve_id, source in package.feature_contract["immutable_sleeves"].items():
        binding_rows.append(
            {
                "sleeve_id": sleeve_id,
                "binding_fingerprint": source["frozen_signal_binding_sha256"],
                "source_campaign": source["sleeve_record"]["source_campaign"],
                "source_campaign_manifest_path": "config/v7/source.json",
                "source_campaign_manifest_sha256": "1" * 64,
                "cheap_screen_path": source["frozen_signal_binding"][
                    "source_cheap_screen_path"
                ],
                "cheap_screen_file_sha256": source["frozen_signal_binding"][
                    "source_cheap_screen_sha256"
                ],
                "cheap_screen_row_sha256": source["frozen_signal_binding"][
                    "source_cheap_screen_row_sha256"
                ],
                "trigger_threshold_reproduced_exactly": True,
                "context_threshold_reproduced_exactly": True,
                "feature_manifest": source["frozen_signal_binding"][
                    "feature_matrix_manifest_path"
                ],
                "feature_bundle_hash": source["frozen_signal_binding"][
                    "feature_matrix_bundle_hash"
                ],
            }
        )
    audit = {
        "schema": "hydra_active_risk_binding_load_audit_v1",
        "campaign_id": "hydra_active_risk_pool_target_velocity_0026",
        "policy_id": policy.policy_id,
        "binding_count": 18,
        "campaign_manifest_path": "config/v7/active.json",
        "campaign_manifest_sha256": package.evidence_provenance[
            "campaign_manifest_sha256"
        ],
        "thresholds_loaded_from_persisted_cheap_screen": True,
        "thresholds_recalibrated_for_forward_use": False,
        "all_required_feature_array_hashes_verified": True,
        "all_calibrations_exactly_reproduced": True,
        "screen_source_resolution": {
            "source_resolution": "ORIGINATING_LEDGER_OR_EXPLICIT_HASH_BOUND_PILOT_FALLBACK",
            "global_directory_search_used": False,
            "pilot_fallback_manifest_path": None,
            "pilot_fallback_manifest_hash": None,
            "pilot_fallback_ledger_path": None,
            "pilot_fallback_ledger_sha256": None,
            "pilot_fallback_sleeve_count": 0,
            "source_ledger_sha256": {"source": "6" * 64},
        },
        "feature_snapshot_binding": {
            "availability_contract": "completed_source_bar_at_or_before_decision",
            "start_inclusive": "2023-01-01",
            "end_exclusive": "2024-10-01",
            "source_data_sha256": "7" * 64,
            "roll_map_sha256": "8" * 64,
            "q4_access_count_delta": 0,
        },
        "bindings": binding_rows,
        "sealed_chain": {
            "decision_report_receipt_hash": "9" * 64,
            "decision_report_sealed_at_utc": "2026-07-15T12:00:00Z",
            "selection_receipt_hash": "a" * 64,
            "selection_sealed_at_utc": "2026-07-15T12:01:00Z",
            "selection_completed_at_utc": "2026-07-15T12:01:00Z",
            "campaign_manifest_sha256": package.evidence_provenance[
                "campaign_manifest_sha256"
            ],
            "decision_report_sha256": package.evidence_provenance[
                "decision_report_sha256"
            ],
            "selection_manifest_sha256": package.evidence_provenance[
                "selection_manifest_sha256"
            ],
            "evidence_receipt_sha256": package.evidence_provenance[
                "evidence_receipt_sha256"
            ],
            "evidence_manifest_sha256": package.evidence_provenance[
                "evidence_manifest_sha256"
            ],
            "evidence_bundle_content_sha256": package.evidence_provenance[
                "evidence_bundle_sha256"
            ],
            "deep_evidence_verification_in_sealed_report": True,
            "freeze_timestamp_source": "EXPORTER_CURRENT_UTC_AFTER_ALL_SEALS",
            "freeze_timestamp_utc": package.freeze_timestamp_utc,
            "package_hash": package.package_hash,
        },
    }
    audit["audit_hash"] = stable_hash(audit)

    binding_tamper = json.loads(json.dumps(audit))
    binding_tamper["bindings"][0]["cheap_screen_file_sha256"] = "0" * 64
    binding_tamper.pop("audit_hash")
    binding_tamper["audit_hash"] = stable_hash(binding_tamper)
    with pytest.raises(
        ActiveRiskBindingLoaderError, match="differs from packaged sleeve"
    ):
        seal_active_risk_shadow_export(
            package, binding_tamper, output_dir=tmp_path / "binding-tamper"
        )

    chain_tamper = json.loads(json.dumps(audit))
    chain_tamper["sealed_chain"]["evidence_bundle_content_sha256"] = "0" * 64
    chain_tamper.pop("audit_hash")
    chain_tamper["audit_hash"] = stable_hash(chain_tamper)
    with pytest.raises(ActiveRiskBindingLoaderError, match="sealed-chain drift"):
        seal_active_risk_shadow_export(
            package, chain_tamper, output_dir=tmp_path / "chain-tamper"
        )

    receipt = seal_active_risk_shadow_export(
        package, audit, output_dir=tmp_path / "export"
    )
    assert receipt["package_hash"] == package.package_hash
    assert receipt["binding_audit_hash"] == audit["audit_hash"]
    assert receipt["safety"]["broker_connectivity"] is False
    assert verify_active_risk_shadow_export(tmp_path / "export") == receipt

    audit_path = tmp_path / "export" / EXPORT_AUDIT_NAME
    persisted = json.loads(audit_path.read_text(encoding="utf-8"))
    persisted["binding_count"] = 17
    audit_path.write_text(json.dumps(persisted), encoding="utf-8")
    with pytest.raises(ActiveRiskBindingLoaderError, match="artifact binding drift"):
        verify_active_risk_shadow_export(tmp_path / "export")


def test_builder_rejects_incomplete_sources_and_premature_freeze() -> None:
    specs, records, components = _sources()
    bindings = _signal_bindings(specs, records)
    policy = _policy(tuple(specs))
    declaration, selection, selection_sha = _selection_chain(
        policy, specs, records
    )
    incomplete = dict(specs)
    incomplete.pop(next(iter(incomplete)))
    with pytest.raises(ActiveRiskShadowPackageError, match="exactly the frozen eighteen"):
        build_active_risk_shadow_package(
            policy,
            incomplete,
            records,
            components,
            bindings,
            source_commit="a" * 40,
            decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
            selection_completed_at_utc="2026-07-15T12:01:00Z",
            freeze_timestamp_utc="2026-07-15T12:02:00Z",
            campaign_manifest_sha256="b" * 64,
            evidence_receipt_sha256="e" * 64,
            evidence_manifest_sha256="f" * 64,
            evidence_bundle_sha256="c" * 64,
            decision_report_sha256="d" * 64,
            selection_manifest_sha256=selection_sha,
            selection_seal_receipt_hash="a" * 64,
            frozen_finalist_declaration=declaration,
            selection_manifest=selection,
        )

    with pytest.raises(ActiveRiskShadowPackageError, match="must follow"):
        build_active_risk_shadow_package(
            policy,
            specs,
            records,
            components,
            bindings,
            source_commit="a" * 40,
            decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
            selection_completed_at_utc="2026-07-15T12:01:00Z",
            freeze_timestamp_utc="2026-07-15T11:59:59Z",
            campaign_manifest_sha256="b" * 64,
            evidence_receipt_sha256="e" * 64,
            evidence_manifest_sha256="f" * 64,
            evidence_bundle_sha256="c" * 64,
            decision_report_sha256="d" * 64,
            selection_manifest_sha256=selection_sha,
            selection_seal_receipt_hash="a" * 64,
            frozen_finalist_declaration=declaration,
            selection_manifest=selection,
        )


def test_builder_rejects_selection_and_frozen_combine_contract_drift() -> None:
    specs, records, components = _sources()
    bindings = _signal_bindings(specs, records)
    policy = _policy(tuple(specs))

    declaration, selection, _ = _selection_chain(policy, specs, records)
    selection["schema"] = "wrong_selection_schema"
    selection_sha = _rehash_selection_chain(declaration, selection)
    with pytest.raises(
        ActiveRiskShadowPackageError,
        match="selection manifest development/forward contract drift",
    ):
        build_active_risk_shadow_package(
            policy,
            specs,
            records,
            components,
            bindings,
            source_commit="a" * 40,
            decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
            selection_completed_at_utc="2026-07-15T12:01:00Z",
            freeze_timestamp_utc="2026-07-15T12:02:00Z",
            campaign_manifest_sha256="b" * 64,
            evidence_receipt_sha256="e" * 64,
            evidence_manifest_sha256="f" * 64,
            evidence_bundle_sha256="c" * 64,
            decision_report_sha256="d" * 64,
            selection_manifest_sha256=selection_sha,
            selection_seal_receipt_hash="a" * 64,
            frozen_finalist_declaration=declaration,
            selection_manifest=selection,
        )

    declaration, selection, _ = _selection_chain(policy, specs, records)
    selection["selection_completed_at_utc"] = "2026-07-15T15:00:00Z"
    selection_sha = _rehash_selection_chain(declaration, selection)
    with pytest.raises(
        ActiveRiskShadowPackageError,
        match="selection manifest development/forward contract drift",
    ):
        build_active_risk_shadow_package(
            policy,
            specs,
            records,
            components,
            bindings,
            source_commit="a" * 40,
            decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
            selection_completed_at_utc="2026-07-15T12:01:00Z",
            freeze_timestamp_utc="2026-07-15T12:02:00Z",
            campaign_manifest_sha256="b" * 64,
            evidence_receipt_sha256="e" * 64,
            evidence_manifest_sha256="f" * 64,
            evidence_bundle_sha256="c" * 64,
            decision_report_sha256="d" * 64,
            selection_manifest_sha256=selection_sha,
            selection_seal_receipt_hash="a" * 64,
            frozen_finalist_declaration=declaration,
            selection_manifest=selection,
        )

    declaration, selection, selection_sha = _selection_chain(policy, specs, records)
    divergent_declaration = dict(declaration)
    divergent_declaration["unbound_field"] = True
    with pytest.raises(
        ActiveRiskShadowPackageError,
        match="selected frozen finalist declaration is not exact",
    ):
        build_active_risk_shadow_package(
            policy,
            specs,
            records,
            components,
            bindings,
            source_commit="a" * 40,
            decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
            selection_completed_at_utc="2026-07-15T12:01:00Z",
            freeze_timestamp_utc="2026-07-15T12:02:00Z",
            campaign_manifest_sha256="b" * 64,
            evidence_receipt_sha256="e" * 64,
            evidence_manifest_sha256="f" * 64,
            evidence_bundle_sha256="c" * 64,
            decision_report_sha256="d" * 64,
            selection_manifest_sha256=selection_sha,
            selection_seal_receipt_hash="a" * 64,
            frozen_finalist_declaration=divergent_declaration,
            selection_manifest=selection,
        )

    declaration, selection, _ = _selection_chain(policy, specs, records)
    declaration["combine_book"]["costs"]["normal_multiplier"] = 1.25
    selection_sha = _rehash_selection_chain(declaration, selection)
    with pytest.raises(
        ActiveRiskShadowPackageError,
        match="sealed Combine/XFA books differ",
    ):
        build_active_risk_shadow_package(
            policy,
            specs,
            records,
            components,
            bindings,
            source_commit="a" * 40,
            decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
            selection_completed_at_utc="2026-07-15T12:01:00Z",
            freeze_timestamp_utc="2026-07-15T12:02:00Z",
            campaign_manifest_sha256="b" * 64,
            evidence_receipt_sha256="e" * 64,
            evidence_manifest_sha256="f" * 64,
            evidence_bundle_sha256="c" * 64,
            decision_report_sha256="d" * 64,
            selection_manifest_sha256=selection_sha,
            selection_seal_receipt_hash="a" * 64,
            frozen_finalist_declaration=declaration,
            selection_manifest=selection,
        )

def test_reconstructor_rejects_governor_xfa_and_source_tampering() -> None:
    _, _, _, _, package = _build()

    governor_tamper = package.to_dict()
    governor_tamper["sizing_policy"]["combine_book"]["policy"][
        "daily_loss_guard"
    ] = 1400.0
    with pytest.raises(ActiveRiskShadowPackageError, match="Combine governor drift"):
        reconstruct_active_risk_shadow_package(governor_tamper)

    xfa_tamper = package.to_dict()
    xfa_tamper["sizing_policy"]["xfa_books"]["STANDARD"][
        "horizon_days"
    ] = 121
    with pytest.raises(ActiveRiskShadowPackageError, match="STANDARD book drift"):
        reconstruct_active_risk_shadow_package(xfa_tamper)

    source_tamper = package.to_dict()
    source_tamper["feature_contract"]["immutable_sleeves"]["sleeve-00"][
        "trade_ledger_sha256"
    ] = "f" * 64
    with pytest.raises(ActiveRiskShadowPackageError, match="ledger binding drift"):
        reconstruct_active_risk_shadow_package(source_tamper)

    calibration_tamper = package.to_dict()
    calibration_tamper["feature_contract"]["immutable_sleeves"]["sleeve-00"][
        "frozen_signal_binding"
    ]["trigger_threshold"] += 1.0
    with pytest.raises(ActiveRiskShadowPackageError, match="signal binding hash drift"):
        reconstruct_active_risk_shadow_package(calibration_tamper)

    unknown_top_level = package.to_dict()
    unknown_top_level["unbound_extension"] = True
    with pytest.raises(
        ActiveRiskShadowPackageError,
        match="top-level field coverage drift",
    ):
        reconstruct_active_risk_shadow_package(unknown_top_level)


def test_real_revision_02_sleeve_sessions_and_roles_are_preserved() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads(
        (
            root
            / "config/v7/active_risk_pool_target_velocity_0026_revision_02.json"
        ).read_text(encoding="utf-8")
    )
    members = manifest["sleeve_bank"]["members"]
    specs = {
        row["sleeve_id"]: active_package_module._sleeve_from_mapping(
            row["sleeve_specification"]
        )
        for row in members
    }
    records = {
        row["sleeve_id"]: SleeveRecord.from_mapping(row["record"])
        for row in members
    }
    components = {
        row["sleeve_id"]: row["immutable_fingerprint"] for row in members
    }
    policy = _policy(tuple(specs))

    checked_specs, checked_records, checked_components = (
        active_package_module._validate_bound_sources(
            policy, specs, records, components
        )
    )
    assert len(checked_specs) == len(checked_records) == len(checked_components) == 18
    assert {record.session for record in records.values()} <= {
        "ALL_ELIGIBLE",
        "OPEN",
        "MID_SESSION",
        "LATE_CLOSE",
    }
    assert any(
        records[sleeve_id].economic_role != specs[sleeve_id].role.value
        for sleeve_id in specs
    )


def test_builder_rejects_missing_frozen_numeric_signal_binding() -> None:
    specs, records, components = _sources()
    bindings = _signal_bindings(specs, records)
    bindings.pop(next(iter(bindings)))
    policy = _policy(tuple(specs))
    declaration, selection, selection_sha = _selection_chain(
        policy, specs, records
    )
    with pytest.raises(
        ActiveRiskShadowPackageError,
        match="must cover all eighteen sleeves",
    ):
        build_active_risk_shadow_package(
            policy,
            specs,
            records,
            components,
            bindings,
            source_commit="a" * 40,
            decision_report_sealed_at_utc="2026-07-15T12:00:00Z",
            selection_completed_at_utc="2026-07-15T12:01:00Z",
            freeze_timestamp_utc="2026-07-15T12:02:00Z",
            campaign_manifest_sha256="b" * 64,
            evidence_receipt_sha256="e" * 64,
            evidence_manifest_sha256="f" * 64,
            evidence_bundle_sha256="c" * 64,
            decision_report_sha256="d" * 64,
            selection_manifest_sha256=selection_sha,
            selection_seal_receipt_hash="a" * 64,
            frozen_finalist_declaration=declaration,
            selection_manifest=selection,
        )


def _six_forward_packages(tmp_path: Path) -> list[Path]:
    paths = []
    for index in range(6):
        _policy_value, _specs, _records, _components, package = _build(
            policy_id=f"active_pool_forward_{index:02d}"
        )
        machine, _dossier = write_shadow_package(
            package, tmp_path / "forward_exports" / package.candidate_id
        )
        paths.append(machine)
    return paths


def _forward_resolution(roots: tuple[str, ...]) -> ContractResolution:
    suffix = {"ES": "U6", "MES": "U6", "NQ": "U6", "MNQ": "U6", "CL": "Q6", "MCL": "Q6"}
    contracts = tuple(
        ResolvedContract(
            root=root,
            contract=f"{root}{suffix[root]}",
            instrument_id=str(index + 1),
            active_start="2026-07-01T00:00:00+00:00",
            active_end="2026-08-01T00:00:00+00:00",
            expiry_date="2026-09-30T00:00:00+00:00",
            tick_size=0.25,
            point_value=1.0,
            map_path="fixture_roll_map.json",
            map_sha256="1" * 64,
            roll_map_hash="2" * 64,
        )
        for index, root in enumerate(roots)
    )
    return ContractResolution(
        status="READY",
        as_of_utc="2026-07-15T12:04:00+00:00",
        required_roots=roots,
        contracts=contracts,
        missing_roots=(),
        unsafe_roll_roots=(),
        reason="exact_dated_explicit_contracts_resolved",
        next_action=None,
        inspected_maps=(),
    )


def test_active_risk_forward_boundary_binds_exactly_six_packages(
    tmp_path: Path,
) -> None:
    packages = _six_forward_packages(tmp_path)
    manifest = build_active_risk_forward_boundary(
        repository_root=tmp_path,
        package_paths=packages,
        created_at=datetime(2026, 7, 15, 12, 10, tzinfo=timezone.utc),
    )

    validate_active_risk_forward_boundary(manifest, repository_root=tmp_path)
    assert manifest["candidate_count"] == 6
    assert manifest["market_data_purchase_authorized"] is False
    assert manifest["q4_access_authorized"] is False
    assert manifest["outbound_orders"] == 0
    assert all(
        row["required_roots"] == ["CL", "ES", "MCL", "MES", "MNQ", "NQ"]
        for row in manifest["candidates"]
    )

    changed = json.loads(json.dumps(manifest))
    changed["candidates"][0]["package_sha256"] = "f" * 64
    changed.pop("manifest_hash")
    changed["manifest_hash"] = stable_hash(changed)
    with pytest.raises(ActiveRiskForwardBoundaryError, match="package bytes drifted"):
        validate_active_risk_forward_boundary(changed, repository_root=tmp_path)


def test_active_risk_forward_processor_persists_only_warmup_no_action_events(
    tmp_path: Path,
) -> None:
    packages = _six_forward_packages(tmp_path)
    manifest = build_active_risk_forward_boundary(
        repository_root=tmp_path,
        package_paths=packages,
        created_at=datetime(2026, 7, 15, 12, 10, tzinfo=timezone.utc),
    )
    boundary = write_manifest(tmp_path / "mission/state/active_boundary.json", manifest)
    boundary_sha = hashlib.sha256(boundary.read_bytes()).hexdigest()
    roots = tuple(manifest["candidates"][0]["required_roots"])
    resolution = _forward_resolution(roots)
    store_path = tmp_path / "shadow/state/forward_data/forward_bars.db"
    store = ForwardBarStore(store_path)
    observed = datetime(2026, 7, 15, 12, 11, tzinfo=timezone.utc)
    with store.writer(writer_id="fixture_read_only_feed") as writer:
        starts = [datetime(2026, 7, 15, 12, 1, tzinfo=timezone.utc)] + [
            datetime(2026, 7, 15, 12, 2, tzinfo=timezone.utc)
            + timedelta(minutes=index)
            for index in range(8)
        ]
        for minute_index, start in enumerate(starts, start=1):
            for root in roots:
                contract = resolution.contract_for(root).contract
                writer.append(
                    ForwardBar(
                        source_id="fixture_forward_source",
                        root=root,
                        contract=contract,
                        timeframe="1m",
                        bar_start_at_utc=start,
                        bar_close_at_utc=start + timedelta(minutes=1),
                        availability_at_utc=start + timedelta(minutes=1),
                        open=100.0,
                        high=101.0,
                        low=99.0,
                        close=100.5,
                        volume=10.0,
                        source_sequence=minute_index,
                    ),
                    observed_at=observed,
                    resolution=resolution,
                )

    result = run_active_risk_forward_processor(
        repository_root=tmp_path,
        boundary_manifest_path=boundary,
        boundary_manifest_sha256=boundary_sha,
        forward_store_path=store_path,
        state_dir=tmp_path / "shadow/state",
        observed_at=observed,
    )

    assert result["candidate_count"] == 6
    assert result["events_appended"] == 48
    assert result["signals_emitted"] == 0
    assert result["virtual_fills_created"] == 0
    assert result["account_mutations"] == 0
    for candidate in result["candidates"]:
        ledger = Path(candidate["ledger_path"])
        events = [json.loads(line) for line in ledger.read_text().splitlines()]
        assert len(events) == 8
        event = events[0]
        assert event["decision_at_utc"] == "2026-07-15T12:03:00+00:00"
        assert event["decision_status"] == "WARMUP_PENDING"
        assert event["causal_warmup"]["pre_freeze_rows_used"] == 0
        assert event["frozen_feature_contract"][
            "online_feature_equivalence_proven"
        ] is False
        assert event["signal"]["emitted"] is False
        assert event["fill"]["created"] is False
        assert event["account"]["mutated"] is False
        assert event["safety"]["outbound_orders"] == 0
        assert event["safety"]["broker_connections"] == 0
        latest = events[-1]
        assert latest["decision_at_utc"] == "2026-07-15T12:10:00+00:00"
        assert all(
            latest["closed_bar_resampling"]["5m"][root][
                "complete_bar_count"
            ]
            == 1
            for root in roots
        )
        assert all(
            event["closed_bar_resampling"]["5m"][root][
                "complete_bar_count"
            ]
            == 0
            for root in roots
        )

    repeated = run_active_risk_forward_processor(
        repository_root=tmp_path,
        boundary_manifest_path=boundary,
        boundary_manifest_sha256=boundary_sha,
        forward_store_path=store_path,
        state_dir=tmp_path / "shadow/state",
        observed_at=observed,
    )
    assert repeated["events_appended"] == 0
    assert repeated["signals_emitted"] == 0

    tampered_ledger = Path(result["candidates"][0]["ledger_path"])
    tampered_rows = [
        json.loads(line) for line in tampered_ledger.read_text().splitlines()
    ]
    tampered_rows[-1]["signal"]["emitted"] = True
    tampered_rows[-1].pop("event_hash")
    tampered_rows[-1]["event_hash"] = stable_hash(tampered_rows[-1])
    tampered_ledger.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in tampered_rows),
        encoding="utf-8",
    )
    with pytest.raises(
        ActiveRiskForwardProcessorError, match="safety contract drift"
    ):
        run_active_risk_forward_processor(
            repository_root=tmp_path,
            boundary_manifest_path=boundary,
            boundary_manifest_sha256=boundary_sha,
            forward_store_path=store_path,
            state_dir=tmp_path / "shadow/state",
            observed_at=observed,
        )
