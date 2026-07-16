"""Immutable no-order forward package for a frozen active-risk-pool book.

The campaign-0026 Combine book is not a ``BookPair``: it is one complete
``ActiveRiskPoolPolicy`` over eighteen immutable sleeves.  Its XFA replay is a
pre-outcome static projection of that policy, evaluated as two *alternative*
Standard and Consistency rule paths.  This module preserves those semantics in
the existing v4 shadow-package envelope without pretending that either XFA
alternative is an additional realised account.

Package construction is intentionally impossible until the caller supplies a
sealed report time, a completed selection time, and a later (or equal) freeze
time.  There is no clock default and this module never selects a candidate or
opens a forward ledger itself.
"""

from __future__ import annotations

import re
import math
import hashlib
import json
from dataclasses import dataclass, fields
from datetime import date, datetime, timezone
from typing import Any, Mapping

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    policy_from_mapping,
)
from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.production.portfolio_books import SleeveRecord, stable_hash
from hydra.propfirm.combine_to_xfa import (
    FrozenRiskProfile,
    UNREALIZED_AGGREGATION_SEMANTICS,
    official_rule_snapshot_2026_07_15,
)
from hydra.research.turbo_feature_builder import FEATURE_BUNDLE_VERSION, FEATURE_DAG_HASH
from hydra.shadow.package_factory import (
    PACKAGE_SCHEMA,
    ImmutableShadowPackage,
    ShadowPackageError,
)


ACTIVE_RISK_PACKAGE_ROLE = "FORWARD_SHADOW_CANDIDATE"
ACTIVE_RISK_PACKAGE_CONTRACT = "hydra_active_risk_pool_shadow_contract_v1"
ACTIVE_RISK_XFA_OVERLAY_SEMANTICS = (
    "FROZEN_STATIC_XFA_PROFILE_NO_COMBINE_GOVERNOR_CONTROLS_V1"
)
ACTIVE_RISK_COMBINE_HORIZONS = (
    "20_TRADING_DAYS",
    "40_TRADING_DAYS",
    "60_TRADING_DAYS",
    "90_TRADING_DAYS",
    "FULL_CHRONOLOGICAL_HORIZON",
)
ACTIVE_RISK_XFA_HORIZON_DAYS = 120
ACTIVE_RISK_SOURCE_SLEEVE_COUNT = 18
FROZEN_SIGNAL_BINDING_VERSION = "hydra_frozen_sleeve_signal_binding_v1"
FROZEN_BOOK_SELECTION_SCHEMA = "hydra_frozen_book_selection_v1"
FROZEN_BOOK_SELECTION_CAMPAIGN_ID = (
    "hydra_active_risk_pool_target_velocity_0026"
)
FROZEN_BOOK_SELECTION_EVIDENCE_STATUS = (
    "DEVELOPMENT_ONLY_NOT_INDEPENDENT_CONFIRMATION"
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SESSION_LABELS = {
    -1: "ALL_ELIGIBLE",
    0: "OPEN",
    1: "MID_SESSION",
    2: "LATE_CLOSE",
}
_PATHS = ("STANDARD", "CONSISTENCY")


class ActiveRiskShadowPackageError(ShadowPackageError):
    """The active-pool forward package is incomplete, mutable, or unsafe."""


@dataclass(frozen=True, slots=True)
class FrozenSignalBinding:
    """Numeric signal calibration and immutable matrix provenance for one sleeve."""

    sleeve_id: str
    trigger_feature: str
    trigger_operator: str
    trigger_threshold: float
    context_feature: str | None
    context_operator: str | None
    context_threshold: float | None
    calibration_start: str
    calibration_end_exclusive: str
    trigger_finite_observation_count: int
    context_finite_observation_count: int | None
    source_execution_fingerprint: str
    source_cheap_screen_path: str
    source_cheap_screen_sha256: str
    source_cheap_screen_row_sha256: str
    feature_matrix_manifest_path: str
    feature_matrix_manifest_sha256: str
    feature_matrix_schema: str
    feature_matrix_bundle_hash: str
    feature_matrix_source_data_sha256: str
    feature_matrix_roll_map_sha256: str
    feature_matrix_market: str
    feature_matrix_execution_market: str
    feature_bundle_version: str
    feature_dag_hash: str
    trigger_array_sha256: str
    context_array_sha256: str | None
    session_day_array_sha256: str
    session_code_array_sha256: str
    no_forward_recalibration: bool = True
    binding_version: str = FROZEN_SIGNAL_BINDING_VERSION

    def __post_init__(self) -> None:
        if (
            not self.sleeve_id
            or not self.trigger_feature
            or not self.trigger_operator
            or not math.isfinite(float(self.trigger_threshold))
            or self.trigger_finite_observation_count < 100
            or self.binding_version != FROZEN_SIGNAL_BINDING_VERSION
            or self.no_forward_recalibration is not True
        ):
            raise ActiveRiskShadowPackageError("frozen signal binding is incomplete")
        try:
            start = date.fromisoformat(self.calibration_start)
            end = date.fromisoformat(self.calibration_end_exclusive)
        except ValueError as exc:
            raise ActiveRiskShadowPackageError(
                "frozen signal calibration dates are invalid"
            ) from exc
        if end <= start:
            raise ActiveRiskShadowPackageError(
                "frozen signal calibration interval is empty"
            )
        if self.context_feature is None:
            if any(
                value is not None
                for value in (
                    self.context_operator,
                    self.context_threshold,
                    self.context_finite_observation_count,
                    self.context_array_sha256,
                )
            ):
                raise ActiveRiskShadowPackageError(
                    "context-free signal binding contains context calibration"
                )
        elif (
            not self.context_operator
            or self.context_threshold is None
            or not math.isfinite(float(self.context_threshold))
            or self.context_finite_observation_count is None
            or self.context_finite_observation_count < 100
            or self.context_array_sha256 is None
        ):
            raise ActiveRiskShadowPackageError(
                "context signal binding lacks numeric calibration"
            )
        for label, value in (
            ("source_execution_fingerprint", self.source_execution_fingerprint),
            ("source_cheap_screen_sha256", self.source_cheap_screen_sha256),
            ("source_cheap_screen_row_sha256", self.source_cheap_screen_row_sha256),
            ("feature_matrix_manifest_sha256", self.feature_matrix_manifest_sha256),
            ("feature_matrix_bundle_hash", self.feature_matrix_bundle_hash),
            ("feature_matrix_source_data_sha256", self.feature_matrix_source_data_sha256),
            ("feature_matrix_roll_map_sha256", self.feature_matrix_roll_map_sha256),
            ("feature_dag_hash", self.feature_dag_hash),
            ("trigger_array_sha256", self.trigger_array_sha256),
            ("session_day_array_sha256", self.session_day_array_sha256),
            ("session_code_array_sha256", self.session_code_array_sha256),
        ):
            _require_sha256(label, value)
        if self.context_array_sha256 is not None:
            _require_sha256("context_array_sha256", self.context_array_sha256)
        for label, value in (
            ("source_cheap_screen_path", self.source_cheap_screen_path),
            ("feature_matrix_manifest_path", self.feature_matrix_manifest_path),
        ):
            path = value.replace("\\", "/")
            if not path or path.startswith("/") or ".." in path.split("/"):
                raise ActiveRiskShadowPackageError(
                    f"{label} must be a safe repository-relative path"
                )

    @property
    def fingerprint(self) -> str:
        return stable_hash(self.semantic_payload())

    def semantic_payload(self) -> dict[str, Any]:
        return {
            field.name: getattr(self, field.name)
            for field in fields(FrozenSignalBinding)
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.semantic_payload(), "fingerprint": self.fingerprint}

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "FrozenSignalBinding":
        expected = {field.name for field in fields(cls)} | {"fingerprint"}
        if set(raw) != expected:
            raise ActiveRiskShadowPackageError(
                "frozen signal binding field coverage drift"
            )
        values = {field.name: raw[field.name] for field in fields(cls)}
        try:
            binding = cls(**values)
        except (TypeError, ValueError) as exc:
            raise ActiveRiskShadowPackageError(
                "frozen signal binding cannot be reconstructed"
            ) from exc
        if raw.get("fingerprint") != binding.fingerprint:
            raise ActiveRiskShadowPackageError("frozen signal binding hash drift")
        return binding


@dataclass(frozen=True)
class ImmutableActiveRiskShadowPackage(ImmutableShadowPackage):
    """The generic v4 envelope with active-pool fail-closed validation."""

    def validate(self) -> None:
        super().validate()
        _validate_active_risk_package(self)


@dataclass(frozen=True, slots=True)
class ReconstructedActiveRiskShadowPackage:
    """Typed executable declarations reconstructed from canonical JSON."""

    package: ImmutableActiveRiskShadowPackage
    combine_policy: ActiveRiskPoolPolicy
    xfa_profile: FrozenRiskProfile
    sleeve_specs: Mapping[str, SleeveSpec]
    sleeve_records: Mapping[str, SleeveRecord]
    frozen_signal_bindings: Mapping[str, FrozenSignalBinding]


def build_active_risk_shadow_package(
    combine_policy: ActiveRiskPoolPolicy,
    sleeve_specs: Mapping[str, SleeveSpec],
    sleeve_records: Mapping[str, SleeveRecord],
    component_fingerprints: Mapping[str, str],
    frozen_signal_bindings: Mapping[str, FrozenSignalBinding],
    *,
    source_commit: str,
    decision_report_sealed_at_utc: str,
    selection_completed_at_utc: str,
    freeze_timestamp_utc: str,
    campaign_manifest_sha256: str,
    evidence_receipt_sha256: str,
    evidence_manifest_sha256: str,
    evidence_bundle_sha256: str,
    decision_report_sha256: str,
    selection_manifest_sha256: str,
    selection_seal_receipt_hash: str,
    frozen_finalist_declaration: Mapping[str, Any],
    selection_manifest: Mapping[str, Any],
) -> ImmutableActiveRiskShadowPackage:
    """Build a reconstructible development-only package after selection.

    ``component_fingerprints`` is the sealed EvidenceBundle identity mapping:
    one component fingerprint for each source sleeve.  It must match the
    corresponding ``SleeveRecord.immutable_fingerprint``.  Keeping it as an
    explicit input prevents a package from silently inventing its evidence
    binding from the executable specification.
    """

    if not isinstance(combine_policy, ActiveRiskPoolPolicy):
        raise ActiveRiskShadowPackageError("combine_policy must be frozen and typed")
    if _GIT_COMMIT.fullmatch(str(source_commit)) is None:
        raise ActiveRiskShadowPackageError("source_commit must be a lowercase Git SHA")
    for name, value in (
        ("campaign_manifest_sha256", campaign_manifest_sha256),
        ("evidence_receipt_sha256", evidence_receipt_sha256),
        ("evidence_manifest_sha256", evidence_manifest_sha256),
        ("evidence_bundle_sha256", evidence_bundle_sha256),
        ("decision_report_sha256", decision_report_sha256),
        ("selection_manifest_sha256", selection_manifest_sha256),
        ("selection_seal_receipt_hash", selection_seal_receipt_hash),
    ):
        _require_sha256(name, value)
    report_time = _utc_timestamp(
        "decision_report_sealed_at_utc", decision_report_sealed_at_utc
    )
    selection_time = _utc_timestamp(
        "selection_completed_at_utc", selection_completed_at_utc
    )
    freeze_time = _utc_timestamp("freeze_timestamp_utc", freeze_timestamp_utc)
    if selection_time < report_time or freeze_time < selection_time:
        raise ActiveRiskShadowPackageError(
            "selection must follow the sealed report and forward freeze must follow selection"
        )

    specs, records, evidence_components = _validate_bound_sources(
        combine_policy,
        sleeve_specs,
        sleeve_records,
        component_fingerprints,
    )
    signal_bindings = _validate_signal_bindings(
        combine_policy,
        specs,
        records,
        frozen_signal_bindings,
    )
    selection_binding = _validate_frozen_selection_binding(
        combine_policy,
        specs,
        records,
        frozen_finalist_declaration=frozen_finalist_declaration,
        selection_manifest=selection_manifest,
        decision_report_sha256=decision_report_sha256,
        selection_manifest_sha256=selection_manifest_sha256,
        decision_report_sealed_at_utc=decision_report_sealed_at_utc,
        selection_completed_at_utc=selection_completed_at_utc,
    )
    sleeve_ids = combine_policy.component_priority
    xfa_profile = _static_xfa_profile(combine_policy)
    rules = official_rule_snapshot_2026_07_15()

    immutable_sleeves = {
        sleeve_id: {
            "sleeve_specification": specs[sleeve_id].to_dict(),
            "sleeve_specification_sha256": stable_hash(
                specs[sleeve_id].to_dict()
            ),
            "sleeve_record": records[sleeve_id].to_dict(),
            "sleeve_record_sha256": records[sleeve_id].record_fingerprint,
            "immutable_fingerprint": records[sleeve_id].immutable_fingerprint,
            "component_fingerprint": evidence_components[sleeve_id],
            "behavioral_fingerprint": records[sleeve_id].behavioral_fingerprint,
            "signal_ledger_sha256": records[sleeve_id].signal_ledger_sha256,
            "trade_ledger_sha256": records[sleeve_id].trade_ledger_sha256,
            "source_role": specs[sleeve_id].role.value,
            "portfolio_role": records[sleeve_id].economic_role,
            "frozen_signal_binding": signal_bindings[sleeve_id].to_dict(),
            "frozen_signal_binding_sha256": signal_bindings[
                sleeve_id
            ].fingerprint,
        }
        for sleeve_id in sleeve_ids
    }
    entries = {
        sleeve_id: {
            "decision_bar": "CLOSED_TIMEFRAME_BAR",
            "entry_bar": "NEXT_ELIGIBLE_BAR",
            "signal_market": specs[sleeve_id].market,
            "execution_market": specs[sleeve_id].execution_market,
            "timeframe": specs[sleeve_id].timeframe,
            "side": specs[sleeve_id].side,
            "trigger_feature": specs[sleeve_id].trigger_feature,
            "trigger_operator": specs[sleeve_id].trigger_operator,
            "trigger_quantile": specs[sleeve_id].trigger_quantile,
            "trigger_threshold": signal_bindings[sleeve_id].trigger_threshold,
            "context_feature": specs[sleeve_id].context_feature,
            "context_operator": specs[sleeve_id].context_operator,
            "context_quantile": specs[sleeve_id].context_quantile,
            "context_threshold": signal_bindings[sleeve_id].context_threshold,
            "calibration_start": signal_bindings[sleeve_id].calibration_start,
            "calibration_end_exclusive": signal_bindings[
                sleeve_id
            ].calibration_end_exclusive,
            "signal_calibration_fingerprint": signal_bindings[
                sleeve_id
            ].fingerprint,
            "threshold_mode": "FROZEN_NUMERIC_NO_FORWARD_RECALIBRATION",
            "underlying_signal_mutated": False,
        }
        for sleeve_id in sleeve_ids
    }
    exits = {
        sleeve_id: {
            "exit_style": specs[sleeve_id].exit_style,
            "holding_bars": specs[sleeve_id].holding_bars,
            "holding_timeframe": specs[sleeve_id].timeframe,
            "mandatory_session_flatten": True,
            "underlying_exit_mutated": False,
        }
        for sleeve_id in sleeve_ids
    }
    sessions = {
        sleeve_id: {
            "session_code": specs[sleeve_id].session_code,
            "session_label": _SESSION_LABELS[specs[sleeve_id].session_code],
            "mandatory_flatten": True,
        }
        for sleeve_id in sleeve_ids
    }
    xfa_books: dict[str, dict[str, Any]] = {}
    for path in _PATHS:
        book = {
            "path": path,
            "sleeve_ids": list(sleeve_ids),
            "profile": xfa_profile.to_dict(),
            "horizon_days": ACTIVE_RISK_XFA_HORIZON_DAYS,
            "overlay_semantics": ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
            "combine_governor_controls_applied": False,
            "profile_selected_from_outcomes": False,
            "combine_profit_transferred": False,
            "fresh_balance_usd": 0.0,
        }
        xfa_books[path] = {**book, "book_fingerprint": stable_hash(book)}
    combine_book = {
        "policy": combine_policy.to_dict(),
        "policy_structural_fingerprint": combine_policy.structural_fingerprint,
        "sleeve_ids": list(sleeve_ids),
        "horizons": list(ACTIVE_RISK_COMBINE_HORIZONS),
    }
    combine_book["book_fingerprint"] = stable_hash(combine_book)

    package = ImmutableActiveRiskShadowPackage(
        candidate_id=combine_policy.policy_id,
        candidate_specification_hash=combine_policy.structural_fingerprint,
        source_commit=source_commit,
        freeze_timestamp_utc=freeze_timestamp_utc,
        role=ACTIVE_RISK_PACKAGE_ROLE,
        market_policy={
            "active_risk_contract": ACTIVE_RISK_PACKAGE_CONTRACT,
            "sleeves": {
                sleeve_id: {
                    "signal_market": specs[sleeve_id].market,
                    "execution_market": specs[sleeve_id].execution_market,
                    "explicit_contract": records[sleeve_id].contract,
                    "component_ids": list(specs[sleeve_id].component_ids),
                }
                for sleeve_id in sleeve_ids
            },
            "explicit_contracts_required": True,
            "date_aware_contract_resolution_required": True,
            "contract_mismatch_action": "REJECT_AND_FAIL_CLOSED",
            "same_instrument_conflict_policy": (
                combine_policy.same_instrument_conflict_rule.value
            ),
        },
        timeframe_profile=tuple(
            sorted({specs[sleeve_id].timeframe for sleeve_id in sleeve_ids})
        ),
        feature_contract={
            "feature_bundle_version": FEATURE_BUNDLE_VERSION,
            "feature_dag_hash": FEATURE_DAG_HASH,
            "immutable_sleeves": immutable_sleeves,
            "source_sleeve_count": ACTIVE_RISK_SOURCE_SLEEVE_COUNT,
            "closed_bars_only": True,
            "source_close_must_precede_decision": True,
            "availability_must_not_exceed_decision": True,
            "higher_timeframe_partial_bars_prohibited": True,
            "forward_threshold_recalibration_prohibited": True,
        },
        entry_policy={
            "type": "IMMUTABLE_INDEPENDENT_SLEEVE_ENTRIES",
            "sleeves": entries,
            "priority_order": list(sleeve_ids),
            "same_instrument_conflict_policy": (
                combine_policy.same_instrument_conflict_rule.value
            ),
            "underlying_signals_blended": False,
        },
        exit_policy={
            "type": "IMMUTABLE_INDEPENDENT_SLEEVE_EXITS",
            "sleeves": exits,
            "mandatory_session_flatten": True,
            "cross_contract_holding_prohibited": True,
        },
        sizing_policy={
            "type": "ACTIVE_RISK_COMBINE_AND_TWO_STATIC_XFA_ALTERNATIVES",
            "combine_book": combine_book,
            "xfa_books": xfa_books,
            "xfa_paths_are_mutually_exclusive_alternatives": True,
            "dynamic_resizing": False,
            "outcome_selected_resizing": False,
        },
        session_policy={
            "calendar": "CME_GLOBEX_AMERICA_CHICAGO",
            "trading_day_from_local_17_00": True,
            "dst_aware": True,
            "sleeves": sessions,
            "mandatory_flatten": True,
        },
        cost_policy={
            "normal": {
                "source_trade_ledger_costs": True,
                "multiplier": 1.0,
            },
            "stressed": {
                "source_trade_ledger_costs": True,
                "multiplier": 1.5,
            },
            "costs_frozen_before_forward_observation": True,
            "virtual_fill_slippage": "ADVERSE_ONE_TICK_OR_WORSE",
        },
        risk_policy={
            "topstep_rule_snapshot": rules.to_dict(),
            "topstep_rule_snapshot_fingerprint": rules.fingerprint,
            "combine_governor": combine_policy.to_dict(),
            "combine_governor_structural_fingerprint": (
                combine_policy.structural_fingerprint
            ),
            "xfa_static_profile": xfa_profile.to_dict(),
            "xfa_overlay_semantics": ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
            "simulated_mll_floor_usd": -rules.maximum_loss_limit,
            "shared_account_contract_limit": rules.combine_maximum_mini_equivalent,
            "shared_mll": True,
            "shared_consistency": True,
            "combine_profit_transfers_to_xfa": False,
            "xfa_starts_at_zero": True,
            "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
            "fail_closed": True,
        },
        data_policy={
            "post_freeze_only": True,
            "decision_report_sealed_at_utc": decision_report_sealed_at_utc,
            "selection_completed_at_utc": selection_completed_at_utc,
            "freeze_timestamp_utc": freeze_timestamp_utc,
            "freeze_basis": "CALLER_SUPPLIED_AFTER_REPORT_AND_SELECTION",
            "first_eligible_bar_close_operator": "STRICTLY_GREATER_THAN_FREEZE",
            "fresh_completed_bars_only": True,
            "expected_bar_seconds": 60,
            "stale_after_seconds": 180,
            "duplicate_bars_rejected": True,
            "duplicate_source_sequences_rejected": True,
            "missing_intervals_rejected": True,
            "out_of_order_bars_rejected": True,
            "contract_mismatches_rejected": True,
            "append_only": True,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        },
        signal_policy={
            "deterministic": True,
            "frozen_numeric_thresholds_required": True,
            "forward_threshold_recalibration_prohibited": True,
            "independent_sleeve_signals": True,
            "duplicate_signal_guard": True,
            "duplicate_window_seconds": 60,
            "stale_data_action": "REJECT_SIGNAL_AND_FAIL_CLOSED",
            "missing_data_action": "REJECT_SIGNAL_AND_FAIL_CLOSED",
            "contract_failure_action": "REJECT_SIGNAL_AND_FAIL_CLOSED",
            "same_instrument_conflict_policy": (
                combine_policy.same_instrument_conflict_rule.value
            ),
            "signal_ledger_sha256": {
                sleeve_id: records[sleeve_id].signal_ledger_sha256
                for sleeve_id in sleeve_ids
            },
            "trade_ledger_sha256": {
                sleeve_id: records[sleeve_id].trade_ledger_sha256
                for sleeve_id in sleeve_ids
            },
        },
        virtual_fill_policy={
            "mode": "CONSERVATIVE_VIRTUAL_ONLY",
            "entry": "NEXT_BAR_OPEN_PLUS_ADVERSE_SLIPPAGE",
            "exit": "FROZEN_SLEEVE_EXIT_MINUS_ADVERSE_SLIPPAGE",
            "same_bar_ambiguity": "ADVERSE_PATH",
            "real_order_submission": False,
            "broker_route": None,
        },
        kill_conditions=(
            "bar_close_not_strictly_post_freeze",
            "stale_or_missing_data",
            "duplicate_or_out_of_order_bar",
            "duplicate_signal",
            "contract_resolution_or_mapping_failure",
            "clock_or_session_mismatch",
            "policy_or_source_fingerprint_mismatch",
            "manifest_bundle_report_or_selection_hash_mismatch",
            "simulated_mll_floor_reached",
        ),
        observability={
            "ledger_path": (
                f"shadow/state/forward/{combine_policy.policy_id}/"
                "forward_evidence.jsonl"
            ),
            "status": ACTIVE_RISK_PACKAGE_ROLE,
            "paper_shadow_ready": False,
            "reconciliation_required": True,
            "sleeve_attribution_required": True,
            "combine_to_xfa_transition_ledger_required": True,
            "xfa_alternatives_reported_separately": True,
            "required_fields": [
                "bar_close_at_utc",
                "source_sequence",
                "contract",
                "sleeve_id",
                "signal_timestamp",
                "theoretical_entry",
                "virtual_entry",
                "virtual_exit",
                "virtual_pnl_usd",
                "shared_mll_buffer_usd",
                "account_state",
            ],
        },
        evidence_provenance={
            "campaign_manifest_sha256": campaign_manifest_sha256,
            "evidence_receipt_sha256": evidence_receipt_sha256,
            "evidence_manifest_sha256": evidence_manifest_sha256,
            "evidence_bundle_sha256": evidence_bundle_sha256,
            "decision_report_sha256": decision_report_sha256,
            "selection_manifest_sha256": selection_manifest_sha256,
            "selection_seal_receipt_hash": selection_seal_receipt_hash,
            "decision_report_hash": selection_binding["decision_report_hash"],
            "decision_report_seal_receipt_hash": selection_binding[
                "decision_report_seal_receipt_hash"
            ],
            "selection_manifest_hash": selection_binding[
                "selection_manifest_hash"
            ],
            "selection_entry_hash": selection_binding["selection_entry_hash"],
            "selection_role": selection_binding["selection_role"],
            "expanded_exact_account_behavior_cluster": selection_binding[
                "expanded_exact_account_behavior_cluster"
            ],
            "expanded_economic_behavior_cluster": selection_binding[
                "expanded_economic_behavior_cluster"
            ],
            "active_risk_policy_sha256": selection_binding[
                "active_risk_policy_sha256"
            ],
            "membership_sha256": selection_binding["membership_sha256"],
            "combine_book_sha256": selection_binding["combine_book_sha256"],
            "xfa_standard_book_sha256": selection_binding[
                "xfa_standard_book_sha256"
            ],
            "xfa_consistency_book_sha256": selection_binding[
                "xfa_consistency_book_sha256"
            ],
            "frozen_finalist_declaration": dict(frozen_finalist_declaration),
            "selection_manifest": dict(selection_manifest),
            "combine_policy_structural_fingerprint": (
                combine_policy.structural_fingerprint
            ),
            "component_fingerprints": dict(evidence_components),
            "source_sleeve_count": ACTIVE_RISK_SOURCE_SLEEVE_COUNT,
            "decision_report_sealed_at_utc": decision_report_sealed_at_utc,
            "selection_completed_at_utc": selection_completed_at_utc,
            "freeze_timestamp_utc": freeze_timestamp_utc,
            "freeze_basis": "CALLER_SUPPLIED_AFTER_REPORT_AND_SELECTION",
            "development_only": True,
            "independently_confirmed": False,
            "status": ACTIVE_RISK_PACKAGE_ROLE,
            "paper_shadow_ready": False,
            "q4_evidence_inherited": False,
        },
        broker_connectivity=False,
        outbound_order_capability=False,
    )
    package.validate()
    return package


def reconstruct_active_risk_shadow_package(
    payload: Mapping[str, Any],
) -> ReconstructedActiveRiskShadowPackage:
    """Rebuild every executable source declaration and verify all bindings."""

    if str(payload.get("schema") or "") != PACKAGE_SCHEMA:
        raise ActiveRiskShadowPackageError("active-risk shadow package schema drift")
    field_names = {field.name for field in fields(ImmutableShadowPackage)}
    if set(payload) != field_names | {"schema", "package_hash"}:
        raise ActiveRiskShadowPackageError(
            "active-risk shadow package top-level field coverage drift"
        )
    values = {name: payload[name] for name in field_names}
    values["timeframe_profile"] = tuple(values["timeframe_profile"])
    values["kill_conditions"] = tuple(values["kill_conditions"])
    package = ImmutableActiveRiskShadowPackage(**values)
    package.validate()
    if str(payload.get("package_hash") or "") != package.package_hash:
        raise ActiveRiskShadowPackageError("active-risk shadow package hash drift")

    try:
        policy = policy_from_mapping(package.sizing_policy["combine_book"]["policy"])
        source_rows = package.feature_contract["immutable_sleeves"]
        profile = _profile_from_mapping(package.risk_policy["xfa_static_profile"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskShadowPackageError(
            "active-risk package lacks reconstructible policy or sources"
        ) from exc
    if not isinstance(source_rows, Mapping):
        raise ActiveRiskShadowPackageError("immutable_sleeves must be a mapping")
    specs = {
        str(sleeve_id): _sleeve_from_mapping(row["sleeve_specification"])
        for sleeve_id, row in source_rows.items()
    }
    records = {
        str(sleeve_id): SleeveRecord.from_mapping(row["sleeve_record"])
        for sleeve_id, row in source_rows.items()
    }
    components = {
        str(sleeve_id): str(row["component_fingerprint"])
        for sleeve_id, row in source_rows.items()
    }
    signal_bindings = {
        str(sleeve_id): FrozenSignalBinding.from_mapping(
            row["frozen_signal_binding"]
        )
        for sleeve_id, row in source_rows.items()
    }
    _validate_bound_sources(policy, specs, records, components)
    _validate_signal_bindings(policy, specs, records, signal_bindings)
    if profile != _static_xfa_profile(policy):
        raise ActiveRiskShadowPackageError("reconstructed XFA profile drift")
    return ReconstructedActiveRiskShadowPackage(
        package=package,
        combine_policy=policy,
        xfa_profile=profile,
        sleeve_specs=specs,
        sleeve_records=records,
        frozen_signal_bindings=signal_bindings,
    )


def _validate_active_risk_package(package: ImmutableActiveRiskShadowPackage) -> None:
    if package.role != ACTIVE_RISK_PACKAGE_ROLE:
        raise ActiveRiskShadowPackageError("active-risk forward status drift")
    if _GIT_COMMIT.fullmatch(package.source_commit) is None:
        raise ActiveRiskShadowPackageError("active-risk source commit drift")
    if (
        package.observability.get("paper_shadow_ready") is not False
        or package.evidence_provenance.get("paper_shadow_ready") is not False
    ):
        raise ActiveRiskShadowPackageError("development package claimed PAPER_SHADOW_READY")
    for field_name in (
        "campaign_manifest_sha256",
        "evidence_receipt_sha256",
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
        "decision_report_sha256",
        "selection_manifest_sha256",
        "selection_seal_receipt_hash",
        "decision_report_hash",
        "decision_report_seal_receipt_hash",
        "selection_manifest_hash",
        "selection_entry_hash",
        "active_risk_policy_sha256",
        "membership_sha256",
        "combine_book_sha256",
        "xfa_standard_book_sha256",
        "xfa_consistency_book_sha256",
    ):
        _require_sha256(
            field_name, str(package.evidence_provenance.get(field_name) or "")
        )

    report_value = str(package.data_policy.get("decision_report_sealed_at_utc") or "")
    selection_value = str(package.data_policy.get("selection_completed_at_utc") or "")
    freeze_value = str(package.data_policy.get("freeze_timestamp_utc") or "")
    report_time = _utc_timestamp("decision_report_sealed_at_utc", report_value)
    selection_time = _utc_timestamp("selection_completed_at_utc", selection_value)
    freeze_time = _utc_timestamp("freeze_timestamp_utc", freeze_value)
    if (
        selection_time < report_time
        or freeze_time < selection_time
        or freeze_value != package.freeze_timestamp_utc
        or package.data_policy.get("freeze_basis")
        != "CALLER_SUPPLIED_AFTER_REPORT_AND_SELECTION"
        or package.data_policy.get("first_eligible_bar_close_operator")
        != "STRICTLY_GREATER_THAN_FREEZE"
    ):
        raise ActiveRiskShadowPackageError("active-risk post-selection freeze drift")
    provenance = package.evidence_provenance
    if (
        provenance.get("decision_report_sealed_at_utc") != report_value
        or provenance.get("selection_completed_at_utc") != selection_value
        or provenance.get("freeze_timestamp_utc") != freeze_value
        or provenance.get("freeze_basis")
        != "CALLER_SUPPLIED_AFTER_REPORT_AND_SELECTION"
        or provenance.get("development_only") is not True
        or provenance.get("independently_confirmed") is not False
        or provenance.get("status") != ACTIVE_RISK_PACKAGE_ROLE
        or provenance.get("q4_evidence_inherited") is not False
    ):
        raise ActiveRiskShadowPackageError("active-risk evidence provenance drift")
    if any(
        package.data_policy.get(name) is not expected
        for name, expected in (
            ("post_freeze_only", True),
            ("fresh_completed_bars_only", True),
            ("duplicate_bars_rejected", True),
            ("duplicate_source_sequences_rejected", True),
            ("missing_intervals_rejected", True),
            ("out_of_order_bars_rejected", True),
            ("contract_mismatches_rejected", True),
            ("q4_access_authorized", False),
            ("new_data_purchase_authorized", False),
        )
    ):
        raise ActiveRiskShadowPackageError("unsafe active-risk forward data policy")
    if (
        package.feature_contract.get("forward_threshold_recalibration_prohibited")
        is not True
        or package.signal_policy.get("frozen_numeric_thresholds_required")
        is not True
        or package.signal_policy.get("forward_threshold_recalibration_prohibited")
        is not True
    ):
        raise ActiveRiskShadowPackageError(
            "forward signal threshold recalibration is not fail-closed"
        )
    if (
        package.broker_connectivity
        or package.outbound_order_capability
        or package.virtual_fill_policy.get("mode") != "CONSERVATIVE_VIRTUAL_ONLY"
        or package.virtual_fill_policy.get("real_order_submission") is not False
        or package.virtual_fill_policy.get("broker_route") is not None
        or package.sizing_policy.get("dynamic_resizing") is not False
        or package.sizing_policy.get("outcome_selected_resizing") is not False
    ):
        raise ActiveRiskShadowPackageError("active-risk package exposes unsafe execution")
    if package.cost_policy != {
        "normal": {"source_trade_ledger_costs": True, "multiplier": 1.0},
        "stressed": {"source_trade_ledger_costs": True, "multiplier": 1.5},
        "costs_frozen_before_forward_observation": True,
        "virtual_fill_slippage": "ADVERSE_ONE_TICK_OR_WORSE",
    }:
        raise ActiveRiskShadowPackageError("active-risk normal/stressed cost drift")

    try:
        combine = package.sizing_policy["combine_book"]
        policy = policy_from_mapping(combine["policy"])
        source_rows = package.feature_contract["immutable_sleeves"]
        profile = _profile_from_mapping(package.risk_policy["xfa_static_profile"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskShadowPackageError(
            "active-risk executable declaration missing"
        ) from exc
    if (
        policy.policy_id != package.candidate_id
        or policy.structural_fingerprint != package.candidate_specification_hash
        or combine.get("policy_structural_fingerprint")
        != policy.structural_fingerprint
        or tuple(combine.get("sleeve_ids") or ()) != policy.component_priority
        or tuple(combine.get("horizons") or ()) != ACTIVE_RISK_COMBINE_HORIZONS
        or combine.get("book_fingerprint")
        != stable_hash(
            {key: value for key, value in combine.items() if key != "book_fingerprint"}
        )
        or package.risk_policy.get("combine_governor_structural_fingerprint")
        != policy.structural_fingerprint
        or package.risk_policy.get("combine_governor") != policy.to_dict()
    ):
        raise ActiveRiskShadowPackageError("frozen Combine governor drift")
    expected_profile = _static_xfa_profile(policy)
    if profile != expected_profile:
        raise ActiveRiskShadowPackageError("frozen static XFA profile drift")

    rules = official_rule_snapshot_2026_07_15()
    if (
        package.risk_policy.get("topstep_rule_snapshot") != rules.to_dict()
        or package.risk_policy.get("topstep_rule_snapshot_fingerprint")
        != rules.fingerprint
        or package.risk_policy.get("xfa_overlay_semantics")
        != ACTIVE_RISK_XFA_OVERLAY_SEMANTICS
        or package.risk_policy.get("combine_profit_transfers_to_xfa") is not False
        or package.risk_policy.get("xfa_starts_at_zero") is not True
        or package.risk_policy.get("shared_mll") is not True
        or package.risk_policy.get("shared_consistency") is not True
        or float(package.risk_policy.get("simulated_mll_floor_usd") or 0.0)
        != -rules.maximum_loss_limit
        or int(package.risk_policy.get("shared_account_contract_limit") or 0)
        != rules.combine_maximum_mini_equivalent
        or package.risk_policy.get("unrealized_aggregation_semantics")
        != UNREALIZED_AGGREGATION_SEMANTICS
    ):
        raise ActiveRiskShadowPackageError("active-risk account-rule snapshot drift")
    _validate_xfa_books(package, policy, profile)

    if not isinstance(source_rows, Mapping):
        raise ActiveRiskShadowPackageError("immutable source declaration must be a mapping")
    specs: dict[str, SleeveSpec] = {}
    records: dict[str, SleeveRecord] = {}
    components: dict[str, str] = {}
    signal_bindings: dict[str, FrozenSignalBinding] = {}
    for sleeve_id, raw in source_rows.items():
        try:
            spec = _sleeve_from_mapping(raw["sleeve_specification"])
            record = SleeveRecord.from_mapping(raw["sleeve_record"])
            spec_sha = str(raw["sleeve_specification_sha256"])
            record_sha = str(raw["sleeve_record_sha256"])
            component = str(raw["component_fingerprint"])
            signal_binding = FrozenSignalBinding.from_mapping(
                raw["frozen_signal_binding"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ActiveRiskShadowPackageError(
                f"incomplete immutable source declaration: {sleeve_id}"
            ) from exc
        if (
            spec.sleeve_id != str(sleeve_id)
            or record.sleeve_id != str(sleeve_id)
            or stable_hash(spec.to_dict()) != spec_sha
            or record.record_fingerprint != record_sha
            or raw.get("immutable_fingerprint") != record.immutable_fingerprint
            or raw.get("behavioral_fingerprint") != record.behavioral_fingerprint
            or raw.get("signal_ledger_sha256") != record.signal_ledger_sha256
            or raw.get("trade_ledger_sha256") != record.trade_ledger_sha256
            or raw.get("source_role") != spec.role.value
            or raw.get("portfolio_role") != record.economic_role
            or raw.get("frozen_signal_binding_sha256")
            != signal_binding.fingerprint
        ):
            raise ActiveRiskShadowPackageError(
                f"source specification or ledger binding drift: {sleeve_id}"
            )
        specs[str(sleeve_id)] = spec
        records[str(sleeve_id)] = record
        components[str(sleeve_id)] = component
        signal_bindings[str(sleeve_id)] = signal_binding
    _validate_bound_sources(policy, specs, records, components)
    _validate_signal_bindings(policy, specs, records, signal_bindings)
    selection_binding = _validate_frozen_selection_binding(
        policy,
        specs,
        records,
        frozen_finalist_declaration=(
            package.evidence_provenance.get("frozen_finalist_declaration") or {}
        ),
        selection_manifest=(
            package.evidence_provenance.get("selection_manifest") or {}
        ),
        decision_report_sha256=str(
            package.evidence_provenance.get("decision_report_sha256") or ""
        ),
        selection_manifest_sha256=str(
            package.evidence_provenance.get("selection_manifest_sha256") or ""
        ),
        decision_report_sealed_at_utc=str(
            package.evidence_provenance.get("decision_report_sealed_at_utc") or ""
        ),
        selection_completed_at_utc=str(
            package.evidence_provenance.get("selection_completed_at_utc") or ""
        ),
    )
    for field_name, value in selection_binding.items():
        if package.evidence_provenance.get(field_name) != value:
            raise ActiveRiskShadowPackageError(
                f"packaged selection provenance drift: {field_name}"
            )
    if (
        package.feature_contract.get("source_sleeve_count")
        != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or package.evidence_provenance.get("source_sleeve_count")
        != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or package.evidence_provenance.get("component_fingerprints") != components
        or package.evidence_provenance.get(
            "combine_policy_structural_fingerprint"
        )
        != policy.structural_fingerprint
    ):
        raise ActiveRiskShadowPackageError("evidence component fingerprint drift")
    _validate_redundant_sleeve_views(
        package, policy, specs, records, signal_bindings
    )


def _validate_xfa_books(
    package: ImmutableActiveRiskShadowPackage,
    policy: ActiveRiskPoolPolicy,
    profile: FrozenRiskProfile,
) -> None:
    books = package.sizing_policy.get("xfa_books")
    if (
        not isinstance(books, Mapping)
        or set(books) != set(_PATHS)
        or package.sizing_policy.get("xfa_paths_are_mutually_exclusive_alternatives")
        is not True
    ):
        raise ActiveRiskShadowPackageError("XFA alternatives are not exactly frozen")
    expected_common = {
        "sleeve_ids": list(policy.component_priority),
        "profile": profile.to_dict(),
        "horizon_days": ACTIVE_RISK_XFA_HORIZON_DAYS,
        "overlay_semantics": ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
        "combine_governor_controls_applied": False,
        "profile_selected_from_outcomes": False,
        "combine_profit_transferred": False,
        "fresh_balance_usd": 0.0,
    }
    for path in _PATHS:
        expected = {"path": path, **expected_common}
        expected["book_fingerprint"] = stable_hash(expected)
        if dict(books[path]) != expected:
            raise ActiveRiskShadowPackageError(f"frozen XFA {path} book drift")


def _validate_frozen_selection_binding(
    policy: ActiveRiskPoolPolicy,
    specs: Mapping[str, SleeveSpec],
    records: Mapping[str, SleeveRecord],
    *,
    frozen_finalist_declaration: Mapping[str, Any],
    selection_manifest: Mapping[str, Any],
    decision_report_sha256: str,
    selection_manifest_sha256: str,
    decision_report_sealed_at_utc: str,
    selection_completed_at_utc: str,
) -> dict[str, str]:
    """Bind the executable policy to the sealed report and selected book entry."""

    if not isinstance(frozen_finalist_declaration, Mapping) or not isinstance(
        selection_manifest, Mapping
    ):
        raise ActiveRiskShadowPackageError(
            "frozen finalist and selection declarations are mandatory"
        )
    forward_contract = selection_manifest.get("forward_contract")
    if (
        selection_manifest.get("schema") != FROZEN_BOOK_SELECTION_SCHEMA
        or selection_manifest.get("campaign_id")
        != FROZEN_BOOK_SELECTION_CAMPAIGN_ID
        or selection_manifest.get("evidence_status")
        != FROZEN_BOOK_SELECTION_EVIDENCE_STATUS
        or selection_manifest.get("maximum_candidate_status")
        != ACTIVE_RISK_PACKAGE_ROLE
        or selection_manifest.get("paper_shadow_ready_assigned") is not False
        or _utc_timestamp(
            "selection manifest completion time",
            str(selection_manifest.get("selection_completed_at_utc") or ""),
        )
        != _utc_timestamp(
            "selection_completed_at_utc", selection_completed_at_utc
        )
        or not isinstance(forward_contract, Mapping)
        or any(
            forward_contract.get(name) is not True
            for name in (
                "append_only_post_freeze_bars_required",
                "no_broker",
                "no_orders",
                "no_q4_access",
                "no_new_data_purchase",
                "paper_shadow_ready_prohibited_from_this_evidence",
            )
        )
    ):
        raise ActiveRiskShadowPackageError(
            "selection manifest development/forward contract drift"
        )
    manifest_payload = dict(selection_manifest)
    claimed_manifest_hash = str(
        manifest_payload.pop("selection_manifest_hash", "")
    )
    if not claimed_manifest_hash or stable_hash(manifest_payload) != claimed_manifest_hash:
        raise ActiveRiskShadowPackageError("selection manifest semantic hash drift")
    serialized_manifest = (
        json.dumps(
            selection_manifest,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    if hashlib.sha256(serialized_manifest).hexdigest() != selection_manifest_sha256:
        raise ActiveRiskShadowPackageError("selection manifest file hash drift")
    source_report = selection_manifest.get("source_decision_report")
    selected_books = selection_manifest.get("selected_books")
    if not isinstance(source_report, Mapping) or not isinstance(selected_books, list):
        raise ActiveRiskShadowPackageError(
            "selection manifest lacks report provenance or selected books"
        )
    report_hash = str(source_report.get("report_hash") or "")
    report_receipt_hash = str(source_report.get("seal_receipt_hash") or "")
    for name, value in (
        ("source decision report hash", report_hash),
        ("source decision report receipt hash", report_receipt_hash),
    ):
        _require_sha256(name, value)
    _require_sha256(
        "source decision report receipt artifact hash",
        str(source_report.get("seal_receipt_sha256") or ""),
    )
    if (
        source_report.get("verified_before_selection") is not True
        or source_report.get("seal_receipt_name")
        != "decision_report_revision_02_seal_receipt.json"
        or source_report.get("report_json_name")
        != "decision_report_revision_02.json"
    ):
        raise ActiveRiskShadowPackageError(
            "selection manifest source-report verification contract drift"
        )
    if str(source_report.get("json_sha256") or "") != decision_report_sha256:
        raise ActiveRiskShadowPackageError(
            "selection manifest references another decision-report artifact"
        )
    if _utc_timestamp(
        "selection source decision-report seal time",
        str(source_report.get("sealed_at_utc") or ""),
    ) != _utc_timestamp(
        "decision_report_sealed_at_utc", decision_report_sealed_at_utc
    ):
        raise ActiveRiskShadowPackageError(
            "selection manifest decision-report seal time drift"
        )

    matches = [
        row
        for row in selected_books
        if isinstance(row, Mapping)
        and str(row.get("policy_id") or "") == policy.policy_id
    ]
    if len(matches) != 1:
        raise ActiveRiskShadowPackageError(
            "policy is absent or duplicated in the selection manifest"
        )
    entry = dict(matches[0])
    claimed_entry_hash = str(entry.pop("entry_hash", ""))
    if not claimed_entry_hash or stable_hash(entry) != claimed_entry_hash:
        raise ActiveRiskShadowPackageError("selection entry hash drift")
    if (
        entry.get("selection_role") not in {"PRIMARY", "BACKUP"}
        or entry.get("status") != ACTIVE_RISK_PACKAGE_ROLE
        or not str(entry.get("expanded_exact_account_behavior_cluster") or "")
        or not str(entry.get("expanded_economic_behavior_cluster") or "")
        or str(entry.get("structural_fingerprint") or "")
        != policy.structural_fingerprint
    ):
        raise ActiveRiskShadowPackageError(
            "selection entry status, cluster, or policy identity drift"
        )

    declaration = frozen_finalist_declaration
    selected_declaration = entry.get("frozen_policy_specification")
    if (
        not isinstance(selected_declaration, Mapping)
        or dict(selected_declaration) != dict(declaration)
        or entry.get("frozen_policy_specification_sha256")
        != stable_hash(declaration)
    ):
        raise ActiveRiskShadowPackageError(
            "selected frozen finalist declaration is not exact or hash-bound"
        )
    membership = declaration.get("membership")
    if (
        str(declaration.get("policy_id") or "") != policy.policy_id
        or str(declaration.get("structural_fingerprint") or "")
        != policy.structural_fingerprint
        or declaration.get("active_risk_policy") != policy.to_dict()
        or not isinstance(membership, list)
        or len(membership) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
    ):
        raise ActiveRiskShadowPackageError(
            "frozen finalist declaration does not match the executable policy"
        )
    active_policy_sha = stable_hash(policy.to_dict())
    membership_sha = stable_hash(membership)
    if (
        declaration.get("active_risk_policy_sha256") != active_policy_sha
        or declaration.get("membership_sha256") != membership_sha
    ):
        raise ActiveRiskShadowPackageError(
            "frozen finalist policy or membership hash drift"
        )
    by_component = {
        str(row.get("component_id") or ""): row
        for row in membership
        if isinstance(row, Mapping)
    }
    if set(by_component) != set(policy.component_priority):
        raise ActiveRiskShadowPackageError("frozen finalist membership coverage drift")
    for sleeve_id in policy.component_priority:
        row = by_component[sleeve_id]
        spec = specs[sleeve_id]
        record = records[sleeve_id]
        if (
            row.get("immutable_fingerprint") != record.immutable_fingerprint
            or row.get("behavioral_fingerprint") != record.behavioral_fingerprint
            or row.get("signal_ledger_sha256") != record.signal_ledger_sha256
            or row.get("trade_ledger_sha256") != record.trade_ledger_sha256
            or row.get("market") != record.market
            or row.get("contract") != record.contract
            or row.get("timeframe") != record.timeframe
            or row.get("session") != record.session
            or row.get("source_campaign") != record.source_campaign
            or row.get("component_role") != record.economic_role
            or row.get("sleeve_specification") != spec.to_dict()
        ):
            raise ActiveRiskShadowPackageError(
                f"frozen finalist sleeve declaration drift: {sleeve_id}"
            )

    books = {
        "combine_book": "combine_book_sha256",
        "xfa_standard_book": "xfa_standard_book_sha256",
        "xfa_consistency_book": "xfa_consistency_book_sha256",
    }
    book_hashes: dict[str, str] = {}
    for book_name, hash_name in books.items():
        book = declaration.get(book_name)
        if not isinstance(book, Mapping):
            raise ActiveRiskShadowPackageError(
                f"frozen finalist lacks {book_name}"
            )
        digest = stable_hash(book)
        if declaration.get(hash_name) != digest or entry.get(hash_name) != digest:
            raise ActiveRiskShadowPackageError(
                f"selected {book_name} hash differs from sealed report"
            )
        book_hashes[hash_name] = digest
    combine_book = declaration["combine_book"]
    account_parameters = combine_book.get("account_parameters")
    costs = combine_book.get("costs")
    expected_profile = _static_xfa_profile(policy).to_dict()
    rules = official_rule_snapshot_2026_07_15().to_dict()
    source_combine_book_sha = stable_hash(combine_book)
    if (
        combine_book.get("book") != "COMBINE_BOOK"
        or combine_book.get("policy_id") != policy.policy_id
        or combine_book.get("active_risk_policy_sha256") != active_policy_sha
        or combine_book.get("membership_sha256") != membership_sha
        or not isinstance(account_parameters, Mapping)
        or float(account_parameters.get("starting_balance", 0.0)) != 150_000.0
        or float(account_parameters.get("profit_target", 0.0)) != 9_000.0
        or float(account_parameters.get("maximum_loss_limit", 0.0)) != 4_500.0
        or int(account_parameters.get("maximum_mini_equivalent", 0)) != 15
        or int(account_parameters.get("maximum_simultaneous_positions", 0)) != 3
        or account_parameters.get("consistency_rule")
        != "TOPSTEP_150K_CONFIGURED"
        or account_parameters.get("session_constraints")
        != "FROZEN_SOURCE_COMPONENT"
        or account_parameters.get("dynamic_loss_streak_ratchet") is not False
        or account_parameters.get("unrealized_aggregation_semantics")
        != UNREALIZED_AGGREGATION_SEMANTICS
        or account_parameters.get("timestamp_exact_combined_unrealized_claimed")
        is not False
        or not isinstance(costs, Mapping)
        or float(costs.get("normal_multiplier", 0.0)) != 1.0
        or float(costs.get("stressed_multiplier", 0.0)) != 1.5
        or costs.get("source_component_costs_frozen") is not True
        or costs.get("retune_after_outcomes") is not False
        or combine_book.get("frozen_horizons") != [20, 40, 60, 90, "FULL"]
        or combine_book.get("underlying_sleeve_logic_mutated") is not False
        or declaration["xfa_standard_book"].get("book") != "XFA_STANDARD_BOOK"
        or declaration["xfa_consistency_book"].get("book")
        != "XFA_CONSISTENCY_BOOK"
        or declaration["xfa_standard_book"].get("xfa_profile") != expected_profile
        or declaration["xfa_consistency_book"].get("xfa_profile")
        != expected_profile
    ):
        raise ActiveRiskShadowPackageError(
            "sealed Combine/XFA books differ from executable frozen semantics"
        )
    for book_name in ("xfa_standard_book", "xfa_consistency_book"):
        book = declaration[book_name]
        if (
            book.get("policy_id") != policy.policy_id
            or book.get("source_combine_book_sha256") != source_combine_book_sha
            or book.get("membership_sha256") != membership_sha
            or book.get("rule_snapshot") != rules
            or book.get("overlay_semantics")
            != ACTIVE_RISK_XFA_OVERLAY_SEMANTICS
            or book.get("combine_profit_transferred_to_xfa") is not False
            or book.get("combine_governor_controls_applied_in_xfa") is not False
            or book.get("book_frozen_before_outcomes") is not True
            or book.get("selected_after_combine_outcome") is not False
        ):
            raise ActiveRiskShadowPackageError(
                f"sealed {book_name} lifecycle semantics drift"
            )
    for name, value in (
        ("active_risk_policy_sha256", active_policy_sha),
        ("membership_sha256", membership_sha),
        *book_hashes.items(),
    ):
        if entry.get(name) != value:
            raise ActiveRiskShadowPackageError(
                f"selection entry {name} differs from sealed finalist"
            )
    return {
        "decision_report_hash": report_hash,
        "decision_report_seal_receipt_hash": report_receipt_hash,
        "selection_manifest_hash": claimed_manifest_hash,
        "selection_entry_hash": claimed_entry_hash,
        "selection_role": str(entry["selection_role"]),
        "expanded_exact_account_behavior_cluster": str(
            entry["expanded_exact_account_behavior_cluster"]
        ),
        "expanded_economic_behavior_cluster": str(
            entry["expanded_economic_behavior_cluster"]
        ),
        "active_risk_policy_sha256": active_policy_sha,
        "membership_sha256": membership_sha,
        **book_hashes,
    }


def _validate_bound_sources(
    policy: ActiveRiskPoolPolicy,
    sleeve_specs: Mapping[str, SleeveSpec],
    sleeve_records: Mapping[str, SleeveRecord],
    component_fingerprints: Mapping[str, str],
) -> tuple[dict[str, SleeveSpec], dict[str, SleeveRecord], dict[str, str]]:
    specs = {str(key): value for key, value in sleeve_specs.items()}
    records = {str(key): value for key, value in sleeve_records.items()}
    components = {str(key): str(value) for key, value in component_fingerprints.items()}
    expected = set(policy.component_priority)
    if (
        len(policy.component_priority) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or set(specs) != expected
        or set(records) != expected
        or set(components) != expected
    ):
        raise ActiveRiskShadowPackageError(
            "active-risk package requires exactly the frozen eighteen source sleeves"
        )
    for label, values in (
        ("immutable", (row.immutable_fingerprint for row in records.values())),
        ("behavioral", (row.behavioral_fingerprint for row in records.values())),
        ("signal ledger", (row.signal_ledger_sha256 for row in records.values())),
        ("trade ledger", (row.trade_ledger_sha256 for row in records.values())),
    ):
        frozen = tuple(values)
        if len(set(frozen)) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT:
            raise ActiveRiskShadowPackageError(
                f"active-risk source {label} fingerprints must be unique"
            )
    for sleeve_id in policy.component_priority:
        spec = specs[sleeve_id]
        record = records[sleeve_id]
        component = components[sleeve_id]
        if not isinstance(spec, SleeveSpec) or spec.sleeve_id != sleeve_id:
            raise ActiveRiskShadowPackageError(f"invalid SleeveSpec: {sleeve_id}")
        if not isinstance(record, SleeveRecord) or record.sleeve_id != sleeve_id:
            raise ActiveRiskShadowPackageError(f"invalid SleeveRecord: {sleeve_id}")
        _require_sha256("component_fingerprint", component)
        if (
            component != record.immutable_fingerprint
            or record.behavioral_fingerprint != spec.behavioral_fingerprint
            or record.market != spec.market
            or record.contract != spec.execution_market
            or record.timeframe != spec.timeframe
            or record.session != _SESSION_LABELS[spec.session_code]
            or record.source_campaign != spec.source_campaign
            or spec.inherited_status is not None
        ):
            raise ActiveRiskShadowPackageError(
                f"immutable sleeve source binding drift: {sleeve_id}"
            )
    return specs, records, components


def _validate_signal_bindings(
    policy: ActiveRiskPoolPolicy,
    specs: Mapping[str, SleeveSpec],
    records: Mapping[str, SleeveRecord],
    bindings: Mapping[str, FrozenSignalBinding],
) -> dict[str, FrozenSignalBinding]:
    checked = {str(key): value for key, value in bindings.items()}
    expected = set(policy.component_priority)
    if set(checked) != expected or len(checked) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT:
        raise ActiveRiskShadowPackageError(
            "frozen numeric signal bindings must cover all eighteen sleeves"
        )
    for sleeve_id in policy.component_priority:
        spec = specs[sleeve_id]
        record = records[sleeve_id]
        binding = checked[sleeve_id]
        if not isinstance(binding, FrozenSignalBinding):
            raise ActiveRiskShadowPackageError(
                f"invalid frozen signal binding: {sleeve_id}"
            )
        if (
            binding.sleeve_id != sleeve_id
            or binding.trigger_feature != spec.trigger_feature
            or binding.trigger_operator != spec.trigger_operator
            or binding.context_feature != spec.context_feature
            or binding.context_operator != spec.context_operator
            or binding.source_execution_fingerprint
            != record.immutable_fingerprint
            or binding.feature_matrix_market != spec.market
            or binding.feature_matrix_execution_market != spec.execution_market
            or binding.feature_bundle_version != FEATURE_BUNDLE_VERSION
            or binding.feature_dag_hash != FEATURE_DAG_HASH
            or binding.feature_matrix_schema != "hydra_canonical_feature_store_v2"
        ):
            raise ActiveRiskShadowPackageError(
                f"frozen signal/specification provenance drift: {sleeve_id}"
            )
    if len({value.fingerprint for value in checked.values()}) != len(checked):
        raise ActiveRiskShadowPackageError(
            "frozen signal calibration bindings must be sleeve-distinct"
        )
    return checked


def _validate_redundant_sleeve_views(
    package: ImmutableActiveRiskShadowPackage,
    policy: ActiveRiskPoolPolicy,
    specs: Mapping[str, SleeveSpec],
    records: Mapping[str, SleeveRecord],
    signal_bindings: Mapping[str, FrozenSignalBinding],
) -> None:
    if (
        tuple(package.entry_policy.get("priority_order") or ())
        != policy.component_priority
        or package.entry_policy.get("same_instrument_conflict_policy")
        != policy.same_instrument_conflict_rule.value
        or package.signal_policy.get("same_instrument_conflict_policy")
        != policy.same_instrument_conflict_rule.value
        or package.market_policy.get("same_instrument_conflict_policy")
        != policy.same_instrument_conflict_rule.value
    ):
        raise ActiveRiskShadowPackageError("active-risk conflict/priority view drift")
    for sleeve_id, spec in specs.items():
        entry = (package.entry_policy.get("sleeves") or {}).get(sleeve_id)
        exit_policy = (package.exit_policy.get("sleeves") or {}).get(sleeve_id)
        session = (package.session_policy.get("sleeves") or {}).get(sleeve_id)
        market = (package.market_policy.get("sleeves") or {}).get(sleeve_id)
        if not all(isinstance(row, Mapping) for row in (entry, exit_policy, session, market)):
            raise ActiveRiskShadowPackageError(
                f"redundant executable sleeve view missing: {sleeve_id}"
            )
        if (
            entry.get("trigger_feature") != spec.trigger_feature
            or entry.get("trigger_operator") != spec.trigger_operator
            or float(entry.get("trigger_quantile")) != spec.trigger_quantile
            or entry.get("context_feature") != spec.context_feature
            or entry.get("context_operator") != spec.context_operator
            or entry.get("context_quantile") != spec.context_quantile
            or entry.get("trigger_threshold")
            != signal_bindings[sleeve_id].trigger_threshold
            or entry.get("context_threshold")
            != signal_bindings[sleeve_id].context_threshold
            or entry.get("calibration_start")
            != signal_bindings[sleeve_id].calibration_start
            or entry.get("calibration_end_exclusive")
            != signal_bindings[sleeve_id].calibration_end_exclusive
            or entry.get("signal_calibration_fingerprint")
            != signal_bindings[sleeve_id].fingerprint
            or entry.get("threshold_mode")
            != "FROZEN_NUMERIC_NO_FORWARD_RECALIBRATION"
            or int(entry.get("side")) != spec.side
            or exit_policy.get("exit_style") != spec.exit_style
            or int(exit_policy.get("holding_bars")) != spec.holding_bars
            or int(session.get("session_code")) != spec.session_code
            or market.get("explicit_contract") != records[sleeve_id].contract
            or market.get("signal_market") != spec.market
            or market.get("execution_market") != spec.execution_market
            or tuple(market.get("component_ids") or ()) != spec.component_ids
            or entry.get("underlying_signal_mutated") is not False
            or exit_policy.get("underlying_exit_mutated") is not False
            or session.get("session_label") != _SESSION_LABELS[spec.session_code]
        ):
            raise ActiveRiskShadowPackageError(
                f"redundant active-risk sleeve view drift: {sleeve_id}"
            )
    if package.signal_policy.get("signal_ledger_sha256") != {
        sleeve_id: records[sleeve_id].signal_ledger_sha256
        for sleeve_id in policy.component_priority
    } or package.signal_policy.get("trade_ledger_sha256") != {
        sleeve_id: records[sleeve_id].trade_ledger_sha256
        for sleeve_id in policy.component_priority
    }:
        raise ActiveRiskShadowPackageError("redundant source ledger map drift")


def _static_xfa_profile(policy: ActiveRiskPoolPolicy) -> FrozenRiskProfile:
    maximum = float(policy.maximum_mini_equivalent)
    if not maximum.is_integer():
        raise ActiveRiskShadowPackageError(
            "XFA projection requires an integral frozen mini-equivalent limit"
        )
    return FrozenRiskProfile(
        profile_id=f"{policy.policy_id}:XFA_PROFILE",
        risk_multiplier=float(policy.static_risk_tier),
        maximum_simultaneous_positions=policy.maximum_concurrent_sleeves,
        maximum_mini_equivalent=int(maximum),
        clip_to_xfa_scaling_plan=True,
        same_market_exclusive=True,
    )


def _profile_from_mapping(value: Mapping[str, Any]) -> FrozenRiskProfile:
    profile = FrozenRiskProfile(
        profile_id=str(value["profile_id"]),
        risk_multiplier=float(value["risk_multiplier"]),
        maximum_simultaneous_positions=int(value["maximum_simultaneous_positions"]),
        maximum_mini_equivalent=int(value["maximum_mini_equivalent"]),
        clip_to_xfa_scaling_plan=bool(value["clip_to_xfa_scaling_plan"]),
        same_market_exclusive=bool(value["same_market_exclusive"]),
        profile_version=str(value["profile_version"]),
    )
    if value.get("fingerprint") not in {None, profile.fingerprint}:
        raise ActiveRiskShadowPackageError("serialized XFA profile fingerprint drift")
    return profile


def _sleeve_from_mapping(value: Mapping[str, Any]) -> SleeveSpec:
    try:
        spec = SleeveSpec(
            sleeve_id=str(value["sleeve_id"]),
            component_ids=tuple(str(row) for row in value["component_ids"]),
            market=str(value["market"]),
            execution_market=str(value["execution_market"]),
            timeframe=str(value["timeframe"]),
            session_code=int(value["session_code"]),
            trigger_feature=str(value["trigger_feature"]),
            trigger_operator=str(value["trigger_operator"]),
            trigger_quantile=float(value["trigger_quantile"]),
            context_feature=(
                None
                if value.get("context_feature") is None
                else str(value["context_feature"])
            ),
            context_operator=(
                None
                if value.get("context_operator") is None
                else str(value["context_operator"])
            ),
            context_quantile=(
                None
                if value.get("context_quantile") is None
                else float(value["context_quantile"])
            ),
            side=int(value["side"]),
            holding_bars=int(value["holding_bars"]),
            exit_style=str(value["exit_style"]),
            role=EconomicRole(str(value["role"])),
            source_campaign=str(value["source_campaign"]),
            lineage_id=str(value["lineage_id"]),
            version=int(value.get("version") or 1),
            inherited_status=None,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskShadowPackageError(
            "invalid executable active-risk SleeveSpec"
        ) from exc
    if (
        value.get("structural_fingerprint") not in {None, spec.structural_fingerprint}
        or value.get("behavioral_fingerprint")
        not in {None, spec.behavioral_fingerprint}
    ):
        raise ActiveRiskShadowPackageError("serialized SleeveSpec fingerprint drift")
    return spec


def _require_sha256(name: str, value: str) -> None:
    if _SHA256.fullmatch(str(value)) is None:
        raise ActiveRiskShadowPackageError(f"{name} must be a lowercase SHA-256")


def _utc_timestamp(name: str, value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ActiveRiskShadowPackageError(
            f"{name} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ActiveRiskShadowPackageError(f"{name} must be explicitly UTC")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "ACTIVE_RISK_COMBINE_HORIZONS",
    "ACTIVE_RISK_PACKAGE_CONTRACT",
    "ACTIVE_RISK_PACKAGE_ROLE",
    "ACTIVE_RISK_SOURCE_SLEEVE_COUNT",
    "ACTIVE_RISK_XFA_HORIZON_DAYS",
    "ACTIVE_RISK_XFA_OVERLAY_SEMANTICS",
    "FROZEN_SIGNAL_BINDING_VERSION",
    "ActiveRiskShadowPackageError",
    "FrozenSignalBinding",
    "ImmutableActiveRiskShadowPackage",
    "ReconstructedActiveRiskShadowPackage",
    "build_active_risk_shadow_package",
    "reconstruct_active_risk_shadow_package",
]
