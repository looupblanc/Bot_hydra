"""Immutable no-order forward package for a frozen portfolio book.

This module deliberately sits outside the portfolio production runtime.  It
turns a *selected* :class:`BookPair` and its complete executable sleeve
specifications into the existing ``hydra_immutable_shadow_package_v4``
contract.  The package contains enough information to reconstruct both books
without consulting a campaign manifest, while remaining checksum-bound to the
EvidenceBundle that justified forward observation.

The caller must supply both the selection-completion time and the later freeze
time.  There is intentionally no clock/default here: a campaign
preregistration timestamp cannot silently become a post-selection freeze.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from typing import Any, Mapping

from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.production.portfolio_books import BookPair, stable_hash
from hydra.propfirm.combine_to_xfa import (
    UNREALIZED_AGGREGATION_SEMANTICS,
    official_rule_snapshot_2026_07_15,
)
from hydra.research.turbo_feature_builder import FEATURE_BUNDLE_VERSION, FEATURE_DAG_HASH
from hydra.shadow.package_factory import (
    PACKAGE_SCHEMA,
    ImmutableShadowPackage,
    ShadowPackageError,
)


PORTFOLIO_PACKAGE_ROLE = "PORTFOLIO_FIRST_ACCOUNT_BOOK"
PORTFOLIO_PACKAGE_CONTRACT = "hydra_portfolio_shadow_book_contract_v1"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SESSION_LABELS = {-1: "ALL", 0: "OPEN", 1: "MIDDLE", 2: "LATE"}


class PortfolioShadowPackageError(ShadowPackageError):
    """The frozen portfolio package is incomplete, unsafe, or inconsistent."""


@dataclass(frozen=True)
class ImmutablePortfolioShadowPackage(ImmutableShadowPackage):
    """The v4 package with portfolio-specific validation on every write."""

    def validate(self) -> None:
        super().validate()
        _validate_portfolio_package(self)


@dataclass(frozen=True, slots=True)
class ReconstructedPortfolioShadowPackage:
    """Typed reconstruction of the executable objects carried by a package."""

    package: ImmutablePortfolioShadowPackage
    book_pair: BookPair
    sleeve_specs: Mapping[str, SleeveSpec]


def build_portfolio_shadow_package(
    book_pair: BookPair,
    sleeve_specs: Mapping[str, SleeveSpec],
    *,
    source_commit: str,
    selection_completed_at_utc: str,
    freeze_timestamp_utc: str,
    evidence_bundle_identity_sha256: str,
    evidence_bundle_configuration_sha256: str,
) -> ImmutablePortfolioShadowPackage:
    """Build a reconstructible, no-order package after portfolio selection.

    ``sleeve_specs`` must contain exactly the union of the Combine and XFA
    members.  Underlying entry and exit logic is copied verbatim and then
    checksum-bound by the package hash.  Book membership, allocation, risk
    tier, and conflict policy remain separate account-level declarations.
    """

    _require_sha256("evidence_bundle_identity_sha256", evidence_bundle_identity_sha256)
    _require_sha256(
        "evidence_bundle_configuration_sha256",
        evidence_bundle_configuration_sha256,
    )
    if _GIT_COMMIT.fullmatch(str(source_commit)) is None:
        raise PortfolioShadowPackageError("source_commit must be a lowercase Git SHA")
    selection_time = _utc_timestamp(
        "selection_completed_at_utc", selection_completed_at_utc
    )
    freeze_time = _utc_timestamp("freeze_timestamp_utc", freeze_timestamp_utc)
    if freeze_time < selection_time:
        raise PortfolioShadowPackageError(
            "forward package freeze cannot precede completed selection"
        )

    specs = _validate_bound_specs(book_pair, sleeve_specs)
    binding_by_id = {row[0]: row for row in book_pair.source_bindings}
    sleeve_ids = tuple(sorted(specs))
    serialized_specs = {
        sleeve_id: {
            "specification": specs[sleeve_id].to_dict(),
            "specification_sha256": stable_hash(specs[sleeve_id].to_dict()),
            "source_binding": {
                "immutable_fingerprint": binding_by_id[sleeve_id][1],
                "behavioral_fingerprint": binding_by_id[sleeve_id][2],
                "signal_ledger_sha256": binding_by_id[sleeve_id][3],
                "trade_ledger_sha256": binding_by_id[sleeve_id][4],
            },
        }
        for sleeve_id in sleeve_ids
    }
    entry_sleeves = {
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
            "context_feature": specs[sleeve_id].context_feature,
            "context_operator": specs[sleeve_id].context_operator,
            "context_quantile": specs[sleeve_id].context_quantile,
            "underlying_signal_mutated": False,
        }
        for sleeve_id in sleeve_ids
    }
    exit_sleeves = {
        sleeve_id: {
            "exit_style": specs[sleeve_id].exit_style,
            "holding_bars": specs[sleeve_id].holding_bars,
            "holding_timeframe": specs[sleeve_id].timeframe,
            "mandatory_session_flatten": True,
            "underlying_exit_mutated": False,
        }
        for sleeve_id in sleeve_ids
    }
    session_sleeves = {
        sleeve_id: {
            "session_code": specs[sleeve_id].session_code,
            "session_label": _SESSION_LABELS[specs[sleeve_id].session_code],
            "mandatory_flatten": True,
        }
        for sleeve_id in sleeve_ids
    }
    combine_allocations = dict(
        zip(
            book_pair.combine_sleeve_ids,
            book_pair.combine_allocation_units,
            strict=True,
        )
    )
    xfa_allocations = dict(
        zip(book_pair.xfa_sleeve_ids, book_pair.xfa_allocation_units, strict=True)
    )
    rules = official_rule_snapshot_2026_07_15()

    package = ImmutablePortfolioShadowPackage(
        candidate_id=book_pair.pair_id,
        candidate_specification_hash=book_pair.structural_fingerprint,
        source_commit=source_commit,
        freeze_timestamp_utc=freeze_timestamp_utc,
        role=PORTFOLIO_PACKAGE_ROLE,
        market_policy={
            "portfolio_contract": PORTFOLIO_PACKAGE_CONTRACT,
            "sleeves": {
                sleeve_id: {
                    "signal_market": specs[sleeve_id].market,
                    "execution_market": specs[sleeve_id].execution_market,
                    "component_ids": list(specs[sleeve_id].component_ids),
                }
                for sleeve_id in sleeve_ids
            },
            "explicit_contracts_required": True,
            "date_aware_contract_resolution_required": True,
            "same_instrument_conflict_policy": book_pair.conflict_policy,
        },
        timeframe_profile=tuple(
            sorted({specs[sleeve_id].timeframe for sleeve_id in sleeve_ids})
        ),
        feature_contract={
            "feature_bundle_version": FEATURE_BUNDLE_VERSION,
            "feature_dag_hash": FEATURE_DAG_HASH,
            "immutable_sleeves": serialized_specs,
            "closed_bars_only": True,
            "source_close_must_precede_decision": True,
            "availability_must_not_exceed_decision": True,
            "higher_timeframe_partial_bars_prohibited": True,
        },
        entry_policy={
            "type": "IMMUTABLE_INDEPENDENT_SLEEVE_ENTRIES",
            "sleeves": entry_sleeves,
            "combine_priority_order": list(book_pair.combine_sleeve_ids),
            "xfa_priority_order": list(book_pair.xfa_sleeve_ids),
            "same_instrument_conflict_policy": book_pair.conflict_policy,
        },
        exit_policy={
            "type": "IMMUTABLE_INDEPENDENT_SLEEVE_EXITS",
            "sleeves": exit_sleeves,
            "mandatory_session_flatten": True,
            "cross_contract_holding_prohibited": True,
        },
        sizing_policy={
            "type": "TWO_FROZEN_ACCOUNT_BOOKS",
            "frozen_book_pair": book_pair.to_dict(),
            "combine_book": {
                "sleeve_ids": list(book_pair.combine_sleeve_ids),
                "allocation_units_by_sleeve": combine_allocations,
                "static_risk_tier": book_pair.combine_risk_tier,
            },
            "xfa_book": {
                "sleeve_ids": list(book_pair.xfa_sleeve_ids),
                "allocation_units_by_sleeve": xfa_allocations,
                "static_risk_tier": book_pair.xfa_risk_tier,
            },
            "maximum_mini_equivalent": 15,
            "dynamic_resizing": False,
            "outcome_selected_resizing": False,
        },
        session_policy={
            "calendar": "CME_GLOBEX_AMERICA_CHICAGO",
            "trading_day_from_local_17_00": True,
            "dst_aware": True,
            "sleeves": session_sleeves,
            "mandatory_flatten": True,
        },
        cost_policy={
            "normal": "FROZEN_SOURCE_TRADE_LEDGER_COSTS",
            "stressed": "FROZEN_SOURCE_TRADE_LEDGER_COSTS_X_1_5",
            "stress_multiplier": 1.5,
            "virtual_fill_slippage": "ADVERSE_ONE_TICK_OR_WORSE",
        },
        risk_policy={
            "topstep_rule_snapshot": rules.to_dict(),
            "topstep_rule_snapshot_fingerprint": rules.fingerprint,
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
            "selection_completed_at_utc": selection_completed_at_utc,
            "freeze_timestamp_utc": freeze_timestamp_utc,
            "freeze_basis": "CALLER_SUPPLIED_POST_SELECTION",
            "first_eligible_bar_close_operator": "STRICTLY_GREATER_THAN_FREEZE",
            "fresh_completed_bars_only": True,
            "expected_bar_seconds": 60,
            "stale_after_seconds": 180,
            "duplicate_bars_rejected": True,
            "duplicate_source_sequences_rejected": True,
            "missing_intervals_flagged": True,
            "out_of_order_bars_rejected": True,
            "append_only": True,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        },
        signal_policy={
            "deterministic": True,
            "independent_sleeve_signals": True,
            "duplicate_signal_guard": True,
            "duplicate_window_seconds": 60,
            "stale_data_action": "REJECT_SIGNAL_AND_FAIL_CLOSED",
            "same_instrument_conflict_policy": book_pair.conflict_policy,
            "signal_ledger_sha256": {
                sleeve_id: binding_by_id[sleeve_id][3] for sleeve_id in sleeve_ids
            },
            "trade_ledger_sha256": {
                sleeve_id: binding_by_id[sleeve_id][4] for sleeve_id in sleeve_ids
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
            "contract_resolution_failure",
            "clock_or_session_mismatch",
            "configuration_hash_mismatch",
            "evidence_identity_hash_mismatch",
            "simulated_mll_floor_reached",
        ),
        observability={
            "ledger_path": (
                f"shadow/state/forward/{book_pair.pair_id}/forward_evidence.jsonl"
            ),
            "reconciliation_required": True,
            "sleeve_attribution_required": True,
            "book_transition_ledger_required": True,
            "required_fields": [
                "bar_close_at_utc",
                "source_sequence",
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
            "evidence_bundle_identity_sha256": evidence_bundle_identity_sha256,
            "evidence_bundle_configuration_sha256": (
                evidence_bundle_configuration_sha256
            ),
            "book_pair_structural_fingerprint": book_pair.structural_fingerprint,
            "book_pair_behavioral_fingerprint": book_pair.behavioral_fingerprint,
            "source_bindings": [list(row) for row in book_pair.source_bindings],
            "selection_completed_at_utc": selection_completed_at_utc,
            "freeze_timestamp_utc": freeze_timestamp_utc,
            "freeze_basis": "CALLER_SUPPLIED_POST_SELECTION",
            "development_only": True,
            "independently_confirmed": False,
            "q4_evidence_inherited": False,
        },
        broker_connectivity=False,
        outbound_order_capability=False,
    )
    package.validate()
    return package


def reconstruct_portfolio_shadow_package(
    payload: Mapping[str, Any],
) -> ReconstructedPortfolioShadowPackage:
    """Rebuild and verify a portfolio package loaded from canonical JSON."""

    if str(payload.get("schema") or "") != PACKAGE_SCHEMA:
        raise PortfolioShadowPackageError("portfolio shadow package schema drift")
    field_names = {field.name for field in fields(ImmutableShadowPackage)}
    missing = field_names.difference(payload)
    if missing:
        raise PortfolioShadowPackageError(
            "portfolio shadow package fields missing: " + ", ".join(sorted(missing))
        )
    values = {name: payload[name] for name in field_names}
    values["timeframe_profile"] = tuple(values["timeframe_profile"])
    values["kill_conditions"] = tuple(values["kill_conditions"])
    package = ImmutablePortfolioShadowPackage(**values)
    package.validate()
    if str(payload.get("package_hash") or "") != package.package_hash:
        raise PortfolioShadowPackageError("portfolio shadow package hash drift")

    try:
        pair = BookPair.from_mapping(package.sizing_policy["frozen_book_pair"])
        rows = package.feature_contract["immutable_sleeves"]
    except (KeyError, TypeError, ValueError) as exc:
        raise PortfolioShadowPackageError(
            "portfolio shadow package lacks reconstructible books or sleeves"
        ) from exc
    if not isinstance(rows, Mapping):
        raise PortfolioShadowPackageError("immutable_sleeves must be a mapping")
    specs = {
        str(sleeve_id): _sleeve_from_mapping(row["specification"])
        for sleeve_id, row in rows.items()
    }
    _validate_bound_specs(pair, specs)
    return ReconstructedPortfolioShadowPackage(
        package=package,
        book_pair=pair,
        sleeve_specs=specs,
    )


def _validate_portfolio_package(package: ImmutablePortfolioShadowPackage) -> None:
    if package.role != PORTFOLIO_PACKAGE_ROLE:
        raise PortfolioShadowPackageError("portfolio package role drift")
    if _GIT_COMMIT.fullmatch(package.source_commit) is None:
        raise PortfolioShadowPackageError("portfolio package source commit drift")
    if package.candidate_specification_hash != str(
        package.evidence_provenance.get("book_pair_structural_fingerprint") or ""
    ):
        raise PortfolioShadowPackageError("portfolio candidate fingerprint drift")
    for field_name in (
        "evidence_bundle_identity_sha256",
        "evidence_bundle_configuration_sha256",
    ):
        _require_sha256(field_name, str(package.evidence_provenance.get(field_name) or ""))
    selection_value = str(package.data_policy.get("selection_completed_at_utc") or "")
    freeze_value = str(package.data_policy.get("freeze_timestamp_utc") or "")
    selection_time = _utc_timestamp("selection_completed_at_utc", selection_value)
    freeze_time = _utc_timestamp("freeze_timestamp_utc", freeze_value)
    if freeze_time < selection_time or freeze_value != package.freeze_timestamp_utc:
        raise PortfolioShadowPackageError("portfolio post-selection freeze drift")
    if (
        package.data_policy.get("freeze_basis") != "CALLER_SUPPLIED_POST_SELECTION"
        or package.data_policy.get("first_eligible_bar_close_operator")
        != "STRICTLY_GREATER_THAN_FREEZE"
        or not package.data_policy.get("fresh_completed_bars_only")
        or package.data_policy.get("q4_access_authorized") is not False
        or package.data_policy.get("new_data_purchase_authorized") is not False
    ):
        raise PortfolioShadowPackageError("unsafe forward data declaration")
    if (
        package.virtual_fill_policy.get("real_order_submission") is not False
        or package.virtual_fill_policy.get("broker_route") is not None
        or package.sizing_policy.get("dynamic_resizing") is not False
        or package.sizing_policy.get("outcome_selected_resizing") is not False
        or package.risk_policy.get("unrealized_aggregation_semantics")
        != UNREALIZED_AGGREGATION_SEMANTICS
    ):
        raise PortfolioShadowPackageError("portfolio package exposes unsafe execution")

    try:
        pair = BookPair.from_mapping(package.sizing_policy["frozen_book_pair"])
        serialized = package.feature_contract["immutable_sleeves"]
    except (KeyError, TypeError, ValueError) as exc:
        raise PortfolioShadowPackageError("portfolio executable declaration missing") from exc
    if not isinstance(serialized, Mapping):
        raise PortfolioShadowPackageError("immutable sleeve declaration must be a mapping")
    specs: dict[str, SleeveSpec] = {}
    for sleeve_id, raw in serialized.items():
        try:
            spec_value = raw["specification"]
            declared_hash = str(raw["specification_sha256"])
            source_binding = raw["source_binding"]
        except (KeyError, TypeError) as exc:
            raise PortfolioShadowPackageError(
                f"incomplete immutable sleeve declaration: {sleeve_id}"
            ) from exc
        spec = _sleeve_from_mapping(spec_value)
        if spec.sleeve_id != str(sleeve_id) or stable_hash(spec.to_dict()) != declared_hash:
            raise PortfolioShadowPackageError(
                f"executable sleeve specification drift: {sleeve_id}"
            )
        specs[str(sleeve_id)] = spec
        expected_binding = next(
            (row for row in pair.source_bindings if row[0] == str(sleeve_id)), None
        )
        if expected_binding is None or (
            str(source_binding.get("immutable_fingerprint") or ""),
            str(source_binding.get("behavioral_fingerprint") or ""),
            str(source_binding.get("signal_ledger_sha256") or ""),
            str(source_binding.get("trade_ledger_sha256") or ""),
        ) != expected_binding[1:]:
            raise PortfolioShadowPackageError(
                f"source ledger binding drift: {sleeve_id}"
            )
    _validate_bound_specs(pair, specs)
    _validate_redundant_execution_views(package, pair, specs)


def _validate_redundant_execution_views(
    package: ImmutablePortfolioShadowPackage,
    pair: BookPair,
    specs: Mapping[str, SleeveSpec],
) -> None:
    combine = package.sizing_policy.get("combine_book") or {}
    xfa = package.sizing_policy.get("xfa_book") or {}
    if (
        tuple(combine.get("sleeve_ids") or ()) != pair.combine_sleeve_ids
        or tuple(xfa.get("sleeve_ids") or ()) != pair.xfa_sleeve_ids
        or combine.get("allocation_units_by_sleeve")
        != dict(zip(pair.combine_sleeve_ids, pair.combine_allocation_units, strict=True))
        or xfa.get("allocation_units_by_sleeve")
        != dict(zip(pair.xfa_sleeve_ids, pair.xfa_allocation_units, strict=True))
        or float(combine.get("static_risk_tier") or 0.0) != pair.combine_risk_tier
        or float(xfa.get("static_risk_tier") or 0.0) != pair.xfa_risk_tier
    ):
        raise PortfolioShadowPackageError("frozen Combine/XFA book view drift")
    if (
        package.entry_policy.get("same_instrument_conflict_policy")
        != pair.conflict_policy
        or package.signal_policy.get("same_instrument_conflict_policy")
        != pair.conflict_policy
    ):
        raise PortfolioShadowPackageError("portfolio conflict policy drift")
    for sleeve_id, spec in specs.items():
        entry = (package.entry_policy.get("sleeves") or {}).get(sleeve_id)
        exit_policy = (package.exit_policy.get("sleeves") or {}).get(sleeve_id)
        session = (package.session_policy.get("sleeves") or {}).get(sleeve_id)
        if not isinstance(entry, Mapping) or not isinstance(exit_policy, Mapping) or not isinstance(session, Mapping):
            raise PortfolioShadowPackageError(
                f"executable sleeve view missing: {sleeve_id}"
            )
        if (
            entry.get("trigger_feature") != spec.trigger_feature
            or entry.get("trigger_operator") != spec.trigger_operator
            or float(entry.get("trigger_quantile")) != spec.trigger_quantile
            or entry.get("context_feature") != spec.context_feature
            or entry.get("context_operator") != spec.context_operator
            or entry.get("context_quantile") != spec.context_quantile
            or int(entry.get("side")) != spec.side
            or exit_policy.get("exit_style") != spec.exit_style
            or int(exit_policy.get("holding_bars")) != spec.holding_bars
            or int(session.get("session_code")) != spec.session_code
        ):
            raise PortfolioShadowPackageError(
                f"redundant executable sleeve view drift: {sleeve_id}"
            )


def _validate_bound_specs(
    book_pair: BookPair, sleeve_specs: Mapping[str, SleeveSpec]
) -> dict[str, SleeveSpec]:
    specs = {str(key): value for key, value in sleeve_specs.items()}
    expected = set(book_pair.combine_sleeve_ids) | set(book_pair.xfa_sleeve_ids)
    if set(specs) != expected:
        missing = sorted(expected.difference(specs))
        extra = sorted(set(specs).difference(expected))
        raise PortfolioShadowPackageError(
            f"sleeve specification coverage drift; missing={missing}, extra={extra}"
        )
    binding_by_id = {row[0]: row for row in book_pair.source_bindings}
    for sleeve_id, spec in specs.items():
        if not isinstance(spec, SleeveSpec) or spec.sleeve_id != sleeve_id:
            raise PortfolioShadowPackageError(
                f"invalid executable SleeveSpec mapping: {sleeve_id}"
            )
        if spec.inherited_status is not None:
            raise PortfolioShadowPackageError(
                f"sleeve inherited a promotion status: {sleeve_id}"
            )
        if binding_by_id[sleeve_id][1] != spec.structural_fingerprint:
            raise PortfolioShadowPackageError(
                f"sleeve immutable specification binding drift: {sleeve_id}"
            )
        if binding_by_id[sleeve_id][2] != spec.behavioral_fingerprint:
            raise PortfolioShadowPackageError(
                f"sleeve behavioral binding drift: {sleeve_id}"
            )
    return specs


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
        raise PortfolioShadowPackageError("invalid executable sleeve specification") from exc
    declared_structural = value.get("structural_fingerprint")
    declared_behavioral = value.get("behavioral_fingerprint")
    if (
        declared_structural is not None
        and str(declared_structural) != spec.structural_fingerprint
    ) or (
        declared_behavioral is not None
        and str(declared_behavioral) != spec.behavioral_fingerprint
    ):
        raise PortfolioShadowPackageError("serialized SleeveSpec fingerprint drift")
    return spec


def _require_sha256(name: str, value: str) -> None:
    if _SHA256.fullmatch(str(value)) is None:
        raise PortfolioShadowPackageError(f"{name} must be a lowercase SHA-256")


def _utc_timestamp(name: str, value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioShadowPackageError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise PortfolioShadowPackageError(f"{name} must be explicitly UTC")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "ImmutablePortfolioShadowPackage",
    "PORTFOLIO_PACKAGE_CONTRACT",
    "PORTFOLIO_PACKAGE_ROLE",
    "PortfolioShadowPackageError",
    "ReconstructedPortfolioShadowPackage",
    "build_portfolio_shadow_package",
    "reconstruct_portfolio_shadow_package",
]
