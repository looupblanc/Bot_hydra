"""Bounded, read-only cross-index breadth economic tripwire.

The autonomous director uses this module as a *development-only* relay after
the existing evidence bank is exhausted.  It deliberately does not append a
manifest, touch the mission queue, write to a database, assign an evidence
tier, or start XFA.  Both feature construction and account replay reuse the
causal/exact primitives that already back FAST-PASS 0029.

The experiment is frozen to sixteen primary specifications:

* ES, NQ, YM and RTY;
* OPEN and MID session roles;
* breadth-confirmed continuation and breadth-failed-progress reversal.

Each target is excluded from its three-index reference set.  Reference rows
are joined strictly as-of ``available_at <= decision_time``.  The breadth
transition is emitted only when all three reference indices move from a
non-unanimous state into unanimous positive or negative alignment at a
completed 15-minute boundary.  There is no threshold, risk, or parameter grid.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.features.feature_matrix import FeatureMatrix
from hydra.production.autonomous_exact_replay import (
    HORIZONS,
    _account_config,
    _apply_session_contract,
    _candidate_coverage,
    _declared_stop_risk_charge_per_mini,
    _exact_cell,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _market_contract_limit_mini,
    _require_scenario_identity,
    _standalone_policy,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.research.causal_target_velocity import (
    CONTEXT_QUANTILES,
    MINUTE_NS,
    QUANTILES,
    CalibratedHazardCandidate,
    HazardCandidate,
    calibrate_candidate,
    discover_intents_batch,
    discover_intents_streaming,
    exact_sleeve_replay,
    frozen_eligible_session_calendar,
    observe_outcomes,
    screen_result,
    with_availability_safe_cross_asset_feature,
)


SCHEMA = "hydra_cross_index_breadth_tripwire_v1"
FEATURE_SCHEMA = "hydra_cross_index_breadth_feature_view_v1"
SOURCE_CAMPAIGN_ID = "hydra_fast_pass_factory_0029"
DEFAULT_MANIFEST = Path("config/v7/fast_pass_factory_0029_revision_05.json")
DEFAULT_RULE_SNAPSHOT = Path("config/rulesets/topstep_official_2026-07-19.json")

INDEX_MARKETS = ("ES", "NQ", "RTY", "YM")
EXECUTION_MARKETS = {"ES": "MES", "NQ": "MNQ", "RTY": "M2K", "YM": "MYM"}
SESSION_ROLES = {0: "OPEN", 1: "MID"}
FAMILIES = (
    "BREADTH_CONFIRMED_CONTINUATION",
    "BREADTH_FAILED_PROGRESS_REVERSAL",
)

# Every numerical choice is a singleton.  These constants are not a search
# grid and must not be expanded after outcomes have been viewed.
TIMEFRAME = "15m"
TRIGGER_QUANTILE = 0.55
CONTEXT_QUANTILE = 0.50
FAVORABLE_R = 1.0
ADVERSE_R = 1.0
MAXIMUM_HORIZON_MINUTES = 60
RISK_LEVEL = 1.0
COOLDOWN_MINUTES = 60
INTEGER_QUANTITY_TIER = 3
ACCOUNT_LABEL = "50K"
GOVERNOR_MODE = "CAUSAL_STATIC_STOP_RISK_GOVERNOR"
MAXIMUM_REFERENCE_STALENESS_MINUTES = 5
DISPERSION_WARMUP_ROWS = 30

DESIGN_BLOCKS = ("B1", "B2")
VALIDATION_BLOCKS = ("B3",)
FINAL_DEVELOPMENT_BLOCKS = ("B4",)

CONTROL_TARGET_ONLY = "TARGET_ONLY"
CONTROL_DIRECTION_FLIP = "DIRECTION_FLIP"
CONTROL_SINGLE_REFERENCE = "SINGLE_ES_REFERENCE"
CONTROL_NAMES = (
    CONTROL_TARGET_ONLY,
    CONTROL_DIRECTION_FLIP,
    CONTROL_SINGLE_REFERENCE,
)


class CrossIndexBreadthTripwireError(RuntimeError):
    """The frozen tripwire contract or its source evidence failed closed."""


@dataclass(frozen=True, slots=True)
class BreadthSpecification:
    target_market: str
    session_code: int
    family: str

    def __post_init__(self) -> None:
        if self.target_market not in INDEX_MARKETS:
            raise ValueError("unsupported cross-index target")
        if self.session_code not in SESSION_ROLES:
            raise ValueError("tripwire is frozen to OPEN/MID")
        if self.family not in FAMILIES:
            raise ValueError("unsupported frozen breadth family")

    @property
    def reference_markets(self) -> tuple[str, ...]:
        return tuple(value for value in INDEX_MARKETS if value != self.target_market)

    @property
    def specification_id(self) -> str:
        return (
            f"breadth:{self.target_market}:{SESSION_ROLES[self.session_code]}:"
            f"{self.family}"
        )

    @property
    def fingerprint(self) -> str:
        return stable_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "specification_id": self.specification_id,
            "target_market": self.target_market,
            "execution_market": EXECUTION_MARKETS[self.target_market],
            "reference_markets": list(self.reference_markets),
            "session_code": self.session_code,
            "session_role": SESSION_ROLES[self.session_code],
            "family": self.family,
            "timeframe": TIMEFRAME,
            "favorable_r": FAVORABLE_R,
            "adverse_r": ADVERSE_R,
            "maximum_horizon_minutes": MAXIMUM_HORIZON_MINUTES,
            "risk_level": RISK_LEVEL,
            "integer_quantity_tier": INTEGER_QUANTITY_TIER,
            "account_label": ACCOUNT_LABEL,
        }


def frozen_specifications() -> tuple[BreadthSpecification, ...]:
    """Return the immutable sixteen-specification primary inventory."""

    rows = tuple(
        BreadthSpecification(target, session_code, family)
        for target in INDEX_MARKETS
        for session_code in sorted(SESSION_ROLES)
        for family in FAMILIES
    )
    if len(rows) != 16 or len({row.fingerprint for row in rows}) != 16:
        raise CrossIndexBreadthTripwireError("frozen breadth inventory drift")
    return rows


def build_cross_index_breadth_view(
    primary: FeatureMatrix,
    references: Mapping[str, FeatureMatrix],
    *,
    maximum_staleness_minutes: int = MAXIMUM_REFERENCE_STALENESS_MINUTES,
) -> FeatureMatrix:
    """Attach three-peer breadth, dispersion and transition descriptors.

    The peer joins are performed independently against the primary decision
    timestamp.  A row is actionable only when all three distinct peers are
    finite and no more than five minutes stale.  Dispersion normalisation uses
    only observations strictly before the current row.
    """

    target = _matrix_market(primary)
    expected = set(INDEX_MARKETS) - {target}
    if set(references) != expected or len(references) != 3:
        raise ValueError(
            f"{target} breadth view requires exactly {sorted(expected)} references"
        )
    if maximum_staleness_minutes != MAXIMUM_REFERENCE_STALENESS_MINUTES:
        raise ValueError("reference-staleness contract is frozen at five minutes")

    decision = np.asarray(primary.array("decision_ns"), dtype=np.int64)
    timestamp = np.asarray(primary.array("timestamp_ns"), dtype=np.int64)
    session_day = np.asarray(primary.array("session_day"), dtype=np.int64)
    session_code = np.asarray(primary.array("session_code"), dtype=np.int64)
    segment_code = np.asarray(primary.array("segment_code"), dtype=np.int64)
    target_return = np.asarray(primary.array("feature__ctx_15m_return"), dtype=float)

    joined_values: list[np.ndarray] = []
    joined_lags: list[np.ndarray] = []
    joined_availability: list[np.ndarray] = []
    arrays = dict(primary.arrays)
    reference_hashes: dict[str, str] = {}
    for reference_market in sorted(references):
        reference = references[reference_market]
        if _matrix_market(reference) != reference_market:
            raise ValueError("reference mapping key and matrix identity differ")
        value, lag, source_available = _availability_safe_join(
            decision,
            reference,
            primary_session_day=session_day,
            primary_session_code=session_code,
            feature="ctx_15m_return",
            maximum_staleness_minutes=maximum_staleness_minutes,
        )
        joined_values.append(value)
        joined_lags.append(lag)
        joined_availability.append(source_available)
        arrays[f"feature__breadth_peer_{reference_market.lower()}_ctx_15m_return"] = value
        reference_hashes[reference_market] = reference.fingerprint

    peer = np.column_stack(joined_values)
    finite = np.all(np.isfinite(peer), axis=1)
    positive = np.sum(peer > 0.0, axis=1)
    negative = np.sum(peer < 0.0, axis=1)
    unanimous_sign = np.zeros(primary.row_count, dtype=np.int8)
    unanimous_sign[finite & (positive == 3)] = 1
    unanimous_sign[finite & (negative == 3)] = -1

    absolute_median = np.full(primary.row_count, np.nan, dtype=np.float64)
    absolute_median[finite] = np.median(np.abs(peer[finite]), axis=1)
    consensus = np.where(
        unanimous_sign != 0,
        unanimous_sign.astype(float) * absolute_median,
        np.nan,
    )

    dispersion = np.full(primary.row_count, np.nan, dtype=np.float64)
    dispersion[finite] = np.std(peer[finite], axis=1)
    dispersion_z = _causal_prior_zscore(dispersion)

    boundary = (((timestamp // MINUTE_NS) + 1) % 15) == 0
    transition = np.full(primary.row_count, np.nan, dtype=np.float64)
    failed_progress = np.full(primary.row_count, np.nan, dtype=np.float64)
    prior_state: dict[tuple[int, int, int], int] = {}
    for raw_index in np.flatnonzero(boundary):
        index = int(raw_index)
        key = (
            int(segment_code[index]),
            int(session_day[index]),
            int(session_code[index]),
        )
        state = int(unanimous_sign[index])
        previous = prior_state.get(key, 0)
        if state != 0 and state != previous:
            transition[index] = float(consensus[index])
            if int(np.sign(target_return[index])) != state:
                failed_progress[index] = float(consensus[index])
        prior_state[key] = state

    lag_stack = np.column_stack(joined_lags)
    maximum_lag = np.full(primary.row_count, np.nan, dtype=np.float64)
    valid_lags = np.all(np.isfinite(lag_stack), axis=1)
    maximum_lag[valid_lags] = np.max(lag_stack[valid_lags], axis=1)

    # Every source used by the decision is represented in ``available_at``.
    # Missing references suppress the feature and retain a negative sentinel;
    # they can never manufacture an earlier effective availability timestamp.
    effective_availability = np.asarray(
        primary.array("availability_ns"), dtype=np.int64
    ).copy()
    for source_available in joined_availability:
        valid_source = source_available >= 0
        effective_availability[valid_source] = np.maximum(
            effective_availability[valid_source], source_available[valid_source]
        )
    effective_availability.flags.writeable = False
    arrays["availability_ns"] = effective_availability

    derived = {
        "feature__cross_index_breadth_consensus": consensus,
        "feature__cross_index_breadth_dispersion": dispersion,
        "feature__cross_index_breadth_dispersion_prior_z": dispersion_z,
        "feature__cross_index_breadth_transition": transition,
        "feature__cross_index_failed_progress_transition": failed_progress,
        "feature__cross_index_reference_maximum_lag_minutes": maximum_lag,
    }
    for name, value in derived.items():
        frozen = np.asarray(value)
        frozen.flags.writeable = False
        arrays[name] = frozen

    feature_receipt = {
        "schema": FEATURE_SCHEMA,
        "target_market": target,
        "primary_matrix_hash": primary.fingerprint,
        "reference_matrix_hashes": dict(sorted(reference_hashes.items())),
        "reference_feature": "ctx_15m_return",
        "join_contract": "LATEST_REFERENCE_AVAILABLE_AT_OR_BEFORE_PRIMARY_DECISION",
        "maximum_staleness_minutes": maximum_staleness_minutes,
        "breadth_contract": "THREE_OF_THREE_PEER_UNANIMITY_TRANSITION",
        "dispersion_contract": "CROSS_SECTIONAL_STD_NORMALIZED_BY_STRICTLY_PRIOR_ROWS",
        "transition_timeframe": TIMEFRAME,
        "target_excluded": True,
        "finite_three_peer_row_count": int(np.sum(finite)),
        "unanimous_transition_count": int(np.sum(np.isfinite(transition))),
        "failed_progress_transition_count": int(np.sum(np.isfinite(failed_progress))),
    }
    manifest = dict(primary.manifest)
    manifest["bundle_hash"] = stable_hash(feature_receipt)
    manifest["cross_index_breadth_receipt"] = feature_receipt
    return FeatureMatrix(root=primary.root, manifest=manifest, arrays=arrays)


def run_cross_index_breadth_tripwire(
    root: str | Path,
    *,
    manifest_path: str | Path = DEFAULT_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
    specification_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Execute the frozen tripwire without writing or promoting anything."""

    started = time.perf_counter()
    project = Path(root).resolve()
    manifest_file = _inside(project, manifest_path)
    rules_file = _inside(project, rule_snapshot_path)
    manifest = _load_self_hashed_manifest(manifest_file)
    if str(manifest.get("campaign_id")) != SOURCE_CAMPAIGN_ID:
        raise CrossIndexBreadthTripwireError("source campaign identity drift")

    rules, rule_receipt = _load_rule_snapshot(rules_file)
    rule = rules[ACCOUNT_LABEL]
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    bindings = dict(dict(manifest["data"])["feature_matrix_bindings"])
    matrices = {
        market: _open_bound_matrix(project, bindings, market)
        for market in INDEX_MARKETS
    }
    evaluation_start_ns = _date_ns(
        str(dict(manifest["data"])["evaluation_start_inclusive"])
    )
    evaluation_end_ns = _date_ns(
        str(dict(manifest["data"])["evaluation_end_exclusive"])
    )

    inventory = frozen_specifications()
    if specification_ids is not None:
        requested = tuple(str(value) for value in specification_ids)
        known = {row.specification_id: row for row in inventory}
        if not requested or len(set(requested)) != len(requested):
            raise CrossIndexBreadthTripwireError("invalid specification subset")
        unknown = sorted(set(requested) - set(known))
        if unknown:
            raise CrossIndexBreadthTripwireError(f"unknown specifications: {unknown}")
        inventory = tuple(known[value] for value in requested)

    results: list[dict[str, Any]] = []
    account_replays = 0
    primary_candidates = 0
    control_candidates = 0
    feature_receipts: dict[str, Any] = {}

    by_target: dict[str, list[BreadthSpecification]] = {}
    for specification in inventory:
        by_target.setdefault(specification.target_market, []).append(specification)

    for target in sorted(by_target):
        primary_matrix = matrices[target]
        reference_matrices = {
            market: matrices[market]
            for market in INDEX_MARKETS
            if market != target
        }
        breadth_view = build_cross_index_breadth_view(
            primary_matrix, reference_matrices
        )
        feature_receipts[target] = dict(
            breadth_view.manifest["cross_index_breadth_receipt"]
        )

        for specification in by_target[target]:
            primary_candidate = _primary_candidate(specification)
            primary = _evaluate_candidate(
                project=project,
                candidate=primary_candidate,
                matrix=breadth_view,
                evaluation_start_ns=evaluation_start_ns,
                evaluation_end_ns=evaluation_end_ns,
                calendar=calendar,
                starts=starts,
                rule=rule,
                evidence_role="PRIMARY_DEVELOPMENT_TRIPWIRE",
                control_name=None,
            )
            primary_candidates += 1
            account_replays += int(primary["exact_account_replays"])

            controls: dict[str, Any] = {}
            for control_name in CONTROL_NAMES:
                control_matrix, control_candidate = _control_candidate(
                    specification,
                    control_name=control_name,
                    primary_matrix=primary_matrix,
                    breadth_view=breadth_view,
                    matrices=matrices,
                )
                control = _evaluate_candidate(
                    project=project,
                    candidate=control_candidate,
                    matrix=control_matrix,
                    evaluation_start_ns=evaluation_start_ns,
                    evaluation_end_ns=evaluation_end_ns,
                    calendar=calendar,
                    starts=starts,
                    rule=rule,
                    evidence_role="DEVELOPMENT_CONTROL",
                    control_name=control_name,
                )
                controls[control_name] = control
                control_candidates += 1
                account_replays += int(control["exact_account_replays"])

            result = {
                "specification": specification.to_dict(),
                "specification_fingerprint": specification.fingerprint,
                "primary": primary,
                "controls": controls,
                "control_deltas": _control_deltas(primary, controls),
            }
            result["result_hash"] = stable_hash(result)
            results.append(result)

    gate = decide_tripwire_gate(results)
    decisive = specification_ids is None
    status = (
        str(gate["status"])
        if decisive
        else "NON_DECISIONAL_SUBSET_SMOKE_COMPLETE"
    )
    core = {
        "schema": SCHEMA,
        "status": status,
        "campaign_role": "BOUNDED_MATERIALLY_DISTINCT_DEVELOPMENT_TRIPWIRE",
        "source_campaign_id": SOURCE_CAMPAIGN_ID,
        "source_manifest": {
            "path": str(manifest_file),
            "manifest_hash": str(manifest["manifest_hash"]),
        },
        "official_rule_snapshot": rule_receipt,
        "frozen_grid": grid_receipt,
        "selection_roles": {
            "DESIGN": list(DESIGN_BLOCKS),
            "VALIDATION": list(VALIDATION_BLOCKS),
            "FINAL_DEVELOPMENT": list(FINAL_DEVELOPMENT_BLOCKS),
            "independent_confirmation_claimed": False,
            "all_blocks_already_viewed_development": True,
        },
        "frozen_contract": frozen_contract(),
        "feature_receipts": feature_receipts,
        "results": results,
        "gate": gate,
        "counts": {
            "primary_specification_count": len(inventory),
            "frozen_primary_inventory_count": 16,
            "control_candidate_count": control_candidates,
            "primary_candidate_count": primary_candidates,
            "exact_account_replays": account_replays,
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "database_writes": 0,
            "registry_writes": 0,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        "promotion_status": None,
        "next_action": (
            "PERSIST_WITH_SINGLE_AUTHORITATIVE_WRITER_AND_APPLY_DIRECTOR_BRANCH_RULE"
            if decisive
            else "RUN_COMPLETE_FROZEN_INVENTORY_BEFORE_ANY_DECISION"
        ),
        "runtime_seconds": time.perf_counter() - started,
        "completed_at_utc": datetime.now(UTC).isoformat(),
    }
    # Runtime and wall-clock completion metadata are operational diagnostics,
    # not economic evidence.  Excluding them makes an identical read-only
    # replay produce the same authoritative result hash.
    core["result_hash"] = stable_hash(
        {
            key: value
            for key, value in core.items()
            if key not in {"runtime_seconds", "completed_at_utc"}
        }
    )
    return core


def frozen_contract() -> dict[str, Any]:
    """Machine-readable preregistration for audit and targeted tests."""

    return {
        "target_markets": list(INDEX_MARKETS),
        "session_roles": dict(SESSION_ROLES),
        "families": list(FAMILIES),
        "primary_specification_count": 16,
        "controls": list(CONTROL_NAMES),
        "reference_count": 3,
        "target_excluded_from_reference_set": True,
        "reference_join": "ASOF_AVAILABLE_AT_OR_BEFORE_DECISION_MAX_5_MINUTES",
        "breadth_transition": "NON_UNANIMOUS_TO_THREE_OF_THREE_UNANIMOUS",
        "timeframe": TIMEFRAME,
        "trigger_quantile": TRIGGER_QUANTILE,
        "context_quantile": CONTEXT_QUANTILE,
        "favorable_r": FAVORABLE_R,
        "adverse_r": ADVERSE_R,
        "maximum_horizon_minutes": MAXIMUM_HORIZON_MINUTES,
        "risk_level": RISK_LEVEL,
        "integer_quantity_tier": INTEGER_QUANTITY_TIER,
        "account_label": ACCOUNT_LABEL,
        "account_horizons_days": list(HORIZONS),
        "design_blocks": list(DESIGN_BLOCKS),
        "validation_blocks": list(VALIDATION_BLOCKS),
        "final_development_blocks": list(FINAL_DEVELOPMENT_BLOCKS),
        "threshold_grid": False,
        "risk_grid": False,
        "confirmation_data_used": False,
        "promotion_authorized": False,
        "xfa_authorized": False,
    }


def decide_tripwire_gate(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Apply the preregistered, fail-closed development-only family gate."""

    qualifiers: list[dict[str, Any]] = []
    weak_ids: list[str] = []
    for raw in results:
        row = dict(raw)
        spec = dict(row["specification"])
        primary = dict(row["primary"])
        controls = dict(row.get("control_deltas") or {})
        behavior_hash = str(primary.get("realized_behavior_hash") or "")
        for cell_value in primary.get("cells", []):
            cell = dict(cell_value)
            normal = dict(cell.get("normal") or {})
            stressed = dict(cell.get("stressed") or {})
            if not normal or not stressed:
                continue
            pass_blocks = set(normal.get("blocks_with_passes", [])) & set(
                stressed.get("blocks_with_passes", [])
            )
            matching_control = dict(
                controls.get(str(cell.get("horizon_trading_days"))) or {}
            )
            control_dominance = int(matching_control.get("controls_beaten", 0))
            same_cell_pass = (
                int(normal.get("pass_count", 0)) > 0
                and int(stressed.get("pass_count", 0)) > 0
            )
            if same_cell_pass or float(stressed.get("net_total_usd", 0.0)) > 0.0:
                weak_ids.append(str(spec["specification_id"]))
            qualifies = (
                same_cell_pass
                and len(pass_blocks) >= 2
                and float(stressed.get("net_total_usd", 0.0)) > 0.0
                and float(stressed.get("mll_breach_rate", 1.0)) <= 0.10
                and bool(stressed.get("all_passing_paths_consistency_compliant", False))
                and control_dominance >= 2
                and bool(behavior_hash)
            )
            if qualifies:
                qualifiers.append(
                    {
                        "specification_id": str(spec["specification_id"]),
                        "target_market": str(spec["target_market"]),
                        "family": str(spec["family"]),
                        "session_role": str(spec["session_role"]),
                        "horizon_trading_days": int(cell["horizon_trading_days"]),
                        "behavior_hash": behavior_hash,
                        "shared_normal_stressed_pass_blocks": sorted(pass_blocks),
                        "controls_beaten": control_dominance,
                    }
                )

    distinct_behavior = {row["behavior_hash"] for row in qualifiers}
    distinct_families = {row["family"] for row in qualifiers}
    outside_nq_ym = any(row["target_market"] in {"ES", "RTY"} for row in qualifiers)
    green = (
        len(distinct_behavior) >= 2
        and len(distinct_families) >= 2
        and outside_nq_ym
    )
    if green:
        status = "CROSS_INDEX_BREADTH_TRIPWIRE_GREEN_DEVELOPMENT_ONLY"
        next_action = "FREEZE_QUALIFIERS_FOR_A_SEPARATE_UNTOUCHED_CONFIRMATION_CONTRACT"
    elif qualifiers or weak_ids:
        status = "CROSS_INDEX_BREADTH_TRIPWIRE_WEAK_DEVELOPMENT_ONLY"
        next_action = "PRESERVE_DIAGNOSTICS_AND_RETURN_DIRECTOR_TO_DISTINCT_BRANCH_SELECTION"
    else:
        status = "CROSS_INDEX_BREADTH_TRIPWIRE_FALSIFIED"
        next_action = "TOMBSTONE_EXACT_BREADTH_GRAMMAR_AND_SELECT_DISTINCT_BRANCH"
    return {
        "status": status,
        "green": green,
        "qualifying_cells": qualifiers,
        "qualifying_cell_count": len(qualifiers),
        "distinct_qualifying_behavior_count": len(distinct_behavior),
        "distinct_qualifying_family_count": len(distinct_families),
        "qualifier_outside_nq_ym": outside_nq_ym,
        "weak_specification_ids": sorted(set(weak_ids)),
        "authoritative_promotion_count": 0,
        "independent_confirmation_claimed": False,
        "xfa_paths_started": 0,
        "next_action": next_action,
    }


def _primary_candidate(specification: BreadthSpecification) -> HazardCandidate:
    failed = specification.family == "BREADTH_FAILED_PROGRESS_REVERSAL"
    return HazardCandidate(
        market=specification.target_market,
        execution_market=EXECUTION_MARKETS[specification.target_market],
        mechanism=specification.family,
        cross_asset_reference_market=None,
        timeframe=TIMEFRAME,
        session_code=specification.session_code,
        trigger_feature=(
            "cross_index_failed_progress_transition"
            if failed
            else "cross_index_breadth_transition"
        ),
        trigger_operator="ABS_GT",
        trigger_quantile=_require_frozen_quantile(TRIGGER_QUANTILE, QUANTILES),
        context_feature="cross_index_breadth_consensus",
        context_operator="SAME_SIGN",
        context_quantile=_require_frozen_quantile(
            CONTEXT_QUANTILE, CONTEXT_QUANTILES
        ),
        direction_rule=(
            "CONTEXT_SIGN_REVERSAL" if failed else "TRIGGER_SIGN_CONTINUATION"
        ),
        favorable_r=FAVORABLE_R,
        adverse_r=ADVERSE_R,
        horizon=MAXIMUM_HORIZON_MINUTES,
        risk_level=RISK_LEVEL,
        cooldown_minutes=COOLDOWN_MINUTES,
        version=1,
    )


def _control_candidate(
    specification: BreadthSpecification,
    *,
    control_name: str,
    primary_matrix: FeatureMatrix,
    breadth_view: FeatureMatrix,
    matrices: Mapping[str, FeatureMatrix],
) -> tuple[FeatureMatrix, HazardCandidate]:
    failed = specification.family == "BREADTH_FAILED_PROGRESS_REVERSAL"
    base = {
        "market": specification.target_market,
        "execution_market": EXECUTION_MARKETS[specification.target_market],
        "timeframe": TIMEFRAME,
        "session_code": specification.session_code,
        "trigger_quantile": _require_frozen_quantile(TRIGGER_QUANTILE, QUANTILES),
        "favorable_r": FAVORABLE_R,
        "adverse_r": ADVERSE_R,
        "horizon": MAXIMUM_HORIZON_MINUTES,
        "risk_level": RISK_LEVEL,
        "cooldown_minutes": COOLDOWN_MINUTES,
        "version": 1,
    }
    if control_name == CONTROL_DIRECTION_FLIP:
        primary = _primary_candidate(specification)
        candidate = HazardCandidate(
            **base,
            mechanism=f"{specification.family}_DIRECTION_FLIP_CONTROL",
            cross_asset_reference_market=None,
            trigger_feature=primary.trigger_feature,
            trigger_operator=primary.trigger_operator,
            context_feature=primary.context_feature,
            context_operator=primary.context_operator,
            context_quantile=primary.context_quantile,
            direction_rule=(
                "TRIGGER_SIGN_CONTINUATION"
                if failed
                else "CONTEXT_SIGN_REVERSAL"
            ),
        )
        return breadth_view, candidate

    if control_name == CONTROL_TARGET_ONLY:
        candidate = HazardCandidate(
            **base,
            mechanism=f"{specification.family}_TARGET_ONLY_CONTROL",
            cross_asset_reference_market=None,
            trigger_feature="ctx_15m_return",
            trigger_operator="ABS_GT",
            context_feature=None,
            context_operator=None,
            context_quantile=None,
            direction_rule=(
                "PAST_RETURN_REVERSAL" if failed else "TRIGGER_SIGN_CONTINUATION"
            ),
        )
        return primary_matrix, candidate

    if control_name == CONTROL_SINGLE_REFERENCE:
        reference_market = "NQ" if specification.target_market == "ES" else "ES"
        view = with_availability_safe_cross_asset_feature(
            primary_matrix,
            matrices[reference_market],
            reference_feature="ctx_15m_return",
            output_feature="cross_asset_ctx_15m_return",
            maximum_staleness_minutes=MAXIMUM_REFERENCE_STALENESS_MINUTES,
        )
        candidate = HazardCandidate(
            **base,
            mechanism="CROSS_ASSET_STATE",
            cross_asset_reference_market=reference_market,
            trigger_feature="ctx_15m_return",
            trigger_operator="ABS_GT",
            context_feature="cross_asset_ctx_15m_return",
            context_operator="SAME_SIGN",
            context_quantile=_require_frozen_quantile(
                CONTEXT_QUANTILE, CONTEXT_QUANTILES
            ),
            direction_rule=(
                "CONTEXT_SIGN_REVERSAL" if failed else "TRIGGER_SIGN_CONTINUATION"
            ),
        )
        return view, candidate
    raise ValueError(f"unknown tripwire control: {control_name}")


def _evaluate_candidate(
    *,
    project: Path,
    candidate: HazardCandidate,
    matrix: FeatureMatrix,
    evaluation_start_ns: int,
    evaluation_end_ns: int,
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    rule: Mapping[str, Any],
    evidence_role: str,
    control_name: str | None,
) -> dict[str, Any]:
    try:
        calibrated = calibrate_candidate(
            candidate,
            matrix,
            calibration_end_exclusive_ns=evaluation_start_ns,
            minimum_observations=20,
        )
    except ValueError as exc:
        return _empty_candidate_result(
            candidate,
            evidence_role=evidence_role,
            control_name=control_name,
            reason=f"CALIBRATION_FAILED:{exc}",
        )

    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=evaluation_start_ns,
        evaluation_end_exclusive_ns=evaluation_end_ns,
    )
    streaming = discover_intents_streaming(
        calibrated,
        matrix,
        evaluation_start_ns=evaluation_start_ns,
        evaluation_end_exclusive_ns=evaluation_end_ns,
    )
    batch_identity = tuple((row.row_index, row.direction) for row in intents)
    if batch_identity != streaming:
        raise CrossIndexBreadthTripwireError("batch/stream decision drift")

    eligible_days = frozen_eligible_session_calendar(
        candidate,
        matrix,
        evaluation_start_ns=evaluation_start_ns,
        evaluation_end_exclusive_ns=evaluation_end_ns,
    )
    events = observe_outcomes(calibrated, matrix, intents)
    block_by_day = _block_by_session_day(matrix, events)
    screen = screen_result(
        calibrated,
        events,
        eligible_session_days=eligible_days,
        block_by_session_day=block_by_day,
    )
    replay = exact_sleeve_replay(
        calibrated,
        events,
        eligible_session_days=eligible_days,
    )
    coverage = _candidate_coverage(replay, calendar, starts)
    normal, normal_session_violations = _apply_session_contract(
        replay.normal_trajectories
    )
    stressed, stressed_session_violations = _apply_session_contract(
        replay.stressed_trajectories
    )
    if normal_session_violations != stressed_session_violations:
        raise CrossIndexBreadthTripwireError("scenario session-contract drift")

    event_mappings = [row.to_dict() for row in events]
    if not replay.normal_trajectories:
        return {
            **_empty_candidate_result(
                candidate,
                evidence_role=evidence_role,
                control_name=control_name,
                reason="NO_COMPLETED_EXECUTABLE_EVENTS",
            ),
            "calibration": _calibration_receipt(calibrated),
            "screen": screen.to_dict(),
            "emitted_intent_count": len(intents),
            "censored_event_count": len(events),
            "batch_stream_equal": True,
        }

    declared_charge = _declared_stop_risk_charge_per_mini(
        event_mappings, candidate.payload
    )
    account_contract_limit = _market_contract_limit_mini(candidate.payload, rule)
    scaled_normal = tuple(
        scale_causal_trajectory(
            row, executable_quantity_multiplier=INTEGER_QUANTITY_TIER
        )
        for row in normal
    )
    scaled_stressed = tuple(
        scale_causal_trajectory(
            row, executable_quantity_multiplier=INTEGER_QUANTITY_TIER
        )
        for row in stressed
    )
    _require_scenario_identity(scaled_normal, scaled_stressed)
    maximum_mini = max(
        (float(row.event.mini_equivalent) for row in scaled_normal), default=0.0
    )
    if maximum_mini > account_contract_limit + 1e-12:
        return {
            **_empty_candidate_result(
                candidate,
                evidence_role=evidence_role,
                control_name=control_name,
                reason="FIXED_TIER_EXCEEDS_50K_CONTRACT_LIMIT",
            ),
            "calibration": _calibration_receipt(calibrated),
            "screen": screen.to_dict(),
            "maximum_mini_equivalent": maximum_mini,
            "account_contract_limit_mini": account_contract_limit,
            "batch_stream_equal": True,
        }

    policy = _standalone_policy(
        candidate.candidate_id,
        rule,
        tier=INTEGER_QUANTITY_TIER,
        declared_risk_charge_per_mini=declared_charge,
        account_contract_limit=account_contract_limit,
        governor_mode=GOVERNOR_MODE,
    )
    config = _account_config(rule)
    cells: list[dict[str, Any]] = []
    exact_account_replays = 0
    for horizon in HORIZONS:
        episode_sets: dict[str, list[tuple[Any, str]]] = {
            "NORMAL": [],
            "STRESSED_1_5X": [],
        }
        for start_day, block in coverage["starts"][horizon]:
            for scenario, trajectories in (
                ("NORMAL", scaled_normal),
                ("STRESSED_1_5X", scaled_stressed),
            ):
                episode = run_causal_shared_account_episode(
                    {candidate.candidate_id: trajectories},
                    calendar,
                    policy=policy,
                    start_day=int(start_day),
                    maximum_duration_days=horizon,
                    config=config,
                )
                episode_sets[scenario].append((episode, block))
                exact_account_replays += 1
        cell = _exact_cell(
            candidate.candidate_id,
            ACCOUNT_LABEL,
            rule,
            tier=INTEGER_QUANTITY_TIER,
            horizon=horizon,
            maximum_mini=maximum_mini,
            account_contract_limit=account_contract_limit,
            declared_risk_charge=declared_charge,
            governor_mode=GOVERNOR_MODE,
            policy=policy,
            requested_starts=len(starts[horizon]),
            censored_starts=coverage["censored"][horizon],
            episodes=episode_sets,
        )
        for scenario_name in ("normal", "stressed"):
            summary = dict(cell[scenario_name])
            passing = int(summary.get("pass_count", 0))
            summary["all_passing_paths_consistency_compliant"] = (
                passing > 0
                and _passing_paths_consistency_compliant(
                    episode_sets[
                        "NORMAL" if scenario_name == "normal" else "STRESSED_1_5X"
                    ]
                )
            )
            summary["blocks_with_passes"] = sorted(
                block
                for block, values in dict(summary.get("by_block") or {}).items()
                if int(dict(values).get("pass_count", 0)) > 0
            )
            cell[scenario_name] = summary
        cells.append(cell)

    behavior_hash = stable_hash(
        [
            {
                "session_day": row.session_day,
                "decision_time_ns": row.decision_time_ns,
                "fill_time_ns": row.fill_time_ns,
                "direction": row.direction,
                "outcome_time_ns": row.outcome_time_ns,
            }
            for row in events
        ]
    )
    result = {
        "candidate": candidate.payload,
        "candidate_id": candidate.candidate_id,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "realized_behavior_hash": behavior_hash,
        "evidence_role": evidence_role,
        "control_name": control_name,
        "calibration": _calibration_receipt(calibrated),
        "screen": screen.to_dict(),
        "coverage": coverage["receipt"],
        "emitted_intent_count": len(intents),
        "completed_event_count": len(replay.normal_trajectories),
        "censored_event_count": len(events) - len(replay.normal_trajectories),
        "batch_stream_equal": True,
        "session_contract_violation_count": normal_session_violations,
        "declared_stop_risk_charge_per_mini_usd": declared_charge,
        "maximum_mini_equivalent": maximum_mini,
        "account_contract_limit_mini": account_contract_limit,
        "cells": cells,
        "exact_account_replays": exact_account_replays,
        "promotion_status": None,
        "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        "xfa_paths_started": 0,
    }
    result["candidate_result_hash"] = stable_hash(result)
    return result


def _control_deltas(
    primary: Mapping[str, Any], controls: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    primary_cells = {
        int(row["horizon_trading_days"]): dict(row)
        for row in primary.get("cells", [])
    }
    for horizon, primary_cell in sorted(primary_cells.items()):
        primary_stressed = dict(primary_cell.get("stressed") or {})
        comparisons: dict[str, Any] = {}
        beaten = 0
        for name, control in controls.items():
            control_cell = next(
                (
                    dict(row)
                    for row in control.get("cells", [])
                    if int(row["horizon_trading_days"]) == horizon
                ),
                None,
            )
            if control_cell is None:
                comparisons[name] = {"available": False}
                continue
            stressed = dict(control_cell.get("stressed") or {})
            pass_delta = float(primary_stressed.get("pass_rate", 0.0)) - float(
                stressed.get("pass_rate", 0.0)
            )
            progress_delta = float(
                primary_stressed.get("target_progress_median", 0.0)
            ) - float(stressed.get("target_progress_median", 0.0))
            net_delta = float(primary_stressed.get("net_total_usd", 0.0)) - float(
                stressed.get("net_total_usd", 0.0)
            )
            control_beaten = (
                pass_delta >= 0.0
                and progress_delta > 0.0
                and net_delta > 0.0
                and (pass_delta > 0.0 or progress_delta >= 0.05)
            )
            beaten += int(control_beaten)
            comparisons[name] = {
                "available": True,
                "stressed_pass_rate_delta": pass_delta,
                "stressed_target_progress_median_delta": progress_delta,
                "stressed_net_total_usd_delta": net_delta,
                "primary_beats_control": control_beaten,
            }
        output[str(horizon)] = {
            "controls_beaten": beaten,
            "comparisons": comparisons,
        }
    return output


def _empty_candidate_result(
    candidate: HazardCandidate,
    *,
    evidence_role: str,
    control_name: str | None,
    reason: str,
) -> dict[str, Any]:
    result = {
        "candidate": candidate.payload,
        "candidate_id": candidate.candidate_id,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "realized_behavior_hash": None,
        "evidence_role": evidence_role,
        "control_name": control_name,
        "status": "FAIL_CLOSED_NO_ECONOMIC_RESULT",
        "reason": reason,
        "cells": [],
        "exact_account_replays": 0,
        "promotion_status": None,
        "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        "xfa_paths_started": 0,
    }
    result["candidate_result_hash"] = stable_hash(result)
    return result


def _availability_safe_join(
    primary_decision: np.ndarray,
    reference: FeatureMatrix,
    *,
    primary_session_day: np.ndarray,
    primary_session_code: np.ndarray,
    feature: str,
    maximum_staleness_minutes: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    reference_available = np.asarray(reference.array("availability_ns"), dtype=np.int64)
    if np.any(reference_available[1:] < reference_available[:-1]):
        raise ValueError("reference availability is not chronological")
    positions = np.searchsorted(reference_available, primary_decision, side="right") - 1
    joined = np.full(len(primary_decision), np.nan, dtype=np.float64)
    lag_minutes = np.full(len(primary_decision), np.nan, dtype=np.float64)
    source_available = np.full(len(primary_decision), -1, dtype=np.int64)
    valid = positions >= 0
    valid_indices = np.flatnonzero(valid)
    lag_ns = (
        primary_decision[valid_indices]
        - reference_available[positions[valid_indices]]
    )
    reference_day = np.asarray(reference.array("session_day"), dtype=np.int64)
    reference_session = np.asarray(reference.array("session_code"), dtype=np.int64)
    same_session = (
        reference_day[positions[valid_indices]] == primary_session_day[valid_indices]
    ) & (
        reference_session[positions[valid_indices]]
        == primary_session_code[valid_indices]
    )
    fresh = (lag_ns >= 0) & same_session & (
        lag_ns <= int(maximum_staleness_minutes) * MINUTE_NS
    )
    valid[valid_indices[~fresh]] = False
    source = np.asarray(reference.array(f"feature__{feature}"), dtype=float)
    joined[valid] = source[positions[valid]]
    lag_minutes[valid] = (
        primary_decision[valid] - reference_available[positions[valid]]
    ) / MINUTE_NS
    source_available[valid] = reference_available[positions[valid]]
    joined.flags.writeable = False
    lag_minutes.flags.writeable = False
    source_available.flags.writeable = False
    return joined, lag_minutes, source_available


def _causal_prior_zscore(values: np.ndarray) -> np.ndarray:
    finite = np.isfinite(values)
    clean = np.where(finite, values, 0.0)
    cumulative_count = np.cumsum(finite.astype(np.int64))
    cumulative_sum = np.cumsum(clean, dtype=np.float64)
    cumulative_sum_sq = np.cumsum(clean * clean, dtype=np.float64)
    prior_count = cumulative_count - finite.astype(np.int64)
    prior_sum = cumulative_sum - clean
    prior_sum_sq = cumulative_sum_sq - clean * clean
    output = np.full(len(values), np.nan, dtype=np.float64)
    valid = finite & (prior_count >= DISPERSION_WARMUP_ROWS)
    mean = np.zeros(len(values), dtype=np.float64)
    variance = np.zeros(len(values), dtype=np.float64)
    mean[valid] = prior_sum[valid] / prior_count[valid]
    variance[valid] = np.maximum(
        0.0,
        prior_sum_sq[valid] / prior_count[valid] - mean[valid] * mean[valid],
    )
    nondegenerate = valid & (variance > 1e-24)
    output[nondegenerate] = (
        values[nondegenerate] - mean[nondegenerate]
    ) / np.sqrt(variance[nondegenerate])
    output.flags.writeable = False
    return output


def _block_by_session_day(
    matrix: FeatureMatrix, events: Sequence[Any]
) -> dict[int, str]:
    # Block identities are inferred only from frozen dates in the source
    # manifest; event outcomes never enter this mapping.
    receipt = dict(matrix.manifest.get("cross_index_breadth_receipt") or {})
    del receipt  # Explicitly demonstrate that derived outcomes are unused.
    boundaries = (
        ("B1", "2023-07-03", "2023-10-05"),
        ("B2", "2023-10-27", "2024-02-02"),
        ("B3", "2024-02-26", "2024-05-31"),
        ("B4", "2024-06-17", "2024-09-26"),
    )
    result: dict[int, str] = {}
    for row in events:
        day = int(row.session_day)
        day_ns = day * 86_400 * 1_000_000_000
        for block, start, end in boundaries:
            if _date_ns(start) <= day_ns <= _date_ns(end):
                result[day] = block
                break
    return result


def _passing_paths_consistency_compliant(
    episodes: Sequence[tuple[Any, str]],
) -> bool:
    passing = [episode for episode, _block in episodes if bool(episode.passed)]
    return bool(passing) and all(bool(episode.consistency_ok) for episode in passing)


def _calibration_receipt(calibrated: CalibratedHazardCandidate) -> dict[str, Any]:
    return {
        "calibration_end_exclusive_ns": calibrated.calibration_end_exclusive_ns,
        "trigger_threshold": calibrated.trigger_threshold,
        "context_threshold": calibrated.context_threshold,
        "finite_trigger_observations": calibrated.finite_trigger_observations,
        "finite_context_observations": calibrated.finite_context_observations,
        "source_matrix_hash": calibrated.source_matrix_hash,
        "calibration_hash": calibrated.fingerprint,
    }


def _matrix_market(matrix: FeatureMatrix) -> str:
    value = str(dict(matrix.manifest.get("key") or {}).get("market") or "")
    if value not in INDEX_MARKETS:
        raise ValueError(f"matrix market is not an index target: {value!r}")
    return value


def _open_bound_matrix(
    project: Path, bindings: Mapping[str, Any], market: str
) -> FeatureMatrix:
    if market not in bindings:
        raise CrossIndexBreadthTripwireError(f"missing frozen matrix binding: {market}")
    binding = dict(bindings[market])
    path = _inside(project, str(binding["path"]))
    matrix = FeatureMatrix.open(path.parent, mmap=True)
    if _matrix_market(matrix) != market:
        raise CrossIndexBreadthTripwireError("matrix binding identity drift")
    return matrix


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CrossIndexBreadthTripwireError("path escapes project root") from exc
    if not resolved.is_file():
        raise CrossIndexBreadthTripwireError(f"required file is absent: {resolved}")
    return resolved


def _date_ns(value: str) -> int:
    return int(
        datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp()
        * 1_000_000_000
    )


def _require_frozen_quantile(value: float, allowed: Sequence[float]) -> float:
    if value not in allowed:
        raise CrossIndexBreadthTripwireError(
            f"frozen singleton quantile {value} is unsupported by the causal kernel"
        )
    return value


__all__ = [
    "BreadthSpecification",
    "CrossIndexBreadthTripwireError",
    "build_cross_index_breadth_view",
    "decide_tripwire_gate",
    "frozen_contract",
    "frozen_specifications",
    "run_cross_index_breadth_tripwire",
]
