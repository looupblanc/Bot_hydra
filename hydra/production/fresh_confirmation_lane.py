"""Isolated, fail-closed fresh-confirmation lane for the frozen YM Tier-G set.

This module has deliberately no mission-runtime, service, database, registry or
network side effects.  It freezes the four current YM Tier-G policies and one
development-only breadth diagnostic against a single predeclared Databento
request.  Acquisition and authoritative persistence remain the controller's
responsibility.

The request is intentionally limited to YM/MYM/ES.  The frozen three-peer
``BREADTH_CONFIRMED_CONTINUATION`` diagnostic also requires NQ and RTY, so it
fails closed under this request instead of silently changing its semantics.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.features.feature_matrix import FeatureMatrix
from hydra.features.canonical_store import CanonicalFeatureKey, CanonicalFeatureStore
from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_loader import normalize_ohlcv_frame, validate_ohlcv_frame
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _build_past_only_feature_frame,
)
from hydra.production.autonomous_exact_replay import (
    HORIZONS,
    _account_config,
    _apply_session_contract,
    _candidate_coverage,
    _exact_cell,
    _load_rule_snapshot,
    _market_contract_limit_mini,
    _require_scenario_identity,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.cross_index_breadth_tripwire import (
    BreadthSpecification,
    build_cross_index_breadth_view,
    _primary_candidate as _breadth_candidate,
)
from hydra.research.causal_target_velocity import (
    CalibratedHazardCandidate,
    HazardCandidate,
    calibrate_candidate,
    discover_intents_batch,
    discover_intents_streaming,
    exact_sleeve_replay,
    frozen_eligible_session_calendar,
    observe_outcomes,
    with_availability_safe_cross_asset_feature,
)
from hydra.research.qd_economic_tournament import _prepare_feature_frame
from hydra.research.turbo_feature_builder import (
    FEATURE_DAG_HASH,
    _market_arrays,
)


SCHEMA = "hydra_fresh_confirmation_contract_v1"
RESULT_SCHEMA = "hydra_fresh_confirmation_result_v1"
DATASET = "GLBX.MDP3"
DATA_SCHEMA = "ohlcv-1m"
SYMBOLS = ("YM.c.0", "MYM.c.0", "ES.c.0")
START = "2025-01-02"
END = "2025-07-01"
DATA_ROLE = "CONFIRMATION"
WARMUP_COMPLETE_SESSIONS = 5
ACCOUNT_LABEL = "50K"
NORMAL_STRESS_MULTIPLIER = 1.5
CONFIRMATION_FEATURE_VERSION = "hydra_confirmation_causal_bundle_v1"

TIER_G_IDS = (
    "hazard_020ae195ccef8e39b1907e38",
    "hazard_19327ab34a21d623c654a6cc",
    "hazard_1f49e74c20f7bad315dd5cee",
    "hazard_367100adab5fe2a69a4f3257",
)
BREADTH_SPECIFICATION_ID = (
    "breadth:YM:OPEN:BREADTH_CONFIRMED_CONTINUATION"
)

# Authority supplied by the mission owner on 2026-07-19.  This module never
# spends it; the values only bind a later controller-owned acquisition receipt.
PRIOR_CUMULATIVE_ACTUAL_USD = 100.720719923081
ADDITIONAL_AUTHORITY_USD = 100.0
CUMULATIVE_HARD_CAP_USD = 200.720719923081
FROZEN_ESTIMATED_REQUEST_COST_USD = 1.847132667899


class FreshConfirmationError(RuntimeError):
    """The frozen confirmation contract cannot be executed honestly."""


def frozen_data_request() -> dict[str, Any]:
    """Return the exact pre-outcome acquisition request and budget envelope."""

    request = {
        "dataset": DATASET,
        "schema": DATA_SCHEMA,
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "data_role": DATA_ROLE,
        "q4_2024_access_allowed": False,
        "broker_or_order_capability": False,
    }
    return {
        **request,
        "request_hash": stable_hash(request),
        "frozen_estimated_cost_usd": FROZEN_ESTIMATED_REQUEST_COST_USD,
        "prior_cumulative_actual_usd": PRIOR_CUMULATIVE_ACTUAL_USD,
        "additional_authority_usd": ADDITIONAL_AUTHORITY_USD,
        "cumulative_hard_cap_usd": CUMULATIVE_HARD_CAP_USD,
        "projected_cumulative_usd": (
            PRIOR_CUMULATIVE_ACTUAL_USD + FROZEN_ESTIMATED_REQUEST_COST_USD
        ),
    }


def freeze_fresh_confirmation_contract(
    root: str | Path,
    *,
    graduation_path: str | Path = (
        "reports/economic_evolution/"
        "autonomous_economic_discovery_director_0035_revision_02/"
        "branch_results/post_source_exhaustion/post_composite/"
        "tier_g_development_graduation.json"
    ),
    candidate_bank_path: str | Path = (
        "reports/economic_evolution/"
        "autonomous_economic_discovery_director_0035_revision_02/"
        "branch_results/post_source_exhaustion/post_composite/"
        "combine_candidate_bank.json"
    ),
    fast_pass_manifest_path: str | Path = (
        "config/v7/fast_pass_factory_0029_revision_05.json"
    ),
    rule_snapshot_path: str | Path = (
        "config/rulesets/topstep_official_2026-07-19.json"
    ),
) -> dict[str, Any]:
    """Freeze development calibrations without reading confirmation outcomes."""

    project = Path(root).resolve()
    graduation_envelope = _read_json(_inside(project, graduation_path))
    graduation = dict(graduation_envelope["tier_g_development_graduation"])
    if stable_hash({k: v for k, v in graduation.items() if k != "result_hash"}) != str(
        graduation.get("result_hash")
    ):
        raise FreshConfirmationError("Tier-G graduation hash does not recompute")
    books = {
        str(row["candidate_id"]): dict(row)
        for row in graduation["graduated_development_books"]
    }
    if tuple(sorted(books)) != tuple(sorted(TIER_G_IDS)):
        raise FreshConfirmationError("frozen Tier-G inventory drift")

    bank_envelope = _read_json(_inside(project, candidate_bank_path))
    bank = dict(bank_envelope["candidate_bank"])
    classified = {
        str(row["candidate_id"]): dict(row) for row in bank["candidates"]
    }
    manifest = _read_json(_inside(project, fast_pass_manifest_path))
    bindings = dict(dict(manifest["data"])["feature_matrix_bindings"])
    matrices = {
        market: _open_bound_matrix(project, bindings, market)
        for market in ("ES", "NQ", "RTY", "YM")
    }
    evaluation_start_ns = _date_ns(str(dict(manifest["data"])["evaluation_start_inclusive"]))
    rules, rule_receipt = _load_rule_snapshot(_inside(project, rule_snapshot_path))

    entries = _load_candidate_entries(project)
    frozen: list[dict[str, Any]] = []
    for candidate_id in TIER_G_IDS:
        entry = entries.get(candidate_id)
        if entry is None:
            raise FreshConfirmationError(f"candidate bank entry absent: {candidate_id}")
        candidate = HazardCandidate(**dict(entry["candidate"]))
        if candidate.candidate_id != candidate_id:
            raise FreshConfirmationError("candidate fingerprint drift")
        matrix = matrices[candidate.market]
        if candidate.cross_asset_reference_market:
            matrix = with_availability_safe_cross_asset_feature(
                matrix, matrices[candidate.cross_asset_reference_market]
            )
        calibrated = calibrate_candidate(
            candidate,
            matrix,
            calibration_end_exclusive_ns=evaluation_start_ns,
            minimum_observations=100,
        )
        development_book = books[candidate_id]
        source_cell = dict(classified[candidate_id]["best_safe_cell"])
        if str(source_cell["cell_hash"]) != str(development_book["selected_cell_hash"]):
            raise FreshConfirmationError("selected account cell drift")
        policy_payload = dict(dict(development_book["combine_book"])["frozen_account_policy"])
        policy = ActiveRiskPoolPolicy.from_mapping(policy_payload)
        if stable_hash(policy.to_dict()) != stable_hash(policy_payload):
            raise FreshConfirmationError("frozen account policy does not reconstruct")
        frozen.append(
            {
                "candidate_id": candidate_id,
                "candidate": candidate.payload,
                "candidate_fingerprint": candidate.structural_fingerprint,
                "calibration": _calibration_payload(calibrated),
                "account_label": ACCOUNT_LABEL,
                "integer_quantity_tier": int(source_cell["integer_quantity_tier"]),
                "selected_development_horizon_days": int(
                    development_book["selected_horizon_trading_days"]
                ),
                "frozen_account_policy": policy.to_dict(),
                "frozen_account_policy_hash": stable_hash(policy.to_dict()),
                "graduation_evidence_hash": str(
                    development_book["graduation_evidence_hash"]
                ),
                "confirmation_eligible": True,
                "prior_evidence_tier": "G",
            }
        )

    breadth_spec = BreadthSpecification(
        target_market="YM",
        session_code=0,
        family="BREADTH_CONFIRMED_CONTINUATION",
    )
    breadth_view = build_cross_index_breadth_view(
        matrices["YM"],
        {market: matrices[market] for market in ("ES", "NQ", "RTY")},
    )
    breadth_calibrated = calibrate_candidate(
        _breadth_candidate(breadth_spec),
        breadth_view,
        calibration_end_exclusive_ns=evaluation_start_ns,
        minimum_observations=20,
    )
    diagnostic = {
        "specification_id": BREADTH_SPECIFICATION_ID,
        "candidate": breadth_calibrated.candidate.payload,
        "candidate_fingerprint": breadth_calibrated.candidate.structural_fingerprint,
        "calibration": _calibration_payload(breadth_calibrated),
        "required_signal_markets": ["YM", "ES", "NQ", "RTY"],
        "requested_signal_markets": ["YM", "ES"],
        "request_coverage_status": "BLOCKED_MISSING_NQ_RTY",
        "confirmation_eligible": False,
        "prior_evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        "no_semantic_substitution_allowed": True,
    }

    core = {
        "schema": SCHEMA,
        "status": "FROZEN_AWAITING_ACQUISITION",
        "data_request": frozen_data_request(),
        "data_partition": {
            "role": DATA_ROLE,
            "entire_post_warmup_block_consumed_once": True,
            "warmup_rule": "FIRST_5_COMMON_COMPLETE_SESSION_DAYS_EXCLUDED",
            "warmup_complete_sessions": WARMUP_COMPLETE_SESSIONS,
            "evaluation_start_rule": "SIXTH_COMMON_COMPLETE_SESSION_DAY",
            "candidate_modification_allowed": False,
            "recalibration_allowed": False,
        },
        "account_replay_contract": {
            "account_label": ACCOUNT_LABEL,
            "horizons_trading_days": list(HORIZONS),
            "start_grid": "MAXIMUM_NON_OVERLAPPING_COMPLETE_WINDOWS_FROM_POST_WARMUP_CALENDAR",
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "normal_stress_multiplier": NORMAL_STRESS_MULTIPLIER,
            "exact_mll": True,
            "exact_consistency": True,
            "retuning_allowed": False,
        },
        "tier_c_gate": {
            "applies_only_to_prior_tier_g": True,
            "matching_horizon_days": 20,
            "minimum_normal_passes": 1,
            "minimum_stressed_passes": 1,
            "positive_stressed_net_required": True,
            "maximum_stressed_mll_breach_rate": 0.10,
            "all_stressed_passing_paths_consistency_compliant": True,
            "complete_confirmation_window_required": True,
            "no_retuning": True,
        },
        "tier_g_candidates": frozen,
        "breadth_diagnostic": diagnostic,
        "official_rule_snapshot": rule_receipt,
        "official_rule_snapshot_payload": {ACCOUNT_LABEL: dict(rules[ACCOUNT_LABEL])},
        "source_graduation_hash": str(graduation["result_hash"]),
        "source_manifest_hash": str(manifest["manifest_hash"]),
        "source_calibration_matrix_hashes": {
            market: matrices[market].fingerprint for market in sorted(matrices)
        },
        "authoritative_writes": 0,
        "data_purchases": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "contract_hash": stable_hash(core)}


def validate_acquisition_receipt(
    contract: Mapping[str, Any], receipt: Mapping[str, Any]
) -> dict[str, Any]:
    """Validate a controller-owned acquisition receipt before opening data."""

    frozen = _verify_contract(contract)
    request = dict(frozen["data_request"])
    actual_request = dict(receipt.get("request") or {})
    compared = ("dataset", "schema", "symbols", "stype_in", "start", "end")
    if any(actual_request.get(key) != request.get(key) for key in compared):
        raise FreshConfirmationError("acquisition request differs from freeze")
    actual = float(receipt.get("actual_cost_usd", -1.0))
    cumulative = float(receipt.get("cumulative_actual_usd", -1.0))
    if actual < 0.0 or cumulative < 0.0:
        raise FreshConfirmationError("acquisition cost is absent")
    if cumulative > float(request["cumulative_hard_cap_usd"]) + 1e-12:
        raise FreshConfirmationError("acquisition exceeds cumulative authority")
    if actual > float(request["additional_authority_usd"]) + 1e-12:
        raise FreshConfirmationError("acquisition exceeds incremental authority")
    files = [dict(row) for row in receipt.get("files", ())]
    if not files or any(not str(row.get("sha256") or "") for row in files):
        raise FreshConfirmationError("acquisition file hashes are incomplete")
    return {
        "status": "ACQUISITION_RECEIPT_RECONCILED",
        "request_hash": str(request["request_hash"]),
        "actual_cost_usd": actual,
        "cumulative_actual_usd": cumulative,
        "file_count": len(files),
        "file_inventory_hash": stable_hash(files),
    }


def build_confirmation_feature_bundles(
    contract: Mapping[str, Any],
    *,
    source_files: Sequence[Mapping[str, Any]],
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Materialise immutable causal YM/ES bundles from acquired parquet.

    The function performs no download and no authoritative write.  Every
    source file and the explicit Databento contract map must already exist and
    be hash-bound by the controller-owned acquisition receipt.  Outcome-only
    ``forward_move`` and legacy ``entry_price`` arrays are removed from the
    decision bundle; causal replay derives the next-tradable-open fill from
    ``bar_open`` after an emitted decision.
    """

    frozen = _verify_contract(contract)
    files = [dict(row) for row in source_files]
    if not files:
        raise FreshConfirmationError("confirmation source parquet inventory is empty")
    frames: list[pd.DataFrame] = []
    source_receipts: list[dict[str, Any]] = []
    roll_map_file = Path(contract_map_path).resolve()
    if not roll_map_file.is_file():
        raise FreshConfirmationError("explicit confirmation contract map is absent")
    roll_map = load_roll_map(roll_map_file)
    if (
        roll_map.dataset != DATASET
        or roll_map.schema != DATA_SCHEMA
        or not str(roll_map.map_type).startswith("EXPLICIT_DATABENTO_")
    ):
        raise FreshConfirmationError("confirmation contract map is not explicit Databento OHLCV")
    required_roots = {"YM", "MYM", "ES"}
    if not required_roots.issubset(set(roll_map.symbols)):
        raise FreshConfirmationError("confirmation contract map lacks YM/MYM/ES")
    symbol_map = {
        **{f"{root}.c.0": root for root in required_roots},
        **{str(row.contract): str(row.root) for row in roll_map.contracts},
    }
    for raw in files:
        path = Path(str(raw.get("path") or "")).resolve()
        expected = str(raw.get("sha256") or "")
        if not path.is_file() or not expected or _sha256(path) != expected:
            raise FreshConfirmationError("confirmation parquet hash mismatch")
        frame = pd.read_parquet(path)
        normalized = normalize_ohlcv_frame(
            frame,
            symbol=None,
            timeframe="1m",
            symbol_map=symbol_map,
        )
        validate_ohlcv_frame(normalized, timeframe="1m")
        timestamps = pd.to_datetime(normalized["timestamp"], utc=True)
        if timestamps.min() < pd.Timestamp(START, tz="UTC") or timestamps.max() >= pd.Timestamp(END, tz="UTC"):
            raise FreshConfirmationError("confirmation parquet escapes frozen half-open dates")
        frames.append(normalized)
        source_receipts.append(
            {"path": str(path), "sha256": expected, "rows": len(normalized)}
        )
    raw = pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"])
    if set(raw["symbol"].astype(str)) != required_roots:
        raise FreshConfirmationError("confirmation parquet does not contain exactly YM/MYM/ES")
    if raw.duplicated(["symbol", "timestamp"]).any():
        raise FreshConfirmationError("confirmation parquet has duplicate symbol timestamps")
    mapped, map_receipt = _apply_explicit_contract_map(
        raw,
        roll_map,
        required_map_type=roll_map.map_type,
    )
    if set(mapped["symbol"].astype(str)) != required_roots:
        raise FreshConfirmationError("contract/roll guards removed an entire requested root")
    featured = _prepare_feature_frame(_build_past_only_feature_frame(mapped))
    source_hash = stable_hash(source_receipts)
    map_hash = _sha256(roll_map_file)
    store = CanonicalFeatureStore(cache_root)
    bundles: dict[str, Any] = {}
    for market in ("YM", "ES"):
        market_frame = featured.loc[featured["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = _market_arrays(market_frame, market)
        # Neither field is a decision input, but excluding it makes the
        # physical decision/outcome separation explicit for confirmation.
        arrays.pop("entry_price", None)
        for key in tuple(arrays):
            if key.startswith("forward_move__"):
                arrays.pop(key)
        key = CanonicalFeatureKey(
            market=market,
            explicit_contract_scope=f"{market}_date_aware_active_contracts_confirmation",
            start_inclusive=START,
            end_exclusive=END,
            source_data_sha256=source_hash,
            roll_map_hash=map_hash,
            transformation_version=CONFIRMATION_FEATURE_VERSION,
            feature_dag_hash=FEATURE_DAG_HASH,
            timeframes=("1m", "5m", "15m", "30m", "60m", "session", "daily"),
        )
        result = store.put(
            key,
            arrays,
            provenance={
                "data_role": DATA_ROLE,
                "request_hash": str(dict(frozen["data_request"])["request_hash"]),
                "source_file_inventory_hash": source_hash,
                "contract_map_sha256": map_hash,
                "contract_map_hash": roll_map.roll_map_hash(),
                "market": market,
                "execution_market": "MYM" if market == "YM" else "MES",
                "feature_names": list(metadata["feature_names"]),
                "future_outcome_arrays_in_decision_bundle": False,
                "entry_fill_contract": "SIGNAL_AFTER_COMPLETED_T_THEN_NEXT_TRADABLE_BAR_OPEN",
                "q4_access_count_delta": 0,
            },
        )
        bundles[market] = {
            "path": str(result.path),
            "bundle_hash": result.bundle_hash,
            "row_count": result.row_count,
            "cache_hit": result.cache_hit,
        }
    core = {
        "status": "CONFIRMATION_CAUSAL_FEATURE_BUNDLES_READY",
        "contract_hash": str(frozen["contract_hash"]),
        "source_files": source_receipts,
        "source_file_inventory_hash": source_hash,
        "contract_map": {
            "path": str(roll_map_file),
            "sha256": map_hash,
            "roll_map_hash": roll_map.roll_map_hash(),
            "map_type": roll_map.map_type,
            "guard_receipt": map_receipt,
        },
        "bundles": bundles,
        "data_role": DATA_ROLE,
        "q4_access_count_delta": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def open_confirmation_matrices(
    feature_receipt: Mapping[str, Any],
) -> dict[str, FeatureMatrix]:
    """Open only the two immutable bundles bound by a builder receipt."""

    receipt = dict(feature_receipt)
    expected = str(receipt.pop("result_hash", ""))
    if not expected or stable_hash(receipt) != expected:
        raise FreshConfirmationError("feature receipt hash does not recompute")
    bundles = dict(receipt["bundles"])
    if set(bundles) != {"YM", "ES"}:
        raise FreshConfirmationError("feature receipt inventory drift")
    output: dict[str, FeatureMatrix] = {}
    for market, raw in bundles.items():
        binding = dict(raw)
        matrix = FeatureMatrix.open(binding["path"], mmap=True)
        if matrix.fingerprint != str(binding["bundle_hash"]):
            raise FreshConfirmationError("confirmation feature bundle drift")
        output[market] = matrix
    return output


def evaluate_fresh_confirmation(
    contract: Mapping[str, Any],
    *,
    matrices: Mapping[str, FeatureMatrix],
    acquisition_receipt: Mapping[str, Any],
    existing_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Consume the post-warmup confirmation block exactly once.

    ``matrices`` must be causal bundles created from the frozen acquired files
    and explicit contract map.  No calibration is performed here.
    """

    frozen = _verify_contract(contract)
    if existing_result is not None:
        raise FreshConfirmationError("confirmation block is already consumed")
    acquisition = validate_acquisition_receipt(frozen, acquisition_receipt)
    if set(matrices) != {"YM", "ES"}:
        raise FreshConfirmationError("confirmation requires exactly YM and ES matrices")
    for market, matrix in matrices.items():
        key = dict(matrix.manifest.get("key") or {})
        if str(key.get("market")) != market:
            raise FreshConfirmationError("confirmation matrix market identity drift")
        if str(key.get("start_inclusive")) != START or str(key.get("end_exclusive")) != END:
            raise FreshConfirmationError("confirmation matrix date role drift")
        if str(dict(matrix.manifest.get("provenance") or {}).get("data_role")) != DATA_ROLE:
            raise FreshConfirmationError("confirmation matrix role is not frozen")

    calendar = _post_warmup_common_calendar(matrices)
    starts = non_overlapping_starts(calendar, HORIZONS)
    raw_rules = dict(frozen["official_rule_snapshot_payload"])
    rule = dict(raw_rules[ACCOUNT_LABEL])
    config = _account_config(rule)

    results: list[dict[str, Any]] = []
    for row in frozen["tier_g_candidates"]:
        candidate_row = dict(row)
        candidate = HazardCandidate(**dict(candidate_row["candidate"]))
        matrix = matrices["YM"]
        if candidate.cross_asset_reference_market:
            matrix = with_availability_safe_cross_asset_feature(matrix, matrices["ES"])
        calibrated = _calibrated_from_payload(candidate, candidate_row["calibration"])
        policy = ActiveRiskPoolPolicy.from_mapping(candidate_row["frozen_account_policy"])
        if stable_hash(policy.to_dict()) != str(candidate_row["frozen_account_policy_hash"]):
            raise FreshConfirmationError("account policy changed after freeze")
        result = _evaluate_one(
            calibrated=calibrated,
            matrix=matrix,
            calendar=calendar,
            starts=starts,
            policy=policy,
            rule=rule,
            integer_quantity_tier=int(candidate_row["integer_quantity_tier"]),
            selected_horizon=int(candidate_row["selected_development_horizon_days"]),
            confirmation_eligible=True,
        )
        results.append(result)

    breadth = {
        "specification_id": BREADTH_SPECIFICATION_ID,
        "status": "FAIL_CLOSED_CONFIRMATION_NOT_EVALUATED",
        "reason": "FROZEN_THREE_PEER_BREADTH_REQUIRES_NQ_AND_RTY_NOT_IN_EXACT_REQUEST",
        "tier_c_promoted": False,
    }
    core = {
        "schema": RESULT_SCHEMA,
        "status": "CONFIRMATION_CONSUMED_ONCE",
        "contract_hash": str(frozen["contract_hash"]),
        "acquisition": acquisition,
        "data_role": DATA_ROLE,
        "calendar": {
            "warmup_complete_sessions": WARMUP_COMPLETE_SESSIONS,
            "post_warmup_session_count": len(calendar),
            "evaluation_first_session_day": int(calendar[0]),
            "evaluation_last_session_day": int(calendar[-1]),
            "non_overlapping_start_counts": {
                str(horizon): len(starts[horizon]) for horizon in HORIZONS
            },
        },
        "candidate_results": results,
        "breadth_diagnostic": breadth,
        "tier_c_candidate_ids": sorted(
            str(row["candidate_id"]) for row in results if row["tier_c_promoted"]
        ),
        "retuning_performed": False,
        "recalibration_performed": False,
        "independent_confirmation_claimed_only_for_gate_passers": True,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def non_overlapping_starts(
    calendar: Sequence[int], horizons: Sequence[int] = HORIZONS
) -> dict[int, tuple[tuple[int, str], ...]]:
    """Build outcome-independent, complete and non-overlapping start grids."""

    days = tuple(int(value) for value in calendar)
    if not days or len(set(days)) != len(days) or tuple(sorted(days)) != days:
        raise FreshConfirmationError("confirmation calendar is not unique chronological")
    output: dict[int, tuple[tuple[int, str], ...]] = {}
    for horizon in horizons:
        value = int(horizon)
        if value < 1:
            raise FreshConfirmationError("confirmation horizon must be positive")
        output[value] = tuple(
            (days[position], "CONFIRMATION")
            for position in range(0, len(days) - value + 1, value)
        )
    return output


def tier_c_gate(
    cells: Sequence[Mapping[str, Any]], *, confirmation_eligible: bool
) -> dict[str, Any]:
    """Apply the frozen matching-20-day gate without adaptive fallbacks."""

    cell = next(
        (dict(row) for row in cells if int(row["horizon_trading_days"]) == 20),
        None,
    )
    if cell is None:
        return {"passed": False, "reason": "NO_COMPLETE_20_DAY_CONFIRMATION_CELL"}
    normal = dict(cell["normal"])
    stressed = dict(cell["stressed"])
    checks = {
        "prior_tier_g": bool(confirmation_eligible),
        "normal_pass": int(normal.get("pass_count", 0)) >= 1,
        "stressed_pass": int(stressed.get("pass_count", 0)) >= 1,
        "positive_stressed_net": float(stressed.get("net_total_usd", 0.0)) > 0.0,
        "controlled_stressed_mll": float(stressed.get("mll_breach_rate", 1.0)) <= 0.10,
        "stressed_passing_consistency": bool(
            stressed.get("all_passing_paths_consistency_compliant", False)
        ),
        "full_coverage": int(cell.get("full_coverage_start_count", 0)) > 0,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "reason": "TIER_C_GATE_PASSED" if all(checks.values()) else "TIER_C_GATE_FAILED",
    }


def _evaluate_one(
    *,
    calibrated: CalibratedHazardCandidate,
    matrix: FeatureMatrix,
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    policy: ActiveRiskPoolPolicy,
    rule: Mapping[str, Any],
    integer_quantity_tier: int,
    selected_horizon: int,
    confirmation_eligible: bool,
) -> dict[str, Any]:
    candidate = calibrated.candidate
    first_day_ns = int(calendar[0]) * 86_400_000_000_000
    end_day_ns = (int(calendar[-1]) + 1) * 86_400_000_000_000
    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=first_day_ns,
        evaluation_end_exclusive_ns=end_day_ns,
    )
    streaming = discover_intents_streaming(
        calibrated,
        matrix,
        evaluation_start_ns=first_day_ns,
        evaluation_end_exclusive_ns=end_day_ns,
    )
    if tuple((row.row_index, row.direction) for row in intents) != streaming:
        raise FreshConfirmationError("batch/stream decision mismatch")
    events = observe_outcomes(calibrated, matrix, intents)
    eligible_days = frozen_eligible_session_calendar(
        candidate,
        matrix,
        evaluation_start_ns=first_day_ns,
        evaluation_end_exclusive_ns=end_day_ns,
    )
    replay = exact_sleeve_replay(calibrated, events, eligible_session_days=eligible_days)
    coverage = _candidate_coverage(replay, calendar, starts)
    normal, normal_violations = _apply_session_contract(replay.normal_trajectories)
    stressed, stressed_violations = _apply_session_contract(replay.stressed_trajectories)
    if normal_violations or stressed_violations:
        raise FreshConfirmationError("session contract violation")
    scaled_normal = tuple(
        scale_causal_trajectory(row, executable_quantity_multiplier=integer_quantity_tier)
        for row in normal
    )
    scaled_stressed = tuple(
        scale_causal_trajectory(row, executable_quantity_multiplier=integer_quantity_tier)
        for row in stressed
    )
    _require_scenario_identity(scaled_normal, scaled_stressed)
    maximum_mini = max(
        (float(row.event.mini_equivalent) for row in scaled_normal), default=0.0
    )
    account_limit = _market_contract_limit_mini(candidate.payload, rule)
    if maximum_mini > account_limit + 1e-12:
        raise FreshConfirmationError("frozen quantity exceeds 50K contract limit")
    cells: list[dict[str, Any]] = []
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
                    maximum_duration_days=int(horizon),
                    config=_account_config(rule),
                )
                episode_sets[scenario].append((episode, block))
        cell = _exact_cell(
            candidate.candidate_id,
            ACCOUNT_LABEL,
            rule,
            tier=integer_quantity_tier,
            horizon=int(horizon),
            maximum_mini=maximum_mini,
            account_contract_limit=account_limit,
            declared_risk_charge=min(policy.nominal_risk_charge_map.values()),
            governor_mode="FROZEN_TIER_G_POLICY",
            policy=policy,
            requested_starts=len(starts[horizon]),
            censored_starts=coverage["censored"][horizon],
            episodes=episode_sets,
        )
        for key, scenario in (("normal", "NORMAL"), ("stressed", "STRESSED_1_5X")):
            summary = dict(cell[key])
            passing = [episode for episode, _ in episode_sets[scenario] if episode.passed]
            summary["all_passing_paths_consistency_compliant"] = bool(passing) and all(
                bool(episode.consistency_ok) for episode in passing
            )
            cell[key] = summary
        cells.append(cell)
    gate = tier_c_gate(cells, confirmation_eligible=confirmation_eligible)
    return {
        "candidate_id": candidate.candidate_id,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "calibration_hash": calibrated.fingerprint,
        "calibration_reused_without_recalibration": True,
        "batch_stream_equal": True,
        "emitted_intent_count": len(intents),
        "completed_event_count": len(replay.normal_trajectories),
        "selected_development_horizon_days": selected_horizon,
        "cells": cells,
        "tier_c_gate": gate,
        "tier_c_promoted": bool(gate["passed"]),
        "evidence_tier": "C" if gate["passed"] else "G_CONFIRMATION_FAILED",
    }


def _post_warmup_common_calendar(
    matrices: Mapping[str, FeatureMatrix]
) -> tuple[int, ...]:
    common: set[int] | None = None
    for matrix in matrices.values():
        timestamp = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
        days = np.asarray(matrix.array("session_day"), dtype=np.int64)
        mask = (timestamp >= _date_ns(START)) & (timestamp < _date_ns(END))
        current = {int(value) for value in days[mask]}
        common = current if common is None else common & current
    ordered = tuple(sorted(common or ()))
    if len(ordered) <= WARMUP_COMPLETE_SESSIONS:
        raise FreshConfirmationError("confirmation lacks deterministic warmup coverage")
    return ordered[WARMUP_COMPLETE_SESSIONS:]


def _calibrated_from_payload(
    candidate: HazardCandidate, raw: Mapping[str, Any]
) -> CalibratedHazardCandidate:
    value = dict(raw)
    calibrated = CalibratedHazardCandidate(
        candidate=candidate,
        calibration_end_exclusive_ns=int(value["calibration_end_exclusive_ns"]),
        trigger_threshold=float(value["trigger_threshold"]),
        context_threshold=(
            None if value.get("context_threshold") is None else float(value["context_threshold"])
        ),
        finite_trigger_observations=int(value["finite_trigger_observations"]),
        finite_context_observations=(
            None
            if value.get("finite_context_observations") is None
            else int(value["finite_context_observations"])
        ),
        source_matrix_hash=str(value["source_matrix_hash"]),
    )
    if calibrated.fingerprint != str(value["calibration_hash"]):
        raise FreshConfirmationError("frozen calibration hash drift")
    return calibrated


def _calibration_payload(value: CalibratedHazardCandidate) -> dict[str, Any]:
    return {
        "calibration_end_exclusive_ns": value.calibration_end_exclusive_ns,
        "trigger_threshold": value.trigger_threshold,
        "context_threshold": value.context_threshold,
        "finite_trigger_observations": value.finite_trigger_observations,
        "finite_context_observations": value.finite_context_observations,
        "source_matrix_hash": value.source_matrix_hash,
        "calibration_hash": value.fingerprint,
    }


def _load_candidate_entries(project: Path) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    base = project / "data/cache/economic_production/hydra_fast_pass_factory_0029"
    for path in sorted(base.glob("wave_*/causal_executable_bank.json")):
        payload = _read_json(path)
        for raw in payload.get("entries", ()):
            row = dict(raw)
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id in TIER_G_IDS:
                prior = output.get(candidate_id)
                if prior is not None and stable_hash(prior["candidate"]) != stable_hash(row["candidate"]):
                    raise FreshConfirmationError("candidate bank semantic drift")
                output[candidate_id] = row
    return output


def _open_bound_matrix(
    project: Path, bindings: Mapping[str, Any], market: str
) -> FeatureMatrix:
    binding = dict(bindings[market])
    manifest_path = _inside(project, str(binding["path"]))
    matrix = FeatureMatrix.open(manifest_path.parent, mmap=True)
    if matrix.fingerprint != str(binding["bundle_hash"]):
        raise FreshConfirmationError("source calibration matrix drift")
    return matrix


def _verify_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    contract = dict(value)
    expected = str(contract.pop("contract_hash", ""))
    if not expected or stable_hash(contract) != expected:
        raise FreshConfirmationError("confirmation contract hash does not recompute")
    return {**contract, "contract_hash": expected}


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FreshConfirmationError("path escapes repository") from exc
    if not resolved.is_file():
        raise FreshConfirmationError(f"required file is absent: {resolved}")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    import json

    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise FreshConfirmationError(f"JSON object expected: {path}")
    return value


def _date_ns(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp() * 1e9)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "FreshConfirmationError",
    "build_confirmation_feature_bundles",
    "evaluate_fresh_confirmation",
    "freeze_fresh_confirmation_contract",
    "frozen_data_request",
    "non_overlapping_starts",
    "open_confirmation_matrices",
    "tier_c_gate",
    "validate_acquisition_receipt",
]
