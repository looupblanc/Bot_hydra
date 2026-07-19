"""Fail-closed two-stage economic runner for the frozen 2026 Tier-Q cohort.

The acquisition process deliberately writes two immutable temporal partitions.
This module keeps the scientific boundary equally explicit:

* FINAL_DEVELOPMENT may be opened first and can only advance Q -> G;
* CONFIRMATION cannot be materialised or evaluated until a self-hashed
  final-development result names at least one Tier-G candidate;
* confirmation is one-shot and can only advance an already-G candidate to C.

The functions are read-only with respect to the mission database, registry and
promotion state.  They return deterministic payloads for the existing single
authoritative writer.  Feature-store writes are immutable content-addressed
caches, not authoritative mission writes.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, replace
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_loader import normalize_ohlcv_frame, validate_ohlcv_frame
from hydra.economic_evolution.schema import stable_hash
from hydra.features.canonical_store import CanonicalFeatureKey, CanonicalFeatureStore
from hydra.features.feature_matrix import FeatureMatrix
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _build_past_only_feature_frame,
)
from hydra.production import autonomous_exact_replay as exact
from hydra.production import autonomous_marginal_combine_books as books
from hydra.production.autonomous_exact_continuation import _unwrap_exact_result
from hydra.production.autonomous_tier_g_controls import _unique_ledger_concentration
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.research import causal_target_velocity as hazard
from hydra.research.causal_target_velocity import (
    CalibratedHazardCandidate,
    HazardCandidate,
    HazardIntent,
    HazardOutcome,
    calibrate_candidate,
    discover_intents_batch,
    discover_intents_streaming,
    exact_sleeve_replay,
    frozen_eligible_session_calendar,
    with_availability_safe_cross_asset_feature,
)
from hydra.research.qd_economic_tournament import _prepare_feature_frame
from hydra.research.turbo_feature_builder import FEATURE_DAG_HASH, _market_arrays
from hydra.validation.lockbox_guard import current_commit


CONTRACT_SCHEMA = "hydra_tier_q_2026_two_stage_contract_v1"
ACQUISITION_SCHEMA = "hydra_tier_q_2026_acquisition_receipt_v1"
FEATURE_SCHEMA = "hydra_tier_q_2026_role_features_v2"
STAGE_SCHEMA = "hydra_tier_q_2026_two_stage_economic_result_v2"
FINAL_DEVELOPMENT = "FINAL_DEVELOPMENT"
CONFIRMATION = "CONFIRMATION"
FEATURE_VERSION = "hydra_tier_q_2026_causal_features_v2"
DAY_NS = 86_400_000_000_000
EXPECTED_FINAL_DEVELOPMENT_ROWS = 1_136_722
EXPECTED_FINAL_DEVELOPMENT_TRADING_DAYS = 85
EXPECTED_THEORETICAL_STARTS = {5: 17, 10: 8, 20: 4}
PARTIAL_SPLIT_SESSION_DAY = "2026-05-01"
GOOD_FRIDAY_2026 = "2026-04-03"
BARRIER_QUANTIZATION_RULE_ID = "MICRO_OUTWARD_TICK_QUANTIZATION_V1"
BARRIER_QUANTIZATION_RULE = {
    "rule_id": BARRIER_QUANTIZATION_RULE_ID,
    "long_target": "CEIL_TO_EXECUTION_TICK",
    "long_stop": "FLOOR_TO_EXECUTION_TICK",
    "short_target": "FLOOR_TO_EXECUTION_TICK",
    "short_stop": "CEIL_TO_EXECUTION_TICK",
    "already_tick_aligned": "UNCHANGED",
    "economic_intent": "TARGET_AND_STOP_NEVER_ROUNDED_TOWARD_ENTRY",
}
BARRIER_QUANTIZATION_RULE_HASH = stable_hash(BARRIER_QUANTIZATION_RULE)

DEFAULT_CANDIDATE_BANK = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/branch_results/"
    "post_source_exhaustion/post_composite/combine_candidate_bank.json"
)
DEFAULT_INITIAL_EXACT = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/branch_results/"
    "epoch_0002_exact_0029_account_race.json"
)
DEFAULT_CONTINUATIONS = tuple(
    Path(
        "reports/economic_evolution/"
        "autonomous_economic_discovery_director_0035_revision_02/branch_results/"
        f"post_source_exhaustion/exact_0029_offset_{offset:04d}.json"
    )
    for offset in (32, 64, 96, 128, 160)
)
DEFAULT_FAST_PASS_MANIFEST = Path("config/v7/fast_pass_factory_0029_revision_05.json")
DEFAULT_RULE_SNAPSHOT = Path("config/rulesets/topstep_official_2026-07-19.json")


class TierQTwoStageError(RuntimeError):
    """A frozen scientific or persistence boundary failed closed."""


@dataclass(frozen=True, slots=True)
class FrozenCandidateBinding:
    candidate_id: str
    candidate: HazardCandidate
    calibrated: CalibratedHazardCandidate
    account_label: str
    horizon_trading_days: int
    integer_quantity_tier: int
    policy: ActiveRiskPoolPolicy
    selected_cell_hash: str
    source_exact_result_hash: str
    source_candidate_result_hash: str
    account_policy_hash: str
    development_evidence_hash: str


def verify_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(contract)
    claimed = str(value.pop("contract_hash", ""))
    if (
        not claimed
        or stable_hash(value) != claimed
        or contract.get("schema") != CONTRACT_SCHEMA
        or contract.get("status") != "FROZEN_AWAITING_ACQUISITION"
        or contract.get("promotion_order") != ["Q", "G", "C"]
        or contract.get("outcome_accessed_at_freeze") is not False
    ):
        raise TierQTwoStageError("frozen two-stage contract identity drift")
    roles = list(contract.get("temporal_roles") or ())
    if [str(row.get("role")) for row in roles] != [FINAL_DEVELOPMENT, CONFIRMATION]:
        raise TierQTwoStageError("two-stage temporal-role order drift")
    if any(row.get("retuning_allowed") is not False for row in roles):
        raise TierQTwoStageError("retuning was enabled in a frozen temporal role")
    return dict(contract)


def verify_acquisition_receipt(
    contract: Mapping[str, Any],
    receipt: Mapping[str, Any],
    *,
    open_role: str | None = None,
) -> dict[str, Any]:
    frozen = verify_contract(contract)
    value = dict(receipt)
    claimed = str(value.pop("receipt_hash", ""))
    if (
        not claimed
        or stable_hash(value) != claimed
        or receipt.get("schema") != ACQUISITION_SCHEMA
        or receipt.get("contract_hash") != frozen["contract_hash"]
        or receipt.get("authorization_manifest_hash")
        != frozen["source_manifest_hash"]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("outcome_evaluation_performed") is not False
        or dict(receipt.get("partition_state") or {}).get(CONFIRMATION)
        != "SEALED_UNTIL_TIER_G_GATE"
    ):
        raise TierQTwoStageError("2026 acquisition receipt identity/seal drift")
    if open_role not in {None, FINAL_DEVELOPMENT, CONFIRMATION}:
        raise TierQTwoStageError("unknown acquisition-open role")
    inventory: dict[str, dict[str, Any]] = {}
    for raw in receipt.get("files") or ():
        row = dict(raw)
        artifact_kind = str(row.get("kind") or row.get("role") or "")
        declared_path = str(row.get("path") or "")
        declared_sha = str(row.get("sha256") or "")
        if (
            not artifact_kind
            or artifact_kind in inventory
            or not declared_path
            or len(declared_sha) != 64
        ):
            raise TierQTwoStageError("acquisition file inventory is absent or duplicated")
        inventory[artifact_kind] = row
    required = {
        "FINAL_DEVELOPMENT_PARQUET",
        "SEALED_CONFIRMATION_PARQUET",
        "EXPLICIT_CONTRACT_MAP",
    }
    if not required.issubset(inventory):
        raise TierQTwoStageError("acquisition receipt lacks a required sealed artifact")
    # Receipt identity validates the declared confirmation hash without reading
    # a byte from its sealed path.  Physical hashing is role-gated so FINAL
    # DEVELOPMENT cannot accidentally inspect the confirmation artifact.
    opened_artifacts = {
        FINAL_DEVELOPMENT: {
            "FINAL_DEVELOPMENT_PARQUET",
            "EXPLICIT_CONTRACT_MAP",
        },
        CONFIRMATION: {
            "FINAL_DEVELOPMENT_PARQUET",
            "SEALED_CONFIRMATION_PARQUET",
            "EXPLICIT_CONTRACT_MAP",
        },
        None: set(),
    }[open_role]
    for artifact_kind in opened_artifacts:
        row = inventory[artifact_kind]
        path = Path(str(row["path"])).resolve()
        if not path.is_file() or _sha256(path) != str(row["sha256"]):
            raise TierQTwoStageError(f"opened acquisition artifact hash drift: {artifact_kind}")
    return {**dict(receipt), "_inventory": inventory}


def load_frozen_bindings(
    root: str | Path,
    contract: Mapping[str, Any],
    *,
    candidate_bank_path: str | Path = DEFAULT_CANDIDATE_BANK,
    initial_exact_path: str | Path = DEFAULT_INITIAL_EXACT,
    continuation_paths: Sequence[str | Path] = DEFAULT_CONTINUATIONS,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> tuple[dict[str, FrozenCandidateBinding], dict[str, dict[str, Any]], dict[str, Any]]:
    """Reconstruct every frozen calibration and account policy from pre-2026 evidence."""

    project = Path(root).resolve()
    frozen = verify_contract(contract)
    envelope = _read_json(_inside(project, candidate_bank_path))
    outer = dict(envelope)
    outer_claimed = str(outer.pop("result_hash", ""))
    if (
        stable_hash(outer) != outer_claimed
        or outer_claimed != frozen.get("source_candidate_bank_wrapper_hash")
    ):
        raise TierQTwoStageError("candidate-bank wrapper hash drift")
    bank = books._verify_candidate_bank(dict(envelope["candidate_bank"]))
    if str(bank["result_hash"]) != str(frozen.get("source_candidate_bank_hash")):
        raise TierQTwoStageError("candidate-bank hash differs from frozen cohort source")

    initial = _read_json(_inside(project, initial_exact_path))
    continuations = [_read_json(_inside(project, path)) for path in continuation_paths]
    _composite, exact_results = books._verified_exact_results(initial, continuations)
    exact_by_hash = {str(row["result_hash"]): dict(row) for row in exact_results}
    if len(exact_by_hash) != len(exact_results):
        raise TierQTwoStageError("duplicate exact-result hash")

    fast_manifest = exact._load_self_hashed_manifest(
        _inside(project, fast_pass_manifest_path)
    )
    matrix_bindings = dict(dict(fast_manifest["data"])["feature_matrix_bindings"])
    matrices = {
        market: _open_bound_matrix(project, raw)
        for market, raw in matrix_bindings.items()
    }
    calibration_end_ns = _date_ns(
        str(dict(fast_manifest["data"])["evaluation_start_inclusive"])
    )
    rules, rule_receipt = exact._load_rule_snapshot(
        _inside(project, rule_snapshot_path)
    )
    if str(rule_receipt["parsed_rule_hash"]) != str(
        frozen["official_rule_snapshot_hash"]
    ):
        raise TierQTwoStageError("official rule snapshot differs from frozen contract")

    classified = {
        str(row["candidate_id"]): dict(row) for row in bank["candidates"]
    }
    output: dict[str, FrozenCandidateBinding] = {}
    for frozen_row_raw in frozen["candidate_cohort"]:
        frozen_row = dict(frozen_row_raw)
        candidate_id = str(frozen_row["candidate_id"])
        source = classified.get(candidate_id)
        if source is None or source.get("computed_development_tier") != "Q":
            raise TierQTwoStageError(f"frozen Tier-Q source missing: {candidate_id}")
        if (
            str(source["candidate_fingerprint"])
            != str(frozen_row["candidate_fingerprint"])
            or str(source["realized_behavioral_fingerprint"])
            != str(frozen_row["behavioral_fingerprint"])
            or str(dict(source["compact_evidence_bundle"])["bundle_hash"])
            != str(frozen_row["source_bundle_hash"])
            or str(dict(source["compact_evidence_bundle"])["candidate_result_hash"])
            != str(frozen_row["development_evidence_hash"])
        ):
            raise TierQTwoStageError(f"candidate evidence binding drift: {candidate_id}")

        candidate = HazardCandidate(**dict(frozen_row["frozen_candidate_specification"]))
        if candidate.candidate_id != candidate_id:
            raise TierQTwoStageError(f"candidate specification hash drift: {candidate_id}")
        matrix = matrices[candidate.market]
        if candidate.cross_asset_reference_market:
            matrix = with_availability_safe_cross_asset_feature(
                matrix, matrices[str(candidate.cross_asset_reference_market)]
            )
        calibrated = calibrate_candidate(
            candidate,
            matrix,
            calibration_end_exclusive_ns=calibration_end_ns,
        )
        if calibrated.fingerprint != str(frozen_row["calibration_hash"]):
            raise TierQTwoStageError(f"pre-2026 calibration drift: {candidate_id}")

        source_hash = str(source["source_exact_result_hash"])
        exact_result = exact_by_hash.get(source_hash)
        if exact_result is None:
            raise TierQTwoStageError(f"exact source result missing: {candidate_id}")
        candidates = [
            dict(row)
            for row in exact_result.get("results") or ()
            if str(row.get("candidate_id")) == candidate_id
        ]
        if len(candidates) != 1:
            raise TierQTwoStageError(f"exact candidate missing or duplicated: {candidate_id}")
        exact_candidate = candidates[0]
        selected_hash = str(frozen_row["selected_cell_hash"])
        cells = [
            dict(cell)
            for cell in exact_candidate.get("frontier") or ()
            if stable_hash(cell) == selected_hash
        ]
        if len(cells) != 1:
            raise TierQTwoStageError(f"selected account cell drift: {candidate_id}")
        cell = cells[0]
        profile = dict(frozen_row["frozen_account_profile"])
        if any(
            (
                str(cell["account_label"]) != str(profile["account_label"]),
                int(cell["horizon_trading_days"])
                != int(profile["horizon_trading_days"]),
                int(cell["integer_quantity_tier"])
                != int(profile["integer_quantity_tier"]),
                str(cell["risk_governor_mode"])
                != str(profile["risk_governor_mode"]),
            )
        ):
            raise TierQTwoStageError(f"selected account profile drift: {candidate_id}")
        policy = ActiveRiskPoolPolicy.from_mapping(dict(cell["account_policy"]))
        if stable_hash(policy.to_dict()) != stable_hash(dict(cell["account_policy"])):
            raise TierQTwoStageError(f"selected account policy drift: {candidate_id}")
        if str(cell["account_label"]) not in rules:
            raise TierQTwoStageError(f"unsupported frozen account size: {candidate_id}")
        output[candidate_id] = FrozenCandidateBinding(
            candidate_id=candidate_id,
            candidate=candidate,
            calibrated=calibrated,
            account_label=str(cell["account_label"]),
            horizon_trading_days=int(cell["horizon_trading_days"]),
            integer_quantity_tier=int(cell["integer_quantity_tier"]),
            policy=policy,
            selected_cell_hash=selected_hash,
            source_exact_result_hash=source_hash,
            source_candidate_result_hash=str(exact_candidate["candidate_result_hash"]),
            account_policy_hash=stable_hash(policy.to_dict()),
            development_evidence_hash=str(frozen_row["development_evidence_hash"]),
        )
    if tuple(output) != tuple(str(row["candidate_id"]) for row in frozen["candidate_cohort"]):
        raise TierQTwoStageError("frozen cohort order/cardinality drift")
    return output, rules, rule_receipt


def build_role_feature_bundles(
    contract: Mapping[str, Any],
    acquisition_receipt: Mapping[str, Any],
    *,
    role: str,
    cache_root: str | Path,
    final_development_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build causal role matrices; confirmation stays unreadable before G."""

    frozen = verify_contract(contract)
    if role not in {FINAL_DEVELOPMENT, CONFIRMATION}:
        raise TierQTwoStageError("unknown two-stage data role")
    if role == CONFIRMATION:
        _authorized_confirmation_ids(frozen, final_development_result)
    elif final_development_result is not None:
        raise TierQTwoStageError("development build does not consume a prior result")
    acquisition = verify_acquisition_receipt(
        frozen, acquisition_receipt, open_role=role
    )

    inventory = dict(acquisition["_inventory"])
    source_roles = (
        ("FINAL_DEVELOPMENT_PARQUET",)
        if role == FINAL_DEVELOPMENT
        else ("FINAL_DEVELOPMENT_PARQUET", "SEALED_CONFIRMATION_PARQUET")
    )
    frames: list[pd.DataFrame] = []
    receipts: list[dict[str, Any]] = []
    expected_roots = {
        str(symbol).split(".", 1)[0]
        for symbol in dict(frozen["data_request"])["symbols"]
    }
    for source_role in source_roles:
        row = dict(inventory[source_role])
        path = Path(str(row["path"])).resolve()
        frame = normalize_ohlcv_frame(
            pd.read_parquet(path), symbol=None, timeframe="1m", symbol_map=None
        )
        validate_ohlcv_frame(frame, timeframe="1m")
        if set(frame["symbol"].astype(str)) != expected_roots:
            raise TierQTwoStageError("role parquet lacks an exact requested root")
        frames.append(frame)
        receipts.append(
            {"role": source_role, "path": str(path), "sha256": row["sha256"], "rows": len(frame)}
        )
    raw = pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"])
    if raw.duplicated(["symbol", "timestamp"]).any():
        raise TierQTwoStageError("duplicate root/timestamp across role partitions")
    coverage_audit = (
        audit_final_development_coverage(raw)
        if role == FINAL_DEVELOPMENT
        else None
    )

    roll_row = dict(inventory["EXPLICIT_CONTRACT_MAP"])
    roll_path = Path(str(roll_row["path"])).resolve()
    roll_map = load_roll_map(roll_path)
    request = dict(frozen["data_request"])
    if (
        roll_map.dataset != request["dataset"]
        or roll_map.schema != request["schema"]
        or not str(roll_map.map_type).startswith("EXPLICIT_DATABENTO_")
        or not expected_roots.issubset(set(roll_map.symbols))
    ):
        raise TierQTwoStageError("explicit 2026 roll map does not cover the freeze")
    mapped, map_receipt = _apply_explicit_contract_map(
        raw, roll_map, required_map_type=roll_map.map_type
    )
    decision_markets = sorted(
        {
            str(row["market"])
            for row in frozen["candidate_cohort"]
        }
        | {
            str(row["reference_market"])
            for row in frozen["candidate_cohort"]
            if row.get("reference_market")
        }
    )
    execution_markets = sorted(
        {str(row["execution_market"]) for row in frozen["candidate_cohort"]}
    )
    required_markets = sorted(set(decision_markets) | set(execution_markets))
    mapped = mapped.loc[mapped["symbol"].astype(str).isin(required_markets)].copy()
    if set(mapped["symbol"].astype(str)) != set(required_markets):
        raise TierQTwoStageError(
            "contract guards removed a required decision or execution market"
        )
    featured = _prepare_feature_frame(_build_past_only_feature_frame(mapped))
    gap_audit = _audit_segment_locality(mapped, featured)
    source_hash = stable_hash(receipts)
    map_hash = _sha256(roll_path)
    store = CanonicalFeatureStore(cache_root)
    bundles: dict[str, Any] = {}
    role_spec = next(row for row in frozen["temporal_roles"] if row["role"] == role)
    source_start = str(frozen["temporal_roles"][0]["start"])
    source_end = str(role_spec["end"])
    for market in required_markets:
        market_frame = featured.loc[featured["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = _market_arrays(market_frame, market)
        arrays.pop("entry_price", None)
        for name in tuple(arrays):
            if name.startswith("forward_move__"):
                arrays.pop(name)
        key = CanonicalFeatureKey(
            market=market,
            explicit_contract_scope=f"{market}_tier_q_2026_{role.lower()}",
            start_inclusive=source_start,
            end_exclusive=source_end,
            source_data_sha256=source_hash,
            roll_map_hash=map_hash,
            transformation_version=FEATURE_VERSION,
            feature_dag_hash=FEATURE_DAG_HASH,
            timeframes=("1m", "5m", "15m", "30m", "60m", "session", "daily"),
        )
        result = store.put(
            key,
            arrays,
            provenance={
                "data_role": role,
                "contract_hash": frozen["contract_hash"],
                "market": market,
                "feature_names": list(metadata["feature_names"]),
                "evaluation_start_inclusive": role_spec["start"],
                "evaluation_end_exclusive": role_spec["end"],
                "future_outcome_arrays_in_decision_bundle": False,
                "entry_fill_contract": "SIGNAL_AFTER_COMPLETED_T_THEN_NEXT_TRADABLE_BAR_OPEN",
            },
        )
        bundles[market] = {
            "path": str(result.path),
            "bundle_hash": result.bundle_hash,
            "row_count": result.row_count,
        }
    identity_rows = []
    for frozen_row_raw in frozen["candidate_cohort"]:
        frozen_row = dict(frozen_row_raw)
        source_market = str(frozen_row["market"])
        execution_market = str(frozen_row["execution_market"])
        reference_market = (
            None
            if not frozen_row.get("reference_market")
            else str(frozen_row["reference_market"])
        )
        identity_rows.append(
            {
                "candidate_id": str(frozen_row["candidate_id"]),
                "decision_market": source_market,
                "reference_market": reference_market,
                "execution_market": execution_market,
                "decision_bundle_hash": bundles[source_market]["bundle_hash"],
                "reference_bundle_hash": (
                    None
                    if reference_market is None
                    else bundles[reference_market]["bundle_hash"]
                ),
                "execution_bundle_hash": bundles[execution_market]["bundle_hash"],
                "decision_and_execution_are_explicitly_separate": (
                    source_market != execution_market
                ),
            }
        )
    source_execution_core = {
        "status": "SOURCE_DECISION_AND_MICRO_EXECUTION_BUNDLES_BOUND",
        "decision_markets": decision_markets,
        "execution_markets": execution_markets,
        "bindings": identity_rows,
        "decision_contract": (
            "FEATURES_SIGNALS_DIRECTION_AND_DECISION_TIME_FROM_DECISION_MARKET"
        ),
        "execution_contract": (
            "ROW_CONTRACT_SEGMENT_SESSION_FILL_AND_OUTCOMES_FROM_EXECUTION_MARKET"
        ),
    }
    source_execution_receipt = {
        **source_execution_core,
        "receipt_hash": stable_hash(source_execution_core),
    }
    core = {
        "schema": FEATURE_SCHEMA,
        "status": "CAUSAL_ROLE_FEATURE_BUNDLES_READY",
        "contract_hash": frozen["contract_hash"],
        "role": role,
        "evaluation_period": [role_spec["start"], role_spec["end"]],
        "source_files": receipts,
        "source_inventory_hash": source_hash,
        "contract_map": {
            "path": str(roll_path),
            "sha256": map_hash,
            "roll_map_hash": roll_map.roll_map_hash(),
            "guard_receipt": map_receipt,
        },
        "coverage_audit": coverage_audit,
        "gap_locality_audit": gap_audit,
        "bundles": bundles,
        "source_execution_identity_receipt": source_execution_receipt,
        "confirmation_opened_after_tier_g": role == CONFIRMATION,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def audit_final_development_coverage(frame: pd.DataFrame) -> dict[str, Any]:
    """Verify the frozen split without converting closed/gappy time into bars."""

    if len(frame) != EXPECTED_FINAL_DEVELOPMENT_ROWS:
        raise TierQTwoStageError("final-development row denominator drift")
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    if (
        timestamps.min() != pd.Timestamp("2026-01-01T23:00:00Z")
        or timestamps.max() != pd.Timestamp("2026-04-30T23:59:00Z")
        or (timestamps >= pd.Timestamp("2026-05-01T00:00:00Z")).any()
    ):
        raise TierQTwoStageError("final-development UTC half-open boundary drift")
    local = timestamps.dt.tz_convert("America/Chicago")
    trading_day = local.dt.normalize().dt.tz_localize(None) + pd.to_timedelta(
        (local.dt.hour >= 17).astype(int), unit="D"
    )
    day_strings = trading_day.dt.strftime("%Y-%m-%d")
    observed_days = tuple(sorted(set(day_strings)))
    if observed_days[-1] != PARTIAL_SPLIT_SESSION_DAY:
        raise TierQTwoStageError("expected split-edge partial session is absent")
    complete_days = tuple(day for day in observed_days if day != PARTIAL_SPLIT_SESSION_DAY)
    if len(complete_days) != EXPECTED_FINAL_DEVELOPMENT_TRADING_DAYS:
        raise TierQTwoStageError("true final-development session count drift")

    values = frame.assign(_trading_day=day_strings)
    partial = values.loc[values["_trading_day"].eq(PARTIAL_SPLIT_SESSION_DAY)]
    partial_counts = {
        str(key): int(value)
        for key, value in partial.groupby("symbol", observed=True).size().items()
    }
    if set(partial_counts) != set(values["symbol"].astype(str)) or any(
        not 113 <= count <= 120 for count in partial_counts.values()
    ):
        raise TierQTwoStageError("split-edge partial-session shape drift")
    good_friday = values.loc[values["_trading_day"].eq(GOOD_FRIDAY_2026)]
    energy_counts = {
        symbol: int(good_friday["symbol"].astype(str).eq(symbol).sum())
        for symbol in ("CL", "MCL")
    }
    if any(energy_counts.values()):
        raise TierQTwoStageError("Good Friday energy closure was not zero-trade")
    core = {
        "status": "FINAL_DEVELOPMENT_COVERAGE_PROVEN",
        "raw_row_count": len(frame),
        "first_timestamp": timestamps.min().isoformat(),
        "last_timestamp": timestamps.max().isoformat(),
        "true_trading_day_count": len(complete_days),
        "true_trading_day_first": complete_days[0],
        "true_trading_day_last": complete_days[-1],
        "partial_split_session": {
            "session_day": PARTIAL_SPLIT_SESSION_DAY,
            "classification": "EXCLUDED_PARTIAL_LABEL_SESSION",
            "rows_by_root": dict(sorted(partial_counts.items())),
            "eligible_as_episode_start": False,
        },
        "good_friday_energy": {
            "session_day": GOOD_FRIDAY_2026,
            "CL_rows": energy_counts["CL"],
            "MCL_rows": energy_counts["MCL"],
            "classification": "CLOSED_ZERO_TRADE_NOT_CENSORED_NOT_SYNTHESIZED",
        },
        "maximum_theoretical_non_overlapping_starts_before_warmup": {
            str(key): value for key, value in EXPECTED_THEORETICAL_STARTS.items()
        },
        "synthetic_bar_count": 0,
        "confirmation_file_opened": False,
    }
    return {**core, "audit_hash": stable_hash(core)}


def _audit_segment_locality(
    mapped: pd.DataFrame, featured: pd.DataFrame
) -> dict[str, Any]:
    """Prove gaps split only affected paths and never delete whole sessions."""

    if len(mapped) != len(featured):
        raise TierQTwoStageError("feature construction dropped rows around an OHLCV gap")
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    bad_segments = 0
    for _key, group in featured.groupby(grouping, sort=False, observed=True):
        delta = pd.to_datetime(group["timestamp"], utc=True).sort_values().diff().iloc[1:]
        bad_segments += int(not delta.eq(pd.Timedelta(minutes=1)).all())
    if bad_segments:
        raise TierQTwoStageError("a contiguous feature segment crosses an OHLCV gap")
    sessions_before = set(
        zip(
            featured["symbol"].astype(str),
            featured["trading_session_id"].astype(str),
            strict=True,
        )
    )
    # Count segment boundaries rather than missing bars; no interval is ever
    # synthesized.  A decision/fill/window may only traverse its own segment.
    segment_count = int(featured.groupby(grouping, observed=True).ngroups)
    core = {
        "status": "GAPS_LOCALIZED_BY_CONTIGUOUS_SEGMENT",
        "mapped_row_count": len(mapped),
        "feature_row_count": len(featured),
        "market_session_count_retained": len(sessions_before),
        "contiguous_segment_count": segment_count,
        "noncontiguous_segment_count": bad_segments,
        "whole_session_removed_by_gap_handling_count": 0,
        "synthetic_fill_count": 0,
        "gap_policy": (
            "NO_TRADE_OR_CENSOR_ONLY_DECISION_FILL_OUTCOME_PATH_CROSSING_"
            "AN_UNEXPECTED_GAP"
        ),
    }
    return {**core, "audit_hash": stable_hash(core)}


def evaluate_stage(
    contract: Mapping[str, Any],
    acquisition_receipt: Mapping[str, Any],
    feature_receipt: Mapping[str, Any],
    *,
    role: str,
    bindings: Mapping[str, FrozenCandidateBinding],
    rules: Mapping[str, Mapping[str, Any]],
    rule_receipt: Mapping[str, Any],
    final_development_result: Mapping[str, Any] | None = None,
    existing_confirmation_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run one immutable economic stage without registry or mission writes."""

    frozen = verify_contract(contract)
    if role == FINAL_DEVELOPMENT:
        if final_development_result is not None or existing_confirmation_result is not None:
            raise TierQTwoStageError("final development cannot overwrite prior evidence")
        candidate_ids = tuple(str(row["candidate_id"]) for row in frozen["candidate_cohort"])
    elif role == CONFIRMATION:
        if existing_confirmation_result is not None:
            raise TierQTwoStageError("one-shot confirmation is already consumed")
        candidate_ids = _authorized_confirmation_ids(frozen, final_development_result)
    else:
        raise TierQTwoStageError("unknown economic-stage role")
    acquisition = verify_acquisition_receipt(
        frozen, acquisition_receipt, open_role=role
    )

    matrices = open_role_matrices(feature_receipt, expected_contract=frozen, expected_role=role)
    results = [
        _evaluate_candidate_role(
            frozen,
            role=role,
            binding=bindings[candidate_id],
            matrices=matrices,
            rule=dict(rules[bindings[candidate_id].account_label]),
        )
        for candidate_id in candidate_ids
    ]
    promoted = sorted(
        str(row["candidate_id"])
        for row in results
        if bool(dict(row["promotion_gate"])["passed"])
    )
    core = {
        "schema": STAGE_SCHEMA,
        "status": (
            "FINAL_DEVELOPMENT_CONSUMED"
            if role == FINAL_DEVELOPMENT
            else "CONFIRMATION_CONSUMED_ONCE"
        ),
        "contract_hash": frozen["contract_hash"],
        "acquisition_receipt_hash": acquisition["receipt_hash"],
        "feature_receipt_hash": feature_receipt["result_hash"],
        "role": role,
        "candidate_results": results,
        "tier_g_candidate_ids": promoted if role == FINAL_DEVELOPMENT else list(candidate_ids),
        "tier_c_candidate_ids": promoted if role == CONFIRMATION else [],
        "promotion_order": ["Q", "G", "C"],
        "retuning_performed": False,
        "recalibration_performed": False,
        "confirmation_evaluated": role == CONFIRMATION,
        "official_rule_snapshot": dict(rule_receipt),
        "source_commit": current_commit(),
        "runner_file_sha256": _sha256(Path(__file__).resolve()),
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def open_role_matrices(
    feature_receipt: Mapping[str, Any],
    *,
    expected_contract: Mapping[str, Any],
    expected_role: str,
) -> dict[str, FeatureMatrix]:
    receipt = dict(feature_receipt)
    claimed = str(receipt.pop("result_hash", ""))
    if (
        not claimed
        or stable_hash(receipt) != claimed
        or feature_receipt.get("schema") != FEATURE_SCHEMA
        or feature_receipt.get("role") != expected_role
        or feature_receipt.get("contract_hash") != expected_contract["contract_hash"]
    ):
        raise TierQTwoStageError("feature receipt identity/role drift")
    output: dict[str, FeatureMatrix] = {}
    for market, raw in dict(feature_receipt["bundles"]).items():
        binding = dict(raw)
        matrix = FeatureMatrix.open(binding["path"], mmap=True)
        if matrix.fingerprint != str(binding["bundle_hash"]):
            raise TierQTwoStageError("role feature bundle hash drift")
        provenance = dict(matrix.manifest.get("provenance") or {})
        if provenance.get("data_role") != expected_role:
            raise TierQTwoStageError("role feature matrix provenance drift")
        output[str(market)] = matrix
    return output


def tier_g_gate(
    normal: Mapping[str, Any],
    stressed: Mapping[str, Any],
    concentration: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> dict[str, Any]:
    normal_blocks = dict(normal.get("by_block") or {})
    stressed_blocks = dict(stressed.get("by_block") or {})
    positive_contexts = sum(
        int(dict(normal_blocks.get(block) or {}).get("pass_count", 0)) > 0
        and int(dict(stressed_blocks.get(block) or {}).get("pass_count", 0)) > 0
        for block in sorted(set(normal_blocks) | set(stressed_blocks))
    )
    maximum_share = max(
        (
            float(value)
            for value in dict(concentration.get("worst_case_maximums") or {}).values()
        ),
        default=1.0,
    )
    checks = {
        "minimum_normal_passes": int(normal.get("pass_count", 0))
        >= int(thresholds["minimum_normal_passes"]),
        "minimum_stressed_passes": int(stressed.get("pass_count", 0))
        >= int(thresholds["minimum_stressed_passes"]),
        "minimum_positive_temporal_contexts": positive_contexts
        >= int(thresholds["minimum_positive_temporal_contexts"]),
        "positive_stressed_net": float(stressed.get("net_total_usd", 0.0)) > 0.0,
        "controlled_stressed_mll": float(stressed.get("mll_breach_rate", 1.0))
        <= float(thresholds["maximum_stressed_mll_breach_rate"]),
        "concentration_complete_and_controlled": bool(concentration.get("cleared"))
        and maximum_share
        <= float(thresholds["single_trade_or_day_profit_share_maximum"]),
        "complete_accounting": bool(normal.get("episode_path_hash"))
        and bool(stressed.get("episode_path_hash")),
        "no_retuning": thresholds.get("no_retuning") is True,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "positive_temporal_context_count": positive_contexts,
        "maximum_profit_concentration_share": maximum_share,
        "resulting_tier": "G" if all(checks.values()) else "Q_FINAL_DEVELOPMENT_FAILED",
    }


def tier_c_gate(
    normal: Mapping[str, Any],
    stressed: Mapping[str, Any],
    concentration: Mapping[str, Any],
    thresholds: Mapping[str, Any],
    *,
    prior_tier_g: bool,
    selected_horizon_matches: bool,
    full_coverage_start_count: int,
) -> dict[str, Any]:
    passing_consistency = bool(stressed.get("passing_paths_consistency_compliant"))
    maximum_share = max(
        (
            float(value)
            for value in dict(concentration.get("worst_case_maximums") or {}).values()
        ),
        default=1.0,
    )
    checks = {
        "prior_tier_g": bool(prior_tier_g),
        "selected_horizon_matches": bool(selected_horizon_matches),
        "minimum_normal_passes": int(normal.get("pass_count", 0))
        >= int(thresholds["minimum_normal_passes"]),
        "minimum_stressed_passes": int(stressed.get("pass_count", 0))
        >= int(thresholds["minimum_stressed_passes"]),
        "positive_stressed_net": float(stressed.get("net_total_usd", 0.0)) > 0.0,
        "controlled_stressed_mll": float(stressed.get("mll_breach_rate", 1.0))
        <= float(thresholds["maximum_stressed_mll_breach_rate"]),
        "stressed_passing_consistency": passing_consistency,
        "complete_coverage": int(full_coverage_start_count) > 0,
        "concentration_complete_and_controlled": bool(concentration.get("cleared"))
        and maximum_share
        <= float(thresholds["single_trade_or_day_profit_share_maximum"]),
        "no_retuning": thresholds.get("no_retuning") is True,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "maximum_profit_concentration_share": maximum_share,
        "resulting_tier": "C" if all(checks.values()) else "G_CONFIRMATION_FAILED",
    }


def _execution_contract_session_view(matrix: FeatureMatrix) -> FeatureMatrix:
    """Replace trade-derived gap segments with roll/session-safe path segments."""

    contracts = matrix.array("contract_code")
    session_days = matrix.array("session_day")
    changes = np.ones(len(contracts), dtype=bool)
    if len(changes) > 1:
        changes[1:] = (contracts[1:] != contracts[:-1]) | (
            session_days[1:] != session_days[:-1]
        )
    segments = np.cumsum(changes, dtype=np.int64) - 1
    segments.flags.writeable = False
    arrays = dict(matrix.arrays)
    arrays["segment_code"] = segments
    manifest = dict(matrix.manifest)
    manifest["bundle_hash"] = stable_hash(
        {
            "schema": "hydra_sparse_next_tradable_execution_view_v1",
            "source_execution_matrix_hash": matrix.fingerprint,
            "segment_contract": "ACTIVE_CONTRACT_AND_SESSION_DAY_ONLY",
            "missing_trade_minutes": "NO_SYNTHESIS_NOT_A_PATH_BREAK",
        }
    )
    return FeatureMatrix(root=matrix.root, manifest=manifest, arrays=arrays)


def _session_flatten_ns(session_day: int) -> int:
    local_day = pd.Timestamp(int(session_day), unit="D").tz_localize(
        "America/Chicago"
    )
    return int(
        (local_day + pd.Timedelta(hours=15, minutes=10)).tz_convert("UTC").value
    )


def _outward_tick_barriers(
    *,
    direction: int,
    tick_size: float,
    favorable_price: float,
    adverse_price: float,
) -> tuple[float, float]:
    """Quantize both barriers away from entry on the execution tick lattice."""

    tick = float(tick_size)
    if direction not in {-1, 1} or not math.isfinite(tick) or tick <= 0.0:
        raise TierQTwoStageError("invalid direction or execution tick for barriers")

    def units(value: float) -> float:
        quotient = float(value) / tick
        nearest = round(quotient)
        # Binary division of an exactly aligned decimal price can land a few
        # ulps either side of its integer tick.  Snap only that numerical dust;
        # all economically non-aligned prices retain directed rounding.
        return (
            float(nearest)
            if math.isclose(quotient, nearest, rel_tol=1e-12, abs_tol=1e-9)
            else quotient
        )

    favorable_units = units(favorable_price)
    adverse_units = units(adverse_price)
    if direction > 0:
        target = math.ceil(favorable_units) * tick
        stop = math.floor(adverse_units) * tick
    else:
        target = math.floor(favorable_units) * tick
        stop = math.ceil(adverse_units) * tick
    return float(target), float(stop)


def _is_tick_aligned(value: float, tick_size: float) -> bool:
    quotient = float(value) / float(tick_size)
    return math.isclose(
        quotient,
        round(quotient),
        rel_tol=1e-12,
        abs_tol=1e-9,
    )


def _sparse_next_tradable_traversal(
    candidate: HazardCandidate,
    intent: HazardIntent,
    *,
    entry_index: int,
    normal_entry: float,
    favorable_price: float,
    adverse_price: float,
    risk_unit: float,
    timestamp: np.ndarray,
    availability: np.ndarray,
    opens: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    segments: np.ndarray,
    contracts: np.ndarray,
    days: np.ndarray,
) -> dict[str, Any]:
    """Observe sparse trade-derived bars without inventing missing minutes."""

    entry_time = int(timestamp[entry_index])
    flatten_time = _session_flatten_ns(intent.session_day)
    deadline = (
        flatten_time
        if candidate.horizon == "SESSION"
        else entry_time + hazard._numeric_horizon(candidate.horizon) * hazard.MINUTE_NS
    )
    best = normal_entry
    worst = normal_entry
    path_indices: list[int] = []
    for index in range(entry_index, len(timestamp)):
        ts = int(timestamp[index])
        if int(contracts[index]) != int(intent.contract_code):
            return hazard._censor_observation(
                "EXECUTION_ACTIVE_CONTRACT_CHANGED_BEFORE_EXIT"
            )
        if (
            int(days[index]) != int(intent.session_day)
            or int(segments[index]) != int(intent.segment_code)
        ):
            return hazard._censor_observation(
                "EXECUTION_SESSION_CHANGED_BEFORE_EXIT"
            )
        # The time exit is an order waiting for the first real micro open at or
        # after its deadline.  Missing trade minutes are not synthetic bars and
        # do not by themselves censor the path.
        if ts >= deadline:
            if ts > flatten_time:
                return hazard._censor_observation(
                    "SESSION_FLATTEN_OPEN_UNAVAILABLE_BY_DEADLINE"
                )
            if not math.isfinite(float(opens[index])):
                return hazard._censor_observation(
                    "MAX_HORIZON_NEXT_TRADABLE_OPEN_NONFINITE"
                )
            return hazard._observed_neither(
                intent,
                normal_entry=normal_entry,
                risk_unit=risk_unit,
                best=best,
                worst=worst,
                raw_exit=float(opens[index]),
                outcome_time_ns=ts,
                exit_fill_semantics="MAX_HORIZON_FIRST_OBSERVED_TRADABLE_OPEN",
                path_indices=path_indices,
            )
        if not all(
            math.isfinite(float(value))
            for value in (opens[index], highs[index], lows[index], closes[index])
        ):
            return hazard._censor_observation("EXECUTION_PATH_NONFINITE_OHLC")
        high = float(highs[index])
        low = float(lows[index])
        path_indices.append(index)
        prior_best = best
        prior_worst = worst
        if intent.direction > 0:
            favorable_hit = high >= favorable_price
            adverse_hit = low <= adverse_price
            best = max(best, high)
            worst = min(worst, low)
        else:
            favorable_hit = low <= favorable_price
            adverse_hit = high >= adverse_price
            best = min(best, low)
            worst = max(worst, high)
        elapsed = int((ts - entry_time) // hazard.MINUTE_NS)
        if favorable_hit or adverse_hit:
            adverse_first = bool(adverse_hit)
            raw_exit = adverse_price if adverse_first else favorable_price
            terminal_best = prior_best if adverse_first else favorable_price
            terminal_worst = adverse_price if adverse_first else worst
            return {
                "outcome": (
                    HazardOutcome.ADVERSE_FIRST
                    if adverse_first
                    else HazardOutcome.FAVORABLE_FIRST
                ),
                "outcome_time_ns": int(availability[index]),
                "time_to_favorable_minutes": (None if adverse_first else elapsed),
                "time_to_adverse_minutes": (elapsed if adverse_first else None),
                "mfe_r": max(
                    0.0,
                    (terminal_best - normal_entry) * intent.direction / risk_unit,
                ),
                "mae_r": max(
                    0.0,
                    (normal_entry - terminal_worst) * intent.direction / risk_unit,
                ),
                "raw_exit_price": raw_exit,
                "best_raw_price": terminal_best,
                "worst_raw_price": terminal_worst,
                "same_bar_ambiguous": bool(favorable_hit and adverse_hit),
                "path_indices": tuple(path_indices),
                "exit_fill_semantics": (
                    "RESTING_ADVERSE_BARRIER_INTRABAR_CONSERVATIVE"
                    if adverse_first
                    else "RESTING_FAVORABLE_BARRIER_INTRABAR"
                ),
                "censor_reason": None,
            }
    return hazard._censor_observation(
        "EXECUTION_DATA_END_BEFORE_TARGET_STOP_OR_EXIT"
    )


def _append_terminal_mark_if_needed(
    marks: Sequence[Any],
    *,
    outcome_time_ns: int,
    terminal_net: float,
) -> tuple[Any, ...]:
    values = list(marks)
    if values and int(values[-1].availability_time_ns) == int(outcome_time_ns):
        return tuple(values)
    worst = min(
        [float(terminal_net)]
        + [float(value.worst_unrealized_pnl) for value in values]
    )
    best = max(
        [float(terminal_net)]
        + [float(value.best_unrealized_pnl) for value in values]
    )
    values.append(
        hazard.CausalTradeMark(
            availability_time_ns=int(outcome_time_ns),
            worst_unrealized_pnl=worst,
            best_unrealized_pnl=best,
            current_unrealized_pnl=float(terminal_net),
        )
    )
    return tuple(values)


def _observe_sparse_execution_outcomes(
    calibrated: CalibratedHazardCandidate,
    matrix: FeatureMatrix,
    intents: Sequence[HazardIntent],
) -> tuple[Any, ...]:
    """Local next-tradable-event observer; the shared dense kernel is unchanged."""

    candidate = calibrated.candidate
    fill_policy = hazard.CausalFillPolicy()
    payload = fill_policy.resolved_payload(
        candidate.execution_market,
        min(60, hazard._numeric_horizon(candidate.horizon)),
    )
    fill_hash = hazard.stable_hash(payload)
    instrument = hazard.instrument_spec(candidate.execution_market)
    quantity = hazard.RISK_LEVEL_TO_MICRO_UNITS[candidate.risk_level]
    normal_ticks = float(payload["normal_slippage_ticks_per_side"])
    stressed_ticks = float(payload["stressed_slippage_ticks_per_side"])
    commission = float(payload["commission_round_turn_usd"]) * quantity
    timestamp = matrix.array("timestamp_ns")
    availability = matrix.array("availability_ns")
    opens = matrix.array("bar_open")
    highs = matrix.array("bar_high")
    lows = matrix.array("bar_low")
    closes = matrix.array("bar_close")
    segments = matrix.array("segment_code")
    contracts = matrix.array("contract_code")
    days = matrix.array("session_day")
    volatility = matrix.array("feature__past_volatility")
    evidence: list[Any] = []
    for ordinal, intent in enumerate(intents, start=1):
        entry_index = int(
            np.searchsorted(timestamp, int(intent.earliest_executable_time_ns), side="left")
        )
        if (
            entry_index >= len(timestamp)
            or int(timestamp[entry_index]) != int(intent.earliest_executable_time_ns)
            or int(segments[entry_index]) != int(intent.segment_code)
            or int(contracts[entry_index]) != int(intent.contract_code)
            or int(days[entry_index]) != int(intent.session_day)
        ):
            evidence.append(
                hazard._censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason="NEXT_TRADABLE_EXECUTION_OPEN_UNOBSERVED_OR_PATH_CHANGED",
                    fill_policy_hash=fill_hash,
                )
            )
            continue
        raw_entry = float(opens[entry_index])
        if not math.isfinite(raw_entry):
            evidence.append(
                hazard._censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason="NEXT_TRADABLE_EXECUTION_OPEN_NONFINITE",
                    fill_policy_hash=fill_hash,
                )
            )
            continue
        normal_entry = raw_entry + intent.direction * normal_ticks * instrument.tick_size
        stressed_entry = raw_entry + intent.direction * stressed_ticks * instrument.tick_size
        signal_volatility = float(volatility[intent.row_index])
        risk_unit = max(
            2.0 * float(instrument.tick_size),
            abs(raw_entry) * max(signal_volatility, 0.0) * math.sqrt(15.0),
        )
        if not math.isfinite(risk_unit) or risk_unit <= 0:
            evidence.append(
                hazard._censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason="CAUSAL_EXECUTION_RISK_UNIT_UNAVAILABLE",
                    fill_policy_hash=fill_hash,
                )
            )
            continue
        unquantized_favorable = (
            normal_entry + intent.direction * candidate.favorable_r * risk_unit
        )
        unquantized_adverse = (
            normal_entry - intent.direction * candidate.adverse_r * risk_unit
        )
        favorable_price, adverse_price = _outward_tick_barriers(
            direction=intent.direction,
            tick_size=float(instrument.tick_size),
            favorable_price=unquantized_favorable,
            adverse_price=unquantized_adverse,
        )
        observed = _sparse_next_tradable_traversal(
            candidate,
            intent,
            entry_index=entry_index,
            normal_entry=normal_entry,
            favorable_price=favorable_price,
            adverse_price=adverse_price,
            risk_unit=risk_unit,
            timestamp=timestamp,
            availability=availability,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            segments=segments,
            contracts=contracts,
            days=days,
        )
        if observed["outcome"] == HazardOutcome.CENSORED_FUTURE_COVERAGE:
            evidence.append(
                hazard._censored_event(
                    candidate,
                    intent,
                    ordinal=ordinal,
                    quantity=quantity,
                    reason=str(observed["censor_reason"]),
                    fill_policy_hash=fill_hash,
                    fill_time_ns=int(timestamp[entry_index]),
                    raw_fill=raw_entry,
                    normal_fill=normal_entry,
                    stressed_fill=stressed_entry,
                    risk_unit=risk_unit,
                    favorable_price=favorable_price,
                    adverse_price=adverse_price,
                )
            )
            continue
        raw_exit = float(observed["raw_exit_price"])
        normal_exit = raw_exit - intent.direction * normal_ticks * instrument.tick_size
        stressed_exit = raw_exit - intent.direction * stressed_ticks * instrument.tick_size
        point_value = float(instrument.point_value)
        normal_net = (
            (normal_exit - normal_entry) * intent.direction * point_value * quantity
            - commission
        )
        stressed_net = (
            (stressed_exit - stressed_entry) * intent.direction * point_value * quantity
            - commission
        )
        normal_adverse = (
            (float(observed["worst_raw_price"]) - normal_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        stressed_adverse = (
            (float(observed["worst_raw_price"]) - stressed_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        normal_favorable = (
            (float(observed["best_raw_price"]) - normal_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        stressed_favorable = (
            (float(observed["best_raw_price"]) - stressed_entry)
            * intent.direction
            * point_value
            * quantity
            - commission
        )
        normal_initial = (
            (raw_entry - normal_entry) * intent.direction * point_value * quantity
            - commission
        )
        stressed_initial = (
            (raw_entry - stressed_entry) * intent.direction * point_value * quantity
            - commission
        )
        barrier_outcome = (
            str(observed["outcome"])
            if str(observed["exit_fill_semantics"]).startswith("RESTING_")
            else None
        )
        normal_marks = hazard._economic_marks(
            observed["path_indices"],
            entry=normal_entry,
            direction=intent.direction,
            quantity=quantity,
            point_value=point_value,
            commission=commission,
            availability=availability,
            highs=highs,
            lows=lows,
            closes=closes,
            terminal_net=normal_net,
            barrier_outcome=barrier_outcome,
            terminal_raw_exit=raw_exit,
        )
        stressed_marks = hazard._economic_marks(
            observed["path_indices"],
            entry=stressed_entry,
            direction=intent.direction,
            quantity=quantity,
            point_value=point_value,
            commission=commission,
            availability=availability,
            highs=highs,
            lows=lows,
            closes=closes,
            terminal_net=stressed_net,
            barrier_outcome=barrier_outcome,
            terminal_raw_exit=raw_exit,
        )
        normal_marks = _append_terminal_mark_if_needed(
            normal_marks,
            outcome_time_ns=int(observed["outcome_time_ns"]),
            terminal_net=normal_net,
        )
        stressed_marks = _append_terminal_mark_if_needed(
            stressed_marks,
            outcome_time_ns=int(observed["outcome_time_ns"]),
            terminal_net=stressed_net,
        )
        evidence.append(
            hazard.HazardEventEvidence(
                event_id=f"{intent.intent_namespace}:{ordinal:07d}",
                candidate_id=candidate.candidate_id,
                intent_namespace=intent.intent_namespace,
                evidence_role=intent.evidence_role,
                control_id=intent.control_id,
                market=candidate.market,
                execution_market=candidate.execution_market,
                contract_code=int(intent.contract_code),
                timeframe=candidate.timeframe,
                session_day=int(intent.session_day),
                session_code=int(intent.session_code),
                segment_code=int(intent.segment_code),
                event_time_ns=int(intent.event_time_ns),
                available_at_ns=int(intent.available_at_ns),
                decision_time_ns=int(intent.decision_time_ns),
                order_submit_time_ns=int(intent.order_submit_time_ns),
                entry_intent=intent.entry_intent,
                earliest_executable_time_ns=int(intent.earliest_executable_time_ns),
                fill_time_ns=int(timestamp[entry_index]),
                raw_fill_price=raw_entry,
                normal_fill_price=float(normal_entry),
                stressed_fill_price=float(stressed_entry),
                direction=int(intent.direction),
                quantity=quantity,
                risk_unit_price=float(risk_unit),
                favorable_r=float(candidate.favorable_r),
                adverse_r=float(candidate.adverse_r),
                favorable_price=float(favorable_price),
                adverse_price=float(adverse_price),
                maximum_horizon=candidate.horizon,
                outcome=str(observed["outcome"]),
                outcome_time_ns=int(observed["outcome_time_ns"]),
                time_to_favorable_minutes=observed["time_to_favorable_minutes"],
                time_to_adverse_minutes=observed["time_to_adverse_minutes"],
                maximum_favorable_excursion_r=float(observed["mfe_r"]),
                maximum_adverse_excursion_r=float(observed["mae_r"]),
                raw_exit_price=raw_exit,
                exit_fill_semantics=str(observed["exit_fill_semantics"]),
                normal_net_pnl=float(normal_net),
                stressed_net_pnl=float(stressed_net),
                normal_worst_unrealized_pnl=float(min(normal_initial, normal_adverse)),
                stressed_worst_unrealized_pnl=float(
                    min(stressed_initial, stressed_adverse)
                ),
                normal_best_unrealized_pnl=float(max(normal_initial, normal_favorable)),
                stressed_best_unrealized_pnl=float(
                    max(stressed_initial, stressed_favorable)
                ),
                normal_initial_unrealized_pnl=float(normal_initial),
                stressed_initial_unrealized_pnl=float(stressed_initial),
                normal_marks=normal_marks,
                stressed_marks=stressed_marks,
                same_bar_ambiguous=bool(observed["same_bar_ambiguous"]),
                censor_reason=None,
                feature_fingerprint=intent.feature_fingerprint,
                fill_policy_id=fill_policy.policy_id,
                fill_policy_hash=fill_hash,
            )
        )
    return tuple(evidence)


def _remap_and_observe_execution_outcomes(
    calibrated: CalibratedHazardCandidate,
    execution_matrix: FeatureMatrix,
    source_intents: Sequence[HazardIntent],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Route frozen source decisions onto the actual micro execution timeline.

    Signal features, direction and all decision timestamps stay attached to the
    source market.  Only execution context and post-decision prices come from
    the candidate's declared micro contract.  A missing context/open or a path
    discontinuity censors the already-emitted signal; it can never remove it.
    """

    candidate = calibrated.candidate
    execution_view = _execution_contract_session_view(execution_matrix)
    timestamp = execution_view.array("timestamp_ns")
    availability = execution_view.array("availability_ns")
    contracts = execution_view.array("contract_code")
    segments = execution_view.array("segment_code")
    session_days = execution_view.array("session_day")
    session_codes = execution_view.array("session_code")
    opens = execution_view.array("bar_open")
    if len(timestamp) == 0 or np.any(timestamp[1:] <= timestamp[:-1]):
        raise TierQTwoStageError("execution timeline is empty or non-chronological")
    if np.any(availability[1:] < availability[:-1]):
        raise TierQTwoStageError("execution availability timeline is non-chronological")

    valid: list[tuple[int, HazardIntent]] = []
    failed: list[tuple[int, HazardIntent, str]] = []
    trace: list[dict[str, Any]] = []
    preserved_fields = (
        "event_time_ns",
        "available_at_ns",
        "decision_time_ns",
        "order_submit_time_ns",
        "entry_intent",
        "direction",
        "feature_fingerprint",
    )
    preservation_mismatches = Counter()
    for ordinal, source in enumerate(source_intents, start=1):
        context_index = int(
            np.searchsorted(availability, int(source.decision_time_ns), side="right")
            - 1
        )
        if context_index < 0:
            remapped = replace(source, market=candidate.execution_market)
            reason = "EXECUTION_CONTEXT_UNAVAILABLE_BEFORE_DECISION"
            failed.append((ordinal, remapped, reason))
            trace.append(
                {
                    "ordinal": ordinal,
                    "source_intent_hash": source.fingerprint,
                    "execution_intent_hash": remapped.fingerprint,
                    "status": "CENSORED",
                    "reason": reason,
                    "execution_context_index": None,
                    "execution_fill_index": None,
                }
            )
            continue

        remapped = replace(
            source,
            row_index=context_index,
            market=candidate.execution_market,
            contract_code=int(contracts[context_index]),
            session_day=int(session_days[context_index]),
            session_code=int(session_codes[context_index]),
            segment_code=int(segments[context_index]),
        )
        for field in preserved_fields:
            if getattr(remapped, field) != getattr(source, field):
                preservation_mismatches[field] += 1

        entry_index = int(
            np.searchsorted(
                timestamp, int(source.earliest_executable_time_ns), side="left"
            )
        )
        reason: str | None = None
        if entry_index >= len(timestamp):
            reason = "EXECUTION_OPEN_UNAVAILABLE_AFTER_SOURCE_EARLIEST"
        elif entry_index <= context_index:
            reason = "EXECUTION_OPEN_NOT_AFTER_CONTEXT"
        elif int(contracts[entry_index]) != int(contracts[context_index]):
            reason = "EXECUTION_CONTRACT_CHANGED_BEFORE_FILL"
        elif int(session_days[entry_index]) != int(session_days[context_index]):
            reason = "EXECUTION_SESSION_CHANGED_BEFORE_FILL"
        elif int(segments[entry_index]) != int(segments[context_index]):
            reason = "EXECUTION_SEGMENT_CHANGED_BEFORE_FILL"
        elif not np.isfinite(float(opens[entry_index])):
            reason = "EXECUTION_OPEN_NONFINITE"

        if reason is not None:
            failed.append((ordinal, remapped, reason))
            status = "CENSORED"
        else:
            remapped = replace(
                remapped,
                earliest_executable_time_ns=int(timestamp[entry_index]),
            )
            valid.append((ordinal, remapped))
            status = "REMAPPED"
        trace.append(
            {
                "ordinal": ordinal,
                "source_intent_hash": source.fingerprint,
                "execution_intent_hash": remapped.fingerprint,
                "status": status,
                "reason": reason,
                "execution_context_index": context_index,
                "execution_context_timestamp_ns": int(timestamp[context_index]),
                "execution_fill_index": (
                    entry_index if entry_index < len(timestamp) else None
                ),
                "execution_fill_timestamp_ns": (
                    int(timestamp[entry_index]) if entry_index < len(timestamp) else None
                ),
            }
        )
    if preservation_mismatches:
        raise TierQTwoStageError(
            f"source decision fields changed during execution remap: "
            f"{dict(preservation_mismatches)}"
        )

    observed_valid = _observe_sparse_execution_outcomes(
        calibrated,
        execution_view,
        tuple(intent for _, intent in valid),
    )
    if len(observed_valid) != len(valid):
        raise TierQTwoStageError("execution outcome count differs from remapped intents")
    events_by_ordinal = {
        ordinal: replace(
            event,
            event_id=f"{intent.intent_namespace}:{ordinal:07d}",
        )
        for (ordinal, intent), event in zip(valid, observed_valid, strict=True)
    }
    fill_policy = hazard.CausalFillPolicy()
    fill_hash = hazard.stable_hash(
        fill_policy.resolved_payload(
            candidate.execution_market,
            min(60, hazard._numeric_horizon(candidate.horizon)),
        )
    )
    quantity = hazard.RISK_LEVEL_TO_MICRO_UNITS[candidate.risk_level]
    for ordinal, intent, reason in failed:
        events_by_ordinal[ordinal] = hazard._censored_event(
            candidate,
            intent,
            ordinal=ordinal,
            quantity=quantity,
            reason=reason,
            fill_policy_hash=fill_hash,
        )
    if tuple(sorted(events_by_ordinal)) != tuple(range(1, len(source_intents) + 1)):
        raise TierQTwoStageError("execution remap lost or duplicated a source signal")
    events = tuple(events_by_ordinal[index] for index in sorted(events_by_ordinal))

    price_trace = []
    price_mismatches = 0
    barrier_trace = []
    barrier_alignment_mismatches = 0
    raw_exit_trace = []
    raw_exit_alignment_mismatches = 0
    execution_tick = float(hazard.instrument_spec(candidate.execution_market).tick_size)
    for event in events:
        if event.favorable_price is not None and event.adverse_price is not None:
            favorable_aligned = _is_tick_aligned(
                float(event.favorable_price), execution_tick
            )
            adverse_aligned = _is_tick_aligned(
                float(event.adverse_price), execution_tick
            )
            barrier_alignment_mismatches += int(
                not favorable_aligned or not adverse_aligned
            )
            barrier_trace.append(
                {
                    "event_id": event.event_id,
                    "favorable_price": float(event.favorable_price),
                    "adverse_price": float(event.adverse_price),
                    "favorable_tick_aligned": favorable_aligned,
                    "adverse_tick_aligned": adverse_aligned,
                }
            )
        if event.raw_exit_price is not None:
            exit_aligned = _is_tick_aligned(
                float(event.raw_exit_price), execution_tick
            )
            raw_exit_alignment_mismatches += int(not exit_aligned)
            raw_exit_trace.append(
                {
                    "event_id": event.event_id,
                    "raw_exit_price": float(event.raw_exit_price),
                    "tick_aligned": exit_aligned,
                }
            )
        if event.fill_time_ns is None:
            continue
        fill_index = int(np.searchsorted(timestamp, int(event.fill_time_ns), side="left"))
        expected = (
            None
            if fill_index >= len(timestamp)
            or int(timestamp[fill_index]) != int(event.fill_time_ns)
            else float(opens[fill_index])
        )
        actual = None if event.raw_fill_price is None else float(event.raw_fill_price)
        if expected is None or actual is None or not np.isclose(
            actual, expected, rtol=0.0, atol=0.0
        ):
            price_mismatches += 1
        price_trace.append(
            {
                "event_id": event.event_id,
                "fill_time_ns": int(event.fill_time_ns),
                "execution_open": expected,
                "raw_fill_price": actual,
            }
        )
    if price_mismatches:
        raise TierQTwoStageError("an observed fill was not priced from the micro matrix")
    if barrier_alignment_mismatches or raw_exit_alignment_mismatches:
        raise TierQTwoStageError(
            "a micro barrier or raw exit escaped the execution tick lattice"
        )

    receipt_core = {
        "status": "SOURCE_DECISIONS_REMAPPED_TO_MICRO_EXECUTION_TIMELINE",
        "candidate_id": candidate.candidate_id,
        "decision_market": candidate.market,
        "execution_market": candidate.execution_market,
        "source_intent_count": len(source_intents),
        "execution_remapped_count": len(valid),
        "execution_mapping_censored_count": len(failed),
        "output_event_count": len(events),
        "source_decision_fields_preserved": not preservation_mismatches,
        "preserved_fields": list(preserved_fields),
        "execution_matrix_hash": execution_matrix.fingerprint,
        "sparse_execution_view_hash": execution_view.fingerprint,
        "execution_path_contract": (
            "FIRST_OBSERVED_TRADABLE_OPEN_WITHIN_ACTIVE_CONTRACT_AND_SESSION"
        ),
        "missing_trade_minute_policy": "NO_SYNTHESIS_NO_AUTOMATIC_CENSOR",
        "mapping_trace_hash": stable_hash(trace),
        "execution_fill_price_check_count": len(price_trace),
        "execution_fill_price_mismatch_count": price_mismatches,
        "execution_fill_price_trace_hash": stable_hash(price_trace),
        "barrier_quantization_rule": dict(BARRIER_QUANTIZATION_RULE),
        "barrier_quantization_rule_hash": BARRIER_QUANTIZATION_RULE_HASH,
        "execution_tick_size": execution_tick,
        "barrier_tick_alignment_check_count": len(barrier_trace),
        "barrier_tick_alignment_mismatch_count": barrier_alignment_mismatches,
        "barrier_tick_alignment_trace_hash": stable_hash(barrier_trace),
        "raw_exit_tick_alignment_check_count": len(raw_exit_trace),
        "raw_exit_tick_alignment_mismatch_count": raw_exit_alignment_mismatches,
        "raw_exit_tick_alignment_trace_hash": stable_hash(raw_exit_trace),
    }
    return events, {**receipt_core, "receipt_hash": stable_hash(receipt_core)}


def _selected_horizon_coverage(
    replay: Any,
    calendar: Sequence[int],
    starts: Sequence[tuple[int, str]],
    *,
    horizon: int,
) -> dict[str, Any]:
    """Apply the canonical candidate-coverage rule to one selected horizon."""

    index = {int(day): position for position, day in enumerate(calendar)}
    eligible = {int(value) for value in replay.eligible_session_days}
    censored_days = {
        int(row.session_day)
        for row in replay.events
        if str(getattr(row.outcome, "value", row.outcome))
        == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
    }
    full: list[tuple[int, str]] = []
    censored: list[dict[str, Any]] = []
    for start_day, block in starts:
        if int(start_day) not in index:
            raise TierQTwoStageError("frozen start is absent from candidate calendar")
        position = index[int(start_day)]
        window = tuple(int(day) for day in calendar[position : position + horizon])
        reasons: list[str] = []
        if len(window) != horizon:
            reasons.append("INCOMPLETE_HORIZON")
        if any(day not in eligible for day in window):
            reasons.append("INELIGIBLE_SESSION_IN_WINDOW")
        affected = sorted(set(window) & censored_days)
        if affected:
            reasons.append("CENSORED_EVENT_IN_WINDOW")
        if reasons:
            censored.append(
                {
                    "start_day": int(start_day),
                    "temporal_block": str(block),
                    "coverage_state": "DATA_CENSORED",
                    "reasons": reasons,
                    "censored_session_days": affected,
                }
            )
        else:
            full.append((int(start_day), str(block)))
    core = {
        "selected_horizon_trading_days": int(horizon),
        "headline_denominator_excludes_censored": True,
        "candidate_event_censored_session_days": sorted(censored_days),
        "full_coverage_starts": [
            {"start_day": day, "temporal_block": block, "coverage_state": "FULL_COVERAGE"}
            for day, block in full
        ],
        "data_censored_starts": censored,
    }
    return {
        **core,
        "full": tuple(full),
        "receipt_hash": stable_hash(core),
    }


def _evaluate_candidate_role(
    contract: Mapping[str, Any],
    *,
    role: str,
    binding: FrozenCandidateBinding,
    matrices: Mapping[str, FeatureMatrix],
    rule: Mapping[str, Any],
) -> dict[str, Any]:
    candidate = binding.candidate
    expected_horizon = (
        5
        if candidate.execution_market == "MES"
        else 10
        if candidate.execution_market == "MCL"
        else 20
    )
    if binding.horizon_trading_days != expected_horizon:
        raise TierQTwoStageError(
            f"selected-horizon contract drift: {binding.candidate_id}"
        )
    decision_matrix = matrices[candidate.market]
    matrix = decision_matrix
    if candidate.cross_asset_reference_market:
        matrix = with_availability_safe_cross_asset_feature(
            matrix, matrices[str(candidate.cross_asset_reference_market)]
        )
    role_spec = next(row for row in contract["temporal_roles"] if row["role"] == role)
    start_ns = _date_ns(str(role_spec["start"]))
    end_ns = _date_ns(str(role_spec["end"]))
    intents = discover_intents_batch(
        binding.calibrated,
        matrix,
        evaluation_start_ns=start_ns,
        evaluation_end_exclusive_ns=end_ns,
    )
    streaming = discover_intents_streaming(
        binding.calibrated,
        matrix,
        evaluation_start_ns=start_ns,
        evaluation_end_exclusive_ns=end_ns,
    )
    if tuple((row.row_index, row.direction) for row in intents) != streaming:
        raise TierQTwoStageError(f"batch/stream mismatch: {binding.candidate_id}")
    role_end_day = end_ns // DAY_NS
    if role == FINAL_DEVELOPMENT:
        # The UTC split cuts through the next Globex trading session.  Its
        # partial rows are retained in the immutable source audit but excluded
        # from this predeclared role before labels/outcomes are attached.
        intents = tuple(
            row for row in intents if int(row.session_day) < int(role_end_day)
        )
    execution_matrix = matrices[candidate.execution_market]
    events, execution_identity = _remap_and_observe_execution_outcomes(
        binding.calibrated,
        execution_matrix,
        intents,
    )
    calendar = frozen_eligible_session_calendar(
        candidate,
        decision_matrix,
        evaluation_start_ns=start_ns,
        evaluation_end_exclusive_ns=end_ns,
    )
    if role == FINAL_DEVELOPMENT:
        calendar = tuple(day for day in calendar if int(day) < int(role_end_day))
    if role == FINAL_DEVELOPMENT and (
        any(int(day) >= role_end_day for day in calendar)
        or any(int(intent.session_day) >= role_end_day for intent in intents)
    ):
        raise TierQTwoStageError(
            f"partial May-01 session entered final-development labels: {binding.candidate_id}"
        )
    replay = exact_sleeve_replay(
        binding.calibrated, events, eligible_session_days=calendar
    )
    normal, normal_violations = exact._apply_session_contract(replay.normal_trajectories)
    stressed, stressed_violations = exact._apply_session_contract(replay.stressed_trajectories)
    if normal_violations or stressed_violations:
        raise TierQTwoStageError(f"session contract violation: {binding.candidate_id}")
    scaled_normal = tuple(
        scale_causal_trajectory(row, executable_quantity_multiplier=binding.integer_quantity_tier)
        for row in normal
    )
    scaled_stressed = tuple(
        scale_causal_trajectory(row, executable_quantity_multiplier=binding.integer_quantity_tier)
        for row in stressed
    )
    exact._require_scenario_identity(scaled_normal, scaled_stressed)
    account_limit = exact._market_contract_limit_mini(candidate.payload, rule)
    maximum_mini = max(
        (float(row.event.mini_equivalent) for row in scaled_normal), default=0.0
    )
    if maximum_mini > account_limit + 1e-12:
        raise TierQTwoStageError(f"frozen quantity exceeds account limit: {binding.candidate_id}")

    blocks = (
        [dict(row) for row in role_spec.get("subblocks") or ()]
        if role == FINAL_DEVELOPMENT
        else [{"block_id": "CONFIRMATION_2026", "start": role_spec["start"], "end": role_spec["end"]}]
    )
    proposed_starts = _non_overlapping_role_starts(
        calendar, blocks=blocks, horizon=binding.horizon_trading_days
    )
    if role == FINAL_DEVELOPMENT and len(proposed_starts) > EXPECTED_THEORETICAL_STARTS[
        binding.horizon_trading_days
    ]:
        raise TierQTwoStageError(
            f"final-development start denominator exceeds coverage: {binding.candidate_id}"
        )
    coverage = _selected_horizon_coverage(
        replay,
        calendar,
        proposed_starts,
        horizon=binding.horizon_trading_days,
    )
    starts = tuple(coverage["full"])
    episodes: dict[str, list[tuple[Any, str]]] = {"normal": [], "stressed": []}
    evidence: list[dict[str, Any]] = []
    for censored_start in coverage["data_censored_starts"]:
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            record = {
                "candidate_id": binding.candidate_id,
                "role": role,
                "scenario": scenario,
                "start_day": int(censored_start["start_day"]),
                "temporal_block": str(censored_start["temporal_block"]),
                "horizon_trading_days": binding.horizon_trading_days,
                "coverage_state": "DATA_CENSORED",
                "coverage_reasons": list(censored_start["reasons"]),
                "censored_session_days": list(
                    censored_start["censored_session_days"]
                ),
                "episode": None,
            }
            evidence.append({**record, "record_hash": stable_hash(record)})
    for start_day, block in starts:
        for scenario, trajectories in (
            ("normal", scaled_normal),
            ("stressed", scaled_stressed),
        ):
            episode = run_causal_shared_account_episode(
                {binding.candidate_id: trajectories},
                calendar,
                policy=binding.policy,
                start_day=start_day,
                maximum_duration_days=binding.horizon_trading_days,
                config=exact._account_config(rule),
            )
            episodes[scenario].append((episode, block))
            record = {
                "candidate_id": binding.candidate_id,
                "role": role,
                "scenario": "NORMAL" if scenario == "normal" else "STRESSED_1_5X",
                "start_day": start_day,
                "temporal_block": block,
                "horizon_trading_days": binding.horizon_trading_days,
                "coverage_state": "FULL_COVERAGE",
                "episode": episode.to_dict(include_paths=True),
            }
            evidence.append({**record, "record_hash": stable_hash(record)})
    summaries = {
        scenario: exact._summarize_exact_episodes(values)
        for scenario, values in episodes.items()
    }
    passing_stressed = [episode for episode, _ in episodes["stressed"] if episode.passed]
    summaries["stressed"]["passing_paths_consistency_compliant"] = bool(
        passing_stressed
    ) and all(episode.consistency_ok for episode in passing_stressed)
    concentration = (
        _unique_ledger_concentration(scaled_normal, scaled_stressed)
        if scaled_normal
        else {
            "denominator": "EMPTY_UNIQUE_CAUSAL_TRAJECTORY_LEDGER",
            "cleared": False,
            "worst_case_maximums": {},
            "concentration_hash": stable_hash([]),
        }
    )
    if role == FINAL_DEVELOPMENT:
        gate = tier_g_gate(
            summaries["normal"], summaries["stressed"], concentration,
            dict(contract["promotion_gates"])["tier_g_final_development"],
        )
    else:
        gate = tier_c_gate(
            summaries["normal"], summaries["stressed"], concentration,
            dict(contract["promotion_gates"])["tier_c_one_shot"],
            prior_tier_g=True,
            selected_horizon_matches=True,
            full_coverage_start_count=len(starts),
        )
    core = {
        "candidate_id": binding.candidate_id,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "calibration_hash": binding.calibrated.fingerprint,
        "calibration_reused_without_recalibration": True,
        "selected_cell_hash": binding.selected_cell_hash,
        "frozen_account_policy_hash": binding.account_policy_hash,
        "account_label": binding.account_label,
        "integer_quantity_tier": binding.integer_quantity_tier,
        "horizon_trading_days": binding.horizon_trading_days,
        "evaluated_horizons_trading_days": [binding.horizon_trading_days],
        "decision_market": candidate.market,
        "execution_market": candidate.execution_market,
        "decision_matrix_hash": decision_matrix.fingerprint,
        "execution_matrix_hash": execution_matrix.fingerprint,
        "source_execution_identity_receipt": execution_identity,
        "batch_stream_equal": True,
        "intent_count": len(intents),
        "observed_event_count": len(events),
        "censored_event_count": sum(
            str(getattr(row.outcome, "value", row.outcome))
            == "CENSORED_FUTURE_COVERAGE"
            for row in events
        ),
        "censor_reason_distribution": dict(
            sorted(
                Counter(
                    str(row.censor_reason)
                    for row in events
                    if str(getattr(row.outcome, "value", row.outcome))
                    == "CENSORED_FUTURE_COVERAGE"
                ).items()
            )
        ),
        "completed_trajectory_count": len(scaled_normal),
        "eligible_session_count": len(calendar),
        "partial_split_session_eligible": any(
            int(day) >= role_end_day for day in calendar
        ),
        "full_coverage_start_count": len(starts),
        "data_censored_start_count": len(coverage["data_censored_starts"]),
        "coverage_receipt": {
            key: value
            for key, value in coverage.items()
            if key != "full"
        },
        "starts": [
            *coverage["full_coverage_starts"],
            *coverage["data_censored_starts"],
        ],
        "normal": summaries["normal"],
        "stressed": summaries["stressed"],
        "concentration": concentration,
        "episode_evidence": evidence,
        "episode_evidence_hash": stable_hash(evidence),
        "promotion_gate": gate,
        "resulting_evidence_tier": gate["resulting_tier"],
        "retuning_performed": False,
        "recalibration_performed": False,
    }
    return {**core, "candidate_result_hash": stable_hash(core)}


def _authorized_confirmation_ids(
    contract: Mapping[str, Any],
    development_result: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    if development_result is None:
        raise TierQTwoStageError("confirmation remains sealed before final development")
    value = dict(development_result)
    claimed = str(value.pop("result_hash", ""))
    if (
        not claimed
        or stable_hash(value) != claimed
        or development_result.get("schema") != STAGE_SCHEMA
        or development_result.get("role") != FINAL_DEVELOPMENT
        or development_result.get("contract_hash") != contract["contract_hash"]
        or development_result.get("retuning_performed") is not False
        or development_result.get("recalibration_performed") is not False
    ):
        raise TierQTwoStageError("final-development authorization receipt drift")
    allowed = tuple(str(value) for value in development_result.get("tier_g_candidate_ids") or ())
    cohort = {str(row["candidate_id"]) for row in contract["candidate_cohort"]}
    results = {
        str(row["candidate_id"]): dict(row)
        for row in development_result.get("candidate_results") or ()
    }
    if (
        not allowed
        or len(allowed) != len(set(allowed))
        or not set(allowed).issubset(cohort)
        or any(
            candidate_id not in results
            or dict(results[candidate_id].get("promotion_gate") or {}).get("passed") is not True
            or results[candidate_id].get("resulting_evidence_tier") != "G"
            for candidate_id in allowed
        )
    ):
        raise TierQTwoStageError("no valid Tier-G candidate authorizes confirmation")
    return allowed


def _non_overlapping_role_starts(
    calendar: Sequence[int],
    *,
    blocks: Sequence[Mapping[str, Any]],
    horizon: int,
) -> tuple[tuple[int, str], ...]:
    if horizon < 1:
        raise TierQTwoStageError("frozen horizon must be positive")
    days = tuple(sorted({int(day) for day in calendar}))
    starts: list[tuple[int, str]] = []
    for raw in blocks:
        block = dict(raw)
        lower = _date_ns(str(block["start"])) // DAY_NS
        upper = _date_ns(str(block["end"])) // DAY_NS
        selected = tuple(day for day in days if lower <= day < upper)
        starts.extend(
            (selected[position], str(block["block_id"]))
            for position in range(0, len(selected) - horizon + 1, horizon)
        )
    return tuple(starts)


def _open_bound_matrix(project: Path, raw: Mapping[str, Any]) -> FeatureMatrix:
    row = dict(raw)
    manifest_path = _inside(project, str(row["path"]))
    if _sha256(manifest_path) != str(row["file_sha256"]):
        raise TierQTwoStageError("pre-2026 feature manifest SHA drift")
    matrix = FeatureMatrix.open(manifest_path.parent, mmap=True)
    if matrix.fingerprint != str(row["bundle_hash"]):
        raise TierQTwoStageError("pre-2026 feature bundle hash drift")
    return matrix


def _inside(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    resolved = (root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise TierQTwoStageError("frozen path escapes repository") from exc
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TierQTwoStageError(f"frozen JSON is absent or invalid: {path}") from exc
    if not isinstance(value, dict):
        raise TierQTwoStageError("frozen JSON payload must be an object")
    return value


def _date_ns(value: str) -> int:
    return int(pd.Timestamp(value, tz="UTC").value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "CONFIRMATION",
    "FINAL_DEVELOPMENT",
    "FrozenCandidateBinding",
    "TierQTwoStageError",
    "build_role_feature_bundles",
    "audit_final_development_coverage",
    "evaluate_stage",
    "load_frozen_bindings",
    "open_role_matrices",
    "tier_c_gate",
    "tier_g_gate",
    "verify_acquisition_receipt",
    "verify_contract",
]
