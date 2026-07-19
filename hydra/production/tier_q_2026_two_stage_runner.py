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
from dataclasses import dataclass
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
from hydra.research.turbo_feature_builder import FEATURE_DAG_HASH, _market_arrays
from hydra.validation.lockbox_guard import current_commit


CONTRACT_SCHEMA = "hydra_tier_q_2026_two_stage_contract_v1"
ACQUISITION_SCHEMA = "hydra_tier_q_2026_acquisition_receipt_v1"
FEATURE_SCHEMA = "hydra_tier_q_2026_role_features_v1"
STAGE_SCHEMA = "hydra_tier_q_2026_two_stage_economic_result_v1"
FINAL_DEVELOPMENT = "FINAL_DEVELOPMENT"
CONFIRMATION = "CONFIRMATION"
FEATURE_VERSION = "hydra_tier_q_2026_causal_features_v1"
DAY_NS = 86_400_000_000_000
EXPECTED_FINAL_DEVELOPMENT_ROWS = 1_136_722
EXPECTED_FINAL_DEVELOPMENT_TRADING_DAYS = 85
EXPECTED_THEORETICAL_STARTS = {5: 17, 10: 8, 20: 4}
PARTIAL_SPLIT_SESSION_DAY = "2026-05-01"
GOOD_FRIDAY_2026 = "2026-04-03"

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
    signal_markets = sorted(
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
    mapped = mapped.loc[mapped["symbol"].astype(str).isin(signal_markets)].copy()
    if set(mapped["symbol"].astype(str)) != set(signal_markets):
        raise TierQTwoStageError("contract guards removed a required signal market")
    featured = _prepare_feature_frame(_build_past_only_feature_frame(mapped))
    gap_audit = _audit_segment_locality(mapped, featured)
    source_hash = stable_hash(receipts)
    map_hash = _sha256(roll_path)
    store = CanonicalFeatureStore(cache_root)
    bundles: dict[str, Any] = {}
    role_spec = next(row for row in frozen["temporal_roles"] if row["role"] == role)
    source_start = str(frozen["temporal_roles"][0]["start"])
    source_end = str(role_spec["end"])
    for market in signal_markets:
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


def _evaluate_candidate_role(
    contract: Mapping[str, Any],
    *,
    role: str,
    binding: FrozenCandidateBinding,
    matrices: Mapping[str, FeatureMatrix],
    rule: Mapping[str, Any],
) -> dict[str, Any]:
    candidate = binding.candidate
    matrix = matrices[candidate.market]
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
    events = observe_outcomes(binding.calibrated, matrix, intents)
    calendar = frozen_eligible_session_calendar(
        candidate,
        matrix,
        evaluation_start_ns=start_ns,
        evaluation_end_exclusive_ns=end_ns,
    )
    role_end_day = end_ns // DAY_NS
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
    starts = _non_overlapping_role_starts(
        calendar, blocks=blocks, horizon=binding.horizon_trading_days
    )
    if role == FINAL_DEVELOPMENT and len(starts) > EXPECTED_THEORETICAL_STARTS[
        binding.horizon_trading_days
    ]:
        raise TierQTwoStageError(
            f"final-development start denominator exceeds coverage: {binding.candidate_id}"
        )
    episodes: dict[str, list[tuple[Any, str]]] = {"normal": [], "stressed": []}
    evidence: list[dict[str, Any]] = []
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
        "data_censored_start_count": 0,
        "starts": [{"start_day": day, "temporal_block": block} for day, block in starts],
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
