"""One-shot untouched-2022 replication of the frozen session-safe M2K+MYM book.

This module performs no acquisition and no authoritative write.  It consumes
only a self-hashed decision card written before the 2022 outcomes are opened.
The source is Tier E, so a successful replication can justify Tier-Q/Tier-G
progression but cannot manufacture Tier C.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.account_policy.active_pool_replay import ActiveRiskPoolPolicy
from hydra.account_policy.causal_active_pool_replay import run_causal_shared_account_episode
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
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.fast_pass_runtime_helpers import _quality_trajectories
from hydra.production.fresh_confirmation_lane import non_overlapping_starts
from hydra.research.causal_target_velocity import (
    CalibratedHazardCandidate,
    HazardCandidate,
    discover_intents_batch,
    discover_intents_streaming,
    exact_sleeve_replay,
    observe_outcomes,
    with_availability_safe_cross_asset_feature,
)
from hydra.research.qd_economic_tournament import _prepare_feature_frame
from hydra.research.turbo_feature_builder import FEATURE_DAG_HASH, _market_arrays


SCHEMA = "hydra_session_safe_m2k_mym_confirmation_v1"
FEATURE_SCHEMA = "hydra_session_safe_m2k_mym_confirmation_features_v1"
DEFAULT_CARD = Path(
    "config/research/session_safe_m2k_mym_confirmation_decision_card_v1.json"
)
REQUIRED_ROOTS = ("RTY", "M2K", "YM", "MYM", "ES")
SIGNAL_MARKETS = ("RTY", "YM", "ES")
HORIZONS = (5, 10, 20)
FEATURE_VERSION = "session_safe_m2k_mym_confirmation_causal_features_v1"


class SessionSafeConfirmationError(RuntimeError):
    """The frozen one-shot confirmation contract cannot be honored."""


def load_decision_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(card, dict):
        raise SessionSafeConfirmationError("decision card must be a JSON object")
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise SessionSafeConfirmationError("decision-card hash drift")
    return _validate_card_semantics(card)


def build_feature_bundles(
    card: Mapping[str, Any],
    *,
    source_files: Sequence[Mapping[str, Any]],
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Build causal RTY/YM/ES matrices from hash-bound acquired data."""

    frozen = load_decision_card_from_mapping(card)
    request = dict(frozen["data_request"])
    files = [dict(row) for row in source_files]
    if not files:
        raise SessionSafeConfirmationError("source parquet inventory is empty")
    roll_path = Path(contract_map_path).resolve()
    if not roll_path.is_file():
        raise SessionSafeConfirmationError("explicit contract map is absent")
    roll_map = load_roll_map(roll_path)
    if (
        roll_map.dataset != request["dataset"]
        or roll_map.schema != request["schema"]
        or not str(roll_map.map_type).startswith("EXPLICIT_DATABENTO_")
        or not set(REQUIRED_ROOTS).issubset(set(roll_map.symbols))
    ):
        raise SessionSafeConfirmationError("explicit roll map does not cover the freeze")
    symbol_map = {
        **{f"{root}.c.0": root for root in REQUIRED_ROOTS},
        **{str(row.contract): str(row.root) for row in roll_map.contracts},
    }
    frames: list[pd.DataFrame] = []
    receipts: list[dict[str, Any]] = []
    start = pd.Timestamp(request["request_start_inclusive"], tz="UTC")
    end = pd.Timestamp(request["end_exclusive"], tz="UTC")
    for raw in files:
        path = Path(str(raw.get("path") or "")).resolve()
        expected = str(raw.get("sha256") or "")
        if not path.is_file() or not expected or _sha256(path) != expected:
            raise SessionSafeConfirmationError("source parquet hash mismatch")
        frame = normalize_ohlcv_frame(
            pd.read_parquet(path), symbol=None, timeframe="1m", symbol_map=symbol_map
        )
        validate_ohlcv_frame(frame, timeframe="1m")
        times = pd.to_datetime(frame["timestamp"], utc=True)
        if times.min() < start or times.max() >= end:
            raise SessionSafeConfirmationError("source data escapes frozen half-open dates")
        frames.append(frame)
        receipts.append({"path": str(path), "sha256": expected, "rows": len(frame)})
    raw = pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"])
    if set(raw["symbol"].astype(str)) != set(REQUIRED_ROOTS):
        raise SessionSafeConfirmationError("source data lacks an exact requested root")
    if raw.duplicated(["symbol", "timestamp"]).any():
        raise SessionSafeConfirmationError("duplicate root/timestamp in confirmation data")
    mapped, map_receipt = _apply_explicit_contract_map(
        raw, roll_map, required_map_type=roll_map.map_type
    )
    featured = _prepare_feature_frame(_build_past_only_feature_frame(mapped))
    source_hash = stable_hash(receipts)
    map_sha = _sha256(roll_path)
    store = CanonicalFeatureStore(cache_root)
    bundles: dict[str, Any] = {}
    for market in SIGNAL_MARKETS:
        market_frame = featured.loc[featured["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = _market_arrays(market_frame, market)
        arrays.pop("entry_price", None)
        for key in tuple(arrays):
            if key.startswith("forward_move__"):
                arrays.pop(key)
        key = CanonicalFeatureKey(
            market=market,
            explicit_contract_scope=f"{market}_2022_untouched_confirmation",
            start_inclusive=request["request_start_inclusive"],
            end_exclusive=request["end_exclusive"],
            source_data_sha256=source_hash,
            roll_map_hash=map_sha,
            transformation_version=FEATURE_VERSION,
            feature_dag_hash=FEATURE_DAG_HASH,
            timeframes=("1m", "5m", "15m", "30m", "60m", "session", "daily"),
        )
        result = store.put(
            key,
            arrays,
            provenance={
                "data_role": "CONFIRMATION",
                "decision_card_hash": frozen["card_hash"],
                "market": market,
                "feature_names": list(metadata["feature_names"]),
                "future_outcome_arrays_in_decision_bundle": False,
                "entry_fill_contract": "SIGNAL_AFTER_COMPLETED_T_THEN_NEXT_TRADABLE_BAR_OPEN",
            },
        )
        bundles[market] = {
            "path": str(result.path),
            "bundle_hash": result.bundle_hash,
            "row_count": result.row_count,
            "cache_hit": result.cache_hit,
        }
    core = {
        "schema": FEATURE_SCHEMA,
        "status": "CAUSAL_FEATURE_BUNDLES_READY",
        "decision_card_hash": frozen["card_hash"],
        "source_files": receipts,
        "contract_map": {
            "path": str(roll_path),
            "sha256": map_sha,
            "roll_map_hash": roll_map.roll_map_hash(),
            "guard_receipt": map_receipt,
        },
        "bundles": bundles,
        "future_outcomes_in_decision_bundle": False,
    }
    return {**core, "result_hash": stable_hash(core)}


def open_feature_matrices(receipt: Mapping[str, Any]) -> dict[str, FeatureMatrix]:
    value = dict(receipt)
    claimed = str(value.pop("result_hash", ""))
    if not claimed or stable_hash(value) != claimed:
        raise SessionSafeConfirmationError("feature receipt hash drift")
    bundles = dict(receipt["bundles"])
    if set(bundles) != set(SIGNAL_MARKETS):
        raise SessionSafeConfirmationError("feature matrix inventory drift")
    output: dict[str, FeatureMatrix] = {}
    for market, raw in bundles.items():
        binding = dict(raw)
        matrix = FeatureMatrix.open(binding["path"], mmap=True)
        if matrix.fingerprint != str(binding["bundle_hash"]):
            raise SessionSafeConfirmationError("feature bundle fingerprint drift")
        output[market] = matrix
    return output


def evaluate_confirmation(
    card: Mapping[str, Any],
    *,
    matrices: Mapping[str, FeatureMatrix],
    acquisition_receipt: Mapping[str, Any],
    existing_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Consume the frozen 2022 evidence exactly once without recalibration."""

    frozen = load_decision_card_from_mapping(card)
    if existing_result is not None:
        raise SessionSafeConfirmationError("one-shot confirmation already consumed")
    _verify_acquisition_receipt(frozen, acquisition_receipt)
    if set(matrices) != set(SIGNAL_MARKETS):
        raise SessionSafeConfirmationError("confirmation matrix inventory drift")
    request = dict(frozen["data_request"])
    for market, matrix in matrices.items():
        key = dict(matrix.manifest.get("key") or {})
        provenance = dict(matrix.manifest.get("provenance") or {})
        if (
            key.get("market") != market
            or key.get("start_inclusive") != request["request_start_inclusive"]
            or key.get("end_exclusive") != request["end_exclusive"]
            or provenance.get("data_role") != "CONFIRMATION"
        ):
            raise SessionSafeConfirmationError("confirmation matrix role/date drift")

    calendar = _common_calendar(matrices, request)
    starts = non_overlapping_starts(calendar, HORIZONS)
    evaluation_start_ns = _date_ns(request["confirmation_start_inclusive"])
    evaluation_end_ns = _date_ns(request["end_exclusive"])
    trajectories: dict[str, dict[str, tuple[Any, ...]]] = {
        "NORMAL": {},
        "STRESSED_1_5X": {},
    }
    component_receipts: dict[str, Any] = {}
    for component in frozen["components"]:
        row = dict(component)
        candidate = HazardCandidate(**dict(row["candidate"]))
        calibrated = _calibrated(candidate, row["calibration"])
        matrix = matrices[candidate.market]
        if candidate.cross_asset_reference_market:
            matrix = with_availability_safe_cross_asset_feature(
                matrix, matrices[str(candidate.cross_asset_reference_market)]
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
        if tuple((x.row_index, x.direction) for x in intents) != streaming:
            raise SessionSafeConfirmationError("batch/stream decision mismatch")
        replay = exact_sleeve_replay(
            calibrated,
            observe_outcomes(calibrated, matrix, intents),
            eligible_session_days=calendar,
        )
        normal, normal_quality = _quality_trajectories(
            replay.normal_trajectories, float(row["quality_tier"])
        )
        stressed, stressed_quality = _quality_trajectories(
            replay.stressed_trajectories, float(row["quality_tier"])
        )
        if normal_quality != stressed_quality:
            raise SessionSafeConfirmationError("normal/stressed quality decision drift")
        normal, normal_violations = exact._apply_session_contract(normal)
        stressed, stressed_violations = exact._apply_session_contract(stressed)
        if normal_violations or stressed_violations:
            raise SessionSafeConfirmationError("session-safe book produced a violation")
        tier = int(frozen["frozen_account"]["integer_quantity_tier"])
        trajectories["NORMAL"][candidate.candidate_id] = tuple(
            scale_causal_trajectory(x, executable_quantity_multiplier=tier) for x in normal
        )
        trajectories["STRESSED_1_5X"][candidate.candidate_id] = tuple(
            scale_causal_trajectory(x, executable_quantity_multiplier=tier) for x in stressed
        )
        component_receipts[candidate.candidate_id] = {
            "candidate_fingerprint": candidate.structural_fingerprint,
            "calibration_hash": calibrated.fingerprint,
            "recalibrated": False,
            "batch_stream_equal": True,
            "intent_count": len(intents),
            "normal_trajectory_count": len(normal),
            "stressed_trajectory_count": len(stressed),
            "quality_scaling": normal_quality,
        }
    policy = ActiveRiskPoolPolicy.from_mapping(frozen["frozen_account"]["policy"])
    rule_path = Path(frozen["frozen_account"]["official_rule_snapshot_path"])
    if _sha256(rule_path) != frozen["frozen_account"]["official_rule_snapshot_sha256"]:
        raise SessionSafeConfirmationError("official rule snapshot hash drift")
    rules, rule_receipt = exact._load_rule_snapshot(rule_path)
    rule = dict(rules["50K"])
    results: dict[str, Any] = {}
    evidence: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        by_scenario: dict[str, list[tuple[Any, str]]] = {
            "NORMAL": [],
            "STRESSED_1_5X": [],
        }
        for start_day, block in starts[horizon]:
            for scenario in by_scenario:
                episode = run_causal_shared_account_episode(
                    trajectories[scenario],
                    calendar,
                    policy=policy,
                    start_day=int(start_day),
                    maximum_duration_days=int(horizon),
                    config=exact._account_config(rule),
                )
                by_scenario[scenario].append((episode, str(block)))
                record = {
                    "policy_id": policy.policy_id,
                    "scenario": scenario,
                    "horizon_trading_days": int(horizon),
                    "start_day": int(start_day),
                    "temporal_block": "CONFIRMATION_2022",
                    "coverage_state": "FULL_COVERAGE",
                    "episode": episode.to_dict(include_paths=True),
                }
                record["record_hash"] = stable_hash(record)
                evidence.append(record)
        results[str(horizon)] = {
            "horizon_trading_days": horizon,
            "full_coverage_start_count": len(starts[horizon]),
            "normal": exact._summarize_exact_episodes(by_scenario["NORMAL"]),
            "stressed": exact._summarize_exact_episodes(
                by_scenario["STRESSED_1_5X"]
            ),
        }
    gate = confirmation_gate(results["20"], frozen["gate"])
    core = {
        "schema": SCHEMA,
        "status": "CONFIRMATION_CONSUMED_ONCE",
        "decision_card_hash": frozen["card_hash"],
        "acquisition_receipt_hash": acquisition_receipt["receipt_hash"],
        "account_label": "50K",
        "policy_id": policy.policy_id,
        "source_evidence_tier": "E",
        "component_results": component_receipts,
        "calendar": {
            "session_count": len(calendar),
            "first_session_day": int(calendar[0]),
            "last_session_day": int(calendar[-1]),
            "non_overlapping_start_counts": {
                str(h): len(starts[h]) for h in HORIZONS
            },
        },
        "horizon_results": results,
        "confirmation_gate": gate,
        "resulting_evidence_status": (
            "FRESH_REPLICATION_SUCCESS_TIER_Q_ELIGIBLE_NOT_TIER_C"
            if gate["passed"]
            else "FRESH_REPLICATION_FAILED_BRANCH_CLOSED"
        ),
        "evidence_bundle_adapter": {
            "evaluated_policy_records": evidence,
            "records_hash": stable_hash(evidence),
            "sealing_performed": False,
        },
        "official_rule_snapshot": rule_receipt,
        "retuning_performed": False,
        "recalibration_performed": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def confirmation_gate(cell: Mapping[str, Any], gate: Mapping[str, Any]) -> dict[str, Any]:
    normal = dict(cell["normal"])
    stressed = dict(cell["stressed"])
    passing = int(stressed.get("pass_count", 0)) > 0
    checks = {
        "normal_pass": int(normal.get("pass_count", 0))
        >= int(gate["minimum_normal_passes"]),
        "stressed_pass": int(stressed.get("pass_count", 0))
        >= int(gate["minimum_stressed_passes"]),
        "positive_stressed_net": float(stressed.get("net_total_usd", 0.0)) > 0.0,
        "controlled_stressed_mll": float(stressed.get("mll_breach_rate", 1.0))
        <= float(gate["maximum_stressed_mll_breach_rate"]),
        "stressed_passing_consistency": passing
        and float(stressed.get("consistency_compliance_rate", 0.0)) >= 1.0,
        "full_coverage": int(cell.get("full_coverage_start_count", 0)) > 0,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "evidence_ceiling": gate["evidence_ceiling_on_pass"],
    }


def load_decision_card_from_mapping(card: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(card)
    claimed = str(value.pop("card_hash", ""))
    if not claimed or stable_hash(value) != claimed:
        raise SessionSafeConfirmationError("decision-card hash drift")
    return _validate_card_semantics(card)


def _validate_card_semantics(card: Mapping[str, Any]) -> dict[str, Any]:
    if (
        card.get("status") != "FROZEN_AWAITING_ACQUISITION"
        or card.get("evidence_role")
        != "ONE_SHOT_UNTOUCHED_CONFIRMATION_REPLICATION"
        or card.get("source", {}).get("source_evidence_tier") != "E"
        or card.get("evaluation", {}).get("parameters_mutable") is not False
        or card.get("evaluation", {}).get("recalibration_allowed") is not False
        or card.get("data_request", {}).get("q4_access_allowed") is not False
        or card.get("untouched_audit", {}).get("period_is_unviewed") is not True
    ):
        raise SessionSafeConfirmationError("decision-card freeze contract drift")
    _verify_frozen_components(card)
    policy = ActiveRiskPoolPolicy.from_mapping(card["frozen_account"]["policy"])
    if stable_hash(policy.to_dict()) != str(card["frozen_account"]["policy_hash"]):
        raise SessionSafeConfirmationError("frozen account policy drift")
    return dict(card)


def _verify_frozen_components(card: Mapping[str, Any]) -> None:
    components = [dict(x) for x in card.get("components", ())]
    if len(components) != 2:
        raise SessionSafeConfirmationError("frozen component inventory drift")
    expected = {
        "hazard_1478619b60a10a3b9bccef4f": ("RTY", "M2K", 1.0),
        "hazard_19327ab34a21d623c654a6cc": ("YM", "MYM", 0.5),
    }
    for row in components:
        candidate = HazardCandidate(**dict(row["candidate"]))
        wanted = expected.get(candidate.candidate_id)
        if (
            wanted is None
            or candidate.structural_fingerprint != row["candidate_fingerprint"]
            or (candidate.market, candidate.execution_market, float(row["quality_tier"]))
            != wanted
            or _calibrated(candidate, row["calibration"]).fingerprint
            != row["calibration"]["calibration_hash"]
        ):
            raise SessionSafeConfirmationError("frozen component semantic drift")


def _calibrated(
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
        raise SessionSafeConfirmationError("frozen calibration hash drift")
    return calibrated


def _common_calendar(
    matrices: Mapping[str, FeatureMatrix], request: Mapping[str, Any]
) -> tuple[int, ...]:
    start = _date_ns(str(request["confirmation_start_inclusive"]))
    end = _date_ns(str(request["end_exclusive"]))
    common: set[int] | None = None
    for matrix in matrices.values():
        timestamp = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
        days = np.asarray(matrix.array("session_day"), dtype=np.int64)
        current = {int(x) for x in days[(timestamp >= start) & (timestamp < end)]}
        common = current if common is None else common & current
    calendar = tuple(sorted(common or ()))
    if len(calendar) < 40:
        raise SessionSafeConfirmationError("confirmation lacks two complete 20-day starts")
    return calendar


def _verify_acquisition_receipt(
    card: Mapping[str, Any], receipt: Mapping[str, Any]
) -> None:
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise SessionSafeConfirmationError("acquisition receipt hash drift")
    request = dict(card["data_request"])
    actual = dict(receipt.get("request") or {})
    for key in ("dataset", "schema", "symbols", "stype_in"):
        if actual.get(key) != request.get(key):
            raise SessionSafeConfirmationError("acquisition request drift")
    if (
        actual.get("start") != request["request_start_inclusive"]
        or actual.get("end") != request["end_exclusive"]
        or receipt.get("data_role") != "CONFIRMATION"
        or float(receipt.get("actual_cost_usd", -1))
        > float(card["official_cost_estimate"]["maximum_authorized_by_this_card_usd"])
        or receipt.get("q4_access_count_delta") != 0
    ):
        raise SessionSafeConfirmationError("acquisition role/cost/date drift")


def _date_ns(value: str) -> int:
    return int(pd.Timestamp(value, tz="UTC").value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "SessionSafeConfirmationError",
    "build_feature_bundles",
    "confirmation_gate",
    "evaluate_confirmation",
    "load_decision_card",
    "open_feature_matrices",
]
