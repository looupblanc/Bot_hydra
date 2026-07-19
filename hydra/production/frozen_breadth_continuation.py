"""Frozen Q3-2025 continuation for the single useful breadth qualifier.

This module deliberately evaluates exactly one previously frozen development
diagnostic.  It does not search, recalibrate, mutate thresholds, write mission
state, or claim independent confirmation.  Q3-2025 is an untouched
final-development block; therefore a successful result is capped at Tier G.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

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
from hydra.production.autonomous_exact_replay import (
    HORIZONS,
    _account_config,
    _apply_session_contract,
    _candidate_coverage,
    _declared_stop_risk_charge_per_mini,
    _exact_cell,
    _load_rule_snapshot,
    _market_contract_limit_mini,
    _require_scenario_identity,
    _standalone_policy,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.cross_index_breadth_tripwire import (
    BreadthSpecification,
    build_cross_index_breadth_view,
    _primary_candidate as _breadth_candidate,
)
from hydra.production.fresh_confirmation_lane import non_overlapping_starts
from hydra.research.causal_target_velocity import (
    CalibratedHazardCandidate,
    HazardCandidate,
    discover_intents_batch,
    discover_intents_streaming,
    exact_sleeve_replay,
    frozen_eligible_session_calendar,
    observe_outcomes,
)
from hydra.research.qd_economic_tournament import _prepare_feature_frame
from hydra.research.turbo_feature_builder import FEATURE_DAG_HASH, _market_arrays


SCHEMA = "hydra_frozen_breadth_q3_continuation_v1"
CONTRACT_SCHEMA = "hydra_frozen_breadth_q3_contract_v1"
FEATURE_SCHEMA = "hydra_frozen_breadth_q3_features_v1"
DATASET = "GLBX.MDP3"
DATA_SCHEMA = "ohlcv-1m"
SYMBOLS = ("YM.c.0", "MYM.c.0", "ES.c.0", "NQ.c.0", "RTY.c.0")
ROOTS = ("YM", "MYM", "ES", "NQ", "RTY")
SIGNAL_MARKETS = ("YM", "ES", "NQ", "RTY")
START = "2025-07-01"
END = "2025-10-01"
DATA_ROLE = "FINAL_DEVELOPMENT"
BLOCK = "FINAL_DEVELOPMENT_Q3_2025"
WARMUP_COMPLETE_SESSIONS = 5
SPECIFICATION_ID = "breadth:YM:OPEN:BREADTH_CONFIRMED_CONTINUATION"
SPECIFICATION_FINGERPRINT = (
    "876cc7340587fdaae650fed0cad69d52c5a90098726b472ae1224ae456e1629d"
)
CANDIDATE_FINGERPRINT = (
    "4561b186a9dad19051f1e83a7150b866fadc830f82110f1bb1576493f274b774"
)
CALIBRATION_HASH = (
    "843a4ac1960ec7ae0c21bfe4ac609be32f3308a8ab6622362914aee9ec4c84e2"
)
DEVELOPMENT_BEHAVIOR_HASH = (
    "e2d14327561baf0779707566bc600130f3ac4c0ad3710d6d7262a8d19da6b37a"
)
TRIPWIRE_RESULT_HASH = (
    "8f23bd4b1d27fdb77c9332a2ce77cab8e72caee056b2b3026d0b755c67ac364f"
)
OFFICIAL_ESTIMATED_OHLCV_USD = 1.586130782962
OFFICIAL_ESTIMATED_DEFINITION_USD = 0.000431466848
OFFICIAL_ESTIMATED_TOTAL_USD = 1.586562249810
ACCOUNT_SIZE_TIERS = {"50K": 3, "100K": 6, "150K": 9}
DEFAULT_RULE_SNAPSHOT = Path("config/rulesets/topstep_official_2026-07-19.json")
DEFAULT_TRIPWIRE = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/"
    "cross_index_breadth_tripwire.json"
)
DEFAULT_PRIOR_CONTRACT = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/"
    "fresh_confirmation_contract.json"
)


class FrozenBreadthContinuationError(RuntimeError):
    """The singleton continuation cannot be evaluated without semantic drift."""


def frozen_data_request() -> dict[str, Any]:
    core = {
        "dataset": DATASET,
        "schema": DATA_SCHEMA,
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "data_role": DATA_ROLE,
        "q4_access_allowed": False,
        "broker_or_order_capability": False,
    }
    return {
        **core,
        "request_hash": stable_hash(core),
        "official_estimated_cost_usd": {
            "ohlcv": OFFICIAL_ESTIMATED_OHLCV_USD,
            "definition": OFFICIAL_ESTIMATED_DEFINITION_USD,
            "total": OFFICIAL_ESTIMATED_TOTAL_USD,
        },
    }


def freeze_breadth_continuation_contract(
    root: str | Path,
    *,
    tripwire_path: str | Path = DEFAULT_TRIPWIRE,
    prior_contract_path: str | Path = DEFAULT_PRIOR_CONTRACT,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Bind the exact development qualifier before opening Q3-2025."""

    project = Path(root).resolve()
    tripwire_envelope = _read_json(_inside(project, tripwire_path))
    tripwire = dict(tripwire_envelope.get("cross_index_breadth_tripwire") or {})
    if str(tripwire.get("result_hash")) != TRIPWIRE_RESULT_HASH:
        raise FrozenBreadthContinuationError("source tripwire result hash drift")
    selected = next(
        (
            dict(row)
            for row in tripwire.get("results", ())
            if str(dict(row).get("specification", {}).get("specification_id"))
            == SPECIFICATION_ID
        ),
        None,
    )
    if selected is None:
        raise FrozenBreadthContinuationError("frozen breadth qualifier is absent")
    primary = dict(selected["primary"])
    if (
        str(selected.get("specification_fingerprint")) != SPECIFICATION_FINGERPRINT
        or str(primary.get("candidate_fingerprint")) != CANDIDATE_FINGERPRINT
        or str(primary.get("realized_behavior_hash")) != DEVELOPMENT_BEHAVIOR_HASH
        or str(dict(primary.get("calibration") or {}).get("calibration_hash"))
        != CALIBRATION_HASH
    ):
        raise FrozenBreadthContinuationError("frozen breadth identity drift")
    h10 = next(
        (
            dict(row)
            for row in primary.get("cells", ())
            if int(dict(row).get("horizon_trading_days", 0)) == 10
        ),
        None,
    )
    if h10 is None:
        raise FrozenBreadthContinuationError("source 10-day evidence is absent")
    normal = dict(h10["normal"])
    stressed = dict(h10["stressed"])
    shared_pass_blocks = sorted(
        set(normal.get("blocks_with_passes") or ())
        & set(stressed.get("blocks_with_passes") or ())
    )
    if shared_pass_blocks != ["B3", "B4"]:
        raise FrozenBreadthContinuationError("source block-diverse pass evidence drift")

    prior = _read_json(_inside(project, prior_contract_path))
    prior_core = dict(prior)
    claimed = str(prior_core.pop("contract_hash", ""))
    if not claimed or stable_hash(prior_core) != claimed:
        raise FrozenBreadthContinuationError("prior frozen contract hash drift")
    diagnostic = dict(prior.get("breadth_diagnostic") or {})
    if (
        str(diagnostic.get("specification_id")) != SPECIFICATION_ID
        or str(diagnostic.get("candidate_fingerprint")) != CANDIDATE_FINGERPRINT
        or str(dict(diagnostic.get("calibration") or {}).get("calibration_hash"))
        != CALIBRATION_HASH
    ):
        raise FrozenBreadthContinuationError("prior breadth freeze drift")

    rules, rule_receipt = _load_rule_snapshot(_inside(project, rule_snapshot_path))
    if set(ACCOUNT_SIZE_TIERS) - set(rules):
        raise FrozenBreadthContinuationError("official account-size rules incomplete")
    core = {
        "schema": CONTRACT_SCHEMA,
        "status": "FROZEN_AWAITING_UNTOUCHED_Q3_ACQUISITION",
        "specification_id": SPECIFICATION_ID,
        "specification_fingerprint": SPECIFICATION_FINGERPRINT,
        "candidate": dict(diagnostic["candidate"]),
        "candidate_fingerprint": CANDIDATE_FINGERPRINT,
        "development_behavior_hash": DEVELOPMENT_BEHAVIOR_HASH,
        "calibration": dict(diagnostic["calibration"]),
        "source_development": {
            "tripwire_result_hash": TRIPWIRE_RESULT_HASH,
            "shared_normal_stressed_pass_blocks": shared_pass_blocks,
            "horizon_trading_days": 10,
            "normal_pass_count": int(normal["pass_count"]),
            "stressed_pass_count": int(stressed["pass_count"]),
            "stressed_net_total_usd": float(stressed["net_total_usd"]),
            "stressed_mll_breach_rate": float(stressed["mll_breach_rate"]),
        },
        "data_request": frozen_data_request(),
        "data_partition": {
            "role": DATA_ROLE,
            "untouched_when_frozen": True,
            "period": f"{START}:{END}",
            "warmup_complete_sessions": WARMUP_COMPLETE_SESSIONS,
            "candidate_modification_allowed": False,
            "recalibration_allowed": False,
            "consumed_once": True,
        },
        "account_size_matrix": [
            {"account_label": label, "integer_quantity_tier": tier}
            for label, tier in ACCOUNT_SIZE_TIERS.items()
        ],
        "horizons_trading_days": list(HORIZONS),
        "tier_g_gate": {
            "matching_horizon_trading_days": 10,
            "minimum_normal_passes": 1,
            "minimum_stressed_passes": 1,
            "positive_stressed_net_required": True,
            "maximum_stressed_mll_breach_rate": 0.10,
            "all_stressed_passing_paths_consistency_compliant": True,
            "source_passes_in_at_least_two_blocks": True,
            "no_retuning": True,
        },
        "evidence_ceiling": "TIER_G_DEVELOPMENT",
        "tier_c_permitted": False,
        "failure_action": "TOMBSTONE_EXACT_BREADTH_SPECIFICATION",
        "official_rule_snapshot": rule_receipt,
        "official_rule_snapshot_payload": {
            label: dict(rules[label]) for label in ACCOUNT_SIZE_TIERS
        },
        "prior_contract_hash": claimed,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "contract_hash": stable_hash(core)}


def validate_acquisition_receipt(
    contract: Mapping[str, Any], receipt: Mapping[str, Any]
) -> dict[str, Any]:
    frozen = _verify_contract(contract)
    request = dict(frozen["data_request"])
    actual_request = dict(receipt.get("request") or {})
    for key in ("dataset", "schema", "symbols", "stype_in", "start", "end"):
        if actual_request.get(key) != request.get(key):
            raise FrozenBreadthContinuationError("acquisition request differs from freeze")
    if str(receipt.get("data_role")) != DATA_ROLE:
        raise FrozenBreadthContinuationError("acquisition evidence role drift")
    actual = float(receipt.get("actual_cost_usd", -1.0))
    if actual < 0.0:
        raise FrozenBreadthContinuationError("acquisition cost absent")
    files = [dict(row) for row in receipt.get("files", ())]
    if not files or any(not str(row.get("sha256") or "") for row in files):
        raise FrozenBreadthContinuationError("acquisition inventory incomplete")
    return {
        "status": "BREADTH_Q3_ACQUISITION_RECONCILED",
        "actual_cost_usd": actual,
        "file_count": len(files),
        "file_inventory_hash": stable_hash(files),
        "request_hash": str(request["request_hash"]),
    }


def build_breadth_feature_bundles(
    contract: Mapping[str, Any],
    *,
    source_files: Sequence[Mapping[str, Any]],
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Build causal Q3 matrices once; future labels are physically excluded."""

    frozen = _verify_contract(contract)
    files = [dict(row) for row in source_files]
    if not files:
        raise FrozenBreadthContinuationError("source parquet inventory is empty")
    roll_map_file = Path(contract_map_path).resolve()
    if not roll_map_file.is_file():
        raise FrozenBreadthContinuationError("explicit Q3 roll map absent")
    roll_map = load_roll_map(roll_map_file)
    if (
        roll_map.dataset != DATASET
        or roll_map.schema != DATA_SCHEMA
        or not str(roll_map.map_type).startswith("EXPLICIT_DATABENTO_")
        or not set(ROOTS).issubset(set(roll_map.symbols))
    ):
        raise FrozenBreadthContinuationError("explicit Q3 roll map identity drift")
    symbol_map = {
        **{f"{root}.c.0": root for root in ROOTS},
        **{str(row.contract): str(row.root) for row in roll_map.contracts},
    }
    frames: list[pd.DataFrame] = []
    source_receipts: list[dict[str, Any]] = []
    for raw in files:
        path = Path(str(raw.get("path") or "")).resolve()
        expected = str(raw.get("sha256") or "")
        if not path.is_file() or not expected or _sha256(path) != expected:
            raise FrozenBreadthContinuationError("Q3 source parquet hash mismatch")
        frame = normalize_ohlcv_frame(
            pd.read_parquet(path), symbol=None, timeframe="1m", symbol_map=symbol_map
        )
        validate_ohlcv_frame(frame, timeframe="1m")
        timestamps = pd.to_datetime(frame["timestamp"], utc=True)
        if timestamps.min() < pd.Timestamp(START, tz="UTC") or timestamps.max() >= pd.Timestamp(END, tz="UTC"):
            raise FrozenBreadthContinuationError("Q3 source escapes half-open dates")
        frames.append(frame)
        source_receipts.append(
            {"path": str(path), "sha256": expected, "rows": int(len(frame))}
        )
    raw = pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"])
    if set(raw["symbol"].astype(str)) != set(ROOTS):
        raise FrozenBreadthContinuationError("Q3 roots differ from frozen request")
    if raw.duplicated(["symbol", "timestamp"]).any():
        raise FrozenBreadthContinuationError("duplicate Q3 symbol timestamps")
    mapped, map_receipt = _apply_explicit_contract_map(
        raw, roll_map, required_map_type=roll_map.map_type
    )
    featured = _prepare_feature_frame(_build_past_only_feature_frame(mapped))
    source_hash = stable_hash(source_receipts)
    map_hash = _sha256(roll_map_file)
    store = CanonicalFeatureStore(cache_root)
    bundles: dict[str, Any] = {}
    for market in SIGNAL_MARKETS:
        market_frame = featured.loc[featured["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = _market_arrays(market_frame, market)
        arrays.pop("entry_price", None)
        for name in tuple(arrays):
            if name.startswith("forward_move__"):
                arrays.pop(name)
        key = CanonicalFeatureKey(
            market=market,
            explicit_contract_scope=f"{market}_date_aware_q3_breadth_final_development",
            start_inclusive=START,
            end_exclusive=END,
            source_data_sha256=source_hash,
            roll_map_hash=map_hash,
            transformation_version=FEATURE_SCHEMA,
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
                "market": market,
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
        "schema": FEATURE_SCHEMA,
        "status": "BREADTH_Q3_FEATURE_BUNDLES_READY",
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


def open_breadth_matrices(receipt: Mapping[str, Any]) -> dict[str, FeatureMatrix]:
    row = dict(receipt)
    expected = str(row.pop("result_hash", ""))
    if not expected or stable_hash(row) != expected:
        raise FrozenBreadthContinuationError("feature receipt hash drift")
    bindings = dict(row.get("bundles") or {})
    if set(bindings) != set(SIGNAL_MARKETS):
        raise FrozenBreadthContinuationError("breadth feature inventory drift")
    output: dict[str, FeatureMatrix] = {}
    for market, raw in bindings.items():
        binding = dict(raw)
        matrix = FeatureMatrix.open(binding["path"], mmap=True)
        if matrix.fingerprint != str(binding["bundle_hash"]):
            raise FrozenBreadthContinuationError("breadth matrix hash drift")
        output[market] = matrix
    return output


def evaluate_breadth_continuation(
    contract: Mapping[str, Any],
    *,
    matrices: Mapping[str, FeatureMatrix],
    acquisition_receipt: Mapping[str, Any],
    existing_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate the singleton candidate once and cap any success at Tier G."""

    frozen = _verify_contract(contract)
    if existing_result is not None:
        raise FrozenBreadthContinuationError("Q3 final-development block consumed")
    acquisition = validate_acquisition_receipt(frozen, acquisition_receipt)
    if set(matrices) != set(SIGNAL_MARKETS):
        raise FrozenBreadthContinuationError("evaluation requires YM/ES/NQ/RTY")
    for market, matrix in matrices.items():
        key = dict(matrix.manifest.get("key") or {})
        provenance = dict(matrix.manifest.get("provenance") or {})
        if (
            str(key.get("market")) != market
            or str(key.get("start_inclusive")) != START
            or str(key.get("end_exclusive")) != END
            or str(provenance.get("data_role")) != DATA_ROLE
        ):
            raise FrozenBreadthContinuationError("Q3 matrix role/identity drift")

    breadth = build_cross_index_breadth_view(
        matrices["YM"], {market: matrices[market] for market in ("ES", "NQ", "RTY")}
    )
    candidate = HazardCandidate(**dict(frozen["candidate"]))
    if candidate.structural_fingerprint != CANDIDATE_FINGERPRINT:
        raise FrozenBreadthContinuationError("candidate changed after freeze")
    calibrated = _calibrated_from_payload(candidate, frozen["calibration"])
    calendar = _post_warmup_common_calendar(matrices)
    starts = non_overlapping_starts(calendar, HORIZONS)
    first_ns = int(calendar[0]) * 86_400_000_000_000
    end_ns = (int(calendar[-1]) + 1) * 86_400_000_000_000
    intents = discover_intents_batch(
        calibrated, breadth, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns
    )
    streaming = discover_intents_streaming(
        calibrated, breadth, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns
    )
    if tuple((row.row_index, row.direction) for row in intents) != streaming:
        raise FrozenBreadthContinuationError("batch/stream breadth decision drift")
    events = observe_outcomes(calibrated, breadth, intents)
    eligible_days = frozen_eligible_session_calendar(
        candidate, breadth, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns
    )
    replay = exact_sleeve_replay(calibrated, events, eligible_session_days=eligible_days)
    coverage = _candidate_coverage(replay, calendar, starts)
    normal, normal_violations = _apply_session_contract(replay.normal_trajectories)
    stressed, stressed_violations = _apply_session_contract(replay.stressed_trajectories)
    if normal_violations or stressed_violations:
        raise FrozenBreadthContinuationError("session contract violation")
    _require_scenario_identity(normal, stressed)
    event_payloads = [row.to_dict() for row in events]
    if not normal or not event_payloads:
        return _terminal_result(
            frozen,
            acquisition=acquisition,
            calendar=calendar,
            starts=starts,
            intents=len(intents),
            events=len(normal),
            account_results=[],
        )
    declared_charge = _declared_stop_risk_charge_per_mini(
        event_payloads, candidate.payload
    )
    rules = dict(frozen["official_rule_snapshot_payload"])
    account_results: list[dict[str, Any]] = []
    for account_label, tier in ACCOUNT_SIZE_TIERS.items():
        rule = dict(rules[account_label])
        scaled_normal = tuple(
            scale_causal_trajectory(row, executable_quantity_multiplier=tier)
            for row in normal
        )
        scaled_stressed = tuple(
            scale_causal_trajectory(row, executable_quantity_multiplier=tier)
            for row in stressed
        )
        _require_scenario_identity(scaled_normal, scaled_stressed)
        maximum_mini = max(
            (float(row.event.mini_equivalent) for row in scaled_normal), default=0.0
        )
        account_limit = _market_contract_limit_mini(candidate.payload, rule)
        if maximum_mini > account_limit + 1e-12:
            raise FrozenBreadthContinuationError("frozen tier exceeds account limit")
        policy = _standalone_policy(
            candidate.candidate_id,
            rule,
            tier=tier,
            declared_risk_charge_per_mini=declared_charge,
            account_contract_limit=account_limit,
            governor_mode="FROZEN_BREADTH_Q3_STATIC_RISK",
        )
        cells: list[dict[str, Any]] = []
        for horizon in HORIZONS:
            episode_sets: dict[str, list[tuple[Any, str]]] = {
                "NORMAL": [],
                "STRESSED_1_5X": [],
            }
            for start_day, _ in coverage["starts"][horizon]:
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
                    episode_sets[scenario].append((episode, BLOCK))
            cell = _exact_cell(
                candidate.candidate_id,
                account_label,
                rule,
                tier=tier,
                horizon=int(horizon),
                maximum_mini=maximum_mini,
                account_contract_limit=account_limit,
                declared_risk_charge=declared_charge,
                governor_mode="FROZEN_BREADTH_Q3_STATIC_RISK",
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
        gate = decide_tier_g_gate(
            cells,
            source_pass_blocks=dict(frozen["source_development"])[
                "shared_normal_stressed_pass_blocks"
            ],
        )
        account_results.append(
            {
                "account_label": account_label,
                "integer_quantity_tier": tier,
                "maximum_mini_equivalent": maximum_mini,
                "account_contract_limit_mini": account_limit,
                "cells": cells,
                "tier_g_gate": gate,
                "tier_g_graduated": bool(gate["passed"]),
            }
        )
    return _terminal_result(
        frozen,
        acquisition=acquisition,
        calendar=calendar,
        starts=starts,
        intents=len(intents),
        events=len(normal),
        account_results=account_results,
    )


def decide_tier_g_gate(
    cells: Sequence[Mapping[str, Any]], *, source_pass_blocks: Sequence[str]
) -> dict[str, Any]:
    cell = next(
        (dict(row) for row in cells if int(row.get("horizon_trading_days", 0)) == 10),
        None,
    )
    if cell is None:
        return {"passed": False, "reason": "NO_COMPLETE_10_DAY_CELL", "checks": {}}
    normal = dict(cell["normal"])
    stressed = dict(cell["stressed"])
    checks = {
        "source_block_diverse": len(set(source_pass_blocks)) >= 2,
        "normal_pass": int(normal.get("pass_count", 0)) >= 1,
        "stressed_pass": int(stressed.get("pass_count", 0)) >= 1,
        "positive_stressed_net": float(stressed.get("net_total_usd", 0.0)) > 0.0,
        "controlled_stressed_mll": float(stressed.get("mll_breach_rate", 1.0)) <= 0.10,
        "stressed_passing_consistency": bool(
            stressed.get("all_passing_paths_consistency_compliant", False)
        ),
        "full_coverage": int(cell.get("full_coverage_start_count", 0)) > 0,
    }
    passed = all(checks.values())
    return {
        "passed": passed,
        "checks": checks,
        "reason": "TIER_G_GATE_PASSED" if passed else "TIER_G_GATE_FAILED",
        "evidence_ceiling": "TIER_G_DEVELOPMENT",
        "tier_c_permitted": False,
    }


def _terminal_result(
    frozen: Mapping[str, Any],
    *,
    acquisition: Mapping[str, Any],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    intents: int,
    events: int,
    account_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    qualifiers = sorted(
        str(row["account_label"])
        for row in account_results
        if bool(row.get("tier_g_graduated"))
    )
    graduated = bool(qualifiers)
    core = {
        "schema": SCHEMA,
        "status": (
            "BREADTH_CONTINUATION_TIER_G_GRADUATED"
            if graduated
            else "BREADTH_CONTINUATION_FALSIFIED_TOMBSTONE_EXACT_SPEC"
        ),
        "contract_hash": str(frozen["contract_hash"]),
        "specification_id": SPECIFICATION_ID,
        "candidate_fingerprint": CANDIDATE_FINGERPRINT,
        "calibration_hash": CALIBRATION_HASH,
        "calibration_reused_without_recalibration": True,
        "retuning_performed": False,
        "data_role": DATA_ROLE,
        "acquisition": dict(acquisition),
        "calendar": {
            "warmup_complete_sessions": WARMUP_COMPLETE_SESSIONS,
            "post_warmup_session_count": len(calendar),
            "evaluation_first_session_day": int(calendar[0]),
            "evaluation_last_session_day": int(calendar[-1]),
            "non_overlapping_start_counts": {
                str(horizon): len(starts[horizon]) for horizon in HORIZONS
            },
        },
        "emitted_intent_count": intents,
        "completed_event_count": events,
        "account_results": [dict(row) for row in account_results],
        "tier_g_account_labels": qualifiers,
        "promotion_status": "TIER_G" if graduated else None,
        "tier_c_promoted": False,
        "evidence_ceiling": "TIER_G_DEVELOPMENT",
        "tombstone": (
            None
            if graduated
            else {
                "specification_id": SPECIFICATION_ID,
                "specification_fingerprint": SPECIFICATION_FINGERPRINT,
                "reason": "UNTOUCHED_Q3_FINAL_DEVELOPMENT_GATE_FAILED",
            }
        ),
        "next_action": (
            "FREEZE_TIER_G_AND_REQUIRE_GENUINELY_FRESH_TIER_C_CONFIRMATION"
            if graduated
            else "CLOSE_EXACT_BREADTH_BRANCH_AND_DISPATCH_DISTINCT_REPRESENTATION"
        ),
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def _post_warmup_common_calendar(
    matrices: Mapping[str, FeatureMatrix]
) -> tuple[int, ...]:
    common: set[int] | None = None
    start_ns = _date_ns(START)
    end_ns = _date_ns(END)
    for matrix in matrices.values():
        timestamps = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
        days = np.asarray(matrix.array("session_day"), dtype=np.int64)
        current = {int(value) for value in days[(timestamps >= start_ns) & (timestamps < end_ns)]}
        common = current if common is None else common & current
    ordered = tuple(sorted(common or ()))
    if len(ordered) <= WARMUP_COMPLETE_SESSIONS:
        raise FrozenBreadthContinuationError("insufficient common Q3 sessions")
    return ordered[WARMUP_COMPLETE_SESSIONS:]


def _calibrated_from_payload(
    candidate: HazardCandidate, raw: Mapping[str, Any]
) -> CalibratedHazardCandidate:
    value = dict(raw)
    calibrated = CalibratedHazardCandidate(
        candidate=candidate,
        calibration_end_exclusive_ns=int(value["calibration_end_exclusive_ns"]),
        trigger_threshold=float(value["trigger_threshold"]),
        context_threshold=float(value["context_threshold"]),
        finite_trigger_observations=int(value["finite_trigger_observations"]),
        finite_context_observations=int(value["finite_context_observations"]),
        source_matrix_hash=str(value["source_matrix_hash"]),
    )
    if calibrated.fingerprint != str(value.get("calibration_hash")) or calibrated.fingerprint != CALIBRATION_HASH:
        raise FrozenBreadthContinuationError("frozen calibration hash drift")
    return calibrated


def _verify_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    expected = str(row.pop("contract_hash", ""))
    if not expected or stable_hash(row) != expected:
        raise FrozenBreadthContinuationError("breadth continuation contract hash drift")
    if (
        row.get("schema") != CONTRACT_SCHEMA
        or row.get("specification_id") != SPECIFICATION_ID
        or row.get("candidate_fingerprint") != CANDIDATE_FINGERPRINT
        or row.get("data_partition", {}).get("candidate_modification_allowed") is not False
        or row.get("data_partition", {}).get("recalibration_allowed") is not False
        or row.get("tier_c_permitted") is not False
    ):
        raise FrozenBreadthContinuationError("breadth continuation freeze semantics drift")
    return {**row, "contract_hash": expected}


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FrozenBreadthContinuationError("path escapes repository") from exc
    if not resolved.is_file():
        raise FrozenBreadthContinuationError(f"required file absent: {resolved}")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise FrozenBreadthContinuationError(f"JSON object expected: {path}")
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
    "FrozenBreadthContinuationError",
    "build_breadth_feature_bundles",
    "decide_tier_g_gate",
    "evaluate_breadth_continuation",
    "freeze_breadth_continuation_contract",
    "frozen_data_request",
    "open_breadth_matrices",
    "validate_acquisition_receipt",
]
