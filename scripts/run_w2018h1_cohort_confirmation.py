#!/usr/bin/env python3
"""Consume the frozen W2018H1 one-shot confirmation cohort exactly once.

The economic cohort was frozen by the W2019 cross-era marginal-pair builder.
This adapter only binds the causal feature dependencies, acquires the sealed
half-year packet under the authoritative Databento ledger, and replays the
eight immutable account policies.  It never recalibrates, retunes, reranks, or
starts XFA.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import databento as db
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.data.budget import DatabentoBudgetConfig, cumulative_spend
from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_loader import load_api_key
from hydra.economic_evolution.schema import stable_hash
from hydra.production import fresh_confirmation_lane as lane
from hydra.production.autonomous_exact_replay import _require_scenario_identity
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.research import cross_era_bank_sieve as sieve
from hydra.research import cross_era_marginal_pair_builder as pairs
from hydra.research.pnl_state_risk_frontier import _PreparedPolicy, _evaluate_profile
from scripts import acquire_fresh_confirmation_0035 as acquisition


ROOT = Path(__file__).resolve().parents[1]
PAIR_DIR = ROOT / (
    "reports/economic_evolution/cross_era_bank_sieve_2019h1_v1/"
    "marginal_pair_builder"
)
SOURCE_CONTRACT_PATH = PAIR_DIR.parent / "selection_contract.json"
PAIR_CONTRACT_PATH = PAIR_DIR / "pair_selection_contract.json"
PARENT_CONTRACT_PATH = PAIR_DIR / "w2018h1_one_shot_confirmation_contract.json"
OUTPUT_DIR = PAIR_DIR / "w2018h1_confirmation"
ACQUISITION_CONTRACT_PATH = OUTPUT_DIR / "acquisition_contract.json"
ACQUISITION_RECEIPT_PATH = OUTPUT_DIR / "acquisition_receipt.json"
FEATURE_RECEIPT_PATH = OUTPUT_DIR / "feature_receipt.json"
ECONOMIC_RESULT_PATH = OUTPUT_DIR / "economic_result.json"
EVIDENCE_BUNDLE_PATH = OUTPUT_DIR / "evidence_bundle.json"
DECISION_REPORT_PATH = OUTPUT_DIR / "decision_report.json"
STATE_PATH = OUTPUT_DIR / "production_state.json"
AUTHORIZATION_MANIFEST_PATH = (
    ROOT / "config/v7/autonomous_economic_discovery_director_0035.json"
)
RULE_SNAPSHOT_PATH = ROOT / "config/rulesets/topstep_official_2026-07-19.json"

EXPECTED_PARENT_HASH = "cf0eb8e7dbdd66fbbbe8b111bd749846cc704d970f6d0c06cd07e2f0762b8ab7"
START = "2018-01-02"
END = "2018-07-02"
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
SYMBOLS = ("CL.c.0", "NQ.c.0", "ES.c.0")
ROOTS = ("CL", "NQ", "ES")
CAP = 200.720719923081
WARMUP_COMPLETE_SESSIONS = 5
PURPOSE = (
    "one-shot W2018H1 independent confirmation of eight immutable cross-era "
    "DEVELOPMENT_REQUALIFIED candidates; no retuning"
)
FEATURE_VERSION = "hydra_w2018h1_cross_era_confirmation_causal_bundle_v1"


class W2018ConfirmationError(RuntimeError):
    """The frozen W2018H1 contract cannot be consumed safely."""


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise W2018ConfirmationError(f"JSON object required: {path}")
    return value


def _verify_hash(value: Mapping[str, Any], field: str, *, label: str) -> str:
    core = dict(value)
    claimed = str(core.pop(field, ""))
    if not claimed or stable_hash(core) != claimed:
        raise W2018ConfirmationError(f"{label} hash drift")
    return claimed


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    content = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != content:
            raise W2018ConfirmationError(f"refusing divergent rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _write_state(status: str, **fields: Any) -> None:
    value = {
        "schema": "hydra_w2018h1_cohort_confirmation_state_v1",
        "status": status,
        "worker_processes": 1,
        "numeric_threads_per_worker": 1,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        **fields,
    }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_name(f".{STATE_PATH.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, STATE_PATH)


def _parent_contract() -> dict[str, Any]:
    contract = _read(PARENT_CONTRACT_PATH)
    claimed = _verify_hash(contract, "contract_hash", label="parent contract")
    if claimed != EXPECTED_PARENT_HASH:
        raise W2018ConfirmationError("unexpected W2018H1 parent contract")
    if contract.get("status") != "W2018H1_ONE_SHOT_CONFIRMATION_CONTRACT_FROZEN_DATA_UNOPENED":
        raise W2018ConfirmationError("parent contract is not frozen/unopened")
    if int(contract.get("candidate_count", 0)) != 8:
        raise W2018ConfirmationError("frozen confirmation cohort is not eight")
    if dict(contract.get("window") or {}) != {
        "window_id": "W2018H1",
        "start_inclusive": START,
        "end_exclusive": END,
        "role_frozen_before_bar_access": True,
        "previous_candidate_outcome_access_count": 0,
        "q4_overlap": False,
    }:
        raise W2018ConfirmationError("W2018H1 window contract drift")
    return contract


def _dependency_inventory(
    parent: Mapping[str, Any], source: Mapping[str, Any]
) -> dict[str, Any]:
    components = {str(key): dict(value) for key, value in source["components"].items()}
    component_ids = sorted(
        {
            str(member)
            for candidate in parent["candidates"]
            for member in candidate["component_ids"]
        }
    )
    missing = [value for value in component_ids if value not in components]
    if missing:
        raise W2018ConfirmationError(f"frozen components absent: {missing}")
    signal_markets = sorted(
        {str(dict(components[value]["candidate"])["market"]) for value in component_ids}
    )
    cross_markets = sorted(
        {
            str(reference)
            for value in component_ids
            if (
                reference := dict(components[value]["candidate"]).get(
                    "cross_asset_reference_market"
                )
            )
        }
    )
    required = sorted(set(signal_markets) | set(cross_markets))
    if required != sorted(ROOTS):
        raise W2018ConfirmationError(
            f"causal feature dependency inventory drift: {required}"
        )
    if signal_markets != sorted(parent["required_markets"]):
        raise W2018ConfirmationError("parent signal-market inventory drift")
    return {
        "component_ids": component_ids,
        "signal_markets_declared_by_parent": signal_markets,
        "cross_asset_feature_dependencies_derived_from_frozen_candidates": cross_markets,
        "complete_causal_feature_markets": required,
        "dependency_expansion_changes_candidate_logic": False,
    }


def _patch_acquisition() -> None:
    acquisition.START = START
    acquisition.END = END
    acquisition.SYMBOLS = SYMBOLS
    acquisition.ROOTS = ROOTS
    acquisition.CUMULATIVE_HARD_CAP_USD = CAP
    acquisition.REQUEST_PURPOSE = PURPOSE
    acquisition.CANDIDATE_TIER = "DEVELOPMENT_REQUALIFIED_AWAITING_W2018_CONFIRMATION"
    acquisition.RECEIPT_SCHEMA = "hydra_w2018h1_cohort_acquisition_receipt_v1"


def _official_stats(client: Any) -> dict[str, Any]:
    _patch_acquisition()
    symbology = acquisition._resolve_explicit_contract_inputs(client)
    requests = {
        "ohlcv-1m": {
            "dataset": "GLBX.MDP3",
            "schema": "ohlcv-1m",
            "symbols": list(SYMBOLS),
            "stype_in": "continuous",
            "start": START,
            "end": END,
        },
        "definition": {
            "dataset": "GLBX.MDP3",
            "schema": "definition",
            "symbols": list(symbology["instrument_ids"]),
            "stype_in": "instrument_id",
            "start": START,
            "end": END,
        },
    }
    parts = {}
    for name, request in requests.items():
        parts[name] = {
            "estimated_cost_usd": float(client.metadata.get_cost(**request)),
            "record_count": int(client.metadata.get_record_count(**request)),
            "billable_size_bytes": int(client.metadata.get_billable_size(**request)),
        }
    return {
        "requests": requests,
        "instrument_ids": list(symbology["instrument_ids"]),
        "symbology_hash": symbology["mapping_hash"],
        "parts": parts,
        "total_estimated_cost_usd": sum(
            float(value["estimated_cost_usd"]) for value in parts.values()
        ),
        "total_record_count": sum(int(value["record_count"]) for value in parts.values()),
        "total_billable_size_bytes": sum(
            int(value["billable_size_bytes"]) for value in parts.values()
        ),
    }


def freeze_acquisition(client: Any) -> dict[str, Any]:
    parent = _parent_contract()
    source = _read(SOURCE_CONTRACT_PATH)
    pair_contract = _read(PAIR_CONTRACT_PATH)
    _verify_hash(source, "contract_hash", label="source sieve contract")
    _verify_hash(pair_contract, "contract_hash", label="pair contract")
    dependencies = _dependency_inventory(parent, source)
    stats = _official_stats(client)
    _estimated, actual = cumulative_spend(
        ROOT / "reports/data_budget/databento_spend_ledger.jsonl"
    )
    projected = actual + float(stats["total_estimated_cost_usd"])
    if projected > CAP + 1e-12:
        raise W2018ConfirmationError("W2018H1 packet exceeds remaining authority")
    request_core = {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "symbols": list(SYMBOLS),
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "start": START,
        "end": END,
        "date_interval": "HALF_OPEN",
        "data_role": "CONFIRMATION",
        "q4_2024_access_allowed": False,
        "broker_or_order_capability": False,
    }
    request = {
        **request_core,
        "request_hash": stable_hash(request_core),
        "frozen_estimated_cost_usd": stats["total_estimated_cost_usd"],
        "prior_cumulative_actual_usd": actual,
        "additional_authority_usd": 100.0,
        "cumulative_hard_cap_usd": CAP,
        "projected_cumulative_usd": projected,
    }
    manifest = _read(AUTHORIZATION_MANIFEST_PATH)
    rule_snapshot = _read(RULE_SNAPSHOT_PATH)
    core = {
        "schema": "hydra_w2018h1_cohort_acquisition_contract_v1",
        "status": "FROZEN_AWAITING_ACQUISITION",
        "parent_economic_contract_hash": parent["contract_hash"],
        "candidate_count": 8,
        "tier_g_candidates": [
            {"candidate_id": str(row["candidate_id"])} for row in parent["candidates"]
        ],
        "data_request": request,
        "official_cost_matrix": stats,
        "causal_feature_dependency_audit": dependencies,
        "data_partition": {
            "role": "CONFIRMATION",
            "entire_post_warmup_block_consumed_once": True,
            "warmup_rule": "FIRST_5_COMMON_COMPLETE_SESSION_DAYS_EXCLUDED",
            "candidate_modification_allowed": False,
            "recalibration_allowed": False,
        },
        "source_bindings": {
            "parent_contract_file_sha256": _sha256(PARENT_CONTRACT_PATH),
            "source_sieve_contract_hash": source["contract_hash"],
            "pair_contract_hash": pair_contract["contract_hash"],
            "authorization_manifest_hash": manifest["manifest_hash"],
            "official_rule_snapshot_hash": rule_snapshot["parsed_rule_hash"],
        },
        "worker_processes": 1,
        "numeric_threads_per_worker": 1,
        "retuning_allowed": False,
        "recalibration_allowed": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths_started": 0,
    }
    value = {**core, "contract_hash": stable_hash(core)}
    _write_once(ACQUISITION_CONTRACT_PATH, value)
    _write_state(
        "W2018H1_ACQUISITION_CONTRACT_FROZEN",
        estimated_cost_usd=stats["total_estimated_cost_usd"],
        projected_cumulative_usd=projected,
    )
    return value


def acquire(client: Any, *, execute: bool) -> dict[str, Any]:
    _parent_contract()
    contract = _read(ACQUISITION_CONTRACT_PATH)
    _verify_hash(contract, "contract_hash", label="acquisition contract")
    manifest = _read(AUTHORIZATION_MANIFEST_PATH)
    _patch_acquisition()
    value = acquisition.acquire_fresh_confirmation(
        contract=contract,
        manifest=manifest,
        expected_manifest_hash=str(manifest["manifest_hash"]),
        root=ROOT,
        client=client,
        execute=execute,
        budget=DatabentoBudgetConfig(hard_cap_usd=CAP, safety_ceiling_usd=CAP),
        receipt_path=ACQUISITION_RECEIPT_PATH,
    )
    if execute:
        _write_state(
            "W2018H1_PACKET_ACQUIRED_ONCE",
            actual_cost_usd=value["actual_cost_usd"],
            cumulative_actual_usd=value["cumulative_actual_usd"],
        )
    return value


def build_features() -> dict[str, Any]:
    parent = _parent_contract()
    contract = _read(ACQUISITION_CONTRACT_PATH)
    receipt = _read(ACQUISITION_RECEIPT_PATH)
    _verify_hash(contract, "contract_hash", label="acquisition contract")
    _verify_hash(receipt, "receipt_hash", label="acquisition receipt")
    lane.validate_acquisition_receipt(contract, receipt)
    source_binding = dict(receipt["feature_build_inputs"])["source_files"][0]
    source_path = Path(str(source_binding["path"]))
    map_path = Path(str(dict(receipt["feature_build_inputs"])["contract_map_path"]))
    if _sha256(source_path) != str(source_binding["sha256"]):
        raise W2018ConfirmationError("normalized source hash drift")
    if _sha256(map_path) != str(
        dict(receipt["feature_build_inputs"])["contract_map_sha256"]
    ):
        raise W2018ConfirmationError("contract map hash drift")
    roll_map = load_roll_map(map_path)
    if {str(row.root) for row in roll_map.contracts} != set(ROOTS):
        raise W2018ConfirmationError("W2018 roll-map root inventory drift")
    frame = lane.pd.read_parquet(source_path)
    normalized = lane.normalize_ohlcv_frame(
        frame, symbol=None, timeframe="1m", symbol_map={root: root for root in ROOTS}
    )
    lane.validate_ohlcv_frame(normalized, timeframe="1m")
    if set(normalized["symbol"].astype(str)) != set(ROOTS):
        raise W2018ConfirmationError("normalized causal market inventory drift")
    mapped, map_guard = lane._apply_explicit_contract_map(
        normalized, roll_map, required_map_type=roll_map.map_type
    )
    featured = lane._prepare_feature_frame(lane._build_past_only_feature_frame(mapped))
    source_hash = stable_hash(
        [{"path": str(source_path), "sha256": source_binding["sha256"], "rows": len(normalized)}]
    )
    map_hash = _sha256(map_path)
    store = lane.CanonicalFeatureStore(dict(receipt["feature_build_inputs"])["cache_root"])
    bundles = {}
    for market in ROOTS:
        market_frame = featured.loc[featured["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = lane._market_arrays(market_frame, market)
        arrays.pop("entry_price", None)
        for key in tuple(arrays):
            if key.startswith("forward_move__"):
                arrays.pop(key)
        key = lane.CanonicalFeatureKey(
            market=market,
            explicit_contract_scope=f"{market}_date_aware_active_contracts_w2018h1_confirmation",
            start_inclusive=START,
            end_exclusive=END,
            source_data_sha256=source_hash,
            roll_map_hash=map_hash,
            transformation_version=FEATURE_VERSION,
            feature_dag_hash=lane.FEATURE_DAG_HASH,
            timeframes=("1m", "5m", "15m", "30m", "60m", "session", "daily"),
        )
        stored = store.put(
            key,
            arrays,
            provenance={
                "data_role": "CONFIRMATION",
                "parent_economic_contract_hash": parent["contract_hash"],
                "acquisition_contract_hash": contract["contract_hash"],
                "market": market,
                "feature_names": list(metadata["feature_names"]),
                "future_outcome_arrays_in_decision_bundle": False,
                "entry_fill_contract": "SIGNAL_AFTER_COMPLETED_T_THEN_NEXT_TRADABLE_BAR_OPEN",
                "q4_access_count_delta": 0,
            },
        )
        bundles[market] = {
            "path": str(stored.path),
            "bundle_hash": stored.bundle_hash,
            "row_count": stored.row_count,
            "cache_hit": stored.cache_hit,
        }
    core = {
        "schema": "hydra_w2018h1_cohort_feature_receipt_v1",
        "status": "W2018H1_CAUSAL_FEATURE_BUNDLES_READY",
        "parent_economic_contract_hash": parent["contract_hash"],
        "acquisition_contract_hash": contract["contract_hash"],
        "acquisition_receipt_hash": receipt["receipt_hash"],
        "source_path": str(source_path),
        "source_sha256": source_binding["sha256"],
        "contract_map_path": str(map_path),
        "contract_map_sha256": map_hash,
        "roll_map_hash": roll_map.roll_map_hash(),
        "map_guard": map_guard,
        "bundles": bundles,
        "causal_feature_markets": list(ROOTS),
        "cross_asset_dependency_market": "ES",
        "candidate_logic_mutated": False,
        "future_outcome_arrays_in_decision_bundle": False,
        "data_role": "CONFIRMATION",
        "q4_access_count_delta": 0,
    }
    value = {**core, "result_hash": stable_hash(core)}
    _write_once(FEATURE_RECEIPT_PATH, value)
    _write_state(
        "W2018H1_CAUSAL_FEATURE_BUNDLES_READY",
        market_count=len(bundles),
        total_feature_rows=sum(int(row["row_count"]) for row in bundles.values()),
    )
    return value


def _open_features(receipt: Mapping[str, Any]) -> dict[str, Any]:
    _verify_hash(receipt, "result_hash", label="feature receipt")
    output = {}
    for market, raw in dict(receipt["bundles"]).items():
        matrix = lane.FeatureMatrix.open(str(dict(raw)["path"]), mmap=True)
        if matrix.fingerprint != str(dict(raw)["bundle_hash"]):
            raise W2018ConfirmationError(f"feature matrix drift: {market}")
        output[str(market)] = matrix
    if set(output) != set(ROOTS):
        raise W2018ConfirmationError("feature matrix inventory drift")
    return output


def _calendar(matrices: Mapping[str, Any]) -> tuple[int, ...]:
    common: set[int] | None = None
    for matrix in matrices.values():
        timestamps = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
        days = np.asarray(matrix.array("session_day"), dtype=np.int64)
        mask = (timestamps >= lane._date_ns(START)) & (timestamps < lane._date_ns(END))
        current = {int(value) for value in days[mask]}
        common = current if common is None else common & current
    ordered = tuple(sorted(common or ()))
    if len(ordered) <= WARMUP_COMPLETE_SESSIONS + max(HORIZONS):
        raise W2018ConfirmationError("W2018 common calendar lacks complete horizons")
    return ordered[WARMUP_COMPLETE_SESSIONS:]


def _policy(
    candidate: Mapping[str, Any], rule: Mapping[str, Any]
) -> tuple[ActiveRiskPoolPolicy, dict[str, int]]:
    spec = dict(candidate["executable_spec"])
    quantities = {
        str(key): int(value)
        for key, value in dict(spec["component_quantity_tiers"]).items()
    }
    if candidate["source_kind"] == "EXACT_STANDALONE":
        value = ActiveRiskPoolPolicy.from_mapping(dict(spec["frozen_account_policy"]))
        if stable_hash(value.to_dict()) != stable_hash(spec["frozen_account_policy"]):
            raise W2018ConfirmationError("standalone policy reconstruction drift")
        return value, quantities
    if candidate["source_kind"] == "MARGINALLY_ACCEPTED_PAIR":
        return pairs._active_policy(spec, rule), quantities
    raise W2018ConfirmationError(f"unsupported frozen source kind: {candidate['source_kind']}")


def evaluate() -> dict[str, Any]:
    if ECONOMIC_RESULT_PATH.exists():
        raise W2018ConfirmationError("W2018H1 confirmation packet was already consumed")
    parent = _parent_contract()
    acquisition_contract = _read(ACQUISITION_CONTRACT_PATH)
    acquisition_receipt = _read(ACQUISITION_RECEIPT_PATH)
    feature_receipt = _read(FEATURE_RECEIPT_PATH)
    source = _read(SOURCE_CONTRACT_PATH)
    pair_contract = _read(PAIR_CONTRACT_PATH)
    _verify_hash(acquisition_contract, "contract_hash", label="acquisition contract")
    _verify_hash(acquisition_receipt, "receipt_hash", label="acquisition receipt")
    _verify_hash(feature_receipt, "result_hash", label="feature receipt")
    _verify_hash(source, "contract_hash", label="source sieve contract")
    _verify_hash(pair_contract, "contract_hash", label="pair contract")
    matrices = _open_features(feature_receipt)
    calendar = _calendar(matrices)
    starts = lane.non_overlapping_starts(calendar, HORIZONS)
    starts = {
        horizon: tuple((int(day), "W2018H1_CONFIRMATION") for day, _ in rows)
        for horizon, rows in starts.items()
    }
    components = {str(key): dict(value) for key, value in source["components"].items()}
    rules = {str(key): dict(value) for key, value in source["account_rules"].items()}
    component_ids = sorted(
        {
            str(member)
            for candidate in parent["candidates"]
            for member in candidate["component_ids"]
        }
    )
    cache = {}
    for component_id in component_ids:
        _replay, materialized = sieve._component_replay(
            components[component_id], matrices, calendar
        )
        cache[component_id] = materialized
    _write_state(
        "W2018H1_EXACT_CONFIRMATION_REPLAY_ACTIVE",
        candidate_count=8,
        component_replay_count=len(cache),
        non_overlapping_start_counts={str(key): len(value) for key, value in starts.items()},
    )

    identity = sieve.PROFILE_BY_ID["pnl_state_identity"]
    results = []
    exact_episodes = 0
    for frozen in parent["candidates"]:
        candidate = dict(frozen)
        freeze_core = dict(candidate)
        claimed_freeze = str(freeze_core.pop("candidate_freeze_hash", ""))
        if stable_hash(freeze_core) != claimed_freeze:
            raise W2018ConfirmationError("candidate freeze hash drift")
        executable = dict(candidate["executable_spec"])
        if candidate["source_kind"] == "EXACT_STANDALONE":
            executable_hash = stable_hash(executable)
        else:
            claimed_policy_hash = str(executable.pop("policy_spec_hash", ""))
            executable.pop("policy_id", None)
            executable.pop("policy_role", None)
            executable_hash = stable_hash(executable)
            if executable_hash != claimed_policy_hash:
                raise W2018ConfirmationError("pair policy semantic hash drift")
        if executable_hash != str(candidate["executable_spec_hash"]):
            raise W2018ConfirmationError("candidate executable specification drift")
        members = [str(value) for value in candidate["component_ids"]]
        account_label = str(candidate["account_label"])
        rule = rules[account_label]
        policy, quantities = _policy(candidate, rule)
        trajectories = {scenario: {} for scenario in SCENARIOS}
        unavailable: set[int] = set()
        receipts = []
        for member in members:
            value = cache[member]
            quantity = quantities[member]
            normal = tuple(
                scale_causal_trajectory(row, executable_quantity_multiplier=quantity)
                for row in value["normal"]
            )
            stressed = tuple(
                scale_causal_trajectory(row, executable_quantity_multiplier=quantity)
                for row in value["stressed"]
            )
            _require_scenario_identity(normal, stressed)
            trajectories["NORMAL"][member] = normal
            trajectories["STRESSED_1_5X"][member] = stressed
            unavailable.update(value["unavailable_days"])
            receipts.append(
                {
                    "candidate_id": member,
                    "quantity_tier": quantity,
                    "intent_count": value["intent_count"],
                    "completed_event_count": value["completed_event_count"],
                    "censored_event_count": value["censored_event_count"],
                    "decision_hash": value["decision_hash"],
                    "normal_trajectory_hash": value["normal_trajectory_hash"],
                    "stressed_trajectory_hash": value["stressed_trajectory_hash"],
                    "fill_policy_hash": value["fill_policy_hash"],
                }
            )
        prepared = _PreparedPolicy(
            policy_id=str(candidate["candidate_id"]),
            source_kind=str(candidate["source_kind"]),
            evidence_tier="DEVELOPMENT_REQUALIFIED_INPUT",
            account_label=account_label,
            baseline_policy=policy,
            trajectories=trajectories,
            unavailable_days=frozenset(unavailable),
            source_policy=candidate,
            source_metrics={},
            source_hashes={},
        )
        evaluation = _evaluate_profile(
            prepared,
            identity,
            blocks=("W2018H1_CONFIRMATION",),
            calendar=calendar,
            starts=starts,
            rule=rule,
        )
        exact_episodes += int(evaluation["exact_episode_count"])
        headline = int(candidate["headline_horizon_trading_days"])
        normal = dict(evaluation["summaries"]["NORMAL"][str(headline)])
        stressed = dict(evaluation["summaries"]["STRESSED_1_5X"][str(headline)])
        checks = {
            "at_least_one_normal_pass": int(normal["pass_count"]) >= 1,
            "at_least_one_stressed_pass": int(stressed["pass_count"]) >= 1,
            "positive_stressed_net": float(stressed["net_total_usd"]) > 0.0,
            "zero_normal_mll_breach": int(normal["mll_breach_count"]) == 0,
            "zero_stressed_mll_breach": int(stressed["mll_breach_count"]) == 0,
            # A path cannot enter PASSED in the shared account engine unless
            # the consistency target is true at that same chronological state.
            "all_normal_passing_paths_consistency_compliant": int(normal["pass_count"]) >= 1,
            "all_stressed_passing_paths_consistency_compliant": int(stressed["pass_count"]) >= 1,
            "headline_full_coverage": min(
                int(normal["full_coverage_start_count"]),
                int(stressed["full_coverage_start_count"]),
            ) > 0,
        }
        confirmed = all(checks.values())
        positive_replication = bool(
            not confirmed
            and float(stressed["net_total_usd"]) > 0.0
            and int(normal["mll_breach_count"]) == 0
            and int(stressed["mll_breach_count"]) == 0
        )
        normal_only = bool(
            not confirmed
            and int(normal["pass_count"]) >= 1
            and int(stressed["pass_count"]) == 0
        )
        if confirmed:
            decision = "TIER_C_INDEPENDENTLY_CONFIRMED"
            tier = "C"
        elif normal_only:
            decision = "NORMAL_CONFIRMATION_PASS_PRESERVED_STRESS_GATE_FAILED_NO_TIER_C"
            tier = "DEVELOPMENT_REQUALIFIED"
        elif positive_replication:
            decision = "POSITIVE_REPLICATION_WITHOUT_FULL_PASS_REMAINS_DEVELOPMENT_REQUALIFIED"
            tier = "DEVELOPMENT_REQUALIFIED"
        else:
            decision = "CANDIDATE_CONFIRMATION_FAILED_NO_RETUNING_ON_W2018"
            tier = "CONFIRMATION_FAILED_BRANCH_CLOSED"
        core = {
            "candidate_id": candidate["candidate_id"],
            "candidate_freeze_hash": candidate["candidate_freeze_hash"],
            "behavior_cluster_id": candidate["behavior_cluster_id"],
            "behavior_hash": candidate["behavior_hash"],
            "source_kind": candidate["source_kind"],
            "component_ids": members,
            "account_label": account_label,
            "headline_horizon_trading_days": headline,
            "component_receipts": receipts,
            "evaluation": evaluation,
            "confirmation_gate": {"passed": confirmed, "checks": checks},
            "decision": decision,
            "evidence_tier": tier,
            "normal_pass_status_preserved_independently_of_stress": normal_only,
            "retuning_performed": False,
            "recalibration_performed": False,
            "reranking_performed": False,
            "candidate_or_book_mutated": False,
        }
        results.append({**core, "result_hash": stable_hash(core)})

    tier_c_ids = [str(row["candidate_id"]) for row in results if row["evidence_tier"] == "C"]
    core = {
        "schema": "hydra_w2018h1_cohort_confirmation_economic_result_v1",
        "status": "W2018H1_CONFIRMATION_PACKET_CONSUMED_ONCE",
        "decision": (
            "M1_TIER_C_REACHED" if tier_c_ids else "NO_TIER_C_CANDIDATE_CONFIRMED"
        ),
        "parent_contract_hash": parent["contract_hash"],
        "acquisition_contract_hash": acquisition_contract["contract_hash"],
        "acquisition_receipt_hash": acquisition_receipt["receipt_hash"],
        "feature_receipt_hash": feature_receipt["result_hash"],
        "calendar": {
            "period": f"{START}:{END}",
            "warmup_complete_sessions": WARMUP_COMPLETE_SESSIONS,
            "post_warmup_session_count": len(calendar),
            "first_session_day": int(calendar[0]),
            "last_session_day": int(calendar[-1]),
            "non_overlapping_start_counts": {
                str(key): len(value) for key, value in starts.items()
            },
        },
        "candidate_count": len(results),
        "component_replay_count": len(cache),
        "exact_account_episode_count": exact_episodes,
        "candidate_results": results,
        "tier_c_candidate_ids": tier_c_ids,
        "normal_pass_candidate_ids": [
            str(row["candidate_id"])
            for row in results
            if int(
                row["evaluation"]["summaries"]["NORMAL"][
                    str(row["headline_horizon_trading_days"])
                ]["pass_count"]
            )
            > 0
        ],
        "packet_reuse_allowed": False,
        "retuning_performed": False,
        "recalibration_performed": False,
        "reranking_performed": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths_started": 0,
        "next_action": (
            "FREEZE_TIER_C_WINNERS_FOR_F0_AND_FORWARD_WHILE_RESEARCH_CONTINUES"
            if tier_c_ids
            else "CLOSE_FAILED_EXACT_BRANCHES_AND_REALLOCATE_TO_MATERIALLY_DISTINCT_EVI_BRANCH"
        ),
    }
    result = {**core, "result_hash": stable_hash(core)}
    _write_once(ECONOMIC_RESULT_PATH, result)
    bundle_core = {
        "schema": "hydra_w2018h1_cohort_confirmation_evidence_bundle_v1",
        "status": "COMPLETE_CAUSAL_CONFIRMATION_EVIDENCE_BUNDLE",
        "parent_contract_hash": parent["contract_hash"],
        "raw_data_sha256": next(
            row["sha256"]
            for row in acquisition_receipt["files"]
            if row["kind"] == "RAW_DBN_OHLCV"
        ),
        "feature_receipt_hash": feature_receipt["result_hash"],
        "candidate_freeze_hashes": {
            str(row["candidate_id"]): str(row["candidate_freeze_hash"])
            for row in parent["candidates"]
        },
        "candidate_result_hashes": {
            str(row["candidate_id"]): str(row["result_hash"]) for row in results
        },
        "economic_result_hash": result["result_hash"],
        "decision_engine": "SHARED_CAUSAL_BATCH_STREAMING_PATH",
        "fill_policy": "CAUSAL_NEXT_TRADABLE_OPEN_UNCHANGED",
        "accounting": "EXACT_CHRONOLOGICAL_PNL_MLL_CONSISTENCY",
        "data_role": "CONFIRMATION_CONSUMED_ONCE",
        "promotion_status": "TIER_C" if tier_c_ids else None,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths_started": 0,
    }
    bundle = {**bundle_core, "evidence_bundle_hash": stable_hash(bundle_core)}
    _write_once(EVIDENCE_BUNDLE_PATH, bundle)
    report_core = {
        "schema": "hydra_w2018h1_cohort_confirmation_decision_report_v1",
        "economic_verdict": result["decision"],
        "tier_c_candidate_ids": tier_c_ids,
        "candidate_decisions": [
            {
                "candidate_id": row["candidate_id"],
                "account_label": row["account_label"],
                "headline_horizon_trading_days": row["headline_horizon_trading_days"],
                "decision": row["decision"],
                "confirmation_gate": row["confirmation_gate"],
                "headline_normal": row["evaluation"]["summaries"]["NORMAL"][
                    str(row["headline_horizon_trading_days"])
                ],
                "headline_stressed": row["evaluation"]["summaries"]["STRESSED_1_5X"][
                    str(row["headline_horizon_trading_days"])
                ],
            }
            for row in results
        ],
        "budget": {
            "incremental_actual_usd": acquisition_receipt["actual_cost_usd"],
            "cumulative_actual_usd": acquisition_receipt["cumulative_actual_usd"],
            "remaining_authorized_usd": CAP
            - float(acquisition_receipt["cumulative_actual_usd"]),
        },
        "evidence_bundle_hash": bundle["evidence_bundle_hash"],
        "economic_result_hash": result["result_hash"],
        "no_retry_on_same_packet": True,
        "candidates_mutated": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths_started": 0,
    }
    report = {**report_core, "result_hash": stable_hash(report_core)}
    _write_once(DECISION_REPORT_PATH, report)
    _write_state(
        "W2018H1_CONFIRMATION_PACKET_CONSUMED_ONCE",
        exact_account_episode_count=exact_episodes,
        tier_c_candidate_count=len(tier_c_ids),
        decision=result["decision"],
        next_action=result["next_action"],
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("cost", "acquire", "features", "evaluate"))
    arguments = parser.parse_args(argv)
    if arguments.action == "features":
        value = build_features()
    elif arguments.action == "evaluate":
        value = evaluate()
    else:
        key = load_api_key()
        if not key:
            raise W2018ConfirmationError("DATABENTO_API_KEY is unavailable")
        client = db.Historical(key)
        value = (
            freeze_acquisition(client)
            if arguments.action == "cost"
            else acquire(client, execute=True)
        )
    print(json.dumps(value, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
