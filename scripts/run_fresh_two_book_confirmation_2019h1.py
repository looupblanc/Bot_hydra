#!/usr/bin/env python3
"""One-shot W2019H1 confirmation of the two immutable marginal Tier-G books.

This adapter freezes the exact book memberships, component calibrations,
quantity tiers, governor policies, fill semantics, account rules, and hard
gate before any W2019H1 outcome is acquired.  It reuses the governed Databento
acquisition primitive and writes no mission DB or registry state.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import databento as db

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.budget import DatabentoBudgetConfig, cumulative_spend
from hydra.data.contract_mapping import load_roll_map
from hydra.data.contract_mapping import valid_outright_future_symbol
from hydra.data.databento_loader import load_api_key
from hydra.economic_evolution.schema import stable_hash
from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.account_policy.causal_active_pool_replay import run_causal_shared_account_episode
from hydra.production import fresh_confirmation_lane as lane
from hydra.production import marginal_book_tier_g_batch as marginal
from hydra.production.fast_pass_runtime_helpers import _summarize_sprint_episodes
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.research.causal_target_velocity import (
    HazardCandidate,
    HazardOutcome,
    calibrate_candidate,
    discover_intents_batch,
    discover_intents_streaming,
    exact_sleeve_replay,
    frozen_eligible_session_calendar,
    observe_outcomes,
)
from scripts import acquire_fresh_confirmation_0035 as acquisition


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports/economic_evolution/fresh_two_book_confirmation_2019h1_v1"
CONTRACT_PATH = REPORT_DIR / "contract.json"
RECEIPT_PATH = REPORT_DIR / "acquisition_receipt.json"
FEATURE_RECEIPT_PATH = REPORT_DIR / "feature_receipt.json"
RESULT_PATH = REPORT_DIR / "economic_result.json"
DECISION_REPORT_PATH = REPORT_DIR / "decision_report.json"
SOURCE_RESULT_PATH = ROOT / "reports/economic_evolution/marginal_book_tier_g_batch_v1/economic_result.json"
SEMANTIC_PATH = ROOT / marginal.SEMANTIC_BOOKS
FAST_PASS_MANIFEST_PATH = ROOT / "config/v7/fast_pass_factory_0029_revision_05.json"
AUTHORIZATION_MANIFEST_PATH = ROOT / "config/v7/autonomous_economic_discovery_director_0035.json"
RULE_SNAPSHOT_PATH = ROOT / "config/rulesets/topstep_official_2026-07-19.json"
ACCESS_LEDGER = ROOT / "reports/data_access/data_access_ledger.jsonl"

BOOK_IDS = (
    "autonomous_marginal_book_b09b8e7b30f90b34737eb724",
    "autonomous_marginal_book_2f3752128ff0fd44a71b2327",
)
START = "2019-01-02"
END = "2019-07-01"
SYMBOLS = (
    "ES.c.0", "MES.c.0", "NQ.c.0", "MNQ.c.0", "YM.c.0",
    "MYM.c.0", "RTY.c.0", "M2K.c.0", "CL.c.0", "MCL.c.0",
)
ROOTS = ("ES", "MES", "NQ", "MNQ", "YM", "MYM", "RTY", "M2K", "CL", "MCL")
SIGNAL_ROOTS = ("ES", "NQ", "YM", "RTY", "CL")
CAP = 200.720719923081
PURPOSE = (
    "one-shot W2019H1 FULL_BANK independent Tier-C confirmation of two "
    "immutable marginal Tier-G books under M1; no retuning"
)
FEATURE_VERSION = "hydra_two_book_confirmation_2019h1_causal_bundle_v1"


class TwoBookConfirmationError(RuntimeError):
    pass


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TwoBookConfirmationError(f"JSON object required: {path}")
    return value


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    content = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file():
        if path.read_text(encoding="utf-8") != content:
            raise TwoBookConfirmationError(f"refusing divergent rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def _verify_result(path: Path) -> dict[str, Any]:
    value = _read(path)
    core = dict(value)
    claimed = str(core.pop("result_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise TwoBookConfirmationError(f"result hash drift: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_untouched() -> dict[str, Any]:
    hits: list[dict[str, Any]] = []
    relevant = set(BOOK_IDS) | {"hazard_16f0da561bc98f2eb7d2efc4", "hazard_0a569f580a2540474116636c", "hazard_367100adab5fe2a69a4f3257"}
    market_tokens = set(SYMBOLS) | set(ROOTS)
    if ACCESS_LEDGER.is_file():
        for line in ACCESS_LEDGER.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            period = str(row.get("period_accessed") or "")
            parts = period.split(":")
            if len(parts) < 2:
                continue
            try:
                left = date.fromisoformat(parts[0][:10])
                right = date.fromisoformat(parts[1][:10])
            except ValueError:
                continue
            if left >= date.fromisoformat(END) or right <= date.fromisoformat(START):
                continue
            text = json.dumps(row, sort_keys=True)
            if any(token in text for token in relevant) or any(f'"{token}"' in text for token in market_tokens):
                hits.append(row)
    if hits:
        raise TwoBookConfirmationError("W2019H1 candidate-lineage/required-market packet is not untouched")
    return {
        "period": f"{START}:{END}",
        "scope": "TWO_FROZEN_BOOK_LINEAGES_AND_FULL_BANK_MARKETS",
        "relevant_access_ledger_hit_count": 0,
        "q4_overlap": False,
        "status": "UNTOUCHED_BEFORE_CONTRACT_FREEZE",
    }


def _candidate_entries() -> dict[str, dict[str, Any]]:
    wanted = {"hazard_16f0da561bc98f2eb7d2efc4", "hazard_0a569f580a2540474116636c", "hazard_367100adab5fe2a69a4f3257"}
    output: dict[str, dict[str, Any]] = {}
    base = ROOT / "data/cache/economic_production/hydra_fast_pass_factory_0029"
    for path in sorted(base.glob("wave_*/causal_executable_bank.json")):
        for raw in _read(path).get("entries", ()):
            row = dict(raw)
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id in wanted:
                prior = output.get(candidate_id)
                if prior is not None and stable_hash(prior["candidate"]) != stable_hash(row["candidate"]):
                    raise TwoBookConfirmationError("candidate semantic drift")
                output[candidate_id] = row
    if set(output) != wanted:
        raise TwoBookConfirmationError(f"component inventory incomplete: {sorted(wanted - set(output))}")
    return output


def _official_stats(client: Any) -> dict[str, Any]:
    resolved = client.symbology.resolve(
        dataset="GLBX.MDP3", symbols=list(SYMBOLS), stype_in="continuous",
        stype_out="instrument_id", start_date=START, end_date=END,
    )
    instrument_ids = sorted(
        {str(item["s"]) for rows in dict(resolved.get("result") or {}).values() for item in rows},
        key=int,
    )
    requests = {
        "ohlcv-1m": {"dataset": "GLBX.MDP3", "schema": "ohlcv-1m", "symbols": list(SYMBOLS), "stype_in": "continuous", "start": START, "end": END},
        "definition": {"dataset": "GLBX.MDP3", "schema": "definition", "symbols": instrument_ids, "stype_in": "instrument_id", "start": START, "end": END},
    }
    parts: dict[str, Any] = {}
    for name, request in requests.items():
        parts[name] = {
            "estimated_cost_usd": float(client.metadata.get_cost(**request)),
            "record_count": int(client.metadata.get_record_count(**request)),
            "billable_size_bytes": int(client.metadata.get_billable_size(**request)),
        }
    return {
        "requests": requests,
        "instrument_ids": instrument_ids,
        "parts": parts,
        "total_estimated_cost_usd": sum(float(row["estimated_cost_usd"]) for row in parts.values()),
        "total_record_count": sum(int(row["record_count"]) for row in parts.values()),
        "total_billable_size_bytes": sum(int(row["billable_size_bytes"]) for row in parts.values()),
    }


def _patch_acquisition() -> None:
    acquisition.START = START
    acquisition.END = END
    acquisition.SYMBOLS = SYMBOLS
    # Several micro contracts did not exist in H1-2019 and Databento's
    # continuous resolver legitimately maps those ambiguous roots to unrelated
    # legacy spreads.  They remain in the immutable raw request, but only
    # outright signal roots may enter the explicit roll map.  Frozen account
    # replay already prices MES/MYM from ES/YM and applies the micro contract
    # multiplier, exactly as in development.
    acquisition.ROOTS = ROOTS
    acquisition.CUMULATIVE_HARD_CAP_USD = CAP
    acquisition.REQUEST_PURPOSE = PURPOSE
    acquisition.CANDIDATE_TIER = "TIER_G_TWO_BOOK_AWAITING_ONE_SHOT_CONFIRMATION"
    acquisition.build_current_roll_map = _build_filtered_roll_map
    acquisition._normalize_confirmation_ohlcv = _normalize_full_bank_ohlcv


def _build_filtered_roll_map(**kwargs: Any) -> Any:
    """Build the roll map only from outright roots that existed in H1-2019."""

    from hydra.data.current_contract_map import build_current_roll_map

    value = dict(kwargs)
    value["roots"] = SIGNAL_ROOTS
    return build_current_roll_map(**value)


def _normalize_full_bank_ohlcv(frame: Any, *, roll_map: Any) -> tuple[Any, dict[str, Any]]:
    """Drop non-outright/pre-launch aliases before causal feature materialisation."""

    source = frame.reset_index()
    if "symbol" not in source.columns:
        if "instrument_id" not in source.columns:
            raise TwoBookConfirmationError("raw OHLCV has no instrument identity")
        source = source.rename(columns={"instrument_id": "symbol"})
    valid_ids = {
        str(row.instrument_id)
        for row in roll_map.contracts
        if row.instrument_id is not None
    }
    raw_symbols = source["symbol"].astype(str)
    accepted = raw_symbols.isin(valid_ids)
    rejected_ids = sorted(set(raw_symbols.loc[~accepted]), key=lambda value: int(value))
    if not accepted.any():
        raise TwoBookConfirmationError("no outright active contract remains after filtering")
    filtered = source.loc[accepted].copy()
    symbol_map = {
        **{str(row.instrument_id): str(row.root) for row in roll_map.contracts if row.instrument_id is not None},
        **{str(row.contract): str(row.root) for row in roll_map.contracts},
    }
    normalized = acquisition.normalize_ohlcv_frame(
        filtered, symbol=None, timeframe="1m", symbol_map=symbol_map
    )
    timestamps = acquisition.pd.to_datetime(normalized["timestamp"], utc=True)
    if timestamps.min() < acquisition.pd.Timestamp(START, tz="UTC") or timestamps.max() >= acquisition.pd.Timestamp(END, tz="UTC"):
        raise TwoBookConfirmationError("downloaded bars escape frozen half-open dates")
    observed = set(normalized["symbol"].astype(str))
    if observed != set(SIGNAL_ROOTS):
        raise TwoBookConfirmationError(
            f"outright signal roots incomplete after spread filter: {sorted(observed)}"
        )
    validation = acquisition.validate_ohlcv_frame(normalized, timeframe="1m")
    return normalized, {
        **dict(validation),
        "requested_roots": list(ROOTS),
        "accepted_outright_roots": list(SIGNAL_ROOTS),
        "rejected_instrument_ids": rejected_ids,
        "rejected_record_count": int((~accepted).sum()),
        "classification": "NON_OUTRIGHT_OR_PRELAUNCH_ALIAS_IGNORED_BEFORE_ROLL_MAP",
        "execution_alias_contract": {
            "MES": "ES_PRICE_WITH_FROZEN_MICRO_MULTIPLIER",
            "MYM": "YM_PRICE_WITH_FROZEN_MICRO_MULTIPLIER",
        },
    }


def freeze(client: Any) -> dict[str, Any]:
    untouched = _assert_untouched()
    source = _verify_result(SOURCE_RESULT_PATH)
    if tuple(row["policy_id"] for row in source["policy_results"] if row["decision"] == "G") != BOOK_IDS:
        raise TwoBookConfirmationError("the exact two-book Tier-G inventory changed")
    context, semantic, pass_bank = marginal._load_sources(ROOT)
    semantic_books = {str(row["policy_id"]): dict(row) for row in semantic["book_results"]}
    source_rows = {str(row["policy_id"]): dict(row) for row in source["policy_results"]}
    selected_books: list[dict[str, Any]] = []
    component_ids: set[str] = set()
    for policy_id in BOOK_IDS:
        row = semantic_books[policy_id]
        audit = source_rows[policy_id]
        if str(row["policy_spec_hash"]) != str(audit["policy_spec_hash"]):
            raise TwoBookConfirmationError("book policy specification drift")
        if str(dict(row["governor_policy"])["policy_id"]) != policy_id:
            raise TwoBookConfirmationError("book governor identity drift")
        component_ids.update(str(value) for value in row["component_ids"])
        selected_books.append({
            "policy_id": policy_id,
            "policy_spec_hash": row["policy_spec_hash"],
            "behavioral_fingerprint": audit["behavioral_fingerprint"],
            "component_ids": list(row["component_ids"]),
            "component_quantity_tiers": dict(row["component_quantity_tiers"]),
            "governor_profile_id": row["governor_profile_id"],
            "governor_policy": dict(row["governor_policy"]),
            "account_label": row["account_label"],
            "development_result_hash": audit["result_hash"],
            "graduation_horizon_trading_days": int(audit["graduation_horizon_trading_days"]),
            "prior_evidence_tier": "G",
        })

    manifest = _read(FAST_PASS_MANIFEST_PATH)
    bindings = dict(dict(manifest["data"])["feature_matrix_bindings"])
    matrices = {market: lane._open_bound_matrix(ROOT, bindings, market) for market in ("ES", "YM")}
    evaluation_start_ns = lane._date_ns(str(dict(manifest["data"])["evaluation_start_inclusive"]))
    entries = _candidate_entries()
    components: list[dict[str, Any]] = []
    for candidate_id in sorted(component_ids):
        candidate = HazardCandidate(**dict(entries[candidate_id]["candidate"]))
        matrix = matrices[candidate.market]
        if candidate.cross_asset_reference_market:
            matrix = lane.with_availability_safe_cross_asset_feature(matrix, matrices[candidate.cross_asset_reference_market])
        calibrated = calibrate_candidate(candidate, matrix, calibration_end_exclusive_ns=evaluation_start_ns, minimum_observations=100)
        prepared = context.components[candidate_id]
        components.append({
            "candidate_id": candidate_id,
            "candidate": candidate.payload,
            "candidate_fingerprint": candidate.structural_fingerprint,
            "calibration": lane._calibration_payload(calibrated),
            "integer_quantity_tier": int(prepared.integer_quantity_tier),
            "declared_risk_charge_per_mini": float(prepared.declared_risk_charge_per_mini),
            "development_component_receipt": dict(prepared.source_receipt),
        })

    stats = _official_stats(client)
    _estimated, current_actual = cumulative_spend(ROOT / "reports/data_budget/databento_spend_ledger.jsonl")
    request_core = {
        "dataset": "GLBX.MDP3", "schema": "ohlcv-1m", "symbols": list(SYMBOLS),
        "stype_in": "continuous", "stype_out": "instrument_id", "start": START,
        "end": END, "date_interval": "HALF_OPEN", "data_role": "CONFIRMATION",
        "q4_2024_access_allowed": False, "broker_or_order_capability": False,
    }
    request = {
        **request_core,
        "request_hash": stable_hash(request_core),
        "frozen_estimated_cost_usd": stats["total_estimated_cost_usd"],
        "prior_cumulative_actual_usd": current_actual,
        "additional_authority_usd": 100.0,
        "cumulative_hard_cap_usd": CAP,
        "projected_cumulative_usd": current_actual + stats["total_estimated_cost_usd"],
    }
    rule_wrapper = _read(RULE_SNAPSHOT_PATH)
    rules, rule_receipt = lane._load_rule_snapshot(RULE_SNAPSHOT_PATH)
    authorization_manifest = _read(AUTHORIZATION_MANIFEST_PATH)
    core = {
        "schema": "hydra_fresh_two_book_confirmation_2019h1_contract_v1",
        "status": "FROZEN_AWAITING_ACQUISITION",
        "decision_class": "NEW_PRE_DATA_TWO_BOOK_M1_CONFIRMATION_DECISION",
        "planner_minimum_three_heuristic_applied": False,
        "scientific_reason": "M1 requires one Tier-C candidate; two high-value immutable G hypotheses justify one shared packet",
        "books": selected_books,
        "components": components,
        "data_request": request,
        "official_cost_matrix": stats,
        "untouched_audit": untouched,
        "data_partition": {
            "role": "CONFIRMATION", "entire_post_warmup_block_consumed_once": True,
            "warmup_rule": "FIRST_5_COMMON_COMPLETE_SESSION_DAYS_EXCLUDED", "warmup_complete_sessions": 5,
            "candidate_modification_allowed": False, "recalibration_allowed": False,
        },
        "account_replay_contract": {
            "account_label": "50K", "horizons_trading_days": [5, 10, 20],
            "start_grid": "MAXIMUM_NON_OVERLAPPING_COMPLETE_WINDOWS_FROM_POST_WARMUP_CALENDAR",
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"], "normal_stress_multiplier": 1.5,
            "fill_semantics": "CAUSAL_NEXT_TRADABLE_OPEN_UNCHANGED", "exact_mll": True,
            "exact_consistency": True, "retuning_allowed": False,
        },
        "tier_c_gate": {
            "matching_horizon_days": 5, "minimum_normal_passes": 1,
            "minimum_stressed_passes": 1, "positive_stressed_net_required": True,
            "maximum_normal_mll_breach_rate": 0.10, "maximum_stressed_mll_breach_rate": 0.10,
            "all_passing_paths_consistency_compliant": True,
            "maximum_positive_day_share": 0.50, "maximum_positive_trade_share": 0.50,
            "maximum_positive_sleeve_share": 0.65, "no_retuning": True,
        },
        "official_rule_snapshot": rule_receipt,
        "official_rule_snapshot_payload": {"50K": dict(rules["50K"])},
        "source_bindings": {
            "marginal_result_path": str(SOURCE_RESULT_PATH.relative_to(ROOT)),
            "marginal_result_file_sha256": _sha256(SOURCE_RESULT_PATH),
            "marginal_result_hash": source["result_hash"],
            "semantic_result_hash": semantic["result_hash"],
            "observed_pass_bank_hash": pass_bank["result_hash"],
            "fast_pass_manifest_hash": manifest["manifest_hash"],
            "authorization_manifest_hash": authorization_manifest["manifest_hash"],
            "rule_snapshot_file_sha256": _sha256(RULE_SNAPSHOT_PATH),
            "rule_snapshot_payload_hash": stable_hash(rule_wrapper),
        },
        "broker_connections": 0, "orders": 0, "q4_access_count_delta": 0,
        "authoritative_database_writes": 0,
    }
    contract = {**core, "contract_hash": stable_hash(core)}
    _write_once(CONTRACT_PATH, contract)
    return contract


def acquire(client: Any, *, execute: bool) -> dict[str, Any]:
    _assert_untouched()
    contract = _read(CONTRACT_PATH)
    manifest = _read(AUTHORIZATION_MANIFEST_PATH)
    _patch_acquisition()
    return acquisition.acquire_fresh_confirmation(
        contract=contract,
        manifest=manifest,
        expected_manifest_hash=str(manifest["manifest_hash"]),
        root=ROOT,
        client=client,
        execute=execute,
        budget=DatabentoBudgetConfig(hard_cap_usd=CAP, safety_ceiling_usd=CAP),
        receipt_path=RECEIPT_PATH,
    )


def build_features() -> dict[str, Any]:
    contract = _read(CONTRACT_PATH)
    receipt = _read(RECEIPT_PATH)
    if stable_hash({k: v for k, v in contract.items() if k != "contract_hash"}) != str(contract["contract_hash"]):
        raise TwoBookConfirmationError("contract hash drift before feature build")
    if stable_hash({k: v for k, v in receipt.items() if k != "receipt_hash"}) != str(receipt["receipt_hash"]):
        raise TwoBookConfirmationError("acquisition receipt hash drift")
    source_binding = dict(receipt["feature_build_inputs"])["source_files"][0]
    source_path = Path(str(source_binding["path"]))
    map_path = Path(str(dict(receipt["feature_build_inputs"])["contract_map_path"]))
    if _sha256(source_path) != str(source_binding["sha256"]) or _sha256(map_path) != str(dict(receipt["feature_build_inputs"])["contract_map_sha256"]):
        raise TwoBookConfirmationError("acquired feature input hash drift")
    roll_map = load_roll_map(map_path)
    definition_path = Path(next(row["path"] for row in receipt["files"] if row["kind"] == "RAW_DBN_DEFINITIONS"))
    definition_sha = next(row["sha256"] for row in receipt["files"] if row["kind"] == "RAW_DBN_DEFINITIONS")
    if _sha256(definition_path) != str(definition_sha):
        raise TwoBookConfirmationError("raw definition hash drift")
    definitions = acquisition._store_frame(
        acquisition._default_dbn_store_loader(definition_path),
        price_type=None,
        map_symbols=False,
    )
    definition_checks: list[dict[str, Any]] = []
    for contract_row in roll_map.contracts:
        selected = definitions.loc[
            definitions["instrument_id"].astype(str).eq(str(contract_row.instrument_id))
            & definitions["raw_symbol"].astype(str).eq(str(contract_row.contract))
        ]
        if selected.empty:
            raise TwoBookConfirmationError(f"definition absent for {contract_row.contract}")
        latest = selected.iloc[-1]
        checks = {
            "valid_outright_future_symbol": valid_outright_future_symbol(str(contract_row.root), str(contract_row.contract)),
            "instrument_class_f": str(latest.get("instrument_class") or "") == "F",
            "security_type_fut": str(latest.get("security_type") or "") == "FUT",
            "asset_equals_root": str(latest.get("asset") or "") == str(contract_row.root),
        }
        if not all(checks.values()):
            raise TwoBookConfirmationError(f"non-outright definition entered roll map: {contract_row.contract}")
        definition_checks.append({
            "root": contract_row.root,
            "contract": contract_row.contract,
            "instrument_id": contract_row.instrument_id,
            "checks": checks,
        })
    roots_in_map = {str(row.root) for row in roll_map.contracts}
    if roots_in_map != set(SIGNAL_ROOTS):
        raise TwoBookConfirmationError("roll-map roots are not the exact SIGNAL_ROOTS set")
    segment_coverage: dict[str, Any] = {}
    for root in SIGNAL_ROOTS:
        rows = sorted((row for row in roll_map.contracts if str(row.root) == root), key=lambda row: str(row.active_start))
        if not rows or str(rows[0].active_start) > START or str(rows[-1].active_end) < END:
            raise TwoBookConfirmationError(f"roll-map segment coverage incomplete: {root}")
        gaps = [
            (str(left.active_end), str(right.active_start))
            for left, right in zip(rows, rows[1:])
            if str(left.active_end) != str(right.active_start)
        ]
        if gaps:
            raise TwoBookConfirmationError(f"roll-map segment gap or overlap: {root} {gaps}")
        segment_coverage[root] = {
            "first_active_start": str(rows[0].active_start),
            "last_active_end": str(rows[-1].active_end),
            "segment_count": len(rows),
            "gaps": [],
        }
    frame = lane.pd.read_parquet(source_path)
    normalized = lane.normalize_ohlcv_frame(
        frame,
        symbol=None,
        timeframe="1m",
        symbol_map={root: root for root in SIGNAL_ROOTS},
    )
    lane.validate_ohlcv_frame(normalized, timeframe="1m")
    observed = set(normalized["symbol"].astype(str))
    if observed != set(SIGNAL_ROOTS):
        raise TwoBookConfirmationError("normalized outright signal-root inventory drift")
    mapped, map_guard = lane._apply_explicit_contract_map(
        normalized,
        roll_map,
        required_map_type=roll_map.map_type,
    )
    featured = lane._prepare_feature_frame(lane._build_past_only_feature_frame(mapped))
    source_hash = stable_hash(
        [{"path": str(source_path), "sha256": str(source_binding["sha256"]), "rows": int(len(normalized))}]
    )
    map_hash = _sha256(map_path)
    store = lane.CanonicalFeatureStore(dict(receipt["feature_build_inputs"])["cache_root"])
    bundles: dict[str, Any] = {}
    for market in ("ES", "YM"):
        market_frame = featured.loc[featured["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = lane._market_arrays(market_frame, market)
        arrays.pop("entry_price", None)
        for key in tuple(arrays):
            if key.startswith("forward_move__"):
                arrays.pop(key)
        key = lane.CanonicalFeatureKey(
            market=market,
            explicit_contract_scope=f"{market}_date_aware_active_contracts_two_book_confirmation",
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
                "contract_hash": contract["contract_hash"],
                "request_hash": dict(contract["data_request"])["request_hash"],
                "market": market,
                "execution_market": "MES" if market == "ES" else "MYM",
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
        "schema": "hydra_fresh_two_book_confirmation_feature_receipt_v1",
        "status": "CAUSAL_FEATURE_BUNDLES_READY",
        "contract_hash": contract["contract_hash"],
        "acquisition_receipt_hash": receipt["receipt_hash"],
        "source_path": str(source_path),
        "source_sha256": str(source_binding["sha256"]),
        "contract_map_path": str(map_path),
        "contract_map_sha256": map_hash,
        "roll_map_hash": roll_map.roll_map_hash(),
        "map_guard": map_guard,
        "outright_definition_checks": definition_checks,
        "segment_coverage": segment_coverage,
        "bundles": bundles,
        "decision_markets": ["ES", "YM"],
        "micro_proxy_feature_reads": 0,
        "micro_execution_aliases_are_accounting_only": True,
        "raw_requested_symbol_count": len(SYMBOLS),
        "rejected_non_outight_record_count": int(dict(receipt["normalization"])["rejected_record_count"]),
        "data_role": "CONFIRMATION",
        "q4_access_count_delta": 0,
    }
    value = {**core, "result_hash": stable_hash(core)}
    _write_once(FEATURE_RECEIPT_PATH, value)
    return value


def _open_features(receipt: Mapping[str, Any]) -> dict[str, Any]:
    core = dict(receipt)
    claimed = str(core.pop("result_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise TwoBookConfirmationError("feature receipt hash drift")
    output: dict[str, Any] = {}
    for market, raw in dict(receipt["bundles"]).items():
        matrix = lane.FeatureMatrix.open(str(dict(raw)["path"]), mmap=True)
        if matrix.fingerprint != str(dict(raw)["bundle_hash"]):
            raise TwoBookConfirmationError("feature matrix fingerprint drift")
        output[str(market)] = matrix
    if set(output) != {"ES", "YM"}:
        raise TwoBookConfirmationError("decision matrix inventory drift")
    return output


def _profit_share(values: list[float]) -> float:
    positive = [max(float(value), 0.0) for value in values]
    denominator = sum(positive)
    return max(positive, default=0.0) / denominator if denominator > 0.0 else 1.0


def _confirmation_concentration(
    members: list[str],
    scenario_trajectories: Mapping[str, Mapping[str, tuple[Any, ...]]],
    episode_sets: Mapping[str, Mapping[int, list[tuple[Any, str]]]],
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        selected = list(episode_sets[scenario][5])
        days: dict[int, float] = {}
        trades: list[float] = []
        sleeves: dict[str, float] = defaultdict(float)
        accepted: set[str] = set()
        source = {
            trajectory.event.event_id: trajectory
            for member in members
            for trajectory in scenario_trajectories[scenario][member]
        }
        for episode, _block in selected:
            for day in episode.daily_path:
                session_day = int(day["session_day"])
                if session_day in days:
                    raise TwoBookConfirmationError("non-overlapping P5 replay reused a day")
                days[session_day] = float(day["day_pnl"])
                for member, amount in dict(day["component_attribution"]).items():
                    sleeves[str(member)] += float(amount)
            for decision in episode.risk_allocation_path:
                if int(decision.get("quantity", 0)) <= 0:
                    continue
                event_id = str(decision["event_id"])
                if event_id in accepted:
                    raise TwoBookConfirmationError("non-overlapping P5 replay reused a trade")
                accepted.add(event_id)
                trajectory = source[event_id]
                ratio = int(decision["quantity"]) / max(int(trajectory.event.quantity), 1)
                trades.append(float(trajectory.event.net_pnl) * ratio)
        row = {
            "denominator": "UNIQUE_NONOVERLAPPING_CONFIRMATION_P5_EPISODES",
            "episode_count": len(selected),
            "unique_day_count": len(days),
            "unique_accepted_trade_count": len(trades),
            "maximum_single_day_positive_profit_share": _profit_share(list(days.values())),
            "maximum_single_trade_positive_profit_share": _profit_share(trades),
            "maximum_single_sleeve_positive_profit_share": _profit_share(list(sleeves.values())),
            "net_by_sleeve_usd": dict(sorted(sleeves.items())),
            "accepted_event_inventory_hash": stable_hash(sorted(accepted)),
            "daily_pnl_hash": stable_hash(dict(sorted(days.items()))),
        }
        row["cleared"] = bool(
            row["maximum_single_day_positive_profit_share"] <= 0.50
            and row["maximum_single_trade_positive_profit_share"] <= 0.50
            and row["maximum_single_sleeve_positive_profit_share"] <= 0.65
        )
        output[scenario] = row
    return output


def evaluate() -> dict[str, Any]:
    if RESULT_PATH.exists():
        raise TwoBookConfirmationError("W2019H1 confirmation was already consumed")
    contract = _read(CONTRACT_PATH)
    receipt = _read(RECEIPT_PATH)
    feature_receipt = _read(FEATURE_RECEIPT_PATH)
    matrices = _open_features(feature_receipt)
    lane.START = START
    lane.END = END
    calendar = lane._post_warmup_common_calendar(matrices)
    starts = lane.non_overlapping_starts(calendar, (5, 10, 20))
    component_rows = {str(row["candidate_id"]): dict(row) for row in contract["components"]}
    replays: dict[str, Any] = {}
    scaled: dict[str, dict[str, tuple[Any, ...]]] = {"NORMAL": {}, "STRESSED_1_5X": {}}
    coverages: dict[str, Any] = {}
    component_receipts: list[dict[str, Any]] = []
    first_ns = int(calendar[0]) * 86_400_000_000_000
    end_ns = (int(calendar[-1]) + 1) * 86_400_000_000_000
    for candidate_id in sorted(component_rows):
        frozen = component_rows[candidate_id]
        candidate = HazardCandidate(**dict(frozen["candidate"]))
        matrix = matrices[candidate.market]
        if candidate.cross_asset_reference_market:
            matrix = lane.with_availability_safe_cross_asset_feature(matrix, matrices[candidate.cross_asset_reference_market])
        calibrated = lane._calibrated_from_payload(candidate, frozen["calibration"])
        intents = discover_intents_batch(calibrated, matrix, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns)
        streaming = discover_intents_streaming(calibrated, matrix, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns)
        if tuple((row.row_index, row.direction) for row in intents) != streaming:
            raise TwoBookConfirmationError(f"batch/stream decision mismatch: {candidate_id}")
        events = observe_outcomes(calibrated, matrix, intents)
        eligible_days = frozen_eligible_session_calendar(candidate, matrix, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns)
        replay = exact_sleeve_replay(calibrated, events, eligible_session_days=eligible_days)
        normal, normal_violations = lane._apply_session_contract(replay.normal_trajectories)
        stressed, stress_violations = lane._apply_session_contract(replay.stressed_trajectories)
        if normal_violations or stress_violations:
            raise TwoBookConfirmationError(f"session contract violation: {candidate_id}")
        quantity = int(frozen["integer_quantity_tier"])
        scaled["NORMAL"][candidate_id] = tuple(scale_causal_trajectory(row, executable_quantity_multiplier=quantity) for row in normal)
        scaled["STRESSED_1_5X"][candidate_id] = tuple(scale_causal_trajectory(row, executable_quantity_multiplier=quantity) for row in stressed)
        lane._require_scenario_identity(scaled["NORMAL"][candidate_id], scaled["STRESSED_1_5X"][candidate_id])
        coverage = lane._candidate_coverage(replay, calendar, starts)
        replays[candidate_id] = replay
        coverages[candidate_id] = coverage
        component_receipts.append({
            "candidate_id": candidate_id,
            "candidate_fingerprint": candidate.structural_fingerprint,
            "calibration_hash": calibrated.fingerprint,
            "batch_stream_equal": True,
            "emitted_intent_count": len(intents),
            "completed_event_count": len(replay.normal_trajectories),
            "censored_event_count": sum(str(row.outcome) == HazardOutcome.CENSORED_FUTURE_COVERAGE.value for row in replay.events),
            "normal_trajectory_hash": replay.normal_trajectory_hash,
            "stressed_trajectory_hash": replay.stressed_trajectory_hash,
            "fill_policy_hash": replay.fill_policy_hash,
            "coverage": coverage["receipt"],
        })

    rule = dict(contract["official_rule_snapshot_payload"])["50K"]
    config = lane._account_config(rule)
    book_results: list[dict[str, Any]] = []
    for frozen_book in contract["books"]:
        book = dict(frozen_book)
        members = [str(value) for value in book["component_ids"]]
        policy = ActiveRiskPoolPolicy.from_mapping(book["governor_policy"])
        if stable_hash(policy.to_dict()) != stable_hash(book["governor_policy"]):
            raise TwoBookConfirmationError("frozen governor policy reconstruction drift")
        episode_sets: dict[str, dict[int, list[tuple[Any, str]]]] = {
            scenario: {horizon: [] for horizon in (5, 10, 20)}
            for scenario in ("NORMAL", "STRESSED_1_5X")
        }
        censored = {scenario: {horizon: 0 for horizon in (5, 10, 20)} for scenario in episode_sets}
        cells: list[dict[str, Any]] = []
        for horizon in (5, 10, 20):
            allowed = set(coverages[members[0]]["starts"][horizon])
            for member in members[1:]:
                allowed &= set(coverages[member]["starts"][horizon])
            ordered_allowed = [row for row in starts[horizon] if row in allowed]
            for scenario in episode_sets:
                censored[scenario][horizon] = len(starts[horizon]) - len(ordered_allowed)
                for start_day, block in ordered_allowed:
                    episode_sets[scenario][horizon].append((
                        run_causal_shared_account_episode(
                            {member: scaled[scenario][member] for member in members},
                            calendar,
                            policy=policy,
                            start_day=int(start_day),
                            maximum_duration_days=int(horizon),
                            config=config,
                        ),
                        str(block),
                    ))
            normal_summary = _summarize_sprint_episodes(
                episode_sets["NORMAL"][horizon],
                requested_start_count=len(starts[horizon]),
                data_censored_count=censored["NORMAL"][horizon],
            )
            stress_summary = _summarize_sprint_episodes(
                episode_sets["STRESSED_1_5X"][horizon],
                requested_start_count=len(starts[horizon]),
                data_censored_count=censored["STRESSED_1_5X"][horizon],
            )
            cells.append({
                "horizon_trading_days": horizon,
                "requested_start_count": len(starts[horizon]),
                "full_coverage_start_count": len(ordered_allowed),
                "data_censored_start_count": len(starts[horizon]) - len(ordered_allowed),
                "normal": normal_summary,
                "stressed": stress_summary,
                "episodes": {
                    scenario: [
                        {
                            "start_day": int(episode.start_day),
                            "terminal_status": str(episode.terminal_reason),
                            "passed": bool(episode.passed),
                            "mll_breached": bool(episode.mll_breached),
                            "consistency_ok": bool(episode.consistency_ok),
                            "net_pnl_usd": float(episode.net_pnl),
                            "minimum_mll_buffer_usd": float(episode.minimum_mll_buffer),
                            "days_to_target": episode.days_to_target,
                        }
                        for episode, _block in episode_sets[scenario][horizon]
                    ]
                    for scenario in episode_sets
                },
            })
        concentration = _confirmation_concentration(members, scaled, episode_sets)
        matching = next(row for row in cells if int(row["horizon_trading_days"]) == int(book["graduation_horizon_trading_days"]))
        normal = dict(matching["normal"])
        stressed = dict(matching["stressed"])
        normal_passing = [episode for episode, _ in episode_sets["NORMAL"][5] if episode.passed]
        stress_passing = [episode for episode, _ in episode_sets["STRESSED_1_5X"][5] if episode.passed]
        checks = {
            "matching_horizon_is_frozen_5d": int(book["graduation_horizon_trading_days"]) == 5,
            "normal_pass_observed": int(normal["pass_count"]) >= 1,
            "stressed_pass_observed": int(stressed["pass_count"]) >= 1,
            "positive_stressed_economics": float(stressed["net_total"]) > 0.0,
            "normal_mll_controlled": float(normal["mll_breach_rate"]) <= 0.10,
            "stressed_mll_controlled": float(stressed["mll_breach_rate"]) <= 0.10,
            "normal_passing_consistency": bool(normal_passing) and all(row.consistency_ok for row in normal_passing),
            "stressed_passing_consistency": bool(stress_passing) and all(row.consistency_ok for row in stress_passing),
            "normal_concentration_cleared": bool(concentration["NORMAL"]["cleared"]),
            "stressed_concentration_cleared": bool(concentration["STRESSED_1_5X"]["cleared"]),
            "full_coverage": int(matching["full_coverage_start_count"]) > 0,
        }
        passed = all(checks.values())
        core = {
            "policy_id": book["policy_id"],
            "policy_spec_hash": book["policy_spec_hash"],
            "component_ids": members,
            "governor_profile_id": book["governor_profile_id"],
            "behavioral_fingerprint": book["behavioral_fingerprint"],
            "cells": cells,
            "concentration": concentration,
            "tier_c_gate": {"passed": passed, "checks": checks, "reason": "TIER_C_GATE_PASSED" if passed else "TIER_C_GATE_FAILED"},
            "tier_c_promoted": passed,
            "evidence_tier": "C" if passed else "G_CONFIRMATION_FAILED_BRANCH_CLOSED",
            "retuning_performed": False,
            "recalibration_performed": False,
            "book_or_component_mutated": False,
        }
        book_results.append({**core, "result_hash": stable_hash(core)})
    core = {
        "schema": "hydra_fresh_two_book_confirmation_2019h1_result_v1",
        "status": "CONFIRMATION_PACKET_CONSUMED_ONCE",
        "decision": "M1_TIER_C_REACHED" if any(row["tier_c_promoted"] for row in book_results) else "BOTH_EXACT_CANDIDATE_CONFIRMATION_BRANCHES_CLOSED",
        "contract_hash": contract["contract_hash"],
        "acquisition_receipt_hash": receipt["receipt_hash"],
        "feature_receipt_hash": feature_receipt["result_hash"],
        "calendar": {
            "period": f"{START}:{END}",
            "warmup_complete_sessions": 5,
            "post_warmup_session_count": len(calendar),
            "first_session_day": int(calendar[0]),
            "last_session_day": int(calendar[-1]),
            "non_overlapping_start_counts": {str(h): len(starts[h]) for h in (5, 10, 20)},
        },
        "component_receipts": component_receipts,
        "book_results": book_results,
        "tier_c_policy_ids": [row["policy_id"] for row in book_results if row["tier_c_promoted"]],
        "retuning_performed": False,
        "recalibration_performed": False,
        "packet_reuse_allowed": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    result = {**core, "result_hash": stable_hash(core)}
    _write_once(RESULT_PATH, result)
    report_core = {
        "schema": "hydra_fresh_two_book_confirmation_2019h1_decision_report_v1",
        "economic_verdict": core["decision"],
        "tier_c_policy_ids": core["tier_c_policy_ids"],
        "book_decisions": [
            {
                "policy_id": row["policy_id"],
                "tier_c_gate": row["tier_c_gate"],
                "cells": row["cells"],
                "concentration": row["concentration"],
                "terminal_branch_status": row["evidence_tier"],
            }
            for row in book_results
        ],
        "budget": {
            "incremental_actual_usd": receipt["actual_cost_usd"],
            "cumulative_actual_usd": receipt["cumulative_actual_usd"],
            "remaining_authorized_usd": CAP - float(receipt["cumulative_actual_usd"]),
        },
        "contract_hash": contract["contract_hash"],
        "economic_result_hash": result["result_hash"],
        "raw_data_sha256": next(row["sha256"] for row in receipt["files"] if row["kind"] == "RAW_DBN_OHLCV"),
        "no_retry_on_same_packet": True,
        "books_mutated": False,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    _write_once(DECISION_REPORT_PATH, {**report_core, "result_hash": stable_hash(report_core)})
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("freeze", "cost", "acquire", "features", "evaluate"))
    args = parser.parse_args()
    if args.action == "features":
        result = build_features()
    elif args.action == "evaluate":
        result = evaluate()
    else:
        key = load_api_key()
        if not key:
            raise TwoBookConfirmationError("DATABENTO_API_KEY is unavailable")
        client = db.Historical(key)
        result = freeze(client) if args.action == "freeze" else acquire(client, execute=args.action == "acquire")
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
