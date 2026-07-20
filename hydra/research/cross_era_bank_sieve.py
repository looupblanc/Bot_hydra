"""Bounded W2019H1 development sieve for the immutable operational bank.

W2019H1 has already been consumed by two exact book confirmations.  This
module therefore treats the packet only as ``VIEWED_DEVELOPMENT`` and never
claims confirmation.  It freezes the complete policy inventory, calibrations,
risk profiles, account grid, and selection rule before computing any candidate
outcome.  Two process workers then reuse the existing causal discovery and
exact chronological account replay primitives.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.data.contract_mapping import load_roll_map
from hydra.economic_evolution.schema import stable_hash
from hydra.production import fresh_confirmation_lane as lane
from hydra.production import marginal_book_tier_g_batch as marginal
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _require_scenario_identity,
)
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
from hydra.research.near_pass_bank_salvage import (
    _cell_fingerprint,
    _source_candidates,
)
from hydra.research.pnl_state_risk_frontier import (
    _PreparedPolicy,
    _evaluate_profile,
    frozen_pnl_state_profiles,
)


SCHEMA = "hydra_cross_era_bank_sieve_2019h1_v1"
ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path("reports/economic_evolution/cross_era_bank_sieve_2019h1_v1")
CONTRACT_NAME = "selection_contract.json"
FEATURE_NAME = "feature_receipt.json"
RESULT_NAME = "economic_result.json"
COHORT_NAME = "development_requalified_cohort.json"
STATE_NAME = "production_state.json"

OPERATIONAL_MATRIX = Path(
    "reports/economic_evolution/operational_candidate_bank_v2/lifecycle_matrix.json"
)
OPERATIONAL_SUMMARY = Path(
    "reports/economic_evolution/operational_candidate_bank_v2/bank_summary.json"
)
PASS_BANK = Path(
    "reports/economic_evolution/autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/combine_pass_observed_bank.json"
)
CANDIDATE_BANK = Path(
    "reports/economic_evolution/autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/combine_candidate_bank.json"
)
MARGINAL_BOOKS = Path(
    "reports/economic_evolution/autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/marginal_books_composite.json"
)
NEAR_PASS_RESULT = Path(
    "reports/economic_evolution/near_pass_bank_salvage_v1/economic_result.json"
)
W2019_ACQUISITION = Path(
    "reports/economic_evolution/fresh_two_book_confirmation_2019h1_v1/"
    "acquisition_receipt.json"
)
W2019_RESULT = Path(
    "reports/economic_evolution/fresh_two_book_confirmation_2019h1_v1/"
    "economic_result.json"
)

START = "2019-01-02"
END = "2019-07-01"
HORIZONS = (5, 10, 20)
ACCOUNT_LABELS = ("50K", "100K", "150K")
MARKETS = ("CL", "ES", "NQ", "RTY", "YM")
WARMUP_COMPLETE_SESSIONS = 5
NEW_POLICY_ID = "hazard_21a565329d4036480effad80"
CLOSED_EXACT_BOOKS = frozenset(
    {
        "autonomous_marginal_book_2f3752128ff0fd44a71b2327",
        "autonomous_marginal_book_b09b8e7b30f90b34737eb724",
    }
)
QUARANTINED_IDS = frozenset(
    {
        "hazard_01340d634a288f435a06760c",
        "hazard_020ae195ccef8e39b1907e38",
        "hazard_0a569f580a2540474116636c",
        "hazard_16a744e747cafb88a7e2c83b",
        "hazard_1f3ff3e1f6d2b9d5e8eec1b3",
        "hazard_2afe13b4c912d4aa7f238626",
    }
)
PROFILE_BY_ID = {row.profile_id: row for row in frozen_pnl_state_profiles()}
FEATURE_VERSION = "hydra_cross_era_2019h1_causal_feature_bundle_v1"


class CrossEraSieveError(RuntimeError):
    """The immutable cross-era replay contract cannot be satisfied."""


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CrossEraSieveError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write(path: Path, value: Mapping[str, Any], *, immutable: bool = False) -> None:
    text = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if immutable and path.is_file():
        if path.read_text(encoding="utf-8") != text:
            raise CrossEraSieveError(f"refusing divergent immutable rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _unwrap(path: Path, key: str) -> dict[str, Any]:
    wrapper = _read(path)
    value = wrapper.get(key)
    if not isinstance(value, Mapping):
        raise CrossEraSieveError(f"missing {key}: {path}")
    return dict(value)


def _bank_entries() -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    base = ROOT / "data/cache/economic_production/hydra_fast_pass_factory_0029"
    for path in sorted(base.glob("wave_*/causal_executable_bank.json")):
        for raw in _read(path).get("entries", ()):
            row = dict(raw)
            candidate_id = str(row.get("candidate_id") or "")
            prior = output.get(candidate_id)
            if prior is not None and stable_hash(prior["candidate"]) != stable_hash(
                row["candidate"]
            ):
                raise CrossEraSieveError(f"candidate semantic drift: {candidate_id}")
            output[candidate_id] = row
    return output


def _full_exact_cell(
    exact: Mapping[str, Any], compact: Mapping[str, Any]
) -> dict[str, Any]:
    wanted = str(compact.get("cell_hash") or "")
    matches = [
        dict(cell)
        for cell in exact.get("frontier", ())
        if _cell_fingerprint(cell) == wanted
    ]
    if len(matches) != 1:
        raise CrossEraSieveError(
            f"canonical exact cell absent or duplicated: {exact.get('candidate_id')}"
        )
    return matches[0]


def _original_calibrations(
    candidate_ids: Sequence[str], entries: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    manifest = _load_self_hashed_manifest(ROOT / DEFAULT_FAST_PASS_MANIFEST)
    bindings = dict(dict(manifest["data"])["feature_matrix_bindings"])
    matrices = {
        market: lane._open_bound_matrix(ROOT, bindings, market) for market in MARKETS
    }
    evaluation_start_ns = lane._date_ns(
        str(dict(manifest["data"])["evaluation_start_inclusive"])
    )
    output: dict[str, dict[str, Any]] = {}
    for candidate_id in sorted(set(candidate_ids)):
        entry = dict(entries.get(candidate_id) or {})
        if not entry:
            raise CrossEraSieveError(f"causal executable entry absent: {candidate_id}")
        candidate = HazardCandidate(**dict(entry["candidate"]))
        if candidate.candidate_id != candidate_id:
            raise CrossEraSieveError(f"candidate fingerprint drift: {candidate_id}")
        matrix = matrices[candidate.market]
        if candidate.cross_asset_reference_market:
            matrix = lane.with_availability_safe_cross_asset_feature(
                matrix, matrices[candidate.cross_asset_reference_market]
            )
        calibrated = calibrate_candidate(
            candidate,
            matrix,
            calibration_end_exclusive_ns=evaluation_start_ns,
            minimum_observations=100,
        )
        output[candidate_id] = {
            "candidate": candidate.payload,
            "candidate_fingerprint": candidate.structural_fingerprint,
            "calibration": lane._calibration_payload(calibrated),
            "development_realized_behavioral_fingerprint": entry.get(
                "realized_behavioral_fingerprint"
            ),
        }
    return output


def freeze_contract(output_dir: Path) -> dict[str, Any]:
    matrix = _read(ROOT / OPERATIONAL_MATRIX)
    summary = _read(ROOT / OPERATIONAL_SUMMARY)
    pass_bank = _unwrap(ROOT / PASS_BANK, "combine_pass_observed_bank")
    candidate_bank = _unwrap(ROOT / CANDIDATE_BANK, "candidate_bank")
    marginal_books = _unwrap(ROOT / MARGINAL_BOOKS, "marginal_book_composite")
    near_pass = _read(ROOT / NEAR_PASS_RESULT)
    w2019 = _read(ROOT / W2019_RESULT)
    if str(w2019.get("status")) != "CONFIRMATION_PACKET_CONSUMED_ONCE":
        raise CrossEraSieveError("W2019H1 consumed packet status drift")

    rows = [dict(row) for row in matrix.get("rows", ())]
    base_rows = [
        row
        for row in rows
        if row.get("record_type") == "BASE_POLICY" and not row.get("quarantine")
    ]
    variant_rows = [
        row
        for row in rows
        if row.get("record_type") == "DYNAMIC_ACCOUNT_POLICY_VARIANT"
        and not row.get("quarantine")
    ]
    if len(base_rows) != 44 or len(variant_rows) != 3:
        raise CrossEraSieveError("operational V2 44-base/3-variant boundary drift")
    observed_quarantine = {
        str(row["base_policy_id"]) for row in rows if row.get("quarantine")
    }
    if observed_quarantine != QUARANTINED_IDS:
        raise CrossEraSieveError("six-policy quarantine boundary drift")
    recovered = [
        dict(row)
        for row in near_pass.get("recovered_strategies", ())
        if str(row.get("candidate_id")) == NEW_POLICY_ID
    ]
    if len(recovered) != 1:
        raise CrossEraSieveError("new hazard_21a565 recovery evidence absent")

    pass_rows = {str(row["policy_id"]): dict(row) for row in pass_bank["policies"]}
    classified = {
        str(row["candidate_id"]): dict(row)
        for row in candidate_bank.get("candidates", ())
    }
    exact_rows = _source_candidates(ROOT)
    semantic_books = {
        str(row["policy_id"]): dict(row)
        for row in marginal_books.get("book_results", ())
    }

    inventory_base_ids = [str(row["base_policy_id"]) for row in base_rows]
    if NEW_POLICY_ID in inventory_base_ids:
        raise CrossEraSieveError("new policy already entered operational V2 base")
    inventory_base_ids.append(NEW_POLICY_ID)
    if len(set(inventory_base_ids)) != 45:
        raise CrossEraSieveError("clean source inventory is not 45 bases")

    configs: list[dict[str, Any]] = []
    for row in base_rows:
        policy_id = str(row["base_policy_id"])
        configs.append(
            {
                "configuration_id": policy_id,
                "base_policy_id": policy_id,
                "record_type": "BASE_POLICY",
                "source_evidence_tier": row.get("evidence_tier"),
                "development_behavior_cluster_id": row.get("behavior_cluster_id"),
                "niches": dict(row.get("niches") or {}),
                "closed_exact_book_branch": policy_id in CLOSED_EXACT_BOOKS,
            }
        )
    configs.append(
        {
            "configuration_id": NEW_POLICY_ID,
            "base_policy_id": NEW_POLICY_ID,
            "record_type": "BASE_POLICY",
            "source_evidence_tier": "E",
            "development_behavior_cluster_id": recovered[0]["behavior_cluster_id"],
            "niches": dict(recovered[0]["niches"]),
            "closed_exact_book_branch": False,
        }
    )
    for row in variant_rows:
        configs.append(
            {
                "configuration_id": str(row["configuration_id"]),
                "base_policy_id": str(row["base_policy_id"]),
                "record_type": "DYNAMIC_ACCOUNT_POLICY_VARIANT",
                "source_evidence_tier": row.get("evidence_tier"),
                "development_behavior_cluster_id": row.get("behavior_cluster_id"),
                "niches": dict(row.get("niches") or {}),
                "closed_exact_book_branch": False,
            }
        )
    if len(configs) != 48:
        raise CrossEraSieveError("source inventory is not 48 configurations")

    component_ids: set[str] = set()
    frozen_configs: list[dict[str, Any]] = []
    profiles = {row.profile_id: row.to_dict() for row in frozen_pnl_state_profiles()}
    for config in configs:
        policy_id = str(config["base_policy_id"])
        if config["closed_exact_book_branch"]:
            frozen_configs.append(
                {
                    **config,
                    "replay_eligible": False,
                    "exclusion_reason": "EXACT_BOOK_CONFIRMATION_BRANCH_CONSUMED_AND_CLOSED",
                }
            )
            continue
        source = pass_rows.get(policy_id)
        kind = str(dict(source or {}).get("source_kind") or "EXACT_STANDALONE")
        if kind == "MARGINALLY_ACCEPTED_BOOK":
            book = semantic_books.get(policy_id)
            if not book:
                raise CrossEraSieveError(f"semantic book absent: {policy_id}")
            members = [str(value) for value in book["component_ids"]]
            component_ids.update(members)
            policy_payload = dict(book["governor_policy"])
            quantities = {
                str(key): int(value)
                for key, value in dict(book["component_quantity_tiers"]).items()
            }
            profile_id = "pnl_state_identity"
            policy_spec_hash = str(book["policy_spec_hash"])
        else:
            members = [policy_id]
            component_ids.add(policy_id)
            exact = exact_rows.get(policy_id)
            if not exact:
                raise CrossEraSieveError(f"exact candidate source absent: {policy_id}")
            if policy_id == NEW_POLICY_ID:
                account = next(
                    dict(row)
                    for row in near_pass["candidate_results"]
                    if str(row["candidate_id"]) == NEW_POLICY_ID
                )["account_results"]
                selected = next(
                    dict(row) for row in account if str(row["account_label"]) == "50K"
                )
                compact = {"cell_hash": selected["frozen_base_cell_hash"]}
                full_cell = _full_exact_cell(exact, compact)
                profile_id = str(selected["selected_profile_id"])
            else:
                classified_row = classified.get(policy_id)
                if not classified_row or not source:
                    raise CrossEraSieveError(f"classified bank row absent: {policy_id}")
                primary = str(dict(source["fingerprints"])["primary_evidence_cell"])
                compact = dict(classified_row.get(primary) or {})
                full_cell = _full_exact_cell(exact, compact)
                profile_id = (
                    "pnl_state_fast_ladder"
                    if config["record_type"] == "DYNAMIC_ACCOUNT_POLICY_VARIANT"
                    else "pnl_state_identity"
                )
            quantities = {policy_id: int(full_cell["integer_quantity_tier"])}
            policy_payload = dict(full_cell["account_policy"])
            policy_spec_hash = str(
                dict(_bank_entries()[policy_id])["candidate_fingerprint"]
            )
        if profile_id not in profiles:
            raise CrossEraSieveError(f"unknown frozen PnL profile: {profile_id}")
        policy = ActiveRiskPoolPolicy.from_mapping(policy_payload)
        if stable_hash(policy.to_dict()) != stable_hash(policy_payload):
            raise CrossEraSieveError(f"account-policy reconstruction drift: {policy_id}")
        frozen_configs.append(
            {
                **config,
                "replay_eligible": True,
                "source_kind": kind,
                "component_ids": members,
                "component_quantity_tiers": quantities,
                "frozen_account_policy": policy.to_dict(),
                "frozen_account_policy_hash": stable_hash(policy.to_dict()),
                "pnl_state_profile": profiles[profile_id],
                "pnl_state_profile_hash": stable_hash(profiles[profile_id]),
                "policy_spec_hash": policy_spec_hash,
                "signal_or_trade_logic_mutated": False,
            }
        )

    entries = _bank_entries()
    components = _original_calibrations(sorted(component_ids), entries)
    acquisition = _read(ROOT / W2019_ACQUISITION)
    source_file = Path(
        str(dict(acquisition["feature_build_inputs"])["source_files"][0]["path"])
    )
    map_file = Path(str(dict(acquisition["feature_build_inputs"])["contract_map_path"]))
    rules, rule_receipt = _load_rule_snapshot(ROOT / DEFAULT_RULE_SNAPSHOT)
    rule_payload = {label: dict(rules[label]) for label in ACCOUNT_LABELS}
    runnable = [row for row in frozen_configs if row["replay_eligible"]]
    core = {
        "schema": f"{SCHEMA}_selection_contract",
        "status": "FROZEN_BEFORE_W2019_CANDIDATE_OUTCOME_REPLAY",
        "evidence_role": "VIEWED_DEVELOPMENT_CROSS_ERA_SIEVE_ONLY",
        "w2019_can_claim_confirmation": False,
        "w2019_prior_packet_status": "CONFIRMATION_PACKET_CONSUMED_ONCE",
        "source_inventory": {
            "clean_v2_base_count": 44,
            "new_clean_base_count": 1,
            "clean_base_count": 45,
            "dynamic_variant_count": 3,
            "source_configuration_count": 48,
            "closed_exact_book_configuration_count": 2,
            "replay_configuration_count": len(runnable),
            "quarantined_exclusion_count": 6,
            "quarantined_policy_ids": sorted(QUARANTINED_IDS),
            "closed_exact_book_ids": sorted(CLOSED_EXACT_BOOKS),
        },
        "configurations": frozen_configs,
        "components": components,
        "account_labels": list(ACCOUNT_LABELS),
        "horizons_trading_days": list(HORIZONS),
        "scenarios": ["NORMAL", "STRESSED_1_5X"],
        "account_rules": rule_payload,
        "official_rule_snapshot": rule_receipt,
        "calendar_contract": {
            "start_inclusive": START,
            "end_exclusive": END,
            "warmup_complete_sessions": WARMUP_COMPLETE_SESSIONS,
            "start_grid": "MAXIMUM_NON_OVERLAPPING_COMPLETE_WINDOWS",
            "block": "W2019H1_VIEWED_DEVELOPMENT",
        },
        "selection_rule": {
            "maximum_representatives": 12,
            "minimum_representatives_when_available": 8,
            "one_primary_per_realized_behavior_cluster": True,
            "maximum_per_market": 3,
            "hard_requirements": [
                "W2019_STRESSED_NET_POSITIVE_IN_SELECTED_CELL",
                "ZERO_MLL_BREACH_IN_SELECTED_NORMAL_AND_STRESSED_CELL",
                "PASS_OR_POSITIVE_STRESSED_MEDIAN_TARGET_PROGRESS",
                "PASSING_EPISODES_CONSISTENCY_COMPLIANT",
            ],
            "cell_rank_order": [
                "STRESSED_PASS_COUNT",
                "NORMAL_PASS_COUNT",
                "MINIMUM_OF_PRIOR_AND_W2019_STRESSED_PROGRESS",
                "W2019_STRESSED_MEDIAN_TARGET_PROGRESS",
                "W2019_STRESSED_NET",
                "MINIMUM_MLL_BUFFER",
                "SHORTER_HORIZON",
                "SMALLER_ACCOUNT",
            ],
            "cluster_contract": (
                "EXACT_W2019_ACCOUNT_EPISODE_PATH_HASH_PLUS_COMPONENT_TRADE_BEHAVIOR"
            ),
            "selected_status": "DEVELOPMENT_REQUALIFIED",
            "future_confirmation_window": "W2018H1_UNTOUCHED_PENDING_FREEZE",
            "normal_pass_required_for_requalification": False,
            "stress_is_robustNESS_not_status_erasure": True,
        },
        "data_binding": {
            "normalized_ohlcv_path": str(source_file),
            "normalized_ohlcv_sha256": _sha256(source_file),
            "contract_map_path": str(map_file),
            "contract_map_sha256": _sha256(map_file),
            "acquisition_receipt_hash": acquisition["receipt_hash"],
            "new_purchase": False,
        },
        "source_hashes": {
            str(path): _sha256(ROOT / path)
            for path in (
                OPERATIONAL_MATRIX,
                OPERATIONAL_SUMMARY,
                PASS_BANK,
                CANDIDATE_BANK,
                MARGINAL_BOOKS,
                NEAR_PASS_RESULT,
                W2019_RESULT,
            )
        },
        "worker_processes": 2,
        "numeric_threads_per_worker": 1,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
    }
    contract = {**core, "contract_hash": stable_hash(core)}
    _write(output_dir / CONTRACT_NAME, contract, immutable=True)
    return contract


def build_features(output_dir: Path) -> dict[str, Any]:
    contract = _read(output_dir / CONTRACT_NAME)
    if stable_hash({k: v for k, v in contract.items() if k != "contract_hash"}) != str(
        contract["contract_hash"]
    ):
        raise CrossEraSieveError("contract hash drift before feature build")
    binding = dict(contract["data_binding"])
    source_path = Path(str(binding["normalized_ohlcv_path"]))
    if _sha256(source_path) != str(binding["normalized_ohlcv_sha256"]):
        raise CrossEraSieveError("W2019 normalized input drift")
    frame = lane.pd.read_parquet(source_path)
    normalized = lane.normalize_ohlcv_frame(
        frame, symbol=None, timeframe="1m", symbol_map={market: market for market in MARKETS}
    )
    lane.validate_ohlcv_frame(normalized, timeframe="1m")
    observed = set(normalized["symbol"].astype(str))
    if observed != set(MARKETS):
        raise CrossEraSieveError(f"W2019 market inventory drift: {sorted(observed)}")
    roll_map = load_roll_map(Path(str(binding["contract_map_path"])))
    mapped, map_guard = lane._apply_explicit_contract_map(
        normalized, roll_map, required_map_type=roll_map.map_type
    )
    featured = lane._prepare_feature_frame(lane._build_past_only_feature_frame(mapped))
    cache_root = output_dir / "feature_matrices"
    store = lane.CanonicalFeatureStore(cache_root)
    bundles: dict[str, Any] = {}
    for market in MARKETS:
        market_frame = featured.loc[featured["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = lane._market_arrays(market_frame, market)
        arrays.pop("entry_price", None)
        for key in tuple(arrays):
            if key.startswith("forward_move__"):
                arrays.pop(key)
        key = lane.CanonicalFeatureKey(
            market=market,
            explicit_contract_scope=f"{market}_w2019h1_cross_era_viewed_development",
            start_inclusive=START,
            end_exclusive=END,
            source_data_sha256=str(binding["normalized_ohlcv_sha256"]),
            roll_map_hash=str(binding["contract_map_sha256"]),
            transformation_version=FEATURE_VERSION,
            feature_dag_hash=lane.FEATURE_DAG_HASH,
            timeframes=("1m", "5m", "15m", "30m", "60m", "session", "daily"),
        )
        stored = store.put(
            key,
            arrays,
            provenance={
                "data_role": "VIEWED_DEVELOPMENT",
                "contract_hash": contract["contract_hash"],
                "market": market,
                "feature_names": list(metadata["feature_names"]),
                "future_outcome_arrays_in_decision_bundle": False,
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
        "schema": f"{SCHEMA}_feature_receipt",
        "status": "W2019H1_CAUSAL_FEATURE_BUNDLES_READY",
        "contract_hash": contract["contract_hash"],
        "bundles": bundles,
        "map_guard": map_guard,
        "market_count": len(bundles),
        "future_outcome_arrays_in_decision_bundle": False,
        "data_role": "VIEWED_DEVELOPMENT",
        "new_purchase": False,
        "q4_access_count_delta": 0,
    }
    receipt = {**core, "result_hash": stable_hash(core)}
    _write(output_dir / FEATURE_NAME, receipt, immutable=True)
    return receipt


def _open_features(receipt: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for market, raw in dict(receipt["bundles"]).items():
        matrix = lane.FeatureMatrix.open(str(dict(raw)["path"]), mmap=True)
        if matrix.fingerprint != str(dict(raw)["bundle_hash"]):
            raise CrossEraSieveError(f"W2019 feature bundle drift: {market}")
        output[str(market)] = matrix
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
    if len(ordered) <= WARMUP_COMPLETE_SESSIONS:
        raise CrossEraSieveError("W2019 common calendar lacks warmup coverage")
    return ordered[WARMUP_COMPLETE_SESSIONS:]


def _component_replay(
    component: Mapping[str, Any], matrices: Mapping[str, Any], calendar: Sequence[int]
) -> tuple[Any, dict[str, Any]]:
    candidate = HazardCandidate(**dict(component["candidate"]))
    calibrated = lane._calibrated_from_payload(candidate, component["calibration"])
    matrix = matrices[candidate.market]
    if candidate.cross_asset_reference_market:
        matrix = lane.with_availability_safe_cross_asset_feature(
            matrix, matrices[candidate.cross_asset_reference_market]
        )
    first_ns = int(calendar[0]) * 86_400_000_000_000
    end_ns = (int(calendar[-1]) + 1) * 86_400_000_000_000
    intents = discover_intents_batch(
        calibrated, matrix, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns
    )
    streaming = discover_intents_streaming(
        calibrated, matrix, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns
    )
    if tuple((row.row_index, row.direction) for row in intents) != streaming:
        raise CrossEraSieveError(f"batch/stream mismatch: {candidate.candidate_id}")
    events = observe_outcomes(calibrated, matrix, intents)
    eligible_days = frozen_eligible_session_calendar(
        candidate, matrix, evaluation_start_ns=first_ns, evaluation_end_exclusive_ns=end_ns
    )
    replay = exact_sleeve_replay(calibrated, events, eligible_session_days=eligible_days)
    normal, normal_violations = lane._apply_session_contract(replay.normal_trajectories)
    stressed, stress_violations = lane._apply_session_contract(replay.stressed_trajectories)
    if normal_violations or stress_violations:
        raise CrossEraSieveError(f"session contract violation: {candidate.candidate_id}")
    unavailable = set(int(value) for value in calendar).difference(
        int(value) for value in replay.eligible_session_days
    )
    unavailable.update(
        int(row.session_day)
        for row in replay.events
        if str(row.outcome) == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
    )
    return replay, {
        "normal": tuple(normal),
        "stressed": tuple(stressed),
        "unavailable_days": frozenset(unavailable),
        "intent_count": len(intents),
        "completed_event_count": len(normal),
        "censored_event_count": sum(
            str(row.outcome) == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
            for row in replay.events
        ),
        "decision_hash": replay.decision_hash,
        "normal_trajectory_hash": replay.normal_trajectory_hash,
        "stressed_trajectory_hash": replay.stressed_trajectory_hash,
        "fill_policy_hash": replay.fill_policy_hash,
    }


def _worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    os.environ.update(
        {
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        }
    )
    contract = _read(Path(str(payload["contract_path"])))
    feature_receipt = _read(Path(str(payload["feature_receipt_path"])))
    matrices = _open_features(feature_receipt)
    calendar = _calendar(matrices)
    starts = lane.non_overlapping_starts(calendar, HORIZONS)
    starts = {
        horizon: tuple((day, "W2019H1_VIEWED_DEVELOPMENT") for day, _ in values)
        for horizon, values in starts.items()
    }
    components = {str(key): dict(value) for key, value in contract["components"].items()}
    rules = {str(key): dict(value) for key, value in contract["account_rules"].items()}
    cache: dict[str, dict[str, Any]] = {}
    results: list[dict[str, Any]] = []
    exact_episodes = 0
    for config in payload["configurations"]:
        row = dict(config)
        members = [str(value) for value in row["component_ids"]]
        for member in members:
            if member not in cache:
                _replay, materialized = _component_replay(
                    components[member], matrices, calendar
                )
                cache[member] = materialized
        trajectories = {"NORMAL": {}, "STRESSED_1_5X": {}}
        unavailable: set[int] = set()
        component_receipts = []
        for member in members:
            value = cache[member]
            quantity = int(dict(row["component_quantity_tiers"])[member])
            normal = tuple(
                scale_causal_trajectory(item, executable_quantity_multiplier=quantity)
                for item in value["normal"]
            )
            stressed = tuple(
                scale_causal_trajectory(item, executable_quantity_multiplier=quantity)
                for item in value["stressed"]
            )
            _require_scenario_identity(normal, stressed)
            trajectories["NORMAL"][member] = normal
            trajectories["STRESSED_1_5X"][member] = stressed
            unavailable.update(value["unavailable_days"])
            component_receipts.append(
                {
                    "candidate_id": member,
                    "quantity_tier": quantity,
                    **{
                        key: value[key]
                        for key in (
                            "intent_count",
                            "completed_event_count",
                            "censored_event_count",
                            "decision_hash",
                            "normal_trajectory_hash",
                            "stressed_trajectory_hash",
                            "fill_policy_hash",
                        )
                    },
                }
            )
        profile_payload = dict(row["pnl_state_profile"])
        profile = PROFILE_BY_ID[str(profile_payload["profile_id"])]
        if stable_hash(profile.to_dict()) != str(row["pnl_state_profile_hash"]):
            raise CrossEraSieveError("frozen PnL profile hash drift")
        prepared = _PreparedPolicy(
            policy_id=str(row["configuration_id"]),
            source_kind=str(row["source_kind"]),
            evidence_tier=str(row["source_evidence_tier"]),
            account_label="50K",
            baseline_policy=ActiveRiskPoolPolicy.from_mapping(
                dict(row["frozen_account_policy"])
            ),
            trajectories=trajectories,
            unavailable_days=frozenset(unavailable),
            source_policy=row,
            source_metrics={},
            source_hashes={},
        )
        account_results = []
        for account_label in ACCOUNT_LABELS:
            evaluation = _evaluate_profile(
                prepared,
                profile,
                blocks=("W2019H1_VIEWED_DEVELOPMENT",),
                calendar=calendar,
                starts=starts,
                rule=rules[account_label],
            )
            exact_episodes += int(evaluation["exact_episode_count"])
            account_results.append(
                {
                    "account_label": account_label,
                    "profile_id": profile.profile_id,
                    "evaluation": evaluation,
                }
            )
        core = {
            "configuration_id": row["configuration_id"],
            "base_policy_id": row["base_policy_id"],
            "record_type": row["record_type"],
            "source_kind": row["source_kind"],
            "source_evidence_tier": row["source_evidence_tier"],
            "policy_spec_hash": row["policy_spec_hash"],
            "niches": row["niches"],
            "component_ids": members,
            "component_receipts": component_receipts,
            "account_results": account_results,
            "signal_or_trade_logic_mutated": False,
            "evidence_role": "VIEWED_DEVELOPMENT_CROSS_ERA_SIEVE_ONLY",
            "promotion_status": None,
        }
        results.append({**core, "result_hash": stable_hash(core)})
    return {
        "results": results,
        "exact_account_episode_count": exact_episodes,
        "component_replay_count": len(cache),
        "calendar": list(calendar),
        "start_counts": {str(key): len(value) for key, value in starts.items()},
    }


def _old_stressed_progress(config: Mapping[str, Any], matrix_rows: Mapping[str, Any]) -> float:
    row = dict(matrix_rows.get(str(config["configuration_id"])) or {})
    if not row:
        row = dict(matrix_rows.get(str(config["base_policy_id"])) or {})
    values = []
    for horizon in HORIZONS:
        overall = dict(dict(dict(row.get("horizons") or {}).get(str(horizon)) or {}).get("overall") or {})
        stressed = dict(overall.get("stressed") or {})
        if stressed.get("target_progress_median") is not None:
            values.append(float(stressed["target_progress_median"]))
    return max(values, default=0.0)


def _selection_cell(config: Mapping[str, Any], old_progress: float) -> dict[str, Any] | None:
    cells = []
    for account in config["account_results"]:
        account_label = str(account["account_label"])
        summaries = dict(account["evaluation"])["summaries"]
        for horizon in HORIZONS:
            normal = dict(summaries["NORMAL"][str(horizon)])
            stressed = dict(summaries["STRESSED_1_5X"][str(horizon)])
            normal_pass = int(normal["pass_count"])
            stress_pass = int(stressed["pass_count"])
            hard_safe = (
                int(normal["mll_breach_count"]) == 0
                and int(stressed["mll_breach_count"]) == 0
                and int(normal["full_coverage_start_count"]) > 0
                and int(stressed["full_coverage_start_count"]) > 0
            )
            useful = normal_pass > 0 or float(stressed["target_progress_median"]) > 0.0
            positive = float(stressed["net_total_usd"]) > 0.0
            rank = (
                stress_pass,
                normal_pass,
                min(old_progress, float(stressed["target_progress_median"])),
                float(stressed["target_progress_median"]),
                float(stressed["net_total_usd"]),
                float(stressed["minimum_mll_buffer_usd"] or -1e18),
                -horizon,
                -int(account_label.removesuffix("K")),
            )
            cells.append(
                {
                    "account_label": account_label,
                    "horizon_trading_days": horizon,
                    "normal": normal,
                    "stressed": stressed,
                    "hard_safe": hard_safe,
                    "useful_progress_or_pass": useful,
                    "positive_stressed_net": positive,
                    "rank": list(rank),
                }
            )
    eligible = [
        cell
        for cell in cells
        if cell["hard_safe"]
        and cell["useful_progress_or_pass"]
        and cell["positive_stressed_net"]
    ]
    return max(eligible, key=lambda value: tuple(value["rank"])) if eligible else None


def _cohort(
    contract: Mapping[str, Any], results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    matrix = _read(ROOT / OPERATIONAL_MATRIX)
    matrix_rows = {str(row["configuration_id"]): dict(row) for row in matrix["rows"]}
    config_by_id = {
        str(row["configuration_id"]): dict(row)
        for row in contract["configurations"]
        if row["replay_eligible"]
    }
    candidates = []
    for raw in results:
        result = dict(raw)
        frozen = config_by_id[str(result["configuration_id"])]
        old_progress = _old_stressed_progress(frozen, matrix_rows)
        selected = _selection_cell(result, old_progress)
        behavior_payload = {
            "component_trade_hashes": sorted(
                str(row["stressed_trajectory_hash"])
                for row in result["component_receipts"]
            ),
            "account_episode_path_hashes": sorted(
                str(dict(account["evaluation"])["summaries"][scenario][str(horizon)]["episode_path_hash"])
                for account in result["account_results"]
                for scenario in ("NORMAL", "STRESSED_1_5X")
                for horizon in HORIZONS
            ),
        }
        cluster_hash = stable_hash(behavior_payload)
        candidates.append(
            {
                "configuration_id": result["configuration_id"],
                "base_policy_id": result["base_policy_id"],
                "realized_behavior_cluster_id": f"w2019_exact_{cluster_hash[:16]}",
                "realized_behavior_hash": cluster_hash,
                "niches": result["niches"],
                "old_development_best_stressed_progress": old_progress,
                "selected_cell": selected,
                "eligible_for_requalification": selected is not None,
                "result_hash": result["result_hash"],
            }
        )
    eligible = [row for row in candidates if row["eligible_for_requalification"]]
    eligible.sort(key=lambda row: tuple(row["selected_cell"]["rank"]), reverse=True)
    retained = []
    seen_clusters: set[str] = set()
    market_counts: dict[str, int] = {}
    for row in eligible:
        cluster = str(row["realized_behavior_cluster_id"])
        markets = list(dict(row["niches"]).get("markets") or ())
        market = str(markets[0]) if len(markets) == 1 else "+".join(sorted(map(str, markets)))
        if not market:
            market = "UNKNOWN"
        if cluster in seen_clusters or market_counts.get(market, 0) >= 3:
            continue
        retained.append(
            {
                **row,
                "status": "DEVELOPMENT_REQUALIFIED",
                "independent_confirmation_claimed": False,
                "future_confirmation_window": "W2018H1_UNTOUCHED_PENDING_FREEZE",
            }
        )
        seen_clusters.add(cluster)
        market_counts[market] = market_counts.get(market, 0) + 1
        if len(retained) == 12:
            break
    core = {
        "schema": f"{SCHEMA}_cohort",
        "status": (
            "DEVELOPMENT_REQUALIFIED_COHORT_FROZEN"
            if retained
            else "NO_CROSS_ERA_POLICY_REQUALIFIED"
        ),
        "contract_hash": contract["contract_hash"],
        "candidate_count": len(candidates),
        "eligible_count": len(eligible),
        "retained_count": len(retained),
        "retained": retained,
        "all_candidate_decisions": candidates,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "w2019_used_as_confirmation": False,
        "promotion_status": None,
        "next_action": (
            "FREEZE_RETAINED_COHORT_FOR_ONE_SHOT_W2018H1_CONFIRMATION"
            if retained
            else "CLOSE_CROSS_ERA_SIEVE_AND_SELECT_MATERIALLY_DISTINCT_BRANCH"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def run(output_dir: Path) -> dict[str, Any]:
    started = time.perf_counter()
    contract = _read(output_dir / CONTRACT_NAME)
    feature_receipt = _read(output_dir / FEATURE_NAME)
    runnable = [dict(row) for row in contract["configurations"] if row["replay_eligible"]]
    midpoint = (len(runnable) + 1) // 2
    shards = [runnable[:midpoint], runnable[midpoint:]]
    payloads = [
        {
            "contract_path": str(output_dir / CONTRACT_NAME),
            "feature_receipt_path": str(output_dir / FEATURE_NAME),
            "configurations": shard,
        }
        for shard in shards
        if shard
    ]
    _write(
        output_dir / STATE_NAME,
        {
            "schema": f"{SCHEMA}_state",
            "status": "ECONOMIC_REPLAY_RUNNING",
            "pid": os.getpid(),
            "configuration_total": len(runnable),
            "configuration_completed": 0,
            "exact_account_episode_count": 0,
            "worker_processes": len(payloads),
            "numeric_threads_per_worker": 1,
            "started_at_utc": datetime.now(UTC).isoformat(),
            "contract_hash": contract["contract_hash"],
        },
    )
    worker_results = []
    completed = 0
    episodes = 0
    with ProcessPoolExecutor(max_workers=min(2, len(payloads))) as pool:
        futures = [pool.submit(_worker, payload) for payload in payloads]
        for future in as_completed(futures):
            value = future.result()
            worker_results.append(value)
            completed += len(value["results"])
            episodes += int(value["exact_account_episode_count"])
            _write(
                output_dir / STATE_NAME,
                {
                    "schema": f"{SCHEMA}_state",
                    "status": "ECONOMIC_REPLAY_RUNNING",
                    "pid": os.getpid(),
                    "configuration_total": len(runnable),
                    "configuration_completed": completed,
                    "exact_account_episode_count": episodes,
                    "worker_processes": len(payloads),
                    "numeric_threads_per_worker": 1,
                    "last_batch_completed_at_utc": datetime.now(UTC).isoformat(),
                    "contract_hash": contract["contract_hash"],
                },
            )
    results = [row for worker in worker_results for row in worker["results"]]
    results.sort(key=lambda row: str(row["configuration_id"]))
    cohort = _cohort(contract, results)
    _write(output_dir / COHORT_NAME, cohort, immutable=True)
    runtime = time.perf_counter() - started
    core = {
        "schema": f"{SCHEMA}_economic_result",
        "status": "CROSS_ERA_BANK_SIEVE_COMPLETE_VIEWED_DEVELOPMENT",
        "contract_hash": contract["contract_hash"],
        "feature_receipt_hash": feature_receipt["result_hash"],
        "configuration_result_count": len(results),
        "exact_account_episode_count": episodes,
        "component_worker_replay_count": sum(
            int(worker["component_replay_count"]) for worker in worker_results
        ),
        "calendar": {
            "start": START,
            "end": END,
            "post_warmup_session_count": len(worker_results[0]["calendar"]),
            "first_session_day": worker_results[0]["calendar"][0],
            "last_session_day": worker_results[0]["calendar"][-1],
            "non_overlapping_start_counts": worker_results[0]["start_counts"],
        },
        "results": results,
        "cohort_hash": cohort["result_hash"],
        "cohort_status": cohort["status"],
        "development_requalified_count": cohort["retained_count"],
        "runtime": {
            "seconds": runtime,
            "worker_processes": len(payloads),
            "numeric_threads_per_worker": 1,
            "episodes_per_hour": episodes / max(runtime, 1e-9) * 3600.0,
        },
        "w2019_evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "w2019_confirmation_claimed": False,
        "strategy_or_risk_retuning_performed": False,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
        "next_action": cohort["next_action"],
    }
    hash_core = dict(core)
    hash_core["runtime"] = {
        "worker_processes": len(payloads),
        "numeric_threads_per_worker": 1,
    }
    result = {**core, "result_hash": stable_hash(hash_core)}
    _write(output_dir / RESULT_NAME, result, immutable=True)
    _write(
        output_dir / STATE_NAME,
        {
            "schema": f"{SCHEMA}_state",
            "status": "COMPLETE",
            "pid": os.getpid(),
            "configuration_total": len(runnable),
            "configuration_completed": len(results),
            "exact_account_episode_count": episodes,
            "development_requalified_count": cohort["retained_count"],
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "result_hash": result["result_hash"],
        },
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    global ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("freeze", "features", "run", "all"))
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    args = parser.parse_args(argv)
    ROOT = Path(args.root).resolve()
    output_dir = (ROOT / args.output_dir).resolve()
    if args.action in {"freeze", "all"}:
        value = freeze_contract(output_dir)
    if args.action in {"features", "all"}:
        value = build_features(output_dir)
    if args.action in {"run", "all"}:
        value = run(output_dir)
    print(json.dumps(value, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
