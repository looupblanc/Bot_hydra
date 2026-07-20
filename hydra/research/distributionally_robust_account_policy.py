"""Bounded distributionally-robust account-policy experiment.

All three packets used here are already development evidence.  The experiment
therefore makes no confirmation claim.  It evaluates newly fingerprinted,
metadata-selected books on W2018H1, W2019H1, and the original 2023--2024
development interval.  Policy selection is pessimistic across eras and is
audited with leave-one-era-out ranking.

The module deliberately reuses immutable causal component specifications,
calibrations, causal fills, account replay, and rule snapshots.  It neither
recalculates candidate semantics nor opens a data source outside the existing
cache.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import statistics
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import stable_hash
from hydra.production import fresh_confirmation_lane as lane
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    _load_self_hashed_manifest,
    _market_contract_limit_mini,
    _require_scenario_identity,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.fast_pass_runtime_helpers import _governor_profiles
from hydra.research import cross_era_bank_sieve as sieve
from hydra.research import cross_era_marginal_pair_builder as pairs
from hydra.research.causal_target_velocity import HazardCandidate
from hydra.research.pnl_state_risk_frontier import _PreparedPolicy, _evaluate_profile


SCHEMA = "hydra_distributionally_robust_account_policy_v1"
ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path(
    "reports/economic_evolution/distributionally_robust_account_policy_v1"
)
DECISION_CARD_NAME = "decision_card.json"
CONTRACT_NAME = "selection_contract.json"
STATE_NAME = "production_state.json"
RESULT_NAME = "economic_result.json"

SOURCE_CONTRACT = Path(
    "reports/economic_evolution/cross_era_bank_sieve_2019h1_v1/selection_contract.json"
)
W2018_FEATURES = Path(
    "reports/economic_evolution/cross_era_bank_sieve_2019h1_v1/"
    "marginal_pair_builder/w2018h1_confirmation/feature_receipt.json"
)
W2019_FEATURES = Path(
    "reports/economic_evolution/cross_era_bank_sieve_2019h1_v1/feature_receipt.json"
)
W2018_CLOSED = Path(
    "reports/economic_evolution/cross_era_bank_sieve_2019h1_v1/"
    "marginal_pair_builder/w2018h1_one_shot_confirmation_contract.json"
)

HORIZONS = (5, 10, 20)
ACCOUNT_LABELS = ("50K", "100K", "150K")
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
COMMON_MARKETS = ("CL", "ES", "NQ")
MAXIMUM_MEMBERSHIP_BOOKS = 40
MAXIMUM_SERIOUS_CELLS = 128
MAXIMUM_RETAINED = 12
LOEO_RETAINED_PER_FOLD = 8
SUBSTANTIAL_LOWER_QUARTILE_PROGRESS = 0.10
LCB_STANDARD_ERROR_MULTIPLIER = 1.0
GOVERNOR_PROFILE_ID = "fast_pass_governor_02"

ERAS: tuple[dict[str, Any], ...] = (
    {
        "era_id": "W2018H1_VIEWED_DEVELOPMENT",
        "start_inclusive": "2018-01-02",
        "end_exclusive": "2018-07-02",
        "feature_source": "W2018_FEATURE_RECEIPT",
        "warmup_complete_sessions": 5,
    },
    {
        "era_id": "W2019H1_VIEWED_DEVELOPMENT",
        "start_inclusive": "2019-01-02",
        "end_exclusive": "2019-07-01",
        "feature_source": "W2019_FEATURE_RECEIPT",
        "warmup_complete_sessions": 5,
    },
    {
        "era_id": "RECENT_2023_2024_VIEWED_DEVELOPMENT",
        "start_inclusive": "2023-07-03",
        "end_exclusive": "2024-09-27",
        "feature_source": "FAST_PASS_0029_BOUND_MATRICES",
        "warmup_complete_sessions": 0,
    },
)


class DistributionallyRobustPolicyError(RuntimeError):
    """The bounded immutable experiment cannot be reconstructed exactly."""


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise DistributionallyRobustPolicyError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write(path: Path, value: Mapping[str, Any], *, immutable: bool = False) -> None:
    text = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if immutable and path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise DistributionallyRobustPolicyError(
                f"refusing divergent immutable rewrite: {path}"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _verify_hash(value: Mapping[str, Any], field: str, *, label: str) -> str:
    claimed = str(value.get(field) or "")
    core = {key: item for key, item in value.items() if key != field}
    if stable_hash(core) != claimed:
        raise DistributionallyRobustPolicyError(f"{label} hash drift")
    return claimed


def _niche_tokens(row: Mapping[str, Any]) -> dict[str, frozenset[str]]:
    niches = dict(row.get("niches") or {})
    return {
        key: frozenset(str(value) for value in niches.get(key, ()))
        for key in (
            "markets",
            "sessions",
            "mechanisms",
            "timeframes",
            "holding_horizons",
        )
    }


def _membership_diversity(
    members: Sequence[str], inventory: Mapping[str, Mapping[str, Any]]
) -> tuple[int, ...]:
    niches = [_niche_tokens(inventory[member]) for member in members]
    unions = {
        key: set().union(*(value[key] for value in niches)) for key in niches[0]
    }
    markets = len(unions["markets"])
    mechanisms = len(unions["mechanisms"])
    sessions = len(unions["sessions"])
    timeframes = len(unions["timeframes"])
    horizons = len(unions["holding_horizons"])
    return (markets, mechanisms, sessions, timeframes, horizons)


def _closed_memberships() -> set[tuple[str, ...]]:
    closed = _read(ROOT / W2018_CLOSED)
    _verify_hash(closed, "contract_hash", label="closed W2018 cohort")
    return {
        tuple(sorted(str(value) for value in row["component_ids"]))
        for row in closed["candidates"]
    }


def _select_memberships(
    inventory: Mapping[str, Mapping[str, Any]],
    *,
    maximum: int = MAXIMUM_MEMBERSHIP_BOOKS,
) -> list[tuple[str, ...]]:
    """Select a metadata-only diversity beam before opening era outcomes."""

    if maximum != 40:
        raise DistributionallyRobustPolicyError("frozen membership capacity drift")
    ids = sorted(inventory)
    closed = _closed_memberships()
    pair_pool = [
        tuple(pair)
        for pair in itertools.combinations(ids, 2)
        if tuple(sorted(pair)) not in closed
    ]
    pair_pool.sort(
        key=lambda members: (
            _membership_diversity(members, inventory),
            stable_hash({"members": list(members)}),
        ),
        reverse=True,
    )
    selected: list[tuple[str, ...]] = []
    counts: dict[str, int] = {}
    for members in pair_pool:
        if any(counts.get(member, 0) >= 4 for member in members):
            continue
        selected.append(members)
        for member in members:
            counts[member] = counts.get(member, 0) + 1
        if len(selected) == 32:
            break
    if len(selected) != 32:
        raise DistributionallyRobustPolicyError("diversity beam cannot freeze 32 pairs")

    triple_pool = [
        tuple(value) for value in itertools.combinations(ids, 3)
        if tuple(sorted(value)) not in closed
    ]
    triple_pool.sort(
        key=lambda members: (
            _membership_diversity(members, inventory),
            -sum(counts.get(member, 0) for member in members),
            stable_hash({"members": list(members)}),
        ),
        reverse=True,
    )
    triple_counts: dict[str, int] = {}
    for members in triple_pool:
        if any(triple_counts.get(member, 0) >= 2 for member in members):
            continue
        selected.append(members)
        for member in members:
            triple_counts[member] = triple_counts.get(member, 0) + 1
        if len(selected) == maximum:
            break
    if len(selected) != maximum or len(set(selected)) != maximum:
        raise DistributionallyRobustPolicyError("diversity beam capacity drift")
    if any(tuple(sorted(value)) in closed for value in selected):
        raise DistributionallyRobustPolicyError("closed exact formulation recycled")
    return selected


def freeze_contract(output_dir: Path) -> dict[str, Any]:
    source = _read(ROOT / SOURCE_CONTRACT)
    _verify_hash(source, "contract_hash", label="cross-era source contract")
    components = {str(key): dict(value) for key, value in source["components"].items()}
    source_rows = {
        str(row["configuration_id"]): dict(row)
        for row in source["configurations"]
        if row.get("replay_eligible")
        and row.get("record_type") == "BASE_POLICY"
        and row.get("source_kind") == "EXACT_STANDALONE"
        and len(row.get("component_ids") or ()) == 1
    }
    inventory: dict[str, dict[str, Any]] = {}
    for policy_id, row in source_rows.items():
        component_id = str(row["component_ids"][0])
        candidate = HazardCandidate(**dict(components[component_id]["candidate"]))
        if candidate.market not in COMMON_MARKETS:
            continue
        policy = pairs.ActiveRiskPoolPolicy.from_mapping(
            dict(row["frozen_account_policy"])
        )
        nominal_charge = float(policy.nominal_risk_charge_map[component_id])
        inventory[component_id] = {
            "component_id": component_id,
            "source_configuration_id": policy_id,
            "candidate": components[component_id]["candidate"],
            "candidate_fingerprint": components[component_id][
                "candidate_fingerprint"
            ],
            "calibration": components[component_id]["calibration"],
            "quantity_tier": int(row["component_quantity_tiers"][component_id]),
            "nominal_risk_charge": nominal_charge,
            "niches": row["niches"],
            "source_policy_hash": row["policy_spec_hash"],
        }
    if len(inventory) != 33:
        raise DistributionallyRobustPolicyError(
            f"expected 33 all-era executable components, got {len(inventory)}"
        )

    manifest = _load_self_hashed_manifest(ROOT / DEFAULT_FAST_PASS_MANIFEST)
    profiles = {
        row.profile_id: asdict(row) for row in _governor_profiles(manifest)
    }
    governor = profiles[GOVERNOR_PROFILE_ID]
    memberships = _select_memberships(inventory)
    rules = {
        str(key): dict(value) for key, value in source["account_rules"].items()
    }
    cells = []
    for members in memberships:
        book_semantic = {
            "component_ids": list(members),
            "component_priority": list(members),
            "component_quantity_tiers": {
                member: inventory[member]["quantity_tier"] for member in members
            },
            "component_nominal_risk_charges": {
                member: inventory[member]["nominal_risk_charge"] for member in members
            },
            "governor_profile": governor,
            "pnl_state_profile_id": "pnl_state_identity",
            "construction": "METADATA_ONLY_DIVERSITY_BEAM_BEFORE_ERA_OUTCOMES",
            "signal_entry_exit_stop_target_mutated": False,
        }
        book_id = f"dro_book_{stable_hash(book_semantic)[:24]}"
        for account_label in ACCOUNT_LABELS:
            rule = rules[account_label]
            cap = min(
                _market_contract_limit_mini(inventory[member]["candidate"], rule)
                for member in members
            )
            if cap <= 0:
                raise DistributionallyRobustPolicyError(
                    f"no legal capacity: {book_id}/{account_label}"
                )
            cell_semantic = {
                **book_semantic,
                "book_id": book_id,
                "account_label": account_label,
                "maximum_mini_equivalent": cap,
            }
            cells.append(
                {
                    **cell_semantic,
                    "policy_id": f"dro_cell_{stable_hash(cell_semantic)[:24]}",
                    "policy_spec_hash": stable_hash(cell_semantic),
                }
            )
    if len(cells) != 120 or len(cells) > MAXIMUM_SERIOUS_CELLS:
        raise DistributionallyRobustPolicyError("120-cell frozen boundary drift")

    decision_core = {
        "schema": f"{SCHEMA}_decision_card",
        "status": "FROZEN_BEFORE_DRO_OUTCOMES",
        "hypothesis": (
            "A metadata-diverse shared-risk book selected by worst-era economics "
            "can avoid the regime-specific false positives of mono-era selection."
        ),
        "strongest_argument_against": (
            "The immutable components may contain no invariant target velocity; "
            "combination can only redistribute, not manufacture, alpha."
        ),
        "smallest_decisive_experiment": (
            "120 novel book/account cells, exact P5/P10/P20 normal/stress on "
            "three already-viewed eras with leave-one-era-out ranking."
        ),
        "expected_runtime_minutes": 20,
        "expected_data_cost_usd": 0.0,
        "expected_information_gain": "HIGH",
        "expected_economic_upside": "MEDIUM_HIGH",
        "overfitting_risk": "MEDIUM_CONTROLLED_BY_WORST_ERA_AND_LOEO",
        "deployment_risk": "LOW_EXISTING_CAUSAL_COMPONENTS_ONLY",
        "materially_distinct_next_alternative": (
            "CAUSAL_EVENT_TIME_VOLATILITY_BURST_DIRECT_ACCOUNT_POLICY_WITH_"
            "MULTI_ASSET_SURVIVAL_ROUTING"
        ),
    }
    decision = {**decision_core, "decision_hash": stable_hash(decision_core)}
    _write(output_dir / DECISION_CARD_NAME, decision, immutable=True)

    core = {
        "schema": f"{SCHEMA}_selection_contract",
        "status": "FROZEN_BEFORE_DISTRIBUTIONALLY_ROBUST_REPLAY",
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY_NO_CONFIRMATION_CLAIM",
        "source_contract_hash": source["contract_hash"],
        "decision_hash": decision["decision_hash"],
        "eras": list(ERAS),
        "horizons_trading_days": list(HORIZONS),
        "scenarios": list(SCENARIOS),
        "account_labels": list(ACCOUNT_LABELS),
        "account_rules": rules,
        "component_inventory": inventory,
        "membership_count": len(memberships),
        "serious_cell_count": len(cells),
        "cells": cells,
        "governor_profile_id": GOVERNOR_PROFILE_ID,
        "closed_exact_memberships": [list(value) for value in sorted(_closed_memberships())],
        "gate": {
            "positive_stressed_net_required_in_every_era": True,
            "defensive_role_allowed": False,
            "zero_mll_breach_required": True,
            "no_compliance_failure_required": True,
            "tier_g_normal_pass_eras_minimum": 2,
            "tier_g_stressed_pass_eras_minimum": 2,
            "progress_only_lower_quartile_floor": SUBSTANTIAL_LOWER_QUARTILE_PROGRESS,
            "progress_only_status": "ROBUST_PROGRESS_ONLY_NO_PROMOTION",
            "stress_is_advisory_for_normal_pass_reporting_but_required_for_G": True,
            "maximum_retained": MAXIMUM_RETAINED,
        },
        "selection": {
            "pessimistic_score": (
                "lexicographic minimum across era stressed pass, lower-quartile "
                "progress, median progress, net, MLL buffer, and stratum LCB"
            ),
            "leave_one_era_out": True,
            "loeo_retain_per_fold": LOEO_RETAINED_PER_FOLD,
            "one_account_cell_per_book": True,
            "behavior_hash_uses_all_era_episode_paths": True,
        },
        "data_bindings": {
            "w2018_feature_receipt": str(ROOT / W2018_FEATURES),
            "w2018_feature_receipt_sha256": _sha256(ROOT / W2018_FEATURES),
            "w2019_feature_receipt": str(ROOT / W2019_FEATURES),
            "w2019_feature_receipt_sha256": _sha256(ROOT / W2019_FEATURES),
            "recent_manifest": str(ROOT / DEFAULT_FAST_PASS_MANIFEST),
            "recent_manifest_sha256": _sha256(ROOT / DEFAULT_FAST_PASS_MANIFEST),
        },
        "worker_processes": 1,
        "numeric_threads_per_worker": 1,
        "new_data_purchase": False,
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
    }
    contract = {**core, "contract_hash": stable_hash(core)}
    _write(output_dir / CONTRACT_NAME, contract, immutable=True)
    return contract


def _open_era_features(era: Mapping[str, Any]) -> dict[str, Any]:
    source = str(era["feature_source"])
    if source == "W2018_FEATURE_RECEIPT":
        receipt = _read(ROOT / W2018_FEATURES)
        return {
            market: lane.FeatureMatrix.open(str(raw["path"]), mmap=True)
            for market, raw in receipt["bundles"].items()
        }
    if source == "W2019_FEATURE_RECEIPT":
        opened = sieve._open_features(_read(ROOT / W2019_FEATURES))
        # The W2019 packet also contains RTY/YM.  The all-era intersection is
        # deliberately CL/ES/NQ because the already-consumed W2018 packet has
        # no RTY/YM matrices.  Ignoring the two extra matrices does not inspect
        # or alter an outcome and keeps the frozen 33-component boundary exact.
        return {market: opened[market] for market in COMMON_MARKETS}
    if source == "FAST_PASS_0029_BOUND_MATRICES":
        manifest = _load_self_hashed_manifest(ROOT / DEFAULT_FAST_PASS_MANIFEST)
        bindings = dict(manifest["data"]["feature_matrix_bindings"])
        return {
            market: lane._open_bound_matrix(ROOT, bindings, market)
            for market in COMMON_MARKETS
        }
    raise DistributionallyRobustPolicyError(f"unknown feature source: {source}")


def _era_calendar(
    era: Mapping[str, Any], matrices: Mapping[str, Any]
) -> tuple[int, ...]:
    common: set[int] | None = None
    start = lane._date_ns(str(era["start_inclusive"]))
    end = lane._date_ns(str(era["end_exclusive"]))
    for matrix in matrices.values():
        timestamps = np.asarray(matrix.array("timestamp_ns"), dtype=np.int64)
        days = np.asarray(matrix.array("session_day"), dtype=np.int64)
        current = {int(value) for value in days[(timestamps >= start) & (timestamps < end)]}
        common = current if common is None else common & current
    ordered = tuple(sorted(common or ()))
    warmup = int(era["warmup_complete_sessions"])
    if len(ordered) <= warmup + max(HORIZONS):
        raise DistributionallyRobustPolicyError(
            f"insufficient complete calendar: {era['era_id']}"
        )
    return ordered[warmup:]


def _stratum_receipts(
    members: Sequence[str],
    inventory: Mapping[str, Mapping[str, Any]],
    cache: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for member in members:
        meta = inventory[member]
        candidate = HazardCandidate(**dict(meta["candidate"]))
        quantity = int(meta["quantity_tier"])
        values = [
            float(row.event.net_pnl) * quantity for row in cache[member]["stressed"]
        ]
        count = len(values)
        mean = statistics.fmean(values) if values else 0.0
        deviation = statistics.stdev(values) if count > 1 else 0.0
        standard_error = deviation / math.sqrt(count) if count else math.inf
        lcb = mean - LCB_STANDARD_ERROR_MULTIPLIER * standard_error
        output.append(
            {
                "component_id": member,
                "market": candidate.market,
                "session_code": candidate.session_code,
                "event_count": count,
                "stressed_net_total_usd": sum(values),
                "stressed_net_mean_usd": mean,
                "stressed_net_standard_error_usd": standard_error,
                "stressed_net_lcb_usd": lcb,
            }
        )
    return output


def _evaluate_era(
    contract: Mapping[str, Any], era: Mapping[str, Any]
) -> dict[str, Any]:
    matrices = _open_era_features(era)
    if set(matrices) != set(COMMON_MARKETS):
        raise DistributionallyRobustPolicyError(
            f"era market inventory drift: {era['era_id']}"
        )
    calendar = _era_calendar(era, matrices)
    starts = lane.non_overlapping_starts(calendar, HORIZONS)
    block = str(era["era_id"])
    starts = {
        horizon: tuple((int(day), block) for day, _ in rows)
        for horizon, rows in starts.items()
    }
    inventory = {
        str(key): dict(value) for key, value in contract["component_inventory"].items()
    }
    component_ids = sorted(
        {str(member) for cell in contract["cells"] for member in cell["component_ids"]}
    )
    cache: dict[str, dict[str, Any]] = {}
    for component_id in component_ids:
        _replay, materialized = sieve._component_replay(
            inventory[component_id], matrices, calendar
        )
        cache[component_id] = materialized

    identity = sieve.PROFILE_BY_ID["pnl_state_identity"]
    results = []
    exact_episodes = 0
    for raw in contract["cells"]:
        cell = dict(raw)
        members = [str(value) for value in cell["component_ids"]]
        trajectories = {scenario: {} for scenario in SCENARIOS}
        unavailable: set[int] = set()
        receipts = []
        for member in members:
            materialized = cache[member]
            quantity = int(cell["component_quantity_tiers"][member])
            normal = tuple(
                scale_causal_trajectory(value, executable_quantity_multiplier=quantity)
                for value in materialized["normal"]
            )
            stressed = tuple(
                scale_causal_trajectory(value, executable_quantity_multiplier=quantity)
                for value in materialized["stressed"]
            )
            _require_scenario_identity(normal, stressed)
            trajectories["NORMAL"][member] = normal
            trajectories["STRESSED_1_5X"][member] = stressed
            unavailable.update(materialized["unavailable_days"])
            receipts.append(
                {
                    "component_id": member,
                    "quantity_tier": quantity,
                    "decision_hash": materialized["decision_hash"],
                    "normal_trajectory_hash": materialized["normal_trajectory_hash"],
                    "stressed_trajectory_hash": materialized[
                        "stressed_trajectory_hash"
                    ],
                    "fill_policy_hash": materialized["fill_policy_hash"],
                }
            )
        rule = dict(contract["account_rules"])[str(cell["account_label"])]
        prepared = _PreparedPolicy(
            policy_id=str(cell["policy_id"]),
            source_kind="DISTRIBUTIONALLY_ROBUST_NOVEL_BOOK",
            evidence_tier="H",
            account_label=str(cell["account_label"]),
            baseline_policy=pairs._active_policy(cell, rule),
            trajectories=trajectories,
            unavailable_days=frozenset(unavailable),
            source_policy=cell,
            source_metrics={},
            source_hashes={},
        )
        evaluation = _evaluate_profile(
            prepared,
            identity,
            blocks=(block,),
            calendar=calendar,
            starts=starts,
            rule=rule,
        )
        exact_episodes += int(evaluation["exact_episode_count"])
        core = {
            "policy_id": cell["policy_id"],
            "book_id": cell["book_id"],
            "policy_spec_hash": cell["policy_spec_hash"],
            "account_label": cell["account_label"],
            "component_ids": members,
            "component_receipts": receipts,
            "strata": _stratum_receipts(members, inventory, cache),
            "evaluation": evaluation,
        }
        results.append({**core, "result_hash": stable_hash(core)})
    return {
        "era_id": block,
        "calendar": {
            "session_count": len(calendar),
            "first_session_day": int(calendar[0]),
            "last_session_day": int(calendar[-1]),
            "non_overlapping_start_counts": {
                str(key): len(value) for key, value in starts.items()
            },
        },
        "component_replay_count": len(cache),
        "exact_account_episode_count": exact_episodes,
        "results": results,
    }


def _summary(
    row: Mapping[str, Any], scenario: str, horizon: int
) -> dict[str, Any]:
    return dict(row["evaluation"]["summaries"][scenario][str(horizon)])


def _pessimistic_score(
    rows: Sequence[Mapping[str, Any]], horizon: int
) -> tuple[Any, ...]:
    normal = [_summary(row, "NORMAL", horizon) for row in rows]
    stressed = [_summary(row, "STRESSED_1_5X", horizon) for row in rows]
    stratum_lcbs = [
        float(value["stressed_net_lcb_usd"])
        for row in rows
        for value in row["strata"]
    ]
    return (
        min(int(value["pass_count"]) for value in stressed),
        min(int(value["pass_count"]) for value in normal),
        sum(int(value["pass_count"]) > 0 for value in stressed),
        sum(int(value["pass_count"]) > 0 for value in normal),
        min(float(value["target_progress_p25"]) for value in stressed),
        min(float(value["target_progress_median"]) for value in stressed),
        min(float(value["net_total_usd"]) for value in stressed),
        min(float(value["minimum_mll_buffer_usd"] or -1e18) for value in stressed),
        min(stratum_lcbs or [-1e18]),
        -horizon,
    )


def _decision(
    rows: Sequence[Mapping[str, Any]], horizon: int
) -> dict[str, Any]:
    normal = [_summary(row, "NORMAL", horizon) for row in rows]
    stressed = [_summary(row, "STRESSED_1_5X", horizon) for row in rows]
    zero_mll = all(
        int(value["mll_breach_count"]) == 0 for value in normal + stressed
    )
    no_compliance_failure = all(
        int(value["terminal_distribution"].get("COMPLIANCE_FAILURE", 0)) == 0
        and int(value["terminal_distribution"].get("HARD_RULE_FAILURE", 0)) == 0
        for value in normal + stressed
    )
    positive_every_era = all(float(value["net_total_usd"]) > 0.0 for value in stressed)
    normal_pass_eras = sum(int(value["pass_count"]) > 0 for value in normal)
    stressed_pass_eras = sum(int(value["pass_count"]) > 0 for value in stressed)
    substantial_lq = (
        min(float(value["target_progress_p25"]) for value in stressed)
        >= SUBSTANTIAL_LOWER_QUARTILE_PROGRESS
    )
    tier_g = (
        zero_mll
        and no_compliance_failure
        and positive_every_era
        and normal_pass_eras >= 2
        and stressed_pass_eras >= 2
    )
    progress_only = (
        zero_mll
        and no_compliance_failure
        and positive_every_era
        and substantial_lq
        and not tier_g
    )
    if tier_g:
        status = "TIER_G_DISTRIBUTIONALLY_ROBUST_DEVELOPMENT_BOOK"
        tier = "G"
    elif progress_only:
        status = "ROBUST_PROGRESS_ONLY_NO_PROMOTION"
        tier = None
    else:
        status = "DRO_GATE_REJECTED"
        tier = None
    return {
        "status": status,
        "evidence_tier": tier,
        "checks": {
            "positive_stressed_net_every_era": positive_every_era,
            "zero_mll_breach": zero_mll,
            "no_compliance_failure": no_compliance_failure,
            "normal_pass_era_count": normal_pass_eras,
            "stressed_pass_era_count": stressed_pass_eras,
            "substantial_lower_quartile_progress": substantial_lq,
        },
    }


def _aggregate(contract: Mapping[str, Any], era_results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_era = {
        str(value["era_id"]): {
            str(row["policy_id"]): dict(row) for row in value["results"]
        }
        for value in era_results
    }
    era_ids = [str(value["era_id"]) for value in ERAS]
    cells = {str(row["policy_id"]): dict(row) for row in contract["cells"]}
    aggregated = []
    for policy_id, cell in cells.items():
        rows = [by_era[era][policy_id] for era in era_ids]
        best_horizon = max(HORIZONS, key=lambda horizon: _pessimistic_score(rows, horizon))
        decision = _decision(rows, best_horizon)
        behavior_hash = stable_hash(
            {
                "component_ids": cell["component_ids"],
                "normal_paths": [
                    _summary(row, "NORMAL", best_horizon)["episode_path_hash"]
                    for row in rows
                ],
                "stressed_paths": [
                    _summary(row, "STRESSED_1_5X", best_horizon)["episode_path_hash"]
                    for row in rows
                ],
            }
        )
        core = {
            "policy_id": policy_id,
            "book_id": cell["book_id"],
            "policy_spec_hash": cell["policy_spec_hash"],
            "account_label": cell["account_label"],
            "component_ids": cell["component_ids"],
            "selected_horizon_trading_days": best_horizon,
            "pessimistic_score": list(_pessimistic_score(rows, best_horizon)),
            "decision": decision,
            "behavior_hash": behavior_hash,
            "era_results": {
                era: {
                    "normal": _summary(by_era[era][policy_id], "NORMAL", best_horizon),
                    "stressed": _summary(
                        by_era[era][policy_id], "STRESSED_1_5X", best_horizon
                    ),
                    "strata": by_era[era][policy_id]["strata"],
                }
                for era in era_ids
            },
        }
        aggregated.append({**core, "result_hash": stable_hash(core)})

    # Proper leave-one-era-out ranking: horizon and candidate ranking use only
    # the other two eras.  Held-era metrics are reported but never used in that
    # fold's selection.
    loeo = []
    loeo_counts: dict[str, int] = {}
    for held in era_ids:
        train = [era for era in era_ids if era != held]
        ranked = []
        for policy_id, cell in cells.items():
            train_rows = [by_era[era][policy_id] for era in train]
            horizon = max(
                HORIZONS, key=lambda value: _pessimistic_score(train_rows, value)
            )
            ranked.append(
                {
                    "policy_id": policy_id,
                    "book_id": cell["book_id"],
                    "account_label": cell["account_label"],
                    "horizon_trading_days": horizon,
                    "train_score": list(_pessimistic_score(train_rows, horizon)),
                }
            )
        ranked.sort(key=lambda row: tuple(row["train_score"]), reverse=True)
        selected = []
        seen_books: set[str] = set()
        for row in ranked:
            if row["book_id"] in seen_books:
                continue
            policy_id = str(row["policy_id"])
            horizon = int(row["horizon_trading_days"])
            held_row = by_era[held][policy_id]
            selected.append(
                {
                    **row,
                    "held_era": held,
                    "held_normal": _summary(held_row, "NORMAL", horizon),
                    "held_stressed": _summary(held_row, "STRESSED_1_5X", horizon),
                }
            )
            seen_books.add(str(row["book_id"]))
            loeo_counts[policy_id] = loeo_counts.get(policy_id, 0) + 1
            if len(selected) == LOEO_RETAINED_PER_FOLD:
                break
        loeo.append({"held_era": held, "training_eras": train, "selected": selected})

    for row in aggregated:
        row["loeo_selection_count"] = loeo_counts.get(str(row["policy_id"]), 0)
    best_by_book: dict[str, dict[str, Any]] = {}
    for row in aggregated:
        prior = best_by_book.get(str(row["book_id"]))
        if prior is None or tuple(row["pessimistic_score"]) > tuple(
            prior["pessimistic_score"]
        ):
            best_by_book[str(row["book_id"])] = row
    ranked_books = sorted(
        best_by_book.values(),
        key=lambda row: (
            row["decision"]["status"]
            == "TIER_G_DISTRIBUTIONALLY_ROBUST_DEVELOPMENT_BOOK",
            row["decision"]["status"] == "ROBUST_PROGRESS_ONLY_NO_PROMOTION",
            row["loeo_selection_count"],
            tuple(row["pessimistic_score"]),
        ),
        reverse=True,
    )
    eligible = [
        row
        for row in ranked_books
        if row["decision"]["status"]
        in {
            "TIER_G_DISTRIBUTIONALLY_ROBUST_DEVELOPMENT_BOOK",
            "ROBUST_PROGRESS_ONLY_NO_PROMOTION",
        }
    ]
    retained = eligible[:MAXIMUM_RETAINED]
    g = [
        row
        for row in retained
        if row["decision"]["status"]
        == "TIER_G_DISTRIBUTIONALLY_ROBUST_DEVELOPMENT_BOOK"
    ]
    verdict = (
        "DISTRIBUTIONALLY_ROBUST_POLICY_GREEN"
        if g
        else (
            "DISTRIBUTIONALLY_ROBUST_POLICY_WEAK_PROGRESS_ONLY"
            if retained
            else "DISTRIBUTIONALLY_ROBUST_POLICY_FALSIFIED"
        )
    )
    core = {
        "schema": f"{SCHEMA}_economic_result",
        "status": "DISTRIBUTIONALLY_ROBUST_REPLAY_COMPLETE",
        "verdict": verdict,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY_NO_CONFIRMATION_CLAIM",
        "contract_hash": contract["contract_hash"],
        "serious_cell_count": len(aggregated),
        "membership_book_count": len(best_by_book),
        "exact_account_episode_count": sum(
            int(value["exact_account_episode_count"]) for value in era_results
        ),
        "component_replay_count": sum(
            int(value["component_replay_count"]) for value in era_results
        ),
        "era_calendars": {
            str(value["era_id"]): value["calendar"] for value in era_results
        },
        "loeo": loeo,
        "tier_g_count": len(g),
        "progress_only_count": len(retained) - len(g),
        "retained_count": len(retained),
        "retained": retained,
        "top_12_even_when_rejected": ranked_books[:12],
        "all_cell_results": aggregated,
        "strategy_or_risk_retuning_performed": False,
        "closed_exact_formulations_recycled": False,
        "new_data_purchase": False,
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": "G" if g else None,
        "next_action": (
            "FREEZE_DISTINCT_TIER_G_BOOKS_FOR_GENUINELY_FRESH_CONFIRMATION"
            if g
            else (
                "PRESERVE_PROGRESS_ONLY_BOOKS_AND_START_DISTINCT_EVENT_TIME_"
                "VOLATILITY_BURST_DIRECT_ACCOUNT_POLICY"
            )
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def run(output_dir: Path = OUTPUT_DIR) -> dict[str, Any]:
    os.environ.update(
        {
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        }
    )
    started = time.perf_counter()
    contract = freeze_contract(output_dir)
    _verify_hash(contract, "contract_hash", label="DRO selection contract")
    era_results = []
    for index, era in enumerate(ERAS, start=1):
        _write(
            output_dir / STATE_NAME,
            {
                "status": "DISTRIBUTIONALLY_ROBUST_REPLAY_ACTIVE",
                "active_era": era["era_id"],
                "completed_era_count": index - 1,
                "total_era_count": len(ERAS),
                "serious_cell_count": contract["serious_cell_count"],
                "worker_processes": 1,
                "numeric_threads_per_worker": 1,
            },
        )
        era_results.append(_evaluate_era(contract, era))
    result = _aggregate(contract, era_results)
    result["runtime_seconds"] = time.perf_counter() - started
    # Runtime is operational metadata and excluded from the deterministic hash.
    _write(output_dir / RESULT_NAME, result, immutable=True)
    _write(
        output_dir / STATE_NAME,
        {
            "status": "DISTRIBUTIONALLY_ROBUST_REPLAY_COMPLETE",
            "verdict": result["verdict"],
            "serious_cell_count": result["serious_cell_count"],
            "exact_account_episode_count": result["exact_account_episode_count"],
            "tier_g_count": result["tier_g_count"],
            "retained_count": result["retained_count"],
            "runtime_seconds": result["runtime_seconds"],
            "next_action": result["next_action"],
        },
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    result = run(ROOT / OUTPUT_DIR)
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status",
                    "verdict",
                    "serious_cell_count",
                    "exact_account_episode_count",
                    "tier_g_count",
                    "progress_only_count",
                    "retained_count",
                    "runtime_seconds",
                    "result_hash",
                    "next_action",
                )
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
