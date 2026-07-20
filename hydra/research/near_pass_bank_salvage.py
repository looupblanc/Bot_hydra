"""Targeted recovery of clean near-pass policies from immutable 0029 evidence.

This is deliberately not a new strategy grammar.  It selects at most twenty
four behaviourally distinct, non-banked causal candidates from the exhausted
FAST-PASS source bank and evaluates the already-preregistered PnL-state sizing
profiles on the frozen 5/10/20-day grid and official 50K/100K/150K rules.

Selection and profile fitting use B1/B2 only.  B3/B4 are replayed after the
choice is frozen.  A normal Combine pass is preserved independently of the
stress result; causality, session compliance, MLL and exact account accounting
remain hard requirements.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    HORIZONS,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _require_scenario_identity,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.research.causal_target_velocity import HazardOutcome
from hydra.research.pnl_state_risk_frontier import (
    ALL_BLOCKS,
    DESIGN_BLOCKS,
    DEFAULT_CANDIDATE_BANK,
    DEFAULT_EXACT_SOURCE_PATHS,
    PnLStateRiskFrontierError,
    _PreparedPolicy,
    _design_rank,
    _evaluate_profile,
    _raw_component,
    _read_json,
    _unwrap,
    frozen_pnl_state_profiles,
)


SCHEMA = "hydra_near_pass_bank_salvage_v1"
SELECTION_SCHEMA = "hydra_near_pass_bank_salvage_selection_v1"
OUTPUT_DIR = Path("reports/economic_evolution/near_pass_bank_salvage_v1")
PASS_BANK = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/"
    "combine_pass_observed_bank.json"
)
OPERATIONAL_SUMMARY = Path(
    "reports/economic_evolution/operational_candidate_bank_v2/bank_summary.json"
)
MAXIMUM_SHORTLIST = 24
RECOVERY_TARGET = 6
ACCOUNT_LABELS = ("50K", "100K", "150K")


class NearPassSalvageError(RuntimeError):
    """Immutable sources cannot support the bounded recovery run."""


def _recovery_verdict(recovered_count: int) -> str:
    """Return the frozen terminal verdict for the six-slot recovery sprint."""

    return (
        "TARGETED_NEAR_PASS_RECOVERY_COMPLETE"
        if int(recovered_count) >= RECOVERY_TARGET
        else "INVENTORY_NEAR_PASS_EXHAUSTED"
    )


def _exact_result(wrapper: Mapping[str, Any]) -> dict[str, Any]:
    continuation = wrapper.get("continuation_result")
    if isinstance(continuation, Mapping):
        nested = continuation.get("exact_result")
        if isinstance(nested, Mapping):
            return dict(nested)
    if isinstance(wrapper.get("results"), list):
        return dict(wrapper)
    raise NearPassSalvageError("exact source wrapper has no result")


def _source_candidates(project: Path) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for relative in DEFAULT_EXACT_SOURCE_PATHS:
        wrapper = _read_json((project / relative).resolve())
        exact = _exact_result(wrapper)
        for raw in exact.get("results", ()):
            row = dict(raw)
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id:
                raise NearPassSalvageError("exact source candidate lacks identity")
            prior = output.get(candidate_id)
            if prior is not None and stable_hash(prior) != stable_hash(row):
                raise NearPassSalvageError(
                    f"duplicate exact candidate drift: {candidate_id}"
                )
            output[candidate_id] = row
    return output


def _pass_and_quarantine_exclusions(project: Path) -> dict[str, Any]:
    pass_wrapper = _read_json((project / PASS_BANK).resolve())
    pass_bank = _unwrap(pass_wrapper, "combine_pass_observed_bank")
    summary = _read_json((project / OPERATIONAL_SUMMARY).resolve())
    excluded_ids = {str(value) for value in pass_bank.get("policy_ids", ())}
    excluded_specs: set[str] = set()
    excluded_behaviors: set[str] = set()
    for raw in pass_bank.get("policies", ()):
        row = dict(raw)
        excluded_ids.update(str(value) for value in row.get("components", ()))
        fingerprints = dict(row.get("fingerprints") or {})
        if fingerprints.get("policy_spec_hash"):
            excluded_specs.add(str(fingerprints["policy_spec_hash"]))
        if fingerprints.get("realized_behavioral_fingerprint"):
            excluded_behaviors.add(
                str(fingerprints["realized_behavioral_fingerprint"])
            )
    quarantined = {
        str(value)
        for value in dict(summary.get("source_reconciliation") or {}).get(
            "quarantined_policy_ids", ()
        )
    }
    failed_confirmation = {
        str(value)
        for value, status in dict(
            dict(summary.get("confirmation") or {}).get(
                "tier_g_terminal_statuses", {}
            )
        ).items()
        if "FAIL" in str(dict(status).get("status") or "")
    }
    return {
        "pass_bank_hash": str(pass_bank["result_hash"]),
        "operational_summary_hash": str(summary["result_hash"]),
        "excluded_ids": excluded_ids | quarantined | failed_confirmation,
        "excluded_specs": excluded_specs,
        "excluded_behaviors": excluded_behaviors,
        "quarantined_ids": sorted(quarantined),
        "confirmation_failed_ids": sorted(failed_confirmation),
    }


def _cell_design_score(cell: Mapping[str, Any]) -> tuple[Any, ...]:
    weights = {5: 4, 10: 2, 20: 1}
    horizon = int(cell["horizon_trading_days"])
    normal = dict(cell["normal"])
    stressed = dict(cell["stressed"])
    normal_blocks = dict(normal.get("by_block") or {})
    stressed_blocks = dict(stressed.get("by_block") or {})
    design_normal_passes = sum(
        int(dict(normal_blocks.get(block) or {}).get("pass_count", 0))
        for block in DESIGN_BLOCKS
    )
    design_stressed_passes = sum(
        int(dict(stressed_blocks.get(block) or {}).get("pass_count", 0))
        for block in DESIGN_BLOCKS
    )
    design_stressed_net = sum(
        float(dict(stressed_blocks.get(block) or {}).get("net_total_usd", 0.0))
        for block in DESIGN_BLOCKS
    )
    progress = statistics.mean(
        float(dict(normal_blocks.get(block) or {}).get("target_progress_median", 0.0))
        for block in DESIGN_BLOCKS
    )
    return (
        weights[horizon] * design_normal_passes,
        weights[horizon] * design_stressed_passes,
        progress,
        design_stressed_net,
        int(cell["integer_quantity_tier"]),
    )


def _safe_static_cells(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    if int(dict(candidate.get("session_contract") or {}).get(
        "event_violation_count", 0
    )) != 0:
        return []
    return [
        dict(cell)
        for cell in candidate.get("frontier", ())
        if cell.get("legally_executable") is True
        and cell.get("account_rule_compliant") is True
        and int(cell.get("hard_compliance_failure_count", 0)) == 0
        and str(cell.get("risk_governor_mode"))
        == "CAUSAL_STATIC_STOP_RISK_GOVERNOR"
        and float(dict(cell.get("normal") or {}).get("mll_breach_rate", 1.0))
        <= 0.10
        and float(dict(cell.get("stressed") or {}).get("mll_breach_rate", 1.0))
        <= 0.10
    ]


def _cell_fingerprint(cell: Mapping[str, Any]) -> str:
    """Return the canonical raw-cell identity (raw exact rows predate cell_hash)."""

    claimed = cell.get("cell_hash")
    return str(claimed) if claimed else stable_hash(dict(cell))


def _selection_rank(candidate: Mapping[str, Any], cells: Sequence[Mapping[str, Any]]) -> tuple[Any, ...]:
    spec = dict(candidate.get("candidate") or {})
    market = str(spec.get("market") or "")
    session = int(spec.get("session_code", 99))
    market_priority = {"RTY": 5, "CL": 4, "ES": 4, "YM": 2, "NQ": 1}.get(
        market, 0
    )
    session_priority = 2 if session in {-1, 2} else 0
    positive_stress = max(
        float(dict(cell["stressed"]).get("net_total_usd", 0.0)) for cell in cells
    )
    maximum_progress = max(
        float(dict(cell["normal"]).get("target_progress_median", 0.0))
        for cell in cells
    )
    return (
        int(positive_stress > 0.0),
        maximum_progress,
        positive_stress,
        session_priority,
        market_priority,
        int(candidate.get("source_completed_event_count", 0)),
    )


def build_selection(root: str | Path, *, maximum: int = MAXIMUM_SHORTLIST) -> dict[str, Any]:
    project = Path(root).resolve()
    if not 1 <= int(maximum) <= MAXIMUM_SHORTLIST:
        raise NearPassSalvageError("shortlist maximum must be in [1,24]")
    source = _source_candidates(project)
    exclusions = _pass_and_quarantine_exclusions(project)
    candidate_wrapper = _read_json((project / DEFAULT_CANDIDATE_BANK).resolve())
    classified = _unwrap(candidate_wrapper, "candidate_bank")
    classified_by_id = {
        str(row["candidate_id"]): dict(row)
        for row in classified.get("candidates", ())
    }
    eligible: list[dict[str, Any]] = []
    rejection_counts: Counter[str] = Counter()
    for candidate_id, candidate in source.items():
        if candidate_id in exclusions["excluded_ids"]:
            rejection_counts["ALREADY_BANKED_QUARANTINED_OR_CONFIRMATION_FAILED"] += 1
            continue
        spec_hash = str(candidate.get("candidate_fingerprint") or "")
        behavior = str(candidate.get("realized_behavioral_fingerprint") or "")
        if spec_hash in exclusions["excluded_specs"]:
            rejection_counts["DUPLICATE_SPEC"] += 1
            continue
        if behavior in exclusions["excluded_behaviors"]:
            rejection_counts["DUPLICATE_REALIZED_BEHAVIOR"] += 1
            continue
        cells = _safe_static_cells(candidate)
        if not cells:
            rejection_counts["NO_CAUSAL_MLL_SAFE_STATIC_CELL"] += 1
            continue
        if max(float(dict(cell["stressed"])["net_total_usd"]) for cell in cells) <= 0:
            rejection_counts["NO_POSITIVE_STRESSED_REFERENCE_CELL"] += 1
            continue
        classified_row = classified_by_id.get(candidate_id)
        if not classified_row or not dict(
            classified_row.get("compact_evidence_bundle") or {}
        ).get("source_event_file_sha256"):
            rejection_counts["INCOMPLETE_EXECUTABLE_EVIDENCE"] += 1
            continue
        spec = dict(candidate.get("candidate") or {})
        eligible.append(
            {
                "candidate_id": candidate_id,
                "candidate_fingerprint": spec_hash,
                "realized_behavioral_fingerprint": behavior,
                "market": spec.get("market"),
                "execution_market": spec.get("execution_market"),
                "session_code": spec.get("session_code"),
                "mechanism": spec.get("mechanism"),
                "timeframe": spec.get("timeframe"),
                "holding_horizon": spec.get("horizon"),
                "source_completed_event_count": int(
                    candidate.get("source_completed_event_count", 0)
                ),
                "selection_rank": list(_selection_rank(candidate, cells)),
                "account_base_cells": {
                    account: _cell_fingerprint(max(
                        (cell for cell in cells if cell["account_label"] == account),
                        key=_cell_design_score,
                    ))
                    for account in ACCOUNT_LABELS
                },
            }
        )

    # Preserve under-represented markets without sacrificing target velocity.
    # Quotas are ceilings rather than obligations; unavailable slots fall back
    # to the global quality ordering.
    eligible.sort(key=lambda row: (tuple(row["selection_rank"]), row["candidate_id"]), reverse=True)
    retained: list[dict[str, Any]] = []
    used_niches: set[tuple[Any, ...]] = set()
    used_behaviors: set[str] = set()
    quotas = {"RTY": 4, "CL": 6, "ES": 6, "YM": 5, "NQ": 3}
    for market, quota in quotas.items():
        added = 0
        for row in eligible:
            if row["market"] != market:
                continue
            niche = (
                row["market"], row["session_code"], row["mechanism"], row["timeframe"]
            )
            behavior = str(row["realized_behavioral_fingerprint"])
            if niche in used_niches or behavior in used_behaviors:
                continue
            retained.append(row)
            used_niches.add(niche)
            used_behaviors.add(behavior)
            added += 1
            if added == quota or len(retained) == int(maximum):
                break
    if len(retained) < int(maximum):
        for row in eligible:
            behavior = str(row["realized_behavioral_fingerprint"])
            if behavior in used_behaviors:
                continue
            retained.append(row)
            used_behaviors.add(behavior)
            if len(retained) == int(maximum):
                break
    core = {
        "schema": SELECTION_SCHEMA,
        "status": "FROZEN_BEFORE_DYNAMIC_REPLAY",
        "source_exact_candidate_count": len(source),
        "eligible_near_pass_count": len(eligible),
        "selected_candidate_count": len(retained),
        "maximum_candidate_count": int(maximum),
        "selected": retained,
        "selected_candidate_ids": [row["candidate_id"] for row in retained],
        "rejection_counts": dict(sorted(rejection_counts.items())),
        "exclusions": {
            "pass_bank_hash": exclusions["pass_bank_hash"],
            "operational_summary_hash": exclusions["operational_summary_hash"],
            "quarantined_ids": exclusions["quarantined_ids"],
            "confirmation_failed_ids": exclusions["confirmation_failed_ids"],
            "tombstone_contract": "SOURCE_0029_BANK_ALREADY_CEMETERY_SCREENED",
        },
        "selection_role": "VIEWED_DEVELOPMENT_B1_B2_ONLY",
        "stress_is_robustness_metric_not_normal_pass_veto": True,
        "no_new_strategy_grammar": True,
        "no_data_purchase": True,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "selection_hash": stable_hash(core)}


def _find_cell(candidate: Mapping[str, Any], cell_hash: str) -> dict[str, Any]:
    matches = [
        dict(cell)
        for cell in candidate.get("frontier", ())
        if _cell_fingerprint(cell) == str(cell_hash)
    ]
    if len(matches) != 1:
        raise NearPassSalvageError("frozen base cell is absent or duplicated")
    return matches[0]


def _candidate_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    os.environ.update(
        {
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        }
    )
    project = Path(str(payload["root"])).resolve()
    selection_rows = [dict(value) for value in payload["selection_rows"]]
    source = _source_candidates(project)
    candidate_wrapper = _read_json((project / DEFAULT_CANDIDATE_BANK).resolve())
    candidate_bank = _unwrap(candidate_wrapper, "candidate_bank")
    classified = {
        str(row["candidate_id"]): dict(row)
        for row in candidate_bank.get("candidates", ())
    }
    manifest = _load_self_hashed_manifest(project / DEFAULT_FAST_PASS_MANIFEST)
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(project / DEFAULT_RULE_SNAPSHOT)
    bank_entries, bank_receipt = _load_banks(project)
    profiles = frozen_pnl_state_profiles()
    raw_cache: dict[str, Any] = {}
    results: list[dict[str, Any]] = []
    exact_episode_count = 0

    for selected in selection_rows:
        candidate_id = str(selected["candidate_id"])
        exact_candidate = source[candidate_id]
        classified_row = classified[candidate_id]
        raw = _raw_component(project, classified_row, bank_entries, raw_cache)
        unavailable = set(int(value) for value in calendar).difference(raw.eligible_days)
        unavailable.update(raw.censored_days)
        account_results = []
        for account_label in ACCOUNT_LABELS:
            cell = _find_cell(
                exact_candidate,
                str(dict(selected["account_base_cells"])[account_label]),
            )
            tier = int(cell["integer_quantity_tier"])
            normal = tuple(
                scale_causal_trajectory(value, executable_quantity_multiplier=tier)
                for value in raw.normal
            )
            stressed = tuple(
                scale_causal_trajectory(value, executable_quantity_multiplier=tier)
                for value in raw.stressed
            )
            _require_scenario_identity(normal, stressed)
            prepared = _PreparedPolicy(
                policy_id=candidate_id,
                source_kind="EXACT_STANDALONE_NEAR_PASS",
                evidence_tier="E",
                account_label=account_label,
                baseline_policy=ActiveRiskPoolPolicy.from_mapping(
                    dict(cell["account_policy"])
                ),
                trajectories={
                    "NORMAL": {candidate_id: normal},
                    "STRESSED_1_5X": {candidate_id: stressed},
                },
                unavailable_days=frozenset(unavailable),
                source_policy=classified_row,
                source_metrics={},
                source_hashes={
                    "source_exact_result_hash": classified_row[
                        "source_exact_result_hash"
                    ],
                    "source_event_file_sha256": dict(
                        classified_row["compact_evidence_bundle"]
                    )["source_event_file_sha256"],
                    "frozen_base_cell_hash": _cell_fingerprint(cell),
                },
            )
            design = [
                _evaluate_profile(
                    prepared,
                    profile,
                    blocks=DESIGN_BLOCKS,
                    calendar=calendar,
                    starts=starts,
                    rule=rules[account_label],
                )
                for profile in profiles
            ]
            exact_episode_count += sum(
                int(value["exact_episode_count"]) for value in design
            )
            selected_design = max(design, key=_design_rank)
            selected_profile = next(
                profile
                for profile in profiles
                if profile.profile_id == selected_design["profile_id"]
            )
            full = _evaluate_profile(
                prepared,
                selected_profile,
                blocks=ALL_BLOCKS,
                calendar=calendar,
                starts=starts,
                rule=rules[account_label],
            )
            exact_episode_count += int(full["exact_episode_count"])
            account_results.append(
                {
                    "account_label": account_label,
                    "base_integer_quantity_tier": tier,
                    "frozen_base_cell_hash": _cell_fingerprint(cell),
                    "selected_profile_id": selected_profile.profile_id,
                    "selected_on_blocks": list(DESIGN_BLOCKS),
                    "selection_held_out_fields_used": False,
                    "design_frontier": design,
                    "all_block_result": full,
                }
            )
        results.append(
            {
                "candidate_id": candidate_id,
                "candidate_fingerprint": selected["candidate_fingerprint"],
                "realized_behavioral_fingerprint": selected[
                    "realized_behavioral_fingerprint"
                ],
                "niches": {
                    key: selected[key]
                    for key in (
                        "market", "execution_market", "session_code", "mechanism",
                        "timeframe", "holding_horizon",
                    )
                },
                "account_results": account_results,
            }
        )
    return {
        "results": results,
        "exact_episode_count": exact_episode_count,
        "grid_hash": grid_receipt["grid_hash"],
        "rule_snapshot_hash": rule_receipt["parsed_rule_hash"],
        "source_bank_receipt_hash": stable_hash(bank_receipt),
    }


def _pass_cells(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    output = []
    for account in candidate.get("account_results", ()):
        profile = str(account["selected_profile_id"])
        summaries = dict(account["all_block_result"])["summaries"]
        for horizon in HORIZONS:
            normal = dict(summaries["NORMAL"][str(horizon)])
            stressed = dict(summaries["STRESSED_1_5X"][str(horizon)])
            normal_pass = int(normal["pass_count"]) > 0
            hard_safe = (
                int(normal["mll_breach_count"]) == 0
                and int(stressed["mll_breach_count"]) == 0
            )
            if normal_pass and hard_safe:
                output.append(
                    {
                        "account_label": account["account_label"],
                        "horizon_trading_days": horizon,
                        "profile_id": profile,
                        "base_integer_quantity_tier": account[
                            "base_integer_quantity_tier"
                        ],
                        "normal": normal,
                        "stressed_robustness": stressed,
                        "classification_status": (
                            "COMBINE_PASS_OBSERVED_DEVELOPMENT"
                        ),
                        "stress_pass_required_for_status": False,
                    }
                )
    return output


def run_salvage(root: str | Path, selection: Mapping[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    rows = [dict(value) for value in selection.get("selected", ())]
    if not rows or len(rows) > MAXIMUM_SHORTLIST:
        raise NearPassSalvageError("frozen shortlist is empty or oversized")
    midpoint = (len(rows) + 1) // 2
    shards = [rows[:midpoint], rows[midpoint:]]
    shards = [value for value in shards if value]
    payloads = [
        {"root": str(project), "selection_rows": shard}
        for shard in shards
    ]
    worker_results = []
    with ProcessPoolExecutor(max_workers=min(2, len(payloads))) as pool:
        futures = [pool.submit(_candidate_worker, payload) for payload in payloads]
        for future in as_completed(futures):
            worker_results.append(future.result())
    results = [
        value
        for worker in worker_results
        for value in worker["results"]
    ]
    results.sort(key=lambda value: str(value["candidate_id"]))
    pass_rows = []
    for candidate in results:
        cells = _pass_cells(candidate)
        if cells:
            pass_rows.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "candidate_fingerprint": candidate["candidate_fingerprint"],
                    "realized_behavioral_fingerprint": candidate[
                        "realized_behavioral_fingerprint"
                    ],
                    "behavior_cluster_id": "near_pass_salvage_"
                    + str(candidate["realized_behavioral_fingerprint"])[:16],
                    "niches": candidate["niches"],
                    "observed_pass_cells": cells,
                    "classification_status": "COMBINE_PASS_OBSERVED_DEVELOPMENT",
                    "evidence_tier": "E",
                    "independent_confirmation_claimed": False,
                    "promotion_status": None,
                }
            )
    # One immutable behaviour representative per recovered strategy.
    deduped = []
    seen = set()
    for row in pass_rows:
        fingerprint = str(row["realized_behavioral_fingerprint"])
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduped.append(row)
        if len(deduped) == RECOVERY_TARGET:
            break
    source_hashes = {
        "grid_hashes": sorted({str(w["grid_hash"]) for w in worker_results}),
        "rule_snapshot_hashes": sorted(
            {str(w["rule_snapshot_hash"]) for w in worker_results}
        ),
        "source_bank_receipt_hashes": sorted(
            {str(w["source_bank_receipt_hash"]) for w in worker_results}
        ),
    }
    exact_episodes = sum(int(w["exact_episode_count"]) for w in worker_results)
    runtime = time.perf_counter() - started
    recovered_count = len(deduped)
    status = _recovery_verdict(recovered_count)
    exhaustion = None
    if status == "INVENTORY_NEAR_PASS_EXHAUSTED":
        rejection_counts = dict(selection.get("rejection_counts") or {})
        exhaustion = {
            "recovery_target": RECOVERY_TARGET,
            "recovered_count": recovered_count,
            "remaining_shortage": max(RECOVERY_TARGET - recovered_count, 0),
            "source_exact_candidate_count": int(
                selection.get("source_exact_candidate_count", 0)
            ),
            "eligible_near_pass_count": int(
                selection.get("eligible_near_pass_count", 0)
            ),
            "selected_distinct_candidate_count": len(rows),
            "selected_without_normal_pass_count": len(rows) - len(pass_rows),
            "source_rejection_counts": rejection_counts,
            "bounded_search_contract": [
                "EXHAUSTIVE_IMMUTABLE_0029_EXACT_SOURCE_ONLY",
                "BANKED_QUARANTINED_CONFIRMATION_FAILED_AND_CLONES_EXCLUDED",
                "NO_NEIGHBOURING_RISK_GRID_EXPANSION",
                "NO_NEW_STRATEGY_GRAMMAR",
            ],
        }
    core = {
        "schema": SCHEMA,
        "status": status,
        "inventory_exhaustion": exhaustion,
        "selection_hash": selection["selection_hash"],
        "source_hashes": source_hashes,
        "candidate_results": results,
        "recovered_strategies": deduped,
        "counts": {
            "selected_candidate_count": len(rows),
            "account_policy_replay_count": len(rows) * len(ACCOUNT_LABELS),
            "profile_design_evaluation_count": (
                len(rows) * len(ACCOUNT_LABELS) * len(frozen_pnl_state_profiles())
            ),
            "exact_account_episode_count": exact_episodes,
            "candidate_with_normal_pass_count": len(pass_rows),
            "clean_behaviorally_distinct_recovered_count": recovered_count,
            "shortage_to_six": max(RECOVERY_TARGET - recovered_count, 0),
            "normal_mll_breach_in_recovered_count": sum(
                int(cell["normal"]["mll_breach_count"])
                for row in deduped
                for cell in row["observed_pass_cells"]
            ),
            "stressed_mll_breach_in_recovered_count": sum(
                int(cell["stressed_robustness"]["mll_breach_count"])
                for row in deduped
                for cell in row["observed_pass_cells"]
            ),
        },
        "normal_pass_contract": {
            "normal_pass_is_preserved_without_stressed_pass": True,
            "stress_is_advisory_robustness_metric": True,
            "mll_and_consistency_remain_hard": True,
            "causality_and_session_contract_remain_hard": True,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "independent_confirmation_claimed": False,
        "promotion_status": None,
        "economic_compute": {
            "worker_processes": min(2, len(payloads)),
            "numeric_threads_per_worker": 1,
            "runtime_seconds": runtime,
            "episodes_per_hour": exact_episodes / max(runtime, 1e-9) * 3600.0,
        },
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "exact_next_action": (
            "APPEND_RECOVERED_ROWS_TO_DERIVED_OPERATIONAL_BANK_WITHOUT_STATUS_INFLATION"
            if deduped
            else "INVENTORY_NEAR_PASS_EXHAUSTED_SELECT_MATERIALLY_DISTINCT_SOURCE"
        ),
    }
    hash_core = json.loads(json.dumps(core))
    hash_core["economic_compute"].pop("runtime_seconds", None)
    hash_core["economic_compute"].pop("episodes_per_hour", None)
    return {**core, "result_hash": stable_hash(hash_core)}


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--maximum", type=int, default=MAXIMUM_SHORTLIST)
    args = parser.parse_args(argv)
    project = Path(args.root).resolve()
    output = (project / args.output_dir).resolve()
    selection = build_selection(project, maximum=args.maximum)
    _write_json(output / "selection_checkpoint.json", selection)
    _write_json(
        output / "production_state.json",
        {
            "schema": "hydra_near_pass_bank_salvage_state_v1",
            "status": "ECONOMIC_REPLAY_RUNNING",
            "pid": os.getpid(),
            "selected_candidate_count": selection["selected_candidate_count"],
            "worker_processes": 2,
            "started_at_utc": datetime.now(UTC).isoformat(),
            "selection_hash": selection["selection_hash"],
        },
    )
    result = run_salvage(project, selection)
    _write_json(output / "economic_result.json", result)
    _write_json(
        output / "production_state.json",
        {
            "schema": "hydra_near_pass_bank_salvage_state_v1",
            "status": "COMPLETE",
            "pid": os.getpid(),
            "selected_candidate_count": selection["selected_candidate_count"],
            "exact_account_episode_count": result["counts"][
                "exact_account_episode_count"
            ],
            "recovered_count": result["counts"][
                "clean_behaviorally_distinct_recovered_count"
            ],
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "result_hash": result["result_hash"],
        },
    )
    print(json.dumps({
        "status": result["status"],
        "counts": result["counts"],
        "result_hash": result["result_hash"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
