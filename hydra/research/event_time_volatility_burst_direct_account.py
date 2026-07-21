"""Bounded causal variance-clock direct-account experiment.

This is the preregistered alternative to the falsified multi-era DRO book
search.  It uses an irregular volatility-budget clock, not a bar-time
volatility threshold: each clock resets only when cumulative *causally
normalised* absolute movement consumes its frozen variance budget.  All source
eras are already viewed development evidence; no confirmation claim is made.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import stable_hash
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.production import fresh_confirmation_lane as lane
from hydra.production.autonomous_exact_replay import _require_scenario_identity
from hydra.research import cross_era_marginal_pair_builder as pairs
from hydra.research import distributionally_robust_account_policy as dro
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
from hydra.research.pnl_state_risk_frontier import _PreparedPolicy, _evaluate_profile
from hydra.research.cross_era_bank_sieve import PROFILE_BY_ID


SCHEMA = "hydra_event_time_volatility_burst_direct_account_v1"
ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path("reports/economic_evolution/event_time_volatility_burst_direct_account_v1")
MARKETS = ("CL", "ES", "NQ")
EXECUTION = {"CL": "MCL", "ES": "MES", "NQ": "MNQ"}
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
ACCOUNT_LABELS = ("50K", "100K", "150K")

# budget, maximum completion bars, minimum directional efficiency, quality tier
CLOCKS = (
    ("Q16_FAST", 16.0, 12, 0.25, 0.75),
    ("Q16_COHERENT", 16.0, 20, 0.50, 1.50),
    ("Q32_FAST", 32.0, 24, 0.25, 0.75),
    ("Q32_COHERENT", 32.0, 40, 0.50, 1.50),
)
PAYOFFS = ((1.5, 0.75, 15), (2.0, 1.0, 30))


class EventTimeBurstError(RuntimeError):
    pass


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EventTimeBurstError(f"JSON object required: {path}")
    return value


def _write(path: Path, value: Mapping[str, Any], *, immutable: bool = False) -> None:
    text = json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n"
    if immutable and path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise EventTimeBurstError(f"refusing divergent immutable rewrite: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def freeze_candidate_contract(output_dir: Path) -> dict[str, Any]:
    cemetery_paths = (
        ROOT / "WORM/v7.1-event-time-grammar-0003-2026-07-12.json",
        ROOT / "config/v7/fast_pass_factory_0029_revision_04.json",
        ROOT / "reports/economic_evolution/distributionally_robust_account_policy_v1/economic_result.json",
    )
    if any(not path.is_file() for path in cemetery_paths):
        raise EventTimeBurstError("cemetery/source audit input absent")
    candidates: list[dict[str, Any]] = []
    for market in MARKETS:
        for clock_id, _budget, _max_bars, _efficiency, risk in CLOCKS:
            for polarity in ("CONTINUATION", "REVERSAL"):
                feature = f"event_time_{clock_id.lower()}_{polarity.lower()}"
                for favorable, adverse, holding in PAYOFFS:
                    candidate = HazardCandidate(
                        market=market,
                        execution_market=EXECUTION[market],
                        mechanism=f"VARIANCE_CLOCK_{clock_id}_{polarity}",
                        cross_asset_reference_market=None,
                        timeframe="1m",
                        session_code=-1,
                        trigger_feature=feature,
                        trigger_operator="ABS_GT",
                        trigger_quantile=0.55,
                        context_feature=None,
                        context_operator=None,
                        context_quantile=None,
                        direction_rule="TRIGGER_SIGN_CONTINUATION",
                        favorable_r=favorable,
                        adverse_r=adverse,
                        horizon=holding,
                        risk_level=risk,
                        cooldown_minutes=holding,
                    )
                    candidates.append(candidate.payload)
    if len(candidates) != 48 or len({stable_hash(row) for row in candidates}) != 48:
        raise EventTimeBurstError("frozen 48-candidate boundary drift")
    decision_core = {
        "schema": f"{SCHEMA}_decision_card",
        "status": "FROZEN_BEFORE_EVENT_OUTCOMES",
        "hypothesis": "Rapid variance-budget completion with coherent displacement can create executable target velocity, while a shared multi-asset survival router limits MLL consumption.",
        "strongest_argument_against": "Prior fixed-quantity event clocks and bar-time volatility expansion were non-robust; a variance clock may only repackage noise and costs.",
        "smallest_decisive_experiment": "48 causal sleeves then no more than 30 direct account cells over three viewed eras and three account sizes.",
        "expected_data_cost_usd": 0.0,
        "materially_distinct_next_alternative": "ROLE_CONDITIONED_SESSION_PATH_ANALOG_DIRECT_ACCOUNT",
    }
    decision = {**decision_core, "decision_hash": stable_hash(decision_core)}
    _write(output_dir / "decision_card.json", decision, immutable=True)
    core = {
        "schema": f"{SCHEMA}_candidate_contract",
        "status": "FROZEN_BEFORE_EVENT_OUTCOMES",
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "decision_hash": decision["decision_hash"],
        "cemetery_audit": {
            str(path.relative_to(ROOT)): _sha256(path) for path in cemetery_paths
        },
        "exact_duplicate_found": False,
        "distinctness": "NORMALISED_VARIANCE_QUOTA_CUSUM_RESET_CLOCK_PLUS_DIRECT_MULTI_ASSET_ACCOUNT; excludes fixed-quantity clocks and simple rv/vol-expansion thresholds",
        "clocks": [
            {
                "clock_id": row[0], "absolute_normalised_budget": row[1],
                "maximum_completion_bars": row[2], "minimum_efficiency": row[3],
                "quality_risk_tier": row[4],
            }
            for row in CLOCKS
        ],
        "candidates": candidates,
        "candidate_count": len(candidates),
        "eras": list(dro.ERAS),
        "account_labels": list(ACCOUNT_LABELS),
        "account_horizons": list(HORIZONS),
        "scenarios": list(SCENARIOS),
        "fill": "CAUSAL_NEXT_TRADABLE_OPEN_V1",
        "worker_processes": 1,
        "numeric_threads_per_worker": 1,
        "data_purchase": False,
        "q4_access": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    contract = {**core, "contract_hash": stable_hash(core)}
    _write(output_dir / "candidate_contract.json", contract, immutable=True)
    return contract


def _augment_with_variance_clocks(matrix: FeatureMatrix) -> FeatureMatrix:
    opens = np.asarray(matrix.array("bar_open"), dtype=float)
    closes = np.asarray(matrix.array("bar_close"), dtype=float)
    volatility = np.asarray(matrix.array("feature__past_volatility"), dtype=float)
    segments = np.asarray(matrix.array("segment_code"), dtype=np.int64)
    sessions = np.asarray(matrix.array("session_code"), dtype=np.int8)
    denominator = np.maximum(np.abs(opens) * np.maximum(volatility, 1e-8), 1e-12)
    z = np.divide(closes - opens, denominator)
    outputs = {
        clock_id: np.zeros(len(opens), dtype=np.float64)
        for clock_id, *_rest in CLOCKS
    }
    acc_abs = {clock_id: 0.0 for clock_id, *_rest in CLOCKS}
    acc_signed = {clock_id: 0.0 for clock_id, *_rest in CLOCKS}
    bars = {clock_id: 0 for clock_id, *_rest in CLOCKS}
    prior_segment: int | None = None
    for index in range(len(opens)):
        segment = int(segments[index])
        if prior_segment != segment or int(sessions[index]) < 0:
            for clock_id, *_rest in CLOCKS:
                acc_abs[clock_id] = 0.0
                acc_signed[clock_id] = 0.0
                bars[clock_id] = 0
            prior_segment = segment
        value = float(z[index])
        if int(sessions[index]) < 0 or not math.isfinite(value):
            continue
        for clock_id, budget, maximum_bars, minimum_efficiency, _risk in CLOCKS:
            acc_abs[clock_id] += abs(value)
            acc_signed[clock_id] += value
            bars[clock_id] += 1
            if acc_abs[clock_id] < budget:
                continue
            efficiency = abs(acc_signed[clock_id]) / max(acc_abs[clock_id], 1e-12)
            if bars[clock_id] <= maximum_bars and efficiency >= minimum_efficiency:
                outputs[clock_id][index] = math.copysign(
                    max(1.0, acc_abs[clock_id] / budget), acc_signed[clock_id]
                )
            acc_abs[clock_id] = 0.0
            acc_signed[clock_id] = 0.0
            bars[clock_id] = 0
    arrays = dict(matrix.arrays)
    for clock_id, values in outputs.items():
        values.flags.writeable = False
        arrays[f"feature__event_time_{clock_id.lower()}_continuation"] = values
        reversed_values = -values.copy()
        reversed_values.flags.writeable = False
        arrays[f"feature__event_time_{clock_id.lower()}_reversal"] = reversed_values
    manifest = dict(matrix.manifest)
    manifest["row_count"] = matrix.row_count
    manifest["bundle_hash"] = stable_hash({
        "schema": "hydra_causal_variance_clock_feature_view_v1",
        "source_matrix_hash": matrix.fingerprint,
        "clock_contract": [list(row) for row in CLOCKS],
        "causal": "completed_current_bar_only; reset after quota; no outcome fields",
    })
    return FeatureMatrix(root=matrix.root, manifest=manifest, arrays=arrays)


def _era_replays(contract: Mapping[str, Any], era: Mapping[str, Any]) -> dict[str, Any]:
    raw_matrices = dro._open_era_features(era)
    matrices = {market: _augment_with_variance_clocks(raw_matrices[market]) for market in MARKETS}
    calendar = dro._era_calendar(era, raw_matrices)
    start_ns = int(calendar[0]) * 86_400_000_000_000
    end_ns = lane._date_ns(str(era["end_exclusive"]))
    rows = []
    replay_cache: dict[str, Any] = {}
    for payload in contract["candidates"]:
        candidate = HazardCandidate(**dict(payload))
        matrix = matrices[candidate.market]
        calibrated = calibrate_candidate(
            candidate, matrix, calibration_end_exclusive_ns=start_ns,
            minimum_observations=100,
        )
        intents = discover_intents_batch(
            calibrated, matrix, evaluation_start_ns=start_ns,
            evaluation_end_exclusive_ns=end_ns,
        )
        streaming = discover_intents_streaming(
            calibrated, matrix, evaluation_start_ns=start_ns,
            evaluation_end_exclusive_ns=end_ns,
        )
        if tuple((row.row_index, row.direction) for row in intents) != streaming:
            raise EventTimeBurstError(f"batch/stream drift: {candidate.candidate_id}")
        evidence = observe_outcomes(calibrated, matrix, intents)
        days = frozen_eligible_session_calendar(
            candidate, matrix, evaluation_start_ns=start_ns,
            evaluation_end_exclusive_ns=end_ns,
        )
        replay = exact_sleeve_replay(calibrated, evidence, eligible_session_days=days)
        complete = [
            row for row in evidence
            if row.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
        ]
        instrument = instrument_spec(candidate.execution_market)
        gross = sum(
            (float(row.raw_exit_price) - float(row.raw_fill_price))
            * row.direction * instrument.point_value * row.quantity
            for row in complete
        )
        normal = sum(float(row.normal_net_pnl or 0.0) for row in complete)
        stressed = sum(float(row.stressed_net_pnl or 0.0) for row in complete)
        positive_gross_upper = sum(
            max(0.0, (float(row.raw_exit_price) - float(row.raw_fill_price))
                * row.direction * instrument.point_value * row.quantity)
            for row in complete
        )
        risk_charges = [
            float(row.risk_unit_price) * candidate.adverse_r
            * instrument.point_value / max(0.1, row.quantity / 10.0)
            for row in complete
        ]
        replay_cache[candidate.candidate_id] = {
            "replay": replay,
            "risk_charge_per_mini": (
                float(np.quantile(risk_charges, 0.90)) if risk_charges else 1e9
            ),
            "censored_days": frozenset(
                int(row.session_day) for row in evidence
                if row.outcome == HazardOutcome.CENSORED_FUTURE_COVERAGE
            ),
        }
        core = {
            "candidate_id": candidate.candidate_id,
            "candidate": candidate.payload,
            "event_count": len(evidence),
            "complete_event_count": len(complete),
            "favorable_first_count": sum(
                row.outcome == HazardOutcome.FAVORABLE_FIRST for row in complete
            ),
            "gross_pnl_usd": gross,
            "normal_net_usd": normal,
            "stressed_net_usd": stressed,
            "non_deployable_perfect_abstention_gross_upper_bound_usd": positive_gross_upper,
            "opportunities_per_20_sessions": len(complete) / max(1, len(calendar)) * 20.0,
            "risk_charge_per_mini_usd": replay_cache[candidate.candidate_id]["risk_charge_per_mini"],
            "decision_hash": replay.decision_hash,
            "normal_trajectory_hash": replay.normal_trajectory_hash,
            "stressed_trajectory_hash": replay.stressed_trajectory_hash,
        }
        rows.append({**core, "result_hash": stable_hash(core)})
    return {
        "era_id": era["era_id"], "calendar": calendar, "starts": lane.non_overlapping_starts(calendar, HORIZONS),
        "rows": rows, "replays": replay_cache,
    }


def _candidate_rank(rows: Sequence[Mapping[str, Any]]) -> tuple[Any, ...]:
    return (
        sum(float(row["stressed_net_usd"]) > 0 for row in rows),
        min(float(row["stressed_net_usd"]) for row in rows),
        min(float(row["normal_net_usd"]) for row in rows),
        min(float(row["opportunities_per_20_sessions"]) for row in rows),
        min(float(row["non_deployable_perfect_abstention_gross_upper_bound_usd"]) for row in rows),
    )


def _freeze_account_contract(output_dir: Path, candidate_contract: Mapping[str, Any], era_values: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    era_ids = [str(row["era_id"]) for row in era_values]
    by_era = {
        str(value["era_id"]): {str(row["candidate_id"]): row for row in value["rows"]}
        for value in era_values
    }
    payloads = {
        HazardCandidate(**dict(row)).candidate_id: dict(row)
        for row in candidate_contract["candidates"]
    }
    selected: dict[str, list[str]] = {}
    for market in MARKETS:
        ids = [
            candidate_id for candidate_id, payload in payloads.items()
            if str(payload["market"]) == market
        ]
        ids.sort(
            key=lambda candidate_id: _candidate_rank(
                [by_era[era][candidate_id] for era in era_ids]
            ), reverse=True,
        )
        selected[market] = ids[:2]
    memberships = (
        (selected["CL"][0], selected["ES"][0], selected["NQ"][0]),
        (selected["CL"][1], selected["ES"][1], selected["NQ"][1]),
        (selected["CL"][0], selected["ES"][0]),
        (selected["CL"][0], selected["NQ"][0]),
        (selected["ES"][0], selected["NQ"][0]),
    )
    profiles = (
        {"profile_id": "SURVIVAL", "maximum_concurrent_sleeves": 1, "open_risk_ceiling_fraction": 0.30, "daily_loss_budget_fraction": 0.35, "daily_profit_lock_fraction": 0.90, "target_protection_fraction": 0.85},
        {"profile_id": "VELOCITY", "maximum_concurrent_sleeves": 2, "open_risk_ceiling_fraction": 0.50, "daily_loss_budget_fraction": 0.50, "daily_profit_lock_fraction": 0.95, "target_protection_fraction": 0.90},
    )
    source = _read(ROOT / dro.SOURCE_CONTRACT)
    rules = dict(source["account_rules"])
    cells = []
    for members in memberships:
        for profile in profiles:
            for account_label in ACCOUNT_LABELS:
                rule = rules[account_label]
                charges = {
                    member: max(
                        float(by_era[era][member]["risk_charge_per_mini_usd"])
                        for era in era_ids
                    ) for member in members
                }
                semantic = {
                    "component_ids": list(members),
                    "component_priority": list(members),
                    "component_quantity_tiers": {member: 1 for member in members},
                    "component_nominal_risk_charges": charges,
                    "governor_profile": profile,
                    "maximum_mini_equivalent": float(rule["maximum_mini_contracts"]),
                    "account_label": account_label,
                    "construction": "CHEAP_EVENT_ECONOMICS_TOP_TWO_PER_MARKET_THEN_FROZEN_DIRECT_ACCOUNT",
                }
                cells.append({
                    **semantic,
                    "policy_id": f"evtburst_{stable_hash(semantic)[:24]}",
                    "policy_spec_hash": stable_hash(semantic),
                })
    if len(cells) != 30 or len(cells) > 64:
        raise EventTimeBurstError("direct account policy bound drift")
    core = {
        "schema": f"{SCHEMA}_account_contract",
        "status": "FROZEN_AFTER_CHEAP_SCREEN_BEFORE_ACCOUNT_OUTCOMES",
        "candidate_contract_hash": candidate_contract["contract_hash"],
        "selected_components_by_market": selected,
        "selection_rule": "TOP_TWO_PER_MARKET_BY_WORST_ERA_STRESS_THEN_NORMAL_THEN_DENSITY",
        "profiles": list(profiles), "account_rules": rules, "cells": cells,
        "cell_count": len(cells), "maximum_exact_policy_cells": 64,
    }
    value = {**core, "contract_hash": stable_hash(core)}
    _write(output_dir / "account_policy_contract.json", value, immutable=True)
    return value


def _evaluate_accounts(contract: Mapping[str, Any], era_values: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    identity = PROFILE_BY_ID["pnl_state_identity"]
    output = []
    for era in era_values:
        block = str(era["era_id"])
        starts = {
            int(horizon): tuple((int(day), block) for day, _ in rows)
            for horizon, rows in era["starts"].items()
        }
        rows = []
        for cell in contract["cells"]:
            members = [str(value) for value in cell["component_ids"]]
            trajectories = {scenario: {} for scenario in SCENARIOS}
            unavailable: set[int] = set()
            for member in members:
                source = era["replays"][member]
                replay = source["replay"]
                normal = replay.normal_trajectories
                stressed = replay.stressed_trajectories
                _require_scenario_identity(normal, stressed)
                trajectories["NORMAL"][member] = normal
                trajectories["STRESSED_1_5X"][member] = stressed
                unavailable.update(source["censored_days"])
            rule = dict(contract["account_rules"])[str(cell["account_label"])]
            prepared = _PreparedPolicy(
                policy_id=str(cell["policy_id"]), source_kind="EVENT_TIME_VOLATILITY_BURST_DIRECT_ACCOUNT",
                evidence_tier="H", account_label=str(cell["account_label"]),
                baseline_policy=pairs._active_policy(cell, rule),
                trajectories=trajectories, unavailable_days=frozenset(unavailable),
                source_policy=cell, source_metrics={}, source_hashes={},
            )
            evaluation = _evaluate_profile(
                prepared, identity, blocks=(block,), calendar=era["calendar"],
                starts=starts, rule=rule,
            )
            rows.append({
                "policy_id": cell["policy_id"], "account_label": cell["account_label"],
                "component_ids": members, "evaluation": evaluation,
            })
        output.append({"era_id": block, "rows": rows})
    return output


def _summary(row: Mapping[str, Any], scenario: str, horizon: int) -> dict[str, Any]:
    return dict(row["evaluation"]["summaries"][scenario][str(horizon)])


def _aggregate(candidate_contract: Mapping[str, Any], era_values: Sequence[Mapping[str, Any]], account_contract: Mapping[str, Any], account_values: Sequence[Mapping[str, Any]], runtime: float) -> dict[str, Any]:
    era_ids = [str(row["era_id"]) for row in account_values]
    by_era = {
        str(value["era_id"]): {str(row["policy_id"]): row for row in value["rows"]}
        for value in account_values
    }
    account_rows = []
    for cell in account_contract["cells"]:
        policy_id = str(cell["policy_id"])
        best_horizon = max(HORIZONS, key=lambda h: (
            min(_summary(by_era[e][policy_id], "STRESSED_1_5X", h)["pass_count"] for e in era_ids),
            sum(_summary(by_era[e][policy_id], "STRESSED_1_5X", h)["pass_count"] for e in era_ids),
            min(_summary(by_era[e][policy_id], "STRESSED_1_5X", h)["target_progress_p25"] for e in era_ids),
        ))
        eras = {
            era: {
                "normal": _summary(by_era[era][policy_id], "NORMAL", best_horizon),
                "stressed": _summary(by_era[era][policy_id], "STRESSED_1_5X", best_horizon),
            } for era in era_ids
        }
        positive_stress = all(float(row["stressed"]["net_total_usd"]) > 0 for row in eras.values())
        no_mll = all(int(row[scenario]["mll_breach_count"]) == 0 for row in eras.values() for scenario in ("normal", "stressed"))
        pass_eras = sum(int(row["stressed"]["pass_count"]) > 0 for row in eras.values())
        core = {
            "policy_id": policy_id, "account_label": cell["account_label"],
            "component_ids": cell["component_ids"], "selected_horizon": best_horizon,
            "era_results": eras, "positive_stressed_every_era": positive_stress,
            "zero_mll_every_era": no_mll, "stressed_pass_era_count": pass_eras,
            "status": (
                "TIER_G_EVENT_TIME_BURST_DEVELOPMENT" if positive_stress and no_mll and pass_eras >= 2
                else "EVENT_TIME_BURST_DEVELOPMENT_REJECTED"
            ),
        }
        account_rows.append({**core, "result_hash": stable_hash(core)})
    ranked = sorted(account_rows, key=lambda row: (
        row["status"] == "TIER_G_EVENT_TIME_BURST_DEVELOPMENT",
        row["stressed_pass_era_count"],
        min(float(v["stressed"]["target_progress_p25"]) for v in row["era_results"].values()),
        min(float(v["stressed"]["net_total_usd"]) for v in row["era_results"].values()),
    ), reverse=True)
    tier_g = [row for row in ranked if row["status"] == "TIER_G_EVENT_TIME_BURST_DEVELOPMENT"]
    event_rows = [row for era in era_values for row in era["rows"]]
    exact_episodes = sum(
        int(row["evaluation"]["exact_episode_count"])
        for era in account_values for row in era["rows"]
    )
    verdict = "EVENT_TIME_VOLATILITY_BURST_GREEN" if tier_g else "EVENT_TIME_VOLATILITY_BURST_FALSIFIED"
    core = {
        "schema": f"{SCHEMA}_economic_result", "status": "COMPLETE", "verdict": verdict,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY_NO_CONFIRMATION_CLAIM",
        "candidate_contract_hash": candidate_contract["contract_hash"],
        "account_contract_hash": account_contract["contract_hash"],
        "exact_sleeve_replay_count": len(event_rows), "direct_policy_cell_count": len(account_rows),
        "exact_account_episode_count": exact_episodes, "tier_g_count": len(tier_g),
        "tier_g": tier_g, "top_cells": ranked[:10],
        "event_screen": {
            "candidate_era_rows": event_rows,
            "positive_stressed_candidate_era_count": sum(float(row["stressed_net_usd"]) > 0 for row in event_rows),
            "total_complete_events": sum(int(row["complete_event_count"]) for row in event_rows),
            "gross_pnl_usd": sum(float(row["gross_pnl_usd"]) for row in event_rows),
            "stressed_net_usd": sum(float(row["stressed_net_usd"]) for row in event_rows),
        },
        "runtime_seconds": runtime, "worker_processes": 1, "numeric_threads_per_worker": 1,
        "new_data_purchase": False, "q4_access_count_delta": 0, "broker_connections": 0,
        "orders": 0, "xfa_paths_started": 0,
        "next_action": (
            "FREEZE_DISTINCT_TIER_G_FOR_FRESH_CONFIRMATION" if tier_g
            else "KILL_VARIANCE_CLOCK_BRANCH_AND_START_ROLE_CONDITIONED_SESSION_PATH_ANALOG"
        ),
    }
    hash_core = {key: value for key, value in core.items() if key != "runtime_seconds"}
    return {**core, "result_hash": stable_hash(hash_core)}


def run(output_dir: Path = OUTPUT_DIR) -> dict[str, Any]:
    os.environ.update({
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1",
    })
    started = time.perf_counter()
    contract = freeze_candidate_contract(output_dir)
    _write(output_dir / "production_state.json", {"status": "CHEAP_EVENT_ECONOMICS_ACTIVE", "pid": os.getpid(), "candidate_count": 48})
    era_values = [_era_replays(contract, era) for era in dro.ERAS]
    _write(output_dir / "production_state.json", {"status": "DIRECT_ACCOUNT_REPLAY_ACTIVE", "pid": os.getpid(), "candidate_era_replays": sum(len(row["rows"]) for row in era_values)})
    account_contract = _freeze_account_contract(output_dir, contract, era_values)
    account_values = _evaluate_accounts(account_contract, era_values)
    result = _aggregate(contract, era_values, account_contract, account_values, time.perf_counter() - started)
    _write(output_dir / "economic_result.json", result, immutable=True)
    _write(output_dir / "production_state.json", {key: result[key] for key in ("status", "verdict", "exact_sleeve_replay_count", "direct_policy_cell_count", "exact_account_episode_count", "tier_g_count", "runtime_seconds", "result_hash", "next_action")})
    return result


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    result = run(ROOT / OUTPUT_DIR)
    print(json.dumps({key: result[key] for key in ("verdict", "exact_sleeve_replay_count", "direct_policy_cell_count", "exact_account_episode_count", "tier_g_count", "runtime_seconds", "result_hash", "next_action")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
