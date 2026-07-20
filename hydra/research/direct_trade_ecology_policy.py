"""Strictly chronological deployable selector for consolidated 0029 events.

The source universe is the complete 5,000-candidate Stage-2 population.  It was
selected with B1/B2 design evidence, unlike the later 180-entry archive whose
ranking used B3/B4 outcomes.  B1 trains a small regularised model, B2 selects
one frozen action profile/account, and B3 is opened exactly once.  B4 is not
read for model fitting, selection, or reporting.
"""

from __future__ import annotations

import gzip
import json
import math
import os
import pickle
import statistics
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import _hazard_event
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _apply_session_contract,
    _declared_stop_risk_charge_per_mini,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
)
from hydra.research.causal_target_velocity import (
    HazardOutcome,
    _hazard_trajectory,
    _trade_path_event,
)
from hydra.research.direct_trade_ecology_oracle import (
    ACCOUNT_RISK_SCALE,
    HORIZON_ORDER,
    HORIZONS,
    SCENARIOS,
    _policy,
    _resize,
    consolidation_key,
    representative_rank,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
import hydra.account_policy.causal_active_pool_replay as causal_account_replay


SCHEMA = "hydra_direct_trade_ecology_deployable_policy_v1"
SOURCE_ROOT = Path("data/cache/economic_production/hydra_fast_pass_factory_0029")
DEFAULT_OUTPUT = Path(
    "reports/economic_evolution/direct_trade_ecology_account_policy_v1/"
    "deployable_policy_result.json"
)
DEFAULT_DATASET_CACHE = Path(
    "reports/economic_evolution/direct_trade_ecology_account_policy_v1/"
    "coverage_complete_source_subset_dataset.pkl"
)
FEATURE_CONCEPTS = (
    "market",
    "mechanism",
    "session",
    "timeframe",
    "favorable_adverse_ratio",
    "horizon_minutes",
    "trigger_quantile",
    "context_quantile",
)
ACTION_PROFILES = {
    "STRICT": (0.56, 0.62, 0.70),
    "BALANCED": (0.50, 0.56, 0.64),
    "BROAD": (0.44, 0.50, 0.58),
}


class DirectTradeEcologyPolicyError(RuntimeError):
    """A frozen source or chronology contract was violated."""


def action_from_probability(probability: float, thresholds: Sequence[float]) -> float:
    """Map one decision-time score onto the complete frozen action lattice."""

    low, middle, high = (float(value) for value in thresholds)
    if not 0.0 <= low < middle < high <= 1.0:
        raise ValueError("action thresholds must be strictly ordered in [0,1]")
    if probability < low:
        return 0.0
    if probability < middle:
        return 0.5
    if probability < high:
        return 1.0
    return 1.5


def safe_features(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Exactly eight preregistered fields available before an event outcome."""

    adverse = max(float(candidate["adverse_r"]), 1e-12)
    horizon = HORIZON_ORDER[str(candidate["horizon"])]
    return {
        "market": str(candidate["execution_market"]),
        "mechanism": str(candidate["mechanism"]),
        "session": str(int(candidate["session_code"])),
        "timeframe": str(candidate["timeframe"]),
        "favorable_adverse_ratio": float(candidate["favorable_r"]) / adverse,
        "horizon_minutes": math.log1p(float(horizon)),
        "trigger_quantile": float(candidate["trigger_quantile"]),
        "context_quantile": float(candidate.get("context_quantile") or 0.0),
    }


def _iter_jsonl(path: Path):
    with path.open("rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _stage2_sources(project: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = project / SOURCE_ROOT
    rows: list[dict[str, Any]] = []
    batch_files = 0
    for wave in (1, 2):
        folder = root / f"wave_{wave:02d}" / "stage2_batches"
        for path in sorted(folder.glob("batch_*.jsonl")):
            batch_files += 1
            for raw in _iter_jsonl(path):
                row = dict(raw)
                row["_source_wave"] = wave
                rows.append(row)
    if len(rows) != 5000 or len({str(row["candidate_id"]) for row in rows}) != 5000:
        raise DirectTradeEcologyPolicyError(
            f"expected 5,000 unique Stage-2 sources, got {len(rows)}"
        )
    # Coverage is an evaluation-role property frozen before this selector is
    # fitted.  It is never a model feature or an event-level veto.  A common
    # FULL_COVERAGE universe is required for identical policy/control starts.
    coverage_complete = [
        row
        for row in rows
        if str(row.get("coverage_status")) == "FULL_COVERAGE"
        and int(row.get("censored_event_count", -1)) == 0
    ]
    common_eligible_days = set(
        int(value) for value in coverage_complete[0]["eligible_session_days"]
    )
    for row in coverage_complete[1:]:
        common_eligible_days.intersection_update(
            int(value) for value in row["eligible_session_days"]
        )
    source_inventory_hash = stable_hash(
        [
            {
                "candidate_id": row["candidate_id"],
                "candidate_fingerprint": row["candidate_fingerprint"],
                "event_sha256": row["event_evidence"]["sha256"],
            }
            for row in coverage_complete
        ]
    )
    return coverage_complete, {
        "stage2_source_count": len(rows),
        "coverage_complete_source_count": len(coverage_complete),
        "coverage_limited_source_count": len(rows) - len(coverage_complete),
        "batch_file_count": batch_files,
        "selection_axis": "STAGE1_B1_B2_DESIGN_ONLY",
        "coverage_axis": (
            "GLOBAL_OUTCOME_AVAILABILITY_SOURCE_COHORT;"
            "NOT_A_DECISION_FEATURE;DIAGNOSTIC_ONLY"
        ),
        "common_eligible_session_days": sorted(common_eligible_days),
        "common_eligible_session_count": len(common_eligible_days),
        "source_inventory_hash": source_inventory_hash,
        "evidence_scope": "COVERAGE_COMPLETE_SOURCE_COHORT_DIAGNOSTIC",
        "coverage_complete_source_subset_used": True,
        "full_5000_common_outcome_coverage_status": (
            "ZERO_B2_B3_STARTS_AFTER_3168_CENSORED_OPPORTUNITIES_ON_297_OF_309_DAYS"
        ),
        "later_180_entry_bank_used": False,
    }


def _evidence_path(project: Path, source: Mapping[str, Any]) -> Path:
    receipt = dict(source["event_evidence"])
    path = project / SOURCE_ROOT / str(receipt["relative_path"])
    if not path.is_file():
        raise DirectTradeEcologyPolicyError(f"missing event evidence: {path}")
    return path


def _read_events(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _block_days(
    calendar: Sequence[int], starts: Mapping[int, Sequence[tuple[int, str]]]
) -> dict[str, set[int]]:
    result = {value: set() for value in ("B1", "B2", "B3", "B4")}
    for start, block in starts[5]:
        index = calendar.index(int(start))
        result[str(block)].update(int(day) for day in calendar[index : index + 5])
    if any(len(result[value]) != 65 for value in result):
        raise DirectTradeEcologyPolicyError("frozen block coverage is not 65 sessions")
    return result


def _build_opportunities(
    project: Path, sources: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, float], dict[str, Any]]:
    """Two-pass consolidation keeps the exact winning paths memory-bounded."""

    source_by_id = {str(row["candidate_id"]): dict(row) for row in sources}
    ranks = {
        candidate_id: representative_rank(row["candidate"], candidate_id)
        for candidate_id, row in source_by_id.items()
    }
    winners: dict[tuple[str, int, int], str] = {}
    raw_count = 0
    causal_defects = 0
    for index, (candidate_id, source) in enumerate(source_by_id.items(), start=1):
        for event in _read_events(_evidence_path(project, source)):
            raw_count += 1
            if not (
                int(event["event_time_ns"]) <= int(event["decision_time_ns"])
                and int(event["available_at_ns"]) <= int(event["decision_time_ns"])
                and int(event["order_submit_time_ns"]) >= int(event["decision_time_ns"])
                and int(event["earliest_executable_time_ns"])
                >= int(event["decision_time_ns"])
            ):
                causal_defects += 1
            key = consolidation_key(event)
            prior = winners.get(key)
            if prior is None or ranks[candidate_id] > ranks[prior]:
                winners[key] = candidate_id
        if index % 250 == 0:
            print(
                f"DATASET_PASS1 sources={index}/{len(source_by_id)} raw={raw_count} "
                f"opportunities={len(winners)}",
                flush=True,
            )
    if causal_defects:
        raise DirectTradeEcologyPolicyError(
            f"reachable Stage-2 event causality defects: {causal_defects}"
        )

    selected_ids = set(winners.values())
    opportunities: list[dict[str, Any]] = []
    risk_charges: dict[str, float] = {}
    for index, candidate_id in enumerate(sorted(selected_ids), start=1):
        source = source_by_id[candidate_id]
        raw_events = list(_read_events(_evidence_path(project, source)))
        risk_charges[candidate_id] = float(
            _declared_stop_risk_charge_per_mini(raw_events, source["candidate"])
        )
        feature_row = safe_features(source["candidate"])
        for raw in raw_events:
            key = consolidation_key(raw)
            if winners.get(key) != candidate_id:
                continue
            event = _hazard_event(raw)
            completed = event.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
            normal = stressed = None
            if completed:
                normal_event = _trade_path_event(event, scenario="NORMAL")
                stress_event = _trade_path_event(event, scenario="STRESSED_1_5X")
                normal = _hazard_trajectory(event, normal_event, scenario="NORMAL")
                stressed = _hazard_trajectory(
                    event, stress_event, scenario="STRESSED_1_5X"
                )
            opportunities.append(
                {
                    "opportunity_id": stable_hash(
                        {"market": key[0], "side": key[1], "decision_ns": key[2]}
                    ),
                    "candidate_id": candidate_id,
                    "session_day": int(event.session_day),
                    "features": feature_row,
                    "completed": completed,
                    "stressed_net_pnl": (
                        None if not completed else float(event.stressed_net_pnl)
                    ),
                    "normal": normal,
                    "stressed": stressed,
                }
            )
        if index % 100 == 0:
            print(
                f"DATASET_PASS2 representatives={index}/{len(selected_ids)} "
                f"opportunities={len(opportunities)}",
                flush=True,
            )
    opportunities.sort(key=lambda row: (row["session_day"], row["opportunity_id"]))
    return opportunities, risk_charges, {
        "raw_event_count": raw_count,
        "consolidated_opportunity_count": len(opportunities),
        "completed_opportunity_count": sum(row["completed"] for row in opportunities),
        "censored_opportunity_count": sum(not row["completed"] for row in opportunities),
        "censored_session_days": sorted(
            {int(row["session_day"]) for row in opportunities if not row["completed"]}
        ),
        "representative_candidate_count": len(selected_ids),
        "causality_defect_count": causal_defects,
        "representative_selection_uses_outcome": False,
        "feature_concepts": list(FEATURE_CONCEPTS),
        "feature_concept_count": len(FEATURE_CONCEPTS),
    }


def _fit_model(rows: Sequence[Mapping[str, Any]], *, c_value: float):
    vectorizer = DictVectorizer(sparse=True)
    x = vectorizer.fit_transform([dict(row["features"]) for row in rows])
    # scipy may expose int64 sparse indices on this host while liblinear only
    # accepts int32.  The feature lattice is tiny, so make the representation
    # contract explicit without changing a value.
    x.indices = x.indices.astype(np.int32, copy=False)
    x.indptr = x.indptr.astype(np.int32, copy=False)
    y = np.asarray([float(row["stressed_net_pnl"]) > 0.0 for row in rows], dtype=int)
    model = LogisticRegression(
        C=float(c_value),
        class_weight="balanced",
        max_iter=500,
        random_state=0,
        solver="liblinear",
    )
    model.fit(x, y)
    return vectorizer, model


def _predict(vectorizer: Any, model: Any, rows: Sequence[Mapping[str, Any]]):
    if not rows:
        return np.asarray([], dtype=float)
    x = vectorizer.transform([dict(row["features"]) for row in rows])
    x.indices = x.indices.astype(np.int32, copy=False)
    x.indptr = x.indptr.astype(np.int32, copy=False)
    return model.predict_proba(x)[:, 1]


def _inner_select_c(rows: Sequence[Mapping[str, Any]]) -> tuple[float, dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (row["session_day"], row["opportunity_id"]))
    cut = int(len(ordered) * 0.67)
    training, audit = ordered[:cut], ordered[cut:]
    scores: dict[str, float] = {}
    for c_value in (0.1, 1.0, 10.0):
        vectorizer, model = _fit_model(training, c_value=c_value)
        probabilities = _predict(vectorizer, model, audit)
        labels = [float(row["stressed_net_pnl"]) > 0.0 for row in audit]
        scores[str(c_value)] = float(log_loss(labels, probabilities, labels=[0, 1]))
    selected = min((0.1, 1.0, 10.0), key=lambda value: (scores[str(value)], value))
    return selected, {
        "method": "B1_EXPANDING_CHRONOLOGICAL_67_33",
        "training_count": len(training),
        "audit_count": len(audit),
        "log_loss_by_c": scores,
        "selected_c": selected,
    }


def _actions(probabilities: Sequence[float], thresholds: Sequence[float]) -> list[float]:
    return [action_from_probability(float(value), thresholds) for value in probabilities]


def _cheap_profile_score(
    rows: Sequence[Mapping[str, Any]], actions: Sequence[float]
) -> tuple[float, float, float, float]:
    completed = [
        (row, action)
        for row, action in zip(rows, actions, strict=True)
        if row["completed"]
    ]
    pnl = [float(row["stressed_net_pnl"]) * float(action) for row, action in completed]
    active = sum(action > 0.0 for _, action in completed)
    action_mix = Counter(float(action) for _, action in completed)
    return (
        float(sum(pnl)),
        float(np.quantile(pnl, 0.25)) if pnl else 0.0,
        active / max(1, len(completed)),
        float(action_mix.get(1.5, 0)),
    )


def _scenario_trajectories(
    opportunities: Sequence[Mapping[str, Any]],
    actions: Sequence[float],
    *,
    scenario: str,
    account_label: str,
) -> tuple[dict[str, tuple[Any, ...]], set[int], dict[str, int]]:
    grouped: dict[str, list[Any]] = {}
    censored_days: set[int] = set()
    mix: Counter[str] = Counter()
    for row, action in zip(opportunities, actions, strict=True):
        mix[str(action)] += 1
        if action <= 0.0:
            continue
        if not row["completed"]:
            censored_days.add(int(row["session_day"]))
            continue
        trajectory = row["normal"] if scenario == "NORMAL" else row["stressed"]
        grouped.setdefault(str(row["candidate_id"]), []).append(
            _resize(
                trajectory,
                account_scale=ACCOUNT_RISK_SCALE[account_label],
                action=float(action),
            )
        )
    checked: dict[str, tuple[Any, ...]] = {}
    normal_violations = 0
    for candidate_id, values in grouped.items():
        rows, violations = _apply_session_contract(tuple(values))
        checked[candidate_id] = tuple(rows)
        normal_violations += int(violations)
    return checked, censored_days, dict(mix)


def _episode_summary(episodes: Sequence[Any], *, requested: int, censored: int):
    if not episodes:
        return {
            "requested": requested,
            "full_coverage": 0,
            "data_censored": censored,
            "passes": 0,
            "pass_rate": 0.0,
            "mll_breaches": 0,
            "consistency_compliance_rate": None,
            "stressed_or_normal_net_total_usd": 0.0,
            "target_progress_p25": None,
            "target_progress_median": None,
            "minimum_mll_buffer_usd": None,
            "median_days_to_pass": None,
        }
    progress = [float(row.target_progress) for row in episodes]
    days = [int(row.days_to_target) for row in episodes if row.days_to_target is not None]
    return {
        "requested": requested,
        "full_coverage": len(episodes),
        "data_censored": censored,
        "passes": sum(bool(row.passed) for row in episodes),
        "pass_rate": sum(bool(row.passed) for row in episodes) / len(episodes),
        "mll_breaches": sum(bool(row.mll_breached) for row in episodes),
        "consistency_compliance_rate": sum(bool(row.consistency_ok) for row in episodes)
        / len(episodes),
        "stressed_or_normal_net_total_usd": sum(float(row.net_pnl) for row in episodes),
        "target_progress_p25": float(np.quantile(progress, 0.25)),
        "target_progress_median": statistics.median(progress),
        "minimum_mll_buffer_usd": min(float(row.minimum_mll_buffer) for row in episodes),
        "median_days_to_pass": None if not days else statistics.median(days),
    }


@contextmanager
def _constant_time_priority(policy: Any):
    """Exact semantic equivalent of tuple.index, isolated to this process."""

    original = causal_account_replay._priority
    lookup = {value: index for index, value in enumerate(policy.component_priority)}

    def priority(active_policy: Any, component_id: str) -> int:
        if active_policy is policy:
            return lookup[component_id]
        return original(active_policy, component_id)

    causal_account_replay._priority = priority
    try:
        yield
    finally:
        causal_account_replay._priority = original


def _exact_matrix(
    *,
    opportunities: Sequence[Mapping[str, Any]],
    actions: Sequence[float],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    rules: Mapping[str, Any],
    risk_charges: Mapping[str, float],
    common_eligible_days: set[int],
    common_outcome_censored_days: set[int],
    blocks: Sequence[str],
    policy_name: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    matrix: dict[str, Any] = {}
    receipts: list[dict[str, Any]] = []
    active_components = sorted(
        {
            str(row["candidate_id"])
            for row, action in zip(opportunities, actions, strict=True)
            if action > 0.0
        }
    )
    for account_label in ("50K", "100K", "150K"):
        config = _account_config(rules[account_label])
        policy = _policy(
            f"deployable:{policy_name}:{account_label}",
            rules[account_label],
            active_components,
            risk_charges,
        )
        matrix[account_label] = {}
        with _constant_time_priority(policy):
            for scenario in SCENARIOS:
                trajectories, censored_days, action_mix = _scenario_trajectories(
                    opportunities,
                    actions,
                    scenario=scenario,
                    account_label=account_label,
                )
                matrix[account_label][scenario] = {}
                for horizon in HORIZONS:
                    for block in blocks:
                        frozen = [
                            int(day)
                            for day, value in starts[horizon]
                            if str(value) == str(block)
                        ]
                        episodes = []
                        censored = 0
                        for start_day in frozen:
                            index = calendar.index(start_day)
                            window = set(calendar[index : index + horizon])
                            if (
                                not window.issubset(common_eligible_days)
                                or window & common_outcome_censored_days
                                or window & censored_days
                            ):
                                censored += 1
                                continue
                            episode = run_causal_shared_account_episode(
                                trajectories,
                                calendar,
                                policy=policy,
                                start_day=start_day,
                                maximum_duration_days=horizon,
                                config=config,
                            )
                            episodes.append(episode)
                            receipts.append(
                                {
                                "policy": policy_name,
                                "account": account_label,
                                "scenario": scenario,
                                "horizon": horizon,
                                "block": block,
                                "start_day": start_day,
                                "passed": bool(episode.passed),
                                "mll_breached": bool(episode.mll_breached),
                                "consistency_ok": bool(episode.consistency_ok),
                                "net_pnl_usd": float(episode.net_pnl),
                                "target_progress": float(episode.target_progress),
                                "minimum_mll_buffer_usd": float(
                                    episode.minimum_mll_buffer
                                ),
                                "days_to_target": episode.days_to_target,
                                "path_hash": stable_hash(
                                    episode.to_dict(include_paths=True)
                                ),
                                }
                            )
                        matrix[account_label][scenario][f"{block}_P{horizon}"] = {
                            **_episode_summary(
                                episodes, requested=len(frozen), censored=censored
                            ),
                            "action_mix": action_mix,
                        }
    return matrix, receipts


def _count_passes(matrix: Mapping[str, Any], *, block: str) -> tuple[int, int]:
    normal = stressed = 0
    for account in matrix.values():
        for key, value in account["NORMAL"].items():
            if key.startswith(f"{block}_"):
                normal += int(value["passes"])
        for key, value in account["STRESSED_1_5X"].items():
            if key.startswith(f"{block}_"):
                stressed += int(value["passes"])
    return normal, stressed


def _count_full_coverage(matrix: Mapping[str, Any], *, block: str) -> int:
    return sum(
        int(value["full_coverage"])
        for account in matrix.values()
        for scenario in SCENARIOS
        for key, value in account[scenario].items()
        if key.startswith(f"{block}_")
    )


def _common_coverage_grid(
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    *,
    eligible_days: set[int],
    censored_days: set[int],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for block in ("B2", "B3"):
        result[block] = {}
        for horizon in HORIZONS:
            accepted: list[int] = []
            rejected_input: list[int] = []
            rejected_outcome: list[int] = []
            for start, value in starts[horizon]:
                if str(value) != block:
                    continue
                index = calendar.index(int(start))
                window = set(calendar[index : index + horizon])
                if not window.issubset(eligible_days):
                    rejected_input.append(int(start))
                elif window & censored_days:
                    rejected_outcome.append(int(start))
                else:
                    accepted.append(int(start))
            result[block][f"P{horizon}"] = {
                "common_full_coverage_starts": accepted,
                "common_full_coverage_count": len(accepted),
                "input_coverage_rejected_starts": rejected_input,
                "outcome_coverage_rejected_starts": rejected_outcome,
            }
    return result


def run_policy(project: Path) -> dict[str, Any]:
    started = time.perf_counter()
    sources, source_audit = _stage2_sources(project)
    manifest = _load_self_hashed_manifest(project / DEFAULT_FAST_PASS_MANIFEST)
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(project / DEFAULT_RULE_SNAPSHOT)
    block_days = _block_days(calendar, starts)
    cache_path = project / DEFAULT_DATASET_CACHE
    cache_key = stable_hash(
        {
            "schema": SCHEMA,
            "source_inventory_hash": source_audit["source_inventory_hash"],
            "representative_contract": "STATIC_DECISION_TIME_RANK_V1",
        }
    )
    cached = None
    if cache_path.is_file():
        with cache_path.open("rb") as handle:
            candidate_cache = pickle.load(handle)
        if candidate_cache.get("cache_key") == cache_key:
            cached = candidate_cache
    if cached is None:
        opportunities, risk_charges, opportunity_audit = _build_opportunities(
            project, sources
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_cache = cache_path.with_suffix(".tmp")
        with temporary_cache.open("wb") as handle:
            pickle.dump(
                {
                    "cache_key": cache_key,
                    "opportunities": opportunities,
                    "risk_charges": risk_charges,
                    "opportunity_audit": opportunity_audit,
                },
                handle,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        temporary_cache.replace(cache_path)
    else:
        opportunities = cached["opportunities"]
        risk_charges = cached["risk_charges"]
        opportunity_audit = {
            **cached["opportunity_audit"],
            "loaded_from_local_deterministic_cache": True,
        }
    common_eligible_days = set(source_audit["common_eligible_session_days"])
    common_outcome_censored_days = set(opportunity_audit["censored_session_days"])
    common_grid = _common_coverage_grid(
        calendar,
        starts,
        eligible_days=common_eligible_days,
        censored_days=common_outcome_censored_days,
    )
    by_block = {
        block: [
            row
            for row in opportunities
            if row["completed"]
            and row["session_day"] in block_days[block]
            and row["session_day"] in common_eligible_days
        ]
        for block in ("B1", "B2", "B3")
    }
    selected_c, inner_audit = _inner_select_c(by_block["B1"])
    vectorizer, model = _fit_model(by_block["B1"], c_value=selected_c)
    b2_all = [
        row
        for row in opportunities
        if row["session_day"] in block_days["B2"]
        and row["session_day"] in common_eligible_days
    ]
    b2_probabilities = _predict(vectorizer, model, b2_all)
    profile_scores: dict[str, Any] = {}
    for name, thresholds in ACTION_PROFILES.items():
        values = _actions(b2_probabilities, thresholds)
        score = _cheap_profile_score(b2_all, values)
        profile_scores[name] = {
            "thresholds": list(thresholds),
            "stressed_net_proxy_usd": score[0],
            "lower_quartile_event_contribution_usd": score[1],
            "trade_coverage": score[2],
            "high_risk_action_count": int(score[3]),
            "action_mix": dict(Counter(str(value) for value in values)),
        }
    selected_profile = max(
        ACTION_PROFILES,
        key=lambda name: (
            profile_scores[name]["stressed_net_proxy_usd"],
            profile_scores[name]["lower_quartile_event_contribution_usd"],
            profile_scores[name]["trade_coverage"],
            name,
        ),
    )
    all_probabilities = _predict(vectorizer, model, opportunities)
    deployable_actions = _actions(
        all_probabilities, ACTION_PROFILES[selected_profile]
    )
    all_trade_actions = [1.0] * len(opportunities)
    oracle_actions = [
        1.5
        if row["completed"] and float(row["stressed_net_pnl"]) > 0.0
        else 0.0
        for row in opportunities
    ]

    comparisons: dict[str, Any] = {}
    all_receipts: list[dict[str, Any]] = []
    for name, actions in (
        ("ALL_TRADE_1X", all_trade_actions),
        ("DEPLOYABLE_SELECTOR", deployable_actions),
        ("NON_DEPLOYABLE_ORACLE", oracle_actions),
    ):
        matrix, receipts = _exact_matrix(
            opportunities=opportunities,
            actions=actions,
            calendar=calendar,
            starts=starts,
            rules=rules,
            risk_charges=risk_charges,
            common_eligible_days=common_eligible_days,
            common_outcome_censored_days=common_outcome_censored_days,
            blocks=("B2", "B3"),
            policy_name=name,
        )
        comparisons[name] = matrix
        all_receipts.extend(receipts)
        print(
            f"EXACT_POLICY_DONE policy={name} episodes={len(receipts)}",
            flush=True,
        )

    b3_normal, b3_stressed = _count_passes(
        comparisons["DEPLOYABLE_SELECTOR"], block="B3"
    )
    b3_full = _count_full_coverage(
        comparisons["DEPLOYABLE_SELECTOR"], block="B3"
    )
    model_payload = {
        "class": "RIDGE_LOGISTIC_REGRESSION",
        "c": selected_c,
        "feature_concepts": list(FEATURE_CONCEPTS),
        "expanded_feature_names": list(vectorizer.get_feature_names_out()),
        "coefficients": [float(value) for value in model.coef_[0]],
        "intercept": float(model.intercept_[0]),
        "action_profile": selected_profile,
        "action_thresholds": list(ACTION_PROFILES[selected_profile]),
        "training_role": "B1_ONLY",
        "selection_role": "B2_ONLY",
        "final_development_role": "B3_OPENED_ONCE",
        "B4_outcomes_accessed": False,
    }
    core = {
        "schema": SCHEMA,
        "status": (
            "COVERAGE_COHORT_LIMITED_NO_COMMON_B3_ACCOUNT_START"
            if b3_full == 0
            else (
                "COVERAGE_COHORT_COMBINE_PASS_OBSERVED_DIAGNOSTIC"
                if b3_normal > 0
                else "COVERAGE_COHORT_SELECTOR_NO_B3_COMBINE_PASS"
            )
        ),
        "source": source_audit,
        "opportunity_audit": opportunity_audit,
        "chronology": {
            "B1": "TRAIN_AND_INNER_MODEL_SELECTION",
            "B2": "ACTION_PROFILE_VALIDATION_SELECTION",
            "B3": "FINAL_DEVELOPMENT_OPENED_ONCE",
            "B4": "UNTOUCHED_BY_THIS_PILOT",
            "inner_selection": inner_audit,
        },
        "model": model_payload,
        "B2_profile_selection": profile_scores,
        "common_coverage_grid_frozen_before_policy_scores": common_grid,
        "exact_comparisons": comparisons,
        "counts": {
            "B3_deployable_normal_pass_observations": b3_normal,
            "B3_deployable_stressed_pass_observations_advisory": b3_stressed,
            "B3_deployable_full_coverage_episode_cells": b3_full,
            "exact_episode_receipt_count": len(all_receipts),
        },
        "episode_receipts": all_receipts,
        "grid_hash": grid_receipt["grid_hash"],
        "official_rule_snapshot_hash": rule_receipt["parsed_rule_hash"],
        "promotion_status": None,
        "evidence_tier_change": False,
        "generalization_to_full_5000_universe_claimed": False,
        "coverage_cohort_bias_warning": (
            "GLOBAL_OUTCOME_AVAILABILITY_FILTER_IS_HORIZON_SESSION_AND_MECHANISM_BIASED"
        ),
        "nondeployable_oracle_is_not_promotion_evidence": True,
        "diagnostic_xfa_required": False,
        "diagnostic_xfa_deferred_reason": (
            "COVERAGE_COHORT_PASS_CANNOT_TRIGGER_GENERAL_POLICY_LIFECYCLE"
            if b3_normal > 0
            else "NO_B3_PASS"
        ),
        "B4_access_count": 0,
        "data_purchase_count": 0,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "wall_clock_seconds": time.perf_counter() - started,
    }
    return {**core, "result_hash": stable_hash(core)}


def write_policy(project: Path, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    target = output if output.is_absolute() else project / output
    target.parent.mkdir(parents=True, exist_ok=True)
    result = run_policy(project.resolve())
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    temporary.replace(target)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    project = Path.cwd().resolve()
    print(
        f"DIRECT_TRADE_ECOLOGY_DEPLOYABLE_STARTED pid={os.getpid()} "
        "sources=5000 train=B1 select=B2 final=B3 B4=UNTOUCHED",
        flush=True,
    )
    result = write_policy(project)
    print(
        json.dumps(
            {
                "status": result["status"],
                "counts": result["counts"],
                "result_hash": result["result_hash"],
                "output": str(project / DEFAULT_OUTPUT),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
