from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.factory.mutation_hypothesis import (
    AccountObjectivePool,
    MutationClass,
    MutationHypothesis,
    assign_objective_pool,
    choose_mutation_class,
    classify_role,
)
from hydra.research.equity_open_gap_reversal import _account_replay
from hydra.utils.config import project_path


DEVELOPMENT_END_EXCLUSIVE = pd.Timestamp("2024-10-01", tz="UTC")
PRIMARY_SUFFIX = "prior_equity_guard_v3"


class MutationIntegrityError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _resolve_source(path_value: str, source_root: Path | None) -> Path:
    relative = Path(path_value)
    candidates = []
    if source_root is not None:
        candidates.append(Path(source_root) / relative)
    candidates.extend([project_path(*relative.parts), Path("/root/hydra-bot") / relative])
    for path in candidates:
        if path.is_file():
            return path
    raise MutationIntegrityError(f"Frozen mutation source missing: {relative}")


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise MutationIntegrityError(f"Invalid JSONL {path}:{line_number}") from exc
            if not isinstance(value, dict):
                raise MutationIntegrityError(f"Non-object JSONL {path}:{line_number}")
            rows.append(value)
    return rows


def _candidate_record(result: dict[str, Any], candidate_id: str) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in result.get("candidates") or []
        if str(row.get("candidate_id") or "") == candidate_id
    ]
    if len(matches) != 1:
        raise MutationIntegrityError(
            f"Expected one frozen parent record for {candidate_id}; found {len(matches)}"
        )
    return matches[0]


def _timestamp_value(row: dict[str, Any]) -> Any:
    return row.get("entry_timestamp") or row.get("decision_timestamp") or row.get("timestamp")


def _normalize_rows(
    rows: list[dict[str, Any]], candidate: dict[str, Any]
) -> pd.DataFrame:
    candidate_id = str(candidate["candidate_id"])
    candidate_tagged = [
        row for row in rows if str(row.get("candidate_id") or "") == candidate_id
    ]
    available = candidate_tagged if candidate_tagged else [
        row for row in rows if not row.get("candidate_id")
    ]
    execution = str(candidate.get("execution_market") or "")
    primary = str(candidate.get("primary_market") or "")
    for symbol in (execution, primary):
        selected = [row for row in available if str(row.get("symbol") or "") == symbol]
        if selected:
            available = selected
            break
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(available):
        timestamp = pd.to_datetime(_timestamp_value(row), utc=True, errors="coerce")
        if pd.isna(timestamp):
            continue
        net = row.get("net_pnl")
        if net is None:
            net = row.get("net_pnl_60")
        gross = row.get("gross_pnl")
        if gross is None:
            gross = row.get("gross_pnl_60")
        if net is None:
            continue
        cost = float(row.get("cost") or 0.0)
        if gross is None:
            gross = float(net) + cost
        session = str(
            row.get("event_session_id")
            or row.get("trading_session_id")
            or timestamp.date().isoformat()
        )
        normalized.append(
            {
                "source_index": index,
                "timestamp": timestamp,
                "event_session_id": session,
                "symbol": str(row.get("symbol") or execution or primary),
                "active_contract": str(row.get("active_contract") or ""),
                "net_pnl": float(net),
                "gross_pnl": float(gross),
                "cost": cost,
                "mae_dollars": float(row.get("mae_dollars") or min(float(net), 0.0)),
                "gap_points": row.get("gap_points"),
                "threshold_q75": row.get("threshold_q75"),
                "raw": row,
            }
        )
    frame = pd.DataFrame(normalized)
    if frame.empty:
        raise MutationIntegrityError(f"No executable frozen trades for {candidate_id}")
    frame = frame.sort_values(["timestamp", "source_index"], kind="stable").reset_index(drop=True)
    if bool((frame["timestamp"] >= DEVELOPMENT_END_EXCLUSIVE).any()):
        raise MutationIntegrityError(f"Protected Q4-or-later row encountered for {candidate_id}")
    return frame


def _failure_analysis(candidate: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    topstep = dict(candidate.get("topstep") or {})
    combine = dict(topstep.get("ten_micro_combine") or {})
    contract = dict(candidate.get("contract_transfer") or {})
    if bool(combine.get("mll_breached")):
        failures.append("shared_or_scaled_MLL_breach")
    if not bool(combine.get("passed")):
        failures.append("combine_target_path_not_passed")
    if contract and not bool(contract.get("passed", True)):
        failures.append("contract_or_micro_transfer_failed")
    if bool(candidate.get("event_dominated")) or bool(
        (candidate.get("concentration") or {}).get("event_dominated")
    ):
        failures.append("event_concentration")
    status = str(candidate.get("status") or "")
    if status != "PAPER_SHADOW_READY":
        failures.append("candidate_level_evidence_incomplete")
    return failures or ["temporal_transfer_uncertainty"]


def _make_primary_hypothesis(candidate: dict[str, Any]) -> MutationHypothesis:
    parent_id = str(candidate["candidate_id"])
    role = classify_role(candidate)
    pool = assign_objective_pool(candidate, role)
    mutation_class = choose_mutation_class(candidate, role)
    child_id = f"{parent_id}__{PRIMARY_SUFFIX}"
    failures = _failure_analysis(candidate)
    hypothesis_id = f"hyp_{_canonical_hash([parent_id, mutation_class, PRIMARY_SUFFIX])[:20]}"
    return MutationHypothesis(
        hypothesis_id=hypothesis_id,
        parent_candidate_id=parent_id,
        child_candidate_id=child_id,
        mutation_class=mutation_class.value,
        strategy_role=role.value,
        objective_pool=pool.value,
        exact_change=(
            "After a frozen warm-up, admit an event only when the sum of the prior "
            "12 completed parent trades is above the frozen first-segment 25th percentile."
        ),
        intended_failure_to_repair=";".join(failures),
        predicted_effect="Reduce adverse clusters and account-path risk while retaining >=50% of opportunities.",
        training_policy="Earliest min(24,max(6,floor(n/3))) events; no outcome from the current event.",
        minimum_retained_fraction=0.50,
    )


def _apply_prior_equity_guard(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    count = len(frame)
    training_count = min(24, max(6, count // 3))
    prior_sum = frame["net_pnl"].shift(1).rolling(12, min_periods=6).sum()
    training_values = prior_sum.iloc[:training_count].dropna()
    threshold = float(training_values.quantile(0.25)) if len(training_values) else 0.0
    warmup = np.arange(count) < training_count
    activation = warmup | prior_sum.ge(threshold).fillna(False).to_numpy()
    audited = frame.copy()
    audited["prior_completed_trade_count"] = np.arange(count)
    audited["prior_12_trade_net"] = prior_sum
    audited["guard_threshold"] = threshold
    audited["guard_active"] = activation
    retained = audited.loc[audited["guard_active"]].copy()
    return retained, {
        "training_count": int(training_count),
        "trailing_window": 12,
        "minimum_prior_observations": 6,
        "frozen_threshold": threshold,
        "current_event_outcome_used": False,
        "activation_shift_periods": 1,
    }


def _behavior_fingerprint(frame: pd.DataFrame) -> str:
    behavior = [
        [row.timestamp.isoformat(), str(row.event_session_id), str(row.symbol)]
        for row in frame.itertuples(index=False)
    ]
    return _canonical_hash(behavior)


def _fold_name(timestamp: pd.Timestamp) -> str:
    if timestamp.year == 2023:
        return "2023_AVAILABLE"
    if timestamp.year == 2024 and timestamp.quarter <= 3:
        return f"2024_Q{timestamp.quarter}"
    return "OUTSIDE_ALLOWED_DEVELOPMENT"


def _candidate_null(frame: pd.DataFrame, *, seed: int) -> float:
    if frame.empty:
        return 1.0
    gross = frame["gross_pnl"].to_numpy(dtype=float)
    costs = frame["cost"].to_numpy(dtype=float)
    block = np.arange(len(frame)) // 5
    block_count = int(block.max()) + 1
    rng = np.random.default_rng(seed)
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(4096, block_count))
    null_net = (signs[:, block] * gross).sum(axis=1) - costs.sum()
    observed = float(gross.sum() - costs.sum())
    return float((1 + np.count_nonzero(null_net >= observed)) / 4097.0)


def _bh_adjust(values: list[float]) -> list[float]:
    if not values:
        return []
    order = np.argsort(np.asarray(values, dtype=float))
    adjusted = np.empty(len(values), dtype=float)
    running = 1.0
    for reverse_rank in range(len(values) - 1, -1, -1):
        position = int(order[reverse_rank])
        rank = reverse_rank + 1
        running = min(running, float(values[position]) * len(values) / rank)
        adjusted[position] = min(running, 1.0)
    return adjusted.tolist()


def _metrics(frame: pd.DataFrame, parent: pd.DataFrame, *, seed: int) -> dict[str, Any]:
    fold_rows: dict[str, dict[str, Any]] = {}
    working = frame.assign(fold=frame["timestamp"].map(_fold_name))
    for fold, group in working.groupby("fold", sort=True):
        fold_rows[str(fold)] = {
            "events": int(len(group)),
            "net_pnl": float(group["net_pnl"].sum()),
            "double_cost_net_pnl": float((group["net_pnl"] - group["cost"]).sum()),
        }
    day_net = frame.groupby("event_session_id", sort=True)["net_pnl"].sum()
    month_net = frame.assign(month=frame["timestamp"].dt.strftime("%Y-%m")).groupby("month")["net_pnl"].sum()
    net = float(frame["net_pnl"].sum())
    concentration_base = float(frame.loc[frame["net_pnl"] > 0, "net_pnl"].sum())
    best_trade = float(frame["net_pnl"].max()) if len(frame) else 0.0
    best_day = float(day_net.max()) if len(day_net) else 0.0
    best_month = float(month_net.max()) if len(month_net) else 0.0
    topstep_input = frame.rename(columns={"net_pnl": "net_pnl_60"})
    topstep = _account_replay(topstep_input)
    available_years = sorted({int(value) for value in frame["timestamp"].dt.year.unique()})
    return {
        "event_count": int(len(frame)),
        "parent_event_count": int(len(parent)),
        "retained_fraction": float(len(frame) / len(parent)),
        "net_pnl": net,
        "double_cost_net_pnl": float((frame["net_pnl"] - frame["cost"]).sum()),
        "folds": fold_rows,
        "available_years": available_years,
        "full_2023_replay_available": 2023 in available_years,
        "candidate_null_raw_p": _candidate_null(frame, seed=seed),
        "best_trade_removed_net": net - best_trade,
        "best_day_removed_net": net - best_day,
        "best_month_removed_net": net - best_month,
        "best_trade_share_of_positive": (
            best_trade / concentration_base if concentration_base > 0 else 1.0
        ),
        "topstep": topstep,
    }


def _evaluate_primary(
    candidate: dict[str, Any], frame: pd.DataFrame
) -> tuple[dict[str, Any], pd.DataFrame]:
    hypothesis = _make_primary_hypothesis(candidate)
    retained, guard = _apply_prior_equity_guard(frame)
    parent_fingerprint = _behavior_fingerprint(frame)
    child_fingerprint = _behavior_fingerprint(retained)
    duplicate = parent_fingerprint == child_fingerprint
    retained_fraction = len(retained) / len(frame)
    minimum_met = retained_fraction >= hypothesis.minimum_retained_fraction
    seed = int(hypothesis.hypothesis_hash[:16], 16) % (2**32)
    metrics = _metrics(retained, frame, seed=seed) if len(retained) else {
        "event_count": 0,
        "parent_event_count": int(len(frame)),
        "retained_fraction": 0.0,
        "candidate_null_raw_p": 1.0,
        "full_2023_replay_available": False,
        "folds": {},
    }
    if duplicate:
        disposition = "REJECTED_BEHAVIORAL_DUPLICATE"
    elif not minimum_met:
        disposition = "REJECTED_MINIMUM_OPPORTUNITY_RETENTION"
    elif not bool(metrics.get("full_2023_replay_available")):
        disposition = "RESEARCH_PROTOTYPE_INCOMPLETE_2023_REPLAY"
    else:
        disposition = "RESEARCH_PROTOTYPE_FORWARD_EVIDENCE_REQUIRED"
    record = {
        "candidate_id": hypothesis.child_candidate_id,
        "parent_candidate_id": hypothesis.parent_candidate_id,
        "status": "RESEARCH_PROTOTYPE",
        "source_parent_status": candidate.get("status"),
        "status_inherited": False,
        "inherited_passes": [],
        "role": hypothesis.strategy_role,
        "objective_pool": hypothesis.objective_pool,
        "hypothesis": hypothesis.to_record(),
        "guard": guard,
        "structural_fingerprint": _canonical_hash(
            [hypothesis.parent_candidate_id, hypothesis.mutation_class, guard]
        ),
        "parent_behavior_fingerprint": parent_fingerprint,
        "behavior_fingerprint": child_fingerprint,
        "behaviorally_duplicate": duplicate,
        "metrics": metrics,
        "disposition": disposition,
        "shadow_activation_allowed": False,
        "paper_shadow_ready": False,
        "q4_access_count": 0,
        "order_capability": False,
    }
    retained = retained.copy()
    retained["candidate_id"] = hypothesis.child_candidate_id
    retained["parent_candidate_id"] = hypothesis.parent_candidate_id
    return record, retained


def _ym_gap_child(candidate: dict[str, Any], frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame] | None:
    usable = frame.loc[frame["gap_points"].notna() & frame["threshold_q75"].notna()].copy()
    training = usable.loc[usable["timestamp"].dt.year == 2023].copy()
    if len(training) < 8:
        return None
    ratio = training["gap_points"].abs() / training["threshold_q75"].astype(float).abs().replace(0.0, np.nan)
    lower, upper = [float(value) for value in ratio.quantile([0.10, 0.90]).to_list()]
    all_ratio = usable["gap_points"].abs() / usable["threshold_q75"].astype(float).abs().replace(0.0, np.nan)
    retained = usable.loc[all_ratio.between(lower, upper, inclusive="both")].copy()
    parent_id = str(candidate["candidate_id"])
    child_id = f"{parent_id}__past_gap_stability_band_v2"
    hypothesis = MutationHypothesis(
        hypothesis_id=f"hyp_{_canonical_hash([parent_id, 'gap_band_v2'])[:20]}",
        parent_candidate_id=parent_id,
        child_candidate_id=child_id,
        mutation_class=MutationClass.PAST_ONLY_GAP_STABILITY_BAND.value,
        strategy_role=classify_role(candidate).value,
        objective_pool=AccountObjectivePool.COMBINE_PASSER_POOL.value,
        exact_change="Retain gap-scale ratios inside frozen 2023 10th-to-90th percentile band.",
        intended_failure_to_repair="temporal concentration and unstable causal gap magnitude",
        predicted_effect="Remove extreme unstable gaps without fitting any 2024 outcome.",
        training_policy="Band fitted from 2023 feature values only; 2024 Q1-Q3 unchanged replay.",
        minimum_retained_fraction=0.50,
    )
    metrics = _metrics(
        retained,
        frame,
        seed=int(hypothesis.hypothesis_hash[:16], 16) % (2**32),
    )
    duplicate = _behavior_fingerprint(retained) == _behavior_fingerprint(frame)
    disposition = (
        "REJECTED_BEHAVIORAL_DUPLICATE"
        if duplicate
        else "RESEARCH_PROTOTYPE_FORWARD_EVIDENCE_REQUIRED"
    )
    record = {
        "candidate_id": child_id,
        "parent_candidate_id": parent_id,
        "status": "RESEARCH_PROTOTYPE",
        "source_parent_status": candidate.get("status"),
        "status_inherited": False,
        "inherited_passes": [],
        "role": hypothesis.strategy_role,
        "objective_pool": hypothesis.objective_pool,
        "hypothesis": hypothesis.to_record(),
        "guard": {"fit_year": 2023, "gap_ratio_lower": lower, "gap_ratio_upper": upper},
        "structural_fingerprint": _canonical_hash([parent_id, lower, upper]),
        "behavior_fingerprint": _behavior_fingerprint(retained),
        "behaviorally_duplicate": duplicate,
        "metrics": metrics,
        "disposition": disposition,
        "shadow_activation_allowed": False,
        "paper_shadow_ready": False,
        "q4_access_count": 0,
        "order_capability": False,
    }
    retained["candidate_id"] = child_id
    retained["parent_candidate_id"] = parent_id
    return record, retained


def _ym_micro_first_hypothesis(candidate: dict[str, Any], frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    parent_id = str(candidate["candidate_id"])
    child_id = f"{parent_id}__micro_first_one_contract_v2"
    hypothesis = MutationHypothesis(
        hypothesis_id=f"hyp_{_canonical_hash([parent_id, 'micro_first_v2'])[:20]}",
        parent_candidate_id=parent_id,
        child_candidate_id=child_id,
        mutation_class=MutationClass.MICRO_FIRST_RISK_IMPLEMENTATION.value,
        strategy_role=classify_role(candidate).value,
        objective_pool=AccountObjectivePool.XFA_PAYOUT_POOL.value,
        exact_change="One MYM contract risk implementation; signal path unchanged.",
        intended_failure_to_repair="sizing, MLL and payout-path uncertainty",
        predicted_effect="Reduce account-path risk without claiming a distinct economic edge.",
        training_policy="No fitted parameter; exact frozen MYM event path.",
        minimum_retained_fraction=1.0,
    )
    metrics = _metrics(
        frame,
        frame,
        seed=int(hypothesis.hypothesis_hash[:16], 16) % (2**32),
    )
    record = {
        "candidate_id": child_id,
        "parent_candidate_id": parent_id,
        "status": "RESEARCH_PROTOTYPE",
        "source_parent_status": candidate.get("status"),
        "status_inherited": False,
        "inherited_passes": [],
        "role": hypothesis.strategy_role,
        "objective_pool": hypothesis.objective_pool,
        "hypothesis": hypothesis.to_record(),
        "behavior_fingerprint": _behavior_fingerprint(frame),
        "behaviorally_duplicate": True,
        "metrics": metrics,
        "disposition": "BACKUP_RISK_CONFIGURATION_NOT_DISTINCT_STRATEGY",
        "counts_as_distinct_strategy": False,
        "shadow_activation_allowed": False,
        "paper_shadow_ready": False,
        "q4_access_count": 0,
        "order_capability": False,
    }
    output = frame.copy()
    output["candidate_id"] = child_id
    output["parent_candidate_id"] = parent_id
    return record, output


def run_promising_lineage_mutation(
    output_dir: Path,
    *,
    source_manifest_path: Path,
    source_manifest_sha256: str,
    code_commit: str,
    source_root: Path | None = None,
) -> dict[str, Any]:
    if _sha256(source_manifest_path) != source_manifest_sha256:
        raise MutationIntegrityError("Promising-lineage source manifest hash drift")
    manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if bool(manifest.get("q4_access_allowed")) or bool(manifest.get("live_or_broker_allowed")):
        raise MutationIntegrityError("Mutation manifest attempts protected or live access")
    if str(manifest.get("development_end_exclusive")) != "2024-10-01":
        raise MutationIntegrityError("Unexpected development data boundary")
    candidates_spec = list(manifest.get("candidates") or [])
    if len(candidates_spec) != 16 or len({row["candidate_id"] for row in candidates_spec}) != 16:
        raise MutationIntegrityError("The immutable mutation cohort must contain exactly 16 parents")

    output_dir.mkdir(parents=True, exist_ok=True)
    candidates: list[dict[str, Any]] = []
    trade_outputs: list[pd.DataFrame] = []
    parent_audit: list[dict[str, Any]] = []
    source_cache: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
    for item in candidates_spec:
        source_name = str(item["source"])
        source_spec = dict(manifest["sources"][source_name])
        if source_name not in source_cache:
            result_path = _resolve_source(source_spec["result_path"], source_root)
            ledger_path = _resolve_source(source_spec["ledger_path"], source_root)
            if _sha256(result_path) != source_spec["result_sha256"]:
                raise MutationIntegrityError(f"Frozen result hash drift: {source_name}")
            if _sha256(ledger_path) != source_spec["ledger_sha256"]:
                raise MutationIntegrityError(f"Frozen trade-ledger hash drift: {source_name}")
            source_cache[source_name] = (
                json.loads(result_path.read_text(encoding="utf-8")),
                _load_jsonl(ledger_path),
            )
        result, ledger_rows = source_cache[source_name]
        parent = _candidate_record(result, str(item["candidate_id"]))
        parent_snapshot = _canonical_hash(parent)
        frame = _normalize_rows(ledger_rows, parent)
        child, child_trades = _evaluate_primary(parent, frame)
        candidates.append(child)
        trade_outputs.append(child_trades)
        parent_audit.append(
            {
                "candidate_id": parent["candidate_id"],
                "source": source_name,
                "parent_record_hash_before": parent_snapshot,
                "parent_record_hash_after": _canonical_hash(parent),
                "parent_unchanged": parent_snapshot == _canonical_hash(parent),
                "source_result_sha256": source_spec["result_sha256"],
                "source_ledger_sha256": source_spec["ledger_sha256"],
                "normalized_events": int(len(frame)),
                "execution_symbol_used": str(frame.iloc[0]["symbol"]),
            }
        )
        if str(parent["candidate_id"]) == "strategy_open_gap_continuation_YM_v1":
            gap_child = _ym_gap_child(parent, frame)
            if gap_child is not None:
                candidates.append(gap_child[0])
                trade_outputs.append(gap_child[1])
            micro_child = _ym_micro_first_hypothesis(parent, frame)
            candidates.append(micro_child[0])
            trade_outputs.append(micro_child[1])

    raw_p = [float((row.get("metrics") or {}).get("candidate_null_raw_p", 1.0)) for row in candidates]
    adjusted = _bh_adjust(raw_p)
    for row, value in zip(candidates, adjusted, strict=True):
        row["metrics"]["candidate_null_bh_adjusted_p"] = float(value)
        row["candidate_level_null_pass"] = bool(value <= 0.20)
        row["candidate_level_null_is_promotional"] = False

    ledger_path = output_dir / "promising_lineage_mutation_trade_ledger.jsonl"
    with ledger_path.open("w", encoding="utf-8") as handle:
        for frame in trade_outputs:
            for row in frame.itertuples(index=False):
                payload = {
                    "candidate_id": row.candidate_id,
                    "parent_candidate_id": row.parent_candidate_id,
                    "timestamp": row.timestamp.isoformat(),
                    "event_session_id": row.event_session_id,
                    "symbol": row.symbol,
                    "active_contract": row.active_contract,
                    "net_pnl": float(row.net_pnl),
                    "gross_pnl": float(row.gross_pnl),
                    "cost": float(row.cost),
                    "mae_dollars": float(row.mae_dollars),
                }
                handle.write(json.dumps(payload, sort_keys=True) + "\n")

    pool_counts: dict[str, int] = {}
    for row in candidates:
        pool = str(row["objective_pool"])
        pool_counts[pool] = pool_counts.get(pool, 0) + 1
    accepted = [
        row
        for row in candidates
        if str(row["disposition"]).startswith("RESEARCH_PROTOTYPE_")
        and not bool(row.get("behaviorally_duplicate"))
    ]
    result: dict[str, Any] = {
        "schema": "hydra_promising_lineage_mutation_result_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "code_commit": code_commit,
        "source_manifest_path": str(source_manifest_path),
        "source_manifest_sha256": source_manifest_sha256,
        "data_role": "DEVELOPMENT_AND_FALSIFICATION_ONLY",
        "development_end_exclusive": "2024-10-01",
        "parent_count": len(parent_audit),
        "candidate_count": len(candidates),
        "structural_prototypes": len(candidates),
        "mutation_hypothesis_count": len(candidates),
        "primary_child_count": 16,
        "ym_versioned_hypotheses": sum(
            str(row["parent_candidate_id"]) == "strategy_open_gap_continuation_YM_v1"
            for row in candidates
        ),
        "accepted_research_prototypes": len(accepted),
        "behavioral_duplicates_rejected": sum(
            bool(row.get("behaviorally_duplicate")) for row in candidates
        ),
        "objective_pool_counts": pool_counts,
        "parent_audit": parent_audit,
        "candidates": candidates,
        "promising_candidates": 0,
        "shadow_candidates": 0,
        "shadow_research_active": 0,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": sum(
            bool(((row.get("metrics") or {}).get("topstep") or {}).get("path_candidate"))
            for row in accepted
        ),
        "q4_access_count": 0,
        "paid_data_requests": 0,
        "network_requests": 0,
        "order_capability": False,
        "scientific_conclusion": (
            "TARGETED_MUTATIONS_CREATED_FORWARD_EVIDENCE_REQUIRED"
            if accepted
            else "TARGETED_MUTATIONS_FALSIFIED_OR_DUPLICATE"
        ),
        "next_recommended_action": (
            "Role-specific promotion of non-duplicate children, while active shadow parents remain immutable."
        ),
        "artifacts": {
            "trade_ledger_path": str(ledger_path),
            "trade_ledger_sha256": _sha256(ledger_path),
        },
    }
    result["result_hash"] = _canonical_hash(result)
    result_path = output_dir / "promising_lineage_mutation_result.json"
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    result["artifacts"]["result_json_path"] = str(result_path)
    result["artifacts"]["result_json_sha256"] = _sha256(result_path)
    return result
