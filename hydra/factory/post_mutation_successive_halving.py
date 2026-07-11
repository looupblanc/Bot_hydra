"""Deterministic, role-specific halving of frozen lineage mutations.

This module is deliberately evidence-neutral with respect to shadow and funded
deployment.  It can only identify children worth a new, frozen promotion run.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from hydra.portfolio.account_contribution import replay_shared_account


POLICY_VERSION = "post_mutation_successive_halving_v1"
PREREGISTRATION_SHA256 = "f48f84c74e7ba714edcbe7a2d692ed4e538b1e46090d350f47c88f579c882dce"
DEVELOPMENT_END_EXCLUSIVE = pd.Timestamp("2024-10-01T00:00:00Z")
BOOTSTRAP_REPLICATES = 2_048
MAXIMUM_ELITES = 12
ALLOWED_SOURCE_STATUS = "RESEARCH_PROTOTYPE"
ALLOWED_OUTPUT_STATUSES = {
    "PROMISING_RESEARCH_CANDIDATE",
    "INSUFFICIENT_EVIDENCE",
    "RESEARCH_REJECTED",
    "HARD_INTEGRITY_REJECTED",
}
PROHIBITED_STATUSES = {
    "SHADOW_RESEARCH_ACTIVE",
    "PAPER_SHADOW_READY",
    "TRADING_READY_CANDIDATE",
    "FUNDED_DEPLOYMENT_ELIGIBLE",
}
POOLS = {
    "COMBINE_PASSER_POOL",
    "XFA_PAYOUT_POOL",
    "DEFENSIVE_ACCOUNT_POOL",
}


class PostMutationIntegrityError(RuntimeError):
    """A frozen input or a hard evidence invariant was violated."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _atomic_json(path: Path, value: Any, *, pretty: bool = True) -> None:
    payload = (
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
        if pretty
        else _canonical_bytes(value).decode("utf-8") + "\n"
    )
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(payload, encoding="utf-8")
    os.replace(temporary, path)


def _atomic_text(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _verify_file(path: Path, expected_hash: str, label: str) -> None:
    if not path.is_file():
        raise PostMutationIntegrityError(f"Missing {label}: {path}")
    actual = _sha256(path)
    if actual != str(expected_hash):
        raise PostMutationIntegrityError(
            f"{label} hash drift: expected {expected_hash}, observed {actual}"
        )


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise PostMutationIntegrityError(
                    f"Invalid mutation trade ledger JSON at line {line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise PostMutationIntegrityError(
                    f"Non-object mutation trade ledger row at line {line_number}"
                )
            values.append(value)
    if not values:
        raise PostMutationIntegrityError("Empty mutation trade ledger")
    return values


def _validate_result(result: dict[str, Any], ledger_sha256: str) -> list[dict[str, Any]]:
    if str(result.get("schema")) != "hydra_promising_lineage_mutation_result_v1":
        raise PostMutationIntegrityError("Unexpected mutation-result schema")
    if str(result.get("development_end_exclusive")) != "2024-10-01":
        raise PostMutationIntegrityError("Mutation result changes the development boundary")
    for field in ("q4_access_count", "network_requests", "paid_data_requests"):
        if int(result.get(field) or 0) != 0:
            raise PostMutationIntegrityError(f"Mutation result reports prohibited {field}")
    if bool(result.get("order_capability")):
        raise PostMutationIntegrityError("Mutation result contains order capability")
    artifact_hash = str((result.get("artifacts") or {}).get("trade_ledger_sha256") or "")
    if artifact_hash != ledger_sha256:
        raise PostMutationIntegrityError("Mutation result/ledger provenance mismatch")
    recorded_hash = str(result.get("result_hash") or "")
    hash_payload = dict(result)
    hash_payload.pop("result_hash", None)
    if recorded_hash and _canonical_hash(hash_payload) != recorded_hash:
        raise PostMutationIntegrityError("Mutation result canonical hash drift")
    candidates = list(result.get("candidates") or [])
    identifiers = [str(row.get("candidate_id") or "") for row in candidates]
    if not candidates or "" in identifiers or len(identifiers) != len(set(identifiers)):
        raise PostMutationIntegrityError("Missing or duplicate mutation candidate IDs")
    parent_audit = list(result.get("parent_audit") or [])
    if not parent_audit or not all(bool(row.get("parent_unchanged")) for row in parent_audit):
        raise PostMutationIntegrityError("A frozen mutation parent changed")
    for candidate in candidates:
        _validate_candidate_contract(candidate)
    return candidates


def _validate_candidate_contract(candidate: dict[str, Any]) -> None:
    candidate_id = str(candidate.get("candidate_id") or "")
    if str(candidate.get("status")) != ALLOWED_SOURCE_STATUS:
        raise PostMutationIntegrityError(f"{candidate_id}: source status is not a prototype")
    if bool(candidate.get("status_inherited")) or list(candidate.get("inherited_passes") or []):
        raise PostMutationIntegrityError(f"{candidate_id}: parent pass/status inheritance")
    if bool(candidate.get("paper_shadow_ready")) or bool(
        candidate.get("shadow_activation_allowed")
    ):
        raise PostMutationIntegrityError(f"{candidate_id}: premature shadow/paper promotion")
    if int(candidate.get("q4_access_count") or 0) != 0 or bool(
        candidate.get("order_capability")
    ):
        raise PostMutationIntegrityError(f"{candidate_id}: protected/order capability")
    hypothesis = dict(candidate.get("hypothesis") or {})
    if str(hypothesis.get("child_candidate_id") or "") != candidate_id:
        raise PostMutationIntegrityError(f"{candidate_id}: mutation hypothesis mismatch")
    if str(hypothesis.get("parent_candidate_id") or "") != str(
        candidate.get("parent_candidate_id") or ""
    ):
        raise PostMutationIntegrityError(f"{candidate_id}: parent provenance mismatch")
    if bool(hypothesis.get("status_inheritance_allowed")) or bool(
        hypothesis.get("q4_access_allowed")
    ) or bool(hypothesis.get("live_or_broker_allowed")):
        raise PostMutationIntegrityError(f"{candidate_id}: unsafe mutation hypothesis")
    if str(candidate.get("objective_pool") or "") not in POOLS:
        raise PostMutationIntegrityError(f"{candidate_id}: unknown objective pool")
    guard = dict(candidate.get("guard") or {})
    mutation_class = str(hypothesis.get("mutation_class") or "")
    if "current_event_outcome_used" in guard and (
        bool(guard.get("current_event_outcome_used"))
        or int(guard.get("activation_shift_periods") or 0) < 1
    ):
        raise PostMutationIntegrityError(f"{candidate_id}: past-only guard not proved")
    if (
        mutation_class.startswith("PRIOR_EQUITY")
        or mutation_class == "AVOIDED_LOSS_POLICY_GUARD"
    ):
        if bool(guard.get("current_event_outcome_used", True)) or int(
            guard.get("activation_shift_periods") or 0
        ) < 1:
            raise PostMutationIntegrityError(f"{candidate_id}: past-only guard not proved")
    if mutation_class == "PAST_ONLY_GAP_STABILITY_BAND" and int(
        guard.get("fit_year") or 0
    ) != 2023:
        raise PostMutationIntegrityError(f"{candidate_id}: gap band not frozen on 2023")


def _normalize_ledger(
    rows: Iterable[dict[str, Any]], candidate_ids: set[str]
) -> pd.DataFrame:
    required = {
        "candidate_id",
        "parent_candidate_id",
        "timestamp",
        "event_session_id",
        "symbol",
        "active_contract",
        "net_pnl",
        "gross_pnl",
        "cost",
        "mae_dollars",
    }
    frame = pd.DataFrame(list(rows))
    missing = sorted(required - set(frame.columns))
    if missing:
        raise PostMutationIntegrityError(f"Mutation trade ledger missing columns: {missing}")
    if not set(frame["candidate_id"].astype(str)).issubset(candidate_ids):
        raise PostMutationIntegrityError("Trade ledger contains an unknown child")
    frame["candidate_id"] = frame["candidate_id"].astype(str)
    frame["parent_candidate_id"] = frame["parent_candidate_id"].astype(str)
    frame["event_session_id"] = frame["event_session_id"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["active_contract"] = frame["active_contract"].astype(str)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="raise")
    if bool((frame["timestamp"] >= DEVELOPMENT_END_EXCLUSIVE).any()):
        raise PostMutationIntegrityError("Q4-or-later trade reached post-mutation halving")
    for column in ("availability_timestamp", "source_bar_close", "decision_timestamp"):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
            if bool((frame[column] > frame["timestamp"]).any()):
                raise PostMutationIntegrityError(f"Future information in {column}")
    for column in ("net_pnl", "gross_pnl", "cost", "mae_dollars"):
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype(float)
        if not np.isfinite(frame[column]).all():
            raise PostMutationIntegrityError(f"Non-finite {column}")
    if bool((frame["cost"] < 0).any()) or bool((frame["mae_dollars"] > 1e-9).any()):
        raise PostMutationIntegrityError("Invalid cost or adverse-excursion sign")
    if not np.allclose(
        frame["gross_pnl"].to_numpy() - frame["cost"].to_numpy(),
        frame["net_pnl"].to_numpy(),
        atol=1e-6,
        rtol=1e-9,
    ):
        raise PostMutationIntegrityError("Gross-cost does not reconcile to net PnL")
    keys = [
        "candidate_id",
        "timestamp",
        "event_session_id",
        "symbol",
        "active_contract",
    ]
    if bool(frame.duplicated(keys).any()):
        raise PostMutationIntegrityError("Duplicate or ambiguous child trade key")
    counts = frame.groupby("candidate_id").size()
    missing_children = sorted(candidate_ids - set(counts.index.astype(str)))
    if missing_children:
        raise PostMutationIntegrityError(f"Children missing trade evidence: {missing_children}")
    return frame.sort_values(keys, kind="mergesort").reset_index(drop=True)


def _behavior_fingerprint(frame: pd.DataFrame) -> str:
    ordered = frame.sort_values(
        ["timestamp", "event_session_id", "symbol", "active_contract"], kind="mergesort"
    )
    values = [
        [
            row.timestamp.isoformat(),
            str(row.event_session_id),
            str(row.symbol),
            str(row.active_contract),
        ]
        for row in ordered.itertuples(index=False)
    ]
    return _canonical_hash(values)


def _fold_name(timestamp: pd.Timestamp) -> str:
    if timestamp.year == 2023:
        return "2023_AVAILABLE"
    if timestamp.year == 2024 and timestamp.quarter <= 3:
        return f"2024_Q{timestamp.quarter}"
    return "OUTSIDE_ALLOWED_DEVELOPMENT"


def _market_and_ecology(frame: pd.DataFrame) -> tuple[str, str]:
    symbols = sorted(set(frame["symbol"].astype(str)))
    market = "+".join(symbols)
    roots = {symbol.lstrip("M") for symbol in symbols}
    if roots & {"CL"}:
        ecology = "energy"
    elif roots & {"GC"}:
        ecology = "metals"
    elif roots & {"ES", "NQ", "RTY", "YM", "2K"}:
        ecology = "equity_indices"
    else:
        ecology = "other"
    return market, ecology


def _mechanism_family(candidate: dict[str, Any]) -> str:
    parent = str(candidate.get("parent_candidate_id") or "").lower()
    for token, family in (
        ("open_gap", "opening_gap"),
        ("barrier_hazard", "barrier_hazard"),
        ("shared_loss_hazard", "shared_loss_hazard"),
        ("hazard", "hazard"),
        ("cross_asset", "cross_asset"),
        ("relative", "relative_value"),
        ("session", "session_geometry"),
        ("participation", "participation_state"),
        ("volatility", "volatility_state"),
    ):
        if token in parent:
            return family
    return str((candidate.get("hypothesis") or {}).get("mutation_class") or "unknown").lower()


def _account_metrics(candidate_id: str, frame: pd.DataFrame) -> dict[str, Any]:
    entries = frame.sort_values("timestamp", kind="mergesort").reset_index(drop=True).copy()
    replay = pd.DataFrame(
        {
            "strategy_id": candidate_id,
            "trade_id": [f"{candidate_id}:{index}" for index in range(len(entries))],
            "event_session_id": entries["event_session_id"],
            "entry_timestamp": entries["timestamp"],
            "exit_timestamp": entries["timestamp"] + pd.Timedelta(minutes=1),
            "net_pnl": entries["net_pnl"],
            "mae_dollars": entries["mae_dollars"],
            "cost": entries["cost"],
            "contracts": 1,
            "underlying": entries["symbol"],
            "side": 0.0,
        }
    )
    return replay_shared_account({candidate_id: replay}).to_dict()


def _sequence_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    net = float(frame["net_pnl"].sum())
    day_net = frame.groupby("event_session_id", sort=True)["net_pnl"].sum()
    month_net = frame.assign(month=frame["timestamp"].dt.strftime("%Y-%m")).groupby(
        "month", sort=True
    )["net_pnl"].sum()
    ordered_days = day_net.sort_values(kind="mergesort").to_numpy(dtype=float)
    cumulative = np.cumsum(ordered_days)
    peak = np.maximum.accumulate(np.maximum(cumulative, 0.0)) if len(cumulative) else np.array([])
    worst_first_drawdown = abs(float(np.min(cumulative - peak))) if len(cumulative) else 0.0
    positive = float(frame.loc[frame["net_pnl"] > 0, "net_pnl"].sum())
    best_trade = float(max(0.0, frame["net_pnl"].max()))
    return {
        "best_trade_removed_net": net - best_trade,
        "best_day_removed_net": net - float(max(0.0, day_net.max())),
        "best_month_removed_net": net - float(max(0.0, month_net.max())),
        "best_trade_share_of_positive": best_trade / positive if positive > 0 else 1.0,
        "worst_first_maximum_drawdown": worst_first_drawdown,
    }


def _bootstrap(candidate_id: str, frame: pd.DataFrame, pool: str) -> dict[str, Any]:
    ordered = frame.sort_values(["timestamp", "event_session_id"], kind="mergesort")
    net = ordered["net_pnl"].to_numpy(dtype=float)
    mae = ordered["mae_dollars"].to_numpy(dtype=float)
    count = len(net)
    block_length = max(2, int(round(math.sqrt(count))))
    block_count = int(math.ceil(count / block_length))
    seed = int(hashlib.sha256(candidate_id.encode("utf-8")).hexdigest()[:16], 16) % (2**32)
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, count, size=(BOOTSTRAP_REPLICATES, block_count))
    offsets = np.arange(block_length, dtype=int)
    indices = ((starts[:, :, None] + offsets) % count).reshape(BOOTSTRAP_REPLICATES, -1)
    indices = indices[:, :count]
    sampled = net[indices]
    sampled_mae = mae[indices]
    cumulative = np.cumsum(sampled, axis=1)
    prior = np.concatenate(
        [np.zeros((BOOTSTRAP_REPLICATES, 1)), cumulative[:, :-1]], axis=1
    )
    peak = np.maximum.accumulate(np.maximum(cumulative, 0.0), axis=1)
    drawdown = np.abs(np.min(cumulative - peak, axis=1))
    trailing_high_before = np.maximum.accumulate(np.maximum(prior, 0.0), axis=1)
    buffer = prior + sampled_mae - (trailing_high_before - 4_500.0)
    breach = buffer < 0.0
    breach_prefix = np.maximum.accumulate(breach, axis=1)
    target = cumulative >= 9_000.0
    target_before_mll = np.any(target & ~breach_prefix, axis=1)
    target_index = np.argmax(target & ~breach_prefix, axis=1) + 1
    target_index = np.where(target_before_mll, target_index, np.nan)
    payout = cumulative >= 5_000.0
    payout_hit = np.any(payout & ~breach_prefix, axis=1)
    payout_index = np.argmax(payout & ~breach_prefix, axis=1) + 1
    payout_index = np.where(payout_hit, payout_index, np.nan)
    payout_cycles = np.floor(np.maximum(cumulative.max(axis=1), 0.0) / 5_000.0)
    qualifying_frequency = np.mean(sampled >= 200.0, axis=1)
    total = sampled.sum(axis=1)
    if pool == "COMBINE_PASSER_POOL":
        utility = (
            2.0 * target_before_mll.astype(float)
            + np.clip(total / 9_000.0, -1.0, 1.0)
            - np.clip(drawdown / 4_500.0, 0.0, 2.0)
        )
    elif pool == "XFA_PAYOUT_POOL":
        utility = (
            np.clip(payout_cycles, 0.0, 3.0)
            + qualifying_frequency
            + (~np.any(breach, axis=1)).astype(float)
        )
    else:
        utility = -np.clip(drawdown / 4_500.0, 0.0, 3.0)
    quantiles = (0.05, 0.50, 0.95)
    return {
        "replicates": BOOTSTRAP_REPLICATES,
        "block_length": block_length,
        "seed": seed,
        "net_pnl_quantiles": [float(value) for value in np.quantile(total, quantiles)],
        "maximum_drawdown_quantiles": [
            float(value) for value in np.quantile(drawdown, quantiles)
        ],
        "objective_utility_quantiles": [
            float(value) for value in np.quantile(utility, quantiles)
        ],
        "probability_positive_net": float(np.mean(total > 0.0)),
        "target_before_mll_probability": float(np.mean(target_before_mll)),
        "median_events_to_target": (
            float(np.nanmedian(target_index)) if bool(target_before_mll.any()) else None
        ),
        "expected_payout_cycles_before_ruin": float(np.mean(payout_cycles * ~np.any(breach, axis=1))),
        "qualifying_event_frequency": float(np.mean(qualifying_frequency)),
        "mll_survival_probability": float(np.mean(~np.any(breach, axis=1))),
        "post_payout_survival_probability": float(np.mean(payout_hit & ~np.any(breach, axis=1))),
        "median_events_to_payout": (
            float(np.nanmedian(payout_index)) if bool(payout_hit.any()) else None
        ),
    }


def _defensive_evidence(candidate: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("defensive_evidence", "account_contribution", "portfolio_role_evidence"):
        value = candidate.get(key)
        if isinstance(value, dict):
            controls = value.get("controls") if isinstance(value.get("controls"), dict) else {}
            return {
                "pool_utility_delta": float(value.get("pool_utility_delta") or 0.0),
                "maximum_drawdown_reduction": float(
                    value.get("maximum_drawdown_reduction") or 0.0
                ),
                "min_mll_buffer_delta": float(value.get("min_mll_buffer_delta") or 0.0),
                "shared_loss_days_reduction": int(value.get("shared_loss_days_reduction") or 0),
                "matched_control_p": float(
                    value.get("matched_control_p")
                    or controls.get("one_sided_p_value")
                    or 1.0
                ),
                "hard_risk_violation": bool(value.get("hard_risk_violation")),
            }
    return None


def _objective_score(
    pool: str,
    account: dict[str, Any],
    bootstrap: dict[str, Any],
    net_pnl: float,
    defensive: dict[str, Any] | None,
) -> float:
    net_component = float(np.tanh(net_pnl / 9_000.0))
    if pool == "COMBINE_PASSER_POOL":
        return float(
            2.0 * bootstrap["target_before_mll_probability"]
            + bootstrap["probability_positive_net"]
            + net_component
            + max(-1.0, float(account["consistency_margin"]))
            - min(2.0, float(account["maximum_drawdown"]) / 4_500.0)
            - float(account["execution_cost_burden"])
        )
    if pool == "XFA_PAYOUT_POOL":
        return float(
            bootstrap["expected_payout_cycles_before_ruin"]
            + bootstrap["qualifying_event_frequency"]
            + bootstrap["mll_survival_probability"]
            + bootstrap["post_payout_survival_probability"]
            + net_component
        )
    if not defensive:
        return -1_000_000.0
    return float(
        defensive["pool_utility_delta"]
        + defensive["maximum_drawdown_reduction"] / 4_500.0
        + defensive["min_mll_buffer_delta"] / 4_500.0
        + defensive["shared_loss_days_reduction"] / 10.0
        - defensive["matched_control_p"]
    )


def _evaluate_candidate(candidate: dict[str, Any], frame: pd.DataFrame) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    pool = str(candidate["objective_pool"])
    market, ecology = _market_and_ecology(frame)
    family = _mechanism_family(candidate)
    behavior = _behavior_fingerprint(frame)
    fold_frame = frame.assign(fold=frame["timestamp"].map(_fold_name))
    folds = {
        str(name): {"events": int(len(group)), "net_pnl": float(group["net_pnl"].sum())}
        for name, group in fold_frame.groupby("fold", sort=True)
    }
    fold_values = [float(row["net_pnl"]) for row in folds.values()]
    positives = [value for value in fold_values if value > 0.0]
    negatives = [value for value in fold_values if value < 0.0]
    pooled_net = float(frame["net_pnl"].sum())
    gross = float(frame["gross_pnl"].sum())
    total_cost = float(frame["cost"].sum())
    cost_stress = {
        "1.0x": pooled_net,
        "1.5x": gross - 1.5 * total_cost,
        "2.0x": gross - 2.0 * total_cost,
    }
    sequence = _sequence_metrics(frame)
    account = _account_metrics(candidate_id, frame)
    boot = _bootstrap(candidate_id, frame, pool)
    defensive = _defensive_evidence(candidate)
    weak_fold_non_catastrophic = bool(
        len(negatives) == 1
        and positives
        and abs(negatives[0]) <= 0.75 * sum(positives)
        and not bool(account["mll_breached"])
        and sequence["best_trade_removed_net"] > 0.0
        and sequence["best_day_removed_net"] > 0.0
    )
    temporal_pass = bool(
        len(folds) >= 2
        and positives
        and (not negatives or weak_fold_non_catastrophic)
        and len(negatives) <= 1
    )
    uncertainty: list[str] = []
    if len(frame) < 12:
        uncertainty.append("LOW_EVENT_COUNT")
    if 2023 not in set(frame["timestamp"].dt.year.astype(int)):
        uncertainty.append("FULL_2023_REPLAY_UNAVAILABLE")
    if negatives:
        uncertainty.append("WEAK_DEVELOPMENT_FOLD")
    if sequence["best_month_removed_net"] <= 0.0:
        uncertainty.append("BEST_MONTH_CONCENTRATION")
    adjusted_p = float((candidate.get("metrics") or {}).get("candidate_null_bh_adjusted_p", 1.0))
    source_metrics = dict(candidate.get("metrics") or {})
    mutation_evidence = {
        # Evidence-only propagation for a separately preregistered downstream
        # shadow-admission audit.  None of these fields participates in the
        # halving score, Pareto vector, deduplication, or elite selection.
        "hypothesis": dict(candidate.get("hypothesis") or {}),
        "guard": dict(candidate.get("guard") or {}),
        "structural_fingerprint": str(candidate.get("structural_fingerprint") or ""),
        "parent_behavior_fingerprint": str(
            candidate.get("parent_behavior_fingerprint") or ""
        ),
        "source_behavior_fingerprint": str(candidate.get("behavior_fingerprint") or ""),
        "retained_fraction": float(source_metrics.get("retained_fraction") or 0.0),
        "full_2023_replay_available": bool(
            source_metrics.get("full_2023_replay_available")
        ),
        "candidate_null_bh_adjusted_p": adjusted_p,
        "topstep": dict(source_metrics.get("topstep") or {}),
    }
    mutation_evidence["evidence_hash"] = _canonical_hash(mutation_evidence)
    if adjusted_p > 0.20:
        uncertainty.append("CANDIDATE_NULL_NOT_SUPPORTIVE")
    if cost_stress["2.0x"] <= 0.0:
        uncertainty.append("DOUBLE_COST_DIAGNOSTIC_FAILED")
    if boot["probability_positive_net"] < 0.60:
        uncertainty.append("BLOCK_BOOTSTRAP_UNCERTAIN")

    base_alpha_pass = bool(
        pooled_net > 0.0
        and cost_stress["1.5x"] > 0.0
        and temporal_pass
        and not bool(account["mll_breached"])
        and sequence["best_trade_removed_net"] > 0.0
        and sequence["best_day_removed_net"] > 0.0
        and len(frame) >= 12
        and boot["probability_positive_net"] >= 0.60
    )
    if pool != "DEFENSIVE_ACCOUNT_POOL" and (
        pooled_net <= 0.0
        or len(negatives) > 1
        or (len(negatives) == 1 and not weak_fold_non_catastrophic)
    ):
        status = "RESEARCH_REJECTED"
        disposition = "NEGATIVE_OR_CATASTROPHIC_DEVELOPMENT_EVIDENCE"
    elif pool == "DEFENSIVE_ACCOUNT_POOL":
        defensive_pass = bool(
            defensive
            and defensive["pool_utility_delta"] > 0.0
            and defensive["maximum_drawdown_reduction"] > 0.0
            and defensive["min_mll_buffer_delta"] > 0.0
            and defensive["shared_loss_days_reduction"] >= 0
            and defensive["matched_control_p"] <= 0.20
            and not defensive["hard_risk_violation"]
        )
        status = "PROMISING_RESEARCH_CANDIDATE" if defensive_pass else "INSUFFICIENT_EVIDENCE"
        disposition = (
            "DEFENSIVE_MARGINAL_UTILITY_SUPPORTED"
            if defensive_pass
            else "DEFENSIVE_MATCHED_CONTROL_EVIDENCE_REQUIRED"
        )
    elif pool == "XFA_PAYOUT_POOL":
        xfa_pass = bool(
            base_alpha_pass
            and boot["expected_payout_cycles_before_ruin"] >= 1.0
            and boot["mll_survival_probability"] >= 0.50
        )
        status = "PROMISING_RESEARCH_CANDIDATE" if xfa_pass else "INSUFFICIENT_EVIDENCE"
        disposition = "XFA_PATH_SUPPORTED" if xfa_pass else "XFA_PAYOUT_OBSERVATIONS_INSUFFICIENT"
    else:
        status = "PROMISING_RESEARCH_CANDIDATE" if base_alpha_pass else "INSUFFICIENT_EVIDENCE"
        disposition = "COMBINE_RESEARCH_PATH_SUPPORTED" if base_alpha_pass else "COMBINE_EVIDENCE_INSUFFICIENT"
    objective_score = _objective_score(pool, account, boot, pooled_net, defensive)
    return {
        "candidate_id": candidate_id,
        "parent_candidate_id": str(candidate["parent_candidate_id"]),
        "source_status": str(candidate["status"]),
        "status": status,
        "status_inherited": False,
        "inherited_passes": [],
        "role": str(candidate.get("role") or "ALPHA"),
        "objective_pool": pool,
        "mechanism_family": family,
        "market": market,
        "market_ecology": ecology,
        "behavior_fingerprint": behavior,
        "behaviorally_duplicate": False,
        "duplicate_of": None,
        "event_count": int(len(frame)),
        "active_month_count": int(frame["timestamp"].dt.strftime("%Y-%m").nunique()),
        "pooled_gross_pnl": gross,
        "pooled_cost": total_cost,
        "pooled_net_pnl": pooled_net,
        "cost_stress": cost_stress,
        "folds": folds,
        "positive_fold_count": len(positives),
        "negative_fold_count": len(negatives),
        "weak_fold_non_catastrophic": weak_fold_non_catastrophic,
        "temporal_screen_pass": temporal_pass,
        "sequence_stress": sequence,
        "account_path": account,
        "block_bootstrap": boot,
        "defensive_evidence": defensive,
        "candidate_null_bh_adjusted_p": adjusted_p,
        "mutation_evidence": mutation_evidence,
        "uncertainty_flags": sorted(set(uncertainty)),
        "objective_score": objective_score,
        "pareto_front": False,
        "selected_elite": False,
        "disposition": disposition,
        "q4_access_count": 0,
        "order_capability": False,
        "shadow_research_active": False,
        "paper_shadow_ready": False,
        "promotion_eligible": False,
    }


def _dominance_vector(row: dict[str, Any]) -> tuple[float, ...]:
    pool = str(row["objective_pool"])
    account = row["account_path"]
    boot = row["block_bootstrap"]
    if pool == "COMBINE_PASSER_POOL":
        return (
            float(boot["target_before_mll_probability"]),
            float(boot["probability_positive_net"]),
            float(row["cost_stress"]["1.5x"]),
            -float(account["maximum_drawdown"]),
            -float(row["sequence_stress"]["best_trade_share_of_positive"]),
        )
    if pool == "XFA_PAYOUT_POOL":
        return (
            float(boot["expected_payout_cycles_before_ruin"]),
            float(boot["qualifying_event_frequency"]),
            float(boot["mll_survival_probability"]),
            float(row["cost_stress"]["1.5x"]),
            -float(account["maximum_drawdown"]),
        )
    defensive = row.get("defensive_evidence") or {}
    return (
        float(defensive.get("pool_utility_delta") or 0.0),
        float(defensive.get("maximum_drawdown_reduction") or 0.0),
        float(defensive.get("min_mll_buffer_delta") or 0.0),
        float(defensive.get("shared_loss_days_reduction") or 0.0),
        -float(defensive.get("matched_control_p") or 1.0),
    )


def _mark_pareto(rows: list[dict[str, Any]]) -> None:
    for pool in sorted(POOLS):
        members = [row for row in rows if row["objective_pool"] == pool]
        vectors = [_dominance_vector(row) for row in members]
        for index, row in enumerate(members):
            dominated = any(
                other != index
                and all(left >= right for left, right in zip(vectors[other], vectors[index]))
                and any(left > right for left, right in zip(vectors[other], vectors[index]))
                for other in range(len(members))
            )
            row["pareto_front"] = not dominated


def _deduplicate(rows: list[dict[str, Any]]) -> None:
    owners: dict[str, str] = {}
    for row in sorted(rows, key=lambda value: str(value["candidate_id"])):
        fingerprint = str(row["behavior_fingerprint"])
        owner = owners.get(fingerprint)
        if owner is None:
            owners[fingerprint] = str(row["candidate_id"])
            continue
        row["behaviorally_duplicate"] = True
        row["duplicate_of"] = owner
        row["status"] = "RESEARCH_REJECTED"
        row["disposition"] = "REJECTED_BEHAVIORAL_DUPLICATE"


def _select_elites(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row["status"] == "PROMISING_RESEARCH_CANDIDATE"
        and not row["behaviorally_duplicate"]
    ]
    target = min(MAXIMUM_ELITES, len({row["parent_candidate_id"] for row in eligible}))
    family_limit = max(1, int(math.ceil(max(target, 1) * 0.25)))
    ecology_limit = max(1, int(math.ceil(max(target, 1) * 0.40)))
    ranked = sorted(
        eligible,
        key=lambda row: (
            -int(bool(row["pareto_front"])),
            -float(row["objective_score"]),
            str(row["candidate_id"]),
        ),
    )
    selected: list[dict[str, Any]] = []
    parents: set[str] = set()
    families: Counter[str] = Counter()
    ecologies: Counter[str] = Counter()
    pools: Counter[str] = Counter()
    relaxations: list[dict[str, Any]] = []

    def add(row: dict[str, Any], *, relax: bool, stage: str) -> bool:
        parent = str(row["parent_candidate_id"])
        family = str(row["mechanism_family"])
        ecology = str(row["market_ecology"])
        if parent in parents:
            return False
        exceeded: list[str] = []
        if families[family] >= family_limit:
            exceeded.append("family_soft_cap")
        if ecologies[ecology] >= ecology_limit:
            exceeded.append("ecology_soft_cap")
        if exceeded and not relax:
            return False
        if exceeded:
            relaxations.append(
                {"candidate_id": row["candidate_id"], "stage": stage, "constraints": exceeded}
            )
        selected.append(row)
        parents.add(parent)
        families[family] += 1
        ecologies[ecology] += 1
        pools[str(row["objective_pool"])] += 1
        row["selected_elite"] = True
        return True

    # Seed every represented objective pool without requiring a missing pool.
    for pool in sorted({str(row["objective_pool"]) for row in ranked}):
        for row in ranked:
            if row["objective_pool"] == pool and add(row, relax=False, stage="pool_seed"):
                break
    for row in ranked:
        if len(selected) >= target:
            break
        add(row, relax=False, stage="diversified_fill")
    for row in ranked:
        if len(selected) >= target:
            break
        add(row, relax=True, stage="maximum_feasible_fill")
    return selected, {
        "target_elites": target,
        "selected_elites": len(selected),
        "maximum_feasible_achieved": len(selected) == target,
        "family_limit": family_limit,
        "ecology_limit": ecology_limit,
        "lineage_limit": 1,
        "pool_counts": dict(pools),
        "family_counts": dict(families),
        "ecology_counts": dict(ecologies),
        "soft_cap_relaxations": relaxations,
    }


def _write_outputs(
    output_dir: Path,
    result: dict[str, Any],
    evidence: list[dict[str, Any]],
    selected: list[dict[str, Any]],
) -> dict[str, Any]:
    evidence_path = output_dir / "post_mutation_candidate_evidence.jsonl"
    evidence_lines = "".join(
        _canonical_bytes(row).decode("utf-8") + "\n"
        for row in sorted(evidence, key=lambda item: str(item["candidate_id"]))
    )
    _atomic_text(evidence_path, evidence_lines)
    manifest = {
        "schema": "hydra_post_mutation_elite_manifest_v1",
        "policy_version": POLICY_VERSION,
        "source_mutation_result_sha256": result["source_mutation_result_sha256"],
        "selected_count": len(selected),
        "selected_candidate_ids": sorted(str(row["candidate_id"]) for row in selected),
        "status_ceiling": "PROMISING_RESEARCH_CANDIDATE",
        "q4_access_count": 0,
        "order_capability": False,
        "paper_shadow_ready": 0,
    }
    manifest["manifest_hash"] = _canonical_hash(manifest)
    manifest_path = output_dir / "post_mutation_elite_manifest.json"
    _atomic_json(manifest_path, manifest)
    report_lines = [
        "# HYDRA post-mutation successive-halving report",
        "",
        f"- Policy: `{POLICY_VERSION}`",
        f"- Children screened: {result['candidate_count']}",
        f"- Behavioral duplicates: {result['behavioral_duplicate_count']}",
        f"- Promising research candidates: {result['promising_candidate_count']}",
        f"- Selected diversified elites: {len(selected)}",
        f"- Insufficient evidence: {result['insufficient_evidence_count']}",
        f"- Research rejected: {result['research_rejected_count']}",
        "- Q4 access: 0",
        "- Order capability: false",
        "- Shadow/PAPER promotion: prohibited by this capability",
        "",
        "Soft robustness failures are uncertainty flags; only integrity failures abort the run.",
        "",
    ]
    report_path = output_dir / "post_mutation_successive_halving_report.md"
    _atomic_text(report_path, "\n".join(report_lines))
    result["artifacts"] = {
        "candidate_evidence": {
            "path": str(evidence_path.resolve()),
            "sha256": _sha256(evidence_path),
        },
        "elite_manifest": {
            "path": str(manifest_path.resolve()),
            "sha256": _sha256(manifest_path),
        },
        "report": {"path": str(report_path.resolve()), "sha256": _sha256(report_path)},
    }
    path_independent = dict(result)
    path_independent["artifacts"] = {
        key: value["sha256"] for key, value in sorted(result["artifacts"].items())
    }
    result["result_hash"] = _canonical_hash(path_independent)
    result_path = output_dir / "post_mutation_successive_halving_result.json"
    _atomic_json(result_path, result)
    # A file cannot contain its own SHA without a circular definition.  The
    # persisted result authenticates every upstream/output artifact; its own
    # absolute path and digest are appended only to the returned controller
    # envelope after the immutable bytes exist.
    result["artifacts"]["result"] = {
        "path": str(result_path.resolve()),
        "sha256": _sha256(result_path),
    }
    return result


def run_post_mutation_successive_halving(
    output_dir: Path,
    *,
    engineering_task_path: Path,
    engineering_task_sha256: str,
    mutation_result_path: Path,
    mutation_result_sha256: str,
    mutation_trade_ledger_path: Path,
    mutation_trade_ledger_sha256: str,
    code_commit: str,
) -> dict[str, Any]:
    """Run the frozen post-mutation screen without touching mission state."""

    output_dir = Path(output_dir)
    engineering_task_path = Path(engineering_task_path)
    mutation_result_path = Path(mutation_result_path)
    mutation_trade_ledger_path = Path(mutation_trade_ledger_path)
    if not str(code_commit).strip():
        raise PostMutationIntegrityError("code_commit is required")
    _verify_file(engineering_task_path, engineering_task_sha256, "engineering task")
    if engineering_task_sha256 != PREREGISTRATION_SHA256:
        raise PostMutationIntegrityError("Unexpected post-mutation preregistration")
    _verify_file(mutation_result_path, mutation_result_sha256, "mutation result")
    _verify_file(
        mutation_trade_ledger_path, mutation_trade_ledger_sha256, "mutation ledger"
    )
    result = json.loads(mutation_result_path.read_text(encoding="utf-8"))
    if not isinstance(result, dict):
        raise PostMutationIntegrityError("Mutation result is not an object")
    candidates = _validate_result(result, mutation_trade_ledger_sha256)
    candidate_by_id = {str(row["candidate_id"]): row for row in candidates}
    ledger = _normalize_ledger(_load_jsonl(mutation_trade_ledger_path), set(candidate_by_id))
    evaluated = [
        _evaluate_candidate(candidate_by_id[candidate_id], group.copy())
        for candidate_id, group in ledger.groupby("candidate_id", sort=True)
    ]
    # The upstream mutator already labels known backups. Recompute identity
    # across the entire cohort so clones can never inflate the archive.
    for row in evaluated:
        source = candidate_by_id[row["candidate_id"]]
        if bool(source.get("behaviorally_duplicate")):
            row["behaviorally_duplicate"] = True
            row["duplicate_of"] = str(source.get("parent_candidate_id") or "FROZEN_PARENT")
            row["status"] = "RESEARCH_REJECTED"
            row["disposition"] = "REJECTED_BEHAVIORAL_DUPLICATE_OR_BACKUP"
    _deduplicate(evaluated)
    if any(str(row["status"]) not in ALLOWED_OUTPUT_STATUSES for row in evaluated):
        raise PostMutationIntegrityError("Output status exceeds the preregistered ceiling")
    if any(str(row["status"]) in PROHIBITED_STATUSES for row in evaluated):
        raise PostMutationIntegrityError("Prohibited promotion status produced")
    _mark_pareto(
        [row for row in evaluated if not row["behaviorally_duplicate"]]
    )
    selected, selection_audit = _select_elites(evaluated)
    counts = Counter(str(row["status"]) for row in evaluated)
    output: dict[str, Any] = {
        "schema": "hydra_post_mutation_successive_halving_result_v1",
        "policy_version": POLICY_VERSION,
        "code_commit": str(code_commit),
        "engineering_task_sha256": engineering_task_sha256,
        "source_mutation_result_sha256": mutation_result_sha256,
        "source_mutation_trade_ledger_sha256": mutation_trade_ledger_sha256,
        "source_mutation_code_commit": str(result.get("code_commit") or ""),
        "development_end_exclusive": "2024-10-01",
        "candidate_count": len(evaluated),
        "evaluated_candidate_count": len(evaluated),
        "behavioral_duplicate_count": sum(
            bool(row["behaviorally_duplicate"]) for row in evaluated
        ),
        "promising_candidate_count": counts["PROMISING_RESEARCH_CANDIDATE"],
        "insufficient_evidence_count": counts["INSUFFICIENT_EVIDENCE"],
        "research_rejected_count": counts["RESEARCH_REJECTED"],
        "hard_integrity_rejected": 0,
        "status_counts": {
            status: int(counts[status]) for status in sorted(ALLOWED_OUTPUT_STATUSES)
        },
        "selected_elite_count": len(selected),
        "selected_candidate_ids": sorted(str(row["candidate_id"]) for row in selected),
        "objective_pool_counts": dict(
            Counter(str(row["objective_pool"]) for row in evaluated)
        ),
        "pool_counts": {
            pool: int(sum(str(row["objective_pool"]) == pool for row in evaluated))
            for pool in sorted(POOLS)
        },
        "selection_audit": selection_audit,
        "candidates": sorted(evaluated, key=lambda row: str(row["candidate_id"])),
        "status_ceiling": "PROMISING_RESEARCH_CANDIDATE",
        "q4_access_count": 0,
        "network_requests": 0,
        "paid_data_requests": 0,
        "order_capability": False,
        "shadow_research_active": 0,
        "paper_shadow_ready": 0,
        "funded_deployment_eligible": 0,
        "scientific_conclusion": (
            "POST_MUTATION_PROMISING_ELITES_IDENTIFIED"
            if selected
            else "POST_MUTATION_EVIDENCE_INSUFFICIENT_OR_FALSIFIED"
        ),
        "next_action": "FREEZE_SELECTED_ELITES_FOR_INDEPENDENT_ROLE_SPECIFIC_PROMOTION",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    return _write_outputs(output_dir, output, evaluated, selected)
