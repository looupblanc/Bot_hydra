from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.mission.calibration_retest_execution import (
    _stable_hash,
    _strict_json_value,
)
from hydra.research.equity_open_gap_reversal import MAP_TYPE, _write_immutable
from hydra.shadow.specification import ShadowSpecification


VERSION = "ym_open_gap_strict_promotion_v1"
CANDIDATE_ID = "strategy_open_gap_continuation_YM_v1"
DEVELOPMENT_END_EXCLUSIVE = pd.Timestamp("2024-10-01T00:00:00Z")
FOLDS = {
    "2024_q1": ("2024-01-01", "2024-04-01"),
    "2024_q2": ("2024-04-01", "2024-07-01"),
    "2024_q3": ("2024-07-01", "2024-10-01"),
}


class YMStrictPromotionError(RuntimeError):
    pass


def run_ym_strict_promotion(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    source_parent_result_path: str | Path,
    source_parent_result_sha256: str,
    source_parent_result_hash: str,
    source_parent_trade_ledger_path: str | Path,
    source_parent_trade_ledger_sha256: str,
    source_freeze_manifest_path: str | Path,
    source_freeze_manifest_sha256: str,
    source_freeze_manifest_hash: str,
    source_shadow_configuration_path: str | Path,
    source_shadow_configuration_sha256: str,
    source_shadow_configuration_hash: str,
    code_commit: str,
    random_seed: int = 772401,
) -> dict[str, Any]:
    sources = {
        "engineering_task": (Path(engineering_task_path), engineering_task_sha256),
        "explicit_contract_map": (Path(repaired_map_path), repaired_map_sha256),
        "parent_result": (Path(source_parent_result_path), source_parent_result_sha256),
        "parent_trade_ledger": (
            Path(source_parent_trade_ledger_path),
            source_parent_trade_ledger_sha256,
        ),
        "freeze_manifest": (Path(source_freeze_manifest_path), source_freeze_manifest_sha256),
        "shadow_configuration": (
            Path(source_shadow_configuration_path),
            source_shadow_configuration_sha256,
        ),
    }
    for label, (path, expected) in sources.items():
        _verify(path, expected, label)
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise YMStrictPromotionError("Worker commit differs from the queued specification.")

    roll_map = load_roll_map(Path(repaired_map_path))
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise YMStrictPromotionError("Explicit-contract map contract changed.")
    parent = _load_json(Path(source_parent_result_path))
    if parent.get("result_hash") != source_parent_result_hash:
        raise YMStrictPromotionError("Frozen parent semantic result hash changed.")
    candidate = next(
        (
            row
            for row in parent.get("candidates") or []
            if row.get("candidate_id") == CANDIDATE_ID
        ),
        None,
    )
    if not isinstance(candidate, dict):
        raise YMStrictPromotionError("Frozen YM candidate is absent from the parent result.")

    freeze = _load_json(Path(source_freeze_manifest_path))
    _verify_embedded_hash(freeze, "freeze_manifest_hash", source_freeze_manifest_hash)
    if (
        freeze.get("candidate_id") != CANDIDATE_ID
        or freeze.get("continuation_result_hash") != source_parent_result_hash
        or freeze.get("development_end_exclusive") != "2024-10-01"
    ):
        raise YMStrictPromotionError("Freeze manifest no longer identifies the exact parent.")
    shadow = _load_shadow_specification(Path(source_shadow_configuration_path))
    if shadow.configuration_hash != source_shadow_configuration_hash:
        raise YMStrictPromotionError("Frozen shadow configuration semantic hash changed.")

    # Freeze the exact audit policy before opening the candidate trade ledger.
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = {
        "schema": "ym_open_gap_strict_promotion_preregistration_v1",
        "candidate_id": CANDIDATE_ID,
        "task_sha256": engineering_task_sha256,
        "source_parent_result_hash": source_parent_result_hash,
        "source_freeze_manifest_hash": source_freeze_manifest_hash,
        "source_shadow_configuration_hash": source_shadow_configuration_hash,
        "data_end_exclusive": "2024-10-01",
        "folds": FOLDS,
        "bootstrap_draws": 8192,
        "bootstrap_block_size": 5,
        "matched_null_draws": 8192,
        "matched_null_strata": ["quarter", "absolute_gap_tercile"],
        "strict_confirmation_gates": {
            "positive_ym_and_mym_pooled_2024": True,
            "minimum_positive_ym_quarters": 2,
            "positive_after_remove_best_month": True,
            "maximum_bootstrap_nonpositive_probability": 0.20,
            "maximum_matched_null_probability": 0.20,
        },
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "paper_shadow_ready_allowed": False,
        "code_commit": code_commit,
        "random_seed": random_seed,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "ym_strict_promotion_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )

    ledger = _load_ledger(Path(source_parent_trade_ledger_path))
    mini = _candidate_contract_events(ledger, "YM")
    micro = _candidate_contract_events(ledger, "MYM")
    _verify_parent_projection(candidate, mini, micro)
    pooled_mini = _period(mini, "2024-01-01", "2024-10-01")
    pooled_micro = _period(micro, "2024-01-01", "2024-10-01")
    fold_results = {name: _metrics(_period(mini, *bounds)) for name, bounds in FOLDS.items()}
    micro_fold_results = {
        name: _metrics(_period(micro, *bounds)) for name, bounds in FOLDS.items()
    }
    pooled_metrics = _metrics(pooled_mini)
    pooled_micro_metrics = _metrics(pooled_micro)
    concentration = _concentration_attacks(pooled_mini)
    bootstrap = _block_bootstrap_probability(
        pooled_mini["net_pnl_60"].to_numpy(dtype=float),
        seed=random_seed,
    )
    matched_null = _matched_direction_probability(pooled_mini, seed=random_seed + 10_007)
    positive_quarters = sum(item["net_pnl"] > 0 for item in fold_results.values())
    frozen_topstep = dict(candidate.get("topstep") or {})
    frozen_neighborhood = dict(candidate.get("parameter_diagnostics") or {})
    frozen_attacks = dict(candidate.get("attacks") or {})
    hard_invalidations: list[str] = []
    if shadow.outbound_orders_enabled:
        hard_invalidations.append("outbound_orders_enabled")
    if not bool((candidate.get("shadow_evidence") or {}).get("no_lookahead")):
        hard_invalidations.append("parent_no_lookahead_not_proven")
    if not bool((candidate.get("contract_transfer") or {}).get("passed")):
        hard_invalidations.append("parent_contract_transfer_invalid")
    if not bool(frozen_topstep.get("micro_one_contract_mll_safe")):
        hard_invalidations.append("parent_one_contract_mll_unsafe")

    strict_confirmation = bool(
        not hard_invalidations
        and pooled_metrics["net_pnl"] > 0
        and pooled_micro_metrics["net_pnl"] > 0
        and positive_quarters >= 2
        and concentration["remove_best_month_net"] > 0
        and bootstrap["probability_non_positive_mean"] <= 0.20
        and matched_null["one_sided_probability"] <= 0.20
        and int(frozen_neighborhood.get("positive_neighbor_count", 0)) >= 1
    )
    shadow_activation_eligible = bool(
        not hard_invalidations
        and bool((candidate.get("admission") or {}).get("permits_zero_risk_shadow"))
        and not shadow.outbound_orders_enabled
    )
    if hard_invalidations:
        conclusion = "YM_STRICT_PROMOTION_HARD_INVALIDATION"
    elif strict_confirmation:
        conclusion = "YM_STRICT_PROMOTION_DEVELOPMENT_CONFIRMED_Q4_STILL_REQUIRED"
    else:
        conclusion = "YM_STRICT_PROMOTION_INSUFFICIENT_TEMPORAL_CONFIRMATION_SHADOW_SAFE"

    audited_candidate = {
        **candidate,
        "status": candidate.get("status"),
        "source_experiment": "equity_open_gap_continuation_pilot_v1",
        "strict_promotion": {
            "confirmed": strict_confirmation,
            "shadow_activation_eligible": shadow_activation_eligible,
            "positive_2024_quarters": positive_quarters,
            "hard_invalidations": hard_invalidations,
        },
    }
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "The immutable parent was audited, not modified. Q1-Q3 2024 remains development "
            "evidence, Q4 was not read, strict confirmation is not PAPER_SHADOW_READY, and "
            "zero-order shadow eligibility is a safety/implementation status rather than proof of edge."
        ),
        "candidate_id": CANDIDATE_ID,
        "candidate_count": 0,
        "candidate_status_preserved": candidate.get("status"),
        "candidates": [audited_candidate],
        "strict_development_confirmation": strict_confirmation,
        "shadow_activation_eligible": shadow_activation_eligible,
        "hard_invalidations": hard_invalidations,
        "fold_results": fold_results,
        "micro_fold_results": micro_fold_results,
        "pooled_2024": pooled_metrics,
        "pooled_2024_micro": pooled_micro_metrics,
        "positive_2024_quarters": positive_quarters,
        "concentration_attacks": concentration,
        "block_bootstrap": bootstrap,
        "matched_candidate_null": matched_null,
        "existing_candidate_null": candidate.get("null_evidence"),
        "delayed_entry_audit": {
            "source": "verified_frozen_parent_candidate_attack",
            "one_bar_delay_net": frozen_attacks.get("one_bar_delay_net"),
            "positive": float(frozen_attacks.get("one_bar_delay_net", 0.0)) > 0,
        },
        "parameter_neighborhood": frozen_neighborhood,
        "contract_transfer": candidate.get("contract_transfer"),
        "topstep": frozen_topstep,
        "event_attribution": _event_attribution(pooled_mini),
        "source_integrity": {
            label: {"path": str(path), "sha256": expected}
            for label, (path, expected) in sources.items()
        },
        "shadow_configuration": {
            "path": str(source_shadow_configuration_path),
            "configuration_hash": shadow.configuration_hash,
            "outbound_orders_enabled": shadow.outbound_orders_enabled,
        },
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "market_data_rows_read": 0,
            "source_ledger_rows_read": int(len(ledger)),
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "paper_shadow_ready": 0,
        "shadow_candidates": int(candidate.get("status") == "SHADOW_RESEARCH_CANDIDATE"),
        "promising_candidates": 1,
        "topstep_path_candidates": int(bool(frozen_topstep.get("path_candidate"))),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "code_commit": code_commit,
        "next_recommended_action": (
            "ACTIVATE_IMMUTABLE_ZERO_ORDER_YM_SHADOW_AND_RESEARCH_VERSIONED_CHILDREN"
            if shadow_activation_eligible
            else "REJECT_YM_SHADOW_AND_RESOLVE_HARD_INVALIDATION"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "ym_strict_promotion_result.json"
    report_path = destination / "ym_strict_promotion_report.md"
    ledger_path = destination / "ym_strict_promotion_candidate_ledger.jsonl"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    _write_candidate_ledger(ledger_path, pd.concat([mini, micro], ignore_index=True))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "candidate_ledger_path": str(ledger_path),
        },
        "report_path": str(report_path),
    }


def _candidate_contract_events(ledger: pd.DataFrame, symbol: str) -> pd.DataFrame:
    events = ledger.loc[ledger["symbol"].eq(symbol)].copy()
    events = events.sort_values("decision_timestamp").reset_index(drop=True)
    if events.empty or events["active_contract"].astype(str).str.contains(r"\.c\.").any():
        raise YMStrictPromotionError(f"Explicit {symbol} event ledger is absent or invalid.")
    return events


def _verify_parent_projection(
    candidate: dict[str, Any], mini: pd.DataFrame, micro: pd.DataFrame
) -> None:
    expected = (
        (len(mini), int(candidate["events"])),
        (len(micro), int(candidate["micro_events"])),
    )
    if any(actual != frozen for actual, frozen in expected):
        raise YMStrictPromotionError("Candidate event counts do not reproduce the parent.")
    if not np.isclose(mini["net_pnl_60"].sum(), float(candidate["net_pnl"])):
        raise YMStrictPromotionError("YM net PnL does not reproduce the parent.")
    if not np.isclose(micro["net_pnl_60"].sum(), float(candidate["micro_net_pnl"])):
        raise YMStrictPromotionError("MYM net PnL does not reproduce the parent.")


def _metrics(events: pd.DataFrame) -> dict[str, Any]:
    values = events["net_pnl_60"].to_numpy(dtype=float)
    return {
        "events": int(len(events)),
        "gross_pnl": float(events["gross_pnl_60"].sum()),
        "costs": float(events["cost"].sum()),
        "net_pnl": float(values.sum()),
        "mean_net_pnl": float(values.mean()) if len(values) else 0.0,
        "win_rate": float(np.mean(values > 0)) if len(values) else 0.0,
        "maximum_drawdown": _maximum_drawdown(values),
    }


def _period(events: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    timestamps = pd.to_datetime(events["decision_timestamp"], utc=True)
    return events.loc[
        (timestamps >= pd.Timestamp(start, tz="UTC"))
        & (timestamps < pd.Timestamp(end, tz="UTC"))
    ].copy()


def _concentration_attacks(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "best_trade_net": 0.0,
            "remove_best_trade_net": 0.0,
            "best_day": None,
            "best_day_net": 0.0,
            "remove_best_day_net": 0.0,
            "best_month": None,
            "best_month_net": 0.0,
            "remove_best_month_net": 0.0,
        }
    total = float(events["net_pnl_60"].sum())
    best_trade = float(events["net_pnl_60"].max())
    daily = events.groupby("event_session_id", sort=True)["net_pnl_60"].sum()
    month_keys = pd.to_datetime(events["decision_timestamp"], utc=True).dt.strftime("%Y-%m")
    monthly = events.assign(_month=month_keys).groupby("_month", sort=True)["net_pnl_60"].sum()
    best_day, best_day_net = str(daily.idxmax()), float(daily.max())
    best_month, best_month_net = str(monthly.idxmax()), float(monthly.max())
    return {
        "best_trade_net": best_trade,
        "remove_best_trade_net": total - best_trade,
        "best_day": best_day,
        "best_day_net": best_day_net,
        "remove_best_day_net": total - best_day_net,
        "best_month": best_month,
        "best_month_net": best_month_net,
        "remove_best_month_net": total - best_month_net,
    }


def _block_bootstrap_probability(
    values: np.ndarray,
    *,
    seed: int,
    draws: int = 8192,
    block_size: int = 5,
) -> dict[str, Any]:
    observations = np.asarray(values, dtype=float)
    if not len(observations):
        return {"draws": draws, "block_size": block_size, "probability_non_positive_mean": 1.0}
    starts = np.arange(max(1, len(observations) - block_size + 1))
    blocks = [observations[start : start + block_size] for start in starts]
    rng = np.random.default_rng(seed)
    means = np.empty(draws, dtype=float)
    needed = int(np.ceil(len(observations) / block_size))
    for draw in range(draws):
        chosen = rng.integers(0, len(blocks), size=needed)
        sample = np.concatenate([blocks[index] for index in chosen])[: len(observations)]
        means[draw] = sample.mean()
    return {
        "draws": draws,
        "block_size": block_size,
        "probability_non_positive_mean": float((np.count_nonzero(means <= 0) + 1) / (draws + 1)),
        "mean_ci_05": float(np.quantile(means, 0.05)),
        "mean_ci_95": float(np.quantile(means, 0.95)),
    }


def _matched_direction_probability(
    events: pd.DataFrame, *, seed: int, draws: int = 8192
) -> dict[str, Any]:
    if events.empty:
        return {"draws": draws, "one_sided_probability": 1.0, "strata": 0}
    work = events.copy().reset_index(drop=True)
    timestamps = pd.to_datetime(work["decision_timestamp"], utc=True)
    work["_quarter"] = (
        timestamps.dt.year.astype(str) + "Q" + timestamps.dt.quarter.astype(str)
    )
    ranks = work["gap_points"].abs().rank(method="first")
    work["_gap_bin"] = pd.qcut(ranks, q=min(3, len(work)), labels=False, duplicates="drop")
    strata = [indices.to_numpy(dtype=int) for _, indices in work.groupby(["_quarter", "_gap_bin"], sort=True).groups.items()]
    side = work["side"].to_numpy(dtype=int)
    gross_directionless = work["gross_pnl_60"].to_numpy(dtype=float) / side
    costs = work["cost"].to_numpy(dtype=float)
    observed = float(np.sum(side * gross_directionless - costs))
    rng = np.random.default_rng(seed)
    exceedances = 0
    for _ in range(draws):
        permuted = side.copy()
        for indices in strata:
            permuted[indices] = rng.permutation(permuted[indices])
        null_net = float(np.sum(permuted * gross_directionless - costs))
        exceedances += int(null_net >= observed)
    return {
        "method": "direction_permutation_within_quarter_and_absolute_gap_tercile",
        "draws": draws,
        "strata": len(strata),
        "observed_net": observed,
        "one_sided_probability": float((exceedances + 1) / (draws + 1)),
    }


def _event_attribution(events: pd.DataFrame) -> dict[str, Any]:
    timestamps = pd.to_datetime(events["decision_timestamp"], utc=True)
    work = events.assign(_month=timestamps.dt.strftime("%Y-%m"))
    by_month = work.groupby("_month", sort=True)["net_pnl_60"].agg(["count", "sum"])
    by_side = work.groupby("side", sort=True)["net_pnl_60"].agg(["count", "sum"])
    by_contract = work.groupby("active_contract", sort=True)["net_pnl_60"].agg(["count", "sum"])
    return {
        "by_month": {str(key): {"events": int(row["count"]), "net_pnl": float(row["sum"])} for key, row in by_month.iterrows()},
        "by_side": {str(key): {"events": int(row["count"]), "net_pnl": float(row["sum"])} for key, row in by_side.iterrows()},
        "by_contract": {str(key): {"events": int(row["count"]), "net_pnl": float(row["sum"])} for key, row in by_contract.iterrows()},
    }


def _maximum_drawdown(values: np.ndarray) -> float:
    if not len(values):
        return 0.0
    equity = np.concatenate(([0.0], np.cumsum(values)))
    return float(np.max(np.maximum.accumulate(equity) - equity))


def _load_ledger(path: Path) -> pd.DataFrame:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    frame = pd.DataFrame(rows)
    required = {
        "symbol", "active_contract", "decision_timestamp", "event_session_id",
        "gap_points", "side", "gross_pnl_60", "net_pnl_60", "cost",
    }
    if not required.issubset(frame.columns):
        raise YMStrictPromotionError("Frozen parent ledger schema is incomplete.")
    timestamps = pd.to_datetime(frame["decision_timestamp"], utc=True)
    if (timestamps >= DEVELOPMENT_END_EXCLUSIVE).any():
        raise YMStrictPromotionError("Q4 or later observation found in the parent ledger.")
    if frame.duplicated(["symbol", "active_contract", "decision_timestamp"]).any():
        raise YMStrictPromotionError("Duplicate parent ledger event detected.")
    frame["decision_timestamp"] = timestamps
    return frame


def _load_shadow_specification(path: Path) -> ShadowSpecification:
    payload = _load_json(path)
    supplied_hash = payload.pop("configuration_hash", None)
    for key in ("feature_versions", "markets", "timeframes", "kill_conditions"):
        payload[key] = tuple(payload[key])
    specification = ShadowSpecification(**payload)
    specification.validate()
    if supplied_hash != specification.configuration_hash:
        raise YMStrictPromotionError("Shadow configuration file hash does not recompute.")
    return specification


def _verify_embedded_hash(payload: dict[str, Any], key: str, expected: str) -> None:
    if payload.get(key) != expected:
        raise YMStrictPromotionError(f"Embedded {key} changed.")
    unhashed = dict(payload)
    unhashed.pop(key, None)
    if _stable_hash(unhashed) != expected:
        raise YMStrictPromotionError(f"Recomputed {key} changed.")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise YMStrictPromotionError(f"Frozen {label} is missing or changed: {path}")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_candidate_ledger(path: Path, events: pd.DataFrame) -> None:
    rows = []
    for row in events.sort_values(["decision_timestamp", "symbol"]).to_dict("records"):
        rows.append(json.dumps(_strict_json_value(row), sort_keys=True, default=str))
    _write_immutable(path, "\n".join(rows) + ("\n" if rows else ""))


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Frozen YM Strict Promotion Replay",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Strict development confirmation: `{payload['strict_development_confirmation']}`",
            f"- Shadow activation eligible: `{payload['shadow_activation_eligible']}`",
            f"- Positive 2024 quarters: `{payload['positive_2024_quarters']}/3`",
            f"- Pooled YM 2024 net: `{payload['pooled_2024']['net_pnl']:.2f}`",
            f"- Pooled MYM 2024 net: `{payload['pooled_2024_micro']['net_pnl']:.2f}`",
            f"- Remove-best-month YM net: `{payload['concentration_attacks']['remove_best_month_net']:.2f}`",
            f"- Block-bootstrap P(mean <= 0): `{payload['block_bootstrap']['probability_non_positive_mean']:.6f}`",
            f"- Matched-null probability: `{payload['matched_candidate_null']['one_sided_probability']:.6f}`",
            f"- PAPER_SHADOW_READY: `{payload['paper_shadow_ready']}`",
            "- Q4 access: `0`",
            "- Outbound orders: `0`",
            "",
            "## Interpretation boundary",
            "",
            str(payload["interpretation_boundary"]),
            "",
        ]
    )
