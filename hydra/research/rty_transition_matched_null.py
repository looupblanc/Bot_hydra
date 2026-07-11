from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_volume_front import VOLUME_FRONT_MAP_TYPE
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.causal_transition_graph import (
    build_source_state_table,
    build_transition_cache,
    build_transition_events,
)
from hydra.research.cross_asset_daily_horizon_primary import (
    _load_tables,
    _micro_execution,
    _source_features,
)
from hydra.research.equity_open_gap_reversal import _write_immutable
from hydra.research.qd_economic_tournament import _period_metrics
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "rty_transition_matched_null_v1"
PARENT_ID = "strategy_transition_RTY_to_RTY_up_expansion_long_h60_v1"
CHILD_ID = "strategy_transition_RTY_up_expansion_matched_long_h60_v2"
CALIPER = 1.25
MINIMUM_PAIRS = 24


class RTYTransitionMatchedNullError(RuntimeError):
    pass


def build_matching_covariates(rty_table: pd.DataFrame) -> pd.DataFrame:
    states = build_source_state_table(rty_table).reset_index(drop=True)
    source = _source_features(
        rty_table.sort_values("session_id").reset_index(drop=True)
    ).reset_index(drop=True)
    frame = states.copy()
    frame["source_prior_close_location"] = pd.to_numeric(
        source["source_prior_close_location"], errors="coerce"
    )
    frame["source_range_ratio"] = (
        pd.to_numeric(frame["source_prior_range"], errors="coerce")
        / pd.to_numeric(frame["source_past_range_median"], errors="coerce").replace(
            0, np.nan
        )
    )
    frame["absolute_trend_ratio"] = pd.to_numeric(
        frame["source_trend_ratio"], errors="coerce"
    ).abs()
    sessions = pd.to_datetime(frame["session_id"])
    frame["calendar_quarter"] = sessions.dt.to_period("Q").astype(str)
    frame["session_ordinal_within_quarter"] = frame.groupby(
        "calendar_quarter", sort=False
    ).cumcount()
    frame["treatment"] = frame["source_state"].eq("UP_EXPANSION")
    frame = frame[
        frame["source_state"].isin(["UP_EXPANSION", "UP_CALM"])
    ].copy()
    required = [
        "session_id",
        "source_prior_session_id",
        "source_state",
        "calendar_quarter",
        "absolute_trend_ratio",
        "source_range_ratio",
        "source_prior_close_location",
        "session_ordinal_within_quarter",
        "treatment",
    ]
    frame = frame[required].dropna().reset_index(drop=True)
    if not (
        pd.to_datetime(frame["source_prior_session_id"])
        < pd.to_datetime(frame["session_id"])
    ).all():
        raise RTYTransitionMatchedNullError("Matching state used a future session.")
    return frame


def match_covariates_only(
    covariates: pd.DataFrame, *, caliper: float = CALIPER
) -> pd.DataFrame:
    prohibited = {
        "net_pnl",
        "gross_pnl",
        "return",
        "future_return",
        "exit_price",
        "mae_dollars",
    }
    present = prohibited & set(map(str, covariates.columns))
    if present:
        raise RTYTransitionMatchedNullError(
            f"Outcome columns are prohibited during matching: {sorted(present)}"
        )
    required = {
        "session_id",
        "calendar_quarter",
        "absolute_trend_ratio",
        "source_range_ratio",
        "source_prior_close_location",
        "session_ordinal_within_quarter",
        "treatment",
    }
    missing = required - set(map(str, covariates.columns))
    if missing:
        raise RTYTransitionMatchedNullError(
            f"Missing matching covariates: {sorted(missing)}"
        )
    feature_columns = [
        "absolute_trend_ratio",
        "source_range_ratio",
        "source_prior_close_location",
        "session_ordinal_within_quarter",
    ]
    pairs: list[dict[str, Any]] = []
    for quarter, quarter_frame in covariates.groupby("calendar_quarter", sort=True):
        quarter_frame = quarter_frame.sort_values("session_id").copy()
        values = quarter_frame[feature_columns].astype(float)
        scale = values.std(ddof=0).replace(0, 1.0)
        standardized = (values - values.mean()) / scale
        standardized.index = quarter_frame.index
        treated = quarter_frame[quarter_frame["treatment"].astype(bool)]
        available = set(
            quarter_frame[~quarter_frame["treatment"].astype(bool)].index.tolist()
        )
        for treated_index, treated_row in treated.iterrows():
            if not available:
                break
            treated_values = standardized.loc[treated_index].to_numpy(dtype=float)
            ranked: list[tuple[float, str, int]] = []
            for control_index in available:
                distance = float(
                    np.linalg.norm(
                        treated_values
                        - standardized.loc[control_index].to_numpy(dtype=float)
                    )
                )
                ranked.append(
                    (
                        distance,
                        str(quarter_frame.loc[control_index, "session_id"]),
                        int(control_index),
                    )
                )
            distance, control_session, control_index = min(ranked)
            if distance > caliper:
                continue
            available.remove(control_index)
            pairs.append(
                {
                    "calendar_quarter": str(quarter),
                    "treated_session_id": str(treated_row["session_id"]),
                    "control_session_id": control_session,
                    "distance": distance,
                }
            )
    result = pd.DataFrame(pairs)
    if not result.empty and result["control_session_id"].duplicated().any():
        raise RTYTransitionMatchedNullError("A control session was reused.")
    return result


def paired_sign_flip_probability(
    differences: np.ndarray, *, seed: int = 991601, draws: int = 16_384
) -> float:
    values = np.asarray(differences, dtype=float)
    if not len(values):
        return 1.0
    observed = float(values.mean())
    if observed <= 0:
        return 1.0
    rng = np.random.default_rng(seed)
    simulated = np.empty(draws, dtype=float)
    for index in range(draws):
        signs = rng.choice(np.array([-1.0, 1.0]), size=len(values), replace=True)
        simulated[index] = float((values * signs).mean())
    return float((1 + np.count_nonzero(simulated >= observed)) / (draws + 1))


def matched_validator_controls(seed: int = 991603) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    negative = rng.normal(0.0, 1.0, size=96)
    injected = rng.normal(0.45, 1.0, size=96)
    negative_p = paired_sign_flip_probability(negative, seed=seed + 1)
    injected_p = paired_sign_flip_probability(injected, seed=seed + 2)
    return {
        "negative_control_probability": negative_p,
        "injected_weak_real_probability": injected_p,
        "negative_control_passed": negative_p > 0.05,
        "injected_control_passed": injected_p <= 0.05,
        "passed": bool(negative_p > 0.05 and injected_p <= 0.05),
    }


def run_rty_transition_matched_null(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    source_result_path: str | Path,
    source_result_sha256: str,
    source_result_hash: str,
    source_manifest_path: str | Path,
    source_manifest_sha256: str,
    source_manifest_hash: str,
    source_trade_ledger_path: str | Path,
    source_trade_ledger_sha256: str,
    core_data_path: str | Path,
    core_data_sha256: str,
    core_map_path: str | Path,
    core_map_sha256: str,
    core_roll_map_hash: str,
    metals_data_path: str | Path,
    metals_data_sha256: str,
    metals_map_path: str | Path,
    metals_map_sha256: str,
    metals_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    frozen = (
        (Path(engineering_task_path), engineering_task_sha256, "engineering task"),
        (Path(source_result_path), source_result_sha256, "source result"),
        (Path(source_manifest_path), source_manifest_sha256, "source manifest"),
        (Path(source_trade_ledger_path), source_trade_ledger_sha256, "source ledger"),
        (Path(core_data_path), core_data_sha256, "core data"),
        (Path(core_map_path), core_map_sha256, "core map"),
        (Path(metals_data_path), metals_data_sha256, "metals data"),
        (Path(metals_map_path), metals_map_sha256, "metals map"),
    )
    for path, expected, label in frozen:
        _verify(path, expected, label)
    if len(code_commit) == 40:
        current = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        if current != code_commit:
            raise RTYTransitionMatchedNullError(
                "Worker commit differs from queued specification."
            )
    source = json.loads(Path(source_result_path).read_text(encoding="utf-8"))
    manifest = json.loads(Path(source_manifest_path).read_text(encoding="utf-8"))
    parents = [
        row
        for row in source.get("candidates") or []
        if str(row.get("candidate_id") or "") == PARENT_ID
    ]
    if (
        source.get("result_hash") != source_result_hash
        or source.get("scientific_conclusion")
        != "CAUSAL_TRANSITION_GRAPH_PROMISING_BUT_INSUFFICIENT"
        or len(parents) != 1
        or parents[0].get("status") != "PROMISING_RESEARCH_CANDIDATE"
        or float((parents[0].get("null_evidence") or {}).get("adjusted_probability", 1))
        <= 0.20
        or manifest.get("elite_manifest_hash") != source_manifest_hash
    ):
        raise RTYTransitionMatchedNullError("Frozen parent evidence changed.")
    core_map = load_roll_map(core_map_path)
    metals_map = load_roll_map(metals_map_path)
    if core_map.roll_map_hash() != core_roll_map_hash:
        raise RTYTransitionMatchedNullError("Core roll map changed.")
    if (
        metals_map.map_type != VOLUME_FRONT_MAP_TYPE
        or metals_map.roll_map_hash() != metals_roll_map_hash
    ):
        raise RTYTransitionMatchedNullError("Metals roll map changed.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration: dict[str, Any] = {
        "schema": VERSION,
        "parent_candidate_id": PARENT_ID,
        "conditional_child_candidate_id": CHILD_ID,
        "source_result_hash": source_result_hash,
        "source_manifest_hash": source_manifest_hash,
        "matching": {
            "treated_state": "UP_EXPANSION",
            "control_state": "UP_CALM",
            "within_calendar_quarter": True,
            "without_replacement": True,
            "caliper": CALIPER,
            "outcome_blind": True,
        },
        "support": {
            "minimum_pairs": MINIMUM_PAIRS,
            "maximum_sign_flip_probability": 0.10,
            "minimum_supportive_quarters": 2,
            "positive_delay_effect": True,
            "positive_concentration_attacks": True,
        },
        "child_status_inherited": False,
        "child_forward_evidence_required": True,
        "q4_access_allowed": False,
        "network_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "code_commit": code_commit,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "rty_matched_null_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once() if record_data_access else None
    tables, provenance = _load_tables(
        Path(core_data_path), core_map, Path(metals_data_path), metals_map, "2024-10-01"
    )
    covariates = build_matching_covariates(tables["RTY"])
    pairs = match_covariates_only(covariates)
    cache = build_transition_cache(tables)
    base_hypothesis = {
        "candidate_id": PARENT_ID,
        "source_market": "RTY",
        "target_market": "RTY",
        "execution_market": "M2K",
        "source_state": "UP_EXPANSION",
        "side_name": "long",
        "horizon": 60,
    }
    treated_signals = build_transition_events(tables, base_hypothesis, cache=cache)
    control_hypothesis = {**base_hypothesis, "source_state": "UP_CALM"}
    control_signals = build_transition_events(
        tables, control_hypothesis, cache=cache, state_override="UP_CALM"
    )
    treated, treated_missing = _micro_execution(treated_signals, tables, base_hypothesis)
    controls, control_missing = _micro_execution(control_signals, tables, control_hypothesis)
    treated_delayed, _ = _micro_execution(
        treated_signals, tables, base_hypothesis, entry_delay_bars=1
    )
    control_delayed, _ = _micro_execution(
        control_signals, tables, control_hypothesis, entry_delay_bars=1
    )
    paired = _join_pair_outcomes(pairs, treated, controls)
    paired_delayed = _join_pair_outcomes(pairs, treated_delayed, control_delayed)
    differences = paired["paired_net_difference"].to_numpy(dtype=float)
    delayed_differences = paired_delayed["paired_net_difference"].to_numpy(dtype=float)
    probability = paired_sign_flip_probability(differences)
    quarter_effects = {
        str(quarter): float(frame["paired_net_difference"].mean())
        for quarter, frame in paired.groupby("calendar_quarter", sort=True)
    }
    supportive_quarters = sum(value > 0 for value in quarter_effects.values())
    best_pair_removed = _remove_best(paired, "paired_net_difference")
    best_month_removed = _remove_best_month(paired)
    balance = _balance_audit(covariates, pairs)
    controls_calibration = matched_validator_controls()
    confidence_interval = _bootstrap_mean_interval(differences)
    mechanism_supported = bool(
        len(paired) >= MINIMUM_PAIRS
        and float(differences.mean()) > 0
        and float(delayed_differences.mean()) > 0
        and supportive_quarters >= 2
        and best_pair_removed > 0
        and best_month_removed > 0
        and probability <= 0.10
        and controls_calibration["passed"]
        and not treated_missing
        and not control_missing
    )
    child: dict[str, Any] | None = None
    child_manifest_path: Path | None = None
    if mechanism_supported:
        specification = {
            "representation": "rty_transition_matched_state_v2",
            "candidate_id": CHILD_ID,
            "parent_candidate_id": PARENT_ID,
            "source_market": "RTY",
            "target_market": "RTY",
            "execution_market": "M2K",
            "source_state": "UP_EXPANSION",
            "side_name": "long",
            "horizon": 60,
            "mechanism_family": "causal_transition_same_market_state",
            "market_ecology": "equity_indices",
            "portfolio_role": "state_conditioned_alpha",
            "causal_rationale": "outcome_blind_up_calm_matched_counterfactual",
            "forward_evidence_required": True,
        }
        fingerprint = structural_fingerprint(specification)
        child = {
            **specification,
            "structural_fingerprint": fingerprint,
            "lineage_id": f"lineage_transition_matched_{fingerprint[:20]}",
            "status": "RESEARCH_PROTOTYPE",
            "research_classification": "RESEARCH_PROTOTYPE_FORWARD_REQUIRED",
            "candidate_status_inherited": False,
            "permits_zero_risk_shadow": False,
            "paper_shadow_ready": False,
        }
        child["child_manifest_hash"] = _stable_hash(child)
        child_manifest_path = destination / "rty_matched_child_manifest.json"
        _write_immutable(
            child_manifest_path, json.dumps(child, indent=2, sort_keys=True) + "\n"
        )
    matched_path = destination / "rty_matched_pairs.jsonl"
    _write_pairs(matched_path, paired)
    conclusion = (
        "RTY_EXPANSION_MATCHED_MECHANISM_SUPPORTED_FORWARD_CHILD_REQUIRED"
        if mechanism_supported
        else "RTY_EXPANSION_MATCHED_MECHANISM_NOT_SUPPORTED"
    )
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "The matched null diagnoses the expansion component. It does not replace "
            "the parent's failed adjusted family null, authorize shadow/orders, or "
            "create independent 2024 evidence for the conditional child."
        ),
        "code_commit": code_commit,
        "parent_candidate_id": PARENT_ID,
        "candidate_count": int(child is not None),
        "structural_prototypes": int(child is not None),
        "candidates": [child] if child else [],
        "mechanism_supported": mechanism_supported,
        "matched_pairs": int(len(paired)),
        "eligible_treated": int(covariates["treatment"].sum()),
        "eligible_controls": int((~covariates["treatment"]).sum()),
        "unmatched_treated": int(covariates["treatment"].sum() - len(paired)),
        "paired_mean_net_effect": float(differences.mean()) if len(differences) else 0.0,
        "paired_delayed_mean_net_effect": (
            float(delayed_differences.mean()) if len(delayed_differences) else 0.0
        ),
        "paired_sign_flip_probability": probability,
        "hodges_lehmann_effect": _hodges_lehmann(differences),
        "bootstrap_mean_effect_interval": confidence_interval,
        "quarter_effects": quarter_effects,
        "supportive_quarters": supportive_quarters,
        "remove_best_pair_mean_effect": best_pair_removed,
        "remove_best_month_mean_effect": best_month_removed,
        "treated_metrics": _period_metrics(treated),
        "control_metrics": _period_metrics(controls),
        "matching_balance": balance,
        "validator_controls": controls_calibration,
        "parent_disposition": (
            "RETAIN_FOR_FORWARD_ONLY_CHILD_DESIGN"
            if mechanism_supported
            else "FREEZE_EXACT_PARENT_MATCHED_NULL_UNSUPPORTED"
        ),
        "promising_candidates": 0,
        "shadow_candidates": 0,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": 0,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "child_manifest_path": str(child_manifest_path) if child_manifest_path else None,
        "matched_pairs_path": str(matched_path),
        "data_provenance": provenance,
        "data_access_record": access,
        "performance": {"total_seconds": time.perf_counter() - started},
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": (
            "COLLECT_UNTOUCHED_FORWARD_EVIDENCE_FOR_FRESH_CHILD"
            if mechanism_supported
            else "PIVOT_TARGETED_MUTATION_TO_OTHER_PROMISING_LINEAGES"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "rty_matched_null_result.json"
    report_path = destination / "rty_matched_null_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "matched_pairs_path": str(matched_path),
            "child_manifest_path": str(child_manifest_path) if child_manifest_path else None,
        },
        "report_path": str(report_path),
    }


def _join_pair_outcomes(
    pairs: pd.DataFrame, treated: pd.DataFrame, controls: pd.DataFrame
) -> pd.DataFrame:
    if pairs.empty:
        return pd.DataFrame(
            columns=["calendar_quarter", "treated_session_id", "control_session_id", "paired_net_difference"]
        )
    treated_values = treated.set_index("trading_session_id")["net_pnl"].astype(float)
    control_values = controls.set_index("trading_session_id")["net_pnl"].astype(float)
    output = pairs.copy()
    output["treated_net_pnl"] = output["treated_session_id"].map(treated_values)
    output["control_net_pnl"] = output["control_session_id"].map(control_values)
    output = output.dropna(subset=["treated_net_pnl", "control_net_pnl"]).copy()
    output["paired_net_difference"] = (
        output["treated_net_pnl"] - output["control_net_pnl"]
    )
    return output.reset_index(drop=True)


def _balance_audit(covariates: pd.DataFrame, pairs: pd.DataFrame) -> dict[str, Any]:
    features = [
        "absolute_trend_ratio",
        "source_range_ratio",
        "source_prior_close_location",
        "session_ordinal_within_quarter",
    ]
    indexed = covariates.set_index("session_id")
    treated = indexed.loc[pairs["treated_session_id"], features].astype(float)
    controls = indexed.loc[pairs["control_session_id"], features].astype(float)
    standardized_differences: dict[str, float] = {}
    for feature in features:
        pooled = float(
            np.sqrt((treated[feature].var(ddof=0) + controls[feature].var(ddof=0)) / 2)
        )
        standardized_differences[feature] = float(
            (treated[feature].mean() - controls[feature].mean()) / max(pooled, 1e-12)
        )
    return {
        "pairs": int(len(pairs)),
        "maximum_distance": float(pairs["distance"].max()) if len(pairs) else None,
        "mean_distance": float(pairs["distance"].mean()) if len(pairs) else None,
        "control_reuse_count": int(pairs["control_session_id"].duplicated().sum()) if len(pairs) else 0,
        "standardized_mean_differences": standardized_differences,
    }


def _remove_best(frame: pd.DataFrame, column: str) -> float:
    if len(frame) <= 1:
        return 0.0
    values = frame[column].astype(float)
    return float(values.drop(values.idxmax()).mean())


def _remove_best_month(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    months = pd.to_datetime(frame["treated_session_id"]).dt.to_period("M")
    monthly = frame.assign(_month=months).groupby("_month")["paired_net_difference"].sum()
    if len(monthly) <= 1:
        return 0.0
    retained = frame[months.ne(monthly.idxmax())]
    return float(retained["paired_net_difference"].mean()) if len(retained) else 0.0


def _hodges_lehmann(values: np.ndarray) -> float:
    values = np.asarray(values, dtype=float)
    if not len(values):
        return 0.0
    walsh = [
        (values[left] + values[right]) / 2
        for left in range(len(values))
        for right in range(left, len(values))
    ]
    return float(np.median(walsh))


def _bootstrap_mean_interval(values: np.ndarray, seed: int = 991607) -> list[float]:
    values = np.asarray(values, dtype=float)
    if not len(values):
        return [0.0, 0.0]
    rng = np.random.default_rng(seed)
    means = np.empty(4096, dtype=float)
    for index in range(len(means)):
        means[index] = float(rng.choice(values, size=len(values), replace=True).mean())
    return [float(value) for value in np.quantile(means, [0.025, 0.975])]


def _record_access_once() -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "outcome-blind RTY UP_EXPANSION versus UP_CALM matched null; Q4 excluded"
    module = "hydra.research.rty_transition_matched_null"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == module
                and row.get("candidate_ids") == [PARENT_ID]
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period, DataRole.DEVELOPMENT, module, [PARENT_ID], reason, None
    )
    return record.__dict__


def _write_pairs(path: Path, frame: pd.DataFrame) -> None:
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in frame.to_dict("records")
    ]
    _write_immutable(path, ("\n".join(lines) + "\n") if lines else "")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise RTYTransitionMatchedNullError(f"Frozen {label} missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# RTY Transition Matched Null",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Matched pairs: `{payload['matched_pairs']}`",
            f"- Mean paired effect: `{payload['paired_mean_net_effect']}`",
            f"- Sign-flip probability: `{payload['paired_sign_flip_probability']}`",
            f"- Supportive quarters: `{payload['supportive_quarters']}`",
            f"- Fresh child created: `{bool(payload['candidate_count'])}`",
            "- Shadow/Paper promotion: `0`",
            "- Q4 access delta: `0`",
            "- Outbound orders: `0`",
            "",
        ]
    )

