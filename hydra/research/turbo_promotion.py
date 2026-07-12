from __future__ import annotations

import hashlib
import json
import math
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.propfirm.topstep_150k import Topstep150KConfig, simulate_combine
from hydra.propfirm.xfa_consistency import simulate_xfa_consistency
from hydra.propfirm.xfa_standard import simulate_xfa_standard
from hydra.research.qd_economic_tournament import MARKET_PAIRS, _benjamini_hochberg, _round_turn_cost_all
from hydra.research.turbo_exact_replay import exact_replay, spec_from_dict
from hydra.research.turbo_feature_builder import FEATURE_BUNDLE_VERSION
from hydra.strategies.turbo_dsl import StrategyRole


VERSION = "hydra_turbo_promotion_batch_v1"
NULL_DRAWS = 2_048


class TurboPromotionError(RuntimeError):
    pass


def run_turbo_promotion_batch(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    source_result_path: str | Path,
    source_result_sha256: str,
    source_result_hash: str,
    exact_results_path: str | Path,
    exact_results_sha256: str,
    code_commit: str,
    random_seed: int = 20260713,
) -> dict[str, Any]:
    task = Path(engineering_task_path)
    source_path = Path(source_result_path)
    exact_path = Path(exact_results_path)
    _verify(task, engineering_task_sha256, "engineering task")
    _verify(source_path, source_result_sha256, "Turbo source result")
    _verify(exact_path, exact_results_sha256, "Turbo exact replay ledger")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise TurboPromotionError("Promotion worker commit differs from queued specification.")
    source = json.loads(source_path.read_text(encoding="utf-8"))
    if source.get("result_hash") != source_result_hash:
        raise TurboPromotionError("Turbo source semantic hash changed.")
    if int(((source.get("governance") or {}).get("q4_access_count_delta") or 0)) != 0:
        raise TurboPromotionError("Promotion source crossed Q4 governance boundary.")
    exact_rows = {
        str(row["candidate_id"]): row
        for row in (
            json.loads(line)
            for line in exact_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    }
    candidate_rows = {
        str(row["candidate_id"]): row for row in source.get("candidates") or []
    }
    selected_ids = [str(value) for value in source.get("promotion_candidate_ids") or []]
    if not selected_ids:
        raise TurboPromotionError("Promotion batch contains no frozen candidate IDs.")
    missing = [value for value in selected_ids if value not in candidate_rows or value not in exact_rows]
    if missing:
        raise TurboPromotionError(f"Promotion evidence is incomplete: {missing[:3]}")
    raw_probabilities: list[float] = []
    evaluations: list[dict[str, Any]] = []
    for index, candidate_id in enumerate(selected_ids):
        candidate = candidate_rows[candidate_id]
        exact = exact_rows[candidate_id]
        spec = spec_from_dict(candidate["specification"])
        matrix = _feature_matrix(spec.market, source)
        gross = np.asarray(exact.get("event_gross_pnl") or [], dtype=float)
        session_days = np.asarray(exact.get("event_session_days") or [], dtype=np.int64)
        raw_probability = _session_block_sign_flip_probability(
            gross,
            session_days,
            cost=spec.round_turn_cost * spec.quantity,
            seed=random_seed + index * 1009,
        )
        raw_probabilities.append(raw_probability)
        neighbors = []
        for multiplier in (0.95, 1.05):
            neighbor = exact_replay(
                replace(
                    spec,
                    candidate_id=f"{candidate_id}__diagnostic_q{multiplier:.2f}",
                    threshold=float(spec.threshold) * multiplier,
                ),
                matrix,
            )
            neighbors.append(
                {
                    "threshold_multiplier": multiplier,
                    "events": int(neighbor["events"]),
                    "net_pnl": float(neighbor["net_pnl"]),
                }
            )
        micro_symbol = MARKET_PAIRS[spec.market]
        micro_spec = replace(
            spec,
            candidate_id=f"{candidate_id}__micro_transfer",
            market=micro_symbol,
            point_value=instrument_spec(micro_symbol).point_value,
            round_turn_cost=_round_turn_cost_all(micro_symbol),
            quantity=1,
        )
        micro = exact_replay(micro_spec, matrix)
        topstep = _role_specific_topstep(micro, spec.role)
        evaluations.append(
            {
                "candidate": candidate,
                "specification": candidate["specification"],
                "primary_exact": exact,
                "raw_null_probability": raw_probability,
                "parameter_neighbors": neighbors,
                "micro_transfer": micro,
                "topstep": topstep,
            }
        )
    adjusted = _benjamini_hochberg(raw_probabilities)
    candidates: list[dict[str, Any]] = []
    for index, row in enumerate(evaluations):
        candidate = dict(row["candidate"])
        exact = row["primary_exact"]
        micro = row["micro_transfer"]
        role = StrategyRole(int(row["specification"]["role"]))
        null_pass = bool(adjusted[index] <= 0.20)
        neighbor_pass = sum(float(value["net_pnl"]) > 0 for value in row["parameter_neighbors"]) >= 1
        micro_pass = bool(
            int(micro["events"]) >= 15
            and float(micro["net_pnl"]) > 0
            and float(micro["cost_stress_1_5x_net"]) > 0
            and int(micro["supportive_temporal_folds"]) >= 1
            and not bool(micro["catastrophic_transfer"])
        )
        delay_pass = float(exact["one_bar_delay_net_pnl"]) > 0
        role_evidence = role not in {StrategyRole.DEFENSIVE, StrategyRole.PORTFOLIO_ONLY}
        robust = bool(null_pass and neighbor_pass and micro_pass and delay_pass and role_evidence)
        shadow_candidate = bool(
            robust
            and not bool(exact["hard_invalidation"])
            and bool(exact["mll_proxy_safe"])
        )
        status = (
            "SHADOW_RESEARCH_CANDIDATE"
            if shadow_candidate
            else "ROBUST_RESEARCH_CANDIDATE"
            if robust
            else "PROMISING_RESEARCH_CANDIDATE"
        )
        if not role_evidence:
            status = "PROMISING_RESEARCH_CANDIDATE"
        candidates.append(
            {
                **candidate,
                "status": status,
                "candidate_null": {
                    "method": "session_block_sign_flip_2048_draws_bh_frozen_batch",
                    "raw_probability": raw_probabilities[index],
                    "family_adjusted_probability": float(adjusted[index]),
                    "passed_shadow_calibrated_threshold": null_pass,
                },
                "parameter_neighborhood": {
                    "diagnostic_only": True,
                    "neighbors": row["parameter_neighbors"],
                    "passed": neighbor_pass,
                },
                "contract_transfer": {
                    "primary": candidate["primary_market"],
                    "micro": candidate["execution_market"],
                    "events": int(micro["events"]),
                    "net_pnl": float(micro["net_pnl"]),
                    "passed": micro_pass,
                },
                "delay_resilience": {
                    "one_bar_delay_net_pnl": float(exact["one_bar_delay_net_pnl"]),
                    "passed": delay_pass,
                },
                "topstep": row["topstep"],
                "role_specific_evidence": {
                    "role": role.name,
                    "direct_alpha_criteria_applicable": role_evidence,
                    "portfolio_matched_control_required": not role_evidence,
                },
                "paper_shadow_ready": False,
                "q4_access_count": 0,
                "evidence_boundary": "development_promotion_q4_unopened_no_status_inheritance",
            }
        )
    shadow = sum(row["status"] == "SHADOW_RESEARCH_CANDIDATE" for row in candidates)
    robust_count = sum(
        row["status"]
        in {"ROBUST_RESEARCH_CANDIDATE", "SHADOW_RESEARCH_CANDIDATE"}
        for row in candidates
    )
    topstep_count = sum(bool((row.get("topstep") or {}).get("path_candidate")) for row in candidates)
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": (
            "TURBO_PROMOTION_SHADOW_RESEARCH_CANDIDATES_FOUND"
            if shadow
            else "TURBO_PROMOTION_COMPLETED_INSUFFICIENT_FOR_SHADOW"
        ),
        "source_batch_index": int(source.get("batch_index") or 0),
        "candidate_count": len(candidates),
        "promising_candidates": len(candidates),
        "robust_research_candidates": robust_count,
        "shadow_candidates": shadow,
        "topstep_path_candidates": topstep_count,
        "paper_shadow_ready": 0,
        "candidates": candidates,
        "null_draws": NULL_DRAWS,
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "outbound_order_capability": False,
        },
        "integrity": {
            "source_hash_verified": True,
            "candidate_level_nulls": True,
            "no_status_inheritance": True,
            "q4_excluded": True,
            "no_outbound_order_capability": True,
        },
        "next_recommended_action": (
            "PACKAGE_IMMUTABLE_ZERO_ORDER_SHADOW_RESEARCH"
            if shadow
            else "CONTINUE_TURBO_DISCOVERY_AND_ROLE_SPECIFIC_RESEARCH"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "turbo_promotion_result.json"
    report_path = destination / "turbo_promotion_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _report(payload))
    return {
        **payload,
        "artifacts": {"result_path": str(result_path), "report_path": str(report_path)},
        "report_path": str(report_path),
    }


def _session_block_sign_flip_probability(
    gross: np.ndarray,
    session_days: np.ndarray,
    *,
    cost: float,
    seed: int,
) -> float:
    if len(gross) < 10 or len(gross) != len(session_days):
        return 1.0
    unique, blocks = np.unique(session_days, return_inverse=True)
    rng = np.random.default_rng(seed)
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(NULL_DRAWS, len(unique)))
    null_net = (signs[:, blocks] * gross).sum(axis=1) - cost * len(gross)
    observed = float(gross.sum() - cost * len(gross))
    return float((1 + np.count_nonzero(null_net >= observed)) / (NULL_DRAWS + 1))


def _role_specific_topstep(micro: Mapping[str, Any], role: StrategyRole) -> dict[str, Any]:
    net = np.asarray(micro.get("event_net_pnl") or [], dtype=float)
    days = np.asarray(micro.get("event_session_days") or [], dtype=np.int64)
    if not len(net):
        return {"path_candidate": False, "reason": "no_micro_events"}
    rows = []
    config = Topstep150KConfig()
    for quantity in (1, 3, 6, 10, 15):
        daily = _daily_frame(net * quantity, days)
        combine = simulate_combine(daily, config)
        standard = simulate_xfa_standard(daily, mll_distance=config.combine_max_loss_limit)
        consistency = simulate_xfa_consistency(daily, mll_distance=config.combine_max_loss_limit)
        xfa_score = max(
            (
                int(standard.get("payout_cycles_survived", 0)),
                int(consistency.get("payout_cycles_survived", 0)),
            )
        )
        rows.append(
            {
                "micro_contracts": quantity,
                "combine": combine,
                "xfa_standard": standard,
                "xfa_consistency": consistency,
                "combine_path": bool(
                    combine["passed"]
                    and not combine["mll_breached"]
                    and combine["consistency_ok"]
                ),
                "xfa_score": xfa_score,
            }
        )
    if role == StrategyRole.COMBINE_PASSER:
        selected = max(
            rows,
            key=lambda row: (
                int(row["combine_path"]),
                float(row["combine"]["min_mll_buffer"]),
                -int(row["combine"]["days_to_pass"] or 10_000),
            ),
        )
        role_utility = "COMBINE_UTILITY"
    else:
        selected = max(
            rows,
            key=lambda row: (
                int(row["xfa_score"]),
                int(row["xfa_standard"].get("survived", False)),
                float(row["combine"]["min_mll_buffer"]),
            ),
        )
        role_utility = "XFA_UTILITY"
    return {
        "rule_version": "topstep_150k_2026-07-10_no_dll_baseline",
        "role_utility": role_utility,
        "selected_micro_contracts": selected["micro_contracts"],
        "combine": selected["combine"],
        "xfa_standard": selected["xfa_standard"],
        "xfa_consistency": selected["xfa_consistency"],
        "path_candidate": bool(selected["combine_path"]),
        "quantity_sensitivity": rows,
        "shared_account_portfolio_replay_required": True,
    }


def _daily_frame(net: np.ndarray, days: np.ndarray) -> pd.DataFrame:
    frame = pd.DataFrame({"day": days, "net": net})
    daily = frame.groupby("day", sort=True)["net"].agg(["sum", "min", "size"]).reset_index()
    return pd.DataFrame(
        {
            "date": [str(np.datetime64(int(value), "D")) for value in daily["day"]],
            "pnl": daily["sum"].astype(float),
            "raw_pnl": daily["sum"].astype(float),
            "worst_intraday_pnl": np.minimum(daily["min"].astype(float), 0.0),
            "trades": daily["size"].astype(int),
            "skipped_trades": 0,
            "hit_daily_stop": False,
            "hit_daily_profit_lock": False,
        }
    )


def _feature_matrix(market: str, source: Mapping[str, Any]) -> FeatureMatrix:
    fingerprint = str((source.get("feature_store") or {}).get("source_fingerprint") or "")
    root = Path("/root/hydra-bot/data/cache/turbo_foundry_v2")
    matches: list[Path] = []
    for path in root.glob("*/manifest.json"):
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        key = manifest.get("key") or {}
        if (
            key.get("market") == market
            and key.get("transformation_version") == FEATURE_BUNDLE_VERSION
            and key.get("source_data_sha256") == fingerprint
        ):
            matches.append(path.parent)
    if len(matches) != 1:
        raise TurboPromotionError(
            f"Expected one verified feature bundle for {market}, found {len(matches)}."
        )
    return FeatureMatrix.open(matches[0], mmap=True)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _sha256(path) != expected:
        raise TurboPromotionError(f"Frozen {label} is missing or changed: {path}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_immutable(path: Path, text: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise TurboPromotionError(f"Refusing to overwrite immutable artifact: {path}")
        return
    path.write_text(text, encoding="utf-8")


def _report(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# HYDRA Turbo Promotion",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Candidates: `{payload['candidate_count']}`",
            f"- Robust / shadow: `{payload['robust_research_candidates']}` / `{payload['shadow_candidates']}`",
            f"- Topstep path: `{payload['topstep_path_candidates']}`",
            "- PAPER_SHADOW_READY: `0`",
            "- Q4 access: `0`",
            "- Outbound orders: `0`",
            "",
        ]
    )
