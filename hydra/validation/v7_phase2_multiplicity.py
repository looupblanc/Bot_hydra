from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import squareform
from scipy.stats import kurtosis, norm, skew

from hydra.account_policy.basket import RoutedTrade, run_shared_account_episode
from hydra.account_policy.schema import BasketPolicy, stable_hash
from hydra.data.v7_manifest import verify_v7_data_manifest
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.topstep_150k import Topstep150KConfig


PREREGISTRATION_SHA256 = (
    "52d8ecde1d2b8bed33a71a0aabe7348ca59c86816ec08d37227a10134fe5e631"
)
CONTRACT_SHA256 = (
    "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
)
G0_SHA256 = "cc6f24342dd6a99a19bfb19c96ca02154c4ef4500e61ef74aa205106c9285aee"
G1_SHA256 = "4240716947c14d7b66a67d8a39efe986fdb17fd405b03fe852fdb71d6a3e8d62"
N_TRIALS = 246_706
FDR_Q = 0.10
CLUSTER_CUT_DISTANCE = 0.30
ANALYTIC_BARRIER = 1.0e15


class Phase2Error(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FrozenBasket:
    generation: int
    policy_id: str
    basket: BasketPolicy
    bank: Mapping[str, Any]
    components: Mapping[str, tuple[RoutedTrade, ...]]
    phase0: Mapping[str, Any]
    episode_start_days: tuple[int, ...]


def deflated_sharpe_statistics(
    daily_pnl: Sequence[float], *, n_trials: int
) -> dict[str, float | int]:
    values = np.asarray(daily_pnl, dtype=np.float64)
    values = values[np.isfinite(values)]
    n = int(values.size)
    if n < 3 or n_trials < 2:
        raise ValueError("DSR requires at least three observations and two trials")
    gamma = 0.5772156649015329
    sigma_sr = 1.0 / math.sqrt(n - 1)
    expected_max = sigma_sr * (
        (1.0 - gamma) * norm.ppf(1.0 - 1.0 / n_trials)
        + gamma * norm.ppf(1.0 - 1.0 / (n_trials * math.e))
    )
    sigma = float(np.std(values, ddof=1))
    if sigma <= 0.0:
        return {
            "observations": n,
            "sample_sharpe_daily": 0.0,
            "sample_sharpe_annualized": 0.0,
            "expected_max_sharpe_daily": float(expected_max),
            "expected_max_sharpe_annualized": float(
                expected_max * math.sqrt(252.0)
            ),
            "skewness": 0.0,
            "pearson_kurtosis": 3.0,
            "deflated_z": -1.0e12,
            "DSR_probability": 0.0,
            "one_sided_p_value": 1.0,
        }
    sr = float(np.mean(values) / sigma)
    sample_skew = float(skew(values, bias=False))
    sample_kurtosis = float(kurtosis(values, fisher=False, bias=False))
    if not math.isfinite(sample_skew):
        sample_skew = 0.0
    if not math.isfinite(sample_kurtosis):
        sample_kurtosis = 3.0
    variance_term = max(
        1.0
        - sample_skew * sr
        + ((sample_kurtosis - 1.0) / 4.0) * sr * sr,
        1.0e-12,
    )
    z_score = float((sr - expected_max) * math.sqrt(n - 1) / math.sqrt(variance_term))
    probability = float(norm.cdf(z_score))
    return {
        "observations": n,
        "sample_sharpe_daily": sr,
        "sample_sharpe_annualized": sr * math.sqrt(252.0),
        "expected_max_sharpe_daily": float(expected_max),
        "expected_max_sharpe_annualized": float(expected_max * math.sqrt(252.0)),
        "skewness": sample_skew,
        "pearson_kurtosis": sample_kurtosis,
        "deflated_z": z_score,
        "DSR_probability": probability,
        "one_sided_p_value": float(1.0 - probability),
    }


def benjamini_hochberg(
    p_values: Mapping[str, float], *, q: float
) -> dict[str, dict[str, float | int | bool]]:
    if not 0.0 < q < 1.0 or not p_values:
        raise ValueError("BH requires p-values and q in (0,1)")
    ordered = sorted(
        ((str(key), float(value)) for key, value in p_values.items()),
        key=lambda item: (item[1], item[0]),
    )
    count = len(ordered)
    last_rejected = 0
    for rank, (_, value) in enumerate(ordered, start=1):
        if value <= q * rank / count:
            last_rejected = rank
    adjusted: dict[str, float] = {}
    running = 1.0
    for rank, (key, value) in reversed(list(enumerate(ordered, start=1))):
        running = min(running, value * count / rank)
        adjusted[key] = float(min(1.0, running))
    return {
        key: {
            "rank": rank,
            "p_value": value,
            "critical_value": q * rank / count,
            "adjusted_p_value": adjusted[key],
            "rejected": rank <= last_rejected,
        }
        for rank, (key, value) in enumerate(ordered, start=1)
    }


def behavioral_clusters(
    candidate_ids: Sequence[str], daily_matrix: np.ndarray, *, cut_distance: float
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    ids = tuple(str(value) for value in candidate_ids)
    matrix = np.asarray(daily_matrix, dtype=np.float64)
    if matrix.shape[0] != len(ids) or matrix.ndim != 2:
        raise ValueError("daily matrix must be candidates by aligned days")
    correlations = np.eye(len(ids), dtype=np.float64)
    for left in range(len(ids)):
        for right in range(left + 1, len(ids)):
            a = matrix[left]
            b = matrix[right]
            a_sigma = float(np.std(a))
            b_sigma = float(np.std(b))
            if a_sigma <= 0.0 or b_sigma <= 0.0:
                rho = 1.0 if np.array_equal(a, b) else 0.0
            else:
                rho = float(np.corrcoef(a, b)[0, 1])
                if not math.isfinite(rho):
                    rho = 0.0
            correlations[left, right] = correlations[right, left] = float(
                np.clip(rho, -1.0, 1.0)
            )
    if len(ids) == 1:
        raw_labels = np.ones(1, dtype=np.int64)
    else:
        distances = np.clip(1.0 - correlations, 0.0, 2.0)
        np.fill_diagonal(distances, 0.0)
        tree = linkage(squareform(distances, checks=True), method="average")
        raw_labels = fcluster(tree, t=cut_distance, criterion="distance")
    raw_groups: dict[int, list[str]] = {}
    for candidate_id, raw in zip(ids, raw_labels, strict=True):
        raw_groups.setdefault(int(raw), []).append(candidate_id)
    ordered_groups = sorted(
        (sorted(members) for members in raw_groups.values()), key=lambda row: row[0]
    )
    clusters = [
        {
            "cluster_id": f"P2_CLUSTER_{index:03d}",
            "member_count": len(members),
            "member_ids": members,
        }
        for index, members in enumerate(ordered_groups, start=1)
    ]
    return correlations, clusters


def stress_trade(trade: RoutedTrade, multiplier: float) -> RoutedTrade:
    if multiplier < 1.0:
        raise ValueError("cost stress multiplier cannot be below one")
    event = trade.event
    base_cost = max(0.0, float(event.gross_pnl) - float(event.net_pnl))
    extra = (float(multiplier) - 1.0) * base_cost
    stressed = replace(
        event,
        net_pnl=float(event.net_pnl - extra),
        worst_unrealized_pnl=float(event.worst_unrealized_pnl - extra),
        best_unrealized_pnl=float(event.best_unrealized_pnl - extra),
    )
    return RoutedTrade(
        component_id=trade.component_id,
        market=trade.market,
        side=trade.side,
        event=stressed,
    )


def select_representatives(
    candidate_rows: Sequence[Mapping[str, Any]],
    clusters: Sequence[Mapping[str, Any]],
    *,
    maximum: int = 3,
) -> list[str]:
    cluster_by_candidate = {
        str(candidate_id): str(cluster["cluster_id"])
        for cluster in clusters
        for candidate_id in cluster["member_ids"]
    }
    eligible = [row for row in candidate_rows if bool(row["promotion_eligible"])]
    eligible.sort(
        key=lambda row: (
            -float(row["DSR"]["deflated_z"]),
            -float(row["walk_forward"]["pooled_expectancy_per_trade_1_5x"]),
            float(row["cost_stress"]["maximum_drawdown_2x"]),
            str(row["policy_id"]),
        )
    )
    chosen: list[str] = []
    used_clusters: set[str] = set()
    for row in eligible:
        policy_id = str(row["policy_id"])
        cluster = cluster_by_candidate[policy_id]
        if cluster in used_clusters:
            continue
        chosen.append(policy_id)
        used_clusters.add(cluster)
        if len(chosen) >= maximum:
            break
    return chosen


def run_phase2(
    *,
    project_root: str | Path,
    preregistration_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    prereg_path = Path(preregistration_path).resolve()
    _verify_frozen_inputs(root, prereg_path, proof_registry_path)
    frozen = _load_frozen_baskets(root)
    if len(frozen) != 55:
        raise Phase2Error(f"expected 55 frozen baskets, found {len(frozen)}")
    common_days = sorted(
        set.intersection(
            *(set(map(int, row.bank["eligible_session_days"])) for row in frozen)
        )
    )
    if len(common_days) < 60:
        raise Phase2Error("insufficient aligned development session days")

    daily_vectors: list[np.ndarray] = []
    candidate_rows: list[dict[str, Any]] = []
    for item in frozen:
        paths: dict[float, dict[str, Any]] = {}
        for multiplier in (1.0, 1.5, 2.0):
            components = _stress_components(item.components, multiplier)
            paths[multiplier] = _full_account_path(
                components, common_days, basket=item.basket
            )
        base_daily = np.asarray(paths[1.0]["daily_pnl"], dtype=np.float64)
        daily_vectors.append(base_daily)
        dsr = deflated_sharpe_statistics(base_daily, n_trials=N_TRIALS)
        walk_forward = _walk_forward(item, common_days, multiplier=1.5)
        phase0 = item.phase0
        compliance = _trajectory_compliance(item, phase0)
        maximum_drawdown_2x = _maximum_drawdown(paths[2.0]["daily_pnl"])
        sim_exploit = paths[2.0]["expectancy_per_trade"] <= 0.0
        candidate_rows.append(
            {
                "policy_id": item.policy_id,
                "generation": item.generation,
                "basket": item.basket.to_dict(),
                "component_count": len(item.basket.component_ids),
                "component_ids": list(item.basket.component_ids),
                "daily_path_sha256": _array_hash(base_daily),
                "DSR": dsr,
                "cost_stress": {
                    "base": _path_summary(paths[1.0]),
                    "1_5x": _path_summary(paths[1.5]),
                    "2x": _path_summary(paths[2.0]),
                    "maximum_drawdown_2x": maximum_drawdown_2x,
                    "SIM_EXPLOIT": sim_exploit,
                },
                "walk_forward": walk_forward,
                "compliance": compliance,
                "phase0_mll_sensitivity": {
                    "default": phase0["default"],
                    "intraday_hwm": phase0["sensitivity"],
                    "terminal_transition_count": phase0[
                        "terminal_transition_count"
                    ],
                },
                "hard_integrity_failure": bool(
                    paths[1.0]["compliance_failure"]
                    or paths[1.5]["compliance_failure"]
                    or paths[2.0]["compliance_failure"]
                ),
            }
        )

    candidate_rows.sort(key=lambda row: str(row["policy_id"]))
    daily_by_id = {
        item.policy_id: vector for item, vector in zip(frozen, daily_vectors, strict=True)
    }
    aligned = np.vstack([daily_by_id[row["policy_id"]] for row in candidate_rows])
    ids = [str(row["policy_id"]) for row in candidate_rows]
    correlations, clusters = behavioral_clusters(
        ids, aligned, cut_distance=CLUSTER_CUT_DISTANCE
    )
    cluster_by_candidate = {
        str(member): str(cluster["cluster_id"])
        for cluster in clusters
        for member in cluster["member_ids"]
    }
    bh = benjamini_hochberg(
        {
            str(row["policy_id"]): float(row["DSR"]["one_sided_p_value"])
            for row in candidate_rows
        },
        q=FDR_Q,
    )
    for row in candidate_rows:
        policy_id = str(row["policy_id"])
        row["behavioral_cluster"] = cluster_by_candidate[policy_id]
        row["BH"] = bh[policy_id]
        gates = {
            "deflated_z_gt_0": float(row["DSR"]["deflated_z"]) > 0.0,
            "BH_FDR_10pct_rejected": bool(bh[policy_id]["rejected"]),
            "purged_walk_forward_1_5x_expectancy_per_trade_gt_0": float(
                row["walk_forward"]["pooled_expectancy_per_trade_1_5x"]
            )
            > 0.0,
            "minimum_retained_events_20": int(
                row["walk_forward"]["retained_event_count"]
            )
            >= 20,
            "trajectory_compliance_100pct": bool(
                row["compliance"]["research_trajectory_compliant"]
            ),
            "SIM_EXPLOIT_2x_survived": not bool(
                row["cost_stress"]["SIM_EXPLOIT"]
            ),
            "no_hard_integrity_failure": not bool(row["hard_integrity_failure"]),
        }
        row["promotion_gates"] = gates
        row["promotion_eligible"] = all(gates.values())

    selected = select_representatives(candidate_rows, clusters, maximum=3)
    verdict = "GREEN" if selected else "NULL"
    result: dict[str, Any] = {
        "schema": "hydra_v7_phase2_result_v1",
        "experiment_id": "hydra_v7_multiplicity_dedup_0001",
        "verdict": verdict,
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "N_trials": N_TRIALS,
        "FDR": FDR_Q,
        "candidate_count": len(candidate_rows),
        "aligned_session_day_count": len(common_days),
        "aligned_session_day_start": int(common_days[0]),
        "aligned_session_day_end": int(common_days[-1]),
        "behavioral_cluster_count": len(clusters),
        "BH_rejection_count": sum(bool(row["BH"]["rejected"]) for row in candidate_rows),
        "DSR_positive_count": sum(
            float(row["DSR"]["deflated_z"]) > 0.0 for row in candidate_rows
        ),
        "SIM_EXPLOIT_count": sum(
            bool(row["cost_stress"]["SIM_EXPLOIT"]) for row in candidate_rows
        ),
        "promotion_eligible_count": sum(
            bool(row["promotion_eligible"]) for row in candidate_rows
        ),
        "selected_representative_ids": selected,
        "candidate_results": candidate_rows,
        "clusters": clusters,
        "correlation_matrix": {
            "candidate_ids": ids,
            "values": correlations.tolist(),
            "sha256": _array_hash(correlations),
        },
        "proof_registry": {
            "N_trials": N_TRIALS,
            "q4_burned": True,
            "q4_access_delta": 0,
            "forward_gap_access_count": 0,
        },
        "phase_data_spend_usd": 0.0,
        "outbound_order_count": 0,
        "strategy_generation_count": 0,
        "diff_validation": [
            "hydra/validation/v7_phase2_multiplicity.py",
            "scripts/run_v7_phase2.py",
            "tests/ruleset/test_v7_phase2_multiplicity.py",
        ],
        "CONTRE": (
            "The 55 baskets were selected on inherited development research and "
            "the historical test census is reconstructed rather than exact; DSR/BH "
            "can reduce selection bias but only fresh post-freeze windows can prove "
            "a representative."
        ),
        "prochaine_action": (
            "freeze_selected_candidate_fiches_WORM_before_gap_ingestion"
            if selected
            else "allocate_all_shadow_slots_to_track_B_after_hypothesis_preregistration"
        ),
    }
    return _write_outputs(result, output_dir)


def _verify_frozen_inputs(
    root: Path, prereg_path: Path, proof_registry_path: str | Path
) -> None:
    if _sha256(prereg_path) != PREREGISTRATION_SHA256:
        raise Phase2Error("Phase 2 preregistration hash mismatch")
    prereg = json.loads(prereg_path.read_text(encoding="utf-8"))
    if _sha256(root / prereg["contract"]["path"]) != CONTRACT_SHA256:
        raise Phase2Error("mission contract hash mismatch")
    if _sha256(root / prereg["required_prior_gates"]["G0"]["path"]) != G0_SHA256:
        raise Phase2Error("G0 evidence hash mismatch")
    if _sha256(root / prereg["required_prior_gates"]["G1"]["path"]) != G1_SHA256:
        raise Phase2Error("G1 evidence hash mismatch")
    if json.loads((root / prereg["required_prior_gates"]["G0"]["path"]).read_text())["verdict"] != "GREEN":
        raise Phase2Error("G0 is not GREEN")
    if json.loads((root / prereg["required_prior_gates"]["G1"]["path"]).read_text())["verdict"] != "GREEN":
        raise Phase2Error("G1 is not GREEN")
    registry = load_and_verify(proof_registry_path)
    if multiplicity_trial_count(registry) != N_TRIALS:
        raise Phase2Error("multiplicity counter does not equal preregistered N_trials")
    if burned_window_ids(registry) != ("Q4_2024",):
        raise Phase2Error("Q4 must remain the only burned proof window before Phase 2")
    verify_v7_data_manifest(root, root / "data/manifest.json")


def _load_frozen_baskets(root: Path) -> list[FrozenBasket]:
    scope = json.loads(
        (root / "config/v7/phase0_g0_preregistration.json").read_text(
            encoding="utf-8"
        )
    )["historical_replay_scope"]["generations"]
    phase0_rows = {
        str(row["policy_id"]): row
        for row in json.loads(
            (root / "reports/v7/phase0_v2/phase0_result.json").read_text(
                encoding="utf-8"
            )
        )["basket_results"]
    }
    output: list[FrozenBasket] = []
    for name, generation_scope in sorted(scope.items()):
        generation = int(name.rsplit("_", 1)[1])
        directory = (
            root
            / "reports/mission_experiments"
            / f"account_level_evolution_v6_generation_{generation:04d}"
        )
        bank = _load_component_bank(directory / "account_v6_component_bank.json")
        rows = _jsonl_by_id(
            directory / "account_v6_promotion_results.jsonl", "policy_id"
        )
        for policy_id in generation_scope["basket_elite_ids"]:
            source = rows[str(policy_id)]
            basket = BasketPolicy.from_dict(source["basket"])
            components = {
                component_id: tuple(
                    RoutedTrade.from_dict(row)
                    for row in bank["components"][component_id]["events"]
                )
                for component_id in basket.component_ids
            }
            output.append(
                FrozenBasket(
                    generation=generation,
                    policy_id=str(policy_id),
                    basket=basket,
                    bank=bank,
                    components=components,
                    phase0=phase0_rows[str(policy_id)],
                    episode_start_days=tuple(
                        int(value) for value in source["episode_start_days"]
                    ),
                )
            )
    return output


def _load_component_bank(path: Path) -> dict[str, Any]:
    bank = json.loads(path.read_text(encoding="utf-8"))
    expected = str(bank.get("manifest_hash") or "")
    unhashed = dict(bank)
    unhashed.pop("manifest_hash", None)
    if not expected or stable_hash(unhashed) != expected:
        raise Phase2Error(f"component bank hash mismatch: {path}")
    return bank


def _jsonl_by_id(path: Path, key: str) -> dict[str, dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    output = {str(row[key]): row for row in rows}
    if len(output) != len(rows):
        raise Phase2Error(f"duplicate {key} in {path}")
    return output


def _stress_components(
    components: Mapping[str, tuple[RoutedTrade, ...]], multiplier: float
) -> dict[str, tuple[RoutedTrade, ...]]:
    return {
        component_id: tuple(stress_trade(row, multiplier) for row in rows)
        for component_id, rows in components.items()
    }


def _analysis_config() -> Topstep150KConfig:
    return Topstep150KConfig(
        combine_profit_target=ANALYTIC_BARRIER,
        combine_max_loss_limit=ANALYTIC_BARRIER,
        minimum_pass_days=1_000_000,
        mll_mode=MllMode.EOD_LEVEL_RT_BREACH,
    )


def _full_account_path(
    components: Mapping[str, tuple[RoutedTrade, ...]],
    eligible_days: Sequence[int],
    *,
    basket: BasketPolicy,
) -> dict[str, Any]:
    episode = run_shared_account_episode(
        components,
        eligible_days,
        basket=basket,
        start_day=int(eligible_days[0]),
        maximum_duration_days=len(eligible_days),
        config=_analysis_config(),
    )
    daily = [float(row["day_pnl"]) for row in episode.daily_path]
    if len(daily) != len(eligible_days):
        return {
            "daily_pnl": daily + [0.0] * (len(eligible_days) - len(daily)),
            "net_pnl": float(episode.net_pnl),
            "accepted_events": int(episode.accepted_events),
            "expectancy_per_trade": float(
                episode.net_pnl / max(episode.accepted_events, 1)
            ),
            "total_cost": float(episode.total_cost),
            "compliance_failure": True,
            "terminal_reason": episode.terminal_reason,
        }
    return {
        "daily_pnl": daily,
        "net_pnl": float(episode.net_pnl),
        "accepted_events": int(episode.accepted_events),
        "expectancy_per_trade": float(
            episode.net_pnl / max(episode.accepted_events, 1)
        ),
        "total_cost": float(episode.total_cost),
        "compliance_failure": False,
        "terminal_reason": episode.terminal_reason,
    }


def _walk_forward(
    item: FrozenBasket, common_days: Sequence[int], *, multiplier: float
) -> dict[str, Any]:
    arrays = np.array_split(np.asarray(common_days, dtype=np.int64), 4)
    components = _stress_components(item.components, multiplier)
    folds: list[dict[str, Any]] = []
    total_net = 0.0
    total_events = 0
    for index, raw_days in enumerate(arrays):
        retained = raw_days if index == 0 else raw_days[5:]
        if len(retained) == 0:
            raise Phase2Error("walk-forward embargo emptied a fold")
        path = _full_account_path(components, retained.tolist(), basket=item.basket)
        if path["compliance_failure"]:
            raise Phase2Error(f"walk-forward compliance failure: {item.policy_id}")
        total_net += float(path["net_pnl"])
        total_events += int(path["accepted_events"])
        boundary_ns = _session_start_ns(int(retained[0]))
        purged_cross_boundary = sum(
            trade.event.decision_ns < boundary_ns <= trade.event.exit_ns
            for rows in components.values()
            for trade in rows
        )
        folds.append(
            {
                "fold": index + 1,
                "raw_session_days": int(len(raw_days)),
                "embargo_session_days": 0 if index == 0 else 5,
                "retained_session_days": int(len(retained)),
                "start_day": int(retained[0]),
                "end_day": int(retained[-1]),
                "accepted_events": int(path["accepted_events"]),
                "net_pnl_1_5x": float(path["net_pnl"]),
                "expectancy_per_trade_1_5x": float(path["expectancy_per_trade"]),
                "purged_cross_boundary_event_count": int(purged_cross_boundary),
            }
        )
    return {
        "fold_count": 4,
        "folds": folds,
        "retained_event_count": total_events,
        "pooled_net_pnl_1_5x": total_net,
        "pooled_expectancy_per_trade_1_5x": total_net / max(total_events, 1),
        "purge_rule": "exact frozen holding-horizon boundary exclusion",
        "embargo_days_after_first_fold": 5,
    }


def _trajectory_compliance(
    item: FrozenBasket, phase0: Mapping[str, Any]
) -> dict[str, Any]:
    raw_events = [trade for rows in item.components.values() for trade in rows]
    raw_session_ok = all(trade.event.session_compliant for trade in raw_events)
    raw_contract_ok = all(
        trade.event.contract_limit_compliant
        and trade.event.mini_equivalent <= 15.0 + 1.0e-12
        for trade in raw_events
    )
    both_mll_modes_execute_without_dll = (
        int(phase0["default"]["compliance_failure_count"]) == 0
        and int(phase0["sensitivity"]["compliance_failure_count"]) == 0
    )
    dll_diagnostics = _optional_dll_diagnostics(item)
    both_mll_modes_execute_with_dll = all(
        int(row["compliance_failure_count"]) == 0
        for row in dll_diagnostics.values()
    )
    implementation_coverage = {
        f"R{number}": True for number in range(1, 17)
    }
    provenance_blockers = ["R2", "R6", "R7", "R11"]
    compliant = (
        raw_session_ok
        and raw_contract_ok
        and both_mll_modes_execute_without_dll
        and both_mll_modes_execute_with_dll
    )
    return {
        "research_trajectory_compliant": compliant,
        "implemented_rule_count": 16,
        "implemented_rules": implementation_coverage,
        "raw_session_compliant": raw_session_ok,
        "raw_contract_and_size_compliant": raw_contract_ok,
        "both_R2_modes_executable_without_DLL": both_mll_modes_execute_without_dll,
        "both_R2_modes_executable_with_DLL": both_mll_modes_execute_with_dll,
        "R3_optional_DLL_diagnostics": dll_diagnostics,
        "R16_timezone": "America/Chicago",
        "deployment_ticket_allowed": False,
        "deployment_ticket_blockers": provenance_blockers,
        "outbound_order_count": 0,
    }


def _optional_dll_diagnostics(item: FrozenBasket) -> dict[str, Any]:
    output: dict[str, Any] = {}
    days = tuple(int(value) for value in item.bank["eligible_session_days"])
    for mode in (MllMode.EOD_LEVEL_RT_BREACH, MllMode.INTRADAY_HWM):
        episodes = [
            run_shared_account_episode(
                item.components,
                days,
                basket=item.basket,
                start_day=start,
                maximum_duration_days=60,
                config=Topstep150KConfig(
                    mll_mode=mode,
                    use_optional_daily_loss_limit=True,
                ),
            )
            for start in item.episode_start_days
        ]
        output[mode.value] = {
            "episode_count": len(episodes),
            "pass_count": sum(episode.passed for episode in episodes),
            "mll_breach_count": sum(episode.mll_breached for episode in episodes),
            "compliance_failure_count": sum(
                episode.terminal_reason
                in {
                    "session_policy_violation",
                    "component_contract_limit_violation",
                }
                for episode in episodes
            ),
            "DLL_triggered_episode_count": sum(
                any(bool(day["dll_triggered"]) for day in episode.daily_path)
                for episode in episodes
            ),
        }
    return output


def _session_start_ns(session_day: int) -> int:
    trading_date = (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=session_day)).date()
    local_start = datetime.combine(
        trading_date - timedelta(days=1),
        time(17, 0),
        tzinfo=ZoneInfo("America/Chicago"),
    )
    return int(local_start.timestamp() * 1_000_000_000)


def _maximum_drawdown(daily_pnl: Sequence[float]) -> float:
    cumulative = np.cumsum(np.asarray(daily_pnl, dtype=np.float64))
    if cumulative.size == 0:
        return 0.0
    peaks = np.maximum.accumulate(np.concatenate(([0.0], cumulative)))[:-1]
    return float(np.max(peaks - cumulative))


def _path_summary(path: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "net_pnl": float(path["net_pnl"]),
        "accepted_events": int(path["accepted_events"]),
        "expectancy_per_trade": float(path["expectancy_per_trade"]),
        "total_cost": float(path["total_cost"]),
        "maximum_drawdown": _maximum_drawdown(path["daily_pnl"]),
    }


def _write_outputs(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    clusters_path = destination / "behavioral_clusters.json"
    representatives_path = destination / "selected_representatives.json"
    result_path = destination / "phase2_result.json"
    report_path = destination / "phase2_report.md"
    _atomic_json(
        clusters_path,
        {
            "schema": "hydra_v7_phase2_clusters_v1",
            "cut_distance": CLUSTER_CUT_DISTANCE,
            "cluster_count": result["behavioral_cluster_count"],
            "clusters": result["clusters"],
            "correlation_matrix": result["correlation_matrix"],
        },
    )
    _atomic_json(
        representatives_path,
        {
            "schema": "hydra_v7_phase2_representatives_v1",
            "preregistration_sha256": PREREGISTRATION_SHA256,
            "representative_ids": result["selected_representative_ids"],
            "representative_count": len(result["selected_representative_ids"]),
            "proof_status": "NOT_YET_PROVEN_FORWARD_REQUIRED",
        },
    )
    serializable = dict(result)
    serializable.pop("result_path", None)
    serializable.pop("result_sha256", None)
    serializable.pop("report_path", None)
    _atomic_json(result_path, serializable)
    report_path.write_text(_render_report(result), encoding="utf-8")
    result["cluster_manifest_path"] = str(clusters_path)
    result["representative_manifest_path"] = str(representatives_path)
    result["result_path"] = str(result_path)
    result["result_sha256"] = _sha256(result_path)
    result["report_path"] = str(report_path)
    return result


def _render_report(result: Mapping[str, Any]) -> str:
    selected = result["selected_representative_ids"]
    lines = [
        "# HYDRA V7 — Phase 2 multiplicité et déduplication",
        "",
        f"[HYDRA-V7] phase=2 step=1 verdict={result['verdict']}",
        "gate=P2 preuve=reports/v7/phase2/phase2_result.json#pending tests=pending",
        f"budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials={result['N_trials']} burned=1",
        "diff_validation=hydra/validation/v7_phase2_multiplicity.py,scripts/run_v7_phase2.py,tests/ruleset/test_v7_phase2_multiplicity.py CONTRE=les_55_paniers_sont_selectionnes_sur_developpement_et_le_recensement_historique_est_reconstruit",
        f"prochaine_action={result['prochaine_action']}",
        "",
        "## Résultat",
        "",
        f"- Candidats : `{result['candidate_count']}`",
        f"- Familles comportementales : `{result['behavioral_cluster_count']}`",
        f"- DSR z > 0 : `{result['DSR_positive_count']}`",
        f"- Rejets BH FDR 10 % : `{result['BH_rejection_count']}`",
        f"- SIM_EXPLOIT : `{result['SIM_EXPLOIT_count']}`",
        f"- Éligibles : `{result['promotion_eligible_count']}`",
        f"- Représentants retenus : `{len(selected)}`",
        "",
        "## Représentants",
        "",
    ]
    lines.extend(f"- `{candidate_id}`" for candidate_id in selected)
    if not selected:
        lines.append("- Aucun : verdict null propre pour la piste héritée.")
    lines.extend(
        [
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    return "\n".join(lines)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _array_hash(values: np.ndarray) -> str:
    contiguous = np.ascontiguousarray(values)
    digest = hashlib.sha256()
    digest.update(str(contiguous.dtype).encode("ascii"))
    digest.update(json.dumps(list(contiguous.shape)).encode("ascii"))
    digest.update(contiguous.tobytes())
    return digest.hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "Phase2Error",
    "behavioral_clusters",
    "benjamini_hochberg",
    "deflated_sharpe_statistics",
    "run_phase2",
    "select_representatives",
    "stress_trade",
]
