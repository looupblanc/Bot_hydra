from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.strategy.v71_event_time_executable import (
    assert_no_order_capability,
    frozen_signal_population,
    load_executable_strategies,
)
from hydra.validation.v71_opportunity_density_tripwire import build_candidate_events
from hydra.validation.v7_d1_new_dataset_tripwire import _eligible_days_by_year
from hydra.validation.v7_report_schema import validate_v7_report_text


DIAGNOSTIC_PATH = "WORM/v7.1-event-time-executable-diagnostic-0001-2026-07-12.json"
DIAGNOSTIC_SHA256 = (
    "058278f8111dc35d6f19ef484ed4b0674f5bb323dbb2a941ebd9d7971080c944"
)
AUDIT_PATH = "reports/v7_1/power_aware_0001/v71_power_aware_candidate_audit_result.json"
AUDIT_SHA256 = "f0eb23117b5703b3d50823365cff7cf9d37c7faeb6ce5628ca7e6c19f04c930b"
TRIPWIRE_PATH = "reports/v7_1/discovery_0003/v71_event_time_tripwire_result.json"
TRIPWIRE_SHA256 = "ae22d7a48eef4ef1804fb81c26453dafc1efdcd138c09c04fd48766cbe1a5b44"
EXPECTED_GLOBAL_N_TRIALS = 263_604
MAXIMUM_DURATION_DAYS = 20
MINIMUM_SERIOUS_STARTS = 24


class V71EventTimeRollingDiagnosticError(RuntimeError):
    pass


def run_event_time_rolling_diagnostic(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/power_aware_0001",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, audit, tripwire = _verify_inputs(root, proof_registry_path)
    configs = load_executable_strategies(root)
    assert_no_order_capability(configs)
    signals, specs, source = frozen_signal_population(root)
    minute, source_audit = source
    cost_model = load_cost_model()
    base_events = build_candidate_events(
        minute,
        signals,
        specs,
        cost_model,
        stress=CostStress.BASE,
    )
    eligible_by_year = _eligible_days_by_year(minute)
    starts_by_year = {
        int(year): tuple(days[: max(0, len(days) - MAXIMUM_DURATION_DAYS + 1)])
        for year, days in eligible_by_year.items()
    }
    mll_modes = tuple(MllMode(value) for value in policy["rolling_combine"]["mll_modes"])
    candidate_results: dict[str, Any] = {}
    for config in configs:
        quantity_results = {}
        for quantity in config.diagnostic_quantities:
            events = tuple(_scale_event(row, quantity) for row in base_events[config.candidate_id])
            mode_results = {
                mode.value: _run_episode_set(
                    events,
                    eligible_by_year,
                    starts_by_year,
                    mode=mode,
                )
                for mode in mll_modes
            }
            quantity_results[str(quantity)] = mode_results
        candidate_results[config.candidate_id] = {
            "alias": config.alias,
            "power_status": next(
                row["status"]
                for row in audit["candidate_results"]
                if row["candidate_id"] == config.candidate_id
            ),
            "full_D1_event_count": len(base_events[config.candidate_id]),
            "full_D1_net_after_BASE_costs_quantity_1": float(
                sum(row.net_pnl for row in base_events[config.candidate_id])
            ),
            "quantity_results": quantity_results,
        }

    basket_events, conflict = serialize_account_events(base_events)
    basket_results = {
        mode.value: _run_episode_set(
            basket_events,
            eligible_by_year,
            starts_by_year,
            mode=mode,
        )
        for mode in mll_modes
    }
    for mode in mll_modes:
        standalone_progress = max(
            float(
                candidate_results[config.candidate_id]["quantity_results"]["1"]
                [mode.value]["median_target_progress"]
            )
            for config in configs
        )
        basket_results[mode.value]["marginal_target_progress_vs_best_standalone"] = (
            float(basket_results[mode.value]["median_target_progress"])
            - standalone_progress
        )
    episode_start_count = sum(len(rows) for rows in starts_by_year.values())
    result = {
        "schema": "hydra_v7_1_event_time_rolling_diagnostic_result_v1",
        "diagnostic_id": "hydra_v7_1_event_time_rolling_diagnostic_0001",
        "scientific_status": "BOUNDED_DIAGNOSTIC_ONLY_NO_PROMOTION",
        "episode_start_count": episode_start_count,
        "minimum_serious_episode_starts": MINIMUM_SERIOUS_STARTS,
        "episode_power_status": (
            "ADEQUATE_STARTS"
            if episode_start_count >= MINIMUM_SERIOUS_STARTS
            else "INSUFFICIENT_EPISODE_STARTS"
        ),
        "starts_by_year": {
            str(year): list(starts) for year, starts in starts_by_year.items()
        },
        "maximum_duration_trading_days": MAXIMUM_DURATION_DAYS,
        "candidate_results": candidate_results,
        "basket": {
            "basket_id": policy["basket"]["basket_id"],
            "accepted_event_count": len(basket_events),
            "blocked_conflict_count": conflict["blocked_conflict_count"],
            "blocked_by_candidate": conflict["blocked_by_candidate"],
            "mode_results": basket_results,
        },
        "event_time_tripwire": {
            "verdict": tripwire["verdict"],
            "real_pass_rate": tripwire["real"]["pass_rate"],
            "pooled_null_pass_rate": tripwire["pooled_null"]["pass_rate"],
            "NULL_RATIO": tripwire["NULL_RATIO"],
            "pass_rates_are_edge_fitness": False,
        },
        "source_audit": source_audit.to_dict(),
        "candidate_thresholds_changed": False,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
        "shadow_promotion_authorized": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "broker_or_order_capability": False,
        "outbound_order_count": 0,
        "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "CONTRE": (
            "Only a handful of 20-day starts exist in the two frozen one-month "
            "D1 blocks, and the grammar tripwire is GEOMETRY_ONLY; these account "
            "paths measure mechanics, not reliable pass probability."
        ),
        "prochaine_action": (
            "freeze_diagnostic_and_keep_candidates_underpowered_pending_independent_confirmation"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def serialize_account_events(
    events: Mapping[str, Sequence[TradePathEvent]],
) -> tuple[tuple[TradePathEvent, ...], dict[str, Any]]:
    merged = sorted(
        (
            replace(row, event_id=f"{candidate_id}|{row.event_id}")
            for candidate_id, candidate_events in events.items()
            for row in candidate_events
        ),
        key=lambda row: (row.session_day, row.decision_ns, row.event_id),
    )
    accepted: list[TradePathEvent] = []
    blocked: Counter[str] = Counter()
    active_exit_by_day: dict[int, int] = {}
    for row in merged:
        active_exit = active_exit_by_day.get(int(row.session_day), -1)
        if row.decision_ns < active_exit:
            blocked[row.event_id.split("|", 1)[0]] += 1
            continue
        accepted.append(row)
        active_exit_by_day[int(row.session_day)] = row.exit_ns
    return tuple(accepted), {
        "blocked_conflict_count": sum(blocked.values()),
        "blocked_by_candidate": dict(sorted(blocked.items())),
    }


def _run_episode_set(
    events: Sequence[TradePathEvent],
    eligible_by_year: Mapping[int, Sequence[int]],
    starts_by_year: Mapping[int, Sequence[int]],
    *,
    mode: MllMode,
) -> dict[str, Any]:
    episodes: list[CombineEpisodeResult] = []
    for year in sorted(eligible_by_year):
        days = tuple(int(value) for value in eligible_by_year[year])
        for start in starts_by_year[year]:
            episodes.append(
                run_combine_episode(
                    events,
                    days,
                    start_day=int(start),
                    maximum_duration_days=MAXIMUM_DURATION_DAYS,
                    config=Topstep150KConfig(mll_mode=mode),
                    maximum_mini_equivalent=15.0,
                )
            )
    terminal = Counter(row.terminal.value for row in episodes)
    passed_days = [row.days_to_target for row in episodes if row.days_to_target is not None]
    return {
        "episode_count": len(episodes),
        "pass_count": terminal["PASSED"],
        "MLL_breach_count": terminal["MLL_BREACH"],
        "timeout_count": terminal["TIMEOUT"],
        "compliance_failure_count": terminal["COMPLIANCE_FAILURE"],
        "pass_rate": terminal["PASSED"] / max(len(episodes), 1),
        "MLL_breach_rate": terminal["MLL_BREACH"] / max(len(episodes), 1),
        "median_target_progress": float(
            np.median([row.target_progress for row in episodes]) if episodes else 0.0
        ),
        "p25_target_progress": float(
            np.quantile([row.target_progress for row in episodes], 0.25)
            if episodes
            else 0.0
        ),
        "p75_target_progress": float(
            np.quantile([row.target_progress for row in episodes], 0.75)
            if episodes
            else 0.0
        ),
        "median_days_to_target_for_passes": (
            float(np.median(passed_days)) if passed_days else None
        ),
        "minimum_MLL_buffer": float(
            min((row.minimum_mll_buffer for row in episodes), default=0.0)
        ),
        "consistency_pass_rate": float(
            np.mean([row.consistency_ok for row in episodes]) if episodes else 0.0
        ),
        "net_after_costs_across_episodes": float(sum(row.net_pnl for row in episodes)),
        "episodes": [row.to_dict() for row in episodes],
    }


def _scale_event(event: TradePathEvent, quantity: int) -> TradePathEvent:
    return replace(
        event,
        event_id=f"{event.event_id}:Q{quantity}",
        net_pnl=event.net_pnl * quantity,
        gross_pnl=event.gross_pnl * quantity,
        worst_unrealized_pnl=event.worst_unrealized_pnl * quantity,
        best_unrealized_pnl=event.best_unrealized_pnl * quantity,
        quantity=quantity,
        mini_equivalent=event.mini_equivalent * quantity,
    )


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    expected = {
        DIAGNOSTIC_PATH: DIAGNOSTIC_SHA256,
        AUDIT_PATH: AUDIT_SHA256,
        TRIPWIRE_PATH: TRIPWIRE_SHA256,
    }
    drift = [
        path for path, expected_sha in expected.items() if _sha256(root / path) != expected_sha
    ]
    if drift:
        raise V71EventTimeRollingDiagnosticError(
            "rolling diagnostic frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71EventTimeRollingDiagnosticError("multiplicity registry drift")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71EventTimeRollingDiagnosticError("unexpected proof-window state")
    policy = json.loads((root / DIAGNOSTIC_PATH).read_text(encoding="utf-8"))
    audit = json.loads((root / AUDIT_PATH).read_text(encoding="utf-8"))
    tripwire = json.loads((root / TRIPWIRE_PATH).read_text(encoding="utf-8"))
    if audit["powered_candidate_ids"]:
        raise V71EventTimeRollingDiagnosticError(
            "this frozen run is principal-named diagnostic-only, not powered promotion"
        )
    if tripwire["verdict"] != "ARTEFACT_GEOMETRY_ONLY":
        raise V71EventTimeRollingDiagnosticError("event-time tripwire verdict drift")
    return policy, audit, tripwire


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_event_time_rolling_diagnostic_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v71_event_time_rolling_diagnostic_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Event-time Rolling Combine diagnostic",
            "",
            "[HYDRA-V7] phase=4 step=145 verdict=NULL",
            f"gate=V71_EVENT_TIME_ROLLING_DIAGNOSTIC preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=2_strategies_plus_1_shared_basket",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_event_time_rolling_diagnostic.py CONTRE=trop_peu_de_starts_et_tripwire_GEOMETRY_ONLY",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Starts: `{result['episode_start_count']}` / minimum sérieux `{result['minimum_serious_episode_starts']}`",
            f"- Statut: `{result['episode_power_status']}`",
            *[
                f"- {row['alias']} Q1/EOD: pass `{row['quantity_results']['1']['eod_level_rt_breach']['pass_rate']}`, MLL `{row['quantity_results']['1']['eod_level_rt_breach']['MLL_breach_rate']}`, progrès médian `{row['quantity_results']['1']['eod_level_rt_breach']['median_target_progress']}`"
                for row in result["candidate_results"].values()
            ],
            f"- Panier Q1/EOD: pass `{result['basket']['mode_results']['eod_level_rt_breach']['pass_rate']}`, MLL `{result['basket']['mode_results']['eod_level_rt_breach']['MLL_breach_rate']}`, progrès médian `{result['basket']['mode_results']['eod_level_rt_breach']['median_target_progress']}`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = result_hash
    result["report_path"] = str(report_path)
    return result


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "V71EventTimeRollingDiagnosticError",
    "run_event_time_rolling_diagnostic",
    "serialize_account_events",
]
