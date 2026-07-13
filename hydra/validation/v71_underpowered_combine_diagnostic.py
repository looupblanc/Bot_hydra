from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
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
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research import v71_cross_clock_flow_grammar as grammar4
from hydra.research import v71_event_mechanism_grammar as grammar1
from hydra.validation.v71_opportunity_density_tripwire import build_candidate_events
from hydra.validation.v7_d1_new_dataset_tripwire import _eligible_days_by_year
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.1-underpowered-combine-research-policy-0001-2026-07-13.json"
POLICY_SHA256 = "33193b3afaf662a7a2b1fe4bcdfb5f9aa2868f6afec55f365e5ea421cd1f3f88"
COHORT_PATH = "WORM/v7.1-underpowered-combine-cohort-0001-2026-07-13.json"
COHORT_SHA256 = "a2973de8e8ad11607d807b7cea5216db9f860dedff3ade815f34fd360b1c28d5"
SELECTION_PATH = (
    "reports/v7_1/combine_research_0001/"
    "v71_underpowered_combine_selection_manifest.json"
)
SELECTION_SHA256 = "6c5c324bbd22bbab4956b9cd310bc98c73ad8e1e48323cb04e08a65b92442dd1"
EXPECTED_GLOBAL_N_TRIALS = 263_814
MAXIMUM_DURATION_DAYS = 10
STARTS_PER_YEAR = 12


class V71UnderpoweredCombineDiagnosticError(RuntimeError):
    pass


def run_underpowered_combine_diagnostic(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/combine_research_0001",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, cohort, selection = _verify_inputs(root, proof_registry_path)
    events, minute, source_audit = _frozen_candidate_events(root, cohort)
    eligible_by_year = _eligible_days_by_year(minute)
    starts_by_year = _select_starts_by_year(eligible_by_year)
    mll_modes = tuple(MllMode(value) for value in policy["episode_design"]["mll_modes"])
    dll_variants = tuple(str(value) for value in policy["episode_design"]["DLL_variants"])
    candidate_results: dict[str, Any] = {}
    for candidate in cohort["candidates"]:
        candidate_id = str(candidate["candidate_id"])
        variants = {
            _variant_key(mode, dll): _run_episode_set(
                events[candidate_id],
                eligible_by_year,
                starts_by_year,
                mode=mode,
                dll_variant=dll,
            )
            for mode in mll_modes
            for dll in dll_variants
        }
        candidate_results[candidate_id] = {
            "selection_rank": int(candidate["selection_rank"]),
            "prior_power_status": "PROMISING_UNDERPOWERED",
            "diagnostic_status": "PROMISING_UNDERPOWERED_COMBINE_RESEARCH",
            "scientific_power_gate_passed": False,
            "full_D1_event_count": len(events[candidate_id]),
            "full_D1_unique_net_after_STRESS_1_5X_costs": float(
                sum(row.net_pnl for row in events[candidate_id])
            ),
            "variants": variants,
        }
    rank = {
        str(row["candidate_id"]): int(row["selection_rank"])
        for row in cohort["candidates"]
    }
    basket_events, conflict = serialize_account_events(events, rank=rank)
    basket_variants = {
        _variant_key(mode, dll): _run_episode_set(
            basket_events,
            eligible_by_year,
            starts_by_year,
            mode=mode,
            dll_variant=dll,
        )
        for mode in mll_modes
        for dll in dll_variants
    }
    primary_key = _variant_key(MllMode.EOD_LEVEL_RT_BREACH, "disabled")
    leave_one_out: dict[str, Any] = {}
    for candidate in cohort["candidates"]:
        candidate_id = str(candidate["candidate_id"])
        reduced = {
            key: value for key, value in events.items() if key != candidate_id
        }
        reduced_rank = {key: value for key, value in rank.items() if key != candidate_id}
        reduced_events, reduced_conflict = serialize_account_events(
            reduced, rank=reduced_rank
        )
        reduced_variants = {
            _variant_key(mode, dll): _run_episode_set(
                reduced_events,
                eligible_by_year,
                starts_by_year,
                mode=mode,
                dll_variant=dll,
            )
            for mode in mll_modes
            for dll in dll_variants
        }
        full = basket_variants[primary_key]
        without = reduced_variants[primary_key]
        leave_one_out[candidate_id] = {
            "primary_variant": primary_key,
            "pass_rate_contribution": float(full["pass_rate"] - without["pass_rate"]),
            "MLL_breach_rate_contribution": float(
                without["MLL_breach_rate"] - full["MLL_breach_rate"]
            ),
            "median_maximum_target_progress_contribution": float(
                full["target_progress"]["maximum_median"]
                - without["target_progress"]["maximum_median"]
            ),
            "median_episode_net_contribution": float(
                full["net_after_costs"]["episode_median"]
                - without["net_after_costs"]["episode_median"]
            ),
            "reduced_basket_conflicts": reduced_conflict,
            "variants_without_candidate": reduced_variants,
        }
    result = {
        "schema": "hydra_v7_1_underpowered_combine_diagnostic_result_v1",
        "diagnostic_id": "hydra_v7_1_underpowered_combine_diagnostic_0001",
        "scientific_status": "BOUNDED_DIAGNOSTIC_ONLY_NO_PROMOTION",
        "candidate_status": "PROMISING_UNDERPOWERED_COMBINE_RESEARCH",
        "candidate_count": len(candidate_results),
        "episode_start_count": sum(len(rows) for rows in starts_by_year.values()),
        "effective_nonoverlapping_block_count": sum(
            _nonoverlapping_count(rows, eligible_by_year[year])
            for year, rows in starts_by_year.items()
        ),
        "starts_by_year": {
            str(year): list(starts) for year, starts in starts_by_year.items()
        },
        "maximum_duration_trading_days": MAXIMUM_DURATION_DAYS,
        "candidate_results": candidate_results,
        "basket": {
            "basket_id": "v71_underpowered_research_basket_0001",
            "component_count": len(events),
            "accepted_event_count": len(basket_events),
            "conflicts": conflict,
            "variants": basket_variants,
            "leave_one_out_contribution": leave_one_out,
        },
        "source_audit": source_audit,
        "selection_manifest_hash": selection["selection_manifest_hash"],
        "candidate_parameters_changed": False,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
        "final_power_requirement": 0.8,
        "final_power_gate_passed_count": 0,
        "shadow_promotion_authorized": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "broker_or_order_capability": False,
        "outbound_order_count": 0,
        "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "CONTRE": (
            "Twenty-four overlapping ten-day starts reduce to only four non-overlapping "
            "blocks; all pass rates and basket contributions are post-selection diagnostics."
        ),
        "prochaine_action": (
            "retain_statuses_and_seek_independent_confirmation_without_parameter_changes"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def serialize_account_events(
    events: Mapping[str, Sequence[TradePathEvent]],
    *,
    rank: Mapping[str, int],
) -> tuple[tuple[TradePathEvent, ...], dict[str, Any]]:
    merged = sorted(
        (
            (
                candidate_id,
                replace(row, event_id=f"{candidate_id}|{row.event_id}"),
            )
            for candidate_id, candidate_events in events.items()
            for row in candidate_events
        ),
        key=lambda item: (
            item[1].session_day,
            item[1].decision_ns,
            int(rank[item[0]]),
            item[1].event_id,
        ),
    )
    accepted: list[TradePathEvent] = []
    blocked: Counter[str] = Counter()
    active_exit_by_day: dict[int, int] = {}
    for candidate_id, row in merged:
        active_exit = active_exit_by_day.get(int(row.session_day), -1)
        if row.decision_ns < active_exit:
            blocked[candidate_id] += 1
            continue
        accepted.append(row)
        active_exit_by_day[int(row.session_day)] = row.exit_ns
    return tuple(accepted), {
        "policy": "earliest_decision_then_frozen_selection_rank_no_overlap",
        "blocked_conflict_count": sum(blocked.values()),
        "blocked_by_candidate": dict(sorted(blocked.items())),
    }


def _run_episode_set(
    events: Sequence[TradePathEvent],
    eligible_by_year: Mapping[int, Sequence[int]],
    starts_by_year: Mapping[int, Sequence[int]],
    *,
    mode: MllMode,
    dll_variant: str,
) -> dict[str, Any]:
    use_dll = dll_variant == "soft_3000"
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
                    config=Topstep150KConfig(
                        mll_mode=mode,
                        no_daily_loss_limit=not use_dll,
                        use_optional_daily_loss_limit=use_dll,
                        optional_daily_loss_limit=3000.0,
                    ),
                    maximum_mini_equivalent=15.0,
                )
            )
    if len(episodes) != 24:
        raise V71UnderpoweredCombineDiagnosticError(
            "diagnostic must contain exactly 24 identical starts"
        )
    terminal = Counter(row.terminal.value for row in episodes)
    terminal_progress = np.asarray([row.target_progress for row in episodes], dtype=float)
    maximum_progress = np.asarray(
        [
            max(
                (
                    (float(day["balance"]) - 150000.0)
                    / max(float(row.required_target), 1.0)
                    for day in row.daily_path
                ),
                default=0.0,
            )
            for row in episodes
        ],
        dtype=float,
    )
    net = np.asarray([row.net_pnl for row in episodes], dtype=float)
    buffers = np.asarray([row.minimum_mll_buffer for row in episodes], dtype=float)
    passing_days = [
        float(row.days_to_target)
        for row in episodes
        if row.days_to_target is not None
    ]
    positive_median_progress = float(np.median(maximum_progress))
    projected_days = (
        float(MAXIMUM_DURATION_DAYS / positive_median_progress)
        if positive_median_progress > 0.0
        else None
    )
    return {
        "mll_mode": mode.value,
        "DLL_variant": dll_variant,
        "episode_count": len(episodes),
        "pass_count": terminal[CombineTerminal.PASSED.value],
        "pass_rate": terminal[CombineTerminal.PASSED.value] / len(episodes),
        "MLL_breach_count": terminal[CombineTerminal.MLL_BREACH.value],
        "MLL_breach_rate": terminal[CombineTerminal.MLL_BREACH.value] / len(episodes),
        "timeout_count": terminal[CombineTerminal.TIMEOUT.value],
        "consistency_pass_rate": float(
            np.mean([row.consistency_ok for row in episodes])
        ),
        "days_to_target": {
            "p25": _percentile_or_none(passing_days, 25),
            "median": _percentile_or_none(passing_days, 50),
            "p75": _percentile_or_none(passing_days, 75),
            "projected_from_median_maximum_progress": projected_days,
        },
        "target_progress": {
            "terminal_p25": float(np.percentile(terminal_progress, 25)),
            "terminal_median": float(np.median(terminal_progress)),
            "terminal_p75": float(np.percentile(terminal_progress, 75)),
            "maximum_p25": float(np.percentile(maximum_progress, 25)),
            "maximum_median": positive_median_progress,
            "maximum_p75": float(np.percentile(maximum_progress, 75)),
        },
        "net_after_costs": {
            "episode_p25": float(np.percentile(net, 25)),
            "episode_median": float(np.median(net)),
            "episode_p75": float(np.percentile(net, 75)),
            "episode_sum_correlated_not_independent": float(np.sum(net)),
            "unique_event_net": float(sum(row.net_pnl for row in events)),
        },
        "minimum_MLL_buffer": float(np.min(buffers)),
        "median_MLL_buffer": float(np.median(buffers)),
        "contract_limit_compliance_rate": float(
            np.mean([row.contract_limit_compliant for row in episodes])
        ),
        "session_compliance_rate": float(
            np.mean([row.session_compliant for row in episodes])
        ),
        "terminal_distribution": dict(sorted(terminal.items())),
        "episodes": [row.to_dict() for row in episodes],
    }


def _frozen_candidate_events(
    root: Path, cohort: Mapping[str, Any]
) -> tuple[dict[str, tuple[TradePathEvent, ...]], Any, dict[str, Any]]:
    wanted_by_grammar: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in cohort["candidates"]:
        wanted_by_grammar[str(row["grammar_id"])].append(row)
    costs = load_cost_model()
    output: dict[str, tuple[TradePathEvent, ...]] = {}
    minute_reference = None
    source_audit: dict[str, Any] = {}
    for grammar_id, wanted in sorted(wanted_by_grammar.items()):
        if grammar_id == grammar1.GRAMMAR_ID:
            minute = grammar1.load_v71_minute_features(root)
            specs = {row.candidate_id: row for row in grammar1.candidate_specs(root)}
            signals = grammar1.generate_signal_population(
                minute, project_root=root, graveyard_path=None
            )
            audit = {"source": "G1_minute", "minute_count": len(minute)}
        elif grammar_id == grammar4.GRAMMAR_ID:
            minute, pairs, cross_audit = grammar4.load_cross_clock_sources(root)
            specs = {row.candidate_id: row for row in grammar4.candidate_specs(root)}
            signals = grammar4.generate_signal_population(
                minute, pairs, project_root=root, graveyard_path=None
            )
            audit = cross_audit.to_dict()
        else:
            raise V71UnderpoweredCombineDiagnosticError(
                f"cohort contains unsupported grammar: {grammar_id}"
            )
        minute_reference = minute if minute_reference is None else minute_reference
        source_audit[grammar_id] = audit
        selected_specs = {}
        selected_signals = {}
        for frozen in wanted:
            candidate_id = str(frozen["candidate_id"])
            spec = specs[candidate_id]
            if spec.specification_hash != frozen["specification_hash"]:
                raise V71UnderpoweredCombineDiagnosticError(
                    f"{candidate_id} specification drift"
                )
            if _signal_path_hash(signals[candidate_id]) != frozen["signal_path_hash"]:
                raise V71UnderpoweredCombineDiagnosticError(
                    f"{candidate_id} signal path drift"
                )
            selected_specs[candidate_id] = spec
            selected_signals[candidate_id] = signals[candidate_id]
        built = build_candidate_events(
            minute,
            selected_signals,
            selected_specs,
            costs,
            stress=CostStress.STRESS_1_5X,
        )
        output.update({key: tuple(value) for key, value in built.items()})
    if minute_reference is None or len(output) != 5:
        raise V71UnderpoweredCombineDiagnosticError(
            "frozen diagnostic candidate population drift"
        )
    return output, minute_reference, source_audit


def _select_starts_by_year(
    eligible_by_year: Mapping[int, Sequence[int]],
) -> dict[int, tuple[int, ...]]:
    output: dict[int, tuple[int, ...]] = {}
    for year in (2023, 2024):
        days = tuple(int(value) for value in eligible_by_year[year])
        usable = len(days) - MAXIMUM_DURATION_DAYS + 1
        if usable < STARTS_PER_YEAR:
            raise V71UnderpoweredCombineDiagnosticError(
                f"{year} cannot supply twelve frozen ten-day starts"
            )
        indices = np.linspace(0, usable - 1, STARTS_PER_YEAR, dtype=int)
        starts = tuple(days[int(index)] for index in indices)
        if len(set(starts)) != STARTS_PER_YEAR:
            raise V71UnderpoweredCombineDiagnosticError(
                f"{year} start selection contains duplicates"
            )
        output[year] = starts
    return output


def _nonoverlapping_count(starts: Sequence[int], days: Sequence[int]) -> int:
    positions = {int(day): position for position, day in enumerate(days)}
    count = 0
    next_position = -1
    for start in starts:
        position = positions[int(start)]
        if position < next_position:
            continue
        count += 1
        next_position = position + MAXIMUM_DURATION_DAYS
    return count


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    expected = {
        POLICY_PATH: POLICY_SHA256,
        COHORT_PATH: COHORT_SHA256,
        SELECTION_PATH: SELECTION_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71UnderpoweredCombineDiagnosticError(
            "underpowered Combine diagnostic frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71UnderpoweredCombineDiagnosticError(
            "underpowered diagnostic multiplicity reservation absent"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71UnderpoweredCombineDiagnosticError(
            "unexpected proof-window state"
        )
    policy = json.loads((root / POLICY_PATH).read_text(encoding="utf-8"))
    cohort = json.loads((root / COHORT_PATH).read_text(encoding="utf-8"))
    selection = json.loads((root / SELECTION_PATH).read_text(encoding="utf-8"))
    if int(cohort.get("candidate_count", 0)) != 5:
        raise V71UnderpoweredCombineDiagnosticError("cohort candidate count drift")
    if int(selection.get("selected_count", 0)) != 5:
        raise V71UnderpoweredCombineDiagnosticError("selection count drift")
    if any(
        row.get("prior_power_status") != "PROMISING_UNDERPOWERED"
        for row in cohort["candidates"]
    ):
        raise V71UnderpoweredCombineDiagnosticError(
            "cohort contains a non-underpowered candidate"
        )
    return policy, cohort, selection


def _variant_key(mode: MllMode, dll_variant: str) -> str:
    return f"{mode.value}__DLL_{dll_variant}"


def _signal_path_hash(signals: Sequence[Any]) -> str:
    payload = [
        (
            row.decision_ns,
            row.entry_minute_start_ns,
            row.exit_minute_start_ns,
            row.side,
            row.contract,
        )
        for row in signals
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _percentile_or_none(values: Sequence[float], percentile: float) -> float | None:
    return (
        float(np.percentile(np.asarray(values, dtype=float), percentile))
        if values
        else None
    )


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_underpowered_combine_diagnostic_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = (
        result_path.relative_to(root)
        if result_path.is_relative_to(root)
        else result_path
    )
    primary = "eod_level_rt_breach__DLL_disabled"
    report_path = destination / "v71_underpowered_combine_diagnostic_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Underpowered Rolling Combine research diagnostic",
            "",
            "[HYDRA-V7] phase=4 step=165 verdict=NULL",
            f"gate=V71_UNDERPOWERED_COMBINE_DIAGNOSTIC preuve={displayed}#{result_hash[:8]} tests=5_candidates_24_starts_2_MLL_2_DLL",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_underpowered_combine_diagnostic.py CONTRE=24_starts_chevauches_seulement_4_blocs_effectifs",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Starts bruts/effectifs: `{result['episode_start_count']}/{result['effective_nonoverlapping_block_count']}`",
            *[
                f"- {candidate_id}: pass `{row['variants'][primary]['pass_rate']}`, MLL `{row['variants'][primary]['MLL_breach_rate']}`, progrès max médian `{row['variants'][primary]['target_progress']['maximum_median']}`"
                for candidate_id, row in result["candidate_results"].items()
            ],
            f"- Panier: pass `{result['basket']['variants'][primary]['pass_rate']}`, MLL `{result['basket']['variants'][primary]['MLL_breach_rate']}`, progrès max médian `{result['basket']['variants'][primary]['target_progress']['maximum_median']}`",
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
    "EXPECTED_GLOBAL_N_TRIALS",
    "V71UnderpoweredCombineDiagnosticError",
    "run_underpowered_combine_diagnostic",
    "serialize_account_events",
]
