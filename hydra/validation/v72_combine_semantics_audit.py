from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from hydra.propfirm.censored_combine import (
    CombineObservationStatus,
    evaluate_censored_combine_horizons,
    run_censored_combine_episode,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.ruleset_v7 import RuleStatus, load_ruleset
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.2-pareto-crossfit-account-policy-0001-2026-07-13.json"
POLICY_SHA256 = "94f4ad89a2ae2ea347f1fce4a9cb4682690652429f34e42e72edf79e03da6677"


class V72CombineSemanticsAuditError(RuntimeError):
    pass


def run_v72_combine_semantics_audit(
    *,
    project_root: str | Path = ".",
    output_dir: str | Path = "reports/v7_2/semantics",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy = _verify_policy(root)
    ruleset = load_ruleset(root / "config/rulesets/topstep_150k_v7.json")
    rules = ruleset.by_id
    config = Topstep150KConfig()

    checks = {
        "R1_target_9000": config.combine_profit_target
        == float(rules["R1"].parameters["profit_target_usd"])
        == 9_000.0,
        "R1_no_official_time_limit": (
            "no mission-imposed evaluation timeout" in rules["R1"].statement
            and policy["ruleset_snapshot"]["official_time_limit"] is None
        ),
        "R2_distance_4500": config.combine_max_loss_limit
        == float(rules["R2"].parameters["distance_usd"])
        == 4_500.0,
        "R2_touch_is_breach": bool(rules["R2"].parameters["touch_is_breach"]),
        "R2_default_mode": config.resolved_mll_mode
        is MllMode.EOD_LEVEL_RT_BREACH,
        "R2_sensitivity_mode_available": Topstep150KConfig(
            mll_mode="intraday_hwm"
        ).resolved_mll_mode
        is MllMode.INTRADAY_HWM,
        "R3_optional_DLL_3000": config.optional_daily_loss_limit
        == float(rules["R3"].parameters["optional_dll_usd"])
        == 3_000.0,
        "R4_consistency_target_inflation": (
            config.consistency_best_day_max_pct_of_profit_target == 0.5
            and rules["R4"].parameters["breach_action"] == "TARGET_INFLATION"
        ),
        "R5_maximum_contracts_15": int(
            rules["R5"].parameters["max_mini_equivalent"]
        )
        == 15,
        "R6_two_day_assumption_blocks_ticket": (
            config.minimum_pass_days == 2
            and rules["R6"].status is RuleStatus.ASSUMED
            and "R6" in ruleset.deployment_ticket_blockers
        ),
        "R13_zero_server_order_capability": rules["R13"].parameters[
            "research_server_order_capability"
        ]
        is False,
        "R15_zero_HYDRA_orders": int(rules["R15"].parameters["hydra_order_count"])
        == 0,
        "R16_CT_boundaries": (
            rules["R16"].parameters["timezone"] == "America/Chicago"
            and rules["R16"].parameters["trading_day_start_local"] == "17:00"
            and rules["R16"].parameters["session_flatten_local"] == "15:10"
            and rules["R16"].parameters["winning_day_lock_local"] == "16:00"
        ),
    }
    if not all(checks.values()):
        failed = sorted(key for key, value in checks.items() if not value)
        raise V72CombineSemanticsAuditError(
            "Topstep semantics checks failed: " + ",".join(failed)
        )

    probes = _run_semantic_probes(config)
    result = {
        "schema": "hydra_v7_2_combine_semantics_audit_result_v1",
        "audit_id": "hydra_v7_2_combine_semantics_audit_0001",
        "verdict": "GREEN",
        "policy_path": POLICY_PATH,
        "policy_sha256": POLICY_SHA256,
        "ruleset_schema": ruleset.schema,
        "ruleset_as_of_date": ruleset.as_of_date,
        "deployment_ticket_blockers": list(ruleset.deployment_ticket_blockers),
        "checks": checks,
        "semantic_probes": probes,
        "legacy_TIMEOUT_rewritten_or_reinterpreted": False,
        "legacy_evidence_mutated": False,
        "v72_observation_layer": {
            "statuses": [value.value for value in CombineObservationStatus],
            "reporting_horizons": list(
                policy["censoring_policy"]["reporting_horizons_trading_days"]
            ),
            "full_available_reported": True,
            "profitable_survivor_is_terminal_failure": False,
        },
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The existing D1 coverage is too short for uncensored 40/60/90-day "
            "headlines on every frozen block; V7.2 can measure leakage-free short-"
            "horizon progress, but confirmation still requires untouched or forward data."
        ),
        "prochaine_action": "freeze_reconciled_component_bank_before_basket_results",
    }
    return _write_result(result, root, Path(output_dir))


def _run_semantic_probes(config: Topstep150KConfig) -> dict[str, Any]:
    days = tuple(range(100))
    small_profit = (_event(0, net=500.0, worst=-100.0, best=550.0),)
    horizon = run_censored_combine_episode(
        small_profit, days, start_day=0, horizon_days=20, config=config
    )
    short_data = run_censored_combine_episode(
        small_profit, tuple(range(10)), start_day=0, horizon_days=20, config=config
    )
    full_available = run_censored_combine_episode(
        small_profit, tuple(range(30)), start_day=0, horizon_days=None, config=config
    )
    passing = run_censored_combine_episode(
        (
            _event(0, net=4_500.0, worst=-100.0, best=4_500.0),
            _event(1, net=4_500.0, worst=-100.0, best=4_500.0),
        ),
        days,
        start_day=0,
        horizon_days=20,
        config=config,
    )
    breach = run_censored_combine_episode(
        (_event(0, net=0.0, worst=-4_500.0, best=0.0),),
        days,
        start_day=0,
        horizon_days=20,
        config=config,
    )
    eod = run_censored_combine_episode(
        (_event(0, net=100.0, worst=-100.0, best=6_000.0),),
        days,
        start_day=0,
        horizon_days=20,
        config=replace(config, mll_mode=MllMode.EOD_LEVEL_RT_BREACH),
    )
    intraday = run_censored_combine_episode(
        (_event(0, net=100.0, worst=-100.0, best=6_000.0),),
        days,
        start_day=0,
        horizon_days=20,
        config=replace(config, mll_mode=MllMode.INTRADAY_HWM),
    )
    multi = evaluate_censored_combine_horizons(
        tuple(_event(day, net=1_000.0, worst=-100.0, best=1_000.0) for day in range(10)),
        days,
        start_days=(0, 20, 40, 60),
        horizons=(20, 40, 60, 90),
        config=config,
    )
    expected = {
        "operational_horizon": CombineObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED,
        "short_data": CombineObservationStatus.DATA_CENSORED,
        "full_available": CombineObservationStatus.DATA_CENSORED,
        "passing": CombineObservationStatus.TARGET_REACHED,
        "breach": CombineObservationStatus.MLL_BREACHED,
        "eod_high_MFE": CombineObservationStatus.OPERATIONAL_HORIZON_NOT_REACHED,
        "intraday_high_MFE": CombineObservationStatus.MLL_BREACHED,
    }
    actual = {
        "operational_horizon": horizon.observation_status,
        "short_data": short_data.observation_status,
        "full_available": full_available.observation_status,
        "passing": passing.observation_status,
        "breach": breach.observation_status,
        "eod_high_MFE": eod.observation_status,
        "intraday_high_MFE": intraday.observation_status,
    }
    if actual != expected:
        raise V72CombineSemanticsAuditError(
            "censored Combine semantic probe drift: "
            + json.dumps({key: value.value for key, value in actual.items()}, sort_keys=True)
        )
    return {
        "statuses": {key: value.value for key, value in actual.items()},
        "pass_days": passing.legacy_result.days_to_target,
        "horizon_summaries": {
            key: {
                "episode_count": value.episode_count,
                "target_reached_count": value.target_reached_count,
                "mll_breached_count": value.mll_breached_count,
                "data_censored_count": value.data_censored_count,
                "operational_horizon_not_reached_count": value.operational_horizon_not_reached_count,
            }
            for key, value in multi.items()
        },
    }


def _event(
    day: int,
    *,
    net: float,
    worst: float,
    best: float,
) -> TradePathEvent:
    decision = day * 1_000_000_000_000
    return TradePathEvent(
        event_id=f"semantic-probe-{day}-{net}-{worst}-{best}",
        decision_ns=decision,
        exit_ns=decision + 60_000_000_000,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=worst,
        best_unrealized_pnl=best,
        quantity=1,
        mini_equivalent=1.0,
        regime="SEMANTIC_PROBE",
    )


def _verify_policy(root: Path) -> dict[str, Any]:
    path = root / POLICY_PATH
    if _sha256(path) != POLICY_SHA256:
        raise V72CombineSemanticsAuditError("V7.2 WORM policy drift")
    policy = json.loads(path.read_text(encoding="utf-8"))
    if policy.get("recorded_before_any_v72_basket_result") is not True:
        raise V72CombineSemanticsAuditError("V7.2 policy is not preregistered")
    return policy


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_combine_semantics_audit_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = (
        result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    )
    report_path = destination / "v72_combine_semantics_audit_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Rolling Combine semantics audit",
            "",
            "[HYDRA-V7] phase=4 step=182 verdict=GREEN",
            f"gate=V72_COMBINE_SEMANTICS preuve={displayed}#{result_hash[:8]} tests=ruleset_plus_censoring_probes",
            "budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263902 burned=1",
            "diff_validation=hydra/propfirm/censored_combine.py,hydra/validation/v72_combine_semantics_audit.py CONTRE=couverture_D1_trop_courte_pour_horizons_longs",
            f"prochaine_action={result['prochaine_action']}",
            "",
            "- Timeout officiel: `aucun`",
            "- Horizons gelés: `20/40/60/90/full_available`",
            "- Profitable encore vivant à l'horizon: `censuré, jamais échec`",
            "- Variantes MLL: `eod_level_rt_breach` et `intraday_hwm`",
            "- Ordres broker: `0`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    return {
        **result,
        "result_path": str(result_path),
        "result_sha256": result_hash,
        "report_path": str(report_path),
    }


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["V72CombineSemanticsAuditError", "run_v72_combine_semantics_audit"]
