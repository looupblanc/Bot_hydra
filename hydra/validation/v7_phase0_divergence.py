from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.basket import RoutedTrade, evaluate_account_policy
from hydra.account_policy.schema import BasketPolicy, stable_hash
from hydra.account_policy.xfa import evaluate_serial_xfa_basket
from hydra.execution.v7_cost_model import load_cost_model, render_cost_model_markdown
from hydra.governance.proof_registry import burned_window_ids, load_and_verify
from hydra.propfirm.mll_variants import (
    MllVariant,
    favorable_first_is_ambiguous,
)
from hydra.propfirm.rolling_combine import EpisodeStartPolicy
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.propfirm.ruleset_v7 import load_ruleset


PREREGISTRATION_SHA256 = (
    "0ef08acfa423f398b78eb944cd533efada48e7f605afa475006ddc845b5d33c6"
)
DEFAULT_VARIANT = MllVariant.EOD_REALIZED_BALANCE
SENSITIVITY_VARIANT = (
    MllVariant.INTRADAY_LIVE_EQUITY_HWM_CONSERVATIVE_MFE_FIRST
)


class Phase0DivergenceError(RuntimeError):
    pass


def run_phase0_divergence(
    *,
    project_root: str | Path,
    preregistration_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    prereg_path = Path(preregistration_path).resolve()
    if _sha256(prereg_path) != PREREGISTRATION_SHA256:
        raise Phase0DivergenceError("Phase 0 preregistration hash mismatch")
    prereg = json.loads(prereg_path.read_text(encoding="utf-8"))
    source_manifest = root / str(prereg["source_manifest_path"])
    _verify_source_manifest(root, source_manifest)
    proof_registry = load_and_verify(proof_registry_path)
    if "Q4_2024" not in burned_window_ids(proof_registry):
        raise Phase0DivergenceError("Q4 must be BURNED before Phase 0")
    ruleset = load_ruleset()

    basket_rows: list[dict[str, Any]] = []
    xfa_rows: list[dict[str, Any]] = []
    exact_mismatches: list[dict[str, Any]] = []
    terminal_transitions = 0
    ambiguity_count = 0

    for generation_name, scope in prereg["historical_replay_scope"][
        "generations"
    ].items():
        generation = int(generation_name.rsplit("_", 1)[1])
        directory = (
            root
            / "reports"
            / "mission_experiments"
            / f"account_level_evolution_v6_generation_{generation:04d}"
        )
        bank = _load_component_bank(directory / "account_v6_component_bank.json")
        promotion = _jsonl_by_id(
            directory / "account_v6_promotion_results.jsonl", "policy_id"
        )
        xfa = _jsonl_by_id(directory / "account_v6_xfa_results.jsonl", "policy_id")

        for policy_id in scope["basket_elite_ids"]:
            if policy_id not in promotion:
                raise Phase0DivergenceError(
                    f"missing frozen promotion policy {policy_id}"
                )
            baseline = promotion[policy_id]
            basket = BasketPolicy.from_dict(baseline["basket"])
            components = _component_events(bank, basket)
            starts = tuple(int(value) for value in baseline["episode_start_days"])
            policy = EpisodeStartPolicy(
                maximum_starts=len(starts),
                minimum_spacing_sessions=2,
                minimum_observation_sessions=30,
                maximum_duration_sessions=60,
                regime_balanced=False,
            )
            default = evaluate_account_policy(
                components,
                bank["eligible_session_days"],
                basket=basket,
                episode_policy=policy,
                explicit_start_days=starts,
                config=Topstep150KConfig(mll_variant=DEFAULT_VARIANT),
            )
            sensitivity = evaluate_account_policy(
                components,
                bank["eligible_session_days"],
                basket=basket,
                episode_policy=policy,
                explicit_start_days=starts,
                config=Topstep150KConfig(mll_variant=SENSITIVITY_VARIANT),
            )
            mismatch = _compare_historical_summary(
                baseline["summary"], default.to_dict()
            )
            if mismatch:
                exact_mismatches.append(
                    {
                        "generation": generation,
                        "policy_id": policy_id,
                        "scope": "COMBINE_BASKET",
                        "fields": mismatch,
                    }
                )
            transitions = sum(
                left.terminal is not right.terminal
                for left, right in zip(
                    default.episodes, sensitivity.episodes, strict=True
                )
            )
            terminal_transitions += transitions
            policy_ambiguity = _ambiguity_count(components)
            ambiguity_count += policy_ambiguity
            basket_rows.append(
                {
                    "generation": generation,
                    "policy_id": policy_id,
                    "default": _combine_metrics(default),
                    "sensitivity": _combine_metrics(sensitivity),
                    "pass_rate_delta": sensitivity.pass_rate - default.pass_rate,
                    "mll_breach_rate_delta": (
                        sensitivity.mll_breach_rate - default.mll_breach_rate
                    ),
                    "terminal_transition_count": transitions,
                    "mfe_mae_order_ambiguity_count": policy_ambiguity,
                    "historical_default_equivalent": not mismatch,
                }
            )

        for policy_id in scope["xfa_policy_ids"]:
            if policy_id not in xfa:
                raise Phase0DivergenceError(f"missing frozen XFA policy {policy_id}")
            baseline = xfa[policy_id]
            basket = BasketPolicy.from_dict(baseline["basket"])
            components = _component_events(bank, basket)
            default = evaluate_serial_xfa_basket(
                components,
                bank["eligible_session_days"],
                basket=basket,
                maximum_starts=12,
                config=Topstep150KConfig(mll_variant=DEFAULT_VARIANT),
            )
            sensitivity = evaluate_serial_xfa_basket(
                components,
                bank["eligible_session_days"],
                basket=basket,
                maximum_starts=12,
                config=Topstep150KConfig(mll_variant=SENSITIVITY_VARIANT),
            )
            mismatch = _compare_xfa_summary(
                baseline["rolling_xfa"], default["rolling_xfa"]
            )
            if mismatch:
                exact_mismatches.append(
                    {
                        "generation": generation,
                        "policy_id": policy_id,
                        "scope": "XFA_POLICY",
                        "fields": mismatch,
                    }
                )
            default_xfa = default["rolling_xfa"]
            sensitivity_xfa = sensitivity["rolling_xfa"]
            policy_ambiguity = _ambiguity_count(components)
            ambiguity_count += policy_ambiguity
            xfa_rows.append(
                {
                    "generation": generation,
                    "policy_id": policy_id,
                    "default": _xfa_metrics(default_xfa),
                    "sensitivity": _xfa_metrics(sensitivity_xfa),
                    "payout_probability_delta": float(
                        sensitivity_xfa["payout_probability"]
                        - default_xfa["payout_probability"]
                    ),
                    "expected_cycles_delta": float(
                        sensitivity_xfa["expected_payout_cycles_before_ruin"]
                        - default_xfa["expected_payout_cycles_before_ruin"]
                    ),
                    "mfe_mae_order_ambiguity_count": policy_ambiguity,
                    "historical_default_equivalent": not mismatch,
                }
            )

    if len(basket_rows) != 55 or len(xfa_rows) != 6:
        raise Phase0DivergenceError("frozen Phase 0 scope is not 55 baskets + 6 XFA")
    classification = (
        "CONTAMINATED" if exact_mismatches else "HISTORICAL_DEFAULT_EQUIVALENT"
    )
    aggregate = {
        "default": _aggregate_combine(basket_rows, "default"),
        "sensitivity": _aggregate_combine(basket_rows, "sensitivity"),
        "terminal_transition_count": terminal_transitions,
        "mfe_mae_order_ambiguity_count": ambiguity_count,
    }
    aggregate["pass_rate_delta"] = (
        aggregate["sensitivity"]["pass_rate"]
        - aggregate["default"]["pass_rate"]
    )
    aggregate["mll_breach_rate_delta"] = (
        aggregate["sensitivity"]["mll_breach_rate"]
        - aggregate["default"]["mll_breach_rate"]
    )
    result = {
        "schema": "hydra_v7_phase0_divergence_result_v1",
        "experiment_id": prereg["experiment_id"],
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "source_manifest_sha256": prereg["source_manifest_sha256"],
        "historical_classification": classification,
        "historical_default_mismatch_count": len(exact_mismatches),
        "historical_default_mismatches": exact_mismatches,
        "basket_count": len(basket_rows),
        "xfa_policy_count": len(xfa_rows),
        "combine": aggregate,
        "xfa": {
            "default": _aggregate_xfa(xfa_rows, "default"),
            "sensitivity": _aggregate_xfa(xfa_rows, "sensitivity"),
        },
        "basket_results": basket_rows,
        "xfa_results": xfa_rows,
        "ruleset": {
            "rule_count": len(ruleset.rules),
            "deployment_ticket_allowed": ruleset.deployment_ticket_allowed,
            "deployment_ticket_blockers": list(
                ruleset.deployment_ticket_blockers
            ),
        },
        "q4_burned": True,
        "phase_spend_usd": 0.0,
        "outbound_order_count": 0,
        "generation_count": 0,
        "diff_validation": [
            "hydra/account_policy/basket.py",
            "hydra/account_policy/xfa.py",
            "hydra/propfirm/combine_episode.py",
            "hydra/propfirm/intraday_mll.py",
            "hydra/propfirm/mll_variants.py",
            "hydra/propfirm/topstep_150k.py",
            "hydra/propfirm/xfa_episode.py",
            "hydra/validation/v7_phase0_divergence.py",
            "tests/ruleset/",
        ],
    }
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "phase0_result.json"
    result_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (destination / "mll_divergence_report.md").write_text(
        _render_divergence_report(result), encoding="utf-8"
    )
    (destination / "costs_model.md").write_text(
        render_cost_model_markdown(load_cost_model()), encoding="utf-8"
    )
    result["result_path"] = str(result_path)
    result["result_sha256"] = _sha256(result_path)
    return result


def _load_component_bank(path: Path) -> dict[str, Any]:
    bank = json.loads(path.read_text(encoding="utf-8"))
    expected = str(bank.get("manifest_hash") or "")
    unhashed = dict(bank)
    unhashed.pop("manifest_hash", None)
    if not expected or stable_hash(unhashed) != expected:
        raise Phase0DivergenceError(f"component bank hash mismatch: {path}")
    return bank


def _component_events(
    bank: Mapping[str, Any], basket: BasketPolicy
) -> dict[str, tuple[RoutedTrade, ...]]:
    return {
        component_id: tuple(
            RoutedTrade.from_dict(row)
            for row in bank["components"][component_id]["events"]
        )
        for component_id in basket.component_ids
    }


def _jsonl_by_id(path: Path, key: str) -> dict[str, dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    output = {str(row[key]): row for row in rows}
    if len(output) != len(rows):
        raise Phase0DivergenceError(f"duplicate {key} in {path}")
    return output


def _compare_historical_summary(
    baseline: Mapping[str, Any], replay: Mapping[str, Any]
) -> list[str]:
    exact = (
        "episode_start_days",
        "terminal_distribution",
        "pass_count",
        "mll_breach_count",
        "compliance_failure_count",
    )
    numeric = (
        "pass_rate",
        "mll_breach_rate",
        "target_progress_median",
        "median_episode_net_pnl",
        "minimum_mll_buffer",
    )
    mismatch = [field for field in exact if baseline[field] != replay[field]]
    mismatch.extend(
        field
        for field in numeric
        if abs(float(baseline[field]) - float(replay[field])) > 1e-9
    )
    return mismatch


def _compare_xfa_summary(
    baseline: Mapping[str, Any], replay: Mapping[str, Any]
) -> list[str]:
    exact = ("selected_path", "episode_start_count")
    numeric = (
        "expected_payout_cycles_before_ruin",
        "payout_probability",
        "survival_rate",
        "post_payout_survival_rate",
        "median_trader_net_payout",
        "minimum_mll_buffer",
    )
    mismatch = [field for field in exact if baseline[field] != replay[field]]
    mismatch.extend(
        field
        for field in numeric
        if abs(float(baseline[field]) - float(replay[field])) > 1e-9
    )
    return mismatch


def _combine_metrics(summary: Any) -> dict[str, Any]:
    return {
        "episode_count": int(summary.episode_start_count),
        "pass_count": int(summary.pass_count),
        "pass_rate": float(summary.pass_rate),
        "mll_breach_count": int(summary.mll_breach_count),
        "mll_breach_rate": float(summary.mll_breach_rate),
        "compliance_failure_count": int(summary.compliance_failure_count),
        "median_episode_net_pnl": float(summary.median_episode_net_pnl),
        "median_target_progress": float(summary.target_progress_median),
        "minimum_mll_buffer": float(summary.minimum_mll_buffer),
        "terminal_distribution": dict(summary.terminal_distribution),
    }


def _xfa_metrics(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "selected_path": str(summary["selected_path"]),
        "episode_count": int(summary["episode_start_count"]),
        "expected_payout_cycles": float(
            summary["expected_payout_cycles_before_ruin"]
        ),
        "payout_probability": float(summary["payout_probability"]),
        "survival_rate": float(summary["survival_rate"]),
        "post_payout_survival_rate": float(
            summary["post_payout_survival_rate"]
        ),
        "median_trader_net_payout": float(summary["median_trader_net_payout"]),
        "minimum_mll_buffer": float(summary["minimum_mll_buffer"]),
    }


def _aggregate_combine(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    episodes = sum(int(row[key]["episode_count"]) for row in rows)
    passes = sum(int(row[key]["pass_count"]) for row in rows)
    breaches = sum(int(row[key]["mll_breach_count"]) for row in rows)
    return {
        "episode_count": episodes,
        "pass_count": passes,
        "pass_rate": passes / episodes,
        "mll_breach_count": breaches,
        "mll_breach_rate": breaches / episodes,
    }


def _aggregate_xfa(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    episodes = sum(int(row[key]["episode_count"]) for row in rows)
    return {
        "episode_count": episodes,
        "mean_policy_payout_probability": sum(
            float(row[key]["payout_probability"]) for row in rows
        )
        / len(rows),
        "mean_policy_expected_cycles": sum(
            float(row[key]["expected_payout_cycles"]) for row in rows
        )
        / len(rows),
        "mean_policy_survival_rate": sum(
            float(row[key]["survival_rate"]) for row in rows
        )
        / len(rows),
    }


def _ambiguity_count(
    components: Mapping[str, Sequence[RoutedTrade]],
) -> int:
    return sum(
        favorable_first_is_ambiguous(
            worst_unrealized_pnl=trade.event.worst_unrealized_pnl,
            best_unrealized_pnl=trade.event.best_unrealized_pnl,
        )
        for trades in components.values()
        for trade in trades
    )


def _verify_source_manifest(root: Path, manifest: Path) -> None:
    expected_manifest_hash = _sha256(manifest)
    if expected_manifest_hash != (
        "aeac4d9f0ba6557d3d57c5078ca9d331091ec379457a668dafd631c7973f770d"
    ):
        raise Phase0DivergenceError("source manifest hash mismatch")
    for raw in manifest.read_text(encoding="utf-8").splitlines():
        expected, relative = raw.split("  ", 1)
        if _sha256(root / relative) != expected:
            raise Phase0DivergenceError(f"source artifact hash mismatch: {relative}")


def _render_divergence_report(result: Mapping[str, Any]) -> str:
    combine = result["combine"]
    xfa = result["xfa"]
    return "\n".join(
        [
            "# HYDRA V7 — Phase 0 MLL divergence",
            "",
            "Validation/simulation diff: "
            + ", ".join(result["diff_validation"]),
            "",
            f"Historical classification: `{result['historical_classification']}`.",
            f"Frozen scope: `{result['basket_count']}` baskets + `{result['xfa_policy_count']}` XFA policies.",
            f"Default episodes/pass/breach: `{combine['default']['episode_count']}` / `{combine['default']['pass_rate']:.6f}` / `{combine['default']['mll_breach_rate']:.6f}`.",
            f"Intraday-HWM episodes/pass/breach: `{combine['sensitivity']['episode_count']}` / `{combine['sensitivity']['pass_rate']:.6f}` / `{combine['sensitivity']['mll_breach_rate']:.6f}`.",
            f"Pass-rate delta: `{combine['pass_rate_delta']:+.6f}`; MLL-breach delta: `{combine['mll_breach_rate_delta']:+.6f}`.",
            f"Terminal transitions: `{combine['terminal_transition_count']}`; unresolved MFE/MAE order observations: `{combine['mfe_mae_order_ambiguity_count']}`.",
            f"XFA mean payout probability default/sensitivity: `{xfa['default']['mean_policy_payout_probability']:.6f}` / `{xfa['sensitivity']['mean_policy_payout_probability']:.6f}`.",
            f"Q4 BURNED: `{result['q4_burned']}`; orders: `{result['outbound_order_count']}`; paid spend: `${result['phase_spend_usd']:.6f}`.",
            "",
            "These are development-only sensitivity results, not promotion evidence.",
            "",
        ]
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["Phase0DivergenceError", "run_phase0_divergence"]
