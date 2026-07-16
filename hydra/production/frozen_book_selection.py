"""Deterministic, development-only selection from the sealed 0026 report.

This module is deliberately downstream of the decision report.  It cannot
read replay caches, mutate a policy, or create a new candidate.  The sealed
report is the sole input and the selection receipt is the sole commit marker.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.compute.result_writer import AtomicResultWriter
from hydra.production.active_risk_report_seal import (
    REPORT_JSON_NAME,
    REPORT_RECEIPT_NAME,
    verify_active_risk_decision_report_seal,
)


CAMPAIGN_ID = "hydra_active_risk_pool_target_velocity_0026"
SELECTION_SCHEMA = "hydra_frozen_book_selection_v1"
SELECTION_RECEIPT_SCHEMA = "hydra_frozen_book_selection_seal_v1"
SELECTION_JSON_NAME = "frozen_book_selection_revision_02.json"
SELECTION_RECEIPT_NAME = "frozen_book_selection_revision_02_seal_receipt.json"
EXPECTED_FINALISTS = 8
BLOCK_IDS = ("B1", "B2", "B3", "B4")
SCENARIOS = ("normal", "stressed")
XFA_PATHS = ("standard", "consistency")
FULL_HORIZON = "FULL_CHRONOLOGICAL_HORIZON"
CUMULATIVE_CLUSTER_SCOPE = (
    "CUMULATIVE_192_STARTS_PER_SCENARIO_STAGE3_48_PLUS_STAGE4_48_"
    "PLUS_STAGE5_96_ACCOUNT_TRAJECTORY_ROUTING_SUPPRESSION_AND_"
    "ADMITTED_TRADE_BEHAVIOR"
)
CUMULATIVE_BEHAVIOR_SCOPE = (
    "EXACT_CUMULATIVE_STAGE3_PLUS_STAGE4_PLUS_STAGE5_CANONICAL_90_DAY_"
    "ACCOUNT_TRAJECTORY_ROUTING_SUPPRESSION_AND_ADMITTED_TRADES"
)
CUMULATIVE_FINGERPRINT_SCHEMA = (
    "hydra_expanded_finalist_cumulative_account_trade_behavior_v1"
)
CUMULATIVE_FEATURE_SCHEMA = (
    "hydra_expanded_finalist_cumulative_behavior_features_v1"
)
CUMULATIVE_CLUSTER_THRESHOLDS = {
    "minimum_account_vector_correlation": 0.995,
    "maximum_account_vector_rmse": 0.05,
    "minimum_terminal_agreement": 0.95,
    "minimum_routing_jaccard": 0.90,
    "minimum_admitted_trade_jaccard": 0.90,
}
CONTROL_NAMES = (
    "static_partition",
    "best_individual_sleeve",
    "equal_risk_active_pool",
    "matched_random_priority",
)


class FrozenBookSelectionError(RuntimeError):
    """The sealed report or requested selection is incomplete or divergent."""


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _number(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise FrozenBookSelectionError(f"{label} is not numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise FrozenBookSelectionError(f"{label} is not numeric") from exc
    if not math.isfinite(result):
        raise FrozenBookSelectionError(f"{label} is not finite")
    return result


def _count(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FrozenBookSelectionError(f"{label} is not a non-negative integer")
    return value


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FrozenBookSelectionError(f"{label} is absent or malformed")
    return value


def _utc_timestamp(value: Any) -> str:
    text = str(value or "")
    if not text.endswith("Z"):
        raise FrozenBookSelectionError("selection_completed_at_utc must end in Z")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise FrozenBookSelectionError(
            "selection_completed_at_utc is not an ISO-8601 UTC timestamp"
        ) from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise FrozenBookSelectionError("selection timestamp is not UTC")
    return text


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _read_sealed_report(report_dir: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    root = Path(report_dir).resolve()
    verified = dict(verify_active_risk_decision_report_seal(root))
    try:
        report = json.loads((root / REPORT_JSON_NAME).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FrozenBookSelectionError("sealed decision report is unreadable") from exc
    if not isinstance(report, dict):
        raise FrozenBookSelectionError("sealed decision report is malformed")
    return report, verified


def _validate_hash(value: Mapping[str, Any], field: str, label: str) -> None:
    claimed = str(value.get(field) or "")
    body = dict(value)
    body.pop(field, None)
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise FrozenBookSelectionError(f"{label} {field} mismatch")


def _frozen_specs(report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    section = _mapping(report.get("frozen_finalist_policy_specs"), "frozen specs")
    raw = section.get("policy_specs")
    if section.get("finalist_count") != EXPECTED_FINALISTS or not isinstance(raw, list):
        raise FrozenBookSelectionError("sealed report must contain exactly 8 frozen specs")
    output: dict[str, dict[str, Any]] = {}
    for item in raw:
        spec = dict(_mapping(item, "frozen finalist spec"))
        policy_id = str(spec.get("policy_id") or "")
        if not policy_id or policy_id in output:
            raise FrozenBookSelectionError("frozen finalist identity is empty or duplicated")
        policy = _mapping(spec.get("active_risk_policy"), f"{policy_id} policy")
        membership = spec.get("membership")
        if (
            not isinstance(membership, list)
            or len(membership) != int(spec.get("membership_row_count", -1))
            or spec.get("membership_rows_all_contain_identical_policy") is not True
        ):
            raise FrozenBookSelectionError(f"{policy_id} membership is not fully frozen")
        expected = {
            "active_risk_policy_sha256": policy,
            "membership_sha256": membership,
            "combine_book_sha256": _mapping(spec.get("combine_book"), f"{policy_id} Combine book"),
            "xfa_standard_book_sha256": _mapping(spec.get("xfa_standard_book"), f"{policy_id} Standard book"),
            "xfa_consistency_book_sha256": _mapping(spec.get("xfa_consistency_book"), f"{policy_id} Consistency book"),
        }
        for field, payload in expected.items():
            if str(spec.get(field) or "") != stable_hash(payload):
                raise FrozenBookSelectionError(f"{policy_id} {field} mismatch")
        if (
            str(policy.get("policy_id") or "") != policy_id
            or str(policy.get("structural_fingerprint") or "")
            != str(spec.get("structural_fingerprint") or "")
            or spec["combine_book"].get("book") != "COMBINE_BOOK"
            or spec["xfa_standard_book"].get("book") != "XFA_STANDARD_BOOK"
            or spec["xfa_consistency_book"].get("book") != "XFA_CONSISTENCY_BOOK"
            or spec["xfa_standard_book"].get("book_frozen_before_outcomes") is not True
            or spec["xfa_consistency_book"].get("book_frozen_before_outcomes") is not True
        ):
            raise FrozenBookSelectionError(f"{policy_id} frozen book identity drift")
        output[policy_id] = spec
    return output


def _cumulative_behavior_fingerprint(row: Mapping[str, Any]) -> str:
    """Validate the exact public 48+48+96 behavior identity for one finalist."""

    policy_id = str(row.get("policy_id") or "")
    public = _mapping(
        row.get("cumulative_account_trade_behavior"),
        f"{policy_id} cumulative account/trade behavior",
    )
    fingerprint = str(
        public.get("authoritative_raw_account_trade_behavior_fingerprint") or ""
    )
    sha_fields = (
        fingerprint,
        str(public.get("episode_key_sha256") or ""),
        str(public.get("feature_vector_sha256") or ""),
        str(public.get("routing_decision_tuple_sha256") or ""),
        str(public.get("admitted_trade_tuple_sha256") or ""),
    )
    if any(not _is_sha256(value) for value in sha_fields):
        raise FrozenBookSelectionError(
            f"{policy_id} cumulative behavior lacks deterministic SHA-256 bindings"
        )
    if (
        public.get("scope") != CUMULATIVE_BEHAVIOR_SCOPE
        or public.get("fingerprint_schema") != CUMULATIVE_FINGERPRINT_SCHEMA
        or public.get("feature_schema") != CUMULATIVE_FEATURE_SCHEMA
        or public.get("observation_count") != 384
        or public.get("per_scenario_observation_count")
        != {"normal": 192, "stressed": 192}
        or public.get("stage_start_counts")
        != {
            "stage3": {"normal": 48, "stressed": 48},
            "stage4": {"normal": 48, "stressed": 48},
            "stage5": {"normal": 96, "stressed": 96},
        }
        or public.get("full_daily_account_trajectory_bound") is not True
        or public.get("emitted_routing_and_suppression_bound") is not True
        or public.get("admitted_source_trade_contribution_bound") is not True
        or public.get("policy_id_excluded_from_behavior_fingerprint") is not True
        or row.get("legacy_frontier_behavior_fingerprint_rederived_exactly")
        is not True
        or str(row.get("sealed_cumulative_account_behavior_fingerprint") or "")
        != fingerprint
        or str(
            row.get(
                "authoritative_cumulative_account_trade_behavior_fingerprint"
            )
            or ""
        )
        != fingerprint
    ):
        raise FrozenBookSelectionError(
            f"{policy_id} cumulative account/trade behavior identity drift"
        )
    return fingerprint


def _path_metrics(row: Mapping[str, Any]) -> dict[str, Any]:
    lifecycle = _mapping(
        row.get("expanded_standard_consistency_xfa_lifecycle_exact"),
        f"{row.get('policy_id')} XFA lifecycle",
    )
    if lifecycle.get("paths_are_alternative_not_additive") is not True:
        raise FrozenBookSelectionError("Standard and Consistency must be alternative paths")
    output: dict[str, Any] = {}
    horizons = _mapping(row.get("horizons"), f"{row.get('policy_id')} horizons")
    for scenario in SCENARIOS:
        scenario_value = _mapping(lifecycle.get(scenario), f"{scenario} XFA")
        full = _mapping(
            _mapping(horizons.get(scenario), f"{scenario} horizons").get(
                FULL_HORIZON
            ),
            f"{scenario} full-horizon Combine result",
        )
        full_attempts = _count(
            full.get("episode_count"), "full-horizon Combine attempts"
        )
        full_passes = _count(full.get("pass_count"), "full-horizon Combine passes")
        if full_attempts != 192 or full_passes > full_attempts:
            raise FrozenBookSelectionError(
                "full-horizon Combine counts are inconsistent with 192 starts"
            )
        output[scenario] = {}
        transitions: int | None = None
        for path in XFA_PATHS:
            value = _mapping(scenario_value.get(path), f"{scenario} {path} XFA")
            attempts = _count(value.get("combine_attempts"), "XFA combine attempts")
            if attempts != 192:
                raise FrozenBookSelectionError("each finalist XFA path must cover 192 starts")
            paths_started = _count(value.get("xfa_paths_started"), "XFA paths started")
            first_payouts = _count(value.get("first_payouts"), "first payouts")
            closures = _count(
                value.get("closure_before_first_payout_count"),
                "pre-payout closures",
            )
            post_payout_survival = _count(
                value.get("post_payout_survival_count"),
                "post-payout survival count",
            )
            trader_net_payout = _number(
                value.get("trader_net_payout"), "XFA trader net payout"
            )
            if paths_started != full_passes or first_payouts > paths_started:
                raise FrozenBookSelectionError(
                    "XFA transitions or first payouts diverge from Combine passes"
                )
            if (
                closures > paths_started
                or first_payouts + closures > paths_started
                or post_payout_survival > first_payouts
                or trader_net_payout < 0.0
            ):
                raise FrozenBookSelectionError(
                    "XFA closure, survival, or trader-payout accounting drift"
                )
            if transitions is None:
                transitions = paths_started
            elif transitions != paths_started:
                raise FrozenBookSelectionError(
                    "Standard and Consistency transition denominators differ"
                )
            lower = _mapping(value.get("unconditional_lower_bound"), "XFA lower bound")
            denominators = _mapping(lower.get("denominators"), "XFA denominators")
            if (
                _count(denominators.get("combine_attempts"), "XFA denominator") != attempts
                or _count(denominators.get("xfa_paths_started"), "XFA denominator")
                != paths_started
                or _count(denominators.get("first_payout_paths"), "XFA denominator")
                != first_payouts
            ):
                raise FrozenBookSelectionError("XFA denominator linkage drift")
            expected_probabilities = {
                "combine_pass_probability": paths_started / attempts,
                "first_payout_probability_conditional_on_combine_pass": (
                    first_payouts / paths_started if paths_started else 0.0
                ),
                "first_payout_probability_per_combine_attempt": (
                    first_payouts / attempts
                ),
                "expected_trader_payout_per_combine_attempt": (
                    trader_net_payout / attempts
                ),
                "post_payout_survival_probability_conditional_on_first_payout": (
                    post_payout_survival / first_payouts
                    if first_payouts
                    else 0.0
                ),
            }
            for field, expected in expected_probabilities.items():
                if not math.isclose(
                    _number(lower.get(field), f"XFA {field}"),
                    expected,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                ):
                    raise FrozenBookSelectionError(
                        f"XFA {field} denominator arithmetic drift"
                    )
            output[scenario][path] = {
                "combine_attempts": attempts,
                "xfa_paths_started": paths_started,
                "first_payout_paths": first_payouts,
                "expected_trader_payout_per_combine_attempt": _number(
                    lower.get("expected_trader_payout_per_combine_attempt"), "XFA payout EV"
                ),
                "post_payout_survival_probability_conditional_on_first_payout": _number(
                    lower.get("post_payout_survival_probability_conditional_on_first_payout"),
                    "XFA survival",
                ),
                "payout_cycles": _count(value.get("payout_cycles"), "payout cycles"),
                "closure_before_first_payout_count": closures,
                "denominators": dict(denominators),
            }
    return output


def _concentration(scenario: Mapping[str, Any]) -> dict[str, float]:
    values = _mapping(scenario.get("concentration"), "concentration")
    trade = _mapping(values.get("trade_concentration"), "trade concentration")
    day = _mapping(scenario.get("day_concentration_exact"), "day concentration")
    output = {
        "block": _number(values.get("maximum_block_positive_profit_share"), "block concentration"),
        "day": _number(day.get("maximum_positive_session_day_aggregate_share"), "day concentration"),
        "trade": max(
            _number(trade.get("maximum_positive_source_trade_observation_share"), "source-trade concentration"),
            _number(trade.get("maximum_positive_single_account_trade_observation_share"), "account-trade concentration"),
        ),
        "market": _number(values.get("maximum_market_positive_profit_share"), "market concentration"),
        "sleeve": _number(values.get("maximum_sleeve_positive_profit_share"), "sleeve concentration"),
    }
    if any(not 0.0 <= value <= 1.0 + 1e-12 for value in output.values()):
        raise FrozenBookSelectionError("concentration is outside [0,1]")
    return output


def _candidate_evidence(
    row: Mapping[str, Any], spec: Mapping[str, Any]
) -> dict[str, Any]:
    policy_id = str(row.get("policy_id") or "")
    if int(row.get("starts_per_scenario", -1)) != 192:
        raise FrozenBookSelectionError(f"{policy_id} does not have 192 starts/scenario")
    source_blocks = list(row.get("source_block_ids") or ())
    if (
        int(row.get("effective_independent_source_block_count", -1)) != 4
        or len(source_blocks) != 4
        or len(set(source_blocks)) != 4
        or set(source_blocks) != set(BLOCK_IDS)
    ):
        raise FrozenBookSelectionError(f"{policy_id} B1-B4 coverage drift")
    control = _mapping(row.get("stage3_matched_control_deltas"), "matched controls")
    if (
        control.get("scope") != "STAGE3_ONLY_48_MATCHED_STARTS"
        or control.get("matched_starts_per_scenario") != 48
        or control.get("expanded_192_status")
        != "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED"
    ):
        raise FrozenBookSelectionError(f"{policy_id} control scope drift")
    control_deltas = _mapping(control.get("deltas"), "control deltas")
    control_improvement: dict[str, Any] = {}
    for name in CONTROL_NAMES:
        candidate_control = _mapping(control_deltas.get(name), f"{name} control")
        control_improvement[name] = {}
        for scenario in SCENARIOS:
            delta = _mapping(candidate_control.get(scenario), f"{name} {scenario}")
            control_improvement[name][scenario] = {
                "target_progress_p25_delta": _number(delta.get("target_progress_p25"), "control target p25"),
                "net_total_delta": _number(delta.get("net_total"), "control net total"),
                "pass_rate_delta": _number(delta.get("pass_rate"), "control pass rate"),
            }

    scenario_metrics: dict[str, Any] = {}
    for scenario in SCENARIOS:
        value = _mapping(row.get(scenario), f"{policy_id} {scenario}")
        aggregate_episodes = _count(value.get("episode_count"), "episode count")
        aggregate_passes = _count(value.get("pass_count"), "pass count")
        if aggregate_episodes != 192 or aggregate_passes > aggregate_episodes:
            raise FrozenBookSelectionError(
                f"{policy_id} {scenario} aggregate episode/pass count drift"
            )
        blocks = _mapping(value.get("block_evidence_exact"), "block evidence")
        if set(blocks) != set(BLOCK_IDS):
            raise FrozenBookSelectionError(f"{policy_id} {scenario} block coverage drift")
        pass_blocks: list[str] = []
        positive_blocks: list[str] = []
        block_view: dict[str, Any] = {}
        block_episode_sum = 0
        block_pass_sum = 0
        for block_id in BLOCK_IDS:
            block = _mapping(blocks[block_id], f"{policy_id} {block_id}")
            passes = _count(block.get("pass_count"), "block pass count")
            episodes = _count(block.get("episode_count"), "block episodes")
            if passes > episodes:
                raise FrozenBookSelectionError(
                    f"{policy_id} {scenario} {block_id} passes exceed episodes"
                )
            block_episode_sum += episodes
            block_pass_sum += passes
            net = _mapping(block.get("net_pnl"), "block net")
            target = _mapping(block.get("target_progress"), "block target")
            net_mean = _number(net.get("mean"), "block net mean")
            target_median = _number(target.get("median"), "block target median")
            if passes:
                pass_blocks.append(block_id)
            if net_mean > 0.0 and target_median > 0.0:
                positive_blocks.append(block_id)
            block_view[block_id] = {
                "episode_count": episodes,
                "pass_count": passes,
                "net_pnl_mean": net_mean,
                "target_progress_median": target_median,
            }
        if block_episode_sum != aggregate_episodes or block_pass_sum != aggregate_passes:
            raise FrozenBookSelectionError(
                f"{policy_id} {scenario} block counts do not reconcile to aggregate"
            )
        scenario_metrics[scenario] = {
            "pass_count": aggregate_passes,
            "net_pnl_total": _number(value.get("net_total"), "net total"),
            "target_progress_p25": _number(value.get("target_progress_p25"), "target p25"),
            "minimum_mll_buffer": _number(value.get("minimum_mll_buffer"), "MLL buffer"),
            "mll_breach_rate": _number(value.get("mll_breach_rate"), "MLL breach rate"),
            "consistency_rate": _number(value.get("consistency_rate"), "consistency rate"),
            "pass_block_ids": pass_blocks,
            "positive_economic_block_ids": positive_blocks,
            "positive_economic_block_definition": "NET_PNL_MEAN_GT_ZERO_AND_TARGET_PROGRESS_MEDIAN_GT_ZERO;NOT_A_CONTROL_DELTA",
            "blocks": block_view,
            "concentration": _concentration(value),
        }
    lifecycle = _path_metrics(row)
    policy = _mapping(spec.get("active_risk_policy"), f"{policy_id} policy")
    complexity = {
        "maximum_concurrent_sleeves": int(policy.get("maximum_concurrent_sleeves", -1)),
        "nondefault_mode_count": sum(
            (
                str(policy.get("concurrency_scaling")) != "PRIORITY",
                str(policy.get("same_instrument_conflict_rule")) != "PRIORITY",
                str(policy.get("target_protection_mode")) != "NONE",
            )
        ),
    }
    stressed_controls = sum(
        int(
            control_improvement[name]["stressed"]["target_progress_p25_delta"] > 0.0
            and control_improvement[name]["stressed"]["net_total_delta"] > 0.0
        )
        for name in CONTROL_NAMES
    )
    return {
        "policy_id": policy_id,
        "expanded_exact_account_behavior_cluster": str(
            row.get("expanded_exact_account_behavior_cluster") or ""
        ),
        "expanded_economic_behavior_cluster": str(
            row.get("expanded_economic_behavior_cluster") or ""
        ),
        "stage3_posthoc_behavioral_cluster": str(
            row.get("stage3_posthoc_behavioral_cluster") or ""
        ),
        "normal": scenario_metrics["normal"],
        "stressed": scenario_metrics["stressed"],
        "stage3_control_comparisons": {
            "scope": "STAGE3_ONLY_48_MATCHED_STARTS",
            "expanded_192_status": "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED",
            "by_control": control_improvement,
            "stressed_joint_net_and_target_improvement_control_count": stressed_controls,
        },
        "xfa_paths": lifecycle,
        "operational_simplicity": complexity,
    }


def _pareto_vector(evidence: Mapping[str, Any]) -> tuple[float, ...]:
    stressed = evidence["stressed"]
    normal = evidence["normal"]
    xfa = evidence["xfa_paths"]["stressed"]
    concentration = stressed["concentration"]
    simplicity = evidence["operational_simplicity"]
    return (
        float(len(stressed["pass_block_ids"])),
        float(len(stressed["positive_economic_block_ids"])),
        float(len(normal["pass_block_ids"])),
        float(evidence["stage3_control_comparisons"]["stressed_joint_net_and_target_improvement_control_count"]),
        stressed["target_progress_p25"],
        stressed["net_pnl_total"],
        -stressed["mll_breach_rate"],
        stressed["minimum_mll_buffer"],
        xfa["standard"]["expected_trader_payout_per_combine_attempt"],
        xfa["consistency"]["expected_trader_payout_per_combine_attempt"],
        xfa["standard"]["post_payout_survival_probability_conditional_on_first_payout"],
        xfa["consistency"]["post_payout_survival_probability_conditional_on_first_payout"],
        -concentration["block"],
        -concentration["day"],
        -concentration["trade"],
        -concentration["market"],
        -concentration["sleeve"],
        -float(simplicity["maximum_concurrent_sleeves"]),
        -float(simplicity["nondefault_mode_count"]),
    )


def _dominates(left: Sequence[float], right: Sequence[float]) -> bool:
    return all(a >= b for a, b in zip(left, right, strict=True)) and any(
        a > b for a, b in zip(left, right, strict=True)
    )


def _pareto_layers(evidence: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    remaining = set(evidence)
    output: dict[str, int] = {}
    layer = 1
    vectors = {key: _pareto_vector(value) for key, value in evidence.items()}
    while remaining:
        frontier = sorted(
            candidate
            for candidate in remaining
            if not any(
                other != candidate
                and _dominates(vectors[other], vectors[candidate])
                for other in remaining
            )
        )
        if not frontier:
            raise FrozenBookSelectionError("Pareto layering failed")
        for candidate in frontier:
            output[candidate] = layer
        remaining.difference_update(frontier)
        layer += 1
    return output


def _lexicographic_key(policy_id: str, evidence: Mapping[str, Any]) -> tuple[Any, ...]:
    stressed = evidence["stressed"]
    normal = evidence["normal"]
    xfa = evidence["xfa_paths"]["stressed"]
    concentration = stressed["concentration"]
    simplicity = evidence["operational_simplicity"]
    # Aggregate pass count is intentionally absent.  Block diversity is first.
    return (
        -len(stressed["pass_block_ids"]),
        -len(stressed["positive_economic_block_ids"]),
        -len(normal["pass_block_ids"]),
        -evidence["stage3_control_comparisons"]["stressed_joint_net_and_target_improvement_control_count"],
        -stressed["target_progress_p25"],
        -stressed["net_pnl_total"],
        stressed["mll_breach_rate"],
        -stressed["minimum_mll_buffer"],
        -min(
            xfa["standard"]["expected_trader_payout_per_combine_attempt"],
            xfa["consistency"]["expected_trader_payout_per_combine_attempt"],
        ),
        -xfa["standard"]["expected_trader_payout_per_combine_attempt"],
        -xfa["consistency"]["expected_trader_payout_per_combine_attempt"],
        -xfa["standard"]["post_payout_survival_probability_conditional_on_first_payout"],
        -xfa["consistency"]["post_payout_survival_probability_conditional_on_first_payout"],
        concentration["block"],
        concentration["day"],
        concentration["trade"],
        concentration["market"],
        concentration["sleeve"],
        simplicity["maximum_concurrent_sleeves"],
        simplicity["nondefault_mode_count"],
        policy_id,
    )


def _report_bindings(
    report: Mapping[str, Any], receipt: Mapping[str, Any], report_dir: Path
) -> dict[str, Any]:
    artifact = _mapping(
        _mapping(receipt.get("artifacts"), "report artifacts").get(REPORT_JSON_NAME),
        "report JSON artifact",
    )
    return {
        "report_hash": str(report.get("report_hash") or ""),
        "json_sha256": str(artifact.get("sha256") or ""),
        "seal_receipt_hash": str(receipt.get("receipt_hash") or ""),
        "seal_receipt_sha256": _file_sha256(report_dir / REPORT_RECEIPT_NAME),
        "sealed_at_utc": str(receipt.get("sealed_at_utc") or ""),
        "seal_receipt_name": REPORT_RECEIPT_NAME,
        "report_json_name": REPORT_JSON_NAME,
        "verified_before_selection": True,
    }


def _validate_pairwise_clustering(
    clustering: Mapping[str, Any], candidate_ids: Sequence[str]
) -> tuple[list[list[str]], dict[tuple[str, str], dict[str, Any]]]:
    """Rebuild the report's lexicographic greedy complete-link partition."""

    ordered = sorted(str(value) for value in candidate_ids)
    expected_pairs = [
        (left, right)
        for index, left in enumerate(ordered)
        for right in ordered[index + 1 :]
    ]
    pairwise = clustering.get("pairwise_diagnostics")
    if (
        not isinstance(pairwise, list)
        or len(pairwise) != len(expected_pairs)
        or clustering.get("pairwise_diagnostic_count") != len(expected_pairs)
        or clustering.get("expected_pairwise_diagnostic_count")
        != len(expected_pairs)
        or clustering.get("pairwise_coverage_complete") is not True
        or clustering.get("pairwise_similarity_decisions_metric_rederived")
        is not True
        or dict(
            _mapping(
                clustering.get("complete_link_thresholds"),
                "complete-link thresholds",
            )
        )
        != CUMULATIVE_CLUSTER_THRESHOLDS
        or str(clustering.get("pairwise_diagnostics_sha256") or "")
        != stable_hash(pairwise)
    ):
        raise FrozenBookSelectionError(
            "cumulative pairwise matrix count, flags, thresholds, or hash drift"
        )
    expected_fields = {
        "left_policy_id",
        "right_policy_id",
        "account_vector_correlation",
        "account_vector_rmse",
        "terminal_agreement",
        "routing_jaccard",
        "admitted_trade_jaccard",
        "similar",
    }
    decisions: dict[tuple[str, str], bool] = {}
    diagnostics: dict[tuple[str, str], dict[str, Any]] = {}
    for expected_pair, raw in zip(expected_pairs, pairwise, strict=True):
        row = _mapping(raw, "cumulative pairwise diagnostic")
        pair = (
            str(row.get("left_policy_id") or ""),
            str(row.get("right_policy_id") or ""),
        )
        if set(row) != expected_fields or pair != expected_pair:
            raise FrozenBookSelectionError(
                "cumulative pairwise matrix ordering or field coverage drift"
            )
        values = {
            "account_vector_correlation": _number(
                row.get("account_vector_correlation"), "pair correlation"
            ),
            "account_vector_rmse": _number(
                row.get("account_vector_rmse"), "pair RMSE"
            ),
            "terminal_agreement": _number(
                row.get("terminal_agreement"), "pair terminal agreement"
            ),
            "routing_jaccard": _number(
                row.get("routing_jaccard"), "pair routing Jaccard"
            ),
            "admitted_trade_jaccard": _number(
                row.get("admitted_trade_jaccard"),
                "pair admitted-trade Jaccard",
            ),
        }
        if (
            not -1.0 - 1e-12
            <= values["account_vector_correlation"]
            <= 1.0 + 1e-12
            or values["account_vector_rmse"] < 0.0
            or any(
                not 0.0 <= values[field] <= 1.0
                for field in (
                    "terminal_agreement",
                    "routing_jaccard",
                    "admitted_trade_jaccard",
                )
            )
        ):
            raise FrozenBookSelectionError(
                "cumulative pairwise metric is outside its admissible range"
            )
        derived = bool(
            values["account_vector_correlation"]
            >= CUMULATIVE_CLUSTER_THRESHOLDS[
                "minimum_account_vector_correlation"
            ]
            and values["account_vector_rmse"]
            <= CUMULATIVE_CLUSTER_THRESHOLDS["maximum_account_vector_rmse"]
            and values["terminal_agreement"]
            >= CUMULATIVE_CLUSTER_THRESHOLDS["minimum_terminal_agreement"]
            and values["routing_jaccard"]
            >= CUMULATIVE_CLUSTER_THRESHOLDS["minimum_routing_jaccard"]
            and values["admitted_trade_jaccard"]
            >= CUMULATIVE_CLUSTER_THRESHOLDS[
                "minimum_admitted_trade_jaccard"
            ]
        )
        if not isinstance(row.get("similar"), bool) or row.get("similar") is not derived:
            raise FrozenBookSelectionError(
                f"cumulative pairwise similarity decision drift for {pair}"
            )
        decisions[pair] = derived
        diagnostics[pair] = values
    groups: list[list[str]] = []
    for candidate_id in ordered:
        for group in groups:
            if all(
                decisions[tuple(sorted((candidate_id, member)))]
                for member in group
            ):
                group.append(candidate_id)
                break
        else:
            groups.append([candidate_id])
    partition = sorted(
        (sorted(group) for group in groups), key=lambda group: tuple(group)
    )
    if (
        clustering.get("complete_link_partition_rederived_from_published_pairwise")
        is not True
        or str(clustering.get("complete_link_partition_sha256") or "")
        != stable_hash(partition)
    ):
        raise FrozenBookSelectionError(
            "cumulative complete-link partition proof or hash drift"
        )
    return partition, diagnostics


def build_frozen_book_selection(
    report_dir: str | Path, *, selection_completed_at_utc: str
) -> dict[str, Any]:
    """Build, but do not publish, the deterministic frozen-book selection."""

    completed = _utc_timestamp(selection_completed_at_utc)
    report_root = Path(report_dir).resolve()
    report, receipt = _read_sealed_report(report_root)
    if (
        report.get("campaign_id") != CAMPAIGN_ID
        or report.get("development_only") is not True
        or report.get("promotion_or_selection_mutated") is not False
    ):
        raise FrozenBookSelectionError("decision report is not sealed development evidence")
    integrity = _mapping(report.get("integrity"), "decision-report integrity")
    for field in (
        "expanded_finalist_decision_metrics_rederived_from_raw_caches",
        "expanded_finalist_runtime_behavior_merge_hash_rederived",
        "expanded_finalist_authoritative_account_trade_behavior_rederived",
        "expanded_finalist_cumulative_economic_behavior_clusters_rederived",
    ):
        if integrity.get(field) is not True:
            raise FrozenBookSelectionError(
                f"decision report lacks required integrity proof {field}"
            )
    lifecycle_totals = _mapping(
        report.get("campaign_wide_sealed_xfa_lifecycle_totals"),
        "campaign-wide XFA audit",
    )
    transition_audit = _mapping(
        lifecycle_totals.get("transition_and_alternative_path_audit"),
        "transition/alternative-path audit",
    )
    if (
        transition_audit.get("alternative_path_multiplier") != 2
        or transition_audit.get("transition_to_alternative_path_identity_valid")
        is not True
        or transition_audit.get("first_payout_observations_within_alternative_path_bound")
        is not True
        or transition_audit.get("duplicate_transition_inflation_detected") is not False
        or transition_audit.get("first_payout_observations_are_combine_to_xfa_transitions")
        is not False
    ):
        raise FrozenBookSelectionError(
            "campaign-wide Standard/Consistency path audit is not decision-grade"
        )
    expanded = _mapping(report.get("expanded_development_finalists"), "expanded finalists")
    rows = expanded.get("rows")
    if (
        expanded.get("finalist_count") != EXPECTED_FINALISTS
        or not isinstance(rows, list)
        or len(rows) != EXPECTED_FINALISTS
        or expanded.get("controls_scope") != "STAGE3_ONLY_48_STARTS"
        or expanded.get("expanded_matched_controls_status")
        != "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED"
        or expanded.get("independent_evidence_status") != "NOT_INDEPENDENT_CONFIRMATION"
    ):
        raise FrozenBookSelectionError("expanded finalist or control scope drift")
    specs = _frozen_specs(report)
    row_by_id: dict[str, Mapping[str, Any]] = {}
    evidence: dict[str, dict[str, Any]] = {}
    cumulative_fingerprints: dict[str, str] = {}
    cumulative_episode_key_hashes: dict[str, str] = {}
    for raw in rows:
        row = _mapping(raw, "expanded finalist")
        policy_id = str(row.get("policy_id") or "")
        if not policy_id or policy_id in row_by_id or policy_id not in specs:
            raise FrozenBookSelectionError("expanded finalist identity drift")
        if (
            str(row.get("structural_fingerprint") or "")
            != str(specs[policy_id].get("structural_fingerprint") or "")
        ):
            raise FrozenBookSelectionError(f"{policy_id} report/spec fingerprint drift")
        row_by_id[policy_id] = row
        cumulative_fingerprints[policy_id] = _cumulative_behavior_fingerprint(row)
        cumulative_episode_key_hashes[policy_id] = str(
            _mapping(
                row.get("cumulative_account_trade_behavior"),
                f"{policy_id} cumulative behavior",
            ).get("episode_key_sha256")
            or ""
        )
        evidence[policy_id] = _candidate_evidence(row, specs[policy_id])
    if set(row_by_id) != set(specs):
        raise FrozenBookSelectionError("frozen spec coverage differs from finalists")
    if len(set(cumulative_episode_key_hashes.values())) != 1:
        raise FrozenBookSelectionError(
            "cumulative finalists do not share one identical episode-key SHA-256"
        )

    clustering = _mapping(
        expanded.get("cumulative_192_economic_behavioral_clustering"),
        "cumulative 192-start economic clustering",
    )
    cluster_rows = clustering.get("clusters")
    membership = clustering.get("membership")
    if (
        clustering.get("scope") != CUMULATIVE_CLUSTER_SCOPE
        or clustering.get("source_signal_or_trade_ledger_summary_only") is not False
        or clustering.get("algorithm")
        != "DETERMINISTIC_COMPLETE_LINK_FIXED_THRESHOLDS_V1"
        or clustering.get("full_192_start_contract_satisfied") is not True
        or clustering.get("overlapping_starts_claimed_independent") is not False
        or not isinstance(cluster_rows, list)
        or not isinstance(membership, Mapping)
        or set(membership) != set(row_by_id)
    ):
        raise FrozenBookSelectionError(
            "cumulative economic clustering contract or finalist coverage drift"
        )
    derived_partition, pairwise_diagnostics = _validate_pairwise_clustering(
        clustering, sorted(row_by_id)
    )
    declared_groups: dict[str, list[str]] = {}
    observed_members: set[str] = set()
    for raw_cluster in cluster_rows:
        cluster = _mapping(raw_cluster, "cumulative economic cluster")
        cluster_id = str(cluster.get("cluster_id") or "")
        members = [str(value) for value in cluster.get("member_ids") or ()]
        expected_cluster_id = "expanded_economic_behavior_" + stable_hash(
            {
                "members": sorted(members),
                "fingerprints": [
                    cumulative_fingerprints[member]
                    for member in sorted(members)
                    if member in cumulative_fingerprints
                ],
            }
        )[:20]
        if (
            not cluster_id
            or cluster_id in declared_groups
            or cluster_id != expected_cluster_id
            or len(members) != int(cluster.get("member_count", -1))
            or not members
            or members != sorted(members)
            or len(members) != len(set(members))
            or any(member not in row_by_id for member in members)
            or observed_members.intersection(members)
        ):
            raise FrozenBookSelectionError(
                "cumulative economic cluster collection is malformed or overlapping"
            )
        thresholds = _mapping(
            cluster.get("complete_link_thresholds"),
            f"{cluster_id} complete-link thresholds",
        )
        if dict(thresholds) != CUMULATIVE_CLUSTER_THRESHOLDS:
            raise FrozenBookSelectionError(
                f"{cluster_id} complete-link thresholds differ from the report contract"
            )
        diagnostics = {
            "minimum_pair_correlation": _number(
                cluster.get("minimum_pair_correlation"), "cluster correlation"
            ),
            "maximum_pair_rmse": _number(
                cluster.get("maximum_pair_rmse"), "cluster RMSE"
            ),
            "minimum_terminal_agreement": _number(
                cluster.get("minimum_terminal_agreement"),
                "cluster terminal agreement",
            ),
            "minimum_routing_jaccard": _number(
                cluster.get("minimum_routing_jaccard"), "cluster routing Jaccard"
            ),
            "minimum_admitted_trade_jaccard": _number(
                cluster.get("minimum_admitted_trade_jaccard"),
                "cluster admitted-trade Jaccard",
            ),
        }
        if (
            not -1.0 - 1e-12
            <= diagnostics["minimum_pair_correlation"]
            <= 1.0 + 1e-12
            or diagnostics["maximum_pair_rmse"] < 0.0
            or any(
                not 0.0 <= diagnostics[field] <= 1.0
                for field in (
                    "minimum_terminal_agreement",
                    "minimum_routing_jaccard",
                    "minimum_admitted_trade_jaccard",
                )
            )
            or diagnostics["minimum_pair_correlation"]
            < CUMULATIVE_CLUSTER_THRESHOLDS[
                "minimum_account_vector_correlation"
            ]
            or diagnostics["maximum_pair_rmse"]
            > CUMULATIVE_CLUSTER_THRESHOLDS["maximum_account_vector_rmse"]
            or diagnostics["minimum_terminal_agreement"]
            < CUMULATIVE_CLUSTER_THRESHOLDS["minimum_terminal_agreement"]
            or diagnostics["minimum_routing_jaccard"]
            < CUMULATIVE_CLUSTER_THRESHOLDS["minimum_routing_jaccard"]
            or diagnostics["minimum_admitted_trade_jaccard"]
            < CUMULATIVE_CLUSTER_THRESHOLDS[
                "minimum_admitted_trade_jaccard"
            ]
        ):
            raise FrozenBookSelectionError(
                f"{cluster_id} complete-link diagnostics violate frozen thresholds"
            )
        if len(members) == 1 and diagnostics != {
            "minimum_pair_correlation": 1.0,
            "maximum_pair_rmse": 0.0,
            "minimum_terminal_agreement": 1.0,
            "minimum_routing_jaccard": 1.0,
            "minimum_admitted_trade_jaccard": 1.0,
        }:
            raise FrozenBookSelectionError(
                f"{cluster_id} singleton diagnostics are not deterministic"
            )
        if len(members) > 1:
            within = [
                pairwise_diagnostics[tuple(sorted((left, right)))]
                for index, left in enumerate(members)
                for right in members[index + 1 :]
            ]
            rederived_cluster_diagnostics = {
                "minimum_pair_correlation": min(
                    value["account_vector_correlation"] for value in within
                ),
                "maximum_pair_rmse": max(
                    value["account_vector_rmse"] for value in within
                ),
                "minimum_terminal_agreement": min(
                    value["terminal_agreement"] for value in within
                ),
                "minimum_routing_jaccard": min(
                    value["routing_jaccard"] for value in within
                ),
                "minimum_admitted_trade_jaccard": min(
                    value["admitted_trade_jaccard"] for value in within
                ),
            }
            if any(
                not math.isclose(
                    diagnostics[field], expected, rel_tol=1e-12, abs_tol=1e-12
                )
                for field, expected in rederived_cluster_diagnostics.items()
            ):
                raise FrozenBookSelectionError(
                    f"{cluster_id} aggregate diagnostics diverge from pairwise matrix"
                )
        expected_report_representative = min(
            members,
            key=lambda member: (
                -evidence[member]["stressed"]["pass_count"] / 192.0,
                -evidence[member]["stressed"]["target_progress_p25"],
                -evidence[member]["stressed"]["net_pnl_total"],
                evidence[member]["stressed"]["mll_breach_rate"],
                member,
            ),
        )
        if cluster.get("representative_id") != expected_report_representative:
            raise FrozenBookSelectionError(
                f"{cluster_id} report representative does not match frozen method"
            )
        declared_groups[cluster_id] = sorted(members)
        observed_members.update(members)
    if (
        observed_members != set(row_by_id)
        or int(clustering.get("cluster_count", -1)) != len(declared_groups)
        or int(expanded.get("cumulative_192_economic_behavior_cluster_count", -1))
        != len(declared_groups)
    ):
        raise FrozenBookSelectionError(
            "cumulative economic cluster collection does not cover exactly 8 finalists"
        )
    published_partition = sorted(
        (sorted(members) for members in declared_groups.values()),
        key=lambda group: tuple(group),
    )
    if published_partition != derived_partition:
        raise FrozenBookSelectionError(
            "published economic clusters differ from rederived greedy complete-link partition"
        )
    fingerprint_membership: dict[str, str] = {}
    for policy_id, value in evidence.items():
        cluster_id = str(membership.get(policy_id) or "")
        if (
            cluster_id not in declared_groups
            or policy_id not in declared_groups[cluster_id]
            or value["expanded_economic_behavior_cluster"] != cluster_id
        ):
            raise FrozenBookSelectionError(
                f"{policy_id} cumulative economic cluster membership drift"
            )
        fingerprint = cumulative_fingerprints[policy_id]
        prior = fingerprint_membership.setdefault(fingerprint, cluster_id)
        if prior != cluster_id:
            raise FrozenBookSelectionError(
                "identical raw cumulative behavior fingerprints were split across "
                "economic clusters"
            )

    exact_groups: dict[str, list[str]] = {}
    for policy_id, value in evidence.items():
        cluster = value["expanded_exact_account_behavior_cluster"]
        if not cluster:
            raise FrozenBookSelectionError(f"{policy_id} exact behavior cluster is absent")
        exact_groups.setdefault(cluster, []).append(policy_id)
    economic_groups = declared_groups
    if len(economic_groups) < 4:
        raise FrozenBookSelectionError(
            "at least four cumulative economic behavior clusters are required"
        )
    candidate_pareto = _pareto_layers(evidence)
    representatives: list[str] = []
    for members in economic_groups.values():
        representatives.append(
            min(
                members,
                key=lambda policy_id: (
                    candidate_pareto[policy_id],
                    _lexicographic_key(policy_id, evidence[policy_id]),
                ),
            )
        )
    representative_pareto = _pareto_layers(
        {policy_id: evidence[policy_id] for policy_id in representatives}
    )
    representatives.sort(
        key=lambda policy_id: (
            representative_pareto[policy_id],
            _lexicographic_key(policy_id, evidence[policy_id]),
        )
    )
    primary_count = min(5, len(representatives) - 1)
    if primary_count < 3:
        raise FrozenBookSelectionError("selection cannot retain 3 primaries and a backup")

    primaries = representatives[:primary_count]
    remaining = [policy_id for policy_id in representatives if policy_id not in primaries]
    backup = remaining[0]

    def selected_entry(policy_id: str, role: str) -> dict[str, Any]:
        spec = specs[policy_id]
        entry: dict[str, Any] = {
            "policy_id": policy_id,
            "selection_role": role,
            "expanded_exact_account_behavior_cluster": evidence[policy_id][
                "expanded_exact_account_behavior_cluster"
            ],
            "expanded_economic_behavior_cluster": evidence[policy_id][
                "expanded_economic_behavior_cluster"
            ],
            "stage3_posthoc_behavioral_cluster": evidence[policy_id][
                "stage3_posthoc_behavioral_cluster"
            ],
            "status": "FORWARD_SHADOW_CANDIDATE",
            "structural_fingerprint": spec["structural_fingerprint"],
            "active_risk_policy_sha256": spec["active_risk_policy_sha256"],
            "membership_sha256": spec["membership_sha256"],
            "combine_book_sha256": spec["combine_book_sha256"],
            "xfa_standard_book_sha256": spec["xfa_standard_book_sha256"],
            "xfa_consistency_book_sha256": spec["xfa_consistency_book_sha256"],
            "frozen_policy_specification": spec,
            "frozen_policy_specification_sha256": stable_hash(spec),
            "pareto_layer_among_economic_cluster_representatives": (
                representative_pareto[policy_id]
            ),
            "pareto_layer_among_all_finalists": candidate_pareto[policy_id],
            "selection_evidence": evidence[policy_id],
        }
        entry["entry_hash"] = stable_hash(entry)
        return entry

    selected = [selected_entry(policy_id, "PRIMARY") for policy_id in primaries]
    selected.append(selected_entry(backup, "BACKUP"))
    manifest: dict[str, Any] = {
        "schema": SELECTION_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "selection_completed_at_utc": completed,
        "source_decision_report": _report_bindings(report, receipt, report_root),
        "evidence_status": "DEVELOPMENT_ONLY_NOT_INDEPENDENT_CONFIRMATION",
        "maximum_candidate_status": "FORWARD_SHADOW_CANDIDATE",
        "paper_shadow_ready_assigned": False,
        "control_contract": {
            "scope": "STAGE3_ONLY_48_STARTS",
            "expanded_192_status": "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED",
            "control_superiority_used_at_192": False,
        },
        "ranking_policy": {
            "method": "WITHIN_CUMULATIVE_ECONOMIC_CLUSTER_REPRESENTATIVE_THEN_REPRESENTATIVE_PARETO_LAYERS_AND_TRANSPARENT_LEXICOGRAPHIC_V1",
            "aggregate_pass_count_primary_rank": False,
            "first_priority": "B1_B2_B3_B4_PASS_AND_POSITIVE_ECONOMIC_DIVERSITY",
            "then": [
                "STAGE3_ONLY_MATCHED_CONTROL_NET_AND_TARGET_IMPROVEMENT_COUNT",
                "STRESSED_TARGET_PROGRESS_P25",
                "STRESSED_NET_PNL",
                "MLL_BREACH_AND_MINIMUM_BUFFER",
                "STANDARD_EXPECTED_PAYOUT_PER_ATTEMPT_SEPARATE",
                "CONSISTENCY_EXPECTED_PAYOUT_PER_ATTEMPT_SEPARATE",
                "STANDARD_AND_CONSISTENCY_POST_PAYOUT_SURVIVAL_SEPARATE",
                "LOW_BLOCK_DAY_TRADE_MARKET_SLEEVE_CONCENTRATION",
                "OPERATIONAL_SIMPLICITY",
            ],
            "standard_and_consistency_cash_aggregated_as_realisable": False,
            "cumulative_economic_cluster_rule": "AT_MOST_ONE_SELECTED_REPRESENTATIVE_ACROSS_PRIMARIES_AND_BACKUP_PER_CUMULATIVE_192_ECONOMIC_BEHAVIOR_CLUSTER",
            "exact_cluster_role": "EXACT_HASH_DIAGNOSTIC_ONLY_NOT_DOWNSTREAM_DIVERSITY_RULE",
            "stage3_similarity_cluster_role": "LEGACY_STAGE3_DIAGNOSTIC_ONLY_NOT_DOWNSTREAM_DIVERSITY_RULE",
            "primary_count_rule": "MIN_5_OR_CUMULATIVE_ECONOMIC_CLUSTER_COUNT_MINUS_ONE;MINIMUM_3;ONE_ECONOMIC_CLUSTER_RESERVED_FOR_BACKUP",
        },
        "finalist_count": EXPECTED_FINALISTS,
        "cumulative_economic_behavior_cluster_count": len(economic_groups),
        "exact_behavior_cluster_count": len(exact_groups),
        "cluster_representatives": representatives,
        "primary_count": len(primaries),
        "backup_count": 1,
        "selected_books": selected,
        "unselected_policy_ids": sorted(set(row_by_id) - set(primaries) - {backup}),
        "forward_contract": {
            "append_only_post_freeze_bars_required": True,
            "no_broker": True,
            "no_orders": True,
            "no_q4_access": True,
            "no_new_data_purchase": True,
            "paper_shadow_ready_prohibited_from_this_evidence": True,
        },
    }
    manifest["selection_manifest_hash"] = stable_hash(manifest)
    return manifest


def _validate_manifest(
    manifest: Mapping[str, Any], report_dir: str | Path
) -> None:
    _validate_hash(manifest, "selection_manifest_hash", "selection manifest")
    expected = build_frozen_book_selection(
        report_dir,
        selection_completed_at_utc=str(manifest.get("selection_completed_at_utc") or ""),
    )
    if dict(manifest) != expected:
        raise FrozenBookSelectionError(
            "selection manifest differs from deterministic sealed-report selection"
        )


def seal_frozen_book_selection(
    manifest: Mapping[str, Any], *, report_dir: str | Path, output_dir: str | Path
) -> Mapping[str, Any]:
    """Publish the immutable selection and its hash-bound receipt last."""

    _validate_manifest(manifest, report_dir)
    root = Path(output_dir).resolve()
    payload = (
        json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    receipt_path = root / SELECTION_RECEIPT_NAME
    if receipt_path.is_file():
        receipt = verify_frozen_book_selection_seal(root, report_dir=report_dir)
        if (root / SELECTION_JSON_NAME).read_bytes() != payload:
            raise FrozenBookSelectionError(
                "existing sealed selection differs from requested immutable manifest"
            )
        return receipt
    report_binding = dict(_mapping(manifest.get("source_decision_report"), "report binding"))
    receipt_body: dict[str, Any] = {
        "schema": SELECTION_RECEIPT_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "selection_manifest_hash": manifest["selection_manifest_hash"],
        "sealed_at_utc": _utc_now(),
        "source_decision_report": report_binding,
        "artifact": {
            "relative_path": SELECTION_JSON_NAME,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
        },
        "publication_contract": {
            "selection_written_atomically_before_receipt": True,
            "receipt_is_commit_marker": True,
            "immutable": True,
        },
    }
    receipt = dict(receipt_body)
    receipt["receipt_hash"] = stable_hash(receipt_body)
    writer = AtomicResultWriter(root, immutable=True)
    writer.write_bytes(SELECTION_JSON_NAME, payload)
    writer.write_json(SELECTION_RECEIPT_NAME, receipt)
    return verify_frozen_book_selection_seal(root, report_dir=report_dir)


def verify_frozen_book_selection_seal(
    output_dir: str | Path, *, report_dir: str | Path
) -> Mapping[str, Any]:
    root = Path(output_dir).resolve()
    try:
        receipt = json.loads((root / SELECTION_RECEIPT_NAME).read_text(encoding="utf-8"))
        payload = (root / SELECTION_JSON_NAME).read_bytes()
        manifest = json.loads(payload)
    except (OSError, json.JSONDecodeError) as exc:
        raise FrozenBookSelectionError("selection seal is incomplete or unreadable") from exc
    if not isinstance(receipt, dict) or not isinstance(manifest, dict):
        raise FrozenBookSelectionError("selection seal is malformed")
    _validate_hash(receipt, "receipt_hash", "selection receipt")
    if receipt.get("schema") != SELECTION_RECEIPT_SCHEMA:
        raise FrozenBookSelectionError("selection receipt schema drift")
    _utc_timestamp(receipt.get("sealed_at_utc"))
    artifact = _mapping(receipt.get("artifact"), "selection artifact")
    if (
        artifact.get("relative_path") != SELECTION_JSON_NAME
        or int(artifact.get("size_bytes", -1)) != len(payload)
        or artifact.get("sha256") != hashlib.sha256(payload).hexdigest()
        or receipt.get("selection_manifest_hash")
        != manifest.get("selection_manifest_hash")
        or dict(_mapping(receipt.get("source_decision_report"), "report binding"))
        != dict(_mapping(manifest.get("source_decision_report"), "report binding"))
    ):
        raise FrozenBookSelectionError("selection artifact/receipt binding drift")
    _validate_manifest(manifest, report_dir)
    return receipt


__all__ = [
    "CAMPAIGN_ID",
    "FrozenBookSelectionError",
    "SELECTION_JSON_NAME",
    "SELECTION_RECEIPT_NAME",
    "SELECTION_RECEIPT_SCHEMA",
    "SELECTION_SCHEMA",
    "build_frozen_book_selection",
    "seal_frozen_book_selection",
    "stable_hash",
    "verify_frozen_book_selection_seal",
]
