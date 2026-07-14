from __future__ import annotations

import json
import math
from dataclasses import dataclass
from statistics import median
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from hydra.selection.selector_manifest import stable_hash


SELECTOR_PROCEDURE_GREEN = "SELECTOR_PROCEDURE_GREEN"
SELECTOR_PROCEDURE_WEAK = "SELECTOR_PROCEDURE_WEAK"
SELECTOR_PROCEDURE_FALSIFIED = "SELECTOR_PROCEDURE_FALSIFIED"

# These are intentionally not function arguments.  The decision boundary is a
# preregistered policy, not a post-result tuning surface.
FROZEN_DECISION_THRESHOLDS: Mapping[str, int | float] = MappingProxyType(
    {
        "minimum_outer_blocks": 4,
        "minimum_positive_stressed_blocks_green": 3,
        "minimum_positive_stressed_blocks_weak": 2,
        "minimum_stressed_passes": 3,
        "minimum_pass_blocks": 2,
        "maximum_mll_breach_rate": 0.10,
        "maximum_block_pass_share": 0.50,
        "maximum_component_profit_share": 0.65,
        "minimum_consistency": 0.75,
    }
)


def frozen_decision_manifest_policy() -> dict[str, dict[str, Any]]:
    """Return the exact human-readable policy executed by this module.

    The runner compares this projection byte-for-byte with the preregistration
    before it loads any selector outcomes.  Keeping the prose-facing manifest
    and the executable scalar boundary in one place prevents a permissive
    manifest validator from silently describing a different experiment.
    """

    return {
        SELECTOR_PROCEDURE_GREEN: {
            "minimum_aggregate_held_out_combine_passes": int(
                FROZEN_DECISION_THRESHOLDS["minimum_stressed_passes"]
            ),
            "minimum_blocks_with_passes": int(
                FROZEN_DECISION_THRESHOLDS["minimum_pass_blocks"]
            ),
            "minimum_positive_economic_blocks": int(
                FROZEN_DECISION_THRESHOLDS[
                    "minimum_positive_stressed_blocks_green"
                ]
            ),
            "positive_aggregate_stressed_net_required": True,
            "normal_and_stressed_improvement_over_best_parent_required": True,
            "held_out_target_progress_improvement_over_best_parent_required": True,
            "maximum_held_out_mll_breach_rate": float(
                FROZEN_DECISION_THRESHOLDS["maximum_mll_breach_rate"]
            ),
            "acceptable_consistency_required": True,
            "minimum_held_out_consistency": float(
                FROZEN_DECISION_THRESHOLDS["minimum_consistency"]
            ),
            "improvement_over_equal_risk_required": True,
            "stronger_than_random_selection_required": True,
            "maximum_single_block_pass_share": float(
                FROZEN_DECISION_THRESHOLDS["maximum_block_pass_share"]
            ),
            "maximum_single_component_profit_share": float(
                FROZEN_DECISION_THRESHOLDS["maximum_component_profit_share"]
            ),
        },
        SELECTOR_PROCEDURE_WEAK: {
            "green_requirements_not_met": True,
            "minimum_positive_economic_blocks": int(
                FROZEN_DECISION_THRESHOLDS[
                    "minimum_positive_stressed_blocks_weak"
                ]
            ),
            "positive_aggregate_stressed_net_required": True,
            "held_out_target_progress_improvement_over_best_parent_required": True,
            "maximum_held_out_mll_breach_rate": float(
                FROZEN_DECISION_THRESHOLDS["maximum_mll_breach_rate"]
            ),
            "any_held_out_improvement_signal_required": True,
        },
        SELECTOR_PROCEDURE_FALSIFIED: {
            "green_requirements_not_met": True,
            "weak_requirements_not_met": True,
            "terminate_static_basket_synthesis": True,
        },
    }

REQUIRED_REPORT_SECTIONS: tuple[tuple[str, str], ...] = (
    (
        "actual_service_state_and_controller_version",
        "Actual service state and controller version",
    ),
    ("campaign_0023_terminal_persistence", "Campaign 0023 terminal persistence"),
    ("temporal_blocks_used", "Temporal blocks used"),
    ("contamination_audit", "Contamination audit"),
    (
        "selector_manifest_and_frozen_ranking",
        "Selector manifest and frozen ranking",
    ),
    ("candidates_available_per_outer_fold", "Candidates available per outer fold"),
    (
        "champion_selected_in_each_design_set",
        "Champion selected in each design set",
    ),
    ("selected_risk_level_per_fold", "Selected risk level per fold"),
    ("held_out_pass_counts", "Held-out pass counts"),
    ("held_out_target_progress_results", "Held-out target-progress results"),
    ("held_out_stressed_net", "Held-out stressed net"),
    ("held_out_mll", "Held-out MLL"),
    ("held_out_consistency", "Held-out consistency"),
    ("best_parent_baseline", "Best-parent baseline"),
    ("equal_risk_baseline", "Equal-risk baseline"),
    ("random_selection_baseline", "Random-selection baseline"),
    ("result_by_independent_block", "Result by independent block"),
    (
        "time_to_target_and_censoring_audit",
        "Time-to-target and censoring audit",
    ),
    (
        "selector_procedure_decision",
        "Selector GREEN / WEAK / FALSIFIED decision",
    ),
    ("final_development_champion", "Final development champion when applicable"),
    ("candidates_promoted_to_96_starts", "Candidates promoted to 96 starts"),
    ("remaining_budget", "Remaining budget"),
    ("q4_status", "Q4 status"),
    ("forward_feed_status", "Forward-feed status"),
    ("current_autonomous_next_action", "Current autonomous next action"),
)


class SelectorReportingError(RuntimeError):
    """Raised when held-out evidence or a selector report is incomplete."""


@dataclass(frozen=True)
class SelectorProcedureDecision:
    status: str
    metrics: Mapping[str, Any]
    checks: Mapping[str, Mapping[str, bool]]
    failure_reasons: tuple[str, ...]
    thresholds: Mapping[str, int | float]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "metrics": _plain_json(self.metrics),
            "checks": _plain_json(self.checks),
            "failure_reasons": list(self.failure_reasons),
            "thresholds": _plain_json(self.thresholds),
        }


def decide_selector_procedure(
    held_out_folds: Sequence[Mapping[str, Any]],
) -> SelectorProcedureDecision:
    """Apply the immutable V7.3 selector decision boundary.

    Each fold must contain ``block_id``, ``selector``, ``best_parent``,
    ``equal_risk``, and ``random_selection``.  The first three policy records
    use the canonical metric names ``normal_net_usd``, ``stressed_net_usd``,
    ``stressed_target_progress``, ``stressed_pass_count``, and
    ``episode_count``.  Selector records additionally contain
    ``mll_breach_count``, ``consistency``, and
    ``maximum_component_profit_share``.  ``random_selection`` is a non-empty
    list of policy records with an additional integer ``seed``; the same fixed
    seed set must appear in every outer fold.
    """

    folds = _validate_and_normalize_folds(held_out_folds)
    selector = _aggregate_policy([fold["selector"] for fold in folds])
    best_parent = _aggregate_policy([fold["best_parent"] for fold in folds])
    equal_risk = _aggregate_policy([fold["equal_risk"] for fold in folds])

    random_by_seed: dict[int, list[Mapping[str, Any]]] = {}
    for fold in folds:
        for row in fold["random_selection"]:
            random_by_seed.setdefault(row["seed"], []).append(row)
    random_aggregates = [
        _aggregate_policy(random_by_seed[seed]) for seed in sorted(random_by_seed)
    ]
    median_random = {
        "normal_net_usd": median(
            row["normal_net_usd"] for row in random_aggregates
        ),
        "stressed_net_usd": median(
            row["stressed_net_usd"] for row in random_aggregates
        ),
        "stressed_target_progress": median(
            row["stressed_target_progress"] for row in random_aggregates
        ),
        "stressed_pass_count": median(
            row["stressed_pass_count"] for row in random_aggregates
        ),
        "fixed_seed_count": len(random_aggregates),
    }

    total_episodes = sum(fold["selector"]["episode_count"] for fold in folds)
    total_mll_breaches = sum(
        fold["selector"]["mll_breach_count"] for fold in folds
    )
    held_out_mll = total_mll_breaches / total_episodes
    held_out_consistency = sum(
        fold["selector"]["consistency"] * fold["selector"]["episode_count"]
        for fold in folds
    ) / total_episodes
    maximum_component_profit_share = max(
        fold["selector"]["maximum_component_profit_share"] for fold in folds
    )
    positive_stressed_blocks = sum(
        fold["selector"]["stressed_net_usd"] > 0 for fold in folds
    )
    pass_blocks = sum(
        fold["selector"]["stressed_pass_count"] > 0 for fold in folds
    )
    block_pass_share = (
        max(fold["selector"]["stressed_pass_count"] for fold in folds)
        / selector["stressed_pass_count"]
        if selector["stressed_pass_count"]
        else 0.0
    )

    green_checks = {
        "positive_aggregate_stressed_net": selector["stressed_net_usd"] > 0,
        "beats_best_parent_normal_net": (
            selector["normal_net_usd"] > best_parent["normal_net_usd"]
        ),
        "beats_best_parent_stressed_net": (
            selector["stressed_net_usd"] > best_parent["stressed_net_usd"]
        ),
        "minimum_positive_stressed_blocks": (
            positive_stressed_blocks
            >= FROZEN_DECISION_THRESHOLDS[
                "minimum_positive_stressed_blocks_green"
            ]
        ),
        "beats_best_parent_stressed_target_progress": (
            selector["stressed_target_progress"]
            > best_parent["stressed_target_progress"]
        ),
        "minimum_stressed_passes": (
            selector["stressed_pass_count"]
            >= FROZEN_DECISION_THRESHOLDS["minimum_stressed_passes"]
        ),
        "minimum_pass_blocks": (
            pass_blocks >= FROZEN_DECISION_THRESHOLDS["minimum_pass_blocks"]
        ),
        "mll_within_tolerance": (
            held_out_mll
            <= FROZEN_DECISION_THRESHOLDS["maximum_mll_breach_rate"]
        ),
        "block_pass_share_within_limit": (
            block_pass_share
            <= FROZEN_DECISION_THRESHOLDS["maximum_block_pass_share"]
        ),
        "component_profit_share_within_limit": (
            maximum_component_profit_share
            <= FROZEN_DECISION_THRESHOLDS[
                "maximum_component_profit_share"
            ]
        ),
        "consistency_meets_minimum": (
            held_out_consistency
            >= FROZEN_DECISION_THRESHOLDS["minimum_consistency"]
        ),
        "beats_equal_risk_stressed_net": (
            selector["stressed_net_usd"] > equal_risk["stressed_net_usd"]
        ),
        "beats_equal_risk_stressed_target_progress": (
            selector["stressed_target_progress"]
            > equal_risk["stressed_target_progress"]
        ),
        "passes_not_worse_than_equal_risk": (
            selector["stressed_pass_count"] >= equal_risk["stressed_pass_count"]
        ),
        "beats_median_random_stressed_net": (
            selector["stressed_net_usd"] > median_random["stressed_net_usd"]
        ),
        "beats_median_random_stressed_target_progress": (
            selector["stressed_target_progress"]
            > median_random["stressed_target_progress"]
        ),
        "passes_not_worse_than_median_random": (
            selector["stressed_pass_count"]
            >= median_random["stressed_pass_count"]
        ),
    }
    weak_checks = {
        "positive_aggregate_stressed_net": selector["stressed_net_usd"] > 0,
        "beats_best_parent_stressed_target_progress": (
            selector["stressed_target_progress"]
            > best_parent["stressed_target_progress"]
        ),
        "mll_within_tolerance": (
            held_out_mll
            <= FROZEN_DECISION_THRESHOLDS["maximum_mll_breach_rate"]
        ),
        "minimum_positive_stressed_blocks": (
            positive_stressed_blocks
            >= FROZEN_DECISION_THRESHOLDS[
                "minimum_positive_stressed_blocks_weak"
            ]
        ),
    }

    green = all(green_checks.values())
    weak = not green and all(weak_checks.values())
    if green:
        status = SELECTOR_PROCEDURE_GREEN
        failures: tuple[str, ...] = ()
    elif weak:
        status = SELECTOR_PROCEDURE_WEAK
        failures = tuple(
            f"green.{name}" for name, passed in green_checks.items() if not passed
        )
    else:
        status = SELECTOR_PROCEDURE_FALSIFIED
        failures = tuple(
            [
                f"green.{name}"
                for name, passed in green_checks.items()
                if not passed
            ]
            + [
                f"weak.{name}"
                for name, passed in weak_checks.items()
                if not passed
            ]
        )

    metrics = {
        "outer_block_count": len(folds),
        "positive_stressed_block_count": positive_stressed_blocks,
        "stressed_pass_block_count": pass_blocks,
        "maximum_block_pass_share": block_pass_share,
        "maximum_component_profit_share": maximum_component_profit_share,
        "held_out_mll_breach_rate": held_out_mll,
        "held_out_consistency": held_out_consistency,
        "selector": selector,
        "best_parent": best_parent,
        "equal_risk": equal_risk,
        "median_fixed_random": median_random,
        "block_ids": [fold["block_id"] for fold in folds],
    }
    return SelectorProcedureDecision(
        status=status,
        metrics=metrics,
        checks={"green": green_checks, "weak": weak_checks},
        failure_reasons=failures,
        thresholds=dict(FROZEN_DECISION_THRESHOLDS),
    )


def render_selector_report(evidence: Mapping[str, Any]) -> str:
    """Render the required 25-section report in a deterministic order.

    Missing or explicitly blank sections are rejected.  A non-applicable
    section should therefore be supplied explicitly, for example as
    ``{"status": "NOT_APPLICABLE"}``, rather than silently omitted.
    """

    if not isinstance(evidence, Mapping):
        raise SelectorReportingError("report evidence must be a mapping")
    missing = [key for key, _ in REQUIRED_REPORT_SECTIONS if key not in evidence]
    if missing:
        raise SelectorReportingError(
            "missing required report sections: " + ", ".join(missing)
        )
    blank = [
        key
        for key, _ in REQUIRED_REPORT_SECTIONS
        if evidence[key] is None
        or (isinstance(evidence[key], str) and not evidence[key].strip())
    ]
    if blank:
        raise SelectorReportingError(
            "blank required report sections: " + ", ".join(blank)
        )

    lines = [
        "# HYDRA V7.3 Nested Selector Sprint Report",
        "",
        (
            "Headline selector evidence is restricted to held-out outer-fold "
            "results. Development-selected references are not independent "
            "confirmation."
        ),
    ]
    for number, (key, title) in enumerate(REQUIRED_REPORT_SECTIONS, start=1):
        lines.extend(("", f"## {number}. {title}", ""))
        value = evidence[key]
        if isinstance(value, str):
            lines.append(value.strip())
        else:
            try:
                encoded = json.dumps(
                    _plain_json(value),
                    sort_keys=True,
                    indent=2,
                    ensure_ascii=True,
                    allow_nan=False,
                )
            except (TypeError, ValueError) as exc:
                raise SelectorReportingError(
                    f"report section {key} is not canonical JSON"
                ) from exc
            lines.extend(("```json", encoded, "```"))
    return "\n".join(lines) + "\n"


def build_manifest_runtime_compatibility_projection(
    decision: SelectorProcedureDecision,
    *,
    result_schema: str,
    campaign_id: str,
    class_id: str,
    population_manifest_hash: str,
    compatibility_policy_pair_count: int,
    primary_rolling_combine_episode_count: int,
) -> dict[str, Any]:
    """Project selector output onto the generic manifest-runtime result shape.

    The generic runtime predates nested selection and names several cardinality
    fields in family-experiment terms.  Those fields are retained for interface
    compatibility and are explicitly labelled as such; scientific conclusions
    remain in ``selector_procedure`` and are held-out-selector conclusions only.
    """

    if not isinstance(decision, SelectorProcedureDecision):
        raise SelectorReportingError(
            "compatibility projection requires a computed selector decision"
        )
    for name, value in (
        ("result_schema", result_schema),
        ("campaign_id", campaign_id),
        ("class_id", class_id),
    ):
        if not isinstance(value, str) or not value.strip():
            raise SelectorReportingError(f"{name} must be non-empty")
    if not _is_sha256(population_manifest_hash):
        raise SelectorReportingError("population_manifest_hash must be SHA-256")
    if not _positive_int(compatibility_policy_pair_count):
        raise SelectorReportingError(
            "compatibility_policy_pair_count must be a positive integer"
        )
    if not _positive_int(primary_rolling_combine_episode_count):
        raise SelectorReportingError(
            "primary_rolling_combine_episode_count must be a positive integer"
        )

    policy_count = compatibility_policy_pair_count
    result: dict[str, Any] = {
        "schema": result_schema,
        "campaign_id": campaign_id,
        "class_id": class_id,
        "population": {
            "manifest_hash": population_manifest_hash,
            "real_policy_count": policy_count,
            "matched_control_policy_count": policy_count,
        },
        "policy_pair_evaluated_count": policy_count,
        "account_policy_economics": {
            "primary_rolling_combine_episode_count": (
                primary_rolling_combine_episode_count
            ),
            "family_average_effect_established": False,
            "selector_held_out_metrics": _plain_json(decision.metrics),
        },
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "scientific_status": decision.status,
        "selector_procedure": decision.to_dict(),
        "compatibility_projection": {
            "is_generic_manifest_runtime_projection": True,
            "selector_evidence_scope": "OUTER_FOLD_HELD_OUT_ONLY",
            "independent_confirmation": False,
            "development_selected_reference_is_independent_evidence": False,
            "family_average_fields_are_selector_evidence": False,
            "interface_only_fields": [
                "population.real_policy_count",
                "population.matched_control_policy_count",
                "policy_pair_evaluated_count",
                "account_policy_economics.primary_rolling_combine_episode_count",
            ],
        },
        "governance": {
            "proof_windows_consumed": 0,
            "new_data_purchase_count": 0,
            "q4_access_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        },
    }
    result["result_sha256"] = stable_hash(result)
    return result


def _validate_and_normalize_folds(
    held_out_folds: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if isinstance(held_out_folds, (str, bytes)) or not isinstance(
        held_out_folds, Sequence
    ):
        raise SelectorReportingError("held-out folds must be a sequence")
    minimum = FROZEN_DECISION_THRESHOLDS["minimum_outer_blocks"]
    if len(held_out_folds) < minimum:
        raise SelectorReportingError(f"at least {minimum} outer folds are required")

    normalized: list[dict[str, Any]] = []
    block_ids: set[str] = set()
    expected_random_seeds: set[int] | None = None
    for index, fold in enumerate(held_out_folds):
        if not isinstance(fold, Mapping):
            raise SelectorReportingError(f"fold {index} must be a mapping")
        block_id = fold.get("block_id")
        if not isinstance(block_id, str) or not block_id.strip():
            raise SelectorReportingError(f"fold {index} has no block_id")
        if block_id in block_ids:
            raise SelectorReportingError(f"duplicate held-out block_id: {block_id}")
        block_ids.add(block_id)

        selector = _normalize_policy(
            fold.get("selector"), f"{block_id}.selector", selector=True
        )
        best_parent = _normalize_policy(
            fold.get("best_parent"), f"{block_id}.best_parent"
        )
        equal_risk = _normalize_policy(
            fold.get("equal_risk"), f"{block_id}.equal_risk"
        )
        random_value = fold.get("random_selection")
        if not isinstance(random_value, list) or not random_value:
            raise SelectorReportingError(
                f"{block_id}.random_selection must be a non-empty list"
            )
        random_rows: list[dict[str, Any]] = []
        seeds: set[int] = set()
        for random_index, raw_row in enumerate(random_value):
            row = _normalize_policy(
                raw_row,
                f"{block_id}.random_selection[{random_index}]",
            )
            seed = raw_row.get("seed") if isinstance(raw_row, Mapping) else None
            if type(seed) is not int or seed < 0 or seed in seeds:
                raise SelectorReportingError(
                    f"{block_id}.random_selection seeds must be unique nonnegative integers"
                )
            seeds.add(seed)
            row["seed"] = seed
            random_rows.append(row)
        if expected_random_seeds is None:
            expected_random_seeds = seeds
        elif seeds != expected_random_seeds:
            raise SelectorReportingError(
                "the same fixed random-selection seeds must appear in every fold"
            )
        normalized.append(
            {
                "block_id": block_id,
                "selector": selector,
                "best_parent": best_parent,
                "equal_risk": equal_risk,
                "random_selection": random_rows,
            }
        )
    return sorted(normalized, key=lambda fold: fold["block_id"])


def _normalize_policy(
    value: Any, label: str, *, selector: bool = False
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SelectorReportingError(f"{label} must be a mapping")
    result = {
        "normal_net_usd": _number(value, "normal_net_usd", label),
        "stressed_net_usd": _number(value, "stressed_net_usd", label),
        "stressed_target_progress": _number(
            value, "stressed_target_progress", label
        ),
        "stressed_pass_count": _nonnegative_int_value(
            value, "stressed_pass_count", label
        ),
        "episode_count": _positive_int_value(value, "episode_count", label),
    }
    if result["stressed_pass_count"] > result["episode_count"]:
        raise SelectorReportingError(
            f"{label}.stressed_pass_count exceeds episode_count"
        )
    if selector:
        result.update(
            {
                "mll_breach_count": _nonnegative_int_value(
                    value, "mll_breach_count", label
                ),
                "consistency": _unit_interval(value, "consistency", label),
                "maximum_component_profit_share": _unit_interval(
                    value, "maximum_component_profit_share", label
                ),
            }
        )
        if result["mll_breach_count"] > result["episode_count"]:
            raise SelectorReportingError(
                f"{label}.mll_breach_count exceeds episode_count"
            )
    return result


def _aggregate_policy(rows: Sequence[Mapping[str, Any]]) -> dict[str, int | float]:
    episode_count = sum(row["episode_count"] for row in rows)
    return {
        "normal_net_usd": sum(row["normal_net_usd"] for row in rows),
        "stressed_net_usd": sum(row["stressed_net_usd"] for row in rows),
        "stressed_target_progress": sum(
            row["stressed_target_progress"] * row["episode_count"] for row in rows
        )
        / episode_count,
        "stressed_pass_count": sum(row["stressed_pass_count"] for row in rows),
        "episode_count": episode_count,
    }


def _number(value: Mapping[str, Any], key: str, label: str) -> int | float:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise SelectorReportingError(f"{label}.{key} must be numeric")
    if not math.isfinite(raw):
        raise SelectorReportingError(f"{label}.{key} must be finite")
    return raw


def _unit_interval(value: Mapping[str, Any], key: str, label: str) -> int | float:
    raw = _number(value, key, label)
    if raw < 0 or raw > 1:
        raise SelectorReportingError(f"{label}.{key} must be in [0, 1]")
    return raw


def _nonnegative_int_value(value: Mapping[str, Any], key: str, label: str) -> int:
    raw = value.get(key)
    if type(raw) is not int or raw < 0:
        raise SelectorReportingError(f"{label}.{key} must be a nonnegative integer")
    return raw


def _positive_int_value(value: Mapping[str, Any], key: str, label: str) -> int:
    raw = value.get(key)
    if not _positive_int(raw):
        raise SelectorReportingError(f"{label}.{key} must be a positive integer")
    return raw


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _plain_json(value: Any) -> Any:
    if isinstance(value, SelectorProcedureDecision):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(item) for item in value]
    return value
