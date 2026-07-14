from __future__ import annotations

import copy
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA = "hydra_v7_3_nested_selector_manifest_v1"
FROZEN_HORIZONS: tuple[int | str, ...] = (20, 40, 60, 90, "full")
PARETO_OBJECTIVES: tuple[tuple[str, str], ...] = (
    ("stressed_combine_pass_count", "MAXIMIZE"),
    ("normal_combine_pass_count", "MAXIMIZE"),
    ("stressed_median_target_progress", "MAXIMIZE"),
    ("lower_quartile_target_progress", "MAXIMIZE"),
    ("stressed_net_pnl", "MAXIMIZE"),
    ("mll_breach_rate", "MINIMIZE"),
    ("consistency", "MAXIMIZE"),
    ("component_concentration", "MINIMIZE"),
    ("temporal_block_concentration", "MINIMIZE"),
    ("operational_simplicity", "MAXIMIZE"),
)
HARD_REQUIREMENTS: tuple[str, ...] = (
    "positive_normal_net",
    "positive_stressed_net",
    "no_hard_execution_or_integrity_issue",
    "no_behavioral_clone",
    "no_excessive_one_component_domination",
    "mll_within_configured_research_tolerance",
)
BASELINES: tuple[str, ...] = (
    "BEST_PARENT_BASELINE",
    "EQUAL_RISK_BASKET_BASELINE",
    "RANDOM_SELECTION_BASELINE",
    "CURRENT_DIAGNOSTIC_CHAMPION",
)
SELECTOR_STATUSES: tuple[str, ...] = (
    "SELECTOR_PROCEDURE_GREEN",
    "SELECTOR_PROCEDURE_WEAK",
    "SELECTOR_PROCEDURE_FALSIFIED",
)


class SelectorManifestError(RuntimeError):
    """Raised when a nested-selector preregistration is incomplete or drifts."""


def stable_hash(value: Any) -> str:
    """Return the repository's canonical SHA-256 for a JSON-compatible value."""

    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def finalize_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and hash a selector manifest without mutating the caller's value.

    Finalization is idempotent for an already valid manifest.  It refuses to
    silently repair a claimed hash, which keeps post-freeze mutations visible.
    """

    if not isinstance(payload, Mapping):
        raise SelectorManifestError("selector manifest must be a JSON object")
    finalized = copy.deepcopy(dict(payload))
    claimed = finalized.pop("manifest_hash", None)
    _validate_unhashed_manifest(finalized)
    try:
        expected = stable_hash(finalized)
    except (TypeError, ValueError) as exc:
        raise SelectorManifestError("selector manifest is not canonical JSON") from exc
    if claimed is not None and claimed != expected:
        raise SelectorManifestError("selector manifest hash drift")
    finalized["manifest_hash"] = expected
    return finalized


def validate_manifest(payload: Mapping[str, Any]) -> None:
    """Validate an in-memory manifest, including its hash when present."""

    if not isinstance(payload, Mapping):
        raise SelectorManifestError("selector manifest must be a JSON object")
    value = dict(payload)
    claimed = value.pop("manifest_hash", None)
    _validate_unhashed_manifest(value)
    if claimed is not None:
        if not _is_sha256(claimed):
            raise SelectorManifestError("selector manifest hash is malformed")
        try:
            expected = stable_hash(value)
        except (TypeError, ValueError) as exc:
            raise SelectorManifestError(
                "selector manifest is not canonical JSON"
            ) from exc
        if claimed != expected:
            raise SelectorManifestError("selector manifest hash drift")


def load_and_verify_selector_manifest(path: str | Path) -> dict[str, Any]:
    """Load a finalized JSON manifest and fail closed on any policy drift."""

    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        raise SelectorManifestError("invalid selector manifest JSON") from exc
    if not isinstance(raw, dict):
        raise SelectorManifestError("selector manifest must be a JSON object")
    if "manifest_hash" not in raw:
        raise SelectorManifestError("selector manifest is not finalized")
    validate_manifest(raw)
    return raw


def load_and_verify_manifest(path: str | Path) -> dict[str, Any]:
    """Compatibility spelling for callers already scoped to this module."""

    return load_and_verify_selector_manifest(path)


def _validate_unhashed_manifest(payload: Mapping[str, Any]) -> None:
    if payload.get("schema") != SCHEMA:
        raise SelectorManifestError("selector manifest schema drift")
    if not _nonempty_string(payload.get("experiment_id")):
        raise SelectorManifestError("experiment_id must be non-empty")

    blocks = payload.get("temporal_blocks")
    _validate_temporal_blocks(blocks)

    if payload.get("frozen_horizons") != list(FROZEN_HORIZONS):
        raise SelectorManifestError("frozen horizon policy drift")

    _validate_risk_policy(payload)
    _validate_outer_crossfit(payload.get("outer_crossfit"))
    _validate_pareto_objectives(payload.get("pareto_objectives"))

    hard_requirements = payload.get("hard_requirements")
    if not isinstance(hard_requirements, Mapping) or set(hard_requirements) != set(
        HARD_REQUIREMENTS
    ):
        raise SelectorManifestError("hard selector requirements drift")
    if any(hard_requirements[name] is not True for name in HARD_REQUIREMENTS):
        raise SelectorManifestError("every hard selector requirement must be enabled")

    if payload.get("baselines") != list(BASELINES):
        raise SelectorManifestError("held-out baseline policy drift")
    seeds = payload.get("random_selection_seeds")
    if (
        not isinstance(seeds, list)
        or len(seeds) < len(blocks)
        or any(type(seed) is not int or seed < 0 for seed in seeds)
        or len(seeds) != len(set(seeds))
    ):
        raise SelectorManifestError(
            "random-selection seeds must be fixed, unique, and cover every fold"
        )

    _validate_decision_thresholds(payload.get("decision_thresholds"))

    governance = payload.get("governance")
    if not isinstance(governance, Mapping):
        raise SelectorManifestError("governance policy is missing")
    for key in (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "broker_or_orders_allowed",
    ):
        if governance.get(key) is not False:
            raise SelectorManifestError(f"{key} must remain false")

    if payload.get("compute_workers") != 3:
        raise SelectorManifestError("selector sprint requires exactly three workers")
    if payload.get("selector_frozen_before_heldout") is not True:
        raise SelectorManifestError(
            "selector must be frozen before any held-out evaluation"
        )


def _validate_temporal_blocks(value: Any) -> None:
    if not isinstance(value, list) or len(value) < 4:
        raise SelectorManifestError("at least four temporal blocks are required")

    block_ids: set[str] = set()
    all_episode_starts: set[str] = set()
    previous_end = None
    for index, block in enumerate(value):
        if not isinstance(block, Mapping):
            raise SelectorManifestError(f"temporal block {index} must be an object")
        block_id = block.get("block_id")
        if not _nonempty_string(block_id) or block_id in block_ids:
            raise SelectorManifestError("temporal block IDs must be non-empty and unique")
        block_ids.add(block_id)

        start = _block_date(block.get("start_date"), f"{block_id}.start_date")
        end = _block_date(
            block.get("end_date_exclusive"), f"{block_id}.end_date_exclusive"
        )
        if end <= start:
            raise SelectorManifestError(f"{block_id} has an empty temporal range")
        if previous_end is not None and start < previous_end:
            raise SelectorManifestError(
                "temporal blocks must be chronological and nonoverlapping"
            )
        previous_end = end

        for key in ("contracts", "markets", "sessions"):
            _validate_unique_strings(block.get(key), f"{block_id}.{key}")
        if not _positive_int(block.get("event_count")):
            raise SelectorManifestError(f"{block_id}.event_count must be positive")
        if not _positive_int(block.get("trading_days")):
            raise SelectorManifestError(f"{block_id}.trading_days must be positive")
        if not _nonempty_string(block.get("volatility_regime")):
            raise SelectorManifestError(
                f"{block_id}.volatility_regime must be non-empty"
            )
        separation = block.get("contract_and_roll_separation")
        if not isinstance(separation, Mapping):
            raise SelectorManifestError(
                f"{block_id} must explicitly audit contracts and rolls"
            )
        required_separation = {
            "contract_identity_recorded_by_market_and_session": True,
            "roll_transition_dates_explicit": True,
            "unsafe_roll_windows_explicit": True,
            "roll_boundaries_not_used_to_invent_independence": True,
        }
        if any(
            separation.get(key) is not expected
            for key, expected in required_separation.items()
        ):
            raise SelectorManifestError(
                f"{block_id} contract/roll provenance is incomplete"
            )
        if type(separation.get("block_may_span_contracts")) is not bool:
            raise SelectorManifestError(
                f"{block_id} must disclose whether it spans contracts"
            )
        if block.get("episode_starts_unique_across_blocks") is not True:
            raise SelectorManifestError(
                f"{block_id} must attest unique starts across blocks"
            )
        if block.get("inference_unit") != "TEMPORAL_BLOCK":
            raise SelectorManifestError(
                f"{block_id} must use the temporal block as inference unit"
            )
        if block.get("within_block_starts_independent") is not False:
            raise SelectorManifestError(
                f"{block_id} cannot call overlapping within-block starts independent"
            )
        if block.get("overlapping_episode_starts_counted_as_independent") is not False:
            raise SelectorManifestError(
                f"{block_id} cannot count overlapping starts as independent evidence"
            )
        if block.get("primary_horizon_complete_for_every_start") is not True:
            raise SelectorManifestError(
                f"{block_id} must preserve every primary observation horizon"
            )

        episode_starts = block.get("episode_starts")
        _validate_unique_strings(episode_starts, f"{block_id}.episode_starts")
        if episode_starts != sorted(episode_starts):
            raise SelectorManifestError(f"{block_id}.episode_starts must be chronological")
        for raw_start in episode_starts:
            episode_date = _episode_date(raw_start, block_id)
            if not start <= episode_date < end:
                raise SelectorManifestError(
                    f"{block_id} contains an episode start outside its date range"
                )
            if raw_start in all_episode_starts:
                raise SelectorManifestError(
                    "episode starts cannot be reused across independent blocks"
                )
            all_episode_starts.add(raw_start)

        contamination = block.get("contamination_history")
        if not isinstance(contamination, list):
            raise SelectorManifestError(
                f"{block_id}.contamination_history must be an explicit list"
            )
        provenance = block.get("provenance")
        if not isinstance(provenance, Mapping) or not provenance:
            raise SelectorManifestError(f"{block_id}.provenance must be complete")


def _validate_outer_crossfit(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise SelectorManifestError("outer cross-fit policy is missing")
    required = {
        "method": "LEAVE_ONE_BLOCK_OUT",
        "held_out_each_block_exactly_once": True,
        "candidate_evidence_design_only": True,
        "primary_champions_per_fold": 1,
        "no_retuning_after_heldout": True,
        "headline_evidence_heldout_only": True,
    }
    if any(value.get(key) != expected for key, expected in required.items()):
        raise SelectorManifestError("outer cross-fit policy drift")
    if value.get("maximum_backups_per_fold") not in {0, 1}:
        raise SelectorManifestError("at most one backup is allowed per fold")


def _validate_risk_policy(payload: Mapping[str, Any]) -> None:
    tiers = payload.get("risk_tiers")
    if not isinstance(tiers, list) or not 2 <= len(tiers) <= 5:
        raise SelectorManifestError("risk frontier must contain two to five tiers")
    if any(not _finite_number(value) for value in tiers):
        raise SelectorManifestError("risk tiers must be finite numeric values")
    normalized = [float(value) for value in tiers]
    if (
        normalized != sorted(normalized)
        or len(normalized) != len(set(normalized))
        or 1.0 not in normalized
        or normalized[0] <= 0.0
        or normalized[-1] > 1.5
    ):
        raise SelectorManifestError(
            "risk tiers must be ordered, unique, positive, include 1.0, and be <=1.5"
        )
    if payload.get("risk_selected_inside_design_set") is not True:
        raise SelectorManifestError("risk must be selected inside each design set")
    if payload.get("static_risk_only") is not True:
        raise SelectorManifestError("only preregistered static risk is allowed")


def _validate_pareto_objectives(value: Any) -> None:
    if not isinstance(value, list) or len(value) != len(PARETO_OBJECTIVES):
        raise SelectorManifestError("Pareto objective set drift")
    actual: list[tuple[str, str]] = []
    for objective in value:
        if not isinstance(objective, Mapping) or set(objective) != {
            "metric",
            "direction",
        }:
            raise SelectorManifestError(
                "Pareto objectives must contain only metric and direction"
            )
        actual.append((objective["metric"], objective["direction"]))
    if tuple(actual) != PARETO_OBJECTIVES:
        raise SelectorManifestError("Pareto objective order or direction drift")


def _validate_decision_thresholds(value: Any) -> None:
    if not isinstance(value, Mapping) or set(value) != set(SELECTOR_STATUSES):
        raise SelectorManifestError("selector decision statuses drift")
    green = value["SELECTOR_PROCEDURE_GREEN"]
    weak = value["SELECTOR_PROCEDURE_WEAK"]
    falsified = value["SELECTOR_PROCEDURE_FALSIFIED"]
    if not all(isinstance(item, Mapping) for item in (green, weak, falsified)):
        raise SelectorManifestError("selector decision thresholds must be objects")

    if not _integer_at_least(
        green.get("minimum_aggregate_held_out_combine_passes"), 3
    ):
        raise SelectorManifestError("GREEN requires at least three held-out passes")
    if not _integer_at_least(green.get("minimum_blocks_with_passes"), 2):
        raise SelectorManifestError("GREEN requires passes in at least two blocks")
    if not _integer_at_least(green.get("minimum_positive_economic_blocks"), 3):
        raise SelectorManifestError("GREEN requires three positive economic blocks")
    for key in (
        "positive_aggregate_stressed_net_required",
        "normal_and_stressed_improvement_over_best_parent_required",
        "held_out_target_progress_improvement_over_best_parent_required",
        "acceptable_consistency_required",
        "improvement_over_equal_risk_required",
        "stronger_than_random_selection_required",
    ):
        if green.get(key) is not True:
            raise SelectorManifestError(f"GREEN threshold {key} must be enabled")

    mll_tolerance = green.get("maximum_held_out_mll_breach_rate")
    if not _rate_at_most(mll_tolerance, 0.10):
        raise SelectorManifestError("GREEN MLL tolerance cannot exceed 10%")
    if not _rate_at_most(green.get("maximum_single_block_pass_share"), 0.50):
        raise SelectorManifestError(
            "GREEN maximum_single_block_pass_share cannot exceed 50%"
        )
    if not _rate_at_most(
        green.get("maximum_single_component_profit_share"), 0.65
    ):
        raise SelectorManifestError(
            "GREEN maximum_single_component_profit_share cannot exceed 65%"
        )

    if (
        weak.get("green_requirements_not_met") is not True
        or not _integer_at_least(weak.get("minimum_positive_economic_blocks"), 1)
        or weak.get("any_held_out_improvement_signal_required") is not True
    ):
        raise SelectorManifestError("WEAK decision policy drift")
    if (
        falsified.get("green_requirements_not_met") is not True
        or falsified.get("weak_requirements_not_met") is not True
        or falsified.get("terminate_static_basket_synthesis") is not True
    ):
        raise SelectorManifestError("FALSIFIED decision policy drift")


def _validate_unique_strings(value: Any, label: str) -> None:
    if (
        not isinstance(value, list)
        or not value
        or any(not _nonempty_string(item) for item in value)
        or len(value) != len(set(value))
    ):
        raise SelectorManifestError(f"{label} must contain unique non-empty strings")


def _block_date(value: Any, label: str):
    if not isinstance(value, str):
        raise SelectorManifestError(f"{label} must use YYYY-MM-DD")
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SelectorManifestError(f"{label} must use YYYY-MM-DD") from exc


def _episode_date(value: str, block_id: str):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise SelectorManifestError(
            f"{block_id}.episode_starts must contain ISO-8601 values"
        ) from exc


def _nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _integer_at_least(value: Any, minimum: int) -> bool:
    return type(value) is int and value >= minimum


def _finite_number(value: Any) -> bool:
    return type(value) in {int, float} and math.isfinite(float(value))


def _rate_at_most(value: Any, maximum: float) -> bool:
    return _finite_number(value) and 0.0 <= float(value) <= maximum


def _is_sha256(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(character in "0123456789abcdef" for character in value)
