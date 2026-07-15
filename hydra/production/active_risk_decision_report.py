"""Streaming, report-only decision audit for active-risk campaign 0026.

The production runtime deliberately persists outcome-rich Stage-3 batches.  A
single batch can be large because it contains exact daily account paths and XFA
ledgers.  This module consumes and releases one batch at a time, validates the
runtime ``rows_hash``, and builds a compact decision report without changing
selection, promotion, manifests, or authoritative campaign evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np


REPORT_SCHEMA = "hydra_active_risk_decision_report_v1"
REPORT_REVISION = "revision_02"
CAMPAIGN_ID = "hydra_active_risk_pool_target_velocity_0026"
CANONICAL_HORIZON = "90_TRADING_DAYS"
FULL_HORIZON = "FULL_CHRONOLOGICAL_HORIZON"
HORIZONS = (
    "20_TRADING_DAYS",
    "40_TRADING_DAYS",
    "60_TRADING_DAYS",
    CANONICAL_HORIZON,
    FULL_HORIZON,
)
SCENARIOS = ("normal", "stressed")
PATHS = ("standard", "consistency")


class ActiveRiskDecisionReportError(RuntimeError):
    """A report input is incomplete, inconsistent, or not campaign 0026."""


def canonical_hash(value: Any) -> str:
    """Return the runtime-compatible stable hash without one giant JSON copy."""

    digest = hashlib.sha256()
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    for chunk in encoder.iterencode(value):
        digest.update(chunk.encode("ascii"))
    return digest.hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ActiveRiskDecisionReportError(f"cannot read JSON {path}: {exc}") from exc


def _verify_embedded_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    claimed = str(payload.get(field) or "")
    checked = dict(payload)
    checked.pop(field, None)
    if not claimed or canonical_hash(checked) != claimed:
        raise ActiveRiskDecisionReportError(f"{label} {field} drift")


def _float(value: Any, *, default: float = 0.0) -> float:
    if value is None:
        return default
    result = float(value)
    if not math.isfinite(result):
        raise ActiveRiskDecisionReportError("non-finite economic value")
    return result


def _quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _distribution(values: Iterable[Any]) -> dict[str, Any]:
    finite = [_float(value) for value in values if value is not None]
    if not finite:
        return {
            "count": 0,
            "minimum": None,
            "p25": None,
            "median": None,
            "p75": None,
            "maximum": None,
            "mean": None,
        }
    return {
        "count": len(finite),
        "minimum": min(finite),
        "p25": _quantile(finite, 0.25),
        "median": statistics.median(finite),
        "p75": _quantile(finite, 0.75),
        "maximum": max(finite),
        "mean": statistics.fmean(finite),
    }


def _scenario_key(value: Any) -> str:
    text = str(value).upper()
    if text == "NORMAL":
        return "normal"
    if text in {"STRESSED", "STRESSED_1_5X"}:
        return "stressed"
    raise ActiveRiskDecisionReportError(f"unknown cost scenario {value!r}")


@dataclass(frozen=True)
class BlockSpec:
    block_id: str
    start: date
    end: date
    markets: tuple[str, ...]
    contract_separation: Any

    def contains_epoch_day(self, epoch_day: int) -> bool:
        value = date(1970, 1, 1) + timedelta(days=int(epoch_day))
        return self.start <= value <= self.end

    def to_dict(self) -> dict[str, Any]:
        return {
            "block_id": self.block_id,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "markets": list(self.markets),
            "contract_separation": self.contract_separation,
        }


def _block_specs(manifest: Mapping[str, Any]) -> tuple[BlockSpec, ...]:
    campaign_id = str(manifest.get("campaign_id") or "")
    if campaign_id != CAMPAIGN_ID:
        raise ActiveRiskDecisionReportError(
            f"manifest campaign is {campaign_id!r}, expected {CAMPAIGN_ID!r}"
        )
    blocks = manifest.get("temporal_blocks", {}).get("blocks")
    if not isinstance(blocks, list) or not blocks:
        raise ActiveRiskDecisionReportError("manifest temporal blocks are absent")
    output: list[BlockSpec] = []
    for raw in blocks:
        try:
            output.append(
                BlockSpec(
                    block_id=str(raw["block_id"]),
                    start=date.fromisoformat(str(raw["start"])),
                    end=date.fromisoformat(str(raw["end"])),
                    markets=tuple(str(value) for value in raw.get("markets") or ()),
                    contract_separation=raw.get("contract_separation"),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ActiveRiskDecisionReportError("malformed temporal block") from exc
    ids = [row.block_id for row in output]
    if len(ids) != len(set(ids)):
        raise ActiveRiskDecisionReportError("duplicate temporal block id")
    return tuple(output)


def _block_for_day(epoch_day: int, blocks: Sequence[BlockSpec]) -> str:
    matches = [row.block_id for row in blocks if row.contains_epoch_day(epoch_day)]
    if len(matches) != 1:
        raise ActiveRiskDecisionReportError(
            f"episode day {epoch_day} maps to {len(matches)} temporal blocks"
        )
    return matches[0]


@dataclass
class BlockAccumulator:
    episode_count: int = 0
    pass_count: int = 0
    mll_breach_count: int = 0
    censored_count: int = 0
    consistency_ok_count: int = 0
    target_progress: list[float] = field(default_factory=list)
    minimum_mll_buffer: list[float] = field(default_factory=list)
    net_pnl: list[float] = field(default_factory=list)
    days_to_target: list[float] = field(default_factory=list)

    def add(self, raw: Mapping[str, Any]) -> None:
        terminal = str(raw.get("terminal_classification") or raw.get("terminal") or "")
        self.episode_count += 1
        self.pass_count += int(terminal == "TARGET_REACHED" or bool(raw.get("passed")))
        self.mll_breach_count += int(
            terminal == "MLL_BREACHED" or bool(raw.get("mll_breached"))
        )
        self.censored_count += int(
            terminal in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"}
            or bool(raw.get("censored"))
        )
        self.consistency_ok_count += int(bool(raw.get("consistency_ok")))
        self.target_progress.append(_float(raw.get("target_progress")))
        self.minimum_mll_buffer.append(_float(raw.get("minimum_mll_buffer")))
        self.net_pnl.append(_float(raw.get("net_pnl")))
        if raw.get("days_to_target") is not None:
            self.days_to_target.append(_float(raw["days_to_target"]))

    def to_dict(self) -> dict[str, Any]:
        denominator = max(self.episode_count, 1)
        return {
            "episode_count": self.episode_count,
            "pass_count": self.pass_count,
            "pass_rate": self.pass_count / denominator,
            "mll_breach_count": self.mll_breach_count,
            "mll_breach_rate": self.mll_breach_count / denominator,
            "censored_count": self.censored_count,
            "consistency_rate": self.consistency_ok_count / denominator,
            "target_progress": _distribution(self.target_progress),
            "minimum_mll_buffer": _distribution(self.minimum_mll_buffer),
            "net_pnl": _distribution(self.net_pnl),
            "days_to_target": _distribution(self.days_to_target),
        }


@dataclass
class HorizonAccumulator:
    policy_count: int = 0
    episode_count: int = 0
    pass_count: int = 0
    mll_breach_count: int = 0
    censored_count: int = 0
    policy_values: dict[str, list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add(self, summary: Mapping[str, Any]) -> None:
        self.policy_count += 1
        self.episode_count += int(summary.get("episode_count", 0))
        self.pass_count += int(summary.get("pass_count", 0))
        self.mll_breach_count += int(summary.get("mll_breach_count", 0))
        self.censored_count += int(summary.get("censored_episode_count", 0))
        for key in (
            "pass_rate",
            "target_progress_p25",
            "target_progress_median",
            "maximum_target_progress",
            "net_median",
            "net_total",
            "mll_breach_rate",
            "minimum_mll_buffer",
            "consistency_rate",
            "projected_active_days_to_target_median",
            "projected_calendar_days_to_target_median",
            "median_days_to_target",
            "maximum_block_profit_share",
            "maximum_sleeve_profit_share",
        ):
            if summary.get(key) is not None:
                self.policy_values[key].append(_float(summary[key]))

    def to_dict(self) -> dict[str, Any]:
        denominator = max(self.episode_count, 1)
        return {
            "policy_count": self.policy_count,
            "episode_count": self.episode_count,
            "pass_count": self.pass_count,
            "pass_rate": self.pass_count / denominator,
            "mll_breach_count": self.mll_breach_count,
            "mll_breach_rate": self.mll_breach_count / denominator,
            "censored_episode_count": self.censored_count,
            "policy_level_distributions": {
                key: _distribution(values)
                for key, values in sorted(self.policy_values.items())
            },
        }


@dataclass
class RiskAccumulator:
    observations: int = 0
    weighted_mean: float = 0.0
    policy_medians: list[float] = field(default_factory=list)
    policy_p25: list[float] = field(default_factory=list)
    policy_p75: list[float] = field(default_factory=list)
    groups: dict[str, dict[str, float]] = field(
        default_factory=lambda: {
            key: {"count": 0.0, "weighted_mean": 0.0, "medians": []}
            for key in ("zero", "one", "two", "three_or_more")
        }
    )

    def add(self, value: Mapping[str, Any]) -> None:
        count = int(value.get("observation_count", 0))
        self.observations += count
        self.weighted_mean += _float(value.get("mean")) * count
        self.policy_medians.append(_float(value.get("median")))
        self.policy_p25.append(_float(value.get("p25")))
        self.policy_p75.append(_float(value.get("p75")))
        source = value.get("by_active_sleeve_count") or {}
        for key, target in self.groups.items():
            row = source.get(key) or {}
            observations = int(row.get("observation_count", 0))
            target["count"] += observations
            target["weighted_mean"] += _float(row.get("mean")) * observations
            target["medians"].append(_float(row.get("median")))

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_measure": "DECLARED_NOMINAL_RISK_UTILISATION",
            "actual_stop_risk_available": False,
            "observation_count": self.observations,
            "mean": self.weighted_mean / self.observations if self.observations else 0.0,
            "policy_median_distribution": _distribution(self.policy_medians),
            "policy_p25_distribution": _distribution(self.policy_p25),
            "policy_p75_distribution": _distribution(self.policy_p75),
            "by_active_sleeve_count": {
                key: {
                    "observation_count": int(value["count"]),
                    "observation_fraction": (
                        value["count"] / self.observations if self.observations else 0.0
                    ),
                    "mean": (
                        value["weighted_mean"] / value["count"]
                        if value["count"]
                        else 0.0
                    ),
                    "policy_median_distribution": _distribution(value["medians"]),
                }
                for key, value in self.groups.items()
            },
        }


@dataclass
class SuppressionAccumulator:
    signals_emitted: int = 0
    signals_accepted: int = 0
    signals_rejected: int = 0
    status_counts: Counter[str] = field(default_factory=Counter)
    foregone_realized_pnl_ex_post: float = 0.0

    def add(self, value: Mapping[str, Any]) -> None:
        self.signals_emitted += int(value.get("signals_emitted", 0))
        self.signals_accepted += int(value.get("signals_accepted", 0))
        self.signals_rejected += int(value.get("signals_rejected", 0))
        self.status_counts.update(value.get("decision_status_counts") or {})
        self.foregone_realized_pnl_ex_post += _float(
            value.get("foregone_realized_pnl_ex_post")
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "signals_emitted": self.signals_emitted,
            "signals_accepted": self.signals_accepted,
            "signals_rejected": self.signals_rejected,
            "acceptance_rate": (
                self.signals_accepted / self.signals_emitted
                if self.signals_emitted
                else 0.0
            ),
            "decision_status_counts": dict(sorted(self.status_counts.items())),
            "foregone_realized_pnl_ex_post": self.foregone_realized_pnl_ex_post,
            "foregone_expected_pnl": None,
            "foregone_expected_pnl_status": (
                "UNAVAILABLE_NO_FROZEN_PRE_OUTCOME_ESTIMATE"
            ),
            "counterfactual_role": "POSTHOC_DIAGNOSTIC_NOT_ROUTING_INPUT",
        }


@dataclass
class LifecyclePathAccumulator:
    combine_attempts: int = 0
    xfa_paths_started: int = 0
    observed_paths: int = 0
    first_payouts: int = 0
    payout_cycles: int = 0
    trader_net_payout: float = 0.0
    post_payout_survived: int = 0
    post_payout_censored: int = 0
    first_payout_days: list[float] = field(default_factory=list)
    minimum_mll_buffers: list[float] = field(default_factory=list)

    def add_attempts(self, count: int) -> None:
        self.combine_attempts += int(count)

    def add_path(self, path: Mapping[str, Any]) -> None:
        self.xfa_paths_started += 1
        self.observed_paths += int(int(path.get("observed_days", 0)) > 0)
        eligible = bool(path.get("payout_eligible"))
        self.first_payouts += int(eligible)
        self.payout_cycles += int(path.get("payout_cycles", 0))
        self.trader_net_payout += _float(path.get("trader_net_payout"))
        self.post_payout_survived += int(bool(path.get("post_payout_survived")))
        self.post_payout_censored += int(bool(path.get("post_payout_censored")))
        self.minimum_mll_buffers.append(_float(path.get("minimum_mll_buffer")))
        if eligible and path.get("first_payout_day") is not None:
            start = path.get("start_day")
            first = path.get("first_payout_day")
            if start is not None:
                self.first_payout_days.append(float(int(first) - int(start) + 1))

    def to_dict(self) -> dict[str, Any]:
        return {
            "combine_attempts": self.combine_attempts,
            "xfa_paths_started": self.xfa_paths_started,
            "observed_xfa_paths": self.observed_paths,
            "combine_pass_rate_full_horizon": (
                self.xfa_paths_started / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
            "first_payouts": self.first_payouts,
            "first_payout_probability_conditional_on_combine_pass": (
                self.first_payouts / self.xfa_paths_started
                if self.xfa_paths_started
                else 0.0
            ),
            "first_payout_probability_per_combine_attempt": (
                self.first_payouts / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
            "payout_cycles": self.payout_cycles,
            "expected_payout_cycles_per_combine_attempt": (
                self.payout_cycles / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
            "trader_net_payout": self.trader_net_payout,
            "expected_trader_payout_per_combine_attempt": (
                self.trader_net_payout / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
            "post_payout_survival_count": self.post_payout_survived,
            "post_payout_survival_rate_among_xfa_paths": (
                self.post_payout_survived / self.xfa_paths_started
                if self.xfa_paths_started
                else 0.0
            ),
            "post_payout_survival_rate_conditional_on_first_payout": (
                self.post_payout_survived / self.first_payouts
                if self.first_payouts
                else 0.0
            ),
            "post_payout_censored_count": self.post_payout_censored,
            "days_to_first_payout": _distribution(self.first_payout_days),
            "minimum_mll_buffer": _distribution(self.minimum_mll_buffers),
        }


def _summary_view(summary: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "episode_count",
        "pass_count",
        "pass_rate",
        "target_progress_p25",
        "target_progress_median",
        "maximum_target_progress",
        "net_median",
        "net_total",
        "mll_breach_count",
        "mll_breach_rate",
        "minimum_mll_buffer",
        "consistency_rate",
        "censored_episode_count",
        "median_days_to_target",
        "projected_active_days_to_target_median",
        "projected_calendar_days_to_target_median",
        "pass_block_count",
        "maximum_block_profit_share",
        "maximum_sleeve_profit_share",
    )
    output = {key: summary.get(key) for key in keys}
    output["pass_block_ids"] = list(summary.get("pass_block_ids") or ())
    output["by_block_net"] = dict(summary.get("by_block_net") or {})
    output["by_block_target_progress_median"] = dict(
        summary.get("by_block_target_progress_median") or {}
    )
    return output


def _metric_view(row: Mapping[str, Any]) -> dict[str, Any]:
    horizons = row.get("horizons") or {}
    return {
        "policy_id": str(row.get("policy_id") or ""),
        "normal": _summary_view(row.get("normal") or {}),
        "stressed": _summary_view(row.get("stressed") or {}),
        "horizons": {
            scenario: {
                label: _summary_view((horizons.get(scenario) or {}).get(label) or {})
                for label in HORIZONS
                if label in (horizons.get(scenario) or {})
            }
            for scenario in SCENARIOS
        },
    }


def _delta(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _float(left.get(key)) - _float(right.get(key))
        for key in (
            "pass_count",
            "pass_rate",
            "target_progress_p25",
            "target_progress_median",
            "net_median",
            "net_total",
            "mll_breach_rate",
            "minimum_mll_buffer",
            "consistency_rate",
            "pass_block_count",
        )
    }


def _load_controls(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _load_json(path)
    if not isinstance(payload, Mapping):
        raise ActiveRiskDecisionReportError("matched controls are not an object")
    if str(payload.get("campaign_id") or "") != CAMPAIGN_ID:
        raise ActiveRiskDecisionReportError("matched-control campaign drift")
    _verify_embedded_hash(payload, "controls_hash", "matched controls")
    if bool(payload.get("random_priority_outcomes_used_for_matching", False)):
        raise ActiveRiskDecisionReportError(
            "random-priority control matching used economic outcomes"
        )
    random_raw = payload.get("random_priority_by_policy") or {}
    matches_raw = payload.get("random_priority_exposure_match_by_policy") or {}
    compact = {
        "static_partition": _metric_view(payload["static_partition"]),
        "standalone_controls": [
            _metric_view(row) for row in payload.get("standalone_controls") or ()
        ],
        "best_standalone": _metric_view(payload["best_standalone"]),
        "equal_risk_active_pool": _metric_view(payload["equal_risk_active_pool"]),
        "always_on_pooled_governor": _metric_view(
            payload["always_on_pooled_governor"]
        ),
        "random_priority_by_policy": {
            str(key): _metric_view(value) for key, value in random_raw.items()
        },
        "random_priority_exposure_match_by_policy": {
            str(key): dict(value) for key, value in matches_raw.items()
        },
        "matched_controls_status": payload.get("matched_controls_status"),
        "random_priority_exposure_matched": payload.get(
            "random_priority_exposure_matched"
        ),
        "random_priority_exposure_match_rate": payload.get(
            "random_priority_exposure_match_rate"
        ),
        "random_priority_fixed_seeds": list(
            payload.get("random_priority_fixed_seeds") or ()
        ),
        "development_only": bool(payload.get("development_only", True)),
    }
    provenance = {
        "path": str(path),
        "sha256": file_sha256(path),
        "controls_hash": str(payload["controls_hash"]),
    }
    return compact, provenance


def _load_halving(directory: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    decisions: dict[str, Any] = {}
    files: list[dict[str, Any]] = []
    for path in sorted(directory.glob("stage*.json")):
        value = _load_json(path)
        if not isinstance(value, Mapping):
            raise ActiveRiskDecisionReportError(f"halving file is malformed: {path}")
        if "decision_hash" in value:
            _verify_embedded_hash(value, "decision_hash", path.name)
        decisions[path.stem] = {
            "stage": value.get("stage"),
            "input_count": int(value.get("input_count", 0)),
            "eligible_count": int(value.get("eligible_count", 0)),
            "output_limit": int(value.get("output_limit", 0)),
            "output_count": int(value.get("output_count", 0)),
            "selected_policy_ids": list(value.get("selected_policy_ids") or ()),
            "excluded": list(value.get("excluded") or ()),
            "decision_hash": value.get("decision_hash"),
            "development_only": bool(value.get("development_only", True)),
        }
        files.append({"path": str(path), "sha256": file_sha256(path)})
    if "stage3" not in decisions:
        raise ActiveRiskDecisionReportError("Stage-3 promotion decision is absent")
    return decisions, {"files": files}


def _sample_path(path: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    if not path:
        return [0.0] * 5
    output: list[float] = []
    for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        index = int(round((len(path) - 1) * fraction))
        output.append(_float(path[index].get(key)))
    return output


def _behavior_vector(
    canonical_raw: Sequence[Mapping[str, Any]],
) -> tuple[
    np.ndarray,
    tuple[int, ...],
    str,
    list[tuple[str, int]],
    frozenset[tuple[str, int, str, int, str]],
]:
    rows = sorted(
        canonical_raw,
        key=lambda row: (_scenario_key(row.get("scenario")), int(row["start_day"])),
    )
    values: list[float] = []
    terminals: list[int] = []
    keys: list[tuple[str, int]] = []
    routing_tuples: set[tuple[str, int, str, int, str]] = set()
    for raw in rows:
        scenario = _scenario_key(raw.get("scenario"))
        start = int(raw["start_day"])
        keys.append((scenario, start))
        accepted = int(raw.get("accepted_events", 0))
        skipped = int(raw.get("skipped_events", 0))
        emitted = accepted + skipped
        terminal = str(raw.get("terminal_classification") or "")
        terminal_code = 1 if terminal == "TARGET_REACHED" else -1 if terminal == "MLL_BREACHED" else 0
        terminals.append(terminal_code)
        eligible = max(int(raw.get("eligible_days", 0)), 1)
        routing = list(raw.get("risk_allocation_path") or ())
        routing_tuples.update(
            (
                scenario,
                start,
                str(decision.get("event_id") or ""),
                int(decision.get("quantity", 0)),
                str(decision.get("decision_status") or "UNKNOWN"),
            )
            for decision in routing
        )
        size_reduced = sum(
            str(row.get("decision_status") or "") == "SIZE_REDUCED" for row in routing
        )
        conflict = sum(
            str(row.get("decision_status") or "") == "CONFLICT_REJECTED"
            for row in routing
        )
        mll_rejected = sum(
            str(row.get("decision_status") or "") == "MLL_RISK_REJECTED"
            for row in routing
        )
        daily = list(raw.get("daily_path") or ())
        features = [
            _float(raw.get("target_progress")),
            _float(raw.get("net_pnl")) / 9000.0,
            _float(raw.get("minimum_mll_buffer")) / 4500.0,
            float(bool(raw.get("consistency_ok"))),
            accepted / emitted if emitted else 0.0,
            skipped / emitted if emitted else 0.0,
            int(raw.get("traded_days", 0)) / eligible,
            _float(raw.get("maximum_mini_equivalent")) / 15.0,
            _float(raw.get("maximum_net_directional_exposure")) / 15.0,
            float(terminal_code),
            size_reduced / len(routing) if routing else 0.0,
            conflict / len(routing) if routing else 0.0,
            mll_rejected / len(routing) if routing else 0.0,
        ]
        features.extend(_sample_path(daily, "target_progress"))
        features.extend(value / 4500.0 for value in _sample_path(daily, "closing_mll_buffer"))
        values.extend(max(-2.0, min(2.0, float(value))) for value in features)
    vector = np.asarray(values, dtype=np.float32)
    rounded = [round(float(value), 8) for value in vector]
    return (
        vector,
        tuple(terminals),
        canonical_hash(rounded),
        keys,
        frozenset(routing_tuples),
    )


def _behavior_similarity(
    left: np.ndarray,
    right: np.ndarray,
    left_terminal: Sequence[int],
    right_terminal: Sequence[int],
    left_routing: frozenset[tuple[str, int, str, int, str]],
    right_routing: frozenset[tuple[str, int, str, int, str]],
) -> tuple[float, float, float, float, bool]:
    if left.shape != right.shape or len(left_terminal) != len(right_terminal):
        return 0.0, math.inf, 0.0, 0.0, False
    if left.size == 0:
        routing_jaccard = _jaccard(left_routing, right_routing)
        return 1.0, 0.0, 1.0, routing_jaccard, routing_jaccard >= 0.90
    left64 = left.astype(np.float64, copy=False)
    right64 = right.astype(np.float64, copy=False)
    ldev = left64 - float(np.mean(left64))
    rdev = right64 - float(np.mean(right64))
    denominator = float(np.linalg.norm(ldev) * np.linalg.norm(rdev))
    correlation = (
        float(np.dot(ldev, rdev) / denominator)
        if denominator > 1e-12
        else float(np.allclose(left64, right64, atol=1e-12, rtol=0.0))
    )
    rmse = float(np.sqrt(np.mean(np.square(left64 - right64))))
    terminal_agreement = sum(
        int(a == b) for a, b in zip(left_terminal, right_terminal, strict=True)
    ) / max(len(left_terminal), 1)
    routing_jaccard = _jaccard(left_routing, right_routing)
    similar = (
        correlation >= 0.995
        and rmse <= 0.05
        and terminal_agreement >= 0.95
        and routing_jaccard >= 0.90
    )
    return correlation, rmse, terminal_agreement, routing_jaccard, similar


def _jaccard(left: frozenset[Any], right: frozenset[Any]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    return len(left & right) / max(len(union), 1)


def _clusters(
    candidate_ids: Sequence[str],
    vectors: Mapping[str, np.ndarray],
    terminals: Mapping[str, Sequence[int]],
    vector_hashes: Mapping[str, str],
    routing_tuples: Mapping[
        str, frozenset[tuple[str, int, str, int, str]]
    ],
    candidates: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    groups: list[list[str]] = []
    pair_diagnostics: dict[
        tuple[str, str], tuple[float, float, float, float, bool]
    ] = {}

    def comparison(
        left: str, right: str
    ) -> tuple[float, float, float, float, bool]:
        key = tuple(sorted((left, right)))
        if key not in pair_diagnostics:
            pair_diagnostics[key] = _behavior_similarity(
                vectors[left],
                vectors[right],
                terminals[left],
                terminals[right],
                routing_tuples[left],
                routing_tuples[right],
            )
        return pair_diagnostics[key]

    for candidate_id in sorted(candidate_ids):
        joined = False
        for group in groups:
            if all(comparison(candidate_id, member)[4] for member in group):
                group.append(candidate_id)
                joined = True
                break
        if not joined:
            groups.append([candidate_id])

    rows: list[dict[str, Any]] = []
    membership: dict[str, str] = {}
    for members in groups:
        ordered = sorted(members)
        cluster_id = "active_behavior_" + hashlib.sha256(
            "|".join(ordered).encode("utf-8")
        ).hexdigest()[:20]
        representative = sorted(
            ordered,
            key=lambda value: (
                -_float(candidates[value]["stressed"].get("pass_rate")),
                -_float(candidates[value]["stressed"].get("target_progress_p25")),
                -_float(candidates[value]["stressed"].get("target_progress_median")),
                -_float(candidates[value]["stressed"].get("net_median")),
                _float(candidates[value]["stressed"].get("mll_breach_rate")),
                value,
            ),
        )[0]
        diagnostics = [
            comparison(left, right)
            for index, left in enumerate(ordered)
            for right in ordered[index + 1 :]
        ]
        row = {
            "cluster_id": cluster_id,
            "member_ids": ordered,
            "member_count": len(ordered),
            "representative_id": representative,
            "exact_vector_equivalent": len({vector_hashes[value] for value in ordered}) == 1,
            "exact_routing_equivalent": len(
                {canonical_hash(sorted(routing_tuples[value])) for value in ordered}
            ) == 1,
            "minimum_pair_correlation": (
                min(value[0] for value in diagnostics) if diagnostics else 1.0
            ),
            "maximum_pair_rmse": (
                max(value[1] for value in diagnostics) if diagnostics else 0.0
            ),
            "minimum_terminal_agreement": (
                min(value[2] for value in diagnostics) if diagnostics else 1.0
            ),
            "minimum_routing_jaccard": (
                min(value[3] for value in diagnostics) if diagnostics else 1.0
            ),
        }
        rows.append(row)
        for candidate_id in ordered:
            membership[candidate_id] = cluster_id
    rows.sort(key=lambda row: str(row["cluster_id"]))
    return {
        "scope": "STAGE3_PROMOTED_TO_96_ONLY",
        "report_only": True,
        "promotion_or_selection_effect": False,
        "method": {
            "name": "LEXICOGRAPHIC_COMPLETE_LINK_CANONICAL_ACCOUNT_VECTOR_V1",
            "canonical_horizon": CANONICAL_HORIZON,
            "inputs": [
                "normal_and_stressed_episode_outcomes_by_frozen_start",
                "sampled_daily_target_progress_and_mll_buffer_paths",
                "accepted_rejected_and_size_reduced_routing_rates",
                "canonical_routing_tuples_scenario_start_event_quantity_status",
                "exposure_trading_days_consistency_and_terminal_state",
            ],
            "fixed_feature_clipping": [-2.0, 2.0],
            "correlation_minimum": 0.995,
            "rmse_maximum": 0.05,
            "terminal_agreement_minimum": 0.95,
            "routing_tuple_jaccard_minimum": 0.90,
            "linkage": "COMPLETE_LINK_GREEDY_IN_LEXICOGRAPHIC_POLICY_ORDER",
            "thresholds_selected_from_campaign_outcomes": False,
            "preregistered_before_campaign": False,
        },
        "candidate_count": len(candidate_ids),
        "cluster_count": len(rows),
        "clusters": rows,
        "membership": dict(sorted(membership.items())),
    }


def _candidate_controls(
    candidate: Mapping[str, Any], controls: Mapping[str, Any]
) -> dict[str, Any]:
    policy_id = str(candidate["policy_id"])
    baselines = {
        "static_partition": controls["static_partition"],
        "best_individual_sleeve": controls["best_standalone"],
        "equal_risk_active_pool": controls["equal_risk_active_pool"],
        "always_on_pooled_governor": controls["always_on_pooled_governor"],
    }
    random_control = (controls.get("random_priority_by_policy") or {}).get(policy_id)
    if random_control is None:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} lacks matched random-priority control"
        )
    baselines["matched_random_priority"] = random_control
    output: dict[str, Any] = {}
    for name, baseline in baselines.items():
        output[name] = {
            scenario: _delta(candidate[scenario], baseline[scenario])
            for scenario in SCENARIOS
        }
    match = (controls.get("random_priority_exposure_match_by_policy") or {}).get(
        policy_id
    )
    if not isinstance(match, Mapping):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} lacks random-priority exposure-match evidence"
        )
    if bool(match.get("economic_outcomes_used_for_selection", False)):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random control used economic outcomes"
        )
    output["matched_random_priority"]["exposure_matching"] = dict(match)
    output["standalone_sleeves"] = {
        str(baseline["policy_id"]): {
            scenario: _delta(candidate[scenario], baseline[scenario])
            for scenario in SCENARIOS
        }
        for baseline in controls.get("standalone_controls") or ()
    }
    return output


def _candidate_summary(
    row: Mapping[str, Any],
    blocks: Sequence[BlockSpec],
    controls: Mapping[str, Any],
    promoted96: set[str],
    surviving96: set[str],
    finalists: set[str],
) -> tuple[dict[str, Any], list[Mapping[str, Any]]]:
    policy_id = str(row.get("policy_id") or "")
    if not policy_id:
        raise ActiveRiskDecisionReportError("Stage-3 row lacks policy id")
    canonical_raw = [
        raw
        for raw in row.get("evidence_raw") or ()
        if str(raw.get("horizon_label")) == CANONICAL_HORIZON
    ]
    by_scenario = Counter(_scenario_key(raw.get("scenario")) for raw in canonical_raw)
    candidate_blocks: dict[str, dict[str, BlockAccumulator]] = {
        scenario: {block.block_id: BlockAccumulator() for block in blocks}
        for scenario in SCENARIOS
    }
    for raw in canonical_raw:
        scenario = _scenario_key(raw.get("scenario"))
        block = _block_for_day(int(raw["start_day"]), blocks)
        candidate_blocks[scenario][block].add(raw)
    for scenario in SCENARIOS:
        summary = row.get(scenario) or {}
        if by_scenario[scenario] != int(summary.get("episode_count", -1)):
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} canonical {scenario} coverage drift"
            )
        if sum(
            value.pass_count for value in candidate_blocks[scenario].values()
        ) != int(summary.get("pass_count", -1)):
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} canonical {scenario} pass drift"
            )
        if sum(
            value.mll_breach_count for value in candidate_blocks[scenario].values()
        ) != int(summary.get("mll_breach_count", -1)):
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} canonical {scenario} MLL drift"
            )
    compact = {
        "policy_id": policy_id,
        "structural_fingerprint": row.get("structural_fingerprint"),
        "actual_account_behavior_fingerprint": row.get(
            "actual_account_behavior_fingerprint"
        ),
        "normal": _summary_view(row.get("normal") or {}),
        "stressed": _summary_view(row.get("stressed") or {}),
        "horizons": _metric_view(row)["horizons"],
        "blocks": {
            scenario: {
                key: value.to_dict()
                for key, value in candidate_blocks[scenario].items()
            }
            for scenario in SCENARIOS
        },
        "risk_utilisation": dict(row.get("risk_utilisation") or {}),
        "suppression": dict(row.get("suppression") or {}),
        "promotion": {
            "promoted_to_96": policy_id in promoted96,
            "survived_96": policy_id in surviving96,
            "development_finalist": policy_id in finalists,
            "promotion_mutated_by_report": False,
        },
    }
    compact["control_deltas"] = _candidate_controls(compact, controls)
    return compact, canonical_raw


def build_active_risk_decision_report(
    *,
    manifest_path: Path,
    stage3_cache_dir: Path,
    matched_controls_path: Path,
    halving_dir: Path,
    expected_stage3_count: int = 256,
) -> dict[str, Any]:
    """Build the revision-02 report while holding at most one Stage-3 cache."""

    manifest = _load_json(manifest_path)
    if not isinstance(manifest, Mapping):
        raise ActiveRiskDecisionReportError("manifest is not an object")
    blocks = _block_specs(manifest)
    controls, controls_provenance = _load_controls(matched_controls_path)
    halving, halving_provenance = _load_halving(halving_dir)
    promoted96 = set(halving["stage3"]["selected_policy_ids"])
    surviving96 = set((halving.get("stage4") or {}).get("selected_policy_ids") or ())
    finalists = set((halving.get("stage5") or {}).get("selected_policy_ids") or ())

    paths = sorted(stage3_cache_dir.glob("batch_*.json"))
    if len(paths) != expected_stage3_count:
        raise ActiveRiskDecisionReportError(
            f"Stage-3 cache count is {len(paths)}, expected {expected_stage3_count}"
        )
    horizon_accumulators = {
        scenario: {label: HorizonAccumulator() for label in HORIZONS}
        for scenario in SCENARIOS
    }
    block_accumulators = {
        scenario: {block.block_id: BlockAccumulator() for block in blocks}
        for scenario in SCENARIOS
    }
    risk = RiskAccumulator()
    suppression = SuppressionAccumulator()
    lifecycle = {
        scenario: {path: LifecyclePathAccumulator() for path in PATHS}
        for scenario in SCENARIOS
    }
    candidates: list[dict[str, Any]] = []
    candidates_by_id: dict[str, dict[str, Any]] = {}
    vectors: dict[str, np.ndarray] = {}
    vector_terminals: dict[str, tuple[int, ...]] = {}
    vector_hashes: dict[str, str] = {}
    vector_routing_tuples: dict[
        str, frozenset[tuple[str, int, str, int, str]]
    ] = {}
    vector_key_reference: list[tuple[str, int]] | None = None
    cache_provenance: list[dict[str, Any]] = []

    for path in paths:
        payload = _load_json(path)
        if not isinstance(payload, Mapping):
            raise ActiveRiskDecisionReportError(f"Stage-3 cache is malformed: {path}")
        if payload.get("schema") != "hydra_active_risk_stage_batch_v1":
            raise ActiveRiskDecisionReportError(f"Stage-3 schema drift: {path}")
        if payload.get("stage") != "stage3":
            raise ActiveRiskDecisionReportError(f"Stage-3 identity drift: {path}")
        rows = payload.get("rows")
        if not isinstance(rows, list) or len(rows) != 1:
            raise ActiveRiskDecisionReportError(f"Stage-3 batch cardinality drift: {path}")
        claimed_rows_hash = str(payload.get("rows_hash") or "")
        if canonical_hash(rows) != claimed_rows_hash:
            raise ActiveRiskDecisionReportError(f"Stage-3 rows_hash drift: {path}")
        row = rows[0]
        compact, canonical_raw = _candidate_summary(
            row, blocks, controls, promoted96, surviving96, finalists
        )
        policy_id = str(compact["policy_id"])
        if policy_id in candidates_by_id:
            raise ActiveRiskDecisionReportError(f"duplicate Stage-3 policy {policy_id}")

        for scenario in SCENARIOS:
            for label in HORIZONS:
                summary = ((row.get("horizons") or {}).get(scenario) or {}).get(label)
                if not isinstance(summary, Mapping):
                    raise ActiveRiskDecisionReportError(
                        f"candidate {policy_id} lacks {scenario} {label}"
                    )
                horizon_accumulators[scenario][label].add(summary)
            full_attempts = int(
                row["horizons"][scenario][FULL_HORIZON].get("episode_count", 0)
            )
            for lifecycle_path in PATHS:
                lifecycle[scenario][lifecycle_path].add_attempts(full_attempts)

        lifecycle_rows = list(row.get("lifecycle_rows") or ())
        expected_lifecycle = sum(
            int(row["horizons"][scenario][FULL_HORIZON].get("pass_count", 0))
            for scenario in SCENARIOS
        )
        if len(lifecycle_rows) != expected_lifecycle:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} FULL-pass/XFA cardinality drift"
            )
        candidate_lifecycle = {
            scenario: {name: LifecyclePathAccumulator() for name in PATHS}
            for scenario in SCENARIOS
        }
        for scenario in SCENARIOS:
            attempts = int(row["horizons"][scenario][FULL_HORIZON]["episode_count"])
            for name in PATHS:
                candidate_lifecycle[scenario][name].add_attempts(attempts)
        for lifecycle_row in lifecycle_rows:
            scenario = _scenario_key(lifecycle_row.get("scenario"))
            for lifecycle_path in PATHS:
                value = lifecycle_row.get(lifecycle_path)
                if not isinstance(value, Mapping):
                    raise ActiveRiskDecisionReportError(
                        f"candidate {policy_id} lacks {lifecycle_path} XFA path"
                    )
                lifecycle[scenario][lifecycle_path].add_path(value)
                candidate_lifecycle[scenario][lifecycle_path].add_path(value)
        compact["xfa_lifecycle"] = {
            scenario: {
                name: candidate_lifecycle[scenario][name].to_dict()
                for name in PATHS
            }
            for scenario in SCENARIOS
        }

        for raw in canonical_raw:
            scenario = _scenario_key(raw.get("scenario"))
            block_id = _block_for_day(int(raw["start_day"]), blocks)
            block_accumulators[scenario][block_id].add(raw)
        risk.add(row.get("risk_utilisation") or {})
        suppression.add(row.get("suppression") or {})
        if policy_id in promoted96:
            (
                vector,
                terminal_vector,
                vector_hash,
                vector_keys,
                routing_tuples,
            ) = _behavior_vector(canonical_raw)
            if vector_key_reference is None:
                vector_key_reference = vector_keys
            elif vector_keys != vector_key_reference:
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} canonical behavior-key drift"
                )
            vectors[policy_id] = vector
            vector_terminals[policy_id] = terminal_vector
            vector_hashes[policy_id] = vector_hash
            vector_routing_tuples[policy_id] = routing_tuples
            compact["posthoc_behavior_vector_hash"] = vector_hash
            compact["posthoc_routing_tuple_hash"] = canonical_hash(
                sorted(routing_tuples)
            )
        candidates.append(compact)
        candidates_by_id[policy_id] = compact
        cache_provenance.append(
            {"path": str(path), "rows_hash": claimed_rows_hash, "row_count": 1}
        )
        # Deliberately discard outcome-rich rows before opening the next batch.
        del lifecycle_rows, canonical_raw, row, rows, payload

    observed_ids = set(candidates_by_id)
    if not promoted96.issubset(observed_ids):
        raise ActiveRiskDecisionReportError("Stage-3 promotions reference absent policies")
    if not surviving96.issubset(promoted96):
        raise ActiveRiskDecisionReportError("96-start survivors were not Stage-3 promotions")
    if not finalists.issubset(surviving96):
        raise ActiveRiskDecisionReportError("finalists were not 96-start survivors")

    clustering = _clusters(
        sorted(promoted96),
        vectors,
        vector_terminals,
        vector_hashes,
        vector_routing_tuples,
        candidates_by_id,
    )
    for candidate in candidates:
        candidate["posthoc_behavioral_cluster"] = clustering["membership"].get(
            candidate["policy_id"]
        )
    candidates.sort(key=lambda row: str(row["policy_id"]))

    report = {
        "schema": REPORT_SCHEMA,
        "revision": REPORT_REVISION,
        "report_id": f"{CAMPAIGN_ID}_{REPORT_REVISION}",
        "campaign_id": CAMPAIGN_ID,
        "report_only": True,
        "runtime_or_manifest_mutated": False,
        "promotion_or_selection_mutated": False,
        "development_only": True,
        "canonical_horizon": CANONICAL_HORIZON,
        "integrity": {
            "stage3_expected_policy_count": expected_stage3_count,
            "stage3_validated_policy_count": len(candidates),
            "stage3_rows_hashes_valid": True,
            "unique_policy_ids": len(candidates_by_id),
            "matched_controls_hash_valid": True,
            "halving_hashes_valid": True,
        },
        "funnel": {
            "stage3_policy_count": len(candidates),
            "promoted_to_96_count": len(promoted96),
            "survived_96_count": len(surviving96),
            "development_finalist_count": len(finalists),
            "promoted_to_96_ids": sorted(promoted96),
            "survived_96_ids": sorted(surviving96),
            "development_finalist_ids": sorted(finalists),
            "halving_decisions": halving,
        },
        "horizon_distributions": {
            scenario: {
                label: horizon_accumulators[scenario][label].to_dict()
                for label in HORIZONS
            }
            for scenario in SCENARIOS
        },
        "temporal_blocks": {
            "definitions": [block.to_dict() for block in blocks],
            "results": {
                scenario: {
                    block.block_id: block_accumulators[scenario][
                        block.block_id
                    ].to_dict()
                    for block in blocks
                }
                for scenario in SCENARIOS
            },
        },
        "risk_utilisation": risk.to_dict(),
        "suppression_and_foregone_pnl": suppression.to_dict(),
        "matched_controls": {
            key: value
            for key, value in controls.items()
            if key not in {
                "random_priority_by_policy",
                "random_priority_exposure_match_by_policy",
            }
        },
        "candidate_control_delta_method": {
            "formula": "candidate_metric_minus_control_metric",
            "canonical_horizon": CANONICAL_HORIZON,
            "controls": [
                "static_partition",
                "best_individual_sleeve",
                "every_component_sleeve_standalone",
                "equal_risk_active_pool",
                "always_on_pooled_governor",
                "matched_random_priority",
            ],
            "random_priority_matching_uses_economic_outcomes": False,
        },
        "candidates": candidates,
        "xfa_lifecycle": {
            "scope": "STAGE3_48_STARTS_FULL_CHRONOLOGICAL_HORIZON",
            "paths_are_alternative_not_additive": True,
            "expected_payout_denominator": (
                "ALL_FULL_HORIZON_COMBINE_ATTEMPTS_IN_THE_SAME_COST_SCENARIO"
            ),
            "normal": {
                path: lifecycle["normal"][path].to_dict() for path in PATHS
            },
            "stressed": {
                path: lifecycle["stressed"][path].to_dict() for path in PATHS
            },
        },
        "posthoc_behavioral_clustering": clustering,
        "known_interpretation_limits": [
            "All evidence is development-only and is not independent confirmation.",
            "Risk utilisation is declared nominal risk because stop-risk paths are unavailable.",
            "Foregone realized PnL is ex-post diagnostic and was never a routing input.",
            "Standard and Consistency XFA paths are alternatives and must not be added as realizable payout.",
            "Behavioral clusters are deterministic post-hoc reporting groups and never alter frozen promotions.",
        ],
        "provenance": {
            "manifest": {
                "path": str(manifest_path),
                "sha256": file_sha256(manifest_path),
            },
            "stage3_caches": cache_provenance,
            "matched_controls": controls_provenance,
            "halving": halving_provenance,
        },
    }
    report["report_hash"] = canonical_hash(report)
    return report


def _percent(value: Any) -> str:
    return f"{100.0 * _float(value):.2f}%"


def _money(value: Any) -> str:
    return f"${_float(value):,.2f}"


def render_markdown(report: Mapping[str, Any]) -> str:
    """Render a compact human report; the JSON remains the complete matrix."""

    lines = [
        "# HYDRA Active-Risk Pool — Decision Report revision_02",
        "",
        "Development-only, post-hoc reporting. This report does not alter the frozen manifest, selection, or promotions.",
        "",
        "## Funnel",
        "",
        "| Stage-3 policies | Promoted to 96 | Survived 96 | Finalists |",
        "|---:|---:|---:|---:|",
        (
            f"| {report['funnel']['stage3_policy_count']} | "
            f"{report['funnel']['promoted_to_96_count']} | "
            f"{report['funnel']['survived_96_count']} | "
            f"{report['funnel']['development_finalist_count']} |"
        ),
        "",
        "## Frozen-horizon economics",
        "",
        "| Horizon | Cost | Passes / episodes | Pass rate | Target P25 (policy median) | Target median (policy median) | MLL rate | Min-buffer median |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for label in HORIZONS:
        for scenario in SCENARIOS:
            value = report["horizon_distributions"][scenario][label]
            distributions = value["policy_level_distributions"]
            lines.append(
                f"| {label} | {scenario} | {value['pass_count']} / {value['episode_count']} | "
                f"{_percent(value['pass_rate'])} | "
                f"{_percent(distributions['target_progress_p25']['median'])} | "
                f"{_percent(distributions['target_progress_median']['median'])} | "
                f"{_percent(value['mll_breach_rate'])} | "
                f"{_money(distributions['minimum_mll_buffer']['median'])} |"
            )
    lines.extend(
        [
            "",
            "## Independent temporal blocks — canonical 90-day horizon",
            "",
            "| Block | Cost | Passes / episodes | Pass rate | Target P25 | Target median | MLL rate | Min-buffer median | Net median |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for block in report["temporal_blocks"]["definitions"]:
        block_id = block["block_id"]
        for scenario in SCENARIOS:
            value = report["temporal_blocks"]["results"][scenario][block_id]
            lines.append(
                f"| {block_id} | {scenario} | {value['pass_count']} / {value['episode_count']} | "
                f"{_percent(value['pass_rate'])} | "
                f"{_percent(value['target_progress']['p25'])} | "
                f"{_percent(value['target_progress']['median'])} | "
                f"{_percent(value['mll_breach_rate'])} | "
                f"{_money(value['minimum_mll_buffer']['median'])} | "
                f"{_money(value['net_pnl']['median'])} |"
            )
    lines.extend(
        [
            "",
            "## Risk utilisation and suppression",
            "",
            f"- Declared nominal-risk utilisation mean: {_percent(report['risk_utilisation']['mean'])}.",
            f"- Signals emitted / accepted / rejected: {report['suppression_and_foregone_pnl']['signals_emitted']} / {report['suppression_and_foregone_pnl']['signals_accepted']} / {report['suppression_and_foregone_pnl']['signals_rejected']}.",
            f"- Foregone realized PnL, ex-post diagnostic only: {_money(report['suppression_and_foregone_pnl']['foregone_realized_pnl_ex_post'])}.",
            "",
            "## XFA lifecycle — paths reported separately",
            "",
            "| Cost | Path | Combine attempts | XFA paths | First payouts | First-payout / attempt | Payout cycles | Expected trader payout / attempt | Post-payout survival |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for scenario in SCENARIOS:
        for path in PATHS:
            value = report["xfa_lifecycle"][scenario][path]
            lines.append(
                f"| {scenario} | {path} | {value['combine_attempts']} | "
                f"{value['xfa_paths_started']} | {value['first_payouts']} | "
                f"{_percent(value['first_payout_probability_per_combine_attempt'])} | "
                f"{value['payout_cycles']} | "
                f"{_money(value['expected_trader_payout_per_combine_attempt'])} | "
                f"{_percent(value['post_payout_survival_rate_conditional_on_first_payout'])} |"
            )
    lines.extend(
        [
            "",
            "## Post-hoc behavioral clusters of Stage-3 promotions",
            "",
            "| Cluster | Representative | Members | Exact vectors |",
            "|---|---|---:|---|",
        ]
    )
    for cluster in report["posthoc_behavioral_clustering"]["clusters"]:
        lines.append(
            f"| {cluster['cluster_id']} | {cluster['representative_id']} | "
            f"{cluster['member_count']} | {str(cluster['exact_vector_equivalent']).lower()} |"
        )
    lines.extend(
        [
            "",
            "## Candidate matrix — canonical 90-day horizon",
            "",
            "| Candidate | N pass | S pass | N target median | S target median | S target P25 | S net median | S MLL | Min buffer | To 96 | Cluster |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for candidate in report["candidates"]:
        normal = candidate["normal"]
        stressed = candidate["stressed"]
        lines.append(
            f"| {candidate['policy_id']} | {normal['pass_count']} | {stressed['pass_count']} | "
            f"{_percent(normal['target_progress_median'])} | "
            f"{_percent(stressed['target_progress_median'])} | "
            f"{_percent(stressed['target_progress_p25'])} | "
            f"{_money(stressed['net_median'])} | {_percent(stressed['mll_breach_rate'])} | "
            f"{_money(stressed['minimum_mll_buffer'])} | "
            f"{str(candidate['promotion']['promoted_to_96']).lower()} | "
            f"{candidate.get('posthoc_behavioral_cluster') or '-'} |"
        )
    lines.extend(
        [
            "",
            "Complete candidate-control deltas, B1–B4 candidate rows, risk paths, suppression and provenance are in the companion JSON.",
            "",
            f"Report hash: `{report['report_hash']}`",
            "",
        ]
    )
    return "\n".join(lines)


def write_active_risk_decision_report(
    report: Mapping[str, Any], *, json_path: Path, markdown_path: Path
) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_text = json.dumps(report, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    markdown_text = render_markdown(report)
    for path, text in ((json_path, json_text), (markdown_path, markdown_text)):
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument("--expected-stage3-count", type=int, default=256)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    report_dir = (
        root
        / "reports/economic_evolution"
        / "active_risk_pool_target_velocity_0026_revision_02"
    )
    report = build_active_risk_decision_report(
        manifest_path=(
            root
            / "config/v7/active_risk_pool_target_velocity_0026_revision_02.json"
        ),
        stage3_cache_dir=(
            root
            / "data/cache/economic_production"
            / CAMPAIGN_ID
            / "stage3_active_batches"
        ),
        matched_controls_path=report_dir / "matched_controls.json",
        halving_dir=report_dir / "successive_halving",
        expected_stage3_count=args.expected_stage3_count,
    )
    write_active_risk_decision_report(
        report,
        json_path=report_dir / "decision_report_revision_02.json",
        markdown_path=report_dir / "decision_report_revision_02.md",
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "ActiveRiskDecisionReportError",
    "build_active_risk_decision_report",
    "canonical_hash",
    "render_markdown",
    "write_active_risk_decision_report",
]
