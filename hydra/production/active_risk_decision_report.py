"""Streaming, report-only decision audit for active-risk campaign 0026.

The production runtime deliberately persists outcome-rich Stage-3 batches.  A
single batch can be large because it contains exact daily account paths and XFA
ledgers.  This module consumes and releases one batch at a time, reproduces its
sealed EvidenceBundle partitions, and builds a compact decision report without
changing selection, promotion, manifests, or authoritative campaign evidence.
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
from types import SimpleNamespace
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.evidence import (
    EvidenceBundleError,
    EvidenceContractError,
    RECORD_SPECS,
    verify_evidence_bundle,
)
from hydra.production.active_risk_runtime import (
    ActiveRiskRuntimeError,
    _active_pool_lifecycle_evidence,
    _exposure_signature,
    _failure_vectors,
    _suppression_summary,
    _utilisation_summary,
)
from hydra.production.episode_evidence import _convert_episode


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
EXPECTED_EPISODE_STARTS_PER_SCENARIO = 48
COMBINE_TERMINALS = (
    "TARGET_REACHED",
    "MLL_BREACHED",
    "HARD_RULE_FAILURE",
    "DATA_CENSORED",
    "OPERATIONAL_HORIZON_NOT_REACHED",
)
EXPOSURE_MATCH_FIELDS = (
    "time_weighted_mini_nanoseconds_per_observed_day",
    "accepted_event_rate",
)
EXPOSURE_SIGNATURE_FIELDS = (
    "time_weighted_mini_nanoseconds_per_observed_day",
    "accepted_event_rate",
    "accepted_event_count",
    "emitted_event_count",
    "observed_episode_days",
    "outcome_fields_used",
)


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


def _stage3_projected_signatures(
    metric: Mapping[str, Any], manifest: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    """Reproduce writer payload hashes while materialising one episode at a time."""

    episode_digest = hashlib.sha256()
    daily_digest = hashlib.sha256()
    episode_count = 0
    daily_count = 0
    lifecycle_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
    for lifecycle in metric.get("lifecycle_rows") or ():
        if not isinstance(lifecycle, Mapping):
            raise ActiveRiskDecisionReportError(
                "Stage-3 lifecycle projection contains a malformed row"
            )
        key = (str(lifecycle["scenario"]), int(lifecycle["combine_start_day"]))
        if key in lifecycle_by_key:
            raise ActiveRiskDecisionReportError(
                f"duplicate Stage-3 lifecycle projection key {key}"
            )
        lifecycle_by_key[key] = lifecycle
    failure = _failure_vectors(metric)[0]

    def raw_sort_key(raw: Mapping[str, Any]) -> tuple[str, ...]:
        policy_id = str(raw["policy_id"])
        return (
            policy_id,
            f"{policy_id}:{int(raw['start_day'])}",
            str(
                raw.get("horizon_label")
                or f"{int(raw['horizon_trading_days'])}_TRADING_DAYS"
            ),
            str(raw["scenario"]),
        )

    def update_digest(digest: Any, row: Mapping[str, Any]) -> None:
        digest.update(
            (
                json.dumps(
                    row,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
        )

    for raw in sorted(metric.get("evidence_raw") or (), key=raw_sort_key):
        scenario = str(raw["scenario"])
        try:
            episode, paths = _convert_episode(raw, manifest, scenario, failure)
            if (
                str(raw.get("horizon_label")) == FULL_HORIZON
                and str(raw.get("terminal_classification")) == "TARGET_REACHED"
            ):
                key = (scenario, int(raw["start_day"]))
                lifecycle = lifecycle_by_key.pop(key, None)
                if lifecycle is None:
                    raise ActiveRiskDecisionReportError(
                        f"FULL-pass episode lacks lifecycle projection {key}"
                    )
                episode["active_risk_pool_lifecycle"] = (
                    _active_pool_lifecycle_evidence(
                        lifecycle,
                        expected_policy_id=str(raw["policy_id"]),
                        expected_scenario=scenario,
                        expected_start_day=int(raw["start_day"]),
                    )
                )
            checked_episode = RECORD_SPECS["episodes"].validate(
                episode, campaign_id=CAMPAIGN_ID
            )
            update_digest(episode_digest, checked_episode)
            episode_count += 1
            paths.sort(
                key=lambda row: tuple(
                    str(row[field])
                    for field in RECORD_SPECS["account_daily_paths"].sort_fields
                )
            )
            for path in paths:
                checked_path = RECORD_SPECS["account_daily_paths"].validate(
                    path, campaign_id=CAMPAIGN_ID
                )
                update_digest(daily_digest, checked_path)
                daily_count += 1
        except (
            ActiveRiskRuntimeError,
            EvidenceContractError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise ActiveRiskDecisionReportError(
                f"Stage-3 cache cannot reproduce sealed evidence: {exc}"
            ) from exc
        finally:
            if "checked_path" in locals():
                del checked_path
            if "checked_episode" in locals():
                del checked_episode
            if "episode" in locals():
                del episode
            if "paths" in locals():
                del paths
    if lifecycle_by_key:
        raise ActiveRiskDecisionReportError(
            "orphan Stage-3 lifecycle projections remain after evidence replay"
        )
    return {
        "episodes": {
            "row_count": episode_count,
            "payload_sha256": episode_digest.hexdigest(),
        },
        "account_daily_paths": {
            "row_count": daily_count,
            "payload_sha256": daily_digest.hexdigest(),
        },
    }


def _stage3_partition_index(
    bundle_manifest: Mapping[str, Any], *, expected_policy_count: int
) -> dict[tuple[str, str], Mapping[str, Any]]:
    files = bundle_manifest.get("files")
    if not isinstance(files, Mapping):
        raise ActiveRiskDecisionReportError(
            "verified EvidenceBundle lacks its file manifest"
        )
    observed: dict[tuple[str, str], Mapping[str, Any]] = {}
    for details in files.values():
        if not isinstance(details, Mapping) or details.get("kind") != "dataset_partition":
            continue
        dataset = str(details.get("dataset") or "")
        batch_id = str(details.get("batch_id") or "")
        if dataset not in {"episodes", "account_daily_paths"} or not batch_id.startswith(
            "active:stage3:"
        ):
            continue
        key = (dataset, batch_id)
        if key in observed:
            raise ActiveRiskDecisionReportError(
                f"duplicate Stage-3 EvidenceBundle partition declaration {key}"
            )
        observed[key] = details
    expected = {
        (
            dataset,
            f"active:stage3:{index:06d}:{suffix}",
        )
        for index in range(expected_policy_count)
        for dataset, suffix in (
            ("episodes", "episodes"),
            ("account_daily_paths", "daily"),
        )
    }
    if set(observed) != expected:
        missing = sorted(expected - set(observed))
        extra = sorted(set(observed) - expected)
        raise ActiveRiskDecisionReportError(
            "Stage-3 EvidenceBundle partition coverage drift: "
            f"missing={missing[:3]!r}, extra={extra[:3]!r}"
        )
    return observed


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


def _required_float(value: Any, *, label: str) -> float:
    if value is None:
        raise ActiveRiskDecisionReportError(f"{label} is missing")
    try:
        return _float(value)
    except (TypeError, ValueError) as exc:
        raise ActiveRiskDecisionReportError(f"{label} is not numeric") from exc


def _assert_close(actual: Any, expected: Any, *, label: str) -> None:
    left = _required_float(actual, label=f"{label} actual")
    right = _required_float(expected, label=f"{label} expected")
    if not math.isclose(left, right, rel_tol=1e-10, abs_tol=1e-8):
        raise ActiveRiskDecisionReportError(
            f"{label} drift: raw={left!r}, summary={right!r}"
        )


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


def _combine_terminal_metrics(
    summary: Mapping[str, Any], *, label: str
) -> dict[str, Any]:
    episode_count = int(summary.get("episode_count", -1))
    if episode_count < 0:
        raise ActiveRiskDecisionReportError(f"{label} episode count is absent")
    raw_distribution = summary.get("terminal_distribution")
    if not isinstance(raw_distribution, Mapping):
        raise ActiveRiskDecisionReportError(f"{label} terminal distribution is absent")
    unknown = set(str(key) for key in raw_distribution) - set(COMBINE_TERMINALS)
    if unknown:
        raise ActiveRiskDecisionReportError(
            f"{label} has unknown terminal classifications: {sorted(unknown)}"
        )
    distribution = {
        terminal: int(raw_distribution.get(terminal, 0))
        for terminal in COMBINE_TERMINALS
    }
    if any(value < 0 for value in distribution.values()):
        raise ActiveRiskDecisionReportError(f"{label} has negative terminal counts")
    if sum(distribution.values()) != episode_count:
        raise ActiveRiskDecisionReportError(
            f"{label} terminal distribution does not cover every episode"
        )
    pass_count = int(summary.get("pass_count", -1))
    breach_count = int(summary.get("mll_breach_count", -1))
    if pass_count != distribution["TARGET_REACHED"]:
        raise ActiveRiskDecisionReportError(f"{label} pass terminal count drift")
    if breach_count != distribution["MLL_BREACHED"]:
        raise ActiveRiskDecisionReportError(f"{label} MLL terminal count drift")
    data_censored = distribution["DATA_CENSORED"]
    operational = distribution["OPERATIONAL_HORIZON_NOT_REACHED"]
    combined_censored = data_censored + operational
    if int(summary.get("censored_episode_count", -1)) != combined_censored:
        raise ActiveRiskDecisionReportError(f"{label} combined censor count drift")
    evaluable_count = episode_count - data_censored
    return {
        "terminal_distribution": {
            key: value for key, value in distribution.items() if value
        },
        "data_censored_episode_count": data_censored,
        "operational_horizon_not_reached_count": operational,
        "combine_evaluable_episode_count": evaluable_count,
        "pass_rate_raw_lower_bound": (
            pass_count / episode_count if episode_count else 0.0
        ),
        "pass_rate_evaluable": (
            pass_count / evaluable_count if evaluable_count else None
        ),
        "mll_breach_rate_raw_lower_bound": (
            breach_count / episode_count if episode_count else 0.0
        ),
        "mll_breach_rate_evaluable": (
            breach_count / evaluable_count if evaluable_count else None
        ),
    }


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
    data_censored_count: int = 0
    operational_horizon_not_reached_count: int = 0
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
        self.data_censored_count += int(terminal == "DATA_CENSORED")
        self.operational_horizon_not_reached_count += int(
            terminal == "OPERATIONAL_HORIZON_NOT_REACHED"
        )
        self.consistency_ok_count += int(bool(raw.get("consistency_ok")))
        self.target_progress.append(
            _required_float(raw.get("target_progress"), label="episode target progress")
        )
        self.minimum_mll_buffer.append(
            _required_float(raw.get("minimum_mll_buffer"), label="episode MLL buffer")
        )
        self.net_pnl.append(_required_float(raw.get("net_pnl"), label="episode net PnL"))
        if raw.get("days_to_target") is not None:
            self.days_to_target.append(_float(raw["days_to_target"]))

    def to_dict(self) -> dict[str, Any]:
        denominator = max(self.episode_count, 1)
        evaluable_count = self.episode_count - self.data_censored_count
        return {
            "episode_count": self.episode_count,
            "pass_count": self.pass_count,
            "pass_rate": self.pass_count / denominator,
            "pass_rate_raw_lower_bound": self.pass_count / denominator,
            "pass_rate_evaluable": (
                self.pass_count / evaluable_count if evaluable_count else None
            ),
            "mll_breach_count": self.mll_breach_count,
            "mll_breach_rate": self.mll_breach_count / denominator,
            "mll_breach_rate_raw_lower_bound": self.mll_breach_count / denominator,
            "mll_breach_rate_evaluable": (
                self.mll_breach_count / evaluable_count if evaluable_count else None
            ),
            "censored_count": self.censored_count,
            "data_censored_episode_count": self.data_censored_count,
            "operational_horizon_not_reached_count": (
                self.operational_horizon_not_reached_count
            ),
            "combine_evaluable_episode_count": evaluable_count,
            "terminal_distribution": {
                key: value
                for key, value in {
                    "TARGET_REACHED": self.pass_count,
                    "MLL_BREACHED": self.mll_breach_count,
                    "DATA_CENSORED": self.data_censored_count,
                    "OPERATIONAL_HORIZON_NOT_REACHED": (
                        self.operational_horizon_not_reached_count
                    ),
                    "HARD_RULE_FAILURE": self.episode_count
                    - self.pass_count
                    - self.mll_breach_count
                    - self.data_censored_count
                    - self.operational_horizon_not_reached_count,
                }.items()
                if value
            },
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
    data_censored_count: int = 0
    operational_horizon_not_reached_count: int = 0
    terminal_distribution: Counter[str] = field(default_factory=Counter)
    policy_values: dict[str, list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add(self, summary: Mapping[str, Any]) -> None:
        terminal_metrics = _combine_terminal_metrics(
            summary, label="horizon summary"
        )
        self.policy_count += 1
        self.episode_count += int(summary.get("episode_count", 0))
        self.pass_count += int(summary.get("pass_count", 0))
        self.mll_breach_count += int(summary.get("mll_breach_count", 0))
        self.censored_count += int(summary.get("censored_episode_count", 0))
        self.data_censored_count += int(
            terminal_metrics["data_censored_episode_count"]
        )
        self.operational_horizon_not_reached_count += int(
            terminal_metrics["operational_horizon_not_reached_count"]
        )
        self.terminal_distribution.update(
            terminal_metrics["terminal_distribution"]
        )
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
            "duration_trading_days_median",
            "active_trading_days_median",
            "calendar_days_median",
            "monthly_subscription_duration_proxy_median",
            "censoring_rate",
            "maximum_block_profit_share",
            "maximum_sleeve_profit_share",
        ):
            if summary.get(key) is not None:
                self.policy_values[key].append(_float(summary[key]))
        for key in (
            "pass_rate_raw_lower_bound",
            "pass_rate_evaluable",
            "mll_breach_rate_raw_lower_bound",
            "mll_breach_rate_evaluable",
        ):
            if terminal_metrics[key] is not None:
                self.policy_values[key].append(_float(terminal_metrics[key]))

    def to_dict(self) -> dict[str, Any]:
        denominator = max(self.episode_count, 1)
        evaluable_count = self.episode_count - self.data_censored_count
        return {
            "policy_count": self.policy_count,
            "episode_count": self.episode_count,
            "pass_count": self.pass_count,
            "pass_rate": self.pass_count / denominator,
            "pass_rate_raw_lower_bound": self.pass_count / denominator,
            "pass_rate_evaluable": (
                self.pass_count / evaluable_count if evaluable_count else None
            ),
            "mll_breach_count": self.mll_breach_count,
            "mll_breach_rate": self.mll_breach_count / denominator,
            "mll_breach_rate_raw_lower_bound": self.mll_breach_count / denominator,
            "mll_breach_rate_evaluable": (
                self.mll_breach_count / evaluable_count if evaluable_count else None
            ),
            "censored_episode_count": self.censored_count,
            "data_censored_episode_count": self.data_censored_count,
            "operational_horizon_not_reached_count": (
                self.operational_horizon_not_reached_count
            ),
            "combine_evaluable_episode_count": evaluable_count,
            "terminal_distribution": dict(sorted(self.terminal_distribution.items())),
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
        if count > 0:
            self.weighted_mean += _required_float(
                value.get("mean"), label="risk mean"
            ) * count
            self.policy_medians.append(_required_float(value.get("median"), label="risk median"))
            self.policy_p25.append(_required_float(value.get("p25"), label="risk p25"))
            self.policy_p75.append(_required_float(value.get("p75"), label="risk p75"))
        source = value.get("by_active_sleeve_count") or {}
        for key, target in self.groups.items():
            row = source.get(key) or {}
            observations = int(row.get("observation_count", 0))
            target["count"] += observations
            if observations > 0:
                target["weighted_mean"] += _required_float(
                    row.get("mean"), label=f"risk {key} mean"
                ) * observations
                target["medians"].append(
                    _required_float(row.get("median"), label=f"risk {key} median")
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_measure": (
                "NORMAL_CANONICAL_90_DAY_DECISION_EVENT_"
                "DECLARED_NOMINAL_RISK_UTILISATION"
            ),
            "scope": "NORMAL_CANONICAL_90_DAY_RISK_BEFORE_AND_AFTER_DECISION_EVENTS",
            "actual_stop_risk_available": False,
            "time_weighted_utilisation": False,
            "duty_cycle_measure": False,
            "interpretation": (
                "DECLARED_NOMINAL_RISK_CHARGE_DIVIDED_BY_CURRENTLY_ADMISSIBLE_"
                "ACCOUNT_RISK_AT_RECORDED_DECISION_EVENTS_ONLY"
            ),
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
class ExposureAccumulator:
    policy_count: int = 0
    fields: dict[str, list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )
    outcome_fields_used_values: set[bool] = field(default_factory=set)

    def add(self, signature: Mapping[str, Any]) -> None:
        if str(signature.get("schema") or "") != "hydra_active_risk_exposure_signature_v1":
            raise ActiveRiskDecisionReportError("candidate exposure-signature schema drift")
        missing = set(EXPOSURE_SIGNATURE_FIELDS) - set(signature)
        if missing:
            raise ActiveRiskDecisionReportError(
                "candidate exposure signature is incomplete: "
                + ", ".join(sorted(missing))
            )
        self.policy_count += 1
        for key in EXPOSURE_SIGNATURE_FIELDS[:-1]:
            self.fields[key].append(
                _required_float(signature[key], label=f"exposure signature {key}")
            )
        self.outcome_fields_used_values.add(bool(signature["outcome_fields_used"]))
        if signature["outcome_fields_used"] is not False:
            raise ActiveRiskDecisionReportError(
                "candidate exposure signature used outcome fields"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": "NORMAL_CANONICAL_90_DAY_ACCOUNT_EXPOSURE_SIGNATURE",
            "role": "DUTY_AND_EXPOSURE_MATCH_EVIDENCE_NOT_RISK_UTILISATION",
            "policy_count": self.policy_count,
            "outcome_fields_used_values": sorted(self.outcome_fields_used_values),
            "field_distributions": {
                key: _distribution(values) for key, values in sorted(self.fields.items())
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


def _validate_xfa_path_accounting(
    path: Mapping[str, Any],
    *,
    label: str,
    combine_end_day: int,
    xfa_start_day: int | None,
    rule_snapshot: Mapping[str, Any],
) -> None:
    def nonnegative_int(value: Any, *, field_name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ActiveRiskDecisionReportError(
                f"{label} {field_name} is not a non-negative integer"
            )
        return int(value)

    ledger = path.get("daily_ledger")
    if not isinstance(ledger, list):
        raise ActiveRiskDecisionReportError(f"{label} daily ledger is absent")
    requested_days = nonnegative_int(
        path.get("requested_horizon_days"), field_name="requested_horizon_days"
    )
    observed_days = nonnegative_int(
        path.get("observed_days"), field_name="observed_days"
    )
    traded_days = nonnegative_int(path.get("traded_days"), field_name="traded_days")
    if requested_days <= 0 or not 0 <= observed_days <= requested_days:
        raise ActiveRiskDecisionReportError(
            f"{label} requested/observed horizon semantics drift"
        )
    if observed_days != len(ledger):
        raise ActiveRiskDecisionReportError(f"{label} observed-day cardinality drift")
    if traded_days > observed_days:
        raise ActiveRiskDecisionReportError(f"{label} traded-day cardinality drift")
    terminal = str(path.get("terminal") or "")
    if terminal not in {
        "SURVIVED_HORIZON",
        "DATA_CENSORED",
        "MLL_BREACHED",
        "HARD_RULE_FAILURE",
        "INACTIVITY_RISK",
    }:
        raise ActiveRiskDecisionReportError(f"{label} XFA terminal drift")
    if terminal == "SURVIVED_HORIZON" and observed_days != requested_days:
        raise ActiveRiskDecisionReportError(
            f"{label} survived-horizon cardinality drift"
        )
    if terminal == "DATA_CENSORED" and observed_days >= requested_days:
        raise ActiveRiskDecisionReportError(
            f"{label} data-censor horizon semantics drift"
        )
    if xfa_start_day is not None and xfa_start_day <= combine_end_day:
        raise ActiveRiskDecisionReportError(
            f"{label} XFA start is not strictly after Combine end"
        )
    session_days = [
        nonnegative_int(row.get("session_day"), field_name="ledger session_day")
        for row in ledger
    ]
    if any(right <= left for left, right in zip(session_days, session_days[1:])):
        raise ActiveRiskDecisionReportError(
            f"{label} daily ledger is not strictly chronological"
        )
    accepted_from_ledger = sum(
        nonnegative_int(row.get("accepted_events", 0), field_name="ledger accepted_events")
        for row in ledger
    )
    skipped_from_ledger = sum(
        nonnegative_int(row.get("skipped_events", 0), field_name="ledger skipped_events")
        for row in ledger
    )
    accepted_events = nonnegative_int(
        path.get("accepted_event_count"), field_name="accepted_event_count"
    )
    skipped_events = nonnegative_int(
        path.get("skipped_event_count"), field_name="skipped_event_count"
    )
    event_count = nonnegative_int(path.get("event_count"), field_name="event_count")
    unclassified_events = event_count - accepted_events - skipped_events
    terminal_reason = str(path.get("terminal_reason") or "")
    expected_unclassified_events = (
        1
        if terminal == "HARD_RULE_FAILURE"
        and terminal_reason
        in {
            "session_close_or_trading_hours_violation",
            "source_contract_limit_violation",
        }
        else 0
    )
    if (
        accepted_events != accepted_from_ledger
        or skipped_events != skipped_from_ledger
        or unclassified_events != expected_unclassified_events
    ):
        raise ActiveRiskDecisionReportError(f"{label} event ledger accounting drift")
    skipped_reasons = path.get("skipped_reasons")
    if not isinstance(skipped_reasons, Mapping) or sum(
        nonnegative_int(value, field_name="skipped-reason count")
        for value in skipped_reasons.values()
    ) != skipped_events:
        raise ActiveRiskDecisionReportError(f"{label} skipped-reason accounting drift")
    payout_indices = [
        index for index, row in enumerate(ledger) if bool(row.get("payout_requested"))
    ]
    cycles = nonnegative_int(path.get("payout_cycles"), field_name="payout_cycles")
    if cycles != len(payout_indices):
        raise ActiveRiskDecisionReportError(f"{label} payout-cycle ledger drift")
    eligible = bool(path.get("payout_eligible"))
    if eligible != (cycles > 0):
        raise ActiveRiskDecisionReportError(f"{label} payout eligibility drift")
    first_payout_day = path.get("first_payout_day")
    expected_first_day = payout_indices[0] + 1 if payout_indices else None
    if first_payout_day is None or expected_first_day is None:
        if first_payout_day is not None or expected_first_day is not None:
            raise ActiveRiskDecisionReportError(f"{label} first-payout day drift")
    elif int(first_payout_day) != expected_first_day:
        raise ActiveRiskDecisionReportError(f"{label} first-payout day drift")
    gross = sum(_float(row.get("gross_payout")) for row in ledger)
    trader = sum(_float(row.get("trader_net_payout")) for row in ledger)
    _assert_close(gross, path.get("gross_payout"), label=f"{label} gross payout")
    _assert_close(
        trader,
        path.get("trader_net_payout"),
        label=f"{label} trader net payout",
    )
    expected_post_days = (
        len(ledger) - (payout_indices[-1] + 1) if payout_indices else 0
    )
    post_payout_observed_days = nonnegative_int(
        path.get("post_payout_observed_days"),
        field_name="post_payout_observed_days",
    )
    if post_payout_observed_days != expected_post_days:
        raise ActiveRiskDecisionReportError(
            f"{label} post-payout observed-day drift"
        )
    expected_post_censored = cycles > 0 and terminal == "DATA_CENSORED"
    if bool(path.get("post_payout_censored")) != expected_post_censored:
        raise ActiveRiskDecisionReportError(f"{label} post-payout censor drift")
    expected_post_survived = bool(
        cycles > 0
        and expected_post_days > 0
        and terminal == "SURVIVED_HORIZON"
    )
    if bool(path.get("post_payout_survived")) != expected_post_survived:
        raise ActiveRiskDecisionReportError(f"{label} post-payout survival drift")
    if observed_days == 0:
        zero_observation_identity = (
            terminal == "DATA_CENSORED"
            and terminal_reason
            == "no_post_combine_session_available_for_xfa_replay"
            and xfa_start_day is None
            and path.get("start_day") is None
            and path.get("end_day") is None
            and traded_days == 0
            and event_count == 0
            and accepted_events == 0
            and skipped_events == 0
            and cycles == 0
            and not eligible
            and path.get("first_payout_day") is None
            and post_payout_observed_days == 0
            and not bool(path.get("post_payout_censored"))
            and not bool(path.get("post_payout_survived"))
            and math.isclose(_float(path.get("gross_payout")), 0.0, abs_tol=1e-12)
            and math.isclose(
                _float(path.get("trader_net_payout")), 0.0, abs_tol=1e-12
            )
            and math.isclose(_float(path.get("total_cost")), 0.0, abs_tol=1e-12)
            and not skipped_reasons
            and not (path.get("component_contribution") or {})
            and math.isclose(
                _required_float(
                    path.get("ending_balance"), label=f"{label} ending balance"
                ),
                _required_float(
                    rule_snapshot.get("xfa_starting_balance"),
                    label=f"{label} rule starting balance",
                ),
                abs_tol=1e-8,
            )
            and math.isclose(
                _required_float(
                    path.get("ending_mll_floor"), label=f"{label} ending MLL floor"
                ),
                _required_float(
                    rule_snapshot.get("xfa_starting_floor"),
                    label=f"{label} rule starting MLL floor",
                ),
                abs_tol=1e-8,
            )
            and math.isclose(
                _required_float(
                    path.get("minimum_mll_buffer"),
                    label=f"{label} minimum MLL buffer",
                ),
                _required_float(
                    rule_snapshot.get("xfa_starting_balance"),
                    label=f"{label} rule starting balance",
                )
                - _required_float(
                    rule_snapshot.get("xfa_starting_floor"),
                    label=f"{label} rule starting MLL floor",
                ),
                abs_tol=1e-8,
            )
            and nonnegative_int(
                path.get("qualifying_winning_days"),
                field_name="qualifying_winning_days",
            )
            == 0
            and math.isclose(
                _required_float(
                    path.get("maximum_consistency_ratio"),
                    label=f"{label} maximum consistency ratio",
                ),
                0.0,
                abs_tol=1e-12,
            )
            and math.isclose(
                _required_float(
                    path.get("maximum_mini_equivalent"),
                    label=f"{label} maximum mini equivalent",
                ),
                0.0,
                abs_tol=1e-12,
            )
            and path.get("calendar_inactivity_auditable") is False
            and path.get("payout_request_policy")
            == "EARLIEST_ELIGIBLE_END_OF_DAY"
            and path.get("payout_path_selected_from_outcomes") is False
        )
        if not zero_observation_identity:
            raise ActiveRiskDecisionReportError(
                f"{label} zero-observation censor identity drift"
            )
    else:
        if xfa_start_day is None:
            raise ActiveRiskDecisionReportError(f"{label} observed XFA start is absent")
        if path.get("start_day") is None or path.get("end_day") is None:
            raise ActiveRiskDecisionReportError(f"{label} observed chronology is absent")
        if int(path["start_day"]) != xfa_start_day:
            raise ActiveRiskDecisionReportError(f"{label} XFA path start-day drift")
        if int(ledger[0]["session_day"]) != int(path["start_day"]) or int(
            ledger[-1]["session_day"]
        ) != int(path["end_day"]):
            raise ActiveRiskDecisionReportError(f"{label} ledger chronology drift")


@dataclass
class LifecyclePathAccumulator:
    combine_attempts: int = 0
    combine_censored_attempts: int = 0
    xfa_paths_started: int = 0
    observed_paths: int = 0
    zero_observation_paths: int = 0
    xfa_censored_paths: int = 0
    evaluable_xfa_paths: int = 0
    first_payout_evaluable_xfa_paths: int = 0
    first_payouts: int = 0
    evaluable_first_payouts: int = 0
    payout_cycles: int = 0
    evaluable_payout_cycles: int = 0
    first_payout_evaluable_observed_cycles: int = 0
    trader_net_payout: float = 0.0
    evaluable_trader_net_payout: float = 0.0
    first_payout_evaluable_observed_payout: float = 0.0
    post_payout_survived: int = 0
    post_payout_censored: int = 0
    evaluable_post_payout_paths: int = 0
    evaluable_post_payout_survived: int = 0
    first_payout_days: list[float] = field(default_factory=list)
    evaluable_first_payout_days: list[float] = field(default_factory=list)
    minimum_mll_buffers: list[float] = field(default_factory=list)
    evaluable_minimum_mll_buffers: list[float] = field(default_factory=list)
    missing_minimum_mll_buffers: int = 0

    def add_combine_episode(self, raw: Mapping[str, Any]) -> None:
        terminal = str(raw.get("terminal_classification") or raw.get("terminal") or "")
        censored = terminal in {
            "DATA_CENSORED",
            "OPERATIONAL_HORIZON_NOT_REACHED",
        } or bool(raw.get("censored"))
        self.combine_attempts += 1
        self.combine_censored_attempts += int(censored)

    def add_path(self, path: Mapping[str, Any]) -> None:
        self.xfa_paths_started += 1
        observed = int(path.get("observed_days", 0)) > 0
        censored = str(path.get("terminal") or "") == "DATA_CENSORED"
        evaluable = observed and not censored
        self.observed_paths += int(observed)
        self.zero_observation_paths += int(not observed)
        self.xfa_censored_paths += int(censored)
        self.evaluable_xfa_paths += int(evaluable)
        eligible = bool(path.get("payout_eligible"))
        first_payout_evaluable = evaluable or eligible
        self.first_payout_evaluable_xfa_paths += int(first_payout_evaluable)
        self.first_payouts += int(eligible)
        self.evaluable_first_payouts += int(eligible and first_payout_evaluable)
        cycles = int(path.get("payout_cycles", 0))
        payout = _required_float(
            path.get("trader_net_payout"), label="XFA trader net payout"
        )
        self.payout_cycles += cycles
        self.evaluable_payout_cycles += cycles if evaluable else 0
        self.first_payout_evaluable_observed_cycles += (
            cycles if first_payout_evaluable else 0
        )
        self.trader_net_payout += payout
        self.evaluable_trader_net_payout += payout if evaluable else 0.0
        self.first_payout_evaluable_observed_payout += (
            payout if first_payout_evaluable else 0.0
        )
        self.post_payout_survived += int(bool(path.get("post_payout_survived")))
        post_censored = bool(path.get("post_payout_censored"))
        self.post_payout_censored += int(post_censored)
        post_evaluable = eligible and not post_censored
        self.evaluable_post_payout_paths += int(post_evaluable)
        self.evaluable_post_payout_survived += int(
            post_evaluable and bool(path.get("post_payout_survived"))
        )
        if path.get("minimum_mll_buffer") is None:
            self.missing_minimum_mll_buffers += 1
        else:
            buffer = _required_float(
                path["minimum_mll_buffer"], label="XFA minimum MLL buffer"
            )
            self.minimum_mll_buffers.append(buffer)
            if evaluable:
                self.evaluable_minimum_mll_buffers.append(buffer)
        if eligible and path.get("first_payout_day") is not None:
            # The lifecycle simulator persists this as a one-based elapsed
            # XFA-day count, not as an epoch/session day.
            elapsed = _required_float(
                path["first_payout_day"], label="XFA elapsed day to first payout"
            )
            self.first_payout_days.append(elapsed)
            if first_payout_evaluable:
                self.evaluable_first_payout_days.append(elapsed)

    def to_dict(self) -> dict[str, Any]:
        combine_evaluable = self.combine_attempts - self.combine_censored_attempts
        unevaluable_xfa = self.xfa_paths_started - self.evaluable_xfa_paths
        complete_lifecycle_evaluable_attempts = max(
            combine_evaluable - unevaluable_xfa, 0
        )
        first_payout_evaluable_attempts = max(
            combine_evaluable
            - (self.xfa_paths_started - self.first_payout_evaluable_xfa_paths),
            0,
        )
        lower_bound = {
            "combine_pass_probability": (
                self.xfa_paths_started / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
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
            "expected_payout_cycles_per_combine_attempt": (
                self.payout_cycles / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
            "expected_trader_payout_per_combine_attempt": (
                self.trader_net_payout / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
            "post_payout_survival_probability_conditional_on_first_payout": (
                self.post_payout_survived / self.first_payouts
                if self.first_payouts
                else 0.0
            ),
            "denominators": {
                "combine_attempts": self.combine_attempts,
                "xfa_paths_started": self.xfa_paths_started,
                "first_payout_paths": self.first_payouts,
            },
        }
        evaluable_only = {
            "combine_pass_probability": (
                self.xfa_paths_started / combine_evaluable
                if combine_evaluable
                else None
            ),
            "first_payout_probability_conditional_on_combine_pass": (
                self.evaluable_first_payouts
                / self.first_payout_evaluable_xfa_paths
                if self.first_payout_evaluable_xfa_paths
                else None
            ),
            "first_payout_probability_per_evaluable_lifecycle_attempt": (
                self.evaluable_first_payouts / first_payout_evaluable_attempts
                if first_payout_evaluable_attempts
                else None
            ),
            "expected_payout_cycles_per_evaluable_lifecycle_attempt": (
                self.evaluable_payout_cycles / complete_lifecycle_evaluable_attempts
                if complete_lifecycle_evaluable_attempts
                else None
            ),
            "expected_trader_payout_per_evaluable_lifecycle_attempt": (
                self.evaluable_trader_net_payout
                / complete_lifecycle_evaluable_attempts
                if complete_lifecycle_evaluable_attempts
                else None
            ),
            "observed_payout_cycles_lower_bound_per_first_payout_evaluable_attempt": (
                self.first_payout_evaluable_observed_cycles
                / first_payout_evaluable_attempts
                if first_payout_evaluable_attempts
                else None
            ),
            "observed_trader_payout_lower_bound_per_first_payout_evaluable_attempt": (
                self.first_payout_evaluable_observed_payout
                / first_payout_evaluable_attempts
                if first_payout_evaluable_attempts
                else None
            ),
            "post_payout_survival_probability_conditional_on_evaluable_first_payout": (
                self.evaluable_post_payout_survived
                / self.evaluable_post_payout_paths
                if self.evaluable_post_payout_paths
                else None
            ),
            "denominators": {
                "combine_non_censored_attempts": combine_evaluable,
                "first_payout_attempts_excluding_unresolved_censored_or_zero_observation_xfa": (
                    first_payout_evaluable_attempts
                ),
                "complete_lifecycle_attempts_excluding_censored_or_zero_observation_xfa": (
                    complete_lifecycle_evaluable_attempts
                ),
                "xfa_paths_excluding_censored_or_zero_observation": (
                    self.evaluable_xfa_paths
                ),
                "first_payout_evaluable_xfa_paths_including_observed_success_before_later_censoring": (
                    self.first_payout_evaluable_xfa_paths
                ),
                "first_payout_paths_with_evaluable_post_payout_survival": (
                    self.evaluable_post_payout_paths
                ),
            },
        }
        return {
            "combine_attempts": self.combine_attempts,
            "combine_censored_attempts": self.combine_censored_attempts,
            "combine_non_censored_attempts": combine_evaluable,
            "xfa_paths_started": self.xfa_paths_started,
            "observed_xfa_paths": self.observed_paths,
            "zero_observation_xfa_paths": self.zero_observation_paths,
            "censored_xfa_paths": self.xfa_censored_paths,
            "evaluable_xfa_paths": self.evaluable_xfa_paths,
            "first_payout_evaluable_xfa_paths": (
                self.first_payout_evaluable_xfa_paths
            ),
            "first_payouts": self.first_payouts,
            "evaluable_first_payouts": self.evaluable_first_payouts,
            "payout_cycles": self.payout_cycles,
            "trader_net_payout": self.trader_net_payout,
            "post_payout_survival_count": self.post_payout_survived,
            "post_payout_censored_count": self.post_payout_censored,
            "unconditional_lower_bound": lower_bound,
            "evaluable_only": evaluable_only,
            "days_to_first_payout": {
                "all_observed_first_payouts": _distribution(self.first_payout_days),
                "evaluable_only": _distribution(self.evaluable_first_payout_days),
            },
            "minimum_mll_buffer": {
                "all_nonmissing_paths": _distribution(self.minimum_mll_buffers),
                "evaluable_only": _distribution(self.evaluable_minimum_mll_buffers),
                "missing_count": self.missing_minimum_mll_buffers,
            },
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
        "duration_trading_days_median",
        "active_trading_days_median",
        "calendar_days_median",
        "monthly_subscription_duration_proxy_median",
        "censoring_rate",
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
    output.update(_combine_terminal_metrics(summary, label="episode summary"))
    return output


def _metric_view(row: Mapping[str, Any]) -> dict[str, Any]:
    horizons = row.get("horizons") or {}
    output = {
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
    if isinstance(row.get("exposure_signature"), Mapping):
        output["exposure_signature"] = dict(row["exposure_signature"])
    if isinstance(row.get("exposure_matching"), Mapping):
        output["exposure_matching"] = dict(row["exposure_matching"])
    return output


def _delta(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _float(left.get(key)) - _float(right.get(key))
        for key in (
            "pass_count",
            "pass_rate",
            "pass_rate_raw_lower_bound",
            "pass_rate_evaluable",
            "target_progress_p25",
            "target_progress_median",
            "net_median",
            "net_total",
            "mll_breach_rate",
            "mll_breach_rate_raw_lower_bound",
            "mll_breach_rate_evaluable",
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
    if payload.get("random_priority_outcomes_used_for_matching") is not False:
        raise ActiveRiskDecisionReportError(
            "random-priority control matching used economic outcomes"
        )
    if payload.get("random_priority_exposure_matched") is not True or not math.isclose(
        _required_float(
            payload.get("random_priority_exposure_match_rate"),
            label="global random-priority exposure match rate",
        ),
        1.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ActiveRiskDecisionReportError(
            "random-priority controls are not fully exposure matched"
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
        if not value.get("decision_hash"):
            raise ActiveRiskDecisionReportError(
                f"halving file lacks required decision_hash: {path}"
            )
        _verify_embedded_hash(value, "decision_hash", path.name)
        stage = str(value.get("stage") or "")
        selected = [str(item) for item in value.get("selected_policy_ids") or ()]
        if not stage:
            raise ActiveRiskDecisionReportError(
                f"halving file lacks stage identity: {path}"
            )
        if not all(selected) or len(selected) != len(set(selected)):
            raise ActiveRiskDecisionReportError(
                f"halving file has duplicate or empty selected policy ids: {path}"
            )
        input_count = int(value.get("input_count", -1))
        eligible_count = int(value.get("eligible_count", -1))
        output_limit = int(value.get("output_limit", -1))
        output_count = int(value.get("output_count", -1))
        if (
            min(input_count, eligible_count, output_limit, output_count) < 0
            or eligible_count > input_count
            or output_count > eligible_count
            or output_count > output_limit
            or output_count != len(selected)
        ):
            raise ActiveRiskDecisionReportError(
                f"halving file count consistency drift: {path}"
            )
        if value.get("development_only") is not True:
            raise ActiveRiskDecisionReportError(
                f"halving file is not development-only: {path}"
            )
        decisions[path.stem] = {
            "stage": stage,
            "input_count": input_count,
            "eligible_count": eligible_count,
            "output_limit": output_limit,
            "output_count": output_count,
            "selected_policy_ids": selected,
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
        routing = _canonical_daily_routing(raw)
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
            "name": (
                "LEXICOGRAPHIC_COMPLETE_LINK_CANONICAL_ACCOUNT_AND_ROUTING_"
                "DECISIONS_VECTOR_V1"
            ),
            "canonical_horizon": CANONICAL_HORIZON,
            "scope": "CANONICAL_ACCOUNT_OUTCOMES_DAILY_PATHS_AND_ROUTING_DECISIONS",
            "source_signal_or_trade_ledgers_used": False,
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
            "interpretation": "REPORT_ONLY_POSTHOC_NOT_SELECTION_OR_PROMOTION_EVIDENCE",
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
    if match.get("economic_outcomes_used_for_selection") is not False:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random control used economic outcomes"
        )
    if match.get("matched") is not True:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random control is not exposure matched"
        )
    if str(match.get("matched_policy_id") or "") != policy_id:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control candidate identity drift"
        )
    random_id = str(random_control.get("policy_id") or "")
    if str(match.get("control_id") or "") != random_id:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control identity drift"
        )
    if list(match.get("selection_key_fields") or ()) != list(EXPOSURE_MATCH_FIELDS):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control selection-key drift"
        )
    fixed_seeds = {int(value) for value in controls.get("random_priority_fixed_seeds") or ()}
    try:
        selected_seed = int(match["selected_seed"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control seed is absent"
        ) from exc
    if selected_seed not in fixed_seeds:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control seed is not preregistered"
        )
    candidate_signature = match.get("candidate_signature")
    control_signature = match.get("control_signature")
    if not isinstance(candidate_signature, Mapping) or not isinstance(
        control_signature, Mapping
    ):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control exposure signature is absent"
        )
    if dict(candidate_signature) != dict(candidate.get("exposure_signature") or {}):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} exposure-signature linkage drift"
        )
    if dict(control_signature) != dict(random_control.get("exposure_signature") or {}):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control signature linkage drift"
        )
    if candidate_signature.get("outcome_fields_used") is not False or (
        control_signature.get("outcome_fields_used") is not False
    ):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} exposure matching used outcome fields"
        )
    tolerance = _required_float(
        match.get("relative_tolerance"),
        label=f"candidate {policy_id} random-control tolerance",
    )
    if tolerance < 0.0:
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control tolerance is negative"
        )
    deltas = match.get("deltas")
    if not isinstance(deltas, Mapping) or set(deltas) != set(EXPOSURE_MATCH_FIELDS):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} random-control delta coverage drift"
        )
    for field_name in EXPOSURE_MATCH_FIELDS:
        detail = deltas[field_name]
        if not isinstance(detail, Mapping):
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} random-control {field_name} delta is malformed"
            )
        expected = _required_float(
            candidate_signature.get(field_name),
            label=f"candidate {policy_id} exposure {field_name}",
        )
        observed = _required_float(
            control_signature.get(field_name),
            label=f"candidate {policy_id} random exposure {field_name}",
        )
        absolute = abs(observed - expected)
        _assert_close(
            detail.get("candidate"), expected, label=f"{policy_id} {field_name} candidate"
        )
        _assert_close(
            detail.get("control"), observed, label=f"{policy_id} {field_name} control"
        )
        _assert_close(
            detail.get("absolute_delta"),
            absolute,
            label=f"{policy_id} {field_name} absolute delta",
        )
        expected_relative = 0.0 if expected == 0.0 and observed == 0.0 else (
            None if expected == 0.0 else absolute / abs(expected)
        )
        if expected_relative is None or detail.get("relative_delta") is None:
            if expected_relative is not None or detail.get("relative_delta") is not None:
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} random-control {field_name} relative delta drift"
                )
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} random-control {field_name} cannot be matched"
            )
        _assert_close(
            detail.get("relative_delta"),
            expected_relative,
            label=f"{policy_id} {field_name} relative delta",
        )
        if expected_relative > tolerance + 1e-12:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} random-control {field_name} exceeds tolerance"
            )
    embedded_match = random_control.get("exposure_matching")
    if isinstance(embedded_match, Mapping) and dict(embedded_match) != dict(match):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} embedded random-control match drift"
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


def _terminal_flags(raw: Mapping[str, Any], *, label: str) -> tuple[bool, bool, bool]:
    terminal = str(raw.get("terminal_classification") or raw.get("terminal") or "")
    passed = terminal == "TARGET_REACHED"
    breached = terminal == "MLL_BREACHED"
    censored = terminal in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"}
    if bool(raw.get("passed")) != passed:
        raise ActiveRiskDecisionReportError(f"{label} pass/terminal drift")
    if bool(raw.get("mll_breached")) != breached:
        raise ActiveRiskDecisionReportError(f"{label} MLL/terminal drift")
    if bool(raw.get("censored")) != (terminal == "DATA_CENSORED"):
        raise ActiveRiskDecisionReportError(f"{label} data-censor/terminal drift")
    return passed, breached, censored


def _unique_horizon_rows(
    raw_rows: Sequence[Mapping[str, Any]], *, horizon: str, policy_id: str
) -> list[Mapping[str, Any]]:
    selected = [row for row in raw_rows if str(row.get("horizon_label")) == horizon]
    keys: set[tuple[str, int]] = set()
    for raw in selected:
        key = (_scenario_key(raw.get("scenario")), int(raw["start_day"]))
        if key in keys:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} duplicate {horizon} episode {key}"
            )
        keys.add(key)
    return selected


def _validate_raw_daily_derivations(
    raw: Mapping[str, Any], *, label: str
) -> None:
    """Bind replay-only episode fields to the sealed daily-path projection."""

    daily = list(raw.get("daily_path") or ())
    if not daily or not all(isinstance(day, Mapping) for day in daily):
        raise ActiveRiskDecisionReportError(f"{label} daily path is absent or malformed")
    try:
        session_days = [int(day["session_day"]) for day in daily]
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskDecisionReportError(f"{label} daily path is unkeyed") from exc
    if any(right <= left for left, right in zip(session_days, session_days[1:])):
        raise ActiveRiskDecisionReportError(
            f"{label} daily session chronology is not strictly increasing"
        )
    if int(raw.get("eligible_days", -1)) != len(daily):
        raise ActiveRiskDecisionReportError(f"{label} eligible-days/daily-path drift")
    if int(raw.get("end_day", -1)) != session_days[-1]:
        raise ActiveRiskDecisionReportError(f"{label} end-day/daily-path drift")

    raw_routing = list(raw.get("risk_allocation_path") or ())
    daily_routing = [
        decision
        for day in daily
        for decision in list(day.get("routing_decisions") or ())
    ]
    if not all(isinstance(value, Mapping) for value in raw_routing + daily_routing):
        raise ActiveRiskDecisionReportError(f"{label} routing path is malformed")

    def routing_multiset(values: Sequence[Mapping[str, Any]]) -> Counter[str]:
        return Counter(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            for value in values
        )

    if routing_multiset(raw_routing) != routing_multiset(daily_routing):
        raise ActiveRiskDecisionReportError(f"{label} raw/daily routing drift")
    if any("allow" not in decision for decision in daily_routing):
        raise ActiveRiskDecisionReportError(f"{label} routing lacks allow decisions")
    accepted = sum(bool(decision["allow"]) for decision in daily_routing)
    skipped = len(daily_routing) - accepted
    if int(raw.get("accepted_events", -1)) != accepted:
        raise ActiveRiskDecisionReportError(f"{label} accepted-event drift")
    if int(raw.get("skipped_events", -1)) != skipped:
        raise ActiveRiskDecisionReportError(f"{label} skipped-event drift")
    traded_days = sum(
        any(bool(decision["allow"]) for decision in day.get("routing_decisions") or ())
        for day in daily
    )
    if int(raw.get("traded_days", -1)) != traded_days:
        raise ActiveRiskDecisionReportError(f"{label} traded-days/daily-routing drift")

    terminal = str(raw.get("terminal_classification") or raw.get("terminal") or "")
    maximum_progress_days = (
        daily[:-1]
        if terminal in {"MLL_BREACHED", "HARD_RULE_FAILURE"}
        else daily
    )
    maximum_progress = max(
        [0.0]
        + [float(day["target_progress"]) for day in maximum_progress_days]
    )
    _assert_close(
        raw.get("maximum_target_progress"),
        maximum_progress,
        label=f"{label} maximum target progress/daily path",
    )
    _assert_close(
        raw.get("net_pnl"),
        daily[-1].get("realized_pnl"),
        label=f"{label} net PnL/daily path",
    )
    _assert_close(
        raw.get("target_progress"),
        daily[-1].get("target_progress"),
        label=f"{label} target progress/daily path",
    )
    if bool(raw.get("consistency_ok")) != bool(daily[-1].get("consistency_ok")):
        raise ActiveRiskDecisionReportError(f"{label} consistency/daily-path drift")
    _assert_close(
        raw.get("minimum_mll_buffer"),
        min(float(day["minimum_mll_buffer"]) for day in daily),
        label=f"{label} minimum MLL buffer/daily path",
    )
    maximum_mini = max(
        float((day.get("exposure") or {}).get("maximum_mini_equivalent", 0.0))
        for day in daily
    )
    maximum_directional = max(
        float((day.get("exposure") or {}).get("maximum_net_directional", 0.0))
        for day in daily
    )
    _assert_close(
        raw.get("maximum_mini_equivalent"),
        maximum_mini,
        label=f"{label} maximum mini-equivalent/daily path",
    )
    _assert_close(
        raw.get("maximum_net_directional_exposure"),
        maximum_directional,
        label=f"{label} maximum directional exposure/daily path",
    )
    contribution: dict[str, float] = defaultdict(float)
    for day in daily:
        attribution = day.get("component_attribution")
        if not isinstance(attribution, Mapping):
            raise ActiveRiskDecisionReportError(
                f"{label} daily component attribution is malformed"
            )
        for component_id, value in attribution.items():
            contribution[str(component_id)] += float(value)
    raw_contribution = raw.get("component_contribution")
    if not isinstance(raw_contribution, Mapping) or set(raw_contribution) != set(
        contribution
    ):
        raise ActiveRiskDecisionReportError(
            f"{label} component-attribution key drift"
        )
    for component_id, value in contribution.items():
        _assert_close(
            raw_contribution[component_id],
            value,
            label=f"{label} component attribution {component_id}/daily path",
        )


def _canonical_daily_routing(raw: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        decision
        for day in raw.get("daily_path") or ()
        for decision in day.get("routing_decisions") or ()
    ]


def _derive_canonical_account_diagnostics(
    canonical_raw: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Recompute every published account diagnostic from sealed NORMAL paths."""

    normal = [
        raw
        for raw in canonical_raw
        if _scenario_key(raw.get("scenario")) == "normal"
    ]
    proxies = [
        SimpleNamespace(
            eligible_days=int(raw["eligible_days"]),
            risk_allocation_path=tuple(_canonical_daily_routing(raw)),
        )
        for raw in normal
    ]
    return {
        "risk_utilisation": _utilisation_summary(proxies),
        "exposure_signature": _exposure_signature(proxies),
        "suppression": _suppression_summary(proxies),
    }


def _normalized_behavior_fingerprint(
    canonical_raw: Sequence[Mapping[str, Any]],
) -> str:
    """Hash only behavior fields recoverable from sealed daily projections."""

    rows: list[dict[str, Any]] = []
    for raw in sorted(
        canonical_raw,
        key=lambda value: (
            _scenario_key(value.get("scenario")),
            int(value["start_day"]),
        ),
    ):
        routing = _canonical_daily_routing(raw)
        rows.append(
            {
                "scenario": _scenario_key(raw.get("scenario")),
                "start_day": int(raw["start_day"]),
                "terminal": str(raw["terminal_classification"]),
                "accepted_events": int(raw["accepted_events"]),
                "skipped_events": int(raw["skipped_events"]),
                "quantity_path": [
                    [
                        str(decision.get("event_id") or ""),
                        int(decision.get("quantity", 0)),
                        str(decision.get("decision_status") or "UNKNOWN"),
                    ]
                    for decision in routing
                ],
            }
        )
    return canonical_hash(
        {
            "schema": "hydra_sealed_normalized_account_behavior_v1",
            "rows": rows,
        }
    )


def _reconcile_episode_summary(
    *,
    policy_id: str,
    scenario: str,
    horizon: str,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    blocks: Sequence[BlockSpec],
) -> None:
    label = f"candidate {policy_id} {scenario} {horizon}"
    terminals: Counter[str] = Counter()
    nets: list[float] = []
    progress: list[float] = []
    maximum_progress: list[float] = []
    buffers: list[float] = []
    durations: list[float] = []
    active_durations: list[float] = []
    calendar_durations: list[float] = []
    days_to_target: list[float] = []
    projected_active_days: list[float] = []
    projected_calendar_days: list[float] = []
    by_block_net: dict[str, float] = defaultdict(float)
    by_block_progress: dict[str, list[float]] = defaultdict(list)
    component_contribution: dict[str, float] = defaultdict(float)
    pass_blocks: set[str] = set()
    passed = breached = censored = consistent = 0
    for raw in rows:
        row_label = f"{label} start {raw.get('start_day')}"
        _validate_raw_daily_derivations(raw, label=row_label)
        terminal = str(raw.get("terminal_classification") or raw.get("terminal") or "")
        if not terminal:
            raise ActiveRiskDecisionReportError(f"{row_label} terminal is absent")
        terminals[terminal] += 1
        is_pass, is_breach, is_censored = _terminal_flags(raw, label=row_label)
        passed += int(is_pass)
        breached += int(is_breach)
        censored += int(is_censored)
        consistent += int(bool(raw.get("consistency_ok")))
        nets.append(_required_float(raw.get("net_pnl"), label=f"{row_label} net PnL"))
        progress_value = _required_float(
            raw.get("target_progress"), label=f"{row_label} target progress"
        )
        progress.append(progress_value)
        maximum_progress.append(
            _required_float(
                raw.get("maximum_target_progress"),
                label=f"{row_label} maximum target progress",
            )
        )
        buffers.append(
            _required_float(
                raw.get("minimum_mll_buffer"), label=f"{row_label} minimum MLL buffer"
            )
        )
        duration = _required_float(
            raw.get("eligible_days"), label=f"{row_label} eligible days"
        )
        active_duration = _required_float(
            raw.get("traded_days"), label=f"{row_label} traded days"
        )
        calendar_duration = float(int(raw["end_day"]) - int(raw["start_day"]) + 1)
        durations.append(duration)
        active_durations.append(active_duration)
        calendar_durations.append(calendar_duration)
        if progress_value > 0.0:
            projected_active_days.append(active_duration / progress_value)
            projected_calendar_days.append(calendar_duration / progress_value)
        block_id = _block_for_day(int(raw["start_day"]), blocks)
        by_block_net[block_id] += nets[-1]
        by_block_progress[block_id].append(progress_value)
        if is_pass:
            pass_blocks.add(block_id)
        raw_contribution = raw.get("component_contribution")
        if not isinstance(raw_contribution, Mapping):
            raise ActiveRiskDecisionReportError(
                f"{row_label} component contribution is absent"
            )
        for component_id, value in raw_contribution.items():
            component_contribution[str(component_id)] += _required_float(
                value, label=f"{row_label} component contribution {component_id}"
            )
        if raw.get("days_to_target") is not None:
            days_to_target.append(
                _required_float(
                    raw["days_to_target"], label=f"{row_label} days to target"
                )
            )
    count = len(rows)
    expected_count = int(summary.get("episode_count", -1))
    if count != expected_count:
        raise ActiveRiskDecisionReportError(f"{label} episode-count drift")
    count_fields = {
        "pass_count": passed,
        "mll_breach_count": breached,
        "censored_episode_count": censored,
        "consistency_ok_count": consistent,
    }
    for field_name, actual in count_fields.items():
        if int(summary.get(field_name, -1)) != actual:
            raise ActiveRiskDecisionReportError(f"{label} {field_name} drift")
    if dict(summary.get("terminal_distribution") or {}) != dict(sorted(terminals.items())):
        raise ActiveRiskDecisionReportError(f"{label} terminal-distribution drift")
    denominator = count if count else 1
    float_fields = {
        "pass_rate": passed / denominator,
        "mll_breach_rate": breached / denominator,
        "censoring_rate": censored / denominator,
        "consistency_rate": consistent / denominator,
        "net_total": sum(nets),
        "net_median": statistics.median(nets) if nets else 0.0,
        "target_progress_median": statistics.median(progress) if progress else 0.0,
        "target_progress_p25": _quantile(progress, 0.25) if progress else 0.0,
        "maximum_target_progress": max(maximum_progress, default=0.0),
        "minimum_mll_buffer": min(buffers, default=4500.0),
        "duration_trading_days_median": (
            statistics.median(durations) if durations else 0.0
        ),
        "active_trading_days_median": (
            statistics.median(active_durations) if active_durations else 0.0
        ),
        "calendar_days_median": (
            statistics.median(calendar_durations) if calendar_durations else 0.0
        ),
        "projected_active_days_to_target_median": (
            statistics.median(projected_active_days)
            if projected_active_days
            else None
        ),
        "projected_calendar_days_to_target_median": (
            statistics.median(projected_calendar_days)
            if projected_calendar_days
            else None
        ),
        "monthly_subscription_duration_proxy_median": (
            statistics.median(projected_calendar_days) / 30.0
            if projected_calendar_days
            else None
        ),
    }
    for field_name, actual in float_fields.items():
        expected = summary.get(field_name)
        if actual is None or expected is None:
            if actual is not None or expected is not None:
                raise ActiveRiskDecisionReportError(f"{label} {field_name} drift")
        else:
            _assert_close(actual, expected, label=f"{label} {field_name}")
    expected_target_days = summary.get("median_days_to_target")
    actual_target_days = statistics.median(days_to_target) if days_to_target else None
    if expected_target_days is None or actual_target_days is None:
        if expected_target_days is not None or actual_target_days is not None:
            raise ActiveRiskDecisionReportError(f"{label} median_days_to_target drift")
    else:
        _assert_close(
            actual_target_days,
            expected_target_days,
            label=f"{label} median_days_to_target",
        )
    sequence_fields = {
        "net_values": nets,
        "target_progress_values": progress,
        "duration_trading_days_values": durations,
        "active_trading_days_values": active_durations,
        "calendar_days_values": calendar_durations,
        "days_to_target_values": days_to_target,
    }
    for field_name, actual_values in sequence_fields.items():
        expected_values = summary.get(field_name)
        if not isinstance(expected_values, list) or len(expected_values) != len(
            actual_values
        ):
            raise ActiveRiskDecisionReportError(f"{label} {field_name} cardinality drift")
        for index, (actual, expected) in enumerate(
            zip(actual_values, expected_values, strict=True)
        ):
            _assert_close(
                actual,
                expected,
                label=f"{label} {field_name}[{index}]",
            )
    if int(summary.get("pass_block_count", -1)) != len(pass_blocks):
        raise ActiveRiskDecisionReportError(f"{label} pass-block count drift")
    if list(summary.get("pass_block_ids") or ()) != sorted(pass_blocks):
        raise ActiveRiskDecisionReportError(f"{label} pass-block identity drift")

    def reconcile_numeric_mapping(
        field_name: str, actual: Mapping[str, float]
    ) -> None:
        expected = summary.get(field_name)
        if not isinstance(expected, Mapping) or set(expected) != set(actual):
            raise ActiveRiskDecisionReportError(f"{label} {field_name} key drift")
        for key, value in actual.items():
            _assert_close(
                value,
                expected[key],
                label=f"{label} {field_name}[{key}]",
            )

    reconcile_numeric_mapping("by_block_net", dict(sorted(by_block_net.items())))
    reconcile_numeric_mapping(
        "by_block_target_progress_median",
        {
            key: statistics.median(values)
            for key, values in sorted(by_block_progress.items())
        },
    )
    reconcile_numeric_mapping(
        "component_contribution", dict(sorted(component_contribution.items()))
    )
    positive_block_total = sum(max(value, 0.0) for value in by_block_net.values())
    positive_component_total = sum(
        max(value, 0.0) for value in component_contribution.values()
    )
    concentration = {
        "maximum_block_profit_share": (
            max((max(value, 0.0) for value in by_block_net.values()), default=0.0)
            / positive_block_total
            if positive_block_total > 0.0
            else 0.0
        ),
        "maximum_sleeve_profit_share": (
            max(
                (max(value, 0.0) for value in component_contribution.values()),
                default=0.0,
            )
            / positive_component_total
            if positive_component_total > 0.0
            else 0.0
        ),
    }
    for field_name, actual in concentration.items():
        _assert_close(actual, summary.get(field_name), label=f"{label} {field_name}")


def _candidate_summary(
    row: Mapping[str, Any],
    blocks: Sequence[BlockSpec],
    controls: Mapping[str, Any],
    promoted96: set[str],
    surviving96: set[str],
    finalists: set[str],
    expected_episode_starts_per_scenario: int,
) -> tuple[
    dict[str, Any], list[Mapping[str, Any]], list[Mapping[str, Any]]
]:
    policy_id = str(row.get("policy_id") or "")
    if not policy_id:
        raise ActiveRiskDecisionReportError("Stage-3 row lacks policy id")
    evidence_raw = list(row.get("evidence_raw") or ())
    if not all(isinstance(raw, Mapping) for raw in evidence_raw):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} evidence contains a malformed episode"
        )
    observed_horizons = {str(raw.get("horizon_label") or "") for raw in evidence_raw}
    if observed_horizons != set(HORIZONS):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} frozen-horizon coverage drift"
        )
    for raw in evidence_raw:
        if str(raw.get("campaign_id") or "") != CAMPAIGN_ID:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} raw campaign identity drift"
            )
        if str(raw.get("policy_id") or "") != policy_id:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} raw policy identity drift"
            )
    horizon_rows: dict[str, list[Mapping[str, Any]]] = {
        horizon: _unique_horizon_rows(
            evidence_raw, horizon=horizon, policy_id=policy_id
        )
        for horizon in HORIZONS
    }
    canonical_raw = horizon_rows[CANONICAL_HORIZON]
    full_raw = horizon_rows[FULL_HORIZON]
    canonical_keys = {
        (_scenario_key(raw.get("scenario")), int(raw["start_day"]))
        for raw in canonical_raw
    }
    for horizon, selected in horizon_rows.items():
        keys = {
            (_scenario_key(raw.get("scenario")), int(raw["start_day"]))
            for raw in selected
        }
        if keys != canonical_keys:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} {horizon} episode-start key coverage drift"
            )
        for scenario in SCENARIOS:
            scenario_rows = [
                raw
                for raw in selected
                if _scenario_key(raw.get("scenario")) == scenario
            ]
            if len(scenario_rows) != expected_episode_starts_per_scenario:
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} {scenario} {horizon} does not have "
                    f"{expected_episode_starts_per_scenario} frozen starts"
                )
            summary = ((row.get("horizons") or {}).get(scenario) or {}).get(horizon)
            if not isinstance(summary, Mapping):
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} lacks {scenario} {horizon} summary"
                )
            _reconcile_episode_summary(
                policy_id=policy_id,
                scenario=scenario,
                horizon=horizon,
                rows=scenario_rows,
                summary=summary,
                blocks=blocks,
            )
        starts_by_scenario = {
            scenario: {
                int(raw["start_day"])
                for raw in selected
                if _scenario_key(raw.get("scenario")) == scenario
            }
            for scenario in SCENARIOS
        }
        if starts_by_scenario["normal"] != starts_by_scenario["stressed"]:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} {horizon} normal/stressed start-set drift"
            )
    for scenario in SCENARIOS:
        canonical_summary = ((row.get("horizons") or {}).get(scenario) or {}).get(
            CANONICAL_HORIZON
        )
        if not isinstance(canonical_summary, Mapping) or dict(
            row.get(scenario) or {}
        ) != dict(canonical_summary):
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} canonical {scenario} summary identity drift"
            )
    candidate_blocks: dict[str, dict[str, BlockAccumulator]] = {
        scenario: {block.block_id: BlockAccumulator() for block in blocks}
        for scenario in SCENARIOS
    }
    for raw in canonical_raw:
        scenario = _scenario_key(raw.get("scenario"))
        block = _block_for_day(int(raw["start_day"]), blocks)
        candidate_blocks[scenario][block].add(raw)
    derived_diagnostics = _derive_canonical_account_diagnostics(canonical_raw)
    for field_name, derived in derived_diagnostics.items():
        cached = row.get(field_name)
        if not isinstance(cached, Mapping) or dict(cached) != derived:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} cached {field_name} diverges from "
                "sealed canonical daily paths"
            )
    compact = {
        "policy_id": policy_id,
        "structural_fingerprint": row.get("structural_fingerprint"),
        "sealed_normalized_account_behavior_fingerprint": (
            _normalized_behavior_fingerprint(canonical_raw)
        ),
        "cached_actual_account_behavior_fingerprint_status": (
            "OMITTED_UNSEALED_ORDER_SENSITIVE_CACHE_SELF_HASH"
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
        "risk_utilisation": dict(derived_diagnostics["risk_utilisation"]),
        "exposure_signature": dict(derived_diagnostics["exposure_signature"]),
        "suppression": dict(derived_diagnostics["suppression"]),
        "promotion": {
            "promoted_to_96": policy_id in promoted96,
            "survived_96": policy_id in surviving96,
            "development_finalist": policy_id in finalists,
            "promotion_mutated_by_report": False,
        },
    }
    compact["control_deltas"] = _candidate_controls(compact, controls)
    return compact, canonical_raw, full_raw


def _load_optional_snapshot(
    path: Path | None, *, hash_field: str, label: str
) -> tuple[Mapping[str, Any] | None, dict[str, Any]]:
    if path is None:
        return None, {"available": False, "path": None, "status": "NOT_REQUESTED"}
    if not path.exists():
        return None, {
            "available": False,
            "path": str(path),
            "status": "NOT_YET_PERSISTED",
        }
    payload = _load_json(path)
    if not isinstance(payload, Mapping):
        raise ActiveRiskDecisionReportError(f"{label} snapshot is not an object")
    if not payload.get(hash_field):
        raise ActiveRiskDecisionReportError(
            f"{label} snapshot lacks required {hash_field}"
        )
    _verify_embedded_hash(payload, hash_field, label)
    if str(payload.get("campaign_id") or "") != CAMPAIGN_ID:
        raise ActiveRiskDecisionReportError(f"{label} campaign identity drift")
    return payload, {
        "available": True,
        "path": str(path),
        "sha256": file_sha256(path),
        hash_field: str(payload[hash_field]),
        "status": "HASH_VALIDATED",
    }


def _production_context(
    *,
    final_result_path: Path | None,
    production_state_path: Path | None,
    campaign_manifest_path: Path,
    campaign_manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    result, result_provenance = _load_optional_snapshot(
        final_result_path, hash_field="result_hash", label="economic final result"
    )
    state, state_provenance = _load_optional_snapshot(
        production_state_path, hash_field="state_hash", label="production state"
    )
    if result is None or state is None:
        raise ActiveRiskDecisionReportError(
            "hash-validated campaign final result and production state are required"
        )
    if str(result.get("status") or "") != "COMPLETE":
        raise ActiveRiskDecisionReportError("economic final result is not terminal")
    if str(state.get("state") or "") != "COMPLETE":
        raise ActiveRiskDecisionReportError("production state is not terminal")
    manifest_hash = str(campaign_manifest.get("manifest_hash") or "")
    source_commit = str(campaign_manifest.get("source_commit") or "")
    for snapshot, label in ((result, "economic final result"), (state, "production state")):
        if str(snapshot.get("manifest_hash") or "") != manifest_hash:
            raise ActiveRiskDecisionReportError(f"{label} manifest linkage drift")
        if str(snapshot.get("source_commit") or "") != source_commit:
            raise ActiveRiskDecisionReportError(f"{label} source-commit linkage drift")
    evidence = result.get("evidence_bundle")
    if not isinstance(evidence, Mapping):
        raise ActiveRiskDecisionReportError(
            "economic final result lacks authoritative EvidenceBundle receipt"
        )
    if evidence.get("evidence_status") != "FRESH_DEVELOPMENT_EVIDENCE" or (
        evidence.get("reconstruction_flag") is not False
    ):
        raise ActiveRiskDecisionReportError(
            "economic final result EvidenceBundle status is not authoritative fresh evidence"
        )
    required_evidence_fields = (
        "campaign_id",
        "bundle_path",
        "manifest_path",
        "manifest_sha256",
        "bundle_content_sha256",
        "dataset_row_counts",
    )
    missing_evidence = [field for field in required_evidence_fields if not evidence.get(field)]
    if missing_evidence:
        raise ActiveRiskDecisionReportError(
            "economic final result EvidenceBundle receipt is incomplete: "
            + ", ".join(missing_evidence)
        )
    if str(result.get("evidence_verification_manifest_sha256") or "") != str(
        evidence["manifest_sha256"]
    ):
        raise ActiveRiskDecisionReportError(
            "economic final result EvidenceBundle manifest linkage drift"
        )
    bundle_path = Path(str(evidence["bundle_path"])).resolve()
    evidence_manifest_path = Path(str(evidence["manifest_path"])).resolve()
    expected_evidence_manifest_path = (
        bundle_path / "evidence_bundle_manifest.json"
    ).resolve()
    if not bundle_path.is_dir() or not evidence_manifest_path.is_file():
        raise ActiveRiskDecisionReportError(
            "authoritative EvidenceBundle or verification manifest is absent"
        )
    if evidence_manifest_path != expected_evidence_manifest_path:
        raise ActiveRiskDecisionReportError(
            "authoritative EvidenceBundle manifest path drift"
        )
    if file_sha256(evidence_manifest_path) != str(evidence["manifest_sha256"]):
        raise ActiveRiskDecisionReportError(
            "authoritative EvidenceBundle verification manifest hash drift"
        )
    try:
        verified_bundle = verify_evidence_bundle(bundle_path, deep=True)
    except (EvidenceBundleError, EvidenceContractError) as exc:
        raise ActiveRiskDecisionReportError(
            f"authoritative EvidenceBundle verification failed: {exc}"
        ) from exc
    receipt_comparisons = {
        "campaign_id": verified_bundle.get("campaign_id"),
        "bundle_content_sha256": verified_bundle.get("bundle_content_sha256"),
        "evidence_status": verified_bundle.get("evidence_status"),
        "reconstruction_flag": verified_bundle.get("reconstruction_flag"),
        "dataset_row_counts": verified_bundle.get("dataset_row_counts"),
    }
    for field_name, expected in receipt_comparisons.items():
        observed = evidence.get(field_name)
        if isinstance(expected, Mapping):
            matches = isinstance(observed, Mapping) and dict(observed) == dict(expected)
        else:
            matches = observed == expected
        if not matches:
            raise ActiveRiskDecisionReportError(
                f"authoritative EvidenceBundle receipt {field_name} drift"
            )
    identity = _load_json(bundle_path / "identity.json")
    if not isinstance(identity, Mapping):
        raise ActiveRiskDecisionReportError(
            "authoritative EvidenceBundle identity is malformed"
        )
    if str(identity.get("campaign_id") or "") != CAMPAIGN_ID:
        raise ActiveRiskDecisionReportError(
            "authoritative EvidenceBundle campaign identity drift"
        )
    if str(identity.get("source_commit") or "") != source_commit:
        raise ActiveRiskDecisionReportError(
            "authoritative EvidenceBundle source-commit drift"
        )
    if str(identity.get("configuration_sha256") or "") != file_sha256(
        campaign_manifest_path
    ):
        raise ActiveRiskDecisionReportError(
            "authoritative EvidenceBundle frozen-manifest checksum drift"
        )
    economic = result.get("economic_results")
    if not isinstance(economic, Mapping):
        raise ActiveRiskDecisionReportError(
            "economic final result lacks economic_results"
        )
    if str(economic.get("campaign_id") or "") != CAMPAIGN_ID:
        raise ActiveRiskDecisionReportError(
            "economic final result embedded campaign identity drift"
        )
    bundle_summary = _load_json(bundle_path / "outputs" / "campaign_summary.json")
    if not isinstance(bundle_summary, Mapping) or dict(bundle_summary) != dict(economic):
        raise ActiveRiskDecisionReportError(
            "economic final result diverges from EvidenceBundle campaign summary"
        )
    economic_controls = economic.get("matched_controls")
    final_controls = result.get("matched_controls")
    if (
        not isinstance(economic_controls, Mapping)
        or not isinstance(final_controls, Mapping)
        or dict(final_controls) != dict(economic_controls)
    ):
        raise ActiveRiskDecisionReportError(
            "final-result matched controls diverge from sealed campaign summary"
        )
    successive = result.get("successive_halving")
    result_stage_decisions = (
        successive.get("stage_decisions")
        if isinstance(successive, Mapping)
        else None
    )
    bundle_pareto = _load_json(bundle_path / "outputs" / "pareto_archive.json")
    bundle_stage_decisions = (
        bundle_pareto.get("stage_decisions")
        if isinstance(bundle_pareto, Mapping)
        else None
    )
    if (
        not isinstance(result_stage_decisions, list)
        or not isinstance(bundle_stage_decisions, list)
        or result_stage_decisions != bundle_stage_decisions
    ):
        raise ActiveRiskDecisionReportError(
            "final-result halving decisions diverge from sealed Pareto archive"
        )

    def required_counter(
        source: Mapping[str, Any], field_name: str, *, label: str
    ) -> int:
        value = source.get(field_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ActiveRiskDecisionReportError(
                f"{label} {field_name} counter is absent or invalid"
            )
        return int(value)

    counters = economic.get("production_counters")
    if not isinstance(counters, Mapping):
        raise ActiveRiskDecisionReportError(
            "economic final result lacks production counters"
        )
    result_counter_values = {
        "combine_episodes_completed": required_counter(
            counters, "combine_episodes_completed", label="economic final result"
        ),
        "normal_episodes_completed": required_counter(
            counters, "normal_episodes_completed", label="economic final result"
        ),
        "stressed_episodes_completed": required_counter(
            counters, "stressed_episodes_completed", label="economic final result"
        ),
    }
    if result_counter_values["combine_episodes_completed"] != (
        result_counter_values["normal_episodes_completed"]
        + result_counter_values["stressed_episodes_completed"]
    ):
        raise ActiveRiskDecisionReportError(
            "economic final result scenario counters do not sum to total episodes"
        )
    bundle_episode_count = required_counter(
        verified_bundle["dataset_row_counts"],
        "episodes",
        label="authoritative EvidenceBundle",
    )
    if result_counter_values["combine_episodes_completed"] != bundle_episode_count:
        raise ActiveRiskDecisionReportError(
            "economic final result counters diverge from EvidenceBundle episodes"
        )
    state_counter_values = {
        field_name: required_counter(state, field_name, label="production state")
        for field_name in result_counter_values
    }
    if state_counter_values != result_counter_values:
        raise ActiveRiskDecisionReportError(
            "production state episode counters diverge from final result"
        )
    funnel_links = (
        ("governor_proposals_generated", "policies_proposed"),
        ("unique_policies_screened", "unique_policies_screened"),
        ("exact_account_replays", "exact_account_replays"),
    )
    for economic_field, state_field in funnel_links:
        if required_counter(
            economic, economic_field, label="economic final result"
        ) != required_counter(state, state_field, label="production state"):
            raise ActiveRiskDecisionReportError(
                f"production state {state_field} diverges from final result"
            )
    state_bundle_path = state.get("evidence_bundle_path")
    if state_bundle_path is None or Path(str(state_bundle_path)).resolve() != bundle_path:
        raise ActiveRiskDecisionReportError(
            "production state EvidenceBundle path linkage drift"
        )
    if str(state.get("evidence_bundle_manifest_sha256") or "") != str(
        evidence["manifest_sha256"]
    ):
        raise ActiveRiskDecisionReportError(
            "production state EvidenceBundle manifest linkage drift"
        )
    configured_state_bundle_path = state.get("evidence_final_path")
    if configured_state_bundle_path is not None and Path(
        str(configured_state_bundle_path)
    ).resolve() != bundle_path:
        raise ActiveRiskDecisionReportError(
            "production state configured EvidenceBundle path drift"
        )
    context: dict[str, Any] = {
        "source": "DEEP_VERIFIED_EVIDENCE_BUNDLE_AND_TERMINAL_SNAPSHOTS",
        "final_result_available": True,
        "production_state_available": True,
        "identity_audit": (
            dict(economic["identity_audit"])
            if isinstance(economic.get("identity_audit"), Mapping)
            else None
        ),
        "identity_audit_status": (
            "PASS"
            if (economic.get("identity_audit") or {}).get("passed") is True
            else "FAIL"
            if (economic.get("identity_audit") or {}).get("passed") is False
            else (economic.get("identity_audit") or {}).get("status")
            if isinstance(economic.get("identity_audit"), Mapping)
            else None
        ),
        "current_production_funnel": {
            "governor_proposals_generated": economic["governor_proposals_generated"],
            "unique_policies_screened": economic["unique_policies_screened"],
            "exact_account_replays": economic["exact_account_replays"],
            "stage3_policy_count": economic["stage3_policy_count"],
            **result_counter_values,
        },
        "scientific_status": result.get("scientific_status")
        or economic.get("scientific_status"),
        "runtime_state": state.get("state"),
        "runtime_stage": state.get("stage"),
        "current_bottleneck": None,
        "current_bottleneck_status": "UNAVAILABLE_NOT_PERSISTED",
        "next_autonomous_action": result.get("autonomous_next_action"),
        "evidence_bundle": {
            "path": str(bundle_path),
            "manifest_sha256": str(evidence["manifest_sha256"]),
            "bundle_content_sha256": str(evidence["bundle_content_sha256"]),
            "dataset_row_counts": dict(evidence["dataset_row_counts"]),
            "verification": "DEEP_VERIFIED",
        },
    }
    if result.get("current_bottleneck") is not None:
        context["current_bottleneck"] = result["current_bottleneck"]
        context["current_bottleneck_status"] = "PERSISTED_IN_FINAL_RESULT"
    elif state.get("error") is not None:
        context["current_bottleneck"] = state["error"]
        context["current_bottleneck_status"] = "PERSISTED_RUNTIME_ERROR"
    return context, {
        "economic_final_result": result_provenance,
        "production_state": state_provenance,
        "evidence_bundle": {
            "path": str(bundle_path),
            "manifest_path": str(evidence_manifest_path),
            "manifest_sha256": str(evidence["manifest_sha256"]),
            "bundle_content_sha256": str(evidence["bundle_content_sha256"]),
            "dataset_row_counts": dict(evidence["dataset_row_counts"]),
            "deep_verification": True,
        },
    }, {
        "matched_controls": dict(economic_controls),
        "stage_decisions": [dict(value) for value in result_stage_decisions],
        "bundle_path": str(bundle_path),
        "economic_results": dict(economic),
        "bundle_manifest": dict(verified_bundle),
        "bundle_identity": dict(identity),
    }


def _validate_sealed_stage3_aggregates(
    *,
    economic: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    horizons: Mapping[str, Mapping[str, HorizonAccumulator]],
    risk: RiskAccumulator,
    suppression: SuppressionAccumulator,
) -> None:
    sealed_frontier = economic.get("horizon_frontier")
    if not isinstance(sealed_frontier, Mapping):
        raise ActiveRiskDecisionReportError(
            "sealed campaign summary lacks its Stage-3 horizon frontier"
        )
    for horizon in HORIZONS:
        sealed_horizon = sealed_frontier.get(horizon)
        if not isinstance(sealed_horizon, Mapping):
            raise ActiveRiskDecisionReportError(
                f"sealed campaign summary lacks horizon {horizon}"
            )
        for scenario in SCENARIOS:
            sealed = sealed_horizon.get(scenario)
            if not isinstance(sealed, Mapping):
                raise ActiveRiskDecisionReportError(
                    f"sealed campaign summary lacks {scenario} {horizon}"
                )
            observed = horizons[scenario][horizon].to_dict()
            for field_name in (
                "policy_count",
                "pass_count",
                "episode_count",
                "censored_episode_count",
            ):
                if int(sealed.get(field_name, -1)) != int(observed[field_name]):
                    raise ActiveRiskDecisionReportError(
                        f"sealed Stage-3 {scenario} {horizon} {field_name} drift"
                    )
            for field_name in ("pass_rate", "mll_breach_rate"):
                _assert_close(
                    observed[field_name],
                    sealed.get(field_name),
                    label=f"sealed Stage-3 {scenario} {horizon} {field_name}",
                )
            distributions = observed["policy_level_distributions"]
            frontier_distribution_fields = (
                ("target_progress_median", "target_progress_policy_median"),
                (
                    "projected_active_days_to_target_median",
                    "projected_active_days_to_target_policy_median",
                ),
                (
                    "projected_calendar_days_to_target_median",
                    "projected_calendar_days_to_target_policy_median",
                ),
            )
            for distribution_name, sealed_name in frontier_distribution_fields:
                observed_value = (distributions.get(distribution_name) or {}).get(
                    "median"
                )
                sealed_value = sealed.get(sealed_name)
                if observed_value is None or sealed_value is None:
                    if observed_value is not None or sealed_value is not None:
                        raise ActiveRiskDecisionReportError(
                            f"sealed Stage-3 {scenario} {horizon} {sealed_name} drift"
                        )
                else:
                    _assert_close(
                        observed_value,
                        sealed_value,
                        label=f"sealed Stage-3 {scenario} {horizon} {sealed_name}",
                    )
    for scenario, headline in (
        ("normal", "normal_combine_passes"),
        ("stressed", "stressed_combine_passes"),
    ):
        observed_passes = horizons[scenario][CANONICAL_HORIZON].pass_count
        if int(economic.get(headline, -1)) != observed_passes:
            raise ActiveRiskDecisionReportError(
                f"sealed Stage-3 {scenario} canonical pass headline drift"
            )
        observed_target = statistics.median(
            _required_float(
                candidate[scenario].get("target_progress_median"),
                label=f"candidate {scenario} target progress",
            )
            for candidate in candidates
        )
        _assert_close(
            observed_target,
            economic.get(f"{scenario}_target_progress_median"),
            label=f"sealed Stage-3 {scenario} target-progress headline",
        )
    observed_stressed_mll_max = max(
        (
            _required_float(
                candidate["stressed"].get("mll_breach_rate"),
                label="candidate stressed MLL breach rate",
            )
            for candidate in candidates
        ),
        default=0.0,
    )
    _assert_close(
        observed_stressed_mll_max,
        economic.get("stressed_mll_breach_rate_maximum"),
        label="sealed Stage-3 stressed MLL maximum",
    )
    sealed_risk = economic.get("risk_utilisation")
    if not isinstance(sealed_risk, Mapping):
        raise ActiveRiskDecisionReportError(
            "sealed campaign summary lacks risk utilisation"
        )
    observed_risk = risk.to_dict()
    if int(sealed_risk.get("observation_count", -1)) != int(
        observed_risk["observation_count"]
    ):
        raise ActiveRiskDecisionReportError(
            "sealed Stage-3 risk-utilisation observation count drift"
        )
    _assert_close(
        observed_risk["mean"],
        sealed_risk.get("mean"),
        label="sealed Stage-3 risk-utilisation mean",
    )
    _assert_close(
        observed_risk["policy_median_distribution"]["median"],
        sealed_risk.get("policy_median_of_medians"),
        label="sealed Stage-3 risk-utilisation policy median",
    )
    sealed_suppression = economic.get("suppression")
    if not isinstance(sealed_suppression, Mapping):
        raise ActiveRiskDecisionReportError(
            "sealed campaign summary lacks suppression evidence"
        )
    observed_suppression = suppression.to_dict()
    for field_name in (
        "signals_emitted",
        "signals_accepted",
        "signals_rejected",
        "decision_status_counts",
    ):
        if sealed_suppression.get(field_name) != observed_suppression.get(field_name):
            raise ActiveRiskDecisionReportError(
                f"sealed Stage-3 suppression {field_name} drift"
            )
    _assert_close(
        observed_suppression["foregone_realized_pnl_ex_post"],
        sealed_suppression.get("foregone_realized_pnl_ex_post"),
        label="sealed Stage-3 foregone realized PnL",
    )


def build_active_risk_decision_report(
    *,
    manifest_path: Path,
    stage3_cache_dir: Path,
    matched_controls_path: Path,
    halving_dir: Path,
    expected_stage3_count: int = 256,
    final_result_path: Path | None = None,
    production_state_path: Path | None = None,
) -> dict[str, Any]:
    """Build the revision-02 report while holding at most one Stage-3 cache."""

    manifest = _load_json(manifest_path)
    if not isinstance(manifest, Mapping):
        raise ActiveRiskDecisionReportError("manifest is not an object")
    if str(manifest.get("campaign_id") or "") != CAMPAIGN_ID:
        raise ActiveRiskDecisionReportError("campaign manifest identity drift")
    if not manifest.get("manifest_hash") or not manifest.get("source_commit"):
        raise ActiveRiskDecisionReportError(
            "campaign manifest lacks immutable manifest/source identity"
        )
    _verify_embedded_hash(manifest, "manifest_hash", "campaign manifest")
    blocks = _block_specs(manifest)
    try:
        manifest_start_count = int(
            (manifest.get("episode_starts") or {})["serious_policy_starts"]
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskDecisionReportError(
            "manifest serious-policy start count is absent"
        ) from exc
    if manifest_start_count != EXPECTED_EPISODE_STARTS_PER_SCENARIO:
        raise ActiveRiskDecisionReportError(
            "manifest serious-policy start count is not the frozen 48"
        )
    controls, controls_provenance = _load_controls(matched_controls_path)
    halving, halving_provenance = _load_halving(halving_dir)
    if final_result_path is None:
        final_result_path = halving_dir.parent / "economic_production_result.json"
    if production_state_path is None:
        production_state_path = halving_dir.parent / "production_state.json"
    (
        production_context,
        production_context_provenance,
        authoritative_chain,
    ) = _production_context(
        final_result_path=final_result_path,
        production_state_path=production_state_path,
        campaign_manifest_path=manifest_path,
        campaign_manifest=manifest,
    )
    raw_controls = _load_json(matched_controls_path)
    if not isinstance(raw_controls, Mapping) or dict(raw_controls) != dict(
        authoritative_chain["matched_controls"]
    ):
        raise ActiveRiskDecisionReportError(
            "external matched controls diverge from authoritative EvidenceBundle"
        )
    raw_halving_decisions = [
        _load_json(path) for path in sorted(halving_dir.glob("stage*.json"))
    ]
    if raw_halving_decisions != authoritative_chain["stage_decisions"]:
        raise ActiveRiskDecisionReportError(
            "external halving decisions diverge from authoritative EvidenceBundle"
        )
    stage3_partitions = _stage3_partition_index(
        authoritative_chain["bundle_manifest"],
        expected_policy_count=expected_stage3_count,
    )
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
    exposure = ExposureAccumulator()
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

    for batch_index, path in enumerate(paths):
        if path.name != f"batch_{batch_index:06d}.json":
            raise ActiveRiskDecisionReportError(
                f"Stage-3 cache filename/index drift: {path}"
            )
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
        compact, canonical_raw, full_raw = _candidate_summary(
            row,
            blocks,
            controls,
            promoted96,
            surviving96,
            finalists,
            EXPECTED_EPISODE_STARTS_PER_SCENARIO,
        )
        policy_id = str(compact["policy_id"])
        sealed_policy_fingerprints = authoritative_chain["bundle_identity"].get(
            "policy_fingerprints"
        )
        if not isinstance(sealed_policy_fingerprints, Mapping) or str(
            sealed_policy_fingerprints.get(policy_id) or ""
        ) != str(compact.get("structural_fingerprint") or ""):
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} structural fingerprint diverges from EvidenceBundle"
            )
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
        lifecycle_rows = list(row.get("lifecycle_rows") or ())
        full_by_key = {
            (_scenario_key(raw.get("scenario")), int(raw["start_day"])): raw
            for raw in full_raw
        }
        full_pass_keys = {
            key
            for key, raw in full_by_key.items()
            if str(raw.get("terminal_classification") or "") == "TARGET_REACHED"
        }
        lifecycle_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
        for lifecycle_row in lifecycle_rows:
            if not isinstance(lifecycle_row, Mapping):
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} has malformed XFA lifecycle evidence"
                )
            try:
                key = (
                    _scenario_key(lifecycle_row.get("scenario")),
                    int(lifecycle_row["combine_start_day"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} has unkeyed XFA lifecycle evidence"
                ) from exc
            if key in lifecycle_by_key:
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} has duplicate XFA lifecycle evidence {key}"
                )
            lifecycle_by_key[key] = lifecycle_row
        if set(lifecycle_by_key) != full_pass_keys:
            raise ActiveRiskDecisionReportError(
                f"candidate {policy_id} FULL-pass/XFA authoritative bijection drift"
            )
        candidate_lifecycle = {
            scenario: {name: LifecyclePathAccumulator() for name in PATHS}
            for scenario in SCENARIOS
        }
        for raw in full_raw:
            scenario = _scenario_key(raw.get("scenario"))
            for name in PATHS:
                lifecycle[scenario][name].add_combine_episode(raw)
                candidate_lifecycle[scenario][name].add_combine_episode(raw)
        for key, lifecycle_row in lifecycle_by_key.items():
            scenario, start_day = key
            full_episode = full_by_key[key]
            if int(lifecycle_row.get("combine_end_day", -1)) != int(
                full_episode.get("end_day", -2)
            ):
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} XFA/Combine end-day linkage drift"
                )
            try:
                sealed_lifecycle = _active_pool_lifecycle_evidence(
                    lifecycle_row,
                    expected_policy_id=policy_id,
                    expected_scenario=str(full_episode["scenario"]),
                    expected_start_day=start_day,
                )
            except (ActiveRiskRuntimeError, KeyError, TypeError, ValueError) as exc:
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} authoritative XFA lifecycle validation failed: {exc}"
                ) from exc
            combine_end_day = int(sealed_lifecycle["combine_end_day"])
            xfa_start_day = (
                None
                if sealed_lifecycle["xfa_start_day"] is None
                else int(sealed_lifecycle["xfa_start_day"])
            )
            if xfa_start_day is not None and xfa_start_day <= combine_end_day:
                raise ActiveRiskDecisionReportError(
                    f"candidate {policy_id} XFA start is not strictly after Combine end"
                )
            for lifecycle_path in PATHS:
                value = sealed_lifecycle.get(lifecycle_path)
                if not isinstance(value, Mapping):
                    raise ActiveRiskDecisionReportError(
                        f"candidate {policy_id} lacks {lifecycle_path} XFA path"
                    )
                if int(value.get("requested_horizon_days", -1)) != int(
                    sealed_lifecycle["xfa_horizon_days"]
                ):
                    raise ActiveRiskDecisionReportError(
                        f"candidate {policy_id} {lifecycle_path} XFA requested horizon drift"
                    )
                _validate_xfa_path_accounting(
                    value,
                    label=f"candidate {policy_id} {scenario} {lifecycle_path}",
                    combine_end_day=combine_end_day,
                    xfa_start_day=xfa_start_day,
                    rule_snapshot=sealed_lifecycle["rule_snapshot"],
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

        projected_signatures = _stage3_projected_signatures(row, manifest)
        for dataset, suffix in (
            ("episodes", "episodes"),
            ("account_daily_paths", "daily"),
        ):
            batch_id = f"active:stage3:{batch_index:06d}:{suffix}"
            expected_partition = stage3_partitions[(dataset, batch_id)]
            observed_signature = projected_signatures[dataset]
            if (
                int(expected_partition.get("row_count", -1))
                != observed_signature["row_count"]
                or str(expected_partition.get("payload_sha256") or "")
                != observed_signature["payload_sha256"]
            ):
                raise ActiveRiskDecisionReportError(
                    f"Stage-3 cache diverges from sealed {dataset} partition {batch_id}"
                )

        for raw in canonical_raw:
            scenario = _scenario_key(raw.get("scenario"))
            block_id = _block_for_day(int(raw["start_day"]), blocks)
            block_accumulators[scenario][block_id].add(raw)
        risk.add(compact["risk_utilisation"])
        exposure.add(compact["exposure_signature"])
        suppression.add(compact["suppression"])
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
        del (
            projected_signatures,
            lifecycle_rows,
            canonical_raw,
            full_raw,
            row,
            rows,
            payload,
        )

    observed_ids = set(candidates_by_id)
    if int(
        production_context["current_production_funnel"]["stage3_policy_count"]
    ) != len(observed_ids):
        raise ActiveRiskDecisionReportError(
            "authoritative final result Stage-3 policy count diverges from caches"
        )
    minimum_stage3_episode_rows = (
        len(observed_ids)
        * EXPECTED_EPISODE_STARTS_PER_SCENARIO
        * len(SCENARIOS)
        * len(HORIZONS)
    )
    if int(
        production_context["current_production_funnel"][
            "combine_episodes_completed"
        ]
    ) < minimum_stage3_episode_rows:
        raise ActiveRiskDecisionReportError(
            "authoritative episode counters undercount reconciled Stage-3 caches"
        )
    if set(controls.get("random_priority_by_policy") or {}) != observed_ids:
        raise ActiveRiskDecisionReportError(
            "random-priority control policy coverage drift"
        )
    if set(
        controls.get("random_priority_exposure_match_by_policy") or {}
    ) != observed_ids:
        raise ActiveRiskDecisionReportError(
            "random-priority exposure-match policy coverage drift"
        )
    if not promoted96.issubset(observed_ids):
        raise ActiveRiskDecisionReportError("Stage-3 promotions reference absent policies")
    if not surviving96.issubset(promoted96):
        raise ActiveRiskDecisionReportError("96-start survivors were not Stage-3 promotions")
    if not finalists.issubset(surviving96):
        raise ActiveRiskDecisionReportError("finalists were not 96-start survivors")

    _validate_sealed_stage3_aggregates(
        economic=authoritative_chain["economic_results"],
        candidates=candidates,
        horizons=horizon_accumulators,
        risk=risk,
        suppression=suppression,
    )

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
            "halving_decision_hashes_required": True,
            "canonical_episode_keys_unique": True,
            "all_five_horizon_summaries_reconciled_to_raw_evidence": True,
            "exact_48_starts_per_scenario_and_policy": True,
            "identical_start_keys_across_all_horizons": True,
            "identical_normal_and_stressed_start_sets_per_horizon": True,
            "combine_data_censoring_separated_from_operational_horizon": True,
            "production_context_campaign_bound_and_required": True,
            "authoritative_evidence_bundle_deep_verified_and_reconciled": True,
            "stage3_cache_partitions_reproduced_from_sealed_bundle": True,
            "stage3_replay_fields_redriven_from_sealed_daily_paths": True,
            "candidate_risk_exposure_suppression_strictly_redriven": True,
            "unsealed_order_sensitive_behavior_cache_hash_published": False,
            "sealed_normalized_behavior_fingerprint_published": True,
            "sealed_campaign_summary_reconciled_to_stage3_caches": True,
            "matched_controls_and_halving_bound_to_sealed_bundle": True,
            "full_pass_xfa_lifecycle_bijection_valid": True,
            "authoritative_xfa_source_path_profile_rule_hashes_valid": True,
            "xfa_horizon_chronology_and_event_accounting_valid": True,
        },
        "production_context": production_context,
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
            "role": "DESCRIPTIVE_CONTRACT_SEPARATED_SOURCE_BLOCKS",
            "independence_qualification": (
                "BLOCKS_ARE_DISTINCT_SOURCE_PERIODS_BUT_OVERLAPPING_ROLLING_"
                "EPISODE_STARTS_ARE_NOT_INDEPENDENT_OBSERVATIONS"
            ),
            "headline_confirmation_role": "DESCRIPTIVE_DEVELOPMENT_EVIDENCE_ONLY",
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
        "duty_and_exposure_match_evidence": exposure.to_dict(),
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
            "controls_by_block_limitation": (
                "CONTROL_SCHEMA_PERSISTS_ONLY_BY_BLOCK_NET_AND_TARGET_PROGRESS_"
                "MEDIANS;RAW_CONTROL_BLOCK_EPISODES_PASS_MLL_CENSOR_AND_"
                "CONSISTENCY_PATHS_ARE_NOT_AVAILABLE_TO_THIS_REPORT"
            ),
        },
        "candidates": candidates,
        "xfa_lifecycle": {
            "scope": "STAGE3_48_STARTS_FULL_CHRONOLOGICAL_HORIZON",
            "paths_are_alternative_not_additive": True,
            "probability_reporting": {
                "unconditional_lower_bound": (
                    "ALL_FULL_HORIZON_COMBINE_ATTEMPTS_WITH_CENSORED_OR_ZERO_"
                    "OBSERVATION_PATHS_CONTRIBUTING_NO_UNOBSERVED_SUCCESS"
                ),
                "evaluable_only": (
                    "EXCLUDES_DATA_CENSORED_COMBINE_ATTEMPTS_AND_ZERO_"
                    "OBSERVATION_OR_UNRESOLVED_DATA_CENSORED_XFA_PATHS;A_"
                    "FIRST_PAYOUT_OBSERVED_BEFORE_LATER_CENSORING_REMAINS_A_"
                    "KNOWN_FIRST_PAYOUT;COMPLETE_LIFECYCLE_EXPECTATIONS_AND_"
                    "POST_PAYOUT_SURVIVAL_EXCLUDE_LATER_CENSORING"
                ),
            },
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
            "Temporal blocks are descriptive source blocks; overlapping rolling episode starts are not independent observations.",
            "Combine raw pass rates are conservative lower bounds over every frozen start; evaluable rates exclude only DATA_CENSORED paths, while OPERATIONAL_HORIZON_NOT_REACHED remains an observed no-pass-by-horizon outcome.",
            "Risk utilisation covers NORMAL canonical 90-day decision events and declared nominal charges only; it is not time-weighted actual stop-risk or duty cycle.",
            "Exposure signatures are outcome-agnostic duty/exposure matching evidence, not risk utilisation.",
            "Foregone realized PnL is ex-post diagnostic and was never a routing input.",
            "Standard and Consistency XFA paths are alternatives and must not be added as realizable payout.",
            "Control block comparisons are limited to persisted net and target-progress medians because raw block-level control paths are absent from the control schema.",
            "Behavioral clusters use canonical account paths and routing decisions, not source signal/trade ledgers; they are deterministic post-hoc reporting groups and never alter frozen promotions.",
        ],
        "provenance": {
            "manifest": {
                "path": str(manifest_path),
                "sha256": file_sha256(manifest_path),
            },
            "stage3_caches": cache_provenance,
            "matched_controls": controls_provenance,
            "halving": halving_provenance,
            "production_context": production_context_provenance,
        },
    }
    report["report_hash"] = canonical_hash(report)
    return report


def _percent(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{100.0 * _float(value):.2f}%"


def _money(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"${_float(value):,.2f}"


def _number(value: Any, *, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):,.{digits}f}"


def render_markdown(report: Mapping[str, Any]) -> str:
    """Render a compact human report; the JSON remains the complete matrix."""

    lines = [
        "# HYDRA Active-Risk Pool — Decision Report revision_02",
        "",
        "Development-only, post-hoc reporting. This report does not alter the frozen manifest, selection, or promotions.",
        "",
        "## Production context",
        "",
        f"- Source: `{report['production_context']['source'] or 'UNAVAILABLE'}`.",
        f"- Identity audit: `{report['production_context']['identity_audit_status'] or 'UNAVAILABLE'}`.",
        f"- Current bottleneck: `{report['production_context']['current_bottleneck'] or report['production_context']['current_bottleneck_status']}`.",
        f"- Next autonomous action: `{report['production_context']['next_autonomous_action'] or 'UNAVAILABLE'}`.",
        "",
        "| Proposed | Unique screened | Exact replays | Current Stage-3 | Episodes completed |",
        "|---:|---:|---:|---:|---:|",
        (
            f"| {_number(report['production_context']['current_production_funnel']['governor_proposals_generated'], digits=0)} | "
            f"{_number(report['production_context']['current_production_funnel']['unique_policies_screened'], digits=0)} | "
            f"{_number(report['production_context']['current_production_funnel']['exact_account_replays'], digits=0)} | "
            f"{_number(report['production_context']['current_production_funnel']['stage3_policy_count'], digits=0)} | "
            f"{_number(report['production_context']['current_production_funnel']['combine_episodes_completed'], digits=0)} |"
        ),
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
        "| Horizon | Cost | Passes / episodes | Pass rate LB | Pass rate evaluable | Data-censored | Operational horizon | Target P25 (policy median) | Target median (policy median) | MLL rate evaluable | Min-buffer median |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in HORIZONS:
        for scenario in SCENARIOS:
            value = report["horizon_distributions"][scenario][label]
            distributions = value["policy_level_distributions"]
            lines.append(
                f"| {label} | {scenario} | {value['pass_count']} / {value['episode_count']} | "
                f"{_percent(value['pass_rate_raw_lower_bound'])} | "
                f"{_percent(value['pass_rate_evaluable'])} | "
                f"{value['data_censored_episode_count']} | "
                f"{value['operational_horizon_not_reached_count']} | "
                f"{_percent(distributions['target_progress_p25']['median'])} | "
                f"{_percent(distributions['target_progress_median']['median'])} | "
                f"{_percent(value['mll_breach_rate_evaluable'])} | "
                f"{_money(distributions['minimum_mll_buffer']['median'])} |"
            )
    lines.extend(
        [
            "",
            "## Horizon duration, censoring, and subscription proxy",
            "",
            "| Horizon | Cost | Trading days | Active days | Calendar days | Projected active days to target | Projected calendar days to target | Subscription months proxy | Data-censored | Operational horizon |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for label in HORIZONS:
        for scenario in SCENARIOS:
            value = report["horizon_distributions"][scenario][label]
            distributions = value["policy_level_distributions"]
            lines.append(
                f"| {label} | {scenario} | "
                f"{_number((distributions.get('duration_trading_days_median') or {}).get('median'))} | "
                f"{_number((distributions.get('active_trading_days_median') or {}).get('median'))} | "
                f"{_number((distributions.get('calendar_days_median') or {}).get('median'))} | "
                f"{_number((distributions.get('projected_active_days_to_target_median') or {}).get('median'))} | "
                f"{_number((distributions.get('projected_calendar_days_to_target_median') or {}).get('median'))} | "
                f"{_number((distributions.get('monthly_subscription_duration_proxy_median') or {}).get('median'))} | "
                f"{value['data_censored_episode_count']} | "
                f"{value['operational_horizon_not_reached_count']} |"
            )
    lines.extend(
        [
            "",
            "## Descriptive source blocks — canonical 90-day horizon",
            "",
            "Blocks are contract-separated source periods; overlapping rolling episode starts are not independent observations.",
            "",
            "| Block | Cost | Passes / episodes | Pass rate LB | Pass rate evaluable | Data-censored | Operational horizon | Target P25 | Target median | MLL rate evaluable | Min-buffer median | Net median |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for block in report["temporal_blocks"]["definitions"]:
        block_id = block["block_id"]
        for scenario in SCENARIOS:
            value = report["temporal_blocks"]["results"][scenario][block_id]
            lines.append(
                f"| {block_id} | {scenario} | {value['pass_count']} / {value['episode_count']} | "
                f"{_percent(value['pass_rate_raw_lower_bound'])} | "
                f"{_percent(value['pass_rate_evaluable'])} | "
                f"{value['data_censored_episode_count']} | "
                f"{value['operational_horizon_not_reached_count']} | "
                f"{_percent(value['target_progress']['p25'])} | "
                f"{_percent(value['target_progress']['median'])} | "
                f"{_percent(value['mll_breach_rate_evaluable'])} | "
                f"{_money(value['minimum_mll_buffer']['median'])} | "
                f"{_money(value['net_pnl']['median'])} |"
            )
    lines.extend(
        [
            "",
            "## Risk utilisation and suppression",
            "",
            f"- NORMAL canonical 90-day decision-event declared nominal-risk utilisation mean: {_percent(report['risk_utilisation']['mean'])}; this is neither time-weighted actual stop-risk nor duty cycle.",
            f"- Signals emitted / accepted / rejected: {report['suppression_and_foregone_pnl']['signals_emitted']} / {report['suppression_and_foregone_pnl']['signals_accepted']} / {report['suppression_and_foregone_pnl']['signals_rejected']}.",
            f"- Foregone realized PnL, ex-post diagnostic only: {_money(report['suppression_and_foregone_pnl']['foregone_realized_pnl_ex_post'])}.",
            "",
            "## XFA lifecycle — paths reported separately",
            "",
            "| Cost | Path | Combine attempts | XFA paths | First payouts | First-payout / attempt lower bound | First-payout / evaluable lifecycle | Expected trader payout / attempt lower bound | Post-payout survival evaluable |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for scenario in SCENARIOS:
        for path in PATHS:
            value = report["xfa_lifecycle"][scenario][path]
            lower = value["unconditional_lower_bound"]
            evaluable = value["evaluable_only"]
            lines.append(
                f"| {scenario} | {path} | {value['combine_attempts']} | "
                f"{value['xfa_paths_started']} | {value['first_payouts']} | "
                f"{_percent(lower['first_payout_probability_per_combine_attempt'])} | "
                f"{_percent(evaluable['first_payout_probability_per_evaluable_lifecycle_attempt'])} | "
                f"{_money(lower['expected_trader_payout_per_combine_attempt'])} | "
                f"{_percent(evaluable['post_payout_survival_probability_conditional_on_evaluable_first_payout'])} |"
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
        final_result_path=report_dir / "economic_production_result.json",
        production_state_path=report_dir / "production_state.json",
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
