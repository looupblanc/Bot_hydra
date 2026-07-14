from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Sequence


EVIDENCE_BUNDLE_CONTRACT = "HYDRA_EVIDENCE_BUNDLE_V1"
EVIDENCE_BUNDLE_SCHEMA_VERSION = 1
PNL_ABS_TOLERANCE = 1e-6
PATH_METRIC_ABS_TOLERANCE = 1e-9
COST_SCENARIOS = ("NORMAL", "STRESSED_1_5X")
EVIDENCE_STATUSES = (
    "FRESH_DEVELOPMENT_EVIDENCE",
    "AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION",
)


class EvidenceContractError(ValueError):
    """Raised when evidence does not satisfy the immutable bundle contract."""


_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _require_number(row: Mapping[str, Any], field: str) -> None:
    if not _is_number(row[field]):
        raise EvidenceContractError(f"{field} must be a finite number")


def _require_nonempty_string(row: Mapping[str, Any], field: str) -> None:
    if not isinstance(row[field], str) or not row[field].strip():
        raise EvidenceContractError(f"{field} must be a non-empty string")


def _require_boolean(row: Mapping[str, Any], field: str) -> None:
    if not isinstance(row[field], bool):
        raise EvidenceContractError(f"{field} must be a boolean")


def _require_timestamp(row: Mapping[str, Any], field: str) -> None:
    _require_nonempty_string(row, field)
    value = str(row[field])
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceContractError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise EvidenceContractError(f"{field} must include a timezone")


def _validate_signal(row: Mapping[str, Any]) -> None:
    for field in ("campaign_id", "component_id", "signal_id", "market", "contract", "timeframe", "component_role"):
        _require_nonempty_string(row, field)
    _require_timestamp(row, "event_time")
    _require_number(row, "sizing")
    if row["stop"] is not None and not _is_number(row["stop"]):
        raise EvidenceContractError("stop must be null or a finite number")
    if row["target"] is not None and not _is_number(row["target"]):
        raise EvidenceContractError("target must be null or a finite number")
    if not isinstance(row["veto"], bool):
        raise EvidenceContractError("veto must be a boolean")


def _validate_entry(row: Mapping[str, Any]) -> None:
    for field in ("campaign_id", "component_id", "trade_id", "market", "contract", "side"):
        _require_nonempty_string(row, field)
    _require_timestamp(row, "entry_time")
    for field in ("quantity", "entry_price", "sizing"):
        _require_number(row, field)
    for field in ("stop_price", "target_price"):
        if row[field] is not None and not _is_number(row[field]):
            raise EvidenceContractError(f"{field} must be null or a finite number")


def _validate_exit(row: Mapping[str, Any]) -> None:
    for field in ("campaign_id", "component_id", "trade_id", "exit_reason"):
        _require_nonempty_string(row, field)
    _require_timestamp(row, "exit_time")
    _require_number(row, "exit_price")


def _validate_trade(row: Mapping[str, Any]) -> None:
    for field in ("campaign_id", "component_id", "trade_id", "market", "contract", "side"):
        _require_nonempty_string(row, field)
    _require_timestamp(row, "entry_time")
    _require_timestamp(row, "exit_time")
    for field in (
        "quantity",
        "entry_price",
        "exit_price",
        "gross_pnl",
        "costs",
        "net_pnl",
    ):
        _require_number(row, field)
    if float(row["quantity"]) <= 0.0:
        raise EvidenceContractError("quantity must be positive")
    if float(row["costs"]) < 0.0:
        raise EvidenceContractError("costs may not be negative")


def _validate_membership(row: Mapping[str, Any]) -> None:
    for field in ("campaign_id", "policy_id", "component_id", "component_role"):
        _require_nonempty_string(row, field)
    _require_number(row, "risk_allocation")


def _validate_account_path(row: Mapping[str, Any]) -> None:
    for field in (
        "campaign_id",
        "policy_id",
        "episode_id",
        "horizon",
        "trading_day",
        "cost_scenario",
    ):
        _require_nonempty_string(row, field)
    for field in (
        "realized_pnl",
        "unrealized_pnl",
        "daily_pnl",
        "equity",
        "mll",
        "mll_buffer",
        "consistency",
        "target_progress",
        "costs",
        "minimum_mll_buffer",
    ):
        _require_number(row, field)
    if float(row["costs"]) < 0.0:
        raise EvidenceContractError("costs may not be negative")
    if float(row["minimum_mll_buffer"]) > float(row["mll_buffer"]) + 1e-12:
        raise EvidenceContractError(
            "minimum_mll_buffer may not exceed the closing mll_buffer"
        )
    _require_boolean(row, "consistency_ok")
    if not isinstance(row["conflicts"], (list, dict)):
        raise EvidenceContractError("conflicts must be a list or object")
    if not isinstance(row["exposure"], Mapping):
        raise EvidenceContractError("exposure must be an object")
    for name, value in row["exposure"].items():
        if not isinstance(name, str) or not name or not _is_number(value):
            raise EvidenceContractError(
                "exposure must map non-empty names to finite numbers"
            )
    if not isinstance(row["component_attribution"], Mapping):
        raise EvidenceContractError("component_attribution must be an object")
    for component_id, value in row["component_attribution"].items():
        if not isinstance(component_id, str) or not component_id or not _is_number(value):
            raise EvidenceContractError(
                "component_attribution must map component IDs to finite numbers"
            )
    if row["cost_scenario"] not in COST_SCENARIOS:
        raise EvidenceContractError(
            "cost_scenario must be NORMAL or STRESSED_1_5X"
        )


_TERMINAL_STATES = {
    "TARGET_REACHED",
    "MLL_BREACHED",
    "HARD_RULE_FAILURE",
    "DATA_CENSORED",
    "OPERATIONAL_HORIZON_NOT_REACHED",
}


def _validate_episode(row: Mapping[str, Any]) -> None:
    for field in (
        "campaign_id",
        "policy_id",
        "episode_id",
        "horizon",
        "temporal_block",
        "cost_scenario",
        "terminal_state",
    ):
        _require_nonempty_string(row, field)
    _require_timestamp(row, "episode_start")
    if not isinstance(row["duration_trading_days"], int) or isinstance(row["duration_trading_days"], bool) or row["duration_trading_days"] < 0:
        raise EvidenceContractError("duration_trading_days must be a non-negative integer")
    _require_boolean(row, "target_reached")
    _require_boolean(row, "mll_breached")
    _require_boolean(row, "censored_state")
    if row["days_to_target"] is not None:
        _require_number(row, "days_to_target")
    for field in ("costs", "net_pnl", "target_progress", "minimum_mll_buffer"):
        _require_number(row, field)
    if float(row["costs"]) < 0.0:
        raise EvidenceContractError("costs may not be negative")
    _require_boolean(row, "consistency_ok")
    if row["terminal_state"] not in _TERMINAL_STATES:
        raise EvidenceContractError(
            "terminal_state must use the frozen Combine classification"
        )
    if not isinstance(row["failure_vector"], (list, dict)):
        raise EvidenceContractError("failure_vector must be a list or object")
    if row["cost_scenario"] not in COST_SCENARIOS:
        raise EvidenceContractError(
            "cost_scenario must be NORMAL or STRESSED_1_5X"
        )
    terminal_state = str(row["terminal_state"])
    if bool(row["target_reached"]) != (terminal_state == "TARGET_REACHED"):
        raise EvidenceContractError("target_reached conflicts with terminal_state")
    if bool(row["mll_breached"]) != (terminal_state == "MLL_BREACHED"):
        raise EvidenceContractError("mll_breached conflicts with terminal_state")
    expected_censored = terminal_state in {
        "DATA_CENSORED",
        "OPERATIONAL_HORIZON_NOT_REACHED",
    }
    if bool(row["censored_state"]) != expected_censored:
        raise EvidenceContractError("censored_state conflicts with terminal_state")


def _validate_provenance(row: Mapping[str, Any]) -> None:
    for field in (
        "campaign_id",
        "validator_version",
        "replay_version",
        "market_data_role",
    ):
        _require_nonempty_string(row, field)
    if not isinstance(row["access_ledger_sha256"], str) or not _SHA256.fullmatch(row["access_ledger_sha256"]):
        raise EvidenceContractError("access_ledger_sha256 must be a lowercase SHA-256")
    _require_boolean(row, "reconstruction_flag")
    _require_timestamp(row, "recorded_at_utc")
    checksums = row["immutable_checksums"]
    if not isinstance(checksums, Mapping) or not checksums:
        raise EvidenceContractError("immutable_checksums must be a non-empty object")
    for name, digest in checksums.items():
        if not isinstance(name, str) or not name or not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise EvidenceContractError("immutable_checksums must map names to lowercase SHA-256 values")


@dataclass(frozen=True)
class RecordSpec:
    name: str
    required_fields: tuple[str, ...]
    sort_fields: tuple[str, ...]
    validator: Callable[[Mapping[str, Any]], None]

    def validate(self, row: Mapping[str, Any], *, campaign_id: str) -> dict[str, Any]:
        if not isinstance(row, Mapping):
            raise EvidenceContractError(f"{self.name} records must be objects")
        missing = [field for field in self.required_fields if field not in row]
        if missing:
            raise EvidenceContractError(
                f"{self.name} record missing required fields: {', '.join(missing)}"
            )
        materialized = dict(row)
        if materialized.get("campaign_id") != campaign_id:
            raise EvidenceContractError(
                f"{self.name} campaign_id does not match bundle identity"
            )
        try:
            json.dumps(materialized, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise EvidenceContractError(
                f"{self.name} record is not canonical JSON: {exc}"
            ) from exc
        self.validator(materialized)
        return materialized


RECORD_SPECS: dict[str, RecordSpec] = {
    "component_signals": RecordSpec(
        "component_signals",
        (
            "campaign_id", "component_id", "signal_id", "event_time", "market",
            "contract", "timeframe", "signal", "sizing", "stop", "target",
            "veto", "component_role",
        ),
        ("component_id", "event_time", "signal_id"),
        _validate_signal,
    ),
    "component_entries": RecordSpec(
        "component_entries",
        (
            "campaign_id", "component_id", "trade_id", "entry_time", "market",
            "contract", "side", "quantity", "entry_price", "sizing",
            "stop_price", "target_price",
        ),
        ("component_id", "entry_time", "trade_id"),
        _validate_entry,
    ),
    "component_exits": RecordSpec(
        "component_exits",
        ("campaign_id", "component_id", "trade_id", "exit_time", "exit_price", "exit_reason"),
        ("component_id", "exit_time", "trade_id"),
        _validate_exit,
    ),
    "component_trades": RecordSpec(
        "component_trades",
        (
            "campaign_id", "component_id", "trade_id", "entry_time", "exit_time",
            "market", "contract", "side", "quantity", "entry_price", "exit_price",
            "gross_pnl", "costs", "net_pnl",
        ),
        ("component_id", "entry_time", "exit_time", "trade_id"),
        _validate_trade,
    ),
    "account_policy_membership": RecordSpec(
        "account_policy_membership",
        ("campaign_id", "policy_id", "component_id", "risk_allocation", "component_role"),
        ("policy_id", "component_id"),
        _validate_membership,
    ),
    "account_daily_paths": RecordSpec(
        "account_daily_paths",
        (
            "campaign_id", "policy_id", "episode_id", "trading_day", "cost_scenario",
            "horizon",
            "realized_pnl", "unrealized_pnl", "daily_pnl", "equity", "mll",
            "mll_buffer", "minimum_mll_buffer", "consistency", "target_progress", "costs", "conflicts",
            "consistency_ok", "exposure", "component_attribution",
        ),
        ("policy_id", "episode_id", "horizon", "cost_scenario", "trading_day"),
        _validate_account_path,
    ),
    "episodes": RecordSpec(
        "episodes",
        (
            "campaign_id", "policy_id", "episode_id", "episode_start", "horizon", "temporal_block",
            "duration_trading_days", "target_reached", "mll_breached", "censored_state",
            "cost_scenario", "costs", "net_pnl", "target_progress",
            "minimum_mll_buffer", "consistency_ok", "days_to_target",
            "failure_vector", "terminal_state",
        ),
        ("policy_id", "episode_id", "horizon", "cost_scenario"),
        _validate_episode,
    ),
    "provenance": RecordSpec(
        "provenance",
        (
            "campaign_id", "validator_version", "replay_version", "market_data_role",
            "access_ledger_sha256", "reconstruction_flag", "immutable_checksums",
            "recorded_at_utc",
        ),
        ("recorded_at_utc", "validator_version", "replay_version"),
        _validate_provenance,
    ),
}


REQUIRED_DATASETS = tuple(RECORD_SPECS)
REQUIRED_COMPACT_OUTPUTS = (
    "campaign_summary",
    "failure_vectors",
    "pareto_archive",
    "next_campaign_recommendations",
)


def validate_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceContractError("identity must be an object")
    required = (
        "campaign_id",
        "grammar_id",
        "policy_fingerprints",
        "component_fingerprints",
        "source_commit",
        "data_fingerprints",
        "configuration_sha256",
        "seeds",
        "created_at_utc",
        "expected_coverage",
    )
    missing = [field for field in required if field not in value]
    if missing:
        raise EvidenceContractError(
            f"identity missing required fields: {', '.join(missing)}"
        )
    identity = dict(value)
    for field in ("campaign_id", "grammar_id"):
        _require_nonempty_string(identity, field)
    if not _SAFE_ID.fullmatch(identity["campaign_id"]):
        raise EvidenceContractError("campaign_id is unsafe for immutable storage")
    if not isinstance(identity["source_commit"], str) or not _GIT_COMMIT.fullmatch(identity["source_commit"]):
        raise EvidenceContractError("source_commit must be a full Git object ID")
    if not isinstance(identity["configuration_sha256"], str) or not _SHA256.fullmatch(identity["configuration_sha256"]):
        raise EvidenceContractError("configuration_sha256 must be a lowercase SHA-256")
    for field in ("policy_fingerprints", "component_fingerprints", "data_fingerprints"):
        mapping = identity[field]
        if not isinstance(mapping, Mapping) or not mapping:
            raise EvidenceContractError(f"{field} must be a non-empty object")
        for name, digest in mapping.items():
            if not isinstance(name, str) or not name:
                raise EvidenceContractError(f"{field} keys must be non-empty strings")
            if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
                raise EvidenceContractError(f"{field} values must be lowercase SHA-256 digests")
    seeds = identity["seeds"]
    if not isinstance(seeds, Sequence) or isinstance(seeds, (str, bytes)) or not seeds:
        raise EvidenceContractError("seeds must be a non-empty sequence")
    if any(not isinstance(seed, int) or isinstance(seed, bool) for seed in seeds):
        raise EvidenceContractError("seeds must contain integers")
    _require_timestamp(identity, "created_at_utc")
    _validate_expected_coverage(identity)
    try:
        json.dumps(identity, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise EvidenceContractError(f"identity is not canonical JSON: {exc}") from exc
    return identity


def _validate_expected_coverage(identity: Mapping[str, Any]) -> None:
    coverage = identity["expected_coverage"]
    if not isinstance(coverage, Mapping):
        raise EvidenceContractError("expected_coverage must be an object")
    required_fields = {
        "policy_ids",
        "component_ids",
        "required_episode_keys",
        "allowed_horizons",
        "cost_scenarios",
        "allow_additional_episode_keys",
    }
    if set(coverage) != required_fields:
        missing = sorted(required_fields - set(coverage))
        extra = sorted(set(coverage) - required_fields)
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if extra:
            detail.append("unknown " + ", ".join(extra))
        raise EvidenceContractError(
            "expected_coverage fields are invalid: " + "; ".join(detail)
        )

    policy_ids = coverage["policy_ids"]
    component_ids = coverage["component_ids"]
    for field, values, fingerprints in (
        ("policy_ids", policy_ids, identity["policy_fingerprints"]),
        ("component_ids", component_ids, identity["component_fingerprints"]),
    ):
        if (
            not isinstance(values, Sequence)
            or isinstance(values, (str, bytes))
            or not values
            or any(not isinstance(item, str) or not item for item in values)
        ):
            raise EvidenceContractError(
                f"expected_coverage.{field} must be a non-empty string sequence"
            )
        if len(set(values)) != len(values):
            raise EvidenceContractError(f"expected_coverage.{field} contains duplicates")
        if set(values) != set(fingerprints):
            raise EvidenceContractError(
                f"expected_coverage.{field} must exactly match immutable fingerprints"
            )

    horizons = coverage["allowed_horizons"]
    if (
        not isinstance(horizons, Sequence)
        or isinstance(horizons, (str, bytes))
        or not horizons
        or any(not isinstance(item, str) or not item.strip() for item in horizons)
        or len(set(horizons)) != len(horizons)
    ):
        raise EvidenceContractError(
            "expected_coverage.allowed_horizons must be unique non-empty strings"
        )
    scenarios = coverage["cost_scenarios"]
    if (
        not isinstance(scenarios, Sequence)
        or isinstance(scenarios, (str, bytes))
        or tuple(scenarios) != COST_SCENARIOS
    ):
        raise EvidenceContractError(
            "expected_coverage.cost_scenarios must be exactly "
            "['NORMAL', 'STRESSED_1_5X']"
        )
    if not isinstance(coverage["allow_additional_episode_keys"], bool):
        raise EvidenceContractError(
            "expected_coverage.allow_additional_episode_keys must be a boolean"
        )

    required_episode_keys = coverage["required_episode_keys"]
    if (
        not isinstance(required_episode_keys, Sequence)
        or isinstance(required_episode_keys, (str, bytes))
        or not required_episode_keys
    ):
        raise EvidenceContractError(
            "expected_coverage.required_episode_keys must be a non-empty sequence"
        )
    observed: set[tuple[str, str, str]] = set()
    covered_policies: set[str] = set()
    for raw_key in required_episode_keys:
        if not isinstance(raw_key, Mapping) or set(raw_key) != {
            "policy_id",
            "episode_id",
            "horizon",
        }:
            raise EvidenceContractError(
                "required_episode_keys entries must contain only "
                "policy_id, episode_id, and horizon"
            )
        for field in ("policy_id", "episode_id", "horizon"):
            if not isinstance(raw_key[field], str) or not raw_key[field].strip():
                raise EvidenceContractError(
                    f"required_episode_keys.{field} must be a non-empty string"
                )
        key = (
            str(raw_key["policy_id"]),
            str(raw_key["episode_id"]),
            str(raw_key["horizon"]),
        )
        if key in observed:
            raise EvidenceContractError("required_episode_keys contains duplicates")
        if key[0] not in set(policy_ids):
            raise EvidenceContractError(
                "required_episode_keys references an unknown policy"
            )
        if key[2] not in set(horizons):
            raise EvidenceContractError(
                "required_episode_keys references a disallowed horizon"
            )
        observed.add(key)
        covered_policies.add(key[0])
    if covered_policies != set(policy_ids):
        raise EvidenceContractError(
            "required_episode_keys must provide base coverage for every policy"
        )


def validate_compact_output(name: str, value: Any) -> Any:
    if name not in REQUIRED_COMPACT_OUTPUTS:
        raise EvidenceContractError(f"unknown compact output: {name}")
    if value is None:
        raise EvidenceContractError(f"compact output {name} may not be null")
    try:
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise EvidenceContractError(
            f"compact output {name} is not canonical JSON: {exc}"
        ) from exc
    return value


__all__ = [
    "EVIDENCE_BUNDLE_CONTRACT",
    "EVIDENCE_BUNDLE_SCHEMA_VERSION",
    "COST_SCENARIOS",
    "EVIDENCE_STATUSES",
    "EvidenceContractError",
    "PATH_METRIC_ABS_TOLERANCE",
    "PNL_ABS_TOLERANCE",
    "RECORD_SPECS",
    "REQUIRED_COMPACT_OUTPUTS",
    "REQUIRED_DATASETS",
    "RecordSpec",
    "validate_compact_output",
    "validate_identity",
]
