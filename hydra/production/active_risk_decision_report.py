"""Streaming, report-only decision audit for active-risk campaign 0026.

The production runtime deliberately persists outcome-rich Stage-3/4/5 batches.
A single batch can be large because it contains exact daily account paths and
XFA ledgers.  This module consumes and releases one batch at a time, reproduces
its sealed EvidenceBundle partitions, and builds a compact decision report
without changing selection, promotion, manifests, or authoritative campaign
evidence.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
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
    ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
    ActiveRiskRuntimeError,
    _active_pool_lifecycle_evidence,
    _exposure_signature,
    _failure_vectors,
    _suppression_summary,
    _utilisation_summary,
)
from hydra.production.active_risk_report_seal import (
    REPORT_JSON_NAME,
    REPORT_MARKDOWN_NAME,
    seal_active_risk_decision_report,
    verify_active_risk_decision_report_seal,
)
from hydra.production.episode_evidence import _convert_episode
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15
from hydra.propfirm.xfa_payout_events import (
    CANONICAL_PAYOUT_EVENT_SCHEMA,
    PayoutPathReconciliation,
    XfaPayoutEventError,
    reconcile_payout_path,
)


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
    metric: Mapping[str, Any],
    manifest: Mapping[str, Any],
    *,
    stage_label: str = "Stage-3",
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
                f"{stage_label} lifecycle projection contains a malformed row"
            )
        key = (str(lifecycle["scenario"]), int(lifecycle["combine_start_day"]))
        if key in lifecycle_by_key:
            raise ActiveRiskDecisionReportError(
                f"duplicate {stage_label} lifecycle projection key {key}"
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
                f"{stage_label} cache cannot reproduce sealed evidence: {exc}"
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
            f"orphan {stage_label} lifecycle projections remain after evidence replay"
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


def _stage_partition_index(
    bundle_manifest: Mapping[str, Any], *, stage: str, expected_policy_count: int
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
            f"active:{stage}:"
        ):
            continue
        key = (dataset, batch_id)
        if key in observed:
            raise ActiveRiskDecisionReportError(
                f"duplicate {stage} EvidenceBundle partition declaration {key}"
            )
        observed[key] = details
    expected = {
        (
            dataset,
            f"active:{stage}:{index:06d}:{suffix}",
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
            f"{stage} EvidenceBundle partition coverage drift: "
            f"missing={missing[:3]!r}, extra={extra[:3]!r}"
        )
    return observed


def _stage3_partition_index(
    bundle_manifest: Mapping[str, Any], *, expected_policy_count: int
) -> dict[tuple[str, str], Mapping[str, Any]]:
    return _stage_partition_index(
        bundle_manifest,
        stage="stage3",
        expected_policy_count=expected_policy_count,
    )


def _episode_dataset_accounting(
    bundle_manifest: Mapping[str, Any], *, canonical_attempt_count: int
) -> dict[str, Any]:
    """Reconcile account attempts with persisted multi-horizon episode rows.

    ``combine_episodes_completed`` is a replay-work counter: one account start,
    cost scenario and policy is one attempt.  The EvidenceBundle ``episodes``
    dataset is a reporting table and persists one row per frozen horizon.  The
    two quantities are deliberately different for Stage 3 onward.
    """

    files = bundle_manifest.get("files")
    row_counts = bundle_manifest.get("dataset_row_counts")
    if not isinstance(files, Mapping) or not isinstance(row_counts, Mapping):
        raise ActiveRiskDecisionReportError(
            "verified EvidenceBundle lacks episode partition accounting"
        )
    persisted_row_count = row_counts.get("episodes")
    if (
        isinstance(persisted_row_count, bool)
        or not isinstance(persisted_row_count, int)
        or persisted_row_count < 0
    ):
        raise ActiveRiskDecisionReportError(
            "verified EvidenceBundle episode row count is invalid"
        )

    horizon_multiplicity = {
        "stage2-eliminated": 1,
        "stage3": len(HORIZONS),
        "stage4": len(HORIZONS),
        "stage5": len(HORIZONS),
    }
    stage_rows: Counter[str] = Counter()
    stage_parts: Counter[str] = Counter()
    unsupported: list[str] = []
    for details in files.values():
        if (
            not isinstance(details, Mapping)
            or details.get("kind") != "dataset_partition"
            or details.get("dataset") != "episodes"
        ):
            continue
        batch_id = str(details.get("batch_id") or "")
        pieces = batch_id.split(":")
        if len(pieces) != 4 or pieces[0] != "active" or pieces[3] != "episodes":
            unsupported.append(batch_id)
            continue
        stage = pieces[1]
        if stage not in horizon_multiplicity:
            unsupported.append(batch_id)
            continue
        try:
            row_count = int(details["row_count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ActiveRiskDecisionReportError(
                f"EvidenceBundle episode partition {batch_id!r} has invalid row count"
            ) from exc
        if row_count < 0:
            raise ActiveRiskDecisionReportError(
                f"EvidenceBundle episode partition {batch_id!r} has negative rows"
            )
        stage_rows[stage] += row_count
        stage_parts[stage] += 1
    if unsupported:
        raise ActiveRiskDecisionReportError(
            "EvidenceBundle has unsupported episode partitions: "
            + repr(sorted(unsupported)[:3])
        )
    if sum(stage_rows.values()) != persisted_row_count:
        raise ActiveRiskDecisionReportError(
            "EvidenceBundle episode partitions do not sum to dataset_row_counts"
        )

    per_stage: dict[str, Any] = {}
    derived_attempt_count = 0
    for stage in horizon_multiplicity:
        rows = int(stage_rows.get(stage, 0))
        multiplicity = int(horizon_multiplicity[stage])
        if rows % multiplicity:
            raise ActiveRiskDecisionReportError(
                f"EvidenceBundle {stage} episode rows are not divisible by the "
                "frozen horizon multiplicity"
            )
        attempts = rows // multiplicity
        derived_attempt_count += attempts
        per_stage[stage] = {
            "partition_count": int(stage_parts.get(stage, 0)),
            "persisted_episode_rows": rows,
            "frozen_horizon_multiplicity": multiplicity,
            "derived_canonical_account_attempts": attempts,
        }
    if derived_attempt_count != canonical_attempt_count:
        raise ActiveRiskDecisionReportError(
            "economic final result canonical attempt counter diverges from the "
            "EvidenceBundle multi-horizon partition formula"
        )
    return {
        "canonical_account_episode_attempts": canonical_attempt_count,
        "persisted_multi_horizon_episode_rows": persisted_row_count,
        "formula_valid": True,
        "per_stage": per_stage,
        "semantics": (
            "CANONICAL_ATTEMPTS_COUNT_POLICY_START_SCENARIO_ONCE;PERSISTED_"
            "EPISODE_ROWS_COUNT_EACH_FROZEN_HORIZON"
        ),
    }


def _sealed_component_trade_evidence(
    bundle_path: Path, bundle_manifest: Mapping[str, Any]
) -> tuple[dict[str, str], dict[tuple[str, str], dict[str, Any]]]:
    """Index the small, already deep-verified immutable component ledger."""

    files = bundle_manifest.get("files")
    if not isinstance(files, Mapping):
        raise ActiveRiskDecisionReportError(
            "verified EvidenceBundle lacks component-trade file provenance"
        )
    markets: dict[str, set[str]] = defaultdict(set)
    trades: dict[tuple[str, str], dict[str, Any]] = {}
    partition_count = 0
    for relative_path, details in files.items():
        if (
            not isinstance(details, Mapping)
            or details.get("kind") != "dataset_partition"
            or details.get("dataset") != "component_trades"
        ):
            continue
        partition_count += 1
        path = (bundle_path / str(relative_path)).resolve()
        if bundle_path not in path.parents:
            raise ActiveRiskDecisionReportError(
                "component-trade partition escaped the EvidenceBundle"
            )
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                header = json.loads(handle.readline())
                envelope = header.get("_evidence_part")
                if (
                    not isinstance(envelope, Mapping)
                    or envelope.get("dataset") != "component_trades"
                ):
                    raise ActiveRiskDecisionReportError(
                        "component-trade partition header drift"
                    )
                observed_rows = 0
                for line in handle:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    component_id = str(row.get("component_id") or "")
                    trade_id = str(row.get("trade_id") or "")
                    market = str(row.get("market") or "")
                    if not component_id or not trade_id or not market:
                        raise ActiveRiskDecisionReportError(
                            "component-trade identity/market attribution is incomplete"
                        )
                    key = (component_id, trade_id)
                    if key in trades:
                        raise ActiveRiskDecisionReportError(
                            f"duplicate sealed component trade {key}"
                        )
                    quantity = row.get("quantity")
                    if (
                        isinstance(quantity, bool)
                        or not isinstance(quantity, int)
                        or quantity <= 0
                    ):
                        raise ActiveRiskDecisionReportError(
                            f"sealed component trade {key} has invalid quantity"
                        )
                    gross = _required_float(
                        row.get("gross_pnl"), label=f"component trade {key} gross"
                    )
                    costs = _required_float(
                        row.get("costs"), label=f"component trade {key} costs"
                    )
                    net = _required_float(
                        row.get("net_pnl"), label=f"component trade {key} net"
                    )
                    if costs < 0.0:
                        raise ActiveRiskDecisionReportError(
                            f"sealed component trade {key} has negative costs"
                        )
                    _assert_close(
                        gross - costs,
                        net,
                        label=f"component trade {key} net/cost identity",
                    )
                    trades[key] = {
                        "component_id": component_id,
                        "trade_id": trade_id,
                        "market": market,
                        "quantity": int(quantity),
                        "gross_pnl": gross,
                        "costs": costs,
                        "net_pnl": net,
                    }
                    markets[component_id].add(market)
                    observed_rows += 1
                if observed_rows != int(envelope.get("row_count", -1)):
                    raise ActiveRiskDecisionReportError(
                        "component-trade partition row count drift"
                    )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ActiveRiskDecisionReportError(
                f"cannot stream sealed component-trade market attribution: {exc}"
            ) from exc
    if partition_count == 0 or not markets or not trades:
        raise ActiveRiskDecisionReportError(
            "sealed EvidenceBundle has no component-trade market attribution"
        )
    ambiguous = {
        component_id: sorted(values)
        for component_id, values in markets.items()
        if len(values) != 1
    }
    if ambiguous:
        raise ActiveRiskDecisionReportError(
            "component-to-market attribution is not one-to-one: "
            + repr(dict(list(sorted(ambiguous.items()))[:3]))
        )
    market_map = {
        component_id: next(iter(values))
        for component_id, values in sorted(markets.items())
    }
    return market_map, trades


def _sealed_component_market_map(
    bundle_path: Path, bundle_manifest: Mapping[str, Any]
) -> dict[str, str]:
    """Compatibility view for callers that need only sleeve-to-market identity."""

    markets, _trades = _sealed_component_trade_evidence(
        bundle_path, bundle_manifest
    )
    return markets


def _sealed_finalist_policy_freeze(
    bundle_path: Path,
    bundle_manifest: Mapping[str, Any],
    bundle_identity: Mapping[str, Any],
    campaign_manifest: Mapping[str, Any],
    finalist_ids: set[str],
) -> dict[str, Any]:
    """Prove finalist governor and sleeve membership from sealed rows."""

    sleeve_bank = campaign_manifest.get("sleeve_bank")
    if not isinstance(sleeve_bank, Mapping):
        raise ActiveRiskDecisionReportError("manifest sleeve bank is absent")
    manifest_members = sleeve_bank.get("members")
    if not isinstance(manifest_members, list):
        raise ActiveRiskDecisionReportError("manifest sleeve members are absent")
    member_count = int(sleeve_bank.get("member_count", -1))
    manifest_by_component: dict[str, Mapping[str, Any]] = {}
    for member in manifest_members:
        if not isinstance(member, Mapping):
            raise ActiveRiskDecisionReportError("manifest sleeve member is malformed")
        component_id = str(member.get("sleeve_id") or "")
        if not component_id or component_id in manifest_by_component:
            raise ActiveRiskDecisionReportError(
                "manifest sleeve identity is empty or duplicated"
            )
        manifest_by_component[component_id] = member
    if member_count != len(manifest_by_component):
        raise ActiveRiskDecisionReportError("manifest sleeve count drift")
    component_fingerprints = bundle_identity.get("component_fingerprints")
    policy_fingerprints = bundle_identity.get("policy_fingerprints")
    if not isinstance(component_fingerprints, Mapping) or not isinstance(
        policy_fingerprints, Mapping
    ):
        raise ActiveRiskDecisionReportError(
            "EvidenceBundle identity lacks policy/component fingerprints"
        )
    for component_id, member in manifest_by_component.items():
        if str(component_fingerprints.get(component_id) or "") != str(
            member.get("immutable_fingerprint") or ""
        ):
            raise ActiveRiskDecisionReportError(
                f"sealed component fingerprint drift for {component_id}"
            )

    files = bundle_manifest.get("files")
    if not isinstance(files, Mapping):
        raise ActiveRiskDecisionReportError(
            "verified EvidenceBundle lacks membership partitions"
        )
    rows_by_policy: dict[str, list[dict[str, Any]]] = {
        policy_id: [] for policy_id in finalist_ids
    }
    partition_count = 0
    for relative_path, details in sorted(files.items()):
        if (
            not isinstance(details, Mapping)
            or details.get("kind") != "dataset_partition"
            or details.get("dataset") != "account_policy_membership"
        ):
            continue
        partition_count += 1
        path = (bundle_path / str(relative_path)).resolve()
        if bundle_path not in path.parents:
            raise ActiveRiskDecisionReportError(
                "membership partition escaped the EvidenceBundle"
            )
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                envelope = json.loads(handle.readline()).get("_evidence_part")
                if (
                    not isinstance(envelope, Mapping)
                    or envelope.get("dataset") != "account_policy_membership"
                ):
                    raise ActiveRiskDecisionReportError(
                        "membership partition header drift"
                    )
                observed_rows = 0
                for line in handle:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    observed_rows += 1
                    policy_id = str(row.get("policy_id") or "")
                    if policy_id in rows_by_policy:
                        rows_by_policy[policy_id].append(row)
                if observed_rows != int(envelope.get("row_count", -1)):
                    raise ActiveRiskDecisionReportError(
                        "membership partition row-count drift"
                    )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ActiveRiskDecisionReportError(
                f"cannot stream sealed finalist membership: {exc}"
            ) from exc
    if partition_count == 0:
        raise ActiveRiskDecisionReportError(
            "EvidenceBundle has no account-policy membership partitions"
        )

    policy_specs: list[dict[str, Any]] = []
    for policy_id in sorted(finalist_ids):
        membership = rows_by_policy[policy_id]
        if len(membership) != member_count:
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} has {len(membership)} sealed sleeve rows, "
                f"expected {member_count}"
            )
        component_ids = [str(row.get("component_id") or "") for row in membership]
        if len(component_ids) != len(set(component_ids)) or set(component_ids) != set(
            manifest_by_component
        ):
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} sealed sleeve membership drift"
            )
        policies = [row.get("active_risk_policy") for row in membership]
        if not all(isinstance(value, Mapping) for value in policies):
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} lacks full active-risk policy rows"
            )
        policy_hashes = {canonical_hash(value) for value in policies}
        if len(policy_hashes) != 1:
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} active-risk policy differs across sleeves"
            )
        policy = dict(policies[0])
        priority = list(policy.get("component_priority") or ())
        risk_charges = policy.get("nominal_risk_charge_per_mini")
        if (
            policy.get("schema") != "hydra_active_risk_pool_policy_v1"
            or policy.get("policy_version")
            != "hydra_active_risk_pool_governor_v1"
            or str(policy.get("policy_id") or "") != policy_id
            or list(dict.fromkeys(priority)) != priority
            or set(priority) != set(component_ids)
            or not isinstance(risk_charges, Mapping)
            or set(risk_charges) != set(component_ids)
            or policy.get("future_outcome_fields_used") is not False
            or policy.get("outbound_order_capability") is not False
            or not str(policy.get("same_instrument_conflict_rule") or "")
        ):
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} frozen governor contract drift"
            )
        structural = str(policy.get("structural_fingerprint") or "")
        if structural != str(policy_fingerprints.get(policy_id) or ""):
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} sealed structural fingerprint drift"
            )
        compact_membership: list[dict[str, Any]] = []
        for row in sorted(membership, key=lambda value: str(value["component_id"])):
            component_id = str(row["component_id"])
            manifest_member = manifest_by_component[component_id]
            if (
                str(row.get("component_role") or "")
                != str(manifest_member.get("role") or "")
                or row.get("inactive_sleeve_reserves_risk") is not False
                or row.get("underlying_signal_mutated") is not False
            ):
                raise ActiveRiskDecisionReportError(
                    f"finalist {policy_id} component {component_id} freeze drift"
                )
            sleeve_specification = manifest_member.get("sleeve_specification")
            if (
                not isinstance(sleeve_specification, Mapping)
                or str(sleeve_specification.get("sleeve_id") or "")
                != component_id
                or sleeve_specification.get("version") is None
                or any(
                    not str(manifest_member.get(field_name) or "")
                    for field_name in (
                        "behavioral_fingerprint",
                        "signal_ledger_sha256",
                        "trade_ledger_sha256",
                        "market",
                        "contract",
                        "timeframe",
                        "session",
                        "source_campaign",
                    )
                )
            ):
                raise ActiveRiskDecisionReportError(
                    f"finalist {policy_id} component {component_id} versioned "
                    "sleeve specification is incomplete"
                )
            compact_membership.append(
                {
                    "component_id": component_id,
                    "component_role": row["component_role"],
                    "risk_allocation": _required_float(
                        row.get("risk_allocation"),
                        label=f"finalist {policy_id} {component_id} risk allocation",
                    ),
                    "immutable_fingerprint": component_fingerprints[component_id],
                    "behavioral_fingerprint": manifest_member.get(
                        "behavioral_fingerprint"
                    ),
                    "signal_ledger_sha256": manifest_member.get(
                        "signal_ledger_sha256"
                    ),
                    "trade_ledger_sha256": manifest_member.get(
                        "trade_ledger_sha256"
                    ),
                    "market": manifest_member.get("market"),
                    "contract": manifest_member.get("contract"),
                    "timeframe": manifest_member.get("timeframe"),
                    "session": manifest_member.get("session"),
                    "source_campaign": manifest_member.get("source_campaign"),
                    "sleeve_specification": dict(sleeve_specification),
                }
            )
        policy_specs.append(
            {
                "policy_id": policy_id,
                "structural_fingerprint": structural,
                "membership_row_count": len(membership),
                "membership_rows_all_contain_identical_policy": True,
                "active_risk_policy": policy,
                "active_risk_policy_sha256": next(iter(policy_hashes)),
                "membership": compact_membership,
                "membership_sha256": canonical_hash(compact_membership),
            }
        )

    frozen_horizons = (
        campaign_manifest.get("successive_halving") or {}
    ).get("frozen_horizons")
    expected_horizons = [20, 40, 60, 90, "FULL"]
    costs = campaign_manifest.get("costs")
    account = campaign_manifest.get("account_parameters")
    lifecycle = campaign_manifest.get("lifecycle")
    xfa_projection = (campaign_manifest.get("successive_halving") or {}).get(
        "xfa_profile_projection"
    )
    if (
        frozen_horizons != expected_horizons
        or not isinstance(costs, Mapping)
        or _required_float(costs.get("normal_multiplier"), label="normal costs")
        != 1.0
        or _required_float(
            costs.get("stressed_multiplier"), label="stressed costs"
        )
        != 1.5
        or not isinstance(account, Mapping)
        or _required_float(
            account.get("starting_balance"), label="Combine starting balance"
        )
        != 150_000.0
        or _required_float(
            account.get("profit_target"), label="Combine profit target"
        )
        != 9_000.0
        or _required_float(
            account.get("maximum_loss_limit"), label="Combine MLL"
        )
        != 4_500.0
        or int(account.get("maximum_mini_equivalent", -1)) != 15
        or not isinstance(lifecycle, Mapping)
        or dict(lifecycle.get("rule_snapshot") or {})
        != official_rule_snapshot_2026_07_15().to_dict()
        or lifecycle.get("standard_and_consistency_both_evaluated") is not True
        or lifecycle.get("books_frozen_before_outcomes") is not True
        or not isinstance(xfa_projection, Mapping)
        or xfa_projection.get("profile_version") != "hydra_combine_to_xfa_v1"
        or xfa_projection.get("clip_to_official_scaling_plan") is not True
        or xfa_projection.get("same_market_exclusive") is not True
        or xfa_projection.get("active_pool_combine_only_controls_applied") is not False
        or xfa_projection.get("selected_after_combine_outcome") is not False
    ):
        raise ActiveRiskDecisionReportError(
            "frozen horizon/cost/account/XFA manifest contract drift"
        )
    contract = {
        "manifest_hash": campaign_manifest["manifest_hash"],
        "source_commit": campaign_manifest["source_commit"],
        "component_bank_member_count": member_count,
        "frozen_horizons": frozen_horizons,
        "costs": dict(costs),
        "account_parameters": dict(account),
        "xfa_rule_snapshot": dict(lifecycle["rule_snapshot"]),
        "xfa_profile_projection": dict(xfa_projection),
        "standard_and_consistency_both_evaluated": True,
        "active_risk_xfa_overlay_semantics": (
            ACTIVE_RISK_XFA_OVERLAY_SEMANTICS
        ),
    }
    for policy_spec in policy_specs:
        policy = policy_spec["active_risk_policy"]
        policy_id = str(policy_spec["policy_id"])
        risk_multiplier = _required_float(
            policy.get("static_risk_tier"),
            label=f"finalist {policy_id} XFA risk multiplier",
        )
        maximum_positions = int(policy.get("maximum_concurrent_sleeves", -1))
        maximum_mini = int(policy.get("maximum_mini_equivalent", -1))
        if (
            risk_multiplier <= 0.0
            or maximum_positions < 1
            or not 1 <= maximum_mini <= 15
        ):
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} cannot project a valid frozen XFA profile"
            )
        xfa_profile_payload = {
            "profile_id": f"{policy_id}:XFA_PROFILE",
            "risk_multiplier": risk_multiplier,
            "maximum_simultaneous_positions": maximum_positions,
            "maximum_mini_equivalent": maximum_mini,
            "clip_to_xfa_scaling_plan": True,
            "same_market_exclusive": True,
            "profile_version": str(xfa_projection["profile_version"]),
        }
        xfa_profile = {
            **xfa_profile_payload,
            "fingerprint": canonical_hash(xfa_profile_payload),
        }
        combine_book = {
            "book": "COMBINE_BOOK",
            "policy_id": policy_id,
            "active_risk_policy_sha256": policy_spec[
                "active_risk_policy_sha256"
            ],
            "membership_sha256": policy_spec["membership_sha256"],
            "account_parameters": dict(account),
            "costs": dict(costs),
            "frozen_horizons": list(frozen_horizons),
            "underlying_sleeve_logic_mutated": False,
        }
        xfa_common = {
            "policy_id": policy_id,
            "source_combine_book_sha256": canonical_hash(combine_book),
            "membership_sha256": policy_spec["membership_sha256"],
            "xfa_profile": xfa_profile,
            "rule_snapshot": dict(lifecycle["rule_snapshot"]),
            "overlay_semantics": ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
            "combine_profit_transferred_to_xfa": False,
            "combine_governor_controls_applied_in_xfa": False,
            "book_frozen_before_outcomes": True,
            "selected_after_combine_outcome": False,
        }
        standard_book = {"book": "XFA_STANDARD_BOOK", **xfa_common}
        consistency_book = {"book": "XFA_CONSISTENCY_BOOK", **xfa_common}
        policy_spec["combine_book"] = combine_book
        policy_spec["combine_book_sha256"] = canonical_hash(combine_book)
        policy_spec["xfa_standard_book"] = standard_book
        policy_spec["xfa_standard_book_sha256"] = canonical_hash(standard_book)
        policy_spec["xfa_consistency_book"] = consistency_book
        policy_spec["xfa_consistency_book_sha256"] = canonical_hash(
            consistency_book
        )
    return {
        "scope": "DEEP_VERIFIED_EVIDENCE_BUNDLE_FINALIST_POLICY_FREEZE",
        "finalist_count": len(policy_specs),
        "policy_specs": policy_specs,
        "campaign_contract": contract,
        "campaign_contract_sha256": canonical_hash(contract),
    }


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


def _nested_evidence_equal(left: Any, right: Any) -> bool:
    """Compare cached diagnostic trees with the frozen numeric tolerance."""

    if isinstance(left, Mapping) or isinstance(right, Mapping):
        return bool(
            isinstance(left, Mapping)
            and isinstance(right, Mapping)
            and set(left) == set(right)
            and all(
                _nested_evidence_equal(left[key], right[key]) for key in left
            )
        )
    if isinstance(left, (list, tuple)) or isinstance(right, (list, tuple)):
        return bool(
            isinstance(left, (list, tuple))
            and isinstance(right, (list, tuple))
            and len(left) == len(right)
            and all(
                _nested_evidence_equal(a, b)
                for a, b in zip(left, right, strict=True)
            )
        )
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        if isinstance(left, int) and isinstance(right, int):
            return left == right
        return bool(
            math.isfinite(float(left))
            and math.isfinite(float(right))
            and math.isclose(
                float(left),
                float(right),
                rel_tol=1e-10,
                abs_tol=1e-8,
            )
        )
    return left == right


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
    if set(ids) != {"B1", "B2", "B3", "B4"}:
        raise ActiveRiskDecisionReportError(
            "campaign 0026 must retain exactly the frozen B1-B4 source blocks"
        )
    if any(
        row.end < row.start
        or not row.markets
        or not str(row.contract_separation or "").strip()
        for row in output
    ):
        raise ActiveRiskDecisionReportError(
            "temporal block lacks dates, markets, or explicit contract separation"
        )
    chronological = sorted(output, key=lambda row: (row.start, row.end, row.block_id))
    if any(
        right.start <= left.end
        for left, right in zip(chronological, chronological[1:])
    ):
        raise ActiveRiskDecisionReportError("temporal source blocks overlap")
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
    maximum_target_progress: list[float] = field(default_factory=list)
    minimum_mll_buffer: list[float] = field(default_factory=list)
    net_pnl: list[float] = field(default_factory=list)
    days_to_target: list[float] = field(default_factory=list)
    duration_trading_days: list[float] = field(default_factory=list)
    active_trading_days: list[float] = field(default_factory=list)
    calendar_days: list[float] = field(default_factory=list)
    projected_active_days_to_target: list[float] = field(default_factory=list)
    projected_calendar_days_to_target: list[float] = field(default_factory=list)

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
        target_progress = _required_float(
            raw.get("target_progress"), label="episode target progress"
        )
        self.target_progress.append(target_progress)
        self.maximum_target_progress.append(
            _required_float(
                raw.get("maximum_target_progress"),
                label="episode maximum target progress",
            )
        )
        self.minimum_mll_buffer.append(
            _required_float(raw.get("minimum_mll_buffer"), label="episode MLL buffer")
        )
        self.net_pnl.append(_required_float(raw.get("net_pnl"), label="episode net PnL"))
        duration = _required_float(
            raw.get("eligible_days"), label="episode eligible trading days"
        )
        active_duration = _required_float(
            raw.get("traded_days"), label="episode active trading days"
        )
        calendar_duration = float(int(raw["end_day"]) - int(raw["start_day"]) + 1)
        self.duration_trading_days.append(duration)
        self.active_trading_days.append(active_duration)
        self.calendar_days.append(calendar_duration)
        if target_progress > 0.0:
            self.projected_active_days_to_target.append(
                active_duration / target_progress
            )
            self.projected_calendar_days_to_target.append(
                calendar_duration / target_progress
            )
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
            "consistency_ok_count": self.consistency_ok_count,
            "target_progress": _distribution(self.target_progress),
            "maximum_target_progress": _distribution(
                self.maximum_target_progress
            ),
            "minimum_mll_buffer": _distribution(self.minimum_mll_buffer),
            "net_pnl": _distribution(self.net_pnl),
            "days_to_target": _distribution(self.days_to_target),
            "duration_trading_days": _distribution(
                self.duration_trading_days
            ),
            "active_trading_days": _distribution(self.active_trading_days),
            "calendar_days": _distribution(self.calendar_days),
            "projected_active_days_to_target": _distribution(
                self.projected_active_days_to_target
            ),
            "projected_calendar_days_to_target": _distribution(
                self.projected_calendar_days_to_target
            ),
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

    def add_decision(self, decision: Mapping[str, Any]) -> None:
        status = str(decision.get("decision_status") or "UNKNOWN")
        accepted = status in {"ACCEPTED", "SIZE_REDUCED"}
        self.signals_emitted += 1
        self.signals_accepted += int(accepted)
        self.signals_rejected += int(not accepted)
        self.status_counts[status] += 1
        self.foregone_realized_pnl_ex_post += _float(
            decision.get("foregone_realized_pnl_ex_post")
        )

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


def _validate_xfa_payout_ledger(
    path: Mapping[str, Any], *, label: str
) -> None:
    """Re-execute payout eligibility, amount, and reset from daily evidence."""

    rules = official_rule_snapshot_2026_07_15()
    ledger = path.get("daily_ledger")
    if not isinstance(ledger, list):
        raise ActiveRiskDecisionReportError(f"{label} daily ledger is absent")
    path_name = str(path.get("path") or "")
    if path_name not in {"XFA_STANDARD", "XFA_CONSISTENCY"}:
        raise ActiveRiskDecisionReportError(f"{label} payout path identity drift")

    winning_days = 0
    total_qualifying_days = 0
    traded_days_cycle = 0
    total_profit_cycle = 0.0
    best_day_cycle = 0.0
    cycle_start_balance = rules.xfa_starting_balance
    cycles = 0
    gross_total = 0.0
    trader_total = 0.0
    maximum_consistency = 0.0
    first_payout_day: int | None = None
    prior_closing = rules.xfa_starting_balance
    prior_floor = rules.xfa_starting_floor
    path_terminal = str(path.get("terminal") or "")
    terminal_row_count = 0
    for elapsed, row in enumerate(ledger, start=1):
        if not isinstance(row, Mapping):
            raise ActiveRiskDecisionReportError(f"{label} malformed daily ledger row")
        opening = _required_float(
            row.get("opening_balance"), label=f"{label} opening balance"
        )
        closing = _required_float(
            row.get("closing_balance"), label=f"{label} closing balance"
        )
        _assert_close(opening, prior_closing, label=f"{label} balance continuity")
        floor_open = _required_float(
            row.get("mll_floor_open"), label=f"{label} opening MLL floor"
        )
        floor_close = _required_float(
            row.get("mll_floor_close"), label=f"{label} closing MLL floor"
        )
        _assert_close(floor_open, prior_floor, label=f"{label} MLL-floor continuity")
        day_pnl = _required_float(row.get("day_pnl"), label=f"{label} day PnL")
        terminal = row.get("terminal")
        if terminal:
            terminal_row_count += 1
            if elapsed != len(ledger) or str(terminal) != path_terminal:
                raise ActiveRiskDecisionReportError(
                    f"{label} terminal row/path chronology drift"
                )
            if bool(row.get("payout_requested")):
                raise ActiveRiskDecisionReportError(
                    f"{label} terminal row cannot request payout"
                )
            _assert_close(
                opening + day_pnl,
                closing,
                label=f"{label} terminal-row closing balance",
            )
            _assert_close(
                floor_close,
                floor_open,
                label=f"{label} terminal-row MLL floor",
            )
            if str(terminal) == "MLL_BREACHED":
                worst_intraday = row.get("worst_intraday_equity")
                breached = closing <= floor_open + 1e-12 or (
                    worst_intraday is not None
                    and _required_float(
                        worst_intraday,
                        label=f"{label} terminal worst intraday equity",
                    )
                    <= floor_open + 1e-12
                )
                if not breached:
                    raise ActiveRiskDecisionReportError(
                        f"{label} MLL terminal lacks a recorded floor breach"
                    )
            prior_closing = closing
            prior_floor = floor_close
            continue

        if bool(row.get("traded")):
            traded_days_cycle += 1
        if day_pnl >= rules.xfa_standard_winning_day_minimum:
            winning_days += 1
            total_qualifying_days += 1
        total_profit_cycle += day_pnl
        best_day_cycle = max(best_day_cycle, day_pnl)
        consistency_ratio = (
            best_day_cycle / total_profit_cycle
            if total_profit_cycle > 0.0 and best_day_cycle > 0.0
            else math.inf
        )
        if math.isfinite(consistency_ratio):
            maximum_consistency = max(maximum_consistency, consistency_ratio)
        pre_payout_balance = opening + day_pnl
        worst_intraday = _required_float(
            row.get("worst_intraday_equity"),
            label=f"{label} worst intraday equity",
        )
        if worst_intraday <= floor_open + 1e-12:
            raise ActiveRiskDecisionReportError(
                f"{label} surviving day crossed the MLL floor"
            )
        expected_trailing_floor = min(
            0.0,
            max(
                floor_open,
                pre_payout_balance - rules.maximum_loss_limit,
            ),
        )
        if path_name == "XFA_STANDARD":
            eligible = winning_days >= rules.xfa_standard_winning_days and (
                cycles == 0
                or pre_payout_balance - cycle_start_balance
                >= rules.later_standard_cycle_minimum_profit - 1e-12
            )
            cap = rules.standard_payout_cap
        else:
            eligible = bool(
                traded_days_cycle >= rules.xfa_consistency_traded_days
                and total_profit_cycle > 0.0
                and consistency_ratio <= rules.xfa_consistency_limit + 1e-12
            )
            cap = rules.consistency_payout_cap
        if bool(row.get("payout_eligible")) != eligible:
            raise ActiveRiskDecisionReportError(
                f"{label} payout eligibility is more permissive than frozen rules"
            )
        expected_gross = 0.0
        expected_net = 0.0
        candidate_gross = 0.0
        execute = False
        if eligible and pre_payout_balance > 0.0:
            candidate_gross = min(pre_payout_balance * rules.payout_fraction, cap)
            if candidate_gross >= rules.minimum_payout - 1e-12:
                expected_gross = candidate_gross
                expected_net = candidate_gross * rules.trader_profit_split
                execute = True
        if bool(row.get("payout_requested")) != execute:
            raise ActiveRiskDecisionReportError(
                f"{label} payout request timing drift"
            )
        observed_gross = _required_float(
            row.get("gross_payout", 0.0),
            label=f"{label} daily gross payout",
        )
        observed_net = _required_float(
            row.get("trader_net_payout", 0.0),
            label=f"{label} daily trader payout",
        )
        legacy_subminimum_marker = bool(
            not execute
            and eligible
            and 0.0 < candidate_gross < rules.minimum_payout
            and math.isclose(
                observed_gross,
                candidate_gross,
                rel_tol=1e-10,
                abs_tol=1e-8,
            )
            and math.isclose(observed_net, 0.0, abs_tol=1e-12)
            and math.isclose(
                closing,
                pre_payout_balance,
                rel_tol=1e-10,
                abs_tol=1e-8,
            )
        )
        _assert_close(
            expected_gross,
            0.0 if legacy_subminimum_marker else observed_gross,
            label=f"{label} daily gross payout",
        )
        _assert_close(
            expected_net,
            observed_net,
            label=f"{label} daily trader payout",
        )
        _assert_close(
            pre_payout_balance - expected_gross,
            closing,
            label=f"{label} closing balance after payout",
        )
        gross_total += expected_gross
        trader_total += expected_net
        if execute:
            cycles += 1
            if first_payout_day is None:
                first_payout_day = elapsed
            winning_days = 0
            traded_days_cycle = 0
            total_profit_cycle = 0.0
            best_day_cycle = 0.0
            cycle_start_balance = closing
            _assert_close(
                floor_close,
                0.0,
                label=f"{label} post-payout MLL-floor reset",
            )
        else:
            _assert_close(
                floor_close,
                expected_trailing_floor,
                label=f"{label} end-of-day trailing MLL floor",
            )
        if cycles > 0:
            _assert_close(
                floor_close,
                0.0,
                label=f"{label} post-payout MLL-floor lock",
            )
        if bool(row.get("post_payout_mll_locked_at_zero")) != (cycles > 0):
            raise ActiveRiskDecisionReportError(
                f"{label} post-payout MLL reset flag drift"
            )
        if int(row.get("winning_days_in_cycle", -1)) != winning_days:
            raise ActiveRiskDecisionReportError(f"{label} winning-day reset drift")
        if int(row.get("traded_days_in_cycle", -1)) != traded_days_cycle:
            raise ActiveRiskDecisionReportError(f"{label} traded-day reset drift")
        if int(row.get("payout_cycles", -1)) != cycles:
            raise ActiveRiskDecisionReportError(f"{label} payout-cycle reset drift")
        _assert_close(
            closing - cycle_start_balance,
            row.get("profit_since_payout"),
            label=f"{label} profit-since-payout reset",
        )
        observed_ratio = row.get("consistency_ratio_before_reset")
        if math.isfinite(consistency_ratio):
            _assert_close(
                consistency_ratio,
                observed_ratio,
                label=f"{label} consistency ratio",
            )
        elif observed_ratio is not None:
            raise ActiveRiskDecisionReportError(
                f"{label} non-finite consistency ratio should be absent"
            )
        prior_closing = closing
        prior_floor = floor_close

    failure_terminals = {"MLL_BREACHED", "HARD_RULE_FAILURE", "INACTIVITY_RISK"}
    expected_terminal_rows = 1 if path_terminal in failure_terminals else 0
    if terminal_row_count != expected_terminal_rows:
        raise ActiveRiskDecisionReportError(
            f"{label} terminal row/path chronology drift"
        )
    expected_ending_balance = (
        prior_closing if ledger else rules.xfa_starting_balance
    )
    expected_ending_floor = prior_floor if ledger else rules.xfa_starting_floor
    _assert_close(
        expected_ending_balance,
        path.get("ending_balance"),
        label=f"{label} ending balance",
    )
    _assert_close(
        expected_ending_floor,
        path.get("ending_mll_floor"),
        label=f"{label} ending MLL floor",
    )
    if path.get("payout_request_policy") != "EARLIEST_ELIGIBLE_END_OF_DAY":
        raise ActiveRiskDecisionReportError(f"{label} payout request policy drift")
    if path.get("payout_path_selected_from_outcomes") is not False:
        raise ActiveRiskDecisionReportError(f"{label} payout path oracle drift")

    if int(path.get("payout_cycles", -1)) != cycles:
        raise ActiveRiskDecisionReportError(f"{label} payout-cycle total drift")
    if bool(path.get("payout_eligible")) != (cycles > 0):
        raise ActiveRiskDecisionReportError(f"{label} payout-eligible total drift")
    _assert_close(gross_total, path.get("gross_payout"), label=f"{label} gross total")
    _assert_close(
        trader_total,
        path.get("trader_net_payout"),
        label=f"{label} trader payout total",
    )
    if path.get("first_payout_day") != first_payout_day:
        raise ActiveRiskDecisionReportError(f"{label} first-payout timing drift")
    if int(path.get("qualifying_winning_days", -1)) != total_qualifying_days:
        raise ActiveRiskDecisionReportError(f"{label} qualifying-day total drift")
    _assert_close(
        maximum_consistency,
        path.get("maximum_consistency_ratio"),
        label=f"{label} maximum consistency ratio",
    )


def _validate_xfa_path_accounting(
    path: Mapping[str, Any],
    *,
    label: str,
    policy_id: str,
    scenario: str,
    combine_start_id: int,
    combine_end_day: int,
    xfa_start_day: int | None,
    rule_snapshot: Mapping[str, Any],
) -> PayoutPathReconciliation:
    def nonnegative_int(value: Any, *, field_name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ActiveRiskDecisionReportError(
                f"{label} {field_name} is not a non-negative integer"
            )
        return int(value)

    ledger = path.get("daily_ledger")
    if not isinstance(ledger, list):
        raise ActiveRiskDecisionReportError(f"{label} daily ledger is absent")
    official_rules = official_rule_snapshot_2026_07_15().to_dict()
    if dict(rule_snapshot) != official_rules:
        raise ActiveRiskDecisionReportError(
            f"{label} rule snapshot differs from official 2026-07-15 freeze"
        )
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
    try:
        payout_reconciliation = reconcile_payout_path(
            path,
            policy_id=policy_id,
            scenario=scenario,
            combine_start_id=combine_start_id,
            allow_legacy_subminimum_marker=True,
        )
    except XfaPayoutEventError as exc:
        raise ActiveRiskDecisionReportError(
            f"{label} canonical payout-event reconciliation failed: {exc}"
        ) from exc
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
    _validate_xfa_payout_ledger(path, label=label)
    return payout_reconciliation


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
    canonical_gross_payout: float = 0.0
    canonical_payout_event_count: int = 0
    legacy_subminimum_marker_count: int = 0
    legacy_subminimum_marker_gross: float = 0.0
    evaluable_trader_net_payout: float = 0.0
    first_payout_evaluable_observed_payout: float = 0.0
    post_payout_survived: int = 0
    post_payout_censored: int = 0
    evaluable_post_payout_paths: int = 0
    evaluable_post_payout_survived: int = 0
    first_payout_days: list[float] = field(default_factory=list)
    evaluable_first_payout_days: list[float] = field(default_factory=list)
    payout_cycles_by_started_path: list[float] = field(default_factory=list)
    payout_cycles_before_observed_closure: list[float] = field(default_factory=list)
    payout_cycles_on_censored_paths: list[float] = field(default_factory=list)
    minimum_mll_buffers: list[float] = field(default_factory=list)
    evaluable_minimum_mll_buffers: list[float] = field(default_factory=list)
    missing_minimum_mll_buffers: int = 0
    terminal_distribution: Counter[str] = field(default_factory=Counter)
    closure_before_first_payout_count: int = 0

    def add_combine_episode(self, raw: Mapping[str, Any]) -> None:
        terminal = str(raw.get("terminal_classification") or raw.get("terminal") or "")
        censored = terminal in {
            "DATA_CENSORED",
            "OPERATIONAL_HORIZON_NOT_REACHED",
        } or bool(raw.get("censored"))
        self.combine_attempts += 1
        self.combine_censored_attempts += int(censored)

    def add_path(
        self,
        path: Mapping[str, Any],
        *,
        payout_reconciliation: PayoutPathReconciliation,
    ) -> None:
        self.xfa_paths_started += 1
        terminal = str(path.get("terminal") or "")
        self.terminal_distribution[terminal] += 1
        observed = int(path.get("observed_days", 0)) > 0
        censored = terminal == "DATA_CENSORED"
        evaluable = observed and not censored
        self.observed_paths += int(observed)
        self.zero_observation_paths += int(not observed)
        self.xfa_censored_paths += int(censored)
        self.evaluable_xfa_paths += int(evaluable)
        eligible = payout_reconciliation.first_payout_count == 1
        self.closure_before_first_payout_count += int(
            not eligible
            and terminal
            in {"MLL_BREACHED", "HARD_RULE_FAILURE", "INACTIVITY_RISK"}
        )
        first_payout_evaluable = evaluable or eligible
        self.first_payout_evaluable_xfa_paths += int(first_payout_evaluable)
        self.first_payouts += int(eligible)
        self.evaluable_first_payouts += int(eligible and first_payout_evaluable)
        cycles = len(payout_reconciliation.payout_events)
        self.payout_cycles_by_started_path.append(float(cycles))
        if terminal in {"MLL_BREACHED", "HARD_RULE_FAILURE", "INACTIVITY_RISK"}:
            self.payout_cycles_before_observed_closure.append(float(cycles))
        if censored:
            self.payout_cycles_on_censored_paths.append(float(cycles))
        payout = payout_reconciliation.canonical_trader_net_payout
        self.payout_cycles += cycles
        self.canonical_gross_payout += (
            payout_reconciliation.canonical_gross_payout
        )
        self.canonical_payout_event_count += cycles
        self.legacy_subminimum_marker_count += (
            payout_reconciliation.legacy_subminimum_marker_count
        )
        self.legacy_subminimum_marker_gross += (
            payout_reconciliation.legacy_subminimum_marker_gross
        )
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
            "expected_payout_cycles_per_successful_combine": (
                self.payout_cycles / self.xfa_paths_started
                if self.xfa_paths_started
                else 0.0
            ),
            "expected_trader_payout_per_combine_attempt": (
                self.trader_net_payout / self.combine_attempts
                if self.combine_attempts
                else 0.0
            ),
            "expected_trader_payout_per_successful_combine": (
                self.trader_net_payout / self.xfa_paths_started
                if self.xfa_paths_started
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
            "closure_before_first_payout_count": (
                self.closure_before_first_payout_count
            ),
            "terminal_distribution": dict(sorted(self.terminal_distribution.items())),
            "evaluable_first_payouts": self.evaluable_first_payouts,
            "payout_cycles": self.payout_cycles,
            "canonical_payout_event_schema": CANONICAL_PAYOUT_EVENT_SCHEMA,
            "canonical_payout_event_count": self.canonical_payout_event_count,
            "canonical_gross_payout": self.canonical_gross_payout,
            "legacy_raw_daily_gross_including_subminimum_markers": (
                self.canonical_gross_payout + self.legacy_subminimum_marker_gross
            ),
            "legacy_subminimum_marker_count": (
                self.legacy_subminimum_marker_count
            ),
            "legacy_subminimum_marker_gross": (
                self.legacy_subminimum_marker_gross
            ),
            "legacy_marker_semantics": (
                "ELIGIBLE_CANDIDATE_BELOW_MINIMUM_NOT_REQUESTED_NOT_PAID;"
                "EXCLUDED_FROM_CANONICAL_PAYOUT_EVENTS_AND_DERIVED_TOTALS"
            ),
            "payout_cycles_by_path": {
                "all_started_paths": _distribution(
                    self.payout_cycles_by_started_path
                ),
                "before_observed_account_closure": _distribution(
                    self.payout_cycles_before_observed_closure
                ),
                "on_censored_paths": _distribution(
                    self.payout_cycles_on_censored_paths
                ),
                "closure_terminal_states": [
                    "HARD_RULE_FAILURE",
                    "INACTIVITY_RISK",
                    "MLL_BREACHED",
                ],
                "interpretation": (
                    "OBSERVED_CYCLES_WITHIN_FROZEN_120_SESSION_XFA_HORIZON;"
                    "CENSORED_PATHS_ARE_NOT_TREATED_AS_CLOSED"
                ),
            },
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


@dataclass
class CampaignLifecycleAuditAccumulator:
    finalist_ids: frozenset[str] = field(default_factory=frozenset)
    transition_stages: dict[tuple[str, str, int], str] = field(default_factory=dict)
    full_episode_stages: dict[tuple[str, str, int], str] = field(default_factory=dict)
    path_keys: set[tuple[str, str, int, str]] = field(default_factory=set)
    lifecycle: dict[str, dict[str, LifecyclePathAccumulator]] = field(
        default_factory=lambda: {
            scenario: {path: LifecyclePathAccumulator() for path in PATHS}
            for scenario in SCENARIOS
        }
    )
    stage_full_episode_counts: Counter[str] = field(default_factory=Counter)
    stage_transition_counts: Counter[str] = field(default_factory=Counter)
    canonical_event_fingerprints: set[str] = field(default_factory=set)
    legacy_marker_path_keys: set[tuple[str, str, int, str]] = field(
        default_factory=set
    )
    legacy_marker_policy_ids: set[str] = field(default_factory=set)
    legacy_marker_gross_distribution: Counter[str] = field(
        default_factory=Counter
    )
    legacy_marker_path_arithmetic: list[dict[str, Any]] = field(
        default_factory=list
    )

    def add_full_episode(self, row: Mapping[str, Any], *, stage: str) -> None:
        policy_id = str(row.get("policy_id") or "")
        scenario = _scenario_key(row.get("cost_scenario"))
        episode_id = str(row.get("episode_id") or "")
        try:
            start_day = int(episode_id.rsplit(":", 1)[1])
        except (IndexError, ValueError) as exc:
            raise ActiveRiskDecisionReportError(
                "sealed FULL episode has malformed start identity"
            ) from exc
        if episode_id != f"{policy_id}:{start_day}":
            raise ActiveRiskDecisionReportError(
                "sealed FULL episode policy/start identity drift"
            )
        try:
            parsed_start = datetime.fromisoformat(
                str(row["episode_start"]).replace("Z", "+00:00")
            )
        except (KeyError, ValueError) as exc:
            raise ActiveRiskDecisionReportError(
                "sealed FULL episode has malformed episode_start"
            ) from exc
        if parsed_start.tzinfo is None or parsed_start.utcoffset() != timedelta(0):
            raise ActiveRiskDecisionReportError(
                "sealed FULL episode_start is not UTC"
            )
        iso_start = parsed_start.date()
        if (iso_start - date(1970, 1, 1)).days != start_day:
            raise ActiveRiskDecisionReportError(
                "sealed FULL episode ISO/epoch start drift"
            )
        key = (policy_id, scenario, start_day)
        prior_stage = self.full_episode_stages.get(key)
        if prior_stage is not None:
            raise ActiveRiskDecisionReportError(
                f"duplicate or inter-stage FULL episode {key}: {prior_stage}/{stage}"
            )
        self.full_episode_stages[key] = stage
        self.stage_full_episode_counts[stage] += 1
        for path in PATHS:
            self.lifecycle[scenario][path].add_combine_episode(
                {
                    "terminal": row.get("terminal_state"),
                    "censored": bool(row.get("censored_state")),
                }
            )

        is_pass = (
            row.get("target_reached") is True
            and str(row.get("terminal_state") or "") == "TARGET_REACHED"
        )
        embedded = row.get("active_risk_pool_lifecycle")
        if is_pass != isinstance(embedded, Mapping):
            raise ActiveRiskDecisionReportError(
                "sealed FULL-pass/lifecycle field bijection drift"
            )
        if not is_pass:
            return
        if key in self.transition_stages:
            raise ActiveRiskDecisionReportError(
                f"duplicate lifecycle transition key {key}"
            )
        self.transition_stages[key] = stage
        self.stage_transition_counts[stage] += 1
        lifecycle_source = dict(embedded)
        lifecycle_source.pop("sealed_lifecycle_sha256", None)
        try:
            validated = _active_pool_lifecycle_evidence(
                lifecycle_source,
                expected_policy_id=policy_id,
                expected_scenario=str(row["cost_scenario"]),
                expected_start_day=start_day,
            )
        except (ActiveRiskRuntimeError, KeyError, TypeError, ValueError) as exc:
            raise ActiveRiskDecisionReportError(
                f"sealed campaign-wide lifecycle validation failed: {exc}"
            ) from exc
        if dict(validated) != dict(embedded):
            raise ActiveRiskDecisionReportError(
                "embedded lifecycle does not reproduce its sealed payload"
            )
        official = official_rule_snapshot_2026_07_15().to_dict()
        if dict(validated.get("rule_snapshot") or {}) != official:
            raise ActiveRiskDecisionReportError(
                "embedded lifecycle rule snapshot differs from official freeze"
            )
        for path, expected_name in (
            ("standard", "XFA_STANDARD"),
            ("consistency", "XFA_CONSISTENCY"),
        ):
            value = validated.get(path)
            if not isinstance(value, Mapping) or str(value.get("path") or "") != expected_name:
                raise ActiveRiskDecisionReportError(
                    f"lifecycle transition {key} lacks exact {expected_name} path"
                )
            path_key = (*key, path)
            if path_key in self.path_keys:
                raise ActiveRiskDecisionReportError(
                    f"duplicate lifecycle alternative path key {path_key}"
                )
            self.path_keys.add(path_key)
            payout_reconciliation = _validate_xfa_path_accounting(
                value,
                label=f"sealed campaign lifecycle {key} {path}",
                policy_id=policy_id,
                scenario=scenario,
                combine_start_id=start_day,
                combine_end_day=int(validated["combine_end_day"]),
                xfa_start_day=(
                    None
                    if validated["xfa_start_day"] is None
                    else int(validated["xfa_start_day"])
                ),
                rule_snapshot=validated["rule_snapshot"],
            )
            for event in payout_reconciliation.payout_events:
                if event.event_fingerprint in self.canonical_event_fingerprints:
                    raise ActiveRiskDecisionReportError(
                        "duplicate canonical payout-event fingerprint"
                    )
                self.canonical_event_fingerprints.add(event.event_fingerprint)
            if payout_reconciliation.legacy_subminimum_marker_count:
                self.legacy_marker_path_keys.add(
                    (policy_id, scenario, start_day, expected_name)
                )
                self.legacy_marker_policy_ids.add(policy_id)
                self.legacy_marker_gross_distribution.update(
                    _canonical_decimal_key(value)
                    for value in (
                        payout_reconciliation.legacy_subminimum_marker_amounts
                    )
                )
                self.legacy_marker_path_arithmetic.append(
                    {
                        "policy_id": policy_id,
                        "scenario": scenario,
                        "combine_start_id": start_day,
                        "xfa_path": expected_name,
                        "raw_daily_gross_including_nonexecuted_marker": (
                            payout_reconciliation.canonical_gross_payout
                            + payout_reconciliation.legacy_subminimum_marker_gross
                        ),
                        "minus_nonexecuted_subminimum_marker_gross": (
                            payout_reconciliation.legacy_subminimum_marker_gross
                        ),
                        "equals_executed_canonical_gross_payout": (
                            payout_reconciliation.canonical_gross_payout
                        ),
                        "canonical_trader_net_payout": (
                            payout_reconciliation.canonical_trader_net_payout
                        ),
                        "canonical_payout_event_count": len(
                            payout_reconciliation.payout_events
                        ),
                    }
                )
            self.lifecycle[scenario][path].add_path(
                value,
                payout_reconciliation=payout_reconciliation,
            )

    def to_dict(self) -> dict[str, Any]:
        transitions = len(self.transition_stages)
        paths = len(self.path_keys)
        if paths != transitions * len(PATHS):
            raise ActiveRiskDecisionReportError(
                "campaign lifecycle does not have exactly two alternatives per transition"
            )
        first_payouts = sum(
            self.lifecycle[scenario][path].first_payouts
            for scenario in SCENARIOS
            for path in PATHS
        )
        payout_cycles = sum(
            self.lifecycle[scenario][path].payout_cycles
            for scenario in SCENARIOS
            for path in PATHS
        )
        canonical_events = sum(
            self.lifecycle[scenario][path].canonical_payout_event_count
            for scenario in SCENARIOS
            for path in PATHS
        )
        legacy_marker_count = sum(
            self.lifecycle[scenario][path].legacy_subminimum_marker_count
            for scenario in SCENARIOS
            for path in PATHS
        )
        legacy_marker_gross = sum(
            self.lifecycle[scenario][path].legacy_subminimum_marker_gross
            for scenario in SCENARIOS
            for path in PATHS
        )
        canonical_gross = sum(
            self.lifecycle[scenario][path].canonical_gross_payout
            for scenario in SCENARIOS
            for path in PATHS
        )
        if canonical_events != payout_cycles:
            raise ActiveRiskDecisionReportError(
                "canonical payout-event count differs from payout cycles"
            )
        if not 0 <= first_payouts <= paths:
            raise ActiveRiskDecisionReportError(
                "first-payout count exceeds unique alternative paths"
            )
        trader_cash = sum(
            self.lifecycle[scenario][path].trader_net_payout
            for scenario in SCENARIOS
            for path in PATHS
        )
        survival = sum(
            self.lifecycle[scenario][path].post_payout_survived
            for scenario in SCENARIOS
            for path in PATHS
        )
        return {
            "source": "DEEP_VERIFIED_EVIDENCE_BUNDLE_EPISODE_PARTITIONS",
            "full_episode_count": len(self.full_episode_stages),
            "combine_to_xfa_transition_count": transitions,
            "alternative_path_count": paths,
            "first_payout_path_observation_count": first_payouts,
            "payout_cycle_observation_count": payout_cycles,
            "canonical_payout_event_schema": CANONICAL_PAYOUT_EVENT_SCHEMA,
            "canonical_payout_event_count": canonical_events,
            "canonical_gross_payout": canonical_gross,
            "legacy_subminimum_marker_count": legacy_marker_count,
            "legacy_subminimum_marker_gross": legacy_marker_gross,
            "legacy_raw_daily_gross_including_subminimum_markers": (
                canonical_gross + legacy_marker_gross
            ),
            "canonical_gross_arithmetic_bridge": {
                "raw_daily_gross_including_nonexecuted_legacy_markers": (
                    canonical_gross + legacy_marker_gross
                ),
                "minus_nonexecuted_subminimum_marker_gross": legacy_marker_gross,
                "equals_executed_canonical_gross_payout": canonical_gross,
            },
            "legacy_subminimum_marker_gross_distribution": dict(
                sorted(self.legacy_marker_gross_distribution.items())
            ),
            "legacy_subminimum_marker_affected_path_keys": [
                {
                    "policy_id": policy_id,
                    "scenario": scenario,
                    "combine_start_id": start_day,
                    "xfa_path": path,
                }
                for policy_id, scenario, start_day, path in sorted(
                    self.legacy_marker_path_keys
                )
            ],
            "legacy_subminimum_marker_affected_policy_ids": sorted(
                self.legacy_marker_policy_ids
            ),
            "legacy_subminimum_marker_path_arithmetic": sorted(
                self.legacy_marker_path_arithmetic,
                key=lambda row: (
                    row["policy_id"],
                    row["scenario"],
                    row["combine_start_id"],
                    row["xfa_path"],
                ),
            ),
            "legacy_subminimum_marker_affected_finalist_ids": sorted(
                self.legacy_marker_policy_ids & set(self.finalist_ids)
            ),
            "canonical_payout_event_unique_fingerprint_count": len(
                self.canonical_event_fingerprints
            ),
            "trader_90_percent_split_cash_observations_before_fees_tax": trader_cash,
            "post_payout_survival_observation_count": survival,
            "stage_full_episode_counts": dict(sorted(self.stage_full_episode_counts.items())),
            "stage_transition_counts": dict(sorted(self.stage_transition_counts.items())),
            "transition_key_uniqueness_proved": True,
            "full_episode_key_uniqueness_proved": True,
            "zero_inter_stage_overlap_proved": True,
            "full_pass_lifecycle_bijection_proved": True,
            "exactly_two_alternative_paths_per_transition_proved": True,
            "path_key_uniqueness_proved": True,
            "first_payout_uniqueness_per_path_proved": True,
            "canonical_event_to_summary_reconciliation_proved": True,
            "standard_consistency_alternatives_kept_separate": True,
            "official_rule_snapshot_exact": True,
            "payout_eligibility_amount_and_reset_reexecuted_from_daily_ledger": True,
            "by_scenario_and_path": {
                scenario: {
                    path: self.lifecycle[scenario][path].to_dict()
                    for path in PATHS
                }
                for scenario in SCENARIOS
            },
        }


def _stream_sealed_campaign_lifecycle(
    bundle_path: Path,
    bundle_manifest: Mapping[str, Any],
    *,
    finalist_ids: Iterable[str] = (),
) -> dict[str, Any]:
    files = bundle_manifest.get("files")
    if not isinstance(files, Mapping):
        raise ActiveRiskDecisionReportError(
            "verified EvidenceBundle lacks episode partitions"
        )
    accumulator = CampaignLifecycleAuditAccumulator(
        finalist_ids=frozenset(str(value) for value in finalist_ids)
    )
    relevant_partition_count = 0
    for relative_path, details in sorted(files.items()):
        if (
            not isinstance(details, Mapping)
            or details.get("kind") != "dataset_partition"
            or details.get("dataset") != "episodes"
        ):
            continue
        batch_id = str(details.get("batch_id") or "")
        pieces = batch_id.split(":")
        if len(pieces) != 4 or pieces[0] != "active" or pieces[3] != "episodes":
            continue
        stage = pieces[1]
        if stage not in {"stage3", "stage4", "stage5"}:
            continue
        relevant_partition_count += 1
        path = (bundle_path / str(relative_path)).resolve()
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                envelope = json.loads(handle.readline()).get("_evidence_part")
                if (
                    not isinstance(envelope, Mapping)
                    or envelope.get("dataset") != "episodes"
                    or envelope.get("batch_id") != batch_id
                ):
                    raise ActiveRiskDecisionReportError(
                        "sealed episode partition header drift"
                    )
                observed_rows = 0
                for line in handle:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    observed_rows += 1
                    if str(row.get("horizon") or "") != FULL_HORIZON:
                        if "active_risk_pool_lifecycle" in row:
                            raise ActiveRiskDecisionReportError(
                                "non-FULL episode contains lifecycle evidence"
                            )
                        continue
                    accumulator.add_full_episode(row, stage=stage)
                if observed_rows != int(envelope.get("row_count", -1)):
                    raise ActiveRiskDecisionReportError(
                        "sealed episode partition row-count drift"
                    )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ActiveRiskDecisionReportError(
                f"cannot stream sealed campaign lifecycle: {exc}"
            ) from exc
    if relevant_partition_count == 0:
        raise ActiveRiskDecisionReportError(
            "sealed EvidenceBundle has no Stage3-5 episode partitions"
        )
    result = accumulator.to_dict()
    result["episode_partition_count"] = relevant_partition_count
    return result


def _canonical_decimal_key(value: float) -> str:
    """Stable human-auditable key for an exact payout-marker distribution."""

    return format(Decimal(str(float(value))).normalize(), "f")


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
    if not _nested_evidence_equal(
        candidate_signature, candidate.get("exposure_signature") or {}
    ):
        raise ActiveRiskDecisionReportError(
            f"candidate {policy_id} exposure-signature linkage drift"
        )
    if not _nested_evidence_equal(
        control_signature, random_control.get("exposure_signature") or {}
    ):
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
    raw_maximum_mini = _required_float(
        raw.get("maximum_mini_equivalent"),
        label=f"{label} authoritative maximum mini-equivalent",
    )
    raw_maximum_directional = _required_float(
        raw.get("maximum_net_directional_exposure"),
        label=f"{label} authoritative maximum directional exposure",
    )
    if (
        min(
            maximum_mini,
            maximum_directional,
            raw_maximum_mini,
            raw_maximum_directional,
        )
        < -1e-12
        or maximum_directional > maximum_mini + 1e-12
        or raw_maximum_directional > raw_maximum_mini + 1e-12
    ):
        raise ActiveRiskDecisionReportError(
            f"{label} exposure bounds are internally inconsistent"
        )
    # The episode-level fields are the authoritative replay maxima.  The
    # enriched daily projection replays accepted entries after the episode and
    # deliberately lacks the frozen component-priority tie-break used when
    # entry/exit timestamps coincide.  Its intraday peak can therefore differ
    # without changing trades, PnL, MLL, consistency, or the sealed episode.
    # Keep the daily values as bounded diagnostics instead of asserting false
    # equality against the authoritative episode-level maxima.
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
    controls: Mapping[str, Any] | None,
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
        if not isinstance(cached, Mapping) or not _nested_evidence_equal(
            cached, derived
        ):
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
    if controls is not None:
        compact["control_deltas"] = _candidate_controls(compact, controls)
    else:
        compact["control_deltas"] = None
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
    recovery = result.get("sealed_result_recovery")
    if (
        not isinstance(recovery, Mapping)
        or recovery.get("preregistered_deep_guard_count") != 2
        or recovery.get("additional_deep_guard_performed") is not False
        or recovery.get("deep_guard_completion_proof")
        != "EXACT_POST_GUARD_FAILED_CLOSED_COUNTER_ASSERTION"
    ):
        raise ActiveRiskDecisionReportError(
            "economic final result lacks the two-guard completion proof"
        )
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
        verified_bundle = verify_evidence_bundle(bundle_path, deep=False)
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
    if (
        not isinstance(bundle_pareto, Mapping)
        or bundle_pareto.get("schema") != "hydra_active_risk_pareto_archive_v1"
        or str(bundle_pareto.get("campaign_id") or "") != CAMPAIGN_ID
        or bundle_pareto.get("opaque_score_used") is not False
    ):
        raise ActiveRiskDecisionReportError(
            "sealed Pareto archive identity or transparent-ranking contract drift"
        )
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
    sealed_finalist_frontier = bundle_pareto.get("frontier")
    if not isinstance(sealed_finalist_frontier, list) or not all(
        isinstance(row, Mapping) for row in sealed_finalist_frontier
    ):
        raise ActiveRiskDecisionReportError(
            "sealed Pareto archive lacks its development-finalist frontier"
        )
    frontier_ids = [str(row.get("policy_id") or "") for row in sealed_finalist_frontier]
    if not all(frontier_ids) or len(frontier_ids) != len(set(frontier_ids)):
        raise ActiveRiskDecisionReportError(
            "sealed Pareto archive has duplicate or empty finalist identities"
        )
    if not result_stage_decisions:
        raise ActiveRiskDecisionReportError("sealed Pareto archive has no halving decisions")
    final_decision_ids = {
        str(value)
        for value in result_stage_decisions[-1].get("selected_policy_ids") or ()
    }
    economic_finalist_ids = {
        str(value) for value in economic.get("development_finalist_ids") or ()
    }
    confirmation_ids = {
        str(value)
        for value in economic.get("confirmation_ready_candidate_ids") or ()
    }
    if (
        set(frontier_ids) != final_decision_ids
        or (economic_finalist_ids and set(frontier_ids) != economic_finalist_ids)
        or (confirmation_ids and set(frontier_ids) != confirmation_ids)
    ):
        raise ActiveRiskDecisionReportError(
            "sealed Pareto finalist identities diverge from terminal halving status"
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
    episode_dataset_accounting = _episode_dataset_accounting(
        verified_bundle,
        canonical_attempt_count=result_counter_values[
            "combine_episodes_completed"
        ],
    )
    component_market_map, component_trade_index = _sealed_component_trade_evidence(
        bundle_path, verified_bundle
    )
    sealed_finalist_policy_freeze = _sealed_finalist_policy_freeze(
        bundle_path,
        verified_bundle,
        identity,
        campaign_manifest,
        set(frontier_ids),
    )
    sealed_campaign_lifecycle_audit = _stream_sealed_campaign_lifecycle(
        bundle_path,
        verified_bundle,
        finalist_ids=frontier_ids,
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
        "source": (
            "TWO_PREREGISTERED_DEEP_GUARDS_REUSED_PLUS_REPORT_"
            "RELATIONAL_REDERIVATION"
        ),
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
        "episode_dataset_accounting": episode_dataset_accounting,
        "sealed_campaign_lifecycle_audit": sealed_campaign_lifecycle_audit,
        "sealed_finalist_policy_freeze": {
            "finalist_count": sealed_finalist_policy_freeze["finalist_count"],
            "campaign_contract_sha256": sealed_finalist_policy_freeze[
                "campaign_contract_sha256"
            ],
            "verification": (
                "PREREGISTERED_DEEP_GUARDS_REUSED_AND_REPORT_REDERIVED"
            ),
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
            "verification": "TWO_PREREGISTERED_DEEP_GUARDS_REUSED",
            "preregistered_deep_guard_count": 2,
            "additional_deep_guard_performed_by_report": False,
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
            "deep_verification_source": (
                "ORIGINAL_RUNNER_TWO_PREREGISTERED_POST_RECEIPT_GUARDS"
            ),
            "additional_deep_guard_performed_by_report": False,
        },
    }, {
        "matched_controls": dict(economic_controls),
        "stage_decisions": [dict(value) for value in result_stage_decisions],
        "bundle_path": str(bundle_path),
        "economic_results": dict(economic),
        "bundle_manifest": dict(verified_bundle),
        "bundle_identity": dict(identity),
        "sealed_finalist_frontier": [
            dict(value) for value in sealed_finalist_frontier
        ],
        "component_market_map": component_market_map,
        "component_trade_index": component_trade_index,
        "sealed_finalist_policy_freeze": sealed_finalist_policy_freeze,
        "sealed_campaign_lifecycle_audit": sealed_campaign_lifecycle_audit,
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


def _validate_finalist_compact_summary(
    summary: Mapping[str, Any], *, label: str, expected_starts: int | None
) -> dict[str, Any]:
    """Validate the exact mergeable fields in one sealed finalist summary."""

    episode_count = int(summary.get("episode_count", -1))
    if episode_count < 0 or (
        expected_starts is not None and episode_count != expected_starts
    ):
        raise ActiveRiskDecisionReportError(
            f"{label} expanded-start episode coverage drift"
        )
    terminal = _combine_terminal_metrics(summary, label=label)
    arrays = {
        "net_values": summary.get("net_values"),
        "target_progress_values": summary.get("target_progress_values"),
        "duration_trading_days_values": summary.get(
            "duration_trading_days_values"
        ),
        "active_trading_days_values": summary.get(
            "active_trading_days_values"
        ),
        "calendar_days_values": summary.get("calendar_days_values"),
    }
    for field_name, values in arrays.items():
        if not isinstance(values, list) or len(values) != episode_count:
            raise ActiveRiskDecisionReportError(
                f"{label} {field_name} does not cover every expanded start"
            )
    days_to_target = summary.get("days_to_target_values")
    if not isinstance(days_to_target, list) or len(days_to_target) != int(
        summary.get("pass_count", -1)
    ):
        raise ActiveRiskDecisionReportError(
            f"{label} days-to-target/pass coverage drift"
        )

    exact_checks = {
        "net_total": sum(_float(value) for value in arrays["net_values"]),
        "net_median": statistics.median(arrays["net_values"])
        if arrays["net_values"]
        else 0.0,
        "target_progress_median": statistics.median(
            arrays["target_progress_values"]
        )
        if arrays["target_progress_values"]
        else 0.0,
        "target_progress_p25": _quantile(
            arrays["target_progress_values"], 0.25
        )
        if arrays["target_progress_values"]
        else 0.0,
        "duration_trading_days_median": statistics.median(
            arrays["duration_trading_days_values"]
        )
        if arrays["duration_trading_days_values"]
        else 0.0,
        "active_trading_days_median": statistics.median(
            arrays["active_trading_days_values"]
        )
        if arrays["active_trading_days_values"]
        else 0.0,
        "calendar_days_median": statistics.median(arrays["calendar_days_values"])
        if arrays["calendar_days_values"]
        else 0.0,
    }
    for field_name, derived in exact_checks.items():
        _assert_close(derived, summary.get(field_name), label=f"{label} {field_name}")
    median_days = statistics.median(days_to_target) if days_to_target else None
    if median_days is None:
        if summary.get("median_days_to_target") is not None:
            raise ActiveRiskDecisionReportError(
                f"{label} median days-to-target should be absent"
            )
    else:
        _assert_close(
            median_days,
            summary.get("median_days_to_target"),
            label=f"{label} median days-to-target",
        )
    consistency_ok = int(summary.get("consistency_ok_count", -1))
    if not 0 <= consistency_ok <= episode_count:
        raise ActiveRiskDecisionReportError(f"{label} consistency count drift")
    _assert_close(
        consistency_ok / episode_count if episode_count else 0.0,
        summary.get("consistency_rate"),
        label=f"{label} consistency rate",
    )
    return {**_summary_view(summary), **terminal}


_PORTFOLIO_STRESS_EVENT_SUFFIX = ":portfolio_cost_stress_1_5x"


def _trade_attribution_from_routing(
    raw: Mapping[str, Any],
    *,
    component_trade_index: Mapping[tuple[str, str], Mapping[str, Any]],
    label: str,
) -> dict[str, Any]:
    """Rebuild realized account PnL per immutable source trade."""

    scenario = _scenario_key(raw.get("scenario"))
    decisions = _canonical_daily_routing(raw)
    seen_events: set[str] = set()
    component_pnl: dict[str, float] = defaultdict(float)
    source_trade_pnl: dict[tuple[str, str], float] = defaultdict(float)
    accepted_observations: list[float] = []
    accepted_trade_rows: list[dict[str, Any]] = []
    for decision in decisions:
        component_id = str(decision.get("component_id") or "")
        routed_event_id = str(decision.get("event_id") or "")
        if not component_id or not routed_event_id:
            raise ActiveRiskDecisionReportError(
                f"{label} routing lacks component/source-trade identity"
            )
        if routed_event_id in seen_events:
            raise ActiveRiskDecisionReportError(
                f"{label} duplicates routed event {routed_event_id}"
            )
        seen_events.add(routed_event_id)
        if scenario == "stressed":
            if not routed_event_id.endswith(_PORTFOLIO_STRESS_EVENT_SUFFIX):
                raise ActiveRiskDecisionReportError(
                    f"{label} stressed routing lacks the frozen stress suffix"
                )
            source_trade_id = routed_event_id[: -len(_PORTFOLIO_STRESS_EVENT_SUFFIX)]
        else:
            if routed_event_id.endswith(_PORTFOLIO_STRESS_EVENT_SUFFIX):
                raise ActiveRiskDecisionReportError(
                    f"{label} normal routing contains a stressed source identity"
                )
            source_trade_id = routed_event_id
        source_key = (component_id, source_trade_id)
        source = component_trade_index.get(source_key)
        if not isinstance(source, Mapping):
            raise ActiveRiskDecisionReportError(
                f"{label} cannot resolve sealed component trade {source_key}"
            )
        base_quantity = decision.get("base_quantity")
        admitted_quantity = decision.get("quantity")
        requested_quantity = decision.get("requested_quantity")
        for field_name, value, allow_zero in (
            ("base_quantity", base_quantity, False),
            ("quantity", admitted_quantity, True),
            ("requested_quantity", requested_quantity, False),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < int(not allow_zero)
            ):
                raise ActiveRiskDecisionReportError(
                    f"{label} {routed_event_id} has invalid {field_name}"
                )
        if int(base_quantity) != int(source["quantity"]):
            raise ActiveRiskDecisionReportError(
                f"{label} {routed_event_id} base/source quantity drift"
            )
        allow = bool(decision.get("allow"))
        accepted_flag = bool(decision.get("accepted"))
        rejected_flag = bool(decision.get("rejected"))
        status = str(decision.get("decision_status") or "")
        accepted_status = status in {"ACCEPTED", "SIZE_REDUCED"}
        if (
            decision.get("emitted") is not True
            or allow != accepted_flag
            or allow != accepted_status
            or rejected_flag == allow
            or (allow and int(admitted_quantity) <= 0)
            or (not allow and int(admitted_quantity) != 0)
            or int(admitted_quantity) > int(requested_quantity)
        ):
            raise ActiveRiskDecisionReportError(
                f"{label} {routed_event_id} routing status/quantity drift"
            )
        expected_reduced = allow and int(admitted_quantity) < int(requested_quantity)
        if bool(decision.get("size_reduced")) != expected_reduced:
            raise ActiveRiskDecisionReportError(
                f"{label} {routed_event_id} size-reduction flag drift"
            )
        _assert_close(
            decision.get("scaling_factor"),
            int(admitted_quantity) / int(base_quantity),
            label=f"{label} {routed_event_id} scaling factor",
        )
        source_net = (
            _required_float(
                source["gross_pnl"], label=f"{label} {source_key} source gross"
            )
            - 1.5
            * _required_float(
                source["costs"], label=f"{label} {source_key} source costs"
            )
            if scenario == "stressed"
            else _required_float(
                source["net_pnl"], label=f"{label} {source_key} source net"
            )
        )
        suppressed = max(0, int(requested_quantity) - int(admitted_quantity))
        _assert_close(
            decision.get("foregone_realized_pnl_ex_post"),
            source_net * suppressed / int(base_quantity),
            label=f"{label} {routed_event_id} foregone realized PnL",
        )
        if allow:
            account_net = source_net * int(admitted_quantity) / int(base_quantity)
            component_pnl[component_id] += account_net
            source_trade_pnl[source_key] += account_net
            accepted_observations.append(account_net)
            accepted_trade_rows.append(
                {
                    "component_id": component_id,
                    "source_trade_id": source_trade_id,
                    "routed_event_id": routed_event_id,
                    "base_quantity": int(base_quantity),
                    "requested_quantity": int(requested_quantity),
                    "admitted_quantity": int(admitted_quantity),
                    "source_net_pnl": source_net,
                    "account_net_pnl": account_net,
                }
            )

    raw_component = raw.get("component_contribution")
    if not isinstance(raw_component, Mapping) or set(raw_component) != set(
        component_pnl
    ):
        raise ActiveRiskDecisionReportError(
            f"{label} routed-trade/component attribution key drift"
        )
    for component_id, value in component_pnl.items():
        _assert_close(
            value,
            raw_component[component_id],
            label=f"{label} routed-trade/component attribution {component_id}",
        )
    return {
        "component_pnl": dict(component_pnl),
        "source_trade_pnl": dict(source_trade_pnl),
        "accepted_trade_observations": accepted_observations,
        "accepted_trade_rows": accepted_trade_rows,
    }


_EXPANDED_BEHAVIOR_FEATURE_SCHEMA = (
    "hydra_expanded_finalist_cumulative_behavior_features_v1"
)
_EXPANDED_BEHAVIOR_FINGERPRINT_SCHEMA = (
    "hydra_expanded_finalist_cumulative_account_trade_behavior_v1"
)
_CUMULATIVE_BEHAVIOR_CLUSTER_THRESHOLDS = {
    "minimum_account_vector_correlation": 0.995,
    "maximum_account_vector_rmse": 0.05,
    "minimum_terminal_agreement": 0.95,
    "minimum_routing_jaccard": 0.90,
    "minimum_admitted_trade_jaccard": 0.90,
}


@dataclass(frozen=True)
class ExpandedBehaviorObservation:
    """Compact, exact binding plus fixed-width features for one raw episode."""

    scenario: str
    start_day: int
    stage: str
    exact_row_hash: str
    feature_values: tuple[float, ...]
    terminal_code: int
    routing_tuples: frozenset[tuple[Any, ...]]
    admitted_trade_tuples: frozenset[tuple[Any, ...]]


def _runtime_behavior_fingerprint_from_raw(
    canonical_raw: Sequence[Mapping[str, Any]],
) -> str:
    """Exactly reproduce ``active_risk_runtime._behavior_rows`` hashing.

    The runtime hashes the internal Combine terminal enum rather than the
    report's censoring classification.  Both censoring classifications map
    back to the single runtime ``TIMEOUT`` value without ambiguity.
    """

    runtime_terminal = {
        "TARGET_REACHED": "PASSED",
        "MLL_BREACHED": "MLL_BREACH",
        "HARD_RULE_FAILURE": "COMPLIANCE_FAILURE",
        "DATA_CENSORED": "TIMEOUT",
        "OPERATIONAL_HORIZON_NOT_REACHED": "TIMEOUT",
    }
    by_scenario: dict[str, list[dict[str, Any]]] = {
        scenario: [] for scenario in SCENARIOS
    }
    for raw in canonical_raw:
        scenario = _scenario_key(raw.get("scenario"))
        terminal = str(raw.get("terminal_classification") or "")
        if terminal not in runtime_terminal:
            raise ActiveRiskDecisionReportError(
                f"cannot reconstruct runtime behavior terminal {terminal!r}"
            )
        by_scenario[scenario].append(
            {
                "start": int(raw["start_day"]),
                "terminal": runtime_terminal[terminal],
                "accepted": int(raw.get("accepted_events", 0)),
                "skipped": int(raw.get("skipped_events", 0)),
                "quantity_path": [
                    [
                        str(decision.get("event_id") or ""),
                        int(decision.get("quantity", 0)),
                        str(decision.get("decision_status") or ""),
                    ]
                    # The legacy runtime fingerprint was created directly
                    # from AccountPolicyEpisode.risk_allocation_path.  The
                    # enriched daily projection contains the same decision
                    # multiset, but its diagnostic sort deliberately lacks
                    # the component-priority tie-break for coincident
                    # timestamps.  Reconstruct the legacy hash from its exact
                    # persisted causal source, not from that derived order.
                    for decision in list(raw.get("risk_allocation_path") or ())
                ],
            }
        )
    return canonical_hash(
        {
            "normal": by_scenario["normal"],
            "stressed": by_scenario["stressed"],
        }
    )


def _expanded_behavior_observation(
    *,
    raw: Mapping[str, Any],
    stage: str,
    attribution: Mapping[str, Any],
) -> ExpandedBehaviorObservation:
    """Project one canonical raw episode into exact and similarity evidence.

    The exact digest intentionally excludes the policy identifier.  Two books
    can therefore be recognized as execution-equivalent, while every account
    trajectory, emitted routing decision, suppression action, and admitted
    source-trade contribution remains bound to the digest.
    """

    scenario = _scenario_key(raw.get("scenario"))
    start_day = int(raw["start_day"])
    routing = _canonical_daily_routing(raw)
    accepted_trade_rows = attribution.get("accepted_trade_rows")
    if not isinstance(accepted_trade_rows, list):
        raise ActiveRiskDecisionReportError(
            f"expanded behavior {scenario} {start_day} lacks accepted trades"
        )

    routing_rows = [
        {
            str(key): value
            for key, value in sorted(decision.items())
            if str(key) != "policy_id"
        }
        for decision in routing
    ]
    daily_account_rows = [
        {
            "session_day": int(day["session_day"]),
            "realized_pnl": day.get("realized_pnl"),
            "unrealized_pnl": day.get("unrealized_pnl"),
            "day_pnl": day.get("day_pnl"),
            "balance": day.get("balance"),
            "mll_floor": day.get("mll_floor"),
            "closing_mll_buffer": day.get("closing_mll_buffer"),
            "minimum_mll_buffer": day.get("minimum_mll_buffer"),
            "consistency_ok": bool(day.get("consistency_ok")),
            "target_progress": day.get("target_progress"),
            "costs": day.get("costs"),
            "cumulative_costs": day.get("cumulative_costs"),
            "conflicts": list(day.get("conflicts") or ()),
            "exposure": dict(day.get("exposure") or {}),
            "component_attribution": dict(
                sorted((day.get("component_attribution") or {}).items())
            ),
        }
        for day in raw.get("daily_path") or ()
    ]
    normalized_accepted_rows = sorted(
        (
            {
                "component_id": str(row["component_id"]),
                "source_trade_id": str(row["source_trade_id"]),
                "base_quantity": int(row["base_quantity"]),
                "requested_quantity": int(row["requested_quantity"]),
                "admitted_quantity": int(row["admitted_quantity"]),
                "source_net_pnl": _required_float(
                    row["source_net_pnl"], label="expanded source-trade net"
                ),
                "account_net_pnl": _required_float(
                    row["account_net_pnl"], label="expanded account-trade net"
                ),
            }
            for row in accepted_trade_rows
        ),
        key=lambda row: (
            row["component_id"],
            row["source_trade_id"],
            row["admitted_quantity"],
        ),
    )
    exact_payload = {
        "schema": _EXPANDED_BEHAVIOR_FINGERPRINT_SCHEMA,
        "scenario": scenario,
        "start_day": start_day,
        "stage": stage,
        "terminal_classification": str(
            raw.get("terminal_classification") or ""
        ),
        "account_summary": {
            "end_day": raw.get("end_day"),
            "duration_trading_days": raw.get(
                "duration_trading_days", raw.get("eligible_days")
            ),
            "active_trading_days": raw.get(
                "active_trading_days", raw.get("traded_days")
            ),
            "calendar_days": (
                int(raw.get("end_day", start_day)) - start_day + 1
            ),
            "days_to_target": raw.get("days_to_target"),
            "projected_active_days_to_target": (
                _float(raw.get("traded_days")) / _float(raw.get("target_progress"))
                if _float(raw.get("target_progress")) > 0.0
                else None
            ),
            "projected_calendar_days_to_target": (
                (int(raw.get("end_day", start_day)) - start_day + 1)
                / _float(raw.get("target_progress"))
                if _float(raw.get("target_progress")) > 0.0
                else None
            ),
            **{
                field_name: raw.get(field_name)
                for field_name in (
                    "net_pnl",
                    "target_progress",
                    "maximum_target_progress",
                    "minimum_mll_buffer",
                    "consistency_ok",
                    "accepted_events",
                    "skipped_events",
                    "maximum_mini_equivalent",
                    "maximum_net_directional_exposure",
                )
            },
        },
        "daily_account_trajectory": daily_account_rows,
        "emitted_routing_and_suppression": routing_rows,
        "admitted_source_trades": normalized_accepted_rows,
        "component_contribution": dict(
            sorted((raw.get("component_contribution") or {}).items())
        ),
    }

    terminal = str(raw.get("terminal_classification") or "")
    terminal_code = (
        1 if terminal == "TARGET_REACHED" else -1 if terminal == "MLL_BREACHED" else 0
    )
    status_counts = Counter(
        str(decision.get("decision_status") or "UNKNOWN")
        for decision in routing
    )
    emitted = len(routing)
    accepted = sum(bool(decision.get("allow")) for decision in routing)
    duration_trading_days = int(
        raw.get("duration_trading_days", raw.get("eligible_days", 0))
    )
    active_trading_days = int(
        raw.get("active_trading_days", raw.get("traded_days", 0))
    )
    calendar_days = int(
        raw.get(
            "calendar_days",
            int(raw.get("end_day", start_day)) - start_day + 1,
        )
    )
    component_values = [
        _required_float(value, label="expanded component contribution")
        for value in (raw.get("component_contribution") or {}).values()
    ]
    component_abs_total = sum(abs(value) for value in component_values)
    component_positive_total = sum(max(value, 0.0) for value in component_values)
    feature_values = [
        _float(raw.get("target_progress")),
        _float(raw.get("maximum_target_progress")),
        _float(raw.get("net_pnl")) / 9000.0,
        _float(raw.get("minimum_mll_buffer")) / 4500.0,
        float(bool(raw.get("consistency_ok"))),
        accepted / emitted if emitted else 0.0,
        (emitted - accepted) / emitted if emitted else 0.0,
        active_trading_days / max(duration_trading_days, 1),
        calendar_days / 126.0,
        _float(raw.get("maximum_mini_equivalent")) / 15.0,
        _float(raw.get("maximum_net_directional_exposure")) / 15.0,
        float(terminal_code),
        status_counts["SIZE_REDUCED"] / emitted if emitted else 0.0,
        status_counts["CONFLICT_REJECTED"] / emitted if emitted else 0.0,
        status_counts["CONTRACT_LIMIT_REJECTED"] / emitted if emitted else 0.0,
        status_counts["MLL_RISK_REJECTED"] / emitted if emitted else 0.0,
        sum(
            _float(decision.get("foregone_realized_pnl_ex_post"))
            for decision in routing
        )
        / 9000.0,
        len(component_values) / 18.0,
        (
            max((max(value, 0.0) for value in component_values), default=0.0)
            / component_positive_total
            if component_positive_total > 0.0
            else 0.0
        ),
        (
            sum((abs(value) / component_abs_total) ** 2 for value in component_values)
            if component_abs_total > 0.0
            else 0.0
        ),
        len(normalized_accepted_rows) / max(emitted, 1),
    ]
    daily = list(raw.get("daily_path") or ())
    for field_name, scale in (
        ("realized_pnl", 9000.0),
        ("unrealized_pnl", 4500.0),
        ("target_progress", 1.0),
        ("closing_mll_buffer", 4500.0),
        ("day_pnl", 9000.0),
        ("cumulative_costs", 9000.0),
    ):
        feature_values.extend(value / scale for value in _sample_path(daily, field_name))
    bounded_features = tuple(
        max(-4.0, min(4.0, float(value))) for value in feature_values
    )
    routing_tuples = frozenset(
        (
            scenario,
            start_day,
            str(row["component_id"]),
            str(row["event_id"]),
            int(row["requested_quantity"]),
            int(row["quantity"]),
            str(row["decision_status"]),
            str(row.get("reason") or ""),
            str(row.get("binding_constraint") or ""),
        )
        for row in routing_rows
    )
    admitted_trade_tuples = frozenset(
        (
            scenario,
            start_day,
            str(row["component_id"]),
            str(row["source_trade_id"]),
            int(row["admitted_quantity"]),
        )
        for row in normalized_accepted_rows
    )
    return ExpandedBehaviorObservation(
        scenario=scenario,
        start_day=start_day,
        stage=stage,
        exact_row_hash=canonical_hash(exact_payload),
        feature_values=bounded_features,
        terminal_code=terminal_code,
        routing_tuples=routing_tuples,
        admitted_trade_tuples=admitted_trade_tuples,
    )


def _cumulative_behavior_profile(
    observations: Mapping[tuple[str, int], ExpandedBehaviorObservation],
    *,
    declared_stage_start_counts: Mapping[str, Mapping[str, int]] | None = None,
    runtime_stage_behavior_hashes: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build an order-independent cumulative account/trade behavior profile."""

    ordered = sorted(
        observations.values(), key=lambda row: (row.scenario, row.start_day)
    )
    if len(ordered) != len(observations):
        raise ActiveRiskDecisionReportError(
            "cumulative behavior observation identity drift"
        )
    keys = tuple((row.scenario, row.start_day) for row in ordered)
    if len(set(keys)) != len(keys):
        raise ActiveRiskDecisionReportError(
            "cumulative behavior contains duplicate scenario/start keys"
        )
    if any(row.scenario not in SCENARIOS for row in ordered):
        raise ActiveRiskDecisionReportError(
            "cumulative behavior contains an unsupported cost scenario"
        )
    feature_widths = {len(row.feature_values) for row in ordered}
    if len(feature_widths) > 1:
        raise ActiveRiskDecisionReportError(
            "cumulative behavior feature width drift"
        )
    derived_stage_counts: dict[str, dict[str, int]] = {}
    for row in ordered:
        derived_stage_counts.setdefault(
            row.stage, {scenario: 0 for scenario in SCENARIOS}
        )[row.scenario] += 1
    if declared_stage_start_counts is not None:
        declared = {
            str(stage): {
                scenario: int(counts.get(scenario, -1))
                for scenario in SCENARIOS
            }
            for stage, counts in declared_stage_start_counts.items()
        }
        if declared != derived_stage_counts:
            raise ActiveRiskDecisionReportError(
                "cumulative behavior stage/start coverage drift"
            )
    exact_rows = [
        [row.scenario, row.start_day, row.stage, row.exact_row_hash]
        for row in ordered
    ]
    vector = np.asarray(
        [value for row in ordered for value in row.feature_values],
        dtype=np.float32,
    )
    terminals = tuple(row.terminal_code for row in ordered)
    routing_tuples = frozenset(
        item for row in ordered for item in row.routing_tuples
    )
    admitted_trade_tuples = frozenset(
        item for row in ordered for item in row.admitted_trade_tuples
    )
    exact_fingerprint = canonical_hash(
        {
            "schema": _EXPANDED_BEHAVIOR_FINGERPRINT_SCHEMA,
            "feature_schema": _EXPANDED_BEHAVIOR_FEATURE_SCHEMA,
            "episode_rows": exact_rows,
        }
    )
    rounded_vector = [round(float(value), 8) for value in vector]
    runtime_hashes = dict(runtime_stage_behavior_hashes or {})
    unexpected_runtime_stages = set(runtime_hashes) - set(derived_stage_counts)
    if unexpected_runtime_stages:
        raise ActiveRiskDecisionReportError(
            "runtime behavior hashes contain stages without raw observations"
        )
    runtime_cumulative: str | None = None
    if runtime_hashes:
        stage_order = [
            stage for stage in ("stage3", "stage4", "stage5") if stage in runtime_hashes
        ]
        if set(stage_order) != set(runtime_hashes):
            raise ActiveRiskDecisionReportError(
                "runtime behavior hashes contain an unsupported stage"
            )
        if stage_order != list(runtime_hashes):
            # Dict insertion order is not authoritative; only the fixed runtime
            # merge order is.  This branch documents that deliberate choice.
            runtime_hashes = {stage: runtime_hashes[stage] for stage in stage_order}
        runtime_cumulative = runtime_hashes[stage_order[0]]
        for stage in stage_order[1:]:
            runtime_cumulative = canonical_hash(
                [runtime_cumulative, runtime_hashes[stage]]
            )
    stage_scope = "_PLUS_".join(stage.upper() for stage in sorted(derived_stage_counts))
    public = {
        "scope": (
            f"EXACT_CUMULATIVE_{stage_scope}_CANONICAL_90_DAY_ACCOUNT_"
            "TRAJECTORY_ROUTING_SUPPRESSION_AND_ADMITTED_TRADES"
        ),
        "fingerprint_schema": _EXPANDED_BEHAVIOR_FINGERPRINT_SCHEMA,
        "feature_schema": _EXPANDED_BEHAVIOR_FEATURE_SCHEMA,
        "authoritative_raw_account_trade_behavior_fingerprint": exact_fingerprint,
        "runtime_legacy_cumulative_behavior_fingerprint_rederived": runtime_cumulative,
        "runtime_stage_behavior_fingerprints_rederived": runtime_hashes,
        "observation_count": len(ordered),
        "per_scenario_observation_count": {
            scenario: sum(row.scenario == scenario for row in ordered)
            for scenario in SCENARIOS
        },
        "stage_start_counts": derived_stage_counts,
        "episode_key_sha256": canonical_hash(keys),
        "feature_width_per_episode": next(iter(feature_widths), 0),
        "feature_vector_sha256": canonical_hash(rounded_vector),
        "routing_decision_tuple_count": len(routing_tuples),
        "routing_decision_tuple_sha256": canonical_hash(sorted(routing_tuples)),
        "admitted_trade_tuple_count": len(admitted_trade_tuples),
        "admitted_trade_tuple_sha256": canonical_hash(
            sorted(admitted_trade_tuples)
        ),
        "full_daily_account_trajectory_bound": True,
        "emitted_routing_and_suppression_bound": True,
        "admitted_source_trade_contribution_bound": True,
        "policy_id_excluded_from_behavior_fingerprint": True,
    }
    return {
        "public": public,
        "episode_keys": keys,
        "vector": vector,
        "terminal_codes": terminals,
        "routing_tuples": routing_tuples,
        "admitted_trade_tuples": admitted_trade_tuples,
    }


def _cumulative_behavior_similarity(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> tuple[float, float, float, float, float, bool]:
    """Compare aligned cumulative 192-start account/trade trajectories."""

    if tuple(left["episode_keys"]) != tuple(right["episode_keys"]):
        return 0.0, math.inf, 0.0, 0.0, 0.0, False
    left_vector = np.asarray(left["vector"], dtype=np.float64)
    right_vector = np.asarray(right["vector"], dtype=np.float64)
    left_terminal = tuple(left["terminal_codes"])
    right_terminal = tuple(right["terminal_codes"])
    if left_vector.shape != right_vector.shape or len(left_terminal) != len(
        right_terminal
    ):
        return 0.0, math.inf, 0.0, 0.0, 0.0, False
    if left_vector.size:
        left_deviation = left_vector - float(np.mean(left_vector))
        right_deviation = right_vector - float(np.mean(right_vector))
        denominator = float(
            np.linalg.norm(left_deviation) * np.linalg.norm(right_deviation)
        )
        correlation = (
            float(np.dot(left_deviation, right_deviation) / denominator)
            if denominator > 1e-12
            else float(
                np.allclose(left_vector, right_vector, atol=1e-12, rtol=0.0)
            )
        )
        rmse = float(np.sqrt(np.mean(np.square(left_vector - right_vector))))
    else:
        correlation, rmse = 1.0, 0.0
    terminal_agreement = sum(
        int(one == two)
        for one, two in zip(left_terminal, right_terminal, strict=True)
    ) / max(len(left_terminal), 1)
    routing_jaccard = _jaccard(
        frozenset(left["routing_tuples"]), frozenset(right["routing_tuples"])
    )
    admitted_trade_jaccard = _jaccard(
        frozenset(left["admitted_trade_tuples"]),
        frozenset(right["admitted_trade_tuples"]),
    )
    thresholds = _CUMULATIVE_BEHAVIOR_CLUSTER_THRESHOLDS
    similar = (
        correlation >= thresholds["minimum_account_vector_correlation"]
        and rmse <= thresholds["maximum_account_vector_rmse"]
        and terminal_agreement >= thresholds["minimum_terminal_agreement"]
        and routing_jaccard >= thresholds["minimum_routing_jaccard"]
        and admitted_trade_jaccard
        >= thresholds["minimum_admitted_trade_jaccard"]
    )
    return (
        correlation,
        rmse,
        terminal_agreement,
        routing_jaccard,
        admitted_trade_jaccard,
        similar,
    )


def _cumulative_pair_similarity_decision(row: Mapping[str, Any]) -> bool:
    thresholds = _CUMULATIVE_BEHAVIOR_CLUSTER_THRESHOLDS
    values = {
        name: _required_float(row.get(name), label=f"pairwise {name}")
        for name in (
            "account_vector_correlation",
            "account_vector_rmse",
            "terminal_agreement",
            "routing_jaccard",
            "admitted_trade_jaccard",
        )
    }
    if (
        not -1.0000001 <= values["account_vector_correlation"] <= 1.0000001
        or values["account_vector_rmse"] < 0.0
        or any(
            not 0.0 <= values[name] <= 1.0
            for name in (
                "terminal_agreement",
                "routing_jaccard",
                "admitted_trade_jaccard",
            )
        )
    ):
        raise ActiveRiskDecisionReportError(
            "cumulative pairwise diagnostic metric range drift"
        )
    return bool(
        values["account_vector_correlation"]
        >= thresholds["minimum_account_vector_correlation"]
        and values["account_vector_rmse"]
        <= thresholds["maximum_account_vector_rmse"]
        and values["terminal_agreement"]
        >= thresholds["minimum_terminal_agreement"]
        and values["routing_jaccard"]
        >= thresholds["minimum_routing_jaccard"]
        and values["admitted_trade_jaccard"]
        >= thresholds["minimum_admitted_trade_jaccard"]
    )


def _complete_link_groups_from_pairwise(
    candidate_ids: Sequence[str], pairwise: Sequence[Mapping[str, Any]]
) -> list[list[str]]:
    """Rebuild the deterministic complete-link partition from published pairs."""

    ordered_ids = sorted(str(value) for value in candidate_ids)
    if len(ordered_ids) != len(set(ordered_ids)) or not all(ordered_ids):
        raise ActiveRiskDecisionReportError(
            "cumulative pairwise clustering has duplicate or empty candidates"
        )
    expected_pairs = {
        (left, right)
        for index, left in enumerate(ordered_ids)
        for right in ordered_ids[index + 1 :]
    }
    decisions: dict[tuple[str, str], bool] = {}
    for raw in pairwise:
        left = str(raw.get("left_policy_id") or "")
        right = str(raw.get("right_policy_id") or "")
        key = tuple(sorted((left, right)))
        if (
            not left
            or not right
            or left == right
            or key not in expected_pairs
            or key in decisions
        ):
            raise ActiveRiskDecisionReportError(
                "cumulative pairwise diagnostic identity drift"
            )
        derived = _cumulative_pair_similarity_decision(raw)
        if raw.get("similar") is not derived:
            raise ActiveRiskDecisionReportError(
                f"cumulative pairwise similarity decision drift for {key}"
            )
        decisions[key] = derived
    if set(decisions) != expected_pairs:
        raise ActiveRiskDecisionReportError(
            "cumulative pairwise diagnostics do not cover every finalist pair"
        )
    groups: list[list[str]] = []
    for candidate_id in ordered_ids:
        for group in groups:
            if all(
                decisions[tuple(sorted((candidate_id, member)))]
                for member in group
            ):
                group.append(candidate_id)
                break
        else:
            groups.append([candidate_id])
    return [sorted(group) for group in groups]


def _validate_cumulative_behavior_clustering_payload(
    payload: Mapping[str, Any], *, expected_candidate_ids: Sequence[str]
) -> None:
    """Fail closed if published pairs cannot regenerate the reported partition."""

    candidate_ids = sorted(str(value) for value in expected_candidate_ids)
    pairwise = payload.get("pairwise_diagnostics")
    if not isinstance(pairwise, list):
        raise ActiveRiskDecisionReportError(
            "cumulative clustering pairwise diagnostics are absent"
        )
    expected_count = len(candidate_ids) * (len(candidate_ids) - 1) // 2
    expected_pair_order = [
        (left, right)
        for index, left in enumerate(candidate_ids)
        for right in candidate_ids[index + 1 :]
    ]
    observed_pair_order = [
        (
            str(row.get("left_policy_id") or ""),
            str(row.get("right_policy_id") or ""),
        )
        for row in pairwise
        if isinstance(row, Mapping)
    ]
    if (
        int(payload.get("pairwise_diagnostic_count", -1)) != len(pairwise)
        or int(payload.get("expected_pairwise_diagnostic_count", -1))
        != expected_count
        or len(pairwise) != expected_count
        or payload.get("pairwise_coverage_complete") is not True
        or payload.get("pairwise_similarity_decisions_metric_rederived") is not True
        or observed_pair_order != expected_pair_order
        or dict(payload.get("complete_link_thresholds") or {})
        != _CUMULATIVE_BEHAVIOR_CLUSTER_THRESHOLDS
    ):
        raise ActiveRiskDecisionReportError(
            "cumulative clustering pairwise coverage drift"
        )
    if canonical_hash(pairwise) != str(
        payload.get("pairwise_diagnostics_sha256") or ""
    ):
        raise ActiveRiskDecisionReportError(
            "cumulative clustering pairwise diagnostics hash drift"
        )
    derived_groups = _complete_link_groups_from_pairwise(candidate_ids, pairwise)
    cluster_rows = payload.get("clusters")
    membership = payload.get("membership")
    if not isinstance(cluster_rows, list) or not isinstance(membership, Mapping):
        raise ActiveRiskDecisionReportError(
            "cumulative clustering partition publication is malformed"
        )
    if not all(isinstance(row, Mapping) for row in cluster_rows):
        raise ActiveRiskDecisionReportError(
            "cumulative clustering contains a malformed cluster row"
        )
    published_groups = sorted(
        (sorted(str(value) for value in row.get("member_ids") or ()) for row in cluster_rows),
        key=lambda values: tuple(values),
    )
    cluster_ids = [str(row.get("cluster_id") or "") for row in cluster_rows]
    derived_groups = sorted(derived_groups, key=lambda values: tuple(values))
    if (
        int(payload.get("cluster_count", -1)) != len(cluster_rows)
        or len(cluster_rows) != len(derived_groups)
        or not all(cluster_ids)
        or len(cluster_ids) != len(set(cluster_ids))
        or any(
            int(row.get("member_count", -1))
            != len(list(row.get("member_ids") or ()))
            or len(list(row.get("member_ids") or ()))
            != len(set(str(value) for value in row.get("member_ids") or ()))
            for row in cluster_rows
        )
        or published_groups != derived_groups
        or canonical_hash(derived_groups)
        != str(payload.get("complete_link_partition_sha256") or "")
        or payload.get("complete_link_partition_rederived_from_published_pairwise")
        is not True
    ):
        raise ActiveRiskDecisionReportError(
            "cumulative clustering published complete-link partition drift"
        )
    published_ids = {
        str(value) for group in published_groups for value in group
    }
    if published_ids != set(candidate_ids) or set(membership) != set(candidate_ids):
        raise ActiveRiskDecisionReportError(
            "cumulative clustering partition candidate coverage drift"
        )
    for row in cluster_rows:
        cluster_id = str(row.get("cluster_id") or "")
        members = [str(value) for value in row.get("member_ids") or ()]
        if not cluster_id or any(str(membership.get(value) or "") != cluster_id for value in members):
            raise ActiveRiskDecisionReportError(
                "cumulative clustering membership/cluster identity drift"
            )


def _cumulative_behavior_clusters(
    profiles: Mapping[str, Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Complete-link clustering of cumulative account and admitted-trade paths."""

    if set(profiles) != set(candidates):
        raise ActiveRiskDecisionReportError(
            "cumulative behavior profile/candidate identity drift"
        )
    key_sets = {tuple(profile["episode_keys"]) for profile in profiles.values()}
    if len(key_sets) > 1:
        raise ActiveRiskDecisionReportError(
            "cumulative finalists do not share identical scenario/start keys"
        )
    pair_diagnostics: dict[
        tuple[str, str], tuple[float, float, float, float, float, bool]
    ] = {}

    def comparison(
        left: str, right: str
    ) -> tuple[float, float, float, float, float, bool]:
        key = tuple(sorted((left, right)))
        if key not in pair_diagnostics:
            pair_diagnostics[key] = _cumulative_behavior_similarity(
                profiles[left], profiles[right]
            )
        return pair_diagnostics[key]

    candidate_ids = sorted(profiles)
    pairwise_diagnostics: list[dict[str, Any]] = []
    for index, left in enumerate(candidate_ids):
        for right in candidate_ids[index + 1 :]:
            diagnostic = comparison(left, right)
            pairwise_diagnostics.append(
                {
                    "left_policy_id": left,
                    "right_policy_id": right,
                    "account_vector_correlation": diagnostic[0],
                    "account_vector_rmse": diagnostic[1],
                    "terminal_agreement": diagnostic[2],
                    "routing_jaccard": diagnostic[3],
                    "admitted_trade_jaccard": diagnostic[4],
                    "similar": diagnostic[5],
                }
            )
    groups = _complete_link_groups_from_pairwise(
        candidate_ids, pairwise_diagnostics
    )

    rows: list[dict[str, Any]] = []
    membership: dict[str, str] = {}
    for members in groups:
        ordered = sorted(members)
        cluster_id = "expanded_economic_behavior_" + canonical_hash(
            {
                "members": ordered,
                "fingerprints": [
                    profiles[member]["public"][
                        "authoritative_raw_account_trade_behavior_fingerprint"
                    ]
                    for member in ordered
                ],
            }
        )[:20]
        representative = sorted(
            ordered,
            key=lambda value: (
                -_float(candidates[value]["stressed"].get("pass_rate")),
                -_float(
                    candidates[value]["stressed"].get("target_progress_p25")
                ),
                -_float(candidates[value]["stressed"].get("net_total")),
                _float(candidates[value]["stressed"].get("mll_breach_rate")),
                value,
            ),
        )[0]
        diagnostics = [
            comparison(left, right)
            for index, left in enumerate(ordered)
            for right in ordered[index + 1 :]
        ]
        rows.append(
            {
                "cluster_id": cluster_id,
                "member_ids": ordered,
                "member_count": len(ordered),
                "representative_id": representative,
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
                "minimum_admitted_trade_jaccard": (
                    min(value[4] for value in diagnostics) if diagnostics else 1.0
                ),
                "complete_link_thresholds": dict(
                    _CUMULATIVE_BEHAVIOR_CLUSTER_THRESHOLDS
                ),
            }
        )
        for candidate_id in ordered:
            membership[candidate_id] = cluster_id
    counts = {
        tuple(
            int(profile["public"]["per_scenario_observation_count"][scenario])
            for scenario in SCENARIOS
        )
        for profile in profiles.values()
    }
    full_192 = counts == {(192, 192)} and all(
        profile["public"]["stage_start_counts"]
        == {
            "stage3": {"normal": 48, "stressed": 48},
            "stage4": {"normal": 48, "stressed": 48},
            "stage5": {"normal": 96, "stressed": 96},
        }
        for profile in profiles.values()
    )
    expected_pairwise_count = len(candidate_ids) * (len(candidate_ids) - 1) // 2
    partition = sorted(
        (sorted(group) for group in groups), key=lambda values: tuple(values)
    )
    result = {
        "scope": (
            "CUMULATIVE_192_STARTS_PER_SCENARIO_STAGE3_48_PLUS_STAGE4_48_"
            "PLUS_STAGE5_96_ACCOUNT_TRAJECTORY_ROUTING_SUPPRESSION_AND_"
            "ADMITTED_TRADE_BEHAVIOR"
            if full_192
            else "CUMULATIVE_AVAILABLE_FROZEN_STARTS_ACCOUNT_TRAJECTORY_"
            "ROUTING_SUPPRESSION_AND_ADMITTED_TRADE_BEHAVIOR"
        ),
        "algorithm": "DETERMINISTIC_COMPLETE_LINK_FIXED_THRESHOLDS_V1",
        "complete_link_thresholds": dict(
            _CUMULATIVE_BEHAVIOR_CLUSTER_THRESHOLDS
        ),
        "cluster_count": len(rows),
        "clusters": rows,
        "membership": membership,
        "pairwise_diagnostics": pairwise_diagnostics,
        "pairwise_diagnostic_count": len(pairwise_diagnostics),
        "expected_pairwise_diagnostic_count": expected_pairwise_count,
        "pairwise_diagnostics_sha256": canonical_hash(pairwise_diagnostics),
        "pairwise_coverage_complete": (
            len(pairwise_diagnostics) == expected_pairwise_count
        ),
        "pairwise_similarity_decisions_metric_rederived": True,
        "complete_link_partition_sha256": canonical_hash(partition),
        "complete_link_partition_rederived_from_published_pairwise": True,
        "source_signal_or_trade_ledger_summary_only": False,
        "overlapping_starts_claimed_independent": False,
        "full_192_start_contract_satisfied": full_192,
    }
    _validate_cumulative_behavior_clustering_payload(
        result, expected_candidate_ids=candidate_ids
    )
    return result


@dataclass
class ExpandedFinalistRawAccumulator:
    policy_id: str
    blocks: tuple[BlockSpec, ...]
    component_trade_index: Mapping[tuple[str, str], Mapping[str, Any]]
    expected_xfa_profile: Mapping[str, Any]
    start_keys: dict[str, set[int]] = field(
        default_factory=lambda: {scenario: set() for scenario in SCENARIOS}
    )
    stage_start_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    runtime_stage_behavior_hashes: dict[str, str] = field(default_factory=dict)
    behavior_observations: dict[
        tuple[str, int], ExpandedBehaviorObservation
    ] = field(default_factory=dict)
    block_results: dict[str, dict[str, BlockAccumulator]] = field(init=False)
    risk_values: dict[str, list[float]] = field(
        default_factory=lambda: {scenario: [] for scenario in SCENARIOS}
    )
    risk_groups: dict[str, dict[str, list[float]]] = field(
        default_factory=lambda: {
            scenario: {
                key: [] for key in ("zero", "one", "two", "three_or_more")
            }
            for scenario in SCENARIOS
        }
    )
    suppression: SuppressionAccumulator = field(default_factory=SuppressionAccumulator)
    suppression_by_scenario: dict[str, SuppressionAccumulator] = field(
        default_factory=lambda: {
            scenario: SuppressionAccumulator() for scenario in SCENARIOS
        }
    )
    suppression_by_scenario_and_component: dict[
        str, defaultdict[str, SuppressionAccumulator]
    ] = field(
        default_factory=lambda: {
            scenario: defaultdict(SuppressionAccumulator) for scenario in SCENARIOS
        }
    )
    component_contribution: dict[str, defaultdict[str, float]] = field(
        default_factory=lambda: {
            scenario: defaultdict(float) for scenario in SCENARIOS
        }
    )
    source_trade_contribution: dict[
        str, defaultdict[tuple[str, str], float]
    ] = field(
        default_factory=lambda: {
            scenario: defaultdict(float) for scenario in SCENARIOS
        }
    )
    accepted_trade_observations: dict[str, list[float]] = field(
        default_factory=lambda: {scenario: [] for scenario in SCENARIOS}
    )
    day_pnl_observations: dict[str, list[float]] = field(
        default_factory=lambda: {scenario: [] for scenario in SCENARIOS}
    )
    day_pnl_by_session_day: dict[str, defaultdict[int, float]] = field(
        default_factory=lambda: {
            scenario: defaultdict(float) for scenario in SCENARIOS
        }
    )
    lifecycle: dict[str, dict[str, LifecyclePathAccumulator]] = field(
        default_factory=lambda: {
            scenario: {path: LifecyclePathAccumulator() for path in PATHS}
            for scenario in SCENARIOS
        }
    )

    def __post_init__(self) -> None:
        self.block_results = {
            scenario: {
                block.block_id: BlockAccumulator() for block in self.blocks
            }
            for scenario in SCENARIOS
        }

    def add_stage(
        self,
        *,
        stage: str,
        compact: Mapping[str, Any],
        canonical_raw: Sequence[Mapping[str, Any]],
        full_raw: Sequence[Mapping[str, Any]],
        lifecycle_rows: Sequence[Mapping[str, Any]],
        expected_starts_per_scenario: int,
    ) -> None:
        if str(compact.get("policy_id") or "") != self.policy_id:
            raise ActiveRiskDecisionReportError(
                f"expanded raw accumulator policy drift for {self.policy_id}"
            )
        if stage in self.stage_start_counts or stage in self.runtime_stage_behavior_hashes:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {self.policy_id} duplicates {stage}"
            )
        runtime_behavior_hash = _runtime_behavior_fingerprint_from_raw(canonical_raw)
        self.runtime_stage_behavior_hashes[stage] = runtime_behavior_hash
        stage_counts: dict[str, int] = {}
        for scenario in SCENARIOS:
            selected = [
                raw
                for raw in canonical_raw
                if _scenario_key(raw.get("scenario")) == scenario
            ]
            keys = {int(raw["start_day"]) for raw in selected}
            if len(selected) != expected_starts_per_scenario or len(keys) != len(
                selected
            ):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} {stage} {scenario} "
                    "start coverage drift"
                )
            overlap = keys & self.start_keys[scenario]
            if overlap:
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} has cross-stage start overlap "
                    f"in {scenario}: {sorted(overlap)[:3]!r}"
                )
            self.start_keys[scenario].update(keys)
            stage_counts[scenario] = len(keys)
            for raw in selected:
                raw_label = (
                    f"expanded finalist {self.policy_id} {stage} {scenario} "
                    f"start {raw.get('start_day')}"
                )
                block_id = _block_for_day(int(raw["start_day"]), self.blocks)
                self.block_results[scenario][block_id].add(raw)
                for day in raw.get("daily_path") or ():
                    day_pnl = _required_float(
                        day.get("day_pnl"),
                        label=(
                            f"expanded finalist {self.policy_id} {stage} "
                            f"{scenario} day PnL"
                        ),
                    )
                    self.day_pnl_observations[scenario].append(day_pnl)
                    self.day_pnl_by_session_day[scenario][
                        int(day["session_day"])
                    ] += day_pnl
                attribution = _trade_attribution_from_routing(
                    raw,
                    component_trade_index=self.component_trade_index,
                    label=raw_label,
                )
                behavior_key = (scenario, int(raw["start_day"]))
                if behavior_key in self.behavior_observations:
                    raise ActiveRiskDecisionReportError(
                        f"expanded finalist {self.policy_id} duplicates cumulative "
                        f"behavior key {behavior_key}"
                    )
                self.behavior_observations[behavior_key] = (
                    _expanded_behavior_observation(
                        raw=raw,
                        stage=stage,
                        attribution=attribution,
                    )
                )
                for component_id, value in attribution["component_pnl"].items():
                    self.component_contribution[scenario][component_id] += value
                for source_key, value in attribution["source_trade_pnl"].items():
                    self.source_trade_contribution[scenario][source_key] += value
                self.accepted_trade_observations[scenario].extend(
                    attribution["accepted_trade_observations"]
                )
                for decision in _canonical_daily_routing(raw):
                    component_id = str(decision.get("component_id") or "")
                    if not component_id:
                        raise ActiveRiskDecisionReportError(
                            f"{raw_label} suppression lacks component identity"
                        )
                    self.suppression.add_decision(decision)
                    self.suppression_by_scenario[scenario].add_decision(decision)
                    self.suppression_by_scenario_and_component[scenario][
                        component_id
                    ].add_decision(decision)
        self.stage_start_counts[stage] = stage_counts

        for raw in canonical_raw:
            scenario = _scenario_key(raw.get("scenario"))
            for decision in _canonical_daily_routing(raw):
                for side in ("risk_before", "risk_after"):
                    audit = decision.get(side)
                    if not isinstance(audit, Mapping) or audit.get(
                        "utilisation"
                    ) is None:
                        continue
                    utilisation = _required_float(
                        audit["utilisation"],
                        label=(
                            f"expanded finalist {self.policy_id} {stage} "
                            "risk utilisation"
                        ),
                    )
                    active = int(audit.get("active_sleeve_count", 0))
                    group = (
                        "zero"
                        if active == 0
                        else "one"
                        if active == 1
                        else "two"
                        if active == 2
                        else "three_or_more"
                    )
                    self.risk_values[scenario].append(utilisation)
                    self.risk_groups[scenario][group].append(utilisation)

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
                    f"expanded finalist {self.policy_id} {stage} malformed lifecycle"
                )
            key = (
                _scenario_key(lifecycle_row.get("scenario")),
                int(lifecycle_row["combine_start_day"]),
            )
            if key in lifecycle_by_key:
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} {stage} duplicate lifecycle {key}"
                )
            lifecycle_by_key[key] = lifecycle_row
        if set(lifecycle_by_key) != full_pass_keys:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {self.policy_id} {stage} FULL-pass/XFA bijection drift"
            )
        for raw in full_raw:
            scenario = _scenario_key(raw.get("scenario"))
            for path in PATHS:
                self.lifecycle[scenario][path].add_combine_episode(raw)
        for (scenario, start_day), lifecycle_row in lifecycle_by_key.items():
            full_episode = full_by_key[(scenario, start_day)]
            if int(lifecycle_row.get("combine_end_day", -1)) != int(
                full_episode.get("end_day", -2)
            ):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} {stage} XFA/Combine "
                    "end-day linkage drift"
                )
            try:
                sealed_lifecycle = _active_pool_lifecycle_evidence(
                    lifecycle_row,
                    expected_policy_id=self.policy_id,
                    expected_scenario=str(full_episode["scenario"]),
                    expected_start_day=start_day,
                )
            except (ActiveRiskRuntimeError, KeyError, TypeError, ValueError) as exc:
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} {stage} XFA validation "
                    f"failed: {exc}"
                ) from exc
            if dict(sealed_lifecycle.get("xfa_profile") or {}) != dict(
                self.expected_xfa_profile
            ):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} {stage} XFA profile "
                    "differs from the frozen finalist books"
                )
            combine_end_day = int(sealed_lifecycle["combine_end_day"])
            xfa_start_day = (
                None
                if sealed_lifecycle["xfa_start_day"] is None
                else int(sealed_lifecycle["xfa_start_day"])
            )
            if xfa_start_day is not None and xfa_start_day <= combine_end_day:
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} {stage} XFA start is "
                    "not strictly after Combine end"
                )
            for path in PATHS:
                value = sealed_lifecycle.get(path)
                if not isinstance(value, Mapping):
                    raise ActiveRiskDecisionReportError(
                        f"expanded finalist {self.policy_id} {stage} lacks {path} path"
                    )
                if int(value.get("requested_horizon_days", -1)) != int(
                    sealed_lifecycle["xfa_horizon_days"]
                ):
                    raise ActiveRiskDecisionReportError(
                        f"expanded finalist {self.policy_id} {stage} {path} "
                        "requested XFA horizon drift"
                    )
                payout_reconciliation = _validate_xfa_path_accounting(
                    value,
                    label=(
                        f"expanded finalist {self.policy_id} {stage} "
                        f"{scenario} {path}"
                    ),
                    policy_id=self.policy_id,
                    scenario=scenario,
                    combine_start_id=start_day,
                    combine_end_day=combine_end_day,
                    xfa_start_day=xfa_start_day,
                    rule_snapshot=sealed_lifecycle["rule_snapshot"],
                )
                self.lifecycle[scenario][path].add_path(
                    value,
                    payout_reconciliation=payout_reconciliation,
                )

    def _canonical_scenario_result(self, scenario: str) -> dict[str, Any]:
        """Re-derive one cumulative 90-day finalist summary from raw episodes."""

        blocks = self.block_results[scenario]

        def combined(field_name: str) -> list[float]:
            return [
                value
                for block_id in sorted(blocks)
                for value in getattr(blocks[block_id], field_name)
            ]

        episode_count = sum(value.episode_count for value in blocks.values())
        pass_count = sum(value.pass_count for value in blocks.values())
        mll_breach_count = sum(
            value.mll_breach_count for value in blocks.values()
        )
        censored_count = sum(value.censored_count for value in blocks.values())
        data_censored_count = sum(
            value.data_censored_count for value in blocks.values()
        )
        operational_count = sum(
            value.operational_horizon_not_reached_count
            for value in blocks.values()
        )
        consistency_ok_count = sum(
            value.consistency_ok_count for value in blocks.values()
        )
        hard_rule_count = (
            episode_count
            - pass_count
            - mll_breach_count
            - data_censored_count
            - operational_count
        )
        if min(
            episode_count,
            pass_count,
            mll_breach_count,
            censored_count,
            data_censored_count,
            operational_count,
            consistency_ok_count,
            hard_rule_count,
        ) < 0:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {self.policy_id} {scenario} raw count drift"
            )
        target_progress = combined("target_progress")
        maximum_target_progress = combined("maximum_target_progress")
        minimum_mll_buffer = combined("minimum_mll_buffer")
        net_pnl = combined("net_pnl")
        days_to_target = combined("days_to_target")
        duration_trading_days = combined("duration_trading_days")
        active_trading_days = combined("active_trading_days")
        calendar_days = combined("calendar_days")
        projected_active = combined("projected_active_days_to_target")
        projected_calendar = combined("projected_calendar_days_to_target")
        for field_name, values in (
            ("target progress", target_progress),
            ("maximum target progress", maximum_target_progress),
            ("minimum MLL buffer", minimum_mll_buffer),
            ("net PnL", net_pnl),
            ("duration trading days", duration_trading_days),
            ("active trading days", active_trading_days),
            ("calendar days", calendar_days),
        ):
            if len(values) != episode_count:
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {self.policy_id} {scenario} {field_name} "
                    "raw coverage drift"
                )
        if len(days_to_target) != pass_count:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {self.policy_id} {scenario} raw "
                "days-to-target/pass coverage drift"
            )
        denominator = max(episode_count, 1)
        evaluable_count = episode_count - data_censored_count
        by_block_net = {
            block_id: sum(value.net_pnl)
            for block_id, value in sorted(blocks.items())
        }
        pass_block_ids = sorted(
            block_id
            for block_id, value in blocks.items()
            if value.pass_count > 0
        )
        positive_block_total = sum(
            max(value, 0.0) for value in by_block_net.values()
        )
        terminal_distribution = {
            key: value
            for key, value in {
                "TARGET_REACHED": pass_count,
                "MLL_BREACHED": mll_breach_count,
                "HARD_RULE_FAILURE": hard_rule_count,
                "DATA_CENSORED": data_censored_count,
                "OPERATIONAL_HORIZON_NOT_REACHED": operational_count,
            }.items()
            if value
        }
        return {
            "scope": (
                "EXACT_CUMULATIVE_STAGE3_STAGE4_STAGE5_CANONICAL_90_DAY_"
                "RAW_EPISODES"
            ),
            "episode_count": episode_count,
            "pass_count": pass_count,
            "pass_rate": pass_count / denominator,
            "pass_rate_raw_lower_bound": pass_count / denominator,
            "pass_rate_evaluable": (
                pass_count / evaluable_count if evaluable_count else None
            ),
            "mll_breach_count": mll_breach_count,
            "mll_breach_rate": mll_breach_count / denominator,
            "mll_breach_rate_raw_lower_bound": mll_breach_count / denominator,
            "mll_breach_rate_evaluable": (
                mll_breach_count / evaluable_count if evaluable_count else None
            ),
            "censored_episode_count": censored_count,
            "data_censored_episode_count": data_censored_count,
            "operational_horizon_not_reached_count": operational_count,
            "combine_evaluable_episode_count": evaluable_count,
            "terminal_distribution": terminal_distribution,
            "consistency_ok_count": consistency_ok_count,
            "consistency_rate": consistency_ok_count / denominator,
            "target_progress_p25": _quantile(target_progress, 0.25),
            "target_progress_median": (
                statistics.median(target_progress) if target_progress else 0.0
            ),
            "maximum_target_progress": max(
                maximum_target_progress, default=0.0
            ),
            "net_total": sum(net_pnl),
            "net_median": statistics.median(net_pnl) if net_pnl else 0.0,
            "minimum_mll_buffer": min(minimum_mll_buffer, default=4500.0),
            "median_days_to_target": (
                statistics.median(days_to_target) if days_to_target else None
            ),
            "projected_active_days_to_target_median": (
                statistics.median(projected_active) if projected_active else None
            ),
            "projected_calendar_days_to_target_median": (
                statistics.median(projected_calendar)
                if projected_calendar
                else None
            ),
            "duration_trading_days_median": (
                statistics.median(duration_trading_days)
                if duration_trading_days
                else 0.0
            ),
            "active_trading_days_median": (
                statistics.median(active_trading_days)
                if active_trading_days
                else 0.0
            ),
            "calendar_days_median": (
                statistics.median(calendar_days) if calendar_days else 0.0
            ),
            "monthly_subscription_duration_proxy_median": (
                statistics.median(projected_calendar) / 30.0
                if projected_calendar
                else None
            ),
            "censoring_rate": censored_count / denominator,
            "pass_block_count": len(pass_block_ids),
            "pass_block_ids": pass_block_ids,
            "by_block_net": by_block_net,
            "maximum_block_profit_share": (
                max(
                    (max(value, 0.0) for value in by_block_net.values()),
                    default=0.0,
                )
                / positive_block_total
                if positive_block_total > 0.0
                else 0.0
            ),
            "target_progress_distribution": _distribution(target_progress),
            "maximum_target_progress_distribution": _distribution(
                maximum_target_progress
            ),
            "net_pnl_distribution": _distribution(net_pnl),
            "minimum_mll_buffer_distribution": _distribution(
                minimum_mll_buffer
            ),
            "days_to_target_distribution": _distribution(days_to_target),
            "duration_trading_days_distribution": _distribution(
                duration_trading_days
            ),
            "active_trading_days_distribution": _distribution(
                active_trading_days
            ),
            "calendar_days_distribution": _distribution(calendar_days),
            "projected_active_days_to_target_distribution": _distribution(
                projected_active
            ),
            "projected_calendar_days_to_target_distribution": _distribution(
                projected_calendar
            ),
        }

    def cumulative_behavior_profile(self) -> dict[str, Any]:
        return _cumulative_behavior_profile(
            self.behavior_observations,
            declared_stage_start_counts=self.stage_start_counts,
            runtime_stage_behavior_hashes=self.runtime_stage_behavior_hashes,
        )

    def to_dict(self) -> dict[str, Any]:
        def risk_view(
            values: Sequence[float], groups: Mapping[str, Sequence[float]]
        ) -> dict[str, Any]:
            return {
                "observation_count": len(values),
                **{
                    key: value
                    for key, value in _distribution(values).items()
                    if key != "count"
                },
                "by_active_sleeve_count": {
                    key: {
                        "observation_count": len(group_values),
                        "observation_fraction": (
                            len(group_values) / len(values) if values else 0.0
                        ),
                        **{
                            name: value
                            for name, value in _distribution(group_values).items()
                            if name != "count"
                        },
                    }
                    for key, group_values in groups.items()
                },
            }

        total_risk_values = [
            value for scenario in SCENARIOS for value in self.risk_values[scenario]
        ]
        total_risk_groups = {
            key: [
                value
                for scenario in SCENARIOS
                for value in self.risk_groups[scenario][key]
            ]
            for key in ("zero", "one", "two", "three_or_more")
        }
        risk = {
            "scope": (
                "EXACT_CANONICAL_90_DAY_DECISION_EVENT_DECLARED_NOMINAL_"
                "RISK_UTILISATION_BY_COST_SCENARIO"
            ),
            "by_scenario": {
                scenario: risk_view(
                    self.risk_values[scenario], self.risk_groups[scenario]
                )
                for scenario in SCENARIOS
            },
            "total_all_scenarios": risk_view(
                total_risk_values, total_risk_groups
            ),
            "actual_stop_risk_available": False,
            "time_weighted_utilisation": False,
            "scenario_mixing_status": (
                "NORMAL_AND_STRESSED_REPORTED_SEPARATELY;TOTAL_IS_AN_"
                "EXPLICIT_DIAGNOSTIC_SUM"
            ),
        }
        day_concentration: dict[str, Any] = {}
        for scenario, values in self.day_pnl_observations.items():
            positive = [max(value, 0.0) for value in values]
            positive_total = sum(positive)
            by_session_day = self.day_pnl_by_session_day[scenario]
            positive_session_days = {
                session_day: max(value, 0.0)
                for session_day, value in by_session_day.items()
            }
            positive_session_day_total = sum(positive_session_days.values())
            day_concentration[scenario] = {
                "episode_day_observation_count": len(values),
                "unique_session_day_count": len(by_session_day),
                "positive_episode_day_observation_profit_denominator": positive_total,
                "maximum_positive_episode_day_profit_share": (
                    max(positive, default=0.0) / positive_total
                    if positive_total > 0.0
                    else 0.0
                ),
                "positive_session_day_aggregate_profit_denominator": (
                    positive_session_day_total
                ),
                "maximum_positive_session_day_aggregate_profit": max(
                    positive_session_days.values(), default=0.0
                ),
                "maximum_positive_session_day_aggregate_share": (
                    max(positive_session_days.values(), default=0.0)
                    / positive_session_day_total
                    if positive_session_day_total > 0.0
                    else 0.0
                ),
                "qualification": (
                    "EXACT_ROLLING_PATH_WEIGHTED_SESSION_DAY_AGGREGATION;THE_"
                    "SAME_SOURCE_DAY_REPEATS_ACROSS_OVERLAPPING_STARTS_BUT_"
                    "IS_GROUPED_BY_SESSION_DAY_BEFORE_THE_DOMINATION_SHARE"
                ),
            }
        declared_block_ids = {block.block_id for block in self.blocks}
        covered_blocks = {
            scenario: sorted(
                block_id
                for block_id, accumulator in self.block_results[scenario].items()
                if accumulator.episode_count > 0
            )
            for scenario in SCENARIOS
        }
        if any(set(covered_blocks[scenario]) != declared_block_ids for scenario in SCENARIOS):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {self.policy_id} lacks exact B1-B4-style "
                "source-block coverage in every scenario"
            )
        component_attribution: dict[str, Any] = {}
        trade_concentration: dict[str, Any] = {}
        for scenario in SCENARIOS:
            components = dict(sorted(self.component_contribution[scenario].items()))
            source_trades = self.source_trade_contribution[scenario]
            account_net_total = sum(
                sum(accumulator.net_pnl)
                for accumulator in self.block_results[scenario].values()
            )
            _assert_close(
                sum(components.values()),
                sum(source_trades.values()),
                label=(
                    f"expanded finalist {self.policy_id} {scenario} "
                    "component/source-trade PnL"
                ),
            )
            _assert_close(
                sum(components.values()),
                account_net_total,
                label=(
                    f"expanded finalist {self.policy_id} {scenario} "
                    "component/account net PnL"
                ),
            )
            positive_components = {
                key: max(value, 0.0) for key, value in components.items()
            }
            positive_component_total = sum(positive_components.values())
            component_attribution[scenario] = {
                "scope": (
                    "COMBINE_CANONICAL_90_DAY_STAGE3_STAGE4_STAGE5_ONLY;"
                    "XFA_COMPONENT_ATTRIBUTION_NOT_AGGREGATED"
                ),
                "by_component": components,
                "total": sum(components.values()),
                "account_net_pnl_total": account_net_total,
                "additive_account_net_reconciliation": True,
                "account_net_minus_component_total": (
                    account_net_total - sum(components.values())
                ),
                "maximum_positive_component_profit_share": (
                    max(positive_components.values(), default=0.0)
                    / positive_component_total
                    if positive_component_total > 0.0
                    else 0.0
                ),
            }
            positive_sources = {
                key: max(value, 0.0) for key, value in source_trades.items()
            }
            positive_source_total = sum(positive_sources.values())
            maximum_source_key = (
                max(positive_sources, key=positive_sources.get)
                if positive_sources
                else None
            )
            observations = self.accepted_trade_observations[scenario]
            positive_observations = [max(value, 0.0) for value in observations]
            positive_observation_total = sum(positive_observations)
            trade_concentration[scenario] = {
                "scope": (
                    "COMBINE_CANONICAL_90_DAY_STAGE3_STAGE4_STAGE5_ONLY;"
                    "XFA_COMPONENT_ATTRIBUTION_NOT_AGGREGATED"
                ),
                "accepted_account_trade_observation_count": len(observations),
                "unique_immutable_source_trade_count": len(source_trades),
                "positive_source_trade_profit_denominator": positive_source_total,
                "maximum_positive_source_trade_observation_profit": max(
                    positive_sources.values(), default=0.0
                ),
                "maximum_positive_source_trade_observation_share": (
                    max(positive_sources.values(), default=0.0)
                    / positive_source_total
                    if positive_source_total > 0.0
                    else 0.0
                ),
                "positive_account_trade_observation_profit_denominator": (
                    positive_observation_total
                ),
                "maximum_positive_single_account_trade_observation_profit": max(
                    positive_observations, default=0.0
                ),
                "maximum_positive_single_account_trade_observation_share": (
                    max(positive_observations, default=0.0)
                    / positive_observation_total
                    if positive_observation_total > 0.0
                    else 0.0
                ),
                "maximum_source_trade_identity": (
                    None
                    if maximum_source_key is None
                    else {
                        "component_id": maximum_source_key[0],
                        "trade_id": maximum_source_key[1],
                    }
                ),
                "qualification": (
                    "EXACT_FROM_DEEP_VERIFIED_COMPONENT_TRADES_AND_ACCEPTED_"
                    "ROUTING_QUANTITIES;SOURCE_TRADES_ARE_GROUPED_ACROSS_"
                    "OVERLAPPING_ROLLING_STARTS_AND_ARE_NOT_INDEPENDENT_"
                    "REPLICATIONS"
                ),
                "selection_gate_auditable": True,
            }
        suppression_total = self.suppression.to_dict()
        scenario_suppression = {
            scenario: self.suppression_by_scenario[scenario].to_dict()
            for scenario in SCENARIOS
        }
        if (
            sum(value["signals_emitted"] for value in scenario_suppression.values())
            != suppression_total["signals_emitted"]
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {self.policy_id} suppression scenario sum drift"
            )
        cumulative_behavior = self.cumulative_behavior_profile()["public"]
        return {
            "stage_start_counts": {
                key: dict(value)
                for key, value in sorted(self.stage_start_counts.items())
            },
            "total_unique_start_count_per_scenario": {
                scenario: len(keys) for scenario, keys in self.start_keys.items()
            },
            "cross_stage_start_overlap_count": 0,
            "source_block_coverage": {
                "declared_block_ids": sorted(declared_block_ids),
                "covered_block_ids_by_scenario": covered_blocks,
                "common_covered_block_ids": sorted(
                    set.intersection(
                        *(set(covered_blocks[scenario]) for scenario in SCENARIOS)
                    )
                ),
                "coverage_complete_in_every_scenario": True,
                "overlapping_starts_count_as_independent_blocks": False,
            },
            "block_results": {
                scenario: {
                    block_id: accumulator.to_dict()
                    for block_id, accumulator in values.items()
                }
                for scenario, values in self.block_results.items()
            },
            "canonical_scenario_results": {
                scenario: self._canonical_scenario_result(scenario)
                for scenario in SCENARIOS
            },
            "cumulative_account_trade_behavior": cumulative_behavior,
            "risk_utilisation": risk,
            "component_contribution": component_attribution,
            "trade_concentration": trade_concentration,
            "suppression": {
                "scope": "CUMULATIVE_STAGE3_STAGE4_STAGE5_ALL_SCENARIOS",
                "total": suppression_total,
                "by_scenario": scenario_suppression,
                "by_scenario_and_component": {
                    scenario: {
                        component_id: accumulator.to_dict()
                        for component_id, accumulator in sorted(
                            self.suppression_by_scenario_and_component[
                                scenario
                            ].items()
                        )
                    }
                    for scenario in SCENARIOS
                },
            },
            "day_concentration": day_concentration,
            "xfa_lifecycle": {
                "scope": "CUMULATIVE_STAGE3_STAGE4_STAGE5_FULL_HORIZON",
                "paths_are_alternative_not_additive": True,
                "every_transition_profile_matches_frozen_finalist_books": True,
                "frozen_xfa_profile": dict(self.expected_xfa_profile),
                **{
                    scenario: {
                        path: self.lifecycle[scenario][path].to_dict()
                        for path in PATHS
                    }
                    for scenario in SCENARIOS
                },
            },
        }


def _stream_expanded_stage_caches(
    *,
    stage: str,
    cache_dir: Path,
    manifest: Mapping[str, Any],
    bundle_manifest: Mapping[str, Any],
    bundle_identity: Mapping[str, Any],
    blocks: Sequence[BlockSpec],
    expected_policy_ids: set[str],
    expected_starts_per_scenario: int,
    promoted96: set[str],
    surviving96: set[str],
    finalists: set[str],
    accumulators: Mapping[str, ExpandedFinalistRawAccumulator],
) -> list[dict[str, Any]]:
    if stage not in {"stage4", "stage5"}:
        raise ActiveRiskDecisionReportError(f"unsupported expanded stage {stage}")
    paths = sorted(cache_dir.glob("batch_*.json"))
    if len(paths) != len(expected_policy_ids):
        raise ActiveRiskDecisionReportError(
            f"{stage} cache count is {len(paths)}, expected {len(expected_policy_ids)}"
        )
    partitions = _stage_partition_index(
        bundle_manifest,
        stage=stage,
        expected_policy_count=len(expected_policy_ids),
    )
    sealed_fingerprints = bundle_identity.get("policy_fingerprints")
    if not isinstance(sealed_fingerprints, Mapping):
        raise ActiveRiskDecisionReportError(
            "EvidenceBundle lacks sealed policy fingerprints"
        )
    observed_ids: set[str] = set()
    provenance: list[dict[str, Any]] = []
    for batch_index, path in enumerate(paths):
        if path.name != f"batch_{batch_index:06d}.json":
            raise ActiveRiskDecisionReportError(
                f"{stage} cache filename/index drift: {path}"
            )
        payload = _load_json(path)
        if not isinstance(payload, Mapping):
            raise ActiveRiskDecisionReportError(f"{stage} cache is malformed: {path}")
        if (
            payload.get("schema") != "hydra_active_risk_stage_batch_v1"
            or payload.get("stage") != stage
        ):
            raise ActiveRiskDecisionReportError(f"{stage} cache identity drift: {path}")
        rows = payload.get("rows")
        if not isinstance(rows, list) or len(rows) != 1:
            raise ActiveRiskDecisionReportError(f"{stage} batch cardinality drift: {path}")
        claimed_rows_hash = str(payload.get("rows_hash") or "")
        if canonical_hash(rows) != claimed_rows_hash:
            raise ActiveRiskDecisionReportError(f"{stage} rows_hash drift: {path}")
        row = rows[0]
        compact, canonical_raw, full_raw = _candidate_summary(
            row,
            blocks,
            None,
            promoted96,
            surviving96,
            finalists,
            expected_starts_per_scenario,
        )
        policy_id = str(compact["policy_id"])
        if policy_id not in expected_policy_ids or policy_id in observed_ids:
            raise ActiveRiskDecisionReportError(
                f"{stage} unexpected or duplicate policy {policy_id}"
            )
        if str(sealed_fingerprints.get(policy_id) or "") != str(
            compact.get("structural_fingerprint") or ""
        ):
            raise ActiveRiskDecisionReportError(
                f"{stage} policy {policy_id} fingerprint drift"
            )
        projected = _stage3_projected_signatures(
            row, manifest, stage_label=stage
        )
        for dataset, suffix in (
            ("episodes", "episodes"),
            ("account_daily_paths", "daily"),
        ):
            batch_id = f"active:{stage}:{batch_index:06d}:{suffix}"
            sealed_partition = partitions[(dataset, batch_id)]
            signature = projected[dataset]
            if (
                int(sealed_partition.get("row_count", -1))
                != int(signature["row_count"])
                or str(sealed_partition.get("payload_sha256") or "")
                != str(signature["payload_sha256"])
            ):
                raise ActiveRiskDecisionReportError(
                    f"{stage} cache diverges from sealed {dataset} partition {batch_id}"
                )
        accumulator = accumulators.get(policy_id)
        lifecycle_rows = list(row.get("lifecycle_rows") or ())
        if accumulator is not None:
            accumulator.add_stage(
                stage=stage,
                compact=compact,
                canonical_raw=canonical_raw,
                full_raw=full_raw,
                lifecycle_rows=lifecycle_rows,
                expected_starts_per_scenario=expected_starts_per_scenario,
            )
        observed_ids.add(policy_id)
        provenance.append(
            {"path": str(path), "rows_hash": claimed_rows_hash, "row_count": 1}
        )
        del (
            projected,
            lifecycle_rows,
            canonical_raw,
            full_raw,
            compact,
            row,
            rows,
            payload,
        )
    if observed_ids != expected_policy_ids:
        raise ActiveRiskDecisionReportError(f"{stage} policy coverage drift")
    return provenance


def _reconcile_expanded_finalist_scenario(
    *,
    canonical: Mapping[str, Any],
    raw: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    """Bind every published decision metric to cumulative raw stage caches."""

    count_fields = (
        "episode_count",
        "pass_count",
        "mll_breach_count",
        "censored_episode_count",
        "data_censored_episode_count",
        "operational_horizon_not_reached_count",
        "combine_evaluable_episode_count",
        "consistency_ok_count",
        "pass_block_count",
    )
    for field_name in count_fields:
        observed = canonical.get(field_name)
        expected = raw.get(field_name)
        if (
            isinstance(observed, bool)
            or not isinstance(observed, int)
            or isinstance(expected, bool)
            or not isinstance(expected, int)
            or observed != expected
        ):
            raise ActiveRiskDecisionReportError(
                f"{label} sealed frontier/raw {field_name} drift"
            )

    canonical_terminals = canonical.get("terminal_distribution")
    raw_terminals = raw.get("terminal_distribution")
    if not isinstance(canonical_terminals, Mapping) or dict(
        canonical_terminals
    ) != dict(raw_terminals or {}):
        raise ActiveRiskDecisionReportError(
            f"{label} sealed frontier/raw terminal distribution drift"
        )
    if list(canonical.get("pass_block_ids") or ()) != list(
        raw.get("pass_block_ids") or ()
    ):
        raise ActiveRiskDecisionReportError(
            f"{label} sealed frontier/raw pass-block identity drift"
        )
    canonical_by_block = canonical.get("by_block_net")
    raw_by_block = raw.get("by_block_net")
    if (
        not isinstance(canonical_by_block, Mapping)
        or not isinstance(raw_by_block, Mapping)
        or set(canonical_by_block) != set(raw_by_block)
    ):
        raise ActiveRiskDecisionReportError(
            f"{label} sealed frontier/raw by-block net coverage drift"
        )
    for block_id, expected in raw_by_block.items():
        _assert_close(
            canonical_by_block[block_id],
            expected,
            label=f"{label} sealed frontier/raw by-block net {block_id}",
        )

    numeric_fields = (
        "pass_rate",
        "pass_rate_raw_lower_bound",
        "pass_rate_evaluable",
        "mll_breach_rate",
        "mll_breach_rate_raw_lower_bound",
        "mll_breach_rate_evaluable",
        "consistency_rate",
        "target_progress_p25",
        "target_progress_median",
        "maximum_target_progress",
        "net_total",
        "net_median",
        "minimum_mll_buffer",
        "median_days_to_target",
        "projected_active_days_to_target_median",
        "projected_calendar_days_to_target_median",
        "duration_trading_days_median",
        "active_trading_days_median",
        "calendar_days_median",
        "monthly_subscription_duration_proxy_median",
        "censoring_rate",
        "maximum_block_profit_share",
    )
    for field_name in numeric_fields:
        observed = canonical.get(field_name)
        expected = raw.get(field_name)
        if observed is None or expected is None:
            if observed is not None or expected is not None:
                raise ActiveRiskDecisionReportError(
                    f"{label} sealed frontier/raw {field_name} drift"
                )
            continue
        _assert_close(
            observed,
            expected,
            label=f"{label} sealed frontier/raw {field_name}",
        )

    distribution_fields = (
        "target_progress_distribution",
        "maximum_target_progress_distribution",
        "net_pnl_distribution",
        "minimum_mll_buffer_distribution",
        "days_to_target_distribution",
        "duration_trading_days_distribution",
        "active_trading_days_distribution",
        "calendar_days_distribution",
        "projected_active_days_to_target_distribution",
        "projected_calendar_days_to_target_distribution",
    )
    distributions: dict[str, Any] = {}
    for field_name in distribution_fields:
        value = raw.get(field_name)
        if not isinstance(value, Mapping):
            raise ActiveRiskDecisionReportError(
                f"{label} raw {field_name} is absent"
            )
        distributions[f"{field_name}_exact_rederived"] = dict(value)
    return {
        "status": "EXACTLY_REDERIVED_FROM_STAGE3_STAGE4_STAGE5_RAW_CACHES",
        "decision_metrics_match": True,
        **distributions,
    }


def _sealed_finalist_matrix(
    *,
    frontier: Sequence[Mapping[str, Any]],
    finalists: set[str],
    candidates_by_id: Mapping[str, Mapping[str, Any]],
    blocks: Sequence[BlockSpec],
    component_market_map: Mapping[str, str],
    expanded_raw_audits: Mapping[str, Mapping[str, Any]],
    expanded_behavior_profiles: Mapping[str, Mapping[str, Any]],
    expected_starts_per_scenario: int | None,
) -> dict[str, Any]:
    """Publish the final no-retune frontier without inventing missing controls."""

    by_id = {str(row.get("policy_id") or ""): row for row in frontier}
    if set(by_id) != finalists:
        raise ActiveRiskDecisionReportError(
            "sealed expanded finalist frontier identity drift"
        )
    if set(expanded_behavior_profiles) != finalists:
        raise ActiveRiskDecisionReportError(
            "expanded cumulative behavior profile identity drift"
        )
    rows: list[dict[str, Any]] = []
    block_ids = [block.block_id for block in blocks]
    for policy_id in sorted(finalists):
        sealed = by_id[policy_id]
        stage3 = candidates_by_id.get(policy_id)
        if stage3 is None:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} is absent from Stage-3 evidence"
            )
        if str(sealed.get("structural_fingerprint") or "") != str(
            stage3.get("structural_fingerprint") or ""
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} mutated after Stage 3"
            )
        horizons = sealed.get("horizons")
        if not isinstance(horizons, Mapping):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} lacks frozen horizons"
            )
        raw_audit = expanded_raw_audits.get(policy_id)
        if not isinstance(raw_audit, Mapping):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} lacks streamed raw audit"
            )
        behavior_profile = expanded_behavior_profiles.get(policy_id)
        if not isinstance(behavior_profile, Mapping) or not isinstance(
            behavior_profile.get("public"), Mapping
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} lacks cumulative behavior profile"
            )
        public_behavior = behavior_profile["public"]
        raw_public_behavior = raw_audit.get("cumulative_account_trade_behavior")
        if not isinstance(raw_public_behavior, Mapping) or dict(
            raw_public_behavior
        ) != dict(public_behavior):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} cumulative behavior publication drift"
            )
        legacy_frontier_fingerprint = str(
            sealed.get("actual_account_behavior_fingerprint") or ""
        )
        if legacy_frontier_fingerprint != str(
            public_behavior.get(
                "runtime_legacy_cumulative_behavior_fingerprint_rederived"
            )
            or ""
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} legacy frontier behavior "
                "fingerprint cannot be rederived from Stage-3/4/5 raw caches"
            )
        behavior_scenario_counts = public_behavior.get(
            "per_scenario_observation_count"
        )
        if not isinstance(behavior_scenario_counts, Mapping) or (
            expected_starts_per_scenario is not None
            and any(
                int(behavior_scenario_counts.get(scenario, -1))
                != expected_starts_per_scenario
                for scenario in SCENARIOS
            )
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} cumulative behavior start-count drift"
            )
        if expected_starts_per_scenario == 192 and public_behavior.get(
            "stage_start_counts"
        ) != {
            "stage3": {"normal": 48, "stressed": 48},
            "stage4": {"normal": 48, "stressed": 48},
            "stage5": {"normal": 96, "stressed": 96},
        }:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} cumulative 48+48+96 behavior "
                "coverage drift"
            )
        scenario_views: dict[str, Any] = {}
        horizon_views: dict[str, Any] = {}
        for scenario in SCENARIOS:
            scenario_horizons = horizons.get(scenario)
            if not isinstance(scenario_horizons, Mapping) or set(
                scenario_horizons
            ) != set(HORIZONS):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} horizon coverage drift"
                )
            horizon_views[scenario] = {}
            for horizon in HORIZONS:
                summary = scenario_horizons[horizon]
                if not isinstance(summary, Mapping):
                    raise ActiveRiskDecisionReportError(
                        f"expanded finalist {policy_id} {scenario} {horizon} is malformed"
                    )
                horizon_views[scenario][horizon] = _validate_finalist_compact_summary(
                    summary,
                    label=f"expanded finalist {policy_id} {scenario} {horizon}",
                    expected_starts=expected_starts_per_scenario,
                )
            canonical = scenario_horizons[CANONICAL_HORIZON]
            if dict(sealed.get(scenario) or {}) != dict(canonical):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} canonical {scenario} identity drift"
                )
            canonical_view = horizon_views[scenario][CANONICAL_HORIZON]
            canonical_view["consistency_ok_count"] = int(
                canonical.get("consistency_ok_count", -1)
            )
            raw_scenario_results = raw_audit.get("canonical_scenario_results")
            raw_scenario = (
                raw_scenario_results.get(scenario)
                if isinstance(raw_scenario_results, Mapping)
                else None
            )
            if not isinstance(raw_scenario, Mapping):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} lacks cumulative "
                    "raw decision metrics"
                )
            canonical_view["expanded_raw_relational_validation"] = (
                _reconcile_expanded_finalist_scenario(
                    canonical=canonical_view,
                    raw=raw_scenario,
                    label=f"expanded finalist {policy_id} {scenario}",
                )
            )
            raw_component_section = (
                (raw_audit.get("component_contribution") or {}).get(scenario)
                if isinstance(raw_audit.get("component_contribution"), Mapping)
                else None
            )
            if not isinstance(raw_component_section, Mapping) or not isinstance(
                raw_component_section.get("by_component"), Mapping
            ):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} lacks exact raw "
                    "component attribution"
                )
            raw_components = dict(raw_component_section["by_component"])
            compact_components = dict(canonical.get("component_contribution") or {})
            if set(raw_components) != set(compact_components):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} compact/raw "
                    "component attribution key drift"
                )
            for component_id, contribution in raw_components.items():
                _assert_close(
                    contribution,
                    compact_components[component_id],
                    label=(
                        f"expanded finalist {policy_id} {scenario} compact/raw "
                        f"component attribution {component_id}"
                    ),
                )
            if raw_component_section.get("additive_account_net_reconciliation") is not True:
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} component/account "
                    "net reconciliation is absent"
                )
            _assert_close(
                raw_component_section.get(
                    "maximum_positive_component_profit_share"
                ),
                canonical_view.get("maximum_sleeve_profit_share"),
                label=(
                    f"expanded finalist {policy_id} {scenario} sealed frontier/raw "
                    "maximum sleeve profit share"
                ),
            )
            _assert_close(
                raw_component_section.get("total"),
                canonical_view.get("net_total"),
                label=(
                    f"expanded finalist {policy_id} {scenario} exact component/"
                    "canonical account net PnL"
                ),
            )
            expected_component_scope = (
                "COMBINE_CANONICAL_90_DAY_STAGE3_STAGE4_STAGE5_ONLY;"
                "XFA_COMPONENT_ATTRIBUTION_NOT_AGGREGATED"
            )
            if raw_component_section.get("scope") != expected_component_scope:
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} component "
                    "attribution scope drift"
                )
            canonical_view["component_attribution"] = raw_components
            canonical_view["component_attribution_scope"] = expected_component_scope
            canonical_view["component_attribution_additive_account_net_reconciliation"] = True
            market_attribution: dict[str, float] = defaultdict(float)
            for component_id, contribution in canonical_view[
                "component_attribution"
            ].items():
                market = component_market_map.get(str(component_id))
                if market is None:
                    raise ActiveRiskDecisionReportError(
                        f"expanded finalist {policy_id} component {component_id} "
                        "lacks sealed market attribution"
                    )
                market_attribution[market] += _required_float(
                    contribution,
                    label=(
                        f"expanded finalist {policy_id} {scenario} component "
                        f"{component_id} contribution"
                    ),
                )
            positive_market_total = sum(
                max(value, 0.0) for value in market_attribution.values()
            )
            maximum_market_share = (
                max(
                    (max(value, 0.0) for value in market_attribution.values()),
                    default=0.0,
                )
                / positive_market_total
                if positive_market_total > 0.0
                else 0.0
            )
            canonical_view["market_attribution"] = dict(
                sorted(market_attribution.items())
            )
            canonical_view["block_evidence"] = {
                block_id: {
                    "net_pnl_total": _float(
                        (canonical.get("by_block_net") or {}).get(block_id)
                    ),
                    "target_progress_median": (
                        canonical.get("by_block_target_progress_median") or {}
                    ).get(block_id),
                    "target_progress_median_status": (
                        "APPROXIMATE_RECURSIVE_MEDIAN_FROM_STAGE_INCREMENTS"
                    ),
                    "contains_at_least_one_pass": block_id
                    in set(canonical.get("pass_block_ids") or ()),
                    "pass_count": None,
                    "pass_count_status": (
                        "UNAVAILABLE_IN_COMPACT_FRONTIER;ONLY_PASS_BLOCK_"
                        "MEMBERSHIP_IS_PERSISTED"
                    ),
                }
                for block_id in block_ids
            }
            raw_trade_concentration = (
                (raw_audit.get("trade_concentration") or {}).get(scenario)
                if isinstance(raw_audit.get("trade_concentration"), Mapping)
                else None
            )
            if (
                not isinstance(raw_trade_concentration, Mapping)
                or raw_trade_concentration.get("selection_gate_auditable") is not True
            ):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} cannot audit "
                    "single-trade domination"
                )
            raw_day_concentration = (
                (raw_audit.get("day_concentration") or {}).get(scenario)
                if isinstance(raw_audit.get("day_concentration"), Mapping)
                else None
            )
            if not isinstance(raw_day_concentration, Mapping):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} cannot audit "
                    "calendar-day domination"
                )
            canonical_view["concentration"] = {
                "maximum_block_positive_profit_share": canonical.get(
                    "maximum_block_profit_share"
                ),
                "maximum_sleeve_positive_profit_share": raw_component_section.get(
                    "maximum_positive_component_profit_share"
                ),
                "day_concentration": dict(raw_day_concentration),
                "day_concentration_status": (
                    "EXACT_ROLLING_PATH_WEIGHTED_SESSION_DAY_AGGREGATION;"
                    "OVERLAPPING_STARTS_ARE_NOT_INDEPENDENT"
                ),
                "trade_concentration": dict(raw_trade_concentration),
                "trade_concentration_status": (
                    "EXACT_FROM_SEALED_COMPONENT_LEDGER_AND_ROUTING_QUANTITIES;"
                    "GROUPED_SOURCE_TRADES_REPEAT_ACROSS_OVERLAPPING_STARTS"
                ),
                "maximum_market_positive_profit_share": maximum_market_share,
                "market_concentration_status": (
                    "DERIVED_FROM_DEEP_VERIFIED_COMPONENT_TRADE_MARKET_MAP_AND_"
                    "EXACT_ADDITIVE_COMPONENT_CONTRIBUTION"
                ),
            }
            scenario_views[scenario] = canonical_view
        transitions = int(sealed.get("xfa_paths_started", 0))
        standard_paths = int(sealed.get("xfa_standard_paths", 0))
        consistency_paths = int(sealed.get("xfa_consistency_paths", 0))
        payout_observations = int(sealed.get("first_payouts", 0))
        survival_observations = int(sealed.get("post_payout_survival_count", 0))
        alternative_paths = standard_paths + consistency_paths
        if (
            min(
                transitions,
                standard_paths,
                consistency_paths,
                payout_observations,
                survival_observations,
            )
            < 0
            or standard_paths != transitions
            or consistency_paths != transitions
            or payout_observations > alternative_paths
            or survival_observations > payout_observations
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} pooled lifecycle count drift"
            )
        expected_survival_rate = (
            survival_observations / alternative_paths if alternative_paths else 0.0
        )
        _assert_close(
            expected_survival_rate,
            sealed.get("post_payout_survival_rate"),
            label=f"expanded finalist {policy_id} post-payout survival rate",
        )
        raw_start_counts = raw_audit.get("total_unique_start_count_per_scenario")
        if not isinstance(raw_start_counts, Mapping):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} raw start audit is malformed"
            )
        source_block_coverage = raw_audit.get("source_block_coverage")
        if not isinstance(source_block_coverage, Mapping):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} source-block coverage is absent"
            )
        covered_by_scenario = source_block_coverage.get(
            "covered_block_ids_by_scenario"
        )
        common_covered = list(
            source_block_coverage.get("common_covered_block_ids") or ()
        )
        if (
            not isinstance(covered_by_scenario, Mapping)
            or set(source_block_coverage.get("declared_block_ids") or ())
            != set(block_ids)
            or any(
                set(covered_by_scenario.get(scenario) or ()) != set(block_ids)
                for scenario in SCENARIOS
            )
            or set(common_covered) != set(block_ids)
            or source_block_coverage.get("coverage_complete_in_every_scenario")
            is not True
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} does not cover every declared "
                "independent source block"
            )
        for scenario in SCENARIOS:
            if int(raw_start_counts.get(scenario, -1)) != int(
                scenario_views[scenario]["episode_count"]
            ):
                raise ActiveRiskDecisionReportError(
                    f"expanded finalist {policy_id} {scenario} raw/frontier "
                    "start-count drift"
                )
            scenario_views[scenario]["block_evidence_exact"] = dict(
                (raw_audit.get("block_results") or {}).get(scenario) or {}
            )
            scenario_views[scenario]["day_concentration_exact"] = dict(
                (raw_audit.get("day_concentration") or {}).get(scenario) or {}
            )
        raw_lifecycle = raw_audit.get("xfa_lifecycle")
        if not isinstance(raw_lifecycle, Mapping):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} raw lifecycle is absent"
            )
        raw_transitions = sum(
            int((raw_lifecycle[scenario]["standard"])["xfa_paths_started"])
            for scenario in SCENARIOS
        )
        raw_first_payouts = sum(
            int((raw_lifecycle[scenario][path])["first_payouts"])
            for scenario in SCENARIOS
            for path in PATHS
        )
        raw_cycles = sum(
            int((raw_lifecycle[scenario][path])["payout_cycles"])
            for scenario in SCENARIOS
            for path in PATHS
        )
        raw_cash = sum(
            _required_float(
                (raw_lifecycle[scenario][path])["trader_net_payout"],
                label=f"expanded finalist {policy_id} raw {scenario} {path} cash",
            )
            for scenario in SCENARIOS
            for path in PATHS
        )
        raw_survival = sum(
            int(
                (raw_lifecycle[scenario][path])[
                    "post_payout_survival_count"
                ]
            )
            for scenario in SCENARIOS
            for path in PATHS
        )
        if (
            raw_transitions != transitions
            or raw_first_payouts != payout_observations
            or raw_cycles != int(sealed.get("payout_cycles", 0))
            or raw_survival != survival_observations
        ):
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {policy_id} raw/compact lifecycle count drift"
            )
        _assert_close(
            raw_cash,
            sealed.get("trader_net_payout"),
            label=f"expanded finalist {policy_id} raw/compact lifecycle cash",
        )
        expanded_raw_complete = (
            expected_starts_per_scenario is not None
            and all(
                int(raw_start_counts.get(scenario, -1))
                == expected_starts_per_scenario
                for scenario in SCENARIOS
            )
        )
        rows.append(
            {
                "policy_id": policy_id,
                "structural_fingerprint": sealed.get("structural_fingerprint"),
                "sealed_cumulative_account_behavior_fingerprint": public_behavior[
                    "authoritative_raw_account_trade_behavior_fingerprint"
                ],
                "authoritative_cumulative_account_trade_behavior_fingerprint": (
                    public_behavior[
                        "authoritative_raw_account_trade_behavior_fingerprint"
                    ]
                ),
                "legacy_frontier_account_behavior_fingerprint": (
                    legacy_frontier_fingerprint
                ),
                "legacy_frontier_behavior_fingerprint_rederived_exactly": True,
                "authoritative_behavior_fingerprint_relation": (
                    "RICH_RAW_FINGERPRINT_REPLACES_LEGACY_FOR_CLUSTERING;LEGACY_"
                    "RUNTIME_MERGE_HASH_REDERIVED_EXACTLY_AND_RECONCILED"
                ),
                "cumulative_account_trade_behavior": dict(public_behavior),
                "stage3_posthoc_behavioral_cluster": stage3.get(
                    "posthoc_behavioral_cluster"
                ),
                "behavior_cluster_scope": (
                    "CUMULATIVE_STAGE3_STAGE4_STAGE5_ACCOUNT_TRAJECTORY_"
                    "ROUTING_SUPPRESSION_AND_ADMITTED_SOURCE_TRADE_BEHAVIOR"
                ),
                "no_retune_stage_path": "STAGE3_48_PLUS_STAGE4_48_PLUS_STAGE5_96",
                "starts_per_scenario": int(
                    scenario_views["normal"]["episode_count"]
                ),
                "normal": scenario_views["normal"],
                "stressed": scenario_views["stressed"],
                "horizons": horizon_views,
                "effective_independent_source_block_count": len(common_covered),
                "source_block_ids": sorted(common_covered),
                "source_block_coverage_exact": dict(source_block_coverage),
                "independence_qualification": (
                    "ONLY_B1_B2_B3_B4_ARE_COUNTED_AS_SOURCE_BLOCKS;THE_"
                    "EXPANDED_ROLLING_STARTS_OVERLAP_AND_ARE_NOT_192_"
                    "INDEPENDENT_REPLICATIONS"
                ),
                "risk_utilisation": dict(sealed.get("risk_utilisation") or {}),
                "risk_utilisation_status": (
                    "COMPACT_STAGE_MERGE;DISTRIBUTION_QUANTILES_ARE_NOT_EXACT_"
                    "WITHOUT_SEALED_DAILY_PATH_REDERIVATION"
                ),
                "risk_utilisation_exact_rederived": dict(
                    raw_audit.get("risk_utilisation") or {}
                ),
                "suppression": dict(sealed.get("suppression") or {}),
                "suppression_status": (
                    "SEALED_COMPACT_LEGACY_SCOPE;USE_SUPPRESSION_EXACT_"
                    "REDERIVED_FOR_CUMULATIVE_NORMAL_AND_STRESSED_DETAIL"
                ),
                "suppression_exact_rederived": dict(
                    raw_audit.get("suppression") or {}
                ),
                "component_contribution_exact_rederived": dict(
                    raw_audit.get("component_contribution") or {}
                ),
                "trade_concentration_exact_rederived": dict(
                    raw_audit.get("trade_concentration") or {}
                ),
                "day_concentration_exact_rederived": dict(
                    raw_audit.get("day_concentration") or {}
                ),
                "expanded_standard_consistency_xfa_lifecycle_exact": dict(
                    raw_lifecycle
                ),
                "exact_streamed_account_audit": dict(raw_audit),
                "stage3_matched_control_deltas": {
                    "scope": "STAGE3_ONLY_48_MATCHED_STARTS",
                    "matched_starts_per_scenario": 48,
                    "matched_attempts_normal_plus_stressed": 96,
                    "expanded_192_status": "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED",
                    "deltas": dict(stage3.get("control_deltas") or {}),
                },
                "stage3_separate_xfa_lifecycle": {
                    "scope": "STAGE3_ONLY_48_STARTS_PER_SCENARIO",
                    "standard_and_consistency_are_separate": True,
                    "results": dict(stage3.get("stage3_xfa_lifecycle") or {}),
                    "expanded_192_status": (
                        "AVAILABLE_IN_EXPANDED_STANDARD_CONSISTENCY_XFA_"
                        "LIFECYCLE_EXACT"
                        if expanded_raw_complete
                        else "NOT_REQUESTED_STAGE3_ONLY_RAW_AUDIT"
                    ),
                },
                "sealed_pooled_xfa_lifecycle": {
                    "combine_to_xfa_transitions": transitions,
                    "standard_alternative_paths": standard_paths,
                    "consistency_alternative_paths": consistency_paths,
                    "alternative_paths": alternative_paths,
                    "first_payout_path_observations": payout_observations,
                    "alternative_paths_without_observed_first_payout": (
                        alternative_paths - payout_observations
                    ),
                    "payout_cycle_observations_within_frozen_horizon": int(
                        sealed.get("payout_cycles", 0)
                    ),
                    "trader_90_percent_split_cash_observations_before_fees_tax": _float(
                        sealed.get("trader_net_payout")
                    ),
                    "summed_alternative_cash_observations_per_combine_transition": (
                        _float(sealed.get("trader_net_payout")) / transitions
                        if transitions
                        else 0.0
                    ),
                    "summed_alternative_cash_per_transition_is_realisable_ev": False,
                    "standard_consistency_outcomes_separate_status": (
                        "AVAILABLE_FROM_STREAMED_SEALED_FULL_HORIZON_EPISODE_PATHS"
                        if expanded_raw_complete
                        else "COMPACT_FRONTIER_POOLED;EXPANDED_RAW_NOT_REQUESTED"
                    ),
                    "paths_are_alternative_not_additive": True,
                    "post_payout_survival_observations": survival_observations,
                    "post_payout_survival_rate_across_all_alternative_paths": (
                        expected_survival_rate
                    ),
                },
            }
        )
    cumulative_clusters = _cumulative_behavior_clusters(
        expanded_behavior_profiles,
        {str(row["policy_id"]): row for row in rows},
    )
    for row in rows:
        row["expanded_economic_behavior_cluster"] = cumulative_clusters[
            "membership"
        ][str(row["policy_id"])]
    exact_hash_groups: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        fingerprint = str(
            row.get("sealed_cumulative_account_behavior_fingerprint") or ""
        )
        if not fingerprint:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {row['policy_id']} lacks sealed behavior fingerprint"
            )
        exact_hash_groups[fingerprint].append(str(row["policy_id"]))
    exact_cluster_rows: list[dict[str, Any]] = []
    for index, (fingerprint, members) in enumerate(
        sorted(exact_hash_groups.items()), start=1
    ):
        cluster_id = f"expanded_exact_account_cluster_{index:02d}"
        members.sort()
        exact_cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "authoritative_cumulative_account_trade_behavior_fingerprint": (
                    fingerprint
                ),
                "member_ids": members,
                "member_count": len(members),
                "equivalence": (
                    "EXACT_RAW_REDERIVED_CUMULATIVE_ACCOUNT_TRAJECTORY_ROUTING_"
                    "SUPPRESSION_AND_ADMITTED_TRADE_HASH"
                ),
            }
        )
        for row in rows:
            if row["policy_id"] in members:
                row["expanded_exact_account_behavior_cluster"] = cluster_id
    stage3_similarity_groups: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        cluster_id = str(row.get("stage3_posthoc_behavioral_cluster") or "")
        if not cluster_id:
            raise ActiveRiskDecisionReportError(
                f"expanded finalist {row['policy_id']} lacks Stage-3 similarity cluster"
            )
        stage3_similarity_groups[cluster_id].append(str(row["policy_id"]))
    stage3_similarity_subset = [
        {
            "cluster_id": cluster_id,
            "finalist_member_ids": sorted(members),
            "finalist_member_count": len(members),
            "scope": "FINALIST_SUBSET_OF_STAGE3_ACCOUNT_AND_ROUTING_CLUSTER",
        }
        for cluster_id, members in sorted(stage3_similarity_groups.items())
    ]
    return {
        "scope": "SEALED_CUMULATIVE_STAGE3_STAGE4_STAGE5_NO_RETUNE_DEVELOPMENT",
        "expected_starts_per_scenario": expected_starts_per_scenario,
        "finalist_count": len(rows),
        "rows": rows,
        "exact_account_behavior_clusters": exact_cluster_rows,
        "exact_account_behavior_cluster_count": len(exact_cluster_rows),
        "cumulative_192_economic_behavioral_clustering": cumulative_clusters,
        "cumulative_192_economic_behavior_cluster_count": cumulative_clusters[
            "cluster_count"
        ],
        "stage3_similarity_clusters_among_finalists": stage3_similarity_subset,
        "stage3_similarity_cluster_count_among_finalists": len(
            stage3_similarity_subset
        ),
        "controls_scope": "STAGE3_ONLY_48_STARTS",
        "expanded_matched_controls_status": (
            "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED"
        ),
        "independent_evidence_status": "NOT_INDEPENDENT_CONFIRMATION",
    }


def _sealed_campaign_wide_lifecycle_totals(
    economic: Mapping[str, Any],
    *,
    sealed_episode_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Expose final-result lifecycle totals without treating paths as additive alpha."""

    def required_count(field_name: str) -> int:
        value = economic.get(field_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ActiveRiskDecisionReportError(
                f"sealed campaign-wide lifecycle {field_name} is not a "
                "non-negative integer"
            )
        return int(value)

    totals = {
        "xfa_paths_started": required_count("xfa_paths_started"),
        "first_payouts": required_count("first_payouts"),
        "payout_cycles": required_count("payout_cycles"),
        "trader_net_payout": _required_float(
            economic.get("trader_net_payout"),
            label="sealed campaign-wide lifecycle trader net payout",
        ),
    }
    if totals["trader_net_payout"] < 0.0:
        raise ActiveRiskDecisionReportError(
            "sealed campaign-wide lifecycle trader net payout is negative"
        )
    optional: dict[str, Any] = {}
    for field_name in (
        "xfa_standard_paths",
        "xfa_consistency_paths",
        "post_payout_survival_count",
    ):
        if field_name in economic and economic[field_name] is not None:
            optional[field_name] = required_count(field_name)
    if economic.get("post_payout_survival_rate") is not None:
        survival_rate = _required_float(
            economic["post_payout_survival_rate"],
            label="sealed campaign-wide lifecycle post-payout survival rate",
        )
        if not 0.0 <= survival_rate <= 1.0:
            raise ActiveRiskDecisionReportError(
                "sealed campaign-wide lifecycle post-payout survival rate is "
                "outside [0, 1]"
            )
        optional["post_payout_survival_rate"] = survival_rate
    for field_name in ("xfa_standard_paths", "xfa_consistency_paths"):
        if field_name in optional and optional[field_name] != totals["xfa_paths_started"]:
            raise ActiveRiskDecisionReportError(
                f"sealed campaign-wide lifecycle {field_name} does not match "
                "XFA transitions"
            )
    alternative_path_count = (
        int(optional.get("xfa_standard_paths", totals["xfa_paths_started"]))
        + int(optional.get("xfa_consistency_paths", totals["xfa_paths_started"]))
    )
    if totals["first_payouts"] > alternative_path_count:
        raise ActiveRiskDecisionReportError(
            "sealed campaign-wide lifecycle first payouts exceed alternative paths"
        )
    if "post_payout_survival_count" in optional:
        survival_count = int(optional["post_payout_survival_count"])
        if survival_count > totals["first_payouts"]:
            raise ActiveRiskDecisionReportError(
                "sealed campaign-wide lifecycle post-payout survival count exceeds "
                "first payouts"
            )
        if "post_payout_survival_rate" in optional:
            expected_rate = (
                survival_count / alternative_path_count
                if alternative_path_count
                else 0.0
            )
            _assert_close(
                optional["post_payout_survival_rate"],
                expected_rate,
                label="sealed campaign-wide lifecycle post-payout survival rate",
            )
    audit_proof_status = "UNAVAILABLE_NOT_REQUESTED"
    raw_survival_count: int | None = None
    if sealed_episode_audit is not None:
        raw_transitions = int(
            sealed_episode_audit.get("combine_to_xfa_transition_count", -1)
        )
        raw_paths = int(sealed_episode_audit.get("alternative_path_count", -1))
        raw_first = int(
            sealed_episode_audit.get("first_payout_path_observation_count", -1)
        )
        raw_cycles = int(
            sealed_episode_audit.get("payout_cycle_observation_count", -1)
        )
        raw_cash = _required_float(
            sealed_episode_audit.get(
                "trader_90_percent_split_cash_observations_before_fees_tax"
            ),
            label="sealed episode lifecycle trader cash",
        )
        raw_survival_count = int(
            sealed_episode_audit.get("post_payout_survival_observation_count", -1)
        )
        if (
            raw_transitions != totals["xfa_paths_started"]
            or raw_paths != alternative_path_count
            or raw_first != totals["first_payouts"]
            or raw_cycles != totals["payout_cycles"]
            or raw_survival_count < 0
            or raw_survival_count > raw_first
        ):
            raise ActiveRiskDecisionReportError(
                "sealed campaign summary lifecycle counts diverge from episode proof"
            )
        _assert_close(
            raw_cash,
            totals["trader_net_payout"],
            label="sealed campaign summary/episode lifecycle trader cash",
        )
        if (
            "post_payout_survival_count" in optional
            and int(optional["post_payout_survival_count"]) != raw_survival_count
        ):
            raise ActiveRiskDecisionReportError(
                "sealed campaign survival count diverges from episode proof"
            )
        required_proofs = (
            "transition_key_uniqueness_proved",
            "full_episode_key_uniqueness_proved",
            "zero_inter_stage_overlap_proved",
            "full_pass_lifecycle_bijection_proved",
            "exactly_two_alternative_paths_per_transition_proved",
            "path_key_uniqueness_proved",
            "first_payout_uniqueness_per_path_proved",
            "canonical_event_to_summary_reconciliation_proved",
            "standard_consistency_alternatives_kept_separate",
            "official_rule_snapshot_exact",
            "payout_eligibility_amount_and_reset_reexecuted_from_daily_ledger",
        )
        if any(sealed_episode_audit.get(field) is not True for field in required_proofs):
            raise ActiveRiskDecisionReportError(
                "sealed episode lifecycle proof flags are incomplete"
            )
        audit_proof_status = "PROVED_FROM_DEEP_VERIFIED_EPISODE_PARTITIONS"
    alternative_paths_without_first_payout = (
        alternative_path_count - totals["first_payouts"]
    )
    semantic_audit = {
        "combine_to_xfa_transition_count": totals["xfa_paths_started"],
        "alternative_path_multiplier": len(PATHS),
        "expected_standard_plus_consistency_path_count": alternative_path_count,
        "first_payout_path_observation_count": totals["first_payouts"],
        "alternative_paths_without_observed_first_payout": (
            alternative_paths_without_first_payout
        ),
        "transition_to_alternative_path_identity_valid": (
            alternative_path_count == totals["xfa_paths_started"] * len(PATHS)
        ),
        "first_payout_observations_within_alternative_path_bound": (
            0 <= totals["first_payouts"] <= alternative_path_count
        ),
        "first_payout_observations_are_combine_to_xfa_transitions": False,
        "first_payouts_above_transition_count_can_be_expected": True,
        "duplicate_transition_inflation_detected": False,
        "duplicate_transition_verdict_basis": audit_proof_status,
        "legacy_subminimum_marker_count": (
            int(sealed_episode_audit.get("legacy_subminimum_marker_count", 0))
            if sealed_episode_audit is not None
            else None
        ),
        "legacy_subminimum_marker_gross": (
            _required_float(
                sealed_episode_audit.get("legacy_subminimum_marker_gross", 0.0),
                label="legacy subminimum marker gross",
            )
            if sealed_episode_audit is not None
            else None
        ),
        "legacy_subminimum_marker_affected_finalist_ids": (
            list(
                sealed_episode_audit.get(
                    "legacy_subminimum_marker_affected_finalist_ids", ()
                )
            )
            if sealed_episode_audit is not None
            else None
        ),
        "semantics": (
            "ONE_UNIQUE_COMBINE_TO_XFA_TRANSITION_FANS_OUT_TO_TWO_MUTUALLY_"
            "EXCLUSIVE_DIAGNOSTIC_PATHS;FIRST_PAYOUTS_COUNT_SUCCESSFUL_PATH_"
            "OBSERVATIONS_NOT_UNIQUE_TRANSITIONS_OR_SIMULTANEOUS_REALIZABLE_"
            "PAYOUTS"
        ),
        "probability_denominator_status": (
            "AVAILABLE_BY_COST_SCENARIO_AND_PREDECLARED_PATH_FROM_DEEP_"
            "VERIFIED_EPISODE_PARTITIONS"
            if sealed_episode_audit is not None
            else "UNAVAILABLE_NOT_REDERIVED_FROM_EPISODE_PARTITIONS"
        ),
    }
    if not semantic_audit["transition_to_alternative_path_identity_valid"]:
        raise ActiveRiskDecisionReportError(
            "sealed lifecycle transition-to-alternative-path identity drift"
        )
    return {
        "scope": "CAMPAIGN_WIDE_SEALED_STAGE3_STAGE4_STAGE5",
        "source": (
            "FINAL_RESULT_ECONOMIC_RESULTS_IDENTICAL_TO_DEEP_VERIFIED_"
            "EVIDENCE_BUNDLE_CAMPAIGN_SUMMARY"
        ),
        "rederived_from_stage3_cache": False,
        "paths_are_alternative_not_additive": True,
        "aggregate_path_semantics": (
            "FIRST_PAYOUTS_PAYOUT_CYCLES_AND_TRADER_NET_PAYOUT_ARE_SEALED_"
            "SUMS_ACROSS_STANDARD_AND_CONSISTENCY_DIAGNOSTIC_ALTERNATIVES_"
            "AND_ARE_NOT_ONE_REALIZABLE_COMBINED_TRADER_PATH"
        ),
        "totals": totals,
        "transition_and_alternative_path_audit": semantic_audit,
        "sealed_episode_lifecycle_proof": (
            dict(sealed_episode_audit) if sealed_episode_audit is not None else None
        ),
        "post_payout_survival_observation_count_rederived": raw_survival_count,
        "optional_path_and_survival_breakdown": optional,
        "optional_breakdown_status": (
            "AVAILABLE_IN_SEALED_CAMPAIGN_SUMMARY"
            if optional
            else "UNAVAILABLE_NOT_PERSISTED_IN_SEALED_CAMPAIGN_SUMMARY"
        ),
        "economic_interpretation": {
            "trader_net_payout_label": (
                "NINETY_PERCENT_SPLIT_CASH_OBSERVATIONS_BEFORE_COMBINE_"
                "SUBSCRIPTION_ACTIVATION_PAYMENT_FEES_TAX_AND_DISCRETIONARY_"
                "LIVE_CALL_UP"
            ),
            "payout_cycles_label": (
                "OBSERVED_CYCLES_WITHIN_FROZEN_120_SESSION_XFA_HORIZON_NOT_"
                "EXPECTED_CYCLES_UNTIL_CLOSURE"
            ),
            "combine_governor_controls_applied_in_xfa": False,
            "xfa_projection": "FROZEN_PREREGISTERED_XFA_RISK_OVERLAY",
            "not_modeled": [
                "RTP_RESTRICTION",
                "BACK2FUNDED",
                "DISCRETIONARY_XFA_TO_LIVE_OR_PRO_CALL_UP",
            ],
        },
    }


def _validate_campaign_lifecycle_contains_stage3(
    *,
    campaign_wide: Mapping[str, Any],
    stage3: Mapping[str, Any],
) -> None:
    totals = campaign_wide["totals"]
    stage3_normal = stage3["normal"]
    stage3_stressed = stage3["stressed"]
    for scenario, values in (
        ("normal", stage3_normal),
        ("stressed", stage3_stressed),
    ):
        standard_paths = int(values["standard"]["xfa_paths_started"])
        consistency_paths = int(values["consistency"]["xfa_paths_started"])
        if standard_paths != consistency_paths:
            raise ActiveRiskDecisionReportError(
                f"Stage-3 {scenario} alternative XFA path coverage drift"
            )
    stage3_xfa_paths = sum(
        int(values["standard"]["xfa_paths_started"])
        for values in (stage3_normal, stage3_stressed)
    )
    stage3_first_payouts = sum(
        int(values[path]["first_payouts"])
        for values in (stage3_normal, stage3_stressed)
        for path in PATHS
    )
    stage3_payout_cycles = sum(
        int(values[path]["payout_cycles"])
        for values in (stage3_normal, stage3_stressed)
        for path in PATHS
    )
    stage3_trader_net = sum(
        _required_float(
            values[path]["trader_net_payout"],
            label=f"Stage-3 {path} trader net payout",
        )
        for values in (stage3_normal, stage3_stressed)
        for path in PATHS
    )
    stage3_comparable = {
        "xfa_paths_started": stage3_xfa_paths,
        "first_payouts": stage3_first_payouts,
        "payout_cycles": stage3_payout_cycles,
        "trader_net_payout": stage3_trader_net,
    }
    for field_name, stage3_value in stage3_comparable.items():
        campaign_value = _required_float(
            totals[field_name],
            label=f"campaign-wide lifecycle {field_name}",
        )
        if campaign_value + 1e-8 < float(stage3_value):
            raise ActiveRiskDecisionReportError(
                f"sealed campaign-wide lifecycle {field_name} undercounts Stage-3"
            )
    optional = campaign_wide["optional_path_and_survival_breakdown"]
    for path, field_name in (
        ("standard", "xfa_standard_paths"),
        ("consistency", "xfa_consistency_paths"),
    ):
        if field_name not in optional:
            continue
        stage3_path_count = sum(
            int(values[path]["xfa_paths_started"])
            for values in (stage3_normal, stage3_stressed)
        )
        if int(optional[field_name]) < stage3_path_count:
            raise ActiveRiskDecisionReportError(
                f"sealed campaign-wide lifecycle {field_name} undercounts Stage-3"
            )


def build_active_risk_decision_report(
    *,
    manifest_path: Path,
    stage3_cache_dir: Path,
    stage4_cache_dir: Path | None = None,
    stage5_cache_dir: Path | None = None,
    matched_controls_path: Path,
    halving_dir: Path,
    expected_stage3_count: int = 256,
    expected_finalist_count: int | None = None,
    expected_finalist_starts_per_scenario: int | None = None,
    final_result_path: Path | None = None,
    production_state_path: Path | None = None,
) -> dict[str, Any]:
    """Build revision_02 while holding at most one outcome-rich stage cache."""

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
    if len(blocks) != 4 or {block.block_id for block in blocks} != {
        "B1",
        "B2",
        "B3",
        "B4",
    }:
        raise ActiveRiskDecisionReportError(
            "campaign 0026 frozen temporal-block contract is not exactly B1-B4"
        )
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
    if expected_finalist_count is not None and len(finalists) != int(
        expected_finalist_count
    ):
        raise ActiveRiskDecisionReportError(
            f"development finalist count is {len(finalists)}, expected "
            f"{expected_finalist_count}"
        )
    frozen_policy_specs = (
        authoritative_chain["sealed_finalist_policy_freeze"].get("policy_specs")
    )
    if not isinstance(frozen_policy_specs, list):
        raise ActiveRiskDecisionReportError("sealed finalist policy specs are absent")
    expected_xfa_profiles: dict[str, Mapping[str, Any]] = {}
    for frozen_spec in frozen_policy_specs:
        if not isinstance(frozen_spec, Mapping):
            raise ActiveRiskDecisionReportError("sealed finalist policy spec is malformed")
        policy_id = str(frozen_spec.get("policy_id") or "")
        standard_book = frozen_spec.get("xfa_standard_book")
        consistency_book = frozen_spec.get("xfa_consistency_book")
        if not isinstance(standard_book, Mapping) or not isinstance(
            consistency_book, Mapping
        ):
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} lacks both frozen XFA books"
            )
        standard_profile = standard_book.get("xfa_profile")
        consistency_profile = consistency_book.get("xfa_profile")
        if (
            not isinstance(standard_profile, Mapping)
            or not isinstance(consistency_profile, Mapping)
            or dict(standard_profile) != dict(consistency_profile)
            or policy_id in expected_xfa_profiles
        ):
            raise ActiveRiskDecisionReportError(
                f"finalist {policy_id} frozen XFA profile identity drift"
            )
        expected_xfa_profiles[policy_id] = dict(standard_profile)
    if set(expected_xfa_profiles) != finalists:
        raise ActiveRiskDecisionReportError(
            "frozen XFA profile coverage differs from finalist identities"
        )
    expanded_raw_accumulators = {
        policy_id: ExpandedFinalistRawAccumulator(
            policy_id,
            tuple(blocks),
            authoritative_chain["component_trade_index"],
            expected_xfa_profiles[policy_id],
        )
        for policy_id in finalists
    }

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
                payout_reconciliation = _validate_xfa_path_accounting(
                    value,
                    label=f"candidate {policy_id} {scenario} {lifecycle_path}",
                    policy_id=policy_id,
                    scenario=scenario,
                    combine_start_id=start_day,
                    combine_end_day=combine_end_day,
                    xfa_start_day=xfa_start_day,
                    rule_snapshot=sealed_lifecycle["rule_snapshot"],
                )
                lifecycle[scenario][lifecycle_path].add_path(
                    value,
                    payout_reconciliation=payout_reconciliation,
                )
                candidate_lifecycle[scenario][lifecycle_path].add_path(
                    value,
                    payout_reconciliation=payout_reconciliation,
                )
        compact["stage3_xfa_lifecycle"] = {
            "scope": "STAGE3_ONLY_48_STARTS_FULL_CHRONOLOGICAL_HORIZON",
            **{
                scenario: {
                    name: candidate_lifecycle[scenario][name].to_dict()
                    for name in PATHS
                }
                for scenario in SCENARIOS
            },
        }
        if policy_id in finalists:
            expanded_raw_accumulators[policy_id].add_stage(
                stage="stage3",
                compact=compact,
                canonical_raw=canonical_raw,
                full_raw=full_raw,
                lifecycle_rows=lifecycle_rows,
                expected_starts_per_scenario=(
                    EXPECTED_EPISODE_STARTS_PER_SCENARIO
                ),
            )

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
    minimum_stage3_canonical_attempts = (
        len(observed_ids)
        * EXPECTED_EPISODE_STARTS_PER_SCENARIO
        * len(SCENARIOS)
    )
    if int(
        production_context["current_production_funnel"][
            "combine_episodes_completed"
        ]
    ) < minimum_stage3_canonical_attempts:
        raise ActiveRiskDecisionReportError(
            "authoritative canonical attempt counter undercounts reconciled "
            "Stage-3 policy/start/scenario attempts"
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
    if (stage4_cache_dir is None) != (stage5_cache_dir is None):
        raise ActiveRiskDecisionReportError(
            "Stage-4 and Stage-5 caches must be requested together"
        )
    expanded_cache_provenance: dict[str, Any] = {
        "status": "STAGE3_ONLY_EXPANDED_CACHE_DIRS_NOT_REQUESTED",
        "stage4": [],
        "stage5": [],
    }
    if stage4_cache_dir is not None and stage5_cache_dir is not None:
        expanded_cache_provenance = {
            "status": "STAGE3_STAGE4_STAGE5_STREAMED_AND_PARTITION_BOUND",
            "stage4": _stream_expanded_stage_caches(
                stage="stage4",
                cache_dir=stage4_cache_dir,
                manifest=manifest,
                bundle_manifest=authoritative_chain["bundle_manifest"],
                bundle_identity=authoritative_chain["bundle_identity"],
                blocks=blocks,
                expected_policy_ids=promoted96,
                expected_starts_per_scenario=48,
                promoted96=promoted96,
                surviving96=surviving96,
                finalists=finalists,
                accumulators=expanded_raw_accumulators,
            ),
            "stage5": _stream_expanded_stage_caches(
                stage="stage5",
                cache_dir=stage5_cache_dir,
                manifest=manifest,
                bundle_manifest=authoritative_chain["bundle_manifest"],
                bundle_identity=authoritative_chain["bundle_identity"],
                blocks=blocks,
                expected_policy_ids=finalists,
                expected_starts_per_scenario=96,
                promoted96=promoted96,
                surviving96=surviving96,
                finalists=finalists,
                accumulators=expanded_raw_accumulators,
            ),
        }
    expanded_raw_audits = {
        policy_id: accumulator.to_dict()
        for policy_id, accumulator in expanded_raw_accumulators.items()
    }
    expanded_behavior_profiles = {
        policy_id: accumulator.cumulative_behavior_profile()
        for policy_id, accumulator in expanded_raw_accumulators.items()
    }
    finalist_matrix = _sealed_finalist_matrix(
        frontier=authoritative_chain["sealed_finalist_frontier"],
        finalists=finalists,
        candidates_by_id=candidates_by_id,
        blocks=blocks,
        component_market_map=authoritative_chain["component_market_map"],
        expanded_raw_audits=expanded_raw_audits,
        expanded_behavior_profiles=expanded_behavior_profiles,
        expected_starts_per_scenario=expected_finalist_starts_per_scenario,
    )
    stage3_xfa_lifecycle = {
        "scope": "STAGE3_ONLY_48_STARTS_FULL_CHRONOLOGICAL_HORIZON",
        "source": "STAGE3_CACHES_REDERIVED_AND_EVIDENCE_BUNDLE_BOUND",
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
    }
    campaign_wide_lifecycle = _sealed_campaign_wide_lifecycle_totals(
        authoritative_chain["economic_results"],
        sealed_episode_audit=authoritative_chain[
            "sealed_campaign_lifecycle_audit"
        ],
    )
    _validate_campaign_lifecycle_contains_stage3(
        campaign_wide=campaign_wide_lifecycle,
        stage3=stage3_xfa_lifecycle,
    )

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
            "preregistered_deep_guard_count_reused": 2,
            "additional_deep_guard_performed_by_report": False,
            "stage3_cache_partitions_reproduced_from_sealed_bundle": True,
            "stage3_replay_fields_redriven_from_sealed_daily_paths": True,
            "episode_level_peak_exposure_source": (
                "SEALED_AUTHORITATIVE_REPLAY_FIELDS"
            ),
            "daily_path_peak_exposure_exact_reconciliation_claimed": False,
            "daily_path_peak_exposure_tie_order_diagnostic_only": True,
            "candidate_risk_exposure_suppression_strictly_redriven": True,
            "unsealed_order_sensitive_behavior_cache_hash_published": False,
            "sealed_normalized_behavior_fingerprint_published": True,
            "sealed_campaign_summary_reconciled_to_stage3_caches": True,
            "matched_controls_and_halving_bound_to_sealed_bundle": True,
            "full_pass_xfa_lifecycle_bijection_valid": True,
            "authoritative_xfa_source_path_profile_rule_hashes_valid": True,
            "xfa_horizon_chronology_and_event_accounting_valid": True,
            "stage3_and_campaign_wide_lifecycle_scopes_separated": True,
            "campaign_wide_lifecycle_totals_sealed_and_final_result_bound": True,
            "canonical_attempts_reconciled_to_multi_horizon_partition_formula": True,
            "sealed_expanded_finalist_frontier_bound_to_stage5": True,
            "expanded_finalist_logic_immutable_from_stage3": True,
            "expanded_finalist_decision_metrics_rederived_from_raw_caches": True,
            "expanded_finalist_runtime_behavior_merge_hash_rederived": True,
            "expanded_finalist_authoritative_account_trade_behavior_rederived": True,
            "expanded_finalist_cumulative_economic_behavior_clusters_rederived": True,
            "finalist_governor_membership_and_component_freeze_deep_verified": True,
            "frozen_cost_horizon_account_and_xfa_contract_verified": True,
            "expanded_192_matched_controls_available": False,
            "expanded_stage4_stage5_caches_streamed": (
                stage4_cache_dir is not None and stage5_cache_dir is not None
            ),
        },
        "production_context": production_context,
        "frozen_finalist_policy_specs": authoritative_chain[
            "sealed_finalist_policy_freeze"
        ],
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
        "expanded_development_finalists": finalist_matrix,
        "stage3_xfa_lifecycle": stage3_xfa_lifecycle,
        "campaign_wide_sealed_xfa_lifecycle_totals": campaign_wide_lifecycle,
        "posthoc_behavioral_clustering": clustering,
        "known_interpretation_limits": [
            "All evidence is development-only and is not independent confirmation.",
            "Temporal blocks are descriptive source blocks; overlapping rolling episode starts are not independent observations.",
            "Combine raw pass rates are conservative lower bounds over every frozen start; evaluable rates exclude only DATA_CENSORED paths, while OPERATIONAL_HORIZON_NOT_REACHED remains an observed no-pass-by-horizon outcome.",
            "Risk utilisation covers NORMAL canonical 90-day decision events and declared nominal charges only; it is not time-weighted actual stop-risk or duty cycle.",
            "Episode-level maximum mini-equivalent and directional exposure are the sealed authoritative replay values. The enriched daily exposure projection is diagnostic only because it does not preserve the component-priority tie-break at coincident entry/exit timestamps.",
            "Exposure signatures are outcome-agnostic duty/exposure matching evidence, not risk utilisation.",
            "Foregone realized PnL is ex-post diagnostic and was never a routing input.",
            "Detailed Standard and Consistency XFA path metrics for expanded finalists are rederived from streamed Stage-3/4/5 FULL-horizon evidence when the expanded cache inputs are requested; the modes remain mutually alternative.",
            "Campaign-wide XFA totals are sealed final-result diagnostics covering Stage 3+4+5; Standard and Consistency are alternatives, so their payout observations must not be interpreted as one realizable combined payout.",
            "The eight expanded finalists use the same B1–B4 development history; Stage-4/5 increments are no-retune development replays, not holdout or independent confirmation.",
            "Matched controls exist only for the Stage-3 48-start slice; no 192-start finalist-versus-control superiority claim is available.",
            "The 192 rolling starts overlap within four source blocks and are descriptive trajectories, not 192 independent replications.",
            "Compact frontier per-block target-progress medians and risk-utilisation quantiles are approximate; the expanded finalist raw audit publishes exact streamed replacements, while the compact values remain visibly qualified.",
            "Control block comparisons are limited to persisted net and target-progress medians because raw block-level control paths are absent from the control schema.",
            "The broad Stage-3 post-hoc clusters remain 48-start account/routing diagnostics. Expanded-finalist economic clusters are separately redriven across cumulative Stage 3+4+5 account trajectories, routing/suppression decisions, and admitted immutable source-trade identities/contributions; neither is independent confirmation.",
            "Sleeve/component contribution and sleeve/market/trade concentration are exact additive diagnostics for cumulative Stage-3/4/5 canonical 90-day Combine paths only; XFA component attribution is not aggregated and no XFA sleeve-contribution claim is made.",
        ],
        "provenance": {
            "manifest": {
                "path": str(manifest_path),
                "sha256": file_sha256(manifest_path),
            },
            "stage3_caches": cache_provenance,
            "expanded_stage_caches": expanded_cache_provenance,
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
        (
            "Canonical account attempts and persisted episode rows have distinct "
            "units: "
            f"{report['production_context']['episode_dataset_accounting']['canonical_account_episode_attempts']:,} "
            "attempts versus "
            f"{report['production_context']['episode_dataset_accounting']['persisted_multi_horizon_episode_rows']:,} "
            "multi-horizon rows; the frozen per-stage partition formula reconciles exactly."
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
            "## Stage-3-only XFA lifecycle — paths reported separately",
            "",
            "| Cost | Path | Combine attempts | XFA paths | First payouts | First-payout / attempt lower bound | First-payout / evaluable lifecycle | Expected trader payout / attempt lower bound | Post-payout survival evaluable |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for scenario in SCENARIOS:
        for path in PATHS:
            value = report["stage3_xfa_lifecycle"][scenario][path]
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
    campaign_lifecycle = report["campaign_wide_sealed_xfa_lifecycle_totals"]
    campaign_totals = campaign_lifecycle["totals"]
    path_audit = campaign_lifecycle["transition_and_alternative_path_audit"]
    lines.extend(
        [
            "",
            "## Campaign-wide sealed XFA lifecycle totals — Stage 3+4+5",
            "",
            (
                "These are final-result totals sealed in the EvidenceBundle campaign "
                "summary. First payouts, payout cycles, and trader payout are sums "
                "across the alternative Standard and Consistency diagnostic paths; "
                "they are not one realizable combined trader path."
            ),
            "",
            "| XFA transitions | First-payout observations | Payout-cycle observations | 90%-split cash observations, pre-fees/tax |",
            "|---:|---:|---:|---:|",
            (
                f"| {campaign_totals['xfa_paths_started']} | "
                f"{campaign_totals['first_payouts']} | "
                f"{campaign_totals['payout_cycles']} | "
                f"{_money(campaign_totals['trader_net_payout'])} |"
            ),
            "",
            (
                f"Exact semantic audit: {path_audit['combine_to_xfa_transition_count']:,} "
                "unique Combine-to-XFA transitions fan out to "
                f"{path_audit['expected_standard_plus_consistency_path_count']:,} "
                "mutually alternative Standard/Consistency paths; "
                f"{path_audit['first_payout_path_observation_count']:,} paths "
                "observe a first payout and "
                f"{path_audit['alternative_paths_without_observed_first_payout']:,} do not. "
                "First-payout observations are neither duplicate transitions nor "
                "simultaneously realizable payouts."
            ),
            "",
            (
                "Path-specific Standard/Consistency denominators and censoring are "
                "rederived from the sealed episodes and retained in JSON. A pooled "
                "probability across both mutually exclusive modes is intentionally "
                "not reported."
            ),
        ]
    )
    optional = campaign_lifecycle["optional_path_and_survival_breakdown"]
    if optional:
        lines.extend(
            [
                "",
                "Optional sealed breakdown: `"
                + json.dumps(optional, sort_keys=True, separators=(",", ":"))
                + "`.",
            ]
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
            "## Expanded development finalists — cumulative Stage 3+4+5",
            "",
            (
                "These are no-retune 192-start development trajectories over only "
                "four source blocks. Starts overlap; matched controls cover the "
                "Stage-3 48-start slice only."
            ),
            "",
            "| Finalist | Starts / cost | N passes | S passes | N pass rate | S pass rate | S target P25 | S target median | S net total | S MLL | Pass blocks | Economic behavior cluster | Exact account/trade cluster |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for finalist in report["expanded_development_finalists"]["rows"]:
        normal = finalist["normal"]
        stressed = finalist["stressed"]
        lines.append(
            f"| {finalist['policy_id']} | {finalist['starts_per_scenario']} | "
            f"{normal['pass_count']} | {stressed['pass_count']} | "
            f"{_percent(normal['pass_rate'])} | {_percent(stressed['pass_rate'])} | "
            f"{_percent(stressed['target_progress_p25'])} | "
            f"{_percent(stressed['target_progress_median'])} | "
            f"{_money(stressed['net_total'])} | "
            f"{_percent(stressed['mll_breach_rate'])} | "
            f"{stressed['pass_block_count']} / {finalist['effective_independent_source_block_count']} | "
            f"{finalist.get('expanded_economic_behavior_cluster') or '-'} | "
            f"{finalist.get('expanded_exact_account_behavior_cluster') or '-'} |"
        )
    lines.extend(
        [
            "",
            "Expanded matched-control deltas are `UNAVAILABLE`: Stage 4/5 did not persist matched controls. The JSON retains each finalist's valid Stage-3-only 48-start deltas to static partition, best sleeve, equal-risk pool, always-on pool, and exposure-matched random priority.",
            "",
            "## Expanded finalist XFA alternatives — exact streamed paths",
            "",
            "| Finalist | Cost | Alternative | Combine attempts | XFA transitions | First payouts | Closure before first payout | Expected 90%-split cash / new attempt, pre-fees/tax | Post-payout survival evaluable |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for finalist in report["expanded_development_finalists"]["rows"]:
        lifecycle = finalist["expanded_standard_consistency_xfa_lifecycle_exact"]
        for scenario in SCENARIOS:
            for path in PATHS:
                value = lifecycle[scenario][path]
                lower = value["unconditional_lower_bound"]
                evaluable = value["evaluable_only"]
                lines.append(
                    f"| {finalist['policy_id']} | {scenario} | {path} | "
                    f"{value['combine_attempts']} | {value['xfa_paths_started']} | "
                    f"{value['first_payouts']} | "
                    f"{value['closure_before_first_payout_count']} | "
                    f"{_money(lower['expected_trader_payout_per_combine_attempt'])} | "
                    f"{_percent(evaluable['post_payout_survival_probability_conditional_on_evaluable_first_payout'])} |"
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
) -> Mapping[str, Any]:
    """Atomically publish JSON/Markdown and their hash-bound commit receipt."""

    if json_path.parent.resolve() != markdown_path.parent.resolve():
        raise ActiveRiskDecisionReportError(
            "decision-report artifacts must share one atomic publication root"
        )
    if json_path.name != REPORT_JSON_NAME or markdown_path.name != REPORT_MARKDOWN_NAME:
        raise ActiveRiskDecisionReportError(
            "decision-report artifact names differ from the frozen revision-02 contract"
        )
    return seal_active_risk_decision_report(
        report,
        markdown_text=render_markdown(report),
        output_dir=json_path.parent,
    )


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
        stage4_cache_dir=(
            root
            / "data/cache/economic_production"
            / CAMPAIGN_ID
            / "stage4_active_batches"
        ),
        stage5_cache_dir=(
            root
            / "data/cache/economic_production"
            / CAMPAIGN_ID
            / "stage5_active_batches"
        ),
        matched_controls_path=report_dir / "matched_controls.json",
        halving_dir=report_dir / "successive_halving",
        expected_stage3_count=args.expected_stage3_count,
        expected_finalist_count=8,
        expected_finalist_starts_per_scenario=192,
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
    "verify_active_risk_decision_report_seal",
    "write_active_risk_decision_report",
]
