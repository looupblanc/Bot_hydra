"""Deterministic post-selection loader for active-risk signal bindings.

The forward package needs numeric thresholds, not quantiles that could be
recalibrated on future bars.  This module recovers those thresholds from the
persisted cheap-screen rows, proves them against the immutable feature matrix,
and constructs the eighteen :class:`FrozenSignalBinding` objects expected by
``active_risk_package``.

Package construction is deliberately downstream of both commit markers.  The
loader itself is read-only; the convenience builder refuses to run unless the
decision-report and frozen-selection seals verify.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    policy_from_mapping,
)
from hydra.economic_evolution.schema import EconomicRole, SleeveSpec
from hydra.production.active_risk_report_seal import (
    REPORT_JSON_NAME,
    verify_active_risk_decision_report_seal,
)
from hydra.compute.result_writer import AtomicResultWriter
from hydra.production.frozen_book_selection import (
    SELECTION_JSON_NAME,
    verify_frozen_book_selection_seal,
)
from hydra.production.portfolio_books import SleeveRecord, stable_hash
from hydra.research.turbo_feature_builder import (
    FEATURE_BUNDLE_VERSION,
    FEATURE_DAG_HASH,
)
from hydra.shadow.active_risk_package import (
    ACTIVE_RISK_SOURCE_SLEEVE_COUNT,
    FrozenSignalBinding,
    ImmutableActiveRiskShadowPackage,
    build_active_risk_shadow_package,
    reconstruct_active_risk_shadow_package,
)
from hydra.shadow.package_factory import write_shadow_package


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_SCREEN_NAME = "cheap_screen_results.jsonl"
_FEATURE_SCHEMA = "hydra_canonical_feature_store_v2"
_FEATURE_AVAILABILITY_CONTRACT = "completed_source_bar_at_or_before_decision"
_PILOT_SOURCE_CAMPAIGN = "hydra_economic_evolution_pilot_0001"
_PILOT_FALLBACK_MANIFEST = (
    "config/v7/active_risk_pilot_0001_calibration_fallback.json"
)
EXPORT_AUDIT_NAME = "binding_audit.json"
EXPORT_RECEIPT_NAME = "forward_shadow_export_receipt.json"
EXPORT_RECEIPT_SCHEMA = "hydra_active_risk_forward_shadow_export_seal_v1"


class ActiveRiskBindingLoaderError(RuntimeError):
    """A frozen source artifact is missing, ambiguous, or divergent."""


@dataclass(frozen=True, slots=True)
class FrozenActiveRiskSources:
    """Typed sources and their verified numeric signal bindings."""

    combine_policy: ActiveRiskPoolPolicy
    sleeve_specs: Mapping[str, SleeveSpec]
    sleeve_records: Mapping[str, SleeveRecord]
    component_fingerprints: Mapping[str, str]
    signal_bindings: Mapping[str, FrozenSignalBinding]
    audit: Mapping[str, Any]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _evidence_bundle_content_hash(value: Mapping[str, Any]) -> str:
    """Reproduce HYDRA_EVIDENCE_BUNDLE_V1 canonical bytes exactly."""

    payload = (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActiveRiskBindingLoaderError(f"{label} is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise ActiveRiskBindingLoaderError(f"{label} is not an object: {path}")
    return value


def _inside(root: Path, path: str | Path, *, label: str) -> Path:
    candidate = Path(path)
    resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ActiveRiskBindingLoaderError(f"{label} escapes repository root") from exc
    return resolved


def _relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root).as_posix()


def _require_sha256(value: Any, *, label: str) -> str:
    result = str(value or "")
    if _SHA256.fullmatch(result) is None:
        raise ActiveRiskBindingLoaderError(f"{label} is not a lowercase SHA-256")
    return result


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _utc(value: Any, *, label: str) -> datetime:
    text = str(value or "")
    if not text.endswith("Z"):
        raise ActiveRiskBindingLoaderError(f"{label} is not an ISO-8601 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise ActiveRiskBindingLoaderError(
            f"{label} is not an ISO-8601 UTC timestamp"
        ) from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ActiveRiskBindingLoaderError(f"{label} is not UTC")
    return parsed


def _sleeve_spec(raw: Mapping[str, Any]) -> SleeveSpec:
    try:
        spec = SleeveSpec(
            sleeve_id=str(raw["sleeve_id"]),
            component_ids=tuple(str(value) for value in raw["component_ids"]),
            market=str(raw["market"]),
            execution_market=str(raw["execution_market"]),
            timeframe=str(raw["timeframe"]),
            session_code=int(raw["session_code"]),
            trigger_feature=str(raw["trigger_feature"]),
            trigger_operator=str(raw["trigger_operator"]),
            trigger_quantile=float(raw["trigger_quantile"]),
            context_feature=(
                None if raw.get("context_feature") is None else str(raw["context_feature"])
            ),
            context_operator=(
                None
                if raw.get("context_operator") is None
                else str(raw["context_operator"])
            ),
            context_quantile=(
                None
                if raw.get("context_quantile") is None
                else float(raw["context_quantile"])
            ),
            side=int(raw["side"]),
            holding_bars=int(raw["holding_bars"]),
            exit_style=str(raw["exit_style"]),
            role=EconomicRole(str(raw["role"])),
            source_campaign=str(raw["source_campaign"]),
            lineage_id=str(raw["lineage_id"]),
            version=int(raw.get("version", 1)),
            inherited_status=raw.get("inherited_status"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskBindingLoaderError("frozen sleeve specification is invalid") from exc
    if dict(raw) != spec.to_dict():
        raise ActiveRiskBindingLoaderError(
            f"frozen sleeve specification hash/field drift: {spec.sleeve_id}"
        )
    return spec


def _source_manifests(root: Path, campaign_ids: set[str]) -> dict[str, dict[str, Any]]:
    found: dict[str, tuple[Path, dict[str, Any]]] = {}
    for path in sorted((root / "config/v7").glob("*.json")):
        value = _json(path, label="source campaign manifest")
        campaign_id = str(value.get("campaign_id") or "")
        if campaign_id not in campaign_ids:
            continue
        if campaign_id in found:
            raise ActiveRiskBindingLoaderError(
                f"source campaign manifest is ambiguous: {campaign_id}"
            )
        if not isinstance(value.get("cheap_screen_policy"), Mapping):
            raise ActiveRiskBindingLoaderError(
                f"source campaign lacks cheap-screen policy: {campaign_id}"
            )
        claimed_hashes = [
            (field, str(value.get(field) or ""))
            for field in ("manifest_hash", "preregistration_hash")
            if value.get(field)
        ]
        if len(claimed_hashes) != 1:
            raise ActiveRiskBindingLoaderError(
                f"source campaign manifest must declare exactly one self-hash: {campaign_id}"
            )
        hash_field, claimed_manifest_hash = claimed_hashes[0]
        manifest_body = dict(value)
        manifest_body.pop(hash_field, None)
        if stable_hash(manifest_body) != claimed_manifest_hash:
            raise ActiveRiskBindingLoaderError(
                f"source campaign manifest hash drift: {campaign_id}"
            )
        found[campaign_id] = (path, value)
    if set(found) != campaign_ids:
        missing = sorted(campaign_ids - set(found))
        raise ActiveRiskBindingLoaderError(
            "source campaign manifest is missing: " + ", ".join(missing)
        )
    return {
        campaign_id: {
            "path": _relative(root, path),
            "sha256": _file_sha256(path),
            "manifest": value,
        }
        for campaign_id, (path, value) in found.items()
    }


def _screen_policy_for_file(root: Path, path: Path) -> Mapping[str, Any] | None:
    preregistration = path.parent / "preregistration_copy.json"
    if preregistration.is_file():
        value = _json(preregistration, label="cheap-screen preregistration")
        policy = value.get("cheap_screen_policy")
        if isinstance(policy, Mapping):
            return policy
    return None


def _pilot_fallback(root: Path) -> dict[str, Any]:
    path = _inside(root, _PILOT_FALLBACK_MANIFEST, label="pilot fallback manifest")
    value = _json(path, label="pilot fallback manifest")
    claimed = str(value.get("manifest_hash") or "")
    body = dict(value)
    body.pop("manifest_hash", None)
    rows = value.get("allowed_sleeve_row_sha256")
    if (
        value.get("schema") != "hydra_pilot_0001_calibration_fallback_v1"
        or value.get("source_campaign") != _PILOT_SOURCE_CAMPAIGN
        or value.get("fallback_reason")
        != "ORIGINAL_CHEAP_SCREEN_LEDGER_NOT_PERSISTED"
        or value.get("outcome_fields_used") is not False
        or value.get("threshold_identity_required") is not True
        or value.get("future_directory_discovery_prohibited") is not True
        or not claimed
        or stable_hash(body) != claimed
        or not isinstance(rows, Mapping)
        or not rows
    ):
        raise ActiveRiskBindingLoaderError("pilot calibration fallback manifest drift")
    for sleeve_id, digest in rows.items():
        if not str(sleeve_id) or _SHA256.fullmatch(str(digest or "")) is None:
            raise ActiveRiskBindingLoaderError("pilot fallback row allowlist drift")
    ledger = _inside(
        root, str(value.get("exact_replay_path") or ""), label="pilot fallback ledger"
    )
    if _file_sha256(ledger) != _require_sha256(
        value.get("exact_replay_file_sha256"), label="pilot fallback ledger"
    ):
        raise ActiveRiskBindingLoaderError("pilot fallback ledger hash drift")
    return {**value, "manifest_path": _relative(root, path), "ledger_path": ledger}


def _screen_index(
    root: Path,
    specs: Mapping[str, SleeveSpec],
) -> tuple[
    dict[str, list[tuple[Path, dict[str, Any], Mapping[str, Any] | None]]],
    dict[str, Any],
]:
    """Read only the originating ledgers plus one explicit hash-bound fallback."""

    output = {sleeve_id: [] for sleeve_id in specs}
    pilot_ids = {
        sleeve_id
        for sleeve_id, spec in specs.items()
        if spec.source_campaign == _PILOT_SOURCE_CAMPAIGN
    }
    fallback = _pilot_fallback(root) if pilot_ids else None
    if fallback is not None and set(fallback["allowed_sleeve_row_sha256"]) != pilot_ids:
        raise ActiveRiskBindingLoaderError(
            "pilot fallback allowlist differs from the frozen sleeve bank"
        )
    source_paths: dict[str, Path] = {}
    for spec in specs.values():
        if spec.source_campaign == _PILOT_SOURCE_CAMPAIGN:
            assert fallback is not None
            source_paths[spec.source_campaign] = fallback["ledger_path"]
            continue
        token = spec.source_campaign.removeprefix("hydra_economic_evolution_")
        expected = _inside(
            root,
            f"reports/economic_evolution/{token}/{_SOURCE_SCREEN_NAME}",
            label=f"originating cheap-screen ledger {spec.source_campaign}",
        )
        if not expected.is_file():
            raise ActiveRiskBindingLoaderError(
                f"originating cheap-screen ledger is absent: {spec.source_campaign}"
            )
        source_paths[spec.source_campaign] = expected

    selected_files = sorted(set(source_paths.values()))
    for path in selected_files:
        policy = _screen_policy_for_file(root, path)
        if policy is None:
            raise ActiveRiskBindingLoaderError(
                f"cheap-screen ledger lacks preregistered calibration policy: {path}"
            )
        line_number = 0
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise ValueError("row is not an object")
                    sleeve_id = str(row.get("sleeve_id") or "")
                    spec = specs.get(sleeve_id)
                    if spec is None or source_paths[spec.source_campaign] != path:
                        continue
                    if spec.source_campaign == _PILOT_SOURCE_CAMPAIGN and stable_hash(
                        row
                    ) != str(
                        (fallback or {})["allowed_sleeve_row_sha256"][sleeve_id]
                    ):
                        raise ActiveRiskBindingLoaderError(
                            f"pilot fallback row hash drift: {sleeve_id}"
                        )
                    output[sleeve_id].append((path, row, policy))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise ActiveRiskBindingLoaderError(
                f"cheap-screen ledger is malformed at {path}:{line_number}"
            ) from exc
    audit = {
        "source_resolution": "ORIGINATING_LEDGER_OR_EXPLICIT_HASH_BOUND_PILOT_FALLBACK",
        "global_directory_search_used": False,
        "pilot_fallback_manifest_path": (
            fallback["manifest_path"] if fallback is not None else None
        ),
        "pilot_fallback_manifest_hash": (
            fallback["manifest_hash"] if fallback is not None else None
        ),
        "pilot_fallback_ledger_path": (
            _relative(root, fallback["ledger_path"])
            if fallback is not None
            else None
        ),
        "pilot_fallback_ledger_sha256": (
            fallback["exact_replay_file_sha256"]
            if fallback is not None
            else None
        ),
        "pilot_fallback_sleeve_count": len(pilot_ids),
        "source_ledger_sha256": {
            campaign_id: _file_sha256(path)
            for campaign_id, path in sorted(source_paths.items())
        },
    }
    return output, audit


def _same_calibration_policy(
    left: Mapping[str, Any] | None, right: Mapping[str, Any]
) -> bool:
    return left is not None and all(
        left.get(field) == right.get(field)
        for field in ("calibration_start", "calibration_end_exclusive")
    )


def _screen_row(
    root: Path,
    spec: SleeveSpec,
    record: SleeveRecord,
    source_manifest: Mapping[str, Any],
    candidates: Sequence[tuple[Path, dict[str, Any], Mapping[str, Any] | None]],
) -> tuple[Path, dict[str, Any]]:
    source_policy = source_manifest["manifest"]["cheap_screen_policy"]
    compatible: list[tuple[Path, dict[str, Any]]] = []
    for path, row, screen_policy in candidates:
        if not _same_calibration_policy(screen_policy, source_policy):
            continue
        if (
            row.get("sleeve_id") == spec.sleeve_id
            and row.get("structural_fingerprint") == spec.structural_fingerprint
            and row.get("behavioral_fingerprint") == spec.behavioral_fingerprint
            and row.get("execution_fingerprint") == record.immutable_fingerprint
            and row.get("market") == spec.market
            and row.get("execution_market") == spec.execution_market
            and row.get("finite") is True
            and row.get("trigger_threshold") is not None
            and (
                spec.context_feature is None
                or row.get("context_threshold") is not None
            )
        ):
            compatible.append((path, row))
    if len(compatible) != 1:
        raise ActiveRiskBindingLoaderError(
            f"exact cheap-screen calibration row cardinality is {len(compatible)} "
            f"for {spec.sleeve_id}"
        )
    calibrations = {
        (
            float(row["trigger_threshold"]).hex(),
            None
            if row.get("context_threshold") is None
            else float(row["context_threshold"]).hex(),
            str(row["execution_fingerprint"]),
        )
        for _path, row in compatible
    }
    if len(calibrations) != 1:
        raise ActiveRiskBindingLoaderError(
            f"persisted cheap-screen calibrations disagree for {spec.sleeve_id}"
        )
    return compatible[0]


class _FeatureResolver:
    def __init__(
        self,
        root: Path,
        feature_cache_root: Path,
        campaign_data: Mapping[str, Any],
    ) -> None:
        self.root = root
        self.cache_root = feature_cache_root
        period = campaign_data.get("period")
        if (
            campaign_data.get("role") != "DEVELOPMENT_ONLY_Q4_EXCLUDED"
            or not isinstance(period, list)
            or len(period) != 2
            or campaign_data.get("cached_features_only") is not True
            or campaign_data.get("feature_recalculation_allowed") is not False
            or campaign_data.get("q4_access_allowed") is not False
            or campaign_data.get("new_purchase_allowed") is not False
        ):
            raise ActiveRiskBindingLoaderError(
                "campaign cached-data/Q4 exclusion contract drift"
            )
        self.start_inclusive = str(period[0])
        self.end_exclusive = str(period[1])
        self.source_data_sha256 = _require_sha256(
            campaign_data.get("feature_source_fingerprint"),
            label="campaign feature source",
        )
        self.roll_map_sha256 = _require_sha256(
            campaign_data.get("contract_map_sha256"),
            label="campaign contract map",
        )
        self.by_market: dict[str, tuple[Path, dict[str, Any]]] = {}
        self.arrays: dict[tuple[Path, str], np.ndarray] = {}
        for path in sorted(feature_cache_root.glob("*/manifest.json")):
            manifest = _json(path, label="feature matrix manifest")
            unhashed = dict(manifest)
            claimed = str(unhashed.pop("bundle_hash", ""))
            if not claimed or stable_hash(unhashed) != claimed:
                raise ActiveRiskBindingLoaderError(
                    f"feature matrix semantic hash drift: {path}"
                )
            market = str((manifest.get("key") or {}).get("market") or "")
            if market in self.by_market:
                raise ActiveRiskBindingLoaderError(
                    f"feature matrix market is ambiguous: {market}"
                )
            self.by_market[market] = (path, manifest)

    def matrix(self, spec: SleeveSpec) -> tuple[Path, dict[str, Any]]:
        try:
            path, manifest = self.by_market[spec.market]
        except KeyError as exc:
            raise ActiveRiskBindingLoaderError(
                f"feature matrix is absent for {spec.market}"
            ) from exc
        key = manifest.get("key") or {}
        provenance = manifest.get("provenance") or {}
        features = set(provenance.get("features") or ())
        required = {spec.trigger_feature}
        if spec.context_feature is not None:
            required.add(spec.context_feature)
        if (
            manifest.get("schema") != _FEATURE_SCHEMA
            or manifest.get("mutable") is not False
            or int(manifest.get("writer_count", -1)) != 1
            or manifest.get("availability_contract")
            != _FEATURE_AVAILABILITY_CONTRACT
            or key.get("start_inclusive") != self.start_inclusive
            or key.get("end_exclusive") != self.end_exclusive
            or key.get("source_data_sha256") != self.source_data_sha256
            or key.get("roll_map_hash") != self.roll_map_sha256
            or key.get("transformation_version") != FEATURE_BUNDLE_VERSION
            or key.get("feature_dag_hash") != FEATURE_DAG_HASH
            or provenance.get("market") != spec.market
            or provenance.get("execution_market") != spec.execution_market
            or provenance.get("outbound_order_capability") is not False
            or int(provenance.get("q4_access_count_delta", -1)) != 0
            or provenance.get("data_fingerprint") != self.source_data_sha256
            or provenance.get("contract_map_sha256") != self.roll_map_sha256
            or not required <= features
        ):
            raise ActiveRiskBindingLoaderError(
                f"feature matrix execution/provenance drift for {spec.sleeve_id}"
            )
        return path, manifest

    def array(
        self, manifest_path: Path, manifest: Mapping[str, Any], name: str
    ) -> tuple[np.ndarray, str]:
        key = (manifest_path, name)
        metadata = (manifest.get("arrays") or {}).get(name)
        if not isinstance(metadata, Mapping):
            raise ActiveRiskBindingLoaderError(f"feature array is absent: {name}")
        expected = _require_sha256(metadata.get("sha256"), label=f"{name} array")
        if key not in self.arrays:
            path = _inside(
                manifest_path.parent,
                str(metadata.get("path") or ""),
                label=f"{name} array",
            )
            if _file_sha256(path) != expected:
                raise ActiveRiskBindingLoaderError(f"feature array hash drift: {name}")
            try:
                array = np.load(path, mmap_mode="r", allow_pickle=False)
            except (OSError, ValueError) as exc:
                raise ActiveRiskBindingLoaderError(
                    f"feature array cannot be opened: {name}"
                ) from exc
            if (
                list(array.shape) != list(metadata.get("shape") or ())
                or str(array.dtype) != str(metadata.get("dtype") or "")
                or len(array) != int(manifest.get("row_count", -1))
            ):
                raise ActiveRiskBindingLoaderError(
                    f"feature array metadata drift: {name}"
                )
            array.flags.writeable = False
            self.arrays[key] = array
        return self.arrays[key], expected


def _epoch_day(value: str) -> int:
    try:
        return (date.fromisoformat(value) - date(1970, 1, 1)).days
    except ValueError as exc:
        raise ActiveRiskBindingLoaderError(
            f"invalid calibration boundary: {value}"
        ) from exc


def _calibration(
    resolver: _FeatureResolver,
    spec: SleeveSpec,
    policy: Mapping[str, Any],
    screen_row: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    manifest_path, manifest = resolver.matrix(spec)
    day, day_sha = resolver.array(manifest_path, manifest, "session_day")
    session, session_sha = resolver.array(manifest_path, manifest, "session_code")
    start = str(policy["calibration_start"])
    end = str(policy["calibration_end_exclusive"])
    selected = (
        (day >= _epoch_day(start))
        & (day < _epoch_day(end))
        & (session >= 0)
    )

    def threshold(feature: str, quantile: float) -> tuple[float, int, str]:
        values, digest = resolver.array(
            manifest_path, manifest, f"feature__{feature}"
        )
        finite = values[selected]
        finite = finite[np.isfinite(finite)]
        if len(finite) < 100:
            raise ActiveRiskBindingLoaderError(
                f"insufficient frozen calibration observations: {spec.sleeve_id}/{feature}"
            )
        return float(np.quantile(finite, quantile)), int(len(finite)), digest

    trigger, trigger_count, trigger_sha = threshold(
        spec.trigger_feature, spec.trigger_quantile
    )
    context: float | None = None
    context_count: int | None = None
    context_sha: str | None = None
    if spec.context_feature is not None:
        context, context_count, context_sha = threshold(
            spec.context_feature, float(spec.context_quantile)
        )
    persisted_trigger = float(screen_row["trigger_threshold"])
    persisted_context = (
        None
        if screen_row.get("context_threshold") is None
        else float(screen_row["context_threshold"])
    )
    if (
        not math.isfinite(persisted_trigger)
        or trigger.hex() != persisted_trigger.hex()
        or (
            context is None
            and persisted_context is not None
            or context is not None
            and (
                persisted_context is None
                or context.hex() != persisted_context.hex()
            )
        )
    ):
        raise ActiveRiskBindingLoaderError(
            f"cheap-screen threshold cannot be reproduced: {spec.sleeve_id}"
        )
    key = manifest["key"]
    provenance = manifest["provenance"]
    return (
        {
            "trigger_threshold": trigger,
            "context_threshold": context,
            "trigger_finite_observation_count": trigger_count,
            "context_finite_observation_count": context_count,
            "calibration_start": start,
            "calibration_end_exclusive": end,
            "feature_matrix_manifest_path": _relative(
                resolver.root, manifest_path
            ),
            "feature_matrix_manifest_sha256": _file_sha256(manifest_path),
            "feature_matrix_schema": manifest["schema"],
            "feature_matrix_bundle_hash": manifest["bundle_hash"],
            "feature_matrix_source_data_sha256": key["source_data_sha256"],
            "feature_matrix_roll_map_sha256": key["roll_map_hash"],
            "feature_matrix_market": provenance["market"],
            "feature_matrix_execution_market": provenance["execution_market"],
            "feature_bundle_version": key["transformation_version"],
            "feature_dag_hash": key["feature_dag_hash"],
            "trigger_array_sha256": trigger_sha,
            "context_array_sha256": context_sha,
            "session_day_array_sha256": day_sha,
            "session_code_array_sha256": session_sha,
        },
        {
            "feature_manifest": _relative(resolver.root, manifest_path),
            "feature_bundle_hash": str(manifest["bundle_hash"]),
        },
    )


def load_frozen_active_risk_sources(
    *,
    repository_root: str | Path,
    campaign_manifest_path: str | Path,
    frozen_finalist_declaration: Mapping[str, Any],
    feature_cache_root: str | Path = "data/cache/economic_evolution/features",
) -> FrozenActiveRiskSources:
    """Rebuild all eighteen frozen bindings without recalibrating a threshold."""

    root = Path(repository_root).resolve()
    campaign_path = _inside(
        root, campaign_manifest_path, label="active-risk campaign manifest"
    )
    campaign = _json(campaign_path, label="active-risk campaign manifest")
    claimed_manifest_hash = str(campaign.get("manifest_hash") or "")
    unhashed_campaign = dict(campaign)
    unhashed_campaign.pop("manifest_hash", None)
    if not claimed_manifest_hash or stable_hash(unhashed_campaign) != claimed_manifest_hash:
        raise ActiveRiskBindingLoaderError("active-risk campaign manifest hash drift")
    bank = campaign.get("sleeve_bank")
    members = bank.get("members") if isinstance(bank, Mapping) else None
    if (
        not isinstance(members, list)
        or int(bank.get("member_count", -1)) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or len(members) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
    ):
        raise ActiveRiskBindingLoaderError("campaign sleeve bank is not exactly eighteen")
    by_member = {
        str(member.get("sleeve_id") or ""): member
        for member in members
        if isinstance(member, Mapping)
    }
    if len(by_member) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT or "" in by_member:
        raise ActiveRiskBindingLoaderError("campaign sleeve bank identity drift")

    declaration = frozen_finalist_declaration
    membership = declaration.get("membership")
    active_policy = declaration.get("active_risk_policy")
    if not isinstance(membership, list) or not isinstance(active_policy, Mapping):
        raise ActiveRiskBindingLoaderError("frozen finalist declaration is incomplete")
    try:
        policy = policy_from_mapping(active_policy)
    except (KeyError, TypeError, ValueError) as exc:
        raise ActiveRiskBindingLoaderError("frozen active-risk policy is invalid") from exc
    if (
        policy.to_dict() != dict(active_policy)
        or declaration.get("active_risk_policy_sha256")
        != stable_hash(policy.to_dict())
        or declaration.get("membership_sha256") != stable_hash(membership)
        or len(membership) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or set(policy.component_priority) != set(by_member)
    ):
        raise ActiveRiskBindingLoaderError("frozen finalist policy/membership drift")
    declaration_by_id = {
        str(row.get("component_id") or ""): row
        for row in membership
        if isinstance(row, Mapping)
    }
    if set(declaration_by_id) != set(policy.component_priority):
        raise ActiveRiskBindingLoaderError("frozen finalist membership coverage drift")

    specs: dict[str, SleeveSpec] = {}
    records: dict[str, SleeveRecord] = {}
    components: dict[str, str] = {}
    for sleeve_id in policy.component_priority:
        member = by_member[sleeve_id]
        declaration_row = declaration_by_id[sleeve_id]
        raw_spec = member.get("sleeve_specification")
        raw_record = member.get("record")
        if not isinstance(raw_spec, Mapping) or not isinstance(raw_record, Mapping):
            raise ActiveRiskBindingLoaderError(
                f"campaign sleeve source is incomplete: {sleeve_id}"
            )
        spec = _sleeve_spec(raw_spec)
        try:
            record = SleeveRecord.from_mapping(raw_record)
        except (KeyError, TypeError, ValueError) as exc:
            raise ActiveRiskBindingLoaderError(
                f"campaign sleeve record is invalid: {sleeve_id}"
            ) from exc
        if (
            spec.sleeve_id != sleeve_id
            or record.sleeve_id != sleeve_id
            or declaration_row.get("sleeve_specification") != spec.to_dict()
            or declaration_row.get("immutable_fingerprint")
            != record.immutable_fingerprint
            or declaration_row.get("behavioral_fingerprint")
            != record.behavioral_fingerprint
            or declaration_row.get("signal_ledger_sha256")
            != record.signal_ledger_sha256
            or declaration_row.get("trade_ledger_sha256")
            != record.trade_ledger_sha256
            or declaration_row.get("market") != record.market
            or declaration_row.get("contract") != record.contract
            or declaration_row.get("timeframe") != record.timeframe
            or declaration_row.get("session") != record.session
            or declaration_row.get("source_campaign") != record.source_campaign
            or spec.behavioral_fingerprint != record.behavioral_fingerprint
            or spec.market != record.market
            or spec.execution_market != record.contract
            or spec.source_campaign != record.source_campaign
        ):
            raise ActiveRiskBindingLoaderError(
                f"frozen declaration/campaign sleeve drift: {sleeve_id}"
            )
        specs[sleeve_id] = spec
        records[sleeve_id] = record
        components[sleeve_id] = record.immutable_fingerprint

    source_manifests = _source_manifests(
        root, {spec.source_campaign for spec in specs.values()}
    )
    screen_rows, screen_resolution_audit = _screen_index(root, specs)
    campaign_data = campaign.get("data")
    if not isinstance(campaign_data, Mapping):
        raise ActiveRiskBindingLoaderError("active-risk campaign data contract is absent")
    resolver = _FeatureResolver(
        root,
        _inside(root, feature_cache_root, label="feature cache root"),
        campaign_data,
    )
    bindings: dict[str, FrozenSignalBinding] = {}
    binding_audit: list[dict[str, Any]] = []
    for sleeve_id in policy.component_priority:
        spec = specs[sleeve_id]
        record = records[sleeve_id]
        source_manifest = source_manifests[spec.source_campaign]
        screen_path, row = _screen_row(
            root,
            spec,
            record,
            source_manifest,
            screen_rows[sleeve_id],
        )
        calibration, feature_audit = _calibration(
            resolver,
            spec,
            source_manifest["manifest"]["cheap_screen_policy"],
            row,
        )
        binding = FrozenSignalBinding(
            sleeve_id=sleeve_id,
            trigger_feature=spec.trigger_feature,
            trigger_operator=spec.trigger_operator,
            context_feature=spec.context_feature,
            context_operator=spec.context_operator,
            source_execution_fingerprint=record.immutable_fingerprint,
            source_cheap_screen_path=_relative(root, screen_path),
            source_cheap_screen_sha256=_file_sha256(screen_path),
            source_cheap_screen_row_sha256=stable_hash(row),
            **calibration,
        )
        bindings[sleeve_id] = binding
        binding_audit.append(
            {
                "sleeve_id": sleeve_id,
                "binding_fingerprint": binding.fingerprint,
                "source_campaign": spec.source_campaign,
                "source_campaign_manifest_path": source_manifest["path"],
                "source_campaign_manifest_sha256": source_manifest["sha256"],
                "cheap_screen_path": binding.source_cheap_screen_path,
                "cheap_screen_file_sha256": binding.source_cheap_screen_sha256,
                "cheap_screen_row_sha256": binding.source_cheap_screen_row_sha256,
                "trigger_threshold_reproduced_exactly": True,
                "context_threshold_reproduced_exactly": True,
                **feature_audit,
            }
        )
    if len(bindings) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT or len(
        {binding.fingerprint for binding in bindings.values()}
    ) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT:
        raise ActiveRiskBindingLoaderError("frozen binding cardinality/identity drift")
    audit: dict[str, Any] = {
        "schema": "hydra_active_risk_binding_load_audit_v1",
        "campaign_id": campaign["campaign_id"],
        "policy_id": policy.policy_id,
        "binding_count": len(bindings),
        "campaign_manifest_path": _relative(root, campaign_path),
        "campaign_manifest_sha256": _file_sha256(campaign_path),
        "thresholds_loaded_from_persisted_cheap_screen": True,
        "thresholds_recalibrated_for_forward_use": False,
        "all_required_feature_array_hashes_verified": True,
        "all_calibrations_exactly_reproduced": True,
        "screen_source_resolution": screen_resolution_audit,
        "feature_snapshot_binding": {
            "availability_contract": _FEATURE_AVAILABILITY_CONTRACT,
            "start_inclusive": resolver.start_inclusive,
            "end_exclusive": resolver.end_exclusive,
            "source_data_sha256": resolver.source_data_sha256,
            "roll_map_sha256": resolver.roll_map_sha256,
            "q4_access_count_delta": 0,
        },
        "bindings": binding_audit,
    }
    audit["audit_hash"] = stable_hash(audit)
    return FrozenActiveRiskSources(
        combine_policy=policy,
        sleeve_specs=specs,
        sleeve_records=records,
        component_fingerprints=components,
        signal_bindings=bindings,
        audit=audit,
    )


def build_active_risk_package_from_sealed_selection(
    *,
    repository_root: str | Path,
    campaign_manifest_path: str | Path,
    report_dir: str | Path,
    selection_dir: str | Path,
    policy_id: str,
    feature_cache_root: str | Path = "data/cache/economic_evolution/features",
) -> tuple[ImmutableActiveRiskShadowPackage, Mapping[str, Any]]:
    """Verify both seals, load bindings, and call the existing package builder."""

    root = Path(repository_root).resolve()
    report_root = _inside(root, report_dir, label="sealed report directory")
    selection_root = _inside(root, selection_dir, label="sealed selection directory")
    report_receipt = verify_active_risk_decision_report_seal(report_root)
    selection_receipt = verify_frozen_book_selection_seal(
        selection_root, report_dir=report_root
    )
    report = _json(report_root / REPORT_JSON_NAME, label="sealed decision report")
    selection = _json(
        selection_root / SELECTION_JSON_NAME, label="sealed selection manifest"
    )
    declarations = (
        report.get("frozen_finalist_policy_specs") or {}
    ).get("policy_specs")
    if not isinstance(declarations, list):
        raise ActiveRiskBindingLoaderError("sealed report lacks finalist declarations")
    matches = [
        row
        for row in declarations
        if isinstance(row, Mapping) and row.get("policy_id") == policy_id
    ]
    if len(matches) != 1:
        raise ActiveRiskBindingLoaderError(
            "requested policy is absent or duplicated in sealed finalist declarations"
        )
    if not any(
        isinstance(row, Mapping) and row.get("policy_id") == policy_id
        for row in selection.get("selected_books") or ()
    ):
        raise ActiveRiskBindingLoaderError("requested policy was not selected")
    campaign_path = _inside(
        root, campaign_manifest_path, label="active-risk campaign manifest"
    )
    campaign = _json(campaign_path, label="active-risk campaign manifest")
    campaign_sha256 = _file_sha256(campaign_path)
    campaign_contract = (
        report.get("frozen_finalist_policy_specs") or {}
    ).get("campaign_contract")
    report_manifest = (report.get("provenance") or {}).get("manifest")
    report_evidence = (report.get("production_context") or {}).get(
        "evidence_bundle"
    )
    report_evidence_provenance = (
        ((report.get("provenance") or {}).get("production_context") or {}).get(
            "evidence_bundle"
        )
    )
    if (
        not isinstance(campaign_contract, Mapping)
        or not isinstance(report_manifest, Mapping)
        or not isinstance(report_evidence, Mapping)
        or not isinstance(report_evidence_provenance, Mapping)
        or report_manifest.get("sha256") != campaign_sha256
        or campaign_contract.get("manifest_hash") != campaign.get("manifest_hash")
        or campaign_contract.get("source_commit") != campaign.get("source_commit")
        or report.get("campaign_id") != campaign.get("campaign_id")
    ):
        raise ActiveRiskBindingLoaderError(
            "sealed report/campaign manifest provenance drift"
        )
    evidence_path = _inside(
        root,
        (campaign.get("evidence_bundle") or {}).get("lightweight_manifest_path", ""),
        label="EvidenceBundle receipt",
    )
    evidence_receipt = _json(evidence_path, label="EvidenceBundle receipt")
    evidence_manifest_path = _inside(
        root,
        evidence_receipt.get("manifest_path", ""),
        label="EvidenceBundle manifest",
    )
    evidence_manifest = _json(
        evidence_manifest_path, label="EvidenceBundle manifest"
    )
    manifest_core = dict(evidence_manifest)
    observed_bundle_hash = str(manifest_core.pop("bundle_content_sha256", ""))
    evidence_manifest_sha256 = _file_sha256(evidence_manifest_path)
    evidence_bundle_path = _inside(
        root,
        evidence_receipt.get("bundle_path", ""),
        label="EvidenceBundle directory",
    )
    identity_path = evidence_bundle_path / "identity.json"
    identity = _json(identity_path, label="EvidenceBundle identity")
    receipt_comparison = {
        "path": str(evidence_bundle_path),
        "manifest_sha256": evidence_manifest_sha256,
        "bundle_content_sha256": observed_bundle_hash,
        "dataset_row_counts": evidence_manifest.get("dataset_row_counts"),
        "verification": "DEEP_VERIFIED",
    }
    if (
        evidence_receipt.get("campaign_id") != campaign.get("campaign_id")
        or evidence_manifest_sha256
        != _require_sha256(
            evidence_receipt.get("manifest_sha256"),
            label="EvidenceBundle manifest hash",
        )
        or not observed_bundle_hash
        or _evidence_bundle_content_hash(manifest_core) != observed_bundle_hash
        or evidence_manifest.get("campaign_id") != campaign.get("campaign_id")
        or evidence_manifest.get("status") != "COMPLETE"
        or evidence_manifest.get("evidence_status")
        != "FRESH_DEVELOPMENT_EVIDENCE"
        or evidence_manifest.get("reconstruction_flag") is not False
        or evidence_receipt.get("bundle_content_sha256") != observed_bundle_hash
        or evidence_receipt.get("dataset_row_counts")
        != evidence_manifest.get("dataset_row_counts")
        or dict(report_evidence) != receipt_comparison
        or report_evidence_provenance.get("manifest_sha256")
        != evidence_manifest_sha256
        or report_evidence_provenance.get("bundle_content_sha256")
        != observed_bundle_hash
        or report_evidence_provenance.get("dataset_row_counts")
        != evidence_manifest.get("dataset_row_counts")
        or report_evidence_provenance.get("deep_verification") is not True
        or identity.get("campaign_id") != campaign.get("campaign_id")
        or identity.get("source_commit") != campaign.get("source_commit")
        or identity.get("configuration_sha256") != campaign_sha256
    ):
        raise ActiveRiskBindingLoaderError(
            "EvidenceBundle/report/campaign provenance drift"
        )
    sources = load_frozen_active_risk_sources(
        repository_root=root,
        campaign_manifest_path=campaign_path,
        frozen_finalist_declaration=matches[0],
        feature_cache_root=feature_cache_root,
    )
    report_artifact = report_receipt["artifacts"][REPORT_JSON_NAME]
    selection_artifact = selection_receipt["artifact"]
    completed = str(selection.get("selection_completed_at_utc") or "")
    report_sealed_at = str(report_receipt.get("sealed_at_utc") or "")
    selection_sealed_at = str(selection_receipt.get("sealed_at_utc") or "")
    freeze_timestamp_utc = _utc_now()
    freeze_time = _utc(freeze_timestamp_utc, label="forward freeze time")
    if freeze_time < max(
        _utc(report_sealed_at, label="decision report seal time"),
        _utc(selection_sealed_at, label="selection seal time"),
        _utc(completed, label="selection manifest completion time"),
    ):
        raise ActiveRiskBindingLoaderError(
            "forward freeze precedes an immutable report/selection boundary"
        )
    package = build_active_risk_shadow_package(
        sources.combine_policy,
        sources.sleeve_specs,
        sources.sleeve_records,
        sources.component_fingerprints,
        sources.signal_bindings,
        source_commit=str(campaign["source_commit"]),
        decision_report_sealed_at_utc=report_sealed_at,
        selection_completed_at_utc=completed,
        freeze_timestamp_utc=freeze_timestamp_utc,
        campaign_manifest_sha256=campaign_sha256,
        evidence_receipt_sha256=_file_sha256(evidence_path),
        evidence_manifest_sha256=evidence_manifest_sha256,
        evidence_bundle_sha256=_require_sha256(
            evidence_receipt.get("bundle_content_sha256"),
            label="EvidenceBundle content hash",
        ),
        decision_report_sha256=_require_sha256(
            report_artifact.get("sha256"), label="decision report file hash"
        ),
        selection_manifest_sha256=_require_sha256(
            selection_artifact.get("sha256"), label="selection manifest file hash"
        ),
        selection_seal_receipt_hash=_require_sha256(
            selection_receipt.get("receipt_hash"),
            label="selection seal receipt hash",
        ),
        frozen_finalist_declaration=matches[0],
        selection_manifest=selection,
    )
    audit = dict(sources.audit)
    audit.pop("audit_hash", None)
    audit["sealed_chain"] = {
        "decision_report_receipt_hash": report_receipt["receipt_hash"],
        "decision_report_sealed_at_utc": report_sealed_at,
        "selection_receipt_hash": selection_receipt["receipt_hash"],
        "selection_sealed_at_utc": selection_sealed_at,
        "selection_completed_at_utc": completed,
        "campaign_manifest_sha256": campaign_sha256,
        "decision_report_sha256": _require_sha256(
            report_artifact.get("sha256"), label="decision report file hash"
        ),
        "selection_manifest_sha256": _require_sha256(
            selection_artifact.get("sha256"), label="selection manifest file hash"
        ),
        "evidence_receipt_sha256": _file_sha256(evidence_path),
        "evidence_manifest_sha256": evidence_manifest_sha256,
        "evidence_bundle_content_sha256": observed_bundle_hash,
        "deep_evidence_verification_in_sealed_report": True,
        "freeze_timestamp_source": "EXPORTER_CURRENT_UTC_AFTER_ALL_SEALS",
        "freeze_timestamp_utc": freeze_timestamp_utc,
        "package_hash": package.package_hash,
    }
    audit["audit_hash"] = stable_hash(audit)
    return package, audit


def _artifact(path: Path) -> dict[str, Any]:
    return {
        "relative_path": path.name,
        "sha256": _file_sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _validate_complete_export_audit(
    audit: Mapping[str, Any], package: ImmutableActiveRiskShadowPackage
) -> str:
    value = dict(audit)
    claimed = str(value.pop("audit_hash", ""))
    bindings = value.get("bindings")
    source_resolution = value.get("screen_source_resolution")
    feature_snapshot = value.get("feature_snapshot_binding")
    sealed = value.get("sealed_chain")
    if (
        value.get("schema") != "hydra_active_risk_binding_load_audit_v1"
        or value.get("campaign_id")
        != "hydra_active_risk_pool_target_velocity_0026"
        or value.get("policy_id") != package.candidate_id
        or int(value.get("binding_count", -1)) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or value.get("thresholds_loaded_from_persisted_cheap_screen") is not True
        or value.get("thresholds_recalibrated_for_forward_use") is not False
        or value.get("all_required_feature_array_hashes_verified") is not True
        or value.get("all_calibrations_exactly_reproduced") is not True
        or not isinstance(bindings, list)
        or len(bindings) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or not isinstance(source_resolution, Mapping)
        or not isinstance(feature_snapshot, Mapping)
        or not isinstance(sealed, Mapping)
        or not claimed
        or stable_hash(value) != claimed
    ):
        raise ActiveRiskBindingLoaderError("binding audit completeness/hash drift")
    _require_sha256(value.get("campaign_manifest_sha256"), label="audit campaign manifest")
    if (
        not str(value.get("campaign_manifest_path") or "")
        or source_resolution.get("source_resolution")
        != "ORIGINATING_LEDGER_OR_EXPLICIT_HASH_BOUND_PILOT_FALLBACK"
        or source_resolution.get("global_directory_search_used") is not False
        or not isinstance(source_resolution.get("source_ledger_sha256"), Mapping)
        or not source_resolution.get("source_ledger_sha256")
    ):
        raise ActiveRiskBindingLoaderError("binding audit source resolution drift")
    for digest in source_resolution["source_ledger_sha256"].values():
        _require_sha256(digest, label="audit source ledger")
    pilot_count = int(source_resolution.get("pilot_fallback_sleeve_count", -1))
    if pilot_count not in {0, 7}:
        raise ActiveRiskBindingLoaderError("binding audit pilot fallback count drift")
    for field in (
        "pilot_fallback_manifest_path",
        "pilot_fallback_manifest_hash",
        "pilot_fallback_ledger_path",
        "pilot_fallback_ledger_sha256",
    ):
        observed = source_resolution.get(field)
        if pilot_count == 7 and not observed:
            raise ActiveRiskBindingLoaderError("binding audit pilot fallback proof absent")
        if pilot_count == 0 and observed is not None:
            raise ActiveRiskBindingLoaderError("unexpected pilot fallback proof")
    if pilot_count == 7:
        _require_sha256(
            source_resolution.get("pilot_fallback_manifest_hash"),
            label="audit pilot fallback manifest",
        )
        _require_sha256(
            source_resolution.get("pilot_fallback_ledger_sha256"),
            label="audit pilot fallback ledger",
        )
    if (
        feature_snapshot.get("availability_contract")
        != _FEATURE_AVAILABILITY_CONTRACT
        or not str(feature_snapshot.get("start_inclusive") or "")
        or not str(feature_snapshot.get("end_exclusive") or "")
        or int(feature_snapshot.get("q4_access_count_delta", -1)) != 0
    ):
        raise ActiveRiskBindingLoaderError("binding audit feature snapshot drift")
    _require_sha256(
        feature_snapshot.get("source_data_sha256"), label="audit feature source"
    )
    _require_sha256(
        feature_snapshot.get("roll_map_sha256"), label="audit feature roll map"
    )
    immutable_sleeves = package.feature_contract.get("immutable_sleeves")
    combine_policy = (
        (package.sizing_policy.get("combine_book") or {}).get("policy") or {}
    )
    component_priority = combine_policy.get("component_priority")
    if (
        not isinstance(immutable_sleeves, Mapping)
        or not isinstance(component_priority, list)
        or len(component_priority) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT
        or set(map(str, component_priority)) != set(map(str, immutable_sleeves))
    ):
        raise ActiveRiskBindingLoaderError(
            "package immutable-sleeve/priority coverage drift"
        )
    sleeve_ids: set[str] = set()
    binding_fingerprints: set[str] = set()
    for row in bindings:
        if not isinstance(row, Mapping):
            raise ActiveRiskBindingLoaderError("binding audit row is malformed")
        sleeve_id = str(row.get("sleeve_id") or "")
        if (
            not sleeve_id
            or sleeve_id in sleeve_ids
            or not str(row.get("source_campaign") or "")
            or not str(row.get("source_campaign_manifest_path") or "")
            or not str(row.get("cheap_screen_path") or "")
            or not str(row.get("feature_manifest") or "")
            or row.get("trigger_threshold_reproduced_exactly") is not True
            or row.get("context_threshold_reproduced_exactly") is not True
        ):
            raise ActiveRiskBindingLoaderError("binding audit row identity drift")
        sleeve_ids.add(sleeve_id)
        fingerprint = _require_sha256(
            row.get("binding_fingerprint"), label="audit binding fingerprint"
        )
        binding_fingerprints.add(fingerprint)
        for field in (
            "source_campaign_manifest_sha256",
            "cheap_screen_file_sha256",
            "cheap_screen_row_sha256",
            "feature_bundle_hash",
        ):
            _require_sha256(row.get(field), label=f"audit {field}")
        source = immutable_sleeves.get(sleeve_id)
        frozen_binding = (
            source.get("frozen_signal_binding")
            if isinstance(source, Mapping)
            else None
        )
        sleeve_spec = (
            source.get("sleeve_specification")
            if isinstance(source, Mapping)
            else None
        )
        if (
            not isinstance(source, Mapping)
            or not isinstance(frozen_binding, Mapping)
            or not isinstance(sleeve_spec, Mapping)
            or fingerprint != source.get("frozen_signal_binding_sha256")
            or fingerprint != frozen_binding.get("fingerprint")
            or row.get("source_campaign") != sleeve_spec.get("source_campaign")
            or row.get("cheap_screen_path")
            != frozen_binding.get("source_cheap_screen_path")
            or row.get("cheap_screen_file_sha256")
            != frozen_binding.get("source_cheap_screen_sha256")
            or row.get("cheap_screen_row_sha256")
            != frozen_binding.get("source_cheap_screen_row_sha256")
            or row.get("feature_manifest")
            != frozen_binding.get("feature_matrix_manifest_path")
            or row.get("feature_bundle_hash")
            != frozen_binding.get("feature_matrix_bundle_hash")
        ):
            raise ActiveRiskBindingLoaderError(
                f"binding audit differs from packaged sleeve: {sleeve_id}"
            )
    if sleeve_ids != set(map(str, immutable_sleeves)):
        raise ActiveRiskBindingLoaderError(
            "binding audit sleeve set differs from packaged sleeves"
        )
    if len(binding_fingerprints) != ACTIVE_RISK_SOURCE_SLEEVE_COUNT:
        raise ActiveRiskBindingLoaderError("binding audit fingerprints are duplicated")
    for field in (
        "decision_report_receipt_hash",
        "selection_receipt_hash",
        "campaign_manifest_sha256",
        "decision_report_sha256",
        "selection_manifest_sha256",
        "evidence_receipt_sha256",
        "evidence_manifest_sha256",
        "evidence_bundle_content_sha256",
        "package_hash",
    ):
        _require_sha256(sealed.get(field), label=f"audit sealed {field}")
    for field in (
        "decision_report_sealed_at_utc",
        "selection_sealed_at_utc",
        "selection_completed_at_utc",
        "freeze_timestamp_utc",
    ):
        _utc(sealed.get(field), label=f"audit sealed {field}")
    if (
        sealed.get("deep_evidence_verification_in_sealed_report") is not True
        or sealed.get("freeze_timestamp_source")
        != "EXPORTER_CURRENT_UTC_AFTER_ALL_SEALS"
        or sealed.get("freeze_timestamp_utc") != package.freeze_timestamp_utc
        or sealed.get("package_hash") != package.package_hash
        or value.get("campaign_manifest_sha256")
        != sealed.get("campaign_manifest_sha256")
        or sealed.get("campaign_manifest_sha256")
        != package.evidence_provenance.get("campaign_manifest_sha256")
        or sealed.get("evidence_receipt_sha256")
        != package.evidence_provenance.get("evidence_receipt_sha256")
        or sealed.get("evidence_manifest_sha256")
        != package.evidence_provenance.get("evidence_manifest_sha256")
        or sealed.get("evidence_bundle_content_sha256")
        != package.evidence_provenance.get("evidence_bundle_sha256")
        or sealed.get("decision_report_receipt_hash")
        != package.evidence_provenance.get(
            "decision_report_seal_receipt_hash"
        )
        or sealed.get("decision_report_sha256")
        != package.evidence_provenance.get("decision_report_sha256")
        or sealed.get("selection_receipt_hash")
        != package.evidence_provenance.get("selection_seal_receipt_hash")
        or sealed.get("selection_manifest_sha256")
        != package.evidence_provenance.get("selection_manifest_sha256")
        or _utc(sealed["freeze_timestamp_utc"], label="audit freeze")
        < max(
            _utc(sealed["decision_report_sealed_at_utc"], label="audit report seal"),
            _utc(sealed["selection_sealed_at_utc"], label="audit selection seal"),
            _utc(sealed["selection_completed_at_utc"], label="audit selection completion"),
        )
    ):
        raise ActiveRiskBindingLoaderError("binding audit sealed-chain drift")
    return claimed


def seal_active_risk_shadow_export(
    package: ImmutableActiveRiskShadowPackage,
    audit: Mapping[str, Any],
    *,
    output_dir: str | Path,
) -> Mapping[str, Any]:
    """Write package, full binding audit, then the sole export commit marker."""

    root = Path(output_dir).resolve()
    receipt_path = root / EXPORT_RECEIPT_NAME
    if receipt_path.is_file():
        receipt = verify_active_risk_shadow_export(root)
        if (
            receipt.get("package_hash") != package.package_hash
            or receipt.get("binding_audit_hash") != audit.get("audit_hash")
        ):
            raise ActiveRiskBindingLoaderError(
                "existing immutable forward export differs from requested package"
            )
        return receipt
    claimed_audit_hash = _validate_complete_export_audit(audit, package)
    machine, dossier = write_shadow_package(package, root)
    writer = AtomicResultWriter(root, immutable=True)
    writer.write_json(EXPORT_AUDIT_NAME, dict(audit))
    audit_path = root / EXPORT_AUDIT_NAME
    artifacts = {
        path.name: _artifact(path) for path in (machine, dossier, audit_path)
    }
    receipt_body: dict[str, Any] = {
        "schema": EXPORT_RECEIPT_SCHEMA,
        "policy_id": package.candidate_id,
        "status": package.role,
        "package_hash": package.package_hash,
        "binding_audit_hash": claimed_audit_hash,
        "freeze_timestamp_utc": package.freeze_timestamp_utc,
        "artifacts": artifacts,
        "safety": {
            "broker_connectivity": False,
            "outbound_order_capability": False,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
            "paper_shadow_ready": False,
        },
        "publication_contract": {
            "package_and_audit_written_before_receipt": True,
            "receipt_is_commit_marker": True,
            "immutable": True,
        },
    }
    receipt = dict(receipt_body)
    receipt["receipt_hash"] = stable_hash(receipt_body)
    writer.write_json(EXPORT_RECEIPT_NAME, receipt)
    return verify_active_risk_shadow_export(root)


def verify_active_risk_shadow_export(
    output_dir: str | Path,
) -> Mapping[str, Any]:
    root = Path(output_dir).resolve()
    receipt = _json(root / EXPORT_RECEIPT_NAME, label="forward export receipt")
    claimed = str(receipt.get("receipt_hash") or "")
    body = dict(receipt)
    body.pop("receipt_hash", None)
    if (
        receipt.get("schema") != EXPORT_RECEIPT_SCHEMA
        or not claimed
        or stable_hash(body) != claimed
        or receipt.get("status") != "FORWARD_SHADOW_CANDIDATE"
        or receipt.get("publication_contract")
        != {
            "package_and_audit_written_before_receipt": True,
            "receipt_is_commit_marker": True,
            "immutable": True,
        }
        or receipt.get("safety")
        != {
            "broker_connectivity": False,
            "outbound_order_capability": False,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
            "paper_shadow_ready": False,
        }
    ):
        raise ActiveRiskBindingLoaderError("forward export receipt drift")
    artifacts = receipt.get("artifacts")
    expected_names = {"shadow_package.json", "shadow_package.md", EXPORT_AUDIT_NAME}
    if not isinstance(artifacts, Mapping) or set(artifacts) != expected_names:
        raise ActiveRiskBindingLoaderError("forward export artifact coverage drift")
    for name, metadata in artifacts.items():
        path = _inside(root, name, label="forward export artifact")
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("relative_path") != name
            or not path.is_file()
            or path.stat().st_size != int(metadata.get("size_bytes", -1))
            or _file_sha256(path) != str(metadata.get("sha256") or "")
        ):
            raise ActiveRiskBindingLoaderError(
                f"forward export artifact binding drift: {name}"
            )
    package_payload = _json(
        root / "shadow_package.json", label="forward shadow package"
    )
    reconstructed = reconstruct_active_risk_shadow_package(package_payload)
    audit = _json(root / EXPORT_AUDIT_NAME, label="binding audit")
    audit_hash = str(audit.get("audit_hash") or "")
    audit_body = dict(audit)
    audit_body.pop("audit_hash", None)
    _validate_complete_export_audit(audit, reconstructed.package)
    if (
        reconstructed.package.package_hash != receipt.get("package_hash")
        or reconstructed.package.candidate_id != receipt.get("policy_id")
        or reconstructed.package.freeze_timestamp_utc
        != receipt.get("freeze_timestamp_utc")
        or stable_hash(audit_body) != audit_hash
        or audit_hash != receipt.get("binding_audit_hash")
        or (audit.get("sealed_chain") or {}).get("package_hash")
        != reconstructed.package.package_hash
    ):
        raise ActiveRiskBindingLoaderError("forward export package/audit drift")
    return receipt


__all__ = [
    "ActiveRiskBindingLoaderError",
    "FrozenActiveRiskSources",
    "EXPORT_AUDIT_NAME",
    "EXPORT_RECEIPT_NAME",
    "build_active_risk_package_from_sealed_selection",
    "load_frozen_active_risk_sources",
    "seal_active_risk_shadow_export",
    "verify_active_risk_shadow_export",
]
