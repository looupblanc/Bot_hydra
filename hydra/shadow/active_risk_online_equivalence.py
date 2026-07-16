"""Fail-closed online/offline equivalence proof for Operating Package V1.

The frozen campaign was produced from immutable columnar feature records.  This
module deliberately separates three claims which must not be conflated:

* raw parquet -> canonical feature bundle identity;
* scalar, record-at-a-time execution -> frozen component/account evidence;
* integration of that scalar engine into the persistent forward processor.

Only when every applicable claim passes may the receipt carry
``ONLINE_OFFLINE_EQUIVALENCE_PROVEN``.  Reading a frozen feature array as both
producer and oracle is labelled as a feature-record replay and is never called
a raw-feature proof.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import struct
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_pool_replay import (
    RoutedTrade,
    run_shared_account_episode,
)
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.production.portfolio_runtime import _restress
from hydra.production.runtime import _block_aware_starts, _block_calendars
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15
from hydra.research.rolling_combine_replay import MINUTE_NS
from hydra.research.turbo_feature_builder import FEATURE_BUNDLE_VERSION, FEATURE_DAG_HASH
from hydra.shadow.active_risk_package import (
    FrozenSignalBinding,
    reconstruct_active_risk_shadow_package,
)


EQUIVALENCE_SCHEMA = "hydra_active_risk_online_offline_equivalence_v1"
ENGINE_VERSION = "hydra_frozen_sleeve_scalar_record_engine_v1"
ONLINE_OFFLINE_EQUIVALENCE_PROVEN = "ONLINE_OFFLINE_EQUIVALENCE_PROVEN"
ONLINE_OFFLINE_EQUIVALENCE_FAILED_CLOSED = (
    "ONLINE_OFFLINE_EQUIVALENCE_NOT_PROVEN_FAIL_CLOSED"
)
DEFAULT_RECEIPT_PATH = Path(
    "mission/state/operating_package_v1_parity/online_offline_equivalence_receipt.json"
)
DEFAULT_AUDIT_PATH = Path(
    "reports/operating/hydra_operating_package_v1/online_offline_equivalence_audit.json"
)
DEFAULT_PACKAGE_GLOB = (
    "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/"
    "forward_shadow/*/shadow_package.json"
)
DEFAULT_EVIDENCE_ROOT = Path(
    "data/cache/evidence_bundles/"
    "hydra_active_risk_pool_target_velocity_0026.evidence-v1"
)
DEFAULT_CAMPAIGN_MANIFEST = Path(
    "config/v7/active_risk_pool_target_velocity_0026_revision_02.json"
)
DEFAULT_OPERATING_MANIFEST = Path(
    "reports/operating/hydra_operating_package_v1/OPERATING_PACKAGE_V1.json"
)
DEFAULT_OPERATING_RECEIPT = Path(
    "reports/operating/hydra_operating_package_v1/OPERATING_PACKAGE_V1_seal_receipt.json"
)
DEFAULT_BOUNDARY_MANIFEST = Path(
    "reports/operating/hydra_operating_package_v1/active_risk_forward_boundary.json"
)
CAMPAIGN_ID = "hydra_active_risk_pool_target_velocity_0026"
DEVELOPMENT_START_DAY = int(np.datetime64("2023-01-01", "D").astype(np.int64))
DEVELOPMENT_END_DAY = int(np.datetime64("2024-10-01", "D").astype(np.int64))
_ZERO_HASH = "0" * 64


class ActiveRiskOnlineEquivalenceError(RuntimeError):
    """A parity input, proof receipt, or deterministic invariant failed."""


def stable_hash(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ActiveRiskOnlineEquivalenceError(
            "equivalence payload is not canonical JSON"
        ) from exc
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class FrozenSleeveStreamState:
    last_segment: int | None = None
    last_exit_ns: int = -(2**63)
    emitted_count: int = 0
    last_row_index: int = -1
    input_chain_hash: str = _ZERO_HASH
    emission_chain_hash: str = _ZERO_HASH

    @classmethod
    def initial(cls) -> "FrozenSleeveStreamState":
        return cls()


class FrozenSleeveOnlineEngine:
    """Scalar frozen-sleeve executor used by both proof and online integration.

    ``record`` is a canonical, closed feature record.  The historical proof
    includes ``horizon_available`` because that exact campaign mask required a
    finite forward-path cell.  Production integration must replace that field
    with a separately proven causal session/roll eligibility rule; callers may
    not infer that this historical oracle is itself forward-safe.
    """

    def __init__(
        self,
        spec: Any,
        binding: FrozenSignalBinding,
        matrix_fingerprint: str,
        state: FrozenSleeveStreamState | None = None,
    ) -> None:
        if spec.sleeve_id != binding.sleeve_id:
            raise ActiveRiskOnlineEquivalenceError("sleeve/binding identity drift")
        if binding.feature_bundle_version != FEATURE_BUNDLE_VERSION:
            raise ActiveRiskOnlineEquivalenceError("feature bundle version drift")
        if binding.feature_dag_hash != FEATURE_DAG_HASH:
            raise ActiveRiskOnlineEquivalenceError("feature DAG drift")
        self.spec = spec
        self.binding = binding
        self.matrix_fingerprint = str(matrix_fingerprint)
        self.state = state or FrozenSleeveStreamState.initial()

    @classmethod
    def from_checkpoint(
        cls,
        spec: Any,
        binding: FrozenSignalBinding,
        matrix_fingerprint: str,
        checkpoint: Mapping[str, Any],
    ) -> "FrozenSleeveOnlineEngine":
        if checkpoint.get("schema") != "hydra_frozen_sleeve_stream_checkpoint_v1":
            raise ActiveRiskOnlineEquivalenceError("stream checkpoint schema drift")
        if checkpoint.get("engine_version") != ENGINE_VERSION:
            raise ActiveRiskOnlineEquivalenceError("stream checkpoint engine drift")
        if checkpoint.get("sleeve_id") != spec.sleeve_id:
            raise ActiveRiskOnlineEquivalenceError("stream checkpoint sleeve drift")
        raw = dict(checkpoint["state"])
        state = FrozenSleeveStreamState(
            last_segment=(
                None if raw["last_segment"] is None else int(raw["last_segment"])
            ),
            last_exit_ns=int(raw["last_exit_ns"]),
            emitted_count=int(raw["emitted_count"]),
            last_row_index=int(raw["last_row_index"]),
            input_chain_hash=str(raw["input_chain_hash"]),
            emission_chain_hash=str(raw["emission_chain_hash"]),
        )
        instance = cls(spec, binding, matrix_fingerprint, state)
        expected = str(checkpoint.get("checkpoint_hash") or "")
        unhashed = dict(checkpoint)
        unhashed.pop("checkpoint_hash", None)
        if expected != stable_hash(unhashed):
            raise ActiveRiskOnlineEquivalenceError("stream checkpoint hash drift")
        return instance

    def checkpoint(self) -> dict[str, Any]:
        payload = {
            "schema": "hydra_frozen_sleeve_stream_checkpoint_v1",
            "engine_version": ENGINE_VERSION,
            "sleeve_id": self.spec.sleeve_id,
            "matrix_fingerprint": self.matrix_fingerprint,
            "state": asdict(self.state),
        }
        payload["checkpoint_hash"] = stable_hash(payload)
        return payload

    def process_record(self, record: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
        index = int(record["row_index"])
        if index != self.state.last_row_index + 1:
            raise ActiveRiskOnlineEquivalenceError(
                f"non-contiguous feature-record index for {self.spec.sleeve_id}"
            )
        if str(record["matrix_fingerprint"]) != self.matrix_fingerprint:
            raise ActiveRiskOnlineEquivalenceError("feature-record matrix drift")
        input_fingerprint = _record_fingerprint(record)
        input_chain = hashlib.sha256(
            (self.state.input_chain_hash + input_fingerprint).encode("ascii")
        ).hexdigest()
        segment = int(record["segment_code"])
        last_exit = self.state.last_exit_ns
        if self.state.last_segment != segment:
            last_exit = -(2**63)
        emitted: tuple[dict[str, Any], ...] = ()
        decision_ns = int(record["decision_ns"])
        if self._eligible(record) and decision_ns >= last_exit:
            ordinal = self.state.emitted_count
            signal_id = f"{self.spec.sleeve_id}:{ordinal:05d}:{decision_ns}"
            event = {
                "sleeve_id": self.spec.sleeve_id,
                "signal_id": signal_id,
                "row_index": index,
                "decision_ns": decision_ns,
                "session_day": int(record["session_day"]),
                "session_code": int(record["session_code"]),
                "segment_code": segment,
                "contract_code": int(record["contract_code"]),
                "direction": int(self.spec.side),
                "size": 1,
                "entry_price": float(record["entry_price"]),
                "stop": None,
                "target": None,
                "veto": False,
                "input_fingerprint": input_fingerprint,
            }
            emitted = (event,)
            last_exit = decision_ns + (int(self.spec.holding_bars) + 1) * MINUTE_NS
            emission_chain = stable_hash(
                {"previous": self.state.emission_chain_hash, "event": event}
            )
            emitted_count = ordinal + 1
        else:
            emission_chain = self.state.emission_chain_hash
            emitted_count = self.state.emitted_count
        self.state = FrozenSleeveStreamState(
            last_segment=segment,
            last_exit_ns=last_exit,
            emitted_count=emitted_count,
            last_row_index=index,
            input_chain_hash=input_chain,
            emission_chain_hash=emission_chain,
        )
        return emitted

    def _eligible(self, row: Mapping[str, Any]) -> bool:
        day = int(row["session_day"])
        session = int(row["session_code"])
        trigger = float(row["trigger_value"])
        if not (DEVELOPMENT_START_DAY <= day < DEVELOPMENT_END_DAY):
            return False
        if not math.isfinite(trigger) or not bool(row["horizon_available"]):
            return False
        if self.spec.session_code >= 0:
            if session != int(self.spec.session_code):
                return False
        elif session < 0:
            return False
        if not _compare_scalar(
            trigger,
            self.binding.trigger_operator,
            float(self.binding.trigger_threshold),
        ):
            return False
        if self.binding.context_feature is not None:
            context = float(row["context_value"])
            if not math.isfinite(context):
                return False
            if not _compare_scalar(
                context,
                str(self.binding.context_operator),
                float(self.binding.context_threshold),
            ):
                return False
        return True


@dataclass(frozen=True, slots=True)
class CanonicalBarOrderState:
    last_timestamp_ns: int | None = None
    last_availability_ns: int | None = None
    last_segment_code: int | None = None
    last_session_day: int | None = None
    last_contract: str | None = None
    last_fingerprint: str | None = None


class CanonicalBarOrderGuard:
    """Reject duplicate, late, out-of-order, and unexplained missing bars."""

    def __init__(self, state: CanonicalBarOrderState | None = None) -> None:
        self.state = state or CanonicalBarOrderState()

    def process(self, record: Mapping[str, Any]) -> str:
        timestamp = int(record["timestamp_ns"])
        availability = int(record.get("availability_ns", timestamp + MINUTE_NS))
        observed = int(record.get("observed_ns", availability))
        segment = int(record["segment_code"])
        session_day = int(record["session_day"])
        contract = str(record["contract"])
        fingerprint = stable_hash(
            {
                key: record[key]
                for key in sorted(record)
                if key not in {"observed_ns"}
            }
        )
        prior = self.state
        if prior.last_fingerprint == fingerprint:
            raise ActiveRiskOnlineEquivalenceError("DUPLICATE_BAR")
        if availability > observed:
            raise ActiveRiskOnlineEquivalenceError("LATE_OR_NOT_YET_AVAILABLE_BAR")
        if prior.last_timestamp_ns is not None and timestamp <= prior.last_timestamp_ns:
            raise ActiveRiskOnlineEquivalenceError("OUT_OF_ORDER_BAR")
        transition = segment != prior.last_segment_code
        if prior.last_timestamp_ns is not None:
            expected = prior.last_timestamp_ns + MINUTE_NS
            if timestamp != expected and not transition:
                raise ActiveRiskOnlineEquivalenceError("MISSING_INTERVAL")
            if contract != prior.last_contract and not transition:
                raise ActiveRiskOnlineEquivalenceError("CONTRACT_ROLL_WITHOUT_SEGMENT")
            if session_day != prior.last_session_day and not transition:
                raise ActiveRiskOnlineEquivalenceError("SESSION_CHANGE_WITHOUT_SEGMENT")
        status = "ACCEPTED"
        if prior.last_timestamp_ns is not None and contract != prior.last_contract:
            status = "CONTRACT_ROLL"
        elif prior.last_timestamp_ns is not None and session_day != prior.last_session_day:
            status = "SESSION_TRANSITION"
        self.state = CanonicalBarOrderState(
            last_timestamp_ns=timestamp,
            last_availability_ns=availability,
            last_segment_code=segment,
            last_session_day=session_day,
            last_contract=contract,
            last_fingerprint=fingerprint,
        )
        return status

    def checkpoint(self) -> dict[str, Any]:
        payload = {
            "schema": "hydra_canonical_bar_order_guard_checkpoint_v1",
            "state": asdict(self.state),
        }
        payload["checkpoint_hash"] = stable_hash(payload)
        return payload


def prove_active_risk_online_equivalence(
    *,
    repository_root: str | Path,
    package_paths: Sequence[str | Path] | None = None,
    evidence_bundle_root: str | Path | None = None,
    raw_rebuild_root: str | Path | None = None,
    output_path: str | Path | None = None,
    reconcile_accounts: bool = True,
    processor_uses_proven_engine: bool = False,
    persistent_processor_version: str | None = None,
) -> dict[str, Any]:
    """Execute and optionally seal the bounded parity proof."""

    root = Path(repository_root).resolve()
    paths = tuple(
        sorted(
            (
                _inside(root, value)
                for value in package_paths
            )
            if package_paths is not None
            else root.glob(DEFAULT_PACKAGE_GLOB)
        )
    )
    if len(paths) != 6:
        raise ActiveRiskOnlineEquivalenceError(
            f"equivalence proof requires six frozen packages, found {len(paths)}"
        )
    reconstructed = []
    package_rows: list[dict[str, Any]] = []
    for path in paths:
        payload = _json(path)
        row = reconstruct_active_risk_shadow_package(payload)
        reconstructed.append(row)
        package_rows.append(
            {
                "candidate_id": row.package.candidate_id,
                "path": _relative(root, path),
                "file_sha256": _sha256(path),
                "package_hash": row.package.package_hash,
                "freeze_timestamp_utc": row.package.freeze_timestamp_utc,
                "combine_policy_fingerprint": row.combine_policy.structural_fingerprint,
            }
        )
    _assert_common_sleeve_contract(reconstructed)
    reference = reconstructed[0]
    matrices = _open_frozen_matrices(root, reference.frozen_signal_bindings)
    feature_rebuild = _reconcile_rebuilt_features(
        root,
        matrices,
        raw_rebuild_root,
    )
    evidence_root = _inside(
        root, evidence_bundle_root or DEFAULT_EVIDENCE_ROOT
    )
    expected_components = {
        name: list(_evidence_rows(evidence_root, name))
        for name in (
            "component_signals",
            "component_entries",
            "component_exits",
            "component_trades",
        )
    }
    generated, routed, sleeve_rows, restart_rows = _scalar_component_replay(
        matrices,
        reference.sleeve_specs,
        reference.sleeve_records,
        reference.frozen_signal_bindings,
    )
    component_result = _compare_component_evidence(generated, expected_components)
    virtual_fill = _virtual_fill_semantics_audit(
        matrices,
        reference.sleeve_specs,
        routed,
    )
    causal_horizon = _causal_horizon_audit(
        matrices,
        reference.sleeve_specs,
        reference.frozen_signal_bindings,
        generated["component_signals"],
    )
    source_tape_result = _compare_source_tape(root, routed)
    account_result = (
        _reconcile_six_books(
            root=root,
            reconstructed=reconstructed,
            routed=routed,
            matrices=matrices,
            evidence_root=evidence_root,
        )
        if reconcile_accounts
        else {
            "status": "NOT_RUN",
            "mismatch_count": 1,
            "reason": "account reconciliation explicitly disabled",
        }
    )
    guards = _guard_self_test()
    operating_manifest = root / DEFAULT_OPERATING_MANIFEST
    operating_receipt = root / DEFAULT_OPERATING_RECEIPT
    boundary_manifest = root / DEFAULT_BOUNDARY_MANIFEST
    processor_path = root / "hydra/shadow/active_risk_forward_processor.py"
    module_path = root / "hydra/shadow/active_risk_online_equivalence.py"
    processor_source = processor_path.read_text(encoding="utf-8")
    detected_engine_binding = bool(
        "FrozenSleeveOnlineEngine" in processor_source
        and "active_risk_online_equivalence" in processor_source
    )
    operating_manifest_payload = _json(operating_manifest)
    operating_receipt_payload = _json(operating_receipt)
    integration = {
        "operating_package_manifest_path": _relative(root, operating_manifest),
        "operating_package_manifest_hash": str(
            operating_manifest_payload["manifest_hash"]
        ),
        "operating_package_manifest_file_sha256": _sha256(operating_manifest),
        "operating_package_receipt_path": _relative(root, operating_receipt),
        "operating_package_receipt_hash": str(
            operating_receipt_payload["receipt_hash"]
        ),
        "operating_package_receipt_file_sha256": _sha256(operating_receipt),
        "boundary_manifest_path": _relative(root, boundary_manifest),
        "boundary_manifest_sha256": _sha256(boundary_manifest),
        "persistent_processor_version": persistent_processor_version,
        "engine_version": ENGINE_VERSION,
        "equivalence_module_sha256": _sha256(module_path),
        "persistent_processor_sha256": _sha256(processor_path),
        "processor_binding_requested": bool(processor_uses_proven_engine),
        "processor_uses_proven_engine": detected_engine_binding,
    }
    mismatch_count = (
        int(feature_rebuild["mismatch_count"])
        + int(component_result["mismatch_count"])
        + int(causal_horizon["mismatch_count"])
        + int(virtual_fill["mismatch_count"])
        + int(source_tape_result["mismatch_count"])
        + int(account_result["mismatch_count"])
        + int(guards["mismatch_count"])
        + sum(int(row["mismatch_count"]) for row in restart_rows)
    )
    conservative_virtual_fill_parity = virtual_fill["mismatch_count"] == 0
    per_bar_account_state_parity = False
    proven = bool(
        mismatch_count == 0
        and feature_rebuild["status"] == "BYTE_EXACT"
        and component_result["status"] == "EXACT"
        and causal_horizon["status"] == "CAUSAL_EQUIVALENCE_EXACT"
        and conservative_virtual_fill_parity
        and per_bar_account_state_parity
        and source_tape_result["status"] == "EXACT"
        and account_result["status"] == "EXACT"
        and guards["status"] == "PASS"
        and detected_engine_binding
    )
    receipt: dict[str, Any] = {
        "schema": EQUIVALENCE_SCHEMA,
        "engine_version": ENGINE_VERSION,
        "created_at_utc": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "status": (
            ONLINE_OFFLINE_EQUIVALENCE_PROVEN
            if proven
            else ONLINE_OFFLINE_EQUIVALENCE_FAILED_CLOSED
        ),
        "mismatch_count": mismatch_count,
        "claim_scope": {
            "raw_parquet_to_canonical_features": feature_rebuild["status"],
            "canonical_feature_records_to_component_evidence": component_result[
                "status"
            ],
            "causal_horizon_eligibility": causal_horizon["status"],
            "component_events_to_six_book_account_evidence": account_result["status"],
            "persistent_processor_integration": detected_engine_binding,
            "historical_horizon_availability_is_causal": causal_horizon[
                "mismatch_count"
            ] == 0,
            "conservative_virtual_fill_parity_claimed": conservative_virtual_fill_parity,
            "per_bar_account_state_oracle_available": per_bar_account_state_parity,
        },
        "feature_rebuild": feature_rebuild,
        "packages": package_rows,
        "sleeves": sleeve_rows,
        "component_evidence": component_result,
        "causal_horizon_audit": causal_horizon,
        "virtual_fill_semantics_audit": virtual_fill,
        "source_tape": source_tape_result,
        "account_evidence": account_result,
        "restart": restart_rows,
        "guards": guards,
        "integration": integration,
        "fail_closed_reasons": [
            value
            for value in (
                None if feature_rebuild["status"] == "BYTE_EXACT" else "RAW_FEATURE_REBUILD_MISMATCH",
                None if component_result["status"] == "EXACT" else "COMPONENT_EVIDENCE_MISMATCH",
                None if source_tape_result["status"] == "EXACT" else "SOURCE_TAPE_MISMATCH",
                None if account_result["status"] == "EXACT" else "ACCOUNT_EVIDENCE_MISMATCH",
                None if guards["status"] == "PASS" else "CORRUPTION_GUARD_FAILURE",
                None if causal_horizon["mismatch_count"] == 0 else "HISTORICAL_FORWARD_MOVE_FINITE_MASK_NOT_CAUSALLY_REPRODUCIBLE",
                None if conservative_virtual_fill_parity else "FROZEN_THEORETICAL_AND_CONSERVATIVE_VIRTUAL_FILL_SEMANTICS_DIVERGE",
                None if detected_engine_binding else "PERSISTENT_PROCESSOR_NOT_BOUND_TO_PROVEN_ENGINE",
                "PER_BAR_ACCOUNT_STATE_EQUIVALENCE_NOT_PROVABLE_FROM_DAILY_FROZEN_ORACLE",
            )
            if value is not None
        ],
        "safety": {
            "broker_connections": 0,
            "orders": 0,
            "q4_access_delta": 0,
            "market_data_purchase_delta_usd": 0.0,
        },
    }
    receipt["proof_hash"] = stable_hash(receipt)
    if output_path is not None:
        destination = _inside(root, output_path)
        authorization_path = (root / DEFAULT_RECEIPT_PATH).resolve()
        if destination == authorization_path and not proven:
            # An invalid authorization artifact would make V17 fail integrity
            # rather than remain safely F0_PENDING.  Failed audits are written
            # to their separate report path; the authorization receipt remains
            # absent until the full proof is true.
            return receipt
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        temporary.replace(destination)
    return receipt


def verify_active_risk_online_equivalence_proof(
    path: str | Path,
    *,
    repository_root: str | Path,
    expected_package_manifest_hash: str,
    expected_package_ids: Sequence[str],
    expected_boundary_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Read-only strict verifier used by the persistent controller/processor."""

    root = Path(repository_root).resolve()
    proof = _json(_inside(root, path))
    expected_hash = str(proof.get("proof_hash") or "")
    unhashed = dict(proof)
    unhashed.pop("proof_hash", None)
    if expected_hash != stable_hash(unhashed):
        raise ActiveRiskOnlineEquivalenceError("equivalence proof hash drift")
    if proof.get("schema") != EQUIVALENCE_SCHEMA:
        raise ActiveRiskOnlineEquivalenceError("equivalence proof schema drift")
    if proof.get("engine_version") != ENGINE_VERSION:
        raise ActiveRiskOnlineEquivalenceError("equivalence proof engine drift")
    if proof.get("status") != ONLINE_OFFLINE_EQUIVALENCE_PROVEN:
        raise ActiveRiskOnlineEquivalenceError("online/offline equivalence is not proven")
    if int(proof.get("mismatch_count", -1)) != 0:
        raise ActiveRiskOnlineEquivalenceError("equivalence proof contains mismatches")
    integration = dict(proof.get("integration") or {})
    if integration.get("operating_package_manifest_hash") != str(
        expected_package_manifest_hash
    ):
        raise ActiveRiskOnlineEquivalenceError("operating package manifest drift")
    boundary = str(integration.get("boundary_manifest_sha256") or "")
    if expected_boundary_manifest_sha256 is not None and boundary != str(
        expected_boundary_manifest_sha256
    ):
        raise ActiveRiskOnlineEquivalenceError("forward boundary manifest drift")
    if integration.get("processor_uses_proven_engine") is not True:
        raise ActiveRiskOnlineEquivalenceError("processor/engine integration is unproven")
    actual_ids = sorted(str(row["candidate_id"]) for row in proof.get("packages") or ())
    if actual_ids != sorted(str(value) for value in expected_package_ids):
        raise ActiveRiskOnlineEquivalenceError("frozen package set drift")
    for section, status in (
        ("feature_rebuild", "BYTE_EXACT"),
        ("component_evidence", "EXACT"),
        ("causal_horizon_audit", "CAUSAL_EQUIVALENCE_EXACT"),
        ("virtual_fill_semantics_audit", "EXACT"),
        ("source_tape", "EXACT"),
        ("account_evidence", "EXACT"),
        ("guards", "PASS"),
    ):
        row = dict(proof.get(section) or {})
        if row.get("status") != status or int(row.get("mismatch_count", -1)) != 0:
            raise ActiveRiskOnlineEquivalenceError(f"invalid proof section: {section}")
    scope = dict(proof.get("claim_scope") or {})
    if scope.get("per_bar_account_state_oracle_available") is not True:
        raise ActiveRiskOnlineEquivalenceError("per-bar account-state parity is unproven")
    return proof


def write_bounded_fail_closed_equivalence_audit(
    *,
    repository_root: str | Path,
    raw_rebuild_root: str | Path = (
        "mission/state/operating_package_v1_parity/raw_feature_rebuild"
    ),
    output_path: str | Path = DEFAULT_AUDIT_PATH,
) -> dict[str, Any]:
    """Seal the already-determined fail-closed findings without a broad rescan.

    This recovery path exists for the bounded case where the exhaustive daily
    EvidenceBundle reader was stopped before OOM.  It does not assert account
    parity.  Counts below are the deterministic findings independently
    established before this writer is called; the frozen bundle/manifests and
    raw-feature hashes are revalidated here.
    """

    root = Path(repository_root).resolve()
    paths = tuple(sorted(root.glob(DEFAULT_PACKAGE_GLOB)))
    if len(paths) != 6:
        raise ActiveRiskOnlineEquivalenceError("bounded audit requires six packages")
    reconstructed = [
        reconstruct_active_risk_shadow_package(_json(path)) for path in paths
    ]
    _assert_common_sleeve_contract(reconstructed)
    reference = reconstructed[0]
    matrices = _open_frozen_matrices(root, reference.frozen_signal_bindings)
    feature_rebuild = _reconcile_rebuilt_features(root, matrices, raw_rebuild_root)
    if feature_rebuild["status"] != "BYTE_EXACT":
        raise ActiveRiskOnlineEquivalenceError("bounded audit raw rebuild drift")
    evidence_root = root / DEFAULT_EVIDENCE_ROOT
    evidence_manifest_path = evidence_root / "evidence_bundle_manifest.json"
    evidence_manifest = _json(evidence_manifest_path)
    counts: dict[str, int] = {}
    for row in _evidence_rows(evidence_root, "component_signals"):
        sleeve_id = str(row["component_id"])
        counts[sleeve_id] = counts.get(sleeve_id, 0) + 1
    if sum(counts.values()) != 2052 or set(counts) != set(reference.sleeve_specs):
        raise ActiveRiskOnlineEquivalenceError("bounded component counts drift")
    causal_findings = {
        "sleeve_39eb74b174e7bdd240520b9d": (9, 4, 5),
        "sleeve_65bad2088913fc9fca0a145d": (3, 0, 3),
        "sleeve_7ecd76490fa9fb34e1af5820": (1, 1, 0),
        "sleeve_c5da4b5a67abadeb7d68eabe": (6, 0, 6),
        "sleeve_e017bb45b0937aef46657631": (2, 1, 1),
    }
    causal_sleeves = []
    for sleeve_id in sorted(reference.sleeve_specs):
        additional, unpredictable, scheduled = causal_findings.get(
            sleeve_id, (0, 0, 0)
        )
        causal_sleeves.append(
            {
                "sleeve_id": sleeve_id,
                "offline_signal_count": counts[sleeve_id],
                "additional_signal_count_without_forward_outcome_mask": additional,
                "unpredictable_gap_count": unpredictable,
                "known_session_roll_holiday_boundary_count": scheduled,
                "mismatch_count": additional,
            }
        )
    fill_audit = _bounded_virtual_fill_counts(
        matrices, reference.sleeve_specs, evidence_root
    )
    package_rows = [
        {
            "candidate_id": row.package.candidate_id,
            "path": _relative(root, path),
            "file_sha256": _sha256(path),
            "package_hash": row.package.package_hash,
            "freeze_timestamp_utc": row.package.freeze_timestamp_utc,
        }
        for path, row in zip(paths, reconstructed, strict=True)
    ]
    per_book = [
        {
            "candidate_id": row.package.candidate_id,
            "offline_90d_episode_oracle_count": 384,
            "causal_signal_divergence_count": 21,
            "causal_account_status": "NOT_EVALUABLE_WITHOUT_FABRICATING_21_TRADE_OUTCOMES",
            "timeline_affected": True,
        }
        for row in reconstructed
    ]
    guards = _guard_self_test()
    processor_path = root / "hydra/shadow/active_risk_forward_processor.py"
    processor_source = processor_path.read_text(encoding="utf-8")
    operating_manifest_path = root / DEFAULT_OPERATING_MANIFEST
    operating_receipt_path = root / DEFAULT_OPERATING_RECEIPT
    operating_manifest_payload = _json(operating_manifest_path)
    operating_receipt_payload = _json(operating_receipt_path)
    integration = {
        "processor_uses_proven_engine": bool(
            "FrozenSleeveOnlineEngine" in processor_source
            and "active_risk_online_equivalence" in processor_source
        ),
        "persistent_processor_sha256": _sha256(processor_path),
        "equivalence_module_sha256": _sha256(
            root / "hydra/shadow/active_risk_online_equivalence.py"
        ),
        "operating_package_manifest_hash": str(
            operating_manifest_payload["manifest_hash"]
        ),
        "operating_package_manifest_file_sha256": _sha256(
            operating_manifest_path
        ),
        "operating_package_receipt_hash": str(
            operating_receipt_payload["receipt_hash"]
        ),
        "operating_package_receipt_file_sha256": _sha256(
            operating_receipt_path
        ),
        "boundary_manifest_sha256": _sha256(root / DEFAULT_BOUNDARY_MANIFEST),
    }
    fill_mismatch = int(fill_audit["mismatch_count"])
    minimum_mismatch = 21 + fill_mismatch + 1
    audit: dict[str, Any] = {
        "schema": EQUIVALENCE_SCHEMA,
        "engine_version": ENGINE_VERSION,
        "created_at_utc": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "status": ONLINE_OFFLINE_EQUIVALENCE_FAILED_CLOSED,
        "mismatch_count": minimum_mismatch,
        "mismatch_semantics": "LOWER_BOUND_UNIQUE_ENGINE_CONTRACT_MISMATCHES",
        "authorization_receipt_written": False,
        "feature_rebuild": feature_rebuild,
        "packages": package_rows,
        "component_evidence": {
            "status": "EXACT_DETERMINISTIC_RECONCILIATION_COMPLETED",
            "mismatch_count": 0,
            "signals": 2052,
            "entries": 2052,
            "exits": 2052,
            "trades": 2052,
            "per_sleeve_signal_counts": counts,
            "evidence_manifest_path": _relative(root, evidence_manifest_path),
            "evidence_manifest_sha256": _sha256(evidence_manifest_path),
            "evidence_bundle_content_sha256": evidence_manifest[
                "bundle_content_sha256"
            ],
        },
        "causal_horizon_audit": {
            "status": "CAUSAL_EQUIVALENCE_FAILED",
            "mismatch_count": 21,
            "additional_signals": 21,
            "unpredictable_gap_exclusions": 6,
            "unpredictable_gap_events": [
                {"market": "YM", "decision_at_utc": "2023-03-14T19:30:00Z"},
                {"market": "YM", "decision_at_utc": "2023-03-15T19:40:00Z"},
                {"market": "CL", "decision_at_utc": "2023-06-16T15:52:00Z"},
                {"market": "YM", "decision_at_utc": "2023-09-13T19:30:00Z"},
                {"market": "NQ", "decision_at_utc": "2024-03-13T19:35:00Z"},
                {"market": "YM", "decision_at_utc": "2024-09-18T19:20:00Z"},
            ],
            "scheduled_boundary_exclusions": 15,
            "sleeves_affected": 5,
            "sleeves": causal_sleeves,
            "finding": (
                "CALENDAR_AND_ROLL_RULES_CANNOT_REPRODUCE_THE_2052_SIGNAL_"
                "ORACLE_EXACTLY_BECAUSE_SIX_EXCLUSIONS_DEPEND_ON_UNPREDICTABLE_GAPS"
            ),
        },
        "virtual_fill_semantics_audit": fill_audit,
        "account_evidence": {
            "status": "CAUSAL_ACCOUNT_EQUIVALENCE_NOT_EVALUABLE",
            "mismatch_count": 1,
            "theoretical_offline_evidence": {
                "episode_rows_all_campaign": int(
                    evidence_manifest["dataset_row_counts"]["episodes"]
                ),
                "account_daily_rows_all_campaign": int(
                    evidence_manifest["dataset_row_counts"]["account_daily_paths"]
                ),
                "six_book_90d_episode_oracle_count": 2304,
                "oracle_granularity": "DAILY_ACCOUNT_PATH",
            },
            "per_bar_account_oracle": "ABSENT",
            "per_book": per_book,
            "replicated_causal_comparisons": 126,
            "affected_book_timelines": 6,
            "warning": (
                "DO_NOT_FABRICATE_PNL_MLL_OR_ACCOUNT_PATHS_FOR_THE_21_"
                "CAUSAL_ONLY_SIGNALS"
            ),
        },
        "guards": {
            **guards,
            "restart_checkpoint_resume": "TARGETED_REGRESSION_PASS",
            "full_18_binding_restart_reconciliation": "NOT_SEALED_BEFORE_BOUNDED_STOP",
        },
        "integration": integration,
        "claim_scope": {
            "raw_parquet_to_canonical_features": "BYTE_EXACT",
            "theoretical_offline_component_replay": "EXACT",
            "theoretical_offline_account_oracle": "AVAILABLE_DAILY",
            "causal_online_signal_equivalence": "FAILED_21",
            "conservative_virtual_fill_equivalence": "FAILED",
            "per_bar_account_state_equivalence": "NOT_PROVABLE_FROM_FROZEN_EVIDENCE",
            "persistent_processor_integration": integration[
                "processor_uses_proven_engine"
            ],
        },
        "fail_closed_reasons": [
            "HISTORICAL_FORWARD_MOVE_FINITE_MASK_NOT_CAUSALLY_REPRODUCIBLE",
            "FROZEN_THEORETICAL_AND_CONSERVATIVE_VIRTUAL_FILL_SEMANTICS_DIVERGE",
            "PER_BAR_ACCOUNT_STATE_EQUIVALENCE_NOT_PROVABLE_FROM_DAILY_FROZEN_ORACLE",
            "PERSISTENT_PROCESSOR_NOT_BOUND_TO_PROVEN_ENGINE",
            "FULL_18_BINDING_RESTART_RECONCILIATION_NOT_SEALED",
        ],
        "safety": {
            "broker_connections": 0,
            "orders": 0,
            "q4_access_delta": 0,
            "market_data_purchase_delta_usd": 0.0,
        },
    }
    audit["proof_hash"] = stable_hash(audit)
    destination = _inside(root, output_path)
    if destination == (root / DEFAULT_RECEIPT_PATH).resolve():
        raise ActiveRiskOnlineEquivalenceError(
            "bounded failed audit cannot target authorization receipt"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(destination)
    return audit


def _scalar_component_replay(
    matrices: Mapping[str, FeatureMatrix],
    specs: Mapping[str, Any],
    records: Mapping[str, Any],
    bindings: Mapping[str, FrozenSignalBinding],
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, tuple[RoutedTrade, ...]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    output = {
        "component_signals": [],
        "component_entries": [],
        "component_exits": [],
        "component_trades": [],
    }
    routed: dict[str, tuple[RoutedTrade, ...]] = {}
    sleeve_results: list[dict[str, Any]] = []
    restart_results: list[dict[str, Any]] = []
    jobs = [
        (
            str(matrices[specs[sleeve_id].market].root),
            specs[sleeve_id],
            records[sleeve_id],
            bindings[sleeve_id],
        )
        for sleeve_id in sorted(specs)
    ]
    with ProcessPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(_scalar_sleeve_worker, jobs))
    for generated_rows, routed_rows, sleeve_result, restart in results:
        sleeve_id = str(sleeve_result["sleeve_id"])
        for key, values in generated_rows.items():
            output[key].extend(values)
        routed[sleeve_id] = tuple(routed_rows)
        restart_results.append(restart)
        sleeve_results.append(sleeve_result)
    return output, routed, sleeve_results, restart_results


def _scalar_sleeve_worker(
    payload: tuple[str, Any, Any, FrozenSignalBinding],
) -> tuple[
    dict[str, list[dict[str, Any]]],
    list[RoutedTrade],
    dict[str, Any],
    dict[str, Any],
]:
    matrix_path, spec, record, binding = payload
    sleeve_id = spec.sleeve_id
    matrix = FeatureMatrix.open(matrix_path)
    array_cache = matrix.arrays
    engine = FrozenSleeveOnlineEngine(spec, binding, matrix.fingerprint)
    events: list[dict[str, Any]] = []
    midpoint = matrix.row_count // 2
    checkpoint: dict[str, Any] | None = None
    for index in range(matrix.row_count):
        if index == midpoint:
            checkpoint = engine.checkpoint()
            engine = FrozenSleeveOnlineEngine.from_checkpoint(
                spec, binding, matrix.fingerprint, checkpoint
            )
        events.extend(
            engine.process_record(
                _matrix_record(
                    matrix,
                    binding,
                    index,
                    spec.holding_bars,
                    array_cache,
                )
            )
        )
    generated_rows, routed_rows = _materialize_scalar_sleeve(
        spec, record, matrix, events
    )
    if checkpoint is None:
        raise ActiveRiskOnlineEquivalenceError("empty matrix cannot prove restart")
    restart = {
        "sleeve_id": sleeve_id,
        "checkpoint_row": midpoint,
        "emitted_count": len(events),
        "checkpoint_hash": checkpoint["checkpoint_hash"],
        "final_input_chain_hash": engine.state.input_chain_hash,
        "final_emission_chain_hash": engine.state.emission_chain_hash,
        "mismatch_count": 0,
        "status": "PASS",
        "evidence_reconciliation_dependency": "component_evidence",
    }
    sleeve_result = {
        "sleeve_id": sleeve_id,
        "market": spec.market,
        "execution_market": spec.execution_market,
        "canonical_feature_rows": matrix.row_count,
        "emitted_signals": len(events),
        "input_chain_hash": engine.state.input_chain_hash,
        "emission_chain_hash": engine.state.emission_chain_hash,
        "restart_exact": restart["mismatch_count"] == 0,
        "mismatch_count": 0,
    }
    return generated_rows, routed_rows, sleeve_result, restart


def _matrix_record(
    matrix: FeatureMatrix,
    binding: FrozenSignalBinding,
    index: int,
    holding_bars: int,
    arrays: Mapping[str, np.ndarray] | None = None,
) -> dict[str, Any]:
    columns = arrays or matrix.arrays
    context_value = (
        None
        if binding.context_feature is None
        else float(columns[f"feature__{binding.context_feature}"][index])
    )
    return {
        "matrix_fingerprint": matrix.fingerprint,
        "row_index": int(index),
        "timestamp_ns": int(columns["timestamp_ns"][index]),
        "decision_ns": int(columns["decision_ns"][index]),
        "availability_ns": int(columns["availability_ns"][index]),
        "segment_code": int(columns["segment_code"][index]),
        "session_day": int(columns["session_day"][index]),
        "session_code": int(columns["session_code"][index]),
        "contract_code": int(columns["contract_code"][index]),
        "entry_price": float(columns["entry_price"][index]),
        "bar_open": float(columns["bar_open"][index]),
        "bar_high": float(columns["bar_high"][index]),
        "bar_low": float(columns["bar_low"][index]),
        "bar_close": float(columns["bar_close"][index]),
        "trigger_value": float(
            columns[f"feature__{binding.trigger_feature}"][index]
        ),
        "context_value": context_value,
        "horizon_available": bool(
            math.isfinite(
                float(columns[f"forward_move__{int(holding_bars)}"][index])
            )
        ),
    }


def _materialize_scalar_sleeve(
    spec: Any,
    record: Any,
    matrix: FeatureMatrix,
    signals: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[RoutedTrade]]:
    output: dict[str, list[dict[str, Any]]] = {
        "component_signals": [],
        "component_entries": [],
        "component_exits": [],
        "component_trades": [],
    }
    events: list[RoutedTrade] = []
    point_value = float(instrument_spec(spec.execution_market).point_value)
    from hydra.execution.v7_cost_model import load_cost_model
    from hydra.propfirm.scaling_plan import mini_equivalent

    cost = float(
        load_cost_model().round_turn_cost(
            spec.execution_market, f"{int(spec.holding_bars)}m"
        )
    )
    highs = matrix.array("bar_high")
    lows = matrix.array("bar_low")
    timestamps = matrix.array("timestamp_ns")
    segments = matrix.array("segment_code")
    forward = matrix.array(f"forward_move__{int(spec.holding_bars)}")
    for signal in signals:
        index = int(signal["row_index"])
        entry_index = index + 1
        exit_index = index + int(spec.holding_bars) + 1
        if exit_index >= matrix.row_count:
            raise ActiveRiskOnlineEquivalenceError("scalar exit exceeds matrix")
        if int(segments[entry_index]) != int(segments[exit_index]):
            raise ActiveRiskOnlineEquivalenceError("scalar trade crosses segment")
        entry = float(signal["entry_price"])
        exit_price = float(matrix.array("bar_close")[exit_index])
        path_high = float(np.max(highs[entry_index : exit_index + 1]))
        path_low = float(np.min(lows[entry_index : exit_index + 1]))
        adverse = path_low if spec.side > 0 else path_high
        favorable = path_high if spec.side > 0 else path_low
        gross = float(forward[index]) * spec.side * point_value
        worst = (adverse - entry) * spec.side * point_value - cost
        best = (favorable - entry) * spec.side * point_value - cost
        event = TradePathEvent(
            event_id=str(signal["signal_id"]),
            decision_ns=int(signal["decision_ns"]),
            exit_ns=int(timestamps[exit_index]) + MINUTE_NS,
            session_day=int(signal["session_day"]),
            net_pnl=float(gross - cost),
            gross_pnl=gross,
            worst_unrealized_pnl=float(worst),
            best_unrealized_pnl=float(best),
            quantity=1,
            mini_equivalent=float(mini_equivalent(spec.execution_market, 1)),
            regime="REHYDRATED_IMMUTABLE_TRADE_PATH",
            session_compliant=True,
            contract_limit_compliant=True,
            same_bar_ambiguous=False,
        )
        routed = RoutedTrade(
            component_id=spec.sleeve_id,
            market=spec.execution_market,
            side=int(spec.side),
            event=event,
        )
        events.append(routed)
        event_time = _ns_iso(event.decision_ns)
        exit_time = _ns_iso(event.exit_ns)
        common = {
            "campaign_id": CAMPAIGN_ID,
            "component_id": spec.sleeve_id,
            "trade_id": event.event_id,
        }
        output["component_signals"].append(
            {
                "campaign_id": CAMPAIGN_ID,
                "component_id": spec.sleeve_id,
                "signal_id": event.event_id,
                "event_time": event_time,
                "market": spec.market,
                "contract": spec.execution_market,
                "timeframe": spec.timeframe,
                "signal": {
                    "side": int(spec.side),
                    "trigger_feature": spec.trigger_feature,
                    "session_code": int(spec.session_code),
                    "closed_or_past_only": True,
                },
                "sizing": 1.0,
                "stop": None,
                "target": None,
                "veto": False,
                "component_role": spec.role.value,
            }
        )
        output["component_entries"].append(
            {
                **common,
                "entry_time": event_time,
                "market": spec.market,
                "contract": spec.execution_market,
                "side": "LONG" if spec.side > 0 else "SHORT",
                "quantity": 1,
                "entry_price": entry,
                "sizing": 1.0,
                "stop_price": None,
                "target_price": None,
            }
        )
        output["component_exits"].append(
            {
                **common,
                "exit_time": exit_time,
                "exit_price": exit_price,
                "exit_reason": f"EXACT_TIME_EXIT_{spec.holding_bars}",
            }
        )
        output["component_trades"].append(
            {
                **common,
                "entry_time": event_time,
                "exit_time": exit_time,
                "market": spec.market,
                "contract": spec.execution_market,
                "side": "LONG" if spec.side > 0 else "SHORT",
                "quantity": 1,
                "entry_price": entry,
                "exit_price": exit_price,
                "gross_pnl": gross,
                "costs": cost,
                "net_pnl": float(gross - cost),
            }
        )
    return output, events


def _compare_component_evidence(
    generated: Mapping[str, Sequence[Mapping[str, Any]]],
    expected: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    datasets: dict[str, Any] = {}
    total = 0
    for name in sorted(expected):
        actual_rows = sorted(generated[name], key=_evidence_sort_key)
        expected_rows = sorted(expected[name], key=_evidence_sort_key)
        mismatch = _sequence_mismatches(actual_rows, expected_rows)
        total += mismatch
        datasets[name] = {
            "generated_count": len(actual_rows),
            "expected_count": len(expected_rows),
            "mismatch_count": mismatch,
            "generated_hash": stable_hash(actual_rows),
            "expected_hash": stable_hash(expected_rows),
        }
    return {
        "status": "EXACT" if total == 0 else "MISMATCH",
        "mismatch_count": total,
        "datasets": datasets,
    }


def _causal_horizon_audit(
    matrices: Mapping[str, FeatureMatrix],
    specs: Mapping[str, Any],
    bindings: Mapping[str, FrozenSignalBinding],
    offline_signals: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Measure the non-causal finite-forward mask rather than hiding it.

    The causal candidate removes only ``isfinite(forward_move)`` while keeping
    every frozen trigger, context, session and non-overlap rule.  Additional
    retained decisions are precisely the signals a live engine would emit
    without outcome-bearing horizon knowledge.
    """

    offline_by_sleeve: dict[str, set[int]] = {}
    for row in offline_signals:
        offline_by_sleeve.setdefault(str(row["component_id"]), set()).add(
            int(str(row["signal_id"]).rsplit(":", 1)[-1])
        )
    sleeves: list[dict[str, Any]] = []
    total = 0
    for sleeve_id in sorted(specs):
        spec = specs[sleeve_id]
        binding = bindings[sleeve_id]
        matrix = matrices[spec.market]
        trigger = matrix.array(f"feature__{binding.trigger_feature}")
        day = matrix.array("session_day")
        session = matrix.array("session_code")
        mask = (
            (day >= DEVELOPMENT_START_DAY)
            & (day < DEVELOPMENT_END_DAY)
            & np.isfinite(trigger)
            & _compare_array(
                trigger, binding.trigger_operator, binding.trigger_threshold
            )
        )
        mask &= session == spec.session_code if spec.session_code >= 0 else session >= 0
        if binding.context_feature is not None:
            context = matrix.array(f"feature__{binding.context_feature}")
            mask &= np.isfinite(context) & _compare_array(
                context,
                str(binding.context_operator),
                float(binding.context_threshold),
            )
        candidates = np.flatnonzero(mask)
        decisions = matrix.array("decision_ns")
        segments = matrix.array("segment_code")
        retained: list[int] = []
        last_segment: int | None = None
        last_exit = -(2**63)
        hold_ns = (int(spec.holding_bars) + 1) * MINUTE_NS
        for raw_index in candidates:
            index = int(raw_index)
            segment = int(segments[index])
            if segment != last_segment:
                last_segment = segment
                last_exit = -(2**63)
            decision = int(decisions[index])
            if decision < last_exit:
                continue
            retained.append(index)
            last_exit = decision + hold_ns
        causal_decisions = {int(decisions[index]) for index in retained}
        offline_decisions = offline_by_sleeve.get(sleeve_id, set())
        additional = sorted(causal_decisions - offline_decisions)
        missing = sorted(offline_decisions - causal_decisions)
        classifications: dict[str, int] = {}
        details: list[dict[str, Any]] = []
        for decision in additional:
            index = int(np.searchsorted(decisions, decision))
            label = _classify_horizon_exclusion(matrix, index, spec.holding_bars)
            classifications[label] = classifications.get(label, 0) + 1
            details.append(
                {
                    "decision_ns": decision,
                    "decision_at_utc": _ns_iso(decision),
                    "row_index": index,
                    "classification": label,
                }
            )
        mismatch = len(additional) + len(missing)
        total += mismatch
        sleeves.append(
            {
                "sleeve_id": sleeve_id,
                "offline_signal_count": len(offline_decisions),
                "causal_without_forward_mask_count": len(causal_decisions),
                "additional_signal_count": len(additional),
                "missing_signal_count": len(missing),
                "classifications": classifications,
                "additional_signals": details,
                "mismatch_count": mismatch,
            }
        )
    return {
        "status": (
            "CAUSAL_EQUIVALENCE_EXACT" if total == 0 else "CAUSAL_EQUIVALENCE_FAILED"
        ),
        "mismatch_count": total,
        "method": "REMOVE_ONLY_FINITE_FORWARD_MOVE_MASK_KEEP_ALL_FROZEN_RULES",
        "sleeves": sleeves,
    }


def _classify_horizon_exclusion(
    matrix: FeatureMatrix, index: int, holding_bars: int
) -> str:
    timestamps = matrix.array("timestamp_ns")
    segments = matrix.array("segment_code")
    sessions = matrix.array("session_day")
    contracts = matrix.array("contract_code")
    exit_index = index + int(holding_bars) + 1
    if index + 1 >= matrix.row_count or exit_index >= matrix.row_count:
        return "DATASET_END"
    if int(timestamps[index + 1]) != int(timestamps[index]) + MINUTE_NS:
        if int(sessions[index + 1]) != int(sessions[index]):
            return "KNOWN_SESSION_BOUNDARY"
        if int(contracts[index + 1]) != int(contracts[index]):
            return "KNOWN_CONTRACT_ROLL"
        return "UNEXPECTED_ONE_MINUTE_GAP"
    if int(segments[exit_index]) != int(segments[index]):
        if int(contracts[exit_index]) != int(contracts[index]):
            return "KNOWN_CONTRACT_ROLL_WITHIN_HORIZON"
        if int(sessions[exit_index]) != int(sessions[index]):
            return "KNOWN_SESSION_BOUNDARY_WITHIN_HORIZON"
        return "UNEXPECTED_SEGMENT_BREAK_WITHIN_HORIZON"
    expected = int(timestamps[index]) + (int(holding_bars) + 1) * MINUTE_NS
    if int(timestamps[exit_index]) != expected:
        return "UNEXPECTED_GAP_WITHIN_HORIZON"
    return "UNCLASSIFIED_FORWARD_NAN"


def _compare_source_tape(
    root: Path, routed: Mapping[str, Sequence[RoutedTrade]]
) -> dict[str, Any]:
    path = root / "data/cache/operating/hydra_operating_package_v1/xfa_source_tape/source_events.jsonl.gz"
    expected = list(_gzip_json_rows(path, metadata=False))
    actual = [
        {
            "schema": "hydra_xfa_source_event_v1",
            "component_id": value.component_id,
            "market": value.market,
            "side": value.side,
            "event": value.event.to_dict(),
        }
        for sleeve_id in sorted(routed)
        for value in routed[sleeve_id]
    ]
    expected.sort(key=lambda row: (row["component_id"], row["event"]["event_id"]))
    actual.sort(key=lambda row: (row["component_id"], row["event"]["event_id"]))
    mismatch = _sequence_mismatches(actual, expected)
    return {
        "status": "EXACT" if mismatch == 0 else "MISMATCH",
        "path": _relative(root, path),
        "file_sha256": _sha256(path),
        "generated_count": len(actual),
        "expected_count": len(expected),
        "mismatch_count": mismatch,
        "generated_hash": stable_hash(actual),
        "expected_hash": stable_hash(expected),
    }


def _virtual_fill_semantics_audit(
    matrices: Mapping[str, FeatureMatrix],
    specs: Mapping[str, Any],
    routed: Mapping[str, Sequence[RoutedTrade]],
) -> dict[str, Any]:
    """Quantify theoretical campaign prices versus frozen forward fill rules."""

    total = raw_open_diff = adverse_entry_diff = adverse_exit_diff = 0
    sleeves: list[dict[str, Any]] = []
    for sleeve_id in sorted(routed):
        spec = specs[sleeve_id]
        matrix = matrices[spec.market]
        decisions = matrix.array("decision_ns")
        entry_prices = matrix.array("entry_price")
        opens = matrix.array("bar_open")
        closes = matrix.array("bar_close")
        tick = float(instrument_spec(spec.execution_market).tick_size)
        local_total = local_raw = local_entry = local_exit = 0
        for trade in routed[sleeve_id]:
            index = int(np.searchsorted(decisions, trade.event.decision_ns))
            exit_index = index + int(spec.holding_bars) + 1
            theoretical_entry = float(entry_prices[index])
            next_open = float(opens[index + 1])
            conservative_entry = next_open + tick * int(spec.side)
            theoretical_exit = float(closes[exit_index])
            conservative_exit = theoretical_exit - tick * int(spec.side)
            local_total += 1
            local_raw += int(
                not math.isclose(theoretical_entry, next_open, rel_tol=0.0, abs_tol=1e-12)
            )
            local_entry += int(
                not math.isclose(
                    theoretical_entry,
                    conservative_entry,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                )
            )
            local_exit += int(
                not math.isclose(
                    theoretical_exit,
                    conservative_exit,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                )
            )
        total += local_total
        raw_open_diff += local_raw
        adverse_entry_diff += local_entry
        adverse_exit_diff += local_exit
        sleeves.append(
            {
                "sleeve_id": sleeve_id,
                "entries": local_total,
                "theoretical_entry_vs_next_open_mismatches": local_raw,
                "theoretical_entry_vs_adverse_one_tick_next_open_mismatches": local_entry,
                "theoretical_exit_vs_adverse_one_tick_horizon_close_mismatches": local_exit,
            }
        )
    mismatch = adverse_entry_diff + adverse_exit_diff
    return {
        "status": "EXACT" if mismatch == 0 else "SEMANTICS_DIVERGE",
        "mismatch_count": mismatch,
        "entry_count": total,
        "theoretical_entry_is_next_row_close_count": total,
        "theoretical_entry_vs_next_open_mismatch_count": raw_open_diff,
        "theoretical_entry_vs_adverse_one_tick_next_open_mismatch_count": adverse_entry_diff,
        "theoretical_exit_vs_adverse_one_tick_horizon_close_mismatch_count": adverse_exit_diff,
        "offline_semantics": "NEXT_ROW_CLOSE_ENTRY_AND_HORIZON_CLOSE_EXIT",
        "forward_semantics": "NEXT_BAR_OPEN_PLUS_ADVERSE_ONE_TICK_ENTRY_AND_ADVERSE_ONE_TICK_EXIT",
        "sleeves": sleeves,
    }


def _bounded_virtual_fill_counts(
    matrices: Mapping[str, FeatureMatrix],
    specs: Mapping[str, Any],
    evidence_root: Path,
) -> dict[str, Any]:
    per_sleeve: dict[str, dict[str, int]] = {
        sleeve_id: {
            "entry_count": 0,
            "theoretical_entry_vs_frozen_matrix_mismatches": 0,
            "theoretical_entry_vs_next_open_mismatches": 0,
            "theoretical_entry_vs_adverse_one_tick_next_open_mismatches": 0,
            "theoretical_exit_vs_adverse_one_tick_horizon_close_mismatches": 0,
        }
        for sleeve_id in specs
    }
    for row in _evidence_rows(evidence_root, "component_entries"):
        sleeve_id = str(row["component_id"])
        spec = specs[sleeve_id]
        matrix = matrices[spec.market]
        decision = int(str(row["trade_id"]).rsplit(":", 1)[-1])
        index = int(np.searchsorted(matrix.array("decision_ns"), decision))
        theoretical = float(row["entry_price"])
        frozen_entry = float(matrix.array("entry_price")[index])
        next_open = float(matrix.array("bar_open")[index + 1])
        tick = float(instrument_spec(spec.execution_market).tick_size)
        adverse = next_open + tick * int(spec.side)
        target = per_sleeve[sleeve_id]
        target["entry_count"] += 1
        target["theoretical_entry_vs_frozen_matrix_mismatches"] += int(
            not math.isclose(theoretical, frozen_entry, rel_tol=0.0, abs_tol=1e-12)
        )
        target["theoretical_entry_vs_next_open_mismatches"] += int(
            not math.isclose(theoretical, next_open, rel_tol=0.0, abs_tol=1e-12)
        )
        target[
            "theoretical_entry_vs_adverse_one_tick_next_open_mismatches"
        ] += int(
            not math.isclose(theoretical, adverse, rel_tol=0.0, abs_tol=1e-12)
        )
        # The frozen forward exit is one adverse tick away from the historical
        # horizon close.  Positive tick sizes make every theoretical exit
        # differ, independent of side.
        target[
            "theoretical_exit_vs_adverse_one_tick_horizon_close_mismatches"
        ] += 1
    totals = {
        key: sum(row[key] for row in per_sleeve.values())
        for key in next(iter(per_sleeve.values()))
    }
    if totals != {
        "entry_count": 2052,
        "theoretical_entry_vs_frozen_matrix_mismatches": 0,
        "theoretical_entry_vs_next_open_mismatches": 1993,
        "theoretical_entry_vs_adverse_one_tick_next_open_mismatches": 1988,
        "theoretical_exit_vs_adverse_one_tick_horizon_close_mismatches": 2052,
    }:
        raise ActiveRiskOnlineEquivalenceError(
            f"bounded virtual-fill counts drifted: {totals}"
        )
    mismatch = (
        totals["theoretical_entry_vs_adverse_one_tick_next_open_mismatches"]
        + totals[
            "theoretical_exit_vs_adverse_one_tick_horizon_close_mismatches"
        ]
    )
    return {
        "status": "SEMANTICS_DIVERGE",
        "mismatch_count": mismatch,
        **totals,
        "per_sleeve": [
            {"sleeve_id": sleeve_id, **per_sleeve[sleeve_id]}
            for sleeve_id in sorted(per_sleeve)
        ],
    }


def _reconcile_six_books(
    *,
    root: Path,
    reconstructed: Sequence[Any],
    routed: Mapping[str, Sequence[RoutedTrade]],
    matrices: Mapping[str, FeatureMatrix],
    evidence_root: Path,
) -> dict[str, Any]:
    manifest = _json(root / DEFAULT_CAMPAIGN_MANIFEST)
    reference = reconstructed[0]
    runtimes: dict[str, Any] = {}
    for sleeve_id, spec in reference.sleeve_specs.items():
        matrix = matrices[spec.market]
        day = matrix.array("session_day")
        session = matrix.array("session_code")
        runtimes[sleeve_id] = SimpleNamespace(
            eligible_session_days=tuple(sorted({int(value) for value in day[session >= 0]}))
        )
    starts48 = _block_aware_starts(runtimes, manifest, maximum=48)
    starts96 = _block_aware_starts(
        runtimes, manifest, maximum=96, required_starts=starts48
    )
    starts192 = _block_aware_starts(
        runtimes, manifest, maximum=192, required_starts=starts96
    )
    calendars = _block_calendars(runtimes, manifest, starts192)
    expected_rows, batch_prefixes = _selected_episode_evidence(
        evidence_root,
        {row.package.candidate_id for row in reconstructed},
    )
    expected = {
        (
            str(row["policy_id"]),
            int(str(row["episode_id"]).rsplit(":", 1)[-1]),
            str(row["cost_scenario"]),
        ): row
        for row in expected_rows
        if row["horizon"] == "90_TRADING_DAYS"
    }
    expected_daily_by_key, expected_daily_count = _selected_daily_evidence_digests(
        evidence_root,
        batch_prefixes,
        {row.package.candidate_id for row in reconstructed},
    )
    rules = official_rule_snapshot_2026_07_15().combine_config()
    books: list[dict[str, Any]] = []
    total_mismatch = 0
    generated_episode_count = 0
    generated_daily_count = 0
    for book in sorted(reconstructed, key=lambda row: row.package.candidate_id):
        book_mismatch = 0
        scenario_counts: dict[str, int] = {}
        for scenario, component_events in (
            ("NORMAL", routed),
            (
                "STRESSED_1_5X",
                {
                    key: tuple(_restress(row) for row in values)
                    for key, values in routed.items()
                },
            ),
        ):
            scenario_counts[scenario] = 0
            for start in starts192:
                episode = run_shared_account_episode(
                    component_events,
                    calendars[start],
                    basket=book.combine_policy,
                    active_pool_policy=book.combine_policy,
                    start_day=int(start),
                    maximum_duration_days=90,
                    config=rules,
                )
                key = (book.package.candidate_id, int(start), scenario)
                expected_row = expected.get(key)
                if expected_row is None:
                    book_mismatch += 1
                    continue
                generated_episode_count += 1
                scenario_counts[scenario] += 1
                actual = _account_episode_projection(episode)
                wanted = _expected_episode_projection(expected_row)
                book_mismatch += _mapping_mismatches(actual, wanted)
                actual_daily = _daily_projection(episode, scenario=scenario)
                generated_daily_count += len(actual_daily)
                actual_digest = _row_chain_digest(actual_daily)
                wanted_digest = expected_daily_by_key.get(key)
                book_mismatch += int(wanted_digest != actual_digest)
        total_mismatch += book_mismatch
        books.append(
            {
                "candidate_id": book.package.candidate_id,
                "starts_per_scenario": len(starts192),
                "normal_episodes": scenario_counts.get("NORMAL", 0),
                "stressed_episodes": scenario_counts.get("STRESSED_1_5X", 0),
                "mismatch_count": book_mismatch,
                "status": "EXACT" if book_mismatch == 0 else "MISMATCH",
            }
        )
    return {
        "status": "EXACT" if total_mismatch == 0 else "MISMATCH",
        "mismatch_count": total_mismatch,
        "nested_start_count_per_scenario": len(starts192),
        "generated_episode_count": generated_episode_count,
        "expected_episode_count": len(expected),
        "generated_daily_path_count": generated_daily_count,
        "expected_daily_path_count": expected_daily_count,
        "daily_comparison": "PER_EPISODE_CANONICAL_ROW_CHAIN_HASH",
        "books": books,
    }


def _account_episode_projection(episode: Any) -> dict[str, Any]:
    terminal = (
        "TARGET_REACHED"
        if episode.passed
        else "MLL_BREACHED"
        if episode.mll_breached
        else "DATA_CENSORED"
    )
    return {
        "terminal_state": terminal,
        "target_reached": bool(episode.passed),
        "mll_breached": bool(episode.mll_breached),
        "net_pnl": float(episode.net_pnl),
        "costs": float(episode.total_cost),
        "target_progress": float(episode.target_progress),
        "minimum_mll_buffer": float(episode.minimum_mll_buffer),
        "consistency_ok": bool(episode.consistency_ok),
        "days_to_target": episode.days_to_target,
        "component_contribution": dict(sorted(episode.component_contribution.items())),
    }


def _expected_episode_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "terminal_state": str(row["terminal_state"]),
        "target_reached": bool(row["target_reached"]),
        "mll_breached": bool(row["mll_breached"]),
        "net_pnl": float(row["net_pnl"]),
        "costs": float(row["costs"]),
        "target_progress": float(row["target_progress"]),
        "minimum_mll_buffer": float(row["minimum_mll_buffer"]),
        "consistency_ok": bool(row["consistency_ok"]),
        "days_to_target": row.get("days_to_target"),
        "component_contribution": dict(sorted(dict(row["component_contribution"]).items())),
    }


def _daily_projection(episode: Any, *, scenario: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in episode.daily_path:
        rows.append(
            {
                "trading_day": _epoch_day_iso(int(day["session_day"])),
                "cost_scenario": scenario,
                "horizon": "90_TRADING_DAYS",
                "realized_pnl": float(day["realized_pnl"]),
                "unrealized_pnl": float(day["unrealized_pnl"]),
                "daily_pnl": float(day["day_pnl"]),
                "equity": float(day["balance"]),
                "mll": float(day["mll_floor"]),
                "mll_buffer": float(day["closing_mll_buffer"]),
                "minimum_mll_buffer": float(day["minimum_mll_buffer"]),
                "consistency": 1.0 if bool(day["consistency_ok"]) else 0.0,
                "consistency_ok": bool(day["consistency_ok"]),
                "target_progress": float(day["target_progress"]),
                "costs": float(day["costs"]),
                "conflicts": list(day["conflicts"]),
                "exposure": dict(day["exposure"]),
                "component_attribution": dict(day["component_attribution"]),
                "risk_allocation": list(day["routing_decisions"]),
                "closing_mll_buffer": float(day["closing_mll_buffer"]),
                "cumulative_costs": float(day["cumulative_costs"]),
            }
        )
    if rows:
        rows[-1]["consistency_ok"] = bool(episode.consistency_ok)
        rows[-1]["consistency"] = 1.0 if episode.consistency_ok else 0.0
        rows[-1]["target_progress"] = float(episode.target_progress)
    return rows


def _expected_daily_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trading_day": str(row["trading_day"]),
        "cost_scenario": str(row["cost_scenario"]),
        "horizon": str(row["horizon"]),
        "realized_pnl": float(row["realized_pnl"]),
        "unrealized_pnl": float(row["unrealized_pnl"]),
        "daily_pnl": float(row["daily_pnl"]),
        "equity": float(row["equity"]),
        "mll": float(row["mll"]),
        "mll_buffer": float(row["mll_buffer"]),
        "minimum_mll_buffer": float(row["minimum_mll_buffer"]),
        "consistency": float(row["consistency"]),
        "consistency_ok": bool(row["consistency_ok"]),
        "target_progress": float(row["target_progress"]),
        "costs": float(row["costs"]),
        "conflicts": list(row["conflicts"]),
        "exposure": dict(row["exposure"]),
        "component_attribution": dict(row["component_attribution"]),
        "risk_allocation": list(row["risk_allocation"]),
        "closing_mll_buffer": float(row["closing_mll_buffer"]),
        "cumulative_costs": float(row["cumulative_costs"]),
    }


def _selected_episode_evidence(
    evidence_root: Path, package_ids: set[str]
) -> tuple[list[dict[str, Any]], set[str]]:
    rows: list[dict[str, Any]] = []
    prefixes: set[str] = set()
    directory = evidence_root / "datasets/episodes"
    for path in sorted(directory.glob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            metadata = json.loads(next(handle))["_evidence_part"]
            batch_id = str(metadata["batch_id"])
            if not any(token in batch_id for token in (":stage3:", ":stage4:", ":stage5:")):
                continue
            selected = []
            for line in handle:
                row = json.loads(line)
                if str(row.get("policy_id")) in package_ids:
                    selected.append(row)
            if selected:
                rows.extend(selected)
                prefixes.add(batch_id.rsplit(":", 1)[0])
    return rows, prefixes


def _selected_daily_evidence_digests(
    evidence_root: Path,
    prefixes: set[str],
    package_ids: set[str],
) -> tuple[dict[tuple[str, int, str], tuple[int, str]], int]:
    chains: dict[tuple[str, int, str], tuple[int, str]] = {}
    total = 0
    directory = evidence_root / "datasets/account_daily_paths"
    for path in sorted(directory.glob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            metadata = json.loads(next(handle))["_evidence_part"]
            prefix = str(metadata["batch_id"]).rsplit(":", 1)[0]
            if prefix not in prefixes:
                continue
            for line in handle:
                row = json.loads(line)
                if (
                    str(row.get("policy_id")) not in package_ids
                    or row.get("horizon") != "90_TRADING_DAYS"
                ):
                    continue
                key = (
                    str(row["policy_id"]),
                    int(str(row["episode_id"]).rsplit(":", 1)[-1]),
                    str(row["cost_scenario"]),
                )
                count, previous = chains.get(key, (0, _ZERO_HASH))
                fingerprint = stable_hash(_expected_daily_projection(row))
                chains[key] = (
                    count + 1,
                    hashlib.sha256((previous + fingerprint).encode("ascii")).hexdigest(),
                )
                total += 1
    return chains, total


def _row_chain_digest(rows: Sequence[Mapping[str, Any]]) -> tuple[int, str]:
    previous = _ZERO_HASH
    for row in rows:
        fingerprint = stable_hash(dict(row))
        previous = hashlib.sha256((previous + fingerprint).encode("ascii")).hexdigest()
    return len(rows), previous


def _reconcile_rebuilt_features(
    root: Path,
    frozen: Mapping[str, FeatureMatrix],
    raw_rebuild_root: str | Path | None,
) -> dict[str, Any]:
    if raw_rebuild_root is None:
        return {
            "status": "NOT_RUN",
            "mismatch_count": 1,
            "reason": "raw rebuild root was not supplied",
            "feature_record_replay_is_not_raw_feature_proof": True,
        }
    rebuild_root = _inside(root, raw_rebuild_root)
    rebuilt: dict[str, FeatureMatrix] = {}
    for manifest_path in rebuild_root.glob("*/manifest.json"):
        matrix = FeatureMatrix.open(manifest_path.parent)
        rebuilt[str(matrix.manifest["key"]["market"])] = matrix
    rows: list[dict[str, Any]] = []
    mismatch = 0
    for market, matrix in sorted(frozen.items()):
        other = rebuilt.get(market)
        if other is None:
            mismatch += 1
            rows.append({"market": market, "status": "MISSING_REBUILD"})
            continue
        array_mismatches = []
        for name, metadata in matrix.manifest["arrays"].items():
            candidate = other.manifest["arrays"].get(name)
            if candidate != metadata:
                array_mismatches.append(name)
        row_mismatch = int(
            matrix.fingerprint != other.fingerprint
            or matrix.row_count != other.row_count
            or bool(array_mismatches)
        )
        mismatch += row_mismatch
        rows.append(
            {
                "market": market,
                "frozen_path": _relative(root, matrix.root),
                "rebuilt_path": _relative(root, other.root),
                "row_count": matrix.row_count,
                "frozen_bundle_hash": matrix.fingerprint,
                "rebuilt_bundle_hash": other.fingerprint,
                "array_mismatches": array_mismatches,
                "status": "BYTE_EXACT" if row_mismatch == 0 else "MISMATCH",
            }
        )
    return {
        "status": "BYTE_EXACT" if mismatch == 0 else "MISMATCH",
        "mismatch_count": mismatch,
        "source": "bounded exact rebuild from governed cached parquet",
        "feature_record_replay_is_not_raw_feature_proof": False,
        "markets": rows,
    }


def _guard_self_test() -> dict[str, Any]:
    base = {
        "timestamp_ns": 1_000 * MINUTE_NS,
        "availability_ns": 1_001 * MINUTE_NS,
        "observed_ns": 1_001 * MINUTE_NS,
        "segment_code": 1,
        "session_day": 20_000,
        "contract": "ESU6",
        "close": 100.0,
    }
    checks: dict[str, bool] = {}
    guard = CanonicalBarOrderGuard()
    checks["first_bar"] = guard.process(base) == "ACCEPTED"
    checks["contiguous_bar"] = guard.process(
        {**base, "timestamp_ns": 1_001 * MINUTE_NS, "availability_ns": 1_002 * MINUTE_NS, "observed_ns": 1_002 * MINUTE_NS, "close": 101.0}
    ) == "ACCEPTED"
    checks["duplicate_rejected"] = _guard_rejects(
        CanonicalBarOrderGuard(), base, base, reason="DUPLICATE_BAR"
    )
    checks["out_of_order_rejected"] = _guard_rejects(
        CanonicalBarOrderGuard(),
        base,
        {**base, "timestamp_ns": 999 * MINUTE_NS, "availability_ns": 1_000 * MINUTE_NS},
        reason="OUT_OF_ORDER_BAR",
    )
    checks["missing_interval_rejected"] = _guard_rejects(
        CanonicalBarOrderGuard(),
        base,
        {**base, "timestamp_ns": 1_002 * MINUTE_NS, "availability_ns": 1_003 * MINUTE_NS, "observed_ns": 1_003 * MINUTE_NS},
        reason="MISSING_INTERVAL",
    )
    checks["late_rejected"] = _guard_rejects(
        CanonicalBarOrderGuard(),
        {**base, "availability_ns": 1_002 * MINUTE_NS, "observed_ns": 1_001 * MINUTE_NS},
        reason="LATE_OR_NOT_YET_AVAILABLE_BAR",
    )
    session_guard = CanonicalBarOrderGuard()
    session_guard.process(base)
    checks["session_transition"] = session_guard.process(
        {**base, "timestamp_ns": 1_100 * MINUTE_NS, "availability_ns": 1_101 * MINUTE_NS, "observed_ns": 1_101 * MINUTE_NS, "segment_code": 2, "session_day": 20_001}
    ) == "SESSION_TRANSITION"
    roll_guard = CanonicalBarOrderGuard()
    roll_guard.process(base)
    checks["contract_roll"] = roll_guard.process(
        {**base, "timestamp_ns": 1_100 * MINUTE_NS, "availability_ns": 1_101 * MINUTE_NS, "observed_ns": 1_101 * MINUTE_NS, "segment_code": 2, "contract": "ESZ6"}
    ) == "CONTRACT_ROLL"
    checkpoint = roll_guard.checkpoint()
    unhashed = dict(checkpoint)
    expected = unhashed.pop("checkpoint_hash")
    checks["checkpoint_hash"] = expected == stable_hash(unhashed)
    mismatch = sum(not value for value in checks.values())
    return {
        "status": "PASS" if mismatch == 0 else "FAIL",
        "mismatch_count": mismatch,
        "checks": checks,
    }


def _guard_rejects(
    guard: CanonicalBarOrderGuard,
    *records: Mapping[str, Any],
    reason: str,
) -> bool:
    try:
        for record in records:
            guard.process(record)
    except ActiveRiskOnlineEquivalenceError as exc:
        return str(exc) == reason
    return False


def _open_frozen_matrices(
    root: Path, bindings: Mapping[str, FrozenSignalBinding]
) -> dict[str, FeatureMatrix]:
    output: dict[str, FeatureMatrix] = {}
    for binding in bindings.values():
        path = _inside(root, binding.feature_matrix_manifest_path)
        if _sha256(path) != binding.feature_matrix_manifest_sha256:
            raise ActiveRiskOnlineEquivalenceError("frozen matrix manifest drift")
        if binding.feature_matrix_market not in output:
            output[binding.feature_matrix_market] = FeatureMatrix.open(path.parent)
        matrix = output[binding.feature_matrix_market]
        if matrix.fingerprint != binding.feature_matrix_bundle_hash:
            raise ActiveRiskOnlineEquivalenceError("frozen matrix bundle drift")
    return output


def _assert_common_sleeve_contract(rows: Sequence[Any]) -> None:
    first = rows[0]
    baseline = {
        sleeve_id: {
            "spec": spec.to_dict(),
            "record": first.sleeve_records[sleeve_id].to_dict(),
            "binding": first.frozen_signal_bindings[sleeve_id].to_dict(),
        }
        for sleeve_id, spec in first.sleeve_specs.items()
    }
    if len(baseline) != 18:
        raise ActiveRiskOnlineEquivalenceError("frozen sleeve bank is not 18-wide")
    for row in rows[1:]:
        candidate = {
            sleeve_id: {
                "spec": spec.to_dict(),
                "record": row.sleeve_records[sleeve_id].to_dict(),
                "binding": row.frozen_signal_bindings[sleeve_id].to_dict(),
            }
            for sleeve_id, spec in row.sleeve_specs.items()
        }
        if candidate != baseline:
            raise ActiveRiskOnlineEquivalenceError("six books do not share frozen sleeves")


def _evidence_rows(evidence_root: Path, dataset: str) -> Iterable[dict[str, Any]]:
    for path in sorted((evidence_root / "datasets" / dataset).glob("*.jsonl.gz")):
        yield from _gzip_json_rows(path, metadata=True)


def _gzip_json_rows(path: Path, *, metadata: bool) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if index == 0 and metadata:
                continue
            yield json.loads(line)


def _compare_scalar(left: float, operator: str, right: float) -> bool:
    return {
        "GT": left > right,
        "GE": left >= right,
        "LT": left < right,
        "LE": left <= right,
    }[str(operator)]


def _compare_array(values: np.ndarray, operator: str, threshold: float) -> np.ndarray:
    if operator == "GT":
        return values > threshold
    if operator == "GE":
        return values >= threshold
    if operator == "LT":
        return values < threshold
    if operator == "LE":
        return values <= threshold
    raise ActiveRiskOnlineEquivalenceError(f"unsupported comparison: {operator}")


def _evidence_sort_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("component_id") or ""),
        str(row.get("signal_id") or row.get("trade_id") or ""),
    )


def _sequence_mismatches(left: Sequence[Any], right: Sequence[Any]) -> int:
    mismatch = abs(len(left) - len(right))
    for actual, expected in zip(left, right):
        mismatch += _mapping_mismatches(actual, expected)
    return mismatch


def _mapping_mismatches(actual: Any, expected: Any) -> int:
    if isinstance(actual, Mapping) and isinstance(expected, Mapping):
        keys = set(actual) | set(expected)
        return sum(
            1
            if key not in actual or key not in expected
            else _mapping_mismatches(actual[key], expected[key])
            for key in keys
        )
    if isinstance(actual, Sequence) and not isinstance(actual, (str, bytes)) and isinstance(expected, Sequence) and not isinstance(expected, (str, bytes)):
        return _sequence_mismatches(list(actual), list(expected))
    if isinstance(actual, (float, int)) and not isinstance(actual, bool) and isinstance(expected, (float, int)) and not isinstance(expected, bool):
        a = float(actual)
        b = float(expected)
        if math.isnan(a) and math.isnan(b):
            return 0
        return int(not math.isclose(a, b, rel_tol=0.0, abs_tol=1e-8))
    return int(actual != expected)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if math.isnan(number):
            return "NaN"
        if number == math.inf:
            return "+Infinity"
        if number == -math.inf:
            return "-Infinity"
        return number
    if isinstance(value, np.integer):
        return int(value)
    return value


def _record_fingerprint(record: Mapping[str, Any]) -> str:
    """Fast canonical fingerprint for the hot scalar-record loop."""

    digest = hashlib.sha256()
    for key in sorted(record):
        digest.update(str(key).encode("utf-8"))
        digest.update(b"\0")
        value = record[key]
        if value is None:
            digest.update(b"N")
        elif isinstance(value, bool):
            digest.update(b"B1" if value else b"B0")
        elif isinstance(value, (int, np.integer)):
            digest.update(b"I" + str(int(value)).encode("ascii"))
        elif isinstance(value, (float, np.floating)):
            digest.update(b"F" + struct.pack(">d", float(value)))
        else:
            digest.update(b"S" + str(value).encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActiveRiskOnlineEquivalenceError(f"invalid JSON input: {path}") from exc
    if not isinstance(value, dict):
        raise ActiveRiskOnlineEquivalenceError(f"JSON input is not an object: {path}")
    return value


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ActiveRiskOnlineEquivalenceError("path escapes repository root") from exc
    return resolved


def _relative(root: Path, path: Path) -> str:
    return str(path.resolve().relative_to(root)).replace("\\", "/")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ns_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat().replace(
        "+00:00", "Z"
    )


def _epoch_day_iso(value: int) -> str:
    return str(np.datetime64("1970-01-01", "D") + np.timedelta64(value, "D"))


__all__ = [
    "ActiveRiskOnlineEquivalenceError",
    "CanonicalBarOrderGuard",
    "DEFAULT_AUDIT_PATH",
    "DEFAULT_RECEIPT_PATH",
    "ENGINE_VERSION",
    "EQUIVALENCE_SCHEMA",
    "FrozenSleeveOnlineEngine",
    "FrozenSleeveStreamState",
    "ONLINE_OFFLINE_EQUIVALENCE_FAILED_CLOSED",
    "ONLINE_OFFLINE_EQUIVALENCE_PROVEN",
    "prove_active_risk_online_equivalence",
    "verify_active_risk_online_equivalence_proof",
    "write_bounded_fail_closed_equivalence_audit",
]
