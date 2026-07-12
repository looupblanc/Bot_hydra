from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from hydra.research.qd_economic_tournament import _round_turn_cost_all
from hydra.research.turbo_feature_builder import FEATURE_BUNDLE_VERSION, FEATURE_DAG_HASH


PACKAGE_SCHEMA = "hydra_immutable_shadow_package_v4"


class ShadowPackageError(RuntimeError):
    pass


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


@dataclass(frozen=True)
class ImmutableShadowPackage:
    candidate_id: str
    candidate_specification_hash: str
    source_commit: str
    freeze_timestamp_utc: str
    role: str
    market_policy: Mapping[str, Any]
    timeframe_profile: tuple[str, ...]
    feature_contract: Mapping[str, Any]
    entry_policy: Mapping[str, Any]
    exit_policy: Mapping[str, Any]
    sizing_policy: Mapping[str, Any]
    session_policy: Mapping[str, Any]
    cost_policy: Mapping[str, Any]
    risk_policy: Mapping[str, Any]
    data_policy: Mapping[str, Any]
    signal_policy: Mapping[str, Any]
    virtual_fill_policy: Mapping[str, Any]
    kill_conditions: tuple[str, ...]
    observability: Mapping[str, Any]
    evidence_provenance: Mapping[str, Any]
    broker_connectivity: bool = False
    outbound_order_capability: bool = False

    def semantic_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schema"] = PACKAGE_SCHEMA
        return payload

    @property
    def package_hash(self) -> str:
        return _stable_hash(self.semantic_payload())

    def validate(self) -> None:
        if self.broker_connectivity or self.outbound_order_capability:
            raise ShadowPackageError("Shadow packages cannot contain broker or order capability.")
        if not self.candidate_id or len(self.candidate_specification_hash) != 64:
            raise ShadowPackageError("Immutable candidate identity is incomplete.")
        if len(self.source_commit) != 40 or not self.freeze_timestamp_utc:
            raise ShadowPackageError("Source commit and freeze time are mandatory.")
        if not self.timeframe_profile or not self.feature_contract:
            raise ShadowPackageError("Feature and timeframe contracts are mandatory.")
        if not self.entry_policy or not self.exit_policy or not self.sizing_policy:
            raise ShadowPackageError("Executable signal rules are incomplete.")
        if float(self.risk_policy.get("simulated_mll_floor_usd") or 0.0) >= 0.0:
            raise ShadowPackageError("The simulated MLL floor must be a negative boundary.")
        if int(self.data_policy.get("stale_after_seconds") or 0) <= 0:
            raise ShadowPackageError("A positive fail-closed stale-data threshold is required.")
        if not bool(self.data_policy.get("post_freeze_only")):
            raise ShadowPackageError("Forward evidence must be post-freeze only.")
        if not bool(self.signal_policy.get("duplicate_signal_guard")):
            raise ShadowPackageError("Duplicate-signal rejection is mandatory.")
        if str(self.virtual_fill_policy.get("mode")) != "CONSERVATIVE_VIRTUAL_ONLY":
            raise ShadowPackageError("Only conservative virtual fills are permitted.")
        if not self.kill_conditions or not self.observability.get("ledger_path"):
            raise ShadowPackageError("Kill conditions and an audit ledger are mandatory.")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {**self.semantic_payload(), "package_hash": self.package_hash}


def build_shadow_package(
    specification: Mapping[str, Any],
    validation: Mapping[str, Any],
    *,
    source_commit: str,
    freeze_timestamp_utc: str,
    evidence_sha256: str,
) -> ImmutableShadowPackage:
    candidate_id = str(specification.get("candidate_id") or "")
    encoded_spec = json.dumps(
        dict(specification), sort_keys=True, separators=(",", ":")
    ).encode()
    specification_hash = hashlib.sha256(encoded_spec).hexdigest()
    if specification_hash != str(validation.get("immutable_specification_hash") or ""):
        raise ShadowPackageError(f"Specification hash drift for {candidate_id}.")
    primary_market = str(validation.get("primary_market") or specification.get("market") or "")
    execution_market = str(validation.get("execution_market") or primary_market)
    topstep = dict((validation.get("risk") or {}).get("topstep") or {})
    quantity = int(topstep.get("selected_micro_contracts") or 1)
    timeframes = tuple(
        part for part in str(specification.get("timeframe") or "1m").split("|") if part
    )
    feature_names = [str(specification.get("feature") or "")]
    if specification.get("context_feature"):
        feature_names.append(str(specification["context_feature"]))
    ledger_path = f"shadow/state/forward/{candidate_id}/forward_evidence.jsonl"
    return ImmutableShadowPackage(
        candidate_id=candidate_id,
        candidate_specification_hash=specification_hash,
        source_commit=source_commit,
        freeze_timestamp_utc=freeze_timestamp_utc,
        role=str(validation.get("role") or "UNKNOWN"),
        market_policy={
            "source_market": primary_market,
            "execution_market": execution_market,
            "explicit_contracts_required": True,
            "date_aware_roll_policy": "provider_symbology_plus_frozen_roll_exclusions_v1",
            "mini_micro_independence_count": "same_economic_mechanism",
        },
        timeframe_profile=timeframes,
        feature_contract={
            "feature_names": feature_names,
            "feature_bundle_version": FEATURE_BUNDLE_VERSION,
            "feature_dag_hash": FEATURE_DAG_HASH,
            "closed_bars_only": True,
            "source_close_must_precede_decision": True,
            "availability_must_not_exceed_decision": True,
            "higher_timeframe_partial_bars_prohibited": True,
        },
        entry_policy={
            "decision_bar": "closed_1m_bar",
            "entry_bar": "next_1m_bar_open_proxy",
            "side": int(specification.get("side") or 0),
            "feature": str(specification.get("feature") or ""),
            "operator": int(specification.get("operator") or 0),
            "threshold": float(specification.get("threshold") or 0.0),
            "context_feature": specification.get("context_feature"),
            "context_operator": specification.get("context_operator"),
            "context_threshold": specification.get("context_threshold"),
            "session_code": int(specification.get("session_code") or 0),
        },
        exit_policy={
            "type": "fixed_completed_bar_horizon",
            "holding_1m_bars": int(specification.get("holding_events") or 0),
            "mandatory_session_flatten": True,
            "cross_contract_holding_prohibited": True,
        },
        sizing_policy={
            "type": "frozen_fixed_micro_quantity",
            "execution_market": execution_market,
            "quantity": quantity,
            "maximum_quantity": quantity,
            "dynamic_resizing": False,
        },
        session_policy={
            "calendar": "CME_GLOBEX_AMERICA_CHICAGO",
            "trading_day_from_local_17_00": True,
            "dst_aware": True,
            "session_code": int(specification.get("session_code") or 0),
            "mandatory_flatten": True,
        },
        cost_policy={
            "frozen_primary_round_turn_usd": float(
                specification.get("round_turn_cost") or 0.0
            ),
            "frozen_execution_round_turn_usd": float(
                _round_turn_cost_all(execution_market)
            ),
            "quantity": quantity,
            "slippage": "one_tick_equivalent_or_worse_virtual_proxy",
            "cost_stress_multiplier": 1.5,
        },
        risk_policy={
            "topstep_rule_version": str(topstep.get("rule_version") or ""),
            "simulated_mll_floor_usd": -4500.0,
            "internal_daily_risk_limit_usd": 1500.0,
            "maximum_open_contracts": quantity,
            "shared_account_contract_limit": 15,
            "fail_closed": True,
        },
        data_policy={
            "post_freeze_only": True,
            "freeze_timestamp_utc": freeze_timestamp_utc,
            "expected_bar_seconds": 60,
            "stale_after_seconds": 180,
            "duplicate_bars_rejected": True,
            "missing_intervals_flagged": True,
            "append_only": True,
        },
        signal_policy={
            "deterministic": True,
            "duplicate_signal_guard": True,
            "duplicate_window_seconds": max(
                int(specification.get("holding_events") or 1) * 60, 60
            ),
            "startup_reconciliation": "replay_append_only_store_then_resume",
            "stale_data_action": "REJECT_SIGNAL_AND_FAIL_CLOSED",
        },
        virtual_fill_policy={
            "mode": "CONSERVATIVE_VIRTUAL_ONLY",
            "entry": "next_bar_open_plus_adverse_slippage",
            "exit": "frozen_horizon_close_minus_adverse_slippage",
            "same_bar_ambiguity": "adverse_path",
            "real_order_submission": False,
        },
        kill_conditions=(
            "stale_or_missing_data",
            "contract_resolution_failure",
            "clock_or_session_mismatch",
            "duplicate_signal",
            "simulated_mll_floor_reached",
            "configuration_hash_mismatch",
        ),
        observability={
            "ledger_path": ledger_path,
            "required_fields": [
                "signal_timestamp",
                "data_freshness_seconds",
                "strategy_version",
                "theoretical_entry",
                "virtual_entry",
                "virtual_exit",
                "slippage_usd",
                "latency_proxy_ms",
                "virtual_pnl_usd",
                "mll_buffer_usd",
                "regime",
            ],
            "reconciliation_required": True,
        },
        evidence_provenance={
            "validation_sha256": evidence_sha256,
            "validation_version": "hydra_evidence_conversion_foundry_v3",
            "candidate_status": "PRE_HOLDOUT_READY",
            "q4_evidence_inherited": False,
        },
    )


def write_shadow_package(
    package: ImmutableShadowPackage, directory: str | Path
) -> tuple[Path, Path]:
    package.validate()
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    machine = root / "shadow_package.json"
    dossier = root / "shadow_package.md"
    payload = json.dumps(package.to_dict(), indent=2, sort_keys=True, default=str) + "\n"
    human = (
        f"# Shadow package — {package.candidate_id}\n\n"
        f"- Role: `{package.role}`\n"
        f"- Frozen at: `{package.freeze_timestamp_utc}`\n"
        f"- Source commit: `{package.source_commit}`\n"
        f"- Specification hash: `{package.candidate_specification_hash}`\n"
        f"- Package hash: `{package.package_hash}`\n"
        "- Broker connectivity: `false`\n"
        "- Outbound order capability: `false`\n"
        "- Execution: conservative virtual fills only\n"
    )
    _write_immutable(machine, payload)
    _write_immutable(dossier, human)
    return machine, dossier


def _write_immutable(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise ShadowPackageError(f"Immutable shadow package drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
