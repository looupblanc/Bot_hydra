from __future__ import annotations

import ast
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Iterable

from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.equity_open_gap_reversal import _write_immutable
from hydra.shadow.specification import ShadowSpecification


VERSION = "ym_immutable_shadow_activation_v1"
CANDIDATE_ID = "strategy_open_gap_continuation_YM_v1"
PROHIBITED_CALL_NAMES = frozenset(
    {"submit_order", "place_order", "send_order", "transmit_order", "execute_order"}
)
PROHIBITED_IMPORT_TOKENS = ("broker", "ibapi", "interactive_brokers", "ccxt")


class ShadowActivationError(RuntimeError):
    pass


def run_ym_shadow_activation(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    strict_result_path: str | Path,
    strict_result_sha256: str,
    strict_result_hash: str,
    shadow_configuration_path: str | Path,
    shadow_configuration_sha256: str,
    shadow_configuration_hash: str,
    code_commit: str,
    code_surface_paths: Iterable[str | Path] | None = None,
) -> dict[str, Any]:
    task = Path(engineering_task_path)
    strict_path = Path(strict_result_path)
    configuration_path = Path(shadow_configuration_path)
    _verify(task, engineering_task_sha256, "engineering task")
    _verify(strict_path, strict_result_sha256, "strict promotion result")
    _verify(configuration_path, shadow_configuration_sha256, "shadow configuration")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise ShadowActivationError("Worker commit differs from the queued specification.")

    strict = json.loads(strict_path.read_text(encoding="utf-8"))
    if (
        strict.get("result_hash") != strict_result_hash
        or strict.get("candidate_id") != CANDIDATE_ID
        or not bool(strict.get("shadow_activation_eligible"))
        or list(strict.get("hard_invalidations") or [])
    ):
        raise ShadowActivationError("Strict result does not authorize zero-order activation.")
    specification = _load_specification(configuration_path)
    if (
        specification.strategy_id != CANDIDATE_ID
        or specification.configuration_hash != shadow_configuration_hash
        or specification.outbound_orders_enabled
    ):
        raise ShadowActivationError("Frozen configuration is not the authorized zero-order version.")
    surfaces = list(code_surface_paths or _default_code_surfaces())
    surface_audit = audit_zero_order_surface(surfaces)
    if not surface_audit["passed"]:
        raise ShadowActivationError(
            f"Prohibited order surface found: {surface_audit['violations']}"
        )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "schema": "hydra_shadow_activation_manifest_v1",
        "candidate_id": CANDIDATE_ID,
        "registry_evidence_tier": "SHADOW_ACTIVE",
        "operational_classification": "SHADOW_RESEARCH_ACTIVE",
        "strict_result_hash": strict_result_hash,
        "configuration_path": str(configuration_path),
        "configuration_sha256": shadow_configuration_sha256,
        "configuration_hash": shadow_configuration_hash,
        "markets": list(specification.markets),
        "timeframes": list(specification.timeframes),
        "stale_data_seconds": specification.stale_data_seconds,
        "maximum_exposure": specification.maximum_exposure,
        "simulated_mll_floor": specification.simulated_mll_floor,
        "internal_daily_risk_limit": specification.internal_daily_risk_limit,
        "kill_conditions": list(specification.kill_conditions),
        "outbound_orders_enabled": False,
        "broker_connections_allowed": 0,
        "virtual_execution_only": True,
        "missing_or_stale_data_policy": "FAIL_CLOSED_NO_SIGNAL_NO_FILL",
        "initial_runtime_state": "WAITING_FOR_FRESH_FORWARD_DATA",
        "q4_access_allowed": False,
        "live_or_broker_allowed": False,
        "code_surface_audit": surface_audit,
        "code_commit": code_commit,
    }
    manifest["activation_manifest_hash"] = _stable_hash(manifest)
    manifest_path = destination / "ym_shadow_activation_manifest.json"
    _write_immutable(manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    source_candidate = (strict.get("candidates") or [{}])[0]
    active_candidate = {
        **source_candidate,
        "candidate_id": CANDIDATE_ID,
        "status": "SHADOW_ACTIVE",
        "operational_classification": "SHADOW_RESEARCH_ACTIVE",
        "activation_manifest_hash": manifest["activation_manifest_hash"],
        "topstep": source_candidate.get("topstep") or strict.get("topstep") or {},
    }
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": "YM_IMMUTABLE_ZERO_ORDER_SHADOW_ACTIVATED",
        "interpretation_boundary": (
            "Activation starts fail-closed forward observation only. It proves neither temporal "
            "persistence nor PAPER_SHADOW_READY and can never submit a real order."
        ),
        "candidate_id": CANDIDATE_ID,
        "candidate_count": 0,
        "candidates": [active_candidate],
        "shadow_active": 1,
        "shadow_candidates": 1,
        "promising_candidates": 1,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(
            bool((active_candidate.get("topstep") or {}).get("path_candidate"))
        ),
        "activation_manifest": manifest,
        "activation_manifest_path": str(manifest_path),
        "runtime_state": "WAITING_FOR_FRESH_FORWARD_DATA",
        "forward_signals": 0,
        "virtual_fills": 0,
        "governance": {
            "q4_access_count_delta": 0,
            "market_data_rows_read": 0,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": "RUN_SHADOW_PIPELINE_WHILE_DISCOVERY_AND_PROMOTION_CONTINUE",
        "code_commit": code_commit,
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "ym_shadow_activation_result.json"
    report_path = destination / "ym_shadow_activation_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "activation_manifest_path": str(manifest_path),
        },
        "report_path": str(report_path),
    }


def audit_zero_order_surface(paths: Iterable[str | Path]) -> dict[str, Any]:
    violations: list[dict[str, str]] = []
    hashes: dict[str, str] = {}
    for raw_path in paths:
        path = Path(raw_path)
        if not path.is_file():
            violations.append({"path": str(path), "reason": "missing_surface"})
            continue
        source = path.read_text(encoding="utf-8")
        hashes[str(path)] = hashlib.sha256(source.encode()).hexdigest()
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [alias.name for alias in node.names]
                if isinstance(node, ast.ImportFrom) and node.module:
                    names.append(node.module)
                for name in names:
                    lowered = name.lower()
                    if any(token in lowered for token in PROHIBITED_IMPORT_TOKENS):
                        violations.append({"path": str(path), "reason": f"prohibited_import:{name}"})
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name.lower() in PROHIBITED_CALL_NAMES:
                    violations.append({"path": str(path), "reason": f"prohibited_function:{node.name}"})
            if isinstance(node, ast.Attribute) and node.attr.lower() in PROHIBITED_CALL_NAMES:
                violations.append({"path": str(path), "reason": f"prohibited_attribute:{node.attr}"})
    return {
        "passed": not violations,
        "files": hashes,
        "violations": violations,
        "outbound_order_capability": False if not violations else None,
    }


def _default_code_surfaces() -> tuple[Path, ...]:
    root = Path(__file__).resolve().parents[2]
    return (
        root / "hydra/shadow/runner.py",
        root / "hydra/shadow/virtual_execution.py",
        root / "hydra/shadow/risk_guard.py",
        root / "hydra/shadow/signal_bus.py",
        root / "hydra/shadow/specification.py",
        root / "scripts/run_shadow_portfolio.py",
    )


def _load_specification(path: Path) -> ShadowSpecification:
    payload = json.loads(path.read_text(encoding="utf-8"))
    supplied_hash = payload.pop("configuration_hash", None)
    for key in ("feature_versions", "markets", "timeframes", "kill_conditions"):
        payload[key] = tuple(payload[key])
    specification = ShadowSpecification(**payload)
    specification.validate()
    if supplied_hash != specification.configuration_hash:
        raise ShadowActivationError("Configuration hash does not recompute.")
    return specification


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ShadowActivationError(f"Frozen {label} is missing or changed: {path}")


def _render_report(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Immutable YM Zero-Order Shadow Activation",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Operational classification: `{payload['activation_manifest']['operational_classification']}`",
            f"- Runtime state: `{payload['runtime_state']}`",
            f"- Configuration: `{payload['activation_manifest']['configuration_hash']}`",
            "- Outbound order capability: `false`",
            "- Broker connections: `0`",
            "- Q4 access: `0`",
            "- PAPER_SHADOW_READY: `0`",
            "",
            "## Interpretation boundary",
            "",
            str(payload["interpretation_boundary"]),
            "",
        ]
    )
