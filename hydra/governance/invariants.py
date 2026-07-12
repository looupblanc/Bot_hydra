from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from hydra.data.budget import DatabentoBudgetConfig, cumulative_spend
from hydra.governance.protected_manifest import build_protected_manifest
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.evidence_scope import ComputationMode, EvidenceScope
from hydra.validation.promotion_contract import evidence_can_support_scope
from hydra.validation.status_provenance import make_status_provenance


class GovernanceViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class GovernanceCheckResult:
    passed: bool
    checks: dict[str, bool]
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def registry_integrity(path: str = "registry/hydra_registry.db") -> str:
    db_path = project_path(path)
    if not db_path.exists():
        return "MISSING"
    conn = sqlite3.connect(db_path)
    try:
        return str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        conn.close()


def q4_access_count(ledger_path: str = "reports/data_access/data_access_ledger.jsonl") -> int:
    path = project_path(ledger_path)
    if not path.exists():
        return 0
    count = 0
    q4_start = "2024-10-01"
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("data_role") in {DataRole.SEALED_BLIND_HOLDOUT.value, DataRole.FINAL_LOCKBOX.value}:
            count += 1
            continue
        period = str(row.get("period_accessed") or "")
        if ":" not in period:
            continue
        start, end = (
            _normalize_exclusive_boundary(value)
            for value in period.split(":", 1)
        )
        if start >= q4_start or end > q4_start:
            count += 1
    return count


def governance_semantic_hash(
    config_path: str = "config/governance/hydra_governance_v1.yaml",
) -> str:
    import yaml

    payload = yaml.safe_load(project_path(config_path).read_text(encoding="utf-8"))
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _normalize_exclusive_boundary(value: str) -> str:
    """Remove one exact terminal metadata marker before date comparison.

    A repeated or non-terminal marker is deliberately left partially or fully
    intact, so malformed labels cannot use this normalization as a Q4-count
    evasion path.
    """

    marker = "_EXCLUSIVE"
    return value[: -len(marker)] if value.endswith(marker) else value


def assert_no_live_trading_enabled() -> bool:
    prohibited = ("BROKER_API_KEY", "TOPSTEP_USERNAME", "LIVE_TRADING_ENABLED")
    return os.environ.get("LIVE_TRADING_ENABLED", "").lower() not in {"1", "true", "yes"} and not any(
        key in os.environ and key != "LIVE_TRADING_ENABLED" for key in prohibited
    )


def assert_lower_scope_cannot_promote() -> bool:
    evidence = make_status_provenance(
        status="COMPONENT_PASS",
        scope=EvidenceScope.COMPONENT,
        payload={"component": "control"},
        code_commit="governance",
        data_fingerprint="control",
        validation_version="governance",
        policy_version="governance",
        computation_mode=ComputationMode.FULL,
        evidence_strength=99.0,
        passed=True,
    )
    return not evidence_can_support_scope(evidence, EvidenceScope.EDGE_ATOM)


def run_governance_checks(*, baseline_commit: str, remaining_budget_usd: float | None = None) -> GovernanceCheckResult:
    budget = DatabentoBudgetConfig()
    _estimated, actual = cumulative_spend(project_path(budget.ledger_path))
    manifest = build_protected_manifest(baseline_commit=baseline_commit)
    from hydra.governance.q4_one_shot import audit_q4_one_shot_state

    access_count = q4_access_count()
    q4_audit = audit_q4_one_shot_state(
        authorization_root=project_path("mission", "state", "q4_one_shot"),
        ledger_path=project_path("reports", "data_access", "data_access_ledger.jsonl"),
    )
    q4_policy_valid = bool(
        q4_audit.get("valid")
        and (
            (access_count == 0 and int(q4_audit.get("transaction_count") or 0) == 0)
            or (
                access_count == 1
                and int(q4_audit.get("transaction_count") or 0) == 1
                and q4_audit.get("status") in {"COMMITTED", "Q4_REVIEW_REQUIRED"}
            )
        )
    )
    checks = {
        "registry_integrity": registry_integrity() == "ok",
        # Compatibility key retained for existing monitoring.  Its V4 meaning
        # is now "unopened OR one valid closed manifest-bound transaction".
        "q4_not_accessed": q4_policy_valid,
        "budget_under_hard_cap": actual <= budget.hard_cap_usd,
        "remaining_budget_matches_or_exceeds_floor": remaining_budget_usd is None or remaining_budget_usd >= 0,
        "no_live_trading": assert_no_live_trading_enabled(),
        "scope_promotion_blocked": assert_lower_scope_cannot_promote(),
        "protected_files_exist": all(item.exists for item in manifest.digests),
    }
    details = {
        "registry_integrity_result": registry_integrity(),
        "q4_access_count": access_count,
        "q4_one_shot_audit": q4_audit,
        "governance_semantic_hash": governance_semantic_hash(),
        "cumulative_actual_databento_spend_usd": actual,
        "protected_manifest_hash": manifest.manifest_hash(),
        "missing_protected_files": [item.path for item in manifest.digests if not item.exists],
    }
    return GovernanceCheckResult(all(checks.values()), checks, details)


def assert_governance_passes(*, baseline_commit: str, remaining_budget_usd: float | None = None) -> GovernanceCheckResult:
    result = run_governance_checks(baseline_commit=baseline_commit, remaining_budget_usd=remaining_budget_usd)
    if not result.passed:
        raise GovernanceViolation(json.dumps(result.to_dict(), sort_keys=True))
    return result
