from __future__ import annotations

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
        start, end = period.split(":", 1)
        if start >= q4_start or end > q4_start:
            count += 1
    return count


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
    checks = {
        "registry_integrity": registry_integrity() == "ok",
        "q4_not_accessed": q4_access_count() == 0,
        "budget_under_hard_cap": actual <= budget.hard_cap_usd,
        "remaining_budget_matches_or_exceeds_floor": remaining_budget_usd is None or remaining_budget_usd >= 0,
        "no_live_trading": assert_no_live_trading_enabled(),
        "scope_promotion_blocked": assert_lower_scope_cannot_promote(),
        "protected_files_exist": all(item.exists for item in manifest.digests),
    }
    details = {
        "registry_integrity_result": registry_integrity(),
        "q4_access_count": q4_access_count(),
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

