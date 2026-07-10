from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from hydra.governance.invariants import GovernanceCheckResult, assert_governance_passes, run_governance_checks
from hydra.governance.protected_manifest import build_protected_manifest, write_manifest
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


KERNEL_VERSION = "hydra_governance_kernel_v1"


@dataclass(frozen=True)
class GovernanceKernelStatus:
    version: str
    checked_at_utc: str
    baseline_commit: str
    manifest_path: str
    manifest_hash: str
    result: GovernanceCheckResult

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["result"] = self.result.to_dict()
        return out


def initialize_governance_kernel(
    *,
    baseline_commit: str,
    manifest_path: str = "mission/state/governance_protected_manifest.json",
    remaining_budget_usd: float | None = None,
) -> GovernanceKernelStatus:
    manifest = build_protected_manifest(baseline_commit=baseline_commit)
    path = write_manifest(manifest, manifest_path)
    result = assert_governance_passes(baseline_commit=baseline_commit, remaining_budget_usd=remaining_budget_usd)
    status = GovernanceKernelStatus(
        version=KERNEL_VERSION,
        checked_at_utc=utc_now_iso(),
        baseline_commit=baseline_commit,
        manifest_path=str(path),
        manifest_hash=manifest.manifest_hash(),
        result=result,
    )
    status_path = project_path("mission", "state", "governance_kernel_status.md")
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        "# HYDRA Governance Kernel Status\n\n```json\n"
        + json.dumps(status.to_dict(), indent=2, sort_keys=True, default=str)
        + "\n```\n",
        encoding="utf-8",
    )
    return status


def check_governance_kernel(*, baseline_commit: str, remaining_budget_usd: float | None = None) -> GovernanceCheckResult:
    return run_governance_checks(baseline_commit=baseline_commit, remaining_budget_usd=remaining_budget_usd)
