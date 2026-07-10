from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole, parameters_mutable


class LockboxViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class DataAccessRecord:
    code_commit: str
    process_id: int
    timestamp_utc: str
    period_accessed: str
    data_role: str
    requesting_module: str
    candidate_ids: list[str]
    reason_for_access: str
    freeze_manifest_hash: str | None
    parameters_mutable: bool


def current_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def enforce_data_access(
    period: str,
    role: DataRole,
    requesting_module: str,
    candidate_ids: list[str],
    reason: str,
    freeze_manifest_hash: str | None,
    ledger_path: str = "reports/data_access/data_access_ledger.jsonl",
) -> DataAccessRecord:
    mutable = parameters_mutable(role)
    if role == DataRole.BLIND_VALIDATION and not freeze_manifest_hash:
        raise LockboxViolation("Q3 blind validation access requires a freeze manifest hash.")
    if role == DataRole.FINAL_LOCKBOX and not freeze_manifest_hash:
        raise LockboxViolation("Q4 final lockbox access requires an immutable freeze manifest hash.")
    record = DataAccessRecord(
        code_commit=current_commit(),
        process_id=os.getpid(),
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        period_accessed=period,
        data_role=role.value,
        requesting_module=requesting_module,
        candidate_ids=sorted(candidate_ids),
        reason_for_access=reason,
        freeze_manifest_hash=freeze_manifest_hash,
        parameters_mutable=mutable,
    )
    append_access_record(record, ledger_path)
    return record


def append_access_record(record: DataAccessRecord, ledger_path: str = "reports/data_access/data_access_ledger.jsonl") -> Path:
    path = project_path(ledger_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(record), sort_keys=True) + "\n")
    return path


def assert_no_q4_mutation(candidate_id: str, access_ledger: str = "reports/data_access/data_access_ledger.jsonl") -> None:
    path = project_path(access_ledger)
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("data_role") == DataRole.FINAL_LOCKBOX.value and candidate_id in set(row.get("candidate_ids") or []):
            raise LockboxViolation(
                f"Candidate {candidate_id} has final lockbox access recorded; mutation would invalidate lockbox status."
            )


def mark_lockbox_contamination(
    period: str,
    role: DataRole,
    reason: str,
    output: str = "reports/data_access/lockbox_contamination_events.jsonl",
) -> Path:
    path = project_path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "code_commit": current_commit(),
        "process_id": os.getpid(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "period_accessed": period,
        "data_role": role.value,
        "reason": reason,
        "consequence": "period_must_be_reclassified_as_development_for_affected_lineages",
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return path
